"""Synthetic current-only cleanup and idle measurements; never opens user data.

Run from a checkout with its development dependencies installed. SQLite is the
default; MariaDB starts a private Testcontainer and accepts no server arguments.
COUNT snapshots are outside timed calls but still warm the database cache.
The POSIX SIGALRM timeout is cooperative: native driver calls and container
startup/teardown can delay interruption. It is not a hard process deadline.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import ingest_maintenance_mariadb as maria_diagnostics  # noqa: E402 - checkout helper.
from compaction_contracts import (  # noqa: E402 - checkout evidence helper.
    current_compaction_layout,
    retained_compaction_roots,
)
from vnext_fault_harness import (  # noqa: E402 - checkout fixture paths are set above.
    backend_of,
    open_connector,
    physical_tables,
)
from vnext_pipeline import (  # noqa: E402 - checkout fixture paths are set above.
    LEASE_MICROSECONDS,
    MemoryLibrary,
    MemorySource,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import (  # noqa: E402 - use this checkout's source, not an installed wheel.
    CatalogPublication,
    CatalogRevision,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb import (  # noqa: E402 - select checkout.
    vnext_cleanup_repository as cleanup_repository,
)
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - select checkout.
from h2hdb.sql_performance import (  # noqa: E402 - select checkout.
    _MeasuredConnector,
    measure_sql,
)
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402 - select checkout.
from h2hdb.vnext_cleanup_repository import (  # noqa: E402 - checkout source path is set above.
    CleanupBatchResult,
    VNextCleanupRepository,
)
from h2hdb.vnext_identity import (  # noqa: E402 - select checkout.
    effective_content_digest,
)


@dataclass
class QueryMeasurement:
    calls: int = 0
    seconds: float = 0.0
    returned_rows: int = 0


SQLITE_PROGRESS_QUANTUM = 100


@dataclass
class SQLiteProgressSample:
    callbacks: int = 0

    def advance(self) -> int:
        self.callbacks += 1
        return 0


@contextmanager
def sample_sqlite_candidate(
    connector: SQLConnector,
) -> Iterator[SQLiteProgressSample | None]:
    """Sample one candidate query on this probe's fresh private connection.

    The harness owns these connections and installs no other progress handler.
    SQLite exposes no handler getter: clearing to None is deliberately not a
    general-purpose restoration of an arbitrary caller's existing handler.
    """
    raw = (
        connector._connector if isinstance(connector, _MeasuredConnector) else connector
    )
    if not isinstance(raw, SQLiteConnector):
        yield None
        return
    sample = SQLiteProgressSample()
    raw.connection.set_progress_handler(sample.advance, SQLITE_PROGRESS_QUANTUM)
    try:
        yield sample
    finally:
        raw.connection.set_progress_handler(None, 0)


class Recorder:
    def __init__(self) -> None:
        self.group = "operation"
        self.target = "unclassified"
        self.queries: dict[tuple[str, str, str, str], QueryMeasurement] = defaultdict(
            QueryMeasurement
        )

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        value = self.queries[
            (self.group, self.target, category, " ".join(query.split()))
        ]
        value.calls += 1
        value.seconds += elapsed
        value.returned_rows += read_rows

    def report(self) -> list[dict[str, Any]]:
        return [
            {
                "group": group,
                "target": target,
                "category": category,
                "sql": sql,
                **asdict(value),
            }
            for (group, target, category, sql), value in sorted(self.queries.items())
        ]


def measure[T](
    action: Callable[[], T], *, profile_mariadb: bool = False
) -> tuple[T, dict[str, Any]]:
    """Attribute state discovery inside the real call without double counting."""
    recorder = Recorder()
    state_checks: list[float] = []
    candidate_probes: list[dict[str, Any]] = []
    advances: list[dict[str, Any]] = []
    transaction_advances: list[int] = []
    original_state = VNextCleanupRepository.current_only_maintenance_state
    original_advance = VNextCleanupRepository.advance_current_only_cycle

    def candidate_probe(
        original: Callable[..., int | None], target: str | None
    ) -> Callable[..., int | None]:
        def wrapped(*args: Any, **kwargs: Any) -> int | None:
            plan = args[1] if target is None and len(args) > 1 else kwargs.get("plan")
            if target is None:
                if plan is None:
                    raise RuntimeError(
                        "static candidate instrumentation requires a plan"
                    )
                measured_target = str(plan.kind.value)
            else:
                measured_target = target
            previous = recorder.target
            recorder.target = measured_target
            started = time.perf_counter()
            try:
                work = args[0] if args else kwargs["work"]
                with sample_sqlite_candidate(work.connector) as sample:
                    if profile_mariadb:
                        result, maria = maria_diagnostics.profile_candidate(
                            work.connector, lambda: original(*args, **kwargs)
                        )
                    else:
                        result, maria = original(*args, **kwargs), None
                candidate_probes.append(
                    {
                        "group": recorder.group,
                        "target": measured_target,
                        "seconds": time.perf_counter() - started,
                        "candidate_found": result is not None,
                        "mariadb_diagnostics": maria,
                        "sqlite_progress_callbacks": (
                            sample.callbacks if sample is not None else None
                        ),
                        "sqlite_progress_operations_estimate": (
                            sample.callbacks * SQLITE_PROGRESS_QUANTUM
                            if sample is not None
                            else None
                        ),
                    }
                )
                return result
            finally:
                recorder.target = previous

        return wrapped

    def state(*args: Any, **kwargs: Any) -> Any:
        previous = recorder.group
        recorder.group = "current_only_maintenance_state"
        started = time.perf_counter()
        try:
            return original_state(*args, **kwargs)
        finally:
            state_checks.append(time.perf_counter() - started)
            recorder.group = previous

    def advance(*args: Any, **kwargs: Any) -> tuple[CleanupBatchResult, ...]:
        results = original_advance(*args, **kwargs)
        transaction_advances.append(len(results))
        for result in results:
            advances.append(
                {
                    "target": result.cycle.target_kind.value,
                    "phase": result.phase,
                    "row_count": result.row_count,
                    "cycle_deleted_count": result.deleted_count,
                    "cycle_complete": result.cycle_complete,
                    "replayed": result.replayed,
                }
            )
        return results

    with (
        patch.object(VNextCleanupRepository, "current_only_maintenance_state", state),
        patch.object(VNextCleanupRepository, "advance_current_only_cycle", advance),
        ExitStack() as probes,
        measure_sql(recorder),
    ):
        # Wrap the original functions, without copying their SQL or selection
        # logic. Each SQL event retains its own duration and discovery group.
        for name, target in (
            ("_next_static_candidate_shard", None),
            ("_next_artifact_blob_candidate_shard", "ARTIFACT_BLOB"),
            ("_next_publication_identity_candidate_shard", "PUBLICATION_IDENTITY"),
            ("_next_file_name_candidate_shard", "FILE_NAME_IDENTITY"),
            ("_next_content_blob_candidate_shard", "CONTENT_BLOB"),
        ):
            probes.enter_context(
                patch.object(
                    cleanup_repository,
                    name,
                    candidate_probe(getattr(cleanup_repository, name), target),
                )
            )
        started = time.perf_counter()
        result = action()
        elapsed = time.perf_counter() - started
    queries = recorder.report()
    return result, {
        "seconds": elapsed,
        "sql_calls": sum(q["calls"] for q in queries if q["category"] == "sql"),
        "sql_seconds": sum(q["seconds"] for q in queries if q["category"] == "sql"),
        "state_check_seconds": state_checks,
        "candidate_probes": candidate_probes,
        "advance_count": len(advances),
        "advance_transactions": len(transaction_advances),
        "phases_per_transaction": transaction_advances,
        "logical_phase_rows": sum(
            value["row_count"] for value in advances if not value["replayed"]
        ),
        "advances": advances,
        "queries": queries,
    }


def counts(config: CoreConfig) -> dict[str, int]:
    backend = backend_of(config)
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            result = {}
            for name in physical_tables(backend):
                row = connector.fetch_one(f"SELECT COUNT(*) FROM {name}")
                assert row is not None
                if row[0]:
                    result[name] = int(row[0])
            return result
    finally:
        connector.close()


def net_decreases(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    """Net physical row-count decrease, not a count of DELETE statements."""
    return {
        name: count - after.get(name, 0)
        for name, count in before.items()
        if count > after.get(name, 0)
    }


def new_report(backend: str, gallery_count: int) -> dict[str, Any]:
    return {
        "status": "incomplete",
        "stage": "starting",
        "backend": backend,
        "gallery_count": gallery_count,
        "pages_per_gallery": 1,
        "artifacts_required": False,
        "history_policy": (
            "one changed gallery per source revision; optional policy-only compaction; "
            "drain after every publication"
        ),
        "measurement_notes": (
            "SQL timings exclude evidence COUNT scans, but those scans warm the cache. "
            "returned_rows count connector results, not database rows examined. "
            "state_check_seconds overlap SQL/wall time and must not be added to them. "
            "candidate_probes seconds also overlap SQL/state/wall time; query target "
            "attribution measures actual SQL calls inside each candidate probe. "
            "logical_phase_rows are cleanup "
            "receipt row_count values, not distinct galleries or physical deleted rows. "
            "advance_count counts durable phase receipts; advance_transactions counts "
            "bounded deletion transactions, which may coalesce empty phases. "
            "Net physical decreases may coexist with new durable cleanup receipts. "
            "Completed means the measurement and correctness checks finished, not that "
            "cleanup efficiency passed a performance threshold."
        ),
        "timeout_notes": (
            "POSIX SIGALRM is cooperative, not a hard deadline. Native driver calls "
            "and container startup/teardown can delay interruption or completion."
        ),
        "sqlite_progress_sampling": {
            "callback_quantum": SQLITE_PROGRESS_QUANTUM,
            "sqlite_runtime_version": sqlite3.sqlite_version
            if backend == "sqlite"
            else None,
            "notes": (
                "Candidate probes use SQLite progress callbacks times 100 as an "
                "approximate operation estimate, not examined rows or an exact VM "
                "instruction count. SQLite defines the interval as approximate; "
                "callbacks can include SQL preparation work. Sub-quantum work can "
                "be unreported; no strict error bound is claimed. Callback overhead "
                "is included in SQLite timings. Each private fixture connection "
                "has no other progress handler; finally clears this handler to None "
                "and does not restore arbitrary external handlers. MariaDB fields "
                "are null because its engine operations are not sampled."
            ),
            "reference": "https://www.sqlite.org/c3ref/progress_handler.html",
        },
        "cases": [],
    }


def write_report(output: Path, report: dict[str, Any]) -> None:
    """Replace complete JSON atomically; interruption keeps the previous report."""
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=".report-",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def record_failure(output: Path, report: dict[str, Any], error: BaseException) -> None:
    report["status"] = "failed"
    report["failure"] = {
        "stage": report["stage"],
        "reason": type(error).__name__,
        "message": "Original exception is re-raised; its text is omitted to protect configuration secrets.",
    }
    try:
        write_report(output, report)
    except BaseException as write_error:
        error.add_note(f"Failed to save probe report: {type(write_error).__name__}")


def source_provenance() -> dict[str, Any]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=3,
    ).stdout.strip()
    files = (
        Path(__file__).relative_to(ROOT),
        Path("scripts/ingest_maintenance_mariadb.py"),
        Path("tests/vnext_pipeline.py"),
        Path("tests/vnext_fault_harness.py"),
        Path("tests/compaction_contracts.py"),
        Path("src/h2hdb/vnext_ingest_facade.py"),
        Path("src/h2hdb/vnext_cleanup_repository.py"),
        Path("src/h2hdb/vnext_maintenance_gate_repository.py"),
    )
    return {
        "checkout": str(ROOT),
        "git_head": commit,
        "python": sys.version,
        "source_sha256": {
            str(path): sha256((ROOT / path).read_bytes()).hexdigest() for path in files
        },
        "note": "File digests describe measured checkout sources, including uncommitted edits.",
    }


def verify_publication(
    catalog: VNextCatalogFacade,
    current: CatalogRevision,
    *,
    previous_revision: int,
    gallery_count: int,
    expected_title: str,
    expected_page: bytes | tuple[bytes, ...],
) -> dict[str, Any]:
    if current.revision <= previous_revision:
        raise RuntimeError(
            "publication revision did not advance; replay is not a new revision"
        )
    if current.publication_count != gallery_count or current.artifact_count:
        raise RuntimeError("unexpected publication/artifact count")
    page = catalog.discover_publications(revision=current, limit=128)
    latest = next((item for item in page.publications if item.gid == 1), None)
    expected_pages = (
        (expected_page,) if isinstance(expected_page, bytes) else expected_page
    )
    expected_content = effective_content_digest(
        tuple(sha256(page).digest() for page in expected_pages)
    ).hex()
    if latest is None or (latest.title, latest.content_sha256) != (
        expected_title,
        expected_content,
    ):
        raise RuntimeError(
            "published gallery 1 does not match the latest synthetic title/content"
        )
    return {
        "actual_revision": current.revision,
        "gallery_1_title": latest.title,
        "gallery_1_content_sha256": latest.content_sha256,
        "expected_content_sha256": expected_content,
    }


def catalog_snapshot(
    catalog: VNextCatalogFacade,
) -> tuple[CatalogRevision, tuple[CatalogPublication, ...]]:
    """Read all public catalog facts for this deliberately bounded fixture."""
    revision = catalog.get_catalog_revision()
    page = catalog.discover_publications(revision=revision, limit=128)
    if (
        page.next_cursor is not None
        or len(page.publications) != revision.publication_count
    ):
        raise RuntimeError("idle probe catalog exceeds its bounded snapshot")
    return revision, page.publications


def measure_idle_sequence(
    facade: VNextIngestFacade,
    catalog: VNextCatalogFacade,
    config: CoreConfig,
    *,
    report: dict[str, Any],
    case: dict[str, Any],
    output: Path,
    profile_mariadb: bool = False,
) -> None:
    """Measure two consecutive DONE probes and one successful periodic claim."""
    before = catalog_snapshot(catalog)
    roots_before = retained_compaction_roots(config)
    lock_before: dict[str, int] | None = None
    if profile_mariadb:
        with closing(open_connector(config)) as connector:
            lock_before = maria_diagnostics.lock_counters(connector)
    sequence: list[dict[str, Any]] = []
    case["idle_sequence"] = sequence
    for ordinal in (1, 2):
        report["stage"] = f"idle_drain_{ordinal}"
        outcome, record = measure(
            lambda: facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
        )
        record.update(operation=f"drain_{ordinal}", outcome=str(outcome))
        sequence.append(record)
        if outcome is not VNextCurrentOnlyMaintenanceOutcome.DONE:
            raise RuntimeError("idle drain did not return DONE")
        if record["advance_count"]:
            raise RuntimeError("idle drain unexpectedly advanced cleanup")
    report["stage"] = "idle_periodic_claim"
    session, claim = measure(lambda: facade.try_claim_ingest(True, LEASE_MICROSECONDS))
    claim.update(operation="periodic_claim", granted=session is not None)
    sequence.append(claim)
    # Persist once after the complete measurement window. Serializing a growing
    # report between calls can perturb the next sample despite being outside its
    # timer. On failure the owning run_case still saves the in-memory samples.
    write_report(output, report)
    if session is None:
        raise RuntimeError("periodic claim after repeated DONE probes was refused")
    if lock_before is not None:
        with closing(open_connector(config)) as connector:
            lock_after = maria_diagnostics.lock_counters(connector)
        case["mariadb_baseline_row_lock_counters"] = {
            "before": lock_before,
            "after": lock_after,
            "delta": maria_diagnostics.counter_delta(lock_before, lock_after),
        }
    # This turn deliberately performs no source/catalog work. Complete through
    # the public API so the measured claim leaves no outstanding capability.
    report["stage"] = "idle_claim_complete"
    facade.complete_ingest(session)
    if catalog_snapshot(catalog) != before:
        raise RuntimeError("idle probes changed public catalog facts")
    if retained_compaction_roots(config) != roots_before:
        raise RuntimeError("idle probes changed retained analysis/source roots")
    # Completing the otherwise empty periodic turn can itself retire an
    # operational generation. Drain that new work outside the idle timings,
    # before another revision or the existing empty-drain correctness probe.
    report["stage"] = "idle_claim_followup_cleanup"
    for attempt in range(1, 257):
        outcome = facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            case["idle_claim_followup_cleanup"] = {
                "attempts": attempt,
                "outcome": str(outcome),
                "outside_idle_timings": True,
            }
            break
        if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
            raise RuntimeError(f"post-claim cleanup did not progress: {outcome}")
    else:
        raise RuntimeError("post-claim cleanup exceeded its 256-attempt budget")
    if profile_mariadb:
        report["stage"] = "idle_mariadb_candidate_diagnostics"
        with closing(open_connector(config)) as connector:
            roundtrip = maria_diagnostics.roundtrip_control(connector)
        outcome, diagnostic = measure(
            lambda: facade.drain_current_only_maintenance(LEASE_MICROSECONDS),
            profile_mariadb=True,
        )
        expected_targets = {
            kind.value for kind in cleanup_repository._CURRENT_ONLY_TARGET_PRIORITY
        }
        if (
            outcome is not VNextCurrentOnlyMaintenanceOutcome.DONE
            or diagnostic["advance_count"]
            or len(diagnostic["candidate_probes"]) != len(expected_targets)
            or {value["target"] for value in diagnostic["candidate_probes"]}
            != expected_targets
        ):
            raise RuntimeError(
                "MariaDB diagnostic pass did not cover the idle fixed point"
            )
        case["mariadb_candidate_diagnostics"] = {
            "roundtrip_control": roundtrip,
            "outcome": str(outcome),
            "measurement": diagnostic,
            **maria_diagnostics.diagnostic_notes(),
        }
    if catalog_snapshot(catalog) != before:
        raise RuntimeError("post-claim cleanup changed public catalog facts")
    if retained_compaction_roots(config) != roots_before:
        raise RuntimeError("post-claim cleanup changed retained analysis/source roots")
    case["idle_catalog_snapshot_unchanged"] = True
    case["idle_retained_roots_unchanged"] = True
    case["idle_claim_completed"] = True


def run_case(
    config: CoreConfig,
    *,
    gallery_count: int,
    revisions: int,
    output: Path,
    report: dict[str, Any] | None = None,
    policy_change_at: int | None = None,
    mode: Literal["cleanup", "idle"] = "cleanup",
    pages_per_gallery: int = 1,
    profile_mariadb: bool = False,
) -> dict[str, Any]:
    if mode not in {"cleanup", "idle"}:
        raise ValueError("mode must be cleanup or idle")
    if not 1 <= gallery_count <= 128 or not 1 <= pages_per_gallery <= 32:
        raise ValueError("galleries must be 1..128 and pages per gallery 1..32")
    if profile_mariadb and (mode != "idle" or config.database.sql_type != "mariadb"):
        raise ValueError("MariaDB diagnostics require MariaDB idle mode")
    if gallery_count * pages_per_gallery > 512:
        raise ValueError("synthetic fixture must not exceed 512 pages")
    if mode == "cleanup" and pages_per_gallery != 1:
        raise ValueError("page scaling requires idle mode")
    if not 1 <= revisions <= 20:
        raise ValueError("revisions must be between 1 and 20")
    if policy_change_at is not None and not 3 <= policy_change_at <= revisions:
        raise ValueError(
            "policy change requires genesis and an incremental predecessor"
        )
    owns_report = report is None
    report = (
        report
        if report is not None
        else new_report(config.database.sql_type, gallery_count)
    )
    write_report(output, report)
    try:
        report["mode"] = mode
        report["pages_per_gallery"] = pages_per_gallery
        report["mariadb_diagnostics_enabled"] = profile_mariadb
        report["measurement_notes"] += (
            " Idle mode snapshots the full public catalog and retained roots before "
            "two consecutive drains plus a periodic claim, and after public completion. "
            "Snapshot reads are outside timings and warm caches. Fixture setup, "
            "cleanup-to-DONE, claim completion, and READY audit are not idle timings."
            if mode == "idle"
            else ""
        )
        report["policy_change_at"] = policy_change_at
        report["stage"] = "source_provenance"
        report["source_provenance"] = source_provenance()
        _collect_case(
            config,
            gallery_count=gallery_count,
            revisions=revisions,
            output=output,
            report=report,
            policy_change_at=policy_change_at,
            mode=mode,
            pages_per_gallery=pages_per_gallery,
            profile_mariadb=profile_mariadb,
        )
    except BaseException as error:
        record_failure(output, report, error)
        raise
    report["status"] = "completed" if owns_report else "incomplete"
    report["stage"] = "measurement_completed"
    write_report(output, report)
    return report


def _collect_case(
    config: CoreConfig,
    *,
    gallery_count: int,
    revisions: int,
    output: Path,
    report: dict[str, Any],
    policy_change_at: int | None,
    mode: Literal["cleanup", "idle"],
    pages_per_gallery: int,
    profile_mariadb: bool,
) -> None:
    report["stage"] = "database_initialize"
    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                gid,
                locator=(f"gallery-{gid:03d}",),
                pages=[
                    f"page-{page}-of-{gid}".encode()
                    for page in range(pages_per_gallery)
                ],
            )
            for gid in range(1, gallery_count + 1)
        ]
    )
    library = MemoryLibrary(source)
    cases = report["cases"]
    previous_revision = 0
    with (
        VNextIngestFacade(config) as facade,
        closing(VNextCatalogFacade(config)) as catalog,
    ):
        for revision in range(1, revisions + 1):
            report["stage"] = f"revision_{revision}_source"
            # A real public policy change reaches the same full materialization
            # path without overriding the production overlay-depth contract.
            source_revision = revision - 1 if revision == policy_change_at else revision
            expected_title = f"Gallery 1 revision {source_revision}"
            expected_pages = tuple(
                f"page {page + 1} revision {source_revision}".encode()
                for page in range(pages_per_gallery)
            )
            source.put(
                gallery(
                    1,
                    locator=("gallery-001",),
                    title=expected_title,
                    pages=expected_pages,
                )
            )
            report["stage"] = f"revision_{revision}_claim"
            session, claim = measure(
                lambda: facade.try_claim_ingest(True, LEASE_MICROSECONDS)
            )
            if session is None:
                raise RuntimeError("claim after completed cleanup was refused")
            report["stage"] = f"revision_{revision}_publication"
            run_ingest_turn(
                facade,
                source=source,
                library=library,
                policy=ingest_policy(
                    artifacts_required=False,
                    spam_occurrence_threshold=(
                        7
                        if policy_change_at is not None and revision >= policy_change_at
                        else 3
                    ),
                ),
                session=session,
            )
            report["stage"] = f"revision_{revision}_publication_oracle"
            current = catalog.get_catalog_revision()
            oracle = verify_publication(
                catalog,
                current,
                previous_revision=previous_revision,
                gallery_count=gallery_count,
                expected_title=expected_title,
                expected_page=expected_pages,
            )
            previous_revision = current.revision
            layout = current_compaction_layout(config)
            case: dict[str, Any] = {
                "revision_ordinal": revision,
                **oracle,
                "published_count": current.publication_count,
                "overlay_depth": layout.depth,
                "policy_id": layout.policy_id,
                "trigger": "policy_change"
                if revision == policy_change_at
                else "source_change",
                "claim_after_prior_cleanup": claim,
                "before_cleanup": counts(config),
                "steps": [],
            }
            cases.append(case)
            if revision > 1:
                report["stage"] = f"revision_{revision}_pending_cleanup_claim"
                pending, case["claim_before_cleanup"] = measure(
                    lambda: facade.try_claim_ingest(True, LEASE_MICROSECONDS)
                )
                case["claim_before_cleanup"]["refused"] = pending is None
                if pending is not None:
                    raise RuntimeError(
                        "pending cleanup did not fence a new ingest claim"
                    )
            for step in range(256):
                report["stage"] = f"revision_{revision}_cleanup_step_{step}"
                before = counts(config)
                outcome, record = measure(
                    lambda: facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
                )
                after = counts(config)
                record.update(
                    {
                        "step": step,
                        "outcome": str(outcome),
                        "net_physical_decreases": net_decreases(before, after),
                        "rows_before": sum(before.values()),
                        "rows_after": sum(after.values()),
                    }
                )
                case["steps"].append(record)
                write_report(output, report)
                if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
                    break
                if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                    raise RuntimeError(f"unexpected cleanup outcome: {outcome}")
            else:
                raise RuntimeError("cleanup exceeded its 256-attempt budget")
            case["after_cleanup"] = counts(config)
            # A pre-cleanup publication check alone cannot detect deletion of
            # current authority. Re-read after DONE, and record exact retained
            # roots independently of physical row counts.
            verify_publication(
                catalog,
                catalog.get_catalog_revision(),
                previous_revision=current.revision - 1,
                gallery_count=gallery_count,
                expected_title=expected_title,
                expected_page=expected_pages,
            )
            analyses, builds = retained_compaction_roots(config)
            case["retained_analysis_count"] = len(analyses)
            case["retained_source_build_count"] = len(builds)
            if layout.depth == 0 and (
                analyses != {layout.analysis_id} or builds != {layout.build_id}
            ):
                raise RuntimeError("full compaction did not reclaim obsolete history")
            if mode == "idle":
                measure_idle_sequence(
                    facade,
                    catalog,
                    config,
                    report=report,
                    case=case,
                    output=output,
                    profile_mariadb=profile_mariadb,
                )
            report["stage"] = f"revision_{revision}_empty_drain"
            empty, case["empty_drain"] = measure(
                lambda: facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
            )
            if empty is not VNextCurrentOnlyMaintenanceOutcome.DONE:
                raise RuntimeError("immediate empty drain did not return DONE")
            report["stage"] = f"revision_{revision}_idle_claim"
            idle, case["idle_claim"] = measure(
                lambda: facade.try_claim_ingest(False, LEASE_MICROSECONDS)
            )
            if idle is not None:
                raise RuntimeError("idle nonperiodic claim unexpectedly succeeded")
            write_report(output, report)
            print(
                json.dumps(
                    {
                        "revision": revision,
                        "actual_revision": current.revision,
                        "overlay_depth": layout.depth,
                        "published": current.publication_count,
                        "attempts": len(case["steps"]),
                        "cleanup_seconds": sum(s["seconds"] for s in case["steps"]),
                        "cleanup_sql": sum(s["sql_calls"] for s in case["steps"]),
                        "logical_phase_rows": sum(
                            s["logical_phase_rows"] for s in case["steps"]
                        ),
                        **(
                            {
                                "idle_sequence": [
                                    {
                                        "operation": step["operation"],
                                        "seconds": step["seconds"],
                                        "sql_calls": step["sql_calls"],
                                    }
                                    for step in case["idle_sequence"]
                                ],
                                "post_claim_cleanup_attempts": case[
                                    "idle_claim_followup_cleanup"
                                ]["attempts"],
                            }
                            if mode == "idle"
                            else {
                                "empty_sql": case["empty_drain"]["sql_calls"],
                                "idle_claim_sql": case["idle_claim"]["sql_calls"],
                            }
                        ),
                    }
                ),
                flush=True,
            )
    if library.render_calls:
        raise RuntimeError("metadata-only probe unexpectedly rendered artifacts")
    report["stage"] = "final_full_ready_audit"
    if full_check(config).state != "READY":
        raise RuntimeError("final full READY audit failed")
    report["full_ready_audit"] = "passed"


@contextmanager
def database(backend: str, root: Path) -> Iterator[CoreConfig]:
    if backend == "sqlite":
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(root / "db.sqlite3")
            )
        )
        return
    from testcontainers.community.mysql import MySqlContainer

    container = MySqlContainer(
        image="mariadb:10.11.11",
        username="probe",
        password="synthetic-probe-password",
        root_password="synthetic-root-password",
        dbname="cleanup_probe",
    )
    original_error: BaseException | None = None
    try:
        container.start()
        config = CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb",
                host=container.get_container_host_ip(),
                port=int(container.get_exposed_port(container.port)),
                user="probe",
                password="synthetic-probe-password",
                database="cleanup_probe",
            )
        )
        connector = open_connector(config)
        try:
            version = connector.fetch_one("SELECT VERSION()")
            if version is None or not str(version[0]).startswith("10.11.11-"):
                raise RuntimeError(f"unexpected isolated MariaDB version: {version}")
        finally:
            connector.close()
        yield config
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            container.stop()
        except BaseException as stop_error:
            if original_error is None:
                raise
            original_error.add_note(
                f"Testcontainer cleanup also failed: {type(stop_error).__name__}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--mode", choices=("cleanup", "idle"), default="cleanup")
    parser.add_argument("--galleries", type=int, choices=range(1, 129), default=2)
    parser.add_argument(
        "--mariadb-diagnostics",
        action="store_true",
        help="separate idle SELECT replay, plans and handler counters on private MariaDB",
    )
    parser.add_argument(
        "--pages-per-gallery",
        type=int,
        choices=range(1, 33),
        default=1,
        help="idle mode only; galleries times pages is capped at 512",
    )
    parser.add_argument("--revisions", type=int, choices=range(1, 21), default=3)
    parser.add_argument(
        "--policy-change-at",
        type=int,
        choices=range(3, 21),
        help="trigger real policy compaction after a genesis and incremental revision",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        choices=range(30, 1201),
        default=180,
        help="cooperative POSIX alarm in seconds; native calls and teardown can exceed it",
    )
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    if args.mariadb_diagnostics and (args.mode != "idle" or args.backend != "mariadb"):
        parser.error("--mariadb-diagnostics requires --mode idle --backend mariadb")
    if args.galleries * args.pages_per_gallery > 512:
        parser.error("--galleries times --pages-per-gallery must not exceed 512")
    if args.mode == "cleanup" and args.pages_per_gallery != 1:
        parser.error("--pages-per-gallery requires --mode idle")
    if args.policy_change_at is not None and args.policy_change_at > args.revisions:
        parser.error("--policy-change-at must not exceed --revisions")
    if not hasattr(signal, "SIGALRM"):
        parser.error(
            "this local probe requires POSIX SIGALRM; no hard deadline is provided"
        )
    args.output_directory.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix=f"{args.backend}-", dir=args.output_directory))
    output = root / "report.json"
    report = new_report(args.backend, args.galleries)
    report.update(mode=args.mode, pages_per_gallery=args.pages_per_gallery)
    report["stage"] = "database_startup"
    write_report(output, report)

    def expired(_signum: int, _frame: object) -> None:
        raise TimeoutError("cleanup probe cooperative alarm expired")

    signal.signal(signal.SIGALRM, expired)
    signal.alarm(args.timeout)
    try:
        with database(args.backend, root) as config:
            run_case(
                config,
                gallery_count=args.galleries,
                revisions=args.revisions,
                output=output,
                report=report,
                policy_change_at=args.policy_change_at,
                mode=args.mode,
                pages_per_gallery=args.pages_per_gallery,
                profile_mariadb=args.mariadb_diagnostics,
            )
            report["status"] = "incomplete"
            report["stage"] = "database_teardown"
            write_report(output, report)
        report["status"] = "completed"
        report["stage"] = "measurement_and_teardown_completed"
        write_report(output, report)
    except BaseException as error:
        record_failure(output, report, error)
        raise
    finally:
        signal.alarm(0)
    print(f"report={root / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
