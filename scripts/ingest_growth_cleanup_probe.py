"""Small, synthetic current-only cleanup measurements; never opens user data.

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
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]

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
    CatalogRevision,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb.sql_performance import measure_sql  # noqa: E402 - select checkout.
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


class Recorder:
    def __init__(self) -> None:
        self.group = "operation"
        self.queries: dict[tuple[str, str, str], QueryMeasurement] = defaultdict(
            QueryMeasurement
        )

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        value = self.queries[(self.group, category, " ".join(query.split()))]
        value.calls += 1
        value.seconds += elapsed
        value.returned_rows += read_rows

    def report(self) -> list[dict[str, Any]]:
        return [
            {"group": group, "category": category, "sql": sql, **asdict(value)}
            for (group, category, sql), value in sorted(self.queries.items())
        ]


def measure[T](action: Callable[[], T]) -> tuple[T, dict[str, Any]]:
    """Attribute state discovery inside the real call without double counting."""
    recorder = Recorder()
    state_checks: list[float] = []
    advances: list[dict[str, Any]] = []
    transaction_advances: list[int] = []
    original_state = VNextCleanupRepository.current_only_maintenance_state
    original_advance = VNextCleanupRepository.advance_current_only_cycle

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
        measure_sql(recorder),
    ):
        started = time.perf_counter()
        result = action()
        elapsed = time.perf_counter() - started
    queries = recorder.report()
    return result, {
        "seconds": elapsed,
        "sql_calls": sum(q["calls"] for q in queries if q["category"] == "sql"),
        "sql_seconds": sum(q["seconds"] for q in queries if q["category"] == "sql"),
        "state_check_seconds": state_checks,
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
    expected_page: bytes,
) -> dict[str, Any]:
    if current.revision <= previous_revision:
        raise RuntimeError(
            "publication revision did not advance; replay is not a new revision"
        )
    if current.publication_count != gallery_count or current.artifact_count:
        raise RuntimeError("unexpected publication/artifact count")
    page = catalog.discover_publications(revision=current, limit=128)
    latest = next((item for item in page.publications if item.gid == 1), None)
    expected_content = effective_content_digest((sha256(expected_page).digest(),)).hex()
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


def run_case(
    config: CoreConfig,
    *,
    gallery_count: int,
    revisions: int,
    output: Path,
    report: dict[str, Any] | None = None,
    policy_change_at: int | None = None,
) -> dict[str, Any]:
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
) -> None:
    report["stage"] = "database_initialize"
    initialize_database(config)
    source = MemorySource([gallery(gid) for gid in range(1, gallery_count + 1)])
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
            expected_page = f"page 1 revision {source_revision}".encode()
            source.put(
                gallery(
                    1,
                    title=expected_title,
                    pages=[expected_page],
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
                expected_page=expected_page,
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
                expected_page=expected_page,
            )
            analyses, builds = retained_compaction_roots(config)
            case["retained_analysis_count"] = len(analyses)
            case["retained_source_build_count"] = len(builds)
            if layout.depth == 0 and (
                analyses != {layout.analysis_id} or builds != {layout.build_id}
            ):
                raise RuntimeError("full compaction did not reclaim obsolete history")
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
                        "empty_sql": case["empty_drain"]["sql_calls"],
                        "idle_claim_sql": case["idle_claim"]["sql_calls"],
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
    parser.add_argument("--galleries", type=int, choices=range(2, 6), default=2)
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
        choices=range(30, 601),
        default=180,
        help="cooperative POSIX alarm in seconds; native calls and teardown can exceed it",
    )
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
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
