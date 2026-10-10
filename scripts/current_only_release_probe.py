"""Local public-workflow experiment for repeated empty cleanup candidate probes.

Inputs are synthetic. MariaDB always owns a private 10.11.11 Testcontainer;
there is deliberately no server/credential option. No foreign keys are disabled.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(os.environ.get("H2HDB_PROBE_SOURCE_ROOT", str(ROOT))).resolve()
sys.path[:0] = [str(SOURCE_ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]
# Pin the package before checkout helpers prepend their own source directory.
if not Path(str(__import__("h2hdb").__file__)).is_relative_to(SOURCE_ROOT / "src"):
    raise RuntimeError("measured package does not match the selected source root")

import ingest_growth_cleanup_probe as growth  # noqa: E402 - checkout helpers.
import ingest_maintenance_mariadb as maria  # noqa: E402 - checkout helpers.
from vnext_fault_harness import open_connector  # noqa: E402 - synthetic native oracle.
from vnext_pipeline import (  # noqa: E402 - checkout helper paths are set above.
    LEASE_MICROSECONDS,
    Clock,
    MemoryLibrary,
    MemorySource,
    catalog_view,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    library_view,
    run_ingest_turn,
    takeover_clock,
)

from h2hdb import (  # noqa: E402 - explicit measured source.
    CoreConfig,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb import (  # noqa: E402 - measured source path.
    vnext_artifact_release_repository as artifact_release,
)
from h2hdb import (  # noqa: E402 - measured source path.
    vnext_maintenance_gate_repository as gate,
)
from h2hdb._cleanup import cycle as cleanup_cycle  # noqa: E402 - measured source path.
from h2hdb._cleanup import registry as cleanup_registry  # noqa: E402 - measured source path.
from h2hdb._cleanup import selection as cleanup_selection  # noqa: E402 - measured source path.
from h2hdb._cleanup.targets import resources as cleanup_resources  # noqa: E402 - measured source path.
from h2hdb._ingest import (  # noqa: E402 - measured source path.
    maintenance,
)
from h2hdb.domain import (  # noqa: E402 - measured source path.
    CurrentOnlyCleanupTerminalState,
)
from h2hdb.mariadb_connector import (  # noqa: E402 - measured source path.
    MariaDBConnector,
)
from h2hdb.sql_connector import (  # noqa: E402 - measured source path.
    SQLConnector,
)
from h2hdb.sql_performance import (  # noqa: E402 - measured source path.
    _MeasuredConnector,
)
from h2hdb.sqlite_connector import (  # noqa: E402 - measured source path.
    SQLiteConnector,
)

# The selector COST_CONTRACT was fixed before candidate implementation. The
# additional NATIVE_BUDGET was fixed after the candidate existed, before paired
# measurements with matching native instrumentation. Neither is a wall-time
# budget or evidence for the 24-hour production goal.
MEASUREMENT_SCHEMA = 3
MAX_DRAIN_CALLS = 2048
NATIVE_BUDGET = {"pure_release_ratio_max": 0.5, "whole_drain_ratio_max": 1.1}
COST_CONTRACT = {
    "pure_release_eligibility_probes_per_call_max": 0,
    "terminal_freshly_fenced_eligibility_passes_min": 1,
    "release_calls_per_resource_max": 1,
    "max_drain_calls": MAX_DRAIN_CALLS,
    "definition": "A pure release call acknowledges resources and performs no database cleanup batches.",
}
N_TABLES = (
    "catalog_canonical_value_allocation_anchors",
    "catalog_content_blobs",
    "catalog_file_name_identities",
    "catalog_gallery_observations",
    "catalog_prepared_artifacts",
)
PROBES = (
    (cleanup_selection, "_next_static_candidate_shard"),
    (cleanup_resources, "_next_artifact_blob_candidate_shard"),
    (cleanup_resources, "_next_publication_identity_candidate_shard"),
    (cleanup_resources, "_next_file_name_candidate_shard"),
    (cleanup_resources, "_next_content_blob_candidate_shard"),
)
_REUSABLE_ABSENCES = frozenset({"CONTENT_BLOB", "FILE_NAME_IDENTITY"})


def _proof_record(proof: Any) -> dict[str, Any] | None:
    if proof is None:
        return None
    return {
        "owner_token": proof.owner_token.hex(),
        "gate_generation": proof.gate_generation,
        "cycle_cutoff_at": proof.cycle_cutoff_at,
        "absent_targets": sorted(proof.absent_targets),
    }


@contextmanager
def terminal_authority() -> Iterator[list[dict[str, Any]]]:
    """Observe committed private transactions without changing shipped telemetry.

    Each measurement owns a new ledger: absence cannot cross public calls. The
    original fence and candidate functions run unchanged. Failed transactions
    append no committed event; maintenance helpers return only after COMMIT.
    """

    events: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    release_locks: list[gate.LockedGateRenewal] = []
    original_fence = cleanup_cycle._require_exclusive_gate
    original_lock = gate.MaintenanceGateRepository.lock_for_renewal
    original_live = gate.LockedGateRenewal.require_live

    def record_fence(lease: gate.GateLease, timestamp: int) -> None:
        assert active is not None
        active["fences"].append(
            {
                "owner_token": lease.owner_token.hex(),
                "gate_generation": lease.gate_generation,
                "mode": lease.mode.value,
                "slots": list(lease.slots),
                "validated_at": timestamp,
                "lease_expires_at": lease.lease_expires_at,
            }
        )

    def fence(*args: Any, **kwargs: Any) -> int:
        timestamp = original_fence(*args, **kwargs)
        if active is not None:
            record_fence(args[1], timestamp)
        return timestamp

    def lock(*args: Any, **kwargs: Any) -> gate.LockedGateRenewal:
        locked = original_lock(*args, **kwargs)
        if active is not None and active["kind"] == "release":
            release_locks.append(locked)
        return locked

    def live(locked: gate.LockedGateRenewal, *, now: int) -> gate.GateLease:
        current = original_live(locked, now=now)
        if (
            active is not None
            and active["kind"] == "release"
            and any(locked is observed for observed in release_locks)
        ):
            # Only a successful live check of an object freshly loaded and
            # exact-matched by the native locking repository in this release
            # transaction can supply release authority. Caller lease fields
            # alone, or a live check on a previously locked object, cannot.
            record_fence(current, now)
        return current

    def probe(original: Callable[..., Any], name: str) -> Callable[..., Any]:
        def observed(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            if active is not None:
                target = {
                    "_next_artifact_blob_candidate_shard": "ARTIFACT_BLOB",
                    "_next_publication_identity_candidate_shard": "PUBLICATION_IDENTITY",
                    "_next_file_name_candidate_shard": "FILE_NAME_IDENTITY",
                    "_next_content_blob_candidate_shard": "CONTENT_BLOB",
                }.get(name)
                if target is None:
                    plan = args[1] if len(args) > 1 else kwargs["plan"]
                    target = plan.kind.value
                active["queries"].append(
                    {"target": target, "candidate_found": result is not None}
                )
            return result

        return observed

    def transaction(original: Callable[..., Any], kind: str) -> Callable[..., Any]:
        def observed(*args: Any, **kwargs: Any) -> Any:
            nonlocal active
            if active is not None:
                raise RuntimeError("nested terminal-authority instrumentation")
            release_locks.clear()
            event: dict[str, Any] = {
                "kind": kind,
                "fences": [],
                "queries": [],
            }
            if kind in {"selection", "state"}:
                event["cycle_cutoff_at"] = kwargs["cycle_cutoff_at"]
                event["proof_in"] = _proof_record(kwargs["eligibility_proof"])
            active = event
            try:
                result = original(*args, **kwargs)
            finally:
                active = None
                release_locks.clear()
            # Appending after the private maintenance helper returns records COMMIT,
            # not merely a repository result from a still-open transaction.
            if kind == "selection":
                event["proof_out"] = _proof_record(result.eligibility_proof)
                event["state"] = (
                    result.cycle.value
                    if isinstance(result.cycle, CurrentOnlyCleanupTerminalState)
                    else "ACTIONABLE"
                )
            elif kind == "state":
                event["state"] = result.value
            elif kind == "advance":
                event["target"] = kwargs["cycle"].target_kind.value
            elif kind == "release":
                lease = args[2]
                event["owner_token"] = lease.owner_token.hex()
                event["gate_generation"] = lease.gate_generation
            events.append(event)
            return result

        return observed

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(cleanup_cycle, "_require_exclusive_gate", fence)
        )
        stack.enter_context(
            patch.object(gate.MaintenanceGateRepository, "lock_for_renewal", lock)
        )
        stack.enter_context(patch.object(gate.LockedGateRenewal, "require_live", live))
        for owner, name in PROBES:
            stack.enter_context(
                patch.object(owner, name, probe(getattr(owner, name), name))
            )
        for name, kind in (
            ("_next_cycle", "selection"),
            ("_state", "state"),
            ("_advance_shard", "advance"),
            ("_release_lease", "release"),
        ):
            stack.enter_context(
                patch.object(
                    maintenance.CurrentOnlyMaintenance,
                    name,
                    transaction(
                        getattr(maintenance.CurrentOnlyMaintenance, name), kind
                    ),
                )
            )
        yield events


def terminal_evidence(events: list[dict[str, Any]]) -> dict[str, bool]:
    """Independently reconstruct absence from queries, fences and invalidation.

    A serialized proof is never sufficient by itself. Its complete contents must
    equal observations committed earlier in this same ledger. No performance
    budget is relaxed: skipped SQL remains skipped work, not a synthetic probe.
    """

    priority = [
        target.value for target in cleanup_registry._CURRENT_ONLY_TARGET_PRIORITY
    ]
    known: set[str] = set()
    identity: tuple[Any, ...] | None = None
    terminal = False
    fresh_scan = False
    try:
        for event in events:
            kind = event["kind"]
            if kind not in {"selection", "state", "advance", "release"}:
                raise ValueError("unknown authority event")
            fences = event["fences"]
            if len(fences) != 1:
                raise ValueError("missing exact fresh fence")
            fence = fences[0]
            if (
                fence["mode"] != "EXCLUSIVE"
                or fence["slots"] != list(range(64))
                or not 0 <= fence["validated_at"] < fence["lease_expires_at"]
            ):
                raise ValueError("invalid exclusive authority")
            gate = (fence["owner_token"], fence["gate_generation"])
            if kind == "release":
                if (
                    event is not events[-1]
                    or identity is None
                    or gate != identity[:2]
                    or gate != (event["owner_token"], event["gate_generation"])
                ):
                    raise ValueError("missing terminal owner release")
                continue
            if kind == "advance":
                if identity is None or gate != identity[:2]:
                    raise ValueError("cleanup changed proof owner")
                if event["target"] != "CANONICAL_VALUE":
                    known.clear()
                terminal = False
                continue
            key = (*gate, event["cycle_cutoff_at"])
            incoming = event["proof_in"]
            if incoming is None:
                known.clear()
            elif identity != key or incoming != {
                "owner_token": key[0],
                "gate_generation": key[1],
                "cycle_cutoff_at": key[2],
                "absent_targets": sorted(known),
            }:
                raise ValueError("unproven or invalidated absence")
            identity = key
            reused = set(known)
            queries = event["queries"]
            expected = [target for target in priority if target not in reused]
            if [query["target"] for query in queries] != expected[: len(queries)]:
                raise ValueError("candidate priority or evidence mismatch")
            if any(query["candidate_found"] for query in queries[:-1]):
                raise ValueError("candidate search continued after match")
            for query in queries:
                if (
                    not query["candidate_found"]
                    and query["target"] in _REUSABLE_ABSENCES
                ):
                    known.add(query["target"])
            if kind == "selection" and event["proof_out"] != {
                "owner_token": key[0],
                "gate_generation": key[1],
                "cycle_cutoff_at": key[2],
                "absent_targets": sorted(known),
            }:
                raise ValueError("returned proof differs from committed observations")
            terminal = (
                event["state"] == "DONE"
                and len(queries) == len(expected)
                and not any(query["candidate_found"] for query in queries)
            )
            fresh_scan = terminal and not reused
        valid = terminal and bool(events) and events[-1]["kind"] == "release"
    except KeyError, TypeError, ValueError:
        valid, fresh_scan = False, False
    return {"valid": valid, "fresh_scan": fresh_scan}


class FixtureBoundary(RuntimeError):
    """Stop after valid public preparation, before publication validation."""


def dimensions(config: CoreConfig) -> dict[str, Any]:
    with closing(open_connector(config)) as connector:
        with connector.read_transaction():
            rows = {}
            for table in N_TABLES:
                row = connector.fetch_one(f"SELECT COUNT(*) FROM {table}")
                if not row:
                    raise RuntimeError("native count oracle returned no row")
                rows[table] = int(row[0])
            states = dict(
                connector.fetch_all(
                    "SELECT state, COUNT(*) FROM catalog_prepared_artifacts GROUP BY state"
                )
            )
            orphan = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_prepared_artifacts prepared "
                "WHERE prepared.state IN ('PENDING', 'PREPARED') "
                "AND NOT EXISTS (SELECT 1 FROM operational_catalog_working_candidates working "
                "WHERE working.candidate_id = prepared.candidate_id) "
                "AND NOT EXISTS (SELECT 1 FROM catalog_publication_commits committed "
                "WHERE committed.candidate_id = prepared.candidate_id)"
            )
            if not orphan:
                raise RuntimeError("native orphan count oracle returned no row")
    return {"N": rows, "prepared_states": states, "orphan_resources": int(orphan[0])}


def seed(
    config: CoreConfig, galleries: int, *, unique_filenames: bool
) -> MemoryLibrary:
    """Produce a current catalog plus abandoned resources with public writers."""
    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                gid,
                other_files={f"unique-{gid}.bin".encode(): f"opaque-{gid}".encode()}
                if unique_filenames
                else None,
            )
            for gid in range(1, galleries + 1)
        ]
    )
    library = MemoryLibrary(source)

    def stop(label: str) -> None:
        if label == "publication.commit:VALIDATE_PREPARED":
            raise FixtureBoundary

    with closing(VNextIngestFacade(config, clock=Clock())) as facade:
        try:
            run_ingest_turn(facade, source=source, library=library, boundary=stop)
        except FixtureBoundary:
            pass
        else:
            raise RuntimeError("fixture did not reach its abandoned candidate boundary")
    with closing(VNextIngestFacade(config, clock=takeover_clock())) as facade:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(spam_occurrence_threshold=7),
        )
    if full_check(config).state != "READY":
        raise RuntimeError("publicly generated fixture failed READY")
    return library


@contextmanager
def native_candidates(backend: str) -> Iterator[list[dict[str, int]]]:
    """Record all Handler reads of each actual candidate query, not a replay.

    Status reads bypass the SQL observer. Their overhead is included in measured
    wall time; it is not separately timed or subtracted to estimate clean runtime.
    SQLite instruction estimates are supplied by growth.measure's progress hook.
    """
    records: list[dict[str, int]] = []
    if backend != "mariadb":
        yield records
        return

    def wrap(original: Callable[..., Any]) -> Callable[..., Any]:
        def measured(*args: Any, **kwargs: Any) -> Any:
            work = args[0] if args else kwargs["work"]
            raw: SQLConnector = work.connector
            if isinstance(raw, _MeasuredConnector):
                raw = raw._connector
            if not isinstance(raw, MariaDBConnector):
                raise RuntimeError("MariaDB native measurement got another backend")
            before = maria.counters(raw, maria.HANDLER_STATUS)
            try:
                return original(*args, **kwargs)
            finally:
                after = maria.counters(raw, maria.HANDLER_STATUS)
                records.append(maria.counter_delta(before, after))

        return measured

    with ExitStack() as stack:
        for owner, name in PROBES:
            stack.enter_context(patch.object(owner, name, wrap(getattr(owner, name))))
        yield records


@contextmanager
def native_operation(backend: str) -> Iterator[dict[str, Any]]:
    """Count native work across every operation-owned connection, including hints."""
    totals: dict[str, Any] = {
        "connections": 0,
        "handler_reads": {},
        "sqlite_progress_callbacks": 0,
    }
    if backend == "mariadb":
        before: dict[int, dict[str, int]] = {}
        original_connect = MariaDBConnector.connect
        original_close = MariaDBConnector.close

        def connect(raw: MariaDBConnector) -> None:
            original_connect(raw)
            before[id(raw)] = maria.counters(raw, maria.HANDLER_STATUS)
            totals["connections"] += 1

        def close(raw: MariaDBConnector) -> None:
            initial = before.pop(id(raw), None)
            try:
                if initial is not None:
                    delta = maria.counter_delta(
                        initial, maria.counters(raw, maria.HANDLER_STATUS)
                    )
                    for name, count in delta.items():
                        totals["handler_reads"][name] = (
                            totals["handler_reads"].get(name, 0) + count
                        )
            finally:
                original_close(raw)

        with (
            patch.object(MariaDBConnector, "connect", connect),
            patch.object(MariaDBConnector, "close", close),
        ):
            yield totals
        if before:
            raise RuntimeError("measured MariaDB connection did not close")
        totals["handler_read_total"] = sum(totals["handler_reads"].values())
        return
    original_sqlite_connect = SQLiteConnector.connect
    original_sqlite_close = SQLiteConnector.close

    def advance() -> int:
        totals["sqlite_progress_callbacks"] += 1
        return 0

    def sqlite_connect(raw: SQLiteConnector) -> None:
        original_sqlite_connect(raw)
        totals["connections"] += 1
        raw.connection.set_progress_handler(advance, growth.SQLITE_PROGRESS_QUANTUM)

    def sqlite_close(raw: SQLiteConnector) -> None:
        raw.connection.set_progress_handler(None, 0)
        original_sqlite_close(raw)

    @contextmanager
    def candidate(connector: SQLConnector) -> Iterator[growth.SQLiteProgressSample]:
        raw = (
            connector._connector
            if isinstance(connector, _MeasuredConnector)
            else connector
        )
        if not isinstance(raw, SQLiteConnector):
            raise RuntimeError("SQLite measurement encountered a different backend")
        sample = growth.SQLiteProgressSample()

        def candidate_advance() -> int:
            advance()
            return sample.advance()

        raw.connection.set_progress_handler(
            candidate_advance, growth.SQLITE_PROGRESS_QUANTUM
        )
        try:
            yield sample
        finally:
            raw.connection.set_progress_handler(advance, growth.SQLITE_PROGRESS_QUANTUM)

    with (
        patch.object(SQLiteConnector, "connect", sqlite_connect),
        patch.object(SQLiteConnector, "close", sqlite_close),
        patch.object(growth, "sample_sqlite_candidate", candidate),
    ):
        yield totals
    totals["sqlite_progress_operations_estimate"] = (
        totals["sqlite_progress_callbacks"] * growth.SQLITE_PROGRESS_QUANTUM
    )


def measured_call(
    action: Callable[[], Any], backend: str
) -> tuple[Any, dict[str, Any]]:
    with (
        native_operation(backend) as whole,
        native_candidates(backend) as native,
        terminal_authority() as authority,
    ):
        outcome, result = growth.measure(action)
    result["whole_operation_native_work"] = whole
    result["terminal_authority"] = authority
    if backend == "mariadb":
        if len(native) != len(result["candidate_probes"]):
            raise RuntimeError("native counter attribution did not match candidates")
        for probe, counters in zip(result["candidate_probes"], native, strict=True):
            probe["handler_reads"] = counters
            probe["handler_read_total"] = sum(counters.values())
    return outcome, result


def native_total(row: dict[str, Any]) -> int:
    native = row["whole_operation_native_work"]
    if native.get("handler_reads"):
        if not maria.HANDLER_COUNTERS <= native["handler_reads"].keys():
            raise ValueError("missing MariaDB Handler counter")
        values = list(native["handler_reads"].values())
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("invalid native engine work counter")
        return sum(values)
    value = native.get("sqlite_progress_operations_estimate")
    if type(value) is not int or value < 0:
        raise ValueError("missing or invalid SQLite native work")
    return value


def evaluate(operations: list[dict[str, Any]], actual_a: int) -> dict[str, Any]:
    if not operations:
        return {
            "passed": False,
            "evidence_complete": False,
            "violations": ["missing_operations"],
        }
    release = [
        row for row in operations if row["released"] and not row["advance_count"]
    ]
    violations = []
    try:
        for row in operations:
            native_total(row)
    except KeyError, TypeError, ValueError:
        return {
            "passed": False,
            "evidence_complete": False,
            "violations": ["missing_or_invalid_native_work"],
        }
    for row in release:
        if row["candidate_probes"]:
            violations.append("pure_release_repeats_eligibility")
    last = operations[-1]
    events = last.get("terminal_authority", [])
    terminal = terminal_evidence(events)
    observed = [query for event in events for query in event.get("queries", [])]
    measured = [
        {"target": probe["target"], "candidate_found": probe["candidate_found"]}
        for probe in last["candidate_probes"][-len(observed) :]
    ]
    fresh = (
        last["outcome"] == "DONE"
        and terminal["valid"]
        and bool(observed)
        and observed == measured
    )
    if not fresh:
        violations.append("terminal_fresh_authority_missing")
    if sum(row["released"] for row in operations) != actual_a:
        violations.append("release_resource_count_mismatch")
    return {
        "passed": not violations,
        "evidence_complete": fresh,
        "violations": sorted(set(violations)),
        "pure_release_calls": len(release),
        "pure_release_eligibility_probes": sum(
            len(row["candidate_probes"]) for row in release
        ),
        "pure_release_native_work": sum(native_total(row) for row in release),
        "whole_drain_native_work": sum(native_total(row) for row in operations),
        "terminal_fresh_scan": fresh and terminal["fresh_scan"],
        "terminal_fresh_authority": fresh,
    }


def compare_native(
    baseline: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any]:
    keys = ("measurement_schema", "backend", "unique_filenames", "actual_A", "B")
    if (
        any(baseline.get(key) != candidate.get(key) for key in keys)
        or baseline.get("measurement_schema") != MEASUREMENT_SCHEMA
        or baseline["before"]["N"] != candidate["before"]["N"]
    ):
        raise ValueError(
            "baseline and candidate do not share measurement schema and actual N/A/B"
        )
    ratios = {}
    for name in ("pure_release", "whole_drain"):
        key = name + "_native_work"
        old = baseline["cost_result"][key]
        new = candidate["cost_result"][key]
        if type(old) is not int or type(new) is not int or old < 0 or new < 0:
            raise ValueError("missing or invalid complete native measurement")
        ratios[name] = new / old if old else (0.0 if not new else float("inf"))
    return {
        "budget": NATIVE_BUDGET,
        "ratios": ratios,
        "passed": all(
            ratios[name] <= NATIVE_BUDGET[name + "_ratio_max"] for name in ratios
        ),
        "notes": "The fixed budget predates the paired candidate measurements. Whole drain includes cleanup shards whose UUID placement varies between independent public fixtures. SQLite samples approximate VM operations; MariaDB counts all eight Handler read families. Wall time is observational and includes instrumentation.",
    }


def run_case(
    config: CoreConfig,
    *,
    galleries: int,
    backlog: int,
    capacity: int,
    unique_filenames: bool = True,
) -> dict[str, Any]:
    if not 1 <= galleries <= 128 or not 0 <= backlog <= 2 * galleries:
        raise ValueError("galleries must be 1..128 and backlog 0..2*galleries")
    if capacity not in (1, 8, 16):
        raise ValueError("experimental capacity must be 1, 8 or 16")
    setup_started = time.perf_counter()
    library = seed(config, galleries, unique_filenames=unique_filenames)
    expected_view = catalog_view(config)
    expected_library = library_view(library)
    # Normalize database-only cleanup before varying A. Both revisions use the
    # same no-adapter fixed-point path, so setup ordering cannot shift database
    # batches into only one measured whole-drain window.
    with closing(VNextIngestFacade(config, clock=takeover_clock())) as facade:
        for _ in range(MAX_DRAIN_CALLS):
            state = facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
            if state is VNextCurrentOnlyMaintenanceOutcome.BLOCKED:
                break
            if state is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                raise RuntimeError("expected an orphan-blocked database fixed point")
        else:
            raise RuntimeError("database-only setup failed to settle")
    pool = 2 * galleries
    released_before = len(library.release_calls)
    with (
        closing(VNextIngestFacade(config, clock=takeover_clock())) as facade,
        patch.object(maintenance, "_CURRENT_ONLY_ARTIFACT_RELEASE_PAGE_LIMIT", 1),
    ):
        for _ in range(MAX_DRAIN_CALLS):
            if len(library.release_calls) - released_before == pool - backlog:
                break
            outcome = facade.drain_current_only_maintenance(
                LEASE_MICROSECONDS,
                artifact_release_adapters={library.adapter_id: library},
            )
            if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                raise RuntimeError(
                    "fixture could not trim the requested orphan backlog"
                )
        else:
            raise RuntimeError(
                "backlog preparation exceeded its bounded attempt budget"
            )
    before = dimensions(config)
    actual_a = before["orphan_resources"]
    if actual_a != backlog:
        raise RuntimeError(
            f"native orphan count differs: expected {backlog}, actual {actual_a}"
        )
    if config.database.sql_type == "mariadb":
        with closing(open_connector(config)) as connector:
            raw = maria.raw_mariadb(connector)
            control = maria.counter_delta(
                maria.counters(raw, maria.HANDLER_STATUS),
                maria.counters(raw, maria.HANDLER_STATUS),
            )
            if any(control.values()):
                raise RuntimeError("status reads changed Handler work counters")
    else:
        control = None
    operations = []
    setup_seconds = time.perf_counter() - setup_started
    started = time.perf_counter()
    with (
        closing(VNextIngestFacade(config, clock=takeover_clock())) as facade,
        patch.object(
            maintenance, "_CURRENT_ONLY_ARTIFACT_RELEASE_PAGE_LIMIT", capacity
        ),
    ):
        for ordinal in range(MAX_DRAIN_CALLS):
            released = len(library.release_calls)
            outcome, record = measured_call(
                lambda: facade.drain_current_only_maintenance(
                    LEASE_MICROSECONDS,
                    artifact_release_adapters={library.adapter_id: library},
                ),
                config.database.sql_type,
            )
            record.update(
                ordinal=ordinal,
                outcome=outcome.value,
                released=len(library.release_calls) - released,
            )
            operations.append(record)
            if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
                break
            if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                raise RuntimeError("measured drain failed to progress")
        else:
            raise RuntimeError("measured drain exhausted its bounded attempt budget")
        elapsed = time.perf_counter() - started
        session = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
        if session is None:
            raise RuntimeError("cleanup DONE did not permit the next public claim")
        facade.complete_ingest(session)
    if (
        catalog_view(config) != expected_view
        or library_view(library) != expected_library
    ):
        raise RuntimeError("cleanup changed current catalog or library resources")
    if full_check(config).state != "READY":
        raise RuntimeError("final full READY audit failed")
    after = dimensions(config)
    if after["orphan_resources"]:
        raise RuntimeError("cleanup DONE retained an orphan resource")
    return {
        "measurement_schema": MEASUREMENT_SCHEMA,
        "backend": config.database.sql_type,
        "input_galleries": galleries,
        "unique_filenames": unique_filenames,
        "requested_A": backlog,
        "actual_A": actual_a,
        "B": capacity,
        "before": before,
        "after": after,
        "setup_seconds": setup_seconds,
        "drain_seconds_with_instrumentation": elapsed,
        "operations": operations,
        "cost_contract": COST_CONTRACT,
        "cost_result": evaluate(operations, actual_a),
        "correctness": {
            "READY": True,
            "current_catalog_unchanged": True,
            "current_library_unchanged": True,
            "next_claim_completed": True,
        },
        "handler_status_control_delta": control,
        "notes": "N is a measured row-count vector, A the native outstanding prepared-resource count. All rows came from public workflows with FK enabled. Setup trims a fixed 2*galleries resource pool through public releases. SQL timings and native work overlap wall; MariaDB status-call overhead is included in wall. These are neutral in-memory bytes, not CBZ/filesystem or production timing.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    parser.add_argument("--galleries", type=int, required=True)
    parser.add_argument("--backlog", type=int, required=True)
    parser.add_argument("--capacity", type=int, choices=(1, 8, 16), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--baseline",
        type=Path,
        help="paired baseline JSON for fixed native-work budgets",
    )
    parser.add_argument(
        "--shared-filenames",
        action="store_true",
        help="calibration only: retain the earlier shared file-name fixture",
    )
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="h2hdb-release-probe-") as directory:
        with growth.database(args.backend, Path(directory)) as config:
            result = run_case(
                config,
                galleries=args.galleries,
                backlog=args.backlog,
                capacity=args.capacity,
                unique_filenames=not args.shared_filenames,
            )
    if args.baseline:
        result["native_comparison"] = compare_native(
            json.loads(args.baseline.read_text()), result
        )
    result["tool_checkout_provenance"] = growth.source_provenance()
    result["probe_sha256"] = sha256(Path(__file__).read_bytes()).hexdigest()
    result["source_root"] = str(SOURCE_ROOT)
    metadata = SOURCE_ROOT / "probe-source.json"
    result["source_snapshot_metadata"] = (
        json.loads(metadata.read_text()) if metadata.is_file() else None
    )
    result["loaded_sources"] = {
        module.__name__: {
            "path": str(module.__file__),
            "sha256": sha256(Path(str(module.__file__)).read_bytes()).hexdigest(),
        }
        for module in (
            *(
                module
                for name, module in sorted(sys.modules.items())
                if name == "h2hdb._cleanup" or name.startswith("h2hdb._cleanup.")
            ),
            sys.modules["h2hdb.vnext_ingest_facade"],
            maintenance,
            artifact_release,
            gate,
        )
    }
    if any(
        not Path(value["path"]).is_relative_to(SOURCE_ROOT / "src")
        for value in result["loaded_sources"].values()
    ):
        raise RuntimeError("measured source does not match requested source root")
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "backend": args.backend,
                "A": result["actual_A"],
                "B": args.capacity,
                "cost_result": result["cost_result"],
                "drain_seconds": result["drain_seconds_with_instrumentation"],
            }
        )
    )
    if not result["cost_result"]["evidence_complete"]:
        return 2
    return (
        0
        if (
            result["cost_result"]["passed"]
            and result.get("native_comparison", {"passed": True})["passed"]
        )
        else 1
    )


if __name__ == "__main__":
    try:
        code = main()
    except Exception as error:
        # Public synthetic fixtures contain no secrets, but never print a driver
        # exception that could grow configuration details in future changes.
        print(
            json.dumps({"status": "incomplete", "error_type": type(error).__name__}),
            file=sys.stderr,
        )
        code = 2
    raise SystemExit(code)
