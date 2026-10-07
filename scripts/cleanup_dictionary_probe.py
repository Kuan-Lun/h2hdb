"""Public-workflow regression for dictionary cleanup selection engine work.

The historical and selected SQL execute at each actual cleanup read cut. Both
retain the original frozen roots, shard, cursor, and bounded result. Counts are
engine work, never a conversion from returned rows. This developer probe owns
only synthetic databases supplied by a registered fixture or explicit caller.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from functools import partial
from pathlib import Path
from statistics import median
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import ingest_maintenance_mariadb as maria  # noqa: E402 - checkout diagnostic.
import vnext_pipeline as pipeline  # noqa: E402 - public workflow fixture.
from vnext_fault_harness import open_connector  # noqa: E402 - native fixture oracle.
from vnext_pipeline import (  # noqa: E402 - public workflow fixture.
    LEASE_MICROSECONDS,
    MemoryLibrary,
    MemorySource,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import (  # noqa: E402 - explicit checkout source.
    CoreConfig,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb import vnext_cleanup_repository as cleanup  # noqa: E402
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402
from h2hdb.sql_connector import SQLConnector  # noqa: E402
from h2hdb.sql_performance import _MeasuredConnector  # noqa: E402
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402

TABLES = (
    "catalog_canonical_value_allocation_anchors",
    "catalog_display_title_choices",
    "catalog_title_sorts",
    "catalog_gallery_observations",
)
DICTIONARIES = TABLES[1:3]
MAX_DRAIN_CALLS = 4096


def native_measurement[T](
    connector: SQLConnector, execute: Callable[[], T]
) -> tuple[T, int]:
    """Count exact SQLite instructions or all MariaDB Handler read families."""
    if isinstance(connector, SQLiteConnector):
        steps = 0
        started = time.monotonic()

        def progress() -> int:
            nonlocal steps
            steps += 1
            return int(time.monotonic() - started > 30)

        connector.connection.set_progress_handler(progress, 1)
        try:
            result = execute()
        finally:
            connector.connection.set_progress_handler(None, 0)
        return result, steps
    if not isinstance(connector, MariaDBConnector):
        raise TypeError("dictionary native probe requires an actual engine connector")
    before = maria.counters(connector, maria.HANDLER_STATUS)
    result = execute()
    after = maria.counters(connector, maria.HANDLER_STATUS)
    return result, sum(maria.counter_delta(before, after).values())


def empty_budget(backend: str, roots: int) -> int:
    """Fixed before candidate measurements; independent of retained data N."""
    if roots < 1:
        raise ValueError("empty probe budget requires frozen roots")
    return 256 + 96 * roots if backend == "sqlite" else 64 + 64 * roots


def assert_query_budget(record: dict[str, Any], backend: str) -> None:
    if record["returned_rows"] == 0:
        bound = empty_budget(backend, record["frozen_roots"])
        if record["candidate_work"] > bound:
            raise AssertionError(
                f"empty dictionary selection exceeded fixed native budget: {record!r}"
            )
    else:
        allowance = 32 if backend == "sqlite" else 64
        if (
            100 * record["candidate_work"]
            > 115 * record["baseline_work"] + 100 * allowance
        ):
            raise AssertionError(
                "dictionary hit selection regressed beyond fixed budget"
            )


def assert_supported_budget(record: dict[str, Any], backend: str) -> None:
    """Budget raw absence independently of N; retain the original failed goal.

    Protected roots with children still need fresh reachability. Apply the
    predeclared hit nonregression allowance there, even when output is empty.
    ``assert_query_budget`` separately records the broader zero-result goal.
    """
    comparison = dict(record)
    if record["raw_children_present"]:
        comparison["returned_rows"] = max(1, record["returned_rows"])
    assert_query_budget(comparison, backend)


def _drain(facade: VNextIngestFacade) -> int:
    for call in range(1, MAX_DRAIN_CALLS + 1):
        if facade.drain_current_only_maintenance(LEASE_MICROSECONDS) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        ):
            return call
    raise RuntimeError("public cleanup failed to reach DONE")


def prepare_fixture(
    config: CoreConfig, *, galleries: int, replacements: int, shared_title: bool
) -> None:
    if not 1 <= replacements <= galleries:
        raise ValueError("replacement cardinality must be within the retained corpus")
    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                gid,
                title="Shared original title"
                if shared_title
                else f"Gallery {gid} original title",
            )
            for gid in range(1, galleries + 1)
        ]
    )
    library = MemoryLibrary(source)
    # The shared helper's 10,000-step guard is intended for small semantic
    # fixtures. A 513-gallery public source needs more bounded issue/commit
    # steps; this finite setup guard is not a cleanup performance allowance.
    with (
        patch.object(
            pipeline,
            "run_source",
            partial(pipeline.run_source, step_budget=max(10_000, galleries * 64)),
        ),
        VNextIngestFacade(config) as facade,
    ):
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
        _drain(facade)
        for gid in range(1, replacements + 1):
            source.put(gallery(gid, title=f"Replacement title for gallery {gid}"))
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
    if full_check(config).state != "READY":
        raise RuntimeError("public fixture did not pass full READY")


def protected_empty_matrix(config: CoreConfig) -> list[dict[str, Any]]:
    """Read retained dictionary roots; these are not admitted cleanup cycles."""
    plan = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.CANONICAL_VALUE]
    cases: list[dict[str, Any]] = []
    with closing(open_connector(config)) as connector, connector.read_transaction():
        for spec, columns in zip(
            plan.phases["CV_DICTIONARY"][1:3],
            (
                ("source_title_sha256", "title_sha256"),
                ("title_sha256", "sort_title_sha256"),
            ),
            strict=True,
        ):
            digests = connector.fetch_one(
                f"SELECT {', '.join(columns)} FROM {spec.table} ORDER BY 1, 2 LIMIT 1"
            )
            if len(digests) != 2:
                raise AssertionError("protected fixture lost its live dictionary")
            for edge, digest in zip(columns, digests, strict=True):
                shard = digest[0]
                kind = cleanup.CleanupTargetKind.CANONICAL_VALUE
                shape = cleanup.CleanupCycle(
                    cleanup._cleanup_id(kind, shard, 1),
                    kind,
                    shard,
                    cleanup._target_key(kind, shard),
                    1,
                    0,
                    128,
                    1,
                )
                predicate, roots = cleanup._frozen_root_predicate(plan, ((digest,),))
                parameters = (
                    cleanup._static_shard_parameters(plan, shape) + roots + (128,)
                )
                variants = {
                    "candidate": cleanup._static_select_sql(
                        plan, spec, exact=False, frozen_root_predicate=predicate
                    ),
                    "baseline": cleanup._static_select_sql(
                        plan,
                        replace(spec, selection_root_probe=None),
                        exact=False,
                        frozen_root_predicate=predicate,
                    ),
                }
                raw_count = connector.fetch_one(
                    f"SELECT COUNT(*) FROM {spec.source} WHERE r.value_sha256 = %s",
                    (digest,),
                )[0]
                if raw_count < 1:
                    raise AssertionError("protected empty control lacks raw children")
                measured: dict[str, list[int]] = {name: [] for name in variants}
                for repetition in range(4):
                    names = (
                        tuple(variants) if repetition % 2 else tuple(reversed(variants))
                    )
                    for name in names:
                        rows, work = native_measurement(
                            connector,
                            lambda: connector.fetch_all(variants[name], parameters),
                        )
                        if rows:
                            raise AssertionError(
                                "live dictionary became eligible for cleanup"
                            )
                        measured[name].append(work)
                record = {
                    "table": spec.table,
                    "edge": edge,
                    "frozen_roots": 1,
                    "raw_children_present": True,
                    "raw_child_count": raw_count,
                    "returned_rows": 0,
                    "candidate_work": int(median(measured["candidate"])),
                    "baseline_work": int(median(measured["baseline"])),
                    "native_repetitions": measured,
                    "candidate_first_work": measured["candidate"][0],
                    "baseline_first_work": measured["baseline"][0],
                }
                record["original_zero_result_budget_met"] = record[
                    "candidate_work"
                ] <= empty_budget(config.database.sql_type, 1)
                assert_supported_budget(record, config.database.sql_type)
                cases.append(record)
    return cases


def observe_cleanup(config: CoreConfig) -> dict[str, Any]:
    """Run real cleanup while comparing every actual dictionary SELECT."""
    backend = config.database.sql_type
    with closing(open_connector(config)) as connector:
        connector_type: type[SQLConnector] = type(connector)
        fk = connector.fetch_one(
            "PRAGMA foreign_keys"
            if backend == "sqlite"
            else "SELECT @@SESSION.foreign_key_checks"
        )
        if fk != (1,):
            raise RuntimeError("fixture foreign keys must remain enabled")
        actual_n = {
            table: int(connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
            for table in TABLES
        }
    original_select = cleanup._static_select_sql
    original_fetch = connector_type.fetch_all
    sql_shapes: dict[str, tuple[str, str, str, str, int, int, bool]] = {}
    records: list[dict[str, Any]] = []
    fanout_control: dict[str, Any] | None = None
    same_snapshot_roots: dict[str, Any] | None = None
    original_phase = cleanup._run_static_phase
    checked_connections: dict[int, SQLConnector] = {}

    def require_foreign_keys(raw: SQLConnector) -> None:
        if id(raw) in checked_connections:
            return
        query = (
            "PRAGMA foreign_keys"
            if backend == "sqlite"
            else "SELECT @@SESSION.foreign_key_checks"
        )
        if original_fetch(raw, query, ()) != [(1,)]:
            raise RuntimeError("measured runtime connection disabled foreign keys")
        checked_connections[id(raw)] = raw

    def select(
        plan: cleanup._StaticTargetPlan,
        spec: cleanup._StaticDeleteSpec,
        *,
        exact: bool,
        frozen_root_predicate: str,
        has_after: bool = False,
        eligibility: str | None = None,
    ) -> str:
        selected = original_select(
            plan,
            spec,
            exact=exact,
            frozen_root_predicate=frozen_root_predicate,
            has_after=has_after,
            eligibility=eligibility,
        )
        if not exact and spec.selection_root_probe is not None:
            historical = original_select(
                plan,
                replace(spec, selection_root_probe=None),
                exact=False,
                frozen_root_predicate=frozen_root_predicate,
                has_after=has_after,
                eligibility=eligibility,
            )
            roots = frozen_root_predicate.count("r.value_sha256 = %s")
            eligible = plan.eligibility if eligibility is None else eligibility
            child_dependent = historical.replace(
                f"WHERE ({eligible}) AND ({spec.extra_predicate})",
                f"WHERE (CASE WHEN ({spec.extra_predicate}) "
                f"THEN ({eligible}) ELSE 0 END) = 1",
            )
            shard_predicate = cleanup._static_shard_sql(plan)
            raw_query = (
                f"SELECT 1 FROM {spec.source} WHERE ({shard_predicate}) "
                f"AND ({frozen_root_predicate}) LIMIT 1"
            )
            raw_binds = shard_predicate.count("%s") + frozen_root_predicate.count("%s")
            sql_shapes[selected] = (
                historical,
                child_dependent,
                raw_query,
                spec.table,
                roots,
                raw_binds,
                has_after,
            )
        return selected

    def fetch(
        self: SQLConnector, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        nonlocal fanout_control
        shape = sql_shapes.get(query)
        if shape is None:
            return original_fetch(self, query, data)
        historical, child_dependent, raw_query, table, roots, raw_binds, has_after = (
            shape
        )
        require_foreign_keys(self)
        # Alternate order; both reads remain inside this actual transaction.
        variants = (
            ("baseline", "candidate")
            if len(records) % 2 == 0
            else ("candidate", "baseline")
        )
        observations: dict[str, list[int]] = {name: [] for name in variants}
        expected: list[tuple[Any, ...]] | None = None
        for repetition in range(4):
            names = variants if repetition % 2 == 0 else tuple(reversed(variants))
            for variant in names:
                sql = historical if variant == "baseline" else query

                def execute(sql: str = sql) -> list[tuple[Any, ...]]:
                    return original_fetch(self, sql, data)

                rows, work = native_measurement(self, execute)
                if expected is None:
                    expected = rows
                if rows != expected:
                    raise AssertionError(
                        "dictionary selector changed exact ordered results"
                    )
                observations[variant].append(work)
        if expected is None:
            raise AssertionError("dictionary selector was not measured")
        rows = expected
        baseline_work = int(median(observations["baseline"]))
        candidate_work = int(median(observations["candidate"]))
        record = {
            "table": table,
            "frozen_roots": roots,
            "has_after": has_after,
            "limit": data[-1],
            "returned_rows": len(rows),
            "baseline_work": baseline_work,
            "candidate_work": candidate_work,
            "native_repetitions": observations,
            "baseline_first_work": observations["baseline"][0],
            "candidate_first_work": observations["candidate"][0],
            "raw_children_present": bool(
                original_fetch(self, raw_query, data[:raw_binds])
            ),
            "original_zero_result_budget_met": (
                candidate_work <= empty_budget(backend, roots) if not rows else None
            ),
        }
        assert_supported_budget(record, backend)
        if fanout_control is None and table == DICTIONARIES[0] and len(rows) >= 16:
            wrong_rows, wrong_work = native_measurement(
                self, lambda: original_fetch(self, child_dependent, data)
            )
            if wrong_rows != rows:
                raise AssertionError("fanout negative control changed query semantics")
            rejected = False
            try:
                assert_query_budget({**record, "candidate_work": wrong_work}, backend)
            except AssertionError:
                rejected = True
            fanout_control = {
                "returned_rows": len(rows),
                "native_work": wrong_work,
                "rejected_by_same_hit_budget": rejected,
            }
        records.append(record)
        return rows

    def phase(*args: Any, **kwargs: Any) -> Any:
        nonlocal same_snapshot_roots
        operation, _cursor, plan, name = args
        if (
            name == "CV_DICTIONARY"
            and same_snapshot_roots is None
            and len(operation.frozen_roots) >= 3
        ):
            raw = operation.work.connector
            if isinstance(raw, _MeasuredConnector):
                raw = raw._connector
            require_foreign_keys(raw)
            counts = {
                table: int(
                    original_fetch(raw, f"SELECT COUNT(*) FROM {table}", ())[0][0]
                )
                for table in TABLES
            }
            samples: list[dict[str, Any]] = []
            for count in (1, 2, 3):
                predicate, values = cleanup._frozen_root_predicate(
                    plan, operation.frozen_roots[:count]
                )
                parameters = (
                    cleanup._static_shard_parameters(plan, operation.cycle)
                    + values
                    + (128,)
                )
                for spec in plan.phases[name]:
                    if spec.selection_root_probe is None:
                        continue
                    queries = {
                        "candidate": original_select(
                            plan, spec, exact=False, frozen_root_predicate=predicate
                        ),
                        "baseline": original_select(
                            plan,
                            replace(spec, selection_root_probe=None),
                            exact=False,
                            frozen_root_predicate=predicate,
                        ),
                    }
                    measured: dict[str, list[int]] = {name: [] for name in queries}
                    expected: list[tuple[Any, ...]] | None = None
                    for repetition in range(4):
                        names = (
                            tuple(queries)
                            if repetition % 2
                            else tuple(reversed(queries))
                        )
                        for variant in names:
                            query = queries[variant]

                            def execute(query: str = query) -> list[tuple[Any, ...]]:
                                return original_fetch(raw, query, parameters)

                            rows, work = native_measurement(raw, execute)
                            if expected is None:
                                expected = rows
                            if rows != expected:
                                raise AssertionError(
                                    "same-snapshot root subsets changed exact results"
                                )
                            measured[variant].append(work)
                    if expected is None:
                        raise AssertionError("root subset selector was not measured")
                    record = {
                        "table": spec.table,
                        "frozen_roots": count,
                        "returned_rows": len(expected),
                        "candidate_work": int(median(measured["candidate"])),
                        "baseline_work": int(median(measured["baseline"])),
                        "native_repetitions": measured,
                        "raw_children_present": bool(
                            original_fetch(
                                raw,
                                f"SELECT 1 FROM {spec.source} WHERE ({cleanup._static_shard_sql(plan)}) AND ({predicate}) LIMIT 1",
                                parameters[:-1],
                            )
                        ),
                    }
                    assert_supported_budget(record, backend)
                    samples.append(record)
            same_snapshot_roots = {
                "actual_N": counts,
                "cases": samples,
                "same_transaction_no_mutations_between_cases": True,
            }
        return original_phase(*args, **kwargs)

    with (
        patch.object(cleanup, "_static_select_sql", select),
        patch.object(connector_type, "fetch_all", fetch),
        patch.object(cleanup, "_run_static_phase", phase),
    ):
        with VNextIngestFacade(config) as facade:
            calls = _drain(facade)
    if full_check(config).state != "READY":
        raise RuntimeError("cleanup did not preserve READY")
    with VNextIngestFacade(config) as facade:
        # DONE is not cached: retry and a subsequent public claim recheck it.
        if (
            facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
            is not VNextCurrentOnlyMaintenanceOutcome.DONE
        ):
            raise RuntimeError("idle cleanup retry did not remain DONE")
        claim = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
        if claim is None:
            raise RuntimeError("cleanup did not admit the next public claim")
        facade.complete_ingest(claim)
    required = {(table, empty) for table in DICTIONARIES for empty in (False, True)}
    observed = {(item["table"], item["returned_rows"] == 0) for item in records}
    if not required <= observed:
        raise AssertionError(
            "public fixture did not exercise both dictionary hit/empty cases"
        )
    baseline = sum(item["baseline_work"] for item in records)
    candidate = sum(item["candidate_work"] for item in records)
    if candidate > baseline:
        raise AssertionError("all dictionary selections in the whole cleanup regressed")
    protected = protected_empty_matrix(config)
    first_baseline = sum(item["baseline_first_work"] for item in records)
    first_candidate = sum(item["candidate_first_work"] for item in records)
    original_failures: list[dict[str, Any]] = []
    for record in (*records, *protected):
        actual = {
            **record,
            "candidate_work": record["candidate_first_work"],
            "baseline_work": record["baseline_first_work"],
        }
        try:
            assert_query_budget(actual, backend)
        except AssertionError:
            original_failures.append(actual)
    return {
        "backend": backend,
        "status": "completed",
        "actual_N_before_cleanup": actual_n,
        "foreign_keys_enabled": True,
        "ready_before": "READY",
        "ready_after": "READY",
        "cleanup_done": True,
        "next_claim_obtained": True,
        "actual_cleanup_calls": calls,
        "actual_selection_calls": len(records),
        "measured_connections_with_foreign_keys_verified": len(checked_connections),
        "selection_native_baseline_total": baseline,
        "selection_native_candidate_total": candidate,
        "selection_native_baseline_first_total": first_baseline,
        "selection_native_candidate_first_total": first_candidate,
        "selection_first_total_nonregression_met": first_candidate <= first_baseline,
        "selection_first_total_failure": None
        if first_candidate <= first_baseline
        else {"baseline": first_baseline, "candidate": first_candidate},
        "fanout_negative_control": fanout_control,
        "same_snapshot_root_subsets": same_snapshot_roots,
        "protected_empty_cases": protected,
        "original_per_query_contract_met": not original_failures,
        "original_per_query_contract_failures": original_failures,
        "original_universal_empty_contract_achieved": all(
            item["original_zero_result_budget_met"] for item in protected
        ),
        "supported_cost_scope": "raw-child-absent short circuit and the predeclared nonregression allowance for existing children; the original universal zero-result bound remains separately reported",
        "records": records,
        "scope": "Same-cut SELECT counterfactuals during actual public cleanup; exact locks and mutations run normally. Native totals cover dictionary selection only, not all workflow SQL. Timing includes diagnostics and is not a speedup measurement.",
    }


def run_case(
    config: CoreConfig, *, galleries: int, replacements: int, shared_title: bool = False
) -> dict[str, Any]:
    prepare_fixture(
        config,
        galleries=galleries,
        replacements=replacements,
        shared_title=shared_title,
    )
    return observe_cleanup(config)
