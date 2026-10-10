"""Bounded dictionary selection evidence on disposable generated-schema databases.

The large fixture preserves generated tables, indexes, and native constraints.
Its canonical payload graph is synthetic query-shape data, not a READY catalog.
Measurements distinguish selector work from actual bounded dictionary phases;
no result certifies deployed storage performance or public cleanup completion.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from ingest_growth_cleanup_probe import database  # noqa: E402 - checkout-owned fixture.
from ingest_maintenance_mariadb import (  # noqa: E402 - checkout-owned engine diagnostics.
    HANDLER_STATUS,
    counter_delta,
    counters,
)
from vnext_fault_harness import open_connector  # noqa: E402 - native fixture authority.
from vnext_pipeline import (  # noqa: E402 - generated schema admission.
    initialize_database,
)

from h2hdb import CoreConfig  # noqa: E402 - explicit checkout source.
from h2hdb._cleanup import keys as cleanup_keys  # noqa: E402 - checkout source.
from h2hdb._cleanup import model as cleanup_model  # noqa: E402 - checkout source.
from h2hdb._cleanup import plan as cleanup_plan  # noqa: E402 - checkout source.
from h2hdb._cleanup import registry as cleanup_registry  # noqa: E402 - checkout source.
from h2hdb._cleanup import static as cleanup_static  # noqa: E402 - checkout source.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402
from h2hdb.sql_connector import SQLConnector  # noqa: E402
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402

Scalar = bytes | int | str
Row = tuple[Scalar, ...]
PLAN = cleanup_registry._STATIC_PLANS[cleanup_model.CleanupTargetKind.CANONICAL_VALUE]
DICTIONARIES = PLAN.phases["CV_DICTIONARY"][1:3]
CONTRACT: dict[str, Any] = {
    "retained_dictionary_rows": [32_768, 131_072],
    "pending_dictionary_rows_each": 512,
    "output_reuse": "90% share sixteen outputs; remaining outputs unique",
    "frozen_roots": [1, 16, 256],
    "selector_limit": 64,
    "empty_work_budget": {
        "sqlite": "4000 + 1200 * roots VM instructions",
        "mariadb": "256 + 80 * roots Handler reads",
    },
    "hit_work_budget": {
        "sqlite": "20000 + 4000 * (roots + returned rows) VM instructions",
        "mariadb": "1024 + 120 * (roots + returned rows) Handler reads",
    },
    "phase_work_budget": {
        "sqlite": "candidate <= 1.15 * baseline + 5000 VM instructions",
        "mariadb": "candidate <= 0.75 * baseline + 10000 Handler reads",
    },
    "phase_wall_budget": "candidate median <= 1.15 * baseline median + 0.050 s",
    "repetitions": 3,
    "phase_baseline": (
        "original OR selector with the same corrected duplicate-page transaction "
        "continuation; the unrepaired historical loop cannot finish this shared-root shape"
    ),
    "negative_control": (
        "semantics-preserving substr reference disables reverse indexes and must "
        "exceed the original empty-root budget on each engine"
    ),
    "scope": "fixed synthetic query shapes; native constraints on; no READY or deployment claim",
}


@dataclass(frozen=True)
class Corpus:
    choices: tuple[Row, ...]
    sorts: tuple[Row, ...]
    pending_rows: int


def target(index: int) -> bytes:
    return b"\xee" + index.to_bytes(31, "big")


def _digest(group: int, index: int) -> bytes:
    return bytes((group,)) + sha256(str(index).encode("ascii")).digest()[1:]


def backend_of(connector: SQLConnector) -> str:
    if isinstance(connector, SQLiteConnector):
        return "sqlite"
    if isinstance(connector, MariaDBConnector):
        return "mariadb"
    raise TypeError("dictionary probe requires one native synthetic database")


def _insert(connector: SQLConnector, table: str, columns: str, rows: list[Row]) -> None:
    if not rows:
        return
    statement = (
        "INSERT OR IGNORE" if backend_of(connector) == "sqlite" else "INSERT IGNORE"
    )
    placeholders = ", ".join("%s" for _ in rows[0])
    query = f"{statement} INTO {table} ({columns}) VALUES ({placeholders})"
    for start in range(0, len(rows), 2048):
        connector.execute_many(query, rows[start : start + 2048])


def seed(
    connector: SQLConnector,
    retained_rows: int,
    *,
    pending_rows: int = 512,
    dual_reference_sorts: bool = False,
) -> Corpus:
    """Seed only a disposable fixture; do not disable any native constraint."""
    if retained_rows < 0 or not 16 <= pending_rows <= 512:
        raise ValueError("dictionary fixture cardinalities are outside the contract")
    values: set[bytes] = set()
    choices: list[Row] = []
    sorts: list[Row] = []
    for index in range(retained_rows):
        source = _digest(0x11, index)
        shared = index % 16 if index % 10 else index + 16
        title, sort = _digest(0x22, shared), _digest(0x33, shared)
        values.update((source, title, sort))
        choices.append((1, source, f"gallery-{index}".encode("ascii"), title))
        sorts.append((1, source, sort))
    for index in range(pending_rows):
        source = target(index % 16)
        title = source if index % 3 == 0 else target((index + 1) % 16)
        values.update((source, title))
        # Mixed binary prefixes exercise SQL/Python ordering rather than only ASCII.
        name = bytes((index % 256,)) + f"target-{index}".encode("ascii")
        choices.append((1, source, name, title))
        source, sort = (
            (target(index % 16), target((index + 1) % 16))
            if dual_reference_sorts
            else (_digest(0x44, index), target(index % 16))
        )
        values.update((source, sort))
        sorts.append((index // 16 + 1 if dual_reference_sorts else 1, source, sort))
    values.update(target(index) for index in range(512))
    ordered = sorted(values)
    connector.commit()
    with connector.transaction():
        _insert(
            connector,
            "catalog_title_sort_policy",
            "title_sort_policy_id,title_sort_algorithm_version,unicode_data_version",
            [
                (policy, policy, b"16.0.0")
                for policy in range(
                    1, (pending_rows + 15) // 16 + 1 if dual_reference_sorts else 2
                )
            ],
        )
        _insert(
            connector,
            "catalog_display_title_policies",
            "display_title_policy_id,display_title_algorithm_version,title_sort_policy_id",
            [(1, 1, 1)],
        )
        domain_row = connector.fetch_one(
            "SELECT digest_domain FROM catalog_canonical_digest_policies ORDER BY digest_domain LIMIT 1"
        )
        if not domain_row or not isinstance(domain_row[0], bytes):
            raise RuntimeError("generated canonical digest registry is missing")
        domain = domain_row[0]
        families: tuple[tuple[str, str, list[Row]], ...] = (
            (
                "catalog_canonical_value_allocation_anchors",
                "value_sha256",
                [(value,) for value in ordered],
            ),
            (
                "catalog_canonical_value_allocation_digest_domains",
                "value_sha256,digest_domain",
                [(value, domain) for value in ordered],
            ),
            (
                "catalog_canonical_value_allocation_byte_counts",
                "value_sha256,byte_count",
                [(value, 1) for value in ordered],
            ),
            (
                "catalog_canonical_value_allocation_allocated_ats",
                "value_sha256,allocated_at",
                [(value, 0) for value in ordered],
            ),
            (
                "catalog_canonical_value_allocation_seals",
                "value_sha256",
                [(value,) for value in ordered],
            ),
            (
                "catalog_canonical_value_page_anchors",
                "page_sha256",
                [(value,) for value in ordered],
            ),
            (
                "catalog_canonical_value_page_payloads",
                "page_sha256,page_bytes",
                [(value, value) for value in ordered],
            ),
            (
                "catalog_canonical_value_page_coordinates",
                "value_sha256,level,page_position,page_sha256",
                [(value, 0, 0, value) for value in ordered],
            ),
            (
                "catalog_canonical_value_page_subtree_item_counts",
                "page_sha256,subtree_item_count",
                [(value, 1) for value in ordered],
            ),
            (
                "catalog_canonical_value_page_seals",
                "page_sha256",
                [(value,) for value in ordered],
            ),
            (
                "catalog_canonical_value_identities",
                "value_sha256,root_page_sha256",
                [(value, value) for value in ordered],
            ),
        )
        for table, columns, rows in families:
            _insert(connector, table, columns, rows)
        _insert(
            connector,
            "catalog_display_title_choices",
            "display_title_policy_id,source_title_sha256,source_gallery_name,title_sha256",
            choices,
        )
        _insert(
            connector,
            "catalog_title_sorts",
            "title_sort_policy_id,title_sha256,sort_title_sha256",
            sorts,
        )
    return Corpus(tuple(choices), tuple(sorts), pending_rows)


def operation(
    connector: SQLConnector, roots: tuple[bytes, ...], *, limit: int = 64
) -> cleanup_model._CleanupOperation:
    kind = cleanup_model.CleanupTargetKind.CANONICAL_VALUE
    shard = roots[0][0] if roots else 238
    cycle = cleanup_model.CleanupCycle(
        cleanup_model._cleanup_id(kind, shard, 1),
        kind,
        shard,
        cleanup_model._target_key(kind, shard),
        1,
        0,
        limit,
        0,
    )
    return cleanup_model._CleanupOperation(
        VNextUnitOfWork(connector, backend=backend_of(connector)),
        cycle,
        None,
        False,
        tuple((root,) for root in roots),
    )


def select(
    connector: SQLConnector,
    spec: cleanup_plan._StaticDeleteSpec,
    roots: tuple[bytes, ...],
    *,
    after: Row | None = None,
    baseline: bool = False,
    limit: int = 64,
) -> list[tuple[object, ...]]:
    op = operation(connector, roots, limit=limit)
    return cleanup_static._select_static_candidates(
        op,
        plan=PLAN,
        spec=replace(spec, canonical_dictionary_columns=None) if baseline else spec,
        after=after,
        eligibility=None,
        policy=(),
        shard=cleanup_keys._static_shard_parameters(PLAN, op.cycle),
        remaining=limit,
    )


def expected_rows(rows: tuple[Row, ...], roots: tuple[bytes, ...]) -> list[Row]:
    return sorted(
        {
            (root, *row[:-1])
            for row in rows
            for root in roots
            if root in (row[1], row[-1])
        }
    )


def native_measure[T](
    connector: SQLConnector, execute: Callable[[], T]
) -> tuple[T, int]:
    if isinstance(connector, SQLiteConnector):
        steps = 0

        def progress() -> int:
            nonlocal steps
            steps += 1
            return 0

        connector.connection.set_progress_handler(progress, 1)
        try:
            result = execute()
        finally:
            connector.connection.set_progress_handler(None, 0)
        return result, steps
    if not isinstance(connector, MariaDBConnector):
        raise TypeError("dictionary measurement requires a native connector")
    before = counters(connector, HANDLER_STATUS)
    result = execute()
    return result, sum(
        counter_delta(before, counters(connector, HANDLER_STATUS)).values()
    )


def selector_budget(backend: str, *, roots: int, rows: int, empty_fixture: bool) -> int:
    if empty_fixture:
        return 4000 + 1200 * roots if backend == "sqlite" else 256 + 80 * roots
    return (
        20_000 + 4000 * (roots + rows)
        if backend == "sqlite"
        else 1024 + 120 * (roots + rows)
    )


def selector_diagnostics(
    connector: SQLConnector,
    spec: cleanup_plan._StaticDeleteSpec,
    roots: tuple[bytes, ...],
    *,
    after: Row | None,
    baseline: bool,
) -> list[dict[str, Any]]:
    """Explain actual emitted SELECTs separately from measured repetitions."""
    captured: list[tuple[str, tuple[Any, ...]]] = []
    original = connector.fetch_all

    def capture(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        captured.append((query, data))
        return original(query, data)

    with patch.object(connector, "fetch_all", side_effect=capture):
        select(connector, spec, roots, after=after, baseline=baseline)
    prefix = (
        "EXPLAIN QUERY PLAN "
        if isinstance(connector, SQLiteConnector)
        else "ANALYZE FORMAT=JSON "
    )
    return [
        {"sql": query, "plan": connector.fetch_all(prefix + query, bindings)}
        for query, bindings in captured
    ]


def negative_control(connector: SQLConnector) -> dict[str, Any]:
    """A semantics-preserving lost-index control must fail the same budget."""
    spec = DICTIONARIES[0]
    source = spec.source
    for column in ("source_title_sha256", "title_sha256"):
        source = source.replace(f"c.{column}", f"substr(c.{column}, 1)")
    degraded = replace(spec, source=source, canonical_dictionary_columns=None)
    rows, work = native_measure(
        connector,
        lambda: select(connector, degraded, (target(300),), baseline=True),
    )
    if rows:
        raise AssertionError("lost-index control changed empty-query semantics")
    bound = selector_budget(backend_of(connector), roots=1, rows=0, empty_fixture=True)
    return {"native_work": work, "fixed_budget": bound, "rejected": work > bound}


def compare_selectors(
    connector: SQLConnector, corpus: Corpus, *, diagnostics: bool = False
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for spec, fixture_rows in zip(
        DICTIONARIES, (corpus.choices, corpus.sorts), strict=True
    ):
        for name, roots in (
            ("empty-1", (target(300),)),
            ("empty-16", tuple(target(i) for i in range(300, 316))),
            ("empty-256", tuple(target(i) for i in range(256, 512))),
            ("hits-16", tuple(target(i) for i in range(16))),
        ):
            expected = expected_rows(fixture_rows, roots)
            # Complete paged sets, independent of the measured first/middle/last cuts.
            for baseline in (True, False):
                gathered: list[tuple[object, ...]] = []
                after = None
                for _ in range(64):
                    page = select(
                        connector, spec, roots, after=after, baseline=baseline
                    )
                    gathered.extend(page)
                    if len(page) < 64:
                        break
                    after = cleanup_keys._static_values(page[-1])
                else:
                    raise RuntimeError(
                        "dictionary full-set oracle exceeded its page budget"
                    )
                if gathered != expected:
                    raise AssertionError("dictionary complete ordered row set changed")
            cuts: list[tuple[str, Row | None]] = [("initial", None)]
            if expected:
                cuts.extend(
                    (
                        ("continuation-first", expected[min(63, len(expected) - 1)]),
                        ("continuation-middle", expected[len(expected) // 2]),
                        ("continuation-last", expected[-1]),
                    )
                )
            for cut, after in cuts:
                wanted = [row for row in expected if after is None or row > after][:64]
                variants: dict[str, list[dict[str, int | float]]] = {
                    "baseline": [],
                    "candidate": [],
                }
                for repetition in range(3):
                    for variant in (
                        tuple(variants) if repetition % 2 else tuple(reversed(variants))
                    ):

                        def execute(variant: str = variant) -> list[tuple[object, ...]]:
                            return select(
                                connector,
                                spec,
                                roots,
                                after=after,
                                baseline=variant == "baseline",
                            )

                        rows, work = native_measure(connector, execute)
                        if rows != wanted:
                            raise AssertionError(
                                "dictionary cursor continuation changed"
                            )
                        started = time.monotonic()
                        if execute() != wanted:
                            raise AssertionError("timed dictionary replay changed")
                        variants[variant].append(
                            {"work": work, "seconds": time.monotonic() - started}
                        )
                bound = selector_budget(
                    backend_of(connector),
                    roots=len(roots),
                    rows=len(wanted),
                    empty_fixture=not expected,
                )
                records.append(
                    {
                        "table": spec.table,
                        "shape": name,
                        "cut": cut,
                        "rows": len(wanted),
                        "budget": bound,
                        "candidate_budget_met": median(
                            item["work"] for item in variants["candidate"]
                        )
                        <= bound,
                        "variants": variants,
                    }
                )
                if (
                    diagnostics
                    and name in {"empty-256", "hits-16"}
                    and cut
                    in {
                        "initial",
                        "continuation-middle",
                    }
                ):
                    records[-1]["diagnostics"] = {
                        variant: selector_diagnostics(
                            connector,
                            spec,
                            roots,
                            after=after,
                            baseline=variant == "baseline",
                        )
                        for variant in variants
                    }
    return records


def _restore(connector: SQLConnector, corpus: Corpus) -> None:
    connector.commit()
    with connector.transaction():
        _insert(
            connector,
            "catalog_display_title_choices",
            "display_title_policy_id,source_title_sha256,source_gallery_name,title_sha256",
            list(corpus.choices[-corpus.pending_rows :]),
        )
        _insert(
            connector,
            "catalog_title_sorts",
            "title_sort_policy_id,title_sha256,sort_title_sha256",
            list(corpus.sorts[-corpus.pending_rows :]),
        )


def run_phase(
    connector: SQLConnector, *, baseline: bool, batch_dictionary: bool = False
) -> tuple[tuple[bytes, tuple[bytes, ...]], ...]:
    plan = (
        replace(
            PLAN,
            phases={
                **PLAN.phases,
                "CV_DICTIONARY": tuple(
                    replace(spec, canonical_dictionary_columns=None)
                    for spec in PLAN.phases["CV_DICTIONARY"]
                ),
            },
        )
        if baseline
        else PLAN
    )
    if batch_dictionary:
        plan = replace(
            plan,
            phases={
                **plan.phases,
                "CV_DICTIONARY": tuple(
                    replace(spec, batch_exact_primary_keys=True)
                    if spec.table == "catalog_title_sorts"
                    else spec
                    for spec in plan.phases["CV_DICTIONARY"]
                ),
            },
        )
    cursor = b""
    trace: list[tuple[bytes, tuple[bytes, ...]]] = []
    for _ in range(64):
        with connector.transaction():
            op = operation(connector, tuple(target(i) for i in range(16)))
            mutation = cleanup_static._run_static_phase(
                op, cursor, plan, "CV_DICTIONARY"
            )
            cursor = mutation.next_cursor
            trace.append((cursor, mutation.row_keys))
        if not mutation.row_keys:
            return tuple(trace)
    raise RuntimeError("dictionary phase exceeded its bounded transaction budget")


def compare_phases(connector: SQLConnector, corpus: Corpus) -> dict[str, Any]:
    variants: dict[str, list[dict[str, int | float]]] = {
        "baseline": [],
        "candidate": [],
    }
    expected = None
    for repetition in range(3):
        for variant in tuple(variants) if repetition % 2 else tuple(reversed(variants)):
            _restore(connector, corpus)

            def execute(
                variant: str = variant,
            ) -> tuple[tuple[bytes, tuple[bytes, ...]], ...]:
                return run_phase(connector, baseline=variant == "baseline")

            trace, work = native_measure(connector, execute)
            if expected is None:
                expected = trace
            if trace != expected:
                raise AssertionError("dictionary durable cursor/deletion trace changed")
            _restore(connector, corpus)
            started = time.monotonic()
            if execute() != expected:
                raise AssertionError("timed dictionary phase changed")
            variants[variant].append(
                {
                    "work": work,
                    "seconds": time.monotonic() - started,
                    "batches": len(trace),
                    "deleted": sum(len(keys) for _, keys in trace),
                }
            )
    before, after = (
        median(item["work"] for item in variants[name])
        for name in ("baseline", "candidate")
    )
    budget = before * (1.15 if backend_of(connector) == "sqlite" else 0.75) + (
        5000 if backend_of(connector) == "sqlite" else 10_000
    )
    initial_seconds, final_seconds = (
        median(item["seconds"] for item in variants[name])
        for name in ("baseline", "candidate")
    )
    return {
        "variants": variants,
        "native_budget": budget,
        "native_budget_met": after <= budget,
        "wall_budget_met": final_seconds <= initial_seconds * 1.15 + 0.050,
        "exact_cursor_and_deletion_trace_equal": True,
    }


def run_case(config: CoreConfig, *, retained_rows: int) -> dict[str, Any]:
    runtime_paths = tuple(sorted((ROOT / "src/h2hdb/_cleanup").rglob("*.py")))
    runtime_hashes = {
        str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
        for path in runtime_paths
    }
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        corpus = seed(connector, retained_rows)
        if isinstance(connector, SQLiteConnector):
            connector.execute("ANALYZE")
        else:
            for table in (PLAN.root_table, *(spec.table for spec in DICTIONARIES)):
                connector.fetch_all(f"ANALYZE TABLE {table}")
        connector.commit()
        with connector.read_transaction():
            selectors = compare_selectors(connector, corpus, diagnostics=True)
            control = negative_control(connector)
        phases = compare_phases(connector, corpus)
    if runtime_hashes != {
        str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
        for path in sorted((ROOT / "src/h2hdb/_cleanup").rglob("*.py"))
    }:
        raise RuntimeError("cleanup runtime changed while the experiment was running")
    return {
        "backend": config.database.sql_type,
        "retained_rows_per_dictionary": retained_rows,
        "pending_rows_per_dictionary": corpus.pending_rows,
        "contract": CONTRACT,
        "selectors": selectors,
        "phases": phases,
        "negative_control": control,
        "runtime_sha256": runtime_hashes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    parser.add_argument(
        "--retained-rows", type=int, choices=(32_768, 131_072), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with (
        tempfile.TemporaryDirectory(prefix="h2hdb-dictionary-probe-") as folder,
        database(args.backend, Path(folder)) as config,
    ):
        result = run_case(config, retained_rows=args.retained_rows)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if (
        not all(row["candidate_budget_met"] for row in result["selectors"])
        or not result["phases"]["native_budget_met"]
        or not result["phases"]["wall_budget_met"]
        or not result["negative_control"]["rejected"]
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
