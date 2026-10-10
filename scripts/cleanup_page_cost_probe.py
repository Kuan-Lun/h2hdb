"""Measure bounded canonical-page deletion on disposable generated databases.

This manual phase experiment retains 10k/100k unrelated pages and removes
63/64/65/4096 pages with a repeating 1/8/64-pages-per-root distribution. Foreign
keys remain enabled. Each production phase call commits at most 256 logical
keys; the exact cursor/row-key trace and independent family counts must agree
with the scalar negative control. It does not claim public lifecycle, READY,
image, production-NAS or complete-maintenance timing evidence.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from ingest_growth_hash_probe import databases  # noqa: E402 - checkout-only fixture.

from h2hdb._cleanup import model as cleanup_model  # noqa: E402 - checkout.
from h2hdb._cleanup import registry as cleanup_registry  # noqa: E402 - checkout.
from h2hdb._cleanup import static as cleanup_static  # noqa: E402 - checkout.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout.
from h2hdb.sql_performance import (  # noqa: E402 - checkout.
    instrument_connector,
    measure_sql,
)
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402 - checkout.
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - checkout.

TARGET_START = b"\x81" + bytes(31)
FAMILIES = (
    ("catalog_canonical_value_allocation_allocated_ats", "value_sha256"),
    ("catalog_canonical_value_allocation_digest_domains", "value_sha256"),
    ("catalog_canonical_value_allocation_byte_counts", "value_sha256"),
    ("catalog_canonical_value_allocation_seals", "value_sha256"),
    ("catalog_canonical_value_page_anchors", "page_sha256"),
    ("catalog_canonical_value_page_payloads", "page_sha256"),
    ("catalog_canonical_value_page_subtree_item_counts", "page_sha256"),
    ("catalog_canonical_value_page_coordinates", "value_sha256"),
)


@dataclass
class Recorder:
    calls: int = 0
    seconds: float = 0.0

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        _query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.calls += 1
            self.seconds += elapsed


def _insert(
    database: SQLConnector,
    table: str,
    columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
) -> None:
    statement = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join('%s' for _ in columns)})"
    )
    for start in range(0, len(rows), 256):
        database.execute_many(statement, rows[start : start + 256])


def seed_pages(
    database: SQLConnector, retained: int, pending: int
) -> tuple[tuple[bytes], ...]:
    """Seed real FK-valid families at the input boundary of CV_PAGE.

    Earlier CV_IDENTITY/CV_PARENT_DESCRIPTOR phases have already removed the
    target identities/seals/parents. Payload bytes are synthetic; this fixture
    measures the physical family, not canonical decoding or full READY validity.
    """
    if not 0 <= retained <= 1_000_000 or not 1 <= pending <= 4096:
        raise ValueError("invalid bounded canonical-page fixture size")
    roots = [b"\x02" + index.to_bytes(31, "big") for index in range(retained)]
    coordinates = [
        (root, 0, 0, b"\x03" + index.to_bytes(31, "big"))
        for index, root in enumerate(roots)
    ]
    targets: list[bytes] = []
    remaining = pending
    while remaining:
        ordinal = len(targets)
        root = b"\x81" + ordinal.to_bytes(31, "big")
        targets.append(root)
        count = min((1, 8, 64)[ordinal % 3], remaining)
        coordinates.extend(
            (
                root,
                0,
                position,
                b"\x82" + (pending - remaining + position).to_bytes(31, "big"),
            )
            for position in range(count)
        )
        remaining -= count
    roots.extend(targets)
    with database.transaction():
        _insert(
            database,
            "catalog_canonical_value_allocation_anchors",
            ("value_sha256",),
            [(root,) for root in roots],
        )
        _insert(
            database,
            "catalog_canonical_value_allocation_allocated_ats",
            ("value_sha256", "allocated_at"),
            [(root, 101 if root[0] == 2 else 1) for root in roots],
        )
        _insert(
            database,
            "catalog_canonical_value_allocation_digest_domains",
            ("value_sha256", "digest_domain"),
            [(root, b"source_title_utf8_v1") for root in roots],
        )
        _insert(
            database,
            "catalog_canonical_value_allocation_byte_counts",
            ("value_sha256", "byte_count"),
            [(root, 1) for root in roots],
        )
        _insert(
            database,
            "catalog_canonical_value_allocation_seals",
            ("value_sha256",),
            [(root,) for root in roots],
        )
        _insert(
            database,
            "catalog_canonical_value_page_anchors",
            ("page_sha256",),
            [(row[3],) for row in coordinates],
        )
        _insert(
            database,
            "catalog_canonical_value_page_payloads",
            ("page_sha256", "page_bytes"),
            [(row[3], b"fixture") for row in coordinates],
        )
        _insert(
            database,
            "catalog_canonical_value_page_subtree_item_counts",
            ("page_sha256", "subtree_item_count"),
            [(row[3], 1) for row in coordinates],
        )
        _insert(
            database,
            "catalog_canonical_value_page_coordinates",
            ("value_sha256", "level", "page_position", "page_sha256"),
            coordinates,
        )
    return tuple((root,) for root in targets)


def call_budget(pages: int, roots: int) -> int:
    """Independent cost envelope for exact pages, metadata and phase probes.

    A <=64-key coordinate page needs one locking SELECT plus four DELETEs.
    Four allocation families each need a locking SELECT and DELETE per key
    page. The envelope also covers selection, cursor and terminal queries at
    <=256-key phase boundaries. Retained population does not raise this budget.
    """
    return (
        32
        + 8 * ((pages + 63) // 64)
        + 16 * ((roots + 63) // 64)
        + 4 * ((pages + 4 * roots + 255) // 256)
    )


def _snapshot(
    database: SQLConnector,
) -> tuple[dict[str, list[tuple[Any, ...]]], dict[str, int]]:
    with database.read_transaction():
        saved = {
            table: database.fetch_all(
                f"SELECT * FROM {table} WHERE {key} >= %s", (TARGET_START,)
            )
            for table, key in FAMILIES
        }
        retained = {
            table: int(
                database.fetch_one(
                    f"SELECT COUNT(*) FROM {table} WHERE {key} < %s", (TARGET_START,)
                )[0]
            )
            for table, key in FAMILIES
        }
    return saved, retained


def _restore(database: SQLConnector, saved: dict[str, list[tuple[Any, ...]]]) -> None:
    with database.transaction():
        for table, _ in FAMILIES:
            rows = saved[table]
            if rows:
                query = (
                    f"INSERT INTO {table} VALUES ({', '.join('%s' for _ in rows[0])})"
                )
                for start in range(0, len(rows), 256):
                    database.execute_many(query, rows[start : start + 256])


def measure_phase(
    raw: SQLConnector,
    roots: tuple[tuple[bytes], ...],
    *,
    scalar: bool,
) -> dict[str, Any]:
    """Run the actual phase in fresh bounded transactions, excluding reseeding."""
    plan = cleanup_registry._STATIC_PLANS[
        cleanup_model.CleanupTargetKind.CANONICAL_VALUE
    ]
    if scalar:
        # Deliberately degraded execution is confined to this dev-only control;
        # production has no runtime switch or legacy fallback.
        phases = dict(plan.phases)
        phases["CV_PAGE"] = tuple(
            replace(spec, batch_exact_primary_keys=False, batch_delete_keys=None)
            for spec in phases["CV_PAGE"]
        )
        plan = replace(plan, phases=phases)
    saved, retained = _snapshot(raw)
    cycle = cleanup_model.CleanupCycle(
        cleanup_model._cleanup_id(plan.kind, 129, 1),
        plan.kind,
        129,
        cleanup_model._target_key(plan.kind, 129),
        1,
        100,
        256,
        0,
    )
    recorder = Recorder()
    outputs: list[tuple[str, list[str]]] = []
    started = time.perf_counter()
    with measure_sql(recorder, observe_nested=True):
        database = instrument_connector(raw)
        cursor = b""
        for _ in range(32):
            with database.transaction():
                operation = cleanup_model._CleanupOperation(
                    VNextUnitOfWork(
                        database,
                        backend="sqlite"
                        if isinstance(raw, SQLiteConnector)
                        else "mariadb",
                    ),
                    cycle,
                    None,
                    False,
                    roots,
                )
                result = cleanup_static._run_static_phase(
                    operation, cursor, plan, "CV_PAGE"
                )
            if len(result.row_keys) > 256:
                raise AssertionError(
                    "canonical-page transaction exceeded its key bound"
                )
            outputs.append(
                (result.next_cursor.hex(), [key.hex() for key in result.row_keys])
            )
            cursor = result.next_cursor
            if not result.row_keys:
                break
        else:
            raise RuntimeError("canonical-page phase exceeded its fixture budget")
        elapsed = time.perf_counter() - started
    with raw.read_transaction():
        for table, key in FAMILIES:
            if raw.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE {key} >= %s", (TARGET_START,)
            ) != (0,):
                raise AssertionError(f"target family remains: {table}")
            if raw.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE {key} < %s", (TARGET_START,)
            ) != (retained[table],):
                raise AssertionError(f"retained family changed: {table}")
    _restore(raw, saved)
    return {
        "variant": "scalar" if scalar else "production",
        "seconds": elapsed,
        "sql_seconds": recorder.seconds,
        "sql_calls": recorder.calls,
        "transactions": len(outputs),
        "trace": outputs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    shapes = ((1000, 1), (10_000, 63), (10_000, 64), (10_000, 65), (100_000, 4096))
    report: dict[str, Any] = {
        "backend": args.backend,
        "scope": __doc__,
        "cases": [],
        "status": "running",
    }
    with databases(args.backend, len(shapes)) as connections:
        for database, (retained, pending) in zip(connections, shapes, strict=True):
            started = time.perf_counter()
            roots = seed_pages(database, retained, pending)
            seed_seconds = time.perf_counter() - started
            samples = []
            for repetition in range(3):
                for scalar in (True, False) if repetition % 2 == 0 else (False, True):
                    samples.append(measure_phase(database, roots, scalar=scalar))
            trace = samples[0]["trace"]
            if any(sample.pop("trace") != trace for sample in samples):
                raise AssertionError("scalar and production phase traces differ")
            stats = {
                variant: {
                    "seconds": statistics.median(
                        sample["seconds"]
                        for sample in samples
                        if sample["variant"] == variant
                    ),
                    "calls": sorted(
                        {
                            sample["sql_calls"]
                            for sample in samples
                            if sample["variant"] == variant
                        }
                    ),
                }
                for variant in ("scalar", "production")
            }
            budget = call_budget(pending, len(roots))
            passed = (
                max(stats["production"]["calls"]) <= budget
                and stats["production"]["seconds"]
                <= stats["scalar"]["seconds"] * 0.8 + 0.005
            )
            control_required = pending >= 63
            control_rejected = min(stats["scalar"]["calls"]) > budget
            case = {
                "retained_pages": retained,
                "pending_pages": pending,
                "pending_roots": len(roots),
                "root_page_distribution": "repeating 1/8/64; final root truncated",
                "seed_seconds": seed_seconds,
                "sql_call_budget": budget,
                "cost_target_met": passed,
                "scalar_control_required": control_required,
                "scalar_control_rejected": control_rejected,
                "exact_trace_and_family_oracle": True,
                "stats": stats,
                "samples": samples,
            }
            report["cases"].append(case)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                json.dumps(
                    {key: value for key, value in case.items() if key != "samples"}
                ),
                flush=True,
            )
    report["status"] = (
        "passed"
        if all(
            case["cost_target_met"]
            and (not case["scalar_control_required"] or case["scalar_control_rejected"])
            for case in report["cases"]
        )
        else "violated"
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
