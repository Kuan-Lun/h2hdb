"""Measure depth-zero decision key selection on disposable generated databases.

The retained production UNION is the negative control, not an alternate runtime.
Costs are source rows, query calls, SQLite VM instructions, and MariaDB handler
operations. No filesystem ingestion, READY audit, aggregate values, or NAS wall
time is measured. No existing server or database can be supplied.

Candidate wall time includes plan preparation, its first complete authenticated
key delivery, and owned-plan cleanup. The old UNION includes one complete key
delivery. Both exclude fixture setup, diagnostic handler queries and EXPLAIN;
two additional candidate traversals check correctness without adding to timing.
These finite local observations are not an end-to-end stage or NAS SLA estimate.
"""

from __future__ import annotations

import argparse
import json
import signal
import sqlite3
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from analysis_changed_hash_probe import handler_counts  # noqa: E402 - checkout tooling.
from ingest_growth_hash_probe import (  # noqa: E402 - isolated database owners.
    Query,
    databases,
    profile_mariadb,
    write_report,
)

from h2hdb import vnext_analysis_repository as analysis  # noqa: E402 - checkout source.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402 - checkout source.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout source.
from h2hdb.sql_performance import query_fingerprint  # noqa: E402 - checkout source.
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402 - checkout source.
from h2hdb.vnext_analysis_hash_keys import (  # noqa: E402 - checkout source.
    AnalysisHashKeyPlan,
    build_analysis_hash_key_plan,
)

ANALYSIS = b"a" * 16
BUILD = b"b" * 16


@dataclass(frozen=True)
class Shape:
    galleries: int
    hashes: int = 4
    rejected: int = 0
    history: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.galleries <= 2048 or not 1 <= self.hashes <= 129:
            raise ValueError("fixture galleries must be 1..2048 and hashes 1..129")
        if not 0 <= self.rejected <= self.galleries or not 0 <= self.history <= 4:
            raise ValueError("invalid rejected membership or history dimension")
        if self.galleries * self.hashes * (self.history + 1) > 100_000:
            raise ValueError("fixture exceeds 100000 occurrence rows")

    @property
    def occurrences(self) -> int:
        return (self.galleries - self.rejected) * self.hashes

    @property
    def branch_ranges(self) -> int:
        return sum(size * (self.hashes // (128 // size) + 1) for size in self.groups)

    @property
    def groups(self) -> tuple[int, ...]:
        groups: list[int] = []
        for first in range(0, self.galleries, 128):
            accepted = max(
                0, min(first + 128, self.galleries) - max(first, self.rejected)
            )
            groups.extend(min(16, accepted - start) for start in range(0, accepted, 16))
        return tuple(groups)

    @property
    def source_calls(self) -> int:
        return (
            1
            + self.galleries // 128
            + 1
            + sum(self.hashes // (128 // size) + 1 for size in self.groups)
        )

    @property
    def vm_budget(self) -> int:
        # 100 is a predeclared conservative regression allowance, not a derived
        # theoretical optimum or an opcode theorem. It covers loop work, PK
        # lookup/coroutine setup, and the <100/query counter sampling remainder.
        # On SQLite 3.50.4 EXPLAIN has 15/36 instructions for changed/member
        # queries and <=25 per occurrence branch, leaving explicit headroom.
        # Other SQLite versions must pass the same allowance; each report stores
        # its version and actual static program sizes instead of assuming them.
        # Ranges include exhausted tails and all bounded UNION branches.
        return 100 * (
            self.occurrences
            + self.galleries
            + 1
            + self.branch_ranges
            + self.source_calls
        )


def digest(value: int) -> bytes:
    return value.to_bytes(32, "big")


def seed(connector: SQLConnector, backend: str, shape: Shape) -> tuple[bytes, ...]:
    connector.execute(
        "PRAGMA foreign_keys=OFF" if backend == "sqlite" else "SET FOREIGN_KEY_CHECKS=0"
    )
    with connector.transaction():
        connector.execute_many(
            "INSERT INTO catalog_source_build_galleries (build_id, gallery_id, observation_id) VALUES (%s, %s, 1)",
            [(BUILD, gallery) for gallery in range(1, shape.galleries + 1)],
        )
        for observation in range(1, shape.history + 2):
            connector.execute_many(
                "INSERT INTO catalog_gallery_observation_validation_dispositions (gallery_id, observation_id, accepted) VALUES (%s, %s, %s)",
                [
                    (gallery, observation, int(gallery > shape.rejected))
                    for gallery in range(1, shape.galleries + 1)
                ],
            )
            for first in range(1, shape.galleries + 1, 128):
                connector.execute_many(
                    "INSERT INTO catalog_gallery_observation_file_hash_occurrences (gallery_id, observation_id, file_sha256, occurrence_count) VALUES (%s, %s, %s, 1)",
                    [
                        (
                            gallery,
                            observation,
                            digest(
                                (observation - 1) * shape.galleries * shape.hashes
                                + (gallery - 1) * shape.hashes
                                + page
                            ),
                        )
                        for gallery in range(
                            first, min(first + 128, shape.galleries + 1)
                        )
                        for page in range(1, shape.hashes + 1)
                    ],
                )
        connector.execute(
            "INSERT INTO catalog_analysis_changed_file_hashes (analysis_id, file_sha256) VALUES (%s, %s)",
            (ANALYSIS, digest(0)),
        )
    return (
        digest(0),
        *(
            digest(value)
            for value in range(
                shape.rejected * shape.hashes + 1, shape.galleries * shape.hashes + 1
            )
        ),
    )


def old_query(after: bytes | None) -> tuple[str, tuple[Any, ...]]:
    """Exact old depth-zero SQL: its after-page fingerprint is cca9a391f4977cca."""
    captured: list[tuple[str, tuple[Any, ...]]] = []

    class Capture:
        def fetch_all(
            self, sql: str, parameters: tuple[Any, ...]
        ) -> list[tuple[Any, ...]]:
            captured.append((sql, parameters))
            return []

    # Reuse the unchanged generic union builder, retaining the removed source
    # branch explicitly so this negative control cannot silently become faster.
    from h2hdb.vnext_transaction import VNextUnitOfWork

    analysis._file_hash_union_page(
        VNextUnitOfWork(Capture(), backend="sqlite"),  # type: ignore[arg-type]  # Capture only SQL text.
        [
            (
                "SELECT file_sha256 FROM catalog_analysis_changed_file_hashes WHERE analysis_id = %s",
                (ANALYSIS,),
            ),
            (
                "SELECT occurrence.file_sha256 AS file_sha256 FROM "
                + analysis._ACCEPTED_SOURCE_MEMBERS
                + " AS member "
                "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
                "ON occurrence.gallery_id = member.gallery_id AND occurrence.observation_id = member.observation_id "
                "WHERE member.build_id = %s",
                (BUILD,),
            ),
        ],
        after=after,
        limit=129,
    )
    return captured[0]


@dataclass
class Reads:
    calls: int = 0
    memberships: int = 0
    occurrences: int = 0
    changed: int = 0
    vm_steps: int = 0
    branch_ranges: int = 0
    samples: dict[str, Query] = field(default_factory=dict)


@contextmanager
def measure_reads(connector: SQLConnector, *, source: bool) -> Iterator[Reads]:
    reads = Reads()
    original = connector.fetch_all

    def fetch(sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        kind = "old_union"
        if source:
            if sql.startswith("SELECT catalog_source_build_galleries.gallery_id,"):
                kind = "memberships"
            elif sql.startswith("SELECT %s AS source_slot, file_sha256 FROM ("):
                kind = "occurrences"
            elif sql.startswith(
                "SELECT file_sha256 FROM catalog_analysis_changed_file_hashes "
            ):
                kind = "changed"
            else:
                raise RuntimeError("unclassified decision source query")
        steps = 0

        def progress() -> int:
            nonlocal steps
            steps += 100
            return 0

        sqlite = connector if isinstance(connector, SQLiteConnector) else None
        if sqlite is not None:
            sqlite.connection.set_progress_handler(progress, 100)
        started = time.perf_counter()
        try:
            rows = original(sql, parameters)
        finally:
            if sqlite is not None:
                sqlite.connection.set_progress_handler(None, 0)
        if len(rows) > (128 if source else 129):
            raise RuntimeError("query returned more than its fixed page bound")
        reads.calls += 1
        if kind == "occurrences":
            reads.branch_ranges += sql.count("AS source_slot")
        reads.vm_steps += steps
        if source:
            setattr(reads, kind, getattr(reads, kind) + len(rows))
        name = kind + ("_after" if "file_sha256 > %s" in sql else "_first")
        reads.samples.setdefault(
            name,
            Query(
                name,
                sql,
                parameters,
                len(rows),
                time.perf_counter() - started,
                steps if sqlite else None,
            ),
        )
        return rows

    with patch.object(connector, "fetch_all", fetch):
        yield reads


def require_vm_budget(reads: Reads, shape: Shape) -> None:
    if reads.vm_steps > shape.vm_budget:
        raise RuntimeError("decision key SQL exceeds fixed linear VM budget")


def require_source_budget(reads: Reads, shape: Shape) -> None:
    if (reads.calls, reads.memberships, reads.occurrences, reads.changed) != (
        shape.source_calls,
        shape.galleries,
        shape.occurrences,
        1,
    ):
        raise RuntimeError("decision key source differs from the one-pass model")
    if reads.branch_ranges != shape.branch_ranges:
        raise RuntimeError("decision key range count differs from the branch model")
    require_vm_budget(reads, shape)


def require_handler_budget(counters: dict[str, int], shape: Shape) -> None:
    # Each branch does one PK seek and streams at most its quota. MariaDB
    # materializes/scans each derived LIMIT branch: at most one row read plus
    # one EOF per branch, with a second such allowance for UNION delivery.
    if (
        counters["Handler_read_next"] > shape.occurrences + shape.galleries + 1
        or counters["Handler_read_key"]
        > shape.branch_ranges + shape.galleries + shape.galleries // 128 + 2
        or counters["Handler_read_prev"] != 0
        or counters["Handler_read_rnd_next"]
        > 2 * (shape.occurrences + shape.branch_ranges)
    ):
        raise RuntimeError("decision key SQL exceeds indexed MariaDB handler budget")


def measure_old(connector: SQLConnector, oracle: tuple[bytes, ...]) -> Reads:
    with measure_reads(connector, source=False) as reads:
        after = None
        matched = 0
        while True:
            sql, parameters = old_query(after)
            keys = tuple(row[0] for row in connector.fetch_all(sql, parameters)[:128])
            if keys != oracle[matched : matched + 128]:
                raise RuntimeError("negative control differs from fixture oracle")
            matched += len(keys)
            if not keys:
                break
            after = keys[-1]
    return reads


def require_plan_delivery(plan: AnalysisHashKeyPlan, oracle: tuple[bytes, ...]) -> None:
    """Consume every key page, including the empty tail, and compare its oracle."""
    after = None
    matched = 0
    while True:
        keys = plan.source_page(after=after, limit=128)
        if keys != oracle[matched : matched + 128]:
            raise RuntimeError("prepared keys differ from fixture oracle")
        matched += len(keys)
        if not keys:
            break
        after = keys[-1]


def measure_case(connector: SQLConnector, backend: str, shape: Shape) -> dict[str, Any]:
    oracle = seed(connector, backend, shape)
    authority = analysis.AnalysisPreparationAuthority(
        ANALYSIS, BUILD, 1, 1, b"m" * 32, (), analysis._PREPARATION_TOKEN
    )
    before = handler_counts(connector)
    started = time.perf_counter()
    with measure_reads(connector, source=True) as reads:
        plan = build_analysis_hash_key_plan(
            authority,
            b"i" * 32,
            analysis._iter_decision_source_hashes(connector, ANALYSIS, BUILD),
            stage=b"file_hash_decision",
        )
    prepared_seconds = time.perf_counter() - started
    try:
        counters = {
            key: value - before[key] for key, value in handler_counts(connector).items()
        }
        require_source_budget(reads, shape)
        if backend == "mariadb":
            require_handler_budget(counters, shape)
        unique_keys = plan.row_count
        with patch.object(
            connector,
            "fetch_all",
            side_effect=AssertionError("prepared traversal attempted SQL"),
        ):
            started = time.perf_counter()
            require_plan_delivery(plan, oracle)
            delivery_seconds = time.perf_counter() - started
            for _cycle in range(2):
                require_plan_delivery(plan, oracle)
    finally:
        started = time.perf_counter()
        plan.close()
        cleanup_seconds = time.perf_counter() - started
    before_old = handler_counts(connector)
    started = time.perf_counter()
    old = measure_old(connector, oracle)
    old_seconds = time.perf_counter() - started
    old_counters = {
        key: value - before_old[key] for key, value in handler_counts(connector).items()
    }
    for sample in (*reads.samples.values(), *old.samples.values()):
        sample.plan = (
            profile_mariadb(connector, sample)
            if isinstance(connector, MariaDBConnector)
            else connector.fetch_all(
                "EXPLAIN QUERY PLAN " + sample.sql, sample.parameters
            )
        )
    sqlite_programs = []
    if isinstance(connector, SQLiteConnector):
        for name, sample in reads.samples.items():
            instructions = len(
                connector.fetch_all("EXPLAIN " + sample.sql, sample.parameters)
            )
            ranges = max(1, sample.sql.count("AS source_slot"))
            sqlite_programs.append(
                {
                    "sample": name,
                    "static_instructions": instructions,
                    "branches": ranges,
                }
            )
    return {
        "cost_contract": {
            "sqlite_version": sqlite3.sqlite_version if backend == "sqlite" else None,
            "sqlite_vm_coefficient": 100,
            "sqlite_vm_budget_basis": "Predeclared conservative finite regression allowance per source row, branch range, membership and SQL call; covers VM loop/seek/coroutine setup plus <100/query sampling remainder. Not an ideal runtime, formal opcode upper-bound proof, or NAS wall-time guarantee.",
            "sqlite_program_sizes": sqlite_programs,
            "source_rows": "Each current membership and each accepted selected-observation occurrence exactly once; excludes unrelated history.",
            "mariadb_handler_budget_basis": "One PK seek per branch plus membership qualification point lookups; Handler_read_next <= source occurrences + memberships + changed rows. Derived LIMIT branch and UNION delivery allowances each <= occurrence rows + one EOF per branch; no reverse scans.",
        },
        "timing_scope": {
            "candidate": "Plan preparation + first complete authenticated key delivery + owned-plan cleanup.",
            "old_union": "One complete key delivery using repeated UNION queries.",
            "excluded": "Fixture setup; handler diagnostic queries; EXPLAIN; two extra correctness traversals; later decision aggregates and validation.",
            "interpretation": "Finite local fixture observation, not full-stage timing or a NAS SLA estimate.",
        },
        "shape": asdict(shape),
        "unique_keys": unique_keys,
        "fixed_vm_budget": shape.vm_budget,
        "candidate": asdict(reads),
        "candidate_seconds": prepared_seconds + delivery_seconds + cleanup_seconds,
        "candidate_preparation_seconds": prepared_seconds,
        "candidate_first_delivery_seconds": delivery_seconds,
        "candidate_cleanup_seconds": cleanup_seconds,
        "candidate_handlers": counters,
        "old_union": asdict(old),
        "old_union_seconds": old_seconds,
        "old_union_handlers": old_counters,
        "three_traversals_without_sql": True,
        "old_after_fingerprint": query_fingerprint(old_query(digest(128))[0]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--galleries", type=int, nargs="+", default=[128, 512, 2048])
    parser.add_argument("--hashes", type=int, default=4)
    parser.add_argument("--history", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 600 or not 1 <= len(args.galleries) <= 6:
        parser.error("timeout must be 1..600 seconds and 1..6 shapes are allowed")
    if args.output.exists():
        parser.error("output must be a new file")
    shapes = [
        Shape(galleries, args.hashes, history=args.history)
        for galleries in args.galleries
    ]
    if max(shape.occurrences for shape in shapes) > 50_000:
        parser.error(
            "complete negative-control traversal is capped at 50000 source keys"
        )
    report: dict[str, Any] = {
        "status": "incomplete",
        "backend": args.backend,
        "scope": __doc__,
        "cases": [],
    }

    def timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError("decision key probe exceeded its cooperative deadline")

    previous = signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, args.timeout_seconds)
    try:
        with databases(args.backend, len(shapes)) as connections:
            for connector, shape in zip(connections, shapes, strict=True):
                case = measure_case(connector, args.backend, shape)
                report["cases"].append(case)
                write_report(args.output, report)
                print(
                    json.dumps(
                        {
                            "shape": case["shape"],
                            "candidate_seconds": case["candidate_seconds"],
                            "old_union_seconds": case["old_union_seconds"],
                        }
                    ),
                    flush=True,
                )
        report["status"] = "complete"
    except BaseException as error:
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        write_report(args.output, report)


if __name__ == "__main__":
    main()
