"""Compare changed-hash preparation with the former repeated UNION query.

Uses disposable generated-schema SQLite/MariaDB 10.11.11 databases. Only the
four query-input relations are seeded with foreign-key checks disabled; this is
an isolated query-cost experiment, not a full ingest or READY audit. Measures
SQLite VM instructions and MariaDB handler operations, not physical disk reads
or exact server rows examined. No existing database/server can be supplied.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from ingest_growth_hash_probe import (  # noqa: E402 - checkout probe utilities.
    Query,
    databases,
    profile_mariadb,
    write_report,
)

from h2hdb import vnext_analysis_repository as analysis  # noqa: E402 - checkout source.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402 - checkout source.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout source.
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402 - checkout source.
from h2hdb.vnext_changed_hash_plan import (  # noqa: E402 - checkout source.
    build_changed_hash_plan,
)

ANALYSIS = b"a" * 16
CURRENT = b"c" * 16
BASELINE = b"b" * 16


@dataclass(frozen=True)
class Shape:
    galleries: int
    hashes: int = 40
    unrelated_galleries: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.galleries <= 10_000:
            raise ValueError("galleries must be between 1 and 10000")
        if not 1 <= self.hashes <= 512:
            raise ValueError("hashes must be between 1 and 512")
        if not 0 <= self.unrelated_galleries <= 10_000:
            raise ValueError("unrelated galleries must be between 0 and 10000")
        if (self.galleries + self.unrelated_galleries) * self.hashes > 500_000:
            raise ValueError("fixture exceeds 500000 unique hashes")

    @property
    def occurrences(self) -> int:
        return self.galleries * self.hashes * 2

    @property
    def source_calls(self) -> int:
        return self.galleries // 128 + 1 + 2 * self.galleries * (self.hashes // 128 + 1)


def key(gallery: int, page: int) -> bytes:
    return sha256(f"changed-hash-fixture:{gallery}:{page}".encode()).digest()


def seed(connector: SQLConnector, backend: str, shape: Shape) -> tuple[bytes, ...]:
    connector.execute(
        "PRAGMA foreign_keys=OFF" if backend == "sqlite" else "SET FOREIGN_KEY_CHECKS=0"
    )
    with connector.transaction():
        connector.execute_many(
            "INSERT INTO catalog_analysis_changed_galleries (analysis_id, gallery_id, change_kind) VALUES (%s, %s, %s)",
            [
                (ANALYSIS, gallery, "REPLACED")
                for gallery in range(1, shape.galleries + 1)
            ],
        )
        total = shape.galleries + shape.unrelated_galleries
        for start in range(1, total + 1, 128):
            galleries = range(start, min(total + 1, start + 128))
            for observation, build in ((1, BASELINE), (2, CURRENT)):
                connector.execute_many(
                    "INSERT INTO catalog_source_build_galleries (build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    [(build, gallery, observation) for gallery in galleries],
                )
                connector.execute_many(
                    "INSERT INTO catalog_gallery_observation_validation_dispositions (gallery_id, observation_id, accepted) VALUES (%s, %s, 1)",
                    [(gallery, observation) for gallery in galleries],
                )
                connector.execute_many(
                    "INSERT INTO catalog_gallery_observation_file_hash_occurrences (gallery_id, observation_id, file_sha256, occurrence_count) VALUES (%s, %s, %s, 1)",
                    [
                        (gallery, observation, key(gallery, page))
                        for gallery in galleries
                        for page in range(shape.hashes)
                    ],
                )
    connector.execute(
        "ANALYZE"
        if backend == "sqlite"
        else "ANALYZE TABLE catalog_analysis_changed_galleries, catalog_source_build_galleries, catalog_gallery_observation_file_hash_occurrences, catalog_gallery_observation_validation_dispositions"
    )
    return tuple(
        sorted(
            key(gallery, page)
            for gallery in range(1, shape.galleries + 1)
            for page in range(shape.hashes)
        )
    )


@dataclass
class Reads:
    calls: int = 0
    rows: int = 0
    memberships: int = 0
    occurrences: int = 0
    vm_steps: int = 0
    samples: dict[str, Query] = field(default_factory=dict)


@contextmanager
def measure_reads(connector: SQLConnector) -> Iterator[Reads]:
    result = Reads()
    original = connector.fetch_all

    def fetch_all(sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        kind = (
            "membership"
            if sql.startswith("SELECT catalog_analysis_changed_galleries.gallery_id,")
            else "occurrence"
        )
        if kind == "occurrence" and not sql.startswith(
            "SELECT file_sha256 FROM catalog_gallery_observation_file_hash_occurrences "
        ):
            raise RuntimeError(
                "changed-hash preparation issued an unclassified source query"
            )
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
        if len(rows) > 128:
            raise RuntimeError("changed-hash source read exceeded 128 rows")
        result.calls += 1
        result.rows += len(rows)
        result.vm_steps += steps
        if kind == "membership":
            result.memberships += len(rows)
        else:
            result.occurrences += len(rows)
        sample = kind + ("_after" if "AND file_sha256 > %s" in sql else "_first")
        result.samples.setdefault(
            sample,
            Query(
                sample,
                sql,
                tuple(parameters),
                len(rows),
                time.perf_counter() - started,
                steps if sqlite is not None else None,
            ),
        )
        return rows

    with patch.object(connector, "fetch_all", fetch_all):
        yield result


def handler_counts(connector: SQLConnector) -> dict[str, int]:
    if not isinstance(connector, MariaDBConnector):
        return {}
    return {
        str(name): int(value)
        for name, value in connector.fetch_all(
            "SHOW SESSION STATUS LIKE 'Handler_read_%'"
        )
    }


def old_query(after: bytes | None) -> tuple[str, tuple[Any, ...]]:
    """Deliberately degraded former production query for the regression oracle."""
    branch = (
        "SELECT occurrence.file_sha256 AS file_sha256 "
        "FROM catalog_analysis_changed_galleries AS changed JOIN "
        + analysis._ACCEPTED_SOURCE_MEMBERS
        + " AS member "
        "ON member.build_id = %s AND member.gallery_id = changed.gallery_id "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id AND occurrence.observation_id = member.observation_id "
        "WHERE changed.analysis_id = %s"
    )
    return (
        "SELECT DISTINCT affected.file_sha256 FROM ("
        + branch
        + " UNION "
        + branch
        + ") AS affected"
        + ("" if after is None else " WHERE affected.file_sha256 > %s")
        + " ORDER BY affected.file_sha256 LIMIT %s",
        (
            CURRENT,
            ANALYSIS,
            BASELINE,
            ANALYSIS,
            *(() if after is None else (after,)),
            129,
        ),
    )


def measure_old(
    connector: SQLConnector, oracle: tuple[bytes, ...], *, complete: bool
) -> dict[str, Any]:
    after = None
    steps = 0
    calls = 0
    matched = 0
    before = handler_counts(connector)

    def progress() -> int:
        nonlocal steps
        steps += 100
        return 0

    sqlite = connector if isinstance(connector, SQLiteConnector) else None
    if sqlite is not None:
        sqlite.connection.set_progress_handler(progress, 100)
    started = time.perf_counter()
    try:
        while True:
            sql, parameters = old_query(after)
            rows = connector.fetch_all(sql, parameters)
            calls += 1
            keys = tuple(row[0] for row in rows[:128])
            if keys != oracle[matched : matched + 128]:
                raise RuntimeError("old query differs from independent fixture oracle")
            matched += len(keys)
            if not complete or not keys:
                break
            after = keys[-1]
    finally:
        if sqlite is not None:
            sqlite.connection.set_progress_handler(None, 0)
    seconds = time.perf_counter() - started
    after_counts = handler_counts(connector)
    result: dict[str, Any] = {
        "complete_traversal": complete,
        "calls": calls,
        "matched": matched,
        "seconds": seconds,
        "vm_steps": steps if sqlite is not None else None,
        "handler_read_delta": {
            key: after_counts[key] - value for key, value in before.items()
        },
    }
    if isinstance(connector, MariaDBConnector):
        sql, parameters = old_query(None)
        result["first_page_plan"] = profile_mariadb(
            connector, Query("old", sql, parameters, min(129, len(oracle)), 0, None)
        )
    return result


def require_linear_source_cost(reads: Reads, shape: Shape) -> None:
    if (reads.calls, reads.memberships, reads.occurrences) != (
        shape.source_calls,
        shape.galleries,
        shape.occurrences,
    ):
        raise RuntimeError("changed-source reads disagree with the one-pass model")
    # A generous finite VM bound includes PK lookup/qualification instructions
    # and one <100-instruction sampling error per query. It is not a DB theorem.
    if reads.vm_steps > 100 * (
        shape.occurrences + shape.galleries + shape.source_calls
    ):
        raise RuntimeError("changed-source VM work exceeds the linear fixture bound")


def require_mariadb_source_cost(counters: dict[str, int], shape: Shape) -> None:
    """Bound this fixture's ordered source ranges and four membership point joins."""
    if (
        counters["Handler_read_next"] > shape.occurrences + shape.galleries
        or counters["Handler_read_key"] > shape.source_calls + 4 * shape.galleries
        or counters["Handler_read_prev"] != 0
        or counters["Handler_read_rnd_next"] != 0
    ):
        raise RuntimeError(
            "changed-source MariaDB work exceeds its indexed one-pass bound"
        )


def measure_case(
    connector: SQLConnector, backend: str, shape: Shape, *, complete_old: bool = False
) -> dict[str, Any]:
    oracle = seed(connector, backend, shape)
    authority = analysis.AnalysisPreparationAuthority(
        ANALYSIS, CURRENT, 1, 1, b"m" * 32, (), analysis._PREPARATION_TOKEN
    )
    before = handler_counts(connector)
    started = time.perf_counter()
    with measure_reads(connector) as reads:
        plan = build_changed_hash_plan(
            authority,
            b"i" * 32,
            analysis._iter_changed_source_hashes(
                connector, ANALYSIS, CURRENT, BASELINE
            ),
        )
    seconds = time.perf_counter() - started
    try:
        after_counts = handler_counts(connector)
        source_handler_delta = {
            key: after_counts[key] - value for key, value in before.items()
        }
        if backend == "mariadb":
            require_mariadb_source_cost(source_handler_delta, shape)
        for _cycle in range(3):
            matched = 0
            after = None
            while True:
                keys = plan.source_page(after=after, limit=128)
                if keys != oracle[matched : matched + 128]:
                    raise RuntimeError(
                        "prepared page differs from independent fixture oracle"
                    )
                matched += len(keys)
                if not keys:
                    break
                after = keys[-1]
            if matched != len(oracle):
                raise RuntimeError("prepared traversal missed source keys")
        require_linear_source_cost(reads, shape)
        samples = []
        for sample in reads.samples.values():
            if isinstance(connector, MariaDBConnector):
                sample.plan = profile_mariadb(connector, sample)
            samples.append(asdict(sample))
        plan._payload.seek(0, 2)
        retained_bytes = plan._payload.tell()
        return {
            "shape": asdict(shape),
            "source_calls": reads.calls,
            "source_memberships": reads.memberships,
            "source_occurrences": reads.occurrences,
            "sqlite_vm_steps": reads.vm_steps if backend == "sqlite" else None,
            "seconds": seconds,
            "unique_hashes": plan.row_count,
            "retained_plan_bytes": retained_bytes,
            "three_local_traversals_match": True,
            "handler_read_delta": source_handler_delta,
            "samples": samples,
            "old": measure_old(connector, oracle, complete=complete_old),
        }
    finally:
        plan.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--galleries", type=int, nargs="+", default=[100, 1000, 10000])
    parser.add_argument("--hashes", type=int, default=40)
    parser.add_argument("--unrelated-galleries", type=int, default=0)
    parser.add_argument("--complete-old", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 900 or not 1 <= len(args.galleries) <= 8:
        parser.error("timeout must be 1..900 seconds and at most 8 shapes are allowed")
    if args.output.exists():
        parser.error("output must be a new file")
    shapes = [
        Shape(value, args.hashes, args.unrelated_galleries) for value in args.galleries
    ]
    if (
        args.complete_old
        and max(shape.galleries * shape.hashes for shape in shapes) > 50_000
    ):
        parser.error("complete old-query traversal is capped at 50000 unique hashes")
    report: dict[str, Any] = {
        "status": "incomplete",
        "backend": args.backend,
        "fixture": "isolated query input relations; no full READY audit or CBZ",
        "cases": [],
    }
    write_report(args.output, report)

    def timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError("changed-hash probe exceeded its cooperative deadline")

    previous = signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, args.timeout_seconds)
    try:
        with databases(args.backend, len(shapes)) as connections:
            for connector, shape in zip(connections, shapes, strict=True):
                case = measure_case(
                    connector, args.backend, shape, complete_old=args.complete_old
                )
                report["cases"].append(case)
                write_report(args.output, report)
                print(
                    json.dumps(
                        {
                            key: case[key]
                            for key in (
                                "shape",
                                "source_calls",
                                "source_occurrences",
                                "seconds",
                                "unique_hashes",
                            )
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
