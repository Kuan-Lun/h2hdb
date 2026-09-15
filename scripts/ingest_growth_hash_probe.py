"""Measure real file-decision validation on disposable synthetic databases.

The fixture seeds repository-level accepted source facts, like the analysis
repository tests. It does not exercise metadata scanning, complete source seals,
CBZ rendering, publication, cleanup, or a full READY audit. No existing database
or caller-supplied server can be opened by the CLI.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Generator, Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field, replace
from itertools import groupby
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]

from test_vnext_analysis_repository import (  # noqa: E402 - select checkout fixtures.
    _map_working_build,
    _seed_build,
    _seed_gallery,
    _seed_root,
    _source_build_id,
)
from vnext_generated_database import (  # noqa: E402 - select checkout fixtures.
    open_generated_sqlite_database,
)

from h2hdb import vnext_analysis_repository as analysis  # noqa: E402 - checkout source.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402 - checkout source.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout source.
from h2hdb.sql_performance import (  # noqa: E402 - checkout source.
    instrument_connector,
    measure_sql,
)
from h2hdb.sqlite_connector import SQLiteConnector  # noqa: E402 - checkout source.
from h2hdb.vnext_ingest_fence_repository import (  # noqa: E402 - checkout source.
    IngestFenceRepository,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (  # noqa: E402 - checkout source.
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_schema_provider import (  # noqa: E402 - checkout source.
    GeneratedVNextSchemaProvider,
)
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - checkout source.


@dataclass(frozen=True)
class Shape:
    hashes_per_gallery: int
    history_builds: int = 0
    galleries: int = 2

    def __post_init__(self) -> None:
        if not 1 <= self.hashes_per_gallery <= 4096:
            raise ValueError("hashes_per_gallery must be between 1 and 4096")
        if not 0 <= self.history_builds <= 64:
            raise ValueError("history_builds must be between 0 and 64")
        if not 2 <= self.galleries <= 5:
            raise ValueError("galleries must be between 2 and 5")

    @property
    def keys(self) -> tuple[bytes, ...]:
        return tuple(
            value.to_bytes(32, "big")
            for value in range(1, self.galleries * self.hashes_per_gallery + 1)
        )


@dataclass
class Query:
    kind: str
    sql: str
    parameters: tuple[Any, ...]
    rows: int
    seconds: float
    sqlite_vm_steps: int | None
    plan: Any = None


@dataclass
class Recorder:
    sql_calls: int = 0
    sql_seconds: float = 0.0
    returned_rows: int = 0
    queries: list[Query] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        if category == "sql":
            self.sql_calls += 1
            self.sql_seconds += elapsed
            self.returned_rows += read_rows


def query_kind(sql: str) -> str | None:
    if sql.startswith("SELECT keyset.file_sha256"):
        return "validation_union"
    if "SUM(occurrence.occurrence_count)" in sql:
        return "occurrences"
    if "COUNT(DISTINCT artist.artist_tag_id)" in sql:
        return "distinct_artists"
    if "MAX(per_gallery.artist_count)" in sql:
        return "maximum_gallery_artists"
    return None


@contextmanager
def record(connector: SQLConnector) -> Iterator[Recorder]:
    recorder = Recorder()
    original = connector.fetch_all

    def fetch_all(sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        kind = query_kind(sql)
        if kind is None:
            return original(sql, parameters)
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
        recorder.queries.append(
            Query(
                kind,
                sql,
                tuple(parameters or ()),
                len(rows),
                time.perf_counter() - started,
                steps if sqlite is not None else None,
            )
        )
        return rows

    with patch.object(connector, "fetch_all", fetch_all), measure_sql(recorder):
        yield recorder


def seed(
    connector: SQLConnector, backend: str, shape: Shape
) -> tuple[bytes, GateLease, IngestTurn]:
    """Keep the first page and its source membership stable as facts grow."""
    fixture_connector = cast(SQLiteConnector, connector)
    with connector.transaction():
        gate = MaintenanceGateRepository.claim_shared(
            VNextUnitOfWork(connector, backend=backend),
            now=10,
            lease_duration=1_000_000,
        )
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend=backend),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=1_000_000,
        )
        scope = _seed_root(fixture_connector)
        build = b""
        for generation in range(shape.history_builds + 1):
            manifest = (generation + 1).to_bytes(32, "big")
            current = _source_build_id(
                fixture_connector,
                scope=scope,
                manifest_sha256=manifest,
                gallery_count=shape.galleries,
            )
            _seed_build(
                fixture_connector,
                build_id=current,
                scope=scope,
                manifest_byte=1,
                manifest_sha256=manifest,
                gallery_count=shape.galleries,
            )
            for gallery in range(1, shape.galleries + 1):
                _seed_gallery(
                    fixture_connector,
                    build_id=current,
                    scope=scope,
                    gallery_id=gallery,
                    observation_id=generation + 1,
                    occurrences=tuple(
                        (
                            (page * shape.galleries + gallery).to_bytes(32, "big"),
                            1,
                        )
                        for page in range(shape.hashes_per_gallery)
                    ),
                    artists=(1, 2) if gallery % 2 else (2, 3),
                    serial=100 + generation * shape.galleries + gallery,
                )
            if generation == 0:
                build = current
        _map_working_build(fixture_connector, build_id=build, generation=1)
    with connector.transaction():
        run = analysis.AnalysisRepository.begin(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build,
            policy_id=1,
            proposed_analysis_id=b"a" * 16,
            now=30,
        )
    return run.analysis_id, gate, turn


def run_stage(
    connector: SQLConnector,
    backend: str,
    run: tuple[bytes, GateLease, IngestTurn],
    stage: str,
    index: int,
) -> list[dict[str, Any]]:
    analysis_id, gate, turn = run
    operation = getattr(analysis.AnalysisRepository, stage)
    pages = []
    for page in range(164):
        with connector.transaction():
            result = operation(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=f"{index}-{page}".encode(),
                max_rows=128,
                now=100 + index * 200 + page,
            )
        pages.append({"row_count": result.row_count, "terminal": result.terminal})
        if result.terminal:
            return pages
    raise RuntimeError("bounded hash stage did not finish")


def profile_mariadb(connector: MariaDBConnector, query: Query) -> dict[str, Any]:
    def counters() -> dict[str, int]:
        return {
            str(key): int(value)
            for key, value in connector.fetch_all(
                "SHOW SESSION STATUS LIKE 'Handler_read_%'"
            )
        }

    assert connector.connection is not None
    before = counters()
    with connector.connection.cursor(dictionary=True) as cursor:
        cursor.execute("ANALYZE " + query.sql, query.parameters)
        actual = cursor.fetchall()
    after = counters()
    with connector.connection.cursor(dictionary=True) as cursor:
        cursor.execute("EXPLAIN " + query.sql, query.parameters)
        explain = cursor.fetchall()
    return {
        "analyze": actual,
        "explain": explain,
        "handler_read_delta": {
            key: after[key] - value for key, value in before.items()
        },
        # MariaDB can emit invalid JSON for binary conditions. Retain this exact
        # server text without parsing or attempting to repair its SQL strings.
        "raw_analyze_json": connector.fetch_one(
            "ANALYZE FORMAT=JSON " + query.sql, query.parameters
        )[0],
    }


def measure_case(
    connector: SQLConnector,
    backend: str,
    shape: Shape,
    *,
    force_index_experiment: bool = False,
) -> dict[str, Any]:
    run = seed(connector, backend, shape)
    for index, stage in enumerate(
        (
            "process_changed_gallery_batch",
            "process_changed_file_hash_batch",
            "process_file_hash_decision_batch",
        )
    ):
        run_stage(connector, backend, run, stage, index)
    with record(connector) as recorder:
        started = time.perf_counter()
        pages = run_stage(
            instrument_connector(connector),
            backend,
            run,
            "validate_file_hash_decision_batch",
            3,
        )
        seconds = time.perf_counter() - started
    expected_queries = {
        "validation_union": len(pages),
        "occurrences": len(pages) - 1,
        "distinct_artists": len(pages) - 1,
        "maximum_gallery_artists": len(pages) - 1,
    }
    actual_queries = dict(Counter(query.kind for query in recorder.queries))
    if actual_queries != expected_queries:
        raise RuntimeError(
            f"incomplete query measurement: expected {expected_queries}, got {actual_queries}"
        )
    rows = connector.fetch_all(
        "SELECT file_sha256, occurrence_count, artist_count, "
        "maximum_gallery_artist_count "
        "FROM catalog_analysis_file_hash_decision_resolved "
        "WHERE analysis_id = %s ORDER BY file_sha256",
        (run[0],),
    )
    expected = [(key, 1, 2, 2) for key in shape.keys]
    if rows != expected:
        raise AssertionError(
            "production decisions differ from independent fixture oracle"
        )
    measured = []
    experiments = []
    experimented_kinds: set[str] = set()
    for query in recorder.queries:
        if backend == "mariadb":
            query.plan = profile_mariadb(cast(MariaDBConnector, connector), query)
        else:
            query.plan = connector.fetch_all(
                "EXPLAIN QUERY PLAN " + query.sql, query.parameters
            )
        measured.append(asdict(query))
        if (
            force_index_experiment
            and backend == "mariadb"
            and query.kind != "validation_union"
            and query.kind not in experimented_kinds
        ):
            forced = replace(
                query,
                sql=query.sql.replace(
                    "AS occurrence ",
                    "AS occurrence FORCE INDEX (ix_observation_hash_occurrence_group) ",
                ),
            )
            expected_rows = connector.fetch_all(query.sql, query.parameters)
            started = time.perf_counter()
            actual_rows = connector.fetch_all(forced.sql, forced.parameters)
            forced.seconds = time.perf_counter() - started
            if actual_rows != expected_rows:
                raise AssertionError("forced-index experiment changed query results")
            forced.plan = profile_mariadb(cast(MariaDBConnector, connector), forced)
            experiments.append(asdict(forced) | {"same_results": True})
            experimented_kinds.add(query.kind)
    return {
        "shape": asdict(shape),
        "active_hashes": len(shape.keys),
        "all_occurrence_rows": len(shape.keys) * (shape.history_builds + 1),
        "oracle_matches": True,
        "pages": pages,
        "seconds": seconds,
        "sql_calls": recorder.sql_calls,
        "sql_seconds": recorder.sql_seconds,
        "returned_rows": recorder.returned_rows,
        "queries": measured,
        "experimental_force_hash_index": experiments,
    }


@contextmanager
def databases(backend: str, count: int) -> Iterator[Iterator[SQLConnector]]:
    """Create fresh databases and deterministically destroy their owner."""
    if backend == "sqlite":
        with tempfile.TemporaryDirectory(prefix="h2hdb-hash-probe-") as directory:

            def sqlite_databases() -> Generator[SQLConnector]:
                for index in range(count):
                    connector = open_generated_sqlite_database(
                        Path(directory) / f"case-{index}.sqlite3"
                    )
                    try:
                        yield connector
                    finally:
                        close_preserving_error(connector.close)

            with closing(sqlite_databases()) as connections:
                yield connections
        return
    from testcontainers.community.mysql import MySqlContainer

    container = MySqlContainer(
        image="mariadb:10.11.11",
        username="hash_probe",
        password="disposable_probe",
        root_password="disposable_root",
        dbname="probe_template",
    )
    try:
        container.start()

        def connect(database: str) -> MariaDBConnector:
            connector = MariaDBConnector(
                host=container.get_container_host_ip(),
                port=int(container.get_exposed_port(container.port)),
                user="root",
                password="disposable_root",
                database=database,
            )
            try:
                connector.connect()
            except BaseException:
                close_preserving_error(connector.close)
                raise
            return connector

        admin = connect("probe_template")
        try:
            version = admin.fetch_one("SELECT VERSION()")[0]
            if not str(version).startswith("10.11.11-"):
                raise RuntimeError(f"unexpected MariaDB version: {version}")

            def maria_databases() -> Generator[SQLConnector]:
                definition = GeneratedVNextSchemaProvider("mariadb").definition
                for index in range(count):
                    name = f"hash_probe_{index}"
                    admin.execute(f"CREATE DATABASE {name}")
                    connector = connect(name)
                    try:
                        for schema_slice in definition.slices:
                            for statement in schema_slice.statements:
                                connector.execute(statement.sql)
                        with connector.transaction():
                            for sql, seeds in groupby(
                                definition.bootstrap_seeds, key=lambda seed: seed.sql
                            ):
                                connector.execute_many(
                                    sql, [seed.parameters for seed in seeds]
                                )
                        yield connector
                    finally:
                        close_preserving_error(connector.close)

            with closing(maria_databases()) as connections:
                yield connections
        finally:
            close_preserving_error(admin.close)
    finally:
        close_preserving_error(container.stop)


def close_preserving_error(close: Callable[[], Any]) -> None:
    original = sys.exception()
    try:
        close()
    except BaseException as error:
        if original is None:
            raise
        original.add_note(f"probe cleanup also failed: {type(error).__name__}: {error}")


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=".hash-report-", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(report, stream, indent=2, default=json_value)
            stream.flush()
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def json_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"unsupported JSON value {type(value).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--hashes", type=int, nargs="+", default=[64, 128, 256, 512])
    parser.add_argument("--history", type=int, nargs="+", default=[0])
    parser.add_argument("--galleries", type=int, default=2)
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=240,
        help="POSIX cooperative alarm; driver/Docker teardown is not a hard deadline",
    )
    parser.add_argument("--force-index-experiment", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not all(hasattr(signal, name) for name in ("SIGALRM", "setitimer")):
        parser.error("this manual probe requires POSIX SIGALRM support")
    if args.force_index_experiment and args.backend != "mariadb":
        parser.error("force-index-experiment requires the MariaDB backend")
    if not 1 <= args.timeout_seconds <= 900:
        parser.error("timeout-seconds must be between 1 and 900")
    if args.output.exists():
        parser.error("output must be a new file")
    shapes = [Shape(n, h, args.galleries) for n in args.hashes for h in args.history]
    if len(shapes) > 16:
        parser.error("at most 16 cases per invocation")

    def timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError("hash growth probe exceeded its wall-clock deadline")

    results: dict[str, Any] = {
        "status": "incomplete",
        "performance_verdict": "measurement only; no performance budget passes or fixes",
        "backend": args.backend,
        "fixture": "repository-seeded accepted facts; no CBZ or full READY audit",
        "history": "unselected source builds/observations, not analysis overlay lineage",
        "sqlite_steps": "100-opcode progress samples; each query error is below 100 steps",
        "mariadb_work": "tabular ANALYZE r_rows plus same-session Handler_read counters; handler operations are not exact rows examined",
        "cases": [],
    }
    write_report(args.output, results)
    previous = signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, args.timeout_seconds)
    try:
        with databases(args.backend, len(shapes)) as connections:
            for connector, shape in zip(connections, shapes, strict=True):
                case = measure_case(
                    connector,
                    args.backend,
                    shape,
                    force_index_experiment=args.force_index_experiment,
                )
                results["cases"].append(case)
                write_report(args.output, results)
                print(
                    json.dumps(
                        {
                            key: case[key]
                            for key in (
                                "shape",
                                "active_hashes",
                                "sql_calls",
                                "seconds",
                                "oracle_matches",
                            )
                        }
                    ),
                    flush=True,
                )
        results["status"] = "completed"
        write_report(args.output, results)
    except BaseException as error:
        results["error"] = {"type": type(error).__name__, "message": str(error)}
        try:
            write_report(args.output, results)
        except BaseException as report_error:
            error.add_note(f"partial report write failed: {report_error}")
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == "__main__":
    main()
