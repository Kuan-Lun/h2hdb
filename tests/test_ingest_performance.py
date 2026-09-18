from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Event, Thread
from typing import Literal

import pytest

from h2hdb.config_loader import CoreConfig, DatabaseConfig, LoggerConfig
from h2hdb.ingest_performance import IngestPerformance, describe_ingest_step
from h2hdb.repository import RepositoryContext
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def performance_log(caplog: pytest.LogCaptureFixture) -> Iterator[logging.Logger]:
    logger = logging.getLogger("h2hdb.ingest_performance.test")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        yield logger


def test_info_counts_real_sqlite_work_without_sql_or_parameter_logging(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="sqlite", clock=clock)
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(tmp_path / "db"))
    )
    assert int(LoggerConfig().level) == logging.INFO
    with performance.step("publication", "commit", "BUILD_CATALOG", 42) as sample:
        context = RepositoryContext.from_config(config)
        try:
            with context.SQLConnector() as connector:
                with connector.transaction():
                    connector.execute(
                        "CREATE TABLE private_value (value TEXT PRIMARY KEY)"
                    )
                    connector.execute_many(
                        "INSERT INTO private_value VALUES (%s)",
                        [("secret-one",), ("secret-two",)],
                    )
                    assert connector.fetch_one(
                        "SELECT value FROM private_value WHERE value=%s",
                        ("secret-one",),
                    ) == ("secret-one",)
                    assert (
                        len(connector.fetch_all("SELECT value FROM private_value")) == 2
                    )
                    assert (
                        connector.execute_affected(
                            "DELETE FROM private_value WHERE value=%s", ("secret-two",)
                        )
                        == 1
                    )
            sample.processed_rows = 2
            sample.terminal = True
        finally:
            context.close()
        clock.now = 2.0
    text = caplog.text
    assert "building catalog metadata and indexes; ingest generation 42" in text
    assert "elapsed 2.0s; database work" in text
    assert "records processed" not in text
    assert "query calls" not in text
    assert (
        "5 completed SQL connector calls; 3 rows returned (not rows examined)" in text
    )
    assert "phases commit: 2.0s, SQL 0ms, 5 SQL calls, 3 rows returned" in text
    assert "ingest_db_performance" not in text
    assert "sql_calls=" not in text
    assert "commit_seconds=" not in text
    assert "private_value" not in text
    assert "secret-one" not in text
    assert "query_top=" not in text
    assert all(record.levelno == logging.INFO for record in caplog.records)


def test_failure_rolls_back_and_restores_instrumentation_context(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    path = tmp_path / "rollback"
    with SQLiteConnector(str(path)) as setup:
        setup.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    with pytest.raises(sqlite3.OperationalError):
        with performance.step("publication", "commit", "BUILD_CATALOG", 1):
            with instrument_connector(SQLiteConnector(str(path))) as connector:
                with connector.transaction():
                    connector.execute("INSERT INTO items VALUES (1)")
                    connector.execute("SELECT missing_column FROM items")
    raw = SQLiteConnector(str(path))
    assert instrument_connector(raw) is raw
    with raw:
        assert raw.fetch_all("SELECT * FROM items") == []
    assert "stage failed: building catalog metadata and indexes" in caplog.text
    assert "database work" in caplog.text
    assert "stage finished" not in caplog.text


def test_debug_query_statistics_are_bounded_and_redacted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.DEBUG
    )
    with performance.step("publication", "prepare", "VALIDATE_CATALOG", 1) as sample:
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connector:
            for position in range(100):
                connector.fetch_one(
                    f"SELECT %s AS field_{position}", ("secret-payload",)
                )
        assert (
            len(sample.queries) == 65
        )  # 64 query fingerprints plus one overflow bucket.
        assert sum(statistics.calls for statistics in sample.queries.values()) == 100
        assert (
            sum(statistics.read_rows for statistics in sample.queries.values()) == 100
        )
        assert sample.queries["other"].calls == 36
        assert sample.queries["other"].read_rows == 36
    performance.close()
    assert "query_top=" in caplog.text
    assert "secret-payload" not in caplog.text
    assert "SELECT" not in caplog.text
    assert "event=completed" in caplog.text


def test_debug_query_top_distinguishes_repeated_work_from_one_slow_call(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(
        performance_log, backend="mariadb", level=logging.DEBUG
    )
    repeated = "SELECT file_sha256 FROM private_decisions WHERE analysis_id = %s"
    slow = "SELECT private_payload FROM private_snapshot WHERE analysis_id = %s"
    repeated_fingerprint = sha256(repeated.encode()).hexdigest()[:16]
    slow_fingerprint = sha256(slow.encode()).hexdigest()[:16]
    with performance.step("analysis", "prepare", "PREPARE_SNAPSHOT", 51) as sample:
        sample.record_sql_operation("sql", 2.0, repeated, 128)
        sample.record_sql_operation("sql", 3.0, repeated, 2)
        sample.record_sql_operation("sql", 4.0, slow, 1)
        for position in range(6):
            sample.record_sql_operation("sql", 0.5, f"SELECT {position}", 1)
    message = next(
        record.message for record in caplog.records if record.levelno == logging.DEBUG
    )
    assert "operation=PREPARE_SNAPSHOT generation=51 phase=prepare" in message
    top = message.split(" query_top=", 1)[1].split(";")
    assert len(top) == 5
    assert top[:2] == [
        f"{repeated_fingerprint}(calls=2,seconds=5.000000,"
        "returned_rows=130,max_seconds=3.000000)",
        f"{slow_fingerprint}(calls=1,seconds=4.000000,"
        "returned_rows=1,max_seconds=4.000000)",
    ]
    assert "private" not in caplog.text
    assert "SELECT" not in caplog.text
    performance.close()


def test_info_does_not_collect_query_statistics(
    performance_log: logging.Logger,
) -> None:
    performance = IngestPerformance(performance_log, backend="mariadb")
    with performance.step("analysis", "prepare", "PREPARE_SNAPSHOT", 51) as sample:
        sample.record_sql_operation("sql", 2.0, "SELECT private_payload", 128)
        assert not sample.queries
        assert len(sample.slowest.snapshot()) == 1
        assert sample.counters.read_rows == 128
    performance.close()


def test_info_slowest_queries_do_not_disappear_after_64_fingerprints(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="mariadb", clock=clock)
    with performance.step("analysis", "prepare", "PREPARE_SNAPSHOT", 1) as sample:
        for _cycle in range(3):
            for index in range(130):
                seconds = 9.0 if index == 129 else 8.0 if index == 64 else 0.0
                sample.record_sql_operation("sql", seconds, f"SELECT {index}", 1)
                clock.now += seconds
        sample.terminal = True
        assert not sample.queries
        assert len(sample.slowest._entries) == 5
    info = "\n".join(
        record.message for record in caplog.records if record.levelno == logging.INFO
    )
    assert "390 completed SQL connector calls; 390 rows returned" in info
    assert sha256(b"SELECT 129").hexdigest()[:16] in info
    assert sha256(b"SELECT 64").hexdigest()[:16] in info
    assert "SELECT" not in info
    performance.close()


@pytest.mark.parametrize("phases", [127, 128, 129, 260])
def test_stage_phase_costs_are_bounded_and_repeated_cycles_conserve_totals(
    phases: int, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="sqlite", clock=clock)
    for _cycle in range(3):
        for index in range(phases):
            with performance.step("source", f"action_{index}", "SOURCE", 1) as sample:
                sample.record_sql_operation("sql", 0.5, "SELECT %s", 2)
                clock.now += 1.0
    stage = performance._stage
    assert stage is not None
    assert len(stage.phases) == min(phases, 128) + int(phases > 128)
    assert stage.phase_counters.keys() == stage.phases.keys()
    assert sum(stage.phases.values()) == phases * 3
    assert sum(item.sql_calls for item in stage.phase_counters.values()) == phases * 3
    assert (
        sum(item.sql_seconds for item in stage.phase_counters.values()) == phases * 1.5
    )
    assert sum(item.read_rows for item in stage.phase_counters.values()) == phases * 6
    performance.close()


def test_correlation_separates_sources_with_the_same_generation(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    for correlation_id in ("a" * 32, "b" * 32):
        for phase in ("TAG_PAGE.prepare", "TAG_PAGE.commit"):
            with performance.step(
                "source", phase, "SOURCE", 1, correlation_id=correlation_id
            ) as sample:
                sample.record_sql_operation("sql", 0.5, "SELECT %s", 128)
        assert performance._stage is not None
        assert performance._stage.calls == 2
        assert performance._stage.counters.read_rows == 256
    performance.close()
    assert f"correlation {'a' * 32}" in caplog.text
    assert f"correlation {'b' * 32}" in caplog.text
    assert caplog.text.count("2 completed SQL connector calls; 256 rows returned") == 2


def test_info_reports_time_based_progress_without_per_batch_messages(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="sqlite", clock=clock)
    for _ in range(100):
        for phase in ("issue", "prepare", "commit"):
            with performance.step("publication", phase, "BUILD_CATALOG", 1) as sample:
                clock.now += 0.01
                if phase == "commit":
                    sample.processed_rows = 128
    assert len(caplog.records) == 1  # Logical operation start only.
    clock.now = 63.0
    with performance.step("publication", "commit", "BUILD_CATALOG", 1):
        pass
    assert len(caplog.records) == 2
    assert "stage in progress" in caplog.text
    assert "elapsed 1m 03s; database work 0ms" in caplog.text
    with performance.step("publication", "prepare", "VALIDATE_CATALOG", 1):
        pass
    performance.close()
    assert len(caplog.records) == 5


def test_long_step_defers_sql_metrics_until_the_safe_call_boundary(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="sqlite", clock=clock)
    with performance.step("publication", "prepare", "VALIDATE_CATALOG", 1) as sample:
        clock.now = 61.0
        sample.record_sql_operation("sql", 55.0, "SELECT private_literal", 128)
        assert not caplog.records
        clock.now = 62.0
        sample.record_sql_operation("sql", 1.0, "SELECT private_literal", 1)
        assert not caplog.records
    assert "stage in progress" in caplog.text
    assert "elapsed 1m 02s" in caplog.text
    assert (
        "database work 56.0s (queries 56.0s, connections 0ms, transaction boundaries 0ms)"
        in caplog.text
    )
    assert "private_literal" not in caplog.text
    assert "stage finished" not in caplog.text


def test_nested_scopes_and_threads_do_not_mix_metrics(
    performance_log: logging.Logger,
    tmp_path: Path,
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as outer:
        with ThreadPoolExecutor(max_workers=1) as pool:
            raw = SQLiteConnector(str(tmp_path / "raw"))
            assert pool.submit(instrument_connector, raw).result() is raw
        with performance.step("analysis", "prepare", "content_owner", 1) as inner:
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "inner"))
            ) as connector:
                assert connector.fetch_one("SELECT 1") == (1,)
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "outer"))
        ) as connector:
            assert connector.fetch_one("SELECT 2") == (2,)
        assert outer.counters.sql_calls == inner.counters.sql_calls == 1
    performance.close()


def test_configured_error_level_suppresses_info_and_debug(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger, tmp_path: Path
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.ERROR
    )
    with performance.step("publication", "commit", "BUILD_CATALOG", 1) as sample:
        raw = SQLiteConnector(str(tmp_path / "disabled"))
        assert instrument_connector(raw) is raw
        assert not sample.active
    performance.close()
    assert not caplog.records


def test_broken_log_handler_does_not_change_success_or_exception() -> None:
    class _BrokenLogger(logging.Logger):
        def handle(self, record: logging.LogRecord) -> None:
            raise OSError("broken log destination")

    performance = IngestPerformance(
        _BrokenLogger("broken", logging.DEBUG), backend="sqlite", level=logging.DEBUG
    )
    with performance.step("publication", "commit", "BUILD_CATALOG", 1):
        pass
    with pytest.raises(ValueError, match="original"):
        with performance.step("publication", "commit", "BUILD_CATALOG", 1):
            raise ValueError("original")
    performance.close()


def test_sql_logging_happens_after_transaction_and_call_exit(tmp_path: Path) -> None:
    clock = _Clock()
    in_transaction: list[bool] = []
    raw = SQLiteConnector(str(tmp_path / "transaction"))

    class _Observer(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            in_transaction.append(raw.connection.in_transaction)

    logger = logging.Logger("transaction-boundary", logging.INFO)
    logger.addHandler(_Observer())
    performance = IngestPerformance(logger, backend="sqlite", clock=clock)
    with raw:
        raw.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        with performance.step("publication", "commit", "BUILD_CATALOG", 1):
            connector = instrument_connector(raw)
            with connector.transaction():
                clock.now = 61.0
                connector.execute("INSERT INTO items VALUES (1)")
                assert in_transaction == []
            assert in_transaction == []
        performance.close()
    assert in_transaction and not any(in_transaction)


def test_logger_can_reenter_close_without_locking_or_recursive_emission() -> None:
    calls: list[str] = []
    errors: list[BaseException] = []

    class _ReentrantLogger(logging.Logger):
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            calls.append(str(msg))
            performance.close()
            with performance.step("analysis", "prepare", "content_owner", 1):
                pass

    performance = IngestPerformance(
        _ReentrantLogger("reentry", logging.INFO), backend="sqlite"
    )

    def run() -> None:
        try:
            with performance.step("publication", "commit", "BUILD_CATALOG", 1):
                pass
        except BaseException as error:
            errors.append(error)

    thread = Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert not errors
    assert len(calls) == 1


def test_nested_calls_defer_bounded_records_and_preserve_own_time(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(
        performance_log, backend="sqlite", clock=clock, level=logging.DEBUG
    )
    with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as outer:
        clock.now = 1.0
        for _ in range(70):
            with performance.step("analysis", "prepare", "content_owner", 1):
                clock.now += 0.1
            assert not caplog.records
        assert len(outer.deferred) == 64
        assert outer.omitted_records == 6
        clock.now = 9.0
    performance.close()
    nested = [
        record.message for record in caplog.records if "scope=nested" in record.message
    ]
    assert len(nested) == 64
    assert all(
        "elapsed_seconds=0.100000 call_seconds=0.100000" in message
        for message in nested
    )
    outer_record = next(
        record.message
        for record in caplog.records
        if "scope=sequential" in record.message
    )
    assert "elapsed_seconds=9.000000 call_seconds=2.000000" in outer_record
    assert "nested_calls=70 nested_seconds=7.000000" in outer_record
    assert "omitted_nested_records=6" in outer_record
    assert "prepare_seconds=2.000000" in caplog.text


def test_overlapping_calls_do_not_share_a_sequential_stage(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    first_entered = Event()
    second_finished = Event()
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.DEBUG
    )

    def first() -> None:
        with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as sample:
            sample.processed_rows = 7
            first_entered.set()
            assert second_finished.wait(timeout=2.0)

    def second() -> None:
        assert first_entered.wait(timeout=2.0)
        with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as sample:
            sample.processed_rows = 11
        second_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(first), pool.submit(second)]
        for future in futures:
            future.result(timeout=3.0)
    performance.close()
    info = [record for record in caplog.records if record.levelno == logging.INFO]
    technical = [record for record in caplog.records if record.levelno == logging.DEBUG]
    assert len(info) == 2
    assert all("overlapping call finished" in record.message for record in info)
    assert all("stage completion not confirmed" in record.message for record in info)
    assert sorted("processed_rows=7" in record.message for record in technical) == [
        False,
        True,
    ]
    assert "stage_" not in caplog.text


def test_clock_and_recorder_failures_preserve_sql_results_and_original_errors(
    tmp_path: Path, performance_log: logging.Logger
) -> None:
    def broken_clock() -> float:
        raise OSError("diagnostic clock unavailable")

    class _BrokenRecorder:
        def record_sql_operation(
            self,
            category: Literal["sql", "connection", "transaction"],
            elapsed: float,
            query: str,
            rows: int,
        ) -> None:
            raise RuntimeError("diagnostic sink unavailable")

    performance = IngestPerformance(
        performance_log, backend="sqlite", clock=broken_clock
    )
    with performance.step("publication", "commit", "BUILD_CATALOG", 1):
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "clock"))
        ) as connector:
            assert connector.fetch_one("SELECT 1") == (1,)
            with pytest.raises(sqlite3.OperationalError, match="missing_column"):
                connector.fetch_one("SELECT missing_column")
    with measure_sql(_BrokenRecorder()):
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "record"))
        ) as connector:
            assert connector.fetch_one("SELECT 2") == (2,)
            with pytest.raises(sqlite3.OperationalError, match="missing_column"):
                connector.fetch_one("SELECT missing_column")
    with pytest.raises(ValueError, match="original"):
        with performance.step("publication", "commit", "BUILD_CATALOG", 1):
            raise ValueError("original")
    performance.close()


def test_copied_context_and_escaped_connector_cannot_extend_scope_lifetime(
    tmp_path: Path, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as sample:
        copied = copy_context()
        connector = instrument_connector(SQLiteConnector(str(tmp_path / "escaped")))
        with connector:
            assert connector.fetch_one("SELECT 1") == (1,)
        with ThreadPoolExecutor(max_workers=1) as pool:
            raw = SQLiteConnector(str(tmp_path / "thread"))
            assert pool.submit(copied.run, instrument_connector, raw).result() is raw
    counts = sample.counters.sql_calls
    with connector:
        assert copied.run(connector.fetch_one, "SELECT 2") == (2,)
    raw = SQLiteConnector(str(tmp_path / "after"))
    assert copied.run(instrument_connector, raw) is raw
    assert sample.counters.sql_calls == counts == 1
    performance.close()


def test_validated_step_description_is_bounded_and_scoped(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    describe_ingest_step(operation="outside", generation=1)
    performance = IngestPerformance(performance_log, backend="sqlite")
    with performance.step("publication", "issue", "ISSUE", 0) as sample:
        describe_ingest_step(operation=b"BUILD_CATALOG", generation=42)
        assert sample.operation == "BUILD_CATALOG"
        assert sample.generation == 42
        describe_ingest_step(operation=b"bad\xff", generation=-1)
        assert sample.operation == "INVALID"
        assert sample.generation == 0
        describe_ingest_step(operation="bad token\nsecret", generation=2**63)
        assert sample.operation == "INVALID"
        assert sample.generation == 0
        describe_ingest_step(operation="A" * 65, generation=1)
        assert sample.operation == "INVALID"
    performance.close()
    assert "secret" not in caplog.text


def test_close_defers_other_owner_logs_until_the_outer_transaction_exits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    closing = IngestPerformance(performance_log, backend="sqlite")
    outer = IngestPerformance(performance_log, backend="sqlite")
    with closing.step("publication", "prepare", "BUILD_CATALOG", 1):
        pass
    caplog.clear()
    connector = SQLiteConnector(str(tmp_path / "close"))
    with connector:
        with outer.step("publication", "commit", "BUILD_CATALOG", 2):
            with connector.transaction():
                closing.close()
                assert connector.connection.in_transaction
                assert not caplog.records
            assert not caplog.records
    assert "stage reporting closed before completion was confirmed" in caplog.text
    outer.close()


def test_child_async_task_does_not_inherit_measurement_ownership(
    tmp_path: Path, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    raw = SQLiteConnector(str(tmp_path / "async"))

    async def child() -> None:
        assert instrument_connector(raw) is raw
        describe_ingest_step(operation="CHILD", generation=2)

    async def run() -> None:
        with performance.step("publication", "prepare", "BUILD_CATALOG", 1) as sample:
            await asyncio.create_task(child())
            assert sample.operation == "BUILD_CATALOG"
            assert sample.generation == 1

    asyncio.run(run())
    performance.close()


def test_info_explains_slow_connections_and_debug_preserves_all_stage_measurements(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(
        performance_log, backend="mariadb", level=logging.DEBUG, clock=clock
    )
    with performance.step("analysis", "prepare", "PREPARE_SNAPSHOT", 51) as sample:
        sample.record_sql_operation("sql", 120.0, "SELECT secret", 128)
        sample.record_sql_operation("connection", 1800.0, "", 0)
        sample.record_sql_operation("transaction", 60.0, "", 0)
        sample.processed_rows = 4
        sample.replayed = True
        sample.terminal = True
        clock.now = 2000.0
    info = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
    ]
    technical = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG
    ]
    assert len(info) == 2
    assert (
        info[0]
        == "Ingest analysis stage started: preparing the analysis snapshot; ingest generation 51."
    )
    assert info[1].startswith(
        "Ingest analysis stage finished: preparing the analysis snapshot; ingest generation 51; "
        "elapsed 33m 20s; database work 33m 00s "
        "(queries 2m 00s, connections 30m 00s, transaction boundaries 1m 00s); "
        "includes reused results; 1 completed SQL connector calls; "
        "128 rows returned (not rows examined); phases prepare: 33m 20s, "
        "SQL 2m 00s, 1 SQL calls, 128 rows returned; slowest SQL calls "
    )
    assert all("PREPARE_SNAPSHOT" not in message for message in info)
    terminal = next(
        message for message in technical if "event=stage_terminal " in message
    )
    assert "pipeline=analysis operation=PREPARE_SNAPSHOT generation=51" in terminal
    assert "wall_seconds=2000.000000 call_seconds=2000.000000" in terminal
    assert "sql_seconds=120.000000" in terminal
    assert (
        "connection_seconds=1800.000000 transaction_calls=1 transaction_seconds=60.000000"
        in terminal
    )
    assert "processed_rows=4 replayed_calls=1" in terminal
    assert "sql_calls=1 sql_seconds=120.000000 read_rows=128" in terminal
    assert "query_top=" in next(
        message for message in technical if "event=completed " in message
    )
    assert "secret" not in caplog.text


@pytest.mark.parametrize(
    "error", [KeyboardInterrupt(), SystemExit(1), asyncio.CancelledError()]
)
def test_interrupted_step_never_reports_stage_success(
    caplog: pytest.LogCaptureFixture,
    performance_log: logging.Logger,
    error: BaseException,
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.DEBUG
    )
    with pytest.raises(type(error)):
        with performance.step("analysis", "prepare", "PREPARE_SNAPSHOT", 2):
            raise error
    performance.close()
    info = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
    ]
    assert any("stage interrupted:" in message for message in info)
    assert not any("finished" in message or "failed" in message for message in info)
    assert "event=interrupted " in caplog.text
    assert "event=stage_interrupted " in caplog.text
    assert "event=stage_terminal " not in caplog.text


def test_failure_and_early_close_are_distinct_from_success(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.DEBUG
    )
    with performance.step("analysis", "issue", "ISSUE", 3):
        pass
    performance.close()
    assert "stage reporting closed before completion was confirmed" in caplog.text
    assert "event=stage_closed" in caplog.text
    assert "stage finished" not in caplog.text
    assert "stage failed" not in caplog.text
    assert "issuing an analysis work request" in caplog.text


def test_info_nested_records_are_summarized_without_raw_per_call_dump(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(performance_log, backend="sqlite")
    with performance.step("publication", "prepare", "BUILD_CATALOG", 1):
        for _ in range(3):
            with performance.step("analysis", "prepare", "content_owner", 1):
                pass
            assert not caplog.records
    performance.close()
    assert len(caplog.records) == 3
    assert "nested work" in caplog.text
    assert "3 calls" not in caplog.text
    assert "scope=" not in caplog.text and "ingest_db_performance" not in caplog.text
    assert "selecting galleries for duplicate content" not in caplog.text


def test_unknown_operation_and_missing_clock_remain_readable_without_claimed_timing(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", clock=lambda: float("nan")
    )
    with performance.step(
        "unknown", "prepare", "PRIVATE_UNKNOWN_OPERATION", 7
    ) as sample:
        sample.terminal = True
    assert "processing ingest work; ingest generation 7" in caplog.text
    assert "elapsed unavailable" in caplog.text
    assert "PRIVATE_UNKNOWN_OPERATION" not in caplog.text
    assert "unknown" not in caplog.text


def test_every_actual_analysis_and_publication_stage_has_a_semantic_description() -> (
    None
):
    from h2hdb.ingest_performance_format import activity
    from h2hdb.vnext_analysis_repository import _STAGES
    from h2hdb.vnext_ingest_analysis import _ANALYSIS_SNAPSHOT_STAGE, _AnalysisAction
    from h2hdb.vnext_ingest_publication import _Action

    analysis = {
        *(stage.decode("ascii") for stage in _STAGES),
        *(action.value for action in _AnalysisAction),
        _ANALYSIS_SNAPSHOT_STAGE.decode("ascii"),
        "ISSUE",
        "PREPARE",
        "COMMIT",
    }
    publication = {
        *(action.value for action in _Action),
        "RECOVERY",
        "ISSUE",
        "PREPARE",
        "COMMIT",
    }
    for pipeline, operations in (("analysis", analysis), ("publication", publication)):
        for operation in operations:
            description = activity(pipeline, operation)
            assert not description.startswith("processing "), operation
            assert "_" not in description and "CBZ" not in description, operation
    assert (
        activity("analysis", "snapshot_manifest")
        == "saving and sealing the analysis snapshot"
    )


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, "0ms"),
        (0.023, "23ms"),
        (1.5, "1.5s"),
        (63, "1m 03s"),
        (4219, "1h 10m 19s"),
        (90000, "25h 00m 00s"),
        (None, "unavailable"),
    ],
)
def test_elapsed_format_preserves_readable_units(
    seconds: float | None, expected: str
) -> None:
    from h2hdb.ingest_performance_format import duration

    assert duration(seconds) == expected


def test_local_validation_preparation_reports_live_boundaries_and_separate_cost(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    from h2hdb.ingest_performance import prepare_ingest_operation

    clock = _Clock()
    performance = IngestPerformance(
        performance_log, backend="sqlite", clock=clock, level=logging.DEBUG
    )
    with performance.step(
        "analysis", "prepare", "validate_file_hash_decision", 9
    ) as outer:
        with prepare_ingest_operation(
            operation="prepare_file_decision_validation", generation=9
        ) as progress:
            assert (
                "preparation started: preparing file hash validation data"
                in caplog.text
            )
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "progress.db"))
            ) as connector:
                with connector.read_transaction():
                    assert connector.fetch_one("SELECT 1") == (1,)
            clock.now = 59
            progress(1)
            assert "preparation in progress" not in caplog.text
            clock.now = 60
            progress(2)
            assert (
                "preparation in progress: preparing file hash validation data"
                in caplog.text
            )
            assert "read 2 galleries; elapsed 1m 00s" in caplog.text
            clock.now = 61
            progress(2)
            assert caplog.text.count("preparation in progress:") == 1
            clock.now = 70
        clock.now = 72
        outer.terminal = True
    raw = [
        record.message for record in caplog.records if record.levelno == logging.DEBUG
    ]
    plan = next(
        line for line in raw if "event=completed " in line and "scope=nested" in line
    )
    assert "operation=prepare_file_decision_validation" in plan
    assert "elapsed_seconds=70.000000 call_seconds=70.000000" in plan
    assert "sql_calls=1 " in plan
    parent = next(
        line
        for line in raw
        if "event=completed " in line and "scope=sequential" in line
    )
    assert "call_seconds=2.000000" in parent
    assert "nested_seconds=70.000000" in parent
    assert "sql_calls=0 " in parent
    assert any(
        "event=stage_terminal" in line and "prepare_seconds=2.000000" in line
        for line in raw
    )
    human = [
        record.message for record in caplog.records if record.levelno == logging.INFO
    ]
    assert any(
        "preparation finished: preparing file hash validation data" in line
        and "elapsed 1m 10s" in line
        for line in human
    )
    assert all("sql_calls=" not in line and "SELECT 1" not in line for line in human)
    assert all("nested work" not in line for line in human)
    assert all("stage completion not confirmed" not in line for line in human)


def test_announced_preparation_does_not_hide_other_nested_failures(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    from h2hdb.ingest_performance import prepare_ingest_operation

    performance = IngestPerformance(performance_log, backend="sqlite")
    with performance.step("analysis", "prepare", "validate_file_hash_decision", 9):
        with prepare_ingest_operation(
            operation="prepare_file_decision_validation", generation=9
        ):
            pass
        with pytest.raises(RuntimeError, match="another nested failure"):
            with performance.step("analysis", "prepare", "snapshot_manifest", 9):
                raise RuntimeError("another nested failure")
    human = [
        record.message for record in caplog.records if record.levelno == logging.INFO
    ]
    assert any("nested call failed: saving and sealing" in line for line in human)
    assert any("nested work" in line for line in human)


@pytest.mark.parametrize(
    "failure", [RuntimeError("failed preparation"), KeyboardInterrupt()]
)
def test_local_validation_preparation_failure_is_never_reported_as_finished(
    caplog: pytest.LogCaptureFixture,
    performance_log: logging.Logger,
    failure: BaseException,
) -> None:
    from h2hdb.ingest_performance import prepare_ingest_operation

    performance = IngestPerformance(performance_log, backend="sqlite")
    with pytest.raises(type(failure)):
        with performance.step("analysis", "prepare", "validate_file_hash_decision", 9):
            with prepare_ingest_operation(
                operation="prepare_file_decision_validation", generation=9
            ):
                raise failure
    expected = "failed" if isinstance(failure, Exception) else "interrupted"
    assert f"preparation {expected}: preparing file hash validation data" in caplog.text
    assert "preparation finished:" not in caplog.text
