from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pytest

from h2hdb.config_loader import CoreConfig, DatabaseConfig, LoggerConfig
from h2hdb.ingest_performance import IngestPerformance, instrument_connector
from h2hdb.repository import RepositoryContext
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
    assert "sql_calls=5" in text
    assert "read_rows=3" in text
    assert "connection_calls=2" in text
    assert "transaction_calls=2" in text
    assert "processed_rows=2" in text
    assert "commit_seconds=2.000000" in text
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
    assert "event=failed" in caplog.text
    assert "sql_calls=2" in caplog.text
    assert "transaction_calls=2" in caplog.text


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
        assert sum(count for count, _ in sample.queries.values()) == 100
    performance.close()
    assert "query_top=" in caplog.text
    assert "secret-payload" not in caplog.text
    assert "SELECT" not in caplog.text
    assert "event=completed" in caplog.text


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
    assert "event=stage_progress" in caplog.text
    assert "processed_rows=12800" in caplog.text
    with performance.step("publication", "prepare", "VALIDATE_CATALOG", 1):
        pass
    performance.close()
    assert len(caplog.records) == 5


def test_in_progress_metrics_include_completed_sql_within_long_step(
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    clock = _Clock()
    performance = IngestPerformance(performance_log, backend="sqlite", clock=clock)
    with performance.step("publication", "prepare", "VALIDATE_CATALOG", 1) as sample:
        clock.now = 61.0
        sample.record("sql", 55.0, "SELECT private_literal", 128)
        assert "event=in_progress" in caplog.text
        assert "sql_seconds=55.000000" in caplog.text
        assert "other_seconds=6.000000" in caplog.text
        clock.now = 62.0
        sample.record("sql", 1.0, "SELECT private_literal", 1)
        assert len(caplog.records) == 1
    assert "private_literal" not in caplog.text


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
    caplog: pytest.LogCaptureFixture, performance_log: logging.Logger
) -> None:
    performance = IngestPerformance(
        performance_log, backend="sqlite", level=logging.ERROR
    )
    with performance.step("publication", "commit", "BUILD_CATALOG", 1):
        pass
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
