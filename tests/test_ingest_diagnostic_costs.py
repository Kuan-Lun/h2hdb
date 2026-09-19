"""Cumulative small calls and transaction waits remain visible at INFO."""

from __future__ import annotations

import json
import logging
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from h2hdb.database_performance import DatabasePerformance, database_phase
from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import SQLTransactionStatistics, instrument_connector
from h2hdb.sqlite_connector import SQLiteConnector


def test_info_aggregates_short_queries_across_steps_and_reports_overflow(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("diagnostic-costs.ingest")
    caplog.set_level(logging.INFO, logger=logger.name)
    performance = IngestPerformance(logger, backend="sqlite", clock=lambda: 0.0)
    repeated = "SELECT private_authority WHERE secret = %s"
    for cycle in range(3):
        with performance.step("analysis", "issue", "VALIDATE_FILE_HASH", 3) as step:
            for _ in range(100):
                step.record_sql_operation("sql", 0.01, repeated, 1)
            if cycle == 0:
                step.record_sql_operation("sql", 2.0, "SELECT one_slow_query", 1)
            # These are fast enough that their overflow would not make top 5.
            for index in range(70):
                step.record_sql_operation("sql", 0.0, f"SELECT private_{index}", 0)
            step.record_sql_operation("transaction", 0.25, "begin", 0)
            step.record_sql_operation("transaction", 2.0, "commit", 0)
            assert len(step.queries) <= 65
    performance.close()
    message = next(
        record.getMessage()
        for record in caplog.records
        if "reporting closed" in record.getMessage()
    )
    key = sha256(repeated.encode()).hexdigest()[:16]
    assert f"{key}(calls=300,seconds=3.000000" in message
    assert "cumulative SQL overflow other(calls=" in message
    assert "transaction operations begin(calls=3,seconds=0.750000" in message
    assert "commit(calls=3,seconds=6.000000,max_seconds=2.000000)" in message
    assert "transaction operations by phase issue:" in message
    assert "private" not in caplog.text and "secret" not in caplog.text


def test_real_transaction_error_and_nested_boundaries_are_conserved(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = logging.getLogger("diagnostic-costs.database")
    caplog.set_level(logging.INFO, logger=logger.name)
    now = 0.0
    original_commit = SQLiteConnector.commit
    original_rollback = SQLiteConnector.rollback

    def commit(self: SQLiteConnector) -> None:
        nonlocal now
        now += 7.0
        original_commit(self)

    def rollback(self: SQLiteConnector) -> None:
        nonlocal now
        now += 2.0
        original_rollback(self)

    monkeypatch.setattr(SQLiteConnector, "commit", commit)
    monkeypatch.setattr(SQLiteConnector, "rollback", rollback)
    performance = DatabasePerformance(
        logger, backend="sqlite", level=logging.INFO, clock=lambda: now
    )
    with performance.operation("acceptance"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as db:
            with db.transaction():
                db.execute("CREATE TABLE rows (id INTEGER PRIMARY KEY)")
            with database_phase("failed_write"), pytest.raises(RuntimeError):
                with db.transaction():
                    db.execute("INSERT INTO rows VALUES (1)")
                    raise RuntimeError("secret failure")
            with db.read_transaction():
                assert db.fetch_all("SELECT * FROM rows") == []
    records: list[dict[str, Any]] = [
        json.loads(record.getMessage().removeprefix("database_performance "))
        for record in caplog.records
    ]
    terminal = records[-1]
    boundaries = {row["operation"]: row for row in terminal["transaction_breakdown"]}
    assert (
        sum(row["calls"] for row in boundaries.values())
        == terminal["transaction_calls"]
    )
    assert (
        sum(row["seconds"] for row in boundaries.values())
        == terminal["transaction_seconds"]
    )
    assert boundaries["commit"]["seconds"] == 14.0
    assert boundaries["rollback"]["seconds"] == 2.0
    phase = next(
        row for row in terminal["phase_totals"] if row["phase"] == "failed_write"
    )
    assert (
        sum(row["seconds"] for row in phase["exclusive_transaction_breakdown"]) == 2.0
    )
    assert terminal["query_top"]
    assert "secret failure" not in caplog.text


def test_transaction_labels_remain_bounded_and_unknown_names_are_redacted() -> None:
    counters = SQLTransactionStatistics()
    for cycle in range(3):
        for operation in ("begin", "begin_read", "commit", "rollback"):
            counters.record(operation, 0.25)
        for index in range(130):
            counters.record(f"private-{cycle}-{index}", 0.5)
    assert len(counters.operations) == 5
    assert counters.operations["other"].calls == 390
    assert "private" not in counters.text()


@pytest.mark.parametrize(
    "failure", (None, RuntimeError("private-error"), KeyboardInterrupt())
)
def test_info_nested_preparation_preserves_cumulative_sql_and_transaction_details(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException | None,
) -> None:
    """Real nested connector calls stay visible without folding them into parent SQL."""
    from contextlib import nullcontext

    from h2hdb.ingest_performance import prepare_ingest_operation

    logger = logging.getLogger("diagnostic-costs.nested-preparation")
    caplog.set_level(logging.INFO, logger=logger.name)
    now = 0.0
    query = "SELECT %s AS private_authority"
    original_fetch = SQLiteConnector.fetch_one
    original_begin = SQLiteConnector.begin_read
    original_commit = SQLiteConnector.commit

    def fetch(
        self: SQLiteConnector, sql: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        nonlocal now
        if sql == query:
            now += 0.01
        return original_fetch(self, sql, data)

    def begin(self: SQLiteConnector) -> None:
        nonlocal now
        now += 0.25
        original_begin(self)

    def commit(self: SQLiteConnector) -> None:
        nonlocal now
        now += 2.0
        original_commit(self)

    monkeypatch.setattr(SQLiteConnector, "fetch_one", fetch)
    monkeypatch.setattr(SQLiteConnector, "begin_read", begin)
    monkeypatch.setattr(SQLiteConnector, "commit", commit)
    performance = IngestPerformance(
        logger, backend="sqlite", level=logging.INFO, clock=lambda: now
    )
    expectation = pytest.raises(type(failure)) if failure is not None else nullcontext()
    with (
        expectation,
        SQLiteConnector(str(tmp_path / "nested.db")) as raw_connector,
    ):
        with performance.step(
            "analysis", "prepare", "validate_file_hash_decision", 9
        ) as parent:
            with prepare_ingest_operation(
                operation="prepare_file_decision_validation", generation=9
            ) as progress:
                connector = instrument_connector(raw_connector)
                for cycle in range(2):
                    with connector.read_transaction():
                        for _ in range(100):
                            assert connector.fetch_one(query, (17,)) == (17,)
                        if cycle == 0:
                            for index in range(70):
                                assert connector.fetch_one(
                                    f"SELECT {index} AS private_{index}"
                                ) == (index,)
                    now += 61.0
                    progress(cycle + 1)
                if failure is not None:
                    raise failure
            assert parent.counters.sql_calls == 0
            assert parent.counters.transaction_calls == 0
            parent.terminal = True
    performance.close()
    assert parent.counters.sql_calls == 0
    assert parent.counters.transaction_calls == 0
    messages = [record.getMessage() for record in caplog.records]
    key = sha256(query.encode()).hexdigest()[:16]
    progress_messages = [
        message for message in messages if "preparation in progress:" in message
    ]
    assert len(progress_messages) == 2
    assert f"{key}(calls=100,seconds=1.000000" in progress_messages[0]
    assert f"{key}(calls=200,seconds=2.000000" in progress_messages[1]
    event = (
        "finished"
        if failure is None
        else "failed"
        if isinstance(failure, Exception)
        else "interrupted"
    )
    terminal = next(
        message for message in messages if f"preparation {event}:" in message
    )
    assert (
        f"{key}(calls=200,seconds=2.000000,returned_rows=200,max_seconds=0.010000)"
        in terminal
    )
    assert "cumulative SQL overflow other(calls=7,seconds=0.000000" in terminal
    assert (
        "transaction operations begin_read(calls=2,seconds=0.500000,max_seconds=0.250000)"
        in terminal
    )
    assert "commit(calls=2,seconds=4.000000,max_seconds=2.000000)" in terminal
    assert "private" not in caplog.text
    assert all(record.levelno == logging.INFO for record in caplog.records)
    if failure is not None:
        assert "preparation finished:" not in caplog.text
    else:
        parent_terminal = next(
            message for message in messages if "stage finished:" in message
        )
        assert key not in parent_terminal
        assert "0 completed SQL connector calls" in parent_terminal
