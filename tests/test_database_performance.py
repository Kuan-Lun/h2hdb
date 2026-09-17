"""Diagnostic attribution and bounds are independent of database authority."""

from __future__ import annotations

import gc
import json
import logging
import subprocess
import sys
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Event, get_ident
from typing import Any

import pytest

from h2hdb import database_performance as diagnostics
from h2hdb.database_performance import DatabasePerformance, database_phase
from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


def _records(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    return [
        json.loads(record.getMessage().removeprefix("database_performance "))
        for record in caplog.records
        if record.getMessage().startswith("database_performance ")
    ]


def _performance(
    caplog: pytest.LogCaptureFixture, **kwargs: Any
) -> DatabasePerformance:
    logger = logging.getLogger("h2hdb.database_performance.unit")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    return DatabasePerformance(logger, backend="sqlite", level=logging.DEBUG, **kwargs)


def test_nested_sql_conservation_and_known_delay_attribution(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    performance = _performance(caplog, clock=clock)
    fetch = SQLiteConnector.fetch_one

    def delayed(
        self: SQLiteConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = fetch(self, query, data)
        if query == "SELECT %s":
            clock.now += 2.0
        return result

    monkeypatch.setattr(SQLiteConnector, "fetch_one", delayed)
    with performance.operation("audit"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connection:
            connection.fetch_one("SELECT 1")
            with database_phase("outer"):
                connection.fetch_one("SELECT 2")
                with database_phase("slow_validator", validator="manifest.authority"):
                    assert connection.fetch_one("SELECT %s", ("private-value",)) == (
                        "private-value",
                    )
                assert not _records(caplog), (
                    "phase sinks must wait for the owning operation"
                )
    records = _records(caplog)
    terminal = records[-1]
    phases = [record for record in records if record["event"] == "phase"]
    slow = next(record for record in phases if record["phase"] == "slow_validator")
    assert terminal["sql_calls"] == 3
    assert terminal["sql_calls"] == terminal["exclusive_sql_calls"] + sum(
        record["exclusive_sql_calls"] for record in phases
    )
    assert terminal["sql_seconds"] == 2.0
    assert (
        slow["elapsed_seconds"]
        == slow["sql_seconds"]
        == slow["exclusive_seconds"]
        == 2.0
    )
    assert terminal["query_top"][0] == {
        "fingerprint": sha256(b"SELECT %s").hexdigest()[:16],
        "calls": 1,
        "seconds": 2.0,
        "max_seconds": 2.0,
        "returned_rows": 1,
    }
    assert "private-value" not in caplog.text
    assert "SELECT" not in caplog.text


@pytest.mark.parametrize("phase_count", [255, 256, 257, 520])
def test_phase_budget_retains_slow_and_failed_work_without_losing_totals(
    phase_count: int, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    clock = _Clock()
    performance = _performance(caplog, clock=clock)
    with performance.operation("cleanup"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connection:
            for index in range(phase_count):
                try:
                    with database_phase("batch", batch=index):
                        connection.fetch_one("SELECT %s", (index,))
                        if index == 0:
                            clock.now += 10.0
                        if index == phase_count - 1:
                            raise ValueError("private-error-content")
                except ValueError:
                    pass
    records = _records(caplog)
    phases = [record for record in records if record["event"] == "phase"]
    summary = records[-1]
    assert len(phases) == min(phase_count, 256)
    assert summary["phase_count"] == phase_count
    assert summary["omitted_phase_records"] == max(0, phase_count - 256)
    assert summary["sql_calls"] == phase_count
    assert any(record["labels"]["batch"] == 0 for record in phases)
    assert any(record["status"] == "failed" for record in phases)
    assert summary["event"] == "completed", (
        "handled child failure is not operation failure"
    )
    assert "private-error-content" not in caplog.text


@pytest.mark.parametrize("queries", [63, 64, 65, 130])
def test_query_capacity_and_repeated_cycles_keep_exact_total_counts(
    queries: int, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    performance = _performance(caplog)
    with performance.operation("audit") as span:
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connection:
            for _lap in range(3):
                for index in range(queries):
                    connection.fetch_one(f"SELECT %s AS q_{index}", ("private",))
        statistics = span._inclusive
        assert len(statistics.queries) == min(queries, 64) + int(queries > 64)
        assert sum(item.calls for item in statistics.queries.values()) == queries * 3
        if queries > 64:
            assert statistics.queries["other"].calls == (queries - 64) * 3
    assert _records(caplog)[-1]["sql_calls"] == queries * 3
    assert "private" not in caplog.text


def test_failure_preserves_exception_and_context_even_with_broken_sink(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    performance = _performance(caplog)

    def broken(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("sink unavailable")

    monkeypatch.setattr(performance.logger, "log", broken)
    original = ValueError("database work failed")
    with pytest.raises(ValueError) as caught:
        with performance.operation("audit"):
            with database_phase("validator"):
                raise original
    assert caught.value is original
    raw = SQLiteConnector(str(tmp_path / "db"))
    assert instrument_connector(raw) is raw
    assert not diagnostics._pulse_operations


def test_copied_context_does_not_attribute_another_thread_to_parent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    performance = _performance(caplog)

    def worker() -> None:
        with database_phase("unowned") as span:
            assert span._root is None
        with performance.operation("worker"):
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "worker"))
            ) as connection:
                connection.fetch_one("SELECT 1")

    with performance.operation("parent"):
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(copy_context().run, worker).result(timeout=10)
    completed = {
        record["operation"]: record
        for record in _records(caplog)
        if record["event"] == "completed"
    }
    assert completed["worker"]["sql_calls"] == 1
    assert completed["parent"]["sql_calls"] == 0


def test_long_operation_reports_active_phase_from_independent_thread(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    performance = _performance(caplog, interval_seconds=0.01)
    reported = Event()
    owner = get_ident()
    threads: list[int] = []

    class ProgressHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            value = json.loads(
                record.getMessage().removeprefix("database_performance ")
            )
            if (
                value["event"] == "progress"
                and value.get("active_phase") == "slow_validator"
            ):
                threads.append(get_ident())
                reported.set()

    handler = ProgressHandler()
    performance.logger.addHandler(handler)
    try:
        with performance.operation("audit"):
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "db"))
            ) as connection:
                with connection.read_transaction():
                    with database_phase("slow_validator"):
                        assert reported.wait(10), (
                            "heartbeat must not wait for transaction completion"
                        )
        assert threads and all(thread != owner for thread in threads)
        assert not diagnostics._pulse_operations
    finally:
        performance.logger.removeHandler(handler)


def test_logger_reentry_does_not_create_recursive_diagnostics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    performance = _performance(caplog)
    entries = 0

    class ReentrantHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            nonlocal entries
            entries += 1
            with performance.operation("logger_callback"):
                pass

    handler = ReentrantHandler()
    performance.logger.addHandler(handler)
    try:
        with performance.operation("outer"):
            pass
        assert entries == 1
        assert [record["operation"] for record in _records(caplog)] == ["outer"]
    finally:
        performance.logger.removeHandler(handler)


def test_disabled_diagnostics_leave_connector_unwrapped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    performance = _performance(caplog)
    performance.level = logging.WARNING
    with performance.operation("audit"):
        with database_phase("validator"):
            raw = SQLiteConnector(str(tmp_path / "db"))
            assert instrument_connector(raw) is raw
    assert not _records(caplog)


def test_nested_ingest_instrumentation_cannot_hide_sql_from_operation(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    performance = _performance(caplog)
    ingest = IngestPerformance(
        logging.getLogger("h2hdb.ingest_performance.unit"), backend="sqlite"
    )
    with performance.operation("cleanup"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connection:
            connection.fetch_one("SELECT 1")
            with database_phase("adapter"):
                with ingest.step("publication", "prepare", "PREPARE", 1):
                    connection.fetch_one("SELECT 2")
                    with ingest.step("publication", "prepare", "VALIDATE", 1):
                        connection.fetch_one("SELECT 3")
    records = _records(caplog)
    assert records[-1]["sql_calls"] == 3
    adapter = next(record for record in records if record["event"] == "phase")
    assert adapter["sql_calls"] == adapter["exclusive_sql_calls"] == 2
    assert records[-1]["exclusive_sql_calls"] == 1


def test_slow_quiet_completion_remains_visible_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock()
    performance = _performance(caplog, clock=clock, interval_seconds=60)
    with performance.operation("current_only_cleanup") as span:
        span.describe(quiet=True)
        clock.now = 61
    assert caplog.records[-1].levelno == logging.INFO
    assert _records(caplog)[-1]["elapsed_seconds"] == 61


def test_heartbeat_releases_completed_operation_snapshots(
    caplog: pytest.LogCaptureFixture,
) -> None:
    performance = _performance(caplog, interval_seconds=0.01)
    progress = Event()
    released = Event()

    class Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if '"event":"progress"' in record.getMessage():
                progress.set()

    handler = Handler()
    performance.logger.addHandler(handler)
    try:
        with performance.operation("audit") as span:
            reference = weakref.ref(span._root, lambda _reference: released.set())
            assert progress.wait(10)
        del span
        # Synchronize with the daemon's next idle wait without relying on a
        # fixed sleep: repeated collections permit the just-emitted pulse to
        # drop its local references, but the assertion has a generous deadline.
        for _attempt in range(100):
            gc.collect()
            if released.wait(0.01):
                break
        assert reference() is None
        assert not diagnostics._pulse_operations
    finally:
        performance.logger.removeHandler(handler)


@pytest.mark.parametrize("level", ["INFO", "DEBUG"])
def test_core_cli_routes_diagnostics_to_console_and_configured_file(
    level: str,
    tmp_path: Path,
) -> None:
    config = tmp_path / "config.json"
    log = tmp_path / "core.log"
    config.write_text(
        json.dumps(
            {
                "database": {"sql_type": "sqlite", "database": str(tmp_path / "db")},
                "logger": {"level": level, "file": str(log)},
            }
        )
    )
    for command in ("migrate", "check"):
        result = subprocess.run(
            [sys.executable, "-m", "h2hdb", command, "--config", str(config)],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    assert '"operation":"schema_check"' in result.stderr
    assert '"operation":"schema_check"' in log.read_text()
    assert ('"event":"phase"' in result.stderr) == (level == "DEBUG")


def test_inclusive_sql_timer_excludes_nested_observer_callback_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    fetch = SQLiteConnector.fetch_one

    @dataclass
    class Recorder:
        callback_seconds: float = 0.0
        sql_seconds: float = 0.0

        def record_sql_operation(
            self, category: str, elapsed: float, query: str, rows: int
        ) -> None:
            if category == "sql":
                self.sql_seconds += elapsed
                clock.now += self.callback_seconds

    def query(
        self: SQLiteConnector, statement: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        value = fetch(self, statement, data)
        clock.now += 2
        return value

    monkeypatch.setattr(SQLiteConnector, "fetch_one", query)
    outer, inner = Recorder(), Recorder(callback_seconds=3)
    with measure_sql(outer, clock=clock, observe_nested=True):
        with measure_sql(inner, clock=clock):
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "db"))
            ) as connection:
                connection.fetch_one("SELECT 1")
    assert inner.sql_seconds == outer.sql_seconds == 2


def test_progress_exclusive_time_excludes_active_nested_phase(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock()
    performance = _performance(caplog, clock=clock)
    with performance.operation("audit") as root_span:
        with database_phase("completed"):
            clock.now = 3
        clock.now = 4
        with database_phase("active"):
            clock.now = 5
            with database_phase("nested"):
                clock.now = 10
                assert root_span._root is not None
                root_span._root.progress()
    progress = next(
        record for record in _records(caplog) if record["event"] == "progress"
    )
    assert progress["elapsed_seconds"] == 10
    assert progress["exclusive_seconds"] == 1
    assert progress["active_phase_seconds"] == 5


def test_heartbeat_does_not_inherit_context_on_python_thread_inherit_mode() -> None:
    script = """
import gc
import logging
import time
import weakref
from contextvars import ContextVar
from threading import Event, current_thread
from h2hdb.database_performance import DatabasePerformance

marker = ContextVar('request_private_context', default='absent')
marker.set('private-context')
observations = []
sampled = Event()
def clock():
    if current_thread().name == 'h2hdb-database-diagnostics':
        observations.append(marker.get())
        sampled.set()
    return time.perf_counter()
performance = DatabasePerformance(logging.getLogger('test'), backend='sqlite',
    level=logging.INFO, clock=clock, interval_seconds=0.01)
with performance.operation('audit') as span:
    reference = weakref.ref(span._root)
    assert sampled.wait(5)
del span
for attempt in range(100):
    gc.collect()
    if reference() is None:
        break
    time.sleep(0.01)
assert observations and set(observations) == {'absent'}, observations
assert reference() is None, 'completed operation retained by daemon context'
"""
    subprocess.run(
        [sys.executable, "-X", "thread_inherit_context=1", "-c", script],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
