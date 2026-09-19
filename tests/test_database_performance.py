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
    caplog: pytest.LogCaptureFixture, *, level: int = logging.DEBUG, **kwargs: Any
) -> DatabasePerformance:
    logger = logging.getLogger("h2hdb.database_performance.unit")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    return DatabasePerformance(logger, backend="sqlite", level=level, **kwargs)


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


@pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
def test_additive_diagnostics_preserve_schema_one_field_meaning(
    level: int,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing consumers can project the same fields at either log level.

    One connector fetch returns three rows, so this also rejects confusing
    calls with returned rows. INFO now includes the same bounded cumulative
    fingerprints as DEBUG; counts retain their meaning at both levels.
    """
    clock = _Clock()
    performance = _performance(caplog, level=level, clock=clock)
    fetch = SQLiteConnector.fetch_all
    query = "SELECT %s UNION ALL SELECT %s UNION ALL SELECT %s"

    def delayed(
        self: SQLiteConnector, sql: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        rows = fetch(self, sql, data)
        clock.now += 2.0
        return rows

    monkeypatch.setattr(SQLiteConnector, "fetch_all", delayed)
    with performance.operation("audit", fixture="contract"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "v1.db"))) as db:
            with db.read_transaction():
                assert db.fetch_all(query, (1, 2, 3)) == [(1,), (2,), (3,)]
                with database_phase("component", validator="contract"):
                    assert db.fetch_all(query, (1, 2, 3)) == [(1,), (2,), (3,)]
                assert db.fetch_all(query, (1, 2, 3)) == [(1,), (2,), (3,)]
    terminal = _records(caplog)[-1]
    previous_fields = {
        "schema": 1,
        "event": "completed",
        "backend": "sqlite",
        "operation": "audit",
        "sequence": 2,
        "phase": "audit",
        "span_id": 0,
        "parent_id": None,
        "labels": {"fixture": "contract"},
        "elapsed_seconds": 6.0,
        "exclusive_seconds": 4.0,
        "sql_calls": 3,
        "sql_seconds": 6.0,
        "read_rows": 9,
        "connection_calls": 2,
        "transaction_calls": 2,
        "exclusive_sql_calls": 2,
        "exclusive_sql_seconds": 4.0,
        "exclusive_read_rows": 6,
        "phase_count": 1,
        "omitted_phase_records": 0,
        "omitted_depth": 0,
        "query_top": [
            {
                "fingerprint": sha256(query.encode()).hexdigest()[:16],
                "calls": 3,
                "seconds": 6.0,
                "max_seconds": 2.0,
                "returned_rows": 9,
            }
        ],
    }
    assert {key: terminal[key] for key in previous_fields} == previous_fields
    previous_phase_fields = {
        "phase": "component",
        "span_id": 1,
        "labels": {"validator": "contract"},
        "elapsed_seconds": 2.0,
        "exclusive_seconds": 2.0,
        "status": "completed",
    }
    assert {
        key: terminal["phase_top"][0][key] for key in previous_phase_fields
    } == previous_phase_fields
    assert terminal["sql_calls_unit"] == "completed_connector_method_calls"
    assert terminal["read_rows_unit"] == "returned_rows_not_examined_rows"
    assert terminal["pending_call"] is None
    assert len(terminal["query_slowest"]) == 3
    assert terminal["phase_totals"][0]["exclusive_sql_calls"] == 1


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


def test_quiet_completion_does_not_build_suppressed_snapshots(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    performance = _performance(caplog, level=logging.INFO, clock=_Clock())
    snapshots: list[bool] = []
    original = diagnostics._Statistics.snapshot

    def snapshot(self: Any) -> dict[str, Any]:
        snapshots.append(True)
        return original(self)

    monkeypatch.setattr(diagnostics._Statistics, "snapshot", snapshot)
    with performance.operation("source_step", quiet=True) as span:
        root = span._root
    assert not snapshots
    assert not _records(caplog)
    assert root is not None and not root.active
    assert not diagnostics._pulse_operations


def test_unknown_clock_cannot_classify_quiet_work_as_fast(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def broken_clock() -> float:
        raise RuntimeError("clock failed")

    performance = _performance(caplog, level=logging.INFO, clock=broken_clock)
    with performance.operation("source_step", quiet=True):
        pass
    assert _records(caplog)[-1]["event"] == "completed"
    assert _records(caplog)[-1]["timing_available"] is False


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


def test_info_heartbeat_identifies_blocked_sql_before_completion(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    performance = _performance(
        caplog, level=logging.INFO, clock=clock, interval_seconds=0.01
    )
    reported = Event()
    reporting_threads: list[int] = []
    fetch = SQLiteConnector.fetch_one

    def blocked(
        self: SQLiteConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        clock.now = 12.0
        assert reported.wait(10), "pending SQL must appear before driver returns"
        return fetch(self, query, data)

    class Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            payload = json.loads(
                record.getMessage().removeprefix("database_performance ")
            )
            pending = payload.get("pending_call")
            if isinstance(pending, dict) and pending.get("category") == "sql":
                reporting_threads.append(get_ident())
                reported.set()

    handler = Handler()
    performance.logger.addHandler(handler)
    monkeypatch.setattr(SQLiteConnector, "fetch_one", blocked)
    try:
        with performance.operation("claim"):
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "db"))
            ) as connector:
                with connector.read_transaction(), database_phase("maintenance_proof"):
                    assert connector.fetch_one("SELECT %s", ("private-payload",)) == (
                        "private-payload",
                    )
        progress = next(
            record
            for record in _records(caplog)
            if record["event"] == "progress" and record["pending_call"] is not None
        )
        assert progress["active_phase"] == "maintenance_proof"
        assert progress["sql_calls"] == progress["sql_seconds"] == 0
        assert progress["pending_call"] == {
            "category": "sql",
            "operation": "query",
            "fingerprint": sha256(b"SELECT %s").hexdigest()[:16],
            "age_seconds": 12.0,
        }
        terminal = _records(caplog)[-1]
        assert terminal["sql_calls"] == 1
        assert terminal["sql_seconds"] == 12.0
        assert terminal["pending_call"] is None
        assert terminal["query_top"][0]["calls"] == 1
        assert terminal["query_slowest"][0]["seconds"] == 12.0
        assert terminal["phase_top"][0]["sql_seconds"] == 12.0
        assert reporting_threads and all(
            thread != get_ident() for thread in reporting_threads
        )
        assert "private-payload" not in caplog.text
        assert "SELECT" not in caplog.text
    finally:
        performance.logger.removeHandler(handler)


@pytest.mark.parametrize("queries", [63, 64, 65, 130])
@pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
def test_slow_queries_after_fingerprint_capacity_remain_identifiable(
    queries: int,
    level: int,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    performance = _performance(caplog, level=level, clock=clock)
    fetch = SQLiteConnector.fetch_one

    def delayed(
        self: SQLiteConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = fetch(self, query, data)
        clock.now += 9.0 if data[0] == queries - 1 else 8.0 if data[0] == 64 else 0.0
        return result

    monkeypatch.setattr(SQLiteConnector, "fetch_one", delayed)
    with performance.operation("audit") as span:
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connector:
            for _cycle in range(3):
                for index in range(queries):
                    connector.fetch_one(f"SELECT %s AS q_{index}", (index,))
        assert len(span._inclusive.slowest._entries) == 5
    terminal = _records(caplog)[-1]
    slow = terminal["query_slowest"]
    expected = sha256(f"SELECT %s AS q_{queries - 1}".encode()).hexdigest()[:16]
    assert [item["fingerprint"] for item in slow[:3]] == [expected] * 3
    if queries == 130:
        assert [item["fingerprint"] for item in slow[3:]] == [
            sha256(b"SELECT %s AS q_64").hexdigest()[:16]
        ] * 2
    assert terminal["sql_calls"] == terminal["read_rows"] == queries * 3
    assert terminal["query_top"]
    assert terminal["query_overflow"]["calls"] == max(0, queries - 64) * 3


@pytest.mark.parametrize("keys", [63, 64, 65, 130])
def test_phase_totals_bound_dimensions_and_preserve_repeated_work(
    keys: int, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    clock = _Clock()
    performance = _performance(caplog, clock=clock)
    with performance.operation("cleanup"):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connector:
            for _cycle in range(3):
                for index in range(keys):
                    with database_phase(
                        "eligibility", target=f"target_{index}", phase="select"
                    ):
                        connector.fetch_one("SELECT 1")
                        clock.now += 1.0
    totals = _records(caplog)[-1]["phase_totals"]
    assert len(totals) == min(keys, 64) + int(keys > 64)
    assert sum(item["calls"] for item in totals) == keys * 3
    assert sum(item["exclusive_seconds"] for item in totals) == keys * 3
    assert sum(item["exclusive_sql_calls"] for item in totals) == keys * 3
    assert sum(item["exclusive_read_rows"] for item in totals) == keys * 3
    assert all(item["calls"] == 3 for item in totals if item["phase"] != "other")
    if keys > 64:
        overflow = next(item for item in totals if item["phase"] == "other")
        assert overflow["calls"] == (keys - 64) * 3


def test_finish_failure_releases_pending_scope_and_heartbeat(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    performance = _performance(caplog)

    def broken(self: Any, *args: Any) -> None:
        raise RuntimeError("diagnostic completion failed")

    monkeypatch.setattr(diagnostics._Operation, "finish", broken)
    original = ValueError("original failure")
    with pytest.raises(ValueError) as caught:
        with performance.operation("audit") as span:
            root = span._root
            assert root is not None
            assert root.measurement is not None
            raise original
    assert caught.value is original
    assert root is not None
    assert root.measurement is None
    assert not root.active
    assert not diagnostics._pulse_operations
