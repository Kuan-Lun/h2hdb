"""Pending-call lifetime and bounded sampling are independent of SQL outcomes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from h2hdb import sql_performance
from h2hdb.sql_performance import (
    SQLQueryStatistics,
    SQLSlowQueries,
    accumulate_query,
    instrument_connector,
    measure_sql,
    query_fingerprint,
)
from h2hdb.sqlite_connector import SQLiteConnector


@dataclass
class _Recorder:
    observations: list[tuple[str, float, str, int]] = field(default_factory=list)
    broken: bool = False

    def record_sql_operation(
        self, category: str, elapsed: float, query: str, rows: int
    ) -> None:
        if self.broken:
            raise RuntimeError("diagnostic recorder failed")
        self.observations.append((category, elapsed, query, rows))


@pytest.mark.parametrize(
    ("category", "method"),
    [
        ("connection", "connect"),
        ("connection", "close"),
        ("transaction", "begin"),
        ("transaction", "begin_read"),
        ("transaction", "commit"),
        ("transaction", "rollback"),
        ("sql", "fetch_one"),
    ],
)
def test_pending_lifetime_and_completed_call_semantics(
    category: str, method: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = _Recorder()
    raw = SQLiteConnector(str(tmp_path / "unused"))
    now = 2.0
    statement = "SELECT %s"

    with measure_sql(recorder, clock=lambda: now) as measurement:

        def action(*args: Any) -> tuple[int, ...]:
            nonlocal now
            now = 5.0
            assert recorder.observations == [], "pending work is not completed work"
            assert measurement.pending_snapshot(now) == {
                "category": category,
                "operation": "query" if category == "sql" else method,
                "fingerprint": sha256(statement.encode()).hexdigest()[:16]
                if category == "sql"
                else None,
                "age_seconds": 3.0,
            }
            return (1,)

        monkeypatch.setattr(raw, method, action)
        connector = instrument_connector(raw)
        getattr(connector, method)(
            statement, ("private",)
        ) if category == "sql" else getattr(connector, method)()
        assert measurement.pending_snapshot(now) is None
    assert recorder.observations == [
        (category, 3.0, statement if category == "sql" else method, 1)
    ]
    assert measurement.pending_snapshot(now) is None


def test_nested_inclusive_scopes_and_reentrant_calls_restore_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = SQLiteConnector(str(tmp_path / "unused"))
    outer, middle, inner = _Recorder(), _Recorder(), _Recorder()
    with (
        measure_sql(outer, observe_nested=True) as included,
        measure_sql(middle) as excluded,
        measure_sql(inner) as current,
    ):
        connector = instrument_connector(raw)

        def action(query: str, data: tuple[Any, ...] = ()) -> tuple[int, ...]:
            pending = current.pending_snapshot(100.0)
            assert pending is not None
            assert pending["fingerprint"] == sha256(query.encode()).hexdigest()[:16]
            assert included.pending_snapshot(100.0) is not None
            assert excluded.pending_snapshot(100.0) is None
            if query == "SELECT 1":
                connector.fetch_one("SELECT 2")
                assert current.pending_snapshot(100.0) == pending
            return (1,)

        monkeypatch.setattr(raw, "fetch_one", action)
        connector.fetch_one("SELECT 1")
        assert current.pending_snapshot(100.0) is None
        assert included.pending_snapshot(100.0) is None
    assert len(outer.observations) == len(inner.observations) == 2
    assert middle.observations == []


def test_foreign_thread_and_task_cannot_replace_owned_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = SQLiteConnector(str(tmp_path / "unused"))
    recorder = _Recorder()
    with measure_sql(recorder, clock=lambda: 0.0) as measurement:
        connector = instrument_connector(raw)

        def action(query: str, data: tuple[Any, ...] = ()) -> tuple[int, ...]:
            pending = measurement.pending_snapshot(1.0)
            assert pending is not None
            assert pending["fingerprint"] == sha256(b"SELECT 1").hexdigest()[:16]
            if query == "SELECT 1":
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(
                        copy_context().run, connector.fetch_one, "SELECT 2"
                    ).result(timeout=10)

                async def child() -> None:
                    connector.fetch_one("SELECT 3")

                async def run() -> None:
                    await asyncio.create_task(child())

                asyncio.run(run())
                assert measurement.pending_snapshot(1.0) == pending
            return (1,)

        monkeypatch.setattr(raw, "fetch_one", action)
        connector.fetch_one("SELECT 1")
    assert len(recorder.observations) == 1


@pytest.mark.parametrize(
    "error", [ValueError("SQL failure"), KeyboardInterrupt(), asyncio.CancelledError()]
)
@pytest.mark.parametrize("broken_clock", [False, True])
def test_pending_clears_after_error_cancel_and_diagnostic_failure(
    error: BaseException,
    broken_clock: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def clock() -> float:
        if broken_clock:
            raise RuntimeError("diagnostic clock failed")
        return 4.0

    raw = SQLiteConnector(str(tmp_path / "unused"))
    recorder = _Recorder(broken=True)
    with measure_sql(recorder, clock=clock) as measurement:

        def action(query: str, data: tuple[Any, ...] = ()) -> tuple[int, ...]:
            snapshot = measurement.pending_snapshot(7.0)
            assert snapshot is not None
            assert snapshot["age_seconds"] == (None if broken_clock else 3.0)
            raise error

        monkeypatch.setattr(raw, "fetch_one", action)
        with pytest.raises(type(error)) as caught:
            instrument_connector(raw).fetch_one("SELECT private", ("private",))
        assert caught.value is error
        assert measurement.pending_snapshot(7.0) is None
    assert not measurement.active


@pytest.mark.parametrize("count", [4, 5, 6, 65, 130])
def test_slowest_capacity_cycles_and_degraded_first_five_counterexample(
    count: int,
) -> None:
    sampler = SQLSlowQueries()
    observed: list[tuple[float, str, int]] = []
    for lap in range(3):
        for index in range(count):
            elapsed = float(index + lap * count)
            fingerprint = sha256(str(index).encode()).hexdigest()[:16]
            sampler.record(fingerprint, elapsed, index)
            observed.append((elapsed, fingerprint, index))
            assert len(sampler._entries) <= 5

    def verify(samples: list[dict[str, str | float | int]]) -> None:
        assert samples == [
            {"seconds": elapsed, "fingerprint": fingerprint, "returned_rows": rows}
            for elapsed, fingerprint, rows in sorted(observed, reverse=True)[:5]
        ]

    verify(sampler.snapshot())
    degraded = SQLSlowQueries()
    for elapsed, fingerprint, rows in observed[:5]:
        degraded.record(fingerprint, elapsed, rows)
    with pytest.raises(AssertionError):
        verify(degraded.snapshot())


def test_fingerprint_failure_cannot_prevent_connector_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_hash(*args: Any) -> None:
        raise RuntimeError("diagnostic hash failed")

    monkeypatch.setattr(sql_performance, "sha256", broken_hash)
    recorder = _Recorder()
    with measure_sql(recorder):
        with instrument_connector(SQLiteConnector(str(tmp_path / "db"))) as connector:
            assert connector.fetch_one("SELECT 1") == (1,)
    assert sum(category == "sql" for category, *_rest in recorder.observations) == 1


@pytest.mark.parametrize(
    "clause",
    ["IN (%s)", "in(%s,%s)", "In ( %s,\n%s , %s )"],
)
def test_placeholder_only_in_arity_has_one_fingerprint(clause: str) -> None:
    query = f"SELECT member FROM objects WHERE member {clause} AND parent = %s"
    canonical = "SELECT member FROM objects WHERE member IN (%s) AND parent = %s"
    assert query_fingerprint(query) == sha256(canonical.encode()).hexdigest()[:16]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'IN (%s,%s)'",
        'SELECT "IN (%s,%s)"',
        "SELECT `IN (%s,%s)`",
        "SELECT [IN (%s,%s)]",
        "SELECT 'escaped '' IN (%s,%s)'",
        r"SELECT 'escaped \' IN (%s,%s)'",
        "SELECT 'unterminated IN (%s,%s)",
        "SELECT 'unterminated IN (%s,%s) \\",
        "SELECT 1 -- IN (%s,%s)\n",
        "SELECT 1 # IN (%s,%s)\n",
        "SELECT 1 /* IN (%s,%s) */",
        "SELECT 1 /* unterminated IN (%s,%s)",
        "SELECT member FROM objects WHERE member IN (SELECT %s,%s)",
        "SELECT member FROM objects WHERE member IN (%s, 1)",
        "SELECT member FROM objects WHERE member IN (%s + %s)",
        "SELECT member FROM objects WHERE member IN ((%s, %s))",
        "SELECT member FROM objects WHERE member IN (%s /* private */, %s)",
        "SELECT member FROM objects WHERE member IN (%S, %S)",
        "SELECT member FROM objects WHERE member IN ()",
        "SELECT customIN(%s,%s)",
        "SELECT custom$IN(%s,%s)",
    ],
)
def test_fingerprint_preserves_protected_or_non_placeholder_syntax(query: str) -> None:
    assert query_fingerprint(query) == sha256(query.encode()).hexdigest()[:16]


def test_fingerprint_normalizes_independent_lists_outside_protected_text() -> None:
    prefix = "SELECT 'IN (%s,%s)', `IN (%s,%s)` /* IN (%s,%s) */ FROM objects "
    query = prefix + "WHERE member IN (%s,%s) AND parent NOT IN (%s,%s,%s)"
    canonical = prefix + "WHERE member IN (%s) AND parent NOT IN (%s)"
    assert query_fingerprint(query) == sha256(canonical.encode()).hexdigest()[:16]
    assert query_fingerprint(canonical.replace("NOT IN", "IN")) != query_fingerprint(
        query
    )


@pytest.mark.parametrize("arities", [63, 64, 65, 127, 128, 129, 260])
def test_in_arity_capacity_cycles_reject_raw_hash_counterexample(arities: int) -> None:
    """Three laps, one family: exact calls/rows/time at every old-budget boundary.

    Counts model completed connector calls, not database work or server rows.
    Retaining the old raw-query hash deliberately violates the family contract.
    """

    def collect(
        fingerprint: Callable[[str], str | None],
    ) -> dict[str, SQLQueryStatistics]:
        queries: dict[str, SQLQueryStatistics] = {}
        for _cycle in range(3):
            for count in range(1, arities + 1):
                placeholders = ", ".join(["%s"] * count)
                key = fingerprint(f"SELECT %s IN ({placeholders})")
                assert key is not None
                statistics = SQLQueryStatistics()
                statistics.record(0.5, 1)
                accumulate_query(queries, key, statistics)
                assert len(queries) <= 65
        return queries

    def verify(queries: dict[str, SQLQueryStatistics]) -> None:
        assert len(queries) == 1
        assert "other" not in queries
        counts = next(iter(queries.values()))
        assert counts.calls == counts.read_rows == arities * 3
        assert counts.seconds == arities * 1.5
        assert counts.max_seconds == 0.5

    verify(collect(query_fingerprint))
    with pytest.raises(AssertionError):
        verify(collect(lambda query: sha256(query.encode()).hexdigest()[:16]))
