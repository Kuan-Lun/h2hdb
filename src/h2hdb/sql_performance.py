"""Scope-owned SQL measurements without logging or ingest dependencies."""

from __future__ import annotations

from asyncio import current_task
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from math import isfinite
from threading import get_ident
from time import perf_counter
from typing import Any, Literal

from .ports import _SQLPerformanceRecorder
from .sql_connector import SQLConnector


def execution_owner() -> tuple[int, object | None]:
    """Copied contexts cannot make another thread/task own an active scope."""
    try:
        task = current_task()
    except RuntimeError:
        task = None
    return get_ident(), task


def read_clock(clock: Callable[[], float]) -> float | None:
    """A diagnostic clock failure must not replace the measured outcome."""
    try:
        value = clock()
        return value if isfinite(value) else None
    except Exception:
        return None


@dataclass
class _MeasurementScope:
    recorder: _SQLPerformanceRecorder
    clock: Callable[[], float]
    owner: tuple[int, object | None]
    active: bool = True


_active_scope: ContextVar[_MeasurementScope | None] = ContextVar(
    "h2hdb_sql_performance", default=None
)


def _current_scope() -> _MeasurementScope | None:
    scope = _active_scope.get()
    if scope is None or not scope.active or scope.owner != execution_owner():
        return None
    return scope


@contextmanager
def measure_sql(
    recorder: _SQLPerformanceRecorder,
    *,
    clock: Callable[[], float] = perf_counter,
) -> Iterator[None]:
    """Measure only synchronous calls owned by this live execution scope."""
    scope = _MeasurementScope(recorder, clock, execution_owner())
    token = _active_scope.set(scope)
    try:
        yield
    finally:
        scope.active = False
        _active_scope.reset(token)


def instrument_connector(connector: SQLConnector) -> SQLConnector:
    """Factory decoration is neutral; all observations remain scope-local."""
    if _current_scope() is None or isinstance(connector, _MeasuredConnector):
        return connector
    return _MeasuredConnector(connector)


class _MeasuredConnector(SQLConnector):
    def __init__(self, connector: SQLConnector) -> None:
        self._connector = connector

    def _call[T](
        self,
        category: Literal["sql", "connection", "transaction"],
        action: Callable[[], T],
        query: str = "",
    ) -> T:
        scope = _current_scope()
        if scope is None:
            return action()
        started = read_clock(scope.clock)
        rows = 0
        try:
            result = action()
            if isinstance(result, list):
                rows = len(result)
            elif isinstance(result, tuple) and result:
                rows = 1
            return result
        finally:
            finished = read_clock(scope.clock)
            elapsed = (
                max(0.0, finished - started)
                if started is not None and finished is not None
                else 0.0
            )
            try:
                scope.recorder.record_sql_operation(category, elapsed, query, rows)
            except Exception:
                # Counters and fingerprints are not transaction/retry authority.
                pass

    def connect(self) -> None:
        self._call("connection", self._connector.connect)

    def close(self) -> None:
        self._call("connection", self._connector.close)

    def begin(self) -> None:
        self._call("transaction", self._connector.begin)

    def begin_read(self) -> None:
        self._call("transaction", self._connector.begin_read)

    def commit(self) -> None:
        self._call("transaction", self._connector.commit)

    def rollback(self) -> None:
        self._call("transaction", self._connector.rollback)

    def check_table_exists(self, table_name: str) -> bool:
        return self._call(
            "sql",
            lambda: self._connector.check_table_exists(table_name),
            "check_table_exists",
        )

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self._call("sql", lambda: self._connector.execute(query, data), query)

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        return self._call(
            "sql", lambda: self._connector.execute_affected(query, data), query
        )

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self._call("sql", lambda: self._connector.execute_many(query, data), query)

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        return self._call("sql", lambda: self._connector.fetch_one(query, data), query)

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        return self._call("sql", lambda: self._connector.fetch_all(query, data), query)
