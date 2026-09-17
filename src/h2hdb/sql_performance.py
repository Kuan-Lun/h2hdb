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


@dataclass
class SQLCounters:
    sql_calls: int = 0
    sql_seconds: float = 0.0
    read_rows: int = 0
    connection_calls: int = 0
    connection_seconds: float = 0.0
    transaction_calls: int = 0
    transaction_seconds: float = 0.0

    def add(self, other: SQLCounters) -> None:
        self.sql_calls += other.sql_calls
        self.sql_seconds += other.sql_seconds
        self.read_rows += other.read_rows
        self.connection_calls += other.connection_calls
        self.connection_seconds += other.connection_seconds
        self.transaction_calls += other.transaction_calls
        self.transaction_seconds += other.transaction_seconds

    @property
    def seconds(self) -> float:
        return self.sql_seconds + self.connection_seconds + self.transaction_seconds

    def text(self) -> str:
        return (
            f"sql_calls={self.sql_calls} sql_seconds={self.sql_seconds:.6f} "
            f"read_rows={self.read_rows} "
            f"connection_calls={self.connection_calls} "
            f"connection_seconds={self.connection_seconds:.6f} "
            f"transaction_calls={self.transaction_calls} "
            f"transaction_seconds={self.transaction_seconds:.6f}"
        )


@dataclass
class SQLQueryStatistics:
    calls: int = 0
    seconds: float = 0.0
    read_rows: int = 0
    max_seconds: float = 0.0

    def record(self, elapsed: float, rows: int) -> None:
        self.calls += 1
        self.seconds += elapsed
        self.read_rows += rows
        self.max_seconds = max(self.max_seconds, elapsed)

    def text(self, fingerprint: str) -> str:
        return (
            f"{fingerprint}(calls={self.calls},seconds={self.seconds:.6f},"
            f"returned_rows={self.read_rows},max_seconds={self.max_seconds:.6f})"
        )


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
    parent: _MeasurementScope | None = None
    observe_nested: bool = False
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
    observe_nested: bool = False,
) -> Iterator[None]:
    """Measure synchronous owned calls, optionally including nested scopes.

    Ordinary step recorders remain exclusive across nested scopes. Whole
    operation observers opt into inclusive delivery, so another instrumentation
    family cannot silently hide SQL from its enclosing operation's totals.
    """
    scope = _MeasurementScope(
        recorder, clock, execution_owner(), _current_scope(), observe_nested
    )
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

    def primary_key_table_reference(self, relation: str) -> str:
        return self._connector.primary_key_table_reference(relation)

    def binary_parameter_expression(self, byte_count: int) -> str:
        return self._connector.binary_parameter_expression(byte_count)

    def _call[T](
        self,
        category: Literal["sql", "connection", "transaction"],
        action: Callable[[], T],
        query: str = "",
    ) -> T:
        scope = _current_scope()
        if scope is None:
            return action()
        observers = [(scope, read_clock(scope.clock))]
        ancestor = scope.parent
        while ancestor is not None:
            if (
                ancestor.active
                and ancestor.observe_nested
                and ancestor.owner == scope.owner
            ):
                observers.append((ancestor, read_clock(ancestor.clock)))
            ancestor = ancestor.parent
        rows = 0
        try:
            result = action()
            if isinstance(result, list):
                rows = len(result)
            elif isinstance(result, tuple) and result:
                rows = 1
            return result
        finally:
            finished_observers = [
                (observer, started, read_clock(observer.clock))
                for observer, started in observers
            ]
            for observer, started, finished in finished_observers:
                elapsed = (
                    max(0.0, finished - started)
                    if started is not None and finished is not None
                    else 0.0
                )
                try:
                    observer.recorder.record_sql_operation(
                        category, elapsed, query, rows
                    )
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
