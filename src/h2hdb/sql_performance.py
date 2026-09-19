"""Scope-owned SQL measurements without logging or ingest dependencies."""

from __future__ import annotations

import heapq
from asyncio import current_task
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from hashlib import sha256
from math import isfinite
from threading import get_ident
from time import perf_counter
from typing import Any, Literal

from .ports import _SQLPerformanceRecorder
from .sql_connector import SQLConnector


@dataclass
class SQLCounters:
    """Completed connector method calls, not server statements or examined rows."""

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

    def add(self, other: SQLQueryStatistics) -> None:
        self.calls += other.calls
        self.seconds += other.seconds
        self.read_rows += other.read_rows
        self.max_seconds = max(self.max_seconds, other.max_seconds)

    def text(self, fingerprint: str) -> str:
        return (
            f"{fingerprint}(calls={self.calls},seconds={self.seconds:.6f},"
            f"returned_rows={self.read_rows},max_seconds={self.max_seconds:.6f})"
        )


def accumulate_query(
    queries: dict[str, SQLQueryStatistics],
    fingerprint: str,
    statistics: SQLQueryStatistics,
    *,
    limit: int = 64,
) -> None:
    """Exact first-admitted fingerprints plus an explicit, conserved overflow.

    This is not an all-fingerprint heavy-hitter algorithm. In particular, the
    overflow is emitted even when it would not rank among the displayed top five.
    Merging a bounded child cannot recover identities already in its overflow.
    """
    if fingerprint not in queries and len(queries) >= limit:
        fingerprint = "other"
    queries.setdefault(fingerprint, SQLQueryStatistics()).add(statistics)


def query_totals_snapshot(
    queries: dict[str, SQLQueryStatistics],
) -> list[dict[str, str | int | float]]:
    return [
        {
            "fingerprint": key,
            "calls": item.calls,
            "seconds": item.seconds,
            "max_seconds": item.max_seconds,
            "returned_rows": item.read_rows,
        }
        for key, item in sorted(
            queries.items(), key=lambda pair: pair[1].seconds, reverse=True
        )[:5]
    ]


@dataclass
class SQLTransactionStatistics:
    """Fixed connector boundary names, including failed completed attempts.

    Client elapsed includes all driver/server/transport waiting. These counters
    identify a slow commit versus begin; they do not identify fsync or lock waits.
    """

    operations: dict[str, SQLQueryStatistics] = field(default_factory=dict)

    def record(self, operation: str, elapsed: float) -> None:
        key: str
        match operation:
            case "begin" | "begin_read" | "commit" | "rollback":
                key = operation
            case _:
                key = "other"
        self.operations.setdefault(key, SQLQueryStatistics()).record(elapsed, 0)

    def add(self, other: SQLTransactionStatistics) -> None:
        for key, value in other.operations.items():
            self.operations.setdefault(key, SQLQueryStatistics()).add(value)

    def snapshot(self) -> list[dict[str, str | int | float]]:
        return [
            {
                "operation": key,
                "calls": value.calls,
                "seconds": value.seconds,
                "max_seconds": value.max_seconds,
            }
            for key, value in sorted(self.operations.items())
        ]

    def text(self) -> str:
        return ";".join(
            f"{key}(calls={value.calls},seconds={value.seconds:.6f},"
            f"max_seconds={value.max_seconds:.6f})"
            for key, value in sorted(self.operations.items())
        )


def query_fingerprint(query: str) -> str | None:
    """Keep SQL text/parameters out of diagnostics, including encoding failures."""
    try:
        return sha256(query.encode()).hexdigest()[:16]
    except Exception:
        return None


@dataclass
class SQLSlowQueries:
    """Exact five slowest completed calls, independent of fingerprint cardinality."""

    _entries: list[tuple[float, int, str, int]] = field(default_factory=list)
    _sequence: int = 0

    def record(self, fingerprint: str | None, elapsed: float, rows: int) -> None:
        if fingerprint is None:
            return
        self._sequence += 1
        entry = (elapsed, self._sequence, fingerprint, rows)
        if len(self._entries) < 5:
            heapq.heappush(self._entries, entry)
        elif entry[:2] > self._entries[0][:2]:
            heapq.heapreplace(self._entries, entry)

    def add(self, other: SQLSlowQueries) -> None:
        for elapsed, _sequence, fingerprint, rows in sorted(other._entries):
            self.record(fingerprint, elapsed, rows)

    def snapshot(self) -> list[dict[str, str | float | int]]:
        return [
            {"fingerprint": fingerprint, "seconds": elapsed, "returned_rows": rows}
            for elapsed, _sequence, fingerprint, rows in sorted(
                self._entries, reverse=True
            )
        ]

    def text(self) -> str:
        return ";".join(
            f"{fingerprint}(seconds={elapsed:.6f},returned_rows={rows})"
            for elapsed, _sequence, fingerprint, rows in sorted(
                self._entries, reverse=True
            )
        )


@dataclass(frozen=True)
class _PendingCall:
    category: str
    operation: str
    fingerprint: str | None
    started: float | None

    def snapshot(self, now: float | None) -> dict[str, str | float | None]:
        return {
            "category": self.category,
            "operation": self.operation,
            "fingerprint": self.fingerprint,
            "age_seconds": max(0.0, now - self.started)
            if now is not None and self.started is not None
            else None,
        }


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
class SQLMeasurement:
    """An owned scope; a heartbeat may read its immutable pending-call snapshot."""

    recorder: _SQLPerformanceRecorder
    clock: Callable[[], float]
    owner: tuple[int, object | None]
    parent: SQLMeasurement | None = None
    observe_nested: bool = False
    active: bool = True
    _pending: _PendingCall | None = None

    def pending_snapshot(
        self, now: float | None
    ) -> dict[str, str | float | None] | None:
        pending = self._pending
        return pending.snapshot(now) if self.active and pending is not None else None


_active_scope: ContextVar[SQLMeasurement | None] = ContextVar(
    "h2hdb_sql_performance", default=None
)


def _current_scope() -> SQLMeasurement | None:
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
) -> Iterator[SQLMeasurement]:
    """Measure synchronous owned calls, optionally including nested scopes.

    Ordinary step recorders remain exclusive across nested scopes. Whole
    operation observers opt into inclusive delivery, so another instrumentation
    family cannot silently hide SQL from its enclosing operation's totals.
    """
    scope = SQLMeasurement(
        recorder, clock, execution_owner(), _current_scope(), observe_nested
    )
    token = _active_scope.set(scope)
    try:
        yield scope
    finally:
        scope.active = False
        scope._pending = None
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
        fingerprint = query_fingerprint(query) if category == "sql" else None
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
        previous = [observer._pending for observer, _started in observers]
        for observer, started in observers:
            observer._pending = _PendingCall(
                category,
                query if category != "sql" else "query",
                fingerprint,
                started,
            )
        rows = 0
        try:
            result = action()
            match result:
                case list():
                    rows = len(result)
                case tuple() if result:
                    rows = 1
            return result
        finally:
            finished_observers = [
                (observer, started, read_clock(observer.clock))
                for observer, started in observers
            ]
            for (observer, _started), pending in zip(observers, previous, strict=True):
                observer._pending = pending if observer.active else None
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
        self._call("connection", self._connector.connect, "connect")

    def close(self) -> None:
        self._call("connection", self._connector.close, "close")

    def begin(self) -> None:
        self._call("transaction", self._connector.begin, "begin")

    def begin_read(self) -> None:
        self._call("transaction", self._connector.begin_read, "begin_read")

    def commit(self) -> None:
        self._call("transaction", self._connector.commit, "commit")

    def rollback(self) -> None:
        self._call("transaction", self._connector.rollback, "rollback")

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
