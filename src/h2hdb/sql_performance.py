"""Scope-owned SQL measurements without logging or ingest dependencies."""

from __future__ import annotations

import heapq
import re
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

QUERY_FINGERPRINT_ALGORITHM = "sha256-in-placeholder-list-v1"

# Consume protected SQL text before looking for a placeholder-only IN list.
# Unterminated strings/comments remain protected through EOF, including failed
# statements. The alternatives deliberately do not parse or rewrite SQL syntax.
_FINGERPRINT_PARTS = re.compile(
    r"'(?:(?:''|\\(?:[\s\S]|\Z)|[^'\\]))*(?:'|\Z)"
    r'|"(?:(?:""|\\(?:[\s\S]|\Z)|[^"\\]))*(?:"|\Z)'
    r"|`(?:(?:``|\\(?:[\s\S]|\Z)|[^`\\]))*(?:`|\Z)"
    r"|\[(?:[^\]]|\]\])*(?:\]|\Z)"
    r"|--[^\r\n]*|\#[^\r\n]*|/\*[\s\S]*?(?:\*/|\Z)"
    r"|(?P<in_list>(?<![\w$])(?i:IN)\s*\(\s*%s\s*(?:,\s*%s\s*)*\))",
)
_IN_LIST_START = re.compile(r"(?i:\bIN)\s*\(")


def _fingerprint_part(match: re.Match[str]) -> str:
    return "IN (%s)" if match.lastgroup == "in_list" else match.group()


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


QUERY_ATTRIBUTION_ALGORITHM = "bounded-duration-upper-lower-v1"
QUERY_ATTRIBUTION_CAPACITY = 64


@dataclass
class _QueryEstimate:
    observed: SQLQueryStatistics = field(default_factory=SQLQueryStatistics)
    error_seconds: float = 0.0
    complete: bool = True

    @property
    def upper(self) -> float:
        return self.observed.seconds + self.error_seconds

    def snapshot(self, fingerprint: str) -> dict[str, str | int | float | bool]:
        return {
            "fingerprint": fingerprint,
            "observed_calls": self.observed.calls,
            "seconds_lower": self.observed.seconds,
            "seconds_upper": self.upper,
            "observed_returned_rows": self.observed.read_rows,
            "observed_max_seconds": self.observed.max_seconds,
            "complete": self.complete,
        }

    def text(self, fingerprint: str) -> str:
        return (
            f"{fingerprint}(observed_calls={self.observed.calls},"
            f"seconds_lower={self.observed.seconds:.6f},seconds_upper={self.upper:.6f},"
            f"observed_returned_rows={self.observed.read_rows},"
            f"observed_max_seconds={self.observed.max_seconds:.6f},"
            f"complete={int(self.complete)})"
        )


class SQLQuerySummary:
    """Duration heavy hitters with deterministic bounds, never estimated counts.

    Retained statistics are actual observations since admission (lower bounds).
    On replacement, the minimum retained upper bound bounds all earlier time of
    the incoming key. The evicted upper bound also bounds every untracked key.
    Updates use an indexed heap: O(log capacity), with no stale heap entries.

    A merge adds per-key intervals, using the missing-key upper bound for absent
    identities, then retains the largest upper bounds. It can widen intervals;
    it cannot invent observed calls/rows or narrow an unsupported bound. Whole
    scope counters remain separate and exact. Floating-point rounding applies.
    """

    def __init__(self, capacity: int = QUERY_ATTRIBUTION_CAPACITY) -> None:
        if capacity < 1:
            raise ValueError("query summary capacity must be positive")
        self.capacity = capacity
        self.entries: dict[str, _QueryEstimate] = {}
        self._heap: list[str] = []
        self._positions: dict[str, int] = {}
        self.missing_seconds_upper = 0.0
        self.replacements = 0
        self.unfingerprinted = SQLQueryStatistics()

    def __len__(self) -> int:
        return len(self.entries)

    def __bool__(self) -> bool:
        return bool(self.entries or self.unfingerprinted.calls)

    def _less(self, left: int, right: int) -> bool:
        left_key, right_key = self._heap[left], self._heap[right]
        return (self.entries[left_key].upper, left_key) < (
            self.entries[right_key].upper,
            right_key,
        )

    def _swap(self, left: int, right: int) -> None:
        self._heap[left], self._heap[right] = self._heap[right], self._heap[left]
        self._positions[self._heap[left]] = left
        self._positions[self._heap[right]] = right

    def _down(self, index: int) -> None:
        while (child := index * 2 + 1) < len(self._heap):
            if child + 1 < len(self._heap) and self._less(child + 1, child):
                child += 1
            if not self._less(child, index):
                break
            self._swap(index, child)
            index = child

    def record(self, fingerprint: str | None, elapsed: float, rows: int) -> None:
        if fingerprint is None:
            self.unfingerprinted.record(elapsed, rows)
            return
        entry = self.entries.get(fingerprint)
        if entry is not None:
            entry.observed.record(elapsed, rows)
            self._down(self._positions[fingerprint])
            return
        error = self.missing_seconds_upper
        complete = self.replacements == 0
        if len(self.entries) == self.capacity:
            evicted = self._heap[0]
            error = self.entries.pop(evicted).upper
            del self._positions[evicted]
            self.missing_seconds_upper = error
            self.replacements += 1
            complete = False
            index = 0
            self._heap[0] = fingerprint
        else:
            index = len(self._heap)
            self._heap.append(fingerprint)
        entry = _QueryEstimate(error_seconds=error, complete=complete)
        entry.observed.record(elapsed, rows)
        self.entries[fingerprint] = entry
        self._positions[fingerprint] = index
        if index == 0:
            self._down(index)
        else:
            while index and self._less(index, (index - 1) // 2):
                parent = (index - 1) // 2
                self._swap(index, parent)
                index = parent

    def add(self, other: SQLQuerySummary) -> None:
        """Merge completed, disjoint scopes in bounded O(capacity log capacity)."""
        self.unfingerprinted.add(other.unfingerprinted)
        if not other.entries:
            return
        combined: dict[str, _QueryEstimate] = {}
        for key in self.entries.keys() | other.entries.keys():
            left, right = self.entries.get(key), other.entries.get(key)
            observed = SQLQueryStatistics()
            if left is not None:
                observed.add(left.observed)
            if right is not None:
                observed.add(right.observed)
            combined[key] = _QueryEstimate(
                observed,
                (left.error_seconds if left else self.missing_seconds_upper)
                + (right.error_seconds if right else other.missing_seconds_upper),
                (left.complete if left else self.replacements == 0)
                and (right.complete if right else other.replacements == 0),
            )
        ordered = sorted(
            combined, key=lambda key: (combined[key].upper, key), reverse=True
        )
        dropped = ordered[self.capacity :]
        self.missing_seconds_upper += other.missing_seconds_upper
        if dropped:
            self.missing_seconds_upper = max(
                self.missing_seconds_upper, combined[dropped[0]].upper
            )
        self.replacements += other.replacements + len(dropped)
        self.entries = {key: combined[key] for key in ordered[: self.capacity]}
        self._heap = list(self.entries)
        self._positions = {key: index for index, key in enumerate(self._heap)}
        for index in reversed(range(len(self._heap) // 2)):
            self._down(index)

    def top(self) -> list[tuple[str, _QueryEstimate]]:
        return sorted(
            self.entries.items(),
            key=lambda pair: (pair[1].upper, pair[0]),
            reverse=True,
        )[:5]

    def snapshot(self) -> dict[str, Any]:
        return {
            "algorithm": QUERY_ATTRIBUTION_ALGORITHM,
            "capacity": self.capacity,
            "retained_families": len(self.entries),
            "replacements": self.replacements,
            "unfingerprinted_calls": self.unfingerprinted.calls,
            "unfingerprinted_seconds": self.unfingerprinted.seconds,
            "unfingerprinted_returned_rows": self.unfingerprinted.read_rows,
            "missing_key_seconds_upper": self.missing_seconds_upper,
            "retained_seconds_lower": sum(
                item.observed.seconds for item in self.entries.values()
            ),
            "top": [item.snapshot(key) for key, item in self.top()],
        }

    def text(self) -> str:
        return (
            f"algorithm={QUERY_ATTRIBUTION_ALGORITHM},capacity={self.capacity},"
            f"retained_families={len(self.entries)},replacements={self.replacements},"
            f"missing_key_seconds_upper={self.missing_seconds_upper:.6f},"
            f"unfingerprinted_calls={self.unfingerprinted.calls},"
            f"unfingerprinted_seconds={self.unfingerprinted.seconds:.6f};"
            + ";".join(item.text(key) for key, item in self.top())
        )


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
    """Hash a query family without exposing SQL text or parameter values.

    Only the arity of a bare ``IN (%s, ...)`` is discarded. Quoted text,
    comments, literals, tuple lists, expressions and subqueries retain their
    exact text. No query cache retains SQL or grows with caller input. This is
    diagnostic grouping, never a query-equivalence or execution authority.
    """
    try:
        normalized = (
            _FINGERPRINT_PARTS.sub(_fingerprint_part, query)
            if _IN_LIST_START.search(query) is not None
            else query
        )
        return sha256(normalized.encode()).hexdigest()[:16]
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
