"""Bounded audit/maintenance diagnostics, independent of durable authority.

SQL callbacks only update counters. Phase records are emitted after the outer
operation releases its transactions. One process-wide daemon reports snapshots
of long operations without borrowing their connections or execution contexts.
Client SQL elapsed includes server work, waits and transport; it cannot separate
those costs. Completed phase samples are inclusive; exclusive counters avoid
double-counting nested work. Truncation never changes operation totals.
"""

from __future__ import annotations

import heapq
import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import asdict, dataclass, field
from math import isfinite
from threading import Condition, Lock, Thread
from time import monotonic, perf_counter
from typing import Any, Literal
from uuid import uuid4

from .sql_performance import (
    SQLCounters,
    SQLMeasurement,
    SQLQueryStatistics,
    SQLSlowQueries,
    SQLTransactionStatistics,
    execution_owner,
    measure_sql,
    query_fingerprint,
    query_totals_snapshot,
    read_clock,
)

type DiagnosticValue = str | int | float | bool | None
type _Status = Literal["completed", "failed", "interrupted"]
_PHASE_LIMIT = 256
_PHASE_TOTAL_LIMIT = 64
_QUERY_LIMIT = 64
_DEPTH_LIMIT = 64
_LABEL_LIMIT = 32
_HEARTBEAT_LIMIT = 64


def _label(value: str) -> str:
    return "".join(
        character
        if character.isascii() and (character.isalnum() or character in "_.:/-")
        else "_"
        for character in value[:160]
    )


def _fields(values: dict[str, DiagnosticValue]) -> dict[str, DiagnosticValue]:
    result: dict[str, DiagnosticValue] = {}
    for key, value in list(values.items())[:_LABEL_LIMIT]:
        if type(value) is str:
            result[_label(key)] = _label(value)
        elif value is None or type(value) in (bool, int):
            result[_label(key)] = value
        elif type(value) is float and isfinite(value):
            result[_label(key)] = value
    return result


@dataclass
class _Statistics:
    counters: SQLCounters = field(default_factory=SQLCounters)
    queries: dict[str, SQLQueryStatistics] = field(default_factory=dict)
    slowest: SQLSlowQueries = field(default_factory=SQLSlowQueries)
    transactions: SQLTransactionStatistics = field(
        default_factory=SQLTransactionStatistics
    )

    def record(
        self,
        category: str,
        elapsed: float,
        fingerprint: str | None,
        rows: int,
        *,
        operation: str,
    ) -> None:
        match category:
            case "sql":
                self.counters.sql_calls += 1
                self.counters.sql_seconds += elapsed
                self.counters.read_rows += rows
                self.slowest.record(fingerprint, elapsed, rows)
                if fingerprint is not None:
                    if (
                        fingerprint not in self.queries
                        and len(self.queries) >= _QUERY_LIMIT
                    ):
                        fingerprint = "other"
                    statistics = self.queries.get(fingerprint)
                    if statistics is None:
                        statistics = self.queries[fingerprint] = SQLQueryStatistics()
                    statistics.record(elapsed, rows)
            case "connection":
                self.counters.connection_calls += 1
                self.counters.connection_seconds += elapsed
            case _:
                self.counters.transaction_calls += 1
                self.counters.transaction_seconds += elapsed
                self.transactions.record(operation, elapsed)

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = asdict(self.counters)
        result["query_slowest"] = self.slowest.snapshot()
        result["query_top"] = query_totals_snapshot(self.queries)
        overflow = self.queries.get("other", SQLQueryStatistics())
        result["query_overflow"] = {
            "calls": overflow.calls,
            "seconds": overflow.seconds,
            "returned_rows": overflow.read_rows,
        }
        result["transaction_breakdown"] = self.transactions.snapshot()
        return result


@dataclass
class DatabaseSpan:
    """Mutable diagnostic labels; never a database capability or receipt."""

    _root: _Operation | None = None
    name: str = ""
    span_id: int = 0
    parent_id: int | None = None
    started: float | None = None
    labels: dict[str, DiagnosticValue] = field(default_factory=dict)
    _inclusive: _Statistics = field(default_factory=_Statistics)
    _exclusive: _Statistics = field(default_factory=_Statistics)
    _child_seconds: float = 0.0

    def describe(self, **fields: DiagnosticValue) -> None:
        root = self._root
        if root is None:
            return
        with root.lock:
            self.labels.update(_fields(fields))
            if len(self.labels) > _LABEL_LIMIT:
                self.labels = dict(list(self.labels.items())[:_LABEL_LIMIT])

    def _snapshot(
        self, now: float | None, *, active_child_seconds: float = 0.0
    ) -> dict[str, Any]:
        elapsed = (
            max(0.0, now - self.started)
            if now is not None and self.started is not None
            else 0.0
        )
        return {
            "phase": self.name,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "elapsed_seconds": elapsed,
            "timing_available": now is not None and self.started is not None,
            "exclusive_seconds": max(
                0.0, elapsed - self._child_seconds - active_child_seconds
            ),
            "labels": dict(self.labels),
            **self._inclusive.snapshot(),
            "exclusive_transaction_breakdown": self._exclusive.transactions.snapshot(),
            **{
                "exclusive_" + key: value
                for key, value in asdict(self._exclusive.counters).items()
            },
        }


@dataclass
class _PhaseTotal:
    phase: str
    labels: dict[str, DiagnosticValue]
    calls: int = 0
    exclusive_seconds: float = 0.0
    counters: SQLCounters = field(default_factory=SQLCounters)
    transactions: SQLTransactionStatistics = field(
        default_factory=SQLTransactionStatistics
    )

    def snapshot(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "labels": self.labels,
            "calls": self.calls,
            "exclusive_seconds": self.exclusive_seconds,
            "exclusive_transaction_breakdown": self.transactions.snapshot(),
            **{
                "exclusive_" + key: value
                for key, value in asdict(self.counters).items()
            },
        }


class _Operation:
    def __init__(
        self, owner: DatabasePerformance, name: str, labels: dict[str, DiagnosticValue]
    ) -> None:
        self.owner = owner
        self.name = _label(name)
        self.operation_id = uuid4().hex
        self.execution = execution_owner()
        self.lock = Lock()
        self.active = True
        self.root = DatabaseSpan(
            self, self.name, started=read_clock(owner.clock), labels=_fields(labels)
        )
        self.stack: list[DatabaseSpan] = [self.root]
        self.next_span_id = 1
        self.phase_records: list[tuple[bool, float, int, dict[str, Any]]] = []
        self.phase_count = 0
        self.phase_totals: dict[
            tuple[str, DiagnosticValue, DiagnosticValue], _PhaseTotal
        ] = {}
        self.omitted_depth = 0
        self.sequence = 0
        self.measurement: SQLMeasurement | None = None
        self.next_progress_at = monotonic() + owner.interval_seconds

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        if not self.active or execution_owner() != self.execution:
            return
        fingerprint = query_fingerprint(query) if category == "sql" else None
        with self.lock:
            for span in self.stack:
                span._inclusive.record(
                    category, elapsed, fingerprint, rows, operation=query
                )
            self.stack[-1]._exclusive.record(
                category, elapsed, fingerprint, rows, operation=query
            )

    def _envelope(self, event: str) -> dict[str, Any]:
        self.sequence += 1
        return {
            "schema": 1,
            "event": event,
            "backend": self.owner.backend,
            "operation": self.name,
            "operation_id": self.operation_id,
            "sequence": self.sequence,
            "sql_calls_unit": "completed_connector_method_calls",
            "read_rows_unit": "returned_rows_not_examined_rows",
            "query_top_scope": "first_64_fingerprints_plus_other",
            "query_slowest_scope": "five_slowest_completed_calls",
        }

    def progress(self) -> None:
        now = read_clock(self.owner.clock)
        with self.lock:
            if not self.active:
                return
            active = self.stack[-1]._snapshot(now)
            direct_child_seconds = (
                self.stack[1]._snapshot(now)["elapsed_seconds"]
                if len(self.stack) > 1
                else 0.0
            )
            record = {
                **self._envelope("progress"),
                **self.root._snapshot(now, active_child_seconds=direct_child_seconds),
                "active_phase": active["phase"],
                "active_span_id": active["span_id"],
                "active_phase_seconds": active["elapsed_seconds"],
                "active_labels": active["labels"],
                "pending_call": self.measurement.pending_snapshot(now)
                if self.measurement is not None
                else None,
            }
        self.owner._emit(logging.INFO, record)

    def finish(self, status: _Status, error: BaseException | None) -> None:
        now = read_clock(self.owner.clock)
        quiet = (
            self.root.labels.get("quiet") is True
            or self.root.labels.get("result") == "already_ready"
        )
        elapsed = (
            max(0.0, now - self.root.started)
            if now is not None and self.root.started is not None
            else 0.0
        )
        if (
            quiet
            and status == "completed"
            and now is not None
            and self.root.started is not None
            and elapsed < self.owner.interval_seconds
            and not self.owner.debug
        ):
            # Short source/idle scopes remain observable while running, but
            # their deliberately suppressed completion needs no JSON snapshots.
            with self.lock:
                self.active = False
                self.measurement = None
            _unregister(self)
            return
        with self.lock:
            self.active = False
            self.measurement = None
            phases = [
                item[3] for item in sorted(self.phase_records, key=lambda item: item[2])
            ]
            records = [{**self._envelope("phase"), **phase} for phase in phases]
            summary = {
                **self._envelope(status),
                **self.root._snapshot(now),
                "phase_count": self.phase_count,
                "omitted_phase_records": self.phase_count - len(phases),
                "omitted_depth": self.omitted_depth,
                "phase_totals": [
                    value.snapshot()
                    for value in sorted(
                        self.phase_totals.values(),
                        key=lambda item: item.exclusive_seconds,
                        reverse=True,
                    )
                ],
                "phase_totals_scope": "completed_phases_first_64_keys_plus_other",
                "pending_call": None,
                "phase_top": [
                    {
                        key: phase[key]
                        for key in (
                            "phase",
                            "span_id",
                            "parent_id",
                            "labels",
                            "elapsed_seconds",
                            "exclusive_seconds",
                            "status",
                            "sql_calls",
                            "sql_seconds",
                            "read_rows",
                            "exclusive_sql_calls",
                            "exclusive_sql_seconds",
                            "exclusive_read_rows",
                            "query_slowest",
                            "query_top",
                            "query_overflow",
                            "transaction_breakdown",
                        )
                    }
                    for phase in sorted(
                        phases, key=lambda item: item["exclusive_seconds"], reverse=True
                    )[:5]
                ],
            }
            if error is not None:
                summary["error_type"] = type(error).__name__
        _unregister(self)
        if self.owner.debug:
            for record in records:
                self.owner._emit(logging.DEBUG, record)
        self.owner._emit(
            logging.DEBUG
            if quiet
            and status == "completed"
            and summary["timing_available"]
            and summary["elapsed_seconds"] < self.owner.interval_seconds
            else logging.INFO,
            summary,
        )


_active: ContextVar[_Operation | None] = ContextVar(
    "h2hdb_database_performance", default=None
)
_emitting: ContextVar[bool] = ContextVar(
    "h2hdb_database_diagnostic_emitting", default=False
)


def _current() -> _Operation | None:
    operation = _active.get()
    return (
        operation
        if operation is not None
        and operation.active
        and operation.execution == execution_owner()
        else None
    )


@contextmanager
def database_phase(name: str, **fields: DiagnosticValue) -> Iterator[DatabaseSpan]:
    root = _current()
    if root is None:
        yield DatabaseSpan()
        return
    started = read_clock(root.owner.clock)
    with root.lock:
        if len(root.stack) >= _DEPTH_LIMIT:
            root.omitted_depth += 1
            span = None
        else:
            span = DatabaseSpan(
                root,
                _label(name),
                root.next_span_id,
                root.stack[-1].span_id,
                started,
                _fields(fields),
            )
            root.next_span_id += 1
            root.stack.append(span)
    if span is None:
        yield DatabaseSpan()
        return
    status: _Status = "completed"
    error_type: str | None = None
    try:
        yield span
    except BaseException as error:
        status = "failed" if isinstance(error, Exception) else "interrupted"
        error_type = type(error).__name__
        raise
    finally:
        now = read_clock(root.owner.clock)
        with root.lock:
            root.stack.pop()
            record = {**span._snapshot(now), "status": status}
            if error_type is not None:
                record["error_type"] = error_type
            root.stack[-1]._child_seconds += record["elapsed_seconds"]
            root.phase_count += 1
            key = (span.name, span.labels.get("target"), span.labels.get("phase"))
            if (
                key not in root.phase_totals
                and len(root.phase_totals) >= _PHASE_TOTAL_LIMIT
            ):
                key = ("other", None, None)
            total = root.phase_totals.setdefault(
                key,
                _PhaseTotal(
                    key[0],
                    {
                        label: value
                        for label, value in zip(
                            ("target", "phase"), key[1:], strict=True
                        )
                        if value is not None
                    },
                ),
            )
            total.calls += 1
            total.exclusive_seconds += record["exclusive_seconds"]
            total.counters.add(span._exclusive.counters)
            total.transactions.add(span._exclusive.transactions)
            ranked = (
                status != "completed",
                record["elapsed_seconds"],
                span.span_id,
                record,
            )
            if len(root.phase_records) < _PHASE_LIMIT:
                heapq.heappush(root.phase_records, ranked)
            elif ranked[:3] > root.phase_records[0][:3]:
                heapq.heapreplace(root.phase_records, ranked)


class DatabasePerformance:
    """Own an operation's counters; observer failures never replace its result."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        backend: str,
        level: int,
        clock: Callable[[], float] = perf_counter,
        interval_seconds: float = 60.0,
    ) -> None:
        self.logger = logger
        self.backend = _label(backend)
        self.level = level
        self.debug = level <= logging.DEBUG
        self.clock = clock
        self.interval_seconds = (
            max(0.01, interval_seconds) if isfinite(interval_seconds) else 60.0
        )

    @contextmanager
    def operation(self, name: str, **fields: DiagnosticValue) -> Iterator[DatabaseSpan]:
        if self.level > logging.INFO or _emitting.get():
            yield DatabaseSpan()
            return
        if _current() is not None:
            with database_phase(name, **fields) as nested:
                yield nested
            return
        root = _Operation(self, name, fields)
        token = _active.set(root)
        status: _Status = "completed"
        failure: BaseException | None = None
        # Only a full check announces start. Routine READY admission and idle
        # cleanup probes must not generate an INFO start record on every poll.
        if name == "schema_check":
            with root.lock:
                started = {
                    **root._envelope("started"),
                    "labels": dict(root.root.labels),
                }
            self._emit(logging.INFO, started)
        _register(root)
        try:
            with measure_sql(
                root, clock=self.clock, observe_nested=True
            ) as measurement:
                root.measurement = measurement
                yield root.root
        except BaseException as error:
            status = "failed" if isinstance(error, Exception) else "interrupted"
            failure = error
            raise
        finally:
            _active.reset(token)
            try:
                root.finish(status, failure)
            except Exception:
                root.active = False
                root.measurement = None
                _unregister(root)

    def _emit(self, level: int, record: dict[str, Any]) -> None:
        if level < self.level or _emitting.get():
            return
        token = _emitting.set(True)
        try:
            self.logger.log(
                level,
                "database_performance %s",
                json.dumps(
                    record, sort_keys=True, separators=(",", ":"), allow_nan=False
                ),
            )
        except Exception:
            # Error messages can contain SQL parameters; never render them.
            pass
        finally:
            _emitting.reset(token)


_pulse_condition = Condition()
_pulse_operations: dict[str, _Operation] = {}
_pulse_thread: Thread | None = None


def _register(operation: _Operation) -> None:
    global _pulse_thread
    with _pulse_condition:
        if len(_pulse_operations) >= _HEARTBEAT_LIMIT:
            operation.root.describe(heartbeat="capacity_exceeded")
            return
        _pulse_operations[operation.operation_id] = operation
        if _pulse_thread is None:
            thread = Thread(
                target=_pulse,
                name="h2hdb-database-diagnostics",
                daemon=True,
                context=Context(),
            )
            try:
                thread.start()
            except RuntimeError:
                _pulse_operations.pop(operation.operation_id, None)
                operation.root.describe(heartbeat="unavailable")
                return
            _pulse_thread = thread
        _pulse_condition.notify()


def _unregister(operation: _Operation) -> None:
    with _pulse_condition:
        _pulse_operations.pop(operation.operation_id, None)
        _pulse_condition.notify()


def _pulse() -> None:
    while True:
        with _pulse_condition:
            while not _pulse_operations:
                _pulse_condition.wait()
            now = monotonic()
            due = [
                operation
                for operation in _pulse_operations.values()
                if operation.next_progress_at <= now
            ]
            if not due:
                _pulse_condition.wait(
                    min(
                        operation.next_progress_at
                        for operation in _pulse_operations.values()
                    )
                    - now
                )
                continue
            for operation in due:
                operation.next_progress_at = now + operation.owner.interval_seconds
        for operation in due:
            try:
                operation.progress()
            except Exception:
                pass
        # A daemon waiting for future work must not retain the last completed
        # operation, including its bounded but potentially sizeable snapshots.
        due.clear()
        del operation
