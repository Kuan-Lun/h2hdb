"""Bounded, process-local ingest diagnostics; never durable work authority.

SQL counters describe connector calls, not wire statements (``execute_many``
counts once). Connection timing includes pool checkout and connection probes;
transaction timing includes begin/commit/rollback. The remaining step time
includes Python, private scratch I/O, adapters, and scheduling, not just CPU.
No SQL text, parameters, credentials, or authority tokens enter the log.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from hashlib import sha256
from threading import Lock
from time import perf_counter
from typing import Any

from .sql_connector import SQLConnector

_REPORT_INTERVAL_SECONDS = 60.0
_QUERY_LIMIT = 64


@dataclass
class _Counters:
    sql_calls: int = 0
    sql_seconds: float = 0.0
    read_rows: int = 0
    connection_calls: int = 0
    connection_seconds: float = 0.0
    transaction_calls: int = 0
    transaction_seconds: float = 0.0

    def add(self, other: _Counters) -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))

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
class PerformanceStep:
    owner: IngestPerformance
    pipeline: str
    phase: str
    operation: str
    generation: int
    started: float
    last_report: float
    counters: _Counters = field(default_factory=_Counters)
    queries: dict[str, tuple[int, float]] = field(default_factory=dict)
    processed_rows: int = 0
    replayed: bool = False
    terminal: bool = False

    def record(self, category: str, elapsed: float, query: str, rows: int) -> None:
        counters = self.counters
        if category == "sql":
            counters.sql_calls += 1
            counters.sql_seconds += elapsed
            counters.read_rows += rows
            if self.owner.debug:
                key = sha256(query.encode()).hexdigest()[:16]
                if key not in self.queries and len(self.queries) >= _QUERY_LIMIT:
                    key = "other"
                count, seconds = self.queries.get(key, (0, 0.0))
                self.queries[key] = (count + 1, seconds + elapsed)
        elif category == "connection":
            counters.connection_calls += 1
            counters.connection_seconds += elapsed
        else:
            counters.transaction_calls += 1
            counters.transaction_seconds += elapsed
        now = self.owner.clock()
        if now - self.last_report >= _REPORT_INTERVAL_SECONDS:
            self.last_report = now
            self.owner.emit_step(self, "in_progress", now, debug=False)


_active_step: ContextVar[PerformanceStep | None] = ContextVar(
    "h2hdb_ingest_performance", default=None
)


@dataclass
class _Stage:
    key: tuple[str, str, int]
    started: float
    reported: float
    calls: int = 0
    processed_rows: int = 0
    replayed: int = 0
    counters: _Counters = field(default_factory=_Counters)
    phases: dict[str, float] = field(default_factory=dict)


class IngestPerformance:
    """One bounded stage accumulator per facade, with optional per-call detail."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        backend: str,
        level: int = logging.INFO,
        clock: Callable[[], float] = perf_counter,
    ) -> None:
        self.logger = logger
        self.backend = backend
        self.level = level
        self.debug = level <= logging.DEBUG
        self.clock = clock
        self._stage: _Stage | None = None
        self._lock = Lock()

    @contextmanager
    def step(
        self, pipeline: str, phase: str, operation: str, generation: int
    ) -> Iterator[PerformanceStep]:
        started = self.clock()
        sample = PerformanceStep(
            self, pipeline, phase, operation, generation, started, started
        )
        token = _active_step.set(sample)
        if self.debug:
            self.emit_step(sample, "started", started, debug=True)
        failed = False
        try:
            yield sample
        except BaseException:
            failed = True
            raise
        finally:
            _active_step.reset(token)
            now = self.clock()
            if failed or self.debug:
                self.emit_step(
                    sample,
                    "failed" if failed else "completed",
                    now,
                    debug=not failed,
                )
            self._complete(sample, now, failed=failed)

    def emit_step(
        self, sample: PerformanceStep, event: str, now: float, *, debug: bool
    ) -> None:
        elapsed = max(0.0, now - sample.started)
        counted = (
            sample.counters.sql_seconds
            + sample.counters.connection_seconds
            + sample.counters.transaction_seconds
        )
        message = (
            f"ingest_db_performance event={event} backend={self.backend} "
            f"pipeline={sample.pipeline} operation={sample.operation} "
            f"generation={sample.generation} phase={sample.phase} "
            f"elapsed_seconds={elapsed:.6f} "
            f"other_seconds={max(0.0, elapsed - counted):.6f} "
            f"processed_rows={sample.processed_rows} replayed={int(sample.replayed)} "
            f"{sample.counters.text()}"
        )
        if debug and sample.queries:
            top = sorted(
                sample.queries.items(), key=lambda item: item[1][1], reverse=True
            )[:5]
            message += " query_top=" + ",".join(
                f"{key}:{count}:{seconds:.6f}" for key, (count, seconds) in top
            )
        self._emit(message, debug=debug)

    def _complete(self, sample: PerformanceStep, now: float, *, failed: bool) -> None:
        with self._lock:
            key = (sample.pipeline, sample.operation, sample.generation)
            if self._stage is None or self._stage.key != key:
                self._flush("transition", sample.started)
                self._stage = _Stage(key, sample.started, now)
                self._report(self._stage, "started", sample.started)
            stage = self._stage
            stage.calls += 1
            stage.processed_rows += sample.processed_rows
            stage.replayed += int(sample.replayed)
            stage.counters.add(sample.counters)
            stage.phases[sample.phase] = stage.phases.get(sample.phase, 0.0) + max(
                0.0, now - sample.started
            )
            if failed or sample.terminal:
                self._flush("failed" if failed else "terminal", now)
            elif now - stage.reported >= _REPORT_INTERVAL_SECONDS:
                self._report(stage, "progress", now)

    def _report(self, stage: _Stage, event: str, now: float) -> None:
        pipeline, operation, generation = stage.key
        call_seconds = sum(stage.phases.values())
        database_seconds = (
            stage.counters.sql_seconds
            + stage.counters.connection_seconds
            + stage.counters.transaction_seconds
        )
        message = (
            f"ingest_db_performance event=stage_{event} backend={self.backend} "
            f"pipeline={pipeline} operation={operation} generation={generation} "
            f"wall_seconds={max(0.0, now - stage.started):.6f} "
            f"call_seconds={call_seconds:.6f} "
            f"other_seconds={max(0.0, call_seconds - database_seconds):.6f} "
            f"calls={stage.calls} "
            f"processed_rows={stage.processed_rows} replayed_calls={stage.replayed} "
            f"{stage.counters.text()}"
        )
        message += " " + " ".join(
            f"{phase}_seconds={seconds:.6f}"
            for phase, seconds in sorted(stage.phases.items())
        )
        self._emit(message, debug=False)
        stage.reported = now

    def _flush(self, event: str, now: float) -> None:
        if self._stage is not None:
            self._report(self._stage, event, now)
            self._stage = None

    def close(self) -> None:
        with self._lock:
            self._flush("closed", self.clock())

    def _emit(self, message: str, *, debug: bool) -> None:
        if self.level > (logging.DEBUG if debug else logging.INFO):
            return
        try:
            if debug:
                self.logger.debug(message)
            else:
                self.logger.info(message)
        except Exception:
            # A diagnostic handler must not change commit/retry semantics.
            pass


def instrument_connector(connector: SQLConnector) -> SQLConnector:
    """Only instrument factory calls made inside an explicit ingest scope."""
    sample = _active_step.get()
    return connector if sample is None else _MeasuredConnector(connector, sample)


class _MeasuredConnector(SQLConnector):
    def __init__(self, connector: SQLConnector, sample: PerformanceStep) -> None:
        self._connector = connector
        self._sample = sample

    def _call[T](self, category: str, action: Callable[[], T], query: str = "") -> T:
        started = self._sample.owner.clock()
        rows = 0
        try:
            result = action()
            if isinstance(result, list):
                rows = len(result)
            elif isinstance(result, tuple) and result:
                rows = 1
            return result
        finally:
            self._sample.record(
                category, max(0.0, self._sample.owner.clock() - started), query, rows
            )

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
