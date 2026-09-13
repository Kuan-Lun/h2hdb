"""Bounded ingest diagnostics emitted only at completed facade-call boundaries.

SQL observers only accumulate process-local counters. Nested calls defer their
records to the outer call, and overlapping calls report separately instead of
sharing a stage accumulator. No diagnostic callback runs under a telemetry lock.
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
from typing import Literal

from .sql_performance import execution_owner, measure_sql, read_clock

_REPORT_INTERVAL_SECONDS = 60.0
_QUERY_LIMIT = 64
_NESTED_RECORD_LIMIT = 64


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


@dataclass(frozen=True)
class _Diagnostic:
    owner: IngestPerformance
    message: str
    debug: bool


@dataclass
class PerformanceStep:
    owner: IngestPerformance
    pipeline: str
    phase: str
    operation: str
    generation: int
    started: float | None
    execution: tuple[int, object | None]
    parent: PerformanceStep | None
    overlap_epoch: int = 0
    active: bool = True
    counters: _Counters = field(default_factory=_Counters)
    queries: dict[str, tuple[int, float]] = field(default_factory=dict)
    processed_rows: int = 0
    replayed: bool = False
    terminal: bool = False
    nested_seconds: float = 0.0
    nested_calls: int = 0
    deferred: list[_Diagnostic] = field(default_factory=list)
    omitted_records: int = 0

    def elapsed(self, now: float | None) -> float:
        if self.started is None or now is None:
            return 0.0
        return max(0.0, now - self.started)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        """Only bounded memory updates; never call a logger or clock here."""
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

    def defer(self, records: list[_Diagnostic]) -> None:
        remaining = _NESTED_RECORD_LIMIT - len(self.deferred)
        self.deferred.extend(records[:remaining])
        self.omitted_records += max(0, len(records) - remaining)


_active_step: ContextVar[PerformanceStep | None] = ContextVar(
    "h2hdb_ingest_performance", default=None
)
_emitting: ContextVar[bool] = ContextVar(
    "h2hdb_ingest_performance_emitting", default=False
)


def _current_step() -> PerformanceStep | None:
    sample = _active_step.get()
    if sample is None or not sample.active or sample.execution != execution_owner():
        return None
    return sample


def describe_ingest_step(*, operation: str | bytes, generation: int) -> None:
    """Attach already validated authority labels without inspecting handles."""
    sample = _current_step()
    if sample is None:
        return
    if type(operation) is bytes:
        try:
            operation = operation.decode("ascii")
        except UnicodeDecodeError:
            operation = "INVALID"
    sample.operation = (
        operation
        if type(operation) is str
        and 0 < len(operation) <= 64
        and operation.isascii()
        and all(character.isalnum() or character == "_" for character in operation)
        else "INVALID"
    )
    sample.generation = (
        generation if type(generation) is int and 0 <= generation < 2**63 else 0
    )


@dataclass
class _Stage:
    key: tuple[str, str, int]
    started: float | None
    reported: float | None
    finished: float | None
    calls: int = 0
    processed_rows: int = 0
    replayed: int = 0
    counters: _Counters = field(default_factory=_Counters)
    phases: dict[str, float] = field(default_factory=dict)


class IngestPerformance:
    """One sequential stage per facade; nested/overlapping calls stay separate."""

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
        self._active_roots = 0
        self._overlap_epoch = 0
        self._closed = False

    @contextmanager
    def step(
        self, pipeline: str, phase: str, operation: str, generation: int
    ) -> Iterator[PerformanceStep]:
        started = read_clock(self.clock)
        sample = PerformanceStep(
            self,
            pipeline,
            phase,
            operation,
            generation,
            started,
            execution_owner(),
            _current_step(),
        )
        with self._lock:
            if sample.parent is None:
                self._active_roots += 1
                if self._active_roots > 1:
                    self._overlap_epoch += 1
            sample.overlap_epoch = self._overlap_epoch
        token = _active_step.set(sample)
        failed = False
        try:
            with measure_sql(sample, clock=self.clock):
                yield sample
        except BaseException:
            failed = True
            raise
        finally:
            sample.active = False
            _active_step.reset(token)
            now = read_clock(self.clock)
            try:
                records = self._complete(sample, now, failed=failed)
                if sample.parent is not None:
                    sample.parent.nested_seconds += sample.elapsed(now)
                    sample.parent.nested_calls += 1 + sample.nested_calls
                    sample.parent.omitted_records += sample.omitted_records
                    sample.parent.defer(records)
                else:
                    for record in records:
                        record.owner._emit(record)
            except Exception:
                # Diagnostic formatting/aggregation must not change DB outcomes.
                pass

    def _step_record(
        self,
        sample: PerformanceStep,
        event: str,
        now: float | None,
        *,
        scope: str,
        debug: bool,
    ) -> _Diagnostic:
        elapsed = sample.elapsed(now)
        own_seconds = max(0.0, elapsed - sample.nested_seconds)
        message = (
            f"ingest_db_performance event={event} backend={self.backend} "
            f"pipeline={sample.pipeline} operation={sample.operation} "
            f"generation={sample.generation} phase={sample.phase} scope={scope} "
            f"elapsed_seconds={elapsed:.6f} call_seconds={own_seconds:.6f} "
            f"other_seconds={max(0.0, own_seconds - sample.counters.seconds):.6f} "
            f"nested_calls={sample.nested_calls} nested_seconds={sample.nested_seconds:.6f} "
            f"omitted_nested_records={sample.omitted_records} "
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
        return _Diagnostic(self, message, debug)

    def _complete(
        self, sample: PerformanceStep, now: float | None, *, failed: bool
    ) -> list[_Diagnostic]:
        records = list(sample.deferred)
        with self._lock:
            if sample.parent is None:
                self._active_roots -= 1
            if self._closed:
                return records
            concurrent = sample.parent is None and (
                self._active_roots > 0 or sample.overlap_epoch != self._overlap_epoch
            )
            isolated = sample.parent is not None or concurrent
            if (
                isolated
                or failed
                or self.debug
                or sample.nested_calls > 0
                or sample.omitted_records > 0
                or sample.elapsed(now) >= _REPORT_INTERVAL_SECONDS
            ):
                records.append(
                    self._step_record(
                        sample,
                        "failed" if failed else "completed",
                        now,
                        scope="nested"
                        if sample.parent is not None
                        else "concurrent"
                        if concurrent
                        else "sequential",
                        debug=self.debug and not failed,
                    )
                )
            if isolated:
                if concurrent:
                    records.extend(self._flush("overlap", sample.started))
                return records
            key = (sample.pipeline, sample.operation, sample.generation)
            if self._stage is None or self._stage.key != key:
                records.extend(self._flush("transition", sample.started))
                self._stage = _Stage(key, sample.started, now, now)
                records.append(self._report(self._stage, "started", sample.started))
            stage = self._stage
            stage.calls += 1
            stage.processed_rows += sample.processed_rows
            stage.replayed += int(sample.replayed)
            stage.counters.add(sample.counters)
            stage.finished = now
            stage.phases[sample.phase] = stage.phases.get(sample.phase, 0.0) + max(
                0.0, sample.elapsed(now) - sample.nested_seconds
            )
            if failed or sample.terminal:
                records.extend(self._flush("failed" if failed else "terminal", now))
            elif (
                now is not None
                and stage.reported is not None
                and now - stage.reported >= _REPORT_INTERVAL_SECONDS
            ):
                records.append(self._report(stage, "progress", now))
        return records

    def _report(self, stage: _Stage, event: str, now: float | None) -> _Diagnostic:
        pipeline, operation, generation = stage.key
        call_seconds = sum(stage.phases.values())
        wall_seconds = (
            max(0.0, now - stage.started)
            if now is not None and stage.started is not None
            else 0.0
        )
        message = (
            f"ingest_db_performance event=stage_{event} backend={self.backend} "
            f"pipeline={pipeline} operation={operation} generation={generation} "
            f"wall_seconds={wall_seconds:.6f} call_seconds={call_seconds:.6f} "
            f"other_seconds={max(0.0, call_seconds - stage.counters.seconds):.6f} "
            f"calls={stage.calls} processed_rows={stage.processed_rows} "
            f"replayed_calls={stage.replayed} {stage.counters.text()}"
        )
        message += " " + " ".join(
            f"{phase}_seconds={seconds:.6f}"
            for phase, seconds in sorted(stage.phases.items())
        )
        stage.reported = now
        return _Diagnostic(self, message, False)

    def _flush(self, event: str, now: float | None) -> list[_Diagnostic]:
        stage = self._stage
        self._stage = None
        if stage is None:
            return []
        if stage.finished is not None:
            now = max(stage.finished, now) if now is not None else stage.finished
        return [self._report(stage, event, now)]

    def close(self) -> None:
        now = read_clock(self.clock)
        with self._lock:
            if self._closed:
                return
            self._closed = True
            records = self._flush("closed", now)
        sample = _current_step()
        if sample is not None:
            sample.defer(records)
            return
        for record in records:
            self._emit(record)

    def _emit(self, record: _Diagnostic) -> None:
        if _emitting.get() or self.level > (
            logging.DEBUG if record.debug else logging.INFO
        ):
            return
        token = _emitting.set(True)
        try:
            if record.debug:
                self.logger.debug(record.message)
            else:
                self.logger.info(record.message)
        except Exception:
            # A diagnostic handler must not change commit/retry semantics.
            pass
        finally:
            _emitting.reset(token)
