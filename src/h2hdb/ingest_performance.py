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

from .ingest_performance_format import (
    activity,
    duration,
    pipeline_name,
    stage_description,
    workload,
)
from .sql_performance import (
    SQLCounters,
    SQLQueryStatistics,
    execution_owner,
    measure_sql,
    read_clock,
)

_REPORT_INTERVAL_SECONDS = 60.0
_QUERY_LIMIT = 64
_NESTED_RECORD_LIMIT = 64


@dataclass(frozen=True)
class _Diagnostic:
    owner: IngestPerformance
    message: str
    info_message: str | None


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
    counters: SQLCounters = field(default_factory=SQLCounters)
    queries: dict[str, SQLQueryStatistics] = field(default_factory=dict)
    processed_rows: int = 0
    replayed: bool = False
    terminal: bool = False
    nested_seconds: float = 0.0
    nested_calls: int = 0
    deferred: list[_Diagnostic] = field(default_factory=list)
    omitted_records: int = 0
    announced_preparation: bool = False
    unannounced_nested_work: bool = False

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
        match category:
            case "sql":
                counters.sql_calls += 1
                counters.sql_seconds += elapsed
                counters.read_rows += rows
                if self.owner.debug:
                    key = sha256(query.encode()).hexdigest()[:16]
                    if key not in self.queries and len(self.queries) >= _QUERY_LIMIT:
                        key = "other"
                    statistics = self.queries.get(key)
                    if statistics is None:
                        statistics = self.queries[key] = SQLQueryStatistics()
                    statistics.record(elapsed, rows)
            case "connection":
                counters.connection_calls += 1
                counters.connection_seconds += elapsed
            case _:
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
class _PreparationProgress:
    sample: PerformanceStep
    reported: float | None
    galleries: int = 0

    def __call__(self, galleries: int) -> None:
        """Called only after a preparation's bounded read transaction ends."""
        if (
            _current_step() is not self.sample
            or type(galleries) is not int
            or galleries < self.galleries
        ):
            return
        self.galleries = galleries
        now = read_clock(self.sample.owner.clock)
        if now is not None and (
            self.reported is None or now - self.reported >= _REPORT_INTERVAL_SECONDS
        ):
            self.emit("progress", now)

    def emit(self, event: str, now: float | None) -> None:
        sample = self.sample
        description = {
            "started": "started",
            "progress": "in progress",
            "completed": "finished",
            "failed": "failed",
            "interrupted": "interrupted",
        }[event]
        message = (
            f"Ingest {pipeline_name(sample.pipeline)} preparation {description}: "
            f"{activity(sample.pipeline, sample.operation)}; "
            f"ingest generation {sample.generation}"
        )
        if event != "started":
            message += f"; read {self.galleries} galleries; " + workload(
                elapsed=sample.elapsed(now)
                if now is not None and sample.started is not None
                else None,
                sql_seconds=sample.counters.sql_seconds,
                connection_seconds=sample.counters.connection_seconds,
                transaction_seconds=sample.counters.transaction_seconds,
                includes_reused_results=False,
            )
        sample.owner._emit(
            _Diagnostic(
                sample.owner,
                "ingest_preparation "
                f"event={event} operation={sample.operation} "
                f"generation={sample.generation} galleries_read={self.galleries} "
                f"elapsed_seconds={sample.elapsed(now):.6f}",
                message + ".",
            )
        )
        self.reported = now


@contextmanager
def prepare_ingest_operation(
    *, operation: str, generation: int
) -> Iterator[Callable[[int], None]]:
    """Measure local preparation separately and report only at safe boundaries.

    Enter outside all database transactions. Its callback is likewise invoked
    only after each bounded preparation read has released its transaction. SQL
    observers continue to accumulate counters without invoking any logger.
    """
    parent = _current_step()
    if parent is None:
        yield lambda _galleries: None
        return
    with parent.owner.step(parent.pipeline, "prepare", operation, generation) as sample:
        sample.announced_preparation = True
        progress = _PreparationProgress(sample, sample.started)
        progress.emit("started", sample.started)
        event = "completed"
        try:
            yield progress
        except BaseException as error:
            event = "failed" if isinstance(error, Exception) else "interrupted"
            raise
        finally:
            progress.emit(event, read_clock(parent.owner.clock))


@dataclass
class _Stage:
    key: tuple[str, str, int]
    started: float | None
    reported: float | None
    finished: float | None
    calls: int = 0
    processed_rows: int = 0
    replayed: int = 0
    counters: SQLCounters = field(default_factory=SQLCounters)
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
        failure: Literal["failed", "interrupted"] | None = None
        try:
            with measure_sql(sample, clock=self.clock):
                yield sample
        except BaseException as error:
            failure = "failed" if isinstance(error, Exception) else "interrupted"
            raise
        finally:
            sample.active = False
            _active_step.reset(token)
            now = read_clock(self.clock)
            try:
                records = self._complete(sample, now, failure=failure)
                if sample.parent is not None:
                    sample.parent.nested_seconds += sample.elapsed(now)
                    sample.parent.nested_calls += 1 + sample.nested_calls
                    sample.parent.unannounced_nested_work |= (
                        not sample.announced_preparation
                        or sample.unannounced_nested_work
                    )
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
        if self.debug and sample.queries:
            top = sorted(
                sample.queries.items(), key=lambda item: item[1].seconds, reverse=True
            )[:5]
            message += " query_top=" + ";".join(
                statistics.text(key) for key, statistics in top
            )
        info_message = None
        if (
            scope == "concurrent"
            or (
                scope == "nested"
                and event in {"failed", "interrupted"}
                and not sample.announced_preparation
            )
            or (sample.nested_calls and sample.unannounced_nested_work)
        ):
            kind = (
                "overlapping call"
                if scope == "concurrent"
                else "nested call"
                if scope == "nested"
                else "call"
            )
            info_message = (
                f"Ingest {pipeline_name(sample.pipeline)} {kind} "
                f"{'finished' if event == 'completed' else event}: "
                f"{activity(sample.pipeline, sample.operation)}; "
                f"ingest generation {sample.generation}; "
                + workload(
                    elapsed=elapsed
                    if now is not None and sample.started is not None
                    else None,
                    sql_seconds=sample.counters.sql_seconds,
                    connection_seconds=sample.counters.connection_seconds,
                    transaction_seconds=sample.counters.transaction_seconds,
                    includes_reused_results=sample.replayed,
                )
            )
            if sample.nested_calls:
                info_message += f"; nested work {duration(sample.nested_seconds)}"
            if sample.omitted_records:
                info_message += "; some nested diagnostic details omitted"
            info_message += "; stage completion not confirmed."
        return _Diagnostic(self, message, info_message)

    def _complete(
        self,
        sample: PerformanceStep,
        now: float | None,
        *,
        failure: Literal["failed", "interrupted"] | None,
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
                or failure is not None
                or self.debug
                or sample.nested_calls > 0
                or sample.omitted_records > 0
                or sample.elapsed(now) >= _REPORT_INTERVAL_SECONDS
            ):
                records.append(
                    self._step_record(
                        sample,
                        failure or "completed",
                        now,
                        scope="nested"
                        if sample.parent is not None
                        else "concurrent"
                        if concurrent
                        else "sequential",
                    )
                )
            if isolated:
                if concurrent:
                    records.extend(self._flush("overlap", sample.started))
                return records
            key = (sample.pipeline, sample.operation, sample.generation)
            entered_stage = self._stage is None or self._stage.key != key
            if entered_stage:
                records.extend(self._flush("transition", sample.started))
                self._stage = _Stage(key, sample.started, now, now)
                records.append(self._report(self._stage, "started", sample.started))
            stage = self._stage
            assert stage is not None
            stage.calls += 1
            stage.processed_rows += sample.processed_rows
            stage.replayed += int(sample.replayed)
            stage.counters.add(sample.counters)
            stage.finished = now
            stage.phases[sample.phase] = stage.phases.get(sample.phase, 0.0) + max(
                0.0, sample.elapsed(now) - sample.nested_seconds
            )
            if failure is not None or sample.terminal:
                records.extend(self._flush(failure or "terminal", now))
            elif (
                now is not None
                and stage.reported is not None
                and (
                    now - stage.reported >= _REPORT_INTERVAL_SECONDS
                    or (
                        entered_stage
                        and sample.elapsed(now) >= _REPORT_INTERVAL_SECONDS
                    )
                )
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
        info_message = stage_description(pipeline, operation, generation, event)
        if event != "started":
            info_message += "; " + workload(
                elapsed=wall_seconds
                if now is not None and stage.started is not None
                else None,
                sql_seconds=stage.counters.sql_seconds,
                connection_seconds=stage.counters.connection_seconds,
                transaction_seconds=stage.counters.transaction_seconds,
                includes_reused_results=bool(stage.replayed),
            )
        if event == "overlap":
            info_message += "; stage completion not confirmed"
        return _Diagnostic(self, message, info_message + ".")

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
        if _emitting.get() or self.level > logging.INFO:
            return
        token = _emitting.set(True)
        try:
            if record.info_message is not None:
                self.logger.info(record.info_message)
            if self.debug:
                self.logger.debug(record.message)
        except Exception:
            # A diagnostic handler must not change commit/retry semantics.
            pass
        finally:
            _emitting.reset(token)
