"""Bounded local-work attribution, independent of SQL and durable authority.

Inclusive seconds describe each operation; exclusive seconds subtract nested
operations in the same execution context. Bytes are actual logical transfers,
including rereads, never physical device traffic. No payload or path is stored.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Literal, get_args

from .sql_performance import execution_owner, read_clock

WorkOperation = Literal[
    "artifact_input_audit",
    "artifact_cache_lookup",
    "artifact_cached_source_verify",
    "artifact_source_open",
    "artifact_source_copy",
    "artifact_source_rehash",
    "artifact_archive_render",
    "artifact_archive_hash",
    "artifact_presentation_render",
    "artifact_presentation_hash",
    "artifact_page_extent_hash",
    "artifact_thumbnail_hash",
    "artifact_protection_before_hash",
    "artifact_protection_adapter",
    "artifact_protection_after_hash",
    "artifact_intent_lookup",
]
_OPERATIONS = frozenset(get_args(WorkOperation))


@dataclass
class WorkCost:
    calls: int = 0
    failures: int = 0
    timing_failures: int = 0
    inclusive_seconds: float = 0.0
    exclusive_seconds: float = 0.0
    read_calls: int = 0
    read_bytes: int = 0
    write_calls: int = 0
    write_bytes: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_invalidations: int = 0

    def add(self, other: WorkCost) -> None:
        self.calls += other.calls
        self.failures += other.failures
        self.timing_failures += other.timing_failures
        self.inclusive_seconds += other.inclusive_seconds
        self.exclusive_seconds += other.exclusive_seconds
        self.read_calls += other.read_calls
        self.read_bytes += other.read_bytes
        self.write_calls += other.write_calls
        self.write_bytes += other.write_bytes
        self.cache_hits += other.cache_hits
        self.cache_misses += other.cache_misses
        self.cache_invalidations += other.cache_invalidations

    def text(self, name: str) -> str:
        text = (
            f"{name}(calls={self.calls},failures={self.failures},timing_failures={self.timing_failures},"
            f"inclusive_seconds={self.inclusive_seconds:.6f},"
            f"exclusive_seconds={self.exclusive_seconds:.6f},"
            f"read_calls={self.read_calls},logical_read_bytes={self.read_bytes},"
            f"write_calls={self.write_calls},logical_write_bytes={self.write_bytes}"
        )
        if self.cache_hits or self.cache_misses:
            text += (
                f",cache_hits={self.cache_hits},cache_misses={self.cache_misses},"
                f"cache_invalidations={self.cache_invalidations}"
            )
        return text + ")"


@dataclass
class WorkCosts:
    operations: dict[WorkOperation, WorkCost] = field(default_factory=dict)

    def add(self, other: WorkCosts) -> None:
        for name, cost in other.operations.items():
            self.operations.setdefault(name, WorkCost()).add(cost)

    def text(self) -> str:
        return ";".join(
            cost.text(name) for name, cost in sorted(self.operations.items())
        )


@dataclass
class _Collector:
    costs: WorkCosts
    clock: Callable[[], float]
    execution: tuple[int, object | None]
    active: bool = True


@dataclass
class _Span:
    collector: _Collector
    cost: WorkCost
    nested_seconds: float = 0.0
    active: bool = True


_collector: ContextVar[_Collector | None] = ContextVar(
    "ingest_work_costs", default=None
)
_span: ContextVar[_Span | None] = ContextVar("ingest_work_span", default=None)


@contextmanager
def collect_ingest_work(costs: WorkCosts, clock: Callable[[], float]) -> Iterator[None]:
    collector = _Collector(costs, clock, execution_owner())
    token = _collector.set(collector)
    span_token = _span.set(None)
    try:
        yield
    finally:
        collector.active = False
        _span.reset(span_token)
        _collector.reset(token)


@contextmanager
def measure_ingest_work(name: WorkOperation) -> Iterator[None]:
    collector = _collector.get()
    if (
        name not in _OPERATIONS
        or collector is None
        or not collector.active
        or collector.execution != execution_owner()
    ):
        yield
        return
    parent = _span.get()
    cost = WorkCost(calls=1)
    span = _Span(collector, cost)
    token = _span.set(span)
    started = read_clock(collector.clock)
    try:
        yield
    except BaseException:
        cost.failures = 1
        raise
    finally:
        span.active = False
        finished = read_clock(collector.clock)
        _span.reset(token)
        elapsed = (
            max(0.0, finished - started)
            if started is not None and finished is not None
            else 0.0
        )
        cost.inclusive_seconds = elapsed
        cost.timing_failures = int(started is None or finished is None)
        cost.exclusive_seconds = max(0.0, elapsed - span.nested_seconds)
        if parent is not None and parent.active and parent.collector is collector:
            parent.nested_seconds += elapsed
        try:
            collector.costs.operations.setdefault(name, WorkCost()).add(cost)
        except Exception:
            # Diagnostics must neither fail successful work nor replace its
            # exception when a recorder/aggregation implementation fails.
            pass


def record_ingest_transfer(
    *, read_bytes: int | None = None, write_bytes: int | None = None
) -> None:
    """Record at one stream boundary, never infer bytes from expected sizes."""
    span = _span.get()
    if (
        span is None
        or not span.active
        or not span.collector.active
        or span.collector.execution != execution_owner()
    ):
        return
    if type(read_bytes) is int and read_bytes >= 0:
        span.cost.read_calls += 1
        span.cost.read_bytes += read_bytes
    if type(write_bytes) is int and write_bytes >= 0:
        span.cost.write_calls += 1
        span.cost.write_bytes += write_bytes


def record_ingest_cache(*, hit: bool, invalidated: bool = False) -> None:
    """Count decisions only in the owned cache lookup; no keys are retained."""
    span = _span.get()
    if (
        span is None
        or not span.active
        or not span.collector.active
        or span.collector.execution != execution_owner()
    ):
        return
    span.cost.cache_hits += int(hit)
    span.cost.cache_misses += int(not hit)
    span.cost.cache_invalidations += int(invalidated)
