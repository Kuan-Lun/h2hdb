from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from io import BytesIO
from typing import BinaryIO, cast

import pytest
from test_vnext_artifact_render import _Adapter, _reference, _render, _verify

import h2hdb.vnext_artifact_render as render_module
from h2hdb.domain import ArtifactSourceRole
from h2hdb.ingest_performance import IngestPerformance
from h2hdb.ingest_work_performance import (
    WorkCost,
    WorkCosts,
    WorkOperation,
    collect_ingest_work,
    measure_ingest_work,
    record_ingest_cache,
    record_ingest_transfer,
)
from h2hdb.source_errors import VNextSourceChangedError


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


def test_nested_wall_attribution_and_actual_transfers_do_not_double_count() -> None:
    costs = WorkCosts()
    clock = _Clock()
    with collect_ingest_work(costs, clock):
        with measure_ingest_work("artifact_source_copy"):
            clock.now = 1.0
            record_ingest_transfer(read_bytes=7, write_bytes=3)
            with measure_ingest_work("artifact_source_open"):
                record_ingest_transfer(read_bytes=11)
                clock.now = 4.0
            clock.now = 6.0
            record_ingest_transfer(read_bytes=0, write_bytes=4)
    outer = costs.operations["artifact_source_copy"]
    inner = costs.operations["artifact_source_open"]
    assert (outer.inclusive_seconds, outer.exclusive_seconds) == (6.0, 3.0)
    assert (inner.inclusive_seconds, inner.exclusive_seconds) == (3.0, 3.0)
    assert (outer.read_calls, outer.read_bytes) == (2, 7)
    assert (outer.write_calls, outer.write_bytes) == (2, 7)
    assert (inner.read_calls, inner.read_bytes) == (1, 11)
    assert sum(cost.exclusive_seconds for cost in costs.operations.values()) == 6


def test_failure_keeps_original_exception_counts_partial_io_and_restores_context() -> (
    None
):
    costs = WorkCosts()
    failure = OSError("fixture partial transfer")
    with pytest.raises(OSError) as caught:
        with collect_ingest_work(costs, _Clock()):
            with measure_ingest_work("artifact_source_copy"):
                record_ingest_transfer(read_bytes=4, write_bytes=2)
                raise failure
    assert caught.value is failure
    cost = costs.operations["artifact_source_copy"]
    assert (cost.calls, cost.failures, cost.read_bytes, cost.write_bytes) == (
        1,
        1,
        4,
        2,
    )
    with measure_ingest_work("artifact_archive_render"):
        record_ingest_transfer(read_bytes=999)
    assert len(costs.operations) == 1


@pytest.mark.parametrize("failed", [False, True])
def test_recorder_failure_does_not_replace_work_outcome(
    monkeypatch: pytest.MonkeyPatch, failed: bool
) -> None:
    def broken(_self: WorkCost, _other: WorkCost) -> None:
        raise RuntimeError("observer aggregation failed")

    monkeypatch.setattr(WorkCost, "add", broken)
    failure = OSError("actual I/O failure")

    def run() -> None:
        with collect_ingest_work(WorkCosts(), _Clock()):
            with measure_ingest_work("artifact_source_copy"):
                record_ingest_transfer(read_bytes=4)
                if failed:
                    raise failure

    if failed:
        with pytest.raises(OSError) as caught:
            run()
        assert caught.value is failure
    else:
        run()


def test_broken_clock_does_not_change_work_but_reports_missing_timing() -> None:
    def broken() -> float:
        raise RuntimeError("observer clock")

    costs = WorkCosts()
    with collect_ingest_work(costs, broken):
        with measure_ingest_work("artifact_source_copy"):
            record_ingest_transfer(read_bytes=4)
    cost = costs.operations["artifact_source_copy"]
    assert (cost.calls, cost.failures, cost.timing_failures, cost.read_bytes) == (
        1,
        0,
        1,
        4,
    )
    assert cost.inclusive_seconds == 0


def test_copied_context_cannot_attribute_another_thread_to_owner() -> None:
    costs = WorkCosts()

    def other_thread() -> None:
        record_ingest_transfer(read_bytes=999)
        with measure_ingest_work("artifact_source_open"):
            record_ingest_transfer(read_bytes=999)

    with collect_ingest_work(costs, _Clock()):
        with measure_ingest_work("artifact_source_copy"):
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(copy_context().run, other_thread).result()
            record_ingest_transfer(read_bytes=4)
    assert len(costs.operations) == 1
    assert costs.operations["artifact_source_copy"].read_bytes == 4


def test_expired_copied_context_cannot_add_after_owner_has_finished() -> None:
    costs = WorkCosts()
    with collect_ingest_work(costs, _Clock()):
        with measure_ingest_work("artifact_source_copy"):
            active = copy_context()
            record_ingest_transfer(read_bytes=4)

    def stale_work() -> None:
        record_ingest_transfer(read_bytes=99)
        with measure_ingest_work("artifact_archive_hash"):
            record_ingest_transfer(read_bytes=999)

    active.run(stale_work)
    assert set(costs.operations) == {"artifact_source_copy"}
    assert costs.operations["artifact_source_copy"].read_bytes == 4


def test_async_child_does_not_inherit_parent_cost_ownership() -> None:
    costs = WorkCosts()

    async def child() -> None:
        record_ingest_transfer(read_bytes=99)
        with measure_ingest_work("artifact_archive_hash"):
            record_ingest_transfer(read_bytes=999)

    async def parent() -> None:
        with collect_ingest_work(costs, _Clock()):
            with measure_ingest_work("artifact_source_copy"):
                await asyncio.create_task(child())
                record_ingest_transfer(read_bytes=4)

    asyncio.run(parent())
    assert set(costs.operations) == {"artifact_source_copy"}
    assert costs.operations["artifact_source_copy"].read_bytes == 4


def test_closed_operation_vocabulary_bounds_untrusted_diagnostic_labels() -> None:
    costs = WorkCosts()
    with collect_ingest_work(costs, _Clock()):
        for number in range(1000):
            with measure_ingest_work(cast(WorkOperation, f"path-sensitive-{number}")):
                record_ingest_transfer(read_bytes=1)
        with measure_ingest_work("artifact_source_copy"):
            record_ingest_transfer(read_bytes=cast(int, "bad stream return"))
            record_ingest_transfer(read_bytes=4)
    assert set(costs.operations) == {"artifact_source_copy"}
    assert costs.operations["artifact_source_copy"].read_bytes == 4
    assert "path-sensitive" not in costs.text()


def test_cache_decisions_survive_step_accumulation_without_retaining_keys() -> None:
    total = WorkCosts()
    for hit, invalidated in ((False, False), (True, False), (False, True)):
        costs = WorkCosts()
        with collect_ingest_work(costs, _Clock()):
            with measure_ingest_work("artifact_cache_lookup"):
                record_ingest_cache(hit=hit, invalidated=invalidated)
        total.add(costs)
    cost = total.operations["artifact_cache_lookup"]
    assert (
        cost.calls,
        cost.cache_hits,
        cost.cache_misses,
        cost.cache_invalidations,
    ) == (3, 1, 2, 1)
    assert "cache_hits=1,cache_misses=2,cache_invalidations=1" in total.text()


@pytest.mark.parametrize("size", [65535, 65536, 65537])
def test_real_render_and_reuse_byte_oracle_across_chunk_boundary(size: int) -> None:
    metadata = b"metadata"
    payload = b"p" * size
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", metadata),
        _reference(1, ArtifactSourceRole.PAGE, b"page", payload),
    )
    adapter = _Adapter({b"metadata.txt": metadata, b"page": payload})
    costs = WorkCosts()
    expected = len(metadata) + len(payload)
    with collect_ingest_work(costs, _Clock()):
        for _cycle in range(3):
            with _render(adapter, references) as rendered:
                assert rendered.archive.read() == metadata + payload
            _verify(adapter, references)
    assert costs.operations["artifact_source_copy"].read_bytes == expected * 3
    assert costs.operations["artifact_source_copy"].write_bytes == expected * 3
    assert costs.operations["artifact_source_rehash"].read_bytes == expected * 3
    assert costs.operations["artifact_archive_hash"].read_bytes == expected * 3
    assert costs.operations["artifact_cached_source_verify"].read_bytes == expected * 3
    assert costs.operations["artifact_source_open"].calls == 12
    assert len(costs.operations) == 6


def test_byte_counters_match_independent_source_stream_reads() -> None:
    reads: list[int] = []
    payload = b"source member" * 10_000

    class CountedSource(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            part = super().read(size)
            reads.append(len(part))
            return part

    class CountedAdapter(_Adapter):
        def open_source(
            self,
            *,
            source_root_components: tuple[str, ...],
            gallery_locator_components: tuple[str, ...],
            source_name: bytes,
        ) -> BinaryIO:
            del source_root_components, gallery_locator_components, source_name
            return CountedSource(payload)

    adapter = CountedAdapter({b"metadata": payload})
    references = (_reference(0, ArtifactSourceRole.METADATA, b"metadata", payload),)
    costs = WorkCosts()
    with collect_ingest_work(costs, _Clock()):
        with _render(adapter, references):
            pass
        _verify(adapter, references)
    reported = [
        costs.operations[name]
        for name in ("artifact_source_copy", "artifact_cached_source_verify")
    ]
    assert sum(item.read_bytes for item in reported) == sum(reads) == 2 * len(payload)
    assert sum(item.read_calls for item in reported) == len(reads)


def test_cost_oracle_rejects_same_output_with_an_extra_complete_rehash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = render_module._StagedMembers.verify_unchanged

    def repeated(staged: render_module._StagedMembers) -> None:
        original(staged)
        original(staged)

    monkeypatch.setattr(render_module._StagedMembers, "verify_unchanged", repeated)
    payload = b"metadata" * 100
    adapter = _Adapter({b"metadata": payload})
    references = (_reference(0, ArtifactSourceRole.METADATA, b"metadata", payload),)
    costs = WorkCosts()
    with collect_ingest_work(costs, _Clock()):
        with _render(adapter, references) as rendered:
            assert rendered.archive.read() == payload
    with pytest.raises(AssertionError, match="unnecessary reread"):
        assert costs.operations["artifact_source_rehash"].read_bytes == len(payload), (
            "unnecessary reread"
        )


def test_source_corruption_still_fails_with_observer() -> None:
    costs = WorkCosts()
    adapter = _Adapter({b"metadata.txt": b"changed"})
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),
    )
    with pytest.raises(VNextSourceChangedError):
        with collect_ingest_work(costs, _Clock()):
            _render(adapter, references)
    assert costs.operations["artifact_source_copy"].failures == 1
    assert costs.operations["artifact_source_copy"].read_bytes == len(b"changed")


def test_info_accumulates_completed_steps_without_emitting_per_transfer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock()
    logger = logging.getLogger("h2hdb.ingest_work_test")
    performance = IngestPerformance(logger, backend="sqlite", clock=clock)
    with caplog.at_level(logging.INFO, logger=logger.name):
        for cycle in range(3):
            with performance.step(
                "publication", "prepare", "PREPARE_ARTIFACT", 9
            ) as sample:
                with measure_ingest_work("artifact_source_copy"):
                    for _chunk in range(129):
                        record_ingest_transfer(read_bytes=7)
                    clock.now += 1
                sample.terminal = cycle == 2
    summaries = [
        record.message for record in caplog.records if "local work" in record.message
    ]
    assert len(summaries) == 1
    assert "ingest generation 9" in summaries[0]
    assert "calls=3,failures=0,timing_failures=0" in summaries[0]
    assert "read_calls=387,logical_read_bytes=2709" in summaries[0]
    assert "inclusive_seconds=3.000000,exclusive_seconds=3.000000" in summaries[0]
    assert all(record.levelno == logging.INFO for record in caplog.records)
