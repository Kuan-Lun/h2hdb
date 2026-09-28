"""Bounded attribution contracts use exact independent event histories."""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from math import ceil, log2

import pytest

from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import SQLQuerySummary, query_fingerprint


@pytest.mark.parametrize("families", [63, 64, 65, 128, 129, 260])
@pytest.mark.parametrize("merge_scopes", [False, True])
def test_capacity_cycles_and_merges_bound_every_identity(
    families: int, merge_scopes: bool
) -> None:
    randomizer = random.Random(421)
    summary = SQLQuerySummary()
    exact: dict[str, float] = defaultdict(float)
    calls: dict[str, int] = defaultdict(int)
    for _cycle in range(3):
        child = SQLQuerySummary()
        target = child if merge_scopes else summary
        for index in range(families * 3):
            identity = index if index < families else randomizer.randrange(families)
            key = f"{identity:016x}"
            seconds = randomizer.randrange(1, 80) / 8
            exact[key] += seconds
            calls[key] += 1
            target.record(key, seconds, 2)
        if merge_scopes:
            summary.add(child)
        assert len(summary.entries) == len(summary._heap) == len(summary._positions)
        assert len(summary) == min(families, 64)
        for key, seconds in exact.items():
            entry = summary.entries.get(key)
            if entry is None:
                assert seconds <= summary.missing_seconds_upper
            else:
                assert entry.observed.seconds <= seconds <= entry.upper
                assert entry.observed.calls <= calls[key]
                if entry.complete:
                    assert entry.observed.calls == calls[key]
                    assert entry.observed.seconds == entry.upper == seconds


def test_late_heavy_family_rejects_first_admitted_negative_control() -> None:
    summary = SQLQuerySummary()
    degraded: dict[str, float] = {}
    events = [(f"{index:016x}", 1.0) for index in range(64)]
    events += [("f" * 16, 0.25)] * 3000
    for key, seconds in events:
        summary.record(key, seconds, 1)
        if key in degraded or len(degraded) < 64:
            degraded[key] = degraded.get(key, 0) + seconds

    def contract(top: str, lower: float, upper: float) -> None:
        assert top == "f" * 16
        assert 749 <= lower <= 750 <= upper <= 751

    top, entry = summary.top()[0]
    contract(top, entry.observed.seconds, entry.upper)
    key = max(degraded, key=degraded.__getitem__)
    with pytest.raises(AssertionError):
        contract(key, degraded[key], degraded[key])


def test_indexed_heap_update_work_is_logarithmic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    comparisons = 0
    less = SQLQuerySummary._less

    def counted(self: SQLQuerySummary, left: int, right: int) -> bool:
        nonlocal comparisons
        comparisons += 1
        return less(self, left, right)

    monkeypatch.setattr(SQLQuerySummary, "_less", counted)
    summary = SQLQuerySummary()
    for cycle in range(3):
        for index in range(260):
            before = comparisons
            summary.record(f"{index:016x}", 1.0 + cycle, 1)
            assert comparisons - before <= 2 * ceil(log2(64)) + 2
            assert len(summary._heap) <= 64
    assert comparisons > 0

    class LinearScanCounterexample(SQLQuerySummary):
        def record(self, fingerprint: str | None, elapsed: float, rows: int) -> None:
            for index in range(len(self._heap)):
                self._less(index, 0)
            super().record(fingerprint, elapsed, rows)

    degraded = LinearScanCounterexample()
    for index in range(64):
        degraded.record(f"{index:016x}", 1, 0)
    before = comparisons
    degraded.record("f" * 16, 1, 0)
    with pytest.raises(AssertionError):
        assert comparisons - before <= 2 * ceil(log2(64)) + 2


def test_nested_detail_overflow_keeps_all_descendant_sql(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("nested.attribution")
    caplog.set_level(logging.INFO, logger=logger.name)
    performance = IngestPerformance(logger, backend="sqlite")
    late = "SELECT private_late_query"
    with performance.step("analysis", "prepare", "CONTENT", 1) as outer:
        outer.record_sql_operation("sql", 1, "SELECT own", 2)
        for index in range(70):
            with performance.step("analysis", "nested", "CONTENT", 1) as child:
                child.record_sql_operation(
                    "sql",
                    100 if index == 69 else 0.25,
                    late if index == 69 else "SELECT private_early",
                    3,
                )
        assert outer.omitted_records == 6
        assert outer.nested_counters.sql_calls == 70
        assert outer.nested_counters.read_rows == 210
        assert outer.nested_counters.sql_seconds == 117.25
        assert outer.nested_queries.top()[0][0] == query_fingerprint(late)
        assert outer.nested_slowest.snapshot()[0]["seconds"] == 100
    stage = performance._stage
    assert stage is not None
    assert stage.counters.sql_calls == 1
    assert stage.nested_counters.sql_calls == 70
    performance.close()
    assert "70 completed SQL connector calls" in caplog.text
    assert "nested SQL (inclusive descendants)" in caplog.text
    assert str(query_fingerprint(late)) in caplog.text
    assert "SELECT" not in caplog.text and "private" not in caplog.text


def test_zero_duration_replacement_never_claims_complete_counts() -> None:
    summary = SQLQuerySummary(1)
    summary.record("a", 0, 1)
    summary.record("b", 0, 1)
    summary.record("a", 0, 1)
    entry = summary.entries["a"]
    assert entry.upper == entry.observed.seconds == 0
    assert entry.observed.calls == 1
    assert entry.complete is False


def test_fingerprint_failure_is_explicit_and_survives_merge() -> None:
    child = SQLQuerySummary()
    child.record(None, 3.0, 7)
    root = SQLQuerySummary()
    root.add(child)
    summary = root.snapshot()
    assert summary["unfingerprinted_calls"] == 1
    assert summary["unfingerprinted_seconds"] == 3.0
    assert summary["unfingerprinted_returned_rows"] == 7
    assert summary["retained_families"] == 0


def test_unfingerprinted_only_scope_is_visible_at_info(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import ingest_performance

    monkeypatch.setattr(ingest_performance, "query_fingerprint", lambda _query: None)
    logger = logging.getLogger("unfingerprinted.attribution")
    caplog.set_level(logging.INFO, logger=logger.name)
    performance = IngestPerformance(logger, backend="sqlite")
    with performance.step("analysis", "prepare", "CONTENT", 1) as sample:
        sample.record_sql_operation("sql", 3.0, "SELECT private", 7)
    performance.close()
    assert "unfingerprinted_calls=1" in caplog.text
    assert "unfingerprinted_seconds=3.000000" in caplog.text
    assert "SELECT" not in caplog.text and "private" not in caplog.text
