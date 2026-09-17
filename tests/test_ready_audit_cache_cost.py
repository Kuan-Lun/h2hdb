"""Finite runtime/cost correspondence, not a proof of NAS wall-clock latency."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import pytest
import test_catalog_plan_preparation_performance as canonical_fixture
import test_catalog_refinement_runtime as catalog_fixture

from h2hdb import catalog_refinement, vnext_identity

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = b"tag_value_utf8_v1"
ENTRY_BYTES = 512
MAX_VALUE_BYTES = 65536
MAX_TOTAL_BYTES = 8 * 1024 * 1024
MAX_ENTRIES = MAX_TOTAL_BYTES // ENTRY_BYTES
WORKING_SETS = (127, 128, 129, 256, 486, 1024)
LAPS = 3


@dataclass(frozen=True, slots=True)
class _Access:
    key: int
    size: int
    succeeds: bool = True


@dataclass(frozen=True, slots=True)
class _Step:
    hit: bool
    misses: int
    entries: tuple[tuple[int, int], ...]

    @property
    def byte_count(self) -> int:
        return sum(size for _key, size in self.entries)


class _Reference:
    """Choose the longest affordable recency suffix; do not copy OrderedDict code."""

    def __init__(
        self,
        *,
        entry_bytes: int = ENTRY_BYTES,
        max_value_bytes: int = MAX_VALUE_BYTES,
        max_total_bytes: int = MAX_TOTAL_BYTES,
    ) -> None:
        self.entry_bytes = entry_bytes
        self.max_value_bytes = max_value_bytes
        self.max_total_bytes = max_total_bytes
        self.entries: list[tuple[int, int]] = []
        self.misses = 0

    def access(self, event: _Access) -> _Step:
        matches = [entry for entry in self.entries if entry[0] == event.key]
        if matches:
            self.entries = [
                entry for entry in self.entries if entry[0] != event.key
            ] + matches
            return _Step(True, self.misses, tuple(self.entries))
        self.misses += 1
        if (
            event.succeeds
            and event.size <= self.max_value_bytes
            and event.size + self.entry_bytes <= self.max_total_bytes
        ):
            candidates = [*self.entries, (event.key, event.size)]
            self.entries = next(
                candidates[start:]
                for start in range(len(candidates) + 1)
                if sum(size + self.entry_bytes for _key, size in candidates[start:])
                <= self.max_total_bytes
            )
        return _Step(False, self.misses, tuple(self.entries))


def _payload(event: _Access) -> bytes:
    return bytes([event.key % 251]) * event.size


def _runtime_step(
    cache: catalog_refinement._CanonicalValidationCache,
    event: _Access,
    *,
    misses: int,
) -> _Step:
    digest = event.key.to_bytes(32, "big")
    opened = cache.open(digest, DOMAIN)
    hit = opened is not None
    if opened is not None:
        spool, byte_count = opened
        with spool:
            assert spool.read() == _payload(event)
        assert byte_count == event.size
    else:
        misses += 1
        if event.succeeds:
            with BytesIO(_payload(event)) as spool:
                cache.remember(digest, DOMAIN, spool, byte_count=event.size)
    entries = tuple(
        (int.from_bytes(key[0], "big"), len(payload))
        for key, payload in cache._values.items()
    )
    assert all(domain == DOMAIN for _digest, domain in cache._values)
    assert cache._byte_count == sum(size for _key, size in entries)
    assert cache._charged_byte_count == (
        cache._byte_count
        + len(entries) * catalog_refinement._CANONICAL_VALIDATION_CACHE_ENTRY_BYTES
    )
    return _Step(hit, misses, entries)


def _cyclic_trace(working_set: int, *, offset: int = 0) -> list[_Access]:
    return [
        _Access(offset + key, key % 11 + 1)
        for _lap in range(LAPS)
        for key in range(working_set)
    ]


def _conformance_trace() -> list[_Access]:
    trace = [
        event
        for index, working_set in enumerate((127, 128, 129, 256))
        for event in _cyclic_trace(working_set, offset=index * 1000)
    ]
    # Fill the full byte budget, then touch an old entry before introducing one
    # extra entry. Failed and oversized misses must leave this state unchanged.
    full_values = MAX_TOTAL_BYTES // (MAX_VALUE_BYTES + ENTRY_BYTES)
    trace.extend(
        _Access(10000 + key, MAX_VALUE_BYTES) for key in range(full_values + 1)
    )
    trace.extend(
        (
            _Access(10000, MAX_VALUE_BYTES),
            _Access(20000, MAX_VALUE_BYTES + 1),
            _Access(20000, MAX_VALUE_BYTES + 1),
            _Access(20001, 1, succeeds=False),
            _Access(20001, 1, succeeds=False),
            _Access(20002, 0),
            _Access(20002, 0),
            _Access(20003, MAX_VALUE_BYTES - 1),
            _Access(10000, MAX_VALUE_BYTES),
        )
    )
    return trace


@pytest.mark.parametrize("working_set", WORKING_SETS)
def test_runtime_lru_retention_and_cyclic_capacity_cliff(working_set: int) -> None:
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference()
    misses = 0
    for event in _cyclic_trace(working_set):
        actual = _runtime_step(cache, event, misses=misses)
        expected = reference.access(event)
        assert actual == expected
        assert len(actual.entries) <= MAX_ENTRIES
        assert cache._charged_byte_count <= MAX_TOTAL_BYTES
        misses = actual.misses

    # Tiny values across the old 128-entry cliff fit the charged byte budget.
    # Retaining only 128 or 512 entries fails this regression at larger sets.
    _assert_retained_set_cost(misses, distinct=working_set)


def _conformance_scenarios() -> tuple[tuple[int, int, list[_Access]], ...]:
    return (
        (MAX_VALUE_BYTES, MAX_TOTAL_BYTES, _conformance_trace()),
        (
            6,
            2 * ENTRY_BYTES + 10,
            [
                _Access(1, 4),
                _Access(2, 4),
                _Access(1, 4),
                _Access(3, 6),
                _Access(4, 0),
                _Access(3, 6),
                _Access(5, 6, succeeds=False),
                _Access(6, 7),
            ],
        ),
        (
            6,
            ENTRY_BYTES + 4,
            [
                _Access(1, 4),
                _Access(2, 5),
                _Access(3, 0),
                _Access(1, 4),
                _Access(2, 5),
            ],
        ),
    )


def test_lean_cost_execution_matches_runtime_and_independent_recency_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenarios = _conformance_scenarios()
    args: list[str] = []
    for max_value_bytes, max_total_bytes, trace in scenarios:
        if args:
            args.append("--next-scenario")
        args.extend(
            (
                str(ENTRY_BYTES),
                str(max_value_bytes),
                str(max_total_bytes),
                "3",
                *(f"{event.key}:{event.size}:{int(event.succeeds)}" for event in trace),
            )
        )
    # The CLI's per-miss cost of 3 is symbolic, including failed and multileaf
    # events. Only the separate successful single-leaf SQL test identifies these
    # units with physical statements; this trace does not extend that premise.
    # Compile once for all three independent scenarios to respect the merge
    # profile's shared deadline; each scenario still starts with an empty cache.
    completed = subprocess.run(
        [
            "lean",
            "--run",
            str(ROOT / "verification" / "lean" / "ReadyAuditCacheCost.lean"),
            *args,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    lines = completed.stdout.splitlines()
    assert len(lines) == sum(len(trace) for _value, _total, trace in scenarios)
    next_line = iter(lines)
    for max_value_bytes, max_total_bytes, trace in scenarios:
        monkeypatch.setattr(
            catalog_refinement,
            "_CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES",
            max_value_bytes,
        )
        monkeypatch.setattr(
            catalog_refinement,
            "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES",
            max_total_bytes,
        )
        cache = catalog_refinement._CanonicalValidationCache()
        reference = _Reference(
            max_value_bytes=max_value_bytes, max_total_bytes=max_total_bytes
        )
        misses = 0
        for event in trace:
            hit, missed, units, count, byte_count, contents = next(next_line).split(
                ",", maxsplit=5
            )
            entries = tuple(
                tuple(map(int, item.split(":"))) for item in contents.split("|") if item
            )
            actual = _runtime_step(cache, event, misses=misses)
            expected = reference.access(event)
            assert actual == expected
            assert int(hit) == actual.hit
            assert int(missed) == actual.misses
            assert int(units) == 3 * actual.misses
            assert int(count) == len(actual.entries)
            assert int(byte_count) == actual.byte_count
            assert entries == actual.entries
            misses = actual.misses


def test_byte_budget_eviction_uses_actual_payload_sizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # These smaller limits exercise exact charge pressure with real key overhead.
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES", 6
    )
    monkeypatch.setattr(
        catalog_refinement,
        "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES",
        2 * ENTRY_BYTES + 10,
    )
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference(max_value_bytes=6, max_total_bytes=2 * ENTRY_BYTES + 10)
    trace = [_Access(key, size) for key, size in ((1, 4), (2, 4), (1, 4), (3, 6))]
    misses = 0
    for event in trace:
        actual = _runtime_step(cache, event, misses=misses)
        assert actual == reference.access(event)
        misses = actual.misses
    assert actual.entries == ((1, 4), (3, 6))
    assert actual.byte_count == 10
    assert cache._charged_byte_count == 2 * ENTRY_BYTES + 10


@pytest.mark.parametrize("working_set", (127, 128, 129))
def test_cyclic_working_set_across_actual_charged_budget(
    monkeypatch: pytest.MonkeyPatch, working_set: int
) -> None:
    budget = 128 * (ENTRY_BYTES + 1)
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES", budget
    )
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference(max_total_bytes=budget)
    misses = 0
    for _lap in range(LAPS):
        for key in range(working_set):
            event = _Access(key, 1)
            actual = _runtime_step(cache, event, misses=misses)
            assert actual == reference.access(event)
            assert cache._charged_byte_count <= budget
            misses = actual.misses
    if working_set <= 128:
        _assert_retained_set_cost(misses, distinct=working_set)
    else:
        # A finite cache can still thrash when the retained workload exceeds its
        # real budget. This is separate from the removed arbitrary entry limit.
        assert misses == working_set * LAPS


def test_empty_payloads_are_charged_and_cannot_grow_without_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget = 4 * ENTRY_BYTES
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES", budget
    )
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference(max_total_bytes=budget)
    misses = 0
    for key in range(5):
        actual = _runtime_step(cache, _Access(key, 0), misses=misses)
        assert actual == reference.access(_Access(key, 0))
        assert cache._charged_byte_count <= budget
        misses = actual.misses
    assert actual.entries == ((1, 0), (2, 0), (3, 0), (4, 0))
    assert cache._byte_count == 0
    assert cache._charged_byte_count == budget


def test_unaffordable_value_and_unbounded_keys_cannot_change_retained_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        catalog_refinement,
        "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES",
        ENTRY_BYTES + 4,
    )
    cache = catalog_refinement._CanonicalValidationCache()
    _runtime_step(cache, _Access(1, 4), misses=0)
    before = tuple(cache._values.items())
    for digest, domain, payload in (
        (b"b" * 32, DOMAIN, b"large"),
        (b"short", DOMAIN, b""),
        (b"c" * 32, b"d" * 65, b""),
        (b"c" * 32, b"", b""),
    ):
        with BytesIO(payload) as spool:
            cache.remember(digest, domain, spool, byte_count=len(payload))
        assert tuple(cache._values.items()) == before
        assert cache._charged_byte_count == ENTRY_BYTES + 4


def test_replacement_and_invalid_spool_preserve_exact_byte_accounting() -> None:
    cache = catalog_refinement._CanonicalValidationCache()
    digest = b"a" * 32
    with BytesIO(b"large") as spool:
        cache.remember(digest, DOMAIN, spool, byte_count=5)
    with BytesIO(b"x") as spool:
        cache.remember(digest, DOMAIN, spool, byte_count=1)
    assert cache._byte_count == 1
    assert cache._charged_byte_count == ENTRY_BYTES + 1
    assert tuple(cache._values.values()) == (b"x",)
    before = tuple(cache._values.items())
    for payload, declared in ((b"short", 6), (b"long", 3), (b"", -1)):
        with (
            BytesIO(payload) as spool,
            pytest.raises(
                catalog_refinement.CatalogSemanticValidationError,
                match="spool changed",
            ),
        ):
            cache.remember(digest, DOMAIN, spool, byte_count=declared)
        assert tuple(cache._values.items()) == before
        assert cache._byte_count == 1
        assert cache._charged_byte_count == ENTRY_BYTES + 1


def test_domain_and_snapshot_scope_cannot_reuse_an_unrelated_validation() -> None:
    first = catalog_refinement._CanonicalValidationCache()
    second = catalog_refinement._CanonicalValidationCache()
    event = _Access(7, 11)
    assert not _runtime_step(first, event, misses=0).hit
    assert _runtime_step(first, event, misses=1).hit
    assert not _runtime_step(second, event, misses=0).hit
    assert first.open(event.key.to_bytes(32, "big"), b"source_title_utf8_v1") is None


def test_real_sql_single_leaf_cost_tracks_threshold_misses(tmp_path: Path) -> None:
    connector = catalog_fixture._generated_catalog_database(tmp_path / "cost.sqlite3")
    try:
        with connector.transaction():
            payloads = tuple(
                f"cost-value-{key}".encode() for key in range(max(WORKING_SETS))
            )
            digests = tuple(
                canonical_fixture._seed_tag_value(connector, value)
                for value in payloads
            )
        connector.begin_read()
        queries: list[str] = []
        connector.connection.set_trace_callback(queries.append)
        for working_set in WORKING_SETS:
            cache = catalog_refinement._CanonicalValidationCache()
            misses = 0
            for _lap in range(LAPS):
                for key in range(working_set):
                    queries.clear()
                    spool, count = catalog_refinement._validated_canonical_spool(
                        connector,
                        digests[key],
                        expected_domain=DOMAIN,
                        detail="single-leaf cost correspondence",
                        cache=cache,
                    )
                    with spool:
                        assert spool.read() == payloads[key]
                    assert count == len(payloads[key])
                    expected_miss = _lap == 0
                    assert len(queries) == (3 if expected_miss else 0)
                    assert all(
                        query.lstrip().startswith(("SELECT", "WITH"))
                        for query in queries
                    )
                    misses += expected_miss
            assert misses == working_set
    finally:
        connector.connection.set_trace_callback(None)
        connector.rollback()
        connector.close()


def test_real_sql_failure_and_multileaf_bypass_are_separate_cost_regimes(
    tmp_path: Path,
) -> None:
    connector = catalog_fixture._generated_catalog_database(tmp_path / "shapes.sqlite3")
    try:
        payload = b"z" * (MAX_VALUE_BYTES + 1)
        with connector.transaction():
            digest = canonical_fixture._seed_tag_value(connector, payload)
            short_digest = canonical_fixture._seed_tag_value(connector, b"short")
        tree = vnext_identity.build_canonical_value_tree(
            digest, len(payload), (payload,)
        )
        assert len(tree.pages) > 1
        connector.begin_read()
        cache = catalog_refinement._CanonicalValidationCache()
        queries: list[str] = []
        connector.connection.set_trace_callback(queries.append)
        costs: list[int] = []
        for _lap in range(LAPS):
            queries.clear()
            spool, count = catalog_refinement._validated_canonical_spool(
                connector,
                digest,
                expected_domain=DOMAIN,
                detail="multileaf bypass",
                cache=cache,
            )
            with spool:
                assert spool.read() == payload
            assert count == len(payload)
            assert cache._byte_count == 0
            assert not cache._values
            costs.append(len(queries))
        assert costs[0] > 3
        assert costs == [costs[0]] * LAPS

        for _lap in range(LAPS):
            queries.clear()
            with pytest.raises(
                catalog_refinement.CatalogSemanticValidationError,
                match="wrong domain",
            ):
                catalog_refinement._validated_canonical_spool(
                    connector,
                    short_digest,
                    expected_domain=b"source_title_utf8_v1",
                    detail="wrong domain",
                    cache=cache,
                )
            assert len(queries) == 3
            assert not cache._values
    finally:
        connector.connection.set_trace_callback(None)
        connector.rollback()
        connector.close()


class _AlwaysMissCache(catalog_refinement._CanonicalValidationCache):
    def open(self, value_sha256: bytes, expected_domain: bytes) -> None:
        return None


class _EntryCappedCache(catalog_refinement._CanonicalValidationCache):
    """Negative control: reproduce independent entry limits, with exact bytes."""

    def __init__(self, capacity: int) -> None:
        super().__init__()
        self.capacity = capacity

    def remember(
        self,
        value_sha256: bytes,
        expected_domain: bytes,
        spool: BinaryIO,
        *,
        byte_count: int,
    ) -> None:
        super().remember(value_sha256, expected_domain, spool, byte_count=byte_count)
        while len(self._values) > self.capacity:
            _key, payload = self._values.popitem(last=False)
            self._byte_count -= len(payload)


def _assert_retained_set_cost(misses: int, *, distinct: int) -> None:
    assert misses == distinct, "retained working set was validated more than once"


def test_cost_oracle_rejects_always_miss_despite_correct_returned_payloads() -> None:
    cache = _AlwaysMissCache()
    trace = _cyclic_trace(256)
    misses = 0
    for event in trace:
        actual = _runtime_step(cache, event, misses=misses)
        # Returning the authoritative bytes after every miss remains semantically
        # correct, but violates the independently stated retained-workload cost.
        assert cache._values[(event.key.to_bytes(32, "big"), DOMAIN)] == _payload(event)
        misses = actual.misses
    assert misses == 256 * LAPS
    with pytest.raises(AssertionError, match="validated more than once"):
        _assert_retained_set_cost(misses, distinct=256)


@pytest.mark.parametrize("capacity", (128, 512))
def test_cost_oracle_rejects_arbitrary_entry_cap_with_unused_budget(
    capacity: int,
) -> None:
    cache = _EntryCappedCache(capacity)
    working_set = capacity + 1
    misses = 0
    for event in _cyclic_trace(working_set):
        actual = _runtime_step(cache, event, misses=misses)
        assert cache._charged_byte_count < MAX_TOTAL_BYTES
        misses = actual.misses
    assert misses == working_set * LAPS
    with pytest.raises(AssertionError, match="validated more than once"):
        _assert_retained_set_cost(misses, distinct=working_set)
