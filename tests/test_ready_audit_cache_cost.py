"""Finite runtime/cost correspondence, not a proof of NAS wall-clock latency."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pytest
import test_catalog_plan_preparation_performance as canonical_fixture
import test_catalog_refinement_runtime as catalog_fixture

from h2hdb import catalog_refinement, vnext_identity

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = b"tag_value_utf8_v1"
CAPACITY = 128
MAX_VALUE_BYTES = 65536
MAX_TOTAL_BYTES = CAPACITY * MAX_VALUE_BYTES
WORKING_SETS = (127, 128, 129, 256)
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
        capacity: int = CAPACITY,
        max_value_bytes: int = MAX_VALUE_BYTES,
        max_total_bytes: int = MAX_TOTAL_BYTES,
    ) -> None:
        self.capacity = capacity
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
        if event.succeeds and event.size <= self.max_value_bytes:
            candidates = [*self.entries, (event.key, event.size)]
            self.entries = next(
                candidates[start:]
                for start in range(len(candidates) + 1)
                if len(candidates[start:]) <= self.capacity
                and sum(size for _key, size in candidates[start:])
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
        for index, working_set in enumerate(WORKING_SETS)
        for event in _cyclic_trace(working_set, offset=index * 1000)
    ]
    # Fill the full byte budget, then touch an old entry before introducing one
    # extra entry. Failed and oversized misses must leave this state unchanged.
    trace.extend(_Access(10000 + key, MAX_VALUE_BYTES) for key in range(CAPACITY))
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
        assert len(actual.entries) <= CAPACITY
        assert actual.byte_count <= MAX_TOTAL_BYTES
        misses = actual.misses

    # This documents the actual inefficiency; an oversized cyclic working set
    # does not satisfy the retained-workload premise of one validation per key.
    if working_set <= CAPACITY:
        _assert_retained_set_cost(misses, distinct=working_set)
    else:
        assert misses == working_set * LAPS


def test_lean_cost_execution_matches_runtime_and_independent_recency_oracle() -> None:
    trace = _conformance_trace()
    # The CLI's per-miss cost of 3 is symbolic, including failed and multileaf
    # events. Only the separate successful single-leaf SQL test identifies these
    # units with physical statements; this trace does not extend that premise.
    completed = subprocess.run(
        [
            "lean",
            "--run",
            str(ROOT / "verification" / "lean" / "ReadyAuditCacheCost.lean"),
            str(CAPACITY),
            str(MAX_VALUE_BYTES),
            str(MAX_TOTAL_BYTES),
            "3",
            *(f"{event.key}:{event.size}:{int(event.succeeds)}" for event in trace),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    lines = completed.stdout.splitlines()
    assert len(lines) == len(trace), completed.stdout
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference()
    misses = 0
    for event, line in zip(trace, lines, strict=True):
        hit, missed, units, count, byte_count, contents = line.split(",", maxsplit=5)
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
    # These valid, smaller limits isolate byte pressure from entry pressure.
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_ENTRIES", 3
    )
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES", 6
    )
    monkeypatch.setattr(
        catalog_refinement, "_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES", 10
    )
    cache = catalog_refinement._CanonicalValidationCache()
    reference = _Reference(capacity=3, max_value_bytes=6, max_total_bytes=10)
    trace = [_Access(key, size) for key, size in ((1, 4), (2, 4), (1, 4), (3, 6))]
    misses = 0
    for event in trace:
        actual = _runtime_step(cache, event, misses=misses)
        assert actual == reference.access(event)
        misses = actual.misses
    assert actual.entries == ((1, 4), (3, 6))
    assert actual.byte_count == 10


def test_replacement_and_invalid_spool_preserve_exact_byte_accounting() -> None:
    cache = catalog_refinement._CanonicalValidationCache()
    digest = b"a" * 32
    with BytesIO(b"large") as spool:
        cache.remember(digest, DOMAIN, spool, byte_count=5)
    with BytesIO(b"x") as spool:
        cache.remember(digest, DOMAIN, spool, byte_count=1)
    assert cache._byte_count == 1
    assert tuple(cache._values.values()) == (b"x",)
    before = tuple(cache._values.items())
    for payload, declared in ((b"short", 6), (b"long", 3)):
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
            payloads = tuple(f"cost-value-{key}".encode() for key in range(256))
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
                    expected_miss = _lap == 0 or working_set > CAPACITY
                    assert len(queries) == (3 if expected_miss else 0)
                    assert all(
                        query.lstrip().startswith(("SELECT", "WITH"))
                        for query in queries
                    )
                    misses += expected_miss
            assert misses == (
                working_set if working_set <= CAPACITY else working_set * LAPS
            )
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


def _assert_retained_set_cost(misses: int, *, distinct: int) -> None:
    assert misses == distinct, "retained working set was validated more than once"


def test_cost_oracle_rejects_always_miss_despite_correct_returned_payloads() -> None:
    cache = _AlwaysMissCache()
    trace = _cyclic_trace(CAPACITY)
    misses = 0
    for event in trace:
        actual = _runtime_step(cache, event, misses=misses)
        # Returning the authoritative bytes after every miss remains semantically
        # correct, but violates the independently stated retained-workload cost.
        assert cache._values[(event.key.to_bytes(32, "big"), DOMAIN)] == _payload(event)
        misses = actual.misses
    assert misses == CAPACITY * LAPS
    with pytest.raises(AssertionError, match="validated more than once"):
        _assert_retained_set_cost(misses, distinct=CAPACITY)
