"""Preparation progress is observable without weakening source authority."""

from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import VNextSourcePreparationOperation as Operation
from h2hdb import VNextSourcePreparationProgress as Progress
from h2hdb.vnext_source_build_repository import SourceDiscoveryPlan


class _CacheSpillConnection(sqlite3.Connection):
    """Request the exclusive lock that rollback-journal cache spill needs."""

    def _require_spill_lock(self, sql: str) -> None:
        if sql.startswith("UPDATE locator_entries") and not self.in_transaction:
            # Deterministic even for tiny inventories: no wall-clock assertion
            # or platform-dependent cache-size threshold is needed.
            super().execute("BEGIN EXCLUSIVE")

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        self._require_spill_lock(sql)
        return super().execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any, /) -> sqlite3.Cursor:
        self._require_spill_lock(sql)
        return super().executemany(sql, parameters)


def test_discovery_order_can_acquire_cache_spill_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained second read cursor would make the exclusive lock fail BUSY."""

    original = sqlite3.connect

    def connect(path: str | Path) -> sqlite3.Connection:
        return original(path, timeout=0, factory=_CacheSpillConnection)

    monkeypatch.setattr(sqlite3, "connect", connect)
    with SourceDiscoveryPlan.from_locators(
        ("nested", f"gallery-{index:05d}") for index in range(769)
    ) as plan:
        locators = tuple(
            locator
            for start in range(0, plan.gallery_count, 256)
            for locator in plan._page(start)
        )
        assert tuple(item.position for item in locators) == tuple(range(769))
        digests = tuple(item.locator_sha256 for item in locators)
        assert digests == tuple(sorted(digests))


def test_discovery_observations_have_exact_totals() -> None:
    observations: list[Progress] = []
    with SourceDiscoveryPlan.from_locators(
        ((f"gallery-{index:05d}",) for index in range(259)),
        progress=observations.append,
    ):
        assert observations[0] == Progress(Operation.DISCOVERY_TRANSFER, 0)
        assert Progress(Operation.DISCOVERY_TRANSFER, 259, 259) in observations
        order = [
            value
            for value in observations
            if value.operation == Operation.DISCOVERY_ORDER
        ]
        assert order == [
            Progress(Operation.DISCOVERY_ORDER, 0, 259),
            Progress(Operation.DISCOVERY_ORDER, 256, 259),
            Progress(Operation.DISCOVERY_ORDER, 259, 259),
        ]


def test_observer_failure_does_not_change_discovery_identity() -> None:
    def fail(_progress: Progress) -> None:
        raise RuntimeError("observer failed")

    locators = (("first",), ("second",))
    with SourceDiscoveryPlan.from_locators(locators) as expected:
        with SourceDiscoveryPlan.from_locators(locators, progress=fail) as observed:
            assert observed.scan_attempt == expected.scan_attempt
            assert observed.tree_observation_sha256 == expected.tree_observation_sha256
            assert observed.gallery_count == expected.gallery_count


def test_observer_cancellation_is_not_swallowed() -> None:
    def cancel(_progress: Progress) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        SourceDiscoveryPlan.from_locators((("first",),), progress=cancel)


def test_empty_inventory_reports_zero_terminal_progress() -> None:
    observations: list[Progress] = []
    with SourceDiscoveryPlan.from_locators((), progress=observations.append):
        assert observations[-1] == Progress(Operation.DISCOVERY_ORDER, 0, 0)
        assert Progress(Operation.DISCOVERY_TRANSFER, 0, 0) in observations


@pytest.mark.parametrize(
    "completed,total", [(-1, None), (True, None), (1, 0), (0, False)]
)
def test_progress_rejects_inconsistent_counts(completed: Any, total: Any) -> None:
    with pytest.raises(ValueError):
        Progress(Operation.BATCH_SELECTION, completed, total)


def test_progress_is_immutable() -> None:
    progress = Progress(Operation.BATCH_SELECTION, 0, 1)
    with pytest.raises(FrozenInstanceError):
        cast(Any, progress).completed = 1


@pytest.mark.parametrize("fail_observer", [False, True])
def test_public_preparation_reports_selected_cut_outside_transactions(
    db_config: Any, monkeypatch: pytest.MonkeyPatch, fail_observer: bool
) -> None:
    from contextlib import contextmanager

    from test_vnext_source_marker import MarkerSource
    from vnext_pipeline import (
        claim_session,
        collect_source,
        gallery,
        ingest_policy,
        initialize_database,
    )

    from h2hdb import VNextIngestFacade
    from h2hdb.sql_connector import SQLConnector
    from h2hdb.vnext_source_observation_spool import FrozenSourceObservationSpool

    initialize_database(db_config)
    source = MarkerSource(tuple(gallery(2000 + index, pages=[]) for index in range(13)))
    active_reads = 0
    callback_reads: list[int] = []
    observations: list[Progress] = []
    original = SQLConnector.read_transaction
    original_open = FrozenSourceObservationSpool.open_gallery
    opened_during: list[Progress] = []

    @contextmanager
    def read_transaction(connector: SQLConnector) -> Any:
        nonlocal active_reads
        with original(connector):
            active_reads += 1
            try:
                yield
            finally:
                active_reads -= 1

    def observe(progress: Progress) -> None:
        callback_reads.append(active_reads)
        observations.append(progress)
        if fail_observer:
            raise RuntimeError("observer failed")

    def open_gallery(spool: FrozenSourceObservationSpool, **kwargs: Any) -> Any:
        opened_during.append(observations[-1])
        return original_open(spool, **kwargs)

    monkeypatch.setattr(SQLConnector, "read_transaction", read_transaction)
    monkeypatch.setattr(FrozenSourceObservationSpool, "open_gallery", open_gallery)
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(
            source, policy=policy, max_new_galleries=10, progress=observe
        ) as cut:
            collect_source(facade, session, policy, cut)
            assert cut.deferred_gallery_count == 3
    assert len(source.deep_reads) == 10
    assert opened_during == [
        Progress(Operation.SOURCE_FREEZE, count, 13) for count in range(1, 11)
    ] + [Progress(Operation.BATCH_SELECTION, count, 10) for count in range(10)]
    assert callback_reads and not any(callback_reads)
    assert Progress(Operation.DISCOVERY_CLEANUP, 0, 13) in observations
    assert Progress(Operation.DISCOVERY_CLEANUP, 13, 13) in observations
    assert Progress(Operation.SOURCE_FREEZE, 13, 13) in observations
    assert [
        value
        for value in observations
        if value.operation == Operation.SOURCE_CHECKPOINT
    ] == [Progress(Operation.SOURCE_CHECKPOINT, count) for count in range(1, 11)] + [
        Progress(Operation.SOURCE_CHECKPOINT, 10, 10)
    ]
    assert [
        value for value in observations if value.operation == Operation.BATCH_SELECTION
    ] == [Progress(Operation.BATCH_SELECTION, count, 10) for count in range(11)]
    assert observations.index(Progress(Operation.BATCH_SELECTION, 10, 10)) < (
        observations.index(Progress(Operation.BATCH_ORDER, 0, 10))
    )
    assert [
        value.completed
        for value in observations
        if value.operation == Operation.SOURCE_FREEZE
    ] == list(range(14))


@pytest.mark.parametrize("fail_observer", [False, True])
def test_published_inventory_reconciliation_progress_preserves_source_cut(
    db_config: Any, monkeypatch: pytest.MonkeyPatch, fail_observer: bool
) -> None:
    from contextlib import contextmanager

    from test_vnext_source_batches import _publish_batch
    from test_vnext_source_deferral import UpdatingSource
    from vnext_pipeline import (
        MemoryLibrary,
        claim_session,
        collect_source,
        gallery,
        ingest_policy,
        initialize_database,
    )

    from h2hdb import VNextIngestFacade
    from h2hdb.sql_connector import SQLConnector
    from h2hdb.vnext_source_batch_repository import SourceBatchRepository

    initialize_database(db_config)
    kept, removed = (gallery(gid, pages=[]) for gid in (1001, 1002))
    source = UpdatingSource([kept, removed])
    _publish_batch(db_config, source, MemoryLibrary(source), limit=None)
    source.omitted.update((kept.locator, removed.locator))
    source.remove(removed.locator)
    observations: list[Progress] = []
    active_reads = 0
    callback_reads: list[int] = []
    original_transaction = SQLConnector.read_transaction
    original_probe = source.gallery_exists
    original_baseline = SourceBatchRepository.load_baseline

    @contextmanager
    def read_transaction(connector: SQLConnector) -> Any:
        nonlocal active_reads
        with original_transaction(connector):
            active_reads += 1
            try:
                yield
            finally:
                active_reads -= 1

    def observe(progress: Progress) -> None:
        callback_reads.append(active_reads)
        observations.append(progress)
        if fail_observer:
            raise RuntimeError("observer failed")

    def gallery_exists(locator: tuple[str, ...]) -> bool:
        assert observations[-1].operation == Operation.DISCOVERY_RECONCILIATION
        assert observations[-1].total is None
        return original_probe(locator)

    def load_baseline(*args: Any, **kwargs: Any) -> Any:
        assert observations[-1] == Progress(Operation.DISCOVERY_RECONCILIATION, 0)
        return original_baseline(*args, **kwargs)

    monkeypatch.setattr(SQLConnector, "read_transaction", read_transaction)
    monkeypatch.setattr(source, "gallery_exists", gallery_exists)
    monkeypatch.setattr(SourceBatchRepository, "load_baseline", load_baseline)
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(source, policy=policy, progress=observe) as prepared:
            collect_source(facade, session, policy, prepared)
            assert prepared.gallery_count == 1
            assert prepared.waiting_gallery_count == 0
            assert prepared.deferred_gallery_count == 0
    assert source.presence_probes == [kept.locator, removed.locator]
    assert callback_reads and not any(callback_reads)
    assert [
        value
        for value in observations
        if value.operation == Operation.DISCOVERY_RECONCILIATION
    ] == [
        Progress(Operation.DISCOVERY_RECONCILIATION, 0),
        Progress(Operation.DISCOVERY_RECONCILIATION, 1),
        Progress(Operation.DISCOVERY_RECONCILIATION, 2),
        Progress(Operation.DISCOVERY_RECONCILIATION, 2, 2),
    ]


def test_public_preparation_rejects_noncallable_observer_before_source_io() -> None:
    from h2hdb import CoreConfig, VNextIngestFacade

    with VNextIngestFacade(CoreConfig()) as facade:
        with pytest.raises(TypeError, match="progress must be a callable"):
            facade.prepare_source(
                cast(Any, object()),
                policy=cast(Any, object()),
                progress=cast(Any, object()),
            )


def test_progress_rejects_unknown_operation() -> None:
    with pytest.raises(TypeError, match="operation"):
        Progress(cast(Any, "unknown"), 0)
