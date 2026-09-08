"""Preparation progress is observable without weakening source authority."""

from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import VNextSourcePreparationOperation as Operation
from h2hdb import VNextSourcePreparationProgress as Progress
from h2hdb.vnext_source_batch_plan import prepare_source_batch
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


def test_discovery_and_batch_observations_have_exact_totals() -> None:
    observations: list[Progress] = []
    with SourceDiscoveryPlan.from_locators(
        ((f"gallery-{index:05d}",) for index in range(259)),
        progress=observations.append,
    ) as inventory:
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
        observations.clear()
        selected = prepare_source_batch(
            inventory,
            max_new_galleries=10,
            lookup_members=lambda page: (False,) * len(page),
            progress=observations.append,
        )
        with selected.plan as batch:
            assert batch.gallery_count == 10
            assert selected.deferred_gallery_count == 249
            assert observations[0] == Progress(Operation.BATCH_SELECTION, 0, 259)
            assert Progress(Operation.BATCH_SELECTION, 259, 259) in observations
            assert observations[-1] == Progress(Operation.BATCH_ORDER, 10, 10)
            assert not any(
                value.operation == Operation.DISCOVERY_TRANSFER
                for value in observations
            )


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


def test_public_preparation_reports_selected_cut_outside_transactions(
    db_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import contextmanager

    from test_vnext_source_marker import MarkerSource
    from vnext_pipeline import gallery, initialize_database

    from h2hdb import VNextIngestFacade
    from h2hdb.sql_connector import SQLConnector

    initialize_database(db_config)
    source = MarkerSource(tuple(gallery(2000 + index, pages=[]) for index in range(13)))
    active_reads = 0
    callback_reads: list[int] = []
    observations: list[Progress] = []
    original = SQLConnector.read_transaction

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

    monkeypatch.setattr(SQLConnector, "read_transaction", read_transaction)
    with VNextIngestFacade(db_config) as facade:
        with facade.prepare_source(
            source, max_new_galleries=10, progress=observe
        ) as cut:
            assert cut.deferred_gallery_count == 3
    assert len(source.deep_reads) == 10
    assert callback_reads and not any(callback_reads)
    assert Progress(Operation.BATCH_SELECTION, 13, 13) in observations
    assert Progress(Operation.DISCOVERY_CLEANUP, 0, 13) in observations
    assert Progress(Operation.DISCOVERY_CLEANUP, 13, 13) in observations
    assert observations[-1] == Progress(Operation.SOURCE_FREEZE, 10, 10)
    assert [
        value.completed
        for value in observations
        if value.operation == Operation.SOURCE_FREEZE
    ] == list(range(11))


def test_public_preparation_rejects_noncallable_observer_before_source_io() -> None:
    from h2hdb import CoreConfig, VNextIngestFacade

    with VNextIngestFacade(CoreConfig()) as facade:
        with pytest.raises(TypeError, match="progress must be a callable"):
            facade.prepare_source(cast(Any, object()), progress=cast(Any, object()))


def test_progress_rejects_unknown_operation() -> None:
    with pytest.raises(TypeError, match="operation"):
        Progress(cast(Any, "unknown"), 0)
