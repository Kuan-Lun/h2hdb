"""Bounded current-only batching, independent of workload wall-clock speed."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from vnext_generated_database import open_generated_sqlite_database

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchResult,
    CleanupCycle,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    LockedGateRenewal,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance


def _claim(connector: SQLConnector) -> GateLease:
    with connector.transaction():
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _begin(
    connector: SQLConnector,
    gate: GateLease,
    kind: CleanupTargetKind,
) -> CleanupCycle:
    with connector.transaction():
        return VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            target_kind=kind,
            shard_no=201,
            cycle_cutoff_at=100,
            max_rows_per_transaction=1,
            now=2,
        )


def _advance(
    connector: SQLConnector, gate: GateLease, cycle: CleanupCycle, *, now: int = 3
) -> tuple[CleanupBatchResult, ...]:
    with connector.transaction():
        return VNextCleanupRepository.advance_current_only_cycle(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cycle=cycle,
            now=now,
        )


@pytest.mark.parametrize("kind", cleanup._CURRENT_ONLY_TARGET_PRIORITY)
def test_empty_phases_share_one_exact_gate_check_and_finish_fixed_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: CleanupTargetKind
) -> None:
    with open_generated_sqlite_database(tmp_path / "empty.sqlite3") as connector:
        gate = _claim(connector)
        cycle = _begin(connector, gate, kind)
        checks: list[int] = []
        original = LockedGateRenewal.require_live

        def checked(*args: Any, **kwargs: Any) -> GateLease:
            checks.append(kwargs["now"])
            return original(*args, **kwargs)

        monkeypatch.setattr(LockedGateRenewal, "require_live", checked)
        results = _advance(connector, gate, cycle)
        assert checks == [3]
        assert len(results) == len(cleanup._STRATEGIES[kind].phases)
        assert all(result.row_count == 0 and not result.replayed for result in results)
        assert results[-1].cycle_complete
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_checkpoints"
        ) == (0,)
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        assert _advance(connector, gate, cycle)[0].replayed


def test_failure_after_empty_transition_rolls_back_all_receipts_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_generated_sqlite_database(tmp_path / "rollback.sqlite3") as connector:
        gate = _claim(connector)
        cycle = _begin(connector, gate, CleanupTargetKind.SOURCE_BUILD)
        before = connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
        original = cleanup._insert_checkpoint
        transitions = 0

        def interrupted(*args: Any, **kwargs: Any) -> None:
            nonlocal transitions
            original(*args, **kwargs)
            transitions += 1
            if transitions == 2:
                raise RuntimeError("interrupted after durable empty transition")

        monkeypatch.setattr(cleanup, "_insert_checkpoint", interrupted)
        with pytest.raises(RuntimeError, match="interrupted after durable"):
            _advance(connector, gate, cycle)
        assert transitions == 2
        assert (
            connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
            == before
        )
        monkeypatch.undo()
        assert _advance(connector, gate, cycle)[-1].cycle_complete


def test_nonempty_batch_stops_at_key_bound_and_lost_response_resumes(
    tmp_path: Path,
) -> None:
    with open_generated_sqlite_database(tmp_path / "bounded.sqlite3") as connector:
        for ordinal in range(3):
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 0)",
                (bytes((201,)) + ordinal.to_bytes(31, "big"),),
            )
        gate = _claim(connector)
        cycle = _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB)
        # The first transaction commits, but its caller loses the result.
        _advance(connector, gate, cycle)
        for expected_remaining in (1, 0):
            results = _advance(connector, gate, cycle)
            assert len(results) == 1
            assert results[0].row_count == 1
            assert not results[0].replayed and not results[0].cycle_complete
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_content_blobs"
            ) == (expected_remaining,)
        assert _advance(connector, gate, cycle)[-1].cycle_complete
        assert _advance(connector, gate, cycle)[-1].deleted_count == 3


def test_expired_authority_cannot_advance_even_an_empty_phase(tmp_path: Path) -> None:
    with open_generated_sqlite_database(tmp_path / "expired.sqlite3") as connector:
        gate = _claim(connector)
        cycle = _begin(connector, gate, CleanupTargetKind.SOURCE_BUILD)
        before = connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
        with pytest.raises(MaintenanceGateUnavailableError):
            _advance(connector, gate, cycle, now=gate.lease_expires_at)
        assert (
            connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
            == before
        )


@pytest.mark.parametrize("clock_after_first_batch", [100, 600, 1_100])
def test_facade_renews_only_at_half_life_and_revalidates_each_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock_after_first_batch: int
) -> None:
    path = tmp_path / "cadence.sqlite3"
    with open_generated_sqlite_database(path) as connector:
        for ordinal in range(2):
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 0)",
                (bytes((201,)) + ordinal.to_bytes(31, "big"),),
            )
        gate = _claim(connector)
        _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB)
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"), gate, now=3
            )
    now = 100
    advances = 0
    renewals: list[int] = []
    original_advance = VNextCleanupRepository.advance_current_only_cycle
    original_renew = LockedGateRenewal.renew

    def advance(*args: Any, **kwargs: Any) -> tuple[CleanupBatchResult, ...]:
        nonlocal now, advances
        result = original_advance(*args, **kwargs)
        advances += 1
        if advances == 1:
            now = clock_after_first_batch
        return result

    def renew(*args: Any, **kwargs: Any) -> GateLease:
        renewals.append(kwargs["now"])
        return original_renew(*args, **kwargs)

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(advance)
    )
    monkeypatch.setattr(LockedGateRenewal, "renew", renew)
    with VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: now,
    ) as facade:
        if clock_after_first_batch == 1_100:
            assert facade.drain_current_only_maintenance(1_000) is (
                VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            )
            assert advances == 1
            with SQLiteConnector(str(path)) as connector:
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_content_blobs"
                ) == (1,)
            return
        assert facade.drain_current_only_maintenance(1_000) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
    assert advances == 3
    assert renewals == ([600] if clock_after_first_batch == 600 else [])
