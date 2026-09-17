"""Maintenance yields expired capabilities and resumes committed checkpoints."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from sqlite3 import OperationalError
from typing import Any

import pytest
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database

import h2hdb.vnext_maintenance_gate_repository as gate_module
from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.vnext_cleanup_repository import (
    CleanupCorruptionError,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    LockedExclusiveGateClaim,
    MaintenanceGateRepository,
    MaintenanceGateTokenCollisionError,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_DURATION = 1_000


@dataclass
class _Clock:
    now: int = 100

    def __call__(self) -> int:
        return self.now


def _seed_open_cycle(config: CoreConfig, *, rows: int = 3) -> None:
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        with connector.transaction():
            for ordinal in range(rows):
                connector.execute(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, 0)",
                    (bytes((201,)) + ordinal.to_bytes(31, "big"),),
                )
            work = VNextUnitOfWork(connector, backend=backend_of(config))
            gate = MaintenanceGateRepository.claim_exclusive(
                work, now=1, lease_duration=_DURATION
            )
        with connector.transaction():
            VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend_of(config)),
                gate_lease=gate,
                target_kind=CleanupTargetKind.CONTENT_BLOB,
                shard_no=201,
                cycle_cutoff_at=100,
                max_rows_per_transaction=1,
                now=2,
            )
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend=backend_of(config)), gate, now=3
            )


def _counts(config: CoreConfig) -> tuple[int, int, int]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        rows, jobs, owners = (
            connector.fetch_one(query)[0]
            for query in (
                "SELECT COUNT(*) FROM catalog_content_blobs",
                "SELECT COUNT(*) FROM operational_cleanup_jobs WHERE state = 'OPEN'",
                "SELECT COUNT(*) FROM operational_maintenance_gate_owners",
            )
        )
        return rows, jobs, owners


def _claim(config: CoreConfig, clock: _Clock) -> GateLease:
    with closing(open_connector(config)) as connector, connector.transaction():
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            now=clock.now,
            lease_duration=_DURATION,
        )


def _release(config: CoreConfig, clock: _Clock, lease: GateLease) -> None:
    with closing(open_connector(config)) as connector, connector.transaction():
        MaintenanceGateRepository.release(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            lease,
            now=clock.now,
        )


def _owner_rows(config: CoreConfig) -> list[tuple[Any, ...]]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return connector.fetch_all(
            "SELECT owner_token, gate_generation, lease_expires_at "
            "FROM operational_maintenance_gate_owners ORDER BY owner_token"
        )


def _checkpoints(config: CoreConfig) -> list[tuple[Any, ...]]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return connector.fetch_all(
            "SELECT * FROM operational_cleanup_checkpoints ORDER BY cleanup_id"
        )


def _job_identities(config: CoreConfig) -> list[tuple[Any, ...]]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return connector.fetch_all(
            "SELECT cleanup_id, target_key, cycle_generation "
            "FROM operational_cleanup_jobs ORDER BY cleanup_id"
        )


@pytest.mark.parametrize("elapsed", [_DURATION, _DURATION + 1])
def test_expired_committed_batch_yields_then_resumes_to_done(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, elapsed: int
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    advances = 0
    original = VNextCleanupRepository.advance_current_only_cycle

    def slow_batch(*args: Any, **kwargs: Any) -> Any:
        nonlocal advances
        result = original(*args, **kwargs)
        advances += 1
        if advances == 1:
            clock.now += elapsed
        return result

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(slow_batch)
    )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
        assert advances == 1
        assert _counts(db_config) == (2, 1, 1)
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert _counts(db_config) == (0, 0, 0)
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        facade.complete_ingest(session)


@pytest.mark.parametrize("phase", ["next_cycle", "advance", "state", "release"])
def test_gate_wait_expiry_yields_without_mutating_under_stale_time(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    method = {
        "next_cycle": "__next_current_only_cycle",
        "advance": "__advance_current_only_shard",
        "state": "__current_only_state",
        "release": "__release_current_only_lease",
    }[phase]
    method = "_VNextIngestFacade" + method
    original_method = getattr(VNextIngestFacade, method)
    original_lock = VNextUnitOfWork.lock_rows
    active = False
    delayed = False

    def operation(*args: Any, **kwargs: Any) -> Any:
        nonlocal active
        active = True
        try:
            return original_method(*args, **kwargs)
        finally:
            active = False

    def lock(self: VNextUnitOfWork, *args: Any, **kwargs: Any) -> Any:
        nonlocal delayed
        result = original_lock(self, *args, **kwargs)
        if active and not delayed and args[0] is LockRank.MAINTENANCE_GATE:
            delayed = True
            clock.now += _DURATION
        return result

    monkeypatch.setattr(VNextIngestFacade, method, operation)
    monkeypatch.setattr(VNextUnitOfWork, "lock_rows", lock)
    progressed = phase in {"state", "release"}
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            if progressed
            else VNextCurrentOnlyMaintenanceOutcome.CONTENDED
        )
        assert delayed
        assert _counts(db_config) == ((0, 0, 1) if progressed else (3, 1, 1))
        assert _owner_rows(db_config)[0][2] == 100 + _DURATION
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        facade.complete_ingest(session)
        # A read-only DONE retry need not manufacture a gate generation just
        # to remove the expired owner. A later SHARED claim can replace only
        # one of its slots; the other stale slots grant no authority.
        assert _counts(db_config) == ((0, 0, 1) if progressed else (0, 0, 0))
        assert all(row[2] <= clock.now for row in _owner_rows(db_config))


def test_exclusive_claim_starts_lease_after_gate_wait(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    delayed = False
    original = VNextUnitOfWork.lock_rows
    deadlines: list[int] = []
    grant = LockedExclusiveGateClaim.grant

    def lock(self: VNextUnitOfWork, *args: Any, **kwargs: Any) -> Any:
        nonlocal delayed
        result = original(self, *args, **kwargs)
        if not delayed and args[0] is LockRank.MAINTENANCE_GATE:
            delayed = True
            clock.now += 2 * _DURATION
        return result

    def recorded_grant(self: LockedExclusiveGateClaim, **kwargs: Any) -> GateLease:
        lease = grant(self, **kwargs)
        deadlines.append(lease.lease_expires_at)
        return lease

    monkeypatch.setattr(VNextUnitOfWork, "lock_rows", lock)
    monkeypatch.setattr(LockedExclusiveGateClaim, "grant", recorded_grant)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
    assert deadlines == [clock.now + _DURATION]
    assert _counts(db_config) == (0, 0, 0)


@pytest.mark.parametrize("wait", [499, 500, 501])
def test_renewal_samples_after_gate_wait_and_never_revives_expired_owner(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, wait: int
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    advances = 0
    advance = VNextCleanupRepository.advance_current_only_cycle
    renewal = "_VNextIngestFacade__renew_current_only_lease"
    renew = getattr(VNextIngestFacade, renewal)
    lock_rows = VNextUnitOfWork.lock_rows
    renewing = False
    delayed = False

    def batch(*args: Any, **kwargs: Any) -> Any:
        nonlocal advances
        result = advance(*args, **kwargs)
        advances += 1
        if advances == 1:
            clock.now = 600
        return result

    def renewal_step(*args: Any, **kwargs: Any) -> Any:
        nonlocal renewing
        renewing = True
        try:
            return renew(*args, **kwargs)
        finally:
            renewing = False

    def lock(self: VNextUnitOfWork, *args: Any, **kwargs: Any) -> Any:
        nonlocal delayed
        result = lock_rows(self, *args, **kwargs)
        if renewing and not delayed and args[0] is LockRank.MAINTENANCE_GATE:
            delayed = True
            clock.now += wait
        return result

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(batch)
    )
    monkeypatch.setattr(VNextIngestFacade, renewal, renewal_step)
    monkeypatch.setattr(VNextUnitOfWork, "lock_rows", lock)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
            if wait < 500
            else VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
        assert delayed
        if wait >= 500:
            assert advances == 1
            assert _counts(db_config) == (2, 1, 1)
            assert _owner_rows(db_config)[0][2] == 1_100
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
        assert _counts(db_config) == (0, 0, 0)


@pytest.mark.parametrize("progressed", [False, True])
def test_replaced_owner_is_not_released_and_fresh_claim_resumes_durable_job(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, progressed: bool
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    method = "_VNextIngestFacade" + (
        "__advance_current_only_shard" if progressed else "__next_current_only_cycle"
    )
    original = getattr(VNextIngestFacade, method)
    replacement: list[GateLease] = []

    def takeover(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        if not replacement:
            clock.now += _DURATION
            replacement.append(_claim(db_config, clock))
        return result

    monkeypatch.setattr(VNextIngestFacade, method, takeover)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            if progressed
            else VNextCurrentOnlyMaintenanceOutcome.CONTENDED
        )
        lease = replacement[0]
        expected_owner = [
            (lease.owner_token, lease.gate_generation, lease.lease_expires_at)
        ]
        assert _owner_rows(db_config) == expected_owner
        assert _counts(db_config) == (2 if progressed else 3, 1, 1)
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED
        )
        assert _owner_rows(db_config) == expected_owner
        _release(db_config, clock, lease)
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert _counts(db_config) == (0, 0, 0)


def test_exclusive_token_collision_is_not_disguised_as_contention(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    existing = _claim(db_config, clock)
    before = _owner_rows(db_config)
    monkeypatch.setattr(gate_module, "_new_owner_token", lambda: existing.owner_token)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with pytest.raises(MaintenanceGateTokenCollisionError):
            facade.drain_current_only_maintenance(_DURATION)
    assert _owner_rows(db_config) == before
    assert _counts(db_config) == (3, 1, 1)


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("fault"),
        OperationalError("db fault"),
        CleanupCorruptionError("corrupt"),
        OSError("I/O fault"),
        MaintenanceGateTokenCollisionError("attempt token collision"),
    ],
)
def test_fatal_failures_propagate_and_uncommitted_batch_rolls_back(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    _seed_open_cycle(db_config)
    checkpoints = _checkpoints(db_config)
    clock = _Clock()
    original = VNextCleanupRepository.advance_current_only_cycle

    def fail(*args: Any, **kwargs: Any) -> Any:
        original(*args, **kwargs)
        raise error

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(fail)
    )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with pytest.raises(type(error)) as raised:
            facade.drain_current_only_maintenance(_DURATION)
        assert raised.value is error
        assert _counts(db_config) == (3, 1, 0)
        assert _checkpoints(db_config) == checkpoints
        monkeypatch.undo()
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert _counts(db_config) == (0, 0, 0)


def test_every_slow_batch_yields_and_finite_work_still_reaches_done(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_open_cycle(db_config)
    original_jobs = _job_identities(db_config)
    clock = _Clock()
    original = VNextCleanupRepository.advance_current_only_cycle
    advances = 0

    def slow_batch(*args: Any, **kwargs: Any) -> Any:
        nonlocal advances
        result = original(*args, **kwargs)
        advances += 1
        clock.now += _DURATION + 1
        return result

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(slow_batch)
    )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        for expected_rows in (2, 1, 0):
            before = advances
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            )
            assert advances == before + 1
            assert _counts(db_config) == (expected_rows, 1, 1)
            assert _job_identities(db_config) == original_jobs
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
        assert _counts(db_config) == (0, 0, 1)
        assert _job_identities(db_config) == original_jobs
        assert advances == 4
        # Completion has to be independently observed by a subsequent call.
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert advances == 4


def test_rolled_back_attempt_never_reports_progress(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_open_cycle(db_config)
    checkpoints = _checkpoints(db_config)
    clock = _Clock()
    original = VNextCleanupRepository.advance_current_only_cycle

    def unavailable(*args: Any, **kwargs: Any) -> Any:
        original(*args, **kwargs)
        clock.now += _DURATION
        raise MaintenanceGateUnavailableError("injected authorization failure")

    monkeypatch.setattr(
        VNextCleanupRepository,
        "advance_current_only_cycle",
        staticmethod(unavailable),
    )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED
        )
        assert _counts(db_config) == (3, 1, 1)
        assert _checkpoints(db_config) == checkpoints
        monkeypatch.undo()
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert _counts(db_config) == (0, 0, 0)


def test_claim_committed_after_expiry_yields_without_starting_cleanup(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_open_cycle(db_config)
    clock = _Clock()
    with closing(open_connector(db_config)) as connector:
        connector_type = type(connector)
    commit = connector_type.commit
    grant = LockedExclusiveGateClaim.grant
    armed = False
    delayed = False

    def granted(self: LockedExclusiveGateClaim, **kwargs: Any) -> GateLease:
        nonlocal armed
        lease = grant(self, **kwargs)
        armed = True
        return lease

    def committed(self: Any) -> None:
        nonlocal delayed
        commit(self)
        if armed and not delayed:
            delayed = True
            clock.now += _DURATION

    monkeypatch.setattr(LockedExclusiveGateClaim, "grant", granted)
    monkeypatch.setattr(connector_type, "commit", committed)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED
        )
        assert delayed
        assert _counts(db_config) == (3, 1, 1)
        assert _owner_rows(db_config)[0][2] == 1_100
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert _counts(db_config) == (0, 0, 0)


@pytest.mark.parametrize(
    "release_error",
    [
        MaintenanceGateUnavailableError("expired release"),
        OperationalError("release database failure"),
        MaintenanceGateTokenCollisionError("release token collision"),
    ],
)
def test_failure_compensation_preserves_primary_or_explicit_error_chain(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, release_error: Exception
) -> None:
    _seed_open_cycle(db_config)
    primary = CleanupCorruptionError("primary cleanup corruption")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise primary

    def fail_release(*args: Any, **kwargs: Any) -> Any:
        raise release_error

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", staticmethod(fail)
    )
    monkeypatch.setattr(
        VNextIngestFacade,
        "_VNextIngestFacade__release_current_only_lease",
        fail_release,
    )
    unavailable = type(release_error) is MaintenanceGateUnavailableError
    expected = primary if unavailable else release_error
    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        with pytest.raises(type(expected)) as raised:
            facade.drain_current_only_maintenance(_DURATION)
        assert raised.value is expected
        if not unavailable:
            assert raised.value.__cause__ is primary
        assert _counts(db_config) == (3, 1, 1)
