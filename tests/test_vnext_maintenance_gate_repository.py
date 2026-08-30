from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_domains import INT63_MAX, DomainValidationError
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateExhaustedError,
    MaintenanceGateRepository,
    MaintenanceGateTokenCollisionError,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


class _FailingHolderConnector(SQLiteConnector):
    fail_holder_number: int | None = None
    holder_inserts = 0

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        if query.lstrip().startswith(
            "INSERT INTO operational_maintenance_gate_holders"
        ):
            self.holder_inserts += 1
            if self.holder_inserts == self.fail_holder_number:
                raise RuntimeError("injected partial EXCLUSIVE holder failure")
        super().execute(query, data)


def _generated_database(
    path: Path, *, connector_type: type[SQLiteConnector] = SQLiteConnector
) -> SQLiteConnector:
    return open_generated_sqlite_database(path, connector_type=connector_type)


def _claim_shared(
    connector: SQLiteConnector,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> GateLease:
    with patch(
        "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
        return_value=token,
    ):
        with connector.transaction():
            return MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=now,
                lease_duration=duration,
            )


def _claim_exclusive(
    connector: SQLiteConnector,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> GateLease:
    with patch(
        "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
        return_value=token,
    ):
        with connector.transaction():
            return MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=now,
                lease_duration=duration,
            )


def _resume(
    connector: SQLiteConnector,
    lease: GateLease,
    *,
    now: int,
) -> GateLease:
    with connector.transaction():
        return MaintenanceGateRepository.resume(
            VNextUnitOfWork(connector, backend="sqlite"),
            lease,
            now=now,
        )


def _gate_snapshot(connector: SQLiteConnector) -> tuple[object, ...]:
    return (
        connector.fetch_all(
            "SELECT gate_generation, mode, created_at "
            "FROM operational_maintenance_gate_generations "
            "ORDER BY gate_generation"
        ),
        connector.fetch_all(
            "SELECT singleton_id, gate_generation, updated_at "
            "FROM operational_maintenance_gate_heads"
        ),
        connector.fetch_all(
            "SELECT owner_token, gate_generation, lease_expires_at "
            "FROM operational_maintenance_gate_owners ORDER BY owner_token"
        ),
        connector.fetch_all(
            "SELECT slot, owner_token FROM operational_maintenance_gate_holders "
            "ORDER BY slot"
        ),
    )


def test_shared_claims_use_first_available_slot_and_resume_without_writes(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "shared.sqlite3")
    try:
        first = _claim_shared(connector, b"a" * 16, now=10, duration=100)
        assert first == GateLease(b"a" * 16, 0, GateMode.SHARED, (0,), 110)
        replay = _resume(connector, first, now=20)
        assert replay == first
        second = _claim_shared(connector, b"b" * 16, now=20, duration=100)
        assert second.slots == (1,)
        assert connector.fetch_all(
            "SELECT slot, owner_token FROM operational_maintenance_gate_holders "
            "ORDER BY slot"
        ) == [(0, b"a" * 16), (1, b"b" * 16)]
    finally:
        connector.close()


def test_repository_generated_token_collision_fails_closed_without_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "token-collision.sqlite3")
    try:
        _claim_shared(connector, b"a" * 16, now=10, duration=100)
        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateTokenCollisionError, match="already exists"):
            _claim_shared(connector, b"a" * 16, now=20, duration=100)
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_expired_shared_slot_is_reclaimed_by_exact_owner_cas(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "shared-reclaim.sqlite3")
    try:
        stale = _claim_shared(connector, b"a" * 16, now=10, duration=10)
        current = _claim_shared(connector, b"b" * 16, now=20, duration=50)
        assert current.slots == (0,)
        assert connector.fetch_all(
            "SELECT owner_token, gate_generation FROM "
            "operational_maintenance_gate_owners"
        ) == [(b"b" * 16, 0)]

        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateUnavailableError, match="stale"):
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend="sqlite"), stale, now=21
                )
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_live_shared_owner_blocks_exclusive_with_zero_writes(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "live-block.sqlite3")
    try:
        _claim_shared(connector, b"a" * 16, now=1, duration=100)
        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateUnavailableError, match="blocks EXCLUSIVE"):
            _claim_exclusive(connector, b"x" * 16, now=2, duration=100)
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_full_shared_slot_domain_rejects_the_sixty_fifth_owner_without_writes(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "full-shared.sqlite3")
    try:
        _claim_shared(connector, b"a" * 16, now=1, duration=100)
        for slot in range(1, 64):
            token = slot.to_bytes(16, "big")
            connector.execute(
                "INSERT INTO operational_maintenance_gate_owners "
                "(owner_token, gate_generation, lease_expires_at) "
                "VALUES (%s, 0, 101)",
                (token,),
            )
            connector.execute(
                "INSERT INTO operational_maintenance_gate_holders "
                "(owner_token, slot) VALUES (%s, %s)",
                (token, slot),
            )
        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateUnavailableError, match="all 64"):
            _claim_shared(connector, b"z" * 16, now=2, duration=100)
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_exclusive_claim_holds_exactly_64_slots_replays_and_releases(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "exclusive.sqlite3")
    try:
        shared = _claim_shared(connector, b"s" * 16, now=1, duration=100)
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"), shared, now=2
            )

        exclusive = _claim_exclusive(connector, b"x" * 16, now=3, duration=100)
        assert exclusive.gate_generation == 1
        assert exclusive.mode == GateMode.EXCLUSIVE
        assert exclusive.slots == tuple(range(64))
        assert connector.fetch_one(
            "SELECT COUNT(*), MIN(slot), MAX(slot), "
            "COUNT(DISTINCT owner_token) "
            "FROM operational_maintenance_gate_holders"
        ) == (64, 0, 63, 1)
        assert _resume(connector, exclusive, now=4) == exclusive

        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateUnavailableError, match="EXCLUSIVE"):
            _claim_shared(connector, b"y" * 16, now=5, duration=100)
        assert _gate_snapshot(connector) == before

        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"), exclusive, now=6
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_maintenance_gate_holders"
        ) == (0,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_maintenance_gate_owners"
        ) == (0,)

        next_shared = _claim_shared(connector, b"n" * 16, now=7, duration=100)
        assert (next_shared.gate_generation, next_shared.slots) == (2, (0,))
    finally:
        connector.close()


def test_expired_exclusive_is_atomically_replaced_in_a_new_generation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "exclusive-reclaim.sqlite3")
    try:
        old = _claim_exclusive(connector, b"a" * 16, now=10, duration=10)
        new = _claim_exclusive(connector, b"b" * 16, now=20, duration=50)
        assert (old.gate_generation, new.gate_generation) == (0, 1)
        assert connector.fetch_one(
            "SELECT COUNT(*), COUNT(DISTINCT owner_token) "
            "FROM operational_maintenance_gate_holders"
        ) == (64, 1)
        assert connector.fetch_all(
            "SELECT owner_token, gate_generation "
            "FROM operational_maintenance_gate_owners"
        ) == [(b"b" * 16, 1)]
    finally:
        connector.close()


def test_shared_claim_after_expired_exclusive_advances_to_new_shared_generation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "exclusive-to-shared.sqlite3")
    try:
        old = _claim_exclusive(connector, b"a" * 16, now=10, duration=10)
        shared = _claim_shared(connector, b"b" * 16, now=20, duration=50)
        assert (old.gate_generation, shared.gate_generation) == (0, 1)
        assert shared == GateLease(b"b" * 16, 1, GateMode.SHARED, (0,), 70)
        assert connector.fetch_one(
            "SELECT h.gate_generation, g.mode FROM "
            "operational_maintenance_gate_heads AS h JOIN "
            "operational_maintenance_gate_generations AS g "
            "ON g.gate_generation = h.gate_generation WHERE h.singleton_id = 1"
        ) == (1, "SHARED")
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_maintenance_gate_holders "
            "WHERE owner_token = %s",
            (b"b" * 16,),
        ) == (1,)
        assert _resume(connector, shared, now=21) == shared
    finally:
        connector.close()


def test_renewal_fences_old_snapshot_and_release_requires_new_snapshot(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "renew.sqlite3")
    try:
        old = _claim_shared(connector, b"a" * 16, now=10, duration=20)
        with connector.transaction():
            renewed = MaintenanceGateRepository.renew(
                VNextUnitOfWork(connector, backend="sqlite"),
                old,
                now=20,
                lease_duration=100,
            )
        assert renewed.lease_expires_at == 120
        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateUnavailableError, match="stale"):
            _resume(connector, old, now=21)
        assert _gate_snapshot(connector) == before
        with pytest.raises(MaintenanceGateUnavailableError, match="stale"):
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend="sqlite"), old, now=21
                )
        assert _gate_snapshot(connector) == before
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"), renewed, now=21
            )
    finally:
        connector.close()


def test_partial_exclusive_insert_rolls_back_generation_owner_and_holders(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "partial.sqlite3", connector_type=_FailingHolderConnector
    )
    assert isinstance(connector, _FailingHolderConnector)
    connector.fail_holder_number = 33
    try:
        with pytest.raises(RuntimeError, match="partial EXCLUSIVE"):
            _claim_exclusive(connector, b"x" * 16, now=1, duration=100)
        assert _gate_snapshot(connector) == ([], [], [], [])
    finally:
        connector.close()


def test_gate_overflow_checks_are_zero_write(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "overflow.sqlite3")
    try:
        with pytest.raises(DomainValidationError, match="duration"):
            _claim_shared(connector, b"a" * 16, now=1, duration=-1)
        with pytest.raises(OverflowError, match="deadline"):
            _claim_shared(connector, b"a" * 16, now=INT63_MAX, duration=1)
        assert _gate_snapshot(connector) == ([], [], [], [])

        connector.execute(
            "INSERT INTO operational_maintenance_gate_generations "
            "(gate_generation, mode, created_at) VALUES (%s, %s, 1)",
            (INT63_MAX, GateMode.SHARED.value),
        )
        connector.execute(
            "INSERT INTO operational_maintenance_gate_heads "
            "(singleton_id, gate_generation, updated_at) VALUES (1, %s, 1)",
            (INT63_MAX,),
        )
        before = _gate_snapshot(connector)
        with pytest.raises(MaintenanceGateExhaustedError):
            _claim_exclusive(connector, b"x" * 16, now=2, duration=10)
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()
