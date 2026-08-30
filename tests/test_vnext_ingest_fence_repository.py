from __future__ import annotations

from pathlib import Path

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_domains import INT63_MAX, DomainValidationError
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceExhaustedError,
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_transaction import LockOrderViolationError, VNextUnitOfWork


def _generated_database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _claim(
    connector: SQLiteConnector,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> IngestTurn:
    with connector.transaction():
        return IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=token,
            now=now,
            lease_duration=duration,
        )


def _authority_snapshot(connector: SQLiteConnector) -> tuple[object, ...]:
    return (
        connector.fetch_all(
            "SELECT generation, started_at, completed_at "
            "FROM operational_ingest_generations ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT singleton_id, current_generation, completed_generation, phase, "
            "last_transition_at FROM operational_ingest_coordination_heads"
        ),
        connector.fetch_all(
            "SELECT generation, owner_token, claimed_at, lease_expires_at "
            "FROM operational_ingest_generation_owners ORDER BY generation"
        ),
    )


def test_fresh_claim_creates_real_completed_genesis_and_replays_exact_turn(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "fresh.sqlite3")
    token = b"a" * 16
    try:
        turn = _claim(connector, token, now=10, duration=100)
        assert (turn.generation, turn.lease_expires_at) == (1, 110)
        assert connector.fetch_all(
            "SELECT generation, started_at, completed_at "
            "FROM operational_ingest_generations ORDER BY generation"
        ) == [(0, 10, 10), (1, 10, None)]
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads WHERE singleton_id = 1"
        ) == (1, 0, "INGESTING")

        replay = _claim(connector, token, now=20, duration=999)
        assert replay == turn
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_ingest_generations"
        ) == (2,)
    finally:
        connector.close()


def test_live_contention_is_zero_write_across_two_connections(tmp_path: Path) -> None:
    database = tmp_path / "two-connections.sqlite3"
    first = _generated_database(database)
    second = SQLiteConnector(str(database))
    second.connect()
    try:
        _claim(first, b"a" * 16, now=1, duration=100)
        before = _authority_snapshot(first)
        with pytest.raises(IngestFenceUnavailableError, match="live lease"):
            _claim(second, b"b" * 16, now=2, duration=100)
        assert _authority_snapshot(first) == before
        assert _authority_snapshot(second) == before
    finally:
        second.close()
        first.close()


def test_expired_takeover_fences_old_turn_and_completion_cleans_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "takeover.sqlite3")
    try:
        stale = _claim(connector, b"a" * 16, now=10, duration=10)
        current = _claim(connector, b"b" * 16, now=20, duration=30)
        assert (stale.generation, current.generation) == (1, 2)

        before = _authority_snapshot(connector)
        with pytest.raises(IngestFenceUnavailableError, match="stale"):
            with connector.transaction():
                IngestFenceRepository.complete(
                    VNextUnitOfWork(connector, backend="sqlite"), stale, now=21
                )
        assert _authority_snapshot(connector) == before

        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"), current, now=30
            )
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads"
        ) == (2, 2, "READY")
        assert connector.fetch_one(
            "SELECT completed_at FROM operational_ingest_generations "
            "WHERE generation = 2"
        ) == (30,)
        assert (
            connector.fetch_all(
                "SELECT generation FROM operational_ingest_generation_owners"
            )
            == []
        )
    finally:
        connector.close()


def test_renew_requires_exact_snapshot_and_authorizes_only_new_turn(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "renew.sqlite3")
    try:
        old = _claim(connector, b"a" * 16, now=10, duration=20)
        with connector.transaction():
            renewed = IngestFenceRepository.renew(
                VNextUnitOfWork(connector, backend="sqlite"),
                old,
                now=20,
                lease_duration=100,
            )
        assert renewed.lease_expires_at == 120

        with pytest.raises(IngestFenceUnavailableError, match="stale"):
            with connector.transaction():
                IngestFenceRepository.lock_and_require_live(
                    VNextUnitOfWork(connector, backend="sqlite"), old, now=21
                )
        with connector.transaction():
            assert (
                IngestFenceRepository.lock_and_require_live(
                    VNextUnitOfWork(connector, backend="sqlite"), renewed, now=21
                )
                == renewed
            )
    finally:
        connector.close()


def test_ingest_overflow_checks_happen_before_authority_mutation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "overflow.sqlite3")
    try:
        with pytest.raises(DomainValidationError, match="duration"):
            _claim(connector, b"a" * 16, now=1, duration=-1)
        with pytest.raises(OverflowError, match="deadline"):
            _claim(connector, b"a" * 16, now=INT63_MAX, duration=1)
        assert (
            connector.fetch_all("SELECT generation FROM operational_ingest_generations")
            == []
        )

        connector.execute(
            "INSERT INTO operational_ingest_generations "
            "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
            (INT63_MAX, 1, 1),
        )
        connector.execute(
            "INSERT INTO operational_ingest_coordination_heads "
            "(singleton_id, current_generation, completed_generation, phase, "
            "last_transition_at) VALUES (1, %s, %s, %s, %s)",
            (INT63_MAX, INT63_MAX, "READY", 1),
        )
        before = _authority_snapshot(connector)
        with pytest.raises(IngestFenceExhaustedError):
            _claim(connector, b"b" * 16, now=2, duration=1)
        assert _authority_snapshot(connector) == before
    finally:
        connector.close()


def test_global_lock_order_requires_gate_before_ingest(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "lock-order.sqlite3")
    try:
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            gate = MaintenanceGateRepository.claim_shared(
                work,
                now=1,
                lease_duration=100,
            )
            turn = IngestFenceRepository.claim(
                work,
                owner_token=b"i" * 16,
                now=1,
                lease_duration=100,
            )
            assert gate.slots == (0,)
            assert turn.generation == 1

        with pytest.raises(LockOrderViolationError):
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend="sqlite")
                IngestFenceRepository.lock_and_require_live(work, turn, now=2)
                MaintenanceGateRepository.lock_and_require_live(work, gate, now=2)
    finally:
        connector.close()
