from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from mysql.connector import Error as MySQLConnectorError
from test_vnext_live_mariadb_operational import (
    _claim_exclusive as _claim_exclusive_mariadb,
)
from test_vnext_live_mariadb_operational import _claim_shared as _claim_shared_mariadb
from test_vnext_live_mariadb_operational import _gate_snapshot as _mariadb_snapshot
from test_vnext_live_mariadb_operational import generated_mariadb as generated_mariadb
from test_vnext_maintenance_gate_repository import (
    _claim_exclusive,
    _claim_shared,
    _gate_snapshot,
    _generated_database,
)

from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sqlite_connector import SQLiteDuplicateKeyError
from h2hdb.vnext_maintenance_gate_repository import (
    MaintenanceGateCorruptionError,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def test_exclusive_holder_mutations_have_constant_round_trips(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "gate.db")
    mutations: list[str] = []
    execute = connector.execute_affected

    def record(query: str, data: tuple[Any, ...] = ()) -> int:
        mutations.append(query)
        return execute(query, data)

    try:
        with patch.object(connector, "execute_affected", record):
            lease = _claim_exclusive(connector, b"a" * 16, now=10, duration=100)
            assert connector.fetch_all(
                "SELECT slot, owner_token FROM operational_maintenance_gate_holders "
                "ORDER BY slot"
            ) == [(slot, lease.owner_token) for slot in range(64)]
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend="sqlite"), lease, now=11
                )
        # One bounded INSERT and one bounded DELETE replace 128 slot writes.
        assert (
            sum("operational_maintenance_gate_holders" in sql for sql in mutations) == 2
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_maintenance_gate_holders"
        ) == (0,)
    finally:
        connector.close()


def test_expired_shared_pool_is_replaced_with_one_owner_delete(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "gate.db")
    mutations: list[str] = []
    execute = connector.execute_affected

    def record(query: str, data: tuple[Any, ...] = ()) -> int:
        mutations.append(query)
        return execute(query, data)

    try:
        old = [
            _claim_shared(connector, slot.to_bytes(16, "big"), now=1, duration=10)
            for slot in range(64)
        ]
        with patch.object(connector, "execute_affected", record):
            new = _claim_exclusive(connector, b"n" * 16, now=11, duration=100)
        assert (
            sum(
                sql.startswith("DELETE FROM operational_maintenance_gate_owners")
                for sql in mutations
            )
            == 1
        )
        assert (
            sum("operational_maintenance_gate_holders" in sql for sql in mutations) == 2
        )
        assert connector.fetch_all(
            "SELECT owner_token, gate_generation, lease_expires_at "
            "FROM operational_maintenance_gate_owners"
        ) == [(new.owner_token, new.gate_generation, new.lease_expires_at)]
        assert connector.fetch_all(
            "SELECT slot, owner_token FROM operational_maintenance_gate_holders ORDER BY slot"
        ) == [(slot, new.owner_token) for slot in range(64)]
        with pytest.raises(MaintenanceGateUnavailableError, match="stale"):
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend="sqlite"), old[0], now=12
                )
    finally:
        connector.close()


@pytest.mark.parametrize(
    "operation,prefix",
    [
        ("claim", "INSERT INTO operational_maintenance_gate_holders"),
        ("claim", "DELETE FROM operational_maintenance_gate_holders"),
        ("claim", "DELETE FROM operational_maintenance_gate_owners"),
        ("release", "DELETE FROM operational_maintenance_gate_holders"),
        ("release", "DELETE FROM operational_maintenance_gate_owners"),
    ],
)
def test_inexact_batch_count_rolls_back_complete_gate_transition(
    tmp_path: Path, operation: str, prefix: str
) -> None:
    connector = _generated_database(tmp_path / "gate.db")
    execute = connector.execute_affected

    def report_partial(query: str, data: tuple[Any, ...] = ()) -> int:
        count = execute(query, data)
        return count - 1 if query.startswith(prefix) else count

    try:
        old = _claim_exclusive(connector, b"a" * 16, now=1, duration=10)
        before = _gate_snapshot(connector)
        with patch.object(connector, "execute_affected", report_partial):
            with pytest.raises(MaintenanceGateCorruptionError):
                if operation == "claim":
                    _claim_exclusive(connector, b"b" * 16, now=11, duration=10)
                else:
                    with connector.transaction():
                        MaintenanceGateRepository.release(
                            VNextUnitOfWork(connector, backend="sqlite"), old, now=2
                        )
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_failed_bulk_takeover_restores_expired_owners_and_holders(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gate.db")
    try:
        _claim_exclusive(connector, b"a" * 16, now=1, duration=10)
        before = _gate_snapshot(connector)
        connector.execute(
            "CREATE TRIGGER fail_holder_insert BEFORE INSERT "
            "ON operational_maintenance_gate_holders WHEN NEW.slot = 32 "
            "BEGIN SELECT RAISE(ABORT, 'injected holder failure'); END"
        )
        with pytest.raises(SQLiteDuplicateKeyError, match="injected holder failure"):
            _claim_exclusive(connector, b"b" * 16, now=11, duration=100)
        assert _gate_snapshot(connector) == before
    finally:
        connector.close()


def test_live_mariadb_reclaims_full_shared_pool_and_releases_exact_set(
    generated_mariadb: MariaDBConnector,
) -> None:
    connector = generated_mariadb
    for slot in range(64):
        _claim_shared_mariadb(connector, slot.to_bytes(16, "big"), now=1, duration=10)
    mutations: list[str] = []
    execute = connector.execute_affected

    def record(query: str, data: tuple[Any, ...] = ()) -> int:
        mutations.append(query)
        return execute(query, data)

    with patch.object(connector, "execute_affected", record):
        lease = _claim_exclusive_mariadb(connector, b"n" * 16, now=11, duration=100)
        assert _mariadb_snapshot(connector)[3] == [
            (slot, lease.owner_token) for slot in range(64)
        ]
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="mariadb")
            assert MaintenanceGateRepository.resume(work, lease, now=12) == lease
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="mariadb"), lease, now=12
            )
    # Reclaiming the old slot/owner sets, inserting the new set and releasing it
    # each takes one statement, independently of the 64 expired owners.
    assert sum("operational_maintenance_gate_holders" in sql for sql in mutations) == 3
    assert (
        sum(
            sql.startswith("DELETE FROM operational_maintenance_gate_owners")
            for sql in mutations
        )
        == 2
    )
    assert _mariadb_snapshot(connector)[2:] == ([], [])


def test_live_mariadb_failed_bulk_takeover_restores_complete_authority(
    generated_mariadb: MariaDBConnector,
) -> None:
    connector = generated_mariadb
    _claim_exclusive_mariadb(connector, b"a" * 16, now=1, duration=10)
    before = _mariadb_snapshot(connector)
    connector.execute(
        "CREATE TRIGGER fail_holder_insert BEFORE INSERT "
        "ON operational_maintenance_gate_holders FOR EACH ROW BEGIN "
        "IF NEW.slot = 32 THEN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'injected holder failure'; END IF; END"
    )
    with pytest.raises(MySQLConnectorError, match="injected holder failure"):
        _claim_exclusive_mariadb(connector, b"b" * 16, now=11, duration=100)
    assert _mariadb_snapshot(connector) == before
