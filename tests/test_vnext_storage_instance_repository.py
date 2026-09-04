from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    StorageInstanceBinding,
    VNextDatabaseAdminFacade,
)
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.operational_refinement import _manifest_sha256
from h2hdb.schema_epoch import SQLiteSchemaEpochCatalog
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_storage_instance_repository import (
    StorageInstanceBindingMismatchError,
    StorageInstanceBindingUnavailableError,
    VNextStorageInstanceRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_FIRST_UUID = bytes.fromhex("00112233445546778899aabbccddeeff")
_SECOND_UUID = bytes.fromhex("102132435465487798a9bacbdcedfe0f")


def _database(path: Path) -> SQLiteConnector:
    connector = open_generated_sqlite_database(path)
    SQLiteSchemaEpochCatalog().create_control_table(connector)
    connector.execute(
        "INSERT INTO h2hdb_schema_epoch "
        "(singleton_id, epoch, schema_version, state, manifest_sha256, "
        "started_at, ready_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            1,
            ARTIFACT["epoch"],
            ARTIFACT["schema_version"],
            "READY",
            _manifest_sha256("sqlite"),
            0,
            0,
        ),
    )
    return connector


def _bind(connector: SQLiteConnector, value: bytes) -> StorageInstanceBinding:
    with connector.transaction():
        return VNextStorageInstanceRepository.bind(
            VNextUnitOfWork(connector, backend="sqlite"),
            storage_instance_uuid=value,
            expected_epoch=int(ARTIFACT["epoch"]),
            expected_schema_version=int(ARTIFACT["schema_version"]),
            expected_manifest_sha256=_manifest_sha256("sqlite"),
        )


def _mutation_statements(statements: list[str]) -> tuple[str, ...]:
    prefixes = ("INSERT ", "UPDATE ", "DELETE ", "REPLACE ")
    return tuple(
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(prefixes)
    )


def test_first_bind_and_exact_replay_preserve_one_uuid(tmp_path: Path) -> None:
    connector = _database(tmp_path / "binding.sqlite3")
    try:
        assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
        statements: list[str] = []
        connector.connection.set_trace_callback(statements.append)
        try:
            assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
        finally:
            connector.connection.set_trace_callback(None)

        assert _mutation_statements(statements) == ()
        assert connector.fetch_all(
            "SELECT singleton_id, storage_instance_uuid "
            "FROM operational_storage_instance_bindings"
        ) == [(1, _FIRST_UUID)]
    finally:
        connector.close()


def test_mismatch_is_zero_write(tmp_path: Path) -> None:
    connector = _database(tmp_path / "mismatch.sqlite3")
    try:
        _bind(connector, _FIRST_UUID)
        statements: list[str] = []
        connector.connection.set_trace_callback(statements.append)
        try:
            with pytest.raises(
                StorageInstanceBindingMismatchError,
                match="different storage instance",
            ):
                _bind(connector, _SECOND_UUID)
        finally:
            connector.connection.set_trace_callback(None)

        assert _mutation_statements(statements) == ()
        assert connector.fetch_one(
            "SELECT storage_instance_uuid "
            "FROM operational_storage_instance_bindings WHERE singleton_id = 1"
        ) == (_FIRST_UUID,)
    finally:
        connector.close()


def test_insert_fault_rolls_back_and_retry_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "fault.sqlite3")
    original_execute = connector.execute
    injected = False

    def execute(query: str, data: tuple[object, ...] = ()) -> None:
        nonlocal injected
        original_execute(query, data)
        if query.startswith("INSERT INTO operational_storage_instance_bindings"):
            injected = True
            raise RuntimeError("injected response loss")

    try:
        monkeypatch.setattr(connector, "execute", execute)
        with pytest.raises(
            StorageInstanceBindingUnavailableError,
            match="could not be recorded",
        ) as caught:
            _bind(connector, _FIRST_UUID)
        assert injected
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert str(caught.value.__cause__) == "injected response loss"
        monkeypatch.setattr(connector, "execute", original_execute)
        assert (
            connector.fetch_all(
                "SELECT storage_instance_uuid FROM operational_storage_instance_bindings"
            )
            == []
        )
        assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
    finally:
        connector.close()


def test_committed_bind_with_lost_response_replays_through_fresh_connector(
    tmp_path: Path,
) -> None:
    path = tmp_path / "committed-response-loss.sqlite3"
    connector = _database(path)
    try:
        with pytest.raises(RuntimeError, match="response was lost"):
            _bind(connector, _FIRST_UUID)
            raise RuntimeError("commit response was lost")
    finally:
        connector.close()

    fresh = SQLiteConnector(str(path))
    fresh.connect()
    try:
        statements: list[str] = []
        fresh.connection.set_trace_callback(statements.append)
        try:
            assert _bind(fresh, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
        finally:
            fresh.connection.set_trace_callback(None)
        assert _mutation_statements(statements) == ()
    finally:
        fresh.close()


def test_facade_rejects_manifest_drift_without_binding(tmp_path: Path) -> None:
    path = tmp_path / "manifest-drift.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    facade = VNextDatabaseAdminFacade(config)
    facade.initialize()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE h2hdb_schema_epoch SET manifest_sha256 = ? WHERE singleton_id = 1",
            (b"x" * 32,),
        )
        connection.commit()

    with pytest.raises(StorageInstanceBindingUnavailableError, match="exact READY"):
        facade.bind_storage_instance(_FIRST_UUID)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_storage_instance_bindings"
        ).fetchone() == (0,)


def test_facade_rejects_uninitialized_database_with_typed_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "uninitialized.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))

    with pytest.raises(
        StorageInstanceBindingUnavailableError,
        match="readable schema authority",
    ):
        VNextDatabaseAdminFacade(config).bind_storage_instance(_FIRST_UUID)

    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            == []
        )


def test_facade_rejects_blocked_provider_before_opening_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import h2hdb.vnext_schema_provider as provider_module

    path = tmp_path / "provider-blocked.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))

    def blocked_provider(_backend: str) -> object:
        raise RuntimeError("generated provider is blocked")

    monkeypatch.setattr(
        provider_module,
        "GeneratedVNextSchemaProvider",
        blocked_provider,
    )
    with pytest.raises(
        StorageInstanceBindingUnavailableError,
        match="schema provider is unavailable",
    ):
        VNextDatabaseAdminFacade(config).bind_storage_instance(_FIRST_UUID)
    assert not path.exists()


@pytest.mark.parametrize("value", (b"", bytes(15), bytes(16), bytes(17)))
def test_binding_rejects_invalid_or_nil_uuid(tmp_path: Path, value: bytes) -> None:
    connector = _database(tmp_path / f"invalid-{len(value)}.sqlite3")
    try:
        with pytest.raises(ValueError, match="storage instance UUID"):
            _bind(connector, value)
        assert (
            connector.fetch_all(
                "SELECT storage_instance_uuid FROM operational_storage_instance_bindings"
            )
            == []
        )
    finally:
        connector.close()


@pytest.mark.mariadb_smoke
def test_live_mariadb_fresh_facades_serialize_competing_first_bind(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
    barrier = Barrier(2)

    def propose(value: bytes) -> bytes | StorageInstanceBindingMismatchError:
        facade = VNextDatabaseAdminFacade(mariadb_config)
        barrier.wait(timeout=10)
        try:
            return facade.bind_storage_instance(value).storage_instance_uuid
        except StorageInstanceBindingMismatchError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(
            future.result()
            for future in (
                pool.submit(propose, _FIRST_UUID),
                pool.submit(propose, _SECOND_UUID),
            )
        )

    winners = tuple(value for value in results if isinstance(value, bytes))
    mismatches = tuple(
        value
        for value in results
        if isinstance(value, StorageInstanceBindingMismatchError)
    )
    assert len(winners) == len(mismatches) == 1
    assert winners[0] in {_FIRST_UUID, _SECOND_UUID}
    assert (
        VNextDatabaseAdminFacade(mariadb_config)
        .bind_storage_instance(winners[0])
        .storage_instance_uuid
        == winners[0]
    )
    VNextDatabaseAdminFacade(mariadb_config).check()
