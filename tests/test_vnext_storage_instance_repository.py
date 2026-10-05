from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    database_connector,
    inspect_all,
    inspect_one,
    open_generated_database,
    trace_statements,
)

from h2hdb import (
    CoreConfig,
    StorageInstanceBinding,
    VNextDatabaseAdminFacade,
)
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.operational_refinement import _manifest_sha256
from h2hdb.schema_epoch import MariaDBSchemaEpochCatalog, SQLiteSchemaEpochCatalog
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_storage_instance_repository import (
    StorageInstanceBindingMismatchError,
    StorageInstanceBindingUnavailableError,
    VNextStorageInstanceRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_FIRST_UUID = bytes.fromhex("00112233445546778899aabbccddeeff")
_SECOND_UUID = bytes.fromhex("102132435465487798a9bacbdcedfe0f")


def _database(config: CoreConfig) -> SQLConnector:
    connector = open_generated_database(config)
    catalog = (
        SQLiteSchemaEpochCatalog()
        if connector_backend(connector) == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )
    catalog.create_control_table(connector)
    connector.execute(
        "INSERT INTO h2hdb_schema_epoch "
        "(singleton_id, epoch, schema_version, state, manifest_sha256, "
        "started_at, ready_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            1,
            ARTIFACT["epoch"],
            ARTIFACT["schema_version"],
            "READY",
            _manifest_sha256(connector_backend(connector)),
            0,
            0,
        ),
    )
    return connector


def _bind(connector: SQLConnector, value: bytes) -> StorageInstanceBinding:
    with connector.transaction():
        return VNextStorageInstanceRepository.bind(
            VNextUnitOfWork(connector, backend=connector_backend(connector)),
            storage_instance_uuid=value,
            expected_epoch=int(ARTIFACT["epoch"]),
            expected_schema_version=int(ARTIFACT["schema_version"]),
            expected_manifest_sha256=_manifest_sha256(connector_backend(connector)),
        )


def _mutation_statements(statements: list[str]) -> tuple[str, ...]:
    prefixes = ("INSERT ", "UPDATE ", "DELETE ", "REPLACE ")
    return tuple(
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(prefixes)
    )


def test_first_bind_and_exact_replay_preserve_one_uuid(
    database_factory: DatabaseFactory, tmp_path: Path
) -> None:
    connector = _database(database_factory.config(str(tmp_path / "binding.sqlite3")))
    try:
        assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
        statements: list[str] = []
        with trace_statements(connector, statements):
            assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)

        assert _mutation_statements(statements) == ()
        assert inspect_all(
            connector,
            "SELECT singleton_id, storage_instance_uuid "
            "FROM operational_storage_instance_bindings",
        ) == [(1, _FIRST_UUID)]
    finally:
        connector.close()


def test_mismatch_is_zero_write(
    database_factory: DatabaseFactory, tmp_path: Path
) -> None:
    connector = _database(database_factory.config(str(tmp_path / "mismatch.sqlite3")))
    try:
        _bind(connector, _FIRST_UUID)
        statements: list[str] = []
        with trace_statements(connector, statements):
            with pytest.raises(
                StorageInstanceBindingMismatchError,
                match="different storage instance",
            ):
                _bind(connector, _SECOND_UUID)

        assert _mutation_statements(statements) == ()
        assert inspect_one(
            connector,
            "SELECT storage_instance_uuid "
            "FROM operational_storage_instance_bindings WHERE singleton_id = 1",
        ) == (_FIRST_UUID,)
    finally:
        connector.close()


def test_insert_fault_rolls_back_and_retry_converges(
    database_factory: DatabaseFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(database_factory.config(str(tmp_path / "fault.sqlite3")))
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
            inspect_all(
                connector,
                "SELECT storage_instance_uuid FROM operational_storage_instance_bindings",
            )
            == []
        )
        assert _bind(connector, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
    finally:
        connector.close()


def test_committed_bind_with_lost_response_replays_through_fresh_connector(
    database_factory: DatabaseFactory,
    tmp_path: Path,
) -> None:
    path = tmp_path / "committed-response-loss.sqlite3"
    connector = _database(database_factory.config(str(path)))
    try:
        with pytest.raises(RuntimeError, match="response was lost"):
            _bind(connector, _FIRST_UUID)
            raise RuntimeError("commit response was lost")
    finally:
        connector.close()

    fresh = database_connector(database_factory.config(str(path)))
    fresh.connect()
    try:
        statements: list[str] = []
        with trace_statements(fresh, statements):
            assert _bind(fresh, _FIRST_UUID) == StorageInstanceBinding(_FIRST_UUID)
        assert _mutation_statements(statements) == ()
    finally:
        fresh.close()


def test_facade_rejects_manifest_drift_without_binding(
    database_factory: DatabaseFactory, tmp_path: Path
) -> None:
    path = tmp_path / "manifest-drift.sqlite3"
    config = database_factory.config(str(path))
    facade = VNextDatabaseAdminFacade(config)
    facade.initialize()
    with database_connector(config) as connection:
        connection.execute(
            "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s WHERE singleton_id = 1",
            (b"x" * 32,),
        )
        connection.commit()

    with pytest.raises(StorageInstanceBindingUnavailableError, match="exact READY"):
        facade.bind_storage_instance(_FIRST_UUID)

    with database_connector(config) as connection:
        assert inspect_one(
            connection, "SELECT COUNT(*) FROM operational_storage_instance_bindings"
        ) == (0,)


def test_facade_rejects_uninitialized_database_with_typed_error(
    database_factory: DatabaseFactory,
    tmp_path: Path,
) -> None:
    path = tmp_path / "uninitialized.sqlite3"
    config = database_factory.config(str(path))

    with pytest.raises(
        StorageInstanceBindingUnavailableError,
        match="readable schema authority",
    ):
        VNextDatabaseAdminFacade(config).bind_storage_instance(_FIRST_UUID)

    with database_connector(config) as connection:
        catalog = (
            SQLiteSchemaEpochCatalog()
            if connector_backend(connection) == "sqlite"
            else MariaDBSchemaEpochCatalog()
        )
        assert catalog.list_objects(connection) == frozenset()


def _assert_blocked_provider_opens_no_database(
    config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import h2hdb.vnext_schema_provider as provider_module

    calls: list[str] = []
    sqlite_connect = SQLiteConnector.connect
    mariadb_connect = MariaDBConnector.connect

    def observed_sqlite_connect(connector: SQLiteConnector) -> None:
        calls.append("sqlite")
        sqlite_connect(connector)

    def observed_mariadb_connect(connector: MariaDBConnector) -> None:
        calls.append("mariadb")
        mariadb_connect(connector)

    def blocked_provider(_backend: str) -> object:
        raise RuntimeError("generated provider is blocked")

    # Allocation precedes this scope; the independent schema inspection follows
    # it. Count attempted native opens even if the facade wraps their exception.
    with monkeypatch.context() as observed:
        observed.setattr(SQLiteConnector, "connect", observed_sqlite_connect)
        observed.setattr(MariaDBConnector, "connect", observed_mariadb_connect)
        observed.setattr(
            provider_module, "GeneratedVNextSchemaProvider", blocked_provider
        )
        with pytest.raises(
            StorageInstanceBindingUnavailableError,
            match="schema provider is unavailable",
        ):
            VNextDatabaseAdminFacade(config).bind_storage_instance(_FIRST_UUID)
    assert calls == [], f"opened database before provider refusal: {calls}"


def _assert_empty_schema(config: CoreConfig) -> None:
    with database_connector(config) as connection:
        catalog = (
            SQLiteSchemaEpochCatalog()
            if connector_backend(connection) == "sqlite"
            else MariaDBSchemaEpochCatalog()
        )
        assert catalog.list_objects(connection) == frozenset()


def test_facade_rejects_blocked_provider_before_opening_database(
    database_factory: DatabaseFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = database_factory.config(str(tmp_path / "provider-blocked.sqlite3"))
    _assert_blocked_provider_opens_no_database(config, monkeypatch)
    _assert_empty_schema(config)


def test_blocked_provider_oracle_rejects_open_before_provider(
    database_factory: DatabaseFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = database_factory.config(str(tmp_path / "provider-open-first.sqlite3"))
    original = VNextDatabaseAdminFacade.bind_storage_instance

    def opens_first(
        facade: VNextDatabaseAdminFacade, storage_instance_uuid: bytes
    ) -> StorageInstanceBinding:
        with database_connector(config):
            pass
        return original(facade, storage_instance_uuid)

    monkeypatch.setattr(VNextDatabaseAdminFacade, "bind_storage_instance", opens_first)
    with pytest.raises(AssertionError, match="opened database before provider refusal"):
        _assert_blocked_provider_opens_no_database(config, monkeypatch)
    # This deliberate regression still satisfies the old, weaker schema oracle.
    _assert_empty_schema(config)


@pytest.mark.parametrize("value", (b"", bytes(15), bytes(16), bytes(17)))
def test_binding_rejects_invalid_or_nil_uuid(
    database_factory: DatabaseFactory, tmp_path: Path, value: bytes
) -> None:
    connector = _database(
        database_factory.config(str(tmp_path / f"invalid-{len(value)}.sqlite3"))
    )
    try:
        with pytest.raises(ValueError, match="storage instance UUID"):
            _bind(connector, value)
        assert (
            inspect_all(
                connector,
                "SELECT storage_instance_uuid FROM operational_storage_instance_bindings",
            )
            == []
        )
    finally:
        connector.close()


@pytest.mark.mariadb_smoke
def test_live_mariadb_fresh_facades_serialize_competing_first_bind(
    db_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(db_config).initialize()
    barrier = Barrier(2)

    def propose(value: bytes) -> bytes | StorageInstanceBindingMismatchError:
        facade = VNextDatabaseAdminFacade(db_config)
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
        VNextDatabaseAdminFacade(db_config)
        .bind_storage_instance(winners[0])
        .storage_instance_uuid
        == winners[0]
    )
    VNextDatabaseAdminFacade(db_config).check()
