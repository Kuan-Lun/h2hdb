from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast

import pytest
from vnext_test_database import database_connector

from h2hdb import (
    CoreConfig,
    DatabaseAccessMode,
    VNextDatabaseAdminFacade,
)
from h2hdb.domain import SchemaProvisioningOutcome
from h2hdb.mariadb_connector import INNODB_DURABILITY_QUERY
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    MariaDBSchemaEpochCatalog,
    SchemaEpochAdmissionError,
    SchemaEpochValidationError,
    SQLiteSchemaEpochCatalog,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_schema_provider import (
    VNextSchemaProviderUnavailableError,
)


def test_empty_database_initializes_epoch_without_legacy_tables(
    db_config: CoreConfig,
) -> None:
    database = VNextDatabaseAdminFacade(db_config)
    context = RepositoryContext.from_config(db_config)

    report = database.initialize()

    assert report.state == "READY"
    assert report.outcome is SchemaProvisioningOutcome.CREATED
    readiness = database.check_readiness()
    assert readiness.manifest_sha256 == report.manifest_sha256
    with context.SQLConnector() as connector:
        assert not connector.check_table_exists("database_maintenance_state")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_ready_full_check_and_marker_check_issue_no_writes(
    db_config: CoreConfig,
) -> None:
    context = RepositoryContext.from_config(db_config)
    VNextDatabaseAdminFacade(db_config).initialize()
    writes: list[str] = []

    def recording_connector() -> SQLConnector:
        connector = database_connector(db_config)
        original_execute = connector.execute

        def execute(query: str, data: tuple[Any, ...] = ()) -> None:
            writes.append(query)
            original_execute(query, data)

        connector.execute = execute  # type: ignore[method-assign] # Observe actual native connector.
        return connector

    context = replace(context, SQLConnector=recording_connector)
    admin = VNextSchemaAdmin(context)

    assert admin.check_readiness().state == "READY"
    assert admin.check().state == "READY"
    assert writes == []


def test_ready_full_check_works_through_read_only_db_config(
    db_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(db_config).initialize()
    read_only_config = db_config.model_copy(
        update={
            "database": db_config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )
    admin = VNextDatabaseAdminFacade(read_only_config)

    assert admin.check_readiness().state == "READY"
    report = admin.check()

    assert report.state == "READY"
    assert not report.transitioned_to_ready


def test_provider_unavailable_fails_before_opening_database(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = VNextDatabaseAdminFacade(db_config)
    opened: list[bool] = []
    native_type = type(database_connector(db_config))

    def forbid_connect(_connector: SQLConnector) -> None:
        opened.append(True)
        raise AssertionError("provider rejection must precede native connection")

    monkeypatch.setattr(native_type, "connect", forbid_connect)

    class UnavailableGeneratedProvider:
        def __init__(self, backend: str) -> None:
            assert backend == db_config.database.sql_type
            raise VNextSchemaProviderUnavailableError("generated provider is blocked")

    monkeypatch.setattr(
        "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider",
        UnavailableGeneratedProvider,
    )
    with pytest.raises(VNextSchemaProviderUnavailableError, match="blocked"):
        database.initialize()

    assert not opened


def test_default_generated_provider_admin_paths_initialize_and_validate_ready_epoch(
    db_config: CoreConfig,
) -> None:
    database = VNextDatabaseAdminFacade(db_config)
    context = RepositoryContext.from_config(db_config)

    initialized = database.initialize()
    replayed = database.initialize()
    checked = database.check()
    readiness = database.check_readiness()

    assert initialized.state == replayed.state == checked.state == "READY"
    assert initialized.outcome is SchemaProvisioningOutcome.CREATED
    assert replayed.outcome is SchemaProvisioningOutcome.ALREADY_READY
    assert not checked.transitioned_to_ready
    assert readiness.state == "READY"
    assert {
        initialized.manifest_sha256,
        replayed.manifest_sha256,
        checked.manifest_sha256,
        readiness.manifest_sha256,
    } == {initialized.manifest_sha256}
    with context.SQLConnector() as connector:
        assert connector.check_table_exists("h2hdb_schema_epoch")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_initialize_rejects_nonempty_legacy_or_foreign_database(
    db_config: CoreConfig,
) -> None:
    database = VNextDatabaseAdminFacade(db_config)
    context = RepositoryContext.from_config(db_config)
    with context.SQLConnector() as connector:
        connector.execute("CREATE TABLE legacy_table (legacy_id INTEGER PRIMARY KEY)")

    with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
        database.initialize()

    with context.SQLConnector() as connector:
        assert not connector.check_table_exists("h2hdb_schema_epoch")


def test_full_check_does_not_resume_building_epoch(db_config: CoreConfig) -> None:
    database = VNextDatabaseAdminFacade(db_config)
    context = RepositoryContext.from_config(db_config)
    database.initialize()
    with context.SQLConnector() as connector:
        with connector.transaction():
            connector.execute(
                "UPDATE h2hdb_schema_epoch SET state = 'BUILDING', ready_at = NULL"
            )

    with pytest.raises(SchemaEpochAdmissionError, match="not READY"):
        database.check()


@pytest.mark.parametrize(
    ("command", "expected_message"),
    [
        ("migrate", "schema provisioned"),
        ("check", "schema is valid"),
        ("ready", "database is ready"),
    ],
)
def test_cli_routes_greenfield_schema_commands(
    command: str,
    expected_message: str,
    monkeypatch: pytest.MonkeyPatch,
    db_config: CoreConfig,
) -> None:
    from h2hdb import __main__ as cli

    if command != "migrate":
        VNextSchemaAdmin(RepositoryContext.from_config(db_config)).initialize()
    messages: list[str] = []

    class Logger:
        def info(self, message: str) -> None:
            messages.append(message)

    monkeypatch.setattr(cli, "load_config", lambda path: db_config)
    monkeypatch.setattr(cli, "setup_logger", lambda config: Logger())

    cli.main((command, "--config", "config.json"))

    assert len(messages) == 1
    assert expected_message in messages[0]
    context = RepositoryContext.from_config(db_config)
    with context.SQLConnector() as connector:
        assert connector.check_table_exists("h2hdb_schema_epoch")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_unreadable_readiness_marker_fails_closed(db_config: CoreConfig) -> None:
    context = RepositoryContext.from_config(db_config)
    with context.SQLConnector() as connector:
        connector.execute(
            "CREATE TABLE h2hdb_schema_epoch (singleton_id INTEGER PRIMARY KEY)"
        )

    with pytest.raises(SchemaEpochValidationError, match="unreadable"):
        VNextSchemaAdmin(context).check_readiness()


def test_public_admin_and_cli_signatures_have_no_provider_injection() -> None:
    from h2hdb import __main__ as cli

    assert (
        "provider"
        not in inspect.signature(VNextDatabaseAdminFacade.initialize).parameters
    )
    assert (
        "provider" not in inspect.signature(VNextDatabaseAdminFacade.check).parameters
    )
    assert (
        "provider"
        not in inspect.signature(VNextDatabaseAdminFacade.check_readiness).parameters
    )
    assert "provider" not in inspect.signature(VNextSchemaAdmin.initialize).parameters
    assert "provider" not in inspect.signature(VNextSchemaAdmin.check).parameters
    assert (
        "provider" not in inspect.signature(VNextSchemaAdmin.check_readiness).parameters
    )
    assert "provider" not in inspect.signature(cli.main).parameters


def test_ready_provisioning_is_read_only_and_has_no_audit_or_inventory(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

    VNextDatabaseAdminFacade(db_config).initialize()
    reads: list[str] = []

    def marker_only_connector() -> SQLConnector:
        read_only = db_config.model_copy(
            update={
                "database": db_config.database.model_copy(
                    update={"access_mode": DatabaseAccessMode.read_only}
                )
            }
        )
        connector = database_connector(read_only)
        original_one = connector.fetch_one
        original_all = connector.fetch_all

        def begin() -> None:
            raise AssertionError("READY provisioning acquired a write transaction")

        def execute(query: str, data: tuple[Any, ...] = ()) -> None:
            raise AssertionError(f"READY provisioning wrote: {query}")

        def fetch_one(query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            reads.append(query)
            return original_one(query, data)

        def fetch_all(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            reads.append(query)
            return original_all(query, data)

        connector.begin = begin  # type: ignore[method-assign] # Native read-only regression guard.
        connector.execute = execute  # type: ignore[method-assign] # Native read-only regression guard.
        connector.fetch_one = fetch_one  # type: ignore[method-assign] # Native query observation.
        connector.fetch_all = fetch_all  # type: ignore[method-assign] # Native query observation.
        return connector

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("READY provisioning performed a full inventory/audit")

    catalog_type = (
        SQLiteSchemaEpochCatalog
        if db_config.database.sql_type == "sqlite"
        else MariaDBSchemaEpochCatalog
    )
    monkeypatch.setattr(catalog_type, "list_objects", forbidden)
    monkeypatch.setattr(GeneratedVNextSchemaProvider, "validate_global", forbidden)
    monkeypatch.setattr(GeneratedVNextSchemaProvider, "validate_semantics", forbidden)
    context = replace(
        RepositoryContext.from_config(db_config), SQLConnector=marker_only_connector
    )
    report = VNextSchemaAdmin(context).initialize()

    assert report.outcome is SchemaProvisioningOutcome.ALREADY_READY
    assert report.activation_audit is None
    if db_config.database.sql_type == "mariadb":
        # Native connect admission checks durability once before provisioning;
        # keep that independent scalar contract separate from schema reads.
        assert reads.count(INNODB_DURABILITY_QUERY) == 1
        reads.remove(INNODB_DURABILITY_QUERY)
    # SQLite: table existence, exact control DDL, marker. MariaDB adds one
    # DATABASE() lookup and five fixed control metadata checks instead of DDL.
    expected_bound = 3 if db_config.database.sql_type == "sqlite" else 8
    assert 1 <= len(reads) <= expected_bound
    assert all(
        "h2hdb_schema_epoch" in query
        or "sqlite_master" in query
        or "information_schema" in query.lower()
        or query.strip().upper() == "SELECT DATABASE()"
        for query in reads
    )


def test_generated_initialization_has_bounded_namespace_inventories(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb.schema_epoch import SchemaObject

    snapshots: list[frozenset[SchemaObject]] = []
    catalog_type = (
        SQLiteSchemaEpochCatalog
        if db_config.database.sql_type == "sqlite"
        else MariaDBSchemaEpochCatalog
    )
    original = cast(
        Callable[[Any, SQLConnector], frozenset[SchemaObject]],
        catalog_type.list_objects,
    )

    def inventory(
        catalog: SQLiteSchemaEpochCatalog | MariaDBSchemaEpochCatalog,
        connector: SQLConnector,
    ) -> frozenset[SchemaObject]:
        snapshot = original(catalog, connector)
        snapshots.append(snapshot)
        return snapshot

    monkeypatch.setattr(catalog_type, "list_objects", inventory)
    report = VNextDatabaseAdminFacade(db_config).initialize()

    assert report.outcome is SchemaProvisioningOutcome.CREATED
    assert report.activation_audit is not None
    assert snapshots[0] == frozenset()
    assert len(snapshots[-1]) > 100
    assert snapshots[-2] == snapshots[-1]
    assert len(snapshots) == 3  # Admission and fresh pre/post-activation boundaries.


def test_initialize_does_not_treat_database_errors_as_empty(
    db_config: CoreConfig,
) -> None:
    def unavailable_connector() -> SQLConnector:
        connector = database_connector(db_config)

        def check_table_exists(table_name: str) -> bool:
            raise RuntimeError("metadata unavailable")

        def execute(query: str, data: tuple[Any, ...] = ()) -> None:
            raise AssertionError("database failure led to schema construction")

        connector.check_table_exists = check_table_exists  # type: ignore[method-assign] # Inject native metadata fault.
        connector.execute = execute  # type: ignore[method-assign] # Guard against writes after rejection.
        return connector

    context = replace(
        RepositoryContext.from_config(db_config), SQLConnector=unavailable_connector
    )
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        VNextSchemaAdmin(context).initialize()


def test_cli_ready_provisioning_explicitly_reports_no_audit(
    monkeypatch: pytest.MonkeyPatch,
    db_config: CoreConfig,
) -> None:
    from h2hdb import __main__ as cli

    VNextDatabaseAdminFacade(db_config).initialize()
    messages: list[str] = []

    class Logger:
        def info(self, message: str) -> None:
            messages.append(message)

    monkeypatch.setattr(cli, "load_config", lambda path: db_config)
    monkeypatch.setattr(cli, "setup_logger", lambda config: Logger())
    cli.main(("migrate", "--config", "config.json"))

    assert len(messages) == 1
    assert "outcome=already_ready" in messages[0]
    assert "audit=not_performed" in messages[0]
    assert "full READY audit" in messages[0]
