from __future__ import annotations

import inspect
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from h2hdb import (
    CoreConfig,
    DatabaseAccessMode,
    VNextDatabaseAdminFacade,
)
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    SchemaEpochAdmissionError,
    SchemaEpochValidationError,
)
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import (
    VNextSchemaProviderUnavailableError,
)


def test_empty_database_initializes_epoch_without_legacy_tables(
    sqlite_config: CoreConfig,
) -> None:
    database = VNextDatabaseAdminFacade(sqlite_config)
    context = RepositoryContext.from_config(sqlite_config)

    report = database.initialize()

    assert report.state == "READY"
    assert report.transitioned_to_ready
    readiness = database.check_readiness()
    assert readiness.manifest_sha256 == report.manifest_sha256
    with context.SQLConnector() as connector:
        assert not connector.check_table_exists("database_maintenance_state")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_ready_full_check_and_marker_check_issue_no_writes(
    sqlite_config: CoreConfig,
) -> None:
    context = RepositoryContext.from_config(sqlite_config)
    VNextDatabaseAdminFacade(sqlite_config).initialize()
    writes: list[str] = []

    class RecordingSQLiteConnector(SQLiteConnector):
        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            writes.append(query)
            super().execute(query, data)

    context = replace(
        context,
        SQLConnector=lambda: RecordingSQLiteConnector(
            database=sqlite_config.database.database
        ),
    )
    admin = VNextSchemaAdmin(context)

    assert admin.check_readiness().state == "READY"
    assert admin.check().state == "READY"
    assert writes == []


def test_ready_full_check_works_through_read_only_sqlite_config(
    sqlite_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(sqlite_config).initialize()
    read_only_config = sqlite_config.model_copy(
        update={
            "database": sqlite_config.database.model_copy(
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
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = Path(sqlite_config.database.database)
    database = VNextDatabaseAdminFacade(sqlite_config)

    class UnavailableGeneratedProvider:
        def __init__(self, backend: str) -> None:
            assert backend == "sqlite"
            raise VNextSchemaProviderUnavailableError("generated provider is blocked")

    monkeypatch.setattr(
        "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider",
        UnavailableGeneratedProvider,
    )
    with pytest.raises(VNextSchemaProviderUnavailableError, match="blocked"):
        database.initialize()

    assert not database_path.exists()


def test_default_generated_provider_admin_paths_initialize_and_validate_ready_epoch(
    sqlite_config: CoreConfig,
) -> None:
    database_path = Path(sqlite_config.database.database)
    database = VNextDatabaseAdminFacade(sqlite_config)
    context = RepositoryContext.from_config(sqlite_config)

    initialized = database.initialize()
    replayed = database.initialize()
    checked = database.check()
    readiness = database.check_readiness()

    assert database_path.exists()
    assert initialized.state == replayed.state == checked.state == "READY"
    assert initialized.transitioned_to_ready
    assert not replayed.transitioned_to_ready
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
    sqlite_config: CoreConfig,
) -> None:
    database = VNextDatabaseAdminFacade(sqlite_config)
    context = RepositoryContext.from_config(sqlite_config)
    with context.SQLConnector() as connector:
        connector.execute("CREATE TABLE legacy_table (legacy_id INTEGER PRIMARY KEY)")

    with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
        database.initialize()

    with context.SQLConnector() as connector:
        assert not connector.check_table_exists("h2hdb_schema_epoch")


def test_full_check_does_not_resume_building_epoch(sqlite_config: CoreConfig) -> None:
    database = VNextDatabaseAdminFacade(sqlite_config)
    context = RepositoryContext.from_config(sqlite_config)
    database.initialize()
    with context.SQLConnector() as connector:
        connector.execute(
            "UPDATE h2hdb_schema_epoch SET state = 'BUILDING', ready_at = NULL"
        )

    with pytest.raises(SchemaEpochAdmissionError, match="not READY"):
        database.check()


@pytest.mark.parametrize(
    ("command", "expected_message"),
    [
        ("migrate", "schema initialized"),
        ("check", "schema is valid"),
        ("ready", "database is ready"),
    ],
)
def test_cli_routes_greenfield_schema_commands(
    command: str,
    expected_message: str,
    monkeypatch: pytest.MonkeyPatch,
    sqlite_config: CoreConfig,
) -> None:
    from h2hdb import __main__ as cli

    if command != "migrate":
        VNextSchemaAdmin(RepositoryContext.from_config(sqlite_config)).initialize()
    messages: list[str] = []

    class Logger:
        def info(self, message: str) -> None:
            messages.append(message)

    monkeypatch.setattr(cli, "load_config", lambda path: sqlite_config)
    monkeypatch.setattr(cli, "setup_logger", lambda config: Logger())

    cli.main((command, "--config", "config.json"))

    assert len(messages) == 1
    assert expected_message in messages[0]
    context = RepositoryContext.from_config(sqlite_config)
    with context.SQLConnector() as connector:
        assert connector.check_table_exists("h2hdb_schema_epoch")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_unreadable_readiness_marker_fails_closed(sqlite_config: CoreConfig) -> None:
    context = RepositoryContext.from_config(sqlite_config)
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
