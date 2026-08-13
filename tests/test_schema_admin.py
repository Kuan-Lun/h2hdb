from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from h2hdb import H2HDB, CoreConfig
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    SchemaCreateStatement,
    SchemaEpochAdmissionError,
    SchemaEpochDefinition,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaObjectKind,
    SchemaSeedStatement,
    SchemaSemanticValidationPhase,
    SchemaSlice,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import (
    VNextSchemaProviderError,
    VNextSchemaProviderUnavailableError,
)

_TABLE = SchemaObject(SchemaObjectKind.TABLE, "epoch_v2_probe")
_DDL = SchemaCreateStatement(
    "create:epoch_v2_probe",
    """
    CREATE TABLE IF NOT EXISTS epoch_v2_probe (
        probe_id INTEGER NOT NULL PRIMARY KEY CHECK (probe_id = 1),
        payload BLOB NOT NULL CHECK (typeof(payload) = 'blob')
    )
    """,
    _TABLE,
)
_SEED = SchemaSeedStatement(
    "seed:epoch_v2_probe",
    "epoch_v2_probe",
    """
    INSERT INTO epoch_v2_probe (probe_id, payload)
    VALUES (%s, %s)
    ON CONFLICT(probe_id) DO NOTHING
    """,
    (1, b"probe"),
)


def _definition() -> SchemaEpochDefinition:
    return SchemaEpochDefinition(
        epoch=2,
        schema_version=1,
        ddl_manifest_sha256="11" * 32,
        seed_manifest_sha256="22" * 32,
        obligation_manifest_sha256="33" * 32,
        expected_objects=frozenset({_TABLE}),
        slices=(SchemaSlice("probe", (_DDL,)),),
        bootstrap_seeds=(_SEED,),
        activation_semantic_obligation_ids=("probe-integrity",),
        ready_semantic_obligation_ids=("probe-integrity",),
    )


@dataclass
class _FakeProvider:
    definition: SchemaEpochDefinition

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None:
        del schema_slice
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = %s", (_TABLE.name,)
        ) == (1,)

    def validate_global(self, connector: SQLConnector) -> None:
        assert connector.fetch_one("SELECT COUNT(*) FROM epoch_v2_probe") == (1,)

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        assert connector.fetch_one("SELECT probe_id, payload FROM epoch_v2_probe") == (
            1,
            b"probe",
        )
        return (_SEED.seed_id,)

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        del connector, phase
        return ("probe-integrity",)


class _UnavailableProvider:
    @property
    def definition(self) -> SchemaEpochDefinition:
        raise VNextSchemaProviderUnavailableError("generated provider is blocked")


def _provider() -> _FakeProvider:
    return _FakeProvider(_definition())


def test_empty_database_initializes_epoch_without_legacy_tables(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)

    report = database.initialize_schema_epoch_v2(_provider())

    assert report.state == "READY"
    assert report.transitioned_to_ready
    readiness = database.check_schema_epoch_v2_readiness(_provider())
    assert readiness.manifest_sha256 == _definition().manifest_sha256
    with database._context.SQLConnector() as connector:
        assert not connector.check_table_exists("database_maintenance_state")
        assert not connector.check_table_exists("h2hdb_schema_migrations")


def test_ready_full_check_and_marker_check_issue_no_writes(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.initialize_schema_epoch_v2(_provider())
    writes: list[str] = []

    class RecordingSQLiteConnector(SQLiteConnector):
        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            writes.append(query)
            super().execute(query, data)

    context = replace(
        database._context,
        SQLConnector=lambda: RecordingSQLiteConnector(
            database=sqlite_config.database.database
        ),
    )
    admin = VNextSchemaAdmin(context)

    assert admin.check_readiness(_provider()).state == "READY"
    assert admin.check(_provider()).state == "READY"
    assert writes == []


def test_provider_unavailable_fails_before_opening_database(
    sqlite_config: CoreConfig,
) -> None:
    database_path = Path(sqlite_config.database.database)
    database = H2HDB(sqlite_config)

    with pytest.raises(VNextSchemaProviderUnavailableError, match="blocked"):
        database.initialize_schema_epoch_v2(_UnavailableProvider())  # type: ignore[arg-type]

    assert not database_path.exists()


def test_default_generated_provider_fails_closed_before_opening_database(
    sqlite_config: CoreConfig,
) -> None:
    database_path = Path(sqlite_config.database.database)

    with pytest.raises(VNextSchemaProviderError):
        H2HDB(sqlite_config).initialize_schema_epoch_v2()

    assert not database_path.exists()

    with pytest.raises(VNextSchemaProviderError):
        H2HDB(sqlite_config).check_schema_epoch_v2_readiness()

    assert not database_path.exists()


def test_initialize_rejects_nonempty_legacy_or_foreign_database(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    with database._context.SQLConnector() as connector:
        connector.execute("CREATE TABLE legacy_table (legacy_id INTEGER PRIMARY KEY)")

    with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
        database.initialize_schema_epoch_v2(_provider())

    with database._context.SQLConnector() as connector:
        assert not connector.check_table_exists("h2hdb_schema_epoch")


def test_full_check_does_not_resume_building_epoch(sqlite_config: CoreConfig) -> None:
    database = H2HDB(sqlite_config)
    provider = _provider()
    database.initialize_schema_epoch_v2(provider)
    with database._context.SQLConnector() as connector:
        connector.execute(
            "UPDATE h2hdb_schema_epoch SET state = 'BUILDING', ready_at = NULL"
        )

    with pytest.raises(SchemaEpochAdmissionError, match="not READY"):
        database.check_schema_epoch_v2(provider)


@pytest.mark.parametrize(
    ("command", "expected_call"),
    [
        ("epoch-v2-initialize", "initialize"),
        ("epoch-v2-check", "check"),
        ("epoch-v2-ready", "ready"),
    ],
)
def test_cli_routes_epoch_v2_commands(
    command: str,
    expected_call: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import __main__ as cli

    calls: list[str] = []

    class Logger:
        def info(self, message: str) -> None:
            del message

    class Database:
        logger = Logger()

        def __init__(self, config: object) -> None:
            del config

        def initialize_schema_epoch_v2(self) -> object:
            calls.append("initialize")
            return SimpleNamespace(epoch=2, schema_version=1, state="READY")

        def check_schema_epoch_v2(self) -> object:
            calls.append("check")
            return SimpleNamespace(epoch=2, schema_version=1, state="READY")

        def check_schema_epoch_v2_readiness(self) -> object:
            calls.append("ready")
            return SimpleNamespace(
                epoch=2,
                schema_version=1,
                manifest_sha256="44" * 32,
            )

    monkeypatch.setattr(cli, "load_config", lambda path: path)
    monkeypatch.setattr(cli, "H2HDB", Database)

    cli.main((command, "--config", "config.json"))

    assert calls == [expected_call]


def test_unreadable_readiness_marker_fails_closed(sqlite_config: CoreConfig) -> None:
    context = RepositoryContext.from_config(sqlite_config)
    with context.SQLConnector() as connector:
        connector.execute(
            "CREATE TABLE h2hdb_schema_epoch (singleton_id INTEGER PRIMARY KEY)"
        )

    with pytest.raises(SchemaEpochValidationError, match="unreadable"):
        VNextSchemaAdmin(context).check_readiness(_provider())
