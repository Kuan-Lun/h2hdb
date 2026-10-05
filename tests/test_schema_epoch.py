from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    fixture_transaction,
    inspect_all,
    inspect_one,
    open_database,
)

from h2hdb import CoreConfig, DatabaseAccessMode
from h2hdb.domain import (
    SchemaEpochReport,
    SchemaProvisioningOutcome,
    SchemaProvisioningReport,
)
from h2hdb.schema_epoch import (
    SCHEMA_EPOCH_CONTROL_TABLE,
    MariaDBSchemaEpochCatalog,
    SchemaCreateStatement,
    SchemaEpochAdmissionError,
    SchemaEpochDefinition,
    SchemaEpochDriftError,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaObjectKind,
    SchemaSeedStatement,
    SchemaSemanticValidationPhase,
    SchemaSlice,
    SQLiteSchemaEpochCatalog,
    run_mariadb_schema_epoch,
    run_sqlite_schema_epoch,
    validate_mariadb_schema_epoch,
    validate_sqlite_schema_epoch,
)
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector

DDL_MANIFEST = "11" * 32
SEED_MANIFEST = "33" * 32
OBLIGATION_MANIFEST = "22" * 32
NOW = datetime(2026, 8, 12, 12, 34, 56, 123456, tzinfo=UTC)

PARENT = SchemaObject(SchemaObjectKind.TABLE, "vnext_epoch_parents")
CHILD = SchemaObject(SchemaObjectKind.TABLE, "vnext_epoch_children")
CHILD_INDEX = SchemaObject(SchemaObjectKind.INDEX, "vnext_epoch_children_parent_idx")

PARENT_STATEMENT = SchemaCreateStatement(
    "create-parent",
    """
    CREATE TABLE IF NOT EXISTS vnext_epoch_parents (
        parent_id INTEGER NOT NULL PRIMARY KEY,
        payload BLOB NOT NULL CHECK (typeof(payload) = 'blob'),
        payload_version INTEGER NOT NULL CHECK (payload_version = 1)
    )
    """,
    PARENT,
)
CHILD_STATEMENT = SchemaCreateStatement(
    "create-child",
    """
    CREATE TABLE IF NOT EXISTS vnext_epoch_children (
        child_id INTEGER NOT NULL PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        digest BLOB NOT NULL
            CHECK (typeof(digest) = 'blob' AND length(digest) = 32),
        FOREIGN KEY (parent_id)
            REFERENCES vnext_epoch_parents (parent_id)
    )
    """,
    CHILD,
)
INDEX_STATEMENT = SchemaCreateStatement(
    "create-child-parent-index",
    """
    CREATE INDEX IF NOT EXISTS vnext_epoch_children_parent_idx
    ON vnext_epoch_children (parent_id, child_id)
    """,
    CHILD_INDEX,
)
PARENT_SEED = SchemaSeedStatement(
    "genesis-parent",
    PARENT.name,
    """
    INSERT INTO vnext_epoch_parents (
        parent_id, payload, payload_version
    ) VALUES (%s, %s, %s)
    ON CONFLICT(parent_id) DO NOTHING
    """,
    (0, b"\x00" * 32, 1),
)


def _definition(
    *,
    ddl_manifest_sha256: str = DDL_MANIFEST,
    obligation_manifest_sha256: str = OBLIGATION_MANIFEST,
) -> SchemaEpochDefinition:
    return SchemaEpochDefinition(
        epoch=3,
        schema_version=9,
        ddl_manifest_sha256=ddl_manifest_sha256,
        seed_manifest_sha256=SEED_MANIFEST,
        obligation_manifest_sha256=obligation_manifest_sha256,
        expected_objects=frozenset({PARENT, CHILD, CHILD_INDEX}),
        slices=(
            SchemaSlice("identity", (PARENT_STATEMENT,)),
            SchemaSlice("membership", (CHILD_STATEMENT, INDEX_STATEMENT)),
        ),
        bootstrap_seeds=(PARENT_SEED,),
        activation_semantic_obligation_ids=(
            "canonical-digest-integrity",
            "versioned-leaf-byte-bounds",
            "singleton-seeds",
        ),
        ready_semantic_obligation_ids=(
            "canonical-digest-integrity",
            "versioned-leaf-byte-bounds",
        ),
    )


@pytest.mark.parametrize("version", [1, 2, 3, 4, 5, 6, 7, 8])
def test_prior_schema_definition_is_rejected_without_compatibility_path(
    version: int,
) -> None:
    with pytest.raises(ValueError, match=f"supports schema version 9, not {version}"):
        replace(_definition(), schema_version=version)


@dataclass
class FakeProvider:
    definition: SchemaEpochDefinition
    backend: str = "sqlite"
    semantic_result: Sequence[str] | None = None
    semantic_error: Exception | None = None
    global_error: Exception | None = None
    slice_error: Exception | None = None
    slice_hook: Callable[[SchemaSlice], None] | None = None
    seed_result: Sequence[str] | None = None
    seed_error: Exception | None = None
    semantic_phases: list[SchemaSemanticValidationPhase] = field(default_factory=list)

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None:
        if self.slice_hook is not None:
            self.slice_hook(schema_slice)
        if self.slice_error is not None:
            raise self.slice_error
        match schema_slice.slice_id:
            case "identity":
                assert _table_columns(connector, PARENT.name, backend=self.backend) == (
                    ("parent_id", "INTEGER", 1, 1),
                    ("payload", "BLOB", 1, 0),
                    ("payload_version", "INTEGER", 1, 0),
                )
            case "membership":
                assert _table_columns(connector, CHILD.name, backend=self.backend) == (
                    ("child_id", "INTEGER", 1, 1),
                    ("parent_id", "INTEGER", 1, 0),
                    ("digest", "BLOB", 1, 0),
                )
                if self.backend == "sqlite":
                    indexes = connector.fetch_all(
                        "PRAGMA index_list(vnext_epoch_children)"
                    )
                    assert CHILD_INDEX.name in {str(row[1]) for row in indexes}
                else:
                    indexes = connector.fetch_all(
                        "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vnext_epoch_children'"
                    )
                    assert CHILD_INDEX.name in {str(row[0]) for row in indexes}
            case _:  # pragma: no cover - protects future fake-provider edits
                raise AssertionError(f"Unknown test slice: {schema_slice.slice_id}")

    def validate_global(self, connector: SQLConnector) -> None:
        if self.global_error is not None:
            raise self.global_error
        if self.backend == "sqlite":
            assert connector.fetch_all("PRAGMA foreign_key_check") == []
        else:
            assert (
                connector.fetch_all(
                    "SELECT child.child_id FROM vnext_epoch_children AS child LEFT JOIN vnext_epoch_parents AS parent ON child.parent_id = parent.parent_id WHERE parent.parent_id IS NULL"
                )
                == []
            )
        assert connector.fetch_one("SELECT COUNT(*) FROM h2hdb_schema_epoch") == (1,)
        if self.backend == "sqlite":
            assert connector.fetch_one(
                "SELECT typeof(manifest_sha256), length(manifest_sha256) FROM h2hdb_schema_epoch WHERE singleton_id = 1"
            ) == ("blob", 32)
        else:
            assert connector.fetch_one(
                "SELECT OCTET_LENGTH(manifest_sha256) FROM h2hdb_schema_epoch WHERE singleton_id = 1"
            ) == (32,)

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        if self.seed_error is not None:
            raise self.seed_error
        rows = connector.fetch_all(
            "SELECT parent_id, payload, payload_version "
            "FROM vnext_epoch_parents ORDER BY parent_id"
        )
        if rows != [(0, b"\x00" * 32, 1)]:
            raise SchemaEpochValidationError(
                f"Bootstrap parent row differs from the formal seed: {rows!r}"
            )
        return (
            tuple(seed.seed_id for seed in self.definition.bootstrap_seeds)
            if self.seed_result is None
            else self.seed_result
        )

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        self.semantic_phases.append(phase)
        if self.semantic_error is not None:
            raise self.semantic_error
        # These fake checks exercise the provider boundary.  The production
        # generated provider will own the actual canonical-digest, bounded-leaf,
        # and seed queries named by its checksum-pinned obligation manifest.
        assert connector.fetch_one(
            "SELECT singleton_id, epoch, schema_version FROM h2hdb_schema_epoch"
        ) == (1, 3, 9)
        expected = (
            self.definition.activation_semantic_obligation_ids
            if phase is SchemaSemanticValidationPhase.ACTIVATION
            else self.definition.ready_semantic_obligation_ids
        )
        return expected if self.semantic_result is None else self.semantic_result


def _table_columns(
    connector: SQLConnector, table_name: str, *, backend: str | None = None
) -> tuple[tuple[str, str, int, int], ...]:
    backend = backend or connector_backend(connector)
    if backend == "sqlite":
        return tuple(
            (str(name), str(column_type), int(not_null), int(primary_key))
            for _, name, column_type, not_null, _, primary_key in inspect_all(
                connector, f"PRAGMA table_info({table_name})"
            )
        )
    return tuple(
        (
            str(name),
            "INTEGER"
            if str(column_type).lower() == "int"
            else str(column_type).upper(),
            int(nullable == "NO"),
            int(key == "PRI"),
        )
        for name, column_type, nullable, key in inspect_all(
            connector,
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (table_name,),
        )
    )


def _connected(config: CoreConfig) -> SQLConnector:
    return open_database(config)


def _catalog(
    connector: SQLConnector,
) -> SQLiteSchemaEpochCatalog | MariaDBSchemaEpochCatalog:
    return (
        SQLiteSchemaEpochCatalog()
        if connector_backend(connector) == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )


def _native_definition(
    definition: SchemaEpochDefinition, backend: str
) -> SchemaEpochDefinition:
    if backend == "sqlite":
        return definition
    parent = replace(
        PARENT_STATEMENT,
        sql="CREATE TABLE IF NOT EXISTS vnext_epoch_parents (parent_id INTEGER NOT NULL PRIMARY KEY, payload BLOB NOT NULL, payload_version INTEGER NOT NULL CHECK (payload_version = 1)) ENGINE=InnoDB",
    )
    child = replace(
        CHILD_STATEMENT,
        sql="CREATE TABLE IF NOT EXISTS vnext_epoch_children (child_id INTEGER NOT NULL PRIMARY KEY, parent_id INTEGER NOT NULL, digest BLOB NOT NULL CHECK (OCTET_LENGTH(digest) = 32), KEY vnext_epoch_children_parent_idx (parent_id, child_id), FOREIGN KEY (parent_id) REFERENCES vnext_epoch_parents (parent_id)) ENGINE=InnoDB",
    )
    return replace(
        definition,
        expected_objects=frozenset({PARENT, CHILD}),
        slices=(
            SchemaSlice("identity", (parent,)),
            SchemaSlice("membership", (child,)),
        ),
        bootstrap_seeds=tuple(
            replace(
                seed,
                sql=seed.sql.replace(
                    "ON CONFLICT(parent_id) DO NOTHING",
                    "ON DUPLICATE KEY UPDATE parent_id = parent_id",
                ),
            )
            for seed in definition.bootstrap_seeds
        ),
    )


def _native_provider(connector: SQLConnector, provider: FakeProvider) -> FakeProvider:
    backend = connector_backend(connector)
    return replace(
        provider,
        definition=_native_definition(provider.definition, backend),
        backend=backend,
    )


def _run_schema_epoch(
    connector: SQLConnector,
    provider: FakeProvider,
    *,
    clock: Callable[[], datetime] | None = None,
) -> SchemaProvisioningReport:
    function = (
        run_sqlite_schema_epoch
        if connector_backend(connector) == "sqlite"
        else run_mariadb_schema_epoch
    )
    return function(connector, _native_provider(connector, provider), clock=clock)


def _validate_schema_epoch(
    connector: SQLConnector, provider: FakeProvider
) -> SchemaEpochReport:
    function = (
        validate_sqlite_schema_epoch
        if connector_backend(connector) == "sqlite"
        else validate_mariadb_schema_epoch
    )
    return function(connector, _native_provider(connector, provider))


def _assert_no_open_transaction(connector: SQLConnector) -> None:
    assert not getattr(connector, "connection").in_transaction
    if connector_backend(connector) == "mariadb":
        assert not getattr(connector, "_in_transaction")


def _assert_unpublished(connector: SQLConnector) -> None:
    if connector_backend(connector) == "sqlite":
        assert _catalog(connector).list_objects(connector) == frozenset()
    else:
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )


def _initialize_building(
    connector: SQLConnector,
    definition: SchemaEpochDefinition,
    *,
    completed_statement_count: int = 0,
) -> None:
    definition = _native_definition(definition, connector_backend(connector))
    statements = [
        statement
        for schema_slice in definition.slices
        for statement in schema_slice.statements
    ]

    def control() -> None:
        _catalog(connector).create_control_table(connector)

    def marker() -> None:
        connector.execute(
            "INSERT INTO h2hdb_schema_epoch (singleton_id, epoch, schema_version, state, manifest_sha256, started_at, ready_at) VALUES (1, %s, %s, 'BUILDING', %s, 1, NULL)",
            (
                definition.epoch,
                definition.schema_version,
                bytes.fromhex(definition.manifest_sha256),
            ),
        )

    if connector_backend(connector) == "sqlite":
        with connector.transaction():
            control()
            marker()
            for statement in statements[:completed_statement_count]:
                connector.execute(statement.sql)
    else:
        control()
        with connector.transaction():
            marker()
        for statement in statements[:completed_statement_count]:
            connector.execute(statement.sql)


class FaultAtProviderStatementConnector(SQLiteConnector):
    def __init__(self, database: str, fail_at: int) -> None:
        super().__init__(database)
        self._fail_at = fail_at
        self._provider_statement_count = 0
        self._failed = False

    def execute(self, query: str, data: tuple[object, ...] = ()) -> None:
        if any(name in query for name in (PARENT.name, CHILD.name, CHILD_INDEX.name)):
            self._maybe_fail()
        super().execute(query, data)

    def execute_many(
        self,
        query: str,
        data: list[tuple[object, ...]],
    ) -> None:
        if any(name in query for name in (PARENT.name, CHILD.name, CHILD_INDEX.name)):
            self._maybe_fail()
        super().execute_many(query, data)

    def _maybe_fail(self) -> None:
        self._provider_statement_count += 1
        if not self._failed and self._provider_statement_count == self._fail_at:
            self._failed = True
            raise RuntimeError(f"fault at provider statement {self._fail_at}")


def test_empty_database_builds_and_ready_rerun_only_probes_marker(
    database_factory: DatabaseFactory,
) -> None:
    database = database_factory.config()
    connector = _connected(database)
    provider = FakeProvider(_definition())
    try:
        first = _run_schema_epoch(connector, provider, clock=lambda: NOW)
        _assert_no_open_transaction(connector)
        second = _run_schema_epoch(connector, provider, clock=lambda: NOW)
        _assert_no_open_transaction(connector)
    finally:
        connector.close()

    assert first.state == "READY"
    assert first.outcome is SchemaProvisioningOutcome.CREATED
    assert first.activation_audit is not None
    assert (
        first.activation_audit.semantic_obligation_ids
        == provider.definition.semantic_obligation_ids
    )
    assert second.state == "READY"
    assert second.outcome is SchemaProvisioningOutcome.ALREADY_READY
    assert second.activation_audit is None


@pytest.mark.parametrize(
    "legacy_ddl",
    [
        "CREATE TABLE h2hdb_schema_migrations (version INTEGER PRIMARY KEY)",
        "CREATE TABLE unrelated (value TEXT)",
        "CREATE VIEW unrelated AS SELECT 1 AS value",
    ],
)
def test_nonempty_database_is_rejected_without_drop_or_adoption(
    database_factory: DatabaseFactory, legacy_ddl: str
) -> None:
    database = database_factory.config()
    connector = _connected(database)
    with fixture_transaction(connector):
        connector.execute(legacy_ddl)
    try:
        with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
            _run_schema_epoch(connector, FakeProvider(_definition()))
        objects = _catalog(connector).list_objects(connector)
    finally:
        connector.close()

    assert (
        SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE) not in objects
    )
    assert objects


def test_control_table_with_wrong_shape_is_rejected(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    with fixture_transaction(connector):
        connector.execute(
            "CREATE TABLE h2hdb_schema_epoch (singleton_id INTEGER PRIMARY KEY)"
        )
    try:
        with pytest.raises(SchemaEpochValidationError, match="wrong shape"):
            _run_schema_epoch(connector, FakeProvider(_definition()))
        columns = _table_columns(connector, "h2hdb_schema_epoch")
    finally:
        connector.close()

    assert len(columns) == 1


@pytest.mark.parametrize("completed_statement_count", [0, 1, 2, 3])
def test_committed_partial_build_resumes_from_slice_one(
    database_factory: DatabaseFactory, completed_statement_count: int
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(
        connector,
        definition,
        completed_statement_count=completed_statement_count,
    )
    visited_slices: list[str] = []
    provider = FakeProvider(
        definition,
        slice_hook=lambda schema_slice: visited_slices.append(schema_slice.slice_id),
    )
    try:
        report = _run_schema_epoch(connector, provider, clock=lambda: NOW)
        _assert_no_open_transaction(connector)
        assert (
            _run_schema_epoch(connector, provider).outcome
            is SchemaProvisioningOutcome.ALREADY_READY
        )
        _assert_no_open_transaction(connector)
    finally:
        connector.close()

    assert visited_slices == ["identity", "membership"]
    assert report.outcome is SchemaProvisioningOutcome.RESUMED
    assert report.activation_audit is not None


@pytest.mark.backend_specific(
    backend="sqlite",
    reason="SQLite transactional DDL rolls every provider statement back; MariaDB committed-DDL recovery is a separate native contract",
)
@pytest.mark.parametrize("fail_at", [1, 2, 3, 4])
def test_each_provider_statement_fault_rolls_back_and_rerun_converges(
    tmp_path: Path, fail_at: int
) -> None:
    database = tmp_path / f"fault-{fail_at}.sqlite3"
    connector = FaultAtProviderStatementConnector(str(database), fail_at)
    connector.connect()
    provider = FakeProvider(_definition())
    try:
        with pytest.raises(RuntimeError, match=f"statement {fail_at}"):
            _run_schema_epoch(connector, provider, clock=lambda: NOW)
        _assert_unpublished(connector)
        report = _run_schema_epoch(connector, provider, clock=lambda: NOW)
    finally:
        connector.close()

    assert report.state == "READY"


def test_existing_same_name_wrong_shape_fails_slice_validation(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition)
    with fixture_transaction(connector):
        connector.execute(
            "CREATE TABLE vnext_epoch_parents (parent_id INTEGER PRIMARY KEY)"
        )
    try:
        with pytest.raises(AssertionError):
            _run_schema_epoch(connector, FakeProvider(definition))
        assert _table_columns(connector, PARENT.name) == (
            ("parent_id", "INTEGER", int(connector_backend(connector) == "mariadb"), 1),
        )
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


@pytest.mark.parametrize("field", ["epoch", "schema_version", "manifest_sha256"])
def test_building_identity_drift_is_rejected(
    database_factory: DatabaseFactory, field: str
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition)
    updates: dict[str, object] = {
        "epoch": 4,
        "schema_version": 1,
        "manifest_sha256": b"x" * 32,
    }
    with fixture_transaction(connector):
        connector.execute(
            f"UPDATE h2hdb_schema_epoch SET {field} = %s WHERE singleton_id = 1",
            (updates[field],),
        )
    try:
        with pytest.raises(SchemaEpochDriftError):
            _run_schema_epoch(connector, FakeProvider(definition))
    finally:
        connector.close()


@pytest.mark.parametrize(
    "provider",
    [
        FakeProvider(_definition(ddl_manifest_sha256="33" * 32)),
        FakeProvider(
            SchemaEpochDefinition(
                epoch=3,
                schema_version=9,
                ddl_manifest_sha256=DDL_MANIFEST,
                seed_manifest_sha256="44" * 32,
                obligation_manifest_sha256=OBLIGATION_MANIFEST,
                expected_objects=frozenset({PARENT, CHILD, CHILD_INDEX}),
                slices=(
                    SchemaSlice("identity", (PARENT_STATEMENT,)),
                    SchemaSlice("membership", (CHILD_STATEMENT, INDEX_STATEMENT)),
                ),
                bootstrap_seeds=(PARENT_SEED,),
                activation_semantic_obligation_ids=(
                    "canonical-digest-integrity",
                    "versioned-leaf-byte-bounds",
                    "singleton-seeds",
                ),
                ready_semantic_obligation_ids=(
                    "canonical-digest-integrity",
                    "versioned-leaf-byte-bounds",
                ),
            )
        ),
        FakeProvider(_definition(obligation_manifest_sha256="44" * 32)),
    ],
)
def test_ready_rejects_ddl_or_obligation_manifest_drift(
    database_factory: DatabaseFactory, provider: FakeProvider
) -> None:
    connector = _connected(database_factory.config())
    try:
        _run_schema_epoch(connector, FakeProvider(_definition()))
        with pytest.raises(SchemaEpochDriftError, match="manifest"):
            _run_schema_epoch(connector, provider)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [("epoch", 4), ("schema_version", 1), ("manifest_sha256", b"z" * 32)],
)
def test_ready_control_identity_drift_is_rejected(
    database_factory: DatabaseFactory, field: str, value: object
) -> None:
    connector = _connected(database_factory.config())
    provider = FakeProvider(_definition())
    try:
        _run_schema_epoch(connector, provider)
        with fixture_transaction(connector):
            connector.execute(
                f"UPDATE h2hdb_schema_epoch SET {field} = %s WHERE singleton_id = 1",
                (value,),
            )
        with pytest.raises(SchemaEpochDriftError):
            _run_schema_epoch(connector, provider)
    finally:
        connector.close()


def test_ready_missing_or_extra_object_is_rejected(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    provider = FakeProvider(_definition())
    try:
        _run_schema_epoch(connector, provider)
        with fixture_transaction(connector):
            connector.execute(
                "DROP INDEX vnext_epoch_children_parent_idx"
                if connector_backend(connector) == "sqlite"
                else "DROP TABLE vnext_epoch_children"
            )
        assert _run_schema_epoch(connector, provider).activation_audit is None
        with (
            connector.read_transaction(),
            pytest.raises(SchemaEpochValidationError, match="missing"),
        ):
            _validate_schema_epoch(connector, provider)

        if connector_backend(connector) == "sqlite":
            with fixture_transaction(connector):
                connector.execute(INDEX_STATEMENT.sql)
            with fixture_transaction(connector):
                connector.execute(
                    "CREATE INDEX unexpected_idx ON vnext_epoch_parents (payload)"
                )
        else:
            with fixture_transaction(connector):
                connector.execute(
                    _native_definition(provider.definition, "mariadb")
                    .slices[1]
                    .statements[0]
                    .sql
                )
            with fixture_transaction(connector):
                connector.execute("CREATE TABLE unexpected_table (value INTEGER)")
        assert _run_schema_epoch(connector, provider).activation_audit is None
        with (
            connector.read_transaction(),
            pytest.raises(SchemaEpochAdmissionError, match="outside"),
        ):
            _validate_schema_epoch(connector, provider)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("semantic_result", "match"),
    [
        (("canonical-digest-integrity",), "reported obligation IDs"),
        (
            (
                "canonical-digest-integrity",
                "versioned-leaf-byte-bounds",
                "singleton-seeds",
                "forged-obligation",
            ),
            "reported obligation IDs",
        ),
    ],
)
def test_semantic_validator_must_report_exact_ordered_obligation_manifest(
    database_factory: DatabaseFactory, semantic_result: tuple[str, ...], match: str
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition)
    try:
        with pytest.raises(SchemaEpochValidationError, match=match):
            _run_schema_epoch(
                connector,
                FakeProvider(definition, semantic_result=semantic_result),
            )
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_bootstrap_validator_must_report_exact_ordered_seed_manifest(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition)
    try:
        with pytest.raises(SchemaEpochValidationError, match="reported seed IDs"):
            _run_schema_epoch(
                connector,
                FakeProvider(definition, seed_result=("forged-seed",)),
            )
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_conflicting_bootstrap_row_is_never_adopted(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    with fixture_transaction(connector):
        connector.execute(
            "INSERT INTO vnext_epoch_parents "
            "(parent_id, payload, payload_version) VALUES (0, %s, 1)",
            (b"x" * 32,),
        )
    try:
        with pytest.raises(SchemaEpochValidationError, match="differs"):
            _run_schema_epoch(connector, FakeProvider(definition))
        assert inspect_one(
            connector, "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        ) == (b"x" * 32,)
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_committed_bootstrap_seed_replays_exactly_once(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    seed = _native_definition(definition, connector_backend(connector)).bootstrap_seeds[
        0
    ]
    with fixture_transaction(connector):
        connector.execute(seed.sql, seed.parameters)
    try:
        report = _run_schema_epoch(connector, FakeProvider(definition))
        rows = inspect_all(
            connector,
            "SELECT parent_id, payload, payload_version FROM vnext_epoch_parents",
        )
    finally:
        connector.close()

    assert report.outcome in {
        SchemaProvisioningOutcome.CREATED,
        SchemaProvisioningOutcome.RESUMED,
    }
    assert report.activation_audit is not None
    assert report.activation_audit.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert rows == [(0, b"\x00" * 32, 1)]


def test_ready_validation_does_not_require_mutable_seed_row_to_stay_at_genesis(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    provider = FakeProvider(_definition())
    try:
        _run_schema_epoch(connector, provider)
        with fixture_transaction(connector):
            connector.execute(
                "UPDATE vnext_epoch_parents SET payload = %s WHERE parent_id = 0",
                (b"m" * 32,),
            )
        with connector.read_transaction():
            report = _validate_schema_epoch(connector, provider)
        current = inspect_one(
            connector, "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        )
    finally:
        connector.close()

    assert not report.transitioned_to_ready
    assert report.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert current == (b"m" * 32,)


def test_explicit_ready_audit_uses_only_recurring_semantic_obligations(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    provider = FakeProvider(_definition())
    try:
        first = _run_schema_epoch(connector, provider)
        assert _run_schema_epoch(connector, provider).activation_audit is None
        with connector.read_transaction():
            second = _validate_schema_epoch(connector, provider)
    finally:
        connector.close()

    assert first.activation_audit is not None
    assert first.activation_audit.semantic_obligation_ids == (
        "canonical-digest-integrity",
        "versioned-leaf-byte-bounds",
        "singleton-seeds",
    )
    assert second.semantic_obligation_ids == (
        "canonical-digest-integrity",
        "versioned-leaf-byte-bounds",
    )
    assert provider.semantic_phases == [
        SchemaSemanticValidationPhase.ACTIVATION,
        SchemaSemanticValidationPhase.READY,
    ]


def test_semantic_validator_cannot_mutate_bootstrap_rows(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    seed = _native_definition(definition, connector_backend(connector)).bootstrap_seeds[
        0
    ]
    with fixture_transaction(connector):
        connector.execute(seed.sql, seed.parameters)

    class MutatingProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
            with fixture_transaction(connector):
                connector.execute(
                    "UPDATE vnext_epoch_parents SET payload = %s WHERE parent_id = 0",
                    (b"z" * 32,),
                )
            return (
                self.definition.activation_semantic_obligation_ids
                if phase is SchemaSemanticValidationPhase.ACTIVATION
                else self.definition.ready_semantic_obligation_ids
            )

    try:
        with pytest.raises(SchemaEpochValidationError, match="read-only"):
            _run_schema_epoch(connector, MutatingProvider(definition))
        assert inspect_one(
            connector, "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        ) == (b"\x00" * 32,)
    finally:
        connector.close()


@pytest.mark.parametrize("validator", ["slice", "global", "semantic"])
def test_validator_failure_never_publishes_ready(
    database_factory: DatabaseFactory, validator: str
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition)
    error = RuntimeError(f"{validator} validator failed")
    provider = FakeProvider(
        definition,
        slice_error=error if validator == "slice" else None,
        global_error=error if validator == "global" else None,
        semantic_error=error if validator == "semantic" else None,
    )
    try:
        with pytest.raises(RuntimeError, match=f"{validator} validator failed"):
            _run_schema_epoch(connector, provider)
        _assert_no_open_transaction(connector)
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
        assert _run_schema_epoch(connector, FakeProvider(definition)).state == "READY"
        _assert_no_open_transaction(connector)
    finally:
        connector.close()


def test_semantic_validator_cannot_create_schema_objects(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()

    class SideEffectProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
            with fixture_transaction(connector):
                connector.execute("CREATE TABLE validator_side_effect (value INTEGER)")
            return (
                self.definition.activation_semantic_obligation_ids
                if phase is SchemaSemanticValidationPhase.ACTIVATION
                else self.definition.ready_semantic_obligation_ids
            )

    try:
        with pytest.raises(SchemaEpochValidationError, match="read-only"):
            _run_schema_epoch(connector, SideEffectProvider(definition))
        _assert_unpublished(connector)
    finally:
        connector.close()


def test_semantic_validator_accepts_one_read_only_cte(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()

    class ReadOnlyCTEProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
            assert connector.fetch_one(
                "WITH family_keys(parent_id) AS ("
                "SELECT parent_id FROM vnext_epoch_parents WHERE parent_id = %s) "
                "SELECT parent.payload FROM family_keys AS family "
                "JOIN vnext_epoch_parents AS parent "
                "ON parent.parent_id = family.parent_id",
                (0,),
            ) == (b"\x00" * 32,)
            return (
                self.definition.activation_semantic_obligation_ids
                if phase is SchemaSemanticValidationPhase.ACTIVATION
                else self.definition.ready_semantic_obligation_ids
            )

    try:
        report = _run_schema_epoch(connector, ReadOnlyCTEProvider(definition))
        assert report.state == "READY"
    finally:
        connector.close()


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM vnext_epoch_parents WHERE parent_id = 0 RETURNING parent_id",
        "PRAGMA foreign_keys = OFF",
        "SELECT GET_LOCK('validator-side-effect', 0)",
        "SELECT 1; DELETE FROM vnext_epoch_parents",
        (
            "WITH changed AS (UPDATE vnext_epoch_parents SET payload = X'00' "
            "WHERE parent_id = 0 RETURNING parent_id) "
            "SELECT parent_id FROM changed"
        ),
        (
            "WITH selected AS (SELECT parent_id FROM vnext_epoch_parents) "
            "UPDATE vnext_epoch_parents SET payload = X'00'"
        ),
    ],
)
def test_semantic_validator_fetch_cannot_smuggle_side_effects(
    database_factory: DatabaseFactory,
    query: str,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()

    class SideEffectProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
            connector.fetch_one(query)
            return (
                self.definition.activation_semantic_obligation_ids
                if phase is SchemaSemanticValidationPhase.ACTIVATION
                else self.definition.ready_semantic_obligation_ids
            )

    try:
        with pytest.raises(SchemaEpochValidationError, match="read-only"):
            _run_schema_epoch(connector, SideEffectProvider(definition))
        _assert_unpublished(connector)
    finally:
        connector.close()


def test_failed_compare_and_set_is_detected(database_factory: DatabaseFactory) -> None:
    database = database_factory.config()
    connector = _connected(database)
    original_execute = connector.execute

    def execute(query: str, data: tuple[Any, ...] = ()) -> None:
        if (
            query.lstrip().upper().startswith("UPDATE H2HDB_SCHEMA_EPOCH")
            and "SET state = 'READY'" in query
        ):
            return
        original_execute(query, data)

    connector.execute = execute  # type: ignore[method-assign] # Inject exact native CAS response loss.
    try:
        with pytest.raises(SchemaEpochValidationError, match="compare-and-set"):
            _run_schema_epoch(connector, FakeProvider(_definition()))
        _assert_unpublished(connector)
    finally:
        connector.close()


def test_two_sqlite_runners_serialize_and_second_revalidates_ready(
    database_factory: DatabaseFactory,
) -> None:
    database = database_factory.config()
    first_inside_slice = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()
    reports: dict[str, object] = {}
    errors: list[BaseException] = []

    def blocking_hook(schema_slice: SchemaSlice) -> None:
        if schema_slice.slice_id == "identity":
            first_inside_slice.set()
            if not release_first.wait(timeout=5):
                raise AssertionError("test did not release first runner")

    def run_first() -> None:
        connector = _connected(database)
        try:
            reports["first"] = _run_schema_epoch(
                connector,
                FakeProvider(_definition(), slice_hook=blocking_hook),
                clock=lambda: NOW,
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)
        finally:
            connector.close()

    def run_second() -> None:
        connector = _connected(database)
        try:
            reports["second"] = _run_schema_epoch(
                connector, FakeProvider(_definition()), clock=lambda: NOW
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)
        finally:
            connector.close()
            second_finished.set()

    first_thread = threading.Thread(target=run_first)
    second_thread = threading.Thread(target=run_second)
    first_thread.start()
    assert first_inside_slice.wait(timeout=5)
    second_thread.start()
    time.sleep(0.1)
    assert not second_finished.is_set()
    release_first.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    first_report = reports["first"]
    second_report = reports["second"]
    assert getattr(first_report, "outcome") is SchemaProvisioningOutcome.CREATED
    assert getattr(second_report, "outcome") is SchemaProvisioningOutcome.ALREADY_READY
    assert getattr(second_report, "activation_audit") is None


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE vnext_epoch_parents",
        "ALTER TABLE vnext_epoch_parents ADD COLUMN value INTEGER",
        "INSERT INTO vnext_epoch_parents VALUES (1, X'00', 1)",
        "CREATE TABLE vnext_epoch_parents (parent_id INTEGER)",
    ],
)
def test_provider_statements_must_be_idempotent_create_prefix(sql: str) -> None:
    with pytest.raises(ValueError, match="CREATE ... IF NOT EXISTS"):
        SchemaCreateStatement("unsafe", sql, PARENT)


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM vnext_epoch_parents",
        "INSERT INTO vnext_epoch_parents (parent_id) VALUES (%s)",
        "INSERT INTO vnext_epoch_parents (parent_id) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE payload = X'00'",
        "INSERT INTO another_table (parent_id) VALUES (%s) "
        "ON CONFLICT(parent_id) DO NOTHING",
        "INSERT INTO vnext_epoch_parents (parent_id) VALUES (%s); "
        "DROP TABLE protected_data",
    ],
)
def test_seed_statement_rejects_destructive_or_non_noop_conflicts(sql: str) -> None:
    with pytest.raises(ValueError, match="idempotent INSERT"):
        SchemaSeedStatement("unsafe-seed", PARENT.name, sql, (0,))


def test_provider_object_whitelist_must_exactly_match_statements() -> None:
    with pytest.raises(ValueError, match="exactly match"):
        SchemaEpochDefinition(
            epoch=3,
            schema_version=9,
            ddl_manifest_sha256=DDL_MANIFEST,
            seed_manifest_sha256=SEED_MANIFEST,
            obligation_manifest_sha256=OBLIGATION_MANIFEST,
            expected_objects=frozenset({PARENT, CHILD}),
            slices=(SchemaSlice("identity", (PARENT_STATEMENT,)),),
            bootstrap_seeds=(PARENT_SEED,),
            activation_semantic_obligation_ids=("singleton-seeds",),
            ready_semantic_obligation_ids=("singleton-seeds",),
        )


def test_provider_cannot_own_control_table() -> None:
    control_object = SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE)
    control_statement = SchemaCreateStatement(
        "control",
        "CREATE TABLE IF NOT EXISTS h2hdb_schema_epoch (value INTEGER)",
        control_object,
    )
    with pytest.raises(ValueError, match="must not declare"):
        SchemaEpochDefinition(
            epoch=3,
            schema_version=9,
            ddl_manifest_sha256=DDL_MANIFEST,
            seed_manifest_sha256=SEED_MANIFEST,
            obligation_manifest_sha256=OBLIGATION_MANIFEST,
            expected_objects=frozenset({control_object}),
            slices=(SchemaSlice("control", (control_statement,)),),
            bootstrap_seeds=(PARENT_SEED,),
            activation_semantic_obligation_ids=("singleton-seeds",),
            ready_semantic_obligation_ids=("singleton-seeds",),
        )


def test_epoch_requires_at_least_one_semantic_obligation() -> None:
    with pytest.raises(
        ValueError, match="must declare activation semantic obligations"
    ):
        SchemaEpochDefinition(
            epoch=3,
            schema_version=9,
            ddl_manifest_sha256=DDL_MANIFEST,
            seed_manifest_sha256=SEED_MANIFEST,
            obligation_manifest_sha256=OBLIGATION_MANIFEST,
            expected_objects=frozenset({PARENT}),
            slices=(SchemaSlice("identity", (PARENT_STATEMENT,)),),
            bootstrap_seeds=(PARENT_SEED,),
            activation_semantic_obligation_ids=(),
            ready_semantic_obligation_ids=("ready",),
        )


@pytest.mark.parametrize("injection_phase", ["slice", "global"])
def test_build_final_inventories_reject_new_objects_and_retry_is_fresh(
    database_factory: DatabaseFactory, injection_phase: str
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=1)

    class InjectingProvider(FakeProvider):
        def validate_slice(
            self, connector: SQLConnector, schema_slice: SchemaSlice
        ) -> None:
            super().validate_slice(connector, schema_slice)
            if injection_phase == "slice" and schema_slice.slice_id == "identity":
                with fixture_transaction(connector):
                    connector.execute(
                        "CREATE TABLE unexpected_during_build (value INT)"
                    )

        def validate_global(self, connector: SQLConnector) -> None:
            super().validate_global(connector)
            if injection_phase == "global":
                with fixture_transaction(connector):
                    connector.execute(
                        "CREATE TABLE unexpected_during_build (value INT)"
                    )

    try:
        with pytest.raises(
            (SchemaEpochAdmissionError, SchemaEpochValidationError),
            match="outside|closed-world",
        ):
            _run_schema_epoch(connector, InjectingProvider(definition))
        _assert_no_open_transaction(connector)
        assert inspect_one(connector, "SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
        retained = connector.check_table_exists("unexpected_during_build")
        assert retained is (connector_backend(connector) == "mariadb")
        if retained:
            # MariaDB DDL commits independently; remove exactly the injected
            # foreign object before demonstrating a fresh inventory on retry.
            with fixture_transaction(connector):
                connector.execute("DROP TABLE unexpected_during_build")
        report = _run_schema_epoch(connector, FakeProvider(definition))
        assert report.outcome is SchemaProvisioningOutcome.RESUMED
        assert report.activation_audit is not None
        _assert_no_open_transaction(connector)
    finally:
        connector.close()


def test_failed_native_seed_dml_leaves_no_transaction_and_durable_prefix_resumes(
    database_factory: DatabaseFactory,
) -> None:
    connector = _connected(database_factory.config())
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=1)
    bad_seed = replace(PARENT_SEED, parameters=(0, None, 1))
    try:
        with pytest.raises(DatabaseDuplicateKeyError):
            _run_schema_epoch(
                connector,
                FakeProvider(replace(definition, bootstrap_seeds=(bad_seed,))),
            )
        _assert_no_open_transaction(connector)
        assert connector.check_table_exists(PARENT.name)
        # The legal parent DDL was committed before the failing attempt on both
        # engines; retry must preserve it and finish the exact same manifest.
        report = _run_schema_epoch(connector, FakeProvider(definition))
        assert report.outcome is SchemaProvisioningOutcome.RESUMED
        _assert_no_open_transaction(connector)
    finally:
        connector.close()


def test_ready_epoch_fully_checks_through_read_only_config(
    database_factory: DatabaseFactory,
) -> None:
    config = database_factory.config()
    provider = FakeProvider(_definition())
    connector = _connected(config)
    try:
        _run_schema_epoch(connector, provider, clock=lambda: NOW)
        _assert_no_open_transaction(connector)
    finally:
        connector.close()
    readonly = config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )
    connector = _connected(readonly)
    try:
        with connector.read_transaction():
            report = _validate_schema_epoch(connector, provider)
        _assert_no_open_transaction(connector)
    finally:
        connector.close()
    assert report.state == "READY"
    assert report.resumed_build
    assert not report.transitioned_to_ready
