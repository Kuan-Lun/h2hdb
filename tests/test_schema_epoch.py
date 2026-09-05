from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from h2hdb.schema_epoch import (
    SCHEMA_EPOCH_CONTROL_TABLE,
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
    run_sqlite_schema_epoch,
)
from h2hdb.sql_connector import SQLConnector
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
        schema_version=3,
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


@pytest.mark.parametrize("version", [1, 2])
def test_prior_schema_definition_is_rejected_without_compatibility_path(
    version: int,
) -> None:
    with pytest.raises(ValueError, match=f"supports schema version 3, not {version}"):
        replace(_definition(), schema_version=version)


@dataclass
class FakeProvider:
    definition: SchemaEpochDefinition
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
        if schema_slice.slice_id == "identity":
            assert _table_columns(connector, PARENT.name) == (
                ("parent_id", "INTEGER", 1, 1),
                ("payload", "BLOB", 1, 0),
                ("payload_version", "INTEGER", 1, 0),
            )
        elif schema_slice.slice_id == "membership":
            assert _table_columns(connector, CHILD.name) == (
                ("child_id", "INTEGER", 1, 1),
                ("parent_id", "INTEGER", 1, 0),
                ("digest", "BLOB", 1, 0),
            )
            indexes = connector.fetch_all("PRAGMA index_list(vnext_epoch_children)")
            assert CHILD_INDEX.name in {str(row[1]) for row in indexes}
        else:  # pragma: no cover - protects future fake-provider edits
            raise AssertionError(f"Unknown test slice: {schema_slice.slice_id}")

    def validate_global(self, connector: SQLConnector) -> None:
        if self.global_error is not None:
            raise self.global_error
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        assert connector.fetch_one("SELECT COUNT(*) FROM h2hdb_schema_epoch") == (1,)
        assert connector.fetch_one(
            "SELECT typeof(manifest_sha256), length(manifest_sha256) "
            "FROM h2hdb_schema_epoch WHERE singleton_id = 1"
        ) == ("blob", 32)

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
        ) == (1, 3, 3)
        expected = (
            self.definition.activation_semantic_obligation_ids
            if phase is SchemaSemanticValidationPhase.ACTIVATION
            else self.definition.ready_semantic_obligation_ids
        )
        return expected if self.semantic_result is None else self.semantic_result


def _table_columns(
    connector: SQLConnector, table_name: str
) -> tuple[tuple[str, str, int, int], ...]:
    return tuple(
        (str(name), str(column_type), int(not_null), int(primary_key))
        for _, name, column_type, not_null, _, primary_key in connector.fetch_all(
            f"PRAGMA table_info({table_name})"
        )
    )


def _connected(database: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(database))
    connector.connect()
    return connector


def _initialize_building(
    connector: SQLiteConnector,
    definition: SchemaEpochDefinition,
    *,
    completed_statement_count: int = 0,
) -> None:
    statements = [
        statement
        for schema_slice in definition.slices
        for statement in schema_slice.statements
    ]
    with connector.transaction():
        SQLiteSchemaEpochCatalog().create_control_table(connector)
        connector.execute(
            """
            INSERT INTO h2hdb_schema_epoch (
                singleton_id, epoch, schema_version, state,
                manifest_sha256, started_at, ready_at
            ) VALUES (1, %s, %s, 'BUILDING', %s, 1, NULL)
            """,
            (
                definition.epoch,
                definition.schema_version,
                bytes.fromhex(definition.manifest_sha256),
            ),
        )
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


class NoOpReadyCASConnector(SQLiteConnector):
    def execute(self, query: str, data: tuple[object, ...] = ()) -> None:
        if (
            query.lstrip().upper().startswith("UPDATE H2HDB_SCHEMA_EPOCH")
            and "SET state = 'READY'" in query
        ):
            return
        super().execute(query, data)


def test_empty_database_builds_ready_and_ready_rerun_only_validates(
    tmp_path: Path,
) -> None:
    database = tmp_path / "epoch.sqlite3"
    connector = _connected(database)
    provider = FakeProvider(_definition())
    try:
        first = run_sqlite_schema_epoch(connector, provider, clock=lambda: NOW)
        second = run_sqlite_schema_epoch(connector, provider, clock=lambda: NOW)
    finally:
        connector.close()

    assert first.state == "READY"
    assert first.resumed_build is False
    assert first.transitioned_to_ready is True
    assert first.semantic_obligation_ids == provider.definition.semantic_obligation_ids
    assert second.state == "READY"
    assert second.resumed_build is True
    assert second.transitioned_to_ready is False


@pytest.mark.parametrize(
    "legacy_ddl",
    [
        "CREATE TABLE h2hdb_schema_migrations (version INTEGER PRIMARY KEY)",
        "CREATE TABLE unrelated (value TEXT)",
        "CREATE VIEW unrelated AS SELECT 1 AS value",
    ],
)
def test_nonempty_database_is_rejected_without_drop_or_adoption(
    tmp_path: Path, legacy_ddl: str
) -> None:
    database = tmp_path / "nonempty.sqlite3"
    connector = _connected(database)
    connector.execute(legacy_ddl)
    try:
        with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
            run_sqlite_schema_epoch(connector, FakeProvider(_definition()))
        objects = SQLiteSchemaEpochCatalog().list_objects(connector)
    finally:
        connector.close()

    assert (
        SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE) not in objects
    )
    assert objects


def test_control_table_with_wrong_shape_is_rejected(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "wrong-control.sqlite3")
    connector.execute(
        "CREATE TABLE h2hdb_schema_epoch (singleton_id INTEGER PRIMARY KEY)"
    )
    try:
        with pytest.raises(SchemaEpochValidationError, match="wrong shape"):
            run_sqlite_schema_epoch(connector, FakeProvider(_definition()))
        columns = connector.fetch_all("PRAGMA table_info(h2hdb_schema_epoch)")
    finally:
        connector.close()

    assert len(columns) == 1


@pytest.mark.parametrize("completed_statement_count", [0, 1, 2, 3])
def test_committed_partial_build_resumes_from_slice_one(
    tmp_path: Path, completed_statement_count: int
) -> None:
    connector = _connected(tmp_path / f"partial-{completed_statement_count}.sqlite3")
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
        report = run_sqlite_schema_epoch(connector, provider, clock=lambda: NOW)
    finally:
        connector.close()

    assert visited_slices == ["identity", "membership"]
    assert report.resumed_build is True
    assert report.transitioned_to_ready is True


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
            run_sqlite_schema_epoch(connector, provider, clock=lambda: NOW)
        assert SQLiteSchemaEpochCatalog().list_objects(connector) == frozenset()
        report = run_sqlite_schema_epoch(connector, provider, clock=lambda: NOW)
    finally:
        connector.close()

    assert report.state == "READY"


def test_existing_same_name_wrong_shape_fails_slice_validation(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "wrong-provider-shape.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition)
    connector.execute("CREATE TABLE vnext_epoch_parents (parent_id TEXT PRIMARY KEY)")
    try:
        with pytest.raises(AssertionError):
            run_sqlite_schema_epoch(connector, FakeProvider(definition))
        assert _table_columns(connector, PARENT.name) == (("parent_id", "TEXT", 0, 1),)
        assert connector.fetch_one("SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


@pytest.mark.parametrize("field", ["epoch", "schema_version", "manifest_sha256"])
def test_building_identity_drift_is_rejected(tmp_path: Path, field: str) -> None:
    connector = _connected(tmp_path / f"building-drift-{field}.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition)
    updates: dict[str, object] = {
        "epoch": 4,
        "schema_version": 1,
        "manifest_sha256": b"x" * 32,
    }
    connector.execute(
        f"UPDATE h2hdb_schema_epoch SET {field} = %s WHERE singleton_id = 1",
        (updates[field],),
    )
    try:
        with pytest.raises(SchemaEpochDriftError):
            run_sqlite_schema_epoch(connector, FakeProvider(definition))
    finally:
        connector.close()


@pytest.mark.parametrize(
    "provider",
    [
        FakeProvider(_definition(ddl_manifest_sha256="33" * 32)),
        FakeProvider(
            SchemaEpochDefinition(
                epoch=3,
                schema_version=3,
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
    tmp_path: Path, provider: FakeProvider
) -> None:
    connector = _connected(tmp_path / "ready-manifest-drift.sqlite3")
    try:
        run_sqlite_schema_epoch(connector, FakeProvider(_definition()))
        with pytest.raises(SchemaEpochDriftError, match="manifest"):
            run_sqlite_schema_epoch(connector, provider)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [("epoch", 4), ("schema_version", 1), ("manifest_sha256", b"z" * 32)],
)
def test_ready_control_identity_drift_is_rejected(
    tmp_path: Path, field: str, value: object
) -> None:
    connector = _connected(tmp_path / f"ready-control-drift-{field}.sqlite3")
    provider = FakeProvider(_definition())
    try:
        run_sqlite_schema_epoch(connector, provider)
        connector.execute(
            f"UPDATE h2hdb_schema_epoch SET {field} = %s WHERE singleton_id = 1",
            (value,),
        )
        with pytest.raises(SchemaEpochDriftError):
            run_sqlite_schema_epoch(connector, provider)
    finally:
        connector.close()


def test_ready_missing_or_extra_object_is_rejected(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "ready-object-drift.sqlite3")
    provider = FakeProvider(_definition())
    try:
        run_sqlite_schema_epoch(connector, provider)
        connector.execute("DROP INDEX vnext_epoch_children_parent_idx")
        with pytest.raises(SchemaEpochValidationError, match="missing"):
            run_sqlite_schema_epoch(connector, provider)

        connector.execute(INDEX_STATEMENT.sql)
        connector.execute(
            "CREATE INDEX unexpected_idx ON vnext_epoch_parents (payload)"
        )
        with pytest.raises(SchemaEpochAdmissionError, match="outside"):
            run_sqlite_schema_epoch(connector, provider)
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
    tmp_path: Path, semantic_result: tuple[str, ...], match: str
) -> None:
    connector = _connected(tmp_path / "semantic-id-drift.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition)
    try:
        with pytest.raises(SchemaEpochValidationError, match=match):
            run_sqlite_schema_epoch(
                connector,
                FakeProvider(definition, semantic_result=semantic_result),
            )
        assert connector.fetch_one("SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_bootstrap_validator_must_report_exact_ordered_seed_manifest(
    tmp_path: Path,
) -> None:
    connector = _connected(tmp_path / "seed-id-drift.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition)
    try:
        with pytest.raises(SchemaEpochValidationError, match="reported seed IDs"):
            run_sqlite_schema_epoch(
                connector,
                FakeProvider(definition, seed_result=("forged-seed",)),
            )
        assert connector.fetch_one("SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_conflicting_bootstrap_row_is_never_adopted(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "seed-collision.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    connector.execute(
        "INSERT INTO vnext_epoch_parents "
        "(parent_id, payload, payload_version) VALUES (0, %s, 1)",
        (b"x" * 32,),
    )
    try:
        with pytest.raises(SchemaEpochValidationError, match="differs"):
            run_sqlite_schema_epoch(connector, FakeProvider(definition))
        assert connector.fetch_one(
            "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        ) == (b"x" * 32,)
        assert connector.fetch_one("SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_committed_bootstrap_seed_replays_exactly_once(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "seed-response-loss.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    connector.execute(PARENT_SEED.sql, PARENT_SEED.parameters)
    try:
        report = run_sqlite_schema_epoch(connector, FakeProvider(definition))
        rows = connector.fetch_all(
            "SELECT parent_id, payload, payload_version FROM vnext_epoch_parents"
        )
    finally:
        connector.close()

    assert report.transitioned_to_ready is True
    assert report.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert rows == [(0, b"\x00" * 32, 1)]


def test_ready_validation_does_not_require_mutable_seed_row_to_stay_at_genesis(
    tmp_path: Path,
) -> None:
    connector = _connected(tmp_path / "mutable-seed-after-ready.sqlite3")
    provider = FakeProvider(_definition())
    try:
        run_sqlite_schema_epoch(connector, provider)
        connector.execute(
            "UPDATE vnext_epoch_parents SET payload = %s WHERE parent_id = 0",
            (b"m" * 32,),
        )
        report = run_sqlite_schema_epoch(connector, provider)
        current = connector.fetch_one(
            "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        )
    finally:
        connector.close()

    assert report.transitioned_to_ready is False
    assert report.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert current == (b"m" * 32,)


def test_ready_rerun_uses_only_recurring_semantic_obligations(
    tmp_path: Path,
) -> None:
    connector = _connected(tmp_path / "semantic-validation-lifecycle.sqlite3")
    provider = FakeProvider(_definition())
    try:
        first = run_sqlite_schema_epoch(connector, provider)
        second = run_sqlite_schema_epoch(connector, provider)
    finally:
        connector.close()

    assert first.semantic_obligation_ids == (
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


def test_semantic_validator_cannot_mutate_bootstrap_rows(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "seed-validator-mutation.sqlite3")
    definition = _definition()
    _initialize_building(connector, definition, completed_statement_count=3)
    connector.execute(PARENT_SEED.sql, PARENT_SEED.parameters)

    class MutatingProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
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
            run_sqlite_schema_epoch(connector, MutatingProvider(definition))
        assert connector.fetch_one(
            "SELECT payload FROM vnext_epoch_parents WHERE parent_id = 0"
        ) == (b"\x00" * 32,)
    finally:
        connector.close()


@pytest.mark.parametrize("validator", ["slice", "global", "semantic"])
def test_validator_failure_never_publishes_ready(
    tmp_path: Path, validator: str
) -> None:
    connector = _connected(tmp_path / f"validator-{validator}.sqlite3")
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
            run_sqlite_schema_epoch(connector, provider)
        assert connector.fetch_one("SELECT state FROM h2hdb_schema_epoch") == (
            "BUILDING",
        )
    finally:
        connector.close()


def test_semantic_validator_cannot_create_schema_objects(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "validator-side-effect.sqlite3")
    definition = _definition()

    class SideEffectProvider(FakeProvider):
        def validate_semantics(
            self,
            connector: SQLConnector,
            phase: SchemaSemanticValidationPhase,
        ) -> Sequence[str]:
            connector.execute("CREATE TABLE validator_side_effect (value INTEGER)")
            return (
                self.definition.activation_semantic_obligation_ids
                if phase is SchemaSemanticValidationPhase.ACTIVATION
                else self.definition.ready_semantic_obligation_ids
            )

    try:
        with pytest.raises(SchemaEpochValidationError, match="read-only"):
            run_sqlite_schema_epoch(connector, SideEffectProvider(definition))
        assert SQLiteSchemaEpochCatalog().list_objects(connector) == frozenset()
    finally:
        connector.close()


def test_semantic_validator_accepts_one_read_only_cte(tmp_path: Path) -> None:
    connector = _connected(tmp_path / "validator-read-only-cte.sqlite3")
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
        report = run_sqlite_schema_epoch(connector, ReadOnlyCTEProvider(definition))
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
    tmp_path: Path,
    query: str,
) -> None:
    connector = _connected(tmp_path / "validator-fetch-side-effect.sqlite3")
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
            run_sqlite_schema_epoch(connector, SideEffectProvider(definition))
        assert SQLiteSchemaEpochCatalog().list_objects(connector) == frozenset()
    finally:
        connector.close()


def test_failed_compare_and_set_is_detected(tmp_path: Path) -> None:
    database = tmp_path / "cas.sqlite3"
    connector = NoOpReadyCASConnector(str(database))
    connector.connect()
    try:
        with pytest.raises(SchemaEpochValidationError, match="compare-and-set"):
            run_sqlite_schema_epoch(connector, FakeProvider(_definition()))
        assert SQLiteSchemaEpochCatalog().list_objects(connector) == frozenset()
    finally:
        connector.close()


def test_two_sqlite_runners_serialize_and_second_revalidates_ready(
    tmp_path: Path,
) -> None:
    database = tmp_path / "concurrent.sqlite3"
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
            reports["first"] = run_sqlite_schema_epoch(
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
            reports["second"] = run_sqlite_schema_epoch(
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
    assert getattr(first_report, "transitioned_to_ready") is True
    assert getattr(second_report, "transitioned_to_ready") is False
    assert getattr(second_report, "resumed_build") is True


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
            schema_version=3,
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
            schema_version=3,
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
            schema_version=3,
            ddl_manifest_sha256=DDL_MANIFEST,
            seed_manifest_sha256=SEED_MANIFEST,
            obligation_manifest_sha256=OBLIGATION_MANIFEST,
            expected_objects=frozenset({PARENT}),
            slices=(SchemaSlice("identity", (PARENT_STATEMENT,)),),
            bootstrap_seeds=(PARENT_SEED,),
            activation_semantic_obligation_ids=(),
            ready_semantic_obligation_ids=("ready",),
        )
