from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from h2hdb import CoreConfig, DatabaseAccessMode
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    MARIADB_SCHEMA_EPOCH_GATE_NAME,
    SCHEMA_EPOCH_CONTROL_TABLE,
    MariaDBAdvisorySchemaEpochGate,
    MariaDBSchemaEpochCatalog,
    SchemaCreateStatement,
    SchemaEpochAdmissionError,
    SchemaEpochDefinition,
    SchemaEpochGateError,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaObjectKind,
    SchemaSeedStatement,
    SchemaSemanticValidationPhase,
    SchemaSlice,
    mariadb_schema_epoch_gate_name,
    run_mariadb_schema_epoch,
)
from h2hdb.sql_connector import SQLConnector

DDL_MANIFEST = "55" * 32
SEED_MANIFEST = "77" * 32
OBLIGATION_MANIFEST = "66" * 32
NOW = datetime(2026, 8, 12, 13, 14, 15, 161718, tzinfo=UTC)

PARENT = SchemaObject(SchemaObjectKind.TABLE, "vnext_mariadb_parents")
CHILD = SchemaObject(SchemaObjectKind.TABLE, "vnext_mariadb_children")
INLINE_INDEX = "ix_vnext_mariadb_parents_payload"

PARENT_STATEMENT = SchemaCreateStatement(
    "mariadb-parent",
    f"""
    CREATE TABLE IF NOT EXISTS vnext_mariadb_parents (
        parent_id BIGINT UNSIGNED NOT NULL,
        payload BINARY(32) NOT NULL,
        PRIMARY KEY (parent_id),
        KEY {INLINE_INDEX} (payload)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
      COLLATE=utf8mb4_nopad_bin
    """,
    PARENT,
)
CHILD_STATEMENT = SchemaCreateStatement(
    "mariadb-child",
    """
    CREATE TABLE IF NOT EXISTS vnext_mariadb_children (
        child_id BIGINT UNSIGNED NOT NULL,
        parent_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (child_id),
        CONSTRAINT fk_vnext_mariadb_child_parent
            FOREIGN KEY (parent_id)
            REFERENCES vnext_mariadb_parents (parent_id)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
      COLLATE=utf8mb4_nopad_bin
    """,
    CHILD,
)
PARENT_SEED = SchemaSeedStatement(
    "mariadb-genesis-parent",
    PARENT.name,
    """
    INSERT INTO vnext_mariadb_parents (parent_id, payload)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE parent_id = parent_id
    """,
    (0, b"\x00" * 32),
)

CONTROL_CHECK_ROWS = [
    ("ck_schema_epoch_control_epoch_nonneg", "`epoch` >= 0"),
    (
        "ck_schema_epoch_control_manifest_sha256_len",
        "octet_length(`manifest_sha256`) = 32",
    ),
    (
        "ck_schema_epoch_control_ready_at_nonneg",
        "`ready_at` is null or `ready_at` >= 0",
    ),
    ("ck_schema_epoch_control_schema_version_nonneg", "`schema_version` >= 0"),
    ("ck_schema_epoch_control_singleton", "`singleton_id` = 1"),
    ("ck_schema_epoch_control_started_at_nonneg", "`started_at` >= 0"),
    ("ck_schema_epoch_manifest_blob", "octet_length(`manifest_sha256`) = 32"),
    (
        "ck_schema_epoch_state",
        "(`state` = 'BUILDING' and `ready_at` is null or "
        "`state` = 'READY' and `ready_at` is not null and "
        "`ready_at` >= `started_at`)",
    ),
]


def _definition() -> SchemaEpochDefinition:
    return SchemaEpochDefinition(
        epoch=2,
        schema_version=1,
        ddl_manifest_sha256=DDL_MANIFEST,
        seed_manifest_sha256=SEED_MANIFEST,
        obligation_manifest_sha256=OBLIGATION_MANIFEST,
        expected_objects=frozenset({PARENT, CHILD}),
        slices=(
            SchemaSlice("parent", (PARENT_STATEMENT,)),
            SchemaSlice("child", (CHILD_STATEMENT,)),
        ),
        bootstrap_seeds=(PARENT_SEED,),
        activation_semantic_obligation_ids=("mariadb-test-obligation",),
        ready_semantic_obligation_ids=("mariadb-test-obligation",),
    )


@dataclass
class MariaDBTestProvider:
    definition: SchemaEpochDefinition

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None:
        table = PARENT.name if schema_slice.slice_id == "parent" else CHILD.name
        count = connector.fetch_one(f"SELECT COUNT(*) FROM {table}")
        assert len(count) == 1 and int(count[0]) >= 0
        assert (
            connector.fetch_one(
                "SELECT IS_USED_LOCK(%s)",
                (
                    mariadb_schema_epoch_gate_name(
                        str(connector.fetch_one("SELECT DATABASE()")[0])
                    ),
                ),
            )[0]
            is not None
        )

    def validate_global(self, connector: SQLConnector) -> None:
        assert connector.fetch_one("SELECT COUNT(*) FROM h2hdb_schema_epoch") == (1,)

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        assert connector.fetch_one(
            "SELECT parent_id, payload FROM vnext_mariadb_parents "
            "WHERE parent_id = 0"
        ) == (0, b"\x00" * 32)
        return tuple(seed.seed_id for seed in self.definition.bootstrap_seeds)

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        assert connector.fetch_one(
            "SELECT OCTET_LENGTH(manifest_sha256) "
            "FROM h2hdb_schema_epoch WHERE singleton_id = 1"
        ) == (32,)
        return (
            self.definition.activation_semantic_obligation_ids
            if phase is SchemaSemanticValidationPhase.ACTIVATION
            else self.definition.ready_semantic_obligation_ids
        )


class FakeMariaDBConnector(SQLConnector):
    """Minimal INFORMATION_SCHEMA and DDL state fixture for epoch orchestration."""

    def __init__(self) -> None:
        self.objects: dict[str, str] = {}
        self.triggers: set[str] = set()
        self.control_shape = "correct"
        self.control_row: tuple[int, int, str, bytes, int, int | None] | None = None
        self.parent_seed: tuple[int, bytes] | None = None
        self.lock_held = False
        self.get_lock_result: object = 1
        self.release_lock_result: object = 1
        self.query_log: list[str] = []
        self.commit_count = 0

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def check_table_exists(self, table_name: str) -> bool:
        return table_name in self.objects

    def commit(self) -> None:
        self.commit_count += 1

    def begin(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self.query_log.append(query)
        normalized = " ".join(query.split()).upper()
        if normalized.startswith("CREATE TABLE H2HDB_SCHEMA_EPOCH"):
            self.objects[SCHEMA_EPOCH_CONTROL_TABLE] = "BASE TABLE"
            self.commit()
        elif normalized.startswith("INSERT INTO H2HDB_SCHEMA_EPOCH"):
            epoch, version, manifest, started_at = data
            self.control_row = (
                int(epoch),
                int(version),
                "BUILDING",
                bytes(manifest),
                int(started_at),
                None,
            )
            self.commit()
        elif normalized.startswith("INSERT INTO VNEXT_MARIADB_PARENTS"):
            if self.parent_seed is None:
                parent_id, payload = data
                self.parent_seed = (int(parent_id), bytes(payload))
            self.commit()
        elif normalized.startswith("UPDATE H2HDB_SCHEMA_EPOCH"):
            ready_at, epoch, version, manifest = data
            if self.control_row is not None:
                old_epoch, old_version, state, old_manifest, started_at, _ = (
                    self.control_row
                )
                if (
                    state == "BUILDING"
                    and old_epoch == int(epoch)
                    and old_version == int(version)
                    and old_manifest == bytes(manifest)
                ):
                    self.control_row = (
                        old_epoch,
                        old_version,
                        "READY",
                        old_manifest,
                        started_at,
                        int(ready_at),
                    )
            self.commit()
        elif normalized.startswith("CREATE TABLE IF NOT EXISTS"):
            if normalized.startswith(
                f"CREATE TABLE IF NOT EXISTS {CHILD.name.upper()}"
            ):
                self.objects[CHILD.name] = "BASE TABLE"
            elif normalized.startswith(
                f"CREATE TABLE IF NOT EXISTS {PARENT.name.upper()}"
            ):
                self.objects[PARENT.name] = "BASE TABLE"
            else:  # pragma: no cover - protects fixture edits
                raise AssertionError(f"Unexpected provider DDL: {query}")
            self.commit()
        else:  # pragma: no cover - protects fixture edits
            raise AssertionError(f"Unexpected execute query: {query}")

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        for row in data:
            self.execute(query, row)

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        self.query_log.append(query)
        normalized = " ".join(query.split()).upper()
        if normalized.startswith("SELECT GET_LOCK"):
            if type(self.get_lock_result) is int and self.get_lock_result == 1:
                self.lock_held = True
            return (self.get_lock_result,)
        if normalized.startswith("SELECT RELEASE_LOCK"):
            result = self.release_lock_result if self.lock_held else None
            if result == 1:
                self.lock_held = False
            return (result,)
        if normalized == "SELECT DATABASE()":
            return ("fake_epoch_database",)
        if normalized.startswith("SELECT IS_USED_LOCK"):
            return (1234 if self.lock_held else None,)
        if normalized == "SELECT COUNT(*) FROM H2HDB_SCHEMA_EPOCH":
            return (0 if self.control_row is None else 1,)
        if normalized.startswith("SELECT COUNT(*) FROM VNEXT_MARIADB_"):
            table = PARENT.name if "PARENTS" in normalized else CHILD.name
            if table not in self.objects:
                raise AssertionError(f"Missing fake table {table}")
            return (1 if table == PARENT.name and self.parent_seed is not None else 0,)
        if normalized.startswith(
            "SELECT PARENT_ID, PAYLOAD FROM VNEXT_MARIADB_PARENTS"
        ):
            return tuple() if self.parent_seed is None else self.parent_seed
        if normalized.startswith("SELECT OCTET_LENGTH(MANIFEST_SHA256)"):
            assert self.control_row is not None
            return (len(self.control_row[3]),)
        raise AssertionError(f"Unexpected fetch_one query: {query}")

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        self.query_log.append(query)
        normalized = " ".join(query.split()).upper()
        if "FROM INFORMATION_SCHEMA.TRIGGERS" in normalized:
            return [(name,) for name in sorted(self.triggers)]
        if "FROM INFORMATION_SCHEMA.TABLES" in normalized:
            if "TABLE_NAME = %S" in normalized:
                if SCHEMA_EPOCH_CONTROL_TABLE not in self.objects:
                    return []
                if self.control_shape == "wrong":
                    return [("BASE TABLE", "INNODB", "utf8mb4_general_ci")]
                return [("BASE TABLE", "InnoDB", "utf8mb4_nopad_bin")]
            return [
                (name, table_type) for name, table_type in sorted(self.objects.items())
            ]
        if "FROM INFORMATION_SCHEMA.COLUMNS" in normalized:
            if self.control_shape == "wrong":
                return [
                    ("singleton_id", 1, "int(11)", "NO", None, None, None, None, "")
                ]
            return [
                (
                    "singleton_id",
                    1,
                    "smallint(5) unsigned",
                    "NO",
                    None,
                    None,
                    None,
                    None,
                    "",
                ),
                ("epoch", 2, "bigint(20) unsigned", "NO", None, None, None, None, ""),
                (
                    "schema_version",
                    3,
                    "bigint unsigned",
                    "NO",
                    None,
                    None,
                    None,
                    None,
                    "",
                ),
                ("state", 4, "varchar(48)", "NO", "ascii", "ascii_bin", 48, None, ""),
                ("manifest_sha256", 5, "binary(32)", "NO", None, None, 32, None, ""),
                ("started_at", 6, "bigint unsigned", "NO", None, None, None, None, ""),
                ("ready_at", 7, "bigint unsigned", "YES", None, None, None, None, ""),
            ]
        if "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC" in normalized:
            return list(CONTROL_CHECK_ROWS)
        if "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS" in normalized:
            return [
                ("PRIMARY", "PRIMARY KEY"),
                *((name, "CHECK") for name, _expression in CONTROL_CHECK_ROWS),
            ]
        if "FROM INFORMATION_SCHEMA.STATISTICS" in normalized:
            return [("PRIMARY", 0, 1, "singleton_id")]
        if "FROM H2HDB_SCHEMA_EPOCH" in normalized:
            if self.control_row is None:
                return []
            return [self.control_row]
        raise AssertionError(f"Unexpected fetch_all query: {query}")


def _initialize_fake_building(
    connector: FakeMariaDBConnector,
    definition: SchemaEpochDefinition,
) -> None:
    connector.objects[SCHEMA_EPOCH_CONTROL_TABLE] = "BASE TABLE"
    connector.control_row = (
        2,
        1,
        "BUILDING",
        bytes.fromhex(definition.manifest_sha256),
        1,
        None,
    )


def test_fake_mariadb_empty_database_builds_ready_and_releases_gate() -> None:
    connector = FakeMariaDBConnector()
    report = run_mariadb_schema_epoch(
        connector,
        MariaDBTestProvider(_definition()),
        clock=lambda: NOW,
        lock_timeout_seconds=0,
    )

    assert report.state == "READY"
    assert report.resumed_build is False
    assert report.transitioned_to_ready is True
    assert connector.lock_held is False
    assert connector.commit_count == 6


def test_fake_mariadb_committed_partial_ddl_resumes_idempotently() -> None:
    connector = FakeMariaDBConnector()
    definition = _definition()
    _initialize_fake_building(connector, definition)
    connector.objects[PARENT.name] = "BASE TABLE"

    report = run_mariadb_schema_epoch(
        connector,
        MariaDBTestProvider(definition),
        clock=lambda: NOW,
        lock_timeout_seconds=0,
    )

    assert report.resumed_build is True
    assert report.transitioned_to_ready is True
    assert {PARENT.name, CHILD.name} <= set(connector.objects)


def test_fake_mariadb_exact_empty_control_residue_is_resumable() -> None:
    connector = FakeMariaDBConnector()
    connector.objects[SCHEMA_EPOCH_CONTROL_TABLE] = "BASE TABLE"

    report = run_mariadb_schema_epoch(
        connector,
        MariaDBTestProvider(_definition()),
        clock=lambda: NOW,
        lock_timeout_seconds=0,
    )

    assert report.resumed_build is True
    assert report.state == "READY"


def test_fake_mariadb_committed_bootstrap_seed_resumes_idempotently() -> None:
    connector = FakeMariaDBConnector()
    definition = _definition()
    _initialize_fake_building(connector, definition)
    connector.objects[PARENT.name] = "BASE TABLE"
    connector.objects[CHILD.name] = "BASE TABLE"
    connector.execute(PARENT_SEED.sql, PARENT_SEED.parameters)

    report = run_mariadb_schema_epoch(
        connector,
        MariaDBTestProvider(definition),
        clock=lambda: NOW,
        lock_timeout_seconds=0,
    )

    assert report.resumed_build is True
    assert report.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert connector.parent_seed == (0, b"\x00" * 32)


def test_fake_mariadb_nonempty_old_schema_is_rejected_without_adoption() -> None:
    connector = FakeMariaDBConnector()
    connector.objects["h2hdb_schema_migrations"] = "BASE TABLE"

    with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
        run_mariadb_schema_epoch(
            connector,
            MariaDBTestProvider(_definition()),
            lock_timeout_seconds=0,
        )

    assert SCHEMA_EPOCH_CONTROL_TABLE not in connector.objects
    assert connector.lock_held is False


def test_fake_mariadb_wrong_control_shape_is_rejected_and_gate_released() -> None:
    connector = FakeMariaDBConnector()
    connector.objects[SCHEMA_EPOCH_CONTROL_TABLE] = "BASE TABLE"
    connector.control_shape = "wrong"

    with pytest.raises(SchemaEpochValidationError, match="wrong shape"):
        run_mariadb_schema_epoch(
            connector,
            MariaDBTestProvider(_definition()),
            lock_timeout_seconds=0,
        )

    assert connector.lock_held is False


def test_mariadb_catalog_does_not_promote_inline_indexes_to_schema_objects() -> None:
    connector = FakeMariaDBConnector()
    connector.objects[PARENT.name] = "BASE TABLE"

    objects = MariaDBSchemaEpochCatalog().list_objects(connector)

    assert objects == frozenset({PARENT})
    assert not any(
        "INFORMATION_SCHEMA.STATISTICS" in query for query in connector.query_log
    )


def test_mariadb_advisory_gate_releases_after_body_error() -> None:
    connector = FakeMariaDBConnector()

    with pytest.raises(RuntimeError, match="body failed"):
        with MariaDBAdvisorySchemaEpochGate(0).acquire(connector):
            assert connector.lock_held is True
            raise RuntimeError("body failed")

    assert connector.lock_held is False


def test_mariadb_default_gate_name_is_stable_and_database_scoped() -> None:
    first = mariadb_schema_epoch_gate_name("catalog_a")
    second = mariadb_schema_epoch_gate_name("catalog_b")

    assert first == mariadb_schema_epoch_gate_name("catalog_a")
    assert first != second
    assert first.startswith(f"{MARIADB_SCHEMA_EPOCH_GATE_NAME}:")
    assert len(first.encode("utf-8")) <= 64


@pytest.mark.parametrize("result", [0, None, 2, "not-a-number", True])
def test_mariadb_advisory_gate_fails_closed_on_bad_acquisition(
    result: object,
) -> None:
    connector = FakeMariaDBConnector()
    connector.get_lock_result = result

    with pytest.raises(SchemaEpochGateError):
        with MariaDBAdvisorySchemaEpochGate(0).acquire(connector):
            raise AssertionError("unreachable")

    assert connector.lock_held is False


def test_generated_invoker_view_is_one_allowed_create_statement() -> None:
    view = SchemaObject(SchemaObjectKind.VIEW, "vnext_resolved")
    statement = SchemaCreateStatement(
        "resolved-view",
        "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS vnext_resolved "
        "AS SELECT 1 AS value;",
        view,
    )

    assert statement.creates == view


def test_generated_invoker_view_rejects_a_second_statement() -> None:
    view = SchemaObject(SchemaObjectKind.VIEW, "vnext_resolved")
    with pytest.raises(ValueError, match="single CREATE"):
        SchemaCreateStatement(
            "resolved-view",
            "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS vnext_resolved "
            "AS SELECT 1 AS value; DROP TABLE protected_data",
            view,
        )


def _mariadb_connector(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    return MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )


def test_mariadb_epoch_empty_database_reaches_ready_and_releases_lock(
    mariadb_config: CoreConfig,
) -> None:
    provider = MariaDBTestProvider(_definition())
    with _mariadb_connector(mariadb_config) as connector:
        report = run_mariadb_schema_epoch(
            connector,
            provider,
            clock=lambda: NOW,
            lock_timeout_seconds=0,
        )
        objects = MariaDBSchemaEpochCatalog().list_objects(connector)
        free_lock = connector.fetch_one(
            "SELECT IS_FREE_LOCK(%s)",
            (
                mariadb_schema_epoch_gate_name(
                    str(connector.fetch_one("SELECT DATABASE()")[0])
                ),
            ),
        )

    assert report.state == "READY"
    assert report.transitioned_to_ready is True
    assert objects == provider.definition.expected_objects | {
        SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE)
    }
    assert free_lock == (1,)


def test_mariadb_ready_epoch_fully_checks_through_read_only_config(
    mariadb_config: CoreConfig,
) -> None:
    provider = MariaDBTestProvider(_definition())
    with _mariadb_connector(mariadb_config) as connector:
        run_mariadb_schema_epoch(
            connector,
            provider,
            clock=lambda: NOW,
            lock_timeout_seconds=0,
        )
    read_only_config = mariadb_config.model_copy(
        update={
            "database": mariadb_config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )

    report = VNextSchemaAdmin(RepositoryContext.from_config(read_only_config)).check(
        provider
    )

    assert report.state == "READY"
    assert report.resumed_build
    assert not report.transitioned_to_ready


def test_mariadb_epoch_resumes_committed_partial_ddl(
    mariadb_config: CoreConfig,
) -> None:
    definition = _definition()
    with _mariadb_connector(mariadb_config) as connector:
        MariaDBSchemaEpochCatalog().create_control_table(connector)
        connector.execute(
            """
            INSERT INTO h2hdb_schema_epoch (
                singleton_id, epoch, schema_version, state,
                manifest_sha256, started_at, ready_at
            ) VALUES (1, 2, 1, 'BUILDING', %s, 1, NULL)
            """,
            (bytes.fromhex(definition.manifest_sha256),),
        )
        connector.execute(PARENT_STATEMENT.sql)

        report = run_mariadb_schema_epoch(
            connector,
            MariaDBTestProvider(definition),
            clock=lambda: NOW,
            lock_timeout_seconds=0,
        )

        state = connector.fetch_one(
            "SELECT state FROM h2hdb_schema_epoch WHERE singleton_id = 1"
        )
        child_count = connector.fetch_one("SELECT COUNT(*) FROM vnext_mariadb_children")

    assert report.resumed_build is True
    assert report.transitioned_to_ready is True
    assert state == ("READY",)
    assert child_count == (0,)


def test_mariadb_epoch_resumes_after_committed_bootstrap_seed(
    mariadb_config: CoreConfig,
) -> None:
    definition = _definition()
    with _mariadb_connector(mariadb_config) as connector:
        MariaDBSchemaEpochCatalog().create_control_table(connector)
        connector.execute(
            """
            INSERT INTO h2hdb_schema_epoch (
                singleton_id, epoch, schema_version, state,
                manifest_sha256, started_at, ready_at
            ) VALUES (1, 2, 1, 'BUILDING', %s, 1, NULL)
            """,
            (bytes.fromhex(definition.manifest_sha256),),
        )
        connector.execute(PARENT_STATEMENT.sql)
        connector.execute(CHILD_STATEMENT.sql)
        connector.execute(PARENT_SEED.sql, PARENT_SEED.parameters)

        report = run_mariadb_schema_epoch(
            connector,
            MariaDBTestProvider(definition),
            clock=lambda: NOW,
            lock_timeout_seconds=0,
        )
        rows = connector.fetch_all(
            "SELECT parent_id, payload FROM vnext_mariadb_parents"
        )

    assert report.resumed_build is True
    assert report.bootstrap_seed_ids == (PARENT_SEED.seed_id,)
    assert rows == [(0, b"\x00" * 32)]


def test_mariadb_epoch_rejects_nonempty_old_database_and_releases_lock(
    mariadb_config: CoreConfig,
) -> None:
    with _mariadb_connector(mariadb_config) as connector:
        connector.execute("CREATE TABLE legacy_schema (value INT NOT NULL)")
        with pytest.raises(SchemaEpochAdmissionError, match="truly empty"):
            run_mariadb_schema_epoch(
                connector,
                MariaDBTestProvider(_definition()),
                lock_timeout_seconds=0,
            )
        free_lock = connector.fetch_one(
            "SELECT IS_FREE_LOCK(%s)",
            (
                mariadb_schema_epoch_gate_name(
                    str(connector.fetch_one("SELECT DATABASE()")[0])
                ),
            ),
        )

    assert free_lock == (1,)


def test_mariadb_epoch_rejects_wrong_control_shape_and_releases_lock(
    mariadb_config: CoreConfig,
) -> None:
    with _mariadb_connector(mariadb_config) as connector:
        connector.execute(
            "CREATE TABLE h2hdb_schema_epoch "
            "(singleton_id SMALLINT UNSIGNED PRIMARY KEY)"
        )
        with pytest.raises(SchemaEpochValidationError, match="wrong shape"):
            run_mariadb_schema_epoch(
                connector,
                MariaDBTestProvider(_definition()),
                lock_timeout_seconds=0,
            )
        free_lock = connector.fetch_one(
            "SELECT IS_FREE_LOCK(%s)",
            (
                mariadb_schema_epoch_gate_name(
                    str(connector.fetch_one("SELECT DATABASE()")[0])
                ),
            ),
        )

    assert free_lock == (1,)
