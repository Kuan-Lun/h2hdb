"""Greenfield schema-epoch orchestration.

This module is the bootstrap mechanism for the sole generated epoch-2 physical
schema. Merely importing it does not activate or alter a database.

The runner owns only the epoch control relation and orchestration.  Physical
DDL, structural validators, and runtime semantic obligations are supplied by
an injected provider so the generated artifact can live in the wheel without
making the runtime depend on the repository's ``verification`` directory.
"""

from __future__ import annotations

__all__ = [
    "MARIADB_SCHEMA_EPOCH_GATE_NAME",
    "SCHEMA_EPOCH_CONTROL_TABLE",
    "V_NEXT_SCHEMA_EPOCH",
    "V_NEXT_SCHEMA_VERSION",
    "MariaDBNamedSchemaEpochGate",
    "MariaDBAdvisorySchemaEpochGate",
    "MariaDBSchemaEpochCatalog",
    "MariaDBSchemaEpochGateAdapter",
    "mariadb_schema_epoch_gate_name",
    "SchemaCreateStatement",
    "SchemaEpochAdmissionError",
    "SchemaEpochCatalog",
    "SchemaEpochDefinition",
    "SchemaEpochDriftError",
    "SchemaEpochError",
    "SchemaEpochGate",
    "SchemaEpochGateError",
    "SchemaEpochProvider",
    "SchemaEpochReport",
    "SchemaEpochRunner",
    "SchemaSeedStatement",
    "SchemaEpochValidationError",
    "SchemaObject",
    "SchemaObjectKind",
    "SchemaSemanticValidationPhase",
    "SchemaSlice",
    "SQLiteImmediateSchemaEpochGate",
    "SQLiteSchemaEpochCatalog",
    "run_mariadb_schema_epoch",
    "run_sqlite_schema_epoch",
    "validate_mariadb_schema_epoch",
    "validate_sqlite_schema_epoch",
]

import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import Any, NoReturn, Protocol

from .sql_connector import SQLConnector

V_NEXT_SCHEMA_EPOCH = 2
V_NEXT_SCHEMA_VERSION = 1
SCHEMA_EPOCH_CONTROL_TABLE = "h2hdb_schema_epoch"
MARIADB_SCHEMA_EPOCH_GATE_NAME = "h2hdb:schema-epoch:2"

_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CREATE_IF_NOT_EXISTS = re.compile(
    r"\A\s*CREATE\s+(?:"
    r"(?:UNIQUE\s+)?(?:TABLE|INDEX|VIEW|TRIGGER)"
    r"|SQL\s+SECURITY\s+INVOKER\s+VIEW"
    r")\s+IF\s+NOT\s+EXISTS\b",
    re.IGNORECASE,
)
_INSERT_INTO = re.compile(
    r"\A\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)
_MARIADB_NOOP_UPDATE = re.compile(
    r".*\bON\s+DUPLICATE\s+KEY\s+UPDATE\s+" r"(`?[A-Za-z_][A-Za-z0-9_]*`?)\s*=\s*\1\Z",
    re.IGNORECASE,
)
_READ_ONLY_SELECT = re.compile(r"\ASELECT\b", re.IGNORECASE)
_SELECT_SIDE_EFFECT = re.compile(
    r"\b(?:INTO\s+(?:OUTFILE|DUMPFILE)|FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|"
    r"GET_LOCK|RELEASE_LOCK|SLEEP|BENCHMARK|LOAD_FILE)\b",
    re.IGNORECASE,
)


class SchemaEpochError(RuntimeError):
    """Base error for greenfield schema-epoch orchestration."""


class SchemaEpochAdmissionError(SchemaEpochError):
    """The database is not an admissible empty or same-epoch database."""


class SchemaEpochDriftError(SchemaEpochError):
    """The durable epoch identity differs from the injected provider."""


class SchemaEpochValidationError(SchemaEpochError):
    """A physical or semantic schema invariant did not validate."""


class SchemaEpochGateError(SchemaEpochError):
    """The backend could not exclusively hold or release the schema gate."""


class SchemaObjectKind(StrEnum):
    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    TRIGGER = "trigger"


class SchemaSemanticValidationPhase(StrEnum):
    ACTIVATION = "BUILDING_TO_READY"
    READY = "READY_REVALIDATION"


@dataclass(frozen=True, slots=True, order=True)
class SchemaObject:
    kind: SchemaObjectKind
    name: str

    def __post_init__(self) -> None:
        if _SAFE_IDENTIFIER.fullmatch(self.name) is None:
            raise ValueError(f"Unsafe schema object identifier: {self.name!r}")


@dataclass(frozen=True, slots=True)
class SchemaCreateStatement:
    """One statement from a checksum-pinned, trusted generated provider.

    The prefix check catches accidental destructive or non-idempotent DDL.  It
    is deliberately not a SQL parser or a security boundary and does not make
    arbitrary untrusted multi-statement text safe to execute.
    """

    statement_id: str
    sql: str
    creates: SchemaObject

    def __post_init__(self) -> None:
        if not self.statement_id:
            raise ValueError("Schema statement IDs must not be empty")
        sql_without_trailing_terminator = self.sql.strip().removesuffix(";").rstrip()
        if (
            _CREATE_IF_NOT_EXISTS.match(sql_without_trailing_terminator) is None
            or ";" in sql_without_trailing_terminator
        ):
            raise ValueError(
                f"Schema statement {self.statement_id!r} must be a "
                "single CREATE ... IF NOT EXISTS statement"
            )


@dataclass(frozen=True, slots=True)
class SchemaSlice:
    slice_id: str
    statements: tuple[SchemaCreateStatement, ...]

    def __post_init__(self) -> None:
        if not self.slice_id:
            raise ValueError("Schema slice IDs must not be empty")
        if not self.statements:
            raise ValueError(f"Schema slice {self.slice_id!r} must not be empty")


type SchemaSeedValue = bytes | int | str | None


@dataclass(frozen=True, slots=True)
class SchemaSeedStatement:
    """One checksum-bound, idempotent bootstrap row insertion.

    Seed statements are trusted generated input, not arbitrary SQL.  The
    deliberately narrow shape check prevents accidental destructive or
    non-replayable statements.  Exact-row collision detection remains the
    provider's responsibility in ``validate_bootstrap_seeds`` because SQL's
    no-op conflict branch alone cannot distinguish an identical replay from a
    conflicting pre-existing row.
    """

    seed_id: str
    target_table: str
    sql: str
    parameters: tuple[SchemaSeedValue, ...]

    def __post_init__(self) -> None:
        if not self.seed_id:
            raise ValueError("Schema seed IDs must not be empty")
        if _SAFE_IDENTIFIER.fullmatch(self.target_table) is None:
            raise ValueError(
                f"Unsafe schema seed target identifier: {self.target_table!r}"
            )
        sql_without_trailing_terminator = self.sql.strip().removesuffix(";").rstrip()
        match = _INSERT_INTO.match(sql_without_trailing_terminator)
        normalized = " ".join(sql_without_trailing_terminator.upper().split())
        is_sqlite_noop = " ON CONFLICT" in f" {normalized}" and normalized.endswith(
            " DO NOTHING"
        )
        is_mariadb_noop = _MARIADB_NOOP_UPDATE.fullmatch(normalized) is not None
        if (
            match is None
            or match.group(1).casefold() != self.target_table.casefold()
            or ";" in sql_without_trailing_terminator
            or not (is_sqlite_noop or is_mariadb_noop)
        ):
            raise ValueError(
                f"Schema seed {self.seed_id!r} must be one idempotent INSERT "
                "INTO its declared target with an explicit no-op conflict branch"
            )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (bytes, int, str, type(None)))
            for value in self.parameters
        ):
            raise ValueError(
                f"Schema seed {self.seed_id!r} has a non-canonical parameter type"
            )


@dataclass(frozen=True, slots=True)
class SchemaEpochDefinition:
    """Immutable identity and closed-world object set for one schema epoch.

    ``ddl_manifest_sha256`` identifies the complete generated physical
    artifact, including the catalog-owned epoch-control projection.
    ``seed_manifest_sha256`` and ``obligation_manifest_sha256`` separately
    identify genesis data and executable semantic obligations.  The runner
    binds all three manifests into the single ``manifest_sha256`` stored in
    the control relation, so changing any input is durable drift even when
    epoch and version remain unchanged.
    """

    epoch: int
    schema_version: int
    ddl_manifest_sha256: str
    seed_manifest_sha256: str
    obligation_manifest_sha256: str
    expected_objects: frozenset[SchemaObject]
    slices: tuple[SchemaSlice, ...]
    bootstrap_seeds: tuple[SchemaSeedStatement, ...]
    activation_semantic_obligation_ids: tuple[str, ...]
    ready_semantic_obligation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.epoch != V_NEXT_SCHEMA_EPOCH:
            raise ValueError(
                f"This runner supports schema epoch {V_NEXT_SCHEMA_EPOCH}, "
                f"not {self.epoch}"
            )
        if self.schema_version != V_NEXT_SCHEMA_VERSION:
            raise ValueError(
                f"This runner supports schema version {V_NEXT_SCHEMA_VERSION}, "
                f"not {self.schema_version}"
            )
        _require_sha256("DDL manifest", self.ddl_manifest_sha256)
        _require_sha256("bootstrap-seed manifest", self.seed_manifest_sha256)
        _require_sha256("semantic-obligation manifest", self.obligation_manifest_sha256)
        if not self.slices:
            raise ValueError("A schema epoch must contain at least one slice")

        slice_ids = [schema_slice.slice_id for schema_slice in self.slices]
        if len(slice_ids) != len(set(slice_ids)):
            raise ValueError("Schema slice IDs must be unique")
        statements = [
            statement
            for schema_slice in self.slices
            for statement in schema_slice.statements
        ]
        statement_ids = [statement.statement_id for statement in statements]
        if len(statement_ids) != len(set(statement_ids)):
            raise ValueError("Schema statement IDs must be unique")
        declared_objects = frozenset(statement.creates for statement in statements)
        if len(declared_objects) != len(statements):
            raise ValueError(
                "Each schema object must have exactly one CREATE statement"
            )
        if declared_objects != self.expected_objects:
            raise ValueError(
                "The expected-object whitelist must exactly match CREATE statements"
            )
        if any(
            schema_object.name == SCHEMA_EPOCH_CONTROL_TABLE
            for schema_object in self.expected_objects
        ):
            raise ValueError(
                "The provider must not declare the epoch control table: it is "
                "generated from the same physical contract but owned and "
                "validated by SchemaEpochCatalog"
            )
        if not self.bootstrap_seeds:
            raise ValueError("A schema epoch must declare bootstrap seeds")
        seed_ids = [seed.seed_id for seed in self.bootstrap_seeds]
        if len(seed_ids) != len(set(seed_ids)):
            raise ValueError("Schema seed IDs must be unique")
        expected_tables = {
            value.name
            for value in self.expected_objects
            if value.kind is SchemaObjectKind.TABLE
        }
        unknown_seed_targets = {
            seed.target_table
            for seed in self.bootstrap_seeds
            if seed.target_table not in expected_tables
        }
        if unknown_seed_targets:
            raise ValueError(
                "Schema seeds must target provider-owned tables: "
                + ", ".join(sorted(unknown_seed_targets))
            )
        for label, obligation_ids in (
            ("activation", self.activation_semantic_obligation_ids),
            ("READY", self.ready_semantic_obligation_ids),
        ):
            if not obligation_ids:
                raise ValueError(
                    f"A schema epoch must declare {label} semantic obligations"
                )
            if any(not obligation_id for obligation_id in obligation_ids):
                raise ValueError("Semantic-obligation IDs must not be empty")
            if len(obligation_ids) != len(set(obligation_ids)):
                raise ValueError(f"{label} semantic-obligation IDs must be unique")

    @property
    def semantic_obligation_ids(self) -> tuple[str, ...]:
        """All recurring semantic IDs in stable first-use order."""

        return tuple(
            dict.fromkeys(
                self.activation_semantic_obligation_ids
                + self.ready_semantic_obligation_ids
            )
        )

    @property
    def manifest_sha256(self) -> str:
        manifest = sha256()
        manifest.update(b"h2hdb-schema-epoch-manifest-v2\0")
        manifest.update(self.ddl_manifest_sha256.encode("ascii"))
        manifest.update(b"\0")
        manifest.update(self.seed_manifest_sha256.encode("ascii"))
        manifest.update(b"\0")
        manifest.update(self.obligation_manifest_sha256.encode("ascii"))
        return manifest.hexdigest()


class SchemaEpochProvider(Protocol):
    @property
    def definition(self) -> SchemaEpochDefinition: ...

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None: ...

    def validate_global(self, connector: SQLConnector) -> None: ...

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]: ...

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]: ...


class _ReadOnlySemanticConnector(SQLConnector):
    """Expose only query operations to semantic validators.

    Structural validators run before this boundary and legitimately need the
    concrete connector's backend metadata.  Semantic validators, however, are
    checksum-bound assertions over that state.  Passing the writable connector
    to them would let a buggy validator mutate rows without changing the
    closed-world object set; on MariaDB such a mutation may also survive DDL
    transaction boundaries.  This facade makes the read-only contract an
    executable property on both backends.
    """

    def __init__(self, connector: SQLConnector) -> None:
        self._connector = connector

    def connect(self) -> None:
        self._reject_mutation("connect")

    def close(self) -> None:
        self._reject_mutation("close")

    def check_table_exists(self, table_name: str) -> bool:
        return self._connector.check_table_exists(table_name)

    def commit(self) -> None:
        self._reject_mutation("commit")

    def begin(self) -> None:
        self._reject_mutation("begin")

    def rollback(self) -> None:
        self._reject_mutation("rollback")

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        del query, data
        self._reject_mutation("execute")

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        del query, data
        self._reject_mutation("execute_affected")

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        del query, data
        self._reject_mutation("execute_many")

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        self._require_read_only_select(query)
        return self._connector.fetch_one(query, data)

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        self._require_read_only_select(query)
        return self._connector.fetch_all(query, data)

    @staticmethod
    def _require_read_only_select(query: str) -> None:
        statement = query.strip()
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        if (
            not _READ_ONLY_SELECT.match(statement)
            or ";" in statement
            or _SELECT_SIDE_EFFECT.search(statement)
        ):
            raise SchemaEpochValidationError(
                "Semantic validators are read-only; fetch operations accept only "
                "one non-locking SELECT without file, advisory-lock, delay, or "
                "multi-statement side effects"
            )

    @staticmethod
    def _reject_mutation(operation: str) -> NoReturn:
        raise SchemaEpochValidationError(
            "Semantic validators are read-only; "
            f"connector operation {operation!r} is forbidden"
        )


class SchemaEpochGate(Protocol):
    """Exclusive gate held for admission, construction, validation, and CAS."""

    def acquire(self, connector: SQLConnector) -> AbstractContextManager[None]: ...


class MariaDBNamedSchemaEpochGate(Protocol):
    """Interface for the future MariaDB named-lock implementation.

    MariaDB DDL can implicitly commit, so its adapter must hold a server-side
    named gate independently of SQL transaction boundaries.  This protocol is
    intentionally abstract until the greenfield provider is wired into the
    production connector.
    """

    def acquire_named(
        self,
        connector: SQLConnector,
        gate_name: str = MARIADB_SCHEMA_EPOCH_GATE_NAME,
    ) -> AbstractContextManager[None]: ...


@dataclass(frozen=True, slots=True)
class MariaDBSchemaEpochGateAdapter:
    """Adapt an abstract server-side named gate to the runner gate protocol."""

    named_gate: MariaDBNamedSchemaEpochGate
    gate_name: str = MARIADB_SCHEMA_EPOCH_GATE_NAME

    def acquire(self, connector: SQLConnector) -> AbstractContextManager[None]:
        return self.named_gate.acquire_named(connector, self.gate_name)


@dataclass(frozen=True, slots=True)
class MariaDBAdvisorySchemaEpochGate:
    """Hold one MariaDB advisory lock across implicit DDL commits.

    ``GET_LOCK`` and ``RELEASE_LOCK`` are connection scoped.  The runner passes
    the very same connector to this context manager and to every catalog,
    provider, and DDL operation, so the lock survives MariaDB's implicit DDL
    transaction boundaries.  A timeout, ``NULL``, malformed result, or failed
    release is an orchestration failure rather than an unlocked fallback.
    """

    timeout_seconds: int = 60

    def __post_init__(self) -> None:
        if isinstance(self.timeout_seconds, bool) or self.timeout_seconds < 0:
            raise ValueError("MariaDB schema gate timeout must be non-negative")

    def acquire(self, connector: SQLConnector) -> AbstractContextManager[None]:
        return self.acquire_named(connector)

    @contextmanager
    def acquire_named(
        self,
        connector: SQLConnector,
        gate_name: str = MARIADB_SCHEMA_EPOCH_GATE_NAME,
    ) -> Iterator[None]:
        _validate_mariadb_gate_name(gate_name)
        try:
            acquired = connector.fetch_one(
                "SELECT GET_LOCK(%s, %s)",
                (gate_name, self.timeout_seconds),
            )
        except Exception as error:
            raise SchemaEpochGateError(
                f"MariaDB schema gate {gate_name!r} acquisition failed"
            ) from error
        if _mariadb_lock_result(acquired) != 1:
            raise SchemaEpochGateError(
                f"MariaDB schema gate {gate_name!r} was not acquired"
            )

        body_error: BaseException | None = None
        try:
            yield
        except BaseException as error:
            body_error = error
            raise
        finally:
            try:
                released = connector.fetch_one(
                    "SELECT RELEASE_LOCK(%s)",
                    (gate_name,),
                )
                if _mariadb_lock_result(released) != 1:
                    raise SchemaEpochGateError(
                        f"MariaDB schema gate {gate_name!r} was not released"
                    )
            except Exception as error:
                release_error = (
                    error
                    if isinstance(error, SchemaEpochGateError)
                    else SchemaEpochGateError(
                        f"MariaDB schema gate {gate_name!r} release failed"
                    )
                )
                if release_error is not error:
                    release_error.__cause__ = error
                if body_error is not None:
                    body_error.add_note(str(release_error))
                else:
                    raise release_error


class SchemaEpochCatalog(Protocol):
    """Backend-specific schema introspection and control-table DDL."""

    @property
    def control_object(self) -> SchemaObject: ...

    def list_objects(self, connector: SQLConnector) -> frozenset[SchemaObject]: ...

    def create_control_table(self, connector: SQLConnector) -> None: ...

    def validate_control_table(self, connector: SQLConnector) -> None: ...


_MARIADB_CONTROL_DDL = """
    CREATE TABLE h2hdb_schema_epoch (
        singleton_id SMALLINT UNSIGNED NOT NULL,
        epoch BIGINT UNSIGNED NOT NULL,
        schema_version BIGINT UNSIGNED NOT NULL,
        state VARCHAR(48) COLLATE ascii_bin NOT NULL,
        manifest_sha256 BINARY(32) NOT NULL,
        started_at BIGINT UNSIGNED NOT NULL,
        ready_at BIGINT UNSIGNED NULL,
        PRIMARY KEY (singleton_id),
        CONSTRAINT ck_schema_epoch_control_singleton
            CHECK (singleton_id = 1),
        CONSTRAINT ck_schema_epoch_control_manifest_sha256_len
            CHECK (octet_length(manifest_sha256) = 32),
        CONSTRAINT ck_schema_epoch_control_started_at_nonneg
            CHECK (started_at >= 0),
        CONSTRAINT ck_schema_epoch_control_ready_at_nonneg
            CHECK (ready_at IS NULL OR ready_at >= 0),
        CONSTRAINT ck_schema_epoch_control_epoch_nonneg
            CHECK (epoch >= 0),
        CONSTRAINT ck_schema_epoch_control_schema_version_nonneg
            CHECK (schema_version >= 0),
        CONSTRAINT ck_schema_epoch_state
            CHECK (
                state = 'BUILDING' AND ready_at IS NULL
                OR state = 'READY'
                   AND ready_at IS NOT NULL
                   AND ready_at >= started_at
            ),
        CONSTRAINT ck_schema_epoch_manifest_blob
            CHECK (octet_length(manifest_sha256) = 32)
    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
      COLLATE=utf8mb4_nopad_bin
"""

_MARIADB_CONTROL_COLUMNS = (
    ("singleton_id", 1, "SMALLINT UNSIGNED", "NO", None, None, None),
    ("epoch", 2, "BIGINT UNSIGNED", "NO", None, None, None),
    ("schema_version", 3, "BIGINT UNSIGNED", "NO", None, None, None),
    ("state", 4, "VARCHAR(48)", "NO", "ascii", "ascii_bin", 48),
    ("manifest_sha256", 5, "BINARY(32)", "NO", None, None, 32),
    ("started_at", 6, "BIGINT UNSIGNED", "NO", None, None, None),
    ("ready_at", 7, "BIGINT UNSIGNED", "YES", None, None, None),
)

_MARIADB_CONTROL_CHECKS = {
    "ck_schema_epoch_control_singleton": "singleton_id = 1",
    "ck_schema_epoch_control_manifest_sha256_len": (
        "octet_length(manifest_sha256) = 32"
    ),
    "ck_schema_epoch_control_started_at_nonneg": "started_at >= 0",
    "ck_schema_epoch_control_ready_at_nonneg": ("ready_at is null or ready_at >= 0"),
    "ck_schema_epoch_control_epoch_nonneg": "epoch >= 0",
    "ck_schema_epoch_control_schema_version_nonneg": "schema_version >= 0",
    "ck_schema_epoch_state": (
        "state = 'building' and ready_at is null or state = 'ready' "
        "and ready_at is not null and ready_at >= started_at"
    ),
    "ck_schema_epoch_manifest_blob": "octet_length(manifest_sha256) = 32",
}


class MariaDBSchemaEpochCatalog:
    """MariaDB catalog for the generated epoch-control physical contract.

    MariaDB indexes are table-internal physical structure: both inline ``KEY``
    clauses and later ``CREATE INDEX`` statements appear identically in
    ``INFORMATION_SCHEMA.STATISTICS``.  They therefore cannot participate in
    this top-level namespace whitelist without misclassifying generated inline
    keys.  The injected provider's slice/global refinement checks own the exact
    PK, UK, FK, CHECK, and index shape for every table.  This catalog lists the
    independently named top-level objects: tables, views, and triggers.
    """

    @property
    def control_object(self) -> SchemaObject:
        return SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE)

    def list_objects(self, connector: SQLConnector) -> frozenset[SchemaObject]:
        try:
            table_rows = connector.fetch_all("""
                SELECT TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME
                """)
            trigger_rows = connector.fetch_all("""
                SELECT TRIGGER_NAME
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                ORDER BY TRIGGER_NAME
                """)
            objects = {
                SchemaObject(
                    (
                        SchemaObjectKind.VIEW
                        if str(table_type).upper() == "VIEW"
                        else SchemaObjectKind.TABLE
                    ),
                    str(name),
                )
                for name, table_type, *_ in table_rows
            }
            objects.update(
                SchemaObject(SchemaObjectKind.TRIGGER, str(row[0]))
                for row in trigger_rows
                if row
            )
            return frozenset(objects)
        except (TypeError, ValueError) as error:
            raise SchemaEpochValidationError(
                f"MariaDB returned an unsupported schema object: {error}"
            ) from error

    def create_control_table(self, connector: SQLConnector) -> None:
        connector.execute(_MARIADB_CONTROL_DDL)

    def validate_control_table(self, connector: SQLConnector) -> None:
        try:
            table_rows = connector.fetch_all(
                """
                SELECT TABLE_TYPE, ENGINE, TABLE_COLLATION
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (SCHEMA_EPOCH_CONTROL_TABLE,),
            )
            expected_table = (("BASE TABLE", "INNODB", "utf8mb4_nopad_bin"),)
            actual_table = tuple(
                (
                    str(table_type).upper(),
                    str(engine).upper(),
                    str(collation).casefold(),
                )
                for table_type, engine, collation, *_ in table_rows
            )
            if actual_table != expected_table:
                _raise_wrong_mariadb_control_shape(
                    f"table metadata is {actual_table!r}"
                )

            column_rows = connector.fetch_all(
                """
                SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE,
                       IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME,
                       CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (SCHEMA_EPOCH_CONTROL_TABLE,),
            )
            actual_columns = tuple(
                (
                    str(name),
                    int(ordinal),
                    _normalize_mariadb_type(str(column_type)),
                    str(nullable).upper(),
                    None if charset is None else str(charset).casefold(),
                    None if collation is None else str(collation).casefold(),
                    None if maximum_length is None else int(maximum_length),
                )
                for (
                    name,
                    ordinal,
                    column_type,
                    nullable,
                    charset,
                    collation,
                    maximum_length,
                    default,
                    extra,
                    *_,
                ) in column_rows
                if (
                    default is None
                    or (
                        str(nullable).upper() == "YES"
                        and str(default).upper() == "NULL"
                    )
                )
                and str(extra) == ""
            )
            if (
                len(actual_columns) != len(column_rows)
                or actual_columns != _MARIADB_CONTROL_COLUMNS
            ):
                _raise_wrong_mariadb_control_shape(
                    f"column metadata is {actual_columns!r}"
                )

            constraint_rows = connector.fetch_all(
                """
                SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY CONSTRAINT_NAME
                """,
                (SCHEMA_EPOCH_CONTROL_TABLE,),
            )
            actual_constraints = {
                (str(name), str(constraint_type).upper())
                for name, constraint_type, *_ in constraint_rows
            }
            expected_constraints = {
                ("PRIMARY", "PRIMARY KEY"),
                *((name, "CHECK") for name in _MARIADB_CONTROL_CHECKS),
            }
            if actual_constraints != expected_constraints:
                _raise_wrong_mariadb_control_shape(
                    f"constraints are {sorted(actual_constraints)!r}"
                )

            index_rows = connector.fetch_all(
                """
                SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (SCHEMA_EPOCH_CONTROL_TABLE,),
            )
            actual_indexes = tuple(
                (str(name), int(non_unique), int(sequence), str(column))
                for name, non_unique, sequence, column, *_ in index_rows
            )
            if actual_indexes != (("PRIMARY", 0, 1, "singleton_id"),):
                _raise_wrong_mariadb_control_shape(f"indexes are {actual_indexes!r}")

            check_rows = connector.fetch_all(
                """
                SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc
                  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                 AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = DATABASE()
                  AND tc.TABLE_NAME = %s
                  AND tc.CONSTRAINT_TYPE = 'CHECK'
                ORDER BY tc.CONSTRAINT_NAME
                """,
                (SCHEMA_EPOCH_CONTROL_TABLE,),
            )
            actual_checks = {
                str(name): _normalize_mariadb_check(str(expression))
                for name, expression, *_ in check_rows
            }
            expected_checks = {
                name: _normalize_mariadb_check(expression)
                for name, expression in _MARIADB_CONTROL_CHECKS.items()
            }
            if actual_checks != expected_checks:
                _raise_wrong_mariadb_control_shape(f"checks are {actual_checks!r}")
        except SchemaEpochError:
            raise
        except Exception as error:
            raise SchemaEpochValidationError(
                "The existing MariaDB schema epoch control table has the "
                f"wrong shape: metadata read failed: {error}"
            ) from error


class SQLiteImmediateSchemaEpochGate:
    """SQLite serialization via the connector's ``BEGIN IMMEDIATE``."""

    @contextmanager
    def acquire(self, connector: SQLConnector) -> Iterator[None]:
        with connector.transaction():
            yield


_SQLITE_CONTROL_DDL = """
    CREATE TABLE h2hdb_schema_epoch (
        singleton_id INTEGER NOT NULL PRIMARY KEY
            CHECK (singleton_id = 1),
        epoch INTEGER NOT NULL CHECK (epoch >= 0),
        schema_version INTEGER NOT NULL CHECK (schema_version >= 0),
        state TEXT COLLATE BINARY NOT NULL
            CHECK (state IN ('BUILDING', 'READY')),
        manifest_sha256 BLOB NOT NULL
            CHECK (
                typeof(manifest_sha256) = 'blob'
                AND length(manifest_sha256) = 32
            ),
        started_at INTEGER NOT NULL CHECK (started_at >= 0),
        ready_at INTEGER CHECK (ready_at IS NULL OR ready_at >= 0),
        CHECK (
            (state = 'BUILDING' AND ready_at IS NULL)
            OR (
                state = 'READY'
                AND ready_at IS NOT NULL
                AND ready_at >= started_at
            )
        )
    )
"""


class SQLiteSchemaEpochCatalog:
    @property
    def control_object(self) -> SchemaObject:
        return SchemaObject(SchemaObjectKind.TABLE, SCHEMA_EPOCH_CONTROL_TABLE)

    def list_objects(self, connector: SQLConnector) -> frozenset[SchemaObject]:
        rows = connector.fetch_all("""
            SELECT type, name
            FROM sqlite_master
            WHERE type IN ('table', 'index', 'view', 'trigger')
              AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
            """)
        try:
            return frozenset(
                SchemaObject(SchemaObjectKind(str(kind)), str(name))
                for kind, name in rows
            )
        except (TypeError, ValueError) as error:
            raise SchemaEpochValidationError(
                f"SQLite returned an unsupported schema object: {error}"
            ) from error

    def create_control_table(self, connector: SQLConnector) -> None:
        connector.execute(_SQLITE_CONTROL_DDL)

    def validate_control_table(self, connector: SQLConnector) -> None:
        row = connector.fetch_one(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = %s",
            (SCHEMA_EPOCH_CONTROL_TABLE,),
        )
        if len(row) != 1 or not isinstance(row[0], str):
            raise SchemaEpochValidationError(
                "The schema epoch control table is missing or unreadable"
            )
        if _normalize_sql(row[0]) != _normalize_sql(_SQLITE_CONTROL_DDL):
            raise SchemaEpochValidationError(
                "The existing schema epoch control table has the wrong shape"
            )


@dataclass(frozen=True, slots=True)
class SchemaEpochReport:
    epoch: int
    schema_version: int
    state: str
    manifest_sha256: str
    bootstrap_seed_ids: tuple[str, ...]
    semantic_obligation_ids: tuple[str, ...]
    resumed_build: bool
    transitioned_to_ready: bool


class SchemaEpochRunner:
    def __init__(
        self,
        *,
        gate: SchemaEpochGate | None,
        catalog: SchemaEpochCatalog,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._gate = gate
        self._catalog = catalog
        self._clock = clock or (lambda: datetime.now(UTC))

    def run(
        self, connector: SQLConnector, provider: SchemaEpochProvider
    ) -> SchemaEpochReport:
        definition = provider.definition
        # Accessing the computed property also ensures all identity inputs were
        # validated by SchemaEpochDefinition before touching the database.
        manifest_sha256 = definition.manifest_sha256
        allowed_objects = definition.expected_objects | {self._catalog.control_object}

        if self._gate is None:
            raise SchemaEpochGateError(
                "Schema construction requires an exclusive backend gate"
            )

        with self._gate.acquire(connector):
            existing_objects = self._catalog.list_objects(connector)
            has_control = self._catalog.control_object in existing_objects
            if not has_control:
                if existing_objects:
                    raise SchemaEpochAdmissionError(
                        "Schema epoch 2 requires a truly empty database; found "
                        f"{_format_objects(existing_objects)}"
                    )
                self._catalog.create_control_table(connector)
                self._catalog.validate_control_table(connector)
                self._insert_building_control(connector, definition, manifest_sha256)
                resumed_build = False
            else:
                self._catalog.validate_control_table(connector)
                resumed_build = True
                control_count = connector.fetch_one(
                    "SELECT COUNT(*) FROM h2hdb_schema_epoch"
                )
                if control_count == (0,):
                    # MariaDB commits CREATE TABLE independently of the seed
                    # INSERT.  Only the exact, otherwise-empty control-table
                    # residue can be adopted as an interrupted bootstrap.
                    if existing_objects != {self._catalog.control_object}:
                        raise SchemaEpochAdmissionError(
                            "An empty schema epoch control relation may be "
                            "resumed only before any provider object exists"
                        )
                    self._insert_building_control(
                        connector, definition, manifest_sha256
                    )

            self._assert_admissible_objects(connector, allowed_objects)
            state = self._read_and_validate_control(
                connector, definition, manifest_sha256
            )
            if state == "READY":
                obligation_ids = self._validate_ready_schema(
                    connector,
                    provider,
                    definition,
                    validate_genesis=False,
                    semantic_phase=SchemaSemanticValidationPhase.READY,
                )
                return SchemaEpochReport(
                    epoch=definition.epoch,
                    schema_version=definition.schema_version,
                    state="READY",
                    manifest_sha256=manifest_sha256,
                    bootstrap_seed_ids=tuple(
                        seed.seed_id for seed in definition.bootstrap_seeds
                    ),
                    semantic_obligation_ids=obligation_ids,
                    resumed_build=resumed_build,
                    transitioned_to_ready=False,
                )

            for schema_slice in definition.slices:
                for statement in schema_slice.statements:
                    connector.execute(statement.sql)
                    actual_objects = self._assert_admissible_objects(
                        connector, allowed_objects
                    )
                    if statement.creates not in actual_objects:
                        raise SchemaEpochValidationError(
                            f"Statement {statement.statement_id!r} did not create "
                            f"its declared {statement.creates.kind.value} "
                            f"{statement.creates.name!r}"
                        )
                provider.validate_slice(connector, schema_slice)

            for seed in definition.bootstrap_seeds:
                connector.execute(seed.sql, seed.parameters)

            obligation_ids = self._validate_ready_schema(
                connector,
                provider,
                definition,
                validate_genesis=True,
                semantic_phase=SchemaSemanticValidationPhase.ACTIVATION,
            )
            connector.execute(
                """
                UPDATE h2hdb_schema_epoch
                SET state = 'READY', ready_at = %s
                WHERE singleton_id = 1
                  AND epoch = %s
                  AND schema_version = %s
                  AND state = 'BUILDING'
                  AND manifest_sha256 = %s
                """,
                (
                    _epoch_microseconds(self._clock()),
                    definition.epoch,
                    definition.schema_version,
                    bytes.fromhex(manifest_sha256),
                ),
            )
            final_state = self._read_and_validate_control(
                connector, definition, manifest_sha256
            )
            if final_state != "READY":
                raise SchemaEpochValidationError(
                    "The compare-and-set transition to READY did not succeed"
                )
            return SchemaEpochReport(
                epoch=definition.epoch,
                schema_version=definition.schema_version,
                state="READY",
                manifest_sha256=manifest_sha256,
                bootstrap_seed_ids=tuple(
                    seed.seed_id for seed in definition.bootstrap_seeds
                ),
                semantic_obligation_ids=obligation_ids,
                resumed_build=resumed_build,
                transitioned_to_ready=True,
            )

    def validate_ready(
        self,
        connector: SQLConnector,
        provider: SchemaEpochProvider,
    ) -> SchemaEpochReport:
        """Fully validate an existing READY epoch without acquiring a write gate.

        The caller owns a stable read transaction.  This path never creates,
        seeds, resumes, or transitions schema state, so SQLite read-only
        consumers do not need ``BEGIN IMMEDIATE`` and MariaDB consumers do not
        need the advisory construction lock.
        """

        definition = provider.definition
        manifest_sha256 = definition.manifest_sha256
        allowed_objects = definition.expected_objects | {self._catalog.control_object}
        existing_objects = self._catalog.list_objects(connector)
        if self._catalog.control_object not in existing_objects:
            raise SchemaEpochAdmissionError(
                "Schema epoch 2 is not initialized: its control table is missing"
            )
        self._catalog.validate_control_table(connector)
        self._assert_admissible_objects(connector, allowed_objects)
        state = self._read_and_validate_control(
            connector,
            definition,
            manifest_sha256,
        )
        if state != "READY":
            raise SchemaEpochAdmissionError(
                f"Schema epoch 2 is not READY (state={state!r})"
            )
        obligation_ids = self._validate_ready_schema(
            connector,
            provider,
            definition,
            validate_genesis=False,
            semantic_phase=SchemaSemanticValidationPhase.READY,
        )
        return SchemaEpochReport(
            epoch=definition.epoch,
            schema_version=definition.schema_version,
            state="READY",
            manifest_sha256=manifest_sha256,
            bootstrap_seed_ids=tuple(
                seed.seed_id for seed in definition.bootstrap_seeds
            ),
            semantic_obligation_ids=obligation_ids,
            resumed_build=True,
            transitioned_to_ready=False,
        )

    def _insert_building_control(
        self,
        connector: SQLConnector,
        definition: SchemaEpochDefinition,
        manifest_sha256: str,
    ) -> None:
        connector.execute(
            """
            INSERT INTO h2hdb_schema_epoch (
                singleton_id,
                epoch,
                schema_version,
                state,
                manifest_sha256,
                started_at,
                ready_at
            ) VALUES (1, %s, %s, 'BUILDING', %s, %s, NULL)
            """,
            (
                definition.epoch,
                definition.schema_version,
                bytes.fromhex(manifest_sha256),
                _epoch_microseconds(self._clock()),
            ),
        )

    def _assert_admissible_objects(
        self,
        connector: SQLConnector,
        allowed_objects: frozenset[SchemaObject],
    ) -> frozenset[SchemaObject]:
        actual_objects = self._catalog.list_objects(connector)
        unexpected = actual_objects - allowed_objects
        if unexpected:
            raise SchemaEpochAdmissionError(
                "The database contains objects outside this epoch manifest: "
                f"{_format_objects(unexpected)}"
            )
        return actual_objects

    def _read_and_validate_control(
        self,
        connector: SQLConnector,
        definition: SchemaEpochDefinition,
        manifest_sha256: str,
    ) -> str:
        rows = connector.fetch_all("""
            SELECT epoch, schema_version, state, manifest_sha256,
                   started_at, ready_at
            FROM h2hdb_schema_epoch
            WHERE singleton_id = 1
            """)
        if len(rows) != 1:
            raise SchemaEpochAdmissionError(
                "The schema epoch control relation must contain exactly one row"
            )
        epoch, schema_version, state, stored_manifest, started_at, ready_at = rows[0]
        if int(epoch) != definition.epoch:
            raise SchemaEpochDriftError(
                f"Database schema epoch is {epoch}, expected {definition.epoch}"
            )
        if int(schema_version) != definition.schema_version:
            raise SchemaEpochDriftError(
                "Database schema version is "
                f"{schema_version}, expected {definition.schema_version}"
            )
        if stored_manifest != bytes.fromhex(manifest_sha256):
            raise SchemaEpochDriftError(
                "Database schema manifest differs from the injected provider"
            )
        if state not in {"BUILDING", "READY"}:
            raise SchemaEpochValidationError(
                f"Unsupported schema epoch state: {state!r}"
            )
        if not isinstance(started_at, int) or started_at < 0:
            raise SchemaEpochValidationError("Schema epoch started_at is invalid")
        if (state == "BUILDING" and ready_at is not None) or (
            state == "READY"
            and (not isinstance(ready_at, int) or ready_at < started_at)
        ):
            raise SchemaEpochValidationError(
                "Schema epoch state and ready_at are inconsistent"
            )
        return str(state)

    def _validate_ready_schema(
        self,
        connector: SQLConnector,
        provider: SchemaEpochProvider,
        definition: SchemaEpochDefinition,
        *,
        validate_genesis: bool,
        semantic_phase: SchemaSemanticValidationPhase,
    ) -> tuple[str, ...]:
        expected = definition.expected_objects | {self._catalog.control_object}
        actual = self._assert_admissible_objects(connector, expected)
        missing = expected - actual
        if missing:
            raise SchemaEpochValidationError(
                f"The schema is missing manifest objects: {_format_objects(missing)}"
            )
        provider.validate_global(connector)
        expected_seed_ids = tuple(seed.seed_id for seed in definition.bootstrap_seeds)
        if validate_genesis:
            reported_seed_ids = tuple(provider.validate_bootstrap_seeds(connector))
            if reported_seed_ids != expected_seed_ids:
                raise SchemaEpochValidationError(
                    "Bootstrap validation reported seed IDs "
                    f"{reported_seed_ids!r}, expected {expected_seed_ids!r}"
                )
        expected_obligation_ids = (
            definition.activation_semantic_obligation_ids
            if semantic_phase is SchemaSemanticValidationPhase.ACTIVATION
            else definition.ready_semantic_obligation_ids
        )
        reported_ids = tuple(
            provider.validate_semantics(
                _ReadOnlySemanticConnector(connector), semantic_phase
            )
        )
        if reported_ids != expected_obligation_ids:
            raise SchemaEpochValidationError(
                f"{semantic_phase.value} semantic validation reported obligation IDs "
                f"{reported_ids!r}, expected "
                f"{expected_obligation_ids!r}"
            )
        if validate_genesis:
            repeated_seed_ids = tuple(provider.validate_bootstrap_seeds(connector))
            if repeated_seed_ids != expected_seed_ids:
                raise SchemaEpochValidationError(
                    "Bootstrap rows changed during semantic validation: reported "
                    f"{repeated_seed_ids!r}, expected {expected_seed_ids!r}"
                )
        # Validators are read-only by contract.  Re-check the closed world
        # immediately before READY so an accidental validator-side CREATE
        # cannot silently expand the schema.
        actual_after_validation = self._catalog.list_objects(connector)
        if actual_after_validation != expected:
            raise SchemaEpochValidationError(
                "The closed-world object set changed during final validation"
            )
        return reported_ids


def run_sqlite_schema_epoch(
    connector: SQLConnector,
    provider: SchemaEpochProvider,
    *,
    clock: Callable[[], datetime] | None = None,
) -> SchemaEpochReport:
    runner = SchemaEpochRunner(
        gate=SQLiteImmediateSchemaEpochGate(),
        catalog=SQLiteSchemaEpochCatalog(),
        clock=clock,
    )
    return runner.run(connector, provider)


def validate_sqlite_schema_epoch(
    connector: SQLConnector,
    provider: SchemaEpochProvider,
) -> SchemaEpochReport:
    """Read-only full validation of one existing SQLite READY epoch."""

    runner = SchemaEpochRunner(
        gate=None,
        catalog=SQLiteSchemaEpochCatalog(),
    )
    return runner.validate_ready(connector, provider)


def run_mariadb_schema_epoch(
    connector: SQLConnector,
    provider: SchemaEpochProvider,
    *,
    clock: Callable[[], datetime] | None = None,
    lock_timeout_seconds: int = 60,
    gate_name: str | None = None,
) -> SchemaEpochReport:
    """Build or validate vNext while one connection-scoped MariaDB lock is held."""

    if gate_name is None:
        database_row = connector.fetch_one("SELECT DATABASE()")
        if len(database_row) != 1 or not isinstance(database_row[0], str):
            raise SchemaEpochGateError(
                "MariaDB did not report one current database for schema locking"
            )
        gate_name = mariadb_schema_epoch_gate_name(database_row[0])
    named_gate = MariaDBAdvisorySchemaEpochGate(lock_timeout_seconds)
    runner = SchemaEpochRunner(
        gate=MariaDBSchemaEpochGateAdapter(named_gate, gate_name),
        catalog=MariaDBSchemaEpochCatalog(),
        clock=clock,
    )
    return runner.run(connector, provider)


def validate_mariadb_schema_epoch(
    connector: SQLConnector,
    provider: SchemaEpochProvider,
) -> SchemaEpochReport:
    """Read-only full validation of one existing MariaDB READY epoch."""

    runner = SchemaEpochRunner(
        gate=None,
        catalog=MariaDBSchemaEpochCatalog(),
    )
    return runner.validate_ready(connector, provider)


def mariadb_schema_epoch_gate_name(database_name: str) -> str:
    """Return a stable advisory-lock name scoped to one exact database name."""

    try:
        encoded = database_name.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("MariaDB database name must be valid UTF-8") from error
    if not encoded or b"\x00" in encoded:
        raise ValueError("MariaDB database name must not be empty or contain NUL")
    digest = sha256()
    digest.update(b"h2hdb-schema-epoch-mariadb-gate-v1\0")
    digest.update(len(encoded).to_bytes(4, "big"))
    digest.update(encoded)
    return f"{MARIADB_SCHEMA_EPOCH_GATE_NAME}:{digest.hexdigest()[:32]}"


def _require_sha256(label: str, value: str) -> None:
    if _HEX_SHA256.fullmatch(value) is None:
        raise ValueError(f"{label} SHA-256 must be 64 lowercase hexadecimal digits")


def _epoch_microseconds(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("Schema epoch clock must return a timezone-aware datetime")
    utc_value = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc_value - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _format_objects(objects: frozenset[SchemaObject]) -> str:
    return ", ".join(
        f"{schema_object.kind.value} {schema_object.name!r}"
        for schema_object in sorted(objects)
    )


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().rstrip(";").split()).lower()


def _validate_mariadb_gate_name(gate_name: str) -> None:
    try:
        encoded = gate_name.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("MariaDB schema gate name must be valid UTF-8") from error
    if not encoded or len(encoded) > 64 or b"\x00" in encoded:
        raise ValueError(
            "MariaDB schema gate name must be 1-64 UTF-8 bytes without NUL"
        )


def _mariadb_lock_result(row: tuple[object, ...]) -> int:
    if (
        len(row) != 1
        or isinstance(row[0], bool)
        or not isinstance(row[0], (int, str, bytes, bytearray))
    ):
        raise SchemaEpochGateError(
            f"MariaDB returned an invalid advisory-lock result: {row!r}"
        )
    raw_result = row[0]
    try:
        result = int(raw_result)
    except (TypeError, ValueError) as error:
        raise SchemaEpochGateError(
            f"MariaDB returned an invalid advisory-lock result: {row!r}"
        ) from error
    if result not in {0, 1}:
        raise SchemaEpochGateError(
            f"MariaDB returned an invalid advisory-lock result: {row!r}"
        )
    return result


def _normalize_mariadb_type(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip()).upper()
    return re.sub(
        r"\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)",
        r"\1",
        normalized,
    )


def _normalize_mariadb_check(value: str) -> str:
    normalized = value.replace("`", "").strip().casefold()
    normalized = re.sub(r"(?<![a-z0-9_])_[a-z0-9]+\s*'", "'", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    while (
        normalized.startswith("(")
        and normalized.endswith(")")
        and _closing_parenthesis(normalized, 0) == len(normalized) - 1
    ):
        normalized = normalized[1:-1].strip()
    return normalized


def _closing_parenthesis(value: str, opening: int) -> int | None:
    depth = 0
    inside_quote = False
    position = opening
    while position < len(value):
        character = value[position]
        if character == "'":
            if (
                inside_quote
                and position + 1 < len(value)
                and value[position + 1] == "'"
            ):
                position += 2
                continue
            inside_quote = not inside_quote
        elif not inside_quote:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    return position
        position += 1
    return None


def _raise_wrong_mariadb_control_shape(detail: str) -> NoReturn:
    raise SchemaEpochValidationError(
        f"The existing MariaDB schema epoch control table has the wrong shape: {detail}"
    )
