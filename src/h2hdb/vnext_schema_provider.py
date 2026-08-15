"""Runtime provider for the generated vNext physical schema.

The large generated artifact is intentionally data-only and wheel-resident.
This module supplies the small handwritten trust boundary that turns it into a
``SchemaEpochProvider``.  It never imports the repository's verification
package.

The generated artifact and wheel registries advertise every unresolved formal,
executable-validator, or recurring writer-hook dependency as a blocker and
:attr:`definition` fails closed.  It is not possible to transition an epoch to
``READY`` by merely echoing prose obligations or by omitting exact bootstrap-row
validation.
"""

from __future__ import annotations

__all__ = [
    "GeneratedVNextSchemaProvider",
    "VNextSchemaArtifactDriftError",
    "VNextSchemaProviderError",
    "VNextSchemaProviderUnavailableError",
]

import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, cast

from ._generated_vnext_schema import ARTIFACT
from .schema_epoch import (
    SchemaCreateStatement,
    SchemaEpochDefinition,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaObjectKind,
    SchemaSeedStatement,
    SchemaSemanticValidationPhase,
    SchemaSlice,
)
from .sql_connector import SQLConnector

if TYPE_CHECKING:
    from .catalog_writer import WriterHookBinding

type Backend = Literal["sqlite", "mariadb"]
type SemanticValidator = Callable[[SQLConnector], None]


def _ddl_identifier(value: str) -> str:
    """Match the generated portable MariaDB 63-byte identifier codec."""

    encoded = value.encode("ascii")
    if len(encoded) <= 63:
        return value
    return f"{value[:50]}_{hashlib.sha256(encoded).hexdigest()[:12]}"


class VNextSchemaProviderError(RuntimeError):
    """Base error for the generated runtime provider."""


class VNextSchemaProviderUnavailableError(VNextSchemaProviderError):
    """The formal/runtime contracts are not sufficient to publish READY."""


class VNextSchemaArtifactDriftError(VNextSchemaProviderError):
    """The embedded generated artifact is internally inconsistent."""


@dataclass(frozen=True, slots=True)
class GeneratedVNextSchemaProvider:
    """Backend-specific provider assembled from the generated wheel artifact.

    The generated physical definition remains unavailable until the exact
    wheel-owned semantic validators *and* recurring transaction writer hooks
    are installed.  Caller-supplied callbacks are deliberately not accepted:
    allowing a mapping of no-op functions here would turn machine-obligation
    names into a READY bypass.
    """

    backend: Backend
    _installed_semantic_validators: Mapping[str, SemanticValidator] = field(
        init=False,
        repr=False,
        compare=False,
    )
    _semantic_registry_error: str | None = field(
        init=False,
        repr=False,
        compare=False,
    )
    _writer_hook_blockers: tuple[str, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )
    _installed_writer_hook_bindings: Mapping[str, WriterHookBinding] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.backend not in {"sqlite", "mariadb"}:
            raise ValueError(f"Unsupported generated schema backend: {self.backend!r}")
        _validate_embedded_artifact(self.backend)
        try:
            validators = _load_builtin_semantic_validators()
        except VNextSchemaProviderUnavailableError as error:
            validators = MappingProxyType({})
            registry_error: str | None = str(error)
        else:
            registry_error = None
        object.__setattr__(self, "_installed_semantic_validators", validators)
        object.__setattr__(self, "_semantic_registry_error", registry_error)
        writer_bindings, writer_blockers = _load_builtin_writer_hook_bindings()
        object.__setattr__(
            self,
            "_installed_writer_hook_bindings",
            writer_bindings,
        )
        object.__setattr__(self, "_writer_hook_blockers", writer_blockers)

    @property
    def generated_definition_data(self) -> Mapping[str, Any]:
        """Read-only generated backend payload, even while READY is blocked."""

        return _backend_payload(self.backend)

    @property
    def semantic_validators(self) -> Mapping[str, SemanticValidator]:
        """Exact immutable wheel-owned recurring semantic-validator registry.

        This property is also the stable hook named by the generated physical
        manifest.  It intentionally has no setter or constructor parameter.
        """

        if self._semantic_registry_error is not None:
            raise VNextSchemaProviderUnavailableError(self._semantic_registry_error)
        return self._installed_semantic_validators

    @property
    def writer_hook_bindings(self) -> Mapping[str, WriterHookBinding]:
        """Exact immutable wheel-owned transaction writer bindings.

        Unresolved recurring obligations are absent and remain represented by
        :attr:`blockers`.  The constructor deliberately accepts no registry or
        callback parameter.
        """

        return self._installed_writer_hook_bindings

    @property
    def blockers(self) -> tuple[str, ...]:
        payload = self.generated_definition_data
        result = list(_strings(payload.get("provider_blockers"), "provider blockers"))
        expected = tuple(
            dict.fromkeys(
                _obligation_ids_for_phase(SchemaSemanticValidationPhase.ACTIVATION)
                + _obligation_ids_for_phase(SchemaSemanticValidationPhase.READY)
            )
        )
        if self._semantic_registry_error is not None:
            result.append(self._semantic_registry_error)
        else:
            validators = self._installed_semantic_validators
            missing = tuple(value for value in expected if value not in validators)
            unexpected = tuple(sorted(set(validators) - set(expected)))
            if missing:
                result.append(
                    "executable semantic validators are missing for IDs: "
                    + ", ".join(repr(value) for value in missing)
                )
            if unexpected:
                result.append(
                    "executable semantic validators were supplied for undeclared "
                    "IDs: " + ", ".join(repr(value) for value in unexpected)
                )
        result.extend(self._writer_hook_blockers)
        return tuple(dict.fromkeys(result))

    @property
    def definition(self) -> SchemaEpochDefinition:
        self._require_available()
        payload = self.generated_definition_data
        obligation_manifest = ARTIFACT.get("obligation_manifest_sha256")
        if not isinstance(obligation_manifest, str):  # guarded, but fail closed
            raise VNextSchemaProviderUnavailableError(
                "The generated semantic-obligation manifest is absent"
            )
        seed_manifest = payload.get("seed_manifest_sha256")
        if not isinstance(seed_manifest, str):  # guarded, but fail closed
            raise VNextSchemaProviderUnavailableError(
                "The generated bootstrap-seed manifest is absent"
            )
        slices = tuple(
            SchemaSlice(
                slice_id,
                tuple(
                    SchemaCreateStatement(
                        statement_id,
                        sql,
                        SchemaObject(SchemaObjectKind(kind), name),
                    )
                    for statement_id, kind, name, sql in statements
                ),
            )
            for slice_id, statements in _slice_records(payload)
        )
        expected_objects = frozenset(
            SchemaObject(SchemaObjectKind(kind), name)
            for kind, name in _object_records(payload)
        )
        activation_obligation_ids = _obligation_ids_for_phase(
            SchemaSemanticValidationPhase.ACTIVATION
        )
        ready_obligation_ids = _obligation_ids_for_phase(
            SchemaSemanticValidationPhase.READY
        )
        bootstrap_seeds = tuple(
            SchemaSeedStatement(
                seed_id=_required_string(value, "seed_id", "bootstrap seed"),
                target_table=_required_string(value, "target_table", "bootstrap seed"),
                sql=_required_string(value, "sql", "bootstrap seed"),
                parameters=_seed_values(
                    value.get("parameters"), "bootstrap seed parameters"
                ),
            )
            for value in _dicts(payload.get("bootstrap_seeds"), "bootstrap seeds")
        )
        return SchemaEpochDefinition(
            epoch=_required_int(ARTIFACT, "epoch", "artifact"),
            schema_version=_required_int(ARTIFACT, "schema_version", "artifact"),
            ddl_manifest_sha256=_required_string(
                payload, "ddl_manifest_sha256", f"{self.backend} payload"
            ),
            seed_manifest_sha256=seed_manifest,
            obligation_manifest_sha256=obligation_manifest,
            expected_objects=expected_objects,
            slices=slices,
            bootstrap_seeds=bootstrap_seeds,
            activation_semantic_obligation_ids=activation_obligation_ids,
            ready_semantic_obligation_ids=ready_obligation_ids,
        )

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None:
        self._require_available()
        expected = dict(_slice_records(self.generated_definition_data))
        records = expected.get(schema_slice.slice_id)
        if records is None:
            raise SchemaEpochValidationError(
                f"Generated provider does not recognize slice {schema_slice.slice_id!r}"
            )
        actual_records = tuple(
            (
                statement.statement_id,
                statement.creates.kind.value,
                statement.creates.name,
                statement.sql,
            )
            for statement in schema_slice.statements
        )
        if actual_records != records:
            raise SchemaEpochValidationError(
                f"Schema slice {schema_slice.slice_id!r} differs from the generated "
                "artifact"
            )
        for _statement_id, kind, name, sql in records:
            self._validate_object_sql(connector, kind=kind, name=name, expected_sql=sql)
        relation_name = schema_slice.slice_id.removeprefix("relation:")
        metadata = _relation_by_name(self.generated_definition_data, relation_name)
        self._validate_relation_shape(connector, metadata)

    def validate_global(self, connector: SQLConnector) -> None:
        self._require_available()
        payload = self.generated_definition_data
        for _slice_id, records in _slice_records(payload):
            for _statement_id, kind, name, sql in records:
                self._validate_object_sql(
                    connector, kind=kind, name=name, expected_sql=sql
                )
        for relation in _dicts(payload.get("relations"), "generated relations"):
            self._validate_relation_shape(connector, relation)
        if self.backend == "sqlite":
            if connector.fetch_one("PRAGMA foreign_keys") != (1,):
                raise SchemaEpochValidationError(
                    "SQLite foreign-key enforcement is disabled"
                )
            # Do not run PRAGMA foreign_key_check here: READY revalidation must
            # remain bounded independently of corpus size. Exact FK definitions
            # are already checksum/DDL validated above, and every production
            # writer must use SQLite's enabled immediate FK enforcement.

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        self._require_available()
        return _validate_bootstrap_seed_records(
            connector, self.generated_definition_data
        )

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        self._require_available()
        validators = self.semantic_validators
        completed: list[str] = []
        for obligation_id in _obligation_ids_for_phase(phase):
            validator = validators.get(obligation_id)
            if validator is None:  # guarded, but never turn absence into success
                raise SchemaEpochValidationError(
                    f"No executable validator for semantic obligation {obligation_id!r}"
                )
            validator(connector)
            completed.append(obligation_id)
        return tuple(completed)

    def _require_available(self) -> None:
        blockers = self.blockers
        if blockers:
            raise VNextSchemaProviderUnavailableError(
                "The generated vNext schema provider is fail-closed:\n- "
                + "\n- ".join(blockers)
            )

    def _validate_object_sql(
        self,
        connector: SQLConnector,
        *,
        kind: str,
        name: str,
        expected_sql: str,
    ) -> None:
        if self.backend == "sqlite":
            row = connector.fetch_one(
                "SELECT type, sql FROM sqlite_master WHERE type = %s AND name = %s",
                (kind, name),
            )
            if len(row) != 2 or not isinstance(row[1], str):
                raise SchemaEpochValidationError(
                    f"SQLite generated {kind} {name!r} is missing or unreadable"
                )
            if _normalize_sqlite_ddl(str(row[1])) != _normalize_sqlite_ddl(
                expected_sql
            ):
                raise SchemaEpochValidationError(
                    f"SQLite generated {kind} {name!r} has the wrong definition"
                )
            return

        if kind == "table":
            row = connector.fetch_one(
                """
                SELECT TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (name,),
            )
            if row != ("BASE TABLE",):
                raise SchemaEpochValidationError(
                    f"MariaDB generated table {name!r} is missing or not a base table"
                )
        elif kind == "view":
            row = connector.fetch_one(
                """
                SELECT VIEW_DEFINITION, SECURITY_TYPE, CHECK_OPTION
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (name,),
            )
            if (
                len(row) != 3
                or not isinstance(row[0], str)
                or str(row[1]).upper() != "INVOKER"
                or str(row[2]).upper() != "NONE"
            ):
                raise SchemaEpochValidationError(
                    f"MariaDB generated view {name!r} is missing, not INVOKER, "
                    "or has a CHECK OPTION"
                )
            database_row = connector.fetch_one("SELECT DATABASE()")
            if len(database_row) != 1 or not isinstance(database_row[0], str):
                raise SchemaEpochValidationError(
                    "MariaDB current database name is missing or invalid"
                )
            expected_body = _mariadb_expected_view_body(expected_sql)
            actual_tokens = _mariadb_view_body_tokens(
                row[0], database_name=database_row[0]
            )
            expected_tokens = _mariadb_view_body_tokens(
                expected_body, database_name=database_row[0]
            )
            if actual_tokens != expected_tokens:
                raise SchemaEpochValidationError(
                    f"MariaDB generated view {name!r} has the wrong query body"
                )
        else:
            # MariaDB indexes are deliberately validated as part of their owning
            # table shape and never appear as top-level epoch catalog objects.
            raise SchemaEpochValidationError(
                f"Unsupported MariaDB top-level generated object kind {kind!r}"
            )

    def _validate_relation_shape(
        self, connector: SQLConnector, relation: Mapping[str, Any]
    ) -> None:
        if self.backend == "sqlite":
            # sqlite_master exact DDL validation above covers columns, collations,
            # keys, FKs, named CHECKs, indexes, and view body.  This extra column
            # assertion catches a provider metadata/statement disagreement.
            actual = tuple(
                str(row[1])
                for row in connector.fetch_all(
                    f"PRAGMA table_info({_quote_sqlite(str(relation['table']))})"
                )
            )
            expected = tuple(
                str(column[1])
                for column in _records(relation.get("columns"), "relation columns")
            )
            if actual != expected:
                raise SchemaEpochValidationError(
                    f"SQLite relation {relation['relation']!r} column order drifts"
                )
            return
        _validate_mariadb_relation(connector, relation)


def _validate_mariadb_relation(
    connector: SQLConnector, relation: Mapping[str, Any]
) -> None:
    table = str(relation["table"])
    rows = connector.fetch_all(
        """
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (table,),
    )
    actual_columns = tuple(
        (
            str(name),
            _normalize_type(str(type_name)),
            str(nullable).upper() == "YES",
            None if collation is None else str(collation).casefold(),
        )
        for name, type_name, nullable, collation in rows
    )
    expected_columns = tuple(
        (
            str(column[1]),
            _normalize_type(str(column[2])),
            bool(column[3]),
            None if column[4] is None else str(column[4]).casefold(),
        )
        for column in _records(relation.get("columns"), "relation columns")
    )
    if actual_columns != expected_columns:
        raise SchemaEpochValidationError(
            f"MariaDB relation {relation['relation']!r} column shape drifts: "
            f"actual={actual_columns!r} expected={expected_columns!r}"
        )
    if relation.get("kind") == "view":
        return

    index_rows = connector.fetch_all(
        """
        SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        (table,),
    )
    index_groups: dict[tuple[str, bool], list[tuple[int, str]]] = {}
    for index_name, non_unique, position, column_name in index_rows:
        key = (str(index_name), not bool(non_unique))
        index_groups.setdefault(key, []).append((int(position), str(column_name)))
    actual_indexes = {
        name: (tuple(value for _position, value in sorted(values)), unique)
        for (name, unique), values in index_groups.items()
    }
    expected_primary = _strings(relation.get("primary_key"), "primary key")
    if actual_indexes.get("PRIMARY") != (expected_primary, True):
        raise SchemaEpochValidationError(
            f"MariaDB relation {relation['relation']!r} primary key drifts"
        )
    for position, key in enumerate(
        _records(relation.get("unique_keys"), "unique keys"), 1
    ):
        name = _ddl_identifier(f"uk_{table}_{position}")
        expected = (tuple(str(value) for value in key), True)
        if actual_indexes.get(name) != expected:
            raise SchemaEpochValidationError(
                f"MariaDB relation {relation['relation']!r} unique key {name!r} drifts"
            )
    for name, columns, unique in _records(relation.get("indexes"), "indexes"):
        expected = (tuple(str(value) for value in columns), bool(unique))
        if actual_indexes.get(str(name)) != expected:
            raise SchemaEpochValidationError(
                f"MariaDB relation {relation['relation']!r} index {name!r} drifts"
            )

    foreign_key_rows = connector.fetch_all(
        """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION,
               REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
        """,
        (table,),
    )
    fk_groups: dict[str, list[tuple[int, str, str, str]]] = {}
    for name, column, position, target_table, target_column in foreign_key_rows:
        fk_groups.setdefault(str(name), []).append(
            (int(position), str(column), str(target_table), str(target_column))
        )
    actual_foreign_keys = {
        name: (
            tuple(value[1] for value in sorted(values)),
            sorted(values)[0][2],
            tuple(value[3] for value in sorted(values)),
        )
        for name, values in fk_groups.items()
    }
    expected_foreign_keys = {
        str(name): (
            tuple(str(value) for value in columns),
            str(target_table),
            tuple(str(value) for value in target_columns),
        )
        for name, columns, target_table, target_columns in _records(
            relation.get("foreign_keys"), "foreign keys"
        )
    }
    if actual_foreign_keys != expected_foreign_keys:
        raise SchemaEpochValidationError(
            f"MariaDB relation {relation['relation']!r} foreign keys drift"
        )
    expected_index_names = {
        "PRIMARY",
        *(
            _ddl_identifier(f"uk_{table}_{position}")
            for position, _key in enumerate(
                _records(relation.get("unique_keys"), "unique keys"), 1
            )
        ),
        *(
            str(name)
            for name, _columns, _unique in _records(relation.get("indexes"), "indexes")
        ),
    }
    # InnoDB may synthesize a child-side index named after the FK constraint
    # when no declared access path already covers it.  This is the sole
    # backend-owned index shape accepted outside the generated manifest.
    for foreign_key_name, (
        columns,
        _target,
        _target_columns,
    ) in expected_foreign_keys.items():
        implicit = actual_indexes.get(foreign_key_name)
        if implicit is None:
            continue
        if implicit != (columns, False):
            raise SchemaEpochValidationError(
                f"MariaDB relation {relation['relation']!r} implicit FK index "
                f"{foreign_key_name!r} has the wrong shape"
            )
        expected_index_names.add(foreign_key_name)
    unexpected_indexes = set(actual_indexes) - expected_index_names
    if unexpected_indexes:
        raise SchemaEpochValidationError(
            f"MariaDB relation {relation['relation']!r} has indexes outside the "
            f"generated/constraint-owned shape: {sorted(unexpected_indexes)!r}"
        )

    check_rows = connector.fetch_all(
        """
        SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc
          ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
         AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
          AND tc.TABLE_NAME = %s
          AND tc.CONSTRAINT_TYPE = 'CHECK'
        ORDER BY tc.CONSTRAINT_NAME
        """,
        (table,),
    )
    actual_checks = {
        str(name): _normalize_check(str(expression)) for name, expression in check_rows
    }
    expected_checks = {
        str(name): _normalize_check(str(expression))
        for name, expression in _records(relation.get("checks"), "checks")
    }
    if actual_checks != expected_checks:
        raise SchemaEpochValidationError(
            f"MariaDB relation {relation['relation']!r} CHECK constraints drift"
        )


_MARIADB_VIEW_TOKEN = re.compile(
    r"""
    `(?:``|[^`])*`
    |'(?:''|\\.|[^'])*'
    |\b(?:0x[0-9a-fA-F]+|[xX]'[0-9a-fA-F]*')\b
    |\b[0-9]+(?:\.[0-9]+)?\b
    |\b[A-Za-z_][A-Za-z0-9_$]*\b
    |<=>|<>|!=|<=|>=|:=|&&|\|\|
    |[(),.=<>+*/%!-]
    """,
    re.VERBOSE,
)


def _mariadb_expected_view_body(expected_sql: str) -> str:
    """Extract the SELECT body from one generated MariaDB CREATE VIEW.

    Generated views always declare an explicit column list, so the closing
    parenthesis followed by ``AS SELECT`` is an unambiguous trust boundary.
    Accepting a looser first-``AS`` split would accidentally stop at a column
    or table alias if the renderer prefix ever changes.
    """

    match = re.search(r"\)\s+AS\s+(?=SELECT\b)", expected_sql, re.IGNORECASE)
    if match is None:
        raise VNextSchemaArtifactDriftError(
            "Generated MariaDB view has no explicit column-list AS SELECT body"
        )
    return expected_sql[match.end() :].strip().removesuffix(";").strip()


def _mariadb_view_body_tokens(
    definition: str,
    *,
    database_name: str,
) -> tuple[str, ...]:
    """Canonicalize a MariaDB view body without weakening its predicates.

    MariaDB persists the same SELECT with server-added current-schema
    qualification, identifier quoting, keyword casing, optional ``AS`` tokens
    for aliases, redundant whole-clause parentheses, ``!`` for ``NOT EXISTS``,
    and ``LIMIT 1`` inside ``EXISTS`` subqueries.  Only those structurally
    proven, semantics-preserving presentation differences are removed.  Every
    other identifier, literal, operator, comma, and parenthesis is retained,
    so a same-column view with a different nearest-ancestor predicate fails the
    READY structural gate.
    """

    if not database_name:
        raise SchemaEpochValidationError("MariaDB current database name is empty")
    raw_tokens = _MARIADB_VIEW_TOKEN.findall(definition)
    residue = _MARIADB_VIEW_TOKEN.sub("", definition)
    if residue.strip():
        raise SchemaEpochValidationError(
            "MariaDB view definition contains unsupported SQL tokens"
        )

    normalized: list[str] = []
    for token in raw_tokens:
        if token.startswith("`"):
            value = token[1:-1].replace("``", "`").casefold()
            normalized.append(f"identifier:{value}")
        elif token.startswith("'") or re.fullmatch(r"[xX]'[0-9a-fA-F]*'", token):
            normalized.append(f"literal:{token}")
        elif re.fullmatch(r"0x[0-9a-fA-F]+", token):
            normalized.append(f"literal:{token.casefold()}")
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", token):
            normalized.append(f"identifier:{token.casefold()}")
        elif token == "!=":
            normalized.append("<>")
        else:
            normalized.append(token)

    # SHOW/INFORMATION_SCHEMA qualifies base relations with the current
    # database.  Generated DDL deliberately does not because the epoch is
    # database-local.  Strip only this exact qualifier, never arbitrary
    # two-part identifiers.
    database_token = f"identifier:{database_name.casefold()}"
    without_database: list[str] = []
    position = 0
    while position < len(normalized):
        if (
            position + 2 < len(normalized)
            and normalized[position] == database_token
            and normalized[position + 1] == "."
            and normalized[position + 2].startswith("identifier:")
        ):
            position += 2
            continue
        without_database.append(normalized[position])
        position += 1

    # MariaDB may omit the optional AS keyword before aliases when it stores a
    # view.  Removing AS everywhere is semantics-preserving for the SELECT
    # subset emitted by our closed renderer and still preserves both the
    # expression and alias tokens.
    without_alias_as = tuple(
        token for token in without_database if token != "identifier:as"
    )
    return _canonicalize_mariadb_view_presentation(without_alias_as)


def _canonicalize_mariadb_view_presentation(
    tokens: tuple[str, ...],
) -> tuple[str, ...]:
    """Remove only MariaDB's structurally recognizable SELECT rewrites.

    MariaDB 11 canonicalizes generated views in three additional ways:

    * ``NOT EXISTS`` is stored as ``!exists``;
    * a complete joined ``FROM`` table-reference and its complete ``ON``
      predicate are wrapped in parentheses; and
    * an exact trailing ``LIMIT 1`` is added to ``EXISTS (SELECT ...)``.

    Parenthesis pairs are validated before any rewrite.  A wrapper is removed
    only when it encloses the complete clause, and ``LIMIT 1`` is removed only
    at the tail of a directly ``EXISTS``-owned SELECT.  This deliberately does
    not normalize arbitrary parentheses or limits.
    """

    canonical = list(tokens)
    for position, token in enumerate(canonical[:-1]):
        if token == "!" and canonical[position + 1] == "identifier:exists":
            canonical[position] = "identifier:not"

    pairs = _mariadb_parenthesis_pairs(canonical)
    removed: set[int] = set()
    for opening, closing in pairs.items():
        if _mariadb_is_complete_join_wrapper(canonical, opening, closing):
            removed.update((opening, closing))
        elif _mariadb_is_complete_on_wrapper(canonical, opening, closing):
            removed.update((opening, closing))

        if (
            opening > 0
            and canonical[opening - 1] == "identifier:exists"
            and opening + 1 < closing
            and canonical[opening + 1] == "identifier:select"
            and closing - opening >= 4
            and canonical[closing - 2 : closing] == ["identifier:limit", "1"]
        ):
            removed.update((closing - 2, closing - 1))

    return tuple(
        token for position, token in enumerate(canonical) if position not in removed
    )


def _mariadb_parenthesis_pairs(tokens: Sequence[str]) -> dict[int, int]:
    stack: list[int] = []
    result: dict[int, int] = {}
    for position, token in enumerate(tokens):
        if token == "(":
            stack.append(position)
        elif token == ")":
            if not stack:
                raise SchemaEpochValidationError(
                    "MariaDB view definition has unbalanced parentheses"
                )
            result[stack.pop()] = position
    if stack:
        raise SchemaEpochValidationError(
            "MariaDB view definition has unbalanced parentheses"
        )
    return result


_MARIADB_COMPLETE_CLAUSE_FOLLOWERS = frozenset(
    {
        "identifier:where",
        "identifier:group",
        "identifier:having",
        "identifier:order",
        "identifier:limit",
        "identifier:union",
        "identifier:join",
        "identifier:left",
        "identifier:right",
        "identifier:inner",
        "identifier:cross",
        ")",
    }
)


def _mariadb_wrapper_has_complete_clause_boundary(
    tokens: Sequence[str], closing: int
) -> bool:
    return closing + 1 == len(tokens) or (
        closing + 1 < len(tokens)
        and tokens[closing + 1] in _MARIADB_COMPLETE_CLAUSE_FOLLOWERS
    )


def _mariadb_is_complete_join_wrapper(
    tokens: Sequence[str], opening: int, closing: int
) -> bool:
    if (
        opening == 0
        or tokens[opening - 1] != "identifier:from"
        or opening + 1 == closing
        or tokens[opening + 1] == "identifier:select"
        or not _mariadb_wrapper_has_complete_clause_boundary(tokens, closing)
    ):
        return False

    depth = 0
    has_top_level_join = False
    for token in tokens[opening + 1 : closing]:
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0:
            if token == ",":
                return False
            if token == "identifier:join":
                has_top_level_join = True
    return has_top_level_join


def _mariadb_is_complete_on_wrapper(
    tokens: Sequence[str], opening: int, closing: int
) -> bool:
    return (
        opening > 0
        and tokens[opening - 1] == "identifier:on"
        and opening + 1 < closing
        and tokens[opening + 1] != "identifier:select"
        and _mariadb_wrapper_has_complete_clause_boundary(tokens, closing)
    )


def _validate_bootstrap_seed_records(
    connector: SQLConnector, payload: Mapping[str, Any]
) -> tuple[str, ...]:
    completed: list[str] = []
    for seed in _dicts(payload.get("bootstrap_seeds"), "bootstrap seeds"):
        seed_id = _required_string(seed, "seed_id", "bootstrap seed")
        rows = connector.fetch_all(
            _required_string(seed, "validation_sql", f"bootstrap seed {seed_id!r}"),
            _seed_values(
                seed.get("validation_parameters"),
                f"bootstrap seed {seed_id!r} validation parameters",
            ),
        )
        expected_row = _seed_values(
            seed.get("expected_row"),
            f"bootstrap seed {seed_id!r} expected row",
        )
        if rows != [expected_row]:
            raise SchemaEpochValidationError(
                f"Bootstrap seed {seed_id!r} differs from its exact generated row: "
                f"actual={rows!r} expected={[expected_row]!r}"
            )
        completed.append(seed_id)

    for relation in _dicts(
        payload.get("bootstrap_seeded_relations"), "bootstrap seeded relations"
    ):
        relation_name = _required_string(
            relation, "relation", "bootstrap seeded relation"
        )
        actual_rows = tuple(
            _seed_values(tuple(row), f"bootstrap relation {relation_name!r} row")
            for row in connector.fetch_all(
                _required_string(
                    relation,
                    "validation_sql",
                    f"bootstrap seeded relation {relation_name!r}",
                )
            )
        )
        expected_rows = tuple(
            _seed_values(row, f"bootstrap relation {relation_name!r} expected row")
            for row in _records(
                relation.get("expected_rows"),
                f"bootstrap relation {relation_name!r} expected rows",
            )
        )
        if Counter(actual_rows) != Counter(expected_rows):
            raise SchemaEpochValidationError(
                f"Bootstrap relation {relation_name!r} does not contain exactly "
                f"its generated genesis rows: actual={actual_rows!r} "
                f"expected={expected_rows!r}"
            )

    for relation in _dicts(
        payload.get("bootstrap_absent_relations"), "bootstrap absent relations"
    ):
        relation_name = _required_string(
            relation, "relation", "bootstrap absent relation"
        )
        rows = connector.fetch_all(
            _required_string(
                relation,
                "validation_sql",
                f"bootstrap absent relation {relation_name!r}",
            )
        )
        if rows:
            raise SchemaEpochValidationError(
                f"Bootstrap-absent relation {relation_name!r} contains data"
            )
    return tuple(completed)


def _load_builtin_semantic_validators() -> Mapping[str, SemanticValidator]:
    """Load and freeze only the two wheel-owned validator registries.

    Imports are deliberately package-local and lazy.  Neither the repository's
    ``verification`` tree nor a caller callback participates in runtime
    dispatch.  Registry overlap, malformed entries, and loader errors all make
    the provider unavailable before a database connector is opened.
    """

    try:
        from .catalog_refinement import builtin_semantic_validators
        from .operational_refinement import (
            builtin_operational_semantic_validators,
        )

        registries: tuple[tuple[str, object], ...] = (
            ("catalog", builtin_semantic_validators()),
            ("operational", builtin_operational_semantic_validators()),
        )
        installed: dict[str, SemanticValidator] = {}
        owners: dict[str, str] = {}
        for registry_name, registry in registries:
            if not isinstance(registry, Mapping):
                raise TypeError(
                    f"{registry_name} semantic-validator registry is not a mapping"
                )
            for obligation_id, validator in registry.items():
                if not isinstance(obligation_id, str) or not obligation_id:
                    raise TypeError(
                        f"{registry_name} semantic-validator registry has an "
                        "invalid obligation ID"
                    )
                if not callable(validator):
                    raise TypeError(
                        f"semantic validator for {obligation_id!r} is not callable"
                    )
                previous_owner = owners.get(obligation_id)
                if previous_owner is not None:
                    raise ValueError(
                        f"semantic obligation {obligation_id!r} is supplied by both "
                        f"{previous_owner} and {registry_name} registries"
                    )
                owners[obligation_id] = registry_name
                installed[obligation_id] = validator
    except VNextSchemaProviderUnavailableError:
        raise
    except Exception as error:
        raise VNextSchemaProviderUnavailableError(
            "The wheel-owned semantic-validator registries could not be loaded "
            f"exactly: {type(error).__name__}: {error}"
        ) from error
    return MappingProxyType(installed)


def _load_builtin_writer_hook_bindings() -> (
    tuple[Mapping[str, WriterHookBinding], tuple[str, ...]]
):
    """Resolve every recurring hook through the closed wheel registry.

    BUILDING-only bootstrap hooks are excluded: generated seed insertion plus
    exact bootstrap validation is their executable path.  Every ACTIVATION or
    READY obligation, however, must also have an exact immutable transaction
    binding before this provider may publish ``READY``.
    """

    from .catalog_writer import (
        WriterHookBinding,
        WriterHookUnavailableError,
        resolve_writer_hook,
        validate_resolved_writer_hook_binding,
    )

    recurring_ids = tuple(
        dict.fromkeys(
            _obligation_ids_for_phase(SchemaSemanticValidationPhase.ACTIVATION)
            + _obligation_ids_for_phase(SchemaSemanticValidationPhase.READY)
        )
    )
    if len(recurring_ids) != len(set(recurring_ids)):
        raise VNextSchemaArtifactDriftError(
            "Generated recurring semantic-obligation IDs are not unique"
        )
    recurring_id_set = set(recurring_ids)
    obligations = tuple(
        obligation
        for obligation in _dicts(
            ARTIFACT.get("semantic_obligations"), "semantic obligations"
        )
        if _required_string(obligation, "id", "semantic obligation") in recurring_id_set
    )
    actual_ids = tuple(
        _required_string(obligation, "id", "semantic obligation")
        for obligation in obligations
    )
    if actual_ids != recurring_ids:
        raise VNextSchemaArtifactDriftError(
            "Generated recurring writer-hook obligations are not closed-world exact"
        )

    resolver: Callable[[str, str, int], object] = resolve_writer_hook
    installed: dict[str, WriterHookBinding] = {}
    blockers: list[str] = []
    for obligation in obligations:
        obligation_id = _required_string(obligation, "id", "semantic obligation")
        contract = obligation.get("contract")
        if not isinstance(contract, Mapping):
            raise VNextSchemaArtifactDriftError(
                f"Generated semantic obligation {obligation_id!r}.contract must "
                "be a mapping"
            )
        hook_name = _required_string(
            contract,
            "writer_hook",
            f"semantic obligation {obligation_id!r}.contract",
        )
        hook_version = _required_int(
            contract,
            "writer_hook_version",
            f"semantic obligation {obligation_id!r}.contract",
        )
        hook_label = (
            f"semantic obligation {obligation_id!r} writer hook "
            f"{hook_name!r} v{hook_version}"
        )
        try:
            resolved = resolver(obligation_id, hook_name, hook_version)
        except WriterHookUnavailableError as error:
            blockers.append(f"{hook_label} is unavailable: {error}")
        except Exception as error:
            blockers.append(
                f"{hook_label} failed closed during resolution: "
                f"{type(error).__name__}: {error}"
            )
        else:
            try:
                validate_resolved_writer_hook_binding(
                    resolved,
                    obligation_id=obligation_id,
                    name=hook_name,
                    version=hook_version,
                )
            except WriterHookUnavailableError as error:
                blockers.append(
                    f"{hook_label} failed closed during exact binding validation: "
                    f"{error}"
                )
            except Exception as error:
                blockers.append(
                    f"{hook_label} failed closed during exact binding validation: "
                    f"{type(error).__name__}: {error}"
                )
            else:
                binding = cast(WriterHookBinding, resolved)
                if obligation_id in installed:
                    blockers.append(
                        f"{hook_label} resolved a duplicate obligation binding"
                    )
                else:
                    installed[obligation_id] = binding
    return MappingProxyType(installed), tuple(blockers)


def _validate_embedded_artifact(backend: Backend) -> None:
    if ARTIFACT.get("artifact_version") != 1:
        raise VNextSchemaArtifactDriftError(
            f"Unsupported generated artifact version: {ARTIFACT.get('artifact_version')!r}"
        )
    payload = _backend_payload(backend)
    expected = _framed_hash(
        "h2hdb-vnext-provider-ddl-v1",
        (
            {
                "epoch": _required_int(ARTIFACT, "epoch", "artifact"),
                "schema_version": _required_int(ARTIFACT, "schema_version", "artifact"),
                "backend": backend,
            },
            payload.get("epoch_control"),
            _dicts(payload.get("relations"), "generated relations"),
            _slice_records(payload),
            tuple(sorted(_object_records(payload))),
        ),
    )
    actual = _required_string(payload, "ddl_manifest_sha256", f"{backend} payload")
    if actual != expected:
        raise VNextSchemaArtifactDriftError(
            f"Generated {backend} DDL manifest is internally inconsistent"
        )
    provenance = _records(ARTIFACT.get("source_provenance"), "source provenance")
    for record in provenance:
        if (
            len(record) != 2
            or not isinstance(record[0], str)
            or not isinstance(record[1], str)
            or re.fullmatch(r"[0-9a-f]{64}", record[1]) is None
        ):
            raise VNextSchemaArtifactDriftError(
                "Generated source-provenance record is malformed"
            )
    expected_source_manifest = _framed_hash(
        "h2hdb-vnext-provider-sources-v1", provenance
    )
    if ARTIFACT.get("source_manifest_sha256") != expected_source_manifest:
        raise VNextSchemaArtifactDriftError(
            "Generated source-provenance manifest is internally inconsistent"
        )
    obligations = _dicts(ARTIFACT.get("semantic_obligations"), "semantic obligations")
    obligation_manifest = ARTIFACT.get("obligation_manifest_sha256")
    if obligations:
        expected_obligations = _framed_hash(
            "h2hdb-vnext-provider-obligations-v1", obligations
        )
        if obligation_manifest != expected_obligations:
            raise VNextSchemaArtifactDriftError(
                "Generated semantic-obligation manifest is internally inconsistent"
            )
    elif obligation_manifest is not None:
        raise VNextSchemaArtifactDriftError(
            "Generated artifact has an obligation hash without obligation records"
        )

    formal_seeds = _dicts(ARTIFACT.get("bootstrap_seeds"), "formal bootstrap seeds")
    bootstrap_contracts = _dicts(
        ARTIFACT.get("bootstrap_contracts"), "bootstrap contracts"
    )
    rendered_seeds = _dicts(payload.get("bootstrap_seeds"), "bootstrap seeds")
    seeded_relations = _dicts(
        payload.get("bootstrap_seeded_relations"), "bootstrap seeded relations"
    )
    absent_relations = _dicts(
        payload.get("bootstrap_absent_relations"), "bootstrap absent relations"
    )
    formal_seed_ids = tuple(
        _required_string(value, "id", "formal bootstrap seed") for value in formal_seeds
    )
    rendered_seed_ids = tuple(
        _required_string(value, "seed_id", "bootstrap seed") for value in rendered_seeds
    )
    if formal_seed_ids != rendered_seed_ids:
        raise VNextSchemaArtifactDriftError(
            "Generated bootstrap seed order/coverage differs from formal records"
        )
    for seed in rendered_seeds:
        _required_string(seed, "target_relation", "bootstrap seed")
        _required_string(seed, "target_table", "bootstrap seed")
        _required_string(seed, "sql", "bootstrap seed")
        _required_string(seed, "validation_sql", "bootstrap seed")
        parameters = _seed_values(seed.get("parameters"), "bootstrap parameters")
        _seed_values(
            seed.get("validation_parameters"), "bootstrap validation parameters"
        )
        expected_row = _seed_values(seed.get("expected_row"), "bootstrap expected row")
        if parameters != expected_row:
            raise VNextSchemaArtifactDriftError(
                f"Generated bootstrap seed {seed['seed_id']!r} insertion and "
                "validation rows differ"
            )
    seed_manifest = payload.get("seed_manifest_sha256")
    if rendered_seeds:
        expected_seed_manifest = _framed_hash(
            "h2hdb-vnext-provider-seeds-v1",
            (
                {
                    "epoch": _required_int(ARTIFACT, "epoch", "artifact"),
                    "schema_version": _required_int(
                        ARTIFACT, "schema_version", "artifact"
                    ),
                    "backend": backend,
                },
                formal_seeds,
                bootstrap_contracts,
                rendered_seeds,
                seeded_relations,
                absent_relations,
            ),
        )
        if seed_manifest != expected_seed_manifest:
            raise VNextSchemaArtifactDriftError(
                f"Generated {backend} bootstrap-seed manifest is internally "
                "inconsistent"
            )
    elif seed_manifest is not None:
        raise VNextSchemaArtifactDriftError(
            "Generated artifact has a seed hash without bootstrap seed records"
        )


def _backend_payload(backend: Backend) -> Mapping[str, Any]:
    backends = ARTIFACT.get("backends")
    if not isinstance(backends, dict):
        raise VNextSchemaArtifactDriftError("Generated artifact lacks backend payloads")
    payload = backends.get(backend)
    if not isinstance(payload, dict):
        raise VNextSchemaArtifactDriftError(
            f"Generated artifact lacks backend {backend!r}"
        )
    return payload


def _obligation_ids_for_phase(
    phase: SchemaSemanticValidationPhase,
) -> tuple[str, ...]:
    allowed_lifecycles = (
        {"ready_validation", "building_to_ready", "ready_and_runtime"}
        if phase is SchemaSemanticValidationPhase.ACTIVATION
        else {"ready_validation", "ready_and_runtime"}
    )
    result: list[str] = []
    for obligation in _dicts(
        ARTIFACT.get("semantic_obligations"), "semantic obligations"
    ):
        obligation_id = _required_string(obligation, "id", "semantic obligation")
        contract = obligation.get("contract")
        if not isinstance(contract, Mapping):
            raise VNextSchemaArtifactDriftError(
                f"Generated semantic obligation {obligation_id!r}.contract must "
                "be a mapping"
            )
        lifecycle = _required_string(
            contract, "lifecycle", f"semantic obligation {obligation_id!r}.contract"
        )
        if lifecycle not in {
            "building_only",
            "building_to_ready",
            "ready_validation",
            "ready_and_runtime",
        }:
            raise VNextSchemaArtifactDriftError(
                f"Semantic obligation {obligation_id!r} has unsupported lifecycle "
                f"{lifecycle!r}"
            )
        if lifecycle in allowed_lifecycles:
            result.append(obligation_id)
    return tuple(result)


def _relation_by_name(
    payload: Mapping[str, Any], relation_name: str
) -> Mapping[str, Any]:
    matches = tuple(
        relation
        for relation in _dicts(payload.get("relations"), "generated relations")
        if relation.get("relation") == relation_name
    )
    if len(matches) != 1:
        raise VNextSchemaArtifactDriftError(
            f"Generated relation metadata for {relation_name!r} is not singular"
        )
    return matches[0]


def _slice_records(
    payload: Mapping[str, Any],
) -> tuple[tuple[str, tuple[tuple[str, str, str, str], ...]], ...]:
    result: list[tuple[str, tuple[tuple[str, str, str, str], ...]]] = []
    for raw_slice in _records(payload.get("slices"), "schema slices"):
        if len(raw_slice) != 2 or not isinstance(raw_slice[0], str):
            raise VNextSchemaArtifactDriftError("Generated schema slice is malformed")
        statements: list[tuple[str, str, str, str]] = []
        for statement in _records(raw_slice[1], "schema statements"):
            if len(statement) != 4:
                raise VNextSchemaArtifactDriftError(
                    f"Generated schema slice {raw_slice[0]!r} has malformed statements"
                )
            statements.append(
                (
                    str(statement[0]),
                    str(statement[1]),
                    str(statement[2]),
                    str(statement[3]),
                )
            )
        result.append((raw_slice[0], tuple(statements)))
    return tuple(result)


def _object_records(payload: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for record in _records(payload.get("expected_objects"), "expected objects"):
        if len(record) != 2:
            raise VNextSchemaArtifactDriftError(
                "Generated expected object is malformed"
            )
        result.append((str(record[0]), str(record[1])))
    return tuple(result)


def _dicts(value: object, context: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, tuple) or not all(
        isinstance(item, dict) for item in value
    ):
        raise VNextSchemaArtifactDriftError(f"Generated {context} is malformed")
    return value


def _records(value: object, context: str) -> tuple[tuple[Any, ...], ...]:
    if not isinstance(value, tuple) or not all(
        isinstance(item, tuple) for item in value
    ):
        raise VNextSchemaArtifactDriftError(f"Generated {context} is malformed")
    return value


def _strings(value: object, context: str) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise VNextSchemaArtifactDriftError(f"Generated {context} is malformed")
    return value


def _seed_values(value: object, context: str) -> tuple[bytes | int | str | None, ...]:
    if not isinstance(value, tuple) or any(
        isinstance(item, bool) or not isinstance(item, (bytes, int, str, type(None)))
        for item in value
    ):
        raise VNextSchemaArtifactDriftError(
            f"Generated {context} contains a non-canonical seed value"
        )
    return value


def _required_string(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise VNextSchemaArtifactDriftError(
            f"Generated {context}.{key} must be a non-empty string"
        )
    return result


def _required_int(value: Mapping[str, Any], key: str, context: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise VNextSchemaArtifactDriftError(
            f"Generated {context}.{key} must be an integer"
        )
    return result


def _framed_hash(domain: str, values: Sequence[object]) -> str:
    digest = hashlib.sha256()
    digest.update(domain.encode("ascii"))
    digest.update(b"\0")
    for value in values:
        payload = json.dumps(
            value,
            default=_json_framing_default,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _json_framing_default(value: object) -> object:
    if isinstance(value, bytes):
        return {"$bytes_hex": value.hex()}
    raise TypeError(f"unsupported manifest value: {type(value).__name__}")


def _normalize_type(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip()).upper()
    return re.sub(
        r"\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)",
        r"\1",
        normalized,
    )


def _normalize_sqlite_ddl(value: str) -> str:
    value = value.strip().rstrip(";")
    value = re.sub(r"\bIF\s+NOT\s+EXISTS\b\s*", "", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def _normalize_check(value: str) -> str:
    value = _normalize_mariadb_not_equal_operator(value)
    value = re.sub(r"\s+", " ", value.replace("`", "").strip()).casefold()
    value = re.sub(r"\s*,\s*", ",", value)
    value = re.sub(r"\(\s+", "(", value)
    value = re.sub(r"\s+\)", ")", value)
    while value.startswith("(") and value.endswith(")"):
        inner = value[1:-1].strip()
        if not _balanced_parentheses(inner):
            break
        value = inner
    return value


def _normalize_mariadb_not_equal_operator(value: str) -> str:
    """Canonicalize MariaDB's ``!=`` rewrite without touching literals."""

    result: list[str] = []
    position = 0
    quote: str | None = None
    while position < len(value):
        character = value[position]
        if quote is not None:
            result.append(character)
            if character == "\\" and position + 1 < len(value):
                position += 1
                result.append(value[position])
            elif character == quote:
                if position + 1 < len(value) and value[position + 1] == quote:
                    position += 1
                    result.append(value[position])
                else:
                    quote = None
        elif character in {"'", '"'}:
            quote = character
            result.append(character)
        elif value.startswith("!=", position):
            result.append("<>")
            position += 1
        else:
            result.append(character)
        position += 1
    return "".join(result)


def _balanced_parentheses(value: str) -> bool:
    depth = 0
    quote: str | None = None
    for character in value:
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and quote is None


def _quote_sqlite(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
