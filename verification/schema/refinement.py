"""Compare the logical vNext schema contract with an introspected SQL schema.

This module deliberately does not mutate or migrate a database.  It normalizes
SQLite and MariaDB metadata into one small model and reports whether selected
logical relations are realized by physical tables.  The vNext manifest and the
currently deployed schema are expected to differ; callers should inspect the
structured mismatch report rather than hiding that difference with ``xfail``.
"""

from __future__ import annotations

import re
import tomllib
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class MetadataReader(Protocol):
    """Read-only subset shared by the h2hdb SQLite and MariaDB connectors."""

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]: ...


@dataclass(frozen=True, order=True)
class ForeignKeyShape:
    columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]


@dataclass(frozen=True, order=True)
class ColumnShape:
    name: str
    type_name: str
    nullable: bool
    collation: str | None


@dataclass(frozen=True, order=True)
class IndexShape:
    name: str
    columns: tuple[str, ...]
    unique: bool


@dataclass(frozen=True, order=True)
class CheckShape:
    name: str
    expression: str


@dataclass(frozen=True)
class TableShape:
    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...]
    foreign_keys: tuple[ForeignKeyShape, ...]
    column_shapes: tuple[ColumnShape, ...] = ()
    indexes: tuple[IndexShape, ...] = ()
    checks: tuple[CheckShape, ...] = ()
    kind: str = "table"

    @property
    def candidate_keys(self) -> tuple[frozenset[str], ...]:
        """Return inclusion-minimal physical PK/UNIQUE column sets."""

        keys = [frozenset(key) for key in (self.primary_key, *self.unique_keys) if key]
        return tuple(_minimal_sets(keys))

    def column(self, name: str) -> ColumnShape | None:
        return next(
            (column for column in self.column_shapes if column.name == name),
            None,
        )


@dataclass(frozen=True)
class DatabaseShape:
    backend: str
    tables: tuple[TableShape, ...]

    def table(self, name: str) -> TableShape | None:
        return next((table for table in self.tables if table.name == name), None)


@dataclass(frozen=True)
class LogicalForeignKey:
    attributes: tuple[str, ...]
    referenced_relation: str
    referenced_attributes: tuple[str, ...]


@dataclass(frozen=True)
class LogicalRelation:
    name: str
    attributes: tuple[str, ...]
    candidate_keys: tuple[frozenset[str], ...]
    foreign_keys: tuple[LogicalForeignKey, ...]


@dataclass(frozen=True)
class LogicalSchema:
    name: str
    relations: tuple[LogicalRelation, ...]

    def relation(self, name: str) -> LogicalRelation | None:
        return next(
            (relation for relation in self.relations if relation.name == name),
            None,
        )


@dataclass(frozen=True)
class BackendColumnSpec:
    type_name: str
    nullable: bool
    collation: str | None


@dataclass(frozen=True)
class PhysicalColumnSpec:
    attribute: str
    column: str
    sqlite: BackendColumnSpec
    mariadb: BackendColumnSpec

    def for_backend(self, backend: str) -> BackendColumnSpec:
        if backend == "sqlite":
            return self.sqlite
        if backend == "mariadb":
            return self.mariadb
        raise ValueError(f"Unsupported physical-schema backend: {backend!r}")


@dataclass(frozen=True)
class PhysicalForeignKeySpec:
    name: str
    attributes: tuple[str, ...]
    referenced_relation: str
    referenced_attributes: tuple[str, ...]


@dataclass(frozen=True)
class PhysicalIndexSpec:
    name: str
    attributes: tuple[str, ...]
    unique: bool


@dataclass(frozen=True)
class PhysicalCheckSpec:
    name: str
    sqlite_expression: str
    mariadb_expression: str

    def expression_for(self, backend: str) -> str:
        if backend == "sqlite":
            return self.sqlite_expression
        if backend == "mariadb":
            return self.mariadb_expression
        raise ValueError(f"Unsupported physical-schema backend: {backend!r}")


@dataclass(frozen=True)
class PhysicalCanonicalDigestProtocol:
    policy_relation: str
    value_relation: str
    digest_attribute: str
    allocation_relation: str
    page_relation: str
    descriptor_relation: str
    parent_relation: str
    root_attribute: str
    byte_count_attribute: str
    algorithm: str
    framing: str
    enforcement: str


@dataclass(frozen=True)
class PhysicalCanonicalValuePageProtocol:
    codec_version: int
    prefix: str
    maximum_page_bytes: int
    chunk_maximum_bytes: int
    branch_capacity: int
    maximum_level: int
    maximum_byte_count: int
    framing: str
    enforcement: str


@dataclass(frozen=True)
class PhysicalSourceLocatorProtocol:
    identity_relation: str
    gallery_relation: str
    digest_attribute: str
    name_attribute: str
    canonical_value_relation: str
    digest_domain: str
    framing: str
    enforcement: str


@dataclass(frozen=True)
class PhysicalBoundedValue:
    attribute: str
    maximum_bytes: int
    encoding: str
    source: str
    runtime_obligation: str


@dataclass(frozen=True)
class OverlayViewSpec:
    """Declarative nearest-ancestor overlay view.

    The view is deliberately represented as a view rather than as a table with
    fictitious PK/FK metadata.  Its business key and referential guarantees are
    checked against the logical contract at specification-load time; database
    introspection separately proves that the rendered object really is a view
    with the exact projected columns.
    """

    ancestry_relation: str
    shadow_relation: str
    tombstone_relation: str


@dataclass(frozen=True)
class PhysicalRelationSpec:
    relation: str
    status: str
    rationale: str
    table: str | None = None
    columns: tuple[PhysicalColumnSpec, ...] = ()
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    runtime_unique_keys: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[PhysicalForeignKeySpec, ...] = ()
    required_indexes: tuple[PhysicalIndexSpec, ...] = ()
    checks: tuple[PhysicalCheckSpec, ...] = ()
    kind: str = "table"
    overlay_view: OverlayViewSpec | None = None

    def column_for(self, attribute: str) -> str:
        column = next(
            (column for column in self.columns if column.attribute == attribute),
            None,
        )
        if column is None:
            raise KeyError(attribute)
        return column.column

    def as_mapping(self) -> RelationMapping:
        if self.table is None:
            raise ValueError(f"Pending relation {self.relation!r} has no mapping")
        return RelationMapping(
            self.relation,
            self.table,
            tuple(
                (column.attribute, column.column)
                for column in self.columns
                if column.attribute != column.column
            ),
            enforce_constraints=self.kind == "table",
            runtime_candidate_keys=tuple(
                frozenset(self.column_for(attribute) for attribute in key)
                for key in self.runtime_unique_keys
            ),
        )


@dataclass(frozen=True)
class PhysicalSchema:
    name: str
    logical_contract: str
    source_slice: tuple[str, ...]
    relations: tuple[PhysicalRelationSpec, ...]
    maximum_mariadb_index_bytes: int = 3072
    canonical_digest_protocol: PhysicalCanonicalDigestProtocol | None = None
    canonical_value_page_protocol: PhysicalCanonicalValuePageProtocol | None = None
    source_locator_protocol: PhysicalSourceLocatorProtocol | None = None
    bounded_values: tuple[PhysicalBoundedValue, ...] = ()
    runtime_obligations: tuple[str, ...] = ()

    def relation(self, name: str) -> PhysicalRelationSpec | None:
        return next(
            (relation for relation in self.relations if relation.relation == name),
            None,
        )

    @property
    def implemented_relations(self) -> tuple[PhysicalRelationSpec, ...]:
        return tuple(
            relation for relation in self.relations if relation.status == "implemented"
        )

    @property
    def pending_relations(self) -> tuple[str, ...]:
        return tuple(
            relation.relation
            for relation in self.relations
            if relation.status == "pending"
        )

    @property
    def complete(self) -> bool:
        return not self.pending_relations


@dataclass(frozen=True)
class RelationMapping:
    """Map a logical relation and optional renamed attributes to one table.

    Unlisted attributes use their logical name.  The comparison is strict by
    default because an unexplained physical column may encode an omitted
    semantic dependency.  A future physical design may opt out per relation,
    but doing so must be explicit at the call site.
    """

    relation: str
    table: str
    column_overrides: tuple[tuple[str, str], ...] = ()
    strict_columns: bool = True
    enforce_constraints: bool = True
    runtime_candidate_keys: tuple[frozenset[str], ...] = ()

    def column_for(self, attribute: str) -> str:
        overrides = dict(self.column_overrides)
        return overrides.get(attribute, attribute)


@dataclass(frozen=True, order=True)
class RefinementMismatch:
    relation: str
    code: str
    detail: str

    def render(self) -> str:
        return f"[{self.code}] {self.relation}: {self.detail}"


@dataclass(frozen=True)
class RefinementReport:
    contract_name: str
    backend: str
    checked_relations: tuple[str, ...]
    mismatches: tuple[RefinementMismatch, ...]

    @property
    def conforms(self) -> bool:
        return not self.mismatches

    def render(self) -> str:
        status = "PASS" if self.conforms else "FAIL"
        header = (
            f"schema refinement {status}: contract={self.contract_name!r} "
            f"backend={self.backend!r} relations={len(self.checked_relations)} "
            f"mismatches={len(self.mismatches)}"
        )
        if not self.mismatches:
            return header
        return (
            header
            + "\n"
            + "\n".join(f"- {mismatch.render()}" for mismatch in self.mismatches)
        )

    def __str__(self) -> str:
        return self.render()


@dataclass(frozen=True)
class PhysicalRefinementReport:
    specification_name: str
    contract_name: str
    backend: str
    checked_relations: tuple[str, ...]
    pending_relations: tuple[str, ...]
    mismatches: tuple[RefinementMismatch, ...]
    runtime_obligations: tuple[str, ...] = ()

    @property
    def conforms(self) -> bool:
        """Whether the explicitly implemented source slice conforms."""

        return not self.mismatches

    @property
    def fully_conforms(self) -> bool:
        """Never claim full vNext conformance while any relation is pending."""

        return self.conforms and not self.pending_relations

    @property
    def ddl_only(self) -> bool:
        """Whether conformance is established by DDL with no runtime premise."""

        return self.fully_conforms and not self.runtime_obligations

    def render(self) -> str:
        status = "PASS" if self.conforms else "FAIL"
        header = (
            f"physical schema refinement {status}: "
            f"specification={self.specification_name!r} "
            f"contract={self.contract_name!r} backend={self.backend!r} "
            f"implemented={len(self.checked_relations)} "
            f"pending={len(self.pending_relations)} "
            f"runtime_obligations={len(self.runtime_obligations)} "
            f"mismatches={len(self.mismatches)}"
        )
        details = [
            *(f"- [runtime-obligation] {value}" for value in self.runtime_obligations),
            *(f"- {mismatch.render()}" for mismatch in self.mismatches),
        ]
        return header if not details else header + "\n" + "\n".join(details)

    def __str__(self) -> str:
        return self.render()


class SchemaRefinementError(AssertionError):
    def __init__(self, report: RefinementReport | PhysicalRefinementReport) -> None:
        self.report = report
        super().__init__(report.render())


def load_logical_schema(path: str | Path) -> LogicalSchema:
    """Load relation attributes, declared keys, and FKs from catalog TOML."""

    contract_path = Path(path)
    with contract_path.open("rb") as stream:
        document = tomllib.load(stream)
    name = document.get("name")
    raw_relations = document.get("relation")
    if not isinstance(name, str) or not name:
        raise ValueError("contract.name must be a non-empty string")
    if not isinstance(raw_relations, list) or not raw_relations:
        raise ValueError("contract.relation must be a non-empty array of tables")

    relations: list[LogicalRelation] = []
    for raw_relation in raw_relations:
        if not isinstance(raw_relation, dict):
            raise ValueError("contract.relation must contain tables")
        relation_name = _required_string(raw_relation, "name")
        attributes = _required_string_tuple(raw_relation, "attributes")
        raw_keys = raw_relation.get("declared_keys")
        if not isinstance(raw_keys, list) or not raw_keys:
            raise ValueError(
                f"relation {relation_name!r}.declared_keys must be non-empty"
            )
        keys = tuple(
            frozenset(
                _string_sequence(
                    raw_key,
                    f"relation {relation_name!r}.declared_keys",
                )
            )
            for raw_key in raw_keys
        )
        raw_foreign_keys = raw_relation.get("foreign_keys", [])
        if not isinstance(raw_foreign_keys, list):
            raise ValueError(
                f"relation {relation_name!r}.foreign_keys must be an array"
            )
        foreign_keys: list[LogicalForeignKey] = []
        for raw_foreign_key in raw_foreign_keys:
            if not isinstance(raw_foreign_key, dict):
                raise ValueError(
                    f"relation {relation_name!r}.foreign_keys must contain tables"
                )
            foreign_keys.append(
                LogicalForeignKey(
                    _required_string_tuple(raw_foreign_key, "attributes"),
                    _required_string(raw_foreign_key, "relation"),
                    _required_string_tuple(raw_foreign_key, "referenced_attributes"),
                )
            )
        relations.append(
            LogicalRelation(
                relation_name,
                attributes,
                tuple(_minimal_sets(keys)),
                tuple(foreign_keys),
            )
        )
    if len({relation.name for relation in relations}) != len(relations):
        raise ValueError("contract contains duplicate relation names")
    return LogicalSchema(name, tuple(relations))


def load_physical_schema(
    path: str | Path,
    logical_schema: LogicalSchema,
) -> PhysicalSchema:
    """Load and cross-check an explicit physical realization contract.

    Coverage is closed-world: every logical relation must occur exactly once as
    either ``implemented`` or ``pending``.  Pending entries need a rationale
    and are never converted into a successful refinement result.
    """

    specification_path = Path(path)
    with specification_path.open("rb") as stream:
        document = tomllib.load(stream)
    name = _required_string(document, "name")
    logical_contract = _required_string(document, "logical_contract")
    source_slice = _required_string_tuple(document, "source_slice")
    maximum_mariadb_index_bytes = document.get("maximum_mariadb_index_bytes", 3072)
    if (
        not isinstance(maximum_mariadb_index_bytes, int)
        or isinstance(maximum_mariadb_index_bytes, bool)
        or maximum_mariadb_index_bytes <= 0
    ):
        raise ValueError("maximum_mariadb_index_bytes must be a positive integer")
    raw_digest_protocol = document.get("canonical_digest_protocol")
    canonical_digest_protocol: PhysicalCanonicalDigestProtocol | None = None
    if raw_digest_protocol is not None:
        if not isinstance(raw_digest_protocol, dict):
            raise ValueError("canonical_digest_protocol must be a table")
        canonical_digest_protocol = PhysicalCanonicalDigestProtocol(
            _required_string(raw_digest_protocol, "policy_relation"),
            _required_string(raw_digest_protocol, "value_relation"),
            _required_string(raw_digest_protocol, "digest_attribute"),
            _required_string(raw_digest_protocol, "allocation_relation"),
            _required_string(raw_digest_protocol, "page_relation"),
            _required_string(raw_digest_protocol, "descriptor_relation"),
            _required_string(raw_digest_protocol, "parent_relation"),
            _required_string(raw_digest_protocol, "root_attribute"),
            _required_string(raw_digest_protocol, "byte_count_attribute"),
            _required_string(raw_digest_protocol, "algorithm"),
            _required_string(raw_digest_protocol, "framing"),
            _required_string(raw_digest_protocol, "enforcement"),
        )
    raw_page_protocol = document.get("canonical_value_page_protocol")
    canonical_value_page_protocol: PhysicalCanonicalValuePageProtocol | None = None
    if raw_page_protocol is not None:
        if not isinstance(raw_page_protocol, dict):
            raise ValueError("canonical_value_page_protocol must be a table")
        integer_fields = (
            "codec_version",
            "maximum_page_bytes",
            "chunk_maximum_bytes",
            "branch_capacity",
            "maximum_level",
            "maximum_byte_count",
        )
        integer_values: list[int] = []
        for field_name in integer_fields:
            value = raw_page_protocol.get(field_name)
            if type(value) is not int or value <= 0:
                raise ValueError(
                    f"canonical_value_page_protocol.{field_name} must be positive"
                )
            integer_values.append(value)
        canonical_value_page_protocol = PhysicalCanonicalValuePageProtocol(
            integer_values[0],
            _required_string(raw_page_protocol, "prefix"),
            integer_values[1],
            integer_values[2],
            integer_values[3],
            integer_values[4],
            integer_values[5],
            _required_string(raw_page_protocol, "framing"),
            _required_string(raw_page_protocol, "enforcement"),
        )
    raw_locator_protocol = document.get("source_locator_protocol")
    source_locator_protocol: PhysicalSourceLocatorProtocol | None = None
    if raw_locator_protocol is not None:
        if not isinstance(raw_locator_protocol, dict):
            raise ValueError("source_locator_protocol must be a table")
        source_locator_protocol = PhysicalSourceLocatorProtocol(
            _required_string(raw_locator_protocol, "identity_relation"),
            _required_string(raw_locator_protocol, "gallery_relation"),
            _required_string(raw_locator_protocol, "digest_attribute"),
            _required_string(raw_locator_protocol, "name_attribute"),
            _required_string(raw_locator_protocol, "canonical_value_relation"),
            _required_string(raw_locator_protocol, "digest_domain"),
            _required_string(raw_locator_protocol, "framing"),
            _required_string(raw_locator_protocol, "enforcement"),
        )
    bounded_values: list[PhysicalBoundedValue] = []
    for raw_bound in _table_array(document.get("bounded_value", []), "bounded_value"):
        maximum_bytes = raw_bound.get("maximum_bytes")
        if (
            not isinstance(maximum_bytes, int)
            or isinstance(maximum_bytes, bool)
            or maximum_bytes <= 0
        ):
            raise ValueError("bounded_value.maximum_bytes must be positive")
        bounded_values.append(
            PhysicalBoundedValue(
                _required_string(raw_bound, "attribute"),
                maximum_bytes,
                _required_string(raw_bound, "encoding"),
                _required_string(raw_bound, "source"),
                _required_string(raw_bound, "runtime_obligation"),
            )
        )
    raw_runtime_obligations = document.get("runtime_obligations", [])
    if not isinstance(raw_runtime_obligations, list) or not all(
        isinstance(value, str) and value for value in raw_runtime_obligations
    ):
        raise ValueError("runtime_obligations must be an array of strings")
    runtime_obligations = tuple(raw_runtime_obligations)
    raw_relations = document.get("relation")
    if not isinstance(raw_relations, list) or not raw_relations:
        raise ValueError("physical relation must be a non-empty array of tables")

    relations: list[PhysicalRelationSpec] = []
    for raw_relation in raw_relations:
        if not isinstance(raw_relation, dict):
            raise ValueError("physical relation must contain tables")
        relation_name = _required_string(raw_relation, "name")
        status = _required_string(raw_relation, "status")
        rationale = _required_string(raw_relation, "rationale")
        if status == "pending":
            unexpected = set(raw_relation) - {"name", "status", "rationale"}
            if unexpected:
                raise ValueError(
                    f"pending relation {relation_name!r} declares physical fields "
                    f"{_format_names(unexpected)}"
                )
            relations.append(PhysicalRelationSpec(relation_name, status, rationale))
            continue
        if status != "implemented":
            raise ValueError(
                f"relation {relation_name!r}.status must be implemented or pending"
            )

        kind_value = raw_relation.get("kind", "table")
        if not isinstance(kind_value, str) or kind_value not in {"table", "view"}:
            raise ValueError(f"relation {relation_name!r}.kind must be table or view")
        table = _required_string(raw_relation, "table")
        _validate_identifier(table, f"relation {relation_name!r}.table")
        overlay_view: OverlayViewSpec | None = None
        raw_view = raw_relation.get("view")
        if kind_value == "view":
            if not isinstance(raw_view, dict):
                raise ValueError(
                    f"view relation {relation_name!r}.view must be a table"
                )
            pattern = _required_string(raw_view, "pattern")
            if pattern != "nearest_ancestor_overlay":
                raise ValueError(
                    f"view relation {relation_name!r}.view.pattern must be "
                    "nearest_ancestor_overlay"
                )
            unexpected_view = set(raw_view) - {
                "pattern",
                "ancestry_relation",
                "shadow_relation",
                "tombstone_relation",
            }
            if unexpected_view:
                raise ValueError(
                    f"view relation {relation_name!r}.view has unknown fields "
                    f"{_format_names(unexpected_view)}"
                )
            overlay_view = OverlayViewSpec(
                _required_string(raw_view, "ancestry_relation"),
                _required_string(raw_view, "shadow_relation"),
                _required_string(raw_view, "tombstone_relation"),
            )
        elif raw_view is not None:
            raise ValueError(f"table relation {relation_name!r} cannot declare view")
        raw_columns = raw_relation.get("column")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError(
                f"relation {relation_name!r}.column must be a non-empty array"
            )
        columns: list[PhysicalColumnSpec] = []
        for raw_column in raw_columns:
            if not isinstance(raw_column, dict):
                raise ValueError(
                    f"relation {relation_name!r}.column must contain tables"
                )
            attribute = _required_string(raw_column, "attribute")
            column_name = _required_string(raw_column, "name")
            _validate_identifier(
                column_name,
                f"relation {relation_name!r} column {attribute!r}.name",
            )
            columns.append(
                PhysicalColumnSpec(
                    attribute,
                    column_name,
                    _backend_column_spec(
                        raw_column.get("sqlite"),
                        f"relation {relation_name!r} column {attribute!r}.sqlite",
                    ),
                    _backend_column_spec(
                        raw_column.get("mariadb"),
                        f"relation {relation_name!r} column {attribute!r}.mariadb",
                    ),
                )
            )

        primary_key = _required_string_tuple(raw_relation, "primary_key")
        unique_keys = _string_sequences(
            raw_relation.get("unique_keys", []),
            f"relation {relation_name!r}.unique_keys",
        )
        runtime_unique_keys = _string_sequences(
            raw_relation.get("runtime_unique_keys", []),
            f"relation {relation_name!r}.runtime_unique_keys",
        )
        foreign_keys: list[PhysicalForeignKeySpec] = []
        for raw_foreign_key in _table_array(
            raw_relation.get("foreign_key", []),
            f"relation {relation_name!r}.foreign_key",
        ):
            constraint_name = _required_string(raw_foreign_key, "name")
            _validate_identifier(
                constraint_name,
                f"relation {relation_name!r} foreign-key name",
            )
            foreign_keys.append(
                PhysicalForeignKeySpec(
                    constraint_name,
                    _required_string_tuple(raw_foreign_key, "attributes"),
                    _required_string(raw_foreign_key, "referenced_relation"),
                    _required_string_tuple(
                        raw_foreign_key,
                        "referenced_attributes",
                    ),
                )
            )
        required_indexes: list[PhysicalIndexSpec] = []
        for raw_index in _table_array(
            raw_relation.get("required_index", []),
            f"relation {relation_name!r}.required_index",
        ):
            index_name = _required_string(raw_index, "name")
            _validate_identifier(
                index_name,
                f"relation {relation_name!r} index name",
            )
            unique = raw_index.get("unique")
            if not isinstance(unique, bool):
                raise ValueError(
                    f"relation {relation_name!r} index {index_name!r}.unique "
                    "must be boolean"
                )
            required_indexes.append(
                PhysicalIndexSpec(
                    index_name,
                    _required_string_tuple(raw_index, "attributes"),
                    unique,
                )
            )
        checks: list[PhysicalCheckSpec] = []
        for raw_check in _table_array(
            raw_relation.get("check", []),
            f"relation {relation_name!r}.check",
        ):
            check_name = _required_string(raw_check, "name")
            _validate_identifier(
                check_name,
                f"relation {relation_name!r} check name",
            )
            checks.append(
                PhysicalCheckSpec(
                    check_name,
                    _required_string(raw_check, "sqlite_expression"),
                    _required_string(raw_check, "mariadb_expression"),
                )
            )
        relations.append(
            PhysicalRelationSpec(
                relation=relation_name,
                status=status,
                rationale=rationale,
                table=table,
                columns=tuple(columns),
                primary_key=primary_key,
                unique_keys=unique_keys,
                runtime_unique_keys=runtime_unique_keys,
                foreign_keys=tuple(foreign_keys),
                required_indexes=tuple(required_indexes),
                checks=tuple(checks),
                kind=kind_value,
                overlay_view=overlay_view,
            )
        )

    physical = PhysicalSchema(
        name=name,
        logical_contract=logical_contract,
        source_slice=source_slice,
        relations=tuple(relations),
        maximum_mariadb_index_bytes=maximum_mariadb_index_bytes,
        canonical_digest_protocol=canonical_digest_protocol,
        canonical_value_page_protocol=canonical_value_page_protocol,
        source_locator_protocol=source_locator_protocol,
        bounded_values=tuple(bounded_values),
        runtime_obligations=runtime_obligations,
    )
    _validate_bootstrap_contract(document, physical)
    _validate_physical_schema(physical, logical_schema)
    return physical


def introspect_sqlite(reader: MetadataReader) -> DatabaseShape:
    """Read tables and views, including table constraints, from SQLite."""

    table_rows = reader.fetch_all("""
        SELECT type, name, sql
        FROM sqlite_schema
        WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """)
    tables: list[TableShape] = []
    for raw_kind, raw_table_name, *table_metadata in table_rows:
        object_kind = str(raw_kind).lower()
        table_name = str(raw_table_name)
        create_sql = (
            str(table_metadata[0])
            if table_metadata and table_metadata[0] is not None
            else ""
        )
        quoted_table = _quote_sqlite_identifier(table_name)
        column_rows = reader.fetch_all(f"PRAGMA table_info({quoted_table})")
        ordered_columns = sorted(column_rows, key=lambda row: int(row[0]))
        columns = tuple(str(row[1]) for row in ordered_columns)
        primary_key = tuple(
            str(row[1])
            for row in sorted(
                (row for row in ordered_columns if int(row[5]) > 0),
                key=lambda row: int(row[5]),
            )
        )
        collations = _sqlite_column_collations(create_sql, columns)
        column_shapes = tuple(
            ColumnShape(
                str(row[1]),
                _normalize_type_name(str(row[2])),
                not (bool(row[3]) or int(row[5]) > 0),
                collations.get(str(row[1])),
            )
            for row in ordered_columns
        )

        unique_keys: list[tuple[str, ...]] = []
        indexes: list[IndexShape] = []
        for index_row in reader.fetch_all(f"PRAGMA index_list({quoted_table})"):
            index_name = str(index_row[1])
            is_unique = bool(index_row[2])
            origin = str(index_row[3]) if len(index_row) > 3 else ""
            is_partial = bool(index_row[4]) if len(index_row) > 4 else False
            if origin == "pk" or is_partial:
                continue
            index_rows = sorted(
                reader.fetch_all(
                    f"PRAGMA index_info({_quote_sqlite_identifier(index_name)})"
                ),
                key=lambda row: int(row[0]),
            )
            index_columns = tuple(
                str(row[2]) for row in index_rows if row[2] is not None
            )
            if not index_columns or len(index_columns) != len(index_rows):
                continue
            if is_unique:
                unique_keys.append(index_columns)
            if origin == "c":
                indexes.append(IndexShape(index_name, index_columns, is_unique))

        foreign_key_groups: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
        for foreign_key_row in reader.fetch_all(
            f"PRAGMA foreign_key_list({quoted_table})"
        ):
            foreign_key_groups[int(foreign_key_row[0])].append(foreign_key_row)
        foreign_keys: list[ForeignKeyShape] = []
        for identifier in sorted(foreign_key_groups):
            rows = sorted(foreign_key_groups[identifier], key=lambda row: int(row[1]))
            referenced_columns = tuple(
                "" if row[4] is None else str(row[4]) for row in rows
            )
            foreign_keys.append(
                ForeignKeyShape(
                    tuple(str(row[3]) for row in rows),
                    str(rows[0][2]),
                    referenced_columns,
                )
            )
        tables.append(
            TableShape(
                table_name,
                columns,
                primary_key,
                tuple(_sorted_tuples(unique_keys)),
                tuple(sorted(foreign_keys)),
                column_shapes,
                tuple(sorted(indexes)),
                _extract_named_checks(create_sql),
                object_kind,
            )
        )
    return DatabaseShape("sqlite", tuple(sorted(tables, key=lambda table: table.name)))


def introspect_mariadb(reader: MetadataReader) -> DatabaseShape:
    """Read connected MariaDB tables and views through INFORMATION_SCHEMA."""

    table_rows = reader.fetch_all("""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY TABLE_NAME
        """)
    table_names = tuple(str(row[0]) for row in table_rows)
    table_kinds = {
        str(row[0]): (
            "view" if len(row) > 1 and str(row[1]).upper() == "VIEW" else "table"
        )
        for row in table_rows
    }
    column_rows = reader.fetch_all("""
        SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE,
               IS_NULLABLE, COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """)
    index_rows = reader.fetch_all("""
        SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """)
    foreign_key_rows = reader.fetch_all("""
        SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION,
               REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION
        """)
    check_rows = reader.fetch_all("""
        SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc
          ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
         AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
          AND tc.CONSTRAINT_TYPE = 'CHECK'
        ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME
        """)

    columns_by_table: dict[str, list[tuple[int, str]]] = defaultdict(list)
    column_shapes_by_table: dict[str, list[tuple[int, ColumnShape]]] = defaultdict(list)
    for table_name, column_name, ordinal, *metadata in column_rows:
        columns_by_table[str(table_name)].append((int(ordinal), str(column_name)))
        type_name = _normalize_type_name(str(metadata[0])) if metadata else ""
        nullable = str(metadata[1]).upper() == "YES" if len(metadata) > 1 else True
        collation = (
            None if len(metadata) < 3 or metadata[2] is None else str(metadata[2])
        )
        column_shapes_by_table[str(table_name)].append(
            (
                int(ordinal),
                ColumnShape(str(column_name), type_name, nullable, collation),
            )
        )

    indexes: dict[tuple[str, str, bool], list[tuple[int, str | None]]] = defaultdict(
        list
    )
    for table_name, index_name, non_unique, sequence, column_name, *_ in index_rows:
        indexes[(str(table_name), str(index_name), not bool(non_unique))].append(
            (int(sequence), None if column_name is None else str(column_name))
        )

    foreign_key_groups: dict[tuple[str, str], list[tuple[int, str, str, str]]] = (
        defaultdict(list)
    )
    for (
        table_name,
        constraint_name,
        column_name,
        ordinal,
        referenced_table,
        referenced_column,
        *_,
    ) in foreign_key_rows:
        foreign_key_groups[(str(table_name), str(constraint_name))].append(
            (
                int(ordinal),
                str(column_name),
                str(referenced_table),
                str(referenced_column),
            )
        )

    checks_by_table: dict[str, list[CheckShape]] = defaultdict(list)
    for table_name, constraint_name, expression, *_ in check_rows:
        checks_by_table[str(table_name)].append(
            CheckShape(
                str(constraint_name),
                _normalize_check_expression(str(expression)),
            )
        )

    tables: list[TableShape] = []
    for table_name in table_names:
        primary_key: tuple[str, ...] = ()
        unique_keys: list[tuple[str, ...]] = []
        table_indexes: list[IndexShape] = []
        for (
            indexed_table,
            index_name,
            is_unique,
        ), index_values in sorted(indexes.items()):
            if indexed_table != table_name:
                continue
            ordered_index_values = sorted(index_values)
            if any(column is None for _sequence, column in ordered_index_values):
                continue
            key = tuple(str(column) for _sequence, column in ordered_index_values)
            if index_name == "PRIMARY":
                primary_key = key
            elif is_unique:
                unique_keys.append(key)
            if index_name != "PRIMARY":
                table_indexes.append(IndexShape(index_name, key, is_unique))

        foreign_keys: list[ForeignKeyShape] = []
        for (local_table, _constraint_name), foreign_key_values in sorted(
            foreign_key_groups.items()
        ):
            if local_table != table_name:
                continue
            ordered_foreign_key_values = sorted(foreign_key_values)
            foreign_keys.append(
                ForeignKeyShape(
                    tuple(value[1] for value in ordered_foreign_key_values),
                    ordered_foreign_key_values[0][2],
                    tuple(value[3] for value in ordered_foreign_key_values),
                )
            )
        tables.append(
            TableShape(
                table_name,
                tuple(
                    column for _ordinal, column in sorted(columns_by_table[table_name])
                ),
                primary_key,
                tuple(_sorted_tuples(unique_keys)),
                tuple(sorted(foreign_keys)),
                tuple(
                    column
                    for _ordinal, column in sorted(column_shapes_by_table[table_name])
                ),
                tuple(sorted(table_indexes)),
                tuple(sorted(checks_by_table[table_name])),
                table_kinds[table_name],
            )
        )
    return DatabaseShape("mariadb", tuple(tables))


def compare_refinement(
    logical_schema: LogicalSchema,
    database: DatabaseShape,
    mappings: Iterable[RelationMapping] | None = None,
) -> RefinementReport:
    """Compare selected logical relations with normalized physical tables."""

    selected_mappings = (
        tuple(mappings)
        if mappings is not None
        else tuple(
            RelationMapping(relation.name, relation.name)
            for relation in logical_schema.relations
        )
    )
    mismatches: list[RefinementMismatch] = []
    mapping_by_relation: dict[str, RelationMapping] = {}
    for mapping in selected_mappings:
        if mapping.relation in mapping_by_relation:
            mismatches.append(
                RefinementMismatch(
                    mapping.relation,
                    "duplicate-mapping",
                    "logical relation has more than one physical mapping",
                )
            )
        else:
            mapping_by_relation[mapping.relation] = mapping

    for mapping in selected_mappings:
        relation = logical_schema.relation(mapping.relation)
        if relation is None:
            mismatches.append(
                RefinementMismatch(
                    mapping.relation,
                    "unknown-relation",
                    f"mapping targets no relation in {logical_schema.name!r}",
                )
            )
            continue
        table = database.table(mapping.table)
        if table is None:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "missing-table",
                    f"expected physical table {mapping.table!r}",
                )
            )
            continue
        mismatches.extend(
            _compare_relation(
                relation,
                table,
                mapping,
                logical_schema,
                database,
                mapping_by_relation,
            )
        )
    return RefinementReport(
        logical_schema.name,
        database.backend,
        tuple(mapping.relation for mapping in selected_mappings),
        tuple(sorted(set(mismatches))),
    )


def compare_physical_refinement(
    logical_schema: LogicalSchema,
    physical_schema: PhysicalSchema,
    database: DatabaseShape,
) -> PhysicalRefinementReport:
    """Compare every explicitly implemented relation with backend metadata."""

    implemented = physical_schema.implemented_relations
    mappings = tuple(relation.as_mapping() for relation in implemented)
    logical_report = compare_refinement(logical_schema, database, mappings)
    mismatches = list(logical_report.mismatches)
    for relation_spec in implemented:
        assert relation_spec.table is not None
        table = database.table(relation_spec.table)
        if table is None:
            continue
        mismatches.extend(
            _compare_physical_details(relation_spec, table, database.backend)
        )
    return PhysicalRefinementReport(
        physical_schema.name,
        logical_schema.name,
        database.backend,
        tuple(relation.relation for relation in implemented),
        physical_schema.pending_relations,
        tuple(sorted(set(mismatches))),
        physical_schema.runtime_obligations,
    )


def assert_refines(report: RefinementReport) -> None:
    if not report.conforms:
        raise SchemaRefinementError(report)


def assert_physical_refines(
    report: PhysicalRefinementReport,
    *,
    require_complete: bool = False,
) -> None:
    if not report.conforms or (require_complete and report.pending_relations):
        raise SchemaRefinementError(report)


def render_sqlite_ddl(
    physical_schema: PhysicalSchema,
    *,
    idempotent: bool = False,
) -> str:
    """Render SQLite DDL; ``idempotent`` supports resumable epoch creation."""

    statements = ["PRAGMA foreign_keys = ON;"]
    relation_by_name = {
        relation.relation: relation
        for relation in physical_schema.implemented_relations
    }
    for relation_name in physical_schema.source_slice:
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                _render_overlay_view(
                    relation,
                    relation_by_name,
                    "sqlite",
                    idempotent=idempotent,
                )
                + ";"
            )
            continue
        definitions: list[str] = []
        for column in relation.columns:
            backend = column.sqlite
            definition = (
                f"{_quote_sqlite_identifier(column.column)} {backend.type_name}"
            )
            if backend.collation is not None:
                definition += f" COLLATE {backend.collation}"
            definition += " NULL" if backend.nullable else " NOT NULL"
            definitions.append(definition)
        definitions.append(
            f"CONSTRAINT {_quote_sqlite_identifier('pk_' + relation.table)} "
            "PRIMARY KEY ("
            + ", ".join(
                _quote_sqlite_identifier(relation.column_for(attribute))
                for attribute in relation.primary_key
            )
            + ")"
        )
        for position, unique_key in enumerate(relation.unique_keys, start=1):
            definitions.append(
                f"CONSTRAINT {_quote_sqlite_identifier(f'uk_{relation.table}_{position}')} "
                "UNIQUE ("
                + ", ".join(
                    _quote_sqlite_identifier(relation.column_for(attribute))
                    for attribute in unique_key
                )
                + ")"
            )
        for foreign_key in relation.foreign_keys:
            target = relation_by_name[foreign_key.referenced_relation]
            assert target.table is not None
            definitions.append(
                f"CONSTRAINT {_quote_sqlite_identifier(foreign_key.name)} "
                "FOREIGN KEY ("
                + ", ".join(
                    _quote_sqlite_identifier(relation.column_for(attribute))
                    for attribute in foreign_key.attributes
                )
                + f") REFERENCES {_quote_sqlite_identifier(target.table)} ("
                + ", ".join(
                    _quote_sqlite_identifier(target.column_for(attribute))
                    for attribute in foreign_key.referenced_attributes
                )
                + ")"
            )
        for check in relation.checks:
            definitions.append(
                f"CONSTRAINT {_quote_sqlite_identifier(check.name)} "
                f"CHECK ({check.sqlite_expression})"
            )
        table_prefix = "CREATE TABLE IF NOT EXISTS" if idempotent else "CREATE TABLE"
        statements.append(
            f"{table_prefix} {_quote_sqlite_identifier(relation.table)} (\n  "
            + ",\n  ".join(definitions)
            + "\n);"
        )
        for index in relation.required_indexes:
            uniqueness = "UNIQUE " if index.unique else ""
            existence = "IF NOT EXISTS " if idempotent else ""
            statements.append(
                f"CREATE {uniqueness}INDEX {existence}"
                f"{_quote_sqlite_identifier(index.name)} "
                f"ON {_quote_sqlite_identifier(relation.table)} ("
                + ", ".join(
                    _quote_sqlite_identifier(relation.column_for(attribute))
                    for attribute in index.attributes
                )
                + ");"
            )
    return "\n".join(statements)


def render_mariadb_ddl(
    physical_schema: PhysicalSchema,
    *,
    idempotent: bool = False,
) -> tuple[str, ...]:
    """Render MariaDB DDL; ``idempotent`` supports partial-DDL resume."""

    statements: list[str] = []
    relation_by_name = {
        relation.relation: relation
        for relation in physical_schema.implemented_relations
    }
    for relation_name in physical_schema.source_slice:
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                _render_overlay_view(
                    relation,
                    relation_by_name,
                    "mariadb",
                    idempotent=idempotent,
                )
            )
            continue
        definitions: list[str] = []
        for column in relation.columns:
            backend = column.mariadb
            definition = (
                f"{_quote_mariadb_identifier(column.column)} {backend.type_name}"
            )
            if backend.collation is not None:
                definition += f" COLLATE {backend.collation}"
            definition += " NULL" if backend.nullable else " NOT NULL"
            definitions.append(definition)
        definitions.append(
            "PRIMARY KEY ("
            + ", ".join(
                _quote_mariadb_identifier(relation.column_for(attribute))
                for attribute in relation.primary_key
            )
            + ")"
        )
        for position, unique_key in enumerate(relation.unique_keys, start=1):
            definitions.append(
                f"CONSTRAINT {_quote_mariadb_identifier(f'uk_{relation.table}_{position}')} "
                "UNIQUE ("
                + ", ".join(
                    _quote_mariadb_identifier(relation.column_for(attribute))
                    for attribute in unique_key
                )
                + ")"
            )
        for index in relation.required_indexes:
            uniqueness = "UNIQUE " if index.unique else ""
            definitions.append(
                f"{uniqueness}KEY {_quote_mariadb_identifier(index.name)} ("
                + ", ".join(
                    _quote_mariadb_identifier(relation.column_for(attribute))
                    for attribute in index.attributes
                )
                + ")"
            )
        for foreign_key in relation.foreign_keys:
            target = relation_by_name[foreign_key.referenced_relation]
            assert target.table is not None
            definitions.append(
                f"CONSTRAINT {_quote_mariadb_identifier(foreign_key.name)} "
                "FOREIGN KEY ("
                + ", ".join(
                    _quote_mariadb_identifier(relation.column_for(attribute))
                    for attribute in foreign_key.attributes
                )
                + f") REFERENCES {_quote_mariadb_identifier(target.table)} ("
                + ", ".join(
                    _quote_mariadb_identifier(target.column_for(attribute))
                    for attribute in foreign_key.referenced_attributes
                )
                + ")"
            )
        for check in relation.checks:
            definitions.append(
                f"CONSTRAINT {_quote_mariadb_identifier(check.name)} "
                f"CHECK ({check.mariadb_expression})"
            )
        table_prefix = "CREATE TABLE IF NOT EXISTS" if idempotent else "CREATE TABLE"
        statements.append(
            f"{table_prefix} {_quote_mariadb_identifier(relation.table)} (\n  "
            + ",\n  ".join(definitions)
            + "\n) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 "
            "COLLATE=utf8mb4_nopad_bin"
        )
    return tuple(statements)


def _render_overlay_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool = False,
) -> str:
    """Render a bounded nearest-ancestor shadow/tombstone resolution view."""

    overlay = relation.overlay_view
    if overlay is None or relation.table is None:
        raise ValueError(f"relation {relation.relation!r} is not an overlay view")
    ancestry = relation_by_name[overlay.ancestry_relation]
    shadow = relation_by_name[overlay.shadow_relation]
    tombstone = relation_by_name[overlay.tombstone_relation]
    assert ancestry.table is not None
    assert shadow.table is not None
    assert tombstone.table is not None
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )

    key_attributes = tuple(
        attribute for attribute in relation.primary_key if attribute != "analysis_id"
    )
    value_attributes = tuple(
        column.attribute
        for column in shadow.columns
        if column.attribute not in relation.primary_key
    )
    expression_by_attribute = {
        "analysis_id": f"path.{quote(ancestry.column_for('analysis_id'))}",
        **{
            attribute: f"shadow.{quote(shadow.column_for(attribute))}"
            for attribute in (*key_attributes, *value_attributes)
        },
    }
    projection = ",\n  ".join(
        f"{expression_by_attribute[column.attribute]} AS {quote(column.column)}"
        for column in relation.columns
    )

    same_key_tombstone = "\n      AND ".join(
        f"same_tomb.{quote(tombstone.column_for(attribute))} "
        f"= shadow.{quote(shadow.column_for(attribute))}"
        for attribute in key_attributes
    )
    nearer_shadow_key = "\n          AND ".join(
        f"near_shadow.{quote(shadow.column_for(attribute))} "
        f"= shadow.{quote(shadow.column_for(attribute))}"
        for attribute in key_attributes
    )
    nearer_tombstone_key = "\n          AND ".join(
        f"near_tomb.{quote(tombstone.column_for(attribute))} "
        f"= shadow.{quote(shadow.column_for(attribute))}"
        for attribute in key_attributes
    )
    if backend == "sqlite":
        prefix = "CREATE VIEW IF NOT EXISTS" if idempotent else "CREATE VIEW"
    else:
        prefix = (
            "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
            if idempotent
            else "CREATE SQL SECURITY INVOKER VIEW"
        )
    column_list = ", ".join(quote(column.column) for column in relation.columns)
    return f"""{prefix} {quote(relation.table)} ({column_list}) AS
SELECT
  {projection}
FROM {quote(ancestry.table)} AS path
JOIN {quote(shadow.table)} AS shadow
  ON shadow.{quote(shadow.column_for("analysis_id"))}
   = path.{quote(ancestry.column_for("ancestor_analysis_id"))}
WHERE NOT EXISTS (
  SELECT 1
  FROM {quote(tombstone.table)} AS same_tomb
  WHERE same_tomb.{quote(tombstone.column_for("analysis_id"))}
      = path.{quote(ancestry.column_for("ancestor_analysis_id"))}
    AND {same_key_tombstone}
)
AND NOT EXISTS (
  SELECT 1
  FROM {quote(ancestry.table)} AS nearer
  WHERE nearer.{quote(ancestry.column_for("analysis_id"))}
      = path.{quote(ancestry.column_for("analysis_id"))}
    AND nearer.{quote(ancestry.column_for("ancestor_depth"))}
      < path.{quote(ancestry.column_for("ancestor_depth"))}
    AND (
      EXISTS (
        SELECT 1
        FROM {quote(shadow.table)} AS near_shadow
        WHERE near_shadow.{quote(shadow.column_for("analysis_id"))}
            = nearer.{quote(ancestry.column_for("ancestor_analysis_id"))}
          AND {nearer_shadow_key}
      )
      OR EXISTS (
        SELECT 1
        FROM {quote(tombstone.table)} AS near_tomb
        WHERE near_tomb.{quote(tombstone.column_for("analysis_id"))}
            = nearer.{quote(ancestry.column_for("ancestor_analysis_id"))}
          AND {nearer_tombstone_key}
      )
    )
)"""


def _compare_physical_details(
    relation: PhysicalRelationSpec,
    table: TableShape,
    backend: str,
) -> list[RefinementMismatch]:
    mismatches: list[RefinementMismatch] = []
    if table.kind != relation.kind:
        mismatches.append(
            RefinementMismatch(
                relation.relation,
                "object-kind",
                f"physical object {table.name!r} is {table.kind!r}, "
                f"expected {relation.kind!r}",
            )
        )
    if relation.kind == "view":
        # SQL views cannot expose enforceable PK/UK/FK constraints.  Their
        # semantic keys and references are checked against the closed-world
        # logical contract by _validate_physical_schema.  Database refinement
        # verifies that the object is genuinely a view and that its exact
        # projection is present, without pretending table constraints exist.
        return mismatches
    if not table.column_shapes:
        mismatches.append(
            RefinementMismatch(
                relation.relation,
                "missing-column-metadata",
                f"backend {backend!r} did not expose type/nullability/collation",
            )
        )
    for column_spec in relation.columns:
        expected = column_spec.for_backend(backend)
        actual = table.column(column_spec.column)
        if actual is None:
            continue
        if _normalize_type_name(actual.type_name) != _normalize_type_name(
            expected.type_name
        ):
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "column-type",
                    f"column {column_spec.column!r} type {actual.type_name!r} "
                    f"does not equal {expected.type_name!r}",
                )
            )
        if actual.nullable != expected.nullable:
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "column-nullability",
                    f"column {column_spec.column!r} nullable={actual.nullable} "
                    f"does not equal {expected.nullable}",
                )
            )
        if _normalize_collation(actual.collation) != _normalize_collation(
            expected.collation
        ):
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "column-collation",
                    f"column {column_spec.column!r} collation "
                    f"{actual.collation!r} does not equal {expected.collation!r}",
                )
            )

    expected_primary = tuple(
        relation.column_for(attribute) for attribute in relation.primary_key
    )
    if table.primary_key != expected_primary:
        mismatches.append(
            RefinementMismatch(
                relation.relation,
                "primary-key-order",
                f"table {table.name!r} primary key {table.primary_key!r} "
                f"does not equal {expected_primary!r}",
            )
        )
    expected_unique = {
        tuple(relation.column_for(attribute) for attribute in key)
        for key in relation.unique_keys
    }
    actual_unique = set(table.unique_keys)
    if expected_unique != actual_unique:
        mismatches.append(
            RefinementMismatch(
                relation.relation,
                "unique-key-order",
                f"table {table.name!r} unique keys {sorted(actual_unique)!r} "
                f"do not equal {sorted(expected_unique)!r}",
            )
        )

    actual_indexes = {index.name: index for index in table.indexes}
    for index in relation.required_indexes:
        expected_index = IndexShape(
            index.name,
            tuple(relation.column_for(attribute) for attribute in index.attributes),
            index.unique,
        )
        actual_index = actual_indexes.get(index.name)
        if actual_index != expected_index:
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "required-index",
                    f"expected {expected_index!r}, found {actual_index!r}",
                )
            )

    expected_checks = {
        check.name: _normalize_check_expression(check.expression_for(backend))
        for check in relation.checks
    }
    actual_checks = {check.name: check.expression for check in table.checks}
    if expected_checks != actual_checks:
        mismatches.append(
            RefinementMismatch(
                relation.relation,
                "check-constraints",
                f"table {table.name!r} checks {actual_checks!r} "
                f"do not equal {expected_checks!r}",
            )
        )
    return mismatches


def _compare_relation(
    relation: LogicalRelation,
    table: TableShape,
    mapping: RelationMapping,
    logical_schema: LogicalSchema,
    database: DatabaseShape,
    mapping_by_relation: Mapping[str, RelationMapping],
) -> list[RefinementMismatch]:
    mismatches: list[RefinementMismatch] = []
    overrides = dict(mapping.column_overrides)
    if len(overrides) != len(mapping.column_overrides):
        mismatches.append(
            RefinementMismatch(
                relation.name,
                "duplicate-column-override",
                "more than one override was declared for a logical attribute",
            )
        )
    unknown_overrides = set(overrides) - set(relation.attributes)
    if unknown_overrides:
        mismatches.append(
            RefinementMismatch(
                relation.name,
                "unknown-attribute-override",
                "overrides unknown logical attributes "
                + _format_names(unknown_overrides),
            )
        )
    mapped_columns = tuple(mapping.column_for(value) for value in relation.attributes)
    collisions = {
        column for column in mapped_columns if mapped_columns.count(column) > 1
    }
    if collisions:
        mismatches.append(
            RefinementMismatch(
                relation.name,
                "column-mapping-collision",
                "multiple logical attributes map to " + _format_names(collisions),
            )
        )

    expected_columns = set(mapped_columns)
    actual_columns = set(table.columns)
    missing_columns = expected_columns - actual_columns
    if missing_columns:
        mismatches.append(
            RefinementMismatch(
                relation.name,
                "missing-columns",
                f"table {table.name!r} lacks {_format_names(missing_columns)}",
            )
        )
    if mapping.strict_columns:
        unexpected_columns = actual_columns - expected_columns
        if unexpected_columns:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "unexpected-columns",
                    f"table {table.name!r} has unmapped "
                    + _format_names(unexpected_columns),
                )
            )

    if mapping.enforce_constraints:
        expected_keys = {
            frozenset(mapping.column_for(attribute) for attribute in key)
            for key in relation.candidate_keys
        }
        actual_keys = set(table.candidate_keys) | set(mapping.runtime_candidate_keys)
        missing_keys = expected_keys - actual_keys
        unexpected_keys = actual_keys - expected_keys
        if missing_keys:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "missing-candidate-keys",
                    f"table {table.name!r} does not enforce "
                    + _format_keys(missing_keys),
                )
            )
        if unexpected_keys:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "unexpected-candidate-keys",
                    f"table {table.name!r} enforces undeclared "
                    + _format_keys(unexpected_keys),
                )
            )
        if table.primary_key and frozenset(table.primary_key) not in expected_keys:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "primary-key-not-candidate",
                    f"table {table.name!r} primary key "
                    f"{_format_key(table.primary_key)} is not a declared candidate key",
                )
            )

    expected_foreign_keys: set[ForeignKeyShape] = set()
    for foreign_key in relation.foreign_keys:
        target_relation = logical_schema.relation(foreign_key.referenced_relation)
        if target_relation is None:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "unknown-foreign-relation",
                    f"foreign key references {foreign_key.referenced_relation!r}",
                )
            )
            continue
        target_mapping = mapping_by_relation.get(
            target_relation.name,
            RelationMapping(target_relation.name, target_relation.name),
        )
        expected_foreign_keys.add(
            ForeignKeyShape(
                tuple(mapping.column_for(value) for value in foreign_key.attributes),
                target_mapping.table,
                tuple(
                    target_mapping.column_for(value)
                    for value in foreign_key.referenced_attributes
                ),
            )
        )
        if database.table(target_mapping.table) is None:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "missing-referenced-table",
                    f"foreign key target table {target_mapping.table!r} is absent",
                )
            )
    if mapping.enforce_constraints:
        actual_foreign_keys = set(table.foreign_keys)
        missing_foreign_keys = expected_foreign_keys - actual_foreign_keys
        unexpected_foreign_keys = actual_foreign_keys - expected_foreign_keys
        if missing_foreign_keys:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "missing-foreign-keys",
                    f"table {table.name!r} does not enforce "
                    + _format_foreign_keys(missing_foreign_keys),
                )
            )
        if unexpected_foreign_keys:
            mismatches.append(
                RefinementMismatch(
                    relation.name,
                    "unexpected-foreign-keys",
                    f"table {table.name!r} enforces undeclared "
                    + _format_foreign_keys(unexpected_foreign_keys),
                )
            )
    return mismatches


def _validate_bootstrap_contract(
    document: Mapping[str, Any],
    physical: PhysicalSchema,
) -> None:
    """Validate the exact BUILDING seed/absence/epoch ownership partition."""

    raw_contract = document.get("bootstrap_contract")
    requires_contract = physical.logical_contract in {
        "h2hdb-vnext-catalog",
        "h2hdb-vnext-operational",
    }
    if raw_contract is None and not requires_contract:
        return
    if not isinstance(raw_contract, dict):
        raise ValueError("bootstrap_contract must be a table")
    if raw_contract.get("version") != 1:
        raise ValueError("bootstrap_contract.version must be one")
    for field in ("seed_validation_lifecycle", "absence_validation_lifecycle"):
        if raw_contract.get(field) != "building_only":
            raise ValueError(f"bootstrap_contract.{field} must be building_only")
    if raw_contract.get("epoch_owned_relation") != "schema_epoch_control":
        raise ValueError("bootstrap_contract epoch owner must be schema_epoch_control")
    seeded = _required_string_tuple(raw_contract, "seeded_relations")
    absent = _required_string_tuple(raw_contract, "absent_relations")
    if len(seeded) != len(set(seeded)) or len(absent) != len(set(absent)):
        raise ValueError("bootstrap_contract relation partitions contain duplicates")
    if set(seeded) & set(absent):
        raise ValueError("bootstrap_contract seeded and absent relations overlap")
    relation_names = {relation.relation for relation in physical.relations}
    if set(seeded) | set(absent) != relation_names:
        raise ValueError(
            "bootstrap_contract does not exactly partition physical relations"
        )
    is_data_contract = physical.logical_contract == "h2hdb-vnext-catalog"
    if is_data_contract:
        expected_seeded = {
            "analysis_stage",
            "artifact_storage_codec",
            "artifact_zip_writer_policy",
            "canonical_digest_policy",
            "channel_registry",
            "publication_stage",
            "source_provider_registry",
        }
        if set(seeded) != expected_seeded or len(absent) != len(relation_names) - 7:
            raise ValueError(
                "data bootstrap_contract must seed exactly seven registries and "
                "classify every other relation absent"
            )

    raw_seeds = _table_array(document.get("bootstrap_seed", []), "bootstrap_seed")
    seed_relations = {_required_string(seed, "relation") for seed in raw_seeds}
    if seed_relations != set(seeded):
        raise ValueError(
            "bootstrap seeds do not cover exactly the seeded relation partition"
        )
    for seed in raw_seeds:
        if seed.get("version") != 1:
            raise ValueError("bootstrap_seed.version must be one")
        cells = _table_array(seed.get("value", []), "bootstrap_seed.value")
        if not cells:
            raise ValueError("bootstrap_seed.value must not be empty")
        if not is_data_contract:
            continue
        for cell in cells:
            _required_string(cell, "attribute")
            if set(cell) == {"attribute", "type", "encoding", "text"}:
                if cell.get("type") != "ascii_enum" or cell.get("encoding") != "utf8":
                    raise ValueError(
                        "data text bootstrap cells must use ascii_enum/utf8"
                    )
                text = _required_string(cell, "text")
                try:
                    text.encode("ascii")
                except UnicodeEncodeError as error:
                    raise ValueError(
                        "data bootstrap seed text must be ASCII"
                    ) from error
            elif set(cell) == {"attribute", "type", "integer"}:
                integer = cell.get("integer")
                if cell.get("type") not in {"uint32", "uint64"} or (
                    not isinstance(integer, int)
                    or isinstance(integer, bool)
                    or integer < 0
                ):
                    raise ValueError(
                        "data integer bootstrap cells must use an unsigned exact type"
                    )
            else:
                raise ValueError(
                    "data bootstrap cells must be exact typed ASCII or integers"
                )

    for field in ("absence_rule", "epoch_rule"):
        if not isinstance(raw_contract.get(field), str) or not raw_contract[field]:
            raise ValueError(f"bootstrap_contract.{field} must be non-empty")


def _validate_physical_schema(
    physical: PhysicalSchema,
    logical: LogicalSchema,
) -> None:
    if physical.logical_contract != logical.name:
        raise ValueError(
            f"physical logical_contract {physical.logical_contract!r} does not "
            f"equal {logical.name!r}"
        )
    if physical.maximum_mariadb_index_bytes > 3072:
        raise ValueError("maximum_mariadb_index_bytes cannot exceed InnoDB 3072")
    if len(physical.runtime_obligations) != len(set(physical.runtime_obligations)):
        raise ValueError("runtime_obligations contains duplicates")
    bounded_names = [value.attribute for value in physical.bounded_values]
    if len(bounded_names) != len(set(bounded_names)):
        raise ValueError("bounded_value contains duplicate attributes")
    for bound in physical.bounded_values:
        if bound.maximum_bytes > physical.maximum_mariadb_index_bytes:
            raise ValueError(
                f"bounded value {bound.attribute!r} exceeds the InnoDB key budget"
            )
        if not bound.source.strip() or not bound.runtime_obligation.strip():
            raise ValueError(
                f"bounded value {bound.attribute!r} lacks source/validation detail"
            )
        if bound.runtime_obligation not in physical.runtime_obligations:
            raise ValueError(
                f"bounded value {bound.attribute!r} runtime obligation is not reported"
            )
    if physical.canonical_digest_protocol is not None:
        protocol = physical.canonical_digest_protocol
        if protocol.algorithm != "SHA-256":
            raise ValueError("canonical digest protocol must use SHA-256")
        if protocol.enforcement != (
            "bounded_stream_recompute_tree_validate_and_collision_compare"
        ):
            raise ValueError(
                "canonical digest protocol must declare bounded streamed tree "
                "validation and collision comparison"
            )
        relation_names = (
            protocol.policy_relation,
            protocol.allocation_relation,
            protocol.page_relation,
            protocol.descriptor_relation,
            protocol.parent_relation,
            protocol.value_relation,
        )
        protocol_relations = tuple(physical.relation(name) for name in relation_names)
        if any(relation is None for relation in protocol_relations):
            raise ValueError(
                "canonical digest protocol references a missing graph relation"
            )
        policy_relation, allocation, page, descriptor, parent, value_relation = (
            protocol_relations
        )
        assert policy_relation is not None
        assert allocation is not None
        assert page is not None
        assert descriptor is not None
        assert parent is not None
        assert value_relation is not None
        if allocation.primary_key != (protocol.digest_attribute,):
            raise ValueError("canonical value allocation must use the digest as SQL PK")
        if page.primary_key != ("page_sha256",) or page.runtime_unique_keys != (
            ("page_bytes",),
        ):
            raise ValueError(
                "canonical page must key raw digest and runtime-key exact bytes"
            )
        if descriptor.primary_key != ("page_sha256",) or descriptor.unique_keys != (
            (protocol.digest_attribute, "level", "page_position"),
        ):
            raise ValueError("canonical page descriptor keys are not exact")
        if parent.primary_key != ("child_sha256",) or parent.unique_keys != (
            ("parent_sha256", "position"),
        ):
            raise ValueError("canonical page parent keys are not exact")
        if value_relation.primary_key != (protocol.digest_attribute,) or (
            value_relation.unique_keys != ((protocol.root_attribute,),)
        ):
            raise ValueError("canonical final identity must key digest and root")
        expected_maria_types = {
            (protocol.allocation_relation, protocol.digest_attribute): "BINARY(32)",
            (protocol.allocation_relation, "digest_domain"): "VARBINARY(64)",
            (protocol.allocation_relation, protocol.byte_count_attribute): (
                "BIGINT UNSIGNED"
            ),
            (protocol.page_relation, "page_sha256"): "BINARY(32)",
            (protocol.page_relation, protocol.digest_attribute): "BINARY(32)",
            (protocol.page_relation, "page_bytes"): "MEDIUMBLOB",
            (protocol.value_relation, protocol.digest_attribute): "BINARY(32)",
            (protocol.value_relation, protocol.root_attribute): "BINARY(32)",
        }
        for (relation_name, attribute), expected_type in expected_maria_types.items():
            relation = physical.relation(relation_name)
            assert relation is not None
            column = next(
                (value for value in relation.columns if value.attribute == attribute),
                None,
            )
            if column is None or _normalize_type_name(
                column.mariadb.type_name
            ) != _normalize_type_name(expected_type):
                raise ValueError(
                    f"canonical graph {relation_name}.{attribute} must use "
                    f"{expected_type}"
                )
        if any(
            column.attribute == "value_bytes"
            for relation in (allocation, page, descriptor, parent, value_relation)
            for column in relation.columns
        ):
            raise ValueError("canonical graph must not store a monolithic value_bytes")
        if not any(
            "insert canonical_value_identity last" in value
            for value in physical.runtime_obligations
        ):
            raise ValueError("canonical graph lacks its final-seal runtime obligation")
    if physical.canonical_value_page_protocol is not None:
        page_protocol = physical.canonical_value_page_protocol
        expected_page_values = (
            page_protocol.codec_version,
            page_protocol.prefix,
            page_protocol.maximum_page_bytes,
            page_protocol.chunk_maximum_bytes,
            page_protocol.branch_capacity,
            page_protocol.maximum_level,
            page_protocol.maximum_byte_count,
            page_protocol.enforcement,
        )
        if expected_page_values != (
            1,
            "h2hdb-vnext-canonical-value-page\\0",
            65536,
            32768,
            256,
            8,
            9223372036854775807,
            "raw_sha256_exact_page_compare_and_owner_tree_seal",
        ):
            raise ValueError("canonical value page protocol constants drifted")
        if not all(
            term in page_protocol.framing
            for term in (
                "raw32(owner_value_sha256)",
                "u64be(page_position_int63)",
                "u64be(subtree_byte_count_int63)",
            )
        ):
            raise ValueError("canonical value page framing is incomplete")
    elif physical.canonical_digest_protocol is not None:
        raise ValueError("canonical digest graph lacks its page protocol")
    if physical.source_locator_protocol is not None:
        locator_protocol = physical.source_locator_protocol
        expected_framing = (
            "u32be(codec_version) || u32be(segment_count) || "
            "repeated(u32be(segment_length) || segment_utf8)"
        )
        if (
            locator_protocol.digest_domain != "source_relative_locator_v1"
            or locator_protocol.framing != expected_framing
        ):
            raise ValueError("source locator protocol must use exact nested framing")
        if locator_protocol.enforcement != "runtime_recompute_and_collision_compare":
            raise ValueError(
                "source locator protocol must declare runtime recomputation and "
                "collision comparison"
            )
        relation = physical.relation(locator_protocol.identity_relation)
        if relation is None or relation.kind != "table":
            raise ValueError(
                "source locator protocol references no physical identity table"
            )
        if relation.primary_key != (locator_protocol.digest_attribute,):
            raise ValueError("source locator physical PK must be locator_sha256")
        if relation.unique_keys:
            raise ValueError("nested locator leaf names must not be unique")
        columns = {column.attribute: column for column in relation.columns}
        expected_types = {
            locator_protocol.digest_attribute: "BINARY(32)",
            locator_protocol.name_attribute: "VARBINARY(255)",
        }
        for attribute, expected_type in expected_types.items():
            column = columns.get(attribute)
            if (
                column is None
                or _normalize_type_name(column.mariadb.type_name) != expected_type
            ):
                raise ValueError(
                    f"source locator protocol attribute {attribute!r} must use "
                    f"{expected_type}"
                )
        gallery = physical.relation(locator_protocol.gallery_relation)
        canonical = physical.relation(locator_protocol.canonical_value_relation)
        if gallery is None or gallery.primary_key != ("gallery_id",):
            raise ValueError("source locator protocol lacks gallery location identity")
        if set(gallery.unique_keys) != {
            ("gallery_key",),
            ("scope_key", locator_protocol.digest_attribute),
        }:
            raise ValueError(
                "gallery identity must uniquely key stable gallery_key and "
                "scope plus full locator"
            )
        if canonical is None or canonical.primary_key != ("value_sha256",):
            raise ValueError("source locator protocol lacks canonical exact bytes")
        if not any(
            "arbitrary-total-length" in value and "byte-compare" in value
            for value in physical.runtime_obligations
        ):
            raise ValueError("source locator protocol lacks its exact write obligation")

    if physical.relation("analysis_state_ancestry") is not None:
        overlay_phrases = (
            "both its shadow and tombstone",
            "acyclic, contiguous through every depth",
            "full evaluator for exact key/value equality",
        )
        for phrase in overlay_phrases:
            if not any(phrase in value for value in physical.runtime_obligations):
                raise ValueError(
                    f"bounded overlay runtime obligations do not cover {phrase!r}"
                )

    artifact_policy = physical.relation("artifact_policy")
    artifact_policy_semantics = physical.relation("artifact_policy_semantics")
    if artifact_policy is not None or artifact_policy_semantics is not None:
        if artifact_policy is None or artifact_policy_semantics is None:
            raise ValueError("artifact policy split is physically incomplete")
        if artifact_policy.primary_key != ("artifact_policy_id",) or (
            artifact_policy.unique_keys != (("policy_component_sha256",),)
        ):
            raise ValueError(
                "artifact policy must implement policy_id <-> policy component"
            )
        policy_mapping_columns = {
            column.attribute: column for column in artifact_policy.columns
        }
        if set(policy_mapping_columns) != {
            "artifact_policy_id",
            "policy_component_sha256",
        }:
            raise ValueError("artifact policy mapping has wrong columns")
        if artifact_policy_semantics.primary_key != (
            "policy_component_sha256",
        ) or artifact_policy_semantics.unique_keys != (
            (
                "artifact_algorithm_version",
                "max_image_short_side",
                "producer_fingerprint_sha256",
            ),
        ):
            raise ValueError(
                "artifact policy semantics must implement component <-> natural tuple"
            )
        semantic_columns = {
            column.attribute: column for column in artifact_policy_semantics.columns
        }
        if set(semantic_columns) != {
            "policy_component_sha256",
            "artifact_algorithm_version",
            "max_image_short_side",
            "producer_fingerprint_sha256",
        }:
            raise ValueError(
                "artifact policy physical columns are incomplete/redundant"
            )
        if not any(
            "max_image_short_side" in value
            and "algorithm/resize/producer tuple change" in value
            for value in physical.runtime_obligations
        ):
            raise ValueError(
                "artifact byte-producer policy runtime obligation is absent"
            )

        producer = physical.relation("artifact_producer_fingerprint")
        zip_policy = physical.relation("artifact_zip_writer_policy")
        storage_codec = physical.relation("artifact_storage_codec")
        if producer is None or zip_policy is None or storage_codec is None:
            raise ValueError("artifact closed producer/storage registries are missing")
        if producer.primary_key != ("producer_fingerprint_sha256",):
            raise ValueError("artifact producer fingerprint has wrong primary key")
        if zip_policy.primary_key != ("artifact_algorithm_version",):
            raise ValueError("artifact ZIP writer policy has wrong primary key")
        if storage_codec.primary_key != ("storage_codec_version",) or (
            storage_codec.unique_keys != (("adapter_id",),)
        ):
            raise ValueError("artifact storage codec has wrong equivalent keys")

        semantic_input = physical.relation("artifact_semantic_input")
        if semantic_input is None:
            raise ValueError("artifact semantic input relation is missing")
        expected_semantic_attributes = {
            "artifact_semantics_sha256",
            "source_manifest_component_sha256",
            "member_plan_component_sha256",
            "effective_content_component_sha256",
            "selected_component_sha256",
            "owner_component_sha256",
            "policy_component_sha256",
        }
        if {column.attribute for column in semantic_input.columns} != (
            expected_semantic_attributes
        ):
            raise ValueError("artifact semantic input has wrong six-component shape")
        if (
            semantic_input.primary_key != ("artifact_semantics_sha256",)
            or len(semantic_input.unique_keys) != 1
            or set(semantic_input.unique_keys[0])
            != (expected_semantic_attributes - {"artifact_semantics_sha256"})
        ):
            raise ValueError("artifact semantic input has wrong exact candidate keys")
        member_plan_phrases = (
            "including excluded entries",
            "no generated archive member",
            "resolved per-file exclusion decisions",
        )
        for phrase in member_plan_phrases:
            if not any(phrase in value for value in physical.runtime_obligations):
                raise ValueError(
                    f"artifact member-plan runtime obligations do not cover {phrase!r}"
                )
    implemented_relation_names = [relation.relation for relation in physical.relations]
    if len(implemented_relation_names) != len(set(implemented_relation_names)):
        raise ValueError("physical specification contains duplicate relation names")
    logical_names = {relation.name for relation in logical.relations}
    physical_names = set(implemented_relation_names)
    if logical_names != physical_names:
        raise ValueError(
            "physical relation coverage differs from the logical contract: "
            f"missing={sorted(logical_names - physical_names)!r} "
            f"unknown={sorted(physical_names - logical_names)!r}"
        )
    if len(physical.source_slice) != len(set(physical.source_slice)):
        raise ValueError("source_slice contains duplicate relation names")
    implemented_names = {
        relation.relation for relation in physical.implemented_relations
    }
    if set(physical.source_slice) != implemented_names:
        raise ValueError(
            "source_slice must equal the implemented relation set: "
            f"source_slice_only={sorted(set(physical.source_slice) - implemented_names)!r} "
            f"implemented_only={sorted(implemented_names - set(physical.source_slice))!r}"
        )

    tables = [
        relation.table
        for relation in physical.implemented_relations
        if relation.table is not None
    ]
    if len(tables) != len(set(tables)):
        raise ValueError("implemented relations must map to distinct SQL objects")

    # SQLite index names live in a database-wide namespace.  MariaDB only
    # requires table-local names, but using the stricter common denominator
    # makes one generated physical contract portable to both backends.
    index_names = [
        index.name
        for relation in physical.implemented_relations
        if relation.kind == "table"
        for index in relation.required_indexes
    ]
    if len(index_names) != len(set(index_names)):
        raise ValueError("required index names must be globally unique")

    for relation_spec in physical.implemented_relations:
        logical_relation = logical.relation(relation_spec.relation)
        assert logical_relation is not None
        if relation_spec.kind not in {"table", "view"}:
            raise ValueError(
                f"relation {relation_spec.relation!r} has unsupported kind "
                f"{relation_spec.kind!r}"
            )
        if relation_spec.kind == "table" and relation_spec.overlay_view is not None:
            raise ValueError(
                f"table relation {relation_spec.relation!r} declares a view pattern"
            )
        if relation_spec.kind == "view" and relation_spec.overlay_view is None:
            raise ValueError(
                f"view relation {relation_spec.relation!r} lacks a view pattern"
            )
        if relation_spec.kind == "view" and (
            relation_spec.required_indexes or relation_spec.checks
        ):
            raise ValueError(
                f"view relation {relation_spec.relation!r} cannot claim indexes "
                "or CHECK constraints"
            )
        attributes = tuple(column.attribute for column in relation_spec.columns)
        physical_columns = tuple(column.column for column in relation_spec.columns)
        if len(attributes) != len(set(attributes)):
            raise ValueError(
                f"relation {relation_spec.relation!r} maps an attribute twice"
            )
        if len(physical_columns) != len(set(physical_columns)):
            raise ValueError(f"relation {relation_spec.relation!r} maps a column twice")
        if set(attributes) != set(logical_relation.attributes):
            raise ValueError(
                f"relation {relation_spec.relation!r} column attributes differ "
                "from the logical contract"
            )
        candidate_keys = {
            frozenset(relation_spec.primary_key),
            *(frozenset(key) for key in relation_spec.unique_keys),
            *(frozenset(key) for key in relation_spec.runtime_unique_keys),
        }
        if candidate_keys != set(logical_relation.candidate_keys):
            raise ValueError(
                f"relation {relation_spec.relation!r} physical PK/UK do not "
                "equal the logical candidate keys"
            )
        for key in (
            relation_spec.primary_key,
            *relation_spec.unique_keys,
            *relation_spec.runtime_unique_keys,
        ):
            _require_attributes(relation_spec, key, "key")
        physical_foreign_keys = {
            (
                foreign_key.attributes,
                foreign_key.referenced_relation,
                foreign_key.referenced_attributes,
            )
            for foreign_key in relation_spec.foreign_keys
        }
        logical_foreign_keys = {
            (
                foreign_key.attributes,
                foreign_key.referenced_relation,
                foreign_key.referenced_attributes,
            )
            for foreign_key in logical_relation.foreign_keys
        }
        if physical_foreign_keys != logical_foreign_keys:
            raise ValueError(
                f"relation {relation_spec.relation!r} physical FKs do not "
                "equal the logical foreign keys"
            )
        for foreign_key in relation_spec.foreign_keys:
            _require_attributes(
                relation_spec,
                foreign_key.attributes,
                f"foreign key {foreign_key.name!r}",
            )
            target = physical.relation(foreign_key.referenced_relation)
            if target is None or target.status != "implemented":
                raise ValueError(
                    f"relation {relation_spec.relation!r} foreign key "
                    f"{foreign_key.name!r} targets a pending relation"
                )
            if relation_spec.kind == "table" and target.kind != "table":
                raise ValueError(
                    f"table relation {relation_spec.relation!r} foreign key "
                    f"{foreign_key.name!r} targets non-table relation "
                    f"{target.relation!r}"
                )
            _require_attributes(
                target,
                foreign_key.referenced_attributes,
                f"referenced key for {foreign_key.name!r}",
            )
        for index in relation_spec.required_indexes:
            _require_attributes(
                relation_spec,
                index.attributes,
                f"index {index.name!r}",
            )
        if relation_spec.kind == "table":
            child_access_paths = (
                relation_spec.primary_key,
                *relation_spec.unique_keys,
                *(index.attributes for index in relation_spec.required_indexes),
            )
            for foreign_key in relation_spec.foreign_keys:
                if not any(
                    access_path[: len(foreign_key.attributes)] == foreign_key.attributes
                    for access_path in child_access_paths
                ):
                    raise ValueError(
                        f"relation {relation_spec.relation!r} foreign key "
                        f"{foreign_key.name!r} lacks a child-side left-prefix "
                        "PK/UK/index"
                    )
        sql_keys = (
            relation_spec.primary_key,
            *relation_spec.unique_keys,
            *(index.attributes for index in relation_spec.required_indexes),
        )
        for key in sql_keys:
            width = sum(
                _mariadb_index_column_bytes(
                    relation_spec,
                    attribute,
                )
                for attribute in key
            )
            if width > physical.maximum_mariadb_index_bytes:
                raise ValueError(
                    f"relation {relation_spec.relation!r} index/key {key!r} "
                    f"requires {width} bytes, above "
                    f"{physical.maximum_mariadb_index_bytes}"
                )
        constraint_names = [
            *(foreign_key.name for foreign_key in relation_spec.foreign_keys),
            *(index.name for index in relation_spec.required_indexes),
            *(check.name for check in relation_spec.checks),
        ]
        if len(constraint_names) != len(set(constraint_names)):
            raise ValueError(
                f"relation {relation_spec.relation!r} reuses a constraint name"
            )

        if relation_spec.overlay_view is not None:
            overlay = relation_spec.overlay_view
            ancestry = physical.relation(overlay.ancestry_relation)
            shadow = physical.relation(overlay.shadow_relation)
            tombstone = physical.relation(overlay.tombstone_relation)
            if any(
                item is None or item.status != "implemented" or item.kind != "table"
                for item in (ancestry, shadow, tombstone)
            ):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} must reference "
                    "implemented ancestry, shadow, and tombstone tables"
                )
            assert ancestry is not None
            assert shadow is not None
            assert tombstone is not None
            if not relation_spec.primary_key or relation_spec.primary_key[0] != (
                "analysis_id"
            ):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} primary key must "
                    "start with analysis_id"
                )
            if {"resolved_analysis_id", "ancestor_depth"} & set(attributes):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} must keep "
                    "provenance analysis/depth internal to preserve BCNF"
                )
            if ancestry.relation != "analysis_state_ancestry":
                raise ValueError(
                    f"view relation {relation_spec.relation!r} must use "
                    "analysis_state_ancestry"
                )
            if relation_spec.primary_key != shadow.primary_key:
                raise ValueError(
                    f"view relation {relation_spec.relation!r} and shadow relation "
                    "must have the same ordered primary key"
                )
            if relation_spec.primary_key != tombstone.primary_key:
                raise ValueError(
                    f"view relation {relation_spec.relation!r} and tombstone "
                    "relation must have the same ordered primary key"
                )
            business_key = relation_spec.primary_key
            expected_shadow = tuple(
                attribute for attribute in logical_relation.attributes
            )
            expected_tombstone = business_key
            if set(column.attribute for column in shadow.columns) != set(
                expected_shadow
            ):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} shadow projection "
                    "does not equal resolved values"
                )
            if (
                tuple(column.attribute for column in tombstone.columns)
                != expected_tombstone
            ):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} tombstone projection "
                    "does not equal the business key"
                )


def _mariadb_index_column_bytes(
    relation: PhysicalRelationSpec,
    attribute: str,
) -> int:
    column = next(value for value in relation.columns if value.attribute == attribute)
    type_name = _normalize_type_name(column.mariadb.type_name)
    sized = re.fullmatch(r"(VAR)?BINARY\((\d+)\)", type_name)
    if sized:
        return int(sized.group(2))
    character = re.fullmatch(r"(?:VAR)?CHAR\((\d+)\)", type_name)
    if character:
        multiplier = 1 if column.mariadb.collation == "ascii_bin" else 4
        return int(character.group(1)) * multiplier
    if type_name.startswith("BIGINT"):
        return 8
    if type_name.startswith("INT"):
        return 4
    if type_name.startswith("TINYINT"):
        return 1
    if type_name.startswith("DATETIME"):
        return 8
    raise ValueError(
        f"relation {relation.relation!r} indexed attribute {attribute!r} "
        f"has unbounded/unsupported MariaDB type {column.mariadb.type_name!r}"
    )


def maximum_mariadb_index_width(
    physical: PhysicalSchema,
) -> tuple[int, str, tuple[str, ...]]:
    """Return the largest declared MariaDB PK/UK/required-index byte width."""

    maximum: tuple[int, str, tuple[str, ...]] = (0, "", ())
    for relation in physical.implemented_relations:
        if relation.kind != "table":
            continue
        keys = (
            relation.primary_key,
            *relation.unique_keys,
            *(index.attributes for index in relation.required_indexes),
        )
        for key in keys:
            width = sum(
                _mariadb_index_column_bytes(relation, attribute) for attribute in key
            )
            candidate = (width, relation.relation, tuple(key))
            if candidate > maximum:
                maximum = candidate
    return maximum


def _require_attributes(
    relation: PhysicalRelationSpec,
    attributes: Iterable[str],
    context: str,
) -> None:
    missing = set(attributes) - {column.attribute for column in relation.columns}
    if missing:
        raise ValueError(
            f"relation {relation.relation!r} {context} uses unknown attributes "
            f"{sorted(missing)!r}"
        )


def _backend_column_spec(value: Any, context: str) -> BackendColumnSpec:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be a table")
    type_name = _required_string(value, "type")
    nullable = value.get("nullable")
    collation_value = _required_string(value, "collation")
    if not isinstance(nullable, bool):
        raise ValueError(f"{context}.nullable must be boolean")
    collation = None if collation_value.upper() == "NONE" else collation_value
    if collation is not None:
        _validate_identifier(collation, f"{context}.collation")
    return BackendColumnSpec(type_name, nullable, collation)


def _table_array(value: Any, context: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{context} must be an array of tables")
    return tuple(value)


def _string_sequences(value: Any, context: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be an array of string arrays")
    return tuple(_string_sequence(item, context) for item in value)


def _validate_identifier(value: str, context: str) -> None:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise ValueError(f"{context} must be a simple SQL identifier")


def _normalize_type_name(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip()).upper()
    return re.sub(
        r"\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)",
        r"\1",
        normalized,
    )


def _normalize_collation(value: str | None) -> str | None:
    return None if value is None else value.casefold()


def _normalize_check_expression(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.replace("`", "").strip()).casefold()
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    while normalized.startswith("(") and normalized.endswith(")"):
        closing = _matching_parenthesis(normalized, 0)
        if closing != len(normalized) - 1:
            break
        normalized = normalized[1:-1].strip()
    return normalized


def _sqlite_column_collations(
    create_sql: str,
    columns: Iterable[str],
) -> dict[str, str]:
    definitions = _sqlite_table_definitions(create_sql)
    expected_columns = set(columns)
    result: dict[str, str] = {}
    for definition in definitions:
        identifier, remainder = _leading_sqlite_identifier(definition)
        if identifier not in expected_columns:
            continue
        match = re.search(
            r"\bCOLLATE\s+([A-Za-z_][A-Za-z0-9_]*)",
            remainder,
            flags=re.IGNORECASE,
        )
        if match is not None:
            result[identifier] = match.group(1)
    return result


def _extract_named_checks(create_sql: str) -> tuple[CheckShape, ...]:
    pattern = re.compile(
        r"\bCONSTRAINT\s+(?:\"([^\"]+)\"|`([^`]+)`|([A-Za-z_][A-Za-z0-9_]*))"
        r"\s+CHECK\s*\(",
        flags=re.IGNORECASE,
    )
    checks: list[CheckShape] = []
    for match in pattern.finditer(create_sql):
        opening = match.end() - 1
        closing = _matching_parenthesis(create_sql, opening)
        if closing is None:
            continue
        name = next(value for value in match.groups() if value is not None)
        checks.append(
            CheckShape(
                name,
                _normalize_check_expression(create_sql[opening + 1 : closing]),
            )
        )
    return tuple(sorted(checks))


def _sqlite_table_definitions(create_sql: str) -> tuple[str, ...]:
    opening = create_sql.find("(")
    if opening < 0:
        return ()
    closing = _matching_parenthesis(create_sql, opening)
    if closing is None:
        return ()
    body = create_sql[opening + 1 : closing]
    definitions: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    for position, character in enumerate(body):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {'"', "'", "`"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            definitions.append(body[start:position].strip())
            start = position + 1
    definitions.append(body[start:].strip())
    return tuple(definition for definition in definitions if definition)


def _leading_sqlite_identifier(definition: str) -> tuple[str, str]:
    stripped = definition.lstrip()
    if not stripped:
        return "", ""
    if stripped[0] in {'"', "`", "["}:
        closing = "]" if stripped[0] == "[" else stripped[0]
        end = stripped.find(closing, 1)
        if end < 0:
            return "", stripped
        return stripped[1:end], stripped[end + 1 :]
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", stripped)
    if match is None:
        return "", stripped
    return match.group(1), stripped[match.end() :]


def _matching_parenthesis(value: str, opening: int) -> int | None:
    depth = 0
    quote: str | None = None
    for position in range(opening, len(value)):
        character = value[position]
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {'"', "'", "`"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return position
    return None


def _required_string(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{key} must be a non-empty string")
    return result


def _required_string_tuple(value: Mapping[str, Any], key: str) -> tuple[str, ...]:
    return _string_sequence(value.get(key), key)


def _string_sequence(value: Any, context: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise ValueError(f"{context} must be a non-empty string array")
    return tuple(value)


def _quote_sqlite_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_mariadb_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _minimal_sets(values: Iterable[frozenset[str]]) -> list[frozenset[str]]:
    ordered = sorted(set(values), key=lambda value: (len(value), tuple(sorted(value))))
    return [value for value in ordered if not any(other < value for other in ordered)]


def _sorted_tuples(values: Iterable[tuple[str, ...]]) -> list[tuple[str, ...]]:
    return sorted(set(values), key=lambda value: (len(value), value))


def _format_names(values: Iterable[str]) -> str:
    return "{" + ", ".join(repr(value) for value in sorted(values)) + "}"


def _format_key(values: Iterable[str]) -> str:
    return "(" + ", ".join(repr(value) for value in sorted(values)) + ")"


def _format_keys(values: Iterable[frozenset[str]]) -> str:
    return (
        "{"
        + ", ".join(
            _format_key(value)
            for value in sorted(values, key=lambda key: (len(key), tuple(sorted(key))))
        )
        + "}"
    )


def _format_foreign_keys(values: Iterable[ForeignKeyShape]) -> str:
    return (
        "{"
        + ", ".join(
            f"{_format_key(value.columns)} -> {value.referenced_table!r}"
            f"{_format_key(value.referenced_columns)}"
            for value in sorted(values)
        )
        + "}"
    )
