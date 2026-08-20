"""Compare the logical vNext schema contract with an introspected SQL schema.

This module deliberately does not mutate or migrate a database.  It normalizes
SQLite and MariaDB metadata into one small model and reports whether selected
logical relations are realized by physical tables.  The vNext manifest and the
currently deployed schema are expected to differ; callers should inspect the
structured mismatch report rather than hiding that difference with ``xfail``.
"""

from __future__ import annotations

import hashlib
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
    definition: str | None = None

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
    referential_unique_keys: tuple[tuple[str, ...], ...] = ()


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
class VerticalViewMemberSpec:
    relation: str
    key_attributes: tuple[str, ...]
    value_attribute: str
    source_relation: str
    source_attributes: tuple[str, ...]
    member_attributes: tuple[str, ...]
    project: bool
    projection_attribute: str = ""
    required: bool = True


@dataclass(frozen=True)
class VerticalOptionalPresenceSpec:
    member_relation: str
    discriminator_relation: str
    discriminator_attribute: str
    present_value: str
    absent_values: tuple[str, ...]


@dataclass(frozen=True)
class SealedVerticalViewSpec:
    """Read-only join over PK-only anchor/seal and PK-plus-one satellites."""

    family: str
    anchor_relation: str
    seal_relation: str
    key_attributes: tuple[str, ...]
    members: tuple[VerticalViewMemberSpec, ...]
    projection_attributes: tuple[str, ...] = ()
    optional_presence: VerticalOptionalPresenceSpec | None = None


@dataclass(frozen=True)
class RevisionGenerationBaselineViewSpec:
    base_relation: str
    mapping_relation: str
    owner_attribute: str
    revision_attribute: str
    mapping_revision_attribute: str
    generation_attribute: str
    mapping_generation_attribute: str


@dataclass(frozen=True)
class RevisionGenerationHeadViewSpec:
    revision_relation: str
    time_relation: str
    mapping_relation: str
    channel_attribute: str
    revision_attribute: str
    generation_attribute: str
    time_attribute: str


@dataclass(frozen=True)
class DerivedViewSpec:
    """Closed-world read projection over explicitly named source relations."""

    pattern: str
    source_relations: tuple[str, ...]
    fields: tuple[tuple[str, str], ...] = ()

    def field(self, name: str) -> str:
        try:
            return dict(self.fields)[name]
        except KeyError as error:
            raise ValueError(
                f"derived view pattern {self.pattern!r} lacks field {name!r}"
            ) from error


@dataclass(frozen=True)
class PhysicalRelationSpec:
    relation: str
    status: str
    rationale: str
    table: str | None = None
    columns: tuple[PhysicalColumnSpec, ...] = ()
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    referential_unique_keys: tuple[tuple[str, ...], ...] = ()
    runtime_unique_keys: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[PhysicalForeignKeySpec, ...] = ()
    required_indexes: tuple[PhysicalIndexSpec, ...] = ()
    checks: tuple[PhysicalCheckSpec, ...] = ()
    kind: str = "table"
    overlay_view: OverlayViewSpec | None = None
    vertical_view: SealedVerticalViewSpec | None = None
    generation_baseline_view: RevisionGenerationBaselineViewSpec | None = None
    generation_head_view: RevisionGenerationHeadViewSpec | None = None
    derived_view: DerivedViewSpec | None = None

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


_DERIVED_VIEW_FIELDS: Mapping[str, frozenset[str]] = {
    "analysis_ancestry_endpoint": frozenset(),
    "analysis_gid_winner_keyset": frozenset(),
    "artifact_delta_old": frozenset(),
    "artifact_delta_new": frozenset(),
    "publication_candidate_projection": frozenset(),
    "batch_receipt_derived": frozenset(
        {
            "owner_attribute",
            "batch_key_attribute",
            "start_generation_attribute",
            "start_cursor_attribute",
            "start_processed_count_attribute",
            "next_cursor_attribute",
            "next_processed_count_attribute",
            "next_state_attribute",
            "row_count_attribute",
            "terminal_attribute",
            "committed_generation_attribute",
            "committed_at_attribute",
            "coordinate_relation",
            "stored_relation",
            "checkpoint_relation",
        }
    ),
    "publication_commit_baseline": frozenset({"projection"}),
    "publication_commit_published_descriptor": frozenset({"projection"}),
    "publication_commit_generation": frozenset({"projection"}),
    "publication_commit_head": frozenset(),
    "publication_commit_head_projection": frozenset({"projection"}),
    "publication_receipt": frozenset(),
    "publication_commit_activation": frozenset(),
}

_DERIVED_VIEW_OPTIONAL_FIELDS: Mapping[str, frozenset[str]] = {
    "batch_receipt_derived": frozenset({"stage_attribute", "page_limit_attribute"}),
}


def parse_derived_view_spec(
    raw_view: Mapping[str, Any], relation_name: str
) -> DerivedViewSpec:
    pattern = _required_string(raw_view, "pattern")
    fields = _DERIVED_VIEW_FIELDS.get(pattern)
    if fields is None:
        raise ValueError(
            f"view relation {relation_name!r}.view.pattern is unsupported: {pattern!r}"
        )
    optional_fields = _DERIVED_VIEW_OPTIONAL_FIELDS.get(pattern, frozenset())
    expected = {"pattern", "source_relations", *fields}
    allowed = {*expected, *optional_fields}
    unexpected = set(raw_view) - allowed
    missing = expected - set(raw_view)
    if unexpected or missing:
        raise ValueError(
            f"view relation {relation_name!r}.view fields drift: "
            f"missing={_format_names(missing)} unexpected={_format_names(unexpected)}"
        )
    sources = _required_string_tuple(raw_view, "source_relations")
    if not sources or len(sources) != len(set(sources)):
        raise ValueError(
            f"view relation {relation_name!r}.source_relations must be distinct"
        )
    return DerivedViewSpec(
        pattern=pattern,
        source_relations=sources,
        fields=tuple(
            (field, _required_string(raw_view, field))
            for field in sorted({*fields, *(set(raw_view) & optional_fields)})
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


def _catalog_external_physical_relations() -> dict[str, PhysicalRelationSpec]:
    """Return exact operational targets referenced by catalog-owned tables."""

    integer = BackendColumnSpec("INTEGER", False, None)
    unsigned = BackendColumnSpec("BIGINT UNSIGNED", False, None)
    blob = BackendColumnSpec("BLOB", False, None)
    binary16 = BackendColumnSpec("BINARY(16)", False, None)

    def column(
        attribute: str,
        sqlite: BackendColumnSpec,
        mariadb: BackendColumnSpec,
    ) -> PhysicalColumnSpec:
        return PhysicalColumnSpec(attribute, attribute, sqlite, mariadb)

    return {
        "operational_preparation_effect_seal": PhysicalRelationSpec(
            relation="operational_preparation_effect_seal",
            status="implemented",
            rationale="Exact cross-plane operational effect-seal target.",
            table="operational_operational_preparation_effect_seals",
            columns=(column("preparation_id", blob, binary16),),
            primary_key=("preparation_id",),
        ),
        "operational_policy": PhysicalRelationSpec(
            relation="operational_policy",
            status="implemented",
            rationale="Exact cross-plane operational policy target.",
            table="operational_operational_policys",
            columns=tuple(
                column(attribute, integer, unsigned)
                for attribute in (
                    "operational_policy_id",
                    "operational_schema_version",
                    "algorithm_version",
                    "max_batch_rows",
                )
            ),
            primary_key=("operational_policy_id",),
            unique_keys=(
                (
                    "operational_schema_version",
                    "algorithm_version",
                    "max_batch_rows",
                ),
            ),
        ),
    }


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
        referential_unique_keys = _string_sequences(
            raw_relation.get("referential_unique_keys", []),
            f"relation {relation_name!r}.referential_unique_keys",
        )
        relations.append(
            LogicalRelation(
                relation_name,
                attributes,
                tuple(_minimal_sets(keys)),
                tuple(foreign_keys),
                referential_unique_keys,
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
        vertical_view: SealedVerticalViewSpec | None = None
        generation_baseline_view: RevisionGenerationBaselineViewSpec | None = None
        generation_head_view: RevisionGenerationHeadViewSpec | None = None
        derived_view: DerivedViewSpec | None = None
        raw_view = raw_relation.get("view")
        if kind_value == "view":
            if not isinstance(raw_view, dict):
                raise ValueError(
                    f"view relation {relation_name!r}.view must be a table"
                )
            pattern = _required_string(raw_view, "pattern")
            if pattern == "nearest_ancestor_overlay":
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
            elif pattern == "sealed_vertical_family":
                unexpected_view = set(raw_view) - {
                    "pattern",
                    "family",
                    "anchor_relation",
                    "seal_relation",
                    "key_attributes",
                    "members",
                    "projection_attributes",
                    "optional_presence",
                }
                if unexpected_view:
                    raise ValueError(
                        f"view relation {relation_name!r}.view has unknown fields "
                        f"{_format_names(unexpected_view)}"
                    )
                vertical_view = SealedVerticalViewSpec(
                    family=_required_string(raw_view, "family"),
                    anchor_relation=_required_string(raw_view, "anchor_relation"),
                    seal_relation=_required_string(raw_view, "seal_relation"),
                    key_attributes=_required_string_tuple(raw_view, "key_attributes"),
                    members=tuple(
                        _parse_vertical_view_member(member)
                        for member in _table_array(
                            raw_view.get("members"),
                            f"view relation {relation_name!r}.view.members",
                        )
                    ),
                    projection_attributes=(
                        _required_string_tuple(raw_view, "projection_attributes")
                        if "projection_attributes" in raw_view
                        else ()
                    ),
                    optional_presence=(
                        VerticalOptionalPresenceSpec(
                            member_relation=_required_string(
                                _required_table(raw_view, "optional_presence"),
                                "member_relation",
                            ),
                            discriminator_relation=_required_string(
                                _required_table(raw_view, "optional_presence"),
                                "discriminator_relation",
                            ),
                            discriminator_attribute=_required_string(
                                _required_table(raw_view, "optional_presence"),
                                "discriminator_attribute",
                            ),
                            present_value=_required_string(
                                _required_table(raw_view, "optional_presence"),
                                "present_value",
                            ),
                            absent_values=_required_string_tuple(
                                _required_table(raw_view, "optional_presence"),
                                "absent_values",
                            ),
                        )
                        if "optional_presence" in raw_view
                        else None
                    ),
                )
            elif pattern == "revision_generation_baseline":
                expected_fields = {
                    "pattern",
                    "base_relation",
                    "mapping_relation",
                    "owner_attribute",
                    "revision_attribute",
                    "mapping_revision_attribute",
                    "generation_attribute",
                    "mapping_generation_attribute",
                }
                unexpected_view = set(raw_view) - expected_fields
                if unexpected_view:
                    raise ValueError(
                        f"view relation {relation_name!r}.view has unknown fields "
                        f"{_format_names(unexpected_view)}"
                    )
                generation_baseline_view = RevisionGenerationBaselineViewSpec(
                    base_relation=_required_string(raw_view, "base_relation"),
                    mapping_relation=_required_string(raw_view, "mapping_relation"),
                    owner_attribute=_required_string(raw_view, "owner_attribute"),
                    revision_attribute=_required_string(raw_view, "revision_attribute"),
                    mapping_revision_attribute=_required_string(
                        raw_view, "mapping_revision_attribute"
                    ),
                    generation_attribute=_required_string(
                        raw_view, "generation_attribute"
                    ),
                    mapping_generation_attribute=_required_string(
                        raw_view, "mapping_generation_attribute"
                    ),
                )
            elif pattern == "revision_generation_head":
                expected_fields = {
                    "pattern",
                    "revision_relation",
                    "time_relation",
                    "mapping_relation",
                    "channel_attribute",
                    "revision_attribute",
                    "generation_attribute",
                    "time_attribute",
                }
                unexpected_view = set(raw_view) - expected_fields
                if unexpected_view:
                    raise ValueError(
                        f"view relation {relation_name!r}.view has unknown fields "
                        f"{_format_names(unexpected_view)}"
                    )
                generation_head_view = RevisionGenerationHeadViewSpec(
                    revision_relation=_required_string(raw_view, "revision_relation"),
                    time_relation=_required_string(raw_view, "time_relation"),
                    mapping_relation=_required_string(raw_view, "mapping_relation"),
                    channel_attribute=_required_string(raw_view, "channel_attribute"),
                    revision_attribute=_required_string(raw_view, "revision_attribute"),
                    generation_attribute=_required_string(
                        raw_view, "generation_attribute"
                    ),
                    time_attribute=_required_string(raw_view, "time_attribute"),
                )
            else:
                derived_view = parse_derived_view_spec(raw_view, relation_name)
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
        referential_unique_keys = _string_sequences(
            raw_relation.get("referential_unique_keys", []),
            f"relation {relation_name!r}.referential_unique_keys",
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
                referential_unique_keys=referential_unique_keys,
                runtime_unique_keys=runtime_unique_keys,
                foreign_keys=tuple(foreign_keys),
                required_indexes=tuple(required_indexes),
                checks=tuple(checks),
                kind=kind_value,
                overlay_view=overlay_view,
                vertical_view=vertical_view,
                generation_baseline_view=generation_baseline_view,
                generation_head_view=generation_head_view,
                derived_view=derived_view,
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
                create_sql if object_kind == "view" else None,
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
    view_rows = reader.fetch_all("""
        SELECT TABLE_NAME, VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME
        """)
    view_definitions = {
        str(table_name): str(definition)
        for table_name, definition, *_ in view_rows
        if definition is not None
    }

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
                view_definitions.get(table_name),
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
    implemented_by_name = {relation.relation: relation for relation in implemented}
    mappings = tuple(relation.as_mapping() for relation in implemented)
    logical_report = compare_refinement(logical_schema, database, mappings)
    mismatches = list(logical_report.mismatches)
    for relation_spec in implemented:
        assert relation_spec.table is not None
        table = database.table(relation_spec.table)
        if table is None:
            continue
        mismatches.extend(
            _compare_physical_details(
                relation_spec,
                table,
                database.backend,
                implemented_by_name,
            )
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
    """Render external key stubs followed by the catalog source slice."""

    statements = ["PRAGMA foreign_keys = ON;"]
    relation_by_name = {
        relation.relation: relation
        for relation in physical_schema.implemented_relations
    }
    external_relations = _catalog_external_physical_relations()
    relation_by_name.update(external_relations)
    for relation_name in (*external_relations, *physical_schema.source_slice):
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                _render_view(
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
        for position, unique_key in enumerate(
            (*relation.unique_keys, *relation.referential_unique_keys), start=1
        ):
            unique_name = _portable_identifier(f"uk_{relation.table}_{position}")
            definitions.append(
                f"CONSTRAINT {_quote_sqlite_identifier(unique_name)} "
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
    """Render external key stubs followed by the catalog source slice."""

    statements: list[str] = []
    relation_by_name = {
        relation.relation: relation
        for relation in physical_schema.implemented_relations
    }
    external_relations = _catalog_external_physical_relations()
    relation_by_name.update(external_relations)
    for relation_name in (*external_relations, *physical_schema.source_slice):
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                _render_view(
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
        for position, unique_key in enumerate(
            (*relation.unique_keys, *relation.referential_unique_keys), start=1
        ):
            unique_name = _portable_identifier(f"uk_{relation.table}_{position}")
            definitions.append(
                f"CONSTRAINT {_quote_mariadb_identifier(unique_name)} "
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


def _render_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool = False,
) -> str:
    if relation.overlay_view is not None:
        return _render_overlay_view(
            relation,
            relation_by_name,
            backend,
            idempotent=idempotent,
        )
    if relation.vertical_view is not None:
        return _render_sealed_vertical_view(
            relation,
            relation_by_name,
            backend,
            idempotent=idempotent,
        )
    if relation.generation_baseline_view is not None:
        return _render_revision_generation_baseline_view(
            relation,
            relation_by_name,
            backend,
            idempotent=idempotent,
        )
    if relation.generation_head_view is not None:
        return _render_revision_generation_head_view(
            relation,
            relation_by_name,
            backend,
            idempotent=idempotent,
        )
    if relation.derived_view is not None:
        return _render_derived_view(
            relation,
            relation_by_name,
            backend,
            idempotent=idempotent,
        )
    raise ValueError(f"relation {relation.relation!r} has no supported view pattern")


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


def _render_sealed_vertical_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool = False,
) -> str:
    """Render a sealed join whose visible rows satisfy total/optional facts."""

    vertical = relation.vertical_view
    if vertical is None or relation.table is None:
        raise ValueError(f"relation {relation.relation!r} is not a vertical view")
    anchor = relation_by_name[vertical.anchor_relation]
    seal = relation_by_name[vertical.seal_relation]
    members = tuple(
        (member, relation_by_name[member.relation]) for member in vertical.members
    )
    if (
        anchor.table is None
        or seal.table is None
        or any(member_relation.table is None for _member, member_relation in members)
    ):
        raise ValueError(f"vertical view {relation.relation!r} has an unmapped source")
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )

    expression_by_attribute = {
        attribute: f"sealed.{quote(seal.column_for(attribute))}"
        for attribute in vertical.key_attributes
    }
    for position, (member, member_relation) in enumerate(members, start=1):
        if not member.project:
            continue
        for attribute in (column.attribute for column in member_relation.columns):
            if attribute in member.member_attributes:
                continue
            projection_attribute = (
                member.projection_attribute or member.value_attribute
                if attribute == member.value_attribute
                else attribute
            )
            expression_by_attribute[projection_attribute] = (
                f"member_{position}.{quote(member_relation.column_for(attribute))}"
            )
    projection = ",\n  ".join(
        f"{expression_by_attribute[column.attribute]} AS {quote(column.column)}"
        for column in relation.columns
    )

    anchor_predicate = "\n AND ".join(
        f"anchor.{quote(anchor.column_for(attribute))} "
        f"= sealed.{quote(seal.column_for(attribute))}"
        for attribute in vertical.key_attributes
    )
    joins = [f"JOIN {quote(str(anchor.table))} AS anchor\n ON " + anchor_predicate]
    alias_by_relation = {
        vertical.seal_relation: "sealed",
        vertical.anchor_relation: "anchor",
    }
    for position, (member, member_relation) in enumerate(members, start=1):
        source = relation_by_name[member.source_relation]
        source_alias = alias_by_relation[member.source_relation]
        member_alias = f"member_{position}"
        predicate = "\n AND ".join(
            f"{member_alias}.{quote(member_relation.column_for(member_attribute))} "
            f"= {source_alias}.{quote(source.column_for(source_attribute))}"
            for source_attribute, member_attribute in zip(
                member.source_attributes,
                member.member_attributes,
                strict=True,
            )
        )
        join_kind = "JOIN" if member.required else "LEFT JOIN"
        joins.append(
            f"{join_kind} {quote(str(member_relation.table))} AS {member_alias}\n ON "
            + predicate
        )
        alias_by_relation[member.relation] = member_alias
    if backend == "sqlite":
        prefix = "CREATE VIEW IF NOT EXISTS" if idempotent else "CREATE VIEW"
    else:
        prefix = (
            "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
            if idempotent
            else "CREATE SQL SECURITY INVOKER VIEW"
        )
    where = ""
    presence = vertical.optional_presence
    if presence is not None:
        optional_member = next(
            member
            for member, _member_relation in members
            if member.relation == presence.member_relation
        )
        optional_relation = relation_by_name[presence.member_relation]
        discriminator_member = next(
            member
            for member, _member_relation in members
            if member.relation == presence.discriminator_relation
        )
        discriminator_relation = relation_by_name[presence.discriminator_relation]
        discriminator_source_attribute = (
            discriminator_member.value_attribute
            if presence.discriminator_attribute
            == (
                discriminator_member.projection_attribute
                or discriminator_member.value_attribute
            )
            else presence.discriminator_attribute
        )

        def literal(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        discriminator_column = (
            f"{alias_by_relation[presence.discriminator_relation]}."
            f"{quote(discriminator_relation.column_for(discriminator_source_attribute))}"
        )
        optional_column = (
            f"{alias_by_relation[presence.member_relation]}."
            f"{quote(optional_relation.column_for(optional_member.value_attribute))}"
        )
        where = (
            "\nWHERE "
            f"{discriminator_column} = {literal(presence.present_value)} "
            f"AND {optional_column} IS NOT NULL\n"
            f"   OR {discriminator_column} IN "
            f"({', '.join(literal(value) for value in presence.absent_values)}) "
            f"AND {optional_column} IS NULL"
        )
    column_list = ", ".join(quote(column.column) for column in relation.columns)
    return f"""{prefix} {quote(relation.table)} ({column_list}) AS
SELECT
  {projection}
FROM {quote(str(seal.table))} AS sealed
{chr(10).join(joins)}{where}"""


def _view_prefix(backend: str, *, idempotent: bool) -> str:
    if backend == "sqlite":
        return "CREATE VIEW IF NOT EXISTS" if idempotent else "CREATE VIEW"
    return (
        "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
        if idempotent
        else "CREATE SQL SECURITY INVOKER VIEW"
    )


def _render_revision_generation_baseline_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool = False,
) -> str:
    spec = relation.generation_baseline_view
    if spec is None or relation.table is None:
        raise ValueError(f"relation {relation.relation!r} is not a baseline view")
    base = relation_by_name[spec.base_relation]
    mapping = relation_by_name[spec.mapping_relation]
    if base.table is None or mapping.table is None:
        raise ValueError(f"baseline view {relation.relation!r} has an unmapped source")
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )
    expressions = {
        spec.owner_attribute: f"base.{quote(base.column_for(spec.owner_attribute))}",
        spec.revision_attribute: (
            f"base.{quote(base.column_for(spec.revision_attribute))}"
        ),
        spec.generation_attribute: (
            f"mapping.{quote(mapping.column_for(spec.mapping_generation_attribute))}"
        ),
    }
    projection = ",\n  ".join(
        f"{expressions[column.attribute]} AS {quote(column.column)}"
        for column in relation.columns
    )
    columns = ", ".join(quote(column.column) for column in relation.columns)
    return f"""{_view_prefix(backend, idempotent=idempotent)} {quote(relation.table)} ({columns}) AS
SELECT
  {projection}
FROM {quote(base.table)} AS base
JOIN {quote(mapping.table)} AS mapping
  ON mapping.{quote(mapping.column_for(spec.mapping_revision_attribute))}
   = base.{quote(base.column_for(spec.revision_attribute))}"""


def _render_revision_generation_head_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool = False,
) -> str:
    spec = relation.generation_head_view
    if spec is None or relation.table is None:
        raise ValueError(f"relation {relation.relation!r} is not a head view")
    revision = relation_by_name[spec.revision_relation]
    timestamp = relation_by_name[spec.time_relation]
    mapping = relation_by_name[spec.mapping_relation]
    if revision.table is None or timestamp.table is None or mapping.table is None:
        raise ValueError(f"head view {relation.relation!r} has an unmapped source")
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )
    expressions = {
        spec.channel_attribute: (
            f"head.{quote(revision.column_for(spec.channel_attribute))}"
        ),
        spec.revision_attribute: (
            f"head.{quote(revision.column_for(spec.revision_attribute))}"
        ),
        spec.generation_attribute: (
            f"mapping.{quote(mapping.column_for(spec.generation_attribute))}"
        ),
        spec.time_attribute: (
            f"advanced.{quote(timestamp.column_for(spec.time_attribute))}"
        ),
    }
    projection = ",\n  ".join(
        f"{expressions[column.attribute]} AS {quote(column.column)}"
        for column in relation.columns
    )
    columns = ", ".join(quote(column.column) for column in relation.columns)
    return f"""{_view_prefix(backend, idempotent=idempotent)} {quote(relation.table)} ({columns}) AS
SELECT
  {projection}
FROM {quote(revision.table)} AS head
JOIN {quote(mapping.table)} AS mapping
  ON mapping.{quote(mapping.column_for(spec.revision_attribute))}
   = head.{quote(revision.column_for(spec.revision_attribute))}
JOIN {quote(timestamp.table)} AS advanced
  ON advanced.{quote(timestamp.column_for(spec.channel_attribute))}
   = head.{quote(revision.column_for(spec.channel_attribute))}"""


def _render_derived_projection(
    relation: PhysicalRelationSpec,
    backend: str,
    expressions: Mapping[str, str],
    from_sql: str,
    *,
    idempotent: bool,
) -> str:
    if relation.table is None:
        raise ValueError(f"view relation {relation.relation!r} has no SQL object")
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )
    missing = {column.attribute for column in relation.columns} - set(expressions)
    if missing:
        raise ValueError(
            f"view relation {relation.relation!r} lacks expressions for "
            f"{sorted(missing)!r}"
        )
    projection = ",\n  ".join(
        f"{expressions[column.attribute]} AS {quote(column.column)}"
        for column in relation.columns
    )
    columns = ", ".join(quote(column.column) for column in relation.columns)
    return f"""{_view_prefix(backend, idempotent=idempotent)} {quote(relation.table)} ({columns}) AS
SELECT
  {projection}
{from_sql}"""


def _render_derived_view(
    relation: PhysicalRelationSpec,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
    backend: str,
    *,
    idempotent: bool,
) -> str:
    spec = relation.derived_view
    if spec is None:
        raise ValueError(f"relation {relation.relation!r} is not a derived view")
    sources = tuple(relation_by_name[name] for name in spec.source_relations)
    if any(source.table is None for source in sources):
        raise ValueError(f"derived view {relation.relation!r} has an unmapped source")
    quote = (
        _quote_sqlite_identifier if backend == "sqlite" else _quote_mariadb_identifier
    )

    def table(source: PhysicalRelationSpec) -> str:
        assert source.table is not None
        return quote(source.table)

    def column(source: PhysicalRelationSpec, attribute: str) -> str:
        return quote(source.column_for(attribute))

    pattern = spec.pattern
    expressions: dict[str, str]
    from_sql: str
    if pattern == "analysis_ancestry_endpoint":
        (ancestry,) = sources
        analysis_id = column(ancestry, "analysis_id")
        ancestor_analysis_id = column(ancestry, "ancestor_analysis_id")
        ancestor_depth = column(ancestry, "ancestor_depth")
        expressions = {
            "analysis_id": f"endpoint.{analysis_id}",
            "anchor_analysis_id": f"endpoint.{ancestor_analysis_id}",
            "overlay_depth": f"endpoint.{ancestor_depth}",
        }
        from_sql = (
            f"FROM {table(ancestry)} AS endpoint\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT 1\n"
            f"  FROM {table(ancestry)} AS deeper\n"
            f"  WHERE deeper.{analysis_id} = endpoint.{analysis_id}\n"
            f"    AND deeper.{ancestor_depth} > endpoint.{ancestor_depth}\n"
            ")"
        )
    elif pattern == "analysis_gid_winner_keyset":
        source_by_name = {source.relation: source for source in sources}
        selection = source_by_name["analysis_gid_winner_selection"]
        impacted = source_by_name["analysis_impacted_gid"]
        run_build = source_by_name["analysis_run_build_id"]
        build_gallery = source_by_name["source_build_gallery"]
        metadata = source_by_name["gallery_observation_metadata"]
        expressions = {
            "analysis_id": f"selected.{column(selection, 'analysis_id')}",
            "gid": f"metadata.{column(metadata, 'gid')}",
            "winner_gallery_id": (f"selected.{column(selection, 'winner_gallery_id')}"),
        }
        from_sql = (
            f"FROM {table(selection)} AS selected\n"
            f"JOIN {table(run_build)} AS run_build\n"
            f"  ON run_build.{column(run_build, 'analysis_id')}\n"
            f"   = selected.{column(selection, 'analysis_id')}\n"
            f"JOIN {table(build_gallery)} AS build_gallery\n"
            f"  ON build_gallery.{column(build_gallery, 'build_id')}\n"
            f"   = run_build.{column(run_build, 'build_id')}\n"
            f" AND build_gallery.{column(build_gallery, 'gallery_id')}\n"
            f"   = selected.{column(selection, 'winner_gallery_id')}\n"
            f"JOIN {table(metadata)} AS metadata\n"
            f"  ON metadata.{column(metadata, 'gallery_id')}\n"
            f"   = build_gallery.{column(build_gallery, 'gallery_id')}\n"
            f" AND metadata.{column(metadata, 'observation_id')}\n"
            f"   = build_gallery.{column(build_gallery, 'observation_id')}\n"
            f"JOIN {table(impacted)} AS impacted\n"
            f"  ON impacted.{column(impacted, 'analysis_id')}\n"
            f"   = selected.{column(selection, 'analysis_id')}\n"
            f" AND impacted.{column(impacted, 'gid')}\n"
            f"   = metadata.{column(metadata, 'gid')}"
        )
    elif pattern == "artifact_delta_old":
        source_by_name = {source.relation: source for source in sources}
        candidate = source_by_name["publication_candidate"]
        base = source_by_name["publication_candidate_base_catalog"]
        occurrence = source_by_name["catalog_artifact"]
        expressions = {
            "candidate_id": f"base.{column(base, 'candidate_id')}",
            "publication_key": (f"occurrence.{column(occurrence, 'publication_key')}"),
            "artifact_semantics_sha256": (
                f"occurrence.{column(occurrence, 'artifact_semantics_sha256')}"
            ),
            "artifact_sha256": (f"occurrence.{column(occurrence, 'artifact_sha256')}"),
        }
        from_sql = (
            f"FROM {table(base)} AS base\n"
            f"JOIN {table(candidate)} AS candidate\n"
            f"  ON candidate.{column(candidate, 'candidate_id')}\n"
            f"   = base.{column(base, 'candidate_id')}\n"
            f"JOIN {table(occurrence)} AS occurrence\n"
            f"  ON occurrence.{column(occurrence, 'revision')}\n"
            f"   = base.{column(base, 'base_revision')}"
        )
    elif pattern == "artifact_delta_new":
        (artifact_input,) = sources
        expressions = {
            attribute: f"input.{column(artifact_input, attribute)}"
            for attribute in (
                "candidate_id",
                "publication_key",
                "artifact_semantics_sha256",
            )
        }
        from_sql = f"FROM {table(artifact_input)} AS input"
    elif pattern == "publication_candidate_projection":
        source_by_name = {source.relation: source for source in sources}
        seal = source_by_name["publication_candidate_projection_seal"]
        checkpoint = source_by_name["publication_checkpoint"]
        receipt = source_by_name["publication_batch_receipt"]

        def stage_literal(value: str) -> str:
            if backend == "sqlite":
                return "X'" + value.encode("ascii").hex().upper() + "'"
            return "'" + value.replace("'", "''") + "'"

        def state_literal(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        count_stages = (
            ("create_count", "VALIDATE_CREATE"),
            ("rebuild_count", "VALIDATE_REBUILD"),
            ("delete_count", "VALIDATE_DELETE"),
            ("new_galleries", "VALIDATE_NEW_GALLERY"),
            ("changed_galleries", "VALIDATE_CHANGED_GALLERY"),
        )
        expressions = {"candidate_id": f"certified.{column(seal, 'candidate_id')}"}
        for attribute, stage in count_stages:
            count_expression = (
                "MAX(CASE WHEN checkpoint."
                f"{column(checkpoint, 'stage')} = {stage_literal(stage)} "
                f"THEN receipt.{column(receipt, 'next_processed_count')} END)"
            )
            if backend == "mariadb":
                count_expression = f"CAST(COALESCE({count_expression}, 0) AS UNSIGNED)"
            expressions[attribute] = count_expression
        stage_literals = ", ".join(stage_literal(stage) for _, stage in count_stages)
        exact_stage_rows = " AND ".join(
            "SUM(CASE WHEN checkpoint."
            f"{column(checkpoint, 'stage')} = {stage_literal(stage)} "
            "THEN 1 ELSE 0 END) = 1"
            for _, stage in count_stages
        )
        from_sql = (
            f"FROM {table(seal)} AS certified\n"
            f"JOIN {table(checkpoint)} AS checkpoint\n"
            f"  ON checkpoint.{column(checkpoint, 'candidate_id')}\n"
            f"   = certified.{column(seal, 'candidate_id')}\n"
            f" AND checkpoint.{column(checkpoint, 'stage')} "
            f"IN ({stage_literals})\n"
            f" AND checkpoint.{column(checkpoint, 'state')} "
            f"= {state_literal('COMPLETE')}\n"
            f"JOIN {table(receipt)} AS receipt\n"
            f"  ON receipt.{column(receipt, 'candidate_id')}\n"
            f"   = checkpoint.{column(checkpoint, 'candidate_id')}\n"
            f" AND receipt.{column(receipt, 'stage')}\n"
            f"   = checkpoint.{column(checkpoint, 'stage')}\n"
            f" AND receipt.{column(receipt, 'committed_generation')}\n"
            f"   = checkpoint.{column(checkpoint, 'generation')}\n"
            f" AND receipt.{column(receipt, 'next_cursor')}\n"
            f"   = checkpoint.{column(checkpoint, 'cursor')}\n"
            f" AND receipt.{column(receipt, 'next_cursor')}\n"
            f"   = receipt.{column(receipt, 'start_cursor')}\n"
            f" AND receipt.{column(receipt, 'next_processed_count')}\n"
            f"   = checkpoint.{column(checkpoint, 'processed_count')}\n"
            f" AND receipt.{column(receipt, 'committed_at')}\n"
            f"   = checkpoint.{column(checkpoint, 'updated_at')}\n"
            f" AND receipt.{column(receipt, 'terminal')} = 1\n"
            f" AND receipt.{column(receipt, 'next_state')}\n"
            f"   = checkpoint.{column(checkpoint, 'state')}\n"
            f"GROUP BY certified.{column(seal, 'candidate_id')}\n"
            f"HAVING {exact_stage_rows}"
        )
    elif pattern == "batch_receipt_derived":
        (stored,) = sources
        expressions = {
            item.attribute: f"stored.{column(stored, item.attribute)}"
            for item in stored.columns
        }
        start_generation = spec.field("start_generation_attribute")
        start_count = spec.field("start_processed_count_attribute")
        row_count = spec.field("row_count_attribute")
        start_generation_sql = f"stored.{column(stored, start_generation)}"
        start_count_sql = f"stored.{column(stored, start_count)}"
        row_count_sql = f"stored.{column(stored, row_count)}"
        terminal_sql = f"CASE WHEN {row_count_sql} = 0 THEN 1 ELSE 0 END"
        next_state_sql = (
            f"CASE WHEN {row_count_sql} = 0 THEN 'COMPLETE' ELSE 'OPEN' END"
        )
        if backend == "mariadb":
            terminal_sql = f"CAST({terminal_sql} AS UNSIGNED)"
            next_state_sql = (
                f"CAST({next_state_sql} AS CHAR(32) CHARSET ascii) " "COLLATE ascii_bin"
            )
        expressions.update(
            {
                spec.field("committed_generation_attribute"): (
                    f"{start_generation_sql} + 1"
                ),
                spec.field("next_processed_count_attribute"): (
                    f"{start_count_sql} + {row_count_sql}"
                ),
                spec.field("terminal_attribute"): terminal_sql,
                spec.field("next_state_attribute"): next_state_sql,
            }
        )
        from_sql = f"FROM {table(stored)} AS stored"
    elif pattern == "publication_commit_baseline":
        base, commit = sources
        base_attributes = {item.attribute for item in base.columns}
        expressions = {}
        for item in relation.columns:
            attribute = item.attribute
            if attribute in base_attributes:
                expressions[attribute] = f"base.{column(base, attribute)}"
            elif attribute in {"base_source_revision", "base_revision"}:
                commit_attribute = (
                    "source_revision"
                    if attribute == "base_source_revision"
                    else "revision"
                )
                expressions[attribute] = f"committed.{column(commit, commit_attribute)}"
            elif attribute in {
                "base_source_generation",
                "base_catalog_generation",
            }:
                expressions[attribute] = f"committed.{column(commit, 'generation')}"
        from_sql = (
            f"FROM {table(base)} AS base\nJOIN {table(commit)} AS committed\n"
            f"  ON committed.{column(commit, 'receipt_id')}\n"
            f"   = base.{column(base, 'base_receipt_id')}"
        )
    elif pattern == "publication_commit_published_descriptor":
        descriptor, commit = sources
        descriptor_attributes = {item.attribute for item in descriptor.columns}
        expressions = {
            attribute: f"descriptor.{column(descriptor, attribute)}"
            for attribute in descriptor_attributes
        }
        expressions["published_at"] = f"committed.{column(commit, 'committed_at')}"
        join_attribute = (
            "source_revision"
            if "source_revision" in descriptor_attributes
            else "revision"
        )
        from_sql = (
            f"FROM {table(descriptor)} AS descriptor\n"
            f"JOIN {table(commit)} AS committed\n"
            f"  ON committed.{column(commit, join_attribute)}\n"
            f"   = descriptor.{column(descriptor, join_attribute)}"
        )
    elif pattern == "publication_commit_generation":
        (commit,) = sources
        expressions = {
            item.attribute: f"committed.{column(commit, item.attribute)}"
            for item in relation.columns
        }
        from_sql = f"FROM {table(commit)} AS committed"
    elif pattern == "publication_commit_head":
        head, commit, source_descriptor = sources
        expressions = {
            item.attribute: (
                f"head.{column(head, 'channel')}"
                if item.attribute == "channel"
                else f"committed.{column(commit, item.attribute)}"
            )
            for item in relation.columns
        }
        from_sql = (
            f"FROM {table(head)} AS head\nJOIN {table(commit)} AS committed\n"
            f"  ON committed.{column(commit, 'receipt_id')}\n"
            f"   = head.{column(head, 'receipt_id')}\n"
            f"JOIN {table(source_descriptor)} AS source_descriptor\n"
            f"  ON source_descriptor.{column(source_descriptor, 'source_revision')}\n"
            f"   = committed.{column(commit, 'source_revision')}\n"
            f" AND source_descriptor.{column(source_descriptor, 'channel')}\n"
            f"   = head.{column(head, 'channel')}"
        )
    elif pattern == "publication_commit_head_projection":
        (head,) = sources
        expressions = {
            item.attribute: f"head.{column(head, 'committed_at' if item.attribute == 'advanced_at' else item.attribute)}"
            for item in relation.columns
        }
        from_sql = f"FROM {table(head)} AS head"
    elif pattern == "publication_receipt":
        source_by_name = {source.relation: source for source in sources}
        commit = source_by_name["publication_commit"]
        catalog_descriptor = source_by_name["catalog_revision_descriptor"]
        source_descriptor = source_by_name["source_revision_descriptor"]
        finalization = source_by_name["publication_commit_finalization"]
        checkpoint = source_by_name["publication_finalization_checkpoint"]
        final_receipt = source_by_name["publication_finalization_batch_receipt"]

        def finalization_literal(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        expressions = {
            item.attribute: f"committed.{column(commit, item.attribute)}"
            for item in commit.columns
        }
        terminal_predicate = (
            f"terminal.{column(final_receipt, 'receipt_id')}\n"
            f"     = final_checkpoint.{column(checkpoint, 'receipt_id')}\n"
            f" AND terminal.{column(final_receipt, 'committed_generation')}\n"
            f"     = final_checkpoint.{column(checkpoint, 'generation')}\n"
            f" AND terminal.{column(final_receipt, 'next_cursor')}\n"
            f"     = final_checkpoint.{column(checkpoint, 'cursor')}\n"
            f" AND terminal.{column(final_receipt, 'next_cursor')}\n"
            f"     = terminal.{column(final_receipt, 'start_cursor')}\n"
            f" AND terminal.{column(final_receipt, 'next_processed_count')}\n"
            f"     = final_checkpoint.{column(checkpoint, 'processed_count')}\n"
            f" AND terminal.{column(final_receipt, 'committed_at')}\n"
            f"     = final_checkpoint.{column(checkpoint, 'updated_at')}\n"
            f" AND terminal.{column(final_receipt, 'terminal')} = 1\n"
            f" AND terminal.{column(final_receipt, 'row_count')} = 0\n"
            f" AND terminal.{column(final_receipt, 'next_state')} "
            f"= {finalization_literal('COMPLETE')}"
        )
        state_expression = (
            f"CASE WHEN finalized.{column(finalization, 'receipt_id')} "
            f"IS NULL THEN {finalization_literal('DB_COMMITTED')} ELSE "
            f"{finalization_literal('PROJECTION_FINALIZED')} END"
        )
        finalized_at_expression = (
            f"CASE WHEN finalized.{column(finalization, 'receipt_id')} "
            "IS NULL THEN NULL ELSE (\n"
            f"  SELECT terminal.{column(final_receipt, 'committed_at')}\n"
            f"  FROM {table(final_receipt)} AS terminal\n"
            f"  WHERE {terminal_predicate}\n"
            ") END"
        )
        if backend == "mariadb":
            state_expression = (
                f"CAST({state_expression} AS CHAR(32) CHARSET ascii) "
                "COLLATE ascii_bin"
            )
            finalized_at_expression = f"CAST({finalized_at_expression} AS UNSIGNED)"
        expressions.update(
            {
                "channel": f"source_descriptor.{column(source_descriptor, 'channel')}",
                "publication_count": (
                    f"catalog_descriptor.{column(catalog_descriptor, 'publication_count')}"
                ),
                "state": state_expression,
                "finalized_at": finalized_at_expression,
            }
        )
        from_sql = (
            f"FROM {table(commit)} AS committed\n"
            f"JOIN {table(catalog_descriptor)} AS catalog_descriptor\n"
            f"  ON catalog_descriptor.{column(catalog_descriptor, 'revision')}\n"
            f"   = committed.{column(commit, 'revision')}\n"
            f"JOIN {table(source_descriptor)} AS source_descriptor\n"
            f"  ON source_descriptor.{column(source_descriptor, 'source_revision')}\n"
            f"   = committed.{column(commit, 'source_revision')}\n"
            f"JOIN {table(checkpoint)} AS final_checkpoint\n"
            f"  ON final_checkpoint.{column(checkpoint, 'receipt_id')}\n"
            f"   = committed.{column(commit, 'receipt_id')}\n"
            f"LEFT JOIN {table(finalization)} AS finalized\n"
            f"  ON finalized.{column(finalization, 'receipt_id')}\n"
            f"   = committed.{column(commit, 'receipt_id')}\n"
            f"WHERE finalized.{column(finalization, 'receipt_id')} IS NULL\n"
            f"       AND final_checkpoint.{column(checkpoint, 'state')} "
            f"= {finalization_literal('OPEN')}\n"
            f"   OR finalized.{column(finalization, 'receipt_id')} IS NOT NULL\n"
            f"       AND final_checkpoint.{column(checkpoint, 'state')} "
            f"= {finalization_literal('COMPLETE')}\n"
            "       AND EXISTS (\n"
            "         SELECT 1\n"
            f"         FROM {table(final_receipt)} AS terminal\n"
            f"         WHERE {terminal_predicate}\n"
            "       )"
        )
    elif pattern == "publication_commit_activation":
        source_by_name = {source.relation: source for source in sources}
        seal = source_by_name["publication_commit_seal"]
        source_member = source_by_name["publication_commit_source_revision"]
        preparation = source_by_name["publication_commit_operational_preparation"]
        policy = source_by_name["publication_commit_operational_policy"]
        timestamp = source_by_name["publication_commit_committed_at"]
        expressions = {
            "source_revision": (
                f"source_member.{column(source_member, 'source_revision')}"
            ),
            "preparation_id": (
                f"preparation_member.{column(preparation, 'preparation_id')}"
            ),
            "operational_policy_id": (
                f"policy_member.{column(policy, 'operational_policy_id')}"
            ),
            "activated_at": f"time_member.{column(timestamp, 'committed_at')}",
        }
        joins = []
        for source, alias in (
            (source_member, "source_member"),
            (preparation, "preparation_member"),
            (policy, "policy_member"),
            (timestamp, "time_member"),
        ):
            joins.append(
                f"JOIN {table(source)} AS {alias}\n"
                f"  ON {alias}.{column(source, 'receipt_id')}\n"
                f"   = sealed.{column(seal, 'receipt_id')}"
            )
        from_sql = f"FROM {table(seal)} AS sealed\n" + "\n".join(joins)
    else:  # pragma: no cover - parser is closed-world
        raise ValueError(f"unsupported derived view pattern {pattern!r}")

    return _render_derived_projection(
        relation,
        backend,
        expressions,
        from_sql,
        idempotent=idempotent,
    )


def _compare_physical_details(
    relation: PhysicalRelationSpec,
    table: TableShape,
    backend: str,
    relation_by_name: Mapping[str, PhysicalRelationSpec],
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
        # verifies that the object is genuinely a view, that its exact
        # projection is present, and that the normalized SELECT/join graph has
        # not drifted, without pretending table constraints exist.
        if table.definition is None:
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "missing-view-definition",
                    f"backend {backend!r} did not expose the view definition",
                )
            )
            return mismatches
        expected_definition = _render_view(
            relation,
            relation_by_name,
            backend,
            idempotent=False,
        )
        table_names = tuple(
            source.table
            for source in relation_by_name.values()
            if source.table is not None
        )
        collapse_inner_join_tree = backend == "mariadb"
        actual_normalized = _normalize_view_definition(
            table.definition,
            table_names=table_names,
            collapse_inner_join_tree=collapse_inner_join_tree,
        )
        expected_normalized = _normalize_view_definition(
            expected_definition,
            table_names=table_names,
            collapse_inner_join_tree=collapse_inner_join_tree,
        )
        if actual_normalized != expected_normalized:
            mismatches.append(
                RefinementMismatch(
                    relation.relation,
                    "view-definition",
                    f"view {table.name!r} normalized definition differs from "
                    "the generated join projection",
                )
            )
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
        for key in (*relation.unique_keys, *relation.referential_unique_keys)
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
            external_target = _catalog_external_physical_relations().get(
                foreign_key.referenced_relation
            )
            if external_target is not None and external_target.table is not None:
                expected_foreign_keys.add(
                    ForeignKeyShape(
                        tuple(
                            mapping.column_for(value)
                            for value in foreign_key.attributes
                        ),
                        external_target.table,
                        tuple(
                            external_target.column_for(value)
                            for value in foreign_key.referenced_attributes
                        ),
                    )
                )
                continue
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
    raw_derived = raw_contract.get("derived_relations", [])
    if not isinstance(raw_derived, list) or not all(
        isinstance(value, str) and value for value in raw_derived
    ):
        raise ValueError("bootstrap_contract.derived_relations must be a string array")
    derived = tuple(raw_derived)
    if (
        len(seeded) != len(set(seeded))
        or len(absent) != len(set(absent))
        or len(derived) != len(set(derived))
    ):
        raise ValueError("bootstrap_contract relation partitions contain duplicates")
    partitions = (set(seeded), set(absent), set(derived))
    if any(
        left & right
        for position, left in enumerate(partitions)
        for right in partitions[position + 1 :]
    ):
        raise ValueError("bootstrap_contract relation partitions overlap")
    relation_names = {relation.relation for relation in physical.relations}
    if set(seeded) | set(absent) | set(derived) != relation_names:
        raise ValueError(
            "bootstrap_contract does not exactly partition physical relations"
        )
    is_data_contract = physical.logical_contract == "h2hdb-vnext-catalog"
    if is_data_contract:
        raw_seeds = document.get("bootstrap_seed", [])
        if not isinstance(raw_seeds, list) or not all(
            isinstance(seed, dict)
            and isinstance(seed.get("relation"), str)
            and seed["relation"]
            for seed in raw_seeds
        ):
            raise ValueError("data bootstrap_seed registry is malformed")
        expected_seeded = {str(seed["relation"]) for seed in raw_seeds}
        expected_derived = {
            relation.relation
            for relation in physical.relations
            if relation.kind == "view"
        }
        expected_absent = relation_names - expected_seeded - expected_derived
        if (
            set(seeded) != expected_seeded
            or set(absent) != expected_absent
            or set(derived) != expected_derived
        ):
            raise ValueError(
                "data bootstrap_contract must seed exact base facts, classify every "
                "other base absent, and classify every SQL view as derived"
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
        allocation_family_names = (
            "canonical_value_allocation_anchor",
            "canonical_value_allocation_digest_domain",
            "canonical_value_allocation_byte_count",
            "canonical_value_allocation_allocated_at",
            "canonical_value_allocation_seal",
        )
        page_family_names = (
            "canonical_value_page_anchor",
            "canonical_value_page_payload",
            "canonical_value_page_coordinate",
            "canonical_value_page_subtree_item_count",
            "canonical_value_page_seal",
        )
        allocation_family = tuple(
            physical.relation(name) for name in allocation_family_names
        )
        page_family = tuple(physical.relation(name) for name in page_family_names)
        if any(
            relation is None or relation.kind != "table"
            for relation in (*allocation_family, *page_family)
        ):
            raise ValueError("canonical digest graph lacks a narrow base family")
        (
            allocation_anchor,
            allocation_domain,
            allocation_count,
            allocation_time,
            allocation_seal,
        ) = allocation_family
        (
            page_anchor,
            page_payload,
            page_coordinate,
            page_count,
            page_seal,
        ) = page_family
        assert allocation_anchor is not None
        assert allocation_domain is not None
        assert allocation_count is not None
        assert allocation_time is not None
        assert allocation_seal is not None
        assert page_anchor is not None
        assert page_payload is not None
        assert page_coordinate is not None
        assert page_count is not None
        assert page_seal is not None
        allocation_base_relations = (
            allocation_anchor,
            allocation_domain,
            allocation_count,
            allocation_time,
            allocation_seal,
        )
        page_base_relations = (
            page_anchor,
            page_payload,
            page_coordinate,
            page_count,
            page_seal,
        )
        if (
            allocation.kind != "view"
            or allocation.vertical_view is None
            or allocation.primary_key != (protocol.digest_attribute,)
            or tuple(relation.primary_key for relation in allocation_base_relations)
            != ((protocol.digest_attribute,),) * len(allocation_base_relations)
        ):
            raise ValueError(
                "canonical value allocation must be a sealed narrow digest family"
            )
        if (
            page.kind != "view"
            or page.vertical_view is None
            or page.primary_key != ("page_sha256",)
            or page.runtime_unique_keys != (("page_bytes",),)
            or page_payload.primary_key != ("page_sha256",)
            or page_payload.runtime_unique_keys != (("page_bytes",),)
        ):
            raise ValueError(
                "canonical page must project a sealed collision-checked payload"
            )
        if (
            descriptor.kind != "view"
            or descriptor.vertical_view is None
            or descriptor.primary_key != ("page_sha256",)
            or descriptor.unique_keys
            != ((protocol.digest_attribute, "level", "page_position"),)
            or page_coordinate.primary_key
            != (protocol.digest_attribute, "level", "page_position")
            or page_coordinate.unique_keys != (("page_sha256",),)
        ):
            raise ValueError("canonical page descriptor keys are not exact")
        if (
            page.vertical_view.family != descriptor.vertical_view.family
            or page.vertical_view.seal_relation
            != descriptor.vertical_view.seal_relation
            or page.vertical_view.anchor_relation
            != descriptor.vertical_view.anchor_relation
            or page.vertical_view.projection_attributes
            != ("page_sha256", protocol.digest_attribute, "page_bytes")
            or descriptor.vertical_view.projection_attributes
        ):
            raise ValueError(
                "canonical page and descriptor must share one sealed family"
            )
        if parent.primary_key != (
            "parent_sha256",
            "position",
        ) or parent.unique_keys != (("child_sha256",),):
            raise ValueError("canonical page parent keys are not exact")
        if value_relation.primary_key != (protocol.digest_attribute,) or (
            value_relation.unique_keys != ((protocol.root_attribute,),)
        ):
            raise ValueError("canonical final identity must key digest and root")
        expected_maria_types = {
            (
                "canonical_value_allocation_anchor",
                protocol.digest_attribute,
            ): "BINARY(32)",
            (
                "canonical_value_allocation_digest_domain",
                "digest_domain",
            ): "VARBINARY(64)",
            ("canonical_value_allocation_byte_count", protocol.byte_count_attribute): (
                "BIGINT UNSIGNED"
            ),
            ("canonical_value_page_payload", "page_sha256"): "BINARY(32)",
            (
                "canonical_value_page_coordinate",
                protocol.digest_attribute,
            ): "BINARY(32)",
            ("canonical_value_page_payload", "page_bytes"): "MEDIUMBLOB",
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
            for relation in (
                *allocation_base_relations,
                *page_base_relations,
                parent,
                value_relation,
            )
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
        if (
            producer.kind != "view"
            or producer.primary_key != ("producer_fingerprint_sha256",)
            or producer.unique_keys
            != (
                ("producer_equivalence_class",),
                (
                    "writer_id",
                    "python_abi",
                    "pillow_build",
                    "libjpeg_build",
                    "zlib_build",
                ),
            )
        ):
            raise ValueError("artifact producer fingerprint view has wrong keys")
        producer_equivalence = physical.relation(
            "artifact_producer_fingerprint_equivalence_class"
        )
        if (
            producer_equivalence is None
            or producer_equivalence.primary_key != ("producer_fingerprint_sha256",)
            or producer_equivalence.unique_keys != (("producer_equivalence_class",),)
        ):
            raise ValueError("artifact producer equivalence codec has wrong keys")
        producer_identity = physical.relation("artifact_producer_fingerprint_identity")
        if (
            producer_identity is None
            or producer_identity.primary_key
            != (
                "writer_id",
                "python_abi",
                "pillow_build",
                "libjpeg_build",
                "zlib_build",
            )
            or producer_identity.unique_keys != (("producer_fingerprint_sha256",),)
        ):
            raise ValueError("artifact producer natural identity has wrong keys")
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

    impacted_lookup_indexes = {
        "analysis_impacted_content_provenance": PhysicalIndexSpec(
            "ix_a_impacted_content_key_gallery",
            ("analysis_id", "content_sha256", "gallery_id"),
            False,
        ),
        "analysis_impacted_gid_provenance": PhysicalIndexSpec(
            "ix_a_impacted_gid_key_gallery",
            ("analysis_id", "gid", "gallery_id"),
            False,
        ),
    }
    for relation_name, expected_index in impacted_lookup_indexes.items():
        relation = physical.relation(relation_name)
        if relation is None or relation.kind != "table":
            raise ValueError(
                f"impacted provenance relation {relation_name!r} is not a table"
            )
        matching_indexes = tuple(
            index
            for index in relation.required_indexes
            if index.attributes == expected_index.attributes
        )
        if matching_indexes != (expected_index,):
            raise ValueError(
                f"impacted provenance relation {relation_name!r} must expose "
                f"the exact key-first lookup index {expected_index!r}"
            )

    for relation_spec in physical.implemented_relations:
        logical_relation = logical.relation(relation_spec.relation)
        assert logical_relation is not None
        if relation_spec.kind not in {"table", "view"}:
            raise ValueError(
                f"relation {relation_spec.relation!r} has unsupported kind "
                f"{relation_spec.kind!r}"
            )
        view_pattern_count = sum(
            value is not None
            for value in (
                relation_spec.overlay_view,
                relation_spec.vertical_view,
                relation_spec.generation_baseline_view,
                relation_spec.generation_head_view,
                relation_spec.derived_view,
            )
        )
        if relation_spec.kind == "table" and view_pattern_count:
            raise ValueError(
                f"table relation {relation_spec.relation!r} declares a view pattern"
            )
        if relation_spec.kind == "view" and view_pattern_count != 1:
            raise ValueError(
                f"view relation {relation_spec.relation!r} must declare exactly "
                "one view pattern"
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
        if (
            relation_spec.referential_unique_keys
            != logical_relation.referential_unique_keys
        ):
            raise ValueError(
                f"relation {relation_spec.relation!r} physical referential UKs do "
                "not equal the logical structural declarations"
            )
        for key in (
            relation_spec.primary_key,
            *relation_spec.unique_keys,
            *relation_spec.referential_unique_keys,
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
            if target is None:
                external_key_registry: dict[str, set[tuple[str, ...]]] = {
                    "operational_preparation_effect_seal": {
                        ("preparation_id",),
                    },
                    "operational_policy": {
                        ("operational_policy_id",),
                        (
                            "operational_schema_version",
                            "algorithm_version",
                            "max_batch_rows",
                        ),
                    },
                }
                external_keys = external_key_registry.get(
                    foreign_key.referenced_relation
                )
                if (
                    external_keys is not None
                    and foreign_key.referenced_attributes in external_keys
                ):
                    continue
                raise ValueError(
                    f"relation {relation_spec.relation!r} foreign key "
                    f"{foreign_key.name!r} targets an unknown external relation"
                )
            if target.status != "implemented":
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
            target_keys = {
                target.primary_key,
                *target.unique_keys,
                *target.referential_unique_keys,
                *target.runtime_unique_keys,
            }
            if foreign_key.referenced_attributes not in target_keys:
                raise ValueError(
                    f"relation {relation_spec.relation!r} foreign key "
                    f"{foreign_key.name!r} does not target a physical PK/UK"
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
                *relation_spec.referential_unique_keys,
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
            *relation_spec.referential_unique_keys,
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
            if (
                ancestry is None
                or ancestry.status != "implemented"
                or ancestry.kind != "table"
                or tombstone is None
                or tombstone.status != "implemented"
                or tombstone.kind != "table"
                or shadow is None
                or shadow.status != "implemented"
                or (
                    shadow.kind == "view"
                    and shadow.vertical_view is None
                    and shadow.derived_view is None
                )
                or shadow.kind not in {"table", "view"}
            ):
                raise ValueError(
                    f"view relation {relation_spec.relation!r} must reference "
                    "an implemented ancestry table, a table or closed derived "
                    "shadow view, and a tombstone table"
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

        if relation_spec.vertical_view is not None:
            vertical = relation_spec.vertical_view
            anchor = physical.relation(vertical.anchor_relation)
            seal = physical.relation(vertical.seal_relation)
            members = tuple(
                (member, physical.relation(member.relation))
                for member in vertical.members
            )
            if any(
                item is None or item.status != "implemented" or item.kind != "table"
                for item in (anchor, seal, *(relation for _member, relation in members))
            ):
                raise ValueError(
                    f"vertical view {relation_spec.relation!r} must reference only "
                    "implemented base tables"
                )
            assert anchor is not None
            assert seal is not None
            key = vertical.key_attributes
            if len(key) != len(set(key)) or not key:
                raise ValueError(
                    f"vertical view {relation_spec.relation!r} has an invalid key"
                )
            if relation_spec.primary_key != key:
                raise ValueError(
                    f"vertical view {relation_spec.relation!r} key differs from its "
                    "declared primary key"
                )
            if tuple(column.attribute for column in anchor.columns) != key or (
                anchor.primary_key != key
            ):
                raise ValueError(
                    f"vertical family {vertical.family!r} anchor must be PK-only"
                )
            if tuple(column.attribute for column in seal.columns) != key or (
                seal.primary_key != key
            ):
                raise ValueError(
                    f"vertical family {vertical.family!r} seal must be PK-only"
                )
            value_attributes = tuple(
                (
                    member.projection_attribute or member.value_attribute
                    if attribute == member.value_attribute
                    else attribute
                )
                for member, member_relation in members
                if member.project
                for attribute in (
                    column.attribute
                    for column in member_relation.columns  # type: ignore[union-attr]
                )
                if attribute not in member.member_attributes
            )
            if len(value_attributes) != len(set(value_attributes)):
                raise ValueError(
                    f"vertical family {vertical.family!r} repeats a value attribute"
                )
            physical_projection = tuple(
                column.attribute for column in relation_spec.columns
            )
            available_projection = (*key, *value_attributes)
            if vertical.projection_attributes:
                if (
                    physical_projection != vertical.projection_attributes
                    or len(vertical.projection_attributes)
                    != len(set(vertical.projection_attributes))
                    or not set(key) <= set(vertical.projection_attributes)
                    or not set(vertical.projection_attributes)
                    <= set(available_projection)
                ):
                    raise ValueError(
                        f"vertical view {relation_spec.relation!r} explicit "
                        "projection is not an exact keyed subset of its members"
                    )
            elif len(physical_projection) != len(available_projection) or set(
                physical_projection
            ) != set(available_projection):
                raise ValueError(
                    f"vertical view {relation_spec.relation!r} projection is not "
                    "exactly the key and every projected non-join attribute"
                )
            for member, member_relation in members:
                assert member_relation is not None
                if member_relation.primary_key != member.key_attributes or tuple(
                    column.attribute for column in member_relation.columns
                ) != (*member.key_attributes, member.value_attribute):
                    raise ValueError(
                        f"vertical family {vertical.family!r} member "
                        f"{member.relation!r} must be PK plus one value"
                    )

            def fk_shapes(
                source: PhysicalRelationSpec,
            ) -> set[tuple[tuple[str, ...], str, tuple[str, ...]]]:
                return {
                    (
                        foreign_key.attributes,
                        foreign_key.referenced_relation,
                        foreign_key.referenced_attributes,
                    )
                    for foreign_key in source.foreign_keys
                }

            available_sources = {vertical.anchor_relation, vertical.seal_relation}
            participation_by_source: dict[
                str, set[tuple[tuple[str, ...], str, tuple[str, ...]]]
            ] = {}
            for member, member_relation in members:
                assert member_relation is not None
                if (
                    member.source_relation not in available_sources
                    or len(member.source_attributes) != len(member.member_attributes)
                    or frozenset(member.member_attributes)
                    not in {
                        frozenset(member_relation.primary_key),
                        *(frozenset(item) for item in member_relation.unique_keys),
                        *(
                            frozenset(item)
                            for item in member_relation.runtime_unique_keys
                        ),
                    }
                ):
                    raise ValueError(
                        f"vertical family {vertical.family!r} member "
                        f"{member.relation!r} has an invalid ordered join"
                    )
                source = physical.relation(member.source_relation)
                assert source is not None
                participation = (
                    member.source_attributes,
                    member.relation,
                    member.member_attributes,
                )
                if member.required:
                    if participation not in fk_shapes(source):
                        raise ValueError(
                            f"vertical family {vertical.family!r} member "
                            f"{member.relation!r} lacks its participation FK"
                        )
                    participation_by_source.setdefault(
                        member.source_relation, set()
                    ).add(participation)
                else:
                    optional_reference = (
                        member.member_attributes,
                        member.source_relation,
                        member.source_attributes,
                    )
                    if optional_reference not in fk_shapes(member_relation):
                        raise ValueError(
                            f"vertical family {vertical.family!r} optional member "
                            f"{member.relation!r} must reference its join source"
                        )
                available_sources.add(member.relation)
            optional_members = tuple(
                member for member, _relation in members if not member.required
            )
            presence = vertical.optional_presence
            if optional_members and presence is None:
                raise ValueError(
                    f"vertical family {vertical.family!r} optional members require "
                    "one closed presence rule"
                )
            if not optional_members and presence is not None:
                raise ValueError(
                    f"vertical family {vertical.family!r} presence rule requires "
                    "an optional member"
                )
            if presence is not None:
                discriminator = next(
                    (
                        member
                        for member, _relation in members
                        if member.relation == presence.discriminator_relation
                    ),
                    None,
                )
                if {member.relation for member in optional_members} != {
                    presence.member_relation
                }:
                    raise ValueError(
                        f"vertical family {vertical.family!r} presence rule must "
                        "name its sole optional member"
                    )
                if (
                    discriminator is None
                    or not discriminator.required
                    or not discriminator.project
                    or presence.discriminator_attribute
                    not in {
                        discriminator.value_attribute,
                        discriminator.projection_attribute
                        or discriminator.value_attribute,
                    }
                ):
                    raise ValueError(
                        f"vertical family {vertical.family!r} presence discriminator "
                        "must be a mandatory projected fact"
                    )
                if (
                    not presence.absent_values
                    or len(presence.absent_values) != len(set(presence.absent_values))
                    or presence.present_value in set(presence.absent_values)
                ):
                    raise ValueError(
                        f"vertical family {vertical.family!r} presence values must "
                        "be nonempty, disjoint, and unique"
                    )
            expected_seal_fks = {
                (key, vertical.anchor_relation, key),
                *participation_by_source.get(vertical.seal_relation, set()),
            }
            if vertical.family == "publication_commit_vertical":
                expected_seal_fks.add(
                    (
                        key,
                        "publication_finalization_checkpoint_seal",
                        key,
                    )
                )
            if fk_shapes(seal) != expected_seal_fks:
                raise ValueError(
                    f"vertical family {vertical.family!r} seal must "
                    "reference exactly its anchor and direct members"
                )
            if fk_shapes(relation_spec) != {(key, vertical.seal_relation, key)}:
                raise ValueError(
                    f"vertical view {relation_spec.relation!r} must expose only "
                    "sealed keys"
                )

        if relation_spec.generation_baseline_view is not None:
            baseline_spec = relation_spec.generation_baseline_view
            base = physical.relation(baseline_spec.base_relation)
            mapping = physical.relation(baseline_spec.mapping_relation)
            if any(
                value is None or value.status != "implemented" or value.kind != "table"
                for value in (base, mapping)
            ):
                raise ValueError(
                    f"generation baseline view {relation_spec.relation!r} must "
                    "reference implemented base tables"
                )
            assert base is not None
            assert mapping is not None
            if tuple(column.attribute for column in relation_spec.columns) != (
                baseline_spec.owner_attribute,
                baseline_spec.revision_attribute,
                baseline_spec.generation_attribute,
            ):
                raise ValueError(
                    f"generation baseline view {relation_spec.relation!r} has the "
                    "wrong ordered projection"
                )
            if tuple(column.attribute for column in base.columns) != (
                baseline_spec.owner_attribute,
                baseline_spec.revision_attribute,
            ) or tuple(column.attribute for column in mapping.columns) != (
                baseline_spec.mapping_revision_attribute,
                baseline_spec.mapping_generation_attribute,
            ):
                raise ValueError(
                    f"generation baseline view {relation_spec.relation!r} source "
                    "shape drifted"
                )

        if relation_spec.generation_head_view is not None:
            head_spec = relation_spec.generation_head_view
            revision = physical.relation(head_spec.revision_relation)
            timestamp = physical.relation(head_spec.time_relation)
            mapping = physical.relation(head_spec.mapping_relation)
            if any(
                value is None or value.status != "implemented" or value.kind != "table"
                for value in (revision, timestamp, mapping)
            ):
                raise ValueError(
                    f"generation head view {relation_spec.relation!r} must reference "
                    "implemented base tables"
                )
            assert revision is not None
            assert timestamp is not None
            assert mapping is not None
            if tuple(column.attribute for column in relation_spec.columns) != (
                head_spec.channel_attribute,
                head_spec.revision_attribute,
                head_spec.generation_attribute,
                head_spec.time_attribute,
            ):
                raise ValueError(
                    f"generation head view {relation_spec.relation!r} has the wrong "
                    "ordered projection"
                )
            if (
                tuple(column.attribute for column in revision.columns)
                != (
                    head_spec.channel_attribute,
                    head_spec.revision_attribute,
                )
                or tuple(column.attribute for column in timestamp.columns)
                != (
                    head_spec.channel_attribute,
                    head_spec.time_attribute,
                )
                or tuple(column.attribute for column in mapping.columns)
                != (
                    head_spec.revision_attribute,
                    head_spec.generation_attribute,
                )
            ):
                raise ValueError(
                    f"generation head view {relation_spec.relation!r} source shape "
                    "drifted"
                )

        if relation_spec.derived_view is not None:
            derived = relation_spec.derived_view
            sources = tuple(
                physical.relation(name) for name in derived.source_relations
            )
            if any(
                source is None or source.status != "implemented" for source in sources
            ):
                raise ValueError(
                    f"derived view {relation_spec.relation!r} must reference only "
                    "implemented physical relations"
                )
            if relation_spec.relation in derived.source_relations:
                raise ValueError(
                    f"derived view {relation_spec.relation!r} cannot reference itself"
                )
            if derived.pattern == "analysis_ancestry_endpoint":
                if derived.source_relations != ("analysis_state_ancestry",):
                    raise ValueError(
                        "analysis ancestry endpoint must derive only from ancestry"
                    )
                (ancestry,) = sources
                assert ancestry is not None
                if ancestry.kind != "table" or tuple(
                    column.attribute for column in ancestry.columns
                ) != (
                    "analysis_id",
                    "ancestor_depth",
                    "ancestor_analysis_id",
                ):
                    raise ValueError("analysis ancestry endpoint source shape drifted")
                if tuple(column.attribute for column in relation_spec.columns) != (
                    "analysis_id",
                    "anchor_analysis_id",
                    "overlay_depth",
                ):
                    raise ValueError(
                        "analysis ancestry endpoint projection shape drifted"
                    )
            elif derived.pattern == "analysis_gid_winner_keyset":
                expected_sources = (
                    "analysis_gid_winner_selection",
                    "analysis_impacted_gid",
                    "analysis_run_build_id",
                    "source_build_gallery",
                    "gallery_observation_metadata",
                )
                if derived.source_relations != expected_sources:
                    raise ValueError(
                        "analysis GID winner keyset source authority drifted"
                    )
                source_by_name = {
                    source.relation: source for source in sources if source is not None
                }
                expected_shapes = {
                    "analysis_gid_winner_selection": (
                        "analysis_id",
                        "winner_gallery_id",
                    ),
                    "analysis_impacted_gid": ("analysis_id", "gid"),
                    "analysis_run_build_id": ("analysis_id", "build_id"),
                    "source_build_gallery": (
                        "build_id",
                        "gallery_id",
                        "observation_id",
                    ),
                    "gallery_observation_metadata": (
                        "gallery_id",
                        "observation_id",
                        "gid",
                        "upload_time",
                        "download_time",
                        "modified_time",
                    ),
                }
                for source_name, expected_shape in expected_shapes.items():
                    source = source_by_name[source_name]
                    if (
                        tuple(column.attribute for column in source.columns)
                        != expected_shape
                    ):
                        raise ValueError(
                            "analysis GID winner keyset source shape drifted: "
                            f"{source_name}"
                        )
                if tuple(column.attribute for column in relation_spec.columns) != (
                    "analysis_id",
                    "gid",
                    "winner_gallery_id",
                ):
                    raise ValueError(
                        "analysis GID winner keyset projection shape drifted"
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
            *relation.referential_unique_keys,
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
    if len(value.encode("ascii")) > 63:
        raise ValueError(f"{context} must fit the portable 63-byte identifier domain")


def _portable_identifier(value: str) -> str:
    """Match the provider/generator codec for derived SQL identifiers."""

    encoded = value.encode("ascii")
    if len(encoded) <= 63:
        return value
    return f"{value[:50]}_{hashlib.sha256(encoded).hexdigest()[:12]}"


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


def _normalize_view_definition(
    value: str,
    *,
    table_names: Iterable[str] = (),
    collapse_inner_join_tree: bool = False,
) -> str:
    """Normalize backend quoting/formatting while preserving the SQL join graph."""

    normalized = value.strip().rstrip(";").replace("`", "").replace('"', "")
    match = re.search(r"\bAS\s+(SELECT\b.*)$", normalized, re.IGNORECASE | re.DOTALL)
    if match is not None:
        normalized = match.group(1)
    physical_table_names = tuple(
        sorted(set(table_names), key=lambda table_name: (-len(table_name), table_name))
    )
    for table_name in physical_table_names:
        normalized = re.sub(
            rf"\b[a-zA-Z0-9_$]+\.{re.escape(table_name)}\b",
            table_name,
            normalized,
            flags=re.IGNORECASE,
        )
    normalized = re.sub(r"\s+", " ", normalized).strip().casefold()
    if collapse_inner_join_tree:
        # MariaDB drops AS from table aliases, wraps each ON predicate, and
        # exposes a flat INNER JOIN chain as a parenthesized left-deep tree.
        # It also spells NOT EXISTS as !EXISTS and appends a redundant LIMIT 1
        # inside EXISTS subqueries. Normalize only those engine rewrites: keep
        # function, predicate, outer-join-association, and subquery parentheses
        # exact so a semantic SQL drift cannot compare equal.
        if physical_table_names:
            table_pattern = "|".join(
                re.escape(table_name.casefold()) for table_name in physical_table_names
            )
            normalized = re.sub(
                rf"\b({table_pattern})\s+as\s+([a-z_][a-z0-9_$]*)\b",
                r"\1 \2",
                normalized,
            )
        normalized = _normalize_mariadb_exists(normalized)
        normalized = _strip_mariadb_join_parentheses(
            normalized,
            physical_table_names,
        )
        normalized = _strip_mariadb_on_parentheses(normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\s*=\s*", "=", normalized)
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    return normalized


def _normalize_mariadb_exists(value: str) -> str:
    """Normalize MariaDB's semantics-preserving EXISTS presentation."""

    normalized = re.sub(r"!\s*exists\b", "not exists", value)
    while True:
        changed = False
        matches = tuple(re.finditer(r"\bexists\s*\(", normalized))
        for match in reversed(matches):
            opening = normalized.find("(", match.start(), match.end())
            closing = _matching_parenthesis(normalized, opening)
            if closing is None:
                continue
            content = normalized[opening + 1 : closing]
            without_limit = re.sub(r"\s+limit\s+1\s*$", "", content)
            if without_limit == content:
                continue
            normalized = (
                normalized[: opening + 1] + without_limit + normalized[closing:]
            )
            changed = True
        if not changed:
            break
    return re.sub(r"\bexists\s+\(", "exists(", normalized)


def _strip_mariadb_join_parentheses(
    value: str,
    table_names: Iterable[str],
) -> str:
    """Remove only MariaDB's redundant wrappers around generated join trees."""

    names = tuple(sorted(set(table_names), key=lambda name: (-len(name), name)))
    if not names:
        return value
    table_pattern = re.compile(
        r"^(?:" + "|".join(re.escape(name) for name in names) + r")\b"
    )
    normalized = value
    while True:
        changed = False
        for opening in reversed(_unquoted_open_parentheses(normalized)):
            closing = _matching_parenthesis(normalized, opening)
            if closing is None:
                continue
            content = normalized[opening + 1 : closing].strip()
            if table_pattern.match(content) is None:
                continue
            top_level = _top_level_parenthesis_content(content)
            joins = tuple(
                re.finditer(
                    r"\b(?:(left|right|full|cross|inner)\s+)?join\b",
                    top_level,
                )
            )
            if not joins:
                continue
            has_non_inner_join = any(
                match.group(1) not in {None, "inner", "cross"} for match in joins
            )
            if has_non_inner_join and not _is_entire_from_wrapper(
                normalized,
                opening,
                closing,
            ):
                continue
            normalized = normalized[:opening] + content + normalized[closing + 1 :]
            changed = True
            break
        if not changed:
            return normalized


def _unquoted_open_parentheses(value: str) -> tuple[int, ...]:
    openings: list[int] = []
    quote: str | None = None
    for position, character in enumerate(value):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {'"', "'", "`"}:
            quote = character
        elif character == "(":
            openings.append(position)
    return tuple(openings)


def _top_level_parenthesis_content(value: str) -> str:
    """Blank nested expressions while retaining the outer token sequence."""

    result: list[str] = []
    depth = 0
    quote: str | None = None
    for character in value:
        if quote is not None:
            if character == quote:
                quote = None
            result.append(character if depth == 0 else " ")
            continue
        if character in {'"', "'", "`"}:
            quote = character
            result.append(character if depth == 0 else " ")
        elif character == "(":
            depth += 1
            result.append(" ")
        elif character == ")":
            depth -= 1
            result.append(" ")
        else:
            result.append(character if depth == 0 else " ")
    return "".join(result)


def _is_entire_from_wrapper(value: str, opening: int, closing: int) -> bool:
    prefix = value[:opening].rstrip()
    suffix = value[closing + 1 :].lstrip()
    if re.search(r"\bfrom$", prefix) is None:
        return False
    return (
        not suffix
        or re.match(
            r"(?:where|group\s+by|having|order\s+by|union|limit)\b|\)",
            suffix,
        )
        is not None
    )


def _strip_mariadb_on_parentheses(value: str) -> str:
    """Remove a redundant pair only when it encloses the complete ON clause."""

    normalized = value
    search_from = 0
    while (match := re.search(r"\bon\s*\(", normalized[search_from:])) is not None:
        match_start = search_from + match.start()
        match_end = search_from + match.end()
        opening = normalized.find("(", match_start, match_end)
        closing = _matching_parenthesis(normalized, opening)
        if closing is None:
            break
        suffix = normalized[closing + 1 :].lstrip()
        if (
            suffix
            and re.match(
                r"(?:(?:inner|left|right|full|cross)\s+)?join\b|"
                r"(?:where|group\s+by|having|order\s+by|union|limit)\b|\)",
                suffix,
            )
            is None
        ):
            search_from = opening + 1
            continue
        content = normalized[opening + 1 : closing].strip()
        normalized = normalized[:opening] + " " + content + normalized[closing + 1 :]
        search_from = opening + len(content)
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


def _required_table(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    result = value.get(key)
    if not isinstance(result, dict):
        raise ValueError(f"{key} must be a table")
    return result


def _required_string_tuple(value: Mapping[str, Any], key: str) -> tuple[str, ...]:
    return _string_sequence(value.get(key), key)


def _parse_vertical_view_member(
    value: Mapping[str, Any],
) -> VerticalViewMemberSpec:
    join = value.get("join")
    if not isinstance(join, dict):
        raise ValueError("vertical view member.join must be a table")
    project = value.get("project")
    if not isinstance(project, bool):
        raise ValueError("vertical view member.project must be a boolean")
    required = value.get("required", True)
    if not isinstance(required, bool):
        raise ValueError("vertical view member.required must be a boolean")
    value_attribute = _required_string(value, "value_attribute")
    return VerticalViewMemberSpec(
        relation=_required_string(value, "relation"),
        key_attributes=_required_string_tuple(value, "key_attributes"),
        value_attribute=value_attribute,
        projection_attribute=(
            _required_string(value, "projection_attribute")
            if "projection_attribute" in value
            else value_attribute
        ),
        source_relation=_required_string(join, "source_relation"),
        source_attributes=_required_string_tuple(join, "source_attributes"),
        member_attributes=_required_string_tuple(join, "member_attributes"),
        project=project,
        required=required,
    )


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
