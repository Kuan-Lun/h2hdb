#!/usr/bin/env python3
"""Generate the wheel-resident vNext schema-provider artifact.

The verification TOML files are development inputs.  They are deliberately not
installed in the wheel, so this script closes the data and operational physical
contracts into one deterministic Python module under ``src/h2hdb``.  The
runtime provider imports only that generated module.

This generator is fail-closed.  It preserves unresolved natural-language
obligations as provenance, but it does not manufacture executable obligation
IDs or bootstrap rows when the formal contracts do not declare them.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import runpy
import sys
import tomllib
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, cast

ROOT = Path(__file__).resolve().parents[1]
GENERATED = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
GENERATED_RESOURCE = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.bin"
CODEC_SOURCE = ROOT / "src" / "h2hdb" / "_schema_artifact_codec.py"
CATALOG_LOGICAL = ROOT / "verification" / "schema" / "catalog.toml"
DATA_PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
OPERATIONAL_LOGICAL = ROOT / "verification" / "schema" / "operational.toml"
OPERATIONAL_PHYSICAL = ROOT / "verification" / "schema" / "operational_physical.toml"
SOURCE_PATHS = (
    CATALOG_LOGICAL,
    DATA_PHYSICAL,
    OPERATIONAL_LOGICAL,
    OPERATIONAL_PHYSICAL,
    CODEC_SOURCE,
    Path(__file__).resolve(),
)


class _DecodeSchemaArtifact(Protocol):
    def __call__(
        self,
        raw: bytes,
        *,
        pickle_protocol: int,
        raw_size: int,
        raw_sha256: str,
    ) -> dict[str, Any]: ...


_CODEC_NAMESPACE = runpy.run_path(str(CODEC_SOURCE))
_encode_schema_artifact = cast(
    Callable[[object], bytes], _CODEC_NAMESPACE["encode_schema_artifact"]
)
_decode_schema_artifact = cast(
    _DecodeSchemaArtifact, _CODEC_NAMESPACE["decode_schema_artifact"]
)
_SchemaArtifactCodecError = cast(
    type[Exception], _CODEC_NAMESPACE["SchemaArtifactCodecError"]
)
_SCHEMA_ARTIFACT_PICKLE_PROTOCOL = cast(
    int, _CODEC_NAMESPACE["SCHEMA_ARTIFACT_PICKLE_PROTOCOL"]
)


def _ddl_identifier(value: str) -> str:
    """Match the physical generators' portable MariaDB 63-byte names."""

    encoded = value.encode("ascii")
    if len(encoded) <= 63:
        return value
    return f"{value[:50]}_{hashlib.sha256(encoded).hexdigest()[:12]}"


EPOCH = 3
SCHEMA_VERSION = 2
CONTROL_RELATION = "schema_epoch_control"
CONTROL_TABLE = "h2hdb_schema_epoch"
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        value = tomllib.load(stream)
    if not isinstance(value, dict):  # pragma: no cover - tomllib contract
        raise TypeError(f"{path} did not contain a TOML document")
    return value


def _required_string(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{context}.{key} must be a non-empty string")
    return result


def _strings(value: object, context: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ValueError(f"{context} must be an array of non-empty strings")
    return tuple(value)


def _tuples(value: object, context: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be an array")
    return tuple(_strings(item, context) for item in value)


def _tables(value: object, context: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{context} must be an array of tables")
    return tuple(value)


def _relations(document: Mapping[str, Any], context: str) -> tuple[dict[str, Any], ...]:
    relations = _tables(document.get("relation"), f"{context}.relation")
    names = [_required_string(item, "name", context) for item in relations]
    if len(names) != len(set(names)):
        raise ValueError(f"{context} contains duplicate relation names")
    return relations


def _relation_map(
    relations: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {str(relation["name"]): relation for relation in relations}


def _normalize_type(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip()).upper()
    return re.sub(
        r"\b(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT)\(\d+\)",
        r"\1",
        normalized,
    )


def _collation(value: object) -> str | None:
    if not isinstance(value, str):
        raise ValueError("physical collation must be a string")
    return None if value.upper() == "NONE" else value


def _backend_column(
    relation: Mapping[str, Any], attribute: str, backend: str
) -> tuple[str, str, bool, str | None]:
    for raw_column in _tables(
        relation.get("column"), f"relation {relation.get('name')!r}.column"
    ):
        if raw_column.get("attribute") != attribute:
            continue
        name = _required_string(raw_column, "name", f"column {attribute!r}")
        raw_backend = raw_column.get(backend)
        if not isinstance(raw_backend, dict):
            raise ValueError(f"column {attribute!r}.{backend} must be a table")
        nullable = raw_backend.get("nullable")
        if not isinstance(nullable, bool):
            raise ValueError(f"column {attribute!r}.{backend}.nullable must be boolean")
        return (
            name,
            _normalize_type(
                _required_string(raw_backend, "type", f"column {attribute!r}.{backend}")
            ),
            nullable,
            _collation(raw_backend.get("collation")),
        )
    raise ValueError(
        f"relation {relation.get('name')!r} has no physical column for {attribute!r}"
    )


def _column_name(relation: Mapping[str, Any], attribute: str) -> str:
    return _backend_column(relation, attribute, "sqlite")[0]


def _logical_keys(relation: Mapping[str, Any]) -> tuple[tuple[str, ...], ...]:
    return _tuples(
        relation.get("declared_keys"),
        f"logical relation {relation.get('name')!r}.declared_keys",
    )


def _physical_sql_keys(relation: Mapping[str, Any]) -> tuple[tuple[str, ...], ...]:
    return (
        _strings(
            relation.get("primary_key"),
            f"physical relation {relation.get('name')!r}.primary_key",
        ),
        *_tuples(
            relation.get("unique_keys", []),
            f"physical relation {relation.get('name')!r}.unique_keys",
        ),
        *_tuples(
            relation.get("referential_unique_keys", []),
            f"physical relation {relation.get('name')!r}.referential_unique_keys",
        ),
    )


def _physical_all_keys(relation: Mapping[str, Any]) -> tuple[tuple[str, ...], ...]:
    return (
        *_physical_sql_keys(relation),
        *_tuples(
            relation.get("runtime_unique_keys", []),
            f"physical relation {relation.get('name')!r}.runtime_unique_keys",
        ),
    )


def _required_indexes(
    relation: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    return _tables(
        relation.get("required_index", []),
        f"physical relation {relation.get('name')!r}.required_index",
    )


def _validate_external_contracts(
    *,
    catalog_logical: Mapping[str, Any],
    operational_logical: Mapping[str, Any],
    data_physical: Mapping[str, Any],
    data_relations: Mapping[str, dict[str, Any]],
    operational_physical: Mapping[str, Any],
) -> None:
    catalog_relations = _relation_map(_relations(catalog_logical, "catalog logical"))
    raw_external = _tables(
        operational_logical.get("external_relation", []),
        "operational logical.external_relation",
    )
    raw_stubs = _tables(
        operational_physical.get("external_stub", []),
        "operational physical.external_stub",
    )
    external = {
        _required_string(value, "name", "operational external relation"): value
        for value in raw_external
    }
    stubs = {
        _required_string(value, "relation", "operational external stub"): value
        for value in raw_stubs
    }
    inline = set(
        _strings(
            operational_physical.get("external_inline_projections", []),
            "operational physical.external_inline_projections",
        )
    )
    if len(external) != len(raw_external) or len(stubs) != len(raw_stubs):
        raise ValueError("operational external declarations contain duplicate names")
    expected_inline = set(external) & set(
        _strings(
            data_physical.get("inline_projections", []),
            "data physical.inline_projections",
        )
    )
    if inline != expected_inline:
        raise ValueError(
            "operational external inline projections drift from data physical"
        )
    if set(stubs) & inline or set(external) != set(stubs) | inline:
        raise ValueError(
            "operational external relations must have exactly one stub or inline "
            "projection classification"
        )

    for name in sorted(external):
        source = external[name]
        logical_target = catalog_relations.get(name)
        physical_target = data_relations.get(name)
        if logical_target is None:
            raise ValueError(
                f"operational external relation {name!r} has no logical data target"
            )
        attributes = _strings(source.get("attributes"), f"external {name}.attributes")
        target_attributes = _strings(
            logical_target.get("attributes"), f"catalog relation {name}.attributes"
        )
        if not set(attributes) <= set(target_attributes):
            raise ValueError(f"external relation {name!r} exposes unknown attributes")
        external_keys = _tuples(
            source.get("declared_keys"), f"external {name}.declared_keys"
        )
        visible_target_keys = tuple(
            key for key in _logical_keys(logical_target) if set(key) <= set(attributes)
        )
        if {frozenset(key) for key in external_keys} != {
            frozenset(key) for key in visible_target_keys
        }:
            raise ValueError(f"external relation {name!r} candidate keys drift")

        if name in inline:
            if physical_target is not None:
                raise ValueError(
                    f"inline external relation {name!r} unexpectedly has a SQL object"
                )
            continue
        if physical_target is None:
            raise ValueError(
                f"operational external relation {name!r} has no physical data target"
            )
        stub = stubs[name]

        target_table = _required_string(
            physical_target, "table", f"physical relation {name!r}"
        )
        if _required_string(stub, "table", f"external stub {name!r}") != target_table:
            raise ValueError(f"external stub {name!r} table drifts from data physical")
        stub_columns = _tables(stub.get("column"), f"external stub {name}.column")
        if {str(value.get("name")) for value in stub_columns} != set(attributes):
            raise ValueError(f"external stub {name!r} columns drift")
        for stub_column in stub_columns:
            attribute = _required_string(
                stub_column, "name", f"external stub {name!r} column"
            )
            for backend, stub_type_key in (
                ("sqlite", "sqlite_type"),
                ("mariadb", "mariadb_type"),
            ):
                actual_name, actual_type, _nullable, _actual_collation = (
                    _backend_column(physical_target, attribute, backend)
                )
                if actual_name != attribute:
                    raise ValueError(
                        f"external stub {name!r}.{attribute} does not map the actual "
                        f"physical column {actual_name!r}"
                    )
                stub_type = _normalize_type(
                    _required_string(
                        stub_column,
                        stub_type_key,
                        f"external stub {name!r}.{attribute}",
                    )
                )
                if stub_type != actual_type:
                    raise ValueError(
                        f"external stub {name!r}.{attribute} {backend} type "
                        f"{stub_type!r} differs from data physical {actual_type!r}"
                    )
        stub_keys = (
            _strings(stub.get("primary_key"), f"stub {name}.primary_key"),
            *_tuples(stub.get("unique_keys", []), f"stub {name}.unique_keys"),
        )
        visible_physical_keys = tuple(
            key
            for key in _physical_all_keys(physical_target)
            if set(key) <= set(attributes)
        )
        if set(stub_keys) != set(visible_physical_keys):
            raise ValueError(f"external stub {name!r} keys drift from data physical")


def _foreign_keys(relation: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return _tables(
        relation.get("foreign_key", []),
        f"physical relation {relation.get('name')!r}.foreign_key",
    )


def _validate_foreign_keys(
    relations: Mapping[str, dict[str, Any]],
) -> tuple[str, ...]:
    blockers: list[str] = []
    for relation_name, relation in relations.items():
        if relation.get("kind", "table") != "table":
            continue
        access_paths = (
            *_physical_sql_keys(relation),
            *(
                _strings(
                    index.get("attributes"),
                    f"relation {relation_name!r} index attributes",
                )
                for index in _required_indexes(relation)
            ),
        )
        for foreign_key in _foreign_keys(relation):
            foreign_key_name = _required_string(
                foreign_key, "name", f"relation {relation_name!r} foreign key"
            )
            attributes = _strings(
                foreign_key.get("attributes"), f"foreign key {foreign_key_name}"
            )
            target_name = _required_string(
                foreign_key,
                "referenced_relation",
                f"foreign key {foreign_key_name}",
            )
            target_attributes = _strings(
                foreign_key.get("referenced_attributes"),
                f"foreign key {foreign_key_name}.referenced_attributes",
            )
            target = relations.get(target_name)
            if target is None or target.get("kind", "table") != "table":
                raise ValueError(
                    f"foreign key {foreign_key_name!r} targets missing/non-table "
                    f"relation {target_name!r}"
                )
            if target_attributes not in _physical_sql_keys(target):
                raise ValueError(
                    f"foreign key {foreign_key_name!r} does not target an ordered "
                    "physical PK/UK"
                )
            if len(attributes) != len(target_attributes):
                raise ValueError(f"foreign key {foreign_key_name!r} arity differs")
            for source_attribute, target_attribute in zip(
                attributes, target_attributes, strict=True
            ):
                for backend in ("sqlite", "mariadb"):
                    _source_name, source_type, _source_nullable, source_collation = (
                        _backend_column(relation, source_attribute, backend)
                    )
                    _target_name, target_type, _target_nullable, target_collation = (
                        _backend_column(target, target_attribute, backend)
                    )
                    if (source_type, source_collation) != (
                        target_type,
                        target_collation,
                    ):
                        raise ValueError(
                            f"foreign key {foreign_key_name!r} {backend} domain "
                            f"drifts at {source_attribute!r}->{target_attribute!r}: "
                            f"{(source_type, source_collation)!r} != "
                            f"{(target_type, target_collation)!r}"
                        )
            if not any(path[: len(attributes)] == attributes for path in access_paths):
                blockers.append(
                    "physical foreign key lacks an explicit child-side left-prefix "
                    f"access path: {relation_name}.{foreign_key_name}{attributes!r}"
                )
    return tuple(blockers)


def _topological_order(
    relations: Mapping[str, dict[str, Any]], preferred_order: Sequence[str]
) -> tuple[str, ...]:
    if set(preferred_order) != set(relations):
        raise ValueError("combined preferred order does not cover the relation set")
    rank = {name: position for position, name in enumerate(preferred_order)}
    dependencies: dict[str, set[str]] = {name: set() for name in relations}
    reverse: dict[str, set[str]] = defaultdict(set)
    for name, relation in relations.items():
        for foreign_key in _foreign_keys(relation):
            target = _required_string(
                foreign_key,
                "referenced_relation",
                f"relation {name!r} foreign key",
            )
            dependencies[name].add(target)
            reverse[target].add(name)
        raw_view = relation.get("view")
        if isinstance(raw_view, dict):
            for target in _view_dependencies(relation):
                dependencies[name].add(target)
                reverse[target].add(name)
    ready = sorted(
        (name for name, values in dependencies.items() if not values),
        key=rank.__getitem__,
    )
    result: list[str] = []
    while ready:
        name = ready.pop(0)
        result.append(name)
        for dependent in sorted(reverse[name], key=rank.__getitem__):
            dependencies[dependent].discard(name)
            if not dependencies[dependent] and dependent not in result + ready:
                ready.append(dependent)
        ready.sort(key=rank.__getitem__)
    if len(result) != len(relations):
        unresolved = {
            name: sorted(values) for name, values in dependencies.items() if values
        }
        raise ValueError(f"combined physical dependency cycle: {unresolved!r}")
    return tuple(result)


def _quote(identifier: str, backend: str) -> str:
    if backend == "sqlite":
        return '"' + identifier.replace('"', '""') + '"'
    return "`" + identifier.replace("`", "``") + "`"


def _view_dependencies(relation: Mapping[str, Any]) -> tuple[str, ...]:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        return ()
    pattern = _required_string(raw_view, "pattern", "view")
    if pattern == "nearest_ancestor_overlay":
        return tuple(
            _required_string(raw_view, field, "view")
            for field in (
                "ancestry_relation",
                "shadow_relation",
                "tombstone_relation",
            )
        )
    if pattern == "sealed_vertical_family":
        return (
            _required_string(raw_view, "anchor_relation", "view"),
            _required_string(raw_view, "seal_relation", "view"),
            *(
                _required_string(member, "relation", "vertical view member")
                for member in _tables(raw_view.get("members"), "vertical view members")
            ),
        )
    if pattern == "revision_generation_baseline":
        return (
            _required_string(raw_view, "base_relation", "view"),
            _required_string(raw_view, "mapping_relation", "view"),
        )
    if pattern == "revision_generation_head":
        return (
            _required_string(raw_view, "revision_relation", "view"),
            _required_string(raw_view, "time_relation", "view"),
            _required_string(raw_view, "mapping_relation", "view"),
        )
    if pattern in {
        "analysis_ancestry_endpoint",
        "analysis_gid_winner_keyset",
        "artifact_delta_old",
        "artifact_delta_new",
        "build_manifest_projection",
        "analysis_impacted_gid_projection",
        "analysis_impacted_gid_provenance_projection",
        "batch_receipt_derived",
        "catalog_publication_occurrence_identity",
        "catalog_publication_projection",
        "catalog_publication_title_projection",
        "gallery_observation_metadata_projection",
        "publication_selection_occurrence_identity",
        "publication_selection_projection",
        "publication_commit_activation",
        "publication_commit_baseline",
        "publication_commit_generation",
        "publication_commit_head",
        "publication_commit_head_projection",
        "publication_commit_published_descriptor",
        "publication_candidate_projection",
        "publication_receipt",
        "lifecycle_projection",
    }:
        return _strings(raw_view.get("source_relations"), "view source_relations")
    raise ValueError(f"unsupported physical view pattern {pattern!r}")


def _render_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    pattern = _required_string(raw_view, "pattern", "view")
    if pattern == "nearest_ancestor_overlay":
        return _render_overlay_view(relation, relations, backend)
    if pattern == "sealed_vertical_family":
        return _render_sealed_vertical_view(relation, relations, backend)
    if pattern == "revision_generation_baseline":
        return _render_revision_generation_baseline_view(relation, relations, backend)
    if pattern == "revision_generation_head":
        return _render_revision_generation_head_view(relation, relations, backend)
    if pattern == "analysis_ancestry_endpoint":
        return _render_analysis_ancestry_endpoint_view(relation, relations, backend)
    if pattern == "analysis_gid_winner_keyset":
        return _render_analysis_gid_winner_keyset_view(relation, relations, backend)
    if pattern == "artifact_delta_old":
        return _render_artifact_delta_old_view(relation, relations, backend)
    if pattern == "artifact_delta_new":
        return _render_artifact_delta_new_view(relation, relations, backend)
    if pattern == "build_manifest_projection":
        return _render_build_manifest_projection_view(relation, relations, backend)
    if pattern in {
        "publication_selection_occurrence_identity",
        "catalog_publication_occurrence_identity",
    }:
        return _render_occurrence_identity_view(relation, relations, backend)
    if pattern == "publication_selection_projection":
        return _render_publication_selection_view(relation, relations, backend)
    if pattern == "catalog_publication_projection":
        return _render_catalog_publication_view(relation, relations, backend)
    if pattern == "catalog_publication_title_projection":
        return _render_catalog_publication_title_view(relation, relations, backend)
    if pattern == "analysis_impacted_gid_provenance_projection":
        return _render_analysis_impacted_gid_provenance_view(
            relation, relations, backend
        )
    if pattern == "analysis_impacted_gid_projection":
        return _render_analysis_impacted_gid_view(relation, relations, backend)
    if pattern == "gallery_observation_metadata_projection":
        return _render_gallery_observation_metadata_view(relation, relations, backend)
    if pattern == "batch_receipt_derived":
        return _render_batch_receipt_view(relation, relations, backend)
    if pattern == "publication_candidate_projection":
        return _render_publication_candidate_projection_view(
            relation, relations, backend
        )
    if pattern == "publication_commit_baseline":
        return _render_publication_commit_baseline_view(relation, relations, backend)
    if pattern == "publication_commit_published_descriptor":
        return _render_publication_commit_published_descriptor_view(
            relation, relations, backend
        )
    if pattern == "publication_commit_generation":
        return _render_publication_commit_generation_view(relation, relations, backend)
    if pattern == "publication_commit_head":
        return _render_publication_commit_head_view(relation, relations, backend)
    if pattern == "publication_commit_head_projection":
        return _render_publication_commit_head_projection_view(
            relation, relations, backend
        )
    if pattern == "publication_receipt":
        return _render_publication_receipt_view(relation, relations, backend)
    if pattern == "publication_commit_activation":
        return _render_publication_commit_activation_view(relation, relations, backend)
    if pattern == "lifecycle_projection":
        return _render_lifecycle_projection_view(relation, relations, backend)
    raise ValueError(f"unsupported physical view pattern {pattern!r}")


def _render_overlay_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    ancestry = relations[_required_string(raw_view, "ancestry_relation", "view")]
    shadow = relations[_required_string(raw_view, "shadow_relation", "view")]
    tombstone = relations[_required_string(raw_view, "tombstone_relation", "view")]
    primary_key = _strings(
        relation.get("primary_key"), f"view {relation.get('name')!r}.primary_key"
    )
    key_attributes = tuple(
        attribute for attribute in primary_key if attribute != "analysis_id"
    )
    shadow_columns = _tables(shadow.get("column"), "shadow columns")
    value_attributes = tuple(
        str(column["attribute"])
        for column in shadow_columns
        if column["attribute"] not in primary_key
    )

    def q(value: str) -> str:
        return _quote(value, backend)

    ancestry_table = q(str(ancestry["table"]))
    shadow_table = q(str(shadow["table"]))
    tombstone_table = q(str(tombstone["table"]))
    ancestry_analysis = q(_column_name(ancestry, "analysis_id"))
    ancestry_ancestor = q(_column_name(ancestry, "ancestor_analysis_id"))
    ancestry_depth = q(_column_name(ancestry, "ancestor_depth"))
    shadow_analysis = q(_column_name(shadow, "analysis_id"))
    tombstone_analysis = q(_column_name(tombstone, "analysis_id"))

    expressions = {
        "analysis_id": f"path.{ancestry_analysis}",
        **{
            attribute: f"shadow.{q(_column_name(shadow, attribute))}"
            for attribute in (*key_attributes, *value_attributes)
        },
    }
    raw_columns = _tables(relation.get("column"), "view columns")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in raw_columns
    )
    same_key = "\n      AND ".join(
        f"same_tomb.{q(_column_name(tombstone, attribute))} "
        f"= shadow.{q(_column_name(shadow, attribute))}"
        for attribute in key_attributes
    )
    nearer_shadow = "\n          AND ".join(
        f"near_shadow.{q(_column_name(shadow, attribute))} "
        f"= shadow.{q(_column_name(shadow, attribute))}"
        for attribute in key_attributes
    )
    nearer_tombstone = "\n          AND ".join(
        f"near_tomb.{q(_column_name(tombstone, attribute))} "
        f"= shadow.{q(_column_name(shadow, attribute))}"
        for attribute in key_attributes
    )
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    table = q(str(relation["table"]))
    column_list = ", ".join(q(str(column["name"])) for column in raw_columns)
    return f"""{prefix} {table} ({column_list}) AS
SELECT
  {projection}
FROM {ancestry_table} AS path
JOIN {shadow_table} AS shadow
  ON shadow.{shadow_analysis} = path.{ancestry_ancestor}
WHERE NOT EXISTS (
  SELECT 1
  FROM {tombstone_table} AS same_tomb
  WHERE same_tomb.{tombstone_analysis} = path.{ancestry_ancestor}
    AND {same_key}
)
AND NOT EXISTS (
  SELECT 1
  FROM {ancestry_table} AS nearer
  WHERE nearer.{ancestry_analysis} = path.{ancestry_analysis}
    AND nearer.{ancestry_depth} < path.{ancestry_depth}
    AND (
      EXISTS (
        SELECT 1
        FROM {shadow_table} AS near_shadow
        WHERE near_shadow.{shadow_analysis} = nearer.{ancestry_ancestor}
          AND {nearer_shadow}
      )
      OR EXISTS (
        SELECT 1
        FROM {tombstone_table} AS near_tomb
        WHERE near_tomb.{tombstone_analysis} = nearer.{ancestry_ancestor}
          AND {nearer_tombstone}
      )
    )
)"""


def _render_sealed_vertical_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    anchor = relations[_required_string(raw_view, "anchor_relation", "view")]
    seal = relations[_required_string(raw_view, "seal_relation", "view")]
    key_attributes = _strings(raw_view.get("key_attributes"), "vertical view key")
    members = tuple(
        (
            member,
            relations[_required_string(member, "relation", "vertical view member")],
        )
        for member in _tables(raw_view.get("members"), "vertical view members")
    )

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        attribute: f"sealed.{q(_column_name(seal, attribute))}"
        for attribute in key_attributes
    }
    for position, (member, member_relation) in enumerate(members, start=1):
        if member.get("project") is not True:
            continue
        join = member.get("join")
        if not isinstance(join, dict):
            raise ValueError("vertical view member.join must be a table")
        projected_join_attributes = set(
            _strings(join.get("member_attributes"), "vertical join member attributes")
        )
        for column in _tables(member_relation.get("column"), "member columns"):
            attribute = str(column["attribute"])
            if attribute in projected_join_attributes:
                continue
            projection_attribute = str(
                member.get("projection_attribute", attribute)
                if attribute == member.get("value_attribute")
                else attribute
            )
            expressions[projection_attribute] = (
                f"member_{position}.{q(_column_name(member_relation, attribute))}"
            )
    raw_columns = _tables(relation.get("column"), "view columns")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in raw_columns
    )

    anchor_predicate = "\n AND ".join(
        f"anchor.{q(_column_name(anchor, attribute))} "
        f"= sealed.{q(_column_name(seal, attribute))}"
        for attribute in key_attributes
    )
    joins = [f"JOIN {q(str(anchor['table']))} AS anchor\n ON " + anchor_predicate]
    alias_by_relation = {
        _required_string(raw_view, "seal_relation", "view"): "sealed",
        _required_string(raw_view, "anchor_relation", "view"): "anchor",
    }
    for position, (member, member_relation) in enumerate(members, start=1):
        join = member.get("join")
        if not isinstance(join, dict):
            raise ValueError("vertical view member.join must be a table")
        source_name = _required_string(join, "source_relation", "vertical join")
        source = relations[source_name]
        source_attributes = _strings(
            join.get("source_attributes"), "vertical join source attributes"
        )
        join_member_attributes = _strings(
            join.get("member_attributes"), "vertical join member attributes"
        )
        if len(source_attributes) != len(join_member_attributes):
            raise ValueError("vertical view join arity differs")
        alias = f"member_{position}"
        source_alias = alias_by_relation[source_name]
        predicate = "\n AND ".join(
            f"{alias}.{q(_column_name(member_relation, member_attribute))} "
            f"= {source_alias}.{q(_column_name(source, source_attribute))}"
            for source_attribute, member_attribute in zip(
                source_attributes, join_member_attributes, strict=True
            )
        )
        join_kind = "JOIN" if member.get("required", True) is True else "LEFT JOIN"
        joins.append(
            f"{join_kind} {q(str(member_relation['table']))} AS {alias}\n ON "
            + predicate
        )
        alias_by_relation[_required_string(member, "relation", "vertical member")] = (
            alias
        )
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    table = q(str(relation["table"]))
    column_list = ", ".join(q(str(column["name"])) for column in raw_columns)
    where = ""
    optional_presence = raw_view.get("optional_presence")
    if isinstance(optional_presence, dict):
        optional_relation_name = _required_string(
            optional_presence, "member_relation", "vertical optional presence"
        )
        discriminator_relation_name = _required_string(
            optional_presence,
            "discriminator_relation",
            "vertical optional presence",
        )
        optional_alias = alias_by_relation[optional_relation_name]
        discriminator_alias = alias_by_relation[discriminator_relation_name]
        optional_member = next(
            member
            for member, _member_relation in members
            if _required_string(member, "relation", "vertical member")
            == optional_relation_name
        )
        optional_relation = relations[optional_relation_name]
        optional_attribute = _required_string(
            optional_member, "value_attribute", "vertical optional member"
        )
        discriminator_relation = relations[discriminator_relation_name]
        discriminator_attribute = _required_string(
            optional_presence,
            "discriminator_attribute",
            "vertical optional presence",
        )
        present_value = _required_string(
            optional_presence, "present_value", "vertical optional presence"
        )
        absent_values = _strings(
            optional_presence.get("absent_values"),
            "vertical optional absent values",
        )

        def literal(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        discriminator_column = (
            f"{discriminator_alias}."
            f"{q(_column_name(discriminator_relation, discriminator_attribute))}"
        )
        optional_column = (
            f"{optional_alias}.{q(_column_name(optional_relation, optional_attribute))}"
        )
        where = (
            "\nWHERE "
            f"{discriminator_column} = {literal(present_value)} "
            f"AND {optional_column} IS NOT NULL\n"
            f"   OR {discriminator_column} IN "
            f"({', '.join(literal(value) for value in absent_values)}) "
            f"AND {optional_column} IS NULL"
        )
    return f"""{prefix} {table} ({column_list}) AS
SELECT
  {projection}
FROM {q(str(seal["table"]))} AS sealed
{chr(10).join(joins)}{where}"""


def _render_lifecycle_projection_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Join an immutable descriptor to mutable state and one terminal fact."""

    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    source_names = _strings(raw_view.get("source_relations"), "view source_relations")
    if len(source_names) != 3:
        raise ValueError("lifecycle_projection requires descriptor, state, terminal")
    descriptor, state, terminal = (relations[name] for name in source_names)
    relation_name = _required_string(relation, "name", "view relation")
    present_state = {
        "analysis_run": "COMPLETE",
        "source_build": "SEALED",
    }.get(relation_name)
    if present_state is None:
        raise ValueError(f"unsupported lifecycle projection relation {relation_name!r}")
    absent_states = {
        "analysis_run": ("OPEN", "ABANDONED"),
        "source_build": ("OPEN", "ABANDONED"),
    }[relation_name]

    def q(value: str) -> str:
        return _quote(value, backend)

    key_attributes = _strings(relation.get("primary_key"), "lifecycle view key")
    descriptor_attributes = {
        str(column["attribute"])
        for column in _tables(descriptor.get("column"), "descriptor columns")
    }
    state_attributes = {
        str(column["attribute"])
        for column in _tables(state.get("column"), "state columns")
    }
    terminal_attributes = {
        str(column["attribute"])
        for column in _tables(terminal.get("column"), "terminal columns")
    }
    expressions: dict[str, str] = {}
    for column in _tables(relation.get("column"), "view columns"):
        attribute = str(column["attribute"])
        if attribute in descriptor_attributes:
            source, alias = descriptor, "descriptor"
        elif attribute in state_attributes:
            source, alias = state, "mutable_state"
        elif attribute in terminal_attributes:
            source, alias = terminal, "terminal"
        else:
            raise ValueError(
                f"lifecycle projection {relation_name!r} cannot source {attribute!r}"
            )
        expressions[attribute] = f"{alias}.{q(_column_name(source, attribute))}"

    raw_columns = _tables(relation.get("column"), "view columns")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in raw_columns
    )
    state_join = "\n AND ".join(
        f"mutable_state.{q(_column_name(state, attribute))} "
        f"= descriptor.{q(_column_name(descriptor, attribute))}"
        for attribute in key_attributes
    )
    terminal_join = "\n AND ".join(
        f"terminal.{q(_column_name(terminal, attribute))} "
        f"= descriptor.{q(_column_name(descriptor, attribute))}"
        for attribute in key_attributes
    )
    state_column = f"mutable_state.{q(_column_name(state, 'state'))}"
    terminal_key = f"terminal.{q(_column_name(terminal, key_attributes[0]))}"

    def literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    absent_sql = ", ".join(literal(value) for value in absent_states)
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    column_list = ", ".join(q(str(column["name"])) for column in raw_columns)
    return f"""{prefix} {q(str(relation["table"]))} ({column_list}) AS
SELECT
  {projection}
FROM {q(str(descriptor["table"]))} AS descriptor
JOIN {q(str(state["table"]))} AS mutable_state
  ON {state_join}
LEFT JOIN {q(str(terminal["table"]))} AS terminal
  ON {terminal_join}
WHERE {state_column} = {literal(present_state)} AND {terminal_key} IS NOT NULL
   OR {state_column} IN ({absent_sql}) AND {terminal_key} IS NULL"""


def _render_build_manifest_projection_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Expose shared build discovery/terminal facts without duplicating authority."""

    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    source_names = _strings(raw_view.get("source_relations"), "view source_relations")
    source_by_name = {name: relations[name] for name in source_names}
    if set(source_by_name) != {
        "build_manifest_core",
        "source_build_discovery",
        "source_build_sealed_at",
    }:
        raise ValueError("build_manifest_projection source set drift")
    core = source_by_name["build_manifest_core"]
    discovery = source_by_name["source_build_discovery"]
    terminal = source_by_name["source_build_sealed_at"]

    def q(value: str) -> str:
        return _quote(value, backend)

    core_attributes = {
        str(column["attribute"])
        for column in _tables(core.get("column"), "build manifest core columns")
    }
    raw_columns = _tables(relation.get("column"), "view columns")
    expressions: dict[str, str] = {}
    for column in raw_columns:
        attribute = str(column["attribute"])
        if attribute in core_attributes:
            expressions[attribute] = f"core.{q(_column_name(core, attribute))}"
        elif attribute == "gallery_count":
            expressions[attribute] = (
                f"discovery.{q(_column_name(discovery, 'gallery_count'))}"
            )
        elif attribute == "computed_at":
            expressions[attribute] = (
                f"terminal.{q(_column_name(terminal, 'sealed_at'))}"
            )
        else:
            raise ValueError(f"build_manifest_projection cannot source {attribute!r}")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in raw_columns
    )
    key = "build_id"
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    column_list = ", ".join(q(str(column["name"])) for column in raw_columns)
    return f"""{prefix} {q(str(relation["table"]))} ({column_list}) AS
SELECT
  {projection}
FROM {q(str(core["table"]))} AS core
JOIN {q(str(discovery["table"]))} AS discovery
  ON discovery.{q(_column_name(discovery, key))}
   = core.{q(_column_name(core, key))}
JOIN {q(str(terminal["table"]))} AS terminal
  ON terminal.{q(_column_name(terminal, key))}
   = core.{q(_column_name(core, key))}"""


def _render_revision_generation_baseline_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    base = relations[_required_string(raw_view, "base_relation", "view")]
    mapping = relations[_required_string(raw_view, "mapping_relation", "view")]
    owner = _required_string(raw_view, "owner_attribute", "view")
    revision = _required_string(raw_view, "revision_attribute", "view")
    mapping_revision = _required_string(raw_view, "mapping_revision_attribute", "view")
    generation = _required_string(raw_view, "generation_attribute", "view")
    mapping_generation = _required_string(
        raw_view, "mapping_generation_attribute", "view"
    )

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        owner: f"base.{q(_column_name(base, owner))}",
        revision: f"base.{q(_column_name(base, revision))}",
        generation: f"mapping.{q(_column_name(mapping, mapping_generation))}",
    }
    columns = _tables(relation.get("column"), "view columns")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in columns
    )
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    column_list = ", ".join(q(str(column["name"])) for column in columns)
    return f"""{prefix} {q(str(relation["table"]))} ({column_list}) AS
SELECT
  {projection}
FROM {q(str(base["table"]))} AS base
JOIN {q(str(mapping["table"]))} AS mapping
  ON mapping.{q(_column_name(mapping, mapping_revision))}
   = base.{q(_column_name(base, revision))}"""


def _render_revision_generation_head_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    revision_relation = relations[
        _required_string(raw_view, "revision_relation", "view")
    ]
    time_relation = relations[_required_string(raw_view, "time_relation", "view")]
    mapping = relations[_required_string(raw_view, "mapping_relation", "view")]
    channel = _required_string(raw_view, "channel_attribute", "view")
    revision = _required_string(raw_view, "revision_attribute", "view")
    generation = _required_string(raw_view, "generation_attribute", "view")
    timestamp = _required_string(raw_view, "time_attribute", "view")

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        channel: f"head.{q(_column_name(revision_relation, channel))}",
        revision: f"head.{q(_column_name(revision_relation, revision))}",
        generation: f"mapping.{q(_column_name(mapping, generation))}",
        timestamp: f"advanced.{q(_column_name(time_relation, timestamp))}",
    }
    columns = _tables(relation.get("column"), "view columns")
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in columns
    )
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    column_list = ", ".join(q(str(column["name"])) for column in columns)
    return f"""{prefix} {q(str(relation["table"]))} ({column_list}) AS
SELECT
  {projection}
FROM {q(str(revision_relation["table"]))} AS head
JOIN {q(str(mapping["table"]))} AS mapping
  ON mapping.{q(_column_name(mapping, revision))}
   = head.{q(_column_name(revision_relation, revision))}
JOIN {q(str(time_relation["table"]))} AS advanced
  ON advanced.{q(_column_name(time_relation, channel))}
   = head.{q(_column_name(revision_relation, channel))}"""


def _view_sources(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    return tuple(
        relations[name]
        for name in _strings(
            raw_view.get("source_relations"),
            f"view {relation.get('name')!r}.source_relations",
        )
    )


def _render_projection_view(
    relation: Mapping[str, Any],
    backend: str,
    expressions: Mapping[str, str],
    from_sql: str,
) -> str:
    """Render one explicitly-columned read-only projection view."""

    def q(value: str) -> str:
        return _quote(value, backend)

    columns = _tables(relation.get("column"), "view columns")
    missing = {
        str(column["attribute"])
        for column in columns
        if str(column["attribute"]) not in expressions
    }
    if missing:
        raise ValueError(
            f"view {relation.get('name')!r} lacks expressions for {sorted(missing)!r}"
        )
    projection = ",\n  ".join(
        f"{expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
        for column in columns
    )
    prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    column_list = ", ".join(q(str(column["name"])) for column in columns)
    return f"""{prefix} {q(str(relation["table"]))} ({column_list}) AS
SELECT
  {projection}
{from_sql}"""


def _render_batch_receipt_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    raw_view = relation.get("view")
    if not isinstance(raw_view, dict):
        raise ValueError(f"view relation {relation.get('name')!r} lacks view metadata")
    (stored,) = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    stored_attributes = {
        str(column["attribute"])
        for column in _tables(stored.get("column"), "stored receipt columns")
    }
    expressions = {
        attribute: f"stored.{q(_column_name(stored, attribute))}"
        for attribute in stored_attributes
    }
    start_generation = _required_string(
        raw_view, "start_generation_attribute", "batch receipt view"
    )
    start_processed_count = _required_string(
        raw_view, "start_processed_count_attribute", "batch receipt view"
    )
    row_count = _required_string(raw_view, "row_count_attribute", "batch receipt view")
    committed_generation = _required_string(
        raw_view, "committed_generation_attribute", "batch receipt view"
    )
    next_processed_count = _required_string(
        raw_view, "next_processed_count_attribute", "batch receipt view"
    )
    terminal = _required_string(raw_view, "terminal_attribute", "batch receipt view")
    next_state = _required_string(
        raw_view, "next_state_attribute", "batch receipt view"
    )
    start_generation_sql = f"stored.{q(_column_name(stored, start_generation))}"
    start_processed_count_sql = (
        f"stored.{q(_column_name(stored, start_processed_count))}"
    )
    row_count_sql = f"stored.{q(_column_name(stored, row_count))}"
    terminal_sql = f"CASE WHEN {row_count_sql} = 0 THEN 1 ELSE 0 END"
    next_state_sql = f"CASE WHEN {row_count_sql} = 0 THEN 'COMPLETE' ELSE 'OPEN' END"
    if backend == "mariadb":
        terminal_sql = f"CAST({terminal_sql} AS UNSIGNED)"
        next_state_sql = (
            f"CAST({next_state_sql} AS CHAR(32) CHARSET ascii) COLLATE ascii_bin"
        )
    expressions.update(
        {
            committed_generation: f"{start_generation_sql} + 1",
            next_processed_count: f"{start_processed_count_sql} + {row_count_sql}",
            terminal: terminal_sql,
            next_state: next_state_sql,
        }
    )
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(stored['table']))} AS stored",
    )


def _render_publication_candidate_projection_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    seal = sources["publication_candidate_projection_seal"]
    checkpoint = sources["publication_checkpoint"]
    receipt = sources["publication_batch_receipt"]

    def q(value: str) -> str:
        return _quote(value, backend)

    def literal(value: str) -> str:
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

    if backend == "sqlite":
        inner_expressions = {
            "candidate_id": f"certified.{q(_column_name(seal, 'candidate_id'))}"
        }
        for attribute, stage in count_stages:
            checkpoint_alias = f"checkpoint_{attribute}"
            receipt_alias = f"receipt_{attribute}"
            inner_expressions[attribute] = (
                f"(SELECT {receipt_alias}."
                f"{q(_column_name(receipt, 'next_processed_count'))}\n"
                f"   FROM {q(str(checkpoint['table']))} AS {checkpoint_alias}\n"
                f"   JOIN {q(str(receipt['table']))} AS {receipt_alias}\n"
                f"     ON {receipt_alias}."
                f"{q(_column_name(receipt, 'candidate_id'))}\n"
                f"      = {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'candidate_id'))}\n"
                f"    AND {receipt_alias}.{q(_column_name(receipt, 'stage'))}\n"
                f"      = {checkpoint_alias}.{q(_column_name(checkpoint, 'stage'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'committed_generation'))}\n"
                f"      = {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'generation'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'next_cursor'))}\n"
                f"      = {checkpoint_alias}.{q(_column_name(checkpoint, 'cursor'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'next_cursor'))}\n"
                f"      = {receipt_alias}."
                f"{q(_column_name(receipt, 'start_cursor'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'next_processed_count'))}\n"
                f"      = {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'processed_count'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'committed_at'))}\n"
                f"      = {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'updated_at'))}\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'terminal'))} = 1\n"
                f"    AND {receipt_alias}."
                f"{q(_column_name(receipt, 'next_state'))}\n"
                f"      = {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'state'))}\n"
                f"  WHERE {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'candidate_id'))}\n"
                f"      = certified.{q(_column_name(seal, 'candidate_id'))}\n"
                f"    AND {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'stage'))} = {literal(stage)}\n"
                f"    AND {checkpoint_alias}."
                f"{q(_column_name(checkpoint, 'state'))} "
                f"= {state_literal('COMPLETE')})"
            )
        columns = _tables(relation.get("column"), "view columns")
        inner_projection = ",\n    ".join(
            f"{inner_expressions[str(column['attribute'])]} AS {q(str(column['name']))}"
            for column in columns
        )
        expressions = {
            str(column["attribute"]): f"exact.{q(str(column['name']))}"
            for column in columns
        }
        complete_counts = "\n  AND ".join(
            f"exact.{q(_column_name(relation, attribute))} IS NOT NULL"
            for attribute, _ in count_stages
        )
        return _render_projection_view(
            relation,
            backend,
            expressions,
            f"FROM (\n"
            f"  SELECT\n"
            f"    {inner_projection}\n"
            f"  FROM {q(str(seal['table']))} AS certified\n"
            f") AS exact\n"
            f"WHERE {complete_counts}",
        )

    expressions = {"candidate_id": f"certified.{q(_column_name(seal, 'candidate_id'))}"}
    joins: list[str] = []
    for attribute, stage in count_stages:
        checkpoint_alias = f"checkpoint_{attribute}"
        receipt_alias = f"receipt_{attribute}"
        expressions[attribute] = (
            f"{receipt_alias}.{q(_column_name(receipt, 'next_processed_count'))}"
        )
        joins.append(
            f"JOIN {q(str(checkpoint['table']))} AS {checkpoint_alias}\n"
            f"  ON {checkpoint_alias}."
            f"{q(_column_name(checkpoint, 'candidate_id'))}\n"
            f"   = certified.{q(_column_name(seal, 'candidate_id'))}\n"
            f" AND {checkpoint_alias}.{q(_column_name(checkpoint, 'stage'))} "
            f"= {literal(stage)}\n"
            f" AND {checkpoint_alias}.{q(_column_name(checkpoint, 'state'))} "
            f"= {state_literal('COMPLETE')}\n"
            f"JOIN {q(str(receipt['table']))} AS {receipt_alias}\n"
            f"  ON {receipt_alias}.{q(_column_name(receipt, 'candidate_id'))}\n"
            f"   = {checkpoint_alias}."
            f"{q(_column_name(checkpoint, 'candidate_id'))}\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'stage'))}\n"
            f"   = {checkpoint_alias}.{q(_column_name(checkpoint, 'stage'))}\n"
            f" AND {receipt_alias}."
            f"{q(_column_name(receipt, 'committed_generation'))}\n"
            f"   = {checkpoint_alias}."
            f"{q(_column_name(checkpoint, 'generation'))}\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'next_cursor'))}\n"
            f"   = {checkpoint_alias}.{q(_column_name(checkpoint, 'cursor'))}\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'next_cursor'))}\n"
            f"   = {receipt_alias}.{q(_column_name(receipt, 'start_cursor'))}\n"
            f" AND {receipt_alias}."
            f"{q(_column_name(receipt, 'next_processed_count'))}\n"
            f"   = {checkpoint_alias}."
            f"{q(_column_name(checkpoint, 'processed_count'))}\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'committed_at'))}\n"
            f"   = {checkpoint_alias}.{q(_column_name(checkpoint, 'updated_at'))}\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'terminal'))} = 1\n"
            f" AND {receipt_alias}.{q(_column_name(receipt, 'next_state'))}\n"
            f"   = {checkpoint_alias}.{q(_column_name(checkpoint, 'state'))}"
        )
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(seal['table']))} AS certified\n" + "\n".join(joins),
    )


def _render_gallery_observation_metadata_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Join observation-local times to the normalized basename/GID/time chain."""

    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    local = sources["gallery_observation_metadata_local"]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]
    upload = sources["gallery_upload_time"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "gallery_id": f"local.{q(_column_name(local, 'gallery_id'))}",
        "observation_id": f"local.{q(_column_name(local, 'observation_id'))}",
        "gid": f"name_gid.{q(_column_name(name_gid, 'gid'))}",
        "upload_time": f"upload.{q(_column_name(upload, 'upload_time'))}",
        "download_time": f"local.{q(_column_name(local, 'download_time'))}",
        "modified_time": f"local.{q(_column_name(local, 'modified_time'))}",
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(local['table']))} AS local\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = local.{q(_column_name(local, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}\n"
        f"JOIN {q(str(upload['table']))} AS upload\n"
        f"  ON upload.{q(_column_name(upload, 'gid'))}\n"
        f"   = name_gid.{q(_column_name(name_gid, 'gid'))}",
    )


def _render_occurrence_identity_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    selection = "publication_selection_storage" in sources
    storage = sources[
        "publication_selection_storage" if selection else "catalog_publication_storage"
    ]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]
    publication = sources["publication_identity"]
    occurrence_attribute = (
        "selection_occurrence_sha256" if selection else "catalog_occurrence_sha256"
    )
    scope_attribute = "candidate_id" if selection else "revision"

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        occurrence_attribute: f"stored.{q(_column_name(storage, occurrence_attribute))}",
        scope_attribute: f"stored.{q(_column_name(storage, scope_attribute))}",
        "publication_key": (
            f"publication.{q(_column_name(publication, 'publication_key'))}"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}\n"
        f"JOIN {q(str(publication['table']))} AS publication\n"
        f"  ON publication.{q(_column_name(publication, 'gid'))}\n"
        f"   = name_gid.{q(_column_name(name_gid, 'gid'))}",
    )


def _render_publication_selection_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    storage = sources["publication_selection_storage"]
    occurrence = sources["publication_selection_occurrence_identity"]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]
    publication = sources["publication_identity"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "candidate_id": f"occurrence.{q(_column_name(occurrence, 'candidate_id'))}",
        "gallery_id": f"stored.{q(_column_name(storage, 'gallery_id'))}",
        "publication_key": (
            f"occurrence.{q(_column_name(occurrence, 'publication_key'))}"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(occurrence['table']))} AS occurrence\n"
        f"  ON occurrence.{q(_column_name(occurrence, 'selection_occurrence_sha256'))}\n"
        f"   = stored.{q(_column_name(storage, 'selection_occurrence_sha256'))}\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}\n"
        f"JOIN {q(str(publication['table']))} AS derived\n"
        f"  ON derived.{q(_column_name(publication, 'gid'))}\n"
        f"   = name_gid.{q(_column_name(name_gid, 'gid'))}\n"
        f" AND derived.{q(_column_name(publication, 'publication_key'))}\n"
        f"   = occurrence.{q(_column_name(occurrence, 'publication_key'))}",
    )


def _render_catalog_publication_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    storage = sources["catalog_publication_storage"]
    occurrence = sources["catalog_publication_occurrence_identity"]
    download = sources["catalog_publication_download_time"]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]
    publication = sources["publication_identity"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "revision": f"occurrence.{q(_column_name(occurrence, 'revision'))}",
        "publication_key": (
            f"occurrence.{q(_column_name(occurrence, 'publication_key'))}"
        ),
        "gallery_id": f"stored.{q(_column_name(storage, 'gallery_id'))}",
        "summary_sha256": f"stored.{q(_column_name(storage, 'summary_sha256'))}",
        "language_sha256": f"stored.{q(_column_name(storage, 'language_sha256'))}",
        "modified_at": f"stored.{q(_column_name(storage, 'modified_at'))}",
        "download_time": f"download.{q(_column_name(download, 'download_time'))}",
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(occurrence['table']))} AS occurrence\n"
        f"  ON occurrence.{q(_column_name(occurrence, 'catalog_occurrence_sha256'))}\n"
        f"   = stored.{q(_column_name(storage, 'catalog_occurrence_sha256'))}\n"
        f"JOIN {q(str(download['table']))} AS download\n"
        f"  ON download.{q(_column_name(download, 'catalog_occurrence_sha256'))}\n"
        f"   = occurrence.{q(_column_name(occurrence, 'catalog_occurrence_sha256'))}\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}\n"
        f"JOIN {q(str(publication['table']))} AS derived\n"
        f"  ON derived.{q(_column_name(publication, 'gid'))}\n"
        f"   = name_gid.{q(_column_name(name_gid, 'gid'))}\n"
        f" AND derived.{q(_column_name(publication, 'publication_key'))}\n"
        f"   = occurrence.{q(_column_name(occurrence, 'publication_key'))}",
    )


def _render_catalog_publication_title_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    storage = sources["catalog_publication_storage"]
    occurrence = sources["catalog_publication_occurrence_identity"]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]
    publication = sources["publication_identity"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "revision": f"occurrence.{q(_column_name(occurrence, 'revision'))}",
        "publication_key": (
            f"occurrence.{q(_column_name(occurrence, 'publication_key'))}"
        ),
        "source_title_sha256": (
            f"stored.{q(_column_name(storage, 'source_title_sha256'))}"
        ),
        "source_gallery_name": (
            f"access.{q(_column_name(access, 'source_gallery_name'))}"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(occurrence['table']))} AS occurrence\n"
        f"  ON occurrence.{q(_column_name(occurrence, 'catalog_occurrence_sha256'))}\n"
        f"   = stored.{q(_column_name(storage, 'catalog_occurrence_sha256'))}\n"
        "\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}\n"
        f"JOIN {q(str(publication['table']))} AS derived\n"
        f"  ON derived.{q(_column_name(publication, 'gid'))}\n"
        f"   = name_gid.{q(_column_name(name_gid, 'gid'))}\n"
        f" AND derived.{q(_column_name(publication, 'publication_key'))}\n"
        f"   = occurrence.{q(_column_name(occurrence, 'publication_key'))}",
    )


def _render_analysis_impacted_gid_provenance_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    storage = sources["analysis_impacted_gid_provenance_storage"]
    access = sources["gallery_source_name_access"]
    name_gid = sources["source_gallery_name_gid"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "analysis_id": f"stored.{q(_column_name(storage, 'analysis_id'))}",
        "gallery_id": f"stored.{q(_column_name(storage, 'gallery_id'))}",
        "gid": f"name_gid.{q(_column_name(name_gid, 'gid'))}",
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(access['table']))} AS access\n"
        f"  ON access.{q(_column_name(access, 'gallery_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'gallery_id'))}\n"
        f"JOIN {q(str(name_gid['table']))} AS name_gid\n"
        f"  ON name_gid.{q(_column_name(name_gid, 'source_gallery_name'))}\n"
        f"   = access.{q(_column_name(access, 'source_gallery_name'))}",
    )


def _render_analysis_impacted_gid_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    storage = sources["analysis_impacted_gid_storage"]
    provenance = sources["analysis_impacted_gid_provenance"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "analysis_id": f"stored.{q(_column_name(storage, 'analysis_id'))}",
        "gid": f"stored.{q(_column_name(storage, 'gid'))}",
        "witness_gallery_id": (
            f"MIN(provenance.{q(_column_name(provenance, 'gallery_id'))})"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(storage['table']))} AS stored\n"
        f"JOIN {q(str(provenance['table']))} AS provenance\n"
        f"  ON provenance.{q(_column_name(provenance, 'analysis_id'))}\n"
        f"   = stored.{q(_column_name(storage, 'analysis_id'))}\n"
        f" AND provenance.{q(_column_name(provenance, 'gid'))}\n"
        f"   = stored.{q(_column_name(storage, 'gid'))}\n"
        f"GROUP BY stored.{q(_column_name(storage, 'analysis_id'))}, "
        f"stored.{q(_column_name(storage, 'gid'))}",
    )


def _render_analysis_ancestry_endpoint_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Project the unique maximum-depth ancestry endpoint for each run."""

    (ancestry,) = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    analysis_id = q(_column_name(ancestry, "analysis_id"))
    ancestor_analysis_id = q(_column_name(ancestry, "ancestor_analysis_id"))
    ancestor_depth = q(_column_name(ancestry, "ancestor_depth"))
    expressions = {
        "analysis_id": f"endpoint.{analysis_id}",
        "anchor_analysis_id": f"endpoint.{ancestor_analysis_id}",
        "overlay_depth": f"endpoint.{ancestor_depth}",
    }
    ancestry_table = q(str(ancestry["table"]))
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {ancestry_table} AS endpoint\n"
        "WHERE NOT EXISTS (\n"
        "  SELECT 1\n"
        f"  FROM {ancestry_table} AS deeper\n"
        f"  WHERE deeper.{analysis_id} = endpoint.{analysis_id}\n"
        f"    AND deeper.{ancestor_depth} > endpoint.{ancestor_depth}\n"
        ")",
    )


def _render_analysis_gid_winner_keyset_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Derive each selected winner's GID from the analysis-pinned build."""

    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    selection = sources["analysis_gid_winner_selection"]
    impacted = sources["analysis_impacted_gid"]
    run_build = sources["analysis_run_descriptor"]
    build_gallery = sources["source_build_gallery"]
    metadata = sources["gallery_observation_metadata"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "analysis_id": f"selected.{q(_column_name(selection, 'analysis_id'))}",
        "gid": f"metadata.{q(_column_name(metadata, 'gid'))}",
        "winner_gallery_id": (
            f"selected.{q(_column_name(selection, 'winner_gallery_id'))}"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(selection['table']))} AS selected\n"
        f"JOIN {q(str(run_build['table']))} AS run_build\n"
        f"  ON run_build.{q(_column_name(run_build, 'analysis_id'))}\n"
        f"   = selected.{q(_column_name(selection, 'analysis_id'))}\n"
        f"JOIN {q(str(build_gallery['table']))} AS build_gallery\n"
        f"  ON build_gallery.{q(_column_name(build_gallery, 'build_id'))}\n"
        f"   = run_build.{q(_column_name(run_build, 'build_id'))}\n"
        f" AND build_gallery.{q(_column_name(build_gallery, 'gallery_id'))}\n"
        f"   = selected.{q(_column_name(selection, 'winner_gallery_id'))}\n"
        f"JOIN {q(str(metadata['table']))} AS metadata\n"
        f"  ON metadata.{q(_column_name(metadata, 'gallery_id'))}\n"
        f"   = build_gallery.{q(_column_name(build_gallery, 'gallery_id'))}\n"
        f" AND metadata.{q(_column_name(metadata, 'observation_id'))}\n"
        f"   = build_gallery.{q(_column_name(build_gallery, 'observation_id'))}\n"
        f"JOIN {q(str(impacted['table']))} AS impacted\n"
        f"  ON impacted.{q(_column_name(impacted, 'analysis_id'))}\n"
        f"   = selected.{q(_column_name(selection, 'analysis_id'))}\n"
        f" AND impacted.{q(_column_name(impacted, 'gid'))}\n"
        f"   = metadata.{q(_column_name(metadata, 'gid'))}",
    )


def _render_artifact_delta_old_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Derive the old artifact side from the candidate-pinned base revision."""

    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    candidate = sources["publication_candidate"]
    base = sources["publication_candidate_base_catalog"]
    occurrence = sources["catalog_artifact"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "candidate_id": f"base.{q(_column_name(base, 'candidate_id'))}",
        "publication_key": (
            f"occurrence.{q(_column_name(occurrence, 'publication_key'))}"
        ),
        "artifact_semantics_sha256": (
            f"occurrence.{q(_column_name(occurrence, 'artifact_semantics_sha256'))}"
        ),
        "artifact_sha256": (
            f"occurrence.{q(_column_name(occurrence, 'artifact_sha256'))}"
        ),
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(base['table']))} AS base\n"
        f"JOIN {q(str(candidate['table']))} AS candidate\n"
        f"  ON candidate.{q(_column_name(candidate, 'candidate_id'))}\n"
        f"   = base.{q(_column_name(base, 'candidate_id'))}\n"
        f"JOIN {q(str(occurrence['table']))} AS occurrence\n"
        f"  ON occurrence.{q(_column_name(occurrence, 'revision'))}\n"
        f"   = base.{q(_column_name(base, 'base_revision'))}",
    )


def _render_artifact_delta_new_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    """Expose desired artifact presence directly from artifact_input."""

    (artifact_input,) = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        attribute: f"input.{q(_column_name(artifact_input, attribute))}"
        for attribute in (
            "candidate_id",
            "publication_key",
            "artifact_semantics_sha256",
        )
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(artifact_input['table']))} AS input",
    )


def _render_publication_commit_baseline_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    base, commit = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions: dict[str, str] = {}
    base_attributes = {
        str(column["attribute"])
        for column in _tables(base.get("column"), "commit baseline columns")
    }
    for column in _tables(relation.get("column"), "baseline view columns"):
        attribute = str(column["attribute"])
        if attribute in base_attributes:
            expressions[attribute] = f"base.{q(_column_name(base, attribute))}"
        elif attribute in {"base_source_revision", "base_revision"}:
            commit_attribute = (
                "source_revision" if attribute == "base_source_revision" else "revision"
            )
            expressions[attribute] = (
                f"committed.{q(_column_name(commit, commit_attribute))}"
            )
        elif attribute in {"base_source_generation", "base_catalog_generation"}:
            expressions[attribute] = (
                f"committed.{q(_column_name(commit, 'generation'))}"
            )
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(base['table']))} AS base\n"
        f"JOIN {q(str(commit['table']))} AS committed\n"
        f"  ON committed.{q(_column_name(commit, 'receipt_id'))}\n"
        f"   = base.{q(_column_name(base, 'base_receipt_id'))}",
    )


def _render_publication_commit_published_descriptor_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    descriptor, commit = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    descriptor_attributes = {
        str(column["attribute"])
        for column in _tables(descriptor.get("column"), "descriptor columns")
    }
    expressions = {
        attribute: f"descriptor.{q(_column_name(descriptor, attribute))}"
        for attribute in descriptor_attributes
    }
    expressions["published_at"] = f"committed.{q(_column_name(commit, 'committed_at'))}"
    join_attribute = (
        "source_revision" if "source_revision" in descriptor_attributes else "revision"
    )
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(descriptor['table']))} AS descriptor\n"
        f"JOIN {q(str(commit['table']))} AS committed\n"
        f"  ON committed.{q(_column_name(commit, join_attribute))}\n"
        f"   = descriptor.{q(_column_name(descriptor, join_attribute))}",
    )


def _render_publication_commit_generation_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    (commit,) = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        str(column["attribute"]): (
            f"committed.{q(_column_name(commit, str(column['attribute'])))}"
        )
        for column in _tables(relation.get("column"), "generation view columns")
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(commit['table']))} AS committed",
    )


def _render_publication_commit_head_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    head, commit, source_descriptor = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        str(column["attribute"]): (
            f"head.{q(_column_name(head, 'channel'))}"
            if str(column["attribute"]) == "channel"
            else f"committed.{q(_column_name(commit, str(column['attribute'])))}"
        )
        for column in _tables(relation.get("column"), "commit head columns")
    }
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(head['table']))} AS head\n"
        f"JOIN {q(str(commit['table']))} AS committed\n"
        f"  ON committed.{q(_column_name(commit, 'receipt_id'))}\n"
        f"   = head.{q(_column_name(head, 'receipt_id'))}\n"
        f"JOIN {q(str(source_descriptor['table']))} AS source_descriptor\n"
        f"  ON source_descriptor.{q(_column_name(source_descriptor, 'source_revision'))}\n"
        f"   = committed.{q(_column_name(commit, 'source_revision'))}\n"
        f" AND source_descriptor.{q(_column_name(source_descriptor, 'channel'))}\n"
        f"   = head.{q(_column_name(head, 'channel'))}",
    )


def _render_publication_commit_head_projection_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    (head,) = _view_sources(relation, relations)

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions: dict[str, str] = {}
    for column in _tables(relation.get("column"), "head projection columns"):
        attribute = str(column["attribute"])
        source_attribute = "committed_at" if attribute == "advanced_at" else attribute
        expressions[attribute] = f"head.{q(_column_name(head, source_attribute))}"
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(head['table']))} AS head",
    )


def _render_publication_receipt_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    commit = sources["publication_commit"]
    catalog_descriptor = sources["catalog_revision_descriptor"]
    source_descriptor = sources["source_revision_descriptor"]
    finalization = sources["publication_commit_finalization"]
    checkpoint = sources["publication_finalization_checkpoint"]
    receipt = sources["publication_finalization_batch_receipt"]

    def q(value: str) -> str:
        return _quote(value, backend)

    def literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    commit_attributes = {
        str(column["attribute"])
        for column in _tables(commit.get("column"), "commit columns")
    }
    expressions = {
        attribute: f"committed.{q(_column_name(commit, attribute))}"
        for attribute in commit_attributes
    }
    terminal_predicate = (
        f"terminal.{q(_column_name(receipt, 'receipt_id'))}\n"
        f"     = final_checkpoint.{q(_column_name(checkpoint, 'receipt_id'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'committed_generation'))}\n"
        f"     = final_checkpoint.{q(_column_name(checkpoint, 'generation'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'next_cursor'))}\n"
        f"     = final_checkpoint.{q(_column_name(checkpoint, 'cursor'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'next_cursor'))}\n"
        f"     = terminal.{q(_column_name(receipt, 'start_cursor'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'next_processed_count'))}\n"
        f"     = final_checkpoint.{q(_column_name(checkpoint, 'processed_count'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'committed_at'))}\n"
        f"     = final_checkpoint.{q(_column_name(checkpoint, 'updated_at'))}\n"
        f" AND terminal.{q(_column_name(receipt, 'terminal'))} = 1\n"
        f" AND terminal.{q(_column_name(receipt, 'row_count'))} = 0\n"
        f" AND terminal.{q(_column_name(receipt, 'next_state'))} "
        f"= {literal('COMPLETE')}"
    )
    state_expression = (
        "CASE WHEN finalized."
        f"{q(_column_name(finalization, 'receipt_id'))} IS NULL THEN "
        f"{literal('DB_COMMITTED')} ELSE "
        f"{literal('PUBLISHED')} END"
    )
    finalized_at_expression = (
        "CASE WHEN finalized."
        f"{q(_column_name(finalization, 'receipt_id'))} IS NULL THEN NULL "
        "ELSE (\n"
        f"  SELECT terminal.{q(_column_name(receipt, 'committed_at'))}\n"
        f"  FROM {q(str(receipt['table']))} AS terminal\n"
        f"  WHERE {terminal_predicate}\n"
        ") END"
    )
    if backend == "mariadb":
        state_expression = (
            f"CAST({state_expression} AS CHAR(16) CHARSET ascii) COLLATE ascii_bin"
        )
        finalized_at_expression = f"CAST({finalized_at_expression} AS UNSIGNED)"
    expressions.update(
        {
            "channel": (
                f"source_descriptor.{q(_column_name(source_descriptor, 'channel'))}"
            ),
            "publication_count": (
                "catalog_descriptor."
                f"{q(_column_name(catalog_descriptor, 'publication_count'))}"
            ),
            "state": state_expression,
            "finalized_at": finalized_at_expression,
        }
    )
    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(commit['table']))} AS committed\n"
        f"JOIN {q(str(catalog_descriptor['table']))} AS catalog_descriptor\n"
        f"  ON catalog_descriptor.{q(_column_name(catalog_descriptor, 'revision'))}\n"
        f"   = committed.{q(_column_name(commit, 'revision'))}\n"
        f"JOIN {q(str(source_descriptor['table']))} AS source_descriptor\n"
        f"  ON source_descriptor.{q(_column_name(source_descriptor, 'source_revision'))}\n"
        f"   = committed.{q(_column_name(commit, 'source_revision'))}\n"
        f"JOIN {q(str(checkpoint['table']))} AS final_checkpoint\n"
        f"  ON final_checkpoint.{q(_column_name(checkpoint, 'receipt_id'))}\n"
        f"   = committed.{q(_column_name(commit, 'receipt_id'))}\n"
        f"LEFT JOIN {q(str(finalization['table']))} AS finalized\n"
        f"  ON finalized.{q(_column_name(finalization, 'receipt_id'))}\n"
        f"   = committed.{q(_column_name(commit, 'receipt_id'))}\n"
        f"WHERE finalized.{q(_column_name(finalization, 'receipt_id'))} IS NULL\n"
        f"       AND final_checkpoint.{q(_column_name(checkpoint, 'state'))} "
        f"= {literal('OPEN')}\n"
        f"   OR finalized.{q(_column_name(finalization, 'receipt_id'))} IS NOT NULL\n"
        f"       AND final_checkpoint.{q(_column_name(checkpoint, 'state'))} "
        f"= {literal('COMPLETE')}\n"
        "       AND EXISTS (\n"
        "         SELECT 1\n"
        f"         FROM {q(str(receipt['table']))} AS terminal\n"
        f"         WHERE {terminal_predicate}\n"
        "       )",
    )


def _render_publication_commit_activation_view(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    sources = {
        str(source["name"]): source for source in _view_sources(relation, relations)
    }
    if set(sources) != {"publication_commit"}:
        raise ValueError("publication commit activation source set drift")
    commit = sources["publication_commit"]

    def q(value: str) -> str:
        return _quote(value, backend)

    expressions = {
        "source_revision": f"committed.{q(_column_name(commit, 'source_revision'))}",
        "preparation_id": f"committed.{q(_column_name(commit, 'preparation_id'))}",
        "operational_policy_id": (
            f"committed.{q(_column_name(commit, 'operational_policy_id'))}"
        ),
        "activated_at": f"committed.{q(_column_name(commit, 'committed_at'))}",
    }

    return _render_projection_view(
        relation,
        backend,
        expressions,
        f"FROM {q(str(commit['table']))} AS committed",
    )


def _render_table(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> str:
    def q(value: str) -> str:
        return _quote(value, backend)

    definitions: list[str] = []
    for raw_column in _tables(relation.get("column"), "physical columns"):
        attribute = _required_string(raw_column, "attribute", "physical column")
        name, type_name, nullable, collation = _backend_column(
            relation, attribute, backend
        )
        definition = f"{q(name)} {type_name}"
        if collation is not None:
            definition += f" COLLATE {collation}"
        definition += " NULL" if nullable else " NOT NULL"
        definitions.append(definition)
    primary_key = _strings(relation.get("primary_key"), "physical primary key")
    if backend == "sqlite":
        definitions.append(
            f"CONSTRAINT {q('pk_' + str(relation['table']))} PRIMARY KEY ("
            + ", ".join(q(_column_name(relation, value)) for value in primary_key)
            + ")"
        )
    else:
        definitions.append(
            "PRIMARY KEY ("
            + ", ".join(q(_column_name(relation, value)) for value in primary_key)
            + ")"
        )
    unique_keys = (
        *_tuples(relation.get("unique_keys", []), "physical unique keys"),
        *_tuples(
            relation.get("referential_unique_keys", []),
            "physical referential unique keys",
        ),
    )
    for position, key in enumerate(unique_keys, 1):
        constraint_name = _ddl_identifier(f"uk_{relation['table']}_{position}")
        definitions.append(
            f"CONSTRAINT {q(constraint_name)} UNIQUE ("
            + ", ".join(q(_column_name(relation, value)) for value in key)
            + ")"
        )
    if backend == "mariadb":
        for index in _required_indexes(relation):
            uniqueness = "UNIQUE " if index.get("unique") is True else ""
            attributes = _strings(index.get("attributes"), "required index")
            definitions.append(
                f"{uniqueness}KEY {q(str(index['name']))} ("
                + ", ".join(q(_column_name(relation, value)) for value in attributes)
                + ")"
            )
    for foreign_key in _foreign_keys(relation):
        target = relations[str(foreign_key["referenced_relation"])]
        attributes = _strings(foreign_key.get("attributes"), "foreign key attributes")
        target_attributes = _strings(
            foreign_key.get("referenced_attributes"), "foreign key target attributes"
        )
        definitions.append(
            f"CONSTRAINT {q(str(foreign_key['name']))} FOREIGN KEY ("
            + ", ".join(q(_column_name(relation, value)) for value in attributes)
            + f") REFERENCES {q(str(target['table']))} ("
            + ", ".join(q(_column_name(target, value)) for value in target_attributes)
            + ")"
        )
    for check in _tables(relation.get("check", []), "physical checks"):
        expression = _required_string(check, f"{backend}_expression", "physical check")
        definitions.append(f"CONSTRAINT {q(str(check['name']))} CHECK ({expression})")
    statement = (
        f"CREATE TABLE IF NOT EXISTS {q(str(relation['table']))} (\n  "
        + ",\n  ".join(definitions)
        + "\n)"
    )
    if backend == "mariadb":
        statement += (
            " ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_nopad_bin"
        )
    return statement


def _render_index(
    relation: Mapping[str, Any], index: Mapping[str, Any], backend: str
) -> str:
    def q(value: str) -> str:
        return _quote(value, backend)

    uniqueness = "UNIQUE " if index.get("unique") is True else ""
    attributes = _strings(index.get("attributes"), "required index attributes")
    return (
        f"CREATE {uniqueness}INDEX IF NOT EXISTS {q(str(index['name']))} "
        f"ON {q(str(relation['table']))} ("
        + ", ".join(q(_column_name(relation, value)) for value in attributes)
        + ")"
    )


def _relation_metadata(
    relation: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> dict[str, Any]:
    columns = []
    for raw_column in _tables(relation.get("column"), "physical columns"):
        attribute = str(raw_column["attribute"])
        name, type_name, nullable, collation = _backend_column(
            relation, attribute, backend
        )
        columns.append((attribute, name, type_name, nullable, collation))
    foreign_keys = []
    for foreign_key in _foreign_keys(relation):
        target = relations[str(foreign_key["referenced_relation"])]
        foreign_keys.append(
            (
                str(foreign_key["name"]),
                tuple(
                    _column_name(relation, value)
                    for value in _strings(
                        foreign_key.get("attributes"), "foreign key attributes"
                    )
                ),
                str(target["table"]),
                tuple(
                    _column_name(target, value)
                    for value in _strings(
                        foreign_key.get("referenced_attributes"),
                        "foreign key target attributes",
                    )
                ),
            )
        )
    return {
        "relation": str(relation["name"]),
        "plane": str(relation["_provider_plane"]),
        "kind": str(relation.get("kind", "table")),
        "table": str(relation["table"]),
        "columns": tuple(columns),
        "primary_key": tuple(
            _column_name(relation, value)
            for value in _strings(relation.get("primary_key"), "primary key")
        ),
        "unique_keys": tuple(
            tuple(_column_name(relation, value) for value in key)
            for key in _tuples(relation.get("unique_keys", []), "unique keys")
        ),
        "referential_unique_keys": tuple(
            tuple(_column_name(relation, value) for value in key)
            for key in _tuples(
                relation.get("referential_unique_keys", []),
                "referential unique keys",
            )
        ),
        "runtime_unique_keys": tuple(
            tuple(_column_name(relation, value) for value in key)
            for key in _tuples(
                relation.get("runtime_unique_keys", []), "runtime unique keys"
            )
        ),
        "foreign_keys": tuple(foreign_keys),
        "indexes": tuple(
            (
                str(index["name"]),
                tuple(
                    _column_name(relation, value)
                    for value in _strings(index.get("attributes"), "required index")
                ),
                index.get("unique") is True,
            )
            for index in _required_indexes(relation)
        ),
        "checks": tuple(
            (
                str(check["name"]),
                _required_string(check, f"{backend}_expression", "physical check"),
            )
            for check in _tables(relation.get("check", []), "physical checks")
        ),
        "view_dependencies": (
            _view_dependencies(relation)
            if isinstance(relation.get("view"), dict)
            else ()
        ),
    }


def _framed_hash(domain: str, values: Iterable[object]) -> str:
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
    """Give binary SQL parameters an unambiguous manifest representation."""

    if isinstance(value, bytes):
        return {"$bytes_hex": value.hex()}
    raise TypeError(f"unsupported manifest value: {type(value).__name__}")


def _machine_obligations(
    data_physical: Mapping[str, Any], operational_physical: Mapping[str, Any]
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    for source_name, document in (
        ("data", data_physical),
        ("operational", operational_physical),
    ):
        for value in _tables(
            document.get("semantic_obligation", []),
            f"{source_name}.semantic_obligation",
        ):
            obligation_id = _required_string(
                value, "id", f"{source_name} semantic obligation"
            )
            version = value.get("version")
            if (
                not isinstance(version, int)
                or isinstance(version, bool)
                or version <= 0
            ):
                raise ValueError(
                    f"semantic obligation {obligation_id!r}.version must be positive"
                )
            scope = _required_string(
                value, "scope", f"semantic obligation {obligation_id!r}"
            )
            result.append(
                {
                    "id": obligation_id,
                    "version": version,
                    "scope": scope,
                    "source": source_name,
                    "contract": dict(sorted(value.items())),
                }
            )
    ids = [str(value["id"]) for value in result]
    if len(ids) != len(set(ids)):
        raise ValueError("machine semantic-obligation IDs must be globally unique")
    return tuple(result)


def _bootstrap_seeds(
    data_physical: Mapping[str, Any],
    operational_physical: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    for source_name, document in (
        ("data", data_physical),
        ("operational", operational_physical),
    ):
        for value in _tables(
            document.get("bootstrap_seed", []), f"{source_name}.bootstrap_seed"
        ):
            seed_id = _required_string(value, "id", f"{source_name} bootstrap seed")
            version = value.get("version")
            if (
                not isinstance(version, int)
                or isinstance(version, bool)
                or version <= 0
            ):
                raise ValueError(f"bootstrap seed {seed_id!r}.version must be positive")
            relation_name = _required_string(
                value, "relation", f"bootstrap seed {seed_id!r}"
            )
            relation = relations.get(relation_name)
            if relation is None or relation.get("kind", "table") != "table":
                raise ValueError(
                    f"bootstrap seed {seed_id!r} targets a missing/non-table "
                    f"provider relation {relation_name!r}"
                )
            cells = _tables(value.get("value"), f"bootstrap seed {seed_id!r}.value")
            attributes = tuple(
                _required_string(cell, "attribute", f"bootstrap seed {seed_id!r} cell")
                for cell in cells
            )
            physical_attributes = tuple(
                _required_string(column, "attribute", f"relation {relation_name!r}")
                for column in _tables(
                    relation.get("column"), f"relation {relation_name!r}.column"
                )
            )
            if attributes != physical_attributes:
                raise ValueError(
                    f"bootstrap seed {seed_id!r} must cover physical attributes "
                    "exactly once and in physical order"
                )
            normalized_cells: list[tuple[str, str, str, int | str]] = []
            for cell in cells:
                attribute = _required_string(
                    cell, "attribute", f"bootstrap seed {seed_id!r} cell"
                )
                value_type = _required_string(
                    cell, "type", f"bootstrap seed {seed_id!r}.{attribute}"
                )
                encodings = tuple(key for key in ("integer", "text") if key in cell)
                allowed_keys = {
                    "attribute",
                    "type",
                    encodings[0] if encodings else "",
                }
                if "encoding" in cell:
                    allowed_keys.add("encoding")
                if len(encodings) != 1 or set(cell) != allowed_keys:
                    raise ValueError(
                        f"bootstrap seed {seed_id!r}.{attribute} must have exactly "
                        "one registered integer or text encoding"
                    )
                encoding: str = encodings[0]
                parameter = cell[encoding]
                if encoding == "integer":
                    if not isinstance(parameter, int) or isinstance(parameter, bool):
                        raise ValueError(
                            f"bootstrap seed {seed_id!r}.{attribute}.integer "
                            "must be an integer"
                        )
                    if value_type in {"uint64", "unix_microseconds"} and parameter < 0:
                        raise ValueError(
                            f"bootstrap seed {seed_id!r}.{attribute} is unsigned"
                        )
                else:
                    if not isinstance(parameter, str):
                        raise ValueError(
                            f"bootstrap seed {seed_id!r}.{attribute}.text must be a "
                            "string"
                        )
                    declared_encoding = cell.get("encoding")
                    if declared_encoding not in {None, "utf8"}:
                        raise ValueError(
                            f"bootstrap seed {seed_id!r}.{attribute} has unsupported "
                            f"text encoding {declared_encoding!r}"
                        )
                    if declared_encoding == "utf8":
                        encoding = "utf8"
                if value_type == "ascii_enum":
                    try:
                        str(parameter).encode("ascii")
                    except UnicodeEncodeError as error:
                        raise ValueError(
                            f"bootstrap seed {seed_id!r}.{attribute} is not ASCII"
                        ) from error
                normalized_cells.append((attribute, value_type, encoding, parameter))
            result.append(
                {
                    "id": seed_id,
                    "version": version,
                    "source": source_name,
                    "relation": relation_name,
                    "value": tuple(normalized_cells),
                    "contract": dict(sorted(value.items())),
                }
            )
        result.extend(
            _expand_bootstrap_seed_ranges(
                source_name=source_name,
                document=document,
                relations=relations,
            )
        )
    ids = [str(value["id"]) for value in result]
    if len(ids) != len(set(ids)):
        raise ValueError("bootstrap-seed IDs must be globally unique")
    keys: list[tuple[str, tuple[object, ...]]] = []
    for seed in result:
        relation = relations[str(seed["relation"])]
        value_by_attribute = {
            str(attribute): parameter
            for attribute, _type, _encoding, parameter in seed["value"]
        }
        primary_key = _strings(
            relation.get("primary_key"),
            f"bootstrap relation {seed['relation']!r}.primary_key",
        )
        keys.append(
            (
                str(seed["relation"]),
                tuple(value_by_attribute[attribute] for attribute in primary_key),
            )
        )
    if len(keys) != len(set(keys)):
        raise ValueError("bootstrap seeds contain duplicate physical primary keys")
    return tuple(result)


def _expand_bootstrap_seed_ranges(
    *,
    source_name: str,
    document: Mapping[str, Any],
    relations: Mapping[str, dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Expand formal fixed-shard ranges into exact checksum-bound SQL rows."""

    result: list[dict[str, Any]] = []
    seen_kinds: set[str] = set()
    for value in _tables(
        document.get("bootstrap_seed_range", []),
        f"{source_name}.bootstrap_seed_range",
    ):
        range_id = _required_string(value, "id", f"{source_name} bootstrap seed range")
        if value.get("version") != 1 or value.get("lifecycle") != "building_only":
            raise ValueError(
                f"bootstrap seed range {range_id!r} must be building-only "
                "protocol version one"
            )
        relation_name = _required_string(
            value, "relation", f"bootstrap seed range {range_id!r}"
        )
        relation = relations.get(relation_name)
        if relation is None or relation.get("kind", "table") != "table":
            raise ValueError(
                f"bootstrap seed range {range_id!r} targets a missing/non-table "
                f"provider relation {relation_name!r}"
            )
        if relation_name != "cleanup_sweep_target":
            raise ValueError(
                f"bootstrap seed range {range_id!r} targets unsupported relation "
                f"{relation_name!r}"
            )
        physical_attributes = tuple(
            _required_string(column, "attribute", f"relation {relation_name!r}")
            for column in _tables(
                relation.get("column"), f"relation {relation_name!r}.column"
            )
        )
        if physical_attributes != ("target_kind", "shard_no", "target_key"):
            raise ValueError(
                "cleanup sweep target physical columns differ from the fixed "
                "range codec"
            )
        target_kind = _required_string(
            value, "target_kind", f"bootstrap seed range {range_id!r}"
        )
        if target_kind in seen_kinds:
            raise ValueError(
                f"bootstrap seed ranges repeat cleanup kind {target_kind!r}"
            )
        seen_kinds.add(target_kind)
        if value.get("shard_start") != 0 or value.get("shard_end") != 255:
            raise ValueError(
                f"bootstrap seed range {range_id!r} must cover shards 0..255"
            )
        if value.get("key_codec") != "target_kind_tag16_u64be_zero8_v1":
            raise ValueError(
                f"bootstrap seed range {range_id!r} has the wrong key codec"
            )
        tag_hex = _required_string(
            value,
            "target_kind_tag_hex",
            f"bootstrap seed range {range_id!r}",
        )
        try:
            tag = bytes.fromhex(tag_hex)
            expected_tag = hashlib.sha256(
                b"h2hdb-cleanup-target-v1\0" + target_kind.encode("ascii")
            ).digest()[:16]
        except (UnicodeEncodeError, ValueError) as error:
            raise ValueError(
                f"bootstrap seed range {range_id!r} has an invalid tag domain"
            ) from error
        if len(tag) != 16 or tag != expected_tag:
            raise ValueError(
                f"bootstrap seed range {range_id!r} tag differs from the "
                "domain-separated target-kind codec"
            )
        for shard_no in range(256):
            target_key = tag + shard_no.to_bytes(8, "big") + bytes(8)
            result.append(
                {
                    "id": f"{range_id}.shard-{shard_no:03d}",
                    "version": 1,
                    "source": source_name,
                    "relation": relation_name,
                    "value": (
                        ("target_kind", "ascii_enum", "text", target_kind),
                        ("shard_no", "uint64", "integer", shard_no),
                        ("target_key", "fixed_bytes32", "hex", target_key.hex()),
                    ),
                    "contract": {
                        **dict(sorted(value.items())),
                        "expanded_shard": shard_no,
                    },
                }
            )
    return tuple(result)


def _bootstrap_contracts(
    data_physical: Mapping[str, Any],
    operational_physical: Mapping[str, Any],
    seeds: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    for source_name, document in (
        ("data", data_physical),
        ("operational", operational_physical),
    ):
        raw_contract = document.get("bootstrap_contract")
        source_seeds = tuple(
            seed for seed in seeds if seed.get("source") == source_name
        )
        if raw_contract is None:
            if source_seeds:
                raise ValueError(
                    f"{source_name} declares bootstrap seeds without a "
                    "bootstrap_contract"
                )
            continue
        if not isinstance(raw_contract, dict):
            raise ValueError(f"{source_name}.bootstrap_contract must be a table")
        version = raw_contract.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
            raise ValueError(
                f"{source_name}.bootstrap_contract.version must be positive"
            )
        seeded = _strings(
            raw_contract.get("seeded_relations"),
            f"{source_name}.bootstrap_contract.seeded_relations",
        )
        absent = _strings(
            raw_contract.get("absent_relations"),
            f"{source_name}.bootstrap_contract.absent_relations",
        )
        raw_derived = raw_contract.get("derived_relations", [])
        if not isinstance(raw_derived, list) or not all(
            isinstance(value, str) and value for value in raw_derived
        ):
            raise ValueError(
                f"{source_name}.bootstrap_contract.derived_relations must be "
                "a string array"
            )
        derived = tuple(raw_derived)
        if (
            len(seeded) != len(set(seeded))
            or len(absent) != len(set(absent))
            or len(derived) != len(set(derived))
        ):
            raise ValueError(
                f"{source_name} bootstrap relation partitions contain duplicates"
            )
        partitions = (set(seeded), set(absent), set(derived))
        if any(
            left & right
            for position, left in enumerate(partitions)
            for right in partitions[position + 1 :]
        ):
            raise ValueError(f"{source_name} bootstrap relation partitions overlap")
        relation_names = {
            _required_string(value, "name", f"{source_name} physical relation")
            for value in _relations(document, source_name)
        }
        epoch_owned = _required_string(
            raw_contract,
            "epoch_owned_relation",
            f"{source_name}.bootstrap_contract",
        )
        if epoch_owned != CONTROL_RELATION:
            raise ValueError(
                f"{source_name} bootstrap contract assigns unexpected epoch owner "
                f"{epoch_owned!r}"
            )
        if set(seeded) | set(absent) | set(derived) | {
            epoch_owned
        } != relation_names | {epoch_owned}:
            raise ValueError(
                f"{source_name} bootstrap contract does not partition its "
                "physical relations plus the external epoch-control owner"
            )
        if {str(seed["relation"]) for seed in source_seeds} != set(seeded):
            raise ValueError(
                f"{source_name} bootstrap seeds do not cover exactly its seeded "
                "relations"
            )
        result.append(
            {
                "source": source_name,
                "version": version,
                "epoch_owned_relation": epoch_owned,
                "seeded_relations": seeded,
                "absent_relations": absent,
                "derived_relations": derived,
                "contract": dict(sorted(raw_contract.items())),
            }
        )
    return tuple(result)


def _seed_parameters(seed: Mapping[str, Any]) -> tuple[int | str | bytes, ...]:
    result: list[int | str | bytes] = []
    for _attribute, _value_type, encoding, parameter in seed["value"]:
        if encoding == "hex":
            if not isinstance(parameter, str):
                raise ValueError(f"bootstrap seed {seed['id']!r} hex value is not text")
            try:
                result.append(bytes.fromhex(parameter))
            except ValueError as error:
                raise ValueError(
                    f"bootstrap seed {seed['id']!r} has invalid hexadecimal bytes"
                ) from error
        elif encoding == "utf8":
            if not isinstance(parameter, str):
                raise ValueError(
                    f"bootstrap seed {seed['id']!r} UTF-8 value is not text"
                )
            result.append(parameter.encode("utf-8"))
        else:
            result.append(parameter)
    return tuple(result)


def _render_bootstrap_seed(
    seed: Mapping[str, Any], relation: Mapping[str, Any], backend: str
) -> dict[str, Any]:
    relation_name = str(seed["relation"])
    target_table = _required_string(
        relation, "table", f"bootstrap relation {relation_name!r}"
    )
    if _SAFE_IDENTIFIER.fullmatch(target_table) is None:
        raise ValueError(
            f"bootstrap target table is not a safe identifier: {target_table!r}"
        )
    attributes = tuple(str(value[0]) for value in seed["value"])
    columns = tuple(_column_name(relation, attribute) for attribute in attributes)
    parameters = _seed_parameters(seed)
    for attribute, _value_type, encoding, _parameter in seed["value"]:
        _name, physical_type, _nullable, _collation_name = _backend_column(
            relation, str(attribute), backend
        )
        if encoding == "integer" and "INT" not in physical_type:
            raise ValueError(
                f"bootstrap seed {seed['id']!r}.{attribute} integer encoding "
                f"does not match {backend} type {physical_type!r}"
            )
        if encoding == "text" and not any(
            marker in physical_type for marker in ("CHAR", "CLOB", "TEXT")
        ):
            raise ValueError(
                f"bootstrap seed {seed['id']!r}.{attribute} text encoding does "
                f"not match {backend} type {physical_type!r}"
            )
        if encoding == "utf8" and not any(
            marker in physical_type for marker in ("BLOB", "BINARY")
        ):
            raise ValueError(
                f"bootstrap seed {seed['id']!r}.{attribute} UTF-8 byte encoding "
                f"does not match {backend} type {physical_type!r}"
            )
        if encoding == "hex" and not any(
            marker in physical_type for marker in ("BLOB", "BINARY")
        ):
            raise ValueError(
                f"bootstrap seed {seed['id']!r}.{attribute} binary encoding does "
                f"not match {backend} type {physical_type!r}"
            )
    primary_key = _strings(
        relation.get("primary_key"), f"bootstrap relation {relation_name!r}.primary_key"
    )
    key_positions = tuple(attributes.index(attribute) for attribute in primary_key)
    key_columns = tuple(_column_name(relation, attribute) for attribute in primary_key)
    key_parameters = tuple(parameters[position] for position in key_positions)
    quoted_columns = ", ".join(_quote(column, backend) for column in columns)
    placeholders = ", ".join("%s" for _column in columns)
    if backend == "sqlite":
        conflict = (
            "ON CONFLICT ("
            + ", ".join(_quote(column, backend) for column in key_columns)
            + ") DO NOTHING"
        )
    else:
        no_op_column = _quote(key_columns[0], backend)
        conflict = f"ON DUPLICATE KEY UPDATE {no_op_column} = {no_op_column}"
    insert_sql = (
        f"INSERT INTO {target_table} ({quoted_columns}) VALUES ({placeholders}) "
        f"{conflict}"
    )
    predicate = " AND ".join(
        f"{_quote(column, backend)} = %s" for column in key_columns
    )
    validation_sql = f"SELECT {quoted_columns} FROM {target_table} WHERE {predicate}"
    return {
        "seed_id": str(seed["id"]),
        "target_relation": relation_name,
        "target_table": target_table,
        "sql": insert_sql,
        "parameters": parameters,
        "validation_sql": validation_sql,
        "validation_parameters": key_parameters,
        "expected_row": parameters,
    }


def _render_bootstrap_validation(
    *,
    contracts: Sequence[Mapping[str, Any]],
    seeds: Sequence[Mapping[str, Any]],
    relations: Mapping[str, dict[str, Any]],
    backend: str,
) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
    seeded_relations: list[dict[str, Any]] = []
    absent_relations: list[dict[str, Any]] = []
    for contract in contracts:
        for relation_name in contract["seeded_relations"]:
            relation = relations[str(relation_name)]
            target_table = str(relation["table"])
            columns = tuple(
                _column_name(relation, str(column["attribute"]))
                for column in _tables(relation.get("column"), "seeded relation columns")
            )
            relation_seeds = tuple(
                seed for seed in seeds if seed.get("relation") == relation_name
            )
            seeded_relations.append(
                {
                    "relation": str(relation_name),
                    "table": target_table,
                    "validation_sql": (
                        "SELECT "
                        + ", ".join(_quote(column, backend) for column in columns)
                        + f" FROM {target_table}"
                    ),
                    "expected_rows": tuple(
                        _seed_parameters(seed) for seed in relation_seeds
                    ),
                }
            )
        for relation_name in contract["absent_relations"]:
            relation = relations[str(relation_name)]
            relation_kind = str(relation.get("kind", "table"))
            if relation_kind not in {"table", "view"}:
                raise ValueError(
                    f"bootstrap absence relation {relation_name!r} has unsupported "
                    f"kind {relation_kind!r}"
                )
            target_table = str(relation["table"])
            absent_relations.append(
                {
                    "relation": str(relation_name),
                    "kind": relation_kind,
                    "table": target_table,
                    "validation_sql": f"SELECT 1 FROM {target_table} LIMIT 1",
                }
            )
    return tuple(seeded_relations), tuple(absent_relations)


def _unresolved_obligation_sources(
    data_physical: Mapping[str, Any], operational_physical: Mapping[str, Any]
) -> tuple[tuple[str, str, str], ...]:
    result: list[tuple[str, str, str]] = []
    raw_runtime = data_physical.get("runtime_obligations", [])
    if not isinstance(raw_runtime, list) or not all(
        isinstance(value, str) and value for value in raw_runtime
    ):
        raise ValueError("data runtime_obligations must be an array of strings")
    data_obligations = _tables(
        data_physical.get("semantic_obligation", []),
        "data.semantic_obligation",
    )
    data_ids = {
        _required_string(value, "id", "data semantic obligation")
        for value in data_obligations
    }
    expected_owner: dict[str, str] = {}
    for obligation in data_obligations:
        obligation_id = _required_string(obligation, "id", "data semantic obligation")
        for path in _strings(
            obligation.get("covers"),
            f"data semantic obligation {obligation_id!r}.covers",
        ):
            if path.startswith("machine_contract."):
                continue
            previous = expected_owner.setdefault(path, obligation_id)
            if previous != obligation_id:
                raise ValueError(
                    f"data runtime obligation {path!r} is multiply owned by "
                    f"{previous!r} and {obligation_id!r}"
                )
    bindings = _tables(
        data_physical.get("runtime_obligation_binding", []),
        "data.runtime_obligation_binding",
    )
    bound_paths: set[str] = set()
    bound_texts: list[str] = []
    for binding in bindings:
        path = _required_string(binding, "path", "data runtime obligation binding")
        prose = _required_string(binding, "text", f"data binding {path!r}")
        obligation_id = _required_string(
            binding, "semantic_obligation_id", f"data binding {path!r}"
        )
        if path in bound_paths:
            raise ValueError(f"duplicate data runtime obligation binding {path!r}")
        bound_paths.add(path)
        bound_texts.append(prose)
        expected = expected_owner.get(path)
        if expected is None:
            raise ValueError(
                f"data runtime obligation binding {path!r} is not machine-covered"
            )
        if obligation_id not in data_ids or obligation_id != expected:
            raise ValueError(
                f"data runtime obligation binding {path!r} names "
                f"{obligation_id!r}, expected {expected!r}"
            )
    if bound_paths != set(expected_owner):
        raise ValueError(
            "data runtime obligation bindings do not exactly cover semantic paths"
        )
    if tuple(bound_texts) != tuple(raw_runtime):
        raise ValueError(
            "data runtime obligation binding text/order differs from "
            "runtime_obligations"
        )
    raw_domains = operational_physical.get("domain_obligations", {})
    if not isinstance(raw_domains, dict) or not all(
        isinstance(key, str) and key and isinstance(value, str) and value
        for key, value in raw_domains.items()
    ):
        raise ValueError("operational domain_obligations must be a string table")
    result.extend(
        ("operational.domain_obligations", str(key), str(value))
        for key, value in raw_domains.items()
    )
    return tuple(result)


def _provider_payload() -> dict[str, Any]:
    catalog_logical = _load(CATALOG_LOGICAL)
    data_physical = _load(DATA_PHYSICAL)
    operational_logical = _load(OPERATIONAL_LOGICAL)
    operational_physical = _load(OPERATIONAL_PHYSICAL)

    data_relation_values = [dict(value) for value in _relations(data_physical, "data")]
    operational_relation_values = [
        dict(value) for value in _relations(operational_physical, "operational")
    ]
    data_relations = _relation_map(data_relation_values)
    operational_relations_all = _relation_map(operational_relation_values)
    if CONTROL_RELATION not in operational_relations_all:
        raise ValueError("operational physical contract omits schema epoch control")
    control = operational_relations_all[CONTROL_RELATION]
    if control.get("table") != CONTROL_TABLE:
        raise ValueError("schema epoch control maps to the wrong table")
    epoch_control = operational_physical.get("epoch_control")
    if not isinstance(epoch_control, dict) or epoch_control != {
        "relation": CONTROL_RELATION,
        "table": CONTROL_TABLE,
        "ownership": "epoch_catalog",
        "provider_slice": False,
        "rationale": epoch_control.get("rationale") if epoch_control else None,
    }:
        raise ValueError(
            "operational epoch_control must assign only h2hdb_schema_epoch to "
            "the epoch catalog and exclude it from the provider slice"
        )
    if tuple(operational_physical.get("epoch_owned_relations", ())) != (
        CONTROL_RELATION,
    ):
        raise ValueError("epoch_owned_relations must contain exactly schema control")
    if CONTROL_RELATION in _strings(
        operational_physical.get("source_slice"), "operational source_slice"
    ):
        raise ValueError("operational provider source_slice includes epoch control")

    operational_relations = {
        name: relation
        for name, relation in operational_relations_all.items()
        if name != CONTROL_RELATION
    }
    collision = set(data_relations) & set(operational_relations)
    if collision:
        raise ValueError(
            f"data/operational local relation collision: {sorted(collision)!r}"
        )
    _validate_external_contracts(
        catalog_logical=catalog_logical,
        operational_logical=operational_logical,
        data_physical=data_physical,
        data_relations=data_relations,
        operational_physical=operational_physical,
    )

    for relation in data_relations.values():
        relation["_provider_plane"] = "data"
    for relation in operational_relations.values():
        relation["_provider_plane"] = "operational"
    combined = {**data_relations, **operational_relations}
    data_order = _strings(data_physical.get("source_slice"), "data source_slice")
    operational_order = _strings(
        operational_physical.get("source_slice"), "operational source_slice"
    )
    data_inline_projections = _strings(
        data_physical.get("inline_projections", []), "data inline_projections"
    )
    operational_inline_projections = _strings(
        operational_physical.get("inline_projections", []),
        "operational inline_projections",
    )
    if set(data_inline_projections) & set(data_order):
        raise ValueError("data inline projections overlap physical source_slice")
    if set(operational_inline_projections) & set(operational_order):
        raise ValueError("operational inline projections overlap physical source_slice")
    complete_operational = _strings(
        operational_physical.get("complete_relations"),
        "operational complete_relations",
    )
    if set(complete_operational) != set(operational_order) | {CONTROL_RELATION}:
        raise ValueError(
            "operational complete_relations differs from provider slice plus control"
        )
    if set(data_order) != set(data_relations):
        raise ValueError("data source_slice differs from physical relation coverage")
    if set(operational_order) != set(operational_relations):
        raise ValueError(
            "operational source_slice differs from provider-owned relation coverage"
        )
    order = _topological_order(combined, (*data_order, *operational_order))
    fk_blockers = _validate_foreign_keys(combined)

    machine_obligations = _machine_obligations(data_physical, operational_physical)
    bootstrap_seeds = _bootstrap_seeds(data_physical, operational_physical, combined)
    bootstrap_contracts = _bootstrap_contracts(
        data_physical, operational_physical, bootstrap_seeds
    )
    unresolved_sources = _unresolved_obligation_sources(
        data_physical, operational_physical
    )
    formal_blockers: list[str] = []
    if not machine_obligations:
        formal_blockers.append(
            "formal physical contracts declare no machine-readable "
            "[[semantic_obligation]] IDs/versions/scopes"
        )
    if not bootstrap_seeds:
        formal_blockers.append(
            "formal physical contracts declare no machine-readable "
            "[[bootstrap_seed]] genesis rows"
        )
    if unresolved_sources:
        formal_blockers.append(
            "formal physical contracts retain natural-language runtime obligations "
            "without machine-readable semantic_obligation IDs"
        )

    source_provenance = tuple(
        (
            str(path.relative_to(ROOT)),
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in SOURCE_PATHS
    )
    source_manifest = _framed_hash("h2hdb-vnext-provider-sources-v1", source_provenance)
    obligation_manifest = (
        _framed_hash("h2hdb-vnext-provider-obligations-v1", machine_obligations)
        if machine_obligations
        else None
    )
    backends: dict[str, Any] = {}
    object_names: set[tuple[str, str]] = set()
    for backend in ("sqlite", "mariadb"):
        slices: list[tuple[str, tuple[tuple[str, str, str, str], ...]]] = []
        relations_metadata: list[dict[str, Any]] = []
        backend_blockers = [*formal_blockers, *fk_blockers]
        rendered_seeds = tuple(
            _render_bootstrap_seed(seed, combined[str(seed["relation"])], backend)
            for seed in bootstrap_seeds
        )
        seeded_validation, absent_validation = _render_bootstrap_validation(
            contracts=bootstrap_contracts,
            seeds=bootstrap_seeds,
            relations=combined,
            backend=backend,
        )
        seed_manifest = (
            _framed_hash(
                "h2hdb-vnext-provider-seeds-v1",
                (
                    {
                        "epoch": EPOCH,
                        "schema_version": SCHEMA_VERSION,
                        "backend": backend,
                    },
                    bootstrap_seeds,
                    bootstrap_contracts,
                    rendered_seeds,
                    seeded_validation,
                    absent_validation,
                ),
            )
            if bootstrap_seeds
            else None
        )
        for relation_name in order:
            relation = combined[relation_name]
            relation_kind = str(relation.get("kind", "table"))
            table_name = str(relation["table"])
            statements: list[tuple[str, str, str, str]] = []
            if relation_kind == "view":
                sql = _render_view(relation, combined, backend)
                statements.append(
                    (f"create:view:{table_name}", "view", table_name, sql)
                )
            elif relation_kind == "table":
                sql = _render_table(relation, combined, backend)
                statements.append(
                    (f"create:table:{table_name}", "table", table_name, sql)
                )
                # SQLite exposes each explicit index as a top-level sqlite_master
                # object.  MariaDB does not retain enough metadata to distinguish
                # inline required indexes from indexes owned/created for PK, UK,
                # and FK constraints, so its epoch catalog deliberately treats
                # every index as part of the owning table shape instead.
                if backend == "sqlite":
                    for index in _required_indexes(relation):
                        index_name = str(index["name"])
                        statements.append(
                            (
                                f"create:index:{index_name}",
                                "index",
                                index_name,
                                _render_index(relation, index, backend),
                            )
                        )
            else:
                raise ValueError(
                    f"unsupported physical relation kind {relation_kind!r}"
                )
            slices.append((f"relation:{relation_name}", tuple(statements)))
            relations_metadata.append(_relation_metadata(relation, combined, backend))
        statement_objects = [
            (kind, name)
            for _slice_id, statements in slices
            for _statement_id, kind, name, _sql in statements
        ]
        if len(statement_objects) != len(set(statement_objects)):
            raise ValueError(f"{backend} provider declares a schema object twice")
        if backend == "sqlite":
            object_names = set(statement_objects)
        else:
            sqlite_non_indexes = {
                value for value in object_names if value[0] != "index"
            }
            if sqlite_non_indexes != set(statement_objects):
                raise ValueError(
                    "SQLite/MariaDB provider table/view/trigger object sets differ"
                )
        control_metadata = _relation_metadata(
            {**control, "_provider_plane": "epoch_catalog"},
            {**combined, CONTROL_RELATION: control},
            backend,
        )
        ddl_manifest = _framed_hash(
            "h2hdb-vnext-provider-ddl-v1",
            (
                {"epoch": EPOCH, "schema_version": SCHEMA_VERSION, "backend": backend},
                control_metadata,
                tuple(relations_metadata),
                slices,
                tuple(sorted(statement_objects)),
            ),
        )
        backends[backend] = {
            "ddl_manifest_sha256": ddl_manifest,
            "seed_manifest_sha256": seed_manifest,
            "expected_objects": tuple(sorted(statement_objects)),
            "slices": tuple(slices),
            "relations": tuple(relations_metadata),
            "epoch_control": control_metadata,
            "bootstrap_seeds": rendered_seeds,
            "bootstrap_seeded_relations": seeded_validation,
            "bootstrap_absent_relations": absent_validation,
            "provider_blockers": tuple(dict.fromkeys(backend_blockers)),
        }

    return {
        "artifact_version": 1,
        "epoch": EPOCH,
        "schema_version": SCHEMA_VERSION,
        "source_provenance": source_provenance,
        "source_manifest_sha256": source_manifest,
        "semantic_obligations": machine_obligations,
        "obligation_manifest_sha256": obligation_manifest,
        "bootstrap_seeds": bootstrap_seeds,
        "bootstrap_contracts": bootstrap_contracts,
        "unresolved_obligation_sources": unresolved_sources,
        "relation_order": order,
        "data_relations": data_order,
        "data_inline_projections": data_inline_projections,
        "operational_relations": operational_order,
        "operational_inline_projections": operational_inline_projections,
        "backends": backends,
    }


_LOADER_CONSTANTS = (
    "_RESOURCE_NAME",
    "_PICKLE_PROTOCOL",
    "_RAW_SIZE",
    "_RAW_SHA256",
)


def _loader_metadata(module_path: Path) -> tuple[str, int, int, str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    values: dict[str, object] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name) or target.id not in _LOADER_CONSTANTS:
            continue
        values[target.id] = ast.literal_eval(statement.value)
    if set(values) != set(_LOADER_CONSTANTS):
        raise ValueError("generated schema loader metadata is incomplete")
    resource_name = values["_RESOURCE_NAME"]
    pickle_protocol = values["_PICKLE_PROTOCOL"]
    raw_size = values["_RAW_SIZE"]
    raw_sha256 = values["_RAW_SHA256"]
    if (
        type(resource_name) is not str
        or type(pickle_protocol) is not int
        or type(raw_size) is not int
        or type(raw_sha256) is not str
    ):
        raise ValueError("generated schema loader metadata has invalid types")
    return (
        resource_name,
        pickle_protocol,
        raw_size,
        raw_sha256,
    )


def _matching_existing_resource(
    *, module_path: Path, resource_path: Path, canonical_raw: bytes
) -> bytes | None:
    if not module_path.is_file() or not resource_path.is_file():
        return None
    try:
        (
            resource_name,
            pickle_protocol,
            raw_size,
            raw_sha256,
        ) = _loader_metadata(module_path)
        if resource_name != resource_path.name:
            return None
        raw = resource_path.read_bytes()
        decoded = _decode_schema_artifact(
            raw,
            pickle_protocol=pickle_protocol,
            raw_size=raw_size,
            raw_sha256=raw_sha256,
        )
        if _encode_schema_artifact(decoded) != canonical_raw:
            return None
    except OSError, SyntaxError, ValueError, _SchemaArtifactCodecError:
        return None
    return raw


def _render_loader(*, raw: bytes) -> str:
    raw_digest = hashlib.sha256(raw).hexdigest()
    return f'''"""Generated vNext schema-provider loader; do not edit by hand.

Regenerate with ``python scripts/generate-vnext-schema-provider.py``.
This module and its binary resource have no verification-package dependency.
"""

from __future__ import annotations

from ._schema_artifact_codec import _load_pinned_schema_artifact_resource

_RESOURCE_NAME = "_generated_vnext_schema.bin"
_PICKLE_PROTOCOL = {_SCHEMA_ARTIFACT_PICKLE_PROTOCOL}
_RAW_SIZE = {len(raw)}
_RAW_SHA256 = "{raw_digest}"

ARTIFACT = _load_pinned_schema_artifact_resource(
    package=__package__,
    resource_name=_RESOURCE_NAME,
    pickle_protocol=_PICKLE_PROTOCOL,
    raw_size=_RAW_SIZE,
    raw_sha256=_RAW_SHA256,
)

del _load_pinned_schema_artifact_resource
'''


def render(
    *, module_path: Path = GENERATED, resource_path: Path = GENERATED_RESOURCE
) -> tuple[str, bytes]:
    canonical_raw = _encode_schema_artifact(_provider_payload())
    raw = _matching_existing_resource(
        module_path=module_path,
        resource_path=resource_path,
        canonical_raw=canonical_raw,
    )
    if raw is None:
        raw = canonical_raw
    return (
        _render_loader(raw=raw),
        raw,
    )


def _write_if_changed(path: Path, content: str | bytes) -> None:
    current: str | bytes | None
    if isinstance(content, str):
        current = path.read_text(encoding="utf-8") if path.is_file() else None
    else:
        current = path.read_bytes() if path.is_file() else None
    if current == content:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--output-directory",
        type=Path,
        help="write the canonical loader and resource to this directory",
    )
    arguments = parser.parse_args()
    if arguments.output_directory is None:
        module_path = GENERATED
        resource_path = GENERATED_RESOURCE
    else:
        module_path = arguments.output_directory / GENERATED.name
        resource_path = arguments.output_directory / GENERATED_RESOURCE.name
    expected_module, expected_resource = render(
        module_path=module_path,
        resource_path=resource_path,
    )
    if arguments.check:
        actual_module = (
            module_path.read_text(encoding="utf-8") if module_path.is_file() else ""
        )
        actual_resource = resource_path.read_bytes() if resource_path.is_file() else b""
        if actual_module != expected_module or actual_resource != expected_resource:
            raise SystemExit(
                f"{module_path} or {resource_path} is stale; regenerate it with "
                "python scripts/generate-vnext-schema-provider.py"
            )
        return
    _write_if_changed(resource_path, expected_resource)
    _write_if_changed(module_path, expected_module)


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    main()
