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
import hashlib
import json
import pprint
import re
import sys
import tomllib
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GENERATED = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
CATALOG_LOGICAL = ROOT / "verification" / "schema" / "catalog.toml"
DATA_PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
OPERATIONAL_LOGICAL = ROOT / "verification" / "schema" / "operational.toml"
OPERATIONAL_PHYSICAL = ROOT / "verification" / "schema" / "operational_physical.toml"
SOURCE_PATHS = (
    CATALOG_LOGICAL,
    DATA_PHYSICAL,
    OPERATIONAL_LOGICAL,
    OPERATIONAL_PHYSICAL,
    Path(__file__).resolve(),
)


def _ddl_identifier(value: str) -> str:
    """Match the physical generators' portable MariaDB 63-byte names."""

    encoded = value.encode("ascii")
    if len(encoded) <= 63:
        return value
    return f"{value[:50]}_{hashlib.sha256(encoded).hexdigest()[:12]}"


EPOCH = 2
SCHEMA_VERSION = 1
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
    if len(external) != len(raw_external) or len(stubs) != len(raw_stubs):
        raise ValueError("operational external declarations contain duplicate names")
    if set(external) != set(stubs):
        raise ValueError("operational external relation and stub sets differ")

    for name in sorted(external):
        source = external[name]
        stub = stubs[name]
        logical_target = catalog_relations.get(name)
        physical_target = data_relations.get(name)
        if logical_target is None or physical_target is None:
            raise ValueError(
                f"operational external relation {name!r} has no data target"
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
            for field in (
                "ancestry_relation",
                "shadow_relation",
                "tombstone_relation",
            ):
                target = _required_string(raw_view, field, f"relation {name!r}.view")
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


def _render_view(
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
    for position, key in enumerate(
        _tuples(relation.get("unique_keys", []), "physical unique keys"), 1
    ):
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
            tuple(
                str(relation["view"][field])
                for field in (
                    "ancestry_relation",
                    "shadow_relation",
                    "tombstone_relation",
                )
            )
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
        if len(seeded) != len(set(seeded)) or len(absent) != len(set(absent)):
            raise ValueError(
                f"{source_name} bootstrap relation partitions contain duplicates"
            )
        if set(seeded) & set(absent):
            raise ValueError(
                f"{source_name} bootstrap seeded and absent relations overlap"
            )
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
        if set(seeded) | set(absent) | {epoch_owned} != relation_names | {epoch_owned}:
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
        "operational_relations": operational_order,
        "backends": backends,
    }


def render() -> str:
    payload = _provider_payload()
    rendered = pprint.pformat(payload, width=100, sort_dicts=True)
    return (
        '"""Generated vNext schema-provider artifact; do not edit by hand.\n\n'
        "Regenerate with ``python scripts/generate-vnext-schema-provider.py``.\n"
        'This module intentionally has no dependency on the repository verification package.\n"""\n\n'
        "from __future__ import annotations\n\n"
        "# fmt: off\n"
        f"ARTIFACT = {rendered}\n"
        "# fmt: on\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    expected = render()
    if arguments.check:
        actual = GENERATED.read_text(encoding="utf-8") if GENERATED.exists() else ""
        if actual != expected:
            raise SystemExit(
                f"{GENERATED.relative_to(ROOT)} is stale; regenerate it with "
                "python scripts/generate-vnext-schema-provider.py"
            )
        return
    GENERATED.write_text(expected, encoding="utf-8")


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    main()
