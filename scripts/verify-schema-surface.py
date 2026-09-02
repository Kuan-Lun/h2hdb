#!/usr/bin/env python3
"""Reject unknown production relations and every mutation of a manifest view."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIRECTORY = REPOSITORY_ROOT / "verification" / "schema"
PHYSICAL_MANIFESTS = (
    SCHEMA_DIRECTORY / "physical.toml",
    SCHEMA_DIRECTORY / "operational_physical.toml",
)
PACKAGE_ROOT = REPOSITORY_ROOT / "src" / "h2hdb"
GENERATED_LOADER_NAME = "_generated_vnext_schema.py"
GENERATED_RESOURCE_NAME = "_generated_vnext_schema.bin"
ARTIFACT_CODEC_NAME = "_schema_artifact_codec.py"

# The epoch-control table is deliberately outside the catalog_/operational_
# namespaces.  It is the only separately admitted schema-control relation.
EPOCH_CONTROL_RELATIONS = frozenset({"h2hdb_schema_epoch"})
_RELATION_IDENTIFIER = r"(?:catalog|operational|h2hdb_schema)_[A-Za-z0-9_]+"
_RELATION_IDENTIFIER_FULL = re.compile(_RELATION_IDENTIFIER, re.IGNORECASE)
_STATIC_RELATION_CALLS = frozenset(
    {
        "_has_key",
        "_insert_or_compare",
        "_require_registry_value",
        "direct",
    }
)
_STATIC_MUTATION_CALLS = {
    "_insert_or_compare": "insert into",
    "direct": "delete from",
}
_SQL_RELATION_REFERENCE = re.compile(
    rf"""
    (?:
        \b(?:from|join|update|references)\s+
      | \b(?:insert|replace)\s+(?:(?:or\s+\w+|ignore)\s+)?into\s+
      | \bdelete\s+from\s+
      | \btruncate\s+(?:table\s+)?
      | \balter\s+table\s+
      | \bdrop\s+(?:table|view)\s+(?:if\s+exists\s+)?
      | \bcreate\s+(?:or\s+replace\s+)?(?:temporary\s+)?
            (?:table|view)\s+(?:if\s+not\s+exists\s+)?
    )
    (?:[`"\[]?[A-Za-z_][A-Za-z0-9_]*[`"\]]?\s*\.\s*)?
    [`"\[]?(?P<relation>{_RELATION_IDENTIFIER})
    (?![A-Za-z0-9_])
    """,
    re.IGNORECASE | re.VERBOSE,
)
_SQL_MUTATION_TARGET = re.compile(
    rf"""
    \b(?P<verb>
        update
      | delete\s+from
      | insert\s+(?:(?:or\s+\w+|ignore)\s+)?into
      | replace\s+(?:(?:or\s+\w+|ignore)\s+)?into
      | truncate\s+(?:table\s+)?
    )\s+
    (?:[`"\[]?[A-Za-z_][A-Za-z0-9_]*[`"\]]?\s*\.\s*)?
    [`"\[]?(?P<relation>{_RELATION_IDENTIFIER})
    (?![A-Za-z0-9_])
    """,
    re.IGNORECASE | re.VERBOSE,
)


class SchemaSurfaceError(RuntimeError):
    """The production distribution names a relation outside its manifests."""


@dataclass(frozen=True, order=True)
class RelationReference:
    source: str
    line: int
    relation: str


@dataclass(frozen=True, order=True)
class MutationReference:
    source: str
    line: int
    relation: str
    verb: str


class _ArtifactDecoder(Protocol):
    def __call__(
        self,
        compressed: bytes,
        *,
        compressed_size: int,
        compressed_sha256: str,
        raw_size: int,
        raw_sha256: str,
    ) -> dict[str, Any]: ...


def allowed_relations(
    manifests: Iterable[Path] = PHYSICAL_MANIFESTS,
) -> frozenset[str]:
    """Return the closed-world physical table/view names from the manifests."""

    names = set(EPOCH_CONTROL_RELATIONS)
    for manifest in manifests:
        document = tomllib.loads(manifest.read_text(encoding="utf-8"))
        for section in ("relation", "external_stub"):
            for relation in document.get(section, ()):
                table = relation.get("table")
                if not isinstance(table, str) or not table:
                    raise SchemaSurfaceError(
                        f"{manifest}: {section} entry has no nonempty table name"
                    )
                names.add(table.casefold())
    return frozenset(names)


def relation_kinds(
    manifests: Iterable[Path] = PHYSICAL_MANIFESTS,
) -> dict[str, str]:
    """Return the closed physical object kind for every owned relation."""

    kinds = {name: "table" for name in EPOCH_CONTROL_RELATIONS}
    for manifest in manifests:
        document = tomllib.loads(manifest.read_text(encoding="utf-8"))
        for relation in document.get("relation", ()):
            table = relation.get("table")
            kind = relation.get("kind", "table")
            if not isinstance(table, str) or not table:
                raise SchemaSurfaceError(
                    f"{manifest}: relation entry has no nonempty table name"
                )
            if kind not in {"table", "view"}:
                raise SchemaSurfaceError(
                    f"{manifest}: relation {table!r} has unknown kind {kind!r}"
                )
            normalized = table.casefold()
            previous = kinds.get(normalized)
            if previous is not None and previous != kind:
                raise SchemaSurfaceError(
                    f"physical relation {table!r} has conflicting object kinds"
                )
            kinds[normalized] = kind
    return kinds


def _references_in_text(
    text: str, *, source: str, starting_line: int = 1
) -> Iterator[RelationReference]:
    for match in _SQL_RELATION_REFERENCE.finditer(text):
        yield RelationReference(
            source=source,
            line=starting_line + text.count("\n", 0, match.start()),
            relation=match.group("relation").casefold(),
        )


def _mutations_in_text(
    text: str, *, source: str, starting_line: int = 1
) -> Iterator[MutationReference]:
    for match in _SQL_MUTATION_TARGET.finditer(text):
        yield MutationReference(
            source=source,
            line=starting_line + text.count("\n", 0, match.start()),
            relation=match.group("relation").casefold(),
            verb=" ".join(match.group("verb").casefold().split()),
        )


def _assignment_name(node: ast.Assign | ast.AnnAssign) -> str | None:
    target: ast.expr
    if isinstance(node, ast.Assign):
        if len(node.targets) != 1:
            return None
        target = node.targets[0]
    else:
        target = node.target
    return target.id if isinstance(target, ast.Name) else None


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _is_static_relation_literal(
    node: ast.Constant,
    *,
    source: str,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    """Recognize table literals used by static dynamic-SQL dispatch.

    Raw ``catalog_*`` strings are not sufficient: digest domains, manifest
    field names, and public Python symbols legitimately share that prefix.
    The admitted contexts below are the places where production code selects
    a SQL relation indirectly instead of spelling it after FROM/INTO/etc.
    """

    if not isinstance(node.value, str) or not _RELATION_IDENTIFIER_FULL.fullmatch(
        node.value
    ):
        return False

    # The generated artifact already carries complete DDL and view SQL, which
    # the SQL-position scanner checks.  Its other exact strings include digest
    # domains and logical relation names, so they are intentionally not table
    # dispatch evidence.
    if PurePosixPath(source).name == "_generated_vnext_schema.py":
        return False
    cleanup_dispatch = PurePosixPath(source).name == "vnext_cleanup_repository.py"

    parent = parents.get(node)
    if isinstance(parent, (ast.Assign, ast.AnnAssign)):
        assignment_name = _assignment_name(parent)
        if (
            assignment_name is not None
            and isinstance(parents.get(parent), ast.Module)
            and "DOMAIN" not in assignment_name
            and assignment_name != "__all__"
        ):
            return True
        if assignment_name in {"root", "table"}:
            return True

    current: ast.AST = node
    while (parent := parents.get(current)) is not None:
        if isinstance(parent, ast.ClassDef):
            return False
        if isinstance(parent, ast.Call):
            call_name = _call_name(parent)
            if (
                cleanup_dispatch
                and call_name is not None
                and call_name.endswith("_spec")
                and len(parent.args) > 1
                and any(descendant is node for descendant in ast.walk(parent.args[1]))
            ):
                # Static cleanup specs pass their primary-key column tuple as
                # positional argument 2.  A column may legitimately begin
                # with ``catalog_`` (for example catalog_occurrence_sha256),
                # but it is not a dynamically selected relation.
                return False
            if call_name in _STATIC_RELATION_CALLS or (
                call_name is not None and call_name.endswith("_spec")
            ):
                return True
            for keyword in parent.keywords:
                if keyword.arg == "table" and any(
                    descendant is node for descendant in ast.walk(keyword.value)
                ):
                    return True
            if not cleanup_dispatch:
                return False
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
            break
        current = parent

    # Cleanup strategies are a closed static dispatch graph.  Their tuple and
    # dict registries feed table names to SQL templates several stack frames
    # later, so no single FROM/INTO string contains the identifier.
    return cleanup_dispatch


def references_in_python(
    source_text: str, *, source: str
) -> tuple[RelationReference, ...]:
    """Extract relation identifiers only from executable Python string values.

    Parsing first is important: generated SQL is commonly represented as
    adjacent source literals, while ordinary Python names such as
    ``catalog_revision`` are not database relations and must not be mistaken
    for one.
    """

    try:
        tree = ast.parse(source_text, filename=source)
    except SyntaxError as error:
        raise SchemaSurfaceError(
            f"cannot parse packaged Python {source}: {error}"
        ) from error

    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    references: set[RelationReference] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        references.update(
            _references_in_text(
                node.value,
                source=source,
                starting_line=getattr(node, "lineno", 1),
            )
        )
        if _is_static_relation_literal(node, source=source, parents=parents):
            references.add(
                RelationReference(
                    source=source,
                    line=getattr(node, "lineno", 1),
                    relation=node.value.casefold(),
                )
            )
    return tuple(sorted(references))


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    constants: dict[str, str] = {}
    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        name = _assignment_name(statement)
        value = statement.value
        if (
            name is not None
            and isinstance(value, ast.Constant)
            and isinstance(value.value, str)
        ):
            constants[name] = value.value
    return constants


def _static_string(node: ast.AST, constants: dict[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left, constants)
        right = _static_string(node.right, constants)
        if left is not None and right is not None:
            return left + right
        return None
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if isinstance(value, ast.FormattedValue):
                rendered = _static_string(value.value, constants)
                if rendered is not None and value.format_spec is None:
                    parts.append(rendered)
                    continue
            return None
        return "".join(parts)
    return None


def _static_string_with_placeholder(
    node: ast.AST, constants: dict[str, str]
) -> str | None:
    """Render a string expression while making unknown f-string slots parseable."""

    rendered = _static_string(node, constants)
    if rendered is not None:
        return rendered
    if not isinstance(node, ast.JoinedStr):
        return None
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        elif isinstance(value, ast.FormattedValue):
            known = _static_string(value.value, constants)
            parts.append("catalog_dynamic_target" if known is None else known)
        else:
            return None
    return "".join(parts)


def mutations_in_python(
    source_text: str, *, source: str
) -> tuple[MutationReference, ...]:
    """Extract statically resolvable SQL mutation targets from Python."""

    try:
        tree = ast.parse(source_text, filename=source)
    except SyntaxError as error:
        raise SchemaSurfaceError(
            f"cannot parse packaged Python {source}: {error}"
        ) from error
    constants = _module_string_constants(tree)
    mutations: set[MutationReference] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Constant, ast.JoinedStr, ast.BinOp)):
            text = _static_string(node, constants)
            if text is not None:
                mutations.update(
                    _mutations_in_text(
                        text,
                        source=source,
                        starting_line=getattr(node, "lineno", 1),
                    )
                )
        if isinstance(node, ast.Call):
            call_name = _call_name(node)
            verb = _STATIC_MUTATION_CALLS.get(call_name or "")
            target = node.args[0] if node.args else None
            if verb is not None and target is not None:
                relation = _static_string(target, constants)
                if relation is not None and _RELATION_IDENTIFIER_FULL.fullmatch(
                    relation
                ):
                    mutations.add(
                        MutationReference(
                            source=source,
                            line=getattr(node, "lineno", 1),
                            relation=relation.casefold(),
                            verb=verb,
                        )
                    )

    # A closed static dispatcher often binds ``table`` from a tuple and then
    # formats ``INSERT INTO {table}`` or ``DELETE FROM {table}``.  Resolve the
    # direct cases above; for an intentionally static but locally-bound target,
    # conservatively associate every relation literal/name in that function
    # with the dynamic DML verb.  The manifest-kind check below only rejects a
    # candidate when that exact physical object is a view.
    for function in (
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ):
        dynamic_verbs: set[str] = set()
        for node in ast.walk(function):
            rendered = _static_string_with_placeholder(node, constants)
            if rendered is not None:
                dynamic_verbs.update(
                    mutation.verb
                    for mutation in _mutations_in_text(rendered, source=source)
                    if mutation.relation == "catalog_dynamic_target"
                )
            if isinstance(node, ast.Call):
                call_name = _call_name(node)
                verb = _STATIC_MUTATION_CALLS.get(call_name or "")
                target = node.args[0] if node.args else None
                if (
                    verb is not None
                    and target is not None
                    and _static_string(target, constants) is None
                ):
                    dynamic_verbs.add(verb)
        if not dynamic_verbs:
            continue
        candidates: set[tuple[int, str]] = set()
        for node in ast.walk(function):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and _RELATION_IDENTIFIER_FULL.fullmatch(node.value)
            ):
                candidates.add((getattr(node, "lineno", function.lineno), node.value))
            elif isinstance(node, ast.Name):
                value = constants.get(node.id)
                if value is not None and _RELATION_IDENTIFIER_FULL.fullmatch(value):
                    candidates.add((getattr(node, "lineno", function.lineno), value))
        for line, relation in candidates:
            for verb in dynamic_verbs:
                mutations.add(
                    MutationReference(
                        source=source,
                        line=line,
                        relation=relation.casefold(),
                        verb=verb,
                    )
                )
    return tuple(sorted(mutations))


def references_in_sql(
    source_text: str, *, source: str
) -> tuple[RelationReference, ...]:
    return tuple(sorted(set(_references_in_text(source_text, source=source))))


def mutations_in_sql(source_text: str, *, source: str) -> tuple[MutationReference, ...]:
    return tuple(sorted(set(_mutations_in_text(source_text, source=source))))


def _references_for_member(
    source_text: str, *, source: str, suffix: str
) -> tuple[RelationReference, ...]:
    if suffix == ".py":
        return references_in_python(source_text, source=source)
    if suffix == ".sql":
        return references_in_sql(source_text, source=source)
    return ()


def _mutations_for_member(
    source_text: str, *, source: str, suffix: str
) -> tuple[MutationReference, ...]:
    if suffix == ".py":
        return mutations_in_python(source_text, source=source)
    if suffix == ".sql":
        return mutations_in_sql(source_text, source=source)
    return ()


_ARTIFACT_LOADER_CONSTANTS = (
    "_RESOURCE_NAME",
    "_COMPRESSED_SIZE",
    "_COMPRESSED_SHA256",
    "_RAW_SIZE",
    "_RAW_SHA256",
)


def _artifact_loader_metadata(
    loader_source: str, *, source: str
) -> tuple[int, str, int, str]:
    try:
        tree = ast.parse(loader_source, filename=source)
    except SyntaxError as error:
        raise SchemaSurfaceError(
            f"cannot parse generated artifact loader {source}: {error}"
        ) from error
    values: dict[str, object] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if (
            not isinstance(target, ast.Name)
            or target.id not in _ARTIFACT_LOADER_CONSTANTS
        ):
            continue
        try:
            values[target.id] = ast.literal_eval(statement.value)
        except (TypeError, ValueError) as error:
            raise SchemaSurfaceError(
                f"generated artifact loader metadata is not literal in {source}"
            ) from error
    if set(values) != set(_ARTIFACT_LOADER_CONSTANTS):
        raise SchemaSurfaceError(
            f"generated artifact loader metadata is incomplete in {source}"
        )
    if values["_RESOURCE_NAME"] != GENERATED_RESOURCE_NAME:
        raise SchemaSurfaceError(
            f"generated artifact loader names an unexpected resource in {source}"
        )
    compressed_size = values["_COMPRESSED_SIZE"]
    compressed_sha256 = values["_COMPRESSED_SHA256"]
    raw_size = values["_RAW_SIZE"]
    raw_sha256 = values["_RAW_SHA256"]
    if (
        type(compressed_size) is not int
        or type(compressed_sha256) is not str
        or type(raw_size) is not int
        or type(raw_sha256) is not str
    ):
        raise SchemaSurfaceError(
            f"generated artifact loader metadata has invalid types in {source}"
        )
    return compressed_size, compressed_sha256, raw_size, raw_sha256


def _artifact_decoder(codec_source: str, *, source: str) -> _ArtifactDecoder:
    namespace: dict[str, Any] = {
        "__file__": source,
        "__name__": "_h2hdb_schema_surface_artifact_codec",
    }
    try:
        exec(compile(codec_source, source, "exec"), namespace)
        decoder = namespace["decode_schema_artifact"]
    except Exception as error:
        raise SchemaSurfaceError(
            f"cannot load packaged schema artifact decoder {source}: {error}"
        ) from error
    if not callable(decoder):
        raise SchemaSurfaceError(
            f"packaged schema artifact decoder is not callable in {source}"
        )
    return cast(_ArtifactDecoder, decoder)


def _decode_artifact(
    *,
    loader_source: str,
    codec_source: str,
    compressed: bytes,
    source: str,
) -> dict[str, Any]:
    compressed_size, compressed_sha256, raw_size, raw_sha256 = (
        _artifact_loader_metadata(loader_source, source=source)
    )
    decoder = _artifact_decoder(codec_source, source=source)
    try:
        return decoder(
            compressed,
            compressed_size=compressed_size,
            compressed_sha256=compressed_sha256,
            raw_size=raw_size,
            raw_sha256=raw_sha256,
        )
    except Exception as error:
        raise SchemaSurfaceError(
            f"cannot decode generated schema artifact {source}: {error}"
        ) from error


def _artifact_strings(value: object) -> Iterator[str]:
    pending = [value]
    while pending:
        current = pending.pop()
        if type(current) is str:
            yield current
        elif type(current) is dict:
            mapping = cast(dict[object, object], current)
            pending.extend(reversed(tuple(mapping.values())))
            pending.extend(reversed(tuple(mapping.keys())))
        elif type(current) in {list, tuple}:
            sequence = cast(list[object] | tuple[object, ...], current)
            pending.extend(reversed(sequence))


def references_in_artifact(
    artifact: dict[str, Any], *, source: str
) -> tuple[RelationReference, ...]:
    references: set[RelationReference] = set()
    for value in _artifact_strings(artifact):
        references.update(_references_in_text(value, source=source))
    return tuple(sorted(references))


def mutations_in_artifact(
    artifact: dict[str, Any], *, source: str
) -> tuple[MutationReference, ...]:
    mutations: set[MutationReference] = set()
    for value in _artifact_strings(artifact):
        mutations.update(_mutations_in_text(value, source=source))
    return tuple(sorted(mutations))


def _source_artifact(package_root: Path) -> dict[str, Any] | None:
    loader = package_root / GENERATED_LOADER_NAME
    resource = package_root / GENERATED_RESOURCE_NAME
    codec = package_root / ARTIFACT_CODEC_NAME
    present = tuple(path.is_file() for path in (loader, resource, codec))
    if not any(present):
        return None
    if not all(present):
        raise SchemaSurfaceError(
            "source package contains an incomplete generated schema artifact boundary"
        )
    return _decode_artifact(
        loader_source=loader.read_text(encoding="utf-8"),
        codec_source=codec.read_text(encoding="utf-8"),
        compressed=resource.read_bytes(),
        source=resource.as_posix(),
    )


def _source_label(path: Path) -> str:
    try:
        return path.relative_to(REPOSITORY_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def source_references(
    package_root: Path = PACKAGE_ROOT,
) -> tuple[RelationReference, ...]:
    references: set[RelationReference] = set()
    for path in sorted(package_root.rglob("*")):
        if path.suffix not in {".py", ".sql"} or not path.is_file():
            continue
        references.update(
            _references_for_member(
                path.read_text(encoding="utf-8"),
                source=_source_label(path),
                suffix=path.suffix,
            )
        )
    artifact = _source_artifact(package_root)
    if artifact is not None:
        references.update(
            references_in_artifact(
                artifact,
                source=(package_root / GENERATED_RESOURCE_NAME).as_posix(),
            )
        )
    return tuple(sorted(references))


def source_mutations(
    package_root: Path = PACKAGE_ROOT,
) -> tuple[MutationReference, ...]:
    mutations: set[MutationReference] = set()
    for path in sorted(package_root.rglob("*")):
        if path.suffix not in {".py", ".sql"} or not path.is_file():
            continue
        mutations.update(
            _mutations_for_member(
                path.read_text(encoding="utf-8"),
                source=_source_label(path),
                suffix=path.suffix,
            )
        )
    artifact = _source_artifact(package_root)
    if artifact is not None:
        mutations.update(
            mutations_in_artifact(
                artifact,
                source=(package_root / GENERATED_RESOURCE_NAME).as_posix(),
            )
        )
    return tuple(sorted(mutations))


def _wheel_artifact(archive: zipfile.ZipFile) -> dict[str, Any] | None:
    loader_name = f"h2hdb/{GENERATED_LOADER_NAME}"
    resource_name = f"h2hdb/{GENERATED_RESOURCE_NAME}"
    codec_name = f"h2hdb/{ARTIFACT_CODEC_NAME}"
    members = set(archive.namelist())
    present = tuple(
        name in members for name in (loader_name, resource_name, codec_name)
    )
    if not any(present):
        return None
    if not all(present):
        raise SchemaSurfaceError(
            "wheel contains an incomplete generated schema artifact boundary"
        )
    try:
        loader_source = archive.read(loader_name).decode("utf-8")
        codec_source = archive.read(codec_name).decode("utf-8")
    except UnicodeDecodeError as error:
        raise SchemaSurfaceError(
            "wheel schema artifact loader or codec is not UTF-8"
        ) from error
    return _decode_artifact(
        loader_source=loader_source,
        codec_source=codec_source,
        compressed=archive.read(resource_name),
        source=resource_name,
    )


def wheel_references(wheel: Path) -> tuple[RelationReference, ...]:
    references: set[RelationReference] = set()
    with zipfile.ZipFile(wheel) as archive:
        for member_name in sorted(archive.namelist()):
            member = PurePosixPath(member_name)
            if (
                not member.parts
                or member.parts[0] != "h2hdb"
                or member.suffix not in {".py", ".sql"}
            ):
                continue
            try:
                source_text = archive.read(member_name).decode("utf-8")
            except UnicodeDecodeError as error:
                raise SchemaSurfaceError(
                    f"packaged production source is not UTF-8: {member_name}"
                ) from error
            references.update(
                _references_for_member(
                    source_text,
                    source=member_name,
                    suffix=member.suffix,
                )
            )
        artifact = _wheel_artifact(archive)
        if artifact is not None:
            references.update(
                references_in_artifact(
                    artifact,
                    source=f"h2hdb/{GENERATED_RESOURCE_NAME}",
                )
            )
    return tuple(sorted(references))


def wheel_mutations(wheel: Path) -> tuple[MutationReference, ...]:
    mutations: set[MutationReference] = set()
    with zipfile.ZipFile(wheel) as archive:
        for member_name in sorted(archive.namelist()):
            member = PurePosixPath(member_name)
            if (
                not member.parts
                or member.parts[0] != "h2hdb"
                or member.suffix not in {".py", ".sql"}
            ):
                continue
            try:
                source_text = archive.read(member_name).decode("utf-8")
            except UnicodeDecodeError as error:
                raise SchemaSurfaceError(
                    f"packaged production source is not UTF-8: {member_name}"
                ) from error
            mutations.update(
                _mutations_for_member(
                    source_text,
                    source=member_name,
                    suffix=member.suffix,
                )
            )
        artifact = _wheel_artifact(archive)
        if artifact is not None:
            mutations.update(
                mutations_in_artifact(
                    artifact,
                    source=f"h2hdb/{GENERATED_RESOURCE_NAME}",
                )
            )
    return tuple(sorted(mutations))


def unexpected_relations(
    references: Iterable[RelationReference],
    *,
    allowed: frozenset[str] | None = None,
) -> tuple[RelationReference, ...]:
    admitted = allowed_relations() if allowed is None else allowed
    return tuple(
        sorted(
            reference for reference in references if reference.relation not in admitted
        )
    )


def assert_closed_schema_surface(
    references: Iterable[RelationReference],
    *,
    allowed: frozenset[str] | None = None,
) -> None:
    unexpected = unexpected_relations(references, allowed=allowed)
    if not unexpected:
        return
    rendered = "\n".join(
        f"  {reference.source}:{reference.line}: {reference.relation}"
        for reference in unexpected
    )
    raise SchemaSurfaceError(
        "production SQL references relations outside the physical manifests:\n"
        + rendered
    )


def assert_no_view_mutations(
    mutations: Iterable[MutationReference],
    *,
    kinds: dict[str, str] | None = None,
) -> None:
    object_kinds = relation_kinds() if kinds is None else kinds
    invalid = tuple(
        sorted(
            mutation
            for mutation in mutations
            if object_kinds.get(mutation.relation) == "view"
        )
    )
    if not invalid:
        return
    rendered = "\n".join(
        f"  {mutation.source}:{mutation.line}: {mutation.verb} {mutation.relation}"
        for mutation in invalid
    )
    raise SchemaSurfaceError(
        "production SQL attempts to mutate read-only manifest views:\n" + rendered
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify the closed-world production SQL relation surface."
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        help="scan an exact wheel instead of the source package",
    )
    args = parser.parse_args()

    try:
        if args.wheel is not None:
            wheel = args.wheel.resolve()
            references = wheel_references(wheel)
            mutations = wheel_mutations(wheel)
        else:
            references = source_references()
            mutations = source_mutations()
        assert_closed_schema_surface(references)
        assert_no_view_mutations(mutations)
    except (OSError, SchemaSurfaceError, tomllib.TOMLDecodeError) as error:
        print(f"schema surface verification failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    location = str(args.wheel) if args.wheel is not None else str(PACKAGE_ROOT)
    print(
        f"Verified {len(references)} production SQL relation references and "
        f"{len(mutations)} mutation targets in {location}."
    )


if __name__ == "__main__":
    main()
