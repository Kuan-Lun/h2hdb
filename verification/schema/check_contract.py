"""Validate the logical vNext catalog schema contract.

The checker intentionally reasons about declared semantic functional
dependencies, not SQL primary-key syntax.  Relations in this contract are
small enough that exhaustive subset enumeration is preferable to a heuristic:
candidate keys and BCNF are checked against F+ for every attribute subset.
"""

from __future__ import annotations

import argparse
import itertools
import sys
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FunctionalDependency:
    determinant: frozenset[str]
    dependent: frozenset[str]


@dataclass(frozen=True)
class ForeignKey:
    attributes: tuple[str, ...]
    relation: str
    referenced_attributes: tuple[str, ...]


@dataclass(frozen=True)
class Relation:
    name: str
    kind: str
    attributes: tuple[str, ...]
    functional_dependencies: tuple[FunctionalDependency, ...]
    declared_keys: tuple[frozenset[str], ...]
    foreign_keys: tuple[ForeignKey, ...]
    materialization: Mapping[str, Any] | None


@dataclass(frozen=True)
class Projection:
    relation: str
    attributes: frozenset[str]


@dataclass(frozen=True)
class Decomposition:
    name: str
    universal_attributes: frozenset[str]
    functional_dependencies: tuple[FunctionalDependency, ...]
    projections: tuple[Projection, ...]
    rationale: str


@dataclass(frozen=True)
class AttributeSemantic:
    name: str
    classification: str
    rationale: str


@dataclass(frozen=True)
class Contract:
    contract_version: int
    name: str
    relations: tuple[Relation, ...]
    decompositions: tuple[Decomposition, ...]
    attribute_semantics: tuple[AttributeSemantic, ...] = ()


@dataclass(frozen=True)
class RelationReport:
    name: str
    candidate_keys: tuple[frozenset[str], ...]
    checked_determinants: int


@dataclass(frozen=True)
class ValidationReport:
    relations: tuple[RelationReport, ...]
    lossless_decompositions: tuple[str, ...]
    dependency_preserving_decompositions: tuple[str, ...]


class ContractError(ValueError):
    """Base error for malformed or invalid schema contracts."""


class ContractFormatError(ContractError):
    """The TOML document does not have the required shape."""


class ContractValidationError(ContractError):
    """The contract is well-shaped TOML but violates schema invariants."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("schema contract validation failed:\n- " + "\n- ".join(errors))


def attribute_closure(
    seed: Iterable[str], functional_dependencies: Iterable[FunctionalDependency]
) -> frozenset[str]:
    """Return seed+ under the supplied functional dependencies."""

    closure = set(seed)
    dependencies = tuple(functional_dependencies)
    changed = True
    while changed:
        changed = False
        for dependency in dependencies:
            if dependency.determinant <= closure:
                before = len(closure)
                closure.update(dependency.dependent)
                changed = changed or len(closure) != before
    return frozenset(closure)


def enumerate_candidate_keys(
    attributes: Iterable[str],
    functional_dependencies: Iterable[FunctionalDependency],
) -> tuple[frozenset[str], ...]:
    """Exhaustively enumerate all inclusion-minimal superkeys."""

    ordered_attributes = tuple(sorted(set(attributes)))
    all_attributes = frozenset(ordered_attributes)
    dependencies = tuple(functional_dependencies)
    keys: list[frozenset[str]] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            candidate = frozenset(values)
            if any(key <= candidate for key in keys):
                continue
            if attribute_closure(candidate, dependencies) == all_attributes:
                keys.append(candidate)
    return tuple(keys)


def bcnf_violations(
    relation: Relation,
) -> tuple[tuple[frozenset[str], frozenset[str]], ...]:
    """Find all F+-implied BCNF violations by determinant enumeration."""

    attributes = frozenset(relation.attributes)
    ordered_attributes = tuple(sorted(attributes))
    violations: list[tuple[frozenset[str], frozenset[str]]] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            determinant = frozenset(values)
            closure = (
                attribute_closure(determinant, relation.functional_dependencies)
                & attributes
            )
            nontrivial = closure - determinant
            if nontrivial and closure != attributes:
                violations.append((determinant, nontrivial))
    return tuple(violations)


def is_binary_lossless(decomposition: Decomposition) -> bool:
    """Decide FD losslessness for a two-way decomposition.

    R -> R1,R2 is lossless under F exactly when the intersection functionally
    determines all of R1 or all of R2.
    """

    if len(decomposition.projections) != 2:
        raise ValueError("binary lossless checking requires exactly two projections")
    left, right = decomposition.projections
    intersection = left.attributes & right.attributes
    closure = attribute_closure(intersection, decomposition.functional_dependencies)
    return left.attributes <= closure or right.attributes <= closure


def project_functional_dependencies(
    attributes: Iterable[str],
    functional_dependencies: Iterable[FunctionalDependency],
) -> tuple[FunctionalDependency, ...]:
    """Return the exact F+ projection onto one relation schema.

    For every X ⊆ Ri, the projected set contains X → (X+ under F) ∩ Ri.
    Keeping the trivial attributes in the dependent makes the construction
    mirror the closure definition directly and does not change implication.
    """

    ordered_attributes = tuple(sorted(set(attributes)))
    projection = frozenset(ordered_attributes)
    dependencies = tuple(functional_dependencies)
    projected: list[FunctionalDependency] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            determinant = frozenset(values)
            dependent = attribute_closure(determinant, dependencies) & projection
            projected.append(FunctionalDependency(determinant, dependent))
    return tuple(projected)


def is_dependency_preserving(decomposition: Decomposition) -> bool:
    """Decide whether the union of all F+ projections implies every FD in F."""

    projected_dependencies = tuple(
        dependency
        for projection in decomposition.projections
        for dependency in project_functional_dependencies(
            projection.attributes, decomposition.functional_dependencies
        )
    )
    return all(
        dependency.dependent
        <= attribute_closure(dependency.determinant, projected_dependencies)
        for dependency in decomposition.functional_dependencies
    )


def load_contract(path: str | Path) -> Contract:
    """Load a contract from TOML and validate its document shape."""

    contract_path = Path(path)
    try:
        with contract_path.open("rb") as stream:
            document = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ContractFormatError(f"cannot read {contract_path}: {error}") from error

    try:
        version = _integer(document, "contract_version", "contract")
        name = _string(document, "name", "contract")
        raw_relations = _table_list(document, "relation", "contract")
        raw_decompositions = document.get("decomposition", [])
        if not isinstance(raw_decompositions, list) or not all(
            isinstance(value, dict) for value in raw_decompositions
        ):
            raise ContractFormatError(
                "contract.decomposition must be an array of tables"
            )
        relations = tuple(_parse_relation(value) for value in raw_relations)
        decompositions = tuple(
            _parse_decomposition(value) for value in raw_decompositions
        )
        semantics = tuple(
            _parse_attribute_semantic(value)
            for value in _optional_table_list(
                document, "attribute_semantic", "contract"
            )
        )
    except ContractFormatError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ContractFormatError(f"invalid contract document: {error}") from error
    return Contract(version, name, relations, decompositions, semantics)


def validate_contract(contract: Contract) -> ValidationReport:
    """Validate keys, F+ BCNF, and decomposition semantic properties."""

    errors: list[str] = []
    relation_by_name: dict[str, Relation] = {}
    reports: list[RelationReport] = []

    if contract.contract_version != 1:
        errors.append(f"contract_version must be 1, got {contract.contract_version!r}")
    if not contract.relations:
        errors.append("contract must declare at least one relation")

    for relation in contract.relations:
        if relation.name in relation_by_name:
            errors.append(f"duplicate relation name {relation.name!r}")
        else:
            relation_by_name[relation.name] = relation

    for relation in contract.relations:
        relation_errors, report = _validate_relation(relation)
        errors.extend(relation_errors)
        reports.append(report)

    for relation in contract.relations:
        errors.extend(_validate_foreign_keys(relation, relation_by_name))
        errors.extend(_validate_materialization(relation, relation_by_name))

    errors.extend(_validate_attribute_semantics(contract, relation_by_name))

    decomposition_names: set[str] = set()
    lossless: list[str] = []
    dependency_preserving: list[str] = []
    for decomposition in contract.decompositions:
        if decomposition.name in decomposition_names:
            errors.append(f"duplicate decomposition name {decomposition.name!r}")
        decomposition_names.add(decomposition.name)
        decomposition_errors = _validate_decomposition(decomposition, relation_by_name)
        errors.extend(decomposition_errors)
        if not decomposition_errors and is_binary_lossless(decomposition):
            lossless.append(decomposition.name)
        if not decomposition_errors and is_dependency_preserving(decomposition):
            dependency_preserving.append(decomposition.name)

    if errors:
        raise ContractValidationError(errors)
    return ValidationReport(
        tuple(reports), tuple(lossless), tuple(dependency_preserving)
    )


_SEMANTIC_CLASSIFICATIONS = frozenset(
    {
        "aggregate_count",
        "audit_digest",
        "canonical_identity_digest",
        "comparator_key",
        "domain_identifier",
        "idempotency_key",
        "locator",
        "natural_key",
        "observational_digest",
        "ordering_key",
        "payload_digest",
        "payload_reference_digest",
        "surrogate_identifier",
    }
)


def _requires_semantic_classification(attribute: str) -> bool:
    return (
        attribute.endswith(("_sha256", "_key", "_id", "_count"))
        or attribute == "locator"
        or attribute.endswith("_locator")
    )


def _validate_attribute_semantics(
    contract: Contract, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    """Require an auditable semantic decision for FD-sensitive attributes.

    Suffixes alone cannot tell us whether a digest is injective, whether an ID
    is global, or whether a count is authoritative.  The registry makes those
    closed-world assumptions explicit.  Identity/payload digests additionally
    need a singleton candidate-key relation, preventing an occurrence table
    from silently being treated as the digest's payload authority.
    """

    errors: list[str] = []
    used_attributes = {
        attribute
        for relation in contract.relations
        for attribute in relation.attributes
    }
    required = {
        attribute
        for attribute in used_attributes
        if _requires_semantic_classification(attribute)
    }
    semantic_by_name: dict[str, AttributeSemantic] = {}
    for semantic in contract.attribute_semantics:
        prefix = f"attribute semantic {semantic.name!r}"
        if semantic.name in semantic_by_name:
            errors.append(f"duplicate attribute semantic {semantic.name!r}")
            continue
        semantic_by_name[semantic.name] = semantic
        if semantic.name not in used_attributes:
            errors.append(f"{prefix} is not used by any relation")
        if semantic.classification not in _SEMANTIC_CLASSIFICATIONS:
            errors.append(
                f"{prefix} has unsupported classification "
                f"{semantic.classification!r}"
            )
        if not semantic.rationale.strip():
            errors.append(f"{prefix} must have a non-empty rationale")

    missing = required - set(semantic_by_name)
    if missing:
        errors.append(
            "attribute semantic registry does not cover " + _format_set(missing)
        )

    singleton_keys = {
        next(iter(key))
        for relation in relation_by_name.values()
        for key in enumerate_candidate_keys(
            relation.attributes, relation.functional_dependencies
        )
        if len(key) == 1
    }
    for semantic in contract.attribute_semantics:
        if (
            semantic.classification
            in {
                "canonical_identity_digest",
                "payload_digest",
            }
            and semantic.name not in singleton_keys
        ):
            errors.append(
                f"attribute semantic {semantic.name!r} classifies an identity "
                "digest but no relation declares it as a singleton candidate key"
            )
    return errors


def _validate_relation(relation: Relation) -> tuple[list[str], RelationReport]:
    errors: list[str] = []
    attributes = frozenset(relation.attributes)
    prefix = f"relation {relation.name!r}"
    if not relation.name:
        errors.append("relation name must not be empty")
    if relation.kind not in {"source_of_truth", "controlled_materialization"}:
        errors.append(f"{prefix} has unsupported kind {relation.kind!r}")
    if not relation.attributes:
        errors.append(f"{prefix} must declare attributes")
    if len(attributes) != len(relation.attributes):
        errors.append(f"{prefix} contains duplicate attributes")

    for dependency in relation.functional_dependencies:
        unknown = (dependency.determinant | dependency.dependent) - attributes
        if unknown:
            errors.append(
                f"{prefix} FD mentions unknown attributes {_format_set(unknown)}"
            )
        if not dependency.dependent:
            errors.append(f"{prefix} contains an FD with an empty dependent")

    computed_keys = enumerate_candidate_keys(
        relation.attributes, relation.functional_dependencies
    )
    computed_key_set = set(computed_keys)
    declared_key_set = set(relation.declared_keys)
    if len(declared_key_set) != len(relation.declared_keys):
        errors.append(f"{prefix} declares a duplicate candidate key")
    for key in relation.declared_keys:
        unknown = key - attributes
        if unknown:
            errors.append(
                f"{prefix} key {_format_set(key)} mentions unknown attributes "
                f"{_format_set(unknown)}"
            )
        closure = attribute_closure(key, relation.functional_dependencies) & attributes
        if closure != attributes:
            errors.append(f"{prefix} declared key {_format_set(key)} is not a superkey")
        elif any(
            attribute_closure(key - {attribute}, relation.functional_dependencies)
            & attributes
            == attributes
            for attribute in key
        ):
            errors.append(f"{prefix} declared key {_format_set(key)} is not minimal")
    missing = computed_key_set - declared_key_set
    extra = declared_key_set - computed_key_set
    if missing:
        errors.append(
            f"{prefix} omits candidate keys "
            + ", ".join(_format_set(key) for key in _sorted_sets(missing))
        )
    if extra:
        errors.append(
            f"{prefix} declares non-candidate keys "
            + ", ".join(_format_set(key) for key in _sorted_sets(extra))
        )

    violations = bcnf_violations(relation)
    if violations:
        determinant, dependent = min(
            violations,
            key=lambda item: (len(item[0]), tuple(sorted(item[0]))),
        )
        errors.append(
            f"{prefix} is not BCNF under F+: {_format_set(determinant)} "
            f"determines {_format_set(dependent)} but is not a superkey"
        )

    return errors, RelationReport(
        relation.name,
        tuple(_sorted_sets(computed_keys)),
        1 << len(attributes),
    )


def _validate_foreign_keys(
    relation: Relation, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    errors: list[str] = []
    local_attributes = frozenset(relation.attributes)
    for index, foreign_key in enumerate(relation.foreign_keys, 1):
        prefix = f"relation {relation.name!r} foreign key {index}"
        if not foreign_key.attributes:
            errors.append(f"{prefix} must contain at least one attribute")
        if len(set(foreign_key.attributes)) != len(foreign_key.attributes):
            errors.append(f"{prefix} contains duplicate local attributes")
        unknown_local = set(foreign_key.attributes) - local_attributes
        if unknown_local:
            errors.append(
                f"{prefix} mentions unknown local attributes "
                f"{_format_set(unknown_local)}"
            )
        target = relation_by_name.get(foreign_key.relation)
        if target is None:
            errors.append(
                f"{prefix} references unknown relation {foreign_key.relation!r}"
            )
            continue
        if len(foreign_key.attributes) != len(foreign_key.referenced_attributes):
            errors.append(f"{prefix} local and referenced arity differ")
            continue
        if len(set(foreign_key.referenced_attributes)) != len(
            foreign_key.referenced_attributes
        ):
            errors.append(f"{prefix} contains duplicate referenced attributes")
        unknown_target = set(foreign_key.referenced_attributes) - set(target.attributes)
        if unknown_target:
            errors.append(
                f"{prefix} mentions unknown referenced attributes "
                f"{_format_set(unknown_target)}"
            )
            continue
        target_keys = set(
            enumerate_candidate_keys(target.attributes, target.functional_dependencies)
        )
        referenced = frozenset(foreign_key.referenced_attributes)
        if referenced not in target_keys:
            errors.append(
                f"{prefix} target {_format_set(referenced)} is not a candidate key "
                f"of {target.name!r}"
            )
    return errors


def _validate_materialization(
    relation: Relation, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    errors: list[str] = []
    metadata = relation.materialization
    prefix = f"relation {relation.name!r}"
    if relation.kind == "source_of_truth":
        if metadata is not None:
            errors.append(
                f"{prefix} is source_of_truth but has materialization metadata"
            )
        return errors
    if relation.kind != "controlled_materialization":
        return errors
    if metadata is None:
        return [f"{prefix} controlled materialization lacks rationale metadata"]
    if metadata.get("authoritative") is not False:
        errors.append(f"{prefix} materialization must set authoritative = false")
    for field in ("rationale", "refresh_strategy"):
        if not isinstance(metadata.get(field), str) or not metadata[field].strip():
            errors.append(f"{prefix} materialization.{field} must be non-empty")
    derived_from = metadata.get("derived_from")
    if (
        not isinstance(derived_from, list)
        or not derived_from
        or not all(isinstance(value, str) and value for value in derived_from)
    ):
        errors.append(
            f"{prefix} materialization.derived_from must be a non-empty string array"
        )
    else:
        unknown = set(derived_from) - set(relation_by_name)
        if unknown:
            errors.append(
                f"{prefix} materialization derives from unknown relations "
                f"{_format_set(unknown)}"
            )
    return errors


def _validate_decomposition(
    decomposition: Decomposition, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    errors: list[str] = []
    prefix = f"decomposition {decomposition.name!r}"
    if not decomposition.universal_attributes:
        errors.append(f"{prefix} must declare universal_attributes")
    if len(decomposition.projections) != 2:
        errors.append(
            f"{prefix} has {len(decomposition.projections)} projections; "
            "only explicit binary FD losslessness is supported"
        )
        return errors
    projected_union: set[str] = set()
    for projection in decomposition.projections:
        projected_union.update(projection.attributes)
        if not projection.attributes:
            errors.append(f"{prefix} contains an empty projection")
        unknown = projection.attributes - decomposition.universal_attributes
        if unknown:
            errors.append(
                f"{prefix} projection {projection.relation!r} contains attributes "
                f"outside the universal relation: {_format_set(unknown)}"
            )
        relation = relation_by_name.get(projection.relation)
        if relation is None:
            errors.append(
                f"{prefix} references unknown projection relation "
                f"{projection.relation!r}"
            )
        elif projection.attributes != frozenset(relation.attributes):
            errors.append(
                f"{prefix} projection for {projection.relation!r} must exactly "
                "match that relation's attributes"
            )
    if frozenset(projected_union) != decomposition.universal_attributes:
        errors.append(f"{prefix} projections do not cover the universal relation")
    for dependency in decomposition.functional_dependencies:
        unknown = (
            dependency.determinant | dependency.dependent
        ) - decomposition.universal_attributes
        if unknown:
            errors.append(
                f"{prefix} FD mentions attributes outside the universal relation: "
                f"{_format_set(unknown)}"
            )
    if not decomposition.rationale.strip():
        errors.append(f"{prefix} must explain its conceptual universal relation")
    if not errors:
        lossless = is_binary_lossless(decomposition)
        dependency_preserving = is_dependency_preserving(decomposition)
        if not lossless:
            left, right = decomposition.projections
            errors.append(
                f"{prefix} is lossy under its declared FDs: intersection "
                f"{_format_set(left.attributes & right.attributes)} determines "
                "neither projection"
            )
        if not dependency_preserving:
            errors.append(
                f"{prefix} is not dependency-preserving under its declared FDs: "
                "the union of the F+ projections does not imply every original FD"
            )
    return errors


def _parse_relation(value: Mapping[str, Any]) -> Relation:
    context = f"relation {value.get('name', '<unnamed>')!r}"
    return Relation(
        name=_string(value, "name", context),
        kind=_string(value, "kind", context),
        attributes=_string_tuple(value, "attributes", context),
        functional_dependencies=tuple(
            _parse_fd(item, f"{context}.fds")
            for item in _table_list(value, "fds", context)
        ),
        declared_keys=tuple(
            frozenset(_string_sequence(item, f"{context}.declared_keys"))
            for item in _list(value, "declared_keys", context)
        ),
        foreign_keys=tuple(
            _parse_foreign_key(item, f"{context}.foreign_keys")
            for item in _optional_table_list(value, "foreign_keys", context)
        ),
        materialization=_optional_table(value, "materialization", context),
    )


def _parse_decomposition(value: Mapping[str, Any]) -> Decomposition:
    context = f"decomposition {value.get('name', '<unnamed>')!r}"
    return Decomposition(
        name=_string(value, "name", context),
        universal_attributes=frozenset(
            _string_tuple(value, "universal_attributes", context)
        ),
        functional_dependencies=tuple(
            _parse_fd(item, f"{context}.fds")
            for item in _table_list(value, "fds", context)
        ),
        projections=tuple(
            Projection(
                _string(item, "relation", f"{context}.projections"),
                frozenset(_string_tuple(item, "attributes", f"{context}.projections")),
            )
            for item in _table_list(value, "projections", context)
        ),
        rationale=_string(value, "rationale", context),
    )


def _parse_attribute_semantic(value: Mapping[str, Any]) -> AttributeSemantic:
    context = f"attribute semantic {value.get('name', '<unnamed>')!r}"
    return AttributeSemantic(
        name=_string(value, "name", context),
        classification=_string(value, "classification", context),
        rationale=_string(value, "rationale", context),
    )


def _parse_fd(value: Mapping[str, Any], context: str) -> FunctionalDependency:
    return FunctionalDependency(
        frozenset(_string_tuple(value, "determinant", context)),
        frozenset(_string_tuple(value, "dependent", context)),
    )


def _parse_foreign_key(value: Mapping[str, Any], context: str) -> ForeignKey:
    return ForeignKey(
        _string_tuple(value, "attributes", context),
        _string(value, "relation", context),
        _string_tuple(value, "referenced_attributes", context),
    )


def _list(value: Mapping[str, Any], key: str, context: str) -> list[Any]:
    result = value.get(key)
    if not isinstance(result, list):
        raise ContractFormatError(f"{context}.{key} must be an array")
    return result


def _table_list(
    value: Mapping[str, Any], key: str, context: str
) -> list[Mapping[str, Any]]:
    result = _list(value, key, context)
    if not all(isinstance(item, dict) for item in result):
        raise ContractFormatError(f"{context}.{key} must contain tables")
    return result


def _optional_table_list(
    value: Mapping[str, Any], key: str, context: str
) -> list[Mapping[str, Any]]:
    if key not in value:
        return []
    return _table_list(value, key, context)


def _optional_table(
    value: Mapping[str, Any], key: str, context: str
) -> Mapping[str, Any] | None:
    result = value.get(key)
    if result is None:
        return None
    if not isinstance(result, dict):
        raise ContractFormatError(f"{context}.{key} must be a table")
    return result


def _string(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ContractFormatError(f"{context}.{key} must be a non-empty string")
    return result


def _integer(value: Mapping[str, Any], key: str, context: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise ContractFormatError(f"{context}.{key} must be an integer")
    return result


def _string_tuple(value: Mapping[str, Any], key: str, context: str) -> tuple[str, ...]:
    return tuple(_string_sequence(_list(value, key, context), f"{context}.{key}"))


def _string_sequence(value: Any, context: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ContractFormatError(f"{context} must be an array of non-empty strings")
    return tuple(value)


def _format_set(values: Iterable[str]) -> str:
    return "{" + ", ".join(sorted(values)) + "}"


def _sorted_sets(values: Iterable[frozenset[str]]) -> list[frozenset[str]]:
    return sorted(values, key=lambda value: (len(value), tuple(sorted(value))))


def _default_contract_path() -> Path:
    return Path(__file__).with_name("catalog.toml")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "contract",
        nargs="?",
        type=Path,
        default=_default_contract_path(),
        help="TOML contract (default: catalog.toml beside this checker)",
    )
    arguments = parser.parse_args(argv)
    try:
        contract = load_contract(arguments.contract)
        report = validate_contract(contract)
    except ContractError as error:
        print(error, file=sys.stderr)
        return 1
    print(
        f"validated {len(report.relations)} BCNF relations and "
        f"{len(report.lossless_decompositions)} lossless decompositions "
        f"and {len(report.dependency_preserving_decompositions)} "
        "dependency-preserving decompositions "
        f"from {arguments.contract}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
