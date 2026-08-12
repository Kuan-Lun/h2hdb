from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "verification" / "schema" / "catalog.toml"
CHECKER = ROOT / "verification" / "schema" / "check_contract.py"


def _load_checker() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_schema_contract_checker", CHECKER
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


checker = _load_checker()


def _fd(determinant: set[str], dependent: set[str]):  # type: ignore[no-untyped-def]
    return checker.FunctionalDependency(frozenset(determinant), frozenset(dependent))


def _relation(
    name: str,
    attributes: tuple[str, ...],
    functional_dependencies: tuple[object, ...],
    declared_keys: tuple[frozenset[str], ...],
    *,
    foreign_keys: tuple[object, ...] = (),
) -> object:
    return checker.Relation(
        name=name,
        kind="source_of_truth",
        attributes=attributes,
        functional_dependencies=functional_dependencies,
        declared_keys=declared_keys,
        foreign_keys=foreign_keys,
        materialization=None,
    )


def test_catalog_contract_is_valid_and_covers_vnext_workflows() -> None:
    contract = checker.load_contract(CATALOG)

    report = checker.validate_contract(contract)

    relation_by_name = {relation.name: relation for relation in contract.relations}
    required = {
        "gallery_identity",
        "content_blob",
        "artifact_blob",
        "file_name_identity",
        "source_file",
        "analysis_gallery_file_hash_occurrence",
        "analysis_file_hash_artist_contribution",
        "analysis_file_hash_artist_stat",
        "excluded_file_hash_evidence",
        "excluded_file_hash",
        "analysis_content_owner_candidate",
        "analysis_content_owner",
        "analysis_gid_candidate",
        "analysis_gid_winner",
        "analysis_checkpoint",
        "analysis_batch_receipt",
        "publication_candidate",
        "publication_selection",
        "publication_checkpoint",
        "publication_batch_receipt",
        "prepared_artifact",
        "catalog_revision",
        "catalog_publication",
        "publication_receipt",
        "publication_head",
    }
    assert required <= relation_by_name.keys()
    assert {item.name for item in report.relations} == relation_by_name.keys()
    assert set(report.lossless_decompositions) == {
        "gallery_identity_and_source_occurrence",
        "file_identity_and_source_occurrence",
        "file_content_payload_and_source_occurrence",
        "tag_identity_and_gallery_association",
        "artifact_payload_and_preparation_occurrence",
        "artifact_payload_and_catalog_occurrence",
        "excluded_file_hash_evidence_and_membership",
    }
    assert set(report.dependency_preserving_decompositions) == {
        "gallery_identity_and_source_occurrence",
        "file_identity_and_source_occurrence",
        "file_content_payload_and_source_occurrence",
        "tag_identity_and_gallery_association",
        "artifact_payload_and_preparation_occurrence",
        "artifact_payload_and_catalog_occurrence",
        "excluded_file_hash_evidence_and_membership",
    }
    assert all(
        relation.materialization is not None
        for relation in contract.relations
        if relation.kind == "controlled_materialization"
    )
    assert all(not checker.bcnf_violations(relation) for relation in contract.relations)


def test_attribute_closure_and_candidate_keys_are_exact() -> None:
    dependencies = (
        _fd({"a"}, {"b"}),
        _fd({"b"}, {"a"}),
    )

    assert checker.attribute_closure({"a", "c"}, dependencies) == frozenset(
        {"a", "b", "c"}
    )
    assert set(checker.enumerate_candidate_keys({"a", "b", "c"}, dependencies)) == {
        frozenset({"a", "c"}),
        frozenset({"b", "c"}),
    }


def test_transitive_f_plus_bcnf_violation_is_rejected() -> None:
    relation = _relation(
        "transitive_violation",
        ("a", "b", "c", "d"),
        (
            _fd({"a"}, {"b"}),
            _fd({"b"}, {"c"}),
        ),
        (frozenset({"a", "d"}),),
    )

    violations = dict(checker.bcnf_violations(relation))
    assert "c" in violations[frozenset({"a"})]

    with pytest.raises(checker.ContractValidationError, match=r"not BCNF under F\+"):
        checker.validate_contract(checker.Contract(1, "negative", (relation,), ()))


@pytest.mark.parametrize(
    ("declared_keys", "message"),
    [
        ((frozenset({"a", "c"}),), "omits candidate keys"),
        (
            (
                frozenset({"a", "c"}),
                frozenset({"b", "c"}),
                frozenset({"a", "b", "c"}),
            ),
            "not minimal",
        ),
    ],
)
def test_declared_keys_must_equal_all_minimal_candidate_keys(
    declared_keys: tuple[frozenset[str], ...], message: str
) -> None:
    relation = _relation(
        "bad_keys",
        ("a", "b", "c"),
        (_fd({"a"}, {"b"}), _fd({"b"}, {"a"})),
        declared_keys,
    )

    with pytest.raises(checker.ContractValidationError, match=message):
        checker.validate_contract(checker.Contract(1, "negative", (relation,), ()))


def test_foreign_key_must_reference_a_candidate_key_with_equal_arity() -> None:
    parent = _relation(
        "parent",
        ("parent_id", "label"),
        (_fd({"parent_id"}, {"label"}),),
        (frozenset({"parent_id"}),),
    )
    child = _relation(
        "child",
        ("child_id", "parent_label"),
        (_fd({"child_id"}, {"parent_label"}),),
        (frozenset({"child_id"}),),
        foreign_keys=(checker.ForeignKey(("parent_label",), "parent", ("label",)),),
    )

    with pytest.raises(checker.ContractValidationError, match="is not a candidate key"):
        checker.validate_contract(checker.Contract(1, "negative", (parent, child), ()))


def test_fd_sensitive_attribute_requires_semantic_registry_entry() -> None:
    relation = _relation(
        "unclassified",
        ("source_id", "label"),
        (_fd({"source_id"}, {"label"}),),
        (frozenset({"source_id"}),),
    )

    with pytest.raises(
        checker.ContractValidationError, match="registry does not cover.*source_id"
    ):
        checker.validate_contract(checker.Contract(1, "negative", (relation,), ()))


def test_identity_digest_requires_singleton_candidate_key_relation() -> None:
    relation = _relation(
        "payload_occurrence",
        ("occurrence_id", "payload_sha256"),
        (_fd({"occurrence_id"}, {"payload_sha256"}),),
        (frozenset({"occurrence_id"}),),
    )
    semantics = (
        checker.AttributeSemantic(
            "occurrence_id", "surrogate_identifier", "test occurrence"
        ),
        checker.AttributeSemantic(
            "payload_sha256", "payload_digest", "test payload identity"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="identity digest but no relation declares it as a singleton candidate key",
    ):
        checker.validate_contract(
            checker.Contract(1, "negative", (relation,), (), semantics)
        )


def test_binary_lossless_decomposition_accepts_key_intersection() -> None:
    decomposition = checker.Decomposition(
        name="lossless",
        universal_attributes=frozenset({"a", "b", "c"}),
        functional_dependencies=(_fd({"b"}, {"a"}),),
        projections=(
            checker.Projection("left", frozenset({"a", "b"})),
            checker.Projection("right", frozenset({"b", "c"})),
        ),
        rationale="b determines the left projection",
    )

    assert checker.is_binary_lossless(decomposition)
    assert checker.is_dependency_preserving(decomposition)


def test_f_plus_projection_includes_dependencies_with_hidden_intermediates() -> None:
    dependencies = (
        _fd({"a"}, {"b"}),
        _fd({"b"}, {"c"}),
    )

    projected = checker.project_functional_dependencies({"a", "c"}, dependencies)

    assert checker.attribute_closure({"a"}, projected) == frozenset({"a", "c"})


def test_lossless_non_dependency_preserving_decomposition_is_rejected() -> None:
    left = _relation(
        "left",
        ("a", "b"),
        (_fd({"a"}, {"b"}),),
        (frozenset({"a"}),),
    )
    right = _relation(
        "right",
        ("a", "c"),
        (_fd({"a"}, {"c"}),),
        (frozenset({"a"}),),
    )
    decomposition = checker.Decomposition(
        name="lossless_but_not_dependency_preserving",
        universal_attributes=frozenset({"a", "b", "c"}),
        functional_dependencies=(
            _fd({"a"}, {"b"}),
            _fd({"b"}, {"c"}),
        ),
        projections=(
            checker.Projection("left", frozenset({"a", "b"})),
            checker.Projection("right", frozenset({"a", "c"})),
        ),
        rationale="negative dependency-preservation fixture",
    )

    assert checker.is_binary_lossless(decomposition)
    assert not checker.is_dependency_preserving(decomposition)
    with pytest.raises(
        checker.ContractValidationError, match="not dependency-preserving"
    ):
        checker.validate_contract(
            checker.Contract(1, "negative", (left, right), (decomposition,))
        )


def test_lossy_binary_decomposition_is_rejected() -> None:
    left = _relation(
        "left",
        ("a", "b"),
        (_fd({"a"}, {"b"}),),
        (frozenset({"a"}),),
    )
    right = _relation(
        "right",
        ("b", "c"),
        (),
        (frozenset({"b", "c"}),),
    )
    decomposition = checker.Decomposition(
        name="lossy",
        universal_attributes=frozenset({"a", "b", "c"}),
        functional_dependencies=(_fd({"a"}, {"b"}),),
        projections=(
            checker.Projection("left", frozenset({"a", "b"})),
            checker.Projection("right", frozenset({"b", "c"})),
        ),
        rationale="negative fixture",
    )

    assert not checker.is_binary_lossless(decomposition)
    assert checker.is_dependency_preserving(decomposition)
    with pytest.raises(checker.ContractValidationError, match="is lossy"):
        checker.validate_contract(
            checker.Contract(1, "negative", (left, right), (decomposition,))
        )


def test_cli_returns_zero_for_catalog_and_nonzero_for_invalid_contract(
    tmp_path: Path,
) -> None:
    contract = checker.load_contract(CATALOG)
    valid = subprocess.run(
        [sys.executable, str(CHECKER), str(CATALOG)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert valid.returncode == 0, valid.stderr
    assert f"{len(contract.relations)} BCNF relations" in valid.stdout
    assert f"{len(contract.decompositions)} lossless decompositions" in valid.stdout
    assert (
        f"{len(contract.decompositions)} dependency-preserving decompositions"
        in valid.stdout
    )

    invalid_contract = tmp_path / "invalid.toml"
    invalid_contract.write_text(
        """
contract_version = 1
name = "invalid"

[[relation]]
name = "not_bcnf"
kind = "source_of_truth"
attributes = ["a", "b", "c"]
declared_keys = [["a", "c"]]
fds = [{ determinant = ["a"], dependent = ["b"] }]
""".strip(),
        encoding="utf-8",
    )
    invalid = subprocess.run(
        [sys.executable, str(CHECKER), str(invalid_contract)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert invalid.returncode != 0
    assert "not BCNF under F+" in invalid.stderr
