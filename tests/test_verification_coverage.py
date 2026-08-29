from __future__ import annotations

import importlib.util
import subprocess
import sys
import tomllib
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
COVERAGE_CHECKER = ROOT / "verification" / "check_coverage.py"
COVERAGE_MANIFEST = ROOT / "verification" / "invariants.toml"
CATALOG_LOGICAL = ROOT / "verification" / "schema" / "catalog.toml"
CATALOG_CHECKER = ROOT / "verification" / "schema" / "check_contract.py"
OPERATIONAL_LOGICAL = ROOT / "verification" / "schema" / "operational.toml"
OPERATIONAL_PHYSICAL = ROOT / "verification" / "schema" / "operational_physical.toml"
OPERATIONAL_REFINEMENT = ROOT / "verification" / "schema" / "operational_refinement.py"
VERTICAL_FAMILY_TLA = ROOT / "verification" / "tla" / "VerticalFamily.tla"
VERTICAL_FAMILY_SMALL = ROOT / "verification" / "tla" / "VerticalFamilySmall.cfg"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


coverage = _load_module("h2hdb_verification_coverage", COVERAGE_CHECKER)
catalog = _load_module("h2hdb_catalog_coverage_contract", CATALOG_CHECKER)
operational = _load_module(
    "h2hdb_operational_coverage_refinement", OPERATIONAL_REFINEMENT
)


def _write_mutation(tmp_path: Path, old: str, new: str) -> Path:
    text = COVERAGE_MANIFEST.read_text(encoding="utf-8")
    assert old in text
    path = tmp_path / "invariants.toml"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    return path


def test_required_invariant_coverage_is_closed_and_nonempty() -> None:
    report = coverage.validate_coverage(COVERAGE_MANIFEST)
    required_ids: set[str] = set()
    for path in (CATALOG_LOGICAL, OPERATIONAL_LOGICAL):
        with path.open("rb") as stream:
            required_ids.update(
                value["id"]
                for value in tomllib.load(stream).get("semantic_obligation", ())
            )

    assert required_ids
    assert set(report.required_invariants) == required_ids
    assert report.evidence_ids
    assert len(report.blockers) == 48
    assert sum(blocker.startswith("catalog.") for blocker in report.blockers) == 21
    assert (
        sum(blocker.startswith("h2hdb.operational.") for blocker in report.blockers)
        == 27
    )


def test_missing_required_invariant_is_rejected(tmp_path: Path) -> None:
    text = COVERAGE_MANIFEST.read_text(encoding="utf-8")
    marker = '[[invariant]]\nid = "h2hdb.operational.physical-domains.v1"'
    start = text.index(marker)
    end = text.index("\n[[invariant]]", start + len(marker))
    path = tmp_path / "invariants.toml"
    path.write_text(text[:start] + text[end + 1 :], encoding="utf-8")

    with pytest.raises(
        coverage.CoverageValidationError, match="required invariants lack coverage"
    ):
        coverage.validate_coverage(path)


def test_required_loader_discovers_new_machine_obligation_ids(tmp_path: Path) -> None:
    source = tmp_path / "contract.toml"
    source.write_text(
        """
[[semantic_obligation]]
id = "h2hdb.test.first.v1"
lifecycle = "ready_and_runtime"

[[semantic_obligation]]
id = "h2hdb.test.newly-added.v1"
lifecycle = "ready_validation"
""",
        encoding="utf-8",
    )

    required = coverage.load_required_invariants((source,))

    assert [value.invariant_id for value in required] == [
        "h2hdb.test.first.v1",
        "h2hdb.test.newly-added.v1",
    ]


def test_empty_layer_coverage_is_rejected(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        'fd = { status = "supporting", evidence = ["fd.operational.closed-world"] }',
        'fd = { status = "supporting", evidence = [] }',
    )

    with pytest.raises(
        coverage.CoverageValidationError, match="must reference nonempty evidence"
    ):
        coverage.validate_coverage(path)


def test_bounded_tlc_cannot_be_labeled_an_unbounded_proof(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        'strength = "bounded_model_check"',
        'strength = "unbounded_theorem"',
    )

    with pytest.raises(
        coverage.CoverageValidationError, match="invalid for layer 'tla'"
    ):
        coverage.validate_coverage(path)


def test_vertical_family_small_declares_bounded_safety_scope() -> None:
    model = VERTICAL_FAMILY_TLA.read_text(encoding="utf-8")
    profile = VERTICAL_FAMILY_SMALL.read_text(encoding="utf-8")
    required_invariants = {
        "VisibleImpliesAllMembers",
        "PartialNeverVisible",
        "SealImmutable",
        "ReplayObservational",
        "CleanupNeverLeavesVisiblePartial",
        "SharedMemberRetention",
    }

    for invariant in required_invariants:
        assert f"{invariant} ==" in model
        assert invariant in profile
    assert "not an unbounded proof" in model
    assert "does not establish" in model
    assert "not an unbounded proof" in profile


def test_runtime_obligation_cannot_hide_missing_fault_coverage(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        'fault = { status = "blocked", evidence = ["fault.operational.binding-corruption"], blocker = "No production operational fault matrix injects every invalid physical value at a real writer boundary and proves transaction rollback." }',
        'fault = { status = "not_applicable", rationale = "This runtime invariant deliberately has no injected fault evidence yet." }',
    )

    with pytest.raises(
        coverage.CoverageValidationError, match="cannot be not_applicable"
    ):
        coverage.validate_coverage(path)


def test_stale_evidence_symbol_is_rejected(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        'symbol = "check_physical_domains_v1"',
        'symbol = "deleted_physical_domain_check"',
    )

    with pytest.raises(coverage.CoverageValidationError, match="symbol .* is absent"):
        coverage.validate_coverage(path)


def test_blocked_layer_requires_an_explicit_machine_blocker(tmp_path: Path) -> None:
    path = _write_mutation(
        tmp_path,
        'blocker = "No production cross-backend fault matrix injects every invalid width, storage class, counter, timestamp, enum, and collation-sensitive value."',
        'blocker = "unknown"',
    )

    with pytest.raises(
        coverage.CoverageValidationError,
        match="blocked requires a specific blocker",
    ):
        coverage.validate_coverage(path)


def test_production_evidence_layer_cannot_stop_at_supporting(
    tmp_path: Path,
) -> None:
    path = _write_mutation(
        tmp_path,
        'runtime_refinement = { status = "covered", evidence = ["runtime.physical-domains", "runtime.production.writer-bindings"] }',
        'runtime_refinement = { status = "supporting", evidence = ["runtime.physical-domains", "runtime.production.writer-bindings"] }',
    )

    with pytest.raises(
        coverage.CoverageValidationError,
        match="cannot use supporting as its terminal production-evidence status",
    ):
        coverage.validate_coverage(path)


def test_each_operational_obligation_rejects_a_corrupted_binding() -> None:
    with OPERATIONAL_LOGICAL.open("rb") as stream:
        logical = tomllib.load(stream)
    with OPERATIONAL_PHYSICAL.open("rb") as stream:
        physical = tomllib.load(stream)

    for index, obligation in enumerate(logical["semantic_obligation"]):
        mutated = deepcopy(logical)
        mutated["semantic_obligation"][index]["check"] = (
            f"{obligation['check']}.corrupted"
        )
        with pytest.raises(
            ValueError, match="unregistered version/scope/lifecycle/class/check"
        ):
            operational.validate_operational_machine_contract_documents(
                mutated, physical
            )


def test_each_data_obligation_rejects_a_corrupted_machine_binding() -> None:
    contract = catalog.load_contract(CATALOG_LOGICAL)
    for index, obligation in enumerate(contract.semantic_obligations):
        mutated_obligations = list(contract.semantic_obligations)
        mutated_obligations[index] = replace(obligation, lifecycle="corrupted")
        with pytest.raises(
            catalog.ContractValidationError,
            match="lifecycle must be",
        ):
            catalog.validate_contract(
                replace(contract, semantic_obligations=tuple(mutated_obligations))
            )


def test_coverage_cli_is_a_required_machine_gate() -> None:
    result = subprocess.run(
        [sys.executable, str(COVERAGE_CHECKER), str(COVERAGE_MANIFEST)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stderr
    assert "formal coverage blocked: invariants=30" in result.stdout
    assert "catalog.identity-codecs.v1:fault:" in result.stdout
    assert "catalog.retention.v2:integration:" in result.stdout
    assert "h2hdb.operational.gallery-staging.v1:fault:" in result.stdout


def test_coverage_validate_only_reports_production_blockers() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "verify-formal.py"),
            "coverage",
            "--validate-only",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (
        "formal coverage contract valid; production readiness blocked: "
        "invariants=30 evidence=121 blockers=48"
    ) in result.stdout
    assert "catalog.identity-codecs.v1:fault:" in result.stdout
    assert "h2hdb.operational.gallery-staging.v1:integration:" in result.stdout


def test_coverage_validate_only_still_rejects_invalid_manifest(
    tmp_path: Path,
) -> None:
    path = _write_mutation(tmp_path, "coverage_version = 1", "coverage_version = 2")

    result = subprocess.run(
        [sys.executable, str(COVERAGE_CHECKER), str(path), "--validate-only"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "coverage_version must be 1" in result.stderr


def test_validate_only_is_restricted_to_the_coverage_target() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "verify-formal.py"),
            "all",
            "--validate-only",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "--validate-only is only valid with the coverage target" in result.stderr
