"""Validate closed-world evidence coverage for required formal invariants."""

from __future__ import annotations

import argparse
import ast
import re
import tomllib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LAYERS = (
    "fd",
    "lean",
    "tla",
    "runtime_refinement",
    "fault",
    "integration",
)
STATUSES = frozenset({"covered", "supporting", "not_applicable", "blocked"})
STRENGTHS = {
    "fd": frozenset({"closed_world_fd_contract"}),
    "lean": frozenset({"unbounded_theorem", "generated_decidable_check"}),
    "tla": frozenset({"bounded_model_check"}),
    "runtime_refinement": frozenset({"executable_contract_check"}),
    "fault": frozenset({"negative_control"}),
    "integration": frozenset(
        {"sqlite_integration", "mariadb_integration", "runtime_integration"}
    ),
}
DIRECTNESS = frozenset({"direct", "supporting"})
RUNTIME_LIFECYCLES = frozenset(
    {"ready_validation", "building_to_ready", "ready_and_runtime"}
)
NOT_APPLICABLE_LAYERS = frozenset({"lean", "tla", "fault", "integration"})
_MISSING_RATIONALE = re.compile(
    r"\b(?:missing|not covered|not yet|todo|unavailable)\b", re.I
)


class CoverageValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("coverage validation failed:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True)
class RequiredInvariant:
    invariant_id: str
    lifecycle: str
    source: str


@dataclass(frozen=True, slots=True)
class Evidence:
    evidence_id: str
    layer: str
    strength: str
    directness: str
    locations: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class CoverageReport:
    required_invariants: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    blockers: tuple[str, ...]


def _tables(value: object, context: str, errors: list[str]) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        errors.append(f"{context} must be an array of tables")
        return []
    return value


def _text(value: object, context: str, errors: list[str]) -> str:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{context} must be nonempty text")
        return ""
    return value


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def load_required_invariants(
    source_paths: tuple[Path, ...],
) -> tuple[RequiredInvariant, ...]:
    errors: list[str] = []
    required: list[RequiredInvariant] = []
    for path in source_paths:
        try:
            document = _load_toml(path)
        except (OSError, tomllib.TOMLDecodeError) as error:
            errors.append(f"cannot load invariant source {path}: {error}")
            continue
        obligations = document.get("semantic_obligation", [])
        if not isinstance(obligations, list):
            errors.append(f"{path}: semantic_obligation must be an array")
            continue
        for index, raw in enumerate(obligations):
            context = f"{path}: semantic_obligation[{index}]"
            if not isinstance(raw, dict):
                errors.append(f"{context} must be a table")
                continue
            invariant_id = _text(raw.get("id"), f"{context}.id", errors)
            lifecycle = _text(raw.get("lifecycle"), f"{context}.lifecycle", errors)
            if invariant_id and lifecycle:
                required.append(RequiredInvariant(invariant_id, lifecycle, str(path)))
    ids = [value.invariant_id for value in required]
    duplicates = sorted({value for value in ids if ids.count(value) > 1})
    if duplicates:
        errors.append("required invariant IDs are duplicated: " + ", ".join(duplicates))
    if not required:
        errors.append("no required semantic obligations were found")
    if errors:
        raise CoverageValidationError(errors)
    return tuple(required)


def _resolve_repo_path(value: str, context: str, errors: list[str]) -> Path | None:
    candidate = Path(value)
    if candidate.is_absolute() or ".." in candidate.parts:
        errors.append(f"{context} must be a repository-relative path")
        return None
    resolved = REPOSITORY_ROOT / candidate
    if not resolved.is_file():
        errors.append(f"{context} does not exist: {value!r}")
        return None
    return resolved


@cache
def _python_symbols(path: Path) -> frozenset[str]:
    """Parse each evidence module once per checker process.

    A production evidence manifest intentionally references the same test and
    implementation modules many times.  Re-parsing those large modules for
    every location made validation proportional to locations times source
    size without changing the validation result.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return frozenset(
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    )


def _location_exists(path: Path, symbol: str) -> bool:
    if path.suffix == ".py":
        return symbol.rsplit(".", 1)[-1] in _python_symbols(path)
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".lean":
        return (
            re.search(rf"(?m)^\s*(?:theorem|def)\s+{re.escape(symbol)}\b", text)
            is not None
        )
    if path.suffix == ".tla":
        return re.search(rf"(?m)^\s*{re.escape(symbol)}\s*==", text) is not None
    return symbol in text


def _parse_evidence(document: dict[str, Any], errors: list[str]) -> dict[str, Evidence]:
    result: dict[str, Evidence] = {}
    for index, raw in enumerate(_tables(document.get("evidence"), "evidence", errors)):
        context = f"evidence[{index}]"
        evidence_id = _text(raw.get("id"), f"{context}.id", errors)
        layer = _text(raw.get("layer"), f"{context}.layer", errors)
        strength = _text(raw.get("strength"), f"{context}.strength", errors)
        directness = _text(raw.get("directness"), f"{context}.directness", errors)
        claim = _text(raw.get("claim"), f"{context}.claim", errors)
        if claim and len(claim) < 20:
            errors.append(f"{context}.claim must explain the evidence scope")
        if layer not in LAYERS:
            errors.append(f"{context}.layer is unsupported: {layer!r}")
        elif strength not in STRENGTHS[layer]:
            errors.append(
                f"{context}.strength {strength!r} is invalid for layer {layer!r}"
            )
        if directness not in DIRECTNESS:
            errors.append(f"{context}.directness is unsupported: {directness!r}")
        assumptions = raw.get("assumptions", [])
        if not isinstance(assumptions, list) or any(
            not isinstance(value, str) or not value.strip() for value in assumptions
        ):
            errors.append(f"{context}.assumptions must be an array of nonempty text")
            assumptions = []
        if strength in {"unbounded_theorem", "bounded_model_check"} and not assumptions:
            errors.append(f"{context} must state assumptions for {strength}")
        raw_locations = _tables(raw.get("location"), f"{context}.location", errors)
        locations: list[tuple[str, str]] = []
        for location_index, location in enumerate(raw_locations):
            location_context = f"{context}.location[{location_index}]"
            path_text = _text(location.get("path"), f"{location_context}.path", errors)
            symbol = _text(location.get("symbol"), f"{location_context}.symbol", errors)
            path = (
                _resolve_repo_path(path_text, f"{location_context}.path", errors)
                if path_text
                else None
            )
            if path is not None and symbol and not _location_exists(path, symbol):
                errors.append(
                    f"{location_context} symbol {symbol!r} is absent from {path_text!r}"
                )
            if path_text and symbol:
                locations.append((path_text, symbol))
        if not locations:
            errors.append(f"{context} must have at least one evidence location")
        if strength == "bounded_model_check":
            suffixes = {Path(path).suffix for path, _symbol in locations}
            if ".tla" not in suffixes or ".cfg" not in suffixes:
                errors.append(
                    f"{context} bounded TLC evidence requires both .tla and .cfg locations"
                )
            if not any(
                "Small.cfg" in path or "Deep.cfg" in path for path, _ in locations
            ):
                errors.append(
                    f"{context} bounded TLC evidence must name a model profile"
                )
        if evidence_id in result:
            errors.append(f"duplicate evidence ID {evidence_id!r}")
        elif evidence_id:
            result[evidence_id] = Evidence(
                evidence_id, layer, strength, directness, tuple(locations)
            )
    return result


def _validate_tool_pins(document: dict[str, Any], errors: list[str]) -> None:
    tools = document.get("tools")
    if not isinstance(tools, dict):
        errors.append("tools must be a table")
        return
    lean_path_text = _text(tools.get("lean_toolchain"), "tools.lean_toolchain", errors)
    tlc_path_text = _text(tools.get("tlc_lock"), "tools.tlc_lock", errors)
    python_version = _text(tools.get("python"), "tools.python", errors)
    pytest_version = _text(tools.get("pytest"), "tools.pytest", errors)
    if python_version and re.fullmatch(r"\d+\.\d+\.\d+", python_version) is None:
        errors.append("tools.python must be an exact three-part version")
    workflow_text = _text(tools.get("ci_workflow"), "tools.ci_workflow", errors)
    if pytest_version and re.fullmatch(r"\d+\.\d+\.\d+", pytest_version) is None:
        errors.append("tools.pytest must be an exact three-part version")
    lean_path = (
        _resolve_repo_path(lean_path_text, "tools.lean_toolchain", errors)
        if lean_path_text
        else None
    )
    if lean_path is not None:
        lean_value = lean_path.read_text(encoding="utf-8").strip()
        if re.fullmatch(r"leanprover/lean4:v\d+\.\d+\.\d+", lean_value) is None:
            errors.append("Lean toolchain must be pinned to an exact release")
    tlc_path = (
        _resolve_repo_path(tlc_path_text, "tools.tlc_lock", errors)
        if tlc_path_text
        else None
    )
    if tlc_path is not None:
        tlc = _load_toml(tlc_path).get("tla_plus", {}).get("tlc", {})
        if (
            not isinstance(tlc, dict)
            or re.fullmatch(r"\d+\.\d+\.\d+", str(tlc.get("version", ""))) is None
            or re.fullmatch(r"[0-9a-f]{64}", str(tlc.get("sha256", ""))) is None
            or re.fullmatch(
                r"[^@]+@sha256:[0-9a-f]{64}", str(tlc.get("runtime_image", ""))
            )
            is None
        ):
            errors.append("TLC lock must pin version, JAR checksum, and image digest")
    workflow_path = (
        _resolve_repo_path(workflow_text, "tools.ci_workflow", errors)
        if workflow_text
        else None
    )
    if workflow_path is not None and pytest_version:
        workflow = workflow_path.read_text(encoding="utf-8")
        required_commands = (
            f'python-version: "{python_version}"',
            f"pytest=={pytest_version}",
            "python scripts/verify-formal.py coverage",
            "python scripts/verify-formal.py schema",
            "python scripts/verify-formal.py lean",
            "python scripts/verify-formal.py tla",
            "python scripts/generate-vnext-schema-provider.py --check",
            "--tla-runtime docker",
            "--deep",
        )
        for command in required_commands:
            if command not in workflow:
                errors.append(
                    f"tools.ci_workflow does not contain required pinned gate {command!r}"
                )


def validate_coverage(path: Path) -> CoverageReport:
    errors: list[str] = []
    try:
        document = _load_toml(path)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise CoverageValidationError(
            [f"cannot load coverage manifest {path}: {error}"]
        )
    if document.get("coverage_version") != 1:
        errors.append("coverage_version must be 1")
    raw_sources = document.get("invariant_sources")
    if not isinstance(raw_sources, list) or any(
        not isinstance(value, str) or not value for value in raw_sources
    ):
        errors.append("invariant_sources must be a nonempty array of paths")
        raw_sources = []
    source_paths: list[Path] = []
    for index, value in enumerate(raw_sources):
        source = _resolve_repo_path(value, f"invariant_sources[{index}]", errors)
        if source is not None:
            source_paths.append(source)
    required: tuple[RequiredInvariant, ...] = ()
    if source_paths:
        try:
            required = load_required_invariants(tuple(source_paths))
        except CoverageValidationError as error:
            errors.extend(error.errors)
    _validate_tool_pins(document, errors)
    evidence = _parse_evidence(document, errors)
    raw_invariants = _tables(document.get("invariant"), "invariant", errors)
    invariant_by_id: dict[str, dict[str, Any]] = {}
    used_evidence: set[str] = set()
    blockers: list[str] = []
    required_by_id = {value.invariant_id: value for value in required}
    for index, raw in enumerate(raw_invariants):
        context = f"invariant[{index}]"
        invariant_id = _text(raw.get("id"), f"{context}.id", errors)
        if invariant_id in invariant_by_id:
            errors.append(f"duplicate invariant coverage ID {invariant_id!r}")
        elif invariant_id:
            invariant_by_id[invariant_id] = raw
        required_value = required_by_id.get(invariant_id)
        covered_layers = 0
        for layer in LAYERS:
            mapping = raw.get(layer)
            if not isinstance(mapping, dict):
                errors.append(f"{context}.{layer} must be a table")
                continue
            status = mapping.get("status")
            if status not in STATUSES:
                errors.append(f"{context}.{layer}.status is unsupported: {status!r}")
                continue
            references = mapping.get("evidence", [])
            rationale = mapping.get("rationale")
            blocker = mapping.get("blocker")
            if status == "not_applicable":
                if references:
                    errors.append(
                        f"{context}.{layer} not_applicable must not reference evidence"
                    )
                if not isinstance(rationale, str) or len(rationale.strip()) < 30:
                    errors.append(
                        f"{context}.{layer} not_applicable requires a specific rationale"
                    )
                elif _MISSING_RATIONALE.search(rationale):
                    errors.append(
                        f"{context}.{layer} not_applicable cannot excuse missing evidence"
                    )
                if layer not in NOT_APPLICABLE_LAYERS:
                    errors.append(
                        f"{context}.{layer} is never allowed to be not_applicable"
                    )
                if (
                    layer == "tla"
                    and isinstance(rationale, str)
                    and "CatalogCore" not in rationale
                ):
                    errors.append(
                        f"{context}.tla not_applicable must identify the unmodeled CatalogCore boundary"
                    )
                if (
                    required_value is not None
                    and required_value.lifecycle in RUNTIME_LIFECYCLES
                    and layer in {"fault", "integration"}
                ):
                    errors.append(
                        f"{context}.{layer} cannot be not_applicable for lifecycle "
                        f"{required_value.lifecycle!r}"
                    )
                continue
            if status == "blocked":
                if rationale is not None:
                    errors.append(f"{context}.{layer} blocked must omit rationale")
                if not isinstance(blocker, str) or len(blocker.strip()) < 30:
                    errors.append(
                        f"{context}.{layer} blocked requires a specific blocker"
                    )
                else:
                    blockers.append(f"{invariant_id}:{layer}: {blocker.strip()}")
            elif blocker is not None:
                errors.append(f"{context}.{layer} is not blocked and must omit blocker")
            if not isinstance(references, list) or any(
                not isinstance(value, str) or not value for value in references
            ):
                errors.append(f"{context}.{layer}.evidence must be an array of IDs")
                continue
            if status != "blocked" and not references:
                errors.append(f"{context}.{layer} must reference nonempty evidence")
                continue
            if rationale is not None:
                errors.append(
                    f"{context}.{layer} uses evidence and must omit rationale"
                )
            referenced: list[Evidence] = []
            for evidence_id in references:
                value = evidence.get(evidence_id)
                if value is None:
                    errors.append(
                        f"{context}.{layer} references unknown evidence {evidence_id!r}"
                    )
                elif value.layer != layer:
                    errors.append(
                        f"{context}.{layer} references {evidence_id!r} from layer "
                        f"{value.layer!r}"
                    )
                else:
                    referenced.append(value)
                    used_evidence.add(evidence_id)
            if status == "covered":
                covered_layers += 1
                if referenced and not any(
                    value.directness == "direct" for value in referenced
                ):
                    errors.append(f"{context}.{layer} covered requires direct evidence")
            if (
                required_value is not None
                and layer in {"runtime_refinement", "fault", "integration"}
                and status == "supporting"
            ):
                errors.append(
                    f"{context}.{layer} cannot use supporting as its terminal "
                    "production-evidence status; use covered or blocked"
                )
        if covered_layers == 0 and not any(
            isinstance(raw.get(layer), dict) and raw[layer].get("status") == "blocked"
            for layer in LAYERS
        ):
            errors.append(f"{context} has no directly covered layer")
    required_ids = set(required_by_id)
    declared_ids = set(invariant_by_id)
    if missing := sorted(required_ids - declared_ids):
        errors.append("required invariants lack coverage: " + ", ".join(missing))
    if extra := sorted(declared_ids - required_ids):
        errors.append("coverage declares unknown invariants: " + ", ".join(extra))
    if unused := sorted(set(evidence) - used_evidence):
        errors.append("evidence is unreferenced: " + ", ".join(unused))
    if errors:
        raise CoverageValidationError(errors)
    return CoverageReport(
        tuple(sorted(required_ids)),
        tuple(sorted(evidence)),
        tuple(blockers),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        default=REPOSITORY_ROOT / "verification" / "invariants.toml",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "validate the closed-world contract while reporting, but not failing "
            "on, production blockers"
        ),
    )
    arguments = parser.parse_args()
    report = validate_coverage(arguments.manifest)
    if report.blockers:
        if arguments.validate_only:
            print(
                "formal coverage contract valid; production readiness blocked: "
                f"invariants={len(report.required_invariants)} "
                f"evidence={len(report.evidence_ids)} "
                f"blockers={len(report.blockers)}",
            )
        else:
            print(
                "formal coverage blocked: "
                f"invariants={len(report.required_invariants)} "
                f"evidence={len(report.evidence_ids)} "
                f"blockers={len(report.blockers)}",
            )
        for blocker in report.blockers:
            print(f"- {blocker}")
        if not arguments.validate_only:
            raise SystemExit(1)
        return
    print(
        "formal coverage valid: "
        f"invariants={len(report.required_invariants)} "
        f"evidence={len(report.evidence_ids)}"
    )


if __name__ == "__main__":
    main()
