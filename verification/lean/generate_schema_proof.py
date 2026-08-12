#!/usr/bin/env python3
"""Generate and drift-check the Lean schema contracts from catalog.toml."""

from __future__ import annotations

import argparse
import hashlib
import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "verification" / "schema" / "catalog.toml"
LEAN_FILE = ROOT / "verification" / "lean" / "VNextSchema.lean"
BEGIN = "/- BEGIN GENERATED CATALOG CONTRACTS -/"
END = "/- END GENERATED CATALOG CONTRACTS -/"


def lean_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def lean_list(values: list[str]) -> str:
    return "[" + ", ".join(lean_string(value) for value in values) + "]"


def lean_nested(values: list[list[str]]) -> str:
    return "[" + ", ".join(lean_list(value) for value in values) + "]"


def contract_name(name: str) -> str:
    return f"{name}_contract"


def render(manifest_bytes: bytes) -> str:
    manifest = tomllib.loads(manifest_bytes.decode("utf-8"))
    relations = manifest["relation"]
    decompositions = manifest.get("decomposition", [])
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    lines: list[str] = [
        BEGIN,
        f'def catalogManifestSha256 : String := "{digest}"',
        "",
        "/-! This section is mechanically generated from catalog.toml. -/",
        "",
    ]
    for relation in relations:
        name = relation["name"]
        fds = relation.get("fds", [])
        lines.extend(
            [
                f"def {contract_name(name)} : RelationContract where",
                f"  name := {lean_string(name)}",
                f"  attributes := {lean_list(relation['attributes'])}",
                f"  declaredKeys := {lean_nested(relation['declared_keys'])}",
                "  declaredFDs := [",
            ]
        )
        for index, fd in enumerate(fds):
            suffix = "," if index + 1 < len(fds) else ""
            lines.append(
                "    { determinant := "
                + lean_list(fd["determinant"])
                + ", dependent := "
                + lean_list(fd["dependent"])
                + " }"
                + suffix
            )
        lines.extend(
            [
                "  ]",
                "",
                f"theorem {name}_schema_well_formed :",
                f"    schemaWellFormedCheck {contract_name(name)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_candidate_keys_check :",
                f"    keysDetermineAllCheck {contract_name(name)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_candidate_keys_determine_all_attributes :",
                f"    KeysDetermineAllAttributes {contract_name(name)} :=",
                f"  keysDetermineAllCheck_sound {contract_name(name)}",
                f"    {name}_candidate_keys_check",
                "",
                f"theorem {name}_candidate_keys_minimal_check :",
                f"    declaredKeysMinimalCheck {contract_name(name)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_declared_keys_are_candidate_keys :",
                f"    DeclaredKeysAreMinimal {contract_name(name)} :=",
                f"  declaredKeysMinimalCheck_sound {contract_name(name)}",
                f"    {name}_candidate_keys_minimal_check",
                "",
                f"theorem {name}_closure_fixed_check :",
                f"    closureFixedPointCheck {contract_name(name)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_closure_reached_fixed_point :",
                f"    ClosureReachedFixedPoint {contract_name(name)} :=",
                f"  closureFixedPointCheck_sound {contract_name(name)}",
                f"    {name}_closure_fixed_check",
                "",
                f"theorem {name}_bcnf_check :",
                f"    bcnfCheck {contract_name(name)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_bcnf : BCNF {contract_name(name)} :=",
                f"  bcnfCheck_sound {contract_name(name)} {name}_bcnf_check",
                "",
            ]
        )

    contract_names = [contract_name(relation["name"]) for relation in relations]
    lines.extend(
        [
            "def manifestContracts : List RelationContract := [",
            *(
                f"  {name}{',' if index + 1 < len(contract_names) else ''}"
                for index, name in enumerate(contract_names)
            ),
            "]",
            "",
            "theorem manifest_relation_count :",
            f"    manifestContracts.length = {len(relations)} := by",
            "  native_decide",
            "",
        ]
    )
    for decomposition in decompositions:
        name = decomposition["name"]
        projections = decomposition["projections"]
        if len(projections) != 2:
            raise ValueError(
                f"{name}: Lean baseline supports binary decompositions only"
            )
        left = projections[0]["attributes"]
        right = projections[1]["attributes"]
        intersection = [value for value in left if value in right]
        fds = decomposition.get("fds", [])
        lines.extend(
            [
                f"def {name}_contract : BinaryDecompositionContract where",
                f"  name := {lean_string(name)}",
                "  universalAttributes := "
                f"{lean_list(decomposition['universal_attributes'])}",
                f"  leftAttributes := {lean_list(left)}",
                f"  rightAttributes := {lean_list(right)}",
                "  declaredFDs := [",
            ]
        )
        for index, fd in enumerate(fds):
            suffix = "," if index + 1 < len(fds) else ""
            lines.append(
                "    { determinant := "
                + lean_list(fd["determinant"])
                + ", dependent := "
                + lean_list(fd["dependent"])
                + " }"
                + suffix
            )
        lines.extend(
            [
                "  ]",
                "",
                f"theorem {name}_projection_check :",
                "    binaryDecompositionWellFormedCheck",
                f"      {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_projection_well_formed :",
                f"    BinaryDecompositionWellFormed {name}_contract :=",
                "  binaryDecompositionWellFormedCheck_sound",
                f"    {name}_contract {name}_projection_check",
                "",
                f"theorem {name}_intersection_check :",
                "    sameAttrSet (attributeIntersection",
                f"      {name}_contract.leftAttributes",
                f"      {name}_contract.rightAttributes)",
                f"      {lean_list(intersection)} = true := by",
                "  native_decide",
                "",
                f"theorem {name}_lossless_check :",
                f"    binaryLosslessCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_lossless : BinaryLossless {name}_contract :=",
                f"  ⟨{name}_projection_well_formed,",
                f"    binaryLosslessCheck_sound {name}_contract",
                f"      {name}_lossless_check⟩",
                "",
                f"theorem {name}_dependency_preservation_check :",
                f"    dependencyPreservationCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_dependency_preserving :",
                f"    DependencyPreserving {name}_contract :=",
                f"  dependencyPreservationCheck_sound {name}_contract",
                f"    {name}_dependency_preservation_check",
                "",
            ]
        )
    if decompositions:
        lossless_prop = " ∧\n    ".join(
            f"BinaryLossless {decomposition['name']}_contract"
            for decomposition in decompositions
        )
        lossless_names = [
            f"{decomposition['name']}_lossless" for decomposition in decompositions
        ]
        lines.extend(
            [
                "theorem all_manifest_decompositions_lossless :",
                f"    {lossless_prop} := by",
                "  exact ⟨" + ",\n    ".join(lossless_names) + "⟩",
                "",
            ]
        )
        dependency_preserving_prop = " ∧\n    ".join(
            f"DependencyPreserving {decomposition['name']}_contract"
            for decomposition in decompositions
        )
        dependency_preserving_names = [
            f"{decomposition['name']}_dependency_preserving"
            for decomposition in decompositions
        ]
        lines.extend(
            [
                "theorem all_manifest_decompositions_dependency_preserving :",
                f"    {dependency_preserving_prop} := by",
                "  exact ⟨" + ",\n    ".join(dependency_preserving_names) + "⟩",
                "",
            ]
        )
    bcnf_prop = " ∧\n    ".join(
        f"BCNF {contract_name(relation['name'])}" for relation in relations
    )
    bcnf_names = [f"{relation['name']}_bcnf" for relation in relations]
    keys_prop = " ∧\n    ".join(
        f"KeysDetermineAllAttributes {contract_name(relation['name'])}"
        for relation in relations
    )
    key_names = [
        f"{relation['name']}_candidate_keys_determine_all_attributes"
        for relation in relations
    ]
    lines.extend(
        [
            "theorem all_manifest_relations_bcnf :",
            f"    {bcnf_prop} := by",
            "  exact ⟨" + ",\n    ".join(bcnf_names) + "⟩",
            "",
            "theorem all_manifest_candidate_keys_determine_attributes :",
            f"    {keys_prop} := by",
            "  exact ⟨" + ",\n    ".join(key_names) + "⟩",
            "",
            END,
        ]
    )
    return "\n".join(lines)


def replace_generated(source: str, generated: str) -> str:
    if BEGIN not in source or END not in source:
        raise RuntimeError("VNextSchema.lean has no generated-section markers")
    prefix, rest = source.split(BEGIN, 1)
    _old, suffix = rest.split(END, 1)
    # The generated section is the last content in this namespace.  Keeping a
    # single canonical closing line also recovers old pre-marker generated tails.
    _unused = suffix
    return prefix + generated + "\n\nend H2HDB.Verification.VNextSchema\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of rewriting when generated Lean is stale",
    )
    args = parser.parse_args()
    expected = replace_generated(
        LEAN_FILE.read_text(encoding="utf-8"),
        render(MANIFEST.read_bytes()),
    )
    actual = LEAN_FILE.read_text(encoding="utf-8")
    if args.check:
        if actual != expected:
            raise SystemExit(
                "VNextSchema.lean is stale; run "
                "verification/lean/generate_schema_proof.py"
            )
        return
    LEAN_FILE.write_text(expected, encoding="utf-8")


if __name__ == "__main__":
    main()
