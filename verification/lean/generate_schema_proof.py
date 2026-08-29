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


def unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def is_logical_view(relation: dict[str, object]) -> bool:
    materialization = relation.get("materialization")
    return isinstance(materialization, dict) and materialization.get("storage") in {
        "logical_view",
        "inline_projection",
    }


def render(manifest_bytes: bytes) -> str:
    manifest = tomllib.loads(manifest_bytes.decode("utf-8"))
    relations = manifest["relation"]
    decompositions = manifest.get("decomposition", [])
    vertical_families = manifest.get("vertical_family", [])
    analysis_candidate_contract = manifest.get("analysis_candidate_contract")
    if analysis_candidate_contract is not None:
        if not isinstance(analysis_candidate_contract, dict):
            raise ValueError("analysis_candidate_contract must be a table")
        expected_gid_candidate_contract: dict[str, object] = {
            "gid_candidate_membership_relation": "analysis_gid_candidate_resolved",
            "gid_winner_selection_relation": "analysis_gid_winner_selection",
            "gid_winner_shadow_relation": "analysis_gid_winner_shadow",
            "gid_keyset_relation": "analysis_impacted_gid",
            "gid_run_build_relation": "analysis_run_descriptor",
            "gid_build_membership_relation": "source_build_gallery",
            "gid_metadata_relation": "gallery_observation_metadata",
            "gid_order_attributes": [
                "prefer_not_already_uploaded",
                "title_scalar_count",
                "download_time",
                "scope_key",
                "locator_sha256",
            ],
        }
        for field_name, expected in expected_gid_candidate_contract.items():
            if analysis_candidate_contract.get(field_name) != expected:
                raise ValueError(
                    f"analysis_candidate_contract.{field_name} must equal {expected!r}"
                )
    impacted_key_contract = manifest.get("analysis_impacted_key_contract")
    impacted_key_families: list[dict[str, object]] = []
    if impacted_key_contract is not None:
        if not isinstance(impacted_key_contract, dict):
            raise ValueError("analysis_impacted_key_contract must be a table")
        raw_impacted_key_families = impacted_key_contract.get("family")
        if (
            impacted_key_contract.get("version") != 1
            or impacted_key_contract.get("maximum_page_galleries") != 128
            or impacted_key_contract.get("maximum_provenance_rows") != 257
            or not isinstance(raw_impacted_key_families, list)
            or not all(isinstance(family, dict) for family in raw_impacted_key_families)
        ):
            raise ValueError(
                "analysis impacted-key Lean protocol must equal v1 exactly"
            )
        impacted_key_families = raw_impacted_key_families
        if {family.get("name") for family in impacted_key_families} != {
            "content",
            "gid",
        }:
            raise ValueError("analysis impacted-key Lean families must be content+gid")
    relation_by_name = {relation["name"]: relation for relation in relations}
    semantic_obligations = manifest.get("semantic_obligation", [])
    if not isinstance(semantic_obligations, list) or not all(
        isinstance(obligation, dict) for obligation in semantic_obligations
    ):
        raise ValueError("semantic_obligation must be an array of tables")
    physical_domain_obligations = [
        obligation
        for obligation in semantic_obligations
        if obligation.get("id") == "catalog.physical-domains.v1"
    ]
    is_catalog_manifest = manifest.get("scope") == "catalog_data_plane"
    if is_catalog_manifest and len(physical_domain_obligations) != 1:
        raise ValueError(
            "catalog.physical-domains.v1 must occur exactly once for Lean generation"
        )
    if not is_catalog_manifest and physical_domain_obligations:
        raise ValueError(
            "catalog.physical-domains.v1 is forbidden outside the catalog manifest"
        )
    physical_domain_names: list[str] | None = None
    physical_domain_mutation_names: list[str] = []
    physical_domain_read_only_names: list[str] = []
    if physical_domain_obligations:
        raw_physical_domain_names = physical_domain_obligations[0].get("relations")
        if not isinstance(raw_physical_domain_names, list) or not all(
            isinstance(name, str) and name for name in raw_physical_domain_names
        ):
            raise ValueError(
                "catalog.physical-domains.v1 relations must be nonempty names"
            )
        physical_domain_names = list(raw_physical_domain_names)
        if len(physical_domain_names) != len(set(physical_domain_names)):
            raise ValueError("catalog physical-domain relations must be unique")
        missing_physical_domain_names = set(physical_domain_names) - set(
            relation_by_name
        )
        if missing_physical_domain_names:
            raise ValueError(
                "catalog physical-domain relations are absent from the manifest: "
                f"{sorted(missing_physical_domain_names)!r}"
            )
        physical_domain_mutation_names = [
            name
            for name in physical_domain_names
            if not is_logical_view(relation_by_name[name])
        ]
        physical_domain_read_only_names = [
            name
            for name in physical_domain_names
            if is_logical_view(relation_by_name[name])
        ]
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
            ]
        )
        if not is_logical_view(relation):
            lines.extend(
                [
                    f"theorem {name}_bcnf_check :",
                    f"    bcnfCheck {contract_name(name)} = true := by",
                    "  native_decide",
                    "",
                    f"theorem {name}_bcnf : BCNF {contract_name(name)} :=",
                    f"  bcnfCheck_sound {contract_name(name)} {name}_bcnf_check",
                    "",
                ]
            )

    if "publication_commit" in relation_by_name:
        lines.extend(
            [
                "theorem publication_commit_has_six_equivalent_candidate_keys :",
                "    publication_commit_contract.declaredKeys.length = 6 := by",
                "  native_decide",
                "",
            ]
        )
    for receipt_name in ("analysis_batch_receipt", "publication_batch_receipt"):
        if receipt_name in relation_by_name:
            lines.extend(
                [
                    f"theorem {receipt_name}_has_three_derived_candidate_keys :",
                    f"    {receipt_name}_contract.declaredKeys.length = 3 := by",
                    "  native_decide",
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
    if physical_domain_names is not None:
        physical_contract_groups = (
            ("catalogPhysicalDomainContracts", physical_domain_names),
            ("catalogPhysicalDomainMutationContracts", physical_domain_mutation_names),
            (
                "catalogPhysicalDomainReadOnlyViewContracts",
                physical_domain_read_only_names,
            ),
        )
        lines.extend(
            [
                "/-! Closed catalog physical-domain authority from the manifest. -/",
                "",
            ]
        )
        for definition, names in physical_contract_groups:
            lines.extend(
                [
                    f"def {definition} : List RelationContract := [",
                    *(
                        f"  {contract_name(name)}{',' if index + 1 < len(names) else ''}"
                        for index, name in enumerate(names)
                    ),
                    "]",
                    "",
                ]
            )
        lines.extend(
            [
                "theorem catalog_physical_domain_relation_count :",
                "    catalogPhysicalDomainContracts.length = "
                f"{len(physical_domain_names)} := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_mutation_relation_count :",
                "    catalogPhysicalDomainMutationContracts.length = "
                f"{len(physical_domain_mutation_names)} := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_read_only_view_count :",
                "    catalogPhysicalDomainReadOnlyViewContracts.length = "
                f"{len(physical_domain_read_only_names)} := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_has_no_duplicates :",
                "    catalogPhysicalDomainContracts.Nodup := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_is_manifest_closed :",
                "    ∀ contract ∈ catalogPhysicalDomainContracts,",
                "      contract ∈ manifestContracts := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_partition_is_exact :",
                "    (catalogPhysicalDomainMutationContracts ++",
                "      catalogPhysicalDomainReadOnlyViewContracts).all",
                "        (fun contract => contract ∈ catalogPhysicalDomainContracts) = true ∧",
                "      catalogPhysicalDomainContracts.all (fun contract =>",
                "        contract ∈ catalogPhysicalDomainMutationContracts ||",
                "          contract ∈ catalogPhysicalDomainReadOnlyViewContracts) = true := by",
                "  native_decide",
                "",
                "theorem catalog_physical_domain_partition_is_disjoint :",
                "    ∀ contract ∈ catalogPhysicalDomainMutationContracts,",
                "      contract ∉ catalogPhysicalDomainReadOnlyViewContracts := by",
                "  native_decide",
                "",
            ]
        )
    if impacted_key_families:
        lines.extend(
            [
                "/-! Unbounded sealed impacted-key protocol instantiated from the manifest. -/",
                "",
                "def analysisImpactedMaximumPageGalleries : Nat := 128",
                "def analysisImpactedMaximumProvenanceRows : Nat := 257",
                "",
                "theorem analysis_impacted_page_bounds_match_manifest :",
                "    analysisImpactedMaximumPageGalleries = 128 ∧",
                "      analysisImpactedMaximumProvenanceRows = 257 := by",
                "  native_decide",
                "",
            ]
        )
        for family in sorted(
            impacted_key_families, key=lambda value: str(value["name"])
        ):
            prefix = f"analysis_impacted_{family['name']}"
            lines.extend(
                [
                    f"theorem {prefix}_first_insert_min_witness",
                    "    (gallery : Nat) :",
                    "    ImpactedMinimumWitness gallery",
                    "        (impactedFirstInsert gallery).provenance ∧",
                    "      impactedKeyVisible (impactedFirstInsert gallery) = true :=",
                    "  impacted_first_insert_establishes_minimum_and_visibility gallery",
                    "",
                    f"theorem {prefix}_append_greater_preserves_min_and_visibility",
                    "    (rows : ImpactedKeyRows)",
                    "    (witness gallery : Nat)",
                    "    (minimum : ImpactedMinimumWitness witness rows.provenance)",
                    "    (greater : witness < gallery)",
                    "    (visible : impactedKeyVisible rows = true) :",
                    "    ImpactedMinimumWitness witness",
                    "        (impactedAppendProvenance rows gallery).provenance ∧",
                    "      impactedKeyVisible (impactedAppendProvenance rows gallery) = true :=",
                    "  impacted_append_greater_preserves_minimum_and_visibility",
                    "    rows witness gallery minimum greater visible",
                    "",
                    f"theorem {prefix}_exact_replay_identity",
                    "    (stored replayed : ImpactedKeyRows)",
                    "    (exact : ImpactedExactReplay stored replayed) :",
                    "    replayed = stored :=",
                    "  impacted_exact_replay_is_identity stored replayed exact",
                    "",
                    f"theorem {prefix}_cleanup_no_dangling",
                    "    (rows : ImpactedKeyRows) :",
                    "    let sealRemoved := impactedRemoveSeal rows",
                    "    let witnessRemoved := impactedRemoveWitness sealRemoved",
                    "    let provenanceRemoved := impactedRemoveProvenance witnessRemoved",
                    "    let anchorRemoved := impactedRemoveAnchor provenanceRemoved",
                    "    sealRemoved.sealed = false ∧",
                    "      witnessRemoved.sealed = false ∧",
                    "      witnessRemoved.witness = none ∧",
                    "      provenanceRemoved.sealed = false ∧",
                    "      provenanceRemoved.witness = none ∧",
                    "      provenanceRemoved.provenance = [] ∧",
                    "      ImpactedNoRows anchorRemoved :=",
                    "  impacted_ordered_cleanup_has_no_dangling_rows rows",
                    "",
                ]
            )
    if analysis_candidate_contract is not None:
        lines.extend(
            [
                "/-! Unbounded GID membership and winner facts instantiated from the manifest. -/",
                "",
                "def analysisGidWinnerOrderAttributes : List String :=",
                "  " + lean_list(analysis_candidate_contract["gid_order_attributes"]),
                "",
                "theorem analysis_gid_winner_order_has_five_atomic_facts :",
                "    analysisGidWinnerOrderAttributes.length = 5 := by",
                "  native_decide",
                "",
                "theorem analysis_gid_candidate_current_group_is_functional",
                "    (metadataGid : Nat → Nat)",
                "    (galleryId leftGid rightGid : Nat)",
                "    (left : GidCandidateCurrentGroup metadataGid galleryId leftGid)",
                "    (right : GidCandidateCurrentGroup metadataGid galleryId rightGid) :",
                "    leftGid = rightGid :=",
                "  gid_candidate_current_gid_functional",
                "    metadataGid galleryId leftGid rightGid left right",
                "",
                "theorem analysis_gid_winner_selection_is_candidate",
                "    (candidates : List GidWinnerCandidate)",
                "    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)",
                "    (winner : GidWinnerCandidate)",
                "    (exact : GidWinnerExact candidates dominates winner) :",
                "    winner ∈ candidates :=",
                "  gid_winner_exact_is_member candidates dominates winner exact",
                "",
                "theorem analysis_gid_winner_selection_dominates_group",
                "    (candidates : List GidWinnerCandidate)",
                "    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)",
                "    (winner candidate : GidWinnerCandidate)",
                "    (exact : GidWinnerExact candidates dominates winner)",
                "    (member : candidate ∈ candidates) :",
                "    dominates candidate winner :=",
                "  gid_winner_exact_dominates_every_candidate",
                "    candidates dominates winner candidate exact member",
                "",
                "theorem analysis_gid_winner_selection_unique_under_stable_identity",
                "    (candidates : List GidWinnerCandidate)",
                "    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)",
                "    (left right : GidWinnerCandidate)",
                "    (leftExact : GidWinnerExact candidates dominates left)",
                "    (rightExact : GidWinnerExact candidates dominates right)",
                "    (stableIdentity :",
                "      ∀ first ∈ candidates, ∀ second ∈ candidates,",
                "        dominates first second → dominates second first →",
                "          first.galleryId = second.galleryId) :",
                "    left.galleryId = right.galleryId :=",
                "  gid_winner_exact_gallery_unique candidates dominates left right",
                "    leftExact rightExact stableIdentity",
                "",
            ]
        )
    for family in vertical_families:
        name = family["name"]
        anchor = relation_by_name[family["anchor_relation"]]
        seal = relation_by_name[family["seal_relation"]]
        view = relation_by_name[family["view_relation"]]
        semantic_attributes = unique(
            list(family["key_attributes"])
            + [
                (
                    member.get("projection_attribute", member["value_attribute"])
                    if member.get("project", False)
                    and attribute == member["value_attribute"]
                    else attribute
                )
                for member in family["members"]
                for attribute in relation_by_name[member["relation"]]["attributes"]
            ]
        )
        lines.extend(
            [
                f"def {name}_contract : VerticalFamilyContract where",
                f"  name := {lean_string(name)}",
                f"  anchorRelation := {lean_string(family['anchor_relation'])}",
                f"  sealRelation := {lean_string(family['seal_relation'])}",
                f"  keyAttributes := {lean_list(family['key_attributes'])}",
                f"  anchorAttributes := {lean_list(anchor['attributes'])}",
                f"  sealAttributes := {lean_list(seal['attributes'])}",
                f"  semanticAttributes := {lean_list(semantic_attributes)}",
                "  semanticFDs := [",
            ]
        )
        for index, fd in enumerate(family["semantic_fds"]):
            suffix = "," if index + 1 < len(family["semantic_fds"]) else ""
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
                f"  viewAttributes := {lean_list(view['attributes'])}",
                "  viewFDs := [",
            ]
        )
        for index, fd in enumerate(view.get("fds", [])):
            suffix = "," if index + 1 < len(view.get("fds", [])) else ""
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
                "  members := [",
            ]
        )
        for index, member in enumerate(family["members"]):
            member_relation = relation_by_name[member["relation"]]
            join = member["join"]
            source_relation = relation_by_name[join["source_relation"]]
            suffix = "," if index + 1 < len(family["members"]) else ""
            lines.append(
                "    { relationName := "
                + lean_string(member["relation"])
                + ", keyAttributes := "
                + lean_list(member["key_attributes"])
                + ", valueAttribute := "
                + lean_string(member["value_attribute"])
                + ", projectionAttribute := "
                + lean_string(
                    member.get("projection_attribute", member["value_attribute"])
                )
                + ", attributes := "
                + lean_list(member_relation["attributes"])
                + ", declaredKeys := "
                + lean_nested(member_relation["declared_keys"])
                + ", declaredFDs := ["
                + ", ".join(
                    "{ determinant := "
                    + lean_list(fd["determinant"])
                    + ", dependent := "
                    + lean_list(fd["dependent"])
                    + " }"
                    for fd in member_relation.get("fds", [])
                )
                + "]"
                + ", sourceRelation := "
                + lean_string(join["source_relation"])
                + ", sourceRelationAttributes := "
                + lean_list(source_relation["attributes"])
                + ", sourceAttributes := "
                + lean_list(join["source_attributes"])
                + ", memberAttributes := "
                + lean_list(join["member_attributes"])
                + ", congruenceMembers := "
                + lean_list(member.get("congruence_members", []))
                + ", project := "
                + ("true" if member["project"] else "false")
                + ", required := "
                + ("true" if member.get("required", True) else "false")
                + " }"
                + suffix
            )
        lines.append("  ]")
        optional_presence = family.get("optional_presence")
        if isinstance(optional_presence, dict):
            lines.extend(
                [
                    "  optionalPresence := some {",
                    "    memberRelation := "
                    + lean_string(optional_presence["member_relation"]),
                    "    discriminatorRelation := "
                    + lean_string(optional_presence["discriminator_relation"]),
                    "    discriminatorAttribute := "
                    + lean_string(optional_presence["discriminator_attribute"]),
                    "    presentValue := "
                    + lean_string(optional_presence["present_value"]),
                    "    absentValues := "
                    + lean_list(optional_presence["absent_values"]),
                    "  }",
                ]
            )
        else:
            lines.append("  optionalPresence := none")
        marker_relation = family.get("marker_relation")
        marker_predicate = family.get("marker_predicate")
        if isinstance(marker_relation, str):
            marker = relation_by_name[marker_relation]
            lines.extend(
                [
                    "  markerRelation := some " + lean_string(marker_relation),
                    "  markerAttributes := " + lean_list(marker["attributes"]),
                    "  markerPredicate := some " + lean_string(marker_predicate),
                ]
            )
        else:
            lines.extend(
                [
                    "  markerRelation := none",
                    "  markerAttributes := []",
                    "  markerPredicate := none",
                ]
            )
        lines.extend(
            [
                "",
                f"theorem {name}_view_projection_check :",
                f"    verticalFamilyViewProjectionCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_view_projection :",
                f"    VerticalFamilyViewProjection {name}_contract :=",
                f"  verticalFamilyViewProjectionCheck_sound {name}_contract",
                f"    {name}_view_projection_check",
                "",
                f"theorem {name}_well_formed_check :",
                f"    verticalFamilyWellFormedCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_well_formed :",
                f"    VerticalFamilyWellFormed {name}_contract :=",
                f"  verticalFamilyWellFormedCheck_sound {name}_contract",
                f"    {name}_well_formed_check",
                "",
                f"theorem {name}_lossless_join_check :",
                f"    verticalFamilyLosslessJoinCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_lossless :",
                f"    VerticalFamilyLossless {name}_contract :=",
                f"  verticalFamilyLosslessJoinCheck_sound {name}_contract",
                f"    {name}_lossless_join_check",
                "",
                f"theorem {name}_dependency_preservation_check :",
                f"    verticalFamilyDependencyPreservationCheck {name}_contract = true := by",
                "  native_decide",
                "",
                f"theorem {name}_dependency_preserving :",
                f"    VerticalFamilyDependencyPreserving {name}_contract :=",
                f"  verticalFamilyDependencyPreservationCheck_sound {name}_contract",
                f"    {name}_dependency_preservation_check",
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
    base_relations = [
        relation for relation in relations if not is_logical_view(relation)
    ]
    bcnf_prop = " ∧\n    ".join(
        f"BCNF {contract_name(relation['name'])}" for relation in base_relations
    )
    bcnf_names = [f"{relation['name']}_bcnf" for relation in base_relations]
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
            "set_option maxRecDepth 10000 in",
            "theorem all_manifest_base_relations_bcnf :",
            f"    {bcnf_prop} := by",
            "  exact ⟨" + ",\n    ".join(bcnf_names) + "⟩",
            "",
            "set_option maxRecDepth 10000 in",
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
