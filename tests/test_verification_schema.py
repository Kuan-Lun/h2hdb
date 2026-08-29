from __future__ import annotations

import importlib.util
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb.vnext_capacity import RECOMPOSED_REGISTRY_MAXIMUM_ROWS

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "verification" / "schema" / "catalog.toml"
OPERATIONAL = ROOT / "verification" / "schema" / "operational.toml"
CHECKER = ROOT / "verification" / "schema" / "check_contract.py"
CATALOG_LEAN = ROOT / "verification" / "lean" / "VNextSchema.lean"
OPERATIONAL_LEAN = ROOT / "verification" / "lean" / "OperationalSchema.lean"


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

    resolution = contract.analysis_resolution_contract
    delta_contract = contract.artifact_delta_contract
    source_locator_contract = contract.source_locator_contract
    long_value_contract = contract.long_value_storage_contract
    assert resolution is not None
    assert delta_contract is not None
    assert source_locator_contract is not None
    assert long_value_contract is not None
    assert resolution.mode == "bounded_shadow_tombstone_overlay"
    assert resolution.max_overlay_depth == 16
    assert resolution.snapshot_relation == "source_build_gallery"
    assert resolution.snapshot_resolution == "analysis_run_build_join"
    assert resolution.delta_basis == "exact_old_new_build_membership"
    assert resolution.read_resolution == "nearest_ancestor_wins"
    assert resolution.compaction == ("depth_zero_full_shadow_at_limit_or_policy_change")
    assert resolution.compaction_ancestry == "self_only"
    assert resolution.cleanup_guard == "no_reachable_descendants"
    assert resolution.cleanup_transition == (
        "remove_component_completion_seal_then_facts_then_ancestry_then_analysis_descriptor"
    )

    component_names = {
        "file_hash_decision",
        "content_owner_candidate",
        "content_owner",
        "gid_candidate",
        "gid_winner",
    }
    assert {component.name for component in resolution.components} == component_names
    assert "analysis_gallery_snapshot" not in relation_by_name
    assert "analysis_comparison_gallery" not in relation_by_name
    assert {
        "analysis_state_anchor",
        "analysis_state_ancestry",
        "gallery_observation_artist",
        "gallery_observation_file_hash_occurrence",
        "publication_candidate_base_source",
    } <= relation_by_name.keys()

    for component in resolution.components:
        shadow = relation_by_name[component.shadow_relation]
        tombstone = relation_by_name[component.tombstone_relation]
        resolved = relation_by_name[component.resolved_relation]
        assert shadow.materialization is not None
        assert tombstone.materialization is not None
        assert resolved.materialization is not None
        assert shadow.materialization["mutually_exclusive_with"] == (
            component.tombstone_relation
        )
        assert tombstone.materialization["mutually_exclusive_with"] == (
            component.shadow_relation
        )
        assert tombstone.declared_keys[0] in shadow.declared_keys
        assert shadow.declared_keys == resolved.declared_keys
        assert set(tombstone.attributes) == set(tombstone.declared_keys[0])
        assert set(resolved.attributes) == set(shadow.attributes)
        assert "resolved_analysis_id" not in resolved.attributes
        assert "ancestor_depth" not in resolved.attributes
        assert resolved.materialization["resolution"] == "minimum_ancestor_depth"
        assert resolved.materialization["max_overlay_depth"] == 16
        assert resolved.materialization["storage"] == "logical_view"

    anchor = relation_by_name["analysis_state_anchor"]
    ancestry = relation_by_name["analysis_state_ancestry"]
    seal = relation_by_name["analysis_state_component_seal"]
    assert anchor.materialization is not None
    assert ancestry.materialization is not None
    assert seal.materialization is None
    assert seal.attributes == (
        "analysis_id",
        "state_component",
        "row_count",
        "sealed_at",
    )
    assert seal.declared_keys == (frozenset({"analysis_id", "state_component"}),)
    assert not checker.bcnf_violations(seal)
    assert anchor.materialization["root_rule"] == "depth_zero_self_anchor"
    assert anchor.materialization["policy_rule"] == (
        "same_policy_or_depth_zero_compaction"
    )
    assert ancestry.materialization["ancestry_invariant"] == (
        "acyclic_depth_contiguous"
    )
    assert resolution.delta_basis == "exact_old_new_build_membership"
    assert resolution.compaction == "depth_zero_full_shadow_at_limit_or_policy_change"
    assert resolution.cleanup_guard == "no_reachable_descendants"

    observation_artist = relation_by_name["gallery_observation_artist"]
    observation_hash = relation_by_name["gallery_observation_file_hash_occurrence"]
    assert "analysis_id" not in observation_artist.attributes
    assert "analysis_id" not in observation_hash.attributes
    assert (
        frozenset({"gallery_id", "observation_id", "artist_tag_id"})
        in observation_artist.declared_keys
    )
    assert (
        frozenset({"gallery_id", "observation_id", "file_sha256"})
        in observation_hash.declared_keys
    )

    tag_term = relation_by_name["tag_term"]
    assert tag_term.attributes == ("tag_id", "namespace", "tag_value_sha256")
    assert frozenset({"namespace", "tag_value_sha256"}) in tag_term.declared_keys
    assert len({b"A", b"a", b"A "}) == 3
    assert "segment_count" in source_locator_contract.framing
    assert source_locator_contract.identity_relation == "source_locator_identity"
    assert {
        "title",
        "comment",
        "upload_account",
        "summary",
        "language",
        "locator",
    }.isdisjoint(long_value_contract.direct_payload_attributes)
    assert set(long_value_contract.direct_payload_attributes) == {
        "metadata_fingerprint",
        "cursor",
        "start_cursor",
        "next_cursor",
    }
    assert {"summary_sha256", "language_sha256"} <= set(
        long_value_contract.canonical_reference_attributes
    )
    assert (
        "artifact_storage_key_sha256"
        not in long_value_contract.canonical_reference_attributes
    )
    assert set(long_value_contract.opaque_audit_references) == {
        "analysis_snapshot_manifest.snapshot_manifest_sha256",
        "source_revision_descriptor.snapshot_manifest_sha256",
    }
    for relation_name in (
        "analysis_snapshot_manifest",
        "source_revision_descriptor",
    ):
        relation = relation_by_name[relation_name]
        assert not any(
            foreign_key.relation == "source_snapshot_manifest_identity_seal"
            for foreign_key in relation.foreign_keys
        )

    retention = contract.retention_contract
    assert retention is not None
    assert {
        "display_title_choice",
        "title_sort",
        "source_snapshot_manifest_identity_anchor",
        "source_snapshot_manifest_identity_gallery_count",
        "source_snapshot_manifest_identity_file_count",
        "source_snapshot_manifest_identity_byte_count",
        "source_snapshot_manifest_identity_seal",
    }.isdisjoint(retention.indefinitely_retained_relations)
    canonical_retention = next(
        target
        for target in contract.retention_targets
        if target.target == "CANONICAL_VALUE"
    )
    assert canonical_retention.child_phases[0][:2] == (
        "display_title_choice",
        "title_sort",
    )
    assert {
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "canonical_value_identity",
    } <= relation_by_name.keys()
    assert relation_by_name["canonical_value_identity"].attributes == (
        "value_sha256",
        "root_page_sha256",
    )
    assert relation_by_name["gallery_observation_metadata"].attributes == (
        "gallery_id",
        "observation_id",
        "gid",
        "upload_time",
        "download_time",
        "modified_time",
    )
    assert report.vertical_families == (
        "gallery_observation_file_filesystem_vertical",
        "analysis_exclusion_delta_vertical",
        "canonical_value_allocation_vertical",
        "canonical_value_page_vertical",
        "gallery_observation_page_descriptor_vertical",
        "gallery_observation_page_key_bounds_vertical",
        "gallery_observation_file_vertical",
        "analysis_file_hash_decision_shadow_vertical",
    )
    assert len(contract.vertical_families) == 8
    inline_projection_names = {
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "source_build_base_source",
        "gallery_observation_page_descriptor",
        "gallery_observation_file",
        "build_manifest",
        "analysis_exclusion_delta",
        "publication_candidate_base_source",
        "artifact_delta_new",
        "catalog_revision_generation",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
    assert {
        relation.name
        for relation in contract.relations
        if relation.materialization is not None
        and relation.materialization.get("storage") == "inline_projection"
    } == inline_projection_names
    assert report.generation_streams == ()
    assert contract.publication_commit_contract is not None
    assert len(contract.batch_receipt_projections) == 3
    for family in contract.vertical_families:
        for relation_name in (family.anchor_relation, family.seal_relation):
            base = relation_by_name[relation_name]
            non_key = set(base.attributes) - set(family.key_attributes)
            assert non_key == set()
        for member in family.members:
            base = relation_by_name[member.relation]
            non_key = set(base.attributes) - set(member.key_attributes)
            assert non_key == {member.value_attribute}
        view = relation_by_name[family.view_relation]
        assert view.materialization is not None
        expected_storage = (
            "inline_projection"
            if family.view_relation in inline_projection_names
            else "logical_view"
        )
        assert view.materialization["storage"] == expected_storage
    metadata_local = relation_by_name["gallery_observation_metadata_local"]
    assert metadata_local.attributes == (
        "gallery_id",
        "observation_id",
        "download_time",
        "modified_time",
    )
    metadata_view = relation_by_name["gallery_observation_metadata"]
    assert metadata_view.materialization is not None
    assert metadata_view.materialization["storage"] == "logical_view"
    assert metadata_view.materialization["view_pattern"] == (
        "gallery_observation_metadata_projection"
    )
    assert tuple(metadata_view.materialization["derived_from"]) == (
        "gallery_observation_metadata_local",
        "gallery_source_name_access",
        "source_gallery_name_gid",
        "gallery_upload_time",
    )
    for relation_name in (
        "gallery_observation_metadata_local",
        "gallery_observation_scan",
        "gallery_observation_directory",
        "gallery_observation_stat",
        "gallery_manifest",
        "catalog_publication_storage",
        "analysis_content_owner_candidate_shadow",
        "analysis_content_owner_shadow",
        "analysis_impacted_content",
        "analysis_impacted_gid_storage",
        "analysis_impacted_gid_provenance_storage",
    ):
        assert relation_by_name[relation_name].materialization is None or (
            relation_by_name[relation_name].materialization.get("storage")
            != "logical_view"
        )

    candidate = relation_by_name["publication_candidate"]
    candidate_base = relation_by_name["publication_candidate_base_source"]
    assert "base_source_revision" not in candidate.attributes
    assert candidate_base.attributes == (
        "candidate_id",
        "base_source_revision",
        "base_source_generation",
    )

    assert delta_contract.classification == (
        "exact_old_new_presence_and_semantic_equality"
    )
    assert delta_contract.semantic_components == (
        "source_manifest",
        "member_plan",
        "effective_content",
        "selected",
        "owner",
        "policy",
    )
    assert set(delta_contract.operations) == {
        "CREATE",
        "REBUILD",
        "DELETE",
        "UNCHANGED",
    }
    component_relation = relation_by_name[delta_contract.semantic_component_relation]
    assert component_relation.kind == "source_of_truth"
    assert component_relation.materialization is None
    assert component_relation.attributes == (
        "artifact_semantics_sha256",
        "source_manifest_component_sha256",
        "member_plan_component_sha256",
        "effective_content_component_sha256",
        "selected_component_sha256",
        "owner_component_sha256",
        "policy_component_sha256",
    )
    byte_producer = contract.artifact_byte_producer_contract
    member_plan = contract.artifact_member_plan_contract
    assert byte_producer is not None
    assert member_plan is not None
    assert byte_producer.independent_parameters == (
        "max_image_short_side",
        "producer_fingerprint_sha256",
    )
    assert "DEFLATE compression level 9" in byte_producer.algorithm_bundle
    assert any(
        "exact producer fingerprint" in value
        for value in byte_producer.algorithm_bundle
    )
    assert byte_producer.producer_equivalence_class_framing == (
        "ascii('h2hdb-vnext-artifact-producer-exact-equivalence-v1\\0') || "
        "raw32(producer_fingerprint_sha256)"
    )
    assert byte_producer.producer_equivalence_class_golden_hex == (
        "68326864622d766e6578742d61727469666163742d70726f64756365722d657861"
        "63742d6571756976616c656e63652d7631007c12521923b06e72b031807d2d2d82"
        "b5bee38afafd408595b5d29ed31cfe892c"
    )
    artifact_policy = relation_by_name[byte_producer.policy_relation]
    assert artifact_policy.attributes == (
        "policy_component_sha256",
        "max_image_short_side",
        "producer_fingerprint_sha256",
    )
    assert set(artifact_policy.declared_keys) == {
        frozenset({"policy_component_sha256"}),
        frozenset({"max_image_short_side", "producer_fingerprint_sha256"}),
    }
    producer_relation = relation_by_name[byte_producer.producer_relation]
    assert byte_producer.algorithm_attribute in producer_relation.attributes
    assert byte_producer.algorithm_attribute not in artifact_policy.attributes
    assert member_plan.component_kind == "member_plan"
    assert "excluded_flag" in member_plan.entry_fields
    assert "source_size_bytes" in member_plan.entry_fields
    assert "ZIP comment envelope" in member_plan.ready_obligation

    assert contract.scope == "catalog_data_plane"
    assert contract.excluded_operational_components
    assert {item.name for item in report.relations} == relation_by_name.keys()
    assert set(report.lossless_decompositions) == {
        decomposition.name for decomposition in contract.decompositions
    }
    assert set(report.dependency_preserving_decompositions) == {
        decomposition.name for decomposition in contract.decompositions
    }
    assert {
        "publication_candidate_and_optional_common_base",
        "catalog_publication_and_title_basis",
        "catalog_publication_and_optional_content",
        "source_build_and_optional_base_source",
    } <= set(report.lossless_decompositions)
    assert all(
        relation.materialization is not None
        for relation in contract.relations
        if relation.kind == "controlled_materialization"
    )
    assert all(
        not checker.bcnf_violations(relation)
        for relation in contract.relations
        if relation.materialization is None
        or relation.materialization.get("storage")
        not in {"logical_view", "inline_projection"}
    )
    assert checker.bcnf_violations(metadata_view)

    publication = relation_by_name["catalog_publication"]
    assert {
        "summary_sha256",
        "language_sha256",
        "modified_at",
    } <= set(publication.attributes)
    assert "published_at" not in publication.attributes
    assert "redownload_required" not in publication.attributes
    assert all(
        "redownload_required" not in decomposition.universal_attributes
        and all(
            "redownload_required" not in dependency.determinant
            and "redownload_required" not in dependency.dependent
            for dependency in decomposition.functional_dependencies
        )
        and all(
            "redownload_required" not in projection.attributes
            for projection in decomposition.projections
        )
        for decomposition in contract.decompositions
    )
    assert {"summary", "language"}.isdisjoint(publication.attributes)
    assert "source_title_sha256" not in publication.attributes
    assert (
        "source_title_sha256"
        in relation_by_name["catalog_publication_title"].attributes
    )
    assert "source_gallery_name" not in publication.attributes
    assert "sort_title" not in publication.attributes
    assert {
        frozenset({"publication_key"}),
        frozenset({"gid"}),
    } <= set(relation_by_name["publication_identity"].declared_keys)
    assert "publication_id" not in relation_by_name["publication_identity"].attributes
    assert "source_revision" not in relation_by_name["catalog_revision"].attributes
    assert "old_revision" not in relation_by_name["artifact_delta_old"].attributes


def test_capacity_plan_is_exact_and_matches_both_manifest_base_counts() -> None:
    catalog = checker.load_contract(CATALOG)
    operational = checker.load_contract(OPERATIONAL)
    plan = catalog.capacity_plan
    assert plan is not None
    assert plan.measurement_scope == (
        "decimal bytes for DATA plus indexes at peak retained state"
    )
    assert plan.newly_recomposed_relation_soft_limit_bytes == 400_000_000
    assert plan.conditional_relation_soft_limit_bytes == 1_000_000_000
    assert plan.conditional_limit_trigger_table_count == 250
    assert (plan.planning_gallery_count, plan.planning_average_files_per_gallery) == (
        300_000,
        50,
    )
    assert plan.stress_gallery_count == 1_000_000
    assert (
        plan.selected_catalog_family_count,
        plan.selected_catalog_physical_relations_before,
        plan.selected_catalog_physical_relations_after,
    ) == (30, 190, 35)
    assert (
        plan.catalog_physical_table_count_before,
        plan.catalog_physical_table_count_after,
        plan.operational_physical_table_count_before,
        plan.operational_physical_table_count_after,
        plan.total_physical_table_count_before,
        plan.total_physical_table_count_after,
    ) == (306, 151, 75, 66, 381, 217)
    assert plan.conditional_one_gigabyte_limit_required is False
    assert plan.mariadb_measurement_version == "10.11.11"
    assert plan.bounded_registry_relations == (
        "manifest_policy",
        "analysis_policy",
        "title_sort_policy",
        "display_title_policy",
        "artifact_producer_fingerprint",
        "artifact_policy_semantics",
    )
    assert (
        plan.bounded_registry_maximum_rows == RECOMPOSED_REGISTRY_MAXIMUM_ROWS == 50_000
    )
    assert len(plan.affected_catalog_relations) == 27
    assert plan.capacity_neutral_catalog_authority_substitutions == (
        "file_name_identity",
        "tag_term",
        "catalog_contributor",
    )
    assert plan.selected_catalog_family_count == len(
        plan.affected_catalog_relations
    ) + len(plan.capacity_neutral_catalog_authority_substitutions)
    assert plan.affected_operational_relations == (
        "download_generation_owner",
        "ingest_generation_owner",
        "gallery_observation_staging",
        "gallery_observation_staging_request",
        "gallery_observation_staging_request_budget",
        "cleanup_job",
        "cleanup_cycle_root",
        "cleanup_checkpoint",
    )
    categories = (
        set(plan.fixed_bootstrap_relations),
        set(plan.bounded_registry_relations),
        set(plan.lineage_current_only_relations),
        set(plan.singleton_owner_relations),
        set(plan.planning_gallery_derived_relations),
        set(plan.bounded_protocol_relations),
        set(plan.staging_capacity_relations),
    )
    assert sum(map(len, categories)) == len(set().union(*categories)) == 35
    assert set().union(*categories) == set(plan.affected_catalog_relations) | set(
        plan.affected_operational_relations
    )
    assert plan.analysis_current_chain_peak_rows == (
        plan.analysis_overlay_max_depth + 1
    )
    assert plan.analysis_latest_abandoned_peak_rows == 1
    assert (
        plan.analysis_retained_run_peak_rows
        == (
            plan.analysis_current_chain_peak_rows
            + plan.analysis_latest_abandoned_peak_rows
            + plan.analysis_working_successor_peak_rows
        )
        == 19
    )
    assert (
        plan.analysis_checkpoint_peak_rows
        == (plan.analysis_retained_run_peak_rows * plan.analysis_stage_count)
        == 285
    )
    assert plan.analysis_receipt_peak_rows == 285
    assert (
        plan.analysis_component_seal_peak_rows
        == (plan.analysis_retained_run_peak_rows * plan.analysis_component_count)
        == 95
    )
    assert plan.source_build_lineage_peak_rows == 19
    assert plan.source_revision_lineage_peak_rows == 18
    assert plan.source_snapshot_manifest_peak_rows == 18
    assert plan.publication_candidate_peak_rows == 2
    assert plan.publication_lineage_peak_rows == 18
    assert (
        plan.publication_checkpoint_peak_rows
        == (
            plan.publication_candidate_peak_rows
            * plan.publication_candidate_stage_count
        )
        == 32
    )
    assert plan.publication_receipt_peak_rows == 32
    assert plan.finalization_receipt_peak_rows == 18
    assert (
        plan.planning_source_scope_peak_rows
        == (plan.planning_gallery_count + plan.source_build_lineage_peak_rows)
        == 300_019
    )
    assert plan.source_scope_measurement_relation == "source_scope"
    assert plan.source_scope_measurement_row_count == 300_019
    assert (
        plan.staging_measurement_accepted_rows
        == (
            plan.staging_measurement_distinct_staging_ids
            * plan.staging_measurement_synthetic_rows_per_staging_id
        )
        == plan.staging_budget_maximum_rows
        == 1_500_000
    )
    assert plan.staging_over_capacity_diagnostic_rows == 1_800_000
    assert plan.staging_over_capacity_diagnostic_accepted is False
    assert plan.staging_in_band_retire_maximum_rows_per_transaction == 256
    assert plan.staging_normal_retained_terminal_gallery_maximum == 1
    budget_contract = operational.gallery_staging_request_budget_contract
    retirement_contract = operational.gallery_staging_retirement_contract
    assert budget_contract is not None
    assert retirement_contract is not None
    assert budget_contract.hard_retained_request_cap == 1_500_000
    assert budget_contract.relation == plan.staging_budget_relation
    assert budget_contract.request_relation == plan.staging_measurement_relation
    assert retirement_contract.maximum_rows_per_transaction == 256
    assert retirement_contract.maximum_terminal_stagings_per_build == 1
    assert "RETIRING_SEALED" in retirement_contract.generic_cleanup_rule
    assert "RETIRING_REUSED" in retirement_contract.generic_cleanup_rule
    staging_relation = next(
        relation
        for relation in operational.relations
        if relation.name == plan.staging_retirement_relation
    )
    assert frozenset({"build_id"}) in staging_relation.declared_keys
    budget_seeds = tuple(
        seed
        for seed in operational.bootstrap_seeds
        if seed.relation == plan.staging_budget_relation
    )
    assert tuple((seed.columns, seed.values) for seed in budget_seeds) == (
        (("singleton_id", "retained_request_count"), ("1", "0")),
    )
    assert (
        plan.cleanup_job_conservative_peak_bytes
        == (plan.cleanup_job_peak_rows * plan.cleanup_job_accounted_bytes_per_row)
        == 46_137_344
    )
    assert (
        plan.cleanup_cycle_root_conservative_peak_bytes
        == (
            plan.cleanup_cycle_root_peak_rows
            * plan.cleanup_cycle_root_accounted_bytes_per_row
        )
        == 327_680
    )
    assert (
        plan.cleanup_checkpoint_conservative_peak_bytes
        == (
            plan.cleanup_checkpoint_peak_rows
            * plan.cleanup_checkpoint_accounted_bytes_per_row
        )
        == 262_144
    )
    assert (
        plan.bounded_nonmeasured_conservative_peak_bytes
        == (
            plan.bounded_nonmeasured_peak_rows
            * plan.bounded_nonmeasured_accounted_bytes_per_row
        )
        == 285_000_000
    )
    assert (
        plan.bounded_nonmeasured_conservative_peak_bytes
        < plan.newly_recomposed_relation_soft_limit_bytes
    )
    assert plan.total_physical_table_count_after <= (
        plan.conditional_limit_trigger_table_count
    )
    assert len(catalog.decompositions) == 28
    report = checker.validate_contract(catalog)
    assert len(report.lossless_decompositions) == 28
    assert len(report.dependency_preserving_decompositions) == 28
    checker.validate_cross_manifest_contracts(catalog, operational)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        (
            "newly_recomposed_relation_soft_limit_bytes",
            400_000_001,
            "newly_recomposed_relation_soft_limit_bytes must be 400000000",
        ),
        (
            "catalog_physical_table_count_after",
            152,
            "catalog_physical_table_count_after must be 151",
        ),
        (
            "affected_operational_relations",
            ("download_generation_owner",),
            "affected_operational_relations must be",
        ),
        (
            "capacity_neutral_catalog_authority_substitutions",
            ("tag_term",),
            "capacity_neutral_catalog_authority_substitutions must be",
        ),
        (
            "conditional_one_gigabyte_limit_required",
            True,
            "1GB conditional flag",
        ),
    ),
)
def test_capacity_plan_rejects_limit_count_and_conditional_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    plan = contract.capacity_plan
    assert plan is not None
    with pytest.raises(checker.ContractValidationError, match=message):
        checker.validate_contract(
            replace(contract, capacity_plan=replace(plan, **{field: value}))
        )


def test_vertical_family_rejects_an_unsealed_member_graph() -> None:
    contract = checker.load_contract(CATALOG)
    family = contract.vertical_families[0]
    seal = next(
        relation
        for relation in contract.relations
        if relation.name == family.seal_relation
    )
    invalid_seal = replace(seal, foreign_keys=seal.foreign_keys[:-1])
    invalid = replace(
        contract,
        relations=tuple(
            invalid_seal if relation.name == invalid_seal.name else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="lacks its total-participation FK",
    ):
        checker.validate_contract(invalid)


def test_vertical_family_rejects_a_redundant_one_member_anchor_wrapper() -> None:
    contract = checker.load_contract(CATALOG)
    family = contract.vertical_families[0]
    invalid_family = replace(family, members=(family.members[0],))
    invalid = replace(
        contract,
        vertical_families=tuple(
            invalid_family if candidate is family else candidate
            for candidate in contract.vertical_families
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="one-member anchor/seal wrapper must be one atomic BCNF relation",
    ):
        checker.validate_contract(invalid)


def test_recomposed_artifact_producer_rejects_a_nonkey_determinant() -> None:
    contract = checker.load_contract(CATALOG)
    producer = next(
        relation
        for relation in contract.relations
        if relation.name == "artifact_producer_fingerprint"
    )
    invalid_producer = replace(
        producer,
        functional_dependencies=(
            *producer.functional_dependencies,
            _fd({"artifact_algorithm_version"}, {"writer_id"}),
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_producer if relation is producer else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="artifact_producer_fingerprint.*not BCNF",
    ):
        checker.validate_contract(invalid)


def test_decomposition_projection_fds_match_relation_closed_world() -> None:
    contract = checker.load_contract(CATALOG)
    decomposition = next(
        value
        for value in contract.decompositions
        if value.name == "artifact_policy_and_registered_producer"
    )
    invalid_decomposition = replace(
        decomposition,
        functional_dependencies=(
            *decomposition.functional_dependencies,
            _fd({"producer_fingerprint_sha256"}, {"max_image_short_side"}),
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="artifact_policy_semantics.*projection semantic dependency missing",
    ):
        checker.validate_contract(
            replace(
                contract,
                decompositions=tuple(
                    invalid_decomposition if value is decomposition else value
                    for value in contract.decompositions
                ),
            )
        )


def test_source_build_lifecycle_projection_rejects_optional_timestamp_drift() -> None:
    contract = checker.load_contract(CATALOG)
    view = next(
        relation for relation in contract.relations if relation.name == "source_build"
    )
    assert view.materialization is not None
    invalid_view = replace(
        view,
        materialization={
            **view.materialization,
            "timestamp_state": "OPEN",
        },
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_view if relation is view else relation
            for relation in contract.relations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="source_build.*exact descriptor/state/optional-timestamp projection",
    ):
        checker.validate_contract(invalid)


def test_build_manifest_projection_rejects_authority_source_drift() -> None:
    contract = checker.load_contract(CATALOG)
    view = next(
        relation for relation in contract.relations if relation.name == "build_manifest"
    )
    assert view.materialization is not None
    invalid_view = replace(
        view,
        materialization={
            **view.materialization,
            "derived_from": ["build_manifest_core", "source_build_discovery"],
        },
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_view if relation is view else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="build_manifest view.*exact core/discovery/lifecycle projection",
    ):
        checker.validate_contract(invalid)


def test_catalog_identities_are_bcnf_bases_and_observation_file_family_remains() -> (
    None
):
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_by_name = {family.name: family for family in contract.vertical_families}

    assert {
        "gallery_observation_file_vertical": (
            "gallery_observation_file_anchor",
            "gallery_observation_file_seal",
            "gallery_observation_file",
            ("gallery_id", "observation_id", "file_key"),
        ),
    } == {
        name: (
            family_by_name[name].anchor_relation,
            family_by_name[name].seal_relation,
            family_by_name[name].view_relation,
            family_by_name[name].key_attributes,
        )
        for name in ("gallery_observation_file_vertical",)
    }
    gallery_identity = relation_by_name["gallery_identity"]
    assert gallery_identity.kind == "source_of_truth"
    assert gallery_identity.materialization is None
    assert gallery_identity.attributes == (
        "gallery_id",
        "gallery_key",
        "scope_key",
        "locator_sha256",
    )
    assert set(gallery_identity.declared_keys) == {
        frozenset({"gallery_id"}),
        frozenset({"gallery_key"}),
        frozenset({"scope_key", "locator_sha256"}),
    }
    tag_term = relation_by_name["tag_term"]
    assert tag_term.kind == "source_of_truth"
    assert tag_term.materialization is None
    assert tag_term.attributes == ("tag_id", "namespace", "tag_value_sha256")
    assert set(tag_term.declared_keys) == {
        frozenset({"tag_id"}),
        frozenset({"namespace", "tag_value_sha256"}),
    }
    assert all(
        dependency.determinant in set(gallery_identity.declared_keys)
        for dependency in gallery_identity.functional_dependencies
    )
    file_identity = relation_by_name["file_name_identity"]
    assert file_identity.kind == "source_of_truth"
    assert file_identity.materialization is None
    assert file_identity.attributes == ("file_key", "name_bytes")
    assert set(file_identity.declared_keys) == {
        frozenset({"file_key"}),
        frozenset({"name_bytes"}),
    }
    contributor = relation_by_name["catalog_contributor"]
    assert contributor.kind == "source_of_truth"
    assert contributor.materialization is None
    assert contributor.attributes == (
        "revision",
        "publication_key",
        "contributor_name_sha256",
        "role",
        "position",
    )
    assert set(contributor.declared_keys) == {
        frozenset({"revision", "publication_key", "contributor_name_sha256", "role"}),
        frozenset({"revision", "publication_key", "position"}),
    }
    assert relation_by_name["gallery_observation_file"].attributes == (
        "gallery_id",
        "observation_id",
        "file_no",
        "file_key",
        "file_sha256",
    )
    assert relation_by_name["gallery_observation_file_file_no"].declared_keys == (
        frozenset({"gallery_id", "observation_id", "file_key"}),
        frozenset({"gallery_id", "observation_id", "file_no"}),
    )
    assert (
        "0..n-1" in family_by_name["gallery_observation_file_vertical"].write_obligation
    )

    old_views = {"gallery_observation_file"}
    assert all(
        foreign_key.relation not in old_views
        for relation in contract.relations
        for foreign_key in relation.foreign_keys
    )
    physical_domains = next(
        obligation
        for obligation in contract.semantic_obligations
        if obligation.id == "catalog.physical-domains.v1"
    )
    assert {
        "gallery_identity",
        "file_name_identity",
        "gallery_observation_file_anchor",
        "gallery_observation_file_file_no",
        "gallery_observation_file_file_sha256",
        "gallery_observation_file_seal",
        "tag_term",
    } <= set(physical_domains.relations)
    assert "gallery_observation_file" not in physical_domains.relations


def test_recomposed_lifecycle_and_build_manifest_preserve_public_read_shapes() -> None:
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_names = {family.name for family in contract.vertical_families}
    assert {
        "source_build_vertical",
        "build_manifest_vertical",
        "source_snapshot_manifest_identity_vertical",
    }.isdisjoint(family_names)

    source_descriptor = relation_by_name["source_build_descriptor"]
    source_state = relation_by_name["source_build_state"]
    source_sealed_at = relation_by_name["source_build_sealed_at"]
    assert source_descriptor.attributes == (
        "build_id",
        "scope_key",
        "manifest_policy_id",
        "created_at",
    )
    assert source_state.attributes == ("build_id", "state")
    assert source_sealed_at.attributes == ("build_id", "sealed_at")
    assert all(
        relation.materialization is None and not checker.bcnf_violations(relation)
        for relation in (source_descriptor, source_state, source_sealed_at)
    )

    source_build = relation_by_name["source_build"]
    assert relation_by_name["source_build"].attributes == (
        "build_id",
        "scope_key",
        "manifest_policy_id",
        "state",
        "created_at",
        "sealed_at",
    )
    assert source_build.materialization is not None
    assert source_build.materialization["view_pattern"] == "lifecycle_projection"
    assert tuple(source_build.materialization["derived_from"]) == (
        "source_build_descriptor",
        "source_build_state",
        "source_build_sealed_at",
    )
    assert source_build.materialization["timestamp_state"] == "SEALED"

    manifest_core = relation_by_name["build_manifest_core"]
    manifest_view = relation_by_name["build_manifest"]
    assert manifest_core.attributes == (
        "build_id",
        "manifest_sha256",
        "file_count",
        "byte_count",
    )
    assert manifest_core.materialization is None
    assert not checker.bcnf_violations(manifest_core)
    assert manifest_view.attributes == (
        "build_id",
        "manifest_sha256",
        "gallery_count",
        "file_count",
        "byte_count",
        "computed_at",
    )
    assert manifest_view.materialization is not None
    assert manifest_view.materialization["view_pattern"] == (
        "build_manifest_projection"
    )
    assert tuple(manifest_view.materialization["derived_from"]) == (
        "build_manifest_core",
        "source_build_discovery",
        "source_build_sealed_at",
    )

    snapshot_identity = relation_by_name["source_snapshot_manifest_identity"]
    assert snapshot_identity.attributes == (
        "snapshot_manifest_sha256",
        "gallery_count",
        "file_count",
        "byte_count",
    )
    assert snapshot_identity.materialization is None
    assert not checker.bcnf_violations(snapshot_identity)
    gallery_manifest = relation_by_name["gallery_manifest"]
    assert gallery_manifest.attributes == (
        "gallery_id",
        "observation_id",
        "manifest_policy_id",
        "manifest_sha256",
        "computed_at",
    )
    assert gallery_manifest.materialization is not None
    assert gallery_manifest.materialization.get("storage") != "logical_view"
    assert "gallery_manifest_vertical" not in family_names
    snapshot_contract = contract.source_snapshot_manifest_contract
    assert snapshot_contract is not None
    assert "AnalysisRepository.handoff_snapshot_manifest" in (
        snapshot_contract.handoff_obligation
    )

    read_only_views = {"source_build", "build_manifest"}
    assert all(
        foreign_key.relation not in read_only_views
        for relation in contract.relations
        for foreign_key in relation.foreign_keys
    )
    physical_domains = next(
        obligation
        for obligation in contract.semantic_obligations
        if obligation.id == "catalog.physical-domains.v1"
    )
    assert {
        "source_build_descriptor",
        "source_build_state",
        "source_build_sealed_at",
        "source_build",
        "build_manifest_core",
        "source_snapshot_manifest_identity",
    } <= set(physical_domains.relations)
    assert "build_manifest" not in physical_domains.relations


def test_b8_physical_domain_closes_the_complete_publication_graph() -> None:
    contract = checker.load_contract(CATALOG)
    relation_order = tuple(relation.name for relation in contract.relations)
    publication_start = relation_order.index("publication_candidate")
    publication_graph = frozenset(relation_order[publication_start:])
    physical_domains = next(
        obligation
        for obligation in contract.semantic_obligations
        if obligation.id == "catalog.physical-domains.v1"
    )
    inline_publication_graph = {
        "publication_candidate_base_source",
        "artifact_delta_new",
        "catalog_revision_generation",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
    assert publication_graph - inline_publication_graph <= set(
        physical_domains.relations
    )
    assert publication_graph & inline_publication_graph == inline_publication_graph
    assert inline_publication_graph.isdisjoint(physical_domains.relations)
    assert len(physical_domains.relations) == 127

    invalid_domains = replace(
        physical_domains,
        relations=tuple(
            relation
            for relation in physical_domains.relations
            if relation != "catalog_subject"
        ),
    )
    invalid = replace(
        contract,
        semantic_obligations=tuple(
            invalid_domains if obligation.id == physical_domains.id else obligation
            for obligation in contract.semantic_obligations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="physical-domain authority must close the complete publication graph",
    ):
        checker.validate_contract(invalid)


def test_generated_lean_closes_the_catalog_physical_domain_partition() -> None:
    catalog_lean = CATALOG_LEAN.read_text(encoding="utf-8")
    operational_lean = OPERATIONAL_LEAN.read_text(encoding="utf-8")

    assert "catalogPhysicalDomainContracts.length = 127" in catalog_lean
    assert "catalogPhysicalDomainMutationContracts.length = 105" in catalog_lean
    assert "catalogPhysicalDomainReadOnlyViewContracts.length = 22" in catalog_lean
    assert "catalog_physical_domain_has_no_duplicates" in catalog_lean
    assert "catalog_physical_domain_is_manifest_closed" in catalog_lean
    assert "catalog_physical_domain_partition_is_exact" in catalog_lean
    assert "catalog_physical_domain_partition_is_disjoint" in catalog_lean
    assert "catalogPhysicalDomainContracts" not in operational_lean


def test_recomposed_analysis_lifecycle_preserves_identity_and_marker_contracts() -> (
    None
):
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_by_name = {family.name: family for family in contract.vertical_families}

    assert "analysis_run_vertical" not in family_by_name
    descriptor = relation_by_name["analysis_run_descriptor"]
    assert descriptor.attributes == (
        "analysis_id",
        "build_id",
        "policy_id",
        "input_manifest_sha256",
        "started_at",
    )
    assert set(descriptor.declared_keys) == {
        frozenset({"analysis_id"}),
        frozenset({"build_id"}),
    }
    assert descriptor.materialization is None
    assert not checker.bcnf_violations(descriptor)
    assert relation_by_name["analysis_run_state"].attributes == (
        "analysis_id",
        "state",
    )
    assert relation_by_name["analysis_run_completed_at"].attributes == (
        "analysis_id",
        "completed_at",
    )
    run = relation_by_name["analysis_run"]
    assert run.materialization is not None
    assert run.materialization["view_pattern"] == "lifecycle_projection"
    assert run.materialization["timestamp_state"] == "COMPLETE"
    assert tuple(run.materialization["derived_from"]) == (
        "analysis_run_descriptor",
        "analysis_run_state",
        "analysis_run_completed_at",
    )

    component = relation_by_name["analysis_state_component_seal"]
    assert component.materialization is None
    assert component.attributes == (
        "analysis_id",
        "state_component",
        "row_count",
        "sealed_at",
    )
    assert not checker.bcnf_violations(component)
    delta = family_by_name["analysis_exclusion_delta_vertical"]
    assert delta.marker_relation == "analysis_exclusion_delta_change"
    assert delta.marker_predicate == "old_excluded != new_excluded"
    endpoint = relation_by_name["analysis_state_anchor"].materialization
    assert endpoint is not None
    assert endpoint["view_pattern"] == "analysis_ancestry_endpoint"
    run_contract = contract.analysis_run_contract
    assert run_contract is not None
    assert "fresh allocation capability" in run_contract.write_obligation
    assert "resume the already stored analysis_id" in run_contract.write_obligation
    assert "reject a different-policy sibling" in run_contract.write_obligation
    assert "ignores the retry proposal" in run_contract.attempt_rule
    assert "exact-compared on natural-key replay" not in run_contract.attempt_rule


@pytest.mark.parametrize("field", ["write_obligation", "attempt_rule"])
def test_analysis_retry_proposal_cannot_be_restored_as_authority(field: str) -> None:
    contract = checker.load_contract(CATALOG)
    run_contract = contract.analysis_run_contract
    assert run_contract is not None
    invalid = replace(run_contract, **{field: "exact-compare retry proposal"})
    with pytest.raises(
        checker.ContractValidationError,
        match="retry proposals non-authoritative after natural-key resolution",
    ):
        checker.validate_contract(replace(contract, analysis_run_contract=invalid))


def test_recomposed_analysis_descriptor_requires_the_natural_candidate_key() -> None:
    contract = checker.load_contract(CATALOG)
    descriptor = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_run_descriptor"
    )
    invalid_descriptor = replace(
        descriptor,
        declared_keys=(frozenset({"analysis_id"}),),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="analysis_run_descriptor.*omits candidate keys|descriptor has the wrong complete BCNF shape",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_descriptor if relation is descriptor else relation
                    for relation in contract.relations
                ),
            )
        )


def test_vertical_change_marker_requires_exact_relation_shape_and_predicate() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "analysis_exclusion_delta_vertical"
    )
    marker = next(
        relation
        for relation in contract.relations
        if relation.name == family.marker_relation
    )

    for invalid_family, invalid_relations, message in (
        (
            replace(family, marker_predicate=None),
            contract.relations,
            "marker relation and predicate must be declared together",
        ),
        (
            replace(family, marker_predicate="old_excluded = new_excluded"),
            contract.relations,
            "change marker must encode exact old/new inequality",
        ),
        (
            family,
            tuple(
                replace(marker, foreign_keys=()) if relation is marker else relation
                for relation in contract.relations
            ),
            "optional marker must be one PK-only anchor child",
        ),
    ):
        with pytest.raises(checker.ContractValidationError, match=message):
            checker.validate_contract(
                replace(
                    contract,
                    relations=invalid_relations,
                    vertical_families=tuple(
                        invalid_family if value is family else value
                        for value in contract.vertical_families
                    ),
                )
            )


def test_gallery_page_descriptor_family_requires_the_raw_payload_member() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "gallery_observation_page_descriptor_vertical"
    )
    invalid_family = replace(
        family,
        members=tuple(
            member
            for member in family.members
            if member.relation != "gallery_observation_page"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="seal must reference exactly its anchor and direct join members",
    ):
        checker.validate_contract(
            replace(
                contract,
                vertical_families=tuple(
                    invalid_family if value is family else value
                    for value in contract.vertical_families
                ),
            )
        )


def test_canonical_page_family_requires_the_payload_reverse_fd() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "canonical_value_page_vertical"
    )
    invalid_family = replace(
        family,
        semantic_fds=tuple(
            dependency
            for dependency in family.semantic_fds
            if dependency.determinant != frozenset({"page_bytes"})
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="member FDs are not an exact dependency-preserving cover",
    ):
        checker.validate_contract(
            replace(
                contract,
                vertical_families=tuple(
                    invalid_family if value is family else value
                    for value in contract.vertical_families
                ),
            )
        )


def test_secondary_sealed_projection_cannot_omit_a_value_source() -> None:
    contract = checker.load_contract(CATALOG)
    page = next(
        relation
        for relation in contract.relations
        if relation.name == "canonical_value_page"
    )
    assert page.materialization is not None
    invalid_page = replace(
        page,
        materialization={
            **page.materialization,
            "projection_members": ["canonical_value_page_coordinate"],
        },
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="exposes attributes not supplied by its selected members",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_page if relation is page else relation
                    for relation in contract.relations
                ),
            )
        )


def test_recomposed_manifest_policy_is_one_atomic_bcnf_registry() -> None:
    contract = checker.load_contract(CATALOG)
    policy = next(
        relation
        for relation in contract.relations
        if relation.name == "manifest_policy"
    )
    assert policy.attributes == (
        "manifest_policy_id",
        "manifest_algorithm_version",
        "file_order_version",
    )
    assert set(policy.declared_keys) == {
        frozenset({"manifest_policy_id"}),
        frozenset({"manifest_algorithm_version", "file_order_version"}),
    }
    assert not checker.bcnf_violations(policy)
    assert {
        "manifest_policy_anchor",
        "manifest_policy_identity",
        "manifest_policy_manifest_algorithm_version",
        "manifest_policy_file_order_version",
        "manifest_policy_seal",
    }.isdisjoint(relation.name for relation in contract.relations)

    invalid_policy = replace(
        policy,
        functional_dependencies=(
            *policy.functional_dependencies,
            _fd({"manifest_algorithm_version"}, {"file_order_version"}),
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="manifest_policy.*not BCNF|manifest_policy.*omits candidate keys",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_policy if relation is policy else relation
                    for relation in contract.relations
                ),
            )
        )


def test_referential_unique_key_must_strictly_contain_a_true_candidate() -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation
        for relation in contract.relations
        if relation.name == "build_manifest_core"
    )
    invalid_fact = replace(
        fact,
        referential_unique_keys=(("manifest_sha256",),),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="referential unique key.*must strictly contain a true candidate key",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_fact if relation is fact else relation
                    for relation in contract.relations
                ),
            )
        )


def test_referential_unique_key_cannot_be_declared_as_a_candidate_key() -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation
        for relation in contract.relations
        if relation.name == "build_manifest_core"
    )
    invalid_fact = replace(
        fact,
        declared_keys=(frozenset({"build_id", "manifest_sha256"}),),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="declared key.*not minimal|omits candidate keys",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_fact if relation is fact else relation
                    for relation in contract.relations
                ),
            )
        )


def test_recomposed_producer_registry_requires_collision_checked_registration() -> None:
    contract = checker.load_contract(CATALOG)
    producer = contract.artifact_byte_producer_contract
    assert producer is not None
    invalid_producer = replace(
        producer,
        runtime_obligation=producer.runtime_obligation.replace(
            "recompute the raw producer fingerprint frame and equivalence codec",
            "trust the supplied digest",
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="artifact byte-producer contract runtime obligation is incomplete",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_byte_producer_contract=invalid_producer,
            )
        )


def test_producer_equivalence_class_requires_exact_reversible_codec() -> None:
    contract = checker.load_contract(CATALOG)
    producer = contract.artifact_byte_producer_contract
    assert producer is not None
    equivalence_domain = next(
        domain
        for domain in contract.byte_domains
        if domain.attribute == "producer_equivalence_class"
    )
    equivalence_relation = next(
        relation
        for relation in contract.relations
        if relation.name == "artifact_producer_fingerprint"
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="producer equivalence-class framing drifted",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_byte_producer_contract=replace(
                    producer,
                    producer_equivalence_class_framing="caller supplied bytes",
                ),
            )
        )

    with pytest.raises(
        checker.ContractValidationError,
        match="byte domain 'producer_equivalence_class' lacks its exact codec",
    ):
        checker.validate_contract(
            replace(
                contract,
                byte_domains=tuple(
                    (
                        replace(domain, runtime_obligation="accept bounded bytes")
                        if domain is equivalence_domain
                        else domain
                    )
                    for domain in contract.byte_domains
                ),
            )
        )

    with pytest.raises(
        checker.ContractValidationError,
        match="producer registry keys are incomplete|omits candidate keys",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    (
                        replace(
                            relation,
                            declared_keys=(frozenset({"producer_fingerprint_sha256"}),),
                        )
                        if relation is equivalence_relation
                        else relation
                    )
                    for relation in contract.relations
                ),
            )
        )


@pytest.mark.parametrize(
    "contract_field",
    ("gallery_observation_identity_contract", "title_sort_contract"),
)
def test_catalog_requires_new_identity_and_sort_contracts(contract_field: str) -> None:
    contract = checker.load_contract(CATALOG)
    invalid = replace(contract, **{contract_field: None})

    with pytest.raises(
        checker.ContractValidationError,
        match=rf"catalog data-plane contract must declare {contract_field}",
    ):
        checker.validate_contract(invalid)


def test_source_locator_contract_rejects_unframed_nested_path() -> None:
    contract = checker.load_contract(CATALOG)
    source_locator_contract = contract.source_locator_contract
    assert source_locator_contract is not None

    invalid = replace(
        contract,
        source_locator_contract=replace(
            source_locator_contract,
            framing="segment_utf8",
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="source locator contract framing must be",
    ):
        checker.validate_contract(invalid)


def test_source_scope_retention_uses_the_root_fact_not_scope_key() -> None:
    contract = checker.load_contract(CATALOG)
    source_scope = contract.source_scope_identity_contract
    assert source_scope is not None
    for required_term in (
        "canonical_value_allocation_anchor.value_sha256",
        "never by treating scope_key as a canonical allocation key",
        "every same-root scope row",
    ):
        invalid_scope = replace(
            source_scope,
            seal_obligation=source_scope.seal_obligation.replace(required_term, ""),
        )
        with pytest.raises(
            checker.ContractValidationError,
            match="collision and build-seal obligations",
        ):
            checker.validate_contract(
                replace(contract, source_scope_identity_contract=invalid_scope)
            )


def test_source_locator_contract_requires_streaming_write_and_exact_eof_receipt() -> (
    None
):
    contract = checker.load_contract(CATALOG)
    locator = contract.source_locator_contract
    assert locator is not None
    mutations = (
        replace(
            locator,
            write_obligation=locator.write_obligation.replace(
                "iter_source_relative_locator_payload", ""
            ),
        ),
        replace(
            locator,
            read_obligation=locator.read_obligation.replace(
                "validate_source_relative_locator_parts", ""
            ),
        ),
    )
    for mutation in mutations:
        with pytest.raises(
            checker.ContractValidationError,
            match="must state non-empty read and write obligations",
        ):
            checker.validate_contract(
                replace(contract, source_locator_contract=mutation)
            )


def test_high_cardinality_canonical_codecs_require_bounded_streaming_paths() -> None:
    contract = checker.load_contract(CATALOG)
    effective = contract.effective_content_contract
    snapshot = contract.source_snapshot_manifest_contract
    member_plan = contract.artifact_member_plan_contract
    assert effective is not None
    assert snapshot is not None
    assert member_plan is not None

    mutations = (
        (
            "effective_content_contract",
            replace(
                effective,
                write_obligation=effective.write_obligation.replace(
                    "effective_content_digest_ordered", ""
                ),
            ),
            "effective content contract must state exact preimage",
        ),
        (
            "source_snapshot_manifest_contract",
            replace(
                snapshot,
                write_obligation=snapshot.write_obligation.replace(
                    "iter_source_snapshot_manifest_payload_rows_ordered", ""
                ),
            ),
            "source snapshot manifest contract write validation is incomplete",
        ),
        (
            "artifact_member_plan_contract",
            replace(
                member_plan,
                runtime_obligation=member_plan.runtime_obligation.replace(
                    "artifact_member_plan_digest_ordered", ""
                ),
            ),
            "artifact member-plan contract runtime construction obligation is incomplete",
        ),
    )
    for field_name, mutation, message in mutations:
        with pytest.raises(checker.ContractValidationError, match=message):
            checker.validate_contract(replace(contract, **{field_name: mutation}))


def test_analysis_candidate_contract_is_one_closed_executable_v1_codec() -> None:
    contract = checker.load_contract(CATALOG)
    candidate = contract.analysis_candidate_contract
    assert candidate is not None
    mutations = (
        replace(
            candidate,
            gid_order_attributes=(*candidate.gid_order_attributes[:-1], "gid"),
        ),
        replace(
            candidate,
            content_ordering_rule=candidate.content_ordering_rule.replace(
                "greatest", "least"
            ),
        ),
        replace(
            candidate,
            already_uploaded_marker_rule=candidate.already_uploaded_marker_rule.replace(
                "ASCII A-Z", "Unicode casefold"
            ),
        ),
        replace(
            candidate,
            gid_winner_selection_relation="analysis_gid_winner_shadow",
        ),
        replace(
            candidate,
            gid_ordering_rule=candidate.gid_ordering_rule.replace("greatest", "least"),
        ),
        replace(
            candidate,
            runtime_obligation=candidate.runtime_obligation.replace(
                "five-field maximum", "caller comparator bytes"
            ),
        ),
    )
    for mutation in mutations:
        with pytest.raises(
            checker.ContractValidationError,
            match="analysis candidate contract must equal the closed executable v1 codec",
        ):
            checker.validate_contract(
                replace(contract, analysis_candidate_contract=mutation)
            )


@pytest.mark.parametrize(
    "removed_source",
    (
        "analysis_impacted_gallery",
        "analysis_run_descriptor",
        "analysis_policy",
        "source_build_gallery",
        "gallery_observation_file",
        "file_name_identity",
        "analysis_file_hash_decision_resolved",
        "gallery_observation_tag",
        "tag_term",
        "canonical_value_identity",
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "gallery_observation_tree_root",
        "gallery_observation_page_child",
        "gallery_observation_metadata_local",
    ),
)
def test_content_candidate_fact_authority_is_closed_world(
    removed_source: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_content_owner_candidate_shadow"
    )
    metadata = dict(fact.materialization or {})
    metadata["derived_from"] = [
        source for source in metadata["derived_from"] if source != removed_source
    ]
    mutation = replace(fact, materialization=metadata)
    with pytest.raises(
        checker.ContractValidationError,
        match="must retain its exact current-observation authority and replay path",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    mutation if relation is fact else relation
                    for relation in contract.relations
                ),
            )
        )


@pytest.mark.parametrize(
    ("authority_text", "invalid_text"),
    (
        (
            "stream and validate every comparator input",
            "trust caller comparator input",
        ),
        (
            "insert the complete row atomically",
            "insert partial facts independently",
        ),
        (
            "exact-compare all values on replay",
            "overwrite values on replay",
        ),
    ),
)
def test_content_candidate_fact_authority_wording_is_executable(
    authority_text: str,
    invalid_text: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_content_owner_candidate_shadow"
    )
    metadata = dict(fact.materialization or {})
    metadata["refresh_strategy"] = str(metadata["refresh_strategy"]).replace(
        authority_text,
        invalid_text,
    )
    mutation = replace(fact, materialization=metadata)
    with pytest.raises(
        checker.ContractValidationError,
        match="must retain its exact current-observation authority and replay path",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    mutation if relation is fact else relation
                    for relation in contract.relations
                ),
            )
        )


def test_impacted_key_contract_rejects_unbounded_or_inexact_provenance() -> None:
    contract = checker.load_contract(CATALOG)
    impacted = contract.analysis_impacted_key_contract
    assert impacted is not None

    with pytest.raises(
        checker.ContractValidationError,
        match="128-gallery/257-row MIN-witness v1 protocol",
    ):
        checker.validate_contract(
            replace(
                contract,
                analysis_impacted_key_contract=replace(
                    impacted, maximum_provenance_rows=256
                ),
            )
        )

    content_family = next(
        family for family in impacted.families if family.name == "content"
    )
    invalid_family = replace(
        content_family,
        witness_fk_attributes=("analysis_id", "gallery_id", "content_sha256"),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="family registry must equal content plus GID exactly",
    ):
        checker.validate_contract(
            replace(
                contract,
                analysis_impacted_key_contract=replace(
                    impacted,
                    families=tuple(
                        invalid_family if family is content_family else family
                        for family in impacted.families
                    ),
                ),
            )
        )

    provenance = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_impacted_content_provenance"
    )
    without_gallery_scope = replace(
        provenance,
        foreign_keys=tuple(
            foreign_key
            for foreign_key in provenance.foreign_keys
            if foreign_key.relation != "analysis_impacted_gallery"
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="provenance must be one FD-free analysis/gallery/key mapping",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    without_gallery_scope if relation is provenance else relation
                    for relation in contract.relations
                ),
            )
        )

    for required_authority in (
        "analysis_baseline",
        "analysis_run_descriptor",
        "file_name_identity",
        "analysis_policy",
    ):
        missing_authority_metadata = dict(provenance.materialization or {})
        missing_authority_metadata["derived_from"] = [
            source
            for source in missing_authority_metadata["derived_from"]
            if source != required_authority
        ]
        without_authority = replace(
            provenance, materialization=missing_authority_metadata
        )
        with pytest.raises(
            checker.ContractValidationError,
            match="provenance authority/replay rule drifts",
        ):
            checker.validate_contract(
                replace(
                    contract,
                    relations=tuple(
                        without_authority if relation is provenance else relation
                        for relation in contract.relations
                    ),
                )
            )

    for authority_text, invalid_text in (
        ("analysis_baseline.base_analysis_id", "current analysis_id"),
        ("current analysis_run_descriptor", "caller build_id"),
        ("file_name_identity.name_bytes classifier", "untyped file name"),
        ("sealed analysis_policy", "unsealed policy"),
    ):
        invalid_metadata = dict(provenance.materialization or {})
        invalid_metadata["refresh_strategy"] = str(
            invalid_metadata["refresh_strategy"]
        ).replace(authority_text, invalid_text)
        invalid_authority = replace(provenance, materialization=invalid_metadata)
        with pytest.raises(
            checker.ContractValidationError,
            match="does not separate immutable old/new authority",
        ):
            checker.validate_contract(
                replace(
                    contract,
                    relations=tuple(
                        invalid_authority if relation is provenance else relation
                        for relation in contract.relations
                    ),
                )
            )


def test_impacted_key_contract_rejects_witness_mutation_and_reversed_cleanup() -> None:
    contract = checker.load_contract(CATALOG)
    witness = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_impacted_content"
    )
    witness_metadata = dict(witness.materialization or {})
    witness_metadata["refresh_strategy"] = witness_metadata["refresh_strategy"].replace(
        "minimum", "maximum"
    )
    mutable_witness = replace(witness, materialization=witness_metadata)
    with pytest.raises(
        checker.ContractValidationError,
        match="base does not pin immutable MIN semantics",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    mutable_witness if relation is witness else relation
                    for relation in contract.relations
                ),
            )
        )

    analysis = next(
        target
        for target in contract.retention_targets
        if target.target == "ANALYSIS_RUN"
    )
    phases = list(analysis.child_phases)
    base_phase = next(
        index
        for index, phase in enumerate(phases)
        if "analysis_impacted_content" in phase
    )
    provenance_phase = next(
        index
        for index, phase in enumerate(phases)
        if "analysis_impacted_content_provenance" in phase
    )
    phases[base_phase], phases[provenance_phase] = (
        phases[provenance_phase],
        phases[base_phase],
    )
    reversed_cleanup = replace(analysis, child_phases=tuple(phases))
    with pytest.raises(
        checker.ContractValidationError,
        match="cleanup must delete the materialized key/witness row before its provenance row",
    ):
        checker.validate_contract(
            replace(
                contract,
                retention_targets=tuple(
                    reversed_cleanup if target is analysis else target
                    for target in contract.retention_targets
                ),
            )
        )


def test_effective_content_contract_rejects_caller_authority_or_unsealed_handoff() -> (
    None
):
    contract = checker.load_contract(CATALOG)
    effective = contract.effective_content_contract
    assert effective is not None
    mutations = (
        replace(
            effective,
            write_obligation=effective.write_obligation.replace(
                "database-owned private typed EffectiveContentPreparation",
                "caller-supplied digest receipt",
            ),
        ),
        replace(
            effective,
            write_obligation=effective.write_obligation.replace(
                "then deletes that claim", "leaves that claim reusable"
            ),
        ),
        replace(
            effective,
            write_obligation=effective.write_obligation.replace(
                "outside every canonical page transaction",
                "inside one unbounded transaction",
            ),
        ),
    )
    for mutation in mutations:
        with pytest.raises(
            checker.ContractValidationError,
            match="effective content contract must state exact preimage",
        ):
            checker.validate_contract(
                replace(contract, effective_content_contract=mutation)
            )


def test_long_value_boundary_rejects_direct_payload_key_promotion() -> None:
    contract = checker.load_contract(CATALOG)
    metadata = next(
        relation
        for relation in contract.relations
        if relation.name == "gallery_observation_discovery_fingerprint"
    )
    invalid_metadata = replace(
        metadata,
        declared_keys=(
            *metadata.declared_keys,
            frozenset({"metadata_fingerprint"}),
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_metadata if relation.name == metadata.name else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="direct payload 'metadata_fingerprint' is illegally used as a key",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize(
    ("relation_name", "attribute"),
    (
        ("catalog_publication_storage", "summary_sha256"),
        ("catalog_publication_storage", "language_sha256"),
    ),
)
def test_unbounded_publication_values_require_canonical_fks(
    relation_name: str,
    attribute: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    relation = next(item for item in contract.relations if item.name == relation_name)
    invalid_relation = replace(
        relation,
        foreign_keys=tuple(
            foreign_key
            for foreign_key in relation.foreign_keys
            if attribute not in foreign_key.attributes
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_relation if item.name == relation_name else item
            for item in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="canonical-reference role|does not reference exact canonical bytes",
    ):
        checker.validate_contract(invalid)


def test_publication_locator_requires_bounded_exact_streaming_codec() -> None:
    contract = checker.load_contract(CATALOG)
    publication = contract.publication_atomic_contract
    assert publication is not None
    for term in (
        "at most 4096 bytes",
        "iter_artifact_storage_key_payload",
        "exact EOF",
    ):
        invalid = replace(
            contract,
            publication_atomic_contract=replace(
                publication,
                runtime_obligation=publication.runtime_obligation.replace(term, ""),
            ),
        )
        with pytest.raises(
            checker.ContractValidationError,
            match="omits a required READY/CAS validation obligation",
        ):
            checker.validate_contract(invalid)


def test_catalog_child_count_is_not_publication_count_authority() -> None:
    contract = checker.load_contract(CATALOG)
    publication = contract.publication_atomic_contract
    assert publication is not None
    validation_stage = next(
        stage
        for stage in publication.batch_stages
        if stage.name == "VALIDATE_CATALOG_PROJECTION"
    )
    assert validation_stage.sealed_scalar == "NONE"
    assert "publication_count only from terminal VALIDATE_SELECTION" in (
        publication.projection_seal_rule
    )

    drifted_stage = replace(
        validation_stage,
        sealed_scalar="publication_count_crosscheck",
    )
    invalid_registry = replace(
        contract,
        publication_atomic_contract=replace(
            publication,
            batch_stages=tuple(
                drifted_stage if stage is validation_stage else stage
                for stage in publication.batch_stages
            ),
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="stage/order/codec/prerequisite registry drifts",
    ):
        checker.validate_contract(invalid_registry)

    invalid_rule = replace(
        contract,
        publication_atomic_contract=replace(
            publication,
            projection_seal_rule=(
                publication.projection_seal_rule
                + "; VALIDATE_CATALOG_PROJECTION processed_count equals "
                "VALIDATE_SELECTION processed_count"
            ),
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="must not equate catalog-child count with publication count",
    ):
        checker.validate_contract(invalid_rule)


def test_publication_receipt_requires_authoritative_count_seal() -> None:
    contract = checker.load_contract(CATALOG)
    receipt = next(
        relation
        for relation in contract.relations
        if relation.name == "publication_receipt"
    )
    invalid_receipt = replace(
        receipt,
        attributes=tuple(
            attribute
            for attribute in receipt.attributes
            if attribute != "publication_count"
        ),
        functional_dependencies=tuple(
            replace(
                fd,
                dependent=frozenset(
                    attribute
                    for attribute in fd.dependent
                    if attribute != "publication_count"
                ),
            )
            for fd in receipt.functional_dependencies
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_receipt if relation is receipt else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=r"requires an O\(1\) authoritative publication_count",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize("mutation", ("missing_seal", "candidate_fk", "missing_key"))
def test_publication_receipt_uses_only_sealed_common_commit_authority(
    mutation: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    receipt = next(
        relation
        for relation in contract.relations
        if relation.name == "publication_receipt"
    )
    assert "reserved_revision" not in receipt.attributes
    if mutation == "missing_seal":
        invalid_receipt = replace(
            receipt,
            foreign_keys=tuple(
                foreign_key
                for foreign_key in receipt.foreign_keys
                if foreign_key.relation != "publication_commit"
            ),
        )
    elif mutation == "candidate_fk":
        invalid_receipt = replace(
            receipt,
            foreign_keys=(
                *receipt.foreign_keys,
                checker.ForeignKey(
                    ("revision",),
                    "publication_candidate",
                    ("reserved_revision",),
                ),
            ),
        )
    else:
        invalid_receipt = replace(
            receipt,
            declared_keys=tuple(
                key
                for key in receipt.declared_keys
                if key != frozenset({"source_revision"})
            ),
        )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_receipt if relation is receipt else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="receipt must remove reserved_revision.*sealed common commit",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize(
    ("obligation", "required_term", "error"),
    (
        (
            "runtime_obligation",
            "sole complete immutable commit authority",
            "hot-path obligation",
        ),
        (
            "runtime_obligation",
            "replay is candidate/preparation-key local O(1)",
            "hot-path obligation",
        ),
        (
            "runtime_obligation",
            "operational_activation is a derived read-only inline projection",
            "hot-path obligation",
        ),
        ("runtime_obligation", "append-only", "hot-path obligation"),
        (
            "ready_obligation",
            "retained current/reachable publication window",
            "full READY/quick readiness distinction",
        ),
        (
            "ready_obligation",
            "no fork/orphan/gap after the compacted floor",
            "full READY/quick readiness distinction",
        ),
        (
            "ready_obligation",
            "unique maximum no-successor head",
            "full READY/quick readiness distinction",
        ),
        (
            "ready_obligation",
            "quick check_readiness remains epoch-only O(1)",
            "full READY/quick readiness distinction",
        ),
    ),
)
def test_publication_commit_contract_distinguishes_local_runtime_and_full_ready(
    obligation: str,
    required_term: str,
    error: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    commit_contract = contract.publication_commit_contract
    assert commit_contract is not None
    invalid = replace(
        contract,
        publication_commit_contract=replace(
            commit_contract,
            **{
                obligation: getattr(commit_contract, obligation).replace(
                    required_term, "missing requirement"
                )
            },
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=error,
    ):
        checker.validate_contract(invalid)


def test_common_head_cannot_bypass_the_sealed_publication_commit() -> None:
    contract = checker.load_contract(CATALOG)
    head = next(
        relation
        for relation in contract.relations
        if relation.name == "publication_commit_head_receipt"
    )
    invalid_head = replace(
        head,
        foreign_keys=tuple(
            (
                checker.ForeignKey(
                    foreign_key.attributes,
                    "publication_commit_anchor",
                    foreign_key.referenced_attributes,
                )
                if foreign_key.relation == "publication_commit"
                else foreign_key
            )
            for foreign_key in head.foreign_keys
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_head if relation is head else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="one common sealed receipt head",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize("determinant", ("terminal", "next_state"))
@pytest.mark.parametrize(
    "relation_name",
    ("analysis_batch_receipt", "publication_batch_receipt"),
)
def test_nonterminal_batch_state_does_not_determine_row_count(
    relation_name: str,
    determinant: str,
) -> None:
    # Concrete counterexample: distinct positive row counts have the same
    # terminal flag and the same derived OPEN state.
    first_row_count: int = 1
    second_row_count: int = 2
    assert (first_row_count == 0) is (second_row_count == 0) is False
    assert ("COMPLETE" if first_row_count == 0 else "OPEN") == (
        "COMPLETE" if second_row_count == 0 else "OPEN"
    )

    contract = checker.load_contract(CATALOG)
    receipt = next(
        relation for relation in contract.relations if relation.name == relation_name
    )
    invalid_receipt = replace(
        receipt,
        functional_dependencies=(
            *receipt.functional_dependencies,
            _fd({determinant}, {"row_count"}),
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_receipt if relation is receipt else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="falsely treats nonterminal state as determining row_count",
    ):
        checker.validate_contract(invalid)


def test_analysis_snapshot_manifest_digest_is_deliberately_nonunique() -> None:
    contract = checker.load_contract(CATALOG)
    binding = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_snapshot_manifest"
    )
    invalid_binding = replace(
        binding,
        declared_keys=(
            *binding.declared_keys,
            frozenset({"snapshot_manifest_sha256"}),
        ),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_binding if relation is binding else relation
            for relation in contract.relations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="analysis output binding must be one-way BCNF opaque audit",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize(
    ("relation_name", "removed_attribute", "error_match"),
    (
        (
            "publication_candidate_projection_seal",
            "new_galleries",
            "projection must be a PK-only certification seal",
        ),
        (
            "publication_checkpoint",
            "processed_count",
            "checkpoint lacks processed-count terminal authority",
        ),
        (
            "publication_batch_receipt",
            "start_processed_count",
            "batch receipt lacks exact replay response authority",
        ),
    ),
)
def test_publication_projection_authority_shapes_fail_closed(
    relation_name: str,
    removed_attribute: str,
    error_match: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    relation = next(
        value for value in contract.relations if value.name == relation_name
    )
    if relation_name == "publication_candidate_projection_seal":
        invalid_relation = replace(
            relation,
            attributes=(*relation.attributes, removed_attribute),
            functional_dependencies=(_fd({"candidate_id"}, {removed_attribute}),),
        )
    else:
        invalid_relation = replace(
            relation,
            attributes=tuple(
                attribute
                for attribute in relation.attributes
                if attribute != removed_attribute
            ),
            functional_dependencies=tuple(
                replace(
                    dependency,
                    dependent=frozenset(
                        attribute
                        for attribute in dependency.dependent
                        if attribute != removed_attribute
                    ),
                )
                for dependency in relation.functional_dependencies
            ),
        )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_relation if value is relation else value
            for value in contract.relations
        ),
    )
    with pytest.raises(checker.ContractValidationError, match=error_match):
        checker.validate_contract(invalid)


def test_publication_requires_bcnf_contiguous_order_projection() -> None:
    contract = checker.load_contract(CATALOG)
    invalid = replace(
        contract,
        relations=tuple(
            relation
            for relation in contract.relations
            if relation.name != "catalog_publication_order"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="requires a BCNF catalog_publication_order projection",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize(
    ("attribute", "wrong_maximum"),
    (
        ("metadata_fingerprint", 41),
        ("cursor", 2049),
        ("protection_token", 513),
        ("producer_equivalence_class", 84),
    ),
)
def test_direct_payload_bounds_are_closed_world(
    attribute: str,
    wrong_maximum: int,
) -> None:
    contract = checker.load_contract(CATALOG)
    invalid = replace(
        contract,
        byte_domains=tuple(
            (
                replace(domain, maximum_bytes=wrong_maximum)
                if domain.attribute == attribute
                else domain
            )
            for domain in contract.byte_domains
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=rf"byte domain '{attribute}' maximum_bytes must be",
    ):
        checker.validate_contract(invalid)


def test_source_root_contract_rejects_codec_and_golden_drift() -> None:
    contract = checker.load_contract(CATALOG)
    source_root = contract.source_root_contract
    assert source_root is not None
    assert source_root.golden_root_payload_hex == "0000000100000000"
    assert source_root.golden_root_sha256 == (
        "25d5c20861a8646652543d9727df88fbef23e53d6ef050b04d1ae7199cbdf75a"
    )
    for changed in (
        replace(source_root, codec_version=2),
        replace(source_root, golden_root_sha256="00" * 32),
        replace(source_root, segment_rule="strict UTF-8"),
        replace(
            source_root,
            write_obligation=source_root.write_obligation.replace(
                "validate_source_root_parts", ""
            ),
        ),
    ):
        with pytest.raises(
            checker.ContractValidationError, match="source root contract"
        ):
            checker.validate_contract(replace(contract, source_root_contract=changed))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("directory_leaf_capacity", 256),
        ("metadata_chunk_maximum_bytes", 65536),
        ("components", ("FILE", "TAG", "DIRECTORY")),
        ("maximum_items", 1 << 63),
    ],
)
def test_observation_page_contract_rejects_capacity_component_and_int63_drift(
    field_name: str,
    value: object,
) -> None:
    contract = checker.load_contract(CATALOG)
    pages = contract.gallery_observation_page_contract
    assert pages is not None
    invalid_pages = replace(pages, **{field_name: value})

    with pytest.raises(
        checker.ContractValidationError,
        match="gallery observation page contract",
    ):
        checker.validate_contract(
            replace(contract, gallery_observation_page_contract=invalid_pages)
        )


@pytest.mark.parametrize(
    ("field_name", "required_term", "error"),
    [
        (
            "canonical_tree_rule",
            "zero-based contiguous",
            "canonical tree boundary rules are incomplete",
        ),
        (
            "materialization_rule",
            "unsigned bytewise first_key less than or equal to last_key",
            "exact decode materialization rule is incomplete",
        ),
        (
            "materialization_rule",
            "exact zero-based contiguous file_no",
            "exact decode materialization rule is incomplete",
        ),
        (
            "collision_obligation",
            "zero durable writes",
            "collision mismatch rule is incomplete",
        ),
        (
            "seal_obligation",
            "all semantic page readers require that descriptor seal",
            "final seal rule is incomplete",
        ),
        (
            "cleanup_rule",
            "descriptor seal, descriptor facts, raw page payload, and descriptor anchor",
            "cleanup rule omits",
        ),
    ],
)
def test_observation_page_vertical_contract_rejects_missing_family_semantics(
    field_name: str,
    required_term: str,
    error: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    pages = contract.gallery_observation_page_contract
    assert pages is not None
    invalid_pages = replace(
        pages,
        **{
            field_name: getattr(pages, field_name).replace(required_term, ""),
        },
    )

    with pytest.raises(checker.ContractValidationError, match=error):
        checker.validate_contract(
            replace(contract, gallery_observation_page_contract=invalid_pages)
        )


def test_page_descriptors_reject_hidden_fd_fields() -> None:
    contract = checker.load_contract(CATALOG)
    for relation_name in (
        "canonical_value_page_descriptor",
        "gallery_observation_page_descriptor",
    ):
        relation = next(
            item for item in contract.relations if item.name == relation_name
        )
        invalid_relation = replace(
            relation,
            attributes=(*relation.attributes, "node_kind", "entry_count"),
        )
        invalid = replace(
            contract,
            relations=tuple(
                invalid_relation if item is relation else item
                for item in contract.relations
            ),
        )
        with pytest.raises(
            checker.ContractValidationError,
            match="wrong normalized shape",
        ):
            checker.validate_contract(invalid)


def test_canonical_value_graph_rejects_monolithic_payload_shape() -> None:
    contract = checker.load_contract(CATALOG)
    identity = next(
        item for item in contract.relations if item.name == "canonical_value_identity"
    )
    invalid_identity = replace(
        identity,
        attributes=(*identity.attributes, "value_bytes"),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_identity if item is identity else item
            for item in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="canonical digest contract relation 'canonical_value_identity'.*wrong normalized shape",
    ):
        checker.validate_contract(invalid)


def test_relation_rejects_duplicate_attributes_before_set_reasoning() -> None:
    contract = checker.load_contract(CATALOG)
    publication = next(
        relation
        for relation in contract.relations
        if relation.name == "catalog_publication"
    )
    invalid_publication = replace(
        publication,
        attributes=(*publication.attributes, "publication_key"),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_publication if relation is publication else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(checker.ContractValidationError, match="duplicate attributes"):
        checker.validate_contract(invalid)


def test_catalog_publication_rejects_mutable_redownload_state() -> None:
    contract = checker.load_contract(CATALOG)
    publication = next(
        relation
        for relation in contract.relations
        if relation.name == "catalog_publication"
    )
    invalid_publication = replace(
        publication,
        attributes=(*publication.attributes, "redownload_required"),
    )
    invalid = replace(
        contract,
        relations=tuple(
            invalid_publication if relation is publication else relation
            for relation in contract.relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="must exclude mutable redownload queue state",
    ):
        checker.validate_contract(invalid)


def test_analysis_resolution_contract_rejects_recursive_baseline_reads() -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    invalid = replace(
        contract,
        analysis_resolution_contract=replace(
            resolution,
            read_resolution="recursive_baseline_chain",
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="read_resolution must be 'nearest_ancestor_wins'",
    ):
        checker.validate_contract(invalid)


def test_analysis_resolution_contract_rejects_unbounded_ancestry() -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    invalid = replace(
        contract,
        analysis_resolution_contract=replace(resolution, max_overlay_depth=17),
    )

    with pytest.raises(
        checker.ContractValidationError, match="does not enforce max overlay depth"
    ):
        checker.validate_contract(invalid)


def test_analysis_resolution_contract_rejects_missing_mutual_exclusion() -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    component = resolution.components[0]
    relations = tuple(
        (
            replace(
                relation,
                materialization={
                    **(relation.materialization or {}),
                    "mutually_exclusive_with": "wrong_relation",
                },
            )
            if relation.name == component.tombstone_relation
            else relation
        )
        for relation in contract.relations
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="does not require shadow/tombstone mutual exclusion",
    ):
        checker.validate_contract(replace(contract, relations=relations))


def test_analysis_resolution_contract_rejects_cross_policy_inheritance() -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    relations = tuple(
        (
            replace(
                relation,
                materialization={
                    **(relation.materialization or {}),
                    "policy_rule": "inherit_any_policy",
                },
            )
            if relation.name == resolution.anchor_relation
            else relation
        )
        for relation in contract.relations
    )

    with pytest.raises(
        checker.ContractValidationError, match="permits cross-policy inheritance"
    ):
        checker.validate_contract(replace(contract, relations=relations))


def test_analysis_resolution_contract_rejects_unsafe_ancestor_cleanup() -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    invalid = replace(
        contract,
        analysis_resolution_contract=replace(
            resolution, cleanup_guard="ignore_descendants"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="cleanup_guard must be 'no_reachable_descendants'",
    ):
        checker.validate_contract(invalid)


def _replace_retention_target(
    contract: Any, target_name: str, **changes: object
) -> Any:
    target = next(
        item for item in contract.retention_targets if item.target == target_name
    )
    changed = replace(target, **changes)
    return replace(
        contract,
        retention_targets=tuple(
            changed if item is target else item for item in contract.retention_targets
        ),
    )


def test_retention_v2_rejects_legacy_obligation_and_history_roots() -> None:
    contract = checker.load_contract(CATALOG)
    retention = contract.retention_contract
    assert retention is not None
    assert retention.version == 2
    assert retention.semantic_obligation_id == "catalog.retention.v2"
    assert retention.analysis_relation == "analysis_run_descriptor"
    assert retention.build_relation == "source_build_descriptor"

    invalid = replace(
        contract,
        retention_contract=replace(
            retention,
            version=1,
            semantic_obligation_id="catalog.retention.v1",
            analysis_relation="analysis_run",
            build_relation="source_build",
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="retention contract must be version 2|catalog.retention.v2",
    ):
        checker.validate_contract(invalid)


def test_retention_v2_rejects_a_legacy_machine_gate_binding() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(item for item in contract.retention_targets if item.machine_gates)
    legacy_gate = replace(
        target.machine_gates[0],
        semantic_obligation_id="catalog.retention.v1",
    )
    invalid = _replace_retention_target(
        contract,
        target.target,
        machine_gates=(legacy_gate, *target.machine_gates[1:]),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="machine gate.*lacks its resolver|machine gates must be exactly",
    ):
        checker.validate_contract(invalid)


def test_retention_contract_rejects_unknown_root_relation() -> None:
    contract = checker.load_contract(CATALOG)
    invalid = _replace_retention_target(
        contract, "SOURCE_BUILD", root_relation="source_buid_typo"
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="references unknown root relation 'source_buid_typo'",
    ):
        checker.validate_contract(invalid)


def test_retention_contract_rejects_misspelled_phase_relation() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item for item in contract.retention_targets if item.target == "SOURCE_BUILD"
    )
    phases = (*target.child_phases, ("build_manfiest_typo",))
    invalid = _replace_retention_target(contract, "SOURCE_BUILD", child_phases=phases)

    with pytest.raises(
        checker.ContractValidationError,
        match="child phase references unknown relation 'build_manfiest_typo'",
    ):
        checker.validate_contract(invalid)


def test_retention_contract_rejects_missing_fk_blocker() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item for item in contract.retention_targets if item.target == "SOURCE_BUILD"
    )
    blockers = tuple(
        blocker
        for blocker in target.external_blockers
        if blocker.attributes != ("build_id",)
    )
    invalid = _replace_retention_target(
        contract, "SOURCE_BUILD", external_blockers=blockers
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=r"leaves FK descendant 'analysis_run_descriptor'.*unclassified",
    ):
        checker.validate_contract(invalid)


def test_retention_contract_rejects_reversed_child_order() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item
        for item in contract.retention_targets
        if item.target == "PUBLICATION_CANDIDATE"
    )
    invalid = _replace_retention_target(
        contract,
        "PUBLICATION_CANDIDATE",
        child_phases=tuple(reversed(target.child_phases)),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="child phase order is reversed",
    ):
        checker.validate_contract(invalid)


def test_candidate_retention_requires_exact_uncommitted_projection_fold() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item
        for item in contract.retention_targets
        if item.target == "PUBLICATION_CANDIDATE"
    )
    assert tuple(gate.id for gate in target.machine_gates) == (
        "catalog.uncommitted_candidate_reserved_projection(candidate_id,reserved_revision)",
    )
    assert tuple(phase[-1] for phase in target.child_phases[:9]) == (
        "catalog_publication_storage",
        "catalog_contributor",
        "artifact_input",
        "publication_checkpoint",
        "catalog_publication_order",
        "catalog_publication_content",
        "catalog_subject",
        "catalog_artifact",
        "catalog_publication_occurrence_identity",
    )

    invalid = _replace_retention_target(
        contract,
        "PUBLICATION_CANDIDATE",
        machine_gates=(),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="machine gates must be exactly",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize(
    ("blocker_changes", "message"),
    (
        (
            {"blocking_predicate": ""},
            "prepared-artifact release semantic blocker",
        ),
        (
            {"attributes": ("publication_key",)},
            "registered indexed left prefix",
        ),
    ),
)
def test_candidate_retention_requires_exact_release_state_semantic_blocker(
    blocker_changes: dict[str, object],
    message: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item
        for item in contract.retention_targets
        if item.target == "PUBLICATION_CANDIDATE"
    )
    assert len(target.semantic_blockers) == 1
    blocker = replace(target.semantic_blockers[0], **blocker_changes)
    invalid = _replace_retention_target(
        contract,
        "PUBLICATION_CANDIDATE",
        semantic_blockers=(blocker,),
    )

    with pytest.raises(checker.ContractValidationError, match=message):
        checker.validate_contract(invalid)


@pytest.mark.parametrize("mutation", ["remove", "swap_to_child"])
def test_page_retention_requires_exact_parent_edge_phase_selector(
    mutation: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item
        for item in contract.retention_targets
        if item.target == "GALLERY_OBSERVATION_PAGE"
    )
    assert len(target.phase_selectors) == 1
    selector = target.phase_selectors[0]
    assert selector.attributes == ("parent_sha256",)
    if mutation == "remove":
        selectors = target.phase_selectors[:0]
    else:
        selectors = (replace(selector, attributes=("child_sha256",)),)
    invalid = _replace_retention_target(
        contract,
        "GALLERY_OBSERVATION_PAGE",
        phase_selectors=selectors,
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="phase selector|phase selectors must exactly disambiguate",
    ):
        checker.validate_contract(invalid)


def test_canonical_retention_requires_generic_generation_upload_gate() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item for item in contract.retention_targets if item.target == "CANONICAL_VALUE"
    )
    assert tuple(gate.id for gate in target.machine_gates) == (
        "catalog.current_source_snapshot_manifest(source_revision,snapshot_manifest_sha256)",
        "operational.live_source_working_analysis_snapshot_manifest(analysis_id,snapshot_manifest_sha256)",
        "catalog.live_publication_candidate_snapshot_manifest(candidate_id,snapshot_manifest_sha256)",
        "operational.canonical_value_upload(generation,value_sha256)",
        "operational.canonical_value_maintenance_fence",
    )
    invalid = _replace_retention_target(
        contract,
        "CANONICAL_VALUE",
        machine_gates=(),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="machine gates must be exactly",
    ):
        checker.validate_contract(invalid)


def test_canonical_cleanup_requires_bootstrap_exception_and_maintenance_fence() -> None:
    contract = checker.load_contract(CATALOG)
    page_contract = contract.canonical_value_page_contract
    assert page_contract is not None
    for term in (
        "source_root_v1 is the sole pre-mapping exception",
        "every other digest domain requires",
        "shared canonical-value maintenance gate",
        "cleanup cycle holds its exclusive form",
        "final identity never releases its upload claim by itself",
        "retention-blocking external consumer and deletion of only that generation claim commit atomically",
        "phase-owned dictionary or type row alone never releases the claim",
        "pre-mapping claim whose generation has no source_build_generation row",
    ):
        invalid = replace(
            contract,
            canonical_value_page_contract=replace(
                page_contract,
                cleanup_rule=page_contract.cleanup_rule.replace(term, ""),
            ),
        )
        with pytest.raises(
            checker.ContractValidationError,
            match="cleanup rule omits",
        ):
            checker.validate_contract(invalid)


def test_retention_contract_rejects_missing_derived_view_classification() -> None:
    contract = checker.load_contract(CATALOG)
    target = next(
        item for item in contract.retention_targets if item.target == "ANALYSIS_RUN"
    )
    invalid = _replace_retention_target(
        contract,
        "ANALYSIS_RUN",
        derived_views=target.derived_views[1:],
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=r"leaves FK descendant 'analysis_run'.*unclassified",
    ):
        checker.validate_contract(invalid)


def test_retention_contract_rejects_broken_active_provenance_path() -> None:
    contract = checker.load_contract(CATALOG)
    retention = contract.retention_contract
    assert retention is not None
    invalid = replace(
        contract,
        retention_contract=replace(retention, provenance_relation="source_revision"),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="provenance_relation must be the exact relation",
    ):
        checker.validate_contract(invalid)


@pytest.mark.parametrize("removed_attribute", ("row_count", "sealed_at"))
def test_analysis_resolution_contract_rejects_incomplete_wide_seal(
    removed_attribute: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    seal = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_state_component_seal"
    )
    invalid_seal = replace(
        seal,
        attributes=tuple(
            attribute for attribute in seal.attributes if attribute != removed_attribute
        ),
        functional_dependencies=tuple(
            replace(
                dependency,
                dependent=frozenset(
                    attribute
                    for attribute in dependency.dependent
                    if attribute != removed_attribute
                ),
            )
            for dependency in seal.functional_dependencies
        ),
    )
    relations = tuple(
        invalid_seal if relation is seal else relation
        for relation in contract.relations
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="completion seal must be one complete BCNF row",
    ):
        checker.validate_contract(replace(contract, relations=relations))


def test_artifact_delta_contract_rejects_wrong_truth_table() -> None:
    contract = checker.load_contract(CATALOG)
    delta = contract.artifact_delta_contract
    assert delta is not None
    invalid = replace(
        contract,
        artifact_delta_contract=replace(
            delta,
            old_state_operations=("CREATE", "REBUILD", "UNCHANGED"),
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="old-state operations must be exactly",
    ):
        checker.validate_contract(invalid)


def test_artifact_name_is_normalized_identity_not_delta_state() -> None:
    contract = checker.load_contract(CATALOG)
    delta = contract.artifact_delta_contract
    assert delta is not None
    assert "artifact_semantics_sha256 is exactly equal" in delta.unchanged_rule
    assert "artifact names are not delta state" in delta.unchanged_rule
    assert "globally derived from immutable positive GID" in delta.rename_rule
    assert "absent from artifact input, delta, operation, prepared" in delta.rename_rule
    assert "never an artifact rename" in delta.rename_rule

    with pytest.raises(
        checker.ContractValidationError,
        match="name-normalization rule is incomplete",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_delta_contract=replace(
                    delta,
                    rename_rule="silently retain the old path",
                ),
            )
        )


@pytest.mark.parametrize(
    ("contract_attribute", "field", "replacement", "message"),
    (
        (
            "artifact_name_contract",
            "golden_name_hex",
            "6832682d30372e63627a",
            "artifact name contract drifts",
        ),
        (
            "artifact_storage_key_contract",
            "golden_payload_hex",
            "00",
            "artifact storage-key golden payload is not GID-derived",
        ),
        (
            "artifact_protection_token_contract",
            "golden_receipt_id",
            "00" * 16,
            "artifact storage-receipt golden does not hash",
        ),
    ),
)
def test_artifact_derived_identity_contract_rejects_corrupt_goldens(
    contract_attribute: str,
    field: str,
    replacement: object,
    message: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    identity_contract = getattr(contract, contract_attribute)
    assert identity_contract is not None
    invalid = replace(
        contract,
        **{
            contract_attribute: replace(
                identity_contract,
                **{field: replacement},
            )
        },
    )

    with pytest.raises(checker.ContractValidationError, match=message):
        checker.validate_contract(invalid)


def test_transition_gates_reject_audit_digest_authority() -> None:
    contract = checker.load_contract(CATALOG)
    authority = contract.transition_authority_contract
    assert authority is not None
    gate_name = "analysis_state_component_seal"
    gate = next(
        relation for relation in contract.relations if relation.name == gate_name
    )
    invalid_gate = replace(
        gate,
        attributes=(*gate.attributes, "output_sha256"),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="contains forbidden audit digests",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_gate if relation.name == gate_name else relation
                    for relation in contract.relations
                ),
            )
        )

    prose_gate = replace(
        gate,
        rationale="the audit digest authorizes the seal transition",
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="grants authority to an audit digest",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    prose_gate if relation.name == gate_name else relation
                    for relation in contract.relations
                ),
            )
        )


def test_artifact_codec_rejects_audit_digest_in_semantic_framing() -> None:
    contract = checker.load_contract(CATALOG)
    selected = next(
        codec
        for codec in contract.artifact_component_codecs
        if codec.kind == "selected"
    )
    invalid = replace(
        selected,
        framing=selected.framing + " || raw32(item_sha256)",
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="grants semantic authority to an audit digest|drifts from v1",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_component_codecs=tuple(
                    invalid if codec.kind == "selected" else codec
                    for codec in contract.artifact_component_codecs
                ),
            )
        )


def test_artifact_delta_contract_rejects_incomplete_semantic_input() -> None:
    contract = checker.load_contract(CATALOG)
    delta = contract.artifact_delta_contract
    assert delta is not None
    invalid = replace(
        contract,
        artifact_delta_contract=replace(
            delta,
            semantic_components=(
                "source_manifest",
                "member_plan",
                "effective_content",
                "selected",
                "policy",
            ),
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="semantic components must be exactly",
    ):
        checker.validate_contract(invalid)


def test_artifact_policy_rejects_missing_resize_parameter() -> None:
    contract = checker.load_contract(CATALOG)
    producer = contract.artifact_byte_producer_contract
    assert producer is not None
    policy = next(
        relation
        for relation in contract.relations
        if relation.name == producer.policy_relation
    )
    invalid_policy = replace(
        policy,
        attributes=("policy_component_sha256", "artifact_algorithm_version"),
        declared_keys=(
            frozenset({"policy_component_sha256"}),
            frozenset({"artifact_algorithm_version"}),
        ),
        functional_dependencies=(
            _fd({"policy_component_sha256"}, {"artifact_algorithm_version"}),
            _fd({"artifact_algorithm_version"}, {"policy_component_sha256"}),
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="artifact byte-producer contract policy relation has redundant or missing columns",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_policy if relation.name == policy.name else relation
                    for relation in contract.relations
                ),
            )
        )


def test_artifact_member_plan_rejects_exclusion_blind_plan() -> None:
    contract = checker.load_contract(CATALOG)
    member_plan = contract.artifact_member_plan_contract
    assert member_plan is not None

    with pytest.raises(
        checker.ContractValidationError,
        match="entry fields are incomplete",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_member_plan_contract=replace(
                    member_plan,
                    entry_fields=tuple(
                        field
                        for field in member_plan.entry_fields
                        if field != "excluded_flag"
                    ),
                ),
            )
        )


def test_artifact_member_plan_rejects_missing_payload_size() -> None:
    contract = checker.load_contract(CATALOG)
    member_plan = contract.artifact_member_plan_contract
    assert member_plan is not None

    with pytest.raises(
        checker.ContractValidationError,
        match="entry fields are incomplete",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_member_plan_contract=replace(
                    member_plan,
                    entry_fields=tuple(
                        field
                        for field in member_plan.entry_fields
                        if field != "source_size_bytes"
                    ),
                ),
            )
        )


def test_artifact_policy_rejects_unfingerprinted_encoder_bundle() -> None:
    contract = checker.load_contract(CATALOG)
    producer = contract.artifact_byte_producer_contract
    assert producer is not None

    with pytest.raises(
        checker.ContractValidationError,
        match="implementation bundle is incomplete",
    ):
        checker.validate_contract(
            replace(
                contract,
                artifact_byte_producer_contract=replace(
                    producer,
                    algorithm_bundle=tuple(
                        value
                        for value in producer.algorithm_bundle
                        if "exact producer fingerprint" not in value
                    ),
                ),
            )
        )


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


def test_gallery_identity_chain_guard_rejects_undeclared_physical_redundancy() -> None:
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    occurrence = relation_by_name["publication_selection_occurrence_identity"]
    relation_by_name[occurrence.name] = replace(
        occurrence,
        attributes=(*occurrence.attributes, "gallery_id"),
    )

    errors = checker._validate_gallery_identity_chain_bcnf(relation_by_name)

    assert any(
        "omits immutable cross-relation identity FD {gallery_id} -> "
        "{publication_key}" in error
        for error in errors
    )
    assert any(
        "physically repeats immutable identity-chain fact" in error for error in errors
    )


def test_gallery_identity_chain_guard_requires_exact_authoritative_backbone() -> None:
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    backbone = relation_by_name["source_gallery_name_gid"]
    relation_by_name[backbone.name] = replace(
        backbone,
        functional_dependencies=(),
    )

    errors = checker._validate_gallery_identity_chain_bcnf(relation_by_name)

    assert (
        "gallery identity chain must retain its exact authoritative backbone "
        "relation 'source_gallery_name_gid'" in errors
    )


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

    with pytest.raises(
        checker.ContractValidationError,
        match="is neither a candidate key nor a declared referential unique key",
    ):
        checker.validate_contract(checker.Contract(1, "negative", (parent, child), ()))


def test_operational_external_relation_key_shapes_are_checked() -> None:
    contract = checker.load_contract(OPERATIONAL)
    assert checker.validate_contract(contract)
    external = next(
        relation
        for relation in contract.external_relations
        if relation.name == "source_build_descriptor"
    )
    invalid = replace(
        contract,
        external_relations=tuple(
            (
                replace(external, declared_keys=(frozenset({"unknown"}),))
                if relation.name == external.name
                else relation
            )
            for relation in contract.external_relations
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="candidate key mentions unknown attributes",
    ):
        checker.validate_contract(invalid)


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


def test_identity_digest_authority_cannot_come_only_from_a_logical_view() -> None:
    occurrence = _relation(
        "payload_occurrence",
        ("occurrence_id", "payload_sha256"),
        (_fd({"occurrence_id"}, {"payload_sha256"}),),
        (frozenset({"occurrence_id"}),),
    )
    identity_view = checker.Relation(
        name="payload_identity_view",
        kind="controlled_materialization",
        attributes=("payload_sha256",),
        functional_dependencies=(),
        declared_keys=(frozenset({"payload_sha256"}),),
        foreign_keys=(),
        materialization={
            "authoritative": False,
            "storage": "logical_view",
            "derived_from": ["payload_occurrence"],
        },
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
            checker.Contract(
                1,
                "negative",
                (occurrence, identity_view),
                (),
                semantics,
            )
        )


@pytest.mark.parametrize(
    ("digest_attribute", "classification"),
    (
        ("audit_sha256", "audit_digest"),
        ("observation_sha256", "observational_digest"),
        ("comparator_sha256", "comparator_digest"),
    ),
)
def test_non_authoritative_digest_cannot_be_a_candidate_key(
    digest_attribute: str, classification: str
) -> None:
    relation = _relation(
        "invalid_digest_identity",
        (digest_attribute, "label"),
        (_fd({digest_attribute}, {"label"}),),
        (frozenset({digest_attribute}),),
    )
    semantics = (
        checker.AttributeSemantic(
            digest_attribute, classification, "non-authoritative test digest"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="candidate key uses non-authoritative digest",
    ):
        checker.validate_contract(
            checker.Contract(1, "negative", (relation,), (), semantics)
        )


@pytest.mark.parametrize(
    ("digest_attribute", "classification"),
    (
        ("audit_sha256", "audit_digest"),
        ("observation_sha256", "observational_digest"),
        ("comparator_sha256", "comparator_digest"),
    ),
)
def test_non_authoritative_digest_cannot_be_an_fd_determinant(
    digest_attribute: str, classification: str
) -> None:
    relation = _relation(
        "invalid_digest_determinant",
        ("owner", digest_attribute, "label"),
        (
            _fd({"owner"}, {digest_attribute, "label"}),
            _fd({digest_attribute}, {"label"}),
        ),
        (frozenset({"owner"}),),
    )
    semantics = (
        checker.AttributeSemantic(
            digest_attribute, classification, "non-authoritative test digest"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="FD determinant uses non-authoritative digest",
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


def test_decomposition_semantic_signatures_must_be_unique() -> None:
    contract = checker.load_contract(CATALOG)
    original = contract.decompositions[0]
    duplicate = replace(
        original,
        name=f"{original.name}_duplicate",
        functional_dependencies=tuple(reversed(original.functional_dependencies)),
        projections=tuple(reversed(original.projections)),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="duplicates the semantic signature of",
    ):
        checker.validate_contract(
            replace(contract, decompositions=(*contract.decompositions, duplicate))
        )


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
    assert "151 BCNF base relations" in valid.stdout
    assert "46 intentional logical projections" in valid.stdout
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
