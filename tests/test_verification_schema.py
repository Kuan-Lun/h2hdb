from __future__ import annotations

import importlib.util
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

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
    assert seal.materialization is not None
    assert anchor.materialization["root_rule"] == "depth_zero_self_anchor"
    assert anchor.materialization["policy_rule"] == (
        "same_policy_or_depth_zero_compaction"
    )
    assert ancestry.materialization["ancestry_invariant"] == (
        "acyclic_depth_contiguous"
    )
    assert seal.materialization["delta_completeness"] == "exact_old_new_snapshot"
    assert seal.materialization["compaction_validation"] == ("full_evaluator_equality")
    assert seal.materialization["cleanup_guard"] == "no_reachable_descendants"

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
    assert {"summary_sha256", "language_sha256", "artifact_locator_sha256"} <= set(
        long_value_contract.canonical_reference_attributes
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
        "gallery_observation_metadata_vertical",
        "source_build_discovery_vertical",
        "gallery_observation_scan_vertical",
        "gallery_observation_directory_vertical",
        "gallery_observation_stat_vertical",
        "gallery_observation_file_filesystem_vertical",
        "artifact_producer_fingerprint_vertical",
        "analysis_stage_vertical",
        "publication_stage_vertical",
        "source_revision_descriptor_vertical",
        "catalog_revision_descriptor_vertical",
        "publication_commit_vertical",
        "analysis_run_vertical",
        "analysis_state_component_seal_vertical",
        "analysis_exclusion_delta_vertical",
        "analysis_checkpoint_vertical",
        "analysis_batch_receipt_stored_vertical",
        "publication_checkpoint_vertical",
        "publication_batch_receipt_stored_vertical",
        "canonical_value_allocation_vertical",
        "canonical_value_page_vertical",
        "gallery_observation_page_descriptor_vertical",
        "gallery_observation_page_key_bounds_vertical",
        "file_name_identity_vertical",
        "gallery_observation_file_vertical",
        "tag_term_vertical",
        "source_build_vertical",
        "build_manifest_vertical",
        "gallery_manifest_vertical",
        "source_snapshot_manifest_identity_vertical",
        "manifest_policy_vertical",
        "source_scope_vertical",
        "analysis_policy_vertical",
        "artifact_zip_writer_policy_vertical",
        "artifact_storage_codec_vertical",
        "artifact_policy_semantics_vertical",
        "artifact_semantic_input_vertical",
        "prepared_artifact_vertical",
        "catalog_artifact_vertical",
        "publication_candidate_vertical",
        "catalog_publication_vertical",
        "catalog_publication_title_vertical",
        "catalog_contributor_vertical",
        "publication_finalization_checkpoint_vertical",
        "publication_finalization_batch_receipt_stored_vertical",
        "title_sort_policy_vertical",
        "display_title_policy_vertical",
        "analysis_file_hash_decision_shadow_vertical",
        "analysis_content_owner_candidate_shadow_vertical",
        "analysis_content_owner_shadow_vertical",
        "analysis_impacted_content_vertical",
        "analysis_impacted_gid_vertical",
    )
    assert len(contract.vertical_families) == 52
    assert report.generation_streams == ()
    assert contract.publication_commit_contract is not None
    assert len(contract.batch_receipt_projections) == 3
    vertical = next(
        family
        for family in contract.vertical_families
        if family.name == "gallery_observation_metadata_vertical"
    )
    assert vertical.key_attributes == ("gallery_id", "observation_id")
    assert vertical.visibility == "sealed_total"
    assert vertical.anchor_relation == "gallery_observation_metadata_anchor"
    assert vertical.seal_relation == "gallery_observation_metadata_seal"
    assert vertical.view_relation == "gallery_observation_metadata"
    assert tuple(
        (
            member.relation,
            member.key_attributes,
            member.value_attribute,
            member.join.source_relation,
            member.project,
        )
        for member in vertical.members
    ) == (
        (
            "gallery_source_name_access",
            ("gallery_id",),
            "source_gallery_name",
            "gallery_observation_metadata_seal",
            False,
        ),
        (
            "source_gallery_name_gid",
            ("source_gallery_name",),
            "gid",
            "gallery_source_name_access",
            True,
        ),
        (
            "gallery_upload_time",
            ("gid",),
            "upload_time",
            "source_gallery_name_gid",
            True,
        ),
        (
            "gallery_observation_download_time",
            ("gallery_id", "observation_id"),
            "download_time",
            "gallery_observation_metadata_seal",
            True,
        ),
        (
            "gallery_observation_modified_time",
            ("gallery_id", "observation_id"),
            "modified_time",
            "gallery_observation_metadata_seal",
            True,
        ),
    )
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
        assert view.materialization["storage"] == "logical_view"
    assert tuple((fd.determinant, fd.dependent) for fd in vertical.semantic_fds) == (
        (frozenset({"gallery_id"}), frozenset({"source_gallery_name"})),
        (frozenset({"source_gallery_name"}), frozenset({"gid"})),
        (frozenset({"gid"}), frozenset({"upload_time"})),
        (
            frozenset({"gallery_id", "observation_id"}),
            frozenset({"download_time", "modified_time"}),
        ),
    )
    metadata_view = relation_by_name[vertical.view_relation]
    assert metadata_view.materialization is not None
    assert metadata_view.materialization["storage"] == "logical_view"

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
    assert component_relation.materialization is not None
    assert component_relation.materialization["storage"] == "logical_view"
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
        "artifact_algorithm_version",
        "max_image_short_side",
        "producer_fingerprint_sha256",
    )
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
        "publication_candidate_and_optional_base_source",
        "catalog_publication_and_title_basis",
        "catalog_publication_and_optional_content",
        "publication_candidate_and_optional_base_catalog",
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
        or relation.materialization.get("storage") != "logical_view"
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


def test_vertical_family_join_must_use_a_complete_member_candidate_key() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "artifact_producer_fingerprint_vertical"
    )
    identity_member = next(
        member
        for member in family.members
        if member.relation == "artifact_producer_fingerprint_identity"
    )
    invalid_member = replace(
        identity_member,
        join=replace(identity_member.join, member_attributes=("writer_id",)),
    )
    invalid_family = replace(
        family,
        members=tuple(
            invalid_member if member is identity_member else member
            for member in family.members
        ),
    )
    invalid = replace(
        contract,
        vertical_families=tuple(
            invalid_family if value is family else value
            for value in contract.vertical_families
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="join must map unique source attributes onto one complete ordered candidate key",
    ):
        checker.validate_contract(invalid)


def test_optional_vertical_member_requires_reverse_fk_and_closed_presence() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "source_build_vertical"
    )
    optional_relation = next(
        relation
        for relation in contract.relations
        if relation.name == "source_build_sealed_at"
    )
    missing_reverse_fk = replace(optional_relation, foreign_keys=())
    invalid_relations = replace(
        contract,
        relations=tuple(
            missing_reverse_fk if relation is optional_relation else relation
            for relation in contract.relations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="optional member 'source_build_sealed_at' must reference",
    ):
        checker.validate_contract(invalid_relations)

    missing_presence = replace(family, optional_presence=None)
    invalid_family = replace(
        contract,
        vertical_families=tuple(
            missing_presence if value is family else value
            for value in contract.vertical_families
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="optional members require one closed presence rule",
    ):
        checker.validate_contract(invalid_family)


def test_vertical_projection_alias_must_match_the_declared_wide_view() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "build_manifest_vertical"
    )
    timestamp = next(
        member
        for member in family.members
        if member.relation == "source_build_sealed_at"
    )
    broken_timestamp = replace(timestamp, projection_attribute="sealed_at")
    broken_family = replace(
        family,
        members=tuple(
            broken_timestamp if member is timestamp else member
            for member in family.members
        ),
    )
    invalid = replace(
        contract,
        vertical_families=tuple(
            broken_family if value is family else value
            for value in contract.vertical_families
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="view must project exactly the family key",
    ):
        checker.validate_contract(invalid)


def test_batch3a_gallery_identity_is_one_bcnf_base_and_other_families_remain() -> None:
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_by_name = {family.name: family for family in contract.vertical_families}

    assert {
        "file_name_identity_vertical": (
            "file_name_identity_anchor",
            "file_name_identity_seal",
            "file_name_identity",
            ("file_key",),
        ),
        "gallery_observation_file_vertical": (
            "gallery_observation_file_anchor",
            "gallery_observation_file_seal",
            "gallery_observation_file",
            ("gallery_id", "observation_id", "file_key"),
        ),
        "tag_term_vertical": (
            "tag_term_anchor",
            "tag_term_seal",
            "tag_term",
            ("tag_id",),
        ),
    } == {
        name: (
            family_by_name[name].anchor_relation,
            family_by_name[name].seal_relation,
            family_by_name[name].view_relation,
            family_by_name[name].key_attributes,
        )
        for name in (
            "file_name_identity_vertical",
            "gallery_observation_file_vertical",
            "tag_term_vertical",
        )
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
    assert all(
        dependency.determinant in set(gallery_identity.declared_keys)
        for dependency in gallery_identity.functional_dependencies
    )
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

    old_views = {
        "file_name_identity",
        "gallery_observation_file",
        "tag_term",
    }
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
        "file_name_identity_anchor",
        "file_name_identity_name_bytes",
        "file_name_identity_file_role",
        "file_name_identity_seal",
        "file_name_identity",
        "gallery_observation_file_anchor",
        "gallery_observation_file_file_no",
        "gallery_observation_file_file_sha256",
        "gallery_observation_file_seal",
        "gallery_observation_file",
        "tag_term_anchor",
        "tag_term_identity",
        "tag_term_seal",
        "tag_term",
    } <= set(physical_domains.relations)


def test_batch3b_vertical_families_preserve_views_and_sealed_authority() -> None:
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_by_name = {family.name: family for family in contract.vertical_families}
    expected = {
        "source_build_vertical": (
            "source_build_anchor",
            "source_build_descriptor_seal",
            "source_build",
            ("build_id",),
        ),
        "build_manifest_vertical": (
            "build_manifest_anchor",
            "build_manifest_seal",
            "build_manifest",
            ("build_id",),
        ),
        "gallery_manifest_vertical": (
            "gallery_manifest_anchor",
            "gallery_manifest_seal",
            "gallery_manifest",
            ("gallery_id", "observation_id", "manifest_policy_id"),
        ),
        "source_snapshot_manifest_identity_vertical": (
            "source_snapshot_manifest_identity_anchor",
            "source_snapshot_manifest_identity_seal",
            "source_snapshot_manifest_identity",
            ("snapshot_manifest_sha256",),
        ),
    }
    assert expected == {
        name: (
            family_by_name[name].anchor_relation,
            family_by_name[name].seal_relation,
            family_by_name[name].view_relation,
            family_by_name[name].key_attributes,
        )
        for name in expected
    }
    assert relation_by_name["source_build"].attributes == (
        "build_id",
        "scope_key",
        "manifest_policy_id",
        "state",
        "created_at",
        "sealed_at",
    )
    assert relation_by_name["build_manifest"].attributes == (
        "build_id",
        "manifest_sha256",
        "gallery_count",
        "file_count",
        "byte_count",
        "computed_at",
    )
    source_build = family_by_name["source_build_vertical"]
    assert source_build.optional_presence is not None
    assert source_build.optional_presence.present_value == "SEALED"
    assert source_build.optional_presence.absent_values == ("OPEN", "ABANDONED")
    build_timestamp = next(
        member
        for member in family_by_name["build_manifest_vertical"].members
        if member.relation == "source_build_sealed_at"
    )
    assert build_timestamp.projection_attribute == "computed_at"
    assert "database-owned created_at" in source_build.write_obligation
    assert "derives its snapshot-attempt identity" in source_build.write_obligation
    assert "canonical v2 build identity" in source_build.write_obligation
    assert "recovery incarnation uses v3 identity" in source_build.write_obligation
    assert (
        "GalleryObservationStagingRepository.seal"
        in family_by_name["gallery_manifest_vertical"].write_obligation
    )
    snapshot_contract = contract.source_snapshot_manifest_contract
    assert snapshot_contract is not None
    assert "AnalysisRepository.handoff_snapshot_manifest" in (
        snapshot_contract.handoff_obligation
    )

    old_views = {
        "source_build",
        "build_manifest",
        "gallery_manifest",
        "source_snapshot_manifest_identity",
    }
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
        relation_name
        for family_name in expected
        for relation_name in (
            family_by_name[family_name].anchor_relation,
            family_by_name[family_name].seal_relation,
            family_by_name[family_name].view_relation,
            *(member.relation for member in family_by_name[family_name].members),
        )
    } <= set(physical_domains.relations)


def test_b8_physical_domain_closes_the_complete_publication_graph() -> None:
    contract = checker.load_contract(CATALOG)
    relation_order = tuple(relation.name for relation in contract.relations)
    publication_start = relation_order.index("publication_candidate_anchor")
    publication_graph = frozenset(relation_order[publication_start:])
    physical_domains = next(
        obligation
        for obligation in contract.semantic_obligations
        if obligation.id == "catalog.physical-domains.v1"
    )
    assert publication_graph <= set(physical_domains.relations)
    assert len(physical_domains.relations) == 316

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

    assert "catalogPhysicalDomainContracts.length = 316" in catalog_lean
    assert "catalogPhysicalDomainMutationContracts.length = 257" in catalog_lean
    assert "catalogPhysicalDomainReadOnlyViewContracts.length = 59" in catalog_lean
    assert "catalog_physical_domain_has_no_duplicates" in catalog_lean
    assert "catalog_physical_domain_is_manifest_closed" in catalog_lean
    assert "catalog_physical_domain_partition_is_exact" in catalog_lean
    assert "catalog_physical_domain_partition_is_disjoint" in catalog_lean
    assert "catalogPhysicalDomainContracts" not in operational_lean


def test_batch4_vertical_families_preserve_selected_identity_and_marker_contracts() -> (
    None
):
    contract = checker.load_contract(CATALOG)
    relation_by_name = {relation.name: relation for relation in contract.relations}
    family_by_name = {family.name: family for family in contract.vertical_families}

    run = family_by_name["analysis_run_vertical"]
    identity = next(
        member for member in run.members if member.relation == "analysis_run_identity"
    )
    assert identity.congruence_members == (
        "analysis_run_build_id",
        "analysis_run_policy_id",
    )
    assert (
        relation_by_name["analysis_run_input_manifest_sha256"].referential_unique_keys
        == ()
    )
    assert run.optional_presence is not None
    assert run.optional_presence.member_relation == "analysis_run_completed_at"
    assert run.optional_presence.present_value == "COMPLETE"
    assert run.optional_presence.absent_values == ("OPEN", "ABANDONED")

    component = family_by_name["analysis_state_component_seal_vertical"]
    assert component.seal_relation == "analysis_state_component_completion_seal"
    delta = family_by_name["analysis_exclusion_delta_vertical"]
    assert delta.marker_relation == "analysis_exclusion_delta_change"
    assert delta.marker_predicate == "old_excluded != new_excluded"
    endpoint = relation_by_name["analysis_state_anchor"].materialization
    assert endpoint is not None
    assert endpoint["view_pattern"] == "analysis_ancestry_endpoint"
    run_contract = contract.analysis_run_contract
    assert run_contract is not None
    assert "fresh allocation capability" in run_contract.write_obligation
    assert "ignoring proposed_analysis_id" in run_contract.write_obligation
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


@pytest.mark.parametrize(
    "congruence_members",
    [
        (),
        ("analysis_run_build_id", "analysis_run_build_id"),
        ("analysis_run_build_id", "analysis_run_missing_fact"),
    ],
)
def test_selected_natural_identity_rejects_empty_duplicate_or_unknown_facts(
    congruence_members: tuple[str, ...],
) -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "analysis_run_vertical"
    )
    identity = next(
        member
        for member in family.members
        if member.relation == "analysis_run_identity"
    )
    invalid_identity = replace(identity, congruence_members=congruence_members)
    invalid_family = replace(
        family,
        members=tuple(
            invalid_identity if member is identity else member
            for member in family.members
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="selects unknown, duplicate, or empty congruence members",
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


def test_selected_natural_identity_requires_each_selected_fact_congruence_fk() -> None:
    contract = checker.load_contract(CATALOG)
    identity = next(
        relation
        for relation in contract.relations
        if relation.name == "analysis_run_identity"
    )
    invalid_identity = replace(
        identity,
        foreign_keys=tuple(
            foreign_key
            for foreign_key in identity.foreign_keys
            if foreign_key.relation != "analysis_run_policy_id"
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="natural identity.*lacks congruence FK",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_identity if relation is identity else relation
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


def test_vertical_identity_requires_every_fact_congruence_fk() -> None:
    contract = checker.load_contract(CATALOG)
    identity = next(
        relation
        for relation in contract.relations
        if relation.name == "manifest_policy_identity"
    )
    invalid_identity = replace(
        identity,
        foreign_keys=tuple(
            foreign_key
            for foreign_key in identity.foreign_keys
            if foreign_key.relation != "manifest_policy_manifest_algorithm_version"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="natural identity.*lacks congruence FK",
    ):
        checker.validate_contract(
            replace(
                contract,
                relations=tuple(
                    invalid_identity if relation is identity else relation
                    for relation in contract.relations
                ),
            )
        )


def test_referential_unique_key_must_strictly_contain_a_true_candidate() -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation
        for relation in contract.relations
        if relation.name == "manifest_policy_manifest_algorithm_version"
    )
    invalid_fact = replace(
        fact,
        referential_unique_keys=(("manifest_algorithm_version",),),
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
        if relation.name == "manifest_policy_manifest_algorithm_version"
    )
    invalid_fact = replace(
        fact,
        declared_keys=(
            frozenset({"manifest_policy_id", "manifest_algorithm_version"}),
        ),
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


def test_producer_vertical_family_requires_collision_checked_registration() -> None:
    contract = checker.load_contract(CATALOG)
    family = next(
        value
        for value in contract.vertical_families
        if value.name == "artifact_producer_fingerprint_vertical"
    )
    invalid_family = replace(
        family,
        write_obligation=family.write_obligation.replace(
            "byte-compares the full five-field preimage", "trusts the digest"
        ),
    )

    with pytest.raises(
        checker.ContractValidationError,
        match="must pin collision-checked runtime registration and consumption",
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
        if relation.name == "artifact_producer_fingerprint_equivalence_class"
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
        match="producer equivalence codec must key both exact representations",
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
        "every same-root scope family",
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
    ("relation_name", "removed_source"),
    (
        (
            "analysis_content_owner_candidate_shadow_content_sha256",
            "analysis_run_build_id",
        ),
        (
            "analysis_content_owner_candidate_shadow_content_sha256",
            "file_name_identity",
        ),
        (
            "analysis_content_owner_candidate_shadow_content_sha256",
            "analysis_run_policy_id",
        ),
        (
            "analysis_content_owner_candidate_shadow_prefer_not_already_uploaded",
            "tag_term_identity",
        ),
        (
            "analysis_content_owner_candidate_shadow_prefer_not_already_uploaded",
            "canonical_value_page",
        ),
        (
            "analysis_content_owner_candidate_shadow_prefer_not_already_uploaded",
            "canonical_value_page_descriptor",
        ),
        (
            "analysis_content_owner_candidate_shadow_title_scalar_count",
            "gallery_observation_page_descriptor",
        ),
        (
            "analysis_content_owner_candidate_shadow_title_scalar_count",
            "source_build_gallery",
        ),
        (
            "analysis_content_owner_candidate_shadow_download_time",
            "gallery_observation_download_time",
        ),
        (
            "analysis_content_owner_candidate_shadow_download_time",
            "gallery_observation_page_descriptor",
        ),
    ),
)
def test_content_candidate_fact_authority_is_closed_world(
    relation_name: str,
    removed_source: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation for relation in contract.relations if relation.name == relation_name
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
    ("relation_name", "authority_text", "invalid_text"),
    (
        (
            "analysis_content_owner_candidate_shadow_content_sha256",
            "file_name_identity.file_role = CONTENT",
            "file_name_identity.file_role != METADATA",
        ),
        (
            "analysis_content_owner_candidate_shadow_content_sha256",
            "analysis_run_policy_id and sealed analysis_policy thresholds",
            "caller policy thresholds",
        ),
        (
            "analysis_content_owner_candidate_shadow_prefer_not_already_uploaded",
            "digest-validate every exact canonical tag value",
            "trust every tag digest",
        ),
        (
            "analysis_content_owner_candidate_shadow_prefer_not_already_uploaded",
            "canonical_value_allocation.digest_domain = tag_value_utf8_v1",
            "any canonical digest domain",
        ),
        (
            "analysis_content_owner_candidate_shadow_title_scalar_count",
            "select exactly one METADATA root",
            "select any metadata row",
        ),
        (
            "analysis_content_owner_candidate_shadow_title_scalar_count",
            "StrictUtf8ScalarCounter",
            "database character length",
        ),
        (
            "analysis_content_owner_candidate_shadow_download_time",
            "select exactly one METADATA root",
            "read only a normalized scalar row",
        ),
    ),
)
def test_content_candidate_fact_authority_wording_is_executable(
    relation_name: str,
    authority_text: str,
    invalid_text: str,
) -> None:
    contract = checker.load_contract(CATALOG)
    fact = next(
        relation for relation in contract.relations if relation.name == relation_name
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
        "analysis_run_build_id",
        "file_name_identity",
        "analysis_run_policy_id",
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
        ("current analysis_run_build_id", "caller build_id"),
        ("file_name_identity.file_role", "untyped file name"),
        ("current analysis_run_policy_id", "caller policy_id"),
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
        if relation.name == "analysis_impacted_content_witness"
    )
    witness_metadata = dict(witness.materialization or {})
    witness_metadata["refresh_strategy"] = witness_metadata["refresh_strategy"].replace(
        "never update", "may update"
    )
    mutable_witness = replace(witness, materialization=witness_metadata)
    with pytest.raises(
        checker.ContractValidationError,
        match="witness does not pin immutable MIN semantics",
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
    witness_phase = next(
        index
        for index, phase in enumerate(phases)
        if "analysis_impacted_content_witness" in phase
    )
    provenance_phase = next(
        index
        for index, phase in enumerate(phases)
        if "analysis_impacted_content_provenance" in phase
    )
    phases[witness_phase], phases[provenance_phase] = (
        phases[provenance_phase],
        phases[witness_phase],
    )
    reversed_cleanup = replace(analysis, child_phases=tuple(phases))
    with pytest.raises(
        checker.ContractValidationError,
        match="cleanup must be seal, witness, provenance, then anchor",
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
        ("catalog_publication_summary_sha256", "summary_sha256"),
        ("catalog_publication_language_sha256", "language_sha256"),
        ("artifact_location", "artifact_locator_sha256"),
    ),
)
def test_unbounded_publication_and_locator_values_require_canonical_fks(
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
    for term in ("at most 4096 bytes", "iter_artifact_locator_payload", "exact EOF"):
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
                if foreign_key.relation != "publication_commit_seal"
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
        ("runtime_obligation", "tip-local O(1)", "hot-path obligation"),
        (
            "runtime_obligation",
            "replay is candidate/preparation-key local O(1)",
            "hot-path obligation",
        ),
        (
            "runtime_obligation",
            "operational_activation is a derived read-only view",
            "hot-path obligation",
        ),
        ("runtime_obligation", "append-only", "hot-path obligation"),
        (
            "ready_obligation",
            "linear full-history scan",
            "full READY/quick readiness distinction",
        ),
        (
            "ready_obligation",
            "no fork, orphan, or gap",
            "full READY/quick readiness distinction",
        ),
        (
            "ready_obligation",
            "head is the maximum no-successor tip",
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
                if foreign_key.relation == "publication_commit_seal"
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
        match="analysis output binding must be one-way BCNF authority",
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
        if blocker.relation != "analysis_run_build_id"
    )
    invalid = _replace_retention_target(
        contract, "SOURCE_BUILD", external_blockers=blockers
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=r"leaves FK descendant 'analysis_run_build_id'.*unclassified",
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


@pytest.mark.parametrize(
    ("field", "bad_value", "message"),
    [
        ("delta_completeness", "changed_rows_only", "delta_completeness"),
        ("compaction_validation", "row_count_only", "compaction_validation"),
        ("ancestry_invariant", "depth_only", "acyclic ancestry"),
    ],
)
def test_analysis_resolution_contract_rejects_unproved_seal_rules(
    field: str, bad_value: str, message: str
) -> None:
    contract = checker.load_contract(CATALOG)
    resolution = contract.analysis_resolution_contract
    assert resolution is not None
    relations = tuple(
        (
            replace(
                relation,
                materialization={
                    **(relation.materialization or {}),
                    field: bad_value,
                },
            )
            if relation.name == "analysis_state_component_seal"
            else relation
        )
        for relation in contract.relations
    )

    with pytest.raises(checker.ContractValidationError, match=message):
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
            "artifact_locator_contract",
            "golden_payload_hex",
            "00",
            "artifact locator golden payload is not SHA-derived",
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
        if relation.name == "source_build_descriptor_seal"
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
    assert "358 BCNF base relations" in valid.stdout
    assert "81 intentional logical views" in valid.stdout
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
