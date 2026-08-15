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
    assert resolution.cleanup_transition == "remove_seal_then_ancestry_then_state"

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
        "catalog_title_basis_and_display_choice",
        "catalog_publication_and_optional_content",
        "catalog_contributor_and_optional_sort_as",
        "publication_candidate_and_optional_base_catalog",
        "source_build_and_optional_base_source",
    } <= set(report.lossless_decompositions)
    assert all(
        relation.materialization is not None
        for relation in contract.relations
        if relation.kind == "controlled_materialization"
    )
    assert all(not checker.bcnf_violations(relation) for relation in contract.relations)

    publication = relation_by_name["catalog_publication"]
    assert {
        "summary_sha256",
        "language_sha256",
        "published_at",
        "modified_at",
    } <= set(publication.attributes)
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
        frozenset({"publication_id"}),
        frozenset({"gid"}),
    } <= set(relation_by_name["publication_identity"].declared_keys)
    assert "source_revision" not in relation_by_name["catalog_revision"].attributes
    assert "old_revision" not in relation_by_name["artifact_delta_old"].attributes


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
            framing=candidate.framing.replace(
                "u64be(download_time_int63)", "i64be(download_time)"
            ),
        ),
        replace(
            candidate,
            ordering_rule=candidate.ordering_rule.replace("greatest", "least"),
        ),
        replace(
            candidate,
            already_uploaded_marker_rule=candidate.already_uploaded_marker_rule.replace(
                "ASCII A-Z", "Unicode casefold"
            ),
        ),
        replace(
            candidate,
            candidate_digest_framing=candidate.candidate_digest_framing.replace(
                "raw16(analysis_id)", "u64be(gallery_id)"
            ),
        ),
        replace(
            candidate,
            runtime_obligation=candidate.runtime_obligation.replace(
                "validate_analysis_candidate_priority", "caller_priority_bytes"
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
        ("catalog_publication", "summary_sha256"),
        ("catalog_publication", "language_sha256"),
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
            "projection seal lacks exact digest-free BCNF shape",
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
    (("metadata_fingerprint", 41), ("cursor", 2049), ("protection_token", 513)),
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
        if blocker.relation != "analysis_run"
    )
    invalid = _replace_retention_target(
        contract, "SOURCE_BUILD", external_blockers=blockers
    )

    with pytest.raises(
        checker.ContractValidationError,
        match=r"leaves FK descendant 'analysis_run'.*unclassified",
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
        match=r"leaves FK descendant 'analysis_file_hash_decision_resolved'.*unclassified",
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
            if relation.name == resolution.seal_relation
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

    with pytest.raises(checker.ContractValidationError, match="is not a candidate key"):
        checker.validate_contract(checker.Contract(1, "negative", (parent, child), ()))


def test_operational_external_relation_key_shapes_are_checked() -> None:
    contract = checker.load_contract(OPERATIONAL)
    assert checker.validate_contract(contract)
    external = next(
        relation
        for relation in contract.external_relations
        if relation.name == "source_build"
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
