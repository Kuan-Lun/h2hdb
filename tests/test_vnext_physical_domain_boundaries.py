from __future__ import annotations

from typing import Any, cast

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.domain import ArtifactSourceRole
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_domains import DomainValidationError, require_text
from h2hdb.vnext_gallery_identity_repository import (
    GalleryIdentityRepository,
    SourceLocatorCommand,
)
from h2hdb.vnext_gallery_staging_repository import (
    BatchAttempt,
    FileBatchCommand,
    FileContentReceipt,
    FileObservation,
    GalleryObservationStagingRepository,
)
from h2hdb.vnext_hash_cache_repository import (
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_operational_event_repository import RemovedGid, _require_effects
from h2hdb.vnext_physical_domains import (
    CATALOG_PHYSICAL_DOMAIN_GUARDS,
    CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS,
    OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_SCHEMA_EPOCH_WRITERS,
)
from h2hdb.vnext_queue_repository import VNextDownloadRequest, VNextQueueRepository
from h2hdb.vnext_source_build_repository import (
    _DISCOVERY_BATCH_TOKEN,
    DiscoveryBatch,
    PreparedDiscoveryLocator,
    SourceBuildRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

# Exact pre-recomposition registry retained as negative evidence that the old
# vertical-table authority surface is not silently reintroduced.
_LEGACY_CATALOG_PHYSICAL_DOMAIN_RELATIONS = frozenset(
    {
        "canonical_value_allocation_anchor",
        "canonical_value_allocation_digest_domain",
        "canonical_value_allocation_byte_count",
        "canonical_value_allocation_allocated_at",
        "canonical_value_allocation_seal",
        "canonical_value_allocation",
        "canonical_value_page_anchor",
        "canonical_value_page_payload",
        "canonical_value_page_coordinate",
        "canonical_value_page_subtree_item_count",
        "canonical_value_page_seal",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "source_locator_identity",
        "gallery_identity",
        "file_name_identity",
        "gallery_observation_file_anchor",
        "gallery_observation_file_file_no",
        "gallery_observation_file_file_sha256",
        "gallery_observation_file_seal",
        "gallery_observation_file",
        "tag_term",
        "source_build_anchor",
        "source_build_scope_key",
        "source_build_manifest_policy_id",
        "source_build_state",
        "source_build_created_at",
        "source_build_descriptor_seal",
        "source_build_sealed_at",
        "source_build",
        "source_build_discovery_gallery_count",
        "build_manifest_anchor",
        "build_manifest_manifest_sha256",
        "build_manifest_file_count",
        "build_manifest_byte_count",
        "build_manifest_seal",
        "build_manifest",
        "gallery_manifest",
        "source_snapshot_manifest_identity_anchor",
        "source_snapshot_manifest_identity_gallery_count",
        "source_snapshot_manifest_identity_file_count",
        "source_snapshot_manifest_identity_byte_count",
        "source_snapshot_manifest_identity_seal",
        "source_snapshot_manifest_identity",
        "publication_identity",
        "gallery_observation_page",
        "gallery_observation_allocation_page",
        "gallery_observation_page_descriptor_anchor",
        "gallery_observation_page_descriptor_component",
        "gallery_observation_page_descriptor_level",
        "gallery_observation_page_descriptor_subtree_item_count",
        "gallery_observation_page_descriptor_seal",
        "gallery_observation_page_descriptor",
        "gallery_observation_page_key_bounds_anchor",
        "gallery_observation_page_key_bounds_first_key",
        "gallery_observation_page_key_bounds_last_key",
        "gallery_observation_page_key_bounds_seal",
        "gallery_observation_page_key_bounds",
        "gallery_observation_page_child",
        "gallery_observation_discovery_fingerprint",
        "analysis_run_anchor",
        "analysis_run_build_id",
        "analysis_run_policy_id",
        "analysis_run_input_manifest_sha256",
        "analysis_run_identity",
        "analysis_run_started_at",
        "analysis_run_state",
        "analysis_run_descriptor_seal",
        "analysis_run_completed_at",
        "analysis_run",
        "analysis_state_anchor",
        "analysis_state_component_anchor",
        "analysis_state_component_row_count",
        "analysis_state_component_sealed_at",
        "analysis_state_component_completion_seal",
        "analysis_state_component_seal",
        "analysis_exclusion_delta_anchor",
        "analysis_exclusion_delta_old_excluded",
        "analysis_exclusion_delta_new_excluded",
        "analysis_exclusion_delta_change",
        "analysis_exclusion_delta_seal",
        "analysis_exclusion_delta",
        "analysis_file_hash_decision_shadow_anchor",
        "analysis_file_hash_decision_shadow_occurrence_count",
        "analysis_file_hash_decision_shadow_artist_count",
        "analysis_file_hash_decision_shadow_maximum_gallery_artist_count",
        "analysis_file_hash_decision_shadow_seal",
        "analysis_file_hash_decision_shadow",
        "analysis_content_owner_candidate_shadow",
        "analysis_content_owner_shadow",
        "analysis_impacted_content_provenance",
        "analysis_impacted_content",
        "analysis_impacted_gid_provenance",
        "analysis_impacted_gid",
        "analysis_impacted_gid_provenance_storage",
        "analysis_impacted_gid_storage",
        "analysis_gid_candidate_shadow",
        "analysis_gid_candidate_tombstone",
        "analysis_gid_candidate_resolved",
        "analysis_gid_winner_selection",
        "analysis_gid_winner_shadow",
        "analysis_gid_winner_tombstone",
        "analysis_gid_winner_resolved",
        "analysis_batch_receipt_page_limit",
        "analysis_checkpoint",
        "publication_checkpoint",
        "publication_batch_receipt",
        "publication_candidate_anchor",
        "publication_candidate_analysis_id",
        "publication_candidate_reserved_revision",
        "publication_candidate_artifact_policy_id",
        "publication_candidate_display_title_policy_id",
        "publication_candidate_artifacts_required",
        "publication_candidate_created_at",
        "publication_candidate_definition_seal",
        "publication_candidate",
        "publication_candidate_projection_seal",
        "publication_candidate_projection",
        "publication_candidate_base_publication_commit",
        "publication_candidate_base_catalog",
        "publication_candidate_base_source",
        "publication_selection",
        "publication_selection_occurrence_identity",
        "publication_selection_storage",
        "publication_stage_anchor",
        "publication_stage_order",
        "publication_stage_cursor_codec",
        "publication_stage_seal",
        "publication_stage",
        "publication_checkpoint_anchor",
        "publication_checkpoint_generation",
        "publication_checkpoint_cursor",
        "publication_checkpoint_processed_count",
        "publication_checkpoint_state",
        "publication_checkpoint_updated_at",
        "publication_checkpoint_seal",
        "publication_batch_receipt_anchor",
        "publication_batch_receipt_coordinate",
        "publication_batch_receipt_start_cursor",
        "publication_batch_receipt_start_processed_count",
        "publication_batch_receipt_next_cursor",
        "publication_batch_receipt_row_count",
        "publication_batch_receipt_committed_at",
        "publication_batch_receipt_seal",
        "publication_batch_receipt_stored",
        "publication_finalization_checkpoint_anchor",
        "publication_finalization_checkpoint_generation",
        "publication_finalization_checkpoint_cursor",
        "publication_finalization_checkpoint_processed_count",
        "publication_finalization_checkpoint_state",
        "publication_finalization_checkpoint_updated_at",
        "publication_finalization_checkpoint_seal",
        "publication_finalization_checkpoint",
        "publication_finalization_batch_receipt_anchor",
        "publication_finalization_batch_receipt_coordinate",
        "publication_finalization_batch_receipt_start_cursor",
        "publication_finalization_batch_receipt_start_processed_count",
        "publication_finalization_batch_receipt_next_cursor",
        "publication_finalization_batch_receipt_row_count",
        "publication_finalization_batch_receipt_committed_at",
        "publication_finalization_batch_receipt_seal",
        "publication_finalization_batch_receipt_stored",
        "publication_finalization_batch_receipt",
        "artifact_policy_semantics",
        "artifact_policy",
        "artifact_input",
        "artifact_delta_old",
        "artifact_delta_new",
        "artifact_operation",
        "artifact_blob",
        "catalog_revision_anchor",
        "catalog_revision_publication_count",
        "catalog_revision_descriptor_seal",
        "catalog_revision_descriptor",
        "catalog_revision",
        "catalog_revision_generation",
        "publication_generation_node",
        "publication_generation_successor",
        "display_title_policy_anchor",
        "display_title_policy_algorithm_version",
        "display_title_policy_title_sort_policy_id",
        "display_title_policy_identity",
        "display_title_policy_seal",
        "display_title_policy",
        "title_sort_policy_anchor",
        "title_sort_policy_algorithm_version",
        "title_sort_policy_unicode_data_version",
        "title_sort_policy_identity",
        "title_sort_policy_seal",
        "title_sort_policy",
        "display_title_choice",
        "title_sort",
        "catalog_publication",
        "catalog_publication_occurrence_identity",
        "catalog_publication_storage",
        "catalog_publication_order",
        "catalog_publication_title",
        "catalog_publication_content",
        "contributor_role_registry",
        "catalog_contributor",
        "catalog_subject",
        "publication_commit_anchor",
        "publication_commit_candidate",
        "publication_commit_catalog_revision",
        "publication_commit_source_revision",
        "publication_commit_generation",
        "publication_commit_operational_preparation",
        "publication_commit_operational_policy",
        "publication_commit_artifact_policy",
        "publication_commit_display_title_policy",
        "publication_commit_new_galleries",
        "publication_commit_changed_galleries",
        "publication_commit_removed_galleries",
        "publication_commit_duplicate_losers",
        "publication_commit_committed_at",
        "publication_commit_seal",
        "publication_commit",
        "publication_commit_finalization",
        "publication_commit_head_receipt",
        "publication_commit_head",
        "publication_receipt",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
        "artifact_semantic_input",
        "prepared_artifact",
        "catalog_artifact",
    }
)

_LEGACY_CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS = frozenset(
    {
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "gallery_observation_file",
        "source_build",
        "build_manifest",
        "source_snapshot_manifest_identity",
        "gallery_observation_page_descriptor",
        "gallery_observation_page_key_bounds",
        "analysis_run",
        "analysis_state_anchor",
        "analysis_state_component_seal",
        "analysis_exclusion_delta",
        "analysis_file_hash_decision_shadow",
        "analysis_impacted_gid_provenance",
        "analysis_impacted_gid",
        "analysis_gid_candidate_resolved",
        "analysis_gid_winner_shadow",
        "analysis_gid_winner_resolved",
        "analysis_checkpoint",
        "publication_checkpoint",
        "publication_selection",
        "publication_batch_receipt",
        "publication_candidate",
        "publication_candidate_projection",
        "publication_candidate_base_catalog",
        "publication_candidate_base_source",
        "publication_stage",
        "publication_batch_receipt_stored",
        "publication_finalization_checkpoint",
        "publication_finalization_batch_receipt_stored",
        "publication_finalization_batch_receipt",
        "artifact_policy_semantics",
        "artifact_delta_old",
        "artifact_delta_new",
        "catalog_revision_descriptor",
        "catalog_revision",
        "catalog_revision_generation",
        "display_title_policy",
        "title_sort_policy",
        "catalog_publication",
        "catalog_publication_title",
        "publication_commit",
        "publication_commit_head",
        "publication_receipt",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
)


class _NoDatabaseWork:
    """Fail if a malformed command gets as far as a database operation."""

    @property
    def connector(self) -> Any:  # pragma: no cover - a test failure path
        raise AssertionError("malformed physical-domain command reached SQL")


def _work_without_database() -> VNextUnitOfWork:
    return cast(VNextUnitOfWork, _NoDatabaseWork())


def _contract_relations(obligation_id: str) -> frozenset[str]:
    records = cast(
        tuple[dict[str, Any], ...],
        ARTIFACT["semantic_obligations"],
    )
    matches = tuple(record for record in records if record["id"] == obligation_id)
    assert len(matches) == 1
    contract = cast(dict[str, Any], matches[0]["contract"])
    return frozenset(cast(list[str], contract["relations"]))


def test_closed_writer_families_match_the_generated_contract_and_real_symbols() -> None:
    assert CATALOG_PHYSICAL_DOMAIN_RELATIONS == _contract_relations(
        "catalog.physical-domains.v1"
    )
    assert OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS == _contract_relations(
        "h2hdb.operational.physical-domains.v1"
    )
    assert (
        CATALOG_PHYSICAL_DOMAIN_RELATIONS != _LEGACY_CATALOG_PHYSICAL_DOMAIN_RELATIONS
    )
    assert (
        CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS
        != _LEGACY_CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS
    )
    assert CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS == (
        CATALOG_PHYSICAL_DOMAIN_RELATIONS - CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS
    )
    assert {
        "analysis_run_anchor",
        "publication_commit_candidate_id",
        "source_build_scope_key",
    }.isdisjoint(CATALOG_PHYSICAL_DOMAIN_RELATIONS)
    assert len(CATALOG_PHYSICAL_DOMAIN_RELATIONS) == 146
    assert len(CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS) == 124
    assert len(CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS) == 22
    assert OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS == (
        OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS - {"schema_epoch_control"}
    )
    assert len(CATALOG_PHYSICAL_DOMAIN_WRITERS) == 54
    assert len(OPERATIONAL_PHYSICAL_DOMAIN_WRITERS) == 9
    assert len(OPERATIONAL_SCHEMA_EPOCH_WRITERS) == 1

    for symbol in (
        *CATALOG_PHYSICAL_DOMAIN_WRITERS,
        *OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
        *OPERATIONAL_SCHEMA_EPOCH_WRITERS,
    ):
        owner_name, method_name = symbol.__qualname__.split(".")
        module = __import__(symbol.__module__, fromlist=[owner_name])
        owner = getattr(module, owner_name)
        assert not method_name.startswith("_")
        assert getattr(owner, method_name) is symbol

    for guard in (
        *CATALOG_PHYSICAL_DOMAIN_GUARDS,
        *OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    ):
        module = __import__(guard.__module__, fromlist=[guard.__name__])
        assert getattr(module, guard.__name__) is guard
        assert guard.__name__ != "<lambda>"


def test_unbounded_sql_text_still_requires_exact_str() -> None:
    assert require_text("https://example.invalid/a", field="url") == (
        "https://example.invalid/a"
    )
    for malformed in (b"url", 1, None):
        with pytest.raises(DomainValidationError):
            require_text(malformed, field="url")


def test_forged_download_completion_is_rejected_before_sql() -> None:
    request = VNextDownloadRequest(7, "", b"r" * 16, 1)
    object.__setattr__(request, "gid", True)
    with pytest.raises(DomainValidationError, match="gid"):
        VNextQueueRepository.complete_download_request(
            _work_without_database(),
            request=request,
        )


def test_forged_canonical_plan_is_rejected_before_sql() -> None:
    plan = CanonicalValueUploadPlan.from_parts("physical_test_v1", (b"payload",))
    try:
        plan.value_sha256 = b"short"
        with pytest.raises(DomainValidationError, match="value_sha256"):
            CanonicalValueRepository.allocate(
                _work_without_database(),
                gate_lease=cast(Any, object()),
                ingest_turn=cast(Any, object()),
                plan=plan,
                now=1,
            )
    finally:
        plan.close()


def test_forged_hash_observation_plan_is_rejected_before_sql() -> None:
    plan = FileHashObservationPlan.from_parts((b"payload",))
    object.__setattr__(plan, "size_bytes", True)
    with pytest.raises(DomainValidationError, match="size_bytes"):
        VNextHashCacheRepository.handoff(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            source_plan=cast(Any, object()),
            fingerprint_plan=cast(Any, object()),
            file_plan=plan,
            observed_at=1,
            cached_at=1,
            now=1,
        )


def test_forged_locator_command_is_revalidated_before_sql() -> None:
    command = SourceLocatorCommand(("gallery",))
    object.__setattr__(command, "components", ())
    with pytest.raises((ValueError, IndexError)):
        GalleryIdentityRepository.handoff_locator(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            build_id=b"b" * 16,
            command=command,
            locator_plan=cast(Any, object()),
            now=1,
        )


def test_forged_gallery_batch_entry_is_rejected_before_sql() -> None:
    content = FileContentReceipt.from_parts((b"payload",))
    entry = FileObservation(
        b"001.jpg",
        content,
        ArtifactSourceRole.PAGE,
        1,
        2,
        3,
        4,
    )
    command = FileBatchCommand(
        (entry,),
        False,
        BatchAttempt(b"o" * 16, None),
    )
    object.__setattr__(entry, "device", True)
    with pytest.raises(ValueError, match="device"):
        GalleryObservationStagingRepository.put_files(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            handle=cast(Any, object()),
            command=command,
            now=1,
        )


def test_forged_discovery_locator_is_rejected_before_sql() -> None:
    plan_capability = object()
    locator = PreparedDiscoveryLocator(
        0,
        b"l" * 32,
        1,
        b"p" * 32,
        b"gallery",
        plan_capability,
    )
    batch = DiscoveryBatch(
        b"b" * 16,
        b"k" * 32,
        b"s" * 16,
        1,
        b"t" * 32,
        1,
        b"",
        0,
        (locator,),
        False,
        False,
        plan_capability,
        object(),
        _DISCOVERY_BATCH_TOKEN,
    )
    object.__setattr__(locator, "position", True)
    with pytest.raises(DomainValidationError, match="position"):
        SourceBuildRepository.resolve_discovery_locator(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            batch=batch,
            locator=locator,
            upload_plan=cast(Any, object()),
            now=1,
        )


def test_forged_operational_effect_is_rejected_before_event_derivation() -> None:
    effect = RemovedGid(7, b"r" * 16)
    object.__setattr__(effect, "gid", True)
    with pytest.raises(DomainValidationError, match="removed gid"):
        _require_effects((effect,), max_rows=128)
