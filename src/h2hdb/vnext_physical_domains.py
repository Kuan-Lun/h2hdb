"""Closed production writer families for the vNext physical-domain contracts.

This registry is deliberately executable wheel code.  It names installed
repository method objects and installed :mod:`h2hdb.vnext_domains` guards;
neither callback markers nor generated DDL checks count as a binding.
"""

from __future__ import annotations

__all__ = [
    "CATALOG_PHYSICAL_DOMAIN_GUARDS",
    "CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS",
    "CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS",
    "CATALOG_PHYSICAL_DOMAIN_RELATIONS",
    "CATALOG_PHYSICAL_DOMAIN_WRITERS",
    "OPERATIONAL_PHYSICAL_DOMAIN_GUARDS",
    "OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS",
    "OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS",
    "OPERATIONAL_PHYSICAL_DOMAIN_WRITERS",
    "OPERATIONAL_SCHEMA_EPOCH_WRITERS",
]

from collections.abc import Callable
from typing import Any

from . import vnext_domains
from .schema_epoch import SchemaEpochRunner
from .vnext_analysis_repository import AnalysisRepository
from .vnext_artifact_preparation_repository import ArtifactPreparationRepository
from .vnext_artifact_release_repository import ArtifactReleaseRepository
from .vnext_canonical_value_repository import CanonicalValueRepository
from .vnext_cleanup_repository import VNextCleanupRepository
from .vnext_gallery_identity_repository import GalleryIdentityRepository
from .vnext_gallery_staging_repository import GalleryObservationStagingRepository
from .vnext_operational_event_repository import OperationalEffectRepository
from .vnext_publication_candidate_repository import PublicationCandidateRepository
from .vnext_publication_finalization_repository import (
    PublicationFinalizationRepository,
)
from .vnext_publication_repository import PublicationRepository
from .vnext_queue_repository import VNextQueueRepository
from .vnext_source_build_repository import SourceBuildRepository

type PhysicalDomainEntrypoint = Callable[..., object]
type PhysicalDomainGuard = Callable[..., Any]


CATALOG_PHYSICAL_DOMAIN_RELATIONS = frozenset(
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
        "source_build_descriptor",
        "source_build_state",
        "source_build_sealed_at",
        "source_build",
        "source_build_discovery",
        "build_manifest_core",
        "build_manifest",
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
        "gallery_manifest",
        "tag_term_anchor",
        "tag_term_identity",
        "tag_term_seal",
        "tag_term",
        "source_snapshot_manifest_identity",
        "publication_identity",
        "artifact_producer_fingerprint",
        "artifact_storage_codec",
        "artifact_semantic_input",
        "prepared_artifact",
        "catalog_artifact",
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
        "analysis_run_descriptor",
        "analysis_run_state",
        "analysis_run_completed_at",
        "analysis_run",
        "analysis_state_anchor",
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
        "analysis_impacted_gid_provenance_storage",
        "analysis_impacted_gid_storage",
        "analysis_impacted_gid_provenance",
        "analysis_impacted_gid",
        "analysis_gid_candidate_shadow",
        "analysis_gid_candidate_tombstone",
        "analysis_gid_candidate_resolved",
        "analysis_gid_winner_selection",
        "analysis_gid_winner_shadow",
        "analysis_gid_winner_tombstone",
        "analysis_gid_winner_resolved",
        "analysis_batch_receipt_stored",
        "analysis_checkpoint",
        "publication_checkpoint",
        "publication_batch_receipt",
        "publication_candidate",
        "publication_candidate_projection_seal",
        "publication_candidate_projection",
        "publication_candidate_base_publication_commit",
        "publication_candidate_base_catalog",
        "publication_candidate_base_source",
        "publication_selection_storage",
        "publication_selection_occurrence_identity",
        "publication_selection",
        "publication_stage",
        "publication_batch_receipt_stored",
        "publication_finalization_checkpoint",
        "publication_finalization_batch_receipt_stored",
        "publication_finalization_batch_receipt",
        "artifact_zip_writer_policy",
        "artifact_policy_semantics",
        "artifact_policy",
        "artifact_input",
        "artifact_delta_old",
        "artifact_delta_new",
        "artifact_operation",
        "artifact_blob",
        "catalog_revision_descriptor",
        "catalog_revision",
        "catalog_revision_generation",
        "publication_generation_node",
        "publication_generation_successor",
        "display_title_policy",
        "title_sort_policy",
        "display_title_choice",
        "title_sort",
        "catalog_publication_storage",
        "catalog_publication_occurrence_identity",
        "catalog_publication",
        "catalog_publication_order",
        "catalog_publication_title",
        "catalog_publication_content",
        "contributor_role_registry",
        "catalog_contributor_anchor",
        "catalog_contributor_name_sha256",
        "catalog_contributor_role",
        "catalog_contributor_identity",
        "catalog_contributor_seal",
        "catalog_contributor",
        "catalog_subject",
        "publication_commit_anchor",
        "publication_commit",
        "publication_commit_finalization",
        "publication_commit_head_receipt",
        "publication_commit_head",
        "publication_receipt",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
)

# These generated non-materialized views remain inside the physical-domain
# authority closure, but no writer family may claim them as mutation targets.
CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS = frozenset(
    {
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "source_build",
        "build_manifest",
        "file_name_identity",
        "gallery_observation_file",
        "tag_term",
        "gallery_observation_page_descriptor",
        "gallery_observation_page_key_bounds",
        "analysis_run",
        "analysis_state_anchor",
        "analysis_exclusion_delta",
        "analysis_file_hash_decision_shadow",
        "analysis_impacted_gid_provenance",
        "analysis_impacted_gid",
        "analysis_gid_candidate_resolved",
        "analysis_gid_winner_shadow",
        "analysis_gid_winner_resolved",
        "publication_batch_receipt",
        "publication_candidate_projection",
        "publication_candidate_base_catalog",
        "publication_candidate_base_source",
        "publication_selection",
        "publication_finalization_batch_receipt",
        "artifact_delta_old",
        "artifact_delta_new",
        "catalog_revision",
        "catalog_revision_generation",
        "catalog_publication",
        "catalog_publication_title",
        "catalog_contributor",
        "publication_commit_head",
        "publication_receipt",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
)
CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS = (
    CATALOG_PHYSICAL_DOMAIN_RELATIONS - CATALOG_PHYSICAL_DOMAIN_READ_ONLY_RELATIONS
)

_ANALYSIS_CHECKPOINT_WRITERS: tuple[PhysicalDomainEntrypoint, ...] = (
    AnalysisRepository.begin,
    AnalysisRepository.process_changed_gallery_batch,
    AnalysisRepository.process_changed_file_hash_batch,
    AnalysisRepository.process_file_hash_decision_batch,
    AnalysisRepository.validate_file_hash_decision_batch,
    AnalysisRepository.process_impacted_gallery_batch,
    AnalysisRepository.process_impacted_content_batch,
    AnalysisRepository.process_content_owner_candidate_batch,
    AnalysisRepository.validate_content_owner_candidate_batch,
    AnalysisRepository.process_content_owner_batch,
    AnalysisRepository.validate_content_owner_batch,
    AnalysisRepository.process_impacted_gid_batch,
    AnalysisRepository.process_gid_candidate_batch,
    AnalysisRepository.validate_gid_candidate_batch,
    AnalysisRepository.process_gid_winner_batch,
    AnalysisRepository.validate_gid_winner_batch,
)

_ARTIFACT_CHECKPOINT_WRITERS: tuple[PhysicalDomainEntrypoint, ...] = (
    ArtifactPreparationRepository.process_artifact_input_batch,
    ArtifactPreparationRepository.process_artifact_delta_operation_batch,
    ArtifactPreparationRepository.validate_artifact_input_delta_batch,
    ArtifactPreparationRepository.validate_prepared_artifact_batch,
    ArtifactPreparationRepository.validate_create_batch,
    ArtifactPreparationRepository.validate_rebuild_batch,
    ArtifactPreparationRepository.validate_delete_batch,
    ArtifactPreparationRepository.validate_unchanged_batch,
    ArtifactPreparationRepository.validate_new_gallery_batch,
    ArtifactPreparationRepository.validate_changed_gallery_batch,
    ArtifactPreparationRepository.validate_removed_gallery_batch,
    ArtifactPreparationRepository.validate_duplicate_loser_batch,
)

CATALOG_PHYSICAL_DOMAIN_WRITERS: tuple[PhysicalDomainEntrypoint, ...] = (
    CanonicalValueRepository.allocate,
    CanonicalValueRepository.put_page,
    ArtifactPreparationRepository.register_producer,
    GalleryIdentityRepository.handoff_locator,
    SourceBuildRepository.handoff_root,
    SourceBuildRepository.abandon,
    SourceBuildRepository.resolve_discovery_locator,
    SourceBuildRepository.assemble_batch,
    GalleryObservationStagingRepository.put_files,
    GalleryObservationStagingRepository.put_directories,
    GalleryObservationStagingRepository.put_tags,
    GalleryObservationStagingRepository.put_metadata,
    GalleryObservationStagingRepository.seal,
    AnalysisRepository.handoff_snapshot_manifest,
    AnalysisRepository.abandon,
    *_ANALYSIS_CHECKPOINT_WRITERS,
    PublicationCandidateRepository.begin,
    PublicationCandidateRepository.process_selection_batch,
    PublicationCandidateRepository.validate_selection_batch,
    PublicationCandidateRepository.process_catalog_projection_batch,
    PublicationCandidateRepository.validate_catalog_projection_batch,
    *_ARTIFACT_CHECKPOINT_WRITERS,
    ArtifactPreparationRepository.persist_prepared_artifact,
    ArtifactPreparationRepository.confirm_prepared_artifact,
    ArtifactReleaseRepository.commit_page,
    PublicationRepository.commit,
    PublicationFinalizationRepository.commit_page,
    VNextCleanupRepository.advance,
)

CATALOG_PHYSICAL_DOMAIN_GUARDS: tuple[PhysicalDomainGuard, ...] = (
    vnext_domains.require_ascii_bytes,
    vnext_domains.require_bool_byte,
    vnext_domains.require_bounded_bytes,
    vnext_domains.require_digest32,
    vnext_domains.require_enum_bytes,
    vnext_domains.require_int63,
    vnext_domains.require_positive_int63,
    vnext_domains.require_uint32,
    vnext_domains.require_utf8_bytes,
    vnext_domains.require_uuid16,
)


OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS = frozenset(
    {
        "schema_epoch_control",
        "download_request",
        "deletion_request_url",
        "operational_preparation_checkpoint",
        "operational_preparation_effect_seal",
        "operational_event",
        "cleanup_checkpoint",
    }
)
OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS = (
    OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS - {"schema_epoch_control"}
)

OPERATIONAL_PHYSICAL_DOMAIN_WRITERS: tuple[PhysicalDomainEntrypoint, ...] = (
    VNextQueueRepository.request_download,
    VNextQueueRepository.ensure_download_request,
    VNextQueueRepository.complete_download_request,
    VNextQueueRepository.request_deletion,
    OperationalEffectRepository.begin,
    OperationalEffectRepository.append_batch,
    OperationalEffectRepository.seal,
    VNextCleanupRepository.begin_cycle,
    VNextCleanupRepository.advance,
)

# Epoch control has a different transaction owner.  Keep it as an explicit
# singleton family instead of falsely mixing it into caller-owned UOW methods.
OPERATIONAL_SCHEMA_EPOCH_WRITERS: tuple[PhysicalDomainEntrypoint, ...] = (
    SchemaEpochRunner.run,
)

OPERATIONAL_PHYSICAL_DOMAIN_GUARDS: tuple[PhysicalDomainGuard, ...] = (
    vnext_domains.require_bounded_bytes,
    vnext_domains.require_digest32,
    vnext_domains.require_int63,
    vnext_domains.require_positive_int63,
    vnext_domains.require_text,
    vnext_domains.require_uuid16,
)


def _validate_closed_family(
    entrypoints: tuple[PhysicalDomainEntrypoint, ...],
    guards: tuple[PhysicalDomainGuard, ...],
) -> None:
    if not entrypoints or len({id(value) for value in entrypoints}) != len(entrypoints):
        raise RuntimeError("physical-domain writer family is empty or duplicated")
    for entrypoint in entrypoints:
        module_name = getattr(entrypoint, "__module__", "")
        qualified_name = getattr(entrypoint, "__qualname__", "")
        parts = qualified_name.split(".")
        if (
            not module_name.startswith("h2hdb.")
            or len(parts) != 2
            or parts[1].startswith("_")
            or getattr(globals().get(parts[0]), parts[1], None) is not entrypoint
        ):
            raise RuntimeError(
                f"physical-domain entrypoint {module_name}.{qualified_name} "
                "is not an installed public method"
            )
    if not guards or len({id(value) for value in guards}) != len(guards):
        raise RuntimeError("physical-domain guard family is empty or duplicated")
    for guard in guards:
        name = getattr(guard, "__name__", "")
        if getattr(vnext_domains, name, None) is not guard:
            raise RuntimeError("physical-domain guard is not an installed symbol")


_validate_closed_family(
    CATALOG_PHYSICAL_DOMAIN_WRITERS,
    CATALOG_PHYSICAL_DOMAIN_GUARDS,
)
_validate_closed_family(
    OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
)
_validate_closed_family(
    OPERATIONAL_SCHEMA_EPOCH_WRITERS,
    (vnext_domains.require_int63,),
)
