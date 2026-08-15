"""Closed production writer families for the vNext physical-domain contracts.

This registry is deliberately executable wheel code.  It names installed
repository method objects and installed :mod:`h2hdb.vnext_domains` guards;
neither callback markers nor generated DDL checks count as a binding.
"""

from __future__ import annotations

__all__ = [
    "CATALOG_PHYSICAL_DOMAIN_GUARDS",
    "CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS",
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
from .vnext_canonical_value_repository import CanonicalValueRepository
from .vnext_cleanup_repository import VNextCleanupRepository
from .vnext_gallery_identity_repository import GalleryIdentityRepository
from .vnext_gallery_staging_repository import GalleryObservationStagingRepository
from .vnext_operational_event_repository import OperationalEffectRepository
from .vnext_publication_candidate_repository import PublicationCandidateRepository
from .vnext_publication_repository import PublicationRepository
from .vnext_queue_repository import VNextQueueRepository
from .vnext_source_build_repository import SourceBuildRepository

type PhysicalDomainEntrypoint = Callable[..., object]
type PhysicalDomainGuard = Callable[..., Any]


CATALOG_PHYSICAL_DOMAIN_RELATIONS = frozenset(
    {
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "source_locator_identity",
        "file_name_identity",
        "tag_term",
        "publication_identity",
        "artifact_identity",
        "artifact_location",
        "artifact_producer_fingerprint",
        "artifact_storage_codec",
        "gallery_observation_page",
        "gallery_observation_allocation_page",
        "gallery_observation_page_descriptor",
        "gallery_observation_page_key_bounds",
        "gallery_observation_page_child",
        "gallery_observation_discovery_fingerprint",
        "analysis_checkpoint",
        "publication_checkpoint",
        "publication_batch_receipt",
        "prepared_artifact",
    }
)

# The two immutable artifact registries are epoch-seeded and have no recurring
# repository mutation.  Every other contract relation has a caller-UOW writer.
CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS = CATALOG_PHYSICAL_DOMAIN_RELATIONS - {
    "artifact_producer_fingerprint",
    "artifact_storage_codec",
}

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
    GalleryIdentityRepository.handoff_locator,
    SourceBuildRepository.resolve_discovery_locator,
    GalleryObservationStagingRepository.put_files,
    GalleryObservationStagingRepository.put_directories,
    GalleryObservationStagingRepository.put_tags,
    GalleryObservationStagingRepository.put_metadata,
    GalleryObservationStagingRepository.seal,
    *_ANALYSIS_CHECKPOINT_WRITERS,
    PublicationCandidateRepository.begin,
    PublicationCandidateRepository.process_selection_batch,
    PublicationCandidateRepository.validate_selection_batch,
    PublicationCandidateRepository.process_catalog_projection_batch,
    PublicationCandidateRepository.validate_catalog_projection_batch,
    *_ARTIFACT_CHECKPOINT_WRITERS,
    ArtifactPreparationRepository.persist_prepared_artifact,
    PublicationRepository.finalize_artifacts,
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
