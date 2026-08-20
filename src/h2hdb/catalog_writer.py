"""Closed-world bindings for vNext transaction writer obligations.

The formal artifact names a recurring writer hook for every semantic
obligation. A hook is installed only when the wheel can point at the complete
production repository family that owns the mutation. Bindings contain real
unbound method objects, not callback-shaped markers, and are resolved by the
full obligation ID/name/version tuple.

Any absent or inexact obligation deliberately fails closed. A READY validator,
a lambda, or a caller-supplied no-op is not transaction-refinement evidence.
"""

from __future__ import annotations

__all__ = [
    "BUILTIN_WRITER_HOOK_BINDINGS",
    "BUILTIN_WRITER_HOOKS",
    "WriterHook",
    "WriterHookBinding",
    "WriterHookFamily",
    "WriterHookUnavailableError",
    "WriterTransactionOwner",
    "resolve_writer_hook",
    "validate_artifact_writer_manifest",
    "validate_resolved_writer_hook_binding",
]

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib import import_module
from types import MappingProxyType
from typing import Any

from ._generated_vnext_schema import ARTIFACT
from .schema_epoch import SchemaEpochRunner
from .vnext_allocator_repository import VNextAllocatorRepository
from .vnext_analysis_repository import AnalysisRepository
from .vnext_artifact_preparation_repository import ArtifactPreparationRepository
from .vnext_artifact_release_repository import ArtifactReleaseRepository
from .vnext_canonical_value_repository import CanonicalValueRepository
from .vnext_cleanup_repository import VNextCleanupRepository
from .vnext_download_ingest_repository import DownloadIngestRepository
from .vnext_gallery_identity_repository import GalleryIdentityRepository
from .vnext_gallery_staging_repository import GalleryObservationStagingRepository
from .vnext_hash_cache_repository import VNextHashCacheRepository
from .vnext_ingest_fence_repository import IngestFenceRepository
from .vnext_maintenance_gate_repository import MaintenanceGateRepository
from .vnext_operational_event_repository import OperationalEffectRepository
from .vnext_physical_domains import (
    CATALOG_PHYSICAL_DOMAIN_GUARDS,
    CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_SCHEMA_EPOCH_WRITERS,
)
from .vnext_publication_candidate_repository import PublicationCandidateRepository
from .vnext_publication_finalization_repository import (
    PublicationFinalizationRepository,
)
from .vnext_publication_repository import PublicationRepository
from .vnext_queue_repository import VNextQueueRepository
from .vnext_source_build_repository import SourceBuildRepository


class WriterHookUnavailableError(RuntimeError):
    """A formal writer hook has no exact production transaction binding."""


@dataclass(frozen=True, slots=True)
class WriterHook:
    obligation_id: str
    name: str
    version: int


_SPECS: tuple[tuple[str, str], ...] = (
    ("catalog.identity-codecs.v1", "catalog_writer.validate_identity_codecs"),
    (
        "catalog.canonical-reference-domains.v1",
        "catalog_writer.validate_canonical_reference_domain",
    ),
    (
        "catalog.source-baseline-channel.v1",
        "catalog_writer.validate_source_baseline_channel_cas",
    ),
    (
        "catalog.incremental-impact.v1",
        "catalog_writer.validate_incremental_impact_freeze",
    ),
    (
        "catalog.overlay-resolution-seal.v1",
        "catalog_writer.validate_overlay_component_transition",
    ),
    (
        "catalog.artifact-semantics.v1",
        "catalog_writer.validate_artifact_semantics",
    ),
    (
        "catalog.publication-atomicity.v1",
        "catalog_writer.validate_publication_transition",
    ),
    ("catalog.state-machines.v1", "catalog_writer.validate_state_transition"),
    ("catalog.role-derivation.v1", "catalog_writer.validate_file_role"),
    ("catalog.physical-domains.v1", "catalog_writer.validate_physical_domain"),
    ("catalog.bootstrap.v1", "schema_epoch.write_catalog_bootstrap"),
    ("catalog.retention.v1", "catalog_writer.validate_retention_transition"),
    (
        "h2hdb.operational.physical-domains.v1",
        "operational_writer.validate_physical_domains",
    ),
    (
        "h2hdb.operational.epoch-manifest.v1",
        "schema_epoch.validate_operational_manifest",
    ),
    (
        "h2hdb.operational.fencing.v1",
        "operational_writer.validate_ingest_fencing",
    ),
    (
        "h2hdb.operational.download-ingest-handoff.v1",
        "operational_writer.validate_download_ingest_handoff",
    ),
    (
        "h2hdb.operational.maintenance-gate.v1",
        "operational_writer.validate_maintenance_gate",
    ),
    (
        "h2hdb.operational.bounded-work.v1",
        "operational_writer.validate_bounded_work",
    ),
    (
        "h2hdb.operational.queue-history.v1",
        "operational_writer.validate_queue_history",
    ),
    (
        "h2hdb.operational.canonical-hash-cache.v1",
        "operational_writer.validate_canonical_hash_cache",
    ),
    (
        "h2hdb.operational.event-integrity.v1",
        "operational_writer.validate_event_integrity",
    ),
    (
        "h2hdb.operational.build-generation.v1",
        "operational_writer.validate_build_generation",
    ),
    (
        "h2hdb.operational.attempt-identity.v1",
        "operational_writer.validate_attempt_identity",
    ),
    (
        "h2hdb.operational.cleanup-reachability.v1",
        "operational_writer.validate_cleanup_reachability",
    ),
    (
        "h2hdb.operational.revision-allocation.v1",
        "operational_writer.validate_revision_allocation",
    ),
    (
        "h2hdb.operational.gallery-staging.v1",
        "operational_writer.validate_gallery_staging",
    ),
    (
        "h2hdb.operational.bootstrap-genesis.v1",
        "schema_epoch.write_operational_bootstrap",
    ),
)

BUILTIN_WRITER_HOOKS = tuple(
    WriterHook(obligation_id, name, 1) for obligation_id, name in _SPECS
)
_HOOKS_BY_ID: Mapping[str, WriterHook] = MappingProxyType(
    {hook.obligation_id: hook for hook in BUILTIN_WRITER_HOOKS}
)


class WriterTransactionOwner(StrEnum):
    """The boundary that owns commit/rollback for a writer family."""

    CALLER_UOW = "caller_uow"
    SCHEMA_EPOCH_RUNNER = "schema_epoch_runner"


type WriterEntrypoint = Callable[..., object]
type DomainGuard = Callable[..., Any]

_PHYSICAL_DOMAIN_IDS = frozenset(
    {
        "catalog.physical-domains.v1",
        "h2hdb.operational.physical-domains.v1",
    }
)

_PRODUCTION_METHOD_OWNERS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "h2hdb.schema_epoch": frozenset({"SchemaEpochRunner"}),
        "h2hdb.vnext_allocator_repository": frozenset({"VNextAllocatorRepository"}),
        "h2hdb.vnext_analysis_repository": frozenset({"AnalysisRepository"}),
        "h2hdb.vnext_artifact_preparation_repository": frozenset(
            {"ArtifactPreparationRepository"}
        ),
        "h2hdb.vnext_artifact_release_repository": frozenset(
            {"ArtifactReleaseRepository"}
        ),
        "h2hdb.vnext_canonical_value_repository": frozenset(
            {"CanonicalValueRepository"}
        ),
        "h2hdb.vnext_cleanup_repository": frozenset({"VNextCleanupRepository"}),
        "h2hdb.vnext_download_ingest_repository": frozenset(
            {"DownloadIngestRepository"}
        ),
        "h2hdb.vnext_gallery_identity_repository": frozenset(
            {"GalleryIdentityRepository"}
        ),
        "h2hdb.vnext_gallery_staging_repository": frozenset(
            {"GalleryObservationStagingRepository"}
        ),
        "h2hdb.vnext_hash_cache_repository": frozenset({"VNextHashCacheRepository"}),
        "h2hdb.vnext_ingest_fence_repository": frozenset({"IngestFenceRepository"}),
        "h2hdb.vnext_maintenance_gate_repository": frozenset(
            {"MaintenanceGateRepository"}
        ),
        "h2hdb.vnext_operational_event_repository": frozenset(
            {"OperationalEffectRepository"}
        ),
        "h2hdb.vnext_publication_candidate_repository": frozenset(
            {"PublicationCandidateRepository"}
        ),
        "h2hdb.vnext_publication_finalization_repository": frozenset(
            {"PublicationFinalizationRepository"}
        ),
        "h2hdb.vnext_publication_repository": frozenset({"PublicationRepository"}),
        "h2hdb.vnext_queue_repository": frozenset({"VNextQueueRepository"}),
        "h2hdb.vnext_source_build_repository": frozenset({"SourceBuildRepository"}),
    }
)


def _validate_production_entrypoint(entrypoint: WriterEntrypoint) -> None:
    if not callable(entrypoint):
        raise WriterHookUnavailableError("writer binding entrypoint is not callable")
    module_name = getattr(entrypoint, "__module__", None)
    qualified_name = getattr(entrypoint, "__qualname__", None)
    if not isinstance(module_name, str) or not isinstance(qualified_name, str):
        raise WriterHookUnavailableError(
            "writer binding entrypoint lacks a stable production symbol"
        )
    parts = qualified_name.split(".")
    if len(parts) != 2 or parts[1].startswith("_") or parts[1] == "<lambda>":
        raise WriterHookUnavailableError(
            f"writer binding entrypoint {qualified_name!r} is not a public method"
        )
    owner_name, method_name = parts
    if owner_name not in _PRODUCTION_METHOD_OWNERS.get(module_name, frozenset()):
        raise WriterHookUnavailableError(
            f"writer binding entrypoint {module_name}.{qualified_name} is not an "
            "approved production repository method"
        )
    module = import_module(module_name)
    owner = getattr(module, owner_name, None)
    if getattr(owner, method_name, None) is not entrypoint:
        raise WriterHookUnavailableError(
            f"writer binding entrypoint {module_name}.{qualified_name} is not the "
            "installed method object"
        )


def _validate_relation_set(label: str, value: frozenset[str]) -> None:
    if type(value) is not frozenset or not value:
        raise WriterHookUnavailableError(f"writer binding {label} must be non-empty")
    if any(
        not isinstance(relation, str)
        or not relation
        or relation.casefold() != relation
        or not relation.replace("_", "").isalnum()
        for relation in value
    ):
        raise WriterHookUnavailableError(
            f"writer binding {label} contains an invalid relation name"
        )


@dataclass(frozen=True, slots=True)
class WriterHookFamily:
    """One immutable mutation family with one honest transaction owner."""

    entrypoints: tuple[WriterEntrypoint, ...]
    transaction_owner: WriterTransactionOwner
    mutation_relations: frozenset[str]

    def __post_init__(self) -> None:
        if type(self.entrypoints) is not tuple or not self.entrypoints:
            raise WriterHookUnavailableError(
                "writer family must contain at least one production entrypoint"
            )
        if len({id(entrypoint) for entrypoint in self.entrypoints}) != len(
            self.entrypoints
        ):
            raise WriterHookUnavailableError("writer family repeats an entrypoint")
        for entrypoint in self.entrypoints:
            _validate_production_entrypoint(entrypoint)
        if not isinstance(self.transaction_owner, WriterTransactionOwner):
            raise WriterHookUnavailableError(
                "writer family transaction_owner is not closed-world typed"
            )
        _validate_relation_set("mutation_relations", self.mutation_relations)


def _validate_domain_guard(guard: DomainGuard) -> None:
    if not callable(guard):
        raise WriterHookUnavailableError("domain guard is not callable")
    module_name = getattr(guard, "__module__", None)
    name = getattr(guard, "__name__", None)
    if (
        module_name != "h2hdb.vnext_domains"
        or not isinstance(name, str)
        or not name.startswith("require_")
        or name == "<lambda>"
        or getattr(import_module(module_name), name, None) is not guard
    ):
        raise WriterHookUnavailableError(
            "writer binding domain guard is not an installed vnext_domains symbol"
        )


@dataclass(frozen=True, slots=True)
class WriterHookBinding:
    """Immutable evidence for every transaction family of one obligation."""

    obligation_id: str
    name: str
    version: int
    families: tuple[WriterHookFamily, ...]
    authority_relations: frozenset[str]
    domain_guards: tuple[DomainGuard, ...] = ()

    def __post_init__(self) -> None:
        expected = _HOOKS_BY_ID.get(self.obligation_id)
        if expected is None:
            raise WriterHookUnavailableError(
                f"writer binding has unknown obligation ID {self.obligation_id!r}"
            )
        if (self.name, self.version) != (expected.name, expected.version):
            raise WriterHookUnavailableError(
                f"writer binding {self.obligation_id!r} has the wrong name/version"
            )
        if type(self.families) is not tuple or not self.families:
            raise WriterHookUnavailableError(
                "writer binding must contain at least one transaction family"
            )
        if not all(type(family) is WriterHookFamily for family in self.families):
            raise WriterHookUnavailableError(
                "writer binding contains a non-canonical transaction family type"
            )
        entrypoints = self.entrypoints
        if len({id(entrypoint) for entrypoint in entrypoints}) != len(entrypoints):
            raise WriterHookUnavailableError(
                f"writer binding {self.obligation_id!r} repeats an entrypoint "
                "across transaction families"
            )
        if type(self.domain_guards) is not tuple:
            raise WriterHookUnavailableError(
                "writer binding domain_guards must be an immutable tuple"
            )
        if len({id(guard) for guard in self.domain_guards}) != len(self.domain_guards):
            raise WriterHookUnavailableError("writer binding repeats a domain guard")
        for guard in self.domain_guards:
            _validate_domain_guard(guard)
        if (self.obligation_id in _PHYSICAL_DOMAIN_IDS) != bool(self.domain_guards):
            raise WriterHookUnavailableError(
                "only physical-domain bindings require installed domain guards"
            )
        _validate_relation_set("authority_relations", self.authority_relations)
        if not self.mutation_relations <= self.authority_relations:
            raise WriterHookUnavailableError(
                f"writer binding {self.obligation_id!r} mutates outside its "
                "authority relation closure"
            )

    @property
    def entrypoints(self) -> tuple[WriterEntrypoint, ...]:
        return tuple(
            entrypoint for family in self.families for entrypoint in family.entrypoints
        )

    @property
    def mutation_relations(self) -> frozenset[str]:
        return frozenset(
            relation
            for family in self.families
            for relation in family.mutation_relations
        )

    @property
    def transaction_owners(self) -> frozenset[WriterTransactionOwner]:
        return frozenset(family.transaction_owner for family in self.families)


def _contract_relations(obligation_id: str) -> frozenset[str]:
    records = ARTIFACT.get("semantic_obligations")
    if not isinstance(records, tuple):
        raise WriterHookUnavailableError(
            "generated semantic-obligation manifest is malformed"
        )
    matches = tuple(
        record
        for record in records
        if isinstance(record, Mapping) and record.get("id") == obligation_id
    )
    if len(matches) != 1:
        raise WriterHookUnavailableError(
            f"generated obligation {obligation_id!r} is not singular"
        )
    contract = matches[0].get("contract")
    relations = contract.get("relations") if isinstance(contract, Mapping) else None
    if (
        not isinstance(relations, list)
        or not relations
        or not all(isinstance(value, str) and value for value in relations)
        or len(relations) != len(set(relations))
    ):
        raise WriterHookUnavailableError(
            f"generated obligation {obligation_id!r} has no exact relation closure"
        )
    return frozenset(relations)


def _known_relation_names() -> frozenset[str]:
    backends = ARTIFACT.get("backends")
    if not isinstance(backends, Mapping):
        raise WriterHookUnavailableError("generated backend registry is malformed")
    names = {"schema_epoch_control"}
    for payload in backends.values():
        if not isinstance(payload, Mapping):
            raise WriterHookUnavailableError(
                "generated backend relation registry is malformed"
            )
        relations = payload.get("relations")
        if not isinstance(relations, tuple):
            raise WriterHookUnavailableError(
                "generated backend relation registry is malformed"
            )
        for relation in relations:
            name = relation.get("relation") if isinstance(relation, Mapping) else None
            if not isinstance(name, str) or not name:
                raise WriterHookUnavailableError(
                    "generated backend relation name is malformed"
                )
            names.add(name)
    return frozenset(names)


def _binding(
    obligation_id: str,
    entrypoints: tuple[WriterEntrypoint, ...],
    mutation_relations: frozenset[str],
    *,
    transaction_owner: WriterTransactionOwner = WriterTransactionOwner.CALLER_UOW,
    additional_families: tuple[WriterHookFamily, ...] = (),
    domain_guards: tuple[DomainGuard, ...] = (),
) -> WriterHookBinding:
    hook = _HOOKS_BY_ID[obligation_id]
    formal_relations = _contract_relations(obligation_id)
    families = (
        WriterHookFamily(entrypoints, transaction_owner, mutation_relations),
        *additional_families,
    )
    all_mutations = frozenset(
        relation for family in families for relation in family.mutation_relations
    )
    if not all_mutations <= _known_relation_names():
        raise WriterHookUnavailableError(
            f"writer binding {obligation_id!r} names a relation outside the "
            "generated physical schema"
        )
    return WriterHookBinding(
        obligation_id=hook.obligation_id,
        name=hook.name,
        version=hook.version,
        families=families,
        authority_relations=formal_relations | all_mutations,
        domain_guards=domain_guards,
    )


_ANALYSIS_BATCH_WRITERS: tuple[WriterEntrypoint, ...] = (
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

_ARTIFACT_BATCH_WRITERS: tuple[WriterEntrypoint, ...] = (
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

_ARTIFACT_PRODUCER_REGISTRY_RELATIONS = frozenset(
    {
        "artifact_producer_fingerprint_anchor",
        "artifact_producer_fingerprint_algorithm_version",
        "artifact_producer_fingerprint_equivalence_class",
        "artifact_producer_fingerprint_identity",
        "artifact_producer_fingerprint_seal",
    }
)

_PUBLICATION_CANDIDATE_BATCH_WRITERS: tuple[WriterEntrypoint, ...] = (
    PublicationCandidateRepository.process_selection_batch,
    PublicationCandidateRepository.validate_selection_batch,
    PublicationCandidateRepository.process_catalog_projection_batch,
    PublicationCandidateRepository.validate_catalog_projection_batch,
)

_GALLERY_STAGING_WRITERS: tuple[WriterEntrypoint, ...] = (
    GalleryObservationStagingRepository.begin,
    GalleryObservationStagingRepository.begin_from_identity,
    GalleryObservationStagingRepository.resume,
    GalleryObservationStagingRepository.takeover,
    GalleryObservationStagingRepository.put_files,
    GalleryObservationStagingRepository.put_directories,
    GalleryObservationStagingRepository.put_tags,
    GalleryObservationStagingRepository.put_metadata,
    GalleryObservationStagingRepository.match_files_to_directory,
    GalleryObservationStagingRepository.seal,
)

_IDENTITY_WRITERS: tuple[WriterEntrypoint, ...] = (
    CanonicalValueRepository.allocate,
    CanonicalValueRepository.put_page,
    CanonicalValueRepository.seal,
    SourceBuildRepository.handoff_root,
    SourceBuildRepository.resolve_discovery_locator,
    GalleryIdentityRepository.handoff_locator,
    GalleryObservationStagingRepository.begin,
    GalleryObservationStagingRepository.begin_from_identity,
    GalleryObservationStagingRepository.put_files,
    GalleryObservationStagingRepository.put_directories,
    GalleryObservationStagingRepository.put_tags,
    GalleryObservationStagingRepository.put_metadata,
    GalleryObservationStagingRepository.match_files_to_directory,
    GalleryObservationStagingRepository.seal,
    AnalysisRepository.begin,
    *_ANALYSIS_BATCH_WRITERS,
    AnalysisRepository.handoff_snapshot_manifest,
    PublicationCandidateRepository.begin,
    PublicationCandidateRepository.process_selection_batch,
    PublicationCandidateRepository.process_catalog_projection_batch,
    PublicationCandidateRepository.validate_catalog_projection_batch,
    *_ARTIFACT_BATCH_WRITERS,
    ArtifactPreparationRepository.persist_prepared_artifact,
    ArtifactPreparationRepository.confirm_prepared_artifact,
    ArtifactPreparationRepository.bind_operational_preparation,
    PublicationRepository.commit,
)

_CANONICAL_REFERENCE_WRITERS: tuple[WriterEntrypoint, ...] = (
    CanonicalValueRepository.allocate,
    CanonicalValueRepository.put_page,
    CanonicalValueRepository.seal,
    SourceBuildRepository.handoff_root,
    SourceBuildRepository.resolve_discovery_locator,
    GalleryIdentityRepository.handoff_locator,
    GalleryObservationStagingRepository.put_files,
    GalleryObservationStagingRepository.put_directories,
    GalleryObservationStagingRepository.put_tags,
    GalleryObservationStagingRepository.put_metadata,
    GalleryObservationStagingRepository.match_files_to_directory,
    GalleryObservationStagingRepository.seal,
    VNextHashCacheRepository.handoff,
    *_ANALYSIS_BATCH_WRITERS,
    AnalysisRepository.handoff_snapshot_manifest,
    PublicationCandidateRepository.process_catalog_projection_batch,
    ArtifactPreparationRepository.process_artifact_input_batch,
    ArtifactPreparationRepository.persist_prepared_artifact,
    PublicationRepository.commit,
)

_INCREMENTAL_WRITERS: tuple[WriterEntrypoint, ...] = (
    AnalysisRepository.process_changed_gallery_batch,
    AnalysisRepository.process_changed_file_hash_batch,
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

_ROLE_DERIVATION_WRITERS: tuple[WriterEntrypoint, ...] = (
    GalleryObservationStagingRepository.put_files,
    AnalysisRepository.process_changed_file_hash_batch,
    AnalysisRepository.process_file_hash_decision_batch,
    AnalysisRepository.validate_file_hash_decision_batch,
    AnalysisRepository.process_impacted_content_batch,
    AnalysisRepository.process_content_owner_candidate_batch,
    AnalysisRepository.validate_content_owner_candidate_batch,
    AnalysisRepository.process_content_owner_batch,
    AnalysisRepository.validate_content_owner_batch,
)

_DOWNLOAD_INGEST_WRITERS: tuple[WriterEntrypoint, ...] = (
    DownloadIngestRepository.claim_download,
    DownloadIngestRepository.resume_download,
    DownloadIngestRepository.renew_download,
    DownloadIngestRepository.handoff_download,
    DownloadIngestRepository.claim_ingest,
    DownloadIngestRepository.resume_ingest,
    DownloadIngestRepository.renew_ingest,
    DownloadIngestRepository.complete_ingest,
)

_BUILD_GENERATION_WRITERS: tuple[WriterEntrypoint, ...] = (
    SourceBuildRepository.handoff_root,
    SourceBuildRepository.prepare_discovery_batch,
    SourceBuildRepository.resolve_discovery_locator,
    SourceBuildRepository.commit_discovery_batch,
    SourceBuildRepository.issue_assembly_batch,
    SourceBuildRepository.assemble_batch,
)

_CLEANUP_WRITERS: tuple[WriterEntrypoint, ...] = (
    VNextCleanupRepository.begin_cycle,
    VNextCleanupRepository.resume_cycle,
    VNextCleanupRepository.advance,
)

_ARTIFACT_RELEASE_WRITERS: tuple[WriterEntrypoint, ...] = (
    ArtifactReleaseRepository.issue_page,
    ArtifactReleaseRepository.release_page,
    ArtifactReleaseRepository.commit_page,
)

_PUBLICATION_FINALIZATION_WRITERS: tuple[WriterEntrypoint, ...] = (
    PublicationFinalizationRepository.issue_page,
    PublicationFinalizationRepository.release_page,
    PublicationFinalizationRepository.commit_page,
)

_INGEST_FENCED_WRITERS: tuple[WriterEntrypoint, ...] = (
    CanonicalValueRepository.allocate,
    CanonicalValueRepository.put_page,
    CanonicalValueRepository.seal,
    ArtifactPreparationRepository.register_producer,
    GalleryIdentityRepository.handoff_locator,
    *_BUILD_GENERATION_WRITERS,
    *_GALLERY_STAGING_WRITERS,
    VNextHashCacheRepository.handoff,
    AnalysisRepository.begin,
    AnalysisRepository.abandon,
    *_ANALYSIS_BATCH_WRITERS,
    AnalysisRepository.handoff_snapshot_manifest,
    PublicationCandidateRepository.begin,
    *_PUBLICATION_CANDIDATE_BATCH_WRITERS,
    *_ARTIFACT_BATCH_WRITERS,
    ArtifactPreparationRepository.persist_prepared_artifact,
    ArtifactPreparationRepository.confirm_prepared_artifact,
    ArtifactPreparationRepository.bind_operational_preparation,
    OperationalEffectRepository.begin,
    OperationalEffectRepository.append_batch,
    OperationalEffectRepository.seal,
    PublicationRepository.commit,
)

_FENCE_AUTHORITY_WRITERS: tuple[WriterEntrypoint, ...] = (
    IngestFenceRepository.claim,
    IngestFenceRepository.renew,
    IngestFenceRepository.lock_and_require_live,
    IngestFenceRepository.lock_and_require_quiescent,
    IngestFenceRepository.complete,
    *_INGEST_FENCED_WRITERS,
)

_MAINTENANCE_GATE_AUTHORITY_WRITERS: tuple[WriterEntrypoint, ...] = (
    MaintenanceGateRepository.claim_shared,
    MaintenanceGateRepository.claim_exclusive,
    MaintenanceGateRepository.resume,
    MaintenanceGateRepository.renew,
    MaintenanceGateRepository.lock_and_require_live,
    MaintenanceGateRepository.release,
    *_INGEST_FENCED_WRITERS,
    *_CLEANUP_WRITERS,
    *_ARTIFACT_RELEASE_WRITERS,
    *_PUBLICATION_FINALIZATION_WRITERS,
)

_BOUNDED_WORK_WRITERS: tuple[WriterEntrypoint, ...] = (
    OperationalEffectRepository.begin,
    OperationalEffectRepository.append_batch,
    OperationalEffectRepository.seal,
    *_CLEANUP_WRITERS,
    *_PUBLICATION_FINALIZATION_WRITERS,
)

_QUEUE_HISTORY_WRITERS: tuple[WriterEntrypoint, ...] = (
    VNextQueueRepository.request_deletion,
    OperationalEffectRepository.begin,
    OperationalEffectRepository.append_batch,
    PublicationRepository.commit,
)

_CANONICAL_HASH_CACHE_WRITERS: tuple[WriterEntrypoint, ...] = (
    VNextHashCacheRepository.handoff,
    CanonicalValueRepository.allocate,
    CanonicalValueRepository.put_page,
    CanonicalValueRepository.seal,
)

_EVENT_INTEGRITY_WRITERS: tuple[WriterEntrypoint, ...] = (
    OperationalEffectRepository.begin,
    OperationalEffectRepository.append_batch,
    OperationalEffectRepository.seal,
    OperationalEffectRepository.acknowledge_through,
    ArtifactPreparationRepository.bind_operational_preparation,
    ArtifactPreparationRepository.confirm_prepared_artifact,
    PublicationRepository.commit,
)

_ATTEMPT_IDENTITY_WRITERS: tuple[WriterEntrypoint, ...] = (
    OperationalEffectRepository.begin,
    *_CLEANUP_WRITERS,
    PublicationRepository.commit,
)

_REVISION_ALLOCATION_WRITERS: tuple[WriterEntrypoint, ...] = (
    VNextAllocatorRepository.allocate_revision,
    PublicationCandidateRepository.begin,
    PublicationRepository.commit,
)

_BOUND_BINDINGS = (
    _binding(
        "catalog.identity-codecs.v1",
        _IDENTITY_WRITERS,
        _contract_relations("catalog.identity-codecs.v1"),
    ),
    _binding(
        "catalog.canonical-reference-domains.v1",
        _CANONICAL_REFERENCE_WRITERS,
        _contract_relations("catalog.canonical-reference-domains.v1")
        - {"canonical_digest_policy"},
    ),
    _binding(
        "catalog.source-baseline-channel.v1",
        (
            SourceBuildRepository.handoff_root,
            SourceBuildRepository.assemble_batch,
            AnalysisRepository.begin,
            AnalysisRepository.handoff_snapshot_manifest,
            PublicationCandidateRepository.begin,
            PublicationRepository.commit,
        ),
        frozenset(
            {
                "source_build",
                "source_build_channel",
                "source_build_base_source",
                "analysis_run",
                "source_snapshot_manifest_identity",
                "analysis_snapshot_manifest",
                "source_revision",
                "source_revision_provenance",
                "source_head",
            }
        ),
    ),
    _binding(
        "catalog.incremental-impact.v1",
        _INCREMENTAL_WRITERS,
        frozenset(
            {
                "analysis_impacted_gallery",
                "analysis_impacted_content",
                "analysis_impacted_gid",
            }
        ),
    ),
    _binding(
        "catalog.overlay-resolution-seal.v1",
        (*_ANALYSIS_BATCH_WRITERS, AnalysisRepository.handoff_snapshot_manifest),
        _contract_relations("catalog.overlay-resolution-seal.v1") - {"analysis_stage"},
    ),
    _binding(
        "catalog.artifact-semantics.v1",
        (
            ArtifactPreparationRepository.register_producer,
            ArtifactPreparationRepository.issue_input_projection_authority,
            *_ARTIFACT_BATCH_WRITERS,
            ArtifactPreparationRepository.audit_inputs,
            ArtifactPreparationRepository.prepare_with_storage_adapter,
            ArtifactPreparationRepository.persist_prepared_artifact,
            ArtifactPreparationRepository.protect_prepared_artifact,
            ArtifactPreparationRepository.confirm_prepared_artifact,
            *_ARTIFACT_RELEASE_WRITERS,
            *_PUBLICATION_FINALIZATION_WRITERS,
        ),
        frozenset(
            {
                "artifact_semantic_input",
                "artifact_input",
                "artifact_delta_old",
                "artifact_delta_new",
                "artifact_operation",
                "prepared_artifact",
                "artifact_location",
                "publication_identity",
            }
        )
        | _ARTIFACT_PRODUCER_REGISTRY_RELATIONS,
    ),
    _binding(
        "catalog.publication-atomicity.v1",
        (
            PublicationCandidateRepository.begin,
            *_PUBLICATION_CANDIDATE_BATCH_WRITERS,
            *_ARTIFACT_BATCH_WRITERS,
            ArtifactPreparationRepository.persist_prepared_artifact,
            ArtifactPreparationRepository.confirm_prepared_artifact,
            ArtifactPreparationRepository.bind_operational_preparation,
            PublicationRepository.commit,
            *_PUBLICATION_FINALIZATION_WRITERS,
        ),
        _contract_relations("catalog.publication-atomicity.v1") - {"publication_stage"},
    ),
    _binding(
        "catalog.state-machines.v1",
        (
            SourceBuildRepository.assemble_batch,
            AnalysisRepository.abandon,
            *_ANALYSIS_BATCH_WRITERS,
            AnalysisRepository.handoff_snapshot_manifest,
            PublicationCandidateRepository.begin,
            *_PUBLICATION_CANDIDATE_BATCH_WRITERS,
            *_ARTIFACT_BATCH_WRITERS,
            ArtifactPreparationRepository.persist_prepared_artifact,
            ArtifactPreparationRepository.confirm_prepared_artifact,
            *_ARTIFACT_RELEASE_WRITERS,
            PublicationRepository.commit,
            *_PUBLICATION_FINALIZATION_WRITERS,
        ),
        _contract_relations("catalog.state-machines.v1"),
    ),
    _binding(
        "catalog.role-derivation.v1",
        _ROLE_DERIVATION_WRITERS,
        _contract_relations("catalog.role-derivation.v1"),
    ),
    _binding(
        "catalog.physical-domains.v1",
        CATALOG_PHYSICAL_DOMAIN_WRITERS,
        CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
        domain_guards=CATALOG_PHYSICAL_DOMAIN_GUARDS,
    ),
    _binding(
        "catalog.retention.v1",
        _CLEANUP_WRITERS,
        _contract_relations("catalog.retention.v1")
        - {"source_head", "publication_head"},
    ),
    _binding(
        "h2hdb.operational.physical-domains.v1",
        OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
        OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
        additional_families=(
            WriterHookFamily(
                OPERATIONAL_SCHEMA_EPOCH_WRITERS,
                WriterTransactionOwner.SCHEMA_EPOCH_RUNNER,
                frozenset({"schema_epoch_control"}),
            ),
        ),
        domain_guards=OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    ),
    _binding(
        "h2hdb.operational.epoch-manifest.v1",
        (SchemaEpochRunner.run,),
        frozenset({"schema_epoch_control"}),
        transaction_owner=WriterTransactionOwner.SCHEMA_EPOCH_RUNNER,
    ),
    _binding(
        "h2hdb.operational.fencing.v1",
        _FENCE_AUTHORITY_WRITERS,
        _contract_relations("h2hdb.operational.fencing.v1"),
    ),
    _binding(
        "h2hdb.operational.download-ingest-handoff.v1",
        _DOWNLOAD_INGEST_WRITERS,
        _contract_relations("h2hdb.operational.download-ingest-handoff.v1"),
    ),
    _binding(
        "h2hdb.operational.maintenance-gate.v1",
        _MAINTENANCE_GATE_AUTHORITY_WRITERS,
        _contract_relations("h2hdb.operational.maintenance-gate.v1"),
    ),
    _binding(
        "h2hdb.operational.bounded-work.v1",
        _BOUNDED_WORK_WRITERS,
        _contract_relations("h2hdb.operational.bounded-work.v1"),
    ),
    _binding(
        "h2hdb.operational.queue-history.v1",
        _QUEUE_HISTORY_WRITERS,
        _contract_relations("h2hdb.operational.queue-history.v1"),
    ),
    _binding(
        "h2hdb.operational.canonical-hash-cache.v1",
        _CANONICAL_HASH_CACHE_WRITERS,
        _contract_relations("h2hdb.operational.canonical-hash-cache.v1"),
    ),
    _binding(
        "h2hdb.operational.event-integrity.v1",
        _EVENT_INTEGRITY_WRITERS,
        _contract_relations("h2hdb.operational.event-integrity.v1"),
    ),
    _binding(
        "h2hdb.operational.build-generation.v1",
        _BUILD_GENERATION_WRITERS,
        _contract_relations("h2hdb.operational.build-generation.v1")
        - {
            "gallery_observation_stat_anchor",
            "gallery_observation_stat_file_count",
            "gallery_observation_stat_byte_count",
            "gallery_observation_stat_seal",
            "gallery_observation_stat",
        },
    ),
    _binding(
        "h2hdb.operational.attempt-identity.v1",
        _ATTEMPT_IDENTITY_WRITERS,
        _contract_relations("h2hdb.operational.attempt-identity.v1")
        - {"operational_policy"},
    ),
    _binding(
        "h2hdb.operational.cleanup-reachability.v1",
        (*_CLEANUP_WRITERS, *_ARTIFACT_RELEASE_WRITERS),
        _contract_relations("h2hdb.operational.cleanup-reachability.v1"),
    ),
    _binding(
        "h2hdb.operational.revision-allocation.v1",
        _REVISION_ALLOCATION_WRITERS,
        _contract_relations("h2hdb.operational.revision-allocation.v1"),
    ),
    _binding(
        "h2hdb.operational.gallery-staging.v1",
        _GALLERY_STAGING_WRITERS,
        _contract_relations("h2hdb.operational.gallery-staging.v1")
        - {"source_build", "gallery_identity"},
    ),
)


def _freeze_bindings(
    bindings: tuple[WriterHookBinding, ...],
) -> Mapping[str, WriterHookBinding]:
    installed: dict[str, WriterHookBinding] = {}
    for binding in bindings:
        if binding.obligation_id in installed:
            raise WriterHookUnavailableError(
                f"duplicate writer binding for {binding.obligation_id!r}"
            )
        installed[binding.obligation_id] = binding
    return MappingProxyType(installed)


BUILTIN_WRITER_HOOK_BINDINGS = _freeze_bindings(_BOUND_BINDINGS)


def resolve_writer_hook(
    obligation_id: str,
    name: str,
    version: int,
) -> WriterHookBinding:
    """Resolve only the exact installed obligation/name/version binding."""

    expected = _HOOKS_BY_ID.get(obligation_id)
    if expected is None:
        raise WriterHookUnavailableError(
            f"vNext writer hook has unknown obligation ID {obligation_id!r}"
        )
    if (name, version) != (expected.name, expected.version):
        raise WriterHookUnavailableError(
            f"vNext writer hook {obligation_id!r} has the wrong name/version"
        )
    binding = BUILTIN_WRITER_HOOK_BINDINGS.get(obligation_id)
    if binding is None:
        raise WriterHookUnavailableError(
            f"vNext writer hook {name!r} v{version} is known but not wired"
        )
    validate_resolved_writer_hook_binding(
        binding,
        obligation_id=obligation_id,
        name=name,
        version=version,
    )
    return binding


def validate_resolved_writer_hook_binding(
    binding: object,
    *,
    obligation_id: str,
    name: str,
    version: int,
) -> None:
    """Reject forged, stale, or callback-shaped resolver results."""

    if type(binding) is not WriterHookBinding:
        raise WriterHookUnavailableError(
            "writer hook resolver did not return a WriterHookBinding"
        )
    canonical = BUILTIN_WRITER_HOOK_BINDINGS.get(obligation_id)
    if canonical is None:
        raise WriterHookUnavailableError(
            f"writer hook {obligation_id!r} has no installed canonical binding"
        )
    if binding is not canonical:
        raise WriterHookUnavailableError(
            f"writer hook {obligation_id!r} returned a non-canonical binding"
        )
    if (binding.obligation_id, binding.name, binding.version) != (
        obligation_id,
        name,
        version,
    ):
        raise WriterHookUnavailableError(
            f"writer hook {obligation_id!r} resolved the wrong identity tuple"
        )
    if not _contract_relations(obligation_id) <= binding.authority_relations:
        raise WriterHookUnavailableError(
            f"writer hook {obligation_id!r} omits its generated relation authority"
        )


def validate_artifact_writer_manifest(obligations: Sequence[Mapping[str, Any]]) -> None:
    """Require exact generated IDs, hook names, versions, and ordering."""

    expected = tuple(
        (hook.obligation_id, hook.name, hook.version) for hook in BUILTIN_WRITER_HOOKS
    )
    actual: list[tuple[str, str, int]] = []
    for value in obligations:
        contract = value.get("contract")
        if not isinstance(contract, Mapping):
            raise WriterHookUnavailableError(
                "generated semantic obligation lacks contract payload"
            )
        obligation_id = value.get("id")
        name = contract.get("writer_hook")
        version = contract.get("writer_hook_version")
        if (
            not isinstance(obligation_id, str)
            or not isinstance(name, str)
            or not isinstance(version, int)
            or isinstance(version, bool)
        ):
            raise WriterHookUnavailableError(
                "generated writer-hook manifest record is malformed"
            )
        actual.append((obligation_id, name, version))
    if tuple(actual) != expected:
        raise WriterHookUnavailableError(
            "generated writer-hook manifest differs from the wheel registry"
        )
