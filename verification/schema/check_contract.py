"""Validate the logical vNext catalog schema contract.

The checker intentionally reasons about declared semantic functional
dependencies, not SQL primary-key syntax.  Relations in this contract are
small enough that exhaustive subset enumeration is preferable to a heuristic:
candidate keys and BCNF are checked against F+ for every attribute subset.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import re
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FunctionalDependency:
    determinant: frozenset[str]
    dependent: frozenset[str]


@dataclass(frozen=True)
class ForeignKey:
    attributes: tuple[str, ...]
    relation: str
    referenced_attributes: tuple[str, ...]


@dataclass(frozen=True)
class ExternalRelation:
    """Candidate-key shape owned by a different closed-world manifest."""

    name: str
    attributes: tuple[str, ...]
    declared_keys: tuple[frozenset[str], ...]


@dataclass(frozen=True)
class Relation:
    name: str
    kind: str
    attributes: tuple[str, ...]
    functional_dependencies: tuple[FunctionalDependency, ...]
    declared_keys: tuple[frozenset[str], ...]
    foreign_keys: tuple[ForeignKey, ...]
    materialization: Mapping[str, Any] | None
    rationale: str = ""


@dataclass(frozen=True)
class Projection:
    relation: str
    attributes: frozenset[str]


@dataclass(frozen=True)
class Decomposition:
    name: str
    universal_attributes: frozenset[str]
    functional_dependencies: tuple[FunctionalDependency, ...]
    projections: tuple[Projection, ...]
    rationale: str


@dataclass(frozen=True)
class AttributeSemantic:
    name: str
    classification: str
    rationale: str


@dataclass(frozen=True)
class CanonicalDigestContract:
    policy_relation: str
    value_relation: str
    digest_attribute: str
    allocation_relation: str
    page_relation: str
    descriptor_relation: str
    parent_relation: str
    root_attribute: str
    byte_count_attribute: str
    algorithm: str
    algorithm_version: int
    encoding: str
    framing: str
    collision_model: str
    write_obligation: str
    read_obligation: str


@dataclass(frozen=True)
class CanonicalValuePageContract:
    codec_version: int
    prefix: str
    maximum_page_bytes: int
    chunk_maximum_bytes: int
    branch_capacity: int
    maximum_level: int
    maximum_byte_count: int
    framing: str
    leaf_record: str
    branch_record: str
    canonical_tree_rule: str
    collision_obligation: str
    seal_obligation: str
    cleanup_rule: str


@dataclass(frozen=True)
class SourceLocatorContract:
    identity_relation: str
    gallery_relation: str
    digest_attribute: str
    name_attribute: str
    canonical_value_relation: str
    canonical_digest_attribute: str
    digest_domain: str
    codec_version: int
    encoding: str
    framing: str
    write_obligation: str
    read_obligation: str


@dataclass(frozen=True)
class SourceRootContract:
    canonical_value_relation: str
    canonical_digest_attribute: str
    digest_domain: str
    codec_version: int
    encoding: str
    framing: str
    root_rule: str
    segment_rule: str
    adapter_rule: str
    write_obligation: str
    golden_root_payload_hex: str
    golden_root_sha256: str
    golden_nested_payload_hex: str
    golden_nested_sha256: str


@dataclass(frozen=True)
class AnalysisCandidateContract:
    content_relation: str
    gid_relation: str
    gallery_relation: str
    priority_attribute: str
    candidate_digest_attribute: str
    stable_tie_break_attributes: tuple[str, ...]
    encoding_version: int
    framing: str
    ordering_rule: str
    already_uploaded_marker_rule: str
    candidate_digest_framing: str
    runtime_obligation: str


@dataclass(frozen=True)
class AnalysisRunContract:
    relation: str
    natural_key: tuple[str, ...]
    manifest_attribute: str
    write_obligation: str
    attempt_rule: str


@dataclass(frozen=True)
class SourceScopeIdentityContract:
    relation: str
    key_attribute: str
    natural_key: tuple[str, ...]
    encoding_version: int
    framing: str
    collision_model: str
    write_obligation: str
    seal_obligation: str


@dataclass(frozen=True)
class EffectiveContentContract:
    reference_attribute: str
    canonical_value_relation: str
    canonical_digest_attribute: str
    digest_domain: str
    encoding_version: int
    framing: str
    collision_model: str
    write_obligation: str
    read_obligation: str


@dataclass(frozen=True)
class SourceSnapshotManifestContract:
    relation: str
    analysis_binding_relation: str
    digest_attribute: str
    canonical_value_relation: str
    canonical_digest_attribute: str
    digest_domain: str
    codec_version: int
    framing: str
    canonical_order: str
    decision_predicate: str
    write_obligation: str
    handoff_obligation: str
    publication_obligation: str
    retention: str


@dataclass(frozen=True)
class FileIdentityContract:
    relation: str
    key_attribute: str
    name_attribute: str
    role_attribute: str
    algorithm_version: int
    framing: str
    collision_model: str
    role_classifier_version: int
    metadata_name: str
    write_obligation: str
    read_obligation: str


@dataclass(frozen=True)
class GalleryObservationIdentityContract:
    relation: str
    gallery_attribute: str
    identifier_attribute: str
    digest_attribute: str
    canonical_value_relation: str
    canonical_digest_attribute: str
    digest_domain: str
    encoding_version: int
    framing: str
    write_obligation: str
    reuse_obligation: str


@dataclass(frozen=True)
class GalleryObservationPageContract:
    allocation_relation: str
    final_relation: str
    page_relation: str
    allocation_page_relation: str
    child_relation: str
    descriptor_relation: str
    key_bounds_relation: str
    tree_root_relation: str
    page_digest_attribute: str
    page_bytes_attribute: str
    algorithm: str
    codec_version: int
    prefix: str
    maximum_page_bytes: int
    maximum_entries: int
    file_leaf_capacity: int
    tag_leaf_capacity: int
    directory_leaf_capacity: int
    metadata_leaf_capacity: int
    metadata_chunk_maximum_bytes: int
    branch_capacity: int
    maximum_level: int
    maximum_items: int
    components: tuple[str, ...]
    node_kinds: tuple[str, ...]
    framing: str
    file_leaf_record: str
    tag_leaf_record: str
    directory_leaf_record: str
    metadata_leaf_record: str
    metadata_stream_framing: str
    branch_record: str
    canonical_tree_rule: str
    materialization_rule: str
    collision_obligation: str
    seal_obligation: str
    cleanup_rule: str
    golden_empty_file_page_hex: str
    golden_empty_file_page_sha256: str


@dataclass(frozen=True)
class TitleSortContract:
    policy_relation: str
    display_policy_relation: str
    sort_relation: str
    algorithm_attribute: str
    unicode_attribute: str
    runtime_obligation: str


@dataclass(frozen=True)
class PublicationBatchStage:
    name: str
    stage_order: int
    cursor_codec: str
    prerequisite: str
    sealed_scalar: str


@dataclass(frozen=True)
class PublicationAtomicContract:
    candidate_relation: str
    selection_relation: str
    artifact_input_relation: str
    artifact_component_relation: str
    operation_relation: str
    prepared_artifact_relation: str
    stage_relation: str
    projection_seal_relation: str
    checkpoint_relation: str
    batch_receipt_relation: str
    source_manifest_binding_relation: str
    revision_relation: str
    head_relation: str
    finalization_stage: str
    selection_rule: str
    cursor_codec_rule: str
    batch_rule: str
    projection_seal_rule: str
    batch_stages: tuple[PublicationBatchStage, ...]
    finalization_rule: str
    runtime_obligation: str


@dataclass(frozen=True)
class LongValueStorageContract:
    canonical_reference_relation: str
    canonical_reference_attribute: str
    canonical_reference_attributes: tuple[str, ...]
    direct_payload_attributes: tuple[str, ...]
    selection_rule: str
    rationale: str


@dataclass(frozen=True)
class ByteDomain:
    attribute: str
    maximum_bytes: int
    encoding: str
    source: str
    runtime_obligation: str


@dataclass(frozen=True)
class ArtifactDeltaContract:
    classification: str
    operation_relation: str
    old_state_relation: str
    new_state_relation: str
    semantic_component_relation: str
    semantic_components: tuple[str, ...]
    operations: tuple[str, ...]
    old_state_operations: tuple[str, ...]
    new_state_operations: tuple[str, ...]
    rebuild_rule: str
    unchanged_rule: str
    rename_rule: str


@dataclass(frozen=True)
class TransitionAuthorityContract:
    version: int
    gate_relations: tuple[str, ...]
    forbidden_digest_attributes: tuple[str, ...]
    audit_only_digest_attributes: tuple[str, ...]
    batch_key_attribute: str
    runtime_obligation: str
    ready_obligation: str


@dataclass(frozen=True)
class ArtifactByteProducerContract:
    policy_relation: str
    algorithm_attribute: str
    producer_relation: str
    zip_writer_policy_relation: str
    storage_codec_relation: str
    independent_parameters: tuple[str, ...]
    algorithm_bundle: tuple[str, ...]
    producer_fingerprint_framing: str
    producer_fingerprint_golden_payload_hex: str
    producer_fingerprint_golden_sha256: str
    runtime_obligation: str


@dataclass(frozen=True)
class ArtifactNameContract:
    relation: str
    gid_attribute: str
    name_attribute: str
    codec_version: int
    framing: str
    golden_gid: int
    golden_name_hex: str
    runtime_obligation: str


@dataclass(frozen=True)
class ArtifactLocatorContract:
    relation: str
    artifact_attribute: str
    locator_attribute: str
    storage_codec_version: int
    locator_codec_version: int
    components: tuple[str, ...]
    derivation: str
    golden_artifact_sha256: str
    golden_payload_hex: str
    golden_locator_sha256: str
    runtime_obligation: str


@dataclass(frozen=True)
class ArtifactProtectionTokenContract:
    relation: str
    storage_codec_relation: str
    codec_version: int
    exact_bytes: int
    receipt_framing: str
    token_framing: str
    golden_receipt_id: str
    golden_token_hex: str
    runtime_obligation: str


@dataclass(frozen=True)
class ArtifactMemberPlanContract:
    semantic_relation: str
    component_attribute: str
    component_kind: str
    canonical_value_relation: str
    canonical_digest_attribute: str
    plan_version: int
    framing: str
    entry_fields: tuple[str, ...]
    runtime_obligation: str
    ready_obligation: str


@dataclass(frozen=True)
class ArtifactMemberPlanEnum:
    entry_kind: Mapping[str, int]
    source_role: Mapping[str, int]
    transform_kind: Mapping[str, int]
    boolean_tags: Mapping[str, int]
    position_rule: str
    source_rule: str
    transform_rule: str


@dataclass(frozen=True)
class ArtifactComponentCodec:
    kind: str
    attribute: str
    digest_domain: str
    codec_version: int
    framing: str
    canonical_order: str
    golden_payload_hex: str
    golden_sha256: str


@dataclass(frozen=True)
class ArtifactSemanticsCodec:
    digest_domain: str
    codec_version: int
    framing: str
    golden_payload_hex: str
    golden_sha256: str


@dataclass(frozen=True)
class ZipCommentContract:
    codec_version: int
    framing: str
    write_obligation: str
    golden_payload_hex: str
    golden_payload_sha256: str


@dataclass(frozen=True)
class QueueHistoryContract:
    deletion_generation_relation: str
    deletion_generation_head_relation: str
    deletion_attempt_relation: str
    deletion_head_relation: str
    deletion_url_relation: str
    consumption_relation: str
    preparation_relation: str
    generation_rule: str
    publication_rule: str
    retention_rule: str
    rule: str


@dataclass(frozen=True)
class CanonicalHashCacheContract:
    canonical_value_relation: str
    canonical_allocation_relation: str
    canonical_page_relation: str
    canonical_digest_attribute: str
    canonical_policy_attribute: str
    canonical_byte_count_attribute: str
    canonical_root_attribute: str
    observation_relation: str
    source_digest_attribute: str
    fingerprint_digest_attribute: str
    source_domain: str
    fingerprint_domain: str
    write_obligation: str
    read_obligation: str


@dataclass(frozen=True)
class OperationalEventIntegrityContract:
    stream_relation: str
    preparation_relation: str
    seal_relation: str
    activation_relation: str
    candidate_binding_relation: str
    base_relation: str
    removed_subtype_relation: str
    deletion_subtype_relation: str
    removed_event_type: str
    deletion_event_type: str
    event_digest_codec: str
    chain_codec: str
    empty_chain_sha256: str
    stream_rule: str
    subtype_rule: str
    seal_rule: str
    activation_rule: str
    candidate_binding_rule: str
    ack_head_relation: str
    ack_rule: str
    cleanup_rule: str


@dataclass(frozen=True)
class SourceBuildGenerationContract:
    reservation_relation: str
    rule: str


@dataclass(frozen=True)
class CleanupAttemptContract:
    job_relation: str
    attempt_attribute: str
    allocation_rule: str


@dataclass(frozen=True)
class PreparationIdentityContract:
    preparation_relation: str
    policy_relation: str
    deletion_generation_relation: str
    natural_key: tuple[str, ...]
    rule: str


@dataclass(frozen=True)
class AnalysisStateComponent:
    name: str
    shadow_relation: str
    tombstone_relation: str
    resolved_relation: str


@dataclass(frozen=True)
class AnalysisBatchStage:
    name: str
    stage_order: int
    cursor_codec: str


@dataclass(frozen=True)
class AnalysisResolutionContract:
    mode: str
    max_overlay_depth: int
    baseline_relation: str
    anchor_relation: str
    ancestry_relation: str
    seal_relation: str
    snapshot_relation: str
    spam_relation: str
    content_owner_relation: str
    gid_winner_relation: str
    immutable_fact_relations: tuple[str, ...]
    delta_relations: tuple[str, ...]
    components: tuple[AnalysisStateComponent, ...]
    initialization: str
    snapshot_resolution: str
    delta_basis: str
    read_resolution: str
    compaction: str
    compaction_ancestry: str
    cleanup_guard: str
    cleanup_transition: str
    stage_relation: str
    checkpoint_relation: str
    batch_receipt_relation: str
    cursor_codec_rule: str
    batch_rule: str
    batch_stages: tuple[AnalysisBatchStage, ...]


@dataclass(frozen=True)
class SemanticObligation:
    """Executable validation identity for one closed-world semantic rule group."""

    id: str
    version: int
    classification: str
    lifecycle: str
    ready_check: str
    writer_hook: str
    writer_hook_version: int
    scope: str
    relations: tuple[str, ...]
    covers: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class BootstrapSeed:
    """Exact schema-version-owned bootstrap row."""

    id: str
    relation: str
    columns: tuple[str, ...]
    values: tuple[str, ...]


@dataclass(frozen=True)
class CanonicalReferenceRole:
    """Closed-world canonical digest domain for one logical FK attribute role."""

    attribute: str
    digest_domain: str
    relations: tuple[str, ...]


@dataclass(frozen=True)
class IdentityCodec:
    id: str
    target_attribute: str
    version: int
    framing: str
    golden_input_hex: str
    golden_sha256: str


@dataclass(frozen=True)
class RetentionContract:
    version: int
    indefinitely_retained_relations: tuple[str, ...]
    active_head_relation: str
    revision_relation: str
    provenance_relation: str
    analysis_relation: str
    build_relation: str
    semantic_obligation_id: str
    source_history_rule: str
    catalog_history_rule: str
    active_source_rule: str
    inactive_source_rule: str


@dataclass(frozen=True)
class RetentionForeignKeyBoundary:
    relation: str
    attributes: tuple[str, ...]
    referenced_relation: str
    referenced_attributes: tuple[str, ...]


@dataclass(frozen=True)
class RetentionSemanticBlocker:
    relation: str
    attributes: tuple[str, ...]
    root_attributes: tuple[str, ...]
    semantic_obligation_id: str


@dataclass(frozen=True)
class RetentionMachineGate:
    id: str
    semantic_obligation_id: str


@dataclass(frozen=True)
class RetentionTarget:
    target: str
    root_relation: str
    root_key: tuple[str, ...]
    external_blockers: tuple[RetentionForeignKeyBoundary, ...]
    retained_outliving: tuple[RetentionForeignKeyBoundary, ...]
    semantic_blockers: tuple[RetentionSemanticBlocker, ...]
    machine_gates: tuple[RetentionMachineGate, ...]
    derived_views: tuple[str, ...]
    child_phases: tuple[tuple[str, ...], ...]
    phase_selectors: tuple[RetentionForeignKeyBoundary, ...] = ()


@dataclass(frozen=True)
class Contract:
    contract_version: int
    name: str
    relations: tuple[Relation, ...]
    decompositions: tuple[Decomposition, ...]
    attribute_semantics: tuple[AttributeSemantic, ...] = ()
    scope: str = ""
    excluded_operational_components: tuple[str, ...] = ()
    artifact_delta_contract: ArtifactDeltaContract | None = None
    analysis_resolution_contract: AnalysisResolutionContract | None = None
    external_relations: tuple[ExternalRelation, ...] = ()
    excluded_data_plane_components: tuple[str, ...] = ()
    canonical_digest_contract: CanonicalDigestContract | None = None
    canonical_value_page_contract: CanonicalValuePageContract | None = None
    byte_domains: tuple[ByteDomain, ...] = ()
    source_locator_contract: SourceLocatorContract | None = None
    analysis_candidate_contract: AnalysisCandidateContract | None = None
    long_value_storage_contract: LongValueStorageContract | None = None
    artifact_byte_producer_contract: ArtifactByteProducerContract | None = None
    artifact_member_plan_contract: ArtifactMemberPlanContract | None = None
    artifact_member_plan_enum: ArtifactMemberPlanEnum | None = None
    queue_history_contract: QueueHistoryContract | None = None
    canonical_hash_cache_contract: CanonicalHashCacheContract | None = None
    operational_event_integrity_contract: OperationalEventIntegrityContract | None = (
        None
    )
    source_build_generation_contract: SourceBuildGenerationContract | None = None
    cleanup_attempt_contract: CleanupAttemptContract | None = None
    preparation_identity_contract: PreparationIdentityContract | None = None
    analysis_run_contract: AnalysisRunContract | None = None
    source_scope_identity_contract: SourceScopeIdentityContract | None = None
    effective_content_contract: EffectiveContentContract | None = None
    source_snapshot_manifest_contract: SourceSnapshotManifestContract | None = None
    publication_atomic_contract: PublicationAtomicContract | None = None
    file_identity_contract: FileIdentityContract | None = None
    gallery_observation_identity_contract: GalleryObservationIdentityContract | None = (
        None
    )
    title_sort_contract: TitleSortContract | None = None
    semantic_obligations: tuple[SemanticObligation, ...] = ()
    bootstrap_seeds: tuple[BootstrapSeed, ...] = ()
    canonical_reference_roles: tuple[CanonicalReferenceRole, ...] = ()
    identity_codecs: tuple[IdentityCodec, ...] = ()
    retention_contract: RetentionContract | None = None
    retention_targets: tuple[RetentionTarget, ...] = ()
    transition_authority_contract: TransitionAuthorityContract | None = None
    artifact_component_codecs: tuple[ArtifactComponentCodec, ...] = ()
    artifact_semantics_codec: ArtifactSemanticsCodec | None = None
    zip_comment_contract: ZipCommentContract | None = None
    source_root_contract: SourceRootContract | None = None
    gallery_observation_page_contract: GalleryObservationPageContract | None = None
    artifact_name_contract: ArtifactNameContract | None = None
    artifact_locator_contract: ArtifactLocatorContract | None = None
    artifact_protection_token_contract: ArtifactProtectionTokenContract | None = None


@dataclass(frozen=True)
class RelationReport:
    name: str
    candidate_keys: tuple[frozenset[str], ...]
    checked_determinants: int


@dataclass(frozen=True)
class ValidationReport:
    relations: tuple[RelationReport, ...]
    lossless_decompositions: tuple[str, ...]
    dependency_preserving_decompositions: tuple[str, ...]


class ContractError(ValueError):
    """Base error for malformed or invalid schema contracts."""


class ContractFormatError(ContractError):
    """The TOML document does not have the required shape."""


class ContractValidationError(ContractError):
    """The contract is well-shaped TOML but violates schema invariants."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("schema contract validation failed:\n- " + "\n- ".join(errors))


_FILESYSTEM_HASH_CACHE_DIGEST_DOMAINS = frozenset(
    {"filesystem_source_identity_v1", "filesystem_fingerprint_v1"}
)


def attribute_closure(
    seed: Iterable[str], functional_dependencies: Iterable[FunctionalDependency]
) -> frozenset[str]:
    """Return seed+ under the supplied functional dependencies."""

    closure = set(seed)
    dependencies = tuple(functional_dependencies)
    changed = True
    while changed:
        changed = False
        for dependency in dependencies:
            if dependency.determinant <= closure:
                before = len(closure)
                closure.update(dependency.dependent)
                changed = changed or len(closure) != before
    return frozenset(closure)


def enumerate_candidate_keys(
    attributes: Iterable[str],
    functional_dependencies: Iterable[FunctionalDependency],
) -> tuple[frozenset[str], ...]:
    """Exhaustively enumerate all inclusion-minimal superkeys."""

    ordered_attributes = tuple(sorted(set(attributes)))
    all_attributes = frozenset(ordered_attributes)
    dependencies = tuple(functional_dependencies)
    keys: list[frozenset[str]] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            candidate = frozenset(values)
            if any(key <= candidate for key in keys):
                continue
            if attribute_closure(candidate, dependencies) == all_attributes:
                keys.append(candidate)
    return tuple(keys)


def bcnf_violations(
    relation: Relation,
) -> tuple[tuple[frozenset[str], frozenset[str]], ...]:
    """Find all F+-implied BCNF violations by determinant enumeration."""

    attributes = frozenset(relation.attributes)
    ordered_attributes = tuple(sorted(attributes))
    violations: list[tuple[frozenset[str], frozenset[str]]] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            determinant = frozenset(values)
            closure = (
                attribute_closure(determinant, relation.functional_dependencies)
                & attributes
            )
            nontrivial = closure - determinant
            if nontrivial and closure != attributes:
                violations.append((determinant, nontrivial))
    return tuple(violations)


def is_binary_lossless(decomposition: Decomposition) -> bool:
    """Decide FD losslessness for a two-way decomposition.

    R -> R1,R2 is lossless under F exactly when the intersection functionally
    determines all of R1 or all of R2.
    """

    if len(decomposition.projections) != 2:
        raise ValueError("binary lossless checking requires exactly two projections")
    left, right = decomposition.projections
    intersection = left.attributes & right.attributes
    closure = attribute_closure(intersection, decomposition.functional_dependencies)
    return left.attributes <= closure or right.attributes <= closure


def project_functional_dependencies(
    attributes: Iterable[str],
    functional_dependencies: Iterable[FunctionalDependency],
) -> tuple[FunctionalDependency, ...]:
    """Return the exact F+ projection onto one relation schema.

    For every X ⊆ Ri, the projected set contains X → (X+ under F) ∩ Ri.
    Keeping the trivial attributes in the dependent makes the construction
    mirror the closure definition directly and does not change implication.
    """

    ordered_attributes = tuple(sorted(set(attributes)))
    projection = frozenset(ordered_attributes)
    dependencies = tuple(functional_dependencies)
    projected: list[FunctionalDependency] = []
    for size in range(len(ordered_attributes) + 1):
        for values in itertools.combinations(ordered_attributes, size):
            determinant = frozenset(values)
            dependent = attribute_closure(determinant, dependencies) & projection
            projected.append(FunctionalDependency(determinant, dependent))
    return tuple(projected)


def is_dependency_preserving(decomposition: Decomposition) -> bool:
    """Decide whether the union of all F+ projections implies every FD in F."""

    projected_dependencies = tuple(
        dependency
        for projection in decomposition.projections
        for dependency in project_functional_dependencies(
            projection.attributes, decomposition.functional_dependencies
        )
    )
    return all(
        dependency.dependent
        <= attribute_closure(dependency.determinant, projected_dependencies)
        for dependency in decomposition.functional_dependencies
    )


def load_contract(path: str | Path) -> Contract:
    """Load a contract from TOML and validate its document shape."""

    contract_path = Path(path)
    try:
        with contract_path.open("rb") as stream:
            document = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ContractFormatError(f"cannot read {contract_path}: {error}") from error

    try:
        version = _integer(document, "contract_version", "contract")
        name = _string(document, "name", "contract")
        raw_scope = document.get("scope", "")
        if not isinstance(raw_scope, str):
            raise ContractFormatError("contract.scope must be a string")
        scope = raw_scope
        raw_exclusions = document.get("excluded_operational_components", [])
        if not isinstance(raw_exclusions, list) or not all(
            isinstance(item, str) and item for item in raw_exclusions
        ):
            raise ContractFormatError(
                "contract.excluded_operational_components must be an array of strings"
            )
        excluded_operational_components = tuple(raw_exclusions)
        raw_data_plane_exclusions = document.get("excluded_data_plane_components", [])
        if not isinstance(raw_data_plane_exclusions, list) or not all(
            isinstance(item, str) and item for item in raw_data_plane_exclusions
        ):
            raise ContractFormatError(
                "contract.excluded_data_plane_components must be an array of strings"
            )
        excluded_data_plane_components = tuple(raw_data_plane_exclusions)
        external_relations = tuple(
            _parse_external_relation(value)
            for value in _optional_table_list(document, "external_relation", "contract")
        )
        raw_relations = _table_list(document, "relation", "contract")
        raw_decompositions = document.get("decomposition", [])
        if not isinstance(raw_decompositions, list) or not all(
            isinstance(value, dict) for value in raw_decompositions
        ):
            raise ContractFormatError(
                "contract.decomposition must be an array of tables"
            )
        relations = tuple(_parse_relation(value) for value in raw_relations)
        decompositions = tuple(
            _parse_decomposition(value) for value in raw_decompositions
        )
        semantics = tuple(
            _parse_attribute_semantic(value)
            for value in _optional_table_list(
                document, "attribute_semantic", "contract"
            )
        )
        semantic_obligations = tuple(
            _parse_semantic_obligation(value)
            for value in _optional_table_list(
                document, "semantic_obligation", "contract"
            )
        )
        bootstrap_seeds = tuple(
            _parse_bootstrap_seed(value)
            for value in _optional_table_list(document, "bootstrap_seed", "contract")
        )
        canonical_reference_roles = tuple(
            _parse_canonical_reference_role(value)
            for value in _optional_table_list(
                document, "canonical_reference_role", "contract"
            )
        )
        identity_codecs = tuple(
            _parse_identity_codec(value)
            for value in _optional_table_list(document, "identity_codec", "contract")
        )
        retention_contract = _parse_optional_contract_table(
            document, "retention_contract", _parse_retention_contract
        )
        retention_targets = tuple(
            _parse_retention_target(value)
            for value in _optional_table_list(document, "retention_target", "contract")
        )
        raw_delta_contract = document.get("artifact_delta_contract")
        artifact_delta_contract = (
            None
            if raw_delta_contract is None
            else _parse_artifact_delta_contract(
                _table(document, "artifact_delta_contract", "contract")
            )
        )
        transition_authority_contract = _parse_optional_contract_table(
            document,
            "transition_authority_contract",
            _parse_transition_authority_contract,
        )
        raw_byte_producer_contract = document.get("artifact_byte_producer_contract")
        artifact_byte_producer_contract = (
            None
            if raw_byte_producer_contract is None
            else _parse_artifact_byte_producer_contract(
                _table(document, "artifact_byte_producer_contract", "contract")
            )
        )
        raw_member_plan_contract = document.get("artifact_member_plan_contract")
        artifact_member_plan_contract = (
            None
            if raw_member_plan_contract is None
            else _parse_artifact_member_plan_contract(
                _table(document, "artifact_member_plan_contract", "contract")
            )
        )
        artifact_member_plan_enum = _parse_optional_contract_table(
            document,
            "artifact_member_plan_enum",
            _parse_artifact_member_plan_enum,
        )
        artifact_component_codecs = tuple(
            _parse_artifact_component_codec(value)
            for value in _optional_table_list(
                document, "artifact_component_codec", "contract"
            )
        )
        artifact_semantics_codec = _parse_optional_contract_table(
            document,
            "artifact_semantics_codec",
            _parse_artifact_semantics_codec,
        )
        artifact_name_contract = _parse_optional_contract_table(
            document,
            "artifact_name_contract",
            _parse_artifact_name_contract,
        )
        artifact_locator_contract = _parse_optional_contract_table(
            document,
            "artifact_locator_contract",
            _parse_artifact_locator_contract,
        )
        artifact_protection_token_contract = _parse_optional_contract_table(
            document,
            "artifact_protection_token_contract",
            _parse_artifact_protection_token_contract,
        )
        zip_comment_contract = _parse_optional_contract_table(
            document,
            "zip_comment_contract",
            _parse_zip_comment_contract,
        )
        raw_resolution_contract = document.get("analysis_resolution_contract")
        analysis_resolution_contract = (
            None
            if raw_resolution_contract is None
            else _parse_analysis_resolution_contract(
                _table(document, "analysis_resolution_contract", "contract")
            )
        )
        raw_digest_contract = document.get("canonical_digest_contract")
        canonical_digest_contract = (
            None
            if raw_digest_contract is None
            else _parse_canonical_digest_contract(
                _table(document, "canonical_digest_contract", "contract")
            )
        )
        canonical_value_page_contract = _parse_optional_contract_table(
            document,
            "canonical_value_page_contract",
            _parse_canonical_value_page_contract,
        )
        raw_source_locator_contract = document.get("source_locator_contract")
        source_locator_contract = (
            None
            if raw_source_locator_contract is None
            else _parse_source_locator_contract(
                _table(document, "source_locator_contract", "contract")
            )
        )
        source_root_contract = _parse_optional_contract_table(
            document,
            "source_root_contract",
            _parse_source_root_contract,
        )
        raw_analysis_candidate_contract = document.get("analysis_candidate_contract")
        analysis_candidate_contract = (
            None
            if raw_analysis_candidate_contract is None
            else _parse_analysis_candidate_contract(
                _table(document, "analysis_candidate_contract", "contract")
            )
        )
        analysis_run_contract = _parse_optional_contract_table(
            document, "analysis_run_contract", _parse_analysis_run_contract
        )
        source_scope_identity_contract = _parse_optional_contract_table(
            document,
            "source_scope_identity_contract",
            _parse_source_scope_identity_contract,
        )
        effective_content_contract = _parse_optional_contract_table(
            document,
            "effective_content_contract",
            _parse_effective_content_contract,
        )
        source_snapshot_manifest_contract = _parse_optional_contract_table(
            document,
            "source_snapshot_manifest_contract",
            _parse_source_snapshot_manifest_contract,
        )
        publication_atomic_contract = _parse_optional_contract_table(
            document,
            "publication_atomic_contract",
            _parse_publication_atomic_contract,
        )
        file_identity_contract = _parse_optional_contract_table(
            document,
            "file_identity_contract",
            _parse_file_identity_contract,
        )
        gallery_observation_identity_contract = _parse_optional_contract_table(
            document,
            "gallery_observation_identity_contract",
            _parse_gallery_observation_identity_contract,
        )
        gallery_observation_page_contract = _parse_optional_contract_table(
            document,
            "gallery_observation_page_contract",
            _parse_gallery_observation_page_contract,
        )
        title_sort_contract = _parse_optional_contract_table(
            document,
            "title_sort_contract",
            _parse_title_sort_contract,
        )
        raw_long_value_contract = document.get("long_value_storage_contract")
        long_value_storage_contract = (
            None
            if raw_long_value_contract is None
            else _parse_long_value_storage_contract(
                _table(document, "long_value_storage_contract", "contract")
            )
        )
        byte_domains = tuple(
            _parse_byte_domain(value)
            for value in _optional_table_list(document, "byte_domain", "contract")
        )
        queue_history_contract = _parse_optional_contract_table(
            document, "queue_history_contract", _parse_queue_history_contract
        )
        canonical_hash_cache_contract = _parse_optional_contract_table(
            document,
            "canonical_hash_cache_contract",
            _parse_canonical_hash_cache_contract,
        )
        operational_event_integrity_contract = _parse_optional_contract_table(
            document,
            "operational_event_integrity_contract",
            _parse_operational_event_integrity_contract,
        )
        source_build_generation_contract = _parse_optional_contract_table(
            document,
            "source_build_generation_contract",
            _parse_source_build_generation_contract,
        )
        cleanup_attempt_contract = _parse_optional_contract_table(
            document, "cleanup_attempt_contract", _parse_cleanup_attempt_contract
        )
        preparation_identity_contract = _parse_optional_contract_table(
            document,
            "preparation_identity_contract",
            _parse_preparation_identity_contract,
        )
    except ContractFormatError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ContractFormatError(f"invalid contract document: {error}") from error
    return Contract(
        version,
        name,
        relations,
        decompositions,
        semantics,
        scope,
        excluded_operational_components,
        artifact_delta_contract,
        analysis_resolution_contract,
        external_relations,
        excluded_data_plane_components,
        canonical_digest_contract,
        canonical_value_page_contract,
        byte_domains,
        source_locator_contract,
        analysis_candidate_contract,
        long_value_storage_contract,
        artifact_byte_producer_contract,
        artifact_member_plan_contract,
        artifact_member_plan_enum,
        queue_history_contract,
        canonical_hash_cache_contract,
        operational_event_integrity_contract,
        source_build_generation_contract,
        cleanup_attempt_contract,
        preparation_identity_contract,
        analysis_run_contract,
        source_scope_identity_contract,
        effective_content_contract,
        source_snapshot_manifest_contract,
        publication_atomic_contract,
        file_identity_contract,
        gallery_observation_identity_contract,
        title_sort_contract,
        semantic_obligations,
        bootstrap_seeds,
        canonical_reference_roles,
        identity_codecs,
        retention_contract,
        retention_targets,
        transition_authority_contract,
        artifact_component_codecs,
        artifact_semantics_codec,
        zip_comment_contract,
        source_root_contract,
        gallery_observation_page_contract,
        artifact_name_contract,
        artifact_locator_contract,
        artifact_protection_token_contract,
    )


def validate_contract(contract: Contract) -> ValidationReport:
    """Validate keys, F+ BCNF, and decomposition semantic properties."""

    errors: list[str] = []
    relation_by_name: dict[str, Relation] = {}
    reports: list[RelationReport] = []

    if contract.contract_version != 1:
        errors.append(f"contract_version must be 1, got {contract.contract_version!r}")
    if not contract.relations:
        errors.append("contract must declare at least one relation")
    supported_scopes = {"catalog_data_plane", "operational_control_plane"}
    if contract.scope and contract.scope not in supported_scopes:
        errors.append(
            "contract scope must be 'catalog_data_plane' or "
            f"'operational_control_plane', got {contract.scope!r}"
        )
    if (
        contract.scope == "catalog_data_plane"
        and not contract.excluded_operational_components
    ):
        errors.append(
            "catalog_data_plane contract must list excluded operational components"
        )
    if (
        contract.scope == "catalog_data_plane"
        and contract.excluded_data_plane_components
    ):
        errors.append(
            "catalog_data_plane contract must not list excluded data-plane components"
        )
    if (
        contract.scope == "operational_control_plane"
        and not contract.excluded_data_plane_components
    ):
        errors.append(
            "operational_control_plane contract must list excluded data-plane components"
        )
    if (
        contract.scope == "operational_control_plane"
        and contract.excluded_operational_components
    ):
        errors.append(
            "operational_control_plane contract must not list excluded operational components"
        )
    if len(set(contract.excluded_operational_components)) != len(
        contract.excluded_operational_components
    ):
        errors.append("excluded operational components contain duplicates")
    if len(set(contract.excluded_data_plane_components)) != len(
        contract.excluded_data_plane_components
    ):
        errors.append("excluded data-plane components contain duplicates")

    for relation in contract.relations:
        if relation.name in relation_by_name:
            errors.append(f"duplicate relation name {relation.name!r}")
        else:
            relation_by_name[relation.name] = relation

    external_by_name: dict[str, ExternalRelation] = {}
    for external in contract.external_relations:
        external_errors = _validate_external_relation(external)
        errors.extend(external_errors)
        if external.name in relation_by_name or external.name in external_by_name:
            errors.append(f"duplicate local/external relation name {external.name!r}")
        else:
            external_by_name[external.name] = external

    for relation in contract.relations:
        relation_errors, report = _validate_relation(relation)
        errors.extend(relation_errors)
        reports.append(report)

    for relation in contract.relations:
        errors.extend(
            _validate_foreign_keys(relation, relation_by_name, external_by_name)
        )
        errors.extend(_validate_materialization(relation, relation_by_name))

    errors.extend(_validate_attribute_semantics(contract, relation_by_name))
    errors.extend(_validate_byte_domains(contract, relation_by_name))
    if contract.canonical_digest_contract is not None:
        errors.extend(
            _validate_canonical_digest_contract(
                contract.canonical_digest_contract,
                contract.canonical_value_page_contract,
                relation_by_name,
            )
        )
    if contract.source_locator_contract is not None:
        errors.extend(
            _validate_source_locator_contract(
                contract.source_locator_contract,
                contract.byte_domains,
                relation_by_name,
            )
        )
    if contract.analysis_candidate_contract is not None:
        errors.extend(
            _validate_analysis_candidate_contract(
                contract.analysis_candidate_contract,
                relation_by_name,
            )
        )
    if contract.long_value_storage_contract is not None:
        errors.extend(
            _validate_long_value_storage_contract(
                contract.long_value_storage_contract,
                relation_by_name,
            )
        )
    if contract.artifact_delta_contract is not None:
        errors.extend(
            _validate_artifact_delta_contract(
                contract.artifact_delta_contract,
                relation_by_name,
            )
        )
    if contract.scope == "catalog_data_plane":
        errors.extend(
            _validate_transition_authority_contract(
                contract.transition_authority_contract,
                relation_by_name,
            )
        )
    if contract.artifact_byte_producer_contract is not None:
        errors.extend(
            _validate_artifact_byte_producer_contract(
                contract.artifact_byte_producer_contract,
                relation_by_name,
            )
        )
    if contract.artifact_member_plan_contract is not None:
        errors.extend(
            _validate_artifact_member_plan_contract(
                contract.artifact_member_plan_contract,
                contract.artifact_member_plan_enum,
                contract.artifact_delta_contract,
                relation_by_name,
            )
        )
    if contract.scope == "catalog_data_plane":
        errors.extend(_validate_artifact_codecs(contract))
        errors.extend(
            _validate_artifact_derived_identity_contracts(contract, relation_by_name)
        )
    if contract.analysis_resolution_contract is not None:
        errors.extend(
            _validate_analysis_resolution_contract(
                contract.analysis_resolution_contract,
                relation_by_name,
            )
        )
    if contract.scope == "catalog_data_plane":
        errors.extend(
            _validate_data_plane_integrity_contracts(contract, relation_by_name)
        )
        errors.extend(_validate_retention_contract(contract, relation_by_name))
        errors.extend(_validate_data_semantic_obligations(contract, relation_by_name))
    if contract.scope == "operational_control_plane":
        errors.extend(
            _validate_operational_integrity_contracts(
                contract,
                relation_by_name,
                external_by_name,
            )
        )

    decomposition_names: set[str] = set()
    lossless: list[str] = []
    dependency_preserving: list[str] = []
    for decomposition in contract.decompositions:
        if decomposition.name in decomposition_names:
            errors.append(f"duplicate decomposition name {decomposition.name!r}")
        decomposition_names.add(decomposition.name)
        decomposition_errors = _validate_decomposition(decomposition, relation_by_name)
        errors.extend(decomposition_errors)
        if not decomposition_errors and is_binary_lossless(decomposition):
            lossless.append(decomposition.name)
        if not decomposition_errors and is_dependency_preserving(decomposition):
            dependency_preserving.append(decomposition.name)

    if errors:
        raise ContractValidationError(errors)
    return ValidationReport(
        tuple(reports), tuple(lossless), tuple(dependency_preserving)
    )


def validate_cross_manifest_contracts(
    catalog: Contract,
    operational: Contract,
) -> None:
    """Bind every operational external authority to its catalog-owned shape."""

    errors: list[str] = []
    if catalog.scope != "catalog_data_plane":
        errors.append("cross-manifest catalog contract has the wrong scope")
    if operational.scope != "operational_control_plane":
        errors.append("cross-manifest operational contract has the wrong scope")

    hash_cache = operational.canonical_hash_cache_contract
    if hash_cache is None:
        errors.append(
            "cross-manifest operational contract lacks canonical_hash_cache_contract"
        )
    else:
        operational_domains = frozenset(
            {hash_cache.source_domain, hash_cache.fingerprint_domain}
        )
        if operational_domains != _FILESYSTEM_HASH_CACHE_DIGEST_DOMAINS:
            errors.append(
                "operational canonical hash-cache domains must be exactly "
                "filesystem_source_identity_v1 and filesystem_fingerprint_v1"
            )

    seeded_domains = frozenset(
        seed.values[0]
        for seed in catalog.bootstrap_seeds
        if seed.relation == "canonical_digest_policy"
        and seed.columns == ("digest_domain",)
        and len(seed.values) == 1
    )
    missing_domains = _FILESYSTEM_HASH_CACHE_DIGEST_DOMAINS - seeded_domains
    if missing_domains:
        errors.append(
            "catalog canonical_digest_policy bootstrap does not register the "
            "operational hash-cache domains " + _format_set(missing_domains)
        )

    catalog_relations = {relation.name: relation for relation in catalog.relations}
    operational_relations = {
        relation.name: relation for relation in operational.relations
    }
    operational_externals = {
        relation.name: relation for relation in operational.external_relations
    }

    expected_catalog_shapes = {
        "source_build_expected_gallery": (
            ("build_id", "position", "gallery_id"),
            {
                frozenset({"build_id", "position"}),
                frozenset({"build_id", "gallery_id"}),
            },
        ),
        "gallery_observation_stat": (
            ("gallery_id", "observation_id", "file_count", "byte_count"),
            {frozenset({"gallery_id", "observation_id"})},
        ),
    }
    for relation_name, (attributes, keys) in expected_catalog_shapes.items():
        relation = catalog_relations.get(relation_name)
        if (
            relation is None
            or relation.attributes != attributes
            or set(relation.declared_keys) != keys
        ):
            errors.append(
                f"catalog {relation_name} must have the exact cross-manifest "
                "authority shape"
            )

    expected_external_shapes = {
        name: (attributes, keys)
        for name, (attributes, keys) in expected_catalog_shapes.items()
    }
    for relation_name, (attributes, keys) in expected_external_shapes.items():
        external_relation = operational_externals.get(relation_name)
        if (
            external_relation is None
            or external_relation.attributes != attributes
            or set(external_relation.declared_keys) != keys
        ):
            errors.append(
                f"operational external {relation_name} must exactly match catalog"
            )

    expected_operational_shapes = {
        "source_build_discovery_checkpoint": (
            (
                "build_id",
                "generation",
                "cursor_bytes",
                "processed_count",
                "state",
                "updated_at",
            ),
            {frozenset({"build_id"})},
        ),
        "source_build_discovery_batch_receipt": (
            (
                "build_id",
                "batch_key",
                "start_generation",
                "start_cursor",
                "start_processed_count",
                "next_cursor",
                "next_processed_count",
                "next_state",
                "row_count",
                "terminal",
                "committed_generation",
                "committed_at",
            ),
            {
                frozenset({"build_id", "batch_key"}),
                frozenset({"build_id", "start_generation"}),
            },
        ),
        "source_build_assembly_checkpoint": (
            (
                "build_id",
                "generation",
                "cursor_bytes",
                "processed_gallery_count",
                "processed_file_count",
                "processed_byte_count",
                "manifest_chain_sha256",
                "state",
                "updated_at",
            ),
            {frozenset({"build_id"})},
        ),
        "source_build_assembly_batch_receipt": (
            (
                "build_id",
                "batch_key",
                "start_generation",
                "start_cursor",
                "start_gallery_count",
                "start_file_count",
                "start_byte_count",
                "start_manifest_chain_sha256",
                "next_cursor",
                "next_gallery_count",
                "next_file_count",
                "next_byte_count",
                "next_manifest_chain_sha256",
                "next_state",
                "row_count",
                "terminal",
                "committed_generation",
                "committed_at",
            ),
            {
                frozenset({"build_id", "batch_key"}),
                frozenset({"build_id", "start_generation"}),
            },
        ),
    }
    for relation_name, (attributes, keys) in expected_operational_shapes.items():
        relation = operational_relations.get(relation_name)
        if (
            relation is None
            or relation.attributes != attributes
            or set(relation.declared_keys) != keys
        ):
            errors.append(
                f"operational {relation_name} must retain complete pre/post authority"
            )

    expected_membership = catalog_relations.get("source_build_expected_gallery")
    membership = catalog_relations.get("source_build_gallery")
    observation_stat = catalog_relations.get("gallery_observation_stat")
    if expected_membership is not None and not (
        _has_fk(
            expected_membership,
            ("build_id",),
            "source_build",
            ("build_id",),
        )
        and _has_fk(
            expected_membership,
            ("gallery_id",),
            "gallery_identity",
            ("gallery_id",),
        )
    ):
        errors.append(
            "catalog source_build_expected_gallery must bind build and gallery identity"
        )
    if membership is None or not _has_fk(
        membership,
        ("build_id", "gallery_id"),
        "source_build_expected_gallery",
        ("build_id", "gallery_id"),
    ):
        errors.append(
            "catalog source_build_gallery must forbid extra expected membership"
        )
    if observation_stat is not None and not _has_fk(
        observation_stat,
        ("gallery_id", "observation_id"),
        "gallery_observation",
        ("gallery_id", "observation_id"),
    ):
        errors.append(
            "catalog gallery_observation_stat must be observation-owned authority"
        )

    retention_targets = {target.target: target for target in catalog.retention_targets}
    gallery_identity_target = retention_targets.get("GALLERY_IDENTITY")
    expected_identity_edge = RetentionForeignKeyBoundary(
        relation="source_build_expected_gallery",
        attributes=("gallery_id",),
        referenced_relation="gallery_identity",
        referenced_attributes=("gallery_id",),
    )
    if (
        gallery_identity_target is None
        or expected_identity_edge not in gallery_identity_target.external_blockers
    ):
        errors.append("GALLERY_IDENTITY retention must list expected source membership")
    gallery_observation_target = retention_targets.get("GALLERY_OBSERVATION")
    if gallery_observation_target is None or not any(
        "gallery_observation_stat" in phase
        for phase in gallery_observation_target.child_phases
    ):
        errors.append(
            "GALLERY_OBSERVATION cleanup must delete observation stat child-first"
        )
    source_build_target = retention_targets.get("SOURCE_BUILD")
    if source_build_target is None or not any(
        "source_build_expected_gallery" in phase
        for phase in source_build_target.child_phases
    ):
        errors.append(
            "SOURCE_BUILD cleanup must delete expected membership child-first"
        )

    if errors:
        raise ContractValidationError(errors)


_DATA_MACHINE_OBLIGATION_IDS = frozenset(
    {
        "catalog.identity-codecs.v1",
        "catalog.canonical-reference-domains.v1",
        "catalog.source-baseline-channel.v1",
        "catalog.incremental-impact.v1",
        "catalog.overlay-resolution-seal.v1",
        "catalog.artifact-semantics.v1",
        "catalog.publication-atomicity.v1",
        "catalog.state-machines.v1",
        "catalog.role-derivation.v1",
        "catalog.physical-domains.v1",
        "catalog.bootstrap.v1",
        "catalog.retention.v1",
    }
)


_DATA_RETENTION_TARGETS: Mapping[str, tuple[str, tuple[str, ...]]] = {
    "PUBLICATION_CANDIDATE": ("publication_candidate", ("candidate_id",)),
    "ANALYSIS_RUN": ("analysis_run", ("analysis_id",)),
    "SOURCE_BUILD": ("source_build", ("build_id",)),
    "GALLERY_OBSERVATION": (
        "gallery_observation_allocation",
        ("gallery_id", "observation_id"),
    ),
    "GALLERY_OBSERVATION_PAGE": ("gallery_observation_page", ("page_sha256",)),
    "ARTIFACT_BLOB": ("artifact_blob", ("artifact_sha256",)),
    "CANONICAL_VALUE": ("canonical_value_allocation", ("value_sha256",)),
    "CONTENT_BLOB": ("content_blob", ("file_sha256",)),
    "FILE_NAME_IDENTITY": ("file_name_identity", ("file_key",)),
    "PUBLICATION_IDENTITY": ("publication_identity", ("publication_key",)),
    "GALLERY_IDENTITY": ("gallery_identity", ("gallery_id",)),
}


def _validate_retention_contract(
    contract: Contract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Prove every cleanup root's reverse-FK frontier is closed and ordered."""

    errors: list[str] = []
    retention = contract.retention_contract
    if retention is None:
        return ["catalog data-plane contract must declare retention_contract"]
    if retention.version != 1:
        errors.append("retention contract version must be exactly one")

    retained = retention.indefinitely_retained_relations
    if len(set(retained)) != len(retained):
        errors.append(
            "retention contract indefinitely_retained_relations contain duplicates"
        )
    unknown_retained = set(retained) - set(relation_by_name)
    if unknown_retained:
        errors.append(
            "retention contract has unknown retained relations "
            f"{_format_set(unknown_retained)}"
        )
    required_retained = {
        "source_revision",
        "source_snapshot_manifest_identity",
        "catalog_revision",
        "display_title_choice",
        "title_sort",
    }
    if set(retained) != required_retained:
        errors.append(
            "retention contract indefinite history must be exactly source and "
            "catalog revision history plus retained title dictionaries"
        )

    expected_active_relations = {
        "active_head_relation": "source_head",
        "revision_relation": "source_revision",
        "provenance_relation": "source_revision_provenance",
        "analysis_relation": "analysis_run",
        "build_relation": "source_build",
    }
    for field, expected_relation in expected_active_relations.items():
        if getattr(retention, field) != expected_relation:
            errors.append(
                "retention contract "
                f"{field} must be the exact relation {expected_relation!r}"
            )
    active_terms = (
        "every source_head.source_revision",
        "exactly one source_revision_provenance",
        "analysis_run",
        "source_build",
        "exact next-run delta base",
    )
    inactive_terms = (
        "head advances",
        "source_revision_provenance",
        "deleted before",
        "self-contained retained history",
    )
    if any(term not in retention.active_source_rule for term in active_terms):
        errors.append("retention contract active provenance rule is incomplete")
    if any(term not in retention.inactive_source_rule for term in inactive_terms):
        errors.append("retention contract inactive provenance rule is incomplete")
    if retention.semantic_obligation_id != "catalog.retention.v1":
        errors.append("retention contract must resolve through catalog.retention.v1")
    if not all(
        token in retention.source_history_rule
        for token in ("indefinitely retained", "source_revision", "self-contained")
    ):
        errors.append("retention contract source history rule is incomplete")
    if not all(
        token in retention.catalog_history_rule
        for token in (
            "indefinitely retained",
            "cross-call pagination",
            "durable reader lease",
            "not a cleanup target",
        )
    ):
        errors.append("retention contract catalog history rule is incomplete")

    head = relation_by_name.get(retention.active_head_relation)
    revision = relation_by_name.get(retention.revision_relation)
    provenance = relation_by_name.get(retention.provenance_relation)
    analysis = relation_by_name.get(retention.analysis_relation)
    build = relation_by_name.get(retention.build_relation)
    if None not in (head, revision, provenance, analysis, build):
        assert head is not None
        assert revision is not None
        assert provenance is not None
        assert analysis is not None
        assert build is not None
        if not _has_fk(head, ("source_revision",), revision.name, ("source_revision",)):
            errors.append(
                "retention contract active head does not FK to source revision"
            )
        if not _has_fk(
            provenance,
            ("source_revision",),
            revision.name,
            ("source_revision",),
        ) or not _has_fk(
            provenance,
            ("analysis_id",),
            analysis.name,
            ("analysis_id",),
        ):
            errors.append("retention contract provenance path has the wrong FKs")
        if set(provenance.declared_keys) != {
            frozenset({"source_revision"}),
            frozenset({"analysis_id"}),
        }:
            errors.append("retention contract provenance must be exactly one-to-one")
        if not _has_fk(analysis, ("build_id",), build.name, ("build_id",)):
            errors.append("retention contract analysis does not FK to its source build")
    else:
        errors.append("retention contract active provenance path is not resolvable")

    targets = contract.retention_targets
    targets_by_name = {target.target: target for target in targets}
    roots = [target.root_relation for target in targets]
    if len(targets_by_name) != len(targets):
        errors.append("retention targets contain duplicate target names")
    if len(set(roots)) != len(roots):
        errors.append("retention targets contain duplicate root relations")
    if set(targets_by_name) != set(_DATA_RETENTION_TARGETS):
        errors.append(
            "retention target registry must be exactly "
            f"{_format_set(_DATA_RETENTION_TARGETS)}"
        )

    reverse_fks: dict[str, list[RetentionForeignKeyBoundary]] = {
        relation_name: [] for relation_name in relation_by_name
    }
    all_fk_edges: set[RetentionForeignKeyBoundary] = set()
    for relation in relation_by_name.values():
        for foreign_key in relation.foreign_keys:
            if foreign_key.relation not in relation_by_name:
                continue
            edge = RetentionForeignKeyBoundary(
                relation=relation.name,
                attributes=foreign_key.attributes,
                referenced_relation=foreign_key.relation,
                referenced_attributes=foreign_key.referenced_attributes,
            )
            reverse_fks[foreign_key.relation].append(edge)
            all_fk_edges.add(edge)

    for target in targets:
        errors.extend(
            _validate_retention_target(
                target,
                relation_by_name,
                reverse_fks,
                all_fk_edges,
            )
        )
        expected_target = _DATA_RETENTION_TARGETS.get(target.target)
        if expected_target is not None and (
            target.root_relation != expected_target[0]
            or target.root_key != expected_target[1]
        ):
            errors.append(f"retention target {target.target!r} has the wrong root/key")
    return errors


def _validate_retention_target(
    target: RetentionTarget,
    relation_by_name: Mapping[str, Relation],
    reverse_fks: Mapping[str, list[RetentionForeignKeyBoundary]],
    all_fk_edges: set[RetentionForeignKeyBoundary],
) -> list[str]:
    errors: list[str] = []
    prefix = f"retention target {target.target!r}"
    root = relation_by_name.get(target.root_relation)
    if root is None:
        return [f"{prefix} references unknown root relation {target.root_relation!r}"]
    if not target.root_key or frozenset(target.root_key) not in set(root.declared_keys):
        errors.append(f"{prefix} root_key must be an exact candidate key")

    phases: dict[str, int] = {}
    for phase_index, phase in enumerate(target.child_phases):
        if not phase:
            errors.append(f"{prefix} contains an empty child phase")
        for relation_name in phase:
            if relation_name in phases:
                errors.append(
                    f"{prefix} child relation {relation_name!r} appears more than once"
                )
            phases[relation_name] = phase_index
            if relation_name not in relation_by_name:
                errors.append(
                    f"{prefix} child phase references unknown relation {relation_name!r}"
                )

    external = set(target.external_blockers)
    retained = set(target.retained_outliving)
    if len(external) != len(target.external_blockers):
        errors.append(f"{prefix} has duplicate external blocker edges")
    if len(retained) != len(target.retained_outliving):
        errors.append(f"{prefix} has duplicate retained-outliving edges")
    overlap = external & retained
    if overlap:
        errors.append(f"{prefix} classifies one FK edge in two boundary classes")
    for edge in external | retained:
        if edge not in all_fk_edges:
            errors.append(
                f"{prefix} boundary edge {edge.relation!r}{edge.attributes!r} "
                "is not an exact declared foreign key"
            )
    selectors = set(target.phase_selectors)
    if len(selectors) != len(target.phase_selectors):
        errors.append(f"{prefix} has duplicate phase selectors")
    for selector in selectors:
        if selector not in all_fk_edges:
            errors.append(f"{prefix} phase selector is not an exact declared FK")
        if selector.relation not in phases:
            errors.append(f"{prefix} phase selector relation is not phase-owned")
        if selector in external or selector in retained:
            errors.append(f"{prefix} phase selector cannot select a blocker edge")

    derived_views = set(target.derived_views)
    if len(derived_views) != len(target.derived_views):
        errors.append(f"{prefix} has duplicate derived views")
    for relation_name in derived_views:
        relation = relation_by_name.get(relation_name)
        if relation is None:
            errors.append(f"{prefix} references unknown derived view {relation_name!r}")
        elif not (
            relation.materialization is not None
            and relation.materialization.get("storage") == "logical_view"
        ):
            errors.append(f"{prefix} {relation_name!r} is not a logical view")
        if relation_name in phases:
            errors.append(f"{prefix} cannot delete logical view {relation_name!r}")

    semantic_seen: set[tuple[str, tuple[str, ...], tuple[str, ...]]] = set()
    for blocker in target.semantic_blockers:
        identity = (blocker.relation, blocker.attributes, blocker.root_attributes)
        if identity in semantic_seen:
            errors.append(f"{prefix} has duplicate semantic blockers")
        semantic_seen.add(identity)
        relation = relation_by_name.get(blocker.relation)
        if relation is None:
            errors.append(
                f"{prefix} semantic blocker references unknown relation "
                f"{blocker.relation!r}"
            )
            continue
        if not blocker.attributes or len(blocker.attributes) != len(
            blocker.root_attributes
        ):
            errors.append(f"{prefix} semantic blocker has invalid join arity")
        if not set(blocker.attributes) <= set(relation.attributes) or not set(
            blocker.root_attributes
        ) <= set(root.attributes):
            errors.append(f"{prefix} semantic blocker has unknown join attributes")
        if frozenset(blocker.attributes) not in set(
            relation.declared_keys
        ) or frozenset(blocker.root_attributes) not in set(root.declared_keys):
            errors.append(f"{prefix} semantic blocker join must use exact keys")
        if blocker.semantic_obligation_id != "catalog.retention.v1":
            errors.append(
                f"{prefix} semantic blocker lacks its machine resolver obligation"
            )

    machine_gate_ids = [gate.id for gate in target.machine_gates]
    if len(machine_gate_ids) != len(set(machine_gate_ids)):
        errors.append(f"{prefix} has duplicate machine gate IDs")
    for gate in target.machine_gates:
        if gate.semantic_obligation_id != "catalog.retention.v1":
            errors.append(f"{prefix} machine gate {gate.id!r} lacks its resolver")
    expected_machine_gates = {
        "CANONICAL_VALUE": (
            "operational.canonical_value_upload(generation,value_sha256)",
            "operational.canonical_value_maintenance_fence",
        ),
        "GALLERY_OBSERVATION_PAGE": (
            "operational.gallery_observation_page_maintenance_fence",
        ),
    }.get(target.target, ())
    if tuple(machine_gate_ids) != expected_machine_gates:
        errors.append(
            f"{prefix} machine gates must be exactly {expected_machine_gates!r}"
        )

    visited_edges: set[RetentionForeignKeyBoundary] = set()
    visited_phases: set[str] = set()
    visited_views: set[str] = set()
    pending = [target.root_relation]
    expanded: set[str] = set()
    while pending:
        parent = pending.pop()
        if parent in expanded:
            continue
        expanded.add(parent)
        for edge in reverse_fks.get(parent, ()):
            visited_edges.add(edge)
            child = edge.relation
            if edge in external or edge in retained:
                continue
            if child in derived_views:
                visited_views.add(child)
                continue
            if child in phases:
                visited_phases.add(child)
                pending.append(child)
                continue
            errors.append(
                f"{prefix} leaves FK descendant {child!r}{edge.attributes!r} "
                "unclassified"
            )

    for edge in (external | retained) - visited_edges:
        errors.append(
            f"{prefix} boundary edge {edge.relation!r}{edge.attributes!r} "
            "is not reachable from the cleanup root"
        )
    for relation_name in set(phases) - visited_phases:
        errors.append(
            f"{prefix} phase-owned relation {relation_name!r} is not reachable "
            "from the cleanup root"
        )
    for relation_name in derived_views - visited_views:
        errors.append(
            f"{prefix} derived view {relation_name!r} is not reachable from the root"
        )

    edges_by_child: dict[tuple[str, str], set[RetentionForeignKeyBoundary]] = {}
    for edge in visited_edges:
        edges_by_child.setdefault((edge.relation, edge.referenced_relation), set()).add(
            edge
        )
    required_selectors = {
        edge
        for (relation_name, _parent), edges in edges_by_child.items()
        if relation_name in phases
        and len(edges) > 1
        and any(candidate in external or candidate in retained for candidate in edges)
        for edge in edges
        if edge not in external and edge not in retained
    }
    if selectors != required_selectors:
        errors.append(
            f"{prefix} phase selectors must exactly disambiguate every "
            "multi-FK phase-owned deletion path"
        )

    boundaries = external | retained
    for edge in visited_edges:
        child_phase = phases.get(edge.relation)
        parent_phase = phases.get(edge.referenced_relation)
        if (
            edge not in boundaries
            and child_phase is not None
            and parent_phase is not None
            and child_phase >= parent_phase
        ):
            errors.append(
                f"{prefix} child phase order is reversed for "
                f"{edge.relation!r} -> {edge.referenced_relation!r}"
            )
    return errors


def _data_prose_obligation_paths(contract: Contract) -> frozenset[str]:
    paths = {
        "canonical_digest_contract.write_obligation",
        "canonical_digest_contract.read_obligation",
        "canonical_value_page_contract.collision_obligation",
        "canonical_value_page_contract.seal_obligation",
        "canonical_value_page_contract.cleanup_rule",
        "source_locator_contract.write_obligation",
        "source_locator_contract.read_obligation",
        "source_root_contract.write_obligation",
        "source_scope_identity_contract.write_obligation",
        "source_scope_identity_contract.seal_obligation",
        "file_identity_contract.write_obligation",
        "file_identity_contract.read_obligation",
        "gallery_observation_identity_contract.write_obligation",
        "gallery_observation_identity_contract.reuse_obligation",
        "gallery_observation_page_contract.collision_obligation",
        "gallery_observation_page_contract.materialization_rule",
        "gallery_observation_page_contract.seal_obligation",
        "gallery_observation_page_contract.cleanup_rule",
        "effective_content_contract.write_obligation",
        "effective_content_contract.read_obligation",
        "source_snapshot_manifest_contract.write_obligation",
        "source_snapshot_manifest_contract.handoff_obligation",
        "source_snapshot_manifest_contract.publication_obligation",
        "analysis_run_contract.write_obligation",
        "analysis_run_contract.attempt_rule",
        "analysis_resolution_contract.cursor_codec_rule",
        "analysis_resolution_contract.batch_rule",
        "analysis_candidate_contract.runtime_obligation",
        "artifact_delta_contract.rebuild_rule",
        "artifact_delta_contract.unchanged_rule",
        "artifact_delta_contract.rename_rule",
        "artifact_byte_producer_contract.runtime_obligation",
        "artifact_member_plan_contract.runtime_obligation",
        "artifact_member_plan_contract.ready_obligation",
        "artifact_name_contract.runtime_obligation",
        "artifact_locator_contract.runtime_obligation",
        "artifact_protection_token_contract.runtime_obligation",
        "publication_atomic_contract.selection_rule",
        "publication_atomic_contract.cursor_codec_rule",
        "publication_atomic_contract.batch_rule",
        "publication_atomic_contract.projection_seal_rule",
        "publication_atomic_contract.runtime_obligation",
        "publication_atomic_contract.finalization_rule",
        "title_sort_contract.runtime_obligation",
        "transition_authority_contract.runtime_obligation",
        "transition_authority_contract.ready_obligation",
        "overlay.shadow_tombstone_exclusion",
        "overlay.ancestry_exactness",
        "overlay.full_evaluator_equality",
    }
    paths.update(
        f"byte_domain.{domain.attribute}.runtime_obligation"
        for domain in contract.byte_domains
    )
    paths.update(
        f"machine_contract.{obligation_id.removeprefix('catalog.').removesuffix('.v1')}"
        for obligation_id in _DATA_MACHINE_OBLIGATION_IDS
    )
    return frozenset(paths)


def _validate_data_semantic_obligations(
    contract: Contract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Require every data prose rule to resolve to READY and writer validators."""

    errors: list[str] = []
    obligations = contract.semantic_obligations
    by_id = {obligation.id: obligation for obligation in obligations}
    if len(by_id) != len(obligations):
        errors.append("data semantic obligations contain duplicate machine IDs")
    if set(by_id) != _DATA_MACHINE_OBLIGATION_IDS:
        errors.append(
            "data semantic obligation registry must be exactly "
            f"{_format_set(_DATA_MACHINE_OBLIGATION_IDS)}"
        )

    covered_by: dict[str, str] = {}
    for obligation in obligations:
        prefix = f"semantic obligation {obligation.id!r}"
        if obligation.version != 1 or obligation.writer_hook_version != 1:
            errors.append(f"{prefix} must pin machine and writer-hook version one")
        expected_scope = obligation.id.removesuffix(".v1")
        if obligation.scope != expected_scope:
            errors.append(f"{prefix} scope must be {expected_scope!r}")
        expected_lifecycle = (
            "building_only"
            if obligation.id == "catalog.bootstrap.v1"
            else "ready_and_runtime"
        )
        if obligation.lifecycle != expected_lifecycle:
            errors.append(f"{prefix} lifecycle must be {expected_lifecycle!r}")
        if not obligation.ready_check.startswith("catalog_refinement.check_"):
            errors.append(f"{prefix} lacks a bounded catalog refinement check")
        if not obligation.writer_hook or not obligation.description.strip():
            errors.append(f"{prefix} lacks an executable writer hook or description")
        if not obligation.relations or len(set(obligation.relations)) != len(
            obligation.relations
        ):
            errors.append(f"{prefix} must list distinct local relations")
        unknown_relations = set(obligation.relations) - set(relation_by_name)
        if unknown_relations:
            errors.append(
                f"{prefix} references unknown relations {_format_set(unknown_relations)}"
            )
        if not obligation.covers or len(set(obligation.covers)) != len(
            obligation.covers
        ):
            errors.append(f"{prefix} must cover distinct machine/prose rules")
        for path in obligation.covers:
            previous = covered_by.setdefault(path, obligation.id)
            if previous != obligation.id:
                errors.append(
                    f"semantic rule {path!r} is multiply owned by {previous!r} "
                    f"and {obligation.id!r}"
                )

    expected_paths = _data_prose_obligation_paths(contract)
    actual_paths = set(covered_by)
    if actual_paths != expected_paths:
        missing = expected_paths - actual_paths
        extra = actual_paths - expected_paths
        if missing:
            errors.append(
                f"data prose obligations lack machine records: {_format_set(missing)}"
            )
        if extra:
            errors.append(
                "data semantic obligations claim unknown prose rules: "
                f"{_format_set(extra)}"
            )
    errors.extend(
        _validate_canonical_reference_roles_and_bootstrap(contract, relation_by_name)
    )
    errors.extend(_validate_identity_codecs(contract))
    return errors


def _validate_identity_codecs(contract: Contract) -> list[str]:
    errors: list[str] = []
    expected = {
        "gallery-key.v1": (
            "gallery_key",
            "ascii('h2hdb-vnext-gallery-key\\0') || u32be(version) || "
            "raw32(scope_key) || raw32(locator_sha256)",
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
            "2b054506682936127fd916b15c50a29ea0e15e96d4e0f249ce68a88bdd3d1ae3",
            "7e5c25a4144d31c0e6cfc3fc5380b1b65659356ca1d1602dea4503c45485975f",
        ),
        "publication-key.v1": (
            "publication_key",
            "ascii('h2hdb-vnext-publication-key\\0') || u32be(version) || u64be(gid)",
            "7fffffffffffffff",
            "ef9a3bbaa67483f863e6aa50c1c8f2b97969a6acf1b21d6ea77df181e3bb0fd2",
        ),
    }
    by_id = {codec.id: codec for codec in contract.identity_codecs}
    if len(by_id) != len(contract.identity_codecs) or set(by_id) != set(expected):
        return [
            "identity codec registry must contain exactly gallery-key.v1 and publication-key.v1"
        ]
    for codec_id, values in expected.items():
        codec = by_id[codec_id]
        target, framing, golden_input, golden_digest = values
        if (
            codec.version != 1
            or codec.target_attribute != target
            or codec.framing != framing
            or codec.golden_input_hex != golden_input
            or codec.golden_sha256 != golden_digest
        ):
            errors.append(
                f"identity codec {codec_id!r} does not match its hardcoded golden"
            )
        for label, value, size in (
            ("golden input", codec.golden_input_hex, len(golden_input)),
            ("golden digest", codec.golden_sha256, 64),
        ):
            if len(value) != size or any(
                character not in "0123456789abcdef" for character in value
            ):
                errors.append(
                    f"identity codec {codec_id!r} {label} is not exact lowercase hex"
                )
    return errors


def _validate_canonical_reference_roles_and_bootstrap(
    contract: Contract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    storage = contract.long_value_storage_contract
    if storage is None:
        return ["data canonical-reference roles require long_value_storage_contract"]
    canonical_attributes = set(storage.canonical_reference_attributes)
    paths: set[tuple[str, str]] = set()
    for relation in relation_by_name.values():
        for foreign_key in relation.foreign_keys:
            for local_attribute, referenced_attribute in zip(
                foreign_key.attributes,
                foreign_key.referenced_attributes,
                strict=True,
            ):
                if (
                    local_attribute in canonical_attributes
                    and foreign_key.relation == storage.canonical_reference_relation
                    and referenced_attribute == storage.canonical_reference_attribute
                ):
                    paths.add((relation.name, local_attribute))
    changed = True
    while changed:
        changed = False
        for relation in relation_by_name.values():
            for foreign_key in relation.foreign_keys:
                for local_attribute, referenced_attribute in zip(
                    foreign_key.attributes,
                    foreign_key.referenced_attributes,
                    strict=True,
                ):
                    path = (relation.name, local_attribute)
                    if (
                        local_attribute in canonical_attributes
                        and local_attribute == referenced_attribute
                        and (foreign_key.relation, referenced_attribute) in paths
                        and path not in paths
                    ):
                        paths.add(path)
                        changed = True

    roles = contract.canonical_reference_roles
    role_by_attribute = {role.attribute: role for role in roles}
    if len(role_by_attribute) != len(roles):
        errors.append("canonical-reference role registry contains duplicate attributes")
    if set(role_by_attribute) != canonical_attributes:
        errors.append(
            "canonical-reference role registry must exactly cover "
            f"{_format_set(canonical_attributes)}"
        )
    for attribute, role in role_by_attribute.items():
        expected_relations = {name for name, local in paths if local == attribute}
        if set(role.relations) != expected_relations or len(role.relations) != len(
            expected_relations
        ):
            errors.append(
                f"canonical-reference role {attribute!r} must list exact FK paths "
                f"{_format_set(expected_relations)}"
            )
        if re.fullmatch(r"[a-z0-9_]+_v[1-9][0-9]*", role.digest_domain) is None:
            errors.append(
                f"canonical-reference role {attribute!r} lacks a versioned domain"
            )

    seeds = contract.bootstrap_seeds
    seed_ids = {seed.id for seed in seeds}
    if len(seed_ids) != len(seeds):
        errors.append("data bootstrap seeds contain duplicate IDs")
    expected_domain_rows = {
        ("canonical_digest_policy", ("digest_domain",), (domain,))
        for domain in (
            {role.digest_domain for role in roles}
            | set(_FILESYSTEM_HASH_CACHE_DIGEST_DOMAINS)
        )
    }
    expected_rows = expected_domain_rows | {
        ("channel_registry", ("channel",), ("default",)),
        ("source_provider_registry", ("source_provider",), ("filesystem",)),
        (
            "artifact_zip_writer_policy",
            (
                "artifact_algorithm_version",
                "zip_codec_version",
                "compression_method",
                "compression_level",
                "dos_date",
                "dos_time",
                "unix_mode",
                "general_purpose_flags",
                "create_system",
                "archive_name_codec_version",
                "artifact_name_codec_version",
            ),
            ("1", "1", "8", "9", "33", "0", "33188", "2048", "3", "1", "1"),
        ),
        (
            "artifact_storage_codec",
            (
                "storage_codec_version",
                "adapter_id",
                "locator_codec_version",
                "protection_token_codec_version",
            ),
            ("1", "managed-filesystem", "1", "1"),
        ),
        *{
            (
                "analysis_stage",
                ("stage", "stage_order", "cursor_codec"),
                (stage, f"{order:02d}", cursor_codec),
            )
            for stage, order, cursor_codec in (
                ("changed_gallery", 1, "analysis_gallery_v1"),
                ("changed_file_hash", 2, "analysis_digest_v1"),
                ("file_hash_decision", 3, "analysis_digest_v1"),
                ("validate_file_hash_decision", 4, "analysis_digest_live_v1"),
                ("impacted_gallery", 5, "analysis_gallery_v1"),
                ("impacted_content", 6, "analysis_gallery_v1"),
                ("content_owner_candidate", 7, "analysis_gallery_v1"),
                ("validate_content_owner_candidate", 8, "analysis_gallery_live_v1"),
                ("content_owner", 9, "analysis_digest_v1"),
                ("validate_content_owner", 10, "analysis_digest_live_v1"),
                ("impacted_gid", 11, "analysis_gallery_v1"),
                ("gid_candidate", 12, "analysis_gallery_v1"),
                ("validate_gid_candidate", 13, "analysis_gallery_live_v1"),
                ("gid_winner", 14, "analysis_gid_v1"),
                ("validate_gid_winner", 15, "analysis_gid_live_v1"),
            )
        },
        *{
            (
                "publication_stage",
                ("stage", "stage_order", "cursor_codec"),
                (stage, f"{order:02d}", cursor_codec),
            )
            for stage, order, cursor_codec in (
                ("BUILD_SELECTION", 1, "publication_gallery_v1"),
                ("VALIDATE_SELECTION", 2, "publication_gallery_v1"),
                (
                    "BUILD_CATALOG_PROJECTION",
                    3,
                    "publication_catalog_child_v1",
                ),
                (
                    "VALIDATE_CATALOG_PROJECTION",
                    4,
                    "publication_catalog_child_v1",
                ),
                ("BUILD_ARTIFACT_INPUT", 5, "publication_key_v1"),
                ("BUILD_ARTIFACT_DELTA_OPERATION", 6, "publication_key_v1"),
                ("VALIDATE_ARTIFACT_INPUT_DELTA", 7, "publication_key_v1"),
                ("VALIDATE_PREPARED_ARTIFACT", 8, "publication_key_v1"),
                ("VALIDATE_CREATE", 9, "publication_key_v1"),
                ("VALIDATE_REBUILD", 10, "publication_key_v1"),
                ("VALIDATE_DELETE", 11, "publication_key_v1"),
                ("VALIDATE_UNCHANGED", 12, "publication_key_v1"),
                ("VALIDATE_NEW_GALLERY", 13, "publication_key_v1"),
                ("VALIDATE_CHANGED_GALLERY", 14, "publication_key_v1"),
                ("VALIDATE_REMOVED_GALLERY", 15, "publication_key_v1"),
                ("VALIDATE_DUPLICATE_LOSER", 16, "publication_gallery_v1"),
                ("FINALIZE_ARTIFACTS", 17, "publication_key_v1"),
            )
        },
    }
    actual_rows = {(seed.relation, seed.columns, seed.values) for seed in seeds}
    if actual_rows != expected_rows or len(actual_rows) != len(seeds):
        errors.append(
            "data bootstrap seeds must be exactly one default channel, one "
            "filesystem provider, the artifact ZIP/storage registries, the fifteen analysis stages, the seventeen "
            "publication stages, every catalog "
            "canonical digest domain, and the two operational filesystem "
            "hash-cache domains"
        )
    for seed in seeds:
        seeded_relation = relation_by_name.get(seed.relation)
        if seeded_relation is None:
            errors.append(f"bootstrap seed {seed.id!r} references unknown relation")
            continue
        if not seed.columns or len(seed.columns) != len(seed.values):
            errors.append(f"bootstrap seed {seed.id!r} has an invalid row arity")
        if not set(seed.columns) <= set(seeded_relation.attributes):
            errors.append(f"bootstrap seed {seed.id!r} has unknown columns")
        if not any(
            key <= frozenset(seed.columns) for key in seeded_relation.declared_keys
        ):
            errors.append(
                f"bootstrap seed {seed.id!r} must supply a complete keyed row"
            )
    return errors


def _has_fk(
    relation: Relation,
    attributes: tuple[str, ...],
    target: str,
    target_attributes: tuple[str, ...],
) -> bool:
    return any(
        foreign_key.attributes == attributes
        and foreign_key.relation == target
        and foreign_key.referenced_attributes == target_attributes
        for foreign_key in relation.foreign_keys
    )


def _validate_data_plane_integrity_contracts(
    contract: Contract,
    relations: Mapping[str, Relation],
) -> list[str]:
    """Close digest identity, deterministic-run, and atomic-publication assumptions."""

    errors: list[str] = []
    required_contracts = {
        "analysis_run_contract": contract.analysis_run_contract,
        "source_scope_identity_contract": contract.source_scope_identity_contract,
        "effective_content_contract": contract.effective_content_contract,
        "source_snapshot_manifest_contract": (
            contract.source_snapshot_manifest_contract
        ),
        "publication_atomic_contract": contract.publication_atomic_contract,
        "file_identity_contract": contract.file_identity_contract,
        "gallery_observation_identity_contract": (
            contract.gallery_observation_identity_contract
        ),
        "source_root_contract": contract.source_root_contract,
        "gallery_observation_page_contract": (
            contract.gallery_observation_page_contract
        ),
        "title_sort_contract": contract.title_sort_contract,
    }
    for name, value in required_contracts.items():
        if value is None:
            errors.append(f"catalog data-plane contract must declare {name}")
    if errors:
        return errors

    analysis = contract.analysis_run_contract
    scope = contract.source_scope_identity_contract
    content = contract.effective_content_contract
    snapshot_manifest = contract.source_snapshot_manifest_contract
    publication = contract.publication_atomic_contract
    file_identity = contract.file_identity_contract
    observation_identity = contract.gallery_observation_identity_contract
    source_root = contract.source_root_contract
    observation_pages = contract.gallery_observation_page_contract
    title_sort = contract.title_sort_contract
    assert analysis is not None
    assert scope is not None
    assert content is not None
    assert snapshot_manifest is not None
    assert publication is not None
    assert file_identity is not None
    assert observation_identity is not None
    assert source_root is not None
    assert observation_pages is not None
    assert title_sort is not None

    errors.extend(_validate_analysis_run_contract(analysis, relations))
    errors.extend(_validate_source_scope_identity_contract(scope, relations))
    errors.extend(_validate_effective_content_contract(content, relations))
    errors.extend(
        _validate_source_snapshot_manifest_contract(snapshot_manifest, relations)
    )
    errors.extend(_validate_publication_atomic_contract(publication, relations))
    errors.extend(_validate_file_identity_contract(file_identity, relations))
    errors.extend(_validate_source_root_contract(source_root, relations))
    errors.extend(
        _validate_gallery_observation_identity_contract(observation_identity, relations)
    )
    errors.extend(
        _validate_gallery_observation_page_contract(observation_pages, relations)
    )
    errors.extend(_validate_title_sort_contract(title_sort, relations))
    return errors


def _validate_analysis_run_contract(
    contract: AnalysisRunContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "analysis run contract"
    if (
        contract.relation != "analysis_run"
        or contract.natural_key != ("build_id", "policy_id")
        or contract.manifest_attribute != "input_manifest_sha256"
    ):
        errors.append(f"{prefix} must use (build_id, policy_id) as the exact key")
    if not all(
        token in contract.write_obligation
        for token in ("deterministically", "at most one analysis_id", "resume")
    ) or not all(
        token in contract.attempt_rule for token in ("separate relation", "never")
    ):
        errors.append(f"{prefix} must separate retries from semantic identity")
    relation = relations.get(contract.relation)
    if relation is None:
        errors.append(f"{prefix} references an unknown relation")
        return errors
    expected_keys = {
        frozenset({"analysis_id"}),
        frozenset(contract.natural_key),
    }
    if set(relation.declared_keys) != expected_keys:
        errors.append(
            f"{prefix} must declare only analysis_id and (build_id, policy_id) keys"
        )
    natural_fd = next(
        (
            dependency
            for dependency in relation.functional_dependencies
            if dependency.determinant == frozenset(contract.natural_key)
        ),
        None,
    )
    if natural_fd is None or contract.manifest_attribute not in natural_fd.dependent:
        errors.append(f"{prefix} natural key must determine input_manifest_sha256")
    return errors


def _validate_source_scope_identity_contract(
    contract: SourceScopeIdentityContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "source scope identity contract"
    expected_natural = (
        "source_provider",
        "source_root_sha256",
        "identity_policy_version",
    )
    if (
        contract.relation != "source_scope"
        or contract.key_attribute != "scope_key"
        or contract.natural_key != expected_natural
        or contract.encoding_version != 1
        or contract.collision_model != "collision_free_identity_domain"
    ):
        errors.append(f"{prefix} has the wrong identity shape")
    if not all(value in contract.framing for value in expected_natural):
        errors.append(f"{prefix} framing omits a natural-key field")
    if not all(
        value in contract.write_obligation
        for value in ("domain-separated SHA-256", "recompute", "conflict", "tuple")
    ) or not all(
        value in contract.seal_obligation
        for value in (
            "source_build.scope_key",
            "gallery_identity.scope_key",
            "cross-scope",
        )
    ):
        errors.append(f"{prefix} must state collision and build-seal obligations")
    relation = relations.get(contract.relation)
    if relation is None:
        errors.append(f"{prefix} references an unknown relation")
        return errors
    if set(relation.declared_keys) != {
        frozenset({contract.key_attribute}),
        frozenset(contract.natural_key),
    }:
        errors.append(f"{prefix} digest and natural tuple must be exact candidate keys")
    if not _has_fk(
        relation,
        ("source_root_sha256",),
        "canonical_value_identity",
        ("value_sha256",),
    ):
        errors.append(f"{prefix} source root must retain exact canonical bytes")
    if not _has_fk(
        relation,
        ("source_provider",),
        "source_provider_registry",
        ("source_provider",),
    ):
        errors.append(f"{prefix} source provider must use the seeded registry")
    return errors


_EFFECTIVE_CONTENT_WRITE_OBLIGATION_V1 = (
    "production prepares effective content only as a database-owned private typed "
    "EffectiveContentPreparation, never as caller authority: from one immutable "
    "SEALED source_build snapshot and immutable COMPLETE analysis snapshot, stream "
    "only resolved non-excluded CONTENT file_sha256 values by an unsigned-bytewise "
    "keyset query into a deterministic external spool and typed receipt outside "
    "every canonical page transaction; exclude METADATA and resolved spam, preserve "
    "duplicate digests, and bind the receipt to the exact sealed build identity, "
    "sealed analysis identity, live ingest generation, file_count, byte_count, "
    "content_sha256, and private spool identity, while no public API accepts digest, "
    "count, sequence, or receipt authority; stream the exact permutation-invariant "
    "but multiplicity-sensitive frame through iter_effective_content_payload_ordered, "
    "effective_content_digest_ordered, and canonical_value_digest_parts without "
    "materializing the full sequence; bounded canonical page transactions validate "
    "and CAS the live-generation canonical_value_upload claim and resume only from "
    "database receipts; final candidate handoff runs in one transaction, locks and "
    "validates the private typed receipt, immutable SEALED build and COMPLETE analysis "
    "canonical identities, sealed canonical_value_identity, and the same current "
    "live-generation claim, writes the candidate, then deletes that claim; immutable "
    "snapshots permit deterministic spool and receipt reconstruction after response "
    "loss; every digest conflict recomputes content_sha256 and byte-compares the full "
    "preimage; no audit digest, caller-supplied digest/count/sequence/receipt, or "
    "unsealed snapshot authorizes write, resume, or handoff"
)


def _validate_effective_content_contract(
    contract: EffectiveContentContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "effective content contract"
    if (
        contract.reference_attribute != "content_sha256"
        or contract.canonical_value_relation != "canonical_value_identity"
        or contract.canonical_digest_attribute != "value_sha256"
        or contract.digest_domain != "effective_content_v1"
        or contract.encoding_version != 1
        or contract.collision_model != "collision_free_identity_domain"
    ):
        errors.append(f"{prefix} has the wrong canonical digest shape")
    for token in (
        "file_count",
        "file_sha256",
        "raw32",
        "unsigned bytewise ascending",
        "duplicate digests remain repeated",
    ):
        if token not in contract.framing:
            errors.append(f"{prefix} framing omits {token}")
    if contract.write_obligation != _EFFECTIVE_CONTENT_WRITE_OBLIGATION_V1 or not all(
        token in contract.read_obligation
        for token in (
            "effective_content_v1",
            "declared file_count",
            "multiplicity",
            "exact EOF",
            "never",
            "non-injective",
        )
    ):
        errors.append(
            f"{prefix} must state exact preimage, collision, and DB-owned typed "
            "preparation obligations"
        )
    expected_relations = {
        "analysis_impacted_content",
        "analysis_content_owner_candidate",
        "analysis_content_owner",
        "analysis_content_owner_candidate_shadow",
        "analysis_content_owner_candidate_resolved",
        "analysis_content_owner_shadow",
        "analysis_content_owner_tombstone",
        "analysis_content_owner_resolved",
        "catalog_publication_content",
    }
    actual_relations = {
        relation.name
        for relation in relations.values()
        if contract.reference_attribute in relation.attributes
    }
    if actual_relations != expected_relations:
        errors.append(f"{prefix} relation registry is incomplete")
    for relation_name in actual_relations:
        relation = relations[relation_name]
        if not _has_fk(
            relation,
            (contract.reference_attribute,),
            contract.canonical_value_relation,
            (contract.canonical_digest_attribute,),
        ):
            errors.append(
                f"{prefix} relation {relation_name!r} lacks exact canonical payload FK"
            )
    return errors


def _validate_source_snapshot_manifest_contract(
    contract: SourceSnapshotManifestContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "source snapshot manifest contract"
    expected_framing = (
        "ascii('h2hdb-vnext-source-snapshot-manifest\\0') || u32be(codec_version) || "
        "u32be(analysis_algorithm_version) || u64be(spam_artist_threshold) || "
        "u64be(spam_occurrence_threshold) || u32be(content_owner_rule_version) || "
        "u32be(gid_winner_rule_version) || u64be(gallery_count) || "
        "u64be(file_count) || u64be(byte_count) || u64be(gallery_entry_count) || "
        "repeated(raw32(gallery_key) || raw32(observation_identity_sha256) || "
        "u8(content_present) || if_present(raw32(content_sha256)) || u64be(gid)) || "
        "u64be(file_hash_decision_count) || repeated(raw32(file_sha256) || "
        "u64be(occurrence_count) || u64be(artist_count) || "
        "u64be(maximum_gallery_artist_count) || u8(excluded_flag)) || "
        "u64be(content_owner_count) || repeated(raw32(content_sha256) || "
        "raw32(owner_gallery_key)) || "
        "u64be(gid_winner_count) || repeated(u64be(gid) || "
        "raw32(winner_gallery_key))"
    )
    if (
        contract.relation != "source_snapshot_manifest_identity"
        or contract.analysis_binding_relation != "analysis_snapshot_manifest"
        or contract.digest_attribute != "snapshot_manifest_sha256"
        or contract.canonical_value_relation != "canonical_value_identity"
        or contract.canonical_digest_attribute != "value_sha256"
        or contract.digest_domain != "source_snapshot_manifest_v1"
        or contract.codec_version != 1
        or contract.framing != expected_framing
    ):
        errors.append(f"{prefix} has the wrong exact codec")
    order_terms = (
        "gallery entries by gallery_key",
        "file-hash decisions by file_sha256",
        "content owners by content_sha256",
        "GID winners by unsigned gid",
        "duplicate keys are forbidden",
    )
    if any(term not in contract.canonical_order for term in order_terms):
        errors.append(f"{prefix} omits a canonical repeated-section order")
    expected_predicate = (
        "excluded_flag = 1 iff occurrence_count >= spam_occurrence_threshold "
        "AND maximum_gallery_artist_count > 0 AND artist_count > "
        "spam_artist_threshold * maximum_gallery_artist_count, evaluated in "
        "unbounded integers; otherwise excluded_flag = 0"
    )
    if contract.decision_predicate != expected_predicate:
        errors.append(f"{prefix} has the wrong exact spam decision predicate")
    write_terms = (
        "OPEN snapshot-ready analysis",
        "all five immutable component seals",
        "component rows are immutable once the fifth seal exists",
        "fully resolved snapshot",
        "gallery_identity.gallery_key",
        "empty or metadata-only gallery",
        "distinct from every 32-byte digest",
        "every resolved spam decision",
        "occurrence_count",
        "artist_count",
        "maximum_gallery_artist_count",
        "without granting identity authority to evidence_sha256",
        "content owner",
        "GID winner",
        "without granting identity authority to decision_sha256",
        "same snapshot",
        "three declared aggregate counts",
        "payload_byte_count",
        "bounded preflight/keyset receipt",
        "iter_source_snapshot_manifest_payload_rows_ordered",
        "source_snapshot_manifest_digest_ordered",
        "canonical_value_digest_parts",
        "without materializing the snapshot",
        "byte-compare",
    )
    if any(term not in contract.write_obligation for term in write_terms):
        errors.append(f"{prefix} write validation is incomplete")
    handoff_terms = (
        "database-owned private typed canonical snapshot plan",
        "OPEN snapshot-ready analysis",
        "same OPEN analysis and all five immutable component seals",
        "sealed canonical identity",
        "live-generation canonical_value_upload claim",
        "inserts analysis_snapshot_manifest",
        "deletes exactly that claim",
        "CAS-transitions analysis_run from OPEN to COMPLETE",
        "all three mutations commit or roll back together",
        "response loss",
        "analysis_id natural key",
        "exact binding digest equality with the claim absent",
        "no caller digest",
        "input_manifest_sha256",
        "output authority",
    )
    publication_terms = (
        "publication_candidate.analysis_id",
        "through analysis_snapshot_manifest",
        "source_revision.snapshot_manifest_sha256",
        "exact equality",
        "pointer transaction",
        "source_revision_provenance alone is audit provenance",
        "never proves",
    )
    if any(term not in contract.handoff_obligation for term in handoff_terms):
        errors.append(f"{prefix} typed completion handoff is incomplete")
    if any(term not in contract.publication_obligation for term in publication_terms):
        errors.append(f"{prefix} publication output binding is incomplete")
    if not all(
        term in contract.retention
        for term in (
            "while any source_revision or analysis_snapshot_manifest references",
            "analysis cleanup deletes analysis_snapshot_manifest before analysis_run",
            "last source revision and analysis binding",
            "garbage-collecting",
        )
    ):
        errors.append(f"{prefix} retention/garbage-collection rule is incomplete")

    manifest = relations.get(contract.relation)
    if (
        manifest is None
        or set(manifest.attributes)
        != {
            contract.digest_attribute,
            "gallery_count",
            "file_count",
            "byte_count",
        }
        or set(manifest.declared_keys) != {frozenset({contract.digest_attribute})}
        or not _has_fk(
            manifest,
            (contract.digest_attribute,),
            contract.canonical_value_relation,
            (contract.canonical_digest_attribute,),
        )
    ):
        errors.append(f"{prefix} identity relation has the wrong BCNF shape")
    revision = relations.get("source_revision")
    if (
        revision is None
        or set(revision.attributes)
        != {
            "source_revision",
            "channel",
            contract.digest_attribute,
            "published_at",
        }
        or set(revision.declared_keys) != {frozenset({"source_revision"})}
        or any(
            foreign_key.relation == "analysis_run"
            for foreign_key in revision.foreign_keys
        )
    ):
        errors.append(f"{prefix} retained source revision is not self-contained")
    provenance = relations.get("source_revision_provenance")
    if (
        provenance is None
        or set(provenance.attributes) != {"source_revision", "analysis_id"}
        or set(provenance.declared_keys)
        != {frozenset({"source_revision"}), frozenset({"analysis_id"})}
        or not _has_fk(
            provenance,
            ("analysis_id",),
            "analysis_run",
            ("analysis_id",),
        )
    ):
        errors.append(f"{prefix} optional prunable provenance has the wrong shape")
    binding = relations.get(contract.analysis_binding_relation)
    if (
        binding is None
        or set(binding.attributes) != {"analysis_id", contract.digest_attribute}
        or set(binding.declared_keys) != {frozenset({"analysis_id"})}
        or set(binding.functional_dependencies)
        != {
            FunctionalDependency(
                frozenset({"analysis_id"}),
                frozenset({contract.digest_attribute}),
            )
        }
        or not _has_fk(
            binding,
            ("analysis_id",),
            "analysis_run",
            ("analysis_id",),
        )
        or not _has_fk(
            binding,
            (contract.digest_attribute,),
            contract.relation,
            (contract.digest_attribute,),
        )
    ):
        errors.append(
            f"{prefix} analysis output binding must be one-way BCNF authority"
        )
    return errors


def _validate_publication_atomic_contract(
    contract: PublicationAtomicContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "publication atomic contract"
    expected_names = {
        "candidate_relation": "publication_candidate",
        "selection_relation": "publication_selection",
        "artifact_input_relation": "artifact_input",
        "artifact_component_relation": "artifact_semantic_input",
        "operation_relation": "artifact_operation",
        "prepared_artifact_relation": "prepared_artifact",
        "stage_relation": "publication_stage",
        "projection_seal_relation": "publication_candidate_projection_seal",
        "checkpoint_relation": "publication_checkpoint",
        "batch_receipt_relation": "publication_batch_receipt",
        "source_manifest_binding_relation": "analysis_snapshot_manifest",
        "revision_relation": "catalog_revision",
        "head_relation": "publication_head",
    }
    for field, name in expected_names.items():
        if getattr(contract, field) != name or name not in relations:
            errors.append(f"{prefix} {field} must be existing relation {name!r}")
    catalog_publication = relations.get("catalog_publication")
    expected_publication_attributes = {
        "revision",
        "gallery_id",
        "publication_key",
        "summary_sha256",
        "language_sha256",
        "published_at",
        "modified_at",
        "item_sha256",
    }
    if catalog_publication is None or set(catalog_publication.attributes) != (
        expected_publication_attributes
    ):
        errors.append(
            f"{prefix} immutable catalog publication must exclude mutable "
            "redownload queue state"
        )
    expected_batch_stages = (
        (
            "BUILD_SELECTION",
            1,
            "publication_gallery_v1",
            "candidate_OPEN_and_analysis_COMPLETE_with_five_seals",
            "NONE",
        ),
        (
            "VALIDATE_SELECTION",
            2,
            "publication_gallery_v1",
            "BUILD_SELECTION",
            "publication_count",
        ),
        (
            "BUILD_CATALOG_PROJECTION",
            3,
            "publication_catalog_child_v1",
            "VALIDATE_SELECTION",
            "NONE",
        ),
        (
            "VALIDATE_CATALOG_PROJECTION",
            4,
            "publication_catalog_child_v1",
            "BUILD_CATALOG_PROJECTION",
            "NONE",
        ),
        (
            "BUILD_ARTIFACT_INPUT",
            5,
            "publication_key_v1",
            "VALIDATE_CATALOG_PROJECTION",
            "NONE",
        ),
        (
            "BUILD_ARTIFACT_DELTA_OPERATION",
            6,
            "publication_key_v1",
            "BUILD_ARTIFACT_INPUT",
            "NONE",
        ),
        (
            "VALIDATE_ARTIFACT_INPUT_DELTA",
            7,
            "publication_key_v1",
            "BUILD_ARTIFACT_DELTA_OPERATION",
            "artifact_input_count",
        ),
        (
            "VALIDATE_PREPARED_ARTIFACT",
            8,
            "publication_key_v1",
            "VALIDATE_ARTIFACT_INPUT_DELTA",
            "prepared_artifact_count",
        ),
        (
            "VALIDATE_CREATE",
            9,
            "publication_key_v1",
            "VALIDATE_PREPARED_ARTIFACT",
            "create_count",
        ),
        (
            "VALIDATE_REBUILD",
            10,
            "publication_key_v1",
            "VALIDATE_CREATE",
            "rebuild_count",
        ),
        (
            "VALIDATE_DELETE",
            11,
            "publication_key_v1",
            "VALIDATE_REBUILD",
            "delete_count",
        ),
        (
            "VALIDATE_UNCHANGED",
            12,
            "publication_key_v1",
            "VALIDATE_DELETE",
            "unchanged_count",
        ),
        (
            "VALIDATE_NEW_GALLERY",
            13,
            "publication_key_v1",
            "VALIDATE_UNCHANGED",
            "new_galleries",
        ),
        (
            "VALIDATE_CHANGED_GALLERY",
            14,
            "publication_key_v1",
            "VALIDATE_NEW_GALLERY",
            "changed_galleries",
        ),
        (
            "VALIDATE_REMOVED_GALLERY",
            15,
            "publication_key_v1",
            "VALIDATE_CHANGED_GALLERY",
            "removed_galleries",
        ),
        (
            "VALIDATE_DUPLICATE_LOSER",
            16,
            "publication_gallery_v1",
            "VALIDATE_REMOVED_GALLERY",
            "duplicate_losers",
        ),
        (
            "FINALIZE_ARTIFACTS",
            17,
            "publication_key_v1",
            "publication_receipt_DB_COMMITTED",
            "finalized_artifact_count",
        ),
    )
    if (
        tuple(
            (
                stage.name,
                stage.stage_order,
                stage.cursor_codec,
                stage.prerequisite,
                stage.sealed_scalar,
            )
            for stage in contract.batch_stages
        )
        != expected_batch_stages
    ):
        errors.append(f"{prefix} stage/order/codec/prerequisite registry drifts")
    selection_terms = (
        "immutable source_build_gallery",
        "unsigned positive gallery_id keyset order",
        "if and only if",
        "analysis_content_owner_resolved owner",
        "no nonexcluded CONTENT digest as its own owner",
        "analysis_gid_winner_resolved winner",
        "publication-key.v1",
        "publication_selection stores no audit digest",
        "VALIDATE_SELECTION independently",
        "exact merge-compares candidate_id, gallery_id, and publication_key",
        "empty terminal receipt owns publication_count",
    )
    if any(term not in contract.selection_rule for term in selection_terms):
        errors.append(f"{prefix} selection predicate is not closed and executable")
    cursor_terms = (
        "publication_gallery_v1 is empty at genesis or u64be(positive gallery_id)",
        "publication_key_v1 is empty at genesis or raw32(publication_key)",
        "publication_catalog_child_v1 is empty at genesis",
        "u8(child_kind",
        "u16be(subkey_length)",
        "exact EOF",
        "unsigned bytes",
        "locked registered stage selects the codec",
        "caller cursor bytes never select or alter a query",
    )
    if any(term not in contract.cursor_codec_rule for term in cursor_terms):
        errors.append(f"{prefix} cursor codec framing is not closed-world")
    batch_terms = (
        "all seventeen provider-seeded publication stages in exact stage_order",
        "generation one, empty cursor, processed_count zero, and OPEN state",
        "hard-capped maximum-128 batch",
        "server-side",
        "full immutable pre/post publication_batch_receipt",
        "committed_generation equals start_generation plus one",
        "next_processed_count equals start_processed_count plus row_count",
        "complete stored tuple without writes",
        "terminal is one if and only if the server-derived page is empty",
        "next_state COMPLETE",
        "nonterminal means positive row_count and next_state OPEN",
        "no caller stage, cursor, count, state, terminal, digest, sequence, or receipt is authority",
    )
    if any(term not in contract.batch_rule for term in batch_terms):
        errors.append(f"{prefix} batch receipt/CAS rule is incomplete")
    seal_terms = (
        "VALIDATE_DUPLICATE_LOSER becomes COMPLETE",
        "terminal checkpoints for VALIDATE_SELECTION",
        "VALIDATE_CATALOG_PROJECTION",
        "VALIDATE_ARTIFACT_INPUT_DELTA",
        "VALIDATE_PREPARED_ARTIFACT",
        "VALIDATE_CREATE",
        "VALIDATE_REBUILD",
        "VALIDATE_DELETE",
        "VALIDATE_UNCHANGED",
        "VALIDATE_NEW_GALLERY",
        "VALIDATE_CHANGED_GALLERY",
        "VALIDATE_REMOVED_GALLERY",
        "VALIDATE_DUPLICATE_LOSER",
        "terminal COMPLETE after its independent exact child merge",
        "never compare its child processed_count with publication count",
        "publication_count only from terminal VALIDATE_SELECTION",
        "prepared_artifact_count equals create_count plus rebuild_count",
        "artifact_input_count equals create_count plus rebuild_count plus unchanged_count",
        "named O(1) terminal validation scalars",
        "never SUM receipts or child relations",
    )
    if any(term not in contract.projection_seal_rule for term in seal_terms):
        errors.append(f"{prefix} projection-seal scalar authority is incomplete")
    if any(
        forbidden in contract.projection_seal_rule
        for forbidden in (
            "VALIDATE_CATALOG_PROJECTION processed_count equals VALIDATE_SELECTION",
            "publication_count_crosscheck",
        )
    ):
        errors.append(
            f"{prefix} must not equate catalog-child count with publication count"
        )
    obligation_terms = (
        "exact selected publication child set",
        "publication_count",
        "catalog_publication_order",
        "contiguous zero-based",
        "six exactly-once",
        "delta classification",
        "byte digest",
        "ZIP comment",
        "protection token",
        "locator",
        "digest/count",
        "compare-and-swap",
        "base head",
        "partial revision",
        "catalog_summary_utf8_v1",
        "catalog_language_utf8_v1",
        "upload claim",
        "unbounded bind",
        "minimum gallery tag position",
        "exact ASCII bytes und",
        "without normalization or trimming",
        "artifact_locator_bytes_v1",
        "at most 4096 bytes",
        "iter_artifact_locator_payload",
        "exact EOF",
        "u32be(segment_count)",
        "strict_utf8_segment",
        "before candidate state SEALED",
        "independent full evaluator",
        "artifact_input_count",
        "prepared_artifact_count",
        "CREATE/REBUILD/DELETE/UNCHANGED counts",
        "new_galleries/changed_galleries/removed_galleries/duplicate_losers",
        "through analysis_snapshot_manifest",
        "publication_candidate_preparation",
        "only O(1)",
        "never COUNT or SUM over child rows",
        "never trust a caller/audit digest or guessed scalar",
    )
    if any(term not in contract.runtime_obligation for term in obligation_terms):
        errors.append(f"{prefix} omits a required READY/CAS validation obligation")
    artifact = relations.get("catalog_artifact")
    if artifact is not None and frozenset({"revision", "artifact_name"}) in set(
        artifact.declared_keys
    ):
        errors.append(
            f"{prefix} must not treat nested-gallery artifact display names as unique"
        )
    receipt = relations.get("publication_receipt")
    receipt_scalars = {
        "publication_count",
        "new_galleries",
        "changed_galleries",
        "removed_galleries",
        "duplicate_losers",
    }
    if receipt is None or not receipt_scalars <= set(receipt.attributes):
        errors.append(
            f"{prefix} requires an O(1) authoritative publication_count and "
            "result scalars on "
            "publication_receipt"
        )
    seal = relations.get(contract.projection_seal_relation)
    seal_attributes = {
        "candidate_id",
        "publication_count",
        "artifact_input_count",
        "prepared_artifact_count",
        "create_count",
        "rebuild_count",
        "delete_count",
        "unchanged_count",
        "new_galleries",
        "changed_galleries",
        "removed_galleries",
        "duplicate_losers",
        "projection_sealed_at",
    }
    if (
        seal is None
        or set(seal.attributes) != seal_attributes
        or set(seal.declared_keys) != {frozenset({"candidate_id"})}
        or not _has_fk(
            seal,
            ("candidate_id",),
            contract.candidate_relation,
            ("candidate_id",),
        )
        or any(
            attribute.endswith("sha256") or attribute.endswith("digest")
            for attribute in seal.attributes
        )
    ):
        errors.append(f"{prefix} projection seal lacks exact digest-free BCNF shape")
    checkpoint = relations.get(contract.checkpoint_relation)
    stage_relation = relations.get(contract.stage_relation)
    if (
        stage_relation is None
        or set(stage_relation.attributes) != {"stage", "stage_order", "cursor_codec"}
        or set(stage_relation.declared_keys)
        != {frozenset({"stage"}), frozenset({"stage_order"})}
    ):
        errors.append(f"{prefix} stage registry lacks its exact BCNF shape")
    if (
        checkpoint is None
        or set(checkpoint.attributes)
        != {
            "candidate_id",
            "stage",
            "generation",
            "cursor",
            "processed_count",
            "state",
            "updated_at",
        }
        or set(checkpoint.declared_keys) != {frozenset({"candidate_id", "stage"})}
        or not _has_fk(
            checkpoint,
            ("stage",),
            contract.stage_relation,
            ("stage",),
        )
    ):
        errors.append(f"{prefix} checkpoint lacks processed-count terminal authority")
    batch_receipt = relations.get(contract.batch_receipt_relation)
    expected_batch_keys = {
        frozenset({"candidate_id", "stage", "batch_key"}),
        frozenset({"candidate_id", "stage", "start_generation"}),
    }
    if (
        batch_receipt is None
        or set(batch_receipt.attributes)
        != {
            "candidate_id",
            "stage",
            "batch_key",
            "start_generation",
            "start_cursor",
            "start_processed_count",
            "next_cursor",
            "next_processed_count",
            "next_state",
            "row_count",
            "terminal",
            "committed_generation",
            "committed_at",
        }
        or set(batch_receipt.declared_keys) != expected_batch_keys
        or not _has_fk(
            batch_receipt,
            ("candidate_id", "stage"),
            contract.checkpoint_relation,
            ("candidate_id", "stage"),
        )
    ):
        errors.append(f"{prefix} batch receipt lacks exact replay response authority")
    finalization_terms = (
        "DB_COMMITTED",
        "never mass-update prepared_artifact",
        "fixed FINALIZE_ARTIFACTS checkpoint",
        "publication_key keyset cursor",
        "processed_count",
        "hard-capped transaction",
        "PREPARED rows server-side",
        "releases their exact protection tokens",
        "COMMITTED",
        "full publication_batch_receipt response tuple",
        "exact batch_key retry",
        "without writes",
        "empty terminal batch alone",
        "terminal=1 and COMPLETE",
        "projection-seal prepared_artifact_count",
        "PROJECTION_FINALIZED",
    )
    if contract.finalization_stage != "FINALIZE_ARTIFACTS" or any(
        term not in contract.finalization_rule for term in finalization_terms
    ):
        errors.append(f"{prefix} bounded artifact finalization protocol is incomplete")
    publication_order = relations.get("catalog_publication_order")
    expected_order_keys = {
        frozenset({"revision", "position"}),
        frozenset({"revision", "publication_key"}),
    }
    if (
        publication_order is None
        or set(publication_order.attributes)
        != {"revision", "position", "publication_key"}
        or set(publication_order.declared_keys) != expected_order_keys
        or not _has_fk(
            publication_order,
            ("revision", "publication_key"),
            "catalog_publication",
            ("revision", "publication_key"),
        )
    ):
        errors.append(f"{prefix} requires a BCNF catalog_publication_order projection")
    return errors


def _validate_file_identity_contract(
    contract: FileIdentityContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "file identity contract"
    if (
        contract.relation != "file_name_identity"
        or contract.key_attribute != "file_key"
        or contract.name_attribute != "name_bytes"
        or contract.role_attribute != "file_role"
        or contract.algorithm_version != 1
        or contract.role_classifier_version != 1
        or contract.metadata_name != "galleryinfo.txt"
        or contract.collision_model != "collision_free_identity_domain"
    ):
        errors.append(f"{prefix} has the wrong versioned identity/classifier shape")
    expected_framing = (
        "ascii('h2hdb-vnext-file-key\\0') || u32be(algorithm_version) || "
        "u32be(name_length) || name_bytes"
    )
    if contract.framing != expected_framing:
        errors.append(f"{prefix} framing must include the exact domain prefix")
    if not all(
        token in contract.write_obligation
        for token in ("SHA-256", "byte-compare", "if and only if", "galleryinfo.txt")
    ) or not all(
        token in contract.read_obligation
        for token in ("recompute", "unknown", "case folding", "collation")
    ):
        errors.append(f"{prefix} must state exact collision and classifier obligations")
    relation = relations.get(contract.relation)
    if relation is None:
        errors.append(f"{prefix} references an unknown relation")
        return errors
    if set(relation.attributes) != {
        contract.key_attribute,
        contract.name_attribute,
        contract.role_attribute,
    } or set(relation.declared_keys) != {
        frozenset({contract.key_attribute}),
        frozenset({contract.name_attribute}),
    }:
        errors.append(f"{prefix} relation must declare exact digest and name keys")
    return errors


def _validate_gallery_observation_identity_contract(
    contract: GalleryObservationIdentityContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "gallery observation identity contract"
    if (
        contract.relation != "gallery_observation"
        or contract.gallery_attribute != "gallery_id"
        or contract.identifier_attribute != "observation_id"
        or contract.digest_attribute != "observation_identity_sha256"
        or contract.canonical_value_relation != "canonical_value_identity"
        or contract.canonical_digest_attribute != "value_sha256"
        or contract.digest_domain != "gallery_observation_v1"
        or contract.encoding_version != 1
    ):
        errors.append(f"{prefix} has the wrong canonical identity shape")
    framing_terms = (
        "METADATA root_page_sha256",
        "METADATA exact_byte_count",
        "FILE root_page_sha256",
        "TAG root_page_sha256",
        "DIRECTORY root_page_sha256",
    )
    forbidden_audits = (
        "metadata_fingerprint",
        "scan_observation_sha256",
        "metadata_sha256",
        "raw_content_sha256",
        "directory_observation_sha256",
    )
    if any(term not in contract.framing for term in framing_terms) or any(
        term in contract.framing for term in forbidden_audits
    ):
        errors.append(f"{prefix} framing omits facts or grants audit digest authority")
    if not all(
        term in contract.write_obligation
        for term in (
            "complete scan",
            "METADATA/FILE/TAG/DIRECTORY page-tree roots",
            "METADATA count",
            "FILE and DIRECTORY exact merge-join coverage",
            "byte-compare",
            "insert the final gallery_observation seal last",
        )
    ) or not all(
        term in contract.reuse_obligation
        for term in (
            "complete current gallery",
            "METADATA/FILE/TAG/DIRECTORY page trees",
            "never authorize reuse",
        )
    ):
        errors.append(f"{prefix} collision/revalidation obligations are incomplete")
    relation = relations.get(contract.relation)
    if relation is None:
        errors.append(f"{prefix} references an unknown relation")
        return errors
    expected_keys = {
        frozenset({contract.gallery_attribute, contract.identifier_attribute}),
        frozenset({contract.gallery_attribute, contract.digest_attribute}),
    }
    if set(relation.declared_keys) != expected_keys:
        errors.append(f"{prefix} relation must declare exact ID and digest keys")
    if not _has_fk(
        relation,
        (contract.digest_attribute,),
        contract.canonical_value_relation,
        (contract.canonical_digest_attribute,),
    ):
        errors.append(f"{prefix} relation lacks canonical exact-preimage FK")
    return errors


def _canonical_value_sha256(domain: str, payload: bytes) -> str:
    domain_bytes = domain.encode("ascii")
    preimage = b"".join(
        (
            b"h2hdb-vnext-canonical-value\0",
            (1).to_bytes(4, "big"),
            len(domain_bytes).to_bytes(4, "big"),
            domain_bytes,
            len(payload).to_bytes(8, "big"),
            payload,
        )
    )
    return hashlib.sha256(preimage).hexdigest()


def _validate_source_root_contract(
    contract: SourceRootContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "source root contract"
    expected = {
        "canonical_value_relation": "canonical_value_identity",
        "canonical_digest_attribute": "value_sha256",
        "digest_domain": "source_root_v1",
        "codec_version": 1,
        "encoding": "absolute_posix_utf8_segments",
        "framing": (
            "u32be(codec_version) || u32be(segment_count) || "
            "repeated(u32be(segment_utf8_byte_length) || strict_segment_utf8)"
        ),
    }
    for field, value in expected.items():
        if getattr(contract, field) != value:
            errors.append(f"{prefix} {field} must be {value!r}")
    required_rules = {
        contract.root_rule: ("absolute POSIX root /", "zero-segment"),
        contract.segment_rule: ("1..255", "strict UTF-8", "NUL", "slash", ".", ".."),
        contract.adapter_rule: ("adapters", "absolute POSIX path", "never accepts"),
        contract.write_obligation: (
            "source_root_v1 canonical digest wrapper",
            "iter_source_root_payload",
            "source_root_digest",
            "canonical_value_digest_parts",
            "without materializing the complete value",
            "validate_source_root_parts",
            "exact EOF receipt",
            "decode_source_root is a convenience oracle only",
            "recompute source_root_sha256",
            "byte-compare",
        ),
    }
    for rule, terms in required_rules.items():
        if any(term not in rule for term in terms):
            errors.append(f"{prefix} has an incomplete canonical rule")
    for payload_hex, digest in (
        (contract.golden_root_payload_hex, contract.golden_root_sha256),
        (contract.golden_nested_payload_hex, contract.golden_nested_sha256),
    ):
        try:
            payload = bytes.fromhex(payload_hex)
        except ValueError:
            errors.append(f"{prefix} golden payload must be lowercase even-length hex")
            continue
        if (
            payload.hex() != payload_hex
            or len(digest) != 64
            or _canonical_value_sha256("source_root_v1", payload) != digest
        ):
            errors.append(f"{prefix} hardcoded golden does not recompute")
    if contract.canonical_value_relation not in relations:
        errors.append(f"{prefix} canonical value relation is missing")
    return errors


def _validate_gallery_observation_page_contract(
    contract: GalleryObservationPageContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "gallery observation page contract"
    expected_names = {
        "allocation_relation": "gallery_observation_allocation",
        "final_relation": "gallery_observation",
        "page_relation": "gallery_observation_page",
        "allocation_page_relation": "gallery_observation_allocation_page",
        "descriptor_relation": "gallery_observation_page_descriptor",
        "key_bounds_relation": "gallery_observation_page_key_bounds",
        "child_relation": "gallery_observation_page_child",
        "tree_root_relation": "gallery_observation_tree_root",
    }
    for field, name in expected_names.items():
        if getattr(contract, field) != name or name not in relations:
            errors.append(f"{prefix} {field} must be existing relation {name!r}")
    if (
        contract.page_digest_attribute != "page_sha256"
        or contract.page_bytes_attribute != "page_bytes"
        or contract.algorithm != "SHA-256"
        or contract.codec_version != 1
        or contract.prefix != "h2hdb-vnext-gallery-observation-page\\0"
        or contract.maximum_page_bytes != 65536
        or contract.maximum_entries != 256
        or contract.file_leaf_capacity != 256
        or contract.tag_leaf_capacity != 256
        or contract.directory_leaf_capacity != 192
        or contract.metadata_leaf_capacity != 1
        or contract.metadata_chunk_maximum_bytes != 32768
        or contract.branch_capacity != 256
        or contract.maximum_level != 8
        or contract.maximum_items != (1 << 63) - 1
        or contract.components != ("FILE", "TAG", "DIRECTORY", "METADATA")
        or contract.node_kinds != ("LEAF", "BRANCH")
    ):
        errors.append(f"{prefix} constants drift from the exact v1 protocol")
    if 56 + contract.directory_leaf_capacity * 311 > contract.maximum_page_bytes:
        errors.append(f"{prefix} DIRECTORY leaf capacity exceeds the byte budget")
    if (
        56
        + contract.metadata_leaf_capacity * (12 + contract.metadata_chunk_maximum_bytes)
        > contract.maximum_page_bytes
    ):
        errors.append(f"{prefix} METADATA leaf capacity exceeds the byte budget")
    required_rules = (
        "exact opaque POSIX 1..255-byte",
        "invalid UTF-8",
        "final leaf has 1..capacity",
        "exact capacity multiple never appends an empty page",
        "fixed-prefix METADATA stream is never empty",
        "zero-based contiguous",
        "no unary wrapper",
        "os.fsencode",
        "surrogateescape",
        "lstat without symlink following",
        "signed i64 two's-complement",
        "every nonfinal METADATA chunk is exactly 32768 bytes",
        "checked in unbounded host arithmetic",
        "without wrap",
    )
    if any(term not in contract.canonical_tree_rule for term in required_rules):
        errors.append(f"{prefix} canonical tree boundary rules are incomplete")
    materialization_terms = (
        "first_key and last_key",
        "file_name_identity",
        "content_blob",
        "gallery_observation_file_filesystem",
        "canonical tag-value payload",
        "sole row-level authority",
        "receipted exact counters",
        "independent durable exact merge-join",
        "every nonregular DIRECTORY record to have no FILE record",
        "METADATA boundaries are exact u64be(byte_offset)",
        "next offset to equal the previous offset plus the exact previous chunk length",
    )
    if any(term not in contract.materialization_rule for term in materialization_terms):
        errors.append(f"{prefix} exact decode materialization rule is incomplete")
    metadata_terms = (
        "gallery-observation-metadata",
        "u8(field=1:title)",
        "u8(field=2:comment)",
        "u8(field=3:upload_account)",
        "u64be(strict_utf8_length_int63)",
        "u8(page_count_presence)",
        "fields are fixed in this order",
        "no generic map",
        "deterministic chunks",
    )
    if any(term not in contract.metadata_stream_framing for term in metadata_terms):
        errors.append(f"{prefix} METADATA stream framing is incomplete")
    if any(
        term not in contract.seal_obligation
        for term in (
            "exactly one METADATA, one FILE, one TAG, and one DIRECTORY",
            "exact receipted leaf-to-normalized-row coverage",
            "bounded scalar checkpoints",
            "never rescans",
        )
    ):
        errors.append(f"{prefix} final seal rule is incomplete")
    for term in (
        "atomically inserts gallery_observation_allocation_page",
        "zero allocation associations",
        "zero incoming child edges",
        "outer maintenance gate",
        "skips FK-blocked rows",
        "repeats parent-level-first until convergence",
    ):
        if term not in contract.cleanup_rule:
            errors.append(f"{prefix} cleanup rule omits {term!r}")
    try:
        golden_page = bytes.fromhex(contract.golden_empty_file_page_hex)
    except ValueError:
        golden_page = b""
    if (
        golden_page.hex() != contract.golden_empty_file_page_hex
        or len(golden_page) != 56
        or hashlib.sha256(golden_page).hexdigest()
        != contract.golden_empty_file_page_sha256
    ):
        errors.append(f"{prefix} empty FILE page golden does not recompute")

    allocation = relations.get(contract.allocation_relation)
    page = relations.get(contract.page_relation)
    allocation_page = relations.get(contract.allocation_page_relation)
    descriptor = relations.get(contract.descriptor_relation)
    bounds = relations.get(contract.key_bounds_relation)
    child = relations.get(contract.child_relation)
    root = relations.get(contract.tree_root_relation)
    final = relations.get(contract.final_relation)
    if allocation is not None and (
        set(allocation.attributes) != {"gallery_id", "observation_id", "allocated_at"}
        or set(allocation.declared_keys)
        != {frozenset({"gallery_id", "observation_id"})}
    ):
        errors.append(f"{prefix} allocation relation has the wrong exact shape")
    if page is not None and (
        set(page.attributes) != {"page_sha256", "page_bytes"}
        or set(page.declared_keys)
        != {frozenset({"page_sha256"}), frozenset({"page_bytes"})}
    ):
        errors.append(f"{prefix} page relation has the wrong collision keys")
    if allocation_page is not None and (
        set(allocation_page.attributes)
        != {"gallery_id", "observation_id", "page_sha256"}
        or set(allocation_page.declared_keys)
        != {frozenset({"gallery_id", "observation_id", "page_sha256"})}
    ):
        errors.append(f"{prefix} allocation-page liveness relation has wrong shape")
    if descriptor is not None and (
        set(descriptor.attributes)
        != {
            "page_sha256",
            "component",
            "level",
            "subtree_item_count",
        }
        or set(descriptor.declared_keys) != {frozenset({"page_sha256"})}
    ):
        errors.append(f"{prefix} descriptor relation has the wrong normalized shape")
    if bounds is not None and (
        set(bounds.attributes) != {"page_sha256", "first_key", "last_key"}
        or set(bounds.declared_keys) != {frozenset({"page_sha256"})}
    ):
        errors.append(f"{prefix} key bounds satellite has the wrong optional shape")
    if child is not None and (
        set(child.attributes) != {"parent_sha256", "position", "child_sha256"}
        or set(child.declared_keys)
        != {
            frozenset({"parent_sha256", "position"}),
            frozenset({"parent_sha256", "child_sha256"}),
        }
    ):
        errors.append(f"{prefix} child relation lacks exact position/digest keys")
    if root is not None and (
        set(root.attributes) != {"gallery_id", "observation_id", "root_page_sha256"}
        or set(root.declared_keys)
        != {frozenset({"gallery_id", "observation_id", "root_page_sha256"})}
    ):
        errors.append(f"{prefix} root relation repeats descriptor authority")
    if final is not None and not _has_fk(
        final,
        ("gallery_id", "observation_id"),
        "gallery_observation_allocation",
        ("gallery_id", "observation_id"),
    ):
        errors.append(f"{prefix} final seal does not reference allocation")
    return errors


def _validate_title_sort_contract(
    contract: TitleSortContract,
    relations: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "title sort contract"
    expected = {
        "policy_relation": "title_sort_policy",
        "display_policy_relation": "display_title_policy",
        "sort_relation": "title_sort",
        "algorithm_attribute": "title_sort_algorithm_version",
        "unicode_attribute": "unicode_data_version",
    }
    for field, value in expected.items():
        if getattr(contract, field) != value:
            errors.append(f"{prefix} {field} must be {value!r}")
    if not all(
        term in contract.runtime_obligation
        for term in ("display_title_policy_id", "Unicode", "unknown", "new policy")
    ):
        errors.append(f"{prefix} runtime version obligation is incomplete")
    policy = relations.get(contract.policy_relation)
    display = relations.get(contract.display_policy_relation)
    sort = relations.get(contract.sort_relation)
    if policy is None or set(policy.declared_keys) != {
        frozenset({"title_sort_policy_id"}),
        frozenset({contract.algorithm_attribute, contract.unicode_attribute}),
    }:
        errors.append(f"{prefix} policy lacks ID and exact version keys")
    if display is None or not _has_fk(
        display,
        ("title_sort_policy_id",),
        contract.policy_relation,
        ("title_sort_policy_id",),
    ):
        errors.append(f"{prefix} display policy does not pin the sort policy")
    if sort is None or set(sort.declared_keys) != {
        frozenset({"title_sort_policy_id", "title_sha256"})
    }:
        errors.append(f"{prefix} sort relation has the wrong policy-scoped key")
    return errors


def _validate_operational_integrity_contracts(
    contract: Contract,
    relations: Mapping[str, Relation],
    external_relations: Mapping[str, ExternalRelation],
) -> list[str]:
    """Close operational history, canonical identity, and lifecycle assumptions."""

    errors: list[str] = []
    required_contracts = {
        "queue_history_contract": contract.queue_history_contract,
        "canonical_hash_cache_contract": contract.canonical_hash_cache_contract,
        "operational_event_integrity_contract": (
            contract.operational_event_integrity_contract
        ),
        "source_build_generation_contract": (contract.source_build_generation_contract),
        "cleanup_attempt_contract": contract.cleanup_attempt_contract,
        "preparation_identity_contract": contract.preparation_identity_contract,
    }
    for name, value in required_contracts.items():
        if value is None:
            errors.append(f"operational contract must declare {name}")
    if errors:
        return errors

    queue = contract.queue_history_contract
    canonical = contract.canonical_hash_cache_contract
    event = contract.operational_event_integrity_contract
    generation = contract.source_build_generation_contract
    cleanup = contract.cleanup_attempt_contract
    preparation = contract.preparation_identity_contract
    assert queue is not None
    assert canonical is not None
    assert event is not None
    assert generation is not None
    assert cleanup is not None
    assert preparation is not None

    expected_queue_names = {
        "deletion_generation_relation": "deletion_request_generation",
        "deletion_generation_head_relation": "deletion_request_generation_head",
        "deletion_attempt_relation": "deletion_request_attempt",
        "deletion_head_relation": "deletion_request_head",
        "deletion_url_relation": "deletion_request_url",
        "consumption_relation": "operational_deletion_consumption_event",
        "preparation_relation": "operational_preparation",
    }
    for field, expected in expected_queue_names.items():
        if getattr(queue, field) != expected:
            errors.append(f"queue history {field} must be {expected!r}")
    if "optional" not in queue.rule or "immutable" not in queue.rule:
        errors.append(
            "queue history rule must state optional URL and immutable history"
        )
    if not all(
        term in queue.generation_rule
        for term in (
            "generation zero",
            "real immutable empty-queue genesis",
            "never a sentinel",
            "exactly current_generation plus one",
            "exact-CAS",
            "2^63-1 fails closed",
        )
    ):
        errors.append(
            "queue generation rule must state real genesis, checked successor, "
            "exact CAS, and fail-closed exhaustion"
        )
    if not all(
        term in queue.publication_rule
        for term in ("exact FK-backed", "singleton head", "O(1)", "differ")
    ):
        errors.append(
            "queue publication rule must state exact FK-backed O(1) head recheck"
        )
    if not all(
        term in queue.retention_rule
        for term in ("retained indefinitely", "immutable FK", "every preparation")
    ):
        errors.append("queue generation history must be retained as FK authority")
    deletion_generation = relations.get(queue.deletion_generation_relation)
    generation_head = relations.get(queue.deletion_generation_head_relation)
    attempt = relations.get(queue.deletion_attempt_relation)
    head = relations.get(queue.deletion_head_relation)
    url = relations.get(queue.deletion_url_relation)
    queue_preparation = relations.get(queue.preparation_relation)
    consumption = relations.get(queue.consumption_relation)
    if (
        deletion_generation is None
        or set(deletion_generation.attributes) != {"generation", "allocated_at"}
        or set(deletion_generation.declared_keys) != {frozenset({"generation"})}
        or set(deletion_generation.functional_dependencies)
        != {
            FunctionalDependency(frozenset({"generation"}), frozenset({"allocated_at"}))
        }
    ):
        errors.append(
            "deletion generation must be exact immutable generation-keyed history"
        )
    if (
        generation_head is None
        or set(generation_head.attributes)
        != {"singleton_id", "current_generation", "updated_at"}
        or set(generation_head.declared_keys) != {frozenset({"singleton_id"})}
        or set(generation_head.functional_dependencies)
        != {
            FunctionalDependency(
                frozenset({"singleton_id"}),
                frozenset({"current_generation", "updated_at"}),
            )
        }
    ):
        errors.append("deletion generation head must have the exact singleton shape")
    elif not _has_fk(
        generation_head,
        ("current_generation",),
        queue.deletion_generation_relation,
        ("generation",),
    ):
        errors.append("deletion generation head must reference exact history")
    if (
        attempt is None
        or set(attempt.attributes)
        != {
            "request_token",
            "gid",
            "requested_at",
        }
        or set(attempt.declared_keys) != {frozenset({"request_token"})}
    ):
        errors.append("deletion attempt must be immutable token-keyed history")
    if (
        head is None
        or set(head.attributes) != {"gid", "request_token"}
        or set(head.declared_keys) != {frozenset({"gid"}), frozenset({"request_token"})}
    ):
        errors.append("deletion head must have exact gid and token candidate keys")
    elif not _has_fk(
        head,
        ("request_token",),
        queue.deletion_attempt_relation,
        ("request_token",),
    ):
        errors.append("deletion head must reference immutable attempt history")
    if (
        url is None
        or set(url.attributes) != {"request_token", "url"}
        or set(url.declared_keys) != {frozenset({"request_token"})}
    ):
        errors.append("deletion URL must be an optional token-keyed satellite")
    elif not _has_fk(
        url,
        ("request_token",),
        queue.deletion_attempt_relation,
        ("request_token",),
    ):
        errors.append("deletion URL must reference immutable attempt history")
    if consumption is None or not _has_fk(
        consumption,
        ("deletion_request_token",),
        queue.deletion_attempt_relation,
        ("request_token",),
    ):
        errors.append("deletion consumption must reference immutable attempt history")
    if queue_preparation is None or not _has_fk(
        queue_preparation,
        ("deletion_request_generation",),
        queue.deletion_generation_relation,
        ("generation",),
    ):
        errors.append("operational preparation must reference exact deletion history")

    expected_canonical_fields = {
        "canonical_value_relation": "canonical_value_identity",
        "canonical_allocation_relation": "canonical_value_allocation",
        "canonical_page_relation": "canonical_value_page",
        "canonical_digest_attribute": "value_sha256",
        "canonical_policy_attribute": "digest_domain",
        "canonical_byte_count_attribute": "byte_count",
        "canonical_root_attribute": "root_page_sha256",
        "observation_relation": "hash_cache_observation",
        "source_digest_attribute": "source_identity_sha256",
        "fingerprint_digest_attribute": "fingerprint_sha256",
    }
    for field, expected in expected_canonical_fields.items():
        if getattr(canonical, field) != expected:
            errors.append(f"canonical hash-cache {field} must be {expected!r}")
    if canonical.source_domain == canonical.fingerprint_domain:
        errors.append("canonical hash-cache source and fingerprint domains must differ")
    if not all(
        term in canonical.write_obligation
        for term in ("stream", "byte-compare", "canonical_value_upload")
    ):
        errors.append(
            "canonical hash-cache write must require streamed collision compare "
            "under a canonical upload claim"
        )
    if not all(
        term in canonical.read_obligation
        for term in ("exact preimage", "page tree", "recompute")
    ):
        errors.append("canonical hash-cache read must require exact preimage authority")
    canonical_external = external_relations.get(canonical.canonical_value_relation)
    if (
        canonical_external is None
        or set(canonical_external.attributes)
        != {
            canonical.canonical_digest_attribute,
            canonical.canonical_root_attribute,
        }
        or set(canonical_external.declared_keys)
        != {
            frozenset({canonical.canonical_digest_attribute}),
            frozenset({canonical.canonical_root_attribute}),
        }
    ):
        errors.append(
            "canonical hash-cache final identity must expose digest and root keys"
        )
    canonical_allocation = external_relations.get(
        canonical.canonical_allocation_relation
    )
    if (
        canonical_allocation is None
        or set(canonical_allocation.attributes)
        != {
            canonical.canonical_digest_attribute,
            canonical.canonical_policy_attribute,
            canonical.canonical_byte_count_attribute,
            "allocated_at",
        }
        or set(canonical_allocation.declared_keys)
        != {frozenset({canonical.canonical_digest_attribute})}
    ):
        errors.append(
            "canonical hash-cache allocation must expose exact domain and byte count"
        )
    canonical_page = external_relations.get(canonical.canonical_page_relation)
    if (
        canonical_page is None
        or set(canonical_page.attributes)
        != {"page_sha256", canonical.canonical_digest_attribute, "page_bytes"}
        or set(canonical_page.declared_keys)
        != {frozenset({"page_sha256"}), frozenset({"page_bytes"})}
    ):
        errors.append(
            "canonical hash-cache page authority must expose exact owner-scoped bytes"
        )
    observation = relations.get(canonical.observation_relation)
    if observation is None:
        errors.append("canonical hash-cache observation relation is missing")
    else:
        for attribute in (
            canonical.source_digest_attribute,
            canonical.fingerprint_digest_attribute,
        ):
            if not _has_fk(
                observation,
                (attribute,),
                canonical.canonical_value_relation,
                (canonical.canonical_digest_attribute,),
            ):
                errors.append(
                    f"canonical hash-cache {attribute!r} lacks exact-preimage FK"
                )

    expected_event_names = {
        "stream_relation": "operational_event_stream",
        "preparation_relation": "operational_preparation",
        "seal_relation": "operational_preparation_effect_seal",
        "activation_relation": "operational_activation",
        "candidate_binding_relation": "publication_candidate_preparation",
        "base_relation": "operational_event",
        "removed_subtype_relation": "operational_removed_gid_event",
        "deletion_subtype_relation": "operational_deletion_consumption_event",
        "ack_head_relation": "operational_event_ack_head",
    }
    for field, expected in expected_event_names.items():
        if getattr(event, field) != expected:
            errors.append(f"event integrity {field} must be {expected!r}")
    if event.removed_event_type != "REMOVED_GID" or event.deletion_event_type != (
        "DELETION_CONSUMPTION"
    ):
        errors.append("event integrity must pin both event type values")
    if event.removed_event_type == event.deletion_event_type:
        errors.append("event subtype type values must be distinct")
    if (
        "one transaction" not in event.stream_rule
        or "no standalone invisible stream" not in event.stream_rule
        or "exactly one" not in event.subtype_rule
        or "event_count" not in event.seal_rule
        or "zero events" not in event.seal_rule
        or "reading or writing no event rows" not in event.activation_rule
        or "candidate_id and preparation_id are both candidate keys"
        not in event.candidate_binding_rule
        or "must not search" not in event.candidate_binding_rule
        or "monotonically" not in event.ack_rule
        or "bounded" not in event.ack_rule
        or "ABANDONED" not in event.cleanup_rule
    ):
        errors.append(
            "event integrity must state exact subtype, completeness seal, O(1) "
            "activation, bounded monotone ack, and cleanup rules"
        )
    if (
        event.empty_chain_sha256
        != "e3963ad6e07ac045502ad95ddb3805ac57deea8ffbb038ddf7c538a816301e71"
        or "preparation_id[16]" not in event.event_digest_codec
        or "chain_0" not in event.chain_codec
        or "chain_(n+1)" not in event.chain_codec
    ):
        errors.append("event digest and chain codecs must be exact and closed-world")
    stream = relations.get(event.stream_relation)
    preparation_event = relations.get(event.preparation_relation)
    seal = relations.get(event.seal_relation)
    activation = relations.get(event.activation_relation)
    candidate_binding = relations.get(event.candidate_binding_relation)
    base = relations.get(event.base_relation)
    removed = relations.get(event.removed_subtype_relation)
    deletion = relations.get(event.deletion_subtype_relation)
    ack_head = relations.get(event.ack_head_relation)
    if (
        stream is None
        or set(stream.attributes) != {"preparation_id", "created_at"}
        or set(stream.declared_keys) != {frozenset({"preparation_id"})}
    ):
        errors.append("event stream must be the durable preparation-keyed root")
    if preparation_event is None or not _has_fk(
        preparation_event,
        ("preparation_id",),
        event.stream_relation,
        ("preparation_id",),
    ):
        errors.append("operational preparation must reference its durable stream")
    if (
        seal is None
        or set(seal.attributes)
        != {"preparation_id", "event_count", "final_chain_sha256", "sealed_at"}
        or set(seal.declared_keys) != {frozenset({"preparation_id"})}
        or not _has_fk(
            seal,
            ("preparation_id",),
            event.stream_relation,
            ("preparation_id",),
        )
    ):
        errors.append("event effect seal must be exact durable stream authority")
    if (
        activation is None
        or set(activation.attributes)
        != {
            "source_revision",
            "preparation_id",
            "operational_policy_id",
            "activated_at",
        }
        or set(activation.declared_keys)
        != {frozenset({"source_revision"}), frozenset({"preparation_id"})}
        or not _has_fk(
            activation,
            ("preparation_id",),
            event.seal_relation,
            ("preparation_id",),
        )
    ):
        errors.append("operational activation must reference the exact effect seal")
    if (
        candidate_binding is None
        or set(candidate_binding.attributes)
        != {"candidate_id", "preparation_id", "bound_at"}
        or set(candidate_binding.declared_keys)
        != {frozenset({"candidate_id"}), frozenset({"preparation_id"})}
        or not _has_fk(
            candidate_binding,
            ("candidate_id",),
            "publication_candidate",
            ("candidate_id",),
        )
        or not _has_fk(
            candidate_binding,
            ("preparation_id",),
            event.preparation_relation,
            ("preparation_id",),
        )
        or not _has_fk(
            candidate_binding,
            ("preparation_id",),
            event.seal_relation,
            ("preparation_id",),
        )
    ):
        errors.append(
            "publication candidate preparation must be exact one-to-one sealed authority"
        )
    expected_event_keys = {
        frozenset({"event_id"}),
        frozenset({"preparation_id", "sequence_no"}),
    }
    if (
        base is None
        or set(base.attributes)
        != {
            "event_id",
            "preparation_id",
            "sequence_no",
            "event_type",
            "event_sha256",
            "created_at",
        }
        or set(base.declared_keys) != expected_event_keys
        or not _has_fk(
            base,
            ("preparation_id",),
            event.stream_relation,
            ("preparation_id",),
        )
    ):
        errors.append(
            "event base must use the preparation coordinate and durable stream FK"
        )
    for label, subtype in (("removed", removed), ("deletion", deletion)):
        if subtype is None or not _has_fk(
            subtype, ("event_id",), event.base_relation, ("event_id",)
        ):
            errors.append(f"{label} event subtype must reference event base")
    if (
        ack_head is None
        or set(ack_head.attributes)
        != {
            "consumer_id",
            "preparation_id",
            "through_sequence_no",
            "updated_at",
        }
        or set(ack_head.declared_keys) != {frozenset({"consumer_id", "preparation_id"})}
    ):
        errors.append("event ack head must be consumer/preparation high-water state")
    elif not _has_fk(
        ack_head,
        ("preparation_id", "through_sequence_no"),
        event.base_relation,
        ("preparation_id", "sequence_no"),
    ):
        errors.append("event ack head must target an event in the same preparation")

    if generation.reservation_relation != "source_build_generation":
        errors.append("build-generation reservation relation has the wrong name")
    reservation = relations.get(generation.reservation_relation)
    if (
        reservation is None
        or set(reservation.attributes)
        != {
            "build_id",
            "generation",
        }
        or set(reservation.declared_keys) != {frozenset({"generation"})}
        or set(reservation.functional_dependencies)
        != {FunctionalDependency(frozenset({"generation"}), frozenset({"build_id"}))}
    ):
        errors.append("build-generation reservation must declare generation -> build")
    if not all(
        term in generation.rule
        for term in (
            "strictly greater",
            "no row",
            "exact current ingest head",
            "matching owner",
            "unexpired lease",
            "rather than an FK",
        )
    ):
        errors.append(
            "build-generation rule must state live writer authorization, takeover, "
            "and no-build semantics"
        )
    elif reservation is not None and (
        not _has_fk(
            reservation,
            ("generation",),
            "ingest_generation",
            ("generation",),
        )
        or _has_fk(
            reservation,
            ("generation",),
            "ingest_generation_owner",
            ("generation",),
        )
    ):
        errors.append(
            "build-generation mapping must reference immutable generation history"
        )
    upload = relations.get("canonical_value_upload")
    if (
        upload is None
        or not _has_fk(
            upload,
            ("generation",),
            "ingest_generation",
            ("generation",),
        )
        or _has_fk(
            upload,
            ("generation",),
            "ingest_generation_owner",
            ("generation",),
        )
    ):
        errors.append("canonical upload must reference immutable generation history")

    if cleanup.job_relation != "cleanup_job" or cleanup.attempt_attribute != (
        "cycle_generation"
    ):
        errors.append("cleanup-attempt contract has the wrong relation or attribute")
    job = relations.get(cleanup.job_relation)
    expected_cleanup_key = frozenset({"target_key"})
    if job is None or expected_cleanup_key not in job.declared_keys:
        errors.append("cleanup job must have one reusable row per fixed shard target")
    if "monotonically" not in cleanup.allocation_rule:
        errors.append("cleanup attempt allocation must be monotone")

    expected_preparation_key = (
        "build_id",
        "deletion_request_generation",
        "operational_policy_id",
    )
    if (
        preparation.preparation_relation != "operational_preparation"
        or (preparation.policy_relation != "operational_policy")
        or preparation.deletion_generation_relation != "deletion_request_generation"
        or preparation.natural_key != expected_preparation_key
    ):
        errors.append(
            "preparation identity contract must include policy and exact deletion "
            "generation authority in its key"
        )
    preparation_relation = relations.get(preparation.preparation_relation)
    if preparation_relation is None or frozenset(expected_preparation_key) not in (
        preparation_relation.declared_keys
    ):
        errors.append("operational preparation natural key omits policy")
    elif not _has_fk(
        preparation_relation,
        ("deletion_request_generation",),
        preparation.deletion_generation_relation,
        ("generation",),
    ):
        errors.append("operational preparation lacks exact deletion generation FK")
    if not all(
        term in preparation.rule
        for term in ("policy", "FK-backed", "singleton", "publication")
    ):
        errors.append(
            "preparation identity must state policy, exact generation, and "
            "publication-head behavior"
        )

    return errors


def _validate_artifact_delta_contract(
    delta_contract: ArtifactDeltaContract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Validate the normalized old/new presence truth table for artifact work."""

    errors: list[str] = []
    if delta_contract.classification != (
        "exact_old_new_presence_and_semantic_equality"
    ):
        errors.append(
            "artifact delta classification must use exact old/new presence "
            "and semantic equality"
        )
    expected_components = (
        "source_manifest",
        "member_plan",
        "effective_content",
        "selected",
        "owner",
        "policy",
    )
    if delta_contract.semantic_components != expected_components:
        errors.append(
            "artifact delta semantic components must be exactly "
            "{source_manifest, member_plan, effective_content, selected, owner, policy}"
        )
    expected_operations = {"CREATE", "REBUILD", "DELETE", "UNCHANGED"}
    operations = set(delta_contract.operations)
    if len(operations) != len(delta_contract.operations):
        errors.append("artifact delta operations contain duplicates")
    if operations != expected_operations:
        errors.append(
            "artifact delta operations must be exactly "
            "{CREATE, DELETE, REBUILD, UNCHANGED}"
        )
    expected_old = {"REBUILD", "DELETE", "UNCHANGED"}
    expected_new = {"CREATE", "REBUILD", "UNCHANGED"}
    if set(delta_contract.old_state_operations) != expected_old:
        errors.append(
            "artifact delta old-state operations must be exactly "
            "{DELETE, REBUILD, UNCHANGED}"
        )
    if set(delta_contract.new_state_operations) != expected_new:
        errors.append(
            "artifact delta new-state operations must be exactly "
            "{CREATE, REBUILD, UNCHANGED}"
        )
    rebuild_terms = (
        "artifact_semantics_sha256 differs",
        "GID-derived artifact name is normalized",
    )
    unchanged_terms = (
        "artifact_semantics_sha256 is exactly equal",
        "artifact names are not delta state",
    )
    rename_terms = (
        "globally derived from immutable positive GID",
        "absent from artifact input, delta, operation, prepared, and catalog occurrence",
        "never an artifact rename",
    )
    if any(term not in delta_contract.rebuild_rule for term in rebuild_terms):
        errors.append("artifact delta REBUILD rule is not semantic-only")
    if any(term not in delta_contract.unchanged_rule for term in unchanged_terms):
        errors.append("artifact delta UNCHANGED rule admits repeated name state")
    if any(term not in delta_contract.rename_rule for term in rename_terms):
        errors.append("artifact delta name-normalization rule is incomplete")
    relation_requirements = {
        delta_contract.operation_relation: {
            "candidate_id",
            "publication_key",
            "operation",
        },
        delta_contract.old_state_relation: {
            "candidate_id",
            "publication_key",
        },
        delta_contract.new_state_relation: {
            "candidate_id",
            "publication_key",
        },
    }
    if len(relation_requirements) != 3:
        errors.append("artifact delta contract relations must be distinct")
    for relation_name, required_attributes in relation_requirements.items():
        relation = relation_by_name.get(relation_name)
        if relation is None:
            errors.append(
                f"artifact delta contract references unknown relation {relation_name!r}"
            )
            continue
        missing = required_attributes - set(relation.attributes)
        if missing:
            errors.append(
                f"artifact delta relation {relation_name!r} lacks "
                f"{_format_set(missing)}"
            )
    component_relation = relation_by_name.get(
        delta_contract.semantic_component_relation
    )
    if component_relation is None:
        errors.append("artifact delta semantic component relation is unknown")
    else:
        component_attributes = tuple(
            f"{component}_component_sha256" for component in expected_components
        )
        required_component_attributes = {
            "artifact_semantics_sha256",
            *component_attributes,
        }
        if set(component_relation.attributes) != required_component_attributes:
            errors.append(
                "artifact delta semantic component relation must expose the "
                "exact six canonical component columns"
            )
        expected_keys = {
            frozenset({"artifact_semantics_sha256"}),
            frozenset(component_attributes),
        }
        if set(component_relation.declared_keys) != expected_keys:
            errors.append(
                "artifact delta semantic relation must key both the semantic "
                "digest and exact six-component tuple"
            )
        expected_fds = {
            FunctionalDependency(
                frozenset({"artifact_semantics_sha256"}),
                frozenset(component_attributes),
            ),
            FunctionalDependency(
                frozenset(component_attributes),
                frozenset({"artifact_semantics_sha256"}),
            ),
        }
        if set(component_relation.functional_dependencies) != expected_fds:
            errors.append(
                "artifact delta semantic relation has the wrong collision-safe FDs"
            )
    return errors


def _validate_transition_authority_contract(
    authority: TransitionAuthorityContract | None,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Forbid audit checksums from serving as transition authority."""

    if authority is None:
        return ["data contract must declare transition_authority_contract"]
    errors: list[str] = []
    expected_relations = {
        "analysis_state_component_seal",
        "analysis_checkpoint",
        "analysis_batch_receipt",
        "publication_candidate_projection_seal",
        "publication_checkpoint",
        "publication_batch_receipt",
        "catalog_revision",
    }
    expected_forbidden = {
        "input_sha256",
        "output_sha256",
        "publication_sha256",
    }
    expected_audit = {
        "candidate_sha256",
        "decision_sha256",
        "evidence_sha256",
        "input_manifest_sha256",
        "item_sha256",
        "manifest_sha256",
    }
    if authority.version != 1:
        errors.append("transition authority contract must pin version one")
    if set(authority.gate_relations) != expected_relations or len(
        authority.gate_relations
    ) != len(expected_relations):
        errors.append("transition authority contract has the wrong gate relations")
    if set(authority.forbidden_digest_attributes) != expected_forbidden or len(
        authority.forbidden_digest_attributes
    ) != len(expected_forbidden):
        errors.append("transition authority contract has the wrong forbidden digests")
    if set(authority.audit_only_digest_attributes) != expected_audit or len(
        authority.audit_only_digest_attributes
    ) != len(expected_audit):
        errors.append("transition authority contract has the wrong audit-only digests")
    if authority.batch_key_attribute != "batch_key":
        errors.append("transition authority contract has the wrong batch-key role")

    runtime_terms = (
        "never authorize",
        "immutable snapshot",
        "server-owned cursor",
        "exact row projection",
        "compare every materialized field",
        "same fenced transaction",
        "compare-and-swap generation/checkpoint",
        "only an opaque attempt/idempotency token",
        "never semantic authority",
        "component-specific full-evaluator equality",
        "exact immutable revision child projection and count",
    )
    ready_terms = (
        "bounded",
        "writer-hook name/version presence",
        "never scans the full corpus",
        "future high-cardinality writes",
    )
    if any(term not in authority.runtime_obligation for term in runtime_terms):
        errors.append("transition authority runtime protocol is incomplete")
    if any(term not in authority.ready_obligation for term in ready_terms):
        errors.append("transition authority READY protocol is incomplete")

    for relation_name in expected_relations:
        relation = relation_by_name.get(relation_name)
        if relation is None:
            errors.append(
                f"transition authority references unknown gate {relation_name!r}"
            )
            continue
        present_forbidden = expected_forbidden & set(relation.attributes)
        if present_forbidden:
            errors.append(
                f"transition gate {relation_name!r} contains forbidden audit "
                f"digests {_format_set(present_forbidden)}"
            )
        prose_values = [relation.rationale]
        if relation.materialization is not None:
            prose_values.extend(
                value
                for value in relation.materialization.values()
                if isinstance(value, str)
            )
        for prose in prose_values:
            lowered = prose.lower()
            authority_phrases = (
                "audit digest authorizes",
                "audit digest is authority",
                "audit checksum authorizes",
                "audit checksum is authority",
                "trust the audit digest",
                "trust the audit checksum",
            )
            grants_authority = False
            for phrase in authority_phrases:
                offset = lowered.find(phrase)
                if offset < 0:
                    continue
                prefix = lowered[max(0, offset - 10) : offset]
                if prefix.endswith(("no ", "never ", "not ")):
                    continue
                grants_authority = True
                break
            if grants_authority:
                errors.append(
                    f"transition gate {relation_name!r} grants authority to an "
                    "audit digest"
                )
    return errors


def _validate_artifact_codecs(contract: Contract) -> list[str]:
    """Pin exact artifact component/envelope codecs to independent goldens."""

    errors: list[str] = []
    expected = {
        "source_manifest": (
            "source_manifest_component_sha256",
            "artifact_source_manifest_v1",
            1,
            "ascii('h2hdb-vnext-artifact-source-manifest\\0') || "
            "u32be(codec_version) || raw32(observation_identity_sha256) || "
            "u32be(manifest_algorithm_version) || u32be(file_order_version)",
            "ba10d8d66e6eae463d8a23bf1547d16de02cedecdf03b3e76e4334cb736cf964",
        ),
        "member_plan": (
            "member_plan_component_sha256",
            "artifact_member_plan_v1",
            1,
            "artifact_member_plan_contract.framing",
            "783a1b7b319bedd73edf61afa00cc9cd419ae34e9b85fe8f4c39bfae7c13f690",
        ),
        "effective_content": (
            "effective_content_component_sha256",
            "artifact_effective_content_v1",
            1,
            "ascii('h2hdb-vnext-artifact-effective-content\\0') || "
            "u32be(codec_version) || u64be(file_count) || repeated "
            "raw32(file_sha256)",
            "668d3f36923edde19a42ee69b207c8d136950b86144b1a2b5fff5995789e0144",
        ),
        "selected": (
            "selected_component_sha256",
            "artifact_selected_v1",
            1,
            "ascii('h2hdb-vnext-artifact-selected\\0') || "
            "u32be(codec_version) || raw32(publication_key) || raw32(gallery_key)",
            "daa161fdd7112e9e73c7ab3c27c94e5dd871b77fc38d2701c0e680a1a11d0281",
        ),
        "owner": (
            "owner_component_sha256",
            "artifact_owner_v1",
            1,
            "ascii('h2hdb-vnext-artifact-owner\\0') || u32be(codec_version) || "
            "raw32(content_sha256) || raw32(owner_gallery_key) || u64be(gid) || "
            "raw32(winner_gallery_key)",
            "32d8d54e00e421fd40af6c8ff6e5849dcefabe8bcfaa067dd16ba337677dd908",
        ),
        "policy": (
            "policy_component_sha256",
            "artifact_policy_v2",
            2,
            "ascii('h2hdb-vnext-artifact-policy\\0') || u32be(codec_version=2) || "
            "u32be(artifact_algorithm_version) || u32be(max_image_short_side) || "
            "raw32(producer_fingerprint_sha256)",
            "055021f55a25bb338b14aa4423b3fee9f8f87ff9ea442e4283ae89db88f47a60",
        ),
    }
    codecs = {codec.kind: codec for codec in contract.artifact_component_codecs}
    if len(codecs) != len(contract.artifact_component_codecs) or set(codecs) != set(
        expected
    ):
        return ["artifact component codec registry must contain exactly six kinds"]

    def decode_hex(value: str, context: str) -> bytes | None:
        if len(value) % 2 or any(
            character not in "0123456789abcdef" for character in value
        ):
            errors.append(f"{context} must be exact lowercase even-length hex")
            return None
        return bytes.fromhex(value)

    def canonical_digest(domain: str, payload: bytes) -> str:
        domain_bytes = domain.encode("ascii")
        preimage = b"".join(
            (
                b"h2hdb-vnext-canonical-value\0",
                (1).to_bytes(4, "big"),
                len(domain_bytes).to_bytes(4, "big"),
                domain_bytes,
                len(payload).to_bytes(8, "big"),
                payload,
            )
        )
        return hashlib.sha256(preimage).hexdigest()

    forbidden_audits = {
        "item_sha256",
        "owner_decision_sha256",
        "winner_decision_sha256",
        "build_manifest_sha256",
        "gallery_manifest_sha256",
    }
    for kind, (attribute, domain, codec_version, framing, golden) in expected.items():
        codec = codecs[kind]
        if (
            codec.attribute != attribute
            or codec.digest_domain != domain
            or codec.codec_version != codec_version
            or codec.framing != framing
            or codec.golden_sha256 != golden
            or not codec.canonical_order.strip()
        ):
            errors.append(f"artifact component codec {kind!r} drifts from registry")
        if forbidden_audits & set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", codec.framing)):
            errors.append(
                f"artifact component codec {kind!r} grants semantic authority "
                "to an audit digest"
            )
        payload = decode_hex(
            codec.golden_payload_hex,
            f"artifact component codec {kind!r} golden payload",
        )
        if payload is not None and canonical_digest(codec.digest_domain, payload) != (
            codec.golden_sha256
        ):
            errors.append(f"artifact component codec {kind!r} golden does not hash")

    semantics = contract.artifact_semantics_codec
    expected_semantics_framing = (
        "ascii('h2hdb-vnext-artifact-semantics\\0') || u32be(codec_version) || "
        "raw32(source_manifest_component_sha256) || "
        "raw32(member_plan_component_sha256) || "
        "raw32(effective_content_component_sha256) || "
        "raw32(selected_component_sha256) || raw32(owner_component_sha256) || "
        "raw32(policy_component_sha256)"
    )
    expected_semantics_sha = (
        "24e1140357d6956ded50b48db8ee90171c7eff0b1179c4cf3636cfaf3dda2047"
    )
    if semantics is None:
        errors.append("artifact semantics codec is missing")
    else:
        if (
            semantics.digest_domain != "artifact_semantics_v1"
            or semantics.codec_version != 1
            or semantics.framing != expected_semantics_framing
            or semantics.golden_sha256 != expected_semantics_sha
        ):
            errors.append("artifact semantics codec drifts from v1")
        payload = decode_hex(
            semantics.golden_payload_hex,
            "artifact semantics codec golden payload",
        )
        if (
            payload is not None
            and canonical_digest(semantics.digest_domain, payload)
            != semantics.golden_sha256
        ):
            errors.append("artifact semantics codec golden does not hash")

    comment = contract.zip_comment_contract
    expected_comment_framing = (
        "ascii('H2HDB-ZIP-COMMENT\\0') || u32be(codec_version) || "
        "raw32(source_manifest_component_sha256) || "
        "raw32(effective_content_component_sha256)"
    )
    expected_comment_sha = (
        "3acf99d73b12b308c807b543d62d43941cf8a530b0fadfc915bf735d614b59d0"
    )
    if comment is None:
        errors.append("ZIP comment codec is missing")
    else:
        if (
            comment.codec_version != 1
            or comment.framing != expected_comment_framing
            or comment.golden_payload_sha256 != expected_comment_sha
            or "exactly one composite" not in comment.write_obligation
            or "neither is claimed to equal the whole envelope"
            not in comment.write_obligation
        ):
            errors.append("ZIP comment codec drifts from v1")
        payload = decode_hex(
            comment.golden_payload_hex,
            "ZIP comment codec golden payload",
        )
        if payload is not None and hashlib.sha256(payload).hexdigest() != (
            comment.golden_payload_sha256
        ):
            errors.append("ZIP comment codec golden does not hash")
    return errors


def _validate_artifact_byte_producer_contract(
    producer: ArtifactByteProducerContract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "artifact byte-producer contract"
    expected_bundle = {
        "CBZ manifest serialization version",
        "streaming archive member naming and collision rules",
        "per-file exclusion-plan encoding",
        "ZIP and ZIP64 writer format",
        "DEFLATE compression level 9",
        "fixed DOS member timestamp, Unix mode, and UTF-8 flags",
        "eligible image suffix set",
        "EXIF transpose, LANCZOS resampling, and no-upscale rule",
        "GIF and JPEG encoding with quality 90, optimize, and alpha background",
        "exact producer fingerprint covering writer/schema plus Python, Pillow, libjpeg, and zlib versions and builds, or a certified byte-equivalence class",
    }
    if producer.policy_relation != "artifact_policy_semantics":
        errors.append(f"{prefix} must reference artifact_policy_semantics")
    if producer.algorithm_attribute != "artifact_algorithm_version":
        errors.append(f"{prefix} must version artifact_algorithm_version")
    if producer.producer_relation != "artifact_producer_fingerprint":
        errors.append(f"{prefix} must reference artifact_producer_fingerprint")
    if producer.zip_writer_policy_relation != "artifact_zip_writer_policy":
        errors.append(f"{prefix} must reference artifact_zip_writer_policy")
    if producer.storage_codec_relation != "artifact_storage_codec":
        errors.append(f"{prefix} must reference artifact_storage_codec")
    if producer.independent_parameters != (
        "max_image_short_side",
        "producer_fingerprint_sha256",
    ):
        errors.append(
            f"{prefix} independent parameters must be resize and producer fingerprint"
        )
    if set(producer.algorithm_bundle) != expected_bundle or len(
        producer.algorithm_bundle
    ) != len(expected_bundle):
        errors.append(f"{prefix} implementation bundle is incomplete")
    if (
        "max_image_short_side" not in producer.runtime_obligation
        or "policy-v2" not in producer.runtime_obligation
        or "registered exact producer fingerprint" not in producer.runtime_obligation
        or "every possible bit change" not in producer.runtime_obligation
        or "certified byte-equivalent" not in producer.runtime_obligation
        or "algorithm/resize/producer tuple change" not in producer.runtime_obligation
    ):
        errors.append(f"{prefix} runtime obligation is incomplete")
    expected_fingerprint_framing = (
        "ascii('h2hdb-vnext-artifact-producer\\0') || u32be(codec_version=1) || "
        "lp32(writer_id) || lp32(python_abi) || lp32(pillow_build) || "
        "lp32(libjpeg_build) || lp32(zlib_build)"
    )
    if producer.producer_fingerprint_framing != expected_fingerprint_framing:
        errors.append(f"{prefix} producer fingerprint framing drifted")
    try:
        producer_payload = bytes.fromhex(
            producer.producer_fingerprint_golden_payload_hex
        )
    except ValueError:
        errors.append(f"{prefix} producer fingerprint golden is not lowercase hex")
    else:
        if hashlib.sha256(producer_payload).hexdigest() != (
            producer.producer_fingerprint_golden_sha256
        ):
            errors.append(f"{prefix} producer fingerprint golden does not hash")

    policy = relation_by_name.get(producer.policy_relation)
    if policy is None:
        errors.append(f"{prefix} references an unknown policy relation")
    else:
        expected_attributes = {
            "policy_component_sha256",
            producer.algorithm_attribute,
            "max_image_short_side",
            "producer_fingerprint_sha256",
        }
        if set(policy.attributes) != expected_attributes:
            errors.append(f"{prefix} policy relation has redundant or missing columns")
        expected_keys = {
            frozenset({"policy_component_sha256"}),
            frozenset(
                {
                    producer.algorithm_attribute,
                    "max_image_short_side",
                    "producer_fingerprint_sha256",
                }
            ),
        }
        if set(policy.declared_keys) != expected_keys:
            errors.append(f"{prefix} policy relation has the wrong natural key")
        expected_dependencies = {
            FunctionalDependency(
                frozenset({"policy_component_sha256"}),
                frozenset(
                    {
                        producer.algorithm_attribute,
                        "max_image_short_side",
                        "producer_fingerprint_sha256",
                    }
                ),
            ),
            FunctionalDependency(
                frozenset(
                    {
                        producer.algorithm_attribute,
                        "max_image_short_side",
                        "producer_fingerprint_sha256",
                    }
                ),
                frozenset({"policy_component_sha256"}),
            ),
        }
        if set(policy.functional_dependencies) != expected_dependencies:
            errors.append(f"{prefix} policy relation has the wrong semantic FDs")
        if not _has_fk(
            policy,
            ("producer_fingerprint_sha256",),
            producer.producer_relation,
            ("producer_fingerprint_sha256",),
        ):
            errors.append(f"{prefix} policy relation lacks producer FK")

    producer_relation = relation_by_name.get(producer.producer_relation)
    producer_natural = frozenset(
        {
            producer.algorithm_attribute,
            "producer_equivalence_class",
            "writer_id",
            "python_abi",
            "pillow_build",
            "libjpeg_build",
            "zlib_build",
        }
    )
    if producer_relation is None:
        errors.append(f"{prefix} producer registry is missing")
    elif set(producer_relation.declared_keys) != {
        frozenset({"producer_fingerprint_sha256"}),
        producer_natural,
    } or not _has_fk(
        producer_relation,
        (producer.algorithm_attribute,),
        producer.zip_writer_policy_relation,
        (producer.algorithm_attribute,),
    ):
        errors.append(f"{prefix} producer registry keys/FK are incomplete")

    zip_policy = relation_by_name.get(producer.zip_writer_policy_relation)
    if zip_policy is None or frozenset({producer.algorithm_attribute}) not in set(
        zip_policy.declared_keys
    ):
        errors.append(f"{prefix} ZIP writer policy registry is missing its key")
    storage = relation_by_name.get(producer.storage_codec_relation)
    if storage is None or set(storage.declared_keys) != {
        frozenset({"storage_codec_version"}),
        frozenset({"adapter_id"}),
    }:
        errors.append(f"{prefix} storage codec registry keys are incomplete")
    return errors


def _validate_artifact_derived_identity_contracts(
    contract: Contract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Pin caller-independent artifact names, locations, and protection evidence."""

    errors: list[str] = []

    name = contract.artifact_name_contract
    expected_name_framing = (
        "ascii('h2h-') || canonical_positive_signed_int63_decimal_without_leading_zero "
        "|| ascii('.cbz')"
    )
    if name is None:
        errors.append("artifact name contract is missing")
    else:
        name_relation = relation_by_name.get(name.relation)
        expected_name_keys = {
            frozenset({"publication_key"}),
            frozenset({"publication_id"}),
            frozenset({name.gid_attribute}),
            frozenset({name.name_attribute}),
        }
        if (
            name.relation != "publication_identity"
            or name.gid_attribute != "gid"
            or name.name_attribute != "artifact_name"
            or name.codec_version != 1
            or name.framing != expected_name_framing
            or name.golden_gid != 7
            or name.golden_name_hex != b"h2h-7.cbz".hex()
            or "reject caller bytes" not in name.runtime_obligation
            or "positive GID" not in name.runtime_obligation
        ):
            errors.append("artifact name contract drifts from exact GID codec v1")
        if name_relation is None or set(name_relation.declared_keys) != (
            expected_name_keys
        ):
            errors.append("artifact name relation lacks four equivalent identity keys")

    locator = contract.artifact_locator_contract
    expected_components = (
        "sha256",
        "lowerhex_artifact_sha256_prefix_2",
        "lowerhex_artifact_sha256_64_plus_dot_cbz",
    )
    if locator is None:
        errors.append("artifact locator contract is missing")
    else:
        location_relation = relation_by_name.get(locator.relation)
        expected_location_keys = {
            frozenset({locator.artifact_attribute}),
            frozenset({locator.locator_attribute}),
        }
        if (
            locator.relation != "artifact_location"
            or locator.artifact_attribute != "artifact_sha256"
            or locator.locator_attribute != "artifact_locator_sha256"
            or locator.storage_codec_version != 1
            or locator.locator_codec_version != 1
            or locator.components != expected_components
            or locator.derivation
            != "artifact_locator_components(raw32 artifact_sha256) = "
            "('sha256', lowerhex[0:2], lowerhex64 + '.cbz')"
            or "reject caller components" not in locator.runtime_obligation
            or "exact-compare both candidate keys" not in locator.runtime_obligation
        ):
            errors.append("artifact locator contract drifts from content codec v1")
        if (
            location_relation is None
            or set(location_relation.attributes)
            != {locator.artifact_attribute, locator.locator_attribute}
            or set(location_relation.declared_keys) != expected_location_keys
            or not _has_fk(
                location_relation,
                (locator.artifact_attribute,),
                "artifact_blob",
                ("artifact_sha256",),
            )
            or not _has_fk(
                location_relation,
                (locator.locator_attribute,),
                "canonical_value_identity",
                ("value_sha256",),
            )
        ):
            errors.append(
                "artifact locator relation lacks exact bidirectional keys/FKs"
            )
        artifact_sha256 = _decode_exact_lower_hex(
            locator.golden_artifact_sha256,
            32,
            "artifact locator golden artifact SHA-256",
            errors,
        )
        payload = _decode_exact_lower_hex(
            locator.golden_payload_hex,
            None,
            "artifact locator golden payload",
            errors,
        )
        if artifact_sha256 is not None and payload is not None:
            lowerhex = artifact_sha256.hex().encode("ascii")
            segments = (b"sha256", lowerhex[:2], lowerhex + b".cbz")
            expected_payload = b"".join(
                (
                    (1).to_bytes(4, "big"),
                    len(segments).to_bytes(4, "big"),
                    *(
                        len(segment).to_bytes(4, "big") + segment
                        for segment in segments
                    ),
                )
            )
            if payload != expected_payload:
                errors.append("artifact locator golden payload is not SHA-derived")
            if _canonical_value_sha256("artifact_locator_bytes_v1", payload) != (
                locator.golden_locator_sha256
            ):
                errors.append("artifact locator golden canonical digest does not hash")

    protection = contract.artifact_protection_token_contract
    expected_receipt_framing = (
        "SHA256(ascii('h2hdb-vnext-artifact-storage-receipt\\0') || "
        "raw16(candidate_id) || raw32(publication_key) || raw32(artifact_sha256) || "
        "raw32(artifact_locator_sha256) || u64be(storage_generation) || "
        "u64be(size_bytes))[0:16]"
    )
    expected_token_framing = (
        "ascii('h2hdb-vnext-artifact-protection\\0') || u32be(codec_version=1) || "
        "u32be(storage_codec_version) || raw16(candidate_id) || "
        "raw32(publication_key) || raw32(artifact_sha256) || "
        "raw32(artifact_locator_sha256) || raw16(receipt_id) || "
        "u64be(storage_generation) || u64be(size_bytes)"
    )
    if protection is None:
        errors.append("artifact protection-token contract is missing")
    else:
        prepared = relation_by_name.get(protection.relation)
        storage = relation_by_name.get(protection.storage_codec_relation)
        if (
            protection.relation != "prepared_artifact"
            or protection.storage_codec_relation != "artifact_storage_codec"
            or protection.codec_version != 1
            or protection.exact_bytes != 184
            or protection.receipt_framing != expected_receipt_framing
            or protection.token_framing != expected_token_framing
            or "no caller receipt or token authority"
            not in protection.runtime_obligation
            or "decode exact EOF" not in protection.runtime_obligation
            or "compare every field on replay" not in protection.runtime_obligation
        ):
            errors.append("artifact protection-token contract drifts from codec v1")
        if (
            prepared is None
            or "protection_token" not in prepared.attributes
            or frozenset({"protection_token"}) not in prepared.declared_keys
            or not _has_fk(
                prepared,
                ("storage_codec_version",),
                protection.storage_codec_relation,
                ("storage_codec_version",),
            )
            or storage is None
        ):
            errors.append("prepared artifact lacks closed protection-token identity")
        token = _decode_exact_lower_hex(
            protection.golden_token_hex,
            protection.exact_bytes,
            "artifact protection-token golden",
            errors,
        )
        receipt = _decode_exact_lower_hex(
            protection.golden_receipt_id,
            16,
            "artifact storage-receipt golden",
            errors,
        )
        if token is not None and receipt is not None:
            candidate = bytes.fromhex("11" * 16)
            publication = bytes.fromhex("22" * 32)
            artifact = bytes.fromhex("33" * 32)
            artifact_locator = bytes.fromhex("44" * 32)
            generation = (7).to_bytes(8, "big")
            size = (9).to_bytes(8, "big")
            expected_receipt = hashlib.sha256(
                b"h2hdb-vnext-artifact-storage-receipt\0"
                + candidate
                + publication
                + artifact
                + artifact_locator
                + generation
                + size
            ).digest()[:16]
            expected_token = (
                b"h2hdb-vnext-artifact-protection\0"
                + (1).to_bytes(4, "big")
                + (1).to_bytes(4, "big")
                + candidate
                + publication
                + artifact
                + artifact_locator
                + expected_receipt
                + generation
                + size
            )
            if receipt != expected_receipt:
                errors.append("artifact storage-receipt golden does not hash")
            if token != expected_token or len(token) != protection.exact_bytes:
                errors.append("artifact protection-token golden does not encode")

    return errors


def _decode_exact_lower_hex(
    value: str,
    exact_bytes: int | None,
    context: str,
    errors: list[str],
) -> bytes | None:
    if (
        len(value) % 2
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        errors.append(f"{context} must be exact lowercase even-length hex")
        return None
    decoded = bytes.fromhex(value)
    if exact_bytes is not None and len(decoded) != exact_bytes:
        errors.append(f"{context} must be exactly {exact_bytes} bytes")
        return None
    return decoded


def _validate_artifact_member_plan_contract(
    plan: ArtifactMemberPlanContract,
    enum: ArtifactMemberPlanEnum | None,
    delta: ArtifactDeltaContract | None,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "artifact member-plan contract"
    expected = {
        "semantic_relation": "artifact_semantic_input",
        "component_attribute": "member_plan_component_sha256",
        "component_kind": "member_plan",
        "canonical_value_relation": "canonical_value_identity",
        "canonical_digest_attribute": "value_sha256",
        "plan_version": 1,
    }
    for field, value in expected.items():
        if getattr(plan, field) != value:
            errors.append(f"{prefix} {field} must be {value!r}")
    expected_fields = (
        "entry_position",
        "entry_kind_source_file",
        "exact_source_name_bytes",
        "source_file_sha256",
        "source_size_bytes",
        "source_role",
        "excluded_flag",
        "optional_exact_archive_member_name_bytes_absent_when_excluded",
        "transform_kind",
    )
    if plan.entry_fields != expected_fields:
        errors.append(f"{prefix} entry fields are incomplete or out of order")
    runtime_terms = (
        "every source entry",
        "including excluded entries",
        "no generated archive member",
        "ZIP comment is not a member",
        "entry_kind is SOURCE_FILE=0",
        "source_role is METADATA=0 or CONTENT=1",
        "excluded entries have presence zero",
        "source_file_sha256 is collision-free payload identity",
        "bytes are rehashed and compared during prepare",
        "reject every unknown tag",
        "bounded preflight/spool receipt",
        "payload_byte_count",
        "iter_artifact_member_plan_payload",
        "artifact_member_plan_digest_ordered",
        "iter_artifact_effective_content_payload_ordered",
        "artifact_effective_content_digest_ordered",
        "canonical_value_digest_parts",
        "never materialize either full high-cardinality payload",
        "byte-for-byte equality on collision",
    )
    ready_terms = (
        "ordered source snapshot",
        "source sizes and rehashed payload identities",
        "resolved per-file exclusion decisions",
        "ZIP comment envelope",
        "effective_content digest alone is never equality authority",
    )
    if any(term not in plan.runtime_obligation for term in runtime_terms):
        errors.append(f"{prefix} runtime construction obligation is incomplete")
    if any(term not in plan.ready_obligation for term in ready_terms):
        errors.append(f"{prefix} READY validation obligation is incomplete")
    if enum is None:
        errors.append(f"{prefix} must declare a closed enum registry")
    else:
        expected_enum_maps = {
            "entry_kind": {"SOURCE_FILE": 0},
            "source_role": {"METADATA": 0, "CONTENT": 1},
            "transform_kind": {
                "RAW_COPY": 0,
                "GIF_NORMALIZE": 1,
                "JPEG_NORMALIZE": 2,
            },
            "boolean_tags": {"FALSE": 0, "TRUE": 1},
        }
        for field, expected_map in expected_enum_maps.items():
            if dict(getattr(enum, field)) != expected_map:
                errors.append(
                    f"{prefix} {field} must be the exact version-one tag registry"
                )
        enum_terms = (
            (enum.position_rule, ("zero-based", "without gaps")),
            (
                enum.source_rule,
                (
                    "every v1 entry is a source file",
                    "no generated identity",
                    "raw32 source_file_sha256",
                ),
            ),
            (
                enum.transform_rule,
                (".gif", ".avif/.bmp/.jpeg/.jpg/.png/.webp", "RAW_COPY"),
            ),
        )
        if any(any(term not in rule for term in terms) for rule, terms in enum_terms):
            errors.append(f"{prefix} enum derivation rules are incomplete")
    if delta is None or plan.component_kind not in delta.semantic_components:
        errors.append(f"{prefix} kind is not required by artifact semantic equality")
    elif delta.semantic_component_relation != plan.semantic_relation:
        errors.append(f"{prefix} does not use the artifact delta component relation")
    framing_terms = (
        "h2hdb-vnext-artifact-member-plan",
        "entry_position",
        "entry_kind",
        "source_name_length",
        "payload_sha256",
        "payload_size",
        "source_role",
        "excluded_flag",
        "archive_name_presence",
        "transform_kind",
    )
    if any(term not in plan.framing for term in framing_terms):
        errors.append(f"{prefix} framing is not an exact typed entry codec")
    relation = relation_by_name.get(plan.semantic_relation)
    if relation is None:
        errors.append(f"{prefix} references unknown semantic relation")
    elif plan.component_attribute not in relation.attributes or not _has_fk(
        relation,
        (plan.component_attribute,),
        plan.canonical_value_relation,
        (plan.canonical_digest_attribute,),
    ):
        errors.append(
            f"{prefix} semantic relation does not reference canonical member-plan bytes"
        )
    return errors


def _validate_analysis_resolution_contract(
    resolution: AnalysisResolutionContract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    """Validate bounded immutable overlay resolution and compaction."""

    errors: list[str] = []
    prefix = "analysis resolution contract"
    expected = {
        "mode": "bounded_shadow_tombstone_overlay",
        "initialization": "depth_zero_full_shadow",
        "snapshot_resolution": "analysis_run_build_join",
        "delta_basis": "exact_old_new_build_membership",
        "read_resolution": "nearest_ancestor_wins",
        "compaction": "depth_zero_full_shadow_at_limit_or_policy_change",
        "compaction_ancestry": "self_only",
        "cleanup_guard": "no_reachable_descendants",
        "cleanup_transition": "remove_seal_then_ancestry_then_state",
    }
    for field, value in expected.items():
        actual = getattr(resolution, field)
        if actual != value:
            errors.append(f"{prefix} {field} must be {value!r}, got {actual!r}")
    if resolution.max_overlay_depth <= 0:
        errors.append(f"{prefix} max_overlay_depth must be positive")

    expected_batch_stages = (
        ("changed_gallery", 1, "analysis_gallery_v1"),
        ("changed_file_hash", 2, "analysis_digest_v1"),
        ("file_hash_decision", 3, "analysis_digest_v1"),
        ("validate_file_hash_decision", 4, "analysis_digest_live_v1"),
        ("impacted_gallery", 5, "analysis_gallery_v1"),
        ("impacted_content", 6, "analysis_gallery_v1"),
        ("content_owner_candidate", 7, "analysis_gallery_v1"),
        (
            "validate_content_owner_candidate",
            8,
            "analysis_gallery_live_v1",
        ),
        ("content_owner", 9, "analysis_digest_v1"),
        ("validate_content_owner", 10, "analysis_digest_live_v1"),
        ("impacted_gid", 11, "analysis_gallery_v1"),
        ("gid_candidate", 12, "analysis_gallery_v1"),
        ("validate_gid_candidate", 13, "analysis_gallery_live_v1"),
        ("gid_winner", 14, "analysis_gid_v1"),
        ("validate_gid_winner", 15, "analysis_gid_live_v1"),
    )
    if (
        resolution.stage_relation != "analysis_stage"
        or resolution.checkpoint_relation != "analysis_checkpoint"
        or resolution.batch_receipt_relation != "analysis_batch_receipt"
        or tuple(
            (stage.name, stage.stage_order, stage.cursor_codec)
            for stage in resolution.batch_stages
        )
        != expected_batch_stages
    ):
        errors.append(f"{prefix} batch stage/order/cursor registry drifts")
    cursor_terms = (
        "analysis_gallery_v1 is u8(version=1)",
        "analysis_digest_v1 is u8(version=1)",
        "analysis_gid_v1 is u8(version=1)",
        "analysis_gallery_live_v1",
        "analysis_digest_live_v1",
        "analysis_gid_live_v1",
        "u64be(live_row_count_int63)",
        "all-zero absent-key genesis",
        "exact EOF",
        "digest keysets compare unsigned bytes",
        "stage-selected codec before every query or CAS",
    )
    if any(term not in resolution.cursor_codec_rule for term in cursor_terms):
        errors.append(f"{prefix} cursor codec framing is not closed-world")
    batch_rule_terms = (
        "all fifteen registered stages in exact stage_order",
        "hard-capped batch",
        "server-side",
        "same transaction",
        "committed_generation is exactly start_generation plus one",
        "next_processed_count is exactly start_processed_count plus row_count",
        "full stored tuple without writes",
        "terminal is one if and only if the derived page is empty",
        "next_state is COMPLETE",
        "nonterminal receipt has positive row_count",
        "caller stage, cursor, count, terminal, state, digest, sequence, or receipt bytes never authorize mutation",
    )
    if any(term not in resolution.batch_rule for term in batch_rule_terms):
        errors.append(f"{prefix} batch receipt/CAS rule is incomplete")

    stage_relation = relation_by_name.get(resolution.stage_relation)
    if (
        stage_relation is None
        or set(stage_relation.attributes) != {"stage", "stage_order", "cursor_codec"}
        or set(stage_relation.declared_keys)
        != {frozenset({"stage"}), frozenset({"stage_order"})}
    ):
        errors.append(f"{prefix} stage registry lacks its exact BCNF shape")
    checkpoint = relation_by_name.get(resolution.checkpoint_relation)
    if (
        checkpoint is None
        or set(checkpoint.attributes)
        != {
            "analysis_id",
            "stage",
            "generation",
            "cursor",
            "processed_count",
            "state",
            "updated_at",
        }
        or set(checkpoint.declared_keys) != {frozenset({"analysis_id", "stage"})}
        or not _has_fk(
            checkpoint,
            ("stage",),
            resolution.stage_relation,
            ("stage",),
        )
    ):
        errors.append(f"{prefix} checkpoint lacks exact typed stage progress authority")
    receipt = relation_by_name.get(resolution.batch_receipt_relation)
    if (
        receipt is None
        or set(receipt.attributes)
        != {
            "analysis_id",
            "stage",
            "batch_key",
            "start_generation",
            "start_cursor",
            "start_processed_count",
            "next_cursor",
            "next_processed_count",
            "next_state",
            "row_count",
            "terminal",
            "committed_generation",
            "committed_at",
        }
        or set(receipt.declared_keys)
        != {
            frozenset({"analysis_id", "stage", "batch_key"}),
            frozenset({"analysis_id", "stage", "start_generation"}),
        }
        or not _has_fk(
            receipt,
            ("analysis_id", "stage"),
            resolution.checkpoint_relation,
            ("analysis_id", "stage"),
        )
    ):
        errors.append(f"{prefix} receipt lacks exact pre/post replay authority")

    baseline = relation_by_name.get(resolution.baseline_relation)
    if baseline is None or not {
        "analysis_id",
        "base_analysis_id",
    } <= set(baseline.attributes if baseline else ()):
        errors.append(f"{prefix} baseline relation must pin analysis and base IDs")

    anchor = relation_by_name.get(resolution.anchor_relation)
    if anchor is None or not {
        "analysis_id",
        "anchor_analysis_id",
        "overlay_depth",
    } <= set(anchor.attributes if anchor else ()):
        errors.append(f"{prefix} anchor relation lacks bounded-chain coordinates")
    else:
        anchor_metadata = anchor.materialization or {}
        if anchor_metadata.get("max_overlay_depth") != resolution.max_overlay_depth:
            errors.append(
                f"{prefix} anchor relation does not enforce max overlay depth"
            )
        if anchor_metadata.get("root_rule") != "depth_zero_self_anchor":
            errors.append(f"{prefix} anchor relation does not enforce a self-only root")
        if anchor_metadata.get("policy_rule") != (
            "same_policy_or_depth_zero_compaction"
        ):
            errors.append(f"{prefix} anchor relation permits cross-policy inheritance")
        if set(anchor_metadata.get("derived_from", ())) != {
            "analysis_run",
            "analysis_baseline",
        }:
            errors.append(f"{prefix} anchor relation has a cyclic dependency graph")
        if not all(
            token in anchor_metadata.get("parent_anchor_precondition", "")
            for token in ("base_analysis_id", "parent", "never read this run")
        ) or not all(
            token in anchor_metadata.get("sealed_parent_precondition", "")
            for token in ("five", "base_analysis_id", "before")
        ):
            errors.append(f"{prefix} anchor lacks exact sealed-parent preconditions")

    ancestry = relation_by_name.get(resolution.ancestry_relation)
    if ancestry is None or not {
        "analysis_id",
        "ancestor_analysis_id",
        "ancestor_depth",
    } <= set(ancestry.attributes if ancestry else ()):
        errors.append(f"{prefix} ancestry relation lacks depth-addressed ancestors")
    else:
        ancestry_metadata = ancestry.materialization or {}
        if ancestry_metadata.get("max_overlay_depth") != resolution.max_overlay_depth:
            errors.append(
                f"{prefix} ancestry relation does not enforce max overlay depth"
            )
        if ancestry_metadata.get("ancestry_invariant") != ("acyclic_depth_contiguous"):
            errors.append(f"{prefix} ancestry relation is not acyclic and contiguous")
        if ancestry_metadata.get("policy_rule") != (
            "same_policy_or_depth_zero_compaction"
        ):
            errors.append(
                f"{prefix} ancestry relation permits cross-policy inheritance"
            )
        if set(ancestry_metadata.get("derived_from", ())) != {
            "analysis_baseline",
            "analysis_state_anchor",
        }:
            errors.append(f"{prefix} ancestry relation has a cyclic dependency graph")
        if not all(
            token in ancestry_metadata.get("parent_ancestry_precondition", "")
            for token in ("base_analysis_id", "sealed parent", "never read this run")
        ):
            errors.append(f"{prefix} ancestry lacks an exact parent-only precondition")

    immutable_names = resolution.immutable_fact_relations
    delta_names = resolution.delta_relations
    for label, names in (
        ("immutable_fact_relations", immutable_names),
        ("delta_relations", delta_names),
    ):
        if not names:
            errors.append(f"{prefix} {label} must not be empty")
        if len(names) != len(set(names)):
            errors.append(f"{prefix} {label} contains duplicates")
        for relation_name in names:
            if relation_name not in relation_by_name:
                errors.append(f"{prefix} references unknown relation {relation_name!r}")

    component_names = [component.name for component in resolution.components]
    required_components = {
        "file_hash_decision",
        "content_owner_candidate",
        "content_owner",
        "gid_candidate",
        "gid_winner",
    }
    if not component_names:
        errors.append(f"{prefix} must declare state components")
    if len(component_names) != len(set(component_names)):
        errors.append(f"{prefix} component names contain duplicates")
    if set(component_names) != required_components:
        errors.append(
            f"{prefix} must overlay every global evaluator component; expected "
            f"{_format_set(required_components)}"
        )
    triples = [
        (
            component.shadow_relation,
            component.tombstone_relation,
            component.resolved_relation,
        )
        for component in resolution.components
    ]
    flat_relations = [name for triple in triples for name in triple]
    if len(flat_relations) != len(set(flat_relations)):
        errors.append(f"{prefix} component relation roles must be globally distinct")

    resolved_names = {
        component.resolved_relation for component in resolution.components
    }
    required_roles = {
        resolution.spam_relation,
        resolution.content_owner_relation,
        resolution.gid_winner_relation,
    }
    if not required_roles <= resolved_names:
        errors.append(f"{prefix} query roles must reference resolved relations")
    if resolution.snapshot_relation != "source_build_gallery":
        errors.append(f"{prefix} snapshot must be the immutable build membership")
    if resolution.snapshot_relation not in relation_by_name:
        errors.append(f"{prefix} references unknown snapshot relation")
    for forbidden_copy in ("analysis_gallery_snapshot", "analysis_comparison_gallery"):
        if forbidden_copy in relation_by_name:
            errors.append(
                f"{prefix} forbids per-analysis corpus copy {forbidden_copy!r}"
            )

    for component in resolution.components:
        shadow = relation_by_name.get(component.shadow_relation)
        tombstone = relation_by_name.get(component.tombstone_relation)
        resolved = relation_by_name.get(component.resolved_relation)
        if shadow is None or tombstone is None or resolved is None:
            errors.append(
                f"{prefix} component {component.name!r} references unknown relations"
            )
            continue
        for label, relation in (
            ("shadow", shadow),
            ("tombstone", tombstone),
            ("resolved", resolved),
        ):
            metadata = relation.materialization or {}
            if metadata.get("overlay_role") != label:
                errors.append(
                    f"{prefix} component {component.name!r} {label} relation "
                    f"does not declare overlay_role={label!r}"
                )
            if metadata.get("state_component") != component.name:
                errors.append(
                    f"{prefix} component {component.name!r} {label} relation "
                    "has the wrong state_component"
                )
            if "analysis_id" not in relation.attributes:
                errors.append(
                    f"{prefix} component {component.name!r} {label} relation "
                    "lacks analysis_id"
                )
            if any("analysis_id" not in key for key in relation.declared_keys):
                errors.append(
                    f"{prefix} component {component.name!r} {label} relation "
                    "has a key not scoped by analysis_id"
                )
        if len(tombstone.declared_keys) != 1:
            errors.append(
                f"{prefix} component {component.name!r} overlay relations must "
                "declare one canonical tombstone key"
            )
            continue
        tombstone_key = set(tombstone.declared_keys[0])
        shadow_keys = {frozenset(key) for key in shadow.declared_keys}
        resolved_keys = {frozenset(key) for key in resolved.declared_keys}
        if frozenset(tombstone_key) not in shadow_keys or shadow_keys != resolved_keys:
            errors.append(
                f"{prefix} component {component.name!r} tombstone must contain "
                "exactly a shadow business key and resolved keys must match shadow"
            )
        if set(tombstone.attributes) != tombstone_key:
            errors.append(
                f"{prefix} component {component.name!r} tombstone has value attributes"
            )
        required_resolved_attributes = set(shadow.attributes)
        if set(resolved.attributes) != required_resolved_attributes:
            errors.append(
                f"{prefix} component {component.name!r} resolved relation must "
                "contain exactly the shadow value; provenance remains internal "
                "to avoid a hidden non-superkey FD"
            )
        shadow_metadata = shadow.materialization or {}
        if shadow_metadata.get("mutually_exclusive_with") != (
            component.tombstone_relation
        ):
            errors.append(
                f"{prefix} component {component.name!r} shadow does not require "
                "shadow/tombstone mutual exclusion"
            )
        if shadow_metadata.get("exactness") != "changed_keys_or_full_compaction":
            errors.append(f"{prefix} component {component.name!r} shadow is not exact")
        tombstone_metadata = tombstone.materialization or {}
        if tombstone_metadata.get("mutually_exclusive_with") != (
            component.shadow_relation
        ):
            errors.append(
                f"{prefix} component {component.name!r} does not require "
                "shadow/tombstone mutual exclusion"
            )
        if tombstone_metadata.get("exactness") != "changed_keys_or_full_compaction":
            errors.append(
                f"{prefix} component {component.name!r} tombstone is not exact"
            )
        resolved_metadata = resolved.materialization or {}
        if resolved_metadata.get("resolution") != "minimum_ancestor_depth":
            errors.append(
                f"{prefix} component {component.name!r} does not resolve the "
                "nearest ancestor"
            )
        if resolved_metadata.get("max_overlay_depth") != resolution.max_overlay_depth:
            errors.append(
                f"{prefix} component {component.name!r} resolved relation does "
                "not pin the maximum depth"
            )
        if resolved_metadata.get("storage") != "logical_view":
            errors.append(
                f"{prefix} component {component.name!r} resolved state must not "
                "copy the complete corpus"
            )
        required_sources = {
            resolution.ancestry_relation,
            component.shadow_relation,
            component.tombstone_relation,
        }
        if set(resolved_metadata.get("derived_from", ())) != required_sources:
            errors.append(
                f"{prefix} component {component.name!r} resolved relation has "
                "an incomplete nearest-ancestor source set"
            )

    seal = relation_by_name.get(resolution.seal_relation)
    if seal is None:
        errors.append(f"{prefix} references unknown seal relation")
    else:
        metadata = seal.materialization or {}
        if metadata.get("completion_gate") != "all_configured_components":
            errors.append(f"{prefix} seal does not gate all components")
        if metadata.get("policy_compatibility") != (
            "same_policy_or_depth_zero_compaction"
        ):
            errors.append(f"{prefix} seal does not enforce policy compatibility")
        if metadata.get("ancestry_invariant") != "acyclic_depth_contiguous":
            errors.append(f"{prefix} seal does not enforce acyclic ancestry")
        exact_seal_rules = {
            "delta_completeness": "exact_old_new_snapshot",
            "compaction_validation": "full_evaluator_equality",
            "cleanup_guard": "no_reachable_descendants",
            "cleanup_transition": "remove_seal_then_ancestry_then_state",
        }
        for field, value in exact_seal_rules.items():
            if metadata.get(field) != value:
                errors.append(f"{prefix} seal {field} must be {value!r}")

    return errors


_SEMANTIC_CLASSIFICATIONS = frozenset(
    {
        "aggregate_count",
        "audit_digest",
        "canonical_identity_digest",
        "comparator_key",
        "domain_identifier",
        "domain_discriminator",
        "fencing_counter",
        "idempotency_key",
        "locator",
        "natural_key",
        "observational_digest",
        "ordering_key",
        "ordinal",
        "payload_digest",
        "payload_reference_digest",
        "surrogate_identifier",
    }
)


def _validate_byte_domains(
    contract: Contract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    used_attributes = {
        attribute
        for relation in relation_by_name.values()
        for attribute in relation.attributes
    }
    domains: dict[str, ByteDomain] = {}
    for domain in contract.byte_domains:
        prefix = f"byte domain {domain.attribute!r}"
        if domain.attribute in domains:
            errors.append(f"duplicate {prefix}")
            continue
        domains[domain.attribute] = domain
        if domain.attribute not in used_attributes:
            errors.append(f"{prefix} is not used by any relation")
        if domain.maximum_bytes <= 0 or domain.maximum_bytes > 3072:
            errors.append(f"{prefix} maximum_bytes must be in 1..3072")
        if domain.encoding not in {"ascii", "utf8", "opaque_bytes"}:
            errors.append(f"{prefix} has unsupported encoding {domain.encoding!r}")
        if not domain.source.strip():
            errors.append(f"{prefix} must identify its filesystem/protocol source")
        if not domain.runtime_obligation.strip():
            errors.append(f"{prefix} must state its runtime validation obligation")

    required = (
        {
            "source_gallery_name": 255,
            "name_bytes": 255,
            "artifact_name": 255,
            "publication_id": 64,
            "artifact_id": 128,
            "namespace": 128,
            "metadata_fingerprint": 40,
            "cursor": 2048,
            "protection_token": 184,
            "adapter_id": 64,
            "producer_equivalence_class": 128,
            "writer_id": 128,
            "python_abi": 128,
            "pillow_build": 128,
            "libjpeg_build": 128,
            "zlib_build": 128,
        }
        if contract.scope == "catalog_data_plane"
        else {}
    )
    for attribute, maximum in required.items():
        required_domain = domains.get(attribute)
        if required_domain is None:
            errors.append(f"byte domain registry does not cover {attribute!r}")
        elif required_domain.maximum_bytes != maximum:
            errors.append(f"byte domain {attribute!r} maximum_bytes must be {maximum}")
    exact_sources = {
        "metadata_fingerprint": "filesystem_stat_fingerprint_v1",
        "cursor": "server_owned_checkpoint_cursor_registry_v1",
        "protection_token": "artifact_projection_protection_token_v1",
    }
    for attribute, source in exact_sources.items():
        registered_domain = domains.get(attribute)
        if registered_domain is not None and registered_domain.source != source:
            errors.append(f"byte domain {attribute!r} source must be {source!r}")
    fingerprint = domains.get("metadata_fingerprint")
    if fingerprint is not None and not all(
        term in fingerprint.runtime_obligation
        for term in ("exactly", "raw8(device_u64)", "40 bytes", "audit-only")
    ):
        errors.append("byte domain 'metadata_fingerprint' lacks its exact codec")
    return errors


def _validate_canonical_digest_contract(
    digest: CanonicalDigestContract,
    pages: CanonicalValuePageContract | None,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "canonical digest contract"
    expected = {
        "algorithm": "SHA-256",
        "algorithm_version": 1,
        "encoding": "opaque_bytes",
        "framing": (
            "ascii('h2hdb-vnext-canonical-value\\0') || u32be(codec_version) || "
            "u32be(digest_domain_length) || digest_domain_ascii || "
            "u64be(payload_length) || payload"
        ),
        "collision_model": "collision_free_identity_domain",
    }
    for field, value in expected.items():
        if getattr(digest, field) != value:
            errors.append(f"{prefix} {field} must be {value!r}")
    if not digest.write_obligation.strip() or not digest.read_obligation.strip():
        errors.append(f"{prefix} must state non-empty read and write obligations")
    for term in (
        "pass one",
        "pass two",
        "bounded transactions",
        "insert canonical_value_identity last",
    ):
        if term not in digest.write_obligation:
            errors.append(f"{prefix} write obligation omits {term!r}")
    for term in ("stream", "recompute", "cross-owner", "noncanonical"):
        if term not in digest.read_obligation:
            errors.append(f"{prefix} read obligation omits {term!r}")
    policy = relation_by_name.get(digest.policy_relation)
    if policy is None:
        errors.append(f"{prefix} references unknown policy relation")
    elif set(policy.attributes) != {"digest_domain"} or set(policy.declared_keys) != {
        frozenset({"digest_domain"})
    }:
        errors.append(f"{prefix} policy relation must be the unary domain registry")
    expected_shapes = {
        digest.allocation_relation: (
            {"value_sha256", "digest_domain", "byte_count", "allocated_at"},
            {frozenset({"value_sha256"})},
        ),
        digest.page_relation: (
            {"page_sha256", "value_sha256", "page_bytes"},
            {frozenset({"page_sha256"}), frozenset({"page_bytes"})},
        ),
        digest.descriptor_relation: (
            {
                "page_sha256",
                "value_sha256",
                "level",
                "page_position",
                "subtree_item_count",
            },
            {
                frozenset({"page_sha256"}),
                frozenset({"value_sha256", "level", "page_position"}),
            },
        ),
        digest.parent_relation: (
            {"child_sha256", "parent_sha256", "position"},
            {frozenset({"child_sha256"}), frozenset({"parent_sha256", "position"})},
        ),
        digest.value_relation: (
            {"value_sha256", "root_page_sha256"},
            {frozenset({"value_sha256"}), frozenset({"root_page_sha256"})},
        ),
    }
    for name, (attributes, keys) in expected_shapes.items():
        relation = relation_by_name.get(name)
        if relation is None:
            errors.append(f"{prefix} references missing relation {name!r}")
        elif (
            set(relation.attributes) != attributes
            or set(relation.declared_keys) != keys
        ):
            errors.append(f"{prefix} relation {name!r} has the wrong normalized shape")
    if pages is None:
        errors.append(f"{prefix} must declare canonical_value_page_contract")
    else:
        if (
            pages.codec_version != 1
            or pages.prefix != "h2hdb-vnext-canonical-value-page\\0"
            or pages.maximum_page_bytes != 65536
            or pages.chunk_maximum_bytes != 32768
            or pages.branch_capacity != 256
            or pages.maximum_level != 8
            or pages.maximum_byte_count != (1 << 63) - 1
        ):
            errors.append(f"{prefix} owner-scoped page constants drift")
        terms = " ".join(
            (
                pages.framing,
                pages.leaf_record,
                pages.branch_record,
                pages.canonical_tree_rule,
                pages.collision_obligation,
                pages.seal_obligation,
                pages.cleanup_rule,
            )
        )
        for term in (
            "owner_value_sha256",
            "32768",
            "zero-based contiguous",
            "no unary root",
            "byte-for-byte",
            "inserted last",
            "child-first",
        ):
            if term not in terms:
                errors.append(f"{prefix} page graph omits {term!r}")
        for term in (
            "source_root_v1 is the sole pre-mapping exception",
            "every other digest domain requires",
            "shared canonical-value maintenance gate",
            "cleanup cycle holds its exclusive form",
            "final identity never releases its upload claim by itself",
            "retention-blocking external consumer and deletion of only that generation claim commit atomically",
            "phase-owned dictionary or type row alone never releases the claim",
            "completed or strictly superseded",
            "pre-mapping claim whose generation has no source_build_generation row",
        ):
            if term not in pages.cleanup_rule:
                errors.append(f"{prefix} cleanup rule omits {term!r}")
    return errors


def _validate_source_locator_contract(
    locator: SourceLocatorContract,
    byte_domains: Sequence[ByteDomain],
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "source locator contract"
    expected = {
        "identity_relation": "source_locator_identity",
        "gallery_relation": "gallery_identity",
        "digest_attribute": "locator_sha256",
        "name_attribute": "source_gallery_name",
        "canonical_value_relation": "canonical_value_identity",
        "canonical_digest_attribute": "value_sha256",
        "digest_domain": "source_relative_locator_v1",
        "codec_version": 1,
        "encoding": "exact_utf8_segments",
        "framing": (
            "u32be(codec_version) || u32be(segment_count) || "
            "repeated(u32be(segment_length) || segment_utf8)"
        ),
    }
    for field, value in expected.items():
        if getattr(locator, field) != value:
            errors.append(f"{prefix} {field} must be {value!r}")
    required_write = (
        "arbitrary-total-length",
        "iter_source_relative_locator_payload",
        "canonical_value_digest_parts",
        "without materializing",
        "byte-compare",
        "final segment",
    )
    required_read = (
        "canonical_value_identity",
        "validate_source_relative_locator_parts",
        "exact EOF",
        "without exposing a component",
        "provisional bounded consumer",
        "final-segment",
    )
    if any(value not in locator.write_obligation for value in required_write) or any(
        value not in locator.read_obligation for value in required_read
    ):
        errors.append(f"{prefix} must state non-empty read and write obligations")

    relation = relation_by_name.get(locator.identity_relation)
    if relation is None:
        errors.append(f"{prefix} references unknown identity relation")
    else:
        expected_attributes = {
            locator.digest_attribute,
            locator.name_attribute,
        }
        if set(relation.attributes) != expected_attributes:
            errors.append(f"{prefix} identity relation has the wrong attributes")
        expected_keys = {frozenset({locator.digest_attribute})}
        if set(relation.declared_keys) != expected_keys:
            errors.append(
                f"{prefix} identity relation must key the full locator digest"
            )
        expected_dependencies = {
            FunctionalDependency(
                frozenset({locator.digest_attribute}),
                frozenset({locator.name_attribute}),
            )
        }
        if set(relation.functional_dependencies) != expected_dependencies:
            errors.append(f"{prefix} identity relation has the wrong typed-locator FD")
        if not _has_fk(
            relation,
            (locator.digest_attribute,),
            locator.canonical_value_relation,
            (locator.canonical_digest_attribute,),
        ):
            errors.append(f"{prefix} identity relation lacks canonical payload FK")

    gallery = relation_by_name.get(locator.gallery_relation)
    if gallery is None or set(gallery.declared_keys) != {
        frozenset({"gallery_id"}),
        frozenset({"gallery_key"}),
        frozenset({"scope_key", locator.digest_attribute}),
    }:
        errors.append(f"{prefix} gallery identity must equal scope plus full locator")

    name_domain = next(
        (item for item in byte_domains if item.attribute == locator.name_attribute),
        None,
    )
    if (
        name_domain is None
        or name_domain.maximum_bytes != 255
        or name_domain.encoding != "utf8"
        or name_domain.source != "source_relative_locator_final_segment_v1"
    ):
        errors.append(f"{prefix} requires an exact 255-byte UTF-8 final-segment domain")
    return errors


_ANALYSIS_CANDIDATE_PRIORITY_FRAMING_V1 = (
    "ascii('h2hdb-vnext-analysis-candidate-priority\\0') || "
    "u32be(encoding_version=1) || u8(candidate_kind: "
    "CONTENT_OWNER=0,GID_WINNER=1) || u8(prefer_not_already_uploaded: "
    "marker_absent=1,marker_present=0) || "
    "u64be(unicode_scalar_title_length_int63) || u64be(download_time_int63) || "
    "if candidate_kind=CONTENT_OWNER then u64be(gid_positive_int63); the kind "
    "fixes the exact frame length and trailing bytes are forbidden"
)
_ANALYSIS_CANDIDATE_ORDERING_RULE_V1 = (
    "within each candidate relation and group select the lexicographically greatest "
    "tuple (priority_key, scope_key, locator_sha256), comparing every byte string "
    "unsigned bytewise; the SQL equivalent is ORDER BY priority_key DESC, scope_key "
    "DESC, locator_sha256 DESC; no collation, truncation, reversal, or incumbent "
    "input is permitted"
)
_ANALYSIS_CANDIDATE_MARKER_RULE_V1 = (
    "the marker is exact ASCII bytes already uploaded; compare every exact "
    "strict-UTF-8 tag value after mapping only ASCII A-Z bytes to a-z, ignoring "
    "namespace; do not apply Unicode casefold, normalization, locale, collation, "
    "or trimming; any marker match encodes zero and absence encodes one"
)
_ANALYSIS_CANDIDATE_DIGEST_FRAMING_V1 = (
    "candidate_sha256 = SHA256(ascii('h2hdb-vnext-analysis-candidate-audit\\0') "
    "|| u32be(encoding_version=1) || u8(candidate_kind: "
    "CONTENT_OWNER=0,GID_WINNER=1) || raw16(analysis_id) || if CONTENT_OWNER "
    "raw32(content_sha256) else u64be(gid_positive_int63) || exact canonical "
    "priority_key of the same kind || raw32(scope_key) || raw32(locator_sha256)); "
    "the kind fixes every field boundary, exact EOF is required, and the digest is "
    "audit-only"
)
_ANALYSIS_CANDIDATE_RUNTIME_OBLIGATION_V1 = (
    "production streams the exact canonical title pages through "
    "StrictUtf8ScalarCounter and count_analysis_title_scalars to obtain one fixed "
    "AnalysisTitleScalarReceipt without materializing unbounded title bytes; "
    "the private production _encode_streamed_candidate_priority adapter first "
    "validates the exact metadata tree, then drives a second bounded page stream "
    "into that receipt and must remain byte-equal to the public reference encoder; "
    "encode_analysis_candidate_priority and validate_analysis_candidate_priority "
    "accept only that fixed scalar receipt, while "
    "decode_analysis_candidate_priority validates the stored frame over immutable "
    "snapshot facts; "
    "analysis_candidate_has_already_uploaded implements the exact ASCII-only marker "
    "rule; analysis_candidate_total_order_key implements the unsigned lexicographic "
    "maximum over priority_key, scope_key, and locator_sha256; "
    "analysis_content_candidate_digest and analysis_gid_candidate_digest construct "
    "candidate_sha256 solely as the exact audit frame; reject monolithic production "
    "title bytes, gallery_id, unique "
    "gallery location, incumbent state, leaf name, allocation order, Unicode "
    "database behavior, collation, truncation, caller bytes, or candidate_sha256 as "
    "comparator authority"
)


def _validate_analysis_candidate_contract(
    candidate: AnalysisCandidateContract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "analysis candidate contract"
    expected_scalar_shape = (
        candidate.content_relation == "analysis_content_owner_candidate"
        and candidate.gid_relation == "analysis_gid_candidate"
        and candidate.gallery_relation == "gallery_identity"
        and candidate.priority_attribute == "priority_key"
        and candidate.candidate_digest_attribute == "candidate_sha256"
        and candidate.stable_tie_break_attributes == ("scope_key", "locator_sha256")
        and candidate.encoding_version == 1
    )
    exact_codec = (
        candidate.framing == _ANALYSIS_CANDIDATE_PRIORITY_FRAMING_V1
        and candidate.ordering_rule == _ANALYSIS_CANDIDATE_ORDERING_RULE_V1
        and candidate.already_uploaded_marker_rule == _ANALYSIS_CANDIDATE_MARKER_RULE_V1
        and candidate.candidate_digest_framing == _ANALYSIS_CANDIDATE_DIGEST_FRAMING_V1
        and candidate.runtime_obligation == _ANALYSIS_CANDIDATE_RUNTIME_OBLIGATION_V1
    )
    if not expected_scalar_shape or not exact_codec:
        errors.append(f"{prefix} must equal the closed executable v1 codec")
    gallery = relation_by_name.get(candidate.gallery_relation)
    if gallery is None or not {
        "gallery_id",
        *candidate.stable_tie_break_attributes,
    } <= set(gallery.attributes):
        errors.append(f"{prefix} references no complete gallery location identity")
    for relation_name in (candidate.content_relation, candidate.gid_relation):
        relation = relation_by_name.get(relation_name)
        if relation is None or not {
            "analysis_id",
            "gallery_id",
            candidate.priority_attribute,
            candidate.candidate_digest_attribute,
        } <= set(relation.attributes):
            errors.append(f"{prefix} relation {relation_name!r} lacks comparator facts")
    return errors


def _validate_long_value_storage_contract(
    storage: LongValueStorageContract,
    relation_by_name: Mapping[str, Relation],
) -> list[str]:
    errors: list[str] = []
    prefix = "long-value storage contract"
    expected_canonical = {
        "source_root_sha256",
        "locator_sha256",
        "content_sha256",
        "observation_identity_sha256",
        "tag_value_sha256",
        "source_title_sha256",
        "title_sha256",
        "sort_title_sha256",
        "contributor_name_sha256",
        "sort_as_sha256",
        "snapshot_manifest_sha256",
        "artifact_semantics_sha256",
        "source_manifest_component_sha256",
        "member_plan_component_sha256",
        "effective_content_component_sha256",
        "selected_component_sha256",
        "owner_component_sha256",
        "policy_component_sha256",
        "summary_sha256",
        "language_sha256",
        "artifact_locator_sha256",
    }
    expected_direct = {
        "metadata_fingerprint",
        "cursor",
        "start_cursor",
        "next_cursor",
    }
    if set(storage.canonical_reference_attributes) != expected_canonical:
        errors.append(f"{prefix} canonical-reference registry is incomplete")
    if set(storage.direct_payload_attributes) != expected_direct:
        errors.append(f"{prefix} direct-payload registry is incomplete")
    if len(storage.canonical_reference_attributes) != len(expected_canonical):
        errors.append(f"{prefix} canonical-reference registry contains duplicates")
    if len(storage.direct_payload_attributes) != len(expected_direct):
        errors.append(f"{prefix} direct-payload registry contains duplicates")
    if expected_canonical & expected_direct:
        errors.append(f"{prefix} registries must be disjoint")
    if (
        storage.canonical_reference_relation != "canonical_value_identity"
        or storage.canonical_reference_attribute != "value_sha256"
    ):
        errors.append(f"{prefix} must reference canonical_value_identity(value_sha256)")
    if "candidate key" not in storage.selection_rule or not storage.rationale.strip():
        errors.append(f"{prefix} must state its selection rule and rationale")

    used_attributes = {
        attribute
        for relation in relation_by_name.values()
        for attribute in relation.attributes
    }
    for attribute in expected_direct:
        if attribute not in used_attributes:
            errors.append(f"{prefix} direct payload {attribute!r} is unused")
        for relation in relation_by_name.values():
            if attribute not in relation.attributes:
                continue
            if any(attribute in key for key in relation.declared_keys):
                errors.append(
                    f"{prefix} direct payload {attribute!r} is illegally used "
                    f"as a key by relation {relation.name!r}"
                )
            if any(
                attribute in foreign_key.attributes
                for foreign_key in relation.foreign_keys
            ):
                errors.append(
                    f"{prefix} direct payload {attribute!r} is illegally used "
                    f"as a foreign key by relation {relation.name!r}"
                )

    canonical_paths: set[tuple[str, str]] = set()
    for relation in relation_by_name.values():
        for foreign_key in relation.foreign_keys:
            for local_attribute, referenced_attribute in zip(
                foreign_key.attributes,
                foreign_key.referenced_attributes,
                strict=True,
            ):
                if (
                    local_attribute in expected_canonical
                    and foreign_key.relation == storage.canonical_reference_relation
                    and referenced_attribute == storage.canonical_reference_attribute
                ):
                    canonical_paths.add((relation.name, local_attribute))
    changed = True
    while changed:
        changed = False
        for relation in relation_by_name.values():
            for foreign_key in relation.foreign_keys:
                for local_attribute, referenced_attribute in zip(
                    foreign_key.attributes,
                    foreign_key.referenced_attributes,
                    strict=True,
                ):
                    path = (relation.name, local_attribute)
                    if (
                        local_attribute in expected_canonical
                        and local_attribute == referenced_attribute
                        and (foreign_key.relation, referenced_attribute)
                        in canonical_paths
                        and path not in canonical_paths
                    ):
                        canonical_paths.add(path)
                        changed = True

    for attribute in expected_canonical:
        if attribute not in used_attributes:
            errors.append(f"{prefix} canonical reference {attribute!r} is unused")
        for relation in relation_by_name.values():
            if attribute not in relation.attributes:
                continue
            if (relation.name, attribute) not in canonical_paths:
                errors.append(
                    f"{prefix} canonical reference {attribute!r} in relation "
                    f"{relation.name!r} does not reference exact canonical bytes"
                )
    return errors


def _requires_semantic_classification(attribute: str) -> bool:
    return (
        attribute.endswith(("_sha256", "_key", "_id", "_count"))
        or attribute == "locator"
        or attribute.endswith("_locator")
    )


def _validate_attribute_semantics(
    contract: Contract, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    """Require an auditable semantic decision for FD-sensitive attributes.

    Suffixes alone cannot tell us whether a digest is injective, whether an ID
    is global, or whether a count is authoritative.  The registry makes those
    closed-world assumptions explicit.  Identity/payload digests additionally
    need a singleton candidate-key relation, preventing an occurrence table
    from silently being treated as the digest's payload authority.
    """

    errors: list[str] = []
    used_attributes = {
        attribute
        for relation in contract.relations
        for attribute in relation.attributes
    }
    required = {
        attribute
        for attribute in used_attributes
        if _requires_semantic_classification(attribute)
    }
    semantic_by_name: dict[str, AttributeSemantic] = {}
    for semantic in contract.attribute_semantics:
        prefix = f"attribute semantic {semantic.name!r}"
        if semantic.name in semantic_by_name:
            errors.append(f"duplicate attribute semantic {semantic.name!r}")
            continue
        semantic_by_name[semantic.name] = semantic
        if semantic.name not in used_attributes:
            errors.append(f"{prefix} is not used by any relation")
        if semantic.classification not in _SEMANTIC_CLASSIFICATIONS:
            errors.append(
                f"{prefix} has unsupported classification {semantic.classification!r}"
            )
        if not semantic.rationale.strip():
            errors.append(f"{prefix} must have a non-empty rationale")

    missing = required - set(semantic_by_name)
    if missing:
        errors.append(
            "attribute semantic registry does not cover " + _format_set(missing)
        )

    singleton_keys = {
        next(iter(key))
        for relation in relation_by_name.values()
        for key in enumerate_candidate_keys(
            relation.attributes, relation.functional_dependencies
        )
        if len(key) == 1
    }
    for semantic in contract.attribute_semantics:
        if (
            semantic.classification
            in {
                "canonical_identity_digest",
                "payload_digest",
            }
            and semantic.name not in singleton_keys
        ):
            errors.append(
                f"attribute semantic {semantic.name!r} classifies an identity "
                "digest but no relation declares it as a singleton candidate key"
            )
    return errors


def _validate_relation(relation: Relation) -> tuple[list[str], RelationReport]:
    errors: list[str] = []
    attributes = frozenset(relation.attributes)
    prefix = f"relation {relation.name!r}"
    if not relation.name:
        errors.append("relation name must not be empty")
    if relation.kind not in {"source_of_truth", "controlled_materialization"}:
        errors.append(f"{prefix} has unsupported kind {relation.kind!r}")
    if not relation.attributes:
        errors.append(f"{prefix} must declare attributes")
    if len(attributes) != len(relation.attributes):
        errors.append(f"{prefix} contains duplicate attributes")

    for dependency in relation.functional_dependencies:
        unknown = (dependency.determinant | dependency.dependent) - attributes
        if unknown:
            errors.append(
                f"{prefix} FD mentions unknown attributes {_format_set(unknown)}"
            )
        if not dependency.dependent:
            errors.append(f"{prefix} contains an FD with an empty dependent")

    computed_keys = enumerate_candidate_keys(
        relation.attributes, relation.functional_dependencies
    )
    computed_key_set = set(computed_keys)
    declared_key_set = set(relation.declared_keys)
    if len(declared_key_set) != len(relation.declared_keys):
        errors.append(f"{prefix} declares a duplicate candidate key")
    for key in relation.declared_keys:
        unknown = key - attributes
        if unknown:
            errors.append(
                f"{prefix} key {_format_set(key)} mentions unknown attributes "
                f"{_format_set(unknown)}"
            )
        closure = attribute_closure(key, relation.functional_dependencies) & attributes
        if closure != attributes:
            errors.append(f"{prefix} declared key {_format_set(key)} is not a superkey")
        elif any(
            attribute_closure(key - {attribute}, relation.functional_dependencies)
            & attributes
            == attributes
            for attribute in key
        ):
            errors.append(f"{prefix} declared key {_format_set(key)} is not minimal")
    missing = computed_key_set - declared_key_set
    extra = declared_key_set - computed_key_set
    if missing:
        errors.append(
            f"{prefix} omits candidate keys "
            + ", ".join(_format_set(key) for key in _sorted_sets(missing))
        )
    if extra:
        errors.append(
            f"{prefix} declares non-candidate keys "
            + ", ".join(_format_set(key) for key in _sorted_sets(extra))
        )

    violations = bcnf_violations(relation)
    if violations:
        determinant, dependent = min(
            violations,
            key=lambda item: (len(item[0]), tuple(sorted(item[0]))),
        )
        errors.append(
            f"{prefix} is not BCNF under F+: {_format_set(determinant)} "
            f"determines {_format_set(dependent)} but is not a superkey"
        )

    return errors, RelationReport(
        relation.name,
        tuple(_sorted_sets(computed_keys)),
        1 << len(attributes),
    )


def _validate_foreign_keys(
    relation: Relation,
    relation_by_name: Mapping[str, Relation],
    external_by_name: Mapping[str, ExternalRelation] | None = None,
) -> list[str]:
    errors: list[str] = []
    external_relations = external_by_name or {}
    local_attributes = frozenset(relation.attributes)
    for index, foreign_key in enumerate(relation.foreign_keys, 1):
        prefix = f"relation {relation.name!r} foreign key {index}"
        if not foreign_key.attributes:
            errors.append(f"{prefix} must contain at least one attribute")
        if len(set(foreign_key.attributes)) != len(foreign_key.attributes):
            errors.append(f"{prefix} contains duplicate local attributes")
        unknown_local = set(foreign_key.attributes) - local_attributes
        if unknown_local:
            errors.append(
                f"{prefix} mentions unknown local attributes "
                f"{_format_set(unknown_local)}"
            )
        target = relation_by_name.get(foreign_key.relation)
        external_target = external_relations.get(foreign_key.relation)
        if target is None:
            if external_target is None:
                errors.append(
                    f"{prefix} references unknown relation {foreign_key.relation!r}"
                )
                continue
        if len(foreign_key.attributes) != len(foreign_key.referenced_attributes):
            errors.append(f"{prefix} local and referenced arity differ")
            continue
        if len(set(foreign_key.referenced_attributes)) != len(
            foreign_key.referenced_attributes
        ):
            errors.append(f"{prefix} contains duplicate referenced attributes")
        if target is not None:
            target_attributes = target.attributes
            target_keys = set(
                enumerate_candidate_keys(
                    target.attributes, target.functional_dependencies
                )
            )
            target_name = target.name
        else:
            assert external_target is not None
            target_attributes = external_target.attributes
            target_keys = set(external_target.declared_keys)
            target_name = external_target.name
        unknown_target = set(foreign_key.referenced_attributes) - set(target_attributes)
        if unknown_target:
            errors.append(
                f"{prefix} mentions unknown referenced attributes "
                f"{_format_set(unknown_target)}"
            )
            continue
        referenced = frozenset(foreign_key.referenced_attributes)
        if referenced not in target_keys:
            errors.append(
                f"{prefix} target {_format_set(referenced)} is not a candidate key "
                f"of {target_name!r}"
            )
    return errors


def _validate_external_relation(external: ExternalRelation) -> list[str]:
    """Validate a closed external candidate-key declaration without inventing FDs."""

    errors: list[str] = []
    prefix = f"external relation {external.name!r}"
    attributes = set(external.attributes)
    if not external.name:
        errors.append("external relation name must not be empty")
    if not external.attributes:
        errors.append(f"{prefix} must declare attributes")
    if len(attributes) != len(external.attributes):
        errors.append(f"{prefix} contains duplicate attributes")
    if not external.declared_keys:
        errors.append(f"{prefix} must declare at least one candidate key shape")
    if len(set(external.declared_keys)) != len(external.declared_keys):
        errors.append(f"{prefix} declares a duplicate candidate key shape")
    for key in external.declared_keys:
        if not key:
            errors.append(f"{prefix} contains an empty candidate key shape")
        unknown = key - attributes
        if unknown:
            errors.append(
                f"{prefix} candidate key mentions unknown attributes "
                f"{_format_set(unknown)}"
            )
        if any(other < key for other in external.declared_keys):
            errors.append(
                f"{prefix} candidate key {_format_set(key)} is not inclusion-minimal"
            )
    return errors


def _validate_materialization(
    relation: Relation, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    errors: list[str] = []
    metadata = relation.materialization
    prefix = f"relation {relation.name!r}"
    if relation.kind == "source_of_truth":
        if metadata is not None:
            errors.append(
                f"{prefix} is source_of_truth but has materialization metadata"
            )
        return errors
    if relation.kind != "controlled_materialization":
        return errors
    if metadata is None:
        return [f"{prefix} controlled materialization lacks rationale metadata"]
    if metadata.get("authoritative") is not False:
        errors.append(f"{prefix} materialization must set authoritative = false")
    for field in ("rationale", "refresh_strategy"):
        if not isinstance(metadata.get(field), str) or not metadata[field].strip():
            errors.append(f"{prefix} materialization.{field} must be non-empty")
    derived_from = metadata.get("derived_from")
    if (
        not isinstance(derived_from, list)
        or not derived_from
        or not all(isinstance(value, str) and value for value in derived_from)
    ):
        errors.append(
            f"{prefix} materialization.derived_from must be a non-empty string array"
        )
    else:
        unknown = set(derived_from) - set(relation_by_name)
        if unknown:
            errors.append(
                f"{prefix} materialization derives from unknown relations "
                f"{_format_set(unknown)}"
            )
    return errors


def _validate_decomposition(
    decomposition: Decomposition, relation_by_name: Mapping[str, Relation]
) -> list[str]:
    errors: list[str] = []
    prefix = f"decomposition {decomposition.name!r}"
    if not decomposition.universal_attributes:
        errors.append(f"{prefix} must declare universal_attributes")
    if len(decomposition.projections) != 2:
        errors.append(
            f"{prefix} has {len(decomposition.projections)} projections; "
            "only explicit binary FD losslessness is supported"
        )
        return errors
    projected_union: set[str] = set()
    for projection in decomposition.projections:
        projected_union.update(projection.attributes)
        if not projection.attributes:
            errors.append(f"{prefix} contains an empty projection")
        unknown = projection.attributes - decomposition.universal_attributes
        if unknown:
            errors.append(
                f"{prefix} projection {projection.relation!r} contains attributes "
                f"outside the universal relation: {_format_set(unknown)}"
            )
        relation = relation_by_name.get(projection.relation)
        if relation is None:
            errors.append(
                f"{prefix} references unknown projection relation "
                f"{projection.relation!r}"
            )
        elif projection.attributes != frozenset(relation.attributes):
            errors.append(
                f"{prefix} projection for {projection.relation!r} must exactly "
                "match that relation's attributes"
            )
    if frozenset(projected_union) != decomposition.universal_attributes:
        errors.append(f"{prefix} projections do not cover the universal relation")
    for dependency in decomposition.functional_dependencies:
        unknown = (
            dependency.determinant | dependency.dependent
        ) - decomposition.universal_attributes
        if unknown:
            errors.append(
                f"{prefix} FD mentions attributes outside the universal relation: "
                f"{_format_set(unknown)}"
            )
    if not decomposition.rationale.strip():
        errors.append(f"{prefix} must explain its conceptual universal relation")
    if not errors:
        lossless = is_binary_lossless(decomposition)
        dependency_preserving = is_dependency_preserving(decomposition)
        if not lossless:
            left, right = decomposition.projections
            errors.append(
                f"{prefix} is lossy under its declared FDs: intersection "
                f"{_format_set(left.attributes & right.attributes)} determines "
                "neither projection"
            )
        if not dependency_preserving:
            errors.append(
                f"{prefix} is not dependency-preserving under its declared FDs: "
                "the union of the F+ projections does not imply every original FD"
            )
    return errors


def _parse_relation(value: Mapping[str, Any]) -> Relation:
    context = f"relation {value.get('name', '<unnamed>')!r}"
    return Relation(
        name=_string(value, "name", context),
        kind=_string(value, "kind", context),
        attributes=_string_tuple(value, "attributes", context),
        functional_dependencies=tuple(
            _parse_fd(item, f"{context}.fds")
            for item in _table_list(value, "fds", context)
        ),
        declared_keys=tuple(
            frozenset(_string_sequence(item, f"{context}.declared_keys"))
            for item in _list(value, "declared_keys", context)
        ),
        foreign_keys=tuple(
            _parse_foreign_key(item, f"{context}.foreign_keys")
            for item in _optional_table_list(value, "foreign_keys", context)
        ),
        materialization=_optional_table(value, "materialization", context),
        rationale=(
            value["rationale"] if isinstance(value.get("rationale"), str) else ""
        ),
    )


def _parse_external_relation(value: Mapping[str, Any]) -> ExternalRelation:
    context = f"external relation {value.get('name', '<unnamed>')!r}"
    return ExternalRelation(
        name=_string(value, "name", context),
        attributes=_string_tuple(value, "attributes", context),
        declared_keys=tuple(
            frozenset(_string_sequence(item, f"{context}.declared_keys"))
            for item in _list(value, "declared_keys", context)
        ),
    )


def _parse_decomposition(value: Mapping[str, Any]) -> Decomposition:
    context = f"decomposition {value.get('name', '<unnamed>')!r}"
    return Decomposition(
        name=_string(value, "name", context),
        universal_attributes=frozenset(
            _string_tuple(value, "universal_attributes", context)
        ),
        functional_dependencies=tuple(
            _parse_fd(item, f"{context}.fds")
            for item in _table_list(value, "fds", context)
        ),
        projections=tuple(
            Projection(
                _string(item, "relation", f"{context}.projections"),
                frozenset(_string_tuple(item, "attributes", f"{context}.projections")),
            )
            for item in _table_list(value, "projections", context)
        ),
        rationale=_string(value, "rationale", context),
    )


def _parse_attribute_semantic(value: Mapping[str, Any]) -> AttributeSemantic:
    context = f"attribute semantic {value.get('name', '<unnamed>')!r}"
    return AttributeSemantic(
        name=_string(value, "name", context),
        classification=_string(value, "classification", context),
        rationale=_string(value, "rationale", context),
    )


def _parse_semantic_obligation(value: Mapping[str, Any]) -> SemanticObligation:
    context = f"semantic obligation {value.get('id', '<unnamed>')!r}"
    raw_ready_check = value.get("ready_check", value.get("check"))
    raw_writer_hook = value.get("writer_hook", value.get("hook"))
    raw_writer_hook_version = value.get("writer_hook_version", value.get("version"))
    if not isinstance(raw_ready_check, str) or not raw_ready_check:
        raise ContractFormatError(f"{context}.ready_check must be a non-empty string")
    if not isinstance(raw_writer_hook, str) or not raw_writer_hook:
        raise ContractFormatError(f"{context}.writer_hook must be a non-empty string")
    if not isinstance(raw_writer_hook_version, int) or isinstance(
        raw_writer_hook_version, bool
    ):
        raise ContractFormatError(f"{context}.writer_hook_version must be an integer")
    raw_covers = value.get("covers", [f"semantic_obligation.{value.get('id', '')}"])
    covers = tuple(_string_sequence(raw_covers, f"{context}.covers"))
    return SemanticObligation(
        id=_string(value, "id", context),
        version=_integer(value, "version", context),
        classification=_string(value, "class", context),
        lifecycle=_string(value, "lifecycle", context),
        ready_check=raw_ready_check,
        writer_hook=raw_writer_hook,
        writer_hook_version=raw_writer_hook_version,
        scope=_string(value, "scope", context),
        relations=_string_tuple(value, "relations", context),
        covers=covers,
        description=_string(value, "description", context),
    )


def _parse_bootstrap_seed(value: Mapping[str, Any]) -> BootstrapSeed:
    context = f"bootstrap seed {value.get('id', '<unnamed>')!r}"
    return BootstrapSeed(
        id=_string(value, "id", context),
        relation=_string(value, "relation", context),
        columns=_string_tuple(value, "columns", context),
        values=_string_tuple(value, "values", context),
    )


def _parse_canonical_reference_role(
    value: Mapping[str, Any],
) -> CanonicalReferenceRole:
    context = f"canonical reference role {value.get('attribute', '<unnamed>')!r}"
    return CanonicalReferenceRole(
        attribute=_string(value, "attribute", context),
        digest_domain=_string(value, "digest_domain", context),
        relations=_string_tuple(value, "relations", context),
    )


def _parse_identity_codec(value: Mapping[str, Any]) -> IdentityCodec:
    context = f"identity codec {value.get('id', '<unnamed>')!r}"
    return IdentityCodec(
        id=_string(value, "id", context),
        target_attribute=_string(value, "target_attribute", context),
        version=_integer(value, "version", context),
        framing=_string(value, "framing", context),
        golden_input_hex=_string(value, "golden_input_hex", context),
        golden_sha256=_string(value, "golden_sha256", context),
    )


def _parse_retention_contract(value: Mapping[str, Any]) -> RetentionContract:
    context = "contract.retention_contract"
    return RetentionContract(
        version=_integer(value, "version", context),
        indefinitely_retained_relations=_string_tuple(
            value, "indefinitely_retained_relations", context
        ),
        active_head_relation=_string(value, "active_head_relation", context),
        revision_relation=_string(value, "revision_relation", context),
        provenance_relation=_string(value, "provenance_relation", context),
        analysis_relation=_string(value, "analysis_relation", context),
        build_relation=_string(value, "build_relation", context),
        semantic_obligation_id=_string(value, "semantic_obligation_id", context),
        source_history_rule=_string(value, "source_history_rule", context),
        catalog_history_rule=_string(value, "catalog_history_rule", context),
        active_source_rule=_string(value, "active_source_rule", context),
        inactive_source_rule=_string(value, "inactive_source_rule", context),
    )


def _parse_retention_fk_boundary(
    value: Mapping[str, Any], context: str
) -> RetentionForeignKeyBoundary:
    return RetentionForeignKeyBoundary(
        relation=_string(value, "relation", context),
        attributes=_string_tuple(value, "attributes", context),
        referenced_relation=_string(value, "referenced_relation", context),
        referenced_attributes=_string_tuple(value, "referenced_attributes", context),
    )


def _parse_retention_semantic_blocker(
    value: Mapping[str, Any], context: str
) -> RetentionSemanticBlocker:
    return RetentionSemanticBlocker(
        relation=_string(value, "relation", context),
        attributes=_string_tuple(value, "attributes", context),
        root_attributes=_string_tuple(value, "root_attributes", context),
        semantic_obligation_id=_string(value, "semantic_obligation_id", context),
    )


def _parse_retention_machine_gate(
    value: Mapping[str, Any], context: str
) -> RetentionMachineGate:
    return RetentionMachineGate(
        id=_string(value, "id", context),
        semantic_obligation_id=_string(value, "semantic_obligation_id", context),
    )


def _parse_retention_target(value: Mapping[str, Any]) -> RetentionTarget:
    context = f"retention target {value.get('target', '<unnamed>')!r}"
    raw_phases = _list(value, "child_phases", context)
    return RetentionTarget(
        target=_string(value, "target", context),
        root_relation=_string(value, "root_relation", context),
        root_key=_string_tuple(value, "root_key", context),
        external_blockers=tuple(
            _parse_retention_fk_boundary(item, f"{context}.external_blockers")
            for item in _optional_table_list(value, "external_blockers", context)
        ),
        retained_outliving=tuple(
            _parse_retention_fk_boundary(item, f"{context}.retained_outliving")
            for item in _optional_table_list(value, "retained_outliving", context)
        ),
        semantic_blockers=tuple(
            _parse_retention_semantic_blocker(item, f"{context}.semantic_blockers")
            for item in _optional_table_list(value, "semantic_blockers", context)
        ),
        machine_gates=tuple(
            _parse_retention_machine_gate(item, f"{context}.machine_gates")
            for item in _optional_table_list(value, "machine_gates", context)
        ),
        derived_views=_string_tuple(value, "derived_views", context),
        child_phases=tuple(
            tuple(_string_sequence(phase, f"{context}.child_phases"))
            for phase in raw_phases
        ),
        phase_selectors=tuple(
            _parse_retention_fk_boundary(item, f"{context}.phase_selectors")
            for item in _optional_table_list(value, "phase_selectors", context)
        ),
    )


def _parse_canonical_digest_contract(
    value: Mapping[str, Any],
) -> CanonicalDigestContract:
    context = "contract.canonical_digest_contract"
    return CanonicalDigestContract(
        policy_relation=_string(value, "policy_relation", context),
        value_relation=_string(value, "value_relation", context),
        digest_attribute=_string(value, "digest_attribute", context),
        allocation_relation=_string(value, "allocation_relation", context),
        page_relation=_string(value, "page_relation", context),
        descriptor_relation=_string(value, "descriptor_relation", context),
        parent_relation=_string(value, "parent_relation", context),
        root_attribute=_string(value, "root_attribute", context),
        byte_count_attribute=_string(value, "byte_count_attribute", context),
        algorithm=_string(value, "algorithm", context),
        algorithm_version=_integer(value, "algorithm_version", context),
        encoding=_string(value, "encoding", context),
        framing=_string(value, "framing", context),
        collision_model=_string(value, "collision_model", context),
        write_obligation=_string(value, "write_obligation", context),
        read_obligation=_string(value, "read_obligation", context),
    )


def _parse_canonical_value_page_contract(
    value: Mapping[str, Any],
) -> CanonicalValuePageContract:
    context = "contract.canonical_value_page_contract"
    return CanonicalValuePageContract(
        codec_version=_integer(value, "codec_version", context),
        prefix=_string(value, "prefix", context),
        maximum_page_bytes=_integer(value, "maximum_page_bytes", context),
        chunk_maximum_bytes=_integer(value, "chunk_maximum_bytes", context),
        branch_capacity=_integer(value, "branch_capacity", context),
        maximum_level=_integer(value, "maximum_level", context),
        maximum_byte_count=_integer(value, "maximum_byte_count", context),
        framing=_string(value, "framing", context),
        leaf_record=_string(value, "leaf_record", context),
        branch_record=_string(value, "branch_record", context),
        canonical_tree_rule=_string(value, "canonical_tree_rule", context),
        collision_obligation=_string(value, "collision_obligation", context),
        seal_obligation=_string(value, "seal_obligation", context),
        cleanup_rule=_string(value, "cleanup_rule", context),
    )


def _parse_source_locator_contract(
    value: Mapping[str, Any],
) -> SourceLocatorContract:
    context = "contract.source_locator_contract"
    return SourceLocatorContract(
        identity_relation=_string(value, "identity_relation", context),
        gallery_relation=_string(value, "gallery_relation", context),
        digest_attribute=_string(value, "digest_attribute", context),
        name_attribute=_string(value, "name_attribute", context),
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        digest_domain=_string(value, "digest_domain", context),
        codec_version=_integer(value, "codec_version", context),
        encoding=_string(value, "encoding", context),
        framing=_string(value, "framing", context),
        write_obligation=_string(value, "write_obligation", context),
        read_obligation=_string(value, "read_obligation", context),
    )


def _parse_source_root_contract(value: Mapping[str, Any]) -> SourceRootContract:
    context = "contract.source_root_contract"
    return SourceRootContract(
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        digest_domain=_string(value, "digest_domain", context),
        codec_version=_integer(value, "codec_version", context),
        encoding=_string(value, "encoding", context),
        framing=_string(value, "framing", context),
        root_rule=_string(value, "root_rule", context),
        segment_rule=_string(value, "segment_rule", context),
        adapter_rule=_string(value, "adapter_rule", context),
        write_obligation=_string(value, "write_obligation", context),
        golden_root_payload_hex=_string(value, "golden_root_payload_hex", context),
        golden_root_sha256=_string(value, "golden_root_sha256", context),
        golden_nested_payload_hex=_string(value, "golden_nested_payload_hex", context),
        golden_nested_sha256=_string(value, "golden_nested_sha256", context),
    )


def _parse_analysis_candidate_contract(
    value: Mapping[str, Any],
) -> AnalysisCandidateContract:
    context = "contract.analysis_candidate_contract"
    return AnalysisCandidateContract(
        content_relation=_string(value, "content_relation", context),
        gid_relation=_string(value, "gid_relation", context),
        gallery_relation=_string(value, "gallery_relation", context),
        priority_attribute=_string(value, "priority_attribute", context),
        candidate_digest_attribute=_string(
            value, "candidate_digest_attribute", context
        ),
        stable_tie_break_attributes=_string_tuple(
            value, "stable_tie_break_attributes", context
        ),
        encoding_version=_integer(value, "encoding_version", context),
        framing=_string(value, "framing", context),
        ordering_rule=_string(value, "ordering_rule", context),
        already_uploaded_marker_rule=_string(
            value, "already_uploaded_marker_rule", context
        ),
        candidate_digest_framing=_string(value, "candidate_digest_framing", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_analysis_run_contract(
    value: Mapping[str, Any],
) -> AnalysisRunContract:
    context = "contract.analysis_run_contract"
    return AnalysisRunContract(
        relation=_string(value, "relation", context),
        natural_key=_string_tuple(value, "natural_key", context),
        manifest_attribute=_string(value, "manifest_attribute", context),
        write_obligation=_string(value, "write_obligation", context),
        attempt_rule=_string(value, "attempt_rule", context),
    )


def _parse_source_scope_identity_contract(
    value: Mapping[str, Any],
) -> SourceScopeIdentityContract:
    context = "contract.source_scope_identity_contract"
    return SourceScopeIdentityContract(
        relation=_string(value, "relation", context),
        key_attribute=_string(value, "key_attribute", context),
        natural_key=_string_tuple(value, "natural_key", context),
        encoding_version=_integer(value, "encoding_version", context),
        framing=_string(value, "framing", context),
        collision_model=_string(value, "collision_model", context),
        write_obligation=_string(value, "write_obligation", context),
        seal_obligation=_string(value, "seal_obligation", context),
    )


def _parse_effective_content_contract(
    value: Mapping[str, Any],
) -> EffectiveContentContract:
    context = "contract.effective_content_contract"
    return EffectiveContentContract(
        reference_attribute=_string(value, "reference_attribute", context),
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        digest_domain=_string(value, "digest_domain", context),
        encoding_version=_integer(value, "encoding_version", context),
        framing=_string(value, "framing", context),
        collision_model=_string(value, "collision_model", context),
        write_obligation=_string(value, "write_obligation", context),
        read_obligation=_string(value, "read_obligation", context),
    )


def _parse_source_snapshot_manifest_contract(
    value: Mapping[str, Any],
) -> SourceSnapshotManifestContract:
    context = "contract.source_snapshot_manifest_contract"
    return SourceSnapshotManifestContract(
        relation=_string(value, "relation", context),
        analysis_binding_relation=_string(value, "analysis_binding_relation", context),
        digest_attribute=_string(value, "digest_attribute", context),
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        digest_domain=_string(value, "digest_domain", context),
        codec_version=_integer(value, "codec_version", context),
        framing=_string(value, "framing", context),
        canonical_order=_string(value, "canonical_order", context),
        decision_predicate=_string(value, "decision_predicate", context),
        write_obligation=_string(value, "write_obligation", context),
        handoff_obligation=_string(value, "handoff_obligation", context),
        publication_obligation=_string(value, "publication_obligation", context),
        retention=_string(value, "retention", context),
    )


def _parse_publication_atomic_contract(
    value: Mapping[str, Any],
) -> PublicationAtomicContract:
    context = "contract.publication_atomic_contract"
    return PublicationAtomicContract(
        candidate_relation=_string(value, "candidate_relation", context),
        selection_relation=_string(value, "selection_relation", context),
        artifact_input_relation=_string(value, "artifact_input_relation", context),
        artifact_component_relation=_string(
            value, "artifact_component_relation", context
        ),
        operation_relation=_string(value, "operation_relation", context),
        prepared_artifact_relation=_string(
            value, "prepared_artifact_relation", context
        ),
        stage_relation=_string(value, "stage_relation", context),
        projection_seal_relation=_string(value, "projection_seal_relation", context),
        checkpoint_relation=_string(value, "checkpoint_relation", context),
        batch_receipt_relation=_string(value, "batch_receipt_relation", context),
        source_manifest_binding_relation=_string(
            value, "source_manifest_binding_relation", context
        ),
        revision_relation=_string(value, "revision_relation", context),
        head_relation=_string(value, "head_relation", context),
        finalization_stage=_string(value, "finalization_stage", context),
        selection_rule=_string(value, "selection_rule", context),
        cursor_codec_rule=_string(value, "cursor_codec_rule", context),
        batch_rule=_string(value, "batch_rule", context),
        projection_seal_rule=_string(value, "projection_seal_rule", context),
        batch_stages=tuple(
            PublicationBatchStage(
                name=_string(stage, "name", f"{context}.batch_stage"),
                stage_order=_integer(stage, "stage_order", f"{context}.batch_stage"),
                cursor_codec=_string(stage, "cursor_codec", f"{context}.batch_stage"),
                prerequisite=_string(stage, "prerequisite", f"{context}.batch_stage"),
                sealed_scalar=_string(stage, "sealed_scalar", f"{context}.batch_stage"),
            )
            for stage in _table_list(value, "batch_stage", context)
        ),
        finalization_rule=_string(value, "finalization_rule", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_file_identity_contract(
    value: Mapping[str, Any],
) -> FileIdentityContract:
    context = "contract.file_identity_contract"
    return FileIdentityContract(
        relation=_string(value, "relation", context),
        key_attribute=_string(value, "key_attribute", context),
        name_attribute=_string(value, "name_attribute", context),
        role_attribute=_string(value, "role_attribute", context),
        algorithm_version=_integer(value, "algorithm_version", context),
        framing=_string(value, "framing", context),
        collision_model=_string(value, "collision_model", context),
        role_classifier_version=_integer(value, "role_classifier_version", context),
        metadata_name=_string(value, "metadata_name", context),
        write_obligation=_string(value, "write_obligation", context),
        read_obligation=_string(value, "read_obligation", context),
    )


def _parse_gallery_observation_identity_contract(
    value: Mapping[str, Any],
) -> GalleryObservationIdentityContract:
    context = "contract.gallery_observation_identity_contract"
    return GalleryObservationIdentityContract(
        relation=_string(value, "relation", context),
        gallery_attribute=_string(value, "gallery_attribute", context),
        identifier_attribute=_string(value, "identifier_attribute", context),
        digest_attribute=_string(value, "digest_attribute", context),
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        digest_domain=_string(value, "digest_domain", context),
        encoding_version=_integer(value, "encoding_version", context),
        framing=_string(value, "framing", context),
        write_obligation=_string(value, "write_obligation", context),
        reuse_obligation=_string(value, "reuse_obligation", context),
    )


def _parse_gallery_observation_page_contract(
    value: Mapping[str, Any],
) -> GalleryObservationPageContract:
    context = "contract.gallery_observation_page_contract"
    return GalleryObservationPageContract(
        allocation_relation=_string(value, "allocation_relation", context),
        final_relation=_string(value, "final_relation", context),
        page_relation=_string(value, "page_relation", context),
        allocation_page_relation=_string(value, "allocation_page_relation", context),
        child_relation=_string(value, "child_relation", context),
        descriptor_relation=_string(value, "descriptor_relation", context),
        key_bounds_relation=_string(value, "key_bounds_relation", context),
        tree_root_relation=_string(value, "tree_root_relation", context),
        page_digest_attribute=_string(value, "page_digest_attribute", context),
        page_bytes_attribute=_string(value, "page_bytes_attribute", context),
        algorithm=_string(value, "algorithm", context),
        codec_version=_integer(value, "codec_version", context),
        prefix=_string(value, "prefix", context),
        maximum_page_bytes=_integer(value, "maximum_page_bytes", context),
        maximum_entries=_integer(value, "maximum_entries", context),
        file_leaf_capacity=_integer(value, "file_leaf_capacity", context),
        tag_leaf_capacity=_integer(value, "tag_leaf_capacity", context),
        directory_leaf_capacity=_integer(value, "directory_leaf_capacity", context),
        metadata_leaf_capacity=_integer(value, "metadata_leaf_capacity", context),
        metadata_chunk_maximum_bytes=_integer(
            value, "metadata_chunk_maximum_bytes", context
        ),
        branch_capacity=_integer(value, "branch_capacity", context),
        maximum_level=_integer(value, "maximum_level", context),
        maximum_items=_integer(value, "maximum_items", context),
        components=_string_tuple(value, "components", context),
        node_kinds=_string_tuple(value, "node_kinds", context),
        framing=_string(value, "framing", context),
        file_leaf_record=_string(value, "file_leaf_record", context),
        tag_leaf_record=_string(value, "tag_leaf_record", context),
        directory_leaf_record=_string(value, "directory_leaf_record", context),
        metadata_leaf_record=_string(value, "metadata_leaf_record", context),
        metadata_stream_framing=_string(value, "metadata_stream_framing", context),
        branch_record=_string(value, "branch_record", context),
        canonical_tree_rule=_string(value, "canonical_tree_rule", context),
        materialization_rule=_string(value, "materialization_rule", context),
        collision_obligation=_string(value, "collision_obligation", context),
        seal_obligation=_string(value, "seal_obligation", context),
        cleanup_rule=_string(value, "cleanup_rule", context),
        golden_empty_file_page_hex=_string(
            value, "golden_empty_file_page_hex", context
        ),
        golden_empty_file_page_sha256=_string(
            value, "golden_empty_file_page_sha256", context
        ),
    )


def _parse_title_sort_contract(
    value: Mapping[str, Any],
) -> TitleSortContract:
    context = "contract.title_sort_contract"
    return TitleSortContract(
        policy_relation=_string(value, "policy_relation", context),
        display_policy_relation=_string(value, "display_policy_relation", context),
        sort_relation=_string(value, "sort_relation", context),
        algorithm_attribute=_string(value, "algorithm_attribute", context),
        unicode_attribute=_string(value, "unicode_attribute", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_long_value_storage_contract(
    value: Mapping[str, Any],
) -> LongValueStorageContract:
    context = "contract.long_value_storage_contract"
    return LongValueStorageContract(
        canonical_reference_relation=_string(
            value, "canonical_reference_relation", context
        ),
        canonical_reference_attribute=_string(
            value, "canonical_reference_attribute", context
        ),
        canonical_reference_attributes=_string_tuple(
            value, "canonical_reference_attributes", context
        ),
        direct_payload_attributes=_string_tuple(
            value, "direct_payload_attributes", context
        ),
        selection_rule=_string(value, "selection_rule", context),
        rationale=_string(value, "rationale", context),
    )


def _parse_byte_domain(value: Mapping[str, Any]) -> ByteDomain:
    context = f"byte domain {value.get('attribute', '<unnamed>')!r}"
    return ByteDomain(
        attribute=_string(value, "attribute", context),
        maximum_bytes=_integer(value, "maximum_bytes", context),
        encoding=_string(value, "encoding", context),
        source=_string(value, "source", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_artifact_delta_contract(
    value: Mapping[str, Any],
) -> ArtifactDeltaContract:
    context = "contract.artifact_delta_contract"
    return ArtifactDeltaContract(
        classification=_string(value, "classification", context),
        operation_relation=_string(value, "operation_relation", context),
        old_state_relation=_string(value, "old_state_relation", context),
        new_state_relation=_string(value, "new_state_relation", context),
        semantic_component_relation=_string(
            value, "semantic_component_relation", context
        ),
        semantic_components=_string_tuple(value, "semantic_components", context),
        operations=_string_tuple(value, "operations", context),
        old_state_operations=_string_tuple(value, "old_state_operations", context),
        new_state_operations=_string_tuple(value, "new_state_operations", context),
        rebuild_rule=_string(value, "rebuild_rule", context),
        unchanged_rule=_string(value, "unchanged_rule", context),
        rename_rule=_string(value, "rename_rule", context),
    )


def _parse_transition_authority_contract(
    value: Mapping[str, Any],
) -> TransitionAuthorityContract:
    context = "contract.transition_authority_contract"
    return TransitionAuthorityContract(
        version=_integer(value, "version", context),
        gate_relations=_string_tuple(value, "gate_relations", context),
        forbidden_digest_attributes=_string_tuple(
            value, "forbidden_digest_attributes", context
        ),
        audit_only_digest_attributes=_string_tuple(
            value, "audit_only_digest_attributes", context
        ),
        batch_key_attribute=_string(value, "batch_key_attribute", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
        ready_obligation=_string(value, "ready_obligation", context),
    )


def _parse_artifact_byte_producer_contract(
    value: Mapping[str, Any],
) -> ArtifactByteProducerContract:
    context = "contract.artifact_byte_producer_contract"
    return ArtifactByteProducerContract(
        policy_relation=_string(value, "policy_relation", context),
        algorithm_attribute=_string(value, "algorithm_attribute", context),
        producer_relation=_string(value, "producer_relation", context),
        zip_writer_policy_relation=_string(
            value, "zip_writer_policy_relation", context
        ),
        storage_codec_relation=_string(value, "storage_codec_relation", context),
        independent_parameters=_string_tuple(value, "independent_parameters", context),
        algorithm_bundle=_string_tuple(value, "algorithm_bundle", context),
        producer_fingerprint_framing=_string(
            value, "producer_fingerprint_framing", context
        ),
        producer_fingerprint_golden_payload_hex=_string(
            value, "producer_fingerprint_golden_payload_hex", context
        ),
        producer_fingerprint_golden_sha256=_string(
            value, "producer_fingerprint_golden_sha256", context
        ),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_artifact_name_contract(
    value: Mapping[str, Any],
) -> ArtifactNameContract:
    context = "contract.artifact_name_contract"
    return ArtifactNameContract(
        relation=_string(value, "relation", context),
        gid_attribute=_string(value, "gid_attribute", context),
        name_attribute=_string(value, "name_attribute", context),
        codec_version=_integer(value, "codec_version", context),
        framing=_string(value, "framing", context),
        golden_gid=_integer(value, "golden_gid", context),
        golden_name_hex=_string(value, "golden_name_hex", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_artifact_locator_contract(
    value: Mapping[str, Any],
) -> ArtifactLocatorContract:
    context = "contract.artifact_locator_contract"
    return ArtifactLocatorContract(
        relation=_string(value, "relation", context),
        artifact_attribute=_string(value, "artifact_attribute", context),
        locator_attribute=_string(value, "locator_attribute", context),
        storage_codec_version=_integer(value, "storage_codec_version", context),
        locator_codec_version=_integer(value, "locator_codec_version", context),
        components=_string_tuple(value, "components", context),
        derivation=_string(value, "derivation", context),
        golden_artifact_sha256=_string(value, "golden_artifact_sha256", context),
        golden_payload_hex=_string(value, "golden_payload_hex", context),
        golden_locator_sha256=_string(value, "golden_locator_sha256", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_artifact_protection_token_contract(
    value: Mapping[str, Any],
) -> ArtifactProtectionTokenContract:
    context = "contract.artifact_protection_token_contract"
    return ArtifactProtectionTokenContract(
        relation=_string(value, "relation", context),
        storage_codec_relation=_string(value, "storage_codec_relation", context),
        codec_version=_integer(value, "codec_version", context),
        exact_bytes=_integer(value, "exact_bytes", context),
        receipt_framing=_string(value, "receipt_framing", context),
        token_framing=_string(value, "token_framing", context),
        golden_receipt_id=_string(value, "golden_receipt_id", context),
        golden_token_hex=_string(value, "golden_token_hex", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
    )


def _parse_artifact_member_plan_contract(
    value: Mapping[str, Any],
) -> ArtifactMemberPlanContract:
    context = "contract.artifact_member_plan_contract"
    return ArtifactMemberPlanContract(
        semantic_relation=_string(value, "semantic_relation", context),
        component_attribute=_string(value, "component_attribute", context),
        component_kind=_string(value, "component_kind", context),
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        plan_version=_integer(value, "plan_version", context),
        framing=_string(value, "framing", context),
        entry_fields=_string_tuple(value, "entry_fields", context),
        runtime_obligation=_string(value, "runtime_obligation", context),
        ready_obligation=_string(value, "ready_obligation", context),
    )


def _parse_artifact_member_plan_enum(
    value: Mapping[str, Any],
) -> ArtifactMemberPlanEnum:
    context = "contract.artifact_member_plan_enum"

    def integer_map(key: str) -> Mapping[str, int]:
        raw = _table(value, key, context)
        if not raw or not all(
            isinstance(name, str)
            and name
            and isinstance(tag, int)
            and not isinstance(tag, bool)
            for name, tag in raw.items()
        ):
            raise ContractFormatError(f"{context}.{key} must map names to integers")
        return dict(raw)

    return ArtifactMemberPlanEnum(
        entry_kind=integer_map("entry_kind"),
        source_role=integer_map("source_role"),
        transform_kind=integer_map("transform_kind"),
        boolean_tags=integer_map("boolean_tags"),
        position_rule=_string(value, "position_rule", context),
        source_rule=_string(value, "source_rule", context),
        transform_rule=_string(value, "transform_rule", context),
    )


def _parse_artifact_component_codec(
    value: Mapping[str, Any],
) -> ArtifactComponentCodec:
    context = f"artifact component codec {value.get('kind', '<unnamed>')!r}"
    return ArtifactComponentCodec(
        kind=_string(value, "kind", context),
        attribute=_string(value, "attribute", context),
        digest_domain=_string(value, "digest_domain", context),
        codec_version=_integer(value, "codec_version", context),
        framing=_string(value, "framing", context),
        canonical_order=_string(value, "canonical_order", context),
        golden_payload_hex=_string(value, "golden_payload_hex", context),
        golden_sha256=_string(value, "golden_sha256", context),
    )


def _parse_artifact_semantics_codec(
    value: Mapping[str, Any],
) -> ArtifactSemanticsCodec:
    context = "contract.artifact_semantics_codec"
    return ArtifactSemanticsCodec(
        digest_domain=_string(value, "digest_domain", context),
        codec_version=_integer(value, "codec_version", context),
        framing=_string(value, "framing", context),
        golden_payload_hex=_string(value, "golden_payload_hex", context),
        golden_sha256=_string(value, "golden_sha256", context),
    )


def _parse_zip_comment_contract(value: Mapping[str, Any]) -> ZipCommentContract:
    context = "contract.zip_comment_contract"
    return ZipCommentContract(
        codec_version=_integer(value, "codec_version", context),
        framing=_string(value, "framing", context),
        write_obligation=_string(value, "write_obligation", context),
        golden_payload_hex=_string(value, "golden_payload_hex", context),
        golden_payload_sha256=_string(value, "golden_payload_sha256", context),
    )


def _parse_optional_contract_table(
    document: Mapping[str, Any],
    key: str,
    parser: Callable[[Mapping[str, Any]], Any],
) -> Any | None:
    if key not in document:
        return None
    return parser(_table(document, key, "contract"))


def _parse_queue_history_contract(value: Mapping[str, Any]) -> QueueHistoryContract:
    context = "contract.queue_history_contract"
    return QueueHistoryContract(
        deletion_generation_relation=_string(
            value, "deletion_generation_relation", context
        ),
        deletion_generation_head_relation=_string(
            value, "deletion_generation_head_relation", context
        ),
        deletion_attempt_relation=_string(value, "deletion_attempt_relation", context),
        deletion_head_relation=_string(value, "deletion_head_relation", context),
        deletion_url_relation=_string(value, "deletion_url_relation", context),
        consumption_relation=_string(value, "consumption_relation", context),
        preparation_relation=_string(value, "preparation_relation", context),
        generation_rule=_string(value, "generation_rule", context),
        publication_rule=_string(value, "publication_rule", context),
        retention_rule=_string(value, "retention_rule", context),
        rule=_string(value, "rule", context),
    )


def _parse_canonical_hash_cache_contract(
    value: Mapping[str, Any],
) -> CanonicalHashCacheContract:
    context = "contract.canonical_hash_cache_contract"
    return CanonicalHashCacheContract(
        canonical_value_relation=_string(value, "canonical_value_relation", context),
        canonical_allocation_relation=_string(
            value, "canonical_allocation_relation", context
        ),
        canonical_page_relation=_string(value, "canonical_page_relation", context),
        canonical_digest_attribute=_string(
            value, "canonical_digest_attribute", context
        ),
        canonical_policy_attribute=_string(
            value, "canonical_policy_attribute", context
        ),
        canonical_byte_count_attribute=_string(
            value, "canonical_byte_count_attribute", context
        ),
        canonical_root_attribute=_string(value, "canonical_root_attribute", context),
        observation_relation=_string(value, "observation_relation", context),
        source_digest_attribute=_string(value, "source_digest_attribute", context),
        fingerprint_digest_attribute=_string(
            value, "fingerprint_digest_attribute", context
        ),
        source_domain=_string(value, "source_domain", context),
        fingerprint_domain=_string(value, "fingerprint_domain", context),
        write_obligation=_string(value, "write_obligation", context),
        read_obligation=_string(value, "read_obligation", context),
    )


def _parse_operational_event_integrity_contract(
    value: Mapping[str, Any],
) -> OperationalEventIntegrityContract:
    context = "contract.operational_event_integrity_contract"
    return OperationalEventIntegrityContract(
        stream_relation=_string(value, "stream_relation", context),
        preparation_relation=_string(value, "preparation_relation", context),
        seal_relation=_string(value, "seal_relation", context),
        activation_relation=_string(value, "activation_relation", context),
        candidate_binding_relation=_string(
            value, "candidate_binding_relation", context
        ),
        base_relation=_string(value, "base_relation", context),
        removed_subtype_relation=_string(value, "removed_subtype_relation", context),
        deletion_subtype_relation=_string(value, "deletion_subtype_relation", context),
        removed_event_type=_string(value, "removed_event_type", context),
        deletion_event_type=_string(value, "deletion_event_type", context),
        event_digest_codec=_string(value, "event_digest_codec", context),
        chain_codec=_string(value, "chain_codec", context),
        empty_chain_sha256=_string(value, "empty_chain_sha256", context),
        stream_rule=_string(value, "stream_rule", context),
        subtype_rule=_string(value, "subtype_rule", context),
        seal_rule=_string(value, "seal_rule", context),
        activation_rule=_string(value, "activation_rule", context),
        candidate_binding_rule=_string(value, "candidate_binding_rule", context),
        ack_head_relation=_string(value, "ack_head_relation", context),
        ack_rule=_string(value, "ack_rule", context),
        cleanup_rule=_string(value, "cleanup_rule", context),
    )


def _parse_source_build_generation_contract(
    value: Mapping[str, Any],
) -> SourceBuildGenerationContract:
    context = "contract.source_build_generation_contract"
    return SourceBuildGenerationContract(
        reservation_relation=_string(value, "reservation_relation", context),
        rule=_string(value, "rule", context),
    )


def _parse_cleanup_attempt_contract(
    value: Mapping[str, Any],
) -> CleanupAttemptContract:
    context = "contract.cleanup_attempt_contract"
    return CleanupAttemptContract(
        job_relation=_string(value, "job_relation", context),
        attempt_attribute=_string(value, "attempt_attribute", context),
        allocation_rule=_string(value, "allocation_rule", context),
    )


def _parse_preparation_identity_contract(
    value: Mapping[str, Any],
) -> PreparationIdentityContract:
    context = "contract.preparation_identity_contract"
    return PreparationIdentityContract(
        preparation_relation=_string(value, "preparation_relation", context),
        policy_relation=_string(value, "policy_relation", context),
        deletion_generation_relation=_string(
            value, "deletion_generation_relation", context
        ),
        natural_key=_string_tuple(value, "natural_key", context),
        rule=_string(value, "rule", context),
    )


def _parse_analysis_resolution_contract(
    value: Mapping[str, Any],
) -> AnalysisResolutionContract:
    context = "contract.analysis_resolution_contract"
    return AnalysisResolutionContract(
        mode=_string(value, "mode", context),
        max_overlay_depth=_integer(value, "max_overlay_depth", context),
        baseline_relation=_string(value, "baseline_relation", context),
        anchor_relation=_string(value, "anchor_relation", context),
        ancestry_relation=_string(value, "ancestry_relation", context),
        seal_relation=_string(value, "seal_relation", context),
        snapshot_relation=_string(value, "snapshot_relation", context),
        spam_relation=_string(value, "spam_relation", context),
        content_owner_relation=_string(value, "content_owner_relation", context),
        gid_winner_relation=_string(value, "gid_winner_relation", context),
        immutable_fact_relations=_string_tuple(
            value, "immutable_fact_relations", context
        ),
        delta_relations=_string_tuple(value, "delta_relations", context),
        components=tuple(
            AnalysisStateComponent(
                name=_string(component, "name", f"{context}.component"),
                shadow_relation=_string(
                    component, "shadow_relation", f"{context}.component"
                ),
                tombstone_relation=_string(
                    component, "tombstone_relation", f"{context}.component"
                ),
                resolved_relation=_string(
                    component, "resolved_relation", f"{context}.component"
                ),
            )
            for component in _table_list(value, "component", context)
        ),
        initialization=_string(value, "initialization", context),
        snapshot_resolution=_string(value, "snapshot_resolution", context),
        delta_basis=_string(value, "delta_basis", context),
        read_resolution=_string(value, "read_resolution", context),
        compaction=_string(value, "compaction", context),
        compaction_ancestry=_string(value, "compaction_ancestry", context),
        cleanup_guard=_string(value, "cleanup_guard", context),
        cleanup_transition=_string(value, "cleanup_transition", context),
        stage_relation=_string(value, "stage_relation", context),
        checkpoint_relation=_string(value, "checkpoint_relation", context),
        batch_receipt_relation=_string(value, "batch_receipt_relation", context),
        cursor_codec_rule=_string(value, "cursor_codec_rule", context),
        batch_rule=_string(value, "batch_rule", context),
        batch_stages=tuple(
            AnalysisBatchStage(
                name=_string(stage, "name", f"{context}.batch_stage"),
                stage_order=_integer(stage, "stage_order", f"{context}.batch_stage"),
                cursor_codec=_string(stage, "cursor_codec", f"{context}.batch_stage"),
            )
            for stage in _table_list(value, "batch_stage", context)
        ),
    )


def _parse_fd(value: Mapping[str, Any], context: str) -> FunctionalDependency:
    return FunctionalDependency(
        frozenset(_string_tuple(value, "determinant", context)),
        frozenset(_string_tuple(value, "dependent", context)),
    )


def _parse_foreign_key(value: Mapping[str, Any], context: str) -> ForeignKey:
    return ForeignKey(
        _string_tuple(value, "attributes", context),
        _string(value, "relation", context),
        _string_tuple(value, "referenced_attributes", context),
    )


def _list(value: Mapping[str, Any], key: str, context: str) -> list[Any]:
    result = value.get(key)
    if not isinstance(result, list):
        raise ContractFormatError(f"{context}.{key} must be an array")
    return result


def _table_list(
    value: Mapping[str, Any], key: str, context: str
) -> list[Mapping[str, Any]]:
    result = _list(value, key, context)
    if not all(isinstance(item, dict) for item in result):
        raise ContractFormatError(f"{context}.{key} must contain tables")
    return result


def _optional_table_list(
    value: Mapping[str, Any], key: str, context: str
) -> list[Mapping[str, Any]]:
    if key not in value:
        return []
    return _table_list(value, key, context)


def _optional_table(
    value: Mapping[str, Any], key: str, context: str
) -> Mapping[str, Any] | None:
    result = value.get(key)
    if result is None:
        return None
    if not isinstance(result, dict):
        raise ContractFormatError(f"{context}.{key} must be a table")
    return result


def _table(value: Mapping[str, Any], key: str, context: str) -> Mapping[str, Any]:
    result = value.get(key)
    if not isinstance(result, dict):
        raise ContractFormatError(f"{context}.{key} must be a table")
    return result


def _string(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ContractFormatError(f"{context}.{key} must be a non-empty string")
    return result


def _integer(value: Mapping[str, Any], key: str, context: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise ContractFormatError(f"{context}.{key} must be an integer")
    return result


def _string_tuple(value: Mapping[str, Any], key: str, context: str) -> tuple[str, ...]:
    return tuple(_string_sequence(_list(value, key, context), f"{context}.{key}"))


def _string_sequence(value: Any, context: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ContractFormatError(f"{context} must be an array of non-empty strings")
    return tuple(value)


def _format_set(values: Iterable[str]) -> str:
    return "{" + ", ".join(sorted(values)) + "}"


def _sorted_sets(values: Iterable[frozenset[str]]) -> list[frozenset[str]]:
    return sorted(values, key=lambda value: (len(value), tuple(sorted(value))))


def _default_contract_path() -> Path:
    return Path(__file__).with_name("catalog.toml")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "contract",
        nargs="?",
        type=Path,
        default=_default_contract_path(),
        help="TOML contract (default: catalog.toml beside this checker)",
    )
    arguments = parser.parse_args(argv)
    try:
        contract = load_contract(arguments.contract)
        report = validate_contract(contract)
        companion_name = {
            "catalog_data_plane": "operational.toml",
            "operational_control_plane": "catalog.toml",
        }.get(contract.scope)
        if companion_name is not None:
            companion_path = arguments.contract.with_name(companion_name)
            if not companion_path.is_file():
                raise ContractValidationError(
                    [f"cross-manifest companion is missing: {companion_path}"]
                )
            companion = load_contract(companion_path)
            validate_contract(companion)
            if contract.scope == "catalog_data_plane":
                validate_cross_manifest_contracts(contract, companion)
            else:
                validate_cross_manifest_contracts(companion, contract)
    except ContractError as error:
        print(error, file=sys.stderr)
        return 1
    print(
        f"validated {len(report.relations)} BCNF relations and "
        f"{len(report.lossless_decompositions)} lossless decompositions "
        f"and {len(report.dependency_preserving_decompositions)} "
        "dependency-preserving decompositions "
        f"from {arguments.contract}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
