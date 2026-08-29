"""Physical refinement helpers for the independent operational contract.

This module intentionally wraps, rather than edits, the data-plane refinement
implementation.  External catalog relations are realized as minimal candidate-
key stubs in verification fixtures so operational foreign keys are exercised
without duplicating data-plane tables in operational.toml.
"""

from __future__ import annotations

import argparse
import codecs
import hashlib
import json
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

try:
    from verification.schema import refinement
except ModuleNotFoundError:  # Direct file loading in the repository test suite.
    import importlib.util
    import sys

    _spec = importlib.util.spec_from_file_location(
        "h2hdb_operational_base_refinement",
        Path(__file__).with_name("refinement.py"),
    )
    if _spec is None or _spec.loader is None:
        raise RuntimeError("cannot load schema refinement helper")
    refinement = importlib.util.module_from_spec(_spec)
    sys.modules[_spec.name] = refinement
    _spec.loader.exec_module(refinement)


@dataclass(frozen=True)
class ExternalStub:
    relation: str
    table: str
    columns: tuple[tuple[str, str, str], ...]
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...] = ()
    nullable_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class SemanticObligation:
    obligation_id: str
    version: int
    scope: str
    obligation_class: str
    lifecycle: str
    check: str
    hook: str
    relations: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class BootstrapCell:
    attribute: str
    value_type: str
    value: str | int


@dataclass(frozen=True)
class BootstrapSeed:
    seed_id: str
    version: int
    lifecycle: str
    relation: str
    cells: tuple[BootstrapCell, ...]
    invariant: str


@dataclass(frozen=True)
class BootstrapSeedRange:
    seed_id: str
    target_kind: str
    shard_start: int
    shard_end: int
    key_codec: str
    target_kind_tag: bytes


@dataclass(frozen=True)
class OperationalMachineContract:
    obligations: tuple[SemanticObligation, ...]
    seeds: tuple[BootstrapSeed, ...]
    seed_ranges: tuple[BootstrapSeedRange, ...]
    seeded_relations: tuple[str, ...]
    absent_relations: tuple[str, ...]
    epoch_owned_relation: str


@dataclass(frozen=True)
class PreparationCleanupFacts:
    state: str
    exclusive_gate_held: bool
    activation_present: bool


@dataclass(frozen=True)
class SourceBuildGenerationCleanupFacts:
    generation_completed: bool
    strictly_superseded: bool
    is_current_coordination_generation: bool
    owner_resume_authority: bool
    live_lease_resume_authority: bool
    handoff_resume_authority: bool


def operational_preparation_cleanup_eligible(facts: PreparationCleanupFacts) -> bool:
    """Closed-world runtime predicate; callers cannot supply SQL or relation names."""

    if not facts.exclusive_gate_held:
        return False
    if facts.state == "ABANDONED":
        return not facts.activation_present
    if facts.state == "COMPLETE":
        return facts.activation_present
    return False


def analysis_run_cleanup_eligible(
    *, retained_root_reachable: bool, active_head_provenance: bool
) -> bool:
    return not retained_root_reachable and not active_head_provenance


def source_build_generation_cleanup_eligible(
    facts: SourceBuildGenerationCleanupFacts,
) -> bool:
    return (
        (facts.generation_completed or facts.strictly_superseded)
        and not facts.is_current_coordination_generation
        and not facts.owner_resume_authority
        and not facts.live_lease_resume_authority
        and not facts.handoff_resume_authority
    )


def canonical_value_mutation_authorized(
    *,
    shared_maintenance_slot_held: bool,
    live_outer_owner_and_lease: bool,
    exact_upload_claim_locked: bool,
    cleanup_exclusive_cycle_active: bool,
) -> bool:
    """Exclude allocation/page/seal writes from a canonical cleanup cycle."""

    return (
        shared_maintenance_slot_held
        and live_outer_owner_and_lease
        and exact_upload_claim_locked
        and not cleanup_exclusive_cycle_active
    )


def canonical_value_upload_begin_authorized(
    *,
    digest_domain: str,
    current_head_owner_and_lease: bool,
    shared_maintenance_slot_held: bool,
    source_build_generation_mapping_present: bool,
) -> bool:
    """Permit only source_root_v1 to bootstrap the build mapping it enables."""

    return (
        current_head_owner_and_lease
        and shared_maintenance_slot_held
        and (
            source_build_generation_mapping_present or digest_domain == "source_root_v1"
        )
    )


def canonical_value_upload_handoff_authorized(
    *,
    digest_domain: str,
    current_head_owner_and_lease: bool,
    shared_maintenance_slot_held: bool,
    cleanup_exclusive_cycle_active: bool,
    final_identity_byte_validated: bool,
    retention_blocking_consumer_present_after_transaction: bool,
    source_build_generation_mapping_present_after_transaction: bool,
    source_scope_build_mapping_inserted_same_transaction: bool,
    exact_own_claim_locked: bool,
    own_claim_deleted: bool,
    retention_blocking_consumer_and_claim_change_same_transaction: bool,
) -> bool:
    """Release a claim only with its first retention-blocking consumer."""

    source_root_handoff_authorized = (
        digest_domain != "source_root_v1"
        or source_scope_build_mapping_inserted_same_transaction
    )
    return (
        current_head_owner_and_lease
        and shared_maintenance_slot_held
        and not cleanup_exclusive_cycle_active
        and final_identity_byte_validated
        and retention_blocking_consumer_present_after_transaction
        and source_build_generation_mapping_present_after_transaction
        and source_root_handoff_authorized
        and exact_own_claim_locked
        and own_claim_deleted
        and retention_blocking_consumer_and_claim_change_same_transaction
    )


def canonical_value_upload_cleanup_authorized(
    *,
    digest_domain: str,
    generation_completed_or_strictly_superseded: bool,
    current_or_live_generation: bool,
    source_build_generation_mapping_present: bool,
    exclusive_maintenance_gate_held: bool,
    exact_claim_and_allocation_locked: bool,
) -> bool:
    """Clean a stale upload, including a crashed pre-mapping source-root claim."""

    mapping_authorized = source_build_generation_mapping_present or (
        digest_domain == "source_root_v1"
    )
    return (
        generation_completed_or_strictly_superseded
        and not current_or_live_generation
        and mapping_authorized
        and exclusive_maintenance_gate_held
        and exact_claim_and_allocation_locked
    )


def ingest_generation_history_cleanup_authorized(
    *,
    strictly_older_than_current: bool,
    current_or_completed_head_reference: bool,
    build_upload_or_staging_reference: bool,
    owner_lease_or_handoff_resume_authority: bool,
    exclusive_maintenance_gate_held: bool,
    rows_selected: int,
    maximum_rows: int,
) -> bool:
    return (
        strictly_older_than_current
        and not current_or_completed_head_reference
        and not build_upload_or_staging_reference
        and not owner_lease_or_handoff_resume_authority
        and exclusive_maintenance_gate_held
        and 0 <= rows_selected <= maximum_rows
        and maximum_rows > 0
    )


def maintenance_generation_history_cleanup_authorized(
    *,
    non_head_generation: bool,
    owner_expired: bool,
    holder_slots_absent: bool,
    owner_or_head_reference_absent: bool,
    newer_live_exclusive_generation: bool,
    rows_selected: int,
    maximum_rows: int,
) -> bool:
    return (
        non_head_generation
        and owner_expired
        and holder_slots_absent
        and owner_or_head_reference_absent
        and newer_live_exclusive_generation
        and 0 <= rows_selected <= maximum_rows
        and maximum_rows > 0
    )


def hash_cache_observation_cleanup_eligible(
    *,
    observed_at: int,
    cycle_cutoff_at: int,
    max_age_microseconds: int,
    exclusive_gate_held: bool,
) -> bool:
    return (
        exclusive_gate_held
        and max_age_microseconds >= 0
        and cycle_cutoff_at >= max_age_microseconds
        and 0 <= observed_at <= cycle_cutoff_at - max_age_microseconds
    )


def cleanup_checkpoint_phase_matches(
    job_target_kind: str, phase_target_kind: str
) -> bool:
    return job_target_kind == phase_target_kind


def cleanup_may_resume(
    *,
    job_state: str,
    job_cleanup_id: bytes,
    job_cycle_generation: int,
    completion_cycle_generation: int | None,
    checkpoint_cleanup_id: bytes | None,
) -> bool:
    return cleanup_replay_state(
        job_state=job_state,
        job_cleanup_id=job_cleanup_id,
        job_cycle_generation=job_cycle_generation,
        completion_cycle_generation=completion_cycle_generation,
        checkpoint_cleanup_id=checkpoint_cleanup_id,
    ) in {"START", "RESUME"}


def cleanup_replay_state(
    *,
    job_state: str,
    job_cleanup_id: bytes,
    job_cycle_generation: int,
    completion_cycle_generation: int | None,
    checkpoint_cleanup_id: bytes | None,
) -> str:
    checkpoint_exists = checkpoint_cleanup_id is not None
    matching_checkpoint = checkpoint_cleanup_id == job_cleanup_id
    if job_state == "OPEN":
        if completion_cycle_generation is not None or (
            checkpoint_exists and not matching_checkpoint
        ):
            raise ValueError("cleanup replay state is corrupt or stale")
        return "RESUME" if checkpoint_exists else "START"
    if job_state == "COMPLETE":
        if checkpoint_exists or completion_cycle_generation != job_cycle_generation:
            raise ValueError("cleanup replay state is corrupt or incomplete")
        return "COMPLETE"
    raise ValueError("cleanup job state is not replayable")


def encode_cleanup_id(target_kind: str, shard_no: int, cycle_generation: int) -> bytes:
    """Injective 16-byte cycle fence; overflow and unregistered kinds fail closed."""

    if target_kind not in _CLEANUP_TARGET_SHAPES:
        raise ValueError("unregistered cleanup target kind")
    if (
        isinstance(shard_no, bool)
        or not isinstance(shard_no, int)
        or not 0 <= shard_no <= 255
    ):
        raise ValueError("cleanup shard must be in 0..255")
    if (
        isinstance(cycle_generation, bool)
        or not isinstance(cycle_generation, int)
        or not 1 <= cycle_generation < 2**63
    ):
        raise ValueError("cleanup cycle generation must be in 1..2^63-1")
    tag = hashlib.sha256(
        b"h2hdb-cleanup-cycle-v1\0" + target_kind.encode("ascii")
    ).digest()[:7]
    return tag + bytes((shard_no,)) + cycle_generation.to_bytes(8, "big")


def encode_cleanup_target_key(
    target_kind: str, values: tuple[bytes | int, ...]
) -> bytes:
    """Executable exact codecs used by version-one cleanup writer hooks."""

    shape = _CLEANUP_TARGET_SHAPES.get(target_kind)
    if shape is None:
        raise ValueError("unregistered cleanup target kind")
    codec = shape[2]
    if codec == "raw_sha256_v1":
        if len(values) != 1 or not isinstance(values[0], bytes) or len(values[0]) != 32:
            raise ValueError("raw_sha256_v1 requires one exact 32-byte digest")
        return values[0]
    tag = hashlib.sha256(
        b"h2hdb-cleanup-target-v1\0" + target_kind.encode("ascii")
    ).digest()[:16]
    if codec == "target_kind_tag16_uuid16_v1":
        if len(values) != 1 or not isinstance(values[0], bytes) or len(values[0]) != 16:
            raise ValueError("UUID cleanup codec requires one exact 16-byte identity")
        return tag + values[0]
    if codec == "target_kind_tag16_u64be_u64be_v1":
        if len(values) != 2:
            raise ValueError("observation cleanup codec requires two uint64 values")
        first, second = values
        if (
            not isinstance(first, int)
            or isinstance(first, bool)
            or not 0 <= first < 2**64
            or not isinstance(second, int)
            or isinstance(second, bool)
            or not 0 <= second < 2**64
        ):
            raise ValueError("observation cleanup codec requires two uint64 values")
        return tag + first.to_bytes(8, "big") + second.to_bytes(8, "big")
    if codec == "target_kind_tag16_u64be_zero8_v1":
        if (
            len(values) != 1
            or not isinstance(values[0], int)
            or isinstance(values[0], bool)
            or not 0 <= values[0] <= 255
        ):
            raise ValueError("sweep cleanup codec requires one shard in 0..255")
        return tag + values[0].to_bytes(8, "big") + bytes(8)
    raise ValueError("unregistered cleanup key codec")


_SEMANTIC_VALIDATOR_HOOK = (
    "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider.semantic_validators"
)
_OBLIGATION_BINDINGS = {
    "h2hdb.operational.physical-domains.v1": (
        "operational.physical-domains",
        "ready_validation",
        "physical_domain",
        "operational_refinement.check_physical_domains_v1",
    ),
    "h2hdb.operational.epoch-manifest.v1": (
        "operational.schema-epoch",
        "building_to_ready",
        "manifest_integrity",
        "operational_refinement.check_epoch_manifest_v1",
    ),
    "h2hdb.operational.fencing.v1": (
        "operational.ingest-fencing",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_fencing_contract_v1",
    ),
    "h2hdb.operational.download-ingest-handoff.v1": (
        "operational.download-ingest-handoff",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_download_ingest_handoff_contract_v1",
    ),
    "h2hdb.operational.maintenance-gate.v1": (
        "operational.maintenance-gate",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_maintenance_gate_contract_v1",
    ),
    "h2hdb.operational.bounded-work.v1": (
        "operational.bounded-work",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_bounded_work_contract_v1",
    ),
    "h2hdb.operational.queue-history.v1": (
        "operational.deletion-generation-history",
        "ready_and_runtime",
        "referential_protocol",
        "operational_refinement.check_queue_history_contract_v1",
    ),
    "h2hdb.operational.canonical-hash-cache.v1": (
        "operational.hash-cache",
        "ready_and_runtime",
        "canonical_digest",
        "operational_refinement.check_canonical_hash_cache_contract_v1",
    ),
    "h2hdb.operational.event-integrity.v1": (
        "operational.events",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_event_integrity_contract_v1",
    ),
    "h2hdb.operational.build-generation.v1": (
        "operational.source-build-reservation",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_build_generation_contract_v1",
    ),
    "h2hdb.operational.attempt-identity.v1": (
        "operational.retry-identity",
        "ready_and_runtime",
        "identity_protocol",
        "operational_refinement.check_attempt_identity_contract_v1",
    ),
    "h2hdb.operational.cleanup-reachability.v1": (
        "operational.cleanup-reachability",
        "ready_and_runtime",
        "retention_protocol",
        "operational_refinement.check_cleanup_reachability_v1",
    ),
    "h2hdb.operational.cleanup-frozen-root-set.v1": (
        "operational.cleanup-frozen-root-set",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_cleanup_frozen_root_set_v1",
    ),
    "h2hdb.operational.revision-allocation.v1": (
        "operational.revision-allocation",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_revision_allocator_contract_v1",
    ),
    "h2hdb.operational.gallery-staging.v1": (
        "operational.gallery-staging",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_gallery_staging_contract_v1",
    ),
    "h2hdb.operational.gallery-staging-request-budget.v1": (
        "operational.gallery-staging-request-retirement",
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_gallery_staging_request_budget_v1",
    ),
    "h2hdb.operational.bootstrap-genesis.v1": (
        "operational.bootstrap",
        "building_only",
        "bootstrap_integrity",
        "operational_refinement.check_bootstrap_contract_v1",
    ),
}

_GENERATION_OBLIGATION_RELATION_BINDINGS = {
    "h2hdb.operational.download-ingest-handoff.v1": (
        (
            "download_generation",
            "download_coordination_head",
            "download_generation_owner",
            "download_ingest_handoff",
            "download_ingest_consumption",
            "coordinated_ingest_completion",
            "ingest_generation",
            "ingest_coordination_head",
            "ingest_generation_owner",
        ),
        "Validate repository-issued download capabilities, exact live handoff or expired takeover, one-to-one ingest consumption, quiescent periodic ingest, coordinated completion, and zero-write exact response-loss replay across normalized download and ingest authority.",
    ),
    "h2hdb.operational.bounded-work.v1": (
        (
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_checkpoint",
            "operational_preparation_batch_receipt",
            "operational_preparation_effect_seal",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
            "cleanup_checkpoint",
            "cleanup_batch_receipt",
        ),
        "Validate server-owned cursors, bounded same-transaction typed effects with receipt/checkpoint CAS, an empty terminal receipt, and an exact immutable effect-completeness seal written before COMPLETE without a publication-time event scan.",
    ),
    "h2hdb.operational.queue-history.v1": (
        (
            "deletion_request_generation",
            "deletion_request_generation_head",
            "deletion_request_attempt",
            "deletion_request_head",
            "deletion_request_url",
            "operational_preparation",
            "operational_deletion_consumption_event",
        ),
        "Validate the real immutable generation-zero empty-queue genesis, exact history-backed singleton generation CAS, immutable deletion attempts, independently mutable per-gid heads, optional exact URL satellites, preparation generation authority, O(1) publication recheck, and consumption references to immutable attempts.",
    ),
    "h2hdb.operational.attempt-identity.v1": (
        (
            "cleanup_job",
            "operational_preparation",
            "operational_policy",
            "deletion_request_generation",
            "deletion_request_generation_head",
        ),
        "Validate monotone cleanup attempt numbers and policy-qualified immutable preparation identity backed by an exact retained deletion generation; publication accepts only the preparation matching the singleton current generation.",
    ),
    "h2hdb.operational.event-integrity.v1": (
        (
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_checkpoint",
            "operational_preparation_batch_receipt",
            "operational_preparation_effect_seal",
            "publication_candidate_preparation",
            "operational_activation",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
            "cleanup_job",
            "cleanup_cycle_root",
            "cleanup_checkpoint",
        ),
        "Bind generated event key shapes; full CHECK rejects effect seals without preparation or commit authority and streams without preparation or seal authority, and admits missing transient coordinates only for an exact frozen OPEN PUBLICATION_COMMIT covered prefix or compound receipt. Contiguous digest-chain, exact subtype, and writer-produced seal completeness remain obligations of bounded event writers and cleanup fault/integration evidence rather than an unbounded READY scan.",
    ),
    "h2hdb.operational.cleanup-reachability.v1": (
        (
            "cleanup_target_kind",
            "cleanup_phase",
            "cleanup_job",
            "cleanup_cycle_root",
            "cleanup_checkpoint",
            "source_build_descriptor",
            "source_build_base_publication_commit",
            "publication_candidate",
            "publication_commit",
            "analysis_snapshot_manifest",
            "source_revision",
            "catalog_revision",
            "canonical_value_identity",
            "content_blob",
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_checkpoint",
            "operational_preparation_batch_receipt",
            "operational_preparation_effect_seal",
            "publication_candidate_preparation",
            "operational_activation",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
        ),
        "Validate the exact seeded cleanup kind/phase registry, 32-byte target-key codecs, frozen-root membership, static writer hooks, retention-root closure, blocker identities, and candidate-to-preparation binding. Generic preparation cleanup retains COMPLETE retry and commit-to-build lineage; only the same unreachable publication-commit lifecycle may release its safe build base, exact-delete its binding and preparation control family, compact transient typed/events with exact cursor/receipt rechecks, and atomically delete commit, effect seal, and stream, while ABANDONED invisible streams leave no orphan.",
    ),
    "h2hdb.operational.cleanup-frozen-root-set.v1": (
        (
            "cleanup_job",
            "cleanup_cycle_root",
            "cleanup_checkpoint",
        ),
        "Validate the single-OPEN serialized cleanup pipeline, exact at-most-256 immutable per-cycle frozen root membership, canonical typed frame maximum derived as 260 bytes from every registered root physical domain, count/digest seal, static-phase membership restriction, and same-transaction terminal membership removal.",
    ),
    "h2hdb.operational.gallery-staging-request-budget.v1": (
        (
            "gallery_observation_staging_request_budget",
            "gallery_observation_staging",
            "gallery_observation_staging_claim",
            "gallery_observation_staging_checkpoint",
            "gallery_observation_staging_request",
            "gallery_observation_staging_request_chunk",
            "gallery_observation_staging_request_predecessor",
            "gallery_observation_staging_page_request",
            "gallery_observation_staging_request_page",
            "gallery_observation_staging_receipt",
            "gallery_observation_staging_frontier",
            "gallery_observation_staging_match_checkpoint",
            "gallery_observation_staging_match_request",
            "gallery_observation_staging_match_receipt",
            "gallery_observation_staging_metadata_parser",
            "source_working_build",
            "source_build_gallery",
        ),
        "Validate the seeded exact request-budget singleton, replay-neutral reserve and actual-delete release accounting under one lock order, the 1,500,000-row emergency cap, one active staging slot per build, and shared-fenced seven-phase implicit-ACK retirement with durable-link replay and terminal generic-cleanup backstop.",
    ),
    "h2hdb.operational.bootstrap-genesis.v1": (
        (
            "revision_allocator",
            "identity_allocator",
            "deletion_request_generation",
            "deletion_request_generation_head",
            "gallery_observation_staging_request_budget",
        ),
        "Validate the exact typed SOURCE/CATALOG revision and GALLERY/TAG/POLICY identity allocator genesis rows, the real immutable deletion generation-zero empty-queue fact and its singleton head, the zero-valued request-budget singleton, and the declared absence of all request, event, lease, staging, work, cache, policy, and cleanup facts.",
    ),
}


def validate_operational_machine_contract(
    logical_path: str | Path, physical_path: str | Path
) -> OperationalMachineContract:
    """Validate machine obligations, protocol tables, and exact genesis rows."""

    with Path(logical_path).open("rb") as stream:
        logical = tomllib.load(stream)
    with Path(physical_path).open("rb") as stream:
        physical = tomllib.load(stream)
    return validate_operational_machine_contract_documents(logical, physical)


def validate_operational_machine_contract_documents(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> OperationalMachineContract:
    """Document-level entry point used by drift and adversarial tests."""

    logical_relations = _raw_relation_map(logical)
    physical_relations = _raw_relation_map(physical)
    external_names = {
        _required_text(value, "name", "external_relation")
        for value in _raw_tables(
            logical.get("external_relation", []), "external_relation"
        )
    }
    obligations: list[SemanticObligation] = []
    raw_obligations = _raw_tables(
        logical.get("semantic_obligation", []), "semantic_obligation"
    )
    for raw in raw_obligations:
        context = "semantic obligation"
        if set(raw) != {
            "id",
            "version",
            "ready_check",
            "writer_hook",
            "writer_hook_version",
            "scope",
            "class",
            "lifecycle",
            "check",
            "hook",
            "relations",
            "description",
        }:
            raise ValueError("semantic obligation fields are not closed-world")
        obligation_id = _required_text(raw, "id", context)
        version = _positive_int(raw.get("version"), f"{obligation_id}.version")
        scope = _required_text(raw, "scope", context)
        obligation_class = _required_text(raw, "class", context)
        lifecycle = _required_text(raw, "lifecycle", context)
        check = _required_text(raw, "check", context)
        hook = _required_text(raw, "hook", context)
        relations = _required_texts(raw, "relations", context)
        description = _required_text(raw, "description", context)
        expected = _OBLIGATION_BINDINGS.get(obligation_id)
        if version != 1 or expected != (
            scope,
            lifecycle,
            obligation_class,
            check,
        ):
            raise ValueError(
                f"semantic obligation {obligation_id!r} has an unregistered "
                "version/scope/lifecycle/class/check binding"
            )
        if hook != _SEMANTIC_VALIDATOR_HOOK:
            raise ValueError(
                f"semantic obligation {obligation_id!r} has the wrong runtime hook"
            )
        if raw.get("ready_check") != check or raw.get("writer_hook_version") != 1:
            raise ValueError(
                f"semantic obligation {obligation_id!r} executable version drifts"
            )
        _required_text(raw, "writer_hook", context)
        unknown = set(relations) - set(logical_relations) - external_names
        if unknown:
            raise ValueError(
                f"semantic obligation {obligation_id!r} references unknown relations"
            )
        generation_binding = _GENERATION_OBLIGATION_RELATION_BINDINGS.get(obligation_id)
        if generation_binding is not None and generation_binding != (
            relations,
            description,
        ):
            raise ValueError(
                f"semantic obligation {obligation_id!r} generation relation or "
                "description binding drifts"
            )
        obligations.append(
            SemanticObligation(
                obligation_id,
                version,
                scope,
                obligation_class,
                lifecycle,
                check,
                hook,
                relations,
                description,
            )
        )
    ids = [value.obligation_id for value in obligations]
    if len(ids) != len(set(ids)) or set(ids) != set(_OBLIGATION_BINDINGS):
        raise ValueError("semantic obligation IDs are duplicate or incomplete")
    if physical.get("semantic_obligation") != logical.get("semantic_obligation"):
        raise ValueError("physical semantic obligations drift from logical contract")

    seeds, seed_ranges, seeded_relations, absent_relations, epoch_owned = (
        _validate_bootstrap(logical, physical, logical_relations)
    )
    checks: dict[str, Callable[[Mapping[str, Any], Mapping[str, Any]], None]] = {
        "operational_refinement.check_physical_domains_v1": check_physical_domains_v1,
        "operational_refinement.check_epoch_manifest_v1": check_epoch_manifest_v1,
        "operational_refinement.check_fencing_contract_v1": check_fencing_contract_v1,
        "operational_refinement.check_download_ingest_handoff_contract_v1": check_download_ingest_handoff_contract_v1,
        "operational_refinement.check_maintenance_gate_contract_v1": check_maintenance_gate_contract_v1,
        "operational_refinement.check_bounded_work_contract_v1": check_bounded_work_contract_v1,
        "operational_refinement.check_queue_history_contract_v1": check_queue_history_contract_v1,
        "operational_refinement.check_canonical_hash_cache_contract_v1": check_canonical_hash_cache_contract_v1,
        "operational_refinement.check_event_integrity_contract_v1": check_event_integrity_contract_v1,
        "operational_refinement.check_build_generation_contract_v1": check_build_generation_contract_v1,
        "operational_refinement.check_attempt_identity_contract_v1": check_attempt_identity_contract_v1,
        "operational_refinement.check_cleanup_reachability_v1": check_cleanup_reachability_v1,
        "operational_refinement.check_cleanup_frozen_root_set_v1": check_cleanup_frozen_root_set_v1,
        "operational_refinement.check_revision_allocator_contract_v1": check_revision_allocator_contract_v1,
        "operational_refinement.check_gallery_staging_contract_v1": check_gallery_staging_contract_v1,
        "operational_refinement.check_gallery_staging_request_budget_v1": check_gallery_staging_request_budget_v1,
        "operational_refinement.check_bootstrap_contract_v1": check_bootstrap_contract_v1,
    }
    if set(checks) != {value.check for value in obligations}:
        raise ValueError("semantic obligation check registry is incomplete or unused")
    for obligation in obligations:
        checks[obligation.check](logical, physical)
    validate_operational_fk_access_paths(physical)
    if set(physical_relations) != set(logical_relations):
        raise ValueError("operational physical relation coverage drifts from logical")
    return OperationalMachineContract(
        tuple(obligations),
        seeds,
        seed_ranges,
        seeded_relations,
        absent_relations,
        epoch_owned,
    )


def check_fencing_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    expected = {
        "model": "verification/tla/CatalogCore.tla",
        "head_relation": "ingest_coordination_head",
        "generation_relation": "ingest_generation",
        "owner_relation": "ingest_generation_owner",
        "build_authorization_relation": "source_build_generation",
        "authorization_rule": "current_generation_and_exact_owner_and_unexpired_lease",
        "takeover_rule": "strictly_greater_generation",
        "stale_rule": "no_mutation",
        "history_cleanup_rule": "under the exclusive maintenance gate, keyset-delete at most the fixed batch bound of ingest generations strictly older than the current head only after source_build_generation, canonical_value_upload, staging claims, handoff, and every live owner row carrying lease resume authority are absent; delete handoff, owner, then generation child-first with row-lock recheck, while current and completed head references always block",
    }
    _require_exact_table(logical, "fencing_contract", expected)
    owner = _raw_relation_map(logical).get("ingest_generation_owner")
    if owner is None or {
        "attributes": owner.get("attributes"),
        "declared_keys": owner.get("declared_keys"),
        "fds": owner.get("fds"),
        "foreign_keys": owner.get("foreign_keys", []),
    } != {
        "attributes": [
            "generation",
            "owner_token",
            "claimed_at",
            "lease_expires_at",
        ],
        "declared_keys": [["generation"], ["owner_token"]],
        "fds": [
            {
                "determinant": ["generation"],
                "dependent": ["owner_token", "claimed_at", "lease_expires_at"],
            },
            {
                "determinant": ["owner_token"],
                "dependent": ["generation", "claimed_at", "lease_expires_at"],
            },
        ],
        "foreign_keys": [
            {
                "attributes": ["generation"],
                "relation": "ingest_generation",
                "referenced_attributes": ["generation"],
            }
        ],
    }:
        raise ValueError("ingest authority must be one complete BCNF owner row")


def check_download_ingest_handoff_contract_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    expected = {
        "version": 1,
        "download_generation_relation": "download_generation",
        "download_head_relation": "download_coordination_head",
        "download_owner_relation": "download_generation_owner",
        "handoff_relation": "download_ingest_handoff",
        "consumption_relation": "download_ingest_consumption",
        "completion_relation": "coordinated_ingest_completion",
        "ingest_generation_relation": "ingest_generation",
        "capability_bytes": 16,
        "handoff_kinds": ["DOWNLOADER", "EXPIRED_TAKEOVER"],
        "lock_order_rule": "every transaction locks the download singleton and exact current download generation satellites before the ingest singleton and exact current ingest generation satellites; each authority transition uses an exact observed-state CAS, and stale or corrupt authority performs zero writes",
        "download_claim_rule": "the repository issues a fresh opaque 16-byte owner capability, creates a strictly greater download generation only while both download and ingest heads are quiescent, inserts one exact owner row carrying claimed_at and lease_expires_at, and CAS-advances the download current head; no new download generation may begin until the linked ingest completion has durably completed the prior download generation and advanced completed_generation",
        "handoff_rule": "only the exact current downloader owner row with an unexpired lease_expires_at may insert DOWNLOADER handoff; when its lease is expired, ingest may fail-closed insert EXPIRED_TAKEOVER; either transaction atomically copies the exact owner_token into immutable handoff history and deletes the complete owner row so no live downloader authority remains",
        "consumption_rule": "one immutable handoff is consumed by exactly one immutable ingest generation and each ingest generation consumes at most one handoff; handoff validation, ingest claim, and consumption insert commit in one transaction, and an existing different tuple rejects without mutation",
        "periodic_rule": "a periodic ingest generation is allowed only while download authority is quiescent and has no handoff or consumption claim; it has no fabricated download linkage, and its completion never changes the download generation or head",
        "completion_rule": "linked completion atomically inserts the exact durable coordinated completion receipt, completes the live ingest fence, sets download_generation.completed_at, and exact-CAS advances download_coordination_head.completed_generation; periodic completion inserts the same ingest-owner receipt and completes only the ingest fence",
        "replay_rule": "response-loss replay exact-compares every retained handoff, consumption, and coordinated completion field including handoff kind, both generations, owner capabilities, and timestamps; an exact replay performs zero writes and every mismatch fails closed",
    }
    _require_exact_table(logical, "download_ingest_handoff_contract", expected)
    relations = _raw_relation_map(logical)
    shapes = {
        "download_generation": (
            ["generation", "started_at", "completed_at"],
            [["generation"]],
            [
                {
                    "determinant": ["generation"],
                    "dependent": ["started_at", "completed_at"],
                }
            ],
            [],
        ),
        "download_coordination_head": (
            [
                "singleton_id",
                "current_generation",
                "completed_generation",
                "last_transition_at",
            ],
            [["singleton_id"]],
            [
                {
                    "determinant": ["singleton_id"],
                    "dependent": [
                        "current_generation",
                        "completed_generation",
                        "last_transition_at",
                    ],
                }
            ],
            [
                {
                    "attributes": ["current_generation"],
                    "relation": "download_generation",
                    "referenced_attributes": ["generation"],
                },
                {
                    "attributes": ["completed_generation"],
                    "relation": "download_generation",
                    "referenced_attributes": ["generation"],
                },
            ],
        ),
        "download_generation_owner": (
            ["generation", "owner_token", "claimed_at", "lease_expires_at"],
            [["generation"], ["owner_token"]],
            [
                {
                    "determinant": ["generation"],
                    "dependent": [
                        "owner_token",
                        "claimed_at",
                        "lease_expires_at",
                    ],
                },
                {
                    "determinant": ["owner_token"],
                    "dependent": [
                        "generation",
                        "claimed_at",
                        "lease_expires_at",
                    ],
                },
            ],
            [
                {
                    "attributes": ["generation"],
                    "relation": "download_generation",
                    "referenced_attributes": ["generation"],
                }
            ],
        ),
        "download_ingest_handoff": (
            ["download_generation", "owner_token", "handoff_kind", "requested_at"],
            [["download_generation"], ["owner_token"]],
            [
                {
                    "determinant": ["download_generation"],
                    "dependent": ["owner_token", "handoff_kind", "requested_at"],
                },
                {
                    "determinant": ["owner_token"],
                    "dependent": [
                        "download_generation",
                        "handoff_kind",
                        "requested_at",
                    ],
                },
            ],
            [
                {
                    "attributes": ["download_generation"],
                    "relation": "download_generation",
                    "referenced_attributes": ["generation"],
                }
            ],
        ),
        "download_ingest_consumption": (
            ["download_generation", "ingest_generation", "consumed_at"],
            [["download_generation"], ["ingest_generation"]],
            [
                {
                    "determinant": ["download_generation"],
                    "dependent": ["ingest_generation", "consumed_at"],
                },
                {
                    "determinant": ["ingest_generation"],
                    "dependent": ["download_generation", "consumed_at"],
                },
            ],
            [
                {
                    "attributes": ["download_generation"],
                    "relation": "download_ingest_handoff",
                    "referenced_attributes": ["download_generation"],
                },
                {
                    "attributes": ["ingest_generation"],
                    "relation": "ingest_generation",
                    "referenced_attributes": ["generation"],
                },
            ],
        ),
        "coordinated_ingest_completion": (
            ["ingest_generation", "owner_token", "completed_at"],
            [["ingest_generation"], ["owner_token"]],
            [
                {
                    "determinant": ["ingest_generation"],
                    "dependent": ["owner_token", "completed_at"],
                },
                {
                    "determinant": ["owner_token"],
                    "dependent": ["ingest_generation", "completed_at"],
                },
            ],
            [
                {
                    "attributes": ["ingest_generation"],
                    "relation": "ingest_generation",
                    "referenced_attributes": ["generation"],
                }
            ],
        ),
    }
    for relation_name, (attributes, keys, fds, foreign_keys) in shapes.items():
        relation = relations.get(relation_name)
        if relation is None or {
            "attributes": relation.get("attributes"),
            "declared_keys": relation.get("declared_keys"),
            "fds": relation.get("fds"),
            "foreign_keys": relation.get("foreign_keys", []),
        } != {
            "attributes": attributes,
            "declared_keys": keys,
            "fds": fds,
            "foreign_keys": foreign_keys,
        }:
            raise ValueError(
                f"download_ingest_handoff_contract relation {relation_name} drifts"
            )

    physical_relations = _raw_relation_map(physical)
    handoff_checks = physical_relations["download_ingest_handoff"].get("check", [])
    if not any(
        value.get("name") == "ck_download_ingest_handoff_kind"
        and value.get("sqlite_expression")
        == "handoff_kind IN ('DOWNLOADER', 'EXPIRED_TAKEOVER')"
        and value.get("mariadb_expression")
        == "handoff_kind IN ('DOWNLOADER', 'EXPIRED_TAKEOVER')"
        for value in handoff_checks
        if isinstance(value, dict)
    ):
        raise ValueError("download_ingest_handoff_contract physical enum drifts")


def check_maintenance_gate_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    expected = {
        "slot_count": 64,
        "head_relation": "maintenance_gate_head",
        "generation_relation": "maintenance_gate_generation",
        "owner_relation": "maintenance_gate_owner",
        "holder_relation": "maintenance_gate_holder",
        "authorization_rule": "exact_holder_owner_and_owner_generation_equals_head_generation_and_unexpired_owner_lease",
        "shared_claim_rule": "current_shared_owner_holds_exactly_one_slot",
        "exclusive_claim_rule": "current_exclusive_owner_holds_every_slot_zero_through_sixty_three",
        "reclaim_rule": "replace_a_slot_only_by_transactional_cas_on_the_exact_observed_owner_after_its_generation_is_stale_or_its_lease_is_expired",
        "stale_rule": "stale_generation_or_expired_owner_authorizes_no_mutation",
        "canonical_value_rule": "every canonical-value allocation, upload-claim, bounded page, and final-seal transaction holds and rechecks one shared maintenance slot; CANONICAL_VALUE cleanup holds the exclusive generation for its complete multi-phase cycle and rechecks current-head, live source-working analysis, live or uncommitted publication-candidate, and upload semantic pins before every bounded destructive batch, so no live producer can claim, write, reseal, or lose its canonical snapshot between phases",
        "history_cleanup_rule": "under a newer live exclusive gate generation, keyset-delete at most the fixed batch bound of expired non-head owners after their holder slots are absent, then delete an unreferenced non-head maintenance generation only after no owner or head references it; every batch row-locks and rechecks head, owner lease, and exact generation, so current or live shared and exclusive authority always blocks",
    }
    _require_exact_table(logical, "maintenance_gate_contract", expected)
    holder = _raw_relation_map(logical)["maintenance_gate_holder"]
    if holder.get("declared_keys") != [["slot"]]:
        raise ValueError("maintenance holder must retain slot as its sole key")


def check_bounded_work_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    expected = {
        "cursor_authority": "server_owned_checkpoint",
        "page_commit": "decision_rows_and_receipt_and_checkpoint_cas_in_one_transaction",
        "terminal_rule": "empty_page_receipt_before_complete",
        "preparation_policy_relation": "operational_policy",
        "preparation_stream_relation": "operational_event_stream",
        "preparation_checkpoint_relation": "operational_preparation_checkpoint",
        "preparation_receipt_relation": "operational_preparation_batch_receipt",
        "preparation_effect_seal_relation": "operational_preparation_effect_seal",
        "preparation_event_relation": "operational_event",
        "preparation_effect_rule": "each bounded page locks the OPEN preparation and exact checkpoint generation, writes a contiguous coordinate range of base events and exactly one matching typed subtype per event, recomputes every event digest and the running chain, and commits those rows with the request receipt and checkpoint CAS; an empty terminal page proves no remaining work, after which one transaction inserts the exact effect seal last and marks the preparation COMPLETE without scanning prior event rows",
        "cleanup_policy_relation": "cleanup_job",
        "cleanup_checkpoint_relation": "cleanup_checkpoint",
        "cleanup_receipt_relation": "cleanup_batch_receipt",
    }
    _require_exact_table(logical, "bounded_work_contract", expected)
    relations = _raw_relation_map(logical)
    expected_keys = {
        "operational_preparation_checkpoint": [["preparation_id", "phase"]],
        "cleanup_checkpoint": [["cleanup_id", "phase"]],
    }
    for relation_name, keys in expected_keys.items():
        if relations[relation_name].get("declared_keys") != keys:
            raise ValueError(f"{relation_name} checkpoint authority key drift")
    seal = relations.get("operational_preparation_effect_seal")
    if seal is None or {
        "attributes": seal.get("attributes"),
        "declared_keys": seal.get("declared_keys"),
        "fds": seal.get("fds"),
    } != {
        "attributes": [
            "preparation_id",
            "event_count",
            "final_chain_sha256",
            "sealed_at",
        ],
        "declared_keys": [["preparation_id"]],
        "fds": [
            {
                "determinant": ["preparation_id"],
                "dependent": ["event_count", "final_chain_sha256", "sealed_at"],
            }
        ],
    }:
        raise ValueError("bounded operational effect completeness seal shape drifts")


def check_queue_history_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    expected = {
        "deletion_generation_relation": "deletion_request_generation",
        "deletion_generation_head_relation": "deletion_request_generation_head",
        "deletion_attempt_relation": "deletion_request_attempt",
        "deletion_head_relation": "deletion_request_head",
        "deletion_url_relation": "deletion_request_url",
        "consumption_relation": "operational_deletion_consumption_event",
        "preparation_relation": "operational_preparation",
        "generation_rule": "generation zero is the real immutable empty-queue genesis fact, never a sentinel; every actual deletion-request authority mutation transactionally inserts exactly current_generation plus one into immutable history, applies the queue mutation, and exact-CAS advances the singleton head from the observed generation, while exhaustion at 2^63-1 fails closed",
        "publication_rule": "preparation records the exact FK-backed deletion generation it scanned; publication locks and compares that generation with the singleton head in O(1) and performs no activation when they differ",
        "retention_rule": "deletion generation history is retained indefinitely as immutable FK and audit authority; the current head and every preparation always reference an exact history row",
        "rule": "the per-gid current head may rotate or disappear without deleting immutable attempts referenced by consumption events; URL is an optional exact payload satellite, and every such authority mutation participates in the global generation insert-and-CAS transaction",
    }
    _require_exact_table(logical, "queue_history_contract", expected)
    relations = _raw_relation_map(logical)
    generation = relations.get("deletion_request_generation")
    generation_head = relations.get("deletion_request_generation_head")
    preparation = relations.get("operational_preparation")
    if generation is None or {
        "attributes": generation.get("attributes"),
        "declared_keys": generation.get("declared_keys"),
        "fds": generation.get("fds"),
    } != {
        "attributes": ["generation", "allocated_at"],
        "declared_keys": [["generation"]],
        "fds": [{"determinant": ["generation"], "dependent": ["allocated_at"]}],
    }:
        raise ValueError("deletion generation history BCNF shape drifts")
    if generation_head is None or {
        "attributes": generation_head.get("attributes"),
        "declared_keys": generation_head.get("declared_keys"),
        "fds": generation_head.get("fds"),
    } != {
        "attributes": ["singleton_id", "current_generation", "updated_at"],
        "declared_keys": [["singleton_id"]],
        "fds": [
            {
                "determinant": ["singleton_id"],
                "dependent": ["current_generation", "updated_at"],
            }
        ],
    }:
        raise ValueError("deletion generation singleton head BCNF shape drifts")
    exact_head_fk = {
        "attributes": ["current_generation"],
        "relation": "deletion_request_generation",
        "referenced_attributes": ["generation"],
    }
    if exact_head_fk not in generation_head.get("foreign_keys", []):
        raise ValueError("deletion generation head lacks exact history FK")
    exact_preparation_fk = {
        "attributes": ["deletion_request_generation"],
        "relation": "deletion_request_generation",
        "referenced_attributes": ["generation"],
    }
    if preparation is None or exact_preparation_fk not in preparation.get(
        "foreign_keys", []
    ):
        raise ValueError("operational preparation lacks exact deletion generation FK")


def check_canonical_hash_cache_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    value = logical.get("canonical_hash_cache_contract")
    if not isinstance(value, dict):
        raise ValueError("canonical_hash_cache_contract must be a table")
    expected_pairs = {
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
        "source_domain": "filesystem_source_identity_v1",
        "fingerprint_domain": "filesystem_fingerprint_v1",
    }
    for key, expected in expected_pairs.items():
        if value.get(key) != expected:
            raise ValueError(f"canonical hash-cache contract {key} drifts")
    for key in ("write_obligation", "read_obligation"):
        if not isinstance(value.get(key), str) or not value[key]:
            raise ValueError(f"canonical hash-cache contract {key} is absent")
    exact_write = "under an exact live canonical_value_upload claim, use the registered distinct digest_domain, stream the exact versioned canonical preimage through bounded canonical_value_page rows, recompute SHA-256 and byte_count, reject every mismatch, and on digest or page conflict byte-compare the complete streamed page tree before accepting final canonical_value_identity; no database surrogate participates"
    if value.get("write_obligation") != exact_write:
        raise ValueError("canonical hash-cache stable digest-domain codec drifts")
    exact_read = "join each digest to final canonical_value_identity and canonical_value_allocation, validate the expected registered domain and byte_count, stream and recompute the exact root page tree, and never accept a digest without that complete exact preimage authority"
    if value.get("read_obligation") != exact_read:
        raise ValueError("canonical hash-cache streamed read authority drifts")


def check_event_integrity_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    expected = {
        "stream_relation": "operational_event_stream",
        "preparation_relation": "operational_preparation",
        "seal_relation": "operational_preparation_effect_seal",
        "activation_relation": "operational_activation",
        "candidate_binding_relation": "publication_candidate_preparation",
        "base_relation": "operational_event",
        "removed_subtype_relation": "operational_removed_gid_event",
        "deletion_subtype_relation": "operational_deletion_consumption_event",
        "removed_event_type": "REMOVED_GID",
        "deletion_event_type": "DELETION_CONSUMPTION",
        "event_digest_codec": "SHA256(ASCII(h2hdb-operational-event-v1) || NUL || preparation_id[16] || u64be(sequence_no) || u16be(event_type_byte_count) || ASCII(event_type) || u32be(subtype_frame_byte_count) || subtype_frame), where REMOVED_GID subtype_frame is u64be(gid) || request_token[16] and DELETION_CONSUMPTION subtype_frame is u64be(gid) || deletion_request_token[16]",
        "chain_codec": "chain_0 = SHA256(ASCII(h2hdb-operational-event-chain-v1) || NUL); chain_(n+1) = SHA256(ASCII(h2hdb-operational-event-chain-link-v1) || NUL || chain_n[32] || event_sha256_n[32])",
        "empty_chain_sha256": "e3963ad6e07ac045502ad95ddb3805ac57deea8ffbb038ddf7c538a816301e71",
        "stream_rule": "begin preparation resolves or allocates one preparation_id from the policy-qualified natural key, then inserts the durable event stream, preparation, and required initial checkpoints in one transaction; commit contains all roots or none, retry resolves the same preparation, and no standalone invisible stream is permitted",
        "subtype_rule": "every bounded event-page transaction inserts each base event and exactly one subtype whose type and exact canonical subtype frame agree with event_type, byte-compares an existing coordinate on retry, and advances the durable running chain only in the same receipt and checkpoint CAS",
        "seal_rule": "after an empty terminal work receipt, insert exactly one immutable seal with event_count equal to the next contiguous sequence number and final_chain_sha256 equal to the durable running chain, then mark the preparation COMPLETE in the same transaction; sequence numbers are exactly zero through event_count minus one, every event digest and subtype are exact, zero events require no event rows and the registered empty-chain digest, and publication trusts only this writer-produced seal without scanning events. The sole retirement exception is a matching frozen OPEN PUBLICATION_COMMIT cleanup whose compound EVENT phase atomically deletes each exactly type-matching subtype before its base event; only complete subtype/event coordinates covered by its durable receipt_id/preparation_id/sequence_no cursor may be absent, and the exception ends with the atomic COMMIT_EFFECT_ROOT receipt",
        "activation_rule": "one short publication transaction locks the exact COMPLETE preparation, its effect seal, matching operational policy, and singleton deletion-generation head; only when the preparation generation is current does it insert the complete immutable publication_commit row last, while reading or writing no event rows; operational_activation is a read-only projection of that one wide commit authority and readers expose an event only by joining its preparation_id through the derived activation",
        "candidate_binding_rule": "before publication_candidate may become SEALED, one transaction binds it to exactly one COMPLETE preparation and exact effect seal through publication_candidate_preparation; candidate_id and preparation_id are both candidate keys, both FKs are exact, and the preparation row pins the operational policy and retained deletion generation; final publication must use this bound preparation and must not search by build/generation/policy, LIMIT 2, caller identifier, or incidental uniqueness",
        "event_lifecycle_rule": "operational events are publication-owned transient current/retry control, not a durable delivery log: only an exact active or replayable preparation/candidate/commit family retains them, no consumer registry or acknowledgement relation exists, and an unreachable finalized non-head publication_commit cleanup removes both typed subtypes and base events before atomically removing commit, effect seal, and stream",
        "cleanup_rule": "generic OPERATIONAL_PREPARATION cleanup retains every COMPLETE preparation, including uncommitted retry authority and the commit-to-build lineage of current or replayable publication; only an unreachable finalized publication_commit cleanup under the exact live EXCLUSIVE gate may release its safe source-build base pin, remove its exact candidate/preparation binding and COMPLETE control root, delete both typed subtypes and base events child-first, then atomically delete commit, effect seal, and stream before checkpoint and anchor cleanup; an ABANDONED preparation with no commit authority is deleted child-first through subtypes, events, seal, preparation, and stream; OPEN and FAILED are retained and no invisible orphan stream may remain",
    }
    _require_exact_table(logical, "operational_event_integrity_contract", expected)
    relations = _raw_relation_map(logical)
    exact_shapes = {
        "operational_event_stream": (
            ["preparation_id", "created_at"],
            [["preparation_id"]],
        ),
        "operational_preparation_effect_seal": (
            ["preparation_id", "event_count", "final_chain_sha256", "sealed_at"],
            [["preparation_id"]],
        ),
        "publication_candidate_preparation": (
            ["candidate_id", "preparation_id", "bound_at"],
            [["candidate_id"], ["preparation_id"]],
        ),
        "operational_activation": (
            [
                "source_revision",
                "preparation_id",
                "operational_policy_id",
                "activated_at",
            ],
            [["source_revision"], ["preparation_id"]],
        ),
        "operational_event": (
            [
                "event_id",
                "preparation_id",
                "sequence_no",
                "event_type",
                "event_sha256",
                "created_at",
            ],
            [["event_id"], ["preparation_id", "sequence_no"]],
        ),
    }
    for relation_name, (attributes, keys) in exact_shapes.items():
        relation = relations.get(relation_name)
        if (
            relation is None
            or relation.get("attributes") != attributes
            or relation.get("declared_keys") != keys
        ):
            raise ValueError(
                f"operational event relation {relation_name} coordinate shape drifts"
            )
    expected_fks = {
        "operational_preparation": {
            "attributes": ["preparation_id"],
            "relation": "operational_event_stream",
            "referenced_attributes": ["preparation_id"],
        },
        "operational_preparation_effect_seal": {
            "attributes": ["preparation_id"],
            "relation": "operational_event_stream",
            "referenced_attributes": ["preparation_id"],
        },
        "operational_activation": {
            "attributes": ["preparation_id"],
            "relation": "publication_commit",
            "referenced_attributes": ["preparation_id"],
        },
        "publication_candidate_preparation": {
            "attributes": ["preparation_id"],
            "relation": "operational_preparation_effect_seal",
            "referenced_attributes": ["preparation_id"],
        },
        "operational_event": {
            "attributes": ["preparation_id"],
            "relation": "operational_event_stream",
            "referenced_attributes": ["preparation_id"],
        },
    }
    for relation_name, foreign_key in expected_fks.items():
        if foreign_key not in relations[relation_name].get("foreign_keys", []):
            raise ValueError(
                f"operational event relation {relation_name} lacks exact effect FK"
            )
    if "source_revision" in relations["operational_event"].get("attributes", []):
        raise ValueError("operational event still uses the publication coordinate")


def check_build_generation_contract_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    _require_exact_table(
        logical,
        "source_build_generation_contract",
        {
            "reservation_relation": "source_build_generation",
            "rule": "first begin or resume locks and verifies the exact current ingest head, matching owner row, and its unexpired lease_expires_at, then transactionally reserves at most one build for that immutable ingest generation; a strictly greater live takeover generation may reserve the same build, a no-build generation has no row, and the mapping may outlive deletion of its completed owner because authorization is a writer check rather than an FK",
            "cleanup_rule": "before SOURCE_BUILD cleanup creates a job and before every destructive batch, each mapped generation must be durably completed or strictly superseded by the current coordination head; an unfinished current generation or any owner row carrying lease authority or handoff resume authority blocks phase one, while completed or superseded mappings and their residual canonical uploads are bounded owned children and make progress",
        },
    )
    reservation = _raw_relation_map(logical)["source_build_generation"]
    immutable_generation_fk = {
        "attributes": ["generation"],
        "relation": "ingest_generation",
        "referenced_attributes": ["generation"],
    }
    ephemeral_owner_fk = {
        "attributes": ["generation"],
        "relation": "ingest_generation_owner",
        "referenced_attributes": ["generation"],
    }
    if immutable_generation_fk not in reservation.get(
        "foreign_keys", []
    ) or ephemeral_owner_fk in reservation.get("foreign_keys", []):
        raise ValueError(
            "source-build generation must retain immutable history, not owner liveness"
        )
    upload = _raw_relation_map(logical)["canonical_value_upload"]
    if immutable_generation_fk not in upload.get(
        "foreign_keys", []
    ) or ephemeral_owner_fk in upload.get("foreign_keys", []):
        raise ValueError(
            "canonical upload must retain immutable history, not owner liveness"
        )
    assembly = logical.get("source_build_assembly_contract")
    expected_assembly_core = {
        "version": 1,
        "discovery_checkpoint_relation": "source_build_discovery_checkpoint",
        "discovery_receipt_relation": "source_build_discovery_batch_receipt",
        "assembly_checkpoint_relation": "source_build_assembly_checkpoint",
        "assembly_receipt_relation": "source_build_assembly_batch_receipt",
        "expected_membership_relation": "source_build_expected_gallery",
        "observation_stat_relation": "gallery_observation_stat",
        "membership_relation": "source_build_gallery",
        "discovery_seal_relation": "source_build_discovery",
        "build_seal_relation": "build_manifest_core",
        "batch_rows_maximum": 256,
        "empty_manifest_chain_sha256": (
            "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
        ),
    }
    if not isinstance(assembly, Mapping) or any(
        assembly.get(name) != value for name, value in expected_assembly_core.items()
    ):
        raise ValueError("source-build assembly structural contract drifts")
    encoded_assembly = json.dumps(
        assembly, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    if hashlib.sha256(encoded_assembly).hexdigest() != (
        "5c143886e10a60892554752b00ee150b15eb4107456d50cca0d952ea2d05fb57"
    ):
        raise ValueError("source-build assembly exact protocol text drifts")

    relations = _raw_relation_map(logical)
    expected_shapes = {
        "source_build_discovery_checkpoint": (
            [
                "build_id",
                "generation",
                "cursor_bytes",
                "processed_count",
                "state",
                "updated_at",
            ],
            [["build_id"]],
        ),
        "source_build_discovery_batch_receipt": (
            [
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
            ],
            [["build_id", "batch_key"], ["build_id", "start_generation"]],
        ),
        "source_build_assembly_checkpoint": (
            [
                "build_id",
                "generation",
                "cursor_bytes",
                "processed_gallery_count",
                "processed_file_count",
                "processed_byte_count",
                "manifest_chain_sha256",
                "state",
                "updated_at",
            ],
            [["build_id"]],
        ),
        "source_build_assembly_batch_receipt": (
            [
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
            ],
            [["build_id", "batch_key"], ["build_id", "start_generation"]],
        ),
    }
    for relation_name, (attributes, keys) in expected_shapes.items():
        relation = relations.get(relation_name)
        if (
            not isinstance(relation, Mapping)
            or relation.get("attributes") != attributes
            or relation.get("declared_keys") != keys
        ):
            raise ValueError(f"{relation_name} source-build authority shape drifts")

    external = {
        str(value.get("name")): value
        for value in _raw_tables(logical.get("external_relation", []), "external")
    }
    expected_external = {
        "source_build_expected_gallery": (
            ["build_id", "position", "gallery_id"],
            [["build_id", "position"], ["build_id", "gallery_id"]],
        ),
        "gallery_observation_stat": (
            ["gallery_id", "observation_id", "file_count", "byte_count"],
            [["gallery_id", "observation_id"]],
        ),
    }
    for relation_name, (attributes, keys) in expected_external.items():
        relation = external.get(relation_name)
        if (
            not isinstance(relation, Mapping)
            or relation.get("attributes") != attributes
            or relation.get("declared_keys") != keys
        ):
            raise ValueError(f"{relation_name} cross-manifest authority shape drifts")

    source_cleanup = next(
        (
            value
            for value in _raw_tables(logical.get("cleanup_target", []), "cleanup")
            if value.get("target_kind") == "SOURCE_BUILD"
        ),
        None,
    )
    expected_phase_relations = {
        "SB_CANONICAL_UPLOAD": [
            "source_build_discovery_batch_receipt",
            "source_build_assembly_batch_receipt",
            "canonical_value_upload",
        ],
        "SB_GALLERY": [
            "source_build_sealed_at",
            "source_build_discovery_checkpoint",
            "source_build_assembly_checkpoint",
            "build_manifest_core",
            "source_build_gallery",
        ],
        "SB_DISCOVERY": ["source_build_discovery"],
        "SB_SATELLITES": [
            "source_build_expected_gallery",
            "source_build_base_publication_commit",
            "source_build_channel",
        ],
    }
    actual_phases = {
        str(value.get("phase")): value.get("relations")
        for value in _raw_tables(
            (
                source_cleanup.get("phases", [])
                if isinstance(source_cleanup, Mapping)
                else []
            ),
            "source cleanup phase",
        )
    }
    if any(
        actual_phases.get(phase) != relation_names
        for phase, relation_names in expected_phase_relations.items()
    ):
        raise ValueError("SOURCE_BUILD cleanup omits child-first assembly authority")

    physical_relations = _raw_relation_map(physical)
    required_checks = {
        "source_build_discovery_checkpoint": {
            "ck_source_build_discovery_checkpoint_state"
        },
        "source_build_discovery_batch_receipt": {
            "ck_source_build_discovery_batch_receipt_transition"
        },
        "source_build_assembly_checkpoint": {
            "ck_source_build_assembly_checkpoint_state"
        },
        "source_build_assembly_batch_receipt": {
            "ck_source_build_assembly_batch_receipt_transition"
        },
    }
    for relation_name, expected_checks in required_checks.items():
        actual_checks = {
            _required_text(value, "name", f"{relation_name}.check")
            for value in _raw_tables(
                physical_relations[relation_name].get("check", []),
                f"{relation_name}.check",
            )
        }
        if not expected_checks <= actual_checks:
            raise ValueError(f"{relation_name} physical transition checks drift")


def check_attempt_identity_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    _require_exact_table(
        logical,
        "cleanup_attempt_contract",
        {
            "job_relation": "cleanup_job",
            "attempt_attribute": "cycle_generation",
            "allocation_rule": "advance positive portable int63 cycle_generation monotonically on the one reusable job row per fixed shard under lock or exact CAS; fail closed before 2^63 and derive cleanup_id exactly as SHA256(h2hdb-cleanup-cycle-v1 NUL target_kind)[0:7] || u8(shard_no) || u64be(cycle_generation), atomically remove the prior completion when opening, require exact recomputed cleanup_id plus checkpoint generation on every mutation, require completion cycle_generation equals job cycle_generation and job state COMPLETE on replay, and reject every stale generation",
        },
    )
    _require_exact_table(
        logical,
        "preparation_identity_contract",
        {
            "preparation_relation": "operational_preparation",
            "policy_relation": "operational_policy",
            "deletion_generation_relation": "deletion_request_generation",
            "natural_key": [
                "build_id",
                "deletion_request_generation",
                "operational_policy_id",
            ],
            "rule": "exact retry under one policy and one FK-backed deletion-request generation resumes; policy or authoritative deletion generation change creates a distinct immutable preparation, and publication accepts only the preparation whose generation still equals the singleton deletion-generation head",
        },
    )
    preparation = _raw_relation_map(logical)["operational_preparation"]
    if {
        "attributes": ["deletion_request_generation"],
        "relation": "deletion_request_generation",
        "referenced_attributes": ["generation"],
    } not in preparation.get("foreign_keys", []):
        raise ValueError("preparation identity lacks exact deletion generation FK")


_CLEANUP_TARGET_SHAPES = {
    "SOURCE_BUILD": (
        "source_build_descriptor",
        ("build_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "source_build_unreferenced_v1",
        "source_build_retention_roots_v1",
        "h2hdb.cleanup.source_build.v1",
        (
            "SB_CANONICAL_UPLOAD",
            "SB_GALLERY",
            "SB_DISCOVERY",
            "SB_SATELLITES",
            "SB_GENERATION",
            "SB_STATE",
            "SB_ROOT",
        ),
    ),
    "ANALYSIS_RUN": (
        "analysis_run_descriptor",
        ("analysis_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "analysis_run_unpublished_leaf_v1",
        "analysis_descendant_reachability_v1",
        "h2hdb.cleanup.analysis_run.v1",
        (
            "AR_BATCH",
            "AR_COMPONENT",
            "AR_OVERLAY",
            "AR_FILE_HASH_VALUES",
            "AR_IMPACT_PROVENANCE",
            "AR_FILE_HASH_ANCHOR",
            "AR_EVIDENCE",
            "AR_EXCLUSION_VALUES",
            "AR_EXCLUSION_ANCHOR",
            "AR_CHECKPOINT",
            "AR_ANCESTRY",
            "AR_BASELINE",
            "AR_BINDINGS",
            "AR_COMPLETION",
            "AR_STATE",
            "AR_ROOT",
        ),
    ),
    "CATALOG_PUBLICATION": (
        "catalog_publication_occurrence_identity",
        ("revision", "publication_key"),
        "target_kind_tag16_u64be_zero8_v1",
        "catalog_publication_finalized_before_finalized_current_head_v1",
        "catalog_publication_finalized_current_head_and_live_predecessor_v1",
        "h2hdb.cleanup.catalog_publication.v1",
        (
            "CP_STORAGE",
            "CP_CONTRIBUTOR_SEAL",
            "CP_CONTRIBUTOR_IDENTITY",
            "CP_CONTRIBUTOR_NAME",
            "CP_CONTRIBUTOR_ROLE",
            "CP_CONTRIBUTOR_ANCHOR",
            "CP_ORDER",
            "CP_CONTENT",
            "CP_SUBJECT",
            "CP_ARTIFACT",
            "CP_ROOT",
        ),
    ),
    "PUBLICATION_COMMIT": (
        "publication_commit_anchor",
        ("receipt_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "publication_commit_unreachable_finalized_v2",
        "publication_commit_reachability_v2",
        "h2hdb.cleanup.publication_commit.v2",
        (
            "PCOM_RELEASE_BUILD_BASE",
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
            "PCOM_EVENT",
            "PCOM_FINALIZATION_MARKER",
            "PCOM_FINALIZATION_BATCH",
            "PCOM_COMMIT_EFFECT_ROOT",
            "PCOM_FINALIZATION_CHECKPOINT",
            "PCOM_ANCHOR",
        ),
    ),
    "CATALOG_REVISION_DESCRIPTOR": (
        "catalog_revision_descriptor",
        ("revision",),
        "target_kind_tag16_u64be_zero8_v1",
        "catalog_revision_descriptor_unreferenced_v2",
        "catalog_revision_descriptor_references_v2",
        "h2hdb.cleanup.catalog_revision_descriptor.v2",
        ("CRD_ROOT",),
    ),
    "SOURCE_REVISION_DESCRIPTOR": (
        "source_revision_descriptor",
        ("source_revision",),
        "target_kind_tag16_u64be_zero8_v1",
        "source_revision_descriptor_unreferenced_v2",
        "source_revision_descriptor_references_v2",
        "h2hdb.cleanup.source_revision_descriptor.v2",
        ("SRD_ROOT",),
    ),
    "PUBLICATION_GENERATION": (
        "publication_generation_node",
        ("generation",),
        "target_kind_tag16_u64be_zero8_v1",
        "publication_generation_unreferenced_prefix_v2",
        "publication_generation_references_and_compacted_floor_v2",
        "h2hdb.cleanup.publication_generation.v2",
        ("PG_EDGE", "PG_ROOT"),
    ),
    "PUBLICATION_CANDIDATE": (
        "publication_candidate",
        ("candidate_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "publication_candidate_inactive_v1",
        "publication_candidate_retention_roots_v1",
        "h2hdb.cleanup.publication_candidate.v1",
        (
            "PC_SEALS",
            "PC_PREPARED",
            "PC_INPUT",
            "PC_CONTRIBUTOR_NAME",
            "PC_CONTRIBUTOR_ROLE",
            "PC_CHECKPOINT",
            "PC_SELECTION_STORAGE",
            "PC_CONTENT",
            "PC_SUBJECT",
            "PC_BASES",
            "PC_SELECTION_IDENTITY",
            "PC_ROOT",
        ),
    ),
    "OPERATIONAL_PREPARATION": (
        "operational_preparation",
        ("preparation_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "operational_preparation_terminal_detached_v1",
        "operational_preparation_resume_authority_v1",
        "h2hdb.cleanup.operational_preparation.v1",
        (
            "OP_BATCH",
            "OP_CHECKPOINT",
            "OP_SUBTYPE",
            "OP_EVENT",
            "OP_SEAL",
            "OP_ROOT",
        ),
    ),
    "GALLERY_OBSERVATION": (
        "gallery_observation_allocation",
        ("gallery_id", "observation_id"),
        "target_kind_tag16_u64be_zero8_v1",
        "gallery_observation_unreferenced_v1",
        "gallery_observation_retention_roots_v1",
        "h2hdb.cleanup.gallery_observation.v1",
        (
            "GO_STAGING_RECEIPT_FRONTIER",
            "GO_STAGING_PAGE_ASSOCIATION",
            "GO_STAGING_REQUEST_DESCRIPTOR",
            "GO_STAGING_REQUEST_IDENTITY",
            "GO_STAGING_CHECKPOINT",
            "GO_STAGING_CLAIM",
            "GO_STAGING_ROOT",
            "GO_FACTS",
            "GO_FILESYSTEM_SEAL",
            "GO_FILESYSTEM_VALUES",
            "GO_FILESYSTEM_ANCHOR",
            "GO_FILES",
            "GO_OBSERVATION_FACTS",
            "GO_DESCRIPTOR",
            "GO_ROOT",
        ),
    ),
    "ARTIFACT_BLOB": (
        "artifact_blob",
        ("artifact_sha256",),
        "target_kind_tag16_u64be_zero8_v1",
        "artifact_blob_unreferenced_v1",
        "artifact_blob_retention_roots_v1",
        "h2hdb.cleanup.artifact_blob.v1",
        ("AB_ROOT",),
    ),
    "CANONICAL_VALUE": (
        "canonical_value_allocation_anchor",
        ("value_sha256",),
        "target_kind_tag16_u64be_zero8_v1",
        "canonical_value_unreferenced_v1",
        "canonical_value_retention_roots_v1",
        "h2hdb.cleanup.canonical_value.v1",
        (
            "CV_DICTIONARY",
            "CV_SEMANTIC_LINK",
            "CV_IDENTITY",
            "CV_PARENT_DESCRIPTOR",
            "CV_PAGE",
            "CV_ROOT",
        ),
    ),
    "CONTENT_BLOB": (
        "content_blob",
        ("file_sha256",),
        "target_kind_tag16_u64be_zero8_v1",
        "content_blob_unreferenced_v1",
        "content_blob_retention_roots_v1",
        "h2hdb.cleanup.content_blob.v1",
        ("CB_ROOT",),
    ),
    "GALLERY_OBSERVATION_PAGE": (
        "gallery_observation_page_descriptor_anchor",
        ("page_sha256",),
        "target_kind_tag16_u64be_zero8_v1",
        "gallery_observation_page_unreferenced_v1",
        "gallery_observation_page_retention_roots_v1",
        "h2hdb.cleanup.gallery_observation_page.v1",
        ("GOP_OUTGOING_CHILD", "GOP_BOUNDS", "GOP_DESCRIPTOR", "GOP_ROOT"),
    ),
    "GALLERY_OBSERVATION_STAGING": (
        "gallery_observation_staging",
        ("staging_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "gallery_observation_staging_terminal_v1",
        "gallery_observation_staging_live_or_unsealed_v1",
        "h2hdb.cleanup.gallery_observation_staging.v1",
        (
            "GOS_RECEIPT_FRONTIER",
            "GOS_PAGE_ASSOCIATION",
            "GOS_REQUEST_DESCRIPTOR",
            "GOS_REQUEST_IDENTITY",
            "GOS_CHECKPOINT",
            "GOS_CLAIM",
            "GOS_ROOT",
        ),
    ),
    "FILE_NAME_IDENTITY": (
        "file_name_identity_anchor",
        ("file_key",),
        "target_kind_tag16_u64be_zero8_v1",
        "file_name_identity_unreferenced_v1",
        "file_name_identity_retention_roots_v1",
        "h2hdb.cleanup.file_name_identity.v1",
        ("FN_ROOT",),
    ),
    "PUBLICATION_IDENTITY": (
        "publication_identity",
        ("publication_key",),
        "target_kind_tag16_u64be_zero8_v1",
        "publication_identity_unreferenced_v1",
        "publication_identity_retention_roots_v1",
        "h2hdb.cleanup.publication_identity.v1",
        ("PI_ROOT",),
    ),
    "GALLERY_IDENTITY": (
        "gallery_identity",
        ("gallery_id",),
        "target_kind_tag16_u64be_zero8_v1",
        "gallery_identity_unreferenced_v1",
        "gallery_identity_retention_roots_v1",
        "h2hdb.cleanup.gallery_identity.v1",
        ("GI_OBSERVATION_ALLOCATOR", "GI_SOURCE_NAME_ACCESS", "GI_ROOT"),
    ),
    "SOURCE_GALLERY_NAME_GID": (
        "source_gallery_name_gid",
        ("source_gallery_name",),
        "target_kind_tag16_u64be_zero8_v1",
        "source_gallery_name_gid_unreferenced_v1",
        "source_gallery_name_gid_retention_roots_v1",
        "h2hdb.cleanup.source_gallery_name_gid.v1",
        ("SNG_ROOT",),
    ),
    "GALLERY_UPLOAD_TIME": (
        "gallery_upload_time",
        ("gid",),
        "target_kind_tag16_u64be_zero8_v1",
        "gallery_upload_time_unreferenced_v1",
        "gallery_upload_time_retention_roots_v1",
        "h2hdb.cleanup.gallery_upload_time.v1",
        ("GUT_ROOT",),
    ),
    "CANONICAL_VALUE_UPLOAD": (
        "canonical_value_upload",
        ("generation", "value_sha256"),
        "target_kind_tag16_u64be_zero8_v1",
        "canonical_value_upload_stale_generation_v1",
        "canonical_value_upload_live_generation_v1",
        "h2hdb.cleanup.canonical_value_upload.v1",
        ("CVU_ROOT",),
    ),
    "HASH_CACHE_OBSERVATION": (
        "hash_cache_observation",
        ("source_identity_sha256", "fingerprint_sha256"),
        "target_kind_tag16_u64be_zero8_v1",
        "hash_cache_observation_expired_v1",
        "hash_cache_observation_live_lease_v1",
        "h2hdb.cleanup.hash_cache_observation.v1",
        ("HC_FILE", "HC_ROOT"),
    ),
}

_CLEANUP_SELECTION_ORDERS = {
    "SOURCE_BUILD": "uuid_first_byte_then_uuid_v1",
    "ANALYSIS_RUN": "uuid_first_byte_then_uuid_v1",
    "CATALOG_PUBLICATION": "sha256_prefix_then_candidate_key_v1",
    "PUBLICATION_COMMIT": "receipt_uuid_first_byte_then_uuid_v2",
    "CATALOG_REVISION_DESCRIPTOR": "revision_mod_256_then_revision_v2",
    "SOURCE_REVISION_DESCRIPTOR": "source_revision_mod_256_then_revision_v2",
    "PUBLICATION_GENERATION": "generation_mod_256_then_generation_v2",
    "PUBLICATION_CANDIDATE": "uuid_first_byte_then_uuid_v1",
    "OPERATIONAL_PREPARATION": "uuid_first_byte_then_uuid_v1",
    "GALLERY_OBSERVATION": "gallery_id_mod_256_then_gallery_id_observation_id_v1",
    "ARTIFACT_BLOB": "sha256_prefix_then_candidate_key_v1",
    "CANONICAL_VALUE": "sha256_prefix_then_candidate_key_v1",
    "CONTENT_BLOB": "sha256_prefix_then_candidate_key_v1",
    "GALLERY_OBSERVATION_PAGE": "sha256_prefix_then_candidate_key_v1",
    "GALLERY_OBSERVATION_STAGING": "uuid_first_byte_then_uuid_v1",
    "FILE_NAME_IDENTITY": "digest_first_byte_then_digest_v1",
    "PUBLICATION_IDENTITY": "digest_first_byte_then_digest_v1",
    "GALLERY_IDENTITY": "gallery_id_mod_256_then_gallery_id_v1",
    "SOURCE_GALLERY_NAME_GID": "source_gallery_name_first_byte_then_bytes_v1",
    "GALLERY_UPLOAD_TIME": "gid_mod_256_then_gid_v1",
    "CANONICAL_VALUE_UPLOAD": "generation_then_value_sha256_v1",
    "HASH_CACHE_OBSERVATION": "source_digest_first_byte_then_source_fingerprint_v1",
}

_CLEANUP_STATE_ROOT_HANDOFF_RULES = {
    "SOURCE_BUILD": "SB_STATE deletes the terminal scalar only after every prior child phase is COMPLETE; SB_ROOT requires the cleanup_id-bound SB_STATE checkpoint to be COMPLETE, requires source_build_state to remain absent, and rechecks every remaining source-build reachability blocker under the same exclusive gate before deleting source_build_descriptor",
    "ANALYSIS_RUN": "AR_COMPLETION deletes the optional COMPLETE timestamp child-first, AR_STATE deletes the terminal scalar only after every prior child phase is COMPLETE, and AR_ROOT requires the cleanup_id-bound AR_STATE checkpoint to be COMPLETE, requires analysis_run_state to remain absent, and rechecks every remaining analysis reachability blocker under the same exclusive gate before deleting analysis_run_descriptor",
}

_CLEANUP_FROZEN_ROOT_INT_ATTRIBUTES = {
    "gallery_id",
    "generation",
    "gid",
    "observation_id",
    "revision",
    "source_revision",
}
_CLEANUP_FROZEN_ROOT_UUID_ATTRIBUTES = {
    "analysis_id",
    "build_id",
    "candidate_id",
    "preparation_id",
    "receipt_id",
    "staging_id",
}
_CLEANUP_FROZEN_ROOT_DIGEST_ATTRIBUTES = {
    "artifact_sha256",
    "file_key",
    "file_sha256",
    "fingerprint_sha256",
    "page_sha256",
    "publication_key",
    "source_identity_sha256",
    "value_sha256",
}


def _cleanup_frozen_root_frame_bytes_by_target() -> dict[str, int]:
    """Derive every registered target's maximum typed root-frame width."""

    def scalar_bytes(attribute: str) -> int:
        if attribute in _CLEANUP_FROZEN_ROOT_INT_ATTRIBUTES:
            return 1 + 8
        if attribute in _CLEANUP_FROZEN_ROOT_UUID_ATTRIBUTES:
            return 1 + 2 + 16
        if attribute in _CLEANUP_FROZEN_ROOT_DIGEST_ATTRIBUTES:
            return 1 + 2 + 32
        if attribute == "source_gallery_name":
            return 1 + 2 + 255
        raise ValueError(
            f"cleanup frozen root attribute {attribute!r} lacks a physical bound"
        )

    # One version byte plus one arity byte precedes the typed scalar frames.
    widths: dict[str, int] = {}
    for kind, shape in _CLEANUP_TARGET_SHAPES.items():
        attributes = shape[1]
        if kind == "PUBLICATION_COMMIT":
            attributes = (*attributes, "preparation_id")
        widths[kind] = 2 + sum(scalar_bytes(attribute) for attribute in attributes)
    return widths


def _maximum_cleanup_frozen_root_frame_bytes() -> int:
    return max(_cleanup_frozen_root_frame_bytes_by_target().values())


def check_cleanup_reachability_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    _require_exact_table(
        logical,
        "cleanup_reachability_contract",
        {
            "version": 1,
            "target_registry_relation": "cleanup_target_kind",
            "sweep_registry_relation": "cleanup_sweep_target",
            "phase_registry_relation": "cleanup_phase",
            "job_relation": "cleanup_job",
            "frozen_root_relation": "cleanup_cycle_root",
            "checkpoint_relation": "cleanup_checkpoint",
            "target_key_bytes": 32,
            "frozen_root_key_max_bytes": 260,
            "frozen_root_count_maximum": 256,
            "registry_rule": "target kinds and phases are exact provider-seeded rows; runtime dispatch is a closed-world map from registered IDs to versioned writer functions and never interpolates a relation, predicate, blocker, or phase from database text",
            "eligibility_rule": "under the exact EXCLUSIVE maintenance gate and one transaction, permit globally at most one OPEN cleanup_job, evaluate the registered initial eligibility predicate and every registered blocker, freeze at most max_rows_per_transaction and no more than 256 typed roots into cleanup_cycle_root, encode each root with the canonical version-one typed scalar frame bounded to the registered-shape maximum 260 bytes, and seal the sorted complete membership with immutable frozen_root_count plus domain-separated frozen_root_set_sha256 on cleanup_job; frozen_root_key is a cleanup-protocol identity decoded back into the registered typed root columns, never business payload, EAV, or a relation-count packing device",
            "phase_rule": "a checkpoint phase must belong to its jobs target kind; complete phases in strictly increasing phase_order; every static phase reloads and validates the exact sealed cleanup_cycle_root set, joins only those typed roots, rejects any current-spec coordinate at or before its durable cursor that reappears, and each batch deletes only the registered relation set child-first while committing rows, receipt, and checkpoint CAS atomically. Before accepting a zero-row terminal transition, runtime probes every raw registered responsibility through the current phase under frozen root, shard, and spec extra predicates but without the mutable plan eligibility predicate; any remaining row blocks rather than falsely completing, and roots made newly eligible by an earlier deletion wait for a later cycle",
            "completion_rule": "a cleanup job becomes COMPLETE only after every registered phase has a durable empty terminal receipt, the target root phase completed, and a final blocker recheck still finds no reachable retention root",
            "compaction_rule": "each page commit atomically advances checkpoint generation and replaces the prior ambiguity receipt so at most one receipt per live checkpoint is retained; that receipt stores the exact prior chain and prior deleted count beside its bounded row-key input digest, and runtime plus full CHECK recompute the canonical output chain and exact post-deleted count without claiming to retain or rescan full history. Terminal transaction exact-validates then deletes cleanup_cycle_root membership, updates cleanup_job COMPLETE and completed_at, upserts the shard latest cleanup_completion generation/final chain/deleted count, deletes live receipts then checkpoints, and makes completion the response-loss replay authority; stale generations cannot resume",
        },
    )
    if _maximum_cleanup_frozen_root_frame_bytes() != 260:
        raise ValueError(
            "registered cleanup root physical bounds no longer derive 260 bytes"
        )
    targets = _raw_tables(logical.get("cleanup_target", []), "cleanup_target")
    by_kind = {
        _required_text(value, "target_kind", "cleanup target"): value
        for value in targets
    }
    if len(by_kind) != len(targets) or set(by_kind) != set(_CLEANUP_TARGET_SHAPES):
        raise ValueError("cleanup target kind registry is duplicate or incomplete")
    cleanup_id_tags = {
        hashlib.sha256(b"h2hdb-cleanup-cycle-v1\0" + kind.encode("ascii")).digest()[:7]
        for kind in by_kind
    }
    if len(cleanup_id_tags) != len(by_kind):
        raise ValueError("cleanup cycle kind tags are not unique")
    _require_exact_table(
        logical,
        "cleanup_sweep_protocol",
        {
            "version": 1,
            "algorithm_version": 2,
            "shard_count": 256,
            "target_key_codec": "target_kind_tag16_u64be_zero8_v1",
            "cleanup_id_codec": "target_kind_tag7_u8_shard_u64be_cycle_v1",
            "cleanup_id_domain": "h2hdb-cleanup-cycle-v1",
            "cycle_generation_minimum": 1,
            "cycle_generation_overflow": "fail_closed_before_2^63",
            "active_receipts_per_checkpoint": 1,
            "history_rule": "one reusable job and latest completion per fixed shard; long-lived audit history belongs in a separate append-only audit stream, not cleanup control tables",
        },
    )
    with Path(__file__).with_name("catalog.toml").open("rb") as stream:
        catalog_document = tomllib.load(stream)
    data_retention_kinds = {
        _required_text(value, "target", "catalog retention target")
        for value in _raw_tables(
            catalog_document.get("retention_target", []), "retention_target"
        )
    }
    if set(by_kind) != data_retention_kinds | {
        "OPERATIONAL_PREPARATION",
        "HASH_CACHE_OBSERVATION",
        "CANONICAL_VALUE_UPLOAD",
        "GALLERY_OBSERVATION_STAGING",
    }:
        raise ValueError(
            "cleanup targets do not exactly cover data retention targets plus bounded operational cleanup targets"
        )
    allowed_fields = {
        "target_kind",
        "mode",
        "sweep_shard_count",
        "selection_order_id",
        "root_relation",
        "root_key",
        "key_codec",
        "eligible_predicate_id",
        "blocker_query_id",
        "writer_hook",
        "retention_roots",
        "phases",
    }
    for kind, target in by_kind.items():
        extra_allowed = (
            {"retained_fk_edges"} if kind != "OPERATIONAL_PREPARATION" else set()
        )
        if kind == "ANALYSIS_RUN":
            extra_allowed.update(
                {"conditional_blockers", "state_rule", "state_root_handoff_rule"}
            )
        if kind == "SOURCE_BUILD":
            extra_allowed.update(
                {
                    "conditional_child_rules",
                    "terminal_staging_rule",
                    "state_root_handoff_rule",
                }
            )
        if kind == "PUBLICATION_COMMIT":
            extra_allowed.add("preparation_control_handoff_rule")
            if target.get("preparation_control_handoff_rule") != (
                "under the exact live EXCLUSIVE maintenance gate, an unreachable finalized commit is initially eligible only with its exact candidate_id/preparation_id binding, a unique COMPLETE preparation and effect seal whose operational_policy_id matches the commit, no source or catalog working root, and no PENDING or PREPARED prepared-artifact protection. The begin transaction freezes each eligible exact (receipt_id, preparation_id) anchor/commit authority in cleanup_cycle_root and includes both identities in the sealed root-set digest; later phases select by receipt_id but fail closed if the commit preparation differs from the frozen pair. The exact closed-world order is RELEASE_BASE, PREP_BINDING, PREP_BATCH, PREP_CHECKPOINT, PREP, EVENT, FINALIZATION_MARKER, FINALIZATION_BATCH, COMMIT_EFFECT_ROOT, FINALIZATION_CHECKPOINT, ANCHOR; every phase requires the exact prior COMPLETE cleanup receipt and prior relation absence. Operational events are per-preparation publication-owned transient current/retry snapshots rather than cross-revision delta or delivery history. The compound EVENT phase locks each base event with exactly one type-matching subtype and atomically deletes subtype then event; a durable (receipt_id, preparation_id, sequence_no) cursor permits only fully absent covered subtype/event coordinates, while partial, mismatched, or ahead-of-cursor absence fails closed. An OPEN PCOM job may temporarily retain the unreachable non-head commit and seal after covered coordinates are gone; the full CHECK accepts that state only for the matching frozen OPEN PCOM job and EVENT cursor/receipt chain before COMMIT_EFFECT_ROOT, while public reads remain current-head-only and exact COMPLETE preparation authority is already removed. COMMIT_EFFECT_ROOT rechecks every pin, prior receipt, child absence, and every cursor-uncovered exact receipt_id/preparation_id commit, effect-seal, and event-stream triple; because the frozen root set is capped at 256, one bounded transaction exact-deletes commit, effect seal, and event stream in FK order with affected-count one for every triple, then commits its cleanup receipt/checkpoint CAS last. Any partial authority, mismatch, or fault rolls back the whole batch. A nonempty compound cursor must equal the final frozen receipt/preparation coordinate, start from the empty cursor, report row_count equal to frozen_root_count, and have every frozen commit, preparation, event, effect seal, and event stream absent; its monotone receipt/checkpoint chain, frozen pair authority, and full CHECK owner/orphan rejection are the durable covered-root proof. The exact empty root set instead uses the terminal empty-cursor receipt as its proof. Separately receipted finalization-checkpoint and anchor phases then complete."
            ):
                raise ValueError(
                    "publication-commit preparation control handoff rule drifts"
                )
            if [value.get("relations") for value in target["phases"]] != [
                ["source_build_base_publication_commit"],
                ["publication_candidate_preparation"],
                ["operational_preparation_batch_receipt"],
                ["operational_preparation_checkpoint"],
                ["operational_preparation"],
                [
                    "operational_removed_gid_event",
                    "operational_deletion_consumption_event",
                    "operational_event",
                ],
                ["publication_commit_finalization"],
                ["publication_finalization_batch_receipt_stored"],
                [
                    "publication_commit",
                    "operational_preparation_effect_seal",
                    "operational_event_stream",
                ],
                ["publication_finalization_checkpoint"],
                ["publication_commit_anchor"],
            ]:
                raise ValueError(
                    "publication-commit preparation child-first phases drift"
                )
        if kind == "PUBLICATION_CANDIDATE":
            extra_allowed.update({"semantic_blockers", "conditional_blockers"})
        if kind == "GALLERY_OBSERVATION":
            extra_allowed.update(
                {
                    "conditional_blockers",
                    "predecessor_selectors",
                    "predecessor_blockers",
                }
            )
        if kind in {"ARTIFACT_BLOB", "CANONICAL_VALUE"}:
            extra_allowed.update({"owned_prunable_intermediates", "required_via_paths"})
        if kind in {
            "PUBLICATION_COMMIT",
            "PUBLICATION_CANDIDATE",
            "CANONICAL_VALUE",
            "GALLERY_OBSERVATION_PAGE",
        }:
            extra_allowed.add("machine_gates")
        if kind in {
            "PUBLICATION_GENERATION",
            "CANONICAL_VALUE",
            "GALLERY_OBSERVATION_PAGE",
        }:
            extra_allowed.add("phase_selectors")
        if kind == "GALLERY_OBSERVATION_PAGE":
            extra_allowed.add("operational_blockers")
        if kind in {
            "SOURCE_BUILD",
            "PUBLICATION_COMMIT",
            "PUBLICATION_CANDIDATE",
            "CANONICAL_VALUE",
            "CONTENT_BLOB",
            "GALLERY_IDENTITY",
        }:
            extra_allowed.add("operational_blockers")
        if kind == "OPERATIONAL_PREPARATION":
            extra_allowed = {
                "operational_blockers",
                "outliving_relations",
                "conditional_cleanup_rule",
                "rationale",
            }
        if kind == "CANONICAL_VALUE_UPLOAD":
            extra_allowed.add("claim_rule")
        if kind == "GALLERY_OBSERVATION_STAGING":
            extra_allowed.update(
                {
                    "compaction_rule",
                    "predecessor_selectors",
                    "predecessor_blockers",
                    "retained_fk_edges",
                }
            )
        if set(target) != allowed_fields | extra_allowed:
            raise ValueError(f"cleanup target {kind} fields are not closed-world")
        expected_state_root_handoff = _CLEANUP_STATE_ROOT_HANDOFF_RULES.get(kind)
        if (
            expected_state_root_handoff is not None
            and target.get("state_root_handoff_rule") != expected_state_root_handoff
        ):
            raise ValueError(f"cleanup target {kind} state-to-root handoff rule drifts")
        root, root_key, codec, predicate, blocker, hook, phase_ids = (
            _CLEANUP_TARGET_SHAPES[kind]
        )
        actual = (
            target.get("root_relation"),
            tuple(target.get("root_key", [])),
            target.get("key_codec"),
            target.get("eligible_predicate_id"),
            target.get("blocker_query_id"),
            target.get("writer_hook"),
        )
        if actual != (root, root_key, codec, predicate, blocker, hook):
            raise ValueError(f"cleanup target {kind} machine binding drifts")
        expected_mode = "SWEEP"
        if target.get("mode") != expected_mode:
            raise ValueError(f"cleanup target {kind} mode drifts")
        if expected_mode == "SWEEP":
            if target.get("sweep_shard_count") != 256 or target.get(
                "selection_order_id"
            ) != _CLEANUP_SELECTION_ORDERS.get(kind):
                raise ValueError(f"cleanup target {kind} sweep registry drifts")
        _required_texts(target, "retention_roots", f"cleanup target {kind}")
        phases = _raw_tables(target.get("phases", []), f"cleanup target {kind}.phases")
        if tuple(value.get("phase") for value in phases) != phase_ids:
            raise ValueError(f"cleanup target {kind} phase registry drifts")
        if tuple(value.get("order") for value in phases) != tuple(
            range(1, len(phases) + 1)
        ):
            raise ValueError(
                f"cleanup target {kind} phases are not contiguous child-first"
            )
        child_relations: set[str] = set()
        for phase in phases:
            if set(phase) != {"phase", "order", "relations"}:
                raise ValueError(
                    f"cleanup target {kind} phase fields are not closed-world"
                )
            relations = _required_texts(
                phase, "relations", f"cleanup target {kind} phase"
            )
            if child_relations.intersection(relations):
                raise ValueError(f"cleanup target {kind} deletes a relation twice")
            child_relations.update(relations)
        if root not in child_relations or root not in tuple(phases[-1]["relations"]):
            raise ValueError(f"cleanup target {kind} root is not in its terminal phase")
        if kind in {"ARTIFACT_BLOB", "CANONICAL_VALUE"}:
            raw_owned = target.get("owned_prunable_intermediates")
            if not isinstance(raw_owned, list) or not all(
                isinstance(value, str) and value for value in raw_owned
            ):
                raise ValueError(
                    f"cleanup target {kind}.owned_prunable_intermediates must be a string array"
                )
            owned = set(raw_owned)
            if owned != child_relations - {root}:
                raise ValueError(
                    f"cleanup target {kind} prunable intermediary coverage drifts"
                )
            expected_via: list[dict[str, object]] = []
            if target.get("required_via_paths") != expected_via:
                raise ValueError(f"cleanup target {kind} verified via paths drift")
        if kind == "GALLERY_OBSERVATION":
            if target.get("conditional_blockers") != [
                {
                    "relation": "gallery_observation_staging",
                    "claim_relation": "gallery_observation_staging_claim",
                    "state_attribute": "state",
                    "ingest_generation_attribute": "ingest_generation",
                    "claim_generation_attribute": "claim_generation",
                    "owner_relation": "ingest_generation_owner",
                    "rule": "under row locks and exact header plus claim recheck, cleanup rejects OPEN whose outer owner row has an unexpired lease_expires_at and every SEALED or RETIRING_SEALED allocation linked by source_build_gallery; ABANDONED is eligible only after its claim generation is stale, while REUSED or RETIRING_REUSED becomes eligible only after the durable build link names a different sealed observation; every bounded batch rechecks the same header state, ingest generation, and claim generation",
                }
            ]:
                raise ValueError("gallery staging cleanup liveness blocker drifts")
        if kind == "SOURCE_BUILD":
            retention_roots = _required_texts(
                target,
                "retention_roots",
                "cleanup target SOURCE_BUILD",
            )
            if not any(
                "at-most-one ABANDONED analysis retirement family" in root
                and "schema-unreachable sibling analysis" in root
                for root in retention_roots
            ):
                raise ValueError("source-build successor-fence retention drifts")
        if kind == "ANALYSIS_RUN":
            retention_roots = _required_texts(
                target,
                "retention_roots",
                "cleanup target ANALYSIS_RUN",
            )
            if not any(
                "source_build_base_publication_commit.base_receipt_id" in root
                and "source_revision_provenance.analysis_id" in root
                for root in retention_roots
            ):
                raise ValueError("source-build base provenance retention drifts")
            if not any(
                "at-most-one ABANDONED analysis" in root
                and "globally latest source_build_generation" in root
                for root in retention_roots
            ):
                raise ValueError("latest analysis retirement retention drifts")
            if not any(
                "schema-unreachable sibling family" in root
                and "audit-bypassing damaged database" in root
                for root in retention_roots
            ):
                raise ValueError("multi-analysis retirement retention drifts")
            if target.get("conditional_blockers") != [
                "source_head.source_revision->source_revision_provenance.analysis_id when the revision is the active channel head",
                "a source-build base receipt retains its exact provenance analysis even after that receipt ceases to be the active head",
                "a latest-mapped ABANDONED run is released only by a strictly newer source-build mapping; any schema-unreachable sibling family remains fail-closed and is never automatically released",
            ]:
                raise ValueError("active source-head provenance blocker drifts")
            if target.get("state_rule") != (
                "OPEN is never cleanup-eligible; only COMPLETE or ABANDONED may be "
                "selected. The schema enforces at most one analysis run per build. A "
                "valid OPEN-to-ABANDONED CAS requires that run, the exact source working "
                "assignment to equal the build's database-owned created_at, and no "
                "catalog working candidate, snapshot binding, publication candidate, "
                "source-revision provenance, or operational preparation; it atomically "
                "removes that exact working root. Cleanup retains the globally latest "
                "ABANDONED proof until a successor mapping and fails closed if an "
                "audit-bypassing damaged database exposes an impossible sibling family"
            ):
                raise ValueError("analysis cleanup state rule drifts")
        if kind == "PUBLICATION_CANDIDATE":
            retention_roots = _required_texts(
                target,
                "retention_roots",
                "cleanup target PUBLICATION_CANDIDATE",
            )
            if not any(
                "publication_commit.candidate_id" in root
                and "source_build_base_publication_commit.base_receipt_id" in root
                for root in retention_roots
            ):
                raise ValueError("source-build base candidate retention drifts")
            if target.get("semantic_blockers") != [
                {
                    "relation": "prepared_artifact",
                    "attributes": ["candidate_id"],
                    "root_attributes": ["candidate_id"],
                    "blocking_predicate": "state IN ('PENDING','PREPARED')",
                    "nonblocking_state": "COMMITTED",
                    "semantic_obligation_id": "catalog.retention.v2",
                    "release_obligation_id": "catalog.artifact-semantics.v1",
                }
            ]:
                raise ValueError(
                    "candidate cleanup prepared-artifact semantic blocker drifts"
                )
            if target.get("conditional_blockers") != [
                {
                    "relation": "prepared_artifact",
                    "candidate_attribute": "candidate_id",
                    "state_attribute": "state",
                    "blocking_states": ["PENDING", "PREPARED"],
                    "release_acknowledged_state": "COMMITTED",
                    "release_token_relation": "prepared_artifact",
                    "rule": "under the locked candidate, initial eligibility, every phase batch, and final completion recheck reject every PENDING or PREPARED row; bounded orphan reconciliation must issue an immutable keyset page from current complete prepared rows under the exact live EXCLUSIVE gate, commit exact candidate and row revalidation before invoking the registered adapter's terminal release outside every database transaction, then revalidate under that gate and compare-and-swap either PENDING or PREPARED to COMMITTED from only the repository-issued opaque acknowledgement; response-loss retries reuse the same tokens, late protect cannot defeat the terminal tombstone, all-COMMITTED replay performs zero DML, and only COMMITTED permits child-first deletion",
                }
            ]:
                raise ValueError("candidate cleanup external-protection blocker drifts")
    prep = by_kind["OPERATIONAL_PREPARATION"]
    if prep.get("operational_blockers") != [
        {
            "relation": "publication_candidate_preparation",
            "attributes": ["preparation_id"],
        },
        {
            "relation": "publication_commit",
            "attributes": ["preparation_id"],
        },
    ]:
        raise ValueError("operational preparation candidate-binding blocker drifts")
    if prep.get("outliving_relations") != [
        "operational_event_stream",
        "operational_preparation_effect_seal",
        "operational_event",
        "operational_removed_gid_event",
        "operational_deletion_consumption_event",
    ]:
        raise ValueError("operational activation outliving registry drifts")
    if prep.get("conditional_cleanup_rule") != (
        "under the exclusive gate, generic OPERATIONAL_PREPARATION cleanup never selects COMPLETE; the unreachable finalized PUBLICATION_COMMIT lifecycle first releases every safe build-base pin, exact-removes its candidate/preparation binding and COMPLETE control family, then deletes both typed subtypes and base events before one atomic commit/effect-seal/event-stream phase; ABANDONED is eligible only with commit and candidate-binding authority absent and deletes receipts, checkpoints, both typed subtypes, base events, seal, preparation, then stream; FAILED must first transition to ABANDONED, and completion rejects any remaining invisible uncommitted stream"
    ):
        raise ValueError("operational preparation conditional cleanup rule drifts")
    if [value.get("relations") for value in prep["phases"]] != [
        ["operational_preparation_batch_receipt"],
        ["operational_preparation_checkpoint"],
        ["operational_removed_gid_event", "operational_deletion_consumption_event"],
        ["operational_event"],
        ["operational_preparation_effect_seal"],
        ["operational_preparation", "operational_event_stream"],
    ]:
        raise ValueError("operational preparation child-first effect phases drift")
    if not isinstance(prep.get("rationale"), str) or any(
        term not in str(prep["rationale"])
        for term in (
            "commit-to-preparation-to-build lineage",
            "publication-owned transient current/retry control",
        )
    ):
        raise ValueError("operational preparation outliving rationale is absent")
    canonical = by_kind["CANONICAL_VALUE"]
    if canonical.get("operational_blockers") != [
        {
            "relation": "hash_cache_observation",
            "attributes": ["source_identity_sha256"],
        },
        {"relation": "hash_cache_observation", "attributes": ["fingerprint_sha256"]},
        {"relation": "canonical_value_upload", "attributes": ["value_sha256"]},
    ]:
        raise ValueError("canonical operational blocker registry drifts")
    candidate = by_kind["PUBLICATION_CANDIDATE"]
    expected_candidate_phase_relations = [
        [
            "publication_candidate_preparation",
            "publication_candidate_projection_seal",
            "publication_batch_receipt_stored",
            "artifact_operation",
            "catalog_publication_storage",
        ],
        ["prepared_artifact", "catalog_contributor_seal"],
        ["artifact_input", "catalog_contributor_identity"],
        ["catalog_contributor_name_sha256"],
        ["catalog_contributor_role"],
        ["publication_checkpoint", "catalog_contributor_anchor"],
        ["publication_selection_storage", "catalog_publication_order"],
        ["catalog_publication_content"],
        ["catalog_subject"],
        ["publication_candidate_base_publication_commit", "catalog_artifact"],
        [
            "publication_selection_occurrence_identity",
            "catalog_publication_occurrence_identity",
        ],
        ["publication_candidate"],
    ]
    if [phase.get("relations") for phase in candidate["phases"]] != (
        expected_candidate_phase_relations
    ):
        raise ValueError(
            "publication candidate phases must fold the exact uncommitted "
            "reserved projection child-first"
        )
    candidate_retention_roots = candidate.get("retention_roots")
    if (
        not isinstance(candidate_retention_roots, list)
        or not candidate_retention_roots
        or candidate_retention_roots[-1]
        != "an uncommitted candidate exclusively owns every catalog projection row whose revision equals its unique reserved_revision; each projection selector and every phase eligibility recheck require the exact candidate to have no publication_commit, while a committed reserved projection blocks PUBLICATION_CANDIDATE cleanup until higher-priority CATALOG_PUBLICATION cleanup removes that payload under its current-head finalization gates"
    ):
        raise ValueError(
            "publication candidate uncommitted reserved-projection boundary drifts"
        )
    upload_cleanup = by_kind["CANONICAL_VALUE_UPLOAD"]
    if upload_cleanup.get("claim_rule") != (
        "under the exclusive maintenance gate, keyset-select at most the fixed batch bound by generation then value_sha256; for every claim lock and recheck current head, generation history, the complete owner row including lease_expires_at, optional source_build_generation mapping, canonical allocation domain, and exact claim. Delete only if the generation is completed or strictly superseded and never current/live. A missing mapping is accepted only for source_root_v1, the sole bootstrap domain; every other domain requires its retained mapping. Receipt and checkpoint CAS commit atomically, after which the unblocked allocation becomes ordinary CANONICAL_VALUE GC work"
    ):
        raise ValueError("canonical upload independent stale-claim cleanup drifts")
    staging_compaction = by_kind["GALLERY_OBSERVATION_STAGING"]
    if staging_compaction.get("retained_fk_edges") != [] or staging_compaction.get(
        "compaction_rule"
    ) != (
        "under the exclusive maintenance gate, keyset-select terminal staging headers in fixed digest shards; before every bounded delete batch in the same transaction, recompute the provisional four-root identity, require final observation identity, FILE-root file_count, immutable terminal_byte_count, final stat, manifest, claim and exact source_build_gallery outcome, then enforce SEALED or RETIRING_SEALED link equality versus REUSED or RETIRING_REUSED link inequality. Delete at most the fixed batch bound child-first; predecessor_selectors delete only outgoing rows whose ownership-bearing request belongs to this staging. Runtime locks both request rows and rejects every cross-owner predecessor in either direction at insertion and retirement; any corrupt cross-owner edge or durable authority is a cleanup blocker and produces zero committed deletes. Preserve gallery_observation_allocation, allocation_page, normalized facts, roots, pages, and final membership. OPEN always blocks; after compaction only live OPEN staging blocks SOURCE_BUILD cleanup. A REUSED or RETIRING_REUSED allocation with no remaining staging header, final gallery_observation, or source_build_gallery reference is an exact GALLERY_OBSERVATION orphan eligible for bounded data cleanup"
    ):
        raise ValueError("terminal gallery-staging compaction boundary drifts")
    predecessor_selectors = [
        {
            "relation": "gallery_observation_staging_request_predecessor",
            "attribute": "request_sha256",
            "owner_relation": "gallery_observation_staging_request",
            "owner_attribute": "request_sha256",
        },
    ]
    predecessor_blockers = [
        {
            "relation": "gallery_observation_staging_request_predecessor",
            "incoming_attribute": "prior_request_sha256",
            "successor_attribute": "request_sha256",
            "owner_relation": "gallery_observation_staging_request",
            "owner_request_attribute": "request_sha256",
            "owner_staging_attribute": "staging_id",
            "rule": "lock both ownership-bearing request rows; an incoming edge whose successor owner differs from the selected staging blocks cleanup and is never deleted by that staging",
        }
    ]
    if any(
        target.get("predecessor_selectors") != predecessor_selectors
        or target.get("predecessor_blockers") != predecessor_blockers
        for target in (by_kind["GALLERY_OBSERVATION"], staging_compaction)
    ):
        raise ValueError("staging predecessor cleanup selectors drift")
    if by_kind["SOURCE_BUILD"].get("conditional_child_rules") != [
        {
            "relation": "source_build_generation",
            "upload_relation": "canonical_value_upload",
            "generation_attribute": "generation",
            "coordination_head_relation": "ingest_coordination_head",
            "generation_relation": "ingest_generation",
            "owner_relation": "ingest_generation_owner",
            "handoff_relation": "ingest_generation_handoff",
            "rule": "before job creation, phase one, and every destructive batch, lock and recheck every mapping for the selected build; any mapping not durably completed and not strictly superseded by the current coordination head blocks the whole job before deletion. For an eligible mapping, bounded cleanup deletes residual canonical_value_upload claims after the same completed-or-superseded recheck and finally deletes the mapping; a current owner row carrying lease authority or handoff resume authority always rejects",
        }
    ]:
        raise ValueError("source-build generation liveness rule drifts")
    if by_kind["SOURCE_BUILD"].get("terminal_staging_rule") != (
        "every remaining staging FK blocks SOURCE_BUILD job creation. OPEN is a live blocker; SEALED, REUSED, RETIRING_SEALED, and RETIRING_REUSED are pending work for in-band retirement or the independent GALLERY_OBSERVATION_STAGING compactor, which preserves final membership and data facts. Therefore SB_GALLERY starts only after terminal control rows are compacted, eliminating the staging-to-membership cleanup cycle"
    ):
        raise ValueError("source-build terminal staging compaction rule drifts")
    _require_exact_table(
        logical,
        "hash_cache_cleanup_contract",
        {
            "version": 1,
            "policy_relation": "cleanup_job",
            "max_age_attribute": "hash_cache_max_age_microseconds",
            "cycle_cutoff_attribute": "cycle_cutoff_at",
            "gate_rule": "exclusive maintenance gate excludes concurrent hash-cache readers and writers",
            "eligibility_rule": "observed_at <= cycle_cutoff_at - hash_cache_max_age_microseconds with checked nonnegative subtraction",
        },
    )

    _validate_catalog_cleanup_fk_coverage(by_kind, _raw_relation_map(logical))

    relation_map = _raw_relation_map(logical)
    if relation_map["cleanup_target_kind"].get("declared_keys") != [["target_kind"]]:
        raise ValueError("cleanup target registry key drifts")
    if relation_map["cleanup_sweep_target"].get("declared_keys") != [
        ["target_kind", "shard_no"],
        ["target_key"],
    ]:
        raise ValueError("cleanup sweep target alternate key drifts")
    if relation_map["cleanup_completion"].get("declared_keys") != [["target_key"]]:
        raise ValueError("cleanup completion key drifts")
    if relation_map["cleanup_phase"].get("declared_keys") != [
        ["phase"],
        ["target_kind", "phase_order"],
    ]:
        raise ValueError("cleanup phase registry keys drift")
    job_fks = relation_map["cleanup_job"].get("foreign_keys", [])
    checkpoint_fks = relation_map["cleanup_checkpoint"].get("foreign_keys", [])
    if {
        "attributes": ["target_key"],
        "relation": "cleanup_sweep_target",
        "referenced_attributes": ["target_key"],
    } not in job_fks:
        raise ValueError("cleanup job target identity is not registry constrained")
    if "target_kind" in relation_map["cleanup_job"].get("attributes", []):
        raise ValueError("cleanup job redundantly stores target kind")
    if {
        "attributes": ["phase"],
        "relation": "cleanup_phase",
        "referenced_attributes": ["phase"],
    } not in checkpoint_fks:
        raise ValueError("cleanup checkpoint phase is not registry constrained")


def _validate_catalog_cleanup_fk_coverage(
    targets: Mapping[str, Mapping[str, Any]],
    operational_relations: Mapping[str, Mapping[str, Any]],
) -> None:
    """Every catalog FK descendant is deleted, explicitly retained, or derived+deleted."""

    with Path(__file__).with_name("catalog.toml").open("rb") as stream:
        catalog = tomllib.load(stream)
    relations = _raw_relation_map(catalog)
    data_targets = {
        _required_text(value, "target", "catalog retention target"): value
        for value in _raw_tables(
            catalog.get("retention_target", []), "retention_target"
        )
    }
    codec_arity = {
        "target_kind_tag16_uuid16_v1": 1,
        "target_kind_tag16_u64be_u64be_v1": 2,
        "target_kind_tag16_u64be_zero8_v1": 0,
        "raw_sha256_v1": 1,
    }
    all_relations = relations | dict(operational_relations)
    for kind, target in targets.items():
        root = str(target["root_relation"])
        root_key = tuple(str(value) for value in target["root_key"])
        declared_keys = {
            tuple(str(attribute) for attribute in key)
            for key in all_relations[root].get("declared_keys", [])
        }
        if root_key not in declared_keys:
            raise ValueError(f"cleanup target {kind} root key is not a candidate key")
        codec = str(target["key_codec"])
        if codec not in codec_arity or (
            target.get("mode") == "ROW" and len(root_key) != codec_arity[codec]
        ):
            raise ValueError(f"cleanup target {kind} codec input attributes drift")
    children: dict[str, list[tuple[str, tuple[str, ...]]]] = {}
    for child_name, relation in relations.items():
        for foreign_key in _raw_tables(
            relation.get("foreign_keys", []), f"{child_name}.foreign_keys"
        ):
            parent = _required_text(foreign_key, "relation", "foreign key")
            attributes = _required_texts(foreign_key, "attributes", "foreign key")
            children.setdefault(parent, []).append((child_name, attributes))
    for kind in sorted(data_targets):
        target = targets[kind]
        data_target = data_targets[kind]
        root = str(target["root_relation"])
        if root != data_target.get("root_relation") or list(
            target["root_key"]
        ) != data_target.get("root_key"):
            raise ValueError(
                f"cleanup target {kind} root binding differs from catalog retention authority"
            )
        raw_blockers = target.get("retained_fk_edges")
        if not isinstance(raw_blockers, list) or not all(
            isinstance(value, dict)
            and set(value)
            in ({"relation", "attributes"}, {"relation", "attributes", "via"})
            for value in raw_blockers
        ):
            raise ValueError(f"cleanup target {kind}.retained_fk_edges is invalid")
        blockers: set[tuple[str, tuple[str, ...]]] = set()
        for blocker in raw_blockers:
            assert isinstance(blocker, dict)
            blocker_relation = _required_text(blocker, "relation", "retained FK edge")
            blocker_attributes = _required_texts(
                blocker, "attributes", "retained FK edge"
            )
            raw_via = blocker.get("via", [])
            if not isinstance(raw_via, list) or not all(
                isinstance(value, str) and value for value in raw_via
            ):
                raise ValueError("retained FK edge via path is invalid")
            blockers.add((blocker_relation, blocker_attributes))
        deleted = {
            relation
            for phase in _raw_tables(target["phases"], f"cleanup target {kind}.phases")
            for relation in _required_texts(phase, "relations", "cleanup phase")
        }
        data_owned = {
            relation
            for phase in data_target.get("child_phases", [])
            for relation in phase
        }
        if (deleted & set(relations)) - {root} != (data_owned & set(relations)) - {
            root
        }:
            raise ValueError(
                f"cleanup target {kind} phase ownership differs from catalog retention authority"
            )
        data_derived = set(data_target.get("derived_views", []))
        if deleted & data_derived:
            raise ValueError(
                f"cleanup target {kind} attempts to delete a catalog logical view"
            )
        data_boundary = {
            (str(edge["relation"]), tuple(str(value) for value in edge["attributes"]))
            for field in ("external_blockers", "retained_outliving")
            for edge in data_target.get(field, [])
        }
        if blockers != data_boundary:
            raise ValueError(
                f"cleanup target {kind} structured FK boundary differs from catalog retention authority"
            )
        if target.get("semantic_blockers", []) != data_target.get(
            "semantic_blockers", []
        ):
            raise ValueError(
                f"cleanup target {kind} semantic blockers differ from catalog retention authority"
            )
        if kind in {
            "PUBLICATION_COMMIT",
            "PUBLICATION_CANDIDATE",
            "CANONICAL_VALUE",
            "GALLERY_OBSERVATION_PAGE",
        } and target.get("machine_gates", []) != data_target.get("machine_gates", []):
            raise ValueError(
                f"cleanup target {kind} machine gates differ from catalog retention authority"
            )
        if kind in {
            "PUBLICATION_GENERATION",
            "CANONICAL_VALUE",
            "GALLERY_OBSERVATION_PAGE",
        } and target.get("phase_selectors", []) != data_target.get(
            "phase_selectors", []
        ):
            raise ValueError(
                f"cleanup target {kind} phase selectors differ from catalog retention authority"
            )
        deletion_position = {
            relation: (int(phase["order"]), index)
            for phase in target["phases"]
            for index, relation in enumerate(phase["relations"])
        }
        for child in deleted:
            for foreign_key in all_relations[child].get("foreign_keys", []):
                parent = str(foreign_key["relation"])
                if (
                    parent in deleted
                    and not deletion_position[child] < deletion_position[parent]
                ):
                    raise ValueError(
                        f"cleanup target {kind} violates child-first order at {child}->{parent}"
                    )
        used_blockers: set[tuple[str, tuple[str, ...]]] = set()
        seen_edges: set[tuple[str, str, tuple[str, ...]]] = set()
        pending = [root]
        while pending:
            parent = pending.pop()
            for child, attributes in children.get(parent, []):
                edge = (parent, child, attributes)
                if edge in seen_edges:
                    continue
                seen_edges.add(edge)
                edge_id = (child, attributes)
                if edge_id in blockers:
                    used_blockers.add(edge_id)
                    continue
                if child in data_derived:
                    continue
                if child not in deleted:
                    relation_kind = relations[child].get("kind")
                    category = (
                        "derived materialization"
                        if relation_kind == "controlled_materialization"
                        else "owned child"
                    )
                    raise ValueError(
                        f"cleanup target {kind} leaves catalog {category} edge {child}.{'+'.join(attributes)} uncovered"
                    )
                pending.append(child)
        if used_blockers != blockers:
            raise ValueError(f"cleanup target {kind} declares stale retained FK edges")
        for via_path in target.get("required_via_paths", []):
            current = root
            for via_relation in via_path["via"]:
                if not any(
                    child == via_relation for child, _attrs in children.get(current, [])
                ):
                    raise ValueError(
                        f"cleanup target {kind} via path is not an FK edge"
                    )
                current = via_relation
            terminal = (str(via_path["relation"]), tuple(via_path["attributes"]))
            if terminal not in set(children.get(current, [])):
                raise ValueError(
                    f"cleanup target {kind} terminal via blocker is not an FK edge"
                )

    operational_children: dict[str, set[tuple[str, tuple[str, ...]]]] = {}
    for child_name, operational_relation in operational_relations.items():
        for foreign_key in operational_relation.get("foreign_keys", []):
            operational_children.setdefault(str(foreign_key["relation"]), set()).add(
                (child_name, tuple(str(value) for value in foreign_key["attributes"]))
            )
    for kind, target in targets.items():
        root = str(target["root_relation"])
        deleted = {
            relation for phase in target["phases"] for relation in phase["relations"]
        }
        actual_blockers = {
            (str(edge["relation"]), tuple(str(value) for value in edge["attributes"]))
            for edge in target.get("operational_blockers", [])
        }
        # A verticalized cleanup root is protected through every owned catalog
        # child as well as through the PK-only anchor itself.  Operational
        # claims therefore remain blockers when their FK targets a completion
        # seal or another family member instead of the anchor directly.
        catalog_target = data_targets.get(kind)
        protected_catalog_relations = {root}
        if catalog_target is not None:
            protected_catalog_relations.update(
                relation
                for phase in catalog_target.get("child_phases", [])
                for relation in phase
            )
        expected_blockers = {
            edge
            for parent in protected_catalog_relations
            for edge in operational_children.get(parent, set())
            if edge[0] not in deleted
        }
        if kind == "CANONICAL_VALUE":
            expected_blockers |= {
                ("hash_cache_observation", ("source_identity_sha256",)),
                ("hash_cache_observation", ("fingerprint_sha256",)),
            }
        if kind == "OPERATIONAL_PREPARATION":
            # The permanent wide catalog commit points at the effect seal,
            # which is an owned child of this cleanup root.  Keep that
            # cross-manifest boundary explicit even though it is represented
            # as an external logical relation in the operational manifest.
            expected_blockers.add(("publication_commit", ("preparation_id",)))
        if actual_blockers != expected_blockers:
            raise ValueError(
                f"cleanup target {kind} operational FK boundary is incomplete: "
                f"missing={sorted(expected_blockers - actual_blockers)!r} "
                f"stale={sorted(actual_blockers - expected_blockers)!r}"
            )


def check_revision_allocator_contract_v1(
    logical: Mapping[str, Any], _physical: Mapping[str, Any]
) -> None:
    _require_exact_table(
        logical,
        "revision_allocator_contract",
        {
            "relation": "revision_allocator",
            "streams": ["SOURCE", "CATALOG"],
            "current_state_rule": "next_revision_is_in_one_through_int63_maximum_with_maximum_reserved_as_the_exhausted_sentinel",
            "allocation_rule": "under_row_lock_or_exact_next_revision_cas_return_current_next_revision_then_increment_it_by_exactly_one_in_one_transaction; the exhausted sentinel fails closed without mutation",
            "publication_rule": "a published revision belongs to the matching stream and is_strictly_less_than_that_streams_current_next_revision",
        },
    )


def check_cleanup_frozen_root_set_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    """Validate the typed bounded frozen-root membership shape."""

    contract = logical.get("cleanup_reachability_contract")
    if not isinstance(contract, Mapping) or any(
        contract.get(field) != expected
        for field, expected in {
            "version": 1,
            "job_relation": "cleanup_job",
            "frozen_root_relation": "cleanup_cycle_root",
            "frozen_root_key_max_bytes": 260,
            "frozen_root_count_maximum": 256,
        }.items()
    ):
        raise ValueError("cleanup frozen-root contract drifts")
    relations = _raw_relation_map(logical)
    if relations["cleanup_cycle_root"].get("declared_keys") != [
        ["cleanup_id", "frozen_root_key"]
    ] or relations["cleanup_cycle_root"].get("foreign_keys") != [
        {
            "attributes": ["cleanup_id"],
            "relation": "cleanup_job",
            "referenced_attributes": ["cleanup_id"],
        }
    ]:
        raise ValueError("cleanup frozen-root relation authority drifts")
    physical_root = _raw_relation_map(physical)["cleanup_cycle_root"]
    columns = {
        _required_text(column, "attribute", "cleanup frozen-root column"): column
        for column in _raw_tables(
            physical_root.get("column", []), "cleanup frozen-root columns"
        )
    }
    frozen_column = columns.get("frozen_root_key")
    if not isinstance(frozen_column, Mapping):
        raise ValueError("cleanup frozen-root physical column is absent")
    mariadb = frozen_column.get("mariadb")
    if not isinstance(mariadb, Mapping) or mariadb.get("type") != "VARBINARY(260)":
        raise ValueError("cleanup frozen-root maximum physical width drifts")
    checks = {
        _required_text(check, "name", "cleanup frozen-root check"): check
        for check in _raw_tables(
            physical_root.get("check", []), "cleanup frozen-root checks"
        )
    }
    bound = checks.get("ck_cleanup_cycle_root_frame_bounds")
    if (
        not isinstance(bound, Mapping)
        or bound.get("sqlite_expression")
        != ("length(frozen_root_key) >= 3 AND length(frozen_root_key) <= 260")
        or bound.get("mariadb_expression")
        != (
            "octet_length(frozen_root_key) >= 3 AND "
            "octet_length(frozen_root_key) <= 260"
        )
    ):
        raise ValueError("cleanup frozen-root frame check drifts")


def check_gallery_staging_contract_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    """Validate the closed giant-gallery staging protocol and its finite bounds."""

    expected_core = {
        "version": 1,
        "identity_allocator_relation": "identity_allocator",
        "identity_streams": ["GALLERY", "TAG"],
        "observation_allocator_relation": "gallery_observation_allocator",
        "staging_relation": "gallery_observation_staging",
        "claim_relation": "gallery_observation_staging_claim",
        "checkpoint_relation": "gallery_observation_staging_checkpoint",
        "request_relation": "gallery_observation_staging_request",
        "request_budget_relation": "gallery_observation_staging_request_budget",
        "request_chunk_relation": "gallery_observation_staging_request_chunk",
        "request_predecessor_relation": "gallery_observation_staging_request_predecessor",
        "page_request_relation": "gallery_observation_staging_page_request",
        "request_page_relation": "gallery_observation_staging_request_page",
        "receipt_relation": "gallery_observation_staging_receipt",
        "frontier_relation": "gallery_observation_staging_frontier",
        "match_checkpoint_relation": "gallery_observation_staging_match_checkpoint",
        "metadata_parser_relation": "gallery_observation_staging_metadata_parser",
        "canonical_upload_relation": "canonical_value_upload",
        "components": ["FILE", "TAG", "DIRECTORY", "METADATA"],
        "file_leaf_rows": 256,
        "tag_leaf_rows": 256,
        "directory_leaf_rows": 192,
        "metadata_leaf_bytes": 32768,
        "branch_fanout": 256,
        "page_bytes_maximum": 65536,
        "max_level": 8,
        "portable_id_maximum": 9223372036854775807,
        "predecessor_rule": "a predecessor edge is inserted only after locking both ownership-bearing request rows and proving both request identities belong to the same staging_id, the exact request frame names that prior digest, and the prior request has no successor; cross-staging predecessor edges fail closed, and cleanup deletes only outgoing edges owned by the selected staging while a corrupt cross-owner incoming edge is a blocker",
        "durable_parser_phases": [
            "PREFIX",
            "VERSION",
            "GID",
            "TITLE_TAG",
            "TITLE_LENGTH",
            "TITLE",
            "COMMENT_TAG",
            "COMMENT_LENGTH",
            "COMMENT",
            "UPLOAD_ACCOUNT_TAG",
            "UPLOAD_ACCOUNT_LENGTH",
            "UPLOAD_ACCOUNT",
            "UPLOAD_TIME",
            "DOWNLOAD_TIME",
            "MODIFIED_TIME",
            "SCAN_VERSION",
            "SOURCE_FILE_COUNT",
            "PAGE_COUNT_PRESENCE",
            "PAGE_COUNT",
            "DONE",
        ],
        "runtime_parser_phase_rule": "only the in-process decoder aliases TITLE_TEXT, COMMENT_TEXT, ACCOUNT_TAG, ACCOUNT_LENGTH, and ACCOUNT_TEXT map respectively to the persisted TITLE, COMMENT, UPLOAD_ACCOUNT_TAG, UPLOAD_ACCOUNT_LENGTH, and UPLOAD_ACCOUNT states; database rows and public validators accept only the exact ordered durable_parser_phases registry, with ASCII case-sensitive equality and no LENGTH, runtime alias, unknown value, trimming, or normalization",
        "directory_audit_framing": "SHA256(ascii('h2hdb-vnext-directory-observation-audit-v1\\0') || raw32(DIRECTORY root_page_sha256) || u64be(item_count_int63))",
        "metadata_audit_framing": "SHA256(ascii('h2hdb-vnext-metadata-observation-audit-v1\\0') || raw32(METADATA root_page_sha256) || u64be(byte_count_int63))",
        "scan_audit_framing": "SHA256(ascii('h2hdb-vnext-scan-observation-audit-v1\\0') || raw32(FILE root_page_sha256) || u64be(FILE item_count_int63) || raw32(TAG root_page_sha256) || u64be(TAG item_count_int63) || raw32(METADATA root_page_sha256) || u64be(METADATA byte_count_int63) || raw32(DIRECTORY root_page_sha256) || u64be(DIRECTORY item_count_int63))",
        "audit_authority_rule": "gallery_directory_audit_digest, gallery_metadata_audit_digest, and gallery_scan_audit_digest validate these exact fixed frames and component order for diagnostics only; directory_observation_sha256, metadata_fingerprint, and scan_observation_sha256 never authorize observation identity, reuse, parser completion, source membership, sealing, or response-loss progress, all of which require typed roots, exact persisted counters, receipts, checkpoints, and canonical identity",
    }
    contract = logical.get("gallery_staging_contract")
    if not isinstance(contract, Mapping):
        raise ValueError("gallery_staging_contract must be a table")
    if any(contract.get(name) != value for name, value in expected_core.items()):
        raise ValueError("gallery staging structural contract drifts")
    encoded_contract = json.dumps(
        contract, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    if hashlib.sha256(encoded_contract).hexdigest() != (
        "83dae97917cf141292613c33fa53d9ab3623df697b2a2d51ba6b8612da76cd6f"
    ):
        raise ValueError("gallery staging exact protocol text drifts")
    gc_boundary = logical.get("gallery_page_gc_boundary")
    if not isinstance(gc_boundary, Mapping):
        raise ValueError("gallery_page_gc_boundary must be a table")
    encoded_gc = json.dumps(
        gc_boundary, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    if hashlib.sha256(encoded_gc).hexdigest() != (
        "2c475e3590861c1a51cee1c760164d5a90cbf61785dffe1f45b498d55fece3ca"
    ):
        raise ValueError("gallery page GC exact selector or fence drifts")
    relations = _raw_relation_map(logical)
    keys = {
        "identity_allocator": [["stream"]],
        "gallery_observation_allocator": [["gallery_id"]],
        "gallery_observation_staging": [
            ["staging_id"],
            ["build_id"],
            ["gallery_id", "observation_id"],
        ],
        "gallery_observation_staging_claim": [["staging_id"]],
        "gallery_observation_staging_checkpoint": [
            ["staging_id", "component", "level"],
        ],
        "gallery_observation_staging_request": [["request_sha256"]],
        "gallery_observation_staging_request_budget": [["singleton_id"]],
        "gallery_observation_staging_request_chunk": [["request_sha256", "position"]],
        "gallery_observation_staging_request_predecessor": [
            ["request_sha256"],
            ["prior_request_sha256"],
        ],
        "gallery_observation_staging_page_request": [
            ["request_sha256"],
            ["staging_id", "component", "level", "start_cursor"],
        ],
        "gallery_observation_staging_request_page": [["request_sha256"]],
        "gallery_observation_staging_receipt": [
            ["staging_id", "component", "level"],
            ["request_sha256"],
        ],
        "gallery_observation_staging_frontier": [["request_sha256"]],
        "gallery_observation_staging_match_checkpoint": [["staging_id"]],
        "gallery_observation_staging_match_request": [
            ["request_sha256"],
            ["staging_id", "start_matched_count"],
            ["staging_id", "start_file_cursor_bytes"],
        ],
        "gallery_observation_staging_match_receipt": [
            ["staging_id"],
            ["request_sha256"],
        ],
        "gallery_observation_staging_metadata_parser": [["staging_id"]],
        "canonical_value_upload": [["generation", "value_sha256"]],
    }
    for relation_name, expected_keys in keys.items():
        if relations[relation_name].get("declared_keys") != expected_keys:
            raise ValueError(f"{relation_name} staging candidate keys drift")
    request = relations["gallery_observation_staging_request"]
    if {
        "attributes": request.get("attributes"),
        "fds": request.get("fds"),
        "foreign_keys": request.get("foreign_keys", []),
    } != {
        "attributes": ["request_sha256", "staging_id"],
        "fds": [
            {
                "determinant": ["request_sha256"],
                "dependent": ["staging_id"],
            }
        ],
        "foreign_keys": [
            {
                "attributes": ["staging_id"],
                "relation": "gallery_observation_staging",
                "referenced_attributes": ["staging_id"],
            }
        ],
    }:
        raise ValueError(
            "request identity and staging ownership must share one BCNF row"
        )
    if relations["gallery_observation_staging_checkpoint"]["fds"] != [
        {
            "determinant": ["staging_id", "component", "level"],
            "dependent": [
                "cursor",
                "regular_count",
                "processed_byte_count",
                "state",
                "updated_at",
            ],
        },
    ]:
        raise ValueError("checkpoint must omit derivable generations and page facts")
    if relations["gallery_observation_staging_receipt"].get("attributes") != [
        "staging_id",
        "component",
        "level",
        "request_sha256",
        "start_processed_byte_count",
        "next_processed_byte_count",
        "committed_at",
    ]:
        raise ValueError("gallery staging receipt lacks exact FILE byte-count pre/post")
    staging = relations["gallery_observation_staging"]
    if staging.get("fds") != [
        {
            "determinant": ["staging_id"],
            "dependent": [
                "build_id",
                "gallery_id",
                "observation_id",
                "state",
                "created_at",
                "sealed_at",
                "terminal_byte_count",
            ],
        },
        {
            "determinant": ["build_id"],
            "dependent": [
                "staging_id",
                "gallery_id",
                "observation_id",
                "state",
                "created_at",
                "sealed_at",
                "terminal_byte_count",
            ],
        },
        {
            "determinant": ["gallery_id", "observation_id"],
            "dependent": [
                "staging_id",
                "build_id",
                "state",
                "created_at",
                "sealed_at",
                "terminal_byte_count",
            ],
        },
    ]:
        raise ValueError("gallery staging build slot FD drifts")
    physical_relations = _raw_relation_map(physical)
    required_checks = {
        "gallery_observation_staging": {
            "ck_gallery_observation_staging_state_time",
            "ck_gallery_observation_staging_identity_portable",
            "ck_gallery_observation_staging_terminal_byte_count_nonneg",
        },
        "gallery_observation_staging_claim": {
            "ck_gallery_observation_staging_claim_generation_portable",
        },
        "gallery_observation_staging_checkpoint": {
            "ck_gallery_observation_staging_checkpoint_component",
            "ck_gallery_observation_staging_checkpoint_level",
            "ck_gallery_observation_staging_checkpoint_state",
            "ck_gallery_observation_staging_checkpoint_regular_count",
            "ck_gallery_observation_staging_checkpoint_byte_count",
        },
        "gallery_observation_staging_receipt": {
            "ck_gallery_observation_staging_receipt_byte_count",
        },
        "gallery_observation_staging_request_chunk": {
            "ck_gallery_observation_staging_request_chunk_bytes_bounded",
        },
        "gallery_observation_staging_page_request": {
            "ck_gallery_observation_staging_page_request_coordinate",
            "ck_gallery_observation_staging_page_request_terminal",
        },
        "gallery_observation_staging_frontier": {
            "ck_gallery_observation_staging_frontier_coordinate",
        },
        "gallery_observation_staging_match_checkpoint": {
            "ck_gallery_observation_staging_match_checkpoint_state",
            "ck_gallery_observation_staging_match_checkpoint_cursor_bounded",
        },
        "gallery_observation_staging_match_request": {
            "ck_gallery_observation_staging_match_request_terminal",
        },
        "gallery_observation_staging_metadata_parser": {
            "ck_gallery_observation_staging_metadata_parser_phase",
            "ck_gallery_observation_staging_metadata_parser_carry_bounded",
        },
        "gallery_observation_staging_request_budget": {
            "ck_gallery_observation_staging_request_budget_singleton",
            "ck_gallery_observation_staging_request_budget_count",
        },
    }
    for relation_name, names in required_checks.items():
        actual = {
            _required_text(value, "name", f"{relation_name}.check")
            for value in _raw_tables(
                physical_relations[relation_name].get("check", []),
                f"{relation_name}.check",
            )
        }
        if not names <= actual:
            raise ValueError(f"{relation_name} physical staging checks drift")
    parser_checks = {
        _required_text(value, "name", "metadata parser check"): value
        for value in _raw_tables(
            physical_relations["gallery_observation_staging_metadata_parser"].get(
                "check", []
            ),
            "gallery_observation_staging_metadata_parser.check",
        )
    }
    parser_phase_check = parser_checks.get(
        "ck_gallery_observation_staging_metadata_parser_phase"
    )
    expected_phase_expression = (
        "phase IN ("
        + ", ".join(
            f"'{phase}'"
            for phase in cast(list[str], expected_core["durable_parser_phases"])
        )
        + ")"
    )
    if not isinstance(parser_phase_check, Mapping) or any(
        parser_phase_check.get(field) != expected_phase_expression
        for field in ("sqlite_expression", "mariadb_expression")
    ):
        raise ValueError("metadata parser physical phase registry is not exact")


def check_gallery_staging_request_budget_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    """Bind in-band staging retirement to exact bounded request accounting."""

    _require_exact_table(
        logical,
        "gallery_staging_request_budget_contract",
        {
            "version": 1,
            "relation": "gallery_observation_staging_request_budget",
            "request_relation": "gallery_observation_staging_request",
            "singleton_id": 1,
            "hard_retained_request_cap": 1500000,
            "normal_terminal_staging_maximum_per_build": 1,
            "reserve_writer": "GalleryObservationStagingRepository._persist_request_identity",
            "retirement_release_writer": "GalleryObservationStagingRepository.retire_sealed",
            "cleanup_release_writer": "VNextCleanupRepository.advance",
            "retirement_release_phase": "REQUEST_IDENTITY",
            "cleanup_release_phases": [
                "GOS_REQUEST_IDENTITY",
                "GO_STAGING_REQUEST_IDENTITY",
            ],
            "reserve_rule": "after exact response-loss replay resolution and every required allocator lock, but before any fresh request CHILD lock, lock the singleton once at HEAD; page/fact writes performed before that lock remain inside the same transaction and capacity failure rolls them back; exact-CAS plus one for the leaf and each deterministic carry or terminal branch request under that retained lock, require every intermediate count at most 1500000, and roll back the complete delta with every later failure; exact replay never reserves",
            "release_rule": "STAGING_RETIRE and both generic request-identity cleanup phases lock the same singleton HEAD before request CHILD locks, delete at most 256 exact request identities, and exact-CAS subtract only the actual affected-row count in the same transaction; missing singleton, underflow, over-cap value, affected-row mismatch, or rollback leaves both identities and counter unchanged",
            "backpressure_rule": "at 1500000 a fresh request raises public GalleryStagingCapacityError with the retained count and performs zero writes; normal terminal work is automatically retired before another gallery begins, while an oversized OPEN gallery must be abandoned and its lease made stale before exclusive cleanup can reclaim it",
            "full_audit_rule": "the full READY check compares the one bounded singleton value with COUNT of gallery_observation_staging_request and requires exact congruence in zero through 1500000; the O(1) ready probe never scans requests",
        },
    )
    _require_exact_table(
        logical,
        "gallery_staging_retirement_contract",
        {
            "version": 1,
            "staging_relation": "gallery_observation_staging",
            "claim_relation": "gallery_observation_staging_claim",
            "completion_relation": "source_build_gallery",
            "serialized_by_relation": "source_working_build",
            "serialized_by_key": ["slot"],
            "maximum_rows_per_transaction": 256,
            "maximum_terminal_stagings_per_build": 1,
            "terminal_states": ["SEALED", "REUSED"],
            "retiring_states": ["RETIRING_SEALED", "RETIRING_REUSED"],
            "phase_order": [
                {
                    "phase": "RECEIPT_FRONTIER",
                    "order": 1,
                    "relations": [
                        "gallery_observation_staging_receipt",
                        "gallery_observation_staging_frontier",
                        "gallery_observation_staging_match_receipt",
                    ],
                },
                {
                    "phase": "PAGE_ASSOCIATION",
                    "order": 2,
                    "relations": ["gallery_observation_staging_request_page"],
                },
                {
                    "phase": "REQUEST_DESCRIPTOR",
                    "order": 3,
                    "relations": [
                        "gallery_observation_staging_page_request",
                        "gallery_observation_staging_match_request",
                        "gallery_observation_staging_request_predecessor",
                        "gallery_observation_staging_request_chunk",
                    ],
                },
                {
                    "phase": "REQUEST_IDENTITY",
                    "order": 4,
                    "relations": ["gallery_observation_staging_request"],
                },
                {
                    "phase": "CHECKPOINT",
                    "order": 5,
                    "relations": [
                        "gallery_observation_staging_checkpoint",
                        "gallery_observation_staging_match_checkpoint",
                        "gallery_observation_staging_metadata_parser",
                    ],
                },
                {
                    "phase": "CLAIM",
                    "order": 6,
                    "relations": ["gallery_observation_staging_claim"],
                },
                {
                    "phase": "ROOT",
                    "order": 7,
                    "relations": ["gallery_observation_staging"],
                },
            ],
            "implicit_ack_rule": "after the facade accepts a fresh or reconstructed GalleryStagingSeal, the next source advance is the implicit ACK: under the shared gate, exact live ingest fence, source working-build lock and durable link validation, the first retirement transaction CASes SEALED to RETIRING_SEALED or REUSED to RETIRING_REUSED and deletes the first child batch atomically; rollback preserves ordinary seal replay authority, while RETIRING states make every old page or seal retry raise typed GalleryStagingRetiredError",
            "recovery_rule": "a crash after seal commit but before response is first reconstructed as a replayed GalleryStagingSeal without ACK; only its following source advance may ACK. A partial RETIRING staging whose claim still exists is taken over only by a new exact live ingest generation after locking the same working build and incrementing the claim generation; after the CLAIM phase committed and only ROOT remains, the exact live global ingest fence plus the same working-build lock is the recovery authority. STAGING_FIND requires at most one terminal unretired staging and fails closed on an audit-bypassing duplicate",
            "validation_rule": "every batch reconstructs the provisional four-root descriptor, compares its digest with the final gallery_observation identity, requires final stat file_count equal the FILE root count and final stat byte_count equal the immutable staging terminal_byte_count captured from the terminal FILE checkpoint, requires policy-derived gallery_manifest congruence, and enforces SEALED or RETIRING_SEALED link equality versus REUSED or RETIRING_REUSED link inequality; any cross-owner predecessor in either direction blocks retirement",
            "deletion_rule": "each transaction inspects phases in fixed order, processes only the first nonempty phase, locks at most 256 exact rows child-first, and never deletes catalog allocation, pages, roots, normalized facts, final observation, manifest, or source_build_gallery; request identity deletion atomically releases the exact budget count",
            "replay_rule": "after ROOT deletion, the exact source_build_gallery plus final observation identity, stat, manifest policy and manifest are the bounded completion replay authority; no staging history row is retained, and caller-supplied state is nonauthoritative",
            "generic_cleanup_rule": "the exclusive GALLERY_OBSERVATION_STAGING backstop accepts all four terminal states and, before every bounded delete batch in the same transaction, applies the same provisional descriptor, final identity, stat file_count, immutable terminal_byte_count, manifest, and link equality or inequality validation used by in-band retirement; GALLERY_OBSERVATION applies that validator before each staging-control delete batch and retains provisional facts while any SEALED, REUSED, RETIRING_SEALED, or RETIRING_REUSED staging root remains",
        },
    )
    relations = _raw_relation_map(logical)
    budget = relations["gallery_observation_staging_request_budget"]
    if budget != {
        "name": "gallery_observation_staging_request_budget",
        "kind": "source_of_truth",
        "attributes": ["singleton_id", "retained_request_count"],
        "declared_keys": [["singleton_id"]],
        "fds": [
            {
                "determinant": ["singleton_id"],
                "dependent": ["retained_request_count"],
            }
        ],
        "rationale": "One normalized singleton is the global emergency backpressure authority. It stores only the exact bounded aggregate count required for O(1) reserve/release decisions; full READY audit independently compares it with the retained request identities, while normal in-band retirement keeps steady work to one staging gallery.",
    }:
        raise ValueError("gallery staging request budget relation drifts")
    staging = relations["gallery_observation_staging"]
    if staging.get("declared_keys") != [
        ["staging_id"],
        ["build_id"],
        ["gallery_id", "observation_id"],
    ]:
        raise ValueError("gallery staging build slot key drifts")
    physical_budget = _raw_relation_map(physical)[
        "gallery_observation_staging_request_budget"
    ]
    if physical_budget.get("primary_key") != ["singleton_id"]:
        raise ValueError("gallery staging request budget physical key drifts")
    checks = {
        _required_text(item, "name", "gallery request budget check"): item
        for item in _raw_tables(
            physical_budget.get("check", []), "gallery request budget checks"
        )
    }
    expected_checks = {
        "ck_gallery_observation_staging_request_budget_singleton": "singleton_id = 1",
        "ck_gallery_observation_staging_request_budget_count": (
            "retained_request_count >= 0 AND retained_request_count <= 1500000"
        ),
    }
    for name, expression in expected_checks.items():
        check = checks.get(name)
        if not isinstance(check, Mapping) or any(
            check.get(field) != expression
            for field in ("sqlite_expression", "mariadb_expression")
        ):
            raise ValueError("gallery staging request budget physical checks drift")


@dataclass(frozen=True)
class GalleryStagingBatchFacts:
    component: str
    claim_generation: int
    presented_claim_generation: int
    owner_matches: bool
    live_outer_lease: bool
    current_outer_head: bool
    checkpoint_state: str
    cursor: int
    presented_start_cursor: int
    prior_request_key: bytes | None
    presented_prior_request_key: bytes | None
    last_page_present: bool
    next_cursor: int
    entry_count: int
    subtree_item_count: int
    terminal: bool
    commits_complete: bool
    exact_page_recomputed: bool
    normalized_facts_congruent: bool
    boundary_ordered: bool
    metadata_offset_contiguous: bool
    request_key_valid: bool
    latest_receipt_exists: bool
    latest_request_key_matches: bool
    latest_request_frame_matches: bool
    receipt_start_cursor: int | None = None
    receipt_next_cursor: int | None = None


def gallery_staging_batch_state(facts: GalleryStagingBatchFacts) -> str:
    """Return the disjoint COMMIT, REPLAY, or REJECT protocol branch."""

    exact_replay = (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and facts.request_key_valid
        and facts.latest_receipt_exists
        and facts.latest_request_key_matches
        and facts.latest_request_frame_matches
        and facts.receipt_start_cursor == facts.presented_start_cursor
        and facts.receipt_next_cursor == facts.cursor
    )
    if exact_replay:
        return "REPLAY"
    if facts.latest_receipt_exists and facts.latest_request_key_matches:
        return "REJECT"
    capacities = {"FILE": 256, "TAG": 256, "DIRECTORY": 192}
    if facts.component == "METADATA":
        terminal_shape = (
            facts.terminal
            and facts.commits_complete
            and facts.entry_count == 1
            and 1 <= facts.subtree_item_count <= 32768
        )
        nonterminal_shape = (
            not facts.terminal
            and not facts.commits_complete
            and facts.entry_count == 1
            and facts.subtree_item_count == 32768
        )
        cursor_advance_is_exact = (
            facts.next_cursor - facts.cursor == facts.subtree_item_count
        )
        ordering_is_exact = facts.metadata_offset_contiguous
    else:
        capacity = capacities.get(facts.component)
        terminal_shape = (
            capacity is not None
            and facts.terminal
            and facts.commits_complete
            and (
                1 <= facts.entry_count <= capacity
                or (
                    facts.entry_count == 0
                    and facts.subtree_item_count == 0
                    and facts.cursor == 0
                    and not facts.last_page_present
                )
            )
        )
        nonterminal_shape = (
            capacity is not None
            and not facts.terminal
            and not facts.commits_complete
            and facts.entry_count == capacity
        )
        cursor_advance_is_exact = (
            facts.subtree_item_count == facts.entry_count
            and facts.next_cursor - facts.cursor == facts.entry_count
        )
        ordering_is_exact = facts.boundary_ordered
    can_commit = (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and 0 <= facts.claim_generation <= 9223372036854775807
        and facts.checkpoint_state == "OPEN"
        and facts.cursor == facts.presented_start_cursor
        and facts.prior_request_key == facts.presented_prior_request_key
        and facts.next_cursor >= facts.cursor
        and cursor_advance_is_exact
        and facts.next_cursor <= 9223372036854775807
        and (terminal_shape or nonterminal_shape)
        and facts.exact_page_recomputed
        and facts.normalized_facts_congruent
        and ordering_is_exact
        and facts.request_key_valid
    )
    return "COMMIT" if can_commit else "REJECT"


def gallery_staging_batch_authorized(facts: GalleryStagingBatchFacts) -> bool:
    """Only COMMIT mutates; exact REPLAY returns its stored receipt unchanged."""

    return gallery_staging_batch_state(facts) == "COMMIT"


def gallery_staging_frontier_bound(
    *, component_count: int = 4, max_level: int = 8, radix: int = 256
) -> int:
    """Maximum incomplete frontier rows; independent of gallery item count."""

    if component_count < 0 or max_level < 0 or radix < 2:
        raise ValueError("frontier bound parameters are invalid")
    return component_count * max_level * (radix - 1)


def portable_allocator_transition(
    *, current_next_id: int, presented_next_id: int
) -> tuple[int, int] | None:
    """Allocate by an exact current-value CAS; MAX is an exhausted sentinel."""

    maximum = 9223372036854775807
    values = (current_next_id, presented_next_id)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        return None
    if current_next_id != presented_next_id or not 1 <= current_next_id < maximum:
        return None
    return current_next_id, current_next_id + 1


@dataclass(frozen=True)
class GalleryStagingBeginFacts:
    live_outer_lease: bool
    current_outer_head: bool
    existing_staging: bool
    existing_identity_matches: bool
    allocator_advanced: bool
    allocation_present: bool
    staging_header_present: bool
    level_zero_checkpoint_count: int
    claim_count: int
    match_checkpoint_count: int
    metadata_parser_count: int
    higher_level_checkpoint_count: int
    request_row_count: int
    transaction_committed: bool


def gallery_staging_begin_state(facts: GalleryStagingBeginFacts) -> str:
    """Classify the all-or-nothing begin transaction and lost-response retry."""

    complete_state = (
        facts.allocation_present
        and facts.staging_header_present
        and facts.level_zero_checkpoint_count == 4
        and facts.claim_count == 1
        and facts.match_checkpoint_count == 1
        and facts.metadata_parser_count == 1
        and facts.higher_level_checkpoint_count == 0
        and facts.request_row_count == 0
    )
    no_state = (
        not facts.allocator_advanced
        and not facts.allocation_present
        and not facts.staging_header_present
        and facts.level_zero_checkpoint_count == 0
        and facts.claim_count == 0
        and facts.match_checkpoint_count == 0
        and facts.metadata_parser_count == 0
        and facts.higher_level_checkpoint_count == 0
        and facts.request_row_count == 0
    )
    if not facts.live_outer_lease or not facts.current_outer_head:
        return "REJECT"
    if facts.existing_staging:
        return (
            "RESUME"
            if facts.existing_identity_matches
            and not facts.allocator_advanced
            and complete_state
            else "REJECT"
        )
    if facts.transaction_committed:
        return "BEGIN" if facts.allocator_advanced and complete_state else "REJECT"
    return "ROLLED_BACK" if no_state else "REJECT"


def gallery_staging_takeover_authorized(
    *,
    staging_state: str,
    live_outer_lease: bool,
    current_outer_head: bool,
    current_generation: int,
    presented_generation: int,
) -> bool:
    """Takeover is one exact, overflow-safe generation CAS on an OPEN header."""

    return (
        staging_state == "OPEN"
        and live_outer_lease
        and current_outer_head
        and current_generation == presented_generation
        and 0 <= current_generation < 9223372036854775807
    )


@dataclass(frozen=True)
class GalleryStagingCarryFacts:
    claim_generation: int
    presented_claim_generation: int
    owner_matches: bool
    live_outer_lease: bool
    current_outer_head: bool
    component: str
    level: int
    child_count: int
    child_subtree_item_count: int
    encoded_subtree_item_count: int
    children_exact_and_ordered: bool
    internal_checkpoint_present: bool
    frontier_prefix_matches: bool
    page_recomputed: bool
    allocation_page_associated: bool
    request_key_valid: bool
    terminal_flush: bool
    prior_same_level_page_present: bool = False


def gallery_staging_carry_state(facts: GalleryStagingCarryFacts) -> str:
    """Authorize one atomic base-256 internal-page carry or final root flush."""

    registered_component = facts.component in {
        "FILE",
        "TAG",
        "DIRECTORY",
        "METADATA",
    }
    exact_shape = (
        registered_component
        and 1 <= facts.level <= 8
        and 1 <= facts.child_count <= 256
        and 0 <= facts.child_subtree_item_count <= 9223372036854775807
        and facts.encoded_subtree_item_count == facts.child_subtree_item_count
        and facts.children_exact_and_ordered
        and facts.internal_checkpoint_present
        and facts.frontier_prefix_matches
        and facts.page_recomputed
        and facts.allocation_page_associated
        and facts.request_key_valid
    )
    if not (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and 0 <= facts.claim_generation <= 9223372036854775807
        and exact_shape
    ):
        return "REJECT"
    if facts.terminal_flush:
        if facts.prior_same_level_page_present:
            return "FLUSH_CARRY"
        return "REUSE_CHILD_ROOT" if facts.child_count == 1 else "FLUSH_ROOT"
    return "CARRY" if facts.child_count == 256 else "REJECT"


@dataclass(frozen=True)
class GalleryPageDescriptorFact:
    page_sha256: bytes
    component: str
    level: int
    subtree_item_count: int
    first_key: bytes
    last_key: bytes


def validate_gallery_branch_children(
    *,
    component: str,
    level: int,
    children: tuple[GalleryPageDescriptorFact, ...],
) -> int | None:
    """Recompute one branch count and ordering from at most 256 child descriptors."""

    if component not in {"FILE", "TAG", "DIRECTORY", "METADATA"}:
        return None
    if not 1 <= level <= 8 or not 1 <= len(children) <= 256:
        return None
    total = 0
    previous: GalleryPageDescriptorFact | None = None
    for child in children:
        if (
            len(child.page_sha256) != 32
            or child.component != component
            or child.level != level - 1
            or not 1 <= child.subtree_item_count <= 9223372036854775807
            or not child.first_key
            or not child.last_key
            or child.first_key > child.last_key
        ):
            return None
        if component in {"FILE", "TAG", "METADATA"} and (
            len(child.first_key) != 8 or len(child.last_key) != 8
        ):
            return None
        if component in {"FILE", "TAG", "METADATA"} and (
            int.from_bytes(child.first_key, "big") > 9223372036854775807
            or int.from_bytes(child.last_key, "big") > 9223372036854775807
        ):
            return None
        if (
            component == "METADATA"
            and child.level == 0
            and (child.first_key != child.last_key)
        ):
            return None
        if component == "DIRECTORY" and (
            len(child.first_key) > 255 or len(child.last_key) > 255
        ):
            return None
        if previous is not None:
            if component == "METADATA":
                expected = int.from_bytes(previous.first_key, "big") + (
                    previous.subtree_item_count
                )
                if (
                    expected > 9223372036854775807
                    or int.from_bytes(child.first_key, "big") != expected
                ):
                    return None
            elif component in {"FILE", "TAG"}:
                previous_last = int.from_bytes(previous.last_key, "big")
                next_first = int.from_bytes(child.first_key, "big")
                if (
                    previous_last >= 9223372036854775807
                    or next_first != previous_last + 1
                ):
                    return None
            elif previous.last_key >= child.first_key:
                return None
        total += child.subtree_item_count
        if total > 9223372036854775807:
            return None
        previous = child
    return total


_GALLERY_PAGE_REQUEST_PREFIX = b"h2hdb-gallery-staging-page-request\0"
_GALLERY_MATCH_REQUEST_PREFIX = b"h2hdb-gallery-staging-match-request\0"
_GALLERY_COMPONENT_TAGS = {"FILE": 0, "TAG": 1, "DIRECTORY": 2, "METADATA": 3}


def encode_gallery_staging_page_request(
    *,
    staging_id: bytes,
    ingest_generation: int,
    claim_generation: int,
    component: str,
    level: int,
    start_cursor: int,
    prior_request_sha256: bytes | None,
    page_sha256: bytes,
    page_bytes: bytes,
    terminal: bool,
) -> bytes:
    """Return the exact bounded v1 request preimage used for idempotency."""

    integers = (ingest_generation, claim_generation, start_cursor)
    if len(staging_id) != 16 or len(page_sha256) != 32:
        raise ValueError("staging/page identities have the wrong width")
    if not isinstance(page_bytes, bytes) or not 1 <= len(page_bytes) <= 65536:
        raise ValueError("exact page bytes must contain one through 65536 bytes")
    if hashlib.sha256(page_bytes).digest() != page_sha256:
        raise ValueError("page digest does not match the exact page bytes")
    if any(
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 9223372036854775807
        for value in integers
    ):
        raise ValueError("request counter is outside portable int63")
    component_tag = _GALLERY_COMPONENT_TAGS.get(component)
    if component_tag is None or not 0 <= level <= 8:
        raise ValueError("request component or level is unregistered")
    if not isinstance(terminal, bool):
        raise ValueError("terminal intent must be boolean")
    if prior_request_sha256 is None:
        prior_frame = b"\x00"
    elif len(prior_request_sha256) == 32:
        prior_frame = b"\x01" + prior_request_sha256
    else:
        raise ValueError("prior request identity has the wrong width")
    return b"".join(
        (
            _GALLERY_PAGE_REQUEST_PREFIX,
            (1).to_bytes(4, "big"),
            staging_id,
            ingest_generation.to_bytes(8, "big"),
            claim_generation.to_bytes(8, "big"),
            bytes((component_tag, level)),
            start_cursor.to_bytes(8, "big"),
            prior_frame,
            page_sha256,
            len(page_bytes).to_bytes(4, "big"),
            page_bytes,
            bytes((int(terminal),)),
        )
    )


def gallery_staging_request_sha256(request_bytes: bytes) -> bytes:
    """Hash an exact already-domain-prefixed request; conflicts compare all bytes."""

    if not request_bytes.startswith(
        (_GALLERY_PAGE_REQUEST_PREFIX, _GALLERY_MATCH_REQUEST_PREFIX)
    ):
        raise ValueError("request has the wrong domain prefix")
    if not 1 <= len(request_bytes) <= 65792:
        raise ValueError("request preimage is outside its bounded domain")
    return hashlib.sha256(request_bytes).digest()


def split_gallery_staging_request(request_bytes: bytes) -> tuple[bytes, ...]:
    """Split one exact bounded request into deterministic 32768-byte chunks."""

    gallery_staging_request_sha256(request_bytes)
    return tuple(
        request_bytes[offset : offset + 32768]
        for offset in range(0, len(request_bytes), 32768)
    )


def validate_gallery_staging_request_chunks(
    *, request_sha256: bytes, chunks: tuple[tuple[int, bytes], ...]
) -> bytes | None:
    """Validate exact contiguous chunking and return the collision-check preimage."""

    if len(request_sha256) != 32 or not 1 <= len(chunks) <= 3:
        return None
    if tuple(position for position, _chunk in chunks) != tuple(range(len(chunks))):
        return None
    for index, (_position, chunk) in enumerate(chunks):
        if not isinstance(chunk, bytes) or not 1 <= len(chunk) <= 32768:
            return None
        if index < len(chunks) - 1 and len(chunk) != 32768:
            return None
    request_bytes = b"".join(chunk for _position, chunk in chunks)
    if len(request_bytes) > 65792:
        return None
    try:
        digest = gallery_staging_request_sha256(request_bytes)
    except ValueError:
        return None
    return request_bytes if digest == request_sha256 else None


def encode_gallery_staging_match_request(
    *,
    staging_id: bytes,
    ingest_generation: int,
    claim_generation: int,
    start_file_cursor_bytes: bytes,
    start_matched_count: int,
    prior_request_sha256: bytes | None,
    terminal: bool,
) -> bytes:
    """Return the exact disjoint v1 FILE-to-DIRECTORY lookup request frame."""

    if len(staging_id) != 16:
        raise ValueError("staging identity has the wrong width")
    if not 0 <= len(start_file_cursor_bytes) <= 2048:
        raise ValueError("match cursor is outside its bounded domain")
    for value in (ingest_generation, claim_generation, start_matched_count):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= 9223372036854775807
        ):
            raise ValueError("match request counter is outside portable int63")
    if prior_request_sha256 is None:
        prior_frame = b"\x00"
    elif len(prior_request_sha256) == 32:
        prior_frame = b"\x01" + prior_request_sha256
    else:
        raise ValueError("prior request identity has the wrong width")
    if not isinstance(terminal, bool):
        raise ValueError("terminal intent must be boolean")
    return b"".join(
        (
            _GALLERY_MATCH_REQUEST_PREFIX,
            (1).to_bytes(4, "big"),
            staging_id,
            ingest_generation.to_bytes(8, "big"),
            claim_generation.to_bytes(8, "big"),
            len(start_file_cursor_bytes).to_bytes(2, "big"),
            start_file_cursor_bytes,
            start_matched_count.to_bytes(8, "big"),
            prior_frame,
            bytes((int(terminal),)),
        )
    )


def gallery_staging_request_subtype_valid(
    *, page_descriptor_count: int, match_descriptor_count: int
) -> bool:
    """Every exact request identity has one and only one decoded subtype row."""

    return (page_descriptor_count, match_descriptor_count) in {(1, 0), (0, 1)}


def gallery_staging_predecessor_authorized(
    *,
    request_owner_staging_id: bytes,
    prior_owner_staging_id: bytes,
    exact_request_frame_names_prior: bool,
    prior_has_successor: bool,
) -> bool:
    """Reject cross-staging links and one-to-many predecessor forks."""

    return (
        len(request_owner_staging_id) == 16
        and request_owner_staging_id == prior_owner_staging_id
        and exact_request_frame_names_prior
        and not prior_has_successor
    )


def validate_gallery_staging_request_materialization(
    *,
    request_sha256: bytes,
    chunks: tuple[tuple[int, bytes], ...],
    owner_count: int,
    page_descriptor_count: int,
    match_descriptor_count: int,
    reencoded_request_bytes: bytes,
) -> bool:
    """Bind collision-checked chunks to exactly one re-encoded request subtype."""

    exact_bytes = validate_gallery_staging_request_chunks(
        request_sha256=request_sha256, chunks=chunks
    )
    return (
        owner_count == 1
        and gallery_staging_request_subtype_valid(
            page_descriptor_count=page_descriptor_count,
            match_descriptor_count=match_descriptor_count,
        )
        and exact_bytes is not None
        and exact_bytes == reencoded_request_bytes
    )


@dataclass(frozen=True)
class GalleryMetadataParserFacts:
    claim_generation: int
    presented_claim_generation: int
    owner_matches: bool
    live_outer_lease: bool
    current_outer_head: bool
    prior_phase: str
    next_phase: str
    prior_field_remaining: int
    next_field_remaining: int
    prior_utf8_tail: bytes
    next_utf8_tail: bytes
    prior_fixed_carry: bytes
    next_fixed_carry: bytes
    chunk_length: int
    transition_exact: bool
    extracted_scalars_portable: bool
    terminal: bool
    trailing_bytes: bool


def gallery_metadata_parser_state(facts: GalleryMetadataParserFacts) -> str:
    """Validate one crash-safe bounded transition of the METADATA stream parser."""

    phases = {
        "HEADER",
        "TITLE",
        "COMMENT",
        "UPLOAD_ACCOUNT",
        "SCALARS",
        "DONE",
    }
    valid = (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and 0 <= facts.claim_generation <= 9223372036854775807
        and facts.prior_phase in phases
        and facts.next_phase in phases
        and 0 <= facts.prior_field_remaining <= 9223372036854775807
        and 0 <= facts.next_field_remaining <= 9223372036854775807
        and len(facts.prior_utf8_tail) <= 3
        and len(facts.next_utf8_tail) <= 3
        and len(facts.prior_fixed_carry) <= 40
        and len(facts.next_fixed_carry) <= 40
        and 1 <= facts.chunk_length <= 32768
        and facts.transition_exact
        and facts.extracted_scalars_portable
        and not facts.trailing_bytes
    )
    if not valid:
        return "REJECT"
    if facts.terminal:
        return (
            "COMPLETE"
            if facts.next_phase == "DONE"
            and facts.next_field_remaining == 0
            and facts.next_utf8_tail == b""
            and facts.next_fixed_carry == b""
            else "REJECT"
        )
    return "ADVANCE" if facts.next_phase != "DONE" else "REJECT"


@dataclass(frozen=True)
class GalleryMetadataParserState:
    phase: str = "PREFIX"
    field_remaining: int = 0
    utf8_tail: bytes = b""
    fixed_carry: bytes = b""
    scalars: tuple[tuple[str, int], ...] = ()


_GALLERY_METADATA_PREFIX = b"h2hdb-vnext-gallery-observation-metadata\0"
_GALLERY_METADATA_FIXED_PHASES: dict[str, tuple[int, str, str | None]] = {
    "PREFIX": (len(_GALLERY_METADATA_PREFIX), "VERSION", None),
    "VERSION": (4, "GID", "version"),
    "GID": (8, "TITLE_TAG", "gid"),
    "TITLE_TAG": (1, "TITLE_LENGTH", "title_tag"),
    "TITLE_LENGTH": (8, "TITLE", "title_length"),
    "COMMENT_TAG": (1, "COMMENT_LENGTH", "comment_tag"),
    "COMMENT_LENGTH": (8, "COMMENT", "comment_length"),
    "UPLOAD_ACCOUNT_TAG": (1, "UPLOAD_ACCOUNT_LENGTH", "upload_account_tag"),
    "UPLOAD_ACCOUNT_LENGTH": (8, "UPLOAD_ACCOUNT", "upload_account_length"),
    "UPLOAD_TIME": (8, "DOWNLOAD_TIME", "upload_time"),
    "DOWNLOAD_TIME": (8, "MODIFIED_TIME", "download_time"),
    "MODIFIED_TIME": (8, "SCAN_VERSION", "modified_time"),
    "SCAN_VERSION": (4, "SOURCE_FILE_COUNT", "scan_version"),
    "SOURCE_FILE_COUNT": (8, "PAGE_COUNT_PRESENCE", "source_file_count"),
    "PAGE_COUNT_PRESENCE": (1, "DONE", "page_count_presence"),
    "PAGE_COUNT": (4, "DONE", "page_count"),
}
_GALLERY_METADATA_TEXT_NEXT = {
    "TITLE": "COMMENT_TAG",
    "COMMENT": "UPLOAD_ACCOUNT_TAG",
    "UPLOAD_ACCOUNT": "UPLOAD_TIME",
}
_GALLERY_METADATA_SCALAR_PREFIX: dict[str, tuple[str, ...]] = {
    "PREFIX": (),
    "VERSION": (),
    "GID": (),
    "TITLE_TAG": ("gid",),
    "TITLE_LENGTH": ("gid",),
    "TITLE": ("gid", "title_byte_count"),
    "COMMENT_TAG": ("gid", "title_byte_count"),
    "COMMENT_LENGTH": ("gid", "title_byte_count"),
    "COMMENT": ("gid", "title_byte_count", "comment_byte_count"),
    "UPLOAD_ACCOUNT_TAG": ("gid", "title_byte_count", "comment_byte_count"),
    "UPLOAD_ACCOUNT_LENGTH": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
    ),
    "UPLOAD_ACCOUNT": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
    ),
    "UPLOAD_TIME": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
    ),
    "DOWNLOAD_TIME": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
        "upload_time",
    ),
    "MODIFIED_TIME": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
        "upload_time",
        "download_time",
    ),
    "SCAN_VERSION": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
        "upload_time",
        "download_time",
        "modified_time",
    ),
    "SOURCE_FILE_COUNT": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
        "upload_time",
        "download_time",
        "modified_time",
        "scan_version",
    ),
    "PAGE_COUNT_PRESENCE": (
        "gid",
        "title_byte_count",
        "comment_byte_count",
        "upload_account_byte_count",
        "upload_time",
        "download_time",
        "modified_time",
        "scan_version",
        "source_file_count",
    ),
}


def _validate_gallery_metadata_parser_state(state: GalleryMetadataParserState) -> None:
    """Reject forged durable parser states before consuming another chunk."""

    valid_phases = (
        set(_GALLERY_METADATA_FIXED_PHASES)
        | set(_GALLERY_METADATA_TEXT_NEXT)
        | {"DONE"}
    )
    if state.phase not in valid_phases:
        raise ValueError("metadata parser phase is unregistered")
    if not 0 <= state.field_remaining <= 9223372036854775807:
        raise ValueError("metadata field length is outside portable int63")
    if len(state.utf8_tail) > 3 or len(state.fixed_carry) > 40:
        raise ValueError("metadata parser carry is outside its fixed bound")
    if state.phase in _GALLERY_METADATA_TEXT_NEXT:
        if state.fixed_carry:
            raise ValueError("metadata text phase has fixed-field carry")
        length_name = {
            "TITLE": "title_byte_count",
            "COMMENT": "comment_byte_count",
            "UPLOAD_ACCOUNT": "upload_account_byte_count",
        }[state.phase]
        declared_length = dict(state.scalars).get(length_name)
        if (
            declared_length is None
            or not 0 < state.field_remaining <= declared_length
            or len(state.utf8_tail) > declared_length - state.field_remaining
        ):
            raise ValueError("metadata text phase remaining length is incoherent")
        if _decode_utf8_chunk(b"", state.utf8_tail, final=False) != state.utf8_tail:
            raise ValueError("metadata durable UTF-8 tail is not an incomplete prefix")
    elif state.field_remaining or state.utf8_tail:
        raise ValueError("metadata fixed phase has text decoder state")
    if state.phase in _GALLERY_METADATA_FIXED_PHASES:
        width = _GALLERY_METADATA_FIXED_PHASES[state.phase][0]
        if len(state.fixed_carry) >= width:
            raise ValueError("metadata fixed carry must be an incomplete prefix")
    elif state.fixed_carry:
        raise ValueError("metadata non-fixed phase has fixed carry")
    names = tuple(name for name, _value in state.scalars)
    if state.phase == "PAGE_COUNT":
        expected_names = _GALLERY_METADATA_SCALAR_PREFIX["PAGE_COUNT_PRESENCE"] + (
            "page_count_presence",
        )
        if not state.scalars or state.scalars[-1] != ("page_count_presence", 1):
            raise ValueError("metadata page-count phase lacks exact presence authority")
    elif state.phase == "DONE":
        base = _GALLERY_METADATA_SCALAR_PREFIX["PAGE_COUNT_PRESENCE"]
        if len(state.scalars) == len(base) + 1:
            expected_names = base + ("page_count_presence",)
            if state.scalars[-1] != ("page_count_presence", 0):
                raise ValueError("metadata DONE state has invalid absent page count")
        elif len(state.scalars) == len(base) + 2:
            expected_names = base + ("page_count_presence", "page_count")
            if state.scalars[-2][1] != 1:
                raise ValueError("metadata DONE state has invalid present page count")
        else:
            raise ValueError("metadata DONE state lacks its complete scalar prefix")
    else:
        expected_names = _GALLERY_METADATA_SCALAR_PREFIX[state.phase]
    if names != expected_names:
        raise ValueError("metadata durable scalars are not the exact phase prefix")
    for name, value in state.scalars:
        maximum = (
            0xFFFFFFFF
            if name in {"scan_version", "page_count"}
            else 9223372036854775807
        )
        if not 0 <= value <= maximum or name in {"gid", "scan_version"} and value == 0:
            raise ValueError("metadata durable scalar is outside its exact domain")


def _append_metadata_scalar(
    scalars: tuple[tuple[str, int], ...], name: str, value: int
) -> tuple[tuple[str, int], ...]:
    if any(existing == name for existing, _value in scalars):
        raise ValueError("metadata scalar was decoded twice")
    return scalars + ((name, value),)


_GALLERY_METADATA_ROW_SCALARS = (
    "gid",
    "title_byte_count",
    "comment_byte_count",
    "upload_account_byte_count",
    "upload_time",
    "download_time",
    "modified_time",
    "scan_observation_version",
    "source_file_count",
    "page_count",
)


def gallery_metadata_parser_state_to_row(
    state: GalleryMetadataParserState,
) -> dict[str, int | bytes | str | None]:
    """Encode the exact bounded parser state into its physical scalar columns."""

    _validate_gallery_metadata_parser_state(state)
    scalar_values = dict(state.scalars)
    result: dict[str, int | bytes | str | None] = {
        "phase": state.phase,
        "fixed_carry": state.fixed_carry,
        "remaining_text_bytes": state.field_remaining,
        "utf8_tail": state.utf8_tail,
    }
    for column in _GALLERY_METADATA_ROW_SCALARS:
        internal_name = (
            "scan_version" if column == "scan_observation_version" else column
        )
        result[column] = scalar_values.get(internal_name)
    return result


def gallery_metadata_parser_state_from_row(
    row: Mapping[str, int | bytes | str | None],
) -> GalleryMetadataParserState:
    """Decode and validate one durable physical parser-state fixture."""

    expected_columns = {
        "phase",
        "fixed_carry",
        "remaining_text_bytes",
        "utf8_tail",
        *_GALLERY_METADATA_ROW_SCALARS,
    }
    if set(row) != expected_columns:
        raise ValueError("metadata parser row columns drift")
    phase = row["phase"]
    fixed_carry = row["fixed_carry"]
    remaining = row["remaining_text_bytes"]
    utf8_tail = row["utf8_tail"]
    if (
        not isinstance(phase, str)
        or not isinstance(fixed_carry, bytes)
        or isinstance(remaining, bool)
        or not isinstance(remaining, int)
        or not isinstance(utf8_tail, bytes)
    ):
        raise ValueError("metadata parser row core types drift")
    scalars: list[tuple[str, int]] = []
    for column in _GALLERY_METADATA_ROW_SCALARS:
        value = row[column]
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("metadata parser row scalar type drifts")
        internal_name = (
            "scan_version" if column == "scan_observation_version" else column
        )
        scalars.append((internal_name, value))
    if phase == "PAGE_COUNT":
        if row["page_count"] is not None:
            raise ValueError("metadata page-count value appears before decoding")
        scalars.append(("page_count_presence", 1))
    elif phase == "DONE":
        page_count = row["page_count"]
        if page_count is None:
            scalars.append(("page_count_presence", 0))
        else:
            scalars.insert(-1, ("page_count_presence", 1))
    state = GalleryMetadataParserState(
        phase=phase,
        field_remaining=remaining,
        utf8_tail=utf8_tail,
        fixed_carry=fixed_carry,
        scalars=tuple(scalars),
    )
    _validate_gallery_metadata_parser_state(state)
    if gallery_metadata_parser_state_to_row(state) != dict(row):
        raise ValueError("metadata parser row is not canonical")
    return state


def _decode_utf8_chunk(tail: bytes, payload: bytes, *, final: bool) -> bytes:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    decoder.setstate((tail, 0))
    decoder.decode(payload, final=final)
    next_tail, flag = decoder.getstate()
    if flag != 0 or len(next_tail) > 3:
        raise ValueError("invalid durable UTF-8 decoder state")
    return next_tail


def advance_gallery_metadata_parser(
    state: GalleryMetadataParserState,
    chunk: bytes,
    *,
    terminal: bool,
) -> GalleryMetadataParserState:
    """Consume exact METADATA bytes without retaining arbitrary text values."""

    if not 1 <= len(chunk) <= 32768:
        raise ValueError("metadata chunk length must be in 1..32768")
    _validate_gallery_metadata_parser_state(state)
    phase = state.phase
    remaining = state.field_remaining
    utf8_tail = state.utf8_tail
    fixed_carry = state.fixed_carry
    scalars = state.scalars
    offset = 0
    while offset < len(chunk) or (
        phase in _GALLERY_METADATA_TEXT_NEXT and remaining == 0
    ):
        if phase == "DONE":
            raise ValueError("metadata stream has trailing bytes")
        if phase in _GALLERY_METADATA_TEXT_NEXT:
            take = min(remaining, len(chunk) - offset)
            piece = chunk[offset : offset + take]
            offset += take
            remaining -= take
            utf8_tail = _decode_utf8_chunk(utf8_tail, piece, final=remaining == 0)
            if remaining == 0:
                if utf8_tail:
                    raise ValueError("metadata UTF-8 field ends mid-codepoint")
                phase = _GALLERY_METADATA_TEXT_NEXT[phase]
            elif offset == len(chunk):
                break
            continue
        spec = _GALLERY_METADATA_FIXED_PHASES.get(phase)
        if spec is None:
            raise ValueError("metadata parser phase is unregistered")
        width, next_phase, field = spec
        needed = width - len(fixed_carry)
        take = min(needed, len(chunk) - offset)
        fixed_carry += chunk[offset : offset + take]
        offset += take
        if len(fixed_carry) < width:
            break
        token = fixed_carry
        fixed_carry = b""
        if phase == "PREFIX":
            if token != _GALLERY_METADATA_PREFIX:
                raise ValueError("metadata stream prefix mismatch")
        else:
            if field is None:
                raise ValueError("metadata fixed phase lacks its scalar field")
            value = int.from_bytes(token, "big")
            if field == "version" and value != 1:
                raise ValueError("metadata stream version mismatch")
            if field == "title_tag" and value != 1:
                raise ValueError("metadata title tag mismatch")
            if field == "comment_tag" and value != 2:
                raise ValueError("metadata comment tag mismatch")
            if field == "upload_account_tag" and value != 3:
                raise ValueError("metadata upload-account tag mismatch")
            if field in {"title_length", "comment_length", "upload_account_length"}:
                if value > 9223372036854775807:
                    raise ValueError("metadata text length is outside portable int63")
                remaining = value
                scalars = _append_metadata_scalar(
                    scalars,
                    field.replace("_length", "_byte_count"),
                    value,
                )
            elif field == "page_count_presence":
                if value not in {0, 1}:
                    raise ValueError("metadata page-count presence is invalid")
                scalars = _append_metadata_scalar(scalars, field, value)
                next_phase = "PAGE_COUNT" if value == 1 else "DONE"
            elif field not in {
                "version",
                "title_tag",
                "comment_tag",
                "upload_account_tag",
            }:
                if value > 9223372036854775807:
                    raise ValueError("metadata scalar is outside portable int63")
                if field in {"gid", "scan_version"} and value == 0:
                    raise ValueError(f"metadata {field} must be positive")
                scalars = _append_metadata_scalar(scalars, field, value)
        phase = next_phase
    result = GalleryMetadataParserState(
        phase=phase,
        field_remaining=remaining,
        utf8_tail=utf8_tail,
        fixed_carry=fixed_carry,
        scalars=scalars,
    )
    _validate_gallery_metadata_parser_state(result)
    if terminal:
        if (
            result.phase != "DONE"
            or result.field_remaining != 0
            or result.utf8_tail
            or result.fixed_carry
        ):
            raise ValueError("terminal metadata chunk is truncated")
    elif result.phase == "DONE":
        raise ValueError("complete metadata stream requires terminal intent")
    return result


@dataclass(frozen=True)
class GalleryStagingMatchFacts:
    claim_generation: int
    presented_claim_generation: int
    owner_matches: bool
    live_outer_lease: bool
    current_outer_head: bool
    checkpoint_state: str
    cursor: bytes
    presented_cursor: bytes
    prior_request_key: bytes | None
    presented_prior_request_key: bytes | None
    next_cursor: bytes
    matched_count: int
    next_matched_count: int
    file_item_count: int
    directory_regular_count: int
    step_count: int
    terminal: bool
    commits_complete: bool
    file_stream_complete: bool
    directory_stream_complete: bool
    cursor_advance_valid: bool
    file_records_exact: bool
    directory_lookups_exact: bool
    request_key_valid: bool
    latest_receipt_exists: bool
    latest_request_key_matches: bool
    latest_request_frame_matches: bool
    receipt_start_cursor: bytes | None = None
    receipt_next_cursor: bytes | None = None
    receipt_next_matched_count: int | None = None


def gallery_staging_match_state(facts: GalleryStagingMatchFacts) -> str:
    """Bounded FILE traversal plus exact DIRECTORY-tree lookup state machine."""

    exact_replay = (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and facts.request_key_valid
        and facts.latest_receipt_exists
        and facts.latest_request_key_matches
        and facts.latest_request_frame_matches
        and facts.receipt_start_cursor == facts.presented_cursor
        and facts.receipt_next_cursor == facts.cursor
        and facts.receipt_next_matched_count == facts.matched_count
    )
    if exact_replay:
        return "REPLAY"
    if facts.latest_receipt_exists and facts.latest_request_key_matches:
        return "REJECT"
    counts_are_portable = (
        0 <= facts.matched_count <= facts.next_matched_count <= 9223372036854775807
        and 0 <= facts.file_item_count <= 9223372036854775807
        and 0 <= facts.directory_regular_count <= 9223372036854775807
    )
    exact_step = (
        0 <= facts.step_count <= 256
        and facts.next_matched_count - facts.matched_count == facts.step_count
    )
    terminal_shape = (
        facts.terminal
        and facts.commits_complete
        and facts.next_matched_count
        == facts.file_item_count
        == facts.directory_regular_count
        and (facts.step_count > 0 or facts.matched_count == facts.file_item_count)
    )
    nonterminal_shape = (
        not facts.terminal
        and not facts.commits_complete
        and 1 <= facts.step_count <= 256
        and facts.next_matched_count < facts.file_item_count
    )
    can_commit = (
        facts.owner_matches
        and facts.live_outer_lease
        and facts.current_outer_head
        and facts.claim_generation == facts.presented_claim_generation
        and 0 <= facts.claim_generation <= 9223372036854775807
        and facts.checkpoint_state == "OPEN"
        and facts.cursor == facts.presented_cursor
        and facts.prior_request_key == facts.presented_prior_request_key
        and facts.file_stream_complete
        and facts.directory_stream_complete
        and facts.cursor_advance_valid
        and facts.file_records_exact
        and facts.directory_lookups_exact
        and facts.request_key_valid
        and counts_are_portable
        and exact_step
        and (terminal_shape or nonterminal_shape)
    )
    return "COMMIT" if can_commit else "REJECT"


@dataclass(frozen=True)
class GalleryObservationCleanupFacts:
    staging_state: str
    outer_lease_live: bool
    selected_generation: int
    locked_generation: int
    final_reference_present: bool
    reuse_target_is_other_observation: bool = False
    staged_children_remaining: bool = False


@dataclass(frozen=True)
class GalleryStagingCompactionFacts:
    staging_state: str
    exact_header_and_claim_locked: bool
    source_build_gallery_present: bool
    link_names_own_observation: bool
    link_names_other_final_observation: bool
    exclusive_maintenance_gate_held: bool
    rows_selected: int
    maximum_rows: int


def gallery_staging_compaction_authorized(
    facts: GalleryStagingCompactionFacts,
) -> bool:
    """Compact successful staging controls without touching final observation data."""

    durable_outcome = facts.source_build_gallery_present and (
        facts.staging_state == "SEALED"
        and facts.link_names_own_observation
        or facts.staging_state == "REUSED"
        and facts.link_names_other_final_observation
    )
    return (
        durable_outcome
        and facts.exact_header_and_claim_locked
        and facts.exclusive_maintenance_gate_held
        and 0 <= facts.rows_selected <= facts.maximum_rows
        and facts.maximum_rows > 0
    )


def gallery_observation_cleanup_eligible(
    facts: GalleryObservationCleanupFacts,
) -> bool:
    """Only a row-locked, unchanged ABANDONED attempt may be reclaimed."""

    terminal_cleanup_state = facts.staging_state in {"ABANDONED", "ABSENT"} or (
        facts.staging_state == "REUSED" and facts.reuse_target_is_other_observation
    )
    return (
        terminal_cleanup_state
        and not facts.outer_lease_live
        and facts.selected_generation == facts.locked_generation
        and 0 <= facts.locked_generation <= 9223372036854775807
        and not facts.final_reference_present
    )


def gallery_observation_cleanup_batch_state(
    *,
    facts: GalleryObservationCleanupFacts,
    phase: str,
    rows_deleted: int,
    maximum_rows: int,
    receipt_matches: bool,
) -> str:
    """Bounded child-first cleanup for ABANDONED and redundant REUSED attempts."""

    phases = (
        "REQUEST_RECEIPT_FRONTIER",
        "CHECKPOINT_MATCH_PARSER",
        "CANONICAL_UPLOAD_LINK",
        "NORMALIZED_FACTS",
        "TREE_ROOT_ASSOCIATION",
        "STAGING_CLAIM_HEADER",
        "ALLOCATION",
    )
    if not gallery_observation_cleanup_eligible(facts):
        return "REJECT"
    if (
        phase not in phases
        or not 0 <= rows_deleted <= maximum_rows
        or maximum_rows <= 0
    ):
        return "REJECT"
    if not receipt_matches:
        return "REJECT"
    if phase == "ALLOCATION" and facts.staged_children_remaining:
        return "REJECT"
    return "COMPLETE" if phase == "ALLOCATION" else "ADVANCE"


def check_physical_domains_v1(
    _logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    relations = _raw_relation_map(physical)
    uuid16 = {
        "build_id",
        "candidate_id",
        "cleanup_id",
        "event_id",
        "owner_token",
        "preparation_id",
        "request_token",
    }
    digest32 = {
        "batch_key",
        "chain_sha256",
        "event_sha256",
        "file_sha256",
        "fingerprint_sha256",
        "input_sha256",
        "manifest_sha256",
        "output_sha256",
        "prior_chain_sha256",
        "source_identity_sha256",
        "target_key",
    }
    for relation_name, relation in relations.items():
        if relation.get("kind", "table") == "view":
            continue
        if (
            relation_name == "schema_epoch_control"
            or relation.get("status") == "implemented"
        ):
            checks = _raw_tables(relation.get("check", []), f"{relation_name}.check")
            check_names = {
                _required_text(value, "name", f"{relation_name}.check")
                for value in checks
            }
            for column in _raw_tables(
                relation.get("column", []), f"{relation_name}.column"
            ):
                attribute = _required_text(column, "attribute", "physical column")
                mariadb = column.get("mariadb")
                if not isinstance(mariadb, dict):
                    raise ValueError(
                        f"{relation_name}.{attribute} lacks MariaDB domain"
                    )
                type_name = mariadb.get("type")
                if attribute in uuid16 and type_name != "BINARY(16)":
                    raise ValueError(f"{relation_name}.{attribute} is not BINARY(16)")
                if attribute in digest32 and type_name != "BINARY(32)":
                    raise ValueError(f"{relation_name}.{attribute} is not BINARY(32)")
                if attribute in uuid16 | digest32:
                    expected_check = _physical_check_name(
                        relation_name, attribute, check_names
                    )
                    if expected_check is None:
                        raise ValueError(
                            f"{relation_name}.{attribute} lacks an exact-width check"
                        )
    for relation_name in ("download_request", "deletion_request_url"):
        relation = relations[relation_name]
        column = _raw_column(relation, "url")
        if column.get("mariadb", {}).get("type") != "LONGTEXT":
            raise ValueError(f"{relation_name}.url must remain LONGTEXT")
        key_paths = [
            tuple(relation.get("primary_key", [])),
            *(tuple(value) for value in relation.get("unique_keys", [])),
            *(
                tuple(value.get("attributes", []))
                for value in relation.get("required_index", [])
            ),
        ]
        if any("url" in value for value in key_paths):
            raise ValueError(f"{relation_name}.url must not be indexed")
    for relation_name in (
        "operational_preparation_checkpoint",
        "operational_preparation_batch_receipt",
        "cleanup_checkpoint",
        "cleanup_batch_receipt",
    ):
        relation = relations[relation_name]
        for attribute in {"cursor_bytes", "start_cursor", "next_cursor"} & {
            str(value.get("attribute")) for value in relation.get("column", [])
        }:
            if (
                _raw_column(relation, attribute).get("mariadb", {}).get("type")
                != "VARBINARY(2048)"
            ):
                raise ValueError(f"{relation_name}.{attribute} cursor bound drifts")


def check_epoch_manifest_v1(
    _logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    epoch = physical.get("epoch_control")
    if epoch != {
        "relation": "schema_epoch_control",
        "table": "h2hdb_schema_epoch",
        "ownership": "epoch_catalog",
        "provider_slice": False,
        "rationale": "Closed-world physical refinement includes the epoch relation, but generated CREATE-only providers must delegate its creation and exact validation to SchemaEpochCatalog.",
    }:
        raise ValueError("schema epoch ownership contract drifts")
    relation = _raw_relation_map(physical)["schema_epoch_control"]
    manifest = _raw_column(relation, "manifest_sha256")
    if manifest.get("mariadb", {}).get("type") != "BINARY(32)":
        raise ValueError("schema epoch manifest is not an exact SHA-256 domain")
    checks = {
        value.get("name")
        for value in relation.get("check", [])
        if isinstance(value, dict)
    }
    if "ck_schema_epoch_manifest_blob" not in checks:
        raise ValueError("schema epoch manifest exact-width check is absent")


def check_bootstrap_contract_v1(
    logical: Mapping[str, Any], physical: Mapping[str, Any]
) -> None:
    _validate_bootstrap(logical, physical, _raw_relation_map(logical))


def validate_operational_fk_access_paths(physical: Mapping[str, Any]) -> None:
    """Require a PK/UK/explicit-index left prefix for every child-side FK."""

    for relation_name, relation in _raw_relation_map(physical).items():
        if relation.get("kind", "table") == "view":
            continue
        access_paths = [
            tuple(str(value) for value in relation.get("primary_key", [])),
            *(
                tuple(str(item) for item in value)
                for value in relation.get("unique_keys", [])
            ),
            *(
                tuple(str(item) for item in value.get("attributes", []))
                for value in relation.get("required_index", [])
                if isinstance(value, dict)
            ),
        ]
        for foreign_key in _raw_tables(
            relation.get("foreign_key", []), f"{relation_name}.foreign_key"
        ):
            attributes = _required_texts(
                foreign_key, "attributes", f"{relation_name} foreign key"
            )
            if not any(path[: len(attributes)] == attributes for path in access_paths):
                name = _required_text(foreign_key, "name", "foreign key")
                raise ValueError(
                    f"{relation_name}.{name} lacks a child-side left-prefix access path"
                )


def _validate_bootstrap(
    logical: Mapping[str, Any],
    physical: Mapping[str, Any],
    logical_relations: Mapping[str, dict[str, Any]],
) -> tuple[
    tuple[BootstrapSeed, ...],
    tuple[BootstrapSeedRange, ...],
    tuple[str, ...],
    tuple[str, ...],
    str,
]:
    contract = logical.get("bootstrap_contract")
    if not isinstance(contract, dict) or set(contract) != {
        "version",
        "seed_validation_lifecycle",
        "absence_validation_lifecycle",
        "epoch_owned_relation",
        "seeded_relations",
        "absent_relations",
        "absence_rule",
        "epoch_rule",
    }:
        raise ValueError("bootstrap_contract fields are not closed-world")
    if contract.get("version") != 1:
        raise ValueError("bootstrap_contract.version must be exactly 1")
    if (
        contract.get("seed_validation_lifecycle") != "building_only"
        or contract.get("absence_validation_lifecycle") != "building_only"
    ):
        raise ValueError("bootstrap seed/absence validation must be BUILDING-only")
    epoch_owned = _required_text(contract, "epoch_owned_relation", "bootstrap")
    seeded = _required_texts(contract, "seeded_relations", "bootstrap")
    absent = _required_texts(contract, "absent_relations", "bootstrap")
    _required_text(contract, "absence_rule", "bootstrap")
    _required_text(contract, "epoch_rule", "bootstrap")
    if epoch_owned != "schema_epoch_control":
        raise ValueError("bootstrap epoch-owned relation drifts")
    if seeded != (
        "revision_allocator",
        "identity_allocator",
        "deletion_request_generation",
        "deletion_request_generation_head",
        "gallery_observation_staging_request_budget",
        "cleanup_target_kind",
        "cleanup_phase",
        "cleanup_sweep_target",
    ):
        raise ValueError("operational genesis registry relation set drifts")
    if len(absent) != len(set(absent)) or set(seeded) & set(absent):
        raise ValueError("bootstrap relation partition overlaps or repeats")
    if set(seeded) | set(absent) | {epoch_owned} != set(logical_relations):
        raise ValueError("bootstrap relation partition is incomplete")
    allocator_rows = {
        "h2hdb.operational.revision-allocator.source.v1": "SOURCE",
        "h2hdb.operational.revision-allocator.catalog.v1": "CATALOG",
    }
    identity_allocator_rows = {
        "h2hdb.operational.identity-allocator.gallery.v1": "GALLERY",
        "h2hdb.operational.identity-allocator.tag.v1": "TAG",
        "h2hdb.operational.identity-allocator.policy.v1": "POLICY",
    }
    deletion_generation_rows = {
        "h2hdb.operational.deletion-request-generation.genesis.v1": (
            "deletion_request_generation",
            (0, 0),
            ("uint64", "unix_microseconds"),
        ),
        "h2hdb.operational.deletion-request-generation-head.genesis.v1": (
            "deletion_request_generation_head",
            (1, 0, 0),
            ("uint64", "uint64", "unix_microseconds"),
        ),
    }
    request_budget_rows = {
        "h2hdb.operational.gallery-staging-request-budget.genesis.v1": (
            "gallery_observation_staging_request_budget",
            (1, 0),
            ("uint64", "uint64"),
        ),
    }
    targets = _raw_tables(logical.get("cleanup_target", []), "cleanup_target")
    target_rows = {
        f"h2hdb.operational.cleanup-target.{str(value['target_kind']).lower().replace('_', '-')}.v1": (
            str(value["target_kind"]),
        )
        for value in targets
    }
    phase_rows = {
        f"h2hdb.operational.cleanup-phase.{str(phase['phase']).lower().replace('_', '-')}.v1": (
            str(phase["phase"]),
            str(target["target_kind"]),
            int(phase["order"]),
        )
        for target in targets
        for phase in _raw_tables(target.get("phases", []), "cleanup phases")
    }
    result: list[BootstrapSeed] = []
    for raw in _raw_tables(logical.get("bootstrap_seed", []), "bootstrap_seed"):
        if set(raw) != {
            "id",
            "columns",
            "values",
            "version",
            "lifecycle",
            "relation",
            "value",
            "invariant",
        }:
            raise ValueError("bootstrap seed fields are not closed-world")
        seed_id = _required_text(raw, "id", "bootstrap seed")
        relation = _required_text(raw, "relation", "bootstrap seed")
        if (
            raw.get("version") != 1
            or raw.get("lifecycle") != "building_only"
            or relation not in seeded
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} version/relation drifts")
        invariant = _required_text(raw, "invariant", f"bootstrap seed {seed_id}")
        cells: list[BootstrapCell] = []
        for cell in _raw_tables(
            raw.get("value", []), f"bootstrap seed {seed_id}.value"
        ):
            attribute = _required_text(cell, "attribute", "bootstrap cell")
            value_type = _required_text(cell, "type", "bootstrap cell")
            if value_type == "ascii_enum" and set(cell) == {
                "attribute",
                "type",
                "text",
            }:
                value: str | int = _required_text(cell, "text", "bootstrap cell")
            elif value_type in {"uint64", "unix_microseconds"} and set(cell) == {
                "attribute",
                "type",
                "integer",
            }:
                integer = cell.get("integer")
                if (
                    not isinstance(integer, int)
                    or isinstance(integer, bool)
                    or not 0 <= integer < 2**64
                ):
                    raise ValueError("bootstrap integer is outside uint64")
                value = integer
            else:
                raise ValueError("bootstrap cell has an unsupported typed shape")
            cells.append(BootstrapCell(attribute, value_type, value))
        relation_attributes = tuple(logical_relations[relation]["attributes"])
        if tuple(value.attribute for value in cells) != relation_attributes:
            raise ValueError("bootstrap seed does not cover attributes in order")
        exact_values = tuple(value.value for value in cells)
        exact_types = tuple(value.value_type for value in cells)
        expected_values: tuple[object, ...] | None
        expected_types: tuple[str, ...]
        if relation == "revision_allocator":
            expected_values = (allocator_rows.get(seed_id), 1, 0)
            expected_types = ("ascii_enum", "uint64", "unix_microseconds")
        elif relation == "identity_allocator":
            expected_values = (identity_allocator_rows.get(seed_id), 1, 0)
            expected_types = (
                "ascii_enum",
                "uint64",
                "unix_microseconds",
            )
        elif relation in {
            "deletion_request_generation",
            "deletion_request_generation_head",
            "gallery_observation_staging_request_budget",
        }:
            singleton_row = (deletion_generation_rows | request_budget_rows).get(
                seed_id
            )
            if singleton_row is None or singleton_row[0] != relation:
                expected_values = None
                expected_types = ()
            else:
                expected_values = singleton_row[1]
                expected_types = singleton_row[2]
        elif relation == "cleanup_target_kind":
            expected_values = target_rows.get(seed_id)
            expected_types = ("ascii_enum",)
        else:
            expected_values = phase_rows.get(seed_id)
            expected_types = ("ascii_enum", "ascii_enum", "uint64")
        if (
            expected_values is None
            or exact_values != expected_values
            or exact_types != expected_types
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} has the wrong typed values")
        if tuple(raw.get("columns", [])) != relation_attributes or tuple(
            raw.get("values", [])
        ) != tuple(str(value) for value in exact_values):
            raise ValueError(f"bootstrap seed {seed_id!r} portable row drifts")
        result.append(
            BootstrapSeed(
                seed_id,
                1,
                "building_only",
                relation,
                tuple(cells),
                invariant,
            )
        )
    ids = [value.seed_id for value in result]
    expected_ids = (
        set(allocator_rows)
        | set(identity_allocator_rows)
        | set(deletion_generation_rows)
        | set(request_budget_rows)
        | set(target_rows)
        | set(phase_rows)
    )
    if len(ids) != len(set(ids)) or set(ids) != expected_ids:
        raise ValueError("bootstrap seed IDs are duplicate or incomplete")
    ranges = _raw_tables(
        logical.get("bootstrap_seed_range", []), "bootstrap_seed_range"
    )
    expected_range_kinds = {
        "SOURCE_BUILD": "7b973d41884dbcdc84faa93629b8db70",
        "ANALYSIS_RUN": "aee565cf30cb51de9e454dfcb1577234",
        "CATALOG_PUBLICATION": "322a87b56f3c8fac8d3b5985d8cc11bd",
        "PUBLICATION_COMMIT": "44bfa9ebeb45cf3ff4630d01fa0c04f5",
        "CATALOG_REVISION_DESCRIPTOR": "c289a29354f92824e2c1a4fbb2eaf37c",
        "SOURCE_REVISION_DESCRIPTOR": "9a7d079272849273b3d015fce8899fa4",
        "PUBLICATION_GENERATION": "d24e2784401c1d65d634c015bee1bb7d",
        "PUBLICATION_CANDIDATE": "dc636b256645946128b728969d39be48",
        "OPERATIONAL_PREPARATION": "93f08650a665d7d4d98b72e183ed7e74",
        "GALLERY_OBSERVATION": "a5ea90668cd7204ebc7d72e131405102",
        "ARTIFACT_BLOB": "8d0db7c218d2a04b41580511c91cdd19",
        "CANONICAL_VALUE": "089873b3842efcfad4669d8ca21ac3b1",
        "CONTENT_BLOB": "b25317c47fc6b84dc61e079bd6dbe8bd",
        "GALLERY_OBSERVATION_PAGE": "c12831ed560200c722ead234511f58cd",
        "CANONICAL_VALUE_UPLOAD": "64933eb72132d5fd01c727ad607ac544",
        "GALLERY_OBSERVATION_STAGING": "912c50427d1731ac3aa4e9a205099bd4",
        "FILE_NAME_IDENTITY": "f948fd92fd7922e73a9355e49cf4814a",
        "PUBLICATION_IDENTITY": "fb6c69d2c3f04eca7b476f654f425843",
        "GALLERY_IDENTITY": "cd3e95057f39f5a06a08a0b5e14f9682",
        "SOURCE_GALLERY_NAME_GID": "c976a237cd5c7f8e68b29150af31ae6f",
        "GALLERY_UPLOAD_TIME": "56ec4397b9e9c240398b3e1baf1d1c75",
        "HASH_CACHE_OBSERVATION": "b09b8a0a89a3167806670e37a16c7f71",
    }
    range_kinds: set[str] = set()
    validated_ranges: list[BootstrapSeedRange] = []
    for seed_range in ranges:
        if set(seed_range) != {
            "id",
            "version",
            "lifecycle",
            "relation",
            "target_kind",
            "shard_start",
            "shard_end",
            "key_codec",
            "target_kind_tag_hex",
            "invariant",
        }:
            raise ValueError("bootstrap sweep range fields are not closed-world")
        kind = _required_text(seed_range, "target_kind", "bootstrap sweep range")
        if (
            kind in range_kinds
            or seed_range.get("version") != 1
            or seed_range.get("lifecycle") != "building_only"
            or seed_range.get("relation") != "cleanup_sweep_target"
            or seed_range.get("shard_start") != 0
            or seed_range.get("shard_end") != 255
            or seed_range.get("key_codec") != "target_kind_tag16_u64be_zero8_v1"
            or seed_range.get("target_kind_tag_hex") != expected_range_kinds.get(kind)
        ):
            raise ValueError("bootstrap sweep range binding drifts")
        range_kinds.add(kind)
        tag = bytes.fromhex(str(seed_range["target_kind_tag_hex"]))
        if (
            tag
            != hashlib.sha256(
                b"h2hdb-cleanup-target-v1\0" + kind.encode("ascii")
            ).digest()[:16]
        ):
            raise ValueError("bootstrap sweep range tag differs from writer encoder")
        validated_ranges.append(
            BootstrapSeedRange(
                _required_text(seed_range, "id", "bootstrap sweep range"),
                kind,
                int(seed_range["shard_start"]),
                int(seed_range["shard_end"]),
                str(seed_range["key_codec"]),
                tag,
            )
        )
    if range_kinds != set(expected_range_kinds):
        raise ValueError("bootstrap sweep ranges are incomplete")
    if physical.get("bootstrap_contract") != logical.get("bootstrap_contract"):
        raise ValueError("physical bootstrap contract drifts from logical")
    if physical.get("bootstrap_seed") != logical.get("bootstrap_seed"):
        raise ValueError("physical bootstrap seeds drift from logical")
    if physical.get("bootstrap_seed_range") != logical.get("bootstrap_seed_range"):
        raise ValueError("physical bootstrap sweep ranges drift from logical")
    return tuple(result), tuple(validated_ranges), seeded, absent, epoch_owned


def _require_exact_table(
    document: Mapping[str, Any], key: str, expected: Mapping[str, Any]
) -> None:
    if document.get(key) != expected:
        raise ValueError(f"{key} differs from its executable v1 contract")


def _raw_tables(value: object, context: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{context} must be an array of tables")
    return tuple(value)


def _raw_relation_map(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    values = _raw_tables(document.get("relation", []), "relation")
    result = {_required_text(value, "name", "relation"): value for value in values}
    if len(result) != len(values):
        raise ValueError("relation names must be unique")
    return result


def _required_text(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{context}.{key} must be a non-empty string")
    return result


def _required_texts(
    value: Mapping[str, Any], key: str, context: str
) -> tuple[str, ...]:
    result = value.get(key)
    if (
        not isinstance(result, list)
        or not result
        or not all(isinstance(item, str) and item for item in result)
    ):
        raise ValueError(f"{context}.{key} must be a non-empty string array")
    return tuple(result)


def _positive_int(value: object, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{context} must be a positive integer")
    return value


def _raw_column(relation: Mapping[str, Any], attribute: str) -> dict[str, Any]:
    for value in _raw_tables(relation.get("column", []), "relation.column"):
        if value.get("attribute") == attribute:
            return value
    raise ValueError(f"physical relation lacks column {attribute!r}")


def _physical_check_name(
    relation: str, attribute: str, check_names: set[str]
) -> str | None:
    full = f"ck_{relation}_{attribute}_len"
    if full in check_names:
        return full
    prefix = full[:50] + "_"
    return next((value for value in check_names if value.startswith(prefix)), None)


def load_external_stubs(path: str | Path) -> tuple[ExternalStub, ...]:
    """Load closed external-relation stubs declared by a physical manifest."""

    with Path(path).open("rb") as stream:
        document = tomllib.load(stream)
    raw_stubs = document.get("external_stub", [])
    if not isinstance(raw_stubs, list):
        raise ValueError("physical external_stub must be an array")
    stubs: list[ExternalStub] = []
    for raw_stub in raw_stubs:
        if not isinstance(raw_stub, dict):
            raise ValueError("physical external_stub must contain tables")
        raw_columns = raw_stub.get("column")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError("external_stub.column must be a non-empty array")
        columns: list[tuple[str, str, str]] = []
        nullable_columns: list[str] = []
        for raw_column in raw_columns:
            if not isinstance(raw_column, dict):
                raise ValueError("external_stub.column must contain tables")
            columns.append(
                (
                    _string(raw_column, "name"),
                    _string(raw_column, "sqlite_type"),
                    _string(raw_column, "mariadb_type"),
                )
            )
            nullable = raw_column.get("nullable")
            if not isinstance(nullable, bool):
                raise ValueError("external_stub.column.nullable must be boolean")
            if nullable:
                nullable_columns.append(_string(raw_column, "name"))
        stubs.append(
            ExternalStub(
                _string(raw_stub, "relation"),
                _string(raw_stub, "table"),
                tuple(columns),
                _strings(raw_stub, "primary_key"),
                tuple(
                    tuple(str(value) for value in key)
                    for key in raw_stub.get("unique_keys", [])
                ),
                tuple(nullable_columns),
            )
        )
    if len({stub.relation for stub in stubs}) != len(stubs):
        raise ValueError("external_stub contains duplicate relations")
    return tuple(stubs)


def provider_relation_names(path: str | Path) -> tuple[str, ...]:
    """Return CREATE-only provider relations, excluding epoch-catalog control."""

    with Path(path).open("rb") as stream:
        document = tomllib.load(stream)
    source_slice = _strings(document, "source_slice")
    epoch_control = document.get("epoch_control")
    if not isinstance(epoch_control, dict):
        raise ValueError("operational physical manifest lacks epoch_control ownership")
    if (
        _string(epoch_control, "relation") != "schema_epoch_control"
        or _string(epoch_control, "table") != "h2hdb_schema_epoch"
        or _string(epoch_control, "ownership") != "epoch_catalog"
        or epoch_control.get("provider_slice") is not False
    ):
        raise ValueError("schema epoch control must be owned by epoch_catalog")
    if "schema_epoch_control" in source_slice:
        raise ValueError("provider source_slice must exclude schema epoch control")
    if tuple(document.get("epoch_owned_relations", [])) != ("schema_epoch_control",):
        raise ValueError("epoch_owned_relations must contain exact epoch control")
    return source_slice


def load_combined_logical_schema(
    logical_path: str | Path,
) -> tuple[refinement.LogicalSchema, tuple[str, ...]]:
    """Load local operational relations plus exact external candidate-key shapes."""

    local = refinement.load_logical_schema(logical_path)
    with Path(logical_path).open("rb") as stream:
        document = tomllib.load(stream)
    external_relations: list[refinement.LogicalRelation] = []
    for raw in document.get("external_relation", []):
        external_relations.append(
            refinement.LogicalRelation(
                _string(raw, "name"),
                _strings(raw, "attributes"),
                tuple(
                    frozenset(str(value) for value in key)
                    for key in raw["declared_keys"]
                ),
                (),
            )
        )
    return (
        refinement.LogicalSchema(
            local.name,
            (*local.relations, *external_relations),
        ),
        tuple(relation.name for relation in local.relations),
    )


def load_operational_physical_schema(
    path: str | Path,
    logical_schema: refinement.LogicalSchema,
    local_relation_names: tuple[str, ...],
) -> refinement.PhysicalSchema:
    """Load and close-world validate a complete operational physical manifest."""

    with Path(path).open("rb") as stream:
        document = tomllib.load(stream)
    relations = tuple(_physical_relation(raw) for raw in document.get("relation", []))
    physical = refinement.PhysicalSchema(
        _string(document, "name"),
        _string(document, "logical_contract"),
        _strings(document, "complete_relations"),
        relations,
    )
    if physical.logical_contract != logical_schema.name:
        raise ValueError("operational physical contract name does not match logical")
    if set(physical.source_slice) != set(local_relation_names):
        raise ValueError(
            "operational complete_relations must equal every local relation"
        )
    expected_provider_relations = tuple(
        name for name in physical.source_slice if name != "schema_epoch_control"
    )
    if provider_relation_names(path) != expected_provider_relations:
        raise ValueError(
            "operational provider slice order differs from owned relations"
        )
    if {relation.relation for relation in relations} != set(local_relation_names):
        raise ValueError("operational physical coverage must be complete")
    if any(relation.status != "implemented" for relation in relations):
        raise ValueError("operational physical relations must all be implemented")
    stub_by_relation = {stub.relation: stub for stub in load_external_stubs(path)}
    external_names = {
        relation.name
        for relation in logical_schema.relations
        if relation.name not in local_relation_names
    }
    if set(stub_by_relation) != external_names:
        raise ValueError("external physical stubs differ from external logical shapes")
    for stub in stub_by_relation.values():
        logical = logical_schema.relation(stub.relation)
        assert logical is not None
        if {column[0] for column in stub.columns} != set(logical.attributes):
            raise ValueError(f"external stub {stub.relation!r} columns drift")
        physical_keys = {
            frozenset(stub.primary_key),
            *(frozenset(key) for key in stub.unique_keys),
        }
        if physical_keys != set(logical.candidate_keys):
            raise ValueError(f"external stub {stub.relation!r} keys drift")
        for key in stub.unique_keys:
            if _external_runtime_only_key(stub, key):
                if stub.relation not in {
                    "canonical_value_page",
                    "gallery_observation_page",
                }:
                    raise ValueError("unexpected runtime-only external key")
    for relation in relations:
        logical = logical_schema.relation(relation.relation)
        assert logical is not None
        if {column.attribute for column in relation.columns} != set(logical.attributes):
            raise ValueError(f"physical columns drift for {relation.relation!r}")
        physical_keys = {
            frozenset(relation.primary_key),
            *(frozenset(key) for key in relation.unique_keys),
        }
        if physical_keys != set(logical.candidate_keys):
            raise ValueError(f"physical keys drift for {relation.relation!r}")
        physical_fks = {
            (
                foreign_key.attributes,
                foreign_key.referenced_relation,
                foreign_key.referenced_attributes,
            )
            for foreign_key in relation.foreign_keys
        }
        logical_fks = {
            (
                foreign_key.attributes,
                foreign_key.referenced_relation,
                foreign_key.referenced_attributes,
            )
            for foreign_key in logical.foreign_keys
        }
        if physical_fks != logical_fks:
            raise ValueError(f"physical foreign keys drift for {relation.relation!r}")
        if relation.kind == "view" and (relation.required_indexes or relation.checks):
            raise ValueError(
                f"physical view {relation.relation!r} cannot declare indexes or checks"
            )
        for foreign_key in relation.foreign_keys:
            target = logical_schema.relation(foreign_key.referenced_relation)
            if target is None or frozenset(
                foreign_key.referenced_attributes
            ) not in set(target.candidate_keys):
                raise ValueError(
                    f"foreign key {foreign_key.name!r} does not target a declared key"
                )
        if relation.derived_view is not None:
            available_sources = {value.relation for value in relations} | set(
                stub_by_relation
            )
            unknown_sources = (
                set(relation.derived_view.source_relations) - available_sources
            )
            if unknown_sources:
                raise ValueError(
                    f"derived view {relation.relation!r} has unknown sources "
                    f"{sorted(unknown_sources)!r}"
                )
    return physical


def compare_operational_refinement(
    logical_schema: refinement.LogicalSchema,
    local_relation_names: tuple[str, ...],
    physical_schema: refinement.PhysicalSchema,
    physical_path: str | Path,
    database: refinement.DatabaseShape,
) -> refinement.PhysicalRefinementReport:
    """Compare all local tables and every external FK stub with real metadata."""

    stubs = load_external_stubs(physical_path)
    mappings = [relation.as_mapping() for relation in physical_schema.relations]
    mappings.extend(_external_mapping(stub) for stub in stubs)
    logical_report = refinement.compare_refinement(
        logical_schema,
        database,
        mappings,
    )
    mismatches = list(logical_report.mismatches)
    relation_by_name = {
        relation.relation: relation for relation in physical_schema.relations
    }
    relation_by_name.update(
        (stub.relation, _external_physical_relation(stub)) for stub in stubs
    )
    for relation in physical_schema.relations:
        assert relation.table is not None
        table = database.table(relation.table)
        if table is not None:
            mismatches.extend(
                refinement._compare_physical_details(  # noqa: SLF001
                    relation,
                    table,
                    database.backend,
                    relation_by_name,
                )
            )
    return refinement.PhysicalRefinementReport(
        physical_schema.name,
        logical_schema.name,
        database.backend,
        tuple(local_relation_names),
        (),
        tuple(sorted(set(mismatches))),
    )


def _external_mapping(stub: ExternalStub) -> refinement.RelationMapping:
    runtime_keys = tuple(
        frozenset(key)
        for key in stub.unique_keys
        if _external_runtime_only_key(stub, key)
    )
    return refinement.RelationMapping(
        stub.relation,
        stub.table,
        runtime_candidate_keys=runtime_keys,
    )


def _external_physical_relation(
    stub: ExternalStub,
) -> refinement.PhysicalRelationSpec:
    """Supply column/table names needed to render cross-plane derived views."""

    return refinement.PhysicalRelationSpec(
        relation=stub.relation,
        status="implemented",
        rationale="Cross-plane physical rendering stub.",
        table=stub.table,
        columns=tuple(
            refinement.PhysicalColumnSpec(
                name,
                name,
                refinement.BackendColumnSpec(
                    sqlite_type,
                    name in stub.nullable_columns,
                    None,
                ),
                refinement.BackendColumnSpec(
                    mariadb_type,
                    name in stub.nullable_columns,
                    None,
                ),
            )
            for name, sqlite_type, mariadb_type in stub.columns
        ),
        primary_key=stub.primary_key,
        unique_keys=stub.unique_keys,
    )


def _ddl_identifier(value: str) -> str:
    """Return a deterministic identifier below MariaDB's 64-byte ceiling."""

    if len(value.encode("ascii")) <= 63:
        return value
    digest = hashlib.sha256(value.encode("ascii")).hexdigest()[:12]
    return value[:50] + "_" + digest


def _ddl_relation_order(
    physical: refinement.PhysicalSchema,
) -> tuple[str, ...]:
    """Return a stable parent-before-child order for inline foreign keys."""

    relation_by_name = {relation.relation: relation for relation in physical.relations}
    preferred_order = physical.source_slice
    if (
        len(relation_by_name) != len(physical.relations)
        or len(preferred_order) != len(relation_by_name)
        or set(preferred_order) != set(relation_by_name)
    ):
        raise ValueError(
            "operational physical source_slice does not exactly cover relations"
        )

    local_names = set(relation_by_name)
    dependencies: dict[str, set[str]] = {}
    for name in preferred_order:
        relation = relation_by_name[name]
        derived_view = relation.derived_view
        dependencies[name] = {
            foreign_key.referenced_relation
            for foreign_key in relation.foreign_keys
            if foreign_key.referenced_relation in local_names
            and foreign_key.referenced_relation != name
        } | (
            set(derived_view.source_relations) & local_names if derived_view else set()
        )
    remaining = set(preferred_order)
    result: list[str] = []
    while remaining:
        ready = next(
            (
                name
                for name in preferred_order
                if name in remaining and not dependencies[name] & remaining
            ),
            None,
        )
        if ready is None:
            unresolved = {
                name: sorted(dependencies[name] & remaining)
                for name in preferred_order
                if name in remaining
            }
            raise ValueError(f"operational physical dependency cycle: {unresolved!r}")
        remaining.remove(ready)
        result.append(ready)
    return tuple(result)


def render_sqlite_external_stubs(stubs: tuple[ExternalStub, ...]) -> str:
    statements: list[str] = []
    for stub in stubs:
        definitions = [
            f'"{name}" {sqlite_type}'
            + (" NULL" if name in stub.nullable_columns else " NOT NULL")
            for name, sqlite_type, _mariadb_type in stub.columns
        ]
        definitions.extend(
            f'CHECK (length("{name}") = {width})'
            for name, _sqlite_type, mariadb_type in stub.columns
            if (width := _binary_width(mariadb_type)) is not None
        )
        definitions.extend(_sqlite_external_checks(stub))
        definitions.append(
            "PRIMARY KEY (" + ", ".join(f'"{name}"' for name in stub.primary_key) + ")"
        )
        definitions.extend(
            "UNIQUE (" + ", ".join(f'"{name}"' for name in key) + ")"
            for key in stub.unique_keys
            if not _external_runtime_only_key(stub, key)
        )
        statements.append(
            f'CREATE TABLE "{stub.table}" (\n  ' + ",\n  ".join(definitions) + "\n);"
        )
    return "\n".join(statements)


def render_sqlite_ddl(
    physical: refinement.PhysicalSchema,
    stubs: tuple[ExternalStub, ...],
) -> str:
    """Render external key stubs followed by the complete operational schema."""

    table_by_relation = {
        **{stub.relation: stub.table for stub in stubs},
        **{relation.relation: str(relation.table) for relation in physical.relations},
    }
    relation_by_name = {relation.relation: relation for relation in physical.relations}
    relation_by_name.update(
        (stub.relation, _external_physical_relation(stub)) for stub in stubs
    )
    statements = ["PRAGMA foreign_keys = ON;", render_sqlite_external_stubs(stubs)]
    for relation_name in _ddl_relation_order(physical):
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                refinement._render_view(  # noqa: SLF001
                    relation,
                    relation_by_name,
                    "sqlite",
                    idempotent=False,
                )
                + ";"
            )
            continue
        definitions: list[str] = []
        for column in relation.columns:
            backend = column.sqlite
            definition = f'"{column.column}" {backend.type_name}'
            if backend.collation is not None:
                definition += f" COLLATE {backend.collation}"
            definition += " NULL" if backend.nullable else " NOT NULL"
            definitions.append(definition)
        definitions.append(
            f'CONSTRAINT "pk_{relation.table}" PRIMARY KEY ('
            + ", ".join(
                f'"{relation.column_for(value)}"' for value in relation.primary_key
            )
            + ")"
        )
        definitions.extend(
            f'CONSTRAINT "{_ddl_identifier(f"uk_{relation.table}_{position}")}" UNIQUE ('
            + ", ".join(f'"{relation.column_for(value)}"' for value in key)
            + ")"
            for position, key in enumerate(relation.unique_keys, 1)
        )
        for foreign_key in relation.foreign_keys:
            target = relation_by_name.get(foreign_key.referenced_relation)
            target_columns = (
                tuple(
                    target.column_for(value)
                    for value in foreign_key.referenced_attributes
                )
                if target is not None
                else foreign_key.referenced_attributes
            )
            definitions.append(
                f'CONSTRAINT "{foreign_key.name}" FOREIGN KEY ('
                + ", ".join(
                    f'"{relation.column_for(value)}"'
                    for value in foreign_key.attributes
                )
                + f') REFERENCES "{table_by_relation[foreign_key.referenced_relation]}" ('
                + ", ".join(f'"{value}"' for value in target_columns)
                + ")"
            )
        definitions.extend(
            f'CONSTRAINT "{check.name}" CHECK ({check.sqlite_expression})'
            for check in relation.checks
        )
        statements.append(
            f'CREATE TABLE "{relation.table}" (\n  '
            + ",\n  ".join(definitions)
            + "\n);"
        )
        for index in relation.required_indexes:
            statements.append(
                f'CREATE {"UNIQUE " if index.unique else ""}INDEX "{index.name}" '
                f'ON "{relation.table}" ('
                + ", ".join(
                    f'"{relation.column_for(value)}"' for value in index.attributes
                )
                + ");"
            )
    return "\n".join(statements)


def render_mariadb_external_stubs(stubs: tuple[ExternalStub, ...]) -> tuple[str, ...]:
    statements: list[str] = []
    for stub in stubs:
        definitions = [
            f"`{name}` {mariadb_type}"
            + (" NULL" if name in stub.nullable_columns else " NOT NULL")
            for name, _sqlite_type, mariadb_type in stub.columns
        ]
        definitions.extend(_mariadb_external_checks(stub))
        definitions.append(
            "PRIMARY KEY (" + ", ".join(f"`{name}`" for name in stub.primary_key) + ")"
        )
        definitions.extend(
            "UNIQUE (" + ", ".join(f"`{name}`" for name in key) + ")"
            for key in stub.unique_keys
            if not _external_runtime_only_key(stub, key)
        )
        statements.append(
            f"CREATE TABLE `{stub.table}` (\n  "
            + ",\n  ".join(definitions)
            + "\n) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 "
            "COLLATE=utf8mb4_nopad_bin"
        )
    return tuple(statements)


def _external_runtime_only_key(stub: ExternalStub, key: tuple[str, ...]) -> bool:
    """Return keys whose exact arbitrary payload uniqueness is runtime-enforced."""

    return stub.relation in {
        "canonical_value_page",
        "gallery_observation_page",
    } and key == ("page_bytes",)


def _sqlite_external_checks(stub: ExternalStub) -> tuple[str, ...]:
    portable_max = 9223372036854775807
    if stub.relation in {
        "gallery_identity",
    }:
        return (f'CHECK ("gallery_id" >= 1 AND "gallery_id" <= {portable_max})',)
    if stub.relation == "gallery_observation_allocation":
        return (
            f'CHECK ("gallery_id" >= 1 AND "gallery_id" <= {portable_max})',
            f'CHECK ("observation_id" >= 1 AND "observation_id" <= {portable_max})',
            f'CHECK ("allocated_at" >= 0 AND "allocated_at" <= {portable_max})',
        )
    if stub.relation == "gallery_observation_page":
        return ('CHECK (length("page_bytes") BETWEEN 1 AND 65536)',)
    if stub.relation == "gallery_observation_page_descriptor":
        return (
            "CHECK (\"component\" IN (X'46494C45', X'544147', "
            "X'4449524543544F5259', X'4D45544144415441'))",
            'CHECK ("level" >= 0 AND "level" <= 8)',
            f'CHECK ("subtree_item_count" >= 0 AND '
            f'"subtree_item_count" <= {portable_max})',
        )
    if stub.relation == "gallery_observation_page_key_bounds":
        return (
            'CHECK (length("first_key") BETWEEN 1 AND 255)',
            'CHECK (length("last_key") BETWEEN 1 AND 255)',
            'CHECK ("first_key" <= "last_key")',
        )
    if stub.relation == "gallery_observation_page_child":
        return ('CHECK ("position" >= 0 AND "position" <= 255)',)
    if stub.relation == "gallery_observation_tree_root":
        return (
            f'CHECK ("gallery_id" >= 1 AND "gallery_id" <= {portable_max})',
            f'CHECK ("observation_id" >= 1 AND "observation_id" <= {portable_max})',
        )
    if stub.relation == "canonical_value_page":
        return ('CHECK (length("page_bytes") BETWEEN 1 AND 65536)',)
    if stub.relation == "canonical_value_page_descriptor":
        return (
            'CHECK ("level" BETWEEN 0 AND 8)',
            f'CHECK ("page_position" >= 0 AND "page_position" <= {portable_max})',
            f'CHECK ("subtree_item_count" >= 0 AND "subtree_item_count" <= {portable_max})',
        )
    if stub.relation == "canonical_value_page_parent":
        return ('CHECK ("position" BETWEEN 0 AND 255)',)
    return ()


def _mariadb_external_checks(stub: ExternalStub) -> tuple[str, ...]:
    portable_max = 9223372036854775807
    if stub.relation in {
        "gallery_identity",
    }:
        return (f"CHECK (`gallery_id` >= 1 AND `gallery_id` <= {portable_max})",)
    if stub.relation == "gallery_observation_allocation":
        return (
            f"CHECK (`gallery_id` >= 1 AND `gallery_id` <= {portable_max})",
            f"CHECK (`observation_id` >= 1 AND `observation_id` <= {portable_max})",
            f"CHECK (`allocated_at` >= 0 AND `allocated_at` <= {portable_max})",
        )
    if stub.relation == "gallery_observation_page":
        return ("CHECK (octet_length(`page_bytes`) BETWEEN 1 AND 65536)",)
    if stub.relation == "gallery_observation_page_descriptor":
        return (
            "CHECK (`component` IN (X'46494C45', X'544147', "
            "X'4449524543544F5259', X'4D45544144415441'))",
            "CHECK (`level` >= 0 AND `level` <= 8)",
            f"CHECK (`subtree_item_count` >= 0 AND "
            f"`subtree_item_count` <= {portable_max})",
        )
    if stub.relation == "gallery_observation_page_key_bounds":
        return (
            "CHECK (octet_length(`first_key`) BETWEEN 1 AND 255)",
            "CHECK (octet_length(`last_key`) BETWEEN 1 AND 255)",
            "CHECK (`first_key` <= `last_key`)",
        )
    if stub.relation == "gallery_observation_page_child":
        return ("CHECK (`position` >= 0 AND `position` <= 255)",)
    if stub.relation == "gallery_observation_tree_root":
        return (
            f"CHECK (`gallery_id` >= 1 AND `gallery_id` <= {portable_max})",
            f"CHECK (`observation_id` >= 1 AND `observation_id` <= {portable_max})",
        )
    if stub.relation == "canonical_value_page":
        return ("CHECK (octet_length(`page_bytes`) BETWEEN 1 AND 65536)",)
    if stub.relation == "canonical_value_page_descriptor":
        return (
            "CHECK (`level` BETWEEN 0 AND 8)",
            f"CHECK (`page_position` >= 0 AND `page_position` <= {portable_max})",
            f"CHECK (`subtree_item_count` >= 0 AND `subtree_item_count` <= {portable_max})",
        )
    if stub.relation == "canonical_value_page_parent":
        return ("CHECK (`position` BETWEEN 0 AND 255)",)
    return ()


def render_mariadb_ddl(
    physical: refinement.PhysicalSchema,
    stubs: tuple[ExternalStub, ...],
) -> tuple[str, ...]:
    """Render external key stubs followed by the complete operational schema."""

    table_by_relation = {
        **{stub.relation: stub.table for stub in stubs},
        **{relation.relation: str(relation.table) for relation in physical.relations},
    }
    relation_by_name = {relation.relation: relation for relation in physical.relations}
    relation_by_name.update(
        (stub.relation, _external_physical_relation(stub)) for stub in stubs
    )
    statements = list(render_mariadb_external_stubs(stubs))
    for relation_name in _ddl_relation_order(physical):
        relation = relation_by_name[relation_name]
        assert relation.table is not None
        if relation.kind == "view":
            statements.append(
                refinement._render_view(  # noqa: SLF001
                    relation,
                    relation_by_name,
                    "mariadb",
                    idempotent=False,
                )
            )
            continue
        definitions: list[str] = []
        for column in relation.columns:
            backend = column.mariadb
            definition = f"`{column.column}` {backend.type_name}"
            if backend.collation is not None:
                definition += f" COLLATE {backend.collation}"
            definition += " NULL" if backend.nullable else " NOT NULL"
            definitions.append(definition)
        definitions.append(
            "PRIMARY KEY ("
            + ", ".join(
                f"`{relation.column_for(value)}`" for value in relation.primary_key
            )
            + ")"
        )
        definitions.extend(
            f"CONSTRAINT `{_ddl_identifier(f'uk_{relation.table}_{position}')}` UNIQUE ("
            + ", ".join(f"`{relation.column_for(value)}`" for value in key)
            + ")"
            for position, key in enumerate(relation.unique_keys, 1)
        )
        for foreign_key in relation.foreign_keys:
            target = relation_by_name.get(foreign_key.referenced_relation)
            target_columns = (
                tuple(
                    target.column_for(value)
                    for value in foreign_key.referenced_attributes
                )
                if target is not None
                else foreign_key.referenced_attributes
            )
            definitions.append(
                f"CONSTRAINT `{foreign_key.name}` FOREIGN KEY ("
                + ", ".join(
                    f"`{relation.column_for(value)}`"
                    for value in foreign_key.attributes
                )
                + f") REFERENCES `{table_by_relation[foreign_key.referenced_relation]}` ("
                + ", ".join(f"`{value}`" for value in target_columns)
                + ")"
            )
        definitions.extend(
            f"CONSTRAINT `{check.name}` CHECK ({check.mariadb_expression})"
            for check in relation.checks
        )
        statements.append(
            f"CREATE TABLE `{relation.table}` (\n  "
            + ",\n  ".join(definitions)
            + "\n) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 "
            "COLLATE=utf8mb4_nopad_bin"
        )
        for index in relation.required_indexes:
            statements.append(
                f"CREATE {'UNIQUE ' if index.unique else ''}INDEX `{index.name}` "
                f"ON `{relation.table}` ("
                + ", ".join(
                    f"`{relation.column_for(value)}`" for value in index.attributes
                )
                + ")"
            )
    return tuple(statements)


def _physical_relation(raw: dict[str, Any]) -> refinement.PhysicalRelationSpec:
    relation_name = _string(raw, "name")
    kind = str(raw.get("kind", "table"))
    if kind not in {"table", "view"}:
        raise ValueError(f"physical relation {relation_name!r} has invalid kind")
    raw_view = raw.get("view")
    derived_view = None
    if kind == "view":
        if not isinstance(raw_view, dict):
            raise ValueError(f"physical view {relation_name!r} lacks view metadata")
        derived_view = refinement.parse_derived_view_spec(raw_view, relation_name)
    elif raw_view is not None:
        raise ValueError(f"physical table {relation_name!r} declares a view")
    columns = tuple(
        refinement.PhysicalColumnSpec(
            _string(column, "attribute"),
            _string(column, "name"),
            _backend(column["sqlite"]),
            _backend(column["mariadb"]),
        )
        for column in raw.get("column", [])
    )
    foreign_keys = tuple(
        refinement.PhysicalForeignKeySpec(
            _string(foreign_key, "name"),
            _strings(foreign_key, "attributes"),
            _string(foreign_key, "referenced_relation"),
            _strings(foreign_key, "referenced_attributes"),
        )
        for foreign_key in raw.get("foreign_key", [])
    )
    indexes = tuple(
        refinement.PhysicalIndexSpec(
            _string(index, "name"),
            _strings(index, "attributes"),
            bool(index["unique"]),
        )
        for index in raw.get("required_index", [])
    )
    checks = tuple(
        refinement.PhysicalCheckSpec(
            _string(check, "name"),
            _string(check, "sqlite_expression"),
            _string(check, "mariadb_expression"),
        )
        for check in raw.get("check", [])
    )
    return refinement.PhysicalRelationSpec(
        relation=relation_name,
        status=_string(raw, "status"),
        rationale=_string(raw, "rationale"),
        table=_string(raw, "table"),
        columns=columns,
        primary_key=_strings(raw, "primary_key"),
        unique_keys=tuple(
            tuple(str(value) for value in key) for key in raw.get("unique_keys", [])
        ),
        foreign_keys=foreign_keys,
        required_indexes=indexes,
        checks=checks,
        kind=kind,
        derived_view=derived_view,
    )


def _backend(raw: dict[str, Any]) -> refinement.BackendColumnSpec:
    collation = raw.get("collation")
    return refinement.BackendColumnSpec(
        _string(raw, "type"),
        bool(raw["nullable"]),
        None if collation == "NONE" else str(collation),
    )


def _binary_width(type_name: str) -> int | None:
    if not type_name.startswith("BINARY(") or not type_name.endswith(")"):
        return None
    return int(type_name.removeprefix("BINARY(").removesuffix(")"))


def _string(value: dict[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{key} must be a non-empty string")
    return result


def _strings(value: dict[str, object], key: str) -> tuple[str, ...]:
    result = value.get(key)
    if (
        not isinstance(result, list)
        or not result
        or not all(isinstance(item, str) and item for item in result)
    ):
        raise ValueError(f"{key} must be a non-empty string array")
    return tuple(result)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate operational physical refinement machine contracts."
    )
    parser.add_argument("logical", type=Path)
    parser.add_argument("physical", type=Path)
    arguments = parser.parse_args()
    contract = validate_operational_machine_contract(
        arguments.logical, arguments.physical
    )
    print(
        "operational machine contract: "
        f"{len(contract.obligations)} obligations, "
        f"{len(contract.seeds)} BUILDING-only seeds, "
        f"{len(contract.absent_relations)} genesis-absent relations"
    )


if __name__ == "__main__":
    main()
