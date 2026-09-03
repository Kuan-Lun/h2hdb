#!/usr/bin/env python3
"""Generate the explicit operational physical contract from operational.toml."""

from __future__ import annotations

import argparse
import hashlib
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
LOGICAL = ROOT / "verification" / "schema" / "operational.toml"
CATALOG_LOGICAL = ROOT / "verification" / "schema" / "catalog.toml"
CATALOG_PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
OUTPUT = ROOT / "verification" / "schema" / "operational_physical.toml"

TABLE_OVERRIDES = {
    "schema_epoch_control": "h2hdb_schema_epoch",
}

UUID16 = {
    "build_id",
    "candidate_id",
    "cleanup_id",
    "event_id",
    "owner_token",
    "preparation_id",
    "request_token",
    "staging_id",
    "deletion_request_token",
}
DIGEST32 = {
    "batch_key",
    "chain_sha256",
    "event_sha256",
    "file_sha256",
    "fingerprint_sha256",
    "final_chain_sha256",
    "frozen_root_set_sha256",
    "input_sha256",
    "manifest_sha256",
    "manifest_chain_sha256",
    "start_manifest_chain_sha256",
    "next_manifest_chain_sha256",
    "output_sha256",
    "prior_chain_sha256",
    "receipt_batch_key",
    "receipt_input_sha256",
    "receipt_prior_chain_sha256",
    "page_sha256",
    "last_page_sha256",
    "root_page_sha256",
    "request_sha256",
    "prior_request_sha256",
    "source_identity_sha256",
    "target_key",
    "value_sha256",
}
TIMESTAMPS = {
    "acked_at",
    "activated_at",
    "allocated_at",
    "applied_at",
    "assigned_at",
    "bound_at",
    "cached_at",
    "claimed_at",
    "committed_at",
    "completed_at",
    "consumed_at",
    "cycle_cutoff_at",
    "created_at",
    "last_evaluated_at",
    "last_optimized_at",
    "last_transition_at",
    "lease_expires_at",
    "observed_at",
    "prepared_at",
    "ready_at",
    "redownload_at",
    "requested_at",
    "sealed_at",
    "started_at",
    "updated_at",
}
COUNTERS = {
    "accumulated_work",
    "algorithm_version",
    "allocator_generation",
    "cycle_generation",
    "committed_generation",
    "completed_generation",
    "current_generation",
    "deleted_count",
    "final_deleted_count",
    "frozen_root_count",
    "deletion_request_generation",
    "download_generation",
    "epoch",
    "event_count",
    "gate_generation",
    "generation",
    "start_generation",
    "ingest_generation",
    "claim_generation",
    "gid",
    "max_batch_rows",
    "max_rows_per_transaction",
    "next_revision",
    "operational_policy_id",
    "operational_schema_version",
    "processed_count",
    "prior_deleted_count",
    "receipt_prior_deleted_count",
    "receipt_row_count",
    "start_processed_count",
    "next_processed_count",
    "processed_gallery_count",
    "processed_file_count",
    "processed_byte_count",
    "terminal_byte_count",
    "start_gallery_count",
    "next_gallery_count",
    "start_file_count",
    "next_file_count",
    "start_byte_count",
    "next_byte_count",
    "start_processed_byte_count",
    "next_processed_byte_count",
    "phase_order",
    "row_count",
    "shard_no",
    "schema_version",
    "sequence_no",
    "source_revision",
    "through_source_revision",
    "through_sequence_no",
    "version",
    "hash_cache_max_age_microseconds",
    "gallery_id",
    "item_count",
    "level",
    "next_id",
    "next_observation_id",
    "observation_id",
    "position",
    "precommit_generation",
    "cursor",
    "subtree_item_count",
    "regular_count",
    "retained_request_count",
    "matched_count",
    "start_matched_count",
    "remaining_text_bytes",
    "title_byte_count",
    "comment_byte_count",
    "upload_account_byte_count",
    "upload_time",
    "download_time",
    "modified_time",
    "scan_observation_version",
    "source_file_count",
    "page_count",
    "file_position",
    "directory_position",
    "verified_regular_count",
    "revision",
    "reserved_revision",
}
SMALL_COUNTERS = {"singleton_id", "slot", "terminal"}
BINARY_CURSOR = {
    "cursor_bytes",
    "start_cursor",
    "receipt_start_cursor",
    "next_cursor",
    "file_cursor_bytes",
    "start_file_cursor_bytes",
}
ENUMS = {
    "event_type",
    "handoff_kind",
    "key_codec",
    "mode",
    "phase",
    "selection_order_id",
    "state",
    "next_state",
    "stream",
    "target_kind",
}
NAMES: set[str] = set()

SEMANTIC_OBLIGATION_CHECKS = {
    "h2hdb.operational.physical-domains.v1": (
        "ready_validation",
        "physical_domain",
        "operational_refinement.check_physical_domains_v1",
    ),
    "h2hdb.operational.epoch-manifest.v1": (
        "building_to_ready",
        "manifest_integrity",
        "operational_refinement.check_epoch_manifest_v1",
    ),
    "h2hdb.operational.fencing.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_fencing_contract_v1",
    ),
    "h2hdb.operational.download-ingest-handoff.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_download_ingest_handoff_contract_v1",
    ),
    "h2hdb.operational.maintenance-gate.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_maintenance_gate_contract_v1",
    ),
    "h2hdb.operational.bounded-work.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_bounded_work_contract_v1",
    ),
    "h2hdb.operational.queue-history.v1": (
        "ready_and_runtime",
        "referential_protocol",
        "operational_refinement.check_queue_history_contract_v1",
    ),
    "h2hdb.operational.canonical-hash-cache.v1": (
        "ready_and_runtime",
        "canonical_digest",
        "operational_refinement.check_canonical_hash_cache_contract_v1",
    ),
    "h2hdb.operational.event-integrity.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_event_integrity_contract_v1",
    ),
    "h2hdb.operational.build-generation.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_build_generation_contract_v1",
    ),
    "h2hdb.operational.attempt-identity.v1": (
        "ready_and_runtime",
        "identity_protocol",
        "operational_refinement.check_attempt_identity_contract_v1",
    ),
    "h2hdb.operational.cleanup-reachability.v1": (
        "ready_and_runtime",
        "retention_protocol",
        "operational_refinement.check_cleanup_reachability_v1",
    ),
    "h2hdb.operational.cleanup-frozen-root-set.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_cleanup_frozen_root_set_v1",
    ),
    "h2hdb.operational.revision-allocation.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_revision_allocator_contract_v1",
    ),
    "h2hdb.operational.gallery-staging.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_gallery_staging_contract_v1",
    ),
    "h2hdb.operational.gallery-staging-request-budget.v1": (
        "ready_and_runtime",
        "transaction_protocol",
        "operational_refinement.check_gallery_staging_request_budget_v1",
    ),
    "h2hdb.operational.bootstrap-genesis.v1": (
        "building_only",
        "bootstrap_integrity",
        "operational_refinement.check_bootstrap_contract_v1",
    ),
}
SEMANTIC_VALIDATOR_HOOK = (
    "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider.semantic_validators"
)
GENERATION_OBLIGATION_BINDINGS = {
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
            "publication_commit",
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


def _column(relation: str, attribute: str) -> tuple[str, bool, str, str]:
    nullable = (
        attribute
        in {
            "completed_at",
            "last_page_sha256",
            "ready_at",
            "root_page_sha256",
        }
        or attribute == "sealed_at"
        and relation == "gallery_observation_staging"
        or attribute == "terminal_byte_count"
        and relation == "gallery_observation_staging"
        or relation == "cleanup_job"
        and attribute in {"final_chain_sha256", "final_deleted_count"}
        or relation == "cleanup_checkpoint"
        and attribute
        in {
            "receipt_batch_key",
            "receipt_start_cursor",
            "receipt_prior_chain_sha256",
            "receipt_prior_deleted_count",
            "receipt_input_sha256",
            "receipt_row_count",
        }
        or (
            relation == "gallery_observation_staging_metadata_parser"
            and attribute
            in {
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
            }
        )
    )
    if attribute == "request_bytes":
        return attribute, nullable, "BLOB", "BLOB"
    if attribute == "fixed_carry":
        return attribute, nullable, "BLOB", "VARBINARY(40)"
    if attribute == "utf8_tail":
        return attribute, nullable, "BLOB", "VARBINARY(3)"
    if attribute == "component":
        return attribute, nullable, "BLOB", "VARBINARY(9)"
    if attribute == "frozen_root_key":
        return attribute, nullable, "BLOB", "VARBINARY(260)"
    if attribute in {"file_page_sha256", "directory_page_sha256"}:
        return attribute, nullable, "BLOB", "BINARY(32)"
    if (
        relation == "gallery_observation_staging_page_request"
        and attribute == "start_cursor"
    ):
        return attribute, nullable, "INTEGER", "BIGINT UNSIGNED"
    if attribute in UUID16:
        return attribute, nullable, "BLOB", "BINARY(16)"
    if attribute in DIGEST32:
        return attribute, nullable, "BLOB", "BINARY(32)"
    if attribute in {"scan_observation_version", "page_count"}:
        return attribute, nullable, "INTEGER", "INT UNSIGNED"
    if attribute in TIMESTAMPS:
        return attribute, nullable, "INTEGER", "BIGINT UNSIGNED"
    if attribute in COUNTERS:
        return attribute, nullable, "INTEGER", "BIGINT UNSIGNED"
    if attribute in SMALL_COUNTERS:
        return attribute, nullable, "INTEGER", "SMALLINT UNSIGNED"
    if attribute in BINARY_CURSOR:
        return attribute, nullable, "BLOB", "VARBINARY(2048)"
    if attribute in ENUMS:
        return attribute, nullable, "TEXT", "VARCHAR(48)"
    if attribute in NAMES:
        return attribute, nullable, "TEXT", "VARCHAR(191)"
    if attribute == "url":
        return attribute, nullable, "TEXT", "LONGTEXT"
    raise ValueError(f"No operational physical domain for {attribute!r}")


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _identifier(value: str) -> str:
    if len(value) <= 63:
        return value
    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return value[:50] + "_" + digest


def _array(values: tuple[str, ...] | list[str]) -> str:
    return "[" + ", ".join(_quote(value) for value in values) + "]"


def _validate_external_candidate_key_shapes(logical: dict[str, Any]) -> None:
    """Bind every external FK target to the catalog contract's real keys."""

    with CATALOG_LOGICAL.open("rb") as stream:
        catalog: dict[str, Any] = tomllib.load(stream)
    catalog_relations = {
        str(relation["name"]): relation for relation in catalog["relation"]
    }
    for external in logical.get("external_relation", []):
        name = str(external["name"])
        target = catalog_relations.get(name)
        if target is None:
            raise ValueError(f"External relation {name!r} is absent from catalog.toml")
        attributes = frozenset(str(value) for value in external["attributes"])
        target_attributes = frozenset(str(value) for value in target["attributes"])
        if not attributes <= target_attributes:
            raise ValueError(
                f"External relation {name!r} declares attributes absent from catalog.toml"
            )
        external_keys = {
            frozenset(str(value) for value in key) for key in external["declared_keys"]
        }
        visible_catalog_keys = {
            frozenset(str(value) for value in key)
            for key in target["declared_keys"]
            if frozenset(str(value) for value in key) <= attributes
        }
        if external_keys != visible_catalog_keys:
            raise ValueError(
                f"External relation {name!r} candidate keys drift from catalog.toml"
            )


def _external_stub_shapes(
    logical: dict[str, Any],
) -> tuple[
    tuple[
        str,
        str,
        tuple[tuple[str, str, str], ...],
        tuple[str, ...],
        tuple[tuple[str, ...], ...],
        frozenset[str],
    ],
    ...,
]:
    """Derive every external stub from the frozen catalog physical contract."""

    with CATALOG_PHYSICAL.open("rb") as stream:
        physical: dict[str, Any] = tomllib.load(stream)
    physical_by_name = {
        str(relation["name"]): relation for relation in physical["relation"]
    }
    inline_projections = {
        str(value) for value in physical.get("inline_projections", [])
    }
    result = []
    for external in logical.get("external_relation", []):
        relation_name = str(external["name"])
        if relation_name in inline_projections:
            continue
        physical_relation = physical_by_name.get(relation_name)
        if physical_relation is None:
            raise ValueError(
                f"External relation {relation_name!r} is absent from physical.toml"
            )
        column_by_attribute = {
            str(column["attribute"]): column
            for column in physical_relation.get("column", [])
        }
        attributes = tuple(str(value) for value in external["attributes"])
        columns: list[tuple[str, str, str]] = []
        nullable: set[str] = set()
        for attribute in attributes:
            column = column_by_attribute.get(attribute)
            if column is None:
                raise ValueError(
                    f"External relation {relation_name!r}.{attribute} lacks a physical column"
                )
            sqlite = column["sqlite"]
            mariadb = column["mariadb"]
            columns.append((attribute, str(sqlite["type"]), str(mariadb["type"])))
            if bool(sqlite["nullable"]) != bool(mariadb["nullable"]):
                raise ValueError(
                    f"External relation {relation_name!r}.{attribute} nullability drifts"
                )
            if bool(sqlite["nullable"]):
                nullable.add(attribute)
        declared_keys = tuple(
            tuple(str(attribute) for attribute in key)
            for key in external["declared_keys"]
        )
        physical_primary = tuple(
            str(value) for value in physical_relation["primary_key"]
        )
        primary = (
            physical_primary if physical_primary in declared_keys else declared_keys[0]
        )
        unique = tuple(key for key in declared_keys if key != primary)
        result.append(
            (
                relation_name,
                str(physical_relation["table"]),
                tuple(columns),
                primary,
                unique,
                frozenset(nullable),
            )
        )
    return tuple(result)


def _string(value: dict[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{context}.{key} must be a non-empty string")
    return result


def _string_array(value: dict[str, Any], key: str, context: str) -> tuple[str, ...]:
    result = value.get(key)
    if (
        not isinstance(result, list)
        or not result
        or not all(isinstance(item, str) and item for item in result)
    ):
        raise ValueError(f"{context}.{key} must be a non-empty string array")
    return tuple(result)


def _semantic_obligations(
    logical: dict[str, Any], relation_names: set[str]
) -> tuple[dict[str, Any], ...]:
    raw_values = logical.get("semantic_obligation")
    if not isinstance(raw_values, list) or not all(
        isinstance(value, dict) for value in raw_values
    ):
        raise ValueError("semantic_obligation must be an array of tables")
    obligations = tuple(raw_values)
    by_id: dict[str, dict[str, Any]] = {}
    external_names = {
        _string(value, "name", "external_relation")
        for value in logical.get("external_relation", [])
    }
    for value in obligations:
        obligation_id = _string(value, "id", "semantic_obligation")
        if obligation_id in by_id:
            raise ValueError(f"duplicate semantic obligation ID {obligation_id!r}")
        by_id[obligation_id] = value
        version = value.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version != 1:
            raise ValueError(
                f"semantic obligation {obligation_id!r}.version must be exactly 1"
            )
        _string(value, "scope", f"semantic obligation {obligation_id!r}")
        obligation_class = _string(
            value, "class", f"semantic obligation {obligation_id!r}"
        )
        lifecycle = _string(
            value, "lifecycle", f"semantic obligation {obligation_id!r}"
        )
        check = _string(value, "check", f"semantic obligation {obligation_id!r}")
        hook = _string(value, "hook", f"semantic obligation {obligation_id!r}")
        description = _string(
            value, "description", f"semantic obligation {obligation_id!r}"
        )
        relations = _string_array(
            value, "relations", f"semantic obligation {obligation_id!r}"
        )
        unknown_relations = set(relations) - relation_names - external_names
        if unknown_relations:
            raise ValueError(
                f"semantic obligation {obligation_id!r} references unknown relations "
                f"{sorted(unknown_relations)!r}"
            )
        generation_binding = GENERATION_OBLIGATION_BINDINGS.get(obligation_id)
        if generation_binding is not None and generation_binding != (
            relations,
            description,
        ):
            raise ValueError(
                f"semantic obligation {obligation_id!r} generation relation or "
                "description binding drifts"
            )
        expected = SEMANTIC_OBLIGATION_CHECKS.get(obligation_id)
        if expected != (lifecycle, obligation_class, check):
            raise ValueError(
                f"semantic obligation {obligation_id!r} has an unregistered "
                "lifecycle/class/check binding"
            )
        if hook != SEMANTIC_VALIDATOR_HOOK:
            raise ValueError(
                f"semantic obligation {obligation_id!r} has the wrong runtime hook"
            )
        if value.get("ready_check") != check or value.get("writer_hook_version") != 1:
            raise ValueError(
                f"semantic obligation {obligation_id!r} executable version drifts"
            )
        _string(value, "writer_hook", f"semantic obligation {obligation_id!r}")
    if set(by_id) != set(SEMANTIC_OBLIGATION_CHECKS):
        raise ValueError(
            "semantic obligation registry is incomplete or contains extras"
        )
    return obligations


def _bootstrap_seeds(
    logical: dict[str, Any], relations: list[dict[str, Any]]
) -> tuple[dict[str, Any], ...]:
    relation_by_name = {str(value["name"]): value for value in relations}
    contract = logical.get("bootstrap_contract")
    if not isinstance(contract, dict):
        raise ValueError("bootstrap_contract must be a table")
    if contract.get("version") != 1:
        raise ValueError("bootstrap_contract.version must be exactly 1")
    if (
        contract.get("seed_validation_lifecycle") != "building_only"
        or contract.get("absence_validation_lifecycle") != "building_only"
    ):
        raise ValueError("bootstrap validation must be BUILDING-only")
    if contract.get("epoch_owned_relation") != "schema_epoch_control":
        raise ValueError("bootstrap epoch ownership must remain schema_epoch_control")
    seeded = _string_array(contract, "seeded_relations", "bootstrap_contract")
    absent = _string_array(contract, "absent_relations", "bootstrap_contract")
    _string(contract, "absence_rule", "bootstrap_contract")
    _string(contract, "epoch_rule", "bootstrap_contract")
    if len(seeded) != len(set(seeded)) or len(absent) != len(set(absent)):
        raise ValueError("bootstrap relation partitions contain duplicates")
    if set(seeded) & set(absent):
        raise ValueError("bootstrap seeded and absent relation sets overlap")
    if set(seeded) | set(absent) | {"schema_epoch_control"} != set(relation_by_name):
        raise ValueError("bootstrap contract does not partition all local relations")
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

    raw_seeds = logical.get("bootstrap_seed")
    if not isinstance(raw_seeds, list) or not all(
        isinstance(value, dict) for value in raw_seeds
    ):
        raise ValueError("bootstrap_seed must be an array of tables")
    seeds = tuple(raw_seeds)
    expected_allocator_rows = {
        "h2hdb.operational.revision-allocator.source.v1": "SOURCE",
        "h2hdb.operational.revision-allocator.catalog.v1": "CATALOG",
    }
    expected_identity_allocator_rows = {
        "h2hdb.operational.identity-allocator.gallery.v1": "GALLERY",
        "h2hdb.operational.identity-allocator.tag.v1": "TAG",
        "h2hdb.operational.identity-allocator.policy.v1": "POLICY",
    }
    expected_deletion_generation_rows = {
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
    expected_request_budget_rows = {
        "h2hdb.operational.gallery-staging-request-budget.genesis.v1": (
            "gallery_observation_staging_request_budget",
            (1, 0),
            ("uint64", "uint64"),
        ),
    }
    targets = logical.get("cleanup_target")
    if not isinstance(targets, list) or not all(
        isinstance(value, dict) for value in targets
    ):
        raise ValueError("cleanup_target must be an array of tables")
    expected_target_rows = {
        f"h2hdb.operational.cleanup-target.{str(value['target_kind']).lower().replace('_', '-')}.v1": (
            str(value["target_kind"]),
        )
        for value in targets
    }
    expected_phase_rows = {
        f"h2hdb.operational.cleanup-phase.{str(phase['phase']).lower().replace('_', '-')}.v1": (
            str(phase["phase"]),
            str(target["target_kind"]),
            int(phase["order"]),
        )
        for target in targets
        for phase in target["phases"]
    }
    seen_ids: set[str] = set()
    seen_keys: set[str] = set()
    for seed in seeds:
        seed_id = _string(seed, "id", "bootstrap_seed")
        if seed_id in seen_ids:
            raise ValueError(f"duplicate bootstrap seed ID {seed_id!r}")
        seen_ids.add(seed_id)
        if seed.get("version") != 1:
            raise ValueError(f"bootstrap seed {seed_id!r}.version must be exactly 1")
        if seed.get("lifecycle") != "building_only":
            raise ValueError(f"bootstrap seed {seed_id!r} must be BUILDING-only")
        relation_name = _string(seed, "relation", f"bootstrap seed {seed_id!r}")
        if relation_name not in seeded:
            raise ValueError(f"bootstrap seed {seed_id!r} targets an absent relation")
        _string(seed, "invariant", f"bootstrap seed {seed_id!r}")
        raw_cells = seed.get("value")
        if not isinstance(raw_cells, list) or not all(
            isinstance(cell, dict) for cell in raw_cells
        ):
            raise ValueError(f"bootstrap seed {seed_id!r}.value must be typed cells")
        attributes = tuple(
            _string(cell, "attribute", f"bootstrap seed {seed_id!r} cell")
            for cell in raw_cells
        )
        if attributes != tuple(relation_by_name[relation_name]["attributes"]):
            raise ValueError(
                f"bootstrap seed {seed_id!r} must cover relation attributes in order"
            )
        if tuple(seed.get("columns", [])) != attributes:
            raise ValueError(f"bootstrap seed {seed_id!r} portable columns drift")
        expected_values: tuple[object, ...] | None
        expected_types: tuple[str, ...]
        if relation_name == "revision_allocator":
            expected_values = (expected_allocator_rows.get(seed_id), 1, 0)
            expected_types = ("ascii_enum", "uint64", "unix_microseconds")
        elif relation_name == "identity_allocator":
            expected_values = (
                expected_identity_allocator_rows.get(seed_id),
                1,
                0,
            )
            expected_types = (
                "ascii_enum",
                "uint64",
                "unix_microseconds",
            )
        elif relation_name in {
            "deletion_request_generation",
            "deletion_request_generation_head",
            "gallery_observation_staging_request_budget",
        }:
            singleton_row = (
                expected_deletion_generation_rows | expected_request_budget_rows
            ).get(seed_id)
            if singleton_row is None or singleton_row[0] != relation_name:
                expected_values = None
                expected_types = ()
            else:
                expected_values = singleton_row[1]
                expected_types = singleton_row[2]
        elif relation_name == "cleanup_target_kind":
            expected_values = expected_target_rows.get(seed_id)
            expected_types = ("ascii_enum",)
        else:
            expected_values = expected_phase_rows.get(seed_id)
            expected_types = ("ascii_enum", "ascii_enum", "uint64")
        actual_values = tuple(
            cell.get("text", cell.get("integer")) for cell in raw_cells
        )
        actual_types = tuple(cell.get("type") for cell in raw_cells)
        if (
            expected_values is None
            or actual_values != expected_values
            or actual_types != expected_types
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} has wrong typed values")
        if tuple(seed.get("values", [])) != tuple(
            str(value) for value in actual_values
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} portable values drift")
        key_value = repr(
            (
                relation_name,
                actual_values[:2]
                if relation_name == "cleanup_phase"
                else actual_values[:1],
            )
        )
        if key_value in seen_keys:
            raise ValueError("bootstrap registry keys are not unique")
        seen_keys.add(key_value)
    expected_ids = (
        set(expected_allocator_rows)
        | set(expected_identity_allocator_rows)
        | set(expected_deletion_generation_rows)
        | set(expected_request_budget_rows)
        | set(expected_target_rows)
        | set(expected_phase_rows)
    )
    if seen_ids != expected_ids:
        raise ValueError("bootstrap seed set is incomplete")
    return seeds


def _render_semantic_obligation(value: dict[str, Any]) -> list[str]:
    return [
        "[[semantic_obligation]]",
        f"id = {_quote(str(value['id']))}",
        f"version = {value['version']}",
        f"ready_check = {_quote(str(value['ready_check']))}",
        f"writer_hook = {_quote(str(value['writer_hook']))}",
        f"writer_hook_version = {value['writer_hook_version']}",
        f"scope = {_quote(str(value['scope']))}",
        f"class = {_quote(str(value['class']))}",
        f"lifecycle = {_quote(str(value['lifecycle']))}",
        f"check = {_quote(str(value['check']))}",
        f"hook = {_quote(str(value['hook']))}",
        f"relations = {_array([str(item) for item in value['relations']])}",
        f"description = {_quote(str(value['description']))}",
        "",
    ]


def _render_bootstrap_seed(value: dict[str, Any]) -> list[str]:
    rendered_cells: list[str] = []
    for cell in value["value"]:
        fields = [
            f"attribute = {_quote(str(cell['attribute']))}",
            f"type = {_quote(str(cell['type']))}",
        ]
        if "text" in cell:
            fields.append(f"text = {_quote(str(cell['text']))}")
        else:
            fields.append(f"integer = {int(cell['integer'])}")
        rendered_cells.append("{ " + ", ".join(fields) + " }")
    return [
        "[[bootstrap_seed]]",
        f"id = {_quote(str(value['id']))}",
        f"columns = {_array([str(item) for item in value['columns']])}",
        f"values = {_array([str(item) for item in value['values']])}",
        f"version = {value['version']}",
        f"lifecycle = {_quote(str(value['lifecycle']))}",
        f"relation = {_quote(str(value['relation']))}",
        "value = [" + ", ".join(rendered_cells) + "]",
        f"invariant = {_quote(str(value['invariant']))}",
        "",
    ]


def _render_bootstrap_seed_range(value: dict[str, Any]) -> list[str]:
    return [
        "[[bootstrap_seed_range]]",
        f"id = {_quote(str(value['id']))}",
        f"version = {int(value['version'])}",
        f"lifecycle = {_quote(str(value['lifecycle']))}",
        f"relation = {_quote(str(value['relation']))}",
        f"target_kind = {_quote(str(value['target_kind']))}",
        f"shard_start = {int(value['shard_start'])}",
        f"shard_end = {int(value['shard_end'])}",
        f"key_codec = {_quote(str(value['key_codec']))}",
        f"target_kind_tag_hex = {_quote(str(value['target_kind_tag_hex']))}",
        f"invariant = {_quote(str(value['invariant']))}",
        "",
    ]


def _topological_relation_order(relations: list[dict[str, Any]]) -> list[str]:
    """Preserve logical order where possible while placing FK parents first."""

    preferred_order = [str(relation["name"]) for relation in relations]
    relation_by_name = {str(relation["name"]): relation for relation in relations}
    if len(relation_by_name) != len(relations):
        raise RuntimeError("Operational logical relations contain duplicate names")

    local_names = set(relation_by_name)
    dependencies = {
        name: {
            str(foreign_key["relation"])
            for foreign_key in relation_by_name[name].get("foreign_keys", [])
            if str(foreign_key["relation"]) in local_names
            and str(foreign_key["relation"]) != name
        }
        for name in preferred_order
    }
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
            raise RuntimeError(
                f"Operational physical FK dependency cycle: {unresolved!r}"
            )
        remaining.remove(ready)
        result.append(ready)
    return result


def render() -> str:
    with LOGICAL.open("rb") as stream:
        logical: dict[str, Any] = tomllib.load(stream)
    _validate_external_candidate_key_shapes(logical)
    logical_relations: list[dict[str, Any]] = logical["relation"]
    relations = [
        relation
        for relation in logical_relations
        if not (
            isinstance(relation.get("materialization"), dict)
            and relation["materialization"].get("storage") == "inline_projection"
        )
    ]
    inline_projections = [
        str(relation["name"])
        for relation in logical_relations
        if isinstance(relation.get("materialization"), dict)
        and relation["materialization"].get("storage") == "inline_projection"
    ]
    with CATALOG_PHYSICAL.open("rb") as stream:
        catalog_physical: dict[str, Any] = tomllib.load(stream)
    catalog_inline_projections = {
        str(value) for value in catalog_physical.get("inline_projections", [])
    }
    external_inline_projections = [
        str(relation["name"])
        for relation in logical.get("external_relation", [])
        if str(relation["name"]) in catalog_inline_projections
    ]
    complete_relations = _topological_relation_order(relations)
    relation_names = {str(value["name"]) for value in logical_relations}
    semantic_obligations = _semantic_obligations(logical, relation_names)
    bootstrap_seeds = _bootstrap_seeds(logical, relations)
    provider_relations = [
        name for name in complete_relations if name != "schema_epoch_control"
    ]
    lines = [
        "physical_contract_version = 1",
        'name = "h2hdb-vnext-operational-physical"',
        f"logical_contract = {_quote(logical['name'])}",
        'description = "Complete verification-only SQLite/MariaDB realization of the operational control plane; production migrations are intentionally unchanged."',
        "complete_relations = " + _array(complete_relations),
        "source_slice = " + _array(provider_relations),
        "inline_projections = " + _array(inline_projections),
        "external_inline_projections = " + _array(external_inline_projections),
        'epoch_owned_relations = ["schema_epoch_control"]',
        "",
        "[epoch_control]",
        'relation = "schema_epoch_control"',
        'table = "h2hdb_schema_epoch"',
        'ownership = "epoch_catalog"',
        "provider_slice = false",
        'rationale = "Closed-world physical refinement includes the epoch relation, but generated CREATE-only providers must delegate its creation and exact validation to SchemaEpochCatalog."',
        "",
    ]
    for obligation in semantic_obligations:
        lines.extend(_render_semantic_obligation(obligation))
    bootstrap_contract = logical["bootstrap_contract"]
    lines.extend(
        [
            "[bootstrap_contract]",
            f"version = {bootstrap_contract['version']}",
            "seed_validation_lifecycle = "
            + _quote(str(bootstrap_contract["seed_validation_lifecycle"])),
            "absence_validation_lifecycle = "
            + _quote(str(bootstrap_contract["absence_validation_lifecycle"])),
            "epoch_owned_relation = "
            + _quote(str(bootstrap_contract["epoch_owned_relation"])),
            "seeded_relations = "
            + _array([str(value) for value in bootstrap_contract["seeded_relations"]]),
            "absent_relations = "
            + _array([str(value) for value in bootstrap_contract["absent_relations"]]),
            "absence_rule = " + _quote(str(bootstrap_contract["absence_rule"])),
            "epoch_rule = " + _quote(str(bootstrap_contract["epoch_rule"])),
            "",
        ]
    )
    for seed in bootstrap_seeds:
        lines.extend(_render_bootstrap_seed(seed))
    for seed_range in logical.get("bootstrap_seed_range", []):
        lines.extend(_render_bootstrap_seed_range(seed_range))
    for (
        external_name,
        table,
        columns,
        external_primary,
        external_unique,
        nullable_columns,
    ) in _external_stub_shapes(logical):
        lines.extend(
            [
                "[[external_stub]]",
                f"relation = {_quote(external_name)}",
                f"table = {_quote(table)}",
                f"primary_key = {_array(external_primary)}",
                "unique_keys = "
                + f"[{', '.join(_array(key) for key in external_unique)}]",
            ]
        )
        for name, sqlite_type, mariadb_type in columns:
            nullable = name in nullable_columns
            lines.append(
                f"[[external_stub.column]]\nname = {_quote(name)}\n"
                f"sqlite_type = {_quote(sqlite_type)}\n"
                f"mariadb_type = {_quote(mariadb_type)}\n"
                f"nullable = {str(nullable).lower()}"
            )
        lines.append("")

    for raw_relation in relations:
        name = raw_relation["name"]
        table = TABLE_OVERRIDES.get(name, "operational_" + name + "s")
        keys = [tuple(key) for key in raw_relation["declared_keys"]]
        primary = keys[0]
        relation_unique = keys[1:]
        materialization = raw_relation.get("materialization")
        view_materialization = (
            materialization
            if (
                isinstance(materialization, dict)
                and materialization.get("storage") == "logical_view"
            )
            else None
        )
        is_logical_view = view_materialization is not None
        lines.extend(
            [
                "[[relation]]",
                f"name = {_quote(name)}",
                'status = "implemented"',
                f"rationale = {_quote(str(view_materialization.get('rationale')) if view_materialization is not None else 'BCNF operational authority or exact durable protocol state for ' + name + '.')}",
                f"table = {_quote(table)}",
            ]
        )
        if is_logical_view:
            assert view_materialization is not None
            pattern = view_materialization.get("view_pattern")
            derived_from = view_materialization.get("derived_from")
            if not isinstance(pattern, str) or not pattern:
                raise ValueError(
                    f"Operational logical view {name!r} lacks view_pattern"
                )
            if (
                not isinstance(derived_from, list)
                or not derived_from
                or not all(isinstance(value, str) and value for value in derived_from)
            ):
                raise ValueError(
                    f"Operational logical view {name!r} lacks derived_from sources"
                )
            lines.extend(
                [
                    'kind = "view"',
                    "view = { pattern = "
                    + _quote(pattern)
                    + ", source_relations = "
                    + _array(derived_from)
                    + " }",
                ]
            )
        lines.extend(
            [
                f"primary_key = {_array(primary)}",
                f"unique_keys = [{', '.join(_array(key) for key in relation_unique)}]",
            ]
        )
        for attribute in raw_relation["attributes"]:
            column, nullable, sqlite_type, mariadb_type = _column(name, attribute)
            sqlite_collation = "BINARY" if sqlite_type == "TEXT" else "NONE"
            mariadb_collation = (
                "ascii_bin"
                if mariadb_type.startswith(("CHAR", "VARCHAR"))
                else "utf8mb4_nopad_bin"
                if mariadb_type == "LONGTEXT"
                else "NONE"
            )
            lines.append(
                "[[relation.column]]\n"
                f"attribute = {_quote(attribute)}\nname = {_quote(column)}\n"
                f"sqlite = {{ type = {_quote(sqlite_type)}, nullable = {str(nullable).lower()}, collation = {_quote(sqlite_collation)} }}\n"
                f"mariadb = {{ type = {_quote(mariadb_type)}, nullable = {str(nullable).lower()}, collation = {_quote(mariadb_collation)} }}"
            )
        for position, fk in enumerate(raw_relation.get("foreign_keys", []), 1):
            lines.append(
                "[[relation.foreign_key]]\n"
                f"name = {_quote(_identifier('fk_' + name + '_' + str(position)))}\n"
                f"attributes = {_array(fk['attributes'])}\n"
                f"referenced_relation = {_quote(fk['relation'])}\n"
                f"referenced_attributes = {_array(fk['referenced_attributes'])}"
            )
        # Physical specification can point to external logical relation names;
        # the independent loader maps them to the declared stub tables.
        indexes = [] if is_logical_view else _indexes(name, raw_relation)
        index_names = [index_name for index_name, _attributes in indexes]
        if len(index_names) != len(set(index_names)):
            raise ValueError(f"Operational relation {name!r} has duplicate index names")
        for index_name, attributes in indexes:
            lines.append(
                "[[relation.required_index]]\n"
                f"name = {_quote(index_name)}\nattributes = {_array(attributes)}\nunique = false"
            )
        checks = [] if is_logical_view else _checks(name, raw_relation)
        check_names = [check_name for check_name, _sqlite, _mariadb in checks]
        if len(check_names) != len(set(check_names)):
            raise ValueError(f"Operational relation {name!r} has duplicate check names")
        for check_name, sqlite_expression, mariadb_expression in checks:
            lines.append(
                "[[relation.check]]\n"
                f"name = {_quote(check_name)}\n"
                f"sqlite_expression = {_quote(sqlite_expression)}\n"
                f"mariadb_expression = {_quote(mariadb_expression)}"
            )
        lines.append("")
    return "\n".join(lines)


def _indexes(name: str, relation: dict[str, Any]) -> list[tuple[str, tuple[str, ...]]]:
    attributes = set(relation["attributes"])
    indexes: list[tuple[str, tuple[str, ...]]] = []
    for raw_index in relation.get("required_indexes", []):
        if not isinstance(raw_index, dict):
            raise ValueError(f"{name} required index must be a table")
        raw_name = raw_index.get("name")
        raw_attributes = raw_index.get("attributes")
        if (
            not isinstance(raw_name, str)
            or not raw_name
            or not isinstance(raw_attributes, list)
            or not raw_attributes
            or not all(isinstance(value, str) for value in raw_attributes)
        ):
            raise ValueError(f"{name} required index is malformed")
        index_attributes = tuple(raw_attributes)
        if not set(index_attributes) <= attributes:
            raise ValueError(f"{name} required index names an absent attribute")
        indexes.append((_identifier(raw_name), index_attributes))
    if name == "maintenance_gate_holder":
        indexes.append(("ix_maintenance_gate_holder_owner", ("owner_token",)))
    if {"state", "updated_at"} <= attributes:
        indexes.append((_identifier(f"ix_{name}_state"), ("state", "updated_at")))
    if {"source_revision", "sequence_no"} <= attributes:
        indexes.append(
            (_identifier(f"ix_{name}_revision"), ("source_revision", "sequence_no"))
        )
    if {"file_sha256", "cached_at"} <= attributes:
        indexes.append((_identifier(f"ix_{name}_hash"), ("file_sha256", "cached_at")))
    access_paths = [
        *(tuple(key) for key in relation["declared_keys"]),
        *(key for _index_name, key in indexes),
    ]
    for position, foreign_key in enumerate(relation.get("foreign_keys", []), 1):
        foreign_key_attributes = tuple(foreign_key["attributes"])
        if any(
            path[: len(foreign_key_attributes)] == foreign_key_attributes
            for path in access_paths
        ):
            continue
        index = (
            _identifier(f"ix_{name}_fk_{position}"),
            foreign_key_attributes,
        )
        indexes.append(index)
        access_paths.append(foreign_key_attributes)
    return indexes


def _checks(name: str, relation: dict[str, Any]) -> list[tuple[str, str, str]]:
    attributes = set(relation["attributes"])
    checks: list[tuple[str, str, str]] = []

    # The physical type declaration is sufficient on MariaDB, but SQLite's
    # affinity model otherwise permits every storage class in ordinary
    # tables.  Generate a closed per-relation predicate for every column so
    # adding a logical attribute cannot silently omit its SQLite domain.
    sqlite_storage: list[str] = []
    mariadb_storage: list[str] = []
    for attribute in sorted(attributes):
        column_name, nullable, sqlite_type, mariadb_type = _column(name, attribute)
        storage_class = sqlite_type.lower()
        sqlite_predicate = f"typeof({column_name}) = '{storage_class}'"
        mariadb_name = f"`{column_name}`" if column_name == "cursor" else column_name
        if nullable:
            sqlite_predicate = f"({column_name} IS NULL OR {sqlite_predicate})"
            mariadb_predicate = (
                f"({mariadb_name} IS NULL OR {mariadb_name} IS NOT NULL)"
            )
        else:
            mariadb_predicate = f"{mariadb_name} IS NOT NULL"
        sqlite_storage.append(sqlite_predicate)
        mariadb_storage.append(mariadb_predicate)
        if "UNSIGNED" in mariadb_type.upper():
            sqlite_storage.append(f"{column_name} >= 0")
            mariadb_storage.append(f"{mariadb_name} >= 0")
    checks.append(
        (
            _identifier(f"ck_{name}_storage_domain"),
            " AND ".join(sqlite_storage),
            " AND ".join(mariadb_storage),
        )
    )
    if "singleton_id" in attributes:
        checks.append(
            (
                _identifier(f"ck_{name}_singleton"),
                "singleton_id = 1",
                "singleton_id = 1",
            )
        )
    for attribute in sorted(attributes & UUID16):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_len"),
                f"length({attribute}) = 16",
                f"octet_length({attribute}) = 16",
            )
        )
    for attribute in sorted(attributes & DIGEST32):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_len"),
                f"length({attribute}) = 32",
                f"octet_length({attribute}) = 32",
            )
        )
    for attribute in sorted(
        (attributes & TIMESTAMPS) - {"completed_at", "ready_at", "sealed_at"}
    ):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_nonneg"),
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
            )
        )
    nullable_timestamps = {
        attribute
        for attribute in attributes & {"completed_at", "ready_at", "sealed_at"}
        if _column(name, attribute)[1]
    }
    for attribute in sorted(nullable_timestamps):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_nonneg"),
                f"{attribute} IS NULL OR {attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{attribute} IS NULL OR {attribute} >= 0 AND {attribute} <= 9223372036854775807",
            )
        )
    for attribute in sorted(
        (attributes & {"completed_at", "ready_at", "sealed_at"}) - nullable_timestamps
    ):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_nonneg"),
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
            )
        )
    nonnegative_counters = attributes & COUNTERS
    if name == "revision_allocator":
        nonnegative_counters -= {"next_revision"}
    for attribute in sorted(nonnegative_counters):
        mariadb_attribute = f"`{attribute}`" if attribute == "cursor" else attribute
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_nonneg"),
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{mariadb_attribute} >= 0 AND "
                f"{mariadb_attribute} <= 9223372036854775807",
            )
        )
    for attribute in sorted(attributes & SMALL_COUNTERS):
        if attribute == "terminal":
            continue
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_portable"),
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
            )
        )
    covered_portable = TIMESTAMPS | COUNTERS | SMALL_COUNTERS
    for attribute in sorted(
        value
        for value in attributes - covered_portable
        if _column(name, value)[3] == "BIGINT UNSIGNED"
    ):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_portable"),
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
                f"{attribute} >= 0 AND {attribute} <= 9223372036854775807",
            )
        )
    for attribute in sorted(attributes & {"scan_observation_version", "page_count"}):
        checks.append(
            (
                _identifier(f"ck_{name}_{attribute}_u32"),
                f"{attribute} IS NULL OR {attribute} >= 0 AND {attribute} <= 4294967295",
                f"{attribute} IS NULL OR {attribute} >= 0 AND {attribute} <= 4294967295",
            )
        )
    if name == "identity_allocator":
        checks.extend(
            [
                (
                    "ck_identity_allocator_next_portable",
                    "next_id >= 1 AND next_id <= 9223372036854775807",
                    "next_id >= 1 AND next_id <= 9223372036854775807",
                ),
                (
                    "ck_identity_allocator_stream",
                    "stream IN ('GALLERY', 'TAG', 'POLICY')",
                    "stream IN ('GALLERY', 'TAG', 'POLICY')",
                ),
            ]
        )
    if name == "gallery_observation_allocator":
        checks.extend(
            [
                (
                    "ck_gallery_observation_allocator_next_portable",
                    "next_observation_id >= 1 AND "
                    "next_observation_id <= 9223372036854775807",
                    "next_observation_id >= 1 AND "
                    "next_observation_id <= 9223372036854775807",
                ),
            ]
        )
    if name == "gallery_observation_staging":
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_identity_portable",
                    "gallery_id >= 1 AND gallery_id <= 9223372036854775807 "
                    "AND observation_id >= 1 AND "
                    "observation_id <= 9223372036854775807",
                    "gallery_id >= 1 AND gallery_id <= 9223372036854775807 "
                    "AND observation_id >= 1 AND "
                    "observation_id <= 9223372036854775807",
                ),
                (
                    "ck_gallery_observation_staging_state_time",
                    "state IN ('OPEN', 'SEALED', 'REUSED', 'ABANDONED', "
                    "'RETIRING_SEALED', 'RETIRING_REUSED') AND "
                    "(state IN ('OPEN', 'ABANDONED') AND sealed_at IS NULL "
                    "AND terminal_byte_count IS NULL OR "
                    "state IN ('SEALED', 'REUSED', 'RETIRING_SEALED', "
                    "'RETIRING_REUSED') AND sealed_at IS NOT NULL "
                    "AND terminal_byte_count IS NOT NULL "
                    "AND sealed_at >= created_at)",
                    "state IN ('OPEN', 'SEALED', 'REUSED', 'ABANDONED', "
                    "'RETIRING_SEALED', 'RETIRING_REUSED') AND "
                    "(state IN ('OPEN', 'ABANDONED') AND sealed_at IS NULL "
                    "AND terminal_byte_count IS NULL OR "
                    "state IN ('SEALED', 'REUSED', 'RETIRING_SEALED', "
                    "'RETIRING_REUSED') AND sealed_at IS NOT NULL "
                    "AND terminal_byte_count IS NOT NULL "
                    "AND sealed_at >= created_at)",
                ),
            ]
        )
    if name == "gallery_observation_staging_request_budget":
        checks.append(
            (
                "ck_gallery_observation_staging_request_budget_count",
                "retained_request_count >= 0 AND retained_request_count <= 1500000",
                "retained_request_count >= 0 AND retained_request_count <= 1500000",
            )
        )
    if name == "gallery_observation_staging_claim":
        checks.append(
            (
                "ck_gallery_observation_staging_claim_generation_portable",
                "ingest_generation <= 9223372036854775807 AND "
                "claim_generation <= 9223372036854775807",
                "ingest_generation <= 9223372036854775807 AND "
                "claim_generation <= 9223372036854775807",
            )
        )
    if name == "gallery_observation_staging_checkpoint":
        checks.extend(
            [
                (
                    _identifier(f"ck_{name}_component"),
                    "component IN (X'46494C45', X'544147', X'4449524543544F5259', X'4D45544144415441')",
                    "component IN (X'46494C45', X'544147', X'4449524543544F5259', X'4D45544144415441')",
                ),
                (
                    _identifier(f"ck_{name}_level"),
                    "level <= 8",
                    "level <= 8",
                ),
            ]
        )
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_checkpoint_state",
                    "state IN ('OPEN', 'COMPLETE')",
                    "state IN ('OPEN', 'COMPLETE')",
                ),
                (
                    "ck_gallery_observation_staging_checkpoint_regular_count",
                    "regular_count <= cursor AND "
                    "(component = X'4449524543544F5259' AND level = 0 OR "
                    "regular_count = 0)",
                    "regular_count <= `cursor` AND "
                    "(component = X'4449524543544F5259' AND level = 0 OR "
                    "regular_count = 0)",
                ),
                (
                    "ck_gallery_observation_staging_checkpoint_byte_count",
                    "(component = X'46494C45' AND level = 0 OR "
                    "processed_byte_count = 0)",
                    "(component = X'46494C45' AND level = 0 OR "
                    "processed_byte_count = 0)",
                ),
            ]
        )
    if name == "gallery_observation_staging_receipt":
        checks.append(
            (
                "ck_gallery_observation_staging_receipt_byte_count",
                "(component = X'46494C45' AND level = 0 AND "
                "next_processed_byte_count >= start_processed_byte_count OR "
                "(component != X'46494C45' OR level != 0) AND "
                "start_processed_byte_count = 0 AND "
                "next_processed_byte_count = 0)",
                "(component = X'46494C45' AND level = 0 AND "
                "next_processed_byte_count >= start_processed_byte_count OR "
                "(component <> X'46494C45' OR level <> 0) AND "
                "start_processed_byte_count = 0 AND "
                "next_processed_byte_count = 0)",
            )
        )
    if name == "gallery_observation_staging_frontier":
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_frontier_coordinate",
                    "position <= 254",
                    "position <= 254",
                ),
            ]
        )
    if name == "gallery_observation_staging_request_chunk":
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_request_chunk_bytes_bounded",
                    "length(request_bytes) >= 1 AND length(request_bytes) <= 32768",
                    "octet_length(request_bytes) >= 1 AND octet_length(request_bytes) <= 32768",
                ),
                (
                    "ck_gallery_observation_staging_request_chunk_position",
                    "position <= 2",
                    "position <= 2",
                ),
            ]
        )
    if name == "gallery_observation_staging_page_request":
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_page_request_coordinate",
                    "level <= 8 AND start_cursor <= 9223372036854775807 AND "
                    "component IN (X'46494C45', X'544147', X'4449524543544F5259', X'4D45544144415441')",
                    "level <= 8 AND start_cursor <= 9223372036854775807 AND "
                    "component IN (X'46494C45', X'544147', X'4449524543544F5259', X'4D45544144415441')",
                ),
                (
                    "ck_gallery_observation_staging_page_request_terminal",
                    "terminal IN (0, 1)",
                    "terminal IN (0, 1)",
                ),
            ]
        )
    if name == "gallery_observation_staging_match_checkpoint":
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_match_checkpoint_state",
                    "state IN ('OPEN', 'COMPLETE')",
                    "state IN ('OPEN', 'COMPLETE')",
                ),
                (
                    "ck_gallery_observation_staging_match_checkpoint_cursor_bounded",
                    "length(file_cursor_bytes) <= 2048",
                    "octet_length(file_cursor_bytes) <= 2048",
                ),
                (
                    "ck_gallery_observation_staging_match_checkpoint_portable",
                    "matched_count <= 9223372036854775807",
                    "matched_count <= 9223372036854775807",
                ),
            ]
        )
    if name == "gallery_observation_staging_match_request":
        checks.append(
            (
                "ck_gallery_observation_staging_match_request_terminal",
                "terminal IN (0, 1)",
                "terminal IN (0, 1)",
            )
        )
    if name == "gallery_observation_staging_metadata_parser":
        metadata_phases = (
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
        )
        phase_expression = (
            "phase IN (" + ", ".join(f"'{value}'" for value in metadata_phases) + ")"
        )
        checks.extend(
            [
                (
                    "ck_gallery_observation_staging_metadata_parser_phase",
                    phase_expression,
                    phase_expression,
                ),
                (
                    "ck_gallery_observation_staging_metadata_parser_carry_bounded",
                    "length(fixed_carry) <= 40 AND length(utf8_tail) <= 3",
                    "octet_length(fixed_carry) <= 40 AND octet_length(utf8_tail) <= 3",
                ),
            ]
        )
    if name in {"cleanup_target_kind", "cleanup_phase"}:
        with LOGICAL.open("rb") as stream:
            logical = tomllib.load(stream)
        values = (
            [str(target["target_kind"]) for target in logical["cleanup_target"]]
            if name == "cleanup_target_kind"
            else [
                str(phase["phase"])
                for target in logical["cleanup_target"]
                for phase in target["phases"]
            ]
        )
        attribute = "target_kind" if name == "cleanup_target_kind" else "phase"
        quoted = ", ".join("'" + value.replace("'", "''") + "'" for value in values)
        expression = f"{attribute} IN ({quoted})"
        checks.append(
            (_identifier(f"ck_{name}_{attribute}_registry"), expression, expression)
        )
    if name == "cleanup_sweep_target":
        checks.append(
            ("ck_cleanup_sweep_target_shard_bound", "shard_no < 256", "shard_no < 256")
        )
    if "cycle_generation" in attributes:
        checks.append(
            (
                _identifier(f"ck_{name}_cycle_generation_portable"),
                "cycle_generation >= 1 AND cycle_generation <= 9223372036854775807",
                "cycle_generation >= 1 AND cycle_generation <= 9223372036854775807",
            )
        )
    if name == "cleanup_phase":
        checks.append(
            ("ck_cleanup_phase_order_positive", "phase_order > 0", "phase_order > 0")
        )
    if name == "schema_epoch_control":
        expression = (
            "state = 'BUILDING' AND ready_at IS NULL OR "
            "state = 'READY' AND ready_at IS NOT NULL AND ready_at >= started_at"
        )
        checks.append(("ck_schema_epoch_state", expression, expression))
        checks.append(
            (
                "ck_schema_epoch_manifest_blob",
                "typeof(manifest_sha256) = 'blob'",
                "octet_length(manifest_sha256) = 32",
            )
        )
    if name == "ingest_coordination_head":
        checks.append(
            (
                "ck_ingest_generation_order",
                "completed_generation <= current_generation",
                "completed_generation <= current_generation",
            )
        )
        checks.append(
            (
                "ck_ingest_phase",
                "phase IN ('READY', 'INGESTING')",
                "phase IN ('READY', 'INGESTING')",
            )
        )
    if name == "download_generation":
        checks.append(
            (
                "ck_download_generation_time_order",
                "completed_at IS NULL OR completed_at >= started_at",
                "completed_at IS NULL OR completed_at >= started_at",
            )
        )
    if name == "download_coordination_head":
        checks.append(
            (
                "ck_download_generation_order",
                "completed_generation <= current_generation",
                "completed_generation <= current_generation",
            )
        )
    if name == "download_ingest_handoff":
        checks.append(
            (
                "ck_download_ingest_handoff_kind",
                "handoff_kind IN ('DOWNLOADER', 'EXPIRED_TAKEOVER')",
                "handoff_kind IN ('DOWNLOADER', 'EXPIRED_TAKEOVER')",
            )
        )
    if name == "maintenance_gate_generation":
        checks.append(
            (
                "ck_maintenance_gate_mode",
                "mode IN ('SHARED', 'EXCLUSIVE')",
                "mode IN ('SHARED', 'EXCLUSIVE')",
            )
        )
    if name == "maintenance_gate_holder":
        checks.append(
            (
                "ck_maintenance_gate_slot",
                "slot >= 0 AND slot < 64",
                "slot >= 0 AND slot < 64",
            )
        )
    if name in {"source_working_build", "catalog_working_candidate"}:
        checks.append((_identifier(f"ck_{name}_slot"), "slot = 1", "slot = 1"))
    if name == "revision_allocator":
        checks.append(
            (
                "ck_revision_allocator_next_positive",
                "next_revision >= 1 AND next_revision <= 9223372036854775807",
                "next_revision >= 1 AND next_revision <= 9223372036854775807",
            )
        )
        checks.append(
            (
                "ck_revision_allocator_stream",
                "stream IN ('SOURCE', 'CATALOG')",
                "stream IN ('SOURCE', 'CATALOG')",
            )
        )
    if name in {
        "operational_preparation_checkpoint",
        "cleanup_checkpoint",
        "source_build_discovery_checkpoint",
        "source_build_assembly_checkpoint",
    }:
        checks.append(
            (
                _identifier(f"ck_{name}_state"),
                "state IN ('OPEN', 'COMPLETE')",
                "state IN ('OPEN', 'COMPLETE')",
            )
        )
    if name == "source_build_discovery_batch_receipt":
        expression = (
            "committed_generation = start_generation + 1 AND "
            "next_processed_count = start_processed_count + row_count AND "
            "(terminal = 0 AND row_count > 0 AND next_state = 'OPEN' OR "
            "terminal = 1 AND row_count = 0 AND next_state = 'COMPLETE' AND "
            "next_cursor = start_cursor AND "
            "next_processed_count = start_processed_count)"
        )
        checks.append(
            (
                "ck_source_build_discovery_batch_receipt_transition",
                expression,
                expression,
            )
        )
    if name == "source_build_assembly_batch_receipt":
        expression = (
            "committed_generation = start_generation + 1 AND "
            "next_gallery_count = start_gallery_count + row_count AND "
            "next_file_count >= start_file_count AND "
            "next_byte_count >= start_byte_count AND "
            "(terminal = 0 AND row_count > 0 AND next_state = 'OPEN' OR "
            "terminal = 1 AND row_count = 0 AND next_state = 'COMPLETE' AND "
            "next_cursor = start_cursor AND "
            "next_gallery_count = start_gallery_count AND "
            "next_file_count = start_file_count AND "
            "next_byte_count = start_byte_count AND "
            "next_manifest_chain_sha256 = start_manifest_chain_sha256)"
        )
        checks.append(
            (
                "ck_source_build_assembly_batch_receipt_transition",
                expression,
                expression,
            )
        )
    if name == "operational_preparation":
        checks.append(
            (
                "ck_operational_preparation_state_time",
                "state IN ('OPEN', 'COMPLETE', 'FAILED', 'ABANDONED') AND "
                "(state = 'OPEN' AND completed_at IS NULL OR "
                "state IN ('COMPLETE', 'FAILED', 'ABANDONED') AND "
                "completed_at IS NOT NULL AND completed_at >= prepared_at)",
                "state IN ('OPEN', 'COMPLETE', 'FAILED', 'ABANDONED') AND "
                "(state = 'OPEN' AND completed_at IS NULL OR "
                "state IN ('COMPLETE', 'FAILED', 'ABANDONED') AND "
                "completed_at IS NOT NULL AND completed_at >= prepared_at)",
            )
        )
    if name == "cleanup_job":
        checks.extend(
            [
                (
                    "ck_cleanup_job_state",
                    "state IN ('OPEN', 'COMPLETE')",
                    "state IN ('OPEN', 'COMPLETE')",
                ),
                (
                    "ck_cleanup_job_progress_bounds",
                    "algorithm_version = 2 AND max_rows_per_transaction > 0 "
                    "AND max_rows_per_transaction <= 256 "
                    "AND frozen_root_count >= 0 "
                    "AND frozen_root_count <= max_rows_per_transaction",
                    "algorithm_version = 2 AND max_rows_per_transaction > 0 "
                    "AND max_rows_per_transaction <= 256 "
                    "AND frozen_root_count >= 0 "
                    "AND frozen_root_count <= max_rows_per_transaction",
                ),
                (
                    "ck_cleanup_job_state_completed_at",
                    "state = 'OPEN' AND completed_at IS NULL "
                    "AND final_chain_sha256 IS NULL AND final_deleted_count IS NULL OR "
                    "state = 'COMPLETE' AND completed_at IS NOT NULL "
                    "AND completed_at >= created_at "
                    "AND final_chain_sha256 IS NOT NULL "
                    "AND final_deleted_count IS NOT NULL",
                    "state = 'OPEN' AND completed_at IS NULL "
                    "AND final_chain_sha256 IS NULL AND final_deleted_count IS NULL OR "
                    "state = 'COMPLETE' AND completed_at IS NOT NULL "
                    "AND completed_at >= created_at "
                    "AND final_chain_sha256 IS NOT NULL "
                    "AND final_deleted_count IS NOT NULL",
                ),
            ]
        )
    if name == "cleanup_checkpoint":
        receipt_transition = (
            "generation = 1 AND receipt_batch_key IS NULL "
            "AND receipt_start_cursor IS NULL "
            "AND receipt_prior_chain_sha256 IS NULL "
            "AND receipt_prior_deleted_count IS NULL "
            "AND receipt_input_sha256 IS NULL AND receipt_row_count IS NULL OR "
            "generation > 1 AND receipt_batch_key IS NOT NULL "
            "AND receipt_start_cursor IS NOT NULL "
            "AND receipt_prior_chain_sha256 IS NOT NULL "
            "AND receipt_prior_deleted_count IS NOT NULL "
            "AND receipt_input_sha256 IS NOT NULL "
            "AND receipt_row_count IS NOT NULL "
            "AND receipt_row_count <= 256 "
            "AND deleted_count = receipt_prior_deleted_count + receipt_row_count "
            "AND (state = 'OPEN' AND receipt_row_count > 0 "
            "AND receipt_start_cursor <> cursor_bytes OR "
            "state = 'COMPLETE' AND receipt_row_count = 0 "
            "AND receipt_start_cursor = cursor_bytes)"
        )
        checks.append(
            (
                "ck_cleanup_checkpoint_receipt_transition",
                receipt_transition,
                receipt_transition,
            )
        )
    if name == "cleanup_cycle_root":
        checks.append(
            (
                "ck_cleanup_cycle_root_frame_bounds",
                "length(frozen_root_key) >= 3 AND length(frozen_root_key) <= 260",
                "octet_length(frozen_root_key) >= 3 "
                "AND octet_length(frozen_root_key) <= 260",
            )
        )
    if name == "operational_event":
        checks.append(
            (
                "ck_operational_event_type",
                "event_type IN ('REMOVED_GID', 'DELETION_CONSUMPTION')",
                "event_type IN ('REMOVED_GID', 'DELETION_CONSUMPTION')",
            )
        )
    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    expected = render()
    if arguments.check:
        actual = OUTPUT.read_text() if OUTPUT.exists() else ""
        if actual != expected:
            raise SystemExit(f"{OUTPUT.relative_to(ROOT)} is stale; regenerate it")
    else:
        OUTPUT.write_text(expected)


if __name__ == "__main__":
    main()
