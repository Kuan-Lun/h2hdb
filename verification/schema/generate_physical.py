from __future__ import annotations

import hashlib
import json
import tomllib
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = ROOT / "verification/schema/catalog.toml"
PHYSICAL_PATH = ROOT / "verification/schema/physical.toml"

OVERLAY_RUNTIME_OBLIGATION_PATHS = (
    "overlay.shadow_tombstone_exclusion",
    "overlay.ancestry_exactness",
    "overlay.full_evaluator_equality",
)


def q(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def runtime_obligation_records(
    catalog: dict[str, Any],
) -> tuple[tuple[str, str, str], ...]:
    """Bind every prose runtime premise to exactly one machine obligation."""

    relations = {value["name"]: value for value in catalog.get("relation", [])}

    def materialization_obligation(relation_name: str) -> str:
        materialization = relations[relation_name]["materialization"]
        return (
            f"{materialization['rationale']} Derived from exactly "
            f"{', '.join(materialization['derived_from'])}. "
            f"{materialization['refresh_strategy']}"
        )

    path_and_text = [
        (
            "canonical_digest_contract.write_obligation",
            catalog["canonical_digest_contract"]["write_obligation"],
        ),
        (
            "canonical_digest_contract.read_obligation",
            catalog["canonical_digest_contract"]["read_obligation"],
        ),
        (
            "canonical_value_page_contract.collision_obligation",
            catalog["canonical_value_page_contract"]["collision_obligation"],
        ),
        (
            "canonical_value_page_contract.seal_obligation",
            catalog["canonical_value_page_contract"]["seal_obligation"],
        ),
        (
            "canonical_value_page_contract.cleanup_rule",
            catalog["canonical_value_page_contract"]["cleanup_rule"],
        ),
        (
            "source_root_contract.write_obligation",
            catalog["source_root_contract"]["write_obligation"],
        ),
        (
            "source_locator_contract.write_obligation",
            catalog["source_locator_contract"]["write_obligation"],
        ),
        (
            "source_locator_contract.read_obligation",
            catalog["source_locator_contract"]["read_obligation"],
        ),
        (
            "source_scope_identity_contract.write_obligation",
            catalog["source_scope_identity_contract"]["write_obligation"],
        ),
        (
            "source_scope_identity_contract.seal_obligation",
            catalog["source_scope_identity_contract"]["seal_obligation"],
        ),
        (
            "file_identity_contract.write_obligation",
            catalog["file_identity_contract"]["write_obligation"],
        ),
        (
            "gallery_observation_page_contract.collision_obligation",
            catalog["gallery_observation_page_contract"]["collision_obligation"],
        ),
        (
            "gallery_observation_page_contract.materialization_rule",
            catalog["gallery_observation_page_contract"]["materialization_rule"],
        ),
        (
            "gallery_observation_page_contract.seal_obligation",
            catalog["gallery_observation_page_contract"]["seal_obligation"],
        ),
        (
            "gallery_observation_page_contract.cleanup_rule",
            catalog["gallery_observation_page_contract"]["cleanup_rule"],
        ),
        (
            "file_identity_contract.read_obligation",
            catalog["file_identity_contract"]["read_obligation"],
        ),
        (
            "gallery_observation_identity_contract.write_obligation",
            catalog["gallery_observation_identity_contract"]["write_obligation"],
        ),
        (
            "gallery_observation_identity_contract.reuse_obligation",
            catalog["gallery_observation_identity_contract"]["reuse_obligation"],
        ),
        (
            "effective_content_contract.write_obligation",
            catalog["effective_content_contract"]["write_obligation"],
        ),
        (
            "effective_content_contract.read_obligation",
            catalog["effective_content_contract"]["read_obligation"],
        ),
        (
            "source_snapshot_manifest_contract.write_obligation",
            catalog["source_snapshot_manifest_contract"]["write_obligation"],
        ),
        (
            "source_snapshot_manifest_contract.handoff_obligation",
            catalog["source_snapshot_manifest_contract"]["handoff_obligation"],
        ),
        (
            "source_snapshot_manifest_contract.publication_obligation",
            catalog["source_snapshot_manifest_contract"]["publication_obligation"],
        ),
        (
            "analysis_run_contract.write_obligation",
            catalog["analysis_run_contract"]["write_obligation"],
        ),
        (
            "analysis_run_contract.attempt_rule",
            catalog["analysis_run_contract"]["attempt_rule"],
        ),
        (
            "analysis_resolution_contract.batch_rule",
            catalog["analysis_resolution_contract"]["batch_rule"],
        ),
        (
            "analysis_resolution_contract.cursor_codec_rule",
            catalog["analysis_resolution_contract"]["cursor_codec_rule"],
        ),
        (
            "analysis_resolution_contract.published_baseline_prune_rule",
            catalog["analysis_resolution_contract"]["published_baseline_prune_rule"],
        ),
        (
            "analysis_candidate_contract.runtime_obligation",
            catalog["analysis_candidate_contract"]["runtime_obligation"],
        ),
        (
            "analysis_impacted_key_contract.append_rule",
            catalog["analysis_impacted_key_contract"]["append_rule"],
        ),
        (
            "analysis_impacted_key_contract.replay_rule",
            catalog["analysis_impacted_key_contract"]["replay_rule"],
        ),
        (
            "analysis_impacted_key_contract.cleanup_rule",
            catalog["analysis_impacted_key_contract"]["cleanup_rule"],
        ),
        (
            "artifact_delta_contract.rebuild_rule",
            catalog["artifact_delta_contract"]["rebuild_rule"],
        ),
        (
            "artifact_delta_contract.unchanged_rule",
            catalog["artifact_delta_contract"]["unchanged_rule"],
        ),
        (
            "artifact_delta_contract.rename_rule",
            catalog["artifact_delta_contract"]["rename_rule"],
        ),
        (
            "artifact_adapter_policy_contract.runtime_obligation",
            catalog["artifact_adapter_policy_contract"]["runtime_obligation"],
        ),
        (
            "artifact_member_plan_contract.runtime_obligation",
            catalog["artifact_member_plan_contract"]["runtime_obligation"],
        ),
        (
            "artifact_member_plan_contract.ready_obligation",
            catalog["artifact_member_plan_contract"]["ready_obligation"],
        ),
        (
            "artifact_storage_key_contract.runtime_obligation",
            catalog["artifact_storage_key_contract"]["runtime_obligation"],
        ),
        (
            "artifact_protection_token_contract.runtime_obligation",
            catalog["artifact_protection_token_contract"]["runtime_obligation"],
        ),
        (
            "publication_atomic_contract.selection_rule",
            catalog["publication_atomic_contract"]["selection_rule"],
        ),
        (
            "publication_atomic_contract.cursor_codec_rule",
            catalog["publication_atomic_contract"]["cursor_codec_rule"],
        ),
        (
            "publication_atomic_contract.batch_rule",
            catalog["publication_atomic_contract"]["batch_rule"],
        ),
        (
            "publication_atomic_contract.projection_seal_rule",
            catalog["publication_atomic_contract"]["projection_seal_rule"],
        ),
        (
            "publication_atomic_contract.runtime_obligation",
            catalog["publication_atomic_contract"]["runtime_obligation"],
        ),
        (
            "publication_atomic_contract.finalization_rule",
            catalog["publication_atomic_contract"]["finalization_rule"],
        ),
        (
            "publication_commit_contract.runtime_obligation",
            catalog["publication_commit_contract"]["runtime_obligation"],
        ),
        (
            "publication_commit_contract.ready_obligation",
            catalog["publication_commit_contract"]["ready_obligation"],
        ),
        (
            "search_policy.rationale",
            relations["search_policy"]["rationale"],
        ),
        *(
            (
                f"{relation_name}.materialization",
                materialization_obligation(relation_name),
            )
            for relation_name in (
                "search_lexeme",
                "search_document",
                "search_posting",
                "title_search_posting",
                "discovery_seal",
                "language_facet_order",
                "subject_facet_order",
                "contributor_facet_order",
            )
        ),
        *(
            (
                f"batch_receipt_projection.{value['name']}.write_obligation",
                value["write_obligation"],
            )
            for value in catalog.get("batch_receipt_projection", [])
        ),
        (
            "title_sort_contract.runtime_obligation",
            catalog["title_sort_contract"]["runtime_obligation"],
        ),
        (
            "transition_authority_contract.runtime_obligation",
            catalog["transition_authority_contract"]["runtime_obligation"],
        ),
        (
            "transition_authority_contract.ready_obligation",
            catalog["transition_authority_contract"]["ready_obligation"],
        ),
        *(
            (
                f"byte_domain.{value['attribute']}.runtime_obligation",
                value["runtime_obligation"],
            )
            for value in catalog["byte_domain"]
        ),
        *zip(
            OVERLAY_RUNTIME_OBLIGATION_PATHS, OVERLAY_RUNTIME_OBLIGATIONS, strict=True
        ),
    ]
    if len({path for path, _text in path_and_text}) != len(path_and_text):
        raise RuntimeError("Runtime-obligation source paths are not unique")
    if len({text for _path, text in path_and_text}) != len(path_and_text):
        raise RuntimeError("Runtime-obligation prose values are not unique")

    covered_by: dict[str, str] = {}
    for obligation in catalog.get("semantic_obligation", []):
        obligation_id = obligation["id"]
        for path in obligation["covers"]:
            if path.startswith("machine_contract."):
                continue
            previous = covered_by.setdefault(path, obligation_id)
            if previous != obligation_id:
                raise RuntimeError(
                    f"Runtime obligation {path!r} is multiply owned by "
                    f"{previous!r} and {obligation_id!r}"
                )
    source_paths = {path for path, _text in path_and_text}
    if source_paths != set(covered_by):
        raise RuntimeError(
            "Runtime-obligation sources and semantic covers differ: "
            f"missing={sorted(source_paths - set(covered_by))!r}, "
            f"extra={sorted(set(covered_by) - source_paths)!r}"
        )
    return tuple((path, text, covered_by[path]) for path, text in path_and_text)


def shape(
    sqlite_type: str,
    mariadb_type: str,
    *,
    sqlite_collation: str = "NONE",
    mariadb_collation: str = "NONE",
    nullable: bool = False,
) -> dict[str, Any]:
    return {
        "sqlite": {
            "type": sqlite_type,
            "nullable": nullable,
            "collation": sqlite_collation,
        },
        "mariadb": {
            "type": mariadb_type,
            "nullable": nullable,
            "collation": mariadb_collation,
        },
    }


UUID_BYTES = shape("BLOB", "BINARY(16)")
DIGEST_BYTES = shape("BLOB", "BINARY(32)")
U64 = shape("INTEGER", "BIGINT UNSIGNED")
U32 = shape("INTEGER", "INT UNSIGNED")
RAW_U64_BYTES = shape("BLOB", "BINARY(8)")
SIGNED_I64_BYTES = shape("BLOB", "BINARY(8)")
ASCII_32_TEXT = shape(
    "TEXT", "VARCHAR(32)", sqlite_collation="BINARY", mariadb_collation="ascii_bin"
)
ASCII_64_TEXT = shape(
    "TEXT", "VARCHAR(64)", sqlite_collation="BINARY", mariadb_collation="ascii_bin"
)
UNIX_MICROSECONDS = shape("INTEGER", "BIGINT UNSIGNED")

TIMESTAMP_ATTRIBUTES = {
    "allocated_at",
    "advanced_at",
    "committed_at",
    "completed_at",
    "computed_at",
    "created_at",
    "download_time",
    "finalized_at",
    "modified_at",
    "modified_time",
    "published_at",
    "sealed_at",
    "started_at",
    "updated_at",
    "upload_time",
}


NEW_ATTRIBUTE_SHAPES: dict[str, dict[str, Any]] = {
    "allocated_at": UNIX_MICROSECONDS,
    "advanced_at": UNIX_MICROSECONDS,
    "algorithm_version": U32,
    "artist_count": U64,
    "artist_tag_id": U64,
    "artifact_role": shape("BLOB", "VARBINARY(8)"),
    "base_source_revision": U64,
    "base_source_generation": U64,
    "base_catalog_generation": U64,
    "batch_key": shape("BLOB", "VARBINARY(512)"),
    "byte_count": U64,
    "change_kind": ASCII_32_TEXT,
    "changed_ns": SIGNED_I64_BYTES,
    "committed_at": UNIX_MICROSECONDS,
    "committed_generation": U64,
    "completed_at": UNIX_MICROSECONDS,
    "computed_at": UNIX_MICROSECONDS,
    "content_owner_rule_version": U32,
    "created_at": UNIX_MICROSECONDS,
    "device": RAW_U64_BYTES,
    "directory_entry_count": U64,
    "download_time": UNIX_MICROSECONDS,
    "file_count": U64,
    "file_no": U64,
    "file_order_version": U32,
    "gallery_count": U64,
    "gallery_id": U64,
    "generation": U64,
    "gid": U64,
    "gid_winner_rule_version": U32,
    "identity_policy_version": U32,
    "inode": RAW_U64_BYTES,
    "manifest_algorithm_version": U32,
    "manifest_policy_id": U64,
    "maximum_field_nfd_bytes": U32,
    "maximum_query_nfd_bytes": U32,
    "maximum_lexeme_bytes": U32,
    "maximum_query_lexemes": U32,
    "maximum_gallery_artist_count": U64,
    "modified_ns": SIGNED_I64_BYTES,
    "modified_time": UNIX_MICROSECONDS,
    "new_excluded": shape("INTEGER", "TINYINT UNSIGNED"),
    "observation_id": U64,
    "occurrence_count": U64,
    "old_excluded": shape("INTEGER", "TINYINT UNSIGNED"),
    "owner_gallery_id": U64,
    "page_count": U32,
    "page_index": U32,
    "page_limit": U64,
    "policy_id": U64,
    "position": U64,
    "prefer_not_already_uploaded": shape("INTEGER", "TINYINT UNSIGNED"),
    "published_at": UNIX_MICROSECONDS,
    "row_count": U64,
    "scan_observation_version": U32,
    "sealed_at": shape("INTEGER", "BIGINT UNSIGNED", nullable=True),
    "size_bytes": U64,
    "source_file_count": U64,
    "source_revision": U64,
    "spam_artist_threshold": U64,
    "spam_occurrence_threshold": U64,
    "started_at": UNIX_MICROSECONDS,
    "state": shape(
        "TEXT",
        "VARCHAR(16)",
        sqlite_collation="BINARY",
        mariadb_collation="ascii_bin",
    ),
    "tag_id": U64,
    "title_scalar_count": U64,
    "updated_at": UNIX_MICROSECONDS,
    "upload_time": UNIX_MICROSECONDS,
    "witness_gallery_id": U64,
    "winner_gallery_id": U64,
    "digest_domain": shape("BLOB", "VARBINARY(64)"),
    "page_bytes": shape("BLOB", "MEDIUMBLOB"),
    "component": shape("BLOB", "VARBINARY(9)"),
    "level": U32,
    "subtree_item_count": U64,
    "page_position": U64,
    "first_key": shape("BLOB", "VARBINARY(255)"),
    "last_key": shape("BLOB", "VARBINARY(255)"),
    "ancestor_analysis_id": UUID_BYTES,
    "ancestor_depth": U32,
    "anchor_analysis_id": UUID_BYTES,
    "artifact_algorithm_version": U32,
    "storage_generation": U64,
    "adapter_id": shape("BLOB", "VARBINARY(64)"),
    "artifact_input_count": U64,
    "artifact_count": U64,
    "artifact_name": shape("BLOB", "VARBINARY(255)"),
    "artifact_policy_id": U64,
    "artifact_sha256": DIGEST_BYTES,
    "artifacts_required": shape("INTEGER", "TINYINT UNSIGNED"),
    "base_revision": U64,
    "candidate_id": UUID_BYTES,
    "changed_galleries": U64,
    "create_count": U64,
    "cursor_codec": shape("BLOB", "VARBINARY(64)"),
    "code": shape("BLOB", "LONGBLOB"),
    "component_name": shape("BLOB", "LONGBLOB"),
    "component_value": shape("BLOB", "LONGBLOB"),
    "display_title_algorithm_version": U32,
    "display_title_policy_id": U64,
    "title_sort_policy_id": U32,
    "title_sort_algorithm_version": U32,
    "unicode_data_version": shape("BLOB", "VARBINARY(32)"),
    "duplicate_losers": U64,
    "delete_count": U64,
    "entry_order_version": U32,
    "finalized_at": shape("INTEGER", "BIGINT UNSIGNED", nullable=True),
    "metadata_format_version": U32,
    "modified_at": UNIX_MICROSECONDS,
    "name": shape("BLOB", "LONGBLOB"),
    "new_galleries": U64,
    "next_cursor": shape("BLOB", "VARBINARY(2048)"),
    "next_processed_count": U64,
    "next_state": ASCII_32_TEXT,
    "operation": ASCII_32_TEXT,
    "overlay_depth": U32,
    "protection_token": shape("BLOB", "BINARY(32)"),
    "prepared_artifact_count": U64,
    "processed_count": U64,
    "publication_count": U64,
    "publication_id": shape("BLOB", "VARBINARY(64)"),
    "publication_key": DIGEST_BYTES,
    "publication_sha256": DIGEST_BYTES,
    "receipt_id": UUID_BYTES,
    "base_receipt_id": UUID_BYTES,
    "preparation_id": UUID_BYTES,
    "operational_policy_id": U64,
    "successor_generation": U64,
    "predecessor_generation": U64,
    "removed_galleries": U64,
    "rebuild_count": U64,
    "reserved_revision": U64,
    "resolved_analysis_id": UUID_BYTES,
    "revision": U64,
    "scheme": shape("BLOB", "LONGBLOB"),
    "sort_title": shape("BLOB", "LONGBLOB"),
    "source_gallery_name": shape("BLOB", "VARBINARY(255)"),
    "source_title": shape("BLOB", "LONGBLOB"),
    "timestamp_policy_version": U32,
    "gallery_name": shape("BLOB", "VARBINARY(255)"),
    "name_bytes": shape("BLOB", "VARBINARY(255)"),
    "namespace": shape("BLOB", "VARBINARY(128)"),
    "source_provider": shape("BLOB", "VARBINARY(64)"),
    "start_cursor": shape("BLOB", "VARBINARY(2048)"),
    "start_generation": U64,
    "start_processed_count": U64,
    "channel": shape("BLOB", "VARBINARY(64)"),
    "stage": shape("BLOB", "VARBINARY(64)"),
    "stage_order": shape("BLOB", "BINARY(2)"),
    "state_component": shape("BLOB", "VARBINARY(64)"),
    # MariaDB exposes CAST(... AS UNSIGNED) from a view as BIGINT UNSIGNED;
    # unlike stored columns it has no CAST target for TINYINT UNSIGNED.
    "terminal": shape("INTEGER", "BIGINT UNSIGNED"),
    "unchanged_count": U64,
    "file_role": shape("BLOB", "VARBINARY(8)"),
    "excluded_flag": shape("INTEGER", "TINYINT UNSIGNED"),
    "role": shape("BLOB", "VARBINARY(64)"),
    "metadata_fingerprint": shape("BLOB", "BINARY(40)"),
    "cursor": shape("BLOB", "VARBINARY(2048)"),
    "key_codec": shape("BLOB", "VARBINARY(64)"),
    "key_segment": shape("BLOB", "VARBINARY(255)"),
    "media_type": shape("BLOB", "VARBINARY(127)"),
    "resource_kind": shape("BLOB", "VARBINARY(11)"),
    "segment_count": U32,
    "segment_position": U32,
    "extent_offset": U64,
    "extent_length": U64,
    "width": U64,
    "height": U64,
}

UUID_ATTRIBUTES = {
    "analysis_id",
    "base_analysis_id",
    "anchor_analysis_id",
    "ancestor_analysis_id",
    "resolved_analysis_id",
    "build_id",
    "candidate_id",
    "receipt_id",
    "base_receipt_id",
    "preparation_id",
    "scan_attempt",
}

DIGEST_KEY_ATTRIBUTES = {
    "scope_key",
    "file_key",
    "publication_key",
    "locator_sha256",
    "gallery_key",
}


def physical_table_name(relation_name: str) -> str:
    """Preserve established names and derive names only for new relations."""

    return TABLE_NAMES.get(relation_name, f"catalog_{relation_name}")


TABLE_NAMES = {
    "manifest_policy": "catalog_manifest_policies",
    "source_build": "catalog_source_builds",
    "source_build_state": "catalog_source_build_states",
    "source_build_sealed_at": "catalog_source_build_sealed_ats",
    "source_build_base_publication_commit": "catalog_source_build_base_publication_commits",
    "source_build_base_source": "catalog_source_build_base_source",
    "source_scope": "catalog_source_scopes",
    "source_build_discovery": "catalog_source_build_discoveries",
    "gallery_identity": "catalog_gallery_identities",
    "gallery_observation": "catalog_gallery_observations",
    "gallery_observation_allocation": "catalog_gallery_observation_allocations",
    "gallery_observation_page": "catalog_gallery_observation_pages",
    "gallery_observation_allocation_page": "catalog_gallery_observation_allocation_pages",
    "gallery_observation_page_descriptor_anchor": "catalog_gallery_observation_page_descriptor_anchors",
    "gallery_observation_page_descriptor_component": "catalog_gallery_observation_page_descriptor_components",
    "gallery_observation_page_descriptor_level": "catalog_gallery_observation_page_descriptor_levels",
    "gallery_observation_page_descriptor_subtree_item_count": "catalog_gallery_observation_page_descriptor_subtree_item_counts",
    "gallery_observation_page_descriptor_seal": "catalog_gallery_observation_page_descriptor_seals",
    "gallery_observation_page_descriptor": "catalog_gallery_observation_page_descriptors",
    "gallery_observation_page_key_bounds_anchor": "catalog_gallery_observation_page_key_bounds_anchors",
    "gallery_observation_page_key_bounds_first_key": "catalog_gallery_observation_page_key_bounds_first_keys",
    "gallery_observation_page_key_bounds_last_key": "catalog_gallery_observation_page_key_bounds_last_keys",
    "gallery_observation_page_key_bounds_seal": "catalog_gallery_observation_page_key_bounds_seals",
    "gallery_observation_page_key_bounds": "catalog_gallery_observation_page_key_bounds",
    "gallery_observation_page_child": "catalog_gallery_observation_page_children",
    "gallery_observation_tree_root": "catalog_gallery_observation_tree_roots",
    "gallery_upload_time": "catalog_gallery_upload_times",
    "source_gallery_name_gid": "catalog_source_gallery_name_gids",
    "gallery_source_name_access": "catalog_gallery_source_name_accesses",
    "gallery_observation_metadata_local": "catalog_gallery_observation_metadata_locals",
    "gallery_observation_metadata": "catalog_gallery_observation_metadata",
    "gallery_observation_scan": "catalog_gallery_observation_scans",
    "gallery_observation_discovery_fingerprint": "catalog_gallery_observation_discovery_fingerprints",
    "gallery_observation_metadata_digest": "catalog_gallery_observation_metadata_digests",
    "gallery_observation_raw_content": "catalog_gallery_observation_raw_content",
    "gallery_observation_page_count": "catalog_gallery_observation_page_counts",
    "gallery_observation_directory": "catalog_gallery_observation_directories",
    "gallery_observation_stat": "catalog_gallery_observation_stat",
    "source_build_gallery": "catalog_source_build_galleries",
    "file_name_identity": "catalog_file_name_identities",
    "content_blob": "catalog_content_blobs",
    "gallery_observation_file_anchor": "catalog_gallery_observation_file_anchors",
    "gallery_observation_file_file_no": "catalog_gallery_observation_file_file_nos",
    "gallery_observation_file_file_sha256": "catalog_gallery_observation_file_file_sha256s",
    "gallery_observation_file_seal": "catalog_gallery_observation_file_seals",
    "gallery_observation_file": "catalog_gallery_observation_files",
    "gallery_observation_file_filesystem_anchor": "catalog_gallery_observation_file_filesystem_anchors",
    "gallery_observation_file_filesystem_device": "catalog_gallery_observation_file_filesystem_devices",
    "gallery_observation_file_filesystem_inode": "catalog_gallery_observation_file_filesystem_inodes",
    "gallery_observation_file_filesystem_modified_ns": "catalog_gallery_observation_file_filesystem_modified_nses",
    "gallery_observation_file_filesystem_changed_ns": "catalog_gallery_observation_file_filesystem_changed_nses",
    "gallery_observation_file_filesystem_seal": "catalog_gallery_observation_file_filesystem_seals",
    "gallery_observation_file_filesystem": "catalog_gallery_observation_file_filesystem",
    "tag_term": "catalog_tag_terms",
    "gallery_observation_tag": "catalog_gallery_observation_tags",
    "build_manifest": "catalog_build_manifests",
    "gallery_manifest": "catalog_gallery_manifests",
    "analysis_policy": "catalog_analysis_policies",
    "analysis_run": "catalog_analysis_runs",
    "analysis_run_state": "catalog_analysis_run_states",
    "analysis_run_completed_at": "catalog_analysis_run_completed_ats",
    "analysis_baseline": "catalog_analysis_baselines",
    "source_snapshot_manifest_identity": "catalog_source_snapshot_manifest_identity",
    "source_revision": "catalog_source_revisions",
    "source_revision_descriptor": "catalog_source_revision_descriptors",
    "source_revision_generation": "catalog_source_revision_generations",
    "source_head_revision": "catalog_source_head_revisions",
    "source_head_advanced_at": "catalog_source_head_advanced_ats",
    "source_head": "catalog_source_heads",
    "gallery_observation_artist": "catalog_gallery_observation_artists",
    "gallery_observation_file_hash_occurrence": "catalog_gallery_observation_file_hash_occurrences",
    "analysis_changed_gallery": "catalog_analysis_changed_galleries",
    "analysis_changed_file_hash": "catalog_analysis_changed_file_hashes",
    "analysis_exclusion_delta": "catalog_analysis_exclusion_deltas",
    "analysis_impacted_gallery": "catalog_analysis_impacted_galleries",
    "analysis_impacted_content_provenance": "catalog_a_impacted_content_provenance",
    "analysis_impacted_content": "catalog_analysis_impacted_content",
    "analysis_impacted_gid_provenance_storage": "catalog_a_impacted_gid_provenance_storage",
    "analysis_impacted_gid_storage": "catalog_analysis_impacted_gid_storage",
    "analysis_impacted_gid_provenance": "catalog_a_impacted_gid_provenance",
    "analysis_impacted_gid": "catalog_analysis_impacted_gid",
    "analysis_checkpoint": "catalog_analysis_checkpoints",
    "analysis_batch_receipt": "catalog_analysis_batch_receipts",
    "analysis_batch_receipt_stored": "catalog_analysis_batch_receipt_stored",
    "analysis_stage": "catalog_analysis_stages",
    "canonical_digest_policy": "catalog_canonical_digest_policies",
    "canonical_value_allocation": "catalog_canonical_value_allocations",
    "canonical_value_allocation_anchor": "catalog_canonical_value_allocation_anchors",
    "canonical_value_allocation_digest_domain": "catalog_canonical_value_allocation_digest_domains",
    "canonical_value_allocation_byte_count": "catalog_canonical_value_allocation_byte_counts",
    "canonical_value_allocation_allocated_at": "catalog_canonical_value_allocation_allocated_ats",
    "canonical_value_allocation_seal": "catalog_canonical_value_allocation_seals",
    "canonical_value_page": "catalog_canonical_value_pages",
    "canonical_value_page_anchor": "catalog_canonical_value_page_anchors",
    "canonical_value_page_payload": "catalog_canonical_value_page_payloads",
    "canonical_value_page_coordinate": "catalog_canonical_value_page_coordinates",
    "canonical_value_page_subtree_item_count": "catalog_canonical_value_page_subtree_item_counts",
    "canonical_value_page_seal": "catalog_canonical_value_page_seals",
    "canonical_value_page_descriptor": "catalog_canonical_value_page_descriptors",
    "canonical_value_page_parent": "catalog_canonical_value_page_parents",
    "canonical_value_identity": "catalog_canonical_value_identities",
    "analysis_state_anchor": "catalog_analysis_state_anchors",
    "analysis_state_ancestry": "catalog_analysis_state_ancestry",
    "analysis_file_hash_decision_shadow_anchor": "catalog_a_file_decision_shadow_anchors",
    "analysis_file_hash_decision_shadow_occurrence_count": "catalog_a_file_decision_shadow_occurrences",
    "analysis_file_hash_decision_shadow_artist_count": "catalog_a_file_decision_shadow_artists",
    "analysis_file_hash_decision_shadow_maximum_gallery_artist_count": "catalog_a_file_decision_shadow_gallery_artist_max",
    "analysis_file_hash_decision_shadow_seal": "catalog_a_file_decision_shadow_seals",
    "analysis_content_owner_candidate_shadow": "catalog_analysis_content_owner_candidate_shadows",
    "analysis_content_owner_candidate_tombstone": "catalog_analysis_content_owner_candidate_tombstones",
    "analysis_content_owner_candidate_resolved": "catalog_analysis_content_owner_candidate_resolved",
    "analysis_content_owner_shadow": "catalog_analysis_content_owner_shadows",
    "analysis_content_owner_tombstone": "catalog_analysis_content_owner_tombstones",
    "analysis_content_owner_resolved": "catalog_analysis_content_owner_resolved",
    "analysis_gid_candidate_shadow": "catalog_analysis_gid_candidate_shadows",
    "analysis_gid_candidate_tombstone": "catalog_analysis_gid_candidate_tombstones",
    "analysis_gid_candidate_resolved": "catalog_analysis_gid_candidate_resolved",
    "analysis_gid_winner_selection": "catalog_analysis_gid_winner_selections",
    "analysis_gid_winner_shadow": "catalog_analysis_gid_winner_shadows",
    "analysis_gid_winner_tombstone": "catalog_analysis_gid_winner_tombstones",
    "analysis_gid_winner_resolved": "catalog_analysis_gid_winner_resolved",
    "analysis_state_component_seal": "catalog_analysis_state_component_seals",
    "analysis_exclusion_delta_anchor": "catalog_analysis_exclusion_delta_anchors",
    "analysis_exclusion_delta_old_excluded": "catalog_analysis_exclusion_delta_old_excluded_flags",
    "analysis_exclusion_delta_new_excluded": "catalog_analysis_exclusion_delta_new_excluded_flags",
    "analysis_exclusion_delta_change": "catalog_analysis_exclusion_delta_changes",
    "analysis_exclusion_delta_seal": "catalog_analysis_exclusion_delta_seals",
    "publication_candidate": "catalog_publication_candidates",
    "publication_candidate_projection_seal": "catalog_publication_candidate_projection_seals",
    "publication_candidate_projection": "catalog_publication_candidate_projections",
    "publication_candidate_base_publication_commit": "catalog_publication_candidate_base_publication_commits",
    "publication_candidate_base_catalog": "catalog_publication_candidate_base_catalog",
    "publication_candidate_base_source": "catalog_publication_candidate_base_sources",
    "publication_selection_storage": "catalog_publication_selection_storage",
    "publication_selection_occurrence_identity": "catalog_publication_selection_occurrence_identities",
    "publication_selection": "catalog_publication_selections",
    "publication_stage": "catalog_publication_stages",
    "publication_checkpoint": "catalog_publication_checkpoints",
    "publication_batch_receipt": "catalog_publication_batch_receipts",
    "publication_batch_receipt_stored": "catalog_publication_batch_receipt_stored",
    "publication_finalization_checkpoint": "catalog_publication_finalization_checkpoints",
    "publication_finalization_batch_receipt_stored": "catalog_publication_finalization_batch_stored",
    "publication_finalization_batch_receipt": "catalog_publication_finalization_batch_receipts",
    "artifact_policy": "catalog_artifact_policies",
    "artifact_semantic_input": "catalog_artifact_semantic_inputs",
    "artifact_input": "catalog_candidate_artifact_inputs",
    "artifact_delta_old": "catalog_artifact_delta_old",
    "artifact_delta_new": "catalog_artifact_delta_new",
    "artifact_operation": "catalog_artifact_operations",
    "artifact_blob": "catalog_artifact_blobs",
    "prepared_artifact": "catalog_prepared_artifacts",
    "catalog_resource_kind": "catalog_resource_kinds",
    "storage_object_key_identity": "catalog_storage_object_key_identities",
    "storage_object_key_segment": "catalog_storage_object_key_segments",
    "prepared_storage_object": "catalog_prepared_storage_objects",
    "prepared_artifact_descriptor": "catalog_prepared_artifact_descriptors",
    "prepared_page": "catalog_prepared_pages",
    "prepared_thumbnail": "catalog_prepared_thumbnails",
    "catalog_storage_object": "catalog_storage_objects",
    "catalog_page": "catalog_pages",
    "catalog_thumbnail": "catalog_thumbnails",
    "catalog_revision": "catalog_revisions",
    "catalog_revision_descriptor": "catalog_revision_descriptors",
    "catalog_revision_generation": "catalog_revision_generations",
    "publication_generation_node": "catalog_publication_generation_nodes",
    "publication_generation_successor": "catalog_publication_generation_successors",
    "publication_identity": "catalog_publication_identities",
    "display_title_policy": "catalog_display_title_policies",
    "display_title_choice": "catalog_display_title_choices",
    "title_sort": "catalog_title_sorts",
    "catalog_publication_storage": "catalog_publication_storage",
    "catalog_publication_download_time": "catalog_publication_download_times",
    "catalog_publication_occurrence_identity": "catalog_publication_occurrence_identities",
    "catalog_publication": "catalog_publications",
    "catalog_publication_order": "catalog_publication_order",
    "catalog_publication_title": "catalog_publication_titles",
    "catalog_publication_content": "catalog_publication_contents",
    "catalog_contributor": "catalog_contributors",
    "catalog_subject": "catalog_subjects",
    "catalog_artifact": "catalog_artifacts",
    "search_policy": "catalog_search_policies",
    "search_lexeme": "catalog_search_lexemes",
    "search_document": "catalog_search_documents",
    "search_posting": "catalog_search_postings",
    "title_search_posting": "catalog_title_search_postings",
    "discovery_seal": "catalog_discovery_seals",
    "language_facet_order": "catalog_language_facet_order",
    "subject_facet_order": "catalog_subject_facet_order",
    "contributor_facet_order": "catalog_contributor_facet_order",
    "publication_receipt": "catalog_publication_receipts",
    "publication_commit_anchor": "catalog_publication_commit_anchors",
    "publication_commit": "catalog_publication_commits",
    "publication_commit_finalization": "catalog_publication_commit_finalizations",
    "publication_commit_head_receipt": "catalog_publication_commit_head_receipts",
    "publication_commit_head": "catalog_publication_commit_heads",
    "publication_head_revision": "catalog_publication_head_revisions",
    "publication_head_advanced_at": "catalog_publication_head_advanced_ats",
    "publication_head": "catalog_publication_heads",
}


VIEW_TRIPLES = {
    "analysis_file_hash_decision_resolved": (
        "analysis_file_hash_decision_shadow",
        "analysis_file_hash_decision_tombstone",
    ),
    "analysis_content_owner_candidate_resolved": (
        "analysis_content_owner_candidate_shadow",
        "analysis_content_owner_candidate_tombstone",
    ),
    "analysis_content_owner_resolved": (
        "analysis_content_owner_shadow",
        "analysis_content_owner_tombstone",
    ),
    "analysis_gid_candidate_resolved": (
        "analysis_gid_candidate_shadow",
        "analysis_gid_candidate_tombstone",
    ),
    "analysis_gid_winner_resolved": (
        "analysis_gid_winner_shadow",
        "analysis_gid_winner_tombstone",
    ),
}


OVERLAY_RUNTIME_OBLIGATIONS = (
    "in the same transaction, reject any overlay business key present in both its shadow and tombstone table for the same analysis_id; the COMPLETE/READY scan must recheck all five component pairs",
    "before an analysis becomes COMPLETE/READY, verify ancestry is self-at-depth-zero, acyclic, contiguous through every depth, bounded to 0..16, and policy-compatible with its anchor; a depth-zero genesis or compaction has self-only ancestry",
    "before sealing each overlay component or publishing from it, compare the resolved SQL view with the full evaluator for exact key/value equality, not only row counts or changed rows",
)


INDEXES: dict[str, list[tuple[str, list[str], bool]]] = {
    "title_search_posting": [
        (
            "ix_title_search_posting_document",
            ["revision", "publication_key", "value_sha256"],
            False,
        ),
    ],
    "search_posting": [
        (
            "ix_search_posting_document",
            ["revision", "publication_key", "value_sha256"],
            False,
        ),
    ],
    "gallery_observation_allocation_page": [
        ("ix_gallery_observation_allocation_page_digest", ["page_sha256"], False),
    ],
    "source_build_descriptor": [
        ("ix_source_build_policy", ["manifest_policy_id", "build_id"], False),
        ("ix_source_build_scope", ["scope_key", "build_id"], False),
        ("ix_source_build_created", ["created_at", "build_id"], False),
    ],
    "source_build_state": [
        ("ix_source_build_state", ["state", "build_id"], False),
    ],
    "source_build_gallery": [
        (
            "ix_build_gallery_observation",
            ["gallery_id", "observation_id", "build_id"],
            False,
        )
    ],
    "gallery_observation_file_file_sha256": [
        (
            "ix_gallery_file_hash",
            ["file_sha256", "gallery_id", "observation_id", "file_key"],
            False,
        ),
        (
            "ix_gallery_file_hash_ready",
            ["gallery_id", "observation_id", "file_sha256", "file_key"],
            False,
        ),
    ],
    "gallery_observation_file_seal": [
        (
            "ix_gallery_file_name",
            ["file_key", "gallery_id", "observation_id"],
            False,
        ),
    ],
    "gallery_observation_tag": [
        (
            "ix_gallery_tag_term",
            ["tag_id", "gallery_id", "observation_id"],
            False,
        )
    ],
    "gallery_manifest": [
        (
            "ix_gallery_manifest_policy",
            ["manifest_policy_id", "gallery_id", "observation_id"],
            False,
        )
    ],
    "analysis_baseline": [
        (
            "ix_analysis_baseline_base",
            ["base_analysis_id", "analysis_id"],
            False,
        )
    ],
    "gallery_observation_artist": [
        (
            "ix_observation_artist_tag",
            ["artist_tag_id", "gallery_id", "observation_id"],
            False,
        )
    ],
    "gallery_observation_file_hash_occurrence": [
        (
            "ix_observation_hash_occurrence_group",
            ["file_sha256", "gallery_id", "observation_id"],
            False,
        )
    ],
    "analysis_changed_gallery": [
        (
            "ix_analysis_changed_gallery_kind",
            ["analysis_id", "change_kind", "gallery_id"],
            False,
        )
    ],
    "analysis_state_ancestry": [
        (
            "ix_analysis_ancestry_reverse",
            ["ancestor_analysis_id", "analysis_id", "ancestor_depth"],
            False,
        )
    ],
    "analysis_impacted_content_provenance": [
        (
            "ix_a_impacted_content_key_gallery",
            ["analysis_id", "content_sha256", "gallery_id"],
            False,
        )
    ],
    "analysis_content_owner_candidate_shadow": [
        (
            "ix_a_content_candidate_group",
            ["analysis_id", "content_sha256", "gallery_id"],
            False,
        )
    ],
    "artifact_input": [],
    "artifact_operation": [
        (
            "ix_artifact_operation_kind",
            ["candidate_id", "operation", "publication_key"],
            False,
        )
    ],
    "prepared_artifact": [
        (
            "ix_prepared_artifact_state",
            ["candidate_id", "state", "publication_key", "resource_kind"],
            False,
        ),
    ],
    "catalog_artifact": [
        (
            "ix_catalog_artifact_blob",
            ["artifact_sha256", "revision", "publication_key"],
            False,
        ),
        (
            "ix_catalog_artifact_semantics",
            ["artifact_semantics_sha256", "revision", "publication_key"],
            False,
        ),
    ],
    "title_sort": [
        ("ix_title_sort_identity", ["sort_title_sha256", "title_sha256"], False)
    ],
}


def relation_checks(
    name: str, attributes: list[str], ordinal: int
) -> list[dict[str, str]]:
    sqlite: list[str] = []
    maria: list[str] = []

    # SQLite affinity is not a storage-domain contract.  In particular, a
    # BLOB column accepts values of every storage class unless the generated
    # DDL explicitly checks ``typeof``.  Derive one closed storage-class
    # predicate for every physical column instead of relying on a growing
    # hand-maintained list of attributes below.  Nullable columns admit NULL
    # explicitly; all non-NULL values must have the declared storage class.
    for attribute in sorted(attributes):
        template: dict[str, Any] | None
        if attribute in UUID_ATTRIBUTES:
            template = UUID_BYTES
        elif attribute.endswith("_sha256") or attribute in DIGEST_KEY_ATTRIBUTES:
            template = DIGEST_BYTES
        else:
            template = NEW_ATTRIBUTE_SHAPES.get(attribute)
        if template is None:
            raise KeyError(f"No physical type for {name}.{attribute}")
        storage_class = str(template["sqlite"]["type"]).lower()
        nullable = nullable_override(name, attribute)
        if nullable is None:
            nullable = bool(template["sqlite"]["nullable"])
        predicate = f"typeof({attribute}) = '{storage_class}'"
        sqlite.append(
            f"({attribute} IS NULL OR {predicate})" if nullable else predicate
        )

    digest_attrs = {
        value
        for value in attributes
        if value.endswith("_sha256") or value in DIGEST_KEY_ATTRIBUTES
    }
    uuid_attrs = set(attributes) & UUID_ATTRIBUTES
    nonnegative = {
        value
        for value in attributes
        if value.endswith("_count")
        or value
        in {
            "generation",
            "predecessor_generation",
            "successor_generation",
            "position",
            "size_bytes",
            "row_count",
            "byte_count",
            "directory_entry_count",
            "file_no",
            "page_count",
            "source_file_count",
            "base_revision",
            "source_revision",
            "revision",
            "artist_count",
            "maximum_gallery_artist_count",
            "new_galleries",
            "changed_galleries",
            "removed_galleries",
            "duplicate_losers",
            "base_source_revision",
        }
    }
    positive = {
        value
        for value in attributes
        if (value.endswith("_version") and value != "unicode_data_version")
        or value
        in {
            "gallery_id",
            "witness_gallery_id",
            "owner_gallery_id",
            "winner_gallery_id",
            "artist_tag_id",
            "artifact_component_id",
            "artifact_policy_id",
            "display_title_policy_id",
            "gid",
            "reserved_revision",
            "manifest_policy_id",
            "policy_id",
            "tag_id",
            "observation_id",
            "base_source_revision",
            "base_source_generation",
            "base_revision",
            "base_catalog_generation",
            "source_revision",
            "revision",
            "generation",
            "successor_generation",
            "segment_count",
            "extent_length",
            "width",
            "height",
        }
    }
    if name == "publication_generation_node":
        positive.discard("generation")
    integer_attrs = {
        attribute
        for attribute in attributes
        if attribute not in UUID_ATTRIBUTES
        and not attribute.endswith("_sha256")
        and attribute not in DIGEST_KEY_ATTRIBUTES
        and NEW_ATTRIBUTE_SHAPES.get(attribute, {}).get("sqlite", {}).get("type")
        == "INTEGER"
    }
    uint32_attrs = {
        attribute
        for attribute in attributes
        if NEW_ATTRIBUTE_SHAPES.get(attribute, {}).get("mariadb", {}).get("type")
        == "INT UNSIGNED"
    }
    unsigned_attrs = {
        attribute
        for attribute in attributes
        if "UNSIGNED"
        in str(
            (
                UUID_BYTES
                if attribute in UUID_ATTRIBUTES
                else DIGEST_BYTES
                if attribute.endswith("_sha256") or attribute in DIGEST_KEY_ATTRIBUTES
                else NEW_ATTRIBUTE_SHAPES.get(attribute, {})
            )
            .get("mariadb", {})
            .get("type", "")
        ).upper()
    }
    for attribute in sorted(digest_attrs):
        sqlite.append(f"typeof({attribute}) = 'blob' AND length({attribute}) = 32")
        maria.append(f"octet_length({attribute}) = 32")
    for attribute in sorted(uuid_attrs):
        sqlite.append(f"typeof({attribute}) = 'blob' AND length({attribute}) = 16")
        maria.append(f"octet_length({attribute}) = 16")
    if "unicode_data_version" in attributes:
        sqlite.append(
            "typeof(unicode_data_version) = 'blob' AND "
            "length(unicode_data_version) BETWEEN 1 AND 32"
        )
        maria.append("octet_length(unicode_data_version) BETWEEN 1 AND 32")
    for attribute in sorted(integer_attrs - (set(attributes) & TIMESTAMP_ATTRIBUTES)):
        sqlite.append(f"typeof({attribute}) = 'integer'")
        sqlite.append(f"{attribute} <= 9223372036854775807")
        maria.append(f"{attribute} <= 9223372036854775807")
    for attribute in sorted(uint32_attrs):
        sqlite.append(f"{attribute} <= 4294967295")
        maria.append(f"{attribute} <= 4294967295")
    # MariaDB UNSIGNED is part of the generated physical type.  Render the
    # same lower bound into SQLite for exact cross-backend portability (and
    # retain it in MariaDB CHECK metadata as an executable contract).
    for attribute in sorted(unsigned_attrs):
        sqlite.append(f"{attribute} >= 0")
        maria.append(f"{attribute} >= 0")
    for attribute in sorted(nonnegative - positive):
        sqlite.append(f"{attribute} >= 0")
        maria.append(f"{attribute} >= 0")
    for attribute in sorted(positive):
        sqlite.append(f"{attribute} > 0")
        maria.append(f"{attribute} > 0")
    for attribute in sorted(set(attributes) & TIMESTAMP_ATTRIBUTES):
        sqlite.append(
            f"({attribute} IS NULL OR typeof({attribute}) = 'integer' AND "
            f"{attribute} BETWEEN 0 AND 9223372036854775807)"
        )
        maria.append(f"({attribute} IS NULL OR {attribute} <= 9223372036854775807)")

    if "overlay_depth" in attributes:
        sqlite.append("overlay_depth <= 16")
        maria.append("overlay_depth <= 16")
    if "ancestor_depth" in attributes:
        sqlite.append("ancestor_depth <= 16")
        maria.append("ancestor_depth <= 16")
    if name == "analysis_state_anchor":
        expression = "(overlay_depth = 0 AND anchor_analysis_id = analysis_id OR overlay_depth BETWEEN 1 AND 16 AND anchor_analysis_id <> analysis_id)"
        sqlite.append(expression)
        maria.append(expression)
    if name == "analysis_state_ancestry":
        expression = "(ancestor_depth = 0 AND ancestor_analysis_id = analysis_id OR ancestor_depth BETWEEN 1 AND 16 AND ancestor_analysis_id <> analysis_id)"
        sqlite.append(expression)
        maria.append(expression)
    if name.startswith("analysis_state_component_") and "state_component" in attributes:
        state_components = (
            "file_hash_decision",
            "content_owner_candidate",
            "content_owner",
            "gid_candidate",
            "gid_winner",
        )
        # state_component is an exact VARBINARY/BLOB byte domain.  Text
        # literals compare as TEXT in SQLite, making the previous predicate
        # incompatible with the simultaneous typeof(...)=blob requirement.
        # Render exact binary literals on both backends instead.
        domain = (
            "("
            + ", ".join(
                f"X'{value.encode('ascii').hex().upper()}'"
                for value in state_components
            )
            + ")"
        )
        sqlite.append(f"state_component IN {domain}")
        maria.append(f"state_component IN {domain}")
    if "artifacts_required" in attributes:
        sqlite.append("artifacts_required IN (0, 1)")
        maria.append("artifacts_required IN (0, 1)")
    if "resource_kind" in attributes:
        resource_kinds = "(X'6163717569736974696F6E', X'7468756D626E61696C')"
        sqlite.append(f"resource_kind IN {resource_kinds}")
        maria.append(f"resource_kind IN {resource_kinds}")
    if name == "source_build_state":
        sqlite.append("state IN ('OPEN', 'SEALED', 'ABANDONED')")
        maria.append("state IN ('OPEN', 'SEALED', 'ABANDONED')")
    elif name == "analysis_run_state":
        sqlite.append("state IN ('OPEN', 'COMPLETE', 'ABANDONED')")
        maria.append("state IN ('OPEN', 'COMPLETE', 'ABANDONED')")
    if name in {"analysis_checkpoint", "publication_checkpoint"}:
        sqlite.append("state IN ('OPEN', 'COMPLETE')")
        maria.append("state IN ('OPEN', 'COMPLETE')")
    elif name == "prepared_artifact":
        sqlite.append("state IN ('PENDING', 'PREPARED', 'COMMITTED')")
        maria.append("state IN ('PENDING', 'PREPARED', 'COMMITTED')")
        sqlite.append(
            "typeof(protection_token) = 'blob' AND length(protection_token) = 32"
        )
        maria.append("octet_length(protection_token) = 32")
    elif name == "publication_receipt":
        sqlite.append("state IN ('DB_COMMITTED', 'PUBLISHED')")
        maria.append("state IN ('DB_COMMITTED', 'PUBLISHED')")
        final_rule = "(state = 'DB_COMMITTED' AND finalized_at IS NULL OR state = 'PUBLISHED' AND finalized_at IS NOT NULL)"
        sqlite.append(final_rule)
        maria.append(final_rule)
    if name == "artifact_operation":
        sqlite.append("operation IN ('CREATE', 'REBUILD', 'DELETE', 'UNCHANGED')")
        maria.append("operation IN ('CREATE', 'REBUILD', 'DELETE', 'UNCHANGED')")
    if name == "catalog_revision_descriptor":
        sqlite.append("(artifact_count = 0 OR artifact_count = publication_count)")
        maria.append("(artifact_count = 0 OR artifact_count = publication_count)")
    if "next_state" in attributes:
        sqlite.append("next_state IN ('OPEN', 'COMPLETE')")
        maria.append("next_state IN ('OPEN', 'COMPLETE')")
    if "terminal" in attributes:
        sqlite.append("terminal IN (0, 1)")
        maria.append("terminal IN (0, 1)")
    if name in {"analysis_batch_receipt", "publication_batch_receipt"}:
        receipt_rule = (
            "committed_generation = start_generation + 1 AND "
            "next_processed_count = start_processed_count + row_count AND "
            "(terminal = 0 AND row_count > 0 AND next_state = 'OPEN' OR "
            "terminal = 1 AND row_count = 0 AND next_state = 'COMPLETE' AND "
            "next_cursor = start_cursor AND "
            "next_processed_count = start_processed_count)"
        )
        sqlite.append(receipt_rule)
        maria.append(receipt_rule)
    if "component" in attributes:
        sqlite.append(
            "component IN (X'46494C45', X'544147', X'4449524543544F5259', X'4D45544144415441')"
        )
        maria.append("component IN ('FILE', 'TAG', 'DIRECTORY', 'METADATA')")
    if "level" in attributes:
        sqlite.append("level BETWEEN 0 AND 8")
        maria.append("level BETWEEN 0 AND 8")
    if name == "canonical_value_page_coordinate":
        sqlite.append("page_position >= 0")
        maria.append("page_position >= 0")
    if name in {"canonical_value_page_parent", "gallery_observation_page_child"}:
        sqlite.append("position BETWEEN 0 AND 255")
        maria.append("position BETWEEN 0 AND 255")
    if "page_bytes" in attributes:
        sqlite.append(
            "typeof(page_bytes) = 'blob' AND length(page_bytes) BETWEEN 1 AND 65536"
        )
        maria.append("octet_length(page_bytes) BETWEEN 1 AND 65536")
    for attribute in ("first_key", "last_key"):
        if attribute in attributes:
            sqlite.append(
                f"typeof({attribute}) = 'blob' AND length({attribute}) BETWEEN 1 AND 255"
            )
            maria.append(f"octet_length({attribute}) BETWEEN 1 AND 255")
    for attribute in ("device", "inode", "modified_ns", "changed_ns"):
        if attribute in attributes:
            sqlite.append(f"typeof({attribute}) = 'blob' AND length({attribute}) = 8")
            maria.append(f"octet_length({attribute}) = 8")
    for attribute in ("old_excluded", "new_excluded"):
        if attribute in attributes:
            sqlite.append(f"{attribute} IN (0, 1)")
            maria.append(f"{attribute} IN (0, 1)")
    if "prefer_not_already_uploaded" in attributes:
        sqlite.append("prefer_not_already_uploaded IN (0, 1)")
        maria.append("prefer_not_already_uploaded IN (0, 1)")
    if "page_limit" in attributes:
        sqlite.append("page_limit BETWEEN 1 AND 128")
        maria.append("page_limit BETWEEN 1 AND 128")
    if "page_count" in attributes:
        sqlite.append("page_count BETWEEN 0 AND 4096")
        maria.append("page_count BETWEEN 0 AND 4096")
    if "page_index" in attributes:
        sqlite.append("page_index BETWEEN 0 AND 4095")
        maria.append("page_index BETWEEN 0 AND 4095")
    if "segment_count" in attributes:
        sqlite.append("segment_count BETWEEN 1 AND 16")
        maria.append("segment_count BETWEEN 1 AND 16")
    if "segment_position" in attributes:
        sqlite.append("segment_position BETWEEN 0 AND 15")
        maria.append("segment_position BETWEEN 0 AND 15")
    if "excluded_flag" in attributes:
        sqlite.append("excluded_flag IN (0, 1)")
        maria.append("excluded_flag IN (0, 1)")
    if "occurrence_count" in attributes and name not in {"analysis_exclusion_delta"}:
        sqlite.append("occurrence_count > 0")
        maria.append("occurrence_count > 0")
    for attribute in (
        "artifact_name",
        "publication_id",
        "source_gallery_name",
        "name",
        "locator",
        "channel",
        "stage",
        "state",
        "operation",
        "component_kind",
        "state_component",
        "adapter_id",
    ):
        if attribute in attributes:
            sqlite.append(f"length({attribute}) > 0")
            maria.append(f"octet_length({attribute}) > 0")

    byte_bounds = {
        "source_gallery_name": 255,
        "name_bytes": 255,
        "artifact_name": 255,
        "publication_id": 64,
        "namespace": 128,
        "source_provider": 64,
        "channel": 64,
        "stage": 64,
        "state_component": 64,
        "component_kind": 32,
        "file_role": 8,
        "role": 64,
        "batch_key": 512,
        "digest_domain": 64,
        "metadata_fingerprint": 40,
        "cursor": 2048,
        "protection_token": 32,
        "adapter_id": 64,
        "key_codec": 64,
        "key_segment": 255,
        "media_type": 127,
    }
    for attribute, maximum in byte_bounds.items():
        if attribute in attributes:
            sqlite.append(
                f"typeof({attribute}) = 'blob' AND length({attribute}) <= {maximum}"
            )
            maria_attribute = f"`{attribute}`" if attribute == "cursor" else attribute
            maria.append(f"octet_length({maria_attribute}) <= {maximum}")

    for attribute in ("key_codec", "key_segment", "media_type"):
        if attribute in attributes:
            sqlite.append(f"length({attribute}) > 0")
            maria.append(f"octet_length({attribute}) > 0")

    if name in {
        "artifact_blob",
        "prepared_storage_object",
        "catalog_storage_object",
    }:
        sqlite.append("size_bytes > 0")
        maria.append("size_bytes > 0")

    if "metadata_fingerprint" in attributes:
        sqlite.append(
            "typeof(metadata_fingerprint) = 'blob' AND "
            "length(metadata_fingerprint) = 40"
        )
        maria.append("octet_length(metadata_fingerprint) = 40")

    if not sqlite:
        return []
    return [
        {
            "name": f"ck_vnext_{ordinal:03}_domain",
            "sqlite_expression": " AND ".join(sqlite),
            "mariadb_expression": " AND ".join(maria),
        }
    ]


def nullable_override(name: str, attribute: str) -> bool | None:
    if (name, attribute) in {
        ("analysis_run", "completed_at"),
        ("publication_candidate", "sealed_at"),
        ("publication_receipt", "finalized_at"),
        ("source_build", "sealed_at"),
    }:
        return True
    if (name, attribute) in {
        ("analysis_state_component_sealed_at", "sealed_at"),
        ("analysis_state_component_seal", "sealed_at"),
        ("publication_candidate_base_source_revision", "base_source_revision"),
        ("source_build_sealed_at", "sealed_at"),
    }:
        return False
    return None


def copy_shape(value: dict[str, Any], nullable: bool | None) -> dict[str, Any]:
    answer = {backend: dict(value[backend]) for backend in ("sqlite", "mariadb")}
    if nullable is not None:
        answer["sqlite"]["nullable"] = nullable
        answer["mariadb"]["nullable"] = nullable
    return answer


def portable_identifier(value: str) -> str:
    """Return one deterministic identifier within MariaDB's 64-byte ceiling."""

    encoded = value.encode("ascii")
    if len(encoded) <= 63:
        return value
    return f"{value[:50]}_{hashlib.sha256(encoded).hexdigest()[:12]}"


def make_relation(
    logical: dict[str, Any],
    ordinal: int,
    *,
    vertical_view: dict[str, Any] | None = None,
    generation_view: dict[str, Any] | None = None,
) -> dict[str, Any]:
    name = str(logical["name"])
    attributes = list(logical["attributes"])
    materialization = logical.get("materialization")
    if isinstance(materialization, dict):
        rationale = str(materialization.get("rationale", ""))
    else:
        rationale = str(logical.get("rationale", ""))
    if not rationale:
        rationale = (
            f"Production physical realization of the closed-world {name} relation."
        )
    relation: dict[str, Any] = {
        "name": name,
        "status": "implemented",
        "rationale": rationale,
        "table": physical_table_name(name),
        "primary_key": list(logical["declared_keys"][0]),
        "unique_keys": [list(key) for key in logical["declared_keys"][1:]],
        "referential_unique_keys": [
            list(key) for key in logical.get("referential_unique_keys", [])
        ],
        "runtime_unique_keys": [],
        "column": [],
        "foreign_key": [],
        "required_index": [],
        "check": [],
    }
    if name in {
        "canonical_value_page",
        "canonical_value_page_payload",
        "gallery_observation_page",
    }:
        relation["runtime_unique_keys"] = relation["unique_keys"]
        relation["unique_keys"] = []
    if name in VIEW_TRIPLES:
        relation["runtime_unique_keys"] = relation["unique_keys"]
        relation["unique_keys"] = []
    for attribute in attributes:
        template: dict[str, Any] | None
        if attribute in UUID_ATTRIBUTES:
            template = UUID_BYTES
        elif attribute.endswith("_sha256") or attribute in DIGEST_KEY_ATTRIBUTES:
            template = DIGEST_BYTES
        else:
            template = NEW_ATTRIBUTE_SHAPES.get(attribute)
        if template is None:
            raise KeyError(f"No physical type for {name}.{attribute}")
        backend_shape = copy_shape(template, nullable_override(name, attribute))
        if (
            name
            in {
                "analysis_batch_receipt",
                "publication_batch_receipt",
                "publication_finalization_batch_receipt",
            }
            and attribute == "next_state"
        ) or (
            (name, attribute)
            in {
                ("analysis_impacted_gid", "witness_gallery_id"),
                ("publication_receipt", "state"),
            }
        ):
            # MariaDB conservatively reports computed view expressions as nullable,
            # including total CASE expressions and MIN over a mandatory inner join.
            # The exact view definition remains the non-null logical authority; this
            # flag describes MariaDB's INFORMATION_SCHEMA presentation.
            backend_shape["mariadb"]["nullable"] = True
        relation["column"].append(
            {
                "attribute": attribute,
                "name": attribute,
                **backend_shape,
            }
        )
    for position, foreign_key in enumerate(logical.get("foreign_keys", []), 1):
        relation["foreign_key"].append(
            {
                "name": portable_identifier(f"fk_{name}_{position}"),
                "attributes": list(foreign_key["attributes"]),
                "referenced_relation": foreign_key["relation"],
                "referenced_attributes": list(foreign_key["referenced_attributes"]),
            }
        )
    for index_name, index_attributes, unique in INDEXES.get(name, []):
        if not set(index_attributes) <= set(attributes):
            continue
        relation["required_index"].append(
            {"name": index_name, "attributes": index_attributes, "unique": unique}
        )
    if name in VIEW_TRIPLES:
        shadow, tombstone = VIEW_TRIPLES[name]
        relation["kind"] = "view"
        relation["view"] = {
            "pattern": "nearest_ancestor_overlay",
            "ancestry_relation": "analysis_state_ancestry",
            "shadow_relation": shadow,
            "tombstone_relation": tombstone,
        }
    elif vertical_view is not None:
        relation["kind"] = "view"
        relation["view"] = {
            "pattern": "sealed_vertical_family",
            "family": str(vertical_view["name"]),
            "anchor_relation": str(vertical_view["anchor_relation"]),
            "seal_relation": str(vertical_view["seal_relation"]),
            "key_attributes": list(vertical_view["key_attributes"]),
            "members": [dict(member) for member in vertical_view["members"]],
        }
        projection_attributes = vertical_view.get("projection_attributes")
        if isinstance(projection_attributes, list):
            relation["view"]["projection_attributes"] = list(projection_attributes)
        optional_presence = vertical_view.get("optional_presence")
        if isinstance(optional_presence, dict):
            relation["view"]["optional_presence"] = dict(optional_presence)
    elif generation_view is not None:
        relation["kind"] = "view"
        relation["view"] = generation_view
    else:
        relation["check"] = relation_checks(name, attributes, ordinal)
    if relation.get("kind") == "view":
        # Logical view access paths remain query-planning guidance only.  SQL
        # views cannot own indexes or CHECK constraints, so the physical
        # contract must not pretend those declarations are enforceable.
        relation["required_index"] = []
        relation["check"] = []
    return relation


def stable_fk_index_name(
    relation_name: str,
    position: int,
    attributes: list[str],
) -> str:
    raw = f"ix_fk_{relation_name}_{position}_{'_'.join(attributes)}"
    return portable_identifier(raw)


def add_missing_foreign_key_indexes(relations: list[dict[str, Any]]) -> None:
    """Give SQLite and MariaDB an explicit left-prefix index for every FK."""

    used_names = {
        str(index["name"])
        for relation in relations
        for index in relation.get("required_index", [])
    }
    for relation in relations:
        if relation.get("kind") == "view":
            continue
        indexes = relation["required_index"]
        assert isinstance(indexes, list)
        key_prefixes = [
            list(relation["primary_key"]),
            *(list(key) for key in relation["unique_keys"]),
            *(list(key) for key in relation.get("referential_unique_keys", [])),
            *(list(index["attributes"]) for index in indexes),
        ]
        for position, foreign_key in enumerate(
            relation.get("foreign_key", []),
            start=1,
        ):
            attributes = list(foreign_key["attributes"])
            if any(key[: len(attributes)] == attributes for key in key_prefixes):
                continue
            index_name = stable_fk_index_name(
                str(relation["name"]),
                position,
                attributes,
            )
            if index_name in used_names:
                raise RuntimeError(f"Duplicate generated FK index {index_name!r}")
            index = {
                "name": index_name,
                "attributes": attributes,
                "unique": False,
            }
            indexes.append(index)
            key_prefixes.append(attributes)
            used_names.add(index_name)


def inline_strings(values: list[str]) -> str:
    return "[" + ", ".join(q(value) for value in values) + "]"


def emit_relation(relation: dict[str, Any]) -> str:
    lines = [
        "[[relation]]",
        f"name = {q(str(relation['name']))}",
        f"status = {q(str(relation['status']))}",
        f"rationale = {q(str(relation['rationale']))}",
        f"table = {q(str(relation['table']))}",
    ]
    if relation.get("kind") == "view":
        lines.append('kind = "view"')
        view = relation["view"]
        assert isinstance(view, dict)
        if view["pattern"] == "nearest_ancestor_overlay":
            lines.append(
                "view = { pattern = "
                + q(str(view["pattern"]))
                + ", ancestry_relation = "
                + q(str(view["ancestry_relation"]))
                + ", shadow_relation = "
                + q(str(view["shadow_relation"]))
                + ", tombstone_relation = "
                + q(str(view["tombstone_relation"]))
                + " }"
            )
        elif view["pattern"] == "sealed_vertical_family":
            members = view["members"]
            assert isinstance(members, list)
            rendered_members = ", ".join(
                "{ relation = "
                + q(str(member["relation"]))
                + ", key_attributes = "
                + inline_strings(member["key_attributes"])
                + ", value_attribute = "
                + q(str(member["value_attribute"]))
                + ", projection_attribute = "
                + q(str(member.get("projection_attribute", member["value_attribute"])))
                + ", join = { source_relation = "
                + q(str(member["join"]["source_relation"]))
                + ", source_attributes = "
                + inline_strings(member["join"]["source_attributes"])
                + ", member_attributes = "
                + inline_strings(member["join"]["member_attributes"])
                + " }, project = "
                + str(bool(member["project"])).lower()
                + ", required = "
                + str(bool(member.get("required", True))).lower()
                + " }"
                for member in members
            )
            lines.append(
                "view = { pattern = "
                + q(str(view["pattern"]))
                + ", family = "
                + q(str(view["family"]))
                + ", anchor_relation = "
                + q(str(view["anchor_relation"]))
                + ", seal_relation = "
                + q(str(view["seal_relation"]))
                + ", key_attributes = "
                + inline_strings(view["key_attributes"])
                + (
                    ", projection_attributes = "
                    + inline_strings(view["projection_attributes"])
                    if "projection_attributes" in view
                    else ""
                )
                + ", members = ["
                + rendered_members
                + "]"
                + (
                    ", optional_presence = { member_relation = "
                    + q(str(view["optional_presence"]["member_relation"]))
                    + ", discriminator_relation = "
                    + q(str(view["optional_presence"]["discriminator_relation"]))
                    + ", discriminator_attribute = "
                    + q(str(view["optional_presence"]["discriminator_attribute"]))
                    + ", present_value = "
                    + q(str(view["optional_presence"]["present_value"]))
                    + ", absent_values = "
                    + inline_strings(view["optional_presence"]["absent_values"])
                    + " }"
                    if "optional_presence" in view
                    else ""
                )
                + " }"
            )
        elif view["pattern"] == "revision_generation_baseline":
            lines.append(
                "view = { pattern = "
                + q(str(view["pattern"]))
                + ", base_relation = "
                + q(str(view["base_relation"]))
                + ", mapping_relation = "
                + q(str(view["mapping_relation"]))
                + ", owner_attribute = "
                + q(str(view["owner_attribute"]))
                + ", revision_attribute = "
                + q(str(view["revision_attribute"]))
                + ", mapping_revision_attribute = "
                + q(str(view["mapping_revision_attribute"]))
                + ", generation_attribute = "
                + q(str(view["generation_attribute"]))
                + ", mapping_generation_attribute = "
                + q(str(view["mapping_generation_attribute"]))
                + " }"
            )
        elif view["pattern"] == "revision_generation_head":
            lines.append(
                "view = { pattern = "
                + q(str(view["pattern"]))
                + ", revision_relation = "
                + q(str(view["revision_relation"]))
                + ", time_relation = "
                + q(str(view["time_relation"]))
                + ", mapping_relation = "
                + q(str(view["mapping_relation"]))
                + ", channel_attribute = "
                + q(str(view["channel_attribute"]))
                + ", revision_attribute = "
                + q(str(view["revision_attribute"]))
                + ", generation_attribute = "
                + q(str(view["generation_attribute"]))
                + ", time_attribute = "
                + q(str(view["time_attribute"]))
                + " }"
            )
        elif view["pattern"] in {
            "analysis_ancestry_endpoint",
            "analysis_gid_winner_keyset",
            "artifact_delta_old",
            "artifact_delta_new",
            "build_manifest_projection",
            "analysis_impacted_gid_projection",
            "analysis_impacted_gid_provenance_projection",
            "catalog_publication_occurrence_identity",
            "catalog_publication_projection",
            "catalog_publication_title_projection",
            "gallery_observation_metadata_projection",
            "publication_selection_occurrence_identity",
            "publication_selection_projection",
            "publication_candidate_projection",
            "batch_receipt_derived",
            "publication_commit_baseline",
            "publication_commit_published_descriptor",
            "publication_commit_generation",
            "publication_commit_head",
            "publication_commit_head_projection",
            "publication_receipt",
            "lifecycle_projection",
        }:
            extra_fields = []
            for field in (
                "projection",
                "owner_attribute",
                "stage_attribute",
                "batch_key_attribute",
                "start_generation_attribute",
                "start_cursor_attribute",
                "start_processed_count_attribute",
                "page_limit_attribute",
                "next_cursor_attribute",
                "next_processed_count_attribute",
                "next_state_attribute",
                "row_count_attribute",
                "terminal_attribute",
                "committed_generation_attribute",
                "committed_at_attribute",
                "stored_relation",
                "checkpoint_relation",
            ):
                if field in view:
                    extra_fields.append(", " + field + " = " + q(str(view[field])))
            lines.append(
                "view = { pattern = "
                + q(str(view["pattern"]))
                + ", source_relations = "
                + inline_strings(view["source_relations"])
                + ""
                + "".join(extra_fields)
                + " }"
            )
        else:
            raise RuntimeError(
                f"Unsupported generated view pattern {view['pattern']!r}"
            )
    lines.append(f"primary_key = {inline_strings(relation['primary_key'])}")
    unique_keys = relation["unique_keys"]
    assert isinstance(unique_keys, list)
    lines.append(
        "unique_keys = [" + ", ".join(inline_strings(key) for key in unique_keys) + "]"
    )
    referential_unique_keys = relation.get("referential_unique_keys", [])
    assert isinstance(referential_unique_keys, list)
    if referential_unique_keys:
        lines.append(
            "referential_unique_keys = ["
            + ", ".join(inline_strings(key) for key in referential_unique_keys)
            + "]"
        )
    runtime_unique_keys = relation.get("runtime_unique_keys", [])
    assert isinstance(runtime_unique_keys, list)
    if runtime_unique_keys:
        lines.append(
            "runtime_unique_keys = ["
            + ", ".join(inline_strings(key) for key in runtime_unique_keys)
            + "]"
        )
    lines.append("column = [")
    for column in relation["column"]:
        sqlite = column["sqlite"]
        mariadb = column["mariadb"]
        lines.append(
            "  { attribute = "
            + q(column["attribute"])
            + ", name = "
            + q(column["name"])
            + ", sqlite = { type = "
            + q(sqlite["type"])
            + ", nullable = "
            + str(sqlite["nullable"]).lower()
            + ", collation = "
            + q(sqlite["collation"])
            + " }, mariadb = { type = "
            + q(mariadb["type"])
            + ", nullable = "
            + str(mariadb["nullable"]).lower()
            + ", collation = "
            + q(mariadb["collation"])
            + " } },"
        )
    lines.append("]")
    for field, heading in (
        ("foreign_key", "foreign_key"),
        ("required_index", "required_index"),
        ("check", "check"),
    ):
        values = relation.get(field, [])
        assert isinstance(values, list)
        if not values:
            continue
        lines.append(f"{heading} = [")
        for value in values:
            if field == "foreign_key":
                rendered = (
                    f"name = {q(value['name'])}, attributes = {inline_strings(value['attributes'])}, "
                    f"referenced_relation = {q(value['referenced_relation'])}, "
                    f"referenced_attributes = {inline_strings(value['referenced_attributes'])}"
                )
            elif field == "required_index":
                rendered = (
                    f"name = {q(value['name'])}, attributes = {inline_strings(value['attributes'])}, "
                    f"unique = {str(value['unique']).lower()}"
                )
            else:
                rendered = (
                    f"name = {q(value['name'])}, sqlite_expression = {q(value['sqlite_expression'])}, "
                    f"mariadb_expression = {q(value['mariadb_expression'])}"
                )
            lines.append("  { " + rendered + " },")
        lines.append("]")
    return "\n".join(lines)


def topological_order(
    relations: list[dict[str, Any]],
    external_relations: set[str] | None = None,
) -> list[str]:
    by_name = {str(relation["name"]): relation for relation in relations}
    external_names = external_relations or set()
    dependencies: dict[str, set[str]] = {name: set() for name in by_name}
    reverse: dict[str, set[str]] = defaultdict(set)
    for name, relation in by_name.items():
        for foreign_key in relation.get("foreign_key", []):
            target = str(foreign_key["referenced_relation"])
            if target in external_names:
                continue
            if target not in by_name:
                raise RuntimeError(
                    f"Physical relation {name!r} references unknown target {target!r}"
                )
            dependencies[name].add(target)
            reverse[target].add(name)
        view = relation.get("view")
        if isinstance(view, dict):
            pattern = str(view["pattern"])
            if pattern == "nearest_ancestor_overlay":
                view_dependencies = tuple(
                    str(view[field])
                    for field in (
                        "ancestry_relation",
                        "shadow_relation",
                        "tombstone_relation",
                    )
                )
            elif pattern == "sealed_vertical_family":
                view_dependencies = (
                    str(view["anchor_relation"]),
                    str(view["seal_relation"]),
                    *(str(member["relation"]) for member in view["members"]),
                )
            elif pattern == "revision_generation_baseline":
                view_dependencies = (
                    str(view["base_relation"]),
                    str(view["mapping_relation"]),
                )
            elif pattern == "revision_generation_head":
                view_dependencies = (
                    str(view["revision_relation"]),
                    str(view["time_relation"]),
                    str(view["mapping_relation"]),
                )
            elif pattern in {
                "analysis_ancestry_endpoint",
                "analysis_gid_winner_keyset",
                "artifact_delta_old",
                "artifact_delta_new",
                "build_manifest_projection",
                "analysis_impacted_gid_projection",
                "analysis_impacted_gid_provenance_projection",
                "catalog_publication_occurrence_identity",
                "catalog_publication_projection",
                "catalog_publication_title_projection",
                "gallery_observation_metadata_projection",
                "publication_selection_occurrence_identity",
                "publication_selection_projection",
                "publication_candidate_projection",
                "batch_receipt_derived",
                "publication_commit_baseline",
                "publication_commit_published_descriptor",
                "publication_commit_generation",
                "publication_commit_head",
                "publication_commit_head_projection",
                "publication_receipt",
                "lifecycle_projection",
            }:
                view_dependencies = tuple(
                    str(value) for value in view["source_relations"]
                )
            else:
                raise RuntimeError(f"Unsupported view pattern {pattern!r}")
            for target in view_dependencies:
                dependencies[name].add(target)
                reverse[target].add(name)
    catalog_order = {name: position for position, name in enumerate(by_name)}
    ready = sorted(
        (name for name, values in dependencies.items() if not values),
        key=catalog_order.__getitem__,
    )
    result: list[str] = []
    while ready:
        name = ready.pop(0)
        result.append(name)
        for dependent in sorted(reverse[name], key=catalog_order.__getitem__):
            dependencies[dependent].discard(name)
            if (
                not dependencies[dependent]
                and dependent not in result
                and dependent not in ready
            ):
                ready.append(dependent)
        ready.sort(key=catalog_order.__getitem__)
    if len(result) != len(by_name):
        unresolved = {name: values for name, values in dependencies.items() if values}
        raise RuntimeError(f"Physical FK/view dependency cycle: {unresolved}")
    return result


def inline_projection_names(catalog: dict[str, Any]) -> tuple[str, ...]:
    """Return logical views expanded by runtime queries instead of SQL VIEWs."""

    names: list[str] = []
    for relation in catalog["relation"]:
        materialization = relation.get("materialization")
        if not isinstance(materialization, dict):
            continue
        storage = materialization.get("storage")
        if storage != "inline_projection":
            continue
        if materialization.get("authoritative") is not False:
            raise RuntimeError(
                f"Inline projection {relation['name']!r} must be non-authoritative"
            )
        names.append(str(relation["name"]))
    return tuple(names)


def render() -> str:
    catalog = tomllib.loads(CATALOG_PATH.read_text())
    inline_projections = inline_projection_names(catalog)
    inline_projection_set = set(inline_projections)
    for logical in catalog["relation"]:
        relation_name = str(logical["name"])
        if relation_name in inline_projection_set:
            continue
        for foreign_key in logical.get("foreign_keys", []):
            target = str(foreign_key["relation"])
            if target in inline_projection_set:
                raise RuntimeError(
                    f"Physical relation {relation_name!r} cannot reference inline "
                    f"projection {target!r} through a foreign key"
                )
        materialization = logical.get("materialization")
        if (
            isinstance(materialization, dict)
            and materialization.get("storage") == "logical_view"
        ):
            dependencies = {
                str(value) for value in materialization.get("derived_from", [])
            }
            invalid = sorted(dependencies & inline_projection_set)
            if invalid:
                raise RuntimeError(
                    f"SQL view {relation_name!r} cannot depend on inline projections: "
                    + ", ".join(invalid)
                )
    obligation_records = runtime_obligation_records(catalog)
    vertical_family_by_name = {
        str(family["name"]): family for family in catalog.get("vertical_family", [])
    }
    if len(vertical_family_by_name) != len(catalog.get("vertical_family", [])):
        raise RuntimeError("Vertical families must have distinct names")
    vertical_views = {
        str(family["view_relation"]): family
        for family in catalog.get("vertical_family", [])
    }
    if len(vertical_views) != len(catalog.get("vertical_family", [])):
        raise RuntimeError("Vertical families must have distinct view relations")
    for logical in catalog["relation"]:
        materialization = logical.get("materialization")
        if (
            not isinstance(materialization, dict)
            or materialization.get("view_pattern") != "sealed_vertical_projection"
        ):
            continue
        relation_name = str(logical["name"])
        if relation_name in vertical_views:
            raise RuntimeError(f"Duplicate sealed vertical view {relation_name!r}")
        family_name = str(materialization["vertical_family"])
        family = vertical_family_by_name.get(family_name)
        if family is None:
            raise RuntimeError(
                f"Sealed vertical projection {relation_name!r} references "
                f"unknown family {family_name!r}"
            )
        member_names = {str(value) for value in materialization["projection_members"]}
        family_member_names = {str(member["relation"]) for member in family["members"]}
        if not member_names <= family_member_names:
            raise RuntimeError(
                f"Sealed vertical projection {relation_name!r} references "
                "unknown family members"
            )
        projection_members = [
            {
                **member,
                "project": str(member["relation"]) in member_names,
            }
            for member in family["members"]
        ]
        vertical_views[relation_name] = {
            **family,
            "view_relation": relation_name,
            "members": projection_members,
            "projection_attributes": list(logical["attributes"]),
        }
    generation_views: dict[str, dict[str, Any]] = {}
    for stream in catalog.get("generation_stream", []):
        head_view = str(stream["head_view_relation"])
        generation_views[head_view] = {
            "pattern": "revision_generation_head",
            "revision_relation": str(stream["head_revision_relation"]),
            "time_relation": str(stream["head_time_relation"]),
            "mapping_relation": str(stream["mapping_relation"]),
            "channel_attribute": str(stream["head_channel_attribute"]),
            "revision_attribute": str(stream["revision_attribute"]),
            "generation_attribute": str(stream["generation_attribute"]),
            "time_attribute": str(stream["head_time_attribute"]),
        }
        for baseline in stream["baselines"]:
            view_relation = str(baseline["view_relation"])
            generation_views[view_relation] = {
                "pattern": "revision_generation_baseline",
                "base_relation": str(baseline["base_relation"]),
                "mapping_relation": str(stream["mapping_relation"]),
                "owner_attribute": str(baseline["owner_attribute"]),
                "revision_attribute": str(baseline["revision_attribute"]),
                "mapping_revision_attribute": str(stream["revision_attribute"]),
                "generation_attribute": str(baseline["generation_attribute"]),
                "mapping_generation_attribute": str(stream["generation_attribute"]),
            }
    expected_generation_views = len(catalog.get("generation_stream", [])) + sum(
        len(stream["baselines"]) for stream in catalog.get("generation_stream", [])
    )
    if len(generation_views) != expected_generation_views:
        raise RuntimeError("Generation streams must have distinct derived views")
    batch_projection_by_view = {
        str(projection["view_relation"]): projection
        for projection in catalog.get("batch_receipt_projection", [])
    }
    for logical in catalog["relation"]:
        materialization = logical.get("materialization")
        if not isinstance(materialization, dict):
            continue
        pattern = materialization.get("view_pattern")
        if not isinstance(pattern, str):
            continue
        if pattern == "sealed_vertical_projection":
            continue
        relation_name = str(logical["name"])
        if relation_name in generation_views:
            raise RuntimeError(f"Duplicate generated derived view {relation_name!r}")
        descriptor: dict[str, Any] = {
            "pattern": pattern,
            "source_relations": [
                str(value) for value in materialization["derived_from"]
            ],
        }
        projection_name = materialization.get("projection")
        if isinstance(projection_name, str):
            descriptor["projection"] = projection_name
        batch_projection = batch_projection_by_view.get(relation_name)
        if batch_projection is not None:
            for field in (
                "owner_attribute",
                "stage_attribute",
                "batch_key_attribute",
                "start_generation_attribute",
                "start_cursor_attribute",
                "start_processed_count_attribute",
                "page_limit_attribute",
                "next_cursor_attribute",
                "next_processed_count_attribute",
                "next_state_attribute",
                "row_count_attribute",
                "terminal_attribute",
                "committed_generation_attribute",
                "committed_at_attribute",
                "stored_relation",
                "checkpoint_relation",
            ):
                if field in batch_projection:
                    descriptor[field] = str(batch_projection[field])
        generation_views[relation_name] = descriptor
    vertical_family_by_view = {
        str(family["view_relation"]): family
        for family in catalog.get("vertical_family", [])
    }
    bootstrap_seeds: list[dict[str, Any]] = []
    for seed in catalog.get("bootstrap_seed", []):
        seed_relation = str(seed["relation"])
        family = vertical_family_by_view.get(seed_relation)
        if family is None:
            bootstrap_seeds.append(dict(seed))
            continue
        source_values = dict(zip(seed["columns"], seed["values"], strict=True))
        family_key = [str(value) for value in family["key_attributes"]]

        def projected_seed(relation_name: str, columns: list[str]) -> dict[str, Any]:
            return {
                "id": f"{seed['id']}.{relation_name}",
                "relation": relation_name,
                "columns": columns,
                "values": [source_values[column] for column in columns],
            }

        bootstrap_seeds.append(
            projected_seed(str(family["anchor_relation"]), family_key)
        )
        for member in family["members"]:
            member_columns = [
                *(str(value) for value in member["key_attributes"]),
                str(member["value_attribute"]),
            ]
            bootstrap_seeds.append(
                projected_seed(str(member["relation"]), member_columns)
            )
        bootstrap_seeds.append(projected_seed(str(family["seal_relation"]), family_key))
    relations: list[dict[str, Any]] = []
    for ordinal, logical in enumerate(catalog["relation"], 1):
        if str(logical["name"]) in inline_projection_set:
            continue
        relations.append(
            make_relation(
                logical,
                ordinal,
                vertical_view=vertical_views.get(str(logical["name"])),
                generation_view=generation_views.get(str(logical["name"])),
            )
        )
    relation_by_name = {str(relation["name"]): relation for relation in relations}
    stale_table_names = sorted(
        set(TABLE_NAMES) - set(relation_by_name) - inline_projection_set
    )
    if stale_table_names:
        raise RuntimeError(
            "Physical table-name registry contains non-catalog relations: "
            + ", ".join(stale_table_names)
        )
    physical_base_names = {
        name
        for name, relation in relation_by_name.items()
        if relation.get("kind", "table") == "table"
    }
    stale_index_relations = sorted(set(INDEXES) - physical_base_names)
    if stale_index_relations:
        raise RuntimeError(
            "Physical index registry must target current base relations only: "
            + ", ".join(stale_index_relations)
        )
    for relation_name, configured_indexes in INDEXES.items():
        relation_attributes = {
            str(column["attribute"])
            for column in relation_by_name[relation_name]["column"]
        }
        for index_name, index_attributes, _unique in configured_indexes:
            if not set(index_attributes) <= relation_attributes:
                raise RuntimeError(
                    f"Physical index {index_name!r} on {relation_name!r} "
                    "references a non-column attribute"
                )
    add_missing_foreign_key_indexes(relations)

    order = topological_order(
        relations,
        {str(value["name"]) for value in catalog.get("external_relation", [])},
    )
    seeded_relation_set = {str(seed["relation"]) for seed in bootstrap_seeds}
    logical_seeded_relation_set = {
        str(seed["relation"]) for seed in catalog.get("bootstrap_seed", [])
    }
    expected_logical_seeded_relations = {
        "analysis_stage",
        "canonical_digest_policy",
        "catalog_resource_kind",
        "channel_registry",
        "contributor_role_registry",
        "publication_generation_node",
        "publication_stage",
        "source_provider_registry",
        "search_policy",
    }
    if logical_seeded_relation_set != expected_logical_seeded_relations:
        raise RuntimeError(
            "Data bootstrap seeds must target exactly the nine closed logical "
            "registry authorities"
        )
    seeded_relations = tuple(name for name in order if name in seeded_relation_set)
    derived_relations = tuple(
        name for name in order if relation_by_name[name].get("kind") == "view"
    )
    absent_relations = tuple(
        name
        for name in order
        if name not in seeded_relation_set
        and relation_by_name[name].get("kind", "table") == "table"
    )

    def render_seed_cell(attribute: str, value: str) -> str:
        attribute_shape = NEW_ATTRIBUTE_SHAPES.get(attribute)
        sqlite_shape = (
            attribute_shape.get("sqlite") if isinstance(attribute_shape, dict) else None
        )
        mariadb_shape = (
            attribute_shape.get("mariadb")
            if isinstance(attribute_shape, dict)
            else None
        )
        sqlite_type = (
            sqlite_shape.get("type") if isinstance(sqlite_shape, dict) else None
        )
        if sqlite_type == "INTEGER":
            if not isinstance(mariadb_shape, dict):
                raise RuntimeError(
                    f"bootstrap seed integer {attribute!r} lacks a MariaDB shape"
                )
            try:
                integer = int(value)
            except ValueError as error:
                raise RuntimeError(
                    f"bootstrap seed integer {attribute!r} is not decimal"
                ) from error
            if str(integer) != value:
                raise RuntimeError(
                    f"bootstrap seed integer {attribute!r} is not canonical decimal"
                )
            value_type = (
                "uint32" if mariadb_shape.get("type") == "INT UNSIGNED" else "uint64"
            )
            return (
                "{ attribute = "
                + q(attribute)
                + ", type = "
                + q(value_type)
                + ", integer = "
                + str(integer)
                + " }"
            )
        return (
            "{ attribute = "
            + q(attribute)
            + ', type = "ascii_enum", encoding = "utf8", text = '
            + q(value)
            + " }"
        )

    header = [
        "physical_contract_version = 2",
        'name = "h2hdb-vnext-physical"',
        'logical_contract = "h2hdb-vnext-catalog"',
        'description = "Closed-world production physical realization of all source, bounded-overlay analysis, publication, and artifact relations. Resolved overlay relations are SQL views, not fictitious constrained tables."',
        "maximum_mariadb_index_bytes = 3072",
        "runtime_obligations = [",
        *(f"  {q(text)}," for _path, text, _owner in obligation_records),
        "]",
        "source_slice = [",
        *(f"  {q(name)}," for name in order),
        "]",
        "inline_projections = [",
        *(f"  {q(name)}," for name in inline_projections),
        "]",
        "",
        "[bootstrap_contract]",
        "version = 1",
        'seed_validation_lifecycle = "building_only"',
        'absence_validation_lifecycle = "building_only"',
        'epoch_owned_relation = "schema_epoch_control"',
        "seeded_relations = [" + ", ".join(q(name) for name in seeded_relations) + "]",
        "absent_relations = [" + ", ".join(q(name) for name in absent_relations) + "]",
        "derived_relations = ["
        + ", ".join(q(name) for name in derived_relations)
        + "]",
        'absence_rule = "A fresh data plane contains exactly the schema-owned canonical digest-domain, default-channel, and filesystem-provider registry seeds and no business, work, receipt, projection, artifact, or history rows; first use creates those facts only through their fenced writer protocols."',
        'epoch_rule = "schema_epoch_control is external to the data plane, created and managed exclusively by SchemaEpochCatalog, and is never a data bootstrap seed."',
        "",
        *(
            line
            for path, prose, obligation_id in obligation_records
            for line in (
                "[[runtime_obligation_binding]]",
                f"path = {q(path)}",
                f"text = {q(prose)}",
                f"semantic_obligation_id = {q(obligation_id)}",
                "",
            )
        ),
        "[canonical_digest_protocol]",
        f"policy_relation = {q(catalog['canonical_digest_contract']['policy_relation'])}",
        f"value_relation = {q(catalog['canonical_digest_contract']['value_relation'])}",
        f"digest_attribute = {q(catalog['canonical_digest_contract']['digest_attribute'])}",
        f"allocation_relation = {q(catalog['canonical_digest_contract']['allocation_relation'])}",
        f"page_relation = {q(catalog['canonical_digest_contract']['page_relation'])}",
        f"descriptor_relation = {q(catalog['canonical_digest_contract']['descriptor_relation'])}",
        f"parent_relation = {q(catalog['canonical_digest_contract']['parent_relation'])}",
        f"root_attribute = {q(catalog['canonical_digest_contract']['root_attribute'])}",
        f"byte_count_attribute = {q(catalog['canonical_digest_contract']['byte_count_attribute'])}",
        f"algorithm = {q(catalog['canonical_digest_contract']['algorithm'])}",
        f"framing = {q(catalog['canonical_digest_contract']['framing'])}",
        'enforcement = "bounded_stream_recompute_tree_validate_and_collision_compare"',
        "",
        "[canonical_value_page_protocol]",
        f"codec_version = {catalog['canonical_value_page_contract']['codec_version']}",
        f"prefix = {q(catalog['canonical_value_page_contract']['prefix'])}",
        f"maximum_page_bytes = {catalog['canonical_value_page_contract']['maximum_page_bytes']}",
        f"chunk_maximum_bytes = {catalog['canonical_value_page_contract']['chunk_maximum_bytes']}",
        f"branch_capacity = {catalog['canonical_value_page_contract']['branch_capacity']}",
        f"maximum_level = {catalog['canonical_value_page_contract']['maximum_level']}",
        f"maximum_byte_count = {catalog['canonical_value_page_contract']['maximum_byte_count']}",
        f"framing = {q(catalog['canonical_value_page_contract']['framing'])}",
        'enforcement = "raw_sha256_exact_page_compare_and_owner_tree_seal"',
        "",
        "[source_locator_protocol]",
        f"identity_relation = {q(catalog['source_locator_contract']['identity_relation'])}",
        f"gallery_relation = {q(catalog['source_locator_contract']['gallery_relation'])}",
        f"digest_attribute = {q(catalog['source_locator_contract']['digest_attribute'])}",
        f"name_attribute = {q(catalog['source_locator_contract']['name_attribute'])}",
        f"canonical_value_relation = {q(catalog['source_locator_contract']['canonical_value_relation'])}",
        f"digest_domain = {q(catalog['source_locator_contract']['digest_domain'])}",
        f"framing = {q(catalog['source_locator_contract']['framing'])}",
        'enforcement = "runtime_recompute_and_collision_compare"',
        "",
        *(
            line
            for value in catalog["byte_domain"]
            for line in (
                "[[bounded_value]]",
                f"attribute = {q(value['attribute'])}",
                f"maximum_bytes = {value['maximum_bytes']}",
                f"encoding = {q(value['encoding'])}",
                f"source = {q(value['source'])}",
                f"runtime_obligation = {q(value['runtime_obligation'])}",
                "",
            )
        ),
        *(
            line
            for obligation in catalog.get("semantic_obligation", [])
            for line in (
                "[[semantic_obligation]]",
                f"id = {q(obligation['id'])}",
                f"version = {obligation['version']}",
                f"class = {q(obligation['class'])}",
                f"lifecycle = {q(obligation['lifecycle'])}",
                f"ready_check = {q(obligation['ready_check'])}",
                f"writer_hook = {q(obligation['writer_hook'])}",
                f"writer_hook_version = {obligation['writer_hook_version']}",
                f"scope = {q(obligation['scope'])}",
                "relations = ["
                + ", ".join(q(value) for value in obligation["relations"])
                + "]",
                "covers = ["
                + ", ".join(q(value) for value in obligation["covers"])
                + "]",
                f"description = {q(obligation['description'])}",
                "",
            )
        ),
        *(
            line
            for seed in bootstrap_seeds
            for line in (
                "[[bootstrap_seed]]",
                f"id = {q(seed['id'])}",
                "version = 1",
                f"relation = {q(seed['relation'])}",
                "value = ["
                + ", ".join(
                    render_seed_cell(attribute, value)
                    for attribute, value in zip(
                        seed["columns"], seed["values"], strict=True
                    )
                )
                + "]",
                "",
            )
        ),
        "",
    ]
    return (
        "\n".join(header)
        + "\n\n".join(emit_relation(relation) for relation in relations)
        + "\n"
    )


def main() -> None:
    parser = ArgumentParser(
        description="Generate the closed-world vNext physical schema"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if physical.toml differs from deterministic generated output",
    )
    arguments = parser.parse_args()
    output = render()
    if arguments.check:
        if PHYSICAL_PATH.read_text() != output:
            raise SystemExit(
                "verification/schema/physical.toml is stale; run "
                "python verification/schema/generate_physical.py"
            )
        return
    PHYSICAL_PATH.write_text(output)


if __name__ == "__main__":
    main()
