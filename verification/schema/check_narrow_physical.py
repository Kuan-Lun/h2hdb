#!/usr/bin/env python3
"""Enforce the catalog base-table width policy and explicit exceptions.

This is deliberately separate from the BCNF checker.  BCNF constrains
functional dependencies; it does not require a relation to have only one
non-key attribute.  The product policy checked here is stricter and purely
physical: every ``catalog_*`` base table has a primary key plus at most one
atomic value column.  Views are excluded, so read models may remain wide.

The current epoch intentionally recomposes a closed set of gallery-linear BCNF
families into complete rows.  The exact exception registry admits only those
reviewed shapes and rejects any additional or changed wide relation, while the
closed semantic-role registry rejects a hidden value moved into the primary key
or an undeclared base table.
"""

from __future__ import annotations

import argparse
import re
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PHYSICAL_PATH = ROOT / "verification" / "schema" / "physical.toml"
MAXIMUM_NON_KEY_COLUMNS = 1


class NarrowPhysicalError(RuntimeError):
    """The physical catalog no longer matches the narrow-layout policy."""


@dataclass(frozen=True)
class ColumnShape:
    attribute: str
    sqlite_type: str
    mariadb_type: str


@dataclass(frozen=True)
class RelationShape:
    table: str
    primary_key: tuple[str, ...]
    columns: tuple[ColumnShape, ...]

    @property
    def attributes(self) -> tuple[str, ...]:
        return tuple(column.attribute for column in self.columns)

    @property
    def non_key_columns(self) -> tuple[str, ...]:
        key = frozenset(self.primary_key)
        return tuple(attribute for attribute in self.attributes if attribute not in key)


@dataclass(frozen=True)
class NarrowLayoutDeclaration:
    """Human-auditable semantic roles for one already-narrow base table.

    ``semantic_key`` prevents making a table look narrow by moving ordinary
    values into its physical primary key.  ``semantic_value`` is intentionally
    zero or one physical attribute.  The representation fields make packed
    tuple/JSON and EAV designs explicit and rejectable instead of silently
    treating them as a single scalar column.
    """

    semantic_key: tuple[str, ...]
    semantic_value: tuple[str, ...]
    value_representation: str = "atomic_column"
    storage_model: str = "direct_relation"


@dataclass(frozen=True)
class NarrowPhysicalReport:
    relations: tuple[RelationShape, ...]
    violations: tuple[RelationShape, ...]
    problems: tuple[str, ...]

    @property
    def is_policy_clean(self) -> bool:
        return not self.problems

    @property
    def is_fully_narrow(self) -> bool:
        return not self.violations

    def render(self) -> str:
        narrow_count = len(self.relations) - len(self.violations)
        lines = [
            "Catalog base-table width report: "
            f"relations={len(self.relations)}, narrow={narrow_count}, "
            f"wide={len(self.violations)}",
            "Approved wide relations (complete):",
        ]
        if not self.violations:
            lines.append("  (none)")
        for relation in self.violations:
            lines.append(
                f"  - {relation.table}: primary_key={relation.primary_key!r}; "
                f"non_key_columns={relation.non_key_columns!r} "
                f"({len(relation.non_key_columns)} > "
                f"{MAXIMUM_NON_KEY_COLUMNS})"
            )
        lines.append("Policy drift:")
        if not self.problems:
            lines.append("  (none)")
        else:
            lines.extend(f"  - {problem}" for problem in self.problems)
        return "\n".join(lines)


# Exact approved-wide registry. Adding a column to the reviewed BCNF exception
# must fail just like introducing a second wide relation. Removing or splitting
# the exception also fails until this registry and the resulting narrow layout
# declarations are updated together.
APPROVED_WIDE_BCNF_RELATIONS: Mapping[str, tuple[str, ...]] = {
    "catalog_analysis_content_owner_candidate_shadows": (
        "content_sha256",
        "prefer_not_already_uploaded",
        "title_scalar_count",
        "download_time",
    ),
    "catalog_artifact_blobs": (
        "size_bytes",
        "artifact_locator_sha256",
    ),
    "catalog_artifact_semantic_inputs": (
        "source_manifest_component_sha256",
        "member_plan_component_sha256",
        "effective_content_component_sha256",
        "selected_component_sha256",
        "owner_component_sha256",
        "policy_component_sha256",
    ),
    "catalog_artifacts": (
        "artifact_sha256",
        "artifact_semantics_sha256",
    ),
    "catalog_gallery_identities": (
        "gallery_key",
        "scope_key",
        "locator_sha256",
    ),
    "catalog_gallery_manifests": (
        "manifest_sha256",
        "computed_at",
    ),
    "catalog_gallery_observation_directories": (
        "directory_entry_count",
        "directory_observation_sha256",
    ),
    "catalog_gallery_observation_metadata_locals": (
        "download_time",
        "modified_time",
    ),
    "catalog_gallery_observation_scans": (
        "scan_observation_sha256",
        "scan_observation_version",
        "source_file_count",
    ),
    "catalog_gallery_observation_stat": (
        "file_count",
        "byte_count",
    ),
    "catalog_prepared_artifacts": (
        "artifact_sha256",
        "storage_codec_version",
        "storage_generation",
        "protection_token",
        "state",
    ),
    "catalog_publication_storage": (
        "gallery_id",
        "summary_sha256",
        "language_sha256",
        "modified_at",
        "source_title_sha256",
    ),
}


# Closed-world semantic-role declarations for all base tables that already meet
# the width limit.  New tables must be consciously classified here.  When a
# approved-wide table is split, remove its exception and declare each narrow
# result, including its true semantic key and its sole atomic value (if any).
_EXPLICIT_NARROW_LAYOUT_DECLARATIONS: Mapping[str, NarrowLayoutDeclaration] = {
    "catalog_canonical_digest_policies": NarrowLayoutDeclaration(
        semantic_key=("digest_domain",),
        semantic_value=(),
    ),
    "catalog_canonical_value_identities": NarrowLayoutDeclaration(
        semantic_key=("value_sha256",),
        semantic_value=("root_page_sha256",),
    ),
    "catalog_channel_registry": NarrowLayoutDeclaration(
        semantic_key=("channel",),
        semantic_value=(),
    ),
    "catalog_source_provider_registry": NarrowLayoutDeclaration(
        semantic_key=("source_provider",),
        semantic_value=(),
    ),
    "catalog_source_build_anchors": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_source_build_scope_keys": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("scope_key",),
    ),
    "catalog_source_build_manifest_policy_ids": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("manifest_policy_id",),
    ),
    "catalog_source_build_states": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("state",),
    ),
    "catalog_source_build_created_ats": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("created_at",),
    ),
    "catalog_source_build_descriptor_seals": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_source_build_sealed_ats": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("sealed_at",),
    ),
    "catalog_source_build_channel": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("channel",),
    ),
    "catalog_source_build_discovery_anchors": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_source_build_discovery_scan_attempts": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("scan_attempt",),
    ),
    "catalog_source_build_discovery_gallery_counts": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("gallery_count",),
    ),
    "catalog_source_build_discovery_tree_observation_sha256s": (
        NarrowLayoutDeclaration(
            semantic_key=("build_id",),
            semantic_value=("tree_observation_sha256",),
        )
    ),
    "catalog_source_build_discovery_completed_ats": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("completed_at",),
    ),
    "catalog_source_build_discovery_seals": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_source_build_expected_gallery": NarrowLayoutDeclaration(
        semantic_key=("build_id", "position"),
        semantic_value=("gallery_id",),
    ),
    "catalog_source_locator_identity": NarrowLayoutDeclaration(
        semantic_key=("locator_sha256",),
        semantic_value=("source_gallery_name",),
    ),
    "catalog_gallery_observation_allocations": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("allocated_at",),
    ),
    "catalog_gallery_observation_pages": NarrowLayoutDeclaration(
        semantic_key=("page_sha256",),
        semantic_value=("page_bytes",),
    ),
    "catalog_canonical_value_page_parents": NarrowLayoutDeclaration(
        semantic_key=("parent_sha256", "position"),
        semantic_value=("child_sha256",),
    ),
    "catalog_gallery_observation_allocation_pages": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id", "page_sha256"),
        semantic_value=(),
    ),
    "catalog_gallery_observation_page_children": NarrowLayoutDeclaration(
        semantic_key=("parent_sha256", "position"),
        semantic_value=("child_sha256",),
    ),
    "catalog_gallery_observation_tree_roots": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id", "root_page_sha256"),
        semantic_value=(),
    ),
    "catalog_gallery_observations": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("observation_identity_sha256",),
    ),
    "catalog_gallery_upload_times": NarrowLayoutDeclaration(
        semantic_key=("gid",),
        semantic_value=("upload_time",),
    ),
    "catalog_source_gallery_name_gids": NarrowLayoutDeclaration(
        semantic_key=("source_gallery_name",),
        semantic_value=("gid",),
    ),
    "catalog_gallery_source_name_accesses": NarrowLayoutDeclaration(
        semantic_key=("gallery_id",),
        semantic_value=("source_gallery_name",),
    ),
    "catalog_gallery_observation_discovery_fingerprints": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id"),
            semantic_value=("metadata_fingerprint",),
        )
    ),
    "catalog_gallery_observation_metadata_digests": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("metadata_sha256",),
    ),
    "catalog_gallery_observation_raw_content": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("raw_content_sha256",),
    ),
    "catalog_gallery_observation_page_counts": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("page_count",),
    ),
    "catalog_source_build_galleries": NarrowLayoutDeclaration(
        semantic_key=("build_id", "gallery_id"),
        semantic_value=("observation_id",),
    ),
    "catalog_build_manifest_anchors": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_build_manifest_manifest_sha256s": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("manifest_sha256",),
    ),
    "catalog_build_manifest_file_counts": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("file_count",),
    ),
    "catalog_build_manifest_byte_counts": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("byte_count",),
    ),
    "catalog_build_manifest_seals": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=(),
    ),
    "catalog_content_blobs": NarrowLayoutDeclaration(
        semantic_key=("file_sha256",),
        semantic_value=("size_bytes",),
    ),
    "catalog_gallery_observation_file_filesystem_anchors": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=(),
        )
    ),
    "catalog_gallery_observation_file_filesystem_devices": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=("device",),
        )
    ),
    "catalog_gallery_observation_file_filesystem_inodes": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=("inode",),
        )
    ),
    "catalog_gallery_observation_file_filesystem_modified_nses": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=("modified_ns",),
        )
    ),
    "catalog_gallery_observation_file_filesystem_changed_nses": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=("changed_ns",),
        )
    ),
    "catalog_gallery_observation_file_filesystem_seals": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_key"),
            semantic_value=(),
        )
    ),
    "catalog_gallery_observation_tags": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id", "position"),
        semantic_value=("tag_id",),
    ),
    "catalog_analysis_baselines": NarrowLayoutDeclaration(
        semantic_key=("analysis_id",),
        semantic_value=("base_analysis_id",),
    ),
    "catalog_analysis_state_ancestry": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "ancestor_depth"),
        semantic_value=("ancestor_analysis_id",),
    ),
    "catalog_analysis_snapshot_manifest": NarrowLayoutDeclaration(
        semantic_key=("analysis_id",),
        semantic_value=("snapshot_manifest_sha256",),
    ),
    "catalog_source_snapshot_manifest_identity_anchors": NarrowLayoutDeclaration(
        semantic_key=("snapshot_manifest_sha256",),
        semantic_value=(),
    ),
    "catalog_source_snapshot_manifest_identity_gallery_counts": (
        NarrowLayoutDeclaration(
            semantic_key=("snapshot_manifest_sha256",),
            semantic_value=("gallery_count",),
        )
    ),
    "catalog_source_snapshot_manifest_identity_file_counts": (
        NarrowLayoutDeclaration(
            semantic_key=("snapshot_manifest_sha256",),
            semantic_value=("file_count",),
        )
    ),
    "catalog_source_snapshot_manifest_identity_byte_counts": (
        NarrowLayoutDeclaration(
            semantic_key=("snapshot_manifest_sha256",),
            semantic_value=("byte_count",),
        )
    ),
    "catalog_source_snapshot_manifest_identity_seals": NarrowLayoutDeclaration(
        semantic_key=("snapshot_manifest_sha256",),
        semantic_value=(),
    ),
    "catalog_source_revision_provenance": NarrowLayoutDeclaration(
        semantic_key=("source_revision",),
        semantic_value=("analysis_id",),
    ),
    "catalog_gallery_observation_artists": NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id", "artist_tag_id"),
        semantic_value=(),
    ),
    "catalog_gallery_observation_file_hash_occurrences": (
        NarrowLayoutDeclaration(
            semantic_key=("gallery_id", "observation_id", "file_sha256"),
            semantic_value=("occurrence_count",),
        )
    ),
    "catalog_analysis_exclusion_delta_changes": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "file_sha256"),
        semantic_value=(),
    ),
    "catalog_analysis_changed_galleries": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id"),
        semantic_value=("change_kind",),
    ),
    "catalog_analysis_changed_file_hashes": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "file_sha256"),
        semantic_value=(),
    ),
    "catalog_analysis_impacted_galleries": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id"),
        semantic_value=(),
    ),
    "catalog_a_impacted_content_provenance": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id", "content_sha256"),
        semantic_value=(),
    ),
    "catalog_analysis_file_hash_decision_tombstone": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "file_sha256"),
        semantic_value=(),
    ),
    "catalog_analysis_content_owner_candidate_tombstones": (
        NarrowLayoutDeclaration(
            semantic_key=("analysis_id", "gallery_id"),
            semantic_value=(),
        )
    ),
    "catalog_analysis_content_owner_tombstones": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "content_sha256"),
        semantic_value=(),
    ),
    "catalog_analysis_content_owner_shadows": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "content_sha256"),
        semantic_value=("owner_gallery_id",),
    ),
    "catalog_analysis_impacted_content": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "content_sha256"),
        semantic_value=("witness_gallery_id",),
    ),
    "catalog_analysis_impacted_gid_storage": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gid"),
        semantic_value=(),
    ),
    "catalog_a_impacted_gid_provenance_storage": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id"),
        semantic_value=(),
    ),
    "catalog_analysis_gid_candidate_tombstones": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id"),
        semantic_value=(),
    ),
    "catalog_analysis_gid_candidate_shadows": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gallery_id"),
        semantic_value=(),
    ),
    "catalog_analysis_gid_winner_selections": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "winner_gallery_id"),
        semantic_value=(),
    ),
    "catalog_analysis_gid_winner_tombstones": NarrowLayoutDeclaration(
        semantic_key=("analysis_id", "gid"),
        semantic_value=(),
    ),
    "catalog_contributor_role_registry": NarrowLayoutDeclaration(
        semantic_key=("role",),
        semantic_value=(),
    ),
    "catalog_publication_candidate_projection_seals": NarrowLayoutDeclaration(
        semantic_key=("candidate_id",),
        semantic_value=(),
    ),
    "catalog_publication_identities": NarrowLayoutDeclaration(
        semantic_key=("publication_key",),
        semantic_value=("gid",),
    ),
    "catalog_publication_selection_storage": NarrowLayoutDeclaration(
        semantic_key=("selection_occurrence_sha256",),
        semantic_value=("gallery_id",),
    ),
    "catalog_publication_selection_occurrence_identities": (
        NarrowLayoutDeclaration(
            semantic_key=("candidate_id", "publication_key"),
            semantic_value=("selection_occurrence_sha256",),
        )
    ),
    "catalog_publication_occurrence_identities": NarrowLayoutDeclaration(
        semantic_key=("revision", "publication_key"),
        semantic_value=("catalog_occurrence_sha256",),
    ),
    "catalog_artifact_producer_fingerprint_anchors": NarrowLayoutDeclaration(
        semantic_key=("producer_fingerprint_sha256",),
        semantic_value=(),
    ),
    "catalog_artifact_producer_fingerprint_algorithm_versions": (
        NarrowLayoutDeclaration(
            semantic_key=("producer_fingerprint_sha256",),
            semantic_value=("artifact_algorithm_version",),
        )
    ),
    "catalog_artifact_producer_fingerprint_equivalence_classes": (
        NarrowLayoutDeclaration(
            semantic_key=("producer_fingerprint_sha256",),
            semantic_value=("producer_equivalence_class",),
        )
    ),
    "catalog_artifact_producer_fingerprint_identities": NarrowLayoutDeclaration(
        semantic_key=(
            "writer_id",
            "python_abi",
            "pillow_build",
            "libjpeg_build",
            "zlib_build",
        ),
        semantic_value=("producer_fingerprint_sha256",),
    ),
    "catalog_artifact_producer_fingerprint_seals": NarrowLayoutDeclaration(
        semantic_key=("producer_fingerprint_sha256",),
        semantic_value=(),
    ),
    "catalog_artifact_policies": NarrowLayoutDeclaration(
        semantic_key=("artifact_policy_id",),
        semantic_value=("policy_component_sha256",),
    ),
    "catalog_candidate_artifact_inputs": NarrowLayoutDeclaration(
        semantic_key=("candidate_id", "publication_key"),
        semantic_value=("artifact_semantics_sha256",),
    ),
    "catalog_artifact_operations": NarrowLayoutDeclaration(
        semantic_key=("candidate_id", "publication_key"),
        semantic_value=("operation",),
    ),
    "catalog_display_title_choices": NarrowLayoutDeclaration(
        semantic_key=(
            "display_title_policy_id",
            "source_title_sha256",
            "source_gallery_name",
        ),
        semantic_value=("title_sha256",),
    ),
    "catalog_title_sorts": NarrowLayoutDeclaration(
        semantic_key=("title_sort_policy_id", "title_sha256"),
        semantic_value=("sort_title_sha256",),
    ),
    "catalog_publication_order": NarrowLayoutDeclaration(
        semantic_key=("revision", "position"),
        semantic_value=("publication_key",),
    ),
    "catalog_publication_contents": NarrowLayoutDeclaration(
        semantic_key=("revision", "publication_key"),
        semantic_value=("content_sha256",),
    ),
    "catalog_subjects": NarrowLayoutDeclaration(
        semantic_key=("revision", "publication_key", "position"),
        semantic_value=("tag_id",),
    ),
    "catalog_source_build_base_publication_commits": NarrowLayoutDeclaration(
        semantic_key=("build_id",),
        semantic_value=("base_receipt_id",),
    ),
    "catalog_publication_candidate_base_publication_commits": (
        NarrowLayoutDeclaration(
            semantic_key=("candidate_id",),
            semantic_value=("base_receipt_id",),
        )
    ),
    "catalog_publication_generation_nodes": NarrowLayoutDeclaration(
        semantic_key=("generation",),
        semantic_value=(),
    ),
    "catalog_publication_generation_successors": NarrowLayoutDeclaration(
        semantic_key=("successor_generation",),
        semantic_value=("predecessor_generation",),
    ),
    "catalog_publication_commit_finalizations": NarrowLayoutDeclaration(
        semantic_key=("receipt_id",),
        semantic_value=(),
    ),
    "catalog_publication_commit_head_receipts": NarrowLayoutDeclaration(
        semantic_key=("channel",),
        semantic_value=("receipt_id",),
    ),
}


def _required_string(value: object, *, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise NarrowPhysicalError(f"{context} must be a nonempty string")
    return value


def _string_sequence(value: object, *, context: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise NarrowPhysicalError(f"{context} must be an array")
    result = tuple(
        _required_string(item, context=f"{context}[{index}]")
        for index, item in enumerate(value)
    )
    if len(result) != len(set(result)):
        raise NarrowPhysicalError(f"{context} contains duplicate attributes")
    return result


def catalog_base_relations(document: Mapping[str, Any]) -> tuple[RelationShape, ...]:
    """Load only physical ``catalog_*`` base tables; deliberately skip views."""

    raw_relations = document.get("relation")
    if not isinstance(raw_relations, list):
        raise NarrowPhysicalError("physical manifest relation must be an array")
    result: list[RelationShape] = []
    seen_tables: set[str] = set()
    for index, raw_relation in enumerate(raw_relations):
        context = f"relation[{index}]"
        if not isinstance(raw_relation, dict):
            raise NarrowPhysicalError(f"{context} must be a table")
        table = _required_string(raw_relation.get("table"), context=f"{context}.table")
        if not table.startswith("catalog_") or raw_relation.get("kind") == "view":
            continue
        if table in seen_tables:
            raise NarrowPhysicalError(f"duplicate physical table {table!r}")
        seen_tables.add(table)

        primary_key = _string_sequence(
            raw_relation.get("primary_key"), context=f"{context}.primary_key"
        )
        if not primary_key:
            raise NarrowPhysicalError(f"{context}.primary_key must not be empty")
        raw_columns = raw_relation.get("column")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise NarrowPhysicalError(f"{context}.column must be a nonempty array")
        columns: list[ColumnShape] = []
        for column_index, raw_column in enumerate(raw_columns):
            column_context = f"{context}.column[{column_index}]"
            if not isinstance(raw_column, dict):
                raise NarrowPhysicalError(f"{column_context} must be a table")
            sqlite = raw_column.get("sqlite")
            mariadb = raw_column.get("mariadb")
            if not isinstance(sqlite, dict) or not isinstance(mariadb, dict):
                raise NarrowPhysicalError(
                    f"{column_context} must declare sqlite and mariadb shapes"
                )
            columns.append(
                ColumnShape(
                    attribute=_required_string(
                        raw_column.get("attribute"),
                        context=f"{column_context}.attribute",
                    ),
                    sqlite_type=_required_string(
                        sqlite.get("type"), context=f"{column_context}.sqlite.type"
                    ),
                    mariadb_type=_required_string(
                        mariadb.get("type"), context=f"{column_context}.mariadb.type"
                    ),
                )
            )
        relation = RelationShape(table, primary_key, tuple(columns))
        if len(relation.attributes) != len(set(relation.attributes)):
            raise NarrowPhysicalError(f"{context} contains duplicate column attributes")
        missing_key_columns = set(primary_key) - set(relation.attributes)
        if missing_key_columns:
            raise NarrowPhysicalError(
                f"{context}.primary_key names missing columns "
                f"{sorted(missing_key_columns)!r}"
            )
        result.append(relation)
    return tuple(sorted(result, key=lambda relation: relation.table))


def load_catalog_base_relations(
    path: Path = PHYSICAL_PATH,
) -> tuple[RelationShape, ...]:
    with path.open("rb") as stream:
        return catalog_base_relations(tomllib.load(stream))


def vertical_family_layout_declarations(
    document: Mapping[str, Any],
) -> Mapping[str, NarrowLayoutDeclaration]:
    """Derive narrow semantic roles from generic sealed-family metadata.

    The family contract, rather than a table-name convention, identifies the
    true semantic key and the one atomic member value.  The ordinary layout
    checks below still require that semantic key to equal the physical primary
    key, so moving the value into the PK cannot make a relation appear narrow.
    """

    raw_relations = document.get("relation")
    if not isinstance(raw_relations, list):
        raise NarrowPhysicalError("physical manifest relation must be an array")
    relation_by_name = {
        _required_string(relation.get("name"), context="relation.name"): relation
        for relation in raw_relations
        if isinstance(relation, dict)
    }
    result: dict[str, NarrowLayoutDeclaration] = {}

    def register(
        relation_name: str,
        semantic_key: tuple[str, ...],
        semantic_value: tuple[str, ...],
    ) -> None:
        relation = relation_by_name.get(relation_name)
        if relation is None or relation.get("kind") == "view":
            raise NarrowPhysicalError(
                f"vertical family references missing/non-base {relation_name!r}"
            )
        table = _required_string(
            relation.get("table"), context=f"relation {relation_name!r}.table"
        )
        declaration = NarrowLayoutDeclaration(
            semantic_key=semantic_key,
            semantic_value=semantic_value,
        )
        previous = result.setdefault(table, declaration)
        if previous != declaration:
            raise NarrowPhysicalError(
                f"vertical families disagree on semantic roles for {table!r}"
            )

    for relation in raw_relations:
        if not isinstance(relation, dict) or relation.get("kind") != "view":
            continue
        view = relation.get("view")
        if (
            not isinstance(view, dict)
            or view.get("pattern") != "sealed_vertical_family"
        ):
            continue
        family_key = _string_sequence(
            view.get("key_attributes"), context="vertical family key_attributes"
        )
        register(
            _required_string(
                view.get("anchor_relation"), context="vertical family anchor_relation"
            ),
            family_key,
            (),
        )
        register(
            _required_string(
                view.get("seal_relation"), context="vertical family seal_relation"
            ),
            family_key,
            (),
        )
        members = view.get("members")
        if not isinstance(members, list) or not members:
            raise NarrowPhysicalError("vertical family members must be nonempty")
        for member in members:
            if not isinstance(member, dict):
                raise NarrowPhysicalError("vertical family member must be a table")
            key = _string_sequence(
                member.get("key_attributes"),
                context="vertical family member key_attributes",
            )
            value = _required_string(
                member.get("value_attribute"),
                context="vertical family member value_attribute",
            )
            if value in key:
                raise NarrowPhysicalError(
                    "vertical family member value must not be hidden in its key"
                )
            register(
                _required_string(
                    member.get("relation"), context="vertical family member relation"
                ),
                key,
                (value,),
            )
    return result


with PHYSICAL_PATH.open("rb") as _physical_stream:
    _VERTICAL_NARROW_LAYOUT_DECLARATIONS = vertical_family_layout_declarations(
        tomllib.load(_physical_stream)
    )

_overlapping_narrow_declarations = set(_EXPLICIT_NARROW_LAYOUT_DECLARATIONS) & set(
    _VERTICAL_NARROW_LAYOUT_DECLARATIONS
)
for _table in _overlapping_narrow_declarations:
    if (
        _EXPLICIT_NARROW_LAYOUT_DECLARATIONS[_table]
        != _VERTICAL_NARROW_LAYOUT_DECLARATIONS[_table]
    ):
        raise NarrowPhysicalError(
            f"explicit and vertical-family semantic roles disagree for {_table!r}"
        )

NARROW_LAYOUT_DECLARATIONS: Mapping[str, NarrowLayoutDeclaration] = {
    **_EXPLICIT_NARROW_LAYOUT_DECLARATIONS,
    **_VERTICAL_NARROW_LAYOUT_DECLARATIONS,
}


def width_violations(
    relations: Iterable[RelationShape],
) -> tuple[RelationShape, ...]:
    return tuple(
        sorted(
            (
                relation
                for relation in relations
                if len(relation.non_key_columns) > MAXIMUM_NON_KEY_COLUMNS
            ),
            key=lambda relation: relation.table,
        )
    )


_JSON_TYPE = re.compile(r"(?:^|\W)JSON(?:\W|$)", re.IGNORECASE)
_EAV_ATTRIBUTE_NAMES = frozenset(
    {
        "attribute",
        "attribute_name",
        "field",
        "field_name",
        "property",
        "property_name",
    }
)
_EAV_VALUE_NAMES = frozenset(
    {"attribute_value", "field_value", "property_value", "value"}
)


def _layout_problems(
    relation: RelationShape, declaration: NarrowLayoutDeclaration
) -> tuple[str, ...]:
    problems: list[str] = []
    if len(declaration.semantic_key) != len(set(declaration.semantic_key)):
        problems.append(f"{relation.table}: semantic_key contains duplicates")
    if len(declaration.semantic_value) != len(set(declaration.semantic_value)):
        problems.append(f"{relation.table}: semantic_value contains duplicates")
    if len(declaration.semantic_value) > MAXIMUM_NON_KEY_COLUMNS:
        problems.append(
            f"{relation.table}: semantic_value declares "
            f"{len(declaration.semantic_value)} attributes; packed values are forbidden"
        )
    overlap = set(declaration.semantic_key) & set(declaration.semantic_value)
    if overlap:
        problems.append(
            f"{relation.table}: attributes cannot be both semantic key and value: "
            f"{sorted(overlap)!r}"
        )
    declared_attributes = declaration.semantic_key + declaration.semantic_value
    if set(declared_attributes) != set(relation.attributes):
        problems.append(
            f"{relation.table}: semantic roles do not cover exactly the physical "
            f"columns; declared={declared_attributes!r}, "
            f"physical={relation.attributes!r}"
        )
    if relation.primary_key != declaration.semantic_key:
        problems.append(
            f"{relation.table}: physical primary_key={relation.primary_key!r} "
            f"does not equal declared semantic_key={declaration.semantic_key!r}; "
            "do not move ordinary values into the primary key"
        )
    if relation.non_key_columns != declaration.semantic_value:
        problems.append(
            f"{relation.table}: physical non-key columns="
            f"{relation.non_key_columns!r} do not equal declared semantic_value="
            f"{declaration.semantic_value!r}"
        )
    if declaration.value_representation != "atomic_column":
        problems.append(
            f"{relation.table}: value_representation="
            f"{declaration.value_representation!r}; tuple/record packing is forbidden"
        )
    if declaration.storage_model != "direct_relation":
        problems.append(
            f"{relation.table}: storage_model={declaration.storage_model!r}; "
            "EAV storage is forbidden"
        )
    for column in relation.columns:
        if _JSON_TYPE.search(column.sqlite_type) or _JSON_TYPE.search(
            column.mariadb_type
        ):
            problems.append(
                f"{relation.table}.{column.attribute}: JSON storage is forbidden"
            )
    attributes = set(relation.attributes)
    if attributes & _EAV_ATTRIBUTE_NAMES and attributes & _EAV_VALUE_NAMES:
        problems.append(
            f"{relation.table}: generic attribute/value columns form an EAV layout"
        )
    return tuple(problems)


def evaluate_policy(
    relations: Sequence[RelationShape],
    *,
    approved_wide_bcnf: Mapping[str, tuple[str, ...]] = APPROVED_WIDE_BCNF_RELATIONS,
    declarations: Mapping[str, NarrowLayoutDeclaration] = NARROW_LAYOUT_DECLARATIONS,
) -> NarrowPhysicalReport:
    """Compare a catalog shape against the exact width-policy registry."""

    ordered_relations = tuple(sorted(relations, key=lambda relation: relation.table))
    relation_by_table: dict[str, RelationShape] = {}
    problems: list[str] = []
    for relation in ordered_relations:
        if relation.table in relation_by_table:
            problems.append(f"duplicate relation shape for {relation.table}")
        relation_by_table[relation.table] = relation

    approved_wide_tables = set(approved_wide_bcnf)
    declared_tables = set(declarations)
    overlap = approved_wide_tables & declared_tables
    if overlap:
        problems.append(
            "tables cannot be both approved-wide BCNF and narrow-declared: "
            f"{sorted(overlap)!r}"
        )

    actual_tables = set(relation_by_table)
    policy_tables = approved_wide_tables | declared_tables
    for table in sorted(actual_tables - policy_tables):
        problems.append(f"{table}: missing closed-world narrow-layout metadata")
    for table in sorted(policy_tables - actual_tables):
        problems.append(f"{table}: stale narrow-layout metadata for missing base table")

    violations = width_violations(ordered_relations)
    violation_by_table = {relation.table: relation for relation in violations}
    for table in sorted(approved_wide_tables):
        approved_relation = relation_by_table.get(table)
        if approved_relation is None:
            continue
        violation = violation_by_table.get(table)
        if violation is None:
            problems.append(
                f"{table}: approved-wide exception is no longer wide; remove it "
                "and add an exact narrow layout declaration"
            )
            continue
        expected = tuple(approved_wide_bcnf[table])
        if violation.non_key_columns != expected:
            problems.append(
                f"{table}: approved-wide non-key columns changed; "
                f"expected={expected!r}, actual={violation.non_key_columns!r}"
            )
    for table in sorted(set(violation_by_table) - approved_wide_tables):
        problems.append(
            f"{table}: new width violation with non-key columns "
            f"{violation_by_table[table].non_key_columns!r}"
        )

    for table in sorted(declared_tables & actual_tables):
        problems.extend(_layout_problems(relation_by_table[table], declarations[table]))

    return NarrowPhysicalReport(
        relations=ordered_relations,
        violations=violations,
        problems=tuple(problems),
    )


def check_current_policy() -> NarrowPhysicalReport:
    report = evaluate_policy(load_catalog_base_relations())
    if not report.is_policy_clean:
        raise NarrowPhysicalError(report.render())
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check catalog base tables for primary-key-plus-one-value layout"
    )
    parser.add_argument(
        "--quiet", action="store_true", help="print only when the check fails"
    )
    arguments = parser.parse_args()
    try:
        report = check_current_policy()
    except NarrowPhysicalError as error:
        raise SystemExit(str(error)) from error
    if not arguments.quiet:
        print(report.render())


if __name__ == "__main__":
    main()
