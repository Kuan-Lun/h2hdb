from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import seed_analysis_run
from vnext_canonical_value_fixtures import (
    seed_canonical_allocation,
    seed_canonical_page,
    seed_canonical_value,
)
from vnext_catalog_identity_fixtures import (
    seed_file_name_identity,
    seed_gallery_identity,
)
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_artifact_producer_fingerprint,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_gallery_page_fixtures import (
    seed_gallery_page_bounds,
    seed_gallery_page_descriptor,
)
from vnext_manifest_fixtures import (
    seed_build_manifest,
    seed_gallery_manifest,
    seed_snapshot_manifest,
    seed_source_build,
)
from vnext_publication_fixtures import (
    seed_publication_candidate,
    seed_publication_commit,
)

import h2hdb.vnext_cleanup_repository as cleanup_module
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sql_connector import DatabaseDuplicateKeyError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CatalogPublicationMaintenanceState,
    CleanupBatchCommand,
    CleanupBatchResult,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork


def _database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def _exclusive(connector: SQLiteConnector, *, token: bytes = b"x" * 16) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=token,
        ),
    ):
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    kind: CleanupTargetKind,
    shard: int,
    *,
    max_rows: int = 1,
    now: int = 2,
) -> CleanupCycle:
    with connector.transaction():
        return VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            target_kind=kind,
            shard_no=shard,
            cycle_cutoff_at=100,
            max_rows_per_transaction=max_rows,
            now=now,
        )


def _advance(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    generation: int,
    batch_key: bytes,
    *,
    now: int,
) -> CleanupBatchResult:
    with connector.transaction():
        return VNextCleanupRepository.advance(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cycle=cycle,
            command=CleanupBatchCommand(batch_key, generation),
            now=now,
        )


def _drain(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int = 3,
) -> list[CleanupBatchResult]:
    generation = 1
    results: list[CleanupBatchResult] = []
    for attempt in range(512):
        result = _advance(
            connector,
            gate,
            cycle,
            generation,
            attempt.to_bytes(32, "big"),
            now=now + attempt,
        )
        results.append(result)
        if result.cycle_complete:
            return results
        assert result.generation is not None
        generation = result.generation
    raise AssertionError("cleanup cycle did not terminate within its fixed phases")


def _fixture_rows(
    connector: SQLiteConnector,
    statements: list[tuple[str, tuple[object, ...]]],
) -> None:
    """Install isolated cleanup fixtures without fabricating all parent planes."""

    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for sql, parameters in statements:
            connector.execute(sql, parameters)
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


_GALLERY_PAGE_DELETE_GROUPS = {
    "catalog_gallery_observation_page_children": (
        "catalog_gallery_observation_page_children",
    ),
    "catalog_gallery_observation_page_key_bounds_seals": (
        "catalog_gallery_observation_page_key_bounds_seals",
        "catalog_gallery_observation_page_key_bounds_first_keys",
        "catalog_gallery_observation_page_key_bounds_last_keys",
        "catalog_gallery_observation_page_key_bounds_anchors",
    ),
    "catalog_gallery_observation_page_descriptor_seals": (
        "catalog_gallery_observation_page_descriptor_seals",
        "catalog_gallery_observation_page_descriptor_components",
        "catalog_gallery_observation_page_descriptor_levels",
        "catalog_gallery_observation_page_descriptor_subtree_item_counts",
        "catalog_gallery_observation_pages",
    ),
    "catalog_gallery_observation_page_descriptor_anchors": (
        "catalog_gallery_observation_page_descriptor_anchors",
    ),
}

_ANALYSIS_OVERLAY_TABLES = (
    "catalog_a_file_decision_shadow_seals",
    "catalog_analysis_content_owner_candidate_shadows",
    "catalog_analysis_content_owner_shadows",
    "catalog_analysis_impacted_content",
    "catalog_analysis_impacted_gid_storage",
    "catalog_analysis_file_hash_decision_tombstone",
    "catalog_analysis_content_owner_candidate_tombstones",
    "catalog_analysis_content_owner_tombstones",
    "catalog_analysis_gid_candidate_shadows",
    "catalog_analysis_gid_candidate_tombstones",
    "catalog_analysis_gid_winner_selections",
    "catalog_analysis_gid_winner_tombstones",
)

_ANALYSIS_FILE_HASH_VALUE_TABLES = (
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
)

_ANALYSIS_IMPACT_PROVENANCE_TABLES = (
    "catalog_a_impacted_content_provenance",
    "catalog_a_impacted_gid_provenance_storage",
)

_ANALYSIS_FILE_HASH_ANCHOR_TABLES = ("catalog_a_file_decision_shadow_anchors",)

_ANALYSIS_OVERLAY_PHASE_TABLES = (
    _ANALYSIS_OVERLAY_TABLES,
    _ANALYSIS_FILE_HASH_VALUE_TABLES,
    _ANALYSIS_IMPACT_PROVENANCE_TABLES,
    _ANALYSIS_FILE_HASH_ANCHOR_TABLES,
)

_ALL_ANALYSIS_OVERLAY_TABLES = tuple(
    table for phase_tables in _ANALYSIS_OVERLAY_PHASE_TABLES for table in phase_tables
)

_CATALOG_PUBLICATION_PAYLOAD_TABLES = (
    "catalog_publication_storage",
    "catalog_contributor_seals",
    "catalog_contributor_identities",
    "catalog_contributor_name_sha256s",
    "catalog_contributor_roles",
    "catalog_contributor_anchors",
    "catalog_publication_order",
    "catalog_publication_contents",
    "catalog_subjects",
    "catalog_artifacts",
    "catalog_publication_occurrence_identities",
)


def _finalize_publication_receipt(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    cursor: bytes,
    processed_count: int,
    finalized_at: int,
) -> None:
    batch_key = (receipt_id, 1)
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_anchors "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        batch_key,
    )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_coordinates "
        "(receipt_id, batch_key, start_generation) VALUES (%s, %s, %s)",
        (receipt_id, b"terminal", 1),
    )
    for table, column, value in (
        (
            "catalog_publication_finalization_batch_start_cursors",
            "start_cursor",
            cursor,
        ),
        (
            "catalog_publication_finalization_batch_start_counts",
            "start_processed_count",
            processed_count,
        ),
        (
            "catalog_publication_finalization_batch_next_cursors",
            "next_cursor",
            cursor,
        ),
        ("catalog_publication_finalization_batch_row_counts", "row_count", 0),
        (
            "catalog_publication_finalization_batch_committed_ats",
            "committed_at",
            finalized_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(receipt_id, start_generation, {column}) VALUES (%s, %s, %s)",
            (*batch_key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_seals "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        batch_key,
    )
    for checkpoint_table, checkpoint_column, checkpoint_value in (
        ("catalog_publication_finalization_checkpoint_generations", "generation", 2),
        ("catalog_publication_finalization_checkpoint_cursors", "cursor", cursor),
        (
            "catalog_publication_finalization_checkpoint_counts",
            "processed_count",
            processed_count,
        ),
        ("catalog_publication_finalization_checkpoint_states", "state", "COMPLETE"),
        (
            "catalog_publication_finalization_checkpoint_updated_ats",
            "updated_at",
            finalized_at,
        ),
    ):
        quoted = (
            f"`{checkpoint_column}`"
            if checkpoint_column == "cursor"
            else checkpoint_column
        )
        connector.execute(
            f"UPDATE {checkpoint_table} SET {quoted} = %s WHERE receipt_id = %s",
            (checkpoint_value, receipt_id),
        )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _seed_catalog_publication_cleanup_fixture(
    connector: SQLiteConnector,
    *,
    finalize_current: bool = True,
) -> tuple[bytes, bytes, bytes]:
    gid = 7
    gallery_id = 1
    source_gallery_name = b"gallery"
    publication_key = identity.publication_key(gid)
    old_receipt = b"o" * 16
    current_receipt = b"n" * 16
    statements: list[tuple[str, tuple[object, ...]]] = [
        (
            "INSERT INTO catalog_gallery_identities "
            "(gallery_id, gallery_key, scope_key, locator_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (gallery_id, b"g" * 32, b"s" * 32, b"l" * 32),
        ),
        (
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, 0)",
            (gid,),
        ),
        (
            "INSERT INTO catalog_source_gallery_name_gids "
            "(source_gallery_name, gid) VALUES (%s, %s)",
            (source_gallery_name, gid),
        ),
        (
            "INSERT INTO catalog_gallery_source_name_accesses "
            "(gallery_id, source_gallery_name) VALUES (%s, %s)",
            (gallery_id, source_gallery_name),
        ),
        (
            "INSERT INTO catalog_publication_identities (publication_key, gid) "
            "VALUES (%s, %s)",
            (publication_key, gid),
        ),
    ]
    for revision in (1, 2):
        statements.extend(
            (
                (
                    "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_revision_publication_counts "
                    "(revision, publication_count) VALUES (%s, 1)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_revision_descriptor_seals "
                    "(revision) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_source_revision_anchors "
                    "(source_revision) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_source_revision_channels "
                    "(source_revision, channel) VALUES (%s, %s)",
                    (revision, b"default"),
                ),
                (
                    "INSERT INTO catalog_source_revision_snapshot_manifests "
                    "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
                    (revision, bytes((revision,)) * 32),
                ),
                (
                    "INSERT INTO catalog_source_revision_descriptor_seals "
                    "(source_revision) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_publication_generation_nodes "
                    "(generation) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_publication_generation_successors "
                    "(successor_generation, predecessor_generation) "
                    "VALUES (%s, %s)",
                    (revision, revision - 1),
                ),
                (
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (
                        identity.catalog_publication_occurrence_sha256(
                            revision, publication_key
                        ),
                        revision,
                        publication_key,
                    ),
                ),
                (
                    "INSERT INTO catalog_publication_storage "
                    "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
                    "language_sha256, modified_at, source_title_sha256) "
                    "VALUES (%s, %s, %s, %s, 1, %s)",
                    (
                        identity.catalog_publication_occurrence_sha256(
                            revision, publication_key
                        ),
                        gallery_id,
                        b"s" * 32,
                        b"l" * 32,
                        b"t" * 32,
                    ),
                ),
                (
                    "INSERT INTO catalog_publication_order "
                    "(revision, position, publication_key) VALUES (%s, 0, %s)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_contents "
                    "(revision, publication_key, content_sha256) VALUES (%s, %s, %s)",
                    (revision, publication_key, b"c" * 32),
                ),
                (
                    "INSERT INTO catalog_contributor_anchors "
                    "(revision, publication_key, position) VALUES (%s, %s, 0)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_contributor_name_sha256s "
                    "(revision, publication_key, position, contributor_name_sha256) "
                    "VALUES (%s, %s, 0, %s)",
                    (revision, publication_key, b"a" * 32),
                ),
                (
                    "INSERT INTO catalog_contributor_roles "
                    "(revision, publication_key, position, role) "
                    "VALUES (%s, %s, 0, %s)",
                    (revision, publication_key, b"artist"),
                ),
                (
                    "INSERT INTO catalog_contributor_identities "
                    "(revision, publication_key, contributor_name_sha256, role, position) "
                    "VALUES (%s, %s, %s, %s, 0)",
                    (revision, publication_key, b"a" * 32, b"artist"),
                ),
                (
                    "INSERT INTO catalog_contributor_seals "
                    "(revision, publication_key, position) VALUES (%s, %s, 0)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_subjects "
                    "(revision, publication_key, position, tag_id) "
                    "VALUES (%s, %s, 0, 1)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_artifacts "
                    "(revision, publication_key, artifact_sha256, "
                    "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
                    (revision, publication_key, b"b" * 32, b"m" * 32),
                ),
            )
        )
    _fixture_rows(connector, statements)
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for receipt_id, revision in ((old_receipt, 1), (current_receipt, 2)):
            seed_publication_commit(
                connector,
                receipt_id=receipt_id,
                candidate_id=bytes((96 + revision,)) * 16,
                revision=revision,
                source_revision=revision,
                generation=revision,
                preparation_id=bytes((112 + revision,)) * 16,
                operational_policy_id=1,
                artifact_policy_id=1,
                display_title_policy_id=1,
                new_galleries=1,
                changed_galleries=0,
                removed_galleries=0,
                duplicate_losers=0,
                committed_at=revision,
                channel=None,
            )
            if revision == 1 or finalize_current:
                _finalize_publication_receipt(
                    connector,
                    receipt_id=receipt_id,
                    cursor=publication_key,
                    processed_count=1,
                    finalized_at=10 + revision,
                )
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", current_receipt),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    return publication_key, old_receipt, current_receipt


def _seed_analysis_overlay_rows(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> None:
    file_sha256 = b"f" * 32
    content_sha256 = b"c" * 32
    gallery_id = 7
    gid = 9
    _fixture_rows(
        connector,
        [
            (
                "INSERT INTO catalog_analysis_impacted_galleries "
                "(analysis_id, gallery_id) VALUES (%s, %s)",
                (analysis_id, gallery_id),
            ),
            *[
                (sql, parameters)
                for sql, parameters in (
                    (
                        "INSERT INTO catalog_a_file_decision_shadow_anchors "
                        "(analysis_id, file_sha256) VALUES (%s, %s)",
                        (analysis_id, file_sha256),
                    ),
                    (
                        "INSERT INTO catalog_a_file_decision_shadow_occurrences "
                        "(analysis_id, file_sha256, occurrence_count) "
                        "VALUES (%s, %s, 1)",
                        (analysis_id, file_sha256),
                    ),
                    (
                        "INSERT INTO catalog_a_file_decision_shadow_artists "
                        "(analysis_id, file_sha256, artist_count) "
                        "VALUES (%s, %s, 1)",
                        (analysis_id, file_sha256),
                    ),
                    (
                        "INSERT INTO catalog_a_file_decision_shadow_gallery_artist_max "
                        "(analysis_id, file_sha256, maximum_gallery_artist_count) "
                        "VALUES (%s, %s, 1)",
                        (analysis_id, file_sha256),
                    ),
                    (
                        "INSERT INTO catalog_a_file_decision_shadow_seals "
                        "(analysis_id, file_sha256) VALUES (%s, %s)",
                        (analysis_id, file_sha256),
                    ),
                    (
                        "INSERT INTO catalog_analysis_content_owner_candidate_shadows "
                        "(analysis_id, gallery_id, content_sha256, "
                        "prefer_not_already_uploaded, title_scalar_count, download_time) "
                        "VALUES (%s, %s, %s, 1, 1, 1)",
                        (analysis_id, gallery_id, content_sha256),
                    ),
                    (
                        "INSERT INTO catalog_analysis_content_owner_shadows "
                        "(analysis_id, content_sha256, owner_gallery_id) "
                        "VALUES (%s, %s, %s)",
                        (analysis_id, content_sha256, gallery_id),
                    ),
                    (
                        "INSERT INTO catalog_a_impacted_content_provenance "
                        "(analysis_id, gallery_id, content_sha256) "
                        "VALUES (%s, %s, %s)",
                        (analysis_id, gallery_id, content_sha256),
                    ),
                    (
                        "INSERT INTO catalog_analysis_impacted_content "
                        "(analysis_id, content_sha256, witness_gallery_id) "
                        "VALUES (%s, %s, %s)",
                        (analysis_id, content_sha256, gallery_id),
                    ),
                    (
                        "INSERT INTO catalog_a_impacted_gid_provenance_storage "
                        "(analysis_id, gallery_id) VALUES (%s, %s)",
                        (analysis_id, gallery_id),
                    ),
                    (
                        "INSERT INTO catalog_analysis_impacted_gid_storage "
                        "(analysis_id, gid) VALUES (%s, %s)",
                        (analysis_id, gid),
                    ),
                    (
                        "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
                        "(analysis_id, file_sha256) VALUES (%s, %s)",
                        (analysis_id, b"t" * 32),
                    ),
                    (
                        "INSERT INTO catalog_analysis_content_owner_candidate_tombstones "
                        "(analysis_id, gallery_id) VALUES (%s, 8)",
                        (analysis_id,),
                    ),
                    (
                        "INSERT INTO catalog_analysis_content_owner_tombstones "
                        "(analysis_id, content_sha256) VALUES (%s, %s)",
                        (analysis_id, b"d" * 32),
                    ),
                    (
                        "INSERT INTO catalog_analysis_gid_candidate_shadows "
                        "(analysis_id, gallery_id) VALUES (%s, %s)",
                        (analysis_id, gallery_id),
                    ),
                    (
                        "INSERT INTO catalog_analysis_gid_candidate_tombstones "
                        "(analysis_id, gallery_id) VALUES (%s, 8)",
                        (analysis_id,),
                    ),
                    (
                        "INSERT INTO catalog_analysis_gid_winner_selections "
                        "(analysis_id, winner_gallery_id) VALUES (%s, %s)",
                        (analysis_id, gallery_id),
                    ),
                    (
                        "INSERT INTO catalog_analysis_gid_winner_tombstones "
                        "(analysis_id, gid) VALUES (%s, 10)",
                        (analysis_id,),
                    ),
                )
            ],
        ],
    )


def _analysis_overlay_rows(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        connector.fetch_one(
            f"SELECT COUNT(*) FROM {table} WHERE analysis_id = %s",
            (analysis_id,),
        )
        for table in _ALL_ANALYSIS_OVERLAY_TABLES
    )


_GALLERY_PAGE_DELETE_PHASE_BY_TABLE = {
    table: group for group in _GALLERY_PAGE_DELETE_GROUPS.values() for table in group
}


def _seed_cleanup_gallery_page(
    connector: SQLiteConnector,
    *,
    parent: bytes,
    child: bytes,
) -> None:
    seed_gallery_page_descriptor(
        connector,
        page_sha256=child,
        page_bytes=b"child",
        component=b"FILE",
        level=0,
        subtree_item_count=1,
    )
    seed_gallery_page_descriptor(
        connector,
        page_sha256=parent,
        page_bytes=b"parent",
        component=b"FILE",
        level=1,
        subtree_item_count=1,
    )
    seed_gallery_page_bounds(
        connector,
        page_sha256=parent,
        first_key=b"a" * 8,
        last_key=b"z" * 8,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_children "
        "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
        (parent, child),
    )


def _gallery_page_group_rows(
    connector: SQLiteConnector,
    *,
    parent: bytes,
    tables: tuple[str, ...],
) -> tuple[list[tuple[Any, ...]], ...]:
    rows: list[list[tuple[Any, ...]]] = []
    for table in tables:
        key = "parent_sha256" if table.endswith("_children") else "page_sha256"
        rows.append(
            connector.fetch_all(
                f"SELECT * FROM {table} WHERE {key} = %s",
                (parent,),
            )
        )
    return tuple(rows)


def _seed_minimal_canonical_value(
    connector: SQLiteConnector,
    *,
    value_sha256: bytes,
    page_sha256: bytes,
    digest_domain: bytes,
) -> None:
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=digest_domain,
        page_sha256=page_sha256,
        page_bytes=b"x",
        subtree_item_count=1,
        allocated_at=0,
    )


def _seed_source_build_scope(
    connector: SQLiteConnector,
    *,
    discriminator: int,
) -> bytes:
    seed_manifest_policy(connector)
    source_root_sha256 = bytes((discriminator,)) + b"r" * 31
    _seed_minimal_canonical_value(
        connector,
        value_sha256=source_root_sha256,
        page_sha256=bytes((discriminator,)) + b"p" * 31,
        digest_domain=b"source_root_v1",
    )
    return seed_source_scope(
        connector,
        source_root_sha256=source_root_sha256,
    ).scope_key


def _seed_abandoned_analysis_for_cleanup(
    connector: SQLiteConnector,
    *,
    discriminator: int,
) -> bytes:
    analysis_id = bytes((discriminator,)) + b"a" * 15
    build_id = bytes((discriminator,)) + b"b" * 15
    scope_key = _seed_source_build_scope(
        connector,
        discriminator=discriminator,
    )
    seed_analysis_policy(connector)
    seed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
        manifest_policy_id=1,
        state="SEALED",
        created_at=0,
        sealed_at=0,
    )
    seed_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        policy_id=1,
        input_manifest_sha256=b"m" * 32,
        started_at=0,
        state="ABANDONED",
    )
    return analysis_id


def _position_analysis_cleanup_at_overlay(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int,
) -> None:
    result: CleanupBatchResult | None = None
    for phase_index in range(6):
        result = _advance(
            connector,
            gate,
            cycle,
            1,
            phase_index.to_bytes(32, "big"),
            now=now + phase_index,
        )
        assert result.row_count == 0
    assert result is not None
    assert result.phase == "AR_OVERLAY"
    assert result.generation == 1


def _cleanup_protocol_snapshot(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    return (
        connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints"),
        connector.fetch_all("SELECT * FROM operational_cleanup_batch_receipts"),
    )


def _source_scope_family_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(query)
        for query in (
            "SELECT * FROM catalog_source_scope_seals",
            "SELECT * FROM catalog_source_scope_identities",
            "SELECT * FROM catalog_source_scope_identity_policy_versions",
            "SELECT * FROM catalog_source_scope_source_root_sha256s",
            "SELECT * FROM catalog_source_scope_source_providers",
            "SELECT * FROM catalog_source_scope_anchors",
        )
    )


def _artifact_policy_semantics_family_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(query)
        for query in (
            "SELECT * FROM catalog_artifact_policy_semantics_seals",
            "SELECT * FROM catalog_artifact_policy_semantics_identities",
            "SELECT * FROM "
            "catalog_artifact_policy_semantics_producer_fingerprint_sha256s",
            "SELECT * FROM catalog_artifact_policy_semantics_max_image_short_sides",
            "SELECT * FROM "
            "catalog_artifact_policy_semantics_artifact_algorithm_versions",
            "SELECT * FROM catalog_artifact_policy_semantics_anchors",
        )
    )


def _seed_cleanup_candidate(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        seed_publication_candidate(
            connector,
            candidate_id=candidate_id,
            analysis_id=b"a" * 16,
            reserved_revision=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            artifacts_required=False,
            created_at=0,
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


_CANDIDATE_DEFINITION_DELETE_ORDER = (
    "catalog_publication_candidate_definition_seals",
    "catalog_publication_candidate_created_ats",
    "catalog_publication_candidate_artifacts_required",
    "catalog_publication_candidate_display_title_policy_ids",
    "catalog_publication_candidate_artifact_policy_ids",
    "catalog_publication_candidate_reserved_revisions",
    "catalog_publication_candidate_analysis_ids",
    "catalog_publication_candidate_anchors",
)


def _candidate_definition_rows(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(
            f"SELECT * FROM {table} WHERE candidate_id = %s",
            (candidate_id,),
        )
        for table in _CANDIDATE_DEFINITION_DELETE_ORDER
    )


def _advance_to_cleanup_phase(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    target_phase: str,
) -> CleanupBatchResult:
    generation = 1
    for attempt in range(64):
        result = _advance(
            connector,
            gate,
            cycle,
            generation,
            (attempt + 1).to_bytes(32, "big"),
            now=3 + attempt,
        )
        if result.phase == target_phase and result.generation == 1:
            assert result.row_count == 0
            return result
        assert result.generation is not None
        generation = result.generation
    raise AssertionError(f"cleanup did not reach {target_phase}")


def _seed_prepared_artifact_family(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    artifact_sha256: bytes,
    state: str,
) -> None:
    _fixture_rows(
        connector,
        [
            (
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key, artifact_sha256, "
                "storage_codec_version, storage_generation, protection_token, state) "
                "VALUES (%s, %s, %s, 1, 7, %s, %s)",
                (candidate_id, publication_key, artifact_sha256, b"t" * 184, state),
            ),
        ],
    )


def _prepared_artifact_family_rows(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
) -> tuple[list[tuple[Any, ...]], ...]:
    return (
        connector.fetch_all(
            "SELECT * FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s",
            (candidate_id, publication_key),
        ),
    )


def _seed_artifact_semantic_input_family(
    connector: SQLiteConnector,
    *,
    artifact_semantics_sha256: bytes,
) -> None:
    components = tuple(bytes((index,)) * 32 for index in range(1, 7))
    _fixture_rows(
        connector,
        [
            (
                "INSERT INTO catalog_artifact_semantic_inputs "
                "(artifact_semantics_sha256, source_manifest_component_sha256, "
                "member_plan_component_sha256, effective_content_component_sha256, "
                "selected_component_sha256, owner_component_sha256, "
                "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (artifact_semantics_sha256, *components),
            ),
        ],
    )


def _artifact_semantic_input_family_rows(
    connector: SQLiteConnector,
    *,
    artifact_semantics_sha256: bytes,
) -> tuple[list[tuple[Any, ...]], ...]:
    return (
        connector.fetch_all(
            "SELECT * FROM catalog_artifact_semantic_inputs "
            "WHERE artifact_semantics_sha256 = %s",
            (artifact_semantics_sha256,),
        ),
    )


def _source_build_discovery_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[object, ...]], ...]:
    return tuple(
        connector.fetch_all(query)
        for query in (
            "SELECT * FROM catalog_source_build_discovery_anchors",
            "SELECT * FROM catalog_source_build_discovery_scan_attempts",
            "SELECT * FROM catalog_source_build_discovery_gallery_counts",
            "SELECT * FROM catalog_source_build_discovery_tree_observation_sha256s",
            "SELECT * FROM catalog_source_build_discovery_completed_ats",
            "SELECT * FROM catalog_source_build_discovery_seals",
        )
    )


def _analysis_run_family_rows(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(
            f"SELECT * FROM {table} WHERE analysis_id = %s",
            (analysis_id,),
        )
        for table in (
            "catalog_analysis_run_completed_ats",
            "catalog_analysis_run_descriptor_seals",
            "catalog_analysis_run_identities",
            "catalog_analysis_run_started_ats",
            "catalog_analysis_run_input_manifest_sha256s",
            "catalog_analysis_run_policy_ids",
            "catalog_analysis_run_build_ids",
            "catalog_analysis_run_states",
            "catalog_analysis_run_anchors",
        )
    )


def test_analysis_cleanup_retains_only_latest_abandoned_recovery_proof(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-latest-recovery-retention.sqlite3")
    try:
        seed_analysis_policy(connector)
        old_scope = _seed_source_build_scope(connector, discriminator=95)
        latest_scope = _seed_source_build_scope(connector, discriminator=96)
        successor_scope = _seed_source_build_scope(connector, discriminator=97)
        old_build = b"O" * 16
        latest_build = b"L" * 16
        successor_build = b"S" * 16
        old_analysis = bytes((95,)) + b"o" * 15
        latest_analysis = bytes((95,)) + b"l" * 15
        for build_id, scope_key, created_at in (
            (old_build, old_scope, 1),
            (latest_build, latest_scope, 2),
            (successor_build, successor_scope, 3),
        ):
            seed_source_build(
                connector,
                build_id=build_id,
                scope_key=scope_key,
                manifest_policy_id=1,
                state="SEALED",
                created_at=created_at,
                sealed_at=created_at,
            )
        for analysis_id, build_id, started_at in (
            (old_analysis, old_build, 1),
            (latest_analysis, latest_build, 2),
        ):
            seed_analysis_run(
                connector,
                analysis_id=analysis_id,
                build_id=build_id,
                policy_id=1,
                input_manifest_sha256=bytes((started_at,)) * 32,
                started_at=started_at,
                state="ABANDONED",
            )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (1, 1, 1),
                ),
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (2, 2, 2),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (old_build, 1),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (latest_build, 2),
                ),
            ],
        )

        gate = _exclusive(connector)
        latest_before = _analysis_run_family_rows(connector, latest_analysis)
        first_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            old_analysis[0],
            max_rows=32,
        )
        _drain(connector, gate, first_cycle)
        assert not any(_analysis_run_family_rows(connector, old_analysis))
        assert _analysis_run_family_rows(connector, latest_analysis) == latest_before

        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (3, 3, 3),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (successor_build, 3),
                ),
            ],
        )
        second_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            latest_analysis[0],
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, second_cycle, now=101)
        assert not any(_analysis_run_family_rows(connector, latest_analysis))
    finally:
        connector.close()


@pytest.mark.parametrize("sibling_state", ("COMPLETE", "ABANDONED"))
def test_analysis_cleanup_never_launders_abandoned_multi_policy_history(
    tmp_path: Path,
    sibling_state: str,
) -> None:
    connector = _database(
        tmp_path / f"analysis-abandoned-sibling-{sibling_state.lower()}.sqlite3"
    )
    try:
        scope_key = _seed_source_build_scope(connector, discriminator=102)
        seed_analysis_policy(connector)
        seed_analysis_policy(connector, policy_id=2, algorithm_version=2)
        build_id = b"M" * 16
        abandoned = bytes((102,)) + b"a" * 15
        sibling = bytes((102,)) + b"s" * 15
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            manifest_policy_id=1,
            state="SEALED",
            created_at=1,
            sealed_at=1,
        )
        seed_analysis_run(
            connector,
            analysis_id=abandoned,
            build_id=build_id,
            policy_id=1,
            input_manifest_sha256=b"a" * 32,
            started_at=1,
            state="ABANDONED",
        )
        seed_analysis_run(
            connector,
            analysis_id=sibling,
            build_id=build_id,
            policy_id=2,
            input_manifest_sha256=b"s" * 32,
            started_at=2,
            state=sibling_state,
            completed_at=3 if sibling_state == "COMPLETE" else None,
        )
        newer_scope = _seed_source_build_scope(connector, discriminator=108)
        newer_build = bytes((108,)) + b"n" * 15
        seed_source_build(
            connector,
            build_id=newer_build,
            scope_key=newer_scope,
            manifest_policy_id=1,
            state="ABANDONED",
            created_at=4,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (1, 1, 1),
                ),
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (2, 2, 2),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (build_id, 1),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (newer_build, 2),
                ),
            ],
        )
        before = {
            abandoned: _analysis_run_family_rows(connector, abandoned),
            sibling: _analysis_run_family_rows(connector, sibling),
        }

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            abandoned[0],
            max_rows=32,
        )
        _drain(connector, gate, cycle)
        assert _analysis_run_family_rows(connector, abandoned) == before[abandoned]
        assert _analysis_run_family_rows(connector, sibling) == before[sibling]

        source_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_BUILD,
            newer_build[0],
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, source_cycle, now=101)
        assert (
            connector.fetch_one(
                "SELECT build_id FROM catalog_source_build_anchors WHERE build_id = %s",
                (newer_build,),
            )
            == ()
        )
    finally:
        connector.close()


def test_candidate_cleanup_retains_historical_source_build_base_lineage(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-source-base-retention.sqlite3")
    try:
        candidate_id = bytes((103,)) + b"c" * 15
        receipt_id = b"R" * 16
        base_build = b"B" * 16
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_commit_candidates "
                    "(receipt_id, candidate_id) VALUES (%s, %s)",
                    (receipt_id, candidate_id),
                ),
                (
                    "INSERT INTO catalog_publication_commit_finalizations "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                ),
                (
                    "INSERT INTO catalog_source_build_base_publication_commits "
                    "(build_id, base_receipt_id) VALUES (%s, %s)",
                    (base_build, receipt_id),
                ),
            ],
        )
        before = _candidate_definition_rows(
            connector,
            candidate_id=candidate_id,
        )

        gate = _exclusive(connector)
        first_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            candidate_id[0],
            max_rows=32,
        )
        _drain(connector, gate, first_cycle)
        assert (
            _candidate_definition_rows(connector, candidate_id=candidate_id) == before
        )

        connector.execute(
            "DELETE FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (base_build,),
        )
        second_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            candidate_id[0],
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, second_cycle, now=101)
        assert not any(_candidate_definition_rows(connector, candidate_id=candidate_id))
    finally:
        connector.close()


def test_current_only_priority_rewinds_after_source_build_releases_candidate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "current-only-priority-rewind.sqlite3")
    try:
        candidate_id = bytes((105,)) + b"c" * 15
        build_id = bytes((106,)) + b"b" * 15
        receipt_id = b"R" * 16
        scope_key = _seed_source_build_scope(connector, discriminator=106)
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            manifest_policy_id=1,
            state="SEALED",
            created_at=1,
            sealed_at=1,
        )
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_commit_candidates "
                    "(receipt_id, candidate_id) VALUES (%s, %s)",
                    (receipt_id, candidate_id),
                ),
                (
                    "INSERT INTO catalog_publication_commit_finalizations "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                ),
                (
                    "INSERT INTO catalog_source_build_base_publication_commits "
                    "(build_id, base_receipt_id) VALUES (%s, %s)",
                    (build_id, receipt_id),
                ),
            ],
        )

        gate = _exclusive(connector)
        with connector.transaction():
            first = VNextCleanupRepository.next_current_only_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle_cutoff_at=100,
                now=2,
            )
        assert first is not None
        assert first.target_kind is CleanupTargetKind.SOURCE_BUILD
        _drain(connector, gate, first, now=3)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_anchors WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidate_anchors "
            "WHERE candidate_id = %s",
            (candidate_id,),
        ) == (candidate_id,)

        with connector.transaction():
            second = VNextCleanupRepository.next_current_only_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle_cutoff_at=100,
                now=100,
            )
        assert second is not None
        assert second.target_kind is CleanupTargetKind.PUBLICATION_CANDIDATE
        _drain(connector, gate, second, now=101)
        assert not any(_candidate_definition_rows(connector, candidate_id=candidate_id))
    finally:
        connector.close()


def test_analysis_cleanup_retains_historical_source_build_base_provenance(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-source-base-retention.sqlite3")
    try:
        scope_key = _seed_source_build_scope(connector, discriminator=104)
        seed_analysis_policy(connector)
        analysis_id = bytes((104,)) + b"a" * 15
        analysis_build = b"A" * 16
        base_build = b"B" * 16
        receipt_id = b"R" * 16
        seed_source_build(
            connector,
            build_id=analysis_build,
            scope_key=scope_key,
            manifest_policy_id=1,
            state="SEALED",
            created_at=1,
            sealed_at=1,
        )
        seed_analysis_run(
            connector,
            analysis_id=analysis_id,
            build_id=analysis_build,
            policy_id=1,
            input_manifest_sha256=b"m" * 32,
            started_at=1,
            state="COMPLETE",
            completed_at=2,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_commit_source_revisions "
                    "(receipt_id, source_revision) VALUES (%s, %s)",
                    (receipt_id, 1),
                ),
                (
                    "INSERT INTO catalog_source_revision_provenance "
                    "(source_revision, analysis_id) VALUES (%s, %s)",
                    (1, analysis_id),
                ),
                (
                    "INSERT INTO catalog_source_build_base_publication_commits "
                    "(build_id, base_receipt_id) VALUES (%s, %s)",
                    (base_build, receipt_id),
                ),
            ],
        )
        before = _analysis_run_family_rows(connector, analysis_id)

        gate = _exclusive(connector)
        first_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
        )
        _drain(connector, gate, first_cycle)
        assert _analysis_run_family_rows(connector, analysis_id) == before

        connector.execute(
            "DELETE FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (base_build,),
        )
        second_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, second_cycle, now=101)
        assert not any(_analysis_run_family_rows(connector, analysis_id))
    finally:
        connector.close()


def test_source_cleanup_preserves_newer_mapping_until_older_retirement_is_gone(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "source-retirement-mapping-order.sqlite3")
    try:
        older_scope = _seed_source_build_scope(connector, discriminator=105)
        newer_scope = _seed_source_build_scope(connector, discriminator=106)
        older_build = bytes((105,)) + b"a" * 15
        newer_build = bytes((106,)) + b"z" * 15
        older_analysis = bytes((107,)) + b"a" * 15
        for build_id, scope_key, created_at, state in (
            (older_build, older_scope, 1, "SEALED"),
            (newer_build, newer_scope, 2, "ABANDONED"),
        ):
            seed_source_build(
                connector,
                build_id=build_id,
                scope_key=scope_key,
                manifest_policy_id=1,
                state=state,
                created_at=created_at,
                sealed_at=created_at if state == "SEALED" else None,
            )
        seed_analysis_policy(connector)
        seed_analysis_run(
            connector,
            analysis_id=older_analysis,
            build_id=older_build,
            policy_id=1,
            input_manifest_sha256=b"a" * 32,
            started_at=1,
            state="ABANDONED",
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (1, 1, 1),
                ),
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (2, 2, 2),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (older_build, 1),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (newer_build, 2),
                ),
            ],
        )

        gate = _exclusive(connector)
        blocked_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_BUILD,
            newer_build[0],
            max_rows=32,
        )
        _drain(connector, gate, blocked_cycle)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (2,),
        ) == (newer_build,)

        analysis_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            older_analysis[0],
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, analysis_cycle, now=101)
        assert not any(_analysis_run_family_rows(connector, older_analysis))

        released_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_BUILD,
            newer_build[0],
            max_rows=32,
            now=200,
        )
        _drain(connector, gate, released_cycle, now=201)
        assert (
            connector.fetch_one(
                "SELECT build_id FROM operational_source_build_generations "
                "WHERE generation = %s",
                (2,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT build_id FROM catalog_source_build_anchors WHERE build_id = %s",
                (newer_build,),
            )
            == ()
        )
    finally:
        connector.close()


def _observation_vertical_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[object, ...]], ...]:
    return tuple(
        connector.fetch_all(query)
        for query in (
            "SELECT * FROM catalog_gallery_observation_directories",
            "SELECT * FROM catalog_gallery_observation_stat",
            "SELECT * FROM catalog_gallery_observation_scans",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_anchors",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_devices",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_inodes",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_modified_nses",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_changed_nses",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_seals",
        )
    )


def test_content_blob_sweep_is_bounded_replayable_and_reusable(tmp_path: Path) -> None:
    connector = _database(tmp_path / "content-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        first_key = bytes((5,)) + b"a" * 31
        second_key = bytes((5,)) + b"b" * 31
        outside_shard = bytes((6,)) + b"c" * 31
        connector.execute_many(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            [(first_key, 1), (second_key, 2), (outside_shard, 3)],
        )

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CONTENT_BLOB,
            5,
            max_rows=1,
        )
        page_one = _advance(connector, gate, cycle, 1, b"a" * 32, now=3)
        assert (page_one.row_count, page_one.generation, page_one.cursor) == (
            1,
            2,
            first_key,
        )
        assert not page_one.phase_complete
        before = connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        )
        replay = _advance(connector, gate, cycle, 1, b"a" * 32, now=4)
        assert replay.replayed
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == before
        )

        page_two = _advance(connector, gate, cycle, 2, b"b" * 32, now=5)
        assert (page_two.row_count, page_two.generation, page_two.cursor) == (
            1,
            3,
            second_key,
        )
        completed = _advance(connector, gate, cycle, 3, b"c" * 32, now=6)
        assert completed.cycle_complete
        assert completed.deleted_count == 2
        assert connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        ) == [(outside_shard,)]
        with connector.transaction():
            resumed = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=7,
            )
        assert resumed.cycle_complete and resumed.replayed
        assert resumed.deleted_count == 2

        next_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CONTENT_BLOB,
            5,
            max_rows=1,
        )
        assert next_cycle.cycle_generation == 2
        assert next_cycle.cleanup_id != cycle.cleanup_id
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_cleanup_completions WHERE target_key = %s",
                (cycle.target_key,),
            )
            == ()
        )
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("kind", "table", "insert_sql", "values", "key"),
    (
        (
            CleanupTargetKind.FILE_NAME_IDENTITY,
            "catalog_file_name_identities",
            None,
            (bytes((9,)) + b"n" * 31, b"001.jpg", b"CONTENT"),
            bytes((9,)) + b"n" * 31,
        ),
        (
            CleanupTargetKind.PUBLICATION_IDENTITY,
            "catalog_publication_identities",
            "INSERT INTO catalog_publication_identities "
            "(publication_key, gid) VALUES (%s, %s)",
            (
                identity.publication_key(9),
                9,
            ),
            identity.publication_key(9),
        ),
    ),
)
def test_leaf_identity_strategies_delete_only_their_fixed_shard(
    tmp_path: Path,
    kind: CleanupTargetKind,
    table: str,
    insert_sql: str | None,
    values: tuple[object, ...],
    key: bytes,
) -> None:
    connector = _database(tmp_path / f"{kind.value}.sqlite3")
    try:
        gate = _exclusive(connector)
        if kind is CleanupTargetKind.FILE_NAME_IDENTITY:
            seed_file_name_identity(
                connector,
                file_key=key,
                name_bytes=cast(bytes, values[1]),
                file_role=cast(bytes, values[2]),
            )
        else:
            assert insert_sql is not None
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, 0)",
                (values[1],),
            )
            connector.execute(insert_sql, values)
        cycle = _begin(connector, gate, kind, key[0], max_rows=8)
        first = _advance(connector, gate, cycle, 1, b"d" * 32, now=3)
        assert first.row_count == 1 and first.cursor == key
        completed = _advance(connector, gate, cycle, 2, b"e" * 32, now=4)
        assert completed.cycle_complete and completed.deleted_count == 1
        assert connector.fetch_all(f"SELECT * FROM {table}") == []
    finally:
        connector.close()


def test_publication_selection_retains_its_derived_publication_identity(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-selection-retention.sqlite3")
    try:
        gid = 17
        publication_key = identity.publication_key(gid)
        candidate_id = bytes((17,)) + b"c" * 15
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
            "VALUES (%s, 0)",
            (gid,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_identities (publication_key, gid) "
            "VALUES (%s, %s)",
            (publication_key, gid),
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (1, %s, %s, %s)",
                    (b"g" * 32, b"s" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO catalog_source_gallery_name_gids "
                    "(source_gallery_name, gid) VALUES (%s, %s)",
                    (b"gallery", gid),
                ),
                (
                    "INSERT INTO catalog_gallery_source_name_accesses "
                    "(gallery_id, source_gallery_name) VALUES (1, %s)",
                    (b"gallery",),
                ),
            ],
        )
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        selection_occurrence = identity.publication_selection_occurrence_sha256(
            candidate_id, publication_key
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_selection_occurrence_identities "
                    "(selection_occurrence_sha256, candidate_id, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (selection_occurrence, candidate_id, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_selection_storage "
                    "(selection_occurrence_sha256, gallery_id) VALUES (%s, 1)",
                    (selection_occurrence,),
                ),
            ],
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_IDENTITY,
            publication_key[0],
            max_rows=8,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT gid FROM catalog_publication_identities WHERE publication_key = %s",
            (publication_key,),
        ) == (gid,)
    finally:
        connector.close()


def test_publication_identity_retains_its_gid_upload_time(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-upload-time-retention.sqlite3")
    try:
        gid = 23
        publication_key = identity.publication_key(gid)
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
            "VALUES (%s, 123)",
            (gid,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_identities (publication_key, gid) "
            "VALUES (%s, %s)",
            (publication_key, gid),
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT upload_time FROM catalog_gallery_upload_times WHERE gid = %s",
            (gid,),
        ) == (123,)
    finally:
        connector.close()


def test_cleanup_fails_closed_for_shared_gate_and_registry_drift(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-fail-closed.sqlite3")
    try:
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"s" * 16,
            ),
        ):
            shared = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=1,
                lease_duration=100_000,
            )
        with pytest.raises(CleanupUnavailableError, match="EXCLUSIVE"):
            _begin(
                connector,
                shared,
                CleanupTargetKind.CONTENT_BLOB,
                0,
            )
        assert connector.fetch_all("SELECT 1 FROM operational_cleanup_jobs") == []

        # Let the shared capability expire before acquiring the exclusive gate.
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"x" * 16,
            ),
        ):
            exclusive = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=100_001,
                lease_duration=100_000,
            )
        connector.execute(
            "DELETE FROM operational_cleanup_phases WHERE phase = 'CB_ROOT'"
        )
        with pytest.raises(CleanupCorruptionError, match="phase seed"):
            _begin(
                connector,
                exclusive,
                CleanupTargetKind.CONTENT_BLOB,
                0,
            )
        assert connector.fetch_all("SELECT 1 FROM operational_cleanup_jobs") == []
    finally:
        connector.close()


def test_all_eighteen_strategies_match_the_closed_phase_registry(
    tmp_path: Path,
) -> None:
    expected = {
        CleanupTargetKind.SOURCE_BUILD: (
            "SB_CANONICAL_UPLOAD",
            "SB_GALLERY",
            "SB_DISCOVERY_SEAL",
            "SB_DISCOVERY_VALUES",
            "SB_DISCOVERY_ANCHOR",
            "SB_SATELLITES",
            "SB_GENERATION",
            "SB_ROOT",
        ),
        CleanupTargetKind.ANALYSIS_RUN: (
            "AR_BATCH_SEAL",
            "AR_BATCH_VALUES",
            "AR_BATCH_ANCHOR",
            "AR_COMPONENT_SEAL",
            "AR_COMPONENT_VALUES",
            "AR_COMPONENT_ANCHOR",
            "AR_OVERLAY",
            "AR_FILE_HASH_VALUES",
            "AR_IMPACT_PROVENANCE",
            "AR_FILE_HASH_ANCHOR",
            "AR_EVIDENCE",
            "AR_EXCLUSION_VALUES",
            "AR_EXCLUSION_ANCHOR",
            "AR_CHECKPOINT_SEAL",
            "AR_CHECKPOINT_VALUES",
            "AR_CHECKPOINT_ANCHOR",
            "AR_ANCESTRY",
            "AR_BASELINE",
            "AR_BINDINGS",
            "AR_DESCRIPTOR",
            "AR_RUN_VALUES",
            "AR_ROOT",
        ),
        CleanupTargetKind.CATALOG_PUBLICATION: (
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
        CleanupTargetKind.PUBLICATION_CANDIDATE: (
            "PC_SEALS",
            "PC_PREPARED",
            "PC_INPUT",
            "PC_BATCH_VALUES",
            "PC_BATCH_ANCHOR",
            "PC_CHECKPOINT_SEAL",
            "PC_SELECTION_STORAGE",
            "PC_CHECKPOINT_VALUES",
            "PC_CHECKPOINT_ANCHOR",
            "PC_BASES",
            "PC_SELECTION_IDENTITY",
            "PC_ROOT",
        ),
        CleanupTargetKind.OPERATIONAL_PREPARATION: (
            "OP_BATCH",
            "OP_CHECKPOINT",
            "OP_SUBTYPE",
            "OP_EVENT",
            "OP_SEAL",
            "OP_ROOT",
        ),
        CleanupTargetKind.GALLERY_OBSERVATION: (
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
        CleanupTargetKind.GALLERY_OBSERVATION_STAGING: (
            "GOS_RECEIPT_FRONTIER",
            "GOS_PAGE_ASSOCIATION",
            "GOS_REQUEST_DESCRIPTOR",
            "GOS_REQUEST_IDENTITY",
            "GOS_CHECKPOINT",
            "GOS_CLAIM",
            "GOS_ROOT",
        ),
        CleanupTargetKind.ARTIFACT_BLOB: ("AB_ROOT",),
        CleanupTargetKind.CANONICAL_VALUE: (
            "CV_DICTIONARY",
            "CV_SEMANTIC_LINK",
            "CV_IDENTITY",
            "CV_PARENT_DESCRIPTOR",
            "CV_PAGE",
            "CV_ROOT",
        ),
        CleanupTargetKind.CONTENT_BLOB: ("CB_ROOT",),
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE: (
            "GOP_OUTGOING_CHILD",
            "GOP_BOUNDS",
            "GOP_DESCRIPTOR",
            "GOP_ROOT",
        ),
        CleanupTargetKind.FILE_NAME_IDENTITY: ("FN_ROOT",),
        CleanupTargetKind.PUBLICATION_IDENTITY: ("PI_ROOT",),
        CleanupTargetKind.GALLERY_IDENTITY: (
            "GI_OBSERVATION_ALLOCATOR",
            "GI_SOURCE_NAME_ACCESS",
            "GI_ROOT",
        ),
        CleanupTargetKind.SOURCE_GALLERY_NAME_GID: ("SNG_ROOT",),
        CleanupTargetKind.GALLERY_UPLOAD_TIME: ("GUT_ROOT",),
        CleanupTargetKind.CANONICAL_VALUE_UPLOAD: ("CVU_ROOT",),
        CleanupTargetKind.HASH_CACHE_OBSERVATION: ("HC_FILE", "HC_ROOT"),
    }
    assert set(expected) == set(CleanupTargetKind)
    assert {
        kind: strategy.phases for kind, strategy in cleanup_module._STRATEGIES.items()
    } == expected

    connector = _database(tmp_path / "all-empty-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        for kind in CleanupTargetKind:
            cycle = _begin(connector, gate, kind, 0, max_rows=8)
            results = _drain(connector, gate, cycle)
            assert results[-1].cycle_complete
            assert len(results) == len(expected[kind])
    finally:
        connector.close()


def test_catalog_publication_cleanup_removes_only_historical_payload(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "catalog-publication-cleanup.sqlite3")
    try:
        publication_key, old_receipt, _current_receipt = (
            _seed_catalog_publication_cleanup_fixture(connector)
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
        )
        results = _drain(connector, gate, cycle)

        assert results[-1].cycle_complete
        assert sum(result.row_count for result in results) == len(
            _CATALOG_PUBLICATION_PAYLOAD_TABLES
        )
        for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES:
            if table == "catalog_publication_storage":
                selector = (
                    "SELECT COUNT(*) FROM catalog_publication_storage AS storage "
                    "JOIN catalog_publication_occurrence_identities AS occurrence "
                    "ON occurrence.catalog_occurrence_sha256 = "
                    "storage.catalog_occurrence_sha256 WHERE occurrence.revision = %s"
                )
            else:
                selector = f"SELECT COUNT(*) FROM {table} WHERE revision = %s"
            assert connector.fetch_one(
                selector,
                (1,),
            ) == (0,)
            assert connector.fetch_one(
                selector,
                (2,),
            ) == (1,)
        assert connector.fetch_one(
            "SELECT publication_count FROM catalog_revision_publication_counts "
            "WHERE revision = 1"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT revision FROM catalog_publication_commit_catalog_revisions "
            "WHERE receipt_id = %s",
            (old_receipt,),
        ) == (1,)
    finally:
        connector.close()


def test_catalog_publication_next_shard_prioritizes_interrupted_cycle(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "catalog-next-shard.sqlite3")
    try:
        publication_key, _old_receipt, _current_receipt = (
            _seed_catalog_publication_cleanup_fixture(connector)
        )
        gate = _exclusive(connector)
        with connector.transaction():
            assert (
                VNextCleanupRepository.catalog_publication_next_maintenance_shard(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    now=2,
                )
                == publication_key[0]
            )

        interrupted_shard = 200
        _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            interrupted_shard,
            now=3,
        )
        with connector.transaction():
            assert (
                VNextCleanupRepository.catalog_publication_next_maintenance_shard(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    now=4,
                )
                == interrupted_shard
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_jobs WHERE state = 'OPEN'"
        ) == (1,)
    finally:
        connector.close()


@pytest.mark.parametrize("damaged_revision", ("historical", "current"))
def test_catalog_publication_cleanup_requires_fully_finalized_old_and_current_receipts(
    tmp_path: Path,
    damaged_revision: str,
) -> None:
    connector = _database(
        tmp_path / f"catalog-partial-finalization-{damaged_revision}.sqlite3"
    )
    try:
        publication_key, old_receipt, current_receipt = (
            _seed_catalog_publication_cleanup_fixture(connector)
        )
        damaged_receipt = (
            old_receipt if damaged_revision == "historical" else current_receipt
        )
        connector.execute(
            "DELETE FROM catalog_publication_finalization_batch_seals "
            "WHERE receipt_id = %s",
            (damaged_receipt,),
        )
        assert (
            connector.fetch_one(
                "SELECT state, finalized_at FROM catalog_publication_receipts "
                "WHERE receipt_id = %s",
                (damaged_receipt,),
            )
            == ()
        )

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
        )
        assert all(result.row_count == 0 for result in _drain(connector, gate, cycle))
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publications "
            "WHERE revision = 1 AND publication_key = %s",
            (publication_key,),
        ) == (1,)
    finally:
        connector.close()


def test_catalog_publication_cleanup_waits_for_db_committed_current_receipt(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "catalog-current-db-committed.sqlite3")
    try:
        publication_key, _old_receipt, current_receipt = (
            _seed_catalog_publication_cleanup_fixture(
                connector,
                finalize_current=False,
            )
        )
        assert connector.fetch_one(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (current_receipt,),
        ) == ("DB_COMMITTED", None)

        gate = _exclusive(connector)
        blocked = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
        )
        assert all(result.row_count == 0 for result in _drain(connector, gate, blocked))
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publications "
            "WHERE revision = 1 AND publication_key = %s",
            (publication_key,),
        ) == (1,)

        committed_candidate = bytes((97,)) * 16
        _seed_cleanup_candidate(
            connector,
            candidate_id=committed_candidate,
        )
        committed_candidate_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            committed_candidate[0],
            max_rows=32,
            now=50,
        )
        assert all(
            result.row_count == 0
            for result in _drain(
                connector,
                gate,
                committed_candidate_cycle,
                now=51,
            )
        )
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidate_anchors "
            "WHERE candidate_id = %s",
            (committed_candidate,),
        ) == (committed_candidate,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publications "
            "WHERE revision = 1 AND publication_key = %s",
            (publication_key,),
        ) == (1,)

        _finalize_publication_receipt(
            connector,
            receipt_id=current_receipt,
            cursor=publication_key,
            processed_count=1,
            finalized_at=100,
        )
        assert connector.fetch_one(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (current_receipt,),
        ) == ("PROJECTION_FINALIZED", 100)
        released = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
            now=101,
        )
        _drain(connector, gate, released, now=102)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publications "
                "WHERE revision = 1 AND publication_key = %s",
                (publication_key,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publications "
            "WHERE revision = 2 AND publication_key = %s",
            (publication_key,),
        ) == (1,)
    finally:
        connector.close()


@pytest.mark.parametrize("predecessor", ("candidate", "build"))
def test_catalog_publication_cleanup_preserves_live_predecessor_base(
    tmp_path: Path,
    predecessor: str,
) -> None:
    connector = _database(tmp_path / f"catalog-live-{predecessor}.sqlite3")
    try:
        publication_key, old_receipt, _current_receipt = (
            _seed_catalog_publication_cleanup_fixture(connector)
        )
        if predecessor == "candidate":
            base_table = "catalog_publication_candidate_base_publication_commits"
            key_column = "candidate_id"
            working_table = "operational_catalog_working_candidates"
            identity_value = b"c" * 16
        else:
            base_table = "catalog_source_build_base_publication_commits"
            key_column = "build_id"
            working_table = "operational_source_working_builds"
            identity_value = b"b" * 16
        _fixture_rows(
            connector,
            [
                (
                    f"INSERT INTO {base_table} ({key_column}, base_receipt_id) "
                    "VALUES (%s, %s)",
                    (identity_value, old_receipt),
                ),
                (
                    f"INSERT INTO {working_table} (slot, {key_column}, assigned_at) "
                    "VALUES (1, %s, 1)",
                    (identity_value,),
                ),
            ],
        )
        gate = _exclusive(connector)
        blocked = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
        )
        assert all(result.row_count == 0 for result in _drain(connector, gate, blocked))
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publications "
            "WHERE revision = 1 AND publication_key = %s",
            (publication_key,),
        ) == (1,)

        connector.execute(f"DELETE FROM {working_table}")
        released = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_PUBLICATION,
            publication_key[0],
            max_rows=1,
            now=20,
        )
        _drain(connector, gate, released, now=21)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publications "
                "WHERE revision = 1 AND publication_key = %s",
                (publication_key,),
            )
            == ()
        )
    finally:
        connector.close()


def test_first_vertical_batch_cleanup_is_exactly_child_first() -> None:
    source = cleanup_module._STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD].phases
    assert tuple(spec.table for spec in source["SB_GALLERY"])[2:] == (
        "catalog_build_manifest_seals",
        "catalog_build_manifest_manifest_sha256s",
        "catalog_build_manifest_file_counts",
        "catalog_build_manifest_byte_counts",
        "catalog_build_manifest_anchors",
        "catalog_source_build_galleries",
    )
    assert tuple(spec.table for spec in source["SB_DISCOVERY_SEAL"]) == (
        "catalog_source_build_discovery_seals",
    )
    assert tuple(spec.table for spec in source["SB_DISCOVERY_VALUES"]) == (
        "catalog_source_build_discovery_scan_attempts",
        "catalog_source_build_discovery_gallery_counts",
        "catalog_source_build_discovery_tree_observation_sha256s",
        "catalog_source_build_discovery_completed_ats",
    )
    assert tuple(spec.table for spec in source["SB_DISCOVERY_ANCHOR"]) == (
        "catalog_source_build_discovery_anchors",
    )
    assert source["SB_ROOT"][0].delete_sql == (
        "DELETE FROM catalog_source_build_sealed_ats WHERE build_id = %s",
        "DELETE FROM catalog_source_build_descriptor_seals WHERE build_id = %s",
        "DELETE FROM catalog_source_build_states WHERE build_id = %s",
        "DELETE FROM catalog_source_build_created_ats WHERE build_id = %s",
        "DELETE FROM catalog_source_build_manifest_policy_ids WHERE build_id = %s",
        "DELETE FROM catalog_source_build_scope_keys WHERE build_id = %s",
        "DELETE FROM catalog_source_build_anchors WHERE build_id = %s",
    )

    analysis = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases
    assert tuple(spec.table for spec in analysis["AR_OVERLAY"]) == (
        _ANALYSIS_OVERLAY_TABLES
    )
    assert tuple(spec.table for spec in analysis["AR_FILE_HASH_VALUES"]) == (
        _ANALYSIS_FILE_HASH_VALUE_TABLES
    )
    assert tuple(spec.table for spec in analysis["AR_IMPACT_PROVENANCE"]) == (
        _ANALYSIS_IMPACT_PROVENANCE_TABLES
    )
    assert tuple(spec.table for spec in analysis["AR_FILE_HASH_ANCHOR"]) == (
        _ANALYSIS_FILE_HASH_ANCHOR_TABLES
    )

    canonical = cleanup_module._STATIC_PLANS[CleanupTargetKind.CANONICAL_VALUE].phases
    assert tuple(spec.table for spec in canonical["CV_DICTIONARY"]) == (
        "catalog_display_title_choices",
        "catalog_title_sorts",
        "catalog_source_scope_anchors",
        "catalog_source_locator_identity",
        "catalog_tag_term_anchors",
        "catalog_source_snapshot_manifest_identity_anchors",
        "catalog_artifact_policies",
        "catalog_artifact_semantic_inputs",
    )
    source_scope = canonical["CV_DICTIONARY"][2]
    assert source_scope.primary_key == ("scope_key",)
    assert (
        "JOIN catalog_source_scope_source_root_sha256s AS scope_root"
        in source_scope.source
    )
    assert source_scope.delete_sql == (
        "DELETE FROM catalog_source_scope_seals WHERE scope_key = %s",
        "DELETE FROM catalog_source_scope_identities WHERE scope_key = %s",
        "DELETE FROM catalog_source_scope_identity_policy_versions "
        "WHERE scope_key = %s",
        "DELETE FROM catalog_source_scope_source_providers WHERE scope_key = %s",
        "DELETE FROM catalog_source_scope_source_root_sha256s WHERE scope_key = %s",
        "DELETE FROM catalog_source_scope_anchors WHERE scope_key = %s",
    )
    tag_term = canonical["CV_DICTIONARY"][4]
    assert tag_term.delete_sql == (
        "DELETE FROM catalog_tag_term_seals WHERE tag_id = %s",
        "DELETE FROM catalog_tag_term_identities WHERE tag_id = %s",
        "DELETE FROM catalog_tag_term_anchors WHERE tag_id = %s",
    )
    assert tuple(spec.table for spec in canonical["CV_SEMANTIC_LINK"]) == (
        "catalog_artifact_policy_semantics_seals",
        "catalog_artifact_policy_semantics_identities",
        "catalog_artifact_policy_semantics_producer_fingerprint_sha256s",
        "catalog_artifact_policy_semantics_max_image_short_sides",
        "catalog_artifact_policy_semantics_artifact_algorithm_versions",
        "catalog_artifact_policy_semantics_anchors",
    )
    assert canonical["CV_DICTIONARY"][-1].delete_sql == (
        "DELETE FROM catalog_artifact_semantic_inputs "
        "WHERE artifact_semantics_sha256 = %s",
    )

    observation = cleanup_module._STATIC_PLANS[
        CleanupTargetKind.GALLERY_OBSERVATION
    ].phases
    assert tuple(spec.table for spec in observation["GO_FILESYSTEM_SEAL"]) == (
        "catalog_gallery_observation_file_filesystem_seals",
    )
    assert tuple(spec.table for spec in observation["GO_FILESYSTEM_VALUES"]) == (
        "catalog_gallery_observation_file_filesystem_devices",
        "catalog_gallery_observation_file_filesystem_inodes",
        "catalog_gallery_observation_file_filesystem_modified_nses",
        "catalog_gallery_observation_file_filesystem_changed_nses",
    )
    assert tuple(spec.table for spec in observation["GO_FILESYSTEM_ANCHOR"]) == (
        "catalog_gallery_observation_file_filesystem_anchors",
    )
    assert tuple(spec.table for spec in observation["GO_OBSERVATION_FACTS"]) == (
        "catalog_gallery_observation_metadata_locals",
        "catalog_gallery_observation_directories",
        "catalog_gallery_observation_stat",
        "catalog_gallery_observation_scans",
    )


def test_source_analysis_and_candidate_strategies_delete_child_first(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "rooted-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)

        build_id = bytes((17,)) + b"b" * 15
        upload_value = b"u" * 32
        scope_key = _seed_source_build_scope(connector, discriminator=17)
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            state="ABANDONED",
            created_at=0,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (1, 0, 1),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (build_id, 1),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_anchors "
                    "(build_id) VALUES (%s)",
                    (build_id,),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_scan_attempts "
                    "(build_id, scan_attempt) VALUES (%s, %s)",
                    (build_id, b"d" * 16),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_gallery_counts "
                    "(build_id, gallery_count) VALUES (%s, 1)",
                    (build_id,),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_tree_observation_sha256s "
                    "(build_id, tree_observation_sha256) VALUES (%s, %s)",
                    (build_id, b"t" * 32),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_completed_ats "
                    "(build_id, completed_at) VALUES (%s, 1)",
                    (build_id,),
                ),
                (
                    "INSERT INTO catalog_source_build_discovery_seals "
                    "(build_id) VALUES (%s)",
                    (build_id,),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (%s, %s)",
                    (1, upload_value),
                ),
                (
                    "INSERT INTO catalog_source_build_expected_gallery "
                    "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
                    (build_id, 0, 1),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    (build_id, 1, 1),
                ),
            ],
        )
        source_cycle = _begin(
            connector, gate, CleanupTargetKind.SOURCE_BUILD, 17, max_rows=1
        )
        source_results = _drain(connector, gate, source_cycle)
        assert source_results[-1].deleted_count == 11
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_builds WHERE build_id = %s", (build_id,)
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = 1 AND value_sha256 = %s",
                (upload_value,),
            )
            == ()
        )
        assert _source_build_discovery_rows(connector) == ([], [], [], [], [], [])

        analysis_id = bytes((18,)) + b"a" * 15
        seed_analysis_policy(connector)
        seed_source_build(
            connector,
            build_id=b"z" * 16,
            scope_key=scope_key,
            manifest_policy_id=1,
            state="SEALED",
            created_at=0,
            sealed_at=0,
        )
        seed_analysis_run(
            connector,
            analysis_id=analysis_id,
            build_id=b"z" * 16,
            policy_id=1,
            input_manifest_sha256=b"m" * 32,
            started_at=0,
            state="ABANDONED",
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_source_revision_provenance "
                    "(source_revision, analysis_id) VALUES (%s, %s)",
                    (9, analysis_id),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_galleries "
                    "(analysis_id, gallery_id) VALUES (%s, %s)",
                    (analysis_id, 1),
                ),
                (
                    "INSERT INTO catalog_a_impacted_gid_provenance_storage "
                    "(analysis_id, gallery_id) VALUES (%s, %s)",
                    (analysis_id, 1),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_gid_storage "
                    "(analysis_id, gid) VALUES (%s, %s)",
                    (analysis_id, 7),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_anchors "
                    "(analysis_id, stage) VALUES (%s, %s)",
                    (analysis_id, b"changed_gallery"),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_generations "
                    "(analysis_id, stage, generation) VALUES (%s, %s, %s)",
                    (analysis_id, b"changed_gallery", 1),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_cursors "
                    "(analysis_id, stage, cursor) VALUES (%s, %s, %s)",
                    (analysis_id, b"changed_gallery", b""),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_processed_counts "
                    "(analysis_id, stage, processed_count) VALUES (%s, %s, %s)",
                    (analysis_id, b"changed_gallery", 0),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_states "
                    "(analysis_id, stage, state) VALUES (%s, %s, 'OPEN')",
                    (analysis_id, b"changed_gallery"),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_updated_ats "
                    "(analysis_id, stage, updated_at) VALUES (%s, %s, %s)",
                    (analysis_id, b"changed_gallery", 0),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoint_seals "
                    "(analysis_id, stage) VALUES (%s, %s)",
                    (analysis_id, b"changed_gallery"),
                ),
                (
                    "INSERT INTO catalog_analysis_state_ancestry "
                    "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                    "VALUES (%s, %s, %s)",
                    (analysis_id, 0, analysis_id),
                ),
            ],
        )
        analysis_cycle = _begin(
            connector, gate, CleanupTargetKind.ANALYSIS_RUN, 18, max_rows=2
        )
        analysis_results = _drain(connector, gate, analysis_cycle, now=50)
        assert analysis_results[-1].deleted_count == 13
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_analysis_runs WHERE analysis_id = %s",
                (analysis_id,),
            )
            == ()
        )
        for table in (
            "catalog_analysis_run_completed_ats",
            "catalog_analysis_run_descriptor_seals",
            "catalog_analysis_run_identities",
            "catalog_analysis_run_started_ats",
            "catalog_analysis_run_input_manifest_sha256s",
            "catalog_analysis_run_policy_ids",
            "catalog_analysis_run_build_ids",
            "catalog_analysis_run_states",
            "catalog_analysis_run_anchors",
        ):
            assert (
                connector.fetch_one(
                    f"SELECT 1 FROM {table} WHERE analysis_id = %s",
                    (analysis_id,),
                )
                == ()
            )

        candidate_id = bytes((19,)) + b"c" * 15
        gid = 19
        publication_key = identity.publication_key(gid)
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        selection_occurrence = identity.publication_selection_occurrence_sha256(
            candidate_id, publication_key
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (2, %s, %s, %s)",
                    (b"g" * 32, b"s" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                    "VALUES (%s, 0)",
                    (gid,),
                ),
                (
                    "INSERT INTO catalog_source_gallery_name_gids "
                    "(source_gallery_name, gid) VALUES (%s, %s)",
                    (b"candidate-gallery", gid),
                ),
                (
                    "INSERT INTO catalog_gallery_source_name_accesses "
                    "(gallery_id, source_gallery_name) VALUES (2, %s)",
                    (b"candidate-gallery",),
                ),
                (
                    "INSERT INTO catalog_publication_identities (publication_key, gid) "
                    "VALUES (%s, %s)",
                    (publication_key, gid),
                ),
                (
                    "INSERT INTO catalog_artifact_operations "
                    "(candidate_id, publication_key, operation) "
                    "VALUES (%s, %s, 'DELETE')",
                    (candidate_id, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_selection_occurrence_identities "
                    "(selection_occurrence_sha256, candidate_id, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (selection_occurrence, candidate_id, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_selection_storage "
                    "(selection_occurrence_sha256, gallery_id) VALUES (%s, 2)",
                    (selection_occurrence,),
                ),
                (
                    "INSERT INTO catalog_publication_candidate_base_publication_commits "
                    "(candidate_id, base_receipt_id) VALUES (%s, %s)",
                    (candidate_id, b"r" * 16),
                ),
            ],
        )
        candidate_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            19,
            max_rows=1,
        )
        candidate_results = _drain(connector, gate, candidate_cycle, now=100)
        assert candidate_results[-1].deleted_count == 12
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publication_selection_storage "
                "WHERE selection_occurrence_sha256 = %s",
                (selection_occurrence,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 "
                "FROM catalog_publication_selection_occurrence_identities "
                "WHERE selection_occurrence_sha256 = %s",
                (selection_occurrence,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publication_candidates WHERE candidate_id = %s",
                (candidate_id,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT gid FROM catalog_publication_identities WHERE publication_key = %s",
            (publication_key,),
        ) == (gid,)
    finally:
        connector.close()


@pytest.mark.parametrize("state", ("PENDING", "PREPARED"))
def test_candidate_cleanup_retains_unresolved_protection_families(
    tmp_path: Path,
    state: str,
) -> None:
    connector = _database(tmp_path / f"candidate-{state.lower()}-cleanup.sqlite3")
    try:
        candidate_id = bytes((20,)) + state.encode("ascii")[:1] * 15
        publication_key = state.encode("ascii")[:1] * 32
        artifact_sha256 = state.encode("ascii")[-1:] * 32
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _seed_prepared_artifact_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=artifact_sha256,
            state=state,
        )
        before = _prepared_artifact_family_rows(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
        )
        assert (
            VNextCleanupRepository.current_only_maintenance_state(
                VNextUnitOfWork(connector, backend="sqlite"),
                cycle_cutoff_at=100,
            )
            is CatalogPublicationMaintenanceState.BLOCKED
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            20,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidates "
            "WHERE candidate_id = %s",
            (candidate_id,),
        ) == (candidate_id,)
        assert (
            _prepared_artifact_family_rows(
                connector,
                candidate_id=candidate_id,
                publication_key=publication_key,
            )
            == before
        )

        connector.execute(
            "UPDATE catalog_prepared_artifacts SET state = 'COMMITTED' "
            "WHERE candidate_id = %s AND publication_key = %s",
            (candidate_id, publication_key),
        )
        assert (
            VNextCleanupRepository.current_only_maintenance_state(
                VNextUnitOfWork(connector, backend="sqlite"),
                cycle_cutoff_at=100,
            )
            is CatalogPublicationMaintenanceState.ACTIONABLE
        )
        released = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            20,
            max_rows=32,
            now=100,
        )
        _drain(connector, gate, released, now=101)
        assert not any(_candidate_definition_rows(connector, candidate_id=candidate_id))
    finally:
        connector.close()


def test_candidate_cleanup_deletes_committed_prepared_family_child_first(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-committed-cleanup.sqlite3")
    try:
        candidate_id = bytes((21,)) + b"c" * 15
        publication_key = b"p" * 32
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _seed_prepared_artifact_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=b"a" * 32,
            state="COMMITTED",
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            21,
            max_rows=32,
        )
        family_deletes: list[str] = []
        original_execute_affected = connector.execute_affected

        def record_delete(sql: str, data: tuple[Any, ...] = ()) -> int:
            statement = sql.lstrip()
            if statement.startswith("DELETE FROM catalog_prepared_artifacts"):
                family_deletes.append(statement.split()[2])
            return original_execute_affected(sql, data)

        with patch.object(connector, "execute_affected", side_effect=record_delete):
            results = _drain(connector, gate, cycle)
        assert family_deletes == ["catalog_prepared_artifacts"]
        assert results[-1].deleted_count == 9
        assert _prepared_artifact_family_rows(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
        ) == ([],)
    finally:
        connector.close()


def test_candidate_cleanup_removes_uncommitted_reserved_catalog_projection(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-reserved-projection-cleanup.sqlite3")
    try:
        candidate_id = bytes((29,)) + b"c" * 15
        revision = 29
        publication_key = b"p" * 32
        occurrence = identity.catalog_publication_occurrence_sha256(
            revision, publication_key
        )
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _fixture_rows(
            connector,
            [
                (
                    "UPDATE catalog_publication_candidate_reserved_revisions "
                    "SET reserved_revision = %s WHERE candidate_id = %s",
                    (revision, candidate_id),
                )
            ],
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (occurrence, revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_storage "
                    "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
                    "language_sha256, modified_at, source_title_sha256) "
                    "VALUES (%s, 1, %s, %s, 1, %s)",
                    (occurrence, b"s" * 32, b"l" * 32, b"t" * 32),
                ),
                (
                    "INSERT INTO catalog_publication_order "
                    "(revision, position, publication_key) VALUES (%s, 0, %s)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_contents "
                    "(revision, publication_key, content_sha256) "
                    "VALUES (%s, %s, %s)",
                    (revision, publication_key, b"c" * 32),
                ),
                (
                    "INSERT INTO catalog_contributor_anchors "
                    "(revision, publication_key, position) VALUES (%s, %s, 0)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_contributor_name_sha256s "
                    "(revision, publication_key, position, contributor_name_sha256) "
                    "VALUES (%s, %s, 0, %s)",
                    (revision, publication_key, b"n" * 32),
                ),
                (
                    "INSERT INTO catalog_contributor_roles "
                    "(revision, publication_key, position, role) "
                    "VALUES (%s, %s, 0, %s)",
                    (revision, publication_key, b"artist"),
                ),
                (
                    "INSERT INTO catalog_contributor_identities "
                    "(revision, publication_key, contributor_name_sha256, "
                    "role, position) VALUES (%s, %s, %s, %s, 0)",
                    (revision, publication_key, b"n" * 32, b"artist"),
                ),
                (
                    "INSERT INTO catalog_contributor_seals "
                    "(revision, publication_key, position) VALUES (%s, %s, 0)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_subjects "
                    "(revision, publication_key, position, tag_id) "
                    "VALUES (%s, %s, 0, 1)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_artifacts "
                    "(revision, publication_key, artifact_sha256, "
                    "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
                    (revision, publication_key, b"a" * 32, b"m" * 32),
                ),
            ],
        )

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            candidate_id[0],
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES:
            assert connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)
        assert not any(_candidate_definition_rows(connector, candidate_id=candidate_id))
    finally:
        connector.close()


def test_candidate_cleanup_has_no_partial_prepared_row_surface(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-partial-prepared.sqlite3")
    try:
        candidate_id = bytes((22,)) + b"c" * 15
        publication_key = b"p" * 32
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        with pytest.raises(DatabaseDuplicateKeyError):
            connector.execute(
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key) VALUES (%s, %s)",
                (candidate_id, publication_key),
            )
        assert not connector.fetch_one("SELECT 1 FROM catalog_prepared_artifacts")
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            22,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 8
    finally:
        connector.close()


def test_candidate_prepared_row_delete_fault_rolls_back_atomically(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-prepared-delete-faults.sqlite3")
    try:
        candidate_id = bytes((23,)) + b"c" * 15
        publication_key = b"p" * 32
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _seed_prepared_artifact_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=b"a" * 32,
            state="COMMITTED",
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            23,
            max_rows=32,
        )
        transitioned = _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PC_PREPARED",
        )
        assert transitioned.generation == 1

        family_before = _prepared_artifact_family_rows(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
        )
        protocol_before = _cleanup_protocol_snapshot(connector)
        value_tables = ("catalog_prepared_artifacts",)
        original_execute_affected = connector.execute_affected
        for failed_table in value_tables:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected prepared family delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(connector, "execute_affected", side_effect=fail_delete),
                pytest.raises(RuntimeError, match="prepared family delete fault"),
            ):
                _advance(connector, gate, cycle, 1, b"v" * 32, now=5)
            assert triggered, failed_table
            assert (
                _prepared_artifact_family_rows(
                    connector,
                    candidate_id=candidate_id,
                    publication_key=publication_key,
                )
                == family_before
            )
            assert _cleanup_protocol_snapshot(connector) == protocol_before
    finally:
        connector.close()


def test_candidate_definition_cleanup_is_seal_last_reversed_and_atomic(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-definition-delete-faults.sqlite3")
    try:
        candidate_id = bytes((24,)) + b"c" * 15
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            24,
            max_rows=32,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PC_ROOT")
        family_before = _candidate_definition_rows(
            connector,
            candidate_id=candidate_id,
        )
        assert all(rows for rows in family_before)
        protocol_before = _cleanup_protocol_snapshot(connector)
        original_execute_affected = connector.execute_affected

        for failed_table in _CANDIDATE_DEFINITION_DELETE_ORDER:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected candidate definition delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(connector, "execute_affected", side_effect=fail_delete),
                pytest.raises(RuntimeError, match="candidate definition delete fault"),
            ):
                _advance(connector, gate, cycle, 1, b"f" * 32, now=100)
            assert triggered, failed_table
            assert (
                _candidate_definition_rows(connector, candidate_id=candidate_id)
                == family_before
            )
            assert _cleanup_protocol_snapshot(connector) == protocol_before

        deleted: list[str] = []

        def record_delete(sql: str, data: tuple[Any, ...] = ()) -> int:
            statement = sql.lstrip()
            if statement.startswith("DELETE FROM catalog_publication_candidate_"):
                deleted.append(statement.split()[2])
            return original_execute_affected(sql, data)

        with patch.object(connector, "execute_affected", side_effect=record_delete):
            result = _advance(connector, gate, cycle, 1, b"g" * 32, now=101)
        assert result.phase == "PC_ROOT" and result.row_count == 8
        assert tuple(deleted) == _CANDIDATE_DEFINITION_DELETE_ORDER
        assert _candidate_definition_rows(
            connector,
            candidate_id=candidate_id,
        ) == tuple([] for _ in _CANDIDATE_DEFINITION_DELETE_ORDER)
    finally:
        connector.close()


def test_source_build_cleanup_explicitly_rejects_open_family(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "open-source-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        build_id = bytes((51,)) + b"o" * 15
        scope_key = _seed_source_build_scope(connector, discriminator=51)
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            state="OPEN",
            created_at=1,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_BUILD,
            51,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (build_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_anchors WHERE build_id = %s",
            (build_id,),
        ) == (build_id,)
    finally:
        connector.close()


def test_source_build_cleanup_deletes_sealed_build_manifest_child_first(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "sealed-source-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        build_id = bytes((52,)) + b"s" * 15
        scope_key = _seed_source_build_scope(connector, discriminator=52)
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            state="SEALED",
            created_at=1,
            sealed_at=2,
        )
        seed_build_manifest(
            connector,
            build_id=build_id,
            manifest_sha256=b"m" * 32,
            gallery_count=0,
            file_count=0,
            byte_count=0,
            computed_at=2,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_BUILD,
            52,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        for table in (
            "catalog_build_manifest_seals",
            "catalog_build_manifest_manifest_sha256s",
            "catalog_build_manifest_file_counts",
            "catalog_build_manifest_byte_counts",
            "catalog_build_manifest_anchors",
            "catalog_source_build_anchors",
        ):
            assert (
                connector.fetch_all(
                    f"SELECT 1 FROM {table} WHERE build_id = %s",
                    (build_id,),
                )
                == []
            )
    finally:
        connector.close()


def test_canonical_cleanup_deletes_snapshot_manifest_family_before_identity(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "snapshot-manifest-cleanup.sqlite3")
    try:
        snapshot = bytes((53,)) + b"m" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=snapshot,
            page_sha256=b"p" * 32,
            digest_domain=b"source_snapshot_manifest_v1",
        )
        seed_snapshot_manifest(
            connector,
            snapshot_manifest_sha256=snapshot,
            gallery_count=0,
            file_count=0,
            byte_count=0,
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            53,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        for table, key_column in (
            (
                "catalog_source_snapshot_manifest_identity_seals",
                "snapshot_manifest_sha256",
            ),
            (
                "catalog_source_snapshot_manifest_identity_gallery_counts",
                "snapshot_manifest_sha256",
            ),
            (
                "catalog_source_snapshot_manifest_identity_file_counts",
                "snapshot_manifest_sha256",
            ),
            (
                "catalog_source_snapshot_manifest_identity_byte_counts",
                "snapshot_manifest_sha256",
            ),
            (
                "catalog_source_snapshot_manifest_identity_anchors",
                "snapshot_manifest_sha256",
            ),
            ("catalog_canonical_value_allocation_anchors", "value_sha256"),
        ):
            assert (
                connector.fetch_all(
                    f"SELECT 1 FROM {table} WHERE {key_column} = %s",
                    (snapshot,),
                )
                == []
            )
    finally:
        connector.close()


def test_operational_preparation_cleanup_preserves_activated_effects(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "preparation-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        activated = bytes((20,)) + b"a" * 15
        abandoned = bytes((20,)) + b"b" * 15
        unactivated = bytes((20,)) + b"c" * 15
        active_event = b"A" * 16
        abandoned_event = b"B" * 16
        _fixture_rows(
            connector,
            [
                *[
                    (
                        "INSERT INTO operational_operational_event_streams "
                        "(preparation_id, created_at) VALUES (%s, 0)",
                        (preparation,),
                    )
                    for preparation in (activated, abandoned, unactivated)
                ],
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 0, 1)",
                    (activated, b"1" * 16),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'ABANDONED', 0, 1)",
                    (abandoned, b"2" * 16),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 0, 1)",
                    (unactivated, b"3" * 16),
                ),
                *[
                    (
                        "INSERT INTO operational_operational_preparation_effect_seals "
                        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                        "VALUES (%s, 1, %s, 1)",
                        (preparation, bytes((index,)) * 32),
                    )
                    for index, preparation in enumerate(
                        (activated, abandoned, unactivated), start=1
                    )
                ],
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 1, 'REMOVED_GID', %s, 1)",
                    (active_event, activated, b"e" * 32),
                ),
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 1, 'REMOVED_GID', %s, 1)",
                    (abandoned_event, abandoned, b"f" * 32),
                ),
                *[
                    (
                        "INSERT INTO operational_operational_removed_gid_events "
                        "(event_id, gid, request_token) VALUES (%s, %s, %s)",
                        (event, gid, token),
                    )
                    for event, gid, token in (
                        (active_event, 1, b"r" * 16),
                        (abandoned_event, 2, b"s" * 16),
                    )
                ],
                (
                    "INSERT INTO catalog_publication_commit_operational_preparations "
                    "(receipt_id, preparation_id) VALUES (%s, %s)",
                    (b"r" * 16, activated),
                ),
                (
                    "INSERT INTO operational_operational_preparation_checkpoints "
                    "(preparation_id, phase, generation, cursor_bytes, "
                    "processed_count, chain_sha256, state, updated_at) "
                    "VALUES (%s, 'REMOVED_GID', 1, %s, 1, %s, 'COMPLETE', 1)",
                    (activated, b"", b"k" * 32),
                ),
            ],
        )

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.OPERATIONAL_PREPARATION,
            20,
            max_rows=1,
        )
        _drain(connector, gate, cycle)

        assert connector.fetch_all(
            "SELECT preparation_id FROM operational_operational_preparations "
            "ORDER BY preparation_id"
        ) == [(unactivated,)]
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_event_streams "
            "WHERE preparation_id = %s",
            (activated,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_preparation_effect_seals "
            "WHERE preparation_id = %s",
            (activated,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_events WHERE event_id = %s",
            (active_event,),
        ) == (1,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_operational_event_streams "
                "WHERE preparation_id = %s",
                (abandoned,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_operational_events WHERE event_id = %s",
                (abandoned_event,),
            )
            == ()
        )
    finally:
        connector.close()


def test_staging_compaction_and_observation_orphan_cleanup_are_separate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "observation-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        staging_id = bytes((21,)) + b"s" * 15
        build_id = b"b" * 16
        request = b"r" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) VALUES (21, 1, 0)",
                    (),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 21, 1, 'SEALED', 0, 1)",
                    (staging_id, build_id),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 21, 1)",
                    (build_id,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
                    "(request_sha256) VALUES (%s)",
                    (request,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (request, staging_id),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_chunks "
                    "(request_sha256, position, request_bytes) VALUES (%s, 0, %s)",
                    (request, b"frame"),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (staging_id,),
                ),
            ],
        )
        staging_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            21,
            max_rows=1,
        )
        _drain(connector, gate, staging_cycle)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_gallery_observation_stagings "
                "WHERE staging_id = %s",
                (staging_id,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = 21",
            (build_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_allocations "
            "WHERE gallery_id = 21 AND observation_id = 1"
        ) == (1,)

        reused_staging = bytes((30,)) + b"u" * 15
        reused_build = b"c" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) VALUES (24, 2, 0)",
                    (),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 24, 2, 'REUSED', 0, 1)",
                    (reused_staging, reused_build),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (reused_staging,),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 24, 1)",
                    (reused_build,),
                ),
                (
                    "INSERT INTO catalog_gallery_observations "
                    "(gallery_id, observation_id, observation_identity_sha256) "
                    "VALUES (24, 2, %s)",
                    (b"o" * 32,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_stat "
                    "(gallery_id, observation_id, file_count, byte_count) "
                    "VALUES (24, 2, 1, 7)",
                    (),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_anchors "
                    "(gallery_id, observation_id, file_key) VALUES (24, 2, %s)",
                    (b"n" * 32,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_file_nos "
                    "(gallery_id, observation_id, file_key, file_no) "
                    "VALUES (24, 2, %s, 0)",
                    (b"n" * 32,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_file_sha256s "
                    "(gallery_id, observation_id, file_key, file_sha256) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"f" * 32),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_seals "
                    "(gallery_id, observation_id, file_key) VALUES (24, 2, %s)",
                    (b"n" * 32,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_directories "
                    "(gallery_id, observation_id, directory_entry_count, "
                    "directory_observation_sha256) VALUES (24, 2, 1, %s)",
                    (b"d" * 32,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_scans "
                    "(gallery_id, observation_id, scan_observation_sha256, "
                    "scan_observation_version, source_file_count) "
                    "VALUES (24, 2, %s, 1, 1)",
                    (b"s" * 32,),
                ),
                (
                    "INSERT INTO "
                    "catalog_gallery_observation_file_filesystem_anchors "
                    "(gallery_id, observation_id, file_key) VALUES (24, 2, %s)",
                    (b"n" * 32,),
                ),
                (
                    "INSERT INTO "
                    "catalog_gallery_observation_file_filesystem_devices "
                    "(gallery_id, observation_id, file_key, device) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"1" * 8),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_filesystem_inodes "
                    "(gallery_id, observation_id, file_key, inode) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"2" * 8),
                ),
                (
                    "INSERT INTO "
                    "catalog_gallery_observation_file_filesystem_modified_nses "
                    "(gallery_id, observation_id, file_key, modified_ns) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"3" * 8),
                ),
                (
                    "INSERT INTO "
                    "catalog_gallery_observation_file_filesystem_changed_nses "
                    "(gallery_id, observation_id, file_key, changed_ns) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"4" * 8),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_file_filesystem_seals "
                    "(gallery_id, observation_id, file_key) VALUES (24, 2, %s)",
                    (b"n" * 32,),
                ),
            ],
        )
        seed_manifest_policy(connector)
        seed_gallery_manifest(
            connector,
            gallery_id=24,
            observation_id=2,
            manifest_policy_id=1,
            manifest_sha256=b"m" * 32,
            computed_at=1,
        )
        observation_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION,
            24,
            max_rows=1,
        )
        _drain(connector, gate, observation_cycle, now=100)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_allocations "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = 24",
            (reused_build,),
        ) == (1,)
        vertical_rows = _observation_vertical_rows(connector)
        assert len(vertical_rows) == 9
        assert all(rows == [] for rows in vertical_rows)
        for table in ("catalog_gallery_manifests",):
            assert (
                connector.fetch_all(
                    f"SELECT 1 FROM {table} WHERE gallery_id = 24 AND observation_id = 2"
                )
                == []
            )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_gallery_observation_directories "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_gallery_observation_stat "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_gallery_observation_scans "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_gallery_observation_file_filesystem "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == []
        )
    finally:
        connector.close()


def test_observation_cleanup_keeps_shared_metadata_for_other_observations_and_locations(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "metadata-reachability-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        source_name = b"shared-gallery"
        gid = 9_001
        _fixture_rows(
            connector,
            [
                *[
                    (
                        "INSERT INTO catalog_gallery_observation_allocations "
                        "(gallery_id, observation_id, allocated_at) "
                        "VALUES (%s, %s, 0)",
                        pair,
                    )
                    for pair in ((42, 1), (42, 2), (298, 1))
                ],
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 42, 2)",
                    (b"a" * 16,),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 298, 1)",
                    (b"b" * 16,),
                ),
                (
                    "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                    "VALUES (%s, 10)",
                    (gid,),
                ),
                (
                    "INSERT INTO catalog_source_gallery_name_gids "
                    "(source_gallery_name, gid) VALUES (%s, %s)",
                    (source_name, gid),
                ),
                *[
                    (
                        "INSERT INTO catalog_gallery_source_name_accesses "
                        "(gallery_id, source_gallery_name) VALUES (%s, %s)",
                        (gallery_id, source_name),
                    )
                    for gallery_id in (42, 298)
                ],
                *[
                    (
                        "INSERT INTO catalog_gallery_observation_metadata_locals "
                        "(gallery_id, observation_id, download_time, modified_time) "
                        "VALUES (%s, %s, %s, %s)",
                        (*pair, download_time, modified_time),
                    )
                    for pair, download_time, modified_time in (
                        ((42, 1), 11, 21),
                        ((42, 2), 12, 22),
                        ((298, 1), 13, 23),
                    )
                ],
            ],
        )

        observation_phases = cleanup_module._STATIC_PLANS[
            CleanupTargetKind.GALLERY_OBSERVATION
        ].phases
        assert tuple(
            spec.table for spec in observation_phases["GO_OBSERVATION_FACTS"]
        ) == (
            "catalog_gallery_observation_metadata_locals",
            "catalog_gallery_observation_directories",
            "catalog_gallery_observation_stat",
            "catalog_gallery_observation_scans",
        )
        assert not {
            "catalog_gallery_upload_times",
            "catalog_source_gallery_name_gids",
            "catalog_gallery_source_name_accesses",
        }.intersection(
            spec.table for specs in observation_phases.values() for spec in specs
        )

        source_name_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
            source_name[0],
            max_rows=8,
        )
        assert _drain(connector, gate, source_name_cycle)[-1].deleted_count == 0
        upload_time_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
        )
        assert _drain(connector, gate, upload_time_cycle)[-1].deleted_count == 0

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION,
            42,
            max_rows=8,
        )
        _drain(connector, gate, cycle, now=100)

        assert connector.fetch_all(
            "SELECT gallery_id, observation_id "
            "FROM catalog_gallery_observation_metadata_locals "
            "ORDER BY gallery_id, observation_id"
        ) == [(42, 2), (298, 1)]
        assert connector.fetch_all(
            "SELECT gallery_id, observation_id, gid, upload_time, download_time, "
            "modified_time FROM catalog_gallery_observation_metadata "
            "ORDER BY gallery_id, observation_id"
        ) == [
            (42, 2, gid, 10, 12, 22),
            (298, 1, gid, 10, 13, 23),
        ]
        assert connector.fetch_all(
            "SELECT gid, upload_time FROM catalog_gallery_upload_times"
        ) == [(gid, 10)]
        assert connector.fetch_all(
            "SELECT source_gallery_name, gid FROM catalog_source_gallery_name_gids"
        ) == [(source_name, gid)]
        assert connector.fetch_all(
            "SELECT gallery_id, source_gallery_name "
            "FROM catalog_gallery_source_name_accesses ORDER BY gallery_id"
        ) == [(42, source_name), (298, source_name)]
        assert connector.fetch_all(
            "SELECT gallery_id, observation_id "
            "FROM catalog_gallery_observation_allocations "
            "ORDER BY gallery_id, observation_id"
        ) == [(42, 2), (298, 1)]
    finally:
        connector.close()


def test_gallery_upload_time_cleanup_honors_analysis_impacted_gid_storage(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "upload-time-analysis-gid-retention.sqlite3")
    try:
        gid = 9_101
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
            "VALUES (%s, 10)",
            (gid,),
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_analysis_impacted_gid_storage "
                    "(analysis_id, gid) VALUES (%s, %s)",
                    (b"a" * 16, gid),
                )
            ],
        )
        gate = _exclusive(connector)
        retained = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
        )
        assert _drain(connector, gate, retained)[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT upload_time FROM catalog_gallery_upload_times WHERE gid = %s",
            (gid,),
        ) == (10,)

        connector.execute(
            "DELETE FROM catalog_analysis_impacted_gid_storage WHERE gid = %s",
            (gid,),
        )
        reclaim = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
            now=100,
        )
        assert _drain(connector, gate, reclaim, now=101)[-1].deleted_count == 1
        assert (
            connector.fetch_one(
                "SELECT upload_time FROM catalog_gallery_upload_times WHERE gid = %s",
                (gid,),
            )
            == ()
        )
    finally:
        connector.close()


def test_shared_metadata_cleanup_follows_identity_name_and_gid_reachability(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "shared-metadata-cleanup-order.sqlite3")
    try:
        gate = _exclusive(connector)
        source_name = b"a"
        gid = 321
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                    "VALUES (%s, 10)",
                    (gid,),
                ),
                (
                    "INSERT INTO catalog_source_gallery_name_gids "
                    "(source_gallery_name, gid) VALUES (%s, %s)",
                    (source_name, gid),
                ),
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (65, %s, %s, %s)",
                    (b"g" * 32, b"s" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO catalog_gallery_source_name_accesses "
                    "(gallery_id, source_gallery_name) VALUES (65, %s)",
                    (source_name,),
                ),
            ],
        )

        blocked_name = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
            source_name[0],
            max_rows=8,
        )
        assert _drain(connector, gate, blocked_name)[-1].deleted_count == 0
        blocked_upload = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
        )
        assert _drain(connector, gate, blocked_upload)[-1].deleted_count == 0

        identity = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_IDENTITY,
            65,
            max_rows=8,
        )
        assert _drain(connector, gate, identity)[-1].deleted_count == 2
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_gallery_source_name_accesses")
            == []
        )

        name = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
            source_name[0],
            max_rows=8,
        )
        assert _drain(connector, gate, name)[-1].deleted_count == 1
        upload = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_UPLOAD_TIME,
            gid % 256,
            max_rows=8,
        )
        assert _drain(connector, gate, upload)[-1].deleted_count == 1
        assert connector.fetch_all("SELECT 1 FROM catalog_gallery_upload_times") == []
    finally:
        connector.close()


def test_gallery_identity_cleanup_retains_witness_only_partial_impact_families(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "gallery-identity-witness-retention.sqlite3")
    try:
        content_gallery_id = 45
        gid_gallery_id = content_gallery_id + 256
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (%s, %s, %s, %s)",
                    (content_gallery_id, b"k" * 32, b"s" * 32, b"c" * 32),
                ),
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (%s, %s, %s, %s)",
                    (gid_gallery_id, b"l" * 32, b"t" * 32, b"g" * 32),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_content "
                    "(analysis_id, content_sha256, witness_gallery_id) "
                    "VALUES (%s, %s, %s)",
                    (b"a" * 16, b"v" * 32, content_gallery_id),
                ),
                (
                    "INSERT INTO catalog_a_impacted_gid_provenance_storage "
                    "(analysis_id, gallery_id) VALUES (%s, %s)",
                    (b"b" * 16, gid_gallery_id),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_gid_storage "
                    "(analysis_id, gid) VALUES (%s, 9)",
                    (b"b" * 16,),
                ),
            ],
        )
        assert "catalog_analysis_impacted_content" in (
            cleanup_module._GALLERY_IDENTITY_ELIGIBILITY
        )
        assert "catalog_a_impacted_gid_provenance_storage" in (
            cleanup_module._GALLERY_IDENTITY_ELIGIBILITY
        )

        gate = _exclusive(connector)
        blocked_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_IDENTITY,
            content_gallery_id,
            max_rows=32,
        )
        blocked = _drain(connector, gate, blocked_cycle)
        assert blocked[-1].deleted_count == 0
        assert connector.fetch_all(
            "SELECT gallery_id FROM catalog_gallery_identities "
            "WHERE MOD(gallery_id, 256) = %s ORDER BY gallery_id",
            (content_gallery_id,),
        ) == [(content_gallery_id,), (gid_gallery_id,)]

        connector.execute(
            "DELETE FROM catalog_analysis_impacted_content "
            "WHERE witness_gallery_id = %s",
            (content_gallery_id,),
        )
        connector.execute(
            "DELETE FROM catalog_a_impacted_gid_provenance_storage "
            "WHERE gallery_id = %s",
            (gid_gallery_id,),
        )
        unblocked_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_IDENTITY,
            content_gallery_id,
            max_rows=32,
            now=200,
        )
        unblocked = _drain(connector, gate, unblocked_cycle, now=201)
        assert unblocked[-1].deleted_count == 2
        assert (
            connector.fetch_all(
                "SELECT gallery_id FROM catalog_gallery_identities "
                "WHERE MOD(gallery_id, 256) = %s ORDER BY gallery_id",
                (content_gallery_id,),
            )
            == []
        )
    finally:
        connector.close()


def test_foreign_owner_predecessor_blocks_staging_compaction(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "predecessor-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        selected = bytes((22,)) + b"a" * 15
        foreign = bytes((23,)) + b"b" * 15
        prior_request = b"p" * 32
        next_request = b"n" * 32
        build = b"d" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 22, 1, 'SEALED', 0, 1)",
                    (selected, build),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 23, 1, 'OPEN', 0, NULL)",
                    (foreign, b"e" * 16),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 22, 1)",
                    (build,),
                ),
                *[
                    (
                        "INSERT INTO operational_gallery_observation_staging_requests "
                        "(request_sha256) VALUES (%s)",
                        (request,),
                    )
                    for request in (prior_request, next_request)
                ],
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (prior_request, selected),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (next_request, foreign),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_predecessors "
                    "(request_sha256, prior_request_sha256) VALUES (%s, %s)",
                    (next_request, prior_request),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            22,
            max_rows=8,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_stagings "
            "WHERE staging_id = %s",
            (selected,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_request_predecessors "
            "WHERE request_sha256 = %s",
            (next_request,),
        ) == (1,)
    finally:
        connector.close()


def test_canonical_source_root_cleanup_waits_for_every_scope_consumer_then_deletes_family(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-source-root-retained.sqlite3")
    try:
        source_root = bytes((71,)) + b"r" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=source_root,
            page_sha256=b"R" * 32,
            digest_domain=b"source_root_v1",
        )
        build_scope = seed_source_scope(
            connector,
            source_root_sha256=source_root,
            identity_policy_version=1,
        )
        gallery_scope = seed_source_scope(
            connector,
            source_root_sha256=source_root,
            identity_policy_version=2,
        )
        seed_manifest_policy(connector)
        build_id = b"b" * 16
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=build_scope.scope_key,
            state="OPEN",
            created_at=1,
        )
        locator = bytes((170,)) + b"l" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=locator,
            page_sha256=b"L" * 32,
            digest_domain=b"source_relative_locator_v1",
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator, b"gallery-two"),
        )
        seed_gallery_identity(
            connector,
            gallery_id=1,
            gallery_key=identity.gallery_key(gallery_scope.scope_key, locator),
            scope_key=gallery_scope.scope_key,
            locator_sha256=locator,
        )
        before = (
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_pages WHERE value_sha256 = %s",
                (source_root,),
            ),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            _source_scope_family_rows(connector),
        )
        assert "catalog_source_scope_source_root_sha256s" in (
            cleanup_module._CANONICAL_VALUE_ELIGIBILITY
        )
        assert "catalog_source_scopes" not in (
            cleanup_module._CANONICAL_VALUE_ELIGIBILITY
        )
        assert "JOIN catalog_source_build_scope_keys build" in (
            cleanup_module._CANONICAL_VALUE_ELIGIBILITY
        )
        assert "JOIN catalog_gallery_identities gallery" in (
            cleanup_module._CANONICAL_VALUE_ELIGIBILITY
        )

        gate = _exclusive(connector)
        both_active_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            source_root[0],
            max_rows=32,
        )
        results = _drain(connector, gate, both_active_cycle)
        assert results[-1].cycle_complete
        assert results[-1].deleted_count == 0
        assert (
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_pages WHERE value_sha256 = %s",
                (source_root,),
            ),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            _source_scope_family_rows(connector),
        ) == before

        for table in (
            "catalog_source_build_descriptor_seals",
            "catalog_source_build_states",
            "catalog_source_build_created_ats",
            "catalog_source_build_manifest_policy_ids",
            "catalog_source_build_scope_keys",
            "catalog_source_build_anchors",
        ):
            connector.execute(f"DELETE FROM {table} WHERE build_id = %s", (build_id,))
        gallery_active_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            source_root[0],
            max_rows=32,
            now=200,
        )
        results = _drain(connector, gate, gallery_active_cycle, now=201)
        assert results[-1].cycle_complete
        assert results[-1].deleted_count == 0
        assert _source_scope_family_rows(connector) == before[-1]

        connector.execute("DELETE FROM catalog_gallery_identities WHERE gallery_id = 1")
        unreferenced_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            source_root[0],
            max_rows=32,
            now=300,
        )
        traced: list[str] = []
        connector.connection.set_trace_callback(traced.append)
        results = _drain(connector, gate, unreferenced_cycle, now=301)
        connector.connection.set_trace_callback(None)
        assert results[-1].cycle_complete
        # Two source-scope family candidates plus eight narrow canonical
        # identity/page/allocation candidates are removed child-first.
        assert results[-1].deleted_count == 10
        assert _source_scope_family_rows(connector) == ([], [], [], [], [], [])
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (source_root,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_pages WHERE value_sha256 = %s",
                (source_root,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            )
            == ()
        )
        scope_delete_tables = (
            "catalog_source_scope_seals",
            "catalog_source_scope_identities",
            "catalog_source_scope_identity_policy_versions",
            "catalog_source_scope_source_providers",
            "catalog_source_scope_source_root_sha256s",
            "catalog_source_scope_anchors",
        )
        observed_scope_deletes = tuple(
            table
            for statement in traced
            for table in scope_delete_tables
            if statement.startswith(f"DELETE FROM {table} ")
        )
        assert observed_scope_deletes == scope_delete_tables * 2
    finally:
        connector.close()


def test_canonical_cleanup_removes_partial_policy_family_child_first(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-partial-policy.sqlite3")
    try:
        policy = bytes((72,)) + b"p" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=policy,
            page_sha256=b"P" * 32,
            digest_domain=b"artifact_policy_v2",
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policy_semantics_anchors "
            "(policy_component_sha256) VALUES (%s)",
            (policy,),
        )
        connector.execute(
            "INSERT INTO "
            "catalog_artifact_policy_semantics_artifact_algorithm_versions "
            "(policy_component_sha256, artifact_algorithm_version) "
            "VALUES (%s, 1)",
            (policy,),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policy_semantics_max_image_short_sides "
            "(policy_component_sha256, max_image_short_side) VALUES (%s, 2048)",
            (policy,),
        )
        assert tuple(
            bool(rows) for rows in _artifact_policy_semantics_family_rows(connector)
        ) == (
            False,
            False,
            False,
            True,
            True,
            True,
        )

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            policy[0],
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        assert _artifact_policy_semantics_family_rows(connector) == (
            [],
            [],
            [],
            [],
            [],
            [],
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (policy,),
            )
            == ()
        )
    finally:
        connector.close()


@pytest.mark.parametrize("max_rows", (1, 256))
def test_canonical_cleanup_deduplicates_orphan_title_cache_multi_root_join(
    tmp_path: Path,
    max_rows: int,
) -> None:
    connector = _database(
        tmp_path / f"canonical-title-cache-multi-root-{max_rows}.sqlite3"
    )
    try:
        seed_title_sort_policy(connector)
        seed_display_title_policy(connector)
        source_title = bytes((74,)) + b"s" * 31
        display_title = bytes((74,)) + b"d" * 31
        sort_title = bytes((74,)) + b"o" * 31
        for index, (value, digest_domain) in enumerate(
            (
                (source_title, b"source_title_utf8_v1"),
                (display_title, b"display_title_utf8_v1"),
                (sort_title, b"title_sort_utf8_v1"),
            )
        ):
            _seed_minimal_canonical_value(
                connector,
                value_sha256=value,
                page_sha256=bytes((180 + index,)) + b"p" * 31,
                digest_domain=digest_domain,
            )
        connector.execute(
            "INSERT INTO catalog_display_title_choices "
            "(display_title_policy_id, source_title_sha256, "
            "source_gallery_name, title_sha256) VALUES (1, %s, %s, %s)",
            (source_title, b"orphan-gallery", display_title),
        )
        connector.execute(
            "INSERT INTO catalog_title_sorts "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (1, %s, %s)",
            (display_title, sort_title),
        )

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            74,
            max_rows=max_rows,
        )
        batch_key = b"r" * 32
        first = _advance(connector, gate, cycle, 1, batch_key, now=3)
        replay = _advance(connector, gate, cycle, 1, batch_key, now=4)
        assert replay.replayed
        assert replay.row_count == first.row_count
        assert replay.cursor == first.cursor
        assert replay.generation == first.generation
        result = first
        for attempt in range(128):
            if result.cycle_complete:
                break
            assert result.generation is not None
            result = _advance(
                connector,
                gate,
                cycle,
                result.generation,
                (attempt + 1).to_bytes(32, "big"),
                now=5 + attempt,
            )
        assert result.cycle_complete
        assert connector.fetch_all("SELECT * FROM catalog_display_title_choices") == []
        assert connector.fetch_all("SELECT * FROM catalog_title_sorts") == []
        for value in (source_title, display_title, sort_title):
            assert (
                connector.fetch_one(
                    "SELECT value_sha256 "
                    "FROM catalog_canonical_value_allocation_anchors "
                    "WHERE value_sha256 = %s",
                    (value,),
                )
                == ()
            )
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        connector.close()


def test_canonical_source_scope_delete_faults_roll_back_every_child_boundary(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-source-scope-faults.sqlite3")
    try:
        source_root = bytes((73,)) + b"r" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=source_root,
            page_sha256=b"T" * 32,
            digest_domain=b"source_root_v1",
        )
        seed_source_scope(
            connector,
            source_root_sha256=source_root,
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            source_root[0],
            max_rows=32,
        )
        family_before = _source_scope_family_rows(connector)
        protocol_before = _cleanup_protocol_snapshot(connector)
        source_scope_spec = cleanup_module._STATIC_PLANS[
            CleanupTargetKind.CANONICAL_VALUE
        ].phases["CV_DICTIONARY"][2]
        tables = tuple(
            statement.split()[2] for statement in source_scope_spec.delete_sql
        )
        original_execute_affected = connector.execute_affected
        for failed_table in tables:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected canonical dictionary delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_delete,
                ),
                pytest.raises(RuntimeError, match="dictionary delete fault"),
            ):
                _advance(connector, gate, cycle, 1, b"d" * 32, now=20)
            assert triggered, failed_table
            assert _source_scope_family_rows(connector) == family_before
            assert _cleanup_protocol_snapshot(connector) == protocol_before

        committed = _advance(connector, gate, cycle, 1, b"d" * 32, now=21)
        assert committed.row_count == 1
        assert _source_scope_family_rows(connector) == ([], [], [], [], [], [])
    finally:
        connector.close()


def test_analysis_overlay_cleanup_observes_exact_child_first_order(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-overlay-order.sqlite3")
    try:
        analysis_id = _seed_abandoned_analysis_for_cleanup(
            connector,
            discriminator=92,
        )
        _seed_analysis_overlay_rows(connector, analysis_id)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
        )
        _position_analysis_cleanup_at_overlay(
            connector,
            gate,
            cycle,
            now=10,
        )

        traced: list[str] = []
        connector.connection.set_trace_callback(traced.append)
        overlay = _advance(
            connector,
            gate,
            cycle,
            1,
            b"o" * 32,
            now=20,
        )
        overlay_terminal = _advance(
            connector,
            gate,
            cycle,
            2,
            b"t" * 32,
            now=21,
        )
        file_values = _advance(
            connector,
            gate,
            cycle,
            1,
            b"v" * 32,
            now=22,
        )
        file_values_terminal = _advance(
            connector,
            gate,
            cycle,
            2,
            b"w" * 32,
            now=23,
        )
        provenance = _advance(
            connector,
            gate,
            cycle,
            1,
            b"p" * 32,
            now=24,
        )
        provenance_terminal = _advance(
            connector,
            gate,
            cycle,
            2,
            b"q" * 32,
            now=25,
        )
        file_anchor = _advance(
            connector,
            gate,
            cycle,
            1,
            b"a" * 32,
            now=26,
        )
        file_anchor_terminal = _advance(
            connector,
            gate,
            cycle,
            2,
            b"b" * 32,
            now=27,
        )
        evidence = _advance(
            connector,
            gate,
            cycle,
            1,
            b"e" * 32,
            now=28,
        )
        connector.connection.set_trace_callback(None)

        expected_order = _ALL_ANALYSIS_OVERLAY_TABLES + (
            "catalog_analysis_impacted_galleries",
        )
        observed_order = tuple(
            table
            for statement in traced
            for table in expected_order
            if statement.startswith(f"DELETE FROM {table} ")
        )
        assert observed_order == expected_order
        assert overlay.phase == "AR_OVERLAY"
        assert overlay.row_count == len(_ANALYSIS_OVERLAY_TABLES)
        assert overlay_terminal.phase == "AR_FILE_HASH_VALUES"
        assert overlay_terminal.row_count == 0
        assert file_values.phase == "AR_FILE_HASH_VALUES"
        assert file_values.row_count == len(_ANALYSIS_FILE_HASH_VALUE_TABLES)
        assert file_values_terminal.phase == "AR_IMPACT_PROVENANCE"
        assert file_values_terminal.row_count == 0
        assert provenance.phase == "AR_IMPACT_PROVENANCE"
        assert provenance.row_count == len(_ANALYSIS_IMPACT_PROVENANCE_TABLES)
        assert provenance_terminal.phase == "AR_FILE_HASH_ANCHOR"
        assert provenance_terminal.row_count == 0
        assert file_anchor.phase == "AR_FILE_HASH_ANCHOR"
        assert file_anchor.row_count == len(_ANALYSIS_FILE_HASH_ANCHOR_TABLES)
        assert file_anchor_terminal.phase == "AR_EVIDENCE"
        assert file_anchor_terminal.row_count == 0
        assert evidence.phase == "AR_EVIDENCE"
        assert evidence.row_count == 1
        assert all(
            row == (0,) for row in _analysis_overlay_rows(connector, analysis_id)
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_analysis_impacted_galleries "
                "WHERE analysis_id = %s",
                (analysis_id,),
            )
            == ()
        )
    finally:
        connector.connection.set_trace_callback(None)
        connector.close()


def test_analysis_overlay_delete_faults_roll_back_every_table_boundary(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-overlay-faults.sqlite3")
    try:
        analysis_id = _seed_abandoned_analysis_for_cleanup(
            connector,
            discriminator=93,
        )
        _seed_analysis_overlay_rows(connector, analysis_id)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
        )
        _position_analysis_cleanup_at_overlay(
            connector,
            gate,
            cycle,
            now=10,
        )
        original_execute_affected = connector.execute_affected
        phase_names = (
            "AR_OVERLAY",
            "AR_FILE_HASH_VALUES",
            "AR_IMPACT_PROVENANCE",
            "AR_FILE_HASH_ANCHOR",
        )
        for phase_index, (phase_name, phase_tables) in enumerate(
            zip(phase_names, _ANALYSIS_OVERLAY_PHASE_TABLES, strict=True)
        ):
            family_before = _analysis_overlay_rows(connector, analysis_id)
            protocol_before = _cleanup_protocol_snapshot(connector)
            batch_key = bytes((64 + phase_index,)) * 32
            for failed_table in phase_tables:
                triggered = False

                def fail_delete(
                    sql: str,
                    data: tuple[Any, ...] = (),
                    *,
                    target: str = failed_table,
                ) -> int:
                    nonlocal triggered
                    if sql.lstrip().startswith(f"DELETE FROM {target} "):
                        triggered = True
                        raise RuntimeError("injected analysis overlay delete fault")
                    return original_execute_affected(sql, data)

                with (
                    patch.object(
                        connector,
                        "execute_affected",
                        side_effect=fail_delete,
                    ),
                    pytest.raises(
                        RuntimeError,
                        match="analysis overlay delete fault",
                    ),
                ):
                    _advance(
                        connector,
                        gate,
                        cycle,
                        1,
                        batch_key,
                        now=30 + phase_index * 2,
                    )
                assert triggered, failed_table
                assert _analysis_overlay_rows(connector, analysis_id) == family_before
                assert _cleanup_protocol_snapshot(connector) == protocol_before

            committed = _advance(
                connector,
                gate,
                cycle,
                1,
                batch_key,
                now=30 + phase_index * 2,
            )
            assert committed.phase == phase_name
            assert committed.row_count == len(phase_tables)
            assert all(
                connector.fetch_one(
                    f"SELECT COUNT(*) FROM {table} WHERE analysis_id = %s",
                    (analysis_id,),
                )
                == (0,)
                for table in phase_tables
            )
            terminal = _advance(
                connector,
                gate,
                cycle,
                2,
                bytes((80 + phase_index,)) * 32,
                now=31 + phase_index * 2,
            )
            expected_next = (
                phase_names[phase_index + 1]
                if phase_index + 1 < len(phase_names)
                else "AR_EVIDENCE"
            )
            assert terminal.phase == expected_next
            assert terminal.row_count == 0
            assert connector.fetch_one(
                "SELECT 1 FROM catalog_analysis_impacted_galleries "
                "WHERE analysis_id = %s",
                (analysis_id,),
            ) == (1,)

        assert all(
            row == (0,) for row in _analysis_overlay_rows(connector, analysis_id)
        )
    finally:
        connector.close()


def test_analysis_overlay_cleanup_removes_atomic_and_orphan_rows_by_phase(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-overlay-partial.sqlite3")
    try:
        analysis_id = _seed_abandoned_analysis_for_cleanup(
            connector,
            discriminator=94,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_a_file_decision_shadow_occurrences "
                    "(analysis_id, file_sha256, occurrence_count) "
                    "VALUES (%s, %s, 1)",
                    (analysis_id, b"f" * 32),
                ),
                (
                    "INSERT INTO catalog_analysis_content_owner_candidate_shadows "
                    "(analysis_id, gallery_id, content_sha256, "
                    "prefer_not_already_uploaded, title_scalar_count, download_time) "
                    "VALUES (%s, 7, %s, 1, 1, 1)",
                    (analysis_id, b"b" * 32),
                ),
                (
                    "INSERT INTO catalog_analysis_content_owner_shadows "
                    "(analysis_id, content_sha256, owner_gallery_id) "
                    "VALUES (%s, %s, 7)",
                    (analysis_id, b"c" * 32),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_content "
                    "(analysis_id, content_sha256, witness_gallery_id) "
                    "VALUES (%s, %s, 7)",
                    (analysis_id, b"d" * 32),
                ),
                (
                    "INSERT INTO catalog_a_impacted_gid_provenance_storage "
                    "(analysis_id, gallery_id) VALUES (%s, 7)",
                    (analysis_id,),
                ),
            ],
        )
        assert (
            sum(
                cast(int, row[0])
                for row in _analysis_overlay_rows(connector, analysis_id)
            )
            == 5
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
        )
        _position_analysis_cleanup_at_overlay(
            connector,
            gate,
            cycle,
            now=10,
        )
        overlay = _advance(
            connector,
            gate,
            cycle,
            1,
            b"p" * 32,
            now=20,
        )
        assert overlay.phase == "AR_OVERLAY"
        assert overlay.row_count == 3
        assert (
            _advance(
                connector,
                gate,
                cycle,
                2,
                b"q" * 32,
                now=21,
            ).phase
            == "AR_FILE_HASH_VALUES"
        )
        file_values = _advance(
            connector,
            gate,
            cycle,
            1,
            b"r" * 32,
            now=22,
        )
        assert file_values.phase == "AR_FILE_HASH_VALUES"
        assert file_values.row_count == 1
        assert (
            _advance(
                connector,
                gate,
                cycle,
                2,
                b"s" * 32,
                now=23,
            ).phase
            == "AR_IMPACT_PROVENANCE"
        )
        provenance = _advance(
            connector,
            gate,
            cycle,
            1,
            b"t" * 32,
            now=24,
        )
        assert provenance.phase == "AR_IMPACT_PROVENANCE"
        assert provenance.row_count == 1
        assert (
            _advance(
                connector,
                gate,
                cycle,
                2,
                b"u" * 32,
                now=25,
            ).phase
            == "AR_FILE_HASH_ANCHOR"
        )
        anchor_terminal = _advance(
            connector,
            gate,
            cycle,
            1,
            b"v" * 32,
            now=26,
        )
        assert anchor_terminal.phase == "AR_EVIDENCE"
        assert anchor_terminal.row_count == 0
        assert all(
            row == (0,) for row in _analysis_overlay_rows(connector, analysis_id)
        )
    finally:
        connector.close()


def test_analysis_root_compound_delete_faults_roll_back_every_member_boundary(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-root-family-faults.sqlite3")
    try:
        analysis_id = bytes((91,)) + b"a" * 15
        build_id = b"R" * 16
        scope_key = _seed_source_build_scope(connector, discriminator=91)
        seed_analysis_policy(connector)
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            manifest_policy_id=1,
            state="SEALED",
            created_at=0,
            sealed_at=0,
        )
        seed_analysis_run(
            connector,
            analysis_id=analysis_id,
            build_id=build_id,
            policy_id=1,
            input_manifest_sha256=b"m" * 32,
            started_at=0,
            state="ABANDONED",
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=32,
        )
        generation = 1
        for attempt in range(21):
            result = _advance(
                connector,
                gate,
                cycle,
                generation,
                attempt.to_bytes(32, "big"),
                now=3 + attempt,
            )
            assert not result.cycle_complete
            if attempt < 20:
                assert result.phase != "AR_ROOT"
            assert result.generation is not None
            generation = result.generation
        assert result.phase == "AR_ROOT" and result.row_count == 0

        family_before = _analysis_run_family_rows(connector, analysis_id)
        protocol_before = _cleanup_protocol_snapshot(connector)
        root_spec = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases[
            "AR_ROOT"
        ][0]
        tables = tuple(statement.split()[2] for statement in root_spec.delete_sql)
        assert len(tables) == 9
        original_execute_affected = connector.execute_affected
        for failed_table in tables:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected analysis family delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_delete,
                ),
                pytest.raises(RuntimeError, match="analysis family delete fault"),
            ):
                _advance(
                    connector,
                    gate,
                    cycle,
                    generation,
                    b"f" * 32,
                    now=30,
                )
            assert triggered
            assert _analysis_run_family_rows(connector, analysis_id) == family_before
            assert _cleanup_protocol_snapshot(connector) == protocol_before

        committed = _advance(
            connector,
            gate,
            cycle,
            generation,
            b"s" * 32,
            now=31,
        )
        assert committed.phase == "AR_ROOT" and committed.row_count == 1
        assert _analysis_run_family_rows(connector, analysis_id) == tuple(
            [] for _table in tables
        )
    finally:
        connector.close()


def test_canonical_policy_delete_faults_roll_back_every_child_boundary(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-policy-faults.sqlite3")
    try:
        producer = seed_artifact_producer_fingerprint(connector)
        policy = identity.artifact_policy_digest(
            1,
            2048,
            producer.producer_fingerprint_sha256,
        )
        _seed_minimal_canonical_value(
            connector,
            value_sha256=policy,
            page_sha256=b"U" * 32,
            digest_domain=b"artifact_policy_v2",
        )
        seed_artifact_policy_semantics(
            connector,
            artifact_algorithm_version=1,
            max_image_short_side=2048,
            producer_fingerprint_sha256=producer.producer_fingerprint_sha256,
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            policy[0],
            max_rows=32,
        )
        dictionary = _advance(connector, gate, cycle, 1, b"c" * 32, now=20)
        assert dictionary.phase == "CV_SEMANTIC_LINK"
        assert dictionary.generation == 1
        family_before = _artifact_policy_semantics_family_rows(connector)
        protocol_before = _cleanup_protocol_snapshot(connector)
        tables = tuple(
            spec.table
            for spec in cleanup_module._STATIC_PLANS[
                CleanupTargetKind.CANONICAL_VALUE
            ].phases["CV_SEMANTIC_LINK"]
        )
        original_execute_affected = connector.execute_affected
        for failed_table in tables:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected canonical semantic delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_delete,
                ),
                pytest.raises(RuntimeError, match="semantic delete fault"),
            ):
                _advance(connector, gate, cycle, 1, b"s" * 32, now=21)
            assert triggered, failed_table
            assert _artifact_policy_semantics_family_rows(connector) == family_before
            assert _cleanup_protocol_snapshot(connector) == protocol_before

        committed = _advance(connector, gate, cycle, 1, b"s" * 32, now=22)
        assert committed.row_count == 6
        assert _artifact_policy_semantics_family_rows(connector) == (
            [],
            [],
            [],
            [],
            [],
            [],
        )
    finally:
        connector.close()


def test_canonical_semantic_family_delete_faults_roll_back_every_statement(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-semantic-family-faults.sqlite3")
    try:
        semantics = bytes((24,)) + b"s" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=semantics,
            page_sha256=b"Q" * 32,
            digest_domain=b"artifact_semantics_v1",
        )
        _seed_artifact_semantic_input_family(
            connector,
            artifact_semantics_sha256=semantics,
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            24,
            max_rows=32,
        )
        family_before = _artifact_semantic_input_family_rows(
            connector,
            artifact_semantics_sha256=semantics,
        )
        protocol_before = _cleanup_protocol_snapshot(connector)
        semantic_spec = cleanup_module._STATIC_PLANS[
            CleanupTargetKind.CANONICAL_VALUE
        ].phases["CV_DICTIONARY"][-1]
        tables = tuple(statement.split()[2] for statement in semantic_spec.delete_sql)
        assert tables == ("catalog_artifact_semantic_inputs",)
        original_execute_affected = connector.execute_affected
        for failed_table in tables:
            triggered = False

            def fail_delete(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> int:
                nonlocal triggered
                if sql.lstrip().startswith(f"DELETE FROM {target}"):
                    triggered = True
                    raise RuntimeError("injected semantic family delete fault")
                return original_execute_affected(sql, data)

            with (
                patch.object(connector, "execute_affected", side_effect=fail_delete),
                pytest.raises(RuntimeError, match="semantic family delete fault"),
            ):
                _advance(connector, gate, cycle, 1, b"f" * 32, now=20)
            assert triggered, failed_table
            assert (
                _artifact_semantic_input_family_rows(
                    connector,
                    artifact_semantics_sha256=semantics,
                )
                == family_before
            )
            assert _cleanup_protocol_snapshot(connector) == protocol_before

        committed = _advance(connector, gate, cycle, 1, b"s" * 32, now=21)
        assert committed.phase == "CV_DICTIONARY" and committed.row_count == 1
        assert _artifact_semantic_input_family_rows(
            connector,
            artifact_semantics_sha256=semantics,
        ) == tuple([] for _ in tables)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("blocker_table", "blocker_sql", "blocker_parameters"),
    (
        (
            "catalog_candidate_artifact_inputs",
            "INSERT INTO catalog_candidate_artifact_inputs "
            "(candidate_id, publication_key, artifact_semantics_sha256) "
            "VALUES (%s, %s, %s)",
            (b"c" * 16, b"p" * 32),
        ),
        (
            "catalog_artifacts",
            "INSERT INTO catalog_artifacts "
            "(revision, publication_key, artifact_sha256, artifact_semantics_sha256) "
            "VALUES (1, %s, %s, %s)",
            (b"p" * 32, b"a" * 32),
        ),
    ),
)
def test_canonical_cleanup_retains_physical_artifact_semantic_consumers(
    tmp_path: Path,
    blocker_table: str,
    blocker_sql: str,
    blocker_parameters: tuple[object, ...],
) -> None:
    connector = _database(tmp_path / f"semantic-retained-{blocker_table}.sqlite3")
    try:
        semantics = bytes((26,)) + b"s" * 31
        _seed_minimal_canonical_value(
            connector,
            value_sha256=semantics,
            page_sha256=b"W" * 32,
            digest_domain=b"artifact_semantics_v1",
        )
        _seed_artifact_semantic_input_family(
            connector,
            artifact_semantics_sha256=semantics,
        )
        _fixture_rows(
            connector,
            [(blocker_sql, (*blocker_parameters, semantics))],
        )
        family_before = _artifact_semantic_input_family_rows(
            connector,
            artifact_semantics_sha256=semantics,
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            26,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert (
            _artifact_semantic_input_family_rows(
                connector,
                artifact_semantics_sha256=semantics,
            )
            == family_before
        )
        assert connector.fetch_one(
            "SELECT value_sha256 FROM catalog_canonical_value_allocation_anchors "
            "WHERE value_sha256 = %s",
            (semantics,),
        ) == (semantics,)
    finally:
        connector.close()


def test_canonical_page_identity_upload_artifact_and_hash_cache_strategies(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "digest-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)

        source_provider = b"filesystem"
        source_root = b"R" * 32
        value = identity.source_scope_key("filesystem", source_root, 1)
        parent_page = b"P" * 32
        child_page = b"C" * 32
        seed_canonical_allocation(
            connector,
            value_sha256=value,
            digest_domain=b"source_root_v1",
            byte_count=2,
            allocated_at=0,
        )
        seed_canonical_page(
            connector,
            page_sha256=child_page,
            value_sha256=value,
            page_bytes=b"child",
            level=0,
            page_position=0,
            subtree_item_count=1,
        )
        seed_canonical_page(
            connector,
            page_sha256=parent_page,
            value_sha256=value,
            page_bytes=b"parent",
            level=1,
            page_position=0,
            subtree_item_count=1,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_canonical_value_page_parents "
                    "(child_sha256, parent_sha256, position) VALUES (%s, %s, 0)",
                    (child_page, parent_page),
                ),
                (
                    "INSERT INTO catalog_canonical_value_identities "
                    "(value_sha256, root_page_sha256) VALUES (%s, %s)",
                    (value, parent_page),
                ),
                (
                    "INSERT INTO catalog_source_scope_anchors (scope_key) VALUES (%s)",
                    (value,),
                ),
                (
                    "INSERT INTO catalog_source_scope_source_providers "
                    "(scope_key, source_provider) VALUES (%s, %s)",
                    (value, source_provider),
                ),
                (
                    "INSERT INTO catalog_source_scope_source_root_sha256s "
                    "(scope_key, source_root_sha256) VALUES (%s, %s)",
                    (value, source_root),
                ),
                (
                    "INSERT INTO catalog_source_scope_identity_policy_versions "
                    "(scope_key, identity_policy_version) VALUES (%s, 1)",
                    (value,),
                ),
                (
                    "INSERT INTO catalog_source_scope_identities "
                    "(source_provider, source_root_sha256, identity_policy_version, "
                    "scope_key) VALUES (%s, %s, 1, %s)",
                    (source_provider, source_root, value),
                ),
                (
                    "INSERT INTO catalog_source_scope_seals (scope_key) VALUES (%s)",
                    (value,),
                ),
            ],
        )
        canonical_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            value[0],
            max_rows=1,
        )
        _drain(connector, gate, canonical_cycle)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (value,),
            )
            == ()
        )
        assert (
            connector.fetch_all(
                "SELECT page_sha256 FROM catalog_canonical_value_pages "
                "WHERE value_sha256 = %s",
                (value,),
            )
            == []
        )

        page = bytes((32,)) + b"g" * 31
        child = bytes((33,)) + b"h" * 31
        seed_gallery_page_descriptor(
            connector,
            page_sha256=child,
            page_bytes=b"child",
            component=b"FILE",
            level=0,
            subtree_item_count=1,
        )
        seed_gallery_page_descriptor(
            connector,
            page_sha256=page,
            page_bytes=b"page",
            component=b"FILE",
            level=1,
            subtree_item_count=1,
        )
        seed_gallery_page_bounds(
            connector,
            page_sha256=page,
            first_key=b"a",
            last_key=b"z",
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_page_children "
            "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
            (page, child),
        )
        page_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
            32,
            max_rows=1,
        )
        _drain(connector, gate, page_cycle, now=50)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
                (page,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
            (child,),
        ) == (1,)

        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (34, %s, %s, %s)",
                    (b"g" * 32, b"s" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO operational_gallery_observation_allocators "
                    "(gallery_id, next_observation_id, updated_at) VALUES (34, 1, 0)",
                    (),
                ),
            ],
        )
        gallery_cycle = _begin(
            connector, gate, CleanupTargetKind.GALLERY_IDENTITY, 34, max_rows=1
        )
        _drain(connector, gate, gallery_cycle, now=100)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_identities WHERE gallery_id = 34"
            )
            == ()
        )

        upload = bytes((35,)) + b"u" * 31
        seed_canonical_allocation(
            connector,
            value_sha256=upload,
            digest_domain=b"source_root_v1",
            byte_count=0,
            allocated_at=0,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (5, 0, 1)",
                    (),
                ),
                (
                    "INSERT INTO operational_ingest_coordination_heads "
                    "(singleton_id, current_generation, completed_generation, "
                    "phase, last_transition_at) VALUES (1, 6, 5, 'READY', 1)",
                    (),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (5, %s)",
                    (upload,),
                ),
            ],
        )
        upload_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
            35,
            max_rows=1,
        )
        _drain(connector, gate, upload_cycle, now=150)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = 5 AND value_sha256 = %s",
                (upload,),
            )
            == ()
        )

        artifact = bytes((36,)) + b"a" * 31
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                    "VALUES (%s, 4, %s)",
                    (artifact, b"l" * 32),
                ),
            ],
        )
        artifact_cycle = _begin(
            connector, gate, CleanupTargetKind.ARTIFACT_BLOB, 36, max_rows=1
        )
        _drain(connector, gate, artifact_cycle, now=200)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
                (artifact,),
            )
            == ()
        )

        source = bytes((37,)) + b"h" * 31
        fingerprint = b"f" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_hash_cache_observations "
                    "(source_identity_sha256, fingerprint_sha256, observed_at) "
                    "VALUES (%s, %s, 10)",
                    (source, fingerprint),
                ),
                (
                    "INSERT INTO operational_file_hash_caches "
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
                    "VALUES (%s, %s, %s, 10)",
                    (source, fingerprint, b"z" * 32),
                ),
            ],
        )
        cache_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            37,
            max_rows=1,
        )
        _drain(connector, gate, cache_cycle, now=250)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_hash_cache_observations "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            == ()
        )
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("blocker_table", "blocker_sql", "blocker_parameters"),
    (
        (
            "catalog_prepared_artifacts",
            "INSERT INTO catalog_prepared_artifacts "
            "(candidate_id, publication_key, protection_token, artifact_sha256, "
            "storage_codec_version, storage_generation, state) "
            "VALUES (%s, %s, %s, %s, 1, 1, 'COMMITTED')",
            (b"c" * 16, b"p" * 32, b"t" * 184),
        ),
        (
            "catalog_artifacts",
            "INSERT INTO catalog_artifacts "
            "(revision, publication_key, artifact_semantics_sha256, artifact_sha256) "
            "VALUES (1, %s, %s, %s)",
            (b"p" * 32, b"s" * 32),
        ),
    ),
)
def test_artifact_blob_cleanup_retains_every_physical_sha_fact(
    tmp_path: Path,
    blocker_table: str,
    blocker_sql: str,
    blocker_parameters: tuple[object, ...],
) -> None:
    connector = _database(tmp_path / f"artifact-retained-{blocker_table}.sqlite3")
    try:
        artifact_sha256 = bytes((25,)) + b"a" * 31
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                    "VALUES (%s, 4, %s)",
                    (artifact_sha256, b"l" * 32),
                ),
                (blocker_sql, (*blocker_parameters, artifact_sha256)),
            ],
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ARTIFACT_BLOB,
            25,
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT artifact_sha256 FROM catalog_artifact_blobs "
            "WHERE artifact_sha256 = %s",
            (artifact_sha256,),
        ) == (artifact_sha256,)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "failed_table",
    tuple(_GALLERY_PAGE_DELETE_PHASE_BY_TABLE),
)
def test_gallery_page_cleanup_fault_rolls_back_its_complete_child_first_phase(
    tmp_path: Path,
    failed_table: str,
) -> None:
    connector = _database(tmp_path / "gallery-page-cleanup-fault.sqlite3")
    try:
        parent = bytes((80,)) + b"p" * 31
        child = bytes((81,)) + b"c" * 31
        _seed_cleanup_gallery_page(connector, parent=parent, child=child)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
            parent[0],
            max_rows=32,
        )
        group = _GALLERY_PAGE_DELETE_PHASE_BY_TABLE[failed_table]
        before = _gallery_page_group_rows(
            connector,
            parent=parent,
            tables=group,
        )
        assert all(before)
        original_execute_affected = connector.execute_affected
        triggered = False

        def fail_one_delete(sql: str, data: tuple[Any, ...] = ()) -> int:
            nonlocal triggered
            if not triggered and sql.lstrip().startswith(f"DELETE FROM {failed_table}"):
                triggered = True
                raise RuntimeError("injected gallery-page delete fault")
            return original_execute_affected(sql, data)

        with (
            patch.object(
                connector,
                "execute_affected",
                side_effect=fail_one_delete,
            ),
            pytest.raises(RuntimeError, match="gallery-page delete fault"),
        ):
            _drain(connector, gate, cycle)
        assert triggered, failed_table
        assert (
            _gallery_page_group_rows(
                connector,
                parent=parent,
                tables=group,
            )
            == before
        )
    finally:
        connector.close()


def test_gallery_page_cleanup_exact_delete_order_and_partial_family_support(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "gallery-page-cleanup-order.sqlite3")
    try:
        parent = bytes((80,)) + b"p" * 31
        child = bytes((81,)) + b"c" * 31
        _seed_cleanup_gallery_page(connector, parent=parent, child=child)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
            parent[0],
            max_rows=32,
        )
        traced: list[str] = []
        connector.connection.set_trace_callback(traced.append)
        results = _drain(connector, gate, cycle)
        connector.connection.set_trace_callback(None)
        expected_order = (
            "catalog_gallery_observation_page_children",
            "catalog_gallery_observation_page_key_bounds_seals",
            "catalog_gallery_observation_page_key_bounds_first_keys",
            "catalog_gallery_observation_page_key_bounds_last_keys",
            "catalog_gallery_observation_page_key_bounds_anchors",
            "catalog_gallery_observation_page_descriptor_seals",
            "catalog_gallery_observation_page_descriptor_components",
            "catalog_gallery_observation_page_descriptor_levels",
            "catalog_gallery_observation_page_descriptor_subtree_item_counts",
            "catalog_gallery_observation_pages",
            "catalog_gallery_observation_page_descriptor_anchors",
        )
        observed_order = tuple(
            table
            for statement in traced
            for table in expected_order
            if statement.startswith(f"DELETE FROM {table} ")
        )
        assert observed_order == expected_order
        assert results[-1].deleted_count == len(expected_order)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_page_descriptor_anchors "
                "WHERE page_sha256 = %s",
                (parent,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_page_descriptor_seals "
            "WHERE page_sha256 = %s",
            (child,),
        ) == (1,)

        partial = bytes((82,)) + b"x" * 31
        seed_gallery_page_descriptor(
            connector,
            page_sha256=partial,
            page_bytes=b"partial",
            component=b"FILE",
            level=0,
            subtree_item_count=1,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_anchors "
            "(page_sha256) VALUES (%s)",
            (partial,),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_first_keys "
            "(page_sha256, first_key) VALUES (%s, %s)",
            (partial, b"a" * 8),
        )
        partial_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
            partial[0],
            max_rows=32,
            now=100,
        )
        partial_results = _drain(connector, gate, partial_cycle, now=101)
        assert partial_results[-1].deleted_count == 8
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_page_descriptor_anchors "
                "WHERE page_sha256 = %s",
                (partial,),
            )
            == ()
        )
    finally:
        connector.close()


def test_live_generation_upload_incoming_page_and_redownload_roots_block(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "retention-blockers.sqlite3")
    try:
        gate = _exclusive(connector)
        build = bytes((40,)) + b"b" * 15
        value = bytes((41,)) + b"v" * 31
        page = bytes((42,)) + b"p" * 31
        parent = bytes((43,)) + b"q" * 31
        seed_canonical_allocation(
            connector,
            value_sha256=value,
            digest_domain=b"source_root_v1",
            byte_count=0,
            allocated_at=0,
        )
        seed_gallery_page_descriptor(
            connector,
            page_sha256=page,
            page_bytes=b"page",
            component=b"FILE",
            level=0,
            subtree_item_count=0,
        )
        seed_gallery_page_descriptor(
            connector,
            page_sha256=parent,
            page_bytes=b"page",
            component=b"FILE",
            level=1,
            subtree_item_count=0,
        )
        scope_key = _seed_source_build_scope(connector, discriminator=40)
        seed_source_build(
            connector,
            build_id=build,
            scope_key=scope_key,
            state="ABANDONED",
            created_at=0,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (10, 0, NULL)",
                    (),
                ),
                (
                    "INSERT INTO operational_ingest_coordination_heads "
                    "(singleton_id, current_generation, completed_generation, "
                    "phase, last_transition_at) VALUES (1, 10, 0, 'INGESTING', 1)",
                    (),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, 10)",
                    (build,),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (10, %s)",
                    (value,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_page_children "
                    "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
                    (parent, page),
                ),
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (44, %s, %s, %s)",
                    (b"g" * 32, b"x" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO operational_gallery_redownload_states "
                    "(gallery_id, redownload_at, through_source_revision, updated_at) "
                    "VALUES (44, 1, 1, 1)",
                    (),
                ),
            ],
        )

        for kind, shard in (
            (CleanupTargetKind.SOURCE_BUILD, 40),
            (CleanupTargetKind.CANONICAL_VALUE_UPLOAD, 41),
            (CleanupTargetKind.CANONICAL_VALUE, 41),
            (CleanupTargetKind.GALLERY_OBSERVATION_PAGE, 42),
            (CleanupTargetKind.GALLERY_IDENTITY, 44),
        ):
            cycle = _begin(connector, gate, kind, shard, max_rows=8)
            results = _drain(connector, gate, cycle, now=20 + shard)
            assert results[-1].deleted_count == 0

        assert connector.fetch_one(
            "SELECT 1 FROM catalog_source_builds WHERE build_id = %s", (build,)
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_canonical_value_uploads "
            "WHERE generation = 10 AND value_sha256 = %s",
            (value,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_canonical_value_allocations WHERE value_sha256 = %s",
            (value,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
            (page,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_identities WHERE gallery_id = 44"
        ) == (1,)
    finally:
        connector.close()


def test_latest_receipt_corruption_and_stale_attempts_fail_before_writes(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "receipt-corruption.sqlite3")
    try:
        gate = _exclusive(connector)
        first = bytes((45,)) + b"a" * 31
        second = bytes((45,)) + b"b" * 31
        connector.execute_many(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            [(first, 1), (second, 2)],
        )
        cycle = _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB, 45, max_rows=1)
        committed = _advance(connector, gate, cycle, 1, b"a" * 32, now=3)
        assert committed.cursor == first
        remaining = connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        )

        with pytest.raises(CleanupUnavailableError, match="stale"):
            _advance(connector, gate, cycle, 1, b"b" * 32, now=4)
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == remaining
        )

        connector.execute(
            "UPDATE operational_cleanup_batch_receipts "
            "SET output_sha256 = %s WHERE cleanup_id = %s AND phase = 'CB_ROOT'",
            (b"x" * 32, cycle.cleanup_id),
        )
        with pytest.raises(CleanupCorruptionError, match="receipt"):
            _advance(connector, gate, cycle, 1, b"a" * 32, now=5)
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == remaining
        )
    finally:
        connector.close()


def test_empty_terminal_response_loss_is_zero_write_replay(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-transition-replay.sqlite3")
    try:
        gate = _exclusive(connector)
        cycle = _begin(connector, gate, CleanupTargetKind.ARTIFACT_BLOB, 47, max_rows=8)
        committed = _advance(connector, gate, cycle, 1, b"t" * 32, now=3)
        assert committed.phase is None
        assert committed.phase_complete and committed.cycle_complete
        assert not committed.replayed
        before = (
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_batch_receipts "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
        )
        replay = _advance(connector, gate, cycle, 1, b"t" * 32, now=4)
        assert replay == CleanupBatchResult(
            cycle=cycle,
            phase=None,
            generation=None,
            cursor=b"",
            deleted_count=0,
            row_count=0,
            phase_complete=True,
            cycle_complete=True,
            replayed=True,
        )
        after = (
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_batch_receipts "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
        )
        assert after == before
    finally:
        connector.close()


def test_batch_rechecks_retention_roots_and_live_exclusive_gate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "retention-race.sqlite3")
    try:
        gate = _exclusive(connector)
        file_sha256 = bytes((48,)) + b"f" * 31
        source = b"s" * 32
        fingerprint = b"p" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, 1)",
                    (file_sha256,),
                ),
                (
                    "INSERT INTO operational_hash_cache_observations "
                    "(source_identity_sha256, fingerprint_sha256, observed_at) "
                    "VALUES (%s, %s, 1)",
                    (source, fingerprint),
                ),
            ],
        )
        cycle = _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB, 48, max_rows=1)
        original = connector.fetch_all
        injected = False

        def race_retention_root(
            sql: str, parameters: tuple[object, ...] = ()
        ) -> list[tuple[Any, ...]]:
            nonlocal injected
            rows = original(sql, parameters)
            if (
                not injected
                and "FROM catalog_content_blobs AS b" in sql
                and "ORDER BY b.file_sha256" in sql
                and rows
            ):
                injected = True
                connector.execute(
                    "INSERT INTO operational_file_hash_caches "
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
                    "VALUES (%s, %s, %s, 1)",
                    (source, fingerprint, file_sha256),
                )
            return rows

        with (
            patch.object(connector, "fetch_all", side_effect=race_retention_root),
            pytest.raises(CleanupRetentionBlockedError, match="retention root"),
        ):
            _advance(connector, gate, cycle, 1, b"r" * 32, now=3)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_content_blobs WHERE file_sha256 = %s",
            (file_sha256,),
        ) == (1,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_file_hash_caches "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            == ()
        )

        with pytest.raises(RuntimeError, match="stale or expired"):
            _advance(connector, gate, cycle, 1, b"e" * 32, now=100_002)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_content_blobs WHERE file_sha256 = %s",
            (file_sha256,),
        ) == (1,)
    finally:
        connector.close()


def test_paired_staging_identity_delete_rolls_back_on_second_write_fault(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "paired-delete-fault.sqlite3")
    try:
        gate = _exclusive(connector)
        staging = bytes((46,)) + b"s" * 15
        request = b"r" * 32
        build = b"b" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 46, 1, 'SEALED', 0, 1)",
                    (staging, build),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 46, 1)",
                    (build,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
                    "(request_sha256) VALUES (%s)",
                    (request,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (request, staging),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (staging,),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            46,
            max_rows=1,
        )
        result = _advance(connector, gate, cycle, 1, b"1" * 32, now=3)
        result = _advance(
            connector,
            gate,
            cycle,
            result.generation or 1,
            b"2" * 32,
            now=4,
        )
        result = _advance(
            connector,
            gate,
            cycle,
            result.generation or 1,
            b"3" * 32,
            now=5,
        )
        assert result.phase == "GOS_REQUEST_IDENTITY" and result.generation == 1

        original = connector.execute_affected

        def fail_second_delete(sql: str, parameters: tuple[object, ...] = ()) -> int:
            if sql.startswith(
                "DELETE FROM operational_gallery_observation_staging_requests"
            ):
                raise RuntimeError("injected second-delete fault")
            return original(sql, parameters)

        with (
            patch.object(connector, "execute_affected", side_effect=fail_second_delete),
            pytest.raises(RuntimeError, match="injected"),
        ):
            _advance(connector, gate, cycle, 1, b"4" * 32, now=6)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_request_owners "
            "WHERE request_sha256 = %s",
            (request,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_requests "
            "WHERE request_sha256 = %s",
            (request,),
        ) == (1,)

        committed = _advance(connector, gate, cycle, 1, b"4" * 32, now=7)
        assert committed.row_count == 1
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_gallery_observation_staging_requests "
                "WHERE request_sha256 = %s",
                (request,),
            )
            == ()
        )
    finally:
        connector.close()


def test_cleanup_sql_is_bounded_static_and_has_portable_mariadb_lock_shape(
    tmp_path: Path,
) -> None:
    source = Path(cleanup_module.__file__).read_text(encoding="utf-8").upper()
    assert "COUNT(" not in source
    assert "SUM(" not in source
    assert "SELECT RELATION" not in source
    assert "SELECT PREDICATE" not in source
    for removed_surface in (
        "CATALOG_ARTIFACT_IDENTITY",
        "FROM CATALOG_ARTIFACT_SEMANTIC_INPUT AS",
        "FROM CATALOG_ARTIFACT_DELTA_OLD",
        "DELETE FROM CATALOG_ARTIFACT_DELTA_OLD",
        "DELETE FROM CATALOG_ARTIFACT_DELTA_NEW",
    ):
        assert removed_surface not in source

    connector = _database(tmp_path / "cleanup-explain.sqlite3")
    try:
        target = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN]
        spec = target.phases["AR_ROOT"][0]
        sql = cleanup_module._static_select_sql(
            target, spec, exact=False, has_after=False
        )
        parameters = cleanup_module._static_shard_parameters(
            target,
            CleanupCycle(
                cleanup_id=cleanup_module._cleanup_id(
                    CleanupTargetKind.ANALYSIS_RUN, 0, 1
                ),
                target_kind=CleanupTargetKind.ANALYSIS_RUN,
                shard_no=0,
                target_key=cleanup_module._target_key(
                    CleanupTargetKind.ANALYSIS_RUN, 0
                ),
                cycle_generation=1,
                cycle_cutoff_at=100,
                max_rows_per_transaction=8,
                hash_cache_max_age_microseconds=0,
            ),
        ) + (8,)
        plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
        assert plan
        assert any("INDEX" in str(row[-1]).upper() for row in plan)
    finally:
        connector.close()

    class _MariaRecorder:
        def __init__(self) -> None:
            self.query = ""

        def fetch_one(
            self, query: str, parameters: tuple[object, ...]
        ) -> tuple[object, ...]:
            self.query = query
            return ()

    recorder = _MariaRecorder()
    work = VNextUnitOfWork(recorder, backend="mariadb")  # type: ignore[arg-type]
    work.lock_row(
        LockRank.CHILD,
        b"cleanup-mariadb-shape",
        "SELECT candidate_id FROM catalog_publication_candidates "
        "WHERE candidate_id = %s",
        (b"c" * 16,),
    )
    assert recorder.query.endswith(" FOR UPDATE")
    assert "%s" in recorder.query and "?" not in recorder.query
