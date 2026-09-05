from __future__ import annotations

from hashlib import sha256
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
    seed_tag_term,
)
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_gallery_page_fixtures import (
    seed_gallery_page_bounds,
    seed_gallery_page_descriptor,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import (
    seed_gallery_manifest,
    seed_sealed_source_build,
    seed_snapshot_manifest,
    seed_source_build,
)
from vnext_publication_fixtures import (
    seed_publication_candidate,
    seed_publication_commit,
    seed_publication_finalization,
)

import h2hdb.operational_refinement as operational_refinement_module
import h2hdb.vnext_cleanup_repository as cleanup_module
from h2hdb import vnext_identity as identity
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

_ARTIFACT_ADAPTER_ID = b"test-artifact-adapter"
_ARTIFACT_POLICY_FINGERPRINT = b"p" * 32


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


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


def _seed_terminal_retirement_authority(
    connector: SQLiteConnector,
    *,
    staging_id: bytes,
    build_id: bytes,
    gallery_id: int,
    provisional_observation_id: int,
    final_observation_id: int,
    file_count: int,
    byte_count: int,
) -> bytes:
    """Seed the exact durable facts generic staging cleanup must revalidate."""

    seed_manifest_policy(connector)
    if not connector.fetch_one(
        "SELECT 1 FROM catalog_source_build_descriptor WHERE build_id = %s",
        (build_id,),
    ):
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_source_build_descriptor "
                    "(build_id, scope_key, manifest_policy_id, created_at) "
                    "VALUES (%s, %s, 1, 0)",
                    (build_id, sha256(b"scope:" + build_id).digest()),
                ),
                (
                    "INSERT INTO catalog_source_build_states (build_id, state) "
                    "VALUES (%s, 'OPEN')",
                    (build_id,),
                ),
            ],
        )

    missing_allocations = [
        observation_id
        for observation_id in dict.fromkeys(
            (provisional_observation_id, final_observation_id)
        )
        if not connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_allocations "
            "WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, observation_id),
        )
    ]
    if missing_allocations:
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, 0)",
                    (gallery_id, observation_id),
                )
                for observation_id in missing_allocations
            ],
        )

    roots: dict[bytes, tuple[bytes, int]] = {}
    for component, count in (
        (b"METADATA", 0),
        (b"FILE", file_count),
        (b"TAG", 0),
        (b"DIRECTORY", file_count),
    ):
        page_bytes = b"cleanup-terminal-root:" + staging_id + b":" + component
        page_sha256 = sha256(page_bytes).digest()
        seed_gallery_page_descriptor(
            connector,
            page_sha256=page_sha256,
            page_bytes=page_bytes,
            component=component,
            level=0,
            subtree_item_count=count,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_tree_roots "
            "(gallery_id, observation_id, root_page_sha256) VALUES (%s, %s, %s)",
            (gallery_id, provisional_observation_id, page_sha256),
        )
        roots[component] = (page_sha256, count)

    descriptor = identity.GalleryObservationDescriptor(
        roots[b"METADATA"][0],
        roots[b"METADATA"][1],
        roots[b"FILE"][0],
        roots[b"FILE"][1],
        roots[b"TAG"][0],
        roots[b"TAG"][1],
        roots[b"DIRECTORY"][0],
        roots[b"DIRECTORY"][1],
    )
    observation_identity = identity.gallery_observation_descriptor_digest(descriptor)
    descriptor_bytes = identity.encode_gallery_observation_descriptor(descriptor)
    canonical_tree = identity.build_canonical_value_tree(
        observation_identity,
        len(descriptor_bytes),
        (descriptor_bytes,),
    )
    assert len(canonical_tree.pages) == 1
    canonical_page = canonical_tree.pages[0]
    seed_canonical_value(
        connector,
        value_sha256=observation_identity,
        digest_domain=b"gallery_observation_v1",
        page_sha256=canonical_page.page_sha256,
        page_bytes=canonical_page.page_bytes,
        subtree_item_count=len(descriptor_bytes),
        allocated_at=0,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observations "
        "(gallery_id, observation_id, observation_identity_sha256) "
        "VALUES (%s, %s, %s)",
        (gallery_id, final_observation_id, observation_identity),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat "
        "(gallery_id, observation_id, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (gallery_id, final_observation_id, file_count, byte_count),
    )
    seed_gallery_manifest(
        connector,
        gallery_id=gallery_id,
        observation_id=final_observation_id,
        manifest_policy_id=1,
        manifest_sha256=identity.artifact_source_manifest_digest(
            observation_identity,
            1,
            1,
        ),
        computed_at=1,
    )
    link = connector.fetch_one(
        "SELECT observation_id FROM catalog_source_build_galleries "
        "WHERE build_id = %s AND gallery_id = %s",
        (build_id, gallery_id),
    )
    if not link:
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            (build_id, gallery_id, final_observation_id),
        )
    elif link != (final_observation_id,):
        raise AssertionError("terminal retirement fixture link differs")
    request_count = connector.fetch_one(
        "SELECT COUNT(*) FROM operational_gallery_observation_staging_requests "
        "WHERE staging_id = %s",
        (staging_id,),
    )[0]
    assert isinstance(request_count, int)
    if request_count:
        connector.execute(
            "UPDATE operational_gallery_observation_staging_request_budgets "
            "SET retained_request_count = retained_request_count + %s "
            "WHERE singleton_id = 1",
            (request_count,),
        )
    return observation_identity


def _seed_publication_commit_cleanup_history(
    connector: SQLiteConnector,
    *,
    finalize_replacement: bool = True,
    additional_old_receipt: bytes | None = None,
) -> tuple[bytes, bytes]:
    old_receipt = bytes.fromhex("21" * 16)
    replacement_receipt = bytes.fromhex("22" * 16)
    history = (
        ((1, old_receipt), (2, replacement_receipt))
        if additional_old_receipt is None
        else ((1, old_receipt), (2, additional_old_receipt), (3, replacement_receipt))
    )
    statements: list[tuple[str, tuple[object, ...]]] = []
    for revision, receipt_id in history:
        candidate_id = bytes((40 + revision,)) * 16
        preparation_id = bytes((50 + revision,)) * 16
        event_id = bytes((60 + revision,)) * 16
        statements.extend(
            (
                (
                    "INSERT INTO catalog_revision_descriptors "
                    "(revision, publication_count, artifact_count) "
                    "VALUES (%s, 0, 0)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_source_revision_descriptors "
                    "(source_revision, channel, snapshot_manifest_sha256) "
                    "VALUES (%s, %s, %s)",
                    (revision, b"default", bytes((revision,)) * 32),
                ),
                (
                    "INSERT INTO catalog_publication_generation_nodes "
                    "(generation) VALUES (%s)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_publication_commit_anchors "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                ),
                (
                    "INSERT INTO catalog_publication_finalization_checkpoints "
                    "(receipt_id, generation, `cursor`, processed_count, state, "
                    "updated_at) VALUES (%s, 2, %s, 0, 'COMPLETE', %s)",
                    (receipt_id, b"", 10 + revision),
                ),
                (
                    "INSERT INTO catalog_publication_finalization_batch_stored "
                    "(receipt_id, start_generation, batch_key, start_cursor, "
                    "start_processed_count, next_cursor, row_count, committed_at) "
                    "VALUES (%s, 1, %s, %s, 0, %s, 0, %s)",
                    (receipt_id, bytes((revision,)), b"", b"", 10 + revision),
                ),
                (
                    "INSERT INTO operational_operational_event_streams "
                    "(preparation_id, created_at) VALUES (%s, %s)",
                    (preparation_id, 5 + revision),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', %s, %s)",
                    (
                        preparation_id,
                        bytes((70 + revision,)) * 16,
                        6 + revision,
                        7 + revision,
                    ),
                ),
                (
                    "INSERT INTO operational_operational_preparation_checkpoints "
                    "(preparation_id, phase, generation, cursor_bytes, "
                    "processed_count, chain_sha256, state, updated_at) "
                    "VALUES (%s, 'REMOVED_GID', 2, %s, 1, %s, 'COMPLETE', %s)",
                    (preparation_id, b"", bytes((80 + revision,)) * 32, 8 + revision),
                ),
                (
                    "INSERT INTO operational_operational_preparation_batch_receipts "
                    "(preparation_id, phase, batch_key, start_cursor, next_cursor, "
                    "input_sha256, output_sha256, row_count, committed_generation, "
                    "committed_at) VALUES (%s, 'REMOVED_GID', %s, %s, %s, %s, "
                    "%s, 0, 2, %s)",
                    (
                        preparation_id,
                        bytes((90 + revision,)) * 32,
                        b"",
                        b"",
                        bytes((100 + revision,)) * 32,
                        bytes((110 + revision,)) * 32,
                        9 + revision,
                    ),
                ),
                (
                    "INSERT INTO operational_operational_preparation_effect_seals "
                    "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                    "VALUES (%s, 1, %s, %s)",
                    (preparation_id, bytes((120 + revision,)) * 32, 10 + revision),
                ),
                (
                    "INSERT INTO operational_publication_candidate_preparations "
                    "(candidate_id, preparation_id, bound_at) VALUES (%s, %s, %s)",
                    (candidate_id, preparation_id, 10 + revision),
                ),
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 0, 'REMOVED_GID', %s, %s)",
                    (
                        event_id,
                        preparation_id,
                        bytes((130 + revision,)) * 32,
                        10 + revision,
                    ),
                ),
                (
                    "INSERT INTO operational_operational_removed_gid_events "
                    "(event_id, gid, request_token) VALUES (%s, %s, %s)",
                    (event_id, revision, bytes((140 + revision,)) * 16),
                ),
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, %s, %s, %s, %s, 1, 1, 1, "
                    "0, 0, 0, 0, %s)",
                    (
                        receipt_id,
                        candidate_id,
                        revision,
                        revision,
                        revision,
                        preparation_id,
                        10 + revision,
                    ),
                ),
            )
        )
        if receipt_id != replacement_receipt or finalize_replacement:
            statements.append(
                (
                    "INSERT INTO catalog_publication_commit_finalizations "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                )
            )
    statements.extend(
        (
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            (successor, successor - 1),
        )
        for successor in range(2, len(history) + 1)
    )
    statements.append(
        (
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", replacement_receipt),
        )
    )
    _fixture_rows(connector, statements)
    return old_receipt, replacement_receipt


def _seed_finalized_cleanup_publication_commit(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    source_revision: int = 1,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=candidate_id,
            revision=source_revision,
            source_revision=source_revision,
            generation=source_revision,
            preparation_id=b"p" * 16,
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=0,
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=1,
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_finalizations "
            "(receipt_id) VALUES (%s)",
            (receipt_id,),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _seed_publication_commit_source_build_base(
    connector: SQLiteConnector,
    *,
    base_receipt: bytes,
    handoff_receipt: bytes,
    discriminator: int = 70,
    build_state: str | None = "SEALED",
    analysis_states: tuple[str | None, ...] = ("COMPLETE",),
    published_analysis_index: int | None = 0,
    working: bool = False,
) -> tuple[bytes, tuple[bytes, ...]]:
    handoff = connector.fetch_one(
        "SELECT source_revision FROM catalog_publication_commits WHERE receipt_id = %s",
        (handoff_receipt,),
    )
    assert len(handoff) == 1 and isinstance(handoff[0], int)
    handoff_source_revision = handoff[0]
    build_id = bytes((discriminator,)) * 16
    analysis_ids = tuple(
        bytes((discriminator + index + 1,)) * 16
        for index in range(len(analysis_states))
    )
    statements: list[tuple[str, tuple[object, ...]]] = [
        (
            "INSERT INTO catalog_source_build_descriptor "
            "(build_id, scope_key, manifest_policy_id, created_at) "
            "VALUES (%s, %s, 1, 1)",
            (build_id, bytes((discriminator,)) * 32),
        ),
        (
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_id, base_receipt),
        ),
    ]
    if build_state is not None:
        statements.append(
            (
                "INSERT INTO catalog_source_build_states (build_id, state) "
                "VALUES (%s, %s)",
                (build_id, build_state),
            )
        )
    for index, (analysis_id, analysis_state) in enumerate(
        zip(analysis_ids, analysis_states, strict=True),
        start=1,
    ):
        statements.append(
            (
                "INSERT INTO catalog_analysis_run_descriptor "
                "(analysis_id, build_id, policy_id, input_manifest_sha256, "
                "started_at) VALUES (%s, %s, %s, %s, 1)",
                (analysis_id, build_id, index, bytes((discriminator + index,)) * 32),
            )
        )
        if analysis_state is not None:
            statements.append(
                (
                    "INSERT INTO catalog_analysis_run_states "
                    "(analysis_id, state) VALUES (%s, %s)",
                    (analysis_id, analysis_state),
                )
            )
    if published_analysis_index is not None:
        statements.append(
            (
                "INSERT INTO catalog_source_revision_provenance "
                "(source_revision, analysis_id) VALUES (%s, %s)",
                (
                    handoff_source_revision,
                    analysis_ids[published_analysis_index],
                ),
            )
        )
    if working:
        statements.append(
            (
                "INSERT INTO operational_source_working_builds "
                "(slot, build_id, assigned_at) VALUES (1, %s, 1)",
                (build_id,),
            )
        )
    _fixture_rows(connector, statements)
    return build_id, analysis_ids


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
    "catalog_title_search_postings",
    "catalog_search_postings",
    "catalog_search_documents",
    "catalog_pages",
    "catalog_thumbnails",
    "catalog_storage_objects",
    "catalog_publication_storage",
    "catalog_publication_download_times",
    "catalog_contributors",
    "catalog_publication_order",
    "catalog_publication_contents",
    "catalog_subjects",
    "catalog_artifacts",
    "catalog_publication_occurrence_identities",
)
_CATALOG_PUBLICATION_PAYLOAD_COUNTS = {
    table: 1 for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES
}
_CATALOG_PUBLICATION_PAYLOAD_COUNTS.update(
    {
        "catalog_storage_objects": 2,
    }
)


def _finalize_publication_receipt(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    cursor: bytes,
    processed_count: int,
    finalized_at: int,
) -> None:
    seed_publication_finalization(
        connector,
        receipt_id=receipt_id,
        cursor=cursor,
        processed_count=processed_count,
        finalized_at=finalized_at,
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
                    "INSERT INTO catalog_revision_descriptors "
                    "(revision, publication_count, artifact_count) "
                    "VALUES (%s, 1, 1)",
                    (revision,),
                ),
                (
                    "INSERT INTO catalog_source_revision_descriptors "
                    "(source_revision, channel, snapshot_manifest_sha256) "
                    "VALUES (%s, %s, %s)",
                    (revision, b"default", bytes((revision,)) * 32),
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
                    "INSERT INTO catalog_publication_download_times "
                    "(catalog_occurrence_sha256, download_time) VALUES (%s, %s)",
                    (
                        identity.catalog_publication_occurrence_sha256(
                            revision, publication_key
                        ),
                        revision,
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
                    "INSERT INTO catalog_contributors "
                    "(revision, publication_key, contributor_name_sha256, role, position) "
                    "VALUES (%s, %s, %s, %s, 0)",
                    (revision, publication_key, b"a" * 32, b"artist"),
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
                    "artifact_semantics_sha256, artifact_name, media_type, "
                    "page_count) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                    (
                        revision,
                        publication_key,
                        b"b" * 32,
                        b"m" * 32,
                        b"artifact.bin",
                        b"application/octet-stream",
                    ),
                ),
                (
                    "INSERT INTO catalog_storage_objects "
                    "(revision, publication_key, resource_kind, "
                    "storage_object_key_sha256, storage_object_sha256, "
                    "size_bytes, modified_at) VALUES (%s, %s, %s, %s, %s, 4, 1)",
                    (revision, publication_key, b"acquisition", b"k" * 32, b"b" * 32),
                ),
                (
                    "INSERT INTO catalog_storage_objects "
                    "(revision, publication_key, resource_kind, "
                    "storage_object_key_sha256, storage_object_sha256, "
                    "size_bytes, modified_at) VALUES (%s, %s, %s, %s, %s, 4, 1)",
                    (revision, publication_key, b"thumbnail", b"q" * 32, b"h" * 32),
                ),
                (
                    "INSERT INTO catalog_pages "
                    "(revision, publication_key, resource_kind, page_index, "
                    "extent_offset, extent_length, media_type, image_sha256, "
                    "width, height) VALUES (%s, %s, %s, 0, 0, 4, %s, %s, 1, 1)",
                    (
                        revision,
                        publication_key,
                        b"acquisition",
                        b"image/jpeg",
                        b"i" * 32,
                    ),
                ),
                (
                    "INSERT INTO catalog_thumbnails "
                    "(revision, publication_key, resource_kind, extent_offset, "
                    "extent_length, media_type, image_sha256, width, height) "
                    "VALUES (%s, %s, %s, 0, 4, %s, %s, 1, 1)",
                    (revision, publication_key, b"thumbnail", b"image/jpeg", b"h" * 32),
                ),
                (
                    "INSERT INTO catalog_search_documents "
                    "(revision, publication_key, row_count) VALUES (%s, %s, 1)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_search_postings "
                    "(revision, value_sha256, publication_key) VALUES (%s, %s, %s)",
                    (revision, b"z" * 32, publication_key),
                ),
                (
                    "INSERT INTO catalog_title_search_postings "
                    "(revision, value_sha256, publication_key) VALUES (%s, %s, %s)",
                    (revision, b"z" * 32, publication_key),
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


def _seed_cleanup_sealed_source_build(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scope_key: bytes,
    manifest_sha256: bytes | None = None,
    gallery_count: int = 0,
    file_count: int = 0,
    byte_count: int = 0,
    manifest_policy_id: int = 1,
    created_at: int = 0,
    sealed_at: int = 0,
) -> None:
    seed_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
        manifest_sha256=(
            build_id + build_id if manifest_sha256 is None else manifest_sha256
        ),
        gallery_count=gallery_count,
        file_count=file_count,
        byte_count=byte_count,
        manifest_policy_id=manifest_policy_id,
        created_at=created_at,
        sealed_at=sealed_at,
    )


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
    _seed_cleanup_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
        manifest_policy_id=1,
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
    for phase_index in range(2):
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
        connector.fetch_all(
            "SELECT * FROM operational_cleanup_jobs ORDER BY cleanup_id"
        ),
        connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints"),
    )


def _source_scope_family_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return (connector.fetch_all("SELECT * FROM catalog_source_scopes"),)


def _artifact_policy_semantics_family_rows(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return (connector.fetch_all("SELECT * FROM catalog_artifact_policy_semantics"),)


_CANONICAL_PAGE_COMPONENT_TABLES = (
    "catalog_canonical_value_page_seals",
    "catalog_canonical_value_page_subtree_item_counts",
    "catalog_canonical_value_page_coordinates",
    "catalog_canonical_value_page_payloads",
    "catalog_canonical_value_page_anchors",
)


def _canonical_page_component_rows(
    connector: SQLiteConnector,
    page_sha256: bytes,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(
            f"SELECT * FROM {table} WHERE page_sha256 = %s",
            (page_sha256,),
        )
        for table in _CANONICAL_PAGE_COMPONENT_TABLES
    )


def _seed_cleanup_candidate(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    reserved_revision: int = 1,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        seed_publication_candidate(
            connector,
            candidate_id=candidate_id,
            analysis_id=b"a" * 16,
            reserved_revision=reserved_revision,
            artifact_policy_id=1,
            display_title_policy_id=1,
            artifacts_required=False,
            created_at=0,
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


_CANDIDATE_DEFINITION_DELETE_ORDER = ("catalog_publication_candidates",)


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
    resource_kind = b"acquisition"
    storage_key_sha256 = b"k" * 32
    statements: list[tuple[str, tuple[object, ...]]] = [
        (
            "INSERT INTO catalog_artifact_blobs "
            "(artifact_sha256, size_bytes) VALUES (%s, 4)",
            (artifact_sha256,),
        ),
        (
            "INSERT INTO catalog_prepared_resource_blob "
            "(candidate_id, publication_key, resource_kind, storage_object_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (candidate_id, publication_key, resource_kind, artifact_sha256),
        ),
        (
            "INSERT INTO catalog_prepared_artifacts "
            "(candidate_id, publication_key, resource_kind, "
            "storage_object_key_sha256, storage_generation, protection_token, state) "
            "VALUES (%s, %s, %s, %s, 7, %s, %s)",
            (
                candidate_id,
                publication_key,
                resource_kind,
                storage_key_sha256,
                b"t" * 32,
                state,
            ),
        ),
    ]
    if state != "PENDING":
        statements.append(
            (
                "INSERT INTO catalog_prepared_storage_objects "
                "(candidate_id, publication_key, resource_kind, "
                "storage_object_sha256, size_bytes, modified_at) "
                "VALUES (%s, %s, %s, %s, 4, 1)",
                (candidate_id, publication_key, resource_kind, artifact_sha256),
            )
        )
    _fixture_rows(connector, statements)


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
        connector.fetch_all(
            "SELECT * FROM catalog_prepared_resource_blob "
            "WHERE candidate_id = %s AND publication_key = %s",
            (candidate_id, publication_key),
        ),
        connector.fetch_all(
            "SELECT * FROM catalog_prepared_storage_objects "
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
    return (connector.fetch_all("SELECT * FROM catalog_source_build_discoveries"),)


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
            "catalog_analysis_run_states",
            "catalog_analysis_run_descriptor",
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
            _seed_cleanup_sealed_source_build(
                connector,
                build_id=build_id,
                scope_key=scope_key,
                manifest_policy_id=1,
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


def test_cleanup_predicates_fail_closed_for_sibling_analysis_corruption() -> None:
    """Retain defense for audit-bypassing corruption unreachable in epoch 3."""

    assert (
        cleanup_module._SOURCE_BUILD_REACHABILITY_ELIGIBILITY.count(
            "catalog_analysis_run_descriptor sibling"
        )
        == 1
    )
    assert (
        cleanup_module._ANALYSIS_RUN_REACHABILITY_ELIGIBILITY.count(
            "catalog_analysis_run_descriptor sibling"
        )
        == 2
    )
    for predicate in (
        cleanup_module._SOURCE_BUILD_REACHABILITY_ELIGIBILITY,
        cleanup_module._ANALYSIS_RUN_REACHABILITY_ELIGIBILITY,
    ):
        assert "sibling.analysis_id <> retired.analysis_id" in predicate
    safe_release = cleanup_module._PUBLICATION_COMMIT_SAFE_BUILD_BASE_RELEASE
    assert "FROM catalog_analysis_run_descriptor analysis" in safe_release
    assert "provenance.analysis_id = analysis.analysis_id" in safe_release
    assert "analysis_state.state NOT IN ('COMPLETE', 'ABANDONED')" in safe_release


def test_candidate_cleanup_retains_historical_source_build_base_lineage(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "candidate-source-base-retention.sqlite3")
    try:
        candidate_id = bytes((103,)) + b"c" * 15
        receipt_id = b"R" * 16
        base_build = b"B" * 16
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _seed_finalized_cleanup_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=candidate_id,
        )
        _fixture_rows(
            connector,
            [
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


def test_current_only_source_build_waits_for_publication_base_release_then_rewinds(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "current-only-priority-rewind.sqlite3")
    try:
        candidate_id = bytes((105,)) + b"c" * 15
        build_id = bytes((106,)) + b"b" * 15
        receipt_id = b"R" * 16
        scope_key = _seed_source_build_scope(connector, discriminator=106)
        _seed_cleanup_sealed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            manifest_policy_id=1,
            created_at=1,
            sealed_at=1,
        )
        _seed_cleanup_candidate(connector, candidate_id=candidate_id)
        _seed_finalized_cleanup_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=candidate_id,
        )
        _fixture_rows(
            connector,
            [
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
        assert first is None
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_source_build_descriptor WHERE build_id = %s",
            (build_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidates "
            "WHERE candidate_id = %s",
            (candidate_id,),
        ) == (candidate_id,)

        connector.execute(
            "DELETE FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (build_id,),
        )
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

        with connector.transaction():
            third = VNextCleanupRepository.next_current_only_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle_cutoff_at=100,
                now=200,
            )
        assert third is not None
        assert third.target_kind is CleanupTargetKind.SOURCE_BUILD
        _drain(connector, gate, third, now=201)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_descriptor WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
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
        _seed_cleanup_sealed_source_build(
            connector,
            build_id=analysis_build,
            scope_key=scope_key,
            manifest_policy_id=1,
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
        _seed_finalized_cleanup_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=b"c" * 16,
        )
        _fixture_rows(
            connector,
            [
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
            if state == "SEALED":
                _seed_cleanup_sealed_source_build(
                    connector,
                    build_id=build_id,
                    scope_key=scope_key,
                    manifest_policy_id=1,
                    created_at=created_at,
                    sealed_at=created_at,
                )
            else:
                seed_source_build(
                    connector,
                    build_id=build_id,
                    scope_key=scope_key,
                    manifest_policy_id=1,
                    state=state,
                    created_at=created_at,
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
                "SELECT build_id FROM catalog_source_build_descriptor "
                "WHERE build_id = %s",
                (newer_build,),
            )
            == ()
        )
    finally:
        connector.close()


def _observation_vertical_rows(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
) -> tuple[list[tuple[object, ...]], ...]:
    return tuple(
        connector.fetch_all(
            query + " WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, observation_id),
        )
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
                "SELECT 1 FROM operational_cleanup_jobs "
                "WHERE cleanup_id = %s AND state = 'COMPLETE'",
                (cycle.cleanup_id,),
            )
            == ()
        )
    finally:
        connector.close()


def test_cleanup_successor_cas_and_transaction_rollback_preserve_attempt_identity(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-successor-identity.sqlite3")
    kind = CleanupTargetKind.ARTIFACT_BLOB
    shard = 52
    try:
        gate = _exclusive(connector)
        first = _begin(connector, gate, kind, shard, max_rows=1)
        completed = _advance(connector, gate, first, 1, b"c" * 32, now=3)
        assert completed.cycle_complete
        before = _cleanup_protocol_snapshot(connector)

        with pytest.raises(RuntimeError, match="abort cleanup successor"):
            with connector.transaction():
                successor = VNextCleanupRepository.begin_cycle(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    target_kind=kind,
                    shard_no=shard,
                    cycle_cutoff_at=100,
                    max_rows_per_transaction=1,
                    now=4,
                )
                assert successor.cycle_generation == first.cycle_generation + 1
                assert successor.cleanup_id == cleanup_module._cleanup_id(
                    kind,
                    shard,
                    successor.cycle_generation,
                )
                assert successor.cleanup_id != first.cleanup_id
                raise RuntimeError("abort cleanup successor")

        assert _cleanup_protocol_snapshot(connector) == before

        with (
            patch.object(connector, "execute_affected", return_value=0),
            pytest.raises(
                CleanupUnavailableError,
                match="completed cleanup cycle changed",
            ),
        ):
            _begin(connector, gate, kind, shard, max_rows=1, now=5)
        assert _cleanup_protocol_snapshot(connector) == before

        successor = _begin(connector, gate, kind, shard, max_rows=1, now=6)
        assert successor.cycle_generation == first.cycle_generation + 1
        assert successor.cleanup_id == cleanup_module._cleanup_id(
            kind,
            shard,
            successor.cycle_generation,
        )
        assert successor.cleanup_id != first.cleanup_id
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


def test_all_twenty_three_strategies_match_the_closed_phase_registry(
    tmp_path: Path,
) -> None:
    expected = {
        CleanupTargetKind.SOURCE_BUILD: (
            "SB_CANONICAL_UPLOAD",
            "SB_GALLERY",
            "SB_DISCOVERY",
            "SB_SATELLITES",
            "SB_GENERATION",
            "SB_STATE",
            "SB_ROOT",
        ),
        CleanupTargetKind.ANALYSIS_RUN: (
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
        CleanupTargetKind.CATALOG_PUBLICATION: (
            "CP_STORAGE",
            "CP_DOWNLOAD_TIME",
            "CP_CONTRIBUTOR",
            "CP_ORDER",
            "CP_CONTENT",
            "CP_SUBJECT",
            "CP_ARTIFACT",
            "CP_ROOT",
        ),
        CleanupTargetKind.PUBLICATION_COMMIT: (
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
        CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR: ("CRD_ROOT",),
        CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR: ("SRD_ROOT",),
        CleanupTargetKind.PUBLICATION_GENERATION: ("PG_EDGE", "PG_ROOT"),
        CleanupTargetKind.PUBLICATION_CANDIDATE: (
            "PC_SEALS",
            "PC_PREPARED",
            "PC_INPUT",
            "PC_CHECKPOINT",
            "PC_SELECTION_STORAGE",
            "PC_CONTENT",
            "PC_SUBJECT",
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
        CleanupTargetKind.STORAGE_OBJECT_KEY: ("SK_SEGMENT", "SK_ROOT"),
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


def test_frozen_root_set_corruption_and_serialized_open_cycle_fail_closed(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-frozen-root-corruption.sqlite3")
    source = bytes((37,)) + b"s" * 31
    fingerprint = b"f" * 32
    try:
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
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, "
                    "cached_at) VALUES (%s, %s, %s, 10)",
                    (source, fingerprint, b"z" * 32),
                ),
            ],
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            37,
            max_rows=8,
        )
        frame_row = connector.fetch_one(
            "SELECT frozen_root_key FROM operational_cleanup_cycle_roots "
            "WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        )
        assert frame_row is not None
        frame = cast(bytes, frame_row[0])
        assert len(frame) == 72
        assert connector.fetch_one(
            "SELECT frozen_root_count FROM operational_cleanup_jobs "
            "WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (1,)

        with pytest.raises(
            CleanupUnavailableError,
            match="another serialized cleanup cycle",
        ):
            _begin(
                connector,
                gate,
                CleanupTargetKind.CONTENT_BLOB,
                0,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_jobs WHERE state = 'OPEN'"
        ) == (1,)

        with connector.transaction():
            connector.execute(
                "UPDATE operational_cleanup_cycle_roots "
                "SET frozen_root_key = %s WHERE cleanup_id = %s",
                (b"\x02" + frame[1:], cycle.cleanup_id),
            )
        with pytest.raises(CleanupCorruptionError, match="codec|digest"):
            _advance(connector, gate, cycle, 1, b"i" * 32, now=3)
        assert connector.fetch_one(
            "SELECT file_sha256 FROM operational_file_hash_caches "
            "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
            (source, fingerprint),
        ) == (b"z" * 32,)

        duplicate_digest = cleanup_module._frozen_root_set_sha256(
            cycle.cleanup_id,
            (frame, frame),
        )
        with connector.transaction():
            connector.execute(
                "UPDATE operational_cleanup_cycle_roots "
                "SET frozen_root_key = %s WHERE cleanup_id = %s",
                (frame, cycle.cleanup_id),
            )
            connector.execute(
                "UPDATE operational_cleanup_jobs "
                "SET frozen_root_count = 2, frozen_root_set_sha256 = %s "
                "WHERE cleanup_id = %s",
                (duplicate_digest, cycle.cleanup_id),
            )
        original_fetch_all = connector.fetch_all

        def duplicate_membership(
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> list[tuple[Any, ...]]:
            if "SELECT frozen_root_key FROM operational_cleanup_cycle_roots" in sql:
                return [(frame,), (frame,)]
            return original_fetch_all(sql, parameters)

        with (
            patch.object(connector, "fetch_all", side_effect=duplicate_membership),
            pytest.raises(CleanupCorruptionError, match="duplicated"),
        ):
            _advance(connector, gate, cycle, 1, b"d" * 32, now=4)
    finally:
        connector.close()


def test_frozen_root_source_gallery_name_has_exact_260_byte_boundary() -> None:
    plan = cleanup_module._STATIC_PLANS[CleanupTargetKind.SOURCE_GALLERY_NAME_GID]
    maximum_name = b"x" * 255
    cleanup_module._validate_frozen_root_values(plan, (maximum_name,))
    encoded = cleanup_module._encode_frozen_root_key((maximum_name,))
    assert len(encoded) == 260
    assert cleanup_module._decode_frozen_root_key(
        encoded,
        root_arity=1,
    ) == (maximum_name,)

    with pytest.raises(ValueError, match=r"source_gallery_name.*1\.\.255"):
        cleanup_module._validate_frozen_root_values(plan, (b"x" * 256,))


def test_frozen_root_set_accepts_exact_256_root_boundary(tmp_path: Path) -> None:
    connector = _database(tmp_path / "cleanup-frozen-root-256.sqlite3")
    fingerprint = b"f" * 32
    roots = [
        (bytes((37,)) + index.to_bytes(31, "big"), fingerprint) for index in range(256)
    ]
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute_many(
                "INSERT INTO operational_hash_cache_observations "
                "(source_identity_sha256, fingerprint_sha256, observed_at) "
                "VALUES (%s, %s, 10)",
                roots,
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            37,
            max_rows=256,
        )
        assert connector.fetch_one(
            "SELECT frozen_root_count FROM operational_cleanup_jobs "
            "WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (256,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_cycle_roots "
            "WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (256,)

        file_terminal = _advance(connector, gate, cycle, 1, b"f" * 32, now=3)
        assert file_terminal.phase == "HC_ROOT" and file_terminal.generation == 1
        deleted = _advance(connector, gate, cycle, 1, b"r" * 32, now=4)
        assert deleted.row_count == 256 and deleted.generation == 2
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_hash_cache_observations"
        ) == (0,)

        assert cleanup_module._require_frozen_root_count(256) == 256
        with pytest.raises(CleanupCorruptionError, match="hard cap"):
            cleanup_module._require_frozen_root_count(257)
    finally:
        connector.close()


def test_frozen_root_terminal_completion_rolls_back_atomically(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-frozen-root-rollback.sqlite3")
    source = bytes((38,)) + b"s" * 31
    fingerprint = b"f" * 32
    try:
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
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, "
                    "cached_at) VALUES (%s, %s, %s, 10)",
                    (source, fingerprint, b"z" * 32),
                ),
            ],
        )
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            38,
            max_rows=1,
        )
        first = _advance(connector, gate, cycle, 1, b"1" * 32, now=3)
        assert first.generation == 2 and first.phase == "HC_FILE"
        second = _advance(connector, gate, cycle, 2, b"2" * 32, now=4)
        assert second.generation == 1 and second.phase == "HC_ROOT"
        third = _advance(connector, gate, cycle, 1, b"3" * 32, now=5)
        assert third.generation == 2 and third.phase == "HC_ROOT"

        command = CleanupBatchCommand(b"4" * 32, 2)
        with pytest.raises(RuntimeError, match="abort frozen completion"):
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=cycle,
                    command=command,
                    now=6,
                )
                assert result.cycle_complete
                raise RuntimeError("abort frozen completion")

        assert connector.fetch_one(
            "SELECT state FROM operational_cleanup_jobs WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_cycle_roots "
            "WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT final_chain_sha256, final_deleted_count "
            "FROM operational_cleanup_jobs WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (None, None)

        committed = _advance(connector, gate, cycle, 2, b"4" * 32, now=7)
        assert committed.cycle_complete and not committed.replayed
        replay = _advance(connector, gate, cycle, 2, b"4" * 32, now=8)
        assert replay.cycle_complete and replay.replayed
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s",
                (cycle.cleanup_id,),
            )
            == ()
        )
    finally:
        connector.close()


def test_current_only_pipeline_resumes_an_open_hash_cache_cycle(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-hash-handoff.sqlite3")
    source = bytes((39,)) + b"s" * 31
    fingerprint = b"f" * 32
    try:
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_hash_cache_observations "
                    "(source_identity_sha256, fingerprint_sha256, observed_at) "
                    "VALUES (%s, %s, 10)",
                    (source, fingerprint),
                ),
            ],
        )
        gate = _exclusive(connector)
        opened = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            39,
            max_rows=1,
        )
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            assert (
                VNextCleanupRepository.current_only_maintenance_state(
                    work,
                    cycle_cutoff_at=100,
                    gate_lease=gate,
                    now=3,
                )
                is CatalogPublicationMaintenanceState.ACTIONABLE
            )
            resumed = VNextCleanupRepository.next_current_only_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle_cutoff_at=100,
                now=3,
            )
        assert resumed == opened
        assert resumed is not None
        _drain(connector, gate, resumed, now=4)
    finally:
        connector.close()


def test_publication_commit_cleanup_is_child_first_replayable_and_fenced(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-commit-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector
        )
        build_id, _analysis_ids = _seed_publication_commit_source_build_base(
            connector,
            base_receipt=old_receipt,
            handoff_receipt=replacement_receipt,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=1,
        )

        first = _advance(connector, gate, cycle, 1, b"a" * 32, now=3)
        assert first.phase == "PCOM_RELEASE_BUILD_BASE"
        assert first.row_count == 1
        replay = _advance(connector, gate, cycle, 1, b"a" * 32, now=4)
        assert replay.replayed and replay == first.__class__(
            cycle=first.cycle,
            phase=first.phase,
            generation=first.generation,
            cursor=first.cursor,
            deleted_count=first.deleted_count,
            row_count=first.row_count,
            phase_complete=first.phase_complete,
            cycle_complete=first.cycle_complete,
            replayed=True,
        )
        with pytest.raises(CleanupUnavailableError, match="generation is stale"):
            _advance(connector, gate, cycle, 1, b"b" * 32, now=5)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
        for retained_parent in (
            "catalog_publication_commit_finalizations",
            "catalog_publication_commits",
        ):
            assert connector.fetch_one(
                f"SELECT 1 FROM {retained_parent} WHERE receipt_id = %s",
                (old_receipt,),
            ) == (1,)

        assert first.generation is not None
        generation = first.generation
        results = [first]
        for attempt in range(1, 32):
            result = _advance(
                connector,
                gate,
                cycle,
                generation,
                (100 + attempt).to_bytes(32, "big"),
                now=5 + attempt,
            )
            results.append(result)
            if result.cycle_complete:
                break
            assert result.generation is not None
            generation = result.generation
        else:
            raise AssertionError("publication-commit cleanup did not terminate")

        nonempty_phases = tuple(
            result.phase for result in results if result.row_count > 0
        )
        assert nonempty_phases == (
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
        )
        for table in (
            "catalog_publication_commit_finalizations",
            "catalog_publication_finalization_batch_stored",
            "catalog_publication_commits",
            "catalog_publication_finalization_checkpoints",
            "catalog_publication_commit_anchors",
        ):
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE receipt_id = %s",
                (old_receipt,),
            ) == (0,)
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE receipt_id = %s",
                (replacement_receipt,),
            ) == (1,)
        old_preparation = bytes((51,)) * 16
        replacement_preparation = bytes((52,)) * 16
        for table in (
            "operational_operational_event_streams",
            "operational_operational_preparations",
            "operational_operational_preparation_checkpoints",
            "operational_operational_preparation_batch_receipts",
            "operational_operational_preparation_effect_seals",
            "operational_operational_events",
        ):
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE preparation_id = %s",
                (old_preparation,),
            ) == (0,)
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE preparation_id = %s",
                (replacement_preparation,),
            ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_removed_gid_events "
            "WHERE event_id = %s",
            (bytes((61,)) * 16,),
        ) == (0,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_removed_gid_events "
            "WHERE event_id = %s",
            (bytes((62,)) * 16,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_publication_candidate_preparations "
            "WHERE preparation_id = %s",
            (old_preparation,),
        ) == (0,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_publication_candidate_preparations "
            "WHERE preparation_id = %s",
            (replacement_preparation,),
        ) == (1,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )

        with connector.transaction():
            completed = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=50,
            )
        assert completed.cycle_complete and completed.replayed
    finally:
        connector.close()


def test_publication_commit_frozen_root_binds_exact_preparation_authority(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-frozen-preparation.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        wrong_preparation = b"z" * 16
        wrong_frame = cleanup_module._encode_frozen_root_key(
            (old_receipt, wrong_preparation)
        )
        wrong_digest = cleanup_module._frozen_root_set_sha256(
            cycle.cleanup_id,
            (wrong_frame,),
        )
        connector.execute(
            "UPDATE operational_cleanup_cycle_roots SET frozen_root_key = %s "
            "WHERE cleanup_id = %s",
            (wrong_frame, cycle.cleanup_id),
        )
        connector.execute(
            "UPDATE operational_cleanup_jobs SET frozen_root_set_sha256 = %s "
            "WHERE cleanup_id = %s",
            (wrong_digest, cycle.cleanup_id),
        )

        with pytest.raises(
            CleanupCorruptionError,
            match="differs from its frozen authority",
        ):
            _advance(connector, gate, cycle, 1, b"v" * 32, now=3)
        with pytest.raises(
            operational_refinement_module.OperationalSemanticValidationError,
            match="differs from frozen authority",
        ):
            operational_refinement_module._validate_open_pcom_event_transition(
                connector,
                "sqlite",
            )
    finally:
        connector.close()


@pytest.mark.parametrize(
    "corruption",
    ("missing_subtype", "wrong_subtype", "both_subtypes", "missing_base"),
)
def test_publication_commit_event_phase_rejects_partial_or_mismatched_coordinates(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _database(tmp_path / f"publication-event-{corruption}.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PCOM_EVENT")
        event_id = bytes((61,)) * 16
        statements: list[tuple[str, tuple[object, ...]]] = []
        if corruption in {"missing_subtype", "wrong_subtype", "missing_base"}:
            statements.append(
                (
                    "DELETE FROM operational_operational_removed_gid_events "
                    "WHERE event_id = %s",
                    (event_id,),
                )
            )
        if corruption in {"wrong_subtype", "both_subtypes"}:
            statements.append(
                (
                    "INSERT INTO operational_operational_deletion_consumption_events "
                    "(event_id, gid, deletion_request_token) VALUES (%s, 1, %s)",
                    (event_id, b"w" * 16),
                )
            )
        if corruption == "missing_base":
            statements.append(
                (
                    "DELETE FROM operational_operational_events WHERE event_id = %s",
                    (event_id,),
                )
            )
        _fixture_rows(connector, statements)
        before = tuple(
            connector.fetch_all(f"SELECT * FROM {table}")
            for table in (
                "operational_operational_events",
                "operational_operational_removed_gid_events",
                "operational_operational_deletion_consumption_events",
            )
        )

        with pytest.raises(CleanupCorruptionError, match="PCOM EVENT"):
            _advance(connector, gate, cycle, 1, b"e" * 32, now=80)

        after = tuple(
            connector.fetch_all(f"SELECT * FROM {table}")
            for table in (
                "operational_operational_events",
                "operational_operational_removed_gid_events",
                "operational_operational_deletion_consumption_events",
            )
        )
        assert after == before
    finally:
        connector.close()


@pytest.mark.parametrize("failing_delete", ("subtype", "event"))
def test_publication_commit_event_compound_delete_fault_rolls_back(
    tmp_path: Path,
    failing_delete: str,
) -> None:
    connector = _database(
        tmp_path / f"publication-event-fault-{failing_delete}.sqlite3"
    )
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PCOM_EVENT")
        event_id = bytes((61,)) * 16
        original = connector.execute_affected

        def fail_delete(sql: str, data: tuple[object, ...] = ()) -> int:
            is_subtype = sql.startswith(
                "DELETE FROM operational_operational_removed_gid_events"
            )
            is_event = sql.startswith("DELETE FROM operational_operational_events")
            if (failing_delete == "subtype" and is_subtype) or (
                failing_delete == "event" and is_event
            ):
                raise RuntimeError(f"injected {failing_delete} delete fault")
            return original(sql, data)

        with (
            patch.object(connector, "execute_affected", side_effect=fail_delete),
            pytest.raises(RuntimeError, match=f"{failing_delete} delete fault"),
        ):
            _advance(connector, gate, cycle, 1, b"f" * 32, now=80)

        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_events WHERE event_id = %s",
            (event_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_removed_gid_events "
            "WHERE event_id = %s",
            (event_id,),
        ) == (1,)
    finally:
        connector.close()


@pytest.mark.parametrize("failing_delete", ("commit", "seal", "stream"))
def test_publication_commit_effect_root_fault_rolls_back_all_three_rows(
    tmp_path: Path,
    failing_delete: str,
) -> None:
    connector = _database(tmp_path / f"publication-root-fault-{failing_delete}.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        preparation_id = bytes((51,)) * 16
        original = connector.execute_affected

        def fail_delete(sql: str, data: tuple[object, ...] = ()) -> int:
            if failing_delete == "commit" and sql.startswith(
                "DELETE FROM catalog_publication_commits"
            ):
                return 0
            if failing_delete == "seal" and sql.startswith(
                "DELETE FROM operational_operational_preparation_effect_seals"
            ):
                return 0
            if failing_delete == "stream" and sql.startswith(
                "DELETE FROM operational_operational_event_streams"
            ):
                return 0
            return original(sql, data)

        with (
            patch.object(connector, "execute_affected", side_effect=fail_delete),
            pytest.raises(CleanupUnavailableError, match="compound root changed"),
        ):
            _advance(connector, gate, cycle, 1, b"g" * 32, now=90)

        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
            (old_receipt,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_preparation_effect_seals "
            "WHERE preparation_id = %s",
            (preparation_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_event_streams "
            "WHERE preparation_id = %s",
            (preparation_id,),
        ) == (1,)
    finally:
        connector.close()


def test_publication_commit_effect_root_rejects_missing_uncovered_triple(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-root-missing.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        preparation_id = bytes((51,)) * 16
        _fixture_rows(
            connector,
            [
                (
                    "DELETE FROM operational_operational_preparation_effect_seals "
                    "WHERE preparation_id = %s",
                    (preparation_id,),
                )
            ],
        )

        with pytest.raises(
            CleanupCorruptionError,
            match="missing or only partially present",
        ):
            _advance(connector, gate, cycle, 1, b"r" * 32, now=90)

        assert connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
            (old_receipt,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_event_streams "
            "WHERE preparation_id = %s",
            (preparation_id,),
        ) == (1,)
    finally:
        connector.close()


def test_publication_commit_event_cursor_rejects_covered_reappearance(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-event-reappearance.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PCOM_EVENT")
        deleted = _advance(connector, gate, cycle, 1, b"h" * 32, now=80)
        assert deleted.phase == "PCOM_EVENT" and deleted.row_count == 1
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 0, 'REMOVED_GID', %s, 10)",
                    (bytes((61,)) * 16, bytes((51,)) * 16, bytes((131,)) * 32),
                ),
                (
                    "INSERT INTO operational_operational_removed_gid_events "
                    "(event_id, gid, request_token) VALUES (%s, 1, %s)",
                    (bytes((61,)) * 16, bytes((141,)) * 16),
                ),
            ],
        )
        assert deleted.generation is not None
        with pytest.raises(CleanupCorruptionError, match="covered coordinate"):
            _advance(
                connector,
                gate,
                cycle,
                deleted.generation,
                b"i" * 32,
                now=81,
            )
    finally:
        connector.close()


@pytest.mark.parametrize(
    "corruption",
    (
        "output",
        "over_budget",
        "deleted_underflow",
        "stationary_nonterminal",
        "moving_forgery",
    ),
)
def test_publication_commit_event_receipt_corruption_fails_full_check(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _database(tmp_path / f"publication-event-receipt-{corruption}.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PCOM_EVENT")
        event_batch = _advance(connector, gate, cycle, 1, b"s" * 32, now=80)
        assert event_batch.row_count == 1 and event_batch.generation == 2
        connector.execute("PRAGMA ignore_check_constraints = ON")
        if corruption == "output":
            statement = (
                "UPDATE operational_cleanup_checkpoints SET chain_sha256 = %s "
                "WHERE cleanup_id = %s AND phase = 'PCOM_EVENT'"
            )
            parameters: tuple[object, ...] = (b"x" * 32, cycle.cleanup_id)
        elif corruption == "over_budget":
            statement = (
                "UPDATE operational_cleanup_checkpoints SET receipt_row_count = 9 "
                "WHERE cleanup_id = %s AND phase = 'PCOM_EVENT'"
            )
            parameters = (cycle.cleanup_id,)
        elif corruption == "deleted_underflow":
            statement = (
                "UPDATE operational_cleanup_checkpoints SET receipt_row_count = 2 "
                "WHERE cleanup_id = %s AND phase = 'PCOM_EVENT'"
            )
            parameters = (cycle.cleanup_id,)
        elif corruption == "stationary_nonterminal":
            statement = (
                "UPDATE operational_cleanup_checkpoints "
                "SET receipt_start_cursor = cursor_bytes "
                "WHERE cleanup_id = %s AND phase = 'PCOM_EVENT'"
            )
            parameters = (cycle.cleanup_id,)
        else:
            statement = (
                "UPDATE operational_cleanup_checkpoints "
                "SET receipt_start_cursor = %s, receipt_input_sha256 = %s "
                "WHERE cleanup_id = %s AND phase = 'PCOM_EVENT'"
            )
            parameters = (b"forged-but-moving", b"q" * 32, cycle.cleanup_id)
        connector.execute(statement, parameters)

        with pytest.raises(
            operational_refinement_module.OperationalSemanticValidationError,
            match="cleanup receipt",
        ):
            operational_refinement_module._validate_fixed_cleanup_state(
                connector,
                "sqlite",
            )
        with pytest.raises(CleanupCorruptionError, match="cleanup latest receipt"):
            _advance(
                connector,
                gate,
                cycle,
                event_batch.generation,
                b"z" * 32,
                now=81,
            )
    finally:
        connector.close()


def test_publication_commit_open_transition_full_check_accepts_exact_receipt_proof(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-transition-check.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(connector, gate, cycle, "PCOM_EVENT")
        event_batch = _advance(connector, gate, cycle, 1, b"j" * 32, now=80)
        assert event_batch.row_count == 1 and event_batch.generation == 2
        operational_refinement_module._validate_open_pcom_event_transition(
            connector,
            "sqlite",
        )

        event_terminal = _advance(
            connector,
            gate,
            cycle,
            event_batch.generation,
            b"k" * 32,
            now=81,
        )
        assert event_terminal.phase == "PCOM_FINALIZATION_MARKER"
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        root_batch = _advance(connector, gate, cycle, 1, b"l" * 32, now=90)
        assert root_batch.row_count == 1 and root_batch.generation == 2
        operational_refinement_module._validate_open_pcom_event_transition(
            connector,
            "sqlite",
        )

        later = _advance(
            connector,
            gate,
            cycle,
            root_batch.generation,
            b"m" * 32,
            now=91,
        )
        assert later.phase == "PCOM_FINALIZATION_CHECKPOINT"
        operational_refinement_module._validate_open_pcom_event_transition(
            connector,
            "sqlite",
        )
    finally:
        connector.close()


def test_empty_publication_commit_cycle_is_full_check_valid_in_every_phase(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-empty-cycle.sqlite3")
    try:
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            250,
            max_rows=8,
        )
        expected_phases = (
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
        )
        for attempt, expected_phase in enumerate(expected_phases, start=1):
            assert connector.fetch_one(
                "SELECT phase FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            ) == (expected_phase,)
            operational_refinement_module._validate_fixed_cleanup_state(
                connector,
                "sqlite",
            )
            operational_refinement_module._validate_open_pcom_event_transition(
                connector,
                "sqlite",
            )
            result = _advance(
                connector,
                gate,
                cycle,
                1,
                attempt.to_bytes(32, "big"),
                now=80 + attempt,
            )
        assert result.cycle_complete
        operational_refinement_module._validate_fixed_cleanup_state(
            connector,
            "sqlite",
        )
        operational_refinement_module._validate_open_pcom_event_transition(
            connector,
            "sqlite",
        )
    finally:
        connector.close()


def test_publication_commit_multi_root_and_zero_event_retirement(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-multi-root.sqlite3")
    second_old = bytes.fromhex("21" + "23" * 15)
    try:
        gate = _exclusive(connector)
        old_receipt, replacement = _seed_publication_commit_cleanup_history(
            connector,
            additional_old_receipt=second_old,
        )
        _fixture_rows(
            connector,
            [
                (
                    "DELETE FROM operational_operational_removed_gid_events "
                    "WHERE event_id = %s",
                    (bytes((62,)) * 16,),
                ),
                (
                    "DELETE FROM operational_operational_events WHERE event_id = %s",
                    (bytes((62,)) * 16,),
                ),
                (
                    "UPDATE operational_operational_preparation_effect_seals "
                    "SET event_count = 0 WHERE preparation_id = %s",
                    (bytes((52,)) * 16,),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        results = _drain(connector, gate, cycle)
        assert any(
            result.phase == "PCOM_COMMIT_EFFECT_ROOT" and result.row_count == 2
            for result in results
        )
        assert connector.fetch_all(
            "SELECT receipt_id FROM catalog_publication_commits ORDER BY receipt_id"
        ) == [(replacement,)]
    finally:
        connector.close()


def test_publication_commit_compound_proof_rejects_each_reappearing_frozen_pair(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-multi-root-reappearance.sqlite3")
    first_receipt = bytes.fromhex("21" * 16)
    second_receipt = bytes.fromhex("21" + "23" * 15)
    first_preparation = bytes((51,)) * 16
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(
            connector,
            additional_old_receipt=second_receipt,
        )
        assert old_receipt == first_receipt
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        deleted = _advance(connector, gate, cycle, 1, b"t" * 32, now=90)
        assert deleted.row_count == 2 and deleted.generation == 2
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_operational_event_streams "
                    "(preparation_id, created_at) VALUES (%s, 6)",
                    (first_preparation,),
                ),
                (
                    "INSERT INTO operational_operational_preparation_effect_seals "
                    "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                    "VALUES (%s, 1, %s, 11)",
                    (first_preparation, bytes((121,)) * 32),
                ),
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 11)",
                    (first_receipt, bytes((41,)) * 16, first_preparation),
                ),
            ],
        )

        with pytest.raises(
            operational_refinement_module.OperationalSemanticValidationError,
            match="commit reappeared",
        ):
            operational_refinement_module._validate_open_pcom_event_transition(
                connector,
                "sqlite",
            )
        with pytest.raises(CleanupCorruptionError, match="commit reappeared"):
            _advance(
                connector,
                gate,
                cycle,
                deleted.generation,
                b"u" * 32,
                now=91,
            )
    finally:
        connector.close()


def test_publication_commit_compound_proof_rejects_recreated_preparation_chain(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-recreated-preparation.sqlite3")
    preparation_id = bytes((51,)) * 16
    event_id = bytes((61,)) * 16
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        deleted = _advance(connector, gate, cycle, 1, b"w" * 32, now=90)
        assert deleted.row_count == 1 and deleted.generation == 2
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_operational_event_streams "
                    "(preparation_id, created_at) VALUES (%s, 6)",
                    (preparation_id,),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 7, 8)",
                    (preparation_id, bytes((71,)) * 16),
                ),
                (
                    "INSERT INTO operational_operational_preparation_effect_seals "
                    "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                    "VALUES (%s, 1, %s, 11)",
                    (preparation_id, bytes((121,)) * 32),
                ),
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 0, 'REMOVED_GID', %s, 11)",
                    (event_id, preparation_id, bytes((131,)) * 32),
                ),
                (
                    "INSERT INTO operational_operational_removed_gid_events "
                    "(event_id, gid, request_token) VALUES (%s, 1, %s)",
                    (event_id, bytes((141,)) * 16),
                ),
            ],
        )

        with pytest.raises(
            operational_refinement_module.OperationalSemanticValidationError,
            match="compound-covered authority reappeared",
        ):
            operational_refinement_module._validate_open_pcom_event_transition(
                connector,
                "sqlite",
            )
        with pytest.raises(CleanupCorruptionError, match="authority reappeared"):
            _advance(
                connector,
                gate,
                cycle,
                deleted.generation,
                b"x" * 32,
                now=91,
            )
    finally:
        connector.close()


@pytest.mark.parametrize(
    "reappearing_family",
    (
        "source_base",
        "preparation_binding",
        "preparation_batch",
        "preparation_checkpoint",
        "preparation",
        "event",
        "seal",
        "stream",
        "finalization_marker",
        "finalization_batch",
        "finalization_checkpoint",
        "anchor",
    ),
)
def test_publication_commit_post_compound_phase_rejects_every_reappearing_family(
    tmp_path: Path,
    reappearing_family: str,
) -> None:
    connector = _database(
        tmp_path / f"publication-post-compound-{reappearing_family}.sqlite3"
    )
    receipt_id = bytes.fromhex("21" * 16)
    candidate_id = bytes((41,)) * 16
    preparation_id = bytes((51,)) * 16
    event_id = bytes((61,)) * 16
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        assert old_receipt == receipt_id
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            receipt_id[0],
            max_rows=8,
        )
        _advance_to_cleanup_phase(
            connector,
            gate,
            cycle,
            "PCOM_COMMIT_EFFECT_ROOT",
        )
        compound = _advance(connector, gate, cycle, 1, b"v" * 32, now=90)
        assert compound.row_count == 1 and compound.generation == 2
        checkpoint = _advance(
            connector,
            gate,
            cycle,
            compound.generation,
            b"w" * 32,
            now=91,
        )
        assert checkpoint.phase == "PCOM_FINALIZATION_CHECKPOINT"
        assert checkpoint.generation == 1

        if reappearing_family == "finalization_checkpoint":
            checkpoint = _advance(
                connector,
                gate,
                cycle,
                checkpoint.generation,
                b"x" * 32,
                now=92,
            )
            assert checkpoint.phase == "PCOM_FINALIZATION_CHECKPOINT"
            assert checkpoint.row_count == 1 and checkpoint.generation == 2
        elif reappearing_family == "anchor":
            assert checkpoint.generation is not None
            deleted_checkpoint = _advance(
                connector,
                gate,
                cycle,
                checkpoint.generation,
                b"x" * 32,
                now=92,
            )
            assert deleted_checkpoint.generation == 2
            assert deleted_checkpoint.generation is not None
            checkpoint = _advance(
                connector,
                gate,
                cycle,
                deleted_checkpoint.generation,
                b"y" * 32,
                now=93,
            )
            assert checkpoint.phase == "PCOM_ANCHOR" and checkpoint.generation == 1
            checkpoint = _advance(
                connector,
                gate,
                cycle,
                checkpoint.generation,
                b"z" * 32,
                now=94,
            )
            assert checkpoint.phase == "PCOM_ANCHOR"
            assert checkpoint.row_count == 1 and checkpoint.generation == 2

        statements_by_family: dict[str, list[tuple[str, tuple[object, ...]]]] = {
            "source_base": [
                (
                    "INSERT INTO catalog_source_build_base_publication_commits "
                    "(build_id, base_receipt_id) VALUES (%s, %s)",
                    (bytes((71,)) * 16, receipt_id),
                )
            ],
            "preparation_binding": [
                (
                    "INSERT INTO operational_publication_candidate_preparations "
                    "(candidate_id, preparation_id, bound_at) VALUES (%s, %s, 11)",
                    (candidate_id, preparation_id),
                )
            ],
            "preparation_batch": [
                (
                    "INSERT INTO operational_operational_preparation_batch_receipts "
                    "(preparation_id, phase, batch_key, start_cursor, next_cursor, "
                    "input_sha256, output_sha256, row_count, committed_generation, "
                    "committed_at) VALUES (%s, 'REMOVED_GID', %s, %s, %s, %s, "
                    "%s, 0, 2, 11)",
                    (
                        preparation_id,
                        bytes((91,)) * 32,
                        b"",
                        b"",
                        bytes((101,)) * 32,
                        bytes((111,)) * 32,
                    ),
                )
            ],
            "preparation_checkpoint": [
                (
                    "INSERT INTO operational_operational_preparation_checkpoints "
                    "(preparation_id, phase, generation, cursor_bytes, "
                    "processed_count, chain_sha256, state, updated_at) "
                    "VALUES (%s, 'REMOVED_GID', 2, %s, 1, %s, 'COMPLETE', 11)",
                    (preparation_id, b"", bytes((81,)) * 32),
                )
            ],
            "preparation": [
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 7, 8)",
                    (preparation_id, bytes((71,)) * 16),
                )
            ],
            "event": [
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 0, 'REMOVED_GID', %s, 11)",
                    (event_id, preparation_id, bytes((131,)) * 32),
                ),
                (
                    "INSERT INTO operational_operational_removed_gid_events "
                    "(event_id, gid, request_token) VALUES (%s, 1, %s)",
                    (event_id, bytes((141,)) * 16),
                ),
            ],
            "seal": [
                (
                    "INSERT INTO operational_operational_preparation_effect_seals "
                    "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                    "VALUES (%s, 1, %s, 11)",
                    (preparation_id, bytes((121,)) * 32),
                )
            ],
            "stream": [
                (
                    "INSERT INTO operational_operational_event_streams "
                    "(preparation_id, created_at) VALUES (%s, 6)",
                    (preparation_id,),
                )
            ],
            "finalization_marker": [
                (
                    "INSERT INTO catalog_publication_commit_finalizations "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                )
            ],
            "finalization_batch": [
                (
                    "INSERT INTO catalog_publication_finalization_batch_stored "
                    "(receipt_id, start_generation, batch_key, start_cursor, "
                    "start_processed_count, next_cursor, row_count, committed_at) "
                    "VALUES (%s, 1, %s, %s, 0, %s, 0, 11)",
                    (receipt_id, b"\x01", b"", b""),
                )
            ],
            "finalization_checkpoint": [
                (
                    "INSERT INTO catalog_publication_finalization_checkpoints "
                    "(receipt_id, generation, `cursor`, processed_count, state, "
                    "updated_at) VALUES (%s, 2, %s, 0, 'COMPLETE', 11)",
                    (receipt_id, b""),
                )
            ],
            "anchor": [
                (
                    "INSERT INTO catalog_publication_commit_anchors "
                    "(receipt_id) VALUES (%s)",
                    (receipt_id,),
                )
            ],
        }
        _fixture_rows(connector, statements_by_family[reappearing_family])
        assert checkpoint.generation is not None
        before = connector.fetch_one(
            "SELECT phase, generation, cursor_bytes, deleted_count, state "
            "FROM operational_cleanup_checkpoints "
            "WHERE cleanup_id = %s AND state = 'OPEN'",
            (cycle.cleanup_id,),
        )
        with pytest.raises(CleanupCorruptionError, match="PCOM"):
            _advance(
                connector,
                gate,
                cycle,
                checkpoint.generation,
                b"q" * 32,
                now=95,
            )
        assert (
            connector.fetch_one(
                "SELECT phase, generation, cursor_bytes, deleted_count, state "
                "FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            == before
        )
    finally:
        connector.close()


def test_publication_commit_cleanup_waits_for_finalized_replacement(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-commit-finalization.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector,
            finalize_replacement=False,
        )
        blocked = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        assert _drain(connector, gate, blocked)[-1].deleted_count == 0

        connector.execute(
            "INSERT INTO catalog_publication_commit_finalizations "
            "(receipt_id) VALUES (%s)",
            (replacement_receipt,),
        )
        actionable = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
            now=20,
        )
        assert _drain(connector, gate, actionable, now=21)[-1].deleted_count == 10
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("blocker_insert", "blocker_delete"),
    [
        (
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            "DELETE FROM catalog_source_build_base_publication_commits",
        ),
        (
            "INSERT INTO catalog_publication_candidate_base_publication_commits "
            "(candidate_id, base_receipt_id) VALUES (%s, %s)",
            "DELETE FROM catalog_publication_candidate_base_publication_commits",
        ),
        (
            "INSERT INTO operational_gallery_redownload_states "
            "(gallery_id, redownload_at, through_source_revision, updated_at) "
            "VALUES (1, 1, %s, 1)",
            "DELETE FROM operational_gallery_redownload_states",
        ),
    ],
)
def test_publication_commit_cleanup_honors_every_exact_dynamic_pin(
    tmp_path: Path,
    blocker_insert: str,
    blocker_delete: str,
) -> None:
    connector = _database(tmp_path / "publication-commit-blocker.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, _replacement = _seed_publication_commit_cleanup_history(connector)
        if "source_build_base" in blocker_insert:
            parameters: tuple[object, ...] = (b"b" * 16, old_receipt)
        elif "candidate_base" in blocker_insert:
            parameters = (b"c" * 16, old_receipt)
        elif "redownload" in blocker_insert:
            parameters = (1,)
        else:
            raise AssertionError("unexpected publication-commit blocker fixture")
        _fixture_rows(connector, [(blocker_insert, parameters)])

        blocked = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        assert _drain(connector, gate, blocked)[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_publication_commits WHERE receipt_id = %s",
            (old_receipt,),
        ) == (1,)

        connector.execute(blocker_delete)
        actionable = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
            now=30,
        )
        assert _drain(connector, gate, actionable, now=31)[-1].deleted_count == 10
    finally:
        connector.close()


@pytest.mark.parametrize(
    (
        "build_state",
        "analysis_states",
        "published_analysis_index",
        "working",
        "handoff_is_current",
    ),
    [
        pytest.param("OPEN", ("COMPLETE",), 0, False, True, id="open-build"),
        pytest.param("SEALED", ("COMPLETE",), 0, True, True, id="active-retry"),
        pytest.param(None, ("COMPLETE",), 0, False, True, id="missing-build-state"),
        pytest.param("SEALED", (None,), None, False, True, id="missing-analysis-state"),
        pytest.param("SEALED", ("OPEN",), None, False, True, id="open-analysis"),
        pytest.param(
            "SEALED",
            ("COMPLETE",),
            None,
            False,
            True,
            id="unpublished-analysis",
        ),
        pytest.param(
            "SEALED",
            ("COMPLETE",),
            0,
            False,
            False,
            id="noncurrent-handoff",
        ),
        pytest.param(
            "SEALED",
            ("ABANDONED",),
            None,
            False,
            True,
            id="no-durable-handoff",
        ),
    ],
)
def test_publication_commit_build_base_release_fails_closed_without_safe_handoff(
    tmp_path: Path,
    build_state: str | None,
    analysis_states: tuple[str | None, ...],
    published_analysis_index: int | None,
    working: bool,
    handoff_is_current: bool,
) -> None:
    connector = _database(tmp_path / "publication-commit-unsafe-build-base.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector
        )
        build_id, _analysis_ids = _seed_publication_commit_source_build_base(
            connector,
            base_receipt=old_receipt,
            handoff_receipt=(
                replacement_receipt if handoff_is_current else old_receipt
            ),
            build_state=build_state,
            analysis_states=analysis_states,
            published_analysis_index=published_analysis_index,
            working=working,
        )

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=8,
        )
        assert _drain(connector, gate, cycle)[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (build_id,),
        ) == (old_receipt,)
        assert connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits WHERE receipt_id = %s",
            (old_receipt,),
        ) == (old_receipt,)
    finally:
        connector.close()


def test_publication_commit_build_base_release_rolls_back_resumes_and_replays(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-commit-base-fault.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector
        )
        build_id, _analysis_ids = _seed_publication_commit_source_build_base(
            connector,
            base_receipt=old_receipt,
            handoff_receipt=replacement_receipt,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=1,
        )
        original_execute_affected = connector.execute_affected

        def fail_after_base_delete(
            sql: str, parameters: tuple[object, ...] = ()
        ) -> int:
            affected = original_execute_affected(sql, parameters)
            if sql.startswith(
                "DELETE FROM catalog_source_build_base_publication_commits"
            ):
                assert affected == 1
                raise RuntimeError("injected build-base release fault")
            return affected

        with (
            patch.object(
                connector,
                "execute_affected",
                side_effect=fail_after_base_delete,
            ),
            pytest.raises(RuntimeError, match="build-base release fault"),
        ):
            _advance(connector, gate, cycle, 1, b"f" * 32, now=3)
        assert connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (build_id,),
        ) == (old_receipt,)

        resumed = _advance(connector, gate, cycle, 1, b"r" * 32, now=4)
        assert resumed.phase == "PCOM_RELEASE_BUILD_BASE"
        assert resumed.row_count == 1
        replayed = _advance(connector, gate, cycle, 1, b"r" * 32, now=5)
        assert replayed.replayed
        assert replayed.row_count == resumed.row_count
        assert replayed.cursor == resumed.cursor
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
        assert resumed.generation is not None
        generation = resumed.generation
        for attempt in range(32):
            completed = _advance(
                connector,
                gate,
                cycle,
                generation,
                (attempt + 100).to_bytes(32, "big"),
                now=6 + attempt,
            )
            if completed.cycle_complete:
                break
            assert completed.generation is not None
            generation = completed.generation
        else:
            raise AssertionError("resumed publication-commit cleanup did not finish")
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
                (old_receipt,),
            )
            == ()
        )
    finally:
        connector.close()


def test_publication_commit_dynamic_pin_blocks_terminal_and_same_phase_resumes(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-commit-dynamic-pin.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector
        )
        build_id, _analysis_ids = _seed_publication_commit_source_build_base(
            connector,
            base_receipt=old_receipt,
            handoff_receipt=replacement_receipt,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=1,
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_gallery_redownload_states "
                    "(gallery_id, redownload_at, through_source_revision, updated_at) "
                    "VALUES (1, 1, 1, 1)",
                    (),
                )
            ],
        )

        before = connector.fetch_one(
            "SELECT phase, generation, cursor_bytes, deleted_count, state "
            "FROM operational_cleanup_checkpoints "
            "WHERE cleanup_id = %s AND state = 'OPEN'",
            (cycle.cleanup_id,),
        )
        assert before == ("PCOM_RELEASE_BUILD_BASE", 1, b"", 0, "OPEN")
        with pytest.raises(CleanupRetentionBlockedError, match="still owns rows"):
            _advance(connector, gate, cycle, 1, b"p" * 32, now=3)
        assert (
            connector.fetch_one(
                "SELECT phase, generation, cursor_bytes, deleted_count, state "
                "FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            == before
        )
        assert connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (build_id,),
        ) == (old_receipt,)

        connector.execute("DELETE FROM operational_gallery_redownload_states")
        resumed = _advance(connector, gate, cycle, 1, b"r" * 32, now=4)
        assert resumed.phase == "PCOM_RELEASE_BUILD_BASE"
        assert resumed.row_count == 1 and resumed.generation == 2
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
    finally:
        connector.close()


def test_publication_commit_build_base_release_exactly_rechecks_active_retry(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "publication-commit-base-race.sqlite3")
    try:
        gate = _exclusive(connector)
        old_receipt, replacement_receipt = _seed_publication_commit_cleanup_history(
            connector
        )
        build_id, _analysis_ids = _seed_publication_commit_source_build_base(
            connector,
            base_receipt=old_receipt,
            handoff_receipt=replacement_receipt,
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_COMMIT,
            old_receipt[0],
            max_rows=1,
        )
        original_fetch_all = connector.fetch_all
        injected = False

        def inject_active_retry(
            sql: str, parameters: tuple[object, ...] = ()
        ) -> list[tuple[Any, ...]]:
            nonlocal injected
            rows = original_fetch_all(sql, parameters)
            if (
                not injected
                and "FROM catalog_source_build_base_publication_commits AS c" in sql
                and "ORDER BY r.receipt_id, c.build_id" in sql
                and rows
            ):
                injected = True
                connector.execute(
                    "INSERT INTO operational_source_working_builds "
                    "(slot, build_id, assigned_at) VALUES (1, %s, 1)",
                    (build_id,),
                )
            return rows

        with (
            patch.object(connector, "fetch_all", side_effect=inject_active_retry),
            pytest.raises(CleanupRetentionBlockedError, match="retention root"),
        ):
            _advance(connector, gate, cycle, 1, b"x" * 32, now=3)
        assert injected
        assert connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (build_id,),
        ) == (old_receipt,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_source_working_builds WHERE build_id = %s",
                (build_id,),
            )
            == ()
        )
    finally:
        connector.close()


def test_state_parent_cleanup_uses_contiguous_generation_prefixes(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "state-parent-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_revision_descriptors "
                    "(revision, publication_count, artifact_count) "
                    "VALUES (%s, 0, 0)",
                    (revision,),
                )
                for revision in (10, 11)
            ]
            + [
                (
                    "INSERT INTO catalog_source_revision_descriptors "
                    "(source_revision, channel, snapshot_manifest_sha256) "
                    "VALUES (%s, %s, %s)",
                    (revision, b"default", bytes((revision,)) * 32),
                )
                for revision in (10, 11)
            ]
            + [
                (
                    "INSERT INTO catalog_publication_generation_nodes "
                    "(generation) VALUES (%s)",
                    (generation,),
                )
                for generation in (1, 2, 3, 4, 5)
            ]
            + [
                (
                    "INSERT INTO catalog_publication_generation_successors "
                    "(successor_generation, predecessor_generation) VALUES (%s, %s)",
                    edge,
                )
                for edge in ((1, 0), (2, 1), (3, 2), (4, 3), (5, 4))
            ]
            + [
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 11, 11, 5, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 1)",
                    (b"r" * 16, b"c" * 16, b"p" * 16),
                ),
            ],
        )

        catalog_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR,
            10,
            max_rows=1,
        )
        assert _drain(connector, gate, catalog_cycle)[-1].deleted_count == 1
        source_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR,
            10,
            max_rows=1,
            now=20,
        )
        assert _drain(connector, gate, source_cycle, now=21)[-1].deleted_count == 1
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_revision_descriptors WHERE revision = 11"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_source_revision_descriptors "
            "WHERE source_revision = 11"
        ) == (1,)

        generation_results: list[CleanupBatchResult] = []
        generation_cycle_index = 0
        while connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_publication_generation_nodes "
            "WHERE generation < 5"
        ) != (0,):
            oldest = connector.fetch_one(
                "SELECT MIN(generation) FROM catalog_publication_generation_nodes"
            )
            assert len(oldest) == 1 and isinstance(oldest[0], int)
            generation_cycle = _begin(
                connector,
                gate,
                CleanupTargetKind.PUBLICATION_GENERATION,
                oldest[0] % 256,
                max_rows=2,
                now=40 + generation_cycle_index * 20,
            )
            generation_results.extend(
                _drain(
                    connector,
                    gate,
                    generation_cycle,
                    now=41 + generation_cycle_index * 20,
                )
            )
            generation_cycle_index += 1
            assert generation_cycle_index <= 3
        assert all(result.row_count <= 2 for result in generation_results)
        assert generation_cycle_index == 3
        assert connector.fetch_all(
            "SELECT generation FROM catalog_publication_generation_nodes "
            "ORDER BY generation"
        ) == [(5,)]
        assert (
            connector.fetch_all(
                "SELECT successor_generation, predecessor_generation "
                "FROM catalog_publication_generation_successors"
            )
            == []
        )

        retained_floor = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_GENERATION,
            44,
            max_rows=8,
            now=100,
        )
        assert _drain(connector, gate, retained_floor, now=101)[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_publication_generation_nodes "
            "WHERE generation = 5"
        ) == (1,)
    finally:
        connector.close()


def test_publication_generation_keeps_genesis_until_generation_one_is_compacted(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "generation-one-floor.sqlite3")
    try:
        gate = _exclusive(connector)
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_generation_nodes "
                    "(generation) VALUES (%s)",
                    (generation,),
                )
                for generation in (1,)
            ]
            + [
                (
                    "INSERT INTO catalog_publication_generation_successors "
                    "(successor_generation, predecessor_generation) VALUES (1, 0)",
                    (),
                ),
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 1)",
                    (b"r" * 16, b"c" * 16, b"p" * 16),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_GENERATION,
            0,
            max_rows=8,
        )
        assert _drain(connector, gate, cycle)[-1].deleted_count == 0
        assert connector.fetch_all(
            "SELECT generation FROM catalog_publication_generation_nodes "
            "ORDER BY generation"
        ) == [(0,), (1,)]
        assert connector.fetch_all(
            "SELECT successor_generation, predecessor_generation "
            "FROM catalog_publication_generation_successors"
        ) == [(1, 0)]
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
        assert sum(result.row_count for result in results) == sum(
            _CATALOG_PUBLICATION_PAYLOAD_COUNTS.values()
        )
        for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES:
            if table in {
                "catalog_publication_storage",
                "catalog_publication_download_times",
            }:
                selector = (
                    f"SELECT COUNT(*) FROM {table} AS child "
                    "JOIN catalog_publication_occurrence_identities AS occurrence "
                    "ON occurrence.catalog_occurrence_sha256 = "
                    "child.catalog_occurrence_sha256 WHERE occurrence.revision = %s"
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
            ) == (_CATALOG_PUBLICATION_PAYLOAD_COUNTS[table],)
        assert connector.fetch_one(
            "SELECT publication_count FROM catalog_revision_descriptors "
            "WHERE revision = 1"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT revision FROM catalog_publication_commits WHERE receipt_id = %s",
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
            "DELETE FROM catalog_publication_finalization_batch_stored "
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
            "SELECT candidate_id FROM catalog_publication_candidates "
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
        ) == ("PUBLISHED", 100)
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
    assert tuple(spec.table for spec in source["SB_GALLERY"]) == (
        "catalog_source_build_sealed_ats",
        "operational_source_build_discovery_checkpoints",
        "operational_source_build_assembly_checkpoints",
        "catalog_build_manifest_core",
        "catalog_source_build_galleries",
    )
    assert tuple(spec.table for spec in source["SB_DISCOVERY"]) == (
        "catalog_source_build_discoveries",
    )
    assert tuple(spec.table for spec in source["SB_STATE"]) == (
        "catalog_source_build_states",
    )
    assert tuple(spec.table for spec in source["SB_ROOT"]) == (
        "catalog_source_build_descriptor",
    )

    analysis = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases
    assert tuple(spec.table for spec in analysis["AR_BATCH"]) == (
        "catalog_analysis_batch_receipt_stored",
    )
    assert tuple(spec.table for spec in analysis["AR_COMPONENT"]) == (
        "catalog_analysis_state_component_seals",
    )
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
        "catalog_search_lexemes",
        "catalog_display_title_choices",
        "catalog_title_sorts",
        "catalog_source_scopes",
        "catalog_source_locator_identity",
        "catalog_tag_terms",
        "catalog_source_snapshot_manifest_identity",
        "catalog_artifact_policies",
        "catalog_artifact_semantic_inputs",
    )
    source_scope = canonical["CV_DICTIONARY"][3]
    assert source_scope.primary_key == ("scope_key",)
    assert "catalog_source_scopes AS c" in source_scope.source
    assert source_scope.delete_sql == (
        "DELETE FROM catalog_source_scopes WHERE scope_key = %s",
    )
    tag_term = canonical["CV_DICTIONARY"][5]
    assert tag_term.delete_sql == ("DELETE FROM catalog_tag_terms WHERE tag_id = %s",)
    assert tuple(spec.table for spec in canonical["CV_SEMANTIC_LINK"]) == (
        "catalog_artifact_policy_semantics",
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
                    "INSERT INTO catalog_source_build_discoveries "
                    "(build_id, scan_attempt, gallery_count, "
                    "tree_observation_sha256, completed_at) "
                    "VALUES (%s, %s, 1, %s, 1)",
                    (build_id, b"d" * 16, b"t" * 32),
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
        assert source_results[-1].deleted_count == 7
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
        assert _source_build_discovery_rows(connector) == ([],)

        analysis_id = bytes((18,)) + b"a" * 15
        seed_analysis_policy(connector)
        _seed_cleanup_sealed_source_build(
            connector,
            build_id=b"z" * 16,
            scope_key=scope_key,
            manifest_policy_id=1,
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
                    "INSERT INTO catalog_analysis_checkpoints "
                    "(analysis_id, stage, generation, `cursor`, processed_count, "
                    "state, updated_at) VALUES (%s, %s, 1, %s, 0, 'OPEN', 0)",
                    (analysis_id, b"changed_gallery", b""),
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
        assert analysis_results[-1].deleted_count == 8
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_analysis_runs WHERE analysis_id = %s",
                (analysis_id,),
            )
            == ()
        )
        for table in (
            "catalog_analysis_run_completed_ats",
            "catalog_analysis_run_states",
            "catalog_analysis_run_descriptor",
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
        assert candidate_results[-1].deleted_count == 5
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
            if statement.startswith(
                (
                    "DELETE FROM catalog_prepared_storage_objects",
                    "DELETE FROM catalog_prepared_resource_blob",
                    "DELETE FROM catalog_prepared_artifacts",
                )
            ):
                family_deletes.append(statement.split()[2])
            return original_execute_affected(sql, data)

        with patch.object(connector, "execute_affected", side_effect=record_delete):
            results = _drain(connector, gate, cycle)
        assert family_deletes == [
            "catalog_prepared_storage_objects",
            "catalog_prepared_resource_blob",
            "catalog_prepared_artifacts",
        ]
        assert results[-1].deleted_count == 4
        assert _prepared_artifact_family_rows(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
        ) == ([], [], [])
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
        _seed_cleanup_candidate(
            connector,
            candidate_id=candidate_id,
            reserved_revision=revision,
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
                    "INSERT INTO catalog_publication_download_times "
                    "(catalog_occurrence_sha256, download_time) VALUES (%s, %s)",
                    (occurrence, revision),
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
                    "INSERT INTO catalog_contributors "
                    "(revision, publication_key, contributor_name_sha256, "
                    "role, position) VALUES (%s, %s, %s, %s, 0)",
                    (revision, publication_key, b"n" * 32, b"artist"),
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
                    "artifact_semantics_sha256, artifact_name, media_type, "
                    "page_count) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                    (
                        revision,
                        publication_key,
                        b"a" * 32,
                        b"m" * 32,
                        b"artifact.bin",
                        b"application/octet-stream",
                    ),
                ),
                (
                    "INSERT INTO catalog_search_documents "
                    "(revision, publication_key, row_count) VALUES (%s, %s, 1)",
                    (revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_search_postings "
                    "(revision, value_sha256, publication_key) VALUES (%s, %s, %s)",
                    (revision, b"z" * 32, publication_key),
                ),
                (
                    "INSERT INTO catalog_title_search_postings "
                    "(revision, value_sha256, publication_key) VALUES (%s, %s, %s)",
                    (revision, b"z" * 32, publication_key),
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
        assert results[-1].deleted_count == 1
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


def test_candidate_wide_root_cleanup_rolls_back_and_retries_atomically(
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
            table = statement.split()[2] if statement.startswith("DELETE FROM ") else ""
            if table in _CANDIDATE_DEFINITION_DELETE_ORDER:
                deleted.append(table)
            return original_execute_affected(sql, data)

        with patch.object(connector, "execute_affected", side_effect=record_delete):
            result = _advance(connector, gate, cycle, 1, b"g" * 32, now=101)
        assert result.phase == "PC_ROOT" and result.row_count == 1
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
            "SELECT build_id FROM catalog_source_build_descriptor WHERE build_id = %s",
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
        _seed_cleanup_sealed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            created_at=1,
            sealed_at=2,
            manifest_sha256=b"m" * 32,
            gallery_count=0,
            file_count=0,
            byte_count=0,
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
            "catalog_source_build_discoveries",
            "catalog_source_build_sealed_ats",
            "catalog_build_manifest_core",
            "catalog_source_build_states",
            "catalog_source_build_descriptor",
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
                "catalog_source_snapshot_manifest_identity",
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


@pytest.mark.parametrize(
    "facet_family",
    ("language", "contributor", "subject"),
)
def test_canonical_cleanup_retains_values_referenced_by_revision_facets(
    tmp_path: Path,
    facet_family: str,
) -> None:
    connector = _database(tmp_path / f"retained-{facet_family}-facet.sqlite3")
    try:
        value_sha256 = b"y" + facet_family.encode().ljust(31, b"-")
        digest_domain = {
            "language": b"catalog_language_utf8_v1",
            "contributor": b"contributor_name_utf8_v1",
            "subject": b"tag_value_utf8_v1",
        }[facet_family]
        _seed_minimal_canonical_value(
            connector,
            value_sha256=value_sha256,
            page_sha256=b"p" + facet_family.encode().ljust(31, b"-"),
            digest_domain=digest_domain,
        )

        facet_insert: tuple[str, tuple[object, ...]]
        facet_delete: tuple[str, tuple[object, ...]]
        if facet_family == "language":
            facet_insert = (
                "INSERT INTO catalog_language_facet_order "
                "(revision, position, language_sha256, occurrence_count) "
                "VALUES (99, 0, %s, 1)",
                (value_sha256,),
            )
            facet_delete = (
                "DELETE FROM catalog_language_facet_order WHERE revision = 99",
                (),
            )
        elif facet_family == "contributor":
            facet_insert = (
                "INSERT INTO catalog_contributor_facet_order "
                "(revision, position, contributor_name_sha256, role, "
                "occurrence_count) VALUES (99, 0, %s, %s, 1)",
                (value_sha256, b"author"),
            )
            facet_delete = (
                "DELETE FROM catalog_contributor_facet_order WHERE revision = 99",
                (),
            )
        else:
            seed_tag_term(
                connector,
                tag_id=99,
                namespace=b"genre",
                tag_value_sha256=value_sha256,
            )
            facet_insert = (
                "INSERT INTO catalog_subject_facet_order "
                "(revision, position, tag_id, occurrence_count) "
                "VALUES (99, 0, 99, 1)",
                (),
            )
            facet_delete = (
                "DELETE FROM catalog_subject_facet_order WHERE revision = 99",
                (),
            )

        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (99, 1, 0)"
        )
        connector.execute(*facet_insert)
        gate = _exclusive(connector)
        retained_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            value_sha256[0],
            max_rows=32,
        )
        retained_results = _drain(connector, gate, retained_cycle)
        assert retained_results[-1].cycle_complete
        assert retained_results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
            "WHERE value_sha256 = %s",
            (value_sha256,),
        ) == (1,)

        connector.execute(*facet_delete)
        released_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            value_sha256[0],
            max_rows=32,
            now=1_000,
        )
        released_results = _drain(connector, gate, released_cycle, now=1_001)
        assert released_results[-1].cycle_complete
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
                "WHERE value_sha256 = %s",
                (value_sha256,),
            )
            == ()
        )
        if facet_family == "subject":
            assert (
                connector.fetch_one("SELECT 1 FROM catalog_tag_terms WHERE tag_id = 99")
                == ()
            )
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        connector.close()


def test_operational_preparation_cleanup_preserves_every_complete_preparation(
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
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 1)",
                    (b"r" * 16, b"c" * 16, activated),
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
        ) == [(activated,), (unactivated,)]
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
                    "created_at, sealed_at, terminal_byte_count) "
                    "VALUES (%s, %s, 21, 1, 'SEALED', 0, 1, 0)",
                    (staging_id, build_id),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 21, 1)",
                    (build_id,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
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
        _seed_terminal_retirement_authority(
            connector,
            staging_id=staging_id,
            build_id=build_id,
            gallery_id=21,
            provisional_observation_id=1,
            final_observation_id=1,
            file_count=0,
            byte_count=0,
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
                    "created_at, sealed_at, terminal_byte_count) "
                    "VALUES (%s, %s, 24, 2, 'REUSED', 0, 1, 7)",
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
                    "INSERT INTO catalog_gallery_observation_file_artifact_role "
                    "(gallery_id, observation_id, file_key, artifact_role) "
                    "VALUES (24, 2, %s, %s)",
                    (b"n" * 32, b"page"),
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
        _seed_terminal_retirement_authority(
            connector,
            staging_id=reused_staging,
            build_id=reused_build,
            gallery_id=24,
            provisional_observation_id=2,
            final_observation_id=1,
            file_count=1,
            byte_count=7,
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
        vertical_rows = _observation_vertical_rows(
            connector,
            gallery_id=24,
            observation_id=2,
        )
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
                    "created_at, sealed_at, terminal_byte_count) "
                    "VALUES (%s, %s, 22, 1, 'SEALED', 0, 1, 0)",
                    (selected, build),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at, terminal_byte_count) "
                    "VALUES (%s, %s, 23, 1, 'OPEN', 0, NULL, NULL)",
                    (foreign, b"e" * 16),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 22, 1)",
                    (build,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (prior_request, selected),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
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
            _canonical_page_component_rows(connector, b"R" * 32),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            _source_scope_family_rows(connector),
        )
        assert "FROM catalog_source_scopes scope_root" in (
            cleanup_module._CANONICAL_VALUE_ELIGIBILITY
        )
        assert "JOIN catalog_source_build_descriptor build" in (
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
            _canonical_page_component_rows(connector, b"R" * 32),
            connector.fetch_all(
                "SELECT * FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            ),
            _source_scope_family_rows(connector),
        ) == before

        for table in (
            "catalog_source_build_states",
            "catalog_source_build_descriptor",
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
        assert _source_scope_family_rows(connector) == ([],)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (source_root,),
            )
            == ()
        )
        assert all(
            not rows for rows in _canonical_page_component_rows(connector, b"R" * 32)
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (source_root,),
            )
            == ()
        )
        scope_delete_tables = ("catalog_source_scopes",)
        observed_scope_deletes = tuple(
            table
            for statement in traced
            for table in scope_delete_tables
            if statement.startswith(f"DELETE FROM {table} ")
        )
        assert observed_scope_deletes == scope_delete_tables * 2
    finally:
        connector.close()


def test_canonical_cleanup_removes_wide_policy_semantics_atomically(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-wide-policy.sqlite3")
    try:
        policy = identity.artifact_policy_digest(
            2,
            _ARTIFACT_ADAPTER_ID,
            _ARTIFACT_POLICY_FINGERPRINT,
        )
        _seed_minimal_canonical_value(
            connector,
            value_sha256=policy,
            page_sha256=b"P" * 32,
            digest_domain=b"artifact_policy_v3",
        )
        seed_artifact_policy_semantics(
            connector,
            artifact_algorithm_version=2,
            adapter_id=_ARTIFACT_ADAPTER_ID,
            policy_fingerprint_sha256=_ARTIFACT_POLICY_FINGERPRINT,
        )
        assert tuple(
            bool(rows) for rows in _artifact_policy_semantics_family_rows(connector)
        ) == (True,)

        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            int(policy[0]),
            max_rows=32,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].cycle_complete
        assert _artifact_policy_semantics_family_rows(connector) == ([],)
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
        all_results: list[CleanupBatchResult] = []
        cycle_index = 0
        while connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_canonical_value_allocation_anchors "
            "WHERE value_sha256 IN (%s, %s, %s)",
            (source_title, display_title, sort_title),
        ) != (0,):
            cycle = _begin(
                connector,
                gate,
                CleanupTargetKind.CANONICAL_VALUE,
                74,
                max_rows=max_rows,
                now=2 + cycle_index * 200,
            )
            if cycle_index == 0:
                batch_key = b"r" * 32
                first = _advance(connector, gate, cycle, 1, batch_key, now=3)
                replay = _advance(connector, gate, cycle, 1, batch_key, now=4)
                assert replay.replayed
                assert replay.row_count == first.row_count
                assert replay.cursor == first.cursor
                assert replay.generation == first.generation
                cycle_results = [first]
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
                    cycle_results.append(result)
                assert result.cycle_complete
            else:
                cycle_results = _drain(
                    connector,
                    gate,
                    cycle,
                    now=3 + cycle_index * 200,
                )
            all_results.extend(cycle_results)
            cycle_index += 1
            assert cycle_index <= 3
        assert all(result.row_count <= max_rows for result in all_results)
        assert cycle_index == (3 if max_rows == 1 else 1)
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
        ].phases["CV_DICTIONARY"][3]
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
        assert _source_scope_family_rows(connector) == ([],)
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


def test_static_terminal_rejects_earlier_spec_reappearance_after_later_cursor(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-overlay-earlier-reappearance.sqlite3")
    file_sha256 = b"f" * 32
    try:
        analysis_id = _seed_abandoned_analysis_for_cleanup(
            connector,
            discriminator=95,
        )
        _seed_analysis_overlay_rows(connector, analysis_id)
        gate = _exclusive(connector)
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.ANALYSIS_RUN,
            analysis_id[0],
            max_rows=1,
        )
        _position_analysis_cleanup_at_overlay(
            connector,
            gate,
            cycle,
            now=10,
        )

        first = _advance(connector, gate, cycle, 1, b"1" * 32, now=20)
        assert first.phase == "AR_OVERLAY" and first.row_count == 1
        assert first.generation is not None
        second = _advance(
            connector,
            gate,
            cycle,
            first.generation,
            b"2" * 32,
            now=21,
        )
        assert second.phase == "AR_OVERLAY" and second.row_count == 1
        plan = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN]
        relation_index, _cursor_values = cleanup_module._decode_static_cursor(
            second.cursor,
            plan.phases["AR_OVERLAY"],
            len(plan.root_key),
        )
        assert relation_index > 0

        connector.execute(
            "INSERT INTO catalog_a_file_decision_shadow_seals "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            (analysis_id, file_sha256),
        )
        assert second.generation is not None
        later = _advance(
            connector,
            gate,
            cycle,
            second.generation,
            b"3" * 32,
            now=22,
        )
        assert later.phase == "AR_OVERLAY" and later.row_count == 1

        for attempt in range(32):
            attempt_generation = later.generation
            assert attempt_generation is not None
            protocol_before = _cleanup_protocol_snapshot(connector)
            try:
                later = _advance(
                    connector,
                    gate,
                    cycle,
                    attempt_generation,
                    (attempt + 4).to_bytes(32, "big"),
                    now=23 + attempt,
                )
            except CleanupRetentionBlockedError as error:
                assert "still owns rows" in str(error)
                assert _cleanup_protocol_snapshot(connector) == protocol_before
                blocked_generation = attempt_generation
                break
            assert later.phase == "AR_OVERLAY"
        else:
            raise AssertionError("earlier overlay reappearance did not block terminal")

        connector.execute(
            "DELETE FROM catalog_a_file_decision_shadow_seals "
            "WHERE analysis_id = %s AND file_sha256 = %s",
            (analysis_id, file_sha256),
        )
        resumed = _advance(
            connector,
            gate,
            cycle,
            blocked_generation,
            b"r" * 32,
            now=60,
        )
        assert resumed.phase == "AR_FILE_HASH_VALUES"
        assert resumed.row_count == 0 and resumed.generation == 1
    finally:
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


def test_analysis_wide_root_delete_fault_rolls_back_and_retries(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-root-family-faults.sqlite3")
    try:
        analysis_id = bytes((91,)) + b"a" * 15
        build_id = b"R" * 16
        scope_key = _seed_source_build_scope(connector, discriminator=91)
        seed_analysis_policy(connector)
        _seed_cleanup_sealed_source_build(
            connector,
            build_id=build_id,
            scope_key=scope_key,
            manifest_policy_id=1,
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
        phase_count = len(
            cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases
        )
        for attempt in range(phase_count):
            result = _advance(
                connector,
                gate,
                cycle,
                generation,
                attempt.to_bytes(32, "big"),
                now=3 + attempt,
            )
            assert not result.cycle_complete
            if attempt < phase_count - 1:
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
        assert tables == ("catalog_analysis_run_descriptor",)
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
        assert not any(_analysis_run_family_rows(connector, analysis_id))
    finally:
        connector.close()


def test_canonical_policy_delete_faults_roll_back_every_child_boundary(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "canonical-policy-faults.sqlite3")
    try:
        policy = identity.artifact_policy_digest(
            2,
            _ARTIFACT_ADAPTER_ID,
            _ARTIFACT_POLICY_FINGERPRINT,
        )
        _seed_minimal_canonical_value(
            connector,
            value_sha256=policy,
            page_sha256=b"U" * 32,
            digest_domain=b"artifact_policy_v3",
        )
        seed_artifact_policy_semantics(
            connector,
            artifact_algorithm_version=2,
            adapter_id=_ARTIFACT_ADAPTER_ID,
            policy_fingerprint_sha256=_ARTIFACT_POLICY_FINGERPRINT,
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
        assert committed.row_count == 1
        assert _artifact_policy_semantics_family_rows(connector) == ([],)
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
            "(revision, publication_key, artifact_sha256, artifact_semantics_sha256, "
            "artifact_name, media_type, page_count) "
            "VALUES (1, %s, %s, %s, X'61727469666163742e62696e', "
            "X'6170706c69636174696f6e2f6f637465742d73747265616d', 1)",
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
                    "INSERT INTO catalog_source_scopes "
                    "(scope_key, source_provider, source_root_sha256, "
                    "identity_policy_version) VALUES (%s, %s, %s, 1)",
                    (value, source_provider, source_root),
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
        assert all(
            not rows
            for page_sha256 in (child_page, parent_page)
            for rows in _canonical_page_component_rows(connector, page_sha256)
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
                    "(artifact_sha256, size_bytes) VALUES (%s, 4)",
                    (artifact,),
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
            "catalog_prepared_resource_blob",
            "INSERT INTO catalog_prepared_resource_blob "
            "(candidate_id, publication_key, resource_kind, storage_object_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (b"c" * 16, b"p" * 32, b"acquisition"),
        ),
        (
            "catalog_artifacts",
            "INSERT INTO catalog_artifacts "
            "(revision, publication_key, artifact_semantics_sha256, artifact_sha256, "
            "artifact_name, media_type, page_count) "
            "VALUES (1, %s, %s, %s, X'61727469666163742e62696e', "
            "X'6170706c69636174696f6e2f6f637465742d73747265616d', 1)",
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
                    "(artifact_sha256, size_bytes) VALUES (%s, 4)",
                    (artifact_sha256,),
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
            "UPDATE operational_cleanup_checkpoints "
            "SET chain_sha256 = %s WHERE cleanup_id = %s AND phase = 'CB_ROOT'",
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
            connector.fetch_one(
                "SELECT state, completed_at, final_chain_sha256, final_deleted_count "
                "FROM operational_cleanup_jobs WHERE cleanup_id = %s",
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
            connector.fetch_one(
                "SELECT state, completed_at, final_chain_sha256, final_deleted_count "
                "FROM operational_cleanup_jobs WHERE cleanup_id = %s",
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


def test_staging_identity_delete_rolls_back_on_write_fault(
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
                    "created_at, sealed_at, terminal_byte_count) "
                    "VALUES (%s, %s, 46, 1, 'SEALED', 0, 1, 0)",
                    (staging, build),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 46, 1)",
                    (build,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
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
        _seed_terminal_retirement_authority(
            connector,
            staging_id=staging,
            build_id=build,
            gallery_id=46,
            provisional_observation_id=1,
            final_observation_id=1,
            file_count=0,
            byte_count=0,
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

        def fail_identity_delete(sql: str, parameters: tuple[object, ...] = ()) -> int:
            if sql.startswith(
                "DELETE FROM operational_gallery_observation_staging_requests"
            ):
                raise RuntimeError("injected second-delete fault")
            return original(sql, parameters)

        with (
            patch.object(
                connector, "execute_affected", side_effect=fail_identity_delete
            ),
            pytest.raises(RuntimeError, match="injected"),
        ):
            _advance(connector, gate, cycle, 1, b"4" * 32, now=6)
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
    assert "SELECT COUNT(" not in source
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
            target,
            spec,
            exact=False,
            frozen_root_predicate="1 = 1",
            has_after=False,
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
