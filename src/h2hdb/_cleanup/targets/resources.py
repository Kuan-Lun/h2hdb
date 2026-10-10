"""Opaque resource and identity cleanup with exact retention revalidation."""

from __future__ import annotations

from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup.model import (
    CleanupCorruptionError,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    _CleanupOperation,
    _Mutation,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan
from h2hdb.vnext_domains import require_digest32
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key


def _digest_bounds(cycle: CleanupCycle, cursor: bytes) -> tuple[bytes, bytes, int]:
    if cursor:
        require_digest32(cursor, field="digest cleanup cursor")
        if cursor[0] != cycle.shard_no:
            raise CleanupCorruptionError("cleanup cursor escaped its fixed shard")
    lower = bytes((cycle.shard_no,)) + bytes(31)
    if cycle.shard_no == 255:
        return lower, b"", 1
    return lower, bytes((cycle.shard_no + 1,)) + bytes(31), 0


def _select_content_blobs(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
    work, cycle = operation.work, operation.cycle
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT b.file_sha256
        FROM catalog_content_blobs AS b
        WHERE b.file_sha256 >= %s
          AND (%s = 0 OR b.file_sha256 > %s)
          AND (%s = 1 OR b.file_sha256 < %s)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = b.file_sha256)
        ORDER BY b.file_sha256
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="content blob key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-content-blob", key),
            """
            SELECT b.file_sha256
            FROM catalog_content_blobs AS b
            WHERE b.file_sha256 = %s
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = b.file_sha256)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError("content blob gained a retention root")
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_content_blobs WHERE file_sha256 = %s", (key,)
            )
            != 1
        ):
            raise CleanupUnavailableError("content blob changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_file_name_identities(
    operation: _CleanupOperation, cursor: bytes
) -> _Mutation:
    work, cycle = operation.work, operation.cycle
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT n.file_key
        FROM catalog_file_name_identities AS n
        WHERE n.file_key >= %s
          AND (%s = 0 OR n.file_key > %s)
          AND (%s = 1 OR n.file_key < %s)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_anchors x WHERE x.file_key = n.file_key)
        ORDER BY n.file_key
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="file-name key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-file-name", key),
            """
            SELECT n.file_key FROM catalog_file_name_identities AS n
            WHERE n.file_key = %s
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_anchors x WHERE x.file_key = n.file_key)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError(
                "file-name identity gained a retention root"
            )
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_file_name_identities WHERE file_key = %s",
                (key,),
            )
            != 1
        ):
            raise CleanupUnavailableError("file-name identity changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_publication_identities(
    operation: _CleanupOperation, cursor: bytes
) -> _Mutation:
    work, cycle = operation.work, operation.cycle
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT p.publication_key
        FROM catalog_publication_identities AS p
        WHERE p.publication_key >= %s
          AND (%s = 0 OR p.publication_key > %s)
          AND (%s = 1 OR p.publication_key < %s)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_publication_occurrence_identities x
              WHERE x.publication_key = p.publication_key)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_publication_selection_occurrence_identities x
              WHERE x.publication_key = p.publication_key)
        ORDER BY p.publication_key
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="publication key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-publication-identity", key),
            """
            SELECT p.publication_key FROM catalog_publication_identities AS p
            WHERE p.publication_key = %s
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_publication_occurrence_identities x
                  WHERE x.publication_key = p.publication_key)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_publication_selection_occurrence_identities x
                  WHERE x.publication_key = p.publication_key)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError(
                "publication identity gained a retention root"
            )
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_publication_identities WHERE publication_key = %s",
                (key,),
            )
            != 1
        ):
            raise CleanupUnavailableError("publication identity changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_artifact_blobs(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
    work, cycle = operation.work, operation.cycle
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT artifact_blob.artifact_sha256
        FROM catalog_artifact_blobs AS artifact_blob
        WHERE artifact_blob.artifact_sha256 >= %s
          AND (%s = 0 OR artifact_blob.artifact_sha256 > %s)
          AND (%s = 1 OR artifact_blob.artifact_sha256 < %s)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_artifact_descriptors prepared
              WHERE prepared.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_resource_blob prepared_resource
              WHERE prepared_resource.storage_object_sha256 =
                    artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_artifacts retained
              WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_storage_objects stored_resource
              WHERE stored_resource.storage_object_sha256 =
                    artifact_blob.artifact_sha256)
        ORDER BY artifact_blob.artifact_sha256
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="artifact blob key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-artifact-blob", key),
            """
            SELECT artifact_blob.artifact_sha256
            FROM catalog_artifact_blobs AS artifact_blob
            WHERE artifact_blob.artifact_sha256 = %s
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_prepared_artifact_descriptors prepared
                  WHERE prepared.artifact_sha256 = artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_prepared_resource_blob prepared_resource
                  WHERE prepared_resource.storage_object_sha256 =
                        artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_artifacts retained
                  WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_storage_objects stored_resource
                  WHERE stored_resource.storage_object_sha256 =
                        artifact_blob.artifact_sha256)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError("artifact blob became retained")
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
                (key,),
            )
            != 1
        ):
            raise CleanupUnavailableError("artifact blob changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


_STORAGE_OBJECT_KEY_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_prepared_artifacts prepared
    WHERE prepared.storage_object_key_sha256 = r.storage_object_key_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_storage_objects retained
    WHERE retained.storage_object_key_sha256 = r.storage_object_key_sha256)
"""


def _storage_object_key_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_storage_object_key_identities"
    key = ("storage_object_key_sha256",)
    return {
        "SK_SEGMENT": (
            _plan._owned_spec(
                "catalog_storage_object_key_segments",
                ("storage_object_key_sha256", "segment_position"),
                root,
                key,
            ),
        ),
        "SK_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


def _next_artifact_blob_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT artifact_blob.artifact_sha256
        FROM catalog_artifact_blobs AS artifact_blob
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_prepared_artifact_descriptors prepared
            WHERE prepared.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_resource_blob prepared_resource
              WHERE prepared_resource.storage_object_sha256 =
                    artifact_blob.artifact_sha256)
          AND NOT EXISTS (
            SELECT 1 FROM catalog_artifacts retained
            WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
            SELECT 1 FROM catalog_storage_objects stored_resource
            WHERE stored_resource.storage_object_sha256 =
                  artifact_blob.artifact_sha256)
        ORDER BY artifact_blob.artifact_sha256
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="artifact blob candidate")


def _next_publication_identity_candidate_shard(
    work: VNextUnitOfWork,
) -> int | None:
    row = work.connector.fetch_one("""
        SELECT identity.publication_key
        FROM catalog_publication_identities AS identity
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_publication_occurrence_identities occurrence
            WHERE occurrence.publication_key = identity.publication_key)
          AND NOT EXISTS (
            SELECT 1
            FROM catalog_publication_selection_occurrence_identities selection
            WHERE selection.publication_key = identity.publication_key)
        ORDER BY identity.publication_key
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="publication identity candidate")


def _next_file_name_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT identity.file_key
        FROM catalog_file_name_identities AS identity
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_gallery_observation_file_anchors retained
            WHERE retained.file_key = identity.file_key)
        ORDER BY identity.file_key
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="file-name identity candidate")


def _next_content_blob_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT content_blob.file_sha256
        FROM catalog_content_blobs AS content_blob
        WHERE NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = content_blob.file_sha256)
        ORDER BY content_blob.file_sha256
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="content blob candidate")


def _digest_candidate_shard(
    row: tuple[object, ...] | None, *, field: str
) -> int | None:
    if not row:
        return None
    if len(row) != 1:
        raise CleanupCorruptionError(f"{field} probe returned an invalid shape")
    return require_digest32(row[0], field=field)[0]


_STORAGE_OBJECT_KEY_PLAN = _StaticTargetPlan(
    CleanupTargetKind.STORAGE_OBJECT_KEY,
    "catalog_storage_object_key_identities",
    ("storage_object_key_sha256",),
    "storage_object_key_sha256",
    32,
    _STORAGE_OBJECT_KEY_ELIGIBILITY,
    _storage_object_key_phases(),
)
