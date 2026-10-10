"""Gallery observation, staging, page, and identity cleanup ownership."""

from __future__ import annotations

from h2hdb._cleanup import keys as _keys
from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup import static as _static
from h2hdb._cleanup.model import (
    CleanupCorruptionError,
    CleanupTargetKind,
    _CleanupOperation,
    _Mutation,
    _Mutator,
    _StaticScalar,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan
from h2hdb.vnext_domains import require_positive_int63, require_uuid16
from h2hdb.vnext_gallery_staging_budget import (
    GalleryStagingBudgetCorruptionError,
    lock_gallery_staging_request_budget,
    release_gallery_staging_request_budget,
)
from h2hdb.vnext_gallery_staging_repository import (
    GalleryStagingConflictError,
    GalleryStagingNotReadyError,
    validate_terminal_staging_retirement_authority,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _validate_terminal_staging_cleanup_roots(
    work: VNextUnitOfWork,
    *,
    kind: CleanupTargetKind,
    phase: str,
    frozen_roots: tuple[tuple[_StaticScalar, ...], ...],
    cursor_relation_index: int,
    cursor_values: tuple[_StaticScalar, ...] | None,
) -> None:
    """Fail closed before a generic transaction deletes terminal staging data."""

    staging_ids: list[bytes] = []
    if kind is CleanupTargetKind.GALLERY_OBSERVATION_STAGING:
        for root in frozen_roots:
            if len(root) != 1:
                raise CleanupCorruptionError(
                    "staging cleanup frozen root has an invalid shape"
                )
            staging_ids.append(require_uuid16(root[0], field="cleanup staging_id"))
    elif kind is CleanupTargetKind.GALLERY_OBSERVATION and phase.startswith(
        "GO_STAGING_"
    ):
        for root in frozen_roots:
            if len(root) != 2:
                raise CleanupCorruptionError(
                    "observation cleanup frozen root has an invalid shape"
                )
            gallery_id = require_positive_int63(
                root[0], field="cleanup staging gallery_id"
            )
            observation_id = require_positive_int63(
                root[1], field="cleanup staging observation_id"
            )
            rows = work.connector.fetch_all(
                "SELECT staging_id FROM "
                "operational_gallery_observation_stagings "
                "WHERE gallery_id = %s AND observation_id = %s "
                "AND state IN ('SEALED', 'REUSED', 'RETIRING_SEALED', "
                "'RETIRING_REUSED') ORDER BY staging_id LIMIT 2",
                (gallery_id, observation_id),
            )
            if len(rows) > 1:
                raise CleanupCorruptionError(
                    "observation cleanup found multiple terminal staging roots"
                )
            for row in rows:
                if len(row) != 1:
                    raise CleanupCorruptionError(
                        "observation cleanup staging probe has an invalid shape"
                    )
                staging_ids.append(require_uuid16(row[0], field="cleanup staging_id"))
    else:
        return

    try:
        for staging_id in staging_ids:
            authority = validate_terminal_staging_retirement_authority(
                work.connector,
                staging_id=staging_id,
            )
            if (
                kind is CleanupTargetKind.GALLERY_OBSERVATION_STAGING
                and authority is None
                and not _gos_root_checkpoint_covers_staging(
                    staging_id=staging_id,
                    phase=phase,
                    frozen_roots=frozen_roots,
                    cursor_relation_index=cursor_relation_index,
                    cursor_values=cursor_values,
                )
            ):
                raise CleanupCorruptionError(
                    "staging cleanup frozen root disappeared before its batch"
                )
    except (
        GalleryStagingConflictError,
        GalleryStagingNotReadyError,
        TypeError,
        ValueError,
    ) as error:
        raise CleanupCorruptionError(
            "terminal staging retirement authority is corrupt"
        ) from error


def _gos_root_checkpoint_covers_staging(
    *,
    staging_id: bytes,
    phase: str,
    frozen_roots: tuple[tuple[_StaticScalar, ...], ...],
    cursor_relation_index: int,
    cursor_values: tuple[_StaticScalar, ...] | None,
) -> bool:
    """Prove that an absent GOS header was deleted by a committed ROOT batch."""

    if phase != "GOS_ROOT" or cursor_values is None:
        return False
    if cursor_relation_index != 0 or len(cursor_values) != 2:
        raise CleanupCorruptionError("staging ROOT checkpoint cursor is malformed")
    cursor_root = require_uuid16(cursor_values[0], field="staging ROOT checkpoint root")
    cursor_primary = require_uuid16(
        cursor_values[1], field="staging ROOT checkpoint primary key"
    )
    if cursor_root != cursor_primary or (cursor_root,) not in frozen_roots:
        raise CleanupCorruptionError(
            "staging ROOT checkpoint cursor is outside its frozen root set"
        )
    # GOS_ROOT is the strategy's sole relation and orders both fixed-width keys
    # bytewise.  The durable keyset cursor therefore proves that every frozen
    # staging key through cursor_root was selected and deleted in an earlier
    # atomic batch.  A missing later key still fails closed.
    return staging_id <= cursor_root


_GALLERY_OBSERVATION_STAGING_ELIGIBILITY = """
(
    (r.state IN ('SEALED', 'RETIRING_SEALED') AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        JOIN operational_gallery_staging_source_builds owner ON owner.build_id = m.build_id
        WHERE owner.staging_id = r.staging_id AND m.gallery_id = r.gallery_id
          AND m.observation_id = r.observation_id))
    OR
    (r.state IN ('REUSED', 'RETIRING_REUSED') AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        JOIN operational_gallery_staging_source_builds owner ON owner.build_id = m.build_id
        WHERE owner.staging_id = r.staging_id AND m.gallery_id = r.gallery_id
          AND m.observation_id <> r.observation_id))
    OR
    (r.state IN ('SEALED', 'RETIRING_SEALED') AND EXISTS (
        SELECT 1 FROM catalog_source_collection_observations m
        JOIN operational_gallery_staging_collections owner ON owner.collection_id = m.collection_id
        WHERE owner.staging_id = r.staging_id AND m.gallery_id = r.gallery_id
          AND m.observation_id = r.observation_id))
    OR
    (r.state IN ('REUSED', 'RETIRING_REUSED') AND EXISTS (
        SELECT 1 FROM catalog_source_collection_observations m
        JOIN operational_gallery_staging_collections owner ON owner.collection_id = m.collection_id
        WHERE owner.staging_id = r.staging_id AND m.gallery_id = r.gallery_id
          AND m.observation_id <> r.observation_id))
)
AND NOT EXISTS (
    SELECT 1
    FROM operational_gallery_observation_staging_request_predecessors p
    JOIN operational_gallery_observation_staging_requests prior_owner
      ON prior_owner.request_sha256 = p.prior_request_sha256
    JOIN operational_gallery_observation_staging_requests next_owner
      ON next_owner.request_sha256 = p.request_sha256
    WHERE prior_owner.staging_id <> next_owner.staging_id
      AND (prior_owner.staging_id = r.staging_id
        OR next_owner.staging_id = r.staging_id))
"""

_GALLERY_OBSERVATION_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_source_build_galleries m
    WHERE m.gallery_id = r.gallery_id AND m.observation_id = r.observation_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_collection_observations m
    WHERE m.gallery_id = r.gallery_id AND m.observation_id = r.observation_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_gallery_observation_stagings s
    WHERE s.gallery_id = r.gallery_id AND s.observation_id = r.observation_id
      AND NOT (
        (s.state IN ('OPEN', 'ABANDONED') AND NOT EXISTS (
            SELECT 1
            FROM operational_gallery_observation_staging_claims claim
            JOIN operational_ingest_coordination_heads head
              ON head.current_generation = claim.ingest_generation
            JOIN operational_ingest_generation_owners owner
              ON owner.generation = claim.ingest_generation
            WHERE claim.staging_id = s.staging_id
              AND owner.lease_expires_at > %s))
        OR
        (s.state = 'REUSED' AND EXISTS (
            SELECT 1 FROM catalog_source_build_galleries linked
            JOIN operational_gallery_staging_source_builds binding ON binding.build_id = linked.build_id
            WHERE binding.staging_id = s.staging_id AND linked.gallery_id = s.gallery_id
              AND linked.observation_id <> s.observation_id))
        OR
        (s.state = 'REUSED' AND EXISTS (
            SELECT 1 FROM catalog_source_collection_observations linked
            JOIN operational_gallery_staging_collections binding ON binding.collection_id = linked.collection_id
            WHERE binding.staging_id = s.staging_id AND linked.gallery_id = s.gallery_id
              AND linked.observation_id <> s.observation_id))))
AND NOT EXISTS (
    SELECT 1
    FROM operational_gallery_observation_stagings s
    JOIN operational_gallery_observation_staging_requests prior_owner
      ON prior_owner.staging_id = s.staging_id
    JOIN operational_gallery_observation_staging_request_predecessors p
      ON p.prior_request_sha256 = prior_owner.request_sha256
    JOIN operational_gallery_observation_staging_requests next_owner
      ON next_owner.request_sha256 = p.request_sha256
    WHERE s.gallery_id = r.gallery_id AND s.observation_id = r.observation_id
      AND next_owner.staging_id <> s.staging_id)
"""


def _staging_owned_spec(table: str, primary_key: tuple[str, ...]) -> _StaticDeleteSpec:
    return _plan._owned_spec(
        table,
        primary_key,
        "operational_gallery_observation_stagings",
        ("staging_id",),
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = r.staging_id)"
        ),
    )


def _staging_request_spec(
    table: str, primary_key: tuple[str, ...]
) -> _StaticDeleteSpec:
    return _plan._indirect_spec(
        table,
        primary_key,
        f"{table} AS c "
        "JOIN operational_gallery_observation_staging_requests AS owned "
        "ON owned.request_sha256 = c.request_sha256 "
        "JOIN operational_gallery_observation_stagings AS r "
        "ON r.staging_id = owned.staging_id",
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = r.staging_id)"
        ),
    )


_STAGING_ROOT_DELETE = (
    "DELETE FROM operational_gallery_staging_source_builds WHERE staging_id = %s",
    "DELETE FROM operational_gallery_staging_collections WHERE staging_id = %s",
    "DELETE FROM operational_gallery_observation_stagings WHERE staging_id = %s",
)

_STAGING_ROOT_ALLOWED = (frozenset((0, 1)), frozenset((0, 1)), frozenset((1,)))


def _gallery_observation_staging_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    request_identity = _plan._indirect_spec(
        "operational_gallery_observation_staging_requests",
        ("request_sha256",),
        "operational_gallery_observation_staging_requests AS c "
        "JOIN operational_gallery_observation_stagings AS r "
        "ON r.staging_id = c.staging_id",
        delete_sql=(
            "DELETE FROM operational_gallery_observation_staging_requests "
            "WHERE request_sha256 = %s",
        ),
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = r.staging_id)"
        ),
    )
    return {
        "GOS_RECEIPT_FRONTIER": (
            _staging_owned_spec(
                "operational_gallery_observation_staging_receipts",
                ("staging_id", "component", "level"),
            ),
            _staging_request_spec(
                "operational_gallery_observation_staging_frontiers",
                ("request_sha256",),
            ),
            _staging_owned_spec(
                "operational_gallery_observation_staging_match_receipts",
                ("staging_id",),
            ),
        ),
        "GOS_PAGE_ASSOCIATION": (
            _staging_request_spec(
                "operational_gallery_observation_staging_request_pages",
                ("request_sha256",),
            ),
        ),
        "GOS_REQUEST_DESCRIPTOR": (
            _staging_owned_spec(
                "operational_gallery_observation_staging_page_requests",
                ("request_sha256",),
            ),
            _staging_owned_spec(
                "operational_gallery_observation_staging_match_requests",
                ("request_sha256",),
            ),
            _staging_request_spec(
                "operational_gallery_observation_staging_request_predecessors",
                ("request_sha256",),
            ),
            _staging_request_spec(
                "operational_gallery_observation_staging_request_chunks",
                ("request_sha256", "position"),
            ),
        ),
        "GOS_REQUEST_IDENTITY": (request_identity,),
        "GOS_CHECKPOINT": (
            _staging_owned_spec(
                "operational_gallery_observation_staging_checkpoints",
                ("staging_id", "component", "level"),
            ),
            _staging_owned_spec(
                "operational_gallery_observation_staging_match_checkpoints",
                ("staging_id",),
            ),
            _staging_owned_spec(
                "operational_gallery_observation_staging_metadata_parsers",
                ("staging_id",),
            ),
        ),
        "GOS_CLAIM": (
            _staging_owned_spec(
                "operational_gallery_observation_staging_claims", ("staging_id",)
            ),
        ),
        "GOS_ROOT": (
            _plan._owned_spec(
                "operational_gallery_observation_stagings",
                ("staging_id",),
                "operational_gallery_observation_stagings",
                ("staging_id",),
                delete_sql=_STAGING_ROOT_DELETE,
                delete_allowed_affected=_STAGING_ROOT_ALLOWED,
            ),
        ),
    }


def _observation_staging_direct(
    table: str, primary_key: tuple[str, ...]
) -> _StaticDeleteSpec:
    return _plan._indirect_spec(
        table,
        primary_key,
        f"{table} AS c "
        "JOIN operational_gallery_observation_stagings AS staged "
        "ON staged.staging_id = c.staging_id "
        "JOIN catalog_gallery_observation_allocations AS r "
        "ON r.gallery_id = staged.gallery_id "
        "AND r.observation_id = staged.observation_id",
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = staged.staging_id)"
        ),
    )


def _observation_request_spec(
    table: str, primary_key: tuple[str, ...]
) -> _StaticDeleteSpec:
    return _plan._indirect_spec(
        table,
        primary_key,
        f"{table} AS c "
        "JOIN operational_gallery_observation_staging_requests AS owned "
        "ON owned.request_sha256 = c.request_sha256 "
        "JOIN operational_gallery_observation_stagings AS staged "
        "ON staged.staging_id = owned.staging_id "
        "JOIN catalog_gallery_observation_allocations AS r "
        "ON r.gallery_id = staged.gallery_id "
        "AND r.observation_id = staged.observation_id",
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = staged.staging_id)"
        ),
    )


def _gallery_observation_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_gallery_observation_allocations"
    key = ("gallery_id", "observation_id")

    def direct(
        table: str,
        pk: tuple[str, ...],
        *,
        batch_exact_primary_keys: bool = False,
    ) -> _StaticDeleteSpec:
        return _plan._owned_spec(
            table,
            pk,
            root,
            key,
            batch_exact_primary_keys=batch_exact_primary_keys,
        )

    request_identity = _plan._indirect_spec(
        "operational_gallery_observation_staging_requests",
        ("request_sha256",),
        "operational_gallery_observation_staging_requests AS c "
        "JOIN operational_gallery_observation_stagings AS staged "
        "ON staged.staging_id = c.staging_id "
        "JOIN catalog_gallery_observation_allocations AS r "
        "ON r.gallery_id = staged.gallery_id "
        "AND r.observation_id = staged.observation_id",
        delete_sql=(
            "DELETE FROM operational_gallery_observation_staging_requests "
            "WHERE request_sha256 = %s",
        ),
        extra_predicate=(
            "EXISTS (SELECT 1 "
            "FROM operational_gallery_observation_staging_claims AS exact_claim "
            "WHERE exact_claim.staging_id = staged.staging_id)"
        ),
    )
    return {
        "GO_STAGING_RECEIPT_FRONTIER": (
            _observation_staging_direct(
                "operational_gallery_observation_staging_receipts",
                ("staging_id", "component", "level"),
            ),
            _observation_request_spec(
                "operational_gallery_observation_staging_frontiers",
                ("request_sha256",),
            ),
            _observation_staging_direct(
                "operational_gallery_observation_staging_match_receipts",
                ("staging_id",),
            ),
        ),
        "GO_STAGING_PAGE_ASSOCIATION": (
            _observation_request_spec(
                "operational_gallery_observation_staging_request_pages",
                ("request_sha256",),
            ),
        ),
        "GO_STAGING_REQUEST_DESCRIPTOR": (
            _observation_staging_direct(
                "operational_gallery_observation_staging_page_requests",
                ("request_sha256",),
            ),
            _observation_staging_direct(
                "operational_gallery_observation_staging_match_requests",
                ("request_sha256",),
            ),
            _observation_request_spec(
                "operational_gallery_observation_staging_request_predecessors",
                ("request_sha256",),
            ),
            _observation_request_spec(
                "operational_gallery_observation_staging_request_chunks",
                ("request_sha256", "position"),
            ),
        ),
        "GO_STAGING_REQUEST_IDENTITY": (request_identity,),
        "GO_STAGING_CHECKPOINT": (
            _observation_staging_direct(
                "operational_gallery_observation_staging_checkpoints",
                ("staging_id", "component", "level"),
            ),
            _observation_staging_direct(
                "operational_gallery_observation_staging_match_checkpoints",
                ("staging_id",),
            ),
            _observation_staging_direct(
                "operational_gallery_observation_staging_metadata_parsers",
                ("staging_id",),
            ),
        ),
        "GO_STAGING_CLAIM": (
            _observation_staging_direct(
                "operational_gallery_observation_staging_claims", ("staging_id",)
            ),
        ),
        "GO_STAGING_ROOT": (
            _plan._indirect_spec(
                "operational_gallery_observation_stagings",
                ("staging_id",),
                "operational_gallery_observation_stagings AS c "
                "JOIN catalog_gallery_observation_allocations AS r "
                "ON r.gallery_id = c.gallery_id "
                "AND r.observation_id = c.observation_id",
                delete_sql=_STAGING_ROOT_DELETE,
                delete_allowed_affected=_STAGING_ROOT_ALLOWED,
            ),
        ),
        "GO_FACTS": tuple(
            direct(
                table,
                pk,
                batch_exact_primary_keys=(
                    table == "catalog_gallery_observation_file_hash_occurrences"
                ),
            )
            for table, pk in (
                (
                    "catalog_gallery_observation_completion_marker",
                    ("gallery_id", "observation_id"),
                ),
                (
                    "catalog_gallery_manifests",
                    ("gallery_id", "observation_id", "manifest_policy_id"),
                ),
                (
                    "catalog_gallery_observation_file_hash_occurrences",
                    ("gallery_id", "observation_id", "file_sha256"),
                ),
                (
                    "catalog_gallery_observation_artists",
                    ("gallery_id", "observation_id", "artist_tag_id"),
                ),
                (
                    "catalog_gallery_observation_tags",
                    ("gallery_id", "observation_id", "position"),
                ),
            )
        ),
        "GO_FILESYSTEM_SEAL": (
            direct(
                "catalog_gallery_observation_file_filesystem_seals",
                ("gallery_id", "observation_id", "file_key"),
                batch_exact_primary_keys=True,
            ),
        ),
        "GO_FILESYSTEM_VALUES": tuple(
            direct(
                table,
                ("gallery_id", "observation_id", "file_key"),
                batch_exact_primary_keys=True,
            )
            for table in (
                "catalog_gallery_observation_file_filesystem_devices",
                "catalog_gallery_observation_file_filesystem_inodes",
                "catalog_gallery_observation_file_filesystem_modified_nses",
                "catalog_gallery_observation_file_filesystem_changed_nses",
            )
        ),
        "GO_FILESYSTEM_ANCHOR": (
            direct(
                "catalog_gallery_observation_file_filesystem_anchors",
                ("gallery_id", "observation_id", "file_key"),
                batch_exact_primary_keys=True,
            ),
        ),
        "GO_FILES": (
            _plan._owned_spec(
                "catalog_gallery_observation_file_anchors",
                ("gallery_id", "observation_id", "file_key"),
                root,
                key,
                delete_sql=(
                    "DELETE FROM catalog_gallery_observation_file_seals "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                    "DELETE FROM catalog_gallery_observation_file_file_sha256s "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                    "DELETE FROM catalog_gallery_observation_file_file_nos "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                    "DELETE FROM catalog_gallery_observation_file_artifact_role "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                    "DELETE FROM catalog_gallery_observation_file_anchors "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                ),
            ),
        ),
        "GO_OBSERVATION_FACTS": tuple(
            direct(table, ("gallery_id", "observation_id"))
            for table in (
                "catalog_gallery_observation_validation_sources",
                "catalog_gallery_observation_validation_reasons",
                "catalog_gallery_observation_validation_dispositions",
                "catalog_gallery_observation_validation_policies",
                "catalog_gallery_observation_metadata_locals",
                "catalog_gallery_observation_upload_times",
                "catalog_gallery_observation_directories",
                "catalog_gallery_observation_stat",
                "catalog_gallery_observation_scans",
            )
        ),
        "GO_DESCRIPTOR": tuple(
            direct(table, pk)
            for table, pk in (
                ("catalog_gallery_observations", ("gallery_id", "observation_id")),
                (
                    "catalog_gallery_observation_tree_roots",
                    ("gallery_id", "observation_id", "root_page_sha256"),
                ),
                (
                    "catalog_gallery_observation_allocation_pages",
                    ("gallery_id", "observation_id", "page_sha256"),
                ),
                (
                    "catalog_gallery_observation_discovery_fingerprints",
                    ("gallery_id", "observation_id"),
                ),
                (
                    "catalog_gallery_observation_metadata_digests",
                    ("gallery_id", "observation_id"),
                ),
                (
                    "catalog_gallery_observation_raw_content",
                    ("gallery_id", "observation_id"),
                ),
                (
                    "catalog_gallery_observation_page_counts",
                    ("gallery_id", "observation_id"),
                ),
            )
        ),
        "GO_ROOT": (direct(root, key),),
    }


_GALLERY_PAGE_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_allocation_pages x
    WHERE x.page_sha256 = r.page_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_tree_roots x
    WHERE x.root_page_sha256 = r.page_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_page_children x
    WHERE x.child_sha256 = r.page_sha256)
AND NOT EXISTS (
    SELECT 1 FROM operational_gallery_observation_staging_request_pages x
    WHERE x.page_sha256 = r.page_sha256)
"""


def _gallery_page_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_gallery_observation_page_descriptor_anchors"
    key = ("page_sha256",)
    return {
        "GOP_OUTGOING_CHILD": (
            _plan._owned_spec(
                "catalog_gallery_observation_page_children",
                ("parent_sha256", "position"),
                root,
                key,
                ("parent_sha256",),
            ),
        ),
        "GOP_BOUNDS": (
            _plan._owned_spec(
                "catalog_gallery_observation_page_key_bounds_seals",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_key_bounds_first_keys",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_key_bounds_last_keys",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_key_bounds_anchors",
                ("page_sha256",),
                root,
                key,
            ),
        ),
        "GOP_DESCRIPTOR": (
            _plan._owned_spec(
                "catalog_gallery_observation_page_descriptor_seals",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_descriptor_components",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_descriptor_levels",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_page_descriptor_subtree_item_counts",
                ("page_sha256",),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_gallery_observation_pages",
                ("page_sha256",),
                root,
                key,
            ),
        ),
        "GOP_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


_GALLERY_IDENTITY_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_allocations x
            WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_source_build_expected_gallery x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_galleries x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_galleries x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_candidate_shadows x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_candidate_tombstones x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_shadows x
                WHERE x.owner_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_provenance x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_content x
                WHERE x.witness_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_gid_provenance_storage x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_candidate_shadows x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_candidate_tombstones x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_winner_selections x
                WHERE x.winner_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_selection_storage x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_metadata_locals x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM operational_gallery_redownload_states x
                WHERE x.gallery_id = r.gallery_id)
"""


def _gallery_identity_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_gallery_identities"
    key = ("gallery_id",)
    return {
        "GI_OBSERVATION_ALLOCATOR": (
            _plan._owned_spec(
                "operational_gallery_observation_allocators",
                ("gallery_id",),
                root,
                key,
            ),
        ),
        "GI_SOURCE_NAME_ACCESS": (
            _plan._owned_spec(
                "catalog_gallery_source_name_accesses",
                ("gallery_id",),
                root,
                key,
            ),
        ),
        "GI_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


_SOURCE_GALLERY_NAME_GID_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM catalog_gallery_source_name_accesses x
            WHERE x.source_gallery_name = r.source_gallery_name)
"""


def _source_gallery_name_gid_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_gallery_name_gids"
    key = ("source_gallery_name",)
    return {"SNG_ROOT": (_plan._owned_spec(root, key, root, key),)}


_GALLERY_GID_IDENTITY_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM catalog_source_gallery_name_gids x
            WHERE x.gid = r.gid)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_identities x
                WHERE x.gid = r.gid)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_gid_storage x
                WHERE x.gid = r.gid)
"""


def _gallery_gid_identity_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_gallery_gid_identities"
    key = ("gid",)
    return {"GGI_ROOT": (_plan._owned_spec(root, key, root, key),)}


_GALLERY_OBSERVATION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.GALLERY_OBSERVATION,
    "catalog_gallery_observation_allocations",
    ("gallery_id", "observation_id"),
    "gallery_id",
    None,
    _GALLERY_OBSERVATION_ELIGIBILITY,
    _gallery_observation_phases(),
    uses_cutoff=True,
)

_GALLERY_OBSERVATION_STAGING_PLAN = _StaticTargetPlan(
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
    "operational_gallery_observation_stagings",
    ("staging_id",),
    "staging_id",
    16,
    _GALLERY_OBSERVATION_STAGING_ELIGIBILITY,
    _gallery_observation_staging_phases(),
)

_GALLERY_OBSERVATION_PAGE_PLAN = _StaticTargetPlan(
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
    "catalog_gallery_observation_page_descriptor_anchors",
    ("page_sha256",),
    "page_sha256",
    32,
    _GALLERY_PAGE_ELIGIBILITY,
    _gallery_page_phases(),
)

_GALLERY_IDENTITY_PLAN = _StaticTargetPlan(
    CleanupTargetKind.GALLERY_IDENTITY,
    "catalog_gallery_identities",
    ("gallery_id",),
    "gallery_id",
    None,
    _GALLERY_IDENTITY_ELIGIBILITY,
    _gallery_identity_phases(),
)

_SOURCE_GALLERY_NAME_GID_PLAN = _StaticTargetPlan(
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
    "catalog_source_gallery_name_gids",
    ("source_gallery_name",),
    "source_gallery_name",
    255,
    _SOURCE_GALLERY_NAME_GID_ELIGIBILITY,
    _source_gallery_name_gid_phases(),
    variable_width_shard=True,
)

_GALLERY_GID_IDENTITY_PLAN = _StaticTargetPlan(
    CleanupTargetKind.GALLERY_GID_IDENTITY,
    "catalog_gallery_gid_identities",
    ("gid",),
    "gid",
    None,
    _GALLERY_GID_IDENTITY_ELIGIBILITY,
    _gallery_gid_identity_phases(),
)


def _gallery_observation_mutator(plan: _StaticTargetPlan, phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        if operation.cycle.target_kind is not plan.kind:
            raise CleanupCorruptionError("gallery cleanup static strategy kind drifted")
        work = operation.work
        start_index, start_values = _keys._decode_static_cursor(
            cursor, plan.phases[phase], len(plan.root_key)
        )
        frozen_roots = operation.frozen_roots
        _validate_terminal_staging_cleanup_roots(
            work,
            kind=plan.kind,
            phase=phase,
            frozen_roots=frozen_roots,
            cursor_relation_index=start_index,
            cursor_values=start_values,
        )
        request_budget_retained: int | None = None
        if phase in {"GOS_REQUEST_IDENTITY", "GO_STAGING_REQUEST_IDENTITY"}:
            try:
                request_budget_retained = lock_gallery_staging_request_budget(work)
            except GalleryStagingBudgetCorruptionError as error:
                raise CleanupCorruptionError(str(error)) from error
        mutation = _static._run_static_phase(operation, cursor, plan, phase)
        if request_budget_retained is not None and mutation.row_keys:
            try:
                release_gallery_staging_request_budget(
                    work,
                    retained_request_count=request_budget_retained,
                    deleted_count=len(mutation.row_keys),
                )
            except GalleryStagingBudgetCorruptionError as error:
                raise CleanupCorruptionError(str(error)) from error
        return mutation

    return mutate
