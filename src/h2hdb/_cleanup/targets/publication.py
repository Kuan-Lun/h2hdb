"""Published catalog, candidate, and descriptor cleanup plans."""

from __future__ import annotations

from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup.model import CleanupTargetKind
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan

_PUBLICATION_CANDIDATE_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM operational_catalog_working_candidates x
    WHERE x.candidate_id = r.candidate_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_prepared_artifacts protected
    WHERE protected.candidate_id = r.candidate_id
      AND protected.state IN ('PENDING', 'PREPARED'))
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.candidate_id = r.candidate_id
      AND (
        NOT EXISTS (
            SELECT 1 FROM catalog_publication_commit_finalizations finalized
            WHERE finalized.receipt_id = committed.receipt_id)
        OR EXISTS (
            SELECT 1 FROM catalog_publication_commit_head_receipts head
            WHERE head.receipt_id = committed.receipt_id)))
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    JOIN catalog_source_build_base_publication_commits base
      ON base.base_receipt_id = committed.receipt_id
    WHERE committed.candidate_id = r.candidate_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    JOIN catalog_publication_candidates reserved
      ON reserved.candidate_id = committed.candidate_id
    JOIN catalog_publication_occurrence_identities projected
      ON projected.revision = reserved.reserved_revision
    WHERE committed.candidate_id = r.candidate_id)
"""

_CATALOG_PUBLICATION_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM catalog_publication_receipts finalized
    WHERE finalized.revision = r.revision
      AND finalized.state = 'PUBLISHED'
      AND finalized.finalized_at IS NOT NULL)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_head_receipts head
    JOIN catalog_publication_receipts current
      ON current.receipt_id = head.receipt_id
    WHERE current.revision > r.revision
      AND current.state = 'PUBLISHED'
      AND current.finalized_at IS NOT NULL)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidate_base_publication_commits base
    JOIN operational_catalog_working_candidates working
      ON working.candidate_id = base.candidate_id
    JOIN catalog_publication_commits pinned
      ON pinned.receipt_id = base.base_receipt_id
    WHERE pinned.revision = r.revision)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    JOIN operational_source_working_builds working
      ON working.build_id = base.build_id
    JOIN catalog_publication_commits pinned
      ON pinned.receipt_id = base.base_receipt_id
    WHERE pinned.revision = r.revision)
"""

_CATALOG_REVISION_DESCRIPTOR_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_occurrence_identities retained
    WHERE retained.revision = r.revision)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits retained
    WHERE retained.revision = r.revision)
"""

_SOURCE_REVISION_DESCRIPTOR_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_source_revision_provenance retained
    WHERE retained.source_revision = r.source_revision)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits retained
    WHERE retained.source_revision = r.source_revision)
"""

_PUBLICATION_GENERATION_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits retained
    WHERE retained.generation = r.generation)
AND (
    SELECT MIN(retained.generation) FROM catalog_publication_commits retained) > 1
AND r.generation < (
    SELECT MIN(retained.generation) FROM catalog_publication_commits retained)
"""


def _catalog_revision_descriptor_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_revision_descriptors"
    key = ("revision",)
    return {
        "CRD_ROOT": (
            _plan._owned_spec("catalog_discovery_seals", key, root, key),
            _plan._owned_spec(
                "catalog_tag_directory_order",
                ("revision", "namespace", "position"),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_language_facet_order",
                ("revision", "position"),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_subject_facet_order",
                ("revision", "position"),
                root,
                key,
            ),
            _plan._owned_spec(
                "catalog_contributor_facet_order",
                ("revision", "position"),
                root,
                key,
            ),
            _plan._owned_spec(root, key, root, key),
        )
    }


def _source_revision_descriptor_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_revision_descriptors"
    key = ("source_revision",)
    return {"SRD_ROOT": (_plan._owned_spec(root, key, root, key),)}


def _publication_generation_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_generation_nodes"
    key = ("generation",)
    edge = "catalog_publication_generation_successors"
    return {
        "PG_EDGE": (
            _plan._owned_spec(
                edge,
                ("successor_generation",),
                root,
                key,
                ("successor_generation",),
            ),
            _plan._owned_spec(
                edge,
                ("successor_generation",),
                root,
                key,
                ("predecessor_generation",),
            ),
        ),
        "PG_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


def _catalog_publication_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_occurrence_identities"
    key = ("revision", "publication_key")

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _plan._owned_spec(table, pk, root, key, batch_exact_primary_keys=True)

    storage = _plan._indirect_spec(
        "catalog_publication_storage",
        ("catalog_occurrence_sha256",),
        "catalog_publication_storage AS c "
        "JOIN catalog_publication_occurrence_identities AS r "
        "ON r.catalog_occurrence_sha256 = c.catalog_occurrence_sha256",
        batch_exact_primary_keys=True,
    )
    download_time = _plan._indirect_spec(
        "catalog_publication_download_times",
        ("catalog_occurrence_sha256",),
        "catalog_publication_download_times AS c "
        "JOIN catalog_publication_occurrence_identities AS r "
        "ON r.catalog_occurrence_sha256 = c.catalog_occurrence_sha256",
        batch_exact_primary_keys=True,
    )
    upload_time = _plan._indirect_spec(
        "catalog_publication_upload_times",
        ("catalog_occurrence_sha256",),
        "catalog_publication_upload_times AS c "
        "JOIN catalog_publication_occurrence_identities AS r "
        "ON r.catalog_occurrence_sha256 = c.catalog_occurrence_sha256",
        batch_exact_primary_keys=True,
    )
    tag_directory = _plan._indirect_spec(
        "catalog_tag_directory_order",
        ("revision", "namespace", "position"),
        "catalog_tag_directory_order AS c "
        "JOIN catalog_tag_terms AS term "
        "ON term.namespace = c.namespace "
        "AND term.tag_value_sha256 = c.tag_value_sha256 "
        "JOIN catalog_tag_publication_order AS first_publication "
        "ON first_publication.revision = c.revision "
        "AND first_publication.tag_id = term.tag_id "
        "AND first_publication.position = 0 "
        "JOIN catalog_publication_occurrence_identities AS r "
        "ON r.revision = first_publication.revision "
        "AND r.publication_key = first_publication.publication_key",
        batch_exact_primary_keys=True,
    )

    return {
        "CP_STORAGE": (
            direct(
                "catalog_title_search_postings",
                ("revision", "value_sha256", "publication_key"),
            ),
            direct(
                "catalog_search_postings",
                ("revision", "value_sha256", "publication_key"),
            ),
            direct("catalog_search_documents", key),
            direct(
                "catalog_pages",
                ("revision", "publication_key", "page_index"),
            ),
            direct("catalog_thumbnails", key),
            direct(
                "catalog_storage_objects",
                ("revision", "publication_key", "resource_kind"),
            ),
            storage,
        ),
        "CP_DOWNLOAD_TIME": (download_time,),
        "CP_CONTRIBUTOR": (
            direct(
                "catalog_contributors",
                (
                    "revision",
                    "publication_key",
                    "contributor_name_sha256",
                    "role",
                ),
            ),
        ),
        "CP_ORDER": (
            tag_directory,
            direct(
                "catalog_tag_publication_order",
                ("revision", "tag_id", "position"),
            ),
            direct(
                "catalog_publication_order",
                ("revision", "position"),
            ),
        ),
        "CP_CONTENT": (direct("catalog_publication_contents", key),),
        "CP_SUBJECT": (
            direct(
                "catalog_subjects",
                ("revision", "publication_key", "position"),
            ),
        ),
        "CP_ARTIFACT": (direct("catalog_artifacts", key),),
        "CP_UPLOAD_TIME": (upload_time,),
        "CP_ROOT": (direct(root, key),),
    }


def _publication_candidate_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_candidates"
    key = ("candidate_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _plan._owned_spec(table, pk, root, key)

    uncommitted = (
        "NOT EXISTS (SELECT 1 FROM catalog_publication_commits committed "
        "WHERE committed.candidate_id = r.candidate_id)"
    )

    prepared_source = (
        "catalog_prepared_artifacts AS c "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = c.candidate_id"
    )
    prepared_key = ("candidate_id", "publication_key", "resource_kind")
    prepared = _plan._indirect_spec(
        "catalog_prepared_artifacts",
        prepared_key,
        prepared_source,
        delete_sql=(
            "DELETE FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s "
            "AND resource_kind = %s",
        ),
    )
    selection_storage = _plan._indirect_spec(
        "catalog_publication_selection_storage",
        ("selection_occurrence_sha256",),
        "catalog_publication_selection_storage AS c "
        "JOIN catalog_publication_selection_occurrence_identities AS occurrence "
        "ON occurrence.selection_occurrence_sha256 = "
        "c.selection_occurrence_sha256 "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = occurrence.candidate_id",
    )
    projection_storage = _plan._indirect_spec(
        "catalog_publication_storage",
        ("catalog_occurrence_sha256",),
        "catalog_publication_storage AS c "
        "JOIN catalog_publication_occurrence_identities AS occurrence "
        "ON occurrence.catalog_occurrence_sha256 = c.catalog_occurrence_sha256 "
        "JOIN catalog_publication_candidates AS reserved "
        "ON reserved.reserved_revision = occurrence.revision "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = reserved.candidate_id",
        extra_predicate=uncommitted,
    )
    projection_download_time = _plan._indirect_spec(
        "catalog_publication_download_times",
        ("catalog_occurrence_sha256",),
        "catalog_publication_download_times AS c "
        "JOIN catalog_publication_occurrence_identities AS occurrence "
        "ON occurrence.catalog_occurrence_sha256 = "
        "c.catalog_occurrence_sha256 "
        "JOIN catalog_publication_candidates AS reserved "
        "ON reserved.reserved_revision = occurrence.revision "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = reserved.candidate_id",
        extra_predicate=uncommitted,
    )

    projection_upload_time = _plan._indirect_spec(
        "catalog_publication_upload_times",
        ("catalog_occurrence_sha256",),
        "catalog_publication_upload_times AS c "
        "JOIN catalog_publication_occurrence_identities AS occurrence "
        "ON occurrence.catalog_occurrence_sha256 = "
        "c.catalog_occurrence_sha256 "
        "JOIN catalog_publication_candidates AS reserved "
        "ON reserved.reserved_revision = occurrence.revision "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = reserved.candidate_id",
        extra_predicate=uncommitted,
    )

    def projection(table: str, primary_key: tuple[str, ...]) -> _StaticDeleteSpec:
        return _plan._indirect_spec(
            table,
            primary_key,
            f"{table} AS c "
            "JOIN catalog_publication_candidates AS reserved "
            "ON reserved.reserved_revision = c.revision "
            "JOIN catalog_publication_candidates AS r "
            "ON r.candidate_id = reserved.candidate_id",
            extra_predicate=uncommitted,
        )

    return {
        "PC_SEALS": (
            projection(
                "catalog_tag_directory_order",
                ("revision", "namespace", "position"),
            ),
            direct(
                "catalog_prepared_pages",
                ("candidate_id", "publication_key", "page_index"),
            ),
            direct(
                "catalog_prepared_thumbnails",
                ("candidate_id", "publication_key"),
            ),
            direct(
                "catalog_prepared_storage_objects",
                ("candidate_id", "publication_key", "resource_kind"),
            ),
            direct(
                "catalog_prepared_resource_blob",
                ("candidate_id", "publication_key", "resource_kind"),
            ),
            projection(
                "catalog_title_search_postings",
                ("revision", "value_sha256", "publication_key"),
            ),
            projection(
                "catalog_search_postings",
                ("revision", "value_sha256", "publication_key"),
            ),
            projection(
                "catalog_pages",
                ("revision", "publication_key", "page_index"),
            ),
            projection(
                "catalog_thumbnails",
                ("revision", "publication_key"),
            ),
            projection(
                "catalog_storage_objects",
                ("revision", "publication_key", "resource_kind"),
            ),
            direct("operational_publication_candidate_preparations", ("candidate_id",)),
            direct(
                "catalog_publication_candidate_projection_seals",
                ("candidate_id",),
            ),
            direct(
                "catalog_publication_batch_receipt_stored",
                ("candidate_id", "stage", "start_generation"),
            ),
            direct("catalog_artifact_operations", ("candidate_id", "publication_key")),
            projection_storage,
            projection_download_time,
            projection_upload_time,
        ),
        "PC_PREPARED": (
            prepared,
            direct(
                "catalog_prepared_artifact_descriptors",
                ("candidate_id", "publication_key"),
            ),
            projection(
                "catalog_search_documents",
                ("revision", "publication_key"),
            ),
            projection(
                "catalog_contributors",
                (
                    "revision",
                    "publication_key",
                    "contributor_name_sha256",
                    "role",
                ),
            ),
        ),
        "PC_INPUT": (
            direct(
                "catalog_candidate_artifact_inputs",
                ("candidate_id", "publication_key"),
            ),
        ),
        "PC_CHECKPOINT": (
            direct("catalog_publication_checkpoints", ("candidate_id", "stage")),
        ),
        "PC_SELECTION_STORAGE": (
            selection_storage,
            projection(
                "catalog_tag_publication_order",
                ("revision", "tag_id", "position"),
            ),
            projection("catalog_publication_order", ("revision", "position")),
        ),
        "PC_CONTENT": (
            projection("catalog_publication_contents", ("revision", "publication_key")),
        ),
        "PC_SUBJECT": (
            projection(
                "catalog_subjects",
                ("revision", "publication_key", "position"),
            ),
        ),
        "PC_BASES": (
            direct(
                "catalog_publication_candidate_base_publication_commits",
                ("candidate_id",),
            ),
            projection("catalog_artifacts", ("revision", "publication_key")),
        ),
        "PC_SELECTION_IDENTITY": (
            direct(
                "catalog_publication_selection_occurrence_identities",
                ("candidate_id", "publication_key"),
            ),
            projection(
                "catalog_publication_occurrence_identities",
                ("revision", "publication_key"),
            ),
        ),
        "PC_ROOT": (direct(root, key),),
    }


_OPERATIONAL_PREPARATION_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM operational_publication_candidate_preparations bound
    WHERE bound.preparation_id = r.preparation_id)
AND r.state = 'ABANDONED'
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.preparation_id = r.preparation_id)
"""


def _operational_preparation_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "operational_operational_preparations"
    key = ("preparation_id",)

    def direct(
        table: str, pk: tuple[str, ...], extra: str = "1 = 1"
    ) -> _StaticDeleteSpec:
        return _plan._owned_spec(table, pk, root, key, extra_predicate=extra)

    abandoned = "r.state = 'ABANDONED'"
    subtype_source = (
        "{table} AS c JOIN operational_operational_events AS event "
        "ON event.event_id = c.event_id "
        "JOIN operational_operational_preparations AS r "
        "ON r.preparation_id = event.preparation_id"
    )
    abandoned_root = _plan._owned_spec(
        root,
        key,
        root,
        key,
        extra_predicate=abandoned,
        delete_sql=(
            "DELETE FROM operational_operational_preparations "
            "WHERE preparation_id = %s",
            "DELETE FROM operational_operational_event_streams "
            "WHERE preparation_id = %s",
        ),
    )
    return {
        "OP_BATCH": (
            direct(
                "operational_operational_preparation_batch_receipts",
                ("preparation_id", "phase", "batch_key"),
            ),
        ),
        "OP_CHECKPOINT": (
            direct(
                "operational_operational_preparation_checkpoints",
                ("preparation_id", "phase"),
            ),
        ),
        "OP_SUBTYPE": (
            _plan._indirect_spec(
                "operational_operational_removed_gid_events",
                ("event_id",),
                subtype_source.format(
                    table="operational_operational_removed_gid_events"
                ),
                extra_predicate=abandoned,
            ),
            _plan._indirect_spec(
                "operational_operational_deletion_consumption_events",
                ("event_id",),
                subtype_source.format(
                    table="operational_operational_deletion_consumption_events"
                ),
                extra_predicate=abandoned,
            ),
        ),
        "OP_EVENT": (
            direct("operational_operational_events", ("event_id",), abandoned),
        ),
        "OP_SEAL": (
            direct(
                "operational_operational_preparation_effect_seals",
                ("preparation_id",),
                abandoned,
            ),
        ),
        "OP_ROOT": (abandoned_root,),
    }


_CATALOG_PUBLICATION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.CATALOG_PUBLICATION,
    "catalog_publication_occurrence_identities",
    ("revision", "publication_key"),
    "publication_key",
    32,
    _CATALOG_PUBLICATION_ELIGIBILITY,
    _catalog_publication_phases(),
)

_CATALOG_REVISION_DESCRIPTOR_PLAN = _StaticTargetPlan(
    CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR,
    "catalog_revision_descriptors",
    ("revision",),
    "revision",
    None,
    _CATALOG_REVISION_DESCRIPTOR_ELIGIBILITY,
    _catalog_revision_descriptor_phases(),
)

_SOURCE_REVISION_DESCRIPTOR_PLAN = _StaticTargetPlan(
    CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR,
    "catalog_source_revision_descriptors",
    ("source_revision",),
    "source_revision",
    None,
    _SOURCE_REVISION_DESCRIPTOR_ELIGIBILITY,
    _source_revision_descriptor_phases(),
)

_PUBLICATION_GENERATION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.PUBLICATION_GENERATION,
    "catalog_publication_generation_nodes",
    ("generation",),
    "generation",
    None,
    _PUBLICATION_GENERATION_ELIGIBILITY,
    _publication_generation_phases(),
    contiguous_integer_prefix=True,
)

_PUBLICATION_CANDIDATE_PLAN = _StaticTargetPlan(
    CleanupTargetKind.PUBLICATION_CANDIDATE,
    "catalog_publication_candidates",
    ("candidate_id",),
    "candidate_id",
    16,
    _PUBLICATION_CANDIDATE_ELIGIBILITY,
    _publication_candidate_phases(),
)

_OPERATIONAL_PREPARATION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.OPERATIONAL_PREPARATION,
    "operational_operational_preparations",
    ("preparation_id",),
    "preparation_id",
    16,
    _OPERATIONAL_PREPARATION_ELIGIBILITY,
    _operational_preparation_phases(),
)
