"""Canonical value and upload retention with child-first cleanup plans."""

from __future__ import annotations

from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup.model import CleanupTargetKind
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan

_CANONICAL_UPLOAD_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM operational_ingest_coordination_heads head
    WHERE r.generation <> head.current_generation
      AND (r.generation < head.current_generation OR EXISTS (
          SELECT 1 FROM operational_ingest_generations history
          WHERE history.generation = r.generation
            AND history.completed_at IS NOT NULL)))
AND NOT EXISTS (
    SELECT 1 FROM operational_ingest_generation_owners owner
    WHERE owner.generation = r.generation AND owner.lease_expires_at > %s)
AND EXISTS (
    SELECT 1 FROM catalog_canonical_value_allocation_seals allocation
    JOIN catalog_canonical_value_allocation_digest_domains domain
      ON domain.value_sha256 = allocation.value_sha256
    WHERE allocation.value_sha256 = r.value_sha256
      AND (domain.digest_domain = X'736F757263655F726F6F745F7631' OR EXISTS (
          SELECT 1 FROM operational_source_build_generations mapped
          WHERE mapped.generation = r.generation)))
"""


def _canonical_upload_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "operational_canonical_value_uploads"
    key = ("generation", "value_sha256")
    return {"CVU_ROOT": (_plan._owned_spec(root, key, root, key),)}


def _live_display_title_choice(alias: str) -> str:
    child = _plan._identifier(alias)
    return f"""
    EXISTS (
        SELECT 1
        FROM catalog_publication_commit_head_receipts head
        JOIN catalog_publication_commits current_revision
          ON current_revision.receipt_id = head.receipt_id
        JOIN catalog_publication_titles current_title
          ON current_title.revision = current_revision.revision
        WHERE current_revision.display_title_policy_id = {child}.display_title_policy_id
          AND current_title.source_title_sha256 = {child}.source_title_sha256
          AND current_title.source_gallery_name = {child}.source_gallery_name)
    OR EXISTS (
        SELECT 1
        FROM catalog_publication_candidates candidate_revision
        JOIN catalog_publication_titles candidate_title
          ON candidate_title.revision = candidate_revision.reserved_revision
        WHERE candidate_revision.display_title_policy_id = {child}.display_title_policy_id
          AND candidate_title.source_title_sha256 = {child}.source_title_sha256
          AND candidate_title.source_gallery_name = {child}.source_gallery_name
          AND (
            EXISTS (
                SELECT 1 FROM operational_catalog_working_candidates working
                WHERE working.candidate_id = candidate_revision.candidate_id)
            OR NOT EXISTS (
                SELECT 1 FROM catalog_publication_commits committed
                WHERE committed.candidate_id = candidate_revision.candidate_id)))
    """


def _live_title_sort(alias: str) -> str:
    child = _plan._identifier(alias)
    # Start from the reverse title-digest index. Joining policy first can scan
    # every choice under a shared policy for every canonical root being tested.
    # The same choice must satisfy both its policy and publication liveness.
    return f"""
    EXISTS (
        SELECT 1
        FROM catalog_display_title_choices choice
        WHERE choice.title_sha256 = {child}.title_sha256
          AND EXISTS (
              SELECT 1 FROM catalog_display_title_policies policy
              WHERE policy.display_title_policy_id = choice.display_title_policy_id
                AND policy.title_sort_policy_id = {child}.title_sort_policy_id)
          AND ({_live_display_title_choice("choice")}))
    """


_CANONICAL_VALUE_ELIGIBILITY = f"""
NOT EXISTS (SELECT 1 FROM operational_canonical_value_uploads x
            WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM operational_hash_cache_observations x
                WHERE x.source_identity_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM operational_hash_cache_observations x
                WHERE x.fingerprint_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_source_build_descriptor build
      ON build.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_source_collections collection ON collection.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_gallery_identities gallery
      ON gallery.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_identities x
                WHERE x.locator_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observations x
                WHERE x.observation_identity_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_tags x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_artists x
    JOIN catalog_tag_terms term ON term.tag_id = x.artist_tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_subjects x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_head_revisions current_source
    JOIN catalog_source_revision_descriptors manifest
      ON manifest.source_revision = current_source.source_revision
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM operational_source_working_builds working
    JOIN catalog_analysis_run_descriptor live_analysis
      ON live_analysis.build_id = working.build_id
    JOIN catalog_analysis_snapshot_manifest manifest
      ON manifest.analysis_id = live_analysis.analysis_id
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidates live_analysis
    JOIN catalog_analysis_snapshot_manifest manifest
      ON manifest.analysis_id = live_analysis.analysis_id
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256
      AND (
        EXISTS (
            SELECT 1 FROM operational_catalog_working_candidates working
            WHERE working.candidate_id = live_analysis.candidate_id)
        OR NOT EXISTS (
            SELECT 1 FROM catalog_publication_commits committed
            WHERE committed.candidate_id = live_analysis.candidate_id)))
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_provenance x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_content x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_candidate_shadows x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_shadows x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_tombstones x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidates x
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = x.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = committed.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.source_manifest_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.member_plan_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.effective_content_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.selected_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.owner_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_inputs x
                WHERE x.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_candidate_artifact_inputs x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifacts x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_search_postings x
                WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_language_facet_order x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributor_facet_order x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_tag_directory_order x
                WHERE x.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_tag_publication_order x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_subject_facet_order x
    JOIN catalog_tag_terms term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_contents x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributors x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.summary_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.source_title_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_display_title_choices choice
    WHERE choice.source_title_sha256 = r.value_sha256
      AND ({_live_display_title_choice("choice")}))
AND NOT EXISTS (
    SELECT 1 FROM catalog_display_title_choices choice
    WHERE choice.title_sha256 = r.value_sha256
      AND ({_live_display_title_choice("choice")}))
AND NOT EXISTS (
    SELECT 1 FROM catalog_title_sorts title_sort
    WHERE title_sort.title_sha256 = r.value_sha256
      AND ({_live_title_sort("title_sort")}))
AND NOT EXISTS (
    SELECT 1 FROM catalog_title_sorts title_sort
    WHERE title_sort.sort_title_sha256 = r.value_sha256
      AND ({_live_title_sort("title_sort")}))
"""


def _canonical_value_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_canonical_value_allocation_anchors"
    key = ("value_sha256",)
    display_title_choice = _plan._indirect_spec(
        "catalog_display_title_choices",
        (
            "display_title_policy_id",
            "source_title_sha256",
            "source_gallery_name",
        ),
        "catalog_display_title_choices AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.source_title_sha256 "
        "OR r.value_sha256 = c.title_sha256",
        extra_predicate=f"NOT ({_live_display_title_choice('c')})",
        canonical_dictionary_columns=("source_title_sha256", "title_sha256"),
    )
    title_sort = _plan._indirect_spec(
        "catalog_title_sorts",
        ("title_sort_policy_id", "title_sha256"),
        "catalog_title_sorts AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.title_sha256 "
        "OR r.value_sha256 = c.sort_title_sha256",
        extra_predicate=f"NOT ({_live_title_sort('c')})",
        canonical_dictionary_columns=("title_sha256", "sort_title_sha256"),
    )
    source_scope = (
        _plan._indirect_spec(
            "catalog_source_scopes",
            ("scope_key",),
            "catalog_source_scopes AS c "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = c.source_root_sha256",
        ),
    )
    locator = _plan._indirect_spec(
        "catalog_source_locator_identity",
        ("locator_sha256",),
        "catalog_source_locator_identity AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.locator_sha256",
    )
    tag = _plan._indirect_spec(
        "catalog_tag_terms",
        ("tag_id",),
        "catalog_tag_terms AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.tag_value_sha256",
    )
    snapshot = _plan._indirect_spec(
        "catalog_source_snapshot_manifest_identity",
        ("snapshot_manifest_sha256",),
        "catalog_source_snapshot_manifest_identity AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.snapshot_manifest_sha256",
    )
    policy = _plan._indirect_spec(
        "catalog_artifact_policies",
        ("artifact_policy_id",),
        "catalog_artifact_policies AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.policy_component_sha256",
    )
    semantic = _plan._indirect_spec(
        "catalog_artifact_semantic_inputs",
        ("artifact_semantics_sha256",),
        "catalog_artifact_semantic_inputs AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.artifact_semantics_sha256",
        delete_sql=(
            "DELETE FROM catalog_artifact_semantic_inputs "
            "WHERE artifact_semantics_sha256 = %s",
        ),
    )
    policy_semantics = (
        _plan._indirect_spec(
            "catalog_artifact_policy_semantics",
            ("policy_component_sha256",),
            "catalog_artifact_policy_semantics AS c "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = c.policy_component_sha256",
        ),
    )
    search_lexeme = _plan._indirect_spec(
        "catalog_search_lexemes",
        ("value_sha256",),
        "catalog_search_lexemes AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.value_sha256",
    )
    parent = _plan._indirect_spec(
        "catalog_canonical_value_page_parents",
        ("parent_sha256", "position"),
        "catalog_canonical_value_page_parents AS c "
        "JOIN catalog_canonical_value_page_coordinates AS owned "
        "ON owned.page_sha256 = c.child_sha256 "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = owned.value_sha256",
    )
    page_seal = _plan._indirect_spec(
        "catalog_canonical_value_page_seals",
        ("page_sha256",),
        "catalog_canonical_value_page_seals AS c "
        "JOIN catalog_canonical_value_page_coordinates AS owned "
        "ON owned.page_sha256 = c.page_sha256 "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = owned.value_sha256",
    )
    page_family = _plan._indirect_spec(
        "catalog_canonical_value_page_coordinates",
        ("value_sha256", "level", "page_position", "page_sha256"),
        "catalog_canonical_value_page_coordinates AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.value_sha256",
        delete_sql=(
            "DELETE FROM catalog_canonical_value_page_coordinates "
            "WHERE value_sha256 = %s AND level = %s AND page_position = %s "
            "AND page_sha256 = %s",
            "DELETE FROM catalog_canonical_value_page_payloads WHERE page_sha256 = %s",
            "DELETE FROM catalog_canonical_value_page_subtree_item_counts "
            "WHERE page_sha256 = %s",
            "DELETE FROM catalog_canonical_value_page_anchors WHERE page_sha256 = %s",
        ),
        delete_parameter_indexes=((0, 1, 2, 3), (3,), (3,), (3,)),
        delete_allowed_affected=(
            frozenset((1,)),
            frozenset((0, 1)),
            frozenset((0, 1)),
            frozenset((0, 1)),
        ),
        batch_exact_primary_keys=True,
        batch_delete_keys=(
            (
                "catalog_canonical_value_page_coordinates",
                ("value_sha256", "level", "page_position", "page_sha256"),
            ),
            ("catalog_canonical_value_page_payloads", ("page_sha256",)),
            ("catalog_canonical_value_page_subtree_item_counts", ("page_sha256",)),
            ("catalog_canonical_value_page_anchors", ("page_sha256",)),
        ),
    )
    return {
        "CV_DICTIONARY": (
            search_lexeme,
            display_title_choice,
            title_sort,
            *source_scope,
            locator,
            tag,
            snapshot,
            policy,
            semantic,
        ),
        "CV_SEMANTIC_LINK": policy_semantics,
        "CV_IDENTITY": (
            _plan._owned_spec(
                "catalog_canonical_value_identities",
                ("value_sha256",),
                root,
                key,
            ),
        ),
        "CV_PARENT_DESCRIPTOR": (
            parent,
            page_seal,
        ),
        "CV_PAGE": (
            page_family,
            _plan._owned_spec(
                "catalog_canonical_value_allocation_seals",
                ("value_sha256",),
                root,
                key,
                batch_exact_primary_keys=True,
            ),
            _plan._owned_spec(
                "catalog_canonical_value_allocation_allocated_ats",
                ("value_sha256",),
                root,
                key,
                batch_exact_primary_keys=True,
            ),
            _plan._owned_spec(
                "catalog_canonical_value_allocation_byte_counts",
                ("value_sha256",),
                root,
                key,
                batch_exact_primary_keys=True,
            ),
            _plan._owned_spec(
                "catalog_canonical_value_allocation_digest_domains",
                ("value_sha256",),
                root,
                key,
                batch_exact_primary_keys=True,
            ),
        ),
        "CV_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


_CANONICAL_VALUE_PLAN = _StaticTargetPlan(
    CleanupTargetKind.CANONICAL_VALUE,
    "catalog_canonical_value_allocation_anchors",
    ("value_sha256",),
    "value_sha256",
    32,
    _CANONICAL_VALUE_ELIGIBILITY,
    _canonical_value_phases(),
)

_CANONICAL_VALUE_UPLOAD_PLAN = _StaticTargetPlan(
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
    "operational_canonical_value_uploads",
    ("generation", "value_sha256"),
    "value_sha256",
    32,
    _CANONICAL_UPLOAD_ELIGIBILITY,
    _canonical_upload_phases(),
    uses_cutoff=True,
)
