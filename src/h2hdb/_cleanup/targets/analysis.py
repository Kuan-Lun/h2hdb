"""Analysis-run retention and child-first retirement."""

from __future__ import annotations

from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup import static as _static
from h2hdb._cleanup.model import (
    CleanupCorruptionError,
    CleanupTargetKind,
    _CleanupOperation,
    _Mutation,
    _Mutator,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan


def _analysis_run_mutator(phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        cycle = operation.cycle
        if cycle.target_kind is not CleanupTargetKind.ANALYSIS_RUN:
            raise CleanupCorruptionError("analysis-run cleanup kind drifted")
        plan = _ANALYSIS_RUN_PLAN
        if phase != "AR_ROOT":
            return _static._run_static_phase(operation, cursor, plan, phase)
        return _static._run_static_phase(
            operation,
            cursor,
            plan,
            phase,
            eligibility=_ANALYSIS_RUN_AFTER_STATE_ELIGIBILITY,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


_ANALYSIS_RUN_TERMINAL_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM catalog_analysis_run_states terminal
    WHERE terminal.analysis_id = r.analysis_id
      AND terminal.state IN ('COMPLETE', 'ABANDONED'))
"""

_ANALYSIS_RUN_REACHABILITY_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_descriptor member
    JOIN catalog_analysis_run_descriptor retired_member
      ON retired_member.build_id = member.build_id
    JOIN catalog_analysis_run_states retired
      ON retired.analysis_id = retired_member.analysis_id
    WHERE member.analysis_id = r.analysis_id
      AND retired.state = 'ABANDONED'
      AND EXISTS (
          SELECT 1 FROM catalog_analysis_run_descriptor sibling
          WHERE sibling.build_id = member.build_id
            AND sibling.analysis_id <> retired.analysis_id))
AND NOT EXISTS (
    SELECT 1
    FROM catalog_analysis_run_states retired
    JOIN catalog_analysis_run_descriptor build
      ON build.analysis_id = retired.analysis_id
    JOIN operational_source_build_generations mapped
      ON mapped.build_id = build.build_id
    WHERE retired.analysis_id = r.analysis_id
      AND retired.state = 'ABANDONED'
      AND NOT EXISTS (
          SELECT 1 FROM operational_source_build_generations newer
          WHERE newer.generation > mapped.generation)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_analysis_run_descriptor sibling
          WHERE sibling.build_id = build.build_id
            AND sibling.analysis_id <> retired.analysis_id))
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_descriptor build
    JOIN operational_source_working_builds working
      ON working.build_id = build.build_id
    WHERE build.analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidates x
    WHERE x.analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_baselines x
    WHERE x.base_analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_state_ancestry x
    WHERE x.ancestor_analysis_id = r.analysis_id
      AND x.analysis_id <> r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_head_receipts h
    JOIN catalog_publication_commits committed
      ON committed.receipt_id = h.receipt_id
    JOIN catalog_source_revision_provenance p
      ON p.source_revision = committed.source_revision
    WHERE p.analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_revision_provenance p
    JOIN catalog_publication_commits committed
      ON committed.source_revision = p.source_revision
    JOIN catalog_source_build_base_publication_commits base
      ON base.base_receipt_id = committed.receipt_id
    WHERE p.analysis_id = r.analysis_id)
"""

_ANALYSIS_RUN_ELIGIBILITY = (
    _ANALYSIS_RUN_TERMINAL_ELIGIBILITY
    + "\nAND "
    + _ANALYSIS_RUN_REACHABILITY_ELIGIBILITY
)

_ANALYSIS_RUN_AFTER_STATE_ELIGIBILITY = (
    """
NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_states state
    WHERE state.analysis_id = r.analysis_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'AR_STATE'
      AND completed.state = 'COMPLETE')
AND """
    + _ANALYSIS_RUN_REACHABILITY_ELIGIBILITY
)


def _analysis_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_analysis_run_descriptor"
    key = ("analysis_id",)

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

    return {
        "AR_BATCH": (
            direct(
                "catalog_analysis_batch_receipt_stored",
                ("analysis_id", "stage", "start_generation"),
            ),
        ),
        "AR_COMPONENT": (
            direct(
                "catalog_analysis_state_component_seals",
                ("analysis_id", "state_component"),
            ),
        ),
        "AR_OVERLAY": tuple(
            direct(
                table,
                pk,
                batch_exact_primary_keys=(
                    table == "catalog_a_file_decision_shadow_seals"
                ),
            )
            for table, pk in (
                (
                    "catalog_a_file_decision_shadow_seals",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_analysis_content_owner_candidate_shadows",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_analysis_content_owner_shadows",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_analysis_impacted_content",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_analysis_impacted_gid_storage",
                    ("analysis_id", "gid"),
                ),
                (
                    "catalog_analysis_file_hash_decision_tombstone",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_analysis_content_owner_candidate_tombstones",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_analysis_content_owner_tombstones",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_analysis_gid_candidate_shadows",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_analysis_gid_candidate_tombstones",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_analysis_gid_winner_selections",
                    ("analysis_id", "winner_gallery_id"),
                ),
                ("catalog_analysis_gid_winner_tombstones", ("analysis_id", "gid")),
            )
        ),
        "AR_FILE_HASH_VALUES": tuple(
            direct(
                table,
                ("analysis_id", "file_sha256"),
                batch_exact_primary_keys=True,
            )
            for table in (
                "catalog_a_file_decision_shadow_occurrences",
                "catalog_a_file_decision_shadow_artists",
                "catalog_a_file_decision_shadow_gallery_artist_max",
            )
        ),
        "AR_IMPACT_PROVENANCE": tuple(
            direct(table, pk)
            for table, pk in (
                (
                    "catalog_a_impacted_content_provenance",
                    ("analysis_id", "gallery_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_gid_provenance_storage",
                    ("analysis_id", "gallery_id"),
                ),
            )
        ),
        "AR_FILE_HASH_ANCHOR": (
            direct(
                "catalog_a_file_decision_shadow_anchors",
                ("analysis_id", "file_sha256"),
                batch_exact_primary_keys=True,
            ),
        ),
        "AR_EVIDENCE": tuple(
            direct(
                table,
                pk,
                batch_exact_primary_keys=(
                    table
                    in {
                        "catalog_analysis_exclusion_delta_seals",
                        "catalog_analysis_changed_file_hashes",
                    }
                ),
            )
            for table, pk in (
                (
                    "catalog_analysis_exclusion_delta_changes",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_analysis_exclusion_delta_seals",
                    ("analysis_id", "file_sha256"),
                ),
                ("catalog_analysis_changed_galleries", ("analysis_id", "gallery_id")),
                (
                    "catalog_analysis_changed_file_hashes",
                    ("analysis_id", "file_sha256"),
                ),
                ("catalog_analysis_impacted_galleries", ("analysis_id", "gallery_id")),
            )
        ),
        "AR_EXCLUSION_VALUES": (
            direct(
                "catalog_analysis_exclusion_delta_old_excluded_flags",
                ("analysis_id", "file_sha256"),
                batch_exact_primary_keys=True,
            ),
            direct(
                "catalog_analysis_exclusion_delta_new_excluded_flags",
                ("analysis_id", "file_sha256"),
                batch_exact_primary_keys=True,
            ),
        ),
        "AR_EXCLUSION_ANCHOR": (
            direct(
                "catalog_analysis_exclusion_delta_anchors",
                ("analysis_id", "file_sha256"),
                batch_exact_primary_keys=True,
            ),
        ),
        "AR_CHECKPOINT": (
            direct("catalog_analysis_checkpoints", ("analysis_id", "stage")),
        ),
        "AR_ANCESTRY": (
            direct(
                "catalog_analysis_state_ancestry",
                ("analysis_id", "ancestor_depth"),
            ),
        ),
        "AR_BASELINE": (direct("catalog_analysis_baselines", ("analysis_id",)),),
        "AR_BINDINGS": (
            direct("catalog_source_revision_provenance", ("source_revision",)),
            direct("catalog_analysis_snapshot_manifest", ("analysis_id",)),
        ),
        "AR_COMPLETION": (direct("catalog_analysis_run_completed_ats", key),),
        "AR_STATE": (direct("catalog_analysis_run_states", key),),
        "AR_ROOT": (direct(root, key),),
    }


_ANALYSIS_RUN_PLAN = _StaticTargetPlan(
    CleanupTargetKind.ANALYSIS_RUN,
    "catalog_analysis_run_descriptor",
    ("analysis_id",),
    "analysis_id",
    16,
    _ANALYSIS_RUN_ELIGIBILITY,
    _analysis_phases(),
)
