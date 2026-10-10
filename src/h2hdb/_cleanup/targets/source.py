"""Source collection and build retention, child-first phases, and retirement."""

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


def _source_collection_mutator(phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        plan = _SOURCE_COLLECTION_PLAN
        if operation.cycle.target_kind is not plan.kind:
            raise CleanupCorruptionError("source-collection cleanup kind drifted")
        if phase != "SC_ROOT":
            return _static._run_static_phase(operation, cursor, plan, phase)
        return _static._run_static_phase(
            operation,
            cursor,
            plan,
            phase,
            eligibility=_SOURCE_COLLECTION_AFTER_STATE,
            policy_parameters=(operation.cycle.cleanup_id,),
        )

    return mutate


def _source_build_mutator(phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        cycle = operation.cycle
        if cycle.target_kind is not CleanupTargetKind.SOURCE_BUILD:
            raise CleanupCorruptionError("source-build cleanup kind drifted")
        plan = _SOURCE_BUILD_PLAN
        if phase != "SB_ROOT":
            return _static._run_static_phase(operation, cursor, plan, phase)
        return _static._run_static_phase(
            operation,
            cursor,
            plan,
            phase,
            eligibility=_SOURCE_BUILD_AFTER_STATE_ELIGIBILITY,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


_SOURCE_COLLECTION_REACHABILITY = """
NOT EXISTS (SELECT 1 FROM operational_source_working_collections working
    WHERE working.collection_id = r.collection_id)
AND NOT EXISTS (SELECT 1 FROM operational_gallery_staging_collections staging
    WHERE staging.collection_id = r.collection_id)
"""

_SOURCE_COLLECTION_ELIGIBILITY = (
    """
EXISTS (SELECT 1 FROM operational_source_collection_states state
    WHERE state.collection_id = r.collection_id AND state.state IN ('CONSUMED', 'ABANDONED'))
AND """
    + _SOURCE_COLLECTION_REACHABILITY
)

_SOURCE_COLLECTION_AFTER_STATE = (
    """
NOT EXISTS (SELECT 1 FROM operational_source_collection_states state
    WHERE state.collection_id = r.collection_id)
AND EXISTS (SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s AND completed.phase = 'SC_STATE' AND completed.state = 'COMPLETE')
AND """
    + _SOURCE_COLLECTION_REACHABILITY
)

_SOURCE_BUILD_TERMINAL_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM catalog_source_build_states terminal
    WHERE terminal.build_id = r.build_id
      AND terminal.state IN ('SEALED', 'ABANDONED'))
"""

_SOURCE_BUILD_REACHABILITY_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_descriptor x WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits x
    WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_source_working_builds x
    WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_operational_preparations x
    WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_gallery_staging_source_builds x
    WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_collection_consumptions x
    WHERE x.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_source_build_generations older
    JOIN catalog_analysis_run_descriptor retired_build
      ON retired_build.build_id = older.build_id
    JOIN catalog_analysis_run_states retired
      ON retired.analysis_id = retired_build.analysis_id
    WHERE older.build_id <> r.build_id
      AND retired.state = 'ABANDONED'
      AND NOT EXISTS (
          SELECT 1 FROM catalog_analysis_run_descriptor sibling
          WHERE sibling.build_id = retired_build.build_id
            AND sibling.analysis_id <> retired.analysis_id)
      AND older.generation < (
          SELECT MIN(current.generation)
          FROM operational_source_build_generations current
          WHERE current.build_id = r.build_id))
AND NOT EXISTS (
    SELECT 1 FROM operational_source_build_generations m
    LEFT JOIN operational_ingest_generations g ON g.generation = m.generation
    WHERE m.build_id = r.build_id
      AND g.completed_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM operational_ingest_coordination_heads h
          WHERE m.generation < h.current_generation))
AND NOT EXISTS (
    SELECT 1 FROM operational_source_build_generations m
    JOIN operational_ingest_generation_owners o ON o.generation = m.generation
    WHERE m.build_id = r.build_id)
"""

_SOURCE_BUILD_ELIGIBILITY = (
    _SOURCE_BUILD_TERMINAL_ELIGIBILITY
    + "\nAND "
    + _SOURCE_BUILD_REACHABILITY_ELIGIBILITY
)

_SOURCE_BUILD_AFTER_STATE_ELIGIBILITY = (
    """
NOT EXISTS (
    SELECT 1 FROM catalog_source_build_states state
    WHERE state.build_id = r.build_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'SB_STATE'
      AND completed.state = 'COMPLETE')
AND """
    + _SOURCE_BUILD_REACHABILITY_ELIGIBILITY
)


def _source_collection_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_collections"
    key = ("collection_id",)

    def direct(table: str, pk: tuple[str, ...] = key) -> _StaticDeleteSpec:
        return _plan._owned_spec(table, pk, root, key)

    return {
        "SC_MEMBERS": (
            direct("catalog_source_collection_consumptions"),
            direct(
                "catalog_source_collection_observations",
                ("collection_id", "gallery_id", "observation_id"),
            ),
        ),
        "SC_CLAIM": (direct("operational_source_collection_claims"),),
        "SC_METADATA": (
            direct("catalog_source_collection_created_ats"),
            direct("catalog_source_collection_qualification_policies"),
            direct("catalog_source_collection_manifest_policies"),
        ),
        "SC_STATE": (direct("operational_source_collection_states"),),
        "SC_ROOT": (direct(root),),
    }


def _source_build_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_build_descriptor"
    key = ("build_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _plan._owned_spec(table, pk, root, key)

    upload = _plan._indirect_spec(
        "operational_canonical_value_uploads",
        ("generation", "value_sha256"),
        "operational_canonical_value_uploads AS c "
        "JOIN operational_source_build_generations AS m "
        "ON m.generation = c.generation "
        "JOIN catalog_source_build_descriptor AS r ON r.build_id = m.build_id",
    )
    return {
        "SB_CANONICAL_UPLOAD": (
            direct(
                "operational_source_build_discovery_batch_receipts",
                ("build_id", "batch_key"),
            ),
            direct(
                "operational_source_build_assembly_batch_receipts",
                ("build_id", "batch_key"),
            ),
            upload,
        ),
        "SB_GALLERY": (
            direct("catalog_source_build_sealed_ats", ("build_id",)),
            direct("operational_source_build_discovery_checkpoints", ("build_id",)),
            direct("operational_source_build_assembly_checkpoints", ("build_id",)),
            direct("catalog_build_manifest_core", ("build_id",)),
            direct("catalog_source_build_galleries", ("build_id", "gallery_id")),
        ),
        "SB_DISCOVERY": (direct("catalog_source_build_discoveries", ("build_id",)),),
        "SB_SATELLITES": (
            direct("catalog_source_build_expected_gallery", ("build_id", "position")),
            direct("catalog_source_build_base_publication_commits", ("build_id",)),
            direct("catalog_source_build_channel", ("build_id",)),
        ),
        "SB_GENERATION": (
            direct("operational_source_build_generations", ("generation",)),
        ),
        "SB_STATE": (direct("catalog_source_build_states", key),),
        "SB_ROOT": (direct(root, key),),
    }


_SOURCE_COLLECTION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.SOURCE_COLLECTION,
    "catalog_source_collections",
    ("collection_id",),
    "collection_id",
    16,
    _SOURCE_COLLECTION_ELIGIBILITY,
    _source_collection_phases(),
)

_SOURCE_BUILD_PLAN = _StaticTargetPlan(
    CleanupTargetKind.SOURCE_BUILD,
    "catalog_source_build_descriptor",
    ("build_id",),
    "build_id",
    16,
    _SOURCE_BUILD_ELIGIBILITY,
    _source_build_phases(),
)
