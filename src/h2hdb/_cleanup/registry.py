"""Closed-world assembly of source-owned cleanup plans and phase strategies."""

from __future__ import annotations

from h2hdb._cleanup import static as _static
from h2hdb._cleanup.model import CleanupTargetKind, _Strategy
from h2hdb._cleanup.plan import _StaticTargetPlan
from h2hdb._cleanup.targets import analysis as _analysis_targets
from h2hdb._cleanup.targets import gallery as _gallery_targets
from h2hdb._cleanup.targets import publication_commit as _publication_commit_targets
from h2hdb._cleanup.targets import resources as _resources_targets
from h2hdb._cleanup.targets import source as _source_targets
from h2hdb._cleanup.targets.analysis import _ANALYSIS_RUN_PLAN
from h2hdb._cleanup.targets.canonical import (
    _CANONICAL_VALUE_PLAN,
    _CANONICAL_VALUE_UPLOAD_PLAN,
)
from h2hdb._cleanup.targets.gallery import (
    _GALLERY_GID_IDENTITY_PLAN,
    _GALLERY_IDENTITY_PLAN,
    _GALLERY_OBSERVATION_PAGE_PLAN,
    _GALLERY_OBSERVATION_PLAN,
    _GALLERY_OBSERVATION_STAGING_PLAN,
    _SOURCE_GALLERY_NAME_GID_PLAN,
)
from h2hdb._cleanup.targets.hash_cache import _HASH_CACHE_OBSERVATION_PLAN
from h2hdb._cleanup.targets.publication import (
    _CATALOG_PUBLICATION_PLAN,
    _CATALOG_REVISION_DESCRIPTOR_PLAN,
    _OPERATIONAL_PREPARATION_PLAN,
    _PUBLICATION_CANDIDATE_PLAN,
    _PUBLICATION_GENERATION_PLAN,
    _SOURCE_REVISION_DESCRIPTOR_PLAN,
)
from h2hdb._cleanup.targets.publication_commit import _PUBLICATION_COMMIT_PLAN
from h2hdb._cleanup.targets.resources import _STORAGE_OBJECT_KEY_PLAN
from h2hdb._cleanup.targets.source import _SOURCE_BUILD_PLAN, _SOURCE_COLLECTION_PLAN

_MAINTENANCE_TARGET_PRIORITY = (
    CleanupTargetKind.CATALOG_PUBLICATION,
    CleanupTargetKind.PUBLICATION_COMMIT,
    CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR,
    CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR,
    CleanupTargetKind.PUBLICATION_GENERATION,
    CleanupTargetKind.PUBLICATION_CANDIDATE,
    CleanupTargetKind.OPERATIONAL_PREPARATION,
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
    CleanupTargetKind.ANALYSIS_RUN,
    CleanupTargetKind.SOURCE_COLLECTION,
    CleanupTargetKind.SOURCE_BUILD,
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
    CleanupTargetKind.GALLERY_OBSERVATION,
    CleanupTargetKind.ARTIFACT_BLOB,
    CleanupTargetKind.STORAGE_OBJECT_KEY,
    CleanupTargetKind.PUBLICATION_IDENTITY,
    CleanupTargetKind.GALLERY_IDENTITY,
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
    CleanupTargetKind.GALLERY_GID_IDENTITY,
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
    CleanupTargetKind.FILE_NAME_IDENTITY,
    CleanupTargetKind.HASH_CACHE_OBSERVATION,
    CleanupTargetKind.CONTENT_BLOB,
    CleanupTargetKind.CANONICAL_VALUE,
)

_CURRENT_ONLY_TARGET_PRIORITY = tuple(
    kind
    for kind in _MAINTENANCE_TARGET_PRIORITY
    if kind is not CleanupTargetKind.HASH_CACHE_OBSERVATION
)

_CURRENT_ONLY_OPEN_ORDER_SQL = (
    "CASE sweep.target_kind "
    + " ".join(
        f"WHEN '{kind.value}' THEN {position}"
        for position, kind in enumerate(_CURRENT_ONLY_TARGET_PRIORITY)
    )
    + " ELSE 999 END"
)

_STATIC_PLANS: dict[CleanupTargetKind, _StaticTargetPlan] = {
    CleanupTargetKind.SOURCE_COLLECTION: _SOURCE_COLLECTION_PLAN,
    CleanupTargetKind.SOURCE_BUILD: _SOURCE_BUILD_PLAN,
    CleanupTargetKind.ANALYSIS_RUN: _ANALYSIS_RUN_PLAN,
    CleanupTargetKind.CATALOG_PUBLICATION: _CATALOG_PUBLICATION_PLAN,
    CleanupTargetKind.PUBLICATION_COMMIT: _PUBLICATION_COMMIT_PLAN,
    CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR: _CATALOG_REVISION_DESCRIPTOR_PLAN,
    CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR: _SOURCE_REVISION_DESCRIPTOR_PLAN,
    CleanupTargetKind.PUBLICATION_GENERATION: _PUBLICATION_GENERATION_PLAN,
    CleanupTargetKind.PUBLICATION_CANDIDATE: _PUBLICATION_CANDIDATE_PLAN,
    CleanupTargetKind.STORAGE_OBJECT_KEY: _STORAGE_OBJECT_KEY_PLAN,
    CleanupTargetKind.OPERATIONAL_PREPARATION: _OPERATIONAL_PREPARATION_PLAN,
    CleanupTargetKind.GALLERY_OBSERVATION: _GALLERY_OBSERVATION_PLAN,
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING: _GALLERY_OBSERVATION_STAGING_PLAN,
    CleanupTargetKind.CANONICAL_VALUE: _CANONICAL_VALUE_PLAN,
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE: _GALLERY_OBSERVATION_PAGE_PLAN,
    CleanupTargetKind.GALLERY_IDENTITY: _GALLERY_IDENTITY_PLAN,
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID: _SOURCE_GALLERY_NAME_GID_PLAN,
    CleanupTargetKind.GALLERY_GID_IDENTITY: _GALLERY_GID_IDENTITY_PLAN,
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD: _CANONICAL_VALUE_UPLOAD_PLAN,
    CleanupTargetKind.HASH_CACHE_OBSERVATION: _HASH_CACHE_OBSERVATION_PLAN,
}


def _static_strategy(kind: CleanupTargetKind) -> _Strategy:
    phases = tuple(_STATIC_PLANS[kind].phases)
    return _Strategy(
        phases,
        tuple(_static._static_mutator(_STATIC_PLANS[kind], phase) for phase in phases),
    )


_STRATEGIES: dict[CleanupTargetKind, _Strategy] = {
    CleanupTargetKind.SOURCE_COLLECTION: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.SOURCE_COLLECTION].phases),
        tuple(
            _source_targets._source_collection_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.SOURCE_COLLECTION].phases
        ),
    ),
    CleanupTargetKind.SOURCE_BUILD: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD].phases),
        tuple(
            _source_targets._source_build_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD].phases
        ),
    ),
    CleanupTargetKind.ANALYSIS_RUN: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases),
        tuple(
            _analysis_targets._analysis_run_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases
        ),
    ),
    CleanupTargetKind.CATALOG_PUBLICATION: _static_strategy(
        CleanupTargetKind.CATALOG_PUBLICATION
    ),
    CleanupTargetKind.PUBLICATION_COMMIT: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT].phases),
        tuple(
            _publication_commit_targets._publication_commit_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT].phases
        ),
    ),
    CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR: _static_strategy(
        CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR
    ),
    CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR: _static_strategy(
        CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR
    ),
    CleanupTargetKind.PUBLICATION_GENERATION: _static_strategy(
        CleanupTargetKind.PUBLICATION_GENERATION
    ),
    CleanupTargetKind.PUBLICATION_CANDIDATE: _static_strategy(
        CleanupTargetKind.PUBLICATION_CANDIDATE
    ),
    CleanupTargetKind.OPERATIONAL_PREPARATION: _static_strategy(
        CleanupTargetKind.OPERATIONAL_PREPARATION
    ),
    CleanupTargetKind.GALLERY_OBSERVATION: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.GALLERY_OBSERVATION].phases),
        tuple(
            _gallery_targets._gallery_observation_mutator(
                _STATIC_PLANS[CleanupTargetKind.GALLERY_OBSERVATION], phase
            )
            for phase in _STATIC_PLANS[CleanupTargetKind.GALLERY_OBSERVATION].phases
        ),
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.GALLERY_OBSERVATION_STAGING].phases),
        tuple(
            _gallery_targets._gallery_observation_mutator(
                _STATIC_PLANS[CleanupTargetKind.GALLERY_OBSERVATION_STAGING], phase
            )
            for phase in _STATIC_PLANS[
                CleanupTargetKind.GALLERY_OBSERVATION_STAGING
            ].phases
        ),
    ),
    CleanupTargetKind.ARTIFACT_BLOB: _Strategy(
        ("AB_ROOT",),
        (_resources_targets._select_artifact_blobs,),
    ),
    CleanupTargetKind.STORAGE_OBJECT_KEY: _static_strategy(
        CleanupTargetKind.STORAGE_OBJECT_KEY
    ),
    CleanupTargetKind.CANONICAL_VALUE: _static_strategy(
        CleanupTargetKind.CANONICAL_VALUE
    ),
    CleanupTargetKind.CONTENT_BLOB: _Strategy(
        ("CB_ROOT",), (_resources_targets._select_content_blobs,)
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE: _static_strategy(
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE
    ),
    CleanupTargetKind.FILE_NAME_IDENTITY: _Strategy(
        ("FN_ROOT",), (_resources_targets._select_file_name_identities,)
    ),
    CleanupTargetKind.PUBLICATION_IDENTITY: _Strategy(
        ("PI_ROOT",), (_resources_targets._select_publication_identities,)
    ),
    CleanupTargetKind.GALLERY_IDENTITY: _static_strategy(
        CleanupTargetKind.GALLERY_IDENTITY
    ),
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID: _static_strategy(
        CleanupTargetKind.SOURCE_GALLERY_NAME_GID
    ),
    CleanupTargetKind.GALLERY_GID_IDENTITY: _static_strategy(
        CleanupTargetKind.GALLERY_GID_IDENTITY
    ),
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD: _static_strategy(
        CleanupTargetKind.CANONICAL_VALUE_UPLOAD
    ),
    CleanupTargetKind.HASH_CACHE_OBSERVATION: _static_strategy(
        CleanupTargetKind.HASH_CACHE_OBSERVATION
    ),
}

_ALL_PHASES = frozenset(
    phase for strategy in _STRATEGIES.values() for phase in strategy.phases
)
