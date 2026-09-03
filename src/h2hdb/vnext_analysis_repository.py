"""Fenced, bounded vNext analysis and canonical snapshot writer.

Every mutable stage is a server-keyset state machine capped at 128 keys.  The
five independently validated state components use exact shadow/tombstone
overlays over a completely materialized, depth-limited ancestry.  Comparator
facts and effective-content identities are derived only from sealed source
observations; callers can carry an opaque preparation capability but cannot
choose a digest, priority, cursor, count, or winner.

The final source-snapshot manifest is a process-private, disk-backed typed
plan.  Its unbounded payload is built and uploaded before the short handoff
transaction.  That transaction reauthorizes the live generation, rechecks the
five immutable seals and exact canonical identity, inserts the one analysis
binding, releases only that generation's upload claim, and marks the analysis
complete atomically.
"""

from __future__ import annotations

__all__ = [
    "ANALYSIS_COMPONENTS",
    "AnalysisBatchResult",
    "AnalysisCorruptionError",
    "AnalysisGalleryPreparation",
    "AnalysisNotReadyError",
    "AnalysisPreparationAuthority",
    "AnalysisRepository",
    "AnalysisRepositoryError",
    "AnalysisRun",
    "AnalysisSnapshotPreparation",
    "AnalysisStageIssue",
    "AnalysisUnsupportedError",
]

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from .sql_connector import SQLConnector
from .vnext_analysis_family import (
    AnalysisExclusionDeltaFamily,
    AnalysisFamilyCollisionError,
    cas_analysis_run_state,
    ensure_analysis_run_family,
    ensure_analysis_state_component_family,
    insert_analysis_exclusion_delta_family,
    insert_analysis_run_completed_at,
    load_analysis_exclusion_delta_families,
    load_analysis_run_family,
    load_analysis_run_family_by_identity,
    load_analysis_state_component_families,
    load_analysis_state_component_family,
)
from .vnext_analysis_overlay_family import (
    AnalysisContentOwnerCandidateShadowFamily,
    AnalysisContentOwnerShadowFamily,
    AnalysisFileHashDecisionShadowFamily,
    apply_analysis_impacted_content_provenance_page,
    apply_analysis_impacted_gid_provenance_page,
    ensure_analysis_content_owner_candidate_shadow_family,
    ensure_analysis_content_owner_shadow_family,
    ensure_analysis_file_hash_decision_shadow_family,
    load_analysis_content_owner_candidate_shadow_family,
    load_analysis_content_owner_shadow_family,
    load_analysis_file_hash_decision_shadow_family,
    prepare_analysis_impacted_content_provenance_page,
    prepare_analysis_impacted_gid_provenance_page,
    require_complete_analysis_impacted_content_keyspace,
    require_complete_analysis_impacted_gid_keyspace,
    require_exact_analysis_impacted_content_provenance_page,
    require_exact_analysis_impacted_gid_provenance_page,
)
from .vnext_canonical_value_family import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    load_sealed_value_identities,
)
from .vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from .vnext_domains import (
    INT63_MAX,
    require_ascii_bytes,
    require_bool_byte,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
    require_uuid16,
)
from .vnext_identity import (
    ANALYSIS_ALREADY_UPLOADED_MARKER,
    AnalysisTitleScalarReceipt,
    GalleryObservationBranchEntry,
    GalleryObservationComponent,
    GalleryObservationMetadataChunk,
    GalleryObservationMetadataDecoder,
    GalleryObservationNodeKind,
    SourceSnapshotContentOwner,
    SourceSnapshotCounts,
    SourceSnapshotFileHashDecision,
    SourceSnapshotGallery,
    SourceSnapshotGidWinner,
    SourceSnapshotPolicy,
    count_analysis_title_scalars,
    decode_gallery_observation_page,
    effective_content_digest_ordered,
    gallery_observation_page_digest,
    iter_effective_content_payload_ordered,
    iter_source_snapshot_manifest_payload_rows_ordered,
    source_snapshot_manifest_digest_ordered,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_manifest_family import (
    ManifestFamilyCollisionError,
    database_unix_microseconds,
    ensure_snapshot_manifest_family,
    load_build_manifest_family,
    load_snapshot_manifest_family,
    load_source_build_family,
)
from .vnext_source_build_repository import (
    SourceBuildManifestSummary,
    source_build_identity,
    source_build_recovery_identity,
    source_build_snapshot_attempt_id,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_STAGE_CHANGED_GALLERY = b"changed_gallery"
_STAGE_CHANGED_FILE_HASH = b"changed_file_hash"
_STAGE_FILE_HASH_DECISION = b"file_hash_decision"
_STAGE_VALIDATE_FILE_HASH = b"validate_file_hash_decision"
_STAGE_IMPACTED_GALLERY = b"impacted_gallery"
_STAGE_IMPACTED_CONTENT = b"impacted_content"
_STAGE_CONTENT_CANDIDATE = b"content_owner_candidate"
_STAGE_VALIDATE_CONTENT_CANDIDATE = b"validate_content_owner_candidate"
_STAGE_CONTENT_OWNER = b"content_owner"
_STAGE_VALIDATE_CONTENT_OWNER = b"validate_content_owner"
_STAGE_IMPACTED_GID = b"impacted_gid"
_STAGE_GID_CANDIDATE = b"gid_candidate"
_STAGE_VALIDATE_GID_CANDIDATE = b"validate_gid_candidate"
_STAGE_GID_WINNER = b"gid_winner"
_STAGE_VALIDATE_GID_WINNER = b"validate_gid_winner"
_STAGES = (
    _STAGE_CHANGED_GALLERY,
    _STAGE_CHANGED_FILE_HASH,
    _STAGE_FILE_HASH_DECISION,
    _STAGE_VALIDATE_FILE_HASH,
    _STAGE_IMPACTED_GALLERY,
    _STAGE_IMPACTED_CONTENT,
    _STAGE_CONTENT_CANDIDATE,
    _STAGE_VALIDATE_CONTENT_CANDIDATE,
    _STAGE_CONTENT_OWNER,
    _STAGE_VALIDATE_CONTENT_OWNER,
    _STAGE_IMPACTED_GID,
    _STAGE_GID_CANDIDATE,
    _STAGE_VALIDATE_GID_CANDIDATE,
    _STAGE_GID_WINNER,
    _STAGE_VALIDATE_GID_WINNER,
)

_COMPONENT_FILE_HASH = b"file_hash_decision"
_COMPONENT_CONTENT_CANDIDATE = b"content_owner_candidate"
_COMPONENT_CONTENT_OWNER = b"content_owner"
_COMPONENT_GID_CANDIDATE = b"gid_candidate"
_COMPONENT_GID_WINNER = b"gid_winner"
ANALYSIS_COMPONENTS = frozenset(
    {
        _COMPONENT_FILE_HASH,
        _COMPONENT_CONTENT_CANDIDATE,
        _COMPONENT_CONTENT_OWNER,
        _COMPONENT_GID_CANDIDATE,
        _COMPONENT_GID_WINNER,
    }
)

_MAX_BATCH_ROWS = 128
_MAX_OVERLAY_DEPTH = 16
_CURSOR_VERSION = 1
_CURSOR_GALLERY = b"G"
_CURSOR_DIGEST = b"D"
_CURSOR_GID = b"I"
_CHECKPOINT_OPEN = "OPEN"
_CHECKPOINT_COMPLETE = "COMPLETE"
_CHECKPOINT_TABLE = "catalog_analysis_checkpoints"
_RECEIPT_STORED_TABLE = "catalog_analysis_batch_receipt_stored"
_ANALYSIS_RUN_DESCRIPTOR_TABLE = "catalog_analysis_run_descriptor"
_ANALYSIS_RUN_VIEW = "catalog_analysis_runs"
_ANALYSIS_BASELINE_TABLE = "catalog_analysis_baselines"
_ANALYSIS_SNAPSHOT_MANIFEST_TABLE = "catalog_analysis_snapshot_manifest"
_PUBLICATION_CANDIDATE_TABLE = "catalog_publication_candidates"
_PUBLICATION_COMMIT_TABLE = "catalog_publication_commits"
_PUBLICATION_RECEIPT_VIEW = "catalog_publication_receipts"
_PUBLICATION_COMMIT_HEAD_TABLE = "catalog_publication_commit_head_receipts"
_SOURCE_REVISION_DESCRIPTOR_TABLE = "catalog_source_revision_descriptors"
_SOURCE_REVISION_PROVENANCE_TABLE = "catalog_source_revision_provenance"
_CATALOG_WORKING_CANDIDATE_TABLE = "operational_catalog_working_candidates"
_OPERATIONAL_PREPARATION_TABLE = "operational_operational_preparations"
_EFFECTIVE_CONTENT_DOMAIN = b"effective_content_v1"
_SNAPSHOT_DOMAIN = b"source_snapshot_manifest_v1"
_METADATA_PREFIX = b"h2hdb-vnext-gallery-observation-metadata\0"
_SNAPSHOT_PREFIX = b"h2hdb-vnext-source-snapshot-manifest\0"
_PREPARATION_TOKEN = object()
_STAGE_ISSUE_TOKEN = object()

# This mirrors the provider-seeded closed registry byte-for-byte.  Runtime
# checks the physical registry before creating or advancing any checkpoint;
# callers can never introduce a stage, order, or cursor codec.
_STAGE_REGISTRY: dict[bytes, tuple[bytes, bytes, bytes, bool]] = {
    _STAGE_CHANGED_GALLERY: (b"01", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_CHANGED_FILE_HASH: (b"02", b"analysis_digest_v1", _CURSOR_DIGEST, False),
    _STAGE_FILE_HASH_DECISION: (b"03", b"analysis_digest_v1", _CURSOR_DIGEST, False),
    _STAGE_VALIDATE_FILE_HASH: (
        b"04",
        b"analysis_digest_live_v1",
        _CURSOR_DIGEST,
        True,
    ),
    _STAGE_IMPACTED_GALLERY: (b"05", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_IMPACTED_CONTENT: (b"06", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_CONTENT_CANDIDATE: (b"07", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_VALIDATE_CONTENT_CANDIDATE: (
        b"08",
        b"analysis_gallery_live_v1",
        _CURSOR_GALLERY,
        True,
    ),
    _STAGE_CONTENT_OWNER: (b"09", b"analysis_digest_v1", _CURSOR_DIGEST, False),
    _STAGE_VALIDATE_CONTENT_OWNER: (
        b"10",
        b"analysis_digest_live_v1",
        _CURSOR_DIGEST,
        True,
    ),
    _STAGE_IMPACTED_GID: (b"11", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_GID_CANDIDATE: (b"12", b"analysis_gallery_v1", _CURSOR_GALLERY, False),
    _STAGE_VALIDATE_GID_CANDIDATE: (
        b"13",
        b"analysis_gallery_live_v1",
        _CURSOR_GALLERY,
        True,
    ),
    _STAGE_GID_WINNER: (b"14", b"analysis_gid_v1", _CURSOR_GID, False),
    _STAGE_VALIDATE_GID_WINNER: (
        b"15",
        b"analysis_gid_live_v1",
        _CURSOR_GID,
        True,
    ),
}
_GALLERY_PREPARATION_STAGES = frozenset(
    {
        _STAGE_IMPACTED_CONTENT,
        _STAGE_CONTENT_CANDIDATE,
        _STAGE_VALIDATE_CONTENT_CANDIDATE,
        _STAGE_GID_CANDIDATE,
        _STAGE_VALIDATE_GID_CANDIDATE,
    }
)


class AnalysisRepositoryError(RuntimeError):
    """Base class for vNext analysis writer failures."""


class AnalysisNotReadyError(AnalysisRepositoryError):
    """Required immutable input or live authority is absent or stale."""


class AnalysisCorruptionError(AnalysisRepositoryError):
    """Persisted analysis state disagrees with its executable contract."""


class AnalysisUnsupportedError(AnalysisRepositoryError):
    """The requested transition needs a component evaluator not implemented here."""


@dataclass(frozen=True, slots=True)
class AnalysisRun:
    analysis_id: bytes
    build_id: bytes
    policy_id: int
    input_manifest_sha256: bytes
    baseline_analysis_id: bytes | None
    anchor_analysis_id: bytes
    overlay_depth: int
    state: str
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="analysis_id")
        require_uuid16(self.build_id, field="analysis build_id")
        require_positive_int63(self.policy_id, field="analysis policy_id")
        require_digest32(
            self.input_manifest_sha256,
            field="analysis input_manifest_sha256",
        )
        if self.baseline_analysis_id is not None:
            require_uuid16(
                self.baseline_analysis_id,
                field="analysis baseline_analysis_id",
            )
        require_uuid16(self.anchor_analysis_id, field="analysis anchor_analysis_id")
        depth = require_int63(self.overlay_depth, field="analysis overlay_depth")
        if depth > _MAX_OVERLAY_DEPTH:
            raise ValueError("analysis overlay_depth exceeds 16")
        if self.state not in {"OPEN", "COMPLETE", "ABANDONED"}:
            raise ValueError("analysis state is not registered")
        if not isinstance(self.replayed, bool):
            raise TypeError("analysis replayed must be bool")


@dataclass(frozen=True, slots=True)
class AnalysisBatchResult:
    analysis_id: bytes
    stage: bytes
    batch_key: bytes
    start_generation: int
    start_cursor: bytes
    start_processed_count: int
    page_limit: int
    next_cursor: bytes
    next_processed_count: int
    next_state: str
    row_count: int
    terminal: bool
    committed_generation: int
    committed_at: int
    replayed: bool
    component_sealed: bool = False

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="analysis batch analysis_id")
        require_bounded_bytes(
            self.stage,
            field="analysis batch stage",
            minimum=1,
            maximum=64,
        )
        require_bounded_bytes(
            self.batch_key,
            field="analysis batch batch_key",
            minimum=1,
            maximum=512,
        )
        require_positive_int63(
            self.start_generation,
            field="analysis batch start_generation",
        )
        require_positive_int63(
            self.committed_generation,
            field="analysis batch committed_generation",
        )
        for label, cursor in (
            ("start_cursor", self.start_cursor),
            ("next_cursor", self.next_cursor),
        ):
            require_bounded_bytes(
                cursor,
                field=f"analysis batch {label}",
                maximum=2048,
            )
        for label, value in (
            ("start_processed_count", self.start_processed_count),
            ("next_processed_count", self.next_processed_count),
            ("row_count", self.row_count),
            ("committed_at", self.committed_at),
        ):
            require_int63(value, field=f"analysis batch {label}")
        limit = require_positive_int63(
            self.page_limit,
            field="analysis batch page_limit",
        )
        if limit > _MAX_BATCH_ROWS:
            raise ValueError("analysis batch page_limit exceeds the server cap")
        if self.next_state not in {_CHECKPOINT_OPEN, _CHECKPOINT_COMPLETE}:
            raise ValueError("analysis checkpoint state is not registered")
        if (
            not isinstance(self.terminal, bool)
            or not isinstance(self.replayed, bool)
            or not isinstance(self.component_sealed, bool)
        ):
            raise TypeError("analysis batch boolean fields must be bool")
        if self.committed_generation != self.start_generation + 1:
            raise ValueError("analysis batch generation is not a successor")
        if self.next_processed_count != self.start_processed_count + self.row_count:
            raise ValueError("analysis batch processed_count is not monotone")
        if self.terminal:
            if (
                self.row_count != 0
                or self.next_cursor != self.start_cursor
                or self.next_state != _CHECKPOINT_COMPLETE
            ):
                raise ValueError("terminal analysis batch is not terminal-empty")
        elif self.row_count == 0 or self.next_state != _CHECKPOINT_OPEN:
            raise ValueError("nonterminal analysis batch must contain rows")


@dataclass(frozen=True, slots=True)
class _Policy:
    policy_id: int
    algorithm_version: int
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    content_owner_rule_version: int
    gid_winner_rule_version: int


@dataclass(frozen=True, slots=True)
class _RunAuthority:
    analysis_id: bytes
    build_id: bytes
    policy: _Policy
    baseline_analysis_id: bytes | None
    overlay_depth: int


@dataclass(frozen=True, slots=True)
class _ContentImpactPage:
    current_observations: dict[int, int | None]
    old_candidates: dict[int, _ContentCandidate | None]


@dataclass(frozen=True, slots=True)
class _GidImpactPage:
    old_gids: dict[int, int | None]
    current_gids: dict[int, int | None]


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    generation: int
    cursor: bytes
    processed_count: int
    state: str
    updated_at: int
    page_limit: int


@dataclass(frozen=True, slots=True)
class _Decision:
    occurrence_count: int
    artist_count: int
    maximum_gallery_artist_count: int

    @property
    def row(self) -> tuple[int, int, int]:
        return (
            self.occurrence_count,
            self.artist_count,
            self.maximum_gallery_artist_count,
        )


@dataclass(frozen=True, slots=True)
class _ContentCandidate:
    content_sha256: bytes
    gallery_id: int
    prefer_not_already_uploaded: int
    title_scalar_count: int
    download_time: int

    def __post_init__(self) -> None:
        require_digest32(self.content_sha256, field="candidate content_sha256")
        require_positive_int63(self.gallery_id, field="candidate gallery_id")
        require_bool_byte(
            self.prefer_not_already_uploaded,
            field="candidate prefer_not_already_uploaded",
        )
        require_int63(
            self.title_scalar_count,
            field="candidate title_scalar_count",
        )
        require_int63(self.download_time, field="candidate download_time")

    @property
    def row(self) -> tuple[bytes, int, int, int, int]:
        return (
            self.content_sha256,
            self.gallery_id,
            self.prefer_not_already_uploaded,
            self.title_scalar_count,
            self.download_time,
        )


@dataclass(frozen=True, slots=True)
class _ContentOwner:
    content_sha256: bytes
    owner_gallery_id: int

    def __post_init__(self) -> None:
        require_digest32(self.content_sha256, field="owner content_sha256")
        require_positive_int63(self.owner_gallery_id, field="owner gallery_id")

    @property
    def row(self) -> tuple[bytes, int]:
        return (self.content_sha256, self.owner_gallery_id)


@dataclass(frozen=True, slots=True)
class _GidCandidate:
    gallery_id: int

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="GID candidate gallery_id")

    @property
    def row(self) -> tuple[int]:
        return (self.gallery_id,)


@dataclass(frozen=True, slots=True)
class _GidWinner:
    gid: int
    winner_gallery_id: int

    def __post_init__(self) -> None:
        require_positive_int63(self.gid, field="winner gid")
        require_positive_int63(self.winner_gallery_id, field="winner gallery_id")

    @property
    def row(self) -> tuple[int, int]:
        return (self.gid, self.winner_gallery_id)


@dataclass(frozen=True, slots=True)
class AnalysisPreparationAuthority:
    """Short-transaction receipt authorizing immutable out-of-tx preparation."""

    analysis_id: bytes
    build_id: bytes
    generation: int
    policy_id: int
    input_manifest_sha256: bytes
    component_seals: tuple[tuple[bytes, int, int], ...]
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PREPARATION_TOKEN:
            raise TypeError("preparation authorities are repository-issued only")
        require_uuid16(self.analysis_id, field="authority analysis_id")
        require_uuid16(self.build_id, field="authority build_id")
        require_positive_int63(self.generation, field="authority generation")
        require_positive_int63(self.policy_id, field="authority policy_id")
        require_digest32(
            self.input_manifest_sha256,
            field="authority input_manifest_sha256",
        )
        previous: bytes | None = None
        for component, row_count, sealed_at in self.component_seals:
            exact = require_bounded_bytes(
                component,
                field="authority state_component",
                minimum=1,
                maximum=64,
            )
            if exact not in ANALYSIS_COMPONENTS or (
                previous is not None and exact <= previous
            ):
                raise ValueError("authority component seals are not exact ordered set")
            require_int63(row_count, field="authority component row_count")
            require_int63(sealed_at, field="authority component sealed_at")
            previous = exact


@dataclass(frozen=True, slots=True)
class AnalysisStageIssue:
    """Opaque exact next-stage authority derived from durable checkpoints."""

    analysis_id: bytes
    build_id: bytes
    stage: bytes | None
    batch_key: bytes | None
    page_limit: int
    checkpoint_generation: int | None
    checkpoint_cursor: bytes | None
    checkpoint_processed_count: int | None
    memberships: tuple[tuple[int, int | None], ...]
    preparation_authority: AnalysisPreparationAuthority
    completion_snapshot_sha256: bytes | None
    replayed_result: AnalysisBatchResult | None
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _STAGE_ISSUE_TOKEN:
            raise TypeError("analysis stage issues are repository-issued only")
        require_uuid16(self.analysis_id, field="stage issue analysis_id")
        require_uuid16(self.build_id, field="stage issue build_id")
        if not isinstance(
            self.preparation_authority,
            AnalysisPreparationAuthority,
        ):
            raise TypeError("stage issue preparation authority is missing")
        self.preparation_authority.__post_init__()
        if (
            self.preparation_authority.analysis_id != self.analysis_id
            or self.preparation_authority.build_id != self.build_id
        ):
            raise ValueError("stage issue differs from its preparation authority")
        if self.stage is None:
            if any(
                value is not None
                for value in (
                    self.batch_key,
                    self.checkpoint_generation,
                    self.checkpoint_cursor,
                    self.checkpoint_processed_count,
                    self.replayed_result,
                )
            ):
                raise ValueError("terminal/snapshot stage issue carries batch state")
            if self.page_limit != 0 or self.memberships:
                raise ValueError("terminal/snapshot stage issue carries a page")
            if self.completion_snapshot_sha256 is not None:
                require_digest32(
                    self.completion_snapshot_sha256,
                    field="stage issue completion_snapshot_sha256",
                )
            return
        if self.stage not in _STAGES:
            raise ValueError("analysis stage issue has an unregistered stage")
        require_bounded_bytes(
            self.batch_key,
            field="stage issue batch_key",
            minimum=1,
            maximum=512,
        )
        require_positive_int63(
            self.checkpoint_generation,
            field="stage issue checkpoint_generation",
        )
        require_bounded_bytes(
            self.checkpoint_cursor,
            field="stage issue checkpoint_cursor",
            maximum=2048,
        )
        require_int63(
            self.checkpoint_processed_count,
            field="stage issue checkpoint_processed_count",
        )
        limit = require_positive_int63(
            self.page_limit,
            field="stage issue page_limit",
        )
        if limit > _MAX_BATCH_ROWS:
            raise ValueError("analysis stage issue exceeds the 128-row cap")
        if self.completion_snapshot_sha256 is not None:
            raise ValueError("an active stage issue cannot be complete")
        previous = 0
        for gallery_id, observation_id in self.memberships:
            gallery = require_positive_int63(
                gallery_id,
                field="stage issue gallery_id",
            )
            if gallery <= previous:
                raise ValueError("stage issue memberships are not strictly ordered")
            if observation_id is not None:
                require_positive_int63(
                    observation_id,
                    field="stage issue observation_id",
                )
            previous = gallery
        if self.stage in _GALLERY_PREPARATION_STAGES:
            if len(self.memberships) > limit:
                raise ValueError("stage issue memberships exceed the page limit")
        elif self.memberships:
            raise ValueError("non-preparation stage issue carries memberships")
        if self.replayed_result is not None:
            if not isinstance(self.replayed_result, AnalysisBatchResult):
                raise TypeError("stage issue replay result has a foreign type")
            if (
                self.replayed_result.analysis_id != self.analysis_id
                or self.replayed_result.stage != self.stage
                or self.replayed_result.batch_key != self.batch_key
                or not self.replayed_result.replayed
            ):
                raise ValueError("stage issue replay result differs from its batch")


@dataclass(frozen=True, slots=True)
class AnalysisGalleryPreparation:
    """Opaque server-derived facts and optional effective-content upload plan."""

    analysis_id: bytes
    build_id: bytes
    gallery_id: int
    observation_id: int
    gid: int
    content_sha256: bytes | None
    content_prefer_not_already_uploaded: int | None
    content_title_scalar_count: int | None
    content_download_time: int | None
    content_upload_plan: CanonicalValueUploadPlan | None
    authority: AnalysisPreparationAuthority
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PREPARATION_TOKEN:
            raise TypeError("gallery preparations are repository-issued only")
        require_uuid16(self.analysis_id, field="preparation analysis_id")
        require_uuid16(self.build_id, field="preparation build_id")
        if not isinstance(self.authority, AnalysisPreparationAuthority):
            raise TypeError("gallery preparation authority is missing")
        self.authority.__post_init__()
        if (
            self.authority.analysis_id != self.analysis_id
            or self.authority.build_id != self.build_id
        ):
            raise ValueError("gallery preparation differs from its authority")
        require_positive_int63(self.gallery_id, field="preparation gallery_id")
        require_positive_int63(
            self.observation_id,
            field="preparation observation_id",
        )
        require_positive_int63(self.gid, field="preparation gid")
        content_fields = (
            self.content_sha256,
            self.content_prefer_not_already_uploaded,
            self.content_title_scalar_count,
            self.content_download_time,
            self.content_upload_plan,
        )
        if self.content_sha256 is None:
            if any(value is not None for value in content_fields[1:]):
                raise ValueError("empty effective content cannot carry candidate facts")
        else:
            require_digest32(
                self.content_sha256,
                field="preparation content_sha256",
            )
            if self.content_prefer_not_already_uploaded is None:
                raise ValueError("content candidate preference is missing")
            require_bool_byte(
                self.content_prefer_not_already_uploaded,
                field="preparation content prefer_not_already_uploaded",
            )
            if self.content_title_scalar_count is None:
                raise ValueError("content candidate title scalar count is missing")
            require_int63(
                self.content_title_scalar_count,
                field="preparation content title_scalar_count",
            )
            if self.content_download_time is None:
                raise ValueError("content candidate download time is missing")
            require_int63(
                self.content_download_time,
                field="preparation content download_time",
            )
            if self.content_upload_plan is None:
                raise ValueError("content candidate upload plan is missing")
            if type(self.content_upload_plan) is not CanonicalValueUploadPlan:
                raise TypeError(
                    "content upload plan must be an exact CanonicalValueUploadPlan"
                )
            require_ascii_bytes(
                self.content_upload_plan.digest_domain,
                field="content upload plan domain",
                minimum=1,
                maximum=64,
            )
            require_digest32(
                self.content_upload_plan.value_sha256,
                field="content upload plan value_sha256",
            )
            require_int63(
                self.content_upload_plan.byte_count,
                field="content upload plan byte_count",
            )
            if (
                self.content_upload_plan.digest_domain != _EFFECTIVE_CONTENT_DOMAIN
                or self.content_upload_plan.value_sha256 != self.content_sha256
            ):
                raise ValueError("content upload plan differs from candidate identity")

    def close(self) -> None:
        if self.content_upload_plan is not None:
            self.content_upload_plan.close()

    def __enter__(self) -> AnalysisGalleryPreparation:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class AnalysisSnapshotPreparation:
    """Opaque exact preflight plus disk-backed canonical snapshot upload."""

    analysis_id: bytes
    build_id: bytes
    gallery_count: int
    file_count: int
    byte_count: int
    gallery_entry_count: int
    decision_entry_count: int
    owner_entry_count: int
    winner_entry_count: int
    payload_byte_count: int
    upload_plan: CanonicalValueUploadPlan
    authority: AnalysisPreparationAuthority
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PREPARATION_TOKEN:
            raise TypeError("snapshot preparations are repository-issued only")
        require_uuid16(self.analysis_id, field="snapshot analysis_id")
        require_uuid16(self.build_id, field="snapshot build_id")
        if not isinstance(self.authority, AnalysisPreparationAuthority):
            raise TypeError("snapshot preparation authority is missing")
        self.authority.__post_init__()
        if (
            self.authority.analysis_id != self.analysis_id
            or self.authority.build_id != self.build_id
            or len(self.authority.component_seals) != len(ANALYSIS_COMPONENTS)
        ):
            raise ValueError("snapshot preparation lacks exact five-seal authority")
        for field_name in (
            "gallery_count",
            "file_count",
            "byte_count",
            "gallery_entry_count",
            "decision_entry_count",
            "owner_entry_count",
            "winner_entry_count",
            "payload_byte_count",
        ):
            require_int63(getattr(self, field_name), field=f"snapshot {field_name}")
        if self.gallery_entry_count != self.gallery_count:
            raise ValueError("snapshot gallery section count differs from build")
        if type(self.upload_plan) is not CanonicalValueUploadPlan:
            raise TypeError("snapshot upload plan must be exact")
        upload_domain = require_ascii_bytes(
            self.upload_plan.digest_domain,
            field="snapshot upload plan domain",
            minimum=1,
            maximum=64,
        )
        require_digest32(
            self.upload_plan.value_sha256,
            field="snapshot upload plan value_sha256",
        )
        require_int63(
            self.upload_plan.byte_count,
            field="snapshot upload plan byte_count",
        )
        if upload_domain != _SNAPSHOT_DOMAIN:
            raise ValueError("snapshot upload plan has the wrong digest domain")
        if self.upload_plan.byte_count != self.payload_byte_count:
            raise ValueError("snapshot upload plan byte count differs from preflight")

    def close(self) -> None:
        self.upload_plan.close()

    def __enter__(self) -> AnalysisSnapshotPreparation:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class AnalysisRepository:
    """Bounded writer for analysis initialization and file-decision overlays."""

    @staticmethod
    def begin(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        policy_id: int,
        proposed_analysis_id: bytes,
        now: int,
    ) -> AnalysisRun:
        """Create or resume the sole semantic run for ``build_id``.

        Baseline identity is never accepted from the caller.  It is derived
        from the build's pinned source baseline and its retained provenance.
        An existing run must use the exact same policy; a policy change needs
        a successor build rather than a sibling analysis.
        """

        build = require_uuid16(build_id, field="analysis build_id")
        policy_key = require_positive_int63(policy_id, field="analysis policy_id")
        attempt = require_uuid16(
            proposed_analysis_id,
            field="proposed analysis_id",
        )
        timestamp = require_int63(now, field="analysis begin now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_exact_stage_registry(work)

        working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("analysis-working-build", 1),
            "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
            (1,),
        )
        if working != (build,):
            raise AnalysisNotReadyError(
                "analysis build is not the sole live source working build"
            )
        _require_generation_mapping(work, generation=generation, build_id=build)

        try:
            source_row = load_source_build_family(work.connector, build_id=build)
            manifest_row = load_build_manifest_family(work.connector, build_id=build)
        except ManifestFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if source_row is None or source_row.state != "SEALED":
            raise AnalysisNotReadyError(
                "analysis requires an exactly SEALED source build"
            )
        source_sealed_at = require_int63(
            source_row.sealed_at,
            field="source build sealed_at",
        )

        if manifest_row is None:
            raise AnalysisNotReadyError("sealed build has no exact build_manifest")
        manifest = manifest_row.manifest_sha256
        manifest_counts = (
            manifest_row.gallery_count,
            manifest_row.file_count,
            manifest_row.byte_count,
        )
        policy = _load_policy(work, policy_key)
        input_digest = _analysis_input_digest(manifest, manifest_counts, policy)

        committed_sibling = _load_committed_sibling_analysis(
            work, build_id=build, policy_id=policy_key
        )
        if committed_sibling is not None:
            return committed_sibling

        try:
            existing_family = load_analysis_run_family_by_identity(
                work.connector,
                build_id=build,
                policy_id=policy_key,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if existing_family is not None:
            if existing_family.input_manifest_sha256 != input_digest:
                raise AnalysisCorruptionError(
                    "analysis build replay changed its server-derived input"
                )
            existing_id = existing_family.analysis_id
            persisted = _load_layout(work, existing_id)
            persisted_baseline = persisted[0]
            try:
                pinned_baseline, _revision, _generation, _channel = (
                    _derive_pinned_baseline(work, build_id=build)
                )
                replay_layout = _derive_layout(
                    work,
                    baseline=pinned_baseline,
                    policy=policy,
                )
            except AnalysisNotReadyError as error:
                raise AnalysisCorruptionError(
                    "analysis build replay lost its sealed baseline"
                ) from error
            if pinned_baseline != persisted_baseline and not (
                # Finalization prunes the obsolete working baseline of a
                # published self-only depth-zero result (a policy change or
                # a depth-16 compaction) while the build's base pin remains
                # until cleanup releases it; that exact prune is the only
                # legitimate difference.
                persisted_baseline is None
                and replay_layout[0] is None
                and _analysis_is_finalized(work, analysis_id=existing_id)
            ):
                raise AnalysisCorruptionError(
                    "analysis build replay differs from its pinned "
                    "source-build baseline"
                )
            expected_anchor = (
                existing_id if replay_layout[0] is None else replay_layout[0]
            )
            expected_ancestry = tuple(
                existing_id if ancestor is None else ancestor
                for ancestor in replay_layout[2]
            )
            expected = (
                persisted_baseline,
                expected_anchor,
                replay_layout[1],
                expected_ancestry,
            )
            if persisted != expected:
                raise AnalysisCorruptionError(
                    "analysis build replay changed its baseline or ancestry"
                )
            return AnalysisRun(
                existing_id,
                build,
                policy_key,
                input_digest,
                persisted_baseline,
                expected_anchor,
                replay_layout[1],
                existing_family.state,
                True,
            )

        baseline = _derive_baseline(work, build_id=build)
        layout = _derive_layout(work, baseline=baseline, policy=policy)

        started_at = database_unix_microseconds(work)
        if started_at < source_sealed_at:
            raise AnalysisNotReadyError(
                "database analysis start time precedes source build sealing"
            )
        try:
            family, created = ensure_analysis_run_family(
                work.connector,
                analysis_id=attempt,
                build_id=build,
                policy_id=policy_key,
                input_manifest_sha256=input_digest,
                started_at=started_at,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if not created:
            # The working-root lock serializes normal begin calls; retaining this
            # branch makes a backend uniqueness race fail closed yet replayable.
            existing_id = family.analysis_id
            state = family.state
            persisted = _load_layout(work, existing_id)
            expected_anchor = existing_id if layout[0] is None else layout[0]
            expected_ancestry = tuple(
                existing_id if ancestor is None else ancestor for ancestor in layout[2]
            )
            expected = (baseline, expected_anchor, layout[1], expected_ancestry)
            if persisted != expected:
                raise AnalysisCorruptionError(
                    "analysis build replay changed its baseline or ancestry"
                )
            return AnalysisRun(
                existing_id,
                build,
                policy_key,
                input_digest,
                baseline,
                expected_anchor,
                layout[1],
                state,
                True,
            )

        if baseline is not None:
            work.connector.execute(
                "INSERT INTO catalog_analysis_baselines "
                "(analysis_id, base_analysis_id) VALUES (%s, %s)",
                (attempt, baseline),
            )
        anchor_id, overlay_depth, ancestry = layout
        # The layout helper uses a sentinel for the not-yet-allocated child.
        if anchor_id is None:
            anchor_id = attempt
        materialized_ancestry = tuple(
            attempt if ancestor is None else ancestor for ancestor in ancestry
        )
        for depth, ancestor in enumerate(materialized_ancestry):
            work.connector.execute(
                "INSERT INTO catalog_analysis_state_ancestry "
                "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                "VALUES (%s, %s, %s)",
                (attempt, depth, ancestor),
            )
        for stage in _STAGES:
            kind, live = _stage_cursor_spec(stage)
            _initialize_checkpoint(
                work,
                analysis_id=attempt,
                stage=stage,
                cursor=_encode_cursor(kind, None, live_count=0 if live else None),
                updated_at=started_at,
            )
        return AnalysisRun(
            attempt,
            build,
            policy_key,
            input_digest,
            baseline,
            anchor_id,
            overlay_depth,
            "OPEN",
            False,
        )

    @staticmethod
    def begin_and_issue_next_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        policy_id: int,
        proposed_analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> tuple[AnalysisRun, AnalysisStageIssue]:
        """Begin/restart and issue one server-selected batch under one fence lock.

        ``begin`` already acquires the gate, ingest fence, and sole working-root
        lock.  Reauthorizing before the checkpoint lock would invert the global
        lock order, so the combined application operation carries the authority
        derived by ``begin`` directly into the bounded issuer.
        """

        timestamp = require_int63(now, field="analysis begin/issue now")
        run = AnalysisRepository.begin(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build_id,
            policy_id=policy_id,
            proposed_analysis_id=proposed_analysis_id,
            now=timestamp,
        )
        authority = _RunAuthority(
            run.analysis_id,
            run.build_id,
            _load_policy(work, run.policy_id),
            run.baseline_analysis_id,
            run.overlay_depth,
        )
        issue = _issue_next_batch_authorized(
            work,
            authority=authority,
            generation=ingest_turn.generation,
            batch_key=batch_key,
            max_rows=max_rows,
        )
        return run, issue

    @staticmethod
    def abandon(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        now: int,
    ) -> AnalysisRun:
        """Fence OPEN to ABANDONED and release its exact working root atomically."""

        analysis = require_uuid16(analysis_id, field="analysis_id")
        timestamp = require_int63(now, field="analysis abandon now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        state_row = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("analysis-run", analysis),
            "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
            (analysis,),
        )
        if len(state_row) != 1:
            raise AnalysisNotReadyError("analysis run is missing")
        try:
            family = load_analysis_run_family(
                work.connector,
                analysis_id=analysis,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if family is None or family.state != _require_run_state(state_row[0]):
            raise AnalysisCorruptionError(
                "analysis run family changed during abandonment lock"
            )
        _require_generation_mapping(
            work,
            generation=generation,
            build_id=family.build_id,
        )
        working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("analysis-working-build", 1),
            "SELECT build_id, assigned_at FROM operational_source_working_builds "
            "WHERE slot = %s",
            (1,),
        )
        catalog_working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("analysis-working-candidate", 1),
            f"SELECT candidate_id FROM {_CATALOG_WORKING_CANDIDATE_TABLE} "
            "WHERE slot = %s",
            (1,),
        )
        binding = work.connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
            (analysis,),
        )
        if family.state not in {"OPEN", "ABANDONED"}:
            raise AnalysisNotReadyError(
                f"analysis run cannot be abandoned from {family.state}"
            )
        related = work.connector.fetch_all(
            f"SELECT analysis_id FROM {_ANALYSIS_RUN_DESCRIPTOR_TABLE} "
            "WHERE build_id = %s LIMIT 2",
            (family.build_id,),
        )
        blockers = (
            (
                _PUBLICATION_CANDIDATE_TABLE,
                "analysis_id",
                analysis,
                "publication candidate",
            ),
            (
                _SOURCE_REVISION_PROVENANCE_TABLE,
                "analysis_id",
                analysis,
                "source revision provenance",
            ),
            (
                _OPERATIONAL_PREPARATION_TABLE,
                "build_id",
                family.build_id,
                "operational preparation",
            ),
        )
        blocking_authority = next(
            (
                label
                for table, column, value, label in blockers
                if work.connector.fetch_one(
                    f"SELECT 1 FROM {table} WHERE {column} = %s LIMIT 1",
                    (value,),
                )
            ),
            None,
        )
        if family.state == "ABANDONED":
            if (
                related != [(analysis,)]
                or working
                or binding
                or family.completed_at is not None
                or blocking_authority is not None
            ):
                raise AnalysisCorruptionError(
                    "ABANDONED analysis retained terminal-incompatible state"
                )
            baseline, anchor, depth, _ancestry = _load_layout(work, analysis)
            return AnalysisRun(
                analysis,
                family.build_id,
                family.policy_id,
                family.input_manifest_sha256,
                baseline,
                anchor,
                depth,
                "ABANDONED",
                True,
            )
        created = work.connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_descriptor "
            "WHERE build_id = %s",
            (family.build_id,),
        )
        if len(created) != 1:
            raise AnalysisCorruptionError(
                "analysis abandonment lost its source build creation time"
            )
        expected_working = (
            family.build_id,
            require_int63(
                created[0],
                field="analysis abandonment source build created_at",
            ),
        )
        if working != expected_working or binding or family.completed_at is not None:
            raise AnalysisNotReadyError(
                "analysis abandonment lost its exact unbound working root"
            )
        if related != [(analysis,)]:
            raise AnalysisNotReadyError(
                "analysis abandonment requires the target to be the sole run "
                "for its source build"
            )
        if catalog_working:
            raise AnalysisNotReadyError(
                "analysis abandonment awaits the live catalog working candidate"
            )
        if blocking_authority is not None:
            raise AnalysisCorruptionError(
                f"OPEN analysis retained a {blocking_authority} authority"
            )
        cas_analysis_run_state(
            work,
            analysis_id=analysis,
            previous="OPEN",
            successor="ABANDONED",
            authority="analysis abandonment",
        )
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_source_working_builds "
            "WHERE slot = %s AND build_id = %s AND assigned_at = %s",
            (1, family.build_id, expected_working[1]),
        )
        if deleted != 1:
            raise AnalysisCorruptionError(
                "analysis working root changed during abandonment"
            )
        baseline, anchor, depth, _ancestry = _load_layout(work, analysis)
        return AnalysisRun(
            analysis,
            family.build_id,
            family.policy_id,
            family.input_manifest_sha256,
            baseline,
            anchor,
            depth,
            "ABANDONED",
            False,
        )

    @staticmethod
    def process_changed_gallery_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_CHANGED_GALLERY,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _changed_gallery_rows(
            work,
            authority,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for gallery_id, change_kind in selected:
            gallery = require_positive_int63(gallery_id, field="changed gallery_id")
            if change_kind not in {"ADDED", "REMOVED", "REPLACED"}:
                raise AnalysisCorruptionError(
                    "server derived an unknown gallery change"
                )
            work.connector.execute(
                "INSERT INTO catalog_analysis_changed_galleries "
                "(analysis_id, gallery_id, change_kind) VALUES (%s, %s, %s)",
                (authority.analysis_id, gallery, change_kind),
            )
        next_key = last
        if selected:
            next_key = require_positive_int63(
                selected[-1][0], field="changed gallery cursor"
            ).to_bytes(8, "big")
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_CHANGED_GALLERY,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_changed_file_hash_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_CHANGED_FILE_HASH,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_CHANGED_GALLERY)
        last, _live_count = _decode_cursor(
            _CURSOR_DIGEST,
            checkpoint.cursor,
            live=False,
        )
        rows = _changed_file_hash_rows(
            work,
            authority,
            after=last,
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for row in selected:
            digest = require_digest32(row[0], field="changed file_sha256")
            work.connector.execute(
                "INSERT INTO catalog_analysis_changed_file_hashes "
                "(analysis_id, file_sha256) VALUES (%s, %s)",
                (authority.analysis_id, digest),
            )
        next_key = (
            last
            if not selected
            else require_digest32(selected[-1][0], field="changed file cursor")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_CHANGED_FILE_HASH,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_DIGEST, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_file_hash_decision_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_FILE_HASH_DECISION,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_CHANGED_FILE_HASH)
        last, _live_count = _decode_cursor(
            _CURSOR_DIGEST,
            checkpoint.cursor,
            live=False,
        )
        rows = _decision_work_rows(
            work,
            authority,
            after=last,
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for row in selected:
            digest = require_digest32(row[0], field="decision file_sha256")
            _materialize_decision(work, authority, digest)
        next_key = (
            last
            if not selected
            else require_digest32(selected[-1][0], field="decision file cursor")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_FILE_HASH_DECISION,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_DIGEST, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def validate_file_hash_decision_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        """Independently compare exact target, overlay, and resolved rows.

        The evaluator reads immutable source facts directly; it does not trust
        ``analysis_file_hash_decision`` or a caller digest/count.  Its cursor
        merge-walks the complete expected/parent/actual key union, so both an
        omitted target row and an extra shadow/tombstone are detected.
        """

        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_VALIDATE_FILE_HASH,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_FILE_HASH_DECISION)
        _require_unsealed_component(
            work,
            authority.analysis_id,
            _COMPONENT_FILE_HASH,
        )

        last, live_count = _decode_cursor(
            _CURSOR_DIGEST,
            checkpoint.cursor,
            live=True,
        )
        rows = _validation_key_rows(
            work,
            authority,
            after=last,
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for row in selected:
            digest = require_digest32(row[0], field="validation file_sha256")
            target = _evaluate_file_decision(work, authority, digest)
            parent = _resolved_decision(
                work,
                authority.baseline_analysis_id,
                digest,
            )
            shadow = _shadow_decision(work, authority.analysis_id, digest)
            tombstone = bool(
                work.connector.fetch_one(
                    "SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone "
                    "WHERE analysis_id = %s AND file_sha256 = %s",
                    (authority.analysis_id, digest),
                )
            )
            if shadow is not None and tombstone:
                raise AnalysisCorruptionError(
                    "file-hash key exists in both shadow and tombstone"
                )
            if authority.overlay_depth == 0:
                expected_shadow = target
                expected_tombstone = False
            elif target is None and parent is not None:
                expected_shadow = None
                expected_tombstone = True
            elif target is not None and target != parent:
                expected_shadow = target
                expected_tombstone = False
            else:
                expected_shadow = None
                expected_tombstone = False
            if shadow != expected_shadow or tombstone != expected_tombstone:
                raise AnalysisCorruptionError(
                    "file-hash shadow/tombstone differs from the full evaluator"
                )
            resolved = _resolved_decision(work, authority.analysis_id, digest)
            if resolved != target:
                raise AnalysisCorruptionError(
                    "resolved file-hash view differs from the full evaluator"
                )
            if target is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="validated file-hash live row count",
                )

        next_key = (
            last
            if not selected
            else require_digest32(selected[-1][0], field="validation file cursor")
        )
        terminal = not selected
        result = _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_VALIDATE_FILE_HASH,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(
                _CURSOR_DIGEST,
                next_key,
                live_count=live_count,
            ),
            row_count=len(selected),
            terminal=terminal,
            now=now,
        )
        if not terminal:
            return result
        try:
            ensure_analysis_state_component_family(
                work.connector,
                analysis_id=authority.analysis_id,
                state_component=_COMPONENT_FILE_HASH,
                row_count=live_count,
                sealed_at=result.committed_at,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        return AnalysisBatchResult(
            result.analysis_id,
            result.stage,
            result.batch_key,
            result.start_generation,
            result.start_cursor,
            result.start_processed_count,
            result.page_limit,
            result.next_cursor,
            result.next_processed_count,
            result.next_state,
            result.row_count,
            result.terminal,
            result.committed_generation,
            result.committed_at,
            result.replayed,
            True,
        )

    @staticmethod
    def issue_preparation_authority(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        now: int,
    ) -> AnalysisPreparationAuthority:
        """Issue a bounded receipt in a short live-fence write transaction."""

        authority = _authorize_analysis(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            now=require_int63(now, field="preparation authority now"),
            allow_complete=True,
        )
        return _preparation_authority_receipt(
            work,
            authority,
            generation=ingest_turn.generation,
        )

    @staticmethod
    def issue_next_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisStageIssue:
        """Fix the exact next durable stage and gallery-membership page.

        The caller supplies only an idempotency key and an upper bound. Stage,
        cursor, processed count, gallery keys, and current membership are all
        loaded under the live analysis authority. No corpus-sized value is
        returned and the hard server cap remains 128.
        """

        authority = _authorize_analysis(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            now=require_int63(now, field="analysis stage issue now"),
            allow_complete=True,
        )
        return _issue_next_batch_authorized(
            work,
            authority=authority,
            generation=ingest_turn.generation,
            batch_key=batch_key,
            max_rows=max_rows,
        )

    @staticmethod
    def process_issued_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        issue: AnalysisStageIssue,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        """Commit exactly one previously issued stage page or replay it."""

        if not isinstance(issue, AnalysisStageIssue):
            raise TypeError("issue must be AnalysisStageIssue")
        issue.__post_init__()
        if issue.stage is None or issue.batch_key is None:
            raise AnalysisNotReadyError("snapshot/complete issue has no stage batch")
        exact_preparations = tuple(preparations)
        if issue.stage not in _GALLERY_PREPARATION_STAGES and exact_preparations:
            raise AnalysisNotReadyError(
                "non-preparation analysis stage received gallery preparations"
            )

        if issue.replayed_result is not None:
            authority = _authorize_analysis(
                work,
                gate_lease=gate_lease,
                ingest_turn=ingest_turn,
                analysis_id=issue.analysis_id,
                now=require_int63(now, field="analysis replay commit now"),
            )
            if authority.build_id != issue.build_id:
                raise AnalysisCorruptionError(
                    "replayed analysis issue changed its build authority"
                )
            return issue.replayed_result

        common: dict[str, Any] = {
            "gate_lease": gate_lease,
            "ingest_turn": ingest_turn,
            "analysis_id": issue.analysis_id,
            "batch_key": issue.batch_key,
            "max_rows": issue.page_limit,
            "now": now,
        }
        if issue.stage == _STAGE_CHANGED_GALLERY:
            result = AnalysisRepository.process_changed_gallery_batch(work, **common)
        elif issue.stage == _STAGE_CHANGED_FILE_HASH:
            result = AnalysisRepository.process_changed_file_hash_batch(work, **common)
        elif issue.stage == _STAGE_FILE_HASH_DECISION:
            result = AnalysisRepository.process_file_hash_decision_batch(work, **common)
        elif issue.stage == _STAGE_VALIDATE_FILE_HASH:
            result = AnalysisRepository.validate_file_hash_decision_batch(
                work, **common
            )
        elif issue.stage == _STAGE_IMPACTED_GALLERY:
            result = AnalysisRepository.process_impacted_gallery_batch(work, **common)
        elif issue.stage == _STAGE_IMPACTED_CONTENT:
            result = AnalysisRepository.process_impacted_content_batch(
                work,
                preparations=exact_preparations,
                **common,
            )
        elif issue.stage == _STAGE_CONTENT_CANDIDATE:
            result = AnalysisRepository.process_content_owner_candidate_batch(
                work,
                preparations=exact_preparations,
                **common,
            )
        elif issue.stage == _STAGE_VALIDATE_CONTENT_CANDIDATE:
            result = AnalysisRepository.validate_content_owner_candidate_batch(
                work,
                preparations=exact_preparations,
                **common,
            )
        elif issue.stage == _STAGE_CONTENT_OWNER:
            result = AnalysisRepository.process_content_owner_batch(work, **common)
        elif issue.stage == _STAGE_VALIDATE_CONTENT_OWNER:
            result = AnalysisRepository.validate_content_owner_batch(work, **common)
        elif issue.stage == _STAGE_IMPACTED_GID:
            result = AnalysisRepository.process_impacted_gid_batch(work, **common)
        elif issue.stage == _STAGE_GID_CANDIDATE:
            result = AnalysisRepository.process_gid_candidate_batch(
                work,
                preparations=exact_preparations,
                **common,
            )
        elif issue.stage == _STAGE_VALIDATE_GID_CANDIDATE:
            result = AnalysisRepository.validate_gid_candidate_batch(
                work,
                preparations=exact_preparations,
                **common,
            )
        elif issue.stage == _STAGE_GID_WINNER:
            result = AnalysisRepository.process_gid_winner_batch(work, **common)
        elif issue.stage == _STAGE_VALIDATE_GID_WINNER:
            result = AnalysisRepository.validate_gid_winner_batch(work, **common)
        else:
            raise AnalysisCorruptionError("issued analysis stage is not executable")
        if (
            result.analysis_id != issue.analysis_id
            or result.stage != issue.stage
            or result.batch_key != issue.batch_key
            or result.start_generation != issue.checkpoint_generation
            or result.start_cursor != issue.checkpoint_cursor
            or result.start_processed_count != issue.checkpoint_processed_count
            or result.page_limit != issue.page_limit
        ):
            raise AnalysisNotReadyError(
                "analysis stage issue is stale against its durable checkpoint"
            )
        return result

    @staticmethod
    def prepare_gallery(
        connector: SQLConnector,
        *,
        backend: str,
        authority: AnalysisPreparationAuthority,
        gallery_id: int,
    ) -> AnalysisGalleryPreparation:
        """Build one gallery plan in an independent immutable read snapshot.

        The effective-content payload is spooled by ``CanonicalValueUploadPlan``;
        exact metadata and tag values are parsed incrementally.  No unbounded
        title, tag, or file list is materialized in memory, and no writer lock
        remains held while this method runs.
        """

        if not isinstance(authority, AnalysisPreparationAuthority):
            raise TypeError("authority must be AnalysisPreparationAuthority")
        authority.__post_init__()
        if _COMPONENT_FILE_HASH not in {row[0] for row in authority.component_seals}:
            raise AnalysisNotReadyError(
                "gallery preparation requires the sealed file-decision component"
            )
        gallery = require_positive_int63(
            gallery_id,
            field="gallery preparation gallery_id",
        )
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            run = _load_preparation_authority(work, authority)
            return _prepare_gallery(work, run, gallery, authority)

    @staticmethod
    def process_impacted_gallery_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_IMPACTED_GALLERY,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_component_sealed(work, authority.analysis_id, _COMPONENT_FILE_HASH)
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _impacted_gallery_rows(
            work,
            authority,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for (gallery_id,) in selected:
            gallery = require_positive_int63(
                gallery_id,
                field="impacted gallery_id",
            )
            work.connector.execute(
                "INSERT INTO catalog_analysis_impacted_galleries "
                "(analysis_id, gallery_id) VALUES (%s, %s)",
                (authority.analysis_id, gallery),
            )
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="impacted gallery cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_IMPACTED_GALLERY,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_impacted_content_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        """Freeze exact old/new content groups and consume canonical claims.

        ``preparations`` must match the server-selected next gallery keys
        exactly.  Their constructor capability proves that neither the caller
        nor a serialized request chose content or comparator authority.
        """

        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_IMPACTED_CONTENT,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(
                work,
                authority,
                replay,
                preparations=preparations,
            )
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_IMPACTED_GALLERY)
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        gallery_ids = tuple(
            require_positive_int63(
                row[0],
                field="impacted content gallery_id",
            )
            for row in selected
        )
        impact_page = _load_content_impact_page(work, authority, gallery_ids)
        exact_preparations = _require_transition_preparations(
            work,
            authority,
            selected,
            preparations,
            memberships=impact_page.current_observations,
        )
        provenance: list[tuple[int, bytes]] = []
        for gallery_id, preparation in zip(
            gallery_ids,
            exact_preparations,
            strict=True,
        ):
            old = impact_page.old_candidates[gallery_id]
            contents = {
                value
                for value in (
                    None if old is None else old.content_sha256,
                    None if preparation is None else preparation.content_sha256,
                )
                if value is not None
            }
            provenance.extend(
                (gallery_id, content_sha256) for content_sha256 in sorted(contents)
            )
        try:
            if selected:
                provenance_receipt = prepare_analysis_impacted_content_provenance_page(
                    work.connector,
                    analysis_id=authority.analysis_id,
                    entries=tuple(provenance),
                )
                _consume_effective_content_claims(
                    work,
                    authority,
                    exact_preparations,
                    preexisting_contents=provenance_receipt.existing_keys,
                )
                apply_analysis_impacted_content_provenance_page(
                    work.connector,
                    provenance_receipt,
                )
            else:
                require_exact_analysis_impacted_content_provenance_page(
                    work.connector,
                    analysis_id=authority.analysis_id,
                    after_gallery_id=(
                        None if last is None else int.from_bytes(last, "big")
                    ),
                    through_gallery_id=None,
                    expected=(),
                )
                require_complete_analysis_impacted_content_keyspace(
                    work.connector,
                    analysis_id=authority.analysis_id,
                )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="impacted content cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_IMPACTED_CONTENT,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_content_owner_candidate_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_CONTENT_CANDIDATE,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(
                work,
                authority,
                replay,
                preparations=preparations,
            )
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_IMPACTED_CONTENT)
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        exact_preparations = _require_transition_preparations(
            work,
            authority,
            selected,
            preparations,
        )
        for (raw_gallery_id,), preparation in zip(
            selected,
            exact_preparations,
            strict=True,
        ):
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="content candidate gallery_id",
            )
            target = (
                None
                if preparation is None
                else _content_candidate_from_preparation(preparation)
            )
            parent = _resolved_content_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            _materialize_content_candidate(
                work,
                authority,
                gallery_id,
                target,
                parent,
            )
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="content candidate cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_CONTENT_CANDIDATE,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def validate_content_owner_candidate_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_VALIDATE_CONTENT_CANDIDATE,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(
                work,
                authority,
                replay,
                preparations=preparations,
            )
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_CONTENT_CANDIDATE)
        _require_unsealed_component(
            work,
            authority.analysis_id,
            _COMPONENT_CONTENT_CANDIDATE,
        )
        last, live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=True,
        )
        rows = _content_candidate_validation_keys(
            work,
            authority,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        exact_preparations = _require_validation_preparations(
            work,
            authority,
            selected,
            preparations,
        )
        for (raw_gallery_id,), preparation in zip(
            selected,
            exact_preparations,
            strict=True,
        ):
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="content candidate validation gallery_id",
            )
            target = (
                None
                if preparation is None
                else _content_candidate_from_preparation(preparation)
            )
            parent = _resolved_content_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            shadow = _shadow_content_candidate(work, authority.analysis_id, gallery_id)
            tombstone = _has_key(
                work,
                "catalog_analysis_content_owner_candidate_tombstones",
                "gallery_id",
                authority.analysis_id,
                gallery_id,
            )
            _require_overlay_exact(
                label="content candidate",
                overlay_depth=authority.overlay_depth,
                target=target,
                parent=parent,
                shadow=shadow,
                tombstone=tombstone,
            )
            if (
                _resolved_content_candidate(work, authority.analysis_id, gallery_id)
                != target
            ):
                raise AnalysisCorruptionError(
                    "resolved content candidate differs from the full evaluator"
                )
            if target is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="validated content candidate live row count",
                )
        return _finish_component_validation(
            work,
            authority=authority,
            stage=_STAGE_VALIDATE_CONTENT_CANDIDATE,
            component=_COMPONENT_CONTENT_CANDIDATE,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor_kind=_CURSOR_GALLERY,
            next_key=(
                last
                if not selected
                else require_positive_int63(
                    selected[-1][0],
                    field="content candidate validation cursor",
                ).to_bytes(8, "big")
            ),
            selected_count=len(selected),
            live_count=live_count,
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_content_owner_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_CONTENT_OWNER,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_component_sealed(
            work,
            authority.analysis_id,
            _COMPONENT_CONTENT_CANDIDATE,
        )
        last, _live_count = _decode_cursor(
            _CURSOR_DIGEST,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_content_rows(
            work,
            authority.analysis_id,
            after=last,
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for (raw_content,) in selected:
            content = require_digest32(raw_content, field="content owner work key")
            target = _evaluate_content_owner(work, authority, content)
            parent = _resolved_content_owner(
                work,
                authority.baseline_analysis_id,
                content,
            )
            _materialize_content_owner(work, authority, content, target, parent)
        next_key = (
            last
            if not selected
            else require_digest32(selected[-1][0], field="content owner cursor")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_CONTENT_OWNER,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_DIGEST, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def validate_content_owner_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_VALIDATE_CONTENT_OWNER,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_CONTENT_OWNER)
        _require_unsealed_component(
            work,
            authority.analysis_id,
            _COMPONENT_CONTENT_OWNER,
        )
        last, live_count = _decode_cursor(
            _CURSOR_DIGEST,
            checkpoint.cursor,
            live=True,
        )
        rows = _content_owner_validation_keys(
            work,
            authority,
            after=last,
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for (raw_content,) in selected:
            content = require_digest32(
                raw_content,
                field="content owner validation key",
            )
            target = _evaluate_content_owner(work, authority, content)
            parent = _resolved_content_owner(
                work,
                authority.baseline_analysis_id,
                content,
            )
            shadow = _shadow_content_owner(work, authority.analysis_id, content)
            tombstone = _has_key(
                work,
                "catalog_analysis_content_owner_tombstones",
                "content_sha256",
                authority.analysis_id,
                content,
            )
            _require_overlay_exact(
                label="content owner",
                overlay_depth=authority.overlay_depth,
                target=target,
                parent=parent,
                shadow=shadow,
                tombstone=tombstone,
            )
            if _resolved_content_owner(work, authority.analysis_id, content) != target:
                raise AnalysisCorruptionError(
                    "resolved content owner differs from the full evaluator"
                )
            if target is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="validated content owner live row count",
                )
        return _finish_component_validation(
            work,
            authority=authority,
            stage=_STAGE_VALIDATE_CONTENT_OWNER,
            component=_COMPONENT_CONTENT_OWNER,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor_kind=_CURSOR_DIGEST,
            next_key=(
                last
                if not selected
                else require_digest32(
                    selected[-1][0],
                    field="content owner validation cursor",
                )
            ),
            selected_count=len(selected),
            live_count=live_count,
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_impacted_gid_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_IMPACTED_GID,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_component_sealed(
            work,
            authority.analysis_id,
            _COMPONENT_CONTENT_OWNER,
        )
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        gallery_ids = tuple(
            require_positive_int63(
                row[0],
                field="impacted GID gallery_id",
            )
            for row in selected
        )
        impact_page = _load_gid_impact_page(work, authority, gallery_ids)
        provenance: list[tuple[int, int]] = []
        for gallery_id in gallery_ids:
            old_gid = impact_page.old_gids[gallery_id]
            new_gid = impact_page.current_gids[gallery_id]
            gids = {gid for gid in (old_gid, new_gid) if gid is not None}
            provenance.extend((gallery_id, gid) for gid in sorted(gids))
        try:
            if selected:
                provenance_receipt = prepare_analysis_impacted_gid_provenance_page(
                    work.connector,
                    analysis_id=authority.analysis_id,
                    entries=tuple(provenance),
                )
                apply_analysis_impacted_gid_provenance_page(
                    work.connector,
                    provenance_receipt,
                )
            else:
                require_exact_analysis_impacted_gid_provenance_page(
                    work.connector,
                    analysis_id=authority.analysis_id,
                    after_gallery_id=(
                        None if last is None else int.from_bytes(last, "big")
                    ),
                    through_gallery_id=None,
                    expected=(),
                )
                require_complete_analysis_impacted_gid_keyspace(
                    work.connector,
                    analysis_id=authority.analysis_id,
                )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="impacted GID cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_IMPACTED_GID,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_gid_candidate_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_GID_CANDIDATE,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(
                work,
                authority,
                replay,
                preparations=preparations,
            )
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_IMPACTED_GID)
        last, _live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        exact_preparations = _require_transition_preparations(
            work,
            authority,
            selected,
            preparations,
        )
        for (raw_gallery_id,), preparation in zip(
            selected,
            exact_preparations,
            strict=True,
        ):
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="GID candidate gallery_id",
            )
            target = (
                None
                if preparation is None
                else _gid_candidate_from_preparation(
                    work,
                    authority,
                    preparation,
                )
            )
            parent = _resolved_gid_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            _materialize_gid_candidate(work, authority, gallery_id, target, parent)
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="GID candidate cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_GID_CANDIDATE,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GALLERY, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def validate_gid_candidate_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        preparations: Sequence[AnalysisGalleryPreparation | None],
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_VALIDATE_GID_CANDIDATE,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(
                work,
                authority,
                replay,
                preparations=preparations,
            )
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_GID_CANDIDATE)
        _require_unsealed_component(
            work,
            authority.analysis_id,
            _COMPONENT_GID_CANDIDATE,
        )
        last, live_count = _decode_cursor(
            _CURSOR_GALLERY,
            checkpoint.cursor,
            live=True,
        )
        rows = _gid_candidate_validation_keys(
            work,
            authority,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        exact_preparations = _require_validation_preparations(
            work,
            authority,
            selected,
            preparations,
        )
        for (raw_gallery_id,), preparation in zip(
            selected,
            exact_preparations,
            strict=True,
        ):
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="GID candidate validation gallery_id",
            )
            target = (
                None
                if preparation is None
                else _gid_candidate_from_preparation(
                    work,
                    authority,
                    preparation,
                )
            )
            parent = _resolved_gid_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            shadow = _shadow_gid_candidate(work, authority.analysis_id, gallery_id)
            tombstone = _has_key(
                work,
                "catalog_analysis_gid_candidate_tombstones",
                "gallery_id",
                authority.analysis_id,
                gallery_id,
            )
            _require_overlay_exact(
                label="GID candidate",
                overlay_depth=authority.overlay_depth,
                target=target,
                parent=parent,
                shadow=shadow,
                tombstone=tombstone,
            )
            if (
                _resolved_gid_candidate(work, authority.analysis_id, gallery_id)
                != target
            ):
                raise AnalysisCorruptionError(
                    "resolved GID candidate differs from the full evaluator"
                )
            if target is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="validated GID candidate live row count",
                )
        return _finish_component_validation(
            work,
            authority=authority,
            stage=_STAGE_VALIDATE_GID_CANDIDATE,
            component=_COMPONENT_GID_CANDIDATE,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor_kind=_CURSOR_GALLERY,
            next_key=(
                last
                if not selected
                else require_positive_int63(
                    selected[-1][0],
                    field="GID candidate validation cursor",
                ).to_bytes(8, "big")
            ),
            selected_count=len(selected),
            live_count=live_count,
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def process_gid_winner_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_GID_WINNER,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_component_sealed(
            work,
            authority.analysis_id,
            _COMPONENT_GID_CANDIDATE,
        )
        last, _live_count = _decode_cursor(
            _CURSOR_GID,
            checkpoint.cursor,
            live=False,
        )
        rows = _workset_gid_rows(
            work,
            authority.analysis_id,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for (raw_gid,) in selected:
            gid = require_positive_int63(raw_gid, field="GID winner work key")
            target = _evaluate_gid_winner(work, authority, gid)
            parent = _resolved_gid_winner(
                work,
                authority.baseline_analysis_id,
                gid,
            )
            _materialize_gid_winner(work, authority, gid, target, parent)
        if not selected:
            _require_complete_gid_winner_keyspace(work, authority.analysis_id)
        next_key = (
            last
            if not selected
            else require_positive_int63(
                selected[-1][0],
                field="GID winner cursor",
            ).to_bytes(8, "big")
        )
        return _commit_batch(
            work,
            authority=authority,
            stage=_STAGE_GID_WINNER,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor=_encode_cursor(_CURSOR_GID, next_key),
            row_count=len(selected),
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def validate_gid_winner_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        batch_key: bytes,
        max_rows: int,
        now: int,
    ) -> AnalysisBatchResult:
        authority, checkpoint, replay = _prepare_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            stage=_STAGE_VALIDATE_GID_WINNER,
            batch_key=batch_key,
            max_rows=max_rows,
            now=now,
        )
        if replay is not None:
            _validate_batch_replay(work, authority, replay)
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_GID_WINNER)
        _require_unsealed_component(
            work,
            authority.analysis_id,
            _COMPONENT_GID_WINNER,
        )
        last, live_count = _decode_cursor(
            _CURSOR_GID,
            checkpoint.cursor,
            live=True,
        )
        rows = _gid_winner_validation_keys(
            work,
            authority,
            after=None if last is None else int.from_bytes(last, "big"),
            limit=checkpoint.page_limit + 1,
        )
        selected = rows[: checkpoint.page_limit]
        for (raw_gid,) in selected:
            gid = require_positive_int63(raw_gid, field="GID winner validation key")
            target = _evaluate_gid_winner(work, authority, gid)
            parent = _resolved_gid_winner(
                work,
                authority.baseline_analysis_id,
                gid,
            )
            shadow = _shadow_gid_winner(work, authority.analysis_id, gid)
            tombstone = _has_key(
                work,
                "catalog_analysis_gid_winner_tombstones",
                "gid",
                authority.analysis_id,
                gid,
            )
            _require_overlay_exact(
                label="GID winner",
                overlay_depth=authority.overlay_depth,
                target=target,
                parent=parent,
                shadow=shadow,
                tombstone=tombstone,
            )
            if _resolved_gid_winner(work, authority.analysis_id, gid) != target:
                raise AnalysisCorruptionError(
                    "resolved GID winner differs from the full evaluator"
                )
            if target is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="validated GID winner live row count",
                )
        if not selected:
            _require_complete_gid_winner_keyspace(work, authority.analysis_id)
        return _finish_component_validation(
            work,
            authority=authority,
            stage=_STAGE_VALIDATE_GID_WINNER,
            component=_COMPONENT_GID_WINNER,
            batch_key=batch_key,
            checkpoint=checkpoint,
            cursor_kind=_CURSOR_GID,
            next_key=(
                last
                if not selected
                else require_positive_int63(
                    selected[-1][0],
                    field="GID winner validation cursor",
                ).to_bytes(8, "big")
            ),
            selected_count=len(selected),
            live_count=live_count,
            terminal=not selected,
            now=now,
        )

    @staticmethod
    def prepare_snapshot_manifest(
        connector: SQLConnector,
        *,
        backend: str,
        authority: AnalysisPreparationAuthority,
    ) -> AnalysisSnapshotPreparation:
        """Build the exact output in a transaction-independent read snapshot."""

        if not isinstance(authority, AnalysisPreparationAuthority):
            raise TypeError("authority must be AnalysisPreparationAuthority")
        authority.__post_init__()
        if {row[0] for row in authority.component_seals} != ANALYSIS_COMPONENTS:
            raise AnalysisNotReadyError(
                "snapshot preparation authority does not carry all five seals"
            )
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            run = _load_preparation_authority(
                work,
                authority,
                allow_complete=True,
            )
            _require_exact_component_seals(work, run.analysis_id)
            state = work.connector.fetch_one(
                "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
                (run.analysis_id,),
            )
            binding = work.connector.fetch_one(
                "SELECT snapshot_manifest_sha256 "
                "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
                (run.analysis_id,),
            )
            if state == ("OPEN",) and binding:
                raise AnalysisCorruptionError(
                    "OPEN analysis already has a snapshot-manifest binding"
                )
            if state == ("COMPLETE",) and len(binding) != 1:
                raise AnalysisCorruptionError(
                    "COMPLETE analysis lacks its atomic snapshot binding"
                )
            if state not in {("OPEN",), ("COMPLETE",)}:
                raise AnalysisNotReadyError(
                    "snapshot preparation requires OPEN or COMPLETE analysis"
                )
            preparation = _prepare_snapshot_manifest(work, run, authority)
            if state == ("COMPLETE",) and binding != (
                preparation.upload_plan.value_sha256,
            ):
                preparation.close()
                raise AnalysisCorruptionError(
                    "rebuilt snapshot digest differs from the COMPLETE binding"
                )
            return preparation

    @staticmethod
    def handoff_snapshot_manifest(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        preparation: AnalysisSnapshotPreparation,
        now: int,
    ) -> bytes:
        """Bind the canonical output and mark COMPLETE in one short transaction."""

        if not isinstance(preparation, AnalysisSnapshotPreparation):
            raise TypeError("preparation must be AnalysisSnapshotPreparation")
        preparation.__post_init__()
        if preparation._capability is not _PREPARATION_TOKEN:
            raise TypeError("snapshot preparation is not repository-issued")

        authorization_time = require_int63(now, field="snapshot handoff now")
        authority = _authorize_analysis(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=preparation.analysis_id,
            now=authorization_time,
            allow_complete=True,
        )
        if (
            authority.analysis_id != preparation.analysis_id
            or authority.build_id != preparation.build_id
        ):
            raise AnalysisCorruptionError(
                "snapshot preparation differs from its durable analysis"
            )
        _validate_authority_receipt(
            work,
            authority,
            preparation.authority,
        )
        _require_exact_component_seals(work, authority.analysis_id)
        try:
            run_family = load_analysis_run_family(
                work.connector,
                analysis_id=authority.analysis_id,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if run_family is None:
            raise AnalysisCorruptionError("snapshot handoff lost its analysis run")
        state_row = (run_family.state, run_family.completed_at)
        binding = work.connector.fetch_one(
            "SELECT snapshot_manifest_sha256 FROM catalog_analysis_snapshot_manifest "
            "WHERE analysis_id = %s",
            (authority.analysis_id,),
        )
        value = preparation.upload_plan.value_sha256
        generation = require_positive_int63(
            ingest_turn.generation,
            field="snapshot handoff generation",
        )
        expected_counts = (
            preparation.gallery_count,
            preparation.file_count,
            preparation.byte_count,
        )
        component_seals = _component_seal_receipts(work, authority.analysis_id)
        if state_row and state_row[0] == "COMPLETE":
            completed_at = require_int63(state_row[1], field="analysis completed_at")
            if (
                completed_at < run_family.started_at
                or any(
                    completed_at < sealed_at
                    for _component, _row_count, sealed_at in component_seals
                )
                or binding != (value,)
            ):
                raise AnalysisCorruptionError(
                    "completed analysis snapshot replay differs from its binding"
                )
            if work.connector.fetch_one(
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (generation, value),
            ):
                raise AnalysisCorruptionError(
                    "completed snapshot replay retained its upload claim"
                )
            _require_snapshot_canonical_identity(
                work,
                preparation,
                missing_is_corruption=True,
            )
            try:
                identity = load_snapshot_manifest_family(
                    work.connector,
                    snapshot_manifest_sha256=value,
                )
            except ManifestFamilyCollisionError as error:
                raise AnalysisCorruptionError(str(error)) from error
            if (
                identity is None
                or (
                    identity.gallery_count,
                    identity.file_count,
                    identity.byte_count,
                )
                != expected_counts
            ):
                raise AnalysisCorruptionError(
                    "completed snapshot replay differs from its sealed count family"
                )
            return value
        if state_row != ("OPEN", None) or binding:
            raise AnalysisCorruptionError(
                "snapshot binding and analysis state are not atomic"
            )

        _require_snapshot_canonical_identity(work, preparation)

        timestamp = database_unix_microseconds(work)
        if timestamp < run_family.started_at or any(
            timestamp < sealed_at
            for _component, _row_count, sealed_at in component_seals
        ):
            raise AnalysisNotReadyError(
                "database completion time precedes analysis or component sealing"
            )

        claim = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("analysis-snapshot-upload", generation, value),
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value),
        )
        if claim != (generation, value):
            raise AnalysisNotReadyError(
                "snapshot handoff requires the exact live-generation upload claim"
            )
        try:
            ensure_snapshot_manifest_family(
                work.connector,
                snapshot_manifest_sha256=value,
                gallery_count=preparation.gallery_count,
                file_count=preparation.file_count,
                byte_count=preparation.byte_count,
            )
        except ManifestFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        work.connector.execute(
            "INSERT INTO catalog_analysis_snapshot_manifest "
            "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
            (authority.analysis_id, value),
        )
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value),
        )
        if deleted != 1:
            raise AnalysisCorruptionError(
                "snapshot upload claim changed during consumer handoff"
            )
        insert_analysis_run_completed_at(
            work.connector,
            analysis_id=authority.analysis_id,
            completed_at=timestamp,
        )
        cas_analysis_run_state(
            work,
            analysis_id=authority.analysis_id,
            previous="OPEN",
            successor="COMPLETE",
            authority="analysis snapshot completion",
        )
        return value

    @staticmethod
    def complete(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        now: int,
    ) -> None:
        """Fail closed unless canonical handoff already completed atomically."""

        authority = _authorize_analysis(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=analysis_id,
            now=require_int63(now, field="analysis completion check now"),
            allow_complete=True,
        )
        _require_exact_component_seals(work, authority.analysis_id)
        try:
            family = load_analysis_run_family(
                work.connector,
                analysis_id=authority.analysis_id,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        binding = work.connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
            (authority.analysis_id,),
        )
        if family is not None and family.state == "COMPLETE" and len(binding) == 1:
            require_digest32(binding[0], field="analysis snapshot_manifest_sha256")
            return
        raise AnalysisUnsupportedError(
            "analysis completion requires atomic canonical snapshot handoff"
        )


def _authorize(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    gate = MaintenanceGateRepository.lock_and_require_live(work, gate_lease, now=now)
    if gate.mode is not GateMode.SHARED:
        raise AnalysisNotReadyError("analysis writes require a live SHARED gate")
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(turn.generation, field="analysis ingest generation")


def _authorize_analysis(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    analysis_id: bytes,
    now: int,
    allow_complete: bool = False,
) -> _RunAuthority:
    analysis = require_uuid16(analysis_id, field="analysis_id")
    generation = _authorize(work, gate_lease, ingest_turn, now=now)
    state_row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("analysis-run", analysis),
        "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
        (analysis,),
    )
    if len(state_row) != 1:
        raise AnalysisNotReadyError("analysis run is missing")
    try:
        family = load_analysis_run_family(
            work.connector,
            analysis_id=analysis,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        raise AnalysisCorruptionError("analysis run descriptor family is missing")
    state = _require_run_state(state_row[0])
    if family.state != state:
        raise AnalysisCorruptionError("analysis run state changed during its lock")
    if state != "OPEN" and not (allow_complete and state == "COMPLETE"):
        raise AnalysisNotReadyError(f"analysis run is not writable: {state}")
    build = family.build_id
    policy_id = family.policy_id
    _require_generation_mapping(work, generation=generation, build_id=build)
    working = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
        (1,),
    )
    if working != (build,):
        raise AnalysisNotReadyError("analysis no longer owns the source working slot")
    baseline_row = work.connector.fetch_one(
        "SELECT base_analysis_id FROM catalog_analysis_baselines "
        "WHERE analysis_id = %s",
        (analysis,),
    )
    baseline = (
        None
        if not baseline_row
        else require_uuid16(baseline_row[0], field="analysis baseline_analysis_id")
    )
    _persisted_baseline, _anchor, depth, _ancestry = _load_layout(work, analysis)
    if depth > _MAX_OVERLAY_DEPTH:
        raise AnalysisCorruptionError("analysis overlay depth exceeds 16")
    return _RunAuthority(
        analysis, build, _load_policy(work, policy_id), baseline, depth
    )


def _require_generation_mapping(
    work: VNextUnitOfWork,
    *,
    generation: int,
    build_id: bytes,
) -> None:
    mapping = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (generation,),
    )
    if mapping != (build_id,):
        raise AnalysisNotReadyError(
            "analysis build is not mapped to the exact live ingest generation"
        )


def _load_policy(work: VNextUnitOfWork, policy_id: int) -> _Policy:
    row = work.connector.fetch_one(
        "SELECT algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version FROM catalog_analysis_policies "
        "WHERE policy_id = %s",
        (policy_id,),
    )
    if len(row) != 5:
        raise AnalysisNotReadyError("analysis policy is missing or incomplete")
    return _Policy(
        require_positive_int63(policy_id, field="analysis policy_id"),
        require_uint32(row[0], field="analysis algorithm_version"),
        require_int63(row[1], field="spam_artist_threshold"),
        require_int63(row[2], field="spam_occurrence_threshold"),
        require_uint32(row[3], field="content_owner_rule_version"),
        require_uint32(row[4], field="gid_winner_rule_version"),
    )


def _analysis_input_digest(
    manifest_sha256: bytes,
    counts: tuple[int, ...],
    policy: _Policy,
) -> bytes:
    if len(counts) != 3:
        raise ValueError("build manifest requires exactly three aggregate counts")
    payload = bytearray(b"h2hdb-vnext-analysis-input\0")
    payload.extend(require_digest32(manifest_sha256, field="manifest_sha256"))
    for index, count in enumerate(counts):
        payload.extend(
            require_int63(count, field=f"manifest count {index}").to_bytes(8, "big")
        )
    payload.extend(policy.algorithm_version.to_bytes(4, "big"))
    payload.extend(policy.spam_artist_threshold.to_bytes(8, "big"))
    payload.extend(policy.spam_occurrence_threshold.to_bytes(8, "big"))
    payload.extend(policy.content_owner_rule_version.to_bytes(4, "big"))
    payload.extend(policy.gid_winner_rule_version.to_bytes(4, "big"))
    return sha256(payload).digest()


def _preparation_authority_receipt(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    generation: int,
) -> AnalysisPreparationAuthority:
    try:
        run_family = load_analysis_run_family(
            work.connector,
            analysis_id=authority.analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if run_family is None:
        raise AnalysisCorruptionError("preparation authority lost its run")
    if (
        run_family.build_id != authority.build_id
        or run_family.policy_id != authority.policy.policy_id
    ):
        raise AnalysisCorruptionError(
            "preparation authority differs from its analysis run"
        )
    return AnalysisPreparationAuthority(
        authority.analysis_id,
        authority.build_id,
        require_positive_int63(
            generation,
            field="preparation authority generation",
        ),
        authority.policy.policy_id,
        run_family.input_manifest_sha256,
        _component_seal_receipts(work, authority.analysis_id),
        _PREPARATION_TOKEN,
    )


def _issued_gallery_memberships(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    stage: bytes,
    checkpoint: _Checkpoint,
) -> tuple[tuple[int, int | None], ...]:
    """Load one exact server-keyset page and its current build membership."""

    if stage not in _GALLERY_PREPARATION_STAGES:
        raise ValueError("stage does not use gallery preparations")
    last, _live_count = _decode_cursor(
        _CURSOR_GALLERY,
        checkpoint.cursor,
        live=stage
        in {
            _STAGE_VALIDATE_CONTENT_CANDIDATE,
            _STAGE_VALIDATE_GID_CANDIDATE,
        },
    )
    after = None if last is None else int.from_bytes(last, "big")
    if stage in {
        _STAGE_IMPACTED_CONTENT,
        _STAGE_CONTENT_CANDIDATE,
        _STAGE_GID_CANDIDATE,
    }:
        rows = _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=after,
            limit=checkpoint.page_limit + 1,
        )
    elif stage == _STAGE_VALIDATE_CONTENT_CANDIDATE:
        rows = _content_candidate_validation_keys(
            work,
            authority,
            after=after,
            limit=checkpoint.page_limit + 1,
        )
    else:
        rows = _gid_candidate_validation_keys(
            work,
            authority,
            after=after,
            limit=checkpoint.page_limit + 1,
        )
    selected = rows[: checkpoint.page_limit]
    gallery_ids = tuple(
        require_positive_int63(row[0], field="issued preparation gallery_id")
        for row in selected
    )
    memberships = _current_memberships_for_page(work, authority, gallery_ids)
    if set(memberships) != set(gallery_ids):
        raise AnalysisCorruptionError(
            "issued gallery membership differs from its exact keyset"
        )
    return tuple((gallery_id, memberships[gallery_id]) for gallery_id in gallery_ids)


def _prepare_gallery(
    work: VNextUnitOfWork,
    run: _RunAuthority,
    gallery_id: int,
    preparation_authority: AnalysisPreparationAuthority,
) -> AnalysisGalleryPreparation:
    row = work.connector.fetch_one(
        "SELECT member.observation_id, metadata.gid, metadata.download_time "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_metadata AS metadata "
        "ON metadata.gallery_id = member.gallery_id "
        "AND metadata.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND member.gallery_id = %s",
        (run.build_id, gallery_id),
    )
    if len(row) != 3:
        raise AnalysisNotReadyError(
            "gallery preparation requires exact sealed membership and metadata"
        )
    observation_id = require_positive_int63(
        row[0],
        field="preparation observation_id",
    )
    persisted_gid = require_positive_int63(row[1], field="preparation gid")
    persisted_download = require_int63(
        row[2],
        field="preparation download_time",
    )
    metadata_gid, download_time, title_scalar_receipt = _metadata_comparator_facts(
        work,
        gallery_id,
        observation_id,
    )
    if (metadata_gid, download_time) != (persisted_gid, persisted_download):
        raise AnalysisCorruptionError(
            "normalized metadata scalars differ from the exact metadata stream"
        )
    marker = _gallery_has_already_uploaded_marker(
        work,
        gallery_id,
        observation_id,
    )
    content_prefer_not_already_uploaded: int | None = None
    content_title_scalar_count: int | None = None
    content_download_time: int | None = None
    content_plan: CanonicalValueUploadPlan | None = None
    content_sha256: bytes | None = None
    content_count = sum(
        1
        for _digest in _iter_effective_content_digests(
            work,
            run,
            gallery_id,
            observation_id,
        )
    )
    if content_count:
        content_plan = CanonicalValueUploadPlan.from_parts(
            _EFFECTIVE_CONTENT_DOMAIN.decode("ascii"),
            iter_effective_content_payload_ordered(
                content_count,
                _iter_effective_content_digests(
                    work,
                    run,
                    gallery_id,
                    observation_id,
                ),
            ),
        )
        reference_digest = effective_content_digest_ordered(
            content_count,
            _iter_effective_content_digests(
                work,
                run,
                gallery_id,
                observation_id,
            ),
        )
        if content_plan.value_sha256 != reference_digest:
            content_plan.close()
            raise AnalysisCorruptionError(
                "effective-content upload plan differs from the registered codec"
            )
        content_sha256 = content_plan.value_sha256
        content_prefer_not_already_uploaded = int(not marker)
        content_title_scalar_count = title_scalar_receipt.scalar_count
        content_download_time = download_time
    return AnalysisGalleryPreparation(
        run.analysis_id,
        run.build_id,
        gallery_id,
        observation_id,
        persisted_gid,
        content_sha256,
        content_prefer_not_already_uploaded,
        content_title_scalar_count,
        content_download_time,
        content_plan,
        preparation_authority,
        _PREPARATION_TOKEN,
    )


def _iter_effective_content_digests(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    observation_id: int,
) -> Iterator[bytes]:
    previous_digest: bytes | None = None
    previous_file_no = 0
    while True:
        if previous_digest is None:
            predicate = ""
            parameters: tuple[Any, ...] = (
                gallery_id,
                observation_id,
                b"galleryinfo.txt",
                _MAX_BATCH_ROWS,
            )
        else:
            predicate = (
                " AND (source_sha.file_sha256 > %s OR "
                "(source_sha.file_sha256 = %s AND source_no.file_no > %s))"
            )
            parameters = (
                gallery_id,
                observation_id,
                b"galleryinfo.txt",
                previous_digest,
                previous_digest,
                previous_file_no,
                _MAX_BATCH_ROWS,
            )
        rows = work.connector.fetch_all(
            "SELECT source_sha.file_sha256, source_no.file_no "
            "FROM catalog_gallery_observation_file_seals AS source_seal "
            "JOIN catalog_gallery_observation_file_file_nos AS source_no "
            "ON source_no.gallery_id = source_seal.gallery_id "
            "AND source_no.observation_id = source_seal.observation_id "
            "AND source_no.file_key = source_seal.file_key "
            "JOIN catalog_gallery_observation_file_file_sha256s AS source_sha "
            "ON source_sha.gallery_id = source_seal.gallery_id "
            "AND source_sha.observation_id = source_seal.observation_id "
            "AND source_sha.file_key = source_seal.file_key "
            "JOIN catalog_file_name_identities AS name "
            "ON name.file_key = source_seal.file_key "
            "WHERE source_seal.gallery_id = %s "
            "AND source_seal.observation_id = %s AND name.name_bytes <> %s"
            + predicate
            + " ORDER BY source_sha.file_sha256, source_no.file_no LIMIT %s",
            parameters,
        )
        if not rows:
            return
        for raw_digest, raw_file_no in rows:
            digest = require_digest32(raw_digest, field="effective file_sha256")
            file_no = require_int63(raw_file_no, field="effective file_no")
            decision = _resolved_decision(work, authority.analysis_id, digest)
            if decision is None:
                raise AnalysisCorruptionError(
                    "sealed file-decision component omitted a CONTENT hash"
                )
            if not _excluded(decision, authority.policy):
                yield digest
            previous_digest = digest
            previous_file_no = file_no
        if len(rows) < _MAX_BATCH_ROWS:
            return


class _PartReader:
    def __init__(self, parts: Iterable[bytes]) -> None:
        self._parts = iter(parts)
        self._carry = b""

    def read_exact(self, size: int) -> bytes:
        if size < 0:
            raise ValueError("stream read size is negative")
        result = bytearray()
        for piece in self.iter_exact(size):
            result.extend(piece)
        return bytes(result)

    def iter_exact(self, size: int) -> Iterator[bytes]:
        remaining = size
        while remaining:
            if not self._carry:
                try:
                    self._carry = require_bounded_bytes(
                        next(self._parts),
                        field="metadata stream chunk",
                        minimum=1,
                        maximum=64 * 1024,
                    )
                except StopIteration as error:
                    raise AnalysisCorruptionError(
                        "metadata stream ended before declared title bytes"
                    ) from error
            amount = min(remaining, len(self._carry))
            yield self._carry[:amount]
            self._carry = self._carry[amount:]
            remaining -= amount


def _metadata_comparator_facts(
    work: VNextUnitOfWork,
    gallery_id: int,
    observation_id: int,
) -> tuple[int, int, AnalysisTitleScalarReceipt]:
    decoder = GalleryObservationMetadataDecoder()
    for chunk in _iter_metadata_chunks(work, gallery_id, observation_id):
        decoder.feed(chunk)
    receipt = decoder.finish()

    reader = _PartReader(_iter_metadata_chunks(work, gallery_id, observation_id))
    if reader.read_exact(len(_METADATA_PREFIX)) != _METADATA_PREFIX:
        raise AnalysisCorruptionError("metadata stream prefix changed after seal")
    if int.from_bytes(reader.read_exact(4), "big") != 1:
        raise AnalysisCorruptionError("metadata stream codec version is not v1")
    gid = require_positive_int63(
        int.from_bytes(reader.read_exact(8), "big"),
        field="streamed metadata gid",
    )
    if reader.read_exact(1) != b"\x01":
        raise AnalysisCorruptionError("metadata title tag is not canonical")
    title_bytes = require_int63(
        int.from_bytes(reader.read_exact(8), "big"),
        field="metadata title byte count",
    )
    try:
        scalar_receipt = count_analysis_title_scalars(reader.iter_exact(title_bytes))
    except ValueError as error:
        raise AnalysisCorruptionError("metadata title is not strict UTF-8") from error
    if (
        gid != receipt.gid
        or title_bytes != receipt.title_byte_count
        or receipt.download_time is None
    ):
        raise AnalysisCorruptionError(
            "streamed comparator facts differ from metadata validation receipt"
        )
    if scalar_receipt.byte_count != title_bytes:
        raise AnalysisCorruptionError("title scalar receipt changed byte count")
    return gid, receipt.download_time, scalar_receipt


def _iter_metadata_chunks(
    work: VNextUnitOfWork,
    gallery_id: int,
    observation_id: int,
) -> Iterator[bytes]:
    roots = work.connector.fetch_all(
        "SELECT root.root_page_sha256 "
        "FROM catalog_gallery_observation_tree_roots AS root "
        "JOIN catalog_gallery_observation_page_descriptor_seals AS seal "
        "ON seal.page_sha256 = root.root_page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_components AS descriptor "
        "ON descriptor.page_sha256 = seal.page_sha256 "
        "WHERE root.gallery_id = %s AND root.observation_id = %s "
        "AND descriptor.component = %s LIMIT 2",
        (gallery_id, observation_id, b"METADATA"),
    )
    if len(roots) != 1:
        raise AnalysisCorruptionError("sealed observation lacks one METADATA root")
    root = require_digest32(roots[0][0], field="metadata root_page_sha256")
    expected_offset = 0

    def visit(page_sha256: bytes, expected_level: int | None) -> Iterator[bytes]:
        nonlocal expected_offset
        row = work.connector.fetch_one(
            "SELECT page.page_bytes, descriptor.component, level.level, "
            "count.subtree_item_count "
            "FROM catalog_gallery_observation_page_descriptor_seals AS seal "
            "JOIN catalog_gallery_observation_pages AS page "
            "ON page.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_components "
            "AS descriptor ON descriptor.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_levels AS level "
            "ON level.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_subtree_item_counts "
            "AS count ON count.page_sha256 = seal.page_sha256 "
            "WHERE seal.page_sha256 = %s",
            (page_sha256,),
        )
        if len(row) != 4:
            raise AnalysisCorruptionError("metadata page or descriptor is missing")
        page_bytes = require_bounded_bytes(
            row[0],
            field="metadata page_bytes",
            minimum=1,
            maximum=64 * 1024,
        )
        if gallery_observation_page_digest(page_bytes) != page_sha256:
            raise AnalysisCorruptionError("metadata page digest differs from bytes")
        page = decode_gallery_observation_page(page_bytes)
        level = require_int63(row[2], field="metadata page level")
        count = require_int63(row[3], field="metadata page subtree count")
        if (
            row[1] != b"METADATA"
            or page.component is not GalleryObservationComponent.METADATA
            or page.level != level
            or page.subtree_item_count != count
            or (expected_level is not None and level != expected_level)
        ):
            raise AnalysisCorruptionError("metadata page descriptor differs from bytes")
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            for entry in page.entries:
                if not isinstance(entry, GalleryObservationMetadataChunk):
                    raise AnalysisCorruptionError(
                        "metadata leaf has a non-chunk record"
                    )
                if entry.byte_offset != expected_offset:
                    raise AnalysisCorruptionError(
                        "metadata leaf offsets are not exactly contiguous"
                    )
                expected_offset = _sum_int63(
                    expected_offset,
                    len(entry.chunk_bytes),
                    field="metadata stream byte offset",
                )
                yield entry.chunk_bytes
            return
        normalized = work.connector.fetch_all(
            "SELECT position, child_sha256 "
            "FROM catalog_gallery_observation_page_children "
            "WHERE parent_sha256 = %s ORDER BY position LIMIT 257",
            (page_sha256,),
        )
        encoded = []
        for position, entry in enumerate(page.entries):
            if not isinstance(entry, GalleryObservationBranchEntry):
                raise AnalysisCorruptionError("metadata branch has a leaf record")
            encoded.append((position, entry.child_sha256))
        if normalized != encoded:
            raise AnalysisCorruptionError(
                "normalized metadata child edges differ from exact page bytes"
            )
        for _position, child in normalized:
            yield from visit(
                require_digest32(child, field="metadata child_sha256"),
                level - 1,
            )

    yield from visit(root, None)


def _gallery_has_already_uploaded_marker(
    work: VNextUnitOfWork,
    gallery_id: int,
    observation_id: int,
) -> bool:
    rows = work.connector.fetch_all(
        "SELECT term.tag_value_sha256 "
        "FROM catalog_gallery_observation_tags AS observed "
        "JOIN catalog_tag_terms AS term ON term.tag_id = observed.tag_id "
        "WHERE observed.gallery_id = %s AND observed.observation_id = %s "
        "ORDER BY observed.position",
        (gallery_id, observation_id),
    )
    for (raw_value,) in rows:
        value = require_digest32(raw_value, field="tag value_sha256")
        position = 0
        matched = True

        def consume(part: bytes) -> None:
            nonlocal position, matched
            for byte in part:
                folded = byte + 32 if 65 <= byte <= 90 else byte
                if (
                    position >= len(ANALYSIS_ALREADY_UPLOADED_MARKER)
                    or folded != ANALYSIS_ALREADY_UPLOADED_MARKER[position]
                ):
                    matched = False
                position += 1

        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=value,
            consume_provisional=consume,
        )
        if receipt.digest_domain != b"tag_value_utf8_v1":
            raise AnalysisCorruptionError(
                "tag canonical stream has the wrong digest domain"
            )
        if receipt.byte_count != position:
            raise AnalysisCorruptionError("tag canonical stream count changed")
        if matched and position == len(ANALYSIS_ALREADY_UPLOADED_MARKER):
            return True
    return False


def _require_snapshot_canonical_identity(
    work: VNextUnitOfWork,
    preparation: AnalysisSnapshotPreparation,
    *,
    missing_is_corruption: bool = False,
) -> None:
    expected_parts = iter(
        part for part in preparation.upload_plan.iter_payload_parts() if part
    )
    expected_chunk = b""
    expected_offset = 0

    def compare_chunk(actual_chunk: bytes) -> None:
        nonlocal expected_chunk, expected_offset
        actual_offset = 0
        while actual_offset < len(actual_chunk):
            if expected_offset == len(expected_chunk):
                try:
                    expected_chunk = next(expected_parts)
                except StopIteration as error:
                    raise AnalysisCorruptionError(
                        "snapshot canonical payload exceeds its exact preparation"
                    ) from error
                expected_offset = 0
            compared = min(
                len(actual_chunk) - actual_offset,
                len(expected_chunk) - expected_offset,
            )
            if (
                actual_chunk[actual_offset : actual_offset + compared]
                != expected_chunk[expected_offset : expected_offset + compared]
            ):
                raise AnalysisCorruptionError(
                    "snapshot canonical payload differs byte-for-byte"
                )
            actual_offset += compared
            expected_offset += compared

    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=preparation.upload_plan.value_sha256,
            consume_provisional=compare_chunk,
        )
    except CanonicalValueNotReadyError as error:
        if not missing_is_corruption:
            raise AnalysisNotReadyError(
                "snapshot canonical identity is not sealed"
            ) from error
        raise AnalysisCorruptionError(
            "completed snapshot canonical payload is missing"
        ) from error
    except CanonicalValueCollisionError as error:
        raise AnalysisCorruptionError(
            "snapshot canonical payload is partial or corrupt"
        ) from error
    if expected_offset != len(expected_chunk) or next(expected_parts, None) is not None:
        raise AnalysisCorruptionError(
            "snapshot canonical payload ends before its exact preparation"
        )
    if (
        receipt.value_sha256 != preparation.upload_plan.value_sha256
        or receipt.digest_domain != _SNAPSHOT_DOMAIN
        or receipt.byte_count != preparation.payload_byte_count
    ):
        raise AnalysisCorruptionError(
            "snapshot canonical identity differs from the typed preparation"
        )


def _require_source_build_sealed(connector: SQLConnector, build_id: bytes) -> None:
    try:
        build = load_source_build_family(connector, build_id=build_id)
    except ManifestFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if build is None or build.state != "SEALED":
        raise AnalysisNotReadyError("preparation build is no longer SEALED")


def _prepare_snapshot_manifest(
    work: VNextUnitOfWork,
    run: _RunAuthority,
    preparation_authority: AnalysisPreparationAuthority,
) -> AnalysisSnapshotPreparation:
    try:
        manifest = load_build_manifest_family(
            work.connector,
            build_id=run.build_id,
        )
    except ManifestFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if manifest is None:
        raise AnalysisNotReadyError("snapshot preparation lost the build manifest")
    counts = SourceSnapshotCounts(
        manifest.gallery_count,
        manifest.file_count,
        manifest.byte_count,
    )
    policy = SourceSnapshotPolicy(
        run.policy.algorithm_version,
        run.policy.spam_artist_threshold,
        run.policy.spam_occurrence_threshold,
        run.policy.content_owner_rule_version,
        run.policy.gid_winner_rule_version,
    )

    gallery_entry_count = 0
    observed_files = 0
    observed_bytes = 0
    content_present_count = 0
    for gallery in _iter_snapshot_galleries(work, run):
        gallery_entry_count = _sum_int63(
            gallery_entry_count,
            1,
            field="snapshot gallery entry count",
        )
        observed_files = _sum_int63(
            observed_files,
            gallery.file_count,
            field="snapshot observed file count",
        )
        observed_bytes = _sum_int63(
            observed_bytes,
            gallery.byte_count,
            field="snapshot observed byte count",
        )
        if gallery.content_sha256 is not None:
            content_present_count = _sum_int63(
                content_present_count,
                1,
                field="snapshot content-present count",
            )
    if (
        gallery_entry_count != counts.gallery_count
        or observed_files != counts.file_count
        or observed_bytes != counts.byte_count
    ):
        raise AnalysisCorruptionError(
            "snapshot keyset preflight differs from sealed build counters"
        )
    decision_entry_count = sum(1 for _decision in _iter_snapshot_decisions(work, run))
    owner_entry_count = sum(1 for _owner in _iter_snapshot_owners(work, run))
    winner_entry_count = sum(1 for _winner in _iter_snapshot_winners(work, run))
    for field_name, value in (
        ("decision entry count", decision_entry_count),
        ("owner entry count", owner_entry_count),
        ("winner entry count", winner_entry_count),
    ):
        require_int63(value, field=f"snapshot {field_name}")
    payload_byte_count = (
        len(_SNAPSHOT_PREFIX)
        + 88
        + 73 * gallery_entry_count
        + 32 * content_present_count
        + 57 * decision_entry_count
        + 64 * owner_entry_count
        + 40 * winner_entry_count
    )
    require_int63(payload_byte_count, field="snapshot payload_byte_count")

    expected_digest = source_snapshot_manifest_digest_ordered(
        policy,
        counts,
        gallery_entry_count,
        _iter_snapshot_galleries(work, run),
        decision_entry_count,
        _iter_snapshot_decisions(work, run),
        owner_entry_count,
        _iter_snapshot_owners(work, run),
        winner_entry_count,
        _iter_snapshot_winners(work, run),
        payload_byte_count=payload_byte_count,
    )
    upload = CanonicalValueUploadPlan.from_parts(
        _SNAPSHOT_DOMAIN.decode("ascii"),
        iter_source_snapshot_manifest_payload_rows_ordered(
            policy,
            counts,
            gallery_entry_count,
            _iter_snapshot_galleries(work, run),
            decision_entry_count,
            _iter_snapshot_decisions(work, run),
            owner_entry_count,
            _iter_snapshot_owners(work, run),
            winner_entry_count,
            _iter_snapshot_winners(work, run),
        ),
    )
    if (
        upload.value_sha256 != expected_digest
        or upload.byte_count != payload_byte_count
    ):
        upload.close()
        raise AnalysisCorruptionError(
            "snapshot upload spool differs from its independent codec replay"
        )
    return AnalysisSnapshotPreparation(
        run.analysis_id,
        run.build_id,
        counts.gallery_count,
        counts.file_count,
        counts.byte_count,
        gallery_entry_count,
        decision_entry_count,
        owner_entry_count,
        winner_entry_count,
        payload_byte_count,
        upload,
        preparation_authority,
        _PREPARATION_TOKEN,
    )


def _iter_snapshot_galleries(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
) -> Iterator[SourceSnapshotGallery]:
    previous: bytes | None = None
    while True:
        predicate = "" if previous is None else " AND identity.gallery_key > %s"
        parameters: list[Any] = [authority.build_id]
        if previous is not None:
            parameters.append(previous)
        parameters.append(_MAX_BATCH_ROWS)
        rows = work.connector.fetch_all(
            "SELECT identity.gallery_key, observation.observation_identity_sha256, "
            "member.gallery_id, member.observation_id, metadata.gid, "
            "scan_count.source_file_count "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = member.gallery_id "
            "JOIN catalog_gallery_observations AS observation "
            "ON observation.gallery_id = member.gallery_id "
            "AND observation.observation_id = member.observation_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "JOIN catalog_gallery_observation_scans AS scan_count "
            "ON scan_count.gallery_id = member.gallery_id "
            "AND scan_count.observation_id = member.observation_id "
            "WHERE member.build_id = %s"
            + predicate
            + " ORDER BY identity.gallery_key LIMIT %s",
            tuple(parameters),
        )
        if not rows:
            return
        for row in rows:
            gallery_key = require_digest32(row[0], field="snapshot gallery_key")
            observation = require_digest32(
                row[1],
                field="snapshot observation_identity_sha256",
            )
            gallery_id = require_positive_int63(row[2], field="snapshot gallery_id")
            observation_id = require_positive_int63(
                row[3],
                field="snapshot observation_id",
            )
            gid = require_positive_int63(row[4], field="snapshot gid")
            declared_file_count = require_int63(
                row[5],
                field="snapshot source_file_count",
            )
            file_count, byte_count = _gallery_file_counts(
                work,
                gallery_id,
                observation_id,
            )
            if file_count != declared_file_count:
                raise AnalysisCorruptionError(
                    "snapshot gallery FILE rows differ from scan source_file_count"
                )
            candidate = _resolved_content_candidate(
                work,
                authority.analysis_id,
                gallery_id,
            )
            yield SourceSnapshotGallery(
                gallery_key,
                observation,
                None if candidate is None else candidate.content_sha256,
                gid,
                file_count,
                byte_count,
            )
            previous = gallery_key
        if len(rows) < _MAX_BATCH_ROWS:
            return


def _gallery_file_counts(
    work: VNextUnitOfWork,
    gallery_id: int,
    observation_id: int,
) -> tuple[int, int]:
    last_file_no = -1
    file_count = 0
    byte_count = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT source_no.file_no, content_blob.size_bytes "
            "FROM catalog_gallery_observation_file_seals AS source_seal "
            "JOIN catalog_gallery_observation_file_file_nos AS source_no "
            "ON source_no.gallery_id = source_seal.gallery_id "
            "AND source_no.observation_id = source_seal.observation_id "
            "AND source_no.file_key = source_seal.file_key "
            "JOIN catalog_gallery_observation_file_file_sha256s AS source_sha "
            "ON source_sha.gallery_id = source_seal.gallery_id "
            "AND source_sha.observation_id = source_seal.observation_id "
            "AND source_sha.file_key = source_seal.file_key "
            "JOIN catalog_content_blobs AS content_blob "
            "ON content_blob.file_sha256 = source_sha.file_sha256 "
            "WHERE source_seal.gallery_id = %s "
            "AND source_seal.observation_id = %s "
            "AND source_no.file_no > %s ORDER BY source_no.file_no LIMIT %s",
            (gallery_id, observation_id, last_file_no, _MAX_BATCH_ROWS),
        )
        if not rows:
            return file_count, byte_count
        for raw_file_no, raw_size in rows:
            last_file_no = require_int63(
                raw_file_no,
                field="snapshot gallery file_no",
            )
            if last_file_no != file_count:
                raise AnalysisCorruptionError(
                    "snapshot gallery FILE ordinals are not zero-based contiguous"
                )
            size = require_int63(raw_size, field="snapshot gallery file size")
            file_count = _sum_int63(
                file_count,
                1,
                field="snapshot gallery file_count",
            )
            byte_count = _sum_int63(
                byte_count,
                size,
                field="snapshot gallery byte_count",
            )
        if len(rows) < _MAX_BATCH_ROWS:
            return file_count, byte_count


def _iter_snapshot_decisions(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
) -> Iterator[SourceSnapshotFileHashDecision]:
    previous: bytes | None = None
    while True:
        predicate = "" if previous is None else " AND file_sha256 > %s"
        parameters: list[Any] = [authority.analysis_id]
        if previous is not None:
            parameters.append(previous)
        parameters.append(_MAX_BATCH_ROWS)
        rows = work.connector.fetch_all(
            "SELECT file_sha256, occurrence_count, artist_count, "
            "maximum_gallery_artist_count "
            "FROM catalog_analysis_file_hash_decision_resolved "
            "WHERE analysis_id = %s" + predicate + " ORDER BY file_sha256 LIMIT %s",
            tuple(parameters),
        )
        if not rows:
            return
        for row in rows:
            digest = require_digest32(row[0], field="snapshot decision file_sha256")
            decision = _decision_from_row(row[1:], field="snapshot decision")
            if decision is None:
                raise AnalysisCorruptionError("snapshot decision row disappeared")
            yield SourceSnapshotFileHashDecision(
                digest,
                decision.occurrence_count,
                decision.artist_count,
                decision.maximum_gallery_artist_count,
                bool(_excluded(decision, authority.policy)),
            )
            previous = digest
        if len(rows) < _MAX_BATCH_ROWS:
            return


def _iter_snapshot_owners(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
) -> Iterator[SourceSnapshotContentOwner]:
    previous: bytes | None = None
    while True:
        predicate = "" if previous is None else " AND owner.content_sha256 > %s"
        parameters: list[Any] = [authority.analysis_id]
        if previous is not None:
            parameters.append(previous)
        parameters.append(_MAX_BATCH_ROWS)
        rows = work.connector.fetch_all(
            "SELECT owner.content_sha256, owner.owner_gallery_id, "
            "identity.gallery_key "
            "FROM catalog_analysis_content_owner_resolved AS owner "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = owner.owner_gallery_id "
            "WHERE owner.analysis_id = %s"
            + predicate
            + " ORDER BY owner.content_sha256 LIMIT %s",
            tuple(parameters),
        )
        if not rows:
            return
        for raw_content, raw_gallery_id, raw_gallery_key in rows:
            content = require_digest32(raw_content, field="snapshot owner content")
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="snapshot owner gallery_id",
            )
            gallery_key = require_digest32(
                raw_gallery_key,
                field="snapshot owner gallery_key",
            )
            member = work.connector.fetch_one(
                "SELECT candidate.content_sha256 "
                "FROM catalog_source_build_galleries AS source "
                "JOIN catalog_analysis_content_owner_candidate_resolved AS candidate "
                "ON candidate.analysis_id = %s "
                "AND candidate.gallery_id = source.gallery_id "
                "WHERE source.build_id = %s AND source.gallery_id = %s",
                (authority.analysis_id, authority.build_id, gallery_id),
            )
            if member != (content,):
                raise AnalysisCorruptionError(
                    "snapshot owner is not a member of its exact content group"
                )
            yield SourceSnapshotContentOwner(content, gallery_key)
            previous = content
        if len(rows) < _MAX_BATCH_ROWS:
            return


def _iter_snapshot_winners(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
) -> Iterator[SourceSnapshotGidWinner]:
    previous = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT winner.gid, winner.winner_gallery_id, identity.gallery_key "
            "FROM catalog_analysis_gid_winner_resolved AS winner "
            "JOIN catalog_analysis_gid_candidate_resolved AS candidate "
            "ON candidate.analysis_id = winner.analysis_id "
            "AND candidate.gallery_id = winner.winner_gallery_id "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s "
            "AND member.gallery_id = winner.winner_gallery_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "AND metadata.gid = winner.gid "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = winner.winner_gallery_id "
            "WHERE winner.analysis_id = %s AND winner.gid > %s "
            "ORDER BY winner.gid LIMIT %s",
            (
                authority.build_id,
                authority.analysis_id,
                previous,
                _MAX_BATCH_ROWS,
            ),
        )
        if not rows:
            return
        for raw_gid, raw_gallery_id, raw_gallery_key in rows:
            gid = require_positive_int63(raw_gid, field="snapshot winner gid")
            require_positive_int63(
                raw_gallery_id,
                field="snapshot winner gallery_id",
            )
            gallery_key = require_digest32(
                raw_gallery_key,
                field="snapshot winner gallery_key",
            )
            yield SourceSnapshotGidWinner(gid, gallery_key)
            previous = gid
        if len(rows) < _MAX_BATCH_ROWS:
            return


def _load_committed_sibling_analysis(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
    policy_id: int,
) -> AnalysisRun | None:
    """Replay a build's COMPLETE analysis of another policy when it is already
    durable publication authority.

    The manifest forbids a different-policy sibling analysis of one build.  A
    session that resolved another policy after that build's publication
    commit became durable (DB_COMMITTED or PUBLISHED) therefore neither
    creates a sibling nor fails closed: the committed analysis is replayed
    under its own stored policy so the publication stage can finalize it.
    Every other policy mismatch stays the zero-write conflict below.
    """

    rows = work.connector.fetch_all(
        f"SELECT analysis_id FROM {_ANALYSIS_RUN_DESCRIPTOR_TABLE} "
        "WHERE build_id = %s ORDER BY analysis_id LIMIT 2",
        (build_id,),
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        return None
    try:
        family = load_analysis_run_family(
            work.connector, analysis_id=require_uuid16(rows[0][0], field="analysis_id")
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None or family.policy_id == policy_id or family.state != "COMPLETE":
        return None
    committed = work.connector.fetch_one(
        "SELECT committed.receipt_id FROM catalog_publication_commits AS committed "
        f"JOIN {_PUBLICATION_CANDIDATE_TABLE} AS candidate "
        "ON candidate.candidate_id = committed.candidate_id "
        "WHERE candidate.analysis_id = %s LIMIT 1",
        (family.analysis_id,),
    )
    if not committed:
        return None
    baseline, anchor, depth, _ancestry = _load_layout(work, family.analysis_id)
    return AnalysisRun(
        family.analysis_id,
        family.build_id,
        family.policy_id,
        family.input_manifest_sha256,
        baseline,
        anchor,
        depth,
        family.state,
        True,
    )


def _analysis_is_finalized(work: VNextUnitOfWork, *, analysis_id: bytes) -> bool:
    """Whether the analysis is the provenance of a finalized publication."""

    row = work.connector.fetch_one(
        f"SELECT 1 FROM {_SOURCE_REVISION_PROVENANCE_TABLE} AS provenance "
        f"JOIN {_PUBLICATION_COMMIT_TABLE} AS committed "
        "ON committed.source_revision = provenance.source_revision "
        "JOIN catalog_publication_commit_finalizations AS finalized "
        "ON finalized.receipt_id = committed.receipt_id "
        "WHERE provenance.analysis_id = %s LIMIT 1",
        (analysis_id,),
    )
    return bool(row)


def _derive_baseline(work: VNextUnitOfWork, *, build_id: bytes) -> bytes | None:
    baseline, revision, generation, channel = _derive_pinned_baseline(
        work,
        build_id=build_id,
    )
    if baseline is None:
        return None
    assert revision is not None
    assert generation is not None
    assert channel is not None
    head = work.connector.fetch_one(
        "SELECT registry.channel, head.source_revision, head.generation, "
        "head.advanced_at FROM catalog_channel_registry AS registry "
        "LEFT JOIN catalog_source_heads AS head ON head.channel = registry.channel "
        "WHERE registry.channel = %s",
        (channel,),
    )
    if len(head) != 4 or head[0] != channel:
        raise AnalysisCorruptionError("source head channel registry row is malformed")
    if any(value is None for value in head[1:]):
        raise AnalysisCorruptionError("source head vertical family is incomplete")
    if head[1:3] != (revision, generation):
        raise AnalysisNotReadyError(
            "source build baseline is stale against its channel head"
        )
    return baseline


@dataclass(frozen=True, slots=True)
class _SourceBuildIdentityAuthority:
    snapshot_attempt_id: bytes
    scope: bytes
    manifest_policy_id: int
    created_at: int

    def derive(self) -> bytes:
        return source_build_identity(
            snapshot_attempt_id=self.snapshot_attempt_id,
            scope=self.scope,
            manifest_policy_id=self.manifest_policy_id,
        )

    def derive_recovery(self) -> bytes:
        return source_build_recovery_identity(
            snapshot_attempt_id=self.snapshot_attempt_id,
            scope=self.scope,
            manifest_policy_id=self.manifest_policy_id,
            created_at=self.created_at,
        )


def _load_source_build_identity_authority(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
) -> _SourceBuildIdentityAuthority:
    try:
        source_build = load_source_build_family(
            work.connector,
            build_id=build_id,
        )
        manifest = load_build_manifest_family(
            work.connector,
            build_id=build_id,
        )
    except ManifestFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if source_build is None or manifest is None:
        raise AnalysisCorruptionError(
            "source build lacks its immutable sealed identity"
        )
    root = work.connector.fetch_one(
        "SELECT source_root_sha256 FROM catalog_source_scopes WHERE scope_key = %s",
        (source_build.scope_key,),
    )
    if len(root) != 1:
        raise AnalysisCorruptionError("source build lacks its immutable source root")
    source_root_sha256 = require_digest32(
        root[0],
        field="source build source_root_sha256",
    )
    summary = SourceBuildManifestSummary(
        manifest.manifest_sha256,
        manifest.gallery_count,
        manifest.file_count,
        manifest.byte_count,
    )
    return _SourceBuildIdentityAuthority(
        source_build_snapshot_attempt_id(source_root_sha256, summary),
        source_build.scope_key,
        source_build.manifest_policy_id,
        source_build.created_at,
    )


def _derive_pinned_baseline(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
) -> tuple[bytes | None, int | None, int | None, bytes | None]:
    """Derive a build's immutable baseline without consulting the live head.

    Existing build-identity analysis replay can legitimately happen after a later
    publication has advanced the channel. Its authority is the publication
    receipt pinned by the source build, not whichever receipt is live now. A
    missing pin is valid only for a true pre-publication genesis snapshot or
    after the build's own published revision durably carries the
    baseline analysis through provenance.
    """

    build = require_uuid16(build_id, field="baseline source build_id")
    channel = work.connector.fetch_one(
        "SELECT channel FROM catalog_source_build_channel WHERE build_id = %s",
        (build,),
    )
    if len(channel) != 1:
        raise AnalysisNotReadyError("source build channel is missing")
    source_channel = require_bounded_bytes(
        channel[0],
        field="source build channel",
        minimum=1,
        maximum=64,
    )

    base = work.connector.fetch_one(
        "SELECT base.base_receipt_id, committed.receipt_id, "
        "committed.source_revision, committed.generation, committed.committed_at, "
        "committed.candidate_id, candidate.analysis_id, "
        "candidate_analysis.build_id, candidate_analysis.state, "
        "descriptor.channel, descriptor.snapshot_manifest_sha256, "
        "provenance.analysis_id, provenance_analysis.build_id, "
        "provenance_analysis.state, snapshot.snapshot_manifest_sha256 "
        "FROM catalog_source_build_base_publication_commits AS base "
        "LEFT JOIN catalog_publication_commits AS committed "
        "ON committed.receipt_id = base.base_receipt_id "
        f"LEFT JOIN {_PUBLICATION_CANDIDATE_TABLE} AS candidate "
        "ON candidate.candidate_id = committed.candidate_id "
        f"LEFT JOIN {_ANALYSIS_RUN_VIEW} AS candidate_analysis "
        "ON candidate_analysis.analysis_id = candidate.analysis_id "
        "LEFT JOIN catalog_source_revision_descriptors AS descriptor "
        "ON descriptor.source_revision = committed.source_revision "
        "LEFT JOIN catalog_source_revision_provenance AS provenance "
        "ON provenance.source_revision = committed.source_revision "
        "LEFT JOIN catalog_analysis_runs AS provenance_analysis "
        "ON provenance_analysis.analysis_id = provenance.analysis_id "
        "LEFT JOIN catalog_analysis_snapshot_manifest AS snapshot "
        "ON snapshot.analysis_id = provenance.analysis_id "
        "WHERE base.build_id = %s",
        (build,),
    )
    if not base:
        identity_authority = _load_source_build_identity_authority(
            work,
            build_id=build,
        )
        if (
            identity_authority.derive() != build
            and identity_authority.derive_recovery() != build
        ):
            raise AnalysisCorruptionError(
                "source build differs from its immutable snapshot identity"
            )
        compacted = _derive_compacted_published_baseline(
            work,
            build_id=build,
            source_channel=source_channel,
        )
        if compacted is not None:
            return compacted
        head = work.connector.fetch_one(
            "SELECT registry.channel, head.receipt_id, committed.receipt_id, "
            "committed.source_revision, committed.generation, descriptor.channel "
            "FROM catalog_channel_registry AS registry "
            f"LEFT JOIN {_PUBLICATION_COMMIT_HEAD_TABLE} AS head "
            "ON head.channel = registry.channel "
            f"LEFT JOIN {_PUBLICATION_COMMIT_TABLE} AS committed "
            "ON committed.receipt_id = head.receipt_id "
            f"LEFT JOIN {_SOURCE_REVISION_DESCRIPTOR_TABLE} AS descriptor "
            "ON descriptor.source_revision = committed.source_revision "
            "WHERE registry.channel = %s",
            (source_channel,),
        )
        if len(head) != 6 or head[0] != source_channel:
            raise AnalysisCorruptionError(
                "source build channel head registry row is malformed"
            )
        head_members = head[1:]
        if all(value is None for value in head_members):
            return None, None, None, source_channel
        if (
            any(value is None for value in head_members)
            or head[1] != head[2]
            or head[5] != source_channel
        ):
            raise AnalysisCorruptionError(
                "source build channel head authority is incomplete"
            )
        raise AnalysisCorruptionError(
            "active source build lost its pinned publication baseline"
        )
    if len(base) != 15 or any(value is None for value in base):
        raise AnalysisCorruptionError(
            "source build baseline lacks its immutable sealed authority"
        )
    receipt_id = require_uuid16(base[0], field="base publication receipt_id")
    if base[1] != receipt_id:
        raise AnalysisCorruptionError("source build baseline receipt is not sealed")
    revision = require_positive_int63(base[2], field="base source revision")
    generation = require_positive_int63(base[3], field="base source generation")
    base_committed_at = require_int63(
        base[4],
        field="base publication committed_at",
    )
    require_uuid16(base[5], field="base publication candidate_id")
    candidate_analysis = require_uuid16(
        base[6],
        field="base publication candidate analysis_id",
    )
    candidate_build = require_uuid16(
        base[7],
        field="base publication candidate build_id",
    )
    if base[9] != source_channel:
        raise AnalysisCorruptionError(
            "source build baseline belongs to another channel"
        )
    descriptor_snapshot = require_digest32(
        base[10],
        field="base publication descriptor snapshot",
    )
    provenance_analysis = require_uuid16(
        base[11],
        field="base publication provenance analysis_id",
    )
    provenance_build = require_uuid16(
        base[12],
        field="base publication provenance build_id",
    )
    provenance_snapshot = require_digest32(
        base[14],
        field="base publication provenance snapshot",
    )
    if (
        candidate_analysis != provenance_analysis
        or candidate_build != provenance_build
        or candidate_build == build
        or base[8] != "COMPLETE"
        or base[13] != "COMPLETE"
        or provenance_snapshot != descriptor_snapshot
    ):
        raise AnalysisCorruptionError(
            "source build baseline candidate and provenance lineage proofs differ"
        )

    identity_authority = _load_source_build_identity_authority(
        work,
        build_id=build,
    )
    if (
        identity_authority.derive() != build
        and identity_authority.derive_recovery() != build
    ):
        raise AnalysisCorruptionError(
            "source build differs from its immutable snapshot identity"
        )
    if base_committed_at > identity_authority.created_at:
        raise AnalysisCorruptionError(
            "source build baseline was committed after build creation"
        )
    baseline = provenance_analysis
    try:
        baseline_run = load_analysis_run_family(
            work.connector,
            analysis_id=baseline,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if baseline_run is None or baseline_run.state != "COMPLETE":
        raise AnalysisNotReadyError("baseline analysis is not COMPLETE")
    _require_exact_component_seals(work, baseline)
    return baseline, revision, generation, source_channel


def _derive_compacted_published_baseline(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
    source_channel: bytes,
) -> tuple[bytes | None, int, int, bytes] | None:
    """Recover a safely compacted pin from the build's finalized provenance."""

    rows = work.connector.fetch_all(
        "SELECT committed.receipt_id, committed.source_revision, "
        "committed.generation, receipt.state, receipt.finalized_at, "
        "descriptor.channel, descriptor.snapshot_manifest_sha256, "
        "provenance.analysis_id, provenance_analysis.build_id, "
        "provenance_analysis.state, snapshot.snapshot_manifest_sha256, "
        "baseline.base_analysis_id "
        f"FROM {_PUBLICATION_COMMIT_TABLE} AS committed "
        f"JOIN {_PUBLICATION_RECEIPT_VIEW} AS receipt "
        "ON receipt.receipt_id = committed.receipt_id "
        f"JOIN {_SOURCE_REVISION_DESCRIPTOR_TABLE} AS descriptor "
        "ON descriptor.source_revision = committed.source_revision "
        f"JOIN {_SOURCE_REVISION_PROVENANCE_TABLE} AS provenance "
        "ON provenance.source_revision = committed.source_revision "
        f"JOIN {_ANALYSIS_RUN_VIEW} AS provenance_analysis "
        "ON provenance_analysis.analysis_id = provenance.analysis_id "
        f"JOIN {_ANALYSIS_SNAPSHOT_MANIFEST_TABLE} AS snapshot "
        "ON snapshot.analysis_id = provenance.analysis_id "
        f"LEFT JOIN {_ANALYSIS_BASELINE_TABLE} AS baseline "
        "ON baseline.analysis_id = provenance.analysis_id "
        "WHERE provenance_analysis.build_id = %s LIMIT 2",
        (build_id,),
    )
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 12:
        raise AnalysisCorruptionError(
            "source build has multiple or malformed finalized publications"
        )
    row = rows[0]
    if row[3] != "PUBLISHED":
        # The build's publication commit is durable but not yet finalized
        # (library activation or finalization is pending): there is no
        # finalized provenance to compact yet, exactly as before the commit.
        return None
    mandatory = row[:11]
    if any(value is None for value in mandatory):
        raise AnalysisCorruptionError(
            "source build publication lacks its durable handoff authority"
        )
    require_uuid16(row[0], field="compacted source publication receipt_id")
    revision = require_positive_int63(
        row[1],
        field="compacted source publication revision",
    )
    generation = require_positive_int63(
        row[2],
        field="compacted source publication generation",
    )
    require_int63(row[4], field="compacted source publication finalized_at")
    channel = require_bounded_bytes(
        row[5],
        field="compacted source publication channel",
        minimum=1,
        maximum=64,
    )
    descriptor_snapshot = require_digest32(
        row[6],
        field="compacted source publication descriptor snapshot",
    )
    require_uuid16(
        row[7],
        field="compacted source publication provenance analysis_id",
    )
    provenance_build = require_uuid16(
        row[8],
        field="compacted source publication provenance build_id",
    )
    provenance_snapshot = require_digest32(
        row[10],
        field="compacted source publication provenance snapshot",
    )
    if (
        row[3] != "PUBLISHED"
        or channel != source_channel
        or provenance_build != build_id
        or row[9] != "COMPLETE"
        or provenance_snapshot != descriptor_snapshot
    ):
        raise AnalysisCorruptionError(
            "source build publication and provenance handoff proofs differ"
        )
    baseline = (
        None
        if row[11] is None
        else require_uuid16(
            row[11],
            field="compacted source publication baseline analysis_id",
        )
    )
    if baseline is not None:
        try:
            baseline_run = load_analysis_run_family(
                work.connector,
                analysis_id=baseline,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if baseline_run is None or baseline_run.state != "COMPLETE":
            raise AnalysisNotReadyError("compacted baseline analysis is not COMPLETE")
        _require_exact_component_seals(work, baseline)
    return baseline, revision, generation, source_channel


def _derive_layout(
    work: VNextUnitOfWork,
    *,
    baseline: bytes | None,
    policy: _Policy,
) -> tuple[bytes | None, int, tuple[bytes | None, ...]]:
    if baseline is None:
        return None, 0, (None,)
    try:
        parent_run = load_analysis_run_family(
            work.connector,
            analysis_id=baseline,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if parent_run is None:
        raise AnalysisCorruptionError("baseline analysis run is missing")
    _parent_baseline, parent_anchor, parent_depth, parent_ancestry = _load_layout(
        work,
        baseline,
    )
    parent_policy = parent_run.policy_id
    _validate_ancestry_suffixes(
        work,
        ancestry=parent_ancestry,
        anchor_analysis_id=parent_anchor,
        policy_id=parent_policy,
    )
    # A policy change or depth-16 parent forces a self-only full compaction.
    if parent_policy != policy.policy_id or parent_depth == _MAX_OVERLAY_DEPTH:
        return None, 0, (None,)
    depth = parent_depth + 1
    return parent_anchor, depth, (None, *parent_ancestry)


def _load_layout(
    work: VNextUnitOfWork,
    analysis_id: bytes,
) -> tuple[bytes | None, bytes, int, tuple[bytes, ...]]:
    baseline_row = work.connector.fetch_one(
        "SELECT base_analysis_id FROM catalog_analysis_baselines "
        "WHERE analysis_id = %s",
        (analysis_id,),
    )
    baseline = (
        None
        if not baseline_row
        else require_uuid16(baseline_row[0], field="persisted baseline_analysis_id")
    )
    ancestry_rows = work.connector.fetch_all(
        "SELECT ancestor_depth, ancestor_analysis_id "
        "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
        "ORDER BY ancestor_depth LIMIT 18",
        (analysis_id,),
    )
    ancestry = tuple(
        require_uuid16(row[1], field="persisted ancestor_analysis_id")
        for row in ancestry_rows
    )
    if (
        not ancestry
        or len(ancestry) > _MAX_OVERLAY_DEPTH + 1
        or ancestry[0] != analysis_id
        or len(set(ancestry)) != len(ancestry)
    ):
        raise AnalysisCorruptionError(
            "persisted analysis ancestry has no exact acyclic endpoint"
        )
    for expected_depth, row_value in enumerate(ancestry_rows):
        if (
            require_int63(row_value[0], field="persisted ancestor_depth")
            != expected_depth
        ):
            raise AnalysisCorruptionError(
                "persisted analysis ancestry depths are not contiguous"
            )
    return (
        baseline,
        ancestry[-1],
        len(ancestry) - 1,
        ancestry,
    )


def _require_exact_component_seals(work: VNextUnitOfWork, analysis_id: bytes) -> None:
    receipts = _component_seal_receipts(work, analysis_id)
    actual = {component for component, _row_count, _sealed_at in receipts}
    if len(receipts) != 5 or actual != ANALYSIS_COMPONENTS:
        raise AnalysisNotReadyError(
            "baseline analysis is not sealed in all five components"
        )
    stage_by_component = {
        _COMPONENT_FILE_HASH: _STAGE_VALIDATE_FILE_HASH,
        _COMPONENT_CONTENT_CANDIDATE: _STAGE_VALIDATE_CONTENT_CANDIDATE,
        _COMPONENT_CONTENT_OWNER: _STAGE_VALIDATE_CONTENT_OWNER,
        _COMPONENT_GID_CANDIDATE: _STAGE_VALIDATE_GID_CANDIDATE,
        _COMPONENT_GID_WINNER: _STAGE_VALIDATE_GID_WINNER,
    }
    for component in actual:
        if not _component_is_sealed(work, analysis_id, stage_by_component[component]):
            raise AnalysisCorruptionError(
                f"component {component!r} lost its terminal receipt"
            )


def _validate_ancestry_suffixes(
    work: VNextUnitOfWork,
    *,
    ancestry: tuple[bytes, ...],
    anchor_analysis_id: bytes,
    policy_id: int,
) -> None:
    """Require every inherited ancestor to materialize its exact sealed suffix."""

    for offset, ancestor in enumerate(ancestry):
        suffix = ancestry[offset:]
        try:
            run = load_analysis_run_family(
                work.connector,
                analysis_id=ancestor,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if run is None:
            raise AnalysisCorruptionError("inherited analysis anchor is missing")
        _baseline, derived_anchor, derived_depth, materialized = _load_layout(
            work,
            ancestor,
        )
        if (
            run.policy_id != policy_id
            or run.state != "COMPLETE"
            or derived_anchor != anchor_analysis_id
            or derived_depth != len(suffix) - 1
        ):
            raise AnalysisCorruptionError(
                "inherited analysis does not match the complete policy suffix"
            )
        if materialized != suffix:
            raise AnalysisCorruptionError(
                "inherited analysis ancestry is not the complete parent suffix"
            )
        baseline_row = work.connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (ancestor,),
        )
        if len(suffix) > 1 and baseline_row != (suffix[1],):
            raise AnalysisCorruptionError(
                "inherited analysis baseline is not its immediate parent"
            )
        _require_exact_component_seals(work, ancestor)


def _initialize_checkpoint(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    stage: bytes,
    cursor: bytes,
    updated_at: int,
) -> None:
    work.connector.execute(
        f"INSERT INTO {_CHECKPOINT_TABLE} "
        "(analysis_id, stage, generation, `cursor`, processed_count, state, "
        "updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (analysis_id, stage, 1, cursor, 0, _CHECKPOINT_OPEN, updated_at),
    )


def _receipt_family_exists(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    stage: bytes,
    start_generation: int,
) -> bool:
    row = work.connector.fetch_one(
        f"SELECT 1 FROM {_RECEIPT_STORED_TABLE} "
        "WHERE analysis_id = %s AND stage = %s AND start_generation = %s",
        (analysis_id, stage, start_generation),
    )
    return bool(row)


def _issue_next_batch_authorized(
    work: VNextUnitOfWork,
    *,
    authority: _RunAuthority,
    generation: int,
    batch_key: bytes,
    max_rows: int,
) -> AnalysisStageIssue:
    exact_generation = require_positive_int63(
        generation,
        field="analysis stage issue generation",
    )
    preparation_authority = _preparation_authority_receipt(
        work,
        authority,
        generation=exact_generation,
    )
    run_state = work.connector.fetch_one(
        "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
        (authority.analysis_id,),
    )
    if run_state == ("COMPLETE",):
        _require_exact_component_seals(work, authority.analysis_id)
        binding = work.connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
            (authority.analysis_id,),
        )
        if len(binding) != 1:
            raise AnalysisCorruptionError(
                "COMPLETE analysis lacks its snapshot-manifest binding"
            )
        return AnalysisStageIssue(
            authority.analysis_id,
            authority.build_id,
            None,
            None,
            0,
            None,
            None,
            None,
            (),
            preparation_authority,
            require_digest32(
                binding[0],
                field="completed analysis snapshot_manifest_sha256",
            ),
            None,
            _STAGE_ISSUE_TOKEN,
        )
    if run_state != ("OPEN",):
        raise AnalysisNotReadyError("analysis stage issue requires an OPEN run")

    states = work.connector.fetch_all(
        f"SELECT stage, state FROM {_CHECKPOINT_TABLE} WHERE analysis_id = %s",
        (authority.analysis_id,),
    )
    if len(states) != len(_STAGES):
        raise AnalysisCorruptionError(
            "analysis run lacks its exact checkpoint state set"
        )
    state_by_stage: dict[bytes, str] = {}
    for raw_stage, raw_state in states:
        stage = require_bounded_bytes(
            raw_stage,
            field="analysis checkpoint stage",
            minimum=1,
            maximum=64,
        )
        if stage in state_by_stage or not isinstance(raw_state, str):
            raise AnalysisCorruptionError("analysis checkpoint state set is malformed")
        state_by_stage[stage] = _require_checkpoint_state(raw_state)
    if set(state_by_stage) != set(_STAGES):
        raise AnalysisCorruptionError(
            "analysis checkpoint state set differs from the registry"
        )
    first_open: bytes | None = None
    saw_open = False
    for stage in _STAGES:
        state = state_by_stage[stage]
        if state == _CHECKPOINT_OPEN:
            saw_open = True
            if first_open is None:
                first_open = stage
        elif saw_open:
            raise AnalysisCorruptionError(
                "analysis checkpoint completion is not a contiguous prefix"
            )
    if first_open is None:
        _require_exact_component_seals(work, authority.analysis_id)
        return AnalysisStageIssue(
            authority.analysis_id,
            authority.build_id,
            None,
            None,
            0,
            None,
            None,
            None,
            (),
            preparation_authority,
            None,
            None,
            _STAGE_ISSUE_TOKEN,
        )

    exact_key = require_bounded_bytes(
        batch_key,
        field="analysis stage issue batch_key",
        minimum=1,
        maximum=512,
    )
    run, checkpoint, replay = _prepare_batch_authorized(
        work,
        authority=authority,
        stage=first_open,
        batch_key=exact_key,
        max_rows=max_rows,
    )
    if replay is not None:
        return AnalysisStageIssue(
            authority.analysis_id,
            authority.build_id,
            first_open,
            exact_key,
            replay.page_limit,
            replay.start_generation,
            replay.start_cursor,
            replay.start_processed_count,
            (),
            preparation_authority,
            None,
            replay,
            _STAGE_ISSUE_TOKEN,
        )
    assert checkpoint is not None
    memberships: tuple[tuple[int, int | None], ...] = ()
    if first_open in _GALLERY_PREPARATION_STAGES:
        memberships = _issued_gallery_memberships(
            work,
            run,
            stage=first_open,
            checkpoint=checkpoint,
        )
    return AnalysisStageIssue(
        authority.analysis_id,
        authority.build_id,
        first_open,
        exact_key,
        checkpoint.page_limit,
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        memberships,
        preparation_authority,
        None,
        None,
        _STAGE_ISSUE_TOKEN,
    )


def _prepare_batch(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    analysis_id: bytes,
    stage: bytes,
    batch_key: bytes,
    max_rows: int,
    now: int,
) -> tuple[_RunAuthority, _Checkpoint | None, AnalysisBatchResult | None]:
    authority = _authorize_analysis(
        work,
        gate_lease=gate_lease,
        ingest_turn=ingest_turn,
        analysis_id=analysis_id,
        now=require_int63(now, field="analysis batch now"),
    )
    return _prepare_batch_authorized(
        work,
        authority=authority,
        stage=stage,
        batch_key=batch_key,
        max_rows=max_rows,
    )


def _prepare_batch_authorized(
    work: VNextUnitOfWork,
    *,
    authority: _RunAuthority,
    stage: bytes,
    batch_key: bytes,
    max_rows: int,
) -> tuple[_RunAuthority, _Checkpoint | None, AnalysisBatchResult | None]:
    key = require_bounded_bytes(
        batch_key,
        field="analysis batch_key",
        minimum=1,
        maximum=512,
    )
    coordinate = work.connector.fetch_one(
        f"SELECT start_generation FROM {_RECEIPT_STORED_TABLE} "
        "WHERE analysis_id = %s AND stage = %s AND batch_key = %s",
        (authority.analysis_id, stage, key),
    )
    if coordinate:
        if len(coordinate) != 1:
            raise AnalysisCorruptionError("analysis batch coordinate is malformed")
        start_generation = require_positive_int63(
            coordinate[0], field="analysis receipt coordinate start_generation"
        )
        receipt = work.connector.fetch_one(
            "SELECT start_generation, start_cursor, start_processed_count, "
            "page_limit, next_cursor, next_processed_count, next_state, row_count, terminal, "
            "committed_generation, committed_at "
            "FROM catalog_analysis_batch_receipts "
            "WHERE analysis_id = %s AND stage = %s AND start_generation = %s",
            (authority.analysis_id, stage, start_generation),
        )
        if not receipt:
            raise AnalysisCorruptionError(
                "analysis batch coordinate has no complete sealed receipt"
            )
        stored = _batch_result_from_receipt(
            authority.analysis_id,
            stage,
            key,
            receipt,
            replayed=True,
            component_sealed=(
                receipt[8] == 1
                and _component_is_sealed(
                    work,
                    authority.analysis_id,
                    stage,
                )
            ),
        )
        return (
            authority,
            None,
            stored,
        )
    limit = min(
        require_positive_int63(max_rows, field="analysis max_rows"),
        _MAX_BATCH_ROWS,
    )
    checkpoint = _lock_checkpoint(
        work,
        authority.analysis_id,
        stage,
        page_limit=limit,
    )
    if _receipt_family_exists(
        work,
        analysis_id=authority.analysis_id,
        stage=stage,
        start_generation=checkpoint.generation,
    ):
        raise AnalysisCorruptionError(
            "analysis checkpoint generation has a partial or conflicting receipt"
        )
    if checkpoint.state == _CHECKPOINT_COMPLETE:
        raise AnalysisNotReadyError(
            "analysis stage is complete under a different terminal batch_key"
        )
    if checkpoint.state != _CHECKPOINT_OPEN:
        raise AnalysisCorruptionError("analysis checkpoint has an unknown state")
    database_time = database_unix_microseconds(work)
    if database_time < checkpoint.updated_at:
        raise AnalysisNotReadyError(
            "database batch time precedes its durable checkpoint"
        )
    return authority, checkpoint, None


def _lock_checkpoint(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    stage: bytes,
    *,
    page_limit: int,
) -> _Checkpoint:
    kind, live = _require_registered_stage(work, stage)
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("analysis-checkpoint", analysis_id, stage),
        f"SELECT generation, `cursor`, processed_count, state, updated_at "
        f"FROM {_CHECKPOINT_TABLE} "
        "WHERE analysis_id = %s AND stage = %s",
        (analysis_id, stage),
    )
    if len(row) != 5 or not isinstance(row[3], str):
        raise AnalysisCorruptionError("analysis checkpoint is missing or malformed")
    generation = require_positive_int63(row[0], field="analysis checkpoint generation")
    checkpoint = _Checkpoint(
        generation,
        require_bounded_bytes(row[1], field="analysis checkpoint cursor", maximum=2048),
        require_int63(row[2], field="analysis checkpoint processed_count"),
        row[3],
        require_int63(row[4], field="analysis checkpoint updated_at"),
        require_positive_int63(page_limit, field="analysis page_limit"),
    )
    _decode_cursor(kind, checkpoint.cursor, live=live)
    return checkpoint


def _commit_batch(
    work: VNextUnitOfWork,
    *,
    authority: _RunAuthority,
    stage: bytes,
    batch_key: bytes,
    checkpoint: _Checkpoint,
    cursor: bytes,
    row_count: int,
    terminal: bool,
    now: int,
) -> AnalysisBatchResult:
    if checkpoint.generation == INT63_MAX:
        raise OverflowError("analysis checkpoint generation is exhausted")
    rows = require_int63(row_count, field="analysis batch row_count")
    if not isinstance(terminal, bool):
        raise TypeError("analysis batch terminal must be bool")
    if terminal:
        if rows != 0 or cursor != checkpoint.cursor:
            raise AnalysisCorruptionError(
                "terminal analysis transition is not an empty page"
            )
    elif rows == 0 or cursor == checkpoint.cursor:
        raise AnalysisCorruptionError(
            "nonterminal analysis transition did not advance a positive page"
        )
    next_generation = checkpoint.generation + 1
    next_processed_count = _sum_int63(
        checkpoint.processed_count,
        rows,
        field="analysis checkpoint processed_count",
    )
    next_state = _CHECKPOINT_COMPLETE if terminal else _CHECKPOINT_OPEN
    require_int63(now, field="analysis batch authorization time")
    timestamp = database_unix_microseconds(work)
    if timestamp < checkpoint.updated_at:
        raise AnalysisNotReadyError(
            "database batch time precedes its durable checkpoint"
        )
    key = require_bounded_bytes(
        batch_key,
        field="analysis batch_key",
        minimum=1,
        maximum=512,
    )
    work.connector.execute(
        f"INSERT INTO {_RECEIPT_STORED_TABLE} "
        "(analysis_id, stage, start_generation, batch_key, start_cursor, "
        "start_processed_count, page_limit, next_cursor, row_count, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            authority.analysis_id,
            stage,
            checkpoint.generation,
            key,
            checkpoint.cursor,
            checkpoint.processed_count,
            checkpoint.page_limit,
            cursor,
            rows,
            timestamp,
        ),
    )

    work.compare_and_swap(
        f"UPDATE {_CHECKPOINT_TABLE} SET generation = %s, `cursor` = %s, "
        "processed_count = %s, state = %s, updated_at = %s "
        "WHERE analysis_id = %s AND stage = %s AND generation = %s "
        "AND `cursor` = %s AND processed_count = %s AND state = %s "
        "AND updated_at = %s",
        (
            next_generation,
            cursor,
            next_processed_count,
            next_state,
            timestamp,
            authority.analysis_id,
            stage,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            checkpoint.state,
            checkpoint.updated_at,
        ),
        authority=f"analysis checkpoint {stage!r} complete row",
    )
    if checkpoint.generation > 1:
        deleted = work.connector.execute_affected(
            f"DELETE FROM {_RECEIPT_STORED_TABLE} "
            "WHERE analysis_id = %s AND stage = %s AND start_generation = %s",
            (authority.analysis_id, stage, checkpoint.generation - 1),
        )
        if deleted != 1:
            raise AnalysisCorruptionError(
                "analysis predecessor receipt is missing before safe acknowledgement"
            )
    return AnalysisBatchResult(
        authority.analysis_id,
        stage,
        key,
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        checkpoint.page_limit,
        cursor,
        next_processed_count,
        next_state,
        rows,
        terminal,
        next_generation,
        timestamp,
        False,
    )


def _batch_result_from_receipt(
    analysis_id: bytes,
    stage: bytes,
    batch_key: bytes,
    row: tuple[Any, ...],
    *,
    replayed: bool,
    component_sealed: bool,
) -> AnalysisBatchResult:
    if len(row) != 11:
        raise AnalysisCorruptionError("analysis batch receipt is malformed")
    terminal_value = require_int63(row[8], field="analysis receipt terminal")
    if terminal_value not in {0, 1}:
        raise AnalysisCorruptionError(
            "analysis batch receipt terminal flag is not boolean"
        )
    try:
        result = AnalysisBatchResult(
            analysis_id,
            stage,
            batch_key,
            require_positive_int63(
                row[0],
                field="analysis receipt start_generation",
            ),
            require_bounded_bytes(
                row[1],
                field="analysis receipt start_cursor",
                maximum=2048,
            ),
            require_int63(
                row[2],
                field="analysis receipt start_processed_count",
            ),
            require_positive_int63(
                row[3],
                field="analysis receipt page_limit",
            ),
            require_bounded_bytes(
                row[4],
                field="analysis receipt next_cursor",
                maximum=2048,
            ),
            require_int63(
                row[5],
                field="analysis receipt next_processed_count",
            ),
            _require_checkpoint_state(row[6]),
            require_int63(row[7], field="analysis receipt row_count"),
            bool(terminal_value),
            require_positive_int63(
                row[9],
                field="analysis receipt committed_generation",
            ),
            require_int63(row[10], field="analysis receipt committed_at"),
            replayed,
            component_sealed,
        )
        kind, live = _stage_cursor_spec(stage)
        _decode_cursor(kind, result.start_cursor, live=live)
        _decode_cursor(kind, result.next_cursor, live=live)
    except (TypeError, ValueError) as error:
        raise AnalysisCorruptionError(
            "analysis batch receipt has invalid domain values"
        ) from error
    return result


def _validate_batch_replay(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    replay: AnalysisBatchResult,
    *,
    preparations: Sequence[AnalysisGalleryPreparation | None] = (),
) -> None:
    """Rederive one committed page with its stored bound before replaying it."""

    if not replay.replayed or replay.analysis_id != authority.analysis_id:
        raise AnalysisCorruptionError("analysis replay lost its durable authority")
    kind, live = _stage_cursor_spec(replay.stage)
    last, live_count = _decode_cursor(kind, replay.start_cursor, live=live)
    rows = _replay_page_rows(
        work,
        authority,
        stage=replay.stage,
        after=last,
        limit=replay.page_limit + 1,
    )
    selected = rows[: replay.page_limit]
    _require_replay_keyed_page_exact(
        work,
        authority.analysis_id,
        stage=replay.stage,
        after=last,
        selected=selected,
        limit=replay.page_limit + 1,
    )
    exact_preparations: tuple[AnalysisGalleryPreparation | None, ...] = ()
    content_impact_page: _ContentImpactPage | None = None
    gid_impact_page: _GidImpactPage | None = None
    if replay.stage == _STAGE_IMPACTED_CONTENT:
        gallery_ids = tuple(
            require_positive_int63(row[0], field="replayed impacted-content gallery_id")
            for row in selected
        )
        content_impact_page = _load_content_impact_page(
            work,
            authority,
            gallery_ids,
        )
        exact_preparations = _require_validation_preparations(
            work,
            authority,
            selected,
            preparations,
            memberships=content_impact_page.current_observations,
        )
    elif replay.stage in {
        _STAGE_CONTENT_CANDIDATE,
        _STAGE_VALIDATE_CONTENT_CANDIDATE,
        _STAGE_GID_CANDIDATE,
        _STAGE_VALIDATE_GID_CANDIDATE,
    }:
        exact_preparations = _require_validation_preparations(
            work,
            authority,
            selected,
            preparations,
        )
    elif replay.stage == _STAGE_IMPACTED_GID:
        gallery_ids = tuple(
            require_positive_int63(row[0], field="replayed impacted-GID gallery_id")
            for row in selected
        )
        gid_impact_page = _load_gid_impact_page(work, authority, gallery_ids)
    elif preparations:
        raise AnalysisNotReadyError(
            "analysis replay received preparations for a scalar stage"
        )
    live_count = _require_replay_page_materialized(
        work,
        authority,
        stage=replay.stage,
        after=last,
        selected=selected,
        preparations=exact_preparations,
        live_count=live_count,
        content_impact_page=content_impact_page,
        gid_impact_page=gid_impact_page,
    )

    next_key = last
    if selected:
        raw_key = selected[-1][0]
        if kind == _CURSOR_DIGEST:
            next_key = require_digest32(raw_key, field="replayed digest cursor")
        else:
            next_key = require_positive_int63(
                raw_key,
                field="replayed integer cursor",
            ).to_bytes(8, "big")
    expected_terminal = not selected
    expected_cursor = _encode_cursor(
        kind,
        next_key,
        live_count=live_count if live else None,
    )
    expected_state = _CHECKPOINT_COMPLETE if expected_terminal else _CHECKPOINT_OPEN
    if (
        replay.next_cursor != expected_cursor
        or replay.row_count != len(selected)
        or replay.terminal is not expected_terminal
        or replay.next_state != expected_state
        or replay.next_processed_count != replay.start_processed_count + len(selected)
    ):
        raise AnalysisCorruptionError(
            "analysis batch receipt differs from its stored-limit evaluator"
        )
    if live and expected_terminal and not replay.component_sealed:
        raise AnalysisCorruptionError(
            "terminal validation receipt lacks its exact component seal"
        )


def _replay_page_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    stage: bytes,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    integer_after = None if after is None else int.from_bytes(after, "big")
    if stage == _STAGE_CHANGED_GALLERY:
        return _changed_gallery_rows(
            work,
            authority,
            after=integer_after,
            limit=limit,
        )
    if stage == _STAGE_CHANGED_FILE_HASH:
        return _changed_file_hash_rows(work, authority, after=after, limit=limit)
    if stage == _STAGE_FILE_HASH_DECISION:
        return _decision_work_rows(work, authority, after=after, limit=limit)
    if stage == _STAGE_VALIDATE_FILE_HASH:
        return _validation_key_rows(work, authority, after=after, limit=limit)
    if stage == _STAGE_IMPACTED_GALLERY:
        return _impacted_gallery_rows(
            work,
            authority,
            after=integer_after,
            limit=limit,
        )
    if stage in {
        _STAGE_IMPACTED_CONTENT,
        _STAGE_CONTENT_CANDIDATE,
        _STAGE_IMPACTED_GID,
        _STAGE_GID_CANDIDATE,
    }:
        return _workset_gallery_rows(
            work,
            authority.analysis_id,
            after=integer_after,
            limit=limit,
        )
    if stage == _STAGE_VALIDATE_CONTENT_CANDIDATE:
        return _content_candidate_validation_keys(
            work,
            authority,
            after=integer_after,
            limit=limit,
        )
    if stage == _STAGE_CONTENT_OWNER:
        return _workset_content_rows(
            work,
            authority.analysis_id,
            after=after,
            limit=limit,
        )
    if stage == _STAGE_VALIDATE_CONTENT_OWNER:
        return _content_owner_validation_keys(
            work,
            authority,
            after=after,
            limit=limit,
        )
    if stage == _STAGE_VALIDATE_GID_CANDIDATE:
        return _gid_candidate_validation_keys(
            work,
            authority,
            after=integer_after,
            limit=limit,
        )
    if stage == _STAGE_GID_WINNER:
        return _workset_gid_rows(
            work,
            authority.analysis_id,
            after=integer_after,
            limit=limit,
        )
    if stage == _STAGE_VALIDATE_GID_WINNER:
        return _gid_winner_validation_keys(
            work,
            authority,
            after=integer_after,
            limit=limit,
        )
    raise AnalysisCorruptionError("analysis receipt names an unregistered stage")


def _require_replay_keyed_page_exact(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    *,
    stage: bytes,
    after: bytes | None,
    selected: Sequence[tuple[Any, ...]],
    limit: int,
) -> None:
    if stage == _STAGE_CHANGED_GALLERY:
        boundary = 0 if after is None else int.from_bytes(after, "big")
        if selected:
            end = require_positive_int63(
                selected[-1][0],
                field="replayed changed-gallery end",
            )
            rows = work.connector.fetch_all(
                "SELECT gallery_id, change_kind "
                "FROM catalog_analysis_changed_galleries "
                "WHERE analysis_id = %s AND gallery_id > %s AND gallery_id <= %s "
                "ORDER BY gallery_id LIMIT %s",
                (analysis_id, boundary, end, limit),
            )
        else:
            rows = work.connector.fetch_all(
                "SELECT gallery_id, change_kind "
                "FROM catalog_analysis_changed_galleries "
                "WHERE analysis_id = %s AND gallery_id > %s "
                "ORDER BY gallery_id LIMIT %s",
                (analysis_id, boundary, limit),
            )
        if rows != list(selected):
            raise AnalysisCorruptionError(
                "changed-gallery materialization differs from its exact page"
            )
        return
    if stage not in {_STAGE_CHANGED_FILE_HASH, _STAGE_IMPACTED_GALLERY}:
        return
    if stage == _STAGE_CHANGED_FILE_HASH:
        table = "catalog_analysis_changed_file_hashes"
        column = "file_sha256"
        parameters: list[Any] = [analysis_id]
        predicates = ["analysis_id = %s"]
        if after is not None:
            predicates.append("file_sha256 > %s")
            parameters.append(require_digest32(after, field="replayed hash start"))
        if selected:
            predicates.append("file_sha256 <= %s")
            parameters.append(
                require_digest32(selected[-1][0], field="replayed hash end")
            )
    else:
        table = "catalog_analysis_impacted_galleries"
        column = "gallery_id"
        boundary = 0 if after is None else int.from_bytes(after, "big")
        parameters = [analysis_id, boundary]
        predicates = ["analysis_id = %s", "gallery_id > %s"]
        if selected:
            predicates.append("gallery_id <= %s")
            parameters.append(
                require_positive_int63(
                    selected[-1][0],
                    field="replayed impacted-gallery end",
                )
            )
    parameters.append(limit)
    rows = work.connector.fetch_all(
        f"SELECT {column} FROM {table} WHERE "
        + " AND ".join(predicates)
        + f" ORDER BY {column} LIMIT %s",
        tuple(parameters),
    )
    if rows != list(selected):
        raise AnalysisCorruptionError(
            "analysis keyed materialization differs from its exact page"
        )


def _require_replay_page_materialized(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    stage: bytes,
    after: bytes | None,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
    live_count: int,
    content_impact_page: _ContentImpactPage | None = None,
    gid_impact_page: _GidImpactPage | None = None,
) -> int:
    if stage == _STAGE_CHANGED_GALLERY:
        for raw_gallery, change_kind in selected:
            gallery = require_positive_int63(
                raw_gallery,
                field="replayed changed gallery_id",
            )
            if work.connector.fetch_one(
                "SELECT change_kind FROM catalog_analysis_changed_galleries "
                "WHERE analysis_id = %s AND gallery_id = %s",
                (authority.analysis_id, gallery),
            ) != (change_kind,):
                raise AnalysisCorruptionError(
                    "changed-gallery page differs from its materialization"
                )
        return live_count
    if stage == _STAGE_CHANGED_FILE_HASH:
        _require_replay_key_rows(
            work,
            authority.analysis_id,
            selected,
            table="catalog_analysis_changed_file_hashes",
            key_column="file_sha256",
            digest=True,
        )
        return live_count
    if stage in {_STAGE_FILE_HASH_DECISION, _STAGE_VALIDATE_FILE_HASH}:
        deltas = None
        if stage == _STAGE_FILE_HASH_DECISION:
            digests = tuple(
                require_digest32(row[0], field="replayed decision file_sha256")
                for row in selected
            )
            try:
                families = load_analysis_exclusion_delta_families(
                    work.connector,
                    analysis_id=authority.analysis_id,
                    file_sha256s=digests,
                )
            except AnalysisFamilyCollisionError as error:
                raise AnalysisCorruptionError(str(error)) from error
            deltas = {family.file_sha256: family for family in families}
            if set(deltas) != set(digests):
                raise AnalysisCorruptionError(
                    "file-decision page lacks its exact exclusion-delta set"
                )
        for row in selected:
            digest = require_digest32(row[0], field="replayed decision file_sha256")
            decision = _require_replay_file_decision(
                work,
                authority,
                digest,
                require_delta=stage == _STAGE_FILE_HASH_DECISION,
                delta=None if deltas is None else deltas[digest],
            )
            if stage == _STAGE_VALIDATE_FILE_HASH and decision is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="replayed file-decision live row count",
                )
        return live_count
    if stage == _STAGE_IMPACTED_GALLERY:
        _require_replay_key_rows(
            work,
            authority.analysis_id,
            selected,
            table="catalog_analysis_impacted_galleries",
            key_column="gallery_id",
            digest=False,
        )
        return live_count
    if stage == _STAGE_IMPACTED_CONTENT:
        if content_impact_page is None:
            raise AnalysisCorruptionError("content replay lost page authority")
        _require_replay_impacted_content(
            work,
            authority,
            after=after,
            selected=selected,
            preparations=preparations,
            impact_page=content_impact_page,
        )
        return live_count
    if stage in {
        _STAGE_CONTENT_CANDIDATE,
        _STAGE_VALIDATE_CONTENT_CANDIDATE,
    }:
        for row, preparation in zip(selected, preparations, strict=True):
            gallery_id = require_positive_int63(
                row[0],
                field="replayed content candidate gallery_id",
            )
            content_candidate = _require_replay_content_candidate(
                work,
                authority,
                gallery_id,
                preparation,
            )
            if (
                stage == _STAGE_VALIDATE_CONTENT_CANDIDATE
                and content_candidate is not None
            ):
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="replayed content-candidate live row count",
                )
        return live_count
    if stage in {_STAGE_CONTENT_OWNER, _STAGE_VALIDATE_CONTENT_OWNER}:
        for row in selected:
            content = require_digest32(row[0], field="replayed content-owner key")
            owner = _require_replay_content_owner(
                work,
                authority,
                content,
            )
            if stage == _STAGE_VALIDATE_CONTENT_OWNER and owner is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="replayed content-owner live row count",
                )
        return live_count
    if stage == _STAGE_IMPACTED_GID:
        if gid_impact_page is None:
            raise AnalysisCorruptionError("GID replay lost page authority")
        _require_replay_impacted_gid(
            work,
            authority,
            after=after,
            selected=selected,
            impact_page=gid_impact_page,
        )
        return live_count
    if stage in {_STAGE_GID_CANDIDATE, _STAGE_VALIDATE_GID_CANDIDATE}:
        for row, preparation in zip(selected, preparations, strict=True):
            gallery_id = require_positive_int63(
                row[0],
                field="replayed GID candidate gallery_id",
            )
            gid_candidate = _require_replay_gid_candidate(
                work,
                authority,
                gallery_id,
                preparation,
            )
            if stage == _STAGE_VALIDATE_GID_CANDIDATE and gid_candidate is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="replayed GID-candidate live row count",
                )
        return live_count
    if stage in {_STAGE_GID_WINNER, _STAGE_VALIDATE_GID_WINNER}:
        for row in selected:
            gid = require_positive_int63(row[0], field="replayed GID winner key")
            winner = _require_replay_gid_winner(
                work,
                authority,
                gid,
            )
            if stage == _STAGE_VALIDATE_GID_WINNER and winner is not None:
                live_count = _sum_int63(
                    live_count,
                    1,
                    field="replayed GID-winner live row count",
                )
        if not selected:
            _require_complete_gid_winner_keyspace(work, authority.analysis_id)
        return live_count
    raise AnalysisCorruptionError("analysis replay stage is not registered")


def _require_replay_key_rows(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    selected: Sequence[tuple[Any, ...]],
    *,
    table: str,
    key_column: str,
    digest: bool,
) -> None:
    registered = {
        ("catalog_analysis_changed_file_hashes", "file_sha256", True),
        ("catalog_analysis_impacted_galleries", "gallery_id", False),
    }
    if (table, key_column, digest) not in registered:
        raise ValueError("unregistered replay materialization")
    for row in selected:
        key: bytes | int = (
            require_digest32(row[0], field="replayed materialized digest")
            if digest
            else require_positive_int63(row[0], field="replayed materialized integer")
        )
        if not work.connector.fetch_one(
            f"SELECT 1 FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
            (analysis_id, key),
        ):
            raise AnalysisCorruptionError(
                "analysis page is missing a derived materialization"
            )


def _require_replay_file_decision(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    digest: bytes,
    *,
    require_delta: bool,
    delta: AnalysisExclusionDeltaFamily | None,
) -> _Decision | None:
    target = _evaluate_file_decision(work, authority, digest)
    parent = _resolved_decision(work, authority.baseline_analysis_id, digest)
    if require_delta:
        parent_policy = (
            authority.policy
            if authority.baseline_analysis_id is None
            else _analysis_policy(work, authority.baseline_analysis_id)
        )
        expected_delta = (
            _excluded(parent, parent_policy),
            _excluded(target, authority.policy),
        )
        if delta is None or (delta.old_excluded, delta.new_excluded) != expected_delta:
            raise AnalysisCorruptionError(
                "file-decision exclusion delta differs from its evaluator"
            )
    shadow = _shadow_decision(work, authority.analysis_id, digest)
    tombstone = bool(
        work.connector.fetch_one(
            "SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone "
            "WHERE analysis_id = %s AND file_sha256 = %s",
            (authority.analysis_id, digest),
        )
    )
    _require_overlay_exact(
        label="file decision",
        overlay_depth=authority.overlay_depth,
        target=target,
        parent=parent,
        shadow=shadow,
        tombstone=tombstone,
    )
    if _resolved_decision(work, authority.analysis_id, digest) != target:
        raise AnalysisCorruptionError(
            "resolved file decision differs from its evaluator"
        )
    return target


def _require_replay_impacted_content(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
    impact_page: _ContentImpactPage,
) -> None:
    expected_rows: list[tuple[int, bytes]] = []
    for row, preparation in zip(selected, preparations, strict=True):
        gallery_id = require_positive_int63(
            row[0],
            field="replayed impacted-content gallery_id",
        )
        old = impact_page.old_candidates[gallery_id]
        gallery_contents = {
            value
            for value in (
                None if old is None else old.content_sha256,
                None if preparation is None else preparation.content_sha256,
            )
            if value is not None
        }
        expected_rows.extend(
            (gallery_id, content) for content in sorted(gallery_contents)
        )
    boundary = None if after is None else int.from_bytes(after, "big")
    through = (
        None
        if not selected
        else require_positive_int63(
            selected[-1][0],
            field="replayed impacted-content through gallery_id",
        )
    )
    try:
        require_exact_analysis_impacted_content_provenance_page(
            work.connector,
            analysis_id=authority.analysis_id,
            after_gallery_id=boundary,
            through_gallery_id=through,
            expected=tuple(expected_rows),
        )
        if not selected:
            require_complete_analysis_impacted_content_keyspace(
                work.connector,
                analysis_id=authority.analysis_id,
            )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error


def _require_replay_content_candidate(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    preparation: AnalysisGalleryPreparation | None,
) -> _ContentCandidate | None:
    target = (
        None
        if preparation is None
        else _content_candidate_from_preparation(preparation)
    )
    parent = _resolved_content_candidate(
        work,
        authority.baseline_analysis_id,
        gallery_id,
    )
    shadow = _shadow_content_candidate(work, authority.analysis_id, gallery_id)
    tombstone = _has_key(
        work,
        "catalog_analysis_content_owner_candidate_tombstones",
        "gallery_id",
        authority.analysis_id,
        gallery_id,
    )
    _require_overlay_exact(
        label="content candidate",
        overlay_depth=authority.overlay_depth,
        target=target,
        parent=parent,
        shadow=shadow,
        tombstone=tombstone,
    )
    if _resolved_content_candidate(work, authority.analysis_id, gallery_id) != target:
        raise AnalysisCorruptionError(
            "resolved content candidate differs from its evaluator"
        )
    return target


def _require_replay_content_owner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    content: bytes,
) -> _ContentOwner | None:
    target = _evaluate_content_owner(work, authority, content)
    parent = _resolved_content_owner(
        work,
        authority.baseline_analysis_id,
        content,
    )
    shadow = _shadow_content_owner(work, authority.analysis_id, content)
    tombstone = _has_key(
        work,
        "catalog_analysis_content_owner_tombstones",
        "content_sha256",
        authority.analysis_id,
        content,
    )
    _require_overlay_exact(
        label="content owner",
        overlay_depth=authority.overlay_depth,
        target=target,
        parent=parent,
        shadow=shadow,
        tombstone=tombstone,
    )
    if _resolved_content_owner(work, authority.analysis_id, content) != target:
        raise AnalysisCorruptionError(
            "resolved content owner differs from its evaluator"
        )
    return target


def _require_replay_impacted_gid(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    selected: Sequence[tuple[Any, ...]],
    impact_page: _GidImpactPage,
) -> None:
    expected_rows: list[tuple[int, int]] = []
    for row in selected:
        gallery_id = require_positive_int63(
            row[0],
            field="replayed impacted-GID gallery_id",
        )
        old_gid = impact_page.old_gids[gallery_id]
        expected = {
            value
            for value in (
                old_gid,
                impact_page.current_gids[gallery_id],
            )
            if value is not None
        }
        expected_rows.extend((gallery_id, gid) for gid in sorted(expected))
    boundary = None if after is None else int.from_bytes(after, "big")
    through = (
        None
        if not selected
        else require_positive_int63(
            selected[-1][0],
            field="replayed impacted-GID through gallery_id",
        )
    )
    try:
        require_exact_analysis_impacted_gid_provenance_page(
            work.connector,
            analysis_id=authority.analysis_id,
            after_gallery_id=boundary,
            through_gallery_id=through,
            expected=tuple(expected_rows),
        )
        if not selected:
            require_complete_analysis_impacted_gid_keyspace(
                work.connector,
                analysis_id=authority.analysis_id,
            )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error


def _require_replay_gid_candidate(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    preparation: AnalysisGalleryPreparation | None,
) -> _GidCandidate | None:
    target = (
        None
        if preparation is None
        else _gid_candidate_from_preparation(work, authority, preparation)
    )
    parent = _resolved_gid_candidate(
        work,
        authority.baseline_analysis_id,
        gallery_id,
    )
    shadow = _shadow_gid_candidate(work, authority.analysis_id, gallery_id)
    tombstone = _has_key(
        work,
        "catalog_analysis_gid_candidate_tombstones",
        "gallery_id",
        authority.analysis_id,
        gallery_id,
    )
    _require_overlay_exact(
        label="GID candidate",
        overlay_depth=authority.overlay_depth,
        target=target,
        parent=parent,
        shadow=shadow,
        tombstone=tombstone,
    )
    if _resolved_gid_candidate(work, authority.analysis_id, gallery_id) != target:
        raise AnalysisCorruptionError(
            "resolved GID candidate differs from its evaluator"
        )
    return target


def _require_replay_gid_winner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gid: int,
) -> _GidWinner | None:
    target = _evaluate_gid_winner(work, authority, gid)
    parent = _resolved_gid_winner(work, authority.baseline_analysis_id, gid)
    shadow = _shadow_gid_winner(work, authority.analysis_id, gid)
    tombstone = _has_key(
        work,
        "catalog_analysis_gid_winner_tombstones",
        "gid",
        authority.analysis_id,
        gid,
    )
    _require_overlay_exact(
        label="GID winner",
        overlay_depth=authority.overlay_depth,
        target=target,
        parent=parent,
        shadow=shadow,
        tombstone=tombstone,
    )
    if _resolved_gid_winner(work, authority.analysis_id, gid) != target:
        raise AnalysisCorruptionError("resolved GID winner differs from its evaluator")
    return target


def _require_stage_complete(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    stage: bytes,
) -> None:
    row = work.connector.fetch_one(
        "SELECT state FROM catalog_analysis_checkpoints "
        "WHERE analysis_id = %s AND stage = %s",
        (analysis_id, stage),
    )
    if row != (_CHECKPOINT_COMPLETE,):
        raise AnalysisNotReadyError(
            f"prerequisite analysis stage {stage!r} is incomplete"
        )


def _require_component_sealed(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    component: bytes,
) -> None:
    try:
        family = load_analysis_state_component_family(
            work.connector,
            analysis_id=analysis_id,
            state_component=component,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        raise AnalysisNotReadyError(
            f"analysis component {component!r} is not independently sealed"
        )


def _require_unsealed_component(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    component: bytes,
) -> None:
    try:
        family = load_analysis_state_component_family(
            work.connector,
            analysis_id=analysis_id,
            state_component=component,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is not None:
        raise AnalysisCorruptionError(
            f"component {component!r} seal exists while validation is OPEN"
        )


def _finish_component_validation(
    work: VNextUnitOfWork,
    *,
    authority: _RunAuthority,
    stage: bytes,
    component: bytes,
    batch_key: bytes,
    checkpoint: _Checkpoint,
    cursor_kind: bytes,
    next_key: bytes | None,
    selected_count: int,
    live_count: int,
    terminal: bool,
    now: int,
) -> AnalysisBatchResult:
    result = _commit_batch(
        work,
        authority=authority,
        stage=stage,
        batch_key=batch_key,
        checkpoint=checkpoint,
        cursor=_encode_cursor(cursor_kind, next_key, live_count=live_count),
        row_count=selected_count,
        terminal=terminal,
        now=now,
    )
    if not terminal:
        return result
    try:
        ensure_analysis_state_component_family(
            work.connector,
            analysis_id=authority.analysis_id,
            state_component=component,
            row_count=live_count,
            sealed_at=result.committed_at,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    return AnalysisBatchResult(
        result.analysis_id,
        result.stage,
        result.batch_key,
        result.start_generation,
        result.start_cursor,
        result.start_processed_count,
        result.page_limit,
        result.next_cursor,
        result.next_processed_count,
        result.next_state,
        result.row_count,
        result.terminal,
        result.committed_generation,
        result.committed_at,
        result.replayed,
        True,
    )


def _require_overlay_exact(
    *,
    label: str,
    overlay_depth: int,
    target: object | None,
    parent: object | None,
    shadow: object | None,
    tombstone: bool,
) -> None:
    if shadow is not None and tombstone:
        raise AnalysisCorruptionError(
            f"{label} key exists in both shadow and tombstone"
        )
    if overlay_depth == 0:
        expected_shadow = target
        expected_tombstone = False
    elif target is None and parent is not None:
        expected_shadow = None
        expected_tombstone = True
    elif target is not None and target != parent:
        expected_shadow = target
        expected_tombstone = False
    else:
        expected_shadow = None
        expected_tombstone = False
    if shadow != expected_shadow or tombstone != expected_tombstone:
        raise AnalysisCorruptionError(
            f"{label} shadow/tombstone differs from the full evaluator"
        )


def _has_key(
    work: VNextUnitOfWork,
    table: str,
    key_column: str,
    analysis_id: bytes,
    key: bytes | int,
) -> bool:
    registered = {
        (
            "catalog_analysis_content_owner_candidate_tombstones",
            "gallery_id",
        ),
        ("catalog_analysis_content_owner_tombstones", "content_sha256"),
        ("catalog_analysis_gid_candidate_tombstones", "gallery_id"),
        ("catalog_analysis_gid_winner_tombstones", "gid"),
    }
    if (table, key_column) not in registered:
        raise ValueError("unregistered overlay tombstone lookup")
    return bool(
        work.connector.fetch_one(
            f"SELECT 1 FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
            (analysis_id, key),
        )
    )


def _workset_gallery_rows(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    boundary = 0 if after is None else require_positive_int63(after, field="gallery")
    return work.connector.fetch_all(
        "SELECT gallery_id FROM catalog_analysis_impacted_galleries "
        "WHERE analysis_id = %s AND gallery_id > %s "
        "ORDER BY gallery_id LIMIT %s",
        (analysis_id, boundary, limit),
    )


def _workset_content_rows(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    parameters: list[Any] = [analysis_id]
    predicate = ""
    if after is not None:
        predicate = " AND content_sha256 > %s"
        parameters.append(require_digest32(after, field="content cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT content_sha256 FROM catalog_analysis_impacted_content "
        "WHERE analysis_id = %s" + predicate + " ORDER BY content_sha256 LIMIT %s",
        tuple(parameters),
    )


def _workset_gid_rows(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    boundary = 0 if after is None else require_positive_int63(after, field="gid")
    return work.connector.fetch_all(
        "SELECT gid FROM catalog_analysis_impacted_gid "
        "WHERE analysis_id = %s AND gid > %s ORDER BY gid LIMIT %s",
        (analysis_id, boundary, limit),
    )


def _proposed_gallery_cte(gallery_ids: Sequence[int]) -> tuple[str, tuple[int, ...]]:
    galleries = tuple(
        require_positive_int63(gallery, field="proposed gallery_id")
        for gallery in gallery_ids
    )
    if (
        not galleries
        or len(galleries) > _MAX_BATCH_ROWS
        or len(set(galleries)) != len(galleries)
    ):
        raise ValueError("proposed galleries must be one nonempty bounded set")
    if tuple(sorted(galleries)) != galleries:
        raise ValueError("proposed galleries are not strictly ordered")
    sql = " UNION ALL ".join(
        "SELECT %s AS gallery_id" if index == 0 else "SELECT %s"
        for index, _gallery in enumerate(galleries)
    )
    return sql, galleries


def _current_memberships_for_page(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_ids: Sequence[int],
) -> dict[int, int | None]:
    if not gallery_ids:
        return {}
    proposed, parameters = _proposed_gallery_cte(gallery_ids)
    rows = work.connector.fetch_all(
        "WITH proposed(gallery_id) AS ("
        + proposed
        + ") SELECT proposed.gallery_id, member.observation_id "
        "FROM proposed LEFT JOIN catalog_source_build_galleries AS member "
        "ON member.build_id = %s AND member.gallery_id = proposed.gallery_id "
        "ORDER BY proposed.gallery_id LIMIT 129",
        (*parameters, authority.build_id),
    )
    if len(rows) != len(parameters):
        raise AnalysisCorruptionError("current membership page changed shape")
    result: dict[int, int | None] = {}
    for expected, row in zip(parameters, rows, strict=True):
        if len(row) != 2 or row[0] != expected:
            raise AnalysisCorruptionError("current membership page changed order")
        result[expected] = (
            None
            if row[1] is None
            else require_positive_int63(row[1], field="current observation_id")
        )
    return result


def _require_transition_preparations(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
    *,
    memberships: dict[int, int | None] | None = None,
) -> tuple[AnalysisGalleryPreparation | None, ...]:
    """Require one live plan, or exact ``None``, for each current/removed key."""

    return _require_validation_preparations(
        work,
        authority,
        selected,
        preparations,
        memberships=memberships,
    )


def _require_validation_preparations(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
    *,
    memberships: dict[int, int | None] | None = None,
) -> tuple[AnalysisGalleryPreparation | None, ...]:
    exact = tuple(preparations)
    if len(exact) != len(selected):
        raise AnalysisNotReadyError(
            "validation preparations do not cover the exact server keyset"
        )
    gallery_ids = tuple(
        require_positive_int63(row[0], field="validation selected gallery_id")
        for row in selected
    )
    exact_memberships = (
        _current_memberships_for_page(work, authority, gallery_ids)
        if memberships is None
        else memberships
    )
    if set(exact_memberships) != set(gallery_ids):
        raise AnalysisCorruptionError("current membership page differs from keyset")
    shared_receipt: AnalysisPreparationAuthority | None = None
    for gallery_id, preparation in zip(gallery_ids, exact, strict=True):
        observation_id = exact_memberships[gallery_id]
        if observation_id is None:
            if preparation is not None:
                raise AnalysisNotReadyError(
                    "removed gallery received a live preparation"
                )
            continue
        if not isinstance(preparation, AnalysisGalleryPreparation):
            raise AnalysisNotReadyError(
                "live validation gallery lacks a repository preparation"
            )
        preparation.__post_init__()
        if (
            preparation.gallery_id != gallery_id
            or preparation.observation_id != observation_id
        ):
            raise AnalysisNotReadyError(
                "validation preparation differs from current membership"
            )
        _validate_preparation_authority(
            work=None,
            run=authority,
            preparation=preparation,
        )
        if shared_receipt is None:
            shared_receipt = preparation.authority
        elif preparation.authority != shared_receipt:
            raise AnalysisNotReadyError(
                "validation preparations carry different authority receipts"
            )
    if shared_receipt is not None:
        _validate_authority_receipt(work, authority, shared_receipt)
    return exact


def _component_seal_receipts(
    work: VNextUnitOfWork,
    analysis_id: bytes,
) -> tuple[tuple[bytes, int, int], ...]:
    try:
        families = load_analysis_state_component_families(
            work.connector,
            analysis_id=analysis_id,
            limit=6,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    return tuple(
        (family.state_component, family.row_count, family.sealed_at)
        for family in families
    )


def _load_preparation_authority(
    work: VNextUnitOfWork,
    receipt: AnalysisPreparationAuthority,
    *,
    allow_complete: bool = False,
) -> _RunAuthority:
    if receipt._capability is not _PREPARATION_TOKEN:
        raise TypeError("preparation authority is not repository-issued")
    try:
        family = load_analysis_run_family(
            work.connector,
            analysis_id=receipt.analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    expected_prefix = (
        receipt.build_id,
        receipt.policy_id,
        receipt.input_manifest_sha256,
    )
    allowed_states = {"OPEN", "COMPLETE"} if allow_complete else {"OPEN"}
    if (
        family is None
        or (
            family.build_id,
            family.policy_id,
            family.input_manifest_sha256,
        )
        != expected_prefix
        or family.state not in allowed_states
    ):
        raise AnalysisNotReadyError(
            "preparation authority differs from the immutable writable run"
        )
    mapping = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (receipt.generation,),
    )
    if mapping != (receipt.build_id,):
        raise AnalysisNotReadyError("preparation generation mapping changed")
    working = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
        (1,),
    )
    if working != (receipt.build_id,):
        raise AnalysisNotReadyError("preparation build lost the working slot")
    _require_source_build_sealed(work.connector, receipt.build_id)
    if _component_seal_receipts(work, receipt.analysis_id) != receipt.component_seals:
        raise AnalysisNotReadyError("preparation component seal set changed")
    baseline_row = work.connector.fetch_one(
        "SELECT base_analysis_id FROM catalog_analysis_baselines "
        "WHERE analysis_id = %s",
        (receipt.analysis_id,),
    )
    baseline = (
        None
        if not baseline_row
        else require_uuid16(baseline_row[0], field="preparation baseline")
    )
    _persisted_baseline, _anchor, depth, _ancestry = _load_layout(
        work,
        receipt.analysis_id,
    )
    if depth > _MAX_OVERLAY_DEPTH:
        raise AnalysisCorruptionError("preparation overlay depth exceeds 16")
    return _RunAuthority(
        receipt.analysis_id,
        receipt.build_id,
        _load_policy(work, receipt.policy_id),
        baseline,
        depth,
    )


def _validate_preparation_authority(
    *,
    work: VNextUnitOfWork | None,
    run: _RunAuthority,
    preparation: AnalysisGalleryPreparation,
) -> None:
    receipt = preparation.authority
    if work is not None:
        _validate_authority_receipt(work, run, receipt)
    elif (
        receipt._capability is not _PREPARATION_TOKEN
        or receipt.analysis_id != run.analysis_id
        or receipt.build_id != run.build_id
        or receipt.policy_id != run.policy.policy_id
    ):
        raise AnalysisNotReadyError("gallery preparation authority changed")


def _validate_authority_receipt(
    work: VNextUnitOfWork,
    run: _RunAuthority,
    receipt: AnalysisPreparationAuthority,
) -> None:
    if (
        receipt._capability is not _PREPARATION_TOKEN
        or receipt.analysis_id != run.analysis_id
        or receipt.build_id != run.build_id
        or receipt.policy_id != run.policy.policy_id
    ):
        raise AnalysisNotReadyError("preparation authority changed")
    current_generation = _generation_for_build(work, run.build_id)
    if current_generation != receipt.generation:
        raise AnalysisNotReadyError("preparation generation is stale")
    try:
        family = load_analysis_run_family(
            work.connector,
            analysis_id=run.analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None or family.input_manifest_sha256 != receipt.input_manifest_sha256:
        raise AnalysisCorruptionError("preparation input manifest changed")
    _require_source_build_sealed(work.connector, run.build_id)
    if _component_seal_receipts(work, run.analysis_id) != receipt.component_seals:
        raise AnalysisNotReadyError("preparation component seal receipt changed")


def _consume_effective_content_claims(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    preparations: Sequence[AnalysisGalleryPreparation | None],
    *,
    preexisting_contents: frozenset[bytes],
) -> None:
    plans: dict[bytes, CanonicalValueUploadPlan] = {}
    generation: int | None = None
    for preparation in preparations:
        if preparation is None or preparation.content_sha256 is None:
            continue
        plan = preparation.content_upload_plan
        content = preparation.content_sha256
        if plan is None:
            raise AnalysisCorruptionError("content claim handoff has no canonical plan")
        if (
            plan.digest_domain != _EFFECTIVE_CONTENT_DOMAIN
            or plan.value_sha256 != content
        ):
            raise AnalysisCorruptionError("content plan differs from preparation")
        if generation is None:
            generation = preparation.authority.generation
        elif generation != preparation.authority.generation:
            raise AnalysisNotReadyError(
                "content claim handoff spans multiple live generations"
            )
        previous = plans.get(content)
        if previous is not None and (
            previous.digest_domain,
            previous.byte_count,
            previous.root_page_sha256,
        ) != (plan.digest_domain, plan.byte_count, plan.root_page_sha256):
            raise AnalysisCorruptionError(
                "duplicate content preparations disagree on canonical identity"
            )
        plans[content] = plan
    if not plans:
        return
    if generation is None:
        raise AnalysisCorruptionError("content claim handoff lost its generation")
    contents = tuple(sorted(plans))
    try:
        allocations = load_sealed_value_identities(
            work.connector,
            value_sha256s=contents,
        )
    except CanonicalValueCollisionError as error:
        raise AnalysisCorruptionError(
            "effective-content canonical identity is partial or corrupt"
        ) from error
    if set(allocations) != set(contents):
        raise AnalysisNotReadyError(
            "effective-content canonical identity set is not exactly sealed"
        )
    for content in contents:
        allocation = allocations[content]
        plan = plans[content]
        if (
            allocation.digest_domain != _EFFECTIVE_CONTENT_DOMAIN
            or allocation.byte_count != plan.byte_count
            or allocation.root_page_sha256 != plan.root_page_sha256
        ):
            raise AnalysisCorruptionError(
                "effective-content canonical identity differs from preparation"
            )
    placeholders = ", ".join("%s" for _content in contents)
    lock_keys = tuple(
        sorted(
            encode_lock_key("analysis-content-upload", generation, content)
            for content in contents
        )
    )
    claims = work.lock_rows(
        LockRank.CHECKPOINT,
        lock_keys,
        "SELECT generation, value_sha256 "
        "FROM operational_canonical_value_uploads "
        f"WHERE generation = %s AND value_sha256 IN ({placeholders}) "
        "ORDER BY value_sha256",
        (generation, *contents),
    )
    claimed: set[bytes] = set()
    previous_claim: bytes | None = None
    for claim in claims:
        if len(claim) != 2 or claim[0] != generation:
            raise AnalysisCorruptionError("effective-content upload claim changed")
        content = require_digest32(claim[1], field="content upload claim digest")
        if content not in plans or (
            previous_claim is not None and content <= previous_claim
        ):
            raise AnalysisCorruptionError(
                "effective-content upload claim set changed shape"
            )
        claimed.add(content)
        previous_claim = content
    missing = set(contents) - claimed
    if not missing.issubset(preexisting_contents):
        raise AnalysisNotReadyError(
            "effective-content handoff requires its live-generation upload claim"
        )
    if not claimed:
        return
    claimed_contents = tuple(sorted(claimed))
    delete_placeholders = ", ".join("%s" for _content in claimed_contents)
    deleted = work.connector.execute_affected(
        "DELETE FROM operational_canonical_value_uploads "
        f"WHERE generation = %s AND value_sha256 IN ({delete_placeholders})",
        (generation, *claimed_contents),
    )
    if deleted != len(claimed_contents):
        raise AnalysisCorruptionError(
            "effective-content upload claim changed during handoff"
        )


def _generation_for_build(work: VNextUnitOfWork, build_id: bytes) -> int:
    """Return the newest generation mapped to ``build_id``.

    An expired-lease takeover of the same build retains the earlier
    generation mapping for cleanup authority and adds the live one, so the
    build can legitimately own several mapping rows.  The live authority is
    always the newest mapping; reading an arbitrary row would report a valid
    takeover receipt as stale.
    """

    row = work.connector.fetch_one(
        "SELECT generation FROM operational_source_build_generations "
        "WHERE build_id = %s ORDER BY generation DESC LIMIT 1",
        (build_id,),
    )
    if len(row) != 1:
        raise AnalysisNotReadyError("analysis build has no live generation")
    return require_positive_int63(row[0], field="analysis build generation")


def _read_analysis_authority(
    work: VNextUnitOfWork,
    analysis_id: bytes,
) -> _RunAuthority:
    """Load immutable analysis/build authority without requiring live ownership."""

    analysis = require_uuid16(analysis_id, field="historical analysis_id")
    try:
        family = load_analysis_run_family(work.connector, analysis_id=analysis)
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None or family.state != "COMPLETE":
        raise AnalysisCorruptionError("historical analysis is not exactly COMPLETE")
    baseline, _anchor, depth, _ancestry = _load_layout(work, analysis)
    return _RunAuthority(
        analysis,
        family.build_id,
        _load_policy(work, family.policy_id),
        baseline,
        depth,
    )


def _load_content_impact_page(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_ids: Sequence[int],
) -> _ContentImpactPage:
    """Load current membership and baseline content authority in one page read."""

    galleries = tuple(gallery_ids)
    if not galleries:
        return _ContentImpactPage({}, {})
    baseline = (
        None
        if authority.baseline_analysis_id is None
        else _read_analysis_authority(work, authority.baseline_analysis_id)
    )
    baseline_build_id = None if baseline is None else baseline.build_id
    baseline_analysis_id = None if baseline is None else baseline.analysis_id
    proposed, parameters = _proposed_gallery_cte(galleries)
    rows = work.connector.fetch_all(
        "WITH proposed(gallery_id) AS ("
        + proposed
        + ") SELECT proposed.gallery_id, current_member.observation_id, "
        "baseline_member.observation_id, baseline_metadata.gid, "
        "baseline_metadata.download_time, candidate.content_sha256, "
        "candidate.prefer_not_already_uploaded, candidate.title_scalar_count, "
        "candidate.download_time "
        "FROM proposed LEFT JOIN catalog_source_build_galleries AS current_member "
        "ON current_member.build_id = %s "
        "AND current_member.gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_source_build_galleries AS baseline_member "
        "ON baseline_member.build_id = %s "
        "AND baseline_member.gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_gallery_observation_metadata AS baseline_metadata "
        "ON baseline_metadata.gallery_id = baseline_member.gallery_id "
        "AND baseline_metadata.observation_id = baseline_member.observation_id "
        "LEFT JOIN catalog_analysis_content_owner_candidate_resolved AS candidate "
        "ON candidate.analysis_id = %s "
        "AND candidate.gallery_id = proposed.gallery_id "
        "ORDER BY proposed.gallery_id LIMIT 129",
        (
            *parameters,
            authority.build_id,
            baseline_build_id,
            baseline_analysis_id,
        ),
    )
    if len(rows) != len(galleries):
        raise AnalysisCorruptionError("content impact authority page changed shape")
    current: dict[int, int | None] = {}
    old: dict[int, _ContentCandidate | None] = {}
    for gallery, row in zip(galleries, rows, strict=True):
        if len(row) != 9 or row[0] != gallery:
            raise AnalysisCorruptionError("content impact authority page changed order")
        current[gallery] = (
            None
            if row[1] is None
            else require_positive_int63(row[1], field="current observation_id")
        )
        if row[5] is None:
            if any(value is not None for value in row[6:]):
                raise AnalysisCorruptionError("baseline content row is partial")
            old[gallery] = None
            continue
        if any(value is None for value in row[2:5]):
            raise AnalysisCorruptionError(
                "baseline content candidate lacks baseline build membership"
            )
        candidate = _content_candidate_from_row(
            (row[5], gallery, row[6], row[7], row[8]),
            field="baseline impacted content candidate",
        )
        if candidate is None or candidate.download_time != require_int63(
            row[4], field="baseline metadata download_time"
        ):
            raise AnalysisCorruptionError(
                "baseline content candidate differs from baseline build metadata"
            )
        require_positive_int63(row[2], field="baseline observation_id")
        require_positive_int63(row[3], field="baseline metadata gid")
        old[gallery] = candidate
    return _ContentImpactPage(current, old)


def _load_gid_impact_page(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_ids: Sequence[int],
) -> _GidImpactPage:
    """Load baseline/current eligible GIDs from their pinned builds once."""

    galleries = tuple(gallery_ids)
    if not galleries:
        return _GidImpactPage({}, {})
    baseline = (
        None
        if authority.baseline_analysis_id is None
        else _read_analysis_authority(work, authority.baseline_analysis_id)
    )
    baseline_build_id = None if baseline is None else baseline.build_id
    baseline_analysis_id = None if baseline is None else baseline.analysis_id
    proposed, parameters = _proposed_gallery_cte(galleries)
    rows = work.connector.fetch_all(
        "WITH proposed(gallery_id) AS ("
        + proposed
        + ") SELECT proposed.gallery_id, baseline_member.observation_id, "
        "baseline_metadata.gid, candidate.gallery_id, owner.content_sha256, "
        "current_member.observation_id, current_metadata.gid FROM proposed "
        "LEFT JOIN catalog_source_build_galleries AS baseline_member "
        "ON baseline_member.build_id = %s "
        "AND baseline_member.gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_gallery_observation_metadata AS baseline_metadata "
        "ON baseline_metadata.gallery_id = baseline_member.gallery_id "
        "AND baseline_metadata.observation_id = baseline_member.observation_id "
        "LEFT JOIN catalog_analysis_gid_candidate_resolved AS candidate "
        "ON candidate.analysis_id = %s "
        "AND candidate.gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_analysis_content_owner_resolved AS owner "
        "ON owner.analysis_id = %s "
        "AND owner.owner_gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_source_build_galleries AS current_member "
        "ON current_member.build_id = %s "
        "AND current_member.gallery_id = proposed.gallery_id "
        "LEFT JOIN catalog_gallery_observation_metadata AS current_metadata "
        "ON current_metadata.gallery_id = current_member.gallery_id "
        "AND current_metadata.observation_id = current_member.observation_id "
        "ORDER BY proposed.gallery_id LIMIT 129",
        (
            *parameters,
            baseline_build_id,
            baseline_analysis_id,
            authority.analysis_id,
            authority.build_id,
        ),
    )
    if len(rows) != len(galleries):
        raise AnalysisCorruptionError("GID impact authority page changed shape")
    old: dict[int, int | None] = {}
    current: dict[int, int | None] = {}
    for gallery, row in zip(galleries, rows, strict=True):
        if len(row) != 7 or row[0] != gallery:
            raise AnalysisCorruptionError("GID impact authority page changed order")
        if row[3] is None:
            old[gallery] = None
        else:
            if row[1] is None or row[2] is None:
                raise AnalysisCorruptionError(
                    "baseline GID candidate lacks baseline build membership"
                )
            require_positive_int63(row[1], field="baseline observation_id")
            derived_gid = require_positive_int63(row[2], field="baseline metadata gid")
            if (
                require_positive_int63(row[3], field="baseline candidate gallery_id")
                != gallery
            ):
                raise AnalysisCorruptionError(
                    "baseline GID candidate membership changed gallery"
                )
            old[gallery] = derived_gid
        if row[4] is None:
            current[gallery] = None
        else:
            require_digest32(row[4], field="current owner content_sha256")
            if row[5] is None or row[6] is None:
                raise AnalysisCorruptionError(
                    "current content owner lacks pinned-build metadata"
                )
            require_positive_int63(row[5], field="current observation_id")
            current[gallery] = require_positive_int63(
                row[6], field="current metadata gid"
            )
    return _GidImpactPage(old, current)


def _content_candidate_from_preparation(
    preparation: AnalysisGalleryPreparation,
) -> _ContentCandidate | None:
    if preparation.content_sha256 is None:
        return None
    if (
        preparation.content_prefer_not_already_uploaded is None
        or preparation.content_title_scalar_count is None
        or preparation.content_download_time is None
    ):
        raise AnalysisCorruptionError("content preparation is internally incomplete")
    return _ContentCandidate(
        preparation.content_sha256,
        preparation.gallery_id,
        preparation.content_prefer_not_already_uploaded,
        preparation.content_title_scalar_count,
        preparation.content_download_time,
    )


def _materialize_content_candidate(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    target: _ContentCandidate | None,
    parent: _ContentCandidate | None,
) -> None:
    gallery = require_positive_int63(gallery_id, field="candidate gallery_id")
    if target is not None and target.gallery_id != gallery:
        raise AnalysisCorruptionError("target content candidate changed gallery key")
    if parent is not None and parent.gallery_id != gallery:
        raise AnalysisCorruptionError("parent content candidate changed gallery key")
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_content_candidate_shadow(work, authority.analysis_id, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_content_owner_candidate_tombstones "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (authority.analysis_id, gallery),
        )
    elif target is not None and target != parent:
        _insert_content_candidate_shadow(work, authority.analysis_id, target)


def _insert_content_candidate_shadow(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    candidate: _ContentCandidate,
) -> None:
    try:
        ensure_analysis_content_owner_candidate_shadow_family(
            work.connector,
            AnalysisContentOwnerCandidateShadowFamily(
                analysis_id,
                candidate.gallery_id,
                candidate.content_sha256,
                candidate.prefer_not_already_uploaded,
                candidate.title_scalar_count,
                candidate.download_time,
            ),
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error


def _resolved_content_candidate(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    gallery_id: int,
) -> _ContentCandidate | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT content_sha256, gallery_id, prefer_not_already_uploaded, "
        "title_scalar_count, download_time "
        "FROM catalog_analysis_content_owner_candidate_resolved "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis_id, gallery_id),
    )
    return _content_candidate_from_row(row, field="resolved content candidate")


def _shadow_content_candidate(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    gallery_id: int,
) -> _ContentCandidate | None:
    try:
        family = load_analysis_content_owner_candidate_shadow_family(
            work.connector,
            analysis_id=analysis_id,
            gallery_id=gallery_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        return None
    return _ContentCandidate(
        family.content_sha256,
        family.gallery_id,
        family.prefer_not_already_uploaded,
        family.title_scalar_count,
        family.download_time,
    )


def _content_candidate_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _ContentCandidate | None:
    if not row:
        return None
    if len(row) != 5:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _ContentCandidate(
        require_digest32(row[0], field=f"{field} content_sha256"),
        require_positive_int63(row[1], field=f"{field} gallery_id"),
        require_bool_byte(
            row[2],
            field=f"{field} prefer_not_already_uploaded",
        ),
        require_int63(row[3], field=f"{field} title_scalar_count"),
        require_int63(row[4], field=f"{field} download_time"),
    )


def _content_candidate_validation_keys(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT gallery_id FROM catalog_source_build_galleries WHERE build_id = %s",
        "SELECT gallery_id FROM catalog_analysis_content_owner_candidate_shadows "
        "WHERE analysis_id = %s",
        "SELECT gallery_id FROM catalog_analysis_content_owner_candidate_tombstones "
        "WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.build_id,
        authority.analysis_id,
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT gallery_id FROM catalog_analysis_content_owner_candidate_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    boundary = 0 if after is None else require_positive_int63(after, field="gallery")
    parameters.extend((boundary, limit))
    return work.connector.fetch_all(
        "SELECT keyset.gallery_id FROM ("
        + " UNION ".join(subqueries)
        + ") AS keyset WHERE keyset.gallery_id > %s "
        "ORDER BY keyset.gallery_id LIMIT %s",
        tuple(parameters),
    )


def _evaluate_content_owner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    content_sha256: bytes,
) -> _ContentOwner | None:
    content = require_digest32(content_sha256, field="owner content_sha256")
    last_gallery = 0
    winner: tuple[tuple[int, int, int, int, bytes, bytes], _ContentCandidate] | None = (
        None
    )
    while True:
        rows = work.connector.fetch_all(
            "SELECT candidate.content_sha256, candidate.gallery_id, "
            "candidate.prefer_not_already_uploaded, "
            "candidate.title_scalar_count, candidate.download_time, "
            "metadata.gid, identity.scope_key, identity.locator_sha256 "
            "FROM catalog_analysis_content_owner_candidate_resolved AS candidate "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s AND member.gallery_id = candidate.gallery_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = candidate.gallery_id "
            "WHERE candidate.analysis_id = %s AND candidate.content_sha256 = %s "
            "AND candidate.gallery_id > %s ORDER BY candidate.gallery_id LIMIT %s",
            (
                authority.build_id,
                authority.analysis_id,
                content,
                last_gallery,
                _MAX_BATCH_ROWS,
            ),
        )
        if not rows:
            break
        for row in rows:
            candidate = _content_candidate_from_row(
                row[:5],
                field="owner candidate",
            )
            if candidate is None:
                raise AnalysisCorruptionError("owner candidate row disappeared")
            gid = require_positive_int63(row[5], field="owner candidate gid")
            scope = require_digest32(row[6], field="owner candidate scope_key")
            locator = require_digest32(row[7], field="owner candidate locator")
            order = (
                candidate.prefer_not_already_uploaded,
                candidate.title_scalar_count,
                candidate.download_time,
                gid,
                scope,
                locator,
            )
            if winner is None or order > winner[0]:
                winner = (order, candidate)
            last_gallery = candidate.gallery_id
        if len(rows) < _MAX_BATCH_ROWS:
            break
    if winner is None:
        return None
    selected = winner[1]
    return _ContentOwner(content, selected.gallery_id)


def _materialize_content_owner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    content_sha256: bytes,
    target: _ContentOwner | None,
    parent: _ContentOwner | None,
) -> None:
    content = require_digest32(content_sha256, field="owner materialization content")
    if target is not None:
        if target.content_sha256 != content:
            raise AnalysisCorruptionError("target owner changed content key")
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_content_owner_shadow(work, authority.analysis_id, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_content_owner_tombstones "
            "(analysis_id, content_sha256) VALUES (%s, %s)",
            (authority.analysis_id, content),
        )
    elif target is not None and target != parent:
        _insert_content_owner_shadow(work, authority.analysis_id, target)


def _insert_content_owner_shadow(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    owner: _ContentOwner,
) -> None:
    try:
        ensure_analysis_content_owner_shadow_family(
            work.connector,
            AnalysisContentOwnerShadowFamily(
                analysis_id,
                owner.content_sha256,
                owner.owner_gallery_id,
            ),
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error


def _resolved_content_owner(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    content_sha256: bytes,
) -> _ContentOwner | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT content_sha256, owner_gallery_id "
        "FROM catalog_analysis_content_owner_resolved "
        "WHERE analysis_id = %s AND content_sha256 = %s",
        (analysis_id, content_sha256),
    )
    return _content_owner_from_row(row, field="resolved content owner")


def _shadow_content_owner(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    content_sha256: bytes,
) -> _ContentOwner | None:
    try:
        family = load_analysis_content_owner_shadow_family(
            work.connector,
            analysis_id=analysis_id,
            content_sha256=content_sha256,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        return None
    return _ContentOwner(family.content_sha256, family.owner_gallery_id)


def _content_owner_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _ContentOwner | None:
    if not row:
        return None
    if len(row) != 2:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _ContentOwner(
        require_digest32(row[0], field=f"{field} content_sha256"),
        require_positive_int63(row[1], field=f"{field} owner_gallery_id"),
    )


def _content_owner_validation_keys(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT content_sha256 FROM "
        "catalog_analysis_content_owner_candidate_resolved "
        "WHERE analysis_id = %s",
        "SELECT content_sha256 FROM catalog_analysis_content_owner_shadows "
        "WHERE analysis_id = %s",
        "SELECT content_sha256 FROM catalog_analysis_content_owner_tombstones "
        "WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.analysis_id,
        authority.analysis_id,
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT content_sha256 FROM catalog_analysis_content_owner_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    predicate = ""
    if after is not None:
        predicate = " WHERE keyset.content_sha256 > %s"
        parameters.append(require_digest32(after, field="owner validation cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT keyset.content_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS keyset"
        + predicate
        + " ORDER BY keyset.content_sha256 LIMIT %s",
        tuple(parameters),
    )


def _eligible_gallery_gid(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
) -> int | None:
    owner = work.connector.fetch_one(
        "SELECT content_sha256 FROM catalog_analysis_content_owner_resolved "
        "WHERE analysis_id = %s AND owner_gallery_id = %s",
        (authority.analysis_id, gallery_id),
    )
    if not owner:
        return None
    if len(owner) != 1:
        raise AnalysisCorruptionError("gallery owns multiple resolved content groups")
    require_digest32(owner[0], field="owned content_sha256")
    row = work.connector.fetch_one(
        "SELECT metadata.gid FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_metadata AS metadata "
        "ON metadata.gallery_id = member.gallery_id "
        "AND metadata.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND member.gallery_id = %s",
        (authority.build_id, gallery_id),
    )
    if len(row) != 1:
        raise AnalysisCorruptionError("content owner lacks current GID metadata")
    return require_positive_int63(row[0], field="eligible gallery gid")


def _gid_candidate_from_preparation(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    preparation: AnalysisGalleryPreparation,
) -> _GidCandidate | None:
    _validate_preparation_authority(
        work=work,
        run=authority,
        preparation=preparation,
    )
    gallery_id = preparation.gallery_id
    gid = _eligible_gallery_gid(work, authority, gallery_id)
    if gid is None:
        return None
    if preparation.gid != gid:
        raise AnalysisCorruptionError("GID preparation changed metadata group")
    return _GidCandidate(gallery_id)


def _materialize_gid_candidate(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    target: _GidCandidate | None,
    parent: _GidCandidate | None,
) -> None:
    gallery = require_positive_int63(gallery_id, field="GID candidate gallery")
    if target is not None and target.gallery_id != gallery:
        raise AnalysisCorruptionError("target GID candidate changed gallery key")
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_gid_candidate_shadow(work, authority.analysis_id, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_gid_candidate_tombstones "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (authority.analysis_id, gallery),
        )
    elif target is not None and target != parent:
        _insert_gid_candidate_shadow(work, authority.analysis_id, target)


def _insert_gid_candidate_shadow(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    candidate: _GidCandidate,
) -> None:
    work.connector.execute(
        "INSERT INTO catalog_analysis_gid_candidate_shadows "
        "(analysis_id, gallery_id) VALUES (%s, %s)",
        (analysis_id, *candidate.row),
    )


def _resolved_gid_candidate(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    gallery_id: int,
) -> _GidCandidate | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT gallery_id "
        "FROM catalog_analysis_gid_candidate_resolved "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis_id, gallery_id),
    )
    return _gid_candidate_from_row(row, field="resolved GID candidate")


def _shadow_gid_candidate(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    gallery_id: int,
) -> _GidCandidate | None:
    row = work.connector.fetch_one(
        "SELECT gallery_id "
        "FROM catalog_analysis_gid_candidate_shadows "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis_id, gallery_id),
    )
    return _gid_candidate_from_row(row, field="shadow GID candidate")


def _gid_candidate_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _GidCandidate | None:
    if not row:
        return None
    if len(row) != 1:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _GidCandidate(require_positive_int63(row[0], field=f"{field} gallery_id"))


def _gid_candidate_validation_keys(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT owner_gallery_id AS gallery_id "
        "FROM catalog_analysis_content_owner_resolved WHERE analysis_id = %s",
        "SELECT gallery_id FROM catalog_analysis_gid_candidate_shadows "
        "WHERE analysis_id = %s",
        "SELECT gallery_id FROM catalog_analysis_gid_candidate_tombstones "
        "WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.analysis_id,
        authority.analysis_id,
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT gallery_id FROM catalog_analysis_gid_candidate_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    boundary = 0 if after is None else require_positive_int63(after, field="gallery")
    parameters.extend((boundary, limit))
    return work.connector.fetch_all(
        "SELECT keyset.gallery_id FROM ("
        + " UNION ".join(subqueries)
        + ") AS keyset WHERE keyset.gallery_id > %s "
        "ORDER BY keyset.gallery_id LIMIT %s",
        tuple(parameters),
    )


def _evaluate_gid_winner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gid: int,
) -> _GidWinner | None:
    exact_gid = require_positive_int63(gid, field="winner gid")
    last_gallery = 0
    winner: tuple[tuple[int, int, int, bytes, bytes], int] | None = None
    while True:
        rows = work.connector.fetch_all(
            "SELECT candidate.gallery_id, content.prefer_not_already_uploaded, "
            "content.title_scalar_count, content.download_time, "
            "identity.scope_key, identity.locator_sha256 "
            "FROM catalog_analysis_gid_candidate_resolved AS candidate "
            "JOIN catalog_analysis_content_owner_candidate_resolved AS content "
            "ON content.analysis_id = candidate.analysis_id "
            "AND content.gallery_id = candidate.gallery_id "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s AND member.gallery_id = candidate.gallery_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "AND metadata.gid = %s "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = candidate.gallery_id "
            "WHERE candidate.analysis_id = %s "
            "AND candidate.gallery_id > %s ORDER BY candidate.gallery_id LIMIT %s",
            (
                authority.build_id,
                exact_gid,
                authority.analysis_id,
                last_gallery,
                _MAX_BATCH_ROWS,
            ),
        )
        if not rows:
            break
        for row in rows:
            gallery_id = require_positive_int63(
                row[0], field="winner candidate gallery_id"
            )
            order = (
                require_bool_byte(
                    row[1], field="winner candidate prefer_not_already_uploaded"
                ),
                require_int63(row[2], field="winner candidate title_scalar_count"),
                require_int63(row[3], field="winner candidate download_time"),
                require_digest32(row[4], field="winner candidate scope_key"),
                require_digest32(row[5], field="winner candidate locator"),
            )
            if winner is None or order > winner[0]:
                winner = (order, gallery_id)
            last_gallery = gallery_id
        if len(rows) < _MAX_BATCH_ROWS:
            break
    if winner is None:
        return None
    return _GidWinner(exact_gid, winner[1])


def _materialize_gid_winner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gid: int,
    target: _GidWinner | None,
    parent: _GidWinner | None,
) -> None:
    exact_gid = require_positive_int63(gid, field="winner materialization gid")
    if target is not None and target.gid != exact_gid:
        raise AnalysisCorruptionError("target GID winner changed group key")
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_gid_winner_selection(work, authority.analysis_id, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_gid_winner_tombstones "
            "(analysis_id, gid) VALUES (%s, %s)",
            (authority.analysis_id, exact_gid),
        )
    elif target is not None and target != parent:
        _insert_gid_winner_selection(work, authority.analysis_id, target)


def _insert_gid_winner_selection(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    winner: _GidWinner,
) -> None:
    work.connector.execute(
        "INSERT INTO catalog_analysis_gid_winner_selections "
        "(analysis_id, winner_gallery_id) VALUES (%s, %s)",
        (analysis_id, winner.winner_gallery_id),
    )


def _resolved_gid_winner(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    gid: int,
) -> _GidWinner | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT gid, winner_gallery_id "
        "FROM catalog_analysis_gid_winner_resolved "
        "WHERE analysis_id = %s AND gid = %s",
        (analysis_id, gid),
    )
    return _gid_winner_from_row(row, field="resolved GID winner")


def _shadow_gid_winner(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    gid: int,
) -> _GidWinner | None:
    row = work.connector.fetch_one(
        "SELECT gid, winner_gallery_id "
        "FROM catalog_analysis_gid_winner_shadows "
        "WHERE analysis_id = %s AND gid = %s",
        (analysis_id, gid),
    )
    return _gid_winner_from_row(row, field="shadow GID winner")


def _gid_winner_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _GidWinner | None:
    if not row:
        return None
    if len(row) != 2:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _GidWinner(
        require_positive_int63(row[0], field=f"{field} gid"),
        require_positive_int63(row[1], field=f"{field} winner_gallery_id"),
    )


def _gid_winner_validation_keys(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT gid FROM catalog_analysis_impacted_gid WHERE analysis_id = %s",
        "SELECT gid FROM catalog_analysis_gid_winner_shadows WHERE analysis_id = %s",
        "SELECT gid FROM catalog_analysis_gid_winner_tombstones WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.analysis_id,
        authority.analysis_id,
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT gid FROM catalog_analysis_gid_winner_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    boundary = 0 if after is None else require_positive_int63(after, field="gid")
    parameters.extend((boundary, limit))
    return work.connector.fetch_all(
        "SELECT keyset.gid FROM ("
        + " UNION ".join(subqueries)
        + ") AS keyset WHERE keyset.gid > %s "
        "ORDER BY keyset.gid LIMIT %s",
        tuple(parameters),
    )


def _require_complete_gid_winner_keyspace(
    work: VNextUnitOfWork,
    analysis_id: bytes,
) -> None:
    exact_analysis = require_uuid16(analysis_id, field="GID winner analysis_id")
    orphan = work.connector.fetch_one(
        "SELECT 1 FROM catalog_analysis_gid_winner_selections AS selected "
        "LEFT JOIN catalog_analysis_gid_winner_shadows AS shadow "
        "ON shadow.analysis_id = selected.analysis_id "
        "AND shadow.winner_gallery_id = selected.winner_gallery_id "
        "WHERE selected.analysis_id = %s AND shadow.analysis_id IS NULL LIMIT 1",
        (exact_analysis,),
    )
    duplicate = work.connector.fetch_one(
        "SELECT 1 FROM catalog_analysis_gid_winner_shadows "
        "WHERE analysis_id = %s GROUP BY gid HAVING COUNT(*) <> 1 LIMIT 1",
        (exact_analysis,),
    )
    noncandidate = work.connector.fetch_one(
        "SELECT 1 FROM catalog_analysis_gid_winner_shadows AS shadow "
        "LEFT JOIN catalog_analysis_gid_candidate_resolved AS candidate "
        "ON candidate.analysis_id = shadow.analysis_id "
        "AND candidate.gallery_id = shadow.winner_gallery_id "
        "WHERE shadow.analysis_id = %s AND candidate.analysis_id IS NULL LIMIT 1",
        (exact_analysis,),
    )
    if orphan or duplicate or noncandidate:
        raise AnalysisCorruptionError(
            "GID winner selections do not form one complete candidate-backed keyset"
        )


def _changed_gallery_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    boundary = (
        0 if after is None else require_positive_int63(after, field="gallery cursor")
    )
    if authority.baseline_analysis_id is None:
        return work.connector.fetch_all(
            "SELECT gallery_id, 'ADDED' FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id > %s "
            "ORDER BY gallery_id LIMIT %s",
            (authority.build_id, boundary, limit),
        )
    base_build = _baseline_build_id(work, authority.baseline_analysis_id)
    return work.connector.fetch_all(
        "SELECT delta.gallery_id, delta.change_kind FROM ("
        "SELECT target.gallery_id AS gallery_id, "
        "CASE WHEN base.gallery_id IS NULL THEN 'ADDED' ELSE 'REPLACED' END "
        "AS change_kind FROM catalog_source_build_galleries AS target "
        "LEFT JOIN catalog_source_build_galleries AS base "
        "ON base.build_id = %s AND base.gallery_id = target.gallery_id "
        "WHERE target.build_id = %s AND "
        "(base.gallery_id IS NULL OR base.observation_id <> target.observation_id) "
        "UNION ALL "
        "SELECT base.gallery_id AS gallery_id, 'REMOVED' AS change_kind "
        "FROM catalog_source_build_galleries AS base "
        "LEFT JOIN catalog_source_build_galleries AS target "
        "ON target.build_id = %s AND target.gallery_id = base.gallery_id "
        "WHERE base.build_id = %s AND target.gallery_id IS NULL"
        ") AS delta WHERE delta.gallery_id > %s "
        "ORDER BY delta.gallery_id LIMIT %s",
        (
            base_build,
            authority.build_id,
            authority.build_id,
            base_build,
            boundary,
            limit,
        ),
    )


def _changed_file_hash_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    base_build = (
        None
        if authority.baseline_analysis_id is None
        else _baseline_build_id(work, authority.baseline_analysis_id)
    )
    subqueries = [
        "SELECT occurrence.file_sha256 AS file_sha256 "
        "FROM catalog_analysis_changed_galleries AS changed "
        "JOIN catalog_source_build_galleries AS member "
        "ON member.build_id = %s AND member.gallery_id = changed.gallery_id "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "WHERE changed.analysis_id = %s"
    ]
    parameters: list[Any] = [authority.build_id, authority.analysis_id]
    if base_build is not None:
        subqueries.append(
            "SELECT occurrence.file_sha256 AS file_sha256 "
            "FROM catalog_analysis_changed_galleries AS changed "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s AND member.gallery_id = changed.gallery_id "
            "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
            "ON occurrence.gallery_id = member.gallery_id "
            "AND occurrence.observation_id = member.observation_id "
            "WHERE changed.analysis_id = %s"
        )
        parameters.extend((base_build, authority.analysis_id))
    where = "" if after is None else " WHERE affected.file_sha256 > %s"
    if after is not None:
        parameters.append(require_digest32(after, field="changed hash cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT DISTINCT affected.file_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS affected"
        + where
        + " ORDER BY affected.file_sha256 LIMIT %s",
        tuple(parameters),
    )


def _impacted_gallery_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    boundary = (
        0 if after is None else require_positive_int63(after, field="impact cursor")
    )
    if authority.overlay_depth == 0:
        # A depth-zero analysis (the first one, or a self-only compaction after
        # a policy change or a depth-16 parent) materializes every key of its
        # build; the validators expect a shadow for each of them.
        return work.connector.fetch_all(
            "SELECT gallery_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id > %s "
            "ORDER BY gallery_id LIMIT %s",
            (authority.build_id, boundary, limit),
        )
    subqueries = [
        "SELECT gallery_id FROM catalog_analysis_changed_galleries "
        "WHERE analysis_id = %s"
    ]
    parameters: list[Any] = [authority.analysis_id]
    build_ids = [authority.build_id]
    if authority.baseline_analysis_id is not None:
        build_ids.append(_baseline_build_id(work, authority.baseline_analysis_id))
    for build_id in build_ids:
        subqueries.append(
            "SELECT member.gallery_id AS gallery_id "
            "FROM catalog_analysis_exclusion_delta_changes AS delta "
            "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
            "ON occurrence.file_sha256 = delta.file_sha256 "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.gallery_id = occurrence.gallery_id "
            "AND member.observation_id = occurrence.observation_id "
            "WHERE delta.analysis_id = %s AND member.build_id = %s"
        )
        parameters.extend((authority.analysis_id, build_id))
    # The owner group of every content a changed gallery contributed (through
    # its old or its new observation) can change: an unchanged gallery that
    # shares those file hashes may gain or lose content ownership and GID
    # candidacy when its co-owners are added, modified, or removed.
    for changed_build_id in build_ids:
        for member_build_id in build_ids:
            subqueries.append(
                "SELECT member.gallery_id AS gallery_id "
                "FROM catalog_analysis_changed_galleries AS changed "
                "JOIN catalog_source_build_galleries AS changed_member "
                "ON changed_member.gallery_id = changed.gallery_id "
                "AND changed_member.build_id = %s "
                "JOIN catalog_gallery_observation_file_hash_occurrences AS shared "
                "ON shared.gallery_id = changed_member.gallery_id "
                "AND shared.observation_id = changed_member.observation_id "
                "JOIN catalog_gallery_observation_file_hash_occurrences AS other "
                "ON other.file_sha256 = shared.file_sha256 "
                "JOIN catalog_source_build_galleries AS member "
                "ON member.gallery_id = other.gallery_id "
                "AND member.observation_id = other.observation_id "
                "WHERE changed.analysis_id = %s AND member.build_id = %s"
            )
            parameters.extend(
                (changed_build_id, authority.analysis_id, member_build_id)
            )
    parameters.extend((boundary, limit))
    return work.connector.fetch_all(
        "SELECT impact.gallery_id FROM ("
        + " UNION ".join(subqueries)
        + ") AS impact WHERE impact.gallery_id > %s "
        "ORDER BY impact.gallery_id LIMIT %s",
        tuple(parameters),
    )


def _decision_work_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT file_sha256 FROM catalog_analysis_changed_file_hashes "
        "WHERE analysis_id = %s"
    ]
    parameters: list[Any] = [authority.analysis_id]
    if authority.overlay_depth == 0:
        subqueries.append(
            "SELECT occurrence.file_sha256 AS file_sha256 "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
            "ON occurrence.gallery_id = member.gallery_id "
            "AND occurrence.observation_id = member.observation_id "
            "WHERE member.build_id = %s"
        )
        parameters.append(authority.build_id)
    where = "" if after is None else " WHERE workset.file_sha256 > %s"
    if after is not None:
        parameters.append(require_digest32(after, field="decision cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT workset.file_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS workset"
        + where
        + " ORDER BY workset.file_sha256 LIMIT %s",
        tuple(parameters),
    )


def _materialize_decision(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    file_sha256: bytes,
) -> None:
    target = _evaluate_file_decision(work, authority, file_sha256)
    parent = _resolved_decision(work, authority.baseline_analysis_id, file_sha256)
    parent_policy = (
        authority.policy
        if authority.baseline_analysis_id is None
        else _analysis_policy(work, authority.baseline_analysis_id)
    )
    old_excluded = _excluded(parent, parent_policy)
    new_excluded = _excluded(target, authority.policy)
    insert_analysis_exclusion_delta_family(
        work.connector,
        analysis_id=authority.analysis_id,
        file_sha256=file_sha256,
        old_excluded=old_excluded,
        new_excluded=new_excluded,
    )
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_shadow(work, authority.analysis_id, file_sha256, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            (authority.analysis_id, file_sha256),
        )
    elif target is not None and target != parent:
        _insert_shadow(work, authority.analysis_id, file_sha256, target)


def _insert_shadow(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    file_sha256: bytes,
    decision: _Decision,
) -> None:
    try:
        ensure_analysis_file_hash_decision_shadow_family(
            work.connector,
            AnalysisFileHashDecisionShadowFamily(
                analysis_id,
                file_sha256,
                decision.occurrence_count,
                decision.artist_count,
                decision.maximum_gallery_artist_count,
            ),
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error


def _evaluate_file_decision(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    file_sha256: bytes,
) -> _Decision | None:
    digest = require_digest32(file_sha256, field="file_sha256")
    occurrence_row = work.connector.fetch_one(
        "SELECT CAST(SUM(occurrence.occurrence_count) AS UNSIGNED) "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND occurrence.file_sha256 = %s",
        (authority.build_id, digest),
    )
    if len(occurrence_row) != 1 or occurrence_row[0] is None:
        return None
    occurrence_count = require_positive_int63(
        occurrence_row[0], field="decision occurrence_count"
    )
    artist_row = work.connector.fetch_one(
        "SELECT COUNT(DISTINCT artist.artist_tag_id) "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "LEFT JOIN catalog_gallery_observation_artists AS artist "
        "ON artist.gallery_id = member.gallery_id "
        "AND artist.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND occurrence.file_sha256 = %s",
        (authority.build_id, digest),
    )
    artist_count = require_int63(artist_row[0], field="decision artist_count")
    maximum_row = work.connector.fetch_one(
        "SELECT MAX(per_gallery.artist_count) FROM ("
        "SELECT member.gallery_id, COUNT(artist.artist_tag_id) AS artist_count "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "LEFT JOIN catalog_gallery_observation_artists AS artist "
        "ON artist.gallery_id = member.gallery_id "
        "AND artist.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND occurrence.file_sha256 = %s "
        "GROUP BY member.gallery_id"
        ") AS per_gallery",
        (authority.build_id, digest),
    )
    maximum = require_int63(
        0 if maximum_row[0] is None else maximum_row[0],
        field="decision maximum_gallery_artist_count",
    )
    return _Decision(occurrence_count, artist_count, maximum)


def _excluded(decision: _Decision | None, policy: _Policy) -> int:
    if decision is None:
        return 0
    return int(
        decision.occurrence_count >= policy.spam_occurrence_threshold
        and decision.maximum_gallery_artist_count > 0
        and decision.artist_count
        > policy.spam_artist_threshold * decision.maximum_gallery_artist_count
    )


def _analysis_policy(work: VNextUnitOfWork, analysis_id: bytes) -> _Policy:
    try:
        family = load_analysis_run_family(
            work.connector,
            analysis_id=analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        raise AnalysisCorruptionError("analysis policy lookup lost its run")
    return _load_policy(work, family.policy_id)


def _resolved_decision(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    file_sha256: bytes,
) -> _Decision | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT occurrence_count, artist_count, maximum_gallery_artist_count "
        "FROM catalog_analysis_file_hash_decision_resolved "
        "WHERE analysis_id = %s AND file_sha256 = %s",
        (analysis_id, file_sha256),
    )
    return _decision_from_row(row, field="resolved file decision")


def _shadow_decision(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    file_sha256: bytes,
) -> _Decision | None:
    try:
        family = load_analysis_file_hash_decision_shadow_family(
            work.connector,
            analysis_id=analysis_id,
            file_sha256=file_sha256,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        return None
    return _Decision(
        family.occurrence_count,
        family.artist_count,
        family.maximum_gallery_artist_count,
    )


def _decision_from_row(row: tuple[Any, ...], *, field: str) -> _Decision | None:
    if not row:
        return None
    if len(row) != 3:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _Decision(
        require_positive_int63(row[0], field=f"{field} occurrence_count"),
        require_int63(row[1], field=f"{field} artist_count"),
        require_int63(row[2], field=f"{field} maximum_gallery_artist_count"),
    )


def _validation_key_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    shadow_tables = (
        "catalog_a_file_decision_shadow_anchors",
        "catalog_a_file_decision_shadow_occurrences",
        "catalog_a_file_decision_shadow_artists",
        "catalog_a_file_decision_shadow_gallery_artist_max",
        "catalog_a_file_decision_shadow_seals",
    )
    subqueries = [
        "SELECT occurrence.file_sha256 AS file_sha256 "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "WHERE member.build_id = %s",
        *(
            f"SELECT file_sha256 FROM {table} WHERE analysis_id = %s"
            for table in shadow_tables
        ),
        "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_tombstone "
        "WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.build_id,
        *(authority.analysis_id for _table in shadow_tables),
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    where = "" if after is None else " WHERE keyset.file_sha256 > %s"
    if after is not None:
        parameters.append(require_digest32(after, field="validation cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT keyset.file_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS keyset"
        + where
        + " ORDER BY keyset.file_sha256 LIMIT %s",
        tuple(parameters),
    )


def _baseline_build_id(work: VNextUnitOfWork, baseline_analysis_id: bytes) -> bytes:
    try:
        family = load_analysis_run_family(
            work.connector,
            analysis_id=baseline_analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        raise AnalysisCorruptionError("baseline analysis build is missing")
    return family.build_id


def _component_is_sealed(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    stage: bytes,
) -> bool:
    component = {
        _STAGE_VALIDATE_FILE_HASH: _COMPONENT_FILE_HASH,
        _STAGE_VALIDATE_CONTENT_CANDIDATE: _COMPONENT_CONTENT_CANDIDATE,
        _STAGE_VALIDATE_CONTENT_OWNER: _COMPONENT_CONTENT_OWNER,
        _STAGE_VALIDATE_GID_CANDIDATE: _COMPONENT_GID_CANDIDATE,
        _STAGE_VALIDATE_GID_WINNER: _COMPONENT_GID_WINNER,
    }.get(stage)
    if component is None:
        return False
    try:
        family = load_analysis_state_component_family(
            work.connector,
            analysis_id=analysis_id,
            state_component=component,
        )
    except AnalysisFamilyCollisionError as error:
        raise AnalysisCorruptionError(str(error)) from error
    if family is None:
        return False
    rows = work.connector.fetch_all(
        "SELECT start_cursor, start_processed_count, page_limit, next_cursor, "
        "next_processed_count, next_state, row_count, terminal, committed_at "
        "FROM catalog_analysis_batch_receipts "
        "WHERE analysis_id = %s AND stage = %s AND row_count = %s "
        "ORDER BY start_generation DESC LIMIT 2",
        (analysis_id, stage, 0),
    )
    if len(rows) != 1 or len(rows[0]) != 9:
        raise AnalysisCorruptionError(
            f"component {component!r} has no unique terminal receipt"
        )
    receipt = rows[0]
    try:
        page_limit = require_positive_int63(
            receipt[2],
            field="terminal receipt page_limit",
        )
        start_count = require_int63(
            receipt[1],
            field="terminal receipt start_processed_count",
        )
        next_count = require_int63(
            receipt[4],
            field="terminal receipt next_processed_count",
        )
        terminal = require_int63(receipt[7], field="terminal receipt terminal")
        committed_at = require_int63(
            receipt[8],
            field="terminal receipt committed_at",
        )
        kind, live = _stage_cursor_spec(stage)
        _last, live_count = _decode_cursor(kind, receipt[3], live=live)
    except (TypeError, ValueError) as error:
        raise AnalysisCorruptionError(
            f"component {component!r} terminal receipt is malformed"
        ) from error
    if (
        page_limit > _MAX_BATCH_ROWS
        or receipt[0] != receipt[3]
        or start_count != next_count
        or receipt[5] != _CHECKPOINT_COMPLETE
        or receipt[6] != 0
        or terminal != 1
        or live_count != family.row_count
        or committed_at != family.sealed_at
    ):
        raise AnalysisCorruptionError(
            f"component {component!r} differs from its terminal receipt"
        )
    checkpoint = work.connector.fetch_one(
        "SELECT `cursor`, processed_count, state, updated_at "
        "FROM catalog_analysis_checkpoints WHERE analysis_id = %s AND stage = %s",
        (analysis_id, stage),
    )
    if checkpoint != (
        receipt[3],
        next_count,
        _CHECKPOINT_COMPLETE,
        committed_at,
    ):
        raise AnalysisCorruptionError(
            f"component {component!r} terminal checkpoint differs from its receipt"
        )
    return True


def _require_exact_stage_registry(work: VNextUnitOfWork) -> None:
    rows = work.connector.fetch_all(
        "SELECT stage, stage_order, cursor_codec FROM catalog_analysis_stages "
        "ORDER BY stage_order LIMIT 16"
    )
    expected = [
        (stage, _STAGE_REGISTRY[stage][0], _STAGE_REGISTRY[stage][1])
        for stage in _STAGES
    ]
    if rows != expected:
        raise AnalysisCorruptionError(
            "analysis stage registry differs from the executable closed set"
        )


def _require_registered_stage(
    work: VNextUnitOfWork,
    stage: bytes,
) -> tuple[bytes, bool]:
    kind, live = _stage_cursor_spec(stage)
    expected_order, expected_codec, _kind, _live = _STAGE_REGISTRY[stage]
    row = work.connector.fetch_one(
        "SELECT stage_order, cursor_codec FROM catalog_analysis_stages "
        "WHERE stage = %s",
        (stage,),
    )
    if row != (expected_order, expected_codec):
        raise AnalysisCorruptionError(
            "analysis stage registry row differs from the executable codec"
        )
    return kind, live


def _stage_cursor_spec(stage: bytes) -> tuple[bytes, bool]:
    try:
        _order, _codec, kind, live = _STAGE_REGISTRY[stage]
    except (KeyError, TypeError) as error:
        raise ValueError("analysis stage is not registered") from error
    return kind, live


def _encode_cursor(
    kind: bytes,
    key: bytes | None,
    *,
    live_count: int | None = None,
) -> bytes:
    expected = (
        8
        if kind in {_CURSOR_GALLERY, _CURSOR_GID}
        else 32
        if kind == _CURSOR_DIGEST
        else 0
    )
    if expected == 0:
        raise ValueError("analysis cursor kind is not registered")
    if key is None:
        payload = bytes(expected)
        present = b"\x00"
    else:
        payload = require_bounded_bytes(
            key,
            field="analysis cursor key",
            minimum=expected,
            maximum=expected,
        )
        present = b"\x01"
        if kind in {_CURSOR_GALLERY, _CURSOR_GID}:
            require_positive_int63(
                int.from_bytes(payload, "big"),
                field="analysis cursor integer key",
            )
    result = bytes((_CURSOR_VERSION,)) + kind + present + payload
    if live_count is not None:
        result += require_int63(
            live_count,
            field="analysis cursor live row count",
        ).to_bytes(8, "big")
    return result


def _decode_cursor(
    kind: bytes,
    cursor: bytes,
    *,
    live: bool,
) -> tuple[bytes | None, int]:
    expected_key = (
        8
        if kind in {_CURSOR_GALLERY, _CURSOR_GID}
        else 32
        if kind == _CURSOR_DIGEST
        else 0
    )
    if expected_key == 0:
        raise ValueError("analysis cursor kind is not registered")
    expected_length = 3 + expected_key + (8 if live else 0)
    value = require_bounded_bytes(
        cursor,
        field="analysis checkpoint cursor",
        minimum=expected_length,
        maximum=expected_length,
    )
    if value[0] != _CURSOR_VERSION or value[1:2] != kind or value[2] not in (0, 1):
        raise AnalysisCorruptionError("analysis checkpoint cursor has an unknown codec")
    key_bytes = value[3 : 3 + expected_key]
    if value[2] == 0 and key_bytes != bytes(expected_key):
        raise AnalysisCorruptionError("empty analysis cursor has a nonzero key")
    key = None if value[2] == 0 else key_bytes
    if key is not None and kind in {_CURSOR_GALLERY, _CURSOR_GID}:
        try:
            require_positive_int63(
                int.from_bytes(key, "big"),
                field="analysis cursor integer key",
            )
        except (TypeError, ValueError) as error:
            raise AnalysisCorruptionError(
                "analysis cursor contains an invalid integer key"
            ) from error
    live_row_count = 0
    if live:
        live_row_count = require_int63(
            int.from_bytes(value[3 + expected_key :], "big"),
            field="analysis cursor live row count",
        )
    return key, live_row_count


def _sum_int63(left: int, right: int, *, field: str) -> int:
    return require_int63(
        require_int63(left, field=field) + require_int63(right, field=field),
        field=field,
    )


def _require_run_state(value: object) -> str:
    if not isinstance(value, str) or value not in {"OPEN", "COMPLETE", "ABANDONED"}:
        raise AnalysisCorruptionError("analysis run has an unknown state")
    return value


def _require_checkpoint_state(value: object) -> str:
    if not isinstance(value, str) or value not in {
        _CHECKPOINT_OPEN,
        _CHECKPOINT_COMPLETE,
    }:
        raise AnalysisCorruptionError("analysis checkpoint has an unknown state")
    return value
