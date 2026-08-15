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
    "AnalysisUnsupportedError",
]

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from .vnext_domains import (
    INT63_MAX,
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
    require_uuid16,
)
from .vnext_identity import (
    ANALYSIS_ALREADY_UPLOADED_MARKER,
    AnalysisCandidateKind,
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
    analysis_candidate_total_order_key,
    analysis_content_candidate_digest,
    analysis_gid_candidate_digest,
    count_analysis_title_scalars,
    decode_analysis_candidate_priority,
    decode_gallery_observation_page,
    effective_content_digest_ordered,
    encode_analysis_candidate_priority,
    gallery_observation_page_digest,
    iter_effective_content_payload_ordered,
    iter_source_snapshot_manifest_payload_rows_ordered,
    source_snapshot_manifest_digest_ordered,
    validate_analysis_candidate_priority,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
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
_EFFECTIVE_CONTENT_DOMAIN = b"effective_content_v1"
_SNAPSHOT_DOMAIN = b"source_snapshot_manifest_v1"
_METADATA_PREFIX = b"h2hdb-vnext-gallery-observation-metadata\0"
_SNAPSHOT_PREFIX = b"h2hdb-vnext-source-snapshot-manifest\0"
_PREPARATION_TOKEN = object()

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

    @property
    def generation(self) -> int:
        """Compatibility spelling for the committed checkpoint generation."""

        return self.committed_generation

    @property
    def cumulative_row_count(self) -> int:
        """Compatibility spelling for the durable processed-row count."""

        return self.next_processed_count

    @property
    def state(self) -> str:
        """Compatibility spelling for the durable successor state."""

        return self.next_state


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
class _Checkpoint:
    generation: int
    cursor: bytes
    processed_count: int
    state: str
    updated_at: int


@dataclass(frozen=True, slots=True)
class _Decision:
    occurrence_count: int
    artist_count: int
    maximum_gallery_artist_count: int
    evidence_sha256: bytes

    @property
    def row(self) -> tuple[int, int, int, bytes]:
        return (
            self.occurrence_count,
            self.artist_count,
            self.maximum_gallery_artist_count,
            self.evidence_sha256,
        )


@dataclass(frozen=True, slots=True)
class _ContentCandidate:
    content_sha256: bytes
    gallery_id: int
    priority_key: bytes
    candidate_sha256: bytes

    def __post_init__(self) -> None:
        require_digest32(self.content_sha256, field="candidate content_sha256")
        require_positive_int63(self.gallery_id, field="candidate gallery_id")
        require_bounded_bytes(
            self.priority_key,
            field="candidate priority_key",
            minimum=1,
            maximum=128,
        )
        require_digest32(self.candidate_sha256, field="candidate_sha256")

    @property
    def row(self) -> tuple[bytes, int, bytes, bytes]:
        return (
            self.content_sha256,
            self.gallery_id,
            self.priority_key,
            self.candidate_sha256,
        )


@dataclass(frozen=True, slots=True)
class _ContentOwner:
    content_sha256: bytes
    owner_gallery_id: int
    decision_sha256: bytes

    def __post_init__(self) -> None:
        require_digest32(self.content_sha256, field="owner content_sha256")
        require_positive_int63(self.owner_gallery_id, field="owner gallery_id")
        require_digest32(self.decision_sha256, field="owner decision_sha256")

    @property
    def row(self) -> tuple[bytes, int, bytes]:
        return (self.content_sha256, self.owner_gallery_id, self.decision_sha256)


@dataclass(frozen=True, slots=True)
class _GidCandidate:
    gallery_id: int
    gid: int
    priority_key: bytes
    candidate_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="GID candidate gallery_id")
        require_positive_int63(self.gid, field="GID candidate gid")
        require_bounded_bytes(
            self.priority_key,
            field="GID candidate priority_key",
            minimum=1,
            maximum=128,
        )
        require_digest32(self.candidate_sha256, field="GID candidate_sha256")

    @property
    def row(self) -> tuple[int, int, bytes, bytes]:
        return (self.gallery_id, self.gid, self.priority_key, self.candidate_sha256)


@dataclass(frozen=True, slots=True)
class _GidWinner:
    gid: int
    winner_gallery_id: int
    decision_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gid, field="winner gid")
        require_positive_int63(self.winner_gallery_id, field="winner gallery_id")
        require_digest32(self.decision_sha256, field="winner decision_sha256")

    @property
    def row(self) -> tuple[int, int, bytes]:
        return (self.gid, self.winner_gallery_id, self.decision_sha256)


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
class AnalysisGalleryPreparation:
    """Opaque server-derived facts and optional effective-content upload plan."""

    analysis_id: bytes
    build_id: bytes
    gallery_id: int
    observation_id: int
    gid: int
    content_sha256: bytes | None
    content_priority_key: bytes | None
    content_candidate_sha256: bytes | None
    gid_priority_key: bytes
    gid_candidate_sha256: bytes
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
        require_bounded_bytes(
            self.gid_priority_key,
            field="preparation GID priority_key",
            minimum=1,
            maximum=128,
        )
        require_digest32(
            self.gid_candidate_sha256,
            field="preparation GID candidate_sha256",
        )
        content_fields = (
            self.content_sha256,
            self.content_priority_key,
            self.content_candidate_sha256,
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
            if self.content_priority_key is None:
                raise ValueError("content candidate priority is missing")
            require_bounded_bytes(
                self.content_priority_key,
                field="preparation content priority_key",
                minimum=1,
                maximum=128,
            )
            if self.content_candidate_sha256 is None:
                raise ValueError("content candidate audit digest is missing")
            require_digest32(
                self.content_candidate_sha256,
                field="preparation content candidate_sha256",
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
        analysis_attempt_id: bytes,
        now: int,
    ) -> AnalysisRun:
        """Create or resume the one semantic run for ``(build_id, policy_id)``.

        Baseline identity is never accepted from the caller.  It is derived
        from the build's pinned source baseline and its retained provenance.
        """

        build = require_uuid16(build_id, field="analysis build_id")
        policy_key = require_positive_int63(policy_id, field="analysis policy_id")
        attempt = require_uuid16(
            analysis_attempt_id,
            field="analysis attempt_id",
        )
        timestamp = require_int63(now, field="analysis begin now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_exact_stage_registry(work)

        working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("analysis-working-build", 1),
            "SELECT build_id FROM operational_source_working_builds " "WHERE slot = %s",
            (1,),
        )
        if working != (build,):
            raise AnalysisNotReadyError(
                "analysis build is not the sole live source working build"
            )
        _require_generation_mapping(work, generation=generation, build_id=build)

        source_row = work.connector.fetch_one(
            "SELECT state, sealed_at FROM catalog_source_builds WHERE build_id = %s",
            (build,),
        )
        if len(source_row) != 2 or source_row[0] != "SEALED" or source_row[1] is None:
            raise AnalysisNotReadyError(
                "analysis requires an exactly SEALED source build"
            )
        require_int63(source_row[1], field="source build sealed_at")

        manifest_row = work.connector.fetch_one(
            "SELECT manifest_sha256, gallery_count, file_count, byte_count "
            "FROM catalog_build_manifests WHERE build_id = %s",
            (build,),
        )
        if len(manifest_row) != 4:
            raise AnalysisNotReadyError("sealed build has no exact build_manifest")
        manifest = require_digest32(manifest_row[0], field="build manifest_sha256")
        manifest_counts = tuple(
            require_int63(value, field=f"build manifest count {index}")
            for index, value in enumerate(manifest_row[1:])
        )
        policy = _load_policy(work, policy_key)
        input_digest = _analysis_input_digest(manifest, manifest_counts, policy)

        baseline = _derive_baseline(work, build_id=build)
        layout = _derive_layout(work, baseline=baseline, policy=policy)

        existing = work.connector.fetch_one(
            "SELECT analysis_id, input_manifest_sha256, state "
            "FROM catalog_analysis_runs WHERE build_id = %s AND policy_id = %s",
            (build, policy_key),
        )
        if existing:
            existing_id = require_uuid16(existing[0], field="existing analysis_id")
            if (
                require_digest32(existing[1], field="existing input_manifest_sha256")
                != input_digest
            ):
                raise AnalysisCorruptionError(
                    "analysis natural-key replay changed its server-derived input"
                )
            state = _require_run_state(existing[2])
            persisted = _load_layout(work, existing_id)
            expected_anchor = existing_id if layout[0] is None else layout[0]
            expected_ancestry = tuple(
                existing_id if ancestor is None else ancestor for ancestor in layout[2]
            )
            expected = (baseline, expected_anchor, layout[1], expected_ancestry)
            if persisted != expected:
                raise AnalysisCorruptionError(
                    "analysis natural-key replay changed its baseline or ancestry"
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

        work.connector.execute(
            "INSERT INTO catalog_analysis_runs "
            "(analysis_id, build_id, policy_id, input_manifest_sha256, state, "
            "started_at, completed_at) VALUES (%s, %s, %s, %s, %s, %s, NULL)",
            (attempt, build, policy_key, input_digest, "OPEN", timestamp),
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
        work.connector.execute(
            "INSERT INTO catalog_analysis_state_anchors "
            "(analysis_id, anchor_analysis_id, overlay_depth) VALUES (%s, %s, %s)",
            (attempt, anchor_id, overlay_depth),
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
            work.connector.execute(
                "INSERT INTO catalog_analysis_checkpoints "
                "(analysis_id, stage, generation, cursor, processed_count, "
                "state, updated_at) VALUES (%s, %s, 1, %s, 0, %s, %s)",
                (
                    attempt,
                    stage,
                    _encode_cursor(kind, None, live_count=0 if live else None),
                    _CHECKPOINT_OPEN,
                    timestamp,
                ),
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            return replay
        assert checkpoint is not None
        _require_stage_complete(work, authority.analysis_id, _STAGE_FILE_HASH_DECISION)
        existing_seal = work.connector.fetch_one(
            "SELECT row_count FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (authority.analysis_id, _COMPONENT_FILE_HASH),
        )
        if existing_seal:
            raise AnalysisCorruptionError(
                "file-hash seal exists while its validation checkpoint is OPEN"
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
        work.connector.execute(
            "INSERT INTO catalog_analysis_state_component_seals "
            "(analysis_id, state_component, row_count, sealed_at) "
            "VALUES (%s, %s, %s, %s)",
            (authority.analysis_id, _COMPONENT_FILE_HASH, live_count, now),
        )
        return AnalysisBatchResult(
            result.analysis_id,
            result.stage,
            result.batch_key,
            result.start_generation,
            result.start_cursor,
            result.start_processed_count,
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
        row = work.connector.fetch_one(
            "SELECT input_manifest_sha256 FROM catalog_analysis_runs "
            "WHERE analysis_id = %s",
            (authority.analysis_id,),
        )
        if len(row) != 1:
            raise AnalysisCorruptionError("preparation authority lost its run")
        seals = _component_seal_receipts(work, authority.analysis_id)
        return AnalysisPreparationAuthority(
            authority.analysis_id,
            authority.build_id,
            require_positive_int63(
                ingest_turn.generation,
                field="preparation authority generation",
            ),
            authority.policy.policy_id,
            require_digest32(row[0], field="analysis input_manifest_sha256"),
            seals,
            _PREPARATION_TOKEN,
        )

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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
                field="impacted content gallery_id",
            )
            old = _resolved_content_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            if old is not None:
                _insert_impacted_content(
                    work,
                    authority.analysis_id,
                    old.content_sha256,
                )
            if preparation is not None and preparation.content_sha256 is not None:
                _consume_effective_content_claim(
                    work,
                    authority,
                    preparation,
                )
                _insert_impacted_content(
                    work,
                    authority.analysis_id,
                    preparation.content_sha256,
                )
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
        for (raw_gallery_id,) in selected:
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="impacted GID gallery_id",
            )
            old = _resolved_gid_candidate(
                work,
                authority.baseline_analysis_id,
                gallery_id,
            )
            if old is not None:
                _insert_impacted_gid(work, authority.analysis_id, old.gid)
            new_gid = _eligible_gallery_gid(work, authority, gallery_id)
            if new_gid is not None:
                _insert_impacted_gid(work, authority.analysis_id, new_gid)
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
        for (raw_gid,) in selected:
            gid = require_positive_int63(raw_gid, field="GID winner work key")
            target = _evaluate_gid_winner(work, authority, gid)
            parent = _resolved_gid_winner(
                work,
                authority.baseline_analysis_id,
                gid,
            )
            _materialize_gid_winner(work, authority, gid, target, parent)
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
            limit=max_rows + 1,
        )
        selected = rows[:max_rows]
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
                "SELECT state FROM catalog_analysis_runs WHERE analysis_id = %s",
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

        timestamp = require_int63(now, field="snapshot handoff now")
        authority = _authorize_analysis(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            analysis_id=preparation.analysis_id,
            now=timestamp,
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
        state_row = work.connector.fetch_one(
            "SELECT state, completed_at FROM catalog_analysis_runs "
            "WHERE analysis_id = %s",
            (authority.analysis_id,),
        )
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
        if state_row and state_row[0] == "COMPLETE":
            completed_at = require_int63(state_row[1], field="analysis completed_at")
            if completed_at < 0 or binding != (value,):
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
            return value
        if state_row != ("OPEN", None) or binding:
            raise AnalysisCorruptionError(
                "snapshot binding and analysis state are not atomic"
            )

        allocation = work.connector.fetch_one(
            "SELECT a.digest_domain, a.byte_count, i.root_page_sha256 "
            "FROM catalog_canonical_value_allocations AS a "
            "JOIN catalog_canonical_value_identities AS i "
            "ON i.value_sha256 = a.value_sha256 WHERE a.value_sha256 = %s",
            (value,),
        )
        if len(allocation) != 3:
            raise AnalysisNotReadyError("snapshot canonical identity is not sealed")
        if (
            allocation[0] != _SNAPSHOT_DOMAIN
            or require_int63(allocation[1], field="snapshot canonical byte_count")
            != preparation.payload_byte_count
            or require_digest32(allocation[2], field="snapshot root_page_sha256")
            != preparation.upload_plan.root_page_sha256
        ):
            raise AnalysisCorruptionError(
                "snapshot canonical identity differs from the typed preparation"
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
        identity = work.connector.fetch_one(
            "SELECT gallery_count, file_count, byte_count "
            "FROM catalog_source_snapshot_manifest_identity "
            "WHERE snapshot_manifest_sha256 = %s",
            (value,),
        )
        expected_counts = (
            preparation.gallery_count,
            preparation.file_count,
            preparation.byte_count,
        )
        if identity:
            if identity != expected_counts:
                raise AnalysisCorruptionError(
                    "snapshot manifest identity has conflicting aggregate counts"
                )
        else:
            work.connector.execute(
                "INSERT INTO catalog_source_snapshot_manifest_identity "
                "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
                "VALUES (%s, %s, %s, %s)",
                (value, *expected_counts),
            )
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
        work.compare_and_swap(
            "UPDATE catalog_analysis_runs SET state = %s, completed_at = %s "
            "WHERE analysis_id = %s AND state = %s AND completed_at IS NULL",
            ("COMPLETE", timestamp, authority.analysis_id, "OPEN"),
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
        row = work.connector.fetch_one(
            "SELECT run.state, run.completed_at, output.snapshot_manifest_sha256 "
            "FROM catalog_analysis_runs AS run "
            "JOIN catalog_analysis_snapshot_manifest AS output "
            "ON output.analysis_id = run.analysis_id WHERE run.analysis_id = %s",
            (authority.analysis_id,),
        )
        if len(row) == 3 and row[0] == "COMPLETE":
            require_int63(row[1], field="analysis completed_at")
            require_digest32(row[2], field="analysis snapshot_manifest_sha256")
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
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("analysis-run", analysis),
        "SELECT build_id, policy_id, state FROM catalog_analysis_runs "
        "WHERE analysis_id = %s",
        (analysis,),
    )
    if len(row) != 3:
        raise AnalysisNotReadyError("analysis run is missing")
    build = require_uuid16(row[0], field="analysis build_id")
    policy_id = require_positive_int63(row[1], field="analysis policy_id")
    state = _require_run_state(row[2])
    if state != "OPEN" and not (allow_complete and state == "COMPLETE"):
        raise AnalysisNotReadyError(f"analysis run is not writable: {state}")
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
    anchor_row = work.connector.fetch_one(
        "SELECT overlay_depth FROM catalog_analysis_state_anchors "
        "WHERE analysis_id = %s",
        (analysis,),
    )
    if len(anchor_row) != 1:
        raise AnalysisCorruptionError("analysis state anchor is missing")
    depth = require_int63(anchor_row[0], field="analysis overlay_depth")
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
        raise AnalysisNotReadyError("analysis policy is missing")
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


def _prepare_gallery(
    work: VNextUnitOfWork,
    run: _RunAuthority,
    gallery_id: int,
    preparation_authority: AnalysisPreparationAuthority,
) -> AnalysisGalleryPreparation:
    row = work.connector.fetch_one(
        "SELECT member.observation_id, metadata.gid, metadata.download_time, "
        "identity.scope_key, identity.locator_sha256 "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_metadata AS metadata "
        "ON metadata.gallery_id = member.gallery_id "
        "AND metadata.observation_id = member.observation_id "
        "JOIN catalog_gallery_identities AS identity "
        "ON identity.gallery_id = member.gallery_id "
        "WHERE member.build_id = %s AND member.gallery_id = %s",
        (run.build_id, gallery_id),
    )
    if len(row) != 5:
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
    scope_key = require_digest32(row[3], field="preparation scope_key")
    locator = require_digest32(row[4], field="preparation locator_sha256")
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
    content_priority: bytes | None = None
    content_candidate: bytes | None = None
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
        content_priority = _encode_streamed_candidate_priority(
            AnalysisCandidateKind.CONTENT_OWNER,
            marker_present=marker,
            title_scalar_receipt=title_scalar_receipt,
            download_time=download_time,
            gid=persisted_gid,
        )
        content_candidate = analysis_content_candidate_digest(
            run.analysis_id,
            content_sha256,
            content_priority,
            scope_key,
            locator,
        )
    gid_priority = _encode_streamed_candidate_priority(
        AnalysisCandidateKind.GID_WINNER,
        marker_present=marker,
        title_scalar_receipt=title_scalar_receipt,
        download_time=download_time,
        gid=None,
    )
    gid_candidate = analysis_gid_candidate_digest(
        run.analysis_id,
        persisted_gid,
        gid_priority,
        scope_key,
        locator,
    )
    return AnalysisGalleryPreparation(
        run.analysis_id,
        run.build_id,
        gallery_id,
        observation_id,
        persisted_gid,
        content_sha256,
        content_priority,
        content_candidate,
        gid_priority,
        gid_candidate,
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
                b"CONTENT",
                _MAX_BATCH_ROWS,
            )
        else:
            predicate = (
                " AND (source.file_sha256 > %s OR "
                "(source.file_sha256 = %s AND source.file_no > %s))"
            )
            parameters = (
                gallery_id,
                observation_id,
                b"CONTENT",
                previous_digest,
                previous_digest,
                previous_file_no,
                _MAX_BATCH_ROWS,
            )
        rows = work.connector.fetch_all(
            "SELECT source.file_sha256, source.file_no "
            "FROM catalog_gallery_observation_files AS source "
            "JOIN catalog_file_name_identities AS name "
            "ON name.file_key = source.file_key "
            "WHERE source.gallery_id = %s AND source.observation_id = %s "
            "AND name.file_role = %s"
            + predicate
            + " ORDER BY source.file_sha256, source.file_no LIMIT %s",
            parameters,
        )
        if not rows:
            return
        for raw_digest, raw_file_no in rows:
            digest = require_digest32(raw_digest, field="effective file_sha256")
            file_no = require_positive_int63(raw_file_no, field="effective file_no")
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
        "JOIN catalog_gallery_observation_page_descriptors AS descriptor "
        "ON descriptor.page_sha256 = root.root_page_sha256 "
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
            "SELECT page.page_bytes, descriptor.component, descriptor.level, "
            "descriptor.subtree_item_count "
            "FROM catalog_gallery_observation_pages AS page "
            "JOIN catalog_gallery_observation_page_descriptors AS descriptor "
            "ON descriptor.page_sha256 = page.page_sha256 "
            "WHERE page.page_sha256 = %s",
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
        if receipt.byte_count != position:
            raise AnalysisCorruptionError("tag canonical stream count changed")
        if matched and position == len(ANALYSIS_ALREADY_UPLOADED_MARKER):
            return True
    return False


def _encode_streamed_candidate_priority(
    candidate_kind: AnalysisCandidateKind,
    *,
    marker_present: bool,
    title_scalar_receipt: AnalysisTitleScalarReceipt,
    download_time: int,
    gid: int | None,
) -> bytes:
    """Emit and independently validate the fixed streamed-title codec."""

    if not isinstance(title_scalar_receipt, AnalysisTitleScalarReceipt):
        raise TypeError("title_scalar_receipt must be AnalysisTitleScalarReceipt")
    observed_download = require_int63(download_time, field="download_time")
    marker_values = [ANALYSIS_ALREADY_UPLOADED_MARKER] if marker_present else []
    priority = encode_analysis_candidate_priority(
        candidate_kind,
        tag_values_utf8=marker_values,
        title_scalar_receipt=title_scalar_receipt,
        download_time=observed_download,
        gid=gid,
    )
    receipt = validate_analysis_candidate_priority(
        priority,
        candidate_kind,
        tag_values_utf8=marker_values,
        title_scalar_receipt=title_scalar_receipt,
        download_time=observed_download,
        gid=gid,
    )
    if (
        receipt.candidate_kind is not candidate_kind
        or receipt.prefer_not_already_uploaded is marker_present
        or receipt.title_scalar_length != title_scalar_receipt.scalar_count
        or receipt.download_time != observed_download
        or receipt.gid != gid
    ):
        raise AnalysisCorruptionError("streamed comparator fixed receipt is incoherent")
    return priority


def _prepare_snapshot_manifest(
    work: VNextUnitOfWork,
    run: _RunAuthority,
    preparation_authority: AnalysisPreparationAuthority,
) -> AnalysisSnapshotPreparation:
    manifest = work.connector.fetch_one(
        "SELECT gallery_count, file_count, byte_count "
        "FROM catalog_build_manifests WHERE build_id = %s",
        (run.build_id,),
    )
    if len(manifest) != 3:
        raise AnalysisNotReadyError("snapshot preparation lost the build manifest")
    counts = SourceSnapshotCounts(
        require_int63(manifest[0], field="snapshot gallery_count"),
        require_int63(manifest[1], field="snapshot file_count"),
        require_int63(manifest[2], field="snapshot byte_count"),
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
            "scan.source_file_count "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = member.gallery_id "
            "JOIN catalog_gallery_observations AS observation "
            "ON observation.gallery_id = member.gallery_id "
            "AND observation.observation_id = member.observation_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "JOIN catalog_gallery_observation_scans AS scan "
            "ON scan.gallery_id = member.gallery_id "
            "AND scan.observation_id = member.observation_id "
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
    last_file_no = 0
    file_count = 0
    byte_count = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT source.file_no, blob.size_bytes "
            "FROM catalog_gallery_observation_files AS source "
            "JOIN catalog_content_blobs AS blob "
            "ON blob.file_sha256 = source.file_sha256 "
            "WHERE source.gallery_id = %s AND source.observation_id = %s "
            "AND source.file_no > %s ORDER BY source.file_no LIMIT %s",
            (gallery_id, observation_id, last_file_no, _MAX_BATCH_ROWS),
        )
        if not rows:
            return file_count, byte_count
        for raw_file_no, raw_size in rows:
            last_file_no = require_positive_int63(
                raw_file_no,
                field="snapshot gallery file_no",
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
            "maximum_gallery_artist_count, evidence_sha256 "
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
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = winner.winner_gallery_id "
            "WHERE winner.analysis_id = %s AND winner.gid > %s "
            "ORDER BY winner.gid LIMIT %s",
            (authority.analysis_id, previous, _MAX_BATCH_ROWS),
        )
        if not rows:
            return
        for raw_gid, raw_gallery_id, raw_gallery_key in rows:
            gid = require_positive_int63(raw_gid, field="snapshot winner gid")
            gallery_id = require_positive_int63(
                raw_gallery_id,
                field="snapshot winner gallery_id",
            )
            gallery_key = require_digest32(
                raw_gallery_key,
                field="snapshot winner gallery_key",
            )
            member = work.connector.fetch_one(
                "SELECT candidate.gid "
                "FROM catalog_source_build_galleries AS source "
                "JOIN catalog_analysis_gid_candidate_resolved AS candidate "
                "ON candidate.analysis_id = %s "
                "AND candidate.gallery_id = source.gallery_id "
                "WHERE source.build_id = %s AND source.gallery_id = %s",
                (authority.analysis_id, authority.build_id, gallery_id),
            )
            if member != (gid,):
                raise AnalysisCorruptionError(
                    "snapshot GID winner is not a member of its exact GID group"
                )
            yield SourceSnapshotGidWinner(gid, gallery_key)
            previous = gid
        if len(rows) < _MAX_BATCH_ROWS:
            return


def _derive_baseline(work: VNextUnitOfWork, *, build_id: bytes) -> bytes | None:
    base = work.connector.fetch_one(
        "SELECT base_source_revision, base_source_generation "
        "FROM catalog_source_build_base_source WHERE build_id = %s",
        (build_id,),
    )
    if not base:
        return None
    if len(base) != 2:
        raise AnalysisCorruptionError("source build baseline has an invalid shape")
    revision = require_positive_int63(base[0], field="base source revision")
    generation = require_positive_int63(base[1], field="base source generation")
    channel = work.connector.fetch_one(
        "SELECT channel FROM catalog_source_build_channel WHERE build_id = %s",
        (build_id,),
    )
    if len(channel) != 1:
        raise AnalysisNotReadyError("source build channel is missing")
    head = work.connector.fetch_one(
        "SELECT source_revision, generation FROM catalog_source_heads "
        "WHERE channel = %s",
        (channel[0],),
    )
    if head != (revision, generation):
        raise AnalysisNotReadyError(
            "source build baseline is stale against its channel head"
        )
    provenance = work.connector.fetch_one(
        "SELECT analysis_id FROM catalog_source_revision_provenance "
        "WHERE source_revision = %s",
        (revision,),
    )
    if len(provenance) != 1:
        raise AnalysisNotReadyError("active source baseline has no analysis provenance")
    baseline = require_uuid16(provenance[0], field="baseline analysis_id")
    row = work.connector.fetch_one(
        "SELECT state FROM catalog_analysis_runs WHERE analysis_id = %s",
        (baseline,),
    )
    if row != ("COMPLETE",):
        raise AnalysisNotReadyError("baseline analysis is not COMPLETE")
    _require_exact_component_seals(work, baseline)
    return baseline


def _derive_layout(
    work: VNextUnitOfWork,
    *,
    baseline: bytes | None,
    policy: _Policy,
) -> tuple[bytes | None, int, tuple[bytes | None, ...]]:
    if baseline is None:
        return None, 0, (None,)
    row = work.connector.fetch_one(
        "SELECT run.policy_id, anchor.anchor_analysis_id, anchor.overlay_depth "
        "FROM catalog_analysis_runs AS run "
        "JOIN catalog_analysis_state_anchors AS anchor "
        "ON anchor.analysis_id = run.analysis_id WHERE run.analysis_id = %s",
        (baseline,),
    )
    if len(row) != 3:
        raise AnalysisCorruptionError("baseline analysis anchor is missing")
    parent_policy = require_positive_int63(row[0], field="baseline policy_id")
    parent_anchor = require_uuid16(row[1], field="baseline anchor_analysis_id")
    parent_depth = require_int63(row[2], field="baseline overlay_depth")
    if parent_depth > _MAX_OVERLAY_DEPTH:
        raise AnalysisCorruptionError("baseline overlay depth exceeds 16")
    ancestry_rows = work.connector.fetch_all(
        "SELECT ancestor_depth, ancestor_analysis_id "
        "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
        "ORDER BY ancestor_depth LIMIT 18",
        (baseline,),
    )
    parent_ancestry = tuple(
        require_uuid16(item[1], field="baseline ancestor_analysis_id")
        for item in ancestry_rows
    )
    if (
        len(parent_ancestry) != parent_depth + 1
        or not parent_ancestry
        or parent_ancestry[0] != baseline
        or parent_ancestry[-1] != parent_anchor
        or len(set(parent_ancestry)) != len(parent_ancestry)
    ):
        raise AnalysisCorruptionError(
            "baseline ancestry is not an exact acyclic suffix"
        )
    for expected_depth, row_value in enumerate(ancestry_rows):
        if (
            require_int63(row_value[0], field="baseline ancestor_depth")
            != expected_depth
        ):
            raise AnalysisCorruptionError("baseline ancestry depths are not contiguous")
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
    anchor = work.connector.fetch_one(
        "SELECT anchor_analysis_id, overlay_depth "
        "FROM catalog_analysis_state_anchors WHERE analysis_id = %s",
        (analysis_id,),
    )
    if len(anchor) != 2:
        raise AnalysisCorruptionError("persisted analysis anchor is missing")
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
        require_uuid16(anchor[0], field="persisted anchor_analysis_id"),
        require_int63(anchor[1], field="persisted overlay_depth"),
        ancestry,
    )


def _require_exact_component_seals(work: VNextUnitOfWork, analysis_id: bytes) -> None:
    rows = work.connector.fetch_all(
        "SELECT state_component FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (analysis_id,),
    )
    actual = {
        require_bounded_bytes(row[0], field="baseline component", minimum=1, maximum=64)
        for row in rows
    }
    if len(rows) != 5 or actual != ANALYSIS_COMPONENTS:
        raise AnalysisNotReadyError(
            "baseline analysis is not sealed in all five components"
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
        row = work.connector.fetch_one(
            "SELECT run.policy_id, run.state, anchor.anchor_analysis_id, "
            "anchor.overlay_depth FROM catalog_analysis_runs AS run "
            "JOIN catalog_analysis_state_anchors AS anchor "
            "ON anchor.analysis_id = run.analysis_id WHERE run.analysis_id = %s",
            (ancestor,),
        )
        if len(row) != 4:
            raise AnalysisCorruptionError("inherited analysis anchor is missing")
        if (
            require_positive_int63(row[0], field="ancestor policy_id") != policy_id
            or row[1] != "COMPLETE"
            or require_uuid16(row[2], field="ancestor anchor_analysis_id")
            != anchor_analysis_id
            or require_int63(row[3], field="ancestor overlay_depth") != len(suffix) - 1
        ):
            raise AnalysisCorruptionError(
                "inherited analysis does not match the complete policy suffix"
            )
        suffix_rows = work.connector.fetch_all(
            "SELECT ancestor_depth, ancestor_analysis_id "
            "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
            "ORDER BY ancestor_depth LIMIT 18",
            (ancestor,),
        )
        materialized: list[bytes] = []
        for expected_depth, suffix_row in enumerate(suffix_rows):
            if (
                require_int63(suffix_row[0], field="ancestor suffix depth")
                != expected_depth
            ):
                raise AnalysisCorruptionError(
                    "inherited analysis suffix depths are not contiguous"
                )
            materialized.append(
                require_uuid16(
                    suffix_row[1],
                    field="ancestor suffix analysis_id",
                )
            )
        if tuple(materialized) != suffix:
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
    timestamp = require_int63(now, field="analysis batch now")
    key = require_bounded_bytes(
        batch_key,
        field="analysis batch_key",
        minimum=1,
        maximum=512,
    )
    limit = require_positive_int63(max_rows, field="analysis max_rows")
    if limit > _MAX_BATCH_ROWS:
        raise ValueError(f"analysis max_rows must not exceed {_MAX_BATCH_ROWS}")
    authority = _authorize_analysis(
        work,
        gate_lease=gate_lease,
        ingest_turn=ingest_turn,
        analysis_id=analysis_id,
        now=timestamp,
    )
    checkpoint = _lock_checkpoint(work, authority.analysis_id, stage)
    receipt = work.connector.fetch_one(
        "SELECT start_generation, start_cursor, start_processed_count, "
        "next_cursor, next_processed_count, next_state, row_count, terminal, "
        "committed_generation, committed_at "
        "FROM catalog_analysis_batch_receipts "
        "WHERE analysis_id = %s AND stage = %s AND batch_key = %s",
        (authority.analysis_id, stage, key),
    )
    if receipt:
        stored = _batch_result_from_receipt(
            authority.analysis_id,
            stage,
            key,
            receipt,
            replayed=True,
            component_sealed=_component_is_sealed(
                work,
                authority.analysis_id,
                stage,
            ),
        )
        return (
            authority,
            None,
            stored,
        )
    if checkpoint.state == _CHECKPOINT_COMPLETE:
        raise AnalysisNotReadyError(
            "analysis stage is complete under a different terminal batch_key"
        )
    if checkpoint.state != _CHECKPOINT_OPEN:
        raise AnalysisCorruptionError("analysis checkpoint has an unknown state")
    if timestamp < checkpoint.updated_at:
        raise AnalysisNotReadyError(
            "analysis batch timestamp precedes its durable checkpoint"
        )
    return authority, checkpoint, None


def _lock_checkpoint(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    stage: bytes,
) -> _Checkpoint:
    kind, live = _require_registered_stage(work, stage)
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("analysis-checkpoint", analysis_id, stage),
        "SELECT generation, cursor, processed_count, state, updated_at "
        "FROM catalog_analysis_checkpoints WHERE analysis_id = %s AND stage = %s",
        (analysis_id, stage),
    )
    if len(row) != 5 or not isinstance(row[3], str):
        raise AnalysisCorruptionError("analysis checkpoint is missing or malformed")
    checkpoint = _Checkpoint(
        require_positive_int63(row[0], field="analysis checkpoint generation"),
        require_bounded_bytes(row[1], field="analysis checkpoint cursor", maximum=2048),
        require_int63(row[2], field="analysis checkpoint processed_count"),
        row[3],
        require_int63(row[4], field="analysis checkpoint updated_at"),
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
    timestamp = require_int63(now, field="analysis batch committed_at")
    key = require_bounded_bytes(
        batch_key,
        field="analysis batch_key",
        minimum=1,
        maximum=512,
    )
    work.connector.execute(
        "INSERT INTO catalog_analysis_batch_receipts "
        "(analysis_id, stage, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            authority.analysis_id,
            stage,
            key,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            cursor,
            next_processed_count,
            next_state,
            rows,
            int(terminal),
            next_generation,
            timestamp,
        ),
    )
    work.compare_and_swap(
        "UPDATE catalog_analysis_checkpoints SET generation = %s, cursor = %s, "
        "processed_count = %s, state = %s, updated_at = %s "
        "WHERE analysis_id = %s AND stage = %s AND generation = %s "
        "AND cursor = %s AND processed_count = %s AND state = %s "
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
        authority=f"analysis checkpoint {stage!r}",
    )
    return AnalysisBatchResult(
        authority.analysis_id,
        stage,
        key,
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
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
    if len(row) != 10:
        raise AnalysisCorruptionError("analysis batch receipt is malformed")
    terminal_value = require_int63(row[7], field="analysis receipt terminal")
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
            require_bounded_bytes(
                row[3],
                field="analysis receipt next_cursor",
                maximum=2048,
            ),
            require_int63(
                row[4],
                field="analysis receipt next_processed_count",
            ),
            _require_checkpoint_state(row[5]),
            require_int63(row[6], field="analysis receipt row_count"),
            bool(terminal_value),
            require_positive_int63(
                row[8],
                field="analysis receipt committed_generation",
            ),
            require_int63(row[9], field="analysis receipt committed_at"),
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
    row = work.connector.fetch_one(
        "SELECT row_count FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s AND state_component = %s",
        (analysis_id, component),
    )
    if len(row) != 1:
        raise AnalysisNotReadyError(
            f"analysis component {component!r} is not independently sealed"
        )
    require_int63(row[0], field=f"component {component!r} row_count")


def _require_unsealed_component(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    component: bytes,
) -> None:
    if work.connector.fetch_one(
        "SELECT 1 FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s AND state_component = %s",
        (analysis_id, component),
    ):
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
    work.connector.execute(
        "INSERT INTO catalog_analysis_state_component_seals "
        "(analysis_id, state_component, row_count, sealed_at) "
        "VALUES (%s, %s, %s, %s)",
        (authority.analysis_id, component, live_count, now),
    )
    return AnalysisBatchResult(
        result.analysis_id,
        result.stage,
        result.batch_key,
        result.start_generation,
        result.start_cursor,
        result.start_processed_count,
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
            f"SELECT 1 FROM {table} " f"WHERE analysis_id = %s AND {key_column} = %s",
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


def _require_transition_preparations(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
) -> tuple[AnalysisGalleryPreparation | None, ...]:
    """Require one live plan, or exact ``None``, for each current/removed key."""

    return _require_validation_preparations(
        work,
        authority,
        selected,
        preparations,
    )


def _require_validation_preparations(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    selected: Sequence[tuple[Any, ...]],
    preparations: Sequence[AnalysisGalleryPreparation | None],
) -> tuple[AnalysisGalleryPreparation | None, ...]:
    exact = tuple(preparations)
    if len(exact) != len(selected):
        raise AnalysisNotReadyError(
            "validation preparations do not cover the exact server keyset"
        )
    for row, preparation in zip(selected, exact, strict=True):
        gallery_id = require_positive_int63(
            row[0],
            field="validation selected gallery_id",
        )
        current = work.connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = %s",
            (authority.build_id, gallery_id),
        )
        if not current:
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
            or preparation.observation_id
            != require_positive_int63(current[0], field="current observation_id")
        ):
            raise AnalysisNotReadyError(
                "validation preparation differs from current membership"
            )
        _validate_preparation_authority(
            work=work,
            run=authority,
            preparation=preparation,
        )
    return exact


def _component_seal_receipts(
    work: VNextUnitOfWork,
    analysis_id: bytes,
) -> tuple[tuple[bytes, int, int], ...]:
    rows = work.connector.fetch_all(
        "SELECT state_component, row_count, sealed_at "
        "FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (analysis_id,),
    )
    return tuple(
        (
            require_bounded_bytes(
                row[0],
                field="component seal state_component",
                minimum=1,
                maximum=64,
            ),
            require_int63(row[1], field="component seal row_count"),
            require_int63(row[2], field="component seal sealed_at"),
        )
        for row in rows
    )


def _load_preparation_authority(
    work: VNextUnitOfWork,
    receipt: AnalysisPreparationAuthority,
    *,
    allow_complete: bool = False,
) -> _RunAuthority:
    if receipt._capability is not _PREPARATION_TOKEN:
        raise TypeError("preparation authority is not repository-issued")
    row = work.connector.fetch_one(
        "SELECT build_id, policy_id, input_manifest_sha256, state "
        "FROM catalog_analysis_runs WHERE analysis_id = %s",
        (receipt.analysis_id,),
    )
    expected_prefix = (
        receipt.build_id,
        receipt.policy_id,
        receipt.input_manifest_sha256,
    )
    allowed_states = {"OPEN", "COMPLETE"} if allow_complete else {"OPEN"}
    if len(row) != 4 or row[:3] != expected_prefix or row[3] not in allowed_states:
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
    if work.connector.fetch_one(
        "SELECT state FROM catalog_source_builds WHERE build_id = %s",
        (receipt.build_id,),
    ) != ("SEALED",):
        raise AnalysisNotReadyError("preparation build is no longer SEALED")
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
    anchor = work.connector.fetch_one(
        "SELECT overlay_depth FROM catalog_analysis_state_anchors "
        "WHERE analysis_id = %s",
        (receipt.analysis_id,),
    )
    if len(anchor) != 1:
        raise AnalysisCorruptionError("preparation analysis anchor is missing")
    depth = require_int63(anchor[0], field="preparation overlay_depth")
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
    input_row = work.connector.fetch_one(
        "SELECT input_manifest_sha256 FROM catalog_analysis_runs "
        "WHERE analysis_id = %s",
        (run.analysis_id,),
    )
    if input_row != (receipt.input_manifest_sha256,):
        raise AnalysisCorruptionError("preparation input manifest changed")
    if work.connector.fetch_one(
        "SELECT state FROM catalog_source_builds WHERE build_id = %s",
        (run.build_id,),
    ) != ("SEALED",):
        raise AnalysisNotReadyError("preparation build is no longer SEALED")
    if _component_seal_receipts(work, run.analysis_id) != receipt.component_seals:
        raise AnalysisNotReadyError("preparation component seal receipt changed")


def _insert_impacted_content(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    content_sha256: bytes,
) -> None:
    content = require_digest32(content_sha256, field="impacted content_sha256")
    existing = work.connector.fetch_one(
        "SELECT analysis_id, content_sha256 FROM catalog_analysis_impacted_content "
        "WHERE analysis_id = %s AND content_sha256 = %s",
        (analysis_id, content),
    )
    if existing:
        if existing != (analysis_id, content):
            raise AnalysisCorruptionError("impacted content natural key changed")
        return
    work.connector.execute(
        "INSERT INTO catalog_analysis_impacted_content "
        "(analysis_id, content_sha256) VALUES (%s, %s)",
        (analysis_id, content),
    )


def _insert_impacted_gid(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    gid: int,
) -> None:
    exact_gid = require_positive_int63(gid, field="impacted gid")
    existing = work.connector.fetch_one(
        "SELECT analysis_id, gid FROM catalog_analysis_impacted_gid "
        "WHERE analysis_id = %s AND gid = %s",
        (analysis_id, exact_gid),
    )
    if existing:
        if existing != (analysis_id, exact_gid):
            raise AnalysisCorruptionError("impacted GID natural key changed")
        return
    work.connector.execute(
        "INSERT INTO catalog_analysis_impacted_gid "
        "(analysis_id, gid) VALUES (%s, %s)",
        (analysis_id, exact_gid),
    )


def _consume_effective_content_claim(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    preparation: AnalysisGalleryPreparation,
) -> None:
    plan = preparation.content_upload_plan
    content = preparation.content_sha256
    if plan is None or content is None:
        raise AnalysisCorruptionError("content claim handoff has no canonical plan")
    if plan.digest_domain != _EFFECTIVE_CONTENT_DOMAIN or plan.value_sha256 != content:
        raise AnalysisCorruptionError("content plan differs from preparation")
    membership = work.connector.fetch_one(
        "SELECT observation_id FROM catalog_source_build_galleries "
        "WHERE build_id = %s AND gallery_id = %s",
        (authority.build_id, preparation.gallery_id),
    )
    if membership != (preparation.observation_id,):
        raise AnalysisNotReadyError("prepared gallery membership changed")
    allocation = work.connector.fetch_one(
        "SELECT a.digest_domain, a.byte_count, i.root_page_sha256 "
        "FROM catalog_canonical_value_allocations AS a "
        "JOIN catalog_canonical_value_identities AS i "
        "ON i.value_sha256 = a.value_sha256 WHERE a.value_sha256 = %s",
        (content,),
    )
    if len(allocation) != 3:
        raise AnalysisNotReadyError(
            "effective-content canonical identity is not sealed"
        )
    if (
        allocation[0] != _EFFECTIVE_CONTENT_DOMAIN
        or require_int63(allocation[1], field="effective-content byte_count")
        != plan.byte_count
        or require_digest32(allocation[2], field="effective-content root")
        != plan.root_page_sha256
    ):
        raise AnalysisCorruptionError(
            "effective-content canonical identity differs from preparation"
        )
    generation = _generation_for_build(work, authority.build_id)
    claim = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("analysis-content-upload", generation, content),
        "SELECT generation, value_sha256 "
        "FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, content),
    )
    if not claim:
        if work.connector.fetch_one(
            "SELECT 1 FROM catalog_analysis_impacted_content "
            "WHERE analysis_id = %s AND content_sha256 = %s",
            (authority.analysis_id, content),
        ):
            return
        raise AnalysisNotReadyError(
            "effective-content handoff requires its live-generation upload claim"
        )
    if claim != (generation, content):
        raise AnalysisCorruptionError("effective-content upload claim changed")
    deleted = work.connector.execute_affected(
        "DELETE FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, content),
    )
    if deleted != 1:
        raise AnalysisCorruptionError(
            "effective-content upload claim changed during handoff"
        )


def _generation_for_build(work: VNextUnitOfWork, build_id: bytes) -> int:
    row = work.connector.fetch_one(
        "SELECT generation FROM operational_source_build_generations "
        "WHERE build_id = %s",
        (build_id,),
    )
    if len(row) != 1:
        raise AnalysisNotReadyError("analysis build has no unique live generation")
    return require_positive_int63(row[0], field="analysis build generation")


def _content_candidate_from_preparation(
    preparation: AnalysisGalleryPreparation,
) -> _ContentCandidate | None:
    if preparation.content_sha256 is None:
        return None
    if (
        preparation.content_priority_key is None
        or preparation.content_candidate_sha256 is None
    ):
        raise AnalysisCorruptionError("content preparation is internally incomplete")
    receipt = decode_analysis_candidate_priority(preparation.content_priority_key)
    if (
        receipt.candidate_kind is not AnalysisCandidateKind.CONTENT_OWNER
        or receipt.gid != preparation.gid
    ):
        raise AnalysisCorruptionError("content preparation priority kind changed")
    return _ContentCandidate(
        preparation.content_sha256,
        preparation.gallery_id,
        preparation.content_priority_key,
        preparation.content_candidate_sha256,
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
    if target is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_content_owner_candidates "
            "(analysis_id, content_sha256, gallery_id, priority_key, "
            "candidate_sha256) VALUES (%s, %s, %s, %s, %s)",
            (authority.analysis_id, *target.row),
        )
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
    work.connector.execute(
        "INSERT INTO catalog_analysis_content_owner_candidate_shadows "
        "(analysis_id, content_sha256, gallery_id, priority_key, "
        "candidate_sha256) VALUES (%s, %s, %s, %s, %s)",
        (analysis_id, *candidate.row),
    )


def _resolved_content_candidate(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    gallery_id: int,
) -> _ContentCandidate | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT content_sha256, gallery_id, priority_key, candidate_sha256 "
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
    row = work.connector.fetch_one(
        "SELECT content_sha256, gallery_id, priority_key, candidate_sha256 "
        "FROM catalog_analysis_content_owner_candidate_shadows "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis_id, gallery_id),
    )
    return _content_candidate_from_row(row, field="shadow content candidate")


def _content_candidate_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _ContentCandidate | None:
    if not row:
        return None
    if len(row) != 4:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    candidate = _ContentCandidate(
        require_digest32(row[0], field=f"{field} content_sha256"),
        require_positive_int63(row[1], field=f"{field} gallery_id"),
        require_bounded_bytes(
            row[2],
            field=f"{field} priority_key",
            minimum=1,
            maximum=128,
        ),
        require_digest32(row[3], field=f"{field} candidate_sha256"),
    )
    receipt = decode_analysis_candidate_priority(candidate.priority_key)
    if receipt.candidate_kind is not AnalysisCandidateKind.CONTENT_OWNER:
        raise AnalysisCorruptionError(f"{field} priority has the wrong kind")
    return candidate


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
        "SELECT keys.gallery_id FROM ("
        + " UNION ".join(subqueries)
        + ") AS keys WHERE keys.gallery_id > %s "
        "ORDER BY keys.gallery_id LIMIT %s",
        tuple(parameters),
    )


def _evaluate_content_owner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    content_sha256: bytes,
) -> _ContentOwner | None:
    content = require_digest32(content_sha256, field="owner content_sha256")
    last_gallery = 0
    winner: tuple[tuple[bytes, bytes, bytes], _ContentCandidate] | None = None
    while True:
        rows = work.connector.fetch_all(
            "SELECT candidate.content_sha256, candidate.gallery_id, "
            "candidate.priority_key, candidate.candidate_sha256, "
            "identity.scope_key, identity.locator_sha256 "
            "FROM catalog_analysis_content_owner_candidate_resolved AS candidate "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = candidate.gallery_id "
            "WHERE candidate.analysis_id = %s AND candidate.content_sha256 = %s "
            "AND candidate.gallery_id > %s ORDER BY candidate.gallery_id LIMIT %s",
            (authority.analysis_id, content, last_gallery, _MAX_BATCH_ROWS),
        )
        if not rows:
            break
        for row in rows:
            candidate = _content_candidate_from_row(
                row[:4],
                field="owner candidate",
            )
            if candidate is None:
                raise AnalysisCorruptionError("owner candidate row disappeared")
            scope = require_digest32(row[4], field="owner candidate scope_key")
            locator = require_digest32(row[5], field="owner candidate locator")
            order = analysis_candidate_total_order_key(
                candidate.priority_key,
                AnalysisCandidateKind.CONTENT_OWNER,
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
    decision = sha256(
        b"h2hdb-vnext-content-owner-decision\0"
        + authority.analysis_id
        + content
        + selected.gallery_id.to_bytes(8, "big")
        + selected.candidate_sha256
    ).digest()
    return _ContentOwner(content, selected.gallery_id, decision)


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
        work.connector.execute(
            "INSERT INTO catalog_analysis_content_owners "
            "(analysis_id, content_sha256, owner_gallery_id, decision_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (authority.analysis_id, *target.row),
        )
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
    work.connector.execute(
        "INSERT INTO catalog_analysis_content_owner_shadows "
        "(analysis_id, content_sha256, owner_gallery_id, decision_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (analysis_id, *owner.row),
    )


def _resolved_content_owner(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    content_sha256: bytes,
) -> _ContentOwner | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT content_sha256, owner_gallery_id, decision_sha256 "
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
    row = work.connector.fetch_one(
        "SELECT content_sha256, owner_gallery_id, decision_sha256 "
        "FROM catalog_analysis_content_owner_shadows "
        "WHERE analysis_id = %s AND content_sha256 = %s",
        (analysis_id, content_sha256),
    )
    return _content_owner_from_row(row, field="shadow content owner")


def _content_owner_from_row(
    row: tuple[Any, ...],
    *,
    field: str,
) -> _ContentOwner | None:
    if not row:
        return None
    if len(row) != 3:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _ContentOwner(
        require_digest32(row[0], field=f"{field} content_sha256"),
        require_positive_int63(row[1], field=f"{field} owner_gallery_id"),
        require_digest32(row[2], field=f"{field} decision_sha256"),
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
        predicate = " WHERE keys.content_sha256 > %s"
        parameters.append(require_digest32(after, field="owner validation cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT keys.content_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS keys"
        + predicate
        + " ORDER BY keys.content_sha256 LIMIT %s",
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
    receipt = decode_analysis_candidate_priority(preparation.gid_priority_key)
    if (
        receipt.candidate_kind is not AnalysisCandidateKind.GID_WINNER
        or receipt.gid is not None
    ):
        raise AnalysisCorruptionError("GID preparation priority kind changed")
    return _GidCandidate(
        gallery_id,
        gid,
        preparation.gid_priority_key,
        preparation.gid_candidate_sha256,
    )


def _materialize_gid_candidate(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gallery_id: int,
    target: _GidCandidate | None,
    parent: _GidCandidate | None,
) -> None:
    gallery = require_positive_int63(gallery_id, field="GID candidate gallery")
    if target is not None:
        if target.gallery_id != gallery:
            raise AnalysisCorruptionError("target GID candidate changed gallery key")
        work.connector.execute(
            "INSERT INTO catalog_analysis_gid_candidates "
            "(analysis_id, gallery_id, gid, priority_key, candidate_sha256) "
            "VALUES (%s, %s, %s, %s, %s)",
            (authority.analysis_id, *target.row),
        )
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
        "(analysis_id, gallery_id, gid, priority_key, candidate_sha256) "
        "VALUES (%s, %s, %s, %s, %s)",
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
        "SELECT gallery_id, gid, priority_key, candidate_sha256 "
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
        "SELECT gallery_id, gid, priority_key, candidate_sha256 "
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
    if len(row) != 4:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    candidate = _GidCandidate(
        require_positive_int63(row[0], field=f"{field} gallery_id"),
        require_positive_int63(row[1], field=f"{field} gid"),
        require_bounded_bytes(
            row[2],
            field=f"{field} priority_key",
            minimum=1,
            maximum=128,
        ),
        require_digest32(row[3], field=f"{field} candidate_sha256"),
    )
    receipt = decode_analysis_candidate_priority(candidate.priority_key)
    if receipt.candidate_kind is not AnalysisCandidateKind.GID_WINNER:
        raise AnalysisCorruptionError(f"{field} priority has the wrong kind")
    return candidate


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
        "SELECT keys.gallery_id FROM ("
        + " UNION ".join(subqueries)
        + ") AS keys WHERE keys.gallery_id > %s "
        "ORDER BY keys.gallery_id LIMIT %s",
        tuple(parameters),
    )


def _evaluate_gid_winner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gid: int,
) -> _GidWinner | None:
    exact_gid = require_positive_int63(gid, field="winner gid")
    last_gallery = 0
    winner: tuple[tuple[bytes, bytes, bytes], _GidCandidate] | None = None
    while True:
        rows = work.connector.fetch_all(
            "SELECT candidate.gallery_id, candidate.gid, candidate.priority_key, "
            "candidate.candidate_sha256, identity.scope_key, "
            "identity.locator_sha256 "
            "FROM catalog_analysis_gid_candidate_resolved AS candidate "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = candidate.gallery_id "
            "WHERE candidate.analysis_id = %s AND candidate.gid = %s "
            "AND candidate.gallery_id > %s ORDER BY candidate.gallery_id LIMIT %s",
            (authority.analysis_id, exact_gid, last_gallery, _MAX_BATCH_ROWS),
        )
        if not rows:
            break
        for row in rows:
            candidate = _gid_candidate_from_row(row[:4], field="winner candidate")
            if candidate is None:
                raise AnalysisCorruptionError("winner candidate row disappeared")
            scope = require_digest32(row[4], field="winner candidate scope_key")
            locator = require_digest32(row[5], field="winner candidate locator")
            order = analysis_candidate_total_order_key(
                candidate.priority_key,
                AnalysisCandidateKind.GID_WINNER,
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
    decision = sha256(
        b"h2hdb-vnext-gid-winner-decision\0"
        + authority.analysis_id
        + exact_gid.to_bytes(8, "big")
        + selected.gallery_id.to_bytes(8, "big")
        + selected.candidate_sha256
    ).digest()
    return _GidWinner(exact_gid, selected.gallery_id, decision)


def _materialize_gid_winner(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    gid: int,
    target: _GidWinner | None,
    parent: _GidWinner | None,
) -> None:
    exact_gid = require_positive_int63(gid, field="winner materialization gid")
    if target is not None:
        if target.gid != exact_gid:
            raise AnalysisCorruptionError("target GID winner changed group key")
        work.connector.execute(
            "INSERT INTO catalog_analysis_gid_winners "
            "(analysis_id, gid, winner_gallery_id, decision_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (authority.analysis_id, *target.row),
        )
    if authority.overlay_depth == 0:
        if target is not None:
            _insert_gid_winner_shadow(work, authority.analysis_id, target)
    elif target is None and parent is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_gid_winner_tombstones "
            "(analysis_id, gid) VALUES (%s, %s)",
            (authority.analysis_id, exact_gid),
        )
    elif target is not None and target != parent:
        _insert_gid_winner_shadow(work, authority.analysis_id, target)


def _insert_gid_winner_shadow(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    winner: _GidWinner,
) -> None:
    work.connector.execute(
        "INSERT INTO catalog_analysis_gid_winner_shadows "
        "(analysis_id, gid, winner_gallery_id, decision_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (analysis_id, *winner.row),
    )


def _resolved_gid_winner(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    gid: int,
) -> _GidWinner | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT gid, winner_gallery_id, decision_sha256 "
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
        "SELECT gid, winner_gallery_id, decision_sha256 "
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
    if len(row) != 3:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _GidWinner(
        require_positive_int63(row[0], field=f"{field} gid"),
        require_positive_int63(row[1], field=f"{field} winner_gallery_id"),
        require_digest32(row[2], field=f"{field} decision_sha256"),
    )


def _gid_winner_validation_keys(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: int | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT gid FROM catalog_analysis_gid_candidate_resolved "
        "WHERE analysis_id = %s",
        "SELECT gid FROM catalog_analysis_gid_winner_shadows " "WHERE analysis_id = %s",
        "SELECT gid FROM catalog_analysis_gid_winner_tombstones "
        "WHERE analysis_id = %s",
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
        "SELECT keys.gid FROM ("
        + " UNION ".join(subqueries)
        + ") AS keys WHERE keys.gid > %s ORDER BY keys.gid LIMIT %s",
        tuple(parameters),
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
            "FROM catalog_analysis_exclusion_deltas AS delta "
            "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
            "ON occurrence.file_sha256 = delta.file_sha256 "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.gallery_id = occurrence.gallery_id "
            "AND member.observation_id = occurrence.observation_id "
            "WHERE delta.analysis_id = %s AND delta.old_excluded <> delta.new_excluded "
            "AND member.build_id = %s"
        )
        parameters.extend((authority.analysis_id, build_id))
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
    if target is not None:
        work.connector.execute(
            "INSERT INTO catalog_analysis_file_hash_decision "
            "(analysis_id, file_sha256, occurrence_count, artist_count, "
            "maximum_gallery_artist_count, evidence_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (authority.analysis_id, file_sha256, *target.row),
        )
    parent_policy = (
        authority.policy
        if authority.baseline_analysis_id is None
        else _analysis_policy(work, authority.baseline_analysis_id)
    )
    old_excluded = _excluded(parent, parent_policy)
    new_excluded = _excluded(target, authority.policy)
    work.connector.execute(
        "INSERT INTO catalog_analysis_exclusion_deltas "
        "(analysis_id, file_sha256, old_excluded, new_excluded) "
        "VALUES (%s, %s, %s, %s)",
        (authority.analysis_id, file_sha256, old_excluded, new_excluded),
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
    work.connector.execute(
        "INSERT INTO catalog_analysis_file_hash_decision_shadow "
        "(analysis_id, file_sha256, occurrence_count, artist_count, "
        "maximum_gallery_artist_count, evidence_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (analysis_id, file_sha256, *decision.row),
    )


def _evaluate_file_decision(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    file_sha256: bytes,
) -> _Decision | None:
    digest = require_digest32(file_sha256, field="file_sha256")
    occurrence_row = work.connector.fetch_one(
        "SELECT SUM(occurrence.occurrence_count) "
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
    evidence = sha256(
        b"h2hdb-vnext-file-hash-evidence\0"
        + digest
        + occurrence_count.to_bytes(8, "big")
        + artist_count.to_bytes(8, "big")
        + maximum.to_bytes(8, "big")
    ).digest()
    return _Decision(occurrence_count, artist_count, maximum, evidence)


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
    row = work.connector.fetch_one(
        "SELECT policy_id FROM catalog_analysis_runs WHERE analysis_id = %s",
        (analysis_id,),
    )
    if len(row) != 1:
        raise AnalysisCorruptionError("analysis policy lookup lost its run")
    return _load_policy(
        work,
        require_positive_int63(row[0], field="baseline analysis policy_id"),
    )


def _resolved_decision(
    work: VNextUnitOfWork,
    analysis_id: bytes | None,
    file_sha256: bytes,
) -> _Decision | None:
    if analysis_id is None:
        return None
    row = work.connector.fetch_one(
        "SELECT occurrence_count, artist_count, maximum_gallery_artist_count, "
        "evidence_sha256 FROM catalog_analysis_file_hash_decision_resolved "
        "WHERE analysis_id = %s AND file_sha256 = %s",
        (analysis_id, file_sha256),
    )
    return _decision_from_row(row, field="resolved file decision")


def _shadow_decision(
    work: VNextUnitOfWork,
    analysis_id: bytes,
    file_sha256: bytes,
) -> _Decision | None:
    row = work.connector.fetch_one(
        "SELECT occurrence_count, artist_count, maximum_gallery_artist_count, "
        "evidence_sha256 FROM catalog_analysis_file_hash_decision_shadow "
        "WHERE analysis_id = %s AND file_sha256 = %s",
        (analysis_id, file_sha256),
    )
    return _decision_from_row(row, field="shadow file decision")


def _decision_from_row(row: tuple[Any, ...], *, field: str) -> _Decision | None:
    if not row:
        return None
    if len(row) != 4:
        raise AnalysisCorruptionError(f"{field} has an invalid shape")
    return _Decision(
        require_positive_int63(row[0], field=f"{field} occurrence_count"),
        require_int63(row[1], field=f"{field} artist_count"),
        require_int63(row[2], field=f"{field} maximum_gallery_artist_count"),
        require_digest32(row[3], field=f"{field} evidence_sha256"),
    )


def _validation_key_rows(
    work: VNextUnitOfWork,
    authority: _RunAuthority,
    *,
    after: bytes | None,
    limit: int,
) -> list[tuple[Any, ...]]:
    subqueries = [
        "SELECT occurrence.file_sha256 AS file_sha256 "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "WHERE member.build_id = %s",
        "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_shadow "
        "WHERE analysis_id = %s",
        "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_tombstone "
        "WHERE analysis_id = %s",
    ]
    parameters: list[Any] = [
        authority.build_id,
        authority.analysis_id,
        authority.analysis_id,
    ]
    if authority.baseline_analysis_id is not None:
        subqueries.append(
            "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_resolved "
            "WHERE analysis_id = %s"
        )
        parameters.append(authority.baseline_analysis_id)
    where = "" if after is None else " WHERE keys.file_sha256 > %s"
    if after is not None:
        parameters.append(require_digest32(after, field="validation cursor"))
    parameters.append(limit)
    return work.connector.fetch_all(
        "SELECT keys.file_sha256 FROM ("
        + " UNION ".join(subqueries)
        + ") AS keys"
        + where
        + " ORDER BY keys.file_sha256 LIMIT %s",
        tuple(parameters),
    )


def _baseline_build_id(work: VNextUnitOfWork, baseline_analysis_id: bytes) -> bytes:
    row = work.connector.fetch_one(
        "SELECT build_id FROM catalog_analysis_runs WHERE analysis_id = %s",
        (baseline_analysis_id,),
    )
    if len(row) != 1:
        raise AnalysisCorruptionError("baseline analysis build is missing")
    return require_uuid16(row[0], field="baseline build_id")


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
    return bool(
        work.connector.fetch_one(
            "SELECT 1 FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (analysis_id, component),
        )
    )


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
        else 32 if kind == _CURSOR_DIGEST else 0
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
        else 32 if kind == _CURSOR_DIGEST else 0
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
