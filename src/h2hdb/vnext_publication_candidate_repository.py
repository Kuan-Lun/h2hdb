"""Server-owned publication-candidate reservation for the vNext data plane.

This module starts the upstream half of joint publication.  The caller names a
completed analysis and registered policies, but never supplies a candidate
identity, revision, channel, manifest, child count, cursor, digest, or head
generation.  Those authorities are derived and locked inside the caller-owned
transaction.

The selection and catalog-projection stages use the generated closed stage
registry and server-owned cursors.  Catalog preparation is performed from a
repository-issued authority in an immutable read snapshot and spooled to disk;
write transactions consume at most 128 typed child rows.  Artifact byte,
locator, protection, and operational-effect preparation remain a separate
typed-adapter slice and are never replaced with caller-supplied digests.
"""

from __future__ import annotations

__all__ = [
    "PublicationCatalogProjectionPlan",
    "PublicationCandidate",
    "PublicationCandidateBatch",
    "PublicationCandidateConflictError",
    "PublicationCandidateHeadRaceError",
    "PublicationCandidateNotReadyError",
    "PublicationCandidateRepository",
    "PublicationCandidateRepositoryError",
    "PublicationCandidateStageRegistryUnavailableError",
    "PublicationProjectionAuthority",
]

import codecs
import secrets
import sqlite3
import unicodedata
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from hashlib import sha256
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Any, BinaryIO

from . import vnext_identity as identity
from .sql_connector import SQLConnector
from .vnext_allocator_repository import RevisionStream, VNextAllocatorRepository
from .vnext_analysis_repository import ANALYSIS_COMPONENTS
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
    require_uuid16,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_CANDIDATE_TABLE = "catalog_publication_candidates"
_BASE_SOURCE_TABLE = "catalog_publication_candidate_base_sources"
_BASE_CATALOG_TABLE = "catalog_publication_candidate_base_catalog"
_ANALYSIS_MANIFEST_TABLE = "catalog_analysis_snapshot_manifest"
_SOURCE_HEAD_TABLE = "catalog_source_heads"
_SOURCE_REVISION_TABLE = "catalog_source_revisions"
_PUBLICATION_HEAD_TABLE = "catalog_publication_heads"
_CATALOG_REVISION_TABLE = "catalog_revisions"
_PUBLICATION_STAGE_TABLE = "catalog_publication_stages"
_PUBLICATION_CHECKPOINT_TABLE = "catalog_publication_checkpoints"
_PUBLICATION_BATCH_RECEIPT_TABLE = "catalog_publication_batch_receipts"
_PUBLICATION_SELECTION_TABLE = "catalog_publication_selections"
_PUBLICATION_IDENTITY_TABLE = "catalog_publication_identities"
_DISPLAY_TITLE_TABLE = "catalog_display_title_choices"
_TITLE_SORT_TABLE = "catalog_title_sorts"
_PUBLICATION_TABLE = "catalog_publications"
_PUBLICATION_ORDER_TABLE = "catalog_publication_order"
_PUBLICATION_TITLE_TABLE = "catalog_publication_titles"
_PUBLICATION_CONTENT_TABLE = "catalog_publication_contents"
_CONTRIBUTOR_TABLE = "catalog_contributors"
_CONTRIBUTOR_SORT_AS_TABLE = "catalog_contributor_sort_as"
_SUBJECT_TABLE = "catalog_subjects"

_BUILD_GENERATION_TABLE = "operational_source_build_generations"
_SOURCE_WORKING_TABLE = "operational_source_working_builds"
_CATALOG_WORKING_TABLE = "operational_catalog_working_candidates"

_SOURCE_MANIFEST_DOMAIN = b"source_snapshot_manifest_v1"
_ARTIFACT_POLICY_DOMAIN = b"artifact_policy_v2"
_SUPPORTED_DISPLAY_TITLE_ALGORITHM = 1
_SUPPORTED_TITLE_SORT_ALGORITHM = 1

_CURSOR_GALLERY = b"publication_gallery_v1"
_CURSOR_PUBLICATION_KEY = b"publication_key_v1"
_CURSOR_CATALOG_CHILD = b"publication_catalog_child_v1"
_CHECKPOINT_OPEN = "OPEN"
_CHECKPOINT_COMPLETE = "COMPLETE"
_PROJECTION_AUTHORITY_TOKEN = object()
_PROJECTION_BUILD_PLAN_TOKEN = object()
_PROJECTION_VALIDATION_PLAN_TOKEN = object()
_CATALOG_BATCH_ROWS = 128
_METADATA_PREFIX = b"h2hdb-vnext-gallery-observation-metadata\0"
_CONTRIBUTOR_NAMESPACES = frozenset(
    {b"artist", b"author", b"cosplayer", b"group", b"illustrator", b"uploader"}
)

_CATALOG_CHILD_PUBLICATION = 0
_CATALOG_CHILD_ORDER = 1
_CATALOG_CHILD_TITLE = 2
_CATALOG_CHILD_CONTENT = 3
_CATALOG_CHILD_CONTRIBUTOR = 4
_CATALOG_CHILD_CONTRIBUTOR_SORT_AS = 5
_CATALOG_CHILD_SUBJECT = 6
_CATALOG_CHILD_ARTIFACT = 7


@dataclass(frozen=True, slots=True)
class _StageSpec:
    name: bytes
    order: bytes
    cursor_codec: bytes


_STAGES = (
    _StageSpec(b"BUILD_SELECTION", b"01", _CURSOR_GALLERY),
    _StageSpec(b"VALIDATE_SELECTION", b"02", _CURSOR_GALLERY),
    _StageSpec(b"BUILD_CATALOG_PROJECTION", b"03", _CURSOR_CATALOG_CHILD),
    _StageSpec(b"VALIDATE_CATALOG_PROJECTION", b"04", _CURSOR_CATALOG_CHILD),
    _StageSpec(b"BUILD_ARTIFACT_INPUT", b"05", _CURSOR_PUBLICATION_KEY),
    _StageSpec(
        b"BUILD_ARTIFACT_DELTA_OPERATION",
        b"06",
        _CURSOR_PUBLICATION_KEY,
    ),
    _StageSpec(
        b"VALIDATE_ARTIFACT_INPUT_DELTA",
        b"07",
        _CURSOR_PUBLICATION_KEY,
    ),
    _StageSpec(b"VALIDATE_PREPARED_ARTIFACT", b"08", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_CREATE", b"09", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_REBUILD", b"10", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_DELETE", b"11", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_UNCHANGED", b"12", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_NEW_GALLERY", b"13", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_CHANGED_GALLERY", b"14", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_REMOVED_GALLERY", b"15", _CURSOR_PUBLICATION_KEY),
    _StageSpec(b"VALIDATE_DUPLICATE_LOSER", b"16", _CURSOR_GALLERY),
    _StageSpec(b"FINALIZE_ARTIFACTS", b"17", _CURSOR_PUBLICATION_KEY),
)
_STAGE_BY_NAME = {stage.name: stage for stage in _STAGES}


class PublicationCandidateRepositoryError(RuntimeError):
    """Base class for publication-candidate protocol failures."""


class PublicationCandidateNotReadyError(PublicationCandidateRepositoryError):
    """A server-derived prerequisite is absent or not terminal."""


class PublicationCandidateConflictError(PublicationCandidateRepositoryError):
    """A durable candidate or normalized authority has conflicting facts."""


class PublicationCandidateHeadRaceError(PublicationCandidateNotReadyError):
    """A source or catalog head differs from the candidate's exact base."""


class PublicationCandidateStageRegistryUnavailableError(
    PublicationCandidateRepositoryError
):
    """The generated schema lacks the closed upstream publication stages."""


@dataclass(frozen=True, slots=True)
class PublicationCandidate:
    candidate_id: bytes
    analysis_id: bytes
    build_id: bytes
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    state: str
    created_at: int
    base_source_revision: int | None
    base_source_generation: int | None
    base_catalog_revision: int | None
    base_catalog_generation: int | None
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication candidate_id")
        require_uuid16(self.analysis_id, field="publication candidate analysis_id")
        require_uuid16(self.build_id, field="publication candidate build_id")
        require_positive_int63(
            self.reserved_revision,
            field="publication candidate reserved_revision",
        )
        require_bounded_bytes(
            self.channel,
            field="publication candidate channel",
            minimum=1,
            maximum=64,
        )
        require_positive_int63(
            self.artifact_policy_id,
            field="publication candidate artifact_policy_id",
        )
        require_positive_int63(
            self.display_title_policy_id,
            field="publication candidate display_title_policy_id",
        )
        if type(self.artifacts_required) is not bool:
            raise TypeError("publication candidate artifacts_required must be bool")
        if self.state not in {"OPEN", "SEALED", "PUBLISHED", "ABANDONED"}:
            raise ValueError("publication candidate state is not registered")
        require_int63(self.created_at, field="publication candidate created_at")
        _validate_optional_base_pair(
            self.base_source_revision,
            self.base_source_generation,
            label="source",
        )
        _validate_optional_base_pair(
            self.base_catalog_revision,
            self.base_catalog_generation,
            label="catalog",
        )
        if type(self.replayed) is not bool:
            raise TypeError("publication candidate replayed must be bool")


@dataclass(frozen=True, slots=True)
class PublicationCandidateBatch:
    """Complete immutable response for one fixed publication stage batch."""

    candidate_id: bytes
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

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication batch candidate_id")
        if self.stage not in _STAGE_BY_NAME:
            raise ValueError("publication batch stage is not registered")
        require_bounded_bytes(
            self.batch_key,
            field="publication batch_key",
            minimum=1,
            maximum=512,
        )
        require_positive_int63(
            self.start_generation,
            field="publication batch start_generation",
        )
        require_positive_int63(
            self.committed_generation,
            field="publication batch committed_generation",
        )
        for field, value in (
            ("start_processed_count", self.start_processed_count),
            ("next_processed_count", self.next_processed_count),
            ("row_count", self.row_count),
            ("committed_at", self.committed_at),
        ):
            require_int63(value, field=f"publication batch {field}")
        _validate_stage_cursor(
            _STAGE_BY_NAME[self.stage].cursor_codec,
            self.start_cursor,
        )
        _validate_stage_cursor(
            _STAGE_BY_NAME[self.stage].cursor_codec,
            self.next_cursor,
        )
        if self.next_state not in {_CHECKPOINT_OPEN, _CHECKPOINT_COMPLETE}:
            raise ValueError("publication batch next_state is not registered")
        if type(self.terminal) is not bool or type(self.replayed) is not bool:
            raise TypeError("publication batch terminal/replayed must be bool")


@dataclass(frozen=True, slots=True)
class PublicationProjectionAuthority:
    """Repository-issued immutable authority for out-of-transaction projection.

    The receipt pins the exact terminal VALIDATE_SELECTION response as well as
    every durable prerequisite that may otherwise change between the short
    issuing transaction and the independent read snapshot.
    """

    candidate_id: bytes
    analysis_id: bytes
    build_id: bytes
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    generation: int
    snapshot_manifest_sha256: bytes
    component_seals: tuple[tuple[bytes, int, int], ...]
    selection_checkpoint: tuple[int, bytes, int, str, int]
    selection_terminal_receipt: tuple[Any, ...]
    base_source: tuple[int, int] | None
    base_catalog: tuple[int, int] | None
    candidate_created_at: int
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PROJECTION_AUTHORITY_TOKEN:
            raise TypeError("projection authorities are repository-issued only")
        require_uuid16(self.candidate_id, field="projection authority candidate_id")
        require_uuid16(self.analysis_id, field="projection authority analysis_id")
        require_uuid16(self.build_id, field="projection authority build_id")
        require_positive_int63(
            self.reserved_revision,
            field="projection authority reserved_revision",
        )
        require_bounded_bytes(
            self.channel,
            field="projection authority channel",
            minimum=1,
            maximum=64,
        )
        require_positive_int63(
            self.artifact_policy_id,
            field="projection authority artifact_policy_id",
        )
        require_positive_int63(
            self.display_title_policy_id,
            field="projection authority display_title_policy_id",
        )
        if type(self.artifacts_required) is not bool:
            raise TypeError("projection authority artifacts_required must be bool")
        require_positive_int63(self.generation, field="projection authority generation")
        require_digest32(
            self.snapshot_manifest_sha256,
            field="projection authority snapshot_manifest_sha256",
        )
        require_int63(
            self.candidate_created_at,
            field="projection authority candidate_created_at",
        )
        _validate_projection_seal_receipts(self.component_seals)
        _validate_projection_selection_checkpoint(self.selection_checkpoint)
        _validate_projection_terminal_receipt(
            self.selection_terminal_receipt,
            checkpoint=self.selection_checkpoint,
        )
        _validate_projection_base(self.base_source, label="source")
        _validate_projection_base(self.base_catalog, label="catalog")

    @property
    def publication_count(self) -> int:
        return require_int63(
            self.selection_checkpoint[2],
            field="projection authority publication_count",
        )


@dataclass(frozen=True, slots=True)
class _ProjectionChild:
    cursor: bytes
    kind: int
    publication_key: bytes
    subkey: bytes

    def __post_init__(self) -> None:
        _validate_stage_cursor(_CURSOR_CATALOG_CHILD, self.cursor)
        if self.kind not in range(8):
            raise ValueError("projection child kind is not registered")
        require_digest32(self.publication_key, field="projection child publication_key")
        require_bounded_bytes(
            self.subkey,
            field="projection child subkey",
            maximum=130,
        )


class PublicationCatalogProjectionPlan:
    """Opaque disk-backed catalog plan produced from one immutable read snapshot.

    High-cardinality publications, children, sort keys, and canonical payloads
    stay in temporary files.  Mutation code can request only the next fixed
    128-row child page; it never scans or sorts the full candidate projection.
    """

    def __init__(
        self,
        *,
        authority: PublicationProjectionAuthority,
        database: sqlite3.Connection,
        payload: BinaryIO,
        temporary_directory: TemporaryDirectory[str],
        publication_count: int,
        child_count: int,
        validation: bool,
        _capability: object,
    ) -> None:
        expected_capability = (
            _PROJECTION_VALIDATION_PLAN_TOKEN
            if validation
            else _PROJECTION_BUILD_PLAN_TOKEN
        )
        if _capability is not expected_capability:
            raise TypeError("catalog projection plans are repository-issued only")
        if not isinstance(authority, PublicationProjectionAuthority):
            raise TypeError("catalog projection plan lacks its authority")
        authority.__post_init__()
        self.authority = authority
        self.publication_count = require_int63(
            publication_count,
            field="catalog projection publication_count",
        )
        self.child_count = require_int63(
            child_count,
            field="catalog projection child_count",
        )
        self.validation = validation
        self._database = database
        self._payload = payload
        self._temporary_directory = temporary_directory
        self._capability = _capability
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._database.close()
        finally:
            try:
                self._payload.close()
            finally:
                self._temporary_directory.cleanup()

    def __enter__(self) -> PublicationCatalogProjectionPlan:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def iter_canonical_value_plans(self) -> Iterator[CanonicalValueUploadPlan]:
        """Yield replayable canonical uploads without materializing a payload."""

        self._require_open()
        cursor = self._database.execute(
            "SELECT digest_domain, payload_offset, byte_count "
            "FROM canonical_values ORDER BY value_sha256"
        )
        while True:
            rows = cursor.fetchmany(_CATALOG_BATCH_ROWS)
            if not rows:
                return
            for domain, offset, byte_count in rows:
                exact_domain = bytes(domain).decode("ascii", errors="strict")
                exact_offset = require_int63(offset, field="canonical payload offset")
                exact_count = require_int63(byte_count, field="canonical payload count")
                yield CanonicalValueUploadPlan.from_parts(
                    exact_domain,
                    _iter_file_range(self._payload, exact_offset, exact_count),
                )

    def _page_after(self, cursor: bytes) -> tuple[_ProjectionChild, ...]:
        self._require_open()
        _validate_stage_cursor(_CURSOR_CATALOG_CHILD, cursor)
        rows = self._database.execute(
            "SELECT cursor, kind, publication_key, subkey FROM children "
            "WHERE cursor > ? ORDER BY cursor LIMIT ?",
            (sqlite3.Binary(cursor), _CATALOG_BATCH_ROWS),
        ).fetchall()
        return tuple(
            _ProjectionChild(bytes(row[0]), int(row[1]), bytes(row[2]), bytes(row[3]))
            for row in rows
        )

    def _require_open(self) -> None:
        if self._closed:
            raise PublicationCandidateNotReadyError(
                "catalog projection plan is already closed"
            )


@dataclass(frozen=True, slots=True)
class _BeginAuthority:
    analysis_id: bytes
    build_id: bytes
    analysis_completed_at: int
    build_sealed_at: int
    channel: bytes
    snapshot_manifest_sha256: bytes
    artifact_policy_id: int
    producer_fingerprint_sha256: bytes
    display_title_policy_id: int


@dataclass(frozen=True, slots=True)
class _Head:
    revision: int
    generation: int
    advanced_at: int


class PublicationCandidateRepository:
    """Reserve or exactly resume the sole invisible publication candidate."""

    @staticmethod
    def begin(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        analysis_id: bytes,
        artifact_policy_id: int,
        display_title_policy_id: int,
        artifacts_required: bool,
        now: int,
    ) -> PublicationCandidate:
        """Create one OPEN candidate or replay the sole exact working root.

        The caller owns the surrounding write transaction.  A head race or any
        later failure therefore rolls the CATALOG allocator back together with
        the candidate, optional bases, and working-root insert.
        """

        analysis_key = require_uuid16(
            analysis_id,
            field="publication begin analysis_id",
        )
        artifact_policy = require_positive_int63(
            artifact_policy_id,
            field="publication begin artifact_policy_id",
        )
        display_policy = require_positive_int63(
            display_title_policy_id,
            field="publication begin display_title_policy_id",
        )
        if type(artifacts_required) is not bool:
            raise TypeError("publication begin artifacts_required must be bool")
        timestamp = require_int63(now, field="publication candidate created_at")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_exact_stage_registry(work)

        authority = _lock_begin_authority(
            work,
            analysis_id=analysis_key,
            artifact_policy_id=artifact_policy,
            display_title_policy_id=display_policy,
        )
        _require_exact_analysis_seals(work, authority)
        mapping = _lock_generation_mapping(work, generation)
        source_working = _lock_source_working(work)
        catalog_working = _lock_catalog_working(work)

        if mapping != authority.build_id:
            raise PublicationCandidateNotReadyError(
                "live ingest generation is not mapped to the analysis build"
            )
        if source_working is None or source_working[1] != authority.build_id:
            raise PublicationCandidateNotReadyError(
                "analysis build does not own the sole source working root"
            )
        if timestamp < max(
            authority.analysis_completed_at,
            authority.build_sealed_at,
            source_working[2],
        ):
            raise PublicationCandidateNotReadyError(
                "candidate timestamp precedes a sealed prerequisite"
            )

        if catalog_working is not None:
            candidate = _lock_candidate(work, catalog_working[1])
            _validate_resume(
                candidate,
                authority=authority,
                artifact_policy_id=artifact_policy,
                display_title_policy_id=display_policy,
                artifacts_required=artifacts_required,
                catalog_working=catalog_working,
                now=timestamp,
            )
            base_source = _load_base_source(work, candidate.candidate_id)
            base_catalog = _load_base_catalog(work, candidate.candidate_id)
            _require_build_base_source(work, authority.build_id, base_source)
            source_head = _lock_source_head(work, authority.channel)
            catalog_head = _lock_catalog_head(work, authority.channel)
            _require_exact_head("source", pinned=base_source, actual=source_head)
            _require_exact_head("catalog", pinned=base_catalog, actual=catalog_head)
            _require_head_timestamps(
                source_head,
                catalog_head,
                now=timestamp,
            )
            _require_exact_candidate_checkpoints(
                work,
                candidate.candidate_id,
                created_at=candidate.created_at,
                now=timestamp,
            )
            return _candidate_result(
                candidate,
                authority=authority,
                base_source=base_source,
                base_catalog=base_catalog,
                replayed=True,
            )

        candidate_id = require_uuid16(
            _new_candidate_id(),
            field="generated publication candidate_id",
        )
        if _lock_candidate_collision(work, candidate_id):
            raise PublicationCandidateConflictError(
                "generated publication candidate identity already exists"
            )

        reserved_revision = VNextAllocatorRepository.allocate_revision(
            work,
            RevisionStream.CATALOG,
            updated_at=timestamp,
        )
        source_head = _lock_source_head(work, authority.channel)
        catalog_head = _lock_catalog_head(work, authority.channel)
        build_base = _load_build_base_source(work, authority.build_id)
        _require_exact_head("source", pinned=build_base, actual=source_head)
        _require_head_timestamps(source_head, catalog_head, now=timestamp)

        connector = work.connector
        connector.execute(
            f"INSERT INTO {_CANDIDATE_TABLE} "
            "(candidate_id, analysis_id, reserved_revision, channel, "
            "artifact_policy_id, display_title_policy_id, artifacts_required, "
            "state, created_at, sealed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)",
            (
                candidate_id,
                authority.analysis_id,
                reserved_revision,
                authority.channel,
                artifact_policy,
                display_policy,
                int(artifacts_required),
                "OPEN",
                timestamp,
            ),
        )
        if build_base is not None:
            connector.execute(
                f"INSERT INTO {_BASE_SOURCE_TABLE} "
                "(candidate_id, base_source_revision, base_source_generation) "
                "VALUES (%s, %s, %s)",
                (
                    candidate_id,
                    build_base.revision,
                    build_base.generation,
                ),
            )
        if catalog_head is not None:
            connector.execute(
                f"INSERT INTO {_BASE_CATALOG_TABLE} "
                "(candidate_id, base_revision, base_catalog_generation) "
                "VALUES (%s, %s, %s)",
                (
                    candidate_id,
                    catalog_head.revision,
                    catalog_head.generation,
                ),
            )
        _initialize_candidate_checkpoints(
            work,
            candidate_id,
            now=timestamp,
        )
        connector.execute(
            f"INSERT INTO {_CATALOG_WORKING_TABLE} "
            "(slot, candidate_id, assigned_at) VALUES (%s, %s, %s)",
            (1, candidate_id, timestamp),
        )

        return PublicationCandidate(
            candidate_id,
            authority.analysis_id,
            authority.build_id,
            reserved_revision,
            authority.channel,
            artifact_policy,
            display_policy,
            artifacts_required,
            "OPEN",
            timestamp,
            None if build_base is None else build_base.revision,
            None if build_base is None else build_base.generation,
            None if catalog_head is None else catalog_head.revision,
            None if catalog_head is None else catalog_head.generation,
            False,
        )

    @staticmethod
    def process_selection_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Materialize the next server-derived selected-gallery keyset page."""

        authority, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"BUILD_SELECTION",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        rows = _derive_selection_page(
            work,
            authority,
            after=_decode_gallery_cursor(checkpoint.cursor),
        )
        for gallery_id, gid in rows:
            publication_key = identity.publication_key(gid)
            publication_id = identity.publication_id(gid)
            _ensure_publication_identity(
                work,
                publication_key=publication_key,
                publication_id=publication_id,
                gid=gid,
            )
            work.connector.execute(
                f"INSERT INTO {_PUBLICATION_SELECTION_TABLE} "
                "(candidate_id, gallery_id, publication_key) VALUES (%s, %s, %s)",
                (authority.candidate.candidate_id, gallery_id, publication_key),
            )
        next_cursor = checkpoint.cursor if not rows else rows[-1][0].to_bytes(8, "big")
        return _commit_candidate_batch(
            work,
            authority=authority,
            checkpoint=checkpoint,
            stage=b"BUILD_SELECTION",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(rows),
            terminal=not rows,
            now=now,
        )

    @staticmethod
    def validate_selection_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Independently merge-compare the exact selected-gallery projection."""

        authority, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"VALIDATE_SELECTION",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        after = _decode_gallery_cursor(checkpoint.cursor)
        expected = tuple(
            (gallery_id, identity.publication_key(gid))
            for gallery_id, gid in _evaluate_selection_page(
                work,
                authority,
                after=after,
            )
        )
        actual_rows = work.connector.fetch_all(
            f"SELECT gallery_id, publication_key FROM {_PUBLICATION_SELECTION_TABLE} "
            "WHERE candidate_id = %s AND gallery_id > %s "
            "ORDER BY gallery_id LIMIT 128",
            (authority.candidate.candidate_id, after),
        )
        actual = tuple(_selection_row(row) for row in actual_rows)
        if actual != expected:
            raise PublicationCandidateConflictError(
                "publication selection differs from its independent evaluator"
            )
        next_cursor = (
            checkpoint.cursor if not expected else expected[-1][0].to_bytes(8, "big")
        )
        return _commit_candidate_batch(
            work,
            authority=authority,
            checkpoint=checkpoint,
            stage=b"VALIDATE_SELECTION",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(expected),
            terminal=not expected,
            now=now,
        )

    @staticmethod
    def issue_projection_authority(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        now: int,
    ) -> PublicationProjectionAuthority:
        """Capture the exact terminal selection authority in one short tx."""

        candidate_key = require_uuid16(
            candidate_id,
            field="projection authority candidate_id",
        )
        timestamp = require_int63(now, field="projection authority now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_exact_stage_registry(work)
        hint = work.connector.fetch_one(
            f"SELECT analysis_id, artifact_policy_id, display_title_policy_id "
            f"FROM {_CANDIDATE_TABLE} WHERE candidate_id = %s",
            (candidate_key,),
        )
        if len(hint) != 3:
            raise PublicationCandidateNotReadyError("publication candidate is missing")
        begin = _lock_begin_authority(
            work,
            analysis_id=require_uuid16(
                hint[0], field="projection authority analysis_id"
            ),
            artifact_policy_id=require_positive_int63(
                hint[1], field="projection authority artifact_policy_id"
            ),
            display_title_policy_id=require_positive_int63(
                hint[2], field="projection authority display_title_policy_id"
            ),
        )
        component_seals = _analysis_seal_receipts(work, begin)
        mapping = _lock_generation_mapping(work, generation)
        source_working = _lock_source_working(work)
        catalog_working = _lock_catalog_working(work)
        candidate = _lock_candidate(work, candidate_key)
        _require_projection_candidate_exact(
            candidate,
            begin=begin,
            mapping=mapping,
            source_working=source_working,
            catalog_working=catalog_working,
            now=timestamp,
        )
        base_source = _load_base_source(work, candidate_key)
        base_catalog = _load_base_catalog(work, candidate_key)
        _require_build_base_source(work, begin.build_id, base_source)
        _require_exact_candidate_checkpoints(
            work,
            candidate_key,
            created_at=candidate.created_at,
            now=timestamp,
        )
        selection = _lock_publication_checkpoint(
            work,
            candidate_key,
            _STAGE_BY_NAME[b"VALIDATE_SELECTION"],
        )
        selection_tuple = (
            selection.generation,
            selection.cursor,
            selection.processed_count,
            selection.state,
            selection.updated_at,
        )
        terminal_receipt = _load_terminal_selection_receipt(
            work,
            candidate_key,
            selection,
        )
        source_head = _lock_source_head(work, begin.channel)
        catalog_head = _lock_catalog_head(work, begin.channel)
        _require_exact_head("source", pinned=base_source, actual=source_head)
        _require_exact_head("catalog", pinned=base_catalog, actual=catalog_head)
        _require_head_timestamps(source_head, catalog_head, now=timestamp)
        return PublicationProjectionAuthority(
            candidate.candidate_id,
            candidate.analysis_id,
            begin.build_id,
            candidate.reserved_revision,
            candidate.channel,
            candidate.artifact_policy_id,
            candidate.display_title_policy_id,
            candidate.artifacts_required,
            generation,
            begin.snapshot_manifest_sha256,
            component_seals,
            selection_tuple,
            terminal_receipt,
            _head_coordinate(base_source),
            _head_coordinate(base_catalog),
            candidate.created_at,
            _PROJECTION_AUTHORITY_TOKEN,
        )

    @staticmethod
    def prepare_catalog_projection(
        connector: SQLConnector,
        *,
        backend: str,
        authority: PublicationProjectionAuthority,
    ) -> PublicationCatalogProjectionPlan:
        """Derive one complete disk-backed catalog plan in a read snapshot."""

        if not isinstance(authority, PublicationProjectionAuthority):
            raise TypeError("authority must be PublicationProjectionAuthority")
        authority.__post_init__()
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _load_projection_authority(work, authority)
            return _prepare_catalog_plan(work, authority, validation=False)

    @staticmethod
    def prepare_catalog_projection_validation(
        connector: SQLConnector,
        *,
        backend: str,
        authority: PublicationProjectionAuthority,
    ) -> PublicationCatalogProjectionPlan:
        """Independently re-evaluate the projection from immutable DB roots."""

        if not isinstance(authority, PublicationProjectionAuthority):
            raise TypeError("authority must be PublicationProjectionAuthority")
        authority.__post_init__()
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _load_projection_authority(work, authority)
            return _prepare_catalog_plan(work, authority, validation=True)

    @staticmethod
    def process_catalog_projection_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        plan: PublicationCatalogProjectionPlan,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Persist at most 128 exact catalog child rows from the typed plan."""

        authority, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"BUILD_CATALOG_PROJECTION",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        _require_projection_plan(work, authority, plan, validation=False)
        rows = plan._page_after(checkpoint.cursor)
        _lock_projection_upload_claims(work, plan, rows)
        _ensure_reserved_catalog_revision(
            work,
            authority,
            publication_count=plan.publication_count,
            now=now,
        )
        for child in rows:
            _insert_projection_child(work, authority, plan, child)
        next_cursor = checkpoint.cursor if not rows else rows[-1].cursor
        return _commit_candidate_batch(
            work,
            authority=authority,
            checkpoint=checkpoint,
            stage=b"BUILD_CATALOG_PROJECTION",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(rows),
            terminal=not rows,
            now=now,
        )

    @staticmethod
    def validate_catalog_projection_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        validation: PublicationCatalogProjectionPlan,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Merge-compare DB children with an independently issued evaluator."""

        authority, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"VALIDATE_CATALOG_PROJECTION",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        _require_projection_plan(work, authority, validation, validation=True)
        _compare_reserved_catalog_revision(
            work,
            authority,
            publication_count=validation.publication_count,
            now=now,
        )
        expected = validation._page_after(checkpoint.cursor)
        actual = _load_catalog_child_page(
            work,
            authority,
            after=checkpoint.cursor,
        )
        if actual != expected:
            raise PublicationCandidateConflictError(
                "catalog projection differs from its independent DB evaluator"
            )
        for child in expected:
            _compare_projection_child(work, authority, validation, child)
        next_cursor = checkpoint.cursor if not expected else expected[-1].cursor
        return _commit_candidate_batch(
            work,
            authority=authority,
            checkpoint=checkpoint,
            stage=b"VALIDATE_CATALOG_PROJECTION",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(expected),
            terminal=not expected,
            now=now,
        )


@dataclass(frozen=True, slots=True)
class _CandidateRow:
    candidate_id: bytes
    analysis_id: bytes
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    state: str
    created_at: int


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    generation: int
    cursor: bytes
    processed_count: int
    state: str
    updated_at: int


@dataclass(frozen=True, slots=True)
class _MutationAuthority:
    candidate: _CandidateRow
    begin: _BeginAuthority
    base_source: _Head | None
    base_catalog: _Head | None


def _validate_projection_seal_receipts(
    seals: tuple[tuple[bytes, int, int], ...],
) -> None:
    if type(seals) is not tuple or len(seals) != len(ANALYSIS_COMPONENTS):
        raise ValueError("projection authority requires exactly five seals")
    previous: bytes | None = None
    seen: set[bytes] = set()
    for component, row_count, sealed_at in seals:
        exact = require_bounded_bytes(
            component,
            field="projection authority state_component",
            minimum=1,
            maximum=64,
        )
        if previous is not None and exact <= previous:
            raise ValueError("projection authority seals are not strictly ordered")
        require_int63(row_count, field="projection authority seal row_count")
        require_int63(sealed_at, field="projection authority seal sealed_at")
        previous = exact
        seen.add(exact)
    if seen != ANALYSIS_COMPONENTS:
        raise ValueError("projection authority seals differ from the closed set")


def _validate_projection_selection_checkpoint(
    checkpoint: tuple[int, bytes, int, str, int],
) -> None:
    if type(checkpoint) is not tuple or len(checkpoint) != 5:
        raise TypeError("projection selection checkpoint is malformed")
    require_positive_int63(checkpoint[0], field="selection checkpoint generation")
    _validate_stage_cursor(_CURSOR_GALLERY, checkpoint[1])
    require_int63(checkpoint[2], field="selection checkpoint processed_count")
    if checkpoint[3] != _CHECKPOINT_COMPLETE:
        raise ValueError("VALIDATE_SELECTION checkpoint is not COMPLETE")
    require_int63(checkpoint[4], field="selection checkpoint updated_at")


def _validate_projection_terminal_receipt(
    receipt: tuple[Any, ...],
    *,
    checkpoint: tuple[int, bytes, int, str, int],
) -> None:
    if type(receipt) is not tuple or len(receipt) != 11:
        raise TypeError("projection selection terminal receipt is malformed")
    batch_key = require_bounded_bytes(
        receipt[0],
        field="selection terminal batch_key",
        minimum=1,
        maximum=512,
    )
    del batch_key
    start_generation = require_positive_int63(
        receipt[1], field="selection terminal start_generation"
    )
    start_cursor = require_bounded_bytes(
        receipt[2], field="selection terminal start_cursor", maximum=2048
    )
    _validate_stage_cursor(_CURSOR_GALLERY, start_cursor)
    start_count = require_int63(
        receipt[3], field="selection terminal start_processed_count"
    )
    next_cursor = require_bounded_bytes(
        receipt[4], field="selection terminal next_cursor", maximum=2048
    )
    next_count = require_int63(
        receipt[5], field="selection terminal next_processed_count"
    )
    if receipt[6] != _CHECKPOINT_COMPLETE:
        raise ValueError("selection terminal receipt next_state is not COMPLETE")
    row_count = require_int63(receipt[7], field="selection terminal row_count")
    terminal = require_int63(receipt[8], field="selection terminal flag")
    committed_generation = require_positive_int63(
        receipt[9], field="selection terminal committed_generation"
    )
    committed_at = require_int63(receipt[10], field="selection terminal committed_at")
    if (
        row_count != 0
        or terminal != 1
        or next_cursor != start_cursor
        or next_count != start_count
        or committed_generation != start_generation + 1
        or (
            committed_generation,
            next_cursor,
            next_count,
            _CHECKPOINT_COMPLETE,
            committed_at,
        )
        != checkpoint
    ):
        raise ValueError("selection terminal receipt disagrees with its checkpoint")


def _validate_projection_base(
    value: tuple[int, int] | None,
    *,
    label: str,
) -> None:
    if value is None:
        return
    if type(value) is not tuple or len(value) != 2:
        raise TypeError(f"projection {label} base is malformed")
    require_positive_int63(value[0], field=f"projection {label} base revision")
    require_positive_int63(value[1], field=f"projection {label} base generation")


def _analysis_seal_receipts(
    work: VNextUnitOfWork,
    authority: _BeginAuthority,
) -> tuple[tuple[bytes, int, int], ...]:
    rows = work.connector.fetch_all(
        "SELECT state_component, row_count, sealed_at "
        "FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (authority.analysis_id,),
    )
    result = tuple(
        (
            require_bounded_bytes(
                row[0], field="analysis state_component", minimum=1, maximum=64
            ),
            require_int63(row[1], field="analysis component row_count"),
            require_int63(row[2], field="analysis component sealed_at"),
        )
        for row in rows
    )
    try:
        _validate_projection_seal_receipts(result)
    except (TypeError, ValueError) as error:
        raise PublicationCandidateConflictError(
            "analysis component seal receipts are not the exact closed set"
        ) from error
    if any(row[2] > authority.analysis_completed_at for row in result):
        raise PublicationCandidateConflictError(
            "analysis completed before one of its component seals"
        )
    return result


def _require_projection_candidate_exact(
    candidate: _CandidateRow,
    *,
    begin: _BeginAuthority,
    mapping: bytes | None,
    source_working: tuple[int, bytes, int] | None,
    catalog_working: tuple[int, bytes, int] | None,
    now: int,
) -> None:
    if (
        candidate.analysis_id != begin.analysis_id
        or candidate.channel != begin.channel
        or candidate.artifact_policy_id != begin.artifact_policy_id
        or candidate.display_title_policy_id != begin.display_title_policy_id
    ):
        raise PublicationCandidateConflictError(
            "publication candidate differs from its sealed analysis authority"
        )
    if candidate.state != "OPEN":
        raise PublicationCandidateNotReadyError(
            "catalog projection requires an OPEN candidate"
        )
    if mapping != begin.build_id:
        raise PublicationCandidateNotReadyError(
            "live generation is not mapped to the candidate build"
        )
    if source_working is None or source_working[1] != begin.build_id:
        raise PublicationCandidateNotReadyError(
            "candidate build no longer owns the source working root"
        )
    if catalog_working is None or catalog_working[1] != candidate.candidate_id:
        raise PublicationCandidateNotReadyError(
            "candidate no longer owns the catalog working root"
        )
    if now < max(
        candidate.created_at,
        source_working[2],
        catalog_working[2],
        begin.analysis_completed_at,
        begin.build_sealed_at,
    ):
        raise PublicationCandidateNotReadyError(
            "projection authority timestamp precedes a sealed prerequisite"
        )


def _load_terminal_selection_receipt(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    checkpoint: _Checkpoint,
) -> tuple[Any, ...]:
    rows = work.connector.fetch_all(
        f"SELECT batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at "
        f"FROM {_PUBLICATION_BATCH_RECEIPT_TABLE} "
        "WHERE candidate_id = %s AND stage = %s AND committed_generation = %s "
        "LIMIT 2",
        (candidate_id, b"VALIDATE_SELECTION", checkpoint.generation),
    )
    if len(rows) != 1:
        raise PublicationCandidateNotReadyError(
            "VALIDATE_SELECTION lacks one exact terminal receipt"
        )
    receipt = tuple(rows[0])
    checkpoint_tuple = (
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        checkpoint.state,
        checkpoint.updated_at,
    )
    try:
        _validate_projection_terminal_receipt(receipt, checkpoint=checkpoint_tuple)
    except (TypeError, ValueError) as error:
        raise PublicationCandidateConflictError(
            "VALIDATE_SELECTION terminal receipt is malformed"
        ) from error
    return receipt


def _load_projection_authority(
    work: VNextUnitOfWork,
    receipt: PublicationProjectionAuthority,
) -> _MutationAuthority:
    """Revalidate an issued receipt without taking locks in a read snapshot."""

    if receipt._capability is not _PROJECTION_AUTHORITY_TOKEN:
        raise TypeError("projection authority is not repository-issued")
    row = work.connector.fetch_one(
        f"SELECT analysis_id, reserved_revision, channel, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, state, created_at, sealed_at "
        f"FROM {_CANDIDATE_TABLE} WHERE candidate_id = %s",
        (receipt.candidate_id,),
    )
    candidate = _candidate_row_from_values(receipt.candidate_id, row)
    expected_candidate = (
        receipt.analysis_id,
        receipt.reserved_revision,
        receipt.channel,
        receipt.artifact_policy_id,
        receipt.display_title_policy_id,
        receipt.artifacts_required,
        "OPEN",
        receipt.candidate_created_at,
    )
    actual_candidate = (
        candidate.analysis_id,
        candidate.reserved_revision,
        candidate.channel,
        candidate.artifact_policy_id,
        candidate.display_title_policy_id,
        candidate.artifacts_required,
        candidate.state,
        candidate.created_at,
    )
    if actual_candidate != expected_candidate:
        raise PublicationCandidateNotReadyError(
            "projection authority differs from its immutable OPEN candidate"
        )
    begin = _lock_begin_authority(
        work,
        analysis_id=receipt.analysis_id,
        artifact_policy_id=receipt.artifact_policy_id,
        display_title_policy_id=receipt.display_title_policy_id,
        locking=False,
    )
    if (
        begin.build_id != receipt.build_id
        or begin.channel != receipt.channel
        or begin.snapshot_manifest_sha256 != receipt.snapshot_manifest_sha256
    ):
        raise PublicationCandidateNotReadyError(
            "projection authority differs from its sealed analysis output"
        )
    if _analysis_seal_receipts(work, begin) != receipt.component_seals:
        raise PublicationCandidateNotReadyError(
            "projection authority analysis seal set changed"
        )
    mapping = work.connector.fetch_one(
        f"SELECT build_id FROM {_BUILD_GENERATION_TABLE} WHERE generation = %s",
        (receipt.generation,),
    )
    if mapping != (receipt.build_id,):
        raise PublicationCandidateNotReadyError(
            "projection authority generation mapping changed"
        )
    source_working = work.connector.fetch_one(
        f"SELECT build_id FROM {_SOURCE_WORKING_TABLE} WHERE slot = %s",
        (1,),
    )
    catalog_working = work.connector.fetch_one(
        f"SELECT candidate_id FROM {_CATALOG_WORKING_TABLE} WHERE slot = %s",
        (1,),
    )
    if source_working != (receipt.build_id,) or catalog_working != (
        receipt.candidate_id,
    ):
        raise PublicationCandidateNotReadyError(
            "projection authority lost a sole working root"
        )
    base_source = _load_base_source(work, receipt.candidate_id)
    base_catalog = _load_base_catalog(work, receipt.candidate_id)
    if (
        _head_coordinate(base_source) != receipt.base_source
        or _head_coordinate(base_catalog) != receipt.base_catalog
    ):
        raise PublicationCandidateNotReadyError(
            "projection authority candidate base changed"
        )
    _require_build_base_source(work, receipt.build_id, base_source)
    source_head = _read_source_head(work, receipt.channel)
    catalog_head = _read_catalog_head(work, receipt.channel)
    _require_exact_head("source", pinned=base_source, actual=source_head)
    _require_exact_head("catalog", pinned=base_catalog, actual=catalog_head)
    checkpoint_row = work.connector.fetch_one(
        f"SELECT generation, cursor, processed_count, state, updated_at "
        f"FROM {_PUBLICATION_CHECKPOINT_TABLE} "
        "WHERE candidate_id = %s AND stage = %s",
        (receipt.candidate_id, b"VALIDATE_SELECTION"),
    )
    checkpoint = _checkpoint_from_row(checkpoint_row)
    checkpoint_tuple = (
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        checkpoint.state,
        checkpoint.updated_at,
    )
    if checkpoint_tuple != receipt.selection_checkpoint:
        raise PublicationCandidateNotReadyError(
            "projection authority selection checkpoint changed"
        )
    terminal = _load_terminal_selection_receipt(
        work,
        receipt.candidate_id,
        checkpoint,
    )
    if terminal != receipt.selection_terminal_receipt:
        raise PublicationCandidateNotReadyError(
            "projection authority terminal selection receipt changed"
        )
    return _MutationAuthority(candidate, begin, base_source, base_catalog)


def _candidate_row_from_values(
    candidate_id: bytes,
    row: tuple[Any, ...],
) -> _CandidateRow:
    if len(row) != 9:
        raise PublicationCandidateNotReadyError("publication candidate is missing")
    if row[5] not in {0, 1} or row[6] not in {
        "OPEN",
        "SEALED",
        "PUBLISHED",
        "ABANDONED",
    }:
        raise PublicationCandidateConflictError(
            "publication candidate has invalid state fields"
        )
    if row[8] is not None:
        require_int63(row[8], field="candidate sealed_at")
    return _CandidateRow(
        require_uuid16(candidate_id, field="publication candidate_id"),
        require_uuid16(row[0], field="publication candidate analysis_id"),
        require_positive_int63(row[1], field="publication candidate revision"),
        require_bounded_bytes(
            row[2], field="publication candidate channel", minimum=1, maximum=64
        ),
        require_positive_int63(row[3], field="publication candidate artifact policy"),
        require_positive_int63(row[4], field="publication candidate display policy"),
        bool(row[5]),
        row[6],
        require_int63(row[7], field="publication candidate created_at"),
    )


def _checkpoint_from_row(row: tuple[Any, ...]) -> _Checkpoint:
    if len(row) != 5:
        raise PublicationCandidateConflictError("publication checkpoint is malformed")
    checkpoint = _Checkpoint(
        require_positive_int63(row[0], field="publication checkpoint generation"),
        require_bounded_bytes(
            row[1], field="publication checkpoint cursor", maximum=2048
        ),
        require_int63(row[2], field="publication checkpoint processed_count"),
        row[3],
        require_int63(row[4], field="publication checkpoint updated_at"),
    )
    if checkpoint.state not in {_CHECKPOINT_OPEN, _CHECKPOINT_COMPLETE}:
        raise PublicationCandidateConflictError(
            "publication checkpoint state is not registered"
        )
    return checkpoint


def _read_source_head(work: VNextUnitOfWork, channel: bytes) -> _Head | None:
    row = work.connector.fetch_one(
        f"SELECT h.source_revision, h.generation, h.advanced_at, r.channel "
        f"FROM {_SOURCE_HEAD_TABLE} h JOIN {_SOURCE_REVISION_TABLE} r "
        "ON r.source_revision = h.source_revision WHERE h.channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 4 or row[3] != channel:
        raise PublicationCandidateConflictError("source head descriptor is malformed")
    return _Head(
        require_positive_int63(row[0], field="source head revision"),
        require_positive_int63(row[1], field="source head generation"),
        require_int63(row[2], field="source head advanced_at"),
    )


def _read_catalog_head(work: VNextUnitOfWork, channel: bytes) -> _Head | None:
    row = work.connector.fetch_one(
        f"SELECT h.revision, h.generation, h.advanced_at, r.revision "
        f"FROM {_PUBLICATION_HEAD_TABLE} h JOIN {_CATALOG_REVISION_TABLE} r "
        "ON r.revision = h.revision WHERE h.channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 4 or row[0] != row[3]:
        raise PublicationCandidateConflictError("catalog head descriptor is malformed")
    return _Head(
        require_positive_int63(row[0], field="catalog head revision"),
        require_positive_int63(row[1], field="catalog head generation"),
        require_int63(row[2], field="catalog head advanced_at"),
    )


def _prepare_catalog_plan(
    work: VNextUnitOfWork,
    authority: PublicationProjectionAuthority,
    *,
    validation: bool,
) -> PublicationCatalogProjectionPlan:
    temporary_directory = TemporaryDirectory(prefix="h2hdb-catalog-projection-")
    payload = TemporaryFile(mode="w+b")
    database = sqlite3.connect(
        f"{temporary_directory.name}/projection.sqlite3",
        isolation_level=None,
    )
    try:
        database.execute("PRAGMA temp_store = FILE")
        database.execute("PRAGMA journal_mode = OFF")
        _initialize_projection_plan_database(database)
        after = 0
        publication_count = 0
        while True:
            rows = _projection_source_page(
                work,
                authority,
                after=after,
                validation=validation,
            )
            if not rows:
                break
            for row in rows:
                _prepare_projection_publication(
                    work,
                    authority,
                    database,
                    payload,
                    row,
                )
                after = require_positive_int63(
                    row[0], field="projection source gallery_id"
                )
                publication_count += 1
                if publication_count > INT63_MAX:
                    raise PublicationCandidateNotReadyError(
                        "catalog projection publication count is exhausted"
                    )
        if publication_count != authority.publication_count:
            raise PublicationCandidateConflictError(
                "catalog projection row count differs from terminal selection"
            )
        _assign_projection_order(database)
        _finalize_projection_item_digests(database)
        child_count = _populate_projection_children(database)
        return PublicationCatalogProjectionPlan(
            authority=authority,
            database=database,
            payload=payload,
            temporary_directory=temporary_directory,
            publication_count=publication_count,
            child_count=child_count,
            validation=validation,
            _capability=(
                _PROJECTION_VALIDATION_PLAN_TOKEN
                if validation
                else _PROJECTION_BUILD_PLAN_TOKEN
            ),
        )
    except BaseException:
        database.close()
        payload.close()
        temporary_directory.cleanup()
        raise


def _initialize_projection_plan_database(database: sqlite3.Connection) -> None:
    database.executescript("""
        CREATE TABLE canonical_values (
            value_sha256 BLOB PRIMARY KEY,
            digest_domain BLOB NOT NULL,
            payload_offset INTEGER NOT NULL,
            byte_count INTEGER NOT NULL,
            consumer_cursor BLOB
        ) WITHOUT ROWID;
        CREATE TABLE publications (
            publication_key BLOB PRIMARY KEY,
            gallery_id INTEGER NOT NULL UNIQUE,
            summary_sha256 BLOB NOT NULL,
            language_sha256 BLOB NOT NULL,
            published_at INTEGER NOT NULL,
            modified_at INTEGER NOT NULL,
            source_title_sha256 BLOB NOT NULL,
            source_gallery_name BLOB NOT NULL,
            title_sha256 BLOB NOT NULL,
            sort_title_sha256 BLOB NOT NULL,
            sort_title BLOB NOT NULL,
            content_sha256 BLOB,
            order_position INTEGER,
            item_sha256 BLOB NOT NULL
        );
        CREATE TABLE contributors (
            publication_key BLOB NOT NULL,
            position INTEGER NOT NULL,
            contributor_name_sha256 BLOB NOT NULL,
            role BLOB NOT NULL,
            sort_as_sha256 BLOB,
            PRIMARY KEY (publication_key, position),
            UNIQUE (publication_key, contributor_name_sha256, role)
        ) WITHOUT ROWID;
        CREATE TABLE subjects (
            publication_key BLOB NOT NULL,
            position INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (publication_key, position),
            UNIQUE (publication_key, tag_id)
        ) WITHOUT ROWID;
        CREATE TABLE children (
            cursor BLOB PRIMARY KEY,
            kind INTEGER NOT NULL,
            publication_key BLOB NOT NULL,
            subkey BLOB NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX canonical_values_by_consumer
        ON canonical_values (consumer_cursor, value_sha256);
        """)


def _projection_source_page(
    work: VNextUnitOfWork,
    authority: PublicationProjectionAuthority,
    *,
    after: int,
    validation: bool,
) -> tuple[tuple[Any, ...], ...]:
    if validation:
        rows = work.connector.fetch_all(
            "SELECT member.gallery_id, selected.publication_key, "
            "member.observation_id, metadata.gid, metadata.upload_time, "
            "metadata.modified_time, locator.source_gallery_name "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "JOIN catalog_gallery_identities AS gallery "
            "ON gallery.gallery_id = member.gallery_id "
            "JOIN catalog_source_locator_identity AS locator "
            "ON locator.locator_sha256 = gallery.locator_sha256 "
            f"LEFT JOIN {_PUBLICATION_SELECTION_TABLE} AS selected "
            "ON selected.candidate_id = %s "
            "AND selected.gallery_id = member.gallery_id "
            "WHERE member.build_id = %s AND member.gallery_id > %s "
            "AND EXISTS (SELECT 1 FROM catalog_analysis_gid_winner_resolved w "
            "WHERE w.analysis_id = %s AND w.gid = metadata.gid "
            "AND w.winner_gallery_id = member.gallery_id) "
            "AND (NOT EXISTS (SELECT 1 "
            "FROM catalog_analysis_content_owner_candidate_resolved c "
            "WHERE c.analysis_id = %s AND c.gallery_id = member.gallery_id) "
            "OR EXISTS (SELECT 1 "
            "FROM catalog_analysis_content_owner_candidate_resolved c "
            "JOIN catalog_analysis_content_owner_resolved o "
            "ON o.analysis_id = c.analysis_id "
            "AND o.content_sha256 = c.content_sha256 "
            "WHERE c.analysis_id = %s AND c.gallery_id = member.gallery_id "
            "AND o.owner_gallery_id = member.gallery_id)) "
            "ORDER BY member.gallery_id LIMIT 128",
            (
                authority.candidate_id,
                authority.build_id,
                after,
                authority.analysis_id,
                authority.analysis_id,
                authority.analysis_id,
            ),
        )
    else:
        rows = work.connector.fetch_all(
            f"SELECT selected.gallery_id, selected.publication_key, "
            "member.observation_id, metadata.gid, metadata.upload_time, "
            "metadata.modified_time, locator.source_gallery_name "
            f"FROM {_PUBLICATION_SELECTION_TABLE} AS selected "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s AND member.gallery_id = selected.gallery_id "
            "JOIN catalog_gallery_observation_metadata AS metadata "
            "ON metadata.gallery_id = member.gallery_id "
            "AND metadata.observation_id = member.observation_id "
            "JOIN catalog_gallery_identities AS gallery "
            "ON gallery.gallery_id = member.gallery_id "
            "JOIN catalog_source_locator_identity AS locator "
            "ON locator.locator_sha256 = gallery.locator_sha256 "
            "WHERE selected.candidate_id = %s AND selected.gallery_id > %s "
            "ORDER BY selected.gallery_id LIMIT 128",
            (authority.build_id, authority.candidate_id, after),
        )
    result: list[tuple[Any, ...]] = []
    for row in rows:
        if len(row) != 7:
            raise PublicationCandidateConflictError(
                "catalog projection source row is malformed"
            )
        gallery_id = require_positive_int63(row[0], field="projection gallery_id")
        gid = require_positive_int63(row[3], field="projection gid")
        expected_key = identity.publication_key(gid)
        if (
            row[1] is None
            or require_digest32(row[1], field="projection publication_key")
            != expected_key
        ):
            raise PublicationCandidateConflictError(
                "catalog projection selection key differs from metadata GID"
            )
        result.append(
            (
                gallery_id,
                expected_key,
                require_positive_int63(row[2], field="projection observation_id"),
                gid,
                require_int63(row[4], field="projection upload_time"),
                require_int63(row[5], field="projection modified_time"),
                require_bounded_bytes(
                    row[6],
                    field="projection source_gallery_name",
                    minimum=1,
                    maximum=255,
                ),
            )
        )
    if any(left[0] >= right[0] for left, right in zip(result, result[1:])):
        raise PublicationCandidateConflictError(
            "catalog projection source is not strict gallery order"
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class _MetadataProjection:
    title_offset: int
    title_count: int
    comment_offset: int
    comment_count: int
    account_offset: int
    account_count: int
    gid: int
    upload_time: int
    modified_time: int


def _prepare_projection_publication(
    work: VNextUnitOfWork,
    authority: PublicationProjectionAuthority,
    database: sqlite3.Connection,
    payload: BinaryIO,
    row: tuple[Any, ...],
) -> None:
    gallery_id = require_positive_int63(row[0], field="projection gallery_id")
    publication_key = require_digest32(row[1], field="projection publication_key")
    observation_id = require_positive_int63(row[2], field="projection observation_id")
    gid = require_positive_int63(row[3], field="projection gid")
    upload_time = require_int63(row[4], field="projection upload_time")
    modified_time = require_int63(row[5], field="projection modified_time")
    source_gallery_name = require_bounded_bytes(
        row[6],
        field="projection source_gallery_name",
        minimum=1,
        maximum=255,
    )
    metadata_file, metadata = _spool_projection_metadata(
        work,
        gallery_id=gallery_id,
        observation_id=observation_id,
    )
    try:
        if (
            metadata.gid != gid
            or metadata.upload_time != upload_time
            or metadata.modified_time != modified_time
        ):
            raise PublicationCandidateConflictError(
                "normalized metadata scalars differ from the exact METADATA tree"
            )
        source_title = _register_projection_canonical_value(
            database,
            payload,
            domain="source_title_utf8_v1",
            parts=_iter_file_range(
                metadata_file,
                metadata.title_offset,
                metadata.title_count,
            ),
        )
        summary = _register_projection_canonical_value(
            database,
            payload,
            domain="catalog_summary_utf8_v1",
            parts=_iter_file_range(
                metadata_file,
                metadata.comment_offset,
                metadata.comment_count,
            ),
        )
        if metadata.title_count:
            display_parts: Iterable[bytes] = _iter_file_range(
                metadata_file,
                metadata.title_offset,
                metadata.title_count,
            )
            sort_parts: Iterable[bytes] = _iter_casefolded_file_range(
                metadata_file,
                metadata.title_offset,
                metadata.title_count,
            )
        else:
            display_parts = (source_gallery_name,)
            sort_parts = (
                source_gallery_name.decode("utf-8", errors="strict")
                .casefold()
                .encode("utf-8"),
            )
        display_title = _register_projection_canonical_value(
            database,
            payload,
            domain="display_title_utf8_v1",
            parts=display_parts,
        )
        sort_title = _register_projection_canonical_value(
            database,
            payload,
            domain="title_sort_utf8_v1",
            parts=sort_parts,
        )
        contributor_position = 0
        if metadata.account_count:
            account = _register_projection_canonical_value(
                database,
                payload,
                domain="contributor_name_utf8_v1",
                parts=_iter_file_range(
                    metadata_file,
                    metadata.account_offset,
                    metadata.account_count,
                ),
            )
            database.execute(
                "INSERT INTO contributors "
                "(publication_key, position, contributor_name_sha256, role, "
                "sort_as_sha256) VALUES (?, ?, ?, ?, NULL)",
                (
                    sqlite3.Binary(publication_key),
                    contributor_position,
                    sqlite3.Binary(account),
                    sqlite3.Binary(b"uploader"),
                ),
            )
            contributor_position += 1
    finally:
        metadata_file.close()

    language: bytes | None = None
    subject_position = 0
    tag_after = -1
    while True:
        tag_rows = work.connector.fetch_all(
            "SELECT observed.position, observed.tag_id, term.namespace, "
            "term.tag_value_sha256 "
            "FROM catalog_gallery_observation_tags AS observed "
            "JOIN catalog_tag_terms AS term ON term.tag_id = observed.tag_id "
            "WHERE observed.gallery_id = %s AND observed.observation_id = %s "
            "AND observed.position > %s ORDER BY observed.position LIMIT 128",
            (gallery_id, observation_id, tag_after),
        )
        if not tag_rows:
            break
        for tag_row in tag_rows:
            if len(tag_row) != 4:
                raise PublicationCandidateConflictError(
                    "projection tag row is malformed"
                )
            position = require_int63(tag_row[0], field="projection tag position")
            if position != subject_position:
                raise PublicationCandidateConflictError(
                    "projection tag positions are not exactly contiguous"
                )
            tag_id = require_positive_int63(tag_row[1], field="projection tag_id")
            namespace = require_bounded_bytes(
                tag_row[2],
                field="projection tag namespace",
                maximum=128,
            )
            tag_value = require_digest32(
                tag_row[3], field="projection tag value_sha256"
            )
            database.execute(
                "INSERT INTO subjects (publication_key, position, tag_id) "
                "VALUES (?, ?, ?)",
                (sqlite3.Binary(publication_key), subject_position, tag_id),
            )
            subject_position += 1
            if namespace == b"language" and language is None:
                source, source_count = _spool_existing_canonical_value(
                    work,
                    tag_value,
                    expected_domain=b"tag_value_utf8_v1",
                )
                try:
                    if source_count:
                        language = _register_projection_canonical_value(
                            database,
                            payload,
                            domain="catalog_language_utf8_v1",
                            parts=_iter_file_range(source, 0, source_count),
                        )
                finally:
                    source.close()
            if namespace in _CONTRIBUTOR_NAMESPACES:
                source, source_count = _spool_existing_canonical_value(
                    work,
                    tag_value,
                    expected_domain=b"tag_value_utf8_v1",
                )
                try:
                    if source_count:
                        contributor_name = _register_projection_canonical_value(
                            database,
                            payload,
                            domain="contributor_name_utf8_v1",
                            parts=_iter_file_range(source, 0, source_count),
                        )
                        inserted = database.execute(
                            "INSERT OR IGNORE INTO contributors "
                            "(publication_key, position, contributor_name_sha256, "
                            "role, sort_as_sha256) VALUES (?, ?, ?, ?, NULL)",
                            (
                                sqlite3.Binary(publication_key),
                                contributor_position,
                                sqlite3.Binary(contributor_name),
                                sqlite3.Binary(namespace),
                            ),
                        ).rowcount
                        if inserted:
                            contributor_position += 1
                finally:
                    source.close()
            tag_after = position
    if language is None:
        language = _register_projection_canonical_value(
            database,
            payload,
            domain="catalog_language_utf8_v1",
            parts=(b"und",),
        )
    content_rows = work.connector.fetch_all(
        "SELECT content_sha256 FROM catalog_analysis_content_owner_resolved "
        "WHERE analysis_id = %s AND owner_gallery_id = %s LIMIT 2",
        (authority.analysis_id, gallery_id),
    )
    if len(content_rows) > 1:
        raise PublicationCandidateConflictError(
            "projection gallery owns multiple effective-content groups"
        )
    content = (
        None
        if not content_rows
        else require_digest32(content_rows[0][0], field="projection content_sha256")
    )
    sort_row = database.execute(
        "SELECT payload_offset, byte_count FROM canonical_values "
        "WHERE value_sha256 = ?",
        (sqlite3.Binary(sort_title),),
    ).fetchone()
    if sort_row is None:
        raise PublicationCandidateConflictError("projection sort payload is missing")
    sort_offset = require_int63(sort_row[0], field="projection sort payload offset")
    sort_count = require_int63(sort_row[1], field="projection sort payload count")
    item_placeholder = b"\0" * 32
    cursor = database.execute(
        "INSERT INTO publications "
        "(publication_key, gallery_id, summary_sha256, language_sha256, "
        "published_at, modified_at, source_title_sha256, "
        "source_gallery_name, title_sha256, sort_title_sha256, sort_title, "
        "content_sha256, order_position, item_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, zeroblob(?), ?, NULL, ?)",
        (
            sqlite3.Binary(publication_key),
            gallery_id,
            sqlite3.Binary(summary),
            sqlite3.Binary(language),
            upload_time,
            modified_time,
            sqlite3.Binary(source_title),
            sqlite3.Binary(source_gallery_name),
            sqlite3.Binary(display_title),
            sqlite3.Binary(sort_title),
            sort_count,
            None if content is None else sqlite3.Binary(content),
            sqlite3.Binary(item_placeholder),
        ),
    )
    rowid = cursor.lastrowid
    if rowid is None:
        raise PublicationCandidateConflictError(
            "projection publication rowid is missing"
        )
    with database.blobopen("publications", "sort_title", rowid, readonly=False) as blob:
        for part in _iter_file_range(payload, sort_offset, sort_count):
            blob.write(part)


def _spool_projection_metadata(
    work: VNextUnitOfWork,
    *,
    gallery_id: int,
    observation_id: int,
) -> tuple[BinaryIO, _MetadataProjection]:
    spool = TemporaryFile(mode="w+b")
    decoder = identity.GalleryObservationMetadataDecoder()
    try:
        for chunk in _iter_projection_metadata_chunks(
            work,
            gallery_id=gallery_id,
            observation_id=observation_id,
        ):
            decoder.feed(chunk)
            if spool.write(chunk) != len(chunk):
                raise OSError("metadata spool accepted a partial write")
        receipt = decoder.finish()
        spool.flush()
        spool.seek(0)
        if _read_exact(spool, len(_METADATA_PREFIX)) != _METADATA_PREFIX:
            raise PublicationCandidateConflictError("METADATA prefix changed")
        if int.from_bytes(_read_exact(spool, 4), "big") != 1:
            raise PublicationCandidateConflictError("METADATA codec is not v1")
        gid = int.from_bytes(_read_exact(spool, 8), "big")
        if _read_exact(spool, 1) != b"\x01":
            raise PublicationCandidateConflictError("METADATA title tag changed")
        title_count = require_int63(
            int.from_bytes(_read_exact(spool, 8), "big"),
            field="METADATA title byte count",
        )
        title_offset = spool.tell()
        spool.seek(title_count, 1)
        if _read_exact(spool, 1) != b"\x02":
            raise PublicationCandidateConflictError("METADATA comment tag changed")
        comment_count = require_int63(
            int.from_bytes(_read_exact(spool, 8), "big"),
            field="METADATA comment byte count",
        )
        comment_offset = spool.tell()
        spool.seek(comment_count, 1)
        if _read_exact(spool, 1) != b"\x03":
            raise PublicationCandidateConflictError("METADATA account tag changed")
        account_count = require_int63(
            int.from_bytes(_read_exact(spool, 8), "big"),
            field="METADATA account byte count",
        )
        account_offset = spool.tell()
        if (
            gid != receipt.gid
            or title_count != receipt.title_byte_count
            or comment_count != receipt.comment_byte_count
            or account_count != receipt.upload_account_byte_count
        ):
            raise PublicationCandidateConflictError(
                "METADATA framing differs from its validation receipt"
            )
        return (
            spool,
            _MetadataProjection(
                title_offset,
                title_count,
                comment_offset,
                comment_count,
                account_offset,
                account_count,
                receipt.gid,
                receipt.upload_time,
                receipt.modified_time,
            ),
        )
    except BaseException:
        spool.close()
        raise


def _iter_projection_metadata_chunks(
    work: VNextUnitOfWork,
    *,
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
        raise PublicationCandidateConflictError(
            "sealed observation lacks one exact METADATA root"
        )
    root = require_digest32(roots[0][0], field="METADATA root_page_sha256")
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
            raise PublicationCandidateConflictError(
                "METADATA page or descriptor is missing"
            )
        page_bytes = require_bounded_bytes(
            row[0], field="METADATA page_bytes", minimum=1, maximum=64 * 1024
        )
        if identity.gallery_observation_page_digest(page_bytes) != page_sha256:
            raise PublicationCandidateConflictError("METADATA page digest changed")
        page = identity.decode_gallery_observation_page(page_bytes)
        level = require_int63(row[2], field="METADATA page level")
        count = require_int63(row[3], field="METADATA page item count")
        if (
            row[1] != b"METADATA"
            or page.component is not identity.GalleryObservationComponent.METADATA
            or page.level != level
            or page.subtree_item_count != count
            or (expected_level is not None and level != expected_level)
        ):
            raise PublicationCandidateConflictError(
                "METADATA descriptor differs from exact page bytes"
            )
        if page.node_kind is identity.GalleryObservationNodeKind.LEAF:
            for entry in page.entries:
                if not isinstance(entry, identity.GalleryObservationMetadataChunk):
                    raise PublicationCandidateConflictError(
                        "METADATA leaf contains a non-chunk entry"
                    )
                if entry.byte_offset != expected_offset:
                    raise PublicationCandidateConflictError(
                        "METADATA offsets are not exactly contiguous"
                    )
                expected_offset += len(entry.chunk_bytes)
                if expected_offset > INT63_MAX:
                    raise PublicationCandidateNotReadyError(
                        "METADATA stream exceeds signed-int63 bytes"
                    )
                yield entry.chunk_bytes
            return
        normalized = work.connector.fetch_all(
            "SELECT position, child_sha256 "
            "FROM catalog_gallery_observation_page_children "
            "WHERE parent_sha256 = %s ORDER BY position",
            (page_sha256,),
        )
        encoded: list[tuple[int, bytes]] = []
        for position, entry in enumerate(page.entries):
            if not isinstance(entry, identity.GalleryObservationBranchEntry):
                raise PublicationCandidateConflictError(
                    "METADATA branch contains a leaf entry"
                )
            encoded.append((position, entry.child_sha256))
        if normalized != encoded:
            raise PublicationCandidateConflictError(
                "METADATA normalized child edges differ from exact bytes"
            )
        for _position, child in normalized:
            yield from visit(
                require_digest32(child, field="METADATA child_sha256"),
                level - 1,
            )

    yield from visit(root, None)


def _read_exact(stream: BinaryIO, amount: int) -> bytes:
    count = require_int63(amount, field="exact stream byte count")
    value = stream.read(count)
    if len(value) != count:
        raise PublicationCandidateConflictError("exact stream is truncated")
    return value


def _iter_file_range(
    stream: BinaryIO,
    offset: int,
    byte_count: int,
) -> Iterator[bytes]:
    start = require_int63(offset, field="file range offset")
    remaining = require_int63(byte_count, field="file range byte_count")
    stream.seek(start)
    while remaining:
        part = stream.read(min(64 * 1024, remaining))
        if not part:
            raise PublicationCandidateConflictError("disk-backed plan is truncated")
        remaining -= len(part)
        yield part


def _iter_casefolded_file_range(
    stream: BinaryIO,
    offset: int,
    byte_count: int,
) -> Iterator[bytes]:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    for part in _iter_file_range(stream, offset, byte_count):
        text = decoder.decode(part, final=False)
        if text:
            encoded = text.casefold().encode("utf-8")
            if encoded:
                yield encoded
    tail = decoder.decode(b"", final=True)
    if tail:
        encoded = tail.casefold().encode("utf-8")
        if encoded:
            yield encoded


def _register_projection_canonical_value(
    database: sqlite3.Connection,
    payload: BinaryIO,
    *,
    domain: str,
    parts: Iterable[bytes],
) -> bytes:
    plan = CanonicalValueUploadPlan.from_parts(domain, parts)
    try:
        existing = database.execute(
            "SELECT digest_domain, payload_offset, byte_count "
            "FROM canonical_values WHERE value_sha256 = ?",
            (sqlite3.Binary(plan.value_sha256),),
        ).fetchone()
        if existing is not None:
            if (
                bytes(existing[0]) != plan.digest_domain
                or require_int63(existing[2], field="canonical value byte_count")
                != plan.byte_count
                or not _equal_streams(
                    _iter_file_range(
                        payload,
                        require_int63(existing[1], field="canonical value offset"),
                        plan.byte_count,
                    ),
                    plan.iter_payload_parts(),
                )
            ):
                raise PublicationCandidateConflictError(
                    "catalog canonical digest collides with different exact bytes"
                )
            return plan.value_sha256
        payload.seek(0, 2)
        offset = require_int63(payload.tell(), field="canonical payload offset")
        written = 0
        for part in plan.iter_payload_parts():
            if payload.write(part) != len(part):
                raise OSError("canonical projection spool accepted a partial write")
            written += len(part)
        if written != plan.byte_count:
            raise PublicationCandidateConflictError(
                "canonical projection spool changed byte count"
            )
        payload.flush()
        database.execute(
            "INSERT INTO canonical_values "
            "(value_sha256, digest_domain, payload_offset, byte_count) "
            "VALUES (?, ?, ?, ?)",
            (
                sqlite3.Binary(plan.value_sha256),
                sqlite3.Binary(plan.digest_domain),
                offset,
                plan.byte_count,
            ),
        )
        return plan.value_sha256
    finally:
        plan.close()


def _equal_streams(left: Iterable[bytes], right: Iterable[bytes]) -> bool:
    left_iter = iter(left)
    right_iter = iter(right)
    left_carry = b""
    right_carry = b""
    while True:
        if not left_carry:
            left_carry = next(left_iter, b"")
        if not right_carry:
            right_carry = next(right_iter, b"")
        if not left_carry or not right_carry:
            return not left_carry and not right_carry
        amount = min(len(left_carry), len(right_carry))
        if left_carry[:amount] != right_carry[:amount]:
            return False
        left_carry = left_carry[amount:]
        right_carry = right_carry[amount:]


def _spool_existing_canonical_value(
    work: VNextUnitOfWork,
    value_sha256: bytes,
    *,
    expected_domain: bytes,
) -> tuple[BinaryIO, int]:
    spool = TemporaryFile(mode="w+b")
    try:

        def consume(part: bytes) -> None:
            if spool.write(part) != len(part):
                raise OSError("canonical source spool accepted a partial write")

        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=value_sha256,
            consume_provisional=consume,
        )
        if receipt.digest_domain != expected_domain:
            raise PublicationCandidateConflictError(
                "canonical source value uses an unexpected digest domain"
            )
        if spool.tell() != receipt.byte_count:
            raise PublicationCandidateConflictError(
                "canonical source stream changed its byte count"
            )
        spool.flush()
        return spool, receipt.byte_count
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        spool.close()
        raise PublicationCandidateConflictError(
            "canonical source value is incomplete or corrupt"
        ) from error
    except BaseException:
        spool.close()
        raise


def _assign_projection_order(database: sqlite3.Connection) -> None:
    cursor = database.execute(
        "SELECT publication_key FROM publications ORDER BY sort_title, publication_key"
    )
    position = 0
    while True:
        rows = cursor.fetchmany(_CATALOG_BATCH_ROWS)
        if not rows:
            break
        for (publication_key,) in rows:
            database.execute(
                "UPDATE publications SET order_position = ? WHERE publication_key = ?",
                (position, publication_key),
            )
            position += 1
            if position > INT63_MAX:
                raise PublicationCandidateNotReadyError(
                    "catalog publication order is exhausted"
                )


def _finalize_projection_item_digests(database: sqlite3.Connection) -> None:
    cursor = database.execute(
        "SELECT publication_key, gallery_id, summary_sha256, language_sha256, "
        "published_at, modified_at, source_title_sha256, "
        "source_gallery_name, title_sha256, sort_title_sha256, content_sha256, "
        "order_position FROM publications ORDER BY publication_key"
    )
    while True:
        rows = cursor.fetchmany(_CATALOG_BATCH_ROWS)
        if not rows:
            return
        for row in rows:
            publication_key = bytes(row[0])
            digest = sha256(b"h2hdb-vnext-catalog-item-audit\0")
            digest.update(publication_key)
            digest.update(int(row[1]).to_bytes(8, "big"))
            for value in (row[2], row[3]):
                digest.update(bytes(value))
            for value in (row[4], row[5]):
                digest.update(int(value).to_bytes(8, "big"))
            digest.update(bytes(row[6]))
            gallery_name = bytes(row[7])
            digest.update(len(gallery_name).to_bytes(2, "big"))
            digest.update(gallery_name)
            digest.update(bytes(row[8]))
            digest.update(bytes(row[9]))
            if row[10] is None:
                digest.update(b"\0")
            else:
                digest.update(b"\1")
                digest.update(bytes(row[10]))
            digest.update(int(row[11]).to_bytes(8, "big"))
            contributor_cursor = database.execute(
                "SELECT position, contributor_name_sha256, role, sort_as_sha256 "
                "FROM contributors WHERE publication_key = ? ORDER BY position",
                (sqlite3.Binary(publication_key),),
            )
            contributor_count = 0
            while True:
                contributor_rows = contributor_cursor.fetchmany(_CATALOG_BATCH_ROWS)
                if not contributor_rows:
                    break
                for contributor in contributor_rows:
                    if int(contributor[0]) != contributor_count:
                        raise PublicationCandidateConflictError(
                            "projection contributor positions are not contiguous"
                        )
                    digest.update(b"C")
                    digest.update(contributor_count.to_bytes(8, "big"))
                    digest.update(bytes(contributor[1]))
                    role = bytes(contributor[2])
                    digest.update(len(role).to_bytes(2, "big"))
                    digest.update(role)
                    if contributor[3] is None:
                        digest.update(b"\0")
                    else:
                        digest.update(b"\1")
                        digest.update(bytes(contributor[3]))
                    contributor_count += 1
            subject_cursor = database.execute(
                "SELECT position, tag_id FROM subjects "
                "WHERE publication_key = ? ORDER BY position",
                (sqlite3.Binary(publication_key),),
            )
            subject_count = 0
            while True:
                subject_rows = subject_cursor.fetchmany(_CATALOG_BATCH_ROWS)
                if not subject_rows:
                    break
                for subject in subject_rows:
                    if int(subject[0]) != subject_count:
                        raise PublicationCandidateConflictError(
                            "projection subject positions are not contiguous"
                        )
                    digest.update(b"S")
                    digest.update(subject_count.to_bytes(8, "big"))
                    digest.update(int(subject[1]).to_bytes(8, "big"))
                    subject_count += 1
            database.execute(
                "UPDATE publications SET item_sha256 = ? WHERE publication_key = ?",
                (sqlite3.Binary(digest.digest()), sqlite3.Binary(publication_key)),
            )


def _populate_projection_children(database: sqlite3.Connection) -> int:
    count = 0

    def insert(kind: int, publication_key: bytes, subkey: bytes) -> None:
        nonlocal count
        cursor = _encode_catalog_child_cursor(kind, publication_key, subkey)
        database.execute(
            "INSERT INTO children (cursor, kind, publication_key, subkey) "
            "VALUES (?, ?, ?, ?)",
            (
                sqlite3.Binary(cursor),
                kind,
                sqlite3.Binary(publication_key),
                sqlite3.Binary(subkey),
            ),
        )
        count += 1
        if count > INT63_MAX:
            raise PublicationCandidateNotReadyError(
                "catalog projection child count is exhausted"
            )

    sources = (
        (
            _CATALOG_CHILD_PUBLICATION,
            "SELECT publication_key, NULL FROM publications ORDER BY publication_key",
        ),
        (
            _CATALOG_CHILD_ORDER,
            "SELECT publication_key, order_position FROM publications "
            "ORDER BY publication_key",
        ),
        (
            _CATALOG_CHILD_TITLE,
            "SELECT publication_key, NULL FROM publications ORDER BY publication_key",
        ),
        (
            _CATALOG_CHILD_CONTENT,
            "SELECT publication_key, NULL FROM publications "
            "WHERE content_sha256 IS NOT NULL ORDER BY publication_key",
        ),
        (
            _CATALOG_CHILD_CONTRIBUTOR,
            "SELECT publication_key, position FROM contributors "
            "ORDER BY publication_key, position",
        ),
        (
            _CATALOG_CHILD_CONTRIBUTOR_SORT_AS,
            "SELECT publication_key, position FROM contributors "
            "WHERE sort_as_sha256 IS NOT NULL ORDER BY publication_key, position",
        ),
        (
            _CATALOG_CHILD_SUBJECT,
            "SELECT publication_key, position FROM subjects "
            "ORDER BY publication_key, position",
        ),
    )
    for kind, query in sources:
        cursor = database.execute(query)
        while True:
            rows = cursor.fetchmany(_CATALOG_BATCH_ROWS)
            if not rows:
                break
            for publication_key, position in rows:
                subkey = (
                    b""
                    if position is None
                    else require_int63(
                        position, field="catalog child position"
                    ).to_bytes(8, "big")
                )
                insert(kind, bytes(publication_key), subkey)
    publication_cursor = database.execute(
        "SELECT publication_key, summary_sha256, language_sha256, "
        "source_title_sha256, title_sha256, sort_title_sha256 "
        "FROM publications ORDER BY publication_key"
    )
    while True:
        rows = publication_cursor.fetchmany(_CATALOG_BATCH_ROWS)
        if not rows:
            break
        for row in rows:
            publication_key = bytes(row[0])
            scalar_cursor = _encode_catalog_child_cursor(
                _CATALOG_CHILD_PUBLICATION,
                publication_key,
                b"",
            )
            title_cursor = _encode_catalog_child_cursor(
                _CATALOG_CHILD_TITLE,
                publication_key,
                b"",
            )
            for value in (row[1], row[2]):
                _assign_projection_canonical_consumer(
                    database, bytes(value), scalar_cursor
                )
            for value in (row[3], row[4], row[5]):
                _assign_projection_canonical_consumer(
                    database, bytes(value), title_cursor
                )
    contributor_cursor = database.execute(
        "SELECT publication_key, position, contributor_name_sha256, sort_as_sha256 "
        "FROM contributors ORDER BY publication_key, position"
    )
    while True:
        rows = contributor_cursor.fetchmany(_CATALOG_BATCH_ROWS)
        if not rows:
            break
        for row in rows:
            publication_key = bytes(row[0])
            position = require_int63(row[1], field="contributor position")
            _assign_projection_canonical_consumer(
                database,
                bytes(row[2]),
                _encode_catalog_child_cursor(
                    _CATALOG_CHILD_CONTRIBUTOR,
                    publication_key,
                    position.to_bytes(8, "big"),
                ),
            )
            if row[3] is not None:
                _assign_projection_canonical_consumer(
                    database,
                    bytes(row[3]),
                    _encode_catalog_child_cursor(
                        _CATALOG_CHILD_CONTRIBUTOR_SORT_AS,
                        publication_key,
                        position.to_bytes(8, "big"),
                    ),
                )
    if database.execute(
        "SELECT 1 FROM canonical_values WHERE consumer_cursor IS NULL LIMIT 1"
    ).fetchone():
        raise PublicationCandidateConflictError(
            "catalog canonical plan has no exact first consumer"
        )
    return count


def _assign_projection_canonical_consumer(
    database: sqlite3.Connection,
    value_sha256: bytes,
    cursor: bytes,
) -> None:
    row = database.execute(
        "SELECT consumer_cursor FROM canonical_values WHERE value_sha256 = ?",
        (sqlite3.Binary(value_sha256),),
    ).fetchone()
    if row is None:
        raise PublicationCandidateConflictError(
            "catalog consumer references an absent canonical plan"
        )
    if row[0] is None or cursor < bytes(row[0]):
        database.execute(
            "UPDATE canonical_values SET consumer_cursor = ? WHERE value_sha256 = ?",
            (sqlite3.Binary(cursor), sqlite3.Binary(value_sha256)),
        )


def _encode_catalog_child_cursor(
    kind: int,
    publication_key: bytes,
    subkey: bytes,
) -> bytes:
    if kind not in range(8):
        raise ValueError("catalog child kind is not registered")
    key = require_digest32(publication_key, field="catalog child publication_key")
    exact_subkey = require_bounded_bytes(
        subkey,
        field="catalog child subkey",
        maximum=130,
    )
    cursor = (
        b"\x01"
        + bytes((kind,))
        + key
        + len(exact_subkey).to_bytes(2, "big")
        + exact_subkey
    )
    _validate_stage_cursor(_CURSOR_CATALOG_CHILD, cursor)
    return cursor


def _require_projection_plan(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    plan: PublicationCatalogProjectionPlan,
    *,
    validation: bool,
) -> None:
    if not isinstance(plan, PublicationCatalogProjectionPlan):
        raise TypeError("plan must be PublicationCatalogProjectionPlan")
    expected_capability = (
        _PROJECTION_VALIDATION_PLAN_TOKEN
        if validation
        else _PROJECTION_BUILD_PLAN_TOKEN
    )
    if plan._capability is not expected_capability:
        raise TypeError("catalog projection plan is not repository-issued")
    plan._require_open()
    plan.authority.__post_init__()
    require_int63(
        plan.publication_count,
        field="catalog projection plan publication_count",
    )
    require_int63(
        plan.child_count,
        field="catalog projection plan child_count",
    )
    if type(plan.validation) is not bool:
        raise TypeError("catalog projection plan validation must be bool")
    if plan.validation is not validation:
        raise PublicationCandidateNotReadyError(
            "catalog projection plan has the wrong build/validation role"
        )
    loaded = _load_projection_authority(work, plan.authority)
    if loaded != authority:
        raise PublicationCandidateNotReadyError(
            "catalog projection plan authority differs from the live candidate"
        )
    if plan.publication_count != plan.authority.publication_count:
        raise PublicationCandidateConflictError(
            "catalog projection plan publication count changed"
        )


def _ensure_reserved_catalog_revision(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    *,
    publication_count: int,
    now: int,
) -> None:
    count = require_int63(publication_count, field="catalog revision publication_count")
    timestamp = require_int63(now, field="catalog revision published_at")
    row = work.connector.fetch_one(
        f"SELECT publication_count, published_at FROM {_CATALOG_REVISION_TABLE} "
        "WHERE revision = %s",
        (authority.candidate.reserved_revision,),
    )
    if row:
        if len(row) != 2 or row[0] != count:
            raise PublicationCandidateConflictError(
                "reserved catalog revision differs from selection authority"
            )
        published_at = require_int63(row[1], field="catalog revision published_at")
        if not authority.candidate.created_at <= published_at <= timestamp:
            raise PublicationCandidateConflictError(
                "reserved catalog revision timestamp is not monotone"
            )
        return
    work.connector.execute(
        f"INSERT INTO {_CATALOG_REVISION_TABLE} "
        "(revision, publication_count, published_at) VALUES (%s, %s, %s)",
        (authority.candidate.reserved_revision, count, timestamp),
    )


def _lock_projection_upload_claims(
    work: VNextUnitOfWork,
    plan: PublicationCatalogProjectionPlan,
    children: tuple[_ProjectionChild, ...],
) -> None:
    values: set[bytes] = set()
    for child in children:
        rows = plan._database.execute(
            "SELECT value_sha256 FROM canonical_values WHERE consumer_cursor = ? "
            "ORDER BY value_sha256",
            (sqlite3.Binary(child.cursor),),
        ).fetchall()
        values.update(bytes(row[0]) for row in rows)
    for value in sorted(values):
        claim = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "publication-canonical-consumer",
                plan.authority.generation,
                value,
            ),
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (plan.authority.generation, value),
        )
        if claim != (plan.authority.generation, value):
            raise PublicationCandidateNotReadyError(
                "first catalog consumer requires its exact generation upload claim"
            )


def _plan_publication(
    plan: PublicationCatalogProjectionPlan,
    publication_key: bytes,
) -> tuple[Any, ...]:
    row = plan._database.execute(
        "SELECT gallery_id, summary_sha256, language_sha256, published_at, "
        "modified_at, source_title_sha256, "
        "source_gallery_name, title_sha256, sort_title_sha256, content_sha256, "
        "order_position, item_sha256 FROM publications WHERE publication_key = ?",
        (sqlite3.Binary(publication_key),),
    ).fetchone()
    if row is None or len(row) != 12:
        raise PublicationCandidateConflictError(
            "catalog projection plan publication is missing"
        )
    return tuple(row)


def _plan_canonical(
    plan: PublicationCatalogProjectionPlan,
    value_sha256: bytes,
) -> tuple[bytes, int, bytes]:
    row = plan._database.execute(
        "SELECT digest_domain, byte_count, consumer_cursor FROM canonical_values "
        "WHERE value_sha256 = ?",
        (sqlite3.Binary(value_sha256),),
    ).fetchone()
    if row is None or len(row) != 3 or row[2] is None:
        raise PublicationCandidateConflictError(
            "catalog projection canonical plan is missing"
        )
    return (
        require_bounded_bytes(
            row[0], field="projection canonical domain", minimum=1, maximum=64
        ),
        require_int63(row[1], field="projection canonical byte_count"),
        require_bounded_bytes(
            row[2],
            field="projection canonical consumer_cursor",
            minimum=36,
            maximum=2048,
        ),
    )


def _consume_projection_canonical(
    work: VNextUnitOfWork,
    plan: PublicationCatalogProjectionPlan,
    value_sha256: bytes,
    *,
    expected_domain: bytes,
    child_cursor: bytes,
) -> None:
    value = require_digest32(value_sha256, field="projection canonical value_sha256")
    plan_domain, plan_count, first_consumer = _plan_canonical(plan, value)
    if plan_domain != expected_domain:
        raise PublicationCandidateConflictError(
            "catalog projection canonical plan has the wrong domain"
        )
    row = work.connector.fetch_one(
        "SELECT allocation.digest_domain, allocation.byte_count, "
        "identity.root_page_sha256 FROM catalog_canonical_value_allocations allocation "
        "JOIN catalog_canonical_value_identities identity "
        "ON identity.value_sha256 = allocation.value_sha256 "
        "WHERE allocation.value_sha256 = %s",
        (value,),
    )
    if (
        len(row) != 3
        or row[0] != expected_domain
        or require_int63(row[1], field="canonical allocation byte_count") != plan_count
    ):
        raise PublicationCandidateNotReadyError(
            "catalog projection canonical value is not exactly sealed"
        )
    require_digest32(row[2], field="canonical identity root_page_sha256")
    if child_cursor != first_consumer:
        return
    claim = work.connector.fetch_one(
        "SELECT generation, value_sha256 "
        "FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (plan.authority.generation, value),
    )
    if claim != (plan.authority.generation, value):
        raise PublicationCandidateNotReadyError(
            "first catalog consumer requires its exact generation upload claim"
        )
    deleted = work.connector.execute_affected(
        "DELETE FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (plan.authority.generation, value),
    )
    if deleted != 1:
        raise PublicationCandidateConflictError(
            "catalog canonical upload claim changed during consumer handoff"
        )


def _insert_projection_child(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    plan: PublicationCatalogProjectionPlan,
    child: _ProjectionChild,
) -> None:
    revision = authority.candidate.reserved_revision
    publication = _plan_publication(plan, child.publication_key)
    if child.kind == _CATALOG_CHILD_PUBLICATION:
        summary = require_digest32(publication[1], field="publication summary_sha256")
        language = require_digest32(publication[2], field="publication language_sha256")
        _consume_projection_canonical(
            work,
            plan,
            summary,
            expected_domain=b"catalog_summary_utf8_v1",
            child_cursor=child.cursor,
        )
        _consume_projection_canonical(
            work,
            plan,
            language,
            expected_domain=b"catalog_language_utf8_v1",
            child_cursor=child.cursor,
        )
        work.connector.execute(
            f"INSERT INTO {_PUBLICATION_TABLE} "
            "(revision, gallery_id, publication_key, summary_sha256, "
            "language_sha256, published_at, modified_at, item_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                revision,
                require_positive_int63(publication[0], field="publication gallery_id"),
                child.publication_key,
                summary,
                language,
                require_int63(publication[3], field="publication published_at"),
                require_int63(publication[4], field="publication modified_at"),
                require_digest32(publication[11], field="publication item_sha256"),
            ),
        )
        return
    if child.kind == _CATALOG_CHILD_ORDER:
        position = _position_subkey(child.subkey, field="publication order position")
        if position != require_int63(publication[10], field="planned order position"):
            raise PublicationCandidateConflictError(
                "catalog order cursor differs from its planned position"
            )
        work.connector.execute(
            f"INSERT INTO {_PUBLICATION_ORDER_TABLE} "
            "(revision, position, publication_key) VALUES (%s, %s, %s)",
            (revision, position, child.publication_key),
        )
        return
    if child.kind == _CATALOG_CHILD_TITLE:
        _insert_projection_title(
            work, authority, plan, child.publication_key, publication
        )
        return
    if child.kind == _CATALOG_CHILD_CONTENT:
        if publication[9] is None:
            raise PublicationCandidateConflictError(
                "catalog content child has no effective-content identity"
            )
        content = require_digest32(publication[9], field="publication content_sha256")
        _require_existing_canonical_domain(
            work,
            content,
            expected_domain=b"effective_content_v1",
        )
        work.connector.execute(
            f"INSERT INTO {_PUBLICATION_CONTENT_TABLE} "
            "(revision, publication_key, content_sha256) VALUES (%s, %s, %s)",
            (revision, child.publication_key, content),
        )
        return
    if child.kind in {
        _CATALOG_CHILD_CONTRIBUTOR,
        _CATALOG_CHILD_CONTRIBUTOR_SORT_AS,
    }:
        _insert_projection_contributor_child(work, authority, plan, child)
        return
    if child.kind == _CATALOG_CHILD_SUBJECT:
        position = _position_subkey(child.subkey, field="subject position")
        row = plan._database.execute(
            "SELECT tag_id FROM subjects WHERE publication_key = ? AND position = ?",
            (sqlite3.Binary(child.publication_key), position),
        ).fetchone()
        if row is None:
            raise PublicationCandidateConflictError("planned subject is missing")
        work.connector.execute(
            f"INSERT INTO {_SUBJECT_TABLE} "
            "(revision, publication_key, position, tag_id) VALUES (%s, %s, %s, %s)",
            (
                revision,
                child.publication_key,
                position,
                require_positive_int63(row[0], field="subject tag_id"),
            ),
        )
        return
    raise PublicationCandidateNotReadyError(
        "catalog artifact child awaits the typed artifact adapter"
    )


def _insert_projection_title(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    plan: PublicationCatalogProjectionPlan,
    publication_key: bytes,
    publication: tuple[Any, ...],
) -> None:
    child_cursor = _encode_catalog_child_cursor(
        _CATALOG_CHILD_TITLE,
        publication_key,
        b"",
    )
    source_title = require_digest32(publication[5], field="source_title_sha256")
    source_gallery_name = require_bounded_bytes(
        publication[6],
        field="source_gallery_name",
        minimum=1,
        maximum=255,
    )
    title = require_digest32(publication[7], field="title_sha256")
    sort_title = require_digest32(publication[8], field="sort_title_sha256")
    _consume_projection_canonical(
        work,
        plan,
        source_title,
        expected_domain=b"source_title_utf8_v1",
        child_cursor=child_cursor,
    )
    _consume_projection_canonical(
        work,
        plan,
        title,
        expected_domain=b"display_title_utf8_v1",
        child_cursor=child_cursor,
    )
    _consume_projection_canonical(
        work,
        plan,
        sort_title,
        expected_domain=b"title_sort_utf8_v1",
        child_cursor=child_cursor,
    )
    display_policy = authority.candidate.display_title_policy_id
    policy = work.connector.fetch_one(
        "SELECT title_sort_policy_id FROM catalog_display_title_policies "
        "WHERE display_title_policy_id = %s",
        (display_policy,),
    )
    if len(policy) != 1:
        raise PublicationCandidateConflictError("display-title policy is missing")
    sort_policy = require_positive_int63(policy[0], field="title_sort_policy_id")
    existing_choice = work.connector.fetch_one(
        f"SELECT title_sha256 FROM {_DISPLAY_TITLE_TABLE} "
        "WHERE display_title_policy_id = %s AND source_title_sha256 = %s "
        "AND source_gallery_name = %s",
        (display_policy, source_title, source_gallery_name),
    )
    if existing_choice:
        if existing_choice != (title,):
            raise PublicationCandidateConflictError(
                "display-title determinant maps to conflicting exact bytes"
            )
    else:
        work.connector.execute(
            f"INSERT INTO {_DISPLAY_TITLE_TABLE} "
            "(display_title_policy_id, source_title_sha256, source_gallery_name, "
            "title_sha256) VALUES (%s, %s, %s, %s)",
            (display_policy, source_title, source_gallery_name, title),
        )
    existing_sort = work.connector.fetch_one(
        f"SELECT sort_title_sha256 FROM {_TITLE_SORT_TABLE} "
        "WHERE title_sort_policy_id = %s AND title_sha256 = %s",
        (sort_policy, title),
    )
    if existing_sort:
        if existing_sort != (sort_title,):
            raise PublicationCandidateConflictError(
                "title-sort determinant maps to conflicting exact bytes"
            )
    else:
        work.connector.execute(
            f"INSERT INTO {_TITLE_SORT_TABLE} "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (%s, %s, %s)",
            (sort_policy, title, sort_title),
        )
    work.connector.execute(
        f"INSERT INTO {_PUBLICATION_TITLE_TABLE} "
        "(revision, publication_key, display_title_policy_id, "
        "source_title_sha256, source_gallery_name) VALUES (%s, %s, %s, %s, %s)",
        (
            authority.candidate.reserved_revision,
            publication_key,
            display_policy,
            source_title,
            source_gallery_name,
        ),
    )


def _insert_projection_contributor_child(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    plan: PublicationCatalogProjectionPlan,
    child: _ProjectionChild,
) -> None:
    position = _position_subkey(child.subkey, field="contributor position")
    row = plan._database.execute(
        "SELECT contributor_name_sha256, role, sort_as_sha256 "
        "FROM contributors WHERE publication_key = ? AND position = ?",
        (sqlite3.Binary(child.publication_key), position),
    ).fetchone()
    if row is None or len(row) != 3:
        raise PublicationCandidateConflictError("planned contributor is missing")
    name = require_digest32(row[0], field="contributor_name_sha256")
    role = require_bounded_bytes(
        row[1], field="contributor role", minimum=1, maximum=64
    )
    if child.kind == _CATALOG_CHILD_CONTRIBUTOR:
        _consume_projection_canonical(
            work,
            plan,
            name,
            expected_domain=b"contributor_name_utf8_v1",
            child_cursor=child.cursor,
        )
        work.connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_TABLE} "
            "(revision, publication_key, position, contributor_name_sha256, role) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                authority.candidate.reserved_revision,
                child.publication_key,
                position,
                name,
                role,
            ),
        )
        return
    if row[2] is None:
        raise PublicationCandidateConflictError(
            "contributor sort-as child has no planned canonical value"
        )
    sort_as = require_digest32(row[2], field="contributor sort_as_sha256")
    _consume_projection_canonical(
        work,
        plan,
        sort_as,
        expected_domain=b"contributor_sort_as_utf8_v1",
        child_cursor=child.cursor,
    )
    work.connector.execute(
        f"INSERT INTO {_CONTRIBUTOR_SORT_AS_TABLE} "
        "(revision, publication_key, position, sort_as_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            authority.candidate.reserved_revision,
            child.publication_key,
            position,
            sort_as,
        ),
    )


def _position_subkey(subkey: bytes, *, field: str) -> int:
    if len(subkey) != 8:
        raise PublicationCandidateConflictError(f"{field} cursor is not u64be")
    return require_int63(int.from_bytes(subkey, "big"), field=field)


def _require_existing_canonical_domain(
    work: VNextUnitOfWork,
    value_sha256: bytes,
    *,
    expected_domain: bytes,
) -> None:
    row = work.connector.fetch_one(
        "SELECT allocation.digest_domain, identity.root_page_sha256 "
        "FROM catalog_canonical_value_allocations allocation "
        "JOIN catalog_canonical_value_identities identity "
        "ON identity.value_sha256 = allocation.value_sha256 "
        "WHERE allocation.value_sha256 = %s",
        (value_sha256,),
    )
    if len(row) != 2 or row[0] != expected_domain:
        raise PublicationCandidateConflictError(
            "referenced canonical identity has the wrong domain"
        )
    require_digest32(row[1], field="canonical identity root_page_sha256")


def _compare_reserved_catalog_revision(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    *,
    publication_count: int,
    now: int,
) -> None:
    row = work.connector.fetch_one(
        f"SELECT publication_count, published_at FROM {_CATALOG_REVISION_TABLE} "
        "WHERE revision = %s",
        (authority.candidate.reserved_revision,),
    )
    if (
        len(row) != 2
        or require_int63(row[0], field="catalog revision publication_count")
        != publication_count
    ):
        raise PublicationCandidateConflictError(
            "reserved catalog revision differs from projection authority"
        )
    published_at = require_int63(row[1], field="catalog revision published_at")
    if (
        not authority.candidate.created_at
        <= published_at
        <= require_int63(now, field="catalog validation now")
    ):
        raise PublicationCandidateConflictError(
            "reserved catalog revision timestamp is not monotone"
        )


def _decode_catalog_child_cursor(cursor: bytes) -> tuple[int, bytes, bytes] | None:
    _validate_stage_cursor(_CURSOR_CATALOG_CHILD, cursor)
    if not cursor:
        return None
    return cursor[1], cursor[2:34], cursor[36:]


def _load_catalog_child_page(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    *,
    after: bytes,
) -> tuple[_ProjectionChild, ...]:
    boundary = _decode_catalog_child_cursor(after)
    result: list[_ProjectionChild] = []
    revision = authority.candidate.reserved_revision
    for kind in range(8):
        if len(result) == _CATALOG_BATCH_ROWS:
            break
        if boundary is not None and kind < boundary[0]:
            continue
        remaining = _CATALOG_BATCH_ROWS - len(result)
        same_boundary = (
            boundary if boundary is not None and kind == boundary[0] else None
        )
        key = None if same_boundary is None else same_boundary[1]
        subkey = b"" if same_boundary is None else same_boundary[2]
        rows = _catalog_child_kind_rows(
            work,
            revision=revision,
            kind=kind,
            after_key=key,
            after_subkey=subkey,
            limit=remaining,
        )
        for publication_key, child_subkey in rows:
            child = _ProjectionChild(
                _encode_catalog_child_cursor(kind, publication_key, child_subkey),
                kind,
                publication_key,
                child_subkey,
            )
            if child.cursor <= after or (result and child.cursor <= result[-1].cursor):
                raise PublicationCandidateConflictError(
                    "catalog child evaluator did not advance in strict order"
                )
            result.append(child)
    return tuple(result)


def _catalog_child_kind_rows(
    work: VNextUnitOfWork,
    *,
    revision: int,
    kind: int,
    after_key: bytes | None,
    after_subkey: bytes,
    limit: int,
) -> tuple[tuple[bytes, bytes], ...]:
    connector = work.connector
    parameters: list[Any] = [revision]
    predicate = ""
    if kind in {
        _CATALOG_CHILD_PUBLICATION,
        _CATALOG_CHILD_TITLE,
        _CATALOG_CHILD_CONTENT,
    }:
        table = {
            _CATALOG_CHILD_PUBLICATION: _PUBLICATION_TABLE,
            _CATALOG_CHILD_TITLE: _PUBLICATION_TITLE_TABLE,
            _CATALOG_CHILD_CONTENT: _PUBLICATION_CONTENT_TABLE,
        }[kind]
        if after_key is not None:
            predicate = " AND publication_key > %s"
            parameters.append(after_key)
        parameters.append(limit)
        raw = connector.fetch_all(
            f"SELECT publication_key FROM {table} WHERE revision = %s"
            + predicate
            + " ORDER BY publication_key LIMIT %s",
            tuple(parameters),
        )
        return tuple(
            (require_digest32(row[0], field="catalog child publication_key"), b"")
            for row in raw
        )
    if kind in {
        _CATALOG_CHILD_ORDER,
        _CATALOG_CHILD_CONTRIBUTOR,
        _CATALOG_CHILD_CONTRIBUTOR_SORT_AS,
        _CATALOG_CHILD_SUBJECT,
    }:
        table = {
            _CATALOG_CHILD_ORDER: _PUBLICATION_ORDER_TABLE,
            _CATALOG_CHILD_CONTRIBUTOR: _CONTRIBUTOR_TABLE,
            _CATALOG_CHILD_CONTRIBUTOR_SORT_AS: _CONTRIBUTOR_SORT_AS_TABLE,
            _CATALOG_CHILD_SUBJECT: _SUBJECT_TABLE,
        }[kind]
        if after_key is not None:
            after_position = _position_subkey(
                after_subkey,
                field="catalog child boundary position",
            )
            predicate = (
                " AND (publication_key > %s OR "
                "(publication_key = %s AND position > %s))"
            )
            parameters.extend((after_key, after_key, after_position))
        parameters.append(limit)
        raw = connector.fetch_all(
            f"SELECT publication_key, position FROM {table} WHERE revision = %s"
            + predicate
            + " ORDER BY publication_key, position LIMIT %s",
            tuple(parameters),
        )
        return tuple(
            (
                require_digest32(row[0], field="catalog child publication_key"),
                require_int63(row[1], field="catalog child position").to_bytes(
                    8, "big"
                ),
            )
            for row in raw
        )
    if kind != _CATALOG_CHILD_ARTIFACT:
        raise ValueError("catalog child kind is not registered")
    if after_key is not None:
        if (
            len(after_subkey) < 3
            or int.from_bytes(after_subkey[:2], "big") != len(after_subkey) - 2
        ):
            raise PublicationCandidateConflictError(
                "catalog artifact boundary is malformed"
            )
        artifact_id = after_subkey[2:]
        predicate = (
            " AND (identity.publication_key > %s OR "
            "(identity.publication_key = %s AND artifact.artifact_id > %s))"
        )
        parameters.extend((after_key, after_key, artifact_id))
    parameters.append(limit)
    raw = connector.fetch_all(
        "SELECT identity.publication_key, artifact.artifact_id "
        "FROM catalog_artifacts AS artifact "
        "JOIN catalog_artifact_identity AS identity "
        "ON identity.artifact_id = artifact.artifact_id "
        "WHERE artifact.revision = %s"
        + predicate
        + " ORDER BY identity.publication_key, artifact.artifact_id LIMIT %s",
        tuple(parameters),
    )
    result: list[tuple[bytes, bytes]] = []
    for row in raw:
        publication_key = require_digest32(
            row[0], field="catalog artifact publication_key"
        )
        artifact_id = require_bounded_bytes(
            row[1], field="catalog artifact_id", minimum=1, maximum=128
        )
        result.append(
            (publication_key, len(artifact_id).to_bytes(2, "big") + artifact_id)
        )
    return tuple(result)


def _compare_projection_child(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    plan: PublicationCatalogProjectionPlan,
    child: _ProjectionChild,
) -> None:
    revision = authority.candidate.reserved_revision
    publication = _plan_publication(plan, child.publication_key)
    if child.kind == _CATALOG_CHILD_PUBLICATION:
        row = work.connector.fetch_one(
            f"SELECT gallery_id, summary_sha256, language_sha256, published_at, "
            "modified_at, item_sha256 "
            f"FROM {_PUBLICATION_TABLE} WHERE revision = %s AND publication_key = %s",
            (revision, child.publication_key),
        )
        expected_publication = (
            require_positive_int63(publication[0], field="planned gallery_id"),
            require_digest32(publication[1], field="planned summary_sha256"),
            require_digest32(publication[2], field="planned language_sha256"),
            require_int63(publication[3], field="planned published_at"),
            require_int63(publication[4], field="planned modified_at"),
            require_digest32(publication[11], field="planned item_sha256"),
        )
        if row != expected_publication:
            raise PublicationCandidateConflictError(
                "catalog publication differs from independent evaluator"
            )
        return
    if child.kind == _CATALOG_CHILD_ORDER:
        position = _position_subkey(child.subkey, field="planned order position")
        row = work.connector.fetch_one(
            f"SELECT publication_key FROM {_PUBLICATION_ORDER_TABLE} "
            "WHERE revision = %s AND position = %s",
            (revision, position),
        )
        if row != (child.publication_key,) or position != publication[10]:
            raise PublicationCandidateConflictError(
                "catalog publication order differs from independent evaluator"
            )
        return
    if child.kind == _CATALOG_CHILD_TITLE:
        row = work.connector.fetch_one(
            f"SELECT display_title_policy_id, source_title_sha256, "
            f"source_gallery_name FROM {_PUBLICATION_TITLE_TABLE} "
            "WHERE revision = %s AND publication_key = %s",
            (revision, child.publication_key),
        )
        expected_title = (
            authority.candidate.display_title_policy_id,
            require_digest32(publication[5], field="planned source_title_sha256"),
            require_bounded_bytes(
                publication[6],
                field="planned source_gallery_name",
                minimum=1,
                maximum=255,
            ),
        )
        if row != expected_title:
            raise PublicationCandidateConflictError(
                "catalog publication title differs from independent evaluator"
            )
        choice = work.connector.fetch_one(
            f"SELECT title_sha256 FROM {_DISPLAY_TITLE_TABLE} "
            "WHERE display_title_policy_id = %s AND source_title_sha256 = %s "
            "AND source_gallery_name = %s",
            expected_title,
        )
        if choice != (require_digest32(publication[7], field="planned title_sha256"),):
            raise PublicationCandidateConflictError(
                "catalog display title differs from independent evaluator"
            )
        policy = work.connector.fetch_one(
            "SELECT title_sort_policy_id FROM catalog_display_title_policies "
            "WHERE display_title_policy_id = %s",
            (authority.candidate.display_title_policy_id,),
        )
        if len(policy) != 1:
            raise PublicationCandidateConflictError("title-sort policy is missing")
        sort_row = work.connector.fetch_one(
            f"SELECT sort_title_sha256 FROM {_TITLE_SORT_TABLE} "
            "WHERE title_sort_policy_id = %s AND title_sha256 = %s",
            (policy[0], publication[7]),
        )
        if sort_row != (
            require_digest32(publication[8], field="planned sort_title_sha256"),
        ):
            raise PublicationCandidateConflictError(
                "catalog title sort differs from independent evaluator"
            )
        return
    if child.kind == _CATALOG_CHILD_CONTENT:
        content = require_digest32(publication[9], field="planned content_sha256")
        row = work.connector.fetch_one(
            f"SELECT content_sha256 FROM {_PUBLICATION_CONTENT_TABLE} "
            "WHERE revision = %s AND publication_key = %s",
            (revision, child.publication_key),
        )
        if row != (content,):
            raise PublicationCandidateConflictError(
                "catalog publication content differs from independent evaluator"
            )
        return
    if child.kind in {
        _CATALOG_CHILD_CONTRIBUTOR,
        _CATALOG_CHILD_CONTRIBUTOR_SORT_AS,
    }:
        position = _position_subkey(child.subkey, field="planned contributor position")
        planned = plan._database.execute(
            "SELECT contributor_name_sha256, role, sort_as_sha256 "
            "FROM contributors WHERE publication_key = ? AND position = ?",
            (sqlite3.Binary(child.publication_key), position),
        ).fetchone()
        if planned is None:
            raise PublicationCandidateConflictError("planned contributor is missing")
        expected_contributor: tuple[Any, ...]
        if child.kind == _CATALOG_CHILD_CONTRIBUTOR:
            row = work.connector.fetch_one(
                f"SELECT contributor_name_sha256, role FROM {_CONTRIBUTOR_TABLE} "
                "WHERE revision = %s AND publication_key = %s AND position = %s",
                (revision, child.publication_key, position),
            )
            expected_contributor = (
                require_digest32(planned[0], field="planned contributor name"),
                require_bounded_bytes(
                    planned[1],
                    field="planned contributor role",
                    minimum=1,
                    maximum=64,
                ),
            )
        else:
            if planned[2] is None:
                raise PublicationCandidateConflictError(
                    "planned contributor sort-as is missing"
                )
            row = work.connector.fetch_one(
                f"SELECT sort_as_sha256 FROM {_CONTRIBUTOR_SORT_AS_TABLE} "
                "WHERE revision = %s AND publication_key = %s AND position = %s",
                (revision, child.publication_key, position),
            )
            expected_contributor = (
                require_digest32(planned[2], field="planned contributor sort_as"),
            )
        if row != expected_contributor:
            raise PublicationCandidateConflictError(
                "catalog contributor differs from independent evaluator"
            )
        return
    if child.kind == _CATALOG_CHILD_SUBJECT:
        position = _position_subkey(child.subkey, field="planned subject position")
        planned = plan._database.execute(
            "SELECT tag_id FROM subjects WHERE publication_key = ? AND position = ?",
            (sqlite3.Binary(child.publication_key), position),
        ).fetchone()
        row = work.connector.fetch_one(
            f"SELECT tag_id FROM {_SUBJECT_TABLE} "
            "WHERE revision = %s AND publication_key = %s AND position = %s",
            (revision, child.publication_key, position),
        )
        if planned is None or row != (planned[0],):
            raise PublicationCandidateConflictError(
                "catalog subject differs from independent evaluator"
            )
        return
    raise PublicationCandidateNotReadyError(
        "catalog artifact validation awaits the typed artifact adapter"
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
        raise PublicationCandidateNotReadyError(
            "publication candidate mutation requires a live SHARED gate"
        )
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(turn.generation, field="publication ingest generation")


def _lock_begin_authority(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    artifact_policy_id: int,
    display_title_policy_id: int,
    locking: bool = True,
) -> _BeginAuthority:
    query = (
        "SELECT a.build_id, a.state, a.completed_at, b.state, b.sealed_at, "
        "bc.channel, sm.snapshot_manifest_sha256, m.gallery_count, "
        "m.file_count, m.byte_count, sva.digest_domain, svi.root_page_sha256, "
        "ap.policy_component_sha256, aps.artifact_algorithm_version, "
        "aps.max_image_short_side, aps.producer_fingerprint_sha256, "
        "ava.digest_domain, avi.root_page_sha256, "
        "dp.display_title_algorithm_version, dp.title_sort_policy_id, "
        "tp.title_sort_algorithm_version, tp.unicode_data_version "
        "FROM catalog_analysis_runs a "
        "JOIN catalog_source_builds b ON b.build_id = a.build_id "
        "JOIN catalog_source_build_channel bc ON bc.build_id = b.build_id "
        f"JOIN {_ANALYSIS_MANIFEST_TABLE} sm ON sm.analysis_id = a.analysis_id "
        "JOIN catalog_source_snapshot_manifest_identity m "
        "ON m.snapshot_manifest_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_canonical_value_allocations sva "
        "ON sva.value_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_canonical_value_identities svi "
        "ON svi.value_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_artifact_policies ap ON ap.artifact_policy_id = %s "
        "JOIN catalog_artifact_policy_semantics aps "
        "ON aps.policy_component_sha256 = ap.policy_component_sha256 "
        "JOIN catalog_canonical_value_allocations ava "
        "ON ava.value_sha256 = ap.policy_component_sha256 "
        "JOIN catalog_canonical_value_identities avi "
        "ON avi.value_sha256 = ap.policy_component_sha256 "
        "JOIN catalog_display_title_policies dp "
        "ON dp.display_title_policy_id = %s "
        "JOIN catalog_title_sort_policy tp "
        "ON tp.title_sort_policy_id = dp.title_sort_policy_id "
        "WHERE a.analysis_id = %s"
    )
    parameters = (artifact_policy_id, display_title_policy_id, analysis_id)
    row = (
        work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("publication-candidate", 0, analysis_id),
            query,
            parameters,
        )
        if locking
        else work.connector.fetch_one(query, parameters)
    )
    if len(row) != 22:
        raise PublicationCandidateNotReadyError(
            "analysis, build, snapshot binding, or registered policy is missing"
        )
    build_id = require_uuid16(row[0], field="candidate source build_id")
    if row[1] != "COMPLETE" or row[2] is None:
        raise PublicationCandidateNotReadyError("candidate analysis is not COMPLETE")
    completed_at = require_int63(row[2], field="candidate analysis completed_at")
    if row[3] != "SEALED" or row[4] is None:
        raise PublicationCandidateNotReadyError("candidate source build is not SEALED")
    sealed_at = require_int63(row[4], field="candidate build sealed_at")
    if sealed_at > completed_at:
        raise PublicationCandidateConflictError(
            "analysis completed before its source build was sealed"
        )
    channel = require_bounded_bytes(
        row[5], field="candidate derived channel", minimum=1, maximum=64
    )
    snapshot = require_digest32(row[6], field="candidate snapshot manifest")
    for field, value in (
        ("snapshot gallery_count", row[7]),
        ("snapshot file_count", row[8]),
        ("snapshot byte_count", row[9]),
    ):
        require_int63(value, field=field)
    if row[10] != _SOURCE_MANIFEST_DOMAIN:
        raise PublicationCandidateConflictError(
            "analysis snapshot uses an unregistered canonical domain"
        )
    require_digest32(row[11], field="snapshot canonical root page")

    policy_component = require_digest32(row[12], field="artifact policy component")
    algorithm_version = require_uint32(row[13], field="artifact algorithm version")
    max_short_side = require_uint32(row[14], field="artifact max short side")
    producer_fingerprint = require_digest32(
        row[15], field="artifact producer fingerprint"
    )
    producer = work.connector.fetch_one(
        "SELECT artifact_algorithm_version, producer_equivalence_class, "
        "writer_id, python_abi, pillow_build, libjpeg_build, zlib_build "
        "FROM catalog_artifact_producer_fingerprints "
        "WHERE producer_fingerprint_sha256 = %s",
        (producer_fingerprint,),
    )
    if (
        len(producer) != 7
        or require_uint32(producer[0], field="artifact producer algorithm version")
        != algorithm_version
    ):
        raise PublicationCandidateNotReadyError(
            "artifact policy producer fingerprint is not registered"
        )
    require_bounded_bytes(
        producer[1],
        field="artifact producer equivalence class",
        minimum=1,
        maximum=128,
    )
    producer_parts = tuple(
        require_bounded_bytes(
            value,
            field=f"artifact producer field {index}",
            minimum=1,
            maximum=128,
        )
        for index, value in enumerate(producer[2:], start=1)
    )
    if identity.artifact_producer_fingerprint_sha256(*producer_parts) != (
        producer_fingerprint
    ):
        raise PublicationCandidateConflictError(
            "artifact producer fingerprint disagrees with its exact build tuple"
        )
    writer = work.connector.fetch_one(
        "SELECT zip_codec_version, compression_method, compression_level, "
        "dos_date, dos_time, unix_mode, general_purpose_flags, create_system, "
        "archive_name_codec_version, artifact_name_codec_version "
        "FROM catalog_artifact_zip_writer_policies "
        "WHERE artifact_algorithm_version = %s",
        (algorithm_version,),
    )
    if writer != (1, 8, 9, 33, 0, 33188, 2048, 3, 1, 1):
        raise PublicationCandidateNotReadyError(
            "artifact policy has no exact registered ZIP writer implementation"
        )
    expected_policy_payload = identity.encode_artifact_policy(
        algorithm_version,
        max_short_side,
        producer_fingerprint,
    )
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            max_short_side,
            producer_fingerprint,
        )
        != policy_component
    ):
        raise PublicationCandidateConflictError(
            "artifact policy component disagrees with its registered tuple"
        )
    if row[16] != _ARTIFACT_POLICY_DOMAIN:
        raise PublicationCandidateConflictError(
            "artifact policy uses an unregistered canonical domain"
        )
    require_digest32(row[17], field="artifact policy canonical root page")
    policy_payload = bytearray()
    try:
        policy_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=policy_component,
            consume_provisional=policy_payload.extend,
        )
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise PublicationCandidateConflictError(
            "artifact policy canonical payload is incomplete or corrupt"
        ) from error
    if (
        policy_receipt.digest_domain != _ARTIFACT_POLICY_DOMAIN
        or policy_receipt.byte_count != len(expected_policy_payload)
        or bytes(policy_payload) != expected_policy_payload
    ):
        raise PublicationCandidateConflictError(
            "artifact policy canonical payload disagrees with its registered tuple"
        )

    display_algorithm = require_uint32(row[18], field="display-title algorithm version")
    require_positive_int63(row[19], field="title-sort policy_id")
    sort_algorithm = require_uint32(row[20], field="title-sort algorithm version")
    unicode_version = require_bounded_bytes(
        row[21], field="title-sort Unicode version", minimum=1, maximum=32
    )
    runtime_unicode = unicodedata.unidata_version.encode("ascii")
    if (
        display_algorithm != _SUPPORTED_DISPLAY_TITLE_ALGORITHM
        or sort_algorithm != _SUPPORTED_TITLE_SORT_ALGORITHM
        or unicode_version != runtime_unicode
    ):
        raise PublicationCandidateNotReadyError(
            "display-title policy has no exact runtime implementation"
        )
    return _BeginAuthority(
        analysis_id,
        build_id,
        completed_at,
        sealed_at,
        channel,
        snapshot,
        artifact_policy_id,
        producer_fingerprint,
        display_title_policy_id,
    )


def _require_exact_analysis_seals(
    work: VNextUnitOfWork,
    authority: _BeginAuthority,
) -> None:
    rows = work.connector.fetch_all(
        "SELECT state_component, sealed_at "
        "FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (authority.analysis_id,),
    )
    if len(rows) != len(ANALYSIS_COMPONENTS):
        raise PublicationCandidateNotReadyError(
            "candidate analysis lacks exactly five component seals"
        )
    components: set[bytes] = set()
    for row in rows:
        if len(row) != 2:
            raise PublicationCandidateConflictError(
                "candidate analysis component seal is malformed"
            )
        component = require_bounded_bytes(
            row[0], field="analysis component", minimum=1, maximum=64
        )
        sealed_at = require_int63(row[1], field="analysis component sealed_at")
        if sealed_at > authority.analysis_completed_at:
            raise PublicationCandidateConflictError(
                "analysis completed before one of its component seals"
            )
        components.add(component)
    if components != ANALYSIS_COMPONENTS:
        raise PublicationCandidateNotReadyError(
            "analysis seals do not match the closed five-component set"
        )


def _lock_generation_mapping(work: VNextUnitOfWork, generation: int) -> bytes | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication-candidate", 1, generation),
        f"SELECT build_id FROM {_BUILD_GENERATION_TABLE} WHERE generation = %s",
        (generation,),
    )
    if not row:
        return None
    if len(row) != 1:
        raise PublicationCandidateConflictError(
            "source-build generation mapping is malformed"
        )
    return require_uuid16(row[0], field="candidate mapped build_id")


def _lock_source_working(
    work: VNextUnitOfWork,
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication-candidate", 2, 1),
        f"SELECT slot, build_id, assigned_at FROM {_SOURCE_WORKING_TABLE} "
        "WHERE slot = %s",
        (1,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCandidateConflictError("source working root is malformed")
    return (
        require_positive_int63(row[0], field="source working slot"),
        require_uuid16(row[1], field="source working build_id"),
        require_int63(row[2], field="source working assigned_at"),
    )


def _lock_catalog_working(
    work: VNextUnitOfWork,
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication-candidate", 3, 1),
        f"SELECT slot, candidate_id, assigned_at FROM {_CATALOG_WORKING_TABLE} "
        "WHERE slot = %s",
        (1,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCandidateConflictError("catalog working root is malformed")
    return (
        require_positive_int63(row[0], field="catalog working slot"),
        require_uuid16(row[1], field="catalog working candidate_id"),
        require_int63(row[2], field="catalog working assigned_at"),
    )


def _lock_candidate(work: VNextUnitOfWork, candidate_id: bytes) -> _CandidateRow:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication-candidate", 4, candidate_id),
        f"SELECT analysis_id, reserved_revision, channel, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, state, created_at, sealed_at "
        f"FROM {_CANDIDATE_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    if len(row) != 9:
        raise PublicationCandidateConflictError(
            "catalog working root points to a missing candidate"
        )
    if row[5] not in {0, 1}:
        raise PublicationCandidateConflictError(
            "candidate artifacts_required is not boolean"
        )
    if row[6] not in {"OPEN", "SEALED", "PUBLISHED", "ABANDONED"}:
        raise PublicationCandidateConflictError("candidate state is not registered")
    if row[8] is not None:
        require_int63(row[8], field="candidate sealed_at")
    return _CandidateRow(
        candidate_id,
        require_uuid16(row[0], field="working candidate analysis_id"),
        require_positive_int63(row[1], field="working candidate reserved_revision"),
        require_bounded_bytes(
            row[2], field="working candidate channel", minimum=1, maximum=64
        ),
        require_positive_int63(row[3], field="working candidate artifact_policy_id"),
        require_positive_int63(
            row[4], field="working candidate display_title_policy_id"
        ),
        bool(row[5]),
        row[6],
        require_int63(row[7], field="working candidate created_at"),
    )


def _lock_candidate_collision(
    work: VNextUnitOfWork,
    candidate_id: bytes,
) -> tuple[Any, ...]:
    return work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication-candidate", 4, candidate_id),
        f"SELECT reserved_revision FROM {_CANDIDATE_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )


def _validate_resume(
    candidate: _CandidateRow,
    *,
    authority: _BeginAuthority,
    artifact_policy_id: int,
    display_title_policy_id: int,
    artifacts_required: bool,
    catalog_working: tuple[int, bytes, int],
    now: int,
) -> None:
    expected = (
        authority.analysis_id,
        authority.channel,
        artifact_policy_id,
        display_title_policy_id,
        artifacts_required,
        "OPEN",
    )
    actual = (
        candidate.analysis_id,
        candidate.channel,
        candidate.artifact_policy_id,
        candidate.display_title_policy_id,
        candidate.artifacts_required,
        candidate.state,
    )
    if actual != expected:
        raise PublicationCandidateConflictError(
            "sole catalog working candidate conflicts with begin request"
        )
    if catalog_working[0] != 1 or catalog_working[1] != candidate.candidate_id:
        raise PublicationCandidateConflictError(
            "catalog working root is not the exact sole candidate"
        )
    if catalog_working[2] != candidate.created_at:
        raise PublicationCandidateConflictError(
            "catalog working assignment disagrees with candidate creation"
        )
    if now < candidate.created_at:
        raise PublicationCandidateNotReadyError(
            "candidate resume timestamp precedes creation"
        )


def _load_build_base_source(
    work: VNextUnitOfWork,
    build_id: bytes,
) -> _Head | None:
    row = work.connector.fetch_one(
        "SELECT base_source_revision, base_source_generation "
        "FROM catalog_source_build_base_source WHERE build_id = %s",
        (build_id,),
    )
    return _base_from_row(row, label="source build base")


def _load_base_source(
    work: VNextUnitOfWork,
    candidate_id: bytes,
) -> _Head | None:
    row = work.connector.fetch_one(
        f"SELECT base_source_revision, base_source_generation "
        f"FROM {_BASE_SOURCE_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    return _base_from_row(row, label="candidate source base")


def _load_base_catalog(
    work: VNextUnitOfWork,
    candidate_id: bytes,
) -> _Head | None:
    row = work.connector.fetch_one(
        f"SELECT base_revision, base_catalog_generation "
        f"FROM {_BASE_CATALOG_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    return _base_from_row(row, label="candidate catalog base")


def _base_from_row(row: tuple[Any, ...], *, label: str) -> _Head | None:
    if not row:
        return None
    if len(row) != 2:
        raise PublicationCandidateConflictError(f"{label} is malformed")
    return _Head(
        require_positive_int63(row[0], field=f"{label} revision"),
        require_positive_int63(row[1], field=f"{label} generation"),
        0,
    )


def _require_build_base_source(
    work: VNextUnitOfWork,
    build_id: bytes,
    candidate_base: _Head | None,
) -> None:
    build_base = _load_build_base_source(work, build_id)
    if _head_coordinate(build_base) != _head_coordinate(candidate_base):
        raise PublicationCandidateConflictError(
            "candidate source base disagrees with its source build"
        )


def _lock_source_head(work: VNextUnitOfWork, channel: bytes) -> _Head | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication-candidate", 0, channel),
        f"SELECT h.source_revision, h.generation, h.advanced_at, r.channel "
        f"FROM {_SOURCE_HEAD_TABLE} h "
        f"JOIN {_SOURCE_REVISION_TABLE} r "
        "ON r.source_revision = h.source_revision WHERE h.channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 4 or row[3] != channel:
        raise PublicationCandidateConflictError(
            "source head descriptor has a conflicting channel"
        )
    return _Head(
        require_positive_int63(row[0], field="source head revision"),
        require_positive_int63(row[1], field="source head generation"),
        require_int63(row[2], field="source head advanced_at"),
    )


def _lock_catalog_head(work: VNextUnitOfWork, channel: bytes) -> _Head | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication-candidate", 1, channel),
        f"SELECT h.revision, h.generation, h.advanced_at, r.revision "
        f"FROM {_PUBLICATION_HEAD_TABLE} h "
        f"JOIN {_CATALOG_REVISION_TABLE} r ON r.revision = h.revision "
        "WHERE h.channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 4 or row[0] != row[3]:
        raise PublicationCandidateConflictError("catalog head descriptor is malformed")
    return _Head(
        require_positive_int63(row[0], field="catalog head revision"),
        require_positive_int63(row[1], field="catalog head generation"),
        require_int63(row[2], field="catalog head advanced_at"),
    )


def _require_exact_head(
    label: str,
    *,
    pinned: _Head | None,
    actual: _Head | None,
) -> None:
    if _head_coordinate(pinned) != _head_coordinate(actual):
        raise PublicationCandidateHeadRaceError(
            f"{label} head differs from the exact candidate base"
        )


def _require_head_timestamps(*heads: _Head | None, now: int) -> None:
    if any(head is not None and head.advanced_at > now for head in heads):
        raise PublicationCandidateNotReadyError(
            "candidate timestamp precedes a pinned head"
        )


def _head_coordinate(value: _Head | None) -> tuple[int, int] | None:
    return None if value is None else (value.revision, value.generation)


def _candidate_result(
    candidate: _CandidateRow,
    *,
    authority: _BeginAuthority,
    base_source: _Head | None,
    base_catalog: _Head | None,
    replayed: bool,
) -> PublicationCandidate:
    return PublicationCandidate(
        candidate.candidate_id,
        candidate.analysis_id,
        authority.build_id,
        candidate.reserved_revision,
        candidate.channel,
        candidate.artifact_policy_id,
        candidate.display_title_policy_id,
        candidate.artifacts_required,
        candidate.state,
        candidate.created_at,
        None if base_source is None else base_source.revision,
        None if base_source is None else base_source.generation,
        None if base_catalog is None else base_catalog.revision,
        None if base_catalog is None else base_catalog.generation,
        replayed,
    )


def _prepare_candidate_batch(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    candidate_id: bytes,
    stage: bytes,
    batch_key: bytes,
    now: int,
) -> tuple[
    _MutationAuthority,
    _Checkpoint,
    bytes,
    PublicationCandidateBatch | None,
]:
    candidate_key = require_uuid16(candidate_id, field="publication batch candidate_id")
    attempt = require_bounded_bytes(
        batch_key,
        field="publication batch_key",
        minimum=1,
        maximum=512,
    )
    timestamp = require_int63(now, field="publication batch committed_at")
    spec = _STAGE_BY_NAME.get(stage)
    if spec is None or stage == b"FINALIZE_ARTIFACTS":
        raise ValueError("publication batch method selected an unsupported stage")

    generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
    _require_exact_stage_registry(work)
    hint = work.connector.fetch_one(
        f"SELECT analysis_id, artifact_policy_id, display_title_policy_id "
        f"FROM {_CANDIDATE_TABLE} WHERE candidate_id = %s",
        (candidate_key,),
    )
    if len(hint) != 3:
        raise PublicationCandidateNotReadyError("publication candidate is missing")
    analysis_id = require_uuid16(hint[0], field="batch candidate analysis_id")
    artifact_policy_id = require_positive_int63(
        hint[1], field="batch candidate artifact_policy_id"
    )
    display_title_policy_id = require_positive_int63(
        hint[2], field="batch candidate display_title_policy_id"
    )
    begin = _lock_begin_authority(
        work,
        analysis_id=analysis_id,
        artifact_policy_id=artifact_policy_id,
        display_title_policy_id=display_title_policy_id,
    )
    _require_exact_analysis_seals(work, begin)
    mapping = _lock_generation_mapping(work, generation)
    source_working = _lock_source_working(work)
    catalog_working = _lock_catalog_working(work)
    candidate = _lock_candidate(work, candidate_key)
    if (
        candidate.analysis_id != begin.analysis_id
        or candidate.channel != begin.channel
        or candidate.artifact_policy_id != begin.artifact_policy_id
        or candidate.display_title_policy_id != begin.display_title_policy_id
    ):
        raise PublicationCandidateConflictError(
            "publication candidate differs from its sealed analysis authority"
        )
    if candidate.state != "OPEN":
        raise PublicationCandidateNotReadyError(
            "publication projection batches require an OPEN candidate"
        )
    if mapping != begin.build_id:
        raise PublicationCandidateNotReadyError(
            "live ingest generation is not mapped to the candidate build"
        )
    if source_working is None or source_working[1] != begin.build_id:
        raise PublicationCandidateNotReadyError(
            "candidate build no longer owns the source working root"
        )
    if catalog_working is None or catalog_working[1] != candidate.candidate_id:
        raise PublicationCandidateNotReadyError(
            "candidate no longer owns the catalog working root"
        )
    if timestamp < max(
        candidate.created_at,
        source_working[2],
        catalog_working[2],
        begin.analysis_completed_at,
        begin.build_sealed_at,
    ):
        raise PublicationCandidateNotReadyError(
            "publication batch timestamp precedes a sealed prerequisite"
        )

    base_source = _load_base_source(work, candidate_key)
    base_catalog = _load_base_catalog(work, candidate_key)
    _require_build_base_source(work, begin.build_id, base_source)
    _require_exact_candidate_checkpoints(
        work,
        candidate_key,
        created_at=candidate.created_at,
        now=timestamp,
    )
    checkpoint = _lock_publication_checkpoint(work, candidate_key, spec)
    _require_stage_prerequisite(work, candidate_key, spec)
    source_head = _lock_source_head(work, begin.channel)
    catalog_head = _lock_catalog_head(work, begin.channel)
    _require_exact_head("source", pinned=base_source, actual=source_head)
    _require_exact_head("catalog", pinned=base_catalog, actual=catalog_head)
    _require_head_timestamps(source_head, catalog_head, now=timestamp)
    if timestamp < checkpoint.updated_at:
        raise PublicationCandidateNotReadyError(
            "publication batch timestamp precedes its checkpoint"
        )

    authority = _MutationAuthority(candidate, begin, base_source, base_catalog)
    stored = _load_candidate_batch(work, candidate_key, stage, attempt)
    if stored is not None:
        replay = _candidate_batch_from_row(
            candidate_key,
            stage,
            attempt,
            stored,
            replayed=True,
        )
        if replay.committed_at < candidate.created_at:
            raise PublicationCandidateConflictError(
                "publication batch receipt predates its candidate"
            )
        _validate_batch_against_checkpoint(replay, checkpoint, now=timestamp)
        return authority, checkpoint, attempt, replay
    if checkpoint.state == _CHECKPOINT_COMPLETE:
        raise PublicationCandidateNotReadyError(
            "publication stage is already COMPLETE for another batch key"
        )
    if checkpoint.state != _CHECKPOINT_OPEN:
        raise PublicationCandidateConflictError(
            "publication checkpoint state is not registered"
        )
    if checkpoint.generation == INT63_MAX:
        raise PublicationCandidateNotReadyError(
            "publication checkpoint generation is exhausted"
        )
    return authority, checkpoint, attempt, None


def _lock_publication_checkpoint(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    spec: _StageSpec,
) -> _Checkpoint:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication-candidate-checkpoint", candidate_id, spec.order),
        f"SELECT generation, cursor, processed_count, state, updated_at "
        f"FROM {_PUBLICATION_CHECKPOINT_TABLE} "
        "WHERE candidate_id = %s AND stage = %s",
        (candidate_id, spec.name),
    )
    if len(row) != 5:
        raise PublicationCandidateConflictError(
            "publication checkpoint is missing or malformed"
        )
    checkpoint = _Checkpoint(
        require_positive_int63(row[0], field="publication checkpoint generation"),
        require_bounded_bytes(
            row[1], field="publication checkpoint cursor", maximum=2048
        ),
        require_int63(row[2], field="publication checkpoint processed_count"),
        row[3],
        require_int63(row[4], field="publication checkpoint updated_at"),
    )
    _validate_stage_cursor(spec.cursor_codec, checkpoint.cursor)
    if checkpoint.state not in {_CHECKPOINT_OPEN, _CHECKPOINT_COMPLETE}:
        raise PublicationCandidateConflictError(
            "publication checkpoint has an unregistered state"
        )
    return checkpoint


def _require_stage_prerequisite(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    spec: _StageSpec,
) -> None:
    index = _STAGES.index(spec)
    if index == 0:
        return
    predecessor = _STAGES[index - 1]
    row = work.connector.fetch_one(
        f"SELECT state FROM {_PUBLICATION_CHECKPOINT_TABLE} "
        "WHERE candidate_id = %s AND stage = %s",
        (candidate_id, predecessor.name),
    )
    if row != (_CHECKPOINT_COMPLETE,):
        raise PublicationCandidateNotReadyError(
            f"publication prerequisite {predecessor.name!r} is incomplete"
        )


def _derive_selection_page(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    *,
    after: int,
) -> tuple[tuple[int, int], ...]:
    rows = work.connector.fetch_all(
        "SELECT member.gallery_id, winner.gid "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_metadata AS metadata "
        "ON metadata.gallery_id = member.gallery_id "
        "AND metadata.observation_id = member.observation_id "
        "JOIN catalog_analysis_gid_winner_resolved AS winner "
        "ON winner.analysis_id = %s AND winner.gid = metadata.gid "
        "AND winner.winner_gallery_id = member.gallery_id "
        "LEFT JOIN catalog_analysis_content_owner_candidate_resolved AS content "
        "ON content.analysis_id = %s AND content.gallery_id = member.gallery_id "
        "LEFT JOIN catalog_analysis_content_owner_resolved AS owner "
        "ON owner.analysis_id = %s "
        "AND owner.content_sha256 = content.content_sha256 "
        "WHERE member.build_id = %s AND member.gallery_id > %s "
        "AND (content.gallery_id IS NULL "
        "OR owner.owner_gallery_id = member.gallery_id) "
        "ORDER BY member.gallery_id LIMIT 128",
        (
            authority.begin.analysis_id,
            authority.begin.analysis_id,
            authority.begin.analysis_id,
            authority.begin.build_id,
            after,
        ),
    )
    result: list[tuple[int, int]] = []
    for row in rows:
        if len(row) != 2:
            raise PublicationCandidateConflictError(
                "selection evaluator returned a malformed row"
            )
        result.append(
            (
                require_positive_int63(row[0], field="selected gallery_id"),
                require_positive_int63(row[1], field="selected gid"),
            )
        )
    if any(left[0] >= right[0] for left, right in zip(result, result[1:])):
        raise PublicationCandidateConflictError(
            "selection evaluator did not return strict gallery order"
        )
    return tuple(result)


def _evaluate_selection_page(
    work: VNextUnitOfWork,
    authority: _MutationAuthority,
    *,
    after: int,
) -> tuple[tuple[int, int], ...]:
    """Independent correlated evaluator for VALIDATE_SELECTION."""

    rows = work.connector.fetch_all(
        "SELECT member.gallery_id, metadata.gid "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_metadata AS metadata "
        "ON metadata.gallery_id = member.gallery_id "
        "AND metadata.observation_id = member.observation_id "
        "WHERE member.build_id = %s AND member.gallery_id > %s "
        "AND EXISTS (SELECT 1 FROM catalog_analysis_gid_winner_resolved AS winner "
        "WHERE winner.analysis_id = %s AND winner.gid = metadata.gid "
        "AND winner.winner_gallery_id = member.gallery_id) "
        "AND (NOT EXISTS (SELECT 1 "
        "FROM catalog_analysis_content_owner_candidate_resolved AS content "
        "WHERE content.analysis_id = %s "
        "AND content.gallery_id = member.gallery_id) "
        "OR EXISTS (SELECT 1 "
        "FROM catalog_analysis_content_owner_candidate_resolved AS content "
        "JOIN catalog_analysis_content_owner_resolved AS owner "
        "ON owner.analysis_id = content.analysis_id "
        "AND owner.content_sha256 = content.content_sha256 "
        "WHERE content.analysis_id = %s "
        "AND content.gallery_id = member.gallery_id "
        "AND owner.owner_gallery_id = member.gallery_id)) "
        "ORDER BY member.gallery_id LIMIT 128",
        (
            authority.begin.build_id,
            after,
            authority.begin.analysis_id,
            authority.begin.analysis_id,
            authority.begin.analysis_id,
        ),
    )
    result: list[tuple[int, int]] = []
    for row in rows:
        if len(row) != 2:
            raise PublicationCandidateConflictError(
                "independent selection evaluator returned a malformed row"
            )
        result.append(
            (
                require_positive_int63(row[0], field="validation selected gallery_id"),
                require_positive_int63(row[1], field="validation selected gid"),
            )
        )
    if any(left[0] >= right[0] for left, right in zip(result, result[1:])):
        raise PublicationCandidateConflictError(
            "independent selection evaluator did not return strict gallery order"
        )
    return tuple(result)


def _ensure_publication_identity(
    work: VNextUnitOfWork,
    *,
    publication_key: bytes,
    publication_id: bytes,
    gid: int,
) -> None:
    artifact_name = identity.artifact_name(gid)
    rows = work.connector.fetch_all(
        f"SELECT publication_key, publication_id, gid, artifact_name "
        f"FROM {_PUBLICATION_IDENTITY_TABLE} "
        "WHERE publication_key = %s OR publication_id = %s OR gid = %s "
        "OR artifact_name = %s LIMIT 2",
        (publication_key, publication_id, gid, artifact_name),
    )
    expected = (publication_key, publication_id, gid, artifact_name)
    if rows:
        if tuple(rows) != (expected,):
            raise PublicationCandidateConflictError(
                "publication identity collides with a different natural tuple"
            )
        return
    work.connector.execute(
        f"INSERT INTO {_PUBLICATION_IDENTITY_TABLE} "
        "(publication_key, publication_id, gid, artifact_name) "
        "VALUES (%s, %s, %s, %s)",
        expected,
    )


def _selection_row(row: tuple[Any, ...]) -> tuple[int, bytes]:
    if len(row) != 2:
        raise PublicationCandidateConflictError(
            "materialized publication selection is malformed"
        )
    return (
        require_positive_int63(row[0], field="materialized selected gallery_id"),
        require_digest32(row[1], field="materialized publication_key"),
    )


def _decode_gallery_cursor(cursor: bytes) -> int:
    _validate_stage_cursor(_CURSOR_GALLERY, cursor)
    return 0 if not cursor else int.from_bytes(cursor, "big")


def _commit_candidate_batch(
    work: VNextUnitOfWork,
    *,
    authority: _MutationAuthority,
    checkpoint: _Checkpoint,
    stage: bytes,
    batch_key: bytes,
    next_cursor: bytes,
    row_count: int,
    terminal: bool,
    now: int,
) -> PublicationCandidateBatch:
    spec = _STAGE_BY_NAME[stage]
    _validate_stage_cursor(spec.cursor_codec, next_cursor)
    count = require_int63(row_count, field="publication batch row_count")
    if terminal != (count == 0):
        raise PublicationCandidateConflictError(
            "publication terminal state does not match an empty derived page"
        )
    if checkpoint.processed_count > INT63_MAX - count:
        raise PublicationCandidateNotReadyError(
            "publication checkpoint processed_count is exhausted"
        )
    next_count = checkpoint.processed_count + count
    next_state = _CHECKPOINT_COMPLETE if terminal else _CHECKPOINT_OPEN
    if terminal:
        if next_cursor != checkpoint.cursor:
            raise PublicationCandidateConflictError(
                "terminal publication batch changed its cursor"
            )
    elif spec.cursor_codec == _CURSOR_GALLERY:
        if _decode_gallery_cursor(next_cursor) <= _decode_gallery_cursor(
            checkpoint.cursor
        ):
            raise PublicationCandidateConflictError(
                "publication gallery cursor did not advance"
            )
    elif next_cursor <= checkpoint.cursor:
        raise PublicationCandidateConflictError(
            "publication byte cursor did not advance"
        )
    successor = checkpoint.generation + 1
    timestamp = require_int63(now, field="publication batch committed_at")
    work.connector.execute(
        f"INSERT INTO {_PUBLICATION_BATCH_RECEIPT_TABLE} "
        "(candidate_id, stage, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            authority.candidate.candidate_id,
            stage,
            batch_key,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            next_cursor,
            next_count,
            next_state,
            count,
            int(terminal),
            successor,
            timestamp,
        ),
    )
    work.compare_and_swap(
        f"UPDATE {_PUBLICATION_CHECKPOINT_TABLE} SET generation = %s, "
        "cursor = %s, processed_count = %s, state = %s, updated_at = %s "
        "WHERE candidate_id = %s AND stage = %s AND generation = %s "
        "AND cursor = %s AND processed_count = %s AND state = %s "
        "AND updated_at = %s",
        (
            successor,
            next_cursor,
            next_count,
            next_state,
            timestamp,
            authority.candidate.candidate_id,
            stage,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            checkpoint.state,
            checkpoint.updated_at,
        ),
        authority=f"publication candidate stage {stage!r}",
    )
    return PublicationCandidateBatch(
        authority.candidate.candidate_id,
        stage,
        batch_key,
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        next_cursor,
        next_count,
        next_state,
        count,
        terminal,
        successor,
        timestamp,
        False,
    )


def _load_candidate_batch(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    stage: bytes,
    batch_key: bytes,
) -> tuple[Any, ...] | None:
    row = work.connector.fetch_one(
        f"SELECT start_generation, start_cursor, start_processed_count, "
        "next_cursor, next_processed_count, next_state, row_count, terminal, "
        f"committed_generation, committed_at FROM {_PUBLICATION_BATCH_RECEIPT_TABLE} "
        "WHERE candidate_id = %s AND stage = %s AND batch_key = %s",
        (candidate_id, stage, batch_key),
    )
    return None if not row else row


def _candidate_batch_from_row(
    candidate_id: bytes,
    stage: bytes,
    batch_key: bytes,
    row: tuple[Any, ...],
    *,
    replayed: bool,
) -> PublicationCandidateBatch:
    if len(row) != 10:
        raise PublicationCandidateConflictError(
            "publication batch receipt is malformed"
        )
    terminal = require_int63(row[7], field="publication batch terminal")
    if terminal not in {0, 1}:
        raise PublicationCandidateConflictError(
            "publication batch terminal flag is not boolean"
        )
    try:
        result = PublicationCandidateBatch(
            candidate_id,
            stage,
            batch_key,
            require_positive_int63(row[0], field="publication batch start_generation"),
            require_bounded_bytes(
                row[1], field="publication batch start_cursor", maximum=2048
            ),
            require_int63(row[2], field="publication batch start_processed_count"),
            require_bounded_bytes(
                row[3], field="publication batch next_cursor", maximum=2048
            ),
            require_int63(row[4], field="publication batch next_processed_count"),
            row[5],
            require_int63(row[6], field="publication batch row_count"),
            bool(terminal),
            require_positive_int63(
                row[8], field="publication batch committed_generation"
            ),
            require_int63(row[9], field="publication batch committed_at"),
            replayed,
        )
    except (TypeError, ValueError) as error:
        raise PublicationCandidateConflictError(
            "publication batch receipt has invalid domain values"
        ) from error
    if result.committed_generation != result.start_generation + 1:
        raise PublicationCandidateConflictError(
            "publication batch generation is not a single successor"
        )
    if result.next_processed_count != (result.start_processed_count + result.row_count):
        raise PublicationCandidateConflictError(
            "publication batch processed_count is not monotone"
        )
    if result.terminal:
        valid_transition = (
            result.row_count == 0
            and result.next_cursor == result.start_cursor
            and result.next_state == _CHECKPOINT_COMPLETE
        )
    else:
        valid_transition = (
            result.row_count > 0 and result.next_state == _CHECKPOINT_OPEN
        )
    if not valid_transition:
        raise PublicationCandidateConflictError(
            "publication batch receipt is not an exact terminal/nonterminal transition"
        )
    spec = _STAGE_BY_NAME[stage]
    if (
        not result.terminal
        and spec.cursor_codec == _CURSOR_GALLERY
        and _decode_gallery_cursor(result.next_cursor)
        <= _decode_gallery_cursor(result.start_cursor)
    ):
        raise PublicationCandidateConflictError(
            "stored publication gallery cursor did not advance"
        )
    if (
        not result.terminal
        and spec.cursor_codec != _CURSOR_GALLERY
        and result.next_cursor <= result.start_cursor
    ):
        raise PublicationCandidateConflictError(
            "stored publication byte cursor did not advance"
        )
    return result


def _validate_batch_against_checkpoint(
    batch: PublicationCandidateBatch,
    checkpoint: _Checkpoint,
    *,
    now: int,
) -> None:
    if not batch.committed_at <= now:
        raise PublicationCandidateConflictError(
            "publication batch receipt is from the future"
        )
    if batch.committed_generation > checkpoint.generation:
        raise PublicationCandidateConflictError(
            "publication batch receipt is ahead of its checkpoint"
        )
    if batch.committed_generation == checkpoint.generation and (
        batch.next_cursor,
        batch.next_processed_count,
        batch.next_state,
        batch.committed_at,
    ) != (
        checkpoint.cursor,
        checkpoint.processed_count,
        checkpoint.state,
        checkpoint.updated_at,
    ):
        raise PublicationCandidateConflictError(
            "latest publication batch receipt disagrees with its checkpoint"
        )


def _validate_optional_base_pair(
    revision: int | None,
    generation: int | None,
    *,
    label: str,
) -> None:
    if (revision is None) != (generation is None):
        raise ValueError(f"publication candidate {label} base pair is partial")
    if revision is not None:
        require_positive_int63(
            revision,
            field=f"publication candidate {label} base revision",
        )
        assert generation is not None
        require_positive_int63(
            generation,
            field=f"publication candidate {label} base generation",
        )


def _require_exact_stage_registry(work: VNextUnitOfWork) -> None:
    """Require the generated provider's closed seventeen-stage registry."""

    if not work.connector.check_table_exists(_PUBLICATION_STAGE_TABLE):
        raise PublicationCandidateStageRegistryUnavailableError(
            "generated schema has no closed publication-stage registry"
        )
    rows = work.connector.fetch_all(
        f"SELECT stage, stage_order, cursor_codec FROM {_PUBLICATION_STAGE_TABLE} "
        "ORDER BY stage_order LIMIT 18"
    )
    actual: list[tuple[bytes, bytes, bytes]] = []
    try:
        for row in rows:
            if len(row) != 3:
                raise ValueError("malformed publication-stage row")
            actual.append(
                (
                    require_bounded_bytes(
                        row[0],
                        field="publication stage",
                        minimum=1,
                        maximum=64,
                    ),
                    require_bounded_bytes(
                        row[1],
                        field="publication stage_order",
                        minimum=2,
                        maximum=2,
                    ),
                    require_bounded_bytes(
                        row[2],
                        field="publication cursor_codec",
                        minimum=1,
                        maximum=64,
                    ),
                )
            )
    except (TypeError, ValueError) as error:
        raise PublicationCandidateConflictError(
            "publication-stage registry contains invalid domain values"
        ) from error
    expected = tuple((stage.name, stage.order, stage.cursor_codec) for stage in _STAGES)
    if tuple(actual) != expected:
        raise PublicationCandidateConflictError(
            "publication-stage registry differs from the closed runtime map"
        )


def _require_exact_candidate_checkpoints(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    *,
    created_at: int,
    now: int,
) -> None:
    rows = work.connector.fetch_all(
        f"SELECT stage, generation, cursor, processed_count, state, updated_at "
        f"FROM {_PUBLICATION_CHECKPOINT_TABLE} WHERE candidate_id = %s "
        "ORDER BY stage LIMIT 18",
        (candidate_id,),
    )
    by_stage: dict[bytes, tuple[Any, ...]] = {}
    for row in rows:
        if len(row) != 6:
            raise PublicationCandidateConflictError(
                "publication checkpoint is malformed"
            )
        stage = require_bounded_bytes(
            row[0], field="publication checkpoint stage", minimum=1, maximum=64
        )
        if stage in by_stage:
            raise PublicationCandidateConflictError(
                "publication candidate has duplicate checkpoints"
            )
        by_stage[stage] = row
    if set(by_stage) != set(_STAGE_BY_NAME):
        raise PublicationCandidateConflictError(
            "publication candidate does not have the exact seventeen checkpoints"
        )
    for spec in _STAGES:
        row = by_stage[spec.name]
        require_positive_int63(row[1], field="publication checkpoint generation")
        cursor = require_bounded_bytes(
            row[2], field="publication checkpoint cursor", maximum=2048
        )
        _validate_stage_cursor(spec.cursor_codec, cursor)
        require_int63(row[3], field="publication checkpoint processed_count")
        if row[4] not in {_CHECKPOINT_OPEN, _CHECKPOINT_COMPLETE}:
            raise PublicationCandidateConflictError(
                "publication checkpoint has an unregistered state"
            )
        updated_at = require_int63(row[5], field="publication checkpoint updated_at")
        if not created_at <= updated_at <= now:
            raise PublicationCandidateConflictError(
                "publication checkpoint timestamp is outside its candidate lifetime"
            )


def _initialize_candidate_checkpoints(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    *,
    now: int,
) -> None:
    affected = work.connector.execute_affected(
        f"INSERT INTO {_PUBLICATION_CHECKPOINT_TABLE} "
        "(candidate_id, stage, generation, cursor, processed_count, state, "
        "updated_at) SELECT %s, stage, %s, %s, %s, %s, %s "
        f"FROM {_PUBLICATION_STAGE_TABLE} ORDER BY stage_order",
        (candidate_id, 1, b"", 0, _CHECKPOINT_OPEN, now),
    )
    if affected != len(_STAGES):
        raise PublicationCandidateConflictError(
            "publication checkpoint initialization did not materialize all stages"
        )


def _validate_stage_cursor(codec: bytes, cursor: bytes) -> None:
    """Validate one server-owned cursor selected by the closed stage map."""

    exact = require_bounded_bytes(
        cursor,
        field="publication stage cursor",
        maximum=2048,
    )
    if not exact:
        return
    if codec == _CURSOR_GALLERY:
        if len(exact) != 8 or not 0 < int.from_bytes(exact, "big") <= INT63_MAX:
            raise PublicationCandidateConflictError(
                "publication gallery cursor is not a positive u64be int63"
            )
        return
    if codec == _CURSOR_PUBLICATION_KEY:
        if len(exact) != 32:
            raise PublicationCandidateConflictError(
                "publication-key cursor is not raw32"
            )
        return
    if codec != _CURSOR_CATALOG_CHILD:
        raise PublicationCandidateConflictError(
            "publication cursor selected an unregistered codec"
        )
    if len(exact) < 36 or exact[0] != 1 or exact[1] > 7:
        raise PublicationCandidateConflictError(
            "catalog-child cursor has an invalid header"
        )
    kind = exact[1]
    subkey_length = int.from_bytes(exact[34:36], "big")
    subkey = exact[36:]
    if subkey_length != len(subkey):
        raise PublicationCandidateConflictError(
            "catalog-child cursor subkey length is not exact"
        )
    if kind in {0, 2, 3}:
        valid = not subkey
    elif kind in {1, 4, 5, 6}:
        valid = len(subkey) == 8 and int.from_bytes(subkey, "big") <= INT63_MAX
    else:
        valid = (
            3 <= len(subkey) <= 130
            and int.from_bytes(subkey[:2], "big") == len(subkey) - 2
        )
    if not valid:
        raise PublicationCandidateConflictError(
            "catalog-child cursor subkey disagrees with its child kind"
        )


def _new_candidate_id() -> bytes:
    return secrets.token_bytes(16)
