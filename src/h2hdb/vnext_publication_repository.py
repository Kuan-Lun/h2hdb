"""Atomic source/catalog publication and durable recovery for vNext.

The public mutation accepts only a candidate identity plus the two live writer
capabilities.  Revisions, projection counts, the canonical source manifest,
the operational preparation, policies, head generations, and receipt identity
are all loaded or allocated by the database-side protocol.

The high-cardinality catalog projection and protected artifacts are prepared by
bounded upstream writers while their reserved revision is not referenced by a
head.  This module deliberately performs no child ``COUNT``/``SUM`` and never
bulk-updates prepared artifacts in the pointer transaction.  An immutable
candidate projection seal is the O(1) authority for those already-validated
facts.  Filesystem finalization remains a separately receipted bounded phase.
"""

from __future__ import annotations

__all__ = [
    "PublicationCommitReceipt",
    "PublicationConflictError",
    "PublicationCorruptionError",
    "PublicationFinalizeBatch",
    "PublicationHeadRaceError",
    "PublicationNotReadyError",
    "PublicationRepository",
    "PublicationRepositoryError",
    "PublicationSchemaNotReadyError",
    "PreparedArtifactRelease",
]

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from .vnext_allocator_repository import RevisionStream, VNextAllocatorRepository
from .vnext_analysis_repository import ANALYSIS_COMPONENTS
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
_BASE_CATALOG_TABLE = "catalog_publication_candidate_base_catalog"
_BASE_SOURCE_TABLE = "catalog_publication_candidate_base_sources"
_PROJECTION_SEAL_TABLE = "catalog_publication_candidate_projection_seal"
_ANALYSIS_MANIFEST_TABLE = "catalog_analysis_snapshot_manifest"
_REVISION_TABLE = "catalog_revisions"
_RECEIPT_TABLE = "catalog_publication_receipts"
_PUBLICATION_HEAD_TABLE = "catalog_publication_heads"
_SOURCE_REVISION_TABLE = "catalog_source_revisions"
_SOURCE_PROVENANCE_TABLE = "catalog_source_revision_provenance"
_SOURCE_HEAD_TABLE = "catalog_source_heads"

_BUILD_GENERATION_TABLE = "operational_source_build_generations"
_SOURCE_WORKING_TABLE = "operational_source_working_builds"
_CATALOG_WORKING_TABLE = "operational_catalog_working_candidates"
_CANDIDATE_PREPARATION_TABLE = "operational_publication_candidate_preparations"
_PREPARATION_TABLE = "operational_operational_preparations"
_EFFECT_SEAL_TABLE = "operational_operational_preparation_effect_seals"
_ACTIVATION_TABLE = "operational_operational_activations"
_DELETION_HEAD_TABLE = "operational_deletion_request_generation_heads"

_SOURCE_MANIFEST_DOMAIN = b"source_snapshot_manifest_v1"
_FINALIZE_STAGE = b"FINALIZE_ARTIFACTS"
_FINALIZE_BATCH_ROWS = 128
_REQUIRED_SCHEMA_TABLES = (
    _ANALYSIS_MANIFEST_TABLE,
    _PROJECTION_SEAL_TABLE,
    _CANDIDATE_PREPARATION_TABLE,
)


class PublicationRepositoryError(RuntimeError):
    """Base class for vNext publication failures."""


class PublicationSchemaNotReadyError(PublicationRepositoryError):
    """The generated schema lacks an O(1) authority required for publication."""


class PublicationNotReadyError(PublicationRepositoryError):
    """The candidate or one of its server-owned prerequisites is incomplete."""


class PublicationConflictError(PublicationRepositoryError):
    """An immutable recovery identity names different exact publication facts."""


class PublicationCorruptionError(PublicationRepositoryError):
    """Persisted normalized facts disagree with the publication contract."""


class PublicationHeadRaceError(PublicationNotReadyError):
    """A candidate-pinned source, catalog, or deletion head has advanced."""


@dataclass(frozen=True, slots=True)
class PublicationCommitReceipt:
    candidate_id: bytes
    receipt_id: bytes
    revision: int
    source_revision: int
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    publication_count: int
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    state: str
    committed_at: int
    finalized_at: int | None
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication candidate_id")
        require_uuid16(self.receipt_id, field="publication receipt_id")
        require_positive_int63(self.revision, field="catalog revision")
        require_positive_int63(self.source_revision, field="source revision")
        require_positive_int63(
            self.reserved_revision, field="reserved catalog revision"
        )
        require_bounded_bytes(
            self.channel,
            field="publication channel",
            minimum=1,
            maximum=64,
        )
        require_positive_int63(
            self.artifact_policy_id,
            field="publication artifact_policy_id",
        )
        require_positive_int63(
            self.display_title_policy_id,
            field="publication display_title_policy_id",
        )
        for field, value in (
            ("publication_count", self.publication_count),
            ("new_galleries", self.new_galleries),
            ("changed_galleries", self.changed_galleries),
            ("removed_galleries", self.removed_galleries),
            ("duplicate_losers", self.duplicate_losers),
        ):
            require_int63(value, field=f"publication {field}")
        if self.state not in {"DB_COMMITTED", "PROJECTION_FINALIZED"}:
            raise ValueError("publication receipt state is not registered")
        require_int63(self.committed_at, field="publication committed_at")
        if self.finalized_at is not None:
            require_int63(self.finalized_at, field="publication finalized_at")
            if self.finalized_at < self.committed_at:
                raise ValueError("publication finalized_at precedes committed_at")
        if (self.state == "DB_COMMITTED") != (self.finalized_at is None):
            raise ValueError("publication state/finalized_at pair is inconsistent")
        if not isinstance(self.replayed, bool):
            raise TypeError("publication replayed must be bool")


@dataclass(frozen=True, slots=True)
class PreparedArtifactRelease:
    """One exact idempotent protection release derived from the database."""

    publication_key: bytes
    artifact_name: bytes
    artifact_sha256: bytes
    protection_token: bytes

    def __post_init__(self) -> None:
        require_digest32(self.publication_key, field="finalize publication_key")
        require_bounded_bytes(
            self.artifact_name,
            field="finalize artifact_name",
            minimum=1,
            maximum=255,
        )
        require_digest32(self.artifact_sha256, field="finalize artifact_sha256")
        require_bounded_bytes(
            self.protection_token,
            field="finalize protection_token",
            maximum=512,
        )


@dataclass(frozen=True, slots=True)
class PublicationFinalizeBatch:
    candidate_id: bytes
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
    receipt_state: str
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="finalize candidate_id")
        require_bounded_bytes(
            self.batch_key,
            field="finalize batch_key",
            minimum=1,
            maximum=512,
        )
        require_positive_int63(self.start_generation, field="finalize start_generation")
        for cursor_field, cursor_value in (
            ("start_cursor", self.start_cursor),
            ("next_cursor", self.next_cursor),
        ):
            require_bounded_bytes(
                cursor_value, field=f"finalize {cursor_field}", maximum=32
            )
            if len(cursor_value) not in {0, 32}:
                raise ValueError(f"finalize {cursor_field} must be empty or raw32")
        for count_field, count_value in (
            ("start_processed_count", self.start_processed_count),
            ("next_processed_count", self.next_processed_count),
            ("row_count", self.row_count),
        ):
            require_int63(count_value, field=f"finalize {count_field}")
        if self.next_state not in {"OPEN", "COMPLETE"}:
            raise ValueError("finalize next_state is not registered")
        require_positive_int63(
            self.committed_generation,
            field="finalize committed_generation",
        )
        require_int63(self.committed_at, field="finalize committed_at")
        if self.receipt_state not in {"DB_COMMITTED", "PROJECTION_FINALIZED"}:
            raise ValueError("finalize receipt_state is not registered")
        if not isinstance(self.terminal, bool) or not isinstance(self.replayed, bool):
            raise TypeError("finalize terminal/replayed must be bool")


@dataclass(frozen=True, slots=True)
class _Candidate:
    candidate_id: bytes
    analysis_id: bytes
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    state: str
    created_at: int
    sealed_at: int
    build_id: bytes
    input_manifest_sha256: bytes
    analysis_completed_at: int
    build_sealed_at: int
    snapshot_manifest_sha256: bytes


@dataclass(frozen=True, slots=True)
class _ProjectionSeal:
    publication_count: int
    artifact_input_count: int
    prepared_artifact_count: int
    create_count: int
    rebuild_count: int
    delete_count: int
    unchanged_count: int
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    sealed_at: int


@dataclass(frozen=True, slots=True)
class _Preparation:
    preparation_id: bytes
    build_id: bytes
    deletion_generation: int
    policy_id: int
    prepared_at: int
    completed_at: int
    bound_at: int


@dataclass(frozen=True, slots=True)
class _BaseHead:
    revision: int
    generation: int


@dataclass(frozen=True, slots=True)
class _LockedHead:
    revision: int
    generation: int
    advanced_at: int


@dataclass(frozen=True, slots=True)
class _FinalizeCheckpoint:
    generation: int
    cursor: bytes
    processed_count: int
    state: str
    updated_at: int
    exists: bool


class PublicationRepository:
    """Publish a sealed candidate and recover its immutable receipt."""

    @staticmethod
    def commit(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        now: int,
    ) -> PublicationCommitReceipt:
        """Atomically advance source/catalog heads for one sealed candidate.

        The caller owns the surrounding transaction.  Any exception therefore
        leaves allocator, revision, activation, receipt, candidate, working
        roots, and both heads unchanged when that transaction is rolled back.
        """

        candidate_key = require_uuid16(candidate_id, field="publication candidate_id")
        timestamp = require_int63(now, field="publication committed_at")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_schema_authorities(work)

        candidate = _lock_candidate(work, candidate_key)
        mapping = _lock_generation_mapping(work, generation)
        source_working = _lock_source_working(work, candidate.build_id)
        catalog_working = _lock_catalog_working(work, candidate_key)
        preparation = _lock_preparation(work, candidate_key)

        if preparation.build_id != candidate.build_id:
            raise PublicationCorruptionError(
                "candidate preparation belongs to a different source build"
            )
        if preparation.bound_at > candidate.sealed_at:
            raise PublicationCorruptionError(
                "candidate was sealed before its operational preparation binding"
            )

        _require_exact_analysis_seals(work, candidate)
        projection = _lock_projection_seal(work, candidate_key)
        if projection.sealed_at > candidate.sealed_at:
            raise PublicationCorruptionError(
                "candidate was sealed before its immutable projection seal"
            )
        _validate_artifact_counts(candidate, projection)
        _lock_and_validate_effect_seal(work, preparation, timestamp=timestamp)
        existing_row = _lock_receipt_by_revision(work, candidate.reserved_revision)

        base_source = _load_base_source(work, candidate_key)
        base_catalog = _load_base_catalog(work, candidate_key)
        _require_build_base_source(work, candidate.build_id, base_source)
        _validate_catalog_revision(work, candidate, projection, timestamp=timestamp)

        if existing_row:
            receipt = _receipt_from_row(candidate_key, existing_row, replayed=True)
            _validate_replay(
                work,
                candidate=candidate,
                preparation=preparation,
                projection=projection,
                base_source=base_source,
                base_catalog=base_catalog,
                receipt=receipt,
            )
            return receipt

        if mapping != candidate.build_id:
            raise PublicationNotReadyError(
                "the live ingest generation is not mapped to the candidate build"
            )
        if candidate.state != "SEALED":
            if candidate.state == "PUBLISHED":
                raise PublicationCorruptionError(
                    "a PUBLISHED candidate has no durable publication receipt"
                )
            raise PublicationNotReadyError("publication requires a SEALED candidate")
        if source_working is None or source_working[1] != candidate.build_id:
            raise PublicationNotReadyError(
                "the candidate build does not own the source working slot"
            )
        if catalog_working is None or catalog_working[1] != candidate.candidate_id:
            raise PublicationNotReadyError(
                "the candidate does not own the catalog working slot"
            )
        if timestamp < max(
            candidate.sealed_at,
            candidate.analysis_completed_at,
            candidate.build_sealed_at,
            projection.sealed_at,
            preparation.completed_at,
            preparation.bound_at,
        ):
            raise PublicationNotReadyError(
                "publication timestamp precedes a sealed prerequisite"
            )

        receipt_id = require_uuid16(_new_receipt_id(), field="generated receipt_id")
        collision = _lock_receipt_id_collision(work, receipt_id)
        if collision:
            raise PublicationConflictError(
                "generated publication receipt identity already exists"
            )

        source_revision = VNextAllocatorRepository.allocate_revision(
            work,
            RevisionStream.SOURCE,
            updated_at=timestamp,
        )
        source_head = _lock_source_head(work, candidate.channel)
        publication_head = _lock_publication_head(work, candidate.channel)
        deletion_generation = _lock_deletion_generation_head(work)

        _require_exact_base(
            "source",
            pinned=base_source,
            actual=source_head,
        )
        _require_exact_base(
            "catalog",
            pinned=base_catalog,
            actual=publication_head,
        )
        if deletion_generation != preparation.deletion_generation:
            raise PublicationHeadRaceError(
                "deletion-request generation advanced after operational preparation"
            )
        _require_no_activation_conflict(
            work,
            source_revision=source_revision,
            preparation_id=preparation.preparation_id,
        )

        source_generation = _successor_generation(base_source)
        catalog_generation = _successor_generation(base_catalog)
        connector = work.connector
        connector.execute(
            f"INSERT INTO {_SOURCE_REVISION_TABLE} "
            "(source_revision, channel, snapshot_manifest_sha256, published_at) "
            "VALUES (%s, %s, %s, %s)",
            (
                source_revision,
                candidate.channel,
                candidate.snapshot_manifest_sha256,
                timestamp,
            ),
        )
        connector.execute(
            f"INSERT INTO {_SOURCE_PROVENANCE_TABLE} "
            "(source_revision, analysis_id) VALUES (%s, %s)",
            (source_revision, candidate.analysis_id),
        )
        connector.execute(
            f"INSERT INTO {_ACTIVATION_TABLE} "
            "(source_revision, preparation_id, operational_policy_id, activated_at) "
            "VALUES (%s, %s, %s, %s)",
            (
                source_revision,
                preparation.preparation_id,
                preparation.policy_id,
                timestamp,
            ),
        )
        connector.execute(
            f"INSERT INTO {_RECEIPT_TABLE} "
            "(receipt_id, revision, source_revision, reserved_revision, channel, "
            "artifact_policy_id, display_title_policy_id, publication_count, "
            "new_galleries, changed_galleries, removed_galleries, "
            "duplicate_losers, state, committed_at, finalized_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, NULL)",
            (
                receipt_id,
                candidate.reserved_revision,
                source_revision,
                candidate.reserved_revision,
                candidate.channel,
                candidate.artifact_policy_id,
                candidate.display_title_policy_id,
                projection.publication_count,
                projection.new_galleries,
                projection.changed_galleries,
                projection.removed_galleries,
                projection.duplicate_losers,
                "DB_COMMITTED",
                timestamp,
            ),
        )
        _advance_source_head(
            work,
            channel=candidate.channel,
            base=source_head,
            revision=source_revision,
            generation=source_generation,
            now=timestamp,
        )
        _advance_publication_head(
            work,
            channel=candidate.channel,
            base=publication_head,
            revision=candidate.reserved_revision,
            generation=catalog_generation,
            now=timestamp,
        )
        work.compare_and_swap(
            f"UPDATE {_CANDIDATE_TABLE} SET state = %s "
            "WHERE candidate_id = %s AND state = %s AND sealed_at = %s",
            ("PUBLISHED", candidate.candidate_id, "SEALED", candidate.sealed_at),
            authority="publication candidate",
        )
        _delete_working_root(
            work,
            table=_SOURCE_WORKING_TABLE,
            label="source working build",
            slot=source_working[0],
            identity_column="build_id",
            identity=candidate.build_id,
        )
        _delete_working_root(
            work,
            table=_CATALOG_WORKING_TABLE,
            label="catalog working candidate",
            slot=catalog_working[0],
            identity_column="candidate_id",
            identity=candidate.candidate_id,
        )

        return PublicationCommitReceipt(
            candidate.candidate_id,
            receipt_id,
            candidate.reserved_revision,
            source_revision,
            candidate.reserved_revision,
            candidate.channel,
            candidate.artifact_policy_id,
            candidate.display_title_policy_id,
            projection.publication_count,
            projection.new_galleries,
            projection.changed_galleries,
            projection.removed_galleries,
            projection.duplicate_losers,
            "DB_COMMITTED",
            timestamp,
            None,
            False,
        )

    @staticmethod
    def finalize_artifacts(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        release_artifacts: Callable[[tuple[PreparedArtifactRelease, ...]], None],
        now: int,
    ) -> PublicationFinalizeBatch:
        """Release one bounded protected-artifact page and CAS its checkpoint.

        ``release_artifacts`` is an internal idempotent adapter.  It receives
        only server-derived exact protection tokens while the database
        transaction is open.  A crash after release but before commit retries
        the same tokens; the adapter must therefore make repeated release a
        no-op.  The final empty page alone moves the durable publication
        receipt to ``PROJECTION_FINALIZED``.
        """

        candidate_key = require_uuid16(candidate_id, field="finalize candidate_id")
        attempt = require_bounded_bytes(
            batch_key,
            field="finalize batch_key",
            minimum=1,
            maximum=512,
        )
        if not callable(release_artifacts):
            raise TypeError("release_artifacts must be callable")
        timestamp = require_int63(now, field="finalize committed_at")
        _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_schema_authorities(work)

        candidate = _lock_candidate(work, candidate_key)
        if candidate.state != "PUBLISHED":
            raise PublicationNotReadyError(
                "artifact finalization requires a PUBLISHED candidate"
            )

        projection = _lock_projection_seal(work, candidate_key)
        _validate_artifact_counts(candidate, projection)
        checkpoint = _lock_finalize_checkpoint(
            work, candidate_key, initialized_at=timestamp
        )
        receipt_row = _lock_receipt_by_revision(work, candidate.reserved_revision)
        if not receipt_row:
            raise PublicationCorruptionError(
                "PUBLISHED candidate has no durable publication receipt"
            )
        receipt = _receipt_from_row(candidate_key, receipt_row, replayed=True)
        _validate_receipt_scalars(candidate, projection, receipt)
        if timestamp < receipt.committed_at or (
            checkpoint.exists and timestamp < checkpoint.updated_at
        ):
            raise PublicationNotReadyError(
                "finalization timestamp precedes its durable checkpoint"
            )

        stored_batch = _load_finalize_batch(work, candidate_key, attempt)
        if stored_batch is not None:
            return _finalize_batch_from_row(
                candidate_key,
                attempt,
                stored_batch,
                receipt_state=receipt.state,
                replayed=True,
            )
        if checkpoint.state == "COMPLETE":
            raise PublicationNotReadyError(
                "artifact finalization is already complete for another batch key"
            )
        if checkpoint.generation == INT63_MAX:
            raise PublicationNotReadyError(
                "artifact finalization checkpoint generation is exhausted"
            )

        if not checkpoint.exists:
            work.connector.execute(
                "INSERT INTO catalog_publication_checkpoints "
                "(candidate_id, stage, generation, cursor, processed_count, "
                "state, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    candidate_key,
                    _FINALIZE_STAGE,
                    checkpoint.generation,
                    checkpoint.cursor,
                    checkpoint.processed_count,
                    checkpoint.state,
                    checkpoint.updated_at,
                ),
            )

        rows = work.connector.fetch_all(
            "SELECT prepared.publication_key, identity.artifact_name, "
            "prepared.artifact_sha256, prepared.protection_token "
            "FROM catalog_prepared_artifacts prepared "
            "JOIN catalog_publication_identities identity "
            "ON identity.publication_key = prepared.publication_key "
            "WHERE prepared.candidate_id = %s AND prepared.state = %s "
            "AND prepared.publication_key > %s "
            "ORDER BY prepared.publication_key LIMIT %s",
            (
                candidate_key,
                "PREPARED",
                checkpoint.cursor,
                _FINALIZE_BATCH_ROWS + 1,
            ),
        )
        selected = rows[:_FINALIZE_BATCH_ROWS]
        releases = tuple(_release_from_row(row) for row in selected)
        if releases:
            release_artifacts(releases)

        for release in releases:
            affected = work.connector.execute_affected(
                "UPDATE catalog_prepared_artifacts SET state = %s "
                "WHERE candidate_id = %s AND publication_key = %s "
                "AND artifact_sha256 = %s AND protection_token = %s AND state = %s",
                (
                    "COMMITTED",
                    candidate_key,
                    release.publication_key,
                    release.artifact_sha256,
                    release.protection_token,
                    "PREPARED",
                ),
            )
            if affected != 1:
                raise PublicationHeadRaceError(
                    "prepared artifact changed during bounded finalization"
                )

        terminal = not releases
        row_count = len(releases)
        next_count = checkpoint.processed_count + row_count
        if next_count > projection.prepared_artifact_count:
            raise PublicationCorruptionError(
                "finalization processed more rows than the projection seal"
            )
        if terminal and next_count != projection.prepared_artifact_count:
            raise PublicationCorruptionError(
                "terminal finalization count disagrees with projection seal"
            )
        next_cursor = checkpoint.cursor if terminal else releases[-1].publication_key
        next_state = "COMPLETE" if terminal else "OPEN"
        successor = checkpoint.generation + 1
        work.connector.execute(
            "INSERT INTO catalog_publication_batch_receipts "
            "(candidate_id, stage, batch_key, start_generation, start_cursor, "
            "start_processed_count, next_cursor, next_processed_count, next_state, "
            "row_count, terminal, committed_generation, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                candidate_key,
                _FINALIZE_STAGE,
                attempt,
                checkpoint.generation,
                checkpoint.cursor,
                checkpoint.processed_count,
                next_cursor,
                next_count,
                next_state,
                row_count,
                int(terminal),
                successor,
                timestamp,
            ),
        )
        work.compare_and_swap(
            "UPDATE catalog_publication_checkpoints SET generation = %s, "
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
                candidate_key,
                _FINALIZE_STAGE,
                checkpoint.generation,
                checkpoint.cursor,
                checkpoint.processed_count,
                checkpoint.state,
                checkpoint.updated_at,
            ),
            authority="publication artifact-finalization checkpoint",
        )
        receipt_state = receipt.state
        if terminal:
            if receipt.state == "DB_COMMITTED":
                work.compare_and_swap(
                    f"UPDATE {_RECEIPT_TABLE} SET state = %s, finalized_at = %s "
                    "WHERE receipt_id = %s AND reserved_revision = %s "
                    "AND state = %s AND finalized_at IS NULL",
                    (
                        "PROJECTION_FINALIZED",
                        timestamp,
                        receipt.receipt_id,
                        candidate.reserved_revision,
                        "DB_COMMITTED",
                    ),
                    authority="publication receipt finalization",
                )
            elif receipt.state != "PROJECTION_FINALIZED":
                raise PublicationCorruptionError(
                    "publication receipt has an invalid finalization state"
                )
            receipt_state = "PROJECTION_FINALIZED"

        return PublicationFinalizeBatch(
            candidate_key,
            attempt,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            next_cursor,
            next_count,
            next_state,
            row_count,
            terminal,
            successor,
            timestamp,
            receipt_state,
            False,
        )


def _authorize(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    live_gate = MaintenanceGateRepository.lock_and_require_live(
        work,
        gate_lease,
        now=now,
    )
    if live_gate.mode is not GateMode.SHARED:
        raise PublicationNotReadyError(
            "publication requires a live SHARED maintenance gate"
        )
    live_turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(live_turn.generation, field="publication ingest generation")


def _require_schema_authorities(work: VNextUnitOfWork) -> None:
    missing = tuple(
        table
        for table in _REQUIRED_SCHEMA_TABLES
        if not work.connector.check_table_exists(table)
    )
    if missing:
        raise PublicationSchemaNotReadyError(
            "publication is fail-closed until generated O(1) authorities exist: "
            + ", ".join(missing)
        )


def _lock_candidate(work: VNextUnitOfWork, candidate_id: bytes) -> _Candidate:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 0, candidate_id),
        f"SELECT c.analysis_id, c.reserved_revision, c.channel, "
        "c.artifact_policy_id, c.display_title_policy_id, "
        "c.artifacts_required, c.state, c.created_at, c.sealed_at, "
        "a.build_id, a.input_manifest_sha256, a.state, a.completed_at, "
        "b.state, b.sealed_at, bc.channel, bm.manifest_sha256, "
        "bm.gallery_count, bm.file_count, bm.byte_count, "
        "sm.snapshot_manifest_sha256, m.gallery_count, m.file_count, "
        "m.byte_count, va.digest_domain, vi.root_page_sha256, va.byte_count, "
        "ap.algorithm_version, ap.spam_artist_threshold, "
        "ap.spam_occurrence_threshold, ap.content_owner_rule_version, "
        "ap.gid_winner_rule_version "
        f"FROM {_CANDIDATE_TABLE} c "
        "JOIN catalog_analysis_runs a ON a.analysis_id = c.analysis_id "
        "JOIN catalog_analysis_policies ap ON ap.policy_id = a.policy_id "
        "JOIN catalog_source_builds b ON b.build_id = a.build_id "
        "JOIN catalog_source_build_channel bc ON bc.build_id = b.build_id "
        "JOIN catalog_build_manifests bm ON bm.build_id = b.build_id "
        f"JOIN {_ANALYSIS_MANIFEST_TABLE} sm ON sm.analysis_id = a.analysis_id "
        "JOIN catalog_source_snapshot_manifest_identity m "
        "ON m.snapshot_manifest_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_canonical_value_allocations va "
        "ON va.value_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_canonical_value_identities vi "
        "ON vi.value_sha256 = sm.snapshot_manifest_sha256 "
        "WHERE c.candidate_id = %s",
        (candidate_id,),
    )
    if len(row) != 32:
        raise PublicationNotReadyError(
            "candidate, analysis, build, or canonical snapshot manifest is missing"
        )
    analysis_id = require_uuid16(row[0], field="candidate analysis_id")
    revision = require_positive_int63(row[1], field="candidate reserved_revision")
    channel = require_bounded_bytes(
        row[2], field="candidate channel", minimum=1, maximum=64
    )
    artifact_policy = require_positive_int63(
        row[3], field="candidate artifact_policy_id"
    )
    display_policy = require_positive_int63(
        row[4], field="candidate display_title_policy_id"
    )
    if row[5] not in {0, 1}:
        raise PublicationCorruptionError("candidate artifacts_required is not boolean")
    state = row[6]
    if state not in {"OPEN", "SEALED", "PUBLISHED", "ABANDONED"}:
        raise PublicationCorruptionError("candidate state is not registered")
    created_at = require_int63(row[7], field="candidate created_at")
    if row[8] is None:
        raise PublicationNotReadyError("candidate has no sealed_at authority")
    sealed_at = require_int63(row[8], field="candidate sealed_at")
    build_id = require_uuid16(row[9], field="candidate build_id")
    input_manifest = require_digest32(row[10], field="analysis input manifest")
    if row[11] != "COMPLETE" or row[12] is None:
        raise PublicationNotReadyError("candidate analysis is not COMPLETE")
    analysis_completed_at = require_int63(row[12], field="analysis completed_at")
    if row[13] != "SEALED" or row[14] is None:
        raise PublicationNotReadyError("candidate source build is not SEALED")
    build_sealed_at = require_int63(row[14], field="source build sealed_at")
    build_channel = require_bounded_bytes(
        row[15], field="source build channel", minimum=1, maximum=64
    )
    build_manifest = require_digest32(row[16], field="source build manifest")
    build_counts = tuple(
        require_int63(value, field=field)
        for field, value in (
            ("build gallery_count", row[17]),
            ("build file_count", row[18]),
            ("build byte_count", row[19]),
        )
    )
    snapshot_manifest = require_digest32(row[20], field="analysis snapshot manifest")
    snapshot_counts = tuple(
        require_int63(value, field=field)
        for field, value in (
            ("snapshot gallery_count", row[21]),
            ("snapshot file_count", row[22]),
            ("snapshot byte_count", row[23]),
        )
    )
    policy_values = (
        require_uint32(row[27], field="analysis algorithm_version"),
        require_int63(row[28], field="analysis spam_artist_threshold"),
        require_int63(row[29], field="analysis spam_occurrence_threshold"),
        require_uint32(row[30], field="analysis content_owner_rule_version"),
        require_uint32(row[31], field="analysis gid_winner_rule_version"),
    )
    expected_input = _analysis_input_digest(
        build_manifest,
        build_counts,
        policy_values,
    )
    if input_manifest != expected_input:
        raise PublicationCorruptionError(
            "analysis input digest disagrees with its build counts and policy"
        )
    if snapshot_counts != build_counts:
        raise PublicationConflictError(
            "analysis snapshot counts disagree with the sealed source build"
        )
    if row[24] != _SOURCE_MANIFEST_DOMAIN:
        raise PublicationCorruptionError(
            "analysis snapshot manifest uses the wrong canonical digest domain"
        )
    require_digest32(row[25], field="snapshot manifest root_page_sha256")
    require_int63(row[26], field="snapshot canonical payload byte_count")
    if build_channel != channel:
        raise PublicationConflictError(
            "candidate channel disagrees with its source build channel"
        )
    # ``input_manifest_sha256`` is an audit digest over the build manifest,
    # all three build counts, and the complete analysis-policy tuple.  It is
    # intentionally not the raw build-manifest digest and never authorizes
    # source publication.  COMPLETE component seals plus the canonical
    # analysis_snapshot_manifest binding are the output authority here.
    if not (created_at <= sealed_at and analysis_completed_at <= sealed_at):
        raise PublicationCorruptionError(
            "candidate lifecycle timestamps are not monotone"
        )
    if build_sealed_at > analysis_completed_at:
        raise PublicationCorruptionError(
            "analysis completed before its source build was sealed"
        )
    return _Candidate(
        candidate_id,
        analysis_id,
        revision,
        channel,
        artifact_policy,
        display_policy,
        bool(row[5]),
        state,
        created_at,
        sealed_at,
        build_id,
        input_manifest,
        analysis_completed_at,
        build_sealed_at,
        snapshot_manifest,
    )


def _analysis_input_digest(
    manifest_sha256: bytes,
    counts: tuple[int, ...],
    policy_values: tuple[int, int, int, int, int],
) -> bytes:
    if len(counts) != 3:
        raise ValueError("build manifest requires exactly three aggregate counts")
    payload = bytearray(b"h2hdb-vnext-analysis-input\0")
    payload.extend(require_digest32(manifest_sha256, field="build manifest_sha256"))
    for index, count in enumerate(counts):
        payload.extend(
            require_int63(count, field=f"build manifest count {index}").to_bytes(
                8, "big"
            )
        )
    payload.extend(policy_values[0].to_bytes(4, "big"))
    payload.extend(policy_values[1].to_bytes(8, "big"))
    payload.extend(policy_values[2].to_bytes(8, "big"))
    payload.extend(policy_values[3].to_bytes(4, "big"))
    payload.extend(policy_values[4].to_bytes(4, "big"))
    return sha256(payload).digest()


def _lock_generation_mapping(work: VNextUnitOfWork, generation: int) -> bytes | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 1, generation),
        f"SELECT build_id FROM {_BUILD_GENERATION_TABLE} WHERE generation = %s",
        (generation,),
    )
    if not row:
        return None
    if len(row) != 1:
        raise PublicationCorruptionError("source-build generation mapping is malformed")
    return require_uuid16(row[0], field="mapped publication build_id")


def _lock_source_working(
    work: VNextUnitOfWork, build_id: bytes
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 2, build_id),
        f"SELECT slot, build_id, assigned_at FROM {_SOURCE_WORKING_TABLE} "
        "WHERE build_id = %s",
        (build_id,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCorruptionError("source working root is malformed")
    slot = require_positive_int63(row[0], field="source working slot")
    mapped = require_uuid16(row[1], field="source working build_id")
    assigned_at = require_int63(row[2], field="source working assigned_at")
    return slot, mapped, assigned_at


def _lock_catalog_working(
    work: VNextUnitOfWork, candidate_id: bytes
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 3, candidate_id),
        f"SELECT slot, candidate_id, assigned_at FROM {_CATALOG_WORKING_TABLE} "
        "WHERE candidate_id = %s",
        (candidate_id,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCorruptionError("catalog working root is malformed")
    slot = require_positive_int63(row[0], field="catalog working slot")
    mapped = require_uuid16(row[1], field="catalog working candidate_id")
    assigned_at = require_int63(row[2], field="catalog working assigned_at")
    return slot, mapped, assigned_at


def _lock_preparation(work: VNextUnitOfWork, candidate_id: bytes) -> _Preparation:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 4, candidate_id),
        f"SELECT a.preparation_id, p.build_id, p.deletion_request_generation, "
        "p.operational_policy_id, p.state, p.prepared_at, p.completed_at, "
        "a.bound_at "
        f"FROM {_CANDIDATE_PREPARATION_TABLE} a "
        f"JOIN {_PREPARATION_TABLE} p ON p.preparation_id = a.preparation_id "
        "WHERE a.candidate_id = %s",
        (candidate_id,),
    )
    if len(row) != 8:
        raise PublicationNotReadyError(
            "candidate has no exact operational preparation authority"
        )
    preparation_id = require_uuid16(row[0], field="operational preparation_id")
    build_id = require_uuid16(row[1], field="operational preparation build_id")
    deletion_generation = require_int63(row[2], field="preparation deletion generation")
    policy_id = require_positive_int63(row[3], field="operational policy_id")
    if row[4] != "COMPLETE" or row[6] is None:
        raise PublicationNotReadyError("operational preparation is not COMPLETE")
    prepared_at = require_int63(row[5], field="operational prepared_at")
    completed_at = require_int63(row[6], field="operational completed_at")
    bound_at = require_int63(row[7], field="candidate preparation bound_at")
    if completed_at < prepared_at:
        raise PublicationCorruptionError(
            "operational preparation lifecycle timestamps are not monotone"
        )
    if bound_at < completed_at:
        raise PublicationCorruptionError(
            "candidate preparation was bound before completion"
        )
    return _Preparation(
        preparation_id,
        build_id,
        deletion_generation,
        policy_id,
        prepared_at,
        completed_at,
        bound_at,
    )


def _require_exact_analysis_seals(work: VNextUnitOfWork, candidate: _Candidate) -> None:
    rows = work.connector.fetch_all(
        "SELECT state_component, row_count, sealed_at "
        "FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (candidate.analysis_id,),
    )
    if len(rows) != len(ANALYSIS_COMPONENTS):
        raise PublicationNotReadyError(
            "publication requires exactly five analysis component seals"
        )
    components: set[bytes] = set()
    for row in rows:
        if len(row) != 3:
            raise PublicationCorruptionError("analysis component seal is malformed")
        component = require_bounded_bytes(
            row[0], field="analysis state component", minimum=1, maximum=64
        )
        require_int63(row[1], field="analysis component row_count")
        sealed_at = require_int63(row[2], field="analysis component sealed_at")
        if sealed_at > candidate.analysis_completed_at:
            raise PublicationCorruptionError(
                "analysis completed before one of its immutable component seals"
            )
        components.add(component)
    if components != ANALYSIS_COMPONENTS:
        raise PublicationNotReadyError(
            "analysis component seals do not match the closed five-component set"
        )


def _lock_projection_seal(
    work: VNextUnitOfWork, candidate_id: bytes
) -> _ProjectionSeal:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 0, candidate_id),
        f"SELECT publication_count, artifact_input_count, "
        "prepared_artifact_count, create_count, rebuild_count, delete_count, "
        "unchanged_count, new_galleries, changed_galleries, removed_galleries, "
        "duplicate_losers, projection_sealed_at "
        f"FROM {_PROJECTION_SEAL_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    if len(row) != 12:
        raise PublicationNotReadyError(
            "candidate has no immutable O(1) projection seal"
        )
    values = tuple(
        require_int63(value, field=f"projection seal {field}")
        for field, value in zip(
            (
                "publication_count",
                "artifact_input_count",
                "prepared_artifact_count",
                "create_count",
                "rebuild_count",
                "delete_count",
                "unchanged_count",
                "new_galleries",
                "changed_galleries",
                "removed_galleries",
                "duplicate_losers",
                "sealed_at",
            ),
            row,
            strict=True,
        )
    )
    seal = _ProjectionSeal(*values)
    if seal.prepared_artifact_count != seal.create_count + seal.rebuild_count:
        raise PublicationCorruptionError(
            "projection prepared count disagrees with CREATE/REBUILD counts"
        )
    if seal.artifact_input_count != (
        seal.create_count + seal.rebuild_count + seal.unchanged_count
    ):
        raise PublicationCorruptionError(
            "projection artifact-input count disagrees with new-side operations"
        )
    return seal


def _lock_finalize_checkpoint(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    *,
    initialized_at: int,
) -> _FinalizeCheckpoint:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 1, candidate_id),
        "SELECT generation, cursor, processed_count, state, updated_at "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (candidate_id, _FINALIZE_STAGE),
    )
    if not row:
        return _FinalizeCheckpoint(1, b"", 0, "OPEN", initialized_at, False)
    if len(row) != 5:
        raise PublicationCorruptionError(
            "artifact-finalization checkpoint is malformed"
        )
    generation = require_positive_int63(row[0], field="finalize checkpoint generation")
    cursor = require_bounded_bytes(
        row[1], field="finalize checkpoint cursor", maximum=32
    )
    if len(cursor) not in {0, 32}:
        raise PublicationCorruptionError(
            "artifact-finalization cursor is not empty or raw32"
        )
    processed = require_int63(row[2], field="finalize checkpoint processed_count")
    state = row[3]
    if state not in {"OPEN", "COMPLETE"}:
        raise PublicationCorruptionError(
            "artifact-finalization checkpoint state is not registered"
        )
    updated_at = require_int63(row[4], field="finalize checkpoint updated_at")
    return _FinalizeCheckpoint(
        generation,
        cursor,
        processed,
        state,
        updated_at,
        True,
    )


def _load_finalize_batch(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    batch_key: bytes,
) -> tuple[Any, ...] | None:
    row = work.connector.fetch_one(
        "SELECT start_generation, start_cursor, start_processed_count, "
        "next_cursor, next_processed_count, next_state, row_count, terminal, "
        "committed_generation, committed_at "
        "FROM catalog_publication_batch_receipts "
        "WHERE candidate_id = %s AND stage = %s AND batch_key = %s",
        (candidate_id, _FINALIZE_STAGE, batch_key),
    )
    return None if not row else row


def _finalize_batch_from_row(
    candidate_id: bytes,
    batch_key: bytes,
    row: tuple[Any, ...],
    *,
    receipt_state: str,
    replayed: bool,
) -> PublicationFinalizeBatch:
    if len(row) != 10:
        raise PublicationCorruptionError(
            "artifact-finalization batch receipt is malformed"
        )
    terminal_value = require_int63(row[7], field="stored finalize terminal")
    if terminal_value not in {0, 1}:
        raise PublicationCorruptionError(
            "artifact-finalization terminal flag is not boolean"
        )
    try:
        result = PublicationFinalizeBatch(
            candidate_id,
            batch_key,
            require_positive_int63(row[0], field="stored finalize start_generation"),
            require_bounded_bytes(
                row[1], field="stored finalize start_cursor", maximum=32
            ),
            require_int63(row[2], field="stored finalize start_processed_count"),
            require_bounded_bytes(
                row[3], field="stored finalize next_cursor", maximum=32
            ),
            require_int63(row[4], field="stored finalize next_processed_count"),
            row[5],
            require_int63(row[6], field="stored finalize row_count"),
            bool(terminal_value),
            require_positive_int63(
                row[8], field="stored finalize committed_generation"
            ),
            require_int63(row[9], field="stored finalize committed_at"),
            receipt_state,
            replayed,
        )
    except (TypeError, ValueError) as error:
        raise PublicationCorruptionError(
            "artifact-finalization batch receipt has invalid domain values"
        ) from error
    if (
        result.committed_generation != result.start_generation + 1
        or result.next_processed_count
        != result.start_processed_count + result.row_count
        or (result.terminal and result.row_count != 0)
        or (not result.terminal and result.row_count == 0)
        or (result.terminal and result.next_state != "COMPLETE")
        or (not result.terminal and result.next_state != "OPEN")
    ):
        raise PublicationCorruptionError(
            "artifact-finalization batch receipt is not a monotone transition"
        )
    if result.terminal and receipt_state != "PROJECTION_FINALIZED":
        raise PublicationCorruptionError(
            "terminal finalization receipt exists before publication finalization"
        )
    return result


def _release_from_row(row: tuple[Any, ...]) -> PreparedArtifactRelease:
    if len(row) != 4:
        raise PublicationCorruptionError("prepared artifact row is malformed")
    try:
        return PreparedArtifactRelease(
            require_digest32(row[0], field="prepared publication_key"),
            require_bounded_bytes(
                row[1], field="prepared artifact_name", minimum=1, maximum=255
            ),
            require_digest32(row[2], field="prepared artifact_sha256"),
            require_bounded_bytes(
                row[3], field="prepared protection_token", maximum=512
            ),
        )
    except (TypeError, ValueError) as error:
        raise PublicationCorruptionError(
            "prepared artifact contains invalid domain values"
        ) from error


def _lock_and_validate_effect_seal(
    work: VNextUnitOfWork,
    preparation: _Preparation,
    *,
    timestamp: int,
) -> None:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 1, preparation.preparation_id),
        f"SELECT event_count, final_chain_sha256, sealed_at "
        f"FROM {_EFFECT_SEAL_TABLE} WHERE preparation_id = %s",
        (preparation.preparation_id,),
    )
    if len(row) != 3:
        raise PublicationNotReadyError("operational preparation effect seal is missing")
    require_int63(row[0], field="operational sealed event_count")
    require_digest32(row[1], field="operational final_chain_sha256")
    sealed_at = require_int63(row[2], field="operational effect sealed_at")
    if sealed_at > preparation.completed_at or timestamp < sealed_at:
        raise PublicationCorruptionError(
            "operational effect seal timestamps disagree with completion"
        )


def _validate_artifact_counts(
    candidate: _Candidate, projection: _ProjectionSeal
) -> None:
    if candidate.artifacts_required:
        if projection.artifact_input_count != projection.publication_count:
            raise PublicationCorruptionError(
                "artifact-enabled projection does not cover every publication"
            )
        return
    if any(
        (
            projection.artifact_input_count,
            projection.prepared_artifact_count,
            projection.create_count,
            projection.rebuild_count,
            projection.unchanged_count,
        )
    ):
        raise PublicationCorruptionError(
            "artifact-disabled projection contains a desired artifact side"
        )


def _lock_receipt_by_revision(
    work: VNextUnitOfWork, reserved_revision: int
) -> tuple[Any, ...]:
    return work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 2, reserved_revision),
        f"SELECT receipt_id, revision, source_revision, reserved_revision, "
        "channel, artifact_policy_id, display_title_policy_id, "
        "publication_count, new_galleries, changed_galleries, "
        "removed_galleries, duplicate_losers, state, committed_at, finalized_at "
        f"FROM {_RECEIPT_TABLE} WHERE reserved_revision = %s",
        (reserved_revision,),
    )


def _lock_receipt_id_collision(
    work: VNextUnitOfWork, receipt_id: bytes
) -> tuple[Any, ...]:
    return work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 3, receipt_id),
        f"SELECT reserved_revision FROM {_RECEIPT_TABLE} WHERE receipt_id = %s",
        (receipt_id,),
    )


def _load_base_source(work: VNextUnitOfWork, candidate_id: bytes) -> _BaseHead | None:
    row = work.connector.fetch_one(
        f"SELECT base_source_revision, base_source_generation "
        f"FROM {_BASE_SOURCE_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    return _base_from_row(row, label="candidate base source")


def _load_base_catalog(work: VNextUnitOfWork, candidate_id: bytes) -> _BaseHead | None:
    row = work.connector.fetch_one(
        f"SELECT base_revision, base_catalog_generation "
        f"FROM {_BASE_CATALOG_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    return _base_from_row(row, label="candidate base catalog")


def _base_from_row(row: tuple[Any, ...], *, label: str) -> _BaseHead | None:
    if not row:
        return None
    if len(row) != 2:
        raise PublicationCorruptionError(f"{label} row is malformed")
    return _BaseHead(
        require_positive_int63(row[0], field=f"{label} revision"),
        require_positive_int63(row[1], field=f"{label} generation"),
    )


def _require_build_base_source(
    work: VNextUnitOfWork,
    build_id: bytes,
    candidate_base: _BaseHead | None,
) -> None:
    row = work.connector.fetch_one(
        "SELECT base_source_revision, base_source_generation "
        "FROM catalog_source_build_base_source WHERE build_id = %s",
        (build_id,),
    )
    build_base = _base_from_row(row, label="source build base source")
    if build_base != candidate_base:
        raise PublicationConflictError(
            "candidate source base disagrees with its source build base"
        )


def _validate_catalog_revision(
    work: VNextUnitOfWork,
    candidate: _Candidate,
    projection: _ProjectionSeal,
    *,
    timestamp: int,
) -> None:
    row = work.connector.fetch_one(
        f"SELECT publication_count, published_at FROM {_REVISION_TABLE} "
        "WHERE revision = %s",
        (candidate.reserved_revision,),
    )
    if len(row) != 2:
        raise PublicationNotReadyError(
            "reserved catalog revision has not been prepared"
        )
    count = require_int63(row[0], field="catalog revision publication_count")
    published_at = require_int63(row[1], field="catalog revision published_at")
    if count != projection.publication_count:
        raise PublicationConflictError(
            "catalog revision count disagrees with its projection seal"
        )
    if published_at > projection.sealed_at or published_at > timestamp:
        raise PublicationCorruptionError(
            "catalog revision timestamp follows its projection seal or commit"
        )


def _lock_source_head(work: VNextUnitOfWork, channel: bytes) -> _LockedHead | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication", 0, channel),
        f"SELECT h.source_revision, h.generation, h.advanced_at, r.channel "
        f"FROM {_SOURCE_HEAD_TABLE} h "
        f"JOIN {_SOURCE_REVISION_TABLE} r ON r.source_revision = h.source_revision "
        "WHERE h.channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 4:
        raise PublicationCorruptionError("source head is malformed")
    descriptor_channel = require_bounded_bytes(
        row[3], field="source head descriptor channel", minimum=1, maximum=64
    )
    if descriptor_channel != channel:
        raise PublicationCorruptionError(
            "source head points to a revision from another channel"
        )
    return _LockedHead(
        require_positive_int63(row[0], field="source head revision"),
        require_positive_int63(row[1], field="source head generation"),
        require_int63(row[2], field="source head advanced_at"),
    )


def _lock_publication_head(work: VNextUnitOfWork, channel: bytes) -> _LockedHead | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication", 1, channel),
        f"SELECT revision, generation, advanced_at FROM {_PUBLICATION_HEAD_TABLE} "
        "WHERE channel = %s",
        (channel,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCorruptionError("publication head is malformed")
    return _LockedHead(
        require_positive_int63(row[0], field="publication head revision"),
        require_positive_int63(row[1], field="publication head generation"),
        require_int63(row[2], field="publication head advanced_at"),
    )


def _lock_deletion_generation_head(work: VNextUnitOfWork) -> int:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication", 2),
        f"SELECT current_generation FROM {_DELETION_HEAD_TABLE} "
        "WHERE singleton_id = %s",
        (1,),
    )
    if len(row) != 1:
        raise PublicationCorruptionError("deletion-request generation head is missing")
    return require_int63(row[0], field="current deletion-request generation")


def _require_exact_base(
    label: str,
    *,
    pinned: _BaseHead | None,
    actual: _LockedHead | None,
) -> None:
    if pinned is None:
        if actual is not None:
            raise PublicationHeadRaceError(
                f"{label} head appeared after genesis candidate reservation"
            )
        return
    if actual is None or (
        actual.revision != pinned.revision or actual.generation != pinned.generation
    ):
        raise PublicationHeadRaceError(
            f"{label} head changed after candidate reservation"
        )


def _successor_generation(base: _BaseHead | None) -> int:
    if base is None:
        return 1
    if base.generation == INT63_MAX:
        raise PublicationNotReadyError("publication head generation is exhausted")
    return base.generation + 1


def _require_no_activation_conflict(
    work: VNextUnitOfWork,
    *,
    source_revision: int,
    preparation_id: bytes,
) -> None:
    rows = work.connector.fetch_all(
        f"SELECT source_revision, preparation_id FROM {_ACTIVATION_TABLE} "
        "WHERE source_revision = %s OR preparation_id = %s "
        "ORDER BY source_revision LIMIT 2",
        (source_revision, preparation_id),
    )
    if rows:
        raise PublicationConflictError(
            "source revision or operational preparation is already activated"
        )


def _advance_source_head(
    work: VNextUnitOfWork,
    *,
    channel: bytes,
    base: _LockedHead | None,
    revision: int,
    generation: int,
    now: int,
) -> None:
    if base is None:
        work.connector.execute(
            f"INSERT INTO {_SOURCE_HEAD_TABLE} "
            "(channel, source_revision, generation, advanced_at) "
            "VALUES (%s, %s, %s, %s)",
            (channel, revision, generation, now),
        )
        return
    work.compare_and_swap(
        f"UPDATE {_SOURCE_HEAD_TABLE} SET source_revision = %s, generation = %s, "
        "advanced_at = %s WHERE channel = %s AND source_revision = %s "
        "AND generation = %s AND advanced_at = %s",
        (
            revision,
            generation,
            now,
            channel,
            base.revision,
            base.generation,
            base.advanced_at,
        ),
        authority="source head",
    )


def _advance_publication_head(
    work: VNextUnitOfWork,
    *,
    channel: bytes,
    base: _LockedHead | None,
    revision: int,
    generation: int,
    now: int,
) -> None:
    if base is None:
        work.connector.execute(
            f"INSERT INTO {_PUBLICATION_HEAD_TABLE} "
            "(channel, revision, generation, advanced_at) "
            "VALUES (%s, %s, %s, %s)",
            (channel, revision, generation, now),
        )
        return
    work.compare_and_swap(
        f"UPDATE {_PUBLICATION_HEAD_TABLE} SET revision = %s, generation = %s, "
        "advanced_at = %s WHERE channel = %s AND revision = %s "
        "AND generation = %s AND advanced_at = %s",
        (
            revision,
            generation,
            now,
            channel,
            base.revision,
            base.generation,
            base.advanced_at,
        ),
        authority="publication head",
    )


def _delete_working_root(
    work: VNextUnitOfWork,
    *,
    table: str,
    label: str,
    slot: int,
    identity_column: str,
    identity: bytes,
) -> None:
    affected = work.connector.execute_affected(
        f"DELETE FROM {table} WHERE slot = %s AND {identity_column} = %s",
        (slot, identity),
    )
    if affected != 1:
        raise PublicationHeadRaceError(f"{label} changed during publication")


def _receipt_from_row(
    candidate_id: bytes,
    row: tuple[Any, ...],
    *,
    replayed: bool,
) -> PublicationCommitReceipt:
    if len(row) != 15:
        raise PublicationCorruptionError("publication receipt row is malformed")
    finalized_at = (
        None
        if row[14] is None
        else require_int63(row[14], field="stored publication finalized_at")
    )
    try:
        return PublicationCommitReceipt(
            candidate_id,
            require_uuid16(row[0], field="stored publication receipt_id"),
            require_positive_int63(row[1], field="stored catalog revision"),
            require_positive_int63(row[2], field="stored source revision"),
            require_positive_int63(row[3], field="stored reserved catalog revision"),
            require_bounded_bytes(
                row[4], field="stored publication channel", minimum=1, maximum=64
            ),
            require_positive_int63(row[5], field="stored artifact_policy_id"),
            require_positive_int63(row[6], field="stored display_title_policy_id"),
            require_int63(row[7], field="stored publication_count"),
            require_int63(row[8], field="stored new_galleries"),
            require_int63(row[9], field="stored changed_galleries"),
            require_int63(row[10], field="stored removed_galleries"),
            require_int63(row[11], field="stored duplicate_losers"),
            row[12],
            require_int63(row[13], field="stored publication committed_at"),
            finalized_at,
            replayed,
        )
    except (TypeError, ValueError) as error:
        raise PublicationCorruptionError(
            "publication receipt contains invalid domain values"
        ) from error


def _validate_replay(
    work: VNextUnitOfWork,
    *,
    candidate: _Candidate,
    preparation: _Preparation,
    projection: _ProjectionSeal,
    base_source: _BaseHead | None,
    base_catalog: _BaseHead | None,
    receipt: PublicationCommitReceipt,
) -> None:
    _validate_receipt_scalars(candidate, projection, receipt)
    if candidate.state != "PUBLISHED":
        raise PublicationCorruptionError(
            "publication receipt exists for a candidate that is not PUBLISHED"
        )
    provenance = work.connector.fetch_one(
        f"SELECT r.channel, r.snapshot_manifest_sha256, r.published_at, p.analysis_id "
        f"FROM {_SOURCE_REVISION_TABLE} r "
        f"JOIN {_SOURCE_PROVENANCE_TABLE} p "
        "ON p.source_revision = r.source_revision "
        "WHERE r.source_revision = %s",
        (receipt.source_revision,),
    )
    if len(provenance) != 4:
        raise PublicationCorruptionError(
            "published source revision or provenance is missing"
        )
    if (
        provenance[0] != candidate.channel
        or provenance[1] != candidate.snapshot_manifest_sha256
        or require_int63(provenance[2], field="stored source published_at")
        != receipt.committed_at
        or provenance[3] != candidate.analysis_id
    ):
        raise PublicationConflictError(
            "source revision conflicts with its publication receipt"
        )
    activation = work.connector.fetch_one(
        f"SELECT preparation_id, operational_policy_id, activated_at "
        f"FROM {_ACTIVATION_TABLE} WHERE source_revision = %s",
        (receipt.source_revision,),
    )
    if activation != (
        preparation.preparation_id,
        preparation.policy_id,
        receipt.committed_at,
    ):
        raise PublicationConflictError(
            "operational activation conflicts with its publication receipt"
        )
    _validate_replay_head(
        work.connector.fetch_one(
            f"SELECT source_revision, generation FROM {_SOURCE_HEAD_TABLE} "
            "WHERE channel = %s",
            (candidate.channel,),
        ),
        expected_revision=receipt.source_revision,
        expected_generation=_successor_generation(base_source),
        label="source",
    )
    _validate_replay_head(
        work.connector.fetch_one(
            f"SELECT revision, generation FROM {_PUBLICATION_HEAD_TABLE} "
            "WHERE channel = %s",
            (candidate.channel,),
        ),
        expected_revision=receipt.revision,
        expected_generation=_successor_generation(base_catalog),
        label="catalog",
    )


def _validate_receipt_scalars(
    candidate: _Candidate,
    projection: _ProjectionSeal,
    receipt: PublicationCommitReceipt,
) -> None:
    expected = (
        candidate.reserved_revision,
        candidate.reserved_revision,
        candidate.channel,
        candidate.artifact_policy_id,
        candidate.display_title_policy_id,
        projection.publication_count,
        projection.new_galleries,
        projection.changed_galleries,
        projection.removed_galleries,
        projection.duplicate_losers,
    )
    actual = (
        receipt.revision,
        receipt.reserved_revision,
        receipt.channel,
        receipt.artifact_policy_id,
        receipt.display_title_policy_id,
        receipt.publication_count,
        receipt.new_galleries,
        receipt.changed_galleries,
        receipt.removed_galleries,
        receipt.duplicate_losers,
    )
    if actual != expected:
        raise PublicationConflictError(
            "publication receipt conflicts with its sealed candidate"
        )


def _validate_replay_head(
    row: tuple[Any, ...],
    *,
    expected_revision: int,
    expected_generation: int,
    label: str,
) -> None:
    if len(row) != 2:
        raise PublicationCorruptionError(f"published {label} head is missing")
    actual_revision = require_positive_int63(
        row[0], field=f"replay {label} head revision"
    )
    actual_generation = require_positive_int63(
        row[1], field=f"replay {label} head generation"
    )
    if actual_generation < expected_generation or (
        actual_generation == expected_generation
        and actual_revision != expected_revision
    ):
        raise PublicationConflictError(
            f"{label} head conflicts with or predates the durable receipt"
        )


def _new_receipt_id() -> bytes:
    return secrets.token_bytes(16)
