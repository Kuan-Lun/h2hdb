"""Bounded, fixed-shard cleanup control for the vNext schema.

The cleanup tables contain *sweep* authorities, not arbitrary table names or
predicates.  Runtime dispatch therefore stays closed-world: every supported
target and phase below is bound to literal SQL owned by this module.  Database
text is never interpolated into a statement.

All twenty-two provider-seeded target kinds are implemented here.  The large
child-first targets use source-owned, immutable statement specifications; the
database registry is checked for exact equality but is never treated as SQL or
as an authorization predicate.
"""

from __future__ import annotations

__all__ = [
    "CatalogPublicationMaintenanceState",
    "CleanupBatchCommand",
    "CleanupBatchResult",
    "CleanupCorruptionError",
    "CleanupCycle",
    "CleanupCycleExhaustedError",
    "CleanupRetentionBlockedError",
    "CleanupTargetKind",
    "CleanupUnavailableError",
    "VNextCleanupRepository",
]

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum

from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_gallery_staging_budget import (
    GalleryStagingBudgetCorruptionError,
    lock_gallery_staging_request_budget,
    release_gallery_staging_request_budget,
)
from .vnext_gallery_staging_repository import (
    GalleryStagingConflictError,
    GalleryStagingNotReadyError,
    validate_terminal_staging_retirement_authority,
)
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_CLEANUP_ID_DOMAIN = b"h2hdb-cleanup-cycle-v1\0"
_TARGET_KEY_DOMAIN = b"h2hdb-cleanup-target-v1\0"
_CHAIN_DOMAIN = b"h2hdb-cleanup-chain-v1\0"
_INPUT_DOMAIN = b"h2hdb-cleanup-input-v1\0"
_FROZEN_ROOT_SET_DOMAIN = b"h2hdb-cleanup-frozen-root-set-v1\0"
_MAX_BATCH_ROWS = 256
_MAX_FROZEN_ROOT_KEY_BYTES = 260
_CLEANUP_ALGORITHM_VERSION = 2
_EMPTY_CURSOR = b""

_SWEEP_TABLE = "operational_cleanup_sweep_targets"
_PHASE_TABLE = "operational_cleanup_phases"
_JOB_TABLE = "operational_cleanup_jobs"
_CHECKPOINT_TABLE = "operational_cleanup_checkpoints"
_RECEIPT_TABLE = "operational_cleanup_batch_receipts"
_COMPLETION_TABLE = "operational_cleanup_completions"
_FROZEN_ROOT_TABLE = "operational_cleanup_cycle_roots"


class CleanupTargetKind(StrEnum):
    SOURCE_BUILD = "SOURCE_BUILD"
    ANALYSIS_RUN = "ANALYSIS_RUN"
    CATALOG_PUBLICATION = "CATALOG_PUBLICATION"
    PUBLICATION_COMMIT = "PUBLICATION_COMMIT"
    CATALOG_REVISION_DESCRIPTOR = "CATALOG_REVISION_DESCRIPTOR"
    SOURCE_REVISION_DESCRIPTOR = "SOURCE_REVISION_DESCRIPTOR"
    PUBLICATION_GENERATION = "PUBLICATION_GENERATION"
    PUBLICATION_CANDIDATE = "PUBLICATION_CANDIDATE"
    OPERATIONAL_PREPARATION = "OPERATIONAL_PREPARATION"
    GALLERY_OBSERVATION = "GALLERY_OBSERVATION"
    GALLERY_OBSERVATION_STAGING = "GALLERY_OBSERVATION_STAGING"
    ARTIFACT_BLOB = "ARTIFACT_BLOB"
    CANONICAL_VALUE = "CANONICAL_VALUE"
    CONTENT_BLOB = "CONTENT_BLOB"
    GALLERY_OBSERVATION_PAGE = "GALLERY_OBSERVATION_PAGE"
    FILE_NAME_IDENTITY = "FILE_NAME_IDENTITY"
    PUBLICATION_IDENTITY = "PUBLICATION_IDENTITY"
    GALLERY_IDENTITY = "GALLERY_IDENTITY"
    SOURCE_GALLERY_NAME_GID = "SOURCE_GALLERY_NAME_GID"
    GALLERY_UPLOAD_TIME = "GALLERY_UPLOAD_TIME"
    CANONICAL_VALUE_UPLOAD = "CANONICAL_VALUE_UPLOAD"
    HASH_CACHE_OBSERVATION = "HASH_CACHE_OBSERVATION"


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
    CleanupTargetKind.SOURCE_BUILD,
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
    CleanupTargetKind.GALLERY_OBSERVATION,
    CleanupTargetKind.ARTIFACT_BLOB,
    CleanupTargetKind.PUBLICATION_IDENTITY,
    CleanupTargetKind.GALLERY_IDENTITY,
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
    CleanupTargetKind.GALLERY_UPLOAD_TIME,
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


class CatalogPublicationMaintenanceState(StrEnum):
    """Optimistic current-only state used to avoid gate writes on idle polls."""

    DONE = "DONE"
    BLOCKED = "BLOCKED"
    ACTIONABLE = "ACTIONABLE"


class CleanupUnavailableError(RuntimeError):
    """A cleanup authority is stale, incomplete, or not installed."""


class CleanupCorruptionError(RuntimeError):
    """Durable cleanup control rows do not refine the closed contract."""


class CleanupRetentionBlockedError(CleanupUnavailableError):
    """A retention root appeared while a destructive batch was in flight."""


class CleanupCycleExhaustedError(OverflowError):
    """The fixed shard cycle generation cannot advance within int63."""


@dataclass(frozen=True, slots=True)
class CleanupCycle:
    cleanup_id: bytes
    target_kind: CleanupTargetKind
    shard_no: int
    target_key: bytes
    cycle_generation: int
    cycle_cutoff_at: int
    max_rows_per_transaction: int
    hash_cache_max_age_microseconds: int

    def __post_init__(self) -> None:
        require_uuid16(self.cleanup_id, field="cleanup_id")
        if not isinstance(self.target_kind, CleanupTargetKind):
            raise TypeError("target_kind must be a CleanupTargetKind")
        _require_shard(self.shard_no)
        require_digest32(self.target_key, field="cleanup target_key")
        require_positive_int63(self.cycle_generation, field="cleanup cycle_generation")
        require_int63(self.cycle_cutoff_at, field="cleanup cycle_cutoff_at")
        _require_batch_bound(self.max_rows_per_transaction)
        require_int63(
            self.hash_cache_max_age_microseconds,
            field="cleanup hash_cache_max_age_microseconds",
        )
        if self.cleanup_id != _cleanup_id(
            self.target_kind, self.shard_no, self.cycle_generation
        ):
            raise CleanupCorruptionError("cleanup_id does not match its fixed shard")
        if self.target_key != _target_key(self.target_kind, self.shard_no):
            raise CleanupCorruptionError("target_key does not match its fixed shard")


@dataclass(frozen=True, slots=True)
class CleanupBatchCommand:
    batch_key: bytes
    expected_generation: int

    def __post_init__(self) -> None:
        require_digest32(self.batch_key, field="cleanup batch_key")
        require_positive_int63(
            self.expected_generation, field="cleanup expected_generation"
        )


@dataclass(frozen=True, slots=True)
class CleanupBatchResult:
    cycle: CleanupCycle
    phase: str | None
    generation: int | None
    cursor: bytes
    deleted_count: int
    row_count: int
    phase_complete: bool
    cycle_complete: bool
    replayed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.cycle, CleanupCycle):
            raise TypeError("cycle must be a CleanupCycle")
        if self.phase is not None and self.phase not in _ALL_PHASES:
            raise CleanupCorruptionError("cleanup result has an unknown phase")
        if self.generation is not None:
            require_positive_int63(self.generation, field="cleanup result generation")
        require_bounded_bytes(self.cursor, field="cleanup result cursor", maximum=2048)
        require_int63(self.deleted_count, field="cleanup result deleted_count")
        require_int63(self.row_count, field="cleanup result row_count")
        if self.cycle_complete and (
            self.phase is not None or self.generation is not None
        ):
            raise CleanupCorruptionError(
                "a completed cleanup cycle cannot expose a live checkpoint"
            )


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    phase: str
    generation: int
    cursor: bytes
    deleted_count: int
    chain_sha256: bytes
    state: str
    receipt_batch_key: bytes | None
    receipt_generation: int | None
    receipt_row_count: int | None
    receipt_start_cursor: bytes | None
    receipt_next_cursor: bytes | None
    receipt_prior_chain_sha256: bytes | None
    receipt_prior_deleted_count: int | None
    receipt_input_sha256: bytes | None
    receipt_output_sha256: bytes | None
    receipt_committed_at: int | None


@dataclass(frozen=True, slots=True)
class _Mutation:
    next_cursor: bytes
    row_keys: tuple[bytes, ...]


_Mutator = Callable[[VNextUnitOfWork, CleanupCycle, bytes], _Mutation]


@dataclass(frozen=True, slots=True)
class _Strategy:
    phases: tuple[str, ...]
    mutators: tuple[_Mutator, ...]

    def __post_init__(self) -> None:
        if not self.phases or len(self.phases) != len(self.mutators):
            raise RuntimeError("cleanup strategy phases and mutators disagree")


class VNextCleanupRepository:
    """Run one fixed cleanup shard through bounded child-first phases."""

    @staticmethod
    def catalog_publication_maintenance_state(
        work: VNextUnitOfWork,
    ) -> CatalogPublicationMaintenanceState:
        """Optimistically classify current-only work without changing the gate.

        DONE and BLOCKED are safe hints: a concurrent publication/finalization
        merely defers cleanup to a later resident poll.  ACTIONABLE callers
        must still claim EXCLUSIVE and recheck under the gate before deleting.
        """

        interrupted = work.connector.fetch_one(
            f"SELECT 1 FROM {_SWEEP_TABLE} AS sweep "
            f"JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key "
            "WHERE sweep.target_kind = %s AND job.state = 'OPEN' LIMIT 1",
            (CleanupTargetKind.CATALOG_PUBLICATION.value,),
        )
        if interrupted and interrupted != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup interrupted-cycle probe returned an invalid shape"
            )
        if interrupted:
            return CatalogPublicationMaintenanceState.ACTIONABLE

        payload = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_occurrence_identities AS publication "
            "JOIN catalog_publication_commit_head_receipts AS head "
            "ON head.channel = %s "
            "JOIN catalog_publication_commits AS current "
            "ON current.receipt_id = head.receipt_id "
            "WHERE publication.revision < current.revision LIMIT 1",
            (b"default",),
        )
        if payload and payload != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup preflight returned an invalid shape"
            )
        if not payload:
            return CatalogPublicationMaintenanceState.DONE

        current = work.connector.fetch_one(
            "SELECT receipt.state, receipt.finalized_at "
            "FROM catalog_publication_commit_head_receipts AS head "
            "JOIN catalog_publication_receipts AS receipt "
            "ON receipt.receipt_id = head.receipt_id "
            "WHERE head.channel = %s",
            (b"default",),
        )
        if not current:
            return CatalogPublicationMaintenanceState.BLOCKED
        if len(current) != 2:
            raise CleanupCorruptionError(
                "catalog cleanup current-receipt probe returned an invalid shape"
            )
        state = _as_text(current[0], field="catalog cleanup current receipt state")
        if state != "PROJECTION_FINALIZED" or current[1] is None:
            return CatalogPublicationMaintenanceState.BLOCKED
        require_int63(current[1], field="catalog cleanup current finalized_at")

        candidate = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_occurrence_identities AS r "
            f"WHERE ({_CATALOG_PUBLICATION_ELIGIBILITY}) LIMIT 1"
        )
        if candidate and candidate != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup actionable probe returned an invalid shape"
            )
        if candidate:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        return CatalogPublicationMaintenanceState.BLOCKED

    @staticmethod
    def catalog_publication_next_maintenance_shard(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        now: int,
    ) -> int | None:
        """Return the next actionable shard, prioritizing interrupted work."""

        timestamp = require_int63(now, field="catalog cleanup next-shard now")
        _require_exclusive_gate(work, gate_lease, now=timestamp)
        _validate_strategy_seeds(work, CleanupTargetKind.CATALOG_PUBLICATION)

        interrupted = work.connector.fetch_one(
            f"SELECT sweep.shard_no FROM {_SWEEP_TABLE} AS sweep "
            f"JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key "
            "WHERE sweep.target_kind = %s AND job.state = 'OPEN' "
            "ORDER BY sweep.shard_no LIMIT 1",
            (CleanupTargetKind.CATALOG_PUBLICATION.value,),
        )
        if interrupted:
            if len(interrupted) != 1:
                raise CleanupCorruptionError(
                    "catalog cleanup interrupted-shard probe returned an invalid shape"
                )
            return _require_shard(interrupted[0])

        candidate = work.connector.fetch_one(
            "SELECT r.publication_key "
            "FROM catalog_publication_occurrence_identities AS r "
            f"WHERE ({_CATALOG_PUBLICATION_ELIGIBILITY}) "
            "ORDER BY r.publication_key LIMIT 1"
        )
        if not candidate:
            return None
        if len(candidate) != 1:
            raise CleanupCorruptionError(
                "catalog cleanup next-candidate probe returned an invalid shape"
            )
        publication_key = require_digest32(
            candidate[0], field="catalog cleanup candidate publication_key"
        )
        return publication_key[0]

    @staticmethod
    def catalog_publication_maintenance_required(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        now: int,
    ) -> bool:
        """Return whether historical payload or an interrupted shard remains."""

        timestamp = require_int63(now, field="catalog cleanup preflight now")
        _require_exclusive_gate(work, gate_lease, now=timestamp)
        return (
            VNextCleanupRepository.catalog_publication_maintenance_state(work)
            is not CatalogPublicationMaintenanceState.DONE
        )

    @staticmethod
    def current_only_maintenance_state(
        work: VNextUnitOfWork,
        *,
        cycle_cutoff_at: int,
        gate_lease: GateLease | None = None,
        now: int | None = None,
    ) -> CatalogPublicationMaintenanceState:
        """Classify the gallery/CBZ fixed point across 21 of 22 targets.

        New ``HASH_CACHE_OBSERVATION`` work is deliberately excluded.  It is a
        file-derived, age-based cache policy rather than gallery/CBZ history;
        including it with a zero max age would make every resident idle poll
        delete newly observed cache rows and prevent a stable fixed point.  An
        already OPEN hash-cache cycle is nevertheless surfaced as ACTIONABLE
        so the sole serialized cleanup authority can always be handed off and
        completed after process loss.

        A lease-free call is an optimistic read-only hint.  Supplying the
        exact EXCLUSIVE lease and ``now`` performs the same exact probes under
        the destructive maintenance fence.
        """

        cutoff = require_int63(
            cycle_cutoff_at, field="current-only maintenance cycle_cutoff_at"
        )
        if (gate_lease is None) != (now is None):
            raise TypeError("gate_lease and now must be supplied together")
        if gate_lease is not None:
            assert now is not None
            timestamp = require_int63(now, field="current-only maintenance state now")
            _require_exclusive_gate(work, gate_lease, now=timestamp)

        if _load_open_current_only_cycle(work) is not None:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        if _next_current_only_candidate(work, cycle_cutoff_at=cutoff) is not None:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        if _catalog_publication_payload_is_blocked(
            work
        ) or _publication_candidate_payload_is_blocked(work):
            return CatalogPublicationMaintenanceState.BLOCKED
        return CatalogPublicationMaintenanceState.DONE

    @staticmethod
    def next_current_only_cycle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cycle_cutoff_at: int,
        now: int,
    ) -> CleanupCycle | None:
        """Resume the sole OPEN job or begin the first actionable target.

        The exact candidate probe and cycle creation share the EXCLUSIVE
        transaction.  A completed cycle is followed by a new call, which
        restarts the priority scan from the first target and therefore reaches
        a dependency fixed point without walking all 5,376 target shards.  A
        previously opened hash-cache cycle is resumed as a liveness handoff;
        this path never starts new hash-cache work.
        """

        timestamp = require_int63(now, field="current-only maintenance next now")
        cutoff = require_int63(
            cycle_cutoff_at, field="current-only maintenance cycle_cutoff_at"
        )
        _require_exclusive_gate(work, gate_lease, now=timestamp)

        interrupted = _load_open_current_only_cycle(work)
        if interrupted is not None:
            _validate_strategy_seeds(work, interrupted.target_kind)
            return interrupted

        candidate = _next_current_only_candidate(work, cycle_cutoff_at=cutoff)
        if candidate is None:
            return None
        kind, shard = candidate
        return _begin_cycle_under_exclusive(
            work,
            kind=kind,
            shard=shard,
            cutoff=cutoff,
            max_rows=_MAX_BATCH_ROWS,
            max_age=0,
            now=timestamp,
        )

    @staticmethod
    def begin_cycle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        target_kind: CleanupTargetKind,
        shard_no: int,
        cycle_cutoff_at: int,
        max_rows_per_transaction: int = _MAX_BATCH_ROWS,
        hash_cache_max_age_microseconds: int = 0,
        now: int,
    ) -> CleanupCycle:
        kind = _require_supported_kind(target_kind)
        shard = _require_shard(shard_no)
        cutoff = require_int63(cycle_cutoff_at, field="cleanup cycle_cutoff_at")
        max_rows = _require_batch_bound(max_rows_per_transaction)
        max_age = require_int63(
            hash_cache_max_age_microseconds,
            field="cleanup hash_cache_max_age_microseconds",
        )
        timestamp = require_int63(now, field="cleanup begin now")
        if kind == CleanupTargetKind.HASH_CACHE_OBSERVATION and max_age > cutoff:
            raise ValueError("hash-cache max age cannot exceed the cycle cutoff")
        if kind != CleanupTargetKind.HASH_CACHE_OBSERVATION and max_age != 0:
            raise ValueError("hash-cache max age is only valid for hash-cache cleanup")
        _require_exclusive_gate(work, gate_lease, now=timestamp)
        return _begin_cycle_under_exclusive(
            work,
            kind=kind,
            shard=shard,
            cutoff=cutoff,
            max_rows=max_rows,
            max_age=max_age,
            now=timestamp,
        )

    @staticmethod
    def resume_cycle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cycle: CleanupCycle,
        now: int,
    ) -> CleanupBatchResult:
        requested = _require_cycle(cycle)
        timestamp = require_int63(now, field="cleanup resume now")
        _require_exclusive_gate(work, gate_lease, now=timestamp)
        job, checkpoint = _lock_cycle(work, requested)
        if job == "COMPLETE":
            deleted_count = _require_completion_row(work, requested)
            return _complete_result(
                requested, deleted_count=deleted_count, replayed=True
            )
        if checkpoint is None:
            raise CleanupCorruptionError("OPEN cleanup cycle lacks a checkpoint")
        if requested.target_kind is CleanupTargetKind.PUBLICATION_COMMIT and (
            checkpoint.phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}
        ):
            _require_publication_commit_post_compound_transition(
                work,
                cycle=requested,
                phase=checkpoint.phase,
                cursor=checkpoint.cursor,
            )
        return _checkpoint_result(requested, checkpoint, row_count=0, replayed=True)

    @staticmethod
    def advance(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cycle: CleanupCycle,
        command: CleanupBatchCommand,
        now: int,
    ) -> CleanupBatchResult:
        requested = _require_cycle(cycle)
        attempt = _require_command(command)
        timestamp = require_int63(now, field="cleanup advance now")
        _require_exclusive_gate(work, gate_lease, now=timestamp)
        job, checkpoint = _lock_cycle(work, requested)
        if job == "COMPLETE":
            deleted_count = _require_completion_row(work, requested)
            return _complete_result(
                requested, deleted_count=deleted_count, replayed=True
            )
        if checkpoint is None:
            raise CleanupCorruptionError("OPEN cleanup cycle lacks a checkpoint")
        if requested.target_kind is CleanupTargetKind.PUBLICATION_COMMIT and (
            checkpoint.phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}
        ):
            _require_publication_commit_post_compound_transition(
                work,
                cycle=requested,
                phase=checkpoint.phase,
                cursor=checkpoint.cursor,
            )

        if (
            checkpoint.generation == attempt.expected_generation + 1
            and checkpoint.receipt_batch_key == attempt.batch_key
            and checkpoint.receipt_generation == checkpoint.generation
        ):
            return _checkpoint_result(
                requested,
                checkpoint,
                row_count=checkpoint.receipt_row_count or 0,
                replayed=True,
            )
        transition_replay = _terminal_transition_replay(
            work, requested, checkpoint, attempt
        )
        if transition_replay is not None:
            return transition_replay
        if checkpoint.generation != attempt.expected_generation:
            raise CleanupUnavailableError("cleanup checkpoint generation is stale")
        if checkpoint.state != "OPEN":
            raise CleanupCorruptionError(
                "a COMPLETE cleanup checkpoint lacks its exact terminal replay"
            )
        if checkpoint.generation == INT63_MAX:
            raise CleanupCycleExhaustedError(
                "cleanup checkpoint generation reached portable int63 maximum"
            )

        strategy = _STRATEGIES[requested.target_kind]
        try:
            phase_index = strategy.phases.index(checkpoint.phase)
        except ValueError as error:
            raise CleanupCorruptionError(
                "cleanup checkpoint phase is not registered for its target"
            ) from error
        mutation = strategy.mutators[phase_index](work, requested, checkpoint.cursor)
        next_generation = checkpoint.generation + 1
        row_count = len(mutation.row_keys)
        next_deleted_count = checkpoint.deleted_count + row_count
        require_int63(next_deleted_count, field="cleanup deleted_count")
        input_sha256 = _input_digest(
            requested, checkpoint.phase, checkpoint.cursor, mutation.row_keys
        )
        next_chain = _next_chain(
            checkpoint.chain_sha256,
            checkpoint.phase,
            next_generation,
            checkpoint.cursor,
            mutation.next_cursor,
            input_sha256,
            row_count,
        )
        terminal = row_count == 0

        work.connector.execute(
            f"DELETE FROM {_RECEIPT_TABLE} WHERE cleanup_id = %s AND phase = %s",
            (requested.cleanup_id, checkpoint.phase),
        )
        work.connector.execute(
            f"""
            INSERT INTO {_RECEIPT_TABLE}
                (cleanup_id, phase, batch_key, start_cursor, next_cursor,
                 prior_chain_sha256, prior_deleted_count,
                 input_sha256, output_sha256, row_count,
                 committed_generation, committed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                requested.cleanup_id,
                checkpoint.phase,
                attempt.batch_key,
                checkpoint.cursor,
                mutation.next_cursor,
                checkpoint.chain_sha256,
                checkpoint.deleted_count,
                input_sha256,
                next_chain,
                row_count,
                next_generation,
                timestamp,
            ),
        )
        work.compare_and_swap(
            f"""
            UPDATE {_CHECKPOINT_TABLE}
            SET generation = %s, cursor_bytes = %s, deleted_count = %s,
                chain_sha256 = %s, state = %s, updated_at = %s
            WHERE cleanup_id = %s AND phase = %s AND generation = %s
              AND cursor_bytes = %s AND deleted_count = %s
              AND chain_sha256 = %s AND state = 'OPEN'
            """,
            (
                next_generation,
                mutation.next_cursor,
                next_deleted_count,
                next_chain,
                "COMPLETE" if terminal else "OPEN",
                timestamp,
                requested.cleanup_id,
                checkpoint.phase,
                checkpoint.generation,
                checkpoint.cursor,
                checkpoint.deleted_count,
                checkpoint.chain_sha256,
            ),
            authority="cleanup checkpoint",
        )

        if not terminal:
            return CleanupBatchResult(
                cycle=requested,
                phase=checkpoint.phase,
                generation=next_generation,
                cursor=mutation.next_cursor,
                deleted_count=next_deleted_count,
                row_count=row_count,
                phase_complete=False,
                cycle_complete=False,
                replayed=False,
            )

        if phase_index + 1 < len(strategy.phases):
            next_phase = strategy.phases[phase_index + 1]
            _validate_phase_seed(
                work, requested.target_kind, next_phase, phase_index + 2
            )
            _insert_checkpoint(
                work,
                cycle=requested,
                phase=next_phase,
                chain_sha256=_phase_chain(next_chain, next_phase),
                now=timestamp,
            )
            return CleanupBatchResult(
                cycle=requested,
                phase=next_phase,
                generation=1,
                cursor=_EMPTY_CURSOR,
                deleted_count=0,
                row_count=0,
                phase_complete=True,
                cycle_complete=False,
                replayed=False,
            )

        total_deleted = _fixed_checkpoint_total(work, requested, strategy.phases)
        _complete_cycle(
            work,
            cycle=requested,
            final_chain_sha256=next_chain,
            deleted_count=total_deleted,
            now=timestamp,
        )
        return _complete_result(
            requested,
            row_count=0,
            deleted_count=total_deleted,
            replayed=False,
        )


def _require_serialized_open_cycle(
    work: VNextUnitOfWork,
    *,
    requested_target_key: bytes,
) -> None:
    rows = work.connector.fetch_all(
        f"SELECT target_key FROM {_JOB_TABLE} "
        "WHERE state = 'OPEN' ORDER BY target_key LIMIT 2"
    )
    if len(rows) > 1:
        raise CleanupCorruptionError(
            "serialized cleanup protocol has multiple OPEN cycles"
        )
    if not rows:
        return
    if len(rows[0]) != 1:
        raise CleanupCorruptionError("OPEN cleanup cycle probe returned invalid shape")
    open_target_key = require_digest32(rows[0][0], field="OPEN cleanup target_key")
    if open_target_key != requested_target_key:
        raise CleanupUnavailableError(
            "another serialized cleanup cycle must complete first"
        )


def _begin_cycle_under_exclusive(
    work: VNextUnitOfWork,
    *,
    kind: CleanupTargetKind,
    shard: int,
    cutoff: int,
    max_rows: int,
    max_age: int,
    now: int,
) -> CleanupCycle:
    """Begin after the caller has fenced this transaction exactly once."""

    _validate_strategy_seeds(work, kind)
    expected_target_key = _target_key(kind, shard)
    _require_serialized_open_cycle(
        work,
        requested_target_key=expected_target_key,
    )
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("cleanup-sweep", expected_target_key),
        f"""
        SELECT s.target_key,
               j.cleanup_id, j.cycle_generation, j.cycle_cutoff_at,
               j.algorithm_version, j.max_rows_per_transaction,
               j.hash_cache_max_age_microseconds,
               j.frozen_root_count, j.frozen_root_set_sha256,
               j.state, j.created_at, j.completed_at,
               c.cycle_generation, c.final_chain_sha256, c.deleted_count
        FROM {_SWEEP_TABLE} AS s
        LEFT JOIN {_JOB_TABLE} AS j ON j.target_key = s.target_key
        LEFT JOIN {_COMPLETION_TABLE} AS c ON c.target_key = s.target_key
        WHERE s.target_kind = %s AND s.shard_no = %s
        """,
        (kind.value, shard),
    )
    if not row or row[0] != expected_target_key:
        raise CleanupCorruptionError("fixed cleanup sweep seed is missing or corrupt")

    if row[1] is None:
        generation = 1
    else:
        existing = _cycle_from_job_row(kind, shard, row)
        state = _as_text(row[9], field="cleanup job state")
        if state == "OPEN":
            _require_cycle_policy(
                existing,
                cutoff=cutoff,
                max_rows=max_rows,
                max_age=max_age,
            )
            _load_frozen_roots(work, existing, _STATIC_PLANS.get(kind))
            return existing
        if state != "COMPLETE":
            raise CleanupCorruptionError("cleanup job has an invalid state")
        _require_complete_job(row, existing)
        _require_no_frozen_roots(work, existing.cleanup_id)
        if existing.cycle_generation == INT63_MAX:
            raise CleanupCycleExhaustedError(
                "cleanup cycle generation reached portable int63 maximum"
            )
        generation = existing.cycle_generation + 1
        work.connector.execute(
            f"DELETE FROM {_COMPLETION_TABLE} WHERE target_key = %s",
            (expected_target_key,),
        )
        affected = work.connector.execute_affected(
            f"DELETE FROM {_JOB_TABLE} "
            "WHERE cleanup_id = %s AND target_key = %s "
            "AND cycle_generation = %s AND state = 'COMPLETE'",
            (
                existing.cleanup_id,
                existing.target_key,
                existing.cycle_generation,
            ),
        )
        if affected != 1:
            raise CleanupUnavailableError("completed cleanup cycle changed")

    cleanup_id = _cleanup_id(kind, shard, generation)
    cycle = CleanupCycle(
        cleanup_id=cleanup_id,
        target_kind=kind,
        shard_no=shard,
        target_key=expected_target_key,
        cycle_generation=generation,
        cycle_cutoff_at=cutoff,
        max_rows_per_transaction=max_rows,
        hash_cache_max_age_microseconds=max_age,
    )
    work.connector.execute(
        f"""
        INSERT INTO {_JOB_TABLE}
            (cleanup_id, target_key, cycle_generation, cycle_cutoff_at,
             algorithm_version, max_rows_per_transaction,
             hash_cache_max_age_microseconds,
             frozen_root_count, frozen_root_set_sha256,
             state, created_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, 'OPEN', %s, NULL)
        """,
        (
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
            cycle.cycle_cutoff_at,
            _CLEANUP_ALGORITHM_VERSION,
            cycle.max_rows_per_transaction,
            cycle.hash_cache_max_age_microseconds,
            _frozen_root_set_sha256(cycle.cleanup_id, ()),
            now,
        ),
    )
    _freeze_static_cycle_roots(work, cycle)
    first_phase = _STRATEGIES[kind].phases[0]
    _insert_checkpoint(
        work,
        cycle=cycle,
        phase=first_phase,
        chain_sha256=_initial_chain(cycle.cleanup_id, first_phase),
        now=now,
    )
    return cycle


def _require_supported_kind(value: object) -> CleanupTargetKind:
    if not isinstance(value, CleanupTargetKind):
        raise TypeError("target_kind must be a CleanupTargetKind")
    if value not in _STRATEGIES:
        raise CleanupUnavailableError(
            f"cleanup strategy {value.value!r} is not installed"
        )
    return value


def _require_cycle(value: object) -> CleanupCycle:
    if type(value) is not CleanupCycle:
        raise TypeError("cycle must be an exact CleanupCycle")
    assert isinstance(value, CleanupCycle)
    value.__post_init__()
    _require_supported_kind(value.target_kind)
    return value


def _require_command(value: object) -> CleanupBatchCommand:
    if type(value) is not CleanupBatchCommand:
        raise TypeError("command must be an exact CleanupBatchCommand")
    assert isinstance(value, CleanupBatchCommand)
    value.__post_init__()
    return value


def _require_shard(value: object) -> int:
    shard = require_int63(value, field="cleanup shard_no")
    if shard > 255:
        raise ValueError("cleanup shard_no must be in 0..255")
    return shard


def _require_batch_bound(value: object) -> int:
    bound = require_positive_int63(value, field="cleanup max_rows_per_transaction")
    if bound > _MAX_BATCH_ROWS:
        raise ValueError(f"cleanup batches are capped at {_MAX_BATCH_ROWS} rows")
    return bound


def _require_exclusive_gate(
    work: VNextUnitOfWork, lease: GateLease, *, now: int
) -> None:
    current = MaintenanceGateRepository.lock_and_require_live(work, lease, now=now)
    if current.mode != GateMode.EXCLUSIVE or current.slots != tuple(range(64)):
        raise CleanupUnavailableError("cleanup requires the exact EXCLUSIVE gate")


def _cleanup_id(kind: CleanupTargetKind, shard_no: int, generation: int) -> bytes:
    tag = hashlib.sha256(_CLEANUP_ID_DOMAIN + kind.value.encode("ascii")).digest()[:7]
    return (
        tag
        + bytes((_require_shard(shard_no),))
        + require_positive_int63(generation, field="cleanup generation").to_bytes(
            8, "big"
        )
    )


def _target_key(kind: CleanupTargetKind, shard_no: int) -> bytes:
    tag = hashlib.sha256(_TARGET_KEY_DOMAIN + kind.value.encode("ascii")).digest()[:16]
    shard = _require_shard(shard_no)
    return tag + shard.to_bytes(8, "big") + bytes(8)


def _cycle_from_job_row(
    kind: CleanupTargetKind, shard_no: int, row: tuple[object, ...]
) -> CleanupCycle:
    if require_int63(row[4], field="stored cleanup algorithm_version") != (
        _CLEANUP_ALGORITHM_VERSION
    ):
        raise CleanupCorruptionError("cleanup algorithm_version is unsupported")
    return CleanupCycle(
        cleanup_id=require_uuid16(row[1], field="stored cleanup_id"),
        target_kind=kind,
        shard_no=shard_no,
        target_key=require_digest32(row[0], field="stored target_key"),
        cycle_generation=require_positive_int63(
            row[2], field="stored cycle_generation"
        ),
        cycle_cutoff_at=require_int63(row[3], field="stored cycle_cutoff_at"),
        max_rows_per_transaction=_require_batch_bound(row[5]),
        hash_cache_max_age_microseconds=require_int63(
            row[6], field="stored hash_cache_max_age_microseconds"
        ),
    )


def _as_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise CleanupCorruptionError(f"{field} must be text")
    return value


def _require_cycle_policy(
    cycle: CleanupCycle, *, cutoff: int, max_rows: int, max_age: int
) -> None:
    if (
        cycle.cycle_cutoff_at != cutoff
        or cycle.max_rows_per_transaction != max_rows
        or cycle.hash_cache_max_age_microseconds != max_age
    ):
        raise CleanupUnavailableError(
            "an OPEN cleanup cycle exists with different immutable policy"
        )


def _require_complete_job(row: tuple[object, ...], cycle: CleanupCycle) -> None:
    if row[11] is None or row[12] != cycle.cycle_generation:
        raise CleanupCorruptionError("COMPLETE cleanup job lacks exact completion")
    _require_frozen_root_count(row[7])
    require_digest32(row[8], field="cleanup frozen_root_set_sha256")
    require_digest32(row[13], field="cleanup final_chain_sha256")
    require_int63(row[14], field="cleanup completion deleted_count")


def _insert_checkpoint(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
    phase: str,
    chain_sha256: bytes,
    now: int,
) -> None:
    _validate_phase_seed(work, cycle.target_kind, phase, _phase_order(cycle, phase))
    work.connector.execute(
        f"""
        INSERT INTO {_CHECKPOINT_TABLE}
            (cleanup_id, phase, generation, cursor_bytes, deleted_count,
             chain_sha256, state, updated_at)
        VALUES (%s, %s, 1, %s, 0, %s, 'OPEN', %s)
        """,
        (cycle.cleanup_id, phase, _EMPTY_CURSOR, chain_sha256, now),
    )


def _phase_order(cycle: CleanupCycle, phase: str) -> int:
    try:
        return _STRATEGIES[cycle.target_kind].phases.index(phase) + 1
    except ValueError as error:
        raise CleanupCorruptionError("cleanup phase is not registered") from error


def _validate_phase_seed(
    work: VNextUnitOfWork,
    kind: CleanupTargetKind,
    phase: str,
    expected_order: int,
) -> None:
    row = work.connector.fetch_one(
        f"SELECT target_kind, phase_order FROM {_PHASE_TABLE} WHERE phase = %s",
        (phase,),
    )
    if row != (kind.value, expected_order):
        raise CleanupCorruptionError("fixed cleanup phase seed is missing or corrupt")


def _validate_strategy_seeds(work: VNextUnitOfWork, kind: CleanupTargetKind) -> None:
    expected = tuple(
        (phase, order) for order, phase in enumerate(_STRATEGIES[kind].phases, start=1)
    )
    rows = work.connector.fetch_all(
        f"SELECT phase, phase_order FROM {_PHASE_TABLE} "
        "WHERE target_kind = %s ORDER BY phase_order LIMIT %s",
        (kind.value, len(expected) + 1),
    )
    if tuple(rows) != expected:
        raise CleanupCorruptionError("fixed cleanup phase seed set is corrupt")


def _lock_cycle(
    work: VNextUnitOfWork, cycle: CleanupCycle
) -> tuple[str, _Checkpoint | None]:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("cleanup-cycle", cycle.target_key),
        f"""
        SELECT j.cleanup_id, j.cycle_generation, j.cycle_cutoff_at,
               j.algorithm_version, j.max_rows_per_transaction,
               j.hash_cache_max_age_microseconds,
               j.frozen_root_count, j.frozen_root_set_sha256, j.state,
               p.phase, p.generation, p.cursor_bytes, p.deleted_count,
               p.chain_sha256, p.state,
               r.batch_key, r.start_cursor, r.next_cursor,
               r.prior_chain_sha256, r.prior_deleted_count,
               r.input_sha256, r.output_sha256, r.row_count,
               r.committed_generation, r.committed_at
        FROM {_JOB_TABLE} AS j
        LEFT JOIN {_CHECKPOINT_TABLE} AS p ON p.cleanup_id = j.cleanup_id
        LEFT JOIN {_PHASE_TABLE} AS cp ON cp.phase = p.phase
        LEFT JOIN {_RECEIPT_TABLE} AS r
          ON r.cleanup_id = p.cleanup_id AND r.phase = p.phase
        WHERE j.target_key = %s
          AND (p.cleanup_id IS NULL OR p.state = 'OPEN'
               OR NOT EXISTS (
                   SELECT 1 FROM {_CHECKPOINT_TABLE} AS later
                   JOIN {_PHASE_TABLE} AS lp ON lp.phase = later.phase
                   JOIN {_PHASE_TABLE} AS cp ON cp.phase = p.phase
                   WHERE later.cleanup_id = p.cleanup_id
                     AND lp.phase_order > cp.phase_order
               ))
        ORDER BY cp.phase_order
        LIMIT 2
        """,
        (cycle.target_key,),
    )
    if not row:
        raise CleanupUnavailableError("cleanup cycle is missing")
    if row[0] != cycle.cleanup_id or row[1] != cycle.cycle_generation:
        raise CleanupUnavailableError("cleanup cycle capability is stale")
    if row[2] != cycle.cycle_cutoff_at or row[3] != _CLEANUP_ALGORITHM_VERSION:
        raise CleanupCorruptionError("cleanup cycle immutable policy changed")
    if (
        row[4] != cycle.max_rows_per_transaction
        or row[5] != cycle.hash_cache_max_age_microseconds
    ):
        raise CleanupCorruptionError("cleanup cycle batch policy changed")
    frozen_count = _require_frozen_root_count(row[6])
    frozen_digest = require_digest32(row[7], field="cleanup frozen_root_set_sha256")
    state = _as_text(row[8], field="cleanup job state")
    if state == "COMPLETE":
        if row[9] is not None:
            raise CleanupCorruptionError("COMPLETE cleanup retains a checkpoint")
        _require_no_frozen_roots(work, cycle.cleanup_id)
        return state, None
    if state != "OPEN" or row[9] is None:
        raise CleanupCorruptionError("OPEN cleanup lacks one current checkpoint")
    _load_frozen_roots(
        work,
        cycle,
        _STATIC_PLANS.get(cycle.target_kind),
        expected_count=frozen_count,
        expected_digest=frozen_digest,
    )
    checkpoint = _Checkpoint(
        phase=_as_text(row[9], field="cleanup phase"),
        generation=require_positive_int63(
            row[10], field="cleanup checkpoint generation"
        ),
        cursor=require_bounded_bytes(row[11], field="cleanup cursor", maximum=2048),
        deleted_count=require_int63(row[12], field="cleanup deleted_count"),
        chain_sha256=require_digest32(row[13], field="cleanup chain_sha256"),
        state=_as_text(row[14], field="cleanup checkpoint state"),
        receipt_batch_key=(
            None
            if row[15] is None
            else require_digest32(row[15], field="cleanup receipt batch_key")
        ),
        receipt_generation=(
            None
            if row[23] is None
            else require_positive_int63(row[23], field="cleanup receipt generation")
        ),
        receipt_row_count=(
            None
            if row[22] is None
            else require_int63(row[22], field="cleanup receipt row_count")
        ),
        receipt_start_cursor=(
            None
            if row[16] is None
            else require_bounded_bytes(
                row[16], field="cleanup receipt start_cursor", maximum=2048
            )
        ),
        receipt_next_cursor=(
            None
            if row[17] is None
            else require_bounded_bytes(
                row[17], field="cleanup receipt next_cursor", maximum=2048
            )
        ),
        receipt_input_sha256=(
            None
            if row[20] is None
            else require_digest32(row[20], field="cleanup receipt input_sha256")
        ),
        receipt_output_sha256=(
            None
            if row[21] is None
            else require_digest32(row[21], field="cleanup receipt output_sha256")
        ),
        receipt_prior_chain_sha256=(
            None
            if row[18] is None
            else require_digest32(row[18], field="cleanup receipt prior_chain_sha256")
        ),
        receipt_prior_deleted_count=(
            None
            if row[19] is None
            else require_int63(row[19], field="cleanup receipt prior_deleted_count")
        ),
        receipt_committed_at=(
            None
            if row[24] is None
            else require_int63(row[24], field="cleanup receipt committed_at")
        ),
    )
    _validate_checkpoint_receipt(cycle, checkpoint)
    return state, checkpoint


def _validate_checkpoint_receipt(cycle: CleanupCycle, checkpoint: _Checkpoint) -> None:
    fields = (
        checkpoint.receipt_batch_key,
        checkpoint.receipt_generation,
        checkpoint.receipt_row_count,
        checkpoint.receipt_start_cursor,
        checkpoint.receipt_next_cursor,
        checkpoint.receipt_prior_chain_sha256,
        checkpoint.receipt_prior_deleted_count,
        checkpoint.receipt_input_sha256,
        checkpoint.receipt_output_sha256,
        checkpoint.receipt_committed_at,
    )
    if all(value is None for value in fields):
        if checkpoint.generation != 1:
            raise CleanupCorruptionError(
                "advanced cleanup checkpoint lacks its latest receipt"
            )
        return
    if any(value is None for value in fields):
        raise CleanupCorruptionError("cleanup latest receipt is incomplete")
    assert checkpoint.receipt_generation is not None
    assert checkpoint.receipt_row_count is not None
    assert checkpoint.receipt_start_cursor is not None
    assert checkpoint.receipt_next_cursor is not None
    assert checkpoint.receipt_prior_chain_sha256 is not None
    assert checkpoint.receipt_prior_deleted_count is not None
    assert checkpoint.receipt_input_sha256 is not None
    assert checkpoint.receipt_output_sha256 is not None
    expected_deleted_count = require_int63(
        checkpoint.receipt_prior_deleted_count + checkpoint.receipt_row_count,
        field="cleanup receipt next deleted_count",
    )
    expected_output_sha256 = _next_chain(
        checkpoint.receipt_prior_chain_sha256,
        checkpoint.phase,
        checkpoint.receipt_generation,
        checkpoint.receipt_start_cursor,
        checkpoint.receipt_next_cursor,
        checkpoint.receipt_input_sha256,
        checkpoint.receipt_row_count,
    )
    if (
        checkpoint.receipt_generation != checkpoint.generation
        or checkpoint.receipt_next_cursor != checkpoint.cursor
        or expected_output_sha256 != checkpoint.receipt_output_sha256
        or checkpoint.receipt_output_sha256 != checkpoint.chain_sha256
        or checkpoint.receipt_row_count > cycle.max_rows_per_transaction
        or checkpoint.deleted_count != expected_deleted_count
    ):
        raise CleanupCorruptionError(
            "cleanup latest receipt does not match its checkpoint"
        )
    terminal = checkpoint.receipt_row_count == 0
    if terminal != (checkpoint.state == "COMPLETE"):
        raise CleanupCorruptionError(
            "cleanup terminal receipt and checkpoint state disagree"
        )
    if terminal != (checkpoint.receipt_start_cursor == checkpoint.receipt_next_cursor):
        raise CleanupCorruptionError(
            "cleanup receipt cursor movement does not match terminal state"
        )


def _terminal_transition_replay(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    checkpoint: _Checkpoint,
    command: CleanupBatchCommand,
) -> CleanupBatchResult | None:
    if (
        command.expected_generation == INT63_MAX
        or checkpoint.generation != 1
        or checkpoint.cursor
        or checkpoint.deleted_count != 0
        or checkpoint.state != "OPEN"
        or checkpoint.receipt_batch_key is not None
    ):
        return None
    rows = work.connector.fetch_all(
        f"""
        SELECT r.phase, r.batch_key, r.start_cursor, r.next_cursor,
               r.prior_chain_sha256, r.prior_deleted_count,
               r.input_sha256, r.output_sha256, r.row_count,
               r.committed_generation, r.committed_at,
               prior.generation, prior.cursor_bytes, prior.deleted_count,
               prior.chain_sha256, prior.state,
               prior_seed.phase_order, current_seed.phase_order
        FROM {_RECEIPT_TABLE} AS r
        JOIN {_CHECKPOINT_TABLE} AS prior
          ON prior.cleanup_id = r.cleanup_id AND prior.phase = r.phase
        JOIN {_PHASE_TABLE} AS prior_seed ON prior_seed.phase = r.phase
        JOIN {_PHASE_TABLE} AS current_seed ON current_seed.phase = %s
        WHERE r.cleanup_id = %s AND r.batch_key = %s
          AND r.committed_generation = %s
          AND current_seed.phase_order = prior_seed.phase_order + 1
        ORDER BY prior_seed.phase_order
        LIMIT 2
        """,
        (
            checkpoint.phase,
            cycle.cleanup_id,
            command.batch_key,
            command.expected_generation + 1,
        ),
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise CleanupCorruptionError("cleanup terminal replay is ambiguous")
    row = rows[0]
    prior_phase = _as_text(row[0], field="cleanup terminal replay phase")
    batch_key = require_digest32(row[1], field="cleanup terminal replay batch_key")
    start_cursor = require_bounded_bytes(
        row[2], field="cleanup terminal replay start_cursor", maximum=2048
    )
    next_cursor = require_bounded_bytes(
        row[3], field="cleanup terminal replay next_cursor", maximum=2048
    )
    receipt_prior_chain = require_digest32(
        row[4], field="cleanup terminal replay prior_chain_sha256"
    )
    receipt_prior_deleted = require_int63(
        row[5], field="cleanup terminal replay prior_deleted_count"
    )
    input_sha256 = require_digest32(
        row[6], field="cleanup terminal replay input_sha256"
    )
    output_sha256 = require_digest32(
        row[7], field="cleanup terminal replay output_sha256"
    )
    row_count = require_int63(row[8], field="cleanup terminal replay row_count")
    receipt_generation = require_positive_int63(
        row[9], field="cleanup terminal replay receipt generation"
    )
    require_int63(row[10], field="cleanup terminal replay committed_at")
    prior_generation = require_positive_int63(
        row[11], field="cleanup terminal checkpoint generation"
    )
    prior_cursor = require_bounded_bytes(
        row[12], field="cleanup terminal checkpoint cursor", maximum=2048
    )
    checkpoint_deleted = require_int63(
        row[13], field="cleanup terminal checkpoint deleted_count"
    )
    checkpoint_chain = require_digest32(
        row[14], field="cleanup terminal checkpoint chain"
    )
    prior_state = _as_text(row[15], field="cleanup terminal checkpoint state")
    prior_order = require_positive_int63(
        row[16], field="cleanup terminal prior phase_order"
    )
    current_order = require_positive_int63(
        row[17], field="cleanup terminal current phase_order"
    )
    expected_output = _next_chain(
        receipt_prior_chain,
        prior_phase,
        receipt_generation,
        start_cursor,
        next_cursor,
        input_sha256,
        row_count,
    )
    expected_deleted = require_int63(
        receipt_prior_deleted + row_count,
        field="cleanup terminal replay next deleted_count",
    )
    if (
        batch_key != command.batch_key
        or row_count != 0
        or start_cursor != next_cursor
        or receipt_generation != command.expected_generation + 1
        or prior_generation != receipt_generation
        or prior_cursor != next_cursor
        or checkpoint_deleted != expected_deleted
        or expected_output != output_sha256
        or checkpoint_chain != output_sha256
        or prior_state != "COMPLETE"
        or current_order != prior_order + 1
    ):
        raise CleanupCorruptionError(
            "cleanup terminal replay does not match its phase transition"
        )
    locked = work.lock_row(
        LockRank.CHILD,
        encode_lock_key(
            "cleanup-terminal-replay", cycle.target_key, prior_order, batch_key
        ),
        f"""
        SELECT r.phase, r.batch_key, r.committed_generation
        FROM {_RECEIPT_TABLE} AS r
        JOIN {_CHECKPOINT_TABLE} AS prior
          ON prior.cleanup_id = r.cleanup_id AND prior.phase = r.phase
        WHERE r.cleanup_id = %s AND r.phase = %s AND r.batch_key = %s
          AND r.row_count = 0 AND prior.state = 'COMPLETE'
          AND prior.generation = r.committed_generation
        """,
        (cycle.cleanup_id, prior_phase, batch_key),
    )
    if locked != (prior_phase, batch_key, receipt_generation):
        raise CleanupUnavailableError("cleanup terminal replay authority changed")
    return CleanupBatchResult(
        cycle=cycle,
        phase=checkpoint.phase,
        generation=checkpoint.generation,
        cursor=checkpoint.cursor,
        deleted_count=checkpoint.deleted_count,
        row_count=0,
        phase_complete=True,
        cycle_complete=False,
        replayed=True,
    )


def _require_completion_row(work: VNextUnitOfWork, cycle: CleanupCycle) -> int:
    row = work.connector.fetch_one(
        f"SELECT cycle_generation, final_chain_sha256, deleted_count "
        f"FROM {_COMPLETION_TABLE} WHERE target_key = %s",
        (cycle.target_key,),
    )
    if not row or row[0] != cycle.cycle_generation:
        raise CleanupCorruptionError("COMPLETE cleanup lacks replay authority")
    require_digest32(row[1], field="cleanup completion chain")
    return require_int63(row[2], field="cleanup completion deleted_count")


def _initial_chain(cleanup_id: bytes, phase: str) -> bytes:
    return hashlib.sha256(
        _CHAIN_DOMAIN + cleanup_id + phase.encode("ascii") + b"\0"
    ).digest()


def _phase_chain(previous: bytes, phase: str) -> bytes:
    return hashlib.sha256(
        _CHAIN_DOMAIN + previous + phase.encode("ascii") + b"\0"
    ).digest()


def _input_digest(
    cycle: CleanupCycle,
    phase: str,
    cursor: bytes,
    row_keys: tuple[bytes, ...],
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_INPUT_DOMAIN)
    digest.update(cycle.cleanup_id)
    digest.update(phase.encode("ascii"))
    digest.update(b"\0")
    digest.update(len(cursor).to_bytes(4, "big"))
    digest.update(cursor)
    for key in row_keys:
        digest.update(len(key).to_bytes(4, "big"))
        digest.update(key)
    return digest.digest()


def _next_chain(
    previous: bytes,
    phase: str,
    generation: int,
    start_cursor: bytes,
    next_cursor: bytes,
    input_sha256: bytes,
    row_count: int,
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_CHAIN_DOMAIN)
    digest.update(previous)
    digest.update(phase.encode("ascii"))
    digest.update(b"\0")
    digest.update(generation.to_bytes(8, "big"))
    digest.update(len(start_cursor).to_bytes(4, "big"))
    digest.update(start_cursor)
    digest.update(len(next_cursor).to_bytes(4, "big"))
    digest.update(next_cursor)
    digest.update(input_sha256)
    digest.update(row_count.to_bytes(8, "big"))
    return digest.digest()


def _fixed_checkpoint_total(
    work: VNextUnitOfWork, cycle: CleanupCycle, phases: tuple[str, ...]
) -> int:
    rows = work.connector.fetch_all(
        f"SELECT phase, deleted_count FROM {_CHECKPOINT_TABLE} "
        "WHERE cleanup_id = %s ORDER BY phase LIMIT %s",
        (cycle.cleanup_id, len(phases) + 1),
    )
    if len(rows) != len(phases) or {row[0] for row in rows} != set(phases):
        raise CleanupCorruptionError("cleanup phase coverage is incomplete")
    total = 0
    for _phase, value in rows:
        total += require_int63(value, field="cleanup phase deleted_count")
        require_int63(total, field="cleanup total deleted_count")
    return total


def _complete_cycle(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
    final_chain_sha256: bytes,
    deleted_count: int,
    now: int,
) -> None:
    frozen_roots = _load_frozen_roots(
        work,
        cycle,
        _STATIC_PLANS.get(cycle.target_kind),
    )
    removed_roots = work.connector.execute_affected(
        f"DELETE FROM {_FROZEN_ROOT_TABLE} WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    )
    if removed_roots != len(frozen_roots):
        raise CleanupUnavailableError("cleanup frozen root set changed")
    work.connector.execute(
        f"DELETE FROM {_RECEIPT_TABLE} WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    )
    work.connector.execute(
        f"DELETE FROM {_CHECKPOINT_TABLE} WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    )
    work.compare_and_swap(
        f"UPDATE {_JOB_TABLE} SET state = 'COMPLETE', completed_at = %s "
        "WHERE cleanup_id = %s AND target_key = %s "
        "AND cycle_generation = %s AND state = 'OPEN'",
        (
            now,
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
        ),
        authority="cleanup job completion",
    )
    work.connector.execute(
        f"INSERT INTO {_COMPLETION_TABLE} "
        "(target_key, cycle_generation, final_chain_sha256, deleted_count) "
        "VALUES (%s, %s, %s, %s)",
        (
            cycle.target_key,
            cycle.cycle_generation,
            final_chain_sha256,
            deleted_count,
        ),
    )


def _checkpoint_result(
    cycle: CleanupCycle,
    checkpoint: _Checkpoint,
    *,
    row_count: int,
    replayed: bool,
) -> CleanupBatchResult:
    return CleanupBatchResult(
        cycle=cycle,
        phase=checkpoint.phase,
        generation=checkpoint.generation,
        cursor=checkpoint.cursor,
        deleted_count=checkpoint.deleted_count,
        row_count=row_count,
        phase_complete=checkpoint.state == "COMPLETE",
        cycle_complete=False,
        replayed=replayed,
    )


def _complete_result(
    cycle: CleanupCycle,
    *,
    row_count: int = 0,
    deleted_count: int = 0,
    replayed: bool,
) -> CleanupBatchResult:
    return CleanupBatchResult(
        cycle=cycle,
        phase=None,
        generation=None,
        cursor=_EMPTY_CURSOR,
        deleted_count=deleted_count,
        row_count=row_count,
        phase_complete=True,
        cycle_complete=True,
        replayed=replayed,
    )


def _digest_bounds(cycle: CleanupCycle, cursor: bytes) -> tuple[bytes, bytes, int]:
    if cursor:
        require_digest32(cursor, field="digest cleanup cursor")
        if cursor[0] != cycle.shard_no:
            raise CleanupCorruptionError("cleanup cursor escaped its fixed shard")
    lower = bytes((cycle.shard_no,)) + bytes(31)
    if cycle.shard_no == 255:
        return lower, b"", 1
    return lower, bytes((cycle.shard_no + 1,)) + bytes(31), 0


def _select_content_blobs(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT b.file_sha256
        FROM catalog_content_blobs AS b
        WHERE b.file_sha256 >= %s
          AND (%s = 0 OR b.file_sha256 > %s)
          AND (%s = 1 OR b.file_sha256 < %s)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = b.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = b.file_sha256)
        ORDER BY b.file_sha256
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="content blob key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-content-blob", key),
            """
            SELECT b.file_sha256
            FROM catalog_content_blobs AS b
            WHERE b.file_sha256 = %s
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = b.file_sha256)
              AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = b.file_sha256)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError("content blob gained a retention root")
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_content_blobs WHERE file_sha256 = %s", (key,)
            )
            != 1
        ):
            raise CleanupUnavailableError("content blob changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_file_name_identities(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT n.file_key
        FROM catalog_file_name_identity_anchors AS n
        WHERE n.file_key >= %s
          AND (%s = 0 OR n.file_key > %s)
          AND (%s = 1 OR n.file_key < %s)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_anchors x WHERE x.file_key = n.file_key)
        ORDER BY n.file_key
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="file-name key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-file-name", key),
            """
            SELECT n.file_key FROM catalog_file_name_identity_anchors AS n
            WHERE n.file_key = %s
              AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_anchors x WHERE x.file_key = n.file_key)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError(
                "file-name identity gained a retention root"
            )
        for table in (
            "catalog_file_name_identity_seals",
            "catalog_file_name_identity_file_roles",
            "catalog_file_name_identity_name_bytes",
            "catalog_file_name_identity_anchors",
        ):
            if (
                work.connector.execute_affected(
                    f"DELETE FROM {table} WHERE file_key = %s",
                    (key,),
                )
                != 1
            ):
                raise CleanupUnavailableError(
                    "file-name identity changed during compound cleanup"
                )
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_publication_identities(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT p.publication_key
        FROM catalog_publication_identities AS p
        WHERE p.publication_key >= %s
          AND (%s = 0 OR p.publication_key > %s)
          AND (%s = 1 OR p.publication_key < %s)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_publication_occurrence_identities x
              WHERE x.publication_key = p.publication_key)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_publication_selection_occurrence_identities x
              WHERE x.publication_key = p.publication_key)
        ORDER BY p.publication_key
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="publication key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-publication-identity", key),
            """
            SELECT p.publication_key FROM catalog_publication_identities AS p
            WHERE p.publication_key = %s
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_publication_occurrence_identities x
                  WHERE x.publication_key = p.publication_key)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_publication_selection_occurrence_identities x
                  WHERE x.publication_key = p.publication_key)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError(
                "publication identity gained a retention root"
            )
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_publication_identities WHERE publication_key = %s",
                (key,),
            )
            != 1
        ):
            raise CleanupUnavailableError("publication identity changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_artifact_blobs(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT artifact_blob.artifact_sha256
        FROM catalog_artifact_blobs AS artifact_blob
        WHERE artifact_blob.artifact_sha256 >= %s
          AND (%s = 0 OR artifact_blob.artifact_sha256 > %s)
          AND (%s = 1 OR artifact_blob.artifact_sha256 < %s)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_artifacts p
              WHERE p.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_artifacts retained
              WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
        ORDER BY artifact_blob.artifact_sha256
        LIMIT %s
        """,
        (
            lower,
            0 if cursor else 1,
            cursor,
            no_upper,
            upper,
            cycle.max_rows_per_transaction,
        ),
    )
    keys = tuple(require_digest32(row[0], field="artifact blob key") for row in rows)
    for key in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-artifact-blob", key),
            """
            SELECT artifact_blob.artifact_sha256
            FROM catalog_artifact_blobs AS artifact_blob
            WHERE artifact_blob.artifact_sha256 = %s
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_prepared_artifacts p
                  WHERE p.artifact_sha256 = artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_artifacts retained
                  WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
            """,
            (key,),
        )
        if locked != (key,):
            raise CleanupRetentionBlockedError("artifact blob became retained")
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
                (key,),
            )
            != 1
        ):
            raise CleanupUnavailableError("artifact blob changed during cleanup")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _hash_cache_cutoff(cycle: CleanupCycle) -> int:
    if cycle.hash_cache_max_age_microseconds > cycle.cycle_cutoff_at:
        raise CleanupCorruptionError("hash-cache cleanup cutoff underflows")
    return cycle.cycle_cutoff_at - cycle.hash_cache_max_age_microseconds


_HASH_CACHE_ELIGIBILITY = "r.observed_at <= %s"


def _hash_cache_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "operational_hash_cache_observations"
    key = ("source_identity_sha256", "fingerprint_sha256")
    return {
        "HC_FILE": (
            _owned_spec(
                "operational_file_hash_caches",
                key,
                root,
                key,
            ),
        ),
        "HC_ROOT": (_owned_spec(root, key, root, key),),
    }


# The specifications below are immutable Python constants.  Their SQL is
# compiled from these constants at import time; no table, column, predicate, or
# phase text read from the database can reach this path.
_StaticScalar = bytes | int | str


@dataclass(frozen=True, slots=True)
class _StaticDeleteSpec:
    table: str
    source: str
    primary_key: tuple[str, ...]
    delete_sql: tuple[str, ...]
    extra_predicate: str = "1 = 1"
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None

    def __post_init__(self) -> None:
        for metadata in (
            self.delete_parameter_indexes,
            self.delete_allowed_affected,
        ):
            if metadata is not None and len(metadata) != len(self.delete_sql):
                raise RuntimeError(
                    "cleanup compound-delete metadata must cover every statement"
                )


@dataclass(frozen=True, slots=True)
class _StaticTargetPlan:
    kind: CleanupTargetKind
    root_table: str
    root_key: tuple[str, ...]
    shard_column: str
    shard_width: int | None
    eligibility: str
    phases: dict[str, tuple[_StaticDeleteSpec, ...]]
    uses_cutoff: bool = False
    variable_width_shard: bool = False


_FROZEN_ROOT_INT_ATTRIBUTES = frozenset(
    {
        "gallery_id",
        "generation",
        "gid",
        "observation_id",
        "revision",
        "source_revision",
    }
)
_FROZEN_ROOT_UUID_ATTRIBUTES = frozenset(
    {
        "analysis_id",
        "build_id",
        "candidate_id",
        "preparation_id",
        "receipt_id",
        "staging_id",
    }
)
_FROZEN_ROOT_DIGEST_ATTRIBUTES = frozenset(
    {
        "artifact_sha256",
        "file_key",
        "file_sha256",
        "fingerprint_sha256",
        "page_sha256",
        "publication_key",
        "source_identity_sha256",
        "value_sha256",
    }
)


def _frozen_root_attributes(plan: _StaticTargetPlan) -> tuple[str, ...]:
    """Return the immutable authority carried by one frozen protocol root."""

    if plan.kind is CleanupTargetKind.PUBLICATION_COMMIT:
        return (*plan.root_key, "preparation_id")
    return plan.root_key


def _validate_frozen_root_values(
    plan: _StaticTargetPlan,
    root: Sequence[_StaticScalar],
) -> None:
    attributes = _frozen_root_attributes(plan)
    if len(root) != len(attributes):
        raise CleanupCorruptionError("cleanup frozen root arity drifted")
    for attribute, value in zip(attributes, root, strict=True):
        if attribute in _FROZEN_ROOT_INT_ATTRIBUTES:
            if isinstance(value, bool) or not isinstance(value, int):
                raise CleanupCorruptionError("cleanup frozen root integer type drifted")
            require_int63(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute in _FROZEN_ROOT_UUID_ATTRIBUTES:
            require_uuid16(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute in _FROZEN_ROOT_DIGEST_ATTRIBUTES:
            require_digest32(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute == "source_gallery_name":
            require_bounded_bytes(
                value,
                field="cleanup frozen root source_gallery_name",
                minimum=1,
                maximum=255,
            )
            continue
        raise CleanupCorruptionError(
            f"cleanup root attribute {attribute!r} lacks a registered codec"
        )


def _identifier(value: str) -> str:
    if not value or any(not (part.isalnum() or part == "_") for part in value):
        raise RuntimeError("cleanup SQL identifiers must be static ASCII identifiers")
    return value


def _delete_sql(table: str, primary_key: Sequence[str]) -> str:
    safe_table = _identifier(table)
    columns = tuple(_identifier(column) for column in primary_key)
    return f"DELETE FROM {safe_table} WHERE " + " AND ".join(
        f"{column} = %s" for column in columns
    )


def _owned_spec(
    table: str,
    primary_key: tuple[str, ...],
    root_table: str,
    root_key: tuple[str, ...],
    owner_key: tuple[str, ...] | None = None,
    *,
    extra_predicate: str = "1 = 1",
    delete_sql: tuple[str, ...] | None = None,
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None,
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None,
) -> _StaticDeleteSpec:
    if owner_key is None:
        owner_key = root_key
    if len(owner_key) != len(root_key):
        raise RuntimeError("cleanup owner and root key arity differ")
    safe_table = _identifier(table)
    safe_root = _identifier(root_table)
    join = " AND ".join(
        f"r.{_identifier(root)} = c.{_identifier(owner)}"
        for root, owner in zip(root_key, owner_key, strict=True)
    )
    return _StaticDeleteSpec(
        table=safe_table,
        source=f"{safe_table} AS c JOIN {safe_root} AS r ON {join}",
        primary_key=tuple(_identifier(column) for column in primary_key),
        delete_sql=(
            (_delete_sql(safe_table, primary_key),)
            if delete_sql is None
            else delete_sql
        ),
        extra_predicate=extra_predicate,
        delete_parameter_indexes=delete_parameter_indexes,
        delete_allowed_affected=delete_allowed_affected,
    )


def _indirect_spec(
    table: str,
    primary_key: tuple[str, ...],
    source: str,
    *,
    extra_predicate: str = "1 = 1",
    delete_sql: tuple[str, ...] | None = None,
    delete_parameter_indexes: tuple[tuple[int, ...], ...] | None = None,
    delete_allowed_affected: tuple[frozenset[int], ...] | None = None,
) -> _StaticDeleteSpec:
    safe_table = _identifier(table)
    return _StaticDeleteSpec(
        table=safe_table,
        source=source,
        primary_key=tuple(_identifier(column) for column in primary_key),
        delete_sql=(
            (_delete_sql(safe_table, primary_key),)
            if delete_sql is None
            else delete_sql
        ),
        extra_predicate=extra_predicate,
        delete_parameter_indexes=delete_parameter_indexes,
        delete_allowed_affected=delete_allowed_affected,
    )


def _encode_static_scalar(value: _StaticScalar) -> bytes:
    if isinstance(value, bool):
        raise CleanupCorruptionError("cleanup cursor contains a boolean key")
    if isinstance(value, int):
        integer = require_int63(value, field="cleanup static cursor integer")
        return b"i" + integer.to_bytes(8, "big")
    if isinstance(value, bytes):
        payload = require_bounded_bytes(
            value, field="cleanup static cursor bytes", maximum=1024
        )
        return b"b" + len(payload).to_bytes(2, "big") + payload
    if isinstance(value, str):
        payload = value.encode("utf-8", errors="strict")
        if len(payload) > 1024:
            raise CleanupCorruptionError("cleanup static text cursor is too large")
        return b"s" + len(payload).to_bytes(2, "big") + payload
    raise CleanupCorruptionError("cleanup cursor contains an unsupported key type")


def _encode_frozen_root_key(values: Sequence[_StaticScalar]) -> bytes:
    if not values or len(values) > 255:
        raise CleanupCorruptionError("cleanup frozen root key arity is invalid")
    encoded = bytearray(b"\x01" + bytes((len(values),)))
    for value in values:
        encoded.extend(_encode_static_scalar(value))
    return require_bounded_bytes(
        bytes(encoded),
        field="cleanup frozen root key",
        maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
    )


def _decode_static_scalars(
    payload: bytes,
    *,
    offset: int,
    count: int,
    field: str,
) -> tuple[_StaticScalar, ...]:
    values: list[_StaticScalar] = []
    for _ in range(count):
        if offset >= len(payload):
            raise CleanupCorruptionError(f"{field} is truncated")
        tag = payload[offset : offset + 1]
        offset += 1
        if tag == b"i":
            if offset + 8 > len(payload):
                raise CleanupCorruptionError(f"{field} integer is truncated")
            values.append(int.from_bytes(payload[offset : offset + 8], "big"))
            offset += 8
            continue
        if tag not in {b"b", b"s"} or offset + 2 > len(payload):
            raise CleanupCorruptionError(f"{field} tag is invalid")
        size = int.from_bytes(payload[offset : offset + 2], "big")
        offset += 2
        if offset + size > len(payload):
            raise CleanupCorruptionError(f"{field} bytes are truncated")
        value = payload[offset : offset + size]
        offset += size
        try:
            values.append(
                value if tag == b"b" else value.decode("utf-8", errors="strict")
            )
        except UnicodeDecodeError as error:
            raise CleanupCorruptionError(f"{field} text is not strict UTF-8") from error
    if offset != len(payload):
        raise CleanupCorruptionError(f"{field} has trailing bytes")
    return tuple(values)


def _decode_frozen_root_key(
    value: object,
    *,
    root_arity: int,
) -> tuple[_StaticScalar, ...]:
    payload = require_bounded_bytes(
        value,
        field="cleanup frozen root key",
        minimum=3,
        maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
    )
    if payload[0] != 1 or payload[1] != root_arity:
        raise CleanupCorruptionError("cleanup frozen root key codec is invalid")
    return _decode_static_scalars(
        payload,
        offset=2,
        count=root_arity,
        field="cleanup frozen root key",
    )


def _require_frozen_root_count(value: object) -> int:
    count = require_int63(value, field="cleanup frozen_root_count")
    if count > _MAX_BATCH_ROWS:
        raise CleanupCorruptionError("cleanup frozen root set exceeds its hard cap")
    return count


def _frozen_root_set_sha256(
    cleanup_id: bytes,
    encoded_roots: Sequence[bytes],
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_FROZEN_ROOT_SET_DOMAIN)
    digest.update(require_uuid16(cleanup_id, field="cleanup frozen root cleanup_id"))
    ordered = tuple(sorted(encoded_roots))
    digest.update(len(ordered).to_bytes(2, "big"))
    for encoded in ordered:
        value = require_bounded_bytes(
            encoded,
            field="cleanup frozen root key",
            minimum=3,
            maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
        )
        digest.update(len(value).to_bytes(2, "big"))
        digest.update(value)
    return digest.digest()


def _encode_static_cursor(index: int, values: Sequence[_StaticScalar]) -> bytes:
    if not 0 <= index <= 65535 or len(values) > 255:
        raise CleanupCorruptionError("cleanup static cursor header is invalid")
    encoded = bytearray(b"\x01" + index.to_bytes(2, "big") + bytes((len(values),)))
    for value in values:
        encoded.extend(_encode_static_scalar(value))
    return require_bounded_bytes(
        bytes(encoded), field="cleanup static cursor", maximum=2048
    )


def _decode_static_cursor(
    cursor: bytes, specs: Sequence[_StaticDeleteSpec], root_arity: int
) -> tuple[int, tuple[_StaticScalar, ...] | None]:
    if not cursor:
        return 0, None
    payload = require_bounded_bytes(
        cursor, field="cleanup static cursor", minimum=4, maximum=2048
    )
    if payload[0] != 1:
        raise CleanupCorruptionError("cleanup static cursor version is unknown")
    index = int.from_bytes(payload[1:3], "big")
    if index >= len(specs):
        raise CleanupCorruptionError("cleanup static cursor relation is unknown")
    count = payload[3]
    if count != root_arity + len(specs[index].primary_key):
        raise CleanupCorruptionError("cleanup static cursor arity is invalid")
    return index, _decode_static_scalars(
        payload,
        offset=4,
        count=count,
        field="cleanup static cursor",
    )


def _keyset_predicate(columns: Sequence[str]) -> str:
    branches: list[str] = []
    for index, column in enumerate(columns):
        equal = " AND ".join(f"{prior} = %s" for prior in columns[:index])
        greater = f"{column} > %s"
        branches.append(f"({equal + ' AND ' if equal else ''}{greater})")
    return " OR ".join(branches)


def _keyset_parameters(values: Sequence[_StaticScalar]) -> tuple[_StaticScalar, ...]:
    parameters: list[_StaticScalar] = []
    for index, value in enumerate(values):
        parameters.extend(values[:index])
        parameters.append(value)
    return tuple(parameters)


def _static_policy_parameters(
    plan: _StaticTargetPlan, cycle: CleanupCycle
) -> tuple[object, ...]:
    if plan.kind is CleanupTargetKind.HASH_CACHE_OBSERVATION:
        return (_hash_cache_cutoff(cycle),)
    return (cycle.cycle_cutoff_at,) if plan.uses_cutoff else ()


def _static_shard_sql(plan: _StaticTargetPlan) -> str:
    if plan.shard_width is None:
        return f"MOD(r.{plan.shard_column}, 256) = %s"
    return f"r.{plan.shard_column} >= %s AND (%s = 1 OR r.{plan.shard_column} < %s)"


def _frozen_root_predicate(
    plan: _StaticTargetPlan,
    roots: Sequence[tuple[_StaticScalar, ...]],
) -> tuple[str, tuple[_StaticScalar, ...]]:
    if not roots:
        return "0 = 1", ()
    branches: list[str] = []
    parameters: list[_StaticScalar] = []
    for root in roots:
        if len(root) != len(_frozen_root_attributes(plan)):
            raise CleanupCorruptionError("cleanup frozen root arity drifted")
        branches.append(
            "(" + " AND ".join(f"r.{column} = %s" for column in plan.root_key) + ")"
        )
        parameters.extend(root[: len(plan.root_key)])
    return " OR ".join(branches), tuple(parameters)


def _static_select_sql(
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    *,
    exact: bool,
    frozen_root_predicate: str,
    has_after: bool = False,
    eligibility: str | None = None,
) -> str:
    root_columns = tuple(f"r.{column}" for column in plan.root_key)
    primary_columns = tuple(f"c.{column}" for column in spec.primary_key)
    ordered = root_columns + primary_columns
    select = ", ".join(ordered)
    shard_sql = _static_shard_sql(plan)
    eligible = plan.eligibility if eligibility is None else eligibility
    if exact:
        exact_root = " AND ".join(f"{column} = %s" for column in root_columns)
        exact_pk = " AND ".join(f"{column} = %s" for column in primary_columns)
        return (
            f"SELECT {select} FROM {spec.source} WHERE ({eligible}) "
            f"AND ({spec.extra_predicate}) AND ({shard_sql}) "
            f"AND ({frozen_root_predicate}) "
            f"AND {exact_root} AND {exact_pk}"
        )
    keyset_sql = ""
    if has_after:
        keyset_sql = f" AND ({_keyset_predicate(ordered)})"
    return (
        f"SELECT {select} FROM {spec.source} WHERE ({eligible}) "
        f"AND ({spec.extra_predicate}) AND ({shard_sql}) "
        f"AND ({frozen_root_predicate}) "
        f"{keyset_sql} ORDER BY {select} LIMIT %s"
    )


def _static_shard_parameters(
    plan: _StaticTargetPlan, cycle: CleanupCycle
) -> tuple[object, ...]:
    if plan.shard_width is None:
        return (cycle.shard_no,)
    lower = bytes((cycle.shard_no,))
    if not plan.variable_width_shard:
        lower += bytes(plan.shard_width - 1)
    if cycle.shard_no == 255:
        return (lower, 1, b"")
    upper = bytes((cycle.shard_no + 1,))
    if not plan.variable_width_shard:
        upper += bytes(plan.shard_width - 1)
    return (lower, 0, upper)


def _static_raw_responsibility_exists(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
    through: tuple[_StaticScalar, ...] | None = None,
) -> bool:
    ordered = tuple(f"r.{column}" for column in plan.root_key) + tuple(
        f"c.{column}" for column in spec.primary_key
    )
    covered = ""
    parameters: tuple[object, ...] = shard_parameters + frozen_root_parameters
    if through is not None:
        if len(through) != len(ordered):
            raise CleanupCorruptionError(
                "cleanup raw responsibility cursor arity drifted"
            )
        covered = f" AND NOT ({_keyset_predicate(ordered)})"
        parameters += _keyset_parameters(through)
    row = work.connector.fetch_one(
        f"SELECT 1 FROM {spec.source} WHERE ({spec.extra_predicate}) "
        f"AND ({_static_shard_sql(plan)}) "
        f"AND ({frozen_root_predicate}){covered} LIMIT 1",
        parameters,
    )
    if not row:
        return False
    if row != (1,):
        raise CleanupCorruptionError(
            "cleanup raw responsibility probe returned an invalid shape"
        )
    return True


def _validate_static_cursor_covered_postcondition(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    phase: str,
    start_index: int,
    start_values: tuple[_StaticScalar, ...] | None,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
) -> None:
    """Reject reappearance in the current spec's durable keyset prefix."""

    current_specs = plan.phases[phase]
    if start_values is not None and _static_raw_responsibility_exists(
        work,
        plan=plan,
        spec=current_specs[start_index],
        frozen_root_predicate=frozen_root_predicate,
        frozen_root_parameters=frozen_root_parameters,
        shard_parameters=shard_parameters,
        through=start_values,
    ):
        raise CleanupCorruptionError(
            f"{plan.kind.value} cursor-covered cleanup row reappeared"
        )


def _require_static_terminal_responsibility_empty(
    work: VNextUnitOfWork,
    *,
    plan: _StaticTargetPlan,
    phase: str,
    frozen_root_predicate: str,
    frozen_root_parameters: tuple[_StaticScalar, ...],
    shard_parameters: tuple[object, ...],
) -> None:
    phase_names = tuple(plan.phases)
    try:
        phase_index = phase_names.index(phase)
    except ValueError as error:
        raise CleanupCorruptionError(
            "cleanup static phase is outside its registered plan"
        ) from error
    for checked_phase in phase_names[: phase_index + 1]:
        for spec in plan.phases[checked_phase]:
            if _static_raw_responsibility_exists(
                work,
                plan=plan,
                spec=spec,
                frozen_root_predicate=frozen_root_predicate,
                frozen_root_parameters=frozen_root_parameters,
                shard_parameters=shard_parameters,
            ):
                raise CleanupRetentionBlockedError(
                    f"{plan.kind.value} still owns rows hidden by a retention predicate"
                )


def _freeze_static_cycle_roots(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
) -> None:
    plan = _STATIC_PLANS.get(cycle.target_kind)
    if plan is None:
        return
    root_columns = ", ".join(f"r.{column}" for column in plan.root_key)
    if plan.kind is CleanupTargetKind.PUBLICATION_COMMIT:
        selected_columns = f"{root_columns}, committed.preparation_id"
        root_source = (
            f"{plan.root_table} AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id"
        )
    else:
        selected_columns = root_columns
        root_source = f"{plan.root_table} AS r"
    query = (
        f"SELECT {selected_columns} FROM {root_source} "
        f"WHERE ({plan.eligibility}) AND ({_static_shard_sql(plan)}) "
        f"ORDER BY {selected_columns} LIMIT %s"
    )
    rows = work.connector.fetch_all(
        query,
        _static_policy_parameters(plan, cycle)
        + _static_shard_parameters(plan, cycle)
        + (cycle.max_rows_per_transaction,),
    )
    roots = tuple(_static_values(row) for row in rows)
    for root in roots:
        _validate_frozen_root_values(plan, root)
    encoded_roots = tuple(_encode_frozen_root_key(root) for root in roots)
    if len(set(encoded_roots)) != len(encoded_roots):
        raise CleanupCorruptionError("cleanup frozen root query returned duplicates")
    for encoded in encoded_roots:
        work.connector.execute(
            f"INSERT INTO {_FROZEN_ROOT_TABLE} (cleanup_id, frozen_root_key) "
            "VALUES (%s, %s)",
            (cycle.cleanup_id, encoded),
        )
    if not encoded_roots:
        return
    expected_empty_digest = _frozen_root_set_sha256(cycle.cleanup_id, ())
    affected = work.connector.execute_affected(
        f"UPDATE {_JOB_TABLE} "
        "SET frozen_root_count = %s, frozen_root_set_sha256 = %s "
        "WHERE cleanup_id = %s AND target_key = %s "
        "AND cycle_generation = %s AND algorithm_version = %s "
        "AND frozen_root_count = 0 AND frozen_root_set_sha256 = %s "
        "AND state = 'OPEN'",
        (
            len(encoded_roots),
            _frozen_root_set_sha256(cycle.cleanup_id, encoded_roots),
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
            _CLEANUP_ALGORITHM_VERSION,
            expected_empty_digest,
        ),
    )
    if affected != 1:
        raise CleanupUnavailableError("cleanup frozen root seal changed")


def _load_frozen_roots(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    plan: _StaticTargetPlan | None,
    *,
    expected_count: int | None = None,
    expected_digest: bytes | None = None,
) -> tuple[tuple[_StaticScalar, ...], ...]:
    if expected_count is None or expected_digest is None:
        job = work.connector.fetch_one(
            f"SELECT frozen_root_count, frozen_root_set_sha256 "
            f"FROM {_JOB_TABLE} WHERE cleanup_id = %s AND target_key = %s "
            "AND cycle_generation = %s AND state = 'OPEN'",
            (cycle.cleanup_id, cycle.target_key, cycle.cycle_generation),
        )
        if not job or len(job) != 2:
            raise CleanupUnavailableError("OPEN cleanup frozen root seal is missing")
        expected_count = _require_frozen_root_count(job[0])
        expected_digest = require_digest32(
            job[1], field="cleanup frozen_root_set_sha256"
        )
    else:
        expected_count = _require_frozen_root_count(expected_count)
        expected_digest = require_digest32(
            expected_digest, field="cleanup frozen_root_set_sha256"
        )
    if expected_count > cycle.max_rows_per_transaction:
        raise CleanupCorruptionError("cleanup frozen root count exceeds cycle policy")
    rows = work.connector.fetch_all(
        f"SELECT frozen_root_key FROM {_FROZEN_ROOT_TABLE} "
        "WHERE cleanup_id = %s ORDER BY frozen_root_key LIMIT %s",
        (cycle.cleanup_id, cycle.max_rows_per_transaction + 1),
    )
    if len(rows) != expected_count:
        raise CleanupCorruptionError("cleanup frozen root membership count drifted")
    encoded_roots: list[bytes] = []
    roots: list[tuple[_StaticScalar, ...]] = []
    for row in rows:
        if len(row) != 1:
            raise CleanupCorruptionError("cleanup frozen root row shape is invalid")
        encoded = require_bounded_bytes(
            row[0],
            field="cleanup frozen root key",
            minimum=3,
            maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
        )
        encoded_roots.append(encoded)
        if plan is None:
            raise CleanupCorruptionError(
                "non-static cleanup cycle retained a frozen root"
            )
        root = _decode_frozen_root_key(
            encoded,
            root_arity=len(_frozen_root_attributes(plan)),
        )
        _validate_frozen_root_values(plan, root)
        if _encode_frozen_root_key(root) != encoded:
            raise CleanupCorruptionError("cleanup frozen root key is non-canonical")
        roots.append(root)
    if len(set(encoded_roots)) != expected_count or len(set(roots)) != expected_count:
        raise CleanupCorruptionError("cleanup frozen root membership is duplicated")
    if len(roots) != expected_count:
        raise CleanupCorruptionError("cleanup decoded frozen root count drifted")
    if _frozen_root_set_sha256(cycle.cleanup_id, encoded_roots) != expected_digest:
        raise CleanupCorruptionError("cleanup frozen root seal digest drifted")
    if plan is None and expected_count != 0:
        raise CleanupCorruptionError("non-static cleanup has frozen root membership")
    return tuple(roots)


def _require_no_frozen_roots(work: VNextUnitOfWork, cleanup_id: bytes) -> None:
    row = work.connector.fetch_one(
        f"SELECT 1 FROM {_FROZEN_ROOT_TABLE} WHERE cleanup_id = %s LIMIT 1",
        (cleanup_id,),
    )
    if not row:
        return
    if row != (1,):
        raise CleanupCorruptionError("cleanup frozen root probe is malformed")
    raise CleanupCorruptionError("COMPLETE cleanup retains frozen root membership")


def _static_values(row: Sequence[object]) -> tuple[_StaticScalar, ...]:
    values: list[_StaticScalar] = []
    for value in row:
        if isinstance(value, bool) or not isinstance(value, (bytes, int, str)):
            raise CleanupCorruptionError("cleanup selected an invalid key value")
        if isinstance(value, int):
            require_int63(value, field="cleanup selected integer key")
        elif isinstance(value, bytes):
            require_bounded_bytes(
                value, field="cleanup selected byte key", maximum=1024
            )
        else:
            if len(value.encode("utf-8", errors="strict")) > 1024:
                raise CleanupCorruptionError("cleanup selected text key is too large")
        values.append(value)
    return tuple(values)


def _run_static_phase(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    cursor: bytes,
    plan: _StaticTargetPlan,
    phase: str,
    *,
    eligibility: str | None = None,
    policy_parameters: tuple[object, ...] | None = None,
) -> _Mutation:
    specs = plan.phases[phase]
    frozen_roots = _load_frozen_roots(work, cycle, plan)
    start_index, start_values = _decode_static_cursor(cursor, specs, len(plan.root_key))
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
    frozen_predicate, frozen_parameters = _frozen_root_predicate(plan, frozen_roots)
    deleted: list[bytes] = []
    next_cursor = cursor
    policy = (
        _static_policy_parameters(plan, cycle)
        if policy_parameters is None
        else policy_parameters
    )
    shard = _static_shard_parameters(plan, cycle)
    _validate_static_cursor_covered_postcondition(
        work,
        plan=plan,
        phase=phase,
        start_index=start_index,
        start_values=start_values,
        frozen_root_predicate=frozen_predicate,
        frozen_root_parameters=frozen_parameters,
        shard_parameters=shard,
    )
    for index in range(start_index, len(specs)):
        spec = specs[index]
        deleted_primary_keys: set[tuple[_StaticScalar, ...]] = set()
        ordered_arity = len(plan.root_key) + len(spec.primary_key)
        after = start_values if index == start_index else None
        if after is not None and len(after) != ordered_arity:
            raise CleanupCorruptionError("cleanup cursor does not match relation key")
        while len(deleted) < cycle.max_rows_per_transaction:
            remaining = cycle.max_rows_per_transaction - len(deleted)
            query = _static_select_sql(
                plan,
                spec,
                exact=False,
                frozen_root_predicate=frozen_predicate,
                has_after=after is not None,
                eligibility=eligibility,
            )
            parameters: tuple[object, ...] = policy + shard + frozen_parameters
            if after is not None:
                parameters += _keyset_parameters(after)
            parameters += (remaining,)
            rows = work.connector.fetch_all(query, parameters)
            if not rows:
                break
            candidates = tuple(_static_values(row) for row in rows)
            deleted_before_page = len(deleted)
            candidates_by_lock = tuple(
                sorted(
                    candidates,
                    key=lambda candidate: encode_lock_key(
                        "cleanup-static",
                        plan.kind.value,
                        phase,
                        index,
                        *candidate,
                    ),
                )
            )
            for candidate in candidates_by_lock:
                root = candidate[: len(plan.root_key)]
                primary = candidate[len(plan.root_key) :]
                if primary in deleted_primary_keys:
                    # An indirect child can reference two eligible roots (for
                    # example a display-title cache row whose input and output
                    # canonical digests share this shard).  Delete the child
                    # exactly once while still advancing past every joined
                    # root/key tuple in the deterministic cursor order.
                    continue
                exact_query = _static_select_sql(
                    plan,
                    spec,
                    exact=True,
                    frozen_root_predicate=frozen_predicate,
                    eligibility=eligibility,
                )
                exact_parameters = policy + shard + frozen_parameters + root + primary
                lock_key = encode_lock_key(
                    "cleanup-static",
                    plan.kind.value,
                    phase,
                    index,
                    *candidate,
                )
                locked = work.lock_row(
                    LockRank.CHILD,
                    lock_key,
                    exact_query,
                    exact_parameters,
                )
                if _static_values(locked) != candidate:
                    raise CleanupRetentionBlockedError(
                        f"{plan.kind.value} gained a retention root"
                    )
                for statement_index, statement in enumerate(spec.delete_sql):
                    indexes = spec.delete_parameter_indexes
                    statement_parameters = (
                        primary
                        if indexes is None
                        else tuple(primary[index] for index in indexes[statement_index])
                    )
                    affected = work.connector.execute_affected(
                        statement,
                        statement_parameters,
                    )
                    allowed = spec.delete_allowed_affected
                    expected = (
                        frozenset((1,)) if allowed is None else allowed[statement_index]
                    )
                    if affected not in expected:
                        raise CleanupUnavailableError(
                            f"{plan.kind.value} cleanup row changed"
                        )
                deleted_primary_keys.add(primary)
                deleted.append(_encode_static_cursor(index, candidate))
            # Cursor order is SQL root/PK order, independent of the unsigned
            # encoded lock-key order used above inside this bounded page.
            after = candidates[-1]
            next_cursor = _encode_static_cursor(index, after)
            if len(candidates) < remaining or len(deleted) - deleted_before_page < len(
                candidates
            ):
                break
    if request_budget_retained is not None and deleted:
        try:
            release_gallery_staging_request_budget(
                work,
                retained_request_count=request_budget_retained,
                deleted_count=len(deleted),
            )
        except GalleryStagingBudgetCorruptionError as error:
            raise CleanupCorruptionError(str(error)) from error
    if not deleted:
        _require_static_terminal_responsibility_empty(
            work,
            plan=plan,
            phase=phase,
            frozen_root_predicate=frozen_predicate,
            frozen_root_parameters=frozen_parameters,
            shard_parameters=shard,
        )
    return _Mutation(next_cursor, tuple(deleted))


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


def _static_mutator(kind: CleanupTargetKind, phase: str) -> _Mutator:
    def mutate(work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes) -> _Mutation:
        if cycle.target_kind != kind:
            raise CleanupCorruptionError("cleanup static strategy kind drifted")
        return _run_static_phase(work, cycle, cursor, _STATIC_PLANS[kind], phase)

    return mutate


def _source_build_mutator(phase: str) -> _Mutator:
    def mutate(work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes) -> _Mutation:
        if cycle.target_kind is not CleanupTargetKind.SOURCE_BUILD:
            raise CleanupCorruptionError("source-build cleanup kind drifted")
        plan = _STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD]
        if phase != "SB_ROOT":
            return _run_static_phase(work, cycle, cursor, plan, phase)
        return _run_static_phase(
            work,
            cycle,
            cursor,
            plan,
            phase,
            eligibility=_SOURCE_BUILD_AFTER_STATE_ELIGIBILITY,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


def _analysis_run_mutator(phase: str) -> _Mutator:
    def mutate(work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes) -> _Mutation:
        if cycle.target_kind is not CleanupTargetKind.ANALYSIS_RUN:
            raise CleanupCorruptionError("analysis-run cleanup kind drifted")
        plan = _STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN]
        if phase != "AR_ROOT":
            return _run_static_phase(work, cycle, cursor, plan, phase)
        return _run_static_phase(
            work,
            cycle,
            cursor,
            plan,
            phase,
            eligibility=_ANALYSIS_RUN_AFTER_STATE_ELIGIBILITY,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


def _publication_commit_mutator(phase: str) -> _Mutator:
    def mutate(work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes) -> _Mutation:
        if cycle.target_kind is not CleanupTargetKind.PUBLICATION_COMMIT:
            raise CleanupCorruptionError("publication-commit cleanup kind drifted")
        plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
        if phase in {
            "PCOM_RELEASE_BUILD_BASE",
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
            "PCOM_FINALIZATION_MARKER",
            "PCOM_FINALIZATION_BATCH",
        }:
            _require_publication_commit_frozen_preparation_mapping(
                work,
                cycle=cycle,
            )
        if phase == "PCOM_RELEASE_BUILD_BASE":
            return _run_static_phase(work, cycle, cursor, plan, phase)
        if phase in {
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
        }:
            _validate_publication_commit_preparation_authority(
                work,
                cycle=cycle,
                phase=phase,
                cursor=cursor,
            )
        if phase == "PCOM_EVENT":
            return _run_publication_commit_event_phase(work, cycle, cursor)
        if phase == "PCOM_COMMIT_EFFECT_ROOT":
            return _run_publication_commit_effect_root_phase(work, cycle, cursor)
        if phase == "PCOM_PREPARATION_BINDING":
            eligibility = _PUBLICATION_COMMIT_AFTER_BUILD_BASE_ELIGIBILITY
        elif phase == "PCOM_PREPARATION_BATCH":
            eligibility = _PUBLICATION_COMMIT_AFTER_PREPARATION_BINDING_ELIGIBILITY
        elif phase == "PCOM_PREPARATION_CHECKPOINT":
            eligibility = _PUBLICATION_COMMIT_AFTER_PREPARATION_BATCH_ELIGIBILITY
        elif phase == "PCOM_PREPARATION":
            eligibility = _PUBLICATION_COMMIT_AFTER_PREPARATION_CHECKPOINT_ELIGIBILITY
        elif phase == "PCOM_FINALIZATION_MARKER":
            eligibility = _PUBLICATION_COMMIT_AFTER_EVENT_ELIGIBILITY
        elif phase == "PCOM_FINALIZATION_BATCH":
            eligibility = _PUBLICATION_COMMIT_AFTER_MARKER_ELIGIBILITY
        elif phase == "PCOM_FINALIZATION_CHECKPOINT":
            eligibility = _PUBLICATION_COMMIT_AFTER_COMPOUND_ROOT_ELIGIBILITY
        else:
            eligibility = _PUBLICATION_COMMIT_AFTER_CHECKPOINT_ELIGIBILITY
        return _run_static_phase(
            work,
            cycle,
            cursor,
            plan,
            phase,
            eligibility=eligibility,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


def _require_publication_commit_frozen_preparation_mapping(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
) -> None:
    plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
    for root in _load_frozen_roots(work, cycle, plan):
        receipt_id = require_uuid16(
            root[0],
            field="publication-commit frozen receipt_id",
        )
        preparation_id = require_uuid16(
            root[1],
            field="publication-commit frozen preparation_id",
        )
        if work.connector.fetch_one(
            "SELECT preparation_id FROM catalog_publication_commits "
            "WHERE receipt_id = %s",
            (receipt_id,),
        ) != (preparation_id,):
            raise CleanupCorruptionError(
                "publication-commit preparation differs from its frozen authority"
            )


def _validate_publication_commit_preparation_authority(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
    phase: str,
    cursor: bytes,
) -> None:
    """Validate the exact commit-owned preparation control family before DML."""

    plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
    specs = plan.phases[phase]
    relation_index, cursor_values = _decode_static_cursor(
        cursor,
        specs,
        len(plan.root_key),
    )
    frozen_roots = _load_frozen_roots(work, cycle, plan)
    for root in frozen_roots:
        if len(root) != 2:
            raise CleanupCorruptionError(
                "publication-commit frozen root has an invalid shape"
            )
        receipt_id = require_uuid16(
            root[0],
            field="publication-commit cleanup receipt_id",
        )
        frozen_preparation_id = require_uuid16(
            root[1],
            field="publication-commit cleanup preparation_id",
        )
        row = work.connector.fetch_one(
            "SELECT committed.candidate_id, committed.preparation_id, "
            "committed.operational_policy_id, preparation.preparation_id, "
            "preparation.state, preparation.operational_policy_id, "
            "by_candidate.candidate_id, by_candidate.preparation_id, "
            "by_preparation.candidate_id, by_preparation.preparation_id "
            "FROM catalog_publication_commit_anchors AS anchor "
            "LEFT JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = anchor.receipt_id "
            "LEFT JOIN operational_operational_preparations AS preparation "
            "ON preparation.preparation_id = committed.preparation_id "
            "LEFT JOIN operational_publication_candidate_preparations AS "
            "by_candidate ON by_candidate.candidate_id = committed.candidate_id "
            "LEFT JOIN operational_publication_candidate_preparations AS "
            "by_preparation "
            "ON by_preparation.preparation_id = committed.preparation_id "
            "WHERE anchor.receipt_id = %s",
            (receipt_id,),
        )
        if not row or len(row) != 10:
            raise CleanupCorruptionError(
                "publication-commit preparation authority is incomplete"
            )
        try:
            candidate_id = require_uuid16(
                row[0], field="publication-commit preparation candidate_id"
            )
            preparation_id = require_uuid16(
                row[1], field="publication-commit preparation_id"
            )
            operational_policy_id = require_positive_int63(
                row[2], field="publication-commit operational_policy_id"
            )
        except (TypeError, ValueError) as error:
            raise CleanupCorruptionError(
                "publication-commit preparation identity is malformed"
            ) from error
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "publication-commit preparation differs from its frozen authority"
            )

        candidate_binding = row[6:8]
        preparation_binding = row[8:10]
        absent_binding = (None, None)
        exact_binding = (candidate_id, preparation_id)
        if candidate_binding not in {absent_binding, exact_binding} or (
            preparation_binding not in {absent_binding, exact_binding}
        ):
            raise CleanupCorruptionError(
                "publication-commit candidate/preparation binding differs"
            )
        if candidate_binding != preparation_binding:
            raise CleanupCorruptionError(
                "publication-commit candidate/preparation binding is partial"
            )
        if phase == "PCOM_PREPARATION_BINDING":
            if candidate_binding == absent_binding and not (
                _publication_commit_preparation_cursor_covers_root(
                    receipt_id=receipt_id,
                    candidate_id=candidate_id,
                    preparation_id=preparation_id,
                    phase=phase,
                    relation_index=relation_index,
                    cursor_values=cursor_values,
                )
            ):
                raise CleanupCorruptionError(
                    "publication-commit preparation binding disappeared before its phase"
                )
        elif candidate_binding != absent_binding:
            raise CleanupCorruptionError(
                "publication-commit preparation binding survived its cleanup phase"
            )

        if row[3:6] == (preparation_id, "COMPLETE", operational_policy_id):
            continue
        if any(value is not None for value in row[3:6]):
            raise CleanupCorruptionError(
                "publication-commit preparation state or identity differs"
            )
        if not _publication_commit_preparation_cursor_covers_root(
            receipt_id=receipt_id,
            candidate_id=candidate_id,
            preparation_id=preparation_id,
            phase=phase,
            relation_index=relation_index,
            cursor_values=cursor_values,
        ):
            raise CleanupCorruptionError(
                "publication-commit preparation disappeared before its root phase"
            )


def _publication_commit_preparation_cursor_covers_root(
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    preparation_id: bytes,
    phase: str,
    relation_index: int,
    cursor_values: tuple[_StaticScalar, ...] | None,
) -> bool:
    if cursor_values is None:
        return False
    if relation_index != 0:
        raise CleanupCorruptionError(
            "publication-commit preparation ROOT cursor is malformed"
        )
    expected_arity = 3 if phase == "PCOM_PREPARATION_BINDING" else 2
    if (
        phase not in {"PCOM_PREPARATION_BINDING", "PCOM_PREPARATION"}
        or len(cursor_values) != expected_arity
    ):
        raise CleanupCorruptionError(
            "publication-commit preparation ROOT cursor is malformed"
        )
    cursor_receipt = require_uuid16(
        cursor_values[0], field="publication-commit preparation cursor receipt_id"
    )
    if phase == "PCOM_PREPARATION_BINDING":
        cursor_candidate = require_uuid16(
            cursor_values[1],
            field="publication-commit preparation cursor candidate_id",
        )
        cursor_preparation = require_uuid16(
            cursor_values[2],
            field="publication-commit preparation cursor preparation_id",
        )
        cursor_identity_matches = (
            candidate_id == cursor_candidate and preparation_id == cursor_preparation
        )
    else:
        cursor_preparation = require_uuid16(
            cursor_values[1],
            field="publication-commit preparation cursor preparation_id",
        )
        cursor_identity_matches = preparation_id == cursor_preparation
    return receipt_id < cursor_receipt or (
        receipt_id == cursor_receipt and cursor_identity_matches
    )


@dataclass(frozen=True, slots=True)
class _PublicationCommitEventCandidate:
    receipt_id: bytes
    preparation_id: bytes
    sequence_no: int
    event_id: bytes
    event_type: str

    @property
    def cursor_values(self) -> tuple[_StaticScalar, ...]:
        return (self.receipt_id, self.preparation_id, self.sequence_no)


def _publication_commit_event_cursor(
    cursor: bytes,
    *,
    plan: _StaticTargetPlan,
) -> tuple[bytes, bytes, int] | None:
    spec = plan.phases["PCOM_EVENT"]
    relation_index, values = _decode_static_cursor(
        cursor,
        spec,
        len(plan.root_key),
    )
    if values is None:
        return None
    if relation_index != 0 or len(values) != 3:
        raise CleanupCorruptionError("publication-commit EVENT cursor is malformed")
    return (
        require_uuid16(values[0], field="PCOM EVENT cursor receipt_id"),
        require_uuid16(values[1], field="PCOM EVENT cursor preparation_id"),
        require_int63(values[2], field="PCOM EVENT cursor sequence_no"),
    )


def _publication_commit_event_authority(
    work: VNextUnitOfWork,
    *,
    cleanup_id: bytes,
    receipt_id: bytes,
) -> tuple[bytes, int]:
    row = work.connector.fetch_one(
        "SELECT committed.preparation_id, seal.event_count "
        "FROM catalog_publication_commit_anchors AS r "
        "JOIN catalog_publication_commits AS committed "
        "ON committed.receipt_id = r.receipt_id "
        "JOIN operational_operational_preparation_effect_seals AS seal "
        "ON seal.preparation_id = committed.preparation_id "
        "JOIN operational_operational_event_streams AS stream "
        "ON stream.preparation_id = committed.preparation_id "
        f"WHERE ({_PUBLICATION_COMMIT_EVENT_ELIGIBILITY}) "
        "AND r.receipt_id = %s",
        (cleanup_id, receipt_id),
    )
    if not row or len(row) != 2:
        raise CleanupRetentionBlockedError("PUBLICATION_COMMIT EVENT authority changed")
    return (
        require_uuid16(row[0], field="PCOM EVENT preparation_id"),
        require_int63(row[1], field="PCOM EVENT event_count"),
    )


def _validate_publication_commit_event_row(
    row: Sequence[object],
    *,
    receipt_id: bytes,
    preparation_id: bytes,
    expected_sequence_no: int,
) -> _PublicationCommitEventCandidate:
    if len(row) != 5:
        raise CleanupCorruptionError("PCOM EVENT row shape is invalid")
    sequence_no = require_int63(row[0], field="PCOM EVENT sequence_no")
    if sequence_no != expected_sequence_no:
        raise CleanupCorruptionError(
            "PCOM EVENT sequence coordinates are not contiguous"
        )
    event_id = require_uuid16(row[1], field="PCOM EVENT event_id")
    event_type = _as_text(row[2], field="PCOM EVENT event_type")
    removed_event_id = row[3]
    deletion_event_id = row[4]
    if event_type == "REMOVED_GID":
        if removed_event_id != event_id or deletion_event_id is not None:
            raise CleanupCorruptionError(
                "PCOM EVENT lacks its exact REMOVED_GID subtype"
            )
    elif event_type == "DELETION_CONSUMPTION":
        if deletion_event_id != event_id or removed_event_id is not None:
            raise CleanupCorruptionError(
                "PCOM EVENT lacks its exact DELETION_CONSUMPTION subtype"
            )
    else:
        raise CleanupCorruptionError("PCOM EVENT type is outside the closed registry")
    return _PublicationCommitEventCandidate(
        receipt_id,
        preparation_id,
        sequence_no,
        event_id,
        event_type,
    )


def _run_publication_commit_event_phase(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    cursor: bytes,
) -> _Mutation:
    """Atomically retire each exact subtype/base-event coordinate."""

    plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
    frozen_roots = _load_frozen_roots(work, cycle, plan)
    event_cursor = _publication_commit_event_cursor(cursor, plan=plan)
    frozen_authorities = tuple(
        (
            require_uuid16(root[0], field="PCOM EVENT frozen receipt_id"),
            require_uuid16(root[1], field="PCOM EVENT frozen preparation_id"),
        )
        for root in frozen_roots
    )
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    if event_cursor is not None and event_cursor[0] not in frozen_receipts:
        raise CleanupCorruptionError("PCOM EVENT cursor is outside its frozen roots")

    candidates: list[_PublicationCommitEventCandidate] = []
    remaining = cycle.max_rows_per_transaction
    for receipt_id, frozen_preparation_id in frozen_authorities:
        preparation_id, event_count = _publication_commit_event_authority(
            work,
            cleanup_id=cycle.cleanup_id,
            receipt_id=receipt_id,
        )
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "PCOM EVENT preparation differs from its frozen authority"
            )
        start_sequence = 0
        if event_cursor is not None:
            cursor_receipt, cursor_preparation, cursor_sequence = event_cursor
            if receipt_id < cursor_receipt:
                start_sequence = event_count
            elif receipt_id == cursor_receipt:
                if (
                    cursor_preparation != preparation_id
                    or cursor_sequence >= event_count
                ):
                    raise CleanupCorruptionError(
                        "PCOM EVENT cursor identity exceeds its sealed authority"
                    )
                start_sequence = cursor_sequence + 1

        if start_sequence:
            covered = work.connector.fetch_one(
                "SELECT 1 FROM operational_operational_events "
                "WHERE preparation_id = %s AND sequence_no < %s LIMIT 1",
                (preparation_id, start_sequence),
            )
            if covered:
                if covered != (1,):
                    raise CleanupCorruptionError(
                        "PCOM EVENT covered-coordinate probe is malformed"
                    )
                raise CleanupCorruptionError(
                    "PCOM EVENT cursor-covered coordinate reappeared"
                )

        if start_sequence == event_count:
            extra = work.connector.fetch_one(
                "SELECT 1 FROM operational_operational_events "
                "WHERE preparation_id = %s AND sequence_no >= %s LIMIT 1",
                (preparation_id, event_count),
            )
            if extra:
                raise CleanupCorruptionError(
                    "PCOM EVENT row exceeds the immutable seal count"
                )
            continue
        if start_sequence > event_count:
            raise CleanupCorruptionError("PCOM EVENT cursor exceeds the seal count")
        if remaining == 0:
            continue

        rows = work.connector.fetch_all(
            "SELECT event.sequence_no, event.event_id, event.event_type, "
            "removed.event_id, consumed.event_id "
            "FROM operational_operational_events AS event "
            "LEFT JOIN operational_operational_removed_gid_events AS removed "
            "ON removed.event_id = event.event_id "
            "LEFT JOIN operational_operational_deletion_consumption_events AS consumed "
            "ON consumed.event_id = event.event_id "
            "WHERE event.preparation_id = %s AND event.sequence_no >= %s "
            "ORDER BY event.sequence_no LIMIT %s",
            (preparation_id, start_sequence, remaining),
        )
        if not rows:
            raise CleanupCorruptionError(
                "PCOM EVENT has an uncovered sealed sequence gap"
            )
        expected = start_sequence
        for row in rows:
            candidate = _validate_publication_commit_event_row(
                row,
                receipt_id=receipt_id,
                preparation_id=preparation_id,
                expected_sequence_no=expected,
            )
            if candidate.sequence_no >= event_count:
                raise CleanupCorruptionError(
                    "PCOM EVENT row exceeds the immutable seal count"
                )
            candidates.append(candidate)
            expected += 1
        if len(rows) < remaining and expected != event_count:
            raise CleanupCorruptionError(
                "PCOM EVENT has an uncovered sealed sequence gap"
            )
        remaining -= len(rows)

    for candidate in sorted(
        candidates,
        key=lambda value: encode_lock_key(
            "cleanup-static",
            CleanupTargetKind.PUBLICATION_COMMIT.value,
            "PCOM_EVENT",
            0,
            *value.cursor_values,
        ),
    ):
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "cleanup-static",
                CleanupTargetKind.PUBLICATION_COMMIT.value,
                "PCOM_EVENT",
                0,
                *candidate.cursor_values,
            ),
            "SELECT event.sequence_no, event.event_id, event.event_type, "
            "removed.event_id, consumed.event_id "
            "FROM operational_operational_events AS event "
            "LEFT JOIN operational_operational_removed_gid_events AS removed "
            "ON removed.event_id = event.event_id "
            "LEFT JOIN operational_operational_deletion_consumption_events AS consumed "
            "ON consumed.event_id = event.event_id "
            "WHERE event.preparation_id = %s AND event.sequence_no = %s",
            (candidate.preparation_id, candidate.sequence_no),
        )
        exact = _validate_publication_commit_event_row(
            locked,
            receipt_id=candidate.receipt_id,
            preparation_id=candidate.preparation_id,
            expected_sequence_no=candidate.sequence_no,
        )
        if exact != candidate:
            raise CleanupRetentionBlockedError("PCOM EVENT coordinate changed")
        subtype_table = (
            "operational_operational_removed_gid_events"
            if candidate.event_type == "REMOVED_GID"
            else "operational_operational_deletion_consumption_events"
        )
        if (
            work.connector.execute_affected(
                f"DELETE FROM {subtype_table} WHERE event_id = %s",
                (candidate.event_id,),
            )
            != 1
        ):
            raise CleanupUnavailableError("PCOM EVENT subtype changed")
        if (
            work.connector.execute_affected(
                "DELETE FROM operational_operational_events "
                "WHERE event_id = %s AND preparation_id = %s AND sequence_no = %s",
                (
                    candidate.event_id,
                    candidate.preparation_id,
                    candidate.sequence_no,
                ),
            )
            != 1
        ):
            raise CleanupUnavailableError("PCOM EVENT base row changed")

    row_keys = tuple(
        _encode_static_cursor(0, candidate.cursor_values) for candidate in candidates
    )
    next_cursor = row_keys[-1] if row_keys else cursor
    return _Mutation(next_cursor, row_keys)


def _run_publication_commit_effect_root_phase(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    cursor: bytes,
) -> _Mutation:
    """Delete every frozen commit/seal/stream triple in one bounded transaction."""

    plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
    specs = plan.phases["PCOM_COMMIT_EFFECT_ROOT"]
    relation_index, cursor_values = _decode_static_cursor(
        cursor,
        specs,
        len(plan.root_key),
    )
    frozen_roots = _load_frozen_roots(work, cycle, plan)
    frozen_authorities = tuple(
        (
            require_uuid16(root[0], field="PCOM compound frozen receipt_id"),
            require_uuid16(root[1], field="PCOM compound frozen preparation_id"),
        )
        for root in frozen_roots
    )
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    spec = specs[0]
    if cursor_values is not None:
        if relation_index != 0 or len(cursor_values) != 3:
            raise CleanupCorruptionError("PCOM compound cursor is malformed")
        cursor_root = require_uuid16(
            cursor_values[0], field="PCOM compound cursor root receipt_id"
        )
        cursor_receipt = require_uuid16(
            cursor_values[1], field="PCOM compound cursor commit receipt_id"
        )
        cursor_preparation = require_uuid16(
            cursor_values[2], field="PCOM compound cursor preparation_id"
        )
        if (
            not frozen_receipts
            or cursor_root != frozen_receipts[-1]
            or cursor_receipt != cursor_root
            or cursor_preparation != frozen_authorities[-1][1]
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor does not cover the exact frozen root set"
            )
        receipt = work.connector.fetch_one(
            f"SELECT start_cursor, next_cursor, row_count FROM {_RECEIPT_TABLE} "
            "WHERE cleanup_id = %s AND phase = 'PCOM_COMMIT_EFFECT_ROOT'",
            (cycle.cleanup_id,),
        )
        if receipt != (_EMPTY_CURSOR, cursor, len(frozen_receipts)):
            raise CleanupCorruptionError(
                "PCOM compound cursor lacks its exact one-batch receipt proof"
            )
        for receipt_id, preparation_id in frozen_authorities:
            if work.connector.fetch_one(
                "SELECT 1 FROM catalog_publication_commits "
                "WHERE receipt_id = %s LIMIT 1",
                (receipt_id,),
            ):
                raise CleanupCorruptionError(
                    "PCOM compound cursor-covered commit reappeared"
                )
            _require_publication_commit_compound_authority_absent(
                work,
                preparation_id=preparation_id,
            )
        return _Mutation(cursor, ())

    candidates: list[tuple[_StaticScalar, ...]] = []
    for receipt_id, frozen_preparation_id in frozen_authorities:
        row = work.connector.fetch_one(
            "SELECT r.receipt_id, committed.receipt_id, committed.preparation_id "
            "FROM catalog_publication_commit_anchors AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id "
            "JOIN operational_operational_preparation_effect_seals AS seal "
            "ON seal.preparation_id = committed.preparation_id "
            "JOIN operational_operational_event_streams AS stream "
            "ON stream.preparation_id = committed.preparation_id "
            f"WHERE ({_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY}) "
            "AND r.receipt_id = %s",
            (cycle.cleanup_id, receipt_id),
        )
        if not row or len(row) != 3:
            raise CleanupCorruptionError(
                "PCOM compound root is missing or only partially present"
            )
        candidate = _static_values(row)
        if candidate[0] != receipt_id or candidate[1] != receipt_id:
            raise CleanupCorruptionError("PCOM compound receipt identity differs")
        preparation_id = require_uuid16(
            candidate[2], field="PCOM compound preparation_id"
        )
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "PCOM compound preparation differs from its frozen authority"
            )
        candidates.append(candidate)

    for candidate in sorted(
        candidates,
        key=lambda value: encode_lock_key(
            "cleanup-static",
            CleanupTargetKind.PUBLICATION_COMMIT.value,
            "PCOM_COMMIT_EFFECT_ROOT",
            0,
            *value,
        ),
    ):
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "cleanup-static",
                CleanupTargetKind.PUBLICATION_COMMIT.value,
                "PCOM_COMMIT_EFFECT_ROOT",
                0,
                *candidate,
            ),
            "SELECT r.receipt_id, committed.receipt_id, committed.preparation_id "
            "FROM catalog_publication_commit_anchors AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id "
            "JOIN operational_operational_preparation_effect_seals AS seal "
            "ON seal.preparation_id = committed.preparation_id "
            "JOIN operational_operational_event_streams AS stream "
            "ON stream.preparation_id = committed.preparation_id "
            f"WHERE ({_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY}) "
            "AND r.receipt_id = %s AND committed.receipt_id = %s "
            "AND committed.preparation_id = %s",
            (cycle.cleanup_id, *candidate),
        )
        if _static_values(locked) != candidate:
            raise CleanupRetentionBlockedError("PCOM compound root changed")
        primary = candidate[1:]
        for statement_index, statement in enumerate(spec.delete_sql):
            assert spec.delete_parameter_indexes is not None
            indexes = spec.delete_parameter_indexes[statement_index]
            parameters = tuple(primary[index] for index in indexes)
            if work.connector.execute_affected(statement, parameters) != 1:
                raise CleanupUnavailableError("PCOM compound root changed")

    for _receipt_id, preparation_id in frozen_authorities:
        _require_publication_commit_compound_authority_absent(
            work,
            preparation_id=preparation_id,
        )
    row_keys = tuple(_encode_static_cursor(0, candidate) for candidate in candidates)
    return _Mutation(row_keys[-1] if row_keys else cursor, row_keys)


def _require_publication_commit_compound_authority_absent(
    work: VNextUnitOfWork,
    *,
    preparation_id: bytes,
) -> None:
    for table in (
        "operational_publication_candidate_preparations",
        "operational_operational_preparation_batch_receipts",
        "operational_operational_preparation_checkpoints",
        "operational_operational_preparations",
        "operational_operational_events",
        "operational_operational_preparation_effect_seals",
        "operational_operational_event_streams",
    ):
        if work.connector.fetch_one(
            f"SELECT 1 FROM {table} WHERE preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor-covered preparation authority reappeared"
            )
    for subtype_table in (
        "operational_operational_removed_gid_events",
        "operational_operational_deletion_consumption_events",
    ):
        if work.connector.fetch_one(
            f"SELECT 1 FROM {subtype_table} AS subtype "
            "JOIN operational_operational_events AS event "
            "ON event.event_id = subtype.event_id "
            "WHERE event.preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor-covered typed event reappeared"
            )


def _require_publication_commit_post_compound_transition(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
    phase: str,
    cursor: bytes,
) -> None:
    plan = _STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT]
    if phase not in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}:
        raise CleanupCorruptionError("PCOM post-compound phase is invalid")
    relation_index, cursor_values = _decode_static_cursor(
        cursor,
        plan.phases[phase],
        len(plan.root_key),
    )
    cursor_receipt: bytes | None = None
    if cursor_values is not None:
        if relation_index != 0 or len(cursor_values) != 2:
            raise CleanupCorruptionError("PCOM post-compound cursor is malformed")
        cursor_receipt = require_uuid16(
            cursor_values[0], field="PCOM post-compound cursor root receipt_id"
        )
        if (
            require_uuid16(
                cursor_values[1],
                field="PCOM post-compound cursor row receipt_id",
            )
            != cursor_receipt
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound cursor receipt identity differs"
            )

    frozen_roots = _load_frozen_roots(work, cycle, plan)
    frozen_receipts = tuple(
        require_uuid16(root[0], field="PCOM post-compound frozen receipt_id")
        for root in frozen_roots
    )
    if cursor_receipt is not None and cursor_receipt not in frozen_receipts:
        raise CleanupCorruptionError(
            "PCOM post-compound cursor is outside its frozen roots"
        )
    for root in frozen_roots:
        receipt_id = require_uuid16(
            root[0], field="PCOM post-compound frozen receipt_id"
        )
        preparation_id = require_uuid16(
            root[1], field="PCOM post-compound frozen preparation_id"
        )
        if work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound commit authority reappeared"
            )
        if work.connector.fetch_one(
            "SELECT 1 FROM catalog_source_build_base_publication_commits "
            "WHERE base_receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound source-build base pin reappeared"
            )
        _require_publication_commit_compound_authority_absent(
            work,
            preparation_id=preparation_id,
        )
        for table, label in (
            (
                "catalog_publication_commit_finalizations",
                "finalization marker",
            ),
            (
                "catalog_publication_finalization_batch_stored",
                "finalization batch",
            ),
        ):
            if work.connector.fetch_one(
                f"SELECT 1 FROM {table} WHERE receipt_id = %s LIMIT 1",
                (receipt_id,),
            ):
                raise CleanupCorruptionError(f"PCOM post-compound {label} reappeared")

        checkpoint_row = work.connector.fetch_one(
            "SELECT state FROM catalog_publication_finalization_checkpoints "
            "WHERE receipt_id = %s",
            (receipt_id,),
        )
        anchor_row = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commit_anchors WHERE receipt_id = %s",
            (receipt_id,),
        )
        covered = cursor_receipt is not None and receipt_id <= cursor_receipt
        if phase == "PCOM_FINALIZATION_CHECKPOINT":
            if (checkpoint_row == ()) != covered:
                raise CleanupCorruptionError(
                    "PCOM finalization-checkpoint cursor coverage differs"
                )
            if not covered and checkpoint_row != ("COMPLETE",):
                raise CleanupCorruptionError(
                    "PCOM uncovered finalization checkpoint is not COMPLETE"
                )
            if anchor_row != (1,):
                raise CleanupCorruptionError(
                    "PCOM anchor disappeared before its cleanup phase"
                )
            continue
        if checkpoint_row:
            raise CleanupCorruptionError(
                "PCOM finalization checkpoint reappeared after its phase"
            )
        if (anchor_row == ()) != covered:
            raise CleanupCorruptionError("PCOM anchor cursor coverage differs")
        if not covered and anchor_row != (1,):
            raise CleanupCorruptionError("PCOM uncovered anchor authority differs")


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
    SELECT 1 FROM operational_gallery_observation_stagings x
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
AND NOT EXISTS (
    SELECT 1 FROM operational_source_build_generations m
    JOIN operational_ingest_generation_handoffs h ON h.generation = m.generation
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
      AND finalized.state = 'PROJECTION_FINALIZED'
      AND finalized.finalized_at IS NOT NULL)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_head_receipts head
    JOIN catalog_publication_receipts current
      ON current.receipt_id = head.receipt_id
    WHERE current.revision > r.revision
      AND current.state = 'PROJECTION_FINALIZED'
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


_PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY = """
EXISTS (
    SELECT 1
    FROM catalog_publication_commits committed
    JOIN catalog_publication_finalization_checkpoints checkpoint
      ON checkpoint.receipt_id = committed.receipt_id
     AND checkpoint.state = 'COMPLETE'
    JOIN catalog_source_revision_descriptors source_revision
      ON source_revision.source_revision = committed.source_revision
    JOIN catalog_publication_commit_head_receipts head
      ON head.channel = source_revision.channel
    JOIN catalog_publication_commits replacement
      ON replacement.receipt_id = head.receipt_id
    JOIN catalog_publication_receipts replacement_receipt
      ON replacement_receipt.receipt_id = replacement.receipt_id
    WHERE committed.receipt_id = r.receipt_id
      AND replacement.revision > committed.revision
      AND replacement_receipt.state = 'PROJECTION_FINALIZED'
      AND replacement_receipt.finalized_at IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM catalog_publication_commit_head_receipts retained_head
          WHERE retained_head.receipt_id = committed.receipt_id)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_publication_candidate_base_publication_commits base
          WHERE base.base_receipt_id = committed.receipt_id)
      AND NOT EXISTS (
          SELECT 1 FROM operational_gallery_redownload_states protected
          WHERE protected.through_source_revision = committed.source_revision)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_prepared_artifacts protected
          WHERE protected.candidate_id = committed.candidate_id
            AND protected.state IN ('PENDING', 'PREPARED'))
      AND NOT EXISTS (
          SELECT 1
          FROM operational_operational_preparations preparation
          JOIN operational_source_working_builds working
            ON working.build_id = preparation.build_id
          WHERE preparation.preparation_id = committed.preparation_id)
      AND NOT EXISTS (
          SELECT 1 FROM operational_catalog_working_candidates working
          WHERE working.candidate_id = committed.candidate_id))
"""

_PUBLICATION_COMMIT_SAFE_BUILD_BASE_RELEASE = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_source_build_base_publication_commits base
    LEFT JOIN catalog_source_build_states build_state
      ON build_state.build_id = base.build_id
    WHERE base.base_receipt_id = r.receipt_id
      AND (
        build_state.build_id IS NULL
        OR build_state.state = 'OPEN'
        OR EXISTS (
            SELECT 1 FROM operational_source_working_builds working
            WHERE working.build_id = base.build_id)
        OR NOT EXISTS (
            SELECT 1
            FROM catalog_analysis_run_descriptor completed_analysis
            JOIN catalog_analysis_run_states completed_state
              ON completed_state.analysis_id = completed_analysis.analysis_id
            WHERE completed_analysis.build_id = base.build_id
              AND completed_state.state = 'COMPLETE')
        OR EXISTS (
            SELECT 1
            FROM catalog_analysis_run_descriptor analysis
            LEFT JOIN catalog_analysis_run_states analysis_state
              ON analysis_state.analysis_id = analysis.analysis_id
            WHERE analysis.build_id = base.build_id
              AND (
                analysis_state.analysis_id IS NULL
                OR analysis_state.state NOT IN ('COMPLETE', 'ABANDONED')
                OR (
                  analysis_state.state = 'COMPLETE'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM catalog_source_revision_provenance provenance
                      JOIN catalog_publication_commits handoff
                        ON handoff.source_revision = provenance.source_revision
                      JOIN catalog_publication_commit_head_receipts handoff_head
                        ON handoff_head.receipt_id = handoff.receipt_id
                      JOIN catalog_publication_receipts handoff_receipt
                        ON handoff_receipt.receipt_id = handoff.receipt_id
                      JOIN catalog_source_revision_descriptors handoff_source
                        ON handoff_source.source_revision = handoff.source_revision
                      JOIN catalog_publication_commits base_commit
                        ON base_commit.receipt_id = base.base_receipt_id
                      JOIN catalog_source_revision_descriptors base_source
                        ON base_source.source_revision = base_commit.source_revision
                      WHERE provenance.analysis_id = analysis.analysis_id
                        AND handoff_head.channel = base_source.channel
                        AND handoff_source.channel = handoff_head.channel
                        AND handoff.revision > base_commit.revision
                        AND handoff_receipt.state = 'PROJECTION_FINALIZED'
                        AND handoff_receipt.finalized_at IS NOT NULL))))))
"""

_PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY = """
AND EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparations preparation
      ON preparation.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id
      AND preparation.state = 'COMPLETE'
      AND preparation.operational_policy_id =
          preparation_owner.operational_policy_id)
"""

_PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY = """
AND EXISTS (
    SELECT 1 FROM catalog_publication_commits preparation_owner
    JOIN operational_publication_candidate_preparations binding
      ON binding.candidate_id = preparation_owner.candidate_id
     AND binding.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id
)
"""

_PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_publication_candidate_preparations binding
      ON (binding.candidate_id = preparation_owner.candidate_id
          OR binding.preparation_id = preparation_owner.preparation_id)
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparation_batch_receipts batch
      ON batch.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparation_checkpoints checkpoint
      ON checkpoint.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparations preparation
      ON preparation.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_EVENTS_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    JOIN operational_operational_removed_gid_events removed
      ON removed.event_id = event.event_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    JOIN operational_operational_deletion_consumption_events consumed
      ON consumed.event_id = event.event_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_SAFE_BUILD_BASE_RELEASE
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY
    + """
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
"""
)

_PUBLICATION_COMMIT_AFTER_BUILD_BASE_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_RELEASE_BUILD_BASE'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_BINDING_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_BINDING'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_BATCH_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_BATCH'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_CHECKPOINT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_CHECKPOINT'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_EVENT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_EVENT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_EVENT'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_MARKER_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_MARKER'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_BATCH'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_COMPOUND_ROOT_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_COMMIT_EFFECT_ROOT'
      AND completed.state = 'COMPLETE')
"""

_PUBLICATION_COMMIT_AFTER_CHECKPOINT_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_checkpoints checkpoint
    WHERE checkpoint.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_CHECKPOINT'
      AND completed.state = 'COMPLETE')
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


def _publication_commit_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_commit_anchors"
    key = ("receipt_id",)
    return {
        "PCOM_RELEASE_BUILD_BASE": (
            _owned_spec(
                "catalog_source_build_base_publication_commits",
                ("build_id",),
                root,
                key,
                ("base_receipt_id",),
            ),
        ),
        "PCOM_PREPARATION_BINDING": (
            _indirect_spec(
                "operational_publication_candidate_preparations",
                ("candidate_id", "preparation_id"),
                "operational_publication_candidate_preparations AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.candidate_id = c.candidate_id "
                "AND committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION_BATCH": (
            _indirect_spec(
                "operational_operational_preparation_batch_receipts",
                ("preparation_id", "phase", "batch_key"),
                "operational_operational_preparation_batch_receipts AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION_CHECKPOINT": (
            _indirect_spec(
                "operational_operational_preparation_checkpoints",
                ("preparation_id", "phase"),
                "operational_operational_preparation_checkpoints AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION": (
            _indirect_spec(
                "operational_operational_preparations",
                ("preparation_id",),
                "operational_operational_preparations AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
                extra_predicate="c.state = 'COMPLETE'",
            ),
        ),
        "PCOM_EVENT": (
            _indirect_spec(
                "operational_operational_events",
                ("preparation_id", "sequence_no"),
                "operational_operational_events AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_FINALIZATION_MARKER": (
            _owned_spec(
                "catalog_publication_commit_finalizations",
                key,
                root,
                key,
            ),
        ),
        "PCOM_FINALIZATION_BATCH": (
            _owned_spec(
                "catalog_publication_finalization_batch_stored",
                ("receipt_id", "start_generation"),
                root,
                key,
            ),
        ),
        "PCOM_COMMIT_EFFECT_ROOT": (
            _indirect_spec(
                "catalog_publication_commits",
                ("receipt_id", "preparation_id"),
                "catalog_publication_commits AS c "
                "JOIN operational_operational_preparation_effect_seals AS seal "
                "ON seal.preparation_id = c.preparation_id "
                "JOIN operational_operational_event_streams AS stream "
                "ON stream.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = c.receipt_id",
                delete_sql=(
                    "DELETE FROM catalog_publication_commits "
                    "WHERE receipt_id = %s AND preparation_id = %s",
                    "DELETE FROM operational_operational_preparation_effect_seals "
                    "WHERE preparation_id = %s",
                    "DELETE FROM operational_operational_event_streams "
                    "WHERE preparation_id = %s",
                ),
                delete_parameter_indexes=((0, 1), (1,), (1,)),
                delete_allowed_affected=(
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                ),
            ),
        ),
        "PCOM_FINALIZATION_CHECKPOINT": (
            _owned_spec(
                "catalog_publication_finalization_checkpoints",
                key,
                root,
                key,
            ),
        ),
        "PCOM_ANCHOR": (_owned_spec(root, key, root, key),),
    }


def _catalog_revision_descriptor_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_revision_descriptors"
    key = ("revision",)
    return {"CRD_ROOT": (_owned_spec(root, key, root, key),)}


def _source_revision_descriptor_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_revision_descriptors"
    key = ("source_revision",)
    return {"SRD_ROOT": (_owned_spec(root, key, root, key),)}


def _publication_generation_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_generation_nodes"
    key = ("generation",)
    edge = "catalog_publication_generation_successors"
    return {
        "PG_EDGE": (
            _owned_spec(
                edge,
                ("successor_generation",),
                root,
                key,
                ("successor_generation",),
            ),
            _owned_spec(
                edge,
                ("successor_generation",),
                root,
                key,
                ("predecessor_generation",),
            ),
        ),
        "PG_ROOT": (_owned_spec(root, key, root, key),),
    }


def _catalog_publication_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_occurrence_identities"
    key = ("revision", "publication_key")

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    storage = _indirect_spec(
        "catalog_publication_storage",
        ("catalog_occurrence_sha256",),
        "catalog_publication_storage AS c "
        "JOIN catalog_publication_occurrence_identities AS r "
        "ON r.catalog_occurrence_sha256 = c.catalog_occurrence_sha256",
    )

    return {
        "CP_STORAGE": (storage,),
        "CP_CONTRIBUTOR_SEAL": (
            direct(
                "catalog_contributor_seals",
                ("revision", "publication_key", "position"),
            ),
        ),
        "CP_CONTRIBUTOR_IDENTITY": (
            direct(
                "catalog_contributor_identities",
                (
                    "revision",
                    "publication_key",
                    "contributor_name_sha256",
                    "role",
                ),
            ),
        ),
        "CP_CONTRIBUTOR_NAME": (
            direct(
                "catalog_contributor_name_sha256s",
                ("revision", "publication_key", "position"),
            ),
        ),
        "CP_CONTRIBUTOR_ROLE": (
            direct(
                "catalog_contributor_roles",
                ("revision", "publication_key", "position"),
            ),
        ),
        "CP_CONTRIBUTOR_ANCHOR": (
            direct(
                "catalog_contributor_anchors",
                ("revision", "publication_key", "position"),
            ),
        ),
        "CP_ORDER": (
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
        "CP_ROOT": (direct(root, key),),
    }


def _analysis_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_analysis_run_descriptor"
    key = ("analysis_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

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
            direct(table, pk)
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
            direct(table, ("analysis_id", "file_sha256"))
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
            ),
        ),
        "AR_EVIDENCE": tuple(
            direct(table, pk)
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
            ),
            direct(
                "catalog_analysis_exclusion_delta_new_excluded_flags",
                ("analysis_id", "file_sha256"),
            ),
        ),
        "AR_EXCLUSION_ANCHOR": (
            direct(
                "catalog_analysis_exclusion_delta_anchors",
                ("analysis_id", "file_sha256"),
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


def _publication_candidate_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_candidates"
    key = ("candidate_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    uncommitted = (
        "NOT EXISTS (SELECT 1 FROM catalog_publication_commits committed "
        "WHERE committed.candidate_id = r.candidate_id)"
    )

    prepared_source = (
        "catalog_prepared_artifacts AS c "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = c.candidate_id"
    )
    prepared_key = ("candidate_id", "publication_key")
    prepared = _indirect_spec(
        "catalog_prepared_artifacts",
        prepared_key,
        prepared_source,
        delete_sql=(
            "DELETE FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s",
        ),
    )
    selection_storage = _indirect_spec(
        "catalog_publication_selection_storage",
        ("selection_occurrence_sha256",),
        "catalog_publication_selection_storage AS c "
        "JOIN catalog_publication_selection_occurrence_identities AS occurrence "
        "ON occurrence.selection_occurrence_sha256 = "
        "c.selection_occurrence_sha256 "
        "JOIN catalog_publication_candidates AS r "
        "ON r.candidate_id = occurrence.candidate_id",
    )
    projection_storage = _indirect_spec(
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

    def projection(table: str, primary_key: tuple[str, ...]) -> _StaticDeleteSpec:
        return _indirect_spec(
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
        ),
        "PC_PREPARED": (
            prepared,
            projection(
                "catalog_contributor_seals",
                ("revision", "publication_key", "position"),
            ),
        ),
        "PC_INPUT": (
            direct(
                "catalog_candidate_artifact_inputs",
                ("candidate_id", "publication_key"),
            ),
            projection(
                "catalog_contributor_identities",
                (
                    "revision",
                    "publication_key",
                    "contributor_name_sha256",
                    "role",
                ),
            ),
        ),
        "PC_CONTRIBUTOR_NAME": (
            projection(
                "catalog_contributor_name_sha256s",
                ("revision", "publication_key", "position"),
            ),
        ),
        "PC_CONTRIBUTOR_ROLE": (
            projection(
                "catalog_contributor_roles",
                ("revision", "publication_key", "position"),
            ),
        ),
        "PC_CHECKPOINT": (
            direct("catalog_publication_checkpoints", ("candidate_id", "stage")),
            projection(
                "catalog_contributor_anchors",
                ("revision", "publication_key", "position"),
            ),
        ),
        "PC_SELECTION_STORAGE": (
            selection_storage,
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


def _source_build_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_build_descriptor"
    key = ("build_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    upload = _indirect_spec(
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


_GALLERY_OBSERVATION_STAGING_ELIGIBILITY = """
(
    (r.state IN ('SEALED', 'RETIRING_SEALED') AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        WHERE m.build_id = r.build_id AND m.gallery_id = r.gallery_id
          AND m.observation_id = r.observation_id))
    OR
    (r.state IN ('REUSED', 'RETIRING_REUSED') AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        WHERE m.build_id = r.build_id AND m.gallery_id = r.gallery_id
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
            WHERE linked.build_id = s.build_id
              AND linked.gallery_id = s.gallery_id
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
    return _owned_spec(
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
    return _indirect_spec(
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


def _gallery_observation_staging_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    request_identity = _indirect_spec(
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
            _owned_spec(
                "operational_gallery_observation_stagings",
                ("staging_id",),
                "operational_gallery_observation_stagings",
                ("staging_id",),
            ),
        ),
    }


def _observation_staging_direct(
    table: str, primary_key: tuple[str, ...]
) -> _StaticDeleteSpec:
    return _indirect_spec(
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
    return _indirect_spec(
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

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    request_identity = _indirect_spec(
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
            _indirect_spec(
                "operational_gallery_observation_stagings",
                ("staging_id",),
                "operational_gallery_observation_stagings AS c "
                "JOIN catalog_gallery_observation_allocations AS r "
                "ON r.gallery_id = c.gallery_id "
                "AND r.observation_id = c.observation_id",
            ),
        ),
        "GO_FACTS": tuple(
            direct(table, pk)
            for table, pk in (
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
            ),
        ),
        "GO_FILESYSTEM_VALUES": tuple(
            direct(table, ("gallery_id", "observation_id", "file_key"))
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
            ),
        ),
        "GO_FILES": (
            _owned_spec(
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
                    "DELETE FROM catalog_gallery_observation_file_anchors "
                    "WHERE gallery_id = %s AND observation_id = %s "
                    "AND file_key = %s",
                ),
            ),
        ),
        "GO_OBSERVATION_FACTS": tuple(
            direct(table, ("gallery_id", "observation_id"))
            for table in (
                "catalog_gallery_observation_metadata_locals",
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
        return _owned_spec(table, pk, root, key, extra_predicate=extra)

    abandoned = "r.state = 'ABANDONED'"
    subtype_source = (
        "{table} AS c JOIN operational_operational_events AS event "
        "ON event.event_id = c.event_id "
        "JOIN operational_operational_preparations AS r "
        "ON r.preparation_id = event.preparation_id"
    )
    abandoned_root = _owned_spec(
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
            _indirect_spec(
                "operational_operational_removed_gid_events",
                ("event_id",),
                subtype_source.format(
                    table="operational_operational_removed_gid_events"
                ),
                extra_predicate=abandoned,
            ),
            _indirect_spec(
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
            _owned_spec(
                "catalog_gallery_observation_page_children",
                ("parent_sha256", "position"),
                root,
                key,
                ("parent_sha256",),
            ),
        ),
        "GOP_BOUNDS": (
            _owned_spec(
                "catalog_gallery_observation_page_key_bounds_seals",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_key_bounds_first_keys",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_key_bounds_last_keys",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_key_bounds_anchors",
                ("page_sha256",),
                root,
                key,
            ),
        ),
        "GOP_DESCRIPTOR": (
            _owned_spec(
                "catalog_gallery_observation_page_descriptor_seals",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_descriptor_components",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_descriptor_levels",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_page_descriptor_subtree_item_counts",
                ("page_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_gallery_observation_pages",
                ("page_sha256",),
                root,
                key,
            ),
        ),
        "GOP_ROOT": (_owned_spec(root, key, root, key),),
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
            _owned_spec(
                "operational_gallery_observation_allocators",
                ("gallery_id",),
                root,
                key,
            ),
        ),
        "GI_SOURCE_NAME_ACCESS": (
            _owned_spec(
                "catalog_gallery_source_name_accesses",
                ("gallery_id",),
                root,
                key,
            ),
        ),
        "GI_ROOT": (_owned_spec(root, key, root, key),),
    }


_SOURCE_GALLERY_NAME_GID_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM catalog_gallery_source_name_accesses x
            WHERE x.source_gallery_name = r.source_gallery_name)
"""


def _source_gallery_name_gid_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_gallery_name_gids"
    key = ("source_gallery_name",)
    return {"SNG_ROOT": (_owned_spec(root, key, root, key),)}


_GALLERY_UPLOAD_TIME_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM catalog_source_gallery_name_gids x
            WHERE x.gid = r.gid)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_identities x
                WHERE x.gid = r.gid)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_impacted_gid_storage x
                WHERE x.gid = r.gid)
"""


def _gallery_upload_time_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_gallery_upload_times"
    key = ("gid",)
    return {"GUT_ROOT": (_owned_spec(root, key, root, key),)}


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
AND NOT EXISTS (
    SELECT 1 FROM operational_ingest_generation_handoffs handoff
    WHERE handoff.generation = r.generation)
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
    return {"CVU_ROOT": (_owned_spec(root, key, root, key),)}


def _live_display_title_choice(alias: str) -> str:
    child = _identifier(alias)
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
    child = _identifier(alias)
    return f"""
    EXISTS (
        SELECT 1
        FROM catalog_display_title_choices choice
        JOIN catalog_display_title_policies policy
          ON policy.display_title_policy_id = choice.display_title_policy_id
        WHERE policy.title_sort_policy_id = {child}.title_sort_policy_id
          AND choice.title_sha256 = {child}.title_sha256
          AND ({_live_display_title_choice("choice")}))
    """


_CANONICAL_VALUE_ELIGIBILITY = f"""
NOT EXISTS (SELECT 1 FROM operational_canonical_value_uploads x
            WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM operational_hash_cache_observations x
                WHERE x.source_identity_sha256 = r.value_sha256
                   OR x.fingerprint_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scopes scope_root
    JOIN catalog_source_build_descriptor build
      ON build.scope_key = scope_root.scope_key
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
    JOIN catalog_tag_term_identities term ON term.tag_id = x.tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_gallery_observation_artists x
    JOIN catalog_tag_term_identities term ON term.tag_id = x.artist_tag_id
    WHERE term.tag_value_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_subjects x
    JOIN catalog_tag_term_identities term ON term.tag_id = x.tag_id
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
AND NOT EXISTS (SELECT 1 FROM catalog_publication_contents x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributor_name_sha256s x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.summary_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_storage x
                WHERE x.source_title_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_blobs x
                WHERE x.artifact_locator_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_display_title_choices choice
    WHERE (choice.source_title_sha256 = r.value_sha256
           OR choice.title_sha256 = r.value_sha256)
      AND ({_live_display_title_choice("choice")}))
AND NOT EXISTS (
    SELECT 1 FROM catalog_title_sorts title_sort
    WHERE (title_sort.title_sha256 = r.value_sha256
           OR title_sort.sort_title_sha256 = r.value_sha256)
      AND ({_live_title_sort("title_sort")}))
"""


def _canonical_value_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_canonical_value_allocation_anchors"
    key = ("value_sha256",)
    display_title_choice = _indirect_spec(
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
    )
    title_sort = _indirect_spec(
        "catalog_title_sorts",
        ("title_sort_policy_id", "title_sha256"),
        "catalog_title_sorts AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.title_sha256 "
        "OR r.value_sha256 = c.sort_title_sha256",
        extra_predicate=f"NOT ({_live_title_sort('c')})",
    )
    source_scope = (
        _indirect_spec(
            "catalog_source_scopes",
            ("scope_key",),
            "catalog_source_scopes AS c "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = c.source_root_sha256",
        ),
    )
    locator = _indirect_spec(
        "catalog_source_locator_identity",
        ("locator_sha256",),
        "catalog_source_locator_identity AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.locator_sha256",
    )
    tag = _indirect_spec(
        "catalog_tag_term_anchors",
        ("tag_id",),
        "catalog_tag_term_anchors AS c "
        "JOIN catalog_tag_term_identities AS identity "
        "ON identity.tag_id = c.tag_id "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = identity.tag_value_sha256",
        delete_sql=(
            "DELETE FROM catalog_tag_term_seals WHERE tag_id = %s",
            "DELETE FROM catalog_tag_term_identities WHERE tag_id = %s",
            "DELETE FROM catalog_tag_term_anchors WHERE tag_id = %s",
        ),
    )
    snapshot = _indirect_spec(
        "catalog_source_snapshot_manifest_identity",
        ("snapshot_manifest_sha256",),
        "catalog_source_snapshot_manifest_identity AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.snapshot_manifest_sha256",
    )
    policy = _indirect_spec(
        "catalog_artifact_policies",
        ("artifact_policy_id",),
        "catalog_artifact_policies AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.policy_component_sha256",
    )
    semantic = _indirect_spec(
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
        _indirect_spec(
            "catalog_artifact_policy_semantics",
            ("policy_component_sha256",),
            "catalog_artifact_policy_semantics AS c "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = c.policy_component_sha256",
        ),
    )
    parent = _indirect_spec(
        "catalog_canonical_value_page_parents",
        ("parent_sha256", "position"),
        "catalog_canonical_value_page_parents AS c "
        "JOIN catalog_canonical_value_page_coordinates AS owned "
        "ON owned.page_sha256 = c.child_sha256 "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = owned.value_sha256",
    )
    page_seal = _indirect_spec(
        "catalog_canonical_value_page_seals",
        ("page_sha256",),
        "catalog_canonical_value_page_seals AS c "
        "JOIN catalog_canonical_value_page_coordinates AS owned "
        "ON owned.page_sha256 = c.page_sha256 "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = owned.value_sha256",
    )
    page_family = _indirect_spec(
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
    )
    return {
        "CV_DICTIONARY": (
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
            _owned_spec(
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
            _owned_spec(
                "catalog_canonical_value_allocation_seals",
                ("value_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_canonical_value_allocation_allocated_ats",
                ("value_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_canonical_value_allocation_byte_counts",
                ("value_sha256",),
                root,
                key,
            ),
            _owned_spec(
                "catalog_canonical_value_allocation_digest_domains",
                ("value_sha256",),
                root,
                key,
            ),
        ),
        "CV_ROOT": (_owned_spec(root, key, root, key),),
    }


_STATIC_PLANS: dict[CleanupTargetKind, _StaticTargetPlan] = {
    CleanupTargetKind.SOURCE_BUILD: _StaticTargetPlan(
        CleanupTargetKind.SOURCE_BUILD,
        "catalog_source_build_descriptor",
        ("build_id",),
        "build_id",
        16,
        _SOURCE_BUILD_ELIGIBILITY,
        _source_build_phases(),
    ),
    CleanupTargetKind.ANALYSIS_RUN: _StaticTargetPlan(
        CleanupTargetKind.ANALYSIS_RUN,
        "catalog_analysis_run_descriptor",
        ("analysis_id",),
        "analysis_id",
        16,
        _ANALYSIS_RUN_ELIGIBILITY,
        _analysis_phases(),
    ),
    CleanupTargetKind.CATALOG_PUBLICATION: _StaticTargetPlan(
        CleanupTargetKind.CATALOG_PUBLICATION,
        "catalog_publication_occurrence_identities",
        ("revision", "publication_key"),
        "publication_key",
        32,
        _CATALOG_PUBLICATION_ELIGIBILITY,
        _catalog_publication_phases(),
    ),
    CleanupTargetKind.PUBLICATION_COMMIT: _StaticTargetPlan(
        CleanupTargetKind.PUBLICATION_COMMIT,
        "catalog_publication_commit_anchors",
        ("receipt_id",),
        "receipt_id",
        16,
        _PUBLICATION_COMMIT_ELIGIBILITY,
        _publication_commit_phases(),
    ),
    CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR: _StaticTargetPlan(
        CleanupTargetKind.CATALOG_REVISION_DESCRIPTOR,
        "catalog_revision_descriptors",
        ("revision",),
        "revision",
        None,
        _CATALOG_REVISION_DESCRIPTOR_ELIGIBILITY,
        _catalog_revision_descriptor_phases(),
    ),
    CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR: _StaticTargetPlan(
        CleanupTargetKind.SOURCE_REVISION_DESCRIPTOR,
        "catalog_source_revision_descriptors",
        ("source_revision",),
        "source_revision",
        None,
        _SOURCE_REVISION_DESCRIPTOR_ELIGIBILITY,
        _source_revision_descriptor_phases(),
    ),
    CleanupTargetKind.PUBLICATION_GENERATION: _StaticTargetPlan(
        CleanupTargetKind.PUBLICATION_GENERATION,
        "catalog_publication_generation_nodes",
        ("generation",),
        "generation",
        None,
        _PUBLICATION_GENERATION_ELIGIBILITY,
        _publication_generation_phases(),
    ),
    CleanupTargetKind.PUBLICATION_CANDIDATE: _StaticTargetPlan(
        CleanupTargetKind.PUBLICATION_CANDIDATE,
        "catalog_publication_candidates",
        ("candidate_id",),
        "candidate_id",
        16,
        _PUBLICATION_CANDIDATE_ELIGIBILITY,
        _publication_candidate_phases(),
    ),
    CleanupTargetKind.OPERATIONAL_PREPARATION: _StaticTargetPlan(
        CleanupTargetKind.OPERATIONAL_PREPARATION,
        "operational_operational_preparations",
        ("preparation_id",),
        "preparation_id",
        16,
        _OPERATIONAL_PREPARATION_ELIGIBILITY,
        _operational_preparation_phases(),
    ),
    CleanupTargetKind.GALLERY_OBSERVATION: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION,
        "catalog_gallery_observation_allocations",
        ("gallery_id", "observation_id"),
        "gallery_id",
        None,
        _GALLERY_OBSERVATION_ELIGIBILITY,
        _gallery_observation_phases(),
        uses_cutoff=True,
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
        "operational_gallery_observation_stagings",
        ("staging_id",),
        "staging_id",
        16,
        _GALLERY_OBSERVATION_STAGING_ELIGIBILITY,
        _gallery_observation_staging_phases(),
    ),
    CleanupTargetKind.CANONICAL_VALUE: _StaticTargetPlan(
        CleanupTargetKind.CANONICAL_VALUE,
        "catalog_canonical_value_allocation_anchors",
        ("value_sha256",),
        "value_sha256",
        32,
        _CANONICAL_VALUE_ELIGIBILITY,
        _canonical_value_phases(),
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
        "catalog_gallery_observation_page_descriptor_anchors",
        ("page_sha256",),
        "page_sha256",
        32,
        _GALLERY_PAGE_ELIGIBILITY,
        _gallery_page_phases(),
    ),
    CleanupTargetKind.GALLERY_IDENTITY: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_IDENTITY,
        "catalog_gallery_identities",
        ("gallery_id",),
        "gallery_id",
        None,
        _GALLERY_IDENTITY_ELIGIBILITY,
        _gallery_identity_phases(),
    ),
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID: _StaticTargetPlan(
        CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
        "catalog_source_gallery_name_gids",
        ("source_gallery_name",),
        "source_gallery_name",
        255,
        _SOURCE_GALLERY_NAME_GID_ELIGIBILITY,
        _source_gallery_name_gid_phases(),
        variable_width_shard=True,
    ),
    CleanupTargetKind.GALLERY_UPLOAD_TIME: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_UPLOAD_TIME,
        "catalog_gallery_upload_times",
        ("gid",),
        "gid",
        None,
        _GALLERY_UPLOAD_TIME_ELIGIBILITY,
        _gallery_upload_time_phases(),
    ),
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD: _StaticTargetPlan(
        CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
        "operational_canonical_value_uploads",
        ("generation", "value_sha256"),
        "value_sha256",
        32,
        _CANONICAL_UPLOAD_ELIGIBILITY,
        _canonical_upload_phases(),
        uses_cutoff=True,
    ),
    CleanupTargetKind.HASH_CACHE_OBSERVATION: _StaticTargetPlan(
        CleanupTargetKind.HASH_CACHE_OBSERVATION,
        "operational_hash_cache_observations",
        ("source_identity_sha256", "fingerprint_sha256"),
        "source_identity_sha256",
        32,
        _HASH_CACHE_ELIGIBILITY,
        _hash_cache_phases(),
    ),
}


def _load_open_current_only_cycle(work: VNextUnitOfWork) -> CleanupCycle | None:
    row = work.connector.fetch_one(
        f"""
        SELECT sweep.target_kind, sweep.shard_no, sweep.target_key,
               job.cleanup_id, job.cycle_generation, job.cycle_cutoff_at,
               job.algorithm_version, job.max_rows_per_transaction,
               job.hash_cache_max_age_microseconds
        FROM {_SWEEP_TABLE} AS sweep
        JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key
        WHERE job.state = 'OPEN'
        ORDER BY {_CURRENT_ONLY_OPEN_ORDER_SQL}, sweep.shard_no
        LIMIT 1
        """
    )
    if not row:
        return None
    if len(row) != 9:
        raise CleanupCorruptionError(
            "current-only interrupted-cycle probe returned an invalid shape"
        )
    try:
        kind = CleanupTargetKind(
            _as_text(row[0], field="current-only interrupted target_kind")
        )
    except ValueError as error:
        raise CleanupCorruptionError(
            "current-only interrupted cycle has an unknown target"
        ) from error
    if kind not in _MAINTENANCE_TARGET_PRIORITY:
        raise CleanupCorruptionError(
            "current-only interrupted cycle has an excluded target"
        )
    shard = _require_shard(row[1])
    if require_int63(row[6], field="cleanup algorithm_version") != (
        _CLEANUP_ALGORITHM_VERSION
    ):
        raise CleanupCorruptionError("cleanup algorithm_version is unsupported")
    cycle = CleanupCycle(
        cleanup_id=require_uuid16(row[3], field="stored cleanup_id"),
        target_kind=kind,
        shard_no=shard,
        target_key=require_digest32(row[2], field="stored target_key"),
        cycle_generation=require_positive_int63(
            row[4], field="stored cycle_generation"
        ),
        cycle_cutoff_at=require_int63(row[5], field="stored cycle_cutoff_at"),
        max_rows_per_transaction=_require_batch_bound(row[7]),
        hash_cache_max_age_microseconds=require_int63(
            row[8], field="stored hash_cache_max_age_microseconds"
        ),
    )
    if cycle.hash_cache_max_age_microseconds != 0:
        raise CleanupCorruptionError(
            "current-only OPEN cycle has an invalid hash-cache age policy"
        )
    return cycle


def _static_candidate_shard(plan: _StaticTargetPlan, value: object) -> int:
    if plan.shard_width is None:
        return require_int63(value, field="cleanup candidate integer shard") % 256
    if isinstance(value, str):
        payload = value.encode("utf-8", errors="strict")
    else:
        payload = require_bounded_bytes(
            value,
            field="cleanup candidate byte shard",
            minimum=1,
            maximum=1024,
        )
    if not payload:
        raise CleanupCorruptionError("cleanup candidate shard value is empty")
    if plan.variable_width_shard:
        if len(payload) > plan.shard_width:
            raise CleanupCorruptionError("cleanup candidate shard value is too wide")
    elif len(payload) != plan.shard_width:
        raise CleanupCorruptionError("cleanup candidate shard width is invalid")
    return payload[0]


def _next_static_candidate_shard(
    work: VNextUnitOfWork,
    plan: _StaticTargetPlan,
    *,
    cycle_cutoff_at: int,
) -> int | None:
    root_table = _identifier(plan.root_table)
    shard_column = _identifier(plan.shard_column)
    order = ", ".join(f"r.{_identifier(column)}" for column in plan.root_key)
    parameters: tuple[object, ...] = (cycle_cutoff_at,) if plan.uses_cutoff else ()
    row = work.connector.fetch_one(
        f"SELECT r.{shard_column} FROM {root_table} AS r "
        f"WHERE ({plan.eligibility}) ORDER BY {order} LIMIT 1",
        parameters,
    )
    if not row:
        return None
    if len(row) != 1:
        raise CleanupCorruptionError(
            f"{plan.kind.value} candidate probe returned an invalid shape"
        )
    return _static_candidate_shard(plan, row[0])


def _next_artifact_blob_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT artifact_blob.artifact_sha256
        FROM catalog_artifact_blobs AS artifact_blob
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_prepared_artifacts prepared
            WHERE prepared.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
            SELECT 1 FROM catalog_artifacts retained
            WHERE retained.artifact_sha256 = artifact_blob.artifact_sha256)
        ORDER BY artifact_blob.artifact_sha256
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="artifact blob candidate")


def _next_publication_identity_candidate_shard(
    work: VNextUnitOfWork,
) -> int | None:
    row = work.connector.fetch_one("""
        SELECT identity.publication_key
        FROM catalog_publication_identities AS identity
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_publication_occurrence_identities occurrence
            WHERE occurrence.publication_key = identity.publication_key)
          AND NOT EXISTS (
            SELECT 1
            FROM catalog_publication_selection_occurrence_identities selection
            WHERE selection.publication_key = identity.publication_key)
        ORDER BY identity.publication_key
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="publication identity candidate")


def _next_file_name_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT identity.file_key
        FROM catalog_file_name_identity_anchors AS identity
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_gallery_observation_file_anchors retained
            WHERE retained.file_key = identity.file_key)
        ORDER BY identity.file_key
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="file-name identity candidate")


def _next_content_blob_candidate_shard(work: VNextUnitOfWork) -> int | None:
    row = work.connector.fetch_one("""
        SELECT content_blob.file_sha256
        FROM catalog_content_blobs AS content_blob
        WHERE NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_file_sha256s x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_file_hash_occurrences x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_changed_file_hashes x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_exclusion_delta_anchors x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_anchors x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_occurrences x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_artists x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_gallery_artist_max x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_a_file_decision_shadow_seals x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM catalog_analysis_file_hash_decision_tombstone x WHERE x.file_sha256 = content_blob.file_sha256)
          AND NOT EXISTS (SELECT 1 FROM operational_file_hash_caches x WHERE x.file_sha256 = content_blob.file_sha256)
        ORDER BY content_blob.file_sha256
        LIMIT 1
        """)
    return _digest_candidate_shard(row, field="content blob candidate")


def _digest_candidate_shard(
    row: tuple[object, ...] | None, *, field: str
) -> int | None:
    if not row:
        return None
    if len(row) != 1:
        raise CleanupCorruptionError(f"{field} probe returned an invalid shape")
    return require_digest32(row[0], field=field)[0]


def _next_current_only_candidate(
    work: VNextUnitOfWork, *, cycle_cutoff_at: int
) -> tuple[CleanupTargetKind, int] | None:
    dynamic = {
        CleanupTargetKind.ARTIFACT_BLOB: _next_artifact_blob_candidate_shard,
        CleanupTargetKind.PUBLICATION_IDENTITY: (
            _next_publication_identity_candidate_shard
        ),
        CleanupTargetKind.FILE_NAME_IDENTITY: _next_file_name_candidate_shard,
        CleanupTargetKind.CONTENT_BLOB: _next_content_blob_candidate_shard,
    }
    for kind in _CURRENT_ONLY_TARGET_PRIORITY:
        plan = _STATIC_PLANS.get(kind)
        if plan is not None:
            shard = _next_static_candidate_shard(
                work, plan, cycle_cutoff_at=cycle_cutoff_at
            )
        else:
            shard = dynamic[kind](work)
        if shard is not None:
            return kind, shard
    return None


def _catalog_publication_payload_is_blocked(work: VNextUnitOfWork) -> bool:
    row = work.connector.fetch_one(
        """
        SELECT 1
        FROM catalog_publication_occurrence_identities AS publication
        JOIN catalog_publication_commit_head_receipts AS head
          ON head.channel = %s
        JOIN catalog_publication_commits AS current
          ON current.receipt_id = head.receipt_id
        WHERE publication.revision < current.revision
        LIMIT 1
        """,
        (b"default",),
    )
    if row and row != (1,):
        raise CleanupCorruptionError(
            "current-only blocked-payload probe returned an invalid shape"
        )
    return bool(row)


def _publication_candidate_payload_is_blocked(work: VNextUnitOfWork) -> bool:
    row = work.connector.fetch_one("""
        SELECT 1
        FROM catalog_publication_candidates AS candidate
        WHERE NOT EXISTS (
            SELECT 1 FROM operational_catalog_working_candidates working
            WHERE working.candidate_id = candidate.candidate_id)
          AND EXISTS (
            SELECT 1 FROM catalog_prepared_artifacts prepared
            WHERE prepared.candidate_id = candidate.candidate_id
              AND prepared.state IN ('PENDING', 'PREPARED'))
        LIMIT 1
        """)
    if row and row != (1,):
        raise CleanupCorruptionError(
            "current-only blocked-candidate probe returned an invalid shape"
        )
    return bool(row)


def _static_strategy(kind: CleanupTargetKind) -> _Strategy:
    phases = tuple(_STATIC_PLANS[kind].phases)
    return _Strategy(
        phases,
        tuple(_static_mutator(kind, phase) for phase in phases),
    )


_STRATEGIES: dict[CleanupTargetKind, _Strategy] = {
    CleanupTargetKind.SOURCE_BUILD: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD].phases),
        tuple(
            _source_build_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.SOURCE_BUILD].phases
        ),
    ),
    CleanupTargetKind.ANALYSIS_RUN: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases),
        tuple(
            _analysis_run_mutator(phase)
            for phase in _STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN].phases
        ),
    ),
    CleanupTargetKind.CATALOG_PUBLICATION: _static_strategy(
        CleanupTargetKind.CATALOG_PUBLICATION
    ),
    CleanupTargetKind.PUBLICATION_COMMIT: _Strategy(
        tuple(_STATIC_PLANS[CleanupTargetKind.PUBLICATION_COMMIT].phases),
        tuple(
            _publication_commit_mutator(phase)
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
    CleanupTargetKind.GALLERY_OBSERVATION: _static_strategy(
        CleanupTargetKind.GALLERY_OBSERVATION
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING: _static_strategy(
        CleanupTargetKind.GALLERY_OBSERVATION_STAGING
    ),
    CleanupTargetKind.ARTIFACT_BLOB: _Strategy(
        ("AB_ROOT",),
        (_select_artifact_blobs,),
    ),
    CleanupTargetKind.CANONICAL_VALUE: _static_strategy(
        CleanupTargetKind.CANONICAL_VALUE
    ),
    CleanupTargetKind.CONTENT_BLOB: _Strategy(("CB_ROOT",), (_select_content_blobs,)),
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE: _static_strategy(
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE
    ),
    CleanupTargetKind.FILE_NAME_IDENTITY: _Strategy(
        ("FN_ROOT",), (_select_file_name_identities,)
    ),
    CleanupTargetKind.PUBLICATION_IDENTITY: _Strategy(
        ("PI_ROOT",), (_select_publication_identities,)
    ),
    CleanupTargetKind.GALLERY_IDENTITY: _static_strategy(
        CleanupTargetKind.GALLERY_IDENTITY
    ),
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID: _static_strategy(
        CleanupTargetKind.SOURCE_GALLERY_NAME_GID
    ),
    CleanupTargetKind.GALLERY_UPLOAD_TIME: _static_strategy(
        CleanupTargetKind.GALLERY_UPLOAD_TIME
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
