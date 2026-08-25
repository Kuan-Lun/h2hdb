"""Bounded, fixed-shard cleanup control for the vNext schema.

The cleanup tables contain *sweep* authorities, not arbitrary table names or
predicates.  Runtime dispatch therefore stays closed-world: every supported
target and phase below is bound to literal SQL owned by this module.  Database
text is never interpolated into a statement.

All seventeen provider-seeded target kinds are implemented here.  The large
child-first targets use source-owned, immutable statement specifications; the
database registry is checked for exact equality but is never treated as SQL or
as an authorization predicate.
"""

from __future__ import annotations

__all__ = [
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
_MAX_BATCH_ROWS = 256
_EMPTY_CURSOR = b""

_SWEEP_TABLE = "operational_cleanup_sweep_targets"
_PHASE_TABLE = "operational_cleanup_phases"
_JOB_TABLE = "operational_cleanup_jobs"
_CHECKPOINT_TABLE = "operational_cleanup_checkpoints"
_RECEIPT_TABLE = "operational_cleanup_batch_receipts"
_COMPLETION_TABLE = "operational_cleanup_completions"


class CleanupTargetKind(StrEnum):
    SOURCE_BUILD = "SOURCE_BUILD"
    ANALYSIS_RUN = "ANALYSIS_RUN"
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
        _validate_strategy_seeds(work, kind)

        expected_target_key = _target_key(kind, shard)
        row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("cleanup-sweep", expected_target_key),
            f"""
            SELECT s.target_key,
                   j.cleanup_id, j.cycle_generation, j.cycle_cutoff_at,
                   j.algorithm_version, j.max_rows_per_transaction,
                   j.hash_cache_max_age_microseconds, j.state,
                   j.created_at, j.completed_at,
                   c.cycle_generation, c.final_chain_sha256, c.deleted_count
            FROM {_SWEEP_TABLE} AS s
            LEFT JOIN {_JOB_TABLE} AS j ON j.target_key = s.target_key
            LEFT JOIN {_COMPLETION_TABLE} AS c ON c.target_key = s.target_key
            WHERE s.target_kind = %s AND s.shard_no = %s
            """,
            (kind.value, shard),
        )
        if not row or row[0] != expected_target_key:
            raise CleanupCorruptionError(
                "fixed cleanup sweep seed is missing or corrupt"
            )

        if row[1] is None:
            generation = 1
        else:
            existing = _cycle_from_job_row(kind, shard, row)
            state = _as_text(row[7], field="cleanup job state")
            if state == "OPEN":
                _require_cycle_policy(
                    existing,
                    cutoff=cutoff,
                    max_rows=max_rows,
                    max_age=max_age,
                )
                return existing
            if state != "COMPLETE":
                raise CleanupCorruptionError("cleanup job has an invalid state")
            _require_complete_job(row, existing)
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
                 hash_cache_max_age_microseconds, state, created_at, completed_at)
            VALUES (%s, %s, %s, %s, 1, %s, %s, 'OPEN', %s, NULL)
            """,
            (
                cycle.cleanup_id,
                cycle.target_key,
                cycle.cycle_generation,
                cycle.cycle_cutoff_at,
                cycle.max_rows_per_transaction,
                cycle.hash_cache_max_age_microseconds,
                timestamp,
            ),
        )
        first_phase = _STRATEGIES[kind].phases[0]
        _insert_checkpoint(
            work,
            cycle=cycle,
            phase=first_phase,
            chain_sha256=_initial_chain(cycle.cleanup_id, first_phase),
            now=timestamp,
        )
        return cycle

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
                 input_sha256, output_sha256, row_count,
                 committed_generation, committed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                requested.cleanup_id,
                checkpoint.phase,
                attempt.batch_key,
                checkpoint.cursor,
                mutation.next_cursor,
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
    if row[9] is None or row[10] != cycle.cycle_generation:
        raise CleanupCorruptionError("COMPLETE cleanup job lacks exact completion")
    require_digest32(row[11], field="cleanup final_chain_sha256")
    require_int63(row[12], field="cleanup completion deleted_count")


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
               j.hash_cache_max_age_microseconds, j.state,
               p.phase, p.generation, p.cursor_bytes, p.deleted_count,
               p.chain_sha256, p.state,
               r.batch_key, r.start_cursor, r.next_cursor,
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
    if row[2] != cycle.cycle_cutoff_at or row[3] != 1:
        raise CleanupCorruptionError("cleanup cycle immutable policy changed")
    if (
        row[4] != cycle.max_rows_per_transaction
        or row[5] != cycle.hash_cache_max_age_microseconds
    ):
        raise CleanupCorruptionError("cleanup cycle batch policy changed")
    state = _as_text(row[6], field="cleanup job state")
    if state == "COMPLETE":
        if row[7] is not None:
            raise CleanupCorruptionError("COMPLETE cleanup retains a checkpoint")
        return state, None
    if state != "OPEN" or row[7] is None:
        raise CleanupCorruptionError("OPEN cleanup lacks one current checkpoint")
    checkpoint = _Checkpoint(
        phase=_as_text(row[7], field="cleanup phase"),
        generation=require_positive_int63(
            row[8], field="cleanup checkpoint generation"
        ),
        cursor=require_bounded_bytes(row[9], field="cleanup cursor", maximum=2048),
        deleted_count=require_int63(row[10], field="cleanup deleted_count"),
        chain_sha256=require_digest32(row[11], field="cleanup chain_sha256"),
        state=_as_text(row[12], field="cleanup checkpoint state"),
        receipt_batch_key=(
            None
            if row[13] is None
            else require_digest32(row[13], field="cleanup receipt batch_key")
        ),
        receipt_generation=(
            None
            if row[19] is None
            else require_positive_int63(row[19], field="cleanup receipt generation")
        ),
        receipt_row_count=(
            None
            if row[18] is None
            else require_int63(row[18], field="cleanup receipt row_count")
        ),
        receipt_start_cursor=(
            None
            if row[14] is None
            else require_bounded_bytes(
                row[14], field="cleanup receipt start_cursor", maximum=2048
            )
        ),
        receipt_next_cursor=(
            None
            if row[15] is None
            else require_bounded_bytes(
                row[15], field="cleanup receipt next_cursor", maximum=2048
            )
        ),
        receipt_input_sha256=(
            None
            if row[16] is None
            else require_digest32(row[16], field="cleanup receipt input_sha256")
        ),
        receipt_output_sha256=(
            None
            if row[17] is None
            else require_digest32(row[17], field="cleanup receipt output_sha256")
        ),
        receipt_committed_at=(
            None
            if row[20] is None
            else require_int63(row[20], field="cleanup receipt committed_at")
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
    assert checkpoint.receipt_output_sha256 is not None
    if (
        checkpoint.receipt_generation != checkpoint.generation
        or checkpoint.receipt_next_cursor != checkpoint.cursor
        or checkpoint.receipt_output_sha256 != checkpoint.chain_sha256
        or checkpoint.receipt_row_count > cycle.max_rows_per_transaction
        or checkpoint.deleted_count < checkpoint.receipt_row_count
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
    require_digest32(row[4], field="cleanup terminal replay input_sha256")
    output_sha256 = require_digest32(
        row[5], field="cleanup terminal replay output_sha256"
    )
    row_count = require_int63(row[6], field="cleanup terminal replay row_count")
    receipt_generation = require_positive_int63(
        row[7], field="cleanup terminal replay receipt generation"
    )
    require_int63(row[8], field="cleanup terminal replay committed_at")
    prior_generation = require_positive_int63(
        row[9], field="cleanup terminal checkpoint generation"
    )
    prior_cursor = require_bounded_bytes(
        row[10], field="cleanup terminal checkpoint cursor", maximum=2048
    )
    prior_deleted = require_int63(
        row[11], field="cleanup terminal checkpoint deleted_count"
    )
    prior_chain = require_digest32(row[12], field="cleanup terminal checkpoint chain")
    prior_state = _as_text(row[13], field="cleanup terminal checkpoint state")
    prior_order = require_positive_int63(
        row[14], field="cleanup terminal prior phase_order"
    )
    current_order = require_positive_int63(
        row[15], field="cleanup terminal current phase_order"
    )
    if (
        batch_key != command.batch_key
        or row_count != 0
        or start_cursor != next_cursor
        or receipt_generation != command.expected_generation + 1
        or prior_generation != receipt_generation
        or prior_cursor != next_cursor
        or prior_deleted < row_count
        or prior_chain != output_sha256
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
              SELECT 1 FROM catalog_publication_anchors x
              WHERE x.publication_key = p.publication_key)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_publication_selections x
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
                  SELECT 1 FROM catalog_publication_anchors x
                  WHERE x.publication_key = p.publication_key)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_publication_selections x
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


def _select_artifact_locations(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    lower, upper, no_upper = _digest_bounds(cycle, cursor)
    rows = work.connector.fetch_all(
        """
        SELECT location.artifact_sha256
        FROM catalog_artifact_location AS location
        WHERE location.artifact_sha256 >= %s
          AND (%s = 0 OR location.artifact_sha256 > %s)
          AND (%s = 1 OR location.artifact_sha256 < %s)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_artifact_sha256s p
              WHERE p.artifact_sha256 = location.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_artifact_sha256s retained
              WHERE retained.artifact_sha256 = location.artifact_sha256)
        ORDER BY location.artifact_sha256
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
    keys = tuple(
        require_digest32(row[0], field="artifact location digest") for row in rows
    )
    for digest in keys:
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-artifact-location", digest),
            """
            SELECT location.artifact_sha256
            FROM catalog_artifact_location AS location
            WHERE location.artifact_sha256 = %s
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_prepared_artifact_sha256s p
                  WHERE p.artifact_sha256 = location.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_artifact_sha256s retained
                  WHERE retained.artifact_sha256 = location.artifact_sha256)
            """,
            (digest,),
        )
        if locked != (digest,):
            raise CleanupRetentionBlockedError("artifact blob gained a retention root")
        if (
            work.connector.execute_affected(
                "DELETE FROM catalog_artifact_location WHERE artifact_sha256 = %s",
                (digest,),
            )
            != 1
        ):
            raise CleanupUnavailableError("artifact location changed during cleanup")
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
              SELECT 1 FROM catalog_artifact_location location
              WHERE location.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_prepared_artifact_sha256s p
              WHERE p.artifact_sha256 = artifact_blob.artifact_sha256)
          AND NOT EXISTS (
              SELECT 1 FROM catalog_artifact_sha256s retained
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
                  SELECT 1 FROM catalog_artifact_location location
                  WHERE location.artifact_sha256 = artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_prepared_artifact_sha256s p
                  WHERE p.artifact_sha256 = artifact_blob.artifact_sha256)
              AND NOT EXISTS (
                  SELECT 1 FROM catalog_artifact_sha256s retained
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


def _hash_cache_cursor(cursor: bytes) -> tuple[bytes, bytes, int]:
    if not cursor:
        return bytes(32), bytes(32), 1
    value = require_bounded_bytes(
        cursor, field="hash-cache cleanup cursor", minimum=64, maximum=64
    )
    return value[:32], value[32:], 0


def _hash_cache_cutoff(cycle: CleanupCycle) -> int:
    if cycle.hash_cache_max_age_microseconds > cycle.cycle_cutoff_at:
        raise CleanupCorruptionError("hash-cache cleanup cutoff underflows")
    return cycle.cycle_cutoff_at - cycle.hash_cache_max_age_microseconds


def _select_hash_cache_files(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    source_cursor, fingerprint_cursor, first = _hash_cache_cursor(cursor)
    cutoff = _hash_cache_cutoff(cycle)
    lower = bytes((cycle.shard_no,)) + bytes(31)
    upper = b"" if cycle.shard_no == 255 else bytes((cycle.shard_no + 1,)) + bytes(31)
    no_upper = 1 if cycle.shard_no == 255 else 0
    rows = work.connector.fetch_all(
        """
        SELECT c.source_identity_sha256, c.fingerprint_sha256
        FROM operational_file_hash_caches AS c
        JOIN operational_hash_cache_observations AS o
          ON o.source_identity_sha256 = c.source_identity_sha256
         AND o.fingerprint_sha256 = c.fingerprint_sha256
        WHERE c.source_identity_sha256 >= %s
          AND (%s = 1 OR c.source_identity_sha256 < %s)
          AND (%s = 1 OR c.source_identity_sha256 > %s
               OR (c.source_identity_sha256 = %s AND c.fingerprint_sha256 > %s))
          AND o.observed_at <= %s
        ORDER BY c.source_identity_sha256, c.fingerprint_sha256
        LIMIT %s
        """,
        (
            lower,
            no_upper,
            upper,
            first,
            source_cursor,
            source_cursor,
            fingerprint_cursor,
            cutoff,
            cycle.max_rows_per_transaction,
        ),
    )
    pairs = tuple(
        (
            require_digest32(row[0], field="hash-cache source key"),
            require_digest32(row[1], field="hash-cache fingerprint key"),
        )
        for row in rows
    )
    keys = tuple(source + fingerprint for source, fingerprint in pairs)
    for source, fingerprint in pairs:
        lock_key = source + fingerprint
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-hash-cache-file", lock_key),
            """
            SELECT c.source_identity_sha256, c.fingerprint_sha256
            FROM operational_file_hash_caches AS c
            JOIN operational_hash_cache_observations AS o
              ON o.source_identity_sha256 = c.source_identity_sha256
             AND o.fingerprint_sha256 = c.fingerprint_sha256
            WHERE c.source_identity_sha256 = %s AND c.fingerprint_sha256 = %s
              AND o.observed_at <= %s
            """,
            (source, fingerprint, cutoff),
        )
        if locked != (source, fingerprint):
            raise CleanupRetentionBlockedError("hash-cache row became live")
        if (
            work.connector.execute_affected(
                "DELETE FROM operational_file_hash_caches "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            != 1
        ):
            raise CleanupUnavailableError("hash-cache file row changed")
    return _Mutation(keys[-1] if keys else cursor, keys)


def _select_hash_cache_observations(
    work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes
) -> _Mutation:
    source_cursor, fingerprint_cursor, first = _hash_cache_cursor(cursor)
    cutoff = _hash_cache_cutoff(cycle)
    lower = bytes((cycle.shard_no,)) + bytes(31)
    upper = b"" if cycle.shard_no == 255 else bytes((cycle.shard_no + 1,)) + bytes(31)
    no_upper = 1 if cycle.shard_no == 255 else 0
    rows = work.connector.fetch_all(
        """
        SELECT o.source_identity_sha256, o.fingerprint_sha256
        FROM operational_hash_cache_observations AS o
        WHERE o.source_identity_sha256 >= %s
          AND (%s = 1 OR o.source_identity_sha256 < %s)
          AND (%s = 1 OR o.source_identity_sha256 > %s
               OR (o.source_identity_sha256 = %s AND o.fingerprint_sha256 > %s))
          AND o.observed_at <= %s
          AND NOT EXISTS (
              SELECT 1 FROM operational_file_hash_caches c
              WHERE c.source_identity_sha256 = o.source_identity_sha256
                AND c.fingerprint_sha256 = o.fingerprint_sha256)
        ORDER BY o.source_identity_sha256, o.fingerprint_sha256
        LIMIT %s
        """,
        (
            lower,
            no_upper,
            upper,
            first,
            source_cursor,
            source_cursor,
            fingerprint_cursor,
            cutoff,
            cycle.max_rows_per_transaction,
        ),
    )
    pairs = tuple(
        (
            require_digest32(row[0], field="hash-cache source key"),
            require_digest32(row[1], field="hash-cache fingerprint key"),
        )
        for row in rows
    )
    keys = tuple(source + fingerprint for source, fingerprint in pairs)
    for source, fingerprint in pairs:
        lock_key = source + fingerprint
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key("cleanup-hash-cache-observation", lock_key),
            """
            SELECT o.source_identity_sha256, o.fingerprint_sha256
            FROM operational_hash_cache_observations AS o
            WHERE o.source_identity_sha256 = %s AND o.fingerprint_sha256 = %s
              AND o.observed_at <= %s
              AND NOT EXISTS (
                  SELECT 1 FROM operational_file_hash_caches c
                  WHERE c.source_identity_sha256 = o.source_identity_sha256
                    AND c.fingerprint_sha256 = o.fingerprint_sha256)
            """,
            (source, fingerprint, cutoff),
        )
        if locked != (source, fingerprint):
            raise CleanupRetentionBlockedError("hash-cache observation became live")
        if (
            work.connector.execute_affected(
                "DELETE FROM operational_hash_cache_observations "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            != 1
        ):
            raise CleanupUnavailableError("hash-cache observation changed")
    return _Mutation(keys[-1] if keys else cursor, keys)


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
    root_key: tuple[str, ...]
    shard_column: str
    shard_width: int | None
    eligibility: str
    phases: dict[str, tuple[_StaticDeleteSpec, ...]]
    uses_cutoff: bool = False
    variable_width_shard: bool = False


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
    offset = 4
    values: list[_StaticScalar] = []
    for _ in range(count):
        if offset >= len(payload):
            raise CleanupCorruptionError("cleanup static cursor is truncated")
        tag = payload[offset : offset + 1]
        offset += 1
        if tag == b"i":
            if offset + 8 > len(payload):
                raise CleanupCorruptionError("cleanup integer cursor is truncated")
            values.append(int.from_bytes(payload[offset : offset + 8], "big"))
            offset += 8
            continue
        if tag not in {b"b", b"s"} or offset + 2 > len(payload):
            raise CleanupCorruptionError("cleanup static cursor tag is invalid")
        size = int.from_bytes(payload[offset : offset + 2], "big")
        offset += 2
        if offset + size > len(payload):
            raise CleanupCorruptionError("cleanup byte cursor is truncated")
        value = payload[offset : offset + size]
        offset += size
        values.append(value if tag == b"b" else value.decode("utf-8", errors="strict"))
    if offset != len(payload):
        raise CleanupCorruptionError("cleanup static cursor has trailing bytes")
    return index, tuple(values)


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
    return (cycle.cycle_cutoff_at,) if plan.uses_cutoff else ()


def _static_select_sql(
    plan: _StaticTargetPlan,
    spec: _StaticDeleteSpec,
    *,
    exact: bool,
    has_after: bool = False,
) -> str:
    root_columns = tuple(f"r.{column}" for column in plan.root_key)
    primary_columns = tuple(f"c.{column}" for column in spec.primary_key)
    ordered = root_columns + primary_columns
    select = ", ".join(ordered)
    shard_sql = (
        f"MOD(r.{plan.shard_column}, 256) = %s"
        if plan.shard_width is None
        else f"r.{plan.shard_column} >= %s AND (%s = 1 OR r.{plan.shard_column} < %s)"
    )
    if exact:
        exact_pk = " AND ".join(f"{column} = %s" for column in primary_columns)
        return (
            f"SELECT {select} FROM {spec.source} WHERE ({plan.eligibility}) "
            f"AND ({spec.extra_predicate}) AND ({shard_sql}) AND {exact_pk}"
        )
    keyset_sql = ""
    if has_after:
        keyset_sql = f" AND ({_keyset_predicate(ordered)})"
    return (
        f"SELECT {select} FROM {spec.source} WHERE ({plan.eligibility}) "
        f"AND ({spec.extra_predicate}) AND ({shard_sql}) "
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
) -> _Mutation:
    specs = plan.phases[phase]
    start_index, start_values = _decode_static_cursor(cursor, specs, len(plan.root_key))
    deleted: list[bytes] = []
    next_cursor = cursor
    policy = _static_policy_parameters(plan, cycle)
    shard = _static_shard_parameters(plan, cycle)
    for index in range(start_index, len(specs)):
        spec = specs[index]
        ordered_arity = len(plan.root_key) + len(spec.primary_key)
        after = start_values if index == start_index else None
        if after is not None and len(after) != ordered_arity:
            raise CleanupCorruptionError("cleanup cursor does not match relation key")
        while len(deleted) < cycle.max_rows_per_transaction:
            remaining = cycle.max_rows_per_transaction - len(deleted)
            query = _static_select_sql(
                plan, spec, exact=False, has_after=after is not None
            )
            parameters: tuple[object, ...] = policy + shard
            if after is not None:
                parameters += _keyset_parameters(after)
            parameters += (remaining,)
            rows = work.connector.fetch_all(query, parameters)
            if not rows:
                break
            candidates = tuple(_static_values(row) for row in rows)
            for candidate in candidates:
                root = candidate[: len(plan.root_key)]
                primary = candidate[len(plan.root_key) :]
                exact_query = _static_select_sql(plan, spec, exact=True)
                exact_parameters = policy + shard + primary
                locked = work.lock_row(
                    LockRank.CHILD,
                    encode_lock_key(
                        "cleanup-static",
                        plan.kind.value,
                        phase,
                        index,
                        *root,
                        *primary,
                    ),
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
                after = candidate
                next_cursor = _encode_static_cursor(index, candidate)
                deleted.append(_encode_static_cursor(index, candidate))
            if len(candidates) < remaining:
                break
    return _Mutation(next_cursor, tuple(deleted))


def _static_mutator(kind: CleanupTargetKind, phase: str) -> _Mutator:
    def mutate(work: VNextUnitOfWork, cycle: CleanupCycle, cursor: bytes) -> _Mutation:
        if cycle.target_kind != kind:
            raise CleanupCorruptionError("cleanup static strategy kind drifted")
        return _run_static_phase(work, cycle, cursor, _STATIC_PLANS[kind], phase)

    return mutate


_SOURCE_BUILD_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM catalog_source_build_states terminal
    WHERE terminal.build_id = r.build_id
      AND terminal.state IN ('SEALED', 'ABANDONED'))
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_build_ids x WHERE x.build_id = r.build_id)
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
    JOIN catalog_analysis_run_build_ids retired_build
      ON retired_build.build_id = older.build_id
    JOIN catalog_analysis_run_states retired
      ON retired.analysis_id = retired_build.analysis_id
    WHERE older.build_id <> r.build_id
      AND retired.state = 'ABANDONED'
      AND NOT EXISTS (
          SELECT 1 FROM catalog_analysis_run_build_ids sibling
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
    JOIN operational_ingest_generation_leases l ON l.generation = m.generation
    WHERE m.build_id = r.build_id)
AND NOT EXISTS (
    SELECT 1 FROM operational_source_build_generations m
    JOIN operational_ingest_generation_handoffs h ON h.generation = m.generation
    WHERE m.build_id = r.build_id)
"""

_ANALYSIS_RUN_ELIGIBILITY = """
EXISTS (
    SELECT 1 FROM catalog_analysis_run_states terminal
    WHERE terminal.analysis_id = r.analysis_id
      AND terminal.state IN ('COMPLETE', 'ABANDONED'))
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_build_ids member
    JOIN catalog_analysis_run_build_ids retired_member
      ON retired_member.build_id = member.build_id
    JOIN catalog_analysis_run_states retired
      ON retired.analysis_id = retired_member.analysis_id
    WHERE member.analysis_id = r.analysis_id
      AND retired.state = 'ABANDONED'
      AND EXISTS (
          SELECT 1 FROM catalog_analysis_run_build_ids sibling
          WHERE sibling.build_id = member.build_id
            AND sibling.analysis_id <> retired.analysis_id))
AND NOT EXISTS (
    SELECT 1
    FROM catalog_analysis_run_states retired
    JOIN catalog_analysis_run_build_ids build
      ON build.analysis_id = retired.analysis_id
    JOIN operational_source_build_generations mapped
      ON mapped.build_id = build.build_id
    WHERE retired.analysis_id = r.analysis_id
      AND retired.state = 'ABANDONED'
      AND NOT EXISTS (
          SELECT 1 FROM operational_source_build_generations newer
          WHERE newer.generation > mapped.generation)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_analysis_run_build_ids sibling
          WHERE sibling.build_id = build.build_id
            AND sibling.analysis_id <> retired.analysis_id))
AND NOT EXISTS (
    SELECT 1 FROM catalog_analysis_run_build_ids build
    JOIN operational_source_working_builds working
      ON working.build_id = build.build_id
    WHERE build.analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidate_analysis_ids x
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
    JOIN catalog_publication_commit_source_revisions committed
      ON committed.receipt_id = h.receipt_id
    JOIN catalog_source_revision_provenance p
      ON p.source_revision = committed.source_revision
    WHERE p.analysis_id = r.analysis_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_revision_provenance p
    JOIN catalog_publication_commit_source_revisions committed
      ON committed.source_revision = p.source_revision
    JOIN catalog_source_build_base_publication_commits base
      ON base.base_receipt_id = committed.receipt_id
    WHERE p.analysis_id = r.analysis_id)
"""

_PUBLICATION_CANDIDATE_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM operational_catalog_working_candidates x
    WHERE x.candidate_id = r.candidate_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_prepared_artifact_states protected
    WHERE protected.candidate_id = r.candidate_id
      AND protected.state IN ('PENDING', 'PREPARED'))
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_candidates committed
    WHERE committed.candidate_id = r.candidate_id
      AND (
        NOT EXISTS (
            SELECT 1 FROM catalog_publication_commit_finalizations finalized
            WHERE finalized.receipt_id = committed.receipt_id)
        OR EXISTS (
            SELECT 1 FROM catalog_publication_commit_head_receipts head
            WHERE head.receipt_id = committed.receipt_id)))
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_candidates committed
    JOIN catalog_source_build_base_publication_commits base
      ON base.base_receipt_id = committed.receipt_id
    WHERE committed.candidate_id = r.candidate_id)
"""


def _analysis_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_analysis_run_anchors"
    key = ("analysis_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    return {
        "AR_BATCH_SEAL": (
            direct(
                "catalog_analysis_batch_receipt_seals",
                ("analysis_id", "stage", "start_generation"),
            ),
        ),
        "AR_BATCH_VALUES": (
            direct(
                "catalog_analysis_batch_receipt_coordinates",
                ("analysis_id", "stage", "batch_key"),
            ),
            direct(
                "catalog_analysis_batch_receipt_committed_ats",
                ("analysis_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_analysis_batch_receipt_row_counts",
                ("analysis_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_analysis_batch_receipt_next_cursors",
                ("analysis_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_analysis_batch_receipt_start_processed_counts",
                ("analysis_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_analysis_batch_receipt_page_limits",
                ("analysis_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_analysis_batch_receipt_start_cursors",
                ("analysis_id", "stage", "start_generation"),
            ),
        ),
        "AR_BATCH_ANCHOR": (
            direct(
                "catalog_analysis_batch_receipt_anchors",
                ("analysis_id", "stage", "start_generation"),
            ),
        ),
        "AR_COMPONENT_SEAL": (
            direct(
                "catalog_analysis_state_component_completion_seals",
                ("analysis_id", "state_component"),
            ),
        ),
        "AR_COMPONENT_VALUES": (
            direct(
                "catalog_analysis_state_component_row_counts",
                ("analysis_id", "state_component"),
            ),
            direct(
                "catalog_analysis_state_component_sealed_ats",
                ("analysis_id", "state_component"),
            ),
        ),
        "AR_COMPONENT_ANCHOR": (
            direct(
                "catalog_analysis_state_component_anchors",
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
                    "catalog_a_content_candidate_shadow_seals",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_owner_shadow_seals",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_content_seals",
                    ("analysis_id", "content_sha256"),
                ),
                ("catalog_a_impacted_gid_seals", ("analysis_id", "gid")),
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
                (
                    "catalog_a_file_decision_shadow_occurrences",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_a_file_decision_shadow_artists",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_a_file_decision_shadow_gallery_artist_max",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_a_content_candidate_shadow_contents",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_candidate_shadow_not_uploaded",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_candidate_shadow_title_counts",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_candidate_shadow_download_times",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_owner_shadow_galleries",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_content_witnesses",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_gid_witnesses",
                    ("analysis_id", "gid"),
                ),
                (
                    "catalog_a_impacted_content_provenance",
                    ("analysis_id", "gallery_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_gid_provenance",
                    ("analysis_id", "gallery_id", "gid"),
                ),
                (
                    "catalog_a_file_decision_shadow_anchors",
                    ("analysis_id", "file_sha256"),
                ),
                (
                    "catalog_a_content_candidate_shadow_anchors",
                    ("analysis_id", "gallery_id"),
                ),
                (
                    "catalog_a_content_owner_shadow_anchors",
                    ("analysis_id", "content_sha256"),
                ),
                (
                    "catalog_a_impacted_content_anchors",
                    ("analysis_id", "content_sha256"),
                ),
                ("catalog_a_impacted_gid_anchors", ("analysis_id", "gid")),
            )
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
        "AR_CHECKPOINT_SEAL": (
            direct("catalog_analysis_checkpoint_seals", ("analysis_id", "stage")),
        ),
        "AR_CHECKPOINT_VALUES": (
            direct("catalog_analysis_checkpoint_updated_ats", ("analysis_id", "stage")),
            direct("catalog_analysis_checkpoint_states", ("analysis_id", "stage")),
            direct(
                "catalog_analysis_checkpoint_processed_counts",
                ("analysis_id", "stage"),
            ),
            direct("catalog_analysis_checkpoint_cursors", ("analysis_id", "stage")),
            direct("catalog_analysis_checkpoint_generations", ("analysis_id", "stage")),
        ),
        "AR_CHECKPOINT_ANCHOR": (
            direct("catalog_analysis_checkpoint_anchors", ("analysis_id", "stage")),
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
        # The terminal state is part of cleanup eligibility, so the descriptor,
        # run values, and anchor must leave in one final compound mutation.  An
        # empty phase preserves the provider's closed 19-phase protocol without
        # making the terminal row unreachable between transactions.
        "AR_DESCRIPTOR": (),
        "AR_RUN_VALUES": (),
        "AR_ROOT": (
            _owned_spec(
                root,
                key,
                root,
                key,
                delete_sql=(
                    "DELETE FROM catalog_analysis_run_completed_ats "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_descriptor_seals "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_identities "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_started_ats "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_input_manifest_sha256s "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_policy_ids "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_build_ids "
                    "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_states " "WHERE analysis_id = %s",
                    "DELETE FROM catalog_analysis_run_anchors "
                    "WHERE analysis_id = %s",
                ),
                delete_allowed_affected=(
                    frozenset((0, 1)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                ),
            ),
        ),
    }


def _publication_candidate_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_candidate_anchors"
    key = ("candidate_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    prepared_source = (
        "catalog_prepared_artifact_anchors AS c "
        "JOIN catalog_publication_candidate_anchors AS r "
        "ON r.candidate_id = c.candidate_id"
    )
    prepared_key = ("candidate_id", "publication_key")
    prepared_seal = _indirect_spec(
        "catalog_prepared_artifact_seals",
        prepared_key,
        prepared_source,
    )
    prepared_values = _indirect_spec(
        "catalog_prepared_artifact_states",
        prepared_key,
        prepared_source,
        delete_sql=(
            "DELETE FROM catalog_prepared_artifact_states "
            "WHERE candidate_id = %s AND publication_key = %s",
            "DELETE FROM catalog_prepared_artifact_protection_tokens "
            "WHERE candidate_id = %s AND publication_key = %s",
            "DELETE FROM catalog_prepared_artifact_storage_generations "
            "WHERE candidate_id = %s AND publication_key = %s",
            "DELETE FROM catalog_prepared_artifact_storage_codec_versions "
            "WHERE candidate_id = %s AND publication_key = %s",
            "DELETE FROM catalog_prepared_artifact_sha256s "
            "WHERE candidate_id = %s AND publication_key = %s",
        ),
    )

    return {
        "PC_SEALS": (
            direct("operational_publication_candidate_preparations", ("candidate_id",)),
            direct(
                "catalog_publication_candidate_projection_seals",
                ("candidate_id",),
            ),
            direct(
                "catalog_publication_batch_receipt_seals",
                ("candidate_id", "stage", "start_generation"),
            ),
            prepared_seal,
            direct("catalog_artifact_operations", ("candidate_id", "publication_key")),
        ),
        "PC_PREPARED_VALUES": (prepared_values,),
        "PC_PREPARED_ANCHOR": (
            direct("catalog_prepared_artifact_anchors", prepared_key),
        ),
        "PC_INPUT": (
            direct(
                "catalog_candidate_artifact_inputs",
                ("candidate_id", "publication_key"),
            ),
        ),
        "PC_BATCH_VALUES": (
            direct(
                "catalog_publication_batch_receipt_coordinates",
                ("candidate_id", "stage", "batch_key"),
            ),
            direct(
                "catalog_publication_batch_receipt_committed_ats",
                ("candidate_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_publication_batch_receipt_row_counts",
                ("candidate_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_publication_batch_receipt_next_cursors",
                ("candidate_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_publication_batch_receipt_start_processed_counts",
                ("candidate_id", "stage", "start_generation"),
            ),
            direct(
                "catalog_publication_batch_receipt_start_cursors",
                ("candidate_id", "stage", "start_generation"),
            ),
        ),
        "PC_BATCH_ANCHOR": (
            direct(
                "catalog_publication_batch_receipt_anchors",
                ("candidate_id", "stage", "start_generation"),
            ),
        ),
        "PC_CHECKPOINT_SEAL": (
            direct("catalog_publication_checkpoint_seals", ("candidate_id", "stage")),
            direct("catalog_publication_selections", ("candidate_id", "gallery_id")),
        ),
        "PC_CHECKPOINT_VALUES": (
            direct(
                "catalog_publication_checkpoint_updated_ats",
                ("candidate_id", "stage"),
            ),
            direct("catalog_publication_checkpoint_states", ("candidate_id", "stage")),
            direct(
                "catalog_publication_checkpoint_processed_counts",
                ("candidate_id", "stage"),
            ),
            direct("catalog_publication_checkpoint_cursors", ("candidate_id", "stage")),
            direct(
                "catalog_publication_checkpoint_generations",
                ("candidate_id", "stage"),
            ),
        ),
        "PC_CHECKPOINT_ANCHOR": (
            direct("catalog_publication_checkpoint_anchors", ("candidate_id", "stage")),
        ),
        "PC_BASES": (
            direct(
                "catalog_publication_candidate_base_publication_commits",
                ("candidate_id",),
            ),
        ),
        "PC_ROOT": (
            direct(
                "catalog_publication_candidate_definition_seals",
                key,
            ),
            direct("catalog_publication_candidate_created_ats", key),
            direct("catalog_publication_candidate_artifacts_required", key),
            direct(
                "catalog_publication_candidate_display_title_policy_ids",
                key,
            ),
            direct("catalog_publication_candidate_artifact_policy_ids", key),
            direct("catalog_publication_candidate_reserved_revisions", key),
            direct("catalog_publication_candidate_analysis_ids", key),
            direct(root, key),
        ),
    }


def _source_build_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_source_build_anchors"
    key = ("build_id",)

    def direct(table: str, pk: tuple[str, ...]) -> _StaticDeleteSpec:
        return _owned_spec(table, pk, root, key)

    upload = _indirect_spec(
        "operational_canonical_value_uploads",
        ("generation", "value_sha256"),
        "operational_canonical_value_uploads AS c "
        "JOIN operational_source_build_generations AS m "
        "ON m.generation = c.generation "
        "JOIN catalog_source_build_anchors AS r ON r.build_id = m.build_id",
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
            direct("operational_source_build_discovery_checkpoints", ("build_id",)),
            direct("operational_source_build_assembly_checkpoints", ("build_id",)),
            direct("catalog_build_manifest_seals", ("build_id",)),
            direct("catalog_build_manifest_manifest_sha256s", ("build_id",)),
            direct("catalog_build_manifest_file_counts", ("build_id",)),
            direct("catalog_build_manifest_byte_counts", ("build_id",)),
            direct("catalog_build_manifest_anchors", ("build_id",)),
            direct("catalog_source_build_galleries", ("build_id", "gallery_id")),
        ),
        "SB_DISCOVERY_SEAL": (
            direct("catalog_source_build_discovery_seals", ("build_id",)),
        ),
        "SB_DISCOVERY_VALUES": (
            direct("catalog_source_build_discovery_scan_attempts", ("build_id",)),
            direct("catalog_source_build_discovery_gallery_counts", ("build_id",)),
            direct(
                "catalog_source_build_discovery_tree_observation_sha256s",
                ("build_id",),
            ),
            direct("catalog_source_build_discovery_completed_ats", ("build_id",)),
        ),
        "SB_DISCOVERY_ANCHOR": (
            direct("catalog_source_build_discovery_anchors", ("build_id",)),
        ),
        "SB_SATELLITES": (
            direct("catalog_source_build_expected_gallery", ("build_id", "position")),
            direct("catalog_source_build_base_publication_commits", ("build_id",)),
            direct("catalog_source_build_channel", ("build_id",)),
        ),
        "SB_GENERATION": (
            direct("operational_source_build_generations", ("generation",)),
        ),
        "SB_ROOT": (
            _owned_spec(
                root,
                key,
                root,
                key,
                delete_sql=(
                    "DELETE FROM catalog_source_build_sealed_ats WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_descriptor_seals WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_states WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_created_ats WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_manifest_policy_ids WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_scope_keys WHERE build_id = %s",
                    "DELETE FROM catalog_source_build_anchors WHERE build_id = %s",
                ),
                delete_allowed_affected=(
                    frozenset((0, 1)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                ),
            ),
        ),
    }


_GALLERY_OBSERVATION_STAGING_ELIGIBILITY = """
(
    (r.state = 'SEALED' AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        WHERE m.build_id = r.build_id AND m.gallery_id = r.gallery_id
          AND m.observation_id = r.observation_id))
    OR
    (r.state = 'REUSED' AND EXISTS (
        SELECT 1 FROM catalog_source_build_galleries m
        WHERE m.build_id = r.build_id AND m.gallery_id = r.gallery_id
          AND m.observation_id <> r.observation_id))
)
AND NOT EXISTS (
    SELECT 1
    FROM operational_gallery_observation_staging_request_predecessors p
    JOIN operational_gallery_observation_staging_request_owners prior_owner
      ON prior_owner.request_sha256 = p.prior_request_sha256
    JOIN operational_gallery_observation_staging_request_owners next_owner
      ON next_owner.request_sha256 = p.request_sha256
    WHERE prior_owner.staging_id = r.staging_id
      AND next_owner.staging_id <> r.staging_id)
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
            JOIN operational_ingest_generation_leases lease
              ON lease.generation = claim.ingest_generation
            WHERE claim.staging_id = s.staging_id
              AND lease.lease_expires_at > %s))
        OR
        (s.state = 'REUSED' AND EXISTS (
            SELECT 1 FROM catalog_source_build_galleries linked
            WHERE linked.build_id = s.build_id
              AND linked.gallery_id = s.gallery_id
              AND linked.observation_id <> s.observation_id))))
AND NOT EXISTS (
    SELECT 1
    FROM operational_gallery_observation_stagings s
    JOIN operational_gallery_observation_staging_request_owners prior_owner
      ON prior_owner.staging_id = s.staging_id
    JOIN operational_gallery_observation_staging_request_predecessors p
      ON p.prior_request_sha256 = prior_owner.request_sha256
    JOIN operational_gallery_observation_staging_request_owners next_owner
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
        "JOIN operational_gallery_observation_staging_request_owners AS owned "
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
    owner_pair = _indirect_spec(
        "operational_gallery_observation_staging_request_owners",
        ("request_sha256",),
        "operational_gallery_observation_staging_request_owners AS c "
        "JOIN operational_gallery_observation_stagings AS r "
        "ON r.staging_id = c.staging_id",
        delete_sql=(
            "DELETE FROM operational_gallery_observation_staging_request_owners "
            "WHERE request_sha256 = %s",
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
        "GOS_REQUEST_IDENTITY": (owner_pair,),
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
        "JOIN operational_gallery_observation_staging_request_owners AS owned "
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

    owner_pair = _indirect_spec(
        "operational_gallery_observation_staging_request_owners",
        ("request_sha256",),
        "operational_gallery_observation_staging_request_owners AS c "
        "JOIN operational_gallery_observation_stagings AS staged "
        "ON staged.staging_id = c.staging_id "
        "JOIN catalog_gallery_observation_allocations AS r "
        "ON r.gallery_id = staged.gallery_id "
        "AND r.observation_id = staged.observation_id",
        delete_sql=(
            "DELETE FROM operational_gallery_observation_staging_request_owners "
            "WHERE request_sha256 = %s",
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
        "GO_STAGING_REQUEST_IDENTITY": (owner_pair,),
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
                    "catalog_gallery_manifest_seals",
                    ("gallery_id", "observation_id", "manifest_policy_id"),
                ),
                (
                    "catalog_gallery_manifest_manifest_sha256s",
                    ("gallery_id", "observation_id", "manifest_policy_id"),
                ),
                (
                    "catalog_gallery_manifest_computed_ats",
                    ("gallery_id", "observation_id", "manifest_policy_id"),
                ),
                (
                    "catalog_gallery_manifest_anchors",
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
        "GO_METADATA_SEAL": (
            direct(
                "catalog_gallery_observation_metadata_seals",
                ("gallery_id", "observation_id"),
            ),
        ),
        "GO_METADATA_VALUES": (
            direct(
                "catalog_gallery_observation_download_times",
                ("gallery_id", "observation_id"),
            ),
            direct(
                "catalog_gallery_observation_modified_times",
                ("gallery_id", "observation_id"),
            ),
        ),
        "GO_OBSERVATION_FACT_SEALS": tuple(
            direct(table, ("gallery_id", "observation_id"))
            for table in (
                "catalog_gallery_observation_directory_seals",
                "catalog_gallery_observation_stat_seals",
                "catalog_gallery_observation_scan_seals",
            )
        ),
        "GO_OBSERVATION_FACT_VALUES": tuple(
            direct(table, ("gallery_id", "observation_id"))
            for table in (
                "catalog_gallery_observation_directory_entry_counts",
                "catalog_gallery_observation_directory_observation_sha256s",
                "catalog_gallery_observation_stat_file_counts",
                "catalog_gallery_observation_stat_byte_counts",
                "catalog_gallery_observation_scan_observation_sha256s",
                "catalog_gallery_observation_scan_observation_versions",
                "catalog_gallery_observation_scan_source_file_counts",
            )
        ),
        "GO_OBSERVATION_FACT_ANCHORS": tuple(
            direct(table, ("gallery_id", "observation_id"))
            for table in (
                "catalog_gallery_observation_metadata_anchors",
                "catalog_gallery_observation_directory_anchors",
                "catalog_gallery_observation_stat_anchors",
                "catalog_gallery_observation_scan_anchors",
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
AND (
    (r.state = 'COMPLETE' AND EXISTS (
        SELECT 1 FROM catalog_publication_commit_operational_preparations committed
        WHERE committed.preparation_id = r.preparation_id))
    OR
    (r.state = 'ABANDONED'
     AND NOT EXISTS (
        SELECT 1 FROM catalog_publication_commit_operational_preparations committed
        WHERE committed.preparation_id = r.preparation_id)
     AND NOT EXISTS (
        SELECT 1 FROM operational_operational_event_ack_heads head
        WHERE head.preparation_id = r.preparation_id)
     AND NOT EXISTS (
        SELECT 1 FROM operational_operational_event_acks ack
        JOIN operational_operational_events event ON event.event_id = ack.event_id
        WHERE event.preparation_id = r.preparation_id))
)
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
    complete_root = direct(root, key, "r.state = 'COMPLETE'")
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
        "OP_ROOT": (complete_root, abandoned_root),
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
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_anchors x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_contents x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_not_uploaded x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_title_counts x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_download_times x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_seals x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_candidate_tombstones x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_owner_shadow_galleries x
                WHERE x.owner_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_provenance x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_witnesses x
                WHERE x.witness_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_gid_provenance x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_gid_witnesses x
                WHERE x.witness_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_candidate_shadows x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_candidate_tombstones x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_gid_winner_selections x
                WHERE x.winner_gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_selections x
                WHERE x.gallery_id = r.gallery_id)
AND NOT EXISTS (SELECT 1 FROM catalog_gallery_observation_metadata_seals x
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
    JOIN operational_ingest_generation_leases lease
      ON lease.generation = owner.generation
    WHERE owner.generation = r.generation AND lease.lease_expires_at > %s)
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


_CANONICAL_VALUE_ELIGIBILITY = """
NOT EXISTS (SELECT 1 FROM operational_canonical_value_uploads x
            WHERE x.value_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM operational_hash_cache_observations x
                WHERE x.source_identity_sha256 = r.value_sha256
                   OR x.fingerprint_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scope_source_root_sha256s scope_root
    JOIN catalog_source_build_scope_keys build
      ON build.scope_key = scope_root.scope_key
    WHERE scope_root.source_root_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_scope_source_root_sha256s scope_root
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
    SELECT 1 FROM catalog_source_revision_descriptor_seals sealed
    JOIN catalog_source_revision_snapshot_manifests manifest
      ON manifest.source_revision = sealed.source_revision
    WHERE manifest.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_snapshot_manifest x
                WHERE x.snapshot_manifest_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_anchors x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_provenance x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_witnesses x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_impacted_content_seals x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_candidate_shadow_contents x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_owner_shadow_anchors x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_owner_shadow_galleries x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_a_content_owner_shadow_seals x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_analysis_content_owner_tombstones x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_candidate_artifact_policy_ids x
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = x.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_artifact_policies committed
    JOIN catalog_artifact_policies policy
      ON policy.artifact_policy_id = committed.artifact_policy_id
    WHERE policy.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_source_manifest_sha256s x
                WHERE x.source_manifest_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_member_plan_sha256s x
                WHERE x.member_plan_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_effective_content_sha256s x
                WHERE x.effective_content_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_selected_sha256s x
                WHERE x.selected_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_owner_sha256s x
                WHERE x.owner_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantic_policy_sha256s x
                WHERE x.policy_component_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_candidate_artifact_inputs x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_semantics_sha256s x
                WHERE x.artifact_semantics_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_contents x
                WHERE x.content_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_contributor_name_sha256s x
                WHERE x.contributor_name_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_summary_sha256s x
                WHERE x.summary_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_language_sha256s x
                WHERE x.language_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_publication_title_source_title_sha256s x
                WHERE x.source_title_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_artifact_location x
                WHERE x.artifact_locator_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_display_title_choices x
                WHERE x.source_title_sha256 = r.value_sha256
                   OR x.title_sha256 = r.value_sha256)
AND NOT EXISTS (SELECT 1 FROM catalog_title_sorts x
                WHERE x.title_sha256 = r.value_sha256
                   OR x.sort_title_sha256 = r.value_sha256)
"""


def _canonical_value_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_canonical_value_allocation_anchors"
    key = ("value_sha256",)
    source_scope = (
        _indirect_spec(
            "catalog_source_scope_anchors",
            ("scope_key",),
            "catalog_source_scope_anchors AS c "
            "JOIN catalog_source_scope_source_root_sha256s AS scope_root "
            "ON scope_root.scope_key = c.scope_key "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = scope_root.source_root_sha256",
            delete_sql=(
                "DELETE FROM catalog_source_scope_seals WHERE scope_key = %s",
                "DELETE FROM catalog_source_scope_identities WHERE scope_key = %s",
                "DELETE FROM catalog_source_scope_identity_policy_versions "
                "WHERE scope_key = %s",
                "DELETE FROM catalog_source_scope_source_providers "
                "WHERE scope_key = %s",
                "DELETE FROM catalog_source_scope_source_root_sha256s "
                "WHERE scope_key = %s",
                "DELETE FROM catalog_source_scope_anchors WHERE scope_key = %s",
            ),
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
        "catalog_source_snapshot_manifest_identity_anchors",
        ("snapshot_manifest_sha256",),
        "catalog_source_snapshot_manifest_identity_anchors AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.snapshot_manifest_sha256",
        delete_sql=(
            "DELETE FROM catalog_source_snapshot_manifest_identity_seals "
            "WHERE snapshot_manifest_sha256 = %s",
            "DELETE FROM catalog_source_snapshot_manifest_identity_gallery_counts "
            "WHERE snapshot_manifest_sha256 = %s",
            "DELETE FROM catalog_source_snapshot_manifest_identity_file_counts "
            "WHERE snapshot_manifest_sha256 = %s",
            "DELETE FROM catalog_source_snapshot_manifest_identity_byte_counts "
            "WHERE snapshot_manifest_sha256 = %s",
            "DELETE FROM catalog_source_snapshot_manifest_identity_anchors "
            "WHERE snapshot_manifest_sha256 = %s",
        ),
    )
    policy = _indirect_spec(
        "catalog_artifact_policies",
        ("artifact_policy_id",),
        "catalog_artifact_policies AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.policy_component_sha256",
    )
    semantic = _indirect_spec(
        "catalog_artifact_semantic_input_anchors",
        ("artifact_semantics_sha256",),
        "catalog_artifact_semantic_input_anchors AS c "
        "JOIN catalog_canonical_value_allocation_anchors AS r "
        "ON r.value_sha256 = c.artifact_semantics_sha256",
        delete_sql=(
            "DELETE FROM catalog_artifact_semantic_input_seals "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_input_identities "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_source_manifest_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_member_plan_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_effective_content_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_selected_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_owner_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_policy_sha256s "
            "WHERE artifact_semantics_sha256 = %s",
            "DELETE FROM catalog_artifact_semantic_input_anchors "
            "WHERE artifact_semantics_sha256 = %s",
        ),
    )
    policy_semantics = tuple(
        _indirect_spec(
            table,
            ("policy_component_sha256",),
            f"{table} AS c "
            "JOIN catalog_canonical_value_allocation_anchors AS r "
            "ON r.value_sha256 = c.policy_component_sha256",
        )
        for table in (
            "catalog_artifact_policy_semantics_seals",
            "catalog_artifact_policy_semantics_identities",
            "catalog_artifact_policy_semantics_producer_fingerprint_sha256s",
            "catalog_artifact_policy_semantics_max_image_short_sides",
            "catalog_artifact_policy_semantics_artifact_algorithm_versions",
            "catalog_artifact_policy_semantics_anchors",
        )
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
        "CV_DICTIONARY": (*source_scope, locator, tag, snapshot, policy, semantic),
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
        ("build_id",),
        "build_id",
        16,
        _SOURCE_BUILD_ELIGIBILITY,
        _source_build_phases(),
    ),
    CleanupTargetKind.ANALYSIS_RUN: _StaticTargetPlan(
        CleanupTargetKind.ANALYSIS_RUN,
        ("analysis_id",),
        "analysis_id",
        16,
        _ANALYSIS_RUN_ELIGIBILITY,
        _analysis_phases(),
    ),
    CleanupTargetKind.PUBLICATION_CANDIDATE: _StaticTargetPlan(
        CleanupTargetKind.PUBLICATION_CANDIDATE,
        ("candidate_id",),
        "candidate_id",
        16,
        _PUBLICATION_CANDIDATE_ELIGIBILITY,
        _publication_candidate_phases(),
    ),
    CleanupTargetKind.OPERATIONAL_PREPARATION: _StaticTargetPlan(
        CleanupTargetKind.OPERATIONAL_PREPARATION,
        ("preparation_id",),
        "preparation_id",
        16,
        _OPERATIONAL_PREPARATION_ELIGIBILITY,
        _operational_preparation_phases(),
    ),
    CleanupTargetKind.GALLERY_OBSERVATION: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION,
        ("gallery_id", "observation_id"),
        "gallery_id",
        None,
        _GALLERY_OBSERVATION_ELIGIBILITY,
        _gallery_observation_phases(),
        uses_cutoff=True,
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_STAGING: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
        ("staging_id",),
        "staging_id",
        16,
        _GALLERY_OBSERVATION_STAGING_ELIGIBILITY,
        _gallery_observation_staging_phases(),
    ),
    CleanupTargetKind.CANONICAL_VALUE: _StaticTargetPlan(
        CleanupTargetKind.CANONICAL_VALUE,
        ("value_sha256",),
        "value_sha256",
        32,
        _CANONICAL_VALUE_ELIGIBILITY,
        _canonical_value_phases(),
    ),
    CleanupTargetKind.GALLERY_OBSERVATION_PAGE: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
        ("page_sha256",),
        "page_sha256",
        32,
        _GALLERY_PAGE_ELIGIBILITY,
        _gallery_page_phases(),
    ),
    CleanupTargetKind.GALLERY_IDENTITY: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_IDENTITY,
        ("gallery_id",),
        "gallery_id",
        None,
        _GALLERY_IDENTITY_ELIGIBILITY,
        _gallery_identity_phases(),
    ),
    CleanupTargetKind.SOURCE_GALLERY_NAME_GID: _StaticTargetPlan(
        CleanupTargetKind.SOURCE_GALLERY_NAME_GID,
        ("source_gallery_name",),
        "source_gallery_name",
        255,
        _SOURCE_GALLERY_NAME_GID_ELIGIBILITY,
        _source_gallery_name_gid_phases(),
        variable_width_shard=True,
    ),
    CleanupTargetKind.GALLERY_UPLOAD_TIME: _StaticTargetPlan(
        CleanupTargetKind.GALLERY_UPLOAD_TIME,
        ("gid",),
        "gid",
        None,
        _GALLERY_UPLOAD_TIME_ELIGIBILITY,
        _gallery_upload_time_phases(),
    ),
    CleanupTargetKind.CANONICAL_VALUE_UPLOAD: _StaticTargetPlan(
        CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
        ("generation", "value_sha256"),
        "value_sha256",
        32,
        _CANONICAL_UPLOAD_ELIGIBILITY,
        _canonical_upload_phases(),
        uses_cutoff=True,
    ),
}


def _static_strategy(kind: CleanupTargetKind) -> _Strategy:
    phases = tuple(_STATIC_PLANS[kind].phases)
    return _Strategy(
        phases,
        tuple(_static_mutator(kind, phase) for phase in phases),
    )


_STRATEGIES: dict[CleanupTargetKind, _Strategy] = {
    CleanupTargetKind.SOURCE_BUILD: _static_strategy(CleanupTargetKind.SOURCE_BUILD),
    CleanupTargetKind.ANALYSIS_RUN: _static_strategy(CleanupTargetKind.ANALYSIS_RUN),
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
        ("AB_LOCATIONS", "AB_ROOT"),
        (
            _select_artifact_locations,
            _select_artifact_blobs,
        ),
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
    CleanupTargetKind.HASH_CACHE_OBSERVATION: _Strategy(
        ("HC_FILE", "HC_ROOT"),
        (_select_hash_cache_files, _select_hash_cache_observations),
    ),
}

_ALL_PHASES = frozenset(
    phase for strategy in _STRATEGIES.values() for phase in strategy.phases
)
