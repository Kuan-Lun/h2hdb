"""Durable cleanup cycle control under the exact EXCLUSIVE maintenance gate."""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Callable
from dataclasses import dataclass

from h2hdb._cleanup import model as _model
from h2hdb._cleanup import roots as _roots
from h2hdb._cleanup.model import (
    _CHECKPOINT_TABLE,
    _CLEANUP_ALGORITHM_VERSION,
    _EMPTY_CURSOR,
    _FROZEN_ROOT_TABLE,
    _JOB_TABLE,
    _MAX_BATCH_ROWS,
    _PHASE_TABLE,
    _SWEEP_TABLE,
    CleanupBatchCommand,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupCycleExhaustedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    _Checkpoint,
    _CleanupOperation,
)
from h2hdb._cleanup.registry import _ALL_PHASES, _STATIC_PLANS, _STRATEGIES
from h2hdb._cleanup.targets import publication_commit as _publication_commit_targets
from h2hdb.database_performance import database_phase
from h2hdb.vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_CHAIN_DOMAIN = b"h2hdb-cleanup-chain-v1\0"

_INPUT_DOMAIN = b"h2hdb-cleanup-input-v1\0"


def _advance_checkpoint(
    operation: _CleanupOperation,
    checkpoint: _Checkpoint,
    attempt: CleanupBatchCommand,
    *,
    timestamp: int,
) -> tuple[CleanupBatchResult, _Checkpoint | None]:
    """Mutate one phase under this transaction's exact locked cycle authority."""

    work, requested = operation.work, operation.cycle
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
    mutation = strategy.mutators[phase_index](operation, checkpoint.cursor)
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

    work.compare_and_swap(
        f"""
        UPDATE {_CHECKPOINT_TABLE}
        SET generation = %s, cursor_bytes = %s, deleted_count = %s,
            chain_sha256 = %s, state = %s, updated_at = %s,
            receipt_batch_key = %s, receipt_start_cursor = %s,
            receipt_prior_chain_sha256 = %s,
            receipt_prior_deleted_count = %s,
            receipt_input_sha256 = %s, receipt_row_count = %s
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
            attempt.batch_key,
            checkpoint.cursor,
            checkpoint.chain_sha256,
            checkpoint.deleted_count,
            input_sha256,
            row_count,
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
        ), None

    if phase_index + 1 < len(strategy.phases):
        next_phase = strategy.phases[phase_index + 1]
        _insert_checkpoint(
            work,
            cycle=requested,
            phase=next_phase,
            chain_sha256=_phase_chain(next_chain, next_phase),
            now=timestamp,
        )
        next_checkpoint = _Checkpoint(
            phase=next_phase,
            generation=1,
            cursor=_EMPTY_CURSOR,
            deleted_count=0,
            chain_sha256=_phase_chain(next_chain, next_phase),
            state="OPEN",
            receipt_batch_key=None,
            receipt_row_count=None,
            receipt_start_cursor=None,
            receipt_prior_chain_sha256=None,
            receipt_prior_deleted_count=None,
            receipt_input_sha256=None,
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
        ), next_checkpoint

    total_deleted = _fixed_checkpoint_total(work, requested, strategy.phases)
    _complete_cycle(
        operation,
        final_chain_sha256=next_chain,
        deleted_count=total_deleted,
        now=timestamp,
    )
    return _complete_result(
        requested,
        row_count=0,
        deleted_count=total_deleted,
        replayed=False,
    ), None


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
    expected_target_key = _model._target_key(kind, shard)
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
               j.final_chain_sha256, j.final_deleted_count
        FROM {_SWEEP_TABLE} AS s
        LEFT JOIN {_JOB_TABLE} AS j ON j.target_key = s.target_key
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
        state = _model._as_text(row[9], field="cleanup job state")
        if state == "OPEN":
            _require_cycle_policy(
                existing,
                cutoff=cutoff,
                max_rows=max_rows,
                max_age=max_age,
            )
            _roots._load_frozen_roots(work, existing, _STATIC_PLANS.get(kind))
            return existing
        if state != "COMPLETE":
            raise CleanupCorruptionError("cleanup job has an invalid state")
        _require_complete_job(row)
        _roots._require_no_frozen_roots(work, existing.cleanup_id)
        if existing.cycle_generation == INT63_MAX:
            raise CleanupCycleExhaustedError(
                "cleanup cycle generation reached portable int63 maximum"
            )
        generation = existing.cycle_generation + 1
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

    cleanup_id = _model._cleanup_id(kind, shard, generation)
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
             state, created_at, completed_at,
             final_chain_sha256, final_deleted_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, 'OPEN', %s, NULL,
                NULL, NULL)
        """,
        (
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
            cycle.cycle_cutoff_at,
            _CLEANUP_ALGORITHM_VERSION,
            cycle.max_rows_per_transaction,
            cycle.hash_cache_max_age_microseconds,
            _roots._frozen_root_set_sha256(cycle.cleanup_id, ()),
            now,
        ),
    )
    _roots._freeze_static_cycle_roots(work, cycle, _STATIC_PLANS.get(kind))
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


def _require_exclusive_gate(
    work: VNextUnitOfWork, lease: GateLease, *, now: int | Callable[[], int]
) -> int:
    # Current-only orchestration supplies its clock, so database/lock waits
    # cannot leave the next operation authorized by a stale sampled time.
    # Explicit repository commands retain their deterministic event timestamp.
    with database_phase("gate_validate") as step:
        with database_phase("gate_lock", action="validate"):
            locked = MaintenanceGateRepository.lock_for_renewal(work, lease)
        timestamp = require_int63(
            now() if callable(now) else now, field="cleanup gate now"
        )
        step.describe(lease_remaining_us=lease.lease_expires_at - timestamp)
        current = locked.require_live(now=timestamp)
        if current.mode != GateMode.EXCLUSIVE or current.slots != tuple(range(64)):
            raise CleanupUnavailableError("cleanup requires the exact EXCLUSIVE gate")
        return timestamp


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
        max_rows_per_transaction=_model._require_batch_bound(row[5]),
        hash_cache_max_age_microseconds=require_int63(
            row[6], field="stored hash_cache_max_age_microseconds"
        ),
    )


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


def _require_complete_job(row: tuple[object, ...]) -> None:
    if row[11] is None or row[12] is None or row[13] is None:
        raise CleanupCorruptionError("COMPLETE cleanup job lacks exact completion")
    _roots._require_frozen_root_count(row[7])
    require_digest32(row[8], field="cleanup frozen_root_set_sha256")
    require_int63(row[11], field="cleanup completed_at")
    require_digest32(row[12], field="cleanup final_chain_sha256")
    require_int63(row[13], field="cleanup final_deleted_count")


def _insert_checkpoint(
    work: VNextUnitOfWork,
    *,
    cycle: CleanupCycle,
    phase: str,
    chain_sha256: bytes,
    now: int,
) -> None:
    # The operation has already exact-compared the complete strategy registry.
    work.connector.execute(
        f"""
        INSERT INTO {_CHECKPOINT_TABLE}
            (cleanup_id, phase, generation, cursor_bytes, deleted_count,
             chain_sha256, state, updated_at)
        VALUES (%s, %s, 1, %s, 0, %s, 'OPEN', %s)
        """,
        (cycle.cleanup_id, phase, _EMPTY_CURSOR, chain_sha256, now),
    )


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


def _lock_cycle(work: VNextUnitOfWork, cycle: CleanupCycle) -> _CleanupOperation:
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
               p.receipt_batch_key, p.receipt_start_cursor,
               p.receipt_prior_chain_sha256,
               p.receipt_prior_deleted_count,
               p.receipt_input_sha256, p.receipt_row_count
        FROM {_JOB_TABLE} AS j
        LEFT JOIN {_CHECKPOINT_TABLE} AS p ON p.cleanup_id = j.cleanup_id
        LEFT JOIN {_PHASE_TABLE} AS cp ON cp.phase = p.phase
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
    frozen_count = _roots._require_frozen_root_count(row[6])
    frozen_digest = require_digest32(row[7], field="cleanup frozen_root_set_sha256")
    state = _model._as_text(row[8], field="cleanup job state")
    if state == "COMPLETE":
        if row[9] is not None:
            raise CleanupCorruptionError("COMPLETE cleanup retains a checkpoint")
        _roots._require_no_frozen_roots(work, cycle.cleanup_id)
        return _CleanupOperation(work, cycle, None, True, ())
    if state != "OPEN" or row[9] is None:
        raise CleanupCorruptionError("OPEN cleanup lacks one current checkpoint")
    _validate_strategy_seeds(work, cycle.target_kind)
    frozen_roots = _roots._load_frozen_roots(
        work,
        cycle,
        _STATIC_PLANS.get(cycle.target_kind),
        expected_count=frozen_count,
        expected_digest=frozen_digest,
    )
    checkpoint = _Checkpoint(
        phase=_model._as_text(row[9], field="cleanup phase"),
        generation=require_positive_int63(
            row[10], field="cleanup checkpoint generation"
        ),
        cursor=require_bounded_bytes(row[11], field="cleanup cursor", maximum=2048),
        deleted_count=require_int63(row[12], field="cleanup deleted_count"),
        chain_sha256=require_digest32(row[13], field="cleanup chain_sha256"),
        state=_model._as_text(row[14], field="cleanup checkpoint state"),
        receipt_batch_key=(
            None
            if row[15] is None
            else require_digest32(row[15], field="cleanup receipt batch_key")
        ),
        receipt_row_count=(
            None
            if row[20] is None
            else require_int63(row[20], field="cleanup receipt row_count")
        ),
        receipt_start_cursor=(
            None
            if row[16] is None
            else require_bounded_bytes(
                row[16], field="cleanup receipt start_cursor", maximum=2048
            )
        ),
        receipt_input_sha256=(
            None
            if row[19] is None
            else require_digest32(row[19], field="cleanup receipt input_sha256")
        ),
        receipt_prior_chain_sha256=(
            None
            if row[17] is None
            else require_digest32(row[17], field="cleanup receipt prior_chain_sha256")
        ),
        receipt_prior_deleted_count=(
            None
            if row[18] is None
            else require_int63(row[18], field="cleanup receipt prior_deleted_count")
        ),
    )
    _validate_checkpoint_receipt(cycle, checkpoint)
    return _CleanupOperation(work, cycle, checkpoint, False, frozen_roots)


def _validate_checkpoint_receipt(cycle: CleanupCycle, checkpoint: _Checkpoint) -> None:
    fields = (
        checkpoint.receipt_batch_key,
        checkpoint.receipt_row_count,
        checkpoint.receipt_start_cursor,
        checkpoint.receipt_prior_chain_sha256,
        checkpoint.receipt_prior_deleted_count,
        checkpoint.receipt_input_sha256,
    )
    if all(value is None for value in fields):
        if checkpoint.generation != 1:
            raise CleanupCorruptionError(
                "advanced cleanup checkpoint lacks its latest receipt"
            )
        return
    if any(value is None for value in fields):
        raise CleanupCorruptionError("cleanup latest receipt is incomplete")
    assert checkpoint.receipt_row_count is not None
    assert checkpoint.receipt_start_cursor is not None
    assert checkpoint.receipt_prior_chain_sha256 is not None
    assert checkpoint.receipt_prior_deleted_count is not None
    assert checkpoint.receipt_input_sha256 is not None
    expected_deleted_count = require_int63(
        checkpoint.receipt_prior_deleted_count + checkpoint.receipt_row_count,
        field="cleanup receipt next deleted_count",
    )
    expected_output_sha256 = _next_chain(
        checkpoint.receipt_prior_chain_sha256,
        checkpoint.phase,
        checkpoint.generation,
        checkpoint.receipt_start_cursor,
        checkpoint.cursor,
        checkpoint.receipt_input_sha256,
        checkpoint.receipt_row_count,
    )
    if (
        expected_output_sha256 != checkpoint.chain_sha256
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
    if terminal != (checkpoint.receipt_start_cursor == checkpoint.cursor):
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
        SELECT prior.phase, prior.receipt_batch_key,
               prior.receipt_start_cursor, prior.cursor_bytes,
               prior.receipt_prior_chain_sha256,
               prior.receipt_prior_deleted_count,
               prior.receipt_input_sha256, prior.chain_sha256,
               prior.receipt_row_count, prior.generation, prior.updated_at,
               prior.deleted_count, prior.state,
               prior_seed.phase_order, current_seed.phase_order
        FROM {_CHECKPOINT_TABLE} AS prior
        JOIN {_PHASE_TABLE} AS prior_seed ON prior_seed.phase = prior.phase
        JOIN {_PHASE_TABLE} AS current_seed ON current_seed.phase = %s
        WHERE prior.cleanup_id = %s AND prior.receipt_batch_key = %s
          AND prior.generation = %s
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
    prior_phase = _model._as_text(row[0], field="cleanup terminal replay phase")
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
    checkpoint_deleted = require_int63(
        row[11], field="cleanup terminal checkpoint deleted_count"
    )
    prior_state = _model._as_text(row[12], field="cleanup terminal checkpoint state")
    prior_order = require_positive_int63(
        row[13], field="cleanup terminal prior phase_order"
    )
    current_order = require_positive_int63(
        row[14], field="cleanup terminal current phase_order"
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
        or checkpoint_deleted != expected_deleted
        or expected_output != output_sha256
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
        SELECT prior.phase, prior.receipt_batch_key, prior.generation
        FROM {_CHECKPOINT_TABLE} AS prior
        WHERE prior.cleanup_id = %s AND prior.phase = %s
          AND prior.receipt_batch_key = %s
          AND prior.receipt_row_count = 0 AND prior.state = 'COMPLETE'
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
        f"SELECT state, cycle_generation, final_chain_sha256, final_deleted_count "
        f"FROM {_JOB_TABLE} WHERE target_key = %s AND cleanup_id = %s",
        (cycle.target_key, cycle.cleanup_id),
    )
    if not row or row[0] != "COMPLETE" or row[1] != cycle.cycle_generation:
        raise CleanupCorruptionError("COMPLETE cleanup lacks replay authority")
    require_digest32(row[2], field="cleanup completion chain")
    return require_int63(row[3], field="cleanup completion deleted_count")


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
    operation: _CleanupOperation,
    *,
    final_chain_sha256: bytes,
    deleted_count: int,
    now: int,
) -> None:
    work, cycle = operation.work, operation.cycle
    removed_roots = work.connector.execute_affected(
        f"DELETE FROM {_FROZEN_ROOT_TABLE} WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    )
    if removed_roots != len(operation.frozen_roots):
        raise CleanupUnavailableError("cleanup frozen root set changed")
    work.connector.execute(
        f"DELETE FROM {_CHECKPOINT_TABLE} WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    )
    work.compare_and_swap(
        f"UPDATE {_JOB_TABLE} SET state = 'COMPLETE', completed_at = %s, "
        "final_chain_sha256 = %s, final_deleted_count = %s "
        "WHERE cleanup_id = %s AND target_key = %s "
        "AND cycle_generation = %s AND state = 'OPEN' "
        "AND completed_at IS NULL AND final_chain_sha256 IS NULL "
        "AND final_deleted_count IS NULL",
        (
            now,
            final_chain_sha256,
            deleted_count,
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
        ),
        authority="cleanup job completion",
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


class CleanupCycleRepository:
    """Own durable cleanup cycles, checkpoints, receipts, and response-loss replay."""

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
        shard = _model._require_shard(shard_no)
        cutoff = require_int63(cycle_cutoff_at, field="cleanup cycle_cutoff_at")
        max_rows = _model._require_batch_bound(max_rows_per_transaction)
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
        operation = _lock_cycle(work, requested)
        checkpoint = operation.initial_checkpoint
        if operation.complete:
            deleted_count = _require_completion_row(work, requested)
            return _complete_result(
                requested, deleted_count=deleted_count, replayed=True
            )
        if checkpoint is None:
            raise CleanupCorruptionError("OPEN cleanup cycle lacks a checkpoint")
        if requested.target_kind is CleanupTargetKind.PUBLICATION_COMMIT and (
            checkpoint.phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}
        ):
            _publication_commit_targets._require_publication_commit_post_compound_transition(
                operation,
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
        operation = _lock_cycle(work, requested)
        checkpoint = operation.initial_checkpoint
        if operation.complete:
            deleted_count = _require_completion_row(work, requested)
            return _complete_result(
                requested, deleted_count=deleted_count, replayed=True
            )
        if checkpoint is None:
            raise CleanupCorruptionError("OPEN cleanup cycle lacks a checkpoint")
        if requested.target_kind is CleanupTargetKind.PUBLICATION_COMMIT and (
            checkpoint.phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}
        ):
            _publication_commit_targets._require_publication_commit_post_compound_transition(
                operation,
                phase=checkpoint.phase,
                cursor=checkpoint.cursor,
            )

        if (
            checkpoint.generation == attempt.expected_generation + 1
            and checkpoint.receipt_batch_key == attempt.batch_key
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
        result, _next_checkpoint = _advance_checkpoint(
            operation, checkpoint, attempt, timestamp=timestamp
        )
        return result

    @staticmethod
    def advance_current_only_cycle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cycle: CleanupCycle,
        now: int | Callable[[], int],
    ) -> tuple[CleanupBatchResult, ...]:
        """Advance empty phases and at most one bounded deletion batch.

        The caller owns no cross-transaction batch command: response loss resumes
        the durable checkpoint on the next attempt. Every phase still writes its
        canonical checkpoint and receipt. Empty transitions share the already
        locked cycle authority, so no lower-rank gate/checkpoint lock is acquired
        after a phase takes its own locks. The first nonempty mutation ends this
        transaction, preserving the cycle's logical-key bound and child lock order.
        """

        requested = _require_cycle(cycle)
        timestamp = _require_exclusive_gate(work, gate_lease, now=now)
        operation = _lock_cycle(work, requested)
        checkpoint = operation.initial_checkpoint
        if operation.complete:
            return (
                _complete_result(
                    requested,
                    deleted_count=_require_completion_row(work, requested),
                    replayed=True,
                ),
            )
        if checkpoint is None:
            raise CleanupCorruptionError("OPEN cleanup cycle lacks a checkpoint")
        results: list[CleanupBatchResult] = []
        for _phase in _STRATEGIES[requested.target_kind].phases:
            with database_phase(
                "cleanup_phase",
                target=requested.target_kind.value,
                shard=requested.shard_no,
                phase=checkpoint.phase,
                checkpoint_generation=checkpoint.generation,
                attempted_logical_rows=None,
            ) as step:
                if requested.target_kind is CleanupTargetKind.PUBLICATION_COMMIT and (
                    checkpoint.phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}
                ):
                    _publication_commit_targets._require_publication_commit_post_compound_transition(
                        operation,
                        phase=checkpoint.phase,
                        cursor=checkpoint.cursor,
                    )
                result, next_checkpoint = _advance_checkpoint(
                    operation,
                    checkpoint,
                    CleanupBatchCommand(secrets.token_bytes(32), checkpoint.generation),
                    timestamp=timestamp,
                )
                step.describe(
                    attempted_logical_rows=result.row_count,
                    phase_complete=result.phase_complete,
                    cycle_complete=result.cycle_complete,
                )
            results.append(result)
            if result.row_count or result.cycle_complete:
                return tuple(results)
            if next_checkpoint is None:
                raise CleanupCorruptionError("empty transition lacks next checkpoint")
            checkpoint = next_checkpoint
        raise CleanupCorruptionError("cleanup exceeded its fixed phase count")
