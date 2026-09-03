"""Bounded operational-effect preparation for the greenfield vNext schema.

The effect objects accepted here are deliberately internal semantic values.
They are produced by a database-derived planner; they are not a public API and
must never be accepted directly from an untrusted client.  In particular, the
caller cannot choose sequence numbers, event identities, digests, rolling
chains, cursors, generations, or receipt counts.  Those authorities are
derived below while the preparation checkpoint is locked in the caller-owned
transaction.
"""

from __future__ import annotations

__all__ = [
    "DeletionConsumption",
    "OperationalBatchLimitError",
    "OperationalBatchReceipt",
    "OperationalEffect",
    "OperationalEffectConflictError",
    "OperationalEffectCorruptionError",
    "OperationalEffectRepository",
    "OperationalEffectSeal",
    "OperationalEffectStateError",
    "OperationalPreparation",
    "RemovedGid",
    "SUPERSEDED_DRAIN_STATES",
    "SupersededDrainPosition",
]

from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256

from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_POLICY_TABLE = "operational_operational_policys"
_STREAM_TABLE = "operational_operational_event_streams"
_PREPARATION_TABLE = "operational_operational_preparations"
_CHECKPOINT_TABLE = "operational_operational_preparation_checkpoints"
_RECEIPT_TABLE = "operational_operational_preparation_batch_receipts"
_SEAL_TABLE = "operational_operational_preparation_effect_seals"
_EVENT_TABLE = "operational_operational_events"
_REMOVED_TABLE = "operational_operational_removed_gid_events"
_DELETION_TABLE = "operational_operational_deletion_consumption_events"

_SOURCE_BUILD_STATE_TABLE = "catalog_source_build_states"
_DELETION_ATTEMPT_TABLE = "operational_deletion_request_attempts"
_DELETION_GENERATION_HEAD_TABLE = "operational_deletion_request_generation_heads"
_BUILD_GENERATION_TABLE = "operational_source_build_generations"
_SOURCE_WORKING_TABLE = "operational_source_working_builds"

_EFFECT_PHASE = "EFFECTS"
_MAX_SUPERSEDED_ABANDON_ROWS = 128
SUPERSEDED_DRAIN_STATES: tuple[str, ...] = ("COMPLETE", "OPEN")
"""Drain order of the two non-terminal preparation states.

A drainage position orders by ``(state index, preparation_id)``, so the two
single-state index ranges form one total keyset over every superseded row."""

_DRAIN_ROW_PREDICATE = (
    "p.build_id = %s AND p.state = %s "
    "AND NOT EXISTS (SELECT 1 FROM catalog_publication_commits AS committed "
    "WHERE committed.preparation_id = p.preparation_id)"
)
_CURRENT_ATTEMPT_EXCLUSION = (
    " AND NOT EXISTS (SELECT 1 "
    "FROM operational_publication_candidate_preparations AS bound "
    "WHERE bound.preparation_id = p.preparation_id)"
    " AND (p.operational_policy_id <> %s OR p.deletion_request_generation <> %s)"
)
_REMOVED_TYPE = "REMOVED_GID"
_DELETION_TYPE = "DELETION_CONSUMPTION"

_PREPARATION_ID_DOMAIN = b"h2hdb-operational-preparation-id-v1"
_EVENT_ID_DOMAIN = b"h2hdb-operational-event-id-v1"
_EVENT_DOMAIN = b"h2hdb-operational-event-v1"
_EMPTY_CHAIN_DOMAIN = b"h2hdb-operational-event-chain-v1"
_CHAIN_LINK_DOMAIN = b"h2hdb-operational-event-chain-link-v1"
_BATCH_KEY_DOMAIN = b"h2hdb-operational-event-batch-key-v1"
_BATCH_INPUT_DOMAIN = b"h2hdb-operational-event-batch-input-v1"
_BATCH_OUTPUT_DOMAIN = b"h2hdb-operational-event-batch-output-v1"


class OperationalEffectStateError(RuntimeError):
    """The requested transition is not valid for the durable state."""


class OperationalEffectConflictError(RuntimeError):
    """An immutable idempotency identity names different exact facts."""


class OperationalEffectCorruptionError(RuntimeError):
    """Persisted normalized facts do not satisfy the writer contract."""


class OperationalBatchLimitError(ValueError):
    """One mutation exceeds the operational policy's database-owned cap."""


@dataclass(frozen=True, slots=True)
class SupersededDrainPosition:
    """Durable seek authority of one superseded-preparation drainage page.

    It is the least ``(state, preparation_id)`` of the build's superseded rows
    at the time it was read from the database, never a caller-invented
    cursor: the page writer reloads it and fails closed when the caller's copy
    differs.  Every committed page abandons the least matching rows, so the
    position read after a committed page is strictly greater than the one
    that page was taken from; that strict advance is the liveness fence the
    orchestrators check between pages."""

    state: str
    preparation_id: bytes

    def __post_init__(self) -> None:
        if self.state not in SUPERSEDED_DRAIN_STATES:
            raise ValueError("drain position state is not a drain state")
        require_uuid16(self.preparation_id, field="drain position preparation_id")

    @property
    def key(self) -> tuple[int, bytes]:
        return (SUPERSEDED_DRAIN_STATES.index(self.state), self.preparation_id)

    def advances_past(self, previous: SupersededDrainPosition | None) -> bool:
        """Whether this position is strictly after ``previous`` in drain order."""

        return previous is None or self.key > previous.key


@dataclass(frozen=True, slots=True)
class RemovedGid:
    gid: int
    request_token: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gid, field="removed gid")
        require_uuid16(self.request_token, field="removed gid request_token")


@dataclass(frozen=True, slots=True)
class DeletionConsumption:
    gid: int
    deletion_request_token: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gid, field="deletion consumption gid")
        require_uuid16(
            self.deletion_request_token,
            field="deletion consumption request_token",
        )


type OperationalEffect = RemovedGid | DeletionConsumption


@dataclass(frozen=True, slots=True)
class OperationalPreparation:
    preparation_id: bytes
    build_id: bytes
    deletion_request_generation: int
    operational_policy_id: int
    prepared_at: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class OperationalBatchReceipt:
    preparation_id: bytes
    start_sequence_no: int
    next_sequence_no: int
    row_count: int
    committed_generation: int
    committed_at: int
    terminal: bool
    replayed: bool


@dataclass(frozen=True, slots=True)
class OperationalEffectSeal:
    preparation_id: bytes
    event_count: int
    final_chain_sha256: bytes
    sealed_at: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class _PreparationRow:
    build_id: bytes
    deletion_request_generation: int
    operational_policy_id: int
    state: str
    prepared_at: int
    completed_at: int | None
    max_batch_rows: int


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    generation: int
    cursor: bytes
    processed_count: int
    chain_sha256: bytes
    state: str
    updated_at: int


@dataclass(frozen=True, slots=True)
class _PreparedEvent:
    sequence_no: int
    event_id: bytes
    event_type: str
    event_sha256: bytes
    effect: OperationalEffect


class OperationalEffectRepository:
    """Prepare and seal one transient publication-owned effect snapshot."""

    @staticmethod
    def begin(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        operational_policy_id: int,
        now: int,
    ) -> OperationalPreparation:
        build = require_uuid16(build_id, field="operational preparation build_id")
        policy_id = require_int63(
            operational_policy_id,
            field="operational policy id",
        )
        timestamp = require_int63(now, field="operational preparation prepared_at")
        _authorize_build(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build,
            now=timestamp,
        )

        authority = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("operational-preparation", 2, build, policy_id),
            f"SELECT b.state, p.max_batch_rows FROM {_SOURCE_BUILD_STATE_TABLE} b "
            f"JOIN {_POLICY_TABLE} p ON p.operational_policy_id = %s "
            "WHERE b.build_id = %s",
            (policy_id, build),
        )
        if len(authority) != 2:
            raise OperationalEffectStateError(
                "source build or operational policy is missing"
            )
        if authority[0] != "SEALED":
            raise OperationalEffectStateError(
                "operational preparation requires a SEALED source build"
            )
        _require_batch_cap(authority[1])

        generation_row = work.connector.fetch_one(
            f"SELECT current_generation FROM {_DELETION_GENERATION_HEAD_TABLE} "
            "WHERE singleton_id = %s",
            (1,),
        )
        if len(generation_row) != 1:
            raise OperationalEffectCorruptionError(
                "deletion-request generation head is missing"
            )
        deletion_generation = require_int63(
            generation_row[0],
            field="preparation deletion_request_generation",
        )
        preparation_id = _preparation_id(build, deletion_generation, policy_id)

        existing = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key(
                "operational-preparation",
                3,
                build,
                deletion_generation,
                policy_id,
            ),
            f"SELECT preparation_id, prepared_at FROM {_PREPARATION_TABLE} "
            "WHERE build_id = %s AND deletion_request_generation = %s "
            "AND operational_policy_id = %s",
            (build, deletion_generation, policy_id),
        )
        if existing:
            if len(existing) != 2:
                raise OperationalEffectCorruptionError(
                    "operational preparation natural key returned malformed facts"
                )
            stored_id = require_uuid16(
                existing[0], field="stored operational preparation id"
            )
            prepared_at = require_int63(
                existing[1], field="stored operational prepared_at"
            )
            if stored_id != preparation_id:
                raise OperationalEffectConflictError(
                    "preparation natural key has a non-canonical identity"
                )
            OperationalEffectRepository._validate_existing_root(
                work, stored_id, prepared_at
            )
            return OperationalPreparation(
                stored_id,
                build,
                deletion_generation,
                policy_id,
                prepared_at,
                True,
            )

        collision = work.connector.fetch_one(
            f"SELECT build_id, deletion_request_generation, operational_policy_id "
            f"FROM {_PREPARATION_TABLE} WHERE preparation_id = %s",
            (preparation_id,),
        )
        if collision:
            raise OperationalEffectConflictError(
                "canonical preparation identity collides with another natural key"
            )

        chain = _empty_chain()
        cursor = _encode_cursor(0)
        work.connector.execute(
            f"INSERT INTO {_STREAM_TABLE} (preparation_id, created_at) VALUES (%s, %s)",
            (preparation_id, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_PREPARATION_TABLE} "
            "(preparation_id, build_id, deletion_request_generation, "
            "operational_policy_id, state, prepared_at, completed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, NULL)",
            (
                preparation_id,
                build,
                deletion_generation,
                policy_id,
                "OPEN",
                timestamp,
            ),
        )
        work.connector.execute(
            f"INSERT INTO {_CHECKPOINT_TABLE} "
            "(preparation_id, phase, generation, cursor_bytes, processed_count, "
            "chain_sha256, state, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                preparation_id,
                _EFFECT_PHASE,
                0,
                cursor,
                0,
                chain,
                "OPEN",
                timestamp,
            ),
        )
        return OperationalPreparation(
            preparation_id,
            build,
            deletion_generation,
            policy_id,
            timestamp,
            False,
        )

    @staticmethod
    def append_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        preparation_id: bytes,
        effects: Sequence[OperationalEffect],
        now: int,
    ) -> OperationalBatchReceipt:
        preparation = require_uuid16(preparation_id, field="operational preparation id")
        timestamp = require_int63(now, field="operational batch committed_at")
        build = _load_preparation_build(work, preparation)
        _authorize_build(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build,
            now=timestamp,
        )
        row = OperationalEffectRepository._lock_preparation(work, preparation)
        if row.build_id != build:
            raise OperationalEffectCorruptionError(
                "operational preparation build changed during authorization"
            )
        exact_effects = _require_effects(
            effects,
            max_rows=row.max_batch_rows,
        )

        request_frame = _effect_request_frame(preparation, exact_effects)
        batch_key = _digest(_BATCH_KEY_DOMAIN, request_frame)
        input_sha256 = _digest(_BATCH_INPUT_DOMAIN, request_frame)
        checkpoint = OperationalEffectRepository._lock_checkpoint(work, preparation)

        receipt_row = work.connector.fetch_one(
            f"SELECT start_cursor, next_cursor, input_sha256, output_sha256, "
            "row_count, committed_generation, committed_at "
            f"FROM {_RECEIPT_TABLE} WHERE preparation_id = %s AND phase = %s "
            "AND batch_key = %s",
            (preparation, _EFFECT_PHASE, batch_key),
        )
        if receipt_row:
            return OperationalEffectRepository._validate_batch_replay(
                work,
                preparation=preparation,
                effects=exact_effects,
                input_sha256=input_sha256,
                checkpoint=checkpoint,
                receipt_row=receipt_row,
            )

        if row.state != "OPEN":
            raise OperationalEffectStateError(
                "only an OPEN preparation accepts new effect batches"
            )
        if checkpoint.state != "OPEN":
            raise OperationalEffectStateError(
                "the preparation checkpoint already has its terminal receipt"
            )
        if timestamp < checkpoint.updated_at:
            raise OperationalEffectStateError(
                "operational batch timestamp precedes its checkpoint"
            )
        start = _decode_cursor(checkpoint.cursor)
        if start != checkpoint.processed_count:
            raise OperationalEffectCorruptionError(
                "checkpoint cursor and processed count disagree"
            )
        if checkpoint.generation == INT63_MAX:
            raise OperationalEffectStateError(
                "operational checkpoint generation is exhausted"
            )
        if len(exact_effects) > INT63_MAX - start:
            raise OperationalEffectStateError("operational event sequence is exhausted")

        prepared_events, next_chain = _prepare_events(
            preparation,
            start,
            checkpoint.chain_sha256,
            exact_effects,
        )
        OperationalEffectRepository._validate_deletion_attempts(work, prepared_events)
        for event in prepared_events:
            work.connector.execute(
                f"INSERT INTO {_EVENT_TABLE} "
                "(event_id, preparation_id, sequence_no, event_type, "
                "event_sha256, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    event.event_id,
                    preparation,
                    event.sequence_no,
                    event.event_type,
                    event.event_sha256,
                    timestamp,
                ),
            )
            if isinstance(event.effect, RemovedGid):
                work.connector.execute(
                    f"INSERT INTO {_REMOVED_TABLE} "
                    "(event_id, gid, request_token) VALUES (%s, %s, %s)",
                    (
                        event.event_id,
                        event.effect.gid,
                        event.effect.request_token,
                    ),
                )
            else:
                work.connector.execute(
                    f"INSERT INTO {_DELETION_TABLE} "
                    "(event_id, gid, deletion_request_token) VALUES (%s, %s, %s)",
                    (
                        event.event_id,
                        event.effect.gid,
                        event.effect.deletion_request_token,
                    ),
                )

        next_sequence = start + len(prepared_events)
        next_cursor = _encode_cursor(next_sequence)
        successor = checkpoint.generation + 1
        terminal = not prepared_events
        output_sha256 = _batch_output_digest(
            preparation,
            start=start,
            next_sequence=next_sequence,
            committed_generation=successor,
            events=prepared_events,
            terminal=terminal,
        )
        work.connector.execute(
            f"INSERT INTO {_RECEIPT_TABLE} "
            "(preparation_id, phase, batch_key, start_cursor, next_cursor, "
            "input_sha256, output_sha256, row_count, committed_generation, "
            "committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                preparation,
                _EFFECT_PHASE,
                batch_key,
                checkpoint.cursor,
                next_cursor,
                input_sha256,
                output_sha256,
                len(prepared_events),
                successor,
                timestamp,
            ),
        )
        work.compare_and_swap(
            f"UPDATE {_CHECKPOINT_TABLE} SET generation = %s, cursor_bytes = %s, "
            "processed_count = %s, chain_sha256 = %s, state = %s, updated_at = %s "
            "WHERE preparation_id = %s AND phase = %s AND generation = %s "
            "AND cursor_bytes = %s AND processed_count = %s AND chain_sha256 = %s "
            "AND state = %s AND updated_at = %s",
            (
                successor,
                next_cursor,
                next_sequence,
                next_chain,
                "COMPLETE" if terminal else "OPEN",
                timestamp,
                preparation,
                _EFFECT_PHASE,
                checkpoint.generation,
                checkpoint.cursor,
                checkpoint.processed_count,
                checkpoint.chain_sha256,
                checkpoint.state,
                checkpoint.updated_at,
            ),
            authority="operational effect checkpoint",
        )
        return OperationalBatchReceipt(
            preparation,
            start,
            next_sequence,
            len(prepared_events),
            successor,
            timestamp,
            terminal,
            False,
        )

    @staticmethod
    def seal(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        preparation_id: bytes,
        now: int,
    ) -> OperationalEffectSeal:
        preparation = require_uuid16(preparation_id, field="operational preparation id")
        timestamp = require_int63(now, field="operational effect sealed_at")
        build = _load_preparation_build(work, preparation)
        _authorize_build(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build,
            now=timestamp,
        )
        row = OperationalEffectRepository._lock_preparation(work, preparation)
        if row.build_id != build:
            raise OperationalEffectCorruptionError(
                "operational preparation build changed during authorization"
            )
        checkpoint = OperationalEffectRepository._lock_checkpoint(work, preparation)

        existing = work.connector.fetch_one(
            f"SELECT event_count, final_chain_sha256, sealed_at "
            f"FROM {_SEAL_TABLE} WHERE preparation_id = %s",
            (preparation,),
        )
        if existing:
            seal = _require_seal_row(preparation, existing, replayed=True)
            if (
                row.state != "COMPLETE"
                or checkpoint.state != "COMPLETE"
                or seal.event_count != checkpoint.processed_count
                or seal.final_chain_sha256 != checkpoint.chain_sha256
            ):
                raise OperationalEffectCorruptionError(
                    "effect seal disagrees with its completed control state"
                )
            return seal

        if row.state != "OPEN" or checkpoint.state != "COMPLETE":
            raise OperationalEffectStateError(
                "effect seal requires an empty terminal batch receipt"
            )
        OperationalEffectRepository._validate_terminal_receipt(
            work, preparation, checkpoint
        )
        if timestamp < row.prepared_at:
            raise OperationalEffectStateError(
                "effect seal timestamp precedes preparation"
            )

        work.connector.execute(
            f"INSERT INTO {_SEAL_TABLE} "
            "(preparation_id, event_count, final_chain_sha256, sealed_at) "
            "VALUES (%s, %s, %s, %s)",
            (
                preparation,
                checkpoint.processed_count,
                checkpoint.chain_sha256,
                timestamp,
            ),
        )
        work.compare_and_swap(
            f"UPDATE {_PREPARATION_TABLE} SET state = %s, completed_at = %s "
            "WHERE preparation_id = %s AND state = %s AND completed_at IS NULL",
            ("COMPLETE", timestamp, preparation, "OPEN"),
            authority="operational preparation completion",
        )
        return OperationalEffectSeal(
            preparation,
            checkpoint.processed_count,
            checkpoint.chain_sha256,
            timestamp,
            False,
        )

    @staticmethod
    def _load_complete_seal_authorized(
        work: VNextUnitOfWork,
        *,
        preparation_id: bytes,
        build_id: bytes,
    ) -> OperationalEffectSeal:
        """Load an immutable seal after the application fenced this transaction.

        This is the read-only replay side used by compound orchestration.  It
        cannot transition an OPEN preparation and therefore never reacquires a
        lower-ranked gate or ingest lock after the outer authorization.
        """

        preparation = require_uuid16(
            preparation_id,
            field="operational preparation id",
        )
        expected_build = require_uuid16(
            build_id,
            field="operational preparation build_id",
        )
        control = work.connector.fetch_one(
            f"SELECT p.build_id, p.state, p.prepared_at, p.completed_at, "
            "c.generation, c.cursor_bytes, c.processed_count, c.chain_sha256, "
            f"c.state, c.updated_at FROM {_PREPARATION_TABLE} AS p "
            f"JOIN {_CHECKPOINT_TABLE} AS c ON c.preparation_id = p.preparation_id "
            "AND c.phase = %s WHERE p.preparation_id = %s",
            (_EFFECT_PHASE, preparation),
        )
        if len(control) != 10:
            raise OperationalEffectCorruptionError(
                "completed operational preparation control is missing"
            )
        if (
            require_uuid16(control[0], field="stored preparation build_id")
            != expected_build
        ):
            raise OperationalEffectStateError(
                "completed operational preparation belongs to another build"
            )
        if control[1] != "COMPLETE" or control[8] != "COMPLETE":
            raise OperationalEffectStateError(
                "operational effect seal replay requires COMPLETE control state"
            )
        prepared_at = require_int63(
            control[2],
            field="stored preparation prepared_at",
        )
        completed_at = require_int63(
            control[3],
            field="stored preparation completed_at",
        )
        require_int63(control[4], field="operational checkpoint generation")
        cursor = require_bounded_bytes(
            control[5],
            field="operational checkpoint cursor",
            minimum=8,
            maximum=8,
        )
        processed_count = require_int63(
            control[6],
            field="operational checkpoint processed_count",
        )
        chain = require_digest32(
            control[7],
            field="operational checkpoint chain",
        )
        updated_at = require_int63(
            control[9],
            field="operational checkpoint updated_at",
        )
        if _decode_cursor(cursor) != processed_count:
            raise OperationalEffectCorruptionError(
                "operational checkpoint cursor and count disagree"
            )
        seal = _require_seal_row(
            preparation,
            work.connector.fetch_one(
                f"SELECT event_count, final_chain_sha256, sealed_at "
                f"FROM {_SEAL_TABLE} WHERE preparation_id = %s",
                (preparation,),
            ),
            replayed=True,
        )
        if (
            seal.event_count != processed_count
            or seal.final_chain_sha256 != chain
            or seal.sealed_at != completed_at
            or seal.sealed_at < max(prepared_at, updated_at)
        ):
            raise OperationalEffectCorruptionError(
                "effect seal disagrees with its completed control state"
            )
        return seal

    @staticmethod
    def _lock_preparation(
        work: VNextUnitOfWork, preparation_id: bytes
    ) -> _PreparationRow:
        row = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("operational-preparation-row", preparation_id),
            f"SELECT o.build_id, o.deletion_request_generation, "
            "o.operational_policy_id, o.state, o.prepared_at, o.completed_at, "
            f"p.max_batch_rows FROM {_PREPARATION_TABLE} o "
            f"JOIN {_POLICY_TABLE} p "
            "ON p.operational_policy_id = o.operational_policy_id "
            "WHERE o.preparation_id = %s",
            (preparation_id,),
        )
        if len(row) != 7:
            raise OperationalEffectStateError(
                "operational preparation or policy is missing"
            )
        state = row[3]
        if state not in {"OPEN", "COMPLETE", "FAILED", "ABANDONED"}:
            raise OperationalEffectCorruptionError(
                "operational preparation has an invalid state"
            )
        completed_at = (
            None
            if row[5] is None
            else require_int63(row[5], field="stored preparation completed_at")
        )
        return _PreparationRow(
            require_uuid16(row[0], field="stored preparation build_id"),
            require_int63(row[1], field="stored preparation deletion generation"),
            require_int63(row[2], field="stored preparation policy id"),
            state,
            require_int63(row[4], field="stored preparation prepared_at"),
            completed_at,
            _require_batch_cap(row[6]),
        )

    @staticmethod
    def _lock_checkpoint(work: VNextUnitOfWork, preparation_id: bytes) -> _Checkpoint:
        row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("operational-effect-checkpoint", preparation_id),
            f"SELECT generation, cursor_bytes, processed_count, chain_sha256, "
            f"state, updated_at FROM {_CHECKPOINT_TABLE} "
            "WHERE preparation_id = %s AND phase = %s",
            (preparation_id, _EFFECT_PHASE),
        )
        if len(row) != 6:
            raise OperationalEffectCorruptionError(
                "operational effect checkpoint is missing"
            )
        state = row[4]
        if state not in {"OPEN", "COMPLETE"}:
            raise OperationalEffectCorruptionError(
                "operational effect checkpoint has an invalid state"
            )
        checkpoint = _Checkpoint(
            require_int63(row[0], field="operational checkpoint generation"),
            require_bounded_bytes(
                row[1], field="operational checkpoint cursor", minimum=8, maximum=8
            ),
            require_int63(row[2], field="operational checkpoint processed_count"),
            require_digest32(row[3], field="operational checkpoint chain"),
            state,
            require_int63(row[5], field="operational checkpoint updated_at"),
        )
        if _decode_cursor(checkpoint.cursor) != checkpoint.processed_count:
            raise OperationalEffectCorruptionError(
                "operational checkpoint cursor and count disagree"
            )
        return checkpoint

    @staticmethod
    def superseded_drain_position(
        work: VNextUnitOfWork,
        *,
        build_id: bytes,
        policy_id: int,
        deletion_generation: int,
    ) -> SupersededDrainPosition | None:
        """Load the durable position of the build's next drainage page.

        A superseded preparation is an unbound, uncommitted OPEN or COMPLETE
        attempt of the build whose policy or deletion-request generation
        differs from the attempt about to begin.  The position is the least
        ``(state, preparation_id)`` still matching, read by one bounded
        ``LIMIT 1`` seek on the ``(build_id, state, preparation_id)`` index per
        drain state; ``None`` means the build is drained.  The issue path
        emits one drainage page per transaction from this position until it
        is ``None``; a replay after a lost commit response simply re-reads
        it."""

        build = require_uuid16(build_id, field="superseded probe build_id")
        return _drain_position(
            work,
            build_id=build,
            exclusion=(
                require_positive_int63(policy_id, field="superseded probe policy"),
                require_int63(deletion_generation, field="superseded probe generation"),
            ),
        )

    @staticmethod
    def abandon_superseded_preparations(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        policy_id: int,
        deletion_generation: int,
        position: SupersededDrainPosition,
        now: int,
    ) -> int:
        """Abandon one bounded page of this build's superseded attempts.

        Generations only advance and a replaced policy is never re-issued for
        the same build, so an unbound, uncommitted OPEN or COMPLETE preparation
        whose policy or generation differs can never be published; left alone
        it would retain its build forever because generic cleanup reclaims only
        ABANDONED attempts.  A bound attempt is superseded by the binding path
        instead, and a committed one is publication lineage.

        The page is one index range seek: the rows of ``position.state`` with
        ``preparation_id >= position.preparation_id``, in preparation_id order,
        hard-capped at 128.  ``position`` is caller-carried authority and is
        therefore not trusted: the durable position is reloaded first and a
        stale copy fails closed with zero writes.  Each abandoned row leaves
        the predicate, so the page is idempotent under response loss and the
        next durable position is strictly greater."""

        build = require_uuid16(build_id, field="superseded abandon build_id")
        timestamp = require_int63(now, field="superseded abandon now")
        exclusion = (
            require_positive_int63(policy_id, field="superseded abandon policy"),
            require_int63(deletion_generation, field="superseded abandon generation"),
        )
        _authorize_build(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build,
            now=timestamp,
        )
        return _abandon_drain_page(
            work,
            build_id=build,
            exclusion=exclusion,
            position=position,
            now=timestamp,
            authority="superseded operational preparation",
        )

    @staticmethod
    def retiring_build_drain_position(
        work: VNextUnitOfWork,
        *,
        build_id: bytes,
    ) -> SupersededDrainPosition | None:
        """Durable position of a retiring build's next drainage page.

        A build being retired (a SEALED working build that will never publish)
        drains every uncommitted OPEN or COMPLETE preparation, bound to its
        orphaned candidate or not, with no current attempt to exclude.  The
        caller holds the source working-root lock; this reads the least
        matching ``(state, preparation_id)`` or ``None`` when drained."""

        build = require_uuid16(build_id, field="retiring build probe build_id")
        return _drain_position(work, build_id=build, exclusion=None)

    @staticmethod
    def abandon_retiring_build_preparations(
        work: VNextUnitOfWork,
        *,
        build_id: bytes,
        position: SupersededDrainPosition,
        now: int,
    ) -> int:
        """Abandon one bounded page of a retiring build's preparations.

        The same seek page as :meth:`abandon_superseded_preparations` without
        a current-attempt exclusion and without the live-generation build
        authorization, which the retiring build no longer satisfies; the
        source handoff that calls this already holds the gate, the ingest
        turn and the sole working-root lock."""

        build = require_uuid16(build_id, field="retiring build page build_id")
        timestamp = require_int63(now, field="retiring build page now")
        return _abandon_drain_page(
            work,
            build_id=build,
            exclusion=None,
            position=position,
            now=timestamp,
            authority="retiring build operational preparation",
        )

    @staticmethod
    def _validate_existing_root(
        work: VNextUnitOfWork, preparation_id: bytes, prepared_at: int
    ) -> None:
        stream = work.connector.fetch_one(
            f"SELECT created_at FROM {_STREAM_TABLE} WHERE preparation_id = %s",
            (preparation_id,),
        )
        if (
            len(stream) != 1
            or require_int63(stream[0], field="stored event stream created_at")
            != prepared_at
        ):
            raise OperationalEffectCorruptionError(
                "preparation and durable event-stream roots disagree"
            )
        count_row = work.connector.fetch_one(
            f"SELECT COUNT(*) FROM {_CHECKPOINT_TABLE} WHERE preparation_id = %s",
            (preparation_id,),
        )
        if count_row != (1,):
            raise OperationalEffectCorruptionError(
                "preparation does not have exactly one required checkpoint"
            )
        OperationalEffectRepository._lock_checkpoint(work, preparation_id)

    @staticmethod
    def _validate_deletion_attempts(
        work: VNextUnitOfWork, events: Sequence[_PreparedEvent]
    ) -> None:
        for event in events:
            if not isinstance(event.effect, DeletionConsumption):
                continue
            attempt = work.connector.fetch_one(
                f"SELECT gid FROM {_DELETION_ATTEMPT_TABLE} WHERE request_token = %s",
                (event.effect.deletion_request_token,),
            )
            if (
                len(attempt) != 1
                or require_positive_int63(
                    attempt[0], field="stored deletion attempt gid"
                )
                != event.effect.gid
            ):
                raise OperationalEffectConflictError(
                    "deletion consumption does not match its immutable request attempt"
                )

    @staticmethod
    def _validate_batch_replay(
        work: VNextUnitOfWork,
        *,
        preparation: bytes,
        effects: tuple[OperationalEffect, ...],
        input_sha256: bytes,
        checkpoint: _Checkpoint,
        receipt_row: tuple[object, ...],
    ) -> OperationalBatchReceipt:
        if len(receipt_row) != 7:
            raise OperationalEffectCorruptionError(
                "operational batch receipt is malformed"
            )
        start_cursor = require_bounded_bytes(
            receipt_row[0], field="receipt start_cursor", minimum=8, maximum=8
        )
        next_cursor = require_bounded_bytes(
            receipt_row[1], field="receipt next_cursor", minimum=8, maximum=8
        )
        stored_input = require_digest32(receipt_row[2], field="receipt input_sha256")
        stored_output = require_digest32(receipt_row[3], field="receipt output_sha256")
        row_count = require_int63(receipt_row[4], field="receipt row_count")
        generation = require_int63(receipt_row[5], field="receipt committed_generation")
        committed_at = require_int63(receipt_row[6], field="receipt committed_at")
        start = _decode_cursor(start_cursor)
        next_sequence = _decode_cursor(next_cursor)
        if (
            stored_input != input_sha256
            or row_count != len(effects)
            or next_sequence != start + len(effects)
            or generation == 0
            or generation > checkpoint.generation
        ):
            raise OperationalEffectConflictError(
                "batch identity does not match its exact receipt facts"
            )

        prepared = OperationalEffectRepository._read_and_validate_events(
            work, preparation, start, effects
        )
        expected_output = _batch_output_digest(
            preparation,
            start=start,
            next_sequence=next_sequence,
            committed_generation=generation,
            events=prepared,
            terminal=not effects,
        )
        if stored_output != expected_output:
            raise OperationalEffectCorruptionError(
                "batch output digest disagrees with exact persisted events"
            )
        if generation == checkpoint.generation and (
            checkpoint.cursor != next_cursor
            or checkpoint.processed_count != next_sequence
            or checkpoint.state != ("COMPLETE" if not effects else "OPEN")
        ):
            raise OperationalEffectCorruptionError(
                "latest batch receipt disagrees with its checkpoint poststate"
            )
        return OperationalBatchReceipt(
            preparation,
            start,
            next_sequence,
            row_count,
            generation,
            committed_at,
            not effects,
            True,
        )

    @staticmethod
    def _read_and_validate_events(
        work: VNextUnitOfWork,
        preparation: bytes,
        start: int,
        effects: Sequence[OperationalEffect],
    ) -> tuple[_PreparedEvent, ...]:
        result: list[_PreparedEvent] = []
        for offset, effect in enumerate(effects):
            sequence = start + offset
            event_row = work.connector.fetch_one(
                f"SELECT event_id, event_type, event_sha256 FROM {_EVENT_TABLE} "
                "WHERE preparation_id = %s AND sequence_no = %s",
                (preparation, sequence),
            )
            if len(event_row) != 3:
                raise OperationalEffectCorruptionError(
                    "batch replay is missing a base event"
                )
            event_type = _event_type(effect)
            event_id = _event_id(preparation, sequence)
            event_sha256 = _event_digest(preparation, sequence, effect)
            if event_row != (event_id, event_type, event_sha256):
                raise OperationalEffectCorruptionError(
                    "base event disagrees with the exact typed effect"
                )
            if isinstance(effect, RemovedGid):
                subtype = work.connector.fetch_one(
                    f"SELECT gid, request_token FROM {_REMOVED_TABLE} "
                    "WHERE event_id = %s",
                    (event_id,),
                )
                opposite = work.connector.fetch_one(
                    f"SELECT event_id FROM {_DELETION_TABLE} WHERE event_id = %s",
                    (event_id,),
                )
                expected_subtype = (effect.gid, effect.request_token)
            else:
                subtype = work.connector.fetch_one(
                    f"SELECT gid, deletion_request_token FROM {_DELETION_TABLE} "
                    "WHERE event_id = %s",
                    (event_id,),
                )
                opposite = work.connector.fetch_one(
                    f"SELECT event_id FROM {_REMOVED_TABLE} WHERE event_id = %s",
                    (event_id,),
                )
                expected_subtype = (effect.gid, effect.deletion_request_token)
            if subtype != expected_subtype or opposite:
                raise OperationalEffectCorruptionError(
                    "event subtype does not equal its complete typed tuple"
                )
            result.append(
                _PreparedEvent(sequence, event_id, event_type, event_sha256, effect)
            )
        return tuple(result)

    @staticmethod
    def _validate_terminal_receipt(
        work: VNextUnitOfWork,
        preparation: bytes,
        checkpoint: _Checkpoint,
    ) -> None:
        request_frame = _effect_request_frame(preparation, ())
        batch_key = _digest(_BATCH_KEY_DOMAIN, request_frame)
        input_sha256 = _digest(_BATCH_INPUT_DOMAIN, request_frame)
        row = work.connector.fetch_one(
            f"SELECT start_cursor, next_cursor, input_sha256, output_sha256, "
            "row_count, committed_generation, committed_at "
            f"FROM {_RECEIPT_TABLE} WHERE preparation_id = %s AND phase = %s "
            "AND batch_key = %s",
            (preparation, _EFFECT_PHASE, batch_key),
        )
        OperationalEffectRepository._validate_batch_replay(
            work,
            preparation=preparation,
            effects=(),
            input_sha256=input_sha256,
            checkpoint=checkpoint,
            receipt_row=row,
        )


def _load_preparation_build(
    work: VNextUnitOfWork,
    preparation_id: bytes,
) -> bytes:
    row = work.connector.fetch_one(
        f"SELECT build_id FROM {_PREPARATION_TABLE} WHERE preparation_id = %s",
        (preparation_id,),
    )
    if len(row) != 1:
        raise OperationalEffectStateError("operational preparation is missing")
    return require_uuid16(row[0], field="operational preparation build_id")


def _drain_sql(*, exclusion: tuple[int, int] | None) -> str:
    """The drain predicate of one state of one build.

    With a current-attempt ``exclusion`` (policy, deletion generation) the
    predicate names the live build's superseded attempts: uncommitted, not
    bound to a candidate, and of another policy or generation.  Without one
    it names every uncommitted attempt of a retiring build, bound or not: the
    retiring build's orphaned candidate can never publish, so its binding is
    no longer retention authority and candidate cleanup removes it later."""

    predicate = _DRAIN_ROW_PREDICATE
    if exclusion is not None:
        predicate += _CURRENT_ATTEMPT_EXCLUSION
    return predicate


def _drain_position(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
    exclusion: tuple[int, int] | None,
) -> SupersededDrainPosition | None:
    predicate = _drain_sql(exclusion=exclusion)
    for state in SUPERSEDED_DRAIN_STATES:
        data: tuple[object, ...] = (build_id, state)
        if exclusion is not None:
            data += exclusion
        row = work.connector.fetch_one(
            f"SELECT p.preparation_id FROM {_PREPARATION_TABLE} AS p "
            f"WHERE {predicate} ORDER BY p.preparation_id LIMIT 1",
            data,
        )
        if row:
            if len(row) != 1:
                raise OperationalEffectCorruptionError(
                    "superseded drainage position row is malformed"
                )
            return SupersededDrainPosition(
                state,
                require_uuid16(row[0], field="superseded drain preparation_id"),
            )
    return None


def _abandon_drain_page(
    work: VNextUnitOfWork,
    *,
    build_id: bytes,
    exclusion: tuple[int, int] | None,
    position: SupersededDrainPosition,
    now: int,
    authority: str,
) -> int:
    if type(position) is not SupersededDrainPosition:
        raise TypeError("position must be an exact SupersededDrainPosition")
    position.__post_init__()
    durable = _drain_position(work, build_id=build_id, exclusion=exclusion)
    if durable != position:
        raise OperationalEffectStateError(
            "superseded drainage position is stale; the durable position "
            "must be re-issued before another page"
        )
    data: tuple[object, ...] = (build_id, position.state)
    if exclusion is not None:
        data += exclusion
    stale = work.connector.fetch_all(
        "SELECT p.preparation_id, p.state, p.prepared_at, p.completed_at "
        f"FROM {_PREPARATION_TABLE} AS p WHERE {_drain_sql(exclusion=exclusion)} "
        "AND p.preparation_id >= %s ORDER BY p.preparation_id LIMIT %s",
        (*data, position.preparation_id, _MAX_SUPERSEDED_ABANDON_ROWS),
    )
    if not stale:
        raise OperationalEffectCorruptionError(
            "superseded drainage page is empty at its durable position"
        )
    for row in stale:
        if len(row) != 4:
            raise OperationalEffectCorruptionError(
                "superseded operational preparation facts are malformed"
            )
        stale_id = require_uuid16(row[0], field="superseded preparation_id")
        state = str(row[1])
        if state != position.state:
            raise OperationalEffectCorruptionError(
                "superseded drainage page crossed its drain state"
            )
        prepared_at = require_int63(row[2], field="superseded prepared_at")
        completed_at = (
            max(prepared_at, now)
            if row[3] is None
            else require_int63(row[3], field="superseded completed_at")
        )
        work.compare_and_swap(
            f"UPDATE {_PREPARATION_TABLE} SET state = 'ABANDONED', "
            "completed_at = %s WHERE preparation_id = %s AND state = %s",
            (completed_at, stale_id, state),
            authority=authority,
        )
    return len(stale)


def drain_page_sql(*, exclusion: bool) -> str:
    """The exact page SQL, exposed for query-plan evidence tests."""

    predicate = _drain_sql(exclusion=(0, 0) if exclusion else None)
    return (
        "SELECT p.preparation_id, p.state, p.prepared_at, p.completed_at "
        f"FROM {_PREPARATION_TABLE} AS p WHERE {predicate} "
        "AND p.preparation_id >= %s ORDER BY p.preparation_id LIMIT %s"
    )


def drain_position_sql(*, exclusion: bool) -> str:
    """The exact position-probe SQL, exposed for query-plan evidence tests."""

    predicate = _drain_sql(exclusion=(0, 0) if exclusion else None)
    return (
        f"SELECT p.preparation_id FROM {_PREPARATION_TABLE} AS p "
        f"WHERE {predicate} ORDER BY p.preparation_id LIMIT 1"
    )


def _authorize_build(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    build_id: bytes,
    now: int,
) -> int:
    gate = MaintenanceGateRepository.lock_and_require_live(work, gate_lease, now=now)
    if gate.mode is not GateMode.SHARED:
        raise OperationalEffectStateError(
            "operational preparation requires a live SHARED maintenance gate"
        )
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    generation = require_int63(turn.generation, field="operational ingest generation")
    mapping = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("operational-preparation", 0, generation),
        f"SELECT build_id FROM {_BUILD_GENERATION_TABLE} WHERE generation = %s",
        (generation,),
    )
    if mapping != (build_id,):
        raise OperationalEffectStateError(
            "live ingest generation is not mapped to the preparation build"
        )
    working = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("operational-preparation", 1, 1),
        f"SELECT build_id FROM {_SOURCE_WORKING_TABLE} WHERE slot = %s",
        (1,),
    )
    if working != (build_id,):
        raise OperationalEffectStateError(
            "preparation build does not own the source working slot"
        )
    return generation


def _require_effects(
    effects: object,
    *,
    max_rows: int,
) -> tuple[OperationalEffect, ...]:
    if isinstance(effects, (bytes, bytearray, str)) or not isinstance(
        effects, Sequence
    ):
        raise TypeError("effects must be a bounded sequence of typed effects")
    if len(effects) > max_rows:
        raise OperationalBatchLimitError(
            f"effect batch has {len(effects)} rows; policy permits {max_rows}"
        )
    exact: list[OperationalEffect] = []
    for effect in effects:
        if type(effect) not in {RemovedGid, DeletionConsumption}:
            raise TypeError(
                "effects must contain only RemovedGid or DeletionConsumption"
            )
        assert isinstance(effect, (RemovedGid, DeletionConsumption))
        # A frozen command can still be changed with ``object.__setattr__``.
        # Re-run its exact physical-domain guards before deriving any event
        # identity or binding subtype values.
        effect.__post_init__()
        exact.append(effect)
    return tuple(exact)


def _require_batch_cap(value: object) -> int:
    try:
        return require_int63(value, field="operational max_batch_rows")
    except ValueError as error:
        raise OperationalEffectCorruptionError(
            "operational policy must have a nonnegative bounded batch cap"
        ) from error


def _preparation_id(build_id: bytes, generation: int, policy_id: int) -> bytes:
    payload = build_id + _u64(generation) + _u64(policy_id)
    return _digest(_PREPARATION_ID_DOMAIN, payload)[:16]


def _event_id(preparation_id: bytes, sequence_no: int) -> bytes:
    return _digest(_EVENT_ID_DOMAIN, preparation_id + _u64(sequence_no))[:16]


def _event_type(effect: OperationalEffect) -> str:
    return _REMOVED_TYPE if isinstance(effect, RemovedGid) else _DELETION_TYPE


def _subtype_frame(effect: OperationalEffect) -> bytes:
    if isinstance(effect, RemovedGid):
        token = effect.request_token
    else:
        token = effect.deletion_request_token
    return _u64(effect.gid) + token


def _event_digest(
    preparation_id: bytes, sequence_no: int, effect: OperationalEffect
) -> bytes:
    event_type = _event_type(effect).encode("ascii", errors="strict")
    subtype = _subtype_frame(effect)
    frame = (
        preparation_id
        + _u64(sequence_no)
        + len(event_type).to_bytes(2, "big")
        + event_type
        + len(subtype).to_bytes(4, "big")
        + subtype
    )
    return _digest(_EVENT_DOMAIN, frame)


def _empty_chain() -> bytes:
    return _digest(_EMPTY_CHAIN_DOMAIN, b"")


def _next_chain(prior: bytes, event_sha256: bytes) -> bytes:
    return _digest(
        _CHAIN_LINK_DOMAIN,
        require_digest32(prior, field="prior event chain")
        + require_digest32(event_sha256, field="event digest"),
    )


def _prepare_events(
    preparation_id: bytes,
    start: int,
    prior_chain: bytes,
    effects: Sequence[OperationalEffect],
) -> tuple[tuple[_PreparedEvent, ...], bytes]:
    chain = require_digest32(prior_chain, field="operational prior chain")
    events: list[_PreparedEvent] = []
    for offset, effect in enumerate(effects):
        sequence = start + offset
        digest = _event_digest(preparation_id, sequence, effect)
        event = _PreparedEvent(
            sequence,
            _event_id(preparation_id, sequence),
            _event_type(effect),
            digest,
            effect,
        )
        events.append(event)
        chain = _next_chain(chain, digest)
    return tuple(events), chain


def _effect_request_frame(
    preparation_id: bytes, effects: Sequence[OperationalEffect]
) -> bytes:
    frame = bytearray(preparation_id)
    frame.extend(len(effects).to_bytes(8, "big"))
    for effect in effects:
        event_type = _event_type(effect).encode("ascii", errors="strict")
        subtype = _subtype_frame(effect)
        frame.extend(len(event_type).to_bytes(2, "big"))
        frame.extend(event_type)
        frame.extend(len(subtype).to_bytes(4, "big"))
        frame.extend(subtype)
    return bytes(frame)


def _batch_output_digest(
    preparation_id: bytes,
    *,
    start: int,
    next_sequence: int,
    committed_generation: int,
    events: Sequence[_PreparedEvent],
    terminal: bool,
) -> bytes:
    frame = bytearray(preparation_id)
    frame.extend(_u64(start))
    frame.extend(_u64(next_sequence))
    frame.extend(_u64(committed_generation))
    frame.extend(_u64(len(events)))
    frame.extend(b"\x01" if terminal else b"\x00")
    for event in events:
        frame.extend(event.event_id)
        frame.extend(event.event_sha256)
    return _digest(_BATCH_OUTPUT_DOMAIN, bytes(frame))


def _digest(domain: bytes, payload: bytes) -> bytes:
    return sha256(domain + b"\x00" + payload).digest()


def _u64(value: object) -> bytes:
    return require_int63(value, field="portable unsigned integer").to_bytes(8, "big")


def _encode_cursor(sequence_no: int) -> bytes:
    return _u64(sequence_no)


def _decode_cursor(cursor: bytes) -> int:
    exact = require_bounded_bytes(
        cursor, field="operational event cursor", minimum=8, maximum=8
    )
    return require_int63(int.from_bytes(exact, "big"), field="decoded event cursor")


def _require_seal_row(
    preparation_id: bytes,
    row: tuple[object, ...],
    *,
    replayed: bool,
) -> OperationalEffectSeal:
    if len(row) != 3:
        raise OperationalEffectStateError("operational effect seal is missing")
    return OperationalEffectSeal(
        preparation_id,
        require_int63(row[0], field="sealed event count"),
        require_digest32(row[1], field="sealed final chain"),
        require_int63(row[2], field="effect sealed_at"),
        replayed,
    )
