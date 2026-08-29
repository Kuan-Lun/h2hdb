"""Generation-fenced ingest authority for the greenfield vNext schema.

Every method operates inside a caller-owned :class:`VNextUnitOfWork`.  The
single coordination head is locked first, so a successful return is valid for
the rest of that transaction only; callers must authorize again in every
later transaction.
"""

from __future__ import annotations

__all__ = [
    "IngestFenceCorruptionError",
    "IngestFenceExhaustedError",
    "IngestFenceRepository",
    "IngestFenceUnavailableError",
    "IngestTurn",
]

from dataclasses import dataclass

from .vnext_domains import INT63_MAX, require_int63, require_uuid16
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_GENERATION_TABLE = "operational_ingest_generations"
_HEAD_TABLE = "operational_ingest_coordination_heads"
_OWNER_TABLE = "operational_ingest_generation_owners"


class IngestFenceUnavailableError(RuntimeError):
    """The requested ingest authority is live elsewhere or is stale."""


class IngestFenceCorruptionError(RuntimeError):
    """Persisted ingest authority does not satisfy the normalized contract."""


class IngestFenceExhaustedError(OverflowError):
    """The current ingest generation is the terminal int63 value."""


@dataclass(frozen=True, slots=True)
class IngestTurn:
    generation: int
    owner_token: bytes
    lease_expires_at: int

    def __post_init__(self) -> None:
        require_int63(self.generation, field="ingest generation")
        require_uuid16(self.owner_token, field="ingest owner_token")
        require_int63(self.lease_expires_at, field="ingest lease_expires_at")


@dataclass(frozen=True, slots=True)
class _Head:
    current_generation: int
    completed_generation: int
    phase: str
    last_transition_at: int


@dataclass(frozen=True, slots=True)
class _GenerationState:
    started_at: int
    completed_at: int | None
    owner_token: bytes | None
    lease_expires_at: int | None


class IngestFenceRepository:
    """Claim, renew, authorize, and complete one ingest generation.

    ``owner_token`` is a caller-generated, cryptographically fresh, single-use
    capability, not a general idempotency request ID.  Its generation is the
    durable fence, so response-loss replay of that same generation is the only
    reason to submit the same token again.
    """

    @staticmethod
    def claim(
        work: VNextUnitOfWork,
        *,
        owner_token: bytes,
        now: int,
        lease_duration: int,
        expected_generation: int | None = None,
    ) -> IngestTurn:
        token = require_uuid16(owner_token, field="ingest owner_token")
        timestamp = require_int63(now, field="ingest claim now")
        deadline = _lease_deadline(timestamp, lease_duration, field="ingest lease")
        expected = (
            None
            if expected_generation is None
            else require_int63(expected_generation, field="expected ingest generation")
        )

        head = IngestFenceRepository._lock_head(work)
        if head is None:
            IngestFenceRepository._create_genesis(work, timestamp)
            head = _Head(0, 0, "READY", timestamp)

        state = IngestFenceRepository._lock_generation_state(
            work, head.current_generation
        )
        IngestFenceRepository._validate_head_state(head, state)

        if (
            head.phase == "INGESTING"
            and state.owner_token == token
            and state.lease_expires_at is not None
            and state.lease_expires_at > timestamp
        ):
            if expected is not None and expected != head.current_generation:
                raise IngestFenceUnavailableError(
                    "the live ingest turn does not match expected_generation"
                )
            return IngestTurn(
                head.current_generation,
                token,
                state.lease_expires_at,
            )

        if state.lease_expires_at is not None and state.lease_expires_at > timestamp:
            raise IngestFenceUnavailableError("another ingest owner has a live lease")

        if head.phase != "READY" and state.lease_expires_at is None:
            raise IngestFenceCorruptionError(
                "an active ingest head has no normalized lease authority"
            )
        if head.current_generation == INT63_MAX:
            raise IngestFenceExhaustedError("ingest generation space is exhausted")
        successor = head.current_generation + 1
        if expected is not None and expected != successor:
            raise IngestFenceUnavailableError(
                "the next ingest generation does not match expected_generation"
            )

        if state.owner_token is not None:
            IngestFenceRepository._remove_expired_authority(
                work,
                generation=head.current_generation,
                owner_token=state.owner_token,
                lease_expires_at=state.lease_expires_at,
                now=timestamp,
            )

        work.connector.execute(
            f"INSERT INTO {_GENERATION_TABLE} "
            "(generation, started_at, completed_at) VALUES (%s, %s, NULL)",
            (successor, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_OWNER_TABLE} "
            "(generation, owner_token, claimed_at, lease_expires_at) "
            "VALUES (%s, %s, %s, %s)",
            (successor, token, timestamp, deadline),
        )
        work.compare_and_swap(
            f"UPDATE {_HEAD_TABLE} SET current_generation = %s, phase = %s, "
            "last_transition_at = %s WHERE singleton_id = 1 "
            "AND current_generation = %s AND completed_generation = %s "
            "AND phase = %s AND last_transition_at = %s",
            (
                successor,
                "INGESTING",
                timestamp,
                head.current_generation,
                head.completed_generation,
                head.phase,
                head.last_transition_at,
            ),
            authority="ingest coordination head",
        )
        return IngestTurn(successor, token, deadline)

    @staticmethod
    def renew(
        work: VNextUnitOfWork,
        turn: IngestTurn,
        *,
        now: int,
        lease_duration: int,
    ) -> IngestTurn:
        current = IngestFenceRepository.lock_and_require_live(work, turn, now=now)
        timestamp = require_int63(now, field="ingest renew now")
        deadline = _lease_deadline(timestamp, lease_duration, field="ingest lease")
        if deadline <= current.lease_expires_at:
            return current
        work.compare_and_swap(
            f"UPDATE {_OWNER_TABLE} SET lease_expires_at = %s "
            "WHERE generation = %s AND owner_token = %s AND lease_expires_at = %s",
            (
                deadline,
                current.generation,
                current.owner_token,
                current.lease_expires_at,
            ),
            authority="ingest generation owner lease",
        )
        return IngestTurn(current.generation, current.owner_token, deadline)

    @staticmethod
    def lock_and_require_live(
        work: VNextUnitOfWork,
        turn: IngestTurn,
        *,
        now: int,
    ) -> IngestTurn:
        requested = _require_turn(turn)
        timestamp = require_int63(now, field="ingest authorization now")
        head = IngestFenceRepository._lock_head(work)
        if head is None:
            raise IngestFenceUnavailableError("the ingest head is missing")
        state = IngestFenceRepository._lock_generation_state(
            work, head.current_generation
        )
        IngestFenceRepository._validate_head_state(head, state)
        if (
            head.phase != "INGESTING"
            or head.current_generation != requested.generation
            or state.owner_token != requested.owner_token
            or state.lease_expires_at != requested.lease_expires_at
            or state.lease_expires_at is None
            or state.lease_expires_at <= timestamp
        ):
            raise IngestFenceUnavailableError("the ingest turn is stale or expired")
        return requested

    @staticmethod
    def lock_and_require_quiescent(work: VNextUnitOfWork) -> int | None:
        """Lock the ingest fence and require no active generation authority.

        ``None`` is the valid never-initialized state.  The first later ingest
        claim creates its real completed generation-zero history in the same
        transaction.
        """

        head = IngestFenceRepository._lock_head(work)
        if head is None:
            return None
        state = IngestFenceRepository._lock_generation_state(
            work, head.current_generation
        )
        IngestFenceRepository._validate_head_state(head, state)
        if head.phase != "READY":
            raise IngestFenceUnavailableError("the ingest fence is not quiescent")
        return head.current_generation

    @staticmethod
    def complete(
        work: VNextUnitOfWork,
        turn: IngestTurn,
        *,
        now: int,
    ) -> None:
        current = IngestFenceRepository.lock_and_require_live(work, turn, now=now)
        timestamp = require_int63(now, field="ingest completion now")
        generation_row = work.connector.fetch_one(
            f"SELECT started_at FROM {_GENERATION_TABLE} WHERE generation = %s",
            (current.generation,),
        )
        if len(generation_row) != 1 or timestamp < require_int63(
            generation_row[0], field="ingest started_at"
        ):
            raise IngestFenceCorruptionError(
                "ingest completion precedes or cannot find generation start"
            )
        work.compare_and_swap(
            f"UPDATE {_GENERATION_TABLE} SET completed_at = %s "
            "WHERE generation = %s AND completed_at IS NULL",
            (timestamp, current.generation),
            authority="ingest generation completion",
        )
        work.compare_and_swap(
            f"UPDATE {_HEAD_TABLE} SET completed_generation = %s, phase = %s, "
            "last_transition_at = %s WHERE singleton_id = 1 "
            "AND current_generation = %s AND completed_generation < %s "
            "AND phase = %s",
            (
                current.generation,
                "READY",
                timestamp,
                current.generation,
                current.generation,
                "INGESTING",
            ),
            authority="ingest coordination completion",
        )
        _delete_exactly_one(
            work,
            f"DELETE FROM {_OWNER_TABLE} WHERE generation = %s AND owner_token = %s "
            "AND lease_expires_at = %s",
            (current.generation, current.owner_token, current.lease_expires_at),
            authority="completed ingest authority",
        )

    @staticmethod
    def _lock_head(work: VNextUnitOfWork) -> _Head | None:
        row = work.lock_row(
            LockRank.INGEST_FENCE,
            encode_lock_key("ingest", 0),
            f"SELECT current_generation, completed_generation, phase, "
            f"last_transition_at FROM {_HEAD_TABLE} WHERE singleton_id = 1",
        )
        if not row:
            return None
        if len(row) != 4 or not isinstance(row[2], str):
            raise IngestFenceCorruptionError("the ingest head has an invalid shape")
        return _Head(
            require_int63(row[0], field="current ingest generation"),
            require_int63(row[1], field="completed ingest generation"),
            row[2],
            require_int63(row[3], field="ingest last_transition_at"),
        )

    @staticmethod
    def _create_genesis(work: VNextUnitOfWork, now: int) -> None:
        work.connector.execute(
            f"INSERT INTO {_GENERATION_TABLE} "
            "(generation, started_at, completed_at) VALUES (0, %s, %s)",
            (now, now),
        )
        work.connector.execute(
            f"INSERT INTO {_HEAD_TABLE} "
            "(singleton_id, current_generation, completed_generation, phase, "
            "last_transition_at) VALUES (1, 0, 0, %s, %s)",
            ("READY", now),
        )

    @staticmethod
    def _lock_generation_state(
        work: VNextUnitOfWork, generation: int
    ) -> _GenerationState:
        generation_row = work.lock_row(
            LockRank.INGEST_FENCE,
            encode_lock_key("ingest", 1, generation),
            f"SELECT started_at, completed_at FROM {_GENERATION_TABLE} "
            "WHERE generation = %s",
            (generation,),
        )
        if len(generation_row) != 2:
            raise IngestFenceCorruptionError("the current ingest generation is missing")
        started_at = require_int63(generation_row[0], field="ingest started_at")
        completed_at = (
            None
            if generation_row[1] is None
            else require_int63(generation_row[1], field="ingest completed_at")
        )
        owner_row = work.lock_row(
            LockRank.INGEST_FENCE,
            encode_lock_key("ingest", 2, generation),
            f"SELECT owner_token, lease_expires_at FROM {_OWNER_TABLE} "
            "WHERE generation = %s",
            (generation,),
        )
        if owner_row and len(owner_row) != 2:
            raise IngestFenceCorruptionError("the ingest owner has an invalid shape")
        owner = (
            None
            if not owner_row
            else require_uuid16(owner_row[0], field="persisted ingest owner_token")
        )
        lease = (
            None
            if not owner_row
            else require_int63(owner_row[1], field="persisted ingest lease_expires_at")
        )
        return _GenerationState(started_at, completed_at, owner, lease)

    @staticmethod
    def _validate_head_state(head: _Head, state: _GenerationState) -> None:
        if head.completed_generation > head.current_generation:
            raise IngestFenceCorruptionError("completed ingest generation is ahead")
        if head.phase == "READY":
            if (
                head.current_generation != head.completed_generation
                or state.completed_at is None
                or state.owner_token is not None
                or state.lease_expires_at is not None
            ):
                raise IngestFenceCorruptionError("READY ingest head is not quiescent")
            return
        if head.phase != "INGESTING":
            raise IngestFenceCorruptionError(
                f"unsupported ingest coordination phase {head.phase!r}"
            )
        if (
            head.current_generation <= head.completed_generation
            or state.completed_at is not None
            or state.owner_token is None
            or state.lease_expires_at is None
        ):
            raise IngestFenceCorruptionError("INGESTING head has incomplete authority")

    @staticmethod
    def _remove_expired_authority(
        work: VNextUnitOfWork,
        *,
        generation: int,
        owner_token: bytes,
        lease_expires_at: int | None,
        now: int,
    ) -> None:
        if lease_expires_at is None or lease_expires_at > now:
            raise IngestFenceUnavailableError("ingest authority is not expired")
        _delete_exactly_one(
            work,
            f"DELETE FROM {_OWNER_TABLE} WHERE generation = %s AND owner_token = %s "
            "AND lease_expires_at = %s",
            (generation, owner_token, lease_expires_at),
            authority="expired ingest authority",
        )


def _require_turn(turn: IngestTurn) -> IngestTurn:
    if not isinstance(turn, IngestTurn):
        raise TypeError("turn must be an IngestTurn")
    # Frozen dataclasses can still be forged through object.__setattr__.
    require_int63(turn.generation, field="ingest generation")
    require_uuid16(turn.owner_token, field="ingest owner_token")
    require_int63(turn.lease_expires_at, field="ingest lease_expires_at")
    return turn


def _lease_deadline(now: int, duration: int, *, field: str) -> int:
    interval = require_int63(duration, field=f"{field} duration")
    if interval > INT63_MAX - now:
        raise OverflowError(f"{field} deadline exceeds int63")
    return now + interval


def _delete_exactly_one(
    work: VNextUnitOfWork,
    query: str,
    data: tuple[object, ...],
    *,
    authority: str,
) -> None:
    affected = work.connector.execute_affected(query, data)
    if affected != 1:
        raise IngestFenceCorruptionError(
            f"{authority} deletion affected {affected} rows instead of 1"
        )
