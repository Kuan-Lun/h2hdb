"""Normalized downloader-to-ingest coordination for the greenfield schema.

Every method runs inside a caller-owned :class:`VNextUnitOfWork`.  Download
authority is always locked before the existing ingest fence.  The repository
owns both capability sources and persists handoff, one-to-one consumption, and
completion receipts so retry authority never comes from caller-supplied phase
or digest data.
"""

from __future__ import annotations

__all__ = [
    "CoordinatedIngestCompletion",
    "CoordinatedIngestTurn",
    "DownloadCapabilityCollisionError",
    "DownloadGenerationExhaustedError",
    "DownloadHandoff",
    "DownloadIngestCorruptionError",
    "DownloadIngestReplayMismatchError",
    "DownloadIngestRepository",
    "DownloadIngestUnavailableError",
    "DownloadTurn",
    "HandoffKind",
]

import secrets
from dataclasses import dataclass
from enum import StrEnum

from .vnext_domains import INT63_MAX, require_int63, require_uuid16
from .vnext_ingest_fence_repository import (
    IngestFenceCorruptionError,
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_DOWNLOAD_GENERATION_TABLE = "operational_download_generations"
_DOWNLOAD_HEAD_TABLE = "operational_download_coordination_heads"
_DOWNLOAD_OWNER_TABLE = "operational_download_generation_owners"
_DOWNLOAD_LEASE_TABLE = "operational_download_generation_leases"
_HANDOFF_TABLE = "operational_download_ingest_handoffs"
_CONSUMPTION_TABLE = "operational_download_ingest_consumptions"
_COMPLETION_TABLE = "operational_coordinated_ingest_completions"

_INGEST_GENERATION_TABLE = "operational_ingest_generations"
_INGEST_HEAD_TABLE = "operational_ingest_coordination_heads"
_INGEST_OWNER_TABLE = "operational_ingest_generation_owners"
_INGEST_LEASE_TABLE = "operational_ingest_generation_leases"


DownloadIngestUnavailableError = IngestFenceUnavailableError
DownloadIngestCorruptionError = IngestFenceCorruptionError


class DownloadCapabilityCollisionError(RuntimeError):
    """A generated capability already exists in durable authority history."""


class DownloadGenerationExhaustedError(OverflowError):
    """The download generation has reached the portable int63 maximum."""


class DownloadIngestReplayMismatchError(RuntimeError):
    """Stored response authority differs from the complete presented tuple."""


class HandoffKind(StrEnum):
    DOWNLOADER = "DOWNLOADER"
    EXPIRED_TAKEOVER = "EXPIRED_TAKEOVER"


@dataclass(frozen=True, slots=True)
class DownloadTurn:
    generation: int
    owner_token: bytes
    lease_expires_at: int

    def __post_init__(self) -> None:
        require_int63(self.generation, field="download generation")
        require_uuid16(self.owner_token, field="download owner_token")
        require_int63(self.lease_expires_at, field="download lease_expires_at")


@dataclass(frozen=True, slots=True)
class DownloadHandoff:
    download_generation: int
    owner_token: bytes
    handoff_kind: HandoffKind
    requested_at: int

    def __post_init__(self) -> None:
        require_int63(self.download_generation, field="handoff download generation")
        require_uuid16(self.owner_token, field="handoff owner_token")
        if not isinstance(self.handoff_kind, HandoffKind):
            raise TypeError("handoff_kind must be a HandoffKind")
        require_int63(self.requested_at, field="handoff requested_at")


@dataclass(frozen=True, slots=True)
class CoordinatedIngestTurn:
    ingest_turn: IngestTurn
    download_generation: int | None
    handoff_owner_token: bytes | None
    handoff_kind: HandoffKind | None
    consumed_at: int | None

    def __post_init__(self) -> None:
        _require_ingest_turn(self.ingest_turn)
        linked = self.download_generation is not None
        if linked != all(
            value is not None
            for value in (
                self.handoff_owner_token,
                self.handoff_kind,
                self.consumed_at,
            )
        ):
            raise ValueError(
                "linked coordinated ingest fields must be all present or all absent"
            )
        if linked:
            require_int63(
                self.download_generation, field="coordinated download generation"
            )
            require_uuid16(
                self.handoff_owner_token, field="coordinated handoff owner_token"
            )
            if not isinstance(self.handoff_kind, HandoffKind):
                raise TypeError("handoff_kind must be a HandoffKind")
            require_int63(self.consumed_at, field="coordinated consumed_at")

    @property
    def is_periodic(self) -> bool:
        return self.download_generation is None


@dataclass(frozen=True, slots=True)
class CoordinatedIngestCompletion:
    ingest_generation: int
    owner_token: bytes
    completed_at: int
    download_generation: int | None

    def __post_init__(self) -> None:
        require_int63(self.ingest_generation, field="completed ingest generation")
        require_uuid16(self.owner_token, field="completed ingest owner_token")
        require_int63(self.completed_at, field="coordinated completed_at")
        if self.download_generation is not None:
            require_int63(
                self.download_generation, field="completed download generation"
            )


@dataclass(frozen=True, slots=True)
class _DownloadHead:
    current_generation: int
    completed_generation: int
    last_transition_at: int


@dataclass(frozen=True, slots=True)
class _DownloadState:
    generation: int
    started_at: int
    completed_at: int | None
    owner_token: bytes | None
    claimed_at: int | None
    lease_expires_at: int | None
    handoff_owner_token: bytes | None
    handoff_kind: HandoffKind | None
    requested_at: int | None
    ingest_generation: int | None
    consumed_at: int | None


@dataclass(frozen=True, slots=True)
class _DownloadHandoffTransition:
    handoff: DownloadHandoff
    created: bool


class DownloadIngestRepository:
    """Orchestrate normalized download authority and the existing ingest fence."""

    @staticmethod
    def claim_download(
        work: VNextUnitOfWork,
        *,
        now: int,
        lease_duration: int,
    ) -> DownloadTurn:
        timestamp = require_int63(now, field="download claim now")
        deadline = _lease_deadline(timestamp, lease_duration, field="download lease")
        token = require_uuid16(
            _new_download_owner_token(), field="generated download owner_token"
        )
        head = _lock_or_create_download_head(work, timestamp)
        state = _lock_download_state(work, head.current_generation)
        _validate_current_download_state(head, state)
        if head.current_generation != head.completed_generation:
            raise DownloadIngestUnavailableError(
                "the current download generation still awaits linked ingest completion"
            )
        if head.current_generation == INT63_MAX:
            raise DownloadGenerationExhaustedError(
                "download generation space is exhausted"
            )
        _require_fresh_download_token(work, token)
        IngestFenceRepository.lock_and_require_quiescent(work)

        successor = head.current_generation + 1
        work.connector.execute(
            f"INSERT INTO {_DOWNLOAD_GENERATION_TABLE} "
            "(generation, started_at, completed_at) VALUES (%s, %s, NULL)",
            (successor, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_DOWNLOAD_OWNER_TABLE} "
            "(generation, owner_token, claimed_at) VALUES (%s, %s, %s)",
            (successor, token, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_DOWNLOAD_LEASE_TABLE} "
            "(generation, lease_expires_at) VALUES (%s, %s)",
            (successor, deadline),
        )
        work.compare_and_swap(
            f"UPDATE {_DOWNLOAD_HEAD_TABLE} SET current_generation = %s, "
            "last_transition_at = %s WHERE singleton_id = 1 "
            "AND current_generation = %s AND completed_generation = %s "
            "AND last_transition_at = %s",
            (
                successor,
                timestamp,
                head.current_generation,
                head.completed_generation,
                head.last_transition_at,
            ),
            authority="download coordination head",
        )
        return DownloadTurn(successor, token, deadline)

    @staticmethod
    def resume_download(
        work: VNextUnitOfWork,
        turn: DownloadTurn,
        *,
        now: int,
    ) -> DownloadTurn:
        requested = _require_download_turn(turn)
        timestamp = require_int63(now, field="download resume now")
        head = _require_download_head(work)
        state = _lock_download_state(work, requested.generation)
        _require_live_download_turn(head, state, requested, now=timestamp)
        return requested

    @staticmethod
    def renew_download(
        work: VNextUnitOfWork,
        turn: DownloadTurn,
        *,
        now: int,
        lease_duration: int,
    ) -> DownloadTurn:
        requested = _require_download_turn(turn)
        timestamp = require_int63(now, field="download renew now")
        deadline = _lease_deadline(timestamp, lease_duration, field="download lease")
        head = _require_download_head(work)
        state = _lock_download_state(work, requested.generation)
        _require_live_download_turn(head, state, requested, now=timestamp)
        if deadline <= requested.lease_expires_at:
            return requested
        work.compare_and_swap(
            f"UPDATE {_DOWNLOAD_LEASE_TABLE} SET lease_expires_at = %s "
            "WHERE generation = %s AND lease_expires_at = %s",
            (deadline, requested.generation, requested.lease_expires_at),
            authority="download generation lease",
        )
        return DownloadTurn(requested.generation, requested.owner_token, deadline)

    @staticmethod
    def handoff_download(
        work: VNextUnitOfWork,
        turn: DownloadTurn,
        *,
        now: int,
        recover_existing: bool = False,
    ) -> DownloadHandoff:
        return _transition_download_handoff(
            work,
            turn,
            now=now,
            recover_existing=recover_existing,
        ).handoff

    @staticmethod
    def ensure_download_handoff(
        work: VNextUnitOfWork,
        turn: DownloadTurn,
        *,
        now: int,
    ) -> _DownloadHandoffTransition:
        """Create a handoff or recover its exact durable response."""

        return _transition_download_handoff(
            work,
            turn,
            now=now,
            recover_existing=True,
        )

    @staticmethod
    def is_download_handoff_complete(
        work: VNextUnitOfWork,
        handoff: DownloadHandoff,
    ) -> bool:
        """Return whether linked ingest durably completed one exact handoff."""

        requested = _require_download_handoff(handoff)
        handoff_row = work.connector.fetch_one(
            f"SELECT owner_token, handoff_kind, requested_at FROM {_HANDOFF_TABLE} "
            "WHERE download_generation = %s",
            (requested.download_generation,),
        )
        if not handoff_row:
            raise DownloadIngestReplayMismatchError(
                "download handoff receipt has no durable authority"
            )
        durable = _download_handoff_from_row(
            requested.download_generation,
            handoff_row,
        )
        if durable != requested:
            raise DownloadIngestReplayMismatchError(
                "stored download handoff differs from the complete receipt"
            )

        row = work.connector.fetch_one(
            f"SELECT consumption.ingest_generation, completion.owner_token, "
            "completion.completed_at, generation.completed_at, "
            f"head.completed_generation FROM {_DOWNLOAD_GENERATION_TABLE} "
            f"AS generation JOIN {_DOWNLOAD_HEAD_TABLE} AS head "
            "ON head.singleton_id = 1 "
            f"LEFT JOIN {_CONSUMPTION_TABLE} AS consumption "
            "ON consumption.download_generation = generation.generation "
            f"LEFT JOIN {_COMPLETION_TABLE} AS completion "
            "ON completion.ingest_generation = consumption.ingest_generation "
            "WHERE generation.generation = %s",
            (requested.download_generation,),
        )
        if len(row) != 5:
            raise DownloadIngestCorruptionError(
                "download handoff completion state is missing or malformed"
            )
        try:
            ingest_generation = (
                None
                if row[0] is None
                else require_int63(row[0], field="consumed ingest generation")
            )
            completion_owner = (
                None
                if row[1] is None
                else require_uuid16(row[1], field="completion owner_token")
            )
            completion_at = (
                None
                if row[2] is None
                else require_int63(row[2], field="coordinated completed_at")
            )
            download_completed_at = (
                None
                if row[3] is None
                else require_int63(row[3], field="download completed_at")
            )
            completed_generation = require_int63(
                row[4],
                field="completed download generation",
            )
        except (TypeError, ValueError) as error:
            raise DownloadIngestCorruptionError(
                "download handoff completion state violates its physical domain"
            ) from error
        if (completion_owner is None) != (completion_at is None):
            raise DownloadIngestCorruptionError(
                "coordinated ingest completion receipt is incomplete"
            )
        if ingest_generation is None:
            if completion_owner is not None:
                raise DownloadIngestCorruptionError(
                    "download completion exists without an ingest consumption"
                )
            if (
                download_completed_at is not None
                or completed_generation >= requested.download_generation
            ):
                raise DownloadIngestCorruptionError(
                    "unconsumed download handoff is already marked complete"
                )
            return False
        if completion_owner is None:
            if (
                download_completed_at is not None
                or completed_generation >= requested.download_generation
            ):
                raise DownloadIngestCorruptionError(
                    "incomplete linked ingest already advanced download completion"
                )
            return False
        if (
            download_completed_at != completion_at
            or completed_generation < requested.download_generation
        ):
            raise DownloadIngestCorruptionError(
                "linked ingest completion disagrees with download authority"
            )
        return True

    @staticmethod
    def claim_ingest(
        work: VNextUnitOfWork,
        *,
        now: int,
        lease_duration: int,
        periodic: bool = False,
    ) -> CoordinatedIngestTurn:
        if not isinstance(periodic, bool):
            raise TypeError("periodic must be bool")
        timestamp = require_int63(now, field="coordinated ingest claim now")
        require_int63(lease_duration, field="coordinated ingest lease duration")
        ingest_token = require_uuid16(
            _new_ingest_owner_token(), field="generated ingest owner_token"
        )
        head = _lock_or_create_download_head(work, timestamp)
        state = _lock_download_state(work, head.current_generation)
        _validate_current_download_state(head, state)

        handoff: DownloadHandoff | None
        if periodic:
            if head.current_generation != head.completed_generation:
                raise DownloadIngestUnavailableError(
                    "periodic ingest requires quiescent download authority"
                )
            handoff = None
        else:
            if head.current_generation == head.completed_generation:
                raise DownloadIngestUnavailableError(
                    "there is no pending download handoff to consume"
                )
            if state.ingest_generation is not None:
                raise DownloadIngestUnavailableError(
                    "the download handoff was already consumed"
                )
            if state.owner_token is not None:
                if state.lease_expires_at is None:
                    raise DownloadIngestCorruptionError(
                        "download owner lacks its normalized lease"
                    )
                if state.lease_expires_at > timestamp:
                    raise DownloadIngestUnavailableError(
                        "the downloader still has a live lease"
                    )
                handoff = _take_over_expired_download(work, state, now=timestamp)
            else:
                handoff = _handoff_from_state(state)

        ingest_turn = IngestFenceRepository.claim(
            work,
            owner_token=ingest_token,
            now=timestamp,
            lease_duration=lease_duration,
        )
        _require_fresh_ingest_completion_token(work, ingest_token)
        if handoff is None:
            return CoordinatedIngestTurn(ingest_turn, None, None, None, None)

        work.connector.execute(
            f"INSERT INTO {_CONSUMPTION_TABLE} "
            "(download_generation, ingest_generation, consumed_at) "
            "VALUES (%s, %s, %s)",
            (handoff.download_generation, ingest_turn.generation, timestamp),
        )
        return CoordinatedIngestTurn(
            ingest_turn,
            handoff.download_generation,
            handoff.owner_token,
            handoff.handoff_kind,
            timestamp,
        )

    @staticmethod
    def resume_ingest(
        work: VNextUnitOfWork,
        turn: CoordinatedIngestTurn,
        *,
        now: int,
    ) -> CoordinatedIngestTurn:
        requested = _require_coordinated_turn(turn)
        timestamp = require_int63(now, field="coordinated ingest resume now")
        _lock_and_validate_active_relationship(work, requested)
        IngestFenceRepository.lock_and_require_live(
            work, requested.ingest_turn, now=timestamp
        )
        return requested

    @staticmethod
    def renew_ingest(
        work: VNextUnitOfWork,
        turn: CoordinatedIngestTurn,
        *,
        now: int,
        lease_duration: int,
    ) -> CoordinatedIngestTurn:
        requested = _require_coordinated_turn(turn)
        timestamp = require_int63(now, field="coordinated ingest renew now")
        _lock_and_validate_active_relationship(work, requested)
        renewed = IngestFenceRepository.renew(
            work,
            requested.ingest_turn,
            now=timestamp,
            lease_duration=lease_duration,
        )
        return CoordinatedIngestTurn(
            renewed,
            requested.download_generation,
            requested.handoff_owner_token,
            requested.handoff_kind,
            requested.consumed_at,
        )

    @staticmethod
    def complete_ingest(
        work: VNextUnitOfWork,
        turn: CoordinatedIngestTurn,
        *,
        now: int,
    ) -> CoordinatedIngestCompletion:
        requested = _require_coordinated_turn(turn)
        timestamp = require_int63(now, field="coordinated ingest completion now")
        replay = DownloadIngestRepository.get_ingest_completion(work, requested)
        if replay is not None:
            return replay
        expected = CoordinatedIngestCompletion(
            requested.ingest_turn.generation,
            requested.ingest_turn.owner_token,
            timestamp,
            requested.download_generation,
        )
        _lock_and_validate_active_relationship(work, requested)
        if work.connector.fetch_one(
            f"SELECT owner_token, completed_at FROM {_COMPLETION_TABLE} "
            "WHERE ingest_generation = %s",
            (requested.ingest_turn.generation,),
        ):
            raise DownloadIngestCorruptionError(
                "live ingest generation already has a completion receipt"
            )
        IngestFenceRepository.complete(work, requested.ingest_turn, now=timestamp)
        work.connector.execute(
            f"INSERT INTO {_COMPLETION_TABLE} "
            "(ingest_generation, owner_token, completed_at) VALUES (%s, %s, %s)",
            (
                expected.ingest_generation,
                expected.owner_token,
                expected.completed_at,
            ),
        )
        if requested.download_generation is None:
            return expected

        work.compare_and_swap(
            f"UPDATE {_DOWNLOAD_GENERATION_TABLE} SET completed_at = %s "
            "WHERE generation = %s AND completed_at IS NULL",
            (timestamp, requested.download_generation),
            authority="linked download generation completion",
        )
        work.compare_and_swap(
            f"UPDATE {_DOWNLOAD_HEAD_TABLE} SET completed_generation = %s, "
            "last_transition_at = %s WHERE singleton_id = 1 "
            "AND current_generation = %s AND completed_generation < %s",
            (
                requested.download_generation,
                timestamp,
                requested.download_generation,
                requested.download_generation,
            ),
            authority="linked download coordination completion",
        )
        return expected

    @staticmethod
    def get_ingest_completion(
        work: VNextUnitOfWork,
        turn: CoordinatedIngestTurn,
    ) -> CoordinatedIngestCompletion | None:
        """Return the canonical stored completion for an exact turn, if any.

        The stored timestamp is authority.  This makes a completion retry
        independent of the retrying process's clock while still validating the
        complete linked-download relationship and ingest history.
        """

        requested = _require_coordinated_turn(turn)
        hint = work.connector.fetch_one(
            f"SELECT owner_token, completed_at FROM {_COMPLETION_TABLE} "
            "WHERE ingest_generation = %s",
            (requested.ingest_turn.generation,),
        )
        if not hint:
            return None
        if len(hint) != 2:
            raise DownloadIngestCorruptionError(
                "coordinated ingest completion has an invalid shape"
            )
        completion = CoordinatedIngestCompletion(
            requested.ingest_turn.generation,
            require_uuid16(hint[0], field="completion receipt owner_token"),
            require_int63(hint[1], field="completion receipt completed_at"),
            requested.download_generation,
        )
        if completion.owner_token != requested.ingest_turn.owner_token:
            raise DownloadIngestReplayMismatchError(
                "stored coordinated completion belongs to another owner"
            )
        _lock_and_validate_completion_relationship(
            work,
            requested,
            completion.completed_at,
        )
        _lock_and_validate_completed_ingest(work, completion)
        return completion


def _transition_download_handoff(
    work: VNextUnitOfWork,
    turn: DownloadTurn,
    *,
    now: int,
    recover_existing: bool,
) -> _DownloadHandoffTransition:
    requested = _require_download_turn(turn)
    if not isinstance(recover_existing, bool):
        raise TypeError("recover_existing must be bool")
    timestamp = require_int63(now, field="download handoff now")
    head = _require_download_head(work)
    state = _lock_download_state(work, requested.generation)
    if state.handoff_kind is not None:
        if recover_existing:
            actual = _handoff_from_state(state)
            if (
                actual.owner_token != requested.owner_token
                or actual.handoff_kind is not HandoffKind.DOWNLOADER
            ):
                raise DownloadIngestReplayMismatchError(
                    "stored download handoff differs from the presented turn capability"
                )
            return _DownloadHandoffTransition(actual, False)
        expected = DownloadHandoff(
            requested.generation,
            requested.owner_token,
            HandoffKind.DOWNLOADER,
            timestamp,
        )
        _require_exact_handoff(state, expected)
        if state.owner_token is not None or state.lease_expires_at is not None:
            raise DownloadIngestCorruptionError(
                "durable handoff retained mutable downloader authority"
            )
        return _DownloadHandoffTransition(expected, False)

    _require_live_download_turn(head, state, requested, now=timestamp)
    if timestamp < state.started_at:
        raise DownloadIngestCorruptionError(
            "download handoff precedes generation start"
        )
    result = DownloadHandoff(
        requested.generation,
        requested.owner_token,
        HandoffKind.DOWNLOADER,
        timestamp,
    )
    work.connector.execute(
        f"INSERT INTO {_HANDOFF_TABLE} "
        "(download_generation, owner_token, handoff_kind, requested_at) "
        "VALUES (%s, %s, %s, %s)",
        (
            result.download_generation,
            result.owner_token,
            result.handoff_kind.value,
            result.requested_at,
        ),
    )
    _delete_exactly_one(
        work,
        f"DELETE FROM {_DOWNLOAD_LEASE_TABLE} WHERE generation = %s "
        "AND lease_expires_at = %s",
        (requested.generation, requested.lease_expires_at),
        authority="handed-off download lease",
    )
    _delete_exactly_one(
        work,
        f"DELETE FROM {_DOWNLOAD_OWNER_TABLE} WHERE generation = %s "
        "AND owner_token = %s",
        (requested.generation, requested.owner_token),
        authority="handed-off download owner",
    )
    return _DownloadHandoffTransition(result, True)


def _lock_download_head(work: VNextUnitOfWork) -> _DownloadHead | None:
    row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 0),
        f"SELECT current_generation, completed_generation, last_transition_at "
        f"FROM {_DOWNLOAD_HEAD_TABLE} WHERE singleton_id = 1",
    )
    if not row:
        return None
    if len(row) != 3:
        raise DownloadIngestCorruptionError("the download head has an invalid shape")
    return _DownloadHead(
        require_int63(row[0], field="current download generation"),
        require_int63(row[1], field="completed download generation"),
        require_int63(row[2], field="download last_transition_at"),
    )


def _require_download_head(work: VNextUnitOfWork) -> _DownloadHead:
    head = _lock_download_head(work)
    if head is None:
        raise DownloadIngestUnavailableError(
            "the download coordination head is missing"
        )
    return head


def _lock_or_create_download_head(work: VNextUnitOfWork, now: int) -> _DownloadHead:
    head = _lock_download_head(work)
    if head is not None:
        return head
    work.connector.execute(
        f"INSERT INTO {_DOWNLOAD_GENERATION_TABLE} "
        "(generation, started_at, completed_at) VALUES (0, %s, %s)",
        (now, now),
    )
    work.connector.execute(
        f"INSERT INTO {_DOWNLOAD_HEAD_TABLE} "
        "(singleton_id, current_generation, completed_generation, "
        "last_transition_at) VALUES (1, 0, 0, %s)",
        (now,),
    )
    return _DownloadHead(0, 0, now)


def _lock_download_state(work: VNextUnitOfWork, generation: int) -> _DownloadState:
    current = require_int63(generation, field="download state generation")
    generation_row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 1, current),
        f"SELECT started_at, completed_at FROM {_DOWNLOAD_GENERATION_TABLE} "
        "WHERE generation = %s",
        (current,),
    )
    if len(generation_row) != 2:
        raise DownloadIngestCorruptionError("download generation history is missing")
    started_at = require_int63(generation_row[0], field="download started_at")
    completed_at = (
        None
        if generation_row[1] is None
        else require_int63(generation_row[1], field="download completed_at")
    )

    owner_row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 2, current),
        f"SELECT owner_token, claimed_at FROM {_DOWNLOAD_OWNER_TABLE} "
        "WHERE generation = %s",
        (current,),
    )
    if owner_row and len(owner_row) != 2:
        raise DownloadIngestCorruptionError("download owner has an invalid shape")
    owner_token = (
        None
        if not owner_row
        else require_uuid16(owner_row[0], field="persisted download owner_token")
    )
    claimed_at = (
        None
        if not owner_row
        else require_int63(owner_row[1], field="download claimed_at")
    )

    lease_row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 3, current),
        f"SELECT lease_expires_at FROM {_DOWNLOAD_LEASE_TABLE} WHERE generation = %s",
        (current,),
    )
    if lease_row and len(lease_row) != 1:
        raise DownloadIngestCorruptionError("download lease has an invalid shape")
    lease_expires_at = (
        None
        if not lease_row
        else require_int63(lease_row[0], field="download lease_expires_at")
    )

    handoff_row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 4, current),
        f"SELECT owner_token, handoff_kind, requested_at FROM {_HANDOFF_TABLE} "
        "WHERE download_generation = %s",
        (current,),
    )
    if handoff_row and len(handoff_row) != 3:
        raise DownloadIngestCorruptionError("download handoff has an invalid shape")
    if handoff_row:
        handoff_owner_token = require_uuid16(
            handoff_row[0], field="persisted handoff owner_token"
        )
        if not isinstance(handoff_row[1], str):
            raise DownloadIngestCorruptionError("download handoff kind is not text")
        try:
            handoff_kind = HandoffKind(handoff_row[1])
        except ValueError as error:
            raise DownloadIngestCorruptionError(
                "download handoff kind is not registered"
            ) from error
        requested_at = require_int63(
            handoff_row[2], field="persisted handoff requested_at"
        )
    else:
        handoff_owner_token = None
        handoff_kind = None
        requested_at = None

    consumption_row = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 5, current),
        f"SELECT ingest_generation, consumed_at FROM {_CONSUMPTION_TABLE} "
        "WHERE download_generation = %s",
        (current,),
    )
    if consumption_row and len(consumption_row) != 2:
        raise DownloadIngestCorruptionError("download consumption has an invalid shape")
    ingest_generation = (
        None
        if not consumption_row
        else require_int63(consumption_row[0], field="consumed ingest generation")
    )
    consumed_at = (
        None
        if not consumption_row
        else require_int63(consumption_row[1], field="persisted consumed_at")
    )
    return _DownloadState(
        current,
        started_at,
        completed_at,
        owner_token,
        claimed_at,
        lease_expires_at,
        handoff_owner_token,
        handoff_kind,
        requested_at,
        ingest_generation,
        consumed_at,
    )


def _validate_current_download_state(
    head: _DownloadHead, state: _DownloadState
) -> None:
    if state.generation != head.current_generation:
        raise DownloadIngestCorruptionError("download state does not match its head")
    if head.completed_generation > head.current_generation:
        raise DownloadIngestCorruptionError("completed download generation is ahead")
    if (state.owner_token is None) != (state.lease_expires_at is None):
        raise DownloadIngestCorruptionError(
            "download owner and lease satellites disagree"
        )
    if (state.handoff_kind is None) != (state.handoff_owner_token is None):
        raise DownloadIngestCorruptionError("download handoff tuple is incomplete")
    if (state.handoff_kind is None) != (state.requested_at is None):
        raise DownloadIngestCorruptionError("download handoff time is incomplete")
    if (state.ingest_generation is None) != (state.consumed_at is None):
        raise DownloadIngestCorruptionError("download consumption tuple is incomplete")
    if state.ingest_generation is not None and state.handoff_kind is None:
        raise DownloadIngestCorruptionError("download consumption lacks a handoff")

    if head.current_generation == head.completed_generation:
        if state.completed_at is None or state.owner_token is not None:
            raise DownloadIngestCorruptionError(
                "quiescent download head retains live authority"
            )
        if (state.handoff_kind is None) != (state.ingest_generation is None):
            raise DownloadIngestCorruptionError(
                "completed linked download history is incomplete"
            )
        return
    if state.completed_at is not None:
        raise DownloadIngestCorruptionError(
            "pending download generation is already marked complete"
        )
    live_owner = state.owner_token is not None
    transferred = state.handoff_kind is not None
    if live_owner == transferred:
        raise DownloadIngestCorruptionError(
            "pending download must have exactly one owner or handoff authority"
        )


def _require_live_download_turn(
    head: _DownloadHead,
    state: _DownloadState,
    turn: DownloadTurn,
    *,
    now: int,
) -> None:
    if state.generation == head.current_generation:
        _validate_current_download_state(head, state)
    if (
        head.current_generation != turn.generation
        or head.completed_generation >= turn.generation
        or state.completed_at is not None
        or state.owner_token != turn.owner_token
        or state.lease_expires_at != turn.lease_expires_at
        or state.lease_expires_at is None
        or state.lease_expires_at <= now
        or state.handoff_kind is not None
        or state.ingest_generation is not None
    ):
        raise DownloadIngestUnavailableError("the download turn is stale or expired")


def _require_exact_handoff(state: _DownloadState, expected: DownloadHandoff) -> None:
    actual = (
        state.generation,
        state.handoff_owner_token,
        state.handoff_kind,
        state.requested_at,
    )
    wanted = (
        expected.download_generation,
        expected.owner_token,
        expected.handoff_kind,
        expected.requested_at,
    )
    if actual != wanted:
        raise DownloadIngestReplayMismatchError(
            "stored download handoff differs from the complete replay tuple"
        )


def _handoff_from_state(state: _DownloadState) -> DownloadHandoff:
    if (
        state.handoff_owner_token is None
        or state.handoff_kind is None
        or state.requested_at is None
        or state.owner_token is not None
        or state.lease_expires_at is not None
    ):
        raise DownloadIngestCorruptionError(
            "pending download lacks exact transferred handoff authority"
        )
    return DownloadHandoff(
        state.generation,
        state.handoff_owner_token,
        state.handoff_kind,
        state.requested_at,
    )


def _take_over_expired_download(
    work: VNextUnitOfWork,
    state: _DownloadState,
    *,
    now: int,
) -> DownloadHandoff:
    if (
        state.owner_token is None
        or state.lease_expires_at is None
        or state.lease_expires_at > now
        or state.handoff_kind is not None
        or state.ingest_generation is not None
    ):
        raise DownloadIngestUnavailableError(
            "download authority is not eligible for expired takeover"
        )
    handoff = DownloadHandoff(
        state.generation,
        state.owner_token,
        HandoffKind.EXPIRED_TAKEOVER,
        now,
    )
    work.connector.execute(
        f"INSERT INTO {_HANDOFF_TABLE} "
        "(download_generation, owner_token, handoff_kind, requested_at) "
        "VALUES (%s, %s, %s, %s)",
        (
            handoff.download_generation,
            handoff.owner_token,
            handoff.handoff_kind.value,
            handoff.requested_at,
        ),
    )
    _delete_exactly_one(
        work,
        f"DELETE FROM {_DOWNLOAD_LEASE_TABLE} WHERE generation = %s "
        "AND lease_expires_at = %s",
        (state.generation, state.lease_expires_at),
        authority="expired download lease",
    )
    _delete_exactly_one(
        work,
        f"DELETE FROM {_DOWNLOAD_OWNER_TABLE} WHERE generation = %s "
        "AND owner_token = %s",
        (state.generation, state.owner_token),
        authority="expired download owner",
    )
    return handoff


def _lock_and_validate_active_relationship(
    work: VNextUnitOfWork, turn: CoordinatedIngestTurn
) -> None:
    head = _require_download_head(work)
    if turn.download_generation is None:
        state = _lock_download_state(work, head.current_generation)
        _validate_current_download_state(head, state)
        if head.current_generation != head.completed_generation:
            raise DownloadIngestUnavailableError(
                "periodic ingest lost quiescent download authority"
            )
        if _lock_consumption_by_ingest(work, turn.ingest_turn.generation):
            raise DownloadIngestCorruptionError(
                "periodic ingest unexpectedly has a download consumption"
            )
        return

    state = _lock_download_state(work, turn.download_generation)
    if (
        head.current_generation != turn.download_generation
        or head.completed_generation >= turn.download_generation
        or state.completed_at is not None
        or state.owner_token is not None
        or state.lease_expires_at is not None
    ):
        raise DownloadIngestUnavailableError(
            "linked ingest no longer owns the pending download generation"
        )
    _require_turn_relationship(state, turn)


def _lock_and_validate_completion_relationship(
    work: VNextUnitOfWork,
    turn: CoordinatedIngestTurn,
    completed_at: int,
) -> None:
    head = _require_download_head(work)
    if turn.download_generation is None:
        if _lock_consumption_by_ingest(work, turn.ingest_turn.generation):
            raise DownloadIngestReplayMismatchError(
                "periodic completion has a durable download consumption"
            )
        return

    state = _lock_download_state(work, turn.download_generation)
    _require_turn_relationship(state, turn)
    if (
        state.completed_at != completed_at
        or head.completed_generation < turn.download_generation
    ):
        raise DownloadIngestReplayMismatchError(
            "linked download completion differs from replay authority"
        )


def _require_turn_relationship(
    state: _DownloadState, turn: CoordinatedIngestTurn
) -> None:
    if turn.download_generation is None:
        raise TypeError("linked relationship requires a download generation")
    actual = (
        state.generation,
        state.handoff_owner_token,
        state.handoff_kind,
        state.ingest_generation,
        state.consumed_at,
    )
    expected = (
        turn.download_generation,
        turn.handoff_owner_token,
        turn.handoff_kind,
        turn.ingest_turn.generation,
        turn.consumed_at,
    )
    if actual != expected:
        raise DownloadIngestReplayMismatchError(
            "stored handoff/consumption differs from the coordinated ingest tuple"
        )


def _lock_consumption_by_ingest(
    work: VNextUnitOfWork, ingest_generation: int
) -> tuple[object, ...]:
    return work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 6, ingest_generation),
        f"SELECT download_generation, consumed_at FROM {_CONSUMPTION_TABLE} "
        "WHERE ingest_generation = %s",
        (ingest_generation,),
    )


def _lock_and_validate_completed_ingest(
    work: VNextUnitOfWork, expected: CoordinatedIngestCompletion
) -> None:
    head = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 0),
        f"SELECT current_generation, completed_generation FROM {_INGEST_HEAD_TABLE} "
        "WHERE singleton_id = 1",
    )
    if len(head) != 2:
        raise DownloadIngestCorruptionError("completed ingest head is missing")
    current = require_int63(head[0], field="current ingest generation")
    completed = require_int63(head[1], field="completed ingest generation")
    if current < completed or completed < expected.ingest_generation:
        raise DownloadIngestCorruptionError(
            "completed ingest receipt is ahead of coordination history"
        )
    generation = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 1, expected.ingest_generation),
        f"SELECT completed_at FROM {_INGEST_GENERATION_TABLE} WHERE generation = %s",
        (expected.ingest_generation,),
    )
    owner = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 2, expected.ingest_generation),
        f"SELECT owner_token FROM {_INGEST_OWNER_TABLE} WHERE generation = %s",
        (expected.ingest_generation,),
    )
    lease = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 3, expected.ingest_generation),
        f"SELECT lease_expires_at FROM {_INGEST_LEASE_TABLE} WHERE generation = %s",
        (expected.ingest_generation,),
    )
    receipt = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 4, expected.ingest_generation),
        f"SELECT owner_token, completed_at FROM {_COMPLETION_TABLE} "
        "WHERE ingest_generation = %s",
        (expected.ingest_generation,),
    )
    if (
        generation != (expected.completed_at,)
        or owner
        or lease
        or len(receipt) != 2
        or require_uuid16(receipt[0], field="completion receipt owner_token")
        != expected.owner_token
        or require_int63(receipt[1], field="completion receipt completed_at")
        != expected.completed_at
    ):
        raise DownloadIngestReplayMismatchError(
            "stored coordinated completion differs from the complete replay tuple"
        )


def _require_fresh_download_token(work: VNextUnitOfWork, token: bytes) -> None:
    owner = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 6, 0, token),
        f"SELECT generation FROM {_DOWNLOAD_OWNER_TABLE} WHERE owner_token = %s",
        (token,),
    )
    handoff = work.lock_row(
        LockRank.DOWNLOAD_FENCE,
        encode_lock_key("download", 6, 1, token),
        f"SELECT download_generation FROM {_HANDOFF_TABLE} WHERE owner_token = %s",
        (token,),
    )
    if owner or handoff:
        raise DownloadCapabilityCollisionError(
            "generated download owner_token already exists"
        )


def _require_fresh_ingest_completion_token(work: VNextUnitOfWork, token: bytes) -> None:
    receipt = work.lock_row(
        LockRank.INGEST_FENCE,
        encode_lock_key("ingest", 4, token),
        f"SELECT ingest_generation FROM {_COMPLETION_TABLE} WHERE owner_token = %s",
        (token,),
    )
    if receipt:
        raise DownloadCapabilityCollisionError(
            "generated ingest owner_token already exists in completion history"
        )


def _require_download_turn(turn: DownloadTurn) -> DownloadTurn:
    if not isinstance(turn, DownloadTurn):
        raise TypeError("turn must be a DownloadTurn")
    require_int63(turn.generation, field="download generation")
    require_uuid16(turn.owner_token, field="download owner_token")
    require_int63(turn.lease_expires_at, field="download lease_expires_at")
    return turn


def _require_download_handoff(value: object) -> DownloadHandoff:
    if not isinstance(value, DownloadHandoff):
        raise TypeError("handoff must be a DownloadHandoff")
    # Frozen dataclasses remain forgeable through ``object.__setattr__``.
    value.__post_init__()
    return value


def _download_handoff_from_row(
    download_generation: int,
    row: tuple[object, ...],
) -> DownloadHandoff:
    if len(row) != 3:
        raise DownloadIngestCorruptionError("download handoff row has an invalid shape")
    try:
        generation = require_int63(
            download_generation,
            field="handoff download generation",
        )
        owner_token = require_uuid16(row[0], field="persisted handoff owner_token")
        if not isinstance(row[1], str):
            raise TypeError("persisted handoff kind must be text")
        handoff_kind = HandoffKind(row[1])
        requested_at = require_int63(row[2], field="persisted handoff requested_at")
    except (TypeError, ValueError) as error:
        raise DownloadIngestCorruptionError(
            "download handoff row violates its physical domain"
        ) from error
    return DownloadHandoff(generation, owner_token, handoff_kind, requested_at)


def _require_ingest_turn(turn: IngestTurn) -> IngestTurn:
    if not isinstance(turn, IngestTurn):
        raise TypeError("ingest_turn must be an IngestTurn")
    require_int63(turn.generation, field="ingest generation")
    require_uuid16(turn.owner_token, field="ingest owner_token")
    require_int63(turn.lease_expires_at, field="ingest lease_expires_at")
    return turn


def _require_coordinated_turn(
    turn: CoordinatedIngestTurn,
) -> CoordinatedIngestTurn:
    if not isinstance(turn, CoordinatedIngestTurn):
        raise TypeError("turn must be a CoordinatedIngestTurn")
    turn.__post_init__()
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
        raise DownloadIngestCorruptionError(
            f"{authority} deletion affected {affected} rows instead of 1"
        )


def _new_download_owner_token() -> bytes:
    """Return a repository-owned opaque capability; tests patch this source."""

    return secrets.token_bytes(16)


def _new_ingest_owner_token() -> bytes:
    """Return a repository-owned ingest capability; tests patch this source."""

    return secrets.token_bytes(16)
