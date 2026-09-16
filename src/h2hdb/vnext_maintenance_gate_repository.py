"""Normalized 64-slot maintenance gate for vNext writers.

The caller owns the write transaction. Every operation locks the singleton
head before reading the fixed slot domain in one bounded locking query.
The head serializes gate mutations; SQL result order is not a claim about
the database engine's physical lock acquisition order. SQLite continues to
serialize writes with ``BEGIN IMMEDIATE``.
"""

from __future__ import annotations

__all__ = [
    "GateLease",
    "GateMode",
    "MaintenanceGateCorruptionError",
    "MaintenanceGateExhaustedError",
    "MaintenanceGateRepository",
    "MaintenanceGateTokenCollisionError",
    "MaintenanceGateUnavailableError",
]

import secrets
from dataclasses import dataclass
from enum import StrEnum

from .vnext_domains import INT63_MAX, require_int63, require_uuid16
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_GENERATION_TABLE = "operational_maintenance_gate_generations"
_HEAD_TABLE = "operational_maintenance_gate_heads"
_OWNER_TABLE = "operational_maintenance_gate_owners"
_HOLDER_TABLE = "operational_maintenance_gate_holders"
_SLOTS = tuple(range(64))


class MaintenanceGateUnavailableError(RuntimeError):
    """The requested gate authority is live elsewhere or is stale."""


class MaintenanceGateCorruptionError(RuntimeError):
    """Persisted gate rows do not refine the 64-slot contract."""


class MaintenanceGateExhaustedError(OverflowError):
    """A new gate generation cannot be represented by int63."""


class MaintenanceGateTokenCollisionError(MaintenanceGateUnavailableError):
    """The repository-generated opaque owner capability already exists."""


class GateMode(StrEnum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


@dataclass(frozen=True, slots=True)
class GateLease:
    owner_token: bytes
    gate_generation: int
    mode: GateMode
    slots: tuple[int, ...]
    lease_expires_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.owner_token, field="gate owner_token")
        require_int63(self.gate_generation, field="gate generation")
        if not isinstance(self.mode, GateMode):
            raise TypeError("gate mode must be a GateMode")
        _require_slots(self.slots, mode=self.mode)
        require_int63(self.lease_expires_at, field="gate lease_expires_at")


@dataclass(frozen=True, slots=True)
class _Head:
    generation: int
    updated_at: int
    mode: GateMode


@dataclass(frozen=True, slots=True)
class _Owner:
    token: bytes
    generation: int
    lease_expires_at: int


@dataclass(frozen=True, slots=True)
class LockedSharedGateClaim:
    """Unpublished claim state, valid only in its owning write transaction."""

    work: VNextUnitOfWork
    token: bytes
    head: _Head | None
    slots: tuple[_Owner | None, ...]

    def require_available(self, *, now: int) -> None:
        """Reject ordinary contention before a potentially expensive proof."""

        timestamp = require_int63(now, field="shared gate admission now")
        _shared_claim_position(self.head, self.slots, now=timestamp)

    def grant(self, *, now: int, lease_duration: int) -> GateLease:
        timestamp = require_int63(now, field="shared gate claim now")
        deadline = _lease_deadline(timestamp, lease_duration)
        work, token, head, slots = self.work, self.token, self.head, self.slots
        next_generation, candidate = _shared_claim_position(head, slots, now=timestamp)
        if head is None:
            MaintenanceGateRepository._create_generation_and_head(
                work, generation=0, mode=GateMode.SHARED, now=timestamp
            )
            work.connector.execute(
                f"INSERT INTO {_OWNER_TABLE} "
                "(owner_token, gate_generation, lease_expires_at) "
                "VALUES (%s, 0, %s)",
                (token, deadline),
            )
            _insert_holders_exact(work, token, (0,))
            return GateLease(token, 0, GateMode.SHARED, (0,), deadline)
        owners = _owners_by_token(slots)
        claim_mode = GateMode.SHARED

        if next_generation != head.generation:
            work.connector.execute(
                f"INSERT INTO {_GENERATION_TABLE} "
                "(gate_generation, mode, created_at) VALUES (%s, %s, %s)",
                (next_generation, claim_mode.value, timestamp),
            )
        work.connector.execute(
            f"INSERT INTO {_OWNER_TABLE} "
            "(owner_token, gate_generation, lease_expires_at) VALUES (%s, %s, %s)",
            (token, next_generation, deadline),
        )
        replaced = slots[candidate]
        if replaced is None:
            _insert_holders_exact(work, token, (candidate,))
        else:
            _replace_holder(work, candidate, replaced.token, token)
            if len(owners[replaced.token]) == 1:
                _delete_owners_exact(work, (replaced,))

        if next_generation != head.generation:
            work.compare_and_swap(
                f"UPDATE {_HEAD_TABLE} SET gate_generation = %s, updated_at = %s "
                "WHERE singleton_id = 1 AND gate_generation = %s AND updated_at = %s",
                (
                    next_generation,
                    timestamp,
                    head.generation,
                    head.updated_at,
                ),
                authority="maintenance gate head",
            )
        return GateLease(token, next_generation, claim_mode, (candidate,), deadline)


@dataclass(frozen=True, slots=True)
class LockedGateRenewal:
    """Exact persisted authority locked by one owning write transaction."""

    work: VNextUnitOfWork
    lease: GateLease

    def require_live(self, *, now: int) -> GateLease:
        timestamp = require_int63(now, field="gate authorization now")
        if self.lease.lease_expires_at <= timestamp:
            raise MaintenanceGateUnavailableError("the gate lease is stale or expired")
        return self.lease

    def renew(self, *, now: int, lease_duration: int) -> GateLease:
        current = self.require_live(now=now)
        deadline = _lease_deadline(now, lease_duration)
        if deadline <= current.lease_expires_at:
            return current
        self.work.compare_and_swap(
            f"UPDATE {_OWNER_TABLE} SET lease_expires_at = %s "
            "WHERE owner_token = %s AND gate_generation = %s "
            "AND lease_expires_at = %s",
            (
                deadline,
                current.owner_token,
                current.gate_generation,
                current.lease_expires_at,
            ),
            authority="maintenance gate owner lease",
        )
        return GateLease(
            current.owner_token,
            current.gate_generation,
            current.mode,
            current.slots,
            deadline,
        )


class MaintenanceGateRepository:
    """Acquire and fence SHARED/EXCLUSIVE maintenance leases.

    Owner tokens are repository-generated, single-use opaque capabilities;
    public claims never accept caller-selected token bytes.  A caller that
    already has the complete capability may use :meth:`resume` after response
    loss.  A generated-token collision fails closed instead of being treated
    as replay.
    """

    @staticmethod
    def claim_shared(
        work: VNextUnitOfWork,
        *,
        now: int,
        lease_duration: int,
    ) -> GateLease:
        timestamp = require_int63(now, field="shared gate claim now")
        _lease_deadline(timestamp, lease_duration)
        return MaintenanceGateRepository.lock_shared_claim(work).grant(
            now=timestamp, lease_duration=lease_duration
        )

    @staticmethod
    def lock_shared_claim(work: VNextUnitOfWork) -> LockedSharedGateClaim:
        """Lock claim authority without starting its lease deadline.

        The result cannot escape this transaction. Compound writers may acquire
        later-ranked locks before granting with a freshly sampled time.
        """

        token = require_uuid16(_new_owner_token(), field="generated gate owner_token")
        head = MaintenanceGateRepository._lock_head_and_mode(work)
        target = MaintenanceGateRepository._lock_owner(work, token)
        slots = MaintenanceGateRepository._lock_slots(work)
        if head is not None:
            _validate_current_holders(head, slots, _owners_by_token(slots))
        if target is not None:
            raise MaintenanceGateTokenCollisionError(
                "generated gate owner token already exists"
            )
        if head is None and any(owner is not None for owner in slots):
            raise MaintenanceGateCorruptionError(
                "gate authority exists without its singleton head"
            )
        return LockedSharedGateClaim(work, token, head, slots)

    @staticmethod
    def claim_exclusive(
        work: VNextUnitOfWork,
        *,
        now: int,
        lease_duration: int,
    ) -> GateLease:
        token = require_uuid16(_new_owner_token(), field="generated gate owner_token")
        timestamp = require_int63(now, field="exclusive gate claim now")
        deadline = _lease_deadline(timestamp, lease_duration)
        head = MaintenanceGateRepository._lock_head_and_mode(work)

        if head is None:
            target = MaintenanceGateRepository._lock_owner(work, token)
            slots = MaintenanceGateRepository._lock_slots(work)
            if target is not None:
                raise MaintenanceGateTokenCollisionError(
                    "generated gate owner token already exists"
                )
            if any(owner is not None for owner in slots):
                raise MaintenanceGateCorruptionError(
                    "gate authority exists without its singleton head"
                )
            MaintenanceGateRepository._create_generation_and_head(
                work,
                generation=0,
                mode=GateMode.EXCLUSIVE,
                now=timestamp,
            )
            work.connector.execute(
                f"INSERT INTO {_OWNER_TABLE} "
                "(owner_token, gate_generation, lease_expires_at) "
                "VALUES (%s, 0, %s)",
                (token, deadline),
            )
            _insert_holders_exact(work, token, _SLOTS)
            return GateLease(token, 0, GateMode.EXCLUSIVE, _SLOTS, deadline)

        target = MaintenanceGateRepository._lock_owner(work, token)
        slots = MaintenanceGateRepository._lock_slots(work)
        owners = _owners_by_token(slots)
        _validate_current_holders(head, slots, owners)

        if target is not None:
            raise MaintenanceGateTokenCollisionError(
                "generated gate owner token already exists"
            )

        live = _live_current_owners(head, slots, owners, now=timestamp)
        if live:
            raise MaintenanceGateUnavailableError(
                "a live current gate holder blocks EXCLUSIVE acquisition"
            )
        next_generation = _successor(head.generation)

        work.connector.execute(
            f"INSERT INTO {_GENERATION_TABLE} "
            "(gate_generation, mode, created_at) VALUES (%s, %s, %s)",
            (next_generation, GateMode.EXCLUSIVE.value, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_OWNER_TABLE} "
            "(owner_token, gate_generation, lease_expires_at) VALUES (%s, %s, %s)",
            (token, next_generation, deadline),
        )
        # The locked singleton head serializes every gate mutation. The exact
        # old slot/owner pairs are durable authority, bounded by the 64-slot
        # domain. Replace that set atomically instead of issuing one round trip
        # per slot; old owners are removed only after their children are gone.
        _delete_holders_exact(
            work,
            tuple(
                (slot, owner.token)
                for slot, owner in enumerate(slots)
                if owner is not None
            ),
        )
        _insert_holders_exact(work, token, _SLOTS)
        _delete_owners_exact(
            work, tuple({owner.token: owner for owner in slots if owner}.values())
        )
        work.compare_and_swap(
            f"UPDATE {_HEAD_TABLE} SET gate_generation = %s, updated_at = %s "
            "WHERE singleton_id = 1 AND gate_generation = %s AND updated_at = %s",
            (next_generation, timestamp, head.generation, head.updated_at),
            authority="maintenance gate head",
        )
        return GateLease(token, next_generation, GateMode.EXCLUSIVE, _SLOTS, deadline)

    @staticmethod
    def resume(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
    ) -> GateLease:
        """Re-read one complete existing capability after response loss."""

        return MaintenanceGateRepository.lock_and_require_live(
            work,
            lease,
            now=now,
        )

    @staticmethod
    def renew(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
        lease_duration: int,
    ) -> GateLease:
        return MaintenanceGateRepository.lock_for_renewal(work, lease).renew(
            now=now, lease_duration=lease_duration
        )

    @staticmethod
    def lock_and_require_live(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
    ) -> GateLease:
        timestamp = require_int63(now, field="gate authorization now")
        return MaintenanceGateRepository.lock_for_renewal(work, lease).require_live(
            now=timestamp
        )

    @staticmethod
    def lock_for_renewal(work: VNextUnitOfWork, lease: GateLease) -> LockedGateRenewal:
        """Lock and exact-match authority; expiry is checked after later locks."""

        requested = _require_lease(lease)
        head = MaintenanceGateRepository._lock_head_and_mode(work)
        if head is None:
            raise MaintenanceGateUnavailableError(
                "the maintenance gate head is missing"
            )
        target = MaintenanceGateRepository._lock_owner(work, requested.owner_token)
        slots = MaintenanceGateRepository._lock_slots(work)
        owners = _owners_by_token(slots)
        _validate_current_holders(head, slots, owners)
        actual_slots = owners.get(requested.owner_token, ())
        if (
            target is None
            or head.generation != requested.gate_generation
            or head.mode != requested.mode
            or target.generation != requested.gate_generation
            or target.lease_expires_at != requested.lease_expires_at
            or actual_slots != requested.slots
        ):
            raise MaintenanceGateUnavailableError("the gate lease is stale or expired")
        _require_slots(actual_slots, mode=head.mode)
        return LockedGateRenewal(work, requested)

    @staticmethod
    def release(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
    ) -> None:
        current = MaintenanceGateRepository.lock_and_require_live(work, lease, now=now)
        _delete_holders_exact(
            work, tuple((slot, current.owner_token) for slot in current.slots)
        )
        _delete_owners_exact(
            work,
            (
                _Owner(
                    current.owner_token,
                    current.gate_generation,
                    current.lease_expires_at,
                ),
            ),
        )

    @staticmethod
    def _lock_head_and_mode(work: VNextUnitOfWork) -> _Head | None:
        row = work.lock_row(
            LockRank.MAINTENANCE_GATE,
            encode_lock_key("gate", 0),
            f"SELECT gate_generation, updated_at FROM {_HEAD_TABLE} "
            "WHERE singleton_id = 1",
        )
        if not row:
            return None
        if len(row) != 2:
            raise MaintenanceGateCorruptionError("the gate head has an invalid shape")
        generation = require_int63(row[0], field="current gate generation")
        updated_at = require_int63(row[1], field="gate head updated_at")
        generation_row = work.lock_row(
            LockRank.MAINTENANCE_GATE,
            encode_lock_key("gate", 1, generation),
            f"SELECT mode FROM {_GENERATION_TABLE} WHERE gate_generation = %s",
            (generation,),
        )
        if len(generation_row) != 1:
            raise MaintenanceGateCorruptionError(
                "the current gate generation is missing"
            )
        try:
            mode = GateMode(generation_row[0])
        except (TypeError, ValueError) as error:
            raise MaintenanceGateCorruptionError(
                "the current gate generation has an unknown mode"
            ) from error
        return _Head(generation, updated_at, mode)

    @staticmethod
    def _lock_owner(work: VNextUnitOfWork, token: bytes) -> _Owner | None:
        row = work.lock_row(
            LockRank.MAINTENANCE_GATE,
            encode_lock_key("gate", 2, token),
            f"SELECT gate_generation, lease_expires_at FROM {_OWNER_TABLE} "
            "WHERE owner_token = %s",
            (token,),
        )
        if not row:
            return None
        if len(row) != 2:
            raise MaintenanceGateCorruptionError("the gate owner has an invalid shape")
        return _Owner(
            token,
            require_int63(row[0], field="owner gate generation"),
            require_int63(row[1], field="owner lease_expires_at"),
        )

    @staticmethod
    def _lock_slots(work: VNextUnitOfWork) -> tuple[_Owner | None, ...]:
        # A sentinel row detects an oversized/corrupt domain without buffering
        # an unbounded result. LEFT JOIN keeps orphaned holders observable.
        rows = work.lock_rows(
            LockRank.MAINTENANCE_GATE,
            tuple(encode_lock_key("gate", 3, slot) for slot in _SLOTS),
            f"SELECT h.slot, h.owner_token, o.owner_token, "
            f"o.gate_generation, o.lease_expires_at FROM {_HOLDER_TABLE} AS h "
            f"LEFT JOIN {_OWNER_TABLE} AS o ON o.owner_token = h.owner_token "
            "ORDER BY h.slot LIMIT %s",
            (len(_SLOTS) + 1,),
        )
        if len(rows) > len(_SLOTS):
            raise MaintenanceGateCorruptionError("gate holder domain exceeds 64 slots")
        result: list[_Owner | None] = [None] * len(_SLOTS)
        previous_slot = -1
        for row in rows:
            if len(row) != 5:
                raise MaintenanceGateCorruptionError("gate holder has an invalid shape")
            slot = require_int63(row[0], field="gate holder slot")
            if slot not in _SLOTS or slot <= previous_slot:
                raise MaintenanceGateCorruptionError(
                    "gate holders are not an exact ordered subset of 0..63"
                )
            token = require_uuid16(row[1], field=f"gate slot {slot} owner_token")
            if row[2] != token or row[3] is None or row[4] is None:
                raise MaintenanceGateCorruptionError(
                    f"gate slot {slot} has no exact owner authority"
                )
            result[slot] = _Owner(
                token,
                require_int63(row[3], field=f"gate slot {slot} generation"),
                require_int63(row[4], field=f"gate slot {slot} lease_expires_at"),
            )
            previous_slot = slot
        return tuple(result)

    @staticmethod
    def _create_generation_and_head(
        work: VNextUnitOfWork,
        *,
        generation: int,
        mode: GateMode,
        now: int,
    ) -> None:
        work.connector.execute(
            f"INSERT INTO {_GENERATION_TABLE} "
            "(gate_generation, mode, created_at) VALUES (%s, %s, %s)",
            (generation, mode.value, now),
        )
        work.connector.execute(
            f"INSERT INTO {_HEAD_TABLE} "
            "(singleton_id, gate_generation, updated_at) VALUES (1, %s, %s)",
            (generation, now),
        )


def _shared_claim_position(
    head: _Head | None,
    slots: tuple[_Owner | None, ...],
    *,
    now: int,
) -> tuple[int, int]:
    """Select from already locked authority without creating a lease."""

    if head is None:
        return 0, 0
    if head.mode == GateMode.EXCLUSIVE:
        if _live_current_owners(head, slots, _owners_by_token(slots), now=now):
            raise MaintenanceGateUnavailableError(
                "the current EXCLUSIVE gate has a live owner"
            )
        generation = _successor(head.generation)
    else:
        generation = head.generation
    candidate = next(
        (
            slot
            for slot, owner in enumerate(slots)
            if owner is None
            or owner.generation != head.generation
            or owner.lease_expires_at <= now
        ),
        None,
    )
    if candidate is None:
        raise MaintenanceGateUnavailableError("all 64 SHARED slots are live")
    return generation, candidate


def _owners_by_token(
    slots: tuple[_Owner | None, ...],
) -> dict[bytes, tuple[int, ...]]:
    mutable: dict[bytes, list[int]] = {}
    for slot, owner in enumerate(slots):
        if owner is not None:
            mutable.setdefault(owner.token, []).append(slot)
    return {token: tuple(owned) for token, owned in mutable.items()}


def _validate_current_holders(
    head: _Head,
    slot_authority: tuple[_Owner | None, ...],
    owners: dict[bytes, tuple[int, ...]],
) -> None:
    authority: dict[bytes, _Owner] = {}
    for owner in slot_authority:
        if owner is None:
            continue
        previous = authority.setdefault(owner.token, owner)
        if previous != owner:
            raise MaintenanceGateCorruptionError(
                "one gate owner token has inconsistent normalized authority"
            )
    for token, slots in owners.items():
        require_uuid16(token, field="persisted gate owner_token")
        if not slots or any(slot not in _SLOTS for slot in slots):
            raise MaintenanceGateCorruptionError("gate holder is outside 0..63")
    current = {
        token: slots
        for token, slots in owners.items()
        if authority[token].generation == head.generation
    }
    if head.mode == GateMode.SHARED:
        if any(len(slots) != 1 for slots in current.values()):
            raise MaintenanceGateCorruptionError(
                "every current SHARED owner must hold exactly one slot"
            )
        return
    if current and (len(current) != 1 or next(iter(current.values())) != _SLOTS):
        raise MaintenanceGateCorruptionError(
            "a current EXCLUSIVE owner must alone hold exactly slots 0..63"
        )


def _live_current_owners(
    head: _Head,
    slot_authority: tuple[_Owner | None, ...],
    owners: dict[bytes, tuple[int, ...]],
    *,
    now: int,
) -> dict[bytes, tuple[int, ...]]:
    authority = {owner.token: owner for owner in slot_authority if owner is not None}
    return {
        token: held
        for token, held in owners.items()
        if authority[token].generation == head.generation
        and authority[token].lease_expires_at > now
    }


def _replace_holder(
    work: VNextUnitOfWork,
    slot: int,
    old_token: bytes,
    new_token: bytes,
) -> None:
    affected = work.connector.execute_affected(
        f"UPDATE {_HOLDER_TABLE} SET owner_token = %s "
        "WHERE slot = %s AND owner_token = %s",
        (new_token, slot, old_token),
    )
    if affected != 1:
        raise MaintenanceGateCorruptionError(
            f"gate slot {slot} reclaim affected {affected} rows"
        )


def _insert_holders_exact(
    work: VNextUnitOfWork, token: bytes, slots: tuple[int, ...]
) -> None:
    affected = work.connector.execute_affected(
        f"INSERT INTO {_HOLDER_TABLE} (owner_token, slot) VALUES "
        + ", ".join("(%s, %s)" for _ in slots),
        tuple(value for slot in slots for value in (token, slot)),
    )
    if affected != len(slots):
        raise MaintenanceGateCorruptionError(
            f"gate holder insertion affected {affected} rows; expected {len(slots)}"
        )


def _delete_holders_exact(
    work: VNextUnitOfWork, holders: tuple[tuple[int, bytes], ...]
) -> None:
    if not holders:
        return
    affected = work.connector.execute_affected(
        f"DELETE FROM {_HOLDER_TABLE} WHERE "
        + " OR ".join("(slot = %s AND owner_token = %s)" for _ in holders),
        tuple(value for holder in holders for value in holder),
    )
    if affected != len(holders):
        raise MaintenanceGateCorruptionError(
            f"gate holder deletion affected {affected} rows; expected {len(holders)}"
        )


def _delete_owners_exact(work: VNextUnitOfWork, owners: tuple[_Owner, ...]) -> None:
    if not owners:
        return
    affected = work.connector.execute_affected(
        f"DELETE FROM {_OWNER_TABLE} WHERE "
        + " OR ".join(
            "(owner_token = %s AND gate_generation = %s AND lease_expires_at = %s)"
            for _ in owners
        ),
        tuple(
            value
            for owner in owners
            for value in (owner.token, owner.generation, owner.lease_expires_at)
        ),
    )
    if affected != len(owners):
        raise MaintenanceGateCorruptionError(
            "reclaimed gate owner was still referenced or changed"
        )


def _require_lease(lease: GateLease) -> GateLease:
    if not isinstance(lease, GateLease):
        raise TypeError("lease must be a GateLease")
    require_uuid16(lease.owner_token, field="gate owner_token")
    require_int63(lease.gate_generation, field="gate generation")
    if not isinstance(lease.mode, GateMode):
        raise TypeError("gate mode must be a GateMode")
    _require_slots(lease.slots, mode=lease.mode)
    require_int63(lease.lease_expires_at, field="gate lease_expires_at")
    return lease


def _require_slots(slots: object, *, mode: GateMode) -> tuple[int, ...]:
    if not isinstance(slots, tuple):
        raise TypeError("gate slots must be a tuple")
    expected_length = 1 if mode == GateMode.SHARED else 64
    if len(slots) != expected_length:
        raise ValueError(f"{mode.value} gate lease must hold {expected_length} slot(s)")
    normalized: list[int] = []
    for slot in slots:
        value = require_int63(slot, field="gate slot")
        if value not in _SLOTS:
            raise ValueError("gate slot must be in 0..63")
        normalized.append(value)
    result = tuple(normalized)
    if result != tuple(sorted(set(result))):
        raise ValueError("gate slots must be unique and ascending")
    if mode == GateMode.EXCLUSIVE and result != _SLOTS:
        raise ValueError("EXCLUSIVE gate lease must hold exactly slots 0..63")
    return result


def _lease_deadline(now: int, duration: int) -> int:
    interval = require_int63(duration, field="gate lease duration")
    if interval > INT63_MAX - now:
        raise OverflowError("gate lease deadline exceeds int63")
    return now + interval


def _successor(current: int) -> int:
    if current == INT63_MAX:
        raise MaintenanceGateExhaustedError("gate generation space is exhausted")
    return current + 1


def _new_owner_token() -> bytes:
    """Return one production capability; tests may patch this private source."""

    return secrets.token_bytes(16)
