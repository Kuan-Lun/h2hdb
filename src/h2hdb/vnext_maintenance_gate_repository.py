"""Normalized 64-slot maintenance gate for vNext writers.

The caller owns the write transaction.  Every operation locks the singleton
head first and the fixed slot domain in ascending order.  This makes both the
SQLite ``BEGIN IMMEDIATE`` path and MariaDB ``FOR UPDATE`` path use the same
bounded, executable locking protocol.
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
        token = require_uuid16(_new_owner_token(), field="generated gate owner_token")
        timestamp = require_int63(now, field="shared gate claim now")
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
                mode=GateMode.SHARED,
                now=timestamp,
            )
            work.connector.execute(
                f"INSERT INTO {_OWNER_TABLE} "
                "(owner_token, gate_generation, lease_expires_at) "
                "VALUES (%s, 0, %s)",
                (token, deadline),
            )
            work.connector.execute(
                f"INSERT INTO {_HOLDER_TABLE} (owner_token, slot) VALUES (%s, 0)",
                (token,),
            )
            return GateLease(token, 0, GateMode.SHARED, (0,), deadline)

        target = MaintenanceGateRepository._lock_owner(work, token)
        slots = MaintenanceGateRepository._lock_slots(work)
        owners = _owners_by_token(slots)
        _validate_current_holders(head, slots, owners)

        if target is not None:
            raise MaintenanceGateTokenCollisionError(
                "generated gate owner token already exists"
            )

        if head.mode == GateMode.EXCLUSIVE:
            live = _live_current_owners(head, slots, owners, now=timestamp)
            if live:
                raise MaintenanceGateUnavailableError(
                    "the current EXCLUSIVE gate has a live owner"
                )
            next_generation = _successor(head.generation)
            claim_mode = GateMode.SHARED
        else:
            next_generation = head.generation
            claim_mode = GateMode.SHARED

        candidate = next(
            (
                slot
                for slot, owner in enumerate(slots)
                if owner is None
                or owner.generation != head.generation
                or owner.lease_expires_at <= timestamp
            ),
            None,
        )
        if candidate is None:
            raise MaintenanceGateUnavailableError("all 64 SHARED slots are live")

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
            work.connector.execute(
                f"INSERT INTO {_HOLDER_TABLE} (owner_token, slot) VALUES (%s, %s)",
                (token, candidate),
            )
        else:
            _replace_holder(work, candidate, replaced.token, token)
            if len(owners[replaced.token]) == 1:
                _delete_owner_exact(work, replaced)

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
            for slot in _SLOTS:
                work.connector.execute(
                    f"INSERT INTO {_HOLDER_TABLE} (owner_token, slot) VALUES (%s, %s)",
                    (token, slot),
                )
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
        replaced_tokens: dict[bytes, _Owner] = {}
        for slot, replaced in enumerate(slots):
            if replaced is None:
                work.connector.execute(
                    f"INSERT INTO {_HOLDER_TABLE} (owner_token, slot) "
                    "VALUES (%s, %s)",
                    (token, slot),
                )
            else:
                _replace_holder(work, slot, replaced.token, token)
                replaced_tokens[replaced.token] = replaced
        for replaced in replaced_tokens.values():
            _delete_owner_exact(work, replaced)
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
        current = MaintenanceGateRepository.lock_and_require_live(work, lease, now=now)
        timestamp = require_int63(now, field="gate renew now")
        deadline = _lease_deadline(timestamp, lease_duration)
        if deadline <= current.lease_expires_at:
            return current
        work.compare_and_swap(
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

    @staticmethod
    def lock_and_require_live(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
    ) -> GateLease:
        requested = _require_lease(lease)
        timestamp = require_int63(now, field="gate authorization now")
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
            or target.lease_expires_at <= timestamp
            or actual_slots != requested.slots
        ):
            raise MaintenanceGateUnavailableError("the gate lease is stale or expired")
        _require_slots(actual_slots, mode=head.mode)
        return requested

    @staticmethod
    def release(
        work: VNextUnitOfWork,
        lease: GateLease,
        *,
        now: int,
    ) -> None:
        current = MaintenanceGateRepository.lock_and_require_live(work, lease, now=now)
        for slot in current.slots:
            affected = work.connector.execute_affected(
                f"DELETE FROM {_HOLDER_TABLE} WHERE slot = %s AND owner_token = %s",
                (slot, current.owner_token),
            )
            if affected != 1:
                raise MaintenanceGateCorruptionError(
                    f"gate holder {slot} deletion affected {affected} rows"
                )
        affected = work.connector.execute_affected(
            f"DELETE FROM {_OWNER_TABLE} WHERE owner_token = %s "
            "AND gate_generation = %s AND lease_expires_at = %s",
            (
                current.owner_token,
                current.gate_generation,
                current.lease_expires_at,
            ),
        )
        if affected != 1:
            raise MaintenanceGateCorruptionError(
                f"gate owner deletion affected {affected} rows"
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
        result: list[_Owner | None] = []
        for slot in _SLOTS:
            row = work.lock_row(
                LockRank.MAINTENANCE_GATE,
                encode_lock_key("gate", 3, slot),
                f"SELECT h.owner_token, o.gate_generation, o.lease_expires_at "
                f"FROM {_HOLDER_TABLE} AS h JOIN {_OWNER_TABLE} AS o "
                "ON o.owner_token = h.owner_token WHERE h.slot = %s",
                (slot,),
            )
            if not row:
                result.append(None)
                continue
            if len(row) != 3:
                raise MaintenanceGateCorruptionError(
                    f"gate slot {slot} has an invalid shape"
                )
            result.append(
                _Owner(
                    require_uuid16(row[0], field=f"gate slot {slot} owner_token"),
                    require_int63(row[1], field=f"gate slot {slot} generation"),
                    require_int63(row[2], field=f"gate slot {slot} lease_expires_at"),
                )
            )
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


def _delete_owner_exact(work: VNextUnitOfWork, owner: _Owner) -> None:
    affected = work.connector.execute_affected(
        f"DELETE FROM {_OWNER_TABLE} WHERE owner_token = %s "
        "AND gate_generation = %s AND lease_expires_at = %s",
        (owner.token, owner.generation, owner.lease_expires_at),
    )
    if affected != 1:
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
