"""Transaction and lock-order primitives shared by vNext repositories."""

from __future__ import annotations

__all__ = [
    "LockOrderViolationError",
    "LockRank",
    "StaleWriteError",
    "VNextUnitOfWork",
    "encode_lock_key",
]

from collections.abc import Sequence
from enum import IntEnum
from typing import Any

from .sql_connector import SQLConnector
from .vnext_domains import require_bounded_bytes, require_int63


class LockOrderViolationError(RuntimeError):
    """A repository attempted to acquire locks in a deadlock-prone order."""


class StaleWriteError(RuntimeError):
    """A compare-and-swap did not mutate its one expected authority row."""


class LockRank(IntEnum):
    """Global write lock order; lower ranks are always acquired first."""

    MAINTENANCE_GATE = 10
    DOWNLOAD_FENCE = 15
    INGEST_FENCE = 20
    WORKING_ROOT = 30
    CHECKPOINT = 40
    ALLOCATOR = 50
    HEAD = 60
    CHILD = 70


def encode_lock_key(*components: bytes | str | int) -> bytes:
    """Encode heterogeneous natural-key components for deterministic ordering."""

    if not components:
        raise ValueError("a lock key requires at least one component")
    encoded = bytearray()
    for index, component in enumerate(components):
        match component:
            case bytes():
                payload = component
                tag = b"b"
            case str():
                payload = component.encode("utf-8", errors="strict")
                tag = b"s"
            case int() if not isinstance(component, bool):
                value = require_int63(component, field=f"lock component {index}")
                payload = value.to_bytes(8, "big")
                tag = b"i"
            case _:
                raise TypeError(
                    "lock-key components must be exact bytes, str, or nonnegative int63"
                )
        if len(payload) > (1 << 32) - 1:
            raise ValueError("lock-key component is too large")
        encoded.extend(tag)
        encoded.extend(len(payload).to_bytes(4, "big"))
        encoded.extend(payload)
    return bytes(encoded)


class VNextUnitOfWork:
    """One already-open write transaction with executable lock ordering."""

    def __init__(self, connector: SQLConnector, *, backend: str) -> None:
        if backend not in {"sqlite", "mariadb"}:
            raise ValueError(f"unsupported SQL backend {backend!r}")
        self.connector = connector
        self.backend = backend
        self._highest_rank: LockRank | None = None
        self._last_key_by_rank: dict[LockRank, bytes] = {}

    def lock_row(
        self,
        rank: LockRank,
        key: bytes,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        """Record the global order and lock/read one authority row."""

        lock_key = require_bounded_bytes(
            key,
            field="lock key",
            minimum=1,
            maximum=4096,
        )
        self._record_lock(rank, lock_key)
        statement = query.strip()
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        if not statement.upper().startswith("SELECT ") or ";" in statement:
            raise ValueError("lock_row accepts one SELECT statement")
        if " FOR UPDATE" in statement.upper():
            raise ValueError("lock_row owns backend-specific FOR UPDATE syntax")
        if self.backend == "mariadb":
            statement += " FOR UPDATE"
        return self.connector.fetch_one(statement, data)

    def lock_rows(
        self,
        rank: LockRank,
        keys: Sequence[bytes],
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        """Record ordered natural keys and lock/read their bounded row set once."""

        lock_keys = tuple(
            require_bounded_bytes(
                key,
                field="lock key",
                minimum=1,
                maximum=4096,
            )
            for key in keys
        )
        if not lock_keys or tuple(sorted(set(lock_keys))) != lock_keys:
            raise ValueError("lock_rows keys must be one nonempty exact ordered set")
        for lock_key in lock_keys:
            self._record_lock(rank, lock_key)
        statement = query.strip()
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        if not statement.upper().startswith("SELECT ") or ";" in statement:
            raise ValueError("lock_rows accepts one SELECT statement")
        if " FOR UPDATE" in statement.upper():
            raise ValueError("lock_rows owns backend-specific FOR UPDATE syntax")
        if self.backend == "mariadb":
            statement += " FOR UPDATE"
        return self.connector.fetch_all(statement, data)

    def compare_and_swap(
        self,
        query: str,
        data: tuple[Any, ...] = (),
        *,
        authority: str,
    ) -> None:
        affected = self.connector.execute_affected(query, data)
        if affected != 1:
            raise StaleWriteError(
                f"{authority} compare-and-swap affected {affected} rows instead of 1"
            )

    def _record_lock(self, rank: LockRank, key: bytes) -> None:
        if not isinstance(rank, LockRank):
            raise TypeError("rank must be a LockRank")
        if self._highest_rank is not None and rank < self._highest_rank:
            raise LockOrderViolationError(
                f"cannot acquire {rank.name} after {self._highest_rank.name}"
            )
        previous = self._last_key_by_rank.get(rank)
        if previous is not None and key < previous:
            raise LockOrderViolationError(
                f"{rank.name} keys must be acquired in unsigned byte order"
            )
        self._last_key_by_rank[rank] = key
        if self._highest_rank is None or rank > self._highest_rank:
            self._highest_rank = rank
