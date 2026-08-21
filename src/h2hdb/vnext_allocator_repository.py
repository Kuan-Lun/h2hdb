"""Portable monotone allocators for the greenfield vNext schema."""

from __future__ import annotations

__all__ = [
    "AllocatorExhaustedError",
    "IdentityStream",
    "RevisionStream",
    "VNextAllocatorRepository",
]

from enum import StrEnum

from .vnext_domains import INT63_MAX, require_int63, require_positive_int63
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key


class AllocatorExhaustedError(OverflowError):
    """The reserved int63 exhaustion sentinel is current; no row changed."""


class RevisionStream(StrEnum):
    SOURCE = "SOURCE"
    CATALOG = "CATALOG"


class IdentityStream(StrEnum):
    GALLERY = "GALLERY"
    TAG = "TAG"
    POLICY = "POLICY"


class VNextAllocatorRepository:
    """Allocate IDs under the caller's fenced, gate-authorized transaction."""

    @staticmethod
    def allocate_revision(
        work: VNextUnitOfWork,
        stream: RevisionStream,
        *,
        updated_at: int,
    ) -> int:
        timestamp = require_int63(updated_at, field="revision allocator updated_at")
        current = VNextAllocatorRepository._lock_current(
            work,
            table="operational_revision_allocators",
            value_column="next_revision",
            stream=stream.value,
        )
        VNextAllocatorRepository._advance(
            work,
            table="operational_revision_allocators",
            value_column="next_revision",
            stream=stream.value,
            current=current,
            updated_at=timestamp,
        )
        return current

    @staticmethod
    def allocate_identity(
        work: VNextUnitOfWork,
        stream: IdentityStream,
        *,
        updated_at: int,
    ) -> int:
        timestamp = require_int63(updated_at, field="identity allocator updated_at")
        current = VNextAllocatorRepository._lock_current(
            work,
            table="operational_identity_allocators",
            value_column="next_id",
            stream=stream.value,
        )
        VNextAllocatorRepository._advance(
            work,
            table="operational_identity_allocators",
            value_column="next_id",
            stream=stream.value,
            current=current,
            updated_at=timestamp,
        )
        return current

    @staticmethod
    def _lock_current(
        work: VNextUnitOfWork,
        *,
        table: str,
        value_column: str,
        stream: str,
    ) -> int:
        # table/column values are closed constants from the two public methods;
        # no database or caller text is ever interpolated here.
        row = work.lock_row(
            LockRank.ALLOCATOR,
            encode_lock_key(table, stream),
            f"SELECT {value_column} FROM {table} WHERE stream = %s",
            (stream,),
        )
        if len(row) != 1:
            raise RuntimeError(f"required allocator row {stream!r} is missing")
        return require_positive_int63(row[0], field=f"{stream} allocator current")

    @staticmethod
    def _advance(
        work: VNextUnitOfWork,
        *,
        table: str,
        value_column: str,
        stream: str,
        current: int,
        updated_at: int,
    ) -> None:
        if current == INT63_MAX:
            raise AllocatorExhaustedError(f"{stream} allocator is exhausted")
        next_value = require_positive_int63(
            current + 1,
            field=f"{stream} allocator successor",
        )
        work.compare_and_swap(
            f"UPDATE {table} SET {value_column} = %s, updated_at = %s "
            f"WHERE stream = %s AND {value_column} = %s",
            (next_value, updated_at, stream, current),
            authority=f"{stream} allocator",
        )
