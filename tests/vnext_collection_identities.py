"""Reproducible source collection inputs for single-threaded fault fixtures.

Collection UUID prefixes select cleanup shards. Replaying a frozen prefix must
therefore replay those UUIDs too. Other production entropy remains untouched.
Different first-byte domains separate prefix, target and recovery identities.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from unittest.mock import patch
from uuid import UUID

import h2hdb.vnext_source_collection_repository as collections

PREFIX_NAMESPACE = b"pipeline-fault-prefix-v1"
TARGET_NAMESPACE = b"pipeline-fault-target-v1"
RECOVERY_NAMESPACE = b"pipeline-fault-recovery-v1"
PREFIX_SHARD = 0x11
TARGET_SHARD = 0x22
RECOVERY_SHARD = 0x33


class CollectionIdentitySequence:
    """Distinct UUIDs within a run, reproducible after resetting the sequence."""

    def __init__(self, namespace: bytes, *, shard: int) -> None:
        if not namespace:
            raise ValueError("collection identity namespace must be nonempty")
        if type(shard) is not int or not 0 <= shard <= 255:
            raise ValueError("collection identity shard must be one byte")
        self._prefix = bytes((shard,)) + sha256(namespace).digest()[:7]
        self._counter = 0

    def __call__(self) -> UUID:
        # UUID's variant occupies the upper two counter bits. Keeping the
        # counter below 2**62 makes distinctness exact, without a hash assumption.
        if self._counter >= 1 << 62:
            raise OverflowError("collection identity sequence exhausted")
        result = UUID(
            bytes=self._prefix + self._counter.to_bytes(8, "big"),
            version=4,
        )
        self._counter += 1
        return result


@contextmanager
def collection_identities(
    namespace: bytes, *, shard: int
) -> Iterator[CollectionIdentitySequence]:
    """Reset only collection UUID authoring; restore the real function on exit."""

    sequence = CollectionIdentitySequence(namespace, shard=shard)
    with patch.object(collections, "uuid4", sequence):
        yield sequence
