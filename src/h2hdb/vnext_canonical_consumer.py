"""Transaction-local canonical authority for bounded consumer handoff.

The caller owns the candidate fence and transaction. All referenced identities
are loaded before writes; generation claims remain until the caller has written
every durable child reference. Nothing here survives the transaction or acts as
a cross-transaction verification cache.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .vnext_canonical_value_family import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    load_sealed_value_identities,
)
from .vnext_domains import require_bounded_bytes, require_digest32, require_int63
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_KEY_PAGE = 128
_MAX_REFERENCES = 128 * 7


@dataclass(frozen=True, slots=True)
class CanonicalConsumerValue:
    value_sha256: bytes
    digest_domain: bytes
    byte_count: int
    first_consumer: bytes

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="consumer value_sha256")
        require_bounded_bytes(
            self.digest_domain, field="consumer digest_domain", minimum=1, maximum=64
        )
        require_int63(self.byte_count, field="consumer byte_count")
        require_bounded_bytes(
            self.first_consumer, field="first consumer", minimum=1, maximum=2048
        )


class CanonicalConsumerBatch:
    """One bounded, validated mapping owned by exactly one managed transaction."""

    def __init__(
        self,
        work: VNextUnitOfWork,
        *,
        generation: int,
        values: Sequence[CanonicalConsumerValue],
        consumers: Sequence[bytes],
    ) -> None:
        if len(values) > _MAX_REFERENCES or len(consumers) > _KEY_PAGE:
            raise ValueError("canonical consumer batch exceeds its bounded page")
        self._work = work
        self._generation = require_int63(generation, field="consumer generation")
        self._values: dict[bytes, CanonicalConsumerValue] = {}
        self._consumed: set[bytes] = set()
        self._finished = False
        for value in values:
            value.__post_init__()
            previous = self._values.setdefault(value.value_sha256, value)
            if previous != value:
                raise CanonicalValueCollisionError("canonical consumer plan collides")
        keys = tuple(sorted(self._values))
        for offset in range(0, len(keys), _KEY_PAGE):
            page = keys[offset : offset + _KEY_PAGE]
            sealed = load_sealed_value_identities(work.connector, value_sha256s=page)
            for key in page:
                expected = self._values[key]
                actual = sealed.get(key)
                if actual is None or (
                    actual.digest_domain != expected.digest_domain
                    or actual.byte_count != expected.byte_count
                ):
                    raise CanonicalValueNotReadyError(
                        "canonical consumer value is not exactly sealed"
                    )
        consumer_keys = set(consumers)
        self._claims = tuple(
            key for key in keys if self._values[key].first_consumer in consumer_keys
        )
        for page in self._claim_pages():
            rows = work.lock_rows(
                LockRank.CHILD,
                tuple(
                    encode_lock_key("publication-canonical-consumer", generation, key)
                    for key in page
                ),
                "SELECT generation, value_sha256 FROM operational_canonical_value_uploads "
                f"WHERE generation = %s AND value_sha256 IN ({', '.join('%s' for _ in page)}) "
                "ORDER BY value_sha256",
                (generation, *page),
            )
            if tuple(rows) != tuple((generation, key) for key in page):
                raise CanonicalValueNotReadyError(
                    "first canonical consumer requires its exact generation upload claim"
                )

    def require(self, value: bytes, *, expected_domain: bytes, consumer: bytes) -> None:
        if self._finished:
            raise CanonicalValueCollisionError("canonical consumer batch is finished")
        planned = self._values.get(value)
        if planned is None or planned.digest_domain != expected_domain:
            raise CanonicalValueCollisionError("canonical consumer plan domain differs")
        if planned.first_consumer == consumer:
            self._consumed.add(value)

    def finish(self) -> None:
        """Release claims only after all child references exist in the same tx."""
        if self._finished or self._consumed != set(self._claims):
            raise CanonicalValueCollisionError(
                "canonical handoff did not consume its exact planned claim set"
            )
        for page in self._claim_pages():
            deleted = self._work.connector.execute_affected(
                "DELETE FROM operational_canonical_value_uploads "
                f"WHERE generation = %s AND value_sha256 IN ({', '.join('%s' for _ in page)})",
                (self._generation, *page),
            )
            if deleted != len(page):
                raise CanonicalValueCollisionError(
                    "canonical upload claims changed during consumer handoff"
                )
        self._finished = True

    def _claim_pages(self) -> tuple[tuple[bytes, ...], ...]:
        return tuple(
            self._claims[offset : offset + _KEY_PAGE]
            for offset in range(0, len(self._claims), _KEY_PAGE)
        )
