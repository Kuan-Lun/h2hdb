"""Canonical-preimage-backed file hash cache for the vNext schema."""

from __future__ import annotations

__all__ = [
    "FileHashCacheConflictError",
    "FileHashCacheHit",
    "FileHashCacheNotReadyError",
    "FileHashObservationPlan",
    "VNextHashCacheRepository",
]

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from hashlib import sha256

from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    _authorize,
)
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_maintenance_gate_repository import GateLease
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_SOURCE_DOMAIN = b"filesystem_source_identity_v1"
_FINGERPRINT_DOMAIN = b"filesystem_fingerprint_v1"
_PLAN_TOKEN = object()


class FileHashCacheConflictError(RuntimeError):
    """An immutable cache tuple or exact canonical preimage disagrees."""


class FileHashCacheNotReadyError(RuntimeError):
    """A required live upload claim or final canonical identity is absent."""


@dataclass(frozen=True, slots=True)
class FileHashObservationPlan:
    """Transaction-independent exact file-byte receipt.

    Construction consumes the byte stream once outside a database transaction.
    The private constructor token prevents a public caller from supplying a
    digest or byte count as authority.
    """

    file_sha256: bytes
    size_bytes: int
    _constructor_token: object

    def __post_init__(self) -> None:
        require_digest32(self.file_sha256, field="file_sha256")
        require_int63(self.size_bytes, field="size_bytes")
        if self._constructor_token is not _PLAN_TOKEN:
            raise TypeError("use FileHashObservationPlan.from_parts")

    @classmethod
    def from_parts(cls, parts: Iterable[bytes]) -> FileHashObservationPlan:
        digest = sha256()
        size = 0
        for part in parts:
            exact = require_bounded_bytes(
                part,
                field="file hash input part",
                maximum=INT63_MAX,
            )
            size += len(exact)
            if size > INT63_MAX:
                raise ValueError("file hash input exceeds signed-int63 bytes")
            digest.update(exact)
        return cls(digest.digest(), size, _PLAN_TOKEN)


@dataclass(frozen=True, slots=True)
class FileHashCacheHit:
    source_identity_sha256: bytes
    fingerprint_sha256: bytes
    file_sha256: bytes
    size_bytes: int
    observed_at: int
    cached_at: int
    replayed: bool = False

    def __post_init__(self) -> None:
        require_digest32(self.source_identity_sha256, field="source_identity_sha256")
        require_digest32(self.fingerprint_sha256, field="fingerprint_sha256")
        require_digest32(self.file_sha256, field="file_sha256")
        require_int63(self.size_bytes, field="size_bytes")
        require_int63(self.observed_at, field="observed_at")
        require_int63(self.cached_at, field="cached_at")
        if not isinstance(self.replayed, bool):
            raise TypeError("replayed must be bool")


class VNextHashCacheRepository:
    """Handoff, exact lookup, and bounded eviction of one cache key."""

    @staticmethod
    def handoff(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        source_plan: CanonicalValueUploadPlan,
        fingerprint_plan: CanonicalValueUploadPlan,
        file_plan: FileHashObservationPlan,
        observed_at: int,
        cached_at: int,
        now: int,
    ) -> FileHashCacheHit:
        exact_file_plan = _require_file_plan(file_plan)
        timestamp = require_int63(now, field="hash-cache handoff now")
        observed = require_int63(observed_at, field="observed_at")
        cached = require_int63(cached_at, field="cached_at")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        source = _require_plan(source_plan, domain=_SOURCE_DOMAIN, label="source")
        fingerprint = _require_plan(
            fingerprint_plan,
            domain=_FINGERPRINT_DOMAIN,
            label="fingerprint",
        )
        if source == fingerprint:
            raise FileHashCacheConflictError(
                "source and fingerprint identities must be domain-distinct"
            )

        existing = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("hash-cache", source, fingerprint),
            "SELECT o.observed_at, c.file_sha256, c.cached_at, b.size_bytes "
            "FROM operational_hash_cache_observations AS o "
            "LEFT JOIN operational_file_hash_caches AS c "
            "ON c.source_identity_sha256 = o.source_identity_sha256 "
            "AND c.fingerprint_sha256 = o.fingerprint_sha256 "
            "LEFT JOIN catalog_content_blobs AS b ON b.file_sha256 = c.file_sha256 "
            "WHERE o.source_identity_sha256 = %s "
            "AND o.fingerprint_sha256 = %s",
            (source, fingerprint),
        )
        if existing:
            if len(existing) != 4 or existing[1] is None or existing[3] is None:
                raise FileHashCacheConflictError(
                    "hash-cache observation lacks its complete materialization"
                )
            expected = (
                observed,
                exact_file_plan.file_sha256,
                cached,
                exact_file_plan.size_bytes,
            )
            if existing != expected:
                raise FileHashCacheConflictError(
                    "hash-cache replay conflicts with its exact tuple"
                )
            _require_sealed_plan(work, source_plan)
            _require_sealed_plan(work, fingerprint_plan)
            return FileHashCacheHit(
                source,
                fingerprint,
                exact_file_plan.file_sha256,
                exact_file_plan.size_bytes,
                observed,
                cached,
                True,
            )

        plans = sorted(
            ((source, source_plan), (fingerprint, fingerprint_plan)),
            key=lambda value: value[0],
        )
        for value, plan in plans:
            claim = work.lock_row(
                LockRank.CHECKPOINT,
                encode_lock_key("canonical-upload", generation, value),
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (generation, value),
            )
            if claim != (generation, value):
                raise FileHashCacheNotReadyError(
                    "exact canonical upload claim is absent"
                )
            _require_sealed_plan(work, plan)

        blob = work.lock_row(
            LockRank.HEAD,
            encode_lock_key("content-blob", exact_file_plan.file_sha256),
            "SELECT size_bytes FROM catalog_content_blobs WHERE file_sha256 = %s",
            (exact_file_plan.file_sha256,),
        )
        if blob:
            if blob != (exact_file_plan.size_bytes,):
                raise FileHashCacheConflictError(
                    "file hash collides with another byte count"
                )
        else:
            work.connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, %s)",
                (exact_file_plan.file_sha256, exact_file_plan.size_bytes),
            )
        work.connector.execute(
            "INSERT INTO operational_hash_cache_observations "
            "(source_identity_sha256, fingerprint_sha256, observed_at) "
            "VALUES (%s, %s, %s)",
            (source, fingerprint, observed),
        )
        work.connector.execute(
            "INSERT INTO operational_file_hash_caches "
            "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
            "VALUES (%s, %s, %s, %s)",
            (source, fingerprint, exact_file_plan.file_sha256, cached),
        )
        for value, _plan in plans:
            affected = work.connector.execute_affected(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (generation, value),
            )
            if affected != 1:
                raise FileHashCacheNotReadyError(
                    "canonical claim changed before hash-cache handoff"
                )
        return FileHashCacheHit(
            source,
            fingerprint,
            exact_file_plan.file_sha256,
            exact_file_plan.size_bytes,
            observed,
            cached,
        )

    @staticmethod
    def lookup_exact(
        work: VNextUnitOfWork,
        *,
        source_plan: CanonicalValueUploadPlan,
        fingerprint_plan: CanonicalValueUploadPlan,
    ) -> FileHashCacheHit | None:
        source = _require_plan(source_plan, domain=_SOURCE_DOMAIN, label="source")
        fingerprint = _require_plan(
            fingerprint_plan,
            domain=_FINGERPRINT_DOMAIN,
            label="fingerprint",
        )
        row = work.connector.fetch_one(
            "SELECT o.observed_at, c.file_sha256, c.cached_at, b.size_bytes "
            "FROM operational_hash_cache_observations AS o "
            "JOIN operational_file_hash_caches AS c "
            "ON c.source_identity_sha256 = o.source_identity_sha256 "
            "AND c.fingerprint_sha256 = o.fingerprint_sha256 "
            "JOIN catalog_content_blobs AS b ON b.file_sha256 = c.file_sha256 "
            "WHERE o.source_identity_sha256 = %s "
            "AND o.fingerprint_sha256 = %s",
            (source, fingerprint),
        )
        if not row:
            return None
        if len(row) != 4:
            raise FileHashCacheConflictError("hash-cache hit has an invalid shape")
        _compare_streamed_preimage(work, source_plan)
        _compare_streamed_preimage(work, fingerprint_plan)
        return FileHashCacheHit(
            source,
            fingerprint,
            require_digest32(row[1], field="cached file_sha256"),
            require_int63(row[3], field="cached size_bytes"),
            require_int63(row[0], field="observed_at"),
            require_int63(row[2], field="cached_at"),
            True,
        )


def _require_plan(
    plan: CanonicalValueUploadPlan,
    *,
    domain: bytes,
    label: str,
) -> bytes:
    if not isinstance(plan, CanonicalValueUploadPlan):
        raise TypeError(f"{label}_plan must be a CanonicalValueUploadPlan")
    receipt = plan.preparation_receipt
    if receipt.digest_domain != domain:
        raise FileHashCacheConflictError(f"{label} plan uses the wrong digest domain")
    if (
        receipt.value_sha256 != plan.value_sha256
        or receipt.byte_count != plan.byte_count
    ):
        raise FileHashCacheConflictError(f"{label} preparation receipt changed")
    return require_digest32(receipt.value_sha256, field=f"{label} identity")


def _require_file_plan(value: object) -> FileHashObservationPlan:
    if type(value) is not FileHashObservationPlan:
        raise TypeError("file_plan must be an exact FileHashObservationPlan")
    assert isinstance(value, FileHashObservationPlan)
    value.__post_init__()
    return value


def _require_sealed_plan(
    work: VNextUnitOfWork,
    plan: CanonicalValueUploadPlan,
) -> None:
    tree = plan.tree_receipt
    row = work.connector.fetch_one(
        "SELECT a.digest_domain, a.byte_count, i.root_page_sha256 "
        "FROM catalog_canonical_value_allocations AS a "
        "JOIN catalog_canonical_value_identities AS i "
        "ON i.value_sha256 = a.value_sha256 WHERE a.value_sha256 = %s",
        (plan.value_sha256,),
    )
    expected = (plan.digest_domain, plan.byte_count, tree.root_page_sha256)
    if row != expected:
        raise FileHashCacheNotReadyError(
            "canonical hash-cache preimage is not exact and sealed"
        )


class _StreamComparator:
    def __init__(self, expected: Iterator[bytes]) -> None:
        self._expected = expected
        self._buffer = bytearray()
        self._ended = False

    def consume(self, actual: bytes) -> None:
        exact = require_bounded_bytes(
            actual,
            field="canonical streamed preimage part",
            maximum=64 * 1024,
        )
        needed = len(exact)
        while len(self._buffer) < needed and not self._ended:
            try:
                self._buffer.extend(next(self._expected))
            except StopIteration:
                self._ended = True
        if bytes(self._buffer[:needed]) != exact:
            raise FileHashCacheConflictError(
                "canonical cache preimage differs from the exact lookup input"
            )
        del self._buffer[:needed]

    def finish(self) -> None:
        if not self._ended:
            try:
                self._buffer.extend(next(self._expected))
            except StopIteration:
                self._ended = True
        if self._buffer or not self._ended:
            raise FileHashCacheConflictError(
                "canonical cache preimage has trailing lookup bytes"
            )


def _compare_streamed_preimage(
    work: VNextUnitOfWork,
    plan: CanonicalValueUploadPlan,
) -> None:
    comparator = _StreamComparator(plan.iter_payload_parts())
    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=plan.value_sha256,
            consume_provisional=comparator.consume,
        )
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise FileHashCacheConflictError(
            "canonical cache preimage failed page-tree validation"
        ) from error
    comparator.finish()
    if (
        receipt.digest_domain != plan.digest_domain
        or receipt.byte_count != plan.byte_count
    ):
        raise FileHashCacheConflictError(
            "canonical cache preimage receipt disagrees with lookup input"
        )
