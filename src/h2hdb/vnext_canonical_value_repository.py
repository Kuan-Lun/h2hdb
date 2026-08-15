"""Bounded canonical-value persistence for the greenfield vNext catalog.

The database stores an owner-prefixed Merkle-style page tree.  This module is
the production refinement of that contract: an arbitrary input is first
spooled and hashed twice outside a transaction, while every database mutation
touches at most one 64 KiB page and 256 child descriptors.  The upload claim is
retained by sealing and is released only by the first durable consumer.

Callers own the already-open transaction and must pass the exact live shared
maintenance-gate lease and ingest turn to every write method.
"""

from __future__ import annotations

__all__ = [
    "CanonicalValueAllocation",
    "CanonicalValueCollisionError",
    "CanonicalValueNotReadyError",
    "CanonicalValuePreparationReceipt",
    "CanonicalValueReadReceipt",
    "CanonicalValueRepository",
    "CanonicalValueTreeReceipt",
    "CanonicalValueUploadPlan",
    "PreparedCanonicalPage",
]

from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass, field
from tempfile import TemporaryFile
from typing import Any, BinaryIO

from .vnext_domains import (
    INT63_MAX,
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
)
from .vnext_identity import (
    CANONICAL_VALUE_BRANCH_CAPACITY,
    CANONICAL_VALUE_CHUNK_BYTES,
    CanonicalValueBranchEntry,
    CanonicalValueChunk,
    CanonicalValuePage,
    GalleryObservationNodeKind,
    SourceRootValidationReceipt,
    canonical_value_digest_parts,
    canonical_value_page_digest,
    decode_canonical_value_page,
    encode_canonical_value_page,
    validate_source_root_parts,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_STREAM_READ_BYTES = 64 * 1024
_DESCRIPTOR_BYTES = 40
_PLAN_CONSTRUCTOR_TOKEN = object()


class CanonicalValueCollisionError(RuntimeError):
    """An immutable digest, page, position, or consumer tuple disagrees."""


class CanonicalValueNotReadyError(RuntimeError):
    """Required upload, page-tree, gate, or ingest authority is absent."""


@dataclass(frozen=True, slots=True)
class CanonicalValueAllocation:
    value_sha256: bytes
    digest_domain: bytes
    byte_count: int
    allocated_at: int

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_ascii_bytes(
            self.digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        require_int63(self.byte_count, field="byte_count")
        require_int63(self.allocated_at, field="allocated_at")


@dataclass(frozen=True, slots=True)
class CanonicalValuePreparationReceipt:
    """Transaction-independent proof of exact input count and digest EOF."""

    value_sha256: bytes
    digest_domain: bytes
    byte_count: int

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_ascii_bytes(
            self.digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        require_int63(self.byte_count, field="byte_count")


@dataclass(frozen=True, slots=True)
class CanonicalValueTreeReceipt:
    """Transaction-independent exact-EOF receipt for a deterministic tree."""

    value_sha256: bytes
    root_page_sha256: bytes
    byte_count: int
    root_level: int

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_digest32(self.root_page_sha256, field="root_page_sha256")
        require_int63(self.byte_count, field="byte_count")
        require_int63(self.root_level, field="root_level")


@dataclass(frozen=True, slots=True)
class PreparedCanonicalPage:
    """One bounded page issued by a particular replayable upload plan."""

    page_sha256: bytes
    page_bytes: bytes
    _plan_capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_digest32(self.page_sha256, field="page_sha256")
        require_bounded_bytes(
            self.page_bytes,
            field="page_bytes",
            minimum=1,
            maximum=64 * 1024,
        )
        if canonical_value_page_digest(self.page_bytes) != self.page_sha256:
            raise ValueError("page_sha256 does not match page_bytes")


@dataclass(frozen=True, slots=True)
class CanonicalValueReadReceipt:
    value_sha256: bytes
    digest_domain: bytes
    byte_count: int
    root_page_sha256: bytes

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_ascii_bytes(
            self.digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        require_int63(self.byte_count, field="byte_count")
        require_digest32(self.root_page_sha256, field="root_page_sha256")


class CanonicalValueUploadPlan:
    """Replayable disk-backed two-pass canonical input and page issuer.

    ``from_parts`` consumes the caller iterable exactly once into a temporary
    spool (pass one) and then recomputes the domain-separated value digest from
    that exact spool (pass two).  Tree construction retains only one bounded
    page plus fixed-width child descriptors in temporary files.
    """

    def __init__(
        self,
        *,
        digest_domain: bytes,
        byte_count: int,
        value_sha256: bytes,
        payload: BinaryIO,
        source_root_receipt: SourceRootValidationReceipt | None,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PLAN_CONSTRUCTOR_TOKEN:
            raise TypeError("use CanonicalValueUploadPlan.from_parts")
        self.digest_domain = require_ascii_bytes(
            digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        self.byte_count = require_int63(byte_count, field="byte_count")
        self.value_sha256 = require_digest32(value_sha256, field="value_sha256")
        self._payload = payload
        self._capability = object()
        self._closed = False
        self._preparation_receipt = CanonicalValuePreparationReceipt(
            self.value_sha256,
            self.digest_domain,
            self.byte_count,
        )
        self._source_root_receipt = source_root_receipt
        self._tree_receipt: CanonicalValueTreeReceipt | None = None

    @classmethod
    def from_parts(
        cls,
        digest_domain: str,
        parts: Iterable[bytes],
    ) -> CanonicalValueUploadPlan:
        domain = require_ascii_bytes(
            digest_domain.encode("ascii", errors="strict"),
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        payload = TemporaryFile(mode="w+b")
        byte_count = 0
        try:
            for part in parts:
                exact = require_bounded_bytes(
                    part,
                    field="canonical value part",
                    maximum=INT63_MAX,
                )
                byte_count += len(exact)
                if byte_count > INT63_MAX:
                    raise ValueError("canonical value exceeds signed-int63 bytes")
                if payload.write(exact) != len(exact):
                    raise OSError("canonical payload spool accepted a partial write")
            payload.flush()

            def replay() -> Iterator[bytes]:
                payload.seek(0)
                while True:
                    chunk = payload.read(_STREAM_READ_BYTES)
                    if not chunk:
                        return
                    yield chunk

            value_sha256 = canonical_value_digest_parts(
                domain.decode("ascii"),
                byte_count,
                replay(),
            )
            source_root_receipt = (
                validate_source_root_parts(replay())
                if domain == b"source_root_v1"
                else None
            )
            return cls(
                digest_domain=domain,
                byte_count=byte_count,
                value_sha256=value_sha256,
                payload=payload,
                source_root_receipt=source_root_receipt,
                _constructor_token=_PLAN_CONSTRUCTOR_TOKEN,
            )
        except BaseException:
            payload.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._payload.close()

    def __enter__(self) -> CanonicalValueUploadPlan:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def root_page_sha256(self) -> bytes:
        return self.tree_receipt.root_page_sha256

    @property
    def preparation_receipt(self) -> CanonicalValuePreparationReceipt:
        return self._preparation_receipt

    @property
    def source_root_receipt(self) -> SourceRootValidationReceipt:
        if self._source_root_receipt is None:
            raise CanonicalValueNotReadyError(
                "upload plan has no exact source-root framing receipt"
            )
        return self._source_root_receipt

    @property
    def tree_receipt(self) -> CanonicalValueTreeReceipt:
        if self._tree_receipt is None:
            raise CanonicalValueNotReadyError(
                "page iteration must reach exact EOF before the root is authority"
            )
        return self._tree_receipt

    @property
    def expected_root_level(self) -> int:
        pages = max(
            1,
            (self.byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1)
            // CANONICAL_VALUE_CHUNK_BYTES,
        )
        level = 0
        while pages > 1:
            pages = (
                pages + CANONICAL_VALUE_BRANCH_CAPACITY - 1
            ) // CANONICAL_VALUE_BRANCH_CAPACITY
            level += 1
        return level

    def iter_payload_parts(self) -> Iterator[bytes]:
        """Replay the exact spool in bounded chunks."""

        self._require_open()
        self._payload.seek(0)
        remaining = self.byte_count
        while remaining:
            chunk = self._payload.read(min(_STREAM_READ_BYTES, remaining))
            if not chunk:
                raise CanonicalValueNotReadyError(
                    "canonical payload spool is truncated"
                )
            remaining -= len(chunk)
            yield chunk
        if self._payload.read(1):
            raise CanonicalValueNotReadyError(
                "canonical payload spool grew after hashing"
            )

    def iter_pages(self) -> Generator[PreparedCanonicalPage, None, bytes]:
        """Issue deterministic leaves then branches using bounded memory.

        The generator's return value and ``root_page_sha256`` become available
        only after every page has been issued.  Aborting and replaying the
        generator is safe: all page bytes and positions are deterministic.
        """

        self._require_open()
        self._tree_receipt = None
        current = TemporaryFile(mode="w+b")
        current_count = max(
            1,
            (self.byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1)
            // CANONICAL_VALUE_CHUNK_BYTES,
        )
        try:
            for position in range(current_count):
                offset = position * CANONICAL_VALUE_CHUNK_BYTES
                remaining = self.byte_count - offset
                chunk_size = min(CANONICAL_VALUE_CHUNK_BYTES, max(remaining, 0))
                self._payload.seek(offset)
                chunk = self._payload.read(chunk_size)
                if len(chunk) != chunk_size:
                    raise CanonicalValueNotReadyError(
                        "canonical payload spool is truncated"
                    )
                leaf_entries: tuple[CanonicalValueChunk, ...]
                if chunk_size:
                    leaf_entries = (CanonicalValueChunk(offset, chunk),)
                else:
                    leaf_entries = ()
                page = CanonicalValuePage(
                    self.value_sha256,
                    GalleryObservationNodeKind.LEAF,
                    0,
                    position,
                    chunk_size,
                    leaf_entries,
                )
                prepared = self._prepare_page(page)
                _write_descriptor(current, prepared.page_sha256, chunk_size)
                yield prepared

            level = 1
            while current_count > 1:
                current.seek(0)
                following = TemporaryFile(mode="w+b")
                following_count = (
                    current_count + CANONICAL_VALUE_BRANCH_CAPACITY - 1
                ) // CANONICAL_VALUE_BRANCH_CAPACITY
                try:
                    for position in range(following_count):
                        branch_entries: list[CanonicalValueBranchEntry] = []
                        for _ in range(
                            min(
                                CANONICAL_VALUE_BRANCH_CAPACITY,
                                current_count
                                - position * CANONICAL_VALUE_BRANCH_CAPACITY,
                            )
                        ):
                            child_sha256, child_count = _read_descriptor(current)
                            branch_entries.append(
                                CanonicalValueBranchEntry(
                                    child_sha256,
                                    child_count,
                                )
                            )
                        subtree_count = sum(
                            entry.child_subtree_byte_count for entry in branch_entries
                        )
                        page = CanonicalValuePage(
                            self.value_sha256,
                            GalleryObservationNodeKind.BRANCH,
                            level,
                            position,
                            subtree_count,
                            tuple(branch_entries),
                        )
                        prepared = self._prepare_page(page)
                        _write_descriptor(
                            following,
                            prepared.page_sha256,
                            subtree_count,
                        )
                        yield prepared
                except BaseException:
                    following.close()
                    raise
                current.close()
                current = following
                current_count = following_count
                level += 1

            current.seek(0)
            root_page_sha256, root_count = _read_descriptor(current)
            if root_count != self.byte_count or current.read(1):
                raise CanonicalValueNotReadyError(
                    "canonical page plan did not terminate at one exact root"
                )
            final_digest = canonical_value_digest_parts(
                self.digest_domain.decode("ascii"),
                self.byte_count,
                self.iter_payload_parts(),
            )
            if final_digest != self.value_sha256:
                raise CanonicalValueCollisionError(
                    "canonical payload changed while its pages were issued"
                )
            if self._source_root_receipt is not None:
                final_source_root = validate_source_root_parts(
                    self.iter_payload_parts()
                )
                if final_source_root != self._source_root_receipt:
                    raise CanonicalValueCollisionError(
                        "source-root frame changed while its pages were issued"
                    )
            self._tree_receipt = CanonicalValueTreeReceipt(
                self.value_sha256,
                root_page_sha256,
                self.byte_count,
                self.expected_root_level,
            )
            return root_page_sha256
        finally:
            current.close()

    def _prepare_page(self, page: CanonicalValuePage) -> PreparedCanonicalPage:
        page_bytes = encode_canonical_value_page(page)
        return PreparedCanonicalPage(
            canonical_value_page_digest(page_bytes),
            page_bytes,
            self._capability,
        )

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("canonical value upload plan is closed")


class CanonicalValueRepository:
    """Transaction-local canonical-value writer and streaming validator."""

    @staticmethod
    def allocate(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        plan: CanonicalValueUploadPlan,
        now: int,
    ) -> CanonicalValueAllocation:
        exact_plan = _require_upload_plan(plan)
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        connector = work.connector
        preparation = exact_plan.preparation_receipt
        _require_exact(
            "canonical preparation receipt",
            (
                preparation.value_sha256,
                preparation.digest_domain,
                preparation.byte_count,
            ),
            (
                exact_plan.value_sha256,
                exact_plan.digest_domain,
                exact_plan.byte_count,
            ),
        )
        if exact_plan.digest_domain == b"source_root_v1":
            root_receipt = exact_plan.source_root_receipt
            if root_receipt.payload_byte_count != exact_plan.byte_count:
                raise CanonicalValueCollisionError(
                    "source-root frame does not reach exact EOF"
                )

        if not connector.fetch_one(
            "SELECT digest_domain FROM catalog_canonical_digest_policies "
            "WHERE digest_domain = %s",
            (exact_plan.digest_domain,),
        ):
            raise CanonicalValueNotReadyError(
                "canonical digest domain is not registered"
            )

        if exact_plan.digest_domain != b"source_root_v1" and not connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (generation,),
        ):
            raise CanonicalValueNotReadyError(
                "non-root canonical upload requires a durable build generation"
            )

        existing = connector.fetch_one(
            "SELECT digest_domain, byte_count, allocated_at "
            "FROM catalog_canonical_value_allocations WHERE value_sha256 = %s",
            (exact_plan.value_sha256,),
        )
        if existing:
            _require_exact(
                "canonical allocation",
                existing[:2],
                (exact_plan.digest_domain, exact_plan.byte_count),
            )
            allocated_at = require_int63(existing[2], field="allocated_at")
        else:
            connector.execute(
                "INSERT INTO catalog_canonical_value_allocations "
                "(value_sha256, digest_domain, byte_count, allocated_at) "
                "VALUES (%s, %s, %s, %s)",
                (
                    exact_plan.value_sha256,
                    exact_plan.digest_domain,
                    exact_plan.byte_count,
                    timestamp,
                ),
            )
            allocated_at = timestamp

        claim = connector.fetch_one(
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, exact_plan.value_sha256),
        )
        if claim:
            _require_exact(
                "canonical upload claim",
                claim,
                (generation, exact_plan.value_sha256),
            )
        else:
            connector.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (%s, %s)",
                (generation, exact_plan.value_sha256),
            )
        return CanonicalValueAllocation(
            exact_plan.value_sha256,
            exact_plan.digest_domain,
            exact_plan.byte_count,
            allocated_at,
        )

    @staticmethod
    def put_page(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        plan: CanonicalValueUploadPlan,
        prepared_page: PreparedCanonicalPage,
        now: int,
    ) -> bytes:
        exact_plan = _require_upload_plan(plan)
        exact_page = _require_prepared_page(prepared_page, plan=exact_plan)
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        page = decode_canonical_value_page(exact_page.page_bytes)
        if page.owner_value_sha256 != exact_plan.value_sha256:
            raise CanonicalValueCollisionError("canonical page has the wrong owner")
        _validate_page_shape(page, exact_plan.byte_count)
        connector = work.connector
        _lock_claim(work, generation, exact_plan.value_sha256)
        allocation = work.lock_row(
            LockRank.HEAD,
            encode_lock_key("canonical-allocation", exact_plan.value_sha256),
            "SELECT digest_domain, byte_count FROM "
            "catalog_canonical_value_allocations WHERE value_sha256 = %s",
            (exact_plan.value_sha256,),
        )
        _require_exact(
            "canonical allocation",
            allocation,
            (exact_plan.digest_domain, exact_plan.byte_count),
        )

        if page.node_kind is GalleryObservationNodeKind.LEAF:
            _require_exact_leaf_source(exact_plan, page)
        else:
            _validate_branch_children(
                connector,
                page,
                byte_count=exact_plan.byte_count,
            )

        existing_page = connector.fetch_one(
            "SELECT value_sha256, page_bytes FROM catalog_canonical_value_pages "
            "WHERE page_sha256 = %s",
            (exact_page.page_sha256,),
        )
        if existing_page:
            _require_exact(
                "canonical page digest",
                existing_page,
                (exact_plan.value_sha256, exact_page.page_bytes),
            )
        else:
            connector.execute(
                "INSERT INTO catalog_canonical_value_pages "
                "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
                (
                    exact_page.page_sha256,
                    exact_plan.value_sha256,
                    exact_page.page_bytes,
                ),
            )

        descriptor_tuple = (
            exact_page.page_sha256,
            exact_plan.value_sha256,
            page.level,
            page.page_position,
            page.subtree_byte_count,
        )
        existing_descriptor = connector.fetch_one(
            "SELECT page_sha256, value_sha256, level, page_position, "
            "subtree_item_count FROM catalog_canonical_value_page_descriptors "
            "WHERE page_sha256 = %s",
            (exact_page.page_sha256,),
        )
        by_position = connector.fetch_one(
            "SELECT page_sha256, value_sha256, level, page_position, "
            "subtree_item_count FROM catalog_canonical_value_page_descriptors "
            "WHERE value_sha256 = %s AND level = %s AND page_position = %s",
            (exact_plan.value_sha256, page.level, page.page_position),
        )
        if existing_descriptor:
            _require_exact(
                "canonical page descriptor", existing_descriptor, descriptor_tuple
            )
        elif by_position:
            _require_exact("canonical page position", by_position, descriptor_tuple)
        else:
            connector.execute(
                "INSERT INTO catalog_canonical_value_page_descriptors "
                "(page_sha256, value_sha256, level, page_position, "
                "subtree_item_count) VALUES (%s, %s, %s, %s, %s)",
                descriptor_tuple,
            )

        if page.node_kind is GalleryObservationNodeKind.BRANCH:
            for position, entry in enumerate(page.entries):
                assert isinstance(entry, CanonicalValueBranchEntry)
                edge = connector.fetch_one(
                    "SELECT child_sha256, parent_sha256, position "
                    "FROM catalog_canonical_value_page_parents "
                    "WHERE child_sha256 = %s",
                    (entry.child_page_sha256,),
                )
                edge_by_position = connector.fetch_one(
                    "SELECT child_sha256, parent_sha256, position "
                    "FROM catalog_canonical_value_page_parents "
                    "WHERE parent_sha256 = %s AND position = %s",
                    (exact_page.page_sha256, position),
                )
                expected_edge = (
                    entry.child_page_sha256,
                    exact_page.page_sha256,
                    position,
                )
                if edge:
                    _require_exact("canonical parent edge", edge, expected_edge)
                elif edge_by_position:
                    _require_exact(
                        "canonical parent position",
                        edge_by_position,
                        expected_edge,
                    )
                else:
                    connector.execute(
                        "INSERT INTO catalog_canonical_value_page_parents "
                        "(child_sha256, parent_sha256, position) "
                        "VALUES (%s, %s, %s)",
                        expected_edge,
                    )
        return exact_page.page_sha256

    @staticmethod
    def seal(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        plan: CanonicalValueUploadPlan,
        now: int,
    ) -> bytes:
        exact_plan = _require_upload_plan(plan)
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        tree_receipt = _require_tree_receipt(exact_plan.tree_receipt)
        root_sha256 = tree_receipt.root_page_sha256
        connector = work.connector
        _lock_claim(work, generation, exact_plan.value_sha256)
        allocation = work.lock_row(
            LockRank.HEAD,
            encode_lock_key("canonical-allocation", exact_plan.value_sha256),
            "SELECT digest_domain, byte_count FROM "
            "catalog_canonical_value_allocations WHERE value_sha256 = %s",
            (exact_plan.value_sha256,),
        )
        _require_exact(
            "canonical allocation",
            allocation,
            (exact_plan.digest_domain, exact_plan.byte_count),
        )
        _require_exact(
            "canonical tree receipt",
            (
                tree_receipt.value_sha256,
                tree_receipt.byte_count,
                tree_receipt.root_level,
            ),
            (
                exact_plan.value_sha256,
                exact_plan.byte_count,
                exact_plan.expected_root_level,
            ),
        )

        root_row = connector.fetch_one(
            "SELECT p.value_sha256, p.page_bytes, d.level, d.page_position, "
            "d.subtree_item_count FROM catalog_canonical_value_pages p "
            "JOIN catalog_canonical_value_page_descriptors d "
            "ON d.page_sha256 = p.page_sha256 WHERE p.page_sha256 = %s",
            (root_sha256,),
        )
        if len(root_row) != 5:
            raise CanonicalValueNotReadyError("canonical root page is not complete")
        page = decode_canonical_value_page(root_row[1])
        if canonical_value_page_digest(root_row[1]) != root_sha256:
            raise CanonicalValueCollisionError("canonical root digest disagrees")
        _validate_page_shape(page, exact_plan.byte_count)
        _require_exact(
            "canonical root descriptor",
            (
                root_row[0],
                root_row[2],
                root_row[3],
                root_row[4],
                page.level,
                page.page_position,
                page.subtree_byte_count,
            ),
            (
                exact_plan.value_sha256,
                exact_plan.expected_root_level,
                0,
                exact_plan.byte_count,
                exact_plan.expected_root_level,
                0,
                exact_plan.byte_count,
            ),
        )
        if page.owner_value_sha256 != exact_plan.value_sha256:
            raise CanonicalValueCollisionError("canonical root owner disagrees")
        if connector.fetch_one(
            "SELECT parent_sha256 FROM catalog_canonical_value_page_parents "
            "WHERE child_sha256 = %s",
            (root_sha256,),
        ):
            raise CanonicalValueCollisionError("canonical root has a parent")

        identity = connector.fetch_one(
            "SELECT value_sha256, root_page_sha256 "
            "FROM catalog_canonical_value_identities WHERE value_sha256 = %s",
            (exact_plan.value_sha256,),
        )
        reverse = connector.fetch_one(
            "SELECT value_sha256, root_page_sha256 "
            "FROM catalog_canonical_value_identities WHERE root_page_sha256 = %s",
            (root_sha256,),
        )
        expected = (exact_plan.value_sha256, root_sha256)
        if identity:
            _require_exact("canonical identity", identity, expected)
        elif reverse:
            _require_exact("canonical root identity", reverse, expected)
        else:
            connector.execute(
                "INSERT INTO catalog_canonical_value_identities "
                "(value_sha256, root_page_sha256) VALUES (%s, %s)",
                expected,
            )
        # Deliberately retain operational_canonical_value_uploads.  Only the
        # first durable external consumer may remove this generation claim.
        return exact_plan.value_sha256

    @staticmethod
    def stream_and_validate(
        work: VNextUnitOfWork,
        *,
        value_sha256: bytes,
        consume_provisional: Callable[[bytes], None],
    ) -> CanonicalValueReadReceipt:
        """Stream bounded chunks, then return authority only after exact EOF.

        ``consume_provisional`` must not expose or commit its side effects until
        this method returns a receipt.  The caller keeps one read transaction
        open for the entire call.  Rows are addressed only by primary keys and
        each page has at most 256 children.
        """

        value = require_digest32(value_sha256, field="value_sha256")
        row = work.connector.fetch_one(
            "SELECT a.digest_domain, a.byte_count, i.root_page_sha256 "
            "FROM catalog_canonical_value_identities i "
            "JOIN catalog_canonical_value_allocations a "
            "ON a.value_sha256 = i.value_sha256 WHERE i.value_sha256 = %s",
            (value,),
        )
        if len(row) != 3:
            raise CanonicalValueNotReadyError("canonical identity is not sealed")
        domain = require_ascii_bytes(
            row[0], field="digest_domain", minimum=1, maximum=64
        )
        byte_count = require_int63(row[1], field="byte_count")
        root = require_digest32(row[2], field="root_page_sha256")
        root_level = _expected_root_level(byte_count)
        consumed = 0

        def payload_parts() -> Iterator[bytes]:
            nonlocal consumed
            for chunk in _iter_tree_payload(
                work,
                owner=value,
                page_sha256=root,
                expected_level=root_level,
                expected_position=0,
                expected_byte_offset=0,
                total_byte_count=byte_count,
            ):
                consumed += len(chunk)
                if consumed > byte_count:
                    raise CanonicalValueCollisionError(
                        "canonical page tree exceeds allocation byte_count"
                    )
                consume_provisional(chunk)
                yield chunk

        recomputed = canonical_value_digest_parts(
            domain.decode("ascii"),
            byte_count,
            payload_parts(),
        )
        if consumed != byte_count or recomputed != value:
            raise CanonicalValueCollisionError(
                "canonical tree does not recompute its value identity"
            )
        return CanonicalValueReadReceipt(value, domain, byte_count, root)


def _require_upload_plan(value: object) -> CanonicalValueUploadPlan:
    """Revalidate every mutable plan scalar before it can reach SQL."""

    if type(value) is not CanonicalValueUploadPlan:
        raise TypeError("plan must be an exact CanonicalValueUploadPlan")
    assert isinstance(value, CanonicalValueUploadPlan)
    value._require_open()
    digest = require_digest32(value.value_sha256, field="plan value_sha256")
    domain = require_ascii_bytes(
        value.digest_domain,
        field="plan digest_domain",
        minimum=1,
        maximum=64,
    )
    count = require_int63(value.byte_count, field="plan byte_count")
    receipt = value.preparation_receipt
    if type(receipt) is not CanonicalValuePreparationReceipt:
        raise TypeError("plan preparation receipt has the wrong type")
    receipt.__post_init__()
    _require_exact(
        "canonical upload plan",
        (digest, domain, count),
        (receipt.value_sha256, receipt.digest_domain, receipt.byte_count),
    )
    return value


def _require_prepared_page(
    value: object,
    *,
    plan: CanonicalValueUploadPlan,
) -> PreparedCanonicalPage:
    if type(value) is not PreparedCanonicalPage:
        raise TypeError("prepared_page must be an exact PreparedCanonicalPage")
    assert isinstance(value, PreparedCanonicalPage)
    value.__post_init__()
    if value._plan_capability is not plan._capability:
        raise CanonicalValueCollisionError(
            "page was not issued by this exact replayable upload plan"
        )
    return value


def _require_tree_receipt(value: object) -> CanonicalValueTreeReceipt:
    if type(value) is not CanonicalValueTreeReceipt:
        raise TypeError("tree receipt has the wrong type")
    assert isinstance(value, CanonicalValueTreeReceipt)
    value.__post_init__()
    return value


def _write_descriptor(stream: BinaryIO, page_sha256: bytes, byte_count: int) -> None:
    digest = require_digest32(page_sha256, field="page_sha256")
    count = require_int63(byte_count, field="byte_count").to_bytes(8, "big")
    if stream.write(digest) != len(digest) or stream.write(count) != len(count):
        raise OSError("canonical descriptor spool accepted a partial write")


def _read_descriptor(stream: BinaryIO) -> tuple[bytes, int]:
    record = stream.read(_DESCRIPTOR_BYTES)
    if len(record) != _DESCRIPTOR_BYTES:
        raise CanonicalValueNotReadyError("temporary page descriptor is truncated")
    return record[:32], int.from_bytes(record[32:], "big")


def _authorize(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    live_gate = MaintenanceGateRepository.lock_and_require_live(
        work, gate_lease, now=now
    )
    if live_gate.mode is not GateMode.SHARED:
        raise CanonicalValueNotReadyError(
            "canonical writes require a live SHARED maintenance gate"
        )
    live_turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(live_turn.generation, field="generation")


def _lock_claim(work: VNextUnitOfWork, generation: int, value_sha256: bytes) -> None:
    claim = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("canonical-upload", generation, value_sha256),
        "SELECT generation, value_sha256 "
        "FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, value_sha256),
    )
    _require_exact(
        "canonical upload claim",
        claim,
        (generation, value_sha256),
    )


def _validate_page_shape(page: CanonicalValuePage, byte_count: int) -> None:
    if page.level == 0:
        expected_pages = max(
            1,
            (byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1)
            // CANONICAL_VALUE_CHUNK_BYTES,
        )
        if page.page_position >= expected_pages:
            raise CanonicalValueCollisionError("leaf page position is out of range")
        expected_offset = page.page_position * CANONICAL_VALUE_CHUNK_BYTES
        expected_count = min(
            CANONICAL_VALUE_CHUNK_BYTES,
            max(byte_count - expected_offset, 0),
        )
        if page.subtree_byte_count != expected_count:
            raise CanonicalValueCollisionError("leaf subtree byte count disagrees")
        if expected_count == 0:
            if page.page_position != 0 or page.entries:
                raise CanonicalValueCollisionError("empty value is not one empty leaf")
        elif len(page.entries) != 1:
            raise CanonicalValueCollisionError("nonempty leaf needs one exact chunk")
        return
    if page.level > _expected_root_level(byte_count):
        raise CanonicalValueCollisionError("branch level exceeds minimal tree height")
    page_count = _page_count_at_level(byte_count, page.level)
    if page.page_position >= page_count:
        raise CanonicalValueCollisionError("branch page position is out of range")
    expected_children = min(
        CANONICAL_VALUE_BRANCH_CAPACITY,
        _page_count_at_level(byte_count, page.level - 1)
        - page.page_position * CANONICAL_VALUE_BRANCH_CAPACITY,
    )
    if len(page.entries) != expected_children:
        raise CanonicalValueCollisionError("branch fanout is not deterministic")


def _require_exact_leaf_source(
    plan: CanonicalValueUploadPlan,
    page: CanonicalValuePage,
) -> None:
    if not page.entries:
        return
    entry = page.entries[0]
    if not isinstance(entry, CanonicalValueChunk):
        raise CanonicalValueCollisionError("leaf entry has the wrong type")
    expected_offset = page.page_position * CANONICAL_VALUE_CHUNK_BYTES
    plan._payload.seek(expected_offset)
    expected = plan._payload.read(len(entry.chunk_bytes))
    _require_exact(
        "canonical leaf source",
        (entry.byte_offset, entry.chunk_bytes),
        (expected_offset, expected),
    )


def _validate_branch_children(
    connector: Any,
    page: CanonicalValuePage,
    *,
    byte_count: int,
) -> None:
    total = 0
    first_position = page.page_position * CANONICAL_VALUE_BRANCH_CAPACITY
    for position, entry in enumerate(page.entries):
        if not isinstance(entry, CanonicalValueBranchEntry):
            raise CanonicalValueCollisionError("branch entry has the wrong type")
        row = connector.fetch_one(
            "SELECT p.value_sha256, p.page_bytes, d.level, d.page_position, "
            "d.subtree_item_count FROM catalog_canonical_value_pages p "
            "JOIN catalog_canonical_value_page_descriptors d "
            "ON d.page_sha256 = p.page_sha256 WHERE p.page_sha256 = %s",
            (entry.child_page_sha256,),
        )
        if len(row) != 5:
            raise CanonicalValueNotReadyError("canonical branch child is incomplete")
        child = decode_canonical_value_page(row[1])
        _validate_page_shape(child, byte_count)
        _require_exact(
            "canonical branch child",
            (
                row[0],
                row[2],
                row[3],
                row[4],
                child.owner_value_sha256,
            ),
            (
                page.owner_value_sha256,
                page.level - 1,
                first_position + position,
                entry.child_subtree_byte_count,
                page.owner_value_sha256,
            ),
        )
        if canonical_value_page_digest(row[1]) != entry.child_page_sha256:
            raise CanonicalValueCollisionError("canonical child digest disagrees")
        total += entry.child_subtree_byte_count
        if total > INT63_MAX:
            raise CanonicalValueCollisionError("canonical branch count overflows int63")
    if total != page.subtree_byte_count:
        raise CanonicalValueCollisionError("canonical branch subtree count disagrees")


def _expected_root_level(byte_count: int) -> int:
    pages = max(
        1,
        (byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1) // CANONICAL_VALUE_CHUNK_BYTES,
    )
    level = 0
    while pages > 1:
        pages = (
            pages + CANONICAL_VALUE_BRANCH_CAPACITY - 1
        ) // CANONICAL_VALUE_BRANCH_CAPACITY
        level += 1
    return level


def _page_count_at_level(byte_count: int, level: int) -> int:
    count = max(
        1,
        (byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1) // CANONICAL_VALUE_CHUNK_BYTES,
    )
    for _ in range(level):
        count = (
            count + CANONICAL_VALUE_BRANCH_CAPACITY - 1
        ) // CANONICAL_VALUE_BRANCH_CAPACITY
    return count


def _iter_tree_payload(
    work: VNextUnitOfWork,
    *,
    owner: bytes,
    page_sha256: bytes,
    expected_level: int,
    expected_position: int,
    expected_byte_offset: int,
    total_byte_count: int,
) -> Iterator[bytes]:
    row = work.connector.fetch_one(
        "SELECT p.value_sha256, p.page_bytes, d.level, d.page_position, "
        "d.subtree_item_count FROM catalog_canonical_value_pages p "
        "JOIN catalog_canonical_value_page_descriptors d "
        "ON d.page_sha256 = p.page_sha256 WHERE p.page_sha256 = %s",
        (page_sha256,),
    )
    if len(row) != 5:
        raise CanonicalValueNotReadyError("canonical page is incomplete")
    if canonical_value_page_digest(row[1]) != page_sha256:
        raise CanonicalValueCollisionError("stored canonical page digest disagrees")
    page = decode_canonical_value_page(row[1])
    _require_exact(
        "canonical streamed page",
        (
            row[0],
            row[2],
            row[3],
            row[4],
            page.owner_value_sha256,
        ),
        (
            owner,
            expected_level,
            expected_position,
            page.subtree_byte_count,
            owner,
        ),
    )
    _validate_page_shape(page, total_byte_count)
    if expected_level == 0:
        if page.entries:
            entry = page.entries[0]
            if not isinstance(entry, CanonicalValueChunk):
                raise CanonicalValueCollisionError("leaf entry has the wrong type")
            _require_exact(
                "canonical streamed leaf offset",
                (entry.byte_offset,),
                (expected_byte_offset,),
            )
            yield entry.chunk_bytes
        return

    running_offset = expected_byte_offset
    for position, entry in enumerate(page.entries):
        if not isinstance(entry, CanonicalValueBranchEntry):
            raise CanonicalValueCollisionError("branch entry has the wrong type")
        edge = work.connector.fetch_one(
            "SELECT child_sha256, parent_sha256, position "
            "FROM catalog_canonical_value_page_parents "
            "WHERE parent_sha256 = %s AND position = %s",
            (page_sha256, position),
        )
        _require_exact(
            "canonical streamed parent edge",
            edge,
            (entry.child_page_sha256, page_sha256, position),
        )
        child_position = expected_position * CANONICAL_VALUE_BRANCH_CAPACITY + position
        yield from _iter_tree_payload(
            work,
            owner=owner,
            page_sha256=entry.child_page_sha256,
            expected_level=expected_level - 1,
            expected_position=child_position,
            expected_byte_offset=running_offset,
            total_byte_count=total_byte_count,
        )
        running_offset += entry.child_subtree_byte_count
    if running_offset - expected_byte_offset != page.subtree_byte_count:
        raise CanonicalValueCollisionError("streamed branch byte count disagrees")


def _require_exact(
    label: str, actual: tuple[Any, ...], expected: tuple[Any, ...]
) -> None:
    if actual != expected:
        raise CanonicalValueCollisionError(
            f"{label} conflicts with its immutable exact tuple"
        )
