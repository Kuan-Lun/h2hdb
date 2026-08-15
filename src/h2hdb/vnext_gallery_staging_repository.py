"""Bounded gallery-observation staging for the greenfield vNext schema.

The public commands in this module contain source observations, never database
authority.  In particular they contain no page/request digest, file role,
descriptor, frontier position, or checkpoint cursor.  Those values are
derived after the repository has locked the live gate, ingest turn, staging
claim, and durable checkpoint.

Every method runs inside a caller-owned :class:`VNextUnitOfWork`.  A response-
loss replay reconstructs the complete request frame and compares all durable
chunks before returning without a write.  New page commits materialize one
bounded canonical page, normalized facts, a base-256 frontier transition, and
the checkpoint receipt in the same transaction.
"""

from __future__ import annotations

__all__ = [
    "BatchAttempt",
    "DirectoryBatchCommand",
    "DirectoryObservation",
    "FileContentReceipt",
    "FileBatchCommand",
    "FileObservation",
    "GalleryObservationStagingRepository",
    "GalleryStagingConflictError",
    "GalleryStagingHandle",
    "GalleryStagingNotReadyError",
    "GalleryStagingReceipt",
    "GalleryStagingSeal",
    "MatchBatchCommand",
    "MatchBatchReceipt",
    "MetadataBatchCommand",
    "TagBatchCommand",
    "TagObservation",
]

import secrets
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any

from .vnext_allocator_repository import (
    AllocatorExhaustedError,
    IdentityStream,
    VNextAllocatorRepository,
)
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_gallery_identity_repository import GalleryIdentityHandoff
from .vnext_identity import (
    GALLERY_OBSERVATION_DURABLE_PARSER_PHASES,
    ByteDomainError,
    GalleryObservationBranchEntry,
    GalleryObservationComponent,
    GalleryObservationDescriptor,
    GalleryObservationDirectoryEntry,
    GalleryObservationDirectoryFileType,
    GalleryObservationFileEntry,
    GalleryObservationMetadataChunk,
    GalleryObservationMetadataDecoder,
    GalleryObservationMetadataDecoderState,
    GalleryObservationNodeKind,
    GalleryObservationPage,
    GalleryObservationTagEntry,
    build_canonical_value_tree,
    canonical_value_digest,
    decode_canonical_value_page,
    decode_gallery_observation_page,
    encode_gallery_observation_descriptor,
    encode_gallery_observation_page,
    file_key,
    file_role,
    gallery_directory_audit_digest,
    gallery_metadata_audit_digest,
    gallery_observation_descriptor_digest,
    gallery_observation_page_digest,
    gallery_observation_page_key_bounds,
    gallery_scan_audit_digest,
    validate_file_name,
    validate_gallery_observation_durable_parser_phase,
    validate_namespace,
)
from .vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestTurn,
)
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import (
    LockRank,
    VNextUnitOfWork,
    encode_lock_key,
)

_STAGING = "operational_gallery_observation_stagings"
_CLAIM = "operational_gallery_observation_staging_claims"
_CHECKPOINT = "operational_gallery_observation_staging_checkpoints"
_REQUEST = "operational_gallery_observation_staging_requests"
_REQUEST_CHUNK = "operational_gallery_observation_staging_request_chunks"
_REQUEST_OWNER = "operational_gallery_observation_staging_request_owners"
_PREDECESSOR = "operational_gallery_observation_staging_request_predecessors"
_PAGE_REQUEST = "operational_gallery_observation_staging_page_requests"
_REQUEST_PAGE = "operational_gallery_observation_staging_request_pages"
_RECEIPT = "operational_gallery_observation_staging_receipts"
_FRONTIER = "operational_gallery_observation_staging_frontiers"
_MATCH_CHECKPOINT = "operational_gallery_observation_staging_match_checkpoints"
_MATCH_REQUEST = "operational_gallery_observation_staging_match_requests"
_MATCH_RECEIPT = "operational_gallery_observation_staging_match_receipts"
_PARSER = "operational_gallery_observation_staging_metadata_parsers"

_PAGE = "catalog_gallery_observation_pages"
_PAGE_DESCRIPTOR = "catalog_gallery_observation_page_descriptors"
_PAGE_BOUNDS = "catalog_gallery_observation_page_key_bounds"
_PAGE_CHILD = "catalog_gallery_observation_page_children"
_ALLOCATION_PAGE = "catalog_gallery_observation_allocation_pages"
_TREE_ROOT = "catalog_gallery_observation_tree_roots"

_COMPONENT_BYTES = {
    GalleryObservationComponent.FILE: b"FILE",
    GalleryObservationComponent.TAG: b"TAG",
    GalleryObservationComponent.DIRECTORY: b"DIRECTORY",
    GalleryObservationComponent.METADATA: b"METADATA",
}
_COMPONENT_FROM_BYTES = {value: key for key, value in _COMPONENT_BYTES.items()}
_LEAF_CAPACITY = {
    GalleryObservationComponent.FILE: 256,
    GalleryObservationComponent.TAG: 256,
    GalleryObservationComponent.DIRECTORY: 192,
    GalleryObservationComponent.METADATA: 1,
}
_REQUEST_CHUNK_BYTES = 32_768
_REQUEST_CHUNK_COUNT = 3
_REQUEST_BYTES_MAXIMUM = _REQUEST_CHUNK_BYTES * _REQUEST_CHUNK_COUNT
_REQUEST_PREFIX = b"h2hdb-vnext-gallery-staging-request\0"
_REQUEST_VERSION = 1
_TAG_VALUE_DOMAIN = "tag_value_utf8_v1"
_OBSERVATION_DOMAIN = "gallery_observation_v1"
_FILE_RECEIPT_TOKEN = object()


class GalleryStagingConflictError(RuntimeError):
    """Immutable staging, request, page, or normalized facts disagree."""


class GalleryStagingNotReadyError(RuntimeError):
    """A live fence, OPEN checkpoint, complete component, or root is absent."""


@dataclass(frozen=True, slots=True)
class FileContentReceipt:
    """Transaction-independent exact-EOF SHA-256 and byte-count receipt.

    The constructor token is module-private.  Public code obtains a receipt by
    streaming the actual file bytes through :meth:`from_parts`; it cannot bind
    an independently supplied digest or size to a FILE observation.
    """

    file_sha256: bytes
    size_bytes: int
    _constructor_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._constructor_token is not _FILE_RECEIPT_TOKEN:
            raise TypeError("use FileContentReceipt.from_parts")
        require_digest32(self.file_sha256, field="file_sha256")
        require_int63(self.size_bytes, field="size_bytes")

    @classmethod
    def from_parts(cls, parts: Iterable[bytes]) -> FileContentReceipt:
        digest = sha256()
        size = 0
        for part in parts:
            exact = require_bounded_bytes(
                part,
                field="file content part",
                maximum=INT63_MAX,
            )
            size += len(exact)
            if size > INT63_MAX:
                raise OverflowError("file content exceeds signed int63 bytes")
            digest.update(exact)
        return cls(digest.digest(), size, _FILE_RECEIPT_TOKEN)


@dataclass(frozen=True, slots=True)
class FileObservation:
    """One source FILE fact; content digest, size, key, and role are derived."""

    name_bytes: bytes
    content: FileContentReceipt
    device: int
    inode: int
    modified_ns: int
    changed_ns: int

    def __post_init__(self) -> None:
        validate_file_name(self.name_bytes)
        if (
            not isinstance(self.content, FileContentReceipt)
            or self.content._constructor_token is not _FILE_RECEIPT_TOKEN
        ):
            raise TypeError("content must be a FileContentReceipt from exact bytes")
        self.content.__post_init__()
        _require_uint64(self.device, field="device")
        _require_uint64(self.inode, field="inode")
        _require_int64(self.modified_ns, field="modified_ns")
        _require_int64(self.changed_ns, field="changed_ns")


@dataclass(frozen=True, slots=True)
class DirectoryObservation:
    """One no-follow direct-child DIRECTORY fact."""

    name_bytes: bytes
    size_bytes: int
    device: int
    inode: int
    modified_ns: int
    changed_ns: int
    file_type: GalleryObservationDirectoryFileType

    def __post_init__(self) -> None:
        validate_file_name(self.name_bytes)
        require_int63(self.size_bytes, field="size_bytes")
        _require_uint64(self.device, field="device")
        _require_uint64(self.inode, field="inode")
        _require_int64(self.modified_ns, field="modified_ns")
        _require_int64(self.changed_ns, field="changed_ns")
        if not isinstance(self.file_type, GalleryObservationDirectoryFileType):
            raise TypeError("file_type must be GalleryObservationDirectoryFileType")


@dataclass(frozen=True, slots=True)
class TagObservation:
    """One exact source tag; its digest and numeric ID are derived internally."""

    namespace: str
    value: str
    _namespace_bytes: bytes = field(init=False, repr=False, compare=False)
    _value_bytes: bytes = field(init=False, repr=False, compare=False)
    _value_sha256: bytes = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        namespace = validate_namespace(self.namespace)
        if not isinstance(self.value, str):
            raise TypeError("tag value must be str")
        value = self.value.encode("utf-8", errors="strict")
        require_bounded_bytes(
            value,
            field="tag value UTF-8",
            maximum=65_536,
        )
        object.__setattr__(self, "_namespace_bytes", namespace)
        object.__setattr__(self, "_value_bytes", value)
        object.__setattr__(
            self,
            "_value_sha256",
            canonical_value_digest(_TAG_VALUE_DOMAIN, value),
        )


@dataclass(frozen=True, slots=True)
class BatchAttempt:
    """Single-flight attempt token plus acknowledgement of the latest attempt.

    This is not a permanent idempotency ledger.  Repeating the current latest
    attempt is response-loss replay.  A new attempt must acknowledge that
    latest operation token; once a successor commits, an older command is
    rejected in O(1) rather than searched in request history.
    """

    operation_id: bytes
    previous_operation_id: bytes | None

    def __post_init__(self) -> None:
        require_uuid16(self.operation_id, field="operation_id")
        if self.previous_operation_id is not None:
            require_uuid16(
                self.previous_operation_id,
                field="previous_operation_id",
            )
            if self.previous_operation_id == self.operation_id:
                raise ValueError("operation_id cannot acknowledge itself")


@dataclass(frozen=True, slots=True)
class FileBatchCommand:
    entries: tuple[FileObservation, ...]
    terminal: bool
    attempt: BatchAttempt

    def __post_init__(self) -> None:
        _require_exact_tuple(self.entries, FileObservation, field_name="FILE entries")
        for entry in self.entries:
            entry.__post_init__()
        _require_exact_bool(self.terminal, field_name="FILE terminal")
        _require_attempt(self.attempt)


@dataclass(frozen=True, slots=True)
class DirectoryBatchCommand:
    entries: tuple[DirectoryObservation, ...]
    terminal: bool
    attempt: BatchAttempt

    def __post_init__(self) -> None:
        _require_exact_tuple(
            self.entries,
            DirectoryObservation,
            field_name="DIRECTORY entries",
        )
        for entry in self.entries:
            entry.__post_init__()
        _require_exact_bool(self.terminal, field_name="DIRECTORY terminal")
        _require_attempt(self.attempt)


@dataclass(frozen=True, slots=True)
class TagBatchCommand:
    entries: tuple[TagObservation, ...]
    terminal: bool
    attempt: BatchAttempt

    def __post_init__(self) -> None:
        _require_exact_tuple(self.entries, TagObservation, field_name="TAG entries")
        for entry in self.entries:
            entry.__post_init__()
        _require_exact_bool(self.terminal, field_name="TAG terminal")
        _require_attempt(self.attempt)


@dataclass(frozen=True, slots=True)
class MetadataBatchCommand:
    chunk_bytes: bytes
    terminal: bool
    attempt: BatchAttempt

    def __post_init__(self) -> None:
        require_bounded_bytes(
            self.chunk_bytes,
            field="METADATA chunk",
            minimum=1,
            maximum=32_768,
        )
        _require_exact_bool(self.terminal, field_name="METADATA terminal")
        _require_attempt(self.attempt)


@dataclass(frozen=True, slots=True)
class MatchBatchCommand:
    """Single-flight match attempt; neither token is traversal authority."""

    operation_id: bytes
    previous_operation_id: bytes | None

    def __post_init__(self) -> None:
        require_uuid16(self.operation_id, field="match operation_id")
        if self.previous_operation_id is not None:
            require_uuid16(
                self.previous_operation_id,
                field="match previous_operation_id",
            )
            if self.previous_operation_id == self.operation_id:
                raise ValueError("match operation_id cannot acknowledge itself")


@dataclass(frozen=True, slots=True)
class GalleryStagingHandle:
    staging_id: bytes
    build_id: bytes
    gallery_id: int
    observation_id: int
    ingest_generation: int
    claim_generation: int

    def __post_init__(self) -> None:
        require_uuid16(self.staging_id, field="staging_id")
        require_uuid16(self.build_id, field="build_id")
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_positive_int63(self.observation_id, field="observation_id")
        require_int63(self.ingest_generation, field="ingest_generation")
        require_int63(self.claim_generation, field="claim_generation")


@dataclass(frozen=True, slots=True)
class GalleryStagingReceipt:
    request_sha256: bytes
    component: GalleryObservationComponent
    cursor: int
    processed_byte_count: int
    state: str
    root_page_sha256: bytes | None
    replayed: bool

    def __post_init__(self) -> None:
        require_digest32(self.request_sha256, field="request_sha256")
        if not isinstance(self.component, GalleryObservationComponent):
            raise TypeError("component must be GalleryObservationComponent")
        require_int63(self.cursor, field="cursor")
        require_int63(self.processed_byte_count, field="processed_byte_count")
        if (
            self.component is not GalleryObservationComponent.FILE
            and self.processed_byte_count != 0
        ):
            raise ValueError("non-FILE processed_byte_count must be zero")
        if self.state not in {"OPEN", "COMPLETE"}:
            raise ValueError("state must be OPEN or COMPLETE")
        if self.root_page_sha256 is not None:
            require_digest32(self.root_page_sha256, field="root_page_sha256")
        _require_exact_bool(self.replayed, field_name="replayed")


@dataclass(frozen=True, slots=True)
class MatchBatchReceipt:
    request_sha256: bytes
    matched_count: int
    state: str
    replayed: bool

    def __post_init__(self) -> None:
        require_digest32(self.request_sha256, field="request_sha256")
        require_int63(self.matched_count, field="matched_count")
        if self.state not in {"OPEN", "COMPLETE"}:
            raise ValueError("state must be OPEN or COMPLETE")
        _require_exact_bool(self.replayed, field_name="replayed")


@dataclass(frozen=True, slots=True)
class GalleryStagingSeal:
    build_id: bytes
    gallery_id: int
    observation_id: int
    observation_identity_sha256: bytes
    state: str
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_positive_int63(self.observation_id, field="observation_id")
        require_digest32(
            self.observation_identity_sha256,
            field="observation_identity_sha256",
        )
        if self.state not in {"SEALED", "REUSED"}:
            raise ValueError("seal state must be SEALED or REUSED")
        _require_exact_bool(self.replayed, field_name="replayed")


@dataclass(frozen=True, slots=True)
class _Header:
    build_id: bytes
    gallery_id: int
    observation_id: int
    state: str
    created_at: int
    sealed_at: int | None


@dataclass(frozen=True, slots=True)
class _Claim:
    ingest_generation: int
    claim_generation: int
    updated_at: int


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    cursor: int
    regular_count: int
    processed_byte_count: int
    state: str
    updated_at: int


@dataclass(frozen=True, slots=True)
class _PreparedPage:
    component: GalleryObservationComponent
    page: GalleryObservationPage
    page_bytes: bytes
    page_sha256: bytes
    semantic_bytes: bytes
    item_count: int
    regular_count: int
    byte_count_delta: int


@dataclass(frozen=True, slots=True)
class _FrontierPage:
    request_sha256: bytes
    position: int
    page_sha256: bytes
    subtree_item_count: int


class GalleryObservationStagingRepository:
    """Transaction-local writer for one gallery-observation attempt."""

    @staticmethod
    def begin(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        gallery_id: int,
        now: int,
    ) -> GalleryStagingHandle:
        """Low-level begin after a gallery identity handoff.

        ``gallery_id`` is not caller-selected surrogate authority.  Application
        code obtains it from :class:`GalleryIdentityHandoff` and should prefer
        :meth:`begin_from_identity`, which exact-compares the complete handoff.
        This lower-level form remains useful inside the repository test seam.
        """

        build = require_uuid16(build_id, field="build_id")
        gallery = require_positive_int63(gallery_id, field="gallery_id")
        timestamp = require_int63(now, field="now")
        generation = _authorize_outer(work, gate_lease, ingest_turn, now=timestamp)
        scope = _lock_and_require_working_build(
            work,
            generation=generation,
            build_id=build,
        )
        row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("gallery-staging", 0, build, gallery),
            f"SELECT staging_id, observation_id, state, created_at, sealed_at "
            f"FROM {_STAGING} WHERE build_id = %s AND gallery_id = %s",
            (build, gallery),
        )
        if row:
            if len(row) != 5:
                raise GalleryStagingConflictError("staging replay row has bad shape")
            staging = require_uuid16(row[0], field="persisted staging_id")
            observation = require_positive_int63(
                row[1], field="persisted observation_id"
            )
            claim = _lock_claim(work, staging)
            if claim.ingest_generation != generation:
                raise GalleryStagingNotReadyError(
                    "existing staging requires an explicit takeover"
                )
            _require_header_state(row[2], row[4])
            return GalleryStagingHandle(
                staging,
                build,
                gallery,
                observation,
                generation,
                claim.claim_generation,
            )

        identity = work.connector.fetch_one(
            "SELECT scope_key FROM catalog_gallery_identities WHERE gallery_id = %s",
            (gallery,),
        )
        if identity != (scope,):
            raise GalleryStagingNotReadyError(
                "gallery identity is absent or belongs to another source scope"
            )
        staging = require_uuid16(_new_staging_id(), field="generated staging_id")
        if work.connector.fetch_one(
            f"SELECT staging_id FROM {_STAGING} WHERE staging_id = %s", (staging,)
        ):
            raise GalleryStagingConflictError("generated staging_id collided")

        allocator = work.lock_row(
            LockRank.ALLOCATOR,
            encode_lock_key("gallery-observation-allocator", gallery),
            "SELECT next_observation_id FROM "
            "operational_gallery_observation_allocators WHERE gallery_id = %s",
            (gallery,),
        )
        if len(allocator) != 1:
            raise GalleryStagingNotReadyError(
                "gallery observation allocator is not initialized"
            )
        observation = require_positive_int63(allocator[0], field="next_observation_id")
        if observation == INT63_MAX:
            raise AllocatorExhaustedError("gallery observation allocator is exhausted")
        work.compare_and_swap(
            "UPDATE operational_gallery_observation_allocators "
            "SET next_observation_id = %s, updated_at = %s "
            "WHERE gallery_id = %s AND next_observation_id = %s",
            (observation + 1, timestamp, gallery, observation),
            authority="gallery observation allocator",
        )
        work.connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, %s)",
            (gallery, observation, timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_STAGING} "
            "(staging_id, build_id, gallery_id, observation_id, state, "
            "created_at, sealed_at) VALUES (%s, %s, %s, %s, %s, %s, NULL)",
            (staging, build, gallery, observation, "OPEN", timestamp),
        )
        work.connector.execute(
            f"INSERT INTO {_CLAIM} "
            "(staging_id, ingest_generation, claim_generation, updated_at) "
            "VALUES (%s, %s, 0, %s)",
            (staging, generation, timestamp),
        )
        for component in GalleryObservationComponent:
            work.connector.execute(
                f"INSERT INTO {_CHECKPOINT} "
                "(staging_id, component, level, cursor, regular_count, "
                "processed_byte_count, state, updated_at) "
                "VALUES (%s, %s, 0, 0, 0, 0, %s, %s)",
                (staging, _COMPONENT_BYTES[component], "OPEN", timestamp),
            )
        work.connector.execute(
            f"INSERT INTO {_MATCH_CHECKPOINT} "
            "(staging_id, file_cursor_bytes, matched_count, state, updated_at) "
            "VALUES (%s, %s, 0, %s, %s)",
            (staging, b"", "OPEN", timestamp),
        )
        _insert_parser(work.connector, staging, timestamp)
        return GalleryStagingHandle(
            staging,
            build,
            gallery,
            observation,
            generation,
            0,
        )

    @staticmethod
    def begin_from_identity(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        identity: GalleryIdentityHandoff,
        now: int,
    ) -> GalleryStagingHandle:
        """Begin from the exact locator-to-gallery consumer handoff.

        The regular ``begin`` authorization first locks and proves the live
        build and source scope.  The remaining immutable handoff fields are
        then byte-compared with the gallery identity in the same transaction;
        a forged or stale handoff therefore rolls back any tentative staging
        allocation.
        """

        if type(identity) is not GalleryIdentityHandoff:
            raise TypeError("identity must be an exact GalleryIdentityHandoff")
        identity.__post_init__()
        handle = GalleryObservationStagingRepository.begin(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=identity.build_id,
            gallery_id=identity.gallery_id,
            now=now,
        )
        durable = work.connector.fetch_one(
            "SELECT gallery_key, scope_key, locator_sha256 "
            "FROM catalog_gallery_identities WHERE gallery_id = %s",
            (identity.gallery_id,),
        )
        expected = (
            identity.gallery_key,
            identity.scope_key,
            identity.locator_sha256,
        )
        if durable != expected:
            raise GalleryStagingConflictError(
                "gallery identity handoff differs from its durable exact tuple"
            )
        return handle

    @staticmethod
    def resume(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        now: int,
    ) -> GalleryStagingHandle:
        """Revalidate and return the exact current staging capability.

        This performs no mutation.  It is distinct from :meth:`takeover`,
        which is the only operation allowed to change the durable claim fence.
        """

        current = _require_handle(handle)
        timestamp = require_int63(now, field="now")
        _authorize_staging(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=current,
            now=timestamp,
        )
        return current

    @staticmethod
    def takeover(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        now: int,
    ) -> GalleryStagingHandle:
        current = _require_handle(handle)
        timestamp = require_int63(now, field="now")
        generation = _authorize_outer(work, gate_lease, ingest_turn, now=timestamp)
        _lock_and_require_working_build(
            work,
            generation=generation,
            build_id=current.build_id,
        )
        header, claim = _lock_header_and_claim(work, current.staging_id)
        _require_handle_rows(current, header, claim, allow_stale_generation=True)
        if header.state != "OPEN":
            raise GalleryStagingNotReadyError("only OPEN staging may be taken over")
        if claim.ingest_generation == generation:
            if claim.claim_generation != current.claim_generation:
                raise GalleryStagingNotReadyError("staging claim is stale")
            return current
        if claim.claim_generation == INT63_MAX:
            raise OverflowError("staging claim generation is exhausted")
        successor = claim.claim_generation + 1
        work.compare_and_swap(
            f"UPDATE {_CLAIM} SET ingest_generation = %s, claim_generation = %s, "
            "updated_at = %s WHERE staging_id = %s AND ingest_generation = %s "
            "AND claim_generation = %s AND updated_at = %s",
            (
                generation,
                successor,
                timestamp,
                current.staging_id,
                claim.ingest_generation,
                claim.claim_generation,
                claim.updated_at,
            ),
            authority="gallery staging takeover",
        )
        return GalleryStagingHandle(
            current.staging_id,
            current.build_id,
            current.gallery_id,
            current.observation_id,
            generation,
            successor,
        )

    @staticmethod
    def put_files(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        command: FileBatchCommand,
        now: int,
    ) -> GalleryStagingReceipt:
        if type(command) is not FileBatchCommand:
            raise TypeError("command must be an exact FileBatchCommand")
        command.__post_init__()
        return _put_component_page(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=_require_handle(handle),
            component=GalleryObservationComponent.FILE,
            source_entries=command.entries,
            terminal=command.terminal,
            attempt=command.attempt,
            now=now,
        )

    @staticmethod
    def put_directories(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        command: DirectoryBatchCommand,
        now: int,
    ) -> GalleryStagingReceipt:
        if type(command) is not DirectoryBatchCommand:
            raise TypeError("command must be an exact DirectoryBatchCommand")
        command.__post_init__()
        return _put_component_page(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=_require_handle(handle),
            component=GalleryObservationComponent.DIRECTORY,
            source_entries=command.entries,
            terminal=command.terminal,
            attempt=command.attempt,
            now=now,
        )

    @staticmethod
    def put_tags(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        command: TagBatchCommand,
        now: int,
    ) -> GalleryStagingReceipt:
        if type(command) is not TagBatchCommand:
            raise TypeError("command must be an exact TagBatchCommand")
        command.__post_init__()
        return _put_component_page(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=_require_handle(handle),
            component=GalleryObservationComponent.TAG,
            source_entries=command.entries,
            terminal=command.terminal,
            attempt=command.attempt,
            now=now,
        )

    @staticmethod
    def put_metadata(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        command: MetadataBatchCommand,
        now: int,
    ) -> GalleryStagingReceipt:
        if type(command) is not MetadataBatchCommand:
            raise TypeError("command must be an exact MetadataBatchCommand")
        command.__post_init__()
        return _put_component_page(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=_require_handle(handle),
            component=GalleryObservationComponent.METADATA,
            source_entries=(command.chunk_bytes,),
            terminal=command.terminal,
            attempt=command.attempt,
            now=now,
        )

    @staticmethod
    def match_files_to_directory(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        command: MatchBatchCommand,
        now: int,
    ) -> MatchBatchReceipt:
        if type(command) is not MatchBatchCommand:
            raise TypeError("command must be an exact MatchBatchCommand")
        command.__post_init__()
        current = _require_handle(handle)
        timestamp = require_int63(now, field="now")
        header, _claim = _authorize_staging(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=current,
            now=timestamp,
        )
        row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("gallery-staging", 2, current.staging_id, b"MATCH"),
            f"SELECT file_cursor_bytes, matched_count, state, updated_at "
            f"FROM {_MATCH_CHECKPOINT} WHERE staging_id = %s",
            (current.staging_id,),
        )
        checkpoint = _decode_match_checkpoint(row)
        latest = work.connector.fetch_one(
            f"SELECT r.request_sha256, q.start_file_cursor_bytes, "
            "q.start_matched_count, q.terminal "
            f"FROM {_MATCH_RECEIPT} r JOIN {_MATCH_REQUEST} q "
            "ON q.request_sha256 = r.request_sha256 WHERE r.staging_id = %s",
            (current.staging_id,),
        )
        if latest:
            replay = _try_replay_match(
                work,
                current,
                command,
                checkpoint,
                latest,
            )
            if replay is not None:
                return replay
        match_attempt = BatchAttempt(
            command.operation_id,
            command.previous_operation_id,
        )
        _require_new_attempt(
            work.connector,
            None if not latest else latest[0],
            match_attempt,
            subtype=b"M",
        )
        if header.state != "OPEN":
            raise GalleryStagingNotReadyError(
                "only OPEN staging accepts a new match batch"
            )
        if checkpoint.state != "OPEN":
            raise GalleryStagingNotReadyError("FILE-to-DIRECTORY match is complete")

        start_file = _decode_file_cursor(checkpoint.file_cursor_bytes)
        matched_rows, terminal = _match_batch_rows(
            work,
            current,
            start_file=start_file,
        )
        next_count = checkpoint.matched_count + len(matched_rows)
        if next_count > INT63_MAX:
            raise OverflowError("matched_count exceeds signed int63")
        next_file = start_file + len(matched_rows)
        next_cursor = b"" if next_file == 0 else next_file.to_bytes(8, "big")
        body = _encode_match_body(match_attempt, matched_rows)
        predecessor = (
            None
            if not latest
            else require_digest32(latest[0], field="prior match request_sha256")
        )
        frame = _encode_match_request(
            current,
            start_cursor=checkpoint.file_cursor_bytes,
            start_matched=checkpoint.matched_count,
            terminal=terminal,
            predecessor=predecessor,
            body=body,
        )
        request_sha256 = _persist_request_identity(
            work,
            current,
            frame,
            predecessor=predecessor,
            owner_lock_level=9,
        )
        work.connector.execute(
            f"INSERT INTO {_MATCH_REQUEST} "
            "(request_sha256, staging_id, start_file_cursor_bytes, "
            "start_matched_count, terminal) VALUES (%s, %s, %s, %s, %s)",
            (
                request_sha256,
                current.staging_id,
                checkpoint.file_cursor_bytes,
                checkpoint.matched_count,
                int(terminal),
            ),
        )
        _replace_match_receipt(
            work,
            current.staging_id,
            previous=predecessor,
            request_sha256=request_sha256,
            now=timestamp,
        )
        next_state = "COMPLETE" if terminal else "OPEN"
        if terminal:
            file_checkpoint = _read_level_zero_checkpoint(
                work.connector,
                current.staging_id,
                GalleryObservationComponent.FILE,
            )
            directory_checkpoint = _read_level_zero_checkpoint(
                work.connector,
                current.staging_id,
                GalleryObservationComponent.DIRECTORY,
            )
            if (
                file_checkpoint.state != "COMPLETE"
                or directory_checkpoint.state != "COMPLETE"
                or next_count != file_checkpoint.cursor
                or next_count != directory_checkpoint.regular_count
            ):
                raise GalleryStagingNotReadyError(
                    "terminal FILE-to-DIRECTORY counts are not congruent"
                )
        work.compare_and_swap(
            f"UPDATE {_MATCH_CHECKPOINT} SET file_cursor_bytes = %s, "
            "matched_count = %s, state = %s, updated_at = %s "
            "WHERE staging_id = %s AND file_cursor_bytes = %s "
            "AND matched_count = %s AND state = %s AND updated_at = %s",
            (
                next_cursor,
                next_count,
                next_state,
                timestamp,
                current.staging_id,
                checkpoint.file_cursor_bytes,
                checkpoint.matched_count,
                checkpoint.state,
                checkpoint.updated_at,
            ),
            authority="gallery FILE-to-DIRECTORY checkpoint",
        )
        return MatchBatchReceipt(request_sha256, next_count, next_state, False)

    @staticmethod
    def seal(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: GalleryStagingHandle,
        now: int,
    ) -> GalleryStagingSeal:
        current = _require_handle(handle)
        timestamp = require_int63(now, field="now")
        header, _claim = _authorize_staging(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            handle=current,
            now=timestamp,
        )
        if header.state in {"SEALED", "REUSED"}:
            return _validate_seal_replay(work.connector, current, header.state)
        if header.state != "OPEN":
            raise GalleryStagingNotReadyError("staging is not OPEN")

        checkpoints: dict[GalleryObservationComponent, _Checkpoint] = {}
        for component in sorted(
            GalleryObservationComponent,
            key=lambda candidate: encode_lock_key(_COMPONENT_BYTES[candidate]),
        ):
            row = work.lock_row(
                LockRank.CHECKPOINT,
                encode_lock_key(
                    "gallery-staging",
                    2,
                    current.staging_id,
                    _COMPONENT_BYTES[component],
                    0,
                ),
                f"SELECT cursor, regular_count, processed_byte_count, state, "
                f"updated_at FROM {_CHECKPOINT} "
                "WHERE staging_id = %s AND component = %s AND level = 0",
                (current.staging_id, _COMPONENT_BYTES[component]),
            )
            checkpoints[component] = _decode_checkpoint(row)
        match = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("gallery-staging", 3, current.staging_id, b"MATCH"),
            f"SELECT file_cursor_bytes, matched_count, state, updated_at "
            f"FROM {_MATCH_CHECKPOINT} WHERE staging_id = %s",
            (current.staging_id,),
        )
        match_checkpoint = _decode_match_checkpoint(match)
        parser_row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("gallery-staging", 4, current.staging_id),
            f"SELECT phase, fixed_carry, remaining_text_bytes, utf8_tail, gid, "
            "title_byte_count, comment_byte_count, upload_account_byte_count, "
            "upload_time, download_time, modified_time, scan_observation_version, "
            f"source_file_count, page_count, updated_at FROM {_PARSER} "
            "WHERE staging_id = %s",
            (current.staging_id,),
        )
        parser_state, _parser_updated_at = _decode_parser(parser_row)
        if any(checkpoint.state != "COMPLETE" for checkpoint in checkpoints.values()):
            raise GalleryStagingNotReadyError(
                "all four component streams must complete"
            )
        if match_checkpoint.state != "COMPLETE":
            raise GalleryStagingNotReadyError(
                "FILE-to-DIRECTORY verification is incomplete"
            )
        file_count, byte_count = _terminal_checkpoint_authority(
            work.connector,
            current,
            checkpoints,
        )
        metadata_receipt = GalleryObservationMetadataDecoder(parser_state).finish()
        if (
            metadata_receipt.source_file_count != file_count
            or match_checkpoint.matched_count != file_count
            or match_checkpoint.matched_count
            != checkpoints[GalleryObservationComponent.DIRECTORY].regular_count
        ):
            raise GalleryStagingConflictError("sealed scalar counts are incongruent")

        roots = _bounded_component_roots(work.connector, current)
        descriptor = GalleryObservationDescriptor(
            roots[GalleryObservationComponent.METADATA][0],
            checkpoints[GalleryObservationComponent.METADATA].cursor,
            roots[GalleryObservationComponent.FILE][0],
            checkpoints[GalleryObservationComponent.FILE].cursor,
            roots[GalleryObservationComponent.TAG][0],
            checkpoints[GalleryObservationComponent.TAG].cursor,
            roots[GalleryObservationComponent.DIRECTORY][0],
            checkpoints[GalleryObservationComponent.DIRECTORY].cursor,
        )
        for component, (root, subtree_count) in roots.items():
            expected = checkpoints[component].cursor
            if subtree_count != expected:
                raise GalleryStagingConflictError(
                    f"{component.name} root count disagrees with checkpoint"
                )
            _require_single_frontier_root(
                work.connector,
                current.staging_id,
                component,
                root,
            )
        _persist_scan_fact(
            work.connector,
            current,
            roots,
            scan_observation_version=metadata_receipt.scan_observation_version,
            source_file_count=metadata_receipt.source_file_count,
        )

        descriptor_bytes = encode_gallery_observation_descriptor(descriptor)
        observation_digest = gallery_observation_descriptor_digest(descriptor)
        canonical_root = _persist_canonical_value(
            work,
            generation=current.ingest_generation,
            digest_domain=_OBSERVATION_DOMAIN,
            payload=descriptor_bytes,
            now=timestamp,
            retain_claim=True,
        )
        allocation = work.connector.fetch_one(
            "SELECT digest_domain, byte_count FROM catalog_canonical_value_allocations "
            "WHERE value_sha256 = %s",
            (observation_digest,),
        )
        if allocation != (_OBSERVATION_DOMAIN.encode("ascii"), len(descriptor_bytes)):
            raise GalleryStagingConflictError("observation canonical preimage differs")
        identity = work.connector.fetch_one(
            "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
            "WHERE value_sha256 = %s",
            (observation_digest,),
        )
        if identity != (canonical_root,):
            raise GalleryStagingConflictError("observation canonical root differs")

        existing = work.connector.fetch_one(
            "SELECT observation_id FROM catalog_gallery_observations "
            "WHERE gallery_id = %s AND observation_identity_sha256 = %s",
            (current.gallery_id, observation_digest),
        )
        if existing:
            final_observation = require_positive_int63(
                existing[0], field="reused observation_id"
            )
            stat = work.connector.fetch_one(
                "SELECT file_count, byte_count FROM "
                "catalog_gallery_observation_stat "
                "WHERE gallery_id = %s AND observation_id = %s",
                (current.gallery_id, final_observation),
            )
            if stat != (file_count, byte_count):
                raise GalleryStagingConflictError(
                    "reused observation stat is absent or differs"
                )
            state = "REUSED"
        else:
            work.connector.execute(
                "INSERT INTO catalog_gallery_observations "
                "(gallery_id, observation_id, observation_identity_sha256) "
                "VALUES (%s, %s, %s)",
                (current.gallery_id, current.observation_id, observation_digest),
            )
            work.connector.execute(
                "INSERT INTO catalog_gallery_observation_stat "
                "(gallery_id, observation_id, file_count, byte_count) "
                "VALUES (%s, %s, %s, %s)",
                (
                    current.gallery_id,
                    current.observation_id,
                    file_count,
                    byte_count,
                ),
            )
            final_observation = current.observation_id
            state = "SEALED"

        link = work.connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = %s",
            (current.build_id, current.gallery_id),
        )
        if link:
            if link != (final_observation,):
                raise GalleryStagingConflictError("source-build gallery link differs")
        else:
            # This is the final reader-visible insert in the staging protocol.
            work.connector.execute(
                "INSERT INTO catalog_source_build_galleries "
                "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                (current.build_id, current.gallery_id, final_observation),
            )
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (current.ingest_generation, observation_digest),
        )
        if deleted != 1:
            raise GalleryStagingConflictError(
                "observation canonical upload claim changed before handoff"
            )
        work.compare_and_swap(
            f"UPDATE {_STAGING} SET state = %s, sealed_at = %s "
            "WHERE staging_id = %s AND state = %s AND sealed_at IS NULL",
            (state, timestamp, current.staging_id, "OPEN"),
            authority="gallery staging seal",
        )
        return GalleryStagingSeal(
            current.build_id,
            current.gallery_id,
            final_observation,
            observation_digest,
            state,
            False,
        )


def _put_component_page(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
    source_entries: Sequence[Any],
    terminal: bool,
    attempt: BatchAttempt,
    now: int,
) -> GalleryStagingReceipt:
    timestamp = require_int63(now, field="now")
    header, _claim = _authorize_staging(
        work,
        gate_lease=gate_lease,
        ingest_turn=ingest_turn,
        handle=handle,
        now=timestamp,
    )
    checkpoint = _lock_checkpoint(work, handle.staging_id, component, 0)
    latest = _latest_page_receipt(work.connector, handle.staging_id, component, 0)
    if latest:
        replay = _try_replay_page(
            work,
            handle,
            component,
            source_entries,
            terminal,
            attempt,
            checkpoint,
            latest,
        )
        if replay is not None:
            return replay
    _require_new_attempt(
        work.connector,
        None if not latest else latest[0],
        attempt,
        subtype=b"P",
    )
    if header.state != "OPEN":
        raise GalleryStagingNotReadyError(
            "only OPEN staging accepts a new component batch"
        )
    if checkpoint.state != "OPEN":
        raise GalleryStagingNotReadyError(f"{component.name} stream is complete")
    if (
        component is not GalleryObservationComponent.FILE
        and checkpoint.processed_byte_count != 0
    ):
        raise GalleryStagingConflictError(
            f"non-FILE {component.name} checkpoint has byte authority"
        )

    prepared = _prepare_leaf(component, checkpoint.cursor, source_entries)
    if (
        component is not GalleryObservationComponent.FILE
        and prepared.byte_count_delta != 0
    ):
        raise GalleryStagingConflictError("non-FILE page produced a byte count")
    next_processed_byte_count = _checked_int63_add(
        checkpoint.processed_byte_count,
        prepared.byte_count_delta,
        field_name=f"{component.name} processed byte count",
    )
    _require_leaf_batch_shape(prepared, terminal=terminal, start=checkpoint.cursor)
    _require_leaf_adjacency(
        work.connector,
        handle.staging_id,
        prepared,
        previous_request=None if not latest else latest[0],
    )
    parser_update: GalleryObservationMetadataDecoderState | None = None
    if component is GalleryObservationComponent.METADATA:
        parser_row = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("gallery-staging", 3, handle.staging_id),
            f"SELECT phase, fixed_carry, remaining_text_bytes, utf8_tail, gid, "
            "title_byte_count, comment_byte_count, upload_account_byte_count, "
            "upload_time, download_time, modified_time, scan_observation_version, "
            f"source_file_count, page_count, updated_at FROM {_PARSER} "
            "WHERE staging_id = %s",
            (handle.staging_id,),
        )
        parser_state, _parser_updated_at = _decode_parser(parser_row)
        decoder = GalleryObservationMetadataDecoder(parser_state)
        metadata_entry = prepared.page.entries[0]
        assert isinstance(metadata_entry, GalleryObservationMetadataChunk)
        decoder.feed(metadata_entry.chunk_bytes)
        if terminal:
            decoder.finish()
        elif decoder.state.phase == "DONE":
            raise GalleryStagingConflictError(
                "METADATA reached exact EOF without terminal intent"
            )
        parser_update = decoder.state

    predecessor = (
        None
        if not latest
        else require_digest32(latest[0], field="prior request_sha256")
    )
    frame = _encode_page_request(
        handle,
        prepared,
        start_cursor=checkpoint.cursor,
        start_processed_byte_count=checkpoint.processed_byte_count,
        terminal=terminal,
        predecessor=predecessor,
        attempt=attempt,
    )
    _persist_observation_page(
        work.connector,
        handle,
        prepared,
    )
    _persist_normalized_leaf_facts(
        work,
        handle,
        prepared,
        source_entries,
        now=timestamp,
    )
    if parser_update is not None:
        _update_parser(work, handle.staging_id, parser_update, now=timestamp)
    request_sha256 = _persist_request_identity(
        work,
        handle,
        frame,
        predecessor=predecessor,
        owner_lock_level=prepared.page.level,
    )
    work.connector.execute(
        f"INSERT INTO {_PAGE_REQUEST} "
        "(request_sha256, staging_id, component, level, start_cursor, terminal) "
        "VALUES (%s, %s, %s, 0, %s, %s)",
        (
            request_sha256,
            handle.staging_id,
            _COMPONENT_BYTES[component],
            checkpoint.cursor,
            int(terminal),
        ),
    )
    work.connector.execute(
        f"INSERT INTO {_REQUEST_PAGE} (request_sha256, page_sha256) VALUES (%s, %s)",
        (request_sha256, prepared.page_sha256),
    )

    next_cursor = checkpoint.cursor + prepared.item_count
    if next_cursor > INT63_MAX:
        raise OverflowError(f"{component.name} cursor exceeds signed int63")
    next_regular = checkpoint.regular_count + prepared.regular_count
    if next_regular > INT63_MAX:
        raise OverflowError("DIRECTORY regular_count exceeds signed int63")
    next_state = "COMPLETE" if terminal else "OPEN"
    _replace_page_receipt(
        work,
        handle.staging_id,
        component,
        level=0,
        previous=predecessor,
        request_sha256=request_sha256,
        start_processed_byte_count=checkpoint.processed_byte_count,
        next_processed_byte_count=next_processed_byte_count,
        now=timestamp,
    )
    work.compare_and_swap(
        f"UPDATE {_CHECKPOINT} SET cursor = %s, regular_count = %s, "
        "processed_byte_count = %s, state = %s, updated_at = %s "
        "WHERE staging_id = %s AND component = %s AND level = 0 "
        "AND cursor = %s AND regular_count = %s AND processed_byte_count = %s "
        "AND state = %s AND updated_at = %s",
        (
            next_cursor,
            next_regular,
            next_processed_byte_count,
            next_state,
            timestamp,
            handle.staging_id,
            _COMPONENT_BYTES[component],
            checkpoint.cursor,
            checkpoint.regular_count,
            checkpoint.processed_byte_count,
            checkpoint.state,
            checkpoint.updated_at,
        ),
        authority=f"gallery {component.name} checkpoint",
    )
    _push_frontier(
        work,
        handle,
        component,
        level=0,
        request_sha256=request_sha256,
        page_sha256=prepared.page_sha256,
        subtree_count=prepared.item_count,
        now=timestamp,
    )
    root: bytes | None = None
    if terminal:
        root = _finish_component(work, handle, component, now=timestamp)
        if component is GalleryObservationComponent.METADATA:
            assert parser_update is not None
            _persist_metadata_facts(
                work.connector,
                handle,
                parser_update,
                root,
            )
        elif component is GalleryObservationComponent.DIRECTORY:
            _insert_or_require(
                work.connector,
                label="gallery directory audit",
                select_sql=(
                    "SELECT directory_entry_count, directory_observation_sha256 "
                    "FROM catalog_gallery_observation_directories "
                    "WHERE gallery_id = %s AND observation_id = %s"
                ),
                select_data=(handle.gallery_id, handle.observation_id),
                insert_sql=(
                    "INSERT INTO catalog_gallery_observation_directories "
                    "(gallery_id, observation_id, directory_entry_count, "
                    "directory_observation_sha256) VALUES (%s, %s, %s, %s)"
                ),
                insert_data=(
                    handle.gallery_id,
                    handle.observation_id,
                    next_cursor,
                    gallery_directory_audit_digest(root, next_cursor),
                ),
                expected=(
                    next_cursor,
                    gallery_directory_audit_digest(root, next_cursor),
                ),
            )
    return GalleryStagingReceipt(
        request_sha256,
        component,
        next_cursor,
        next_processed_byte_count,
        next_state,
        root,
        False,
    )


def _authorize_outer(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    gate = MaintenanceGateRepository.lock_and_require_live(work, gate_lease, now=now)
    if gate.mode is not GateMode.SHARED:
        raise GalleryStagingNotReadyError(
            "gallery staging requires a live SHARED maintenance gate"
        )
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(turn.generation, field="ingest generation")


def _authorize_staging(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    handle: GalleryStagingHandle,
    now: int,
) -> tuple[_Header, _Claim]:
    generation = _authorize_outer(work, gate_lease, ingest_turn, now=now)
    if generation != handle.ingest_generation:
        raise GalleryStagingNotReadyError("handle belongs to another ingest generation")
    _lock_and_require_working_build(
        work,
        generation=generation,
        build_id=handle.build_id,
    )
    header, claim = _lock_header_and_claim(work, handle.staging_id)
    _require_handle_rows(handle, header, claim)
    return header, claim


def _lock_and_require_working_build(
    work: VNextUnitOfWork,
    *,
    generation: int,
    build_id: bytes,
) -> bytes:
    mapping = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 0, generation),
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (generation,),
    )
    if mapping != (build_id,):
        raise GalleryStagingNotReadyError(
            "live ingest generation is not mapped to this source build"
        )
    working = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 1, 1),
        "SELECT build_id FROM operational_source_working_builds WHERE slot = 1",
    )
    if working != (build_id,):
        raise GalleryStagingNotReadyError("source build is not the working root")
    build = work.connector.fetch_one(
        "SELECT scope_key, state FROM catalog_source_builds WHERE build_id = %s",
        (build_id,),
    )
    if len(build) != 2 or build[1] != "OPEN":
        raise GalleryStagingNotReadyError("source build is not OPEN")
    return require_digest32(build[0], field="source build scope_key")


def _lock_header_and_claim(
    work: VNextUnitOfWork,
    staging_id: bytes,
) -> tuple[_Header, _Claim]:
    header_row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("gallery-staging", 0, staging_id),
        f"SELECT build_id, gallery_id, observation_id, state, created_at, sealed_at "
        f"FROM {_STAGING} WHERE staging_id = %s",
        (staging_id,),
    )
    claim = _lock_claim(work, staging_id)
    return _decode_header(header_row), claim


def _lock_claim(work: VNextUnitOfWork, staging_id: bytes) -> _Claim:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("gallery-staging", 1, staging_id),
        f"SELECT ingest_generation, claim_generation, updated_at FROM {_CLAIM} "
        "WHERE staging_id = %s",
        (staging_id,),
    )
    if len(row) != 3:
        raise GalleryStagingNotReadyError("staging claim is missing")
    return _Claim(
        require_int63(row[0], field="claim ingest_generation"),
        require_int63(row[1], field="claim_generation"),
        require_int63(row[2], field="claim updated_at"),
    )


def _lock_checkpoint(
    work: VNextUnitOfWork,
    staging_id: bytes,
    component: GalleryObservationComponent,
    level: int,
) -> _Checkpoint:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key(
            "gallery-staging",
            2,
            staging_id,
            _COMPONENT_BYTES[component],
            level,
        ),
        f"SELECT cursor, regular_count, processed_byte_count, state, updated_at "
        f"FROM {_CHECKPOINT} "
        "WHERE staging_id = %s AND component = %s AND level = %s",
        (staging_id, _COMPONENT_BYTES[component], level),
    )
    return _decode_checkpoint(row)


def _prepare_leaf(
    component: GalleryObservationComponent,
    start: int,
    source_entries: Sequence[Any],
) -> _PreparedPage:
    semantic = bytearray()
    entries: list[Any] = []
    regular_count = 0
    byte_count_delta = 0
    if component is GalleryObservationComponent.FILE:
        for offset, source in enumerate(source_entries):
            if not isinstance(source, FileObservation):
                raise TypeError("FILE entries must be FileObservation")
            byte_count_delta = _checked_int63_add(
                byte_count_delta,
                source.content.size_bytes,
                field_name="FILE batch byte count",
            )
            key = file_key(source.name_bytes)
            entries.append(
                GalleryObservationFileEntry(
                    start + offset,
                    key,
                    source.content.file_sha256,
                    source.content.size_bytes,
                    source.device,
                    source.inode,
                    source.modified_ns,
                    source.changed_ns,
                )
            )
            semantic.extend(len(source.name_bytes).to_bytes(4, "big"))
            semantic.extend(source.name_bytes)
    elif component is GalleryObservationComponent.DIRECTORY:
        for offset, source in enumerate(source_entries):
            if not isinstance(source, DirectoryObservation):
                raise TypeError("DIRECTORY entries must be DirectoryObservation")
            entries.append(
                GalleryObservationDirectoryEntry(
                    start + offset,
                    source.name_bytes,
                    source.size_bytes,
                    source.device,
                    source.inode,
                    source.modified_ns,
                    source.changed_ns,
                    source.file_type,
                )
            )
            if source.file_type is GalleryObservationDirectoryFileType.REGULAR:
                regular_count += 1
    elif component is GalleryObservationComponent.TAG:
        for offset, source in enumerate(source_entries):
            if not isinstance(source, TagObservation):
                raise TypeError("TAG entries must be TagObservation")
            entries.append(
                GalleryObservationTagEntry(
                    start + offset,
                    source.namespace,
                    source._value_sha256,
                )
            )
            semantic.extend(len(source._value_bytes).to_bytes(4, "big"))
            semantic.extend(source._value_bytes)
    else:
        if len(source_entries) != 1 or not isinstance(source_entries[0], bytes):
            raise TypeError("METADATA commit needs exactly one bytes chunk")
        chunk = require_bounded_bytes(
            source_entries[0],
            field="METADATA chunk",
            minimum=1,
            maximum=32_768,
        )
        entries.append(GalleryObservationMetadataChunk(start, chunk))
    item_count = (
        len(entries[0].chunk_bytes)
        if component is GalleryObservationComponent.METADATA
        else len(entries)
    )
    page = GalleryObservationPage(
        component,
        GalleryObservationNodeKind.LEAF,
        0,
        item_count,
        tuple(entries),
    )
    page_bytes = encode_gallery_observation_page(page)
    return _PreparedPage(
        component,
        page,
        page_bytes,
        gallery_observation_page_digest(page_bytes),
        bytes(semantic),
        item_count,
        regular_count,
        byte_count_delta,
    )


def _require_leaf_batch_shape(
    prepared: _PreparedPage,
    *,
    terminal: bool,
    start: int,
) -> None:
    capacity = _LEAF_CAPACITY[prepared.component]
    entry_count = len(prepared.page.entries)
    if prepared.component is GalleryObservationComponent.METADATA:
        chunk = prepared.page.entries[0]
        assert isinstance(chunk, GalleryObservationMetadataChunk)
        if not terminal and len(chunk.chunk_bytes) != 32_768:
            raise GalleryStagingConflictError(
                "nonterminal METADATA chunks must contain exactly 32768 bytes"
            )
        return
    if not terminal and entry_count != capacity:
        raise GalleryStagingConflictError(
            f"nonterminal {prepared.component.name} leaves require {capacity} records"
        )
    if terminal and entry_count == 0 and start != 0:
        raise GalleryStagingConflictError(
            "only a whole empty component may use an empty terminal leaf"
        )
    if terminal and entry_count > capacity:
        raise GalleryStagingConflictError("terminal leaf exceeds its capacity")


def _require_leaf_adjacency(
    connector: Any,
    staging_id: bytes,
    prepared: _PreparedPage,
    *,
    previous_request: bytes | None,
) -> None:
    if previous_request is None or not prepared.page.entries:
        return
    previous = connector.fetch_one(
        f"SELECT p.page_bytes FROM {_REQUEST_PAGE} rp JOIN {_PAGE} p "
        "ON p.page_sha256 = rp.page_sha256 WHERE rp.request_sha256 = %s",
        (previous_request,),
    )
    if len(previous) != 1:
        raise GalleryStagingConflictError("prior request page is missing")
    prior_page = decode_gallery_observation_page(previous[0])
    if prior_page.component is not prepared.component or prior_page.level != 0:
        raise GalleryStagingConflictError("prior request page coordinate differs")
    if prepared.component is GalleryObservationComponent.DIRECTORY:
        prior_last = prior_page.entries[-1]
        current_first = prepared.page.entries[0]
        assert isinstance(prior_last, GalleryObservationDirectoryEntry)
        assert isinstance(current_first, GalleryObservationDirectoryEntry)
        if current_first.name_bytes <= prior_last.name_bytes:
            raise GalleryStagingConflictError(
                "DIRECTORY names are not globally strict byte-ordered"
            )
    del staging_id


def _encode_page_request(
    handle: GalleryStagingHandle,
    prepared: _PreparedPage,
    *,
    start_cursor: int,
    start_processed_byte_count: int,
    terminal: bool,
    predecessor: bytes | None,
    attempt: BatchAttempt,
) -> bytes:
    return _bounded_request_frame(
        b"P",
        handle,
        (
            bytes((int(prepared.component),)),
            prepared.page.level.to_bytes(1, "big"),
            start_cursor.to_bytes(8, "big"),
            start_processed_byte_count.to_bytes(8, "big"),
            bytes((int(terminal),)),
            _encode_optional_digest(predecessor),
            _encode_attempt(attempt),
            len(prepared.semantic_bytes).to_bytes(4, "big"),
            prepared.semantic_bytes,
            len(prepared.page_bytes).to_bytes(4, "big"),
            prepared.page_bytes,
        ),
    )


def _encode_internal_page_request(
    handle: GalleryStagingHandle,
    prepared: _PreparedPage,
    *,
    start_cursor: int,
    start_processed_byte_count: int,
    terminal: bool,
    predecessor: bytes | None,
) -> bytes:
    return _bounded_request_frame(
        b"B",
        handle,
        (
            bytes((int(prepared.component),)),
            prepared.page.level.to_bytes(1, "big"),
            start_cursor.to_bytes(8, "big"),
            start_processed_byte_count.to_bytes(8, "big"),
            bytes((int(terminal),)),
            _encode_optional_digest(predecessor),
            len(prepared.semantic_bytes).to_bytes(4, "big"),
            prepared.semantic_bytes,
            len(prepared.page_bytes).to_bytes(4, "big"),
            prepared.page_bytes,
        ),
    )


def _bounded_request_frame(
    subtype: bytes,
    handle: GalleryStagingHandle,
    body: Iterable[bytes],
) -> bytes:
    frame = b"".join(
        (
            _REQUEST_PREFIX,
            _REQUEST_VERSION.to_bytes(4, "big"),
            subtype,
            handle.staging_id,
            handle.build_id,
            handle.gallery_id.to_bytes(8, "big"),
            handle.observation_id.to_bytes(8, "big"),
            handle.ingest_generation.to_bytes(8, "big"),
            handle.claim_generation.to_bytes(8, "big"),
            *body,
        )
    )
    if len(frame) > _REQUEST_BYTES_MAXIMUM:
        raise GalleryStagingConflictError(
            "derived request frame exceeds three 32768-byte chunks"
        )
    return frame


def _persist_request_identity(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    frame: bytes,
    *,
    predecessor: bytes | None,
    owner_lock_level: int,
) -> bytes:
    request_sha256 = sha256(frame).digest()
    lock_level = require_int63(owner_lock_level, field="request owner lock level")
    owner_rows: dict[bytes, tuple[Any, ...]] = {}
    owner_digests = [request_sha256]
    if predecessor is not None and predecessor != request_sha256:
        owner_digests.append(predecessor)
    for digest in sorted(owner_digests):
        owner_rows[digest] = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "gallery-staging-request",
                handle.staging_id,
                lock_level,
                0,
                digest,
            ),
            f"SELECT staging_id FROM {_REQUEST_OWNER} WHERE request_sha256 = %s",
            (digest,),
        )
    existing = work.connector.fetch_one(
        f"SELECT request_sha256 FROM {_REQUEST} WHERE request_sha256 = %s",
        (request_sha256,),
    )
    if existing:
        stored = _load_request_bytes(work.connector, request_sha256)
        if stored != frame:
            raise GalleryStagingConflictError(
                "request digest collision has a different complete preimage"
            )
        owner = owner_rows[request_sha256]
        if owner != (handle.staging_id,):
            raise GalleryStagingConflictError(
                "request digest belongs to another staging"
            )
        raise GalleryStagingConflictError(
            "an old request identity cannot reserve a new checkpoint prestate"
        )
    if predecessor is not None:
        prior_owner = owner_rows[predecessor]
        if prior_owner != (handle.staging_id,):
            raise GalleryStagingConflictError(
                "request predecessor belongs to another staging"
            )
        successor = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "gallery-staging-request",
                handle.staging_id,
                lock_level,
                1,
                predecessor,
            ),
            f"SELECT request_sha256 FROM {_PREDECESSOR} "
            "WHERE prior_request_sha256 = %s",
            (predecessor,),
        )
        if successor:
            raise GalleryStagingConflictError(
                "request predecessor already has a successor"
            )
    work.connector.execute(
        f"INSERT INTO {_REQUEST} (request_sha256) VALUES (%s)",
        (request_sha256,),
    )
    for position, offset in enumerate(range(0, len(frame), _REQUEST_CHUNK_BYTES)):
        work.connector.execute(
            f"INSERT INTO {_REQUEST_CHUNK} "
            "(request_sha256, position, request_bytes) VALUES (%s, %s, %s)",
            (
                request_sha256,
                position,
                frame[offset : offset + _REQUEST_CHUNK_BYTES],
            ),
        )
    work.connector.execute(
        f"INSERT INTO {_REQUEST_OWNER} (request_sha256, staging_id) VALUES (%s, %s)",
        (request_sha256, handle.staging_id),
    )
    if predecessor is not None:
        work.connector.execute(
            f"INSERT INTO {_PREDECESSOR} "
            "(request_sha256, prior_request_sha256) VALUES (%s, %s)",
            (request_sha256, predecessor),
        )
    return request_sha256


def _load_request_bytes(connector: Any, request_sha256: bytes) -> bytes:
    rows = connector.fetch_all(
        f"SELECT position, request_bytes FROM {_REQUEST_CHUNK} "
        "WHERE request_sha256 = %s ORDER BY position",
        (request_sha256,),
    )
    if not rows or len(rows) > _REQUEST_CHUNK_COUNT:
        raise GalleryStagingConflictError("request chunks are missing or excessive")
    output = bytearray()
    for expected, row in enumerate(rows):
        if len(row) != 2 or row[0] != expected:
            raise GalleryStagingConflictError("request chunks are not consecutive")
        chunk = require_bounded_bytes(
            row[1],
            field="persisted request chunk",
            minimum=1,
            maximum=_REQUEST_CHUNK_BYTES,
        )
        if expected < len(rows) - 1 and len(chunk) != _REQUEST_CHUNK_BYTES:
            raise GalleryStagingConflictError("nonfinal request chunk is not full")
        output.extend(chunk)
    if len(output) > _REQUEST_BYTES_MAXIMUM:
        raise GalleryStagingConflictError("request frame exceeds its bound")
    frame = bytes(output)
    if sha256(frame).digest() != request_sha256:
        raise GalleryStagingConflictError(
            "request chunks do not recompute their digest"
        )
    return frame


def _try_replay_page(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
    source_entries: Sequence[Any],
    terminal: bool,
    attempt: BatchAttempt,
    checkpoint: _Checkpoint,
    latest: tuple[Any, ...],
) -> GalleryStagingReceipt | None:
    if len(latest) != 6:
        raise GalleryStagingConflictError("latest page receipt has bad shape")
    request_sha256 = require_digest32(latest[0], field="latest request_sha256")
    start = require_int63(latest[1], field="latest start_cursor")
    stored_terminal = _decode_bool(latest[2], field_name="latest terminal")
    stored_page_sha = require_digest32(latest[3], field="latest page_sha256")
    start_processed_byte_count = require_int63(
        latest[4], field="latest start_processed_byte_count"
    )
    next_processed_byte_count = require_int63(
        latest[5], field="latest next_processed_byte_count"
    )
    if component is not GalleryObservationComponent.FILE and (
        start_processed_byte_count != 0 or next_processed_byte_count != 0
    ):
        raise GalleryStagingConflictError("non-FILE replay receipt has byte authority")
    if stored_terminal is not terminal:
        return None
    prepared = _prepare_leaf(component, start, source_entries)
    if prepared.page_sha256 != stored_page_sha:
        return None
    predecessor_row = work.connector.fetch_one(
        f"SELECT prior_request_sha256 FROM {_PREDECESSOR} WHERE request_sha256 = %s",
        (request_sha256,),
    )
    predecessor = (
        None
        if not predecessor_row
        else require_digest32(predecessor_row[0], field="replayed predecessor")
    )
    candidate = _encode_page_request(
        handle,
        prepared,
        start_cursor=start,
        start_processed_byte_count=start_processed_byte_count,
        terminal=terminal,
        predecessor=predecessor,
        attempt=attempt,
    )
    if candidate != _load_request_bytes(work.connector, request_sha256):
        return None
    expected_cursor = start + prepared.item_count
    if expected_cursor > INT63_MAX:
        raise GalleryStagingConflictError("replayed cursor exceeds signed int63")
    expected_processed_byte_count = _checked_int63_add(
        start_processed_byte_count,
        prepared.byte_count_delta,
        field_name="replayed processed byte count",
    )
    expected_state = "COMPLETE" if terminal else "OPEN"
    if (
        next_processed_byte_count != expected_processed_byte_count
        or checkpoint.cursor != expected_cursor
        or checkpoint.processed_byte_count != next_processed_byte_count
        or checkpoint.state != expected_state
    ):
        raise GalleryStagingConflictError(
            "replayed request does not match durable checkpoint poststate"
        )
    root = _component_root(work.connector, handle, component)[0] if terminal else None
    return GalleryStagingReceipt(
        request_sha256,
        component,
        checkpoint.cursor,
        checkpoint.processed_byte_count,
        checkpoint.state,
        root,
        True,
    )


def _persist_observation_page(
    connector: Any,
    handle: GalleryStagingHandle,
    prepared: _PreparedPage,
) -> None:
    existing = connector.fetch_one(
        f"SELECT page_bytes FROM {_PAGE} WHERE page_sha256 = %s",
        (prepared.page_sha256,),
    )
    if existing:
        if existing != (prepared.page_bytes,):
            raise GalleryStagingConflictError(
                "page digest collision has different exact bytes"
            )
    else:
        connector.execute(
            f"INSERT INTO {_PAGE} (page_sha256, page_bytes) VALUES (%s, %s)",
            (prepared.page_sha256, prepared.page_bytes),
        )
    descriptor = (
        prepared.page_sha256,
        _COMPONENT_BYTES[prepared.component],
        prepared.page.level,
        prepared.page.subtree_item_count,
    )
    _insert_or_require(
        connector,
        label="gallery page descriptor",
        select_sql=(
            f"SELECT page_sha256, component, level, subtree_item_count "
            f"FROM {_PAGE_DESCRIPTOR} WHERE page_sha256 = %s"
        ),
        select_data=(prepared.page_sha256,),
        insert_sql=(
            f"INSERT INTO {_PAGE_DESCRIPTOR} "
            "(page_sha256, component, level, subtree_item_count) "
            "VALUES (%s, %s, %s, %s)"
        ),
        insert_data=descriptor,
        expected=descriptor,
    )
    child_bounds: dict[bytes, tuple[bytes, bytes]] = {}
    if prepared.page.node_kind is GalleryObservationNodeKind.BRANCH:
        for position, entry in enumerate(prepared.page.entries):
            assert isinstance(entry, GalleryObservationBranchEntry)
            child_descriptor = connector.fetch_one(
                f"SELECT component, level, subtree_item_count "
                f"FROM {_PAGE_DESCRIPTOR} WHERE page_sha256 = %s",
                (entry.child_sha256,),
            )
            if child_descriptor != (
                _COMPONENT_BYTES[prepared.component],
                prepared.page.level - 1,
                entry.child_subtree_item_count,
            ):
                raise GalleryStagingConflictError("branch child descriptor differs")
            child_bound = connector.fetch_one(
                f"SELECT first_key, last_key FROM {_PAGE_BOUNDS} "
                "WHERE page_sha256 = %s",
                (entry.child_sha256,),
            )
            if len(child_bound) != 2:
                raise GalleryStagingConflictError("nonempty branch child lacks bounds")
            child_bounds[entry.child_sha256] = (child_bound[0], child_bound[1])
            _insert_or_require(
                connector,
                label="gallery page child",
                select_sql=(
                    f"SELECT parent_sha256, position, child_sha256 FROM {_PAGE_CHILD} "
                    "WHERE parent_sha256 = %s AND position = %s"
                ),
                select_data=(prepared.page_sha256, position),
                insert_sql=(
                    f"INSERT INTO {_PAGE_CHILD} "
                    "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)"
                ),
                insert_data=(prepared.page_sha256, position, entry.child_sha256),
                expected=(prepared.page_sha256, position, entry.child_sha256),
            )
    bounds = gallery_observation_page_key_bounds(
        prepared.page,
        child_bounds=child_bounds or None,
    )
    existing_bounds = connector.fetch_one(
        f"SELECT first_key, last_key FROM {_PAGE_BOUNDS} WHERE page_sha256 = %s",
        (prepared.page_sha256,),
    )
    if bounds is None:
        if existing_bounds:
            raise GalleryStagingConflictError("empty page unexpectedly has bounds")
    elif existing_bounds:
        if existing_bounds != bounds:
            raise GalleryStagingConflictError("page key bounds differ")
    else:
        connector.execute(
            f"INSERT INTO {_PAGE_BOUNDS} (page_sha256, first_key, last_key) "
            "VALUES (%s, %s, %s)",
            (prepared.page_sha256, bounds[0], bounds[1]),
        )
    association = connector.fetch_one(
        f"SELECT gallery_id, observation_id, page_sha256 FROM {_ALLOCATION_PAGE} "
        "WHERE gallery_id = %s AND observation_id = %s AND page_sha256 = %s",
        (handle.gallery_id, handle.observation_id, prepared.page_sha256),
    )
    expected_association = (
        handle.gallery_id,
        handle.observation_id,
        prepared.page_sha256,
    )
    if association:
        if association != expected_association:
            raise GalleryStagingConflictError("allocation-page association differs")
    else:
        connector.execute(
            f"INSERT INTO {_ALLOCATION_PAGE} "
            "(gallery_id, observation_id, page_sha256) VALUES (%s, %s, %s)",
            expected_association,
        )


def _persist_normalized_leaf_facts(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    prepared: _PreparedPage,
    source_entries: Sequence[Any],
    *,
    now: int,
) -> None:
    connector = work.connector
    if prepared.component is GalleryObservationComponent.FILE:
        for page_entry, source in zip(
            prepared.page.entries, source_entries, strict=True
        ):
            assert isinstance(page_entry, GalleryObservationFileEntry)
            assert isinstance(source, FileObservation)
            role = file_role(source.name_bytes)
            _insert_or_require(
                connector,
                label="file-name identity",
                select_sql=(
                    "SELECT file_key, name_bytes, file_role "
                    "FROM catalog_file_name_identities WHERE file_key = %s"
                ),
                select_data=(page_entry.file_key,),
                insert_sql=(
                    "INSERT INTO catalog_file_name_identities "
                    "(file_key, name_bytes, file_role) VALUES (%s, %s, %s)"
                ),
                insert_data=(page_entry.file_key, source.name_bytes, role),
                expected=(page_entry.file_key, source.name_bytes, role),
            )
            _insert_or_require(
                connector,
                label="content blob",
                select_sql=(
                    "SELECT file_sha256, size_bytes FROM catalog_content_blobs "
                    "WHERE file_sha256 = %s"
                ),
                select_data=(source.content.file_sha256,),
                insert_sql=(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, %s)"
                ),
                insert_data=(
                    source.content.file_sha256,
                    source.content.size_bytes,
                ),
                expected=(source.content.file_sha256, source.content.size_bytes),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_files "
                "(gallery_id, observation_id, file_no, file_key, file_sha256) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    handle.gallery_id,
                    handle.observation_id,
                    page_entry.file_no,
                    page_entry.file_key,
                    source.content.file_sha256,
                ),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_filesystem "
                "(gallery_id, observation_id, file_key, device, inode, "
                "modified_ns, changed_ns) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    handle.gallery_id,
                    handle.observation_id,
                    page_entry.file_key,
                    source.device.to_bytes(8, "big"),
                    source.inode.to_bytes(8, "big"),
                    source.modified_ns.to_bytes(8, "big", signed=True),
                    source.changed_ns.to_bytes(8, "big", signed=True),
                ),
            )
    elif prepared.component is GalleryObservationComponent.TAG:
        for page_entry, source in zip(
            prepared.page.entries, source_entries, strict=True
        ):
            assert isinstance(page_entry, GalleryObservationTagEntry)
            assert isinstance(source, TagObservation)
            _persist_canonical_value(
                work,
                generation=handle.ingest_generation,
                digest_domain=_TAG_VALUE_DOMAIN,
                payload=source._value_bytes,
                now=now,
                retain_claim=True,
            )
            row = connector.fetch_one(
                "SELECT tag_id FROM catalog_tag_terms "
                "WHERE namespace = %s AND tag_value_sha256 = %s",
                (source._namespace_bytes, source._value_sha256),
            )
            if row:
                tag_id = require_positive_int63(row[0], field="tag_id")
            else:
                tag_id = VNextAllocatorRepository.allocate_identity(
                    work,
                    IdentityStream.TAG,
                    updated_at=now,
                )
                connector.execute(
                    "INSERT INTO catalog_tag_terms "
                    "(tag_id, namespace, tag_value_sha256) VALUES (%s, %s, %s)",
                    (tag_id, source._namespace_bytes, source._value_sha256),
                )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_tags "
                "(gallery_id, observation_id, position, tag_id) "
                "VALUES (%s, %s, %s, %s)",
                (
                    handle.gallery_id,
                    handle.observation_id,
                    page_entry.position,
                    tag_id,
                ),
            )
            deleted = connector.execute_affected(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (handle.ingest_generation, source._value_sha256),
            )
            if deleted != 1:
                raise GalleryStagingConflictError(
                    "tag canonical claim changed before tag-term handoff"
                )


def _push_frontier(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
    *,
    level: int,
    request_sha256: bytes,
    page_sha256: bytes,
    subtree_count: int,
    now: int,
) -> None:
    if level > 8:
        raise OverflowError("gallery observation tree exceeds level eight")
    pending = _frontier_pages(work.connector, handle.staging_id, component, level)
    if len(pending) < 255:
        work.connector.execute(
            f"INSERT INTO {_FRONTIER} (request_sha256, position) VALUES (%s, %s)",
            (request_sha256, len(pending)),
        )
        return
    children = [
        *pending,
        _FrontierPage(request_sha256, 255, page_sha256, subtree_count),
    ]
    _delete_frontier(work.connector, pending)
    branch_request, branch_page, branch_count = _commit_branch(
        work,
        handle,
        component,
        level=level + 1,
        children=children,
        terminal=False,
        now=now,
    )
    _push_frontier(
        work,
        handle,
        component,
        level=level + 1,
        request_sha256=branch_request,
        page_sha256=branch_page,
        subtree_count=branch_count,
        now=now,
    )


def _commit_branch(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
    *,
    level: int,
    children: Sequence[_FrontierPage],
    terminal: bool,
    now: int,
) -> tuple[bytes, bytes, int]:
    if not 1 <= len(children) <= 256:
        raise GalleryStagingConflictError("branch fanout must be in 1..256")
    if level > 8:
        raise OverflowError("gallery observation tree exceeds level eight")
    total = sum(child.subtree_item_count for child in children)
    if total > INT63_MAX:
        raise OverflowError("gallery observation subtree count exceeds int63")
    page = GalleryObservationPage(
        component,
        GalleryObservationNodeKind.BRANCH,
        level,
        total,
        tuple(
            GalleryObservationBranchEntry(
                child.page_sha256,
                child.subtree_item_count,
            )
            for child in children
        ),
    )
    page_bytes = encode_gallery_observation_page(page)
    prepared = _PreparedPage(
        component,
        page,
        page_bytes,
        gallery_observation_page_digest(page_bytes),
        b"".join(
            child.page_sha256 + child.subtree_item_count.to_bytes(8, "big")
            for child in children
        ),
        total,
        0,
        0,
    )
    checkpoint = _ensure_internal_checkpoint(
        work.connector,
        handle.staging_id,
        component,
        level,
        now=now,
    )
    if checkpoint.processed_byte_count != 0:
        raise GalleryStagingConflictError(
            "internal checkpoint has nonzero processed_byte_count"
        )
    latest = _latest_page_receipt(work.connector, handle.staging_id, component, level)
    predecessor = (
        None
        if not latest
        else require_digest32(latest[0], field="prior branch request_sha256")
    )
    frame = _encode_internal_page_request(
        handle,
        prepared,
        start_cursor=checkpoint.cursor,
        start_processed_byte_count=0,
        terminal=terminal,
        predecessor=predecessor,
    )
    request_sha256 = _persist_request_identity(
        work,
        handle,
        frame,
        predecessor=predecessor,
        owner_lock_level=level,
    )
    _persist_observation_page(work.connector, handle, prepared)
    work.connector.execute(
        f"INSERT INTO {_PAGE_REQUEST} "
        "(request_sha256, staging_id, component, level, start_cursor, terminal) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            request_sha256,
            handle.staging_id,
            _COMPONENT_BYTES[component],
            level,
            checkpoint.cursor,
            int(terminal),
        ),
    )
    work.connector.execute(
        f"INSERT INTO {_REQUEST_PAGE} (request_sha256, page_sha256) VALUES (%s, %s)",
        (request_sha256, prepared.page_sha256),
    )
    _replace_page_receipt(
        work,
        handle.staging_id,
        component,
        level=level,
        previous=predecessor,
        request_sha256=request_sha256,
        start_processed_byte_count=0,
        next_processed_byte_count=0,
        now=now,
    )
    next_cursor = checkpoint.cursor + len(children)
    work.compare_and_swap(
        f"UPDATE {_CHECKPOINT} SET cursor = %s, processed_byte_count = 0, "
        "state = %s, updated_at = %s "
        "WHERE staging_id = %s AND component = %s AND level = %s "
        "AND cursor = %s AND regular_count = %s AND processed_byte_count = 0 "
        "AND state = %s AND updated_at = %s",
        (
            next_cursor,
            "COMPLETE" if terminal else "OPEN",
            now,
            handle.staging_id,
            _COMPONENT_BYTES[component],
            level,
            checkpoint.cursor,
            checkpoint.regular_count,
            checkpoint.state,
            checkpoint.updated_at,
        ),
        authority=f"gallery {component.name} level-{level} checkpoint",
    )
    return request_sha256, prepared.page_sha256, total


def _finish_component(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
    *,
    now: int,
) -> bytes:
    while True:
        by_level = {
            level: _frontier_pages(
                work.connector,
                handle.staging_id,
                component,
                level,
            )
            for level in range(9)
        }
        nonempty = [level for level, pages in by_level.items() if pages]
        total_pages = sum(len(pages) for pages in by_level.values())
        if total_pages == 1:
            root = by_level[nonempty[0]][0]
            break
        if not nonempty:
            raise GalleryStagingConflictError("component frontier is empty")
        level = min(nonempty)
        children = by_level[level]
        higher_exists = any(candidate > level for candidate in nonempty)
        if len(children) == 1 and not higher_exists:
            root = children[0]
            break
        _delete_frontier(work.connector, children)
        request, page, count = _commit_branch(
            work,
            handle,
            component,
            level=level + 1,
            children=children,
            terminal=not higher_exists,
            now=now,
        )
        _push_frontier(
            work,
            handle,
            component,
            level=level + 1,
            request_sha256=request,
            page_sha256=page,
            subtree_count=count,
            now=now,
        )
    _insert_or_require(
        work.connector,
        label="gallery component root",
        select_sql=(
            f"SELECT gallery_id, observation_id, root_page_sha256 FROM {_TREE_ROOT} "
            "WHERE gallery_id = %s AND observation_id = %s "
            "AND root_page_sha256 = %s"
        ),
        select_data=(handle.gallery_id, handle.observation_id, root.page_sha256),
        insert_sql=(
            f"INSERT INTO {_TREE_ROOT} "
            "(gallery_id, observation_id, root_page_sha256) VALUES (%s, %s, %s)"
        ),
        insert_data=(handle.gallery_id, handle.observation_id, root.page_sha256),
        expected=(handle.gallery_id, handle.observation_id, root.page_sha256),
    )
    # Every lazily-created internal checkpoint is terminal once its one root is
    # fixed.  The level-zero checkpoint was already completed by the caller.
    work.connector.execute_affected(
        f"UPDATE {_CHECKPOINT} SET state = %s, updated_at = %s "
        "WHERE staging_id = %s AND component = %s AND level > 0 AND state = %s",
        ("COMPLETE", now, handle.staging_id, _COMPONENT_BYTES[component], "OPEN"),
    )
    return root.page_sha256


def _frontier_pages(
    connector: Any,
    staging_id: bytes,
    component: GalleryObservationComponent,
    level: int,
) -> list[_FrontierPage]:
    rows = connector.fetch_all(
        f"SELECT f.request_sha256, f.position, rp.page_sha256, "
        f"d.subtree_item_count FROM {_FRONTIER} f "
        f"JOIN {_PAGE_REQUEST} q ON q.request_sha256 = f.request_sha256 "
        f"JOIN {_REQUEST_PAGE} rp ON rp.request_sha256 = f.request_sha256 "
        f"JOIN {_PAGE_DESCRIPTOR} d ON d.page_sha256 = rp.page_sha256 "
        "WHERE q.staging_id = %s AND q.component = %s AND q.level = %s "
        "ORDER BY f.position",
        (staging_id, _COMPONENT_BYTES[component], level),
    )
    if len(rows) > 255:
        raise GalleryStagingConflictError("frontier exceeds 255 pending pages")
    output: list[_FrontierPage] = []
    for expected, row in enumerate(rows):
        if len(row) != 4 or row[1] != expected:
            raise GalleryStagingConflictError("frontier positions are not contiguous")
        output.append(
            _FrontierPage(
                require_digest32(row[0], field="frontier request_sha256"),
                expected,
                require_digest32(row[2], field="frontier page_sha256"),
                require_int63(row[3], field="frontier subtree_item_count"),
            )
        )
    return output


def _delete_frontier(connector: Any, pages: Sequence[_FrontierPage]) -> None:
    for page in pages:
        affected = connector.execute_affected(
            f"DELETE FROM {_FRONTIER} WHERE request_sha256 = %s AND position = %s",
            (page.request_sha256, page.position),
        )
        if affected != 1:
            raise GalleryStagingConflictError("frontier changed during carry")


def _ensure_internal_checkpoint(
    connector: Any,
    staging_id: bytes,
    component: GalleryObservationComponent,
    level: int,
    *,
    now: int,
) -> _Checkpoint:
    row = connector.fetch_one(
        f"SELECT cursor, regular_count, processed_byte_count, state, updated_at "
        f"FROM {_CHECKPOINT} "
        "WHERE staging_id = %s AND component = %s AND level = %s",
        (staging_id, _COMPONENT_BYTES[component], level),
    )
    if not row:
        connector.execute(
            f"INSERT INTO {_CHECKPOINT} "
            "(staging_id, component, level, cursor, regular_count, "
            "processed_byte_count, state, updated_at) "
            "VALUES (%s, %s, %s, 0, 0, 0, %s, %s)",
            (staging_id, _COMPONENT_BYTES[component], level, "OPEN", now),
        )
        return _Checkpoint(0, 0, 0, "OPEN", now)
    checkpoint = _decode_checkpoint(row)
    if checkpoint.state != "OPEN":
        raise GalleryStagingConflictError("internal checkpoint is already complete")
    return checkpoint


def _persist_canonical_value(
    work: VNextUnitOfWork,
    *,
    generation: int,
    digest_domain: str,
    payload: bytes,
    now: int,
    retain_claim: bool,
) -> bytes:
    value_sha256 = canonical_value_digest(digest_domain, payload)
    domain_bytes = digest_domain.encode("ascii")
    if work.connector.fetch_one(
        "SELECT digest_domain FROM catalog_canonical_digest_policies "
        "WHERE digest_domain = %s",
        (domain_bytes,),
    ) != (domain_bytes,):
        raise GalleryStagingNotReadyError(
            f"canonical digest policy {digest_domain!r} is not registered"
        )
    allocation = work.connector.fetch_one(
        "SELECT digest_domain, byte_count FROM catalog_canonical_value_allocations "
        "WHERE value_sha256 = %s",
        (value_sha256,),
    )
    if allocation:
        if allocation != (domain_bytes, len(payload)):
            raise GalleryStagingConflictError("canonical digest preimage differs")
    else:
        work.connector.execute(
            "INSERT INTO catalog_canonical_value_allocations "
            "(value_sha256, digest_domain, byte_count, allocated_at) "
            "VALUES (%s, %s, %s, %s)",
            (value_sha256, domain_bytes, len(payload), now),
        )
    claim = work.connector.fetch_one(
        "SELECT generation, value_sha256 FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, value_sha256),
    )
    if claim:
        if claim != (generation, value_sha256):
            raise GalleryStagingConflictError("canonical upload claim differs")
    else:
        work.connector.execute(
            "INSERT INTO operational_canonical_value_uploads "
            "(generation, value_sha256) VALUES (%s, %s)",
            (generation, value_sha256),
        )
    tree = build_canonical_value_tree(value_sha256, len(payload), (payload,))
    for encoded in tree.pages:
        page = decode_canonical_value_page(encoded.page_bytes)
        _insert_or_require(
            work.connector,
            label="canonical page",
            select_sql=(
                "SELECT page_sha256, value_sha256, page_bytes "
                "FROM catalog_canonical_value_pages WHERE page_sha256 = %s"
            ),
            select_data=(encoded.page_sha256,),
            insert_sql=(
                "INSERT INTO catalog_canonical_value_pages "
                "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)"
            ),
            insert_data=(encoded.page_sha256, value_sha256, encoded.page_bytes),
            expected=(encoded.page_sha256, value_sha256, encoded.page_bytes),
        )
        descriptor = (
            encoded.page_sha256,
            value_sha256,
            page.level,
            page.page_position,
            page.subtree_byte_count,
        )
        _insert_or_require(
            work.connector,
            label="canonical page descriptor",
            select_sql=(
                "SELECT page_sha256, value_sha256, level, page_position, "
                "subtree_item_count FROM catalog_canonical_value_page_descriptors "
                "WHERE page_sha256 = %s"
            ),
            select_data=(encoded.page_sha256,),
            insert_sql=(
                "INSERT INTO catalog_canonical_value_page_descriptors "
                "(page_sha256, value_sha256, level, page_position, "
                "subtree_item_count) VALUES (%s, %s, %s, %s, %s)"
            ),
            insert_data=descriptor,
            expected=descriptor,
        )
        if page.node_kind is GalleryObservationNodeKind.BRANCH:
            for position, entry in enumerate(page.entries):
                _insert_or_require(
                    work.connector,
                    label="canonical parent edge",
                    select_sql=(
                        "SELECT child_sha256, parent_sha256, position "
                        "FROM catalog_canonical_value_page_parents "
                        "WHERE child_sha256 = %s"
                    ),
                    select_data=(entry.child_page_sha256,),  # type: ignore[union-attr]
                    insert_sql=(
                        "INSERT INTO catalog_canonical_value_page_parents "
                        "(child_sha256, parent_sha256, position) "
                        "VALUES (%s, %s, %s)"
                    ),
                    insert_data=(
                        entry.child_page_sha256,  # type: ignore[union-attr]
                        encoded.page_sha256,
                        position,
                    ),
                    expected=(
                        entry.child_page_sha256,  # type: ignore[union-attr]
                        encoded.page_sha256,
                        position,
                    ),
                )
    _insert_or_require(
        work.connector,
        label="canonical identity",
        select_sql=(
            "SELECT value_sha256, root_page_sha256 "
            "FROM catalog_canonical_value_identities WHERE value_sha256 = %s"
        ),
        select_data=(value_sha256,),
        insert_sql=(
            "INSERT INTO catalog_canonical_value_identities "
            "(value_sha256, root_page_sha256) VALUES (%s, %s)"
        ),
        insert_data=(value_sha256, tree.root_page_sha256),
        expected=(value_sha256, tree.root_page_sha256),
    )
    if not retain_claim:
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value_sha256),
        )
        if deleted != 1:
            raise GalleryStagingConflictError("canonical upload claim changed")
    return tree.root_page_sha256


def _match_batch_rows(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    *,
    start_file: int,
) -> tuple[list[tuple[Any, ...]], bool]:
    rows = work.connector.fetch_all(
        "SELECT f.file_no, f.file_key, n.name_bytes, f.file_sha256, b.size_bytes, "
        "s.device, s.inode, s.modified_ns, s.changed_ns "
        "FROM catalog_gallery_observation_files f "
        "JOIN catalog_file_name_identities n ON n.file_key = f.file_key "
        "JOIN catalog_content_blobs b ON b.file_sha256 = f.file_sha256 "
        "JOIN catalog_gallery_observation_file_filesystem s "
        "ON s.gallery_id = f.gallery_id AND s.observation_id = f.observation_id "
        "AND s.file_key = f.file_key "
        "WHERE f.gallery_id = %s AND f.observation_id = %s AND f.file_no >= %s "
        "ORDER BY f.file_no LIMIT 257",
        (handle.gallery_id, handle.observation_id, start_file),
    )
    terminal = len(rows) <= 256
    selected = rows[:256]
    for offset, row in enumerate(selected):
        if row[0] != start_file + offset:
            raise GalleryStagingConflictError("FILE rows are not contiguous")
        directory = _lookup_directory_entry(
            work.connector,
            handle,
            require_bounded_bytes(
                row[2], field="persisted file name", minimum=1, maximum=255
            ),
        )
        if directory.file_type is not GalleryObservationDirectoryFileType.REGULAR:
            raise GalleryStagingConflictError("FILE maps to a nonregular DIRECTORY row")
        expected = (
            row[2],
            row[4],
            int.from_bytes(row[5], "big"),
            int.from_bytes(row[6], "big"),
            int.from_bytes(row[7], "big", signed=True),
            int.from_bytes(row[8], "big", signed=True),
        )
        actual = (
            directory.name_bytes,
            directory.size_bytes,
            directory.device,
            directory.inode,
            directory.modified_ns,
            directory.changed_ns,
        )
        if actual != expected or file_key(directory.name_bytes) != row[1]:
            raise GalleryStagingConflictError("FILE and DIRECTORY facts differ")
    return selected, terminal


def _lookup_directory_entry(
    connector: Any,
    handle: GalleryStagingHandle,
    name_bytes: bytes,
) -> GalleryObservationDirectoryEntry:
    root, _count = _component_root(
        connector,
        handle,
        GalleryObservationComponent.DIRECTORY,
    )
    page_sha = root
    for expected_level in range(8, -1, -1):
        row = connector.fetch_one(
            f"SELECT p.page_bytes, d.level FROM {_PAGE} p JOIN {_PAGE_DESCRIPTOR} d "
            "ON d.page_sha256 = p.page_sha256 WHERE p.page_sha256 = %s",
            (page_sha,),
        )
        if len(row) != 2:
            raise GalleryStagingConflictError("DIRECTORY lookup page is missing")
        page = decode_gallery_observation_page(row[0])
        if page.component is not GalleryObservationComponent.DIRECTORY:
            raise GalleryStagingConflictError("DIRECTORY root crosses component")
        if page.level != row[1] or page.level > expected_level:
            raise GalleryStagingConflictError("DIRECTORY page level differs")
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            matches = [
                entry
                for entry in page.entries
                if isinstance(entry, GalleryObservationDirectoryEntry)
                and entry.name_bytes == name_bytes
            ]
            if len(matches) != 1:
                raise GalleryStagingConflictError(
                    "FILE has no unique DIRECTORY name match"
                )
            return matches[0]
        children = connector.fetch_all(
            f"SELECT c.child_sha256, b.first_key, b.last_key FROM {_PAGE_CHILD} c "
            f"JOIN {_PAGE_BOUNDS} b ON b.page_sha256 = c.child_sha256 "
            "WHERE c.parent_sha256 = %s ORDER BY c.position",
            (page_sha,),
        )
        if not 1 <= len(children) <= 256:
            raise GalleryStagingConflictError("DIRECTORY branch fanout differs")
        candidates = [row for row in children if row[1] <= name_bytes <= row[2]]
        if len(candidates) != 1:
            raise GalleryStagingConflictError(
                "DIRECTORY bounds do not select one exact child"
            )
        page_sha = require_digest32(candidates[0][0], field="directory child")
    raise GalleryStagingConflictError("DIRECTORY lookup exceeds depth eight")


def _encode_match_body(
    attempt: BatchAttempt,
    rows: Sequence[tuple[Any, ...]],
) -> bytes:
    output = bytearray(_encode_attempt(attempt))
    output.extend(len(rows).to_bytes(4, "big"))
    for row in rows:
        name = require_bounded_bytes(
            row[2], field="matched file name", minimum=1, maximum=255
        )
        output.extend(row[0].to_bytes(8, "big"))
        output.extend(row[1])
        output.extend(len(name).to_bytes(4, "big"))
        output.extend(name)
        output.extend(row[3])
        output.extend(row[4].to_bytes(8, "big"))
        output.extend(row[5])
        output.extend(row[6])
        output.extend(row[7])
        output.extend(row[8])
    return bytes(output)


def _encode_match_request(
    handle: GalleryStagingHandle,
    *,
    start_cursor: bytes,
    start_matched: int,
    terminal: bool,
    predecessor: bytes | None,
    body: bytes,
) -> bytes:
    return _bounded_request_frame(
        b"M",
        handle,
        (
            len(start_cursor).to_bytes(4, "big"),
            start_cursor,
            start_matched.to_bytes(8, "big"),
            bytes((int(terminal),)),
            _encode_optional_digest(predecessor),
            len(body).to_bytes(4, "big"),
            body,
        ),
    )


def _try_replay_match(
    work: VNextUnitOfWork,
    handle: GalleryStagingHandle,
    command: MatchBatchCommand,
    checkpoint: Any,
    latest: tuple[Any, ...],
) -> MatchBatchReceipt | None:
    if len(latest) != 4:
        raise GalleryStagingConflictError("latest match receipt has bad shape")
    request_sha = require_digest32(latest[0], field="latest match request")
    start_cursor = require_bounded_bytes(
        latest[1], field="match start cursor", maximum=2048
    )
    start_count = require_int63(latest[2], field="match start count")
    terminal = _decode_bool(latest[3], field_name="match terminal")
    rows, recomputed_terminal = _match_batch_rows(
        work,
        handle,
        start_file=_decode_file_cursor(start_cursor),
    )
    if terminal != recomputed_terminal:
        raise GalleryStagingConflictError("stored match terminal intent differs")
    predecessor_row = work.connector.fetch_one(
        f"SELECT prior_request_sha256 FROM {_PREDECESSOR} WHERE request_sha256 = %s",
        (request_sha,),
    )
    predecessor = (
        None
        if not predecessor_row
        else require_digest32(predecessor_row[0], field="match predecessor")
    )
    frame = _encode_match_request(
        handle,
        start_cursor=start_cursor,
        start_matched=start_count,
        terminal=terminal,
        predecessor=predecessor,
        body=_encode_match_body(
            BatchAttempt(command.operation_id, command.previous_operation_id),
            rows,
        ),
    )
    if frame != _load_request_bytes(work.connector, request_sha):
        return None
    post_count = start_count + len(rows)
    state = "COMPLETE" if terminal else "OPEN"
    if checkpoint.matched_count != post_count or checkpoint.state != state:
        raise GalleryStagingConflictError("match replay poststate differs")
    return MatchBatchReceipt(request_sha, post_count, state, True)


def _replace_page_receipt(
    work: VNextUnitOfWork,
    staging_id: bytes,
    component: GalleryObservationComponent,
    *,
    level: int,
    previous: bytes | None,
    request_sha256: bytes,
    start_processed_byte_count: int,
    next_processed_byte_count: int,
    now: int,
) -> None:
    start_bytes = require_int63(
        start_processed_byte_count,
        field="receipt start_processed_byte_count",
    )
    next_bytes = require_int63(
        next_processed_byte_count,
        field="receipt next_processed_byte_count",
    )
    if component is GalleryObservationComponent.FILE and level == 0:
        if next_bytes < start_bytes:
            raise GalleryStagingConflictError("FILE receipt byte count regressed")
    elif start_bytes != 0 or next_bytes != 0:
        raise GalleryStagingConflictError("non-FILE receipt byte count is nonzero")
    if previous is None:
        work.connector.execute(
            f"INSERT INTO {_RECEIPT} "
            "(staging_id, component, level, request_sha256, "
            "start_processed_byte_count, next_processed_byte_count, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                staging_id,
                _COMPONENT_BYTES[component],
                level,
                request_sha256,
                start_bytes,
                next_bytes,
                now,
            ),
        )
    else:
        work.compare_and_swap(
            f"UPDATE {_RECEIPT} SET request_sha256 = %s, "
            "start_processed_byte_count = %s, next_processed_byte_count = %s, "
            "committed_at = %s "
            "WHERE staging_id = %s AND component = %s AND level = %s "
            "AND request_sha256 = %s",
            (
                request_sha256,
                start_bytes,
                next_bytes,
                now,
                staging_id,
                _COMPONENT_BYTES[component],
                level,
                previous,
            ),
            authority=f"gallery {component.name} request receipt",
        )


def _replace_match_receipt(
    work: VNextUnitOfWork,
    staging_id: bytes,
    *,
    previous: bytes | None,
    request_sha256: bytes,
    now: int,
) -> None:
    if previous is None:
        work.connector.execute(
            f"INSERT INTO {_MATCH_RECEIPT} "
            "(staging_id, request_sha256, committed_at) VALUES (%s, %s, %s)",
            (staging_id, request_sha256, now),
        )
    else:
        work.compare_and_swap(
            f"UPDATE {_MATCH_RECEIPT} SET request_sha256 = %s, committed_at = %s "
            "WHERE staging_id = %s AND request_sha256 = %s",
            (request_sha256, now, staging_id, previous),
            authority="gallery match request receipt",
        )


def _latest_page_receipt(
    connector: Any,
    staging_id: bytes,
    component: GalleryObservationComponent,
    level: int,
) -> tuple[Any, ...]:
    row: tuple[Any, ...] = connector.fetch_one(
        f"SELECT r.request_sha256, q.start_cursor, q.terminal, p.page_sha256, "
        "r.start_processed_byte_count, r.next_processed_byte_count "
        f"FROM {_RECEIPT} r JOIN {_PAGE_REQUEST} q "
        "ON q.request_sha256 = r.request_sha256 "
        f"JOIN {_REQUEST_PAGE} p ON p.request_sha256 = r.request_sha256 "
        "WHERE r.staging_id = %s AND r.component = %s AND r.level = %s",
        (staging_id, _COMPONENT_BYTES[component], level),
    )
    return row


def _terminal_checkpoint_authority(
    connector: Any,
    handle: GalleryStagingHandle,
    checkpoints: dict[GalleryObservationComponent, _Checkpoint],
) -> tuple[int, int]:
    """Validate the four bounded latest receipts and return FILE scalars."""

    if set(checkpoints) != set(GalleryObservationComponent):
        raise GalleryStagingConflictError(
            "terminal checkpoint authority lacks the exact component set"
        )
    rows = connector.fetch_all(
        f"SELECT r.request_sha256, r.component, r.level, "
        "r.start_processed_byte_count, "
        "r.next_processed_byte_count, q.component, q.level, q.start_cursor, "
        "q.terminal, d.subtree_item_count, p.page_bytes, "
        f"pr.prior_request_sha256 FROM {_RECEIPT} r "
        f"JOIN {_PAGE_REQUEST} q ON q.request_sha256 = r.request_sha256 "
        f"JOIN {_REQUEST_PAGE} rp ON rp.request_sha256 = r.request_sha256 "
        f"JOIN {_PAGE_DESCRIPTOR} d ON d.page_sha256 = rp.page_sha256 "
        f"JOIN {_PAGE} p ON p.page_sha256 = rp.page_sha256 "
        f"LEFT JOIN {_PREDECESSOR} pr ON pr.request_sha256 = r.request_sha256 "
        "WHERE r.staging_id = %s AND r.level = 0",
        (handle.staging_id,),
    )
    if len(rows) != len(GalleryObservationComponent):
        raise GalleryStagingConflictError(
            "terminal component receipts are absent or duplicated"
        )
    seen: set[GalleryObservationComponent] = set()
    for row in rows:
        if len(row) != 12:
            raise GalleryStagingConflictError(
                "terminal component receipt has an invalid shape"
            )
        request_sha256 = require_digest32(
            row[0], field="terminal receipt request_sha256"
        )
        component = _COMPONENT_FROM_BYTES.get(row[1])
        if component is None or component in seen:
            raise GalleryStagingConflictError(
                "terminal component receipt is unknown or duplicated"
            )
        seen.add(component)
        level = require_int63(row[2], field="terminal receipt level")
        start_bytes = require_int63(
            row[3], field="terminal receipt start_processed_byte_count"
        )
        next_bytes = require_int63(
            row[4], field="terminal receipt next_processed_byte_count"
        )
        page_component = _COMPONENT_FROM_BYTES.get(row[5])
        page_level = require_int63(row[6], field="terminal page level")
        start_cursor = require_int63(row[7], field="terminal page start_cursor")
        terminal = _decode_bool(row[8], field_name="terminal page intent")
        page_count = require_int63(row[9], field="terminal page item_count")
        page_bytes = require_bounded_bytes(
            row[10], field="terminal page bytes", maximum=65_536
        )
        durable_predecessor = (
            None
            if row[11] is None
            else require_digest32(row[11], field="terminal request predecessor")
        )
        request_coordinate = _decode_terminal_page_request(
            _load_request_bytes(connector, request_sha256),
            handle,
        )
        try:
            terminal_page = decode_gallery_observation_page(page_bytes)
        except ByteDomainError as error:
            raise GalleryStagingConflictError(
                "terminal page bytes are invalid"
            ) from error
        checkpoint = checkpoints[component]
        expected_cursor = _checked_int63_add(
            start_cursor,
            page_count,
            field_name=f"terminal {component.name} cursor",
        )
        if (
            level != 0
            or page_level != 0
            or page_component is not component
            or not terminal
            or request_coordinate
            != (
                component,
                level,
                start_cursor,
                start_bytes,
                terminal,
                durable_predecessor,
                page_bytes,
            )
            or terminal_page.component is not component
            or terminal_page.level != level
            or terminal_page.subtree_item_count != page_count
            or checkpoint.state != "COMPLETE"
            or expected_cursor != checkpoint.cursor
        ):
            raise GalleryStagingConflictError(
                f"terminal {component.name} checkpoint/receipt differs"
            )
        if component is GalleryObservationComponent.FILE:
            if (
                next_bytes < start_bytes
                or next_bytes != checkpoint.processed_byte_count
            ):
                raise GalleryStagingConflictError(
                    "terminal FILE byte checkpoint/receipt differs"
                )
        elif (
            start_bytes != 0 or next_bytes != 0 or checkpoint.processed_byte_count != 0
        ):
            raise GalleryStagingConflictError(
                f"terminal non-FILE {component.name} byte authority is nonzero"
            )
    if seen != set(GalleryObservationComponent):
        raise GalleryStagingConflictError(
            "terminal component receipts lack the exact component set"
        )
    file_checkpoint = checkpoints[GalleryObservationComponent.FILE]
    return file_checkpoint.cursor, file_checkpoint.processed_byte_count


def _bounded_component_roots(
    connector: Any,
    handle: GalleryStagingHandle,
) -> dict[GalleryObservationComponent, tuple[bytes, int]]:
    rows = connector.fetch_all(
        f"SELECT r.root_page_sha256, d.component, d.subtree_item_count "
        f"FROM {_TREE_ROOT} r JOIN {_PAGE_DESCRIPTOR} d "
        "ON d.page_sha256 = r.root_page_sha256 "
        "WHERE r.gallery_id = %s AND r.observation_id = %s",
        (handle.gallery_id, handle.observation_id),
    )
    if len(rows) != 4:
        raise GalleryStagingNotReadyError("exactly four component roots are required")
    output: dict[GalleryObservationComponent, tuple[bytes, int]] = {}
    for root, component_bytes, count in rows:
        component = _COMPONENT_FROM_BYTES.get(component_bytes)
        if component is None or component in output:
            raise GalleryStagingConflictError(
                "component roots are duplicated or unknown"
            )
        output[component] = (
            require_digest32(root, field="component root"),
            require_int63(count, field="component root count"),
        )
    return output


def _component_root(
    connector: Any,
    handle: GalleryStagingHandle,
    component: GalleryObservationComponent,
) -> tuple[bytes, int]:
    rows = connector.fetch_all(
        f"SELECT r.root_page_sha256, d.subtree_item_count FROM {_TREE_ROOT} r "
        f"JOIN {_PAGE_DESCRIPTOR} d ON d.page_sha256 = r.root_page_sha256 "
        "WHERE r.gallery_id = %s AND r.observation_id = %s AND d.component = %s",
        (handle.gallery_id, handle.observation_id, _COMPONENT_BYTES[component]),
    )
    if len(rows) != 1:
        raise GalleryStagingNotReadyError(
            f"{component.name} has no unique completed root"
        )
    return (
        require_digest32(rows[0][0], field=f"{component.name} root"),
        require_int63(rows[0][1], field=f"{component.name} root count"),
    )


def _require_single_frontier_root(
    connector: Any,
    staging_id: bytes,
    component: GalleryObservationComponent,
    root_page_sha256: bytes,
) -> None:
    rows = connector.fetch_all(
        f"SELECT rp.page_sha256 FROM {_FRONTIER} f "
        f"JOIN {_PAGE_REQUEST} q ON q.request_sha256 = f.request_sha256 "
        f"JOIN {_REQUEST_PAGE} rp ON rp.request_sha256 = f.request_sha256 "
        "WHERE q.staging_id = %s AND q.component = %s",
        (staging_id, _COMPONENT_BYTES[component]),
    )
    if rows != [(root_page_sha256,)]:
        raise GalleryStagingConflictError(
            f"{component.name} frontier is not its sole durable root"
        )


def _persist_metadata_facts(
    connector: Any,
    handle: GalleryStagingHandle,
    state: GalleryObservationMetadataDecoderState,
    root_page_sha256: bytes,
) -> None:
    receipt = GalleryObservationMetadataDecoder(state).finish()
    _insert_or_require(
        connector,
        label="gallery metadata",
        select_sql=(
            "SELECT gid, upload_time, download_time, modified_time "
            "FROM catalog_gallery_observation_metadata "
            "WHERE gallery_id = %s AND observation_id = %s"
        ),
        select_data=(handle.gallery_id, handle.observation_id),
        insert_sql=(
            "INSERT INTO catalog_gallery_observation_metadata "
            "(gallery_id, observation_id, gid, upload_time, download_time, "
            "modified_time) VALUES (%s, %s, %s, %s, %s, %s)"
        ),
        insert_data=(
            handle.gallery_id,
            handle.observation_id,
            receipt.gid,
            receipt.upload_time,
            receipt.download_time,
            receipt.modified_time,
        ),
        expected=(
            receipt.gid,
            receipt.upload_time,
            receipt.download_time,
            receipt.modified_time,
        ),
    )
    metadata_root_count = _component_root(
        connector,
        handle,
        GalleryObservationComponent.METADATA,
    )[1]
    metadata_digest = gallery_metadata_audit_digest(
        root_page_sha256,
        metadata_root_count,
    )
    _insert_or_require(
        connector,
        label="gallery metadata digest",
        select_sql=(
            "SELECT metadata_sha256 FROM catalog_gallery_observation_metadata_digests "
            "WHERE gallery_id = %s AND observation_id = %s"
        ),
        select_data=(handle.gallery_id, handle.observation_id),
        insert_sql=(
            "INSERT INTO catalog_gallery_observation_metadata_digests "
            "(gallery_id, observation_id, metadata_sha256) VALUES (%s, %s, %s)"
        ),
        insert_data=(handle.gallery_id, handle.observation_id, metadata_digest),
        expected=(metadata_digest,),
    )
    if receipt.page_count is not None:
        _insert_or_require(
            connector,
            label="gallery page count",
            select_sql=(
                "SELECT page_count FROM catalog_gallery_observation_page_counts "
                "WHERE gallery_id = %s AND observation_id = %s"
            ),
            select_data=(handle.gallery_id, handle.observation_id),
            insert_sql=(
                "INSERT INTO catalog_gallery_observation_page_counts "
                "(gallery_id, observation_id, page_count) VALUES (%s, %s, %s)"
            ),
            insert_data=(handle.gallery_id, handle.observation_id, receipt.page_count),
            expected=(receipt.page_count,),
        )


def _persist_scan_fact(
    connector: Any,
    handle: GalleryStagingHandle,
    roots: dict[GalleryObservationComponent, tuple[bytes, int]],
    *,
    scan_observation_version: int,
    source_file_count: int,
) -> None:
    scan_digest = gallery_scan_audit_digest(roots)
    _insert_or_require(
        connector,
        label="gallery scan",
        select_sql=(
            "SELECT scan_observation_sha256, scan_observation_version, "
            "source_file_count FROM catalog_gallery_observation_scans "
            "WHERE gallery_id = %s AND observation_id = %s"
        ),
        select_data=(handle.gallery_id, handle.observation_id),
        insert_sql=(
            "INSERT INTO catalog_gallery_observation_scans "
            "(gallery_id, observation_id, scan_observation_sha256, "
            "scan_observation_version, source_file_count) "
            "VALUES (%s, %s, %s, %s, %s)"
        ),
        insert_data=(
            handle.gallery_id,
            handle.observation_id,
            scan_digest,
            scan_observation_version,
            source_file_count,
        ),
        expected=(scan_digest, scan_observation_version, source_file_count),
    )


def _insert_parser(connector: Any, staging_id: bytes, now: int) -> None:
    state = GalleryObservationMetadataDecoder().state
    values = _parser_values(state)
    connector.execute(
        f"INSERT INTO {_PARSER} "
        "(staging_id, phase, fixed_carry, remaining_text_bytes, utf8_tail, gid, "
        "title_byte_count, comment_byte_count, upload_account_byte_count, "
        "upload_time, download_time, modified_time, scan_observation_version, "
        "source_file_count, page_count, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (staging_id, *values, now),
    )


def _update_parser(
    work: VNextUnitOfWork,
    staging_id: bytes,
    state: GalleryObservationMetadataDecoderState,
    *,
    now: int,
) -> None:
    values = _parser_values(state)
    work.compare_and_swap(
        f"UPDATE {_PARSER} SET phase = %s, fixed_carry = %s, "
        "remaining_text_bytes = %s, utf8_tail = %s, gid = %s, "
        "title_byte_count = %s, comment_byte_count = %s, "
        "upload_account_byte_count = %s, upload_time = %s, download_time = %s, "
        "modified_time = %s, scan_observation_version = %s, "
        "source_file_count = %s, page_count = %s, updated_at = %s "
        "WHERE staging_id = %s",
        (*values, now, staging_id),
        authority="gallery metadata parser",
    )


def _parser_values(state: GalleryObservationMetadataDecoderState) -> tuple[Any, ...]:
    return (
        _durable_parser_phase(state.phase),
        state.fixed_carry,
        state.remaining_text_bytes,
        state.utf8_tail,
        state.gid,
        state.text_lengths[0],
        state.text_lengths[1],
        state.text_lengths[2],
        state.upload_time,
        state.download_time,
        state.modified_time,
        state.scan_observation_version,
        state.source_file_count,
        state.page_count,
    )


def _decode_parser(
    row: tuple[Any, ...],
) -> tuple[GalleryObservationMetadataDecoderState, int]:
    if len(row) != 15:
        raise GalleryStagingNotReadyError("metadata parser row is missing")
    state = GalleryObservationMetadataDecoderState(
        _runtime_parser_phase(row[0]),
        require_bounded_bytes(row[1], field="parser fixed_carry", maximum=40),
        require_int63(row[2], field="parser remaining_text_bytes"),
        require_bounded_bytes(row[3], field="parser utf8_tail", maximum=3),
        None if row[4] is None else require_positive_int63(row[4], field="parser gid"),
        (
            require_int63(row[5], field="parser title_byte_count"),
            require_int63(row[6], field="parser comment_byte_count"),
            require_int63(row[7], field="parser upload_account_byte_count"),
        ),
        None if row[8] is None else require_int63(row[8], field="parser upload_time"),
        None if row[9] is None else require_int63(row[9], field="parser download_time"),
        (
            None
            if row[10] is None
            else require_int63(row[10], field="parser modified_time")
        ),
        (
            None
            if row[11] is None
            else require_positive_int63(row[11], field="parser scan version")
        ),
        (
            None
            if row[12] is None
            else require_int63(row[12], field="parser source_file_count")
        ),
        None if row[13] is None else require_int63(row[13], field="parser page_count"),
    )
    # Construction validates all phase/carry/scalar coherence.
    GalleryObservationMetadataDecoder(state)
    return state, require_int63(row[14], field="parser updated_at")


def _durable_parser_phase(runtime: str) -> str:
    runtime_aliases = {
        "TITLE_TEXT": "TITLE",
        "COMMENT_TEXT": "COMMENT",
        "ACCOUNT_TAG": "UPLOAD_ACCOUNT_TAG",
        "ACCOUNT_LENGTH": "UPLOAD_ACCOUNT_LENGTH",
        "ACCOUNT_TEXT": "UPLOAD_ACCOUNT",
    }
    if not isinstance(runtime, str):
        raise GalleryStagingConflictError("unknown runtime metadata parser phase")
    durable = runtime_aliases.get(runtime, runtime)
    if runtime not in runtime_aliases and runtime not in (
        GALLERY_OBSERVATION_DURABLE_PARSER_PHASES
    ):
        raise GalleryStagingConflictError("unknown runtime metadata parser phase")
    try:
        return validate_gallery_observation_durable_parser_phase(durable).decode(
            "ascii"
        )
    except ByteDomainError as error:
        raise GalleryStagingConflictError(
            "unknown runtime metadata parser phase"
        ) from error


def _runtime_parser_phase(durable: object) -> str:
    if not isinstance(durable, str):
        raise GalleryStagingConflictError("metadata parser phase is not text")
    try:
        exact = validate_gallery_observation_durable_parser_phase(durable).decode(
            "ascii"
        )
    except ByteDomainError as error:
        raise GalleryStagingConflictError(
            "unknown durable metadata parser phase"
        ) from error
    return {
        "TITLE": "TITLE_TEXT",
        "COMMENT": "COMMENT_TEXT",
        "UPLOAD_ACCOUNT_TAG": "ACCOUNT_TAG",
        "UPLOAD_ACCOUNT_LENGTH": "ACCOUNT_LENGTH",
        "UPLOAD_ACCOUNT": "ACCOUNT_TEXT",
    }.get(exact, exact)


def _validate_seal_replay(
    connector: Any,
    handle: GalleryStagingHandle,
    state: str,
) -> GalleryStagingSeal:
    checkpoints = {
        component: _read_level_zero_checkpoint(
            connector,
            handle.staging_id,
            component,
        )
        for component in GalleryObservationComponent
    }
    if any(checkpoint.state != "COMPLETE" for checkpoint in checkpoints.values()):
        raise GalleryStagingConflictError(
            "terminal staging has an incomplete component checkpoint"
        )
    file_count, byte_count = _terminal_checkpoint_authority(
        connector,
        handle,
        checkpoints,
    )
    link = connector.fetch_one(
        "SELECT g.observation_id, o.observation_identity_sha256, "
        "s.file_count, s.byte_count "
        "FROM catalog_source_build_galleries g "
        "JOIN catalog_gallery_observations o ON o.gallery_id = g.gallery_id "
        "AND o.observation_id = g.observation_id "
        "JOIN catalog_gallery_observation_stat s ON s.gallery_id = g.gallery_id "
        "AND s.observation_id = g.observation_id "
        "WHERE g.build_id = %s AND g.gallery_id = %s",
        (handle.build_id, handle.gallery_id),
    )
    if len(link) != 4:
        raise GalleryStagingConflictError("terminal staging has no exact build link")
    observation = require_positive_int63(link[0], field="sealed observation_id")
    if link[2:] != (file_count, byte_count):
        raise GalleryStagingConflictError("terminal observation stat differs")
    if (state == "SEALED") != (observation == handle.observation_id):
        raise GalleryStagingConflictError("terminal staging state/link disagree")
    return GalleryStagingSeal(
        handle.build_id,
        handle.gallery_id,
        observation,
        require_digest32(link[1], field="sealed observation identity"),
        state,
        True,
    )


def _read_level_zero_checkpoint(
    connector: Any,
    staging_id: bytes,
    component: GalleryObservationComponent,
) -> _Checkpoint:
    return _decode_checkpoint(
        connector.fetch_one(
            f"SELECT cursor, regular_count, processed_byte_count, state, updated_at "
            f"FROM {_CHECKPOINT} "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (staging_id, _COMPONENT_BYTES[component]),
        )
    )


def _decode_header(row: tuple[Any, ...]) -> _Header:
    if len(row) != 6:
        raise GalleryStagingNotReadyError("staging header is missing")
    _require_header_state(row[3], row[5])
    return _Header(
        require_uuid16(row[0], field="staging build_id"),
        require_positive_int63(row[1], field="staging gallery_id"),
        require_positive_int63(row[2], field="staging observation_id"),
        row[3],
        require_int63(row[4], field="staging created_at"),
        None if row[5] is None else require_int63(row[5], field="staging sealed_at"),
    )


def _decode_checkpoint(row: tuple[Any, ...]) -> _Checkpoint:
    if len(row) != 5 or row[3] not in {"OPEN", "COMPLETE"}:
        raise GalleryStagingNotReadyError("staging checkpoint is missing or invalid")
    return _Checkpoint(
        require_int63(row[0], field="checkpoint cursor"),
        require_int63(row[1], field="checkpoint regular_count"),
        require_int63(row[2], field="checkpoint processed_byte_count"),
        row[3],
        require_int63(row[4], field="checkpoint updated_at"),
    )


@dataclass(frozen=True, slots=True)
class _MatchCheckpoint:
    file_cursor_bytes: bytes
    matched_count: int
    state: str
    updated_at: int


def _decode_match_checkpoint(row: tuple[Any, ...]) -> _MatchCheckpoint:
    if len(row) != 4 or row[2] not in {"OPEN", "COMPLETE"}:
        raise GalleryStagingNotReadyError("match checkpoint is missing or invalid")
    return _MatchCheckpoint(
        require_bounded_bytes(row[0], field="file cursor", maximum=2048),
        require_int63(row[1], field="matched_count"),
        row[2],
        require_int63(row[3], field="match updated_at"),
    )


def _require_handle_rows(
    handle: GalleryStagingHandle,
    header: _Header,
    claim: _Claim,
    *,
    allow_stale_generation: bool = False,
) -> None:
    if (
        header.build_id,
        header.gallery_id,
        header.observation_id,
    ) != (handle.build_id, handle.gallery_id, handle.observation_id):
        raise GalleryStagingConflictError("staging handle/header tuple differs")
    if claim.claim_generation != handle.claim_generation:
        raise GalleryStagingNotReadyError("staging claim generation is stale")
    if (
        not allow_stale_generation
        and claim.ingest_generation != handle.ingest_generation
    ):
        raise GalleryStagingNotReadyError("staging ingest generation is stale")


def _require_header_state(state: object, sealed_at: object) -> None:
    if state not in {"OPEN", "ABANDONED", "SEALED", "REUSED"}:
        raise GalleryStagingConflictError("staging state is unknown")
    if (state in {"SEALED", "REUSED"}) != (sealed_at is not None):
        raise GalleryStagingConflictError("staging state/sealed_at disagree")


def _insert_or_require(
    connector: Any,
    *,
    label: str,
    select_sql: str,
    select_data: tuple[Any, ...],
    insert_sql: str,
    insert_data: tuple[Any, ...],
    expected: tuple[Any, ...],
) -> None:
    row = connector.fetch_one(select_sql, select_data)
    if row:
        if row != expected:
            raise GalleryStagingConflictError(f"{label} differs")
    else:
        connector.execute(insert_sql, insert_data)


def _encode_optional_digest(value: bytes | None) -> bytes:
    if value is None:
        return b"\x00"
    return b"\x01" + require_digest32(value, field="predecessor request_sha256")


def _encode_attempt(attempt: BatchAttempt) -> bytes:
    current = _require_attempt(attempt)
    previous = current.previous_operation_id
    return b"".join(
        (
            current.operation_id,
            b"\x00" if previous is None else b"\x01" + previous,
        )
    )


def _require_new_attempt(
    connector: Any,
    latest_request_sha256: object | None,
    attempt: BatchAttempt,
    *,
    subtype: bytes,
) -> None:
    current = _require_attempt(attempt)
    if latest_request_sha256 is None:
        if current.previous_operation_id is not None:
            raise GalleryStagingConflictError(
                "first single-flight batch cannot acknowledge a prior operation"
            )
        return
    latest_request = require_digest32(
        latest_request_sha256,
        field="latest request_sha256",
    )
    latest_subtype, latest_operation = _request_operation_id(
        _load_request_bytes(connector, latest_request)
    )
    if latest_subtype != subtype:
        raise GalleryStagingConflictError("latest request has the wrong subtype")
    if current.previous_operation_id != latest_operation:
        raise GalleryStagingConflictError(
            "new single-flight batch does not acknowledge the latest operation"
        )


def _decode_terminal_page_request(
    frame: bytes,
    handle: GalleryStagingHandle,
) -> tuple[
    GalleryObservationComponent,
    int,
    int,
    int,
    bool,
    bytes | None,
    bytes,
]:
    current = _require_handle(handle)
    prefix_end = len(_REQUEST_PREFIX)
    fixed_size = prefix_end + 5 + 16 + 16 + 8 * 4
    if len(frame) < fixed_size + 19 or not frame.startswith(_REQUEST_PREFIX):
        raise GalleryStagingConflictError("terminal request frame is truncated")
    if int.from_bytes(frame[prefix_end : prefix_end + 4], "big") != _REQUEST_VERSION:
        raise GalleryStagingConflictError("terminal request version is unknown")
    if frame[prefix_end + 4 : prefix_end + 5] != b"P":
        raise GalleryStagingConflictError("terminal receipt does not own a leaf page")
    offset = prefix_end + 5
    staging_id = frame[offset : offset + 16]
    offset += 16
    build_id = frame[offset : offset + 16]
    offset += 16
    gallery_id = require_int63(
        int.from_bytes(frame[offset : offset + 8], "big"),
        field="terminal request gallery_id",
    )
    offset += 8
    observation_id = require_int63(
        int.from_bytes(frame[offset : offset + 8], "big"),
        field="terminal request observation_id",
    )
    offset += 8
    require_int63(
        int.from_bytes(frame[offset : offset + 8], "big"),
        field="terminal request ingest_generation",
    )
    offset += 8
    require_int63(
        int.from_bytes(frame[offset : offset + 8], "big"),
        field="terminal request claim_generation",
    )
    offset += 8
    if (staging_id, build_id, gallery_id, observation_id) != (
        current.staging_id,
        current.build_id,
        current.gallery_id,
        current.observation_id,
    ):
        raise GalleryStagingConflictError("terminal request owner tuple differs")
    try:
        component = GalleryObservationComponent(frame[offset])
    except ValueError as error:
        raise GalleryStagingConflictError(
            "terminal request component is unknown"
        ) from error
    level = frame[offset + 1]
    start_cursor = require_int63(
        int.from_bytes(frame[offset + 2 : offset + 10], "big"),
        field="terminal request start_cursor",
    )
    start_bytes = require_int63(
        int.from_bytes(frame[offset + 10 : offset + 18], "big"),
        field="terminal request start_processed_byte_count",
    )
    terminal = _decode_bool(
        frame[offset + 18],
        field_name="terminal request intent",
    )
    offset += 19
    predecessor, offset = _decode_optional_digest_at(frame, offset)
    _operation, _previous_operation, offset = _decode_attempt_at(frame, offset)
    semantic_size, offset = _take_frame_uint32(frame, offset)
    semantic_end = offset + semantic_size
    if semantic_end > len(frame):
        raise GalleryStagingConflictError("terminal request semantic body is truncated")
    page_size, offset = _take_frame_uint32(frame, semantic_end)
    page_end = offset + page_size
    if page_size > 65_536 or page_end != len(frame):
        raise GalleryStagingConflictError("terminal request page body has invalid EOF")
    return (
        component,
        level,
        start_cursor,
        start_bytes,
        terminal,
        predecessor,
        frame[offset:page_end],
    )


def _request_operation_id(frame: bytes) -> tuple[bytes, bytes]:
    prefix_end = len(_REQUEST_PREFIX)
    if not frame.startswith(_REQUEST_PREFIX) or len(frame) < prefix_end + 4 + 1:
        raise GalleryStagingConflictError("request frame prefix is invalid")
    if int.from_bytes(frame[prefix_end : prefix_end + 4], "big") != _REQUEST_VERSION:
        raise GalleryStagingConflictError("request frame version is unknown")
    subtype = frame[prefix_end + 4 : prefix_end + 5]
    offset = prefix_end + 5 + 16 + 16 + 8 * 4
    if subtype == b"P":
        # component, level, start cursor, start processed bytes, terminal
        offset += 1 + 1 + 8 + 8 + 1
        offset = _skip_optional_digest(frame, offset)
        operation, _previous, _end = _decode_attempt_at(frame, offset)
        return subtype, operation
    if subtype == b"M":
        cursor_size, offset = _take_frame_uint32(frame, offset)
        offset += cursor_size + 8 + 1
        offset = _skip_optional_digest(frame, offset)
        body_size, offset = _take_frame_uint32(frame, offset)
        if offset + body_size != len(frame):
            raise GalleryStagingConflictError("match request body has invalid EOF")
        operation, _previous, _end = _decode_attempt_at(frame, offset)
        return subtype, operation
    raise GalleryStagingConflictError("request subtype has no public operation token")


def _skip_optional_digest(frame: bytes, offset: int) -> int:
    if offset >= len(frame):
        raise GalleryStagingConflictError("request predecessor flag is truncated")
    presence = frame[offset]
    if presence == 0:
        return offset + 1
    if presence == 1 and offset + 33 <= len(frame):
        return offset + 33
    raise GalleryStagingConflictError("request predecessor frame is invalid")


def _decode_optional_digest_at(
    frame: bytes,
    offset: int,
) -> tuple[bytes | None, int]:
    next_offset = _skip_optional_digest(frame, offset)
    if frame[offset] == 0:
        return None, next_offset
    return (
        require_digest32(
            frame[offset + 1 : next_offset],
            field="request predecessor digest",
        ),
        next_offset,
    )


def _decode_attempt_at(
    frame: bytes,
    offset: int,
) -> tuple[bytes, bytes | None, int]:
    if offset + 17 > len(frame):
        raise GalleryStagingConflictError("request attempt is truncated")
    operation = require_uuid16(
        frame[offset : offset + 16],
        field="request operation_id",
    )
    presence = frame[offset + 16]
    if presence == 0:
        return operation, None, offset + 17
    if presence == 1 and offset + 33 <= len(frame):
        previous = require_uuid16(
            frame[offset + 17 : offset + 33],
            field="request previous_operation_id",
        )
        return operation, previous, offset + 33
    raise GalleryStagingConflictError("request previous-operation frame is invalid")


def _take_frame_uint32(frame: bytes, offset: int) -> tuple[int, int]:
    if offset + 4 > len(frame):
        raise GalleryStagingConflictError("request frame is truncated")
    return int.from_bytes(frame[offset : offset + 4], "big"), offset + 4


def _decode_file_cursor(value: bytes) -> int:
    if value == b"":
        return 0
    if len(value) != 8:
        raise GalleryStagingConflictError("FILE match cursor is not exact u64be")
    return require_int63(int.from_bytes(value, "big"), field="FILE match cursor")


def _decode_bool(value: object, *, field_name: str) -> bool:
    if value not in (0, 1) or isinstance(value, bool):
        raise GalleryStagingConflictError(f"{field_name} is not an exact bool byte")
    return value == 1


def _require_exact_tuple(
    values: object,
    expected_type: type[Any],
    *,
    field_name: str,
) -> None:
    if type(values) is not tuple or any(
        type(value) is not expected_type for value in values
    ):
        raise TypeError(
            f"{field_name} must be an exact tuple of {expected_type.__name__}"
        )


def _require_exact_bool(value: object, *, field_name: str) -> None:
    if type(value) is not bool:
        raise TypeError(f"{field_name} must be bool")


def _checked_int63_add(left: int, right: int, *, field_name: str) -> int:
    first = require_int63(left, field=f"{field_name} left")
    second = require_int63(right, field=f"{field_name} right")
    result = first + second
    if result > INT63_MAX:
        raise OverflowError(f"{field_name} exceeds signed int63")
    return result


def _require_uint64(value: object, *, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value < 1 << 64
    ):
        raise ValueError(f"{field} must be an unsigned 64-bit integer")
    return value


def _require_int64(value: object, *, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not -(1 << 63) <= value < 1 << 63
    ):
        raise ValueError(f"{field} must be a signed 64-bit integer")
    return value


def _require_handle(handle: GalleryStagingHandle) -> GalleryStagingHandle:
    if type(handle) is not GalleryStagingHandle:
        raise TypeError("handle must be an exact GalleryStagingHandle")
    handle.__post_init__()
    return handle


def _require_attempt(attempt: BatchAttempt) -> BatchAttempt:
    if type(attempt) is not BatchAttempt:
        raise TypeError("attempt must be an exact BatchAttempt")
    attempt.__post_init__()
    return attempt


def _new_staging_id() -> bytes:
    return secrets.token_bytes(16)
