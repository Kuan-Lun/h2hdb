"""Pure runtime codecs for the greenfield vNext identity contract.

The functions in this module deliberately stop at the binary persistence
boundary: digests, filesystem names, and generated identifiers are returned as
``bytes``.  Hexadecimal text is available only through the explicit digest
helpers.  No function here reads configuration, touches a database, or mutates
the currently deployed schema.
"""

from __future__ import annotations

import codecs

__all__ = [
    "ARTIFACT_COMPONENT_KINDS",
    "ARTIFACT_MEMBER_PLAN_VERSION",
    "ARTIFACT_COMPONENT_CODEC_VERSION",
    "ARTIFACT_POLICY_CODEC_VERSION",
    "ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION",
    "ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION",
    "ARTIFACT_SEMANTICS_CODEC_VERSION",
    "ARTIFACT_LOCATOR_CODEC_VERSION",
    "ARTIFACT_LOCATOR_MAXIMUM_BYTES",
    "ANALYSIS_STATE_COMPONENTS",
    "ANALYSIS_ALREADY_UPLOADED_MARKER",
    "EFFECTIVE_CONTENT_ENCODING_VERSION",
    "GALLERY_KEY_ALGORITHM_VERSION",
    "PUBLICATION_KEY_ALGORITHM_VERSION",
    "FILE_IDENTITY_ALGORITHM_VERSION",
    "FILE_ROLE_CLASSIFIER_VERSION",
    "SOURCE_LOCATOR_CODEC_VERSION",
    "SOURCE_ROOT_CODEC_VERSION",
    "SOURCE_ROOT_DIGEST_DOMAIN",
    "GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION",
    "GALLERY_OBSERVATION_METADATA_CODEC_VERSION",
    "GALLERY_OBSERVATION_PAGE_CODEC_VERSION",
    "GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES",
    "GALLERY_OBSERVATION_BRANCH_CAPACITY",
    "GALLERY_OBSERVATION_FILE_LEAF_CAPACITY",
    "GALLERY_OBSERVATION_TAG_LEAF_CAPACITY",
    "GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY",
    "GALLERY_OBSERVATION_METADATA_CHUNK_BYTES",
    "GALLERY_OBSERVATION_DURABLE_PARSER_PHASES",
    "SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION",
    "SOURCE_SCOPE_KEY_ALGORITHM_VERSION",
    "ZIP_COMMENT_CODEC_VERSION",
    "SOURCE_PROVIDERS",
    "CANONICAL_DIGEST_CODEC_VERSION",
    "CANONICAL_VALUE_PAGE_CODEC_VERSION",
    "CANONICAL_VALUE_PAGE_MAXIMUM_BYTES",
    "CANONICAL_VALUE_CHUNK_BYTES",
    "CANONICAL_VALUE_BRANCH_CAPACITY",
    "FILESYSTEM_STAT_FINGERPRINT_BYTES",
    "CATALOG_SUMMARY_DIGEST_DOMAIN",
    "CATALOG_LANGUAGE_DIGEST_DOMAIN",
    "ARTIFACT_LOCATOR_DIGEST_DOMAIN",
    "ByteDomainError",
    "CanonicalIdentityCollisionError",
    "DigestFormatError",
    "DigestMismatchError",
    "IntegerDomainError",
    "RegisteredIdentifierError",
    "VNextIdentityError",
    "ArtifactMemberEntryKind",
    "ArtifactMemberPlanEntry",
    "ArtifactProtectionToken",
    "ArtifactSourceRole",
    "ArtifactTransformKind",
    "AnalysisTitleScalarReceipt",
    "StrictUtf8ScalarCounter",
    "GalleryObservationComponent",
    "GalleryObservationNodeKind",
    "GalleryObservationDirectoryFileType",
    "GalleryObservationFileEntry",
    "GalleryObservationTagEntry",
    "GalleryObservationDirectoryEntry",
    "GalleryObservationMetadataChunk",
    "GalleryObservationBranchEntry",
    "GalleryObservationPage",
    "GalleryObservationEncodedPage",
    "GalleryObservationTree",
    "GalleryObservationMetadata",
    "GalleryObservationDescriptor",
    "CanonicalValueChunk",
    "CanonicalValueBranchEntry",
    "CanonicalValuePage",
    "CanonicalValueEncodedPage",
    "CanonicalValueTree",
    "GalleryObservationMetadataDecoderState",
    "GalleryObservationMetadataScalarReceipt",
    "GalleryObservationMetadataDecoder",
    "SourceSnapshotContentOwner",
    "SourceSnapshotCounts",
    "SourceSnapshotFileHashDecision",
    "SourceSnapshotGallery",
    "SourceSnapshotGidWinner",
    "SourceSnapshotPolicy",
    "SourceRelativeLocatorValidationReceipt",
    "SourceRootValidationReceipt",
    "artifact_id",
    "artifact_member_plan_digest",
    "artifact_member_plan_digest_ordered",
    "artifact_effective_content_digest",
    "artifact_effective_content_digest_ordered",
    "artifact_owner_digest",
    "artifact_policy_digest",
    "artifact_producer_equivalence_class",
    "artifact_producer_fingerprint_sha256",
    "artifact_storage_receipt_id",
    "artifact_name",
    "artifact_archive_member_name",
    "artifact_selected_digest",
    "artifact_semantics_digest",
    "artifact_source_manifest_digest",
    "artifact_locator_digest",
    "artifact_locator_components",
    "analysis_candidate_has_already_uploaded",
    "count_analysis_title_scalars",
    "catalog_summary_digest",
    "catalog_summary_digest_parts",
    "catalog_language_digest",
    "catalog_language_digest_parts",
    "canonical_value_digest",
    "canonical_value_digest_parts",
    "canonical_value_digest_hex",
    "encode_canonical_value_page",
    "decode_canonical_value_page",
    "canonical_value_page_digest",
    "verify_canonical_value_page_conflict",
    "build_canonical_value_tree",
    "validate_canonical_value_tree",
    "decode_source_relative_locator",
    "validate_source_relative_locator_parts",
    "decode_artifact_id",
    "decode_artifact_name",
    "decode_artifact_locator",
    "decode_artifact_protection_token",
    "iter_decode_artifact_locator",
    "decode_source_root",
    "validate_source_root_parts",
    "decode_gallery_observation_page",
    "decode_gallery_observation_metadata",
    "validate_gallery_observation_metadata_parts",
    "decode_gallery_observation_descriptor",
    "decode_artifact_member_plan",
    "digest_from_hex",
    "digest_to_hex",
    "encode_source_relative_locator",
    "encode_artifact_locator",
    "encode_artifact_protection_token",
    "encode_artifact_producer_fingerprint",
    "iter_artifact_locator_payload",
    "iter_source_relative_locator_payload",
    "encode_source_root",
    "iter_source_root_payload",
    "source_root_digest",
    "encode_gallery_observation_page",
    "gallery_observation_page_digest",
    "gallery_observation_page_key_bounds",
    "verify_gallery_observation_page_conflict",
    "build_gallery_observation_tree",
    "build_gallery_observation_metadata_tree",
    "validate_gallery_observation_tree",
    "iter_gallery_observation_metadata_stream",
    "encode_gallery_observation_metadata",
    "encode_gallery_observation_descriptor",
    "gallery_observation_descriptor_digest",
    "gallery_directory_audit_digest",
    "gallery_metadata_audit_digest",
    "gallery_scan_audit_digest",
    "encode_artifact_member_plan",
    "encode_artifact_effective_content",
    "iter_artifact_effective_content_payload_ordered",
    "iter_artifact_member_plan_payload",
    "encode_artifact_owner",
    "encode_artifact_policy",
    "encode_artifact_selected",
    "encode_artifact_semantics",
    "encode_artifact_source_manifest",
    "encode_zip_comment",
    "encode_effective_content",
    "iter_effective_content_payload_ordered",
    "encode_source_snapshot_manifest",
    "iter_source_snapshot_manifest_payload_ordered",
    "iter_source_snapshot_manifest_payload_rows_ordered",
    "effective_content_digest",
    "effective_content_digest_ordered",
    "encode_filesystem_stat_fingerprint",
    "decode_filesystem_stat_fingerprint",
    "file_key",
    "file_role",
    "gallery_key",
    "gallery_key_hex",
    "publication_id",
    "decode_publication_id",
    "publication_key",
    "publication_key_hex",
    "validate_artifact_component_kind",
    "validate_canonical_value_identity",
    "validate_file_name",
    "validate_gallery_name",
    "validate_gallery_observation_durable_parser_phase",
    "validate_namespace",
    "validate_registered_ascii_identifier",
    "source_relative_locator_digest",
    "source_snapshot_manifest_digest",
    "source_snapshot_manifest_digest_ordered",
    "source_scope_key",
    "validate_state_component",
    "verify_canonical_value_conflict",
]

from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from hashlib import sha256

_SHA256_BYTES = 32
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT63_MAX = (1 << 63) - 1
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

GALLERY_KEY_ALGORITHM_VERSION = 1
PUBLICATION_KEY_ALGORITHM_VERSION = 1
FILE_IDENTITY_ALGORITHM_VERSION = 1
FILE_ROLE_CLASSIFIER_VERSION = 1
SOURCE_LOCATOR_CODEC_VERSION = 1
SOURCE_ROOT_CODEC_VERSION = 1
SOURCE_ROOT_DIGEST_DOMAIN = "source_root_v1"
SOURCE_SCOPE_KEY_ALGORITHM_VERSION = 1
SOURCE_PROVIDERS = frozenset({"filesystem"})

CANONICAL_DIGEST_CODEC_VERSION = 1
CANONICAL_VALUE_PAGE_CODEC_VERSION = 1
CANONICAL_VALUE_PAGE_MAXIMUM_BYTES = 65536
CANONICAL_VALUE_CHUNK_BYTES = 32768
CANONICAL_VALUE_BRANCH_CAPACITY = 256
FILESYSTEM_STAT_FINGERPRINT_BYTES = 40
EFFECTIVE_CONTENT_ENCODING_VERSION = 1
ARTIFACT_MEMBER_PLAN_VERSION = 1
ARTIFACT_COMPONENT_CODEC_VERSION = 1
ARTIFACT_POLICY_CODEC_VERSION = 2
ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION = 1
ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION = 1
ARTIFACT_SEMANTICS_CODEC_VERSION = 1
ARTIFACT_LOCATOR_CODEC_VERSION = 1
ARTIFACT_LOCATOR_MAXIMUM_BYTES = 4096
ZIP_COMMENT_CODEC_VERSION = 1
SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION = 1
GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION = 1
GALLERY_OBSERVATION_METADATA_CODEC_VERSION = 1
GALLERY_OBSERVATION_PAGE_CODEC_VERSION = 1
GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES = 65536
GALLERY_OBSERVATION_BRANCH_CAPACITY = 256
GALLERY_OBSERVATION_FILE_LEAF_CAPACITY = 256
GALLERY_OBSERVATION_TAG_LEAF_CAPACITY = 256
GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY = 192
GALLERY_OBSERVATION_METADATA_CHUNK_BYTES = 32768
GALLERY_OBSERVATION_DURABLE_PARSER_PHASES = (
    "PREFIX",
    "VERSION",
    "GID",
    "TITLE_TAG",
    "TITLE_LENGTH",
    "TITLE",
    "COMMENT_TAG",
    "COMMENT_LENGTH",
    "COMMENT",
    "UPLOAD_ACCOUNT_TAG",
    "UPLOAD_ACCOUNT_LENGTH",
    "UPLOAD_ACCOUNT",
    "UPLOAD_TIME",
    "DOWNLOAD_TIME",
    "MODIFIED_TIME",
    "SCAN_VERSION",
    "SOURCE_FILE_COUNT",
    "PAGE_COUNT_PRESENCE",
    "PAGE_COUNT",
    "DONE",
)
EFFECTIVE_CONTENT_DIGEST_DOMAIN = "effective_content_v1"
ARTIFACT_MEMBER_PLAN_DIGEST_DOMAIN = "artifact_member_plan_v1"
SOURCE_SNAPSHOT_MANIFEST_DIGEST_DOMAIN = "source_snapshot_manifest_v1"
ARTIFACT_SOURCE_MANIFEST_DIGEST_DOMAIN = "artifact_source_manifest_v1"
ARTIFACT_EFFECTIVE_CONTENT_DIGEST_DOMAIN = "artifact_effective_content_v1"
ARTIFACT_SELECTED_DIGEST_DOMAIN = "artifact_selected_v1"
ARTIFACT_OWNER_DIGEST_DOMAIN = "artifact_owner_v1"
ARTIFACT_POLICY_DIGEST_DOMAIN = "artifact_policy_v2"
ARTIFACT_SEMANTICS_DIGEST_DOMAIN = "artifact_semantics_v1"
GALLERY_OBSERVATION_DIGEST_DOMAIN = "gallery_observation_v1"
CATALOG_SUMMARY_DIGEST_DOMAIN = "catalog_summary_utf8_v1"
CATALOG_LANGUAGE_DIGEST_DOMAIN = "catalog_language_utf8_v1"
ARTIFACT_LOCATOR_DIGEST_DOMAIN = "artifact_locator_bytes_v1"
CONTENT_FILE_ROLE = b"CONTENT"
METADATA_FILE_ROLE = b"METADATA"
METADATA_FILE_NAME = b"galleryinfo.txt"
ANALYSIS_ALREADY_UPLOADED_MARKER = b"already uploaded"

_EFFECTIVE_CONTENT_PREFIX = b"h2hdb-vnext-effective-content\0"
_ARTIFACT_MEMBER_PLAN_PREFIX = b"h2hdb-vnext-artifact-member-plan\0"
_SOURCE_SNAPSHOT_MANIFEST_PREFIX = b"h2hdb-vnext-source-snapshot-manifest\0"
_ARTIFACT_SOURCE_MANIFEST_PREFIX = b"h2hdb-vnext-artifact-source-manifest\0"
_ARTIFACT_EFFECTIVE_CONTENT_PREFIX = b"h2hdb-vnext-artifact-effective-content\0"
_ARTIFACT_SELECTED_PREFIX = b"h2hdb-vnext-artifact-selected\0"
_ARTIFACT_OWNER_PREFIX = b"h2hdb-vnext-artifact-owner\0"
_ARTIFACT_POLICY_PREFIX = b"h2hdb-vnext-artifact-policy\0"
_ARTIFACT_PRODUCER_FINGERPRINT_PREFIX = b"h2hdb-vnext-artifact-producer\0"
_ARTIFACT_PRODUCER_EQUIVALENCE_PREFIX = (
    b"h2hdb-vnext-artifact-producer-exact-equivalence-v1\0"
)
_ARTIFACT_STORAGE_RECEIPT_PREFIX = b"h2hdb-vnext-artifact-storage-receipt\0"
_ARTIFACT_PROTECTION_PREFIX = b"h2hdb-vnext-artifact-protection\0"
_ARTIFACT_SEMANTICS_PREFIX = b"h2hdb-vnext-artifact-semantics\0"
_PUBLICATION_ID_PREFIX = b"urn:h2h:gallery:"
_ARTIFACT_ID_PREFIX = b"urn:h2h:artifact:cbz:"
_ARTIFACT_ID_DIGEST_SEPARATOR = b":sha256:"
_ARTIFACT_NAME_PREFIX = b"h2h-"
_ARTIFACT_NAME_SUFFIX = b".cbz"
_ZIP_COMMENT_PREFIX = b"H2HDB-ZIP-COMMENT\0"
_GALLERY_OBSERVATION_PAGE_PREFIX = b"h2hdb-vnext-gallery-observation-page\0"
_CANONICAL_VALUE_PAGE_PREFIX = b"h2hdb-vnext-canonical-value-page\0"
_GALLERY_OBSERVATION_METADATA_PREFIX = b"h2hdb-vnext-gallery-observation-metadata\0"
_GALLERY_DIRECTORY_AUDIT_PREFIX = b"h2hdb-vnext-directory-observation-audit-v1\0"
_GALLERY_METADATA_AUDIT_PREFIX = b"h2hdb-vnext-metadata-observation-audit-v1\0"
_GALLERY_SCAN_AUDIT_PREFIX = b"h2hdb-vnext-scan-observation-audit-v1\0"
_JPEG_NORMALIZED_SUFFIXES = frozenset(
    {b".avif", b".bmp", b".jpeg", b".jpg", b".png", b".webp"}
)

ANALYSIS_STATE_COMPONENTS = frozenset(
    {
        "file_hash_decision",
        "content_owner_candidate",
        "content_owner",
        "gid_candidate",
        "gid_winner",
    }
)

ARTIFACT_COMPONENT_KINDS = frozenset(
    {
        "source_manifest",
        "member_plan",
        "effective_content",
        "selected",
        "owner",
        "policy",
    }
)


class ArtifactMemberEntryKind(IntEnum):
    """Closed v1 member-plan entry-kind registry."""

    SOURCE_FILE = 0


class ArtifactSourceRole(IntEnum):
    """Closed v1 source-role registry."""

    METADATA = 0
    CONTENT = 1


class ArtifactTransformKind(IntEnum):
    """Closed v1 byte-transform registry."""

    RAW_COPY = 0
    GIF_NORMALIZE = 1
    JPEG_NORMALIZE = 2


class GalleryObservationComponent(IntEnum):
    """Closed gallery-observation page component registry."""

    FILE = 0
    TAG = 1
    DIRECTORY = 2
    METADATA = 3


class GalleryObservationNodeKind(IntEnum):
    """Closed gallery-observation page node registry."""

    LEAF = 0
    BRANCH = 1


class GalleryObservationDirectoryFileType(IntEnum):
    """No-follow ``lstat`` classifier for direct directory entries."""

    REGULAR = 0
    DIRECTORY = 1
    SYMLINK = 2
    OTHER = 3


class VNextIdentityError(ValueError):
    """Base class for a rejected vNext identity or byte-domain value."""


class IntegerDomainError(VNextIdentityError):
    """An integer is not in the contract's unsigned, non-zero domain."""


class ByteDomainError(VNextIdentityError):
    """Text or opaque bytes violate an exact byte-domain contract."""


class RegisteredIdentifierError(ByteDomainError):
    """An identifier is not an exact member of its registered ASCII domain."""


class DigestFormatError(VNextIdentityError):
    """A digest is not represented at the required binary or hex boundary."""


class DigestMismatchError(VNextIdentityError):
    """A supplied identity digest does not match its canonical input."""


class CanonicalIdentityCollisionError(VNextIdentityError):
    """One digest key was observed with a different canonical input."""


@dataclass(frozen=True, slots=True)
class AnalysisTitleScalarReceipt:
    """Fixed bounded result of one exact strict-UTF-8 title stream."""

    byte_count: int
    scalar_count: int

    def __post_init__(self) -> None:
        _require_int63(self.byte_count, field_name="title byte_count")
        _require_int63(self.scalar_count, field_name="title scalar_count")


class StrictUtf8ScalarCounter:
    """Incrementally validate strict UTF-8 and count Unicode scalar values."""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._byte_count = 0
        self._scalar_count = 0
        self._finished = False

    def feed(self, chunk: bytes) -> None:
        """Consume one exact chunk without retaining the unbounded title."""

        if self._finished:
            raise ByteDomainError("title scalar counter is already finalized")
        exact = _require_bytes(chunk, field_name="title UTF-8 chunk")
        try:
            decoded = self._decoder.decode(exact, final=False)
        except UnicodeDecodeError as error:
            raise ByteDomainError("title stream is not strict UTF-8") from error
        self._byte_count = _require_int63(
            self._byte_count + len(exact), field_name="title byte_count"
        )
        self._scalar_count = _require_int63(
            self._scalar_count + len(decoded), field_name="title scalar_count"
        )

    def finalize(self) -> AnalysisTitleScalarReceipt:
        """Require exact UTF-8 EOF and return the fixed scalar receipt."""

        if self._finished:
            raise ByteDomainError("title scalar counter is already finalized")
        try:
            decoded = self._decoder.decode(b"", final=True)
        except UnicodeDecodeError as error:
            raise ByteDomainError("title stream ends inside a UTF-8 scalar") from error
        self._finished = True
        self._scalar_count = _require_int63(
            self._scalar_count + len(decoded), field_name="title scalar_count"
        )
        return AnalysisTitleScalarReceipt(self._byte_count, self._scalar_count)


def count_analysis_title_scalars(
    chunks: Iterable[bytes],
) -> AnalysisTitleScalarReceipt:
    """Return the fixed receipt for a replayable strict-UTF-8 title stream."""

    counter = StrictUtf8ScalarCounter()
    for chunk in chunks:
        counter.feed(chunk)
    return counter.finalize()


@dataclass(frozen=True, slots=True)
class GalleryObservationFileEntry:
    """One exact FILE leaf record."""

    file_no: int
    file_key: bytes
    file_sha256: bytes
    size_bytes: int
    device: int
    inode: int
    modified_ns: int
    changed_ns: int

    def __post_init__(self) -> None:
        _require_int63(self.file_no, field_name="file_no")
        _require_digest(self.file_key, field_name="file_key")
        _require_digest(self.file_sha256, field_name="file_sha256")
        _require_int63(self.size_bytes, field_name="size_bytes")
        _require_uint(self.device, bits=64, field_name="device")
        _require_uint(self.inode, bits=64, field_name="inode")
        _require_int64(self.modified_ns, field_name="modified_ns")
        _require_int64(self.changed_ns, field_name="changed_ns")


@dataclass(frozen=True, slots=True)
class GalleryObservationTagEntry:
    """One exact TAG leaf record."""

    position: int
    namespace: str
    tag_value_sha256: bytes

    def __post_init__(self) -> None:
        _require_int63(self.position, field_name="position")
        validate_namespace(self.namespace)
        _require_digest(self.tag_value_sha256, field_name="tag_value_sha256")


@dataclass(frozen=True, slots=True)
class GalleryObservationDirectoryEntry:
    """One no-follow exact DIRECTORY leaf record.

    ``name_bytes`` is the byte-for-byte ``os.fsencode`` representation of the
    host dirent.  Filesystem adapters must preserve the platform
    ``surrogateescape`` round trip and classify ``file_type`` from ``lstat``;
    this pure codec never resolves a symlink.
    """

    canonical_ordinal: int
    name_bytes: bytes
    size_bytes: int
    device: int
    inode: int
    modified_ns: int
    changed_ns: int
    file_type: GalleryObservationDirectoryFileType

    def __post_init__(self) -> None:
        _require_int63(self.canonical_ordinal, field_name="canonical_ordinal")
        validate_file_name(self.name_bytes)
        _require_int63(self.size_bytes, field_name="size_bytes")
        _require_uint(self.device, bits=64, field_name="device")
        _require_uint(self.inode, bits=64, field_name="inode")
        _require_int64(self.modified_ns, field_name="modified_ns")
        _require_int64(self.changed_ns, field_name="changed_ns")
        if type(self.file_type) is not GalleryObservationDirectoryFileType:
            raise ByteDomainError(
                "file_type must be GalleryObservationDirectoryFileType"
            )


@dataclass(frozen=True, slots=True)
class GalleryObservationMetadataChunk:
    """One deterministic METADATA byte-stream chunk."""

    byte_offset: int
    chunk_bytes: bytes

    def __post_init__(self) -> None:
        _require_int63(self.byte_offset, field_name="byte_offset")
        chunk = _require_bytes(self.chunk_bytes, field_name="chunk_bytes")
        if not 1 <= len(chunk) <= GALLERY_OBSERVATION_METADATA_CHUNK_BYTES:
            raise ByteDomainError("metadata chunk length must be in 1..32768")


@dataclass(frozen=True, slots=True)
class GalleryObservationBranchEntry:
    """One ordered branch child descriptor."""

    child_sha256: bytes
    child_subtree_item_count: int

    def __post_init__(self) -> None:
        _require_digest(self.child_sha256, field_name="child_sha256")
        _require_positive_int63(
            self.child_subtree_item_count,
            field_name="child_subtree_item_count",
        )


GalleryObservationLeafEntry = (
    GalleryObservationFileEntry
    | GalleryObservationTagEntry
    | GalleryObservationDirectoryEntry
    | GalleryObservationMetadataChunk
)
GalleryObservationPageEntry = (
    GalleryObservationLeafEntry | GalleryObservationBranchEntry
)


@dataclass(frozen=True, slots=True)
class GalleryObservationPage:
    """Decoded semantic value of one exact bounded page frame."""

    component: GalleryObservationComponent
    node_kind: GalleryObservationNodeKind
    level: int
    subtree_item_count: int
    entries: tuple[GalleryObservationPageEntry, ...]

    def __post_init__(self) -> None:
        if type(self.component) is not GalleryObservationComponent:
            raise ByteDomainError("component must be GalleryObservationComponent")
        if type(self.node_kind) is not GalleryObservationNodeKind:
            raise ByteDomainError("node_kind must be GalleryObservationNodeKind")
        _require_uint(self.level, bits=8, field_name="level")
        if self.level > 8:
            raise IntegerDomainError("level must be in 0..8")
        _require_int63(self.subtree_item_count, field_name="subtree_item_count")
        object.__setattr__(self, "entries", tuple(self.entries))
        _validate_gallery_observation_page(self)


@dataclass(frozen=True, slots=True)
class GalleryObservationEncodedPage:
    """One content-addressed exact page frame."""

    page_sha256: bytes
    page_bytes: bytes

    def __post_init__(self) -> None:
        digest = _require_digest(self.page_sha256, field_name="page_sha256")
        payload = _require_bytes(self.page_bytes, field_name="page_bytes")
        if len(payload) > GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES:
            raise ByteDomainError("page_bytes exceeds 65536 bytes")
        if sha256(payload).digest() != digest:
            raise DigestMismatchError("page_sha256 does not match exact page_bytes")


@dataclass(frozen=True, slots=True)
class GalleryObservationTree:
    """Canonical tree result plus every page needed to persist it."""

    component: GalleryObservationComponent
    root_page_sha256: bytes
    item_count: int
    pages: tuple[GalleryObservationEncodedPage, ...]

    def __post_init__(self) -> None:
        if type(self.component) is not GalleryObservationComponent:
            raise ByteDomainError("component must be GalleryObservationComponent")
        _require_digest(self.root_page_sha256, field_name="root_page_sha256")
        _require_int63(self.item_count, field_name="item_count")
        object.__setattr__(self, "pages", tuple(self.pages))


@dataclass(frozen=True, slots=True)
class GalleryObservationMetadata:
    """Fixed-field input to the streaming METADATA component codec."""

    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: int
    download_time: int
    modified_time: int
    scan_observation_version: int
    source_file_count: int
    page_count: int | None

    def __post_init__(self) -> None:
        _require_positive_int63(self.gid, field_name="gid")
        for field_name in ("title", "comment", "upload_account"):
            _validate_unbounded_utf8(getattr(self, field_name), field_name=field_name)
        for field_name in ("upload_time", "download_time", "modified_time"):
            _require_int63(getattr(self, field_name), field_name=field_name)
        _require_positive_uint(
            self.scan_observation_version,
            bits=32,
            field_name="scan_observation_version",
        )
        _require_int63(self.source_file_count, field_name="source_file_count")
        if self.page_count is not None:
            _require_uint(self.page_count, bits=32, field_name="page_count")


@dataclass(frozen=True, slots=True)
class GalleryObservationDescriptor:
    """Bounded descriptor that pins the four canonical observation trees."""

    metadata_root_sha256: bytes
    metadata_byte_count: int
    file_root_sha256: bytes
    file_item_count: int
    tag_root_sha256: bytes
    tag_item_count: int
    directory_root_sha256: bytes
    directory_item_count: int

    def __post_init__(self) -> None:
        for field_name in (
            "metadata_root_sha256",
            "file_root_sha256",
            "tag_root_sha256",
            "directory_root_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name=field_name)
        for field_name in (
            "metadata_byte_count",
            "file_item_count",
            "tag_item_count",
            "directory_item_count",
        ):
            _require_int63(getattr(self, field_name), field_name=field_name)


@dataclass(frozen=True, slots=True)
class CanonicalValueChunk:
    """One deterministic exact payload chunk in an owner-scoped value tree."""

    byte_offset: int
    chunk_bytes: bytes

    def __post_init__(self) -> None:
        _require_int63(self.byte_offset, field_name="byte_offset")
        chunk = _require_bytes(self.chunk_bytes, field_name="chunk_bytes")
        if not 1 <= len(chunk) <= CANONICAL_VALUE_CHUNK_BYTES:
            raise ByteDomainError("canonical value chunk length must be in 1..32768")


@dataclass(frozen=True, slots=True)
class CanonicalValueBranchEntry:
    """One exact child/count record in a canonical-value branch page."""

    child_page_sha256: bytes
    child_subtree_byte_count: int

    def __post_init__(self) -> None:
        _require_digest(self.child_page_sha256, field_name="child_page_sha256")
        _require_positive_int63(
            self.child_subtree_byte_count,
            field_name="child_subtree_byte_count",
        )


CanonicalValuePageEntry = CanonicalValueChunk | CanonicalValueBranchEntry


@dataclass(frozen=True, slots=True)
class CanonicalValuePage:
    """Decoded semantic value of one owner-scoped bounded page frame."""

    owner_value_sha256: bytes
    node_kind: GalleryObservationNodeKind
    level: int
    page_position: int
    subtree_byte_count: int
    entries: tuple[CanonicalValuePageEntry, ...]

    def __post_init__(self) -> None:
        _require_digest(self.owner_value_sha256, field_name="owner_value_sha256")
        if type(self.node_kind) is not GalleryObservationNodeKind:
            raise ByteDomainError("node_kind must be GalleryObservationNodeKind")
        _require_uint(self.level, bits=8, field_name="level")
        if self.level > 8:
            raise IntegerDomainError("level must be in 0..8")
        _require_int63(self.page_position, field_name="page_position")
        _require_int63(self.subtree_byte_count, field_name="subtree_byte_count")
        object.__setattr__(self, "entries", tuple(self.entries))
        _validate_canonical_value_page(self)


@dataclass(frozen=True, slots=True)
class CanonicalValueEncodedPage:
    """One raw-SHA content-addressed owner-scoped page frame."""

    page_sha256: bytes
    page_bytes: bytes

    def __post_init__(self) -> None:
        digest = _require_digest(self.page_sha256, field_name="page_sha256")
        payload = _require_bytes(self.page_bytes, field_name="page_bytes")
        if not payload or len(payload) > CANONICAL_VALUE_PAGE_MAXIMUM_BYTES:
            raise ByteDomainError("canonical value page length must be in 1..65536")
        if sha256(payload).digest() != digest:
            raise DigestMismatchError("page_sha256 does not match exact page_bytes")


@dataclass(frozen=True, slots=True)
class CanonicalValueTree:
    """In-memory reference result for one deterministic owner-scoped tree."""

    owner_value_sha256: bytes
    root_page_sha256: bytes
    byte_count: int
    pages: tuple[CanonicalValueEncodedPage, ...]

    def __post_init__(self) -> None:
        _require_digest(self.owner_value_sha256, field_name="owner_value_sha256")
        _require_digest(self.root_page_sha256, field_name="root_page_sha256")
        _require_int63(self.byte_count, field_name="byte_count")
        object.__setattr__(self, "pages", tuple(self.pages))


@dataclass(frozen=True, slots=True)
class GalleryObservationMetadataScalarReceipt:
    """Bounded scalar result of exact incremental METADATA validation."""

    gid: int
    title_byte_count: int
    comment_byte_count: int
    upload_account_byte_count: int
    upload_time: int
    download_time: int
    modified_time: int
    scan_observation_version: int
    source_file_count: int
    page_count: int | None

    def __post_init__(self) -> None:
        _require_positive_int63(self.gid, field_name="gid")
        for field_name in (
            "title_byte_count",
            "comment_byte_count",
            "upload_account_byte_count",
            "upload_time",
            "download_time",
            "modified_time",
            "source_file_count",
        ):
            _require_int63(getattr(self, field_name), field_name=field_name)
        _require_positive_uint(
            self.scan_observation_version,
            bits=32,
            field_name="scan_observation_version",
        )
        if self.page_count is not None:
            _require_uint(self.page_count, bits=32, field_name="page_count")


@dataclass(frozen=True, slots=True)
class GalleryObservationMetadataDecoderState:
    """Serializable bounded checkpoint for the incremental metadata parser.

    A checkpoint is parser state, not standalone proof that earlier bytes ever
    existed.  Production resume must bind it atomically to the immutable
    page/receipt chain that produced it; a final seal never trusts a caller-
    supplied state without that provenance.
    """

    phase: str
    fixed_carry: bytes
    remaining_text_bytes: int
    utf8_tail: bytes
    gid: int | None
    text_lengths: tuple[int, int, int]
    upload_time: int | None
    download_time: int | None
    modified_time: int | None
    scan_observation_version: int | None
    source_file_count: int | None
    page_count: int | None

    def __post_init__(self) -> None:
        if type(self.phase) is not str:
            raise ByteDomainError("metadata decoder phase must be str")
        _require_bytes(self.fixed_carry, field_name="fixed_carry")
        _require_int63(
            self.remaining_text_bytes,
            field_name="remaining_text_bytes",
        )
        tail = _require_bytes(self.utf8_tail, field_name="utf8_tail")
        if len(tail) > 3:
            raise ByteDomainError("metadata decoder UTF-8 tail exceeds three bytes")
        if type(self.text_lengths) is not tuple or len(self.text_lengths) != 3:
            raise ByteDomainError("metadata decoder needs exactly three text lengths")
        for index, length in enumerate(self.text_lengths):
            _require_int63(length, field_name=f"text_lengths[{index}]")
        if self.gid is not None:
            _require_positive_int63(self.gid, field_name="gid")
        for field_name in (
            "upload_time",
            "download_time",
            "modified_time",
            "source_file_count",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_int63(value, field_name=field_name)
        if self.scan_observation_version is not None:
            _require_positive_uint(
                self.scan_observation_version,
                bits=32,
                field_name="scan_observation_version",
            )
        if self.page_count is not None:
            _require_uint(self.page_count, bits=32, field_name="page_count")


class GalleryObservationMetadataDecoder:
    """Bounded-state incremental validator for the exact METADATA stream."""

    _TEXT_PHASES = {"TITLE_TEXT": 0, "COMMENT_TEXT": 1, "ACCOUNT_TEXT": 2}
    _FIXED_SIZES = {
        "PREFIX": len(_GALLERY_OBSERVATION_METADATA_PREFIX),
        "VERSION": 4,
        "GID": 8,
        "TITLE_TAG": 1,
        "TITLE_LENGTH": 8,
        "COMMENT_TAG": 1,
        "COMMENT_LENGTH": 8,
        "ACCOUNT_TAG": 1,
        "ACCOUNT_LENGTH": 8,
        "UPLOAD_TIME": 8,
        "DOWNLOAD_TIME": 8,
        "MODIFIED_TIME": 8,
        "SCAN_VERSION": 4,
        "SOURCE_FILE_COUNT": 8,
        "PAGE_COUNT_PRESENCE": 1,
        "PAGE_COUNT": 4,
    }
    _PHASE_ORDER = (
        "PREFIX",
        "VERSION",
        "GID",
        "TITLE_TAG",
        "TITLE_LENGTH",
        "TITLE_TEXT",
        "COMMENT_TAG",
        "COMMENT_LENGTH",
        "COMMENT_TEXT",
        "ACCOUNT_TAG",
        "ACCOUNT_LENGTH",
        "ACCOUNT_TEXT",
        "UPLOAD_TIME",
        "DOWNLOAD_TIME",
        "MODIFIED_TIME",
        "SCAN_VERSION",
        "SOURCE_FILE_COUNT",
        "PAGE_COUNT_PRESENCE",
        "PAGE_COUNT",
        "DONE",
    )

    def __init__(
        self,
        state: GalleryObservationMetadataDecoderState | None = None,
    ) -> None:
        if state is None:
            self._phase = "PREFIX"
            self._fixed_carry = bytearray()
            self._remaining_text_bytes = 0
            self._utf8_tail = b""
            self._gid: int | None = None
            self._text_lengths = [0, 0, 0]
            self._upload_time: int | None = None
            self._download_time: int | None = None
            self._modified_time: int | None = None
            self._scan_version: int | None = None
            self._source_file_count: int | None = None
            self._page_count: int | None = None
            return
        if type(state) is not GalleryObservationMetadataDecoderState:
            raise ByteDomainError(
                "state must be GalleryObservationMetadataDecoderState"
            )
        if state.phase not in {*self._FIXED_SIZES, *self._TEXT_PHASES, "DONE"}:
            raise ByteDomainError("metadata decoder state has an unknown phase")
        if state.phase in self._TEXT_PHASES:
            if state.fixed_carry or state.remaining_text_bytes == 0:
                raise ByteDomainError("metadata text checkpoint is incoherent")
            text_index = self._TEXT_PHASES[state.phase]
            if state.remaining_text_bytes > state.text_lengths[text_index]:
                raise ByteDomainError(
                    "metadata text remainder exceeds its declared field length"
                )
            if any(state.text_lengths[index] for index in range(text_index + 1, 3)):
                raise ByteDomainError("future metadata text lengths must remain zero")
            consumed_text_bytes = (
                state.text_lengths[text_index] - state.remaining_text_bytes
            )
            if len(state.utf8_tail) > consumed_text_bytes:
                raise ByteDomainError(
                    "metadata UTF-8 tail exceeds consumed field bytes"
                )
            if state.utf8_tail:
                decoder = codecs.getincrementaldecoder("utf-8")("strict")
                try:
                    decoder.decode(state.utf8_tail, final=False)
                except UnicodeDecodeError as error:
                    raise ByteDomainError(
                        "metadata UTF-8 tail is not a valid incomplete prefix"
                    ) from error
                if decoder.getstate()[0] != state.utf8_tail:
                    raise ByteDomainError(
                        "metadata UTF-8 tail must be an incomplete code point"
                    )
        else:
            if state.remaining_text_bytes or state.utf8_tail:
                raise ByteDomainError("non-text metadata checkpoint has text carry")
            if state.phase == "DONE":
                if state.fixed_carry:
                    raise ByteDomainError("DONE metadata checkpoint has fixed carry")
            elif len(state.fixed_carry) >= self._FIXED_SIZES[state.phase]:
                raise ByteDomainError("metadata decoder fixed carry is not partial")
            known_text_counts = {
                "PREFIX": 0,
                "VERSION": 0,
                "GID": 0,
                "TITLE_TAG": 0,
                "TITLE_LENGTH": 0,
                "COMMENT_TAG": 1,
                "COMMENT_LENGTH": 1,
                "ACCOUNT_TAG": 2,
                "ACCOUNT_LENGTH": 2,
                "UPLOAD_TIME": 3,
                "DOWNLOAD_TIME": 3,
                "MODIFIED_TIME": 3,
                "SCAN_VERSION": 3,
                "SOURCE_FILE_COUNT": 3,
                "PAGE_COUNT_PRESENCE": 3,
                "PAGE_COUNT": 3,
                "DONE": 3,
            }
            known_count = known_text_counts[state.phase]
            if any(state.text_lengths[known_count:]):
                raise ByteDomainError("future metadata text lengths must remain zero")
        phase_index = self._PHASE_ORDER.index(state.phase)
        required_after = {
            "gid": "TITLE_TAG",
            "upload_time": "DOWNLOAD_TIME",
            "download_time": "MODIFIED_TIME",
            "modified_time": "SCAN_VERSION",
            "scan_observation_version": "SOURCE_FILE_COUNT",
            "source_file_count": "PAGE_COUNT_PRESENCE",
        }
        for field_name, first_phase in required_after.items():
            value = getattr(state, field_name)
            should_exist = phase_index >= self._PHASE_ORDER.index(first_phase)
            if should_exist != (value is not None):
                raise ByteDomainError(
                    f"metadata decoder {field_name} checkpoint is incoherent"
                )
        if state.phase == "PAGE_COUNT" and state.page_count is not None:
            raise ByteDomainError("PAGE_COUNT checkpoint already has a page count")
        if state.phase != "DONE" and state.page_count is not None:
            raise ByteDomainError("page_count exists before metadata DONE")
        self._phase = state.phase
        self._fixed_carry = bytearray(
            _require_bytes(state.fixed_carry, field_name="fixed_carry")
        )
        self._remaining_text_bytes = _require_int63(
            state.remaining_text_bytes,
            field_name="remaining_text_bytes",
        )
        self._utf8_tail = _require_bytes(state.utf8_tail, field_name="utf8_tail")
        self._gid = state.gid
        self._text_lengths = list(state.text_lengths)
        self._upload_time = state.upload_time
        self._download_time = state.download_time
        self._modified_time = state.modified_time
        self._scan_version = state.scan_observation_version
        self._source_file_count = state.source_file_count
        self._page_count = state.page_count

    @property
    def state(self) -> GalleryObservationMetadataDecoderState:
        """Return the complete bounded checkpoint needed for durable resume."""

        return GalleryObservationMetadataDecoderState(
            self._phase,
            bytes(self._fixed_carry),
            self._remaining_text_bytes,
            self._utf8_tail,
            self._gid,
            (
                self._text_lengths[0],
                self._text_lengths[1],
                self._text_lengths[2],
            ),
            self._upload_time,
            self._download_time,
            self._modified_time,
            self._scan_version,
            self._source_file_count,
            self._page_count,
        )

    def feed(self, part: bytes) -> None:
        """Advance the exact parser with one arbitrary page or network fragment."""

        exact = _require_bytes(part, field_name="metadata stream part")
        offset = 0
        while offset < len(exact):
            if self._phase == "DONE":
                raise ByteDomainError("gallery observation metadata has trailing bytes")
            if self._phase in self._TEXT_PHASES:
                amount = min(self._remaining_text_bytes, len(exact) - offset)
                piece = exact[offset : offset + amount]
                offset += amount
                self._remaining_text_bytes -= amount
                final = self._remaining_text_bytes == 0
                decoder = codecs.getincrementaldecoder("utf-8")("strict")
                decoder.setstate((self._utf8_tail, 0))
                try:
                    decoder.decode(piece, final=final)
                except UnicodeDecodeError as error:
                    raise ByteDomainError(
                        "metadata text must be strict UTF-8"
                    ) from error
                self._utf8_tail = decoder.getstate()[0]
                if final:
                    if self._utf8_tail:
                        raise ByteDomainError(
                            "metadata text ends inside a UTF-8 code point"
                        )
                    self._phase = {
                        "TITLE_TEXT": "COMMENT_TAG",
                        "COMMENT_TEXT": "ACCOUNT_TAG",
                        "ACCOUNT_TEXT": "UPLOAD_TIME",
                    }[self._phase]
                continue
            required = self._FIXED_SIZES[self._phase]
            amount = min(required - len(self._fixed_carry), len(exact) - offset)
            self._fixed_carry.extend(exact[offset : offset + amount])
            offset += amount
            if len(self._fixed_carry) == required:
                value = bytes(self._fixed_carry)
                self._fixed_carry.clear()
                self._accept_fixed(value)

    def finish(self) -> GalleryObservationMetadataScalarReceipt:
        """Require exact EOF and return bounded scalar/congruence evidence.

        If this decoder was restored, callers must also validate the durable
        page/receipt-chain provenance described by its state type.
        """

        if self._phase != "DONE" or self._fixed_carry or self._utf8_tail:
            raise ByteDomainError("gallery observation metadata is truncated")
        assert self._gid is not None
        assert self._upload_time is not None
        assert self._download_time is not None
        assert self._modified_time is not None
        assert self._scan_version is not None
        assert self._source_file_count is not None
        return GalleryObservationMetadataScalarReceipt(
            self._gid,
            self._text_lengths[0],
            self._text_lengths[1],
            self._text_lengths[2],
            self._upload_time,
            self._download_time,
            self._modified_time,
            self._scan_version,
            self._source_file_count,
            self._page_count,
        )

    def _accept_fixed(self, value: bytes) -> None:
        phase = self._phase
        if phase == "PREFIX":
            if value != _GALLERY_OBSERVATION_METADATA_PREFIX:
                raise ByteDomainError("gallery observation metadata has wrong prefix")
            self._phase = "VERSION"
        elif phase == "VERSION":
            if (
                int.from_bytes(value, "big")
                != GALLERY_OBSERVATION_METADATA_CODEC_VERSION
            ):
                raise IntegerDomainError("metadata codec_version is not registered")
            self._phase = "GID"
        elif phase == "GID":
            self._gid = _require_positive_int63(
                int.from_bytes(value, "big"), field_name="gid"
            )
            self._phase = "TITLE_TAG"
        elif phase.endswith("_TAG"):
            expected = {"TITLE_TAG": 1, "COMMENT_TAG": 2, "ACCOUNT_TAG": 3}[phase]
            if value != bytes((expected,)):
                raise ByteDomainError("metadata field tag is unknown or out of order")
            self._phase = phase.removesuffix("_TAG") + "_LENGTH"
        elif phase.endswith("_LENGTH"):
            index = {"TITLE_LENGTH": 0, "COMMENT_LENGTH": 1, "ACCOUNT_LENGTH": 2}[phase]
            length = _require_int63(
                int.from_bytes(value, "big"), field_name="metadata text length"
            )
            self._text_lengths[index] = length
            self._remaining_text_bytes = length
            self._utf8_tail = b""
            text_phase = ("TITLE_TEXT", "COMMENT_TEXT", "ACCOUNT_TEXT")[index]
            if length:
                self._phase = text_phase
            else:
                self._phase = {
                    "TITLE_TEXT": "COMMENT_TAG",
                    "COMMENT_TEXT": "ACCOUNT_TAG",
                    "ACCOUNT_TEXT": "UPLOAD_TIME",
                }[text_phase]
        elif phase == "UPLOAD_TIME":
            self._upload_time = _require_int63(
                int.from_bytes(value, "big"), field_name="upload_time"
            )
            self._phase = "DOWNLOAD_TIME"
        elif phase == "DOWNLOAD_TIME":
            self._download_time = _require_int63(
                int.from_bytes(value, "big"), field_name="download_time"
            )
            self._phase = "MODIFIED_TIME"
        elif phase == "MODIFIED_TIME":
            self._modified_time = _require_int63(
                int.from_bytes(value, "big"), field_name="modified_time"
            )
            self._phase = "SCAN_VERSION"
        elif phase == "SCAN_VERSION":
            self._scan_version = _require_positive_uint(
                int.from_bytes(value, "big"),
                bits=32,
                field_name="scan_observation_version",
            )
            self._phase = "SOURCE_FILE_COUNT"
        elif phase == "SOURCE_FILE_COUNT":
            self._source_file_count = _require_int63(
                int.from_bytes(value, "big"), field_name="source_file_count"
            )
            self._phase = "PAGE_COUNT_PRESENCE"
        elif phase == "PAGE_COUNT_PRESENCE":
            if value == b"\x00":
                self._page_count = None
                self._phase = "DONE"
            elif value == b"\x01":
                self._phase = "PAGE_COUNT"
            else:
                raise ByteDomainError("page_count presence must be zero or one")
        elif phase == "PAGE_COUNT":
            self._page_count = _require_uint(
                int.from_bytes(value, "big"),
                bits=32,
                field_name="page_count",
            )
            self._phase = "DONE"
        else:  # pragma: no cover - closed phase registry
            raise AssertionError("unreachable metadata decoder phase")


@dataclass(frozen=True, slots=True)
class ArtifactMemberPlanEntry:
    """One source-file row in the closed artifact member-plan v1 protocol.

    ``source_role`` and ``transform_kind`` are deliberately derived rather
    than accepted from the caller.  They remain explicit dataclass fields so a
    decoded plan exposes every encoded semantic field without allowing the
    duplicated tags to disagree with the exact source filename.
    """

    entry_position: int
    source_name_bytes: bytes
    source_file_sha256: bytes
    source_size_bytes: int
    excluded_flag: bool
    entry_kind: ArtifactMemberEntryKind = field(init=False)
    source_role: ArtifactSourceRole = field(init=False)
    transform_kind: ArtifactTransformKind = field(init=False)
    archive_member_name_bytes: bytes | None = field(init=False)

    def __post_init__(self) -> None:
        _require_int63(self.entry_position, field_name="entry_position")
        validate_file_name(self.source_name_bytes)
        _require_digest(
            self.source_file_sha256,
            field_name="source_file_sha256",
        )
        _require_int63(self.source_size_bytes, field_name="source_size_bytes")
        if type(self.excluded_flag) is not bool:
            raise ByteDomainError("excluded_flag must be exactly bool")
        object.__setattr__(
            self,
            "entry_kind",
            ArtifactMemberEntryKind.SOURCE_FILE,
        )
        object.__setattr__(
            self,
            "source_role",
            _source_role_for_name(self.source_name_bytes),
        )
        object.__setattr__(
            self,
            "transform_kind",
            _transform_kind_for_name(self.source_name_bytes),
        )
        object.__setattr__(
            self,
            "archive_member_name_bytes",
            artifact_archive_member_name(
                self.entry_position,
                self.source_role,
                self.transform_kind,
                self.excluded_flag,
            ),
        )


@dataclass(frozen=True, slots=True)
class ArtifactProtectionToken:
    """Decoded, exact artifact-storage protection-token v1 fields."""

    codec_version: int
    storage_codec_version: int
    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    artifact_locator_sha256: bytes
    receipt_id: bytes
    storage_generation: int
    size_bytes: int

    def __post_init__(self) -> None:
        _require_registered_version(
            self.codec_version,
            registered=ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION,
            field_name="artifact protection codec_version",
        )
        _require_positive_uint(
            self.storage_codec_version,
            bits=32,
            field_name="storage_codec_version",
        )
        candidate = _require_bytes(self.candidate_id, field_name="candidate_id")
        if len(candidate) != 16:
            raise ByteDomainError("candidate_id must contain exactly 16 bytes")
        _require_digest(self.publication_key, field_name="publication_key")
        _require_digest(self.artifact_sha256, field_name="artifact_sha256")
        _require_digest(
            self.artifact_locator_sha256,
            field_name="artifact_locator_sha256",
        )
        receipt = _require_bytes(self.receipt_id, field_name="receipt_id")
        if len(receipt) != 16:
            raise ByteDomainError("receipt_id must contain exactly 16 bytes")
        _require_int63(self.storage_generation, field_name="storage_generation")
        _require_int63(self.size_bytes, field_name="size_bytes")
        expected_receipt = artifact_storage_receipt_id(
            candidate,
            self.publication_key,
            self.artifact_sha256,
            self.artifact_locator_sha256,
            self.storage_generation,
            self.size_bytes,
        )
        if receipt != expected_receipt:
            raise DigestMismatchError(
                "artifact protection receipt_id does not match exact fields"
            )


@dataclass(frozen=True, slots=True)
class SourceSnapshotPolicy:
    """Natural analysis-policy tuple embedded in a retained source snapshot."""

    analysis_algorithm_version: int
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    content_owner_rule_version: int
    gid_winner_rule_version: int

    def __post_init__(self) -> None:
        _require_positive_uint(
            self.analysis_algorithm_version,
            bits=32,
            field_name="analysis_algorithm_version",
        )
        _require_int63(self.spam_artist_threshold, field_name="spam_artist_threshold")
        _require_int63(
            self.spam_occurrence_threshold, field_name="spam_occurrence_threshold"
        )
        _require_positive_uint(
            self.content_owner_rule_version,
            bits=32,
            field_name="content_owner_rule_version",
        )
        _require_positive_uint(
            self.gid_winner_rule_version,
            bits=32,
            field_name="gid_winner_rule_version",
        )


@dataclass(frozen=True, slots=True)
class SourceSnapshotCounts:
    """Declared build counters that must equal the gallery validation witnesses."""

    gallery_count: int
    file_count: int
    byte_count: int

    def __post_init__(self) -> None:
        _require_int63(self.gallery_count, field_name="gallery_count")
        _require_int63(self.file_count, field_name="file_count")
        _require_int63(self.byte_count, field_name="byte_count")


@dataclass(frozen=True, slots=True)
class SourceSnapshotGallery:
    """One stable gallery entry plus witnesses for aggregate validation.

    ``file_count`` and ``byte_count`` are checked against the top-level build
    counters.  They are not repeated inside the gallery section because the
    exact observation identity already commits to the gallery's file facts.
    """

    gallery_key: bytes
    observation_identity_sha256: bytes
    content_sha256: bytes | None
    gid: int
    file_count: int
    byte_count: int

    def __post_init__(self) -> None:
        _require_digest(self.gallery_key, field_name="gallery_key")
        _require_digest(
            self.observation_identity_sha256,
            field_name="observation_identity_sha256",
        )
        if self.content_sha256 is not None:
            _require_digest(self.content_sha256, field_name="content_sha256")
        _require_positive_int63(self.gid, field_name="gid")
        _require_int63(self.file_count, field_name="gallery file_count")
        _require_int63(self.byte_count, field_name="gallery byte_count")
        if self.file_count == 0 and self.byte_count != 0:
            raise IntegerDomainError(
                "a gallery with zero files must have zero byte_count"
            )


@dataclass(frozen=True, slots=True)
class SourceSnapshotFileHashDecision:
    """Exact aggregate inputs and supplied answer for one resolved file hash."""

    file_sha256: bytes
    occurrence_count: int
    artist_count: int
    maximum_gallery_artist_count: int
    excluded_flag: bool

    def __post_init__(self) -> None:
        _require_digest(self.file_sha256, field_name="file_sha256")
        _require_int63(self.occurrence_count, field_name="occurrence_count")
        _require_int63(self.artist_count, field_name="artist_count")
        _require_int63(
            self.maximum_gallery_artist_count,
            field_name="maximum_gallery_artist_count",
        )
        if type(self.excluded_flag) is not bool:
            raise ByteDomainError("excluded_flag must be exactly bool")


@dataclass(frozen=True, slots=True)
class SourceSnapshotContentOwner:
    """Stable content-group key and its stable owner gallery key."""

    content_sha256: bytes
    owner_gallery_key: bytes

    def __post_init__(self) -> None:
        _require_digest(self.content_sha256, field_name="content_sha256")
        _require_digest(self.owner_gallery_key, field_name="owner_gallery_key")


@dataclass(frozen=True, slots=True)
class SourceSnapshotGidWinner:
    """Unsigned GID group and its stable winner gallery key."""

    gid: int
    winner_gallery_key: bytes

    def __post_init__(self) -> None:
        _require_positive_int63(self.gid, field_name="gid")
        _require_digest(self.winner_gallery_key, field_name="winner_gallery_key")


@dataclass(frozen=True, slots=True)
class SourceRelativeLocatorValidationReceipt:
    """Bounded proof that one immutable locator stream reached exact EOF."""

    component_count: int
    payload_byte_count: int
    payload_sha256: bytes

    def __post_init__(self) -> None:
        _require_positive_uint(
            self.component_count, bits=32, field_name="component_count"
        )
        _require_int63(self.payload_byte_count, field_name="payload_byte_count")
        _require_digest(self.payload_sha256, field_name="payload_sha256")


@dataclass(frozen=True, slots=True)
class SourceRootValidationReceipt:
    """Bounded proof that one immutable absolute-root stream reached exact EOF."""

    component_count: int
    payload_byte_count: int
    payload_sha256: bytes

    def __post_init__(self) -> None:
        _require_uint(self.component_count, bits=32, field_name="component_count")
        _require_int63(self.payload_byte_count, field_name="payload_byte_count")
        _require_digest(self.payload_sha256, field_name="payload_sha256")


def canonical_value_digest(
    digest_domain: str,
    payload: bytes,
    *,
    codec_version: int = CANONICAL_DIGEST_CODEC_VERSION,
) -> bytes:
    """Return the canonical SHA-256 identity for exact opaque ``payload``.

    The preimage is the stable protocol framing
    ``ascii('h2hdb-vnext-canonical-value\\0') || u32be(codec_version) ||
    u32be(len(digest_domain)) || digest_domain_ascii ||
    u64be(len(payload)) || payload``.  The semantic domain is part of the
    digest; a database-assigned numeric policy ID is deliberately not.
    """

    validated_payload = _require_bytes(payload, field_name="payload")
    return canonical_value_digest_parts(
        digest_domain,
        len(validated_payload),
        (validated_payload,),
        codec_version=codec_version,
    )


def canonical_value_digest_parts(
    digest_domain: str,
    declared_byte_count: int,
    parts: Iterable[bytes],
    *,
    codec_version: int = CANONICAL_DIGEST_CODEC_VERSION,
) -> bytes:
    """Hash replayable exact parts without materializing one payload value.

    The caller must know ``declared_byte_count`` before hashing because the
    canonical frame places its u64 length before the payload.  The function
    verifies the actual streamed count and exact EOF before returning a digest;
    unknown-length adapters must spool/count first and then replay.
    """

    version = _require_registered_version(
        codec_version,
        registered=CANONICAL_DIGEST_CODEC_VERSION,
        field_name="codec_version",
    )
    domain = _validate_ascii_identifier_bytes(
        digest_domain,
        field_name="digest_domain",
        maximum_bytes=64,
    )
    payload_length = _require_int63(
        declared_byte_count,
        field_name="declared_byte_count",
    )

    digest = sha256(b"h2hdb-vnext-canonical-value\0")
    digest.update(version.to_bytes(4, "big"))
    digest.update(len(domain).to_bytes(4, "big"))
    digest.update(domain)
    digest.update(payload_length.to_bytes(8, "big"))
    consumed = 0
    for part in parts:
        exact = _require_bytes(part, field_name="payload part")
        consumed += len(exact)
        if consumed > payload_length:
            raise ByteDomainError("payload parts exceed declared_byte_count")
        digest.update(exact)
    if consumed != payload_length:
        raise ByteDomainError("payload parts do not equal declared_byte_count")
    return digest.digest()


def canonical_value_digest_hex(
    digest_domain: str,
    payload: bytes,
    *,
    codec_version: int = CANONICAL_DIGEST_CODEC_VERSION,
) -> str:
    """Return the API-only lowercase hex form of a canonical value digest."""

    return digest_to_hex(
        canonical_value_digest(digest_domain, payload, codec_version=codec_version)
    )


def encode_canonical_value_page(
    page: CanonicalValuePage,
    *,
    codec_version: int = CANONICAL_VALUE_PAGE_CODEC_VERSION,
) -> bytes:
    """Encode one exact owner-scoped canonical-value page frame."""

    if type(page) is not CanonicalValuePage:
        raise ByteDomainError("page must be CanonicalValuePage")
    version = _require_registered_version(
        codec_version,
        registered=CANONICAL_VALUE_PAGE_CODEC_VERSION,
        field_name="codec_version",
    )
    payload = bytearray(_CANONICAL_VALUE_PAGE_PREFIX)
    payload.extend(version.to_bytes(4, "big"))
    payload.extend(page.owner_value_sha256)
    payload.append(int(page.node_kind))
    payload.append(page.level)
    payload.extend(page.page_position.to_bytes(8, "big"))
    payload.extend(page.subtree_byte_count.to_bytes(8, "big"))
    payload.extend(len(page.entries).to_bytes(4, "big"))
    for entry in page.entries:
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            assert type(entry) is CanonicalValueChunk
            payload.extend(entry.byte_offset.to_bytes(8, "big"))
            payload.extend(len(entry.chunk_bytes).to_bytes(4, "big"))
            payload.extend(entry.chunk_bytes)
        else:
            assert type(entry) is CanonicalValueBranchEntry
            payload.extend(entry.child_page_sha256)
            payload.extend(entry.child_subtree_byte_count.to_bytes(8, "big"))
    if len(payload) > CANONICAL_VALUE_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("encoded canonical value page exceeds 65536 bytes")
    return bytes(payload)


def canonical_value_page_digest(page_bytes: bytes) -> bytes:
    """Return raw SHA-256 of one exact owner-prefixed page frame."""

    payload = _require_bytes(page_bytes, field_name="page_bytes")
    if not payload or len(payload) > CANONICAL_VALUE_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("page_bytes length must be in 1..65536")
    decode_canonical_value_page(payload)
    return sha256(payload).digest()


def verify_canonical_value_page_conflict(
    existing_page_sha256: bytes,
    existing_page_bytes: bytes,
    proposed_page_bytes: bytes,
) -> None:
    """Require byte-for-byte equality before reusing an owner-scoped page."""

    expected = _require_digest(existing_page_sha256, field_name="page_sha256")
    existing = _require_bytes(existing_page_bytes, field_name="existing_page_bytes")
    proposed = _require_bytes(proposed_page_bytes, field_name="proposed_page_bytes")
    if canonical_value_page_digest(existing) != expected:
        raise DigestMismatchError("existing page bytes do not match page_sha256")
    if proposed != existing:
        raise CanonicalIdentityCollisionError(
            "canonical value page digest maps to different exact page bytes"
        )
    if canonical_value_page_digest(proposed) != expected:  # pragma: no cover
        raise DigestMismatchError("proposed page bytes do not match page_sha256")


def decode_canonical_value_page(payload: bytes) -> CanonicalValuePage:
    """Decode and re-encode-check one exact owner-scoped page frame."""

    encoded = _require_bytes(payload, field_name="canonical value page")
    header_size = len(_CANONICAL_VALUE_PAGE_PREFIX) + 58
    if len(encoded) < header_size or len(encoded) > CANONICAL_VALUE_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("canonical value page is truncated or oversized")
    if not encoded.startswith(_CANONICAL_VALUE_PAGE_PREFIX):
        raise ByteDomainError("canonical value page has the wrong prefix")
    offset = len(_CANONICAL_VALUE_PAGE_PREFIX)
    version, offset = _take_uint(encoded, offset, 4, "page codec version")
    if version != CANONICAL_VALUE_PAGE_CODEC_VERSION:
        raise IntegerDomainError(f"codec_version {version} is not registered")
    owner, offset = _take_exact(encoded, offset, 32, "owner_value_sha256")
    node_raw, offset = _take_uint(encoded, offset, 1, "node kind")
    level, offset = _take_uint(encoded, offset, 1, "level")
    page_position, offset = _take_uint(encoded, offset, 8, "page_position")
    subtree_count, offset = _take_uint(encoded, offset, 8, "subtree_byte_count")
    entry_count, offset = _take_uint(encoded, offset, 4, "entry_count")
    try:
        node_kind = GalleryObservationNodeKind(node_raw)
    except ValueError as error:
        raise ByteDomainError("unknown canonical value node kind") from error
    maximum_entries = 1 if node_kind is GalleryObservationNodeKind.LEAF else 256
    if entry_count > maximum_entries:
        raise ByteDomainError("canonical value page entry_count exceeds capacity")
    entries: list[CanonicalValuePageEntry] = []
    for _ in range(entry_count):
        if node_kind is GalleryObservationNodeKind.LEAF:
            byte_offset, offset = _take_uint(encoded, offset, 8, "byte_offset")
            chunk_size, offset = _take_uint(encoded, offset, 4, "chunk length")
            chunk, offset = _take_exact(encoded, offset, chunk_size, "chunk bytes")
            entries.append(CanonicalValueChunk(byte_offset, chunk))
        else:
            child, offset = _take_exact(encoded, offset, 32, "child page digest")
            count, offset = _take_uint(encoded, offset, 8, "child byte count")
            entries.append(CanonicalValueBranchEntry(child, count))
    if offset != len(encoded):
        raise ByteDomainError("canonical value page contains trailing bytes")
    page = CanonicalValuePage(
        owner,
        node_kind,
        level,
        page_position,
        subtree_count,
        tuple(entries),
    )
    if encode_canonical_value_page(page) != encoded:
        raise ByteDomainError("canonical value page is not canonical")
    return page


def build_canonical_value_tree(
    owner_value_sha256: bytes,
    declared_byte_count: int,
    parts: Iterable[bytes],
) -> CanonicalValueTree:
    """Build every page as an in-memory reference oracle, not a giant writer."""

    owner = _require_digest(owner_value_sha256, field_name="owner_value_sha256")
    byte_count = _require_int63(declared_byte_count, field_name="declared_byte_count")
    chunks = tuple(_chunk_canonical_value_parts(parts, byte_count))
    pages: list[CanonicalValueEncodedPage] = []
    current: list[tuple[CanonicalValueEncodedPage, int]] = []
    if not chunks:
        leaf = CanonicalValuePage(
            owner,
            GalleryObservationNodeKind.LEAF,
            0,
            0,
            0,
            (),
        )
        encoded = _encode_canonical_value_page_value(leaf)
        return CanonicalValueTree(owner, encoded.page_sha256, 0, (encoded,))
    for position, chunk in enumerate(chunks):
        leaf = CanonicalValuePage(
            owner,
            GalleryObservationNodeKind.LEAF,
            0,
            position,
            len(chunk.chunk_bytes),
            (chunk,),
        )
        encoded = _encode_canonical_value_page_value(leaf)
        pages.append(encoded)
        current.append((encoded, len(chunk.chunk_bytes)))
    level = 1
    while len(current) > 1:
        if level > 8:
            raise IntegerDomainError("canonical value tree exceeds depth eight")
        next_level: list[tuple[CanonicalValueEncodedPage, int]] = []
        for position, start in enumerate(range(0, len(current), 256)):
            group = current[start : start + 256]
            total = sum(count for _page, count in group)
            if total > _INT63_MAX:
                raise IntegerDomainError("canonical value tree exceeds signed-int63")
            branch = CanonicalValuePage(
                owner,
                GalleryObservationNodeKind.BRANCH,
                level,
                position,
                total,
                tuple(
                    CanonicalValueBranchEntry(page.page_sha256, count)
                    for page, count in group
                ),
            )
            encoded = _encode_canonical_value_page_value(branch)
            pages.append(encoded)
            next_level.append((encoded, total))
        current = next_level
        level += 1
    root, count = current[0]
    tree = CanonicalValueTree(owner, root.page_sha256, count, tuple(pages))
    validate_canonical_value_tree(tree)
    return tree


def validate_canonical_value_tree(tree: CanonicalValueTree) -> None:
    """Prove exact owner, counts, offsets, positions, fanout, and minimal height."""

    if type(tree) is not CanonicalValueTree:
        raise ByteDomainError("tree must be CanonicalValueTree")
    decoded: dict[bytes, CanonicalValuePage] = {}
    for encoded in tree.pages:
        page = decode_canonical_value_page(encoded.page_bytes)
        if page.owner_value_sha256 != tree.owner_value_sha256:
            raise ByteDomainError("canonical value tree contains a cross-owner page")
        if encoded.page_sha256 in decoded:
            raise ByteDomainError("canonical value tree contains a duplicate page")
        decoded[encoded.page_sha256] = page
    root = decoded.get(tree.root_page_sha256)
    if root is None:
        raise ByteDomainError("canonical value tree root is missing")
    visited: set[bytes] = set()
    levels: dict[int, list[CanonicalValuePage]] = {}
    leaves: list[CanonicalValuePage] = []

    def visit(digest: bytes, expected_level: int | None = None) -> int:
        if digest in visited:
            raise ByteDomainError("canonical value tree reuses one child page")
        visited.add(digest)
        page = decoded[digest]
        if expected_level is not None and page.level != expected_level:
            raise ByteDomainError("canonical value child level mismatch")
        levels.setdefault(page.level, []).append(page)
        if page.level == 0:
            leaves.append(page)
            return page.subtree_byte_count
        total = 0
        for entry in page.entries:
            assert type(entry) is CanonicalValueBranchEntry
            child = decoded.get(entry.child_page_sha256)
            if child is None:
                raise ByteDomainError("canonical value branch child is missing")
            if child.subtree_byte_count != entry.child_subtree_byte_count:
                raise ByteDomainError("canonical value child count mismatch")
            total += visit(entry.child_page_sha256, page.level - 1)
            if total > _INT63_MAX:
                raise IntegerDomainError("canonical value tree count exceeds int63")
        if total != page.subtree_byte_count:
            raise ByteDomainError("canonical value branch count mismatch")
        return total

    total = visit(tree.root_page_sha256)
    if total != tree.byte_count or visited != set(decoded):
        raise ByteDomainError("canonical value root count or reachability mismatch")
    if root.level > 0 and len(root.entries) < 2:
        raise ByteDomainError("canonical value tree has a unary root wrapper")
    for level, pages_at_level in levels.items():
        if [page.page_position for page in pages_at_level] != list(
            range(len(pages_at_level))
        ):
            raise ByteDomainError("page_position is not zero-based contiguous")
        if level > 0:
            for page in pages_at_level[:-1]:
                if len(page.entries) != CANONICAL_VALUE_BRANCH_CAPACITY:
                    raise ByteDomainError("nonfinal canonical branch is not full")
    expected_offset = 0
    if tree.byte_count == 0:
        if len(leaves) != 1 or leaves[0].entries:
            raise ByteDomainError("empty canonical value must be one empty leaf")
        return
    for index, leaf in enumerate(leaves):
        if len(leaf.entries) != 1:
            raise ByteDomainError("nonempty canonical value leaf needs one chunk")
        chunk = leaf.entries[0]
        assert type(chunk) is CanonicalValueChunk
        if chunk.byte_offset != expected_offset:
            raise ByteDomainError("canonical value chunk offsets are not contiguous")
        if index < len(leaves) - 1 and len(chunk.chunk_bytes) != 32768:
            raise ByteDomainError("nonfinal canonical value chunk is not full")
        expected_offset += len(chunk.chunk_bytes)
    if expected_offset != tree.byte_count:
        raise ByteDomainError("canonical value chunks do not match byte_count")


def encode_effective_content(
    file_sha256s: Sequence[bytes],
    *,
    encoding_version: int = EFFECTIVE_CONTENT_ENCODING_VERSION,
) -> bytes:
    """Materialize the reference effective-content v1 preimage.

    The caller supplies only resolved, non-excluded ``CONTENT`` file digests.
    Digests are sorted as unsigned 32-byte values, making input permutations
    equivalent while retaining repeated values as distinct occurrences.
    """

    version = _require_registered_version(
        encoding_version,
        registered=EFFECTIVE_CONTENT_ENCODING_VERSION,
        field_name="encoding_version",
    )
    if len(file_sha256s) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("effective content has too many file digests")
    ordered = sorted(
        _require_digest(value, field_name="file_sha256") for value in file_sha256s
    )

    return b"".join(
        iter_effective_content_payload_ordered(
            len(ordered), ordered, encoding_version=version
        )
    )


def iter_effective_content_payload_ordered(
    file_count: int,
    file_sha256s: Iterable[bytes],
    *,
    encoding_version: int = EFFECTIVE_CONTENT_ENCODING_VERSION,
) -> Iterator[bytes]:
    """Yield a pre-sorted effective-content frame with O(1) codec memory.

    Production obtains the unsigned-bytewise order from a keyset query or an
    external bounded sort.  Duplicate adjacent digests remain significant.
    """

    version = _require_registered_version(
        encoding_version,
        registered=EFFECTIVE_CONTENT_ENCODING_VERSION,
        field_name="encoding_version",
    )
    count = _require_int63(file_count, field_name="file_count")
    yield _EFFECTIVE_CONTENT_PREFIX
    yield version.to_bytes(4, "big")
    yield count.to_bytes(8, "big")
    previous: bytes | None = None
    emitted = 0
    for value in file_sha256s:
        digest = _require_digest(value, field_name="file_sha256")
        if previous is not None and digest < previous:
            raise ByteDomainError("effective content digests are not ordered")
        previous = digest
        emitted += 1
        if emitted > count:
            raise ByteDomainError("effective content exceeds declared file_count")
        yield digest
    if emitted != count:
        raise ByteDomainError("effective content does not equal declared file_count")


def effective_content_digest(
    file_sha256s: Sequence[bytes],
    *,
    encoding_version: int = EFFECTIVE_CONTENT_ENCODING_VERSION,
) -> bytes:
    """Return the reference ``effective_content_v1`` identity digest."""

    ordered = sorted(
        _require_digest(value, field_name="file_sha256") for value in file_sha256s
    )
    parts = iter_effective_content_payload_ordered(
        len(ordered), ordered, encoding_version=encoding_version
    )
    byte_count = len(_EFFECTIVE_CONTENT_PREFIX) + 12 + 32 * len(ordered)
    return canonical_value_digest_parts(
        EFFECTIVE_CONTENT_DIGEST_DOMAIN,
        byte_count,
        parts,
    )


def effective_content_digest_ordered(
    file_count: int,
    file_sha256s: Iterable[bytes],
    *,
    encoding_version: int = EFFECTIVE_CONTENT_ENCODING_VERSION,
) -> bytes:
    """Hash a keyset-ordered effective-content stream without buffering it."""

    count = _require_int63(file_count, field_name="file_count")
    return canonical_value_digest_parts(
        EFFECTIVE_CONTENT_DIGEST_DOMAIN,
        len(_EFFECTIVE_CONTENT_PREFIX) + 12 + 32 * count,
        iter_effective_content_payload_ordered(
            count,
            file_sha256s,
            encoding_version=encoding_version,
        ),
    )


def analysis_candidate_has_already_uploaded(
    tag_values_utf8: Iterable[bytes],
) -> bool:
    """Match the v1 marker without Unicode, locale, or collation behavior.

    Every tag value must already be exact UTF-8. Equality maps only ASCII
    ``A`` through ``Z`` to lowercase; all other bytes remain unchanged.
    Namespaces are intentionally absent because the v1 policy ignores them.
    """

    matched = False
    for value in tag_values_utf8:
        exact = _require_bytes(value, field_name="tag_value_utf8")
        _require_exact_utf8(exact, field_name="tag_value_utf8")
        folded = bytes(byte + 32 if 65 <= byte <= 90 else byte for byte in exact)
        if folded == ANALYSIS_ALREADY_UPLOADED_MARKER:
            matched = True
    return matched


def encode_artifact_source_manifest(
    observation_identity_sha256: bytes,
    manifest_algorithm_version: int,
    file_order_version: int,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Encode the exact source-manifest component for one gallery artifact."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_COMPONENT_CODEC_VERSION,
        field_name="codec_version",
    )
    observation = _require_digest(
        observation_identity_sha256,
        field_name="observation_identity_sha256",
    )
    manifest_version = _require_positive_uint(
        manifest_algorithm_version,
        bits=32,
        field_name="manifest_algorithm_version",
    )
    order_version = _require_positive_uint(
        file_order_version,
        bits=32,
        field_name="file_order_version",
    )
    return b"".join(
        (
            _ARTIFACT_SOURCE_MANIFEST_PREFIX,
            version.to_bytes(4, "big"),
            observation,
            manifest_version.to_bytes(4, "big"),
            order_version.to_bytes(4, "big"),
        )
    )


def artifact_source_manifest_digest(
    observation_identity_sha256: bytes,
    manifest_algorithm_version: int,
    file_order_version: int,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Return the canonical source-manifest component identity."""

    return canonical_value_digest(
        ARTIFACT_SOURCE_MANIFEST_DIGEST_DOMAIN,
        encode_artifact_source_manifest(
            observation_identity_sha256,
            manifest_algorithm_version,
            file_order_version,
            codec_version=codec_version,
        ),
    )


def encode_artifact_effective_content(
    file_sha256s: Sequence[bytes],
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Materialize the reference sorted artifact effective-content payload."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_COMPONENT_CODEC_VERSION,
        field_name="codec_version",
    )
    if len(file_sha256s) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("artifact effective content has too many file digests")
    ordered = sorted(
        _require_digest(value, field_name="file_sha256") for value in file_sha256s
    )
    return b"".join(
        iter_artifact_effective_content_payload_ordered(
            len(ordered), ordered, codec_version=version
        )
    )


def iter_artifact_effective_content_payload_ordered(
    file_count: int,
    file_sha256s: Iterable[bytes],
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> Iterator[bytes]:
    """Yield the pre-sorted artifact component with O(1) codec memory."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_COMPONENT_CODEC_VERSION,
        field_name="codec_version",
    )
    count = _require_int63(file_count, field_name="file_count")
    yield _ARTIFACT_EFFECTIVE_CONTENT_PREFIX
    yield version.to_bytes(4, "big")
    yield count.to_bytes(8, "big")
    previous: bytes | None = None
    emitted = 0
    for value in file_sha256s:
        digest = _require_digest(value, field_name="file_sha256")
        if previous is not None and digest < previous:
            raise ByteDomainError("artifact effective content digests are not ordered")
        previous = digest
        emitted += 1
        if emitted > count:
            raise ByteDomainError(
                "artifact effective content exceeds declared file_count"
            )
        yield digest
    if emitted != count:
        raise ByteDomainError(
            "artifact effective content does not equal declared file_count"
        )


def artifact_effective_content_digest(
    file_sha256s: Sequence[bytes],
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Return the reference artifact effective-content component identity."""

    ordered = sorted(
        _require_digest(value, field_name="file_sha256") for value in file_sha256s
    )
    parts = iter_artifact_effective_content_payload_ordered(
        len(ordered), ordered, codec_version=codec_version
    )
    byte_count = len(_ARTIFACT_EFFECTIVE_CONTENT_PREFIX) + 12 + 32 * len(ordered)
    return canonical_value_digest_parts(
        ARTIFACT_EFFECTIVE_CONTENT_DIGEST_DOMAIN,
        byte_count,
        parts,
    )


def artifact_effective_content_digest_ordered(
    file_count: int,
    file_sha256s: Iterable[bytes],
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Hash keyset-ordered artifact content with O(1) codec memory."""

    count = _require_int63(file_count, field_name="file_count")
    return canonical_value_digest_parts(
        ARTIFACT_EFFECTIVE_CONTENT_DIGEST_DOMAIN,
        len(_ARTIFACT_EFFECTIVE_CONTENT_PREFIX) + 12 + 32 * count,
        iter_artifact_effective_content_payload_ordered(
            count,
            file_sha256s,
            codec_version=codec_version,
        ),
    )


def encode_artifact_selected(
    publication_key_value: bytes,
    gallery_key_value: bytes,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Encode selected occurrence identity without using item-audit digests as authority."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_COMPONENT_CODEC_VERSION,
        field_name="codec_version",
    )
    publication = _require_digest(
        publication_key_value,
        field_name="publication_key",
    )
    gallery = _require_digest(gallery_key_value, field_name="gallery_key")
    return b"".join(
        (
            _ARTIFACT_SELECTED_PREFIX,
            version.to_bytes(4, "big"),
            publication,
            gallery,
        )
    )


def artifact_selected_digest(
    publication_key_value: bytes,
    gallery_key_value: bytes,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Return the canonical selected component identity."""

    return canonical_value_digest(
        ARTIFACT_SELECTED_DIGEST_DOMAIN,
        encode_artifact_selected(
            publication_key_value,
            gallery_key_value,
            codec_version=codec_version,
        ),
    )


def encode_artifact_owner(
    content_sha256: bytes,
    owner_gallery_key: bytes,
    gid: int,
    winner_gallery_key: bytes,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Encode exact resolved owner/winner facts without decision audit digests."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_COMPONENT_CODEC_VERSION,
        field_name="codec_version",
    )
    content = _require_digest(content_sha256, field_name="content_sha256")
    owner = _require_digest(owner_gallery_key, field_name="owner_gallery_key")
    validated_gid = _require_positive_int63(gid, field_name="gid")
    winner = _require_digest(winner_gallery_key, field_name="winner_gallery_key")
    return b"".join(
        (
            _ARTIFACT_OWNER_PREFIX,
            version.to_bytes(4, "big"),
            content,
            owner,
            validated_gid.to_bytes(8, "big"),
            winner,
        )
    )


def artifact_owner_digest(
    content_sha256: bytes,
    owner_gallery_key: bytes,
    gid: int,
    winner_gallery_key: bytes,
    *,
    codec_version: int = ARTIFACT_COMPONENT_CODEC_VERSION,
) -> bytes:
    """Return the canonical owner component identity."""

    return canonical_value_digest(
        ARTIFACT_OWNER_DIGEST_DOMAIN,
        encode_artifact_owner(
            content_sha256,
            owner_gallery_key,
            gid,
            winner_gallery_key,
            codec_version=codec_version,
        ),
    )


def encode_artifact_policy(
    artifact_algorithm_version: int,
    max_image_short_side: int,
    producer_fingerprint_sha256: bytes,
    *,
    codec_version: int = ARTIFACT_POLICY_CODEC_VERSION,
) -> bytes:
    """Encode the complete natural policy tuple for artifact byte semantics."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_POLICY_CODEC_VERSION,
        field_name="codec_version",
    )
    algorithm = _require_positive_uint(
        artifact_algorithm_version,
        bits=32,
        field_name="artifact_algorithm_version",
    )
    short_side = _require_positive_uint(
        max_image_short_side,
        bits=32,
        field_name="max_image_short_side",
    )
    producer = _require_digest(
        producer_fingerprint_sha256,
        field_name="producer_fingerprint_sha256",
    )
    return b"".join(
        (
            _ARTIFACT_POLICY_PREFIX,
            version.to_bytes(4, "big"),
            algorithm.to_bytes(4, "big"),
            short_side.to_bytes(4, "big"),
            producer,
        )
    )


def artifact_policy_digest(
    artifact_algorithm_version: int,
    max_image_short_side: int,
    producer_fingerprint_sha256: bytes,
    *,
    codec_version: int = ARTIFACT_POLICY_CODEC_VERSION,
) -> bytes:
    """Return the canonical policy component identity."""

    return canonical_value_digest(
        ARTIFACT_POLICY_DIGEST_DOMAIN,
        encode_artifact_policy(
            artifact_algorithm_version,
            max_image_short_side,
            producer_fingerprint_sha256,
            codec_version=codec_version,
        ),
    )


def encode_artifact_producer_fingerprint(
    writer_id: bytes,
    python_abi: bytes,
    pillow_build: bytes,
    libjpeg_build: bytes,
    zlib_build: bytes,
    *,
    codec_version: int = ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION,
) -> bytes:
    """Encode the exact closed producer/build fingerprint preimage."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION,
        field_name="artifact producer codec_version",
    )
    parts = [_ARTIFACT_PRODUCER_FINGERPRINT_PREFIX, version.to_bytes(4, "big")]
    for field_name, value in (
        ("writer_id", writer_id),
        ("python_abi", python_abi),
        ("pillow_build", pillow_build),
        ("libjpeg_build", libjpeg_build),
        ("zlib_build", zlib_build),
    ):
        exact = _require_bytes(value, field_name=field_name)
        if not exact:
            raise ByteDomainError(f"{field_name} must not be empty")
        if len(exact) > _UINT32_MAX:  # pragma: no cover - impossible in CPython
            raise ByteDomainError(f"{field_name} exceeds u32 framing")
        parts.extend((len(exact).to_bytes(4, "big"), exact))
    return b"".join(parts)


def artifact_producer_fingerprint_sha256(
    writer_id: bytes,
    python_abi: bytes,
    pillow_build: bytes,
    libjpeg_build: bytes,
    zlib_build: bytes,
    *,
    codec_version: int = ARTIFACT_PRODUCER_FINGERPRINT_CODEC_VERSION,
) -> bytes:
    """Return raw SHA-256 of one exact producer fingerprint frame."""

    return sha256(
        encode_artifact_producer_fingerprint(
            writer_id,
            python_abi,
            pillow_build,
            libjpeg_build,
            zlib_build,
            codec_version=codec_version,
        )
    ).digest()


def artifact_producer_equivalence_class(
    producer_fingerprint_sha256: bytes,
) -> bytes:
    """Return the repository-certified exact producer equivalence class.

    vNext does not accept a caller's claim that two distinct producer builds
    emit identical bytes.  Until an independently certified equivalence
    protocol exists, every complete producer fingerprint is its own class.
    """

    fingerprint = _require_digest(
        producer_fingerprint_sha256,
        field_name="producer_fingerprint_sha256",
    )
    return _ARTIFACT_PRODUCER_EQUIVALENCE_PREFIX + fingerprint


def encode_artifact_semantics(
    source_manifest_component_sha256: bytes,
    member_plan_component_sha256: bytes,
    effective_content_component_sha256: bytes,
    selected_component_sha256: bytes,
    owner_component_sha256: bytes,
    policy_component_sha256: bytes,
    *,
    codec_version: int = ARTIFACT_SEMANTICS_CODEC_VERSION,
) -> bytes:
    """Encode the exact ordered six-component artifact semantic tuple."""

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_SEMANTICS_CODEC_VERSION,
        field_name="codec_version",
    )
    components = tuple(
        _require_digest(value, field_name=field_name)
        for field_name, value in (
            ("source_manifest_component_sha256", source_manifest_component_sha256),
            ("member_plan_component_sha256", member_plan_component_sha256),
            (
                "effective_content_component_sha256",
                effective_content_component_sha256,
            ),
            ("selected_component_sha256", selected_component_sha256),
            ("owner_component_sha256", owner_component_sha256),
            ("policy_component_sha256", policy_component_sha256),
        )
    )
    return b"".join(
        (
            _ARTIFACT_SEMANTICS_PREFIX,
            version.to_bytes(4, "big"),
            *components,
        )
    )


def artifact_semantics_digest(
    source_manifest_component_sha256: bytes,
    member_plan_component_sha256: bytes,
    effective_content_component_sha256: bytes,
    selected_component_sha256: bytes,
    owner_component_sha256: bytes,
    policy_component_sha256: bytes,
    *,
    codec_version: int = ARTIFACT_SEMANTICS_CODEC_VERSION,
) -> bytes:
    """Return the global collision-checked artifact semantic identity."""

    return canonical_value_digest(
        ARTIFACT_SEMANTICS_DIGEST_DOMAIN,
        encode_artifact_semantics(
            source_manifest_component_sha256,
            member_plan_component_sha256,
            effective_content_component_sha256,
            selected_component_sha256,
            owner_component_sha256,
            policy_component_sha256,
            codec_version=codec_version,
        ),
    )


def encode_zip_comment(
    source_manifest_component_sha256: bytes,
    effective_content_component_sha256: bytes,
    *,
    codec_version: int = ZIP_COMMENT_CODEC_VERSION,
) -> bytes:
    """Encode the one exact composite ZIP comment envelope."""

    version = _require_registered_version(
        codec_version,
        registered=ZIP_COMMENT_CODEC_VERSION,
        field_name="codec_version",
    )
    source = _require_digest(
        source_manifest_component_sha256,
        field_name="source_manifest_component_sha256",
    )
    effective = _require_digest(
        effective_content_component_sha256,
        field_name="effective_content_component_sha256",
    )
    return b"".join(
        (
            _ZIP_COMMENT_PREFIX,
            version.to_bytes(4, "big"),
            source,
            effective,
        )
    )


def encode_source_snapshot_manifest(
    policy: SourceSnapshotPolicy,
    counts: SourceSnapshotCounts,
    galleries: Sequence[SourceSnapshotGallery],
    file_hash_decisions: Sequence[SourceSnapshotFileHashDecision],
    content_owners: Sequence[SourceSnapshotContentOwner],
    gid_winners: Sequence[SourceSnapshotGidWinner],
    *,
    codec_version: int = SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
) -> bytes:
    """Encode one complete, canonical retained source-snapshot manifest.

    Repeated sections accept any input permutation and are sorted by their
    protocol key.  Duplicate keys, incomplete owner/winner group coverage,
    unstable gallery references, aggregate mismatches, and a supplied spam
    answer that differs from the frozen predicate are rejected before bytes
    are emitted.
    """

    version = _require_registered_version(
        codec_version,
        registered=SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
        field_name="codec_version",
    )
    if type(policy) is not SourceSnapshotPolicy:
        raise ByteDomainError("policy must be SourceSnapshotPolicy")
    if type(counts) is not SourceSnapshotCounts:
        raise ByteDomainError("counts must be SourceSnapshotCounts")

    ordered_galleries = _canonical_source_snapshot_galleries(galleries)
    ordered_decisions = _canonical_source_snapshot_decisions(
        file_hash_decisions,
        policy=policy,
    )
    ordered_owners = _canonical_source_snapshot_owners(
        content_owners,
        galleries=ordered_galleries,
    )
    ordered_winners = _canonical_source_snapshot_winners(
        gid_winners,
        galleries=ordered_galleries,
    )
    _validate_source_snapshot_counts(counts, galleries=ordered_galleries)

    return b"".join(
        iter_source_snapshot_manifest_payload_ordered(
            policy,
            counts,
            ordered_galleries,
            ordered_decisions,
            ordered_owners,
            ordered_winners,
            codec_version=version,
        )
    )


def iter_source_snapshot_manifest_payload_ordered(
    policy: SourceSnapshotPolicy,
    counts: SourceSnapshotCounts,
    galleries: Sequence[SourceSnapshotGallery],
    file_hash_decisions: Sequence[SourceSnapshotFileHashDecision],
    content_owners: Sequence[SourceSnapshotContentOwner],
    gid_winners: Sequence[SourceSnapshotGidWinner],
    *,
    codec_version: int = SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
) -> Iterator[bytes]:
    """Yield one pre-ordered snapshot manifest with O(1) codec memory.

    Production supplies keyset-ordered rows plus the separately receipted
    owner/winner membership proof required by the formal contract.  The
    convenience encoder performs the full cross-relation validation and sort
    first; this iterator enforces local order/count/predicate invariants and
    never builds the final payload buffer.
    """

    return iter_source_snapshot_manifest_payload_rows_ordered(
        policy,
        counts,
        len(galleries),
        galleries,
        len(file_hash_decisions),
        file_hash_decisions,
        len(content_owners),
        content_owners,
        len(gid_winners),
        gid_winners,
        codec_version=codec_version,
    )


def iter_source_snapshot_manifest_payload_rows_ordered(
    policy: SourceSnapshotPolicy,
    counts: SourceSnapshotCounts,
    gallery_entry_count: int,
    galleries: Iterable[SourceSnapshotGallery],
    decision_entry_count: int,
    file_hash_decisions: Iterable[SourceSnapshotFileHashDecision],
    owner_entry_count: int,
    content_owners: Iterable[SourceSnapshotContentOwner],
    winner_entry_count: int,
    gid_winners: Iterable[SourceSnapshotGidWinner],
    *,
    codec_version: int = SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
) -> Iterator[bytes]:
    """Yield receipted, keyset-ordered snapshot rows with O(1) codec memory."""

    version = _require_registered_version(
        codec_version,
        registered=SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
        field_name="codec_version",
    )
    if (
        type(policy) is not SourceSnapshotPolicy
        or type(counts) is not SourceSnapshotCounts
    ):
        raise ByteDomainError("policy/counts have the wrong snapshot types")
    yield _SOURCE_SNAPSHOT_MANIFEST_PREFIX
    yield version.to_bytes(4, "big")
    yield policy.analysis_algorithm_version.to_bytes(4, "big")
    yield policy.spam_artist_threshold.to_bytes(8, "big")
    yield policy.spam_occurrence_threshold.to_bytes(8, "big")
    yield policy.content_owner_rule_version.to_bytes(4, "big")
    yield policy.gid_winner_rule_version.to_bytes(4, "big")
    yield counts.gallery_count.to_bytes(8, "big")
    yield counts.file_count.to_bytes(8, "big")
    yield counts.byte_count.to_bytes(8, "big")
    declared_gallery_count = _require_int63(
        gallery_entry_count, field_name="gallery_entry_count"
    )
    declared_decision_count = _require_int63(
        decision_entry_count, field_name="decision_entry_count"
    )
    declared_owner_count = _require_int63(
        owner_entry_count, field_name="owner_entry_count"
    )
    declared_winner_count = _require_int63(
        winner_entry_count, field_name="winner_entry_count"
    )
    yield declared_gallery_count.to_bytes(8, "big")
    previous_gallery_key: bytes | None = None
    actual_file_count = 0
    actual_byte_count = 0
    emitted_galleries = 0
    for gallery in galleries:
        if type(gallery) is not SourceSnapshotGallery:
            raise ByteDomainError(
                "gallery entries must be SourceSnapshotGallery instances"
            )
        if (
            previous_gallery_key is not None
            and gallery.gallery_key <= previous_gallery_key
        ):
            raise ByteDomainError("snapshot galleries are not strictly ordered")
        previous_gallery_key = gallery.gallery_key
        actual_file_count += gallery.file_count
        actual_byte_count += gallery.byte_count
        if actual_file_count > _INT63_MAX or actual_byte_count > _INT63_MAX:
            raise ByteDomainError("snapshot aggregate exceeds signed-int63")
        emitted_galleries += 1
        if emitted_galleries > declared_gallery_count:
            raise ByteDomainError("snapshot exceeds declared gallery_entry_count")
        yield gallery.gallery_key
        yield gallery.observation_identity_sha256
        yield b"\0" if gallery.content_sha256 is None else b"\1"
        if gallery.content_sha256 is not None:
            yield gallery.content_sha256
        yield gallery.gid.to_bytes(8, "big")
    if (
        emitted_galleries != declared_gallery_count
        or emitted_galleries != counts.gallery_count
        or actual_file_count != counts.file_count
        or actual_byte_count != counts.byte_count
    ):
        raise ByteDomainError("snapshot aggregate counts do not match galleries")
    yield declared_decision_count.to_bytes(8, "big")
    previous_file_sha: bytes | None = None
    emitted_decisions = 0
    for decision in file_hash_decisions:
        if type(decision) is not SourceSnapshotFileHashDecision:
            raise ByteDomainError(
                "file-hash decisions must be SourceSnapshotFileHashDecision instances"
            )
        if previous_file_sha is not None and decision.file_sha256 <= previous_file_sha:
            raise ByteDomainError("snapshot decisions are not strictly ordered")
        previous_file_sha = decision.file_sha256
        expected_excluded = (
            decision.occurrence_count >= policy.spam_occurrence_threshold
            and decision.maximum_gallery_artist_count > 0
            and decision.artist_count
            > policy.spam_artist_threshold * decision.maximum_gallery_artist_count
        )
        if decision.excluded_flag is not expected_excluded:
            raise ByteDomainError(
                "excluded_flag does not match the frozen unbounded-integer predicate"
            )
        emitted_decisions += 1
        if emitted_decisions > declared_decision_count:
            raise ByteDomainError("snapshot exceeds declared decision_entry_count")
        yield decision.file_sha256
        yield decision.occurrence_count.to_bytes(8, "big")
        yield decision.artist_count.to_bytes(8, "big")
        yield decision.maximum_gallery_artist_count.to_bytes(8, "big")
        yield bytes((1 if decision.excluded_flag else 0,))
    if emitted_decisions != declared_decision_count:
        raise ByteDomainError("snapshot decision_entry_count is incomplete")
    yield declared_owner_count.to_bytes(8, "big")
    previous_content_sha: bytes | None = None
    emitted_owners = 0
    for owner in content_owners:
        if type(owner) is not SourceSnapshotContentOwner:
            raise ByteDomainError(
                "content owners must be SourceSnapshotContentOwner instances"
            )
        if (
            previous_content_sha is not None
            and owner.content_sha256 <= previous_content_sha
        ):
            raise ByteDomainError("snapshot content owners are not strictly ordered")
        previous_content_sha = owner.content_sha256
        emitted_owners += 1
        if emitted_owners > declared_owner_count:
            raise ByteDomainError("snapshot exceeds declared owner_entry_count")
        yield owner.content_sha256
        yield owner.owner_gallery_key
    if emitted_owners != declared_owner_count:
        raise ByteDomainError("snapshot owner_entry_count is incomplete")
    yield declared_winner_count.to_bytes(8, "big")
    previous_gid: int | None = None
    emitted_winners = 0
    for winner in gid_winners:
        if type(winner) is not SourceSnapshotGidWinner:
            raise ByteDomainError("GID winners must be SourceSnapshotGidWinner")
        if previous_gid is not None and winner.gid <= previous_gid:
            raise ByteDomainError("snapshot GID winners are not strictly ordered")
        previous_gid = winner.gid
        emitted_winners += 1
        if emitted_winners > declared_winner_count:
            raise ByteDomainError("snapshot exceeds declared winner_entry_count")
        yield winner.gid.to_bytes(8, "big")
        yield winner.winner_gallery_key
    if emitted_winners != declared_winner_count:
        raise ByteDomainError("snapshot winner_entry_count is incomplete")


def source_snapshot_manifest_digest_ordered(
    policy: SourceSnapshotPolicy,
    counts: SourceSnapshotCounts,
    gallery_entry_count: int,
    galleries: Iterable[SourceSnapshotGallery],
    decision_entry_count: int,
    file_hash_decisions: Iterable[SourceSnapshotFileHashDecision],
    owner_entry_count: int,
    content_owners: Iterable[SourceSnapshotContentOwner],
    winner_entry_count: int,
    gid_winners: Iterable[SourceSnapshotGidWinner],
    *,
    payload_byte_count: int,
    codec_version: int = SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
) -> bytes:
    """Hash an independently receipted ordered snapshot stream in one replay."""

    return canonical_value_digest_parts(
        SOURCE_SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
        _require_int63(payload_byte_count, field_name="payload_byte_count"),
        iter_source_snapshot_manifest_payload_rows_ordered(
            policy,
            counts,
            gallery_entry_count,
            galleries,
            decision_entry_count,
            file_hash_decisions,
            owner_entry_count,
            content_owners,
            winner_entry_count,
            gid_winners,
            codec_version=codec_version,
        ),
    )


def source_snapshot_manifest_digest(
    policy: SourceSnapshotPolicy,
    counts: SourceSnapshotCounts,
    galleries: Sequence[SourceSnapshotGallery],
    file_hash_decisions: Sequence[SourceSnapshotFileHashDecision],
    content_owners: Sequence[SourceSnapshotContentOwner],
    gid_winners: Sequence[SourceSnapshotGidWinner],
    *,
    codec_version: int = SOURCE_SNAPSHOT_MANIFEST_CODEC_VERSION,
) -> bytes:
    """Return the canonical ``source_snapshot_manifest_v1`` identity."""

    ordered_galleries = _canonical_source_snapshot_galleries(galleries)
    ordered_decisions = _canonical_source_snapshot_decisions(
        file_hash_decisions, policy=policy
    )
    ordered_owners = _canonical_source_snapshot_owners(
        content_owners, galleries=ordered_galleries
    )
    ordered_winners = _canonical_source_snapshot_winners(
        gid_winners, galleries=ordered_galleries
    )
    _validate_source_snapshot_counts(counts, galleries=ordered_galleries)
    payload_byte_count = (
        len(_SOURCE_SNAPSHOT_MANIFEST_PREFIX)
        + 88
        + 73 * len(ordered_galleries)
        + 32 * sum(gallery.content_sha256 is not None for gallery in ordered_galleries)
        + 57 * len(ordered_decisions)
        + 64 * len(ordered_owners)
        + 40 * len(ordered_winners)
    )
    return source_snapshot_manifest_digest_ordered(
        policy,
        counts,
        len(ordered_galleries),
        ordered_galleries,
        len(ordered_decisions),
        ordered_decisions,
        len(ordered_owners),
        ordered_owners,
        len(ordered_winners),
        ordered_winners,
        payload_byte_count=payload_byte_count,
        codec_version=codec_version,
    )


def gallery_key(
    scope_key: bytes,
    locator_sha256: bytes,
    *,
    algorithm_version: int = GALLERY_KEY_ALGORITHM_VERSION,
) -> bytes:
    """Return the scope-local identity for one canonical full locator."""

    validated_version = _require_registered_version(
        algorithm_version,
        registered=GALLERY_KEY_ALGORITHM_VERSION,
        field_name="algorithm_version",
    )
    validated_scope_key = _require_fixed_bytes(
        scope_key, length=32, field_name="scope_key"
    )
    validated_locator_sha256 = _require_fixed_bytes(
        locator_sha256, length=32, field_name="locator_sha256"
    )

    digest = sha256(b"h2hdb-vnext-gallery-key\0")
    digest.update(validated_version.to_bytes(4, "big"))
    digest.update(validated_scope_key)
    digest.update(validated_locator_sha256)
    return digest.digest()


def source_scope_key(
    source_provider: str,
    source_root_sha256: bytes,
    identity_policy_version: int,
    *,
    algorithm_version: int = SOURCE_SCOPE_KEY_ALGORITHM_VERSION,
) -> bytes:
    """Derive the exact source-scope identity from all semantic inputs."""

    version = _require_registered_version(
        algorithm_version,
        registered=SOURCE_SCOPE_KEY_ALGORITHM_VERSION,
        field_name="algorithm_version",
    )
    provider = validate_registered_ascii_identifier(
        source_provider,
        allowed=SOURCE_PROVIDERS,
        field_name="source_provider",
        maximum_bytes=64,
    )
    root = _require_digest(source_root_sha256, field_name="source_root_sha256")
    identity_version = _require_positive_uint(
        identity_policy_version,
        bits=32,
        field_name="identity_policy_version",
    )

    digest = sha256(b"h2hdb-vnext-source-scope-key\0")
    digest.update(version.to_bytes(4, "big"))
    digest.update(len(provider).to_bytes(4, "big"))
    digest.update(provider)
    digest.update(root)
    digest.update(identity_version.to_bytes(4, "big"))
    return digest.digest()


def file_key(
    name_bytes: bytes,
    *,
    algorithm_version: int = FILE_IDENTITY_ALGORITHM_VERSION,
) -> bytes:
    """Derive a versioned collision-checked identity for one exact dirent."""

    version = _require_registered_version(
        algorithm_version,
        registered=FILE_IDENTITY_ALGORITHM_VERSION,
        field_name="algorithm_version",
    )
    name = validate_file_name(name_bytes)
    digest = sha256(b"h2hdb-vnext-file-key\0")
    digest.update(version.to_bytes(4, "big"))
    digest.update(len(name).to_bytes(4, "big"))
    digest.update(name)
    return digest.digest()


def file_role(
    name_bytes: bytes,
    *,
    classifier_version: int = FILE_ROLE_CLASSIFIER_VERSION,
) -> bytes:
    """Classify the exact metadata filename under a pinned protocol version."""

    _require_registered_version(
        classifier_version,
        registered=FILE_ROLE_CLASSIFIER_VERSION,
        field_name="classifier_version",
    )
    name = validate_file_name(name_bytes)
    return METADATA_FILE_ROLE if name == METADATA_FILE_NAME else CONTENT_FILE_ROLE


def encode_artifact_member_plan(
    entries: Sequence[ArtifactMemberPlanEntry],
    *,
    plan_version: int = ARTIFACT_MEMBER_PLAN_VERSION,
) -> bytes:
    """Materialize the reference source-only member-plan v1 payload."""

    version = _require_registered_version(
        plan_version,
        registered=ARTIFACT_MEMBER_PLAN_VERSION,
        field_name="plan_version",
    )
    validated_entries = _validate_artifact_member_plan_entries(entries)

    return b"".join(
        iter_artifact_member_plan_payload(
            len(validated_entries), validated_entries, plan_version=version
        )
    )


def iter_artifact_member_plan_payload(
    entry_count: int,
    entries: Iterable[ArtifactMemberPlanEntry],
    *,
    plan_version: int = ARTIFACT_MEMBER_PLAN_VERSION,
) -> Iterator[bytes]:
    """Yield an already-positioned member plan with bounded codec memory."""

    version = _require_registered_version(
        plan_version,
        registered=ARTIFACT_MEMBER_PLAN_VERSION,
        field_name="plan_version",
    )
    count = _require_int63(entry_count, field_name="entry_count")
    yield _ARTIFACT_MEMBER_PLAN_PREFIX
    yield version.to_bytes(4, "big")
    yield count.to_bytes(8, "big")
    emitted = 0
    for entry in entries:
        if type(entry) is not ArtifactMemberPlanEntry:
            raise ByteDomainError(
                "artifact member plan entries must be ArtifactMemberPlanEntry"
            )
        if emitted >= count:
            raise ByteDomainError("artifact member plan exceeds declared entry_count")
        if entry.entry_position != emitted:
            raise ByteDomainError(
                "entry_position must equal its zero-based contiguous array index"
            )
        if entry.entry_kind is not ArtifactMemberEntryKind.SOURCE_FILE:
            raise ByteDomainError("v1 admits only SOURCE_FILE member-plan entries")
        if entry.source_role is not _source_role_for_name(entry.source_name_bytes):
            raise ByteDomainError(
                "source_role does not match the exact source filename"
            )
        if entry.transform_kind is not _transform_kind_for_name(
            entry.source_name_bytes
        ):
            raise ByteDomainError(
                "transform_kind does not match the exact ASCII-casefolded suffix"
            )
        archive_name = entry.archive_member_name_bytes
        _validate_archive_member_presence(
            excluded_flag=entry.excluded_flag,
            archive_member_name_bytes=archive_name,
        )
        if archive_name != artifact_archive_member_name(
            entry.entry_position,
            entry.source_role,
            entry.transform_kind,
            entry.excluded_flag,
        ):
            raise ByteDomainError(
                "archive member name does not match position/role/transform"
            )
        # Cross-entry archive-name uniqueness is proved by the ordered source
        # projection/DB constraint before this O(1)-memory framing iterator.
        yield entry.entry_position.to_bytes(8, "big")
        yield bytes((int(entry.entry_kind),))
        yield len(entry.source_name_bytes).to_bytes(4, "big")
        yield entry.source_name_bytes
        yield entry.source_file_sha256
        yield entry.source_size_bytes.to_bytes(8, "big")
        yield bytes((int(entry.source_role), 1 if entry.excluded_flag else 0))
        if archive_name is None:
            yield b"\0"
        else:
            yield b"\1"
            yield len(archive_name).to_bytes(4, "big")
            yield archive_name
        yield bytes((int(entry.transform_kind),))
        emitted += 1
    if emitted != count:
        raise ByteDomainError(
            "artifact member plan does not equal declared entry_count"
        )


def decode_artifact_member_plan(payload: bytes) -> tuple[ArtifactMemberPlanEntry, ...]:
    """Decode and fully revalidate one canonical member-plan v1 payload."""

    encoded = _require_bytes(payload, field_name="artifact member-plan payload")
    if not encoded.startswith(_ARTIFACT_MEMBER_PLAN_PREFIX):
        raise ByteDomainError("artifact member-plan prefix is missing or invalid")
    offset = len(_ARTIFACT_MEMBER_PLAN_PREFIX)

    def take(size: int, *, field_name: str) -> bytes:
        nonlocal offset
        if len(encoded) - offset < size:
            raise ByteDomainError(f"{field_name} is truncated")
        value = encoded[offset : offset + size]
        offset += size
        return value

    def take_uint(size: int, *, field_name: str) -> int:
        return int.from_bytes(take(size, field_name=field_name), "big")

    version = take_uint(4, field_name="plan_version")
    _require_registered_version(
        version,
        registered=ARTIFACT_MEMBER_PLAN_VERSION,
        field_name="plan_version",
    )
    entry_count = take_uint(8, field_name="entry_count")
    # Every entry has at least 58 bytes (one-byte source and no archive name).
    # Reject impossible counts before iterating over attacker-controlled u64s.
    if entry_count > (len(encoded) - offset) // 58:
        raise ByteDomainError("artifact member-plan entries are truncated")

    entries: list[ArtifactMemberPlanEntry] = []
    for expected_position in range(entry_count):
        entry_position = take_uint(8, field_name="entry_position")
        if entry_position != expected_position:
            raise ByteDomainError(
                "entry_position must equal its zero-based contiguous array index"
            )

        entry_kind_tag = take_uint(1, field_name="entry_kind")
        if entry_kind_tag != int(ArtifactMemberEntryKind.SOURCE_FILE):
            raise ByteDomainError(f"entry_kind tag {entry_kind_tag} is not registered")

        source_name_length = take_uint(4, field_name="source_name_length")
        source_name_bytes = take(
            source_name_length,
            field_name="source_name_bytes",
        )
        source_file_sha256 = take(32, field_name="source_file_sha256")
        source_size_bytes = take_uint(8, field_name="source_size_bytes")

        source_role_tag = take_uint(1, field_name="source_role")
        if source_role_tag not in {
            int(ArtifactSourceRole.METADATA),
            int(ArtifactSourceRole.CONTENT),
        }:
            raise ByteDomainError(
                f"source_role tag {source_role_tag} is not registered"
            )

        excluded_tag = take_uint(1, field_name="excluded_flag")
        if excluded_tag not in {0, 1}:
            raise ByteDomainError(
                f"excluded_flag tag {excluded_tag} is not exactly zero or one"
            )
        excluded_flag = bool(excluded_tag)

        archive_presence = take_uint(1, field_name="archive_name_presence")
        if archive_presence not in {0, 1}:
            raise ByteDomainError(
                "archive_name_presence tag must be exactly zero or one"
            )
        archive_member_name_bytes: bytes | None = None
        if archive_presence == 1:
            archive_name_length = take_uint(
                4,
                field_name="archive_name_length",
            )
            archive_member_name_bytes = take(
                archive_name_length,
                field_name="archive_member_name_bytes",
            )

        transform_kind_tag = take_uint(1, field_name="transform_kind")
        if transform_kind_tag not in {
            int(ArtifactTransformKind.RAW_COPY),
            int(ArtifactTransformKind.GIF_NORMALIZE),
            int(ArtifactTransformKind.JPEG_NORMALIZE),
        }:
            raise ByteDomainError(
                f"transform_kind tag {transform_kind_tag} is not registered"
            )

        entry = ArtifactMemberPlanEntry(
            entry_position=entry_position,
            source_name_bytes=source_name_bytes,
            source_file_sha256=source_file_sha256,
            source_size_bytes=source_size_bytes,
            excluded_flag=excluded_flag,
        )
        if int(entry.source_role) != source_role_tag:
            raise ByteDomainError(
                "source_role tag does not match the exact source filename"
            )
        if int(entry.transform_kind) != transform_kind_tag:
            raise ByteDomainError(
                "transform_kind tag does not match the exact ASCII-casefolded suffix"
            )
        if entry.archive_member_name_bytes != archive_member_name_bytes:
            raise ByteDomainError(
                "archive member name does not match position/role/transform"
            )
        entries.append(entry)

    if offset != len(encoded):
        raise ByteDomainError("artifact member-plan payload contains trailing bytes")
    return _validate_artifact_member_plan_entries(entries)


def artifact_member_plan_digest(
    entries: Sequence[ArtifactMemberPlanEntry],
    *,
    plan_version: int = ARTIFACT_MEMBER_PLAN_VERSION,
) -> bytes:
    """Return the reference ``artifact_member_plan_v1`` component digest."""

    validated = _validate_artifact_member_plan_entries(entries)
    payload_byte_count = len(_ARTIFACT_MEMBER_PLAN_PREFIX) + 12
    for entry in validated:
        payload_byte_count += 57 + len(entry.source_name_bytes)
        if entry.archive_member_name_bytes is not None:
            payload_byte_count += 4 + len(entry.archive_member_name_bytes)
    return artifact_member_plan_digest_ordered(
        len(validated),
        payload_byte_count,
        validated,
        plan_version=plan_version,
    )


def artifact_member_plan_digest_ordered(
    entry_count: int,
    payload_byte_count: int,
    entries: Iterable[ArtifactMemberPlanEntry],
    *,
    plan_version: int = ARTIFACT_MEMBER_PLAN_VERSION,
) -> bytes:
    """Hash a preflight-receipted plan replay without a full payload buffer."""

    count = _require_int63(entry_count, field_name="entry_count")
    declared_bytes = _require_int63(
        payload_byte_count,
        field_name="payload_byte_count",
    )
    return canonical_value_digest_parts(
        ARTIFACT_MEMBER_PLAN_DIGEST_DOMAIN,
        declared_bytes,
        iter_artifact_member_plan_payload(
            count,
            entries,
            plan_version=plan_version,
        ),
    )


def gallery_key_hex(
    scope_key: bytes,
    locator_sha256: bytes,
    *,
    algorithm_version: int = GALLERY_KEY_ALGORITHM_VERSION,
) -> str:
    """Return the API-only lowercase hex form of :func:`gallery_key`."""

    return digest_to_hex(
        gallery_key(
            scope_key,
            locator_sha256,
            algorithm_version=algorithm_version,
        )
    )


def validate_gallery_name(value: str) -> bytes:
    """Validate and encode one direct-child gallery name (at most 255 bytes)."""

    return _validate_utf8_leaf(value, field_name="gallery_name", maximum_bytes=255)


def validate_file_name(value: bytes) -> bytes:
    """Validate one exact opaque POSIX direct-child dirent.

    POSIX reserves NUL and slash, but a backslash is an ordinary filename byte.
    The filesystem adapter owns the ``os.fsencode``/``surrogateescape`` round
    trip; this byte-domain validator deliberately performs no text decoding.
    """

    return _validate_posix_leaf_bytes(
        value,
        field_name="name_bytes",
        maximum_bytes=255,
    )


def encode_filesystem_stat_fingerprint(
    *,
    device: int,
    inode: int,
    size_bytes: int,
    modified_ns: int,
    changed_ns: int,
) -> bytes:
    """Encode the exact fixed-width filesystem discovery fingerprint.

    This 40-byte value is an audit/change-detection hint only.  It never
    authorizes observation reuse, which requires the sealed observation tree.
    """

    return b"".join(
        (
            _require_uint(device, bits=64, field_name="device").to_bytes(8, "big"),
            _require_uint(inode, bits=64, field_name="inode").to_bytes(8, "big"),
            _require_int63(size_bytes, field_name="size_bytes").to_bytes(8, "big"),
            _require_int64(modified_ns, field_name="modified_ns").to_bytes(
                8, "big", signed=True
            ),
            _require_int64(changed_ns, field_name="changed_ns").to_bytes(
                8, "big", signed=True
            ),
        )
    )


def decode_filesystem_stat_fingerprint(
    value: bytes,
) -> tuple[int, int, int, int, int]:
    """Decode and validate one exact 40-byte discovery fingerprint."""

    encoded = _require_bytes(value, field_name="metadata_fingerprint")
    if len(encoded) != FILESYSTEM_STAT_FINGERPRINT_BYTES:
        raise ByteDomainError("metadata_fingerprint must be exactly 40 bytes")
    device = int.from_bytes(encoded[0:8], "big")
    inode = int.from_bytes(encoded[8:16], "big")
    size_bytes = int.from_bytes(encoded[16:24], "big")
    modified_ns = int.from_bytes(encoded[24:32], "big", signed=True)
    changed_ns = int.from_bytes(encoded[32:40], "big", signed=True)
    _require_int63(size_bytes, field_name="size_bytes")
    return device, inode, size_bytes, modified_ns, changed_ns


def encode_source_relative_locator(
    components: Sequence[str],
    *,
    codec_version: int = SOURCE_LOCATOR_CODEC_VERSION,
) -> bytes:
    """Encode one nested root-relative gallery directory without separators.

    The canonical payload is ``u32be(version) || u32be(component_count)``
    followed by ``u32be(byte_length) || exact_utf8`` for every component.
    Component boundaries therefore never depend on a platform path separator.
    """

    return b"".join(
        iter_source_relative_locator_payload(
            components,
            codec_version=codec_version,
        )
    )


def iter_source_relative_locator_payload(
    components: Sequence[str],
    *,
    codec_version: int = SOURCE_LOCATOR_CODEC_VERSION,
) -> Iterator[bytes]:
    """Yield the exact locator frame in bounded segment-sized pieces."""

    version = _require_registered_version(
        codec_version,
        registered=SOURCE_LOCATOR_CODEC_VERSION,
        field_name="codec_version",
    )
    if not components:
        raise ByteDomainError("source relative locator must contain a component")
    if len(components) > (1 << 32) - 1:
        raise ByteDomainError("source relative locator has too many components")

    yield version.to_bytes(4, "big")
    yield len(components).to_bytes(4, "big")
    for component in components:
        encoded = _validate_utf8_leaf(
            component,
            field_name="source locator component",
            maximum_bytes=255,
        )
        yield len(encoded).to_bytes(4, "big")
        yield encoded


def decode_source_relative_locator(payload: bytes) -> tuple[str, ...]:
    """Materialize one small decoded locator (convenience/oracle API)."""

    _receipt, components = _consume_source_relative_locator_parts(
        (payload,), collect_components=True
    )
    return components


def validate_source_relative_locator_parts(
    parts: Iterable[bytes],
) -> SourceRelativeLocatorValidationReceipt:
    """Consume arbitrary bounded parts and prove exact canonical EOF.

    Production readers first obtain this receipt from collision-checked,
    immutable canonical pages.  Any later streaming consumer is provisional
    until replay of the same sealed page root completes; this function never
    exposes a component before the complete frame has been validated.
    """

    receipt, _components = _consume_source_relative_locator_parts(
        parts, collect_components=False
    )
    return receipt


def _consume_source_relative_locator_parts(
    parts: Iterable[bytes], *, collect_components: bool
) -> tuple[SourceRelativeLocatorValidationReceipt, tuple[str, ...]]:
    phase = "VERSION"
    carry = bytearray()
    component_count = 0
    emitted = 0
    segment_size = 0
    components: list[str] = []
    digest = sha256()
    consumed = 0
    for part in parts:
        exact = _require_bytes(part, field_name="source locator part")
        consumed += len(exact)
        if consumed > _INT63_MAX:
            raise ByteDomainError("source locator exceeds signed-int63 bytes")
        digest.update(exact)
        offset = 0
        while offset < len(exact):
            if phase == "DONE":
                raise ByteDomainError("source locator payload contains trailing bytes")
            required = segment_size if phase == "SEGMENT" else 4
            amount = min(required - len(carry), len(exact) - offset)
            carry.extend(exact[offset : offset + amount])
            offset += amount
            if len(carry) != required:
                continue
            value = bytes(carry)
            carry.clear()
            if phase == "VERSION":
                version = int.from_bytes(value, "big")
                if version != SOURCE_LOCATOR_CODEC_VERSION:
                    raise IntegerDomainError(
                        f"codec_version {version} is not registered"
                    )
                phase = "COUNT"
            elif phase == "COUNT":
                component_count = int.from_bytes(value, "big")
                if component_count == 0:
                    raise ByteDomainError(
                        "source relative locator must contain a component"
                    )
                phase = "LENGTH"
            elif phase == "LENGTH":
                segment_size = int.from_bytes(value, "big")
                if not 1 <= segment_size <= 255:
                    raise ByteDomainError(
                        "source locator component length must be in [1, 255]"
                    )
                phase = "SEGMENT"
            else:
                try:
                    component = value.decode("utf-8", errors="strict")
                except UnicodeDecodeError as error:
                    raise ByteDomainError(
                        "source locator component must be exact UTF-8"
                    ) from error
                validated = _validate_utf8_leaf(
                    component,
                    field_name="source locator component",
                    maximum_bytes=255,
                )
                if validated != value:  # pragma: no cover - strict UTF-8 is unique
                    raise ByteDomainError(
                        "source locator component is not canonical UTF-8"
                    )
                if collect_components:
                    components.append(component)
                emitted += 1
                phase = "DONE" if emitted == component_count else "LENGTH"
    if phase != "DONE" or carry:
        raise ByteDomainError("source locator payload is truncated")
    receipt = SourceRelativeLocatorValidationReceipt(
        component_count, consumed, digest.digest()
    )
    return receipt, tuple(components)


def source_relative_locator_digest(
    digest_domain: str,
    components: Sequence[str],
) -> bytes:
    """Stream the canonical identity digest for a nested gallery locator."""

    byte_count = sum(
        len(part) for part in iter_source_relative_locator_payload(components)
    )
    return canonical_value_digest_parts(
        digest_domain,
        byte_count,
        iter_source_relative_locator_payload(components),
    )


def artifact_locator_components(artifact_sha256: bytes) -> tuple[str, str, str]:
    """Derive the only registered managed-filesystem locator from artifact bytes."""

    digest = _require_digest(artifact_sha256, field_name="artifact_sha256")
    lowerhex = digest.hex()
    return ("sha256", lowerhex[:2], f"{lowerhex}.cbz")


def encode_artifact_locator(
    components: Sequence[str],
    *,
    codec_version: int = ARTIFACT_LOCATOR_CODEC_VERSION,
) -> bytes:
    """Materialize one small artifact locator for APIs, tests, and decoding.

    Production canonical-value writers must use
    :func:`iter_artifact_locator_payload`; this convenience helper deliberately
    returns one bytes object and therefore is not a bounded-memory write path.
    """

    return b"".join(
        iter_artifact_locator_payload(components, codec_version=codec_version)
    )


def iter_artifact_locator_payload(
    components: Sequence[str],
    *,
    codec_version: int = ARTIFACT_LOCATOR_CODEC_VERSION,
) -> Iterator[bytes]:
    """Validate the complete bounded locator, then yield its exact pieces."""

    yield from _artifact_locator_payload_parts(components, codec_version=codec_version)


def _artifact_locator_payload_parts(
    components: Sequence[str],
    *,
    codec_version: int,
) -> tuple[bytes, ...]:

    version = _require_registered_version(
        codec_version,
        registered=ARTIFACT_LOCATOR_CODEC_VERSION,
        field_name="artifact locator codec_version",
    )
    if isinstance(components, (str, bytes, bytearray)):
        raise ByteDomainError("artifact locator components must be a segment sequence")
    if not components:
        raise ByteDomainError("artifact locator must contain a component")
    if len(components) > _UINT32_MAX:
        raise ByteDomainError("artifact locator has too many components")
    if 8 + 5 * len(components) > ARTIFACT_LOCATOR_MAXIMUM_BYTES:
        raise ByteDomainError(
            f"artifact locator exceeds {ARTIFACT_LOCATOR_MAXIMUM_BYTES} bytes"
        )
    output = [version.to_bytes(4, "big"), len(components).to_bytes(4, "big")]
    byte_count = 8
    for component in components:
        encoded = _validate_utf8_leaf(
            component,
            field_name="artifact locator component",
            maximum_bytes=255,
        )
        byte_count += 4 + len(encoded)
        if byte_count > ARTIFACT_LOCATOR_MAXIMUM_BYTES:
            raise ByteDomainError(
                f"artifact locator exceeds {ARTIFACT_LOCATOR_MAXIMUM_BYTES} bytes"
            )
        output.extend((len(encoded).to_bytes(4, "big"), encoded))
    return tuple(output)


def decode_artifact_locator(payload: bytes) -> tuple[str, ...]:
    """Materialize one decoded locator (convenience/oracle API)."""

    return tuple(iter_decode_artifact_locator((payload,)))


def artifact_storage_receipt_id(
    candidate_id: bytes,
    publication_key_value: bytes,
    artifact_sha256: bytes,
    artifact_locator_sha256: bytes,
    storage_generation: int,
    size_bytes: int,
) -> bytes:
    """Derive the fixed 16-byte storage receipt bound into protection v1."""

    candidate = _require_bytes(candidate_id, field_name="candidate_id")
    if len(candidate) != 16:
        raise ByteDomainError("candidate_id must contain exactly 16 bytes")
    publication = _require_digest(
        publication_key_value,
        field_name="publication_key",
    )
    artifact = _require_digest(artifact_sha256, field_name="artifact_sha256")
    locator = _require_digest(
        artifact_locator_sha256,
        field_name="artifact_locator_sha256",
    )
    generation = _require_int63(
        storage_generation,
        field_name="storage_generation",
    )
    size = _require_int63(size_bytes, field_name="size_bytes")
    return sha256(
        b"".join(
            (
                _ARTIFACT_STORAGE_RECEIPT_PREFIX,
                candidate,
                publication,
                artifact,
                locator,
                generation.to_bytes(8, "big"),
                size.to_bytes(8, "big"),
            )
        )
    ).digest()[:16]


def encode_artifact_protection_token(
    storage_codec_version: int,
    candidate_id: bytes,
    publication_key_value: bytes,
    artifact_sha256: bytes,
    artifact_locator_sha256: bytes,
    storage_generation: int,
    size_bytes: int,
    *,
    codec_version: int = ARTIFACT_PROTECTION_TOKEN_CODEC_VERSION,
) -> bytes:
    """Encode one exact 184-byte protection token; no caller receipt is accepted."""

    receipt = artifact_storage_receipt_id(
        candidate_id,
        publication_key_value,
        artifact_sha256,
        artifact_locator_sha256,
        storage_generation,
        size_bytes,
    )
    token = ArtifactProtectionToken(
        codec_version=codec_version,
        storage_codec_version=storage_codec_version,
        candidate_id=candidate_id,
        publication_key=publication_key_value,
        artifact_sha256=artifact_sha256,
        artifact_locator_sha256=artifact_locator_sha256,
        receipt_id=receipt,
        storage_generation=storage_generation,
        size_bytes=size_bytes,
    )
    return b"".join(
        (
            _ARTIFACT_PROTECTION_PREFIX,
            token.codec_version.to_bytes(4, "big"),
            token.storage_codec_version.to_bytes(4, "big"),
            token.candidate_id,
            token.publication_key,
            token.artifact_sha256,
            token.artifact_locator_sha256,
            token.receipt_id,
            token.storage_generation.to_bytes(8, "big"),
            token.size_bytes.to_bytes(8, "big"),
        )
    )


def decode_artifact_protection_token(payload: bytes) -> ArtifactProtectionToken:
    """Decode and revalidate one protection token with exact 184-byte EOF."""

    exact = _require_bytes(payload, field_name="artifact protection token")
    if len(exact) != 184:
        raise ByteDomainError(
            "artifact protection token must contain exactly 184 bytes"
        )
    prefix_length = len(_ARTIFACT_PROTECTION_PREFIX)
    if prefix_length != 32:  # pragma: no cover - fixed module constant
        raise AssertionError("artifact protection prefix must contain 32 bytes")
    if exact[:prefix_length] != _ARTIFACT_PROTECTION_PREFIX:
        raise ByteDomainError("artifact protection token prefix is invalid")
    offset = prefix_length

    def take(size: int) -> bytes:
        nonlocal offset
        value = exact[offset : offset + size]
        offset += size
        return value

    token = ArtifactProtectionToken(
        codec_version=int.from_bytes(take(4), "big"),
        storage_codec_version=int.from_bytes(take(4), "big"),
        candidate_id=take(16),
        publication_key=take(32),
        artifact_sha256=take(32),
        artifact_locator_sha256=take(32),
        receipt_id=take(16),
        storage_generation=int.from_bytes(take(8), "big"),
        size_bytes=int.from_bytes(take(8), "big"),
    )
    if offset != len(exact):  # pragma: no cover - fixed frame arithmetic
        raise ByteDomainError("artifact protection token has trailing bytes")
    return token


def iter_decode_artifact_locator(parts: Iterable[bytes]) -> Iterator[str]:
    """Validate exact EOF within the v1 cap before exposing any component."""

    payload = bytearray()
    for part in parts:
        exact = _require_bytes(part, field_name="artifact locator part")
        if len(payload) + len(exact) > ARTIFACT_LOCATOR_MAXIMUM_BYTES:
            raise ByteDomainError(
                f"artifact locator exceeds {ARTIFACT_LOCATOR_MAXIMUM_BYTES} bytes"
            )
        payload.extend(exact)
    yield from _decode_artifact_locator_payload(bytes(payload))


def _decode_artifact_locator_payload(payload: bytes) -> tuple[str, ...]:
    """Decode one fully buffered, bounded locator after exact EOF is known."""

    phase = "VERSION"
    carry = bytearray()
    component_count = 0
    emitted = 0
    segment_size = 0
    components: list[str] = []
    for part in (payload,):
        exact = _require_bytes(part, field_name="artifact locator part")
        offset = 0
        while offset < len(exact):
            if phase == "DONE":
                raise ByteDomainError(
                    "artifact locator payload contains trailing bytes"
                )
            required = segment_size if phase == "SEGMENT" else 4
            amount = min(required - len(carry), len(exact) - offset)
            carry.extend(exact[offset : offset + amount])
            offset += amount
            if len(carry) != required:
                continue
            value = bytes(carry)
            carry.clear()
            if phase == "VERSION":
                version = int.from_bytes(value, "big")
                if version != ARTIFACT_LOCATOR_CODEC_VERSION:
                    raise IntegerDomainError(
                        f"artifact locator codec_version {version} is not registered"
                    )
                phase = "COUNT"
            elif phase == "COUNT":
                component_count = int.from_bytes(value, "big")
                if component_count == 0:
                    raise ByteDomainError("artifact locator must contain a component")
                phase = "LENGTH"
            elif phase == "LENGTH":
                segment_size = int.from_bytes(value, "big")
                if not 1 <= segment_size <= 255:
                    raise ByteDomainError(
                        "artifact locator component length must be in [1, 255]"
                    )
                phase = "SEGMENT"
            else:
                try:
                    component = value.decode("utf-8", errors="strict")
                except UnicodeDecodeError as error:
                    raise ByteDomainError(
                        "artifact locator component must be exact UTF-8"
                    ) from error
                validated = _validate_utf8_leaf(
                    component,
                    field_name="artifact locator component",
                    maximum_bytes=255,
                )
                if validated != value:  # pragma: no cover - strict UTF-8 is unique
                    raise ByteDomainError(
                        "artifact locator component is not canonical UTF-8"
                    )
                emitted += 1
                phase = "DONE" if emitted == component_count else "LENGTH"
                components.append(component)
    if phase != "DONE" or carry:
        raise ByteDomainError("artifact locator payload is truncated")
    return tuple(components)


def artifact_locator_digest(components: Sequence[str]) -> bytes:
    """Stream the canonical digest of exact artifact storage locator bytes."""

    parts = _artifact_locator_payload_parts(
        components, codec_version=ARTIFACT_LOCATOR_CODEC_VERSION
    )
    return canonical_value_digest_parts(
        ARTIFACT_LOCATOR_DIGEST_DOMAIN,
        sum(len(part) for part in parts),
        parts,
    )


def catalog_summary_digest(payload: bytes) -> bytes:
    """Digest one already-materialized summary (convenience/oracle API)."""

    encoded = _require_bytes(payload, field_name="catalog summary")
    return catalog_summary_digest_parts(len(encoded), (encoded,))


def catalog_summary_digest_parts(
    declared_byte_count: int, parts: Iterable[bytes]
) -> bytes:
    """Stream exact strict-UTF-8 summary bytes into their canonical digest."""

    return _canonical_utf8_digest_parts(
        CATALOG_SUMMARY_DIGEST_DOMAIN,
        declared_byte_count,
        parts,
        field_name="catalog summary",
        require_nonempty=False,
    )


def catalog_language_digest(payload: bytes) -> bytes:
    """Digest one materialized language value (convenience/oracle API)."""

    encoded = _require_bytes(payload, field_name="catalog language")
    return catalog_language_digest_parts(len(encoded), (encoded,))


def catalog_language_digest_parts(
    declared_byte_count: int, parts: Iterable[bytes]
) -> bytes:
    """Stream one nonempty exact UTF-8 language value into its digest."""

    return _canonical_utf8_digest_parts(
        CATALOG_LANGUAGE_DIGEST_DOMAIN,
        declared_byte_count,
        parts,
        field_name="catalog language",
        require_nonempty=True,
    )


def _canonical_utf8_digest_parts(
    digest_domain: str,
    declared_byte_count: int,
    parts: Iterable[bytes],
    *,
    field_name: str,
    require_nonempty: bool,
) -> bytes:
    byte_count = _require_int63(
        declared_byte_count,
        field_name=f"{field_name} declared_byte_count",
    )
    if require_nonempty and byte_count == 0:
        raise ByteDomainError(f"{field_name} must not be empty")

    def validated_parts() -> Iterator[bytes]:
        decoder = codecs.getincrementaldecoder("utf-8")("strict")
        for part in parts:
            exact = _require_bytes(part, field_name=f"{field_name} part")
            try:
                decoder.decode(exact, final=False)
            except UnicodeDecodeError as error:
                raise ByteDomainError(f"{field_name} must be exact UTF-8") from error
            yield exact
        try:
            decoder.decode(b"", final=True)
        except UnicodeDecodeError as error:
            raise ByteDomainError(f"{field_name} must be exact UTF-8") from error

    return canonical_value_digest_parts(
        digest_domain,
        byte_count,
        validated_parts(),
    )


def encode_source_root(
    components: Sequence[str],
    *,
    codec_version: int = SOURCE_ROOT_CODEC_VERSION,
) -> bytes:
    """Encode an already-parsed absolute POSIX source-root segment tuple.

    The adapter is responsible for establishing absoluteness before calling
    this codec.  ``()`` is the absolute root ``/``.  No normalization,
    case-folding, symlink resolution, or ``realpath`` operation occurs here.
    """

    return b"".join(iter_source_root_payload(components, codec_version=codec_version))


def iter_source_root_payload(
    components: Sequence[str],
    *,
    codec_version: int = SOURCE_ROOT_CODEC_VERSION,
) -> Iterator[bytes]:
    """Yield the exact absolute-root frame in bounded segment pieces."""

    version = _require_registered_version(
        codec_version,
        registered=SOURCE_ROOT_CODEC_VERSION,
        field_name="codec_version",
    )
    if isinstance(components, (str, bytes, bytearray)):
        raise ByteDomainError("source root components must be a segment sequence")
    if len(components) > _UINT32_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("source root has too many components")
    yield version.to_bytes(4, "big")
    yield len(components).to_bytes(4, "big")
    for component in components:
        encoded = _validate_source_root_segment(component)
        yield len(encoded).to_bytes(4, "big")
        yield encoded


def decode_source_root(payload: bytes) -> tuple[str, ...]:
    """Materialize one small decoded source root (convenience/oracle API)."""

    _receipt, components = _consume_source_root_parts(
        (payload,), collect_components=True
    )
    return components


def validate_source_root_parts(parts: Iterable[bytes]) -> SourceRootValidationReceipt:
    """Consume arbitrary bounded parts and prove exact framing and EOF."""

    receipt, _components = _consume_source_root_parts(parts, collect_components=False)
    return receipt


def _consume_source_root_parts(
    parts: Iterable[bytes], *, collect_components: bool
) -> tuple[SourceRootValidationReceipt, tuple[str, ...]]:
    phase = "VERSION"
    carry = bytearray()
    component_count = 0
    emitted = 0
    segment_size = 0
    components: list[str] = []
    digest = sha256()
    consumed = 0
    for part in parts:
        exact = _require_bytes(part, field_name="source root part")
        consumed += len(exact)
        if consumed > _INT63_MAX:
            raise ByteDomainError("source root exceeds signed-int63 bytes")
        digest.update(exact)
        offset = 0
        while offset < len(exact):
            if phase == "DONE":
                raise ByteDomainError("source root payload contains trailing bytes")
            required = segment_size if phase == "SEGMENT" else 4
            amount = min(required - len(carry), len(exact) - offset)
            carry.extend(exact[offset : offset + amount])
            offset += amount
            if len(carry) != required:
                continue
            value = bytes(carry)
            carry.clear()
            if phase == "VERSION":
                version = int.from_bytes(value, "big")
                if version != SOURCE_ROOT_CODEC_VERSION:
                    raise IntegerDomainError(
                        f"codec_version {version} is not registered"
                    )
                phase = "COUNT"
            elif phase == "COUNT":
                component_count = int.from_bytes(value, "big")
                phase = "DONE" if component_count == 0 else "LENGTH"
            elif phase == "LENGTH":
                segment_size = int.from_bytes(value, "big")
                if not 1 <= segment_size <= 255:
                    raise ByteDomainError(
                        "source root segment length must be in [1, 255]"
                    )
                phase = "SEGMENT"
            else:
                try:
                    segment = value.decode("utf-8", errors="strict")
                except UnicodeDecodeError as error:
                    raise ByteDomainError(
                        "source root segment must be strict UTF-8"
                    ) from error
                if _validate_source_root_segment(segment) != value:
                    raise ByteDomainError("source root segment is not canonical UTF-8")
                if collect_components:
                    components.append(segment)
                emitted += 1
                phase = "DONE" if emitted == component_count else "LENGTH"
    if phase != "DONE" or carry:
        raise ByteDomainError("source root payload is truncated")
    return (
        SourceRootValidationReceipt(component_count, consumed, digest.digest()),
        tuple(components),
    )


def source_root_digest(
    components: Sequence[str],
    *,
    codec_version: int = SOURCE_ROOT_CODEC_VERSION,
) -> bytes:
    """Stream the canonical ``source_root_v1`` identity digest."""

    byte_count = 0
    for part in iter_source_root_payload(components, codec_version=codec_version):
        byte_count += len(part)
        if byte_count > _INT63_MAX:
            raise ByteDomainError("source root exceeds signed-int63 bytes")
    return canonical_value_digest_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        byte_count,
        iter_source_root_payload(components, codec_version=codec_version),
    )


def encode_gallery_observation_page(
    page: GalleryObservationPage,
    *,
    codec_version: int = GALLERY_OBSERVATION_PAGE_CODEC_VERSION,
) -> bytes:
    """Encode one exact bounded observation page frame."""

    if type(page) is not GalleryObservationPage:
        raise ByteDomainError("page must be GalleryObservationPage")
    version = _require_registered_version(
        codec_version,
        registered=GALLERY_OBSERVATION_PAGE_CODEC_VERSION,
        field_name="codec_version",
    )
    payload = bytearray(_GALLERY_OBSERVATION_PAGE_PREFIX)
    payload.extend(version.to_bytes(4, "big"))
    payload.append(int(page.component))
    payload.append(int(page.node_kind))
    payload.append(page.level)
    payload.extend(page.subtree_item_count.to_bytes(8, "big"))
    payload.extend(len(page.entries).to_bytes(4, "big"))
    for entry in page.entries:
        _append_gallery_observation_page_entry(payload, page, entry)
    if len(payload) > GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("encoded gallery observation page exceeds 65536 bytes")
    return bytes(payload)


def gallery_observation_page_digest(page_bytes: bytes) -> bytes:
    """Return raw SHA-256 of an exact already-domain-prefixed page frame."""

    payload = _require_bytes(page_bytes, field_name="page_bytes")
    if not payload or len(payload) > GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("page_bytes length must be in 1..65536")
    decode_gallery_observation_page(payload)
    return sha256(payload).digest()


def verify_gallery_observation_page_conflict(
    existing_page_sha256: bytes,
    existing_page_bytes: bytes,
    proposed_page_bytes: bytes,
) -> None:
    """Byte-compare an existing page before content-addressed reuse."""

    expected = _require_digest(
        existing_page_sha256,
        field_name="existing_page_sha256",
    )
    existing = _require_bytes(existing_page_bytes, field_name="existing_page_bytes")
    proposed = _require_bytes(proposed_page_bytes, field_name="proposed_page_bytes")
    if gallery_observation_page_digest(existing) != expected:
        raise DigestMismatchError("existing page bytes do not match page_sha256")
    if existing != proposed:
        raise CanonicalIdentityCollisionError(
            "page_sha256 conflict has a different exact page frame"
        )
    if gallery_observation_page_digest(proposed) != expected:  # pragma: no cover
        raise DigestMismatchError("proposed page bytes do not match page_sha256")


def gallery_observation_page_key_bounds(
    page: GalleryObservationPage,
    *,
    child_bounds: Mapping[bytes, tuple[bytes, bytes]] | None = None,
) -> tuple[bytes, bytes] | None:
    """Derive the normalized first/last-key satellite for one decoded page."""

    if type(page) is not GalleryObservationPage:
        raise ByteDomainError("page must be GalleryObservationPage")
    if page.node_kind is GalleryObservationNodeKind.LEAF:
        if not page.entries:
            if page.component is GalleryObservationComponent.METADATA:
                raise ByteDomainError("METADATA has no canonical empty leaf")
            return None
        keys = tuple(_encoded_leaf_key(entry) for entry in page.entries)
        return keys[0], keys[-1]
    if child_bounds is None:
        raise ByteDomainError("branch page key bounds require child bounds")
    ordered: list[tuple[bytes, bytes]] = []
    for entry in page.entries:
        assert type(entry) is GalleryObservationBranchEntry
        bounds = child_bounds.get(entry.child_sha256)
        if bounds is None:
            raise ByteDomainError("branch child key bounds are missing")
        first, last = bounds
        _validate_page_bound(first, field_name="child first_key")
        _validate_page_bound(last, field_name="child last_key")
        if first > last:
            raise ByteDomainError("branch child key bounds are reversed")
        if ordered and ordered[-1][1] >= first:
            raise ByteDomainError("branch child key ranges overlap or are unordered")
        ordered.append((first, last))
    return ordered[0][0], ordered[-1][1]


def decode_gallery_observation_page(payload: bytes) -> GalleryObservationPage:
    """Decode an exact page and reject unknown tags, truncation, or trailing bytes."""

    encoded = _require_bytes(payload, field_name="gallery observation page")
    if len(encoded) > GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES:
        raise ByteDomainError("gallery observation page exceeds 65536 bytes")
    header_size = len(_GALLERY_OBSERVATION_PAGE_PREFIX) + 19
    if len(encoded) < header_size:
        raise ByteDomainError("gallery observation page is truncated")
    if not encoded.startswith(_GALLERY_OBSERVATION_PAGE_PREFIX):
        raise ByteDomainError("gallery observation page has the wrong prefix")
    offset = len(_GALLERY_OBSERVATION_PAGE_PREFIX)
    version, offset = _take_uint(encoded, offset, 4, "page codec version")
    if version != GALLERY_OBSERVATION_PAGE_CODEC_VERSION:
        raise IntegerDomainError(f"codec_version {version} is not registered")
    component_raw, offset = _take_uint(encoded, offset, 1, "page component")
    node_raw, offset = _take_uint(encoded, offset, 1, "page node kind")
    level, offset = _take_uint(encoded, offset, 1, "page level")
    subtree_count, offset = _take_uint(encoded, offset, 8, "page subtree count")
    entry_count, offset = _take_uint(encoded, offset, 4, "page entry count")
    try:
        component = GalleryObservationComponent(component_raw)
    except ValueError as error:
        raise ByteDomainError("unknown gallery observation component") from error
    try:
        node_kind = GalleryObservationNodeKind(node_raw)
    except ValueError as error:
        raise ByteDomainError("unknown gallery observation node kind") from error
    if entry_count > _page_entry_capacity(component, node_kind):
        raise ByteDomainError("page entry_count exceeds its component capacity")
    entries: list[GalleryObservationPageEntry] = []
    for _ in range(entry_count):
        entry, offset = _decode_gallery_observation_page_entry(
            encoded,
            offset,
            component=component,
            node_kind=node_kind,
        )
        entries.append(entry)
    if offset != len(encoded):
        raise ByteDomainError("gallery observation page contains trailing bytes")
    page = GalleryObservationPage(
        component,
        node_kind,
        level,
        subtree_count,
        tuple(entries),
    )
    if encode_gallery_observation_page(page) != encoded:
        raise ByteDomainError("gallery observation page is not canonical")
    return page


def build_gallery_observation_tree(
    component: GalleryObservationComponent,
    entries: Sequence[GalleryObservationLeafEntry],
) -> GalleryObservationTree:
    """Build a canonical FILE, TAG, or DIRECTORY tree as an in-memory oracle.

    This reference/test helper intentionally returns every page and is not the
    production giant-gallery writer.  Production staging feeds individual
    :class:`GalleryObservationPage` values to
    :func:`encode_gallery_observation_page` and persists a durable frontier.
    """

    if type(component) is not GalleryObservationComponent:
        raise ByteDomainError("component must be GalleryObservationComponent")
    if component is GalleryObservationComponent.METADATA:
        raise ByteDomainError(
            "use build_gallery_observation_metadata_tree for METADATA"
        )
    ordered = _canonical_observation_entries(component, entries)
    capacity = _page_entry_capacity(component, GalleryObservationNodeKind.LEAF)
    leaf_groups: list[tuple[GalleryObservationLeafEntry, ...]]
    if ordered:
        leaf_groups = [
            tuple(ordered[offset : offset + capacity])
            for offset in range(0, len(ordered), capacity)
        ]
    else:
        leaf_groups = [()]
    pages: list[GalleryObservationEncodedPage] = []
    current: list[tuple[GalleryObservationEncodedPage, int]] = []
    for group in leaf_groups:
        page = GalleryObservationPage(
            component,
            GalleryObservationNodeKind.LEAF,
            0,
            len(group),
            group,
        )
        encoded_page = _encode_page_value(page)
        pages.append(encoded_page)
        current.append((encoded_page, len(group)))
    return _finish_gallery_observation_tree(component, current, pages)


def build_gallery_observation_metadata_tree(
    metadata: GalleryObservationMetadata,
) -> GalleryObservationTree:
    """Build deterministic METADATA pages as an in-memory reference oracle.

    Production writers consume :func:`iter_gallery_observation_metadata_stream`
    incrementally and persist each bounded page plus their durable frontier.
    """

    if type(metadata) is not GalleryObservationMetadata:
        raise ByteDomainError("metadata must be GalleryObservationMetadata")
    parts: Iterable[bytes] = iter_gallery_observation_metadata_stream(metadata)
    chunks = tuple(_chunk_exact_byte_stream(parts))
    if not chunks:
        raise AssertionError("the fixed METADATA stream is never empty")
    pages: list[GalleryObservationEncodedPage] = []
    current: list[tuple[GalleryObservationEncodedPage, int]] = []
    for chunk in chunks:
        page = GalleryObservationPage(
            GalleryObservationComponent.METADATA,
            GalleryObservationNodeKind.LEAF,
            0,
            len(chunk.chunk_bytes),
            (chunk,),
        )
        encoded_page = _encode_page_value(page)
        pages.append(encoded_page)
        current.append((encoded_page, len(chunk.chunk_bytes)))
    return _finish_gallery_observation_tree(
        GalleryObservationComponent.METADATA,
        current,
        pages,
    )


def validate_gallery_observation_tree(tree: GalleryObservationTree) -> None:
    """Decode every supplied page and prove exact canonical tree structure."""

    if type(tree) is not GalleryObservationTree:
        raise ByteDomainError("tree must be GalleryObservationTree")
    page_by_digest: dict[
        bytes, tuple[GalleryObservationEncodedPage, GalleryObservationPage]
    ] = {}
    for encoded in tree.pages:
        if type(encoded) is not GalleryObservationEncodedPage:
            raise ByteDomainError("tree pages must be GalleryObservationEncodedPage")
        page = decode_gallery_observation_page(encoded.page_bytes)
        if page.component is not tree.component:
            raise ByteDomainError("tree contains a cross-component page")
        previous = page_by_digest.get(encoded.page_sha256)
        if previous is not None:
            if previous[0].page_bytes != encoded.page_bytes:
                raise CanonicalIdentityCollisionError(
                    "one page digest maps to different exact page bytes"
                )
            raise ByteDomainError("tree page set contains a duplicate page digest")
        page_by_digest[encoded.page_sha256] = (encoded, page)
    if tree.root_page_sha256 not in page_by_digest:
        raise ByteDomainError("tree root page is missing")

    visited: set[bytes] = set()

    def visit(digest: bytes, expected_level: int | None = None) -> int:
        if digest in visited:
            raise ByteDomainError("tree reuses a page within one traversal")
        visited.add(digest)
        _encoded, page = page_by_digest[digest]
        if expected_level is not None and page.level != expected_level:
            raise ByteDomainError("branch child level is not parent level minus one")
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            return page.subtree_item_count
        total = 0
        for entry in page.entries:
            assert type(entry) is GalleryObservationBranchEntry
            child = page_by_digest.get(entry.child_sha256)
            if child is None:
                raise ByteDomainError("branch child page is missing")
            if child[1].subtree_item_count != entry.child_subtree_item_count:
                raise ByteDomainError("branch child count disagrees with child page")
            child_count = visit(entry.child_sha256, page.level - 1)
            total += child_count
            if total > _INT63_MAX:
                raise IntegerDomainError("tree count exceeds signed-int63")
        if total != page.subtree_item_count:
            raise ByteDomainError("branch subtree count is not its exact child sum")
        return total

    total = visit(tree.root_page_sha256)
    if total != tree.item_count:
        raise ByteDomainError("tree item_count disagrees with its root page")
    if visited != set(page_by_digest):
        raise ByteDomainError("tree contains pages unreachable from its root")
    _validate_tree_leaf_sequence(tree, page_by_digest)
    _validate_tree_branch_shape(tree, page_by_digest)


def iter_gallery_observation_metadata_stream(
    metadata: GalleryObservationMetadata,
) -> Iterator[bytes]:
    """Yield the exact fixed-field metadata stream without one giant byte bind."""

    if type(metadata) is not GalleryObservationMetadata:
        raise ByteDomainError("metadata must be GalleryObservationMetadata")
    yield _GALLERY_OBSERVATION_METADATA_PREFIX
    yield GALLERY_OBSERVATION_METADATA_CODEC_VERSION.to_bytes(4, "big")
    yield metadata.gid.to_bytes(8, "big")
    for tag, value in (
        (1, metadata.title),
        (2, metadata.comment),
        (3, metadata.upload_account),
    ):
        size = _strict_utf8_size(value, field_name=f"metadata field {tag}")
        yield bytes((tag,))
        yield size.to_bytes(8, "big")
        yield from _iter_strict_utf8(value, field_name=f"metadata field {tag}")
    yield metadata.upload_time.to_bytes(8, "big")
    yield metadata.download_time.to_bytes(8, "big")
    yield metadata.modified_time.to_bytes(8, "big")
    yield metadata.scan_observation_version.to_bytes(4, "big")
    yield metadata.source_file_count.to_bytes(8, "big")
    if metadata.page_count is None:
        yield b"\x00"
    else:
        yield b"\x01"
        yield metadata.page_count.to_bytes(4, "big")


def encode_gallery_observation_metadata(metadata: GalleryObservationMetadata) -> bytes:
    """Materialize the exact metadata stream for tests or bounded callers.

    Production writers should consume :func:`iter_gallery_observation_metadata_stream`
    into the canonical chunk-tree builder instead of creating a monolithic bind.
    """

    return b"".join(iter_gallery_observation_metadata_stream(metadata))


def decode_gallery_observation_metadata(payload: bytes) -> GalleryObservationMetadata:
    """Decode the fixed metadata stream and reject unknown/trailing fields."""

    encoded = _require_bytes(payload, field_name="gallery observation metadata")
    validate_gallery_observation_metadata_parts((encoded,))
    if not encoded.startswith(_GALLERY_OBSERVATION_METADATA_PREFIX):
        raise ByteDomainError("gallery observation metadata has the wrong prefix")
    offset = len(_GALLERY_OBSERVATION_METADATA_PREFIX)
    version, offset = _take_uint(encoded, offset, 4, "metadata codec version")
    if version != GALLERY_OBSERVATION_METADATA_CODEC_VERSION:
        raise IntegerDomainError(f"codec_version {version} is not registered")
    gid, offset = _take_uint(encoded, offset, 8, "gid")
    values: list[str] = []
    for expected_tag, field_name in (
        (1, "title"),
        (2, "comment"),
        (3, "upload_account"),
    ):
        tag, offset = _take_uint(encoded, offset, 1, f"{field_name} tag")
        if tag != expected_tag:
            raise ByteDomainError(f"unknown or out-of-order metadata tag {tag}")
        size, offset = _take_uint(encoded, offset, 8, f"{field_name} length")
        _require_int63(size, field_name=f"{field_name} length")
        value_bytes, offset = _take_exact(encoded, offset, size, field_name)
        try:
            value = value_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ByteDomainError(f"{field_name} must be strict UTF-8") from error
        values.append(value)
    upload_time, offset = _take_uint(encoded, offset, 8, "upload_time")
    download_time, offset = _take_uint(encoded, offset, 8, "download_time")
    modified_time, offset = _take_uint(encoded, offset, 8, "modified_time")
    scan_version, offset = _take_uint(encoded, offset, 4, "scan_observation_version")
    source_file_count, offset = _take_uint(encoded, offset, 8, "source_file_count")
    presence, offset = _take_uint(encoded, offset, 1, "page_count presence")
    if presence == 0:
        page_count = None
    elif presence == 1:
        page_count, offset = _take_uint(encoded, offset, 4, "page_count")
    else:
        raise ByteDomainError("page_count presence must be exactly zero or one")
    if offset != len(encoded):
        raise ByteDomainError("gallery observation metadata contains trailing bytes")
    return GalleryObservationMetadata(
        gid,
        values[0],
        values[1],
        values[2],
        upload_time,
        download_time,
        modified_time,
        scan_version,
        source_file_count,
        page_count,
    )


def validate_gallery_observation_metadata_parts(
    parts: Iterable[bytes],
) -> GalleryObservationMetadataScalarReceipt:
    """Incrementally validate arbitrary splits and return bounded scalar evidence."""

    decoder = GalleryObservationMetadataDecoder()
    for part in parts:
        decoder.feed(part)
    return decoder.finish()


def encode_gallery_observation_descriptor(
    descriptor: GalleryObservationDescriptor,
    *,
    codec_version: int = GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION,
) -> bytes:
    """Encode the bounded four-root observation descriptor."""

    if type(descriptor) is not GalleryObservationDescriptor:
        raise ByteDomainError("descriptor must be GalleryObservationDescriptor")
    version = _require_registered_version(
        codec_version,
        registered=GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION,
        field_name="codec_version",
    )
    return b"".join(
        (
            version.to_bytes(4, "big"),
            descriptor.metadata_root_sha256,
            descriptor.metadata_byte_count.to_bytes(8, "big"),
            descriptor.file_root_sha256,
            descriptor.file_item_count.to_bytes(8, "big"),
            descriptor.tag_root_sha256,
            descriptor.tag_item_count.to_bytes(8, "big"),
            descriptor.directory_root_sha256,
            descriptor.directory_item_count.to_bytes(8, "big"),
        )
    )


def decode_gallery_observation_descriptor(
    payload: bytes,
) -> GalleryObservationDescriptor:
    """Decode the exact fixed-width four-root observation descriptor."""

    encoded = _require_bytes(payload, field_name="gallery observation descriptor")
    expected_size = 4 + 4 * (32 + 8)
    if len(encoded) != expected_size:
        raise ByteDomainError(
            f"gallery observation descriptor must be exactly {expected_size} bytes"
        )
    version = int.from_bytes(encoded[:4], "big")
    if version != GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION:
        raise IntegerDomainError(f"codec_version {version} is not registered")
    offset = 4
    values: list[bytes | int] = []
    for name in ("metadata", "file", "tag", "directory"):
        digest, offset = _take_exact(encoded, offset, 32, f"{name} root digest")
        count, offset = _take_uint(encoded, offset, 8, f"{name} count")
        values.extend((digest, count))
    return GalleryObservationDescriptor(
        values[0],  # type: ignore[arg-type]
        values[1],  # type: ignore[arg-type]
        values[2],  # type: ignore[arg-type]
        values[3],  # type: ignore[arg-type]
        values[4],  # type: ignore[arg-type]
        values[5],  # type: ignore[arg-type]
        values[6],  # type: ignore[arg-type]
        values[7],  # type: ignore[arg-type]
    )


def gallery_observation_descriptor_digest(
    descriptor: GalleryObservationDescriptor,
    *,
    codec_version: int = GALLERY_OBSERVATION_DESCRIPTOR_CODEC_VERSION,
) -> bytes:
    """Return the canonical ``gallery_observation_v1`` descriptor identity."""

    return canonical_value_digest(
        GALLERY_OBSERVATION_DIGEST_DOMAIN,
        encode_gallery_observation_descriptor(
            descriptor,
            codec_version=codec_version,
        ),
    )


def gallery_directory_audit_digest(
    root_page_sha256: bytes,
    item_count: int,
) -> bytes:
    """Hash the closed DIRECTORY audit frame; never identity authority."""

    root = _require_digest(root_page_sha256, field_name="DIRECTORY audit root")
    count = _require_int63(item_count, field_name="DIRECTORY audit item_count")
    return sha256(
        _GALLERY_DIRECTORY_AUDIT_PREFIX + root + count.to_bytes(8, "big")
    ).digest()


def gallery_metadata_audit_digest(
    root_page_sha256: bytes,
    byte_count: int,
) -> bytes:
    """Hash the closed METADATA audit frame; never identity authority."""

    root = _require_digest(root_page_sha256, field_name="METADATA audit root")
    count = _require_int63(byte_count, field_name="METADATA audit byte_count")
    return sha256(
        _GALLERY_METADATA_AUDIT_PREFIX + root + count.to_bytes(8, "big")
    ).digest()


def gallery_scan_audit_digest(
    roots: Mapping[GalleryObservationComponent, tuple[bytes, int]],
) -> bytes:
    """Hash exact FILE/TAG/METADATA/DIRECTORY root-count audit tuples."""

    if not isinstance(roots, Mapping):
        raise ByteDomainError("scan audit roots must be a mapping")
    expected_components = (
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.TAG,
        GalleryObservationComponent.METADATA,
        GalleryObservationComponent.DIRECTORY,
    )
    if (
        len(roots) != len(expected_components)
        or any(
            type(component) is not GalleryObservationComponent for component in roots
        )
        or set(roots) != set(expected_components)
    ):
        raise ByteDomainError("scan audit roots must contain the exact component set")
    payload = bytearray(_GALLERY_SCAN_AUDIT_PREFIX)
    for component in expected_components:
        value = roots[component]
        if type(value) is not tuple or len(value) != 2:
            raise ByteDomainError(
                f"{component.name} scan audit value must be an exact pair"
            )
        root, count = value
        payload.extend(
            _require_digest(root, field_name=f"{component.name} scan audit root")
        )
        payload.extend(
            _require_int63(
                count,
                field_name=f"{component.name} scan audit count",
            ).to_bytes(8, "big")
        )
    return sha256(payload).digest()


def artifact_name(gid: int) -> bytes:
    """Derive the sole artifact leaf name for one positive GID."""

    validated_gid = _require_positive_int63(gid, field_name="gid")
    return (
        _ARTIFACT_NAME_PREFIX
        + str(validated_gid).encode("ascii")
        + _ARTIFACT_NAME_SUFFIX
    )


def decode_artifact_name(value: bytes) -> int:
    """Decode one exact canonical artifact leaf name into its positive GID."""

    encoded = _require_bytes(value, field_name="artifact_name")
    maximum_bytes = (
        len(_ARTIFACT_NAME_PREFIX) + len(str(_INT63_MAX)) + len(_ARTIFACT_NAME_SUFFIX)
    )
    if len(encoded) > maximum_bytes:
        raise ByteDomainError(f"artifact_name exceeds {maximum_bytes} bytes")
    if not encoded.startswith(_ARTIFACT_NAME_PREFIX):
        raise ByteDomainError("artifact_name has the wrong registered prefix")
    if not encoded.endswith(_ARTIFACT_NAME_SUFFIX):
        raise ByteDomainError("artifact_name has the wrong registered suffix")

    gid_ascii = encoded[len(_ARTIFACT_NAME_PREFIX) : -len(_ARTIFACT_NAME_SUFFIX)]
    gid = _decode_canonical_positive_int63_ascii(
        gid_ascii,
        field_name="artifact_name gid",
    )
    if artifact_name(gid) != encoded:  # defensive canonicality closure
        raise ByteDomainError("artifact_name is not in canonical encoded form")
    return gid


def artifact_archive_member_name(
    entry_position: int,
    source_role: ArtifactSourceRole,
    transform_kind: ArtifactTransformKind,
    excluded_flag: bool,
) -> bytes | None:
    """Derive the sole archive member name from the closed semantic tags."""

    position = _require_int63(entry_position, field_name="entry_position")
    if type(source_role) is not ArtifactSourceRole:
        raise ByteDomainError("source_role must be ArtifactSourceRole")
    if type(transform_kind) is not ArtifactTransformKind:
        raise ByteDomainError("transform_kind must be ArtifactTransformKind")
    if type(excluded_flag) is not bool:
        raise ByteDomainError("excluded_flag must be exactly bool")
    if source_role is ArtifactSourceRole.METADATA:
        if transform_kind is not ArtifactTransformKind.RAW_COPY:
            raise ByteDomainError("METADATA archive members must use RAW_COPY")
        leaf = b"metadata.txt"
    elif transform_kind is ArtifactTransformKind.RAW_COPY:
        leaf = b"content.bin"
    elif transform_kind is ArtifactTransformKind.GIF_NORMALIZE:
        leaf = b"content.gif"
    elif transform_kind is ArtifactTransformKind.JPEG_NORMALIZE:
        leaf = b"content.jpg"
    else:  # pragma: no cover - closed enum
        raise AssertionError("unreachable artifact transform")
    if excluded_flag:
        return None
    return f"{position:016x}".encode("ascii") + b"__" + leaf


def validate_namespace(value: str) -> bytes:
    """Validate one exact UTF-8 tag namespace without normalization."""

    return _validate_utf8_bytes(value, field_name="namespace", maximum_bytes=128)


def validate_registered_ascii_identifier(
    value: str,
    *,
    allowed: Collection[str],
    field_name: str,
    maximum_bytes: int = 64,
) -> bytes:
    """Validate exact membership in a caller-supplied ASCII registry.

    This is shared by protocol-owned stages, providers, channels, formats, and
    similar finite domains.  Membership is exact: no trimming, normalization,
    or case folding is performed.
    """

    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise RegisteredIdentifierError("maximum_bytes must be a positive integer")
    registered = frozenset(allowed)
    if not registered:
        raise RegisteredIdentifierError(f"{field_name} registry must not be empty")
    if any(type(item) is not str for item in registered):
        raise RegisteredIdentifierError(
            f"{field_name} registry entries must all be str"
        )
    for item in registered:
        _validate_ascii_identifier_bytes(
            item,
            field_name=f"registered {field_name}",
            maximum_bytes=maximum_bytes,
        )

    encoded = _validate_ascii_identifier_bytes(
        value,
        field_name=field_name,
        maximum_bytes=maximum_bytes,
    )
    if value not in registered:
        raise RegisteredIdentifierError(
            f"{field_name} {value!r} is not a registered identifier"
        )
    return encoded


def validate_state_component(value: str) -> bytes:
    """Validate one of the five vNext analysis overlay components."""

    return validate_registered_ascii_identifier(
        value,
        allowed=ANALYSIS_STATE_COMPONENTS,
        field_name="state_component",
        maximum_bytes=64,
    )


def validate_gallery_observation_durable_parser_phase(value: str) -> bytes:
    """Validate an exact persisted metadata-parser phase, rejecting aliases."""

    return validate_registered_ascii_identifier(
        value,
        allowed=GALLERY_OBSERVATION_DURABLE_PARSER_PHASES,
        field_name="gallery_observation_durable_parser_phase",
        maximum_bytes=32,
    )


def validate_artifact_component_kind(value: str) -> bytes:
    """Validate one of the six semantic artifact-input components."""

    return validate_registered_ascii_identifier(
        value,
        allowed=ARTIFACT_COMPONENT_KINDS,
        field_name="component_kind",
        maximum_bytes=32,
    )


def publication_id(gid: int) -> bytes:
    """Construct the gallery URN for one positive signed-int63 GID."""

    validated_gid = _require_positive_int63(gid, field_name="gid")
    encoded = _PUBLICATION_ID_PREFIX + str(validated_gid).encode("ascii")
    if len(encoded) > 64:  # protects the contract if the prefix changes
        raise ByteDomainError("publication_id exceeds 64 bytes")
    return encoded


def decode_publication_id(value: bytes) -> int:
    """Decode one exact canonical gallery URN into its positive GID."""

    encoded = _require_bytes(value, field_name="publication_id")
    if len(encoded) > 64:
        raise ByteDomainError("publication_id exceeds 64 bytes")
    if not encoded.startswith(_PUBLICATION_ID_PREFIX):
        raise ByteDomainError("publication_id has the wrong registered prefix")

    gid = _decode_canonical_positive_int63_ascii(
        encoded[len(_PUBLICATION_ID_PREFIX) :],
        field_name="publication_id gid",
    )
    if publication_id(gid) != encoded:  # defensive canonicality closure
        raise ByteDomainError("publication_id is not in canonical encoded form")
    return gid


def publication_key(
    gid: int,
    *,
    algorithm_version: int = PUBLICATION_KEY_ALGORITHM_VERSION,
) -> bytes:
    """Return the stable domain-separated binary publication identity."""

    version = _require_registered_version(
        algorithm_version,
        registered=PUBLICATION_KEY_ALGORITHM_VERSION,
        field_name="algorithm_version",
    )
    validated_gid = _require_positive_int63(gid, field_name="gid")
    digest = sha256(b"h2hdb-vnext-publication-key\0")
    digest.update(version.to_bytes(4, "big"))
    digest.update(validated_gid.to_bytes(8, "big"))
    return digest.digest()


def publication_key_hex(
    gid: int,
    *,
    algorithm_version: int = PUBLICATION_KEY_ALGORITHM_VERSION,
) -> str:
    """Return the API-only lowercase hex form of :func:`publication_key`."""

    return digest_to_hex(publication_key(gid, algorithm_version=algorithm_version))


def artifact_id(gid: int, artifact_sha256: bytes) -> bytes:
    """Construct the registered CBZ artifact URN from binary identity inputs."""

    validated_gid = _require_positive_int63(gid, field_name="gid")
    validated_digest = _require_digest(artifact_sha256, field_name="artifact_sha256")
    encoded = b"".join(
        (
            _ARTIFACT_ID_PREFIX,
            str(validated_gid).encode("ascii"),
            _ARTIFACT_ID_DIGEST_SEPARATOR,
            validated_digest.hex().encode("ascii"),
        )
    )
    if len(encoded) > 128:  # protects the contract if the prefix changes
        raise ByteDomainError("artifact_id exceeds 128 bytes")
    return encoded


def decode_artifact_id(value: bytes) -> tuple[int, bytes]:
    """Decode one exact canonical CBZ artifact URN into its binary authority.

    The GID grammar is deliberately narrower than Python's integer parser: it
    is one or more ASCII decimal digits, has no leading zero, and lies in the
    positive signed-int63 domain.  The digest is exactly 64 lowercase ASCII
    hexadecimal characters.  No whitespace, sign, Unicode digit, suffix, or
    other alternate spelling is accepted.
    """

    encoded = _require_bytes(value, field_name="artifact_id")
    if len(encoded) > 128:
        raise ByteDomainError("artifact_id exceeds 128 bytes")
    if not encoded.startswith(_ARTIFACT_ID_PREFIX):
        raise ByteDomainError("artifact_id has the wrong registered prefix")

    remainder = encoded[len(_ARTIFACT_ID_PREFIX) :]
    gid_ascii, separator, digest_ascii = remainder.partition(
        _ARTIFACT_ID_DIGEST_SEPARATOR
    )
    if separator != _ARTIFACT_ID_DIGEST_SEPARATOR:
        raise ByteDomainError("artifact_id is missing the sha256 separator")
    if not gid_ascii or any(not 48 <= character <= 57 for character in gid_ascii):
        raise ByteDomainError("artifact_id gid must be canonical ASCII decimal digits")
    if len(gid_ascii) > 1 and gid_ascii[0] == ord("0"):
        raise ByteDomainError("artifact_id gid must not contain a leading zero")

    gid = _require_positive_int63(int(gid_ascii), field_name="artifact_id gid")
    try:
        digest_hex = digest_ascii.decode("ascii")
    except UnicodeDecodeError as error:
        raise DigestFormatError(
            "artifact_id digest must be exactly 64 lowercase hex characters"
        ) from error
    digest = digest_from_hex(digest_hex)

    if artifact_id(gid, digest) != encoded:  # defensive canonicality closure
        raise ByteDomainError("artifact_id is not in canonical encoded form")
    return gid, digest


def digest_to_hex(digest: bytes) -> str:
    """Convert an exact 32-byte digest to lowercase API text."""

    return _require_digest(digest, field_name="digest").hex()


def digest_from_hex(value: str) -> bytes:
    """Parse exactly 64 lowercase hexadecimal characters into binary form."""

    if type(value) is not str:
        raise DigestFormatError("digest hex must be str")
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise DigestFormatError(
            "digest hex must be exactly 64 lowercase hex characters"
        )
    return bytes.fromhex(value)


def validate_canonical_value_identity(
    digest: bytes,
    *,
    digest_domain: str,
    payload: bytes,
) -> None:
    """Reject a persisted digest that does not recompute from its exact input."""

    validated_digest = _require_digest(digest, field_name="digest")
    expected = canonical_value_digest(digest_domain, payload)
    if validated_digest != expected:
        raise DigestMismatchError(
            "canonical value digest does not match digest_domain and exact payload"
        )


def verify_canonical_value_conflict(
    existing_digest: bytes,
    existing_digest_domain: str,
    existing_payload: bytes,
    *,
    digest_domain: str,
    payload: bytes,
) -> None:
    """Verify an existing row found for a proposed canonical digest.

    Call this after an insert encounters the digest key already present.  The
    proposed digest is recomputed locally.  A row under another key is a lookup
    or storage mismatch; the same key with different framed input is an
    explicit stored-identity digest collision and must be rejected.
    """

    validated_existing_digest = _require_digest(
        existing_digest, field_name="existing_digest"
    )
    validated_existing_domain = _validate_ascii_identifier_bytes(
        existing_digest_domain,
        field_name="existing_digest_domain",
        maximum_bytes=64,
    )
    validated_existing_payload = _require_bytes(
        existing_payload, field_name="existing_payload"
    )
    validated_domain = _validate_ascii_identifier_bytes(
        digest_domain,
        field_name="digest_domain",
        maximum_bytes=64,
    )
    validated_payload = _require_bytes(payload, field_name="payload")
    proposed_digest = canonical_value_digest(digest_domain, validated_payload)

    if validated_existing_digest != proposed_digest:
        raise DigestMismatchError(
            "existing digest key does not match the proposed canonical value"
        )
    if (
        validated_existing_domain != validated_domain
        or validated_existing_payload != validated_payload
    ):
        raise CanonicalIdentityCollisionError(
            "one canonical digest key maps to different domain/payload input"
        )


def _validate_source_root_segment(value: str) -> bytes:
    encoded = _validate_utf8_bytes(
        value,
        field_name="source root segment",
        maximum_bytes=255,
    )
    if not encoded:
        raise ByteDomainError("source root segment must not be empty")
    if b"\x00" in encoded:
        raise ByteDomainError("source root segment must not contain NUL")
    if b"/" in encoded:
        raise ByteDomainError("source root segment must not contain slash")
    if encoded in {b".", b".."}:
        raise ByteDomainError("source root segment must not be a dot component")
    return encoded


def _validate_canonical_value_page(page: CanonicalValuePage) -> None:
    if page.node_kind is GalleryObservationNodeKind.LEAF:
        if page.level != 0:
            raise ByteDomainError("canonical value LEAF page must have level zero")
        if len(page.entries) > 1 or any(
            type(entry) is not CanonicalValueChunk for entry in page.entries
        ):
            raise ByteDomainError("canonical value LEAF capacity is one chunk")
        expected = (
            0 if not page.entries else len(page.entries[0].chunk_bytes)  # type: ignore[union-attr]
        )
        if page.subtree_byte_count != expected:
            raise ByteDomainError("canonical value leaf byte count mismatch")
        if not page.entries and page.page_position != 0:
            raise ByteDomainError("empty canonical value leaf position must be zero")
        if page.entries:
            expected_offset = page.page_position * CANONICAL_VALUE_CHUNK_BYTES
            if expected_offset > _INT63_MAX:
                raise IntegerDomainError("canonical value leaf offset exceeds int63")
            entry = page.entries[0]
            assert type(entry) is CanonicalValueChunk
            if entry.byte_offset != expected_offset:
                raise ByteDomainError(
                    "canonical value leaf offset must equal page_position * 32768"
                )
        return
    if page.level == 0:
        raise ByteDomainError("canonical value BRANCH level must be in 1..8")
    if not 1 <= len(page.entries) <= CANONICAL_VALUE_BRANCH_CAPACITY:
        raise ByteDomainError("canonical value BRANCH fanout must be in 1..256")
    if any(type(entry) is not CanonicalValueBranchEntry for entry in page.entries):
        raise ByteDomainError("canonical value BRANCH contains a leaf chunk")
    child_digests = tuple(
        entry.child_page_sha256
        for entry in page.entries
        if type(entry) is CanonicalValueBranchEntry
    )
    if len(set(child_digests)) != len(child_digests):
        raise ByteDomainError("canonical value BRANCH has a duplicate child")
    total = sum(
        entry.child_subtree_byte_count
        for entry in page.entries
        if type(entry) is CanonicalValueBranchEntry
    )
    if total > _INT63_MAX:
        raise IntegerDomainError("canonical value branch count exceeds int63")
    if total != page.subtree_byte_count:
        raise ByteDomainError("canonical value branch byte count mismatch")


def _encode_canonical_value_page_value(
    page: CanonicalValuePage,
) -> CanonicalValueEncodedPage:
    payload = encode_canonical_value_page(page)
    return CanonicalValueEncodedPage(canonical_value_page_digest(payload), payload)


def _chunk_canonical_value_parts(
    parts: Iterable[bytes],
    declared_byte_count: int,
) -> Iterator[CanonicalValueChunk]:
    buffer = bytearray()
    offset = 0
    consumed = 0
    for part in parts:
        exact = _require_bytes(part, field_name="canonical value part")
        consumed += len(exact)
        if consumed > declared_byte_count:
            raise ByteDomainError("canonical value parts exceed declared_byte_count")
        position = 0
        while position < len(exact):
            room = CANONICAL_VALUE_CHUNK_BYTES - len(buffer)
            amount = min(room, len(exact) - position)
            buffer.extend(exact[position : position + amount])
            position += amount
            if len(buffer) == CANONICAL_VALUE_CHUNK_BYTES:
                yield CanonicalValueChunk(offset, bytes(buffer))
                offset += len(buffer)
                buffer.clear()
    if consumed != declared_byte_count:
        raise ByteDomainError("canonical value parts do not equal declared_byte_count")
    if buffer:
        yield CanonicalValueChunk(offset, bytes(buffer))


def _page_entry_capacity(
    component: GalleryObservationComponent,
    node_kind: GalleryObservationNodeKind,
) -> int:
    if node_kind is GalleryObservationNodeKind.BRANCH:
        return GALLERY_OBSERVATION_BRANCH_CAPACITY
    return {
        GalleryObservationComponent.FILE: GALLERY_OBSERVATION_FILE_LEAF_CAPACITY,
        GalleryObservationComponent.TAG: GALLERY_OBSERVATION_TAG_LEAF_CAPACITY,
        GalleryObservationComponent.DIRECTORY: (
            GALLERY_OBSERVATION_DIRECTORY_LEAF_CAPACITY
        ),
        GalleryObservationComponent.METADATA: 1,
    }[component]


def _leaf_entry_type(
    component: GalleryObservationComponent,
) -> type[GalleryObservationLeafEntry]:
    return {
        GalleryObservationComponent.FILE: GalleryObservationFileEntry,
        GalleryObservationComponent.TAG: GalleryObservationTagEntry,
        GalleryObservationComponent.DIRECTORY: GalleryObservationDirectoryEntry,
        GalleryObservationComponent.METADATA: GalleryObservationMetadataChunk,
    }[component]


def _leaf_order_key(entry: GalleryObservationLeafEntry) -> bytes:
    if type(entry) is GalleryObservationFileEntry:
        return entry.file_no.to_bytes(8, "big")
    if type(entry) is GalleryObservationTagEntry:
        return entry.position.to_bytes(8, "big")
    if type(entry) is GalleryObservationDirectoryEntry:
        return entry.name_bytes
    if type(entry) is GalleryObservationMetadataChunk:
        return entry.byte_offset.to_bytes(8, "big")
    raise AssertionError("unreachable leaf entry type")


def _encoded_leaf_key(entry: GalleryObservationPageEntry) -> bytes:
    if type(entry) is GalleryObservationFileEntry:
        return entry.file_no.to_bytes(8, "big")
    if type(entry) is GalleryObservationTagEntry:
        return entry.position.to_bytes(8, "big")
    if type(entry) is GalleryObservationDirectoryEntry:
        return entry.name_bytes
    if type(entry) is GalleryObservationMetadataChunk:
        return entry.byte_offset.to_bytes(8, "big")
    raise ByteDomainError("branch entries require recursively supplied child bounds")


def _validate_page_bound(value: bytes, *, field_name: str) -> bytes:
    exact = _require_bytes(value, field_name=field_name)
    if not 1 <= len(exact) <= 255:
        raise ByteDomainError(f"{field_name} length must be in 1..255")
    return exact


def _validate_gallery_observation_page(page: GalleryObservationPage) -> None:
    capacity = _page_entry_capacity(page.component, page.node_kind)
    if len(page.entries) > capacity:
        raise ByteDomainError("page has more entries than its fixed capacity")
    if page.node_kind is GalleryObservationNodeKind.LEAF:
        if page.level != 0:
            raise ByteDomainError("LEAF pages must have level zero")
        expected_type = _leaf_entry_type(page.component)
        if any(type(entry) is not expected_type for entry in page.entries):
            raise ByteDomainError("leaf page contains an entry from another component")
        leaf_entries: list[GalleryObservationLeafEntry] = []
        for entry in page.entries:
            if isinstance(entry, GalleryObservationBranchEntry):
                raise ByteDomainError(
                    "leaf page contains an entry from another component"
                )
            leaf_entries.append(entry)
        ordered_keys = [_leaf_order_key(entry) for entry in leaf_entries]
        if any(left >= right for left, right in zip(ordered_keys, ordered_keys[1:])):
            raise ByteDomainError("leaf records must be strictly ordered and unique")
        if page.component is GalleryObservationComponent.DIRECTORY:
            for expected_ordinal, entry in enumerate(page.entries):
                assert type(entry) is GalleryObservationDirectoryEntry
                # An ordinal is global, so a standalone non-first page may start
                # above zero.  Within a page it must remain contiguous.
                if expected_ordinal and entry.canonical_ordinal != (
                    page.entries[expected_ordinal - 1].canonical_ordinal + 1  # type: ignore[union-attr]
                ):
                    raise ByteDomainError(
                        "DIRECTORY ordinals must be contiguous within a leaf"
                    )
        if page.component is GalleryObservationComponent.METADATA:
            if len(page.entries) != 1:
                raise ByteDomainError("METADATA leaf must contain exactly one chunk")
            expected_count = sum(
                len(entry.chunk_bytes)
                for entry in page.entries
                if type(entry) is GalleryObservationMetadataChunk
            )
        else:
            expected_count = len(page.entries)
        if page.subtree_item_count != expected_count:
            raise ByteDomainError("leaf subtree count does not match exact records")
        return

    if page.level == 0:
        raise ByteDomainError("BRANCH pages must have level in 1..8")
    if not page.entries:
        raise ByteDomainError("BRANCH pages must contain at least one child")
    if any(type(entry) is not GalleryObservationBranchEntry for entry in page.entries):
        raise ByteDomainError("BRANCH page contains a leaf record")
    child_digests = tuple(
        entry.child_sha256
        for entry in page.entries
        if type(entry) is GalleryObservationBranchEntry
    )
    if len(set(child_digests)) != len(child_digests):
        raise ByteDomainError("BRANCH page contains a duplicate child digest")
    total = sum(
        entry.child_subtree_item_count
        for entry in page.entries
        if type(entry) is GalleryObservationBranchEntry
    )
    if total > _INT63_MAX:
        raise IntegerDomainError("branch subtree sum exceeds signed-int63")
    if page.subtree_item_count != total:
        raise ByteDomainError("branch subtree count is not its exact child sum")


def _append_gallery_observation_page_entry(
    payload: bytearray,
    page: GalleryObservationPage,
    entry: GalleryObservationPageEntry,
) -> None:
    if page.node_kind is GalleryObservationNodeKind.BRANCH:
        if type(entry) is not GalleryObservationBranchEntry:
            raise ByteDomainError("BRANCH page contains a leaf record")
        payload.extend(entry.child_sha256)
        payload.extend(entry.child_subtree_item_count.to_bytes(8, "big"))
        return
    if page.component is GalleryObservationComponent.FILE:
        assert type(entry) is GalleryObservationFileEntry
        payload.extend(entry.file_no.to_bytes(8, "big"))
        payload.extend(entry.file_key)
        payload.extend(entry.file_sha256)
        payload.extend(entry.size_bytes.to_bytes(8, "big"))
        payload.extend(entry.device.to_bytes(8, "big"))
        payload.extend(entry.inode.to_bytes(8, "big"))
        payload.extend(entry.modified_ns.to_bytes(8, "big", signed=True))
        payload.extend(entry.changed_ns.to_bytes(8, "big", signed=True))
        return
    if page.component is GalleryObservationComponent.TAG:
        assert type(entry) is GalleryObservationTagEntry
        namespace = validate_namespace(entry.namespace)
        payload.extend(entry.position.to_bytes(8, "big"))
        payload.extend(len(namespace).to_bytes(4, "big"))
        payload.extend(namespace)
        payload.extend(entry.tag_value_sha256)
        return
    if page.component is GalleryObservationComponent.DIRECTORY:
        assert type(entry) is GalleryObservationDirectoryEntry
        payload.extend(entry.canonical_ordinal.to_bytes(8, "big"))
        payload.extend(len(entry.name_bytes).to_bytes(4, "big"))
        payload.extend(entry.name_bytes)
        payload.extend(entry.size_bytes.to_bytes(8, "big"))
        payload.extend(entry.device.to_bytes(8, "big"))
        payload.extend(entry.inode.to_bytes(8, "big"))
        payload.extend(entry.modified_ns.to_bytes(8, "big", signed=True))
        payload.extend(entry.changed_ns.to_bytes(8, "big", signed=True))
        payload.extend(int(entry.file_type).to_bytes(4, "big"))
        return
    assert type(entry) is GalleryObservationMetadataChunk
    payload.extend(entry.byte_offset.to_bytes(8, "big"))
    payload.extend(len(entry.chunk_bytes).to_bytes(4, "big"))
    payload.extend(entry.chunk_bytes)


def _decode_gallery_observation_page_entry(
    payload: bytes,
    offset: int,
    *,
    component: GalleryObservationComponent,
    node_kind: GalleryObservationNodeKind,
) -> tuple[GalleryObservationPageEntry, int]:
    if node_kind is GalleryObservationNodeKind.BRANCH:
        digest, offset = _take_exact(payload, offset, 32, "branch child digest")
        count, offset = _take_uint(payload, offset, 8, "branch child count")
        return GalleryObservationBranchEntry(digest, count), offset
    if component is GalleryObservationComponent.FILE:
        file_no, offset = _take_uint(payload, offset, 8, "file_no")
        file_key_value, offset = _take_exact(payload, offset, 32, "file_key")
        file_digest, offset = _take_exact(payload, offset, 32, "file_sha256")
        size, offset = _take_uint(payload, offset, 8, "size_bytes")
        device, offset = _take_uint(payload, offset, 8, "device")
        inode, offset = _take_uint(payload, offset, 8, "inode")
        modified, offset = _take_int64(payload, offset, "modified_ns")
        changed, offset = _take_int64(payload, offset, "changed_ns")
        return (
            GalleryObservationFileEntry(
                file_no,
                file_key_value,
                file_digest,
                size,
                device,
                inode,
                modified,
                changed,
            ),
            offset,
        )
    if component is GalleryObservationComponent.TAG:
        position, offset = _take_uint(payload, offset, 8, "tag position")
        size, offset = _take_uint(payload, offset, 4, "namespace length")
        namespace_bytes, offset = _take_exact(payload, offset, size, "namespace")
        try:
            namespace = namespace_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ByteDomainError("namespace must be strict UTF-8") from error
        value_digest, offset = _take_exact(payload, offset, 32, "tag_value_sha256")
        return GalleryObservationTagEntry(position, namespace, value_digest), offset
    if component is GalleryObservationComponent.DIRECTORY:
        ordinal, offset = _take_uint(payload, offset, 8, "canonical ordinal")
        name_size, offset = _take_uint(payload, offset, 4, "directory name length")
        name, offset = _take_exact(payload, offset, name_size, "directory name")
        size, offset = _take_uint(payload, offset, 8, "size_bytes")
        device, offset = _take_uint(payload, offset, 8, "device")
        inode, offset = _take_uint(payload, offset, 8, "inode")
        modified, offset = _take_int64(payload, offset, "modified_ns")
        changed, offset = _take_int64(payload, offset, "changed_ns")
        file_type_raw, offset = _take_uint(payload, offset, 4, "file_type")
        try:
            directory_type = GalleryObservationDirectoryFileType(file_type_raw)
        except ValueError as error:
            raise ByteDomainError("unknown DIRECTORY file_type") from error
        return (
            GalleryObservationDirectoryEntry(
                ordinal,
                name,
                size,
                device,
                inode,
                modified,
                changed,
                directory_type,
            ),
            offset,
        )
    byte_offset, offset = _take_uint(payload, offset, 8, "metadata byte offset")
    chunk_size, offset = _take_uint(payload, offset, 4, "metadata chunk length")
    chunk, offset = _take_exact(payload, offset, chunk_size, "metadata chunk")
    return GalleryObservationMetadataChunk(byte_offset, chunk), offset


def _canonical_observation_entries(
    component: GalleryObservationComponent,
    entries: Sequence[GalleryObservationLeafEntry],
) -> tuple[GalleryObservationLeafEntry, ...]:
    expected_type = _leaf_entry_type(component)
    if any(type(entry) is not expected_type for entry in entries):
        raise ByteDomainError("tree input contains an entry from another component")
    ordered = tuple(sorted(entries, key=_leaf_order_key))
    keys = tuple(_leaf_order_key(entry) for entry in ordered)
    if len(set(keys)) != len(keys):
        raise ByteDomainError("tree input contains a duplicate canonical key")
    if component is GalleryObservationComponent.DIRECTORY:
        for ordinal, entry in enumerate(ordered):
            assert type(entry) is GalleryObservationDirectoryEntry
            if entry.canonical_ordinal != ordinal:
                raise ByteDomainError(
                    "DIRECTORY canonical_ordinal must equal sorted zero-based position"
                )
    if component is GalleryObservationComponent.FILE:
        for index, entry in enumerate(ordered):
            assert type(entry) is GalleryObservationFileEntry
            if entry.file_no != index:
                raise ByteDomainError(
                    "FILE file_no must equal its zero-based canonical position"
                )
    if component is GalleryObservationComponent.TAG:
        for index, entry in enumerate(ordered):
            assert type(entry) is GalleryObservationTagEntry
            if entry.position != index:
                raise ByteDomainError(
                    "TAG position must equal its zero-based canonical position"
                )
    return ordered


def _encode_page_value(page: GalleryObservationPage) -> GalleryObservationEncodedPage:
    payload = encode_gallery_observation_page(page)
    return GalleryObservationEncodedPage(
        gallery_observation_page_digest(payload), payload
    )


def _finish_gallery_observation_tree(
    component: GalleryObservationComponent,
    current: list[tuple[GalleryObservationEncodedPage, int]],
    pages: list[GalleryObservationEncodedPage],
) -> GalleryObservationTree:
    level = 1
    while len(current) > 1:
        if level > 8:
            raise IntegerDomainError("gallery observation tree exceeds depth eight")
        next_level: list[tuple[GalleryObservationEncodedPage, int]] = []
        for offset in range(0, len(current), GALLERY_OBSERVATION_BRANCH_CAPACITY):
            group = current[offset : offset + GALLERY_OBSERVATION_BRANCH_CAPACITY]
            total = sum(count for _page, count in group)
            if total > _INT63_MAX:
                raise IntegerDomainError("gallery observation tree exceeds int63")
            branch = GalleryObservationPage(
                component,
                GalleryObservationNodeKind.BRANCH,
                level,
                total,
                tuple(
                    GalleryObservationBranchEntry(page.page_sha256, count)
                    for page, count in group
                ),
            )
            encoded_branch = _encode_page_value(branch)
            pages.append(encoded_branch)
            next_level.append((encoded_branch, total))
        current = next_level
        level += 1
    root, count = current[0]
    return GalleryObservationTree(component, root.page_sha256, count, tuple(pages))


def _chunk_exact_byte_stream(
    parts: Iterable[bytes],
) -> Iterator[GalleryObservationMetadataChunk]:
    buffer = bytearray()
    offset = 0
    for part in parts:
        exact = _require_bytes(part, field_name="metadata stream part")
        position = 0
        while position < len(exact):
            room = GALLERY_OBSERVATION_METADATA_CHUNK_BYTES - len(buffer)
            consumed = min(room, len(exact) - position)
            buffer.extend(exact[position : position + consumed])
            position += consumed
            if len(buffer) == GALLERY_OBSERVATION_METADATA_CHUNK_BYTES:
                yield GalleryObservationMetadataChunk(offset, bytes(buffer))
                offset += len(buffer)
                if offset > _INT63_MAX:
                    raise IntegerDomainError(
                        "metadata stream exceeds signed-int63 bytes"
                    )
                buffer.clear()
    if buffer:
        yield GalleryObservationMetadataChunk(offset, bytes(buffer))


def _validate_tree_leaf_sequence(
    tree: GalleryObservationTree,
    page_by_digest: dict[
        bytes, tuple[GalleryObservationEncodedPage, GalleryObservationPage]
    ],
) -> None:
    leaves: list[GalleryObservationPage] = []

    def collect(digest: bytes) -> None:
        page = page_by_digest[digest][1]
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            leaves.append(page)
            return
        for entry in page.entries:
            assert type(entry) is GalleryObservationBranchEntry
            collect(entry.child_sha256)

    collect(tree.root_page_sha256)
    if tree.item_count == 0:
        if tree.component is GalleryObservationComponent.METADATA:
            raise ByteDomainError("the fixed METADATA stream is never empty")
        if len(leaves) != 1 or leaves[0].entries:
            raise ByteDomainError("empty tree must be one empty leaf")
        return
    if any(not leaf.entries for leaf in leaves):
        raise ByteDomainError("nonempty tree must not contain an empty leaf")
    if tree.component is GalleryObservationComponent.METADATA:
        expected_offset = 0
        for index, leaf in enumerate(leaves):
            if len(leaf.entries) != 1:
                raise ByteDomainError("METADATA leaves must contain exactly one chunk")
            entry = leaf.entries[0]
            assert type(entry) is GalleryObservationMetadataChunk
            if entry.byte_offset != expected_offset:
                raise ByteDomainError("METADATA chunk offsets are not contiguous")
            if (
                index < len(leaves) - 1
                and len(entry.chunk_bytes) != GALLERY_OBSERVATION_METADATA_CHUNK_BYTES
            ):
                raise ByteDomainError(
                    "nonfinal METADATA chunk must be exactly 32768 bytes"
                )
            expected_offset += len(entry.chunk_bytes)
            if expected_offset > _INT63_MAX:
                raise IntegerDomainError("METADATA byte count exceeds signed-int63")
        if expected_offset != tree.item_count:
            raise ByteDomainError("METADATA byte count does not match chunk stream")
        validate_gallery_observation_metadata_parts(
            entry.chunk_bytes
            for leaf in leaves
            for entry in leaf.entries
            if type(entry) is GalleryObservationMetadataChunk
        )
        return
    capacity = _page_entry_capacity(tree.component, GalleryObservationNodeKind.LEAF)
    for leaf in leaves[:-1]:
        if len(leaf.entries) != capacity:
            raise ByteDomainError("every nonfinal leaf must be full")
    all_entries = tuple(entry for leaf in leaves for entry in leaf.entries)
    keys = tuple(_leaf_order_key(entry) for entry in all_entries)  # type: ignore[arg-type]
    if any(left >= right for left, right in zip(keys, keys[1:])):
        raise ByteDomainError("tree leaf records are not globally strictly ordered")
    if tree.component is GalleryObservationComponent.DIRECTORY:
        for ordinal, entry in enumerate(all_entries):
            assert type(entry) is GalleryObservationDirectoryEntry
            if entry.canonical_ordinal != ordinal:
                raise ByteDomainError("DIRECTORY ordinals are not globally canonical")
    elif tree.component is GalleryObservationComponent.FILE:
        for index, entry in enumerate(all_entries):
            assert type(entry) is GalleryObservationFileEntry
            if entry.file_no != index:
                raise ByteDomainError(
                    "FILE file_no is not globally zero-based contiguous"
                )
    elif tree.component is GalleryObservationComponent.TAG:
        for index, entry in enumerate(all_entries):
            assert type(entry) is GalleryObservationTagEntry
            if entry.position != index:
                raise ByteDomainError(
                    "TAG position is not globally zero-based contiguous"
                )


def _validate_tree_branch_shape(
    tree: GalleryObservationTree,
    page_by_digest: dict[
        bytes, tuple[GalleryObservationEncodedPage, GalleryObservationPage]
    ],
) -> None:
    """Prove the minimal deterministic fanout grouping at every tree level."""

    root = page_by_digest[tree.root_page_sha256][1]
    if root.node_kind is GalleryObservationNodeKind.LEAF:
        if root.level != 0:
            raise ByteDomainError("leaf root must be level zero")
        return
    if len(root.entries) < 2:
        raise ByteDomainError("canonical tree must not add a unary root branch")

    current = [root]
    expected_level = root.level
    while expected_level > 0:
        if any(
            page.node_kind is not GalleryObservationNodeKind.BRANCH
            or page.level != expected_level
            for page in current
        ):
            raise ByteDomainError("tree level mixes node kinds or levels")
        for page in current[:-1]:
            if len(page.entries) != GALLERY_OBSERVATION_BRANCH_CAPACITY:
                raise ByteDomainError("every nonfinal branch page must be full")
        if not 1 <= len(current[-1].entries) <= GALLERY_OBSERVATION_BRANCH_CAPACITY:
            raise ByteDomainError("final branch page has a noncanonical fanout")
        children: list[GalleryObservationPage] = []
        for page in current:
            for entry in page.entries:
                assert type(entry) is GalleryObservationBranchEntry
                children.append(page_by_digest[entry.child_sha256][1])
        expected_level -= 1
        if any(child.level != expected_level for child in children):
            raise ByteDomainError("branch child level is not parent level minus one")
        if expected_level == 0:
            if any(
                child.node_kind is not GalleryObservationNodeKind.LEAF
                for child in children
            ):
                raise ByteDomainError("level-zero children must be leaves")
            return
        current = children


def _validate_unbounded_utf8(value: str, *, field_name: str) -> None:
    if type(value) is not str:
        raise ByteDomainError(f"{field_name} must be str")
    # Incremental strict encoding catches surrogates without creating one
    # monolithic bytes object.
    for _part in _iter_strict_utf8(value, field_name=field_name):
        pass


def _iter_strict_utf8(value: str, *, field_name: str) -> Iterator[bytes]:
    if type(value) is not str:
        raise ByteDomainError(f"{field_name} must be str")
    for offset in range(0, len(value), 8192):
        try:
            yield value[offset : offset + 8192].encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise ByteDomainError(f"{field_name} must be strict UTF-8") from error


def _strict_utf8_size(value: str, *, field_name: str) -> int:
    size = sum(len(part) for part in _iter_strict_utf8(value, field_name=field_name))
    if size > _INT63_MAX:
        raise ByteDomainError(f"{field_name} exceeds signed-int63 UTF-8 bytes")
    return size


def _take_exact(
    payload: bytes,
    offset: int,
    size: int,
    field_name: str,
) -> tuple[bytes, int]:
    if size < 0 or len(payload) - offset < size:
        raise ByteDomainError(f"{field_name} is truncated")
    return payload[offset : offset + size], offset + size


def _take_uint(
    payload: bytes,
    offset: int,
    size: int,
    field_name: str,
) -> tuple[int, int]:
    value, next_offset = _take_exact(payload, offset, size, field_name)
    return int.from_bytes(value, "big"), next_offset


def _take_int64(
    payload: bytes,
    offset: int,
    field_name: str,
) -> tuple[int, int]:
    value, next_offset = _take_exact(payload, offset, 8, field_name)
    return int.from_bytes(value, "big", signed=True), next_offset


def _source_role_for_name(name_bytes: bytes) -> ArtifactSourceRole:
    return (
        ArtifactSourceRole.METADATA
        if name_bytes == METADATA_FILE_NAME
        else ArtifactSourceRole.CONTENT
    )


def _transform_kind_for_name(name_bytes: bytes) -> ArtifactTransformKind:
    _, separator, suffix = name_bytes.rpartition(b".")
    if not separator:
        return ArtifactTransformKind.RAW_COPY
    exact_suffix = b"." + suffix
    ascii_casefolded_suffix = exact_suffix.translate(
        bytes.maketrans(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ", b"abcdefghijklmnopqrstuvwxyz")
    )
    if ascii_casefolded_suffix == b".gif":
        return ArtifactTransformKind.GIF_NORMALIZE
    if ascii_casefolded_suffix in _JPEG_NORMALIZED_SUFFIXES:
        return ArtifactTransformKind.JPEG_NORMALIZE
    return ArtifactTransformKind.RAW_COPY


def _validate_archive_member_presence(
    *,
    excluded_flag: bool,
    archive_member_name_bytes: bytes | None,
) -> None:
    if excluded_flag:
        if archive_member_name_bytes is not None:
            raise ByteDomainError(
                "excluded member-plan entries must not have an archive member name"
            )
        return
    if archive_member_name_bytes is None:
        raise ByteDomainError(
            "non-excluded member-plan entries must have an archive member name"
        )
    name = _require_bytes(
        archive_member_name_bytes,
        field_name="archive_member_name_bytes",
    )
    if not name:
        raise ByteDomainError("archive_member_name_bytes must not be empty")
    if len(name) > _UINT32_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("archive_member_name_bytes exceeds u32 framing")


def _validate_artifact_member_plan_entries(
    entries: Sequence[ArtifactMemberPlanEntry],
) -> tuple[ArtifactMemberPlanEntry, ...]:
    if len(entries) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("artifact member plan has too many entries")
    validated: list[ArtifactMemberPlanEntry] = []
    emitted_names: set[bytes] = set()
    for expected_position, entry in enumerate(entries):
        if type(entry) is not ArtifactMemberPlanEntry:
            raise ByteDomainError(
                "artifact member plan entries must be ArtifactMemberPlanEntry"
            )
        if entry.entry_position != expected_position:
            raise ByteDomainError(
                "entry_position must equal its zero-based contiguous array index"
            )
        if entry.entry_kind is not ArtifactMemberEntryKind.SOURCE_FILE:
            raise ByteDomainError("v1 admits only SOURCE_FILE member-plan entries")
        if entry.source_role is not _source_role_for_name(entry.source_name_bytes):
            raise ByteDomainError(
                "source_role does not match the exact source filename"
            )
        if entry.transform_kind is not _transform_kind_for_name(
            entry.source_name_bytes
        ):
            raise ByteDomainError(
                "transform_kind does not match the exact ASCII-casefolded suffix"
            )
        _validate_archive_member_presence(
            excluded_flag=entry.excluded_flag,
            archive_member_name_bytes=entry.archive_member_name_bytes,
        )
        if entry.archive_member_name_bytes != artifact_archive_member_name(
            entry.entry_position,
            entry.source_role,
            entry.transform_kind,
            entry.excluded_flag,
        ):
            raise ByteDomainError(
                "archive member name does not match position/role/transform"
            )
        if entry.archive_member_name_bytes is not None:
            if entry.archive_member_name_bytes in emitted_names:
                raise ByteDomainError(
                    "emitted archive member names must be unique exact bytes"
                )
            emitted_names.add(entry.archive_member_name_bytes)
        validated.append(entry)
    return tuple(validated)


def _canonical_source_snapshot_galleries(
    galleries: Sequence[SourceSnapshotGallery],
) -> tuple[SourceSnapshotGallery, ...]:
    if len(galleries) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("source snapshot has too many gallery entries")
    if any(type(gallery) is not SourceSnapshotGallery for gallery in galleries):
        raise ByteDomainError("gallery entries must be SourceSnapshotGallery instances")
    ordered = sorted(galleries, key=lambda gallery: gallery.gallery_key)
    previous_key: bytes | None = None
    for gallery in ordered:
        if previous_key == gallery.gallery_key:
            raise ByteDomainError("duplicate source snapshot gallery_key")
        previous_key = gallery.gallery_key
    return tuple(ordered)


def _canonical_source_snapshot_decisions(
    decisions: Sequence[SourceSnapshotFileHashDecision],
    *,
    policy: SourceSnapshotPolicy,
) -> tuple[SourceSnapshotFileHashDecision, ...]:
    if len(decisions) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("source snapshot has too many file-hash decisions")
    if any(
        type(decision) is not SourceSnapshotFileHashDecision for decision in decisions
    ):
        raise ByteDomainError(
            "file-hash decisions must be SourceSnapshotFileHashDecision instances"
        )
    ordered = sorted(decisions, key=lambda decision: decision.file_sha256)
    previous_key: bytes | None = None
    for decision in ordered:
        if previous_key == decision.file_sha256:
            raise ByteDomainError("duplicate source snapshot file_sha256 decision")
        previous_key = decision.file_sha256
        expected_excluded = (
            decision.occurrence_count >= policy.spam_occurrence_threshold
            and decision.maximum_gallery_artist_count > 0
            and decision.artist_count
            > policy.spam_artist_threshold * decision.maximum_gallery_artist_count
        )
        if decision.excluded_flag is not expected_excluded:
            raise ByteDomainError(
                "excluded_flag does not match the frozen unbounded-integer predicate"
            )
    return tuple(ordered)


def _canonical_source_snapshot_owners(
    owners: Sequence[SourceSnapshotContentOwner],
    *,
    galleries: Sequence[SourceSnapshotGallery],
) -> tuple[SourceSnapshotContentOwner, ...]:
    if len(owners) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("source snapshot has too many content owners")
    gallery_by_key = {gallery.gallery_key: gallery for gallery in galleries}
    expected_content_groups = {
        gallery.content_sha256
        for gallery in galleries
        if gallery.content_sha256 is not None
    }
    if any(type(owner) is not SourceSnapshotContentOwner for owner in owners):
        raise ByteDomainError(
            "content owners must be SourceSnapshotContentOwner instances"
        )
    ordered = sorted(owners, key=lambda owner: owner.content_sha256)
    seen_content: set[bytes] = set()
    seen_owner_galleries: set[bytes] = set()
    for owner in ordered:
        if owner.content_sha256 in seen_content:
            raise ByteDomainError("duplicate source snapshot content owner group")
        if owner.owner_gallery_key in seen_owner_galleries:
            raise ByteDomainError("one gallery cannot own multiple content groups")
        seen_content.add(owner.content_sha256)
        seen_owner_galleries.add(owner.owner_gallery_key)
        owner_gallery = gallery_by_key.get(owner.owner_gallery_key)
        if owner_gallery is None:
            raise ByteDomainError("content owner gallery is not a snapshot member")
        if owner_gallery.content_sha256 != owner.content_sha256:
            raise ByteDomainError(
                "content owner gallery does not belong to the declared content group"
            )
    if seen_content != expected_content_groups:
        raise ByteDomainError(
            "content owner section must cover every snapshot content group exactly once"
        )
    return tuple(ordered)


def _canonical_source_snapshot_winners(
    winners: Sequence[SourceSnapshotGidWinner],
    *,
    galleries: Sequence[SourceSnapshotGallery],
) -> tuple[SourceSnapshotGidWinner, ...]:
    if len(winners) > _UINT64_MAX:  # pragma: no cover - impossible in CPython
        raise ByteDomainError("source snapshot has too many GID winners")
    gallery_by_key = {gallery.gallery_key: gallery for gallery in galleries}
    expected_gid_groups = {gallery.gid for gallery in galleries}
    if any(type(winner) is not SourceSnapshotGidWinner for winner in winners):
        raise ByteDomainError("GID winners must be SourceSnapshotGidWinner instances")
    ordered = sorted(winners, key=lambda winner: winner.gid)
    seen_gids: set[int] = set()
    seen_winner_galleries: set[bytes] = set()
    for winner in ordered:
        if winner.gid in seen_gids:
            raise ByteDomainError("duplicate source snapshot GID winner group")
        if winner.winner_gallery_key in seen_winner_galleries:
            raise ByteDomainError("one gallery cannot win multiple GID groups")
        seen_gids.add(winner.gid)
        seen_winner_galleries.add(winner.winner_gallery_key)
        winner_gallery = gallery_by_key.get(winner.winner_gallery_key)
        if winner_gallery is None:
            raise ByteDomainError("GID winner gallery is not a snapshot member")
        if winner_gallery.gid != winner.gid:
            raise ByteDomainError(
                "GID winner gallery does not belong to the declared GID group"
            )
    if seen_gids != expected_gid_groups:
        raise ByteDomainError(
            "GID winner section must cover every snapshot GID group exactly once"
        )
    return tuple(ordered)


def _validate_source_snapshot_counts(
    counts: SourceSnapshotCounts,
    *,
    galleries: Sequence[SourceSnapshotGallery],
) -> None:
    actual_gallery_count = len(galleries)
    actual_file_count = sum(gallery.file_count for gallery in galleries)
    actual_byte_count = sum(gallery.byte_count for gallery in galleries)
    if actual_file_count > _UINT64_MAX or actual_byte_count > _UINT64_MAX:
        raise IntegerDomainError("source snapshot aggregate sum exceeds u64 framing")
    if (
        counts.gallery_count != actual_gallery_count
        or counts.file_count != actual_file_count
        or counts.byte_count != actual_byte_count
    ):
        raise ByteDomainError(
            "declared gallery_count/file_count/byte_count do not match the snapshot"
        )


def _require_uint(value: int, *, bits: int, field_name: str) -> int:
    maximum = (1 << bits) - 1
    if type(value) is not int or not 0 <= value <= maximum:
        raise IntegerDomainError(f"{field_name} must be an integer in [0, {maximum}]")
    return value


def _require_positive_uint(value: int, *, bits: int, field_name: str) -> int:
    maximum = (1 << bits) - 1
    if type(value) is not int or not 1 <= value <= maximum:
        raise IntegerDomainError(f"{field_name} must be an integer in [1, {maximum}]")
    return value


def _require_int63(value: int, *, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _INT63_MAX:
        raise IntegerDomainError(
            f"{field_name} must be an integer in [0, {_INT63_MAX}]"
        )
    return value


def _require_positive_int63(value: int, *, field_name: str) -> int:
    if type(value) is not int or not 1 <= value <= _INT63_MAX:
        raise IntegerDomainError(
            f"{field_name} must be an integer in [1, {_INT63_MAX}]"
        )
    return value


def _decode_canonical_positive_int63_ascii(
    value: bytes,
    *,
    field_name: str,
) -> int:
    if not value or any(character < 48 or character > 57 for character in value):
        raise ByteDomainError(f"{field_name} must be canonical ASCII decimal digits")
    if len(value) > 1 and value[0] == ord("0"):
        raise ByteDomainError(f"{field_name} must not contain a leading zero")
    return _require_positive_int63(int(value), field_name=field_name)


def _require_int64(value: int, *, field_name: str) -> int:
    if type(value) is not int or not _INT64_MIN <= value <= _INT64_MAX:
        raise IntegerDomainError(
            f"{field_name} must be an integer in [{_INT64_MIN}, {_INT64_MAX}]"
        )
    return value


def _require_registered_version(
    value: int,
    *,
    registered: int,
    field_name: str,
) -> int:
    validated = _require_positive_uint(value, bits=32, field_name=field_name)
    if validated != registered:
        raise IntegerDomainError(f"{field_name} {validated} is not registered")
    return validated


def _require_bytes(value: bytes, *, field_name: str) -> bytes:
    if type(value) is not bytes:
        raise ByteDomainError(f"{field_name} must be immutable bytes")
    return value


def _require_fixed_bytes(value: bytes, *, length: int, field_name: str) -> bytes:
    if type(value) is not bytes or len(value) != length:
        raise ByteDomainError(f"{field_name} must be exactly {length} immutable bytes")
    return value


def _require_digest(value: bytes, *, field_name: str) -> bytes:
    if type(value) is not bytes or len(value) != _SHA256_BYTES:
        raise DigestFormatError(f"{field_name} must be exactly 32 immutable bytes")
    return value


def _validate_utf8_leaf(
    value: str,
    *,
    field_name: str,
    maximum_bytes: int,
) -> bytes:
    encoded = _validate_utf8_bytes(
        value,
        field_name=field_name,
        maximum_bytes=maximum_bytes,
    )
    return _validate_leaf_bytes(
        encoded,
        field_name=field_name,
        maximum_bytes=maximum_bytes,
    )


def _validate_utf8_bytes(
    value: str,
    *,
    field_name: str,
    maximum_bytes: int,
) -> bytes:
    if type(value) is not str:
        raise ByteDomainError(f"{field_name} must be str")
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeEncodeError as exc:
        raise ByteDomainError(f"{field_name} must be valid UTF-8") from exc
    if len(encoded) > maximum_bytes:
        raise ByteDomainError(f"{field_name} exceeds {maximum_bytes} bytes")
    return encoded


def _require_exact_utf8(value: bytes, *, field_name: str) -> str:
    try:
        return value.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ByteDomainError(f"{field_name} must be exact UTF-8") from exc


def _validate_ascii_identifier_bytes(
    value: str,
    *,
    field_name: str,
    maximum_bytes: int,
) -> bytes:
    if type(value) is not str:
        raise RegisteredIdentifierError(f"{field_name} must be str")
    try:
        encoded = value.encode("ascii", "strict")
    except UnicodeEncodeError as exc:
        raise RegisteredIdentifierError(f"{field_name} must be exact ASCII") from exc
    return _validate_leaf_bytes(
        encoded,
        field_name=field_name,
        maximum_bytes=maximum_bytes,
        error_type=RegisteredIdentifierError,
    )


def _validate_leaf_bytes(
    value: bytes,
    *,
    field_name: str,
    maximum_bytes: int,
    error_type: type[ByteDomainError] = ByteDomainError,
) -> bytes:
    if type(value) is not bytes:
        raise error_type(f"{field_name} must be immutable bytes")
    if not value:
        raise error_type(f"{field_name} must not be empty")
    if len(value) > maximum_bytes:
        raise error_type(f"{field_name} exceeds {maximum_bytes} bytes")
    if b"\x00" in value:
        raise error_type(f"{field_name} must not contain NUL")
    if value in {b".", b".."}:
        raise error_type(f"{field_name} must not be a dot component")
    if b"/" in value or b"\\" in value:
        raise error_type(f"{field_name} must be a direct-child leaf")
    return value


def _validate_posix_leaf_bytes(
    value: bytes,
    *,
    field_name: str,
    maximum_bytes: int,
) -> bytes:
    if type(value) is not bytes:
        raise ByteDomainError(f"{field_name} must be immutable bytes")
    if not value:
        raise ByteDomainError(f"{field_name} must not be empty")
    if len(value) > maximum_bytes:
        raise ByteDomainError(f"{field_name} exceeds {maximum_bytes} bytes")
    if b"\x00" in value:
        raise ByteDomainError(f"{field_name} must not contain NUL")
    if value in {b".", b".."}:
        raise ByteDomainError(f"{field_name} must not be a dot component")
    if b"/" in value:
        raise ByteDomainError(f"{field_name} must be a POSIX direct-child leaf")
    return value
