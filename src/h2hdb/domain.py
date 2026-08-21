from __future__ import annotations

__all__ = [
    "CANONICAL_SOURCE_MANIFEST_VERSION",
    "CatalogAnalysisPhase",
    "CatalogAnalysisPhaseCheckpoint",
    "CatalogBuild",
    "CatalogBuildBatchResult",
    "CatalogBuildPhase",
    "CatalogBuildPublishResult",
    "CatalogBuildPruneResult",
    "CatalogBuildProjection",
    "CatalogBuildProjectionBatchResult",
    "CatalogBuildProjectionPhase",
    "CatalogBuildProjectionPruneResult",
    "CatalogBuildProjectionPublishResult",
    "CatalogBuildOperationalPhase",
    "CatalogBuildOperationalState",
    "CatalogBuildSourcePage",
    "CatalogGalleryStageProgress",
    "CatalogPendingGalleryPage",
    "CatalogSourceGalleryAnalysis",
    "CatalogSourceGalleryCompletion",
    "CatalogSourceDiscoveryCompletion",
    "CatalogSourceGalleryDiscovery",
    "CatalogSourceGalleryRecord",
    "CatalogSourceGalleryHeader",
    "CatalogSourceFileCursor",
    "CatalogSourceFileChunk",
    "CatalogSourceFilePage",
    "CatalogSnapshot",
    "CatalogArtifact",
    "CatalogContributor",
    "CatalogPage",
    "CatalogPublishResult",
    "CatalogPublication",
    "CatalogPublicationSelection",
    "CatalogRevision",
    "CatalogSourcePage",
    "CatalogSourceRevision",
    "CatalogSubject",
    "DirectoryObservation",
    "CatalogPreparedArtifact",
    "CatalogProjectionArtifactCursor",
    "CatalogProjectionArtifactPage",
    "CatalogProjectionCheckpoint",
    "CatalogProjectionPublicationReceipt",
    "CatalogProjectionPublicationState",
    "CatalogProjectionSelectedFile",
    "CatalogProjectionSelectedFileCursor",
    "CatalogProjectionSelectedFilePage",
    "CatalogProjectionSelectedGallery",
    "CatalogProjectionSelectedGalleryCursor",
    "CatalogProjectionSelectedGalleryPage",
    "CatalogProjectionSelection",
    "CatalogProjectionSelectionCursor",
    "CatalogProjectionSelectionPage",
    "CatalogContentCandidateCursor",
    "CatalogContentCandidatePage",
    "CatalogContentCandidateRow",
    "CatalogContentDigest",
    "CatalogContentOwner",
    "CatalogDeduplicationCandidate",
    "CatalogFileHashAggregate",
    "CatalogFileHashAggregatePage",
    "CatalogFileSpamPageApplyResult",
    "CatalogFinalAnalysisCursor",
    "CatalogFinalAnalysisPage",
    "CatalogGalleryFileHashCursor",
    "CatalogGalleryFileHashPage",
    "CatalogGalleryFileHashRow",
    "CatalogGidCandidateCursor",
    "CatalogGidCandidatePage",
    "CatalogGidCandidateRow",
    "CatalogGidWinner",
    "CatalogSourceManifest",
    "CatalogSourceManifestCursor",
    "CatalogSourceManifestPage",
    "CatalogSourceManifestRow",
    "DownloadCandidateState",
    "FileContentReceipt",
    "FileObservation",
    "GallerySourceFile",
    "GallerySourceRecord",
    "GalleryTag",
    "ArtifactReleaseStorageEvidence",
    "ArtifactStorageEvidence",
    "FileHashCacheEntry",
    "FileHashCacheKey",
    "SchemaCompatibility",
    "TagObservation",
    "VNextArtifactProducer",
    "VNextArtifactStoragePolicy",
    "VNextArtifactZipPolicy",
    "VNextCurrentProjectionItem",
    "VNextCurrentProjectionPage",
    "VNextIngestAdvanceResult",
    "VNextIngestCompletionReceipt",
    "VNextIngestGalleryObservation",
    "VNextIngestCursor",
    "VNextIngestPage",
    "VNextIngestPhase",
    "VNextIngestPolicy",
    "VNextIngestSession",
    "VNextIngestSourceReceipt",
    "VNextResolvedIngestPolicy",
]

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from unicodedata import unidata_version
from uuid import UUID

from .vnext_domains import (
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
)
from .vnext_identity import (
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    artifact_locator_components,
    artifact_policy_digest,
    artifact_producer_fingerprint_sha256,
    canonical_value_digest,
    encode_source_relative_locator,
    publication_key,
    validate_file_name,
    validate_namespace,
)

CANONICAL_SOURCE_MANIFEST_VERSION = 1

_FILE_CONTENT_RECEIPT_TOKEN = object()
_TAG_VALUE_DOMAIN = "tag_value_utf8_v1"

VNextIngestCursor = tuple[str, ...] | bytes | int


def _validate_sha256(value: str, *, label: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{label} must contain 64 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"{label} is not hexadecimal") from error


def _validate_leaf_name(value: str, *, label: str) -> None:
    if not value or value in {".", ".."}:
        raise ValueError(f"{label} must not be blank or a traversal segment")
    if "/" in value or "\\" in value or Path(value).name != value:
        raise ValueError(f"{label} must be a single leaf name")


def _validate_build_id(value: str) -> None:
    try:
        parsed = UUID(value)
    except ValueError as error:
        raise ValueError("Catalog build ID must be a UUID") from error
    if parsed.hex != value:
        raise ValueError("Catalog build ID must be a normalized 32-character UUID")


class CatalogBuildPhase(StrEnum):
    discovering = "DISCOVERING"
    staging = "STAGING"
    analyzing = "ANALYZING"
    artifacts = "ARTIFACTS"
    sealed = "SEALED"
    published = "PUBLISHED"
    abandoned = "ABANDONED"


class CatalogBuildProjectionPhase(StrEnum):
    """Durable phases for an invisible, revision-keyed catalog projection."""

    preparing_artifacts = "PREPARING_ARTIFACTS"
    staging_selections = "STAGING_SELECTIONS"
    complete = "COMPLETE"
    sealed = "SEALED"
    published = "PUBLISHED"


class CatalogBuildOperationalPhase(StrEnum):
    """Bounded preparation phases for operational source cutover effects."""

    normalizing_times = "NORMALIZING_TIMES"
    removed_gid_requests = "REMOVED_GID_REQUESTS"
    deletion_consumptions = "DELETION_CONSUMPTIONS"
    complete = "COMPLETE"


class CatalogProjectionPublicationState(StrEnum):
    database_committed = "DB_COMMITTED"
    projection_finalized = "PROJECTION_FINALIZED"


class CatalogAnalysisPhase(StrEnum):
    """Durable, ordered reducers used to analyze one staged source build."""

    source_manifests = "SOURCE_MANIFESTS"
    file_spam = "FILE_SPAM"
    content_digests = "CONTENT_DIGESTS"
    content_owners = "CONTENT_OWNERS"
    gid_winners = "GID_WINNERS"
    final_analyses = "FINAL_ANALYSES"


@dataclass(frozen=True, slots=True)
class CatalogAnalysisPhaseCheckpoint:
    build_id: str
    phase: CatalogAnalysisPhase
    completed_at: datetime
    applied: bool

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)


@dataclass(frozen=True, slots=True)
class CatalogBuild:
    """Durable source-snapshot build state.

    ``build_id`` is deliberately independent of an ingest generation. A lease
    reclaim can bind a new turn token to the same durable build without
    changing the identity of already staged rows.
    """

    build_id: str
    scope_key: str
    phase: CatalogBuildPhase
    ingest_generation: int
    base_source_revision: int
    base_active_build_id: str | None
    discovered_gallery_count: int
    expected_gallery_count: int | None
    staged_gallery_count: int
    staged_file_count: int
    analyzed_gallery_count: int
    created_at: datetime
    updated_at: datetime
    published_source_revision: int | None = None
    seal_sha256: str | None = None
    discovery_epoch: str | None = None
    discovery_tree_sha256: str | None = None

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if not self.scope_key:
            raise ValueError("Catalog build scope key must not be blank")
        if self.base_active_build_id is not None:
            _validate_build_id(self.base_active_build_id)
        if self.ingest_generation < 0 or self.base_source_revision < 0:
            raise ValueError(
                "Catalog build generations and revisions must be non-negative"
            )
        counts = (
            self.discovered_gallery_count,
            self.staged_gallery_count,
            self.staged_file_count,
            self.analyzed_gallery_count,
        )
        if min(counts) < 0:
            raise ValueError("Catalog build counts must be non-negative")
        if self.expected_gallery_count is not None and self.expected_gallery_count < 0:
            raise ValueError("Expected gallery count must be non-negative")
        if (
            self.published_source_revision is not None
            and self.published_source_revision <= 0
        ):
            raise ValueError("Published source revision must be positive")
        if self.seal_sha256 is not None:
            _validate_sha256(self.seal_sha256, label="Catalog build seal SHA-256")
        if self.discovery_epoch is not None and not self.discovery_epoch:
            raise ValueError("Catalog discovery epoch must not be blank")
        if self.discovery_tree_sha256 is not None:
            _validate_sha256(
                self.discovery_tree_sha256,
                label="Catalog discovery tree SHA-256",
            )


@dataclass(frozen=True, slots=True)
class CatalogBuildBatchResult:
    build_id: str
    batch_id: str
    applied: bool
    item_count: int
    file_count: int = 0

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if not self.batch_id:
            raise ValueError("Catalog build batch ID must not be blank")
        if self.item_count < 0 or self.file_count < 0:
            raise ValueError("Catalog build batch counts must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogGalleryStageProgress:
    gallery_name: str
    source_locator: str
    header_staged: bool
    staged_file_count: int

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if not self.source_locator:
            raise ValueError("Gallery source locator must not be blank")
        if self.staged_file_count < 0:
            raise ValueError("Staged source file count must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogPendingGalleryPage:
    build: CatalogBuild
    galleries: tuple[CatalogGalleryStageProgress, ...]
    after_gallery_name: str | None
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        if self.limit <= 0:
            raise ValueError("Pending gallery page limit must be positive")


@dataclass(frozen=True, slots=True)
class FileHashCacheKey:
    """Opaque source locator and change fingerprint owned by the producer."""

    source_key: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not self.source_key:
            raise ValueError("File hash cache source key must not be blank")
        if not self.fingerprint:
            raise ValueError("File hash cache fingerprint must not be blank")


@dataclass(frozen=True, slots=True)
class FileHashCacheEntry:
    key: FileHashCacheKey
    sha256: str

    def __post_init__(self) -> None:
        _validate_sha256(self.sha256, label="Cached file SHA-256")


@dataclass(frozen=True, slots=True)
class CatalogBuildPublishResult:
    """Result of atomically activating an immutable *source* snapshot.

    This descriptor intentionally does not claim that a user-facing catalog
    projection has also been prepared. Publication projection staging is a
    separate workflow from this initial source-build vertical slice.
    """

    build: CatalogBuild
    source_revision: int
    previous_build_id: str | None

    def __post_init__(self) -> None:
        if self.source_revision <= 0:
            raise ValueError("Published source revision must be positive")
        if self.previous_build_id is not None:
            _validate_build_id(self.previous_build_id)


@dataclass(frozen=True, slots=True)
class CatalogBuildPruneResult:
    build_id: str
    deleted_rows: int
    complete: bool

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if self.deleted_rows < 0:
            raise ValueError("Deleted catalog build row count must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildProjection:
    """Descriptor for one invisible catalog revision prepared from a source build."""

    build_id: str
    reserved_revision: int
    base_catalog_revision: int
    artifacts_required: bool
    phase: CatalogBuildProjectionPhase
    artifact_after_gallery_key: str | None
    selection_after_gallery_key: str | None
    selected_gallery_count: int
    protected_artifact_count: int
    staged_selection_count: int
    projection_chain_sha256: str
    projection_xor_sha256: str
    projection_sum_sha256: str
    projection_sha256: str | None
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    created_at: datetime
    updated_at: datetime
    published_catalog_revision: int | None = None

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if self.reserved_revision <= 0:
            raise ValueError("A reserved catalog revision must be positive")
        if self.base_catalog_revision < 0:
            raise ValueError("A base catalog revision must not be negative")
        for cursor in (
            self.artifact_after_gallery_key,
            self.selection_after_gallery_key,
        ):
            if cursor is not None:
                _validate_sha256(cursor, label="Projection gallery cursor")
        counts = (
            self.selected_gallery_count,
            self.protected_artifact_count,
            self.staged_selection_count,
            self.new_galleries,
            self.changed_galleries,
            self.removed_galleries,
            self.duplicate_losers,
        )
        if min(counts) < 0:
            raise ValueError("Catalog projection counts must be non-negative")
        if self.projection_sha256 is not None:
            _validate_sha256(
                self.projection_sha256,
                label="Catalog projection SHA-256",
            )
        _validate_sha256(
            self.projection_chain_sha256,
            label="Catalog projection staging chain SHA-256",
        )
        _validate_sha256(
            self.projection_xor_sha256,
            label="Catalog projection XOR accumulator",
        )
        _validate_sha256(
            self.projection_sum_sha256,
            label="Catalog projection sum accumulator",
        )
        if (
            self.published_catalog_revision is not None
            and self.published_catalog_revision < 0
        ):
            raise ValueError("Published catalog revision must not be negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildProjectionBatchResult:
    build_id: str
    batch_id: str
    applied: bool
    item_count: int

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if not self.batch_id:
            raise ValueError("Catalog projection batch ID must not be blank")
        if self.item_count < 0:
            raise ValueError("Catalog projection batch count must not be negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildProjectionPruneResult:
    build_id: str
    deleted_rows: int
    complete: bool

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if self.deleted_rows < 0:
            raise ValueError("Deleted projection row count must not be negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildOperationalState:
    """Restartable checkpoint for one build's invisible operational effects."""

    build_id: str
    preparation_id: str
    phase: CatalogBuildOperationalPhase
    deletion_request_generation: int
    after_gallery_key: str | None
    after_gid: int | None
    normalized_gallery_count: int
    removed_gid_request_count: int
    deletion_consumption_count: int
    prepared_at: datetime
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        _validate_build_id(self.preparation_id)
        if self.deletion_request_generation < 0:
            raise ValueError("Deletion request generation must be non-negative")
        if self.after_gallery_key is not None:
            _validate_sha256(
                self.after_gallery_key,
                label="Operational gallery checkpoint",
            )
        if self.after_gid is not None and self.after_gid <= 0:
            raise ValueError("Operational GID checkpoint must be positive")
        if (
            min(
                self.normalized_gallery_count,
                self.removed_gid_request_count,
                self.deletion_consumption_count,
            )
            < 0
        ):
            raise ValueError("Operational preparation counts must be non-negative")
        if (self.phase is CatalogBuildOperationalPhase.complete) != (
            self.completed_at is not None
        ):
            raise ValueError(
                "Only a complete operational preparation has a completion time"
            )

    @property
    def complete(self) -> bool:
        return self.phase is CatalogBuildOperationalPhase.complete


@dataclass(frozen=True, slots=True)
class CatalogProjectionCheckpoint:
    build_id: str
    phase: CatalogBuildProjectionPhase
    artifact_after_gallery_key: str | None
    selection_after_gallery_key: str | None

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        for cursor in (
            self.artifact_after_gallery_key,
            self.selection_after_gallery_key,
        ):
            if cursor is not None:
                _validate_sha256(cursor, label="Projection gallery cursor")


@dataclass(frozen=True, order=True, slots=True)
class CatalogProjectionSelectedGalleryCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Selected gallery cursor")


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelectedGallery:
    gallery_key: str
    gallery_name: str
    source_locator: str
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    page_count: int
    tags: tuple[GalleryTag, ...]
    metadata_sha256: str
    source_manifest_sha256: str
    content_sha256: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        _validate_sha256(self.gallery_key, label="Selected gallery key")
        _validate_leaf_name(self.gallery_name, label="Selected gallery name")
        if not self.source_locator:
            raise ValueError("Selected gallery source locator must not be blank")
        if self.gid <= 0 or self.page_count < 0:
            raise ValueError("Selected gallery GID/pages are invalid")
        _validate_sha256(self.metadata_sha256, label="Gallery metadata SHA-256")
        _validate_sha256(
            self.source_manifest_sha256,
            label="Gallery source manifest SHA-256",
        )
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Gallery content SHA-256")

    @property
    def cursor(self) -> CatalogProjectionSelectedGalleryCursor:
        return CatalogProjectionSelectedGalleryCursor(self.gallery_key)


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelectedGalleryPage:
    items: tuple[CatalogProjectionSelectedGallery, ...]
    next_cursor: CatalogProjectionSelectedGalleryCursor | None
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Selected gallery page must honor its positive limit")


@dataclass(frozen=True, order=True, slots=True)
class CatalogProjectionSelectedFileCursor:
    file_sort_key: str
    file_name: str
    file_key: str


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelectedFile:
    gallery_key: str
    file_key: str
    file_sort_key: str
    file_name: str
    relative_locator: str
    device: int
    inode: int
    modified_ns: int
    changed_ns: int
    size_bytes: int
    sha256: str
    excluded: bool

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Selected gallery key")
        _validate_sha256(self.file_key, label="Selected file key")
        _validate_leaf_name(self.file_name, label="Selected source file name")
        if not self.relative_locator:
            raise ValueError("Selected source file locator must not be blank")
        if min(self.device, self.inode, self.size_bytes) < 0:
            raise ValueError("Selected source file stat values must be non-negative")
        _validate_sha256(self.sha256, label="Selected source file SHA-256")

    @property
    def cursor(self) -> CatalogProjectionSelectedFileCursor:
        return CatalogProjectionSelectedFileCursor(
            self.file_sort_key,
            self.file_name,
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelectedFilePage:
    items: tuple[CatalogProjectionSelectedFile, ...]
    next_cursor: CatalogProjectionSelectedFileCursor | None
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Selected file page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogPreparedArtifact:
    gallery_key: str
    artifact: CatalogArtifact

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Prepared artifact gallery key")


@dataclass(frozen=True, order=True, slots=True)
class CatalogProjectionSelectionCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Projection selection cursor")


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelection:
    gallery_key: str
    artifact: CatalogArtifact | None = None
    redownload_required: bool = False

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Projection selection gallery key")

    @property
    def cursor(self) -> CatalogProjectionSelectionCursor:
        return CatalogProjectionSelectionCursor(self.gallery_key)


@dataclass(frozen=True, slots=True)
class CatalogProjectionSelectionPage:
    items: tuple[CatalogProjectionSelection, ...]
    next_cursor: CatalogProjectionSelectionCursor | None
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Projection selection page must honor its positive limit")


@dataclass(frozen=True, order=True, slots=True)
class CatalogProjectionArtifactCursor:
    artifact_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.artifact_key, label="Published artifact cursor")


@dataclass(frozen=True, slots=True)
class CatalogPublishedArtifact:
    """Pinned artifact plus the source identity needed for crash recovery."""

    artifact: CatalogArtifact
    gallery_name: str
    gid: int
    upload_time: datetime

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Published artifact gallery name")
        if self.gid <= 0:
            raise ValueError("Published artifact GID must be positive")


@dataclass(frozen=True, slots=True)
class CatalogProjectionArtifactPage:
    revision: CatalogRevision
    items: tuple[CatalogPublishedArtifact, ...]
    next_cursor: CatalogProjectionArtifactCursor | None
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Published artifact page must honor its positive limit")

    @property
    def artifacts(self) -> tuple[CatalogArtifact, ...]:
        """Compatibility view for consumers that need only neutral artifacts."""

        return tuple(item.artifact for item in self.items)


@dataclass(frozen=True, slots=True)
class CatalogProjectionPublicationReceipt:
    build_id: str
    source_revision: int
    catalog_revision: CatalogRevision
    projection_sha256: str
    state: CatalogProjectionPublicationState
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    selected_galleries: int
    committed_at: datetime
    finalized_at: datetime | None = None

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        if self.source_revision <= 0:
            raise ValueError("Published source revision must be positive")
        _validate_sha256(self.projection_sha256, label="Projection SHA-256")
        if (
            min(
                self.new_galleries,
                self.changed_galleries,
                self.removed_galleries,
                self.duplicate_losers,
                self.selected_galleries,
            )
            < 0
        ):
            raise ValueError("Projection publication counts must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildProjectionPublishResult:
    build: CatalogBuild
    projection: CatalogBuildProjection
    receipt: CatalogProjectionPublicationReceipt


@dataclass(frozen=True, slots=True)
class GallerySourceFile:
    name: str
    size_bytes: int
    sha256: str
    relative_locator: str | None = None
    device: int | None = None
    inode: int | None = None
    modified_ns: int | None = None
    changed_ns: int | None = None

    def __post_init__(self) -> None:
        _validate_leaf_name(self.name, label="Source file name")
        if len(self.name) > 255:
            raise ValueError("Source file name must not exceed 255 characters")
        if self.size_bytes < 0:
            raise ValueError("Source file size must not be negative")
        _validate_sha256(self.sha256, label="Source file SHA-256")
        observations = (
            self.device,
            self.inode,
            self.modified_ns,
            self.changed_ns,
        )
        if self.relative_locator is None and any(
            value is not None for value in observations
        ):
            raise ValueError("A source file stat signature requires a relative locator")
        if self.relative_locator is not None:
            if not self.relative_locator:
                raise ValueError("Source file relative locator must not be blank")
            if any(value is None for value in observations):
                raise ValueError(
                    "A source file relative locator requires a complete stat signature"
                )
        if any(value is not None and value < 0 for value in (self.device, self.inode)):
            raise ValueError("Source file device and inode must be non-negative")


@dataclass(frozen=True, slots=True)
class GalleryTag:
    name: str
    value: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Gallery tag name must not be blank")
        if len(self.name) > 191 or len(self.value) > 191:
            raise ValueError(
                "Gallery tag name and value must not exceed 191 characters"
            )


@dataclass(frozen=True, slots=True)
class GallerySourceRecord:
    gallery_name: str
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    tags: tuple[GalleryTag, ...]
    files: tuple[GallerySourceFile, ...]
    source_manifest_sha256: str
    source_manifest_version: int = 1
    source_file_count: int | None = None
    source_locator: str | None = None
    content_sha256: str | None = None
    duplicate_of_gallery_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        object.__setattr__(self, "files", tuple(self.files))
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if len(self.gallery_name) > 255:
            raise ValueError("Gallery name must not exceed 255 characters")
        if self.gid <= 0:
            raise ValueError("Gallery GID must be positive")
        _validate_sha256(
            self.source_manifest_sha256,
            label="Gallery source manifest SHA-256",
        )
        if self.source_manifest_version <= 0:
            raise ValueError("Gallery source manifest version must be positive")
        if self.source_locator is not None and not self.source_locator:
            raise ValueError("Gallery source locator must not be blank")
        if self.source_file_count is None:
            object.__setattr__(self, "source_file_count", len(self.files))
        elif self.source_file_count < 0:
            raise ValueError("Gallery source file count must be non-negative")
        elif self.files and self.source_file_count != len(self.files):
            raise ValueError(
                "Materialized gallery files must match the source file count"
            )
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Gallery content SHA-256")
        if self.duplicate_of_gallery_name is not None and self.content_sha256 is None:
            raise ValueError("A duplicate gallery must have a content SHA-256")
        file_names = [source_file.name for source_file in self.files]
        if len(file_names) != len(set(file_names)):
            raise ValueError("Gallery source files must have unique leaf names")
        tag_pairs = [(tag.name, tag.value) for tag in self.tags]
        if len(tag_pairs) != len(set(tag_pairs)):
            raise ValueError("Gallery tags must be unique")


@dataclass(frozen=True, slots=True)
class CatalogSourceGalleryHeader:
    """Bounded gallery metadata staged before any source-file chunks."""

    gallery_name: str
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    tags: tuple[GalleryTag, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if len(self.gallery_name) > 255:
            raise ValueError("Gallery name must not exceed 255 characters")
        if self.gid <= 0:
            raise ValueError("Gallery GID must be positive")
        tag_pairs = [(tag.name, tag.value) for tag in self.tags]
        if len(tag_pairs) != len(set(tag_pairs)):
            raise ValueError("Gallery tags must be unique")


@dataclass(frozen=True, slots=True)
class CatalogSourceGalleryDiscovery:
    """One source folder, keyed by a scope-unique gallery leaf name."""

    gallery_name: str
    source_locator: str
    metadata_fingerprint: str | None = None

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if not self.source_locator:
            raise ValueError("Gallery source locator must not be blank")
        if self.metadata_fingerprint is not None and not self.metadata_fingerprint:
            raise ValueError("Gallery metadata fingerprint must not be blank")


@dataclass(frozen=True, slots=True)
class CatalogSourceDiscoveryCompletion:
    scan_attempt: str
    gallery_count: int
    tree_observation_sha256: str

    def __post_init__(self) -> None:
        if not self.scan_attempt:
            raise ValueError("Catalog discovery scan attempt must not be blank")
        if self.gallery_count < 0:
            raise ValueError("Catalog discovery gallery count must be non-negative")
        _validate_sha256(
            self.tree_observation_sha256,
            label="Catalog discovery tree observation SHA-256",
        )


@dataclass(frozen=True, slots=True)
class CatalogSourceGalleryCompletion:
    gallery_name: str
    expected_file_count: int
    scan_observation_sha256: str
    scan_observation_version: int
    raw_content_sha256: str | None = None
    metadata_sha256: str | None = None
    page_count: int | None = None
    directory_entry_count: int | None = None
    directory_observation_sha256: str | None = None
    canonical_source_manifest_sha256: str | None = None
    canonical_source_manifest_version: int | None = None

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if self.expected_file_count < 0:
            raise ValueError("Expected source file count must be non-negative")
        _validate_sha256(
            self.scan_observation_sha256,
            label="Gallery scan observation SHA-256",
        )
        if self.scan_observation_version <= 0:
            raise ValueError("Gallery scan observation version must be positive")
        if self.raw_content_sha256 is not None:
            _validate_sha256(
                self.raw_content_sha256,
                label="Raw gallery content SHA-256",
            )
        if (self.canonical_source_manifest_sha256 is None) != (
            self.canonical_source_manifest_version is None
        ):
            raise ValueError(
                "Canonical source manifest digest and version must be supplied together"
            )
        if self.canonical_source_manifest_sha256 is not None:
            _validate_sha256(
                self.canonical_source_manifest_sha256,
                label="Canonical source manifest SHA-256",
            )
        if (
            self.canonical_source_manifest_version is not None
            and self.canonical_source_manifest_version
            != CANONICAL_SOURCE_MANIFEST_VERSION
        ):
            raise ValueError(
                "Canonical source manifest version must be "
                f"{CANONICAL_SOURCE_MANIFEST_VERSION}"
            )
        for label, digest in (
            ("Gallery metadata SHA-256", self.metadata_sha256),
            (
                "Gallery directory observation SHA-256",
                self.directory_observation_sha256,
            ),
        ):
            if digest is not None:
                _validate_sha256(digest, label=label)
        for label, count in (
            ("Gallery page count", self.page_count),
            ("Gallery directory entry count", self.directory_entry_count),
        ):
            if count is not None and count < 0:
                raise ValueError(f"{label} must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogSourceGalleryAnalysis:
    gallery_name: str
    content_sha256: str | None
    selected: bool
    duplicate_of_gallery_name: str | None = None
    source_manifest_sha256: str | None = None
    source_manifest_version: int | None = None
    gallery_key: str | None = None

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Gallery content SHA-256")
        if (self.source_manifest_sha256 is None) != (
            self.source_manifest_version is None
        ):
            raise ValueError(
                "Canonical source manifest digest and version must be supplied together"
            )
        if self.source_manifest_sha256 is not None:
            _validate_sha256(
                self.source_manifest_sha256,
                label="Canonical gallery source manifest SHA-256",
            )
        if (
            self.source_manifest_version is not None
            and self.source_manifest_version <= 0
        ):
            raise ValueError("Canonical source manifest version must be positive")
        if self.duplicate_of_gallery_name is not None:
            _validate_leaf_name(
                self.duplicate_of_gallery_name,
                label="Duplicate owner gallery name",
            )
            if self.duplicate_of_gallery_name == self.gallery_name:
                raise ValueError("A duplicate gallery cannot target itself")
            if self.content_sha256 is None:
                raise ValueError("A duplicate gallery must have a content SHA-256")
            if self.selected:
                raise ValueError("A duplicate gallery cannot be selected")
        if self.gallery_key is not None:
            _validate_sha256(self.gallery_key, label="Analyzed gallery key")


@dataclass(frozen=True, order=True, slots=True)
class CatalogSourceManifestCursor:
    gallery_key: str
    file_sort_key: str
    file_name: str
    file_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Manifest gallery cursor")


@dataclass(frozen=True, slots=True)
class CatalogSourceManifestRow:
    """One canonical-manifest input row, including empty-gallery sentinels."""

    gallery_name: str
    gallery_key: str
    file_sort_key: str
    file_name: str | None
    file_key: str
    size_bytes: int
    file_sha256: str
    empty_gallery_metadata_sha256: str | None = None

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        _validate_sha256(self.gallery_key, label="Manifest gallery key")
        if self.file_name is None:
            if any(
                (
                    self.file_sort_key,
                    self.file_key,
                    self.size_bytes,
                    self.file_sha256,
                )
            ):
                raise ValueError("An empty-gallery manifest sentinel must be empty")
            if self.empty_gallery_metadata_sha256 is None:
                raise ValueError(
                    "An empty-gallery manifest sentinel requires metadata SHA-256"
                )
            _validate_sha256(
                self.empty_gallery_metadata_sha256,
                label="Empty-gallery metadata SHA-256",
            )
            return
        _validate_leaf_name(self.file_name, label="Source file name")
        if self.file_sort_key != self.file_name.casefold():
            raise ValueError("Source file sort key must use Python casefold")
        _validate_sha256(self.file_key, label="Source file key")
        if self.size_bytes < 0:
            raise ValueError("Source file size must be non-negative")
        _validate_sha256(self.file_sha256, label="Source file SHA-256")
        if self.empty_gallery_metadata_sha256 is not None:
            raise ValueError("A source file row cannot carry sentinel metadata")

    @property
    def cursor(self) -> CatalogSourceManifestCursor:
        return CatalogSourceManifestCursor(
            self.gallery_key,
            self.file_sort_key,
            self.file_name or "",
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class CatalogSourceManifestPage:
    items: tuple[CatalogSourceManifestRow, ...]
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Manifest page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogSourceManifest:
    gallery_name: str
    source_manifest_sha256: str
    source_manifest_version: int = 1

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        _validate_sha256(
            self.source_manifest_sha256,
            label="Canonical source manifest SHA-256",
        )
        if self.source_manifest_version != 1:
            raise ValueError("Canonical staged manifests must use version 1")


@dataclass(frozen=True, slots=True)
class CatalogFileHashAggregate:
    file_sha256: str
    occurrence_count: int
    distinct_artist_count: int
    maximum_gallery_artist_count: int
    minimum_occurrences: int

    def __post_init__(self) -> None:
        _validate_sha256(self.file_sha256, label="Source file SHA-256")
        if self.occurrence_count <= 0:
            raise ValueError("File occurrence count must be positive")
        if self.minimum_occurrences <= 0:
            raise ValueError("File-hash minimum occurrences must be positive")
        if self.occurrence_count < self.minimum_occurrences:
            raise ValueError("File hash does not satisfy its stream occurrence bound")
        if min(self.distinct_artist_count, self.maximum_gallery_artist_count) < 0:
            raise ValueError("File artist counts must be non-negative")
        if self.maximum_gallery_artist_count > self.distinct_artist_count:
            raise ValueError("Maximum gallery artists cannot exceed distinct artists")


@dataclass(frozen=True, slots=True)
class CatalogFileHashAggregatePage:
    items: tuple[CatalogFileHashAggregate, ...]
    limit: int
    minimum_occurrences: int
    checkpoint_generation: int
    start_cursor_sha256: str
    next_cursor_sha256: str
    input_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("File-hash aggregate page must honor its positive limit")
        if self.minimum_occurrences <= 0:
            raise ValueError("File-spam minimum occurrences must be positive")
        if any(
            item.minimum_occurrences != self.minimum_occurrences for item in self.items
        ):
            raise ValueError("File-spam page rows belong to a different policy")
        if self.checkpoint_generation <= 0:
            raise ValueError("File-spam checkpoint generation must be positive")
        for label, value in (
            ("File-spam start cursor", self.start_cursor_sha256),
            ("File-spam next cursor", self.next_cursor_sha256),
        ):
            if value:
                _validate_sha256(value, label=label)
        _validate_sha256(self.input_sha256, label="File-spam page input")
        expected_next = (
            self.items[-1].file_sha256 if self.items else self.start_cursor_sha256
        )
        if self.next_cursor_sha256 != expected_next:
            raise ValueError("File-spam page next cursor does not match its rows")

    @property
    def terminal(self) -> bool:
        return not self.items


@dataclass(frozen=True, slots=True)
class CatalogFileSpamPageApplyResult:
    build_id: str
    batch_key: str
    checkpoint_generation: int
    row_count: int
    excluded_count: int
    complete: bool
    applied: bool

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        _validate_sha256(self.batch_key, label="File-spam scan batch key")
        if self.checkpoint_generation <= 0:
            raise ValueError("File-spam checkpoint generation must be positive")
        if min(self.row_count, self.excluded_count) < 0:
            raise ValueError("File-spam page counts must be non-negative")
        if self.excluded_count > self.row_count:
            raise ValueError("Excluded hashes must belong to the applied page")
        if self.complete != (self.row_count == 0):
            raise ValueError("Only an empty file-spam page completes the scan")


@dataclass(frozen=True, order=True, slots=True)
class CatalogGalleryFileHashCursor:
    gallery_key: str
    file_sha256: str
    file_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Gallery file-hash cursor")


@dataclass(frozen=True, slots=True)
class CatalogGalleryFileHashRow:
    gallery_name: str
    gallery_key: str
    file_key: str
    file_sha256: str
    metadata_file: bool
    excluded_as_spam: bool

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        _validate_sha256(self.gallery_key, label="File-hash gallery key")
        if self.empty_gallery_sentinel:
            if self.metadata_file or self.excluded_as_spam:
                raise ValueError("An empty-gallery content sentinel must be empty")
            return
        _validate_sha256(self.file_key, label="Source file key")
        _validate_sha256(self.file_sha256, label="Source file SHA-256")

    @property
    def empty_gallery_sentinel(self) -> bool:
        return not self.file_key and not self.file_sha256

    @property
    def cursor(self) -> CatalogGalleryFileHashCursor:
        return CatalogGalleryFileHashCursor(
            self.gallery_key,
            self.file_sha256,
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class CatalogGalleryFileHashPage:
    items: tuple[CatalogGalleryFileHashRow, ...]
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Gallery file-hash page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogContentDigest:
    gallery_name: str
    content_sha256: str | None
    duplicate_hash_deletion_candidate: bool = False

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Content SHA-256")


@dataclass(frozen=True, slots=True)
class CatalogDeduplicationCandidate:
    """Neutral selection facts; ingest owns the Python policy itself."""

    gallery_name: str
    gid: int
    title: str
    download_time: datetime
    content_sha256: str | None
    tags: tuple[GalleryTag, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if self.gid <= 0:
            raise ValueError("Candidate GID must be positive")
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Content SHA-256")


@dataclass(frozen=True, order=True, slots=True)
class CatalogContentCandidateCursor:
    content_sha256: str
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.content_sha256, label="Content SHA-256")
        _validate_sha256(self.gallery_key, label="Content candidate gallery key")


@dataclass(frozen=True, slots=True)
class CatalogContentCandidateRow:
    candidate: CatalogDeduplicationCandidate
    incumbent_gallery_name: str | None
    gallery_key: str

    def __post_init__(self) -> None:
        if self.candidate.content_sha256 is None:
            raise ValueError("A content candidate requires a content SHA-256")
        _validate_sha256(self.gallery_key, label="Content candidate gallery key")

    @property
    def cursor(self) -> CatalogContentCandidateCursor:
        digest = self.candidate.content_sha256
        assert digest is not None
        return CatalogContentCandidateCursor(digest, self.gallery_key)


@dataclass(frozen=True, slots=True)
class CatalogContentCandidatePage:
    items: tuple[CatalogContentCandidateRow, ...]
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Content candidate page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogContentOwner:
    content_sha256: str
    owner_gallery_name: str

    def __post_init__(self) -> None:
        _validate_sha256(self.content_sha256, label="Content SHA-256")
        _validate_leaf_name(self.owner_gallery_name, label="Content owner gallery")


@dataclass(frozen=True, order=True, slots=True)
class CatalogGidCandidateCursor:
    gid: int
    gallery_key: str

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("Candidate GID must be positive")
        _validate_sha256(self.gallery_key, label="GID candidate gallery key")


@dataclass(frozen=True, slots=True)
class CatalogGidCandidateRow:
    candidate: CatalogDeduplicationCandidate
    incumbent_gallery_name: str | None
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="GID candidate gallery key")

    @property
    def cursor(self) -> CatalogGidCandidateCursor:
        return CatalogGidCandidateCursor(
            self.candidate.gid,
            self.gallery_key,
        )


@dataclass(frozen=True, slots=True)
class CatalogGidCandidatePage:
    items: tuple[CatalogGidCandidateRow, ...]
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("GID candidate page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogGidWinner:
    gid: int
    winner_gallery_name: str

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("Winner GID must be positive")
        _validate_leaf_name(self.winner_gallery_name, label="GID winner gallery")


@dataclass(frozen=True, order=True, slots=True)
class CatalogFinalAnalysisCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.gallery_key, label="Final analysis gallery cursor")


@dataclass(frozen=True, slots=True)
class CatalogFinalAnalysisPage:
    items: tuple[CatalogSourceGalleryAnalysis, ...]
    limit: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.limit <= 0 or len(self.items) > self.limit:
            raise ValueError("Final analysis page must honor its positive limit")


@dataclass(frozen=True, slots=True)
class CatalogSourceGalleryRecord:
    """Bounded source metadata; files are read through a keyset-paged API."""

    gallery_name: str
    source_locator: str
    metadata_fingerprint: str | None
    metadata_sha256: str | None
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    tags: tuple[GalleryTag, ...]
    source_file_count: int
    source_manifest_sha256: str | None
    source_manifest_version: int | None
    scan_observation_sha256: str | None
    scan_observation_version: int | None
    page_count: int | None
    directory_entry_count: int | None
    directory_observation_sha256: str | None
    content_sha256: str | None = None
    duplicate_of_gallery_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if not self.source_locator:
            raise ValueError("Gallery source locator must not be blank")
        if self.metadata_fingerprint is not None and not self.metadata_fingerprint:
            raise ValueError("Gallery metadata fingerprint must not be blank")
        if self.metadata_sha256 is not None:
            _validate_sha256(
                self.metadata_sha256,
                label="Gallery metadata SHA-256",
            )
        if self.gid <= 0:
            raise ValueError("Gallery GID must be positive")
        if self.source_file_count < 0:
            raise ValueError("Gallery source file count must be non-negative")
        if (self.source_manifest_sha256 is None) != (
            self.source_manifest_version is None
        ):
            raise ValueError(
                "Gallery source manifest digest and version must be supplied together"
            )
        if self.source_manifest_sha256 is not None:
            _validate_sha256(
                self.source_manifest_sha256,
                label="Gallery source manifest SHA-256",
            )
        if (
            self.source_manifest_version is not None
            and self.source_manifest_version <= 0
        ):
            raise ValueError("Gallery source manifest version must be positive")
        if (self.scan_observation_sha256 is None) != (
            self.scan_observation_version is None
        ):
            raise ValueError(
                "Gallery scan observation digest and version must be supplied together"
            )
        if self.scan_observation_sha256 is not None:
            _validate_sha256(
                self.scan_observation_sha256,
                label="Gallery scan observation SHA-256",
            )
        if (
            self.scan_observation_version is not None
            and self.scan_observation_version <= 0
        ):
            raise ValueError("Gallery scan observation version must be positive")
        for label, count in (
            ("Gallery page count", self.page_count),
            ("Gallery directory entry count", self.directory_entry_count),
        ):
            if count is not None and count < 0:
                raise ValueError(f"{label} must be non-negative")
        if self.directory_observation_sha256 is not None:
            _validate_sha256(
                self.directory_observation_sha256,
                label="Gallery directory observation SHA-256",
            )
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Gallery content SHA-256")


@dataclass(frozen=True, slots=True)
class CatalogSourceFileChunk:
    gallery_name: str
    files: tuple[GallerySourceFile, ...]

    def __post_init__(self) -> None:
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        object.__setattr__(self, "files", tuple(self.files))
        names = [source_file.name for source_file in self.files]
        if len(names) != len(set(names)):
            raise ValueError("A source file chunk contains duplicate file names")


@dataclass(frozen=True, slots=True)
class CatalogSourceRevision:
    revision: int
    build_id: str | None
    published_at: datetime
    gallery_count: int
    file_count: int

    def __post_init__(self) -> None:
        if self.revision < 0:
            raise ValueError("Catalog source revision must be non-negative")
        if self.build_id is not None:
            _validate_build_id(self.build_id)
        if self.gallery_count < 0 or self.file_count < 0:
            raise ValueError("Catalog source revision counts must be non-negative")
        if self.revision == 0 and self.build_id is not None:
            raise ValueError("The empty source revision cannot reference a build")
        if self.revision > 0 and self.build_id is None:
            raise ValueError("A published source revision must reference a build")


@dataclass(frozen=True, slots=True)
class CatalogSourcePage:
    revision: CatalogSourceRevision
    galleries: tuple[CatalogSourceGalleryRecord, ...]
    offset: int
    limit: int
    total: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        if min(self.offset, self.limit, self.total) < 0:
            raise ValueError("Catalog source page values must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogBuildSourcePage:
    build: CatalogBuild
    galleries: tuple[CatalogSourceGalleryRecord, ...]
    offset: int
    limit: int
    total: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        if min(self.offset, self.limit, self.total) < 0:
            raise ValueError("Catalog build source page values must be non-negative")


@dataclass(frozen=True, slots=True)
class CatalogSourceFileCursor:
    file_sort_key: str
    file_name: str
    file_key: str

    def __post_init__(self) -> None:
        if not self.file_name:
            raise ValueError("Catalog source file cursor name must not be blank")
        _validate_sha256(self.file_key, label="Catalog source file cursor key")


@dataclass(frozen=True, slots=True)
class CatalogSourceFilePage:
    build_id: str
    gallery_name: str
    files: tuple[GallerySourceFile, ...]
    after: CatalogSourceFileCursor | None
    next_cursor: CatalogSourceFileCursor | None
    limit: int
    has_more: bool

    def __post_init__(self) -> None:
        _validate_build_id(self.build_id)
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        object.__setattr__(self, "files", tuple(self.files))
        if self.limit <= 0:
            raise ValueError("Catalog source file page limit must be positive")


@dataclass(frozen=True, slots=True)
class CatalogContributor:
    name: str
    role: str


@dataclass(frozen=True, slots=True)
class CatalogSubject:
    name: str
    scheme: str | None = None
    code: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogArtifact:
    artifact_id: str
    # Neutral, user-facing download name. The physical/content-addressed path
    # belongs in ``location`` and must not be inferred from this value.
    name: str
    location: Path
    media_type: str
    size_bytes: int
    sha256: str
    modified_at: datetime

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("Artifact size must not be negative")
        if len(self.sha256) != 64:
            raise ValueError("Artifact SHA-256 must contain 64 hexadecimal characters")
        try:
            bytes.fromhex(self.sha256)
        except ValueError as error:
            raise ValueError("Artifact SHA-256 is not hexadecimal") from error


@dataclass(frozen=True, slots=True)
class CatalogPublication:
    publication_id: str
    gid: int
    title: str
    source_title: str
    sort_title: str
    summary: str
    language: str
    published_at: datetime
    modified_at: datetime
    # The selected canonical source is part of every immutable revision.
    source_gallery_name: str
    contributors: tuple[CatalogContributor, ...] = field(default_factory=tuple)
    subjects: tuple[CatalogSubject, ...] = field(default_factory=tuple)
    artifacts: tuple[CatalogArtifact, ...] = field(default_factory=tuple)
    redownload_required: bool = False
    # A gallery can legitimately have no content digest when it contains no
    # non-galleryinfo content or every such file is excluded.
    content_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("Publication GID must be positive")
        if not self.publication_id:
            raise ValueError("Publication ID must not be blank")
        if not self.title:
            raise ValueError("Publication title must not be blank")
        _validate_leaf_name(
            self.source_gallery_name,
            label="Publication source gallery name",
        )
        if self.content_sha256 is not None:
            _validate_sha256(
                self.content_sha256,
                label="Publication content SHA-256",
            )


@dataclass(frozen=True, slots=True)
class CatalogPublicationSelection:
    """Select one canonical source record for the published projection.

    Canonical metadata deliberately does not belong here.  The core derives it
    from the matching :class:`GallerySourceRecord` in the same snapshot.
    """

    source_gallery_name: str
    artifacts: tuple[CatalogArtifact, ...] = field(default_factory=tuple)
    redownload_required: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        if not self.source_gallery_name:
            raise ValueError("Selected source gallery name must not be blank")


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    galleries: tuple[GallerySourceRecord, ...]
    selections: tuple[CatalogPublicationSelection, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        object.__setattr__(self, "selections", tuple(self.selections))

        gallery_names = [gallery.gallery_name for gallery in self.galleries]
        if len(gallery_names) != len(set(gallery_names)):
            raise ValueError("Catalog snapshot contains a duplicate gallery name")

        galleries_by_name = {
            gallery.gallery_name: gallery for gallery in self.galleries
        }
        owner_by_content_sha256: dict[str, str] = {}
        for gallery in self.galleries:
            target_name = gallery.duplicate_of_gallery_name
            if target_name is None:
                if gallery.content_sha256 is not None:
                    previous_owner = owner_by_content_sha256.setdefault(
                        gallery.content_sha256,
                        gallery.gallery_name,
                    )
                    if previous_owner != gallery.gallery_name:
                        raise ValueError(
                            "Catalog snapshot has multiple owners for content SHA-256 "
                            f"{gallery.content_sha256}"
                        )
                continue
            if target_name == gallery.gallery_name:
                raise ValueError("A duplicate gallery cannot target itself")
            target = galleries_by_name.get(target_name)
            if target is None:
                raise ValueError(
                    f"Duplicate target gallery {target_name!r} is not in the snapshot"
                )
            if target.duplicate_of_gallery_name is not None:
                raise ValueError(
                    "A duplicate gallery must target the final content owner"
                )
            if target.content_sha256 != gallery.content_sha256:
                raise ValueError(
                    "A duplicate gallery and its target must have the same content "
                    "SHA-256"
                )

        selected_names = [
            selection.source_gallery_name for selection in self.selections
        ]
        if len(selected_names) != len(set(selected_names)):
            raise ValueError("Catalog snapshot contains a duplicate selected source")
        missing_names = set(selected_names).difference(galleries_by_name)
        if missing_names:
            raise ValueError(
                "Catalog selections reference absent canonical source galleries: "
                + ", ".join(repr(name) for name in sorted(missing_names))
            )
        selected_gids = [galleries_by_name[name].gid for name in selected_names]
        if len(selected_gids) != len(set(selected_gids)):
            raise ValueError(
                "Catalog snapshot contains a duplicate GID among selected sources"
            )
        artifact_ids = [
            artifact.artifact_id
            for selection in self.selections
            for artifact in selection.artifacts
        ]
        artifact_names = [
            artifact.name
            for selection in self.selections
            for artifact in selection.artifacts
        ]
        for label, values in (
            ("artifact ID", artifact_ids),
            ("artifact name", artifact_names),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"Catalog snapshot contains a duplicate {label}")


@dataclass(frozen=True, slots=True)
class CatalogRevision:
    revision: int
    published_at: datetime
    publication_count: int


@dataclass(frozen=True, slots=True)
class CatalogPublishResult:
    revision: CatalogRevision
    new_galleries: int
    changed_galleries: int
    removed_galleries: int

    def __post_init__(self) -> None:
        if (
            min(
                self.new_galleries,
                self.changed_galleries,
                self.removed_galleries,
            )
            < 0
        ):
            raise ValueError("Catalog publish counts must not be negative")


@dataclass(frozen=True, slots=True)
class CatalogPage:
    revision: CatalogRevision
    publications: tuple[CatalogPublication, ...]
    offset: int
    limit: int
    total: int


@dataclass(frozen=True, slots=True)
class DownloadCandidateState:
    gid: int
    cataloged: bool
    redownload_required: bool
    requested: bool


@dataclass(frozen=True, slots=True)
class SchemaCompatibility:
    database_version: int
    minimum_supported: int
    maximum_supported: int


def _require_uint64(value: object, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value < 1 << 64
    ):
        raise ValueError(f"{field_name} must be an unsigned 64-bit integer")
    return value


def _require_int64(value: object, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not -(1 << 63) <= value < 1 << 63
    ):
        raise ValueError(f"{field_name} must be a signed 64-bit integer")
    return value


@dataclass(frozen=True, slots=True)
class FileContentReceipt:
    """Exact-EOF content digest and size derived only from actual byte parts."""

    file_sha256: bytes
    size_bytes: int
    _constructor_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._constructor_token is not _FILE_CONTENT_RECEIPT_TOKEN:
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
                maximum=(1 << 63) - 1,
            )
            size += len(exact)
            if size > (1 << 63) - 1:
                raise OverflowError("file content exceeds signed int63 bytes")
            digest.update(exact)
        return cls(digest.digest(), size, _FILE_CONTENT_RECEIPT_TOKEN)


@dataclass(frozen=True, slots=True)
class FileObservation:
    """One source FILE fact with a content-derived receipt."""

    name_bytes: bytes
    content: FileContentReceipt
    device: int
    inode: int
    modified_ns: int
    changed_ns: int

    def __post_init__(self) -> None:
        validate_file_name(self.name_bytes)
        if not isinstance(self.content, FileContentReceipt):
            raise TypeError("content must be a FileContentReceipt from exact bytes")
        self.content.__post_init__()
        _require_uint64(self.device, field_name="device")
        _require_uint64(self.inode, field_name="inode")
        _require_int64(self.modified_ns, field_name="modified_ns")
        _require_int64(self.changed_ns, field_name="changed_ns")


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
        _require_uint64(self.device, field_name="device")
        _require_uint64(self.inode, field_name="inode")
        _require_int64(self.modified_ns, field_name="modified_ns")
        _require_int64(self.changed_ns, field_name="changed_ns")
        if not isinstance(self.file_type, GalleryObservationDirectoryFileType):
            raise TypeError("file_type must be GalleryObservationDirectoryFileType")


@dataclass(frozen=True, slots=True)
class TagObservation:
    """One exact source tag; its canonical bytes and digest are derived."""

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
class ArtifactStorageEvidence:
    """Untrusted storage-adapter acknowledgement."""

    stored: bool

    def __post_init__(self) -> None:
        if type(self.stored) is not bool:
            raise TypeError("artifact storage acknowledgement must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactReleaseStorageEvidence:
    """Untrusted storage-adapter tombstone acknowledgement."""

    released: bool

    def __post_init__(self) -> None:
        if type(self.released) is not bool:
            raise TypeError("artifact release acknowledgement must be bool")


@dataclass(frozen=True, slots=True)
class VNextCurrentProjectionItem:
    """Neutral immutable facts for one current content-addressed artifact."""

    publication_key: bytes
    gid: int
    source_gallery_name: str
    upload_time: int
    artifact_locator_components: tuple[str, ...]
    artifact_sha256: bytes
    size_bytes: int

    def __post_init__(self) -> None:
        key = require_digest32(
            self.publication_key,
            field="current projection publication_key",
        )
        gid = require_positive_int63(self.gid, field="current projection gid")
        if publication_key(gid) != key:
            raise ValueError(
                "current projection publication_key disagrees with its GID"
            )
        if not isinstance(self.source_gallery_name, str):
            raise TypeError("current projection source_gallery_name must be str")
        source_name = self.source_gallery_name.encode("utf-8", errors="strict")
        require_bounded_bytes(
            source_name,
            field="current projection source_gallery_name UTF-8",
            minimum=1,
            maximum=255,
        )
        if b"\x00" in source_name:
            raise ValueError("current projection source_gallery_name contains NUL")
        require_int63(self.upload_time, field="current projection upload_time")
        artifact = require_digest32(
            self.artifact_sha256,
            field="current projection artifact_sha256",
        )
        if not isinstance(
            self.artifact_locator_components, tuple
        ) or self.artifact_locator_components != artifact_locator_components(artifact):
            raise ValueError(
                "current projection artifact locator is not content-addressed"
            )
        size = require_int63(self.size_bytes, field="current projection size_bytes")
        if size < 0:
            raise ValueError("current projection size_bytes must be non-negative")


@dataclass(frozen=True, slots=True)
class VNextCurrentProjectionPage:
    """One bounded keyset page pinned to an immutable publication receipt."""

    receipt_id: bytes
    catalog_revision: int
    items: tuple[VNextCurrentProjectionItem, ...]
    next_cursor: bytes | None
    terminal: bool

    def __post_init__(self) -> None:
        require_bounded_bytes(
            self.receipt_id,
            field="current projection receipt_id",
            minimum=16,
            maximum=16,
        )
        require_positive_int63(
            self.catalog_revision,
            field="current projection catalog_revision",
        )
        if not isinstance(self.items, tuple):
            raise TypeError("current projection items must be an exact tuple")
        if len(self.items) > 128:
            raise ValueError("current projection page cannot exceed 128 items")
        keys: list[bytes] = []
        for item in self.items:
            if not isinstance(item, VNextCurrentProjectionItem):
                raise TypeError("current projection page contains a foreign item")
            item.__post_init__()
            keys.append(item.publication_key)
        if keys != sorted(set(keys)):
            raise ValueError(
                "current projection publication keys must be strictly ordered"
            )
        if type(self.terminal) is not bool:
            raise TypeError("current projection terminal must be bool")
        if not keys:
            if not self.terminal or self.next_cursor is not None:
                raise ValueError("an empty current projection page must be terminal")
            return
        cursor = require_digest32(
            self.next_cursor,
            field="current projection next_cursor",
        )
        if self.terminal or cursor != keys[-1]:
            raise ValueError(
                "a nonempty current projection page must advance its exact cursor"
            )


def _require_positive_uint32(value: object, *, field_name: str) -> int:
    result = require_uint32(value, field=field_name)
    if result == 0:
        raise ValueError(f"{field_name} must be positive")
    return result


@dataclass(frozen=True, slots=True)
class VNextArtifactProducer:
    """Natural identity of the executable artifact producer."""

    writer_id: bytes
    python_abi: bytes
    pillow_build: bytes
    libjpeg_build: bytes
    zlib_build: bytes

    def __post_init__(self) -> None:
        for field_name in (
            "writer_id",
            "python_abi",
            "pillow_build",
            "libjpeg_build",
            "zlib_build",
        ):
            require_bounded_bytes(
                getattr(self, field_name),
                field=f"artifact producer {field_name}",
                minimum=1,
                maximum=128,
            )

    @property
    def fingerprint_sha256(self) -> bytes:
        return artifact_producer_fingerprint_sha256(
            self.writer_id,
            self.python_abi,
            self.pillow_build,
            self.libjpeg_build,
            self.zlib_build,
        )


@dataclass(frozen=True, slots=True)
class VNextArtifactStoragePolicy:
    """Natural identity of the consumer-owned storage adapter codec."""

    adapter_id: bytes
    storage_codec_version: int = 1
    locator_codec_version: int = 1
    protection_token_codec_version: int = 1

    def __post_init__(self) -> None:
        require_ascii_bytes(
            self.adapter_id,
            field="artifact storage adapter_id",
            minimum=1,
            maximum=64,
        )
        for field_name in (
            "storage_codec_version",
            "locator_codec_version",
            "protection_token_codec_version",
        ):
            _require_positive_uint32(
                getattr(self, field_name),
                field_name=f"artifact storage {field_name}",
            )


@dataclass(frozen=True, slots=True)
class VNextArtifactZipPolicy:
    """Exact deterministic ZIP-writer facts used by artifact production."""

    zip_codec_version: int = 1
    compression_method: int = 8
    compression_level: int = 9
    dos_date: int = 33
    dos_time: int = 0
    unix_mode: int = 33_188
    general_purpose_flags: int = 2_048
    create_system: int = 3
    archive_name_codec_version: int = 1
    artifact_name_codec_version: int = 1

    def __post_init__(self) -> None:
        for field_name in (
            "zip_codec_version",
            "archive_name_codec_version",
            "artifact_name_codec_version",
        ):
            _require_positive_uint32(
                getattr(self, field_name),
                field_name=f"artifact ZIP {field_name}",
            )
        for field_name in (
            "compression_method",
            "compression_level",
            "dos_date",
            "dos_time",
            "unix_mode",
            "general_purpose_flags",
            "create_system",
        ):
            require_uint32(
                getattr(self, field_name),
                field=f"artifact ZIP {field_name}",
            )


@dataclass(frozen=True, slots=True)
class VNextIngestPolicy:
    """Complete natural policy facts; callers never select registry IDs."""

    producer: VNextArtifactProducer
    storage: VNextArtifactStoragePolicy
    manifest_algorithm_version: int = 1
    file_order_version: int = 1
    analysis_algorithm_version: int = 1
    spam_artist_threshold: int = 1
    spam_occurrence_threshold: int = 3
    content_owner_rule_version: int = 1
    gid_winner_rule_version: int = 1
    artifact_algorithm_version: int = 1
    max_image_short_side: int = 2_048
    zip: VNextArtifactZipPolicy = field(default_factory=VNextArtifactZipPolicy)
    display_title_algorithm_version: int = 1
    title_sort_algorithm_version: int = 1
    unicode_data_version: bytes = unidata_version.encode("ascii")
    operational_schema_version: int = 1
    operational_algorithm_version: int = 1
    operational_max_batch_rows: int = 128
    artifacts_required: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.producer, VNextArtifactProducer):
            raise TypeError("producer must be VNextArtifactProducer")
        if not isinstance(self.storage, VNextArtifactStoragePolicy):
            raise TypeError("storage must be VNextArtifactStoragePolicy")
        if not isinstance(self.zip, VNextArtifactZipPolicy):
            raise TypeError("zip must be VNextArtifactZipPolicy")
        self.producer.__post_init__()
        self.storage.__post_init__()
        self.zip.__post_init__()
        for field_name in (
            "manifest_algorithm_version",
            "file_order_version",
            "analysis_algorithm_version",
            "content_owner_rule_version",
            "gid_winner_rule_version",
            "artifact_algorithm_version",
            "max_image_short_side",
            "display_title_algorithm_version",
            "title_sort_algorithm_version",
            "operational_schema_version",
            "operational_algorithm_version",
            "operational_max_batch_rows",
        ):
            _require_positive_uint32(
                getattr(self, field_name),
                field_name=f"ingest policy {field_name}",
            )
        require_int63(
            self.spam_artist_threshold,
            field="ingest policy spam_artist_threshold",
        )
        require_int63(
            self.spam_occurrence_threshold,
            field="ingest policy spam_occurrence_threshold",
        )
        require_bounded_bytes(
            self.unicode_data_version,
            field="ingest policy unicode_data_version",
            minimum=1,
            maximum=32,
        )
        if type(self.artifacts_required) is not bool:
            raise TypeError("artifacts_required must be bool")

    @property
    def producer_fingerprint_sha256(self) -> bytes:
        return self.producer.fingerprint_sha256

    @property
    def artifact_policy_sha256(self) -> bytes:
        return artifact_policy_digest(
            self.artifact_algorithm_version,
            self.max_image_short_side,
            self.producer_fingerprint_sha256,
        )


@dataclass(frozen=True, slots=True)
class VNextResolvedIngestPolicy:
    """Registry authority resolved from one complete natural policy."""

    policy: VNextIngestPolicy
    manifest_policy_id: int
    analysis_policy_id: int
    artifact_policy_sha256: bytes
    producer_fingerprint_sha256: bytes
    display_title_policy_id: int
    title_sort_policy_id: int
    operational_policy_id: int
    replayed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.policy, VNextIngestPolicy):
            raise TypeError("policy must be VNextIngestPolicy")
        for field_name in (
            "manifest_policy_id",
            "analysis_policy_id",
            "display_title_policy_id",
            "title_sort_policy_id",
            "operational_policy_id",
        ):
            require_positive_int63(
                getattr(self, field_name), field=f"resolved {field_name}"
            )
        artifact = require_digest32(
            self.artifact_policy_sha256,
            field="resolved artifact_policy_sha256",
        )
        producer = require_digest32(
            self.producer_fingerprint_sha256,
            field="resolved producer_fingerprint_sha256",
        )
        if artifact != self.policy.artifact_policy_sha256:
            raise ValueError("resolved artifact policy differs from natural facts")
        if producer != self.policy.producer_fingerprint_sha256:
            raise ValueError("resolved producer differs from natural facts")
        if type(self.replayed) is not bool:
            raise TypeError("resolved policy replayed must be bool")


@dataclass(frozen=True, slots=True)
class VNextIngestPage[IngestItemT]:
    """One bounded, replayable source-observation page."""

    items: tuple[IngestItemT, ...]
    next_after: VNextIngestCursor | None
    terminal: bool

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError("ingest page items must be an exact tuple")
        if len(self.items) > 256:
            raise ValueError("ingest page cannot contain more than 256 items")
        if type(self.terminal) is not bool:
            raise TypeError("ingest page terminal must be bool")
        if not self.terminal and not self.items:
            raise ValueError("a nonterminal ingest page cannot be empty")
        if self.terminal and self.next_after is not None:
            raise ValueError("a terminal ingest page cannot expose next_after")
        if not self.terminal and self.next_after is None:
            raise ValueError("a nonterminal ingest page requires next_after")
        if isinstance(self.next_after, int):
            require_int63(self.next_after, field="ingest page next_after")
        elif isinstance(self.next_after, bytes):
            require_bounded_bytes(
                self.next_after,
                field="ingest page next_after",
                minimum=1,
                maximum=255,
            )
        elif isinstance(self.next_after, tuple):
            encode_source_relative_locator(self.next_after)
        elif self.next_after is not None:
            raise TypeError("ingest page next_after has an unsupported cursor type")


@dataclass(frozen=True, slots=True)
class VNextIngestGalleryObservation:
    """Small gallery header used to reopen bounded component page streams."""

    locator_components: tuple[str, ...]
    metadata: GalleryObservationMetadata

    def __post_init__(self) -> None:
        if not isinstance(self.locator_components, tuple):
            raise TypeError("gallery locator_components must be an exact tuple")
        encode_source_relative_locator(self.locator_components)
        if not isinstance(self.metadata, GalleryObservationMetadata):
            raise TypeError("metadata must be GalleryObservationMetadata")
        self.metadata.__post_init__()


@dataclass(frozen=True, slots=True)
class VNextIngestSession:
    """Exact restart capability for the shared gate and coordinated ingest."""

    gate_owner_token: bytes
    gate_generation: int
    gate_slot: int
    gate_lease_expires_at: int
    ingest_generation: int
    ingest_owner_token: bytes
    ingest_lease_expires_at: int
    download_generation: int | None
    handoff_owner_token: bytes | None
    handoff_kind: str | None
    consumed_at: int | None

    def __post_init__(self) -> None:
        require_bounded_bytes(
            self.gate_owner_token,
            field="ingest session gate_owner_token",
            minimum=16,
            maximum=16,
        )
        require_int63(self.gate_generation, field="ingest session gate_generation")
        if isinstance(self.gate_slot, bool) or not isinstance(self.gate_slot, int):
            raise TypeError("ingest session gate_slot must be int")
        if not 0 <= self.gate_slot < 64:
            raise ValueError("ingest session gate_slot must be in 0..63")
        require_int63(
            self.gate_lease_expires_at,
            field="ingest session gate_lease_expires_at",
        )
        require_int63(
            self.ingest_generation,
            field="ingest session ingest_generation",
        )
        require_bounded_bytes(
            self.ingest_owner_token,
            field="ingest session ingest_owner_token",
            minimum=16,
            maximum=16,
        )
        require_int63(
            self.ingest_lease_expires_at,
            field="ingest session ingest_lease_expires_at",
        )
        linked = self.download_generation is not None
        if linked != all(
            value is not None
            for value in (
                self.handoff_owner_token,
                self.handoff_kind,
                self.consumed_at,
            )
        ):
            raise ValueError(
                "linked ingest session fields must be all present or absent"
            )
        if linked:
            require_int63(
                self.download_generation,
                field="ingest session download_generation",
            )
            require_bounded_bytes(
                self.handoff_owner_token,
                field="ingest session handoff_owner_token",
                minimum=16,
                maximum=16,
            )
            if self.handoff_kind not in {"DOWNLOADER", "EXPIRED_TAKEOVER"}:
                raise ValueError("ingest session handoff_kind is not registered")
            require_int63(self.consumed_at, field="ingest session consumed_at")


@dataclass(frozen=True, slots=True)
class VNextIngestCompletionReceipt:
    """Durable, response-loss-safe completion of one ingest capability."""

    ingest_generation: int
    owner_token: bytes
    completed_at: int
    download_generation: int | None
    replayed: bool

    def __post_init__(self) -> None:
        require_int63(
            self.ingest_generation,
            field="ingest completion ingest_generation",
        )
        require_bounded_bytes(
            self.owner_token,
            field="ingest completion owner_token",
            minimum=16,
            maximum=16,
        )
        require_int63(self.completed_at, field="ingest completion completed_at")
        if self.download_generation is not None:
            require_int63(
                self.download_generation,
                field="ingest completion download_generation",
            )
        if type(self.replayed) is not bool:
            raise TypeError("ingest completion replayed must be bool")


@dataclass(frozen=True, slots=True)
class VNextIngestSourceReceipt:
    build_id: bytes
    discovered_galleries: int
    staged_galleries: int
    sealed: bool
    replayed: bool

    def __post_init__(self) -> None:
        require_bounded_bytes(
            self.build_id,
            field="source receipt build_id",
            minimum=16,
            maximum=16,
        )
        require_int63(
            self.discovered_galleries,
            field="source receipt discovered_galleries",
        )
        require_int63(
            self.staged_galleries,
            field="source receipt staged_galleries",
        )
        if self.staged_galleries > self.discovered_galleries:
            raise ValueError("staged_galleries cannot exceed discovered_galleries")
        if type(self.sealed) is not bool or type(self.replayed) is not bool:
            raise TypeError("source receipt flags must be bool")


class VNextIngestPhase(StrEnum):
    SOURCE = "SOURCE"
    ANALYSIS = "ANALYSIS"
    PUBLICATION = "PUBLICATION"
    FINALIZATION = "FINALIZATION"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class VNextIngestAdvanceResult:
    """Bounded progress result; terminal only means this phase is complete."""

    phase: VNextIngestPhase
    processed_rows: int
    terminal: bool
    replayed: bool
    source_receipt: VNextIngestSourceReceipt | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.phase, VNextIngestPhase):
            raise TypeError("phase must be VNextIngestPhase")
        require_int63(self.processed_rows, field="ingest processed_rows")
        if type(self.terminal) is not bool or type(self.replayed) is not bool:
            raise TypeError("ingest result flags must be bool")
        if self.source_receipt is not None:
            if not isinstance(self.source_receipt, VNextIngestSourceReceipt):
                raise TypeError("source_receipt must be VNextIngestSourceReceipt")
            self.source_receipt.__post_init__()
