"""Public application protocols for the greenfield catalog."""

from __future__ import annotations

__all__ = [
    "ArtifactReleaseAdapter",
    "ArtifactStorageAdapter",
    "CatalogReader",
    "VNextIngestSourceAdapter",
]

from collections.abc import Mapping, Sequence
from typing import BinaryIO, Protocol, runtime_checkable

from .domain import (
    ArtifactReleaseStorageEvidence,
    ArtifactStorageEvidence,
    ArtifactStorageKey,
    CatalogArtifact,
    CatalogArtifactCursor,
    CatalogArtifactPage,
    CatalogPage,
    CatalogPublication,
    CatalogRevision,
    DirectoryObservation,
    FileObservation,
    TagObservation,
    VNextIngestGalleryObservation,
    VNextIngestPage,
)
from .vnext_identity import ArtifactTransformKind


@runtime_checkable
class CatalogReader(Protocol):
    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision: ...

    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPage: ...

    def list_artifact_publications(
        self,
        *,
        after: CatalogArtifactCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifactPage: ...

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None: ...

    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | int | None = None,
    ) -> Mapping[str, CatalogPublication]: ...

    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifact | None: ...


@runtime_checkable
class VNextIngestSourceAdapter(Protocol):
    """Restartable, bounded observation boundary implemented by consumers.

    Every page method is keyset-addressed and may be called again after
    response loss.  The facade always requests the registered leaf capacity:
    256 FILE rows, 192 DIRECTORY rows, and 256 TAG rows.
    """

    @property
    def source_root_components(self) -> tuple[str, ...]: ...

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> VNextIngestPage[tuple[str, ...]]: ...

    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextIngestGalleryObservation: ...

    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]: ...

    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]: ...

    def list_tag_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]: ...


@runtime_checkable
class ArtifactStorageAdapter(Protocol):
    """Consumer-owned deterministic renderer and monotone protection store."""

    adapter_id: bytes
    producer_fingerprint_sha256: bytes

    def render_member(
        self,
        source: BinaryIO,
        transform_kind: ArtifactTransformKind,
        destination: BinaryIO,
    ) -> None: ...

    def protect(
        self,
        archive: BinaryIO,
        storage_key: ArtifactStorageKey,
        expected_artifact_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence: ...


@runtime_checkable
class ArtifactReleaseAdapter(Protocol):
    """Consumer-owned terminal, idempotent protection-token tombstone."""

    adapter_id: bytes

    def release(
        self,
        storage_key: ArtifactStorageKey,
        expected_artifact_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence: ...
