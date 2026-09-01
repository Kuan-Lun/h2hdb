"""Public application protocols for the greenfield catalog."""

from __future__ import annotations

__all__ = [
    "ArtifactReleaseAdapter",
    "ArtifactStorageAdapter",
    "CatalogReader",
    "VNextIngestSourceAdapter",
]

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import BinaryIO, Protocol, runtime_checkable

from .domain import (
    DEFAULT_CATALOG_DISCOVERY_QUERY,
    ArtifactArchiveRenderEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactReleaseStorageEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactStorageEvidence,
    CatalogArtifact,
    CatalogDiscoveryBundle,
    CatalogDiscoveryCursor,
    CatalogDiscoveryPage,
    CatalogDiscoveryQuery,
    CatalogFacetCursor,
    CatalogFacetKind,
    CatalogFacetPage,
    CatalogImageResource,
    CatalogPublication,
    CatalogPublicationPresentation,
    CatalogRecentOrder,
    CatalogRecentWindow,
    CatalogResourceKind,
    CatalogRevision,
    DirectoryObservation,
    FileObservation,
    StorageObjectKey,
    TagObservation,
    VNextIngestGalleryObservation,
    VNextIngestPage,
)


@runtime_checkable
class CatalogReader(Protocol):
    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision: ...

    def discover_publications(
        self,
        *,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogDiscoveryCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogDiscoveryPage: ...

    def discover_publications_with_facets(
        self,
        *,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogDiscoveryCursor | None = None,
        limit: int = 50,
        facet_limit: int = 128,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogDiscoveryBundle: ...

    def list_publication_facets(
        self,
        *,
        facet: CatalogFacetKind,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogFacetCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogFacetPage: ...

    def list_recent_publications(
        self,
        *,
        order: CatalogRecentOrder,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogRecentWindow: ...

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None: ...

    def get_publication_presentation(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublicationPresentation | None: ...

    def get_publication_page(
        self,
        publication_id: str,
        page_index: int,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogImageResource | None: ...

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
    policy_fingerprint_sha256: bytes

    def storage_key(
        self,
        gid: int,
        resource_kind: CatalogResourceKind,
    ) -> StorageObjectKey:
        """Return the deterministic adapter-owned key for one logical resource."""
        ...

    def open_source(
        self,
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        """Open one adapter-observed source leaf without exposing path semantics."""
        ...

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence: ...

    def protect(
        self,
        archive: BinaryIO,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        modified_at: datetime,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence: ...

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence: ...


@runtime_checkable
class ArtifactReleaseAdapter(Protocol):
    """Consumer-owned terminal, idempotent protection-token tombstone."""

    adapter_id: bytes

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence: ...
