__all__ = [
    "CatalogPublisher",
    "CatalogReader",
    "DatabaseAdmin",
    "DownloadCoordinator",
]

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from .domain import (
    CatalogArtifact,
    CatalogPage,
    CatalogPublication,
    CatalogPublishResult,
    CatalogRevision,
    CatalogSnapshot,
    DownloadCandidateState,
    SchemaCompatibility,
)
from .table_database_maintenance import DatabaseMaintenanceResult
from .table_gallery_ingest_coordination import (
    DownloadTurn,
    GalleryIngestState,
    GalleryIngestTurn,
)
from .todownload_queue import DownloadRequest, EnsureDownloadRequestResult


@runtime_checkable
class DatabaseAdmin(Protocol):
    def migrate(self) -> int: ...

    def check_compatibility(self) -> SchemaCompatibility: ...

    def database_gate(
        self, *, timeout_seconds: int | None = None
    ) -> AbstractContextManager[None]: ...

    def optimize_database(self) -> DatabaseMaintenanceResult: ...

    def run_scheduled_database_maintenance(self) -> DatabaseMaintenanceResult: ...

    def record_catalog_changes(self, *, changed: int, removed: int) -> None: ...

    def analyze_database(self) -> None: ...


@runtime_checkable
class CatalogReader(Protocol):
    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision: ...

    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | None = None,
        require_artifact: bool = False,
    ) -> CatalogPage: ...

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogPublication | None: ...

    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | None = None,
    ) -> Mapping[str, CatalogPublication]: ...

    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogArtifact | None: ...


@runtime_checkable
class CatalogPublisher(Protocol):
    def publish_snapshot(
        self,
        snapshot: CatalogSnapshot,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogPublishResult: ...


@runtime_checkable
class DownloadCoordinator(Protocol):
    def request_download(self, gid: int, url: str = "") -> DownloadRequest: ...

    def ensure_download_request(
        self, gid: int, url: str = ""
    ) -> EnsureDownloadRequestResult: ...

    def get_download_request(self, gid: int) -> DownloadRequest | None: ...

    def get_download_requests(self) -> list[DownloadRequest]: ...

    def get_candidate_states(
        self, gids: Sequence[int]
    ) -> Mapping[int, DownloadCandidateState]: ...

    def get_pending_redownload_gids(self) -> list[int]: ...

    def complete_download_request(self, request: DownloadRequest) -> None: ...

    def complete_missing_download_request(
        self, request: DownloadRequest, gid: int
    ) -> None: ...

    def record_gallery_found(self, *gids: int) -> None: ...

    def record_accepted_submission(
        self,
        gid: int,
        *,
        request: DownloadRequest | None = None,
    ) -> None: ...

    def request_gallery_deletion(self, gid: int) -> None: ...

    def get_gallery_deletion_requests(self) -> list[int]: ...

    def claim_download_turn(self, *, lease_seconds: int) -> DownloadTurn | None: ...

    def renew_download_turn(
        self, turn: DownloadTurn, *, lease_seconds: int
    ) -> bool: ...

    def request_gallery_ingest(self, turn: DownloadTurn) -> bool: ...

    def complete_download_request_in_turn(
        self, turn: DownloadTurn, request: DownloadRequest
    ) -> bool: ...

    def complete_missing_download_request_in_turn(
        self, turn: DownloadTurn, request: DownloadRequest, gid: int
    ) -> bool: ...

    def finish_download_turn(
        self, turn: DownloadTurn, request: DownloadRequest
    ) -> bool: ...

    def finish_missing_download_turn(
        self, turn: DownloadTurn, request: DownloadRequest, gid: int
    ) -> bool: ...

    def get_gallery_ingest_state(self) -> GalleryIngestState: ...

    def claim_gallery_ingest(
        self, *, lease_seconds: int, periodic_scan: bool
    ) -> GalleryIngestTurn | None: ...

    def renew_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int | None: ...

    def complete_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        allow_expired_sqlite_lease: bool = False,
    ) -> bool: ...
