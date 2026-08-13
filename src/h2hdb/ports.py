__all__ = [
    "CatalogBuildAnalyzer",
    "CatalogBuildCoordinator",
    "CatalogBuildProjectionCoordinator",
    "CatalogPublisher",
    "CatalogReader",
    "DatabaseAdmin",
    "DownloadCoordinator",
]

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from .domain import (
    CatalogAnalysisPhase,
    CatalogAnalysisPhaseCheckpoint,
    CatalogArtifact,
    CatalogBuild,
    CatalogBuildBatchResult,
    CatalogBuildOperationalState,
    CatalogBuildProjection,
    CatalogBuildProjectionBatchResult,
    CatalogBuildProjectionPruneResult,
    CatalogBuildProjectionPublishResult,
    CatalogBuildPruneResult,
    CatalogBuildPublishResult,
    CatalogBuildSourcePage,
    CatalogContentCandidateCursor,
    CatalogContentCandidatePage,
    CatalogContentDigest,
    CatalogContentOwner,
    CatalogFileHashAggregatePage,
    CatalogFileSpamPageApplyResult,
    CatalogFinalAnalysisCursor,
    CatalogFinalAnalysisPage,
    CatalogGalleryFileHashCursor,
    CatalogGalleryFileHashPage,
    CatalogGidCandidateCursor,
    CatalogGidCandidatePage,
    CatalogGidWinner,
    CatalogPage,
    CatalogPendingGalleryPage,
    CatalogPreparedArtifact,
    CatalogProjectionArtifactCursor,
    CatalogProjectionArtifactPage,
    CatalogProjectionCheckpoint,
    CatalogProjectionPublicationReceipt,
    CatalogProjectionSelectedFileCursor,
    CatalogProjectionSelectedFilePage,
    CatalogProjectionSelectedGalleryCursor,
    CatalogProjectionSelectedGalleryPage,
    CatalogProjectionSelection,
    CatalogProjectionSelectionCursor,
    CatalogProjectionSelectionPage,
    CatalogPublication,
    CatalogPublishResult,
    CatalogRevision,
    CatalogSnapshot,
    CatalogSourceDiscoveryCompletion,
    CatalogSourceFileChunk,
    CatalogSourceFileCursor,
    CatalogSourceFilePage,
    CatalogSourceGalleryAnalysis,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryDiscovery,
    CatalogSourceGalleryHeader,
    CatalogSourceManifest,
    CatalogSourceManifestCursor,
    CatalogSourceManifestPage,
    CatalogSourcePage,
    CatalogSourceRevision,
    DownloadCandidateState,
    FileHashCacheEntry,
    FileHashCacheKey,
    GallerySourceFile,
    SchemaCompatibility,
)
from .schema_admin import SchemaEpochReadiness
from .schema_epoch import SchemaEpochProvider, SchemaEpochReport
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

    def initialize_schema_epoch_v2(
        self, provider: SchemaEpochProvider | None = None
    ) -> SchemaEpochReport: ...

    def check_schema_epoch_v2(
        self, provider: SchemaEpochProvider | None = None
    ) -> SchemaEpochReport: ...

    def check_schema_epoch_v2_readiness(
        self, provider: SchemaEpochProvider | None = None
    ) -> SchemaEpochReadiness: ...

    def check_compatibility(self) -> SchemaCompatibility: ...

    def check_readiness(self) -> SchemaCompatibility: ...

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
class CatalogBuildCoordinator(Protocol):
    """Incrementally build and activate immutable source snapshots.

    Source activation is intentionally distinct from publishing the
    user-facing catalog projection.
    """

    def begin_catalog_build(
        self,
        *,
        scope_key: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild: ...

    def resume_catalog_build(
        self,
        *,
        scope_key: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild | None: ...

    def discover_catalog_galleries(
        self,
        build: CatalogBuild,
        gallery_names: Sequence[str],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def discover_catalog_sources(
        self,
        build: CatalogBuild,
        discoveries: Sequence[CatalogSourceGalleryDiscovery],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def complete_catalog_discovery(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
        completion: CatalogSourceDiscoveryCompletion | None = None,
    ) -> CatalogBuild: ...

    def begin_catalog_gallery(
        self,
        build: CatalogBuild,
        header: CatalogSourceGalleryHeader,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def stage_catalog_gallery_headers(
        self,
        build: CatalogBuild,
        headers: Sequence[CatalogSourceGalleryHeader],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def stage_catalog_file_chunk(
        self,
        build: CatalogBuild,
        gallery_name: str,
        files: Sequence[GallerySourceFile],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def stage_catalog_file_chunks(
        self,
        build: CatalogBuild,
        chunks: Sequence[CatalogSourceFileChunk],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def complete_catalog_gallery(
        self,
        build: CatalogBuild,
        completion: CatalogSourceGalleryCompletion,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def complete_catalog_galleries(
        self,
        build: CatalogBuild,
        completions: Sequence[CatalogSourceGalleryCompletion],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def complete_catalog_source_staging(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild: ...

    def complete_catalog_analysis(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild: ...

    def cache_catalog_file_hashes(
        self,
        build: CatalogBuild,
        entries: Sequence[FileHashCacheEntry],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def get_catalog_file_hashes(
        self,
        keys: Sequence[FileHashCacheKey],
    ) -> Mapping[FileHashCacheKey, str]: ...

    def seal_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild: ...

    def prepare_catalog_build_operations(
        self,
        build: CatalogBuild,
        *,
        max_rows: int = 1000,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildOperationalState: ...

    def abandon_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild: ...

    def publish_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildPublishResult: ...

    def prune_catalog_build(
        self,
        build_id: str,
        *,
        max_rows: int = 1000,
    ) -> CatalogBuildPruneResult: ...

    def list_catalog_build_cleanup_candidates(
        self,
        *,
        limit: int = 100,
    ) -> tuple[CatalogBuild, ...]: ...

    def get_catalog_build(self, build_id: str) -> CatalogBuild | None: ...

    def get_working_catalog_build(self) -> CatalogBuild | None: ...

    def get_active_catalog_build(self) -> CatalogBuild | None: ...

    def get_catalog_source_revision(
        self,
        revision: int | None = None,
    ) -> CatalogSourceRevision: ...

    def list_catalog_sources(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogSourceRevision | None = None,
    ) -> CatalogSourcePage: ...

    def list_catalog_build_sources(
        self,
        build_id: str,
        *,
        offset: int = 0,
        limit: int = 50,
    ) -> CatalogBuildSourcePage: ...

    def list_pending_catalog_galleries(
        self,
        build_id: str,
        *,
        after_gallery_name: str | None = None,
        limit: int = 100,
    ) -> CatalogPendingGalleryPage: ...

    def list_catalog_build_files(
        self,
        build_id: str,
        gallery_name: str,
        *,
        after: CatalogSourceFileCursor | None = None,
        limit: int = 100,
    ) -> CatalogSourceFilePage: ...


@runtime_checkable
class CatalogBuildAnalyzer(Protocol):
    """Bounded durable storage used by the ingest-owned deduplication policy."""

    def is_catalog_analysis_phase_complete(
        self,
        build_id: str,
        phase: CatalogAnalysisPhase,
    ) -> bool: ...

    def list_catalog_source_manifest_rows(
        self,
        build_id: str,
        *,
        after: CatalogSourceManifestCursor | None = None,
        limit: int = 1000,
    ) -> CatalogSourceManifestPage: ...

    def stage_catalog_source_manifests(
        self,
        build: CatalogBuild,
        manifests: Sequence[CatalogSourceManifest],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def get_catalog_file_spam_page(
        self,
        build: CatalogBuild,
        *,
        minimum_occurrences: int,
        limit: int = 1000,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogFileHashAggregatePage: ...

    def apply_catalog_file_spam_page(
        self,
        build: CatalogBuild,
        page: CatalogFileHashAggregatePage,
        excluded_hashes: Sequence[str],
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogFileSpamPageApplyResult: ...

    def list_catalog_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: CatalogGalleryFileHashCursor | None = None,
        limit: int = 1000,
    ) -> CatalogGalleryFileHashPage: ...

    def stage_catalog_content_digests(
        self,
        build: CatalogBuild,
        digests: Sequence[CatalogContentDigest],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def list_catalog_content_candidates(
        self,
        build_id: str,
        *,
        after: CatalogContentCandidateCursor | None = None,
        limit: int = 1000,
    ) -> CatalogContentCandidatePage: ...

    def stage_catalog_content_owners(
        self,
        build: CatalogBuild,
        owners: Sequence[CatalogContentOwner],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def list_catalog_gid_candidates(
        self,
        build_id: str,
        *,
        after: CatalogGidCandidateCursor | None = None,
        limit: int = 1000,
    ) -> CatalogGidCandidatePage: ...

    def stage_catalog_gid_winners(
        self,
        build: CatalogBuild,
        winners: Sequence[CatalogGidWinner],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def list_catalog_final_analyses(
        self,
        build_id: str,
        *,
        after: CatalogFinalAnalysisCursor | None = None,
        limit: int = 1000,
    ) -> CatalogFinalAnalysisPage: ...

    def stage_catalog_final_analyses(
        self,
        build: CatalogBuild,
        analyses: Sequence[CatalogSourceGalleryAnalysis],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult: ...

    def complete_catalog_analysis_phase(
        self,
        build: CatalogBuild,
        phase: CatalogAnalysisPhase,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogAnalysisPhaseCheckpoint: ...


@runtime_checkable
class CatalogBuildProjectionCoordinator(Protocol):
    """Bounded staging and atomic publication of a source-build projection."""

    def begin_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        artifacts_required: bool,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection: ...

    def get_catalog_build_projection(
        self, build_id: str
    ) -> CatalogBuildProjection | None: ...

    def get_catalog_projection_checkpoint(
        self, build_id: str
    ) -> CatalogProjectionCheckpoint: ...

    def list_catalog_projection_selected_galleries(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectedGalleryCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectedGalleryPage: ...

    def list_catalog_projection_selected_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: CatalogProjectionSelectedFileCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectedFilePage: ...

    def record_catalog_prepared_artifact(
        self,
        build: CatalogBuild,
        prepared: CatalogPreparedArtifact,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult: ...

    def record_catalog_prepared_artifacts(
        self,
        build: CatalogBuild,
        prepared: Sequence[CatalogPreparedArtifact],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult: ...

    def advance_catalog_artifact_checkpoint(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        after: CatalogProjectionSelectedGalleryCursor,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult: ...

    def complete_catalog_artifact_preparation(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection: ...

    def list_catalog_projection_selections(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectionCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectionPage: ...

    def stage_catalog_projection_selections(
        self,
        build: CatalogBuild,
        selections: Sequence[CatalogProjectionSelection],
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        after: CatalogProjectionSelectionCursor,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult: ...

    def complete_catalog_projection_staging(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection: ...

    def seal_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection: ...

    def publish_catalog_build_with_projection(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionPublishResult: ...

    def get_catalog_projection_publication_receipt(
        self,
        build_id: str | None = None,
        *,
        pending_only: bool = False,
    ) -> CatalogProjectionPublicationReceipt | None: ...

    def list_published_catalog_projection_artifacts(
        self,
        build_id: str,
        *,
        after: CatalogProjectionArtifactCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionArtifactPage: ...

    def acknowledge_catalog_projection_finalized(
        self,
        build: CatalogBuild,
        *,
        catalog_revision: int,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt: ...

    def prune_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        max_rows: int = 1000,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionPruneResult: ...


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
