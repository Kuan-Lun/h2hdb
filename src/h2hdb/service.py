from __future__ import annotations

__all__ = [
    "CoordinatorUnavailableError",
    "H2HDB",
    "IngestTurnLostError",
    "open_database",
]

import sqlite3
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
from types import TracebackType
from typing import Concatenate

from .canonical_repository import CanonicalSnapshotRepository
from .catalog_analysis_repository import CatalogAnalysisRepository
from .catalog_build_repository import CatalogBuildRepository, CatalogBuildStateError
from .catalog_operational_repository import CatalogOperationalRepository
from .catalog_projection_build_repository import CatalogBuildProjectionRepository
from .catalog_repository import CatalogProjectionRepository
from .config_loader import CoreConfig
from .domain import (
    CatalogAnalysisPhase,
    CatalogAnalysisPhaseCheckpoint,
    CatalogAnalysisScanCompletion,
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
    CatalogContributor,
    CatalogFileHashAggregateCursor,
    CatalogFileHashAggregatePage,
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
    CatalogSubject,
    DownloadCandidateState,
    FileHashCacheEntry,
    FileHashCacheKey,
    GallerySourceFile,
    SchemaCompatibility,
)
from .migrations import MigrationRunner
from .repository import RepositoryContext
from .sql_connector import SQLConnector
from .table_database_maintenance import (
    DatabaseMaintenanceResult,
    H2HDBDatabaseMaintenance,
)
from .table_database_setting import H2HDBCheckDatabaseSettings
from .table_gallery_ingest_coordination import (
    DownloadTurn,
    GalleryIngestState,
    GalleryIngestTurn,
    H2HDBGalleryIngestCoordination,
    _DownloadHandoffResult,
)
from .table_removed_gids import H2HDBRemovedGalleries
from .todelete_queue import H2HDBToDeleteQueue
from .todownload_queue import (
    DownloadRequest,
    EnsureDownloadRequestResult,
    H2HDBToDownloadQueue,
)


class CoordinatorUnavailableError(RuntimeError):
    pass


class IngestTurnLostError(RuntimeError):
    pass


_CONTRIBUTOR_TAGS = frozenset(
    {"artist", "author", "cosplayer", "group", "illustrator", "uploader"}
)


def _database_operation[**P, R](
    method: Callable[Concatenate[H2HDB, P], R],
) -> Callable[Concatenate[H2HDB, P], R]:
    """Make a public facade operation participate in the maintenance gate."""

    @wraps(method)
    def guarded(self: H2HDB, *args: P.args, **kwargs: P.kwargs) -> R:
        with self.database_gate():
            return method(self, *args, **kwargs)

    return guarded


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _deduplicate_contributors(
    contributors: Iterable[CatalogContributor],
) -> tuple[CatalogContributor, ...]:
    unique: list[CatalogContributor] = []
    seen: set[tuple[str, str]] = set()
    for contributor in contributors:
        key = (contributor.role, contributor.name)
        if key in seen:
            continue
        seen.add(key)
        unique.append(contributor)
    return tuple(unique)


def _derive_publications(snapshot: CatalogSnapshot) -> tuple[CatalogPublication, ...]:
    galleries_by_name = {
        gallery.gallery_name: gallery for gallery in snapshot.galleries
    }
    publications: list[CatalogPublication] = []
    for selection in snapshot.selections:
        gallery = galleries_by_name[selection.source_gallery_name]
        display_title = gallery.title or gallery.gallery_name
        contributors: list[CatalogContributor] = []
        if gallery.upload_account:
            contributors.append(
                CatalogContributor(name=gallery.upload_account, role="uploader")
            )
        contributors.extend(
            CatalogContributor(name=tag.value, role=tag.name)
            for tag in gallery.tags
            if tag.name in _CONTRIBUTOR_TAGS and tag.value
        )
        language = next(
            (tag.value for tag in gallery.tags if tag.name == "language" and tag.value),
            "und",
        )
        modified_at = max(
            (_as_utc(gallery.modified_time),)
            + tuple(_as_utc(artifact.modified_at) for artifact in selection.artifacts)
        )
        publications.append(
            CatalogPublication(
                publication_id=f"urn:h2h:gallery:{gallery.gid}",
                gid=gallery.gid,
                title=display_title,
                source_title=gallery.title,
                sort_title=display_title.casefold(),
                summary=gallery.comment,
                language=language,
                published_at=_as_utc(gallery.upload_time),
                modified_at=modified_at,
                contributors=_deduplicate_contributors(contributors),
                subjects=tuple(
                    CatalogSubject(
                        name=tag.value,
                        scheme=f"h2h:tag:{tag.name}",
                        code=tag.name,
                    )
                    for tag in gallery.tags
                ),
                artifacts=selection.artifacts,
                redownload_required=selection.redownload_required,
                source_gallery_name=gallery.gallery_name,
                content_sha256=gallery.content_sha256,
            )
        )
    return tuple(publications)


def _is_sqlite_lock_error(error: BaseException) -> bool:
    if not isinstance(error, sqlite3.OperationalError):
        return False
    code = getattr(error, "sqlite_errorcode", None)
    if code is None:
        message = str(error).casefold()
        return "locked" in message or "busy" in message
    return int(code) & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


class H2HDB:
    def __init__(self, config: CoreConfig) -> None:
        self.config = config
        self._context = RepositoryContext.from_config(config)
        self.logger = self._context.logger
        self._database_settings = H2HDBCheckDatabaseSettings(self._context)
        self._database_maintenance = H2HDBDatabaseMaintenance(
            self._context,
            self._database_settings,
        )
        self._gallery_ingest = H2HDBGalleryIngestCoordination(self._context)
        self._download_queue = H2HDBToDownloadQueue(self._context)
        self._to_delete_queue = H2HDBToDeleteQueue(self._context)
        self._removed_galleries = H2HDBRemovedGalleries(self._context)
        self._canonical = CanonicalSnapshotRepository(self._context)
        self._catalog = CatalogProjectionRepository(self._context)
        self._catalog_builds = CatalogBuildRepository(self._context)
        self._catalog_operations = CatalogOperationalRepository(
            self._context,
            self._catalog_builds,
            self._download_queue,
        )
        self._catalog_build_projections = CatalogBuildProjectionRepository(
            self._context,
            self._catalog_builds,
            self._catalog,
        )
        self._catalog_analysis = CatalogAnalysisRepository(
            self._context,
            self._catalog_builds,
        )
        self._migrations = MigrationRunner(self._context)
        self._database_gate_depth: ContextVar[int] = ContextVar(
            f"h2hdb_database_gate_depth_{id(self)}",
            default=0,
        )

    def __enter__(self) -> H2HDB:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def migrate(self) -> int:
        with self._database_maintenance.maintenance_gate():
            self._database_settings.check_database_character_set()
            self._database_settings.check_database_collation()
            return self._migrations.migrate()

    @_database_operation
    def check_compatibility(self) -> SchemaCompatibility:
        return self._migrations.check_compatibility()

    def check_readiness(self) -> SchemaCompatibility:
        """Check the schema ledger without waiting on the maintenance gate."""

        return self._migrations.check_readiness()

    @contextmanager
    def database_gate(
        self,
        *,
        timeout_seconds: int | None = None,
    ) -> Generator[None]:
        depth = self._database_gate_depth.get()
        if depth:
            token = self._database_gate_depth.set(depth + 1)
            try:
                yield
            finally:
                self._database_gate_depth.reset(token)
            return

        with self._database_maintenance.database_gate(timeout_seconds=timeout_seconds):
            token = self._database_gate_depth.set(1)
            try:
                yield
            finally:
                self._database_gate_depth.reset(token)

    def optimize_database(self) -> DatabaseMaintenanceResult:
        return self._database_maintenance.optimize_now()

    def run_scheduled_database_maintenance(self) -> DatabaseMaintenanceResult:
        return self._database_maintenance.run_scheduled_optimization()

    @_database_operation
    def record_catalog_changes(self, *, changed: int, removed: int) -> None:
        self._database_maintenance.record_gallery_changes(
            changed_galleries=changed,
            removed_galleries=removed,
        )

    def analyze_database(self) -> None:
        with self._database_maintenance.maintenance_gate():
            self._database_settings.analyze_database()

    @_database_operation
    def publish_snapshot(
        self,
        snapshot: CatalogSnapshot,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogPublishResult:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                if not self._gallery_ingest._gallery_ingest_turn_is_live_with_connector(
                    connector,
                    ingest_turn,
                ):
                    raise IngestTurnLostError(
                        "Catalog revision publish rejected because the ingest turn "
                        "is no longer live"
                    )
                lock = (
                    " FOR UPDATE" if self.config.database.sql_type == "mariadb" else ""
                )
                source_pointer = connector.fetch_one("""
                    SELECT active_build_id
                    FROM catalog_source_revision
                    WHERE singleton_id = 1
                    """ + lock)
                if not source_pointer:
                    raise RuntimeError("catalog_source_revision singleton is missing")
                if source_pointer[0] is not None:
                    raise CatalogBuildStateError(
                        "Legacy snapshot publication is disabled after source-build "
                        "activation"
                    )
                # Legacy publication may consume deletion markers.  Advancing
                # this scalar fence makes any concurrently prepared staged
                # effects refresh before their pointer cutover.
                connector.execute("""
                    UPDATE catalog_source_revision
                    SET deletion_request_generation =
                        deletion_request_generation + 1
                    WHERE singleton_id = 1
                    """)
                diff = self._canonical._sync_snapshot_with_connector(
                    connector,
                    snapshot.galleries,
                )
                publications = _derive_publications(snapshot)
                prepared = self._catalog._prepare_revision_with_connector(
                    connector,
                    publications,
                )
                for gid in diff.redownload_gids:
                    self._download_queue._ensure_download_request_with_connector(
                        connector,
                        gid,
                    )
                if prepared.created:
                    self._catalog._advance_revision_pointer_with_connector(
                        connector,
                        prepared.revision,
                    )
        return CatalogPublishResult(
            revision=prepared.revision,
            new_galleries=diff.new,
            changed_galleries=diff.changed,
            removed_galleries=diff.removed,
        )

    @contextmanager
    def _catalog_build_transaction(
        self,
        ingest_turn: GalleryIngestTurn,
    ) -> Generator[SQLConnector]:
        """Open one write transaction fenced by the database's live lease."""

        with self._context.SQLConnector() as connector:
            with connector.transaction():
                if not self._gallery_ingest._gallery_ingest_turn_is_live_with_connector(
                    connector,
                    ingest_turn,
                ):
                    raise IngestTurnLostError(
                        "Catalog build mutation rejected because the ingest turn "
                        "is no longer live"
                    )
                yield connector

    @_database_operation
    def begin_catalog_build(
        self,
        *,
        scope_key: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._begin_with_connector(
                connector,
                ingest_turn,
                scope_key,
            )

    @_database_operation
    def resume_catalog_build(
        self,
        *,
        scope_key: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild | None:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._resume_with_connector(
                connector,
                ingest_turn,
                scope_key,
            )

    @_database_operation
    def discover_catalog_galleries(
        self,
        build: CatalogBuild,
        gallery_names: Sequence[str],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        """Compatibility helper for flat roots without source observations."""

        discoveries = tuple(
            CatalogSourceGalleryDiscovery(
                gallery_name=name,
                source_locator=name,
            )
            for name in gallery_names
        )
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._discover_with_connector(
                connector,
                build.build_id,
                discoveries,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def discover_catalog_sources(
        self,
        build: CatalogBuild,
        discoveries: Sequence[CatalogSourceGalleryDiscovery],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._discover_with_connector(
                connector,
                build.build_id,
                discoveries,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_discovery(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
        completion: CatalogSourceDiscoveryCompletion | None = None,
    ) -> CatalogBuild:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._complete_discovery_with_connector(
                connector,
                build.build_id,
                ingest_turn,
                completion,
            )

    @_database_operation
    def begin_catalog_gallery(
        self,
        build: CatalogBuild,
        header: CatalogSourceGalleryHeader,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._begin_gallery_with_connector(
                connector,
                build.build_id,
                header,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def stage_catalog_gallery_headers(
        self,
        build: CatalogBuild,
        headers: Sequence[CatalogSourceGalleryHeader],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._stage_gallery_headers_with_connector(
                connector,
                build.build_id,
                headers,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def stage_catalog_file_chunk(
        self,
        build: CatalogBuild,
        gallery_name: str,
        files: Sequence[GallerySourceFile],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._stage_file_chunk_with_connector(
                connector,
                build.build_id,
                gallery_name,
                files,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def stage_catalog_file_chunks(
        self,
        build: CatalogBuild,
        chunks: Sequence[CatalogSourceFileChunk],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._stage_file_chunks_with_connector(
                connector,
                build.build_id,
                chunks,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_gallery(
        self,
        build: CatalogBuild,
        completion: CatalogSourceGalleryCompletion,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._complete_gallery_with_connector(
                connector,
                build.build_id,
                completion,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_galleries(
        self,
        build: CatalogBuild,
        completions: Sequence[CatalogSourceGalleryCompletion],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._complete_galleries_with_connector(
                connector,
                build.build_id,
                completions,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_source_staging(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._complete_source_staging_with_connector(
                connector,
                build.build_id,
                ingest_turn,
            )

    @_database_operation
    def stage_catalog_analysis(
        self,
        build: CatalogBuild,
        analyses: Sequence[CatalogSourceGalleryAnalysis],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._stage_analysis_with_connector(
                connector,
                build.build_id,
                analyses,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_analysis(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._complete_analysis_with_connector(
                connector,
                build.build_id,
                ingest_turn,
            )

    @_database_operation
    def cache_catalog_file_hashes(
        self,
        build: CatalogBuild,
        entries: Sequence[FileHashCacheEntry],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._cache_hashes_with_connector(
                connector,
                build.build_id,
                entries,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def get_catalog_file_hashes(
        self,
        keys: Sequence[FileHashCacheKey],
    ) -> Mapping[FileHashCacheKey, str]:
        return self._catalog_builds.get_file_hashes(keys)

    @_database_operation
    def seal_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._seal_with_connector(
                connector,
                build.build_id,
                ingest_turn,
            )

    @_database_operation
    def prepare_catalog_build_operations(
        self,
        build: CatalogBuild,
        *,
        max_rows: int = 1000,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildOperationalState:
        """Advance one bounded page of invisible operational cutover work.

        The same SEALED build may be refreshed after a deletion-generation
        race.  Callers repeat this operation until ``state.complete`` before
        attempting joint publication.
        """

        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_operations.prepare(
                connector,
                build.build_id,
                max_rows=max_rows,
                turn=ingest_turn,
            )

    @_database_operation
    def abandon_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        """Terminally abandon partial source rows while retaining hash cache."""

        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_builds._abandon_with_connector(
                connector,
                build.build_id,
                ingest_turn,
            )

    @_database_operation
    def publish_catalog_build(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildPublishResult:
        """Activate a sealed source snapshot with a short pointer transaction.

        This does not publish the legacy/user-facing catalog projection. That
        projection needs its own pre-staged immutable rows before both pointers
        can be swapped as one complete catalog publication.
        """

        # Source-only publication is retained for compatibility.  It performs
        # the same bounded operational preparation as joint publication before
        # entering its short pointer transaction.
        if build.phase.value != "PUBLISHED":
            while True:
                with self._catalog_build_transaction(ingest_turn) as connector:
                    state = self._catalog_operations.prepare(
                        connector,
                        build.build_id,
                        max_rows=1000,
                        turn=ingest_turn,
                    )
                if state.complete:
                    break
        with self._catalog_build_transaction(ingest_turn) as connector:
            self._catalog_builds._require_owned_build(
                connector,
                build.build_id,
                ingest_turn,
            )
            authority = self._catalog_operations.active_authority(
                connector,
                for_update=True,
            )
            if authority is not None and authority[0] == build.build_id:
                return self._catalog_builds._publish_with_connector(
                    connector,
                    build.build_id,
                    ingest_turn,
                )
            state = self._catalog_operations.require_ready_for_activation(
                connector,
                build.build_id,
            )
            result = self._catalog_builds._publish_with_connector(
                connector,
                build.build_id,
                ingest_turn,
            )
            self._catalog_operations.activate(
                connector,
                state,
                source_revision=result.source_revision,
                activated_at=result.build.updated_at,
            )
            return result

    @_database_operation
    def prune_catalog_build(
        self,
        build_id: str,
        *,
        max_rows: int = 1000,
    ) -> CatalogBuildPruneResult:
        return self._catalog_builds.prune_build(build_id, max_rows=max_rows)

    @_database_operation
    def list_catalog_build_cleanup_candidates(
        self,
        *,
        limit: int = 100,
    ) -> tuple[CatalogBuild, ...]:
        return self._catalog_builds.list_cleanup_candidates(limit=limit)

    @_database_operation
    def get_catalog_build(self, build_id: str) -> CatalogBuild | None:
        return self._catalog_builds.get_build(build_id)

    @_database_operation
    def get_working_catalog_build(self) -> CatalogBuild | None:
        return self._catalog_builds.get_working_build()

    @_database_operation
    def get_active_catalog_build(self) -> CatalogBuild | None:
        return self._catalog_builds.get_active_build()

    @_database_operation
    def get_catalog_source_revision(
        self,
        revision: int | None = None,
    ) -> CatalogSourceRevision:
        return self._catalog_builds.get_source_revision(revision)

    @_database_operation
    def list_catalog_sources(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogSourceRevision | None = None,
    ) -> CatalogSourcePage:
        return self._catalog_builds.list_sources(
            offset=offset,
            limit=limit,
            revision=revision,
        )

    @_database_operation
    def list_catalog_build_sources(
        self,
        build_id: str,
        *,
        offset: int = 0,
        limit: int = 50,
    ) -> CatalogBuildSourcePage:
        return self._catalog_builds.list_build_sources(
            build_id,
            offset=offset,
            limit=limit,
        )

    @_database_operation
    def list_pending_catalog_galleries(
        self,
        build_id: str,
        *,
        after_gallery_name: str | None = None,
        limit: int = 100,
    ) -> CatalogPendingGalleryPage:
        return self._catalog_builds.list_pending_galleries(
            build_id,
            after_gallery_name=after_gallery_name,
            limit=limit,
        )

    @_database_operation
    def list_catalog_build_files(
        self,
        build_id: str,
        gallery_name: str,
        *,
        after: CatalogSourceFileCursor | None = None,
        limit: int = 100,
    ) -> CatalogSourceFilePage:
        return self._catalog_builds.list_build_files(
            build_id,
            gallery_name,
            after=after,
            limit=limit,
        )

    @_database_operation
    def is_catalog_analysis_phase_complete(
        self,
        build_id: str,
        phase: CatalogAnalysisPhase,
    ) -> bool:
        return self._catalog_analysis.is_phase_complete(build_id, phase)

    @_database_operation
    def list_catalog_source_manifest_rows(
        self,
        build_id: str,
        *,
        after: CatalogSourceManifestCursor | None = None,
        limit: int = 1000,
    ) -> CatalogSourceManifestPage:
        return self._catalog_analysis.list_source_manifest_rows(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_source_manifests(
        self,
        build: CatalogBuild,
        manifests: Sequence[CatalogSourceManifest],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_source_manifests(
                connector,
                build.build_id,
                manifests,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_file_hash_aggregates(
        self,
        build_id: str,
        *,
        after: CatalogFileHashAggregateCursor | None = None,
        limit: int = 1000,
    ) -> CatalogFileHashAggregatePage:
        return self._catalog_analysis.list_file_hash_aggregates(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_excluded_file_hashes(
        self,
        build: CatalogBuild,
        hashes: Sequence[str],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_excluded_file_hashes(
                connector,
                build.build_id,
                hashes,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: CatalogGalleryFileHashCursor | None = None,
        limit: int = 1000,
    ) -> CatalogGalleryFileHashPage:
        return self._catalog_analysis.list_gallery_file_hashes(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_content_digests(
        self,
        build: CatalogBuild,
        digests: Sequence[CatalogContentDigest],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_content_digests(
                connector,
                build.build_id,
                digests,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_content_candidates(
        self,
        build_id: str,
        *,
        after: CatalogContentCandidateCursor | None = None,
        limit: int = 1000,
    ) -> CatalogContentCandidatePage:
        return self._catalog_analysis.list_content_candidates(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_content_owners(
        self,
        build: CatalogBuild,
        owners: Sequence[CatalogContentOwner],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_content_owners(
                connector,
                build.build_id,
                owners,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_gid_candidates(
        self,
        build_id: str,
        *,
        after: CatalogGidCandidateCursor | None = None,
        limit: int = 1000,
    ) -> CatalogGidCandidatePage:
        return self._catalog_analysis.list_gid_candidates(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_gid_winners(
        self,
        build: CatalogBuild,
        winners: Sequence[CatalogGidWinner],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_gid_winners(
                connector,
                build.build_id,
                winners,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_final_analyses(
        self,
        build_id: str,
        *,
        after: CatalogFinalAnalysisCursor | None = None,
        limit: int = 1000,
    ) -> CatalogFinalAnalysisPage:
        return self._catalog_analysis.list_final_analyses(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_final_analyses(
        self,
        build: CatalogBuild,
        analyses: Sequence[CatalogSourceGalleryAnalysis],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.stage_final_analyses(
                connector,
                build.build_id,
                analyses,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_analysis_phase(
        self,
        build: CatalogBuild,
        phase: CatalogAnalysisPhase,
        *,
        ingest_turn: GalleryIngestTurn,
        scan_completion: CatalogAnalysisScanCompletion | None = None,
    ) -> CatalogAnalysisPhaseCheckpoint:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_analysis.complete_phase(
                connector,
                build.build_id,
                phase,
                turn=ingest_turn,
                scan_completion=scan_completion,
            )

    @_database_operation
    def begin_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        artifacts_required: bool,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections._begin_with_connector(
                connector,
                build.build_id,
                artifacts_required=artifacts_required,
                turn=ingest_turn,
            )

    @_database_operation
    def get_catalog_build_projection(
        self,
        build_id: str,
    ) -> CatalogBuildProjection | None:
        return self._catalog_build_projections.get_projection(build_id)

    @_database_operation
    def get_catalog_projection_checkpoint(
        self,
        build_id: str,
    ) -> CatalogProjectionCheckpoint:
        return self._catalog_build_projections.get_checkpoint(build_id)

    @_database_operation
    def list_catalog_projection_selected_galleries(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectedGalleryCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectedGalleryPage:
        return self._catalog_build_projections.page_selected_galleries(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def list_catalog_projection_selected_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: CatalogProjectionSelectedFileCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectedFilePage:
        return self._catalog_build_projections.page_selected_files(
            build_id,
            gallery_key,
            after=after,
            limit=limit,
        )

    @_database_operation
    def record_catalog_prepared_artifact(
        self,
        build: CatalogBuild,
        prepared: CatalogPreparedArtifact,
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.record_prepared_artifact(
                connector,
                build.build_id,
                prepared,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def record_catalog_prepared_artifacts(
        self,
        build: CatalogBuild,
        prepared: Sequence[CatalogPreparedArtifact],
        *,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.record_prepared_artifacts(
                connector,
                build.build_id,
                prepared,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def advance_catalog_artifact_checkpoint(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        after: CatalogProjectionSelectedGalleryCursor,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.advance_artifact_checkpoint(
                connector,
                build.build_id,
                expected_after=expected_after,
                after=after,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_artifact_preparation(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.complete_artifact_preparation(
                connector,
                build.build_id,
                expected_after=expected_after,
                turn=ingest_turn,
            )

    @_database_operation
    def list_catalog_projection_selections(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectionCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionSelectionPage:
        return self._catalog_build_projections.page_projection_selections(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def stage_catalog_projection_selections(
        self,
        build: CatalogBuild,
        selections: Sequence[CatalogProjectionSelection],
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        after: CatalogProjectionSelectionCursor,
        batch_id: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.stage_projection_selections(
                connector,
                build.build_id,
                selections,
                expected_after=expected_after,
                after=after,
                batch_id=batch_id,
                turn=ingest_turn,
            )

    @_database_operation
    def complete_catalog_projection_staging(
        self,
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.complete_projection_staging(
                connector,
                build.build_id,
                expected_after=expected_after,
                turn=ingest_turn,
            )

    @_database_operation
    def seal_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.seal_projection(
                connector,
                build.build_id,
                turn=ingest_turn,
            )

    @_database_operation
    def publish_catalog_build_with_projection(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionPublishResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            existing_receipt = connector.fetch_one(
                """
                SELECT 1
                FROM catalog_projection_publication_receipts
                WHERE build_id = %s
                """,
                (build.build_id,),
            )
            if existing_receipt:
                return self._catalog_build_projections.publish_with_projection(
                    connector,
                    build.build_id,
                    turn=ingest_turn,
                )
            self._catalog_builds._require_owned_build(
                connector,
                build.build_id,
                ingest_turn,
            )
            self._catalog_build_projections._require_projection(
                connector,
                build.build_id,
                for_update=True,
            )
            state = self._catalog_operations.require_ready_for_activation(
                connector,
                build.build_id,
            )
            result = self._catalog_build_projections.publish_with_projection(
                connector,
                build.build_id,
                turn=ingest_turn,
            )
            self._catalog_operations.activate(
                connector,
                state,
                source_revision=result.receipt.source_revision,
                activated_at=result.receipt.committed_at,
            )
            self._database_maintenance._record_gallery_changes_with_connector(
                connector,
                changed_galleries=(
                    result.receipt.new_galleries + result.receipt.changed_galleries
                ),
                removed_galleries=result.receipt.removed_galleries,
            )
            return result

    @_database_operation
    def get_catalog_projection_publication_receipt(
        self,
        build_id: str | None = None,
        *,
        pending_only: bool = False,
    ) -> CatalogProjectionPublicationReceipt | None:
        return self._catalog_build_projections.get_publication_receipt(
            build_id,
            pending_only=pending_only,
        )

    @_database_operation
    def list_published_catalog_projection_artifacts(
        self,
        build_id: str,
        *,
        after: CatalogProjectionArtifactCursor | None = None,
        limit: int = 100,
    ) -> CatalogProjectionArtifactPage:
        return self._catalog_build_projections.page_published_artifacts(
            build_id,
            after=after,
            limit=limit,
        )

    @_database_operation
    def acknowledge_catalog_projection_finalized(
        self,
        build: CatalogBuild,
        *,
        catalog_revision: int,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.acknowledge_finalized(
                connector,
                build.build_id,
                catalog_revision=catalog_revision,
                turn=ingest_turn,
            )

    @_database_operation
    def prune_catalog_build_projection(
        self,
        build: CatalogBuild,
        *,
        max_rows: int = 1000,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionPruneResult:
        with self._catalog_build_transaction(ingest_turn) as connector:
            return self._catalog_build_projections.prune_projection(
                connector,
                build.build_id,
                max_rows=max_rows,
            )

    @_database_operation
    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision:
        return self._catalog.get_catalog_revision(revision)

    @_database_operation
    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | None = None,
        require_artifact: bool = False,
    ) -> CatalogPage:
        return self._catalog.list_publications(
            query=query,
            offset=offset,
            limit=limit,
            revision=revision,
            require_artifact=require_artifact,
        )

    @_database_operation
    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogPublication | None:
        return self._catalog.get_publication(publication_id, revision=revision)

    @_database_operation
    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | None = None,
    ) -> Mapping[str, CatalogPublication]:
        return self._catalog.get_publications_by_artifact_names(
            names,
            revision=revision,
        )

    @_database_operation
    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogArtifact | None:
        return self._catalog.get_artifact(artifact_id, revision=revision)

    @_database_operation
    def request_download(self, gid: int, url: str = "") -> DownloadRequest:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                return self._catalog_operations.request_download_with_connector(
                    connector,
                    gid,
                    url,
                )

    @_database_operation
    def ensure_download_request(
        self,
        gid: int,
        url: str = "",
    ) -> EnsureDownloadRequestResult:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                return self._catalog_operations.ensure_download_request_with_connector(
                    connector,
                    gid,
                    url,
                )

    @_database_operation
    def get_download_request(self, gid: int) -> DownloadRequest | None:
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                return self._catalog_operations.get_download_request_with_connector(
                    connector,
                    gid,
                )

    @_database_operation
    def get_download_requests(self) -> list[DownloadRequest]:
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                return self._catalog_operations.get_download_requests_with_connector(
                    connector
                )

    @_database_operation
    def get_candidate_states(
        self,
        gids: Sequence[int],
    ) -> Mapping[int, DownloadCandidateState]:
        ordered_gids = tuple(dict.fromkeys(gids))
        if any(gid <= 0 for gid in ordered_gids):
            raise ValueError("Candidate GIDs must be positive")
        if not ordered_gids:
            return {}
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                cataloged = self._catalog_operations.cataloged_gids(
                    connector,
                    ordered_gids,
                )
                redownload_required = set(
                    self._catalog_operations.pending_redownload_gids(
                        connector,
                        ordered_gids,
                    )
                )
                requested = self._catalog_operations.requested_gids(
                    connector,
                    ordered_gids,
                )
        return {
            gid: DownloadCandidateState(
                gid=gid,
                cataloged=gid in cataloged,
                redownload_required=gid in redownload_required,
                requested=gid in requested,
            )
            for gid in ordered_gids
        }

    @_database_operation
    def get_pending_redownload_gids(self) -> list[int]:
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                return self._catalog_operations.pending_redownload_gids(connector)

    @_database_operation
    def complete_download_request(self, request: DownloadRequest) -> None:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                self._catalog_operations.complete_download_request_with_connector(
                    connector,
                    request,
                )

    @staticmethod
    def _validate_missing_request(request: DownloadRequest, gid: int) -> None:
        if request.gid != gid:
            raise ValueError(
                f"Download request GID {request.gid} does not match missing GID {gid}"
            )

    @_database_operation
    def complete_missing_download_request(
        self,
        request: DownloadRequest,
        gid: int,
    ) -> None:
        self._validate_missing_request(request, gid)
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                if self._catalog_operations.complete_download_request_with_connector(
                    connector,
                    request,
                ):
                    self._removed_galleries._insert_removed_gallery_gid_with_connector(
                        connector,
                        gid,
                    )

    @_database_operation
    def record_gallery_found(self, *gids: int) -> None:
        unique_gids = tuple(dict.fromkeys(gids))
        if any(gid <= 0 for gid in unique_gids):
            raise ValueError("Found gallery GIDs must be positive")
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                for gid in unique_gids:
                    self._removed_galleries._delete_removed_gallery_gid_with_connector(
                        connector,
                        gid,
                    )

    @_database_operation
    def record_accepted_submission(
        self,
        gid: int,
        *,
        request: DownloadRequest | None = None,
    ) -> None:
        if gid <= 0:
            raise ValueError("Submitted gallery GID must be positive")
        if request is not None:
            self._validate_missing_request(request, gid)
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                self._catalog_operations.active_authority(
                    connector,
                    for_update=True,
                )
                if (
                    request is not None
                    and not self._catalog_operations.complete_download_request_with_connector(
                        connector,
                        request,
                    )
                ):
                    return
                self._removed_galleries._delete_removed_gallery_gid_with_connector(
                    connector,
                    gid,
                )
                if self._catalog_operations.record_accepted_runtime(connector, gid):
                    return
                match self.config.database.sql_type:
                    case "mariadb":
                        connector.execute(
                            """
                            UPDATE galleries_redownload_times AS redownload
                            JOIN galleries_gids AS gids
                                ON gids.db_gallery_id = redownload.db_gallery_id
                            SET redownload.time = NOW()
                            WHERE gids.gid = %s
                            """,
                            (gid,),
                        )
                    case "sqlite":
                        connector.execute(
                            """
                            UPDATE galleries_redownload_times
                            SET time = datetime('now')
                            WHERE db_gallery_id IN (
                                SELECT db_gallery_id
                                FROM galleries_gids
                                WHERE gid = %s
                            )
                            """,
                            (gid,),
                        )

    @_database_operation
    def request_gallery_deletion(self, gid: int) -> None:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                self._catalog_operations.request_gallery_deletion_with_connector(
                    connector,
                    gid,
                )

    @_database_operation
    def get_gallery_deletion_requests(self) -> list[int]:
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                return self._catalog_operations.effective_deletion_gids(connector)

    @_database_operation
    def claim_download_turn(self, *, lease_seconds: int) -> DownloadTurn | None:
        try:
            return self._gallery_ingest.claim_download_turn(lease_seconds=lease_seconds)
        except BaseException as error:
            if _is_sqlite_lock_error(error):
                return None
            raise

    @_database_operation
    def renew_download_turn(
        self,
        turn: DownloadTurn,
        *,
        lease_seconds: int,
    ) -> bool:
        try:
            return self._gallery_ingest.renew_download_turn(
                turn,
                lease_seconds=lease_seconds,
            )
        except BaseException as error:
            if _is_sqlite_lock_error(error):
                return False
            raise

    @_database_operation
    def request_gallery_ingest(self, turn: DownloadTurn) -> bool:
        return self._gallery_ingest.request_gallery_ingest(turn)

    @_database_operation
    def complete_download_request_in_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
    ) -> bool:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                if not self._gallery_ingest._download_turn_is_live_with_connector(
                    connector,
                    turn,
                ):
                    return False
                self._catalog_operations.complete_download_request_with_connector(
                    connector,
                    request,
                )
        return True

    @_database_operation
    def complete_missing_download_request_in_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
        gid: int,
    ) -> bool:
        self._validate_missing_request(request, gid)
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                if not self._gallery_ingest._download_turn_is_live_with_connector(
                    connector,
                    turn,
                ):
                    return False
                if self._catalog_operations.complete_download_request_with_connector(
                    connector,
                    request,
                ):
                    self._removed_galleries._insert_removed_gallery_gid_with_connector(
                        connector,
                        gid,
                    )
        return True

    @_database_operation
    def finish_download_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
    ) -> bool:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                handoff = self._gallery_ingest._handoff_download_turn_with_connector(
                    connector,
                    turn,
                )
                if handoff is _DownloadHandoffResult.rejected:
                    return False
                if handoff is _DownloadHandoffResult.accepted:
                    self._catalog_operations.complete_download_request_with_connector(
                        connector,
                        request,
                    )
        return True

    @_database_operation
    def finish_missing_download_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
        gid: int,
    ) -> bool:
        self._validate_missing_request(request, gid)
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                handoff = self._gallery_ingest._handoff_download_turn_with_connector(
                    connector,
                    turn,
                )
                if handoff is _DownloadHandoffResult.rejected:
                    return False
                if handoff is _DownloadHandoffResult.accepted:
                    if self._catalog_operations.complete_download_request_with_connector(
                        connector,
                        request,
                    ):
                        self._removed_galleries._insert_removed_gallery_gid_with_connector(
                            connector,
                            gid,
                        )
        return True

    @_database_operation
    def get_gallery_ingest_state(self) -> GalleryIngestState:
        try:
            return self._gallery_ingest.get_state()
        except BaseException as error:
            if _is_sqlite_lock_error(error):
                raise CoordinatorUnavailableError(
                    "Gallery ingest state is temporarily locked"
                ) from error
            raise

    @_database_operation
    def claim_gallery_ingest(
        self,
        *,
        lease_seconds: int,
        periodic_scan: bool,
    ) -> GalleryIngestTurn | None:
        try:
            return self._gallery_ingest.claim_gallery_ingest(
                lease_seconds=lease_seconds,
                periodic_scan=periodic_scan,
            )
        except BaseException as error:
            if _is_sqlite_lock_error(error):
                return None
            raise

    @_database_operation
    def renew_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int | None:
        return self._gallery_ingest.renew_gallery_ingest_lease(
            turn,
            lease_seconds=lease_seconds,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        )

    @_database_operation
    def complete_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        allow_expired_sqlite_lease: bool = False,
    ) -> bool:
        return self._gallery_ingest.complete_gallery_ingest(
            turn,
            allow_expired_sqlite_lease=allow_expired_sqlite_lease,
        )


def open_database(config: CoreConfig, *, require_compatible: bool = True) -> H2HDB:
    database = H2HDB(config)
    if require_compatible:
        database.check_compatibility()
    return database
