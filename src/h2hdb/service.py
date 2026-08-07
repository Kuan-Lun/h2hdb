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
from .catalog_repository import CatalogProjectionRepository
from .config_loader import CoreConfig
from .domain import (
    CatalogArtifact,
    CatalogContributor,
    CatalogPage,
    CatalogPublication,
    CatalogPublishResult,
    CatalogRevision,
    CatalogSnapshot,
    CatalogSubject,
    DownloadCandidateState,
    SchemaCompatibility,
)
from .migrations import MigrationRunner
from .repository import RepositoryContext
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
        return self._download_queue.request_download(gid, url)

    @_database_operation
    def ensure_download_request(
        self,
        gid: int,
        url: str = "",
    ) -> EnsureDownloadRequestResult:
        return self._download_queue.ensure_download_request(gid, url)

    @_database_operation
    def get_download_request(self, gid: int) -> DownloadRequest | None:
        return self._download_queue.get_download_request(gid)

    @_database_operation
    def get_download_requests(self) -> list[DownloadRequest]:
        return self._download_queue.get_download_requests()

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
        placeholders = ", ".join("%s" for _ in ordered_gids)
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                catalog_rows = connector.fetch_all(
                    f"SELECT gid FROM galleries_gids WHERE gid IN ({placeholders})",
                    ordered_gids,
                )
                redownload_rows = connector.fetch_all(
                    f"SELECT gid FROM pending_download_gids "
                    f"WHERE gid IN ({placeholders})",
                    ordered_gids,
                )
                request_rows = connector.fetch_all(
                    f"SELECT gid FROM todownload_gids WHERE gid IN ({placeholders})",
                    ordered_gids,
                )
        cataloged = {int(row[0]) for row in catalog_rows}
        redownload_required = {int(row[0]) for row in redownload_rows}
        requested = {int(row[0]) for row in request_rows}
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
        return self._download_queue.get_pending_download_gids()

    @_database_operation
    def complete_download_request(self, request: DownloadRequest) -> None:
        self._download_queue.complete_download_request(request)

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
                if self._download_queue._complete_download_request_with_connector(
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
                if (
                    request is not None
                    and not self._download_queue._complete_download_request_with_connector(
                        connector,
                        request,
                    )
                ):
                    return
                self._removed_galleries._delete_removed_gallery_gid_with_connector(
                    connector,
                    gid,
                )
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
        self._to_delete_queue.request_gallery_deletion(gid)

    @_database_operation
    def get_gallery_deletion_requests(self) -> list[int]:
        return self._to_delete_queue.get_gallery_deletion_requests()

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
                self._download_queue._complete_download_request_with_connector(
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
                if self._download_queue._complete_download_request_with_connector(
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
                    self._download_queue._complete_download_request_with_connector(
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
                    if self._download_queue._complete_download_request_with_connector(
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
