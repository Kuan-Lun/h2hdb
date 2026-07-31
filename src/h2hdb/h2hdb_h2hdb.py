__all__ = [
    "H2HDB",
    "GalleryScan",
    "SyncOutcome",
    "DatabaseMaintenanceResult",
    "DownloadRequest",
    "DownloadTurn",
    "GalleryIngestPhase",
    "GalleryIngestState",
    "GALLERY_INFO_FILE_NAME",
]


import contextlib
import hashlib
from collections.abc import Generator
from dataclasses import dataclass
from itertools import islice
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from pathlib import Path
from time import monotonic

from h2h_galleryinfo_parser import (
    GalleryInfoParser,
    parse_galleryinfo,
)

from .cbz_files import (
    CBZCompressionSummary,
    ExistingCBZPolicy,
    H2HDBCBZFiles,
)
from .config_loader import H2HDBConfig
from .duplicated_hashes import H2HDBDuplicatedHashes
from .gallery_deduplication import (
    ContentClaim,
    H2HDBGalleryDeduplication,
)
from .gallery_source_manifest import (
    GalleryChange,
    build_gallery_source_manifest,
)
from .hash_dict import HASH_ALGORITHMS
from .information import FileInformation, TagInformation
from .repository import BaseRepository, RepositoryContext
from .settings import (
    COMPARISON_HASH_ALGORITHM,
    GALLERY_INFO_FILE_NAME,
    chunk_list,
    hash_function_by_file,
)
from .table_comments import H2HDBGalleriesComments
from .table_database_maintenance import (
    DatabaseMaintenanceResult,
    H2HDBDatabaseMaintenance,
)
from .table_database_setting import H2HDBCheckDatabaseSettings
from .table_files_dbids import H2HDBFiles
from .table_gallery_ingest_coordination import (
    DownloadTurn,
    GalleryIngestPhase,
    GalleryIngestState,
    GalleryIngestTurn,
    H2HDBGalleryIngestCoordination,
    _DownloadHandoffResult,
)
from .table_gallery_source_manifests import H2HDBGallerySourceManifests
from .table_gids import H2HDBGalleriesGIDs, H2HDBGalleriesIDs
from .table_pending_cbz_rebuilds import H2HDBPendingCBZRebuilds
from .table_pending_removals import H2HDBPendingGalleryRemovals
from .table_removed_gids import H2HDBRemovedGalleries
from .table_tags import H2HDBGalleriesTags
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles
from .table_uploadaccounts import H2HDBUploadAccounts
from .todelete_queue import H2HDBToDeleteQueue
from .todownload_queue import DownloadRequest, H2HDBToDownloadQueue
from .view_ginfo import H2HDBGalleriesInfos

GALLERY_METADATA_BATCH_SIZE = 500
CPU_NUM = cpu_count()
POOL_CPU_LIMIT = max(CPU_NUM - 2, 1)
PROGRESSIVE_GALLERIES_PER_WORKER = 16
PROGRESSIVE_GALLERY_CHUNK_SIZE = min(
    GALLERY_METADATA_BATCH_SIZE,
    max(64, POOL_CPU_LIMIT * PROGRESSIVE_GALLERIES_PER_WORKER),
)
PERF_HEARTBEAT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class GalleryScan:
    folders: tuple[Path, ...]
    names: frozenset[str]
    removed_galleries: int


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    new_galleries: int
    changed_galleries: int
    removed_galleries: int

    @property
    def has_changes(self) -> bool:
        return bool(
            self.new_galleries or self.changed_galleries or self.removed_galleries
        )

    @property
    def needs_immediate_rescan(self) -> bool:
        return bool(self.new_galleries or self.changed_galleries)

    @property
    def maintenance_work(self) -> int:
        return self.changed_galleries + self.removed_galleries


def _cbz_summary_fields(summary: CBZCompressionSummary) -> str:
    return (
        f"checked={summary.checked} created={summary.created} "
        f"rebuilt={summary.rebuilt} unchanged={summary.unchanged}"
    )


class H2HDB(BaseRepository):
    def __init__(self, config: H2HDBConfig) -> None:
        context = RepositoryContext.from_config(config)
        super().__init__(context)

        self.database_settings = H2HDBCheckDatabaseSettings(context)
        self.database_maintenance = H2HDBDatabaseMaintenance(
            context, self.database_settings
        )
        self.gallery_ingest = H2HDBGalleryIngestCoordination(context)
        self.todownload_queue = H2HDBToDownloadQueue(context)
        self.todelete_queue = H2HDBToDeleteQueue(context)
        self.gallery_ids = H2HDBGalleriesIDs(context)
        self.pending_removals = H2HDBPendingGalleryRemovals(
            context, self.gallery_ids, self.todownload_queue
        )
        self.gallery_gids = H2HDBGalleriesGIDs(context, self.gallery_ids)
        self.gallery_times = H2HDBTimes(context, self.gallery_ids)
        self.gallery_titles = H2HDBGalleriesTitles(context, self.gallery_ids)
        self.upload_accounts = H2HDBUploadAccounts(context, self.gallery_ids)
        self.gallery_infos = H2HDBGalleriesInfos(context)
        self.gallery_comments = H2HDBGalleriesComments(context, self.gallery_ids)
        self.gallery_tags = H2HDBGalleriesTags(context, self.gallery_ids)
        self.files = H2HDBFiles(context, self.gallery_ids)
        self.gallery_source_manifests = H2HDBGallerySourceManifests(
            context, self.gallery_ids
        )
        self.pending_cbz_rebuilds = H2HDBPendingCBZRebuilds(context)
        self.removed_galleries = H2HDBRemovedGalleries(context)
        self.cbz = H2HDBCBZFiles(context, self.gallery_times, self.gallery_ids)
        self.gallery_deduplication = H2HDBGalleryDeduplication(
            context, self.gallery_ids, self.gallery_times, self.gallery_titles
        )
        self.duplicated_hashes = H2HDBDuplicatedHashes(context)

    def __enter__(self) -> H2HDB:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        if exc_type is None:
            with self.SQLConnector() as connector:
                connector.commit()

    def check_database_character_set(self) -> None:
        self.database_settings.check_database_character_set()

    def check_database_collation(self) -> None:
        self.database_settings.check_database_collation()

    def insert_pending_gallery_removal(self, gallery_name: str) -> None:
        self.pending_removals.insert_pending_gallery_removal(gallery_name)

    def insert_pending_gallery_removals(self, gallery_names: list[str]) -> None:
        self.pending_removals.insert_pending_gallery_removals(gallery_names)

    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        return self.pending_removals.check_pending_gallery_removal(gallery_name)

    def get_pending_gallery_removals(self) -> list[str]:
        return self.pending_removals.get_pending_gallery_removals()

    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        self.pending_removals.delete_pending_gallery_removal(gallery_name)

    def delete_pending_gallery_removals_by_names(
        self, gallery_names: list[str]
    ) -> None:
        self.pending_removals.delete_pending_gallery_removals_by_names(gallery_names)

    def refresh_gallery(self, gallery_name: str) -> None:
        self.pending_removals.refresh_gallery(gallery_name)

    def refresh_galleries(self, gallery_names: list[str]) -> None:
        self.pending_removals.refresh_galleries(gallery_names)

    def optimize_database(self) -> DatabaseMaintenanceResult:
        return self.database_maintenance.optimize_now()

    def run_scheduled_database_maintenance(self) -> DatabaseMaintenanceResult:
        return self.database_maintenance.run_scheduled_optimization()

    @contextlib.contextmanager
    def database_gate(self, *, timeout_seconds: int | None = None) -> Generator[None]:
        with self.database_maintenance.database_gate(timeout_seconds=timeout_seconds):
            yield

    def analyze_database(self) -> None:
        with self.database_gate():
            self.database_settings.analyze_database()

    def get_pending_download_gids(self) -> list[int]:
        return self.todownload_queue.get_pending_download_gids()

    def is_gallery_deletion_requested(self, gid: int) -> bool:
        return self.todelete_queue.is_gallery_deletion_requested(gid)

    def request_gallery_deletion(self, gid: int) -> None:
        self.todelete_queue.request_gallery_deletion(gid)

    def request_download(self, gid: int, url: str = "") -> DownloadRequest:
        return self.todownload_queue.request_download(gid, url)

    def get_download_request(self, gid: int) -> DownloadRequest | None:
        return self.todownload_queue.get_download_request(gid)

    def complete_download_request(self, request: DownloadRequest) -> None:
        self.todownload_queue.complete_download_request(request)

    @staticmethod
    def _validate_missing_download_request(
        request: DownloadRequest,
        gid: int,
    ) -> None:
        if request.gid != gid:
            raise ValueError(
                f"Download request GID {request.gid} does not match missing "
                f"gallery GID {gid}."
            )

    def complete_missing_download_request(
        self,
        request: DownloadRequest,
        gid: int,
    ) -> None:
        self._validate_missing_download_request(request, gid)
        with self.SQLConnector() as connector:
            with connector.transaction():
                if self.todownload_queue._complete_download_request_with_connector(
                    connector,
                    request,
                ):
                    self.removed_galleries._insert_removed_gallery_gid_with_connector(
                        connector,
                        gid,
                    )

    def clear_removed_gallery_gid(self, gid: int) -> None:
        self.removed_galleries.delete_removed_gallery_gid(gid)

    def get_download_requests(self) -> list[DownloadRequest]:
        return self.todownload_queue.get_download_requests()

    def claim_download_turn(self, *, lease_seconds: int) -> DownloadTurn | None:
        return self.gallery_ingest.claim_download_turn(lease_seconds=lease_seconds)

    def renew_download_turn(
        self,
        turn: DownloadTurn,
        *,
        lease_seconds: int,
    ) -> bool:
        return self.gallery_ingest.renew_download_turn(
            turn,
            lease_seconds=lease_seconds,
        )

    def request_gallery_ingest(self, turn: DownloadTurn) -> bool:
        return self.gallery_ingest.request_gallery_ingest(turn)

    def finish_download_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
    ) -> bool:
        with self.SQLConnector() as connector:
            with connector.transaction():
                handoff = self.gallery_ingest._handoff_download_turn_with_connector(
                    connector,
                    turn,
                )
                if handoff is _DownloadHandoffResult.rejected:
                    return False
                if handoff is _DownloadHandoffResult.already_accepted:
                    return True
                # Exact-token deletion is deliberately a no-op when the same
                # GID was re-enqueued with a newer token. The completed live
                # turn still hands off immediately, while the newer request
                # remains durable for a future turn.
                self.todownload_queue._complete_download_request_with_connector(
                    connector,
                    request,
                )
        return True

    def finish_missing_download_turn(
        self,
        turn: DownloadTurn,
        request: DownloadRequest,
        gid: int,
    ) -> bool:
        self._validate_missing_download_request(request, gid)
        with self.SQLConnector() as connector:
            with connector.transaction():
                handoff = self.gallery_ingest._handoff_download_turn_with_connector(
                    connector,
                    turn,
                )
                if handoff is _DownloadHandoffResult.rejected:
                    return False
                if handoff is _DownloadHandoffResult.already_accepted:
                    return True
                if self.todownload_queue._complete_download_request_with_connector(
                    connector,
                    request,
                ):
                    self.removed_galleries._insert_removed_gallery_gid_with_connector(
                        connector,
                        gid,
                    )
        return True

    def get_gallery_ingest_state(self) -> GalleryIngestState:
        return self.gallery_ingest.get_state()

    def _claim_gallery_ingest(
        self,
        *,
        lease_seconds: int,
        periodic_scan: bool,
    ) -> GalleryIngestTurn | None:
        return self.gallery_ingest.claim_gallery_ingest(
            lease_seconds=lease_seconds,
            periodic_scan=periodic_scan,
        )

    def _renew_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
    ) -> bool:
        return self.gallery_ingest.renew_gallery_ingest(
            turn,
            lease_seconds=lease_seconds,
        )

    def _renew_gallery_ingest_lease(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int | None:
        return self.gallery_ingest.renew_gallery_ingest_lease(
            turn,
            lease_seconds=lease_seconds,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        )

    def _complete_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        allow_expired_sqlite_lease: bool = False,
    ) -> bool:
        return self.gallery_ingest.complete_gallery_ingest(
            turn,
            allow_expired_sqlite_lease=allow_expired_sqlite_lease,
        )

    def create_main_tables(self) -> None:
        self.logger.debug("Ensuring database schema objects exist...")
        self.database_maintenance._create_database_maintenance_state_table()
        self.gallery_ingest._create_gallery_ingest_state_table()
        self.todownload_queue._create_todownload_gids_table()
        self.pending_removals._create_pending_gallery_removals_table()
        self.gallery_ids._create_galleries_names_table()
        self.gallery_gids._create_galleries_gids_table()
        self.todelete_queue._create_todelete_gids_table()
        self.gallery_times._create_galleries_download_times_table()
        self.gallery_times._create_galleries_redownload_times_table()
        self.gallery_times._create_galleries_upload_times_table()
        self.removed_galleries._create_removed_galleries_gids_table()
        self.gallery_times._create_galleries_modified_times_table()
        self.gallery_times._create_galleries_access_times_table()
        self.gallery_titles._create_galleries_titles_table()
        self.upload_accounts._create_upload_account_table()
        self.gallery_comments._create_galleries_comments_table()
        self.files._create_files_names_table()
        self.gallery_source_manifests._create_gallery_source_manifests_table()
        self.pending_cbz_rebuilds._create_pending_cbz_rebuilds_table()
        self.gallery_infos._create_galleries_infos_view()
        self.files._create_galleries_files_hashs_tables()
        self.files._create_gallery_image_hash_view()
        self.gallery_infos._create_duplicate_hash_in_gallery_view()
        self.gallery_deduplication._create_gallery_content_hashes_table()
        self.gallery_deduplication._create_gallery_duplicate_warnings_table()
        self.gallery_deduplication._create_gallery_duplicate_warnings_names_view()
        self.todelete_queue._create_todelete_gallery_candidates_view()
        self.todelete_queue._create_todelete_galleries_table()
        self.todelete_queue._create_todelete_rm_commands_view()
        self.todownload_queue._create_pending_download_gids_view()
        self.gallery_tags._create_galleries_tags_table()
        self.logger.info(
            "Database schema initialization completed: "
            f"backend={self.config.database.sql_type.lower()}."
        )

    def update_redownload_time_to_now_by_gid(self, gid: int) -> None:
        db_gallery_id = self.gallery_gids._get_db_gallery_id_by_gid(gid)
        self.gallery_times.update_redownload_time_to_now(db_gallery_id)

    @property
    def _insert_rows_batch_size(self) -> int:
        return GALLERY_METADATA_BATCH_SIZE

    def _insert_gallery_names(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> dict[str, int]:
        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        self._insert_rows(
            "galleries_dbids",
            column_name_parts,
            [
                tuple(self._split_gallery_name(galleryinfo_params.gallery_name))
                for galleryinfo_params in galleryinfo_params_list
            ],
        )

        db_gallery_ids = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                [
                    galleryinfo_params.gallery_name
                    for galleryinfo_params in galleryinfo_params_list
                ]
            )
        )
        self._insert_rows(
            "galleries_names",
            ["db_gallery_id", "full_name"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.gallery_name,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )
        return db_gallery_ids

    def _insert_gallery_metadata_rows(
        self,
        galleryinfo_params_list: list[GalleryInfoParser],
        db_gallery_ids: dict[str, int],
    ) -> None:
        self._insert_rows(
            "galleries_gids",
            ["db_gallery_id", "gid"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.gid,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )
        self._insert_rows(
            "galleries_titles",
            ["db_gallery_id", "title"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.title,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )
        self._insert_rows(
            "galleries_upload_times",
            ["db_gallery_id", "time"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.upload_time,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )
        self._insert_rows(
            "galleries_comments",
            ["db_gallery_id", "comment"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.galleries_comments,
                )
                for galleryinfo_params in galleryinfo_params_list
                if galleryinfo_params.galleries_comments != ""
            ],
        )
        self._insert_rows(
            "galleries_upload_accounts",
            ["db_gallery_id", "account"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.upload_account,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )
        download_time_rows = [
            (
                db_gallery_ids[galleryinfo_params.gallery_name],
                galleryinfo_params.download_time,
            )
            for galleryinfo_params in galleryinfo_params_list
        ]
        self._insert_rows(
            "galleries_download_times", ["db_gallery_id", "time"], download_time_rows
        )
        self._insert_rows(
            "galleries_redownload_times", ["db_gallery_id", "time"], download_time_rows
        )
        self._insert_rows(
            "galleries_access_times", ["db_gallery_id", "time"], download_time_rows
        )
        self._insert_rows(
            "galleries_modified_times",
            ["db_gallery_id", "time"],
            [
                (
                    db_gallery_ids[galleryinfo_params.gallery_name],
                    galleryinfo_params.modified_time,
                )
                for galleryinfo_params in galleryinfo_params_list
            ],
        )

    def _insert_gallery_infos(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> None:
        if not galleryinfo_params_list:
            return

        insert_started = monotonic()
        gallery_count = len(galleryinfo_params_list)
        self.logger.debug(
            f"PERF event=start stage=gallery_insert galleries={gallery_count}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_pending galleries={gallery_count}"
        )
        self.insert_pending_gallery_removals(
            [
                galleryinfo_params.gallery_name
                for galleryinfo_params in galleryinfo_params_list
            ]
        )
        self.logger.debug(
            "PERF event=end stage=gallery_insert_pending "
            f"galleries={gallery_count} elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_names galleries={gallery_count}"
        )
        db_gallery_ids = self._insert_gallery_names(galleryinfo_params_list)
        self.logger.debug(
            "PERF event=end stage=gallery_insert_names "
            f"galleries={gallery_count} db_gallery_ids={len(db_gallery_ids)} "
            f"elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            "PERF event=start stage=gallery_insert_source_manifests "
            f"galleries={gallery_count}"
        )
        self.gallery_source_manifests._insert_many(
            {
                db_gallery_ids[galleryinfo_params.gallery_name]: (
                    build_gallery_source_manifest(galleryinfo_params)
                )
                for galleryinfo_params in galleryinfo_params_list
            }
        )
        self.logger.debug(
            "PERF event=end stage=gallery_insert_source_manifests "
            f"galleries={gallery_count} "
            f"elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_metadata galleries={gallery_count}"
        )
        self._insert_gallery_metadata_rows(galleryinfo_params_list, db_gallery_ids)
        self.logger.debug(
            "PERF event=end stage=gallery_insert_metadata "
            f"galleries={gallery_count} elapsed_s={monotonic() - stage_started:.6f}"
        )

        file_pairs: list[FileInformation] = list()
        stage_started = monotonic()
        next_heartbeat = stage_started + PERF_HEARTBEAT_SECONDS
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_file_manifest "
            f"galleries={gallery_count}"
        )
        for gallery_index, galleryinfo_params in enumerate(
            galleryinfo_params_list, start=1
        ):
            db_gallery_id = db_gallery_ids[galleryinfo_params.gallery_name]
            file_names = [file_path.name for file_path in galleryinfo_params.files_path]
            db_file_ids_by_name = self.files._insert_gallery_files(
                db_gallery_id, file_names
            )
            for file_path in galleryinfo_params.files_path:
                db_file_id = db_file_ids_by_name[file_path.name]
                file_pairs.append(FileInformation(file_path, db_file_id))

            now = monotonic()
            if now >= next_heartbeat:
                self.logger.debug(
                    "PERF event=heartbeat stage=gallery_insert_file_manifest "
                    f"galleries_processed={gallery_index} "
                    f"galleries={gallery_count} files={len(file_pairs)} "
                    f"elapsed_s={now - stage_started:.6f}"
                )
                next_heartbeat = now + PERF_HEARTBEAT_SECONDS
        self.logger.debug(
            "PERF event=end stage=gallery_insert_file_manifest "
            f"galleries={gallery_count} files={len(file_pairs)} "
            f"elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_hash "
            f"galleries={gallery_count} files={len(file_pairs)}"
        )
        self.files._insert_gallery_file_hash_for_db_gallery_id(file_pairs)
        self.logger.debug(
            "PERF event=end stage=gallery_insert_hash "
            f"galleries={gallery_count} files={len(file_pairs)} "
            f"elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_tags galleries={gallery_count}"
        )
        tags_by_gallery_id = {
            db_gallery_ids[galleryinfo_params.gallery_name]: [
                TagInformation(tag_name, tag_value)
                for tag_name, tag_value in galleryinfo_params.tags
            ]
            for galleryinfo_params in galleryinfo_params_list
        }
        self.gallery_tags._insert_gallery_tags_many(tags_by_gallery_id)
        self.logger.debug(
            "PERF event=end stage=gallery_insert_tags "
            f"galleries={gallery_count} elapsed_s={monotonic() - stage_started:.6f}"
        )

        stage_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=gallery_insert_pending_clear "
            f"galleries={gallery_count}"
        )
        self.delete_pending_gallery_removals_by_names(
            [
                galleryinfo_params.gallery_name
                for galleryinfo_params in galleryinfo_params_list
            ]
        )
        self.logger.debug(
            "PERF event=end stage=gallery_insert_pending_clear "
            f"galleries={gallery_count} elapsed_s={monotonic() - stage_started:.6f}"
        )
        self.logger.debug(
            "PERF event=end stage=gallery_insert "
            f"galleries={gallery_count} files={len(file_pairs)} "
            f"elapsed_s={monotonic() - insert_started:.6f}"
        )

    def _classify_gallery_changes(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> list[GalleryChange]:
        # Batched lookups keep query count independent of gallery count. The
        # source manifest catches file add/delete/rename operations without
        # rereading image bytes; galleryinfo.txt is still content-hashed so a
        # same-name metadata edit is detected as well.
        if not galleryinfo_params_list:
            return []

        change_detection_started = monotonic()
        gallery_count = len(galleryinfo_params_list)
        self.logger.debug(
            f"PERF event=start stage=gallery_change_detection "
            f"galleries={gallery_count}"
        )

        # Read from galleries_dbids itself because it is the authoritative
        # parent table that deletions target.  The migration also removes any
        # historical SQLite orphans created before foreign keys were enabled.
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                [
                    galleryinfo_params.gallery_name
                    for galleryinfo_params in galleryinfo_params_list
                ]
            )
        )
        self.logger.debug(
            "PERF event=progress stage=gallery_change_detection "
            f"step=gallery_id_lookup galleries={gallery_count} "
            f"existing_galleries={len(db_gallery_ids_by_name)} "
            f"elapsed_s={monotonic() - change_detection_started:.6f}"
        )
        source_manifests_by_gallery_id = (
            self.gallery_source_manifests._get_by_db_gallery_ids(
                list(db_gallery_ids_by_name.values())
            )
        )
        self.logger.debug(
            "PERF event=progress stage=gallery_change_detection "
            f"step=source_manifest_lookup galleries={gallery_count} "
            f"existing_manifests={len(source_manifests_by_gallery_id)} "
            f"elapsed_s={monotonic() - change_detection_started:.6f}"
        )
        db_file_ids_by_gallery_id = self.files._get_db_file_ids_by_gallery_ids_for_name(
            list(db_gallery_ids_by_name.values()), GALLERY_INFO_FILE_NAME
        )
        self.logger.debug(
            "PERF event=progress stage=gallery_change_detection "
            f"step=file_id_lookup galleries={gallery_count} "
            f"existing_files={len(db_file_ids_by_gallery_id)} "
            f"elapsed_s={monotonic() - change_detection_started:.6f}"
        )
        hash_values_by_file_id = self.files._get_hash_values_by_file_ids(
            list(db_file_ids_by_gallery_id.values()), COMPARISON_HASH_ALGORITHM
        )
        self.logger.debug(
            "PERF event=progress stage=gallery_change_detection "
            f"step=hash_lookup galleries={gallery_count} "
            f"existing_hashes={len(hash_values_by_file_id)} "
            f"elapsed_s={monotonic() - change_detection_started:.6f}"
        )

        change_list: list[GalleryChange] = list()
        next_heartbeat = change_detection_started + PERF_HEARTBEAT_SECONDS
        for gallery_index, galleryinfo_params in enumerate(
            galleryinfo_params_list, start=1
        ):
            db_gallery_id = db_gallery_ids_by_name.get(galleryinfo_params.gallery_name)
            if db_gallery_id is None:
                change = GalleryChange.new
            else:
                stored_source_manifest = source_manifests_by_gallery_id.get(
                    db_gallery_id
                )
                current_source_manifest = build_gallery_source_manifest(
                    galleryinfo_params
                )
                gallery_info_file_id = db_file_ids_by_gallery_id.get(db_gallery_id)
                original_hash_value = (
                    None
                    if gallery_info_file_id is None
                    else hash_values_by_file_id.get(gallery_info_file_id)
                )
                if (
                    stored_source_manifest != current_source_manifest
                    or original_hash_value is None
                ):
                    change = GalleryChange.changed
                else:
                    absolute_file_path = (
                        galleryinfo_params.gallery_folder / GALLERY_INFO_FILE_NAME
                    )
                    current_hash_value = hash_function_by_file(
                        absolute_file_path, COMPARISON_HASH_ALGORITHM
                    )
                    change = (
                        GalleryChange.unchanged
                        if original_hash_value == current_hash_value
                        else GalleryChange.changed
                    )
            change_list.append(change)
            now = monotonic()
            if now >= next_heartbeat:
                self.logger.debug(
                    "PERF event=heartbeat stage=gallery_change_detection "
                    f"galleries_checked={gallery_index} galleries={gallery_count} "
                    f"elapsed_s={now - change_detection_started:.6f}"
                )
                next_heartbeat = now + PERF_HEARTBEAT_SECONDS
        new_count = change_list.count(GalleryChange.new)
        changed_count = change_list.count(GalleryChange.changed)
        unchanged_count = change_list.count(GalleryChange.unchanged)
        self.logger.debug(
            "PERF event=end stage=gallery_change_detection "
            f"galleries={gallery_count} new={new_count} changed={changed_count} "
            f"unchanged={unchanged_count} "
            f"elapsed_s={monotonic() - change_detection_started:.6f}"
        )
        return change_list

    def insert_gallery_infos(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> list[GalleryChange]:
        change_list = self._classify_gallery_changes(galleryinfo_params_list)

        to_insert: list[GalleryInfoParser] = list()
        changed_gallery_names: list[str] = list()
        for galleryinfo_params, change in zip(
            galleryinfo_params_list, change_list, strict=True
        ):
            if change != GalleryChange.unchanged:
                self.logger.debug(
                    "Applying gallery database change: "
                    f"gallery={galleryinfo_params.gallery_name!r} "
                    f"change={change.value}."
                )
                to_insert.append(galleryinfo_params)
            if change == GalleryChange.changed:
                changed_gallery_names.append(galleryinfo_params.gallery_name)

        # Persist this before deleting/reinserting changed gallery rows. If the
        # process stops before final CBZ reconciliation, the next run still
        # knows that a same-member-name CBZ must be rebuilt.
        self.pending_cbz_rebuilds.insert_pending_gallery_names(changed_gallery_names)
        self.refresh_galleries(changed_gallery_names)
        self._insert_gallery_infos(to_insert)

        for galleryinfo_params, change in zip(
            galleryinfo_params_list, change_list, strict=True
        ):
            if change != GalleryChange.unchanged:
                self.logger.debug(
                    "Gallery database change applied: "
                    f"gallery={galleryinfo_params.gallery_name!r} "
                    f"change={change.value}."
                )
        return change_list

    def scan_current_galleries_folders(self) -> GalleryScan:
        scan_started = monotonic()
        self.logger.debug(
            "PERF event=start stage=scan "
            f"backend={self.config.database.sql_type.lower()}"
        )

        with self.SQLConnector() as connector:
            tmp_table_name = "tmp_current_galleries"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.sqlite_name_columns("name")
                    )
            query = f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS {tmp_table_name} (
                    {create_gallery_name_parts_sql},
                    PRIMARY KEY ({", ".join(column_name_parts)})
                )
            """

            stage_started = monotonic()
            self.logger.debug("PERF event=start stage=scan_temp_table_create")
            connector.execute(query)
            self.logger.debug(
                "PERF event=end stage=scan_temp_table_create "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )

            insert_query = f"""
                INSERT INTO {tmp_table_name}
                    ({", ".join(column_name_parts)})
                VALUES ({", ".join(["%s" for _ in column_name_parts])})
            """

            data: list[tuple[str, ...]] = list()
            current_galleries_folders: list[Path] = list()
            current_galleries_names: set[str] = set()
            stage_started = monotonic()
            next_heartbeat = stage_started + PERF_HEARTBEAT_SECONDS
            directories_scanned = 0
            self.logger.debug("PERF event=start stage=scan_walk")
            for root, _, files in self.config.h2h.download_path.walk():
                directories_scanned += 1
                if GALLERY_INFO_FILE_NAME in files:
                    current_galleries_folders.append(root)
                    gallery_name = current_galleries_folders[-1].name
                    current_galleries_names.add(gallery_name)
                    gallery_name_parts = self._split_gallery_name(gallery_name)
                    data.append(tuple(gallery_name_parts))
                now = monotonic()
                if now >= next_heartbeat:
                    self.logger.debug(
                        "PERF event=heartbeat stage=scan_walk "
                        f"directories={directories_scanned} "
                        f"galleries={len(current_galleries_folders)} "
                        f"elapsed_s={now - stage_started:.6f}"
                    )
                    next_heartbeat = now + PERF_HEARTBEAT_SECONDS
            self.logger.debug(
                "PERF event=end stage=scan_walk "
                f"directories={directories_scanned} "
                f"galleries={len(current_galleries_folders)} "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )

            recovered_removed_count = (
                self.pending_removals.recover_pending_gallery_removals(
                    current_galleries_names
                )
            )

            group_size = 5000
            it = iter(data)
            stage_started = monotonic()
            batch_count = (len(data) + group_size - 1) // group_size
            self.logger.debug(
                "PERF event=start stage=scan_temp_table_insert "
                f"rows={len(data)} batches={batch_count} batch_size={group_size}"
            )
            for _ in range(0, len(data), group_size):
                connector.execute_many(insert_query, list(islice(it, group_size)))
            self.logger.debug(
                "PERF event=end stage=scan_temp_table_insert "
                f"rows={len(data)} batches={batch_count} "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )

            match self.config.database.sql_type.lower():
                case "mariadb":
                    fetch_query = f"""
                        SELECT CONCAT({",".join(["galleries_dbids."+column_name for column_name in column_name_parts])})
                        FROM galleries_dbids
                        LEFT JOIN {tmp_table_name} USING ({",".join(column_name_parts)})
                        WHERE {tmp_table_name}.{column_name_parts[0]} IS NULL
                    """
                case "sqlite":
                    # SQLite branch never splits the name across columns (see
                    # sqlite_name_columns), so there's exactly one column to select --
                    # no CONCAT needed.
                    fetch_query = f"""
                        SELECT galleries_dbids.{column_name_parts[0]}
                        FROM galleries_dbids
                        LEFT JOIN {tmp_table_name} USING ({",".join(column_name_parts)})
                        WHERE {tmp_table_name}.{column_name_parts[0]} IS NULL
                    """
            stage_started = monotonic()
            self.logger.debug("PERF event=start stage=scan_removed_fetch")
            raw_removed_galleries = connector.fetch_all(fetch_query)
            removed_gallery_names = [
                str(gallery[0]) for gallery in raw_removed_galleries
            ]
            self.logger.debug(
                "PERF event=end stage=scan_removed_fetch "
                f"removed_galleries={len(removed_gallery_names)} "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )

        removed_count = (
            recovered_removed_count
            + self.pending_removals.delete_confirmed_missing_galleries(
                removed_gallery_names
            )
        )

        self.logger.debug(
            "PERF event=end stage=scan "
            f"galleries={len(current_galleries_folders)} "
            f"unique_names={len(current_galleries_names)} "
            f"removed_galleries={removed_count} "
            f"elapsed_s={monotonic() - scan_started:.6f}"
        )
        return GalleryScan(
            folders=tuple(current_galleries_folders),
            names=frozenset(current_galleries_names),
            removed_galleries=removed_count,
        )

    def _refresh_current_files_hashs(self, algorithm: str) -> None:
        if algorithm not in HASH_ALGORITHMS:
            raise ValueError(
                f"Invalid hash algorithm: {algorithm} not in {HASH_ALGORITHMS}"
            )

        with self.SQLConnector() as connector:
            # RIGHT JOIN is standard SQL, supported by both MariaDB and SQLite (3.39+).
            def get_delete_db_hash_id_query(x: str, y: str) -> str:
                return f"""
                DELETE FROM {y}
                WHERE db_hash_id IN (
                        SELECT db_hash_id
                        FROM {x}
                        RIGHT JOIN {y} USING (db_hash_id)
                        WHERE {x}.db_hash_id IS NULL
                    )
                """

            hash_table_name = f"files_hashs_{algorithm.lower()}"
            db_table_name = f"files_hashs_{algorithm.lower()}_dbids"
            connector.execute(
                get_delete_db_hash_id_query(hash_table_name, db_table_name)
            )

    def refresh_current_files_hashs(self) -> None:
        for algorithm in HASH_ALGORITHMS:
            self._refresh_current_files_hashs(algorithm)

    def _insert_gallery_chunk_with_split_retry(
        self, gallery_chunk: list[Path]
    ) -> list[GalleryChange]:
        try:
            parse_started = monotonic()
            self.logger.debug(
                f"PERF event=start stage=chunk_parse galleries={len(gallery_chunk)}"
            )
            galleryinfo_params_list = [
                parse_galleryinfo(gallery_folder) for gallery_folder in gallery_chunk
            ]
            self.logger.debug(
                "PERF event=end stage=chunk_parse "
                f"galleries={len(gallery_chunk)} "
                f"elapsed_s={monotonic() - parse_started:.6f}"
            )
            insert_started = monotonic()
            self.logger.debug(
                f"PERF event=start stage=chunk_insert galleries={len(gallery_chunk)}"
            )
            result = self.insert_gallery_infos(galleryinfo_params_list)
            inserted_or_updated = sum(
                change != GalleryChange.unchanged for change in result
            )
            self.logger.debug(
                "PERF event=end stage=chunk_insert "
                f"galleries={len(gallery_chunk)} "
                f"inserted_or_updated={inserted_or_updated} "
                f"elapsed_s={monotonic() - insert_started:.6f}"
            )
            return result
        except Exception as e:
            if len(gallery_chunk) == 1:
                raise
            mid = len(gallery_chunk) // 2
            self.logger.warning(
                "Gallery batch insert failed; retrying with split batches: "
                f"galleries={len(gallery_chunk)} left={mid} "
                f"right={len(gallery_chunk) - mid} error={e!r}."
            )
            return self._insert_gallery_chunk_with_split_retry(
                gallery_chunk[:mid]
            ) + self._insert_gallery_chunk_with_split_retry(gallery_chunk[mid:])

    def _sort_galleries_for_processing(
        self, current_galleries_folders: list[Path]
    ) -> list[Path]:
        sort_started = monotonic()
        self.logger.debug(
            "PERF event=start stage=sort "
            f"galleries={len(current_galleries_folders)} "
            f"mode={self.config.h2h.cbz_sort}"
        )
        self.logger.info(
            "Preparing gallery processing order: "
            f"galleries={len(current_galleries_folders)} "
            f"sort={self.config.h2h.cbz_sort}..."
        )
        if self.config.h2h.cbz_sort in ["upload_time", "download_time", "gid", "title"]:
            sorted_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_galleryinfo(x), self.config.h2h.cbz_sort),
                reverse=True,
            )
        elif "no" in self.config.h2h.cbz_sort:
            sorted_galleries_folders = current_galleries_folders
        elif "pages" in self.config.h2h.cbz_sort:
            zero_level = (
                max(1, int(self.config.h2h.cbz_sort.split("+")[-1]))
                if "+" in self.config.h2h.cbz_sort
                else 20
            )
            sorted_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: abs(getattr(parse_galleryinfo(x), "pages") - zero_level),
            )
        else:
            sorted_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_galleryinfo(x), "pages"),
            )
        sort_elapsed = monotonic() - sort_started
        self.logger.info(
            "Gallery processing order prepared: "
            f"galleries={len(sorted_galleries_folders)} "
            f"sort={self.config.h2h.cbz_sort} elapsed_s={sort_elapsed:.3f}."
        )
        self.logger.debug(
            "PERF event=end stage=sort "
            f"galleries={len(sorted_galleries_folders)} "
            f"mode={self.config.h2h.cbz_sort} "
            f"elapsed_s={sort_elapsed:.6f}"
        )
        return sorted_galleries_folders

    def _collect_gallery_deduplication_batch(
        self,
        gallery_chunk: list[Path],
        exclude_hashs: set[bytes],
    ) -> tuple[list[ContentClaim], dict[str, int]]:
        if not gallery_chunk:
            return ([], {})

        gallery_names = [folder.name for folder in gallery_chunk]
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                gallery_names
            )
        )
        db_gallery_ids = list(db_gallery_ids_by_name.values())
        files_by_db_gallery_id = self.cbz._get_files_by_db_gallery_ids(db_gallery_ids)
        download_times_by_db_gallery_id = (
            self.gallery_times.get_download_times_by_db_gallery_ids(db_gallery_ids)
        )
        titles_by_db_gallery_id = self.gallery_titles.get_titles_by_db_gallery_ids(
            db_gallery_ids
        )
        already_uploaded_by_db_gallery_id = (
            self.gallery_deduplication.get_already_uploaded_flags_by_db_gallery_ids(
                db_gallery_ids
            )
        )

        claims: list[ContentClaim] = []
        for folder in gallery_chunk:
            db_gallery_id = db_gallery_ids_by_name.get(folder.name)
            if db_gallery_id is None:
                continue

            # galleryinfo.txt is excluded from the content hash below: it embeds
            # this gallery's own GID/metadata, so it necessarily differs
            # between galleries even when every other file is identical.
            gallery_files = [
                (file_name, file_hash)
                for file_name, file_hash in files_by_db_gallery_id.get(
                    db_gallery_id, []
                )
                if file_name != GALLERY_INFO_FILE_NAME
            ]

            content_files = [
                file_hash
                for _, file_hash in gallery_files
                if file_hash not in exclude_hashs
            ]
            content_hash = (
                hashlib.sha256(b"".join(sorted(content_files))).digest()
                if content_files
                else None
            )
            priority_key = (
                not already_uploaded_by_db_gallery_id[db_gallery_id],
                len(titles_by_db_gallery_id[db_gallery_id]),
                download_times_by_db_gallery_id[db_gallery_id],
            )
            claims.append(
                ContentClaim(
                    db_gallery_id=db_gallery_id,
                    sha256=content_hash,
                    priority_key=priority_key,
                )
            )

        return (claims, db_gallery_ids_by_name)

    def _filter_galleries_for_deduplication(
        self,
        galleries: list[Path],
        exclude_hashs: set[bytes],
    ) -> list[Path]:
        collection_started = monotonic()
        claims: list[ContentClaim] = []
        db_gallery_ids_by_name: dict[str, int] = {}
        gallery_chunks = chunk_list(galleries, GALLERY_METADATA_BATCH_SIZE)
        self.logger.debug(
            "PERF event=start stage=global_dedup_collection "
            f"galleries={len(galleries)} batches={len(gallery_chunks)} "
            f"excluded_hashes={len(exclude_hashs)}"
        )
        galleries_processed = 0
        for batch_index, gallery_chunk in enumerate(gallery_chunks, start=1):
            batch_started = monotonic()
            chunk_claims, chunk_db_gallery_ids_by_name = (
                self._collect_gallery_deduplication_batch(gallery_chunk, exclude_hashs)
            )
            claims.extend(chunk_claims)
            db_gallery_ids_by_name.update(chunk_db_gallery_ids_by_name)
            galleries_processed += len(gallery_chunk)
            self.logger.debug(
                "PERF event=batch stage=global_dedup_collection "
                f"batch_index={batch_index} batches={len(gallery_chunks)} "
                f"batch_galleries={len(gallery_chunk)} "
                f"galleries_processed={galleries_processed} "
                f"galleries={len(galleries)} batch_claims={len(chunk_claims)} "
                f"elapsed_s={monotonic() - batch_started:.6f}"
            )
        self.logger.debug(
            "PERF event=end stage=global_dedup_collection "
            f"galleries={len(galleries)} claims={len(claims)} "
            f"elapsed_s={monotonic() - collection_started:.6f}"
        )

        missing_gallery_names = {folder.name for folder in galleries}.difference(
            db_gallery_ids_by_name
        )
        if missing_gallery_names:
            raise RuntimeError(
                "Cannot reconcile galleries missing from the database: "
                f"{sorted(missing_gallery_names)}"
            )

        reconcile_started = monotonic()
        self.logger.debug(
            f"PERF event=start stage=global_dedup_reconcile claims={len(claims)}"
        )
        result = self.gallery_deduplication.reconcile_many(claims)
        self.logger.debug(
            "PERF event=end stage=global_dedup_reconcile "
            f"claims={len(claims)} "
            f"eligible_galleries={len(result.eligible_db_gallery_ids)} "
            f"losing_galleries={len(result.losing_db_gallery_ids)} "
            f"elapsed_s={monotonic() - reconcile_started:.6f}"
        )
        losing_db_gallery_ids = list(result.losing_db_gallery_ids)
        if self.config.h2h.cbz_path is not None:
            self._delete_duplicate_gallery_cbz_files(losing_db_gallery_ids)

        return [
            folder
            for folder in galleries
            if db_gallery_ids_by_name[folder.name] in result.eligible_db_gallery_ids
        ]

    def _delete_duplicate_gallery_cbz_files(self, db_gallery_ids: list[int]) -> None:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        assert self.config.h2h.cbz_path is not None
        if not db_gallery_ids:
            return

        gallery_names = self.gallery_ids.get_gallery_names_by_db_gallery_ids(
            db_gallery_ids
        ).values()
        target_file_names = {
            gallery_name_to_cbz_file_name(gallery_name)
            for gallery_name in gallery_names
        }
        removed = 0
        for root, _, files in self.config.h2h.cbz_path.walk():
            for file_name in target_file_names.intersection(files):
                cbz_path = root / file_name
                cbz_path.unlink()
                removed += 1
                self.logger.debug(
                    f"Duplicate-content CBZ removed: path={str(cbz_path)!r}."
                )
        self.logger.info(
            "Duplicate-content CBZ cleanup completed: "
            f"targeted={len(target_file_names)} removed={removed}."
        )

    def synchronize_once(self) -> SyncOutcome:
        run_started = monotonic()
        self.logger.debug(
            "PERF event=start stage=synchronize_once "
            f"backend={self.config.database.sql_type.lower()} "
            f"cbz_enabled={self.config.h2h.cbz_path is not None} "
            f"cpu_count={CPU_NUM} pool_workers={POOL_CPU_LIMIT} "
            f"file_hash_workers={self.config.h2h.file_hash_workers}"
        )

        gallery_scan = self.scan_current_galleries_folders()
        self.database_maintenance.record_gallery_changes(
            changed_galleries=0,
            removed_galleries=gallery_scan.removed_galleries,
        )
        current_galleries_folders = list(gallery_scan.folders)
        current_galleries_names = set(gallery_scan.names)
        self.pending_cbz_rebuilds.delete_stale_gallery_names(current_galleries_names)

        current_galleries_folders = self._sort_galleries_for_processing(
            current_galleries_folders
        )

        total_inserted_in_database = 0
        total_new_galleries = 0
        total_changed_galleries = 0
        total_cbz_summary = CBZCompressionSummary()
        provisional_cbz_summary = CBZCompressionSummary()
        final_cbz_summary = CBZCompressionSummary()
        gallery_chunk_size = (
            PROGRESSIVE_GALLERY_CHUNK_SIZE
            if self.config.h2h.cbz_path is not None
            else 1000 * POOL_CPU_LIMIT
        )
        chunked_galleries_folders = chunk_list(
            current_galleries_folders, gallery_chunk_size
        )
        total_chunks = len(chunked_galleries_folders)
        total_galleries = len(current_galleries_folders)
        galleries_processed = 0
        self.logger.debug(
            "PERF event=progress stage=synchronize_once "
            f"step=chunk_plan galleries={total_galleries} chunks={total_chunks} "
            f"chunk_size={gallery_chunk_size}"
        )
        self.logger.info(
            f"Checking {total_galleries} galleries for database changes "
            f"across {total_chunks} chunk(s)..."
        )
        with contextlib.ExitStack() as stack:
            cbz_pool = (
                stack.enter_context(Pool(POOL_CPU_LIMIT))
                if self.config.h2h.cbz_path is not None
                else None
            )

            # This snapshot is intentionally provisional. Only missing CBZ
            # files for brand-new database rows are created here. Existing CBZ
            # files are preserved until the authoritative final exclusion set
            # is available, preventing a fresh database from rebuilding valid
            # output once with an empty exclusion set and then rebuilding it
            # again during final reconciliation.
            provisional_exclude_hashs = (
                self.duplicated_hashes._get_duplicated_hash_values_by_count_artist_ratio()
                if cbz_pool is not None
                else set()
            )
            self.logger.debug(
                "PERF event=progress stage=synchronize_once "
                f"step=provisional_exclusions "
                f"excluded_hashes={len(provisional_exclude_hashs)}"
            )

            for chunk_index, gallery_chunk in enumerate(chunked_galleries_folders, 1):
                chunk_started = monotonic()
                chunk_cbz_writes_before = total_cbz_summary.write_operations
                galleries_processed += len(gallery_chunk)
                self.logger.debug(
                    "PERF event=start stage=insertion_chunk "
                    f"chunk_index={chunk_index} chunks={total_chunks} "
                    f"chunk_galleries={len(gallery_chunk)} "
                    f"galleries_processed={galleries_processed} "
                    f"galleries={total_galleries}"
                )
                self.logger.info(
                    f"Checking gallery chunk {chunk_index}/{total_chunks}: "
                    f"galleries={len(gallery_chunk)} "
                    f"cumulative={galleries_processed}/{total_galleries}..."
                )
                change_list = self._insert_gallery_chunk_with_split_retry(gallery_chunk)
                new_count = change_list.count(GalleryChange.new)
                changed_count = change_list.count(GalleryChange.changed)
                unchanged_count = change_list.count(GalleryChange.unchanged)
                inserted_or_updated = new_count + changed_count
                total_new_galleries += new_count
                total_changed_galleries += changed_count
                total_inserted_in_database += inserted_or_updated
                self.database_maintenance.record_gallery_changes(
                    changed_galleries=changed_count,
                    removed_galleries=0,
                )

                if cbz_pool is not None:
                    provisional_galleries = [
                        gallery_folder
                        for gallery_folder, change in zip(
                            gallery_chunk, change_list, strict=True
                        )
                        if change == GalleryChange.new
                    ]
                    if provisional_galleries:
                        provisional_started = monotonic()
                        self.logger.info(
                            f"Checking provisional CBZ output for gallery chunk "
                            f"{chunk_index}/{total_chunks}: "
                            f"galleries={len(provisional_galleries)}..."
                        )
                        chunk_provisional_summary = self.cbz.compress_galleries_to_cbz(
                            provisional_galleries,
                            provisional_exclude_hashs,
                            cbz_pool,
                            existing_cbz_policy=ExistingCBZPolicy.preserve,
                        )
                        provisional_cbz_summary += chunk_provisional_summary
                        total_cbz_summary += chunk_provisional_summary
                        self.logger.info(
                            f"Provisional CBZ check for gallery chunk "
                            f"{chunk_index}/{total_chunks} completed: "
                            f"{_cbz_summary_fields(chunk_provisional_summary)} "
                            f"elapsed_s={monotonic() - provisional_started:.3f}."
                        )

                chunk_cbz_writes = (
                    total_cbz_summary.write_operations - chunk_cbz_writes_before
                )
                self.logger.info(
                    f"Gallery chunk {chunk_index}/{total_chunks} completed: "
                    f"checked={len(gallery_chunk)} "
                    f"new={new_count} changed={changed_count} "
                    f"unchanged={unchanged_count} "
                    f"provisional_cbz_writes={chunk_cbz_writes} "
                    f"elapsed_s={monotonic() - chunk_started:.3f}."
                )

                self.logger.debug(
                    "PERF event=end stage=insertion_chunk "
                    f"chunk_index={chunk_index} chunks={total_chunks} "
                    f"chunk_galleries={len(gallery_chunk)} "
                    f"new={new_count} changed={changed_count} "
                    f"unchanged={unchanged_count} "
                    f"cbz_write_operations={chunk_cbz_writes} "
                    f"elapsed_s={monotonic() - chunk_started:.6f}"
                )

            self.logger.info(
                "Gallery database changes applied: "
                f"insert_or_update_operations={total_inserted_in_database}."
            )

            # Freeze one final exclusion set only after every insertion chunk
            # has updated the file-hash tables. Reconcile all current galleries
            # globally against that same snapshot, so chunk boundaries cannot
            # leave stale or conflicting ownership behind.
            self.logger.info("Computing final content-hash exclusion set...")
            stage_started = monotonic()
            self.logger.debug(
                "PERF event=start stage=final_exclusions "
                f"galleries={total_galleries}"
            )
            final_exclude_hashs = (
                self.duplicated_hashes._get_duplicated_hash_values_by_count_artist_ratio()
            )
            self.logger.debug(
                "PERF event=end stage=final_exclusions "
                f"excluded_hashes={len(final_exclude_hashs)} "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )
            self.logger.info(
                "Final content-hash exclusion set computed: "
                f"hashes={len(final_exclude_hashs)}."
            )
            stage_started = monotonic()
            self.logger.debug(
                "PERF event=start stage=final_dedup "
                f"galleries={total_galleries} "
                f"excluded_hashes={len(final_exclude_hashs)}"
            )
            galleries_to_compress = self._filter_galleries_for_deduplication(
                current_galleries_folders, final_exclude_hashs
            )
            self.logger.debug(
                "PERF event=end stage=final_dedup "
                f"galleries={total_galleries} "
                f"eligible_galleries={len(galleries_to_compress)} "
                f"elapsed_s={monotonic() - stage_started:.6f}"
            )

            if cbz_pool is not None:
                eligible_gallery_names = {
                    gallery_folder.name for gallery_folder in galleries_to_compress
                }
                self.cbz._refresh_current_cbz_files(eligible_gallery_names)
                pending_rebuild_gallery_names = (
                    self.pending_cbz_rebuilds.get_pending_gallery_names(
                        list(current_galleries_names)
                    )
                )

                chunked_galleries_to_compress = chunk_list(
                    galleries_to_compress, 1000 * POOL_CPU_LIMIT
                )
                final_chunk_count = len(chunked_galleries_to_compress)
                for chunk_index, gallery_chunk in enumerate(
                    chunked_galleries_to_compress, 1
                ):
                    final_cbz_started = monotonic()
                    self.logger.info(
                        f"Checking final CBZ chunk {chunk_index}/{final_chunk_count}: "
                        f"galleries={len(gallery_chunk)}..."
                    )
                    chunk_final_summary = self.cbz.compress_galleries_to_cbz(
                        gallery_chunk,
                        final_exclude_hashs,
                        cbz_pool,
                        force_rebuild_gallery_names=(
                            pending_rebuild_gallery_names.intersection(
                                gallery_folder.name for gallery_folder in gallery_chunk
                            )
                        ),
                    )
                    self.pending_cbz_rebuilds.delete_pending_gallery_names(
                        [
                            gallery_folder.name
                            for gallery_folder in gallery_chunk
                            if gallery_folder.name in pending_rebuild_gallery_names
                        ]
                    )
                    final_cbz_summary += chunk_final_summary
                    total_cbz_summary += chunk_final_summary
                    self.logger.info(
                        f"Final CBZ chunk {chunk_index}/{final_chunk_count} completed: "
                        f"{_cbz_summary_fields(chunk_final_summary)} "
                        f"elapsed_s={monotonic() - final_cbz_started:.3f}."
                    )
                # Pending rows for deduplication losers are also complete: their
                # CBZ files were removed before the final winner checks.
                self.pending_cbz_rebuilds.delete_pending_gallery_names(
                    list(current_galleries_names)
                )

        if self.config.h2h.cbz_path is not None:
            self.logger.info(
                "CBZ processing totals: "
                f"compression_checks={total_cbz_summary.checked} "
                f"create_operations={total_cbz_summary.created} "
                f"rebuild_operations={total_cbz_summary.rebuilt} "
                f"unchanged_checks={total_cbz_summary.unchanged} "
                f"write_operations={total_cbz_summary.write_operations} "
                f"provisional_writes={provisional_cbz_summary.write_operations} "
                f"final_writes={final_cbz_summary.write_operations}."
            )

        self.logger.info(
            "Cleaning orphaned derived records: "
            f"file_hash_algorithms={len(HASH_ALGORITHMS)} "
            "source_manifest_tables=1..."
        )
        stage_started = monotonic()
        self.logger.debug(
            "PERF event=start stage=cleanup_file_hashes "
            f"algorithms={len(HASH_ALGORITHMS)}"
        )
        self.refresh_current_files_hashs()
        self.gallery_source_manifests._delete_stale_rows()
        cleanup_elapsed = monotonic() - stage_started
        self.logger.debug(
            "PERF event=end stage=cleanup_file_hashes "
            f"algorithms={len(HASH_ALGORITHMS)} "
            f"elapsed_s={cleanup_elapsed:.6f}"
        )
        self.logger.info(
            "Orphaned derived-record cleanup completed: "
            f"file_hash_algorithms={len(HASH_ALGORITHMS)} "
            f"source_manifest_tables=1 elapsed_s={cleanup_elapsed:.3f}."
        )

        self.todelete_queue.refresh_todelete_galleries()
        outcome = SyncOutcome(
            new_galleries=total_new_galleries,
            changed_galleries=total_changed_galleries,
            removed_galleries=gallery_scan.removed_galleries,
        )
        self.logger.debug(
            "PERF event=end stage=synchronize_once "
            f"galleries={total_galleries} "
            f"new={outcome.new_galleries} "
            f"changed={outcome.changed_galleries} "
            f"removed={outcome.removed_galleries} "
            f"cbz_write_operations={total_cbz_summary.write_operations} "
            f"elapsed_s={monotonic() - run_started:.6f}"
        )

        return outcome

    def reset_redownload_times(self) -> None:
        self.gallery_times._reset_redownload_times()

    def get_komga_metadata(
        self, gallery_names: list[str]
    ) -> dict[str, dict[str, str | list[dict[str, str]]]]:
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                gallery_names
            )
        )
        db_gallery_ids = list(db_gallery_ids_by_name.values())

        titles = self.gallery_titles.get_titles_by_db_gallery_ids(db_gallery_ids)
        comments = self.gallery_comments.get_comments_by_db_gallery_ids(db_gallery_ids)
        upload_times = self.gallery_times.get_upload_times_by_db_gallery_ids(
            db_gallery_ids
        )
        tags_by_gallery_id = self.gallery_tags.get_tag_pairs_by_db_gallery_ids(
            db_gallery_ids
        )
        gids = self.gallery_gids.get_gids_by_db_gallery_ids(db_gallery_ids)

        result = dict[str, dict[str, str | list[dict[str, str]]]]()
        for gallery_name in gallery_names:
            db_gallery_id = db_gallery_ids_by_name[gallery_name]
            metadata: dict[str, str | list[dict[str, str]]] = dict()
            metadata["title"] = titles[db_gallery_id]
            metadata["summary"] = comments.get(db_gallery_id, "")
            upload_time = upload_times[db_gallery_id]
            metadata["releaseDate"] = "-".join(
                [
                    str(upload_time.year),
                    f"{upload_time.month:02d}",
                    f"{upload_time.day:02d}",
                ]
            )
            tags = tags_by_gallery_id.get(db_gallery_id, [])
            authors = [
                {"name": value, "role": key} for key, value in tags if value != ""
            ]
            authors.append({"name": str(gids[db_gallery_id]), "role": "gid"})
            metadata["authors"] = authors
            result[gallery_name] = metadata
        return result
