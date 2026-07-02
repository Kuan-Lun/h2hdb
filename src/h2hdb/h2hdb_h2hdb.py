__all__ = ["H2HDB", "GALLERY_INFO_FILE_NAME"]


import contextlib
import hashlib
import os
from itertools import islice
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from typing import cast

from h2h_galleryinfo_parser import (
    GalleryInfoParser,
    parse_galleryinfo,
)

from .cbz_files import H2HDBCBZFiles, cbz_scrub_worker, run_in_parallel
from .cbz_integrity import H2HDBCBZIntegrity
from .config_loader import H2HDBConfig
from .duplicated_hashes import H2HDBDuplicatedHashes
from .gallery_deduplication import H2HDBGalleryDeduplication
from .hash_dict import HASH_ALGORITHMS
from .information import FileInformation, TagInformation
from .logger import HentaiDBLogger
from .repository import BaseRepository, RepositoryContext
from .settings import (
    COMPARISON_HASH_ALGORITHM,
    GALLERY_INFO_FILE_NAME,
    chunk_list,
    hash_function_by_file,
)
from .table_comments import H2HDBGalleriesComments
from .table_database_setting import H2HDBCheckDatabaseSettings
from .table_files_dbids import H2HDBFiles
from .table_gids import H2HDBGalleriesGIDs, H2HDBGalleriesIDs
from .table_pending_removals import H2HDBPendingGalleryRemovals
from .table_removed_gids import H2HDBRemovedGalleries
from .table_tags import H2HDBGalleriesTags
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles
from .table_uploadaccounts import H2HDBUploadAccounts
from .todelete_queue import H2HDBToDeleteQueue
from .todownload_queue import H2HDBToDownloadQueue
from .view_ginfo import H2HDBGalleriesInfos

GALLERY_METADATA_BATCH_SIZE = 500
CPU_NUM = cpu_count()
POOL_CPU_LIMIT = max(CPU_NUM - 2, 1)


class _ExcludeHashesTracker:
    """Recreated per call: a longer-lived cache would miss duplicate-count drops caused by deletions."""

    def __init__(
        self, duplicated_hashes: H2HDBDuplicatedHashes, logger: HentaiDBLogger
    ) -> None:
        self._duplicated_hashes = duplicated_hashes
        self._logger = logger
        self._previously_count_duplicated_files = (
            duplicated_hashes._count_duplicated_files_hashs_sha512()
        )
        self.exclude_hashs = (
            duplicated_hashes._get_duplicated_hash_values_by_count_artist_ratio()
        )

    def refresh_if_new_duplicates(self) -> None:
        self._logger.debug("Checking for duplicated files...")
        current_count_duplicated_files = (
            self._duplicated_hashes._count_duplicated_files_hashs_sha512()
        )
        if current_count_duplicated_files > self._previously_count_duplicated_files:
            self._logger.debug(
                "Duplicated files found. Updating excluded hash values..."
            )
            self._previously_count_duplicated_files = current_count_duplicated_files
            self.exclude_hashs = (
                self._duplicated_hashes._get_duplicated_hash_values_by_count_artist_ratio()
            )
            self._logger.info("Excluded hash values updated.")


class H2HDB(BaseRepository):
    def __init__(self, config: H2HDBConfig) -> None:
        context = RepositoryContext.from_config(config)
        super().__init__(context)

        self.database_settings = H2HDBCheckDatabaseSettings(context)
        self.todownload_queue = H2HDBToDownloadQueue(context)
        self.todelete_queue = H2HDBToDeleteQueue(context)
        self.gallery_ids = H2HDBGalleriesIDs(context)
        self.pending_removals = H2HDBPendingGalleryRemovals(context, self.gallery_ids)
        self.gallery_gids = H2HDBGalleriesGIDs(context, self.gallery_ids)
        self.gallery_times = H2HDBTimes(context, self.gallery_ids)
        self.gallery_titles = H2HDBGalleriesTitles(context, self.gallery_ids)
        self.upload_accounts = H2HDBUploadAccounts(context, self.gallery_ids)
        self.gallery_infos = H2HDBGalleriesInfos(context)
        self.gallery_comments = H2HDBGalleriesComments(context, self.gallery_ids)
        self.gallery_tags = H2HDBGalleriesTags(context, self.gallery_ids)
        self.files = H2HDBFiles(context, self.gallery_ids)
        self.removed_galleries = H2HDBRemovedGalleries(context)
        self.cbz = H2HDBCBZFiles(context, self.gallery_times, self.gallery_ids)
        self.cbz_integrity = H2HDBCBZIntegrity(context, self.gallery_ids)
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

    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        return self.pending_removals.check_pending_gallery_removal(gallery_name)

    def get_pending_gallery_removals(self) -> list[str]:
        return self.pending_removals.get_pending_gallery_removals()

    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        self.pending_removals.delete_pending_gallery_removal(gallery_name)

    def delete_pending_gallery_removals(self) -> None:
        self.pending_removals.delete_pending_gallery_removals()

    def delete_gallery_file(self, gallery_name: str) -> None:
        self.pending_removals.delete_gallery_file(gallery_name)

    def delete_gallery(self, gallery_name: str) -> None:
        self.pending_removals.delete_gallery(gallery_name)

    def optimize_database(self) -> None:
        self.database_settings.optimize_database()

    def analyze_database(self) -> None:
        self.database_settings.analyze_database()

    def get_pending_download_gids(self) -> list[int]:
        return self.todownload_queue.get_pending_download_gids()

    def check_todelete_gid(self, gid: int) -> bool:
        return self.todelete_queue.check_todelete_gid(gid)

    def insert_todelete_gid(self, gid: int) -> None:
        self.todelete_queue.insert_todelete_gid(gid)

    def check_todownload_gid(self, gid: int, url: str) -> bool:
        return self.todownload_queue.check_todownload_gid(gid, url)

    def insert_todownload_gid(self, gid: int, url: str) -> None:
        self.todownload_queue.insert_todownload_gid(gid, url)

    def update_todownload_gid(self, gid: int, url: str) -> None:
        self.todownload_queue.update_todownload_gid(gid, url)

    def remove_todownload_gid(self, gid: int) -> None:
        self.todownload_queue.remove_todownload_gid(gid)

    def get_todownload_gids(self) -> list[tuple[int, str]]:
        return self.todownload_queue.get_todownload_gids()

    def create_main_tables(self) -> None:
        self.logger.debug("Creating main tables...")
        self.todownload_queue._create_todownload_gids_table()
        self.pending_removals._create_pending_gallery_removals_table()
        self.gallery_ids._create_galleries_names_table()
        self.gallery_gids._create_galleries_gids_table()
        self.todelete_queue._create_todelete_gids_table()
        self.gallery_times._create_galleries_download_times_table()
        self.gallery_times._create_galleries_redownload_times_table()
        self.gallery_times._create_galleries_upload_times_table()
        self.todownload_queue._create_pending_download_gids_view()
        self.gallery_times._create_galleries_modified_times_table()
        self.gallery_times._create_galleries_access_times_table()
        self.gallery_titles._create_galleries_titles_table()
        self.upload_accounts._create_upload_account_table()
        self.gallery_comments._create_galleries_comments_table()
        self.files._create_files_names_table()
        self.gallery_infos._create_galleries_infos_view()
        self.files._create_galleries_files_hashs_tables()
        self.files._create_gallery_image_hash_view()
        self.gallery_infos._create_duplicate_hash_in_gallery_view()
        self.gallery_deduplication._create_gallery_content_hashes_table()
        self.gallery_deduplication._create_gallery_full_content_hashes_table()
        self.gallery_deduplication._create_gallery_full_duplicate_names_view()
        self.gallery_deduplication._create_gallery_duplicate_warnings_table()
        self.gallery_deduplication._create_gallery_duplicate_warnings_names_view()
        self.todelete_queue._create_todelete_names_view()
        self.todelete_queue._create_todelete_names_cache_table()
        self.todelete_queue._create_todelete_rm_commands_view()
        self.removed_galleries._create_removed_galleries_gids_table()
        self.gallery_tags._create_galleries_tags_table()
        self.cbz_integrity._create_cbz_hashes_table()
        self.cbz_integrity._create_cbz_verifications_table()
        self.logger.info("Main tables created.")

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

        for galleryinfo_params in galleryinfo_params_list:
            self.insert_pending_gallery_removal(galleryinfo_params.gallery_name)

        db_gallery_ids = self._insert_gallery_names(galleryinfo_params_list)
        self._insert_gallery_metadata_rows(galleryinfo_params_list, db_gallery_ids)

        file_pairs: list[FileInformation] = list()
        for galleryinfo_params in galleryinfo_params_list:
            db_gallery_id = db_gallery_ids[galleryinfo_params.gallery_name]
            db_file_ids_by_name = self.files._insert_gallery_files(
                db_gallery_id, galleryinfo_params.files_path
            )
            for file_path in galleryinfo_params.files_path:
                db_file_id = db_file_ids_by_name[file_path]
                absolute_file_path = os.path.join(
                    galleryinfo_params.gallery_folder, file_path
                )
                file_pairs.append(FileInformation(absolute_file_path, db_file_id))

        self.files._insert_gallery_file_hash_for_db_gallery_id(file_pairs)

        tags_by_gallery_id = {
            db_gallery_ids[galleryinfo_params.gallery_name]: [
                TagInformation(tag_name, tag_value)
                for tag_name, tag_value in galleryinfo_params.tags
            ]
            for galleryinfo_params in galleryinfo_params_list
        }
        self.gallery_tags._insert_gallery_tags_many(tags_by_gallery_id)

        for galleryinfo_params in galleryinfo_params_list:
            self.delete_pending_gallery_removal(galleryinfo_params.gallery_name)

    def _check_gallery_info_file_hashes(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> list[bool]:
        # Three batched lookups (gallery id, galleryinfo.txt file id, its stored
        # hash) keep query count independent of the number of galleries, instead
        # of issuing them per gallery.
        if not galleryinfo_params_list:
            return []

        # Reads from galleries_dbids itself, not galleries_names: galleries_dbids
        # is the table deletions actually hit, while galleries_names can carry
        # stale orphaned rows on SQLite (its ON DELETE CASCADE is a no-op there
        # since foreign key enforcement is never turned on).
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                [
                    galleryinfo_params.gallery_name
                    for galleryinfo_params in galleryinfo_params_list
                ]
            )
        )
        db_file_ids_by_gallery_id = self.files._get_db_file_ids_by_gallery_ids_for_name(
            list(db_gallery_ids_by_name.values()), GALLERY_INFO_FILE_NAME
        )
        hash_values_by_file_id = self.files._get_hash_values_by_file_ids(
            list(db_file_ids_by_gallery_id.values()), COMPARISON_HASH_ALGORITHM
        )

        issame_list: list[bool] = list()
        for galleryinfo_params in galleryinfo_params_list:
            db_gallery_id = db_gallery_ids_by_name.get(galleryinfo_params.gallery_name)
            gallery_info_file_id = (
                None
                if db_gallery_id is None
                else db_file_ids_by_gallery_id.get(db_gallery_id)
            )
            original_hash_value = (
                None
                if gallery_info_file_id is None
                else hash_values_by_file_id.get(gallery_info_file_id)
            )
            if original_hash_value is None:
                issame_list.append(False)
                continue
            absolute_file_path = os.path.join(
                galleryinfo_params.gallery_folder, GALLERY_INFO_FILE_NAME
            )
            current_hash_value = hash_function_by_file(
                absolute_file_path, COMPARISON_HASH_ALGORITHM
            )
            issame_list.append(original_hash_value == current_hash_value)
        return issame_list

    def insert_gallery_infos(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> list[bool]:
        issame_list = self._check_gallery_info_file_hashes(galleryinfo_params_list)

        is_insert_list: list[bool] = list()
        to_insert: list[GalleryInfoParser] = list()
        for galleryinfo_params, issame in zip(
            galleryinfo_params_list, issame_list, strict=True
        ):
            is_insert = issame is False
            is_insert_list.append(is_insert)
            if is_insert:
                self.logger.debug(
                    f"Inserting gallery '{galleryinfo_params.gallery_name}'..."
                )
                self.delete_gallery_file(galleryinfo_params.gallery_name)
                self.delete_gallery(galleryinfo_params.gallery_name)
                to_insert.append(galleryinfo_params)

        self._insert_gallery_infos(to_insert)

        for galleryinfo_params in to_insert:
            self.logger.debug(f"Gallery '{galleryinfo_params.gallery_name}' inserted.")
        return is_insert_list

    def scan_current_galleries_folders(self) -> tuple[list[str], set[str]]:
        self.delete_pending_gallery_removals()

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

            connector.execute(query)
            self.logger.info(f"{tmp_table_name} table created.")

            insert_query = f"""
                INSERT INTO {tmp_table_name}
                    ({", ".join(column_name_parts)})
                VALUES ({", ".join(["%s" for _ in column_name_parts])})
            """

            data: list[tuple[str, ...]] = list()
            current_galleries_folders: list[str] = list()
            current_galleries_names: set[str] = set()
            for root, _, files in os.walk(self.config.h2h.download_path):
                if GALLERY_INFO_FILE_NAME in files:
                    current_galleries_folders.append(root)
                    gallery_name = os.path.basename(current_galleries_folders[-1])
                    current_galleries_names.add(gallery_name)
                    gallery_name_parts = self._split_gallery_name(gallery_name)
                    data.append(tuple(gallery_name_parts))
            group_size = 5000
            it = iter(data)
            for _ in range(0, len(data), group_size):
                connector.execute_many(insert_query, list(islice(it, group_size)))

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
            raw_removed_galleries = connector.fetch_all(fetch_query)
            removed_gallery_names = [
                str(gallery[0]) for gallery in raw_removed_galleries
            ]

        for removed_gallery in removed_gallery_names:
            self.insert_pending_gallery_removal(removed_gallery)

        self.delete_pending_gallery_removals()

        return (current_galleries_folders, current_galleries_names)

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
        self, gallery_chunk: list[str]
    ) -> list[bool]:
        try:
            return self.insert_gallery_infos(
                [parse_galleryinfo(gallery_folder) for gallery_folder in gallery_chunk]
            )
        except Exception as e:
            if len(gallery_chunk) == 1:
                raise
            mid = len(gallery_chunk) // 2
            self.logger.error(
                f"Error inserting {len(gallery_chunk)} galleries: {e}. "
                f"Retrying as two batches of {mid} and {len(gallery_chunk) - mid}..."
            )
            return self._insert_gallery_chunk_with_split_retry(
                gallery_chunk[:mid]
            ) + self._insert_gallery_chunk_with_split_retry(gallery_chunk[mid:])

    def _sort_galleries_for_processing(
        self, current_galleries_folders: list[str]
    ) -> list[str]:
        if self.config.h2h.cbz_sort in ["upload_time", "download_time", "gid", "title"]:
            self.logger.info(f"Sorting by {self.config.h2h.cbz_sort}...")
            sorted_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_galleryinfo(x), self.config.h2h.cbz_sort),
                reverse=True,
            )
        elif "no" in self.config.h2h.cbz_sort:
            self.logger.info("No sorting...")
            sorted_galleries_folders = current_galleries_folders
        elif "pages" in self.config.h2h.cbz_sort:
            zero_level = (
                max(1, int(self.config.h2h.cbz_sort.split("+")[-1]))
                if "+" in self.config.h2h.cbz_sort
                else 20
            )
            self.logger.info(
                f"Sorting by pages with adjustment based on {zero_level} pages..."
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
        self.logger.info("Galleries sorted.")
        return sorted_galleries_folders

    def _filter_gallery_chunk_for_deduplication(
        self,
        gallery_chunk: list[str],
        exclude_hashs: set[bytes],
        is_insert_list: list[bool],
    ) -> list[str]:
        if not gallery_chunk:
            return []

        gallery_names = [os.path.basename(folder) for folder in gallery_chunk]
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

        kept_folders: list[str] = []
        for folder, is_insert in zip(gallery_chunk, is_insert_list, strict=True):
            db_gallery_id = db_gallery_ids_by_name.get(os.path.basename(folder))
            if db_gallery_id is None:
                # Not yet resolvable (e.g. its insert was skipped/retried) --
                # let it through unchanged rather than silently dropping it.
                kept_folders.append(folder)
                continue

            # galleryinfo.txt is excluded from both hashes below: it embeds
            # this gallery's own GID/metadata, so it necessarily differs
            # between galleries even when every other file is identical --
            # including it would mean these hashes could never match across
            # genuinely duplicate galleries.
            gallery_files = [
                (file_name, file_hash)
                for file_name, file_hash in files_by_db_gallery_id.get(
                    db_gallery_id, []
                )
                if file_name != GALLERY_INFO_FILE_NAME
            ]

            # An empty file set (e.g. every file happens to be excluded, or
            # the gallery genuinely has none besides galleryinfo.txt) hashes
            # to the same value for every such gallery -- that's not a
            # meaningful content match, so galleries in this state opt out of
            # both hash tables entirely rather than being wrongly treated as
            # duplicates of each other.
            content_files = [
                file_hash
                for _, file_hash in gallery_files
                if file_hash not in exclude_hashs
            ]

            # Unfiltered by exclude_hashs: only matches another gallery when
            # every single file (not just the ones that would survive
            # exclusion) is bit-for-bit identical, so a match here is strong
            # enough evidence of a true duplicate re-upload to feed
            # todelete_names automatically (see gallery_full_duplicate_names).
            # Only recomputed when galleryinfo.txt actually changed this run
            # (is_insert) -- its stored hash is a proxy for "this gallery's
            # files could have changed", since every file, including
            # galleryinfo.txt, is only ever (re)written together as a whole
            # gallery re-insert.
            if gallery_files and is_insert:
                full_hash = hashlib.sha256(
                    b"".join(sorted(file_hash for _, file_hash in gallery_files))
                ).digest()
                self.gallery_deduplication._set_full_content_hash(
                    db_gallery_id, full_hash
                )

            if not content_files:
                kept_folders.append(folder)
                continue

            content_hash = hashlib.sha256(b"".join(sorted(content_files))).digest()
            priority_key = (
                not already_uploaded_by_db_gallery_id[db_gallery_id],
                len(titles_by_db_gallery_id[db_gallery_id]),
                download_times_by_db_gallery_id[db_gallery_id],
            )
            should_compress, evicted_db_gallery_id = self.gallery_deduplication.resolve(
                db_gallery_id, content_hash, priority_key
            )
            if evicted_db_gallery_id is not None:
                self._evict_duplicate_gallery_cbz(evicted_db_gallery_id)
            if should_compress:
                kept_folders.append(folder)

        return kept_folders

    def _evict_duplicate_gallery_cbz(self, db_gallery_id: int) -> None:
        self.cbz_integrity._delete_hash(db_gallery_id)
        self.cbz_integrity._delete_verification(db_gallery_id)
        if self.config.h2h.cbz_path is None:
            # No CBZ pipeline running, so there's no physical file to remove.
            return
        gallery_name = self.gallery_ids.get_gallery_names_by_db_gallery_ids(
            [db_gallery_id]
        ).get(db_gallery_id)
        if gallery_name is not None:
            self.cbz._delete_cbz_file_for_gallery_name(gallery_name)

    def _locate_cbz_paths(self, db_gallery_ids: list[int]) -> list[tuple[int, str]]:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        assert self.config.h2h.cbz_path is not None
        if not db_gallery_ids:
            return []

        gallery_names_by_id = self.gallery_ids.get_gallery_names_by_db_gallery_ids(
            db_gallery_ids
        )
        current_cbzs: dict[str, str] = dict()
        for root, _, files in os.walk(self.config.h2h.cbz_path):
            for file in files:
                current_cbzs[file] = root

        located: list[tuple[int, str]] = []
        for db_gallery_id in db_gallery_ids:
            gallery_name = gallery_names_by_id.get(db_gallery_id)
            if gallery_name is None:
                continue
            cbz_file_name = gallery_name_to_cbz_file_name(gallery_name)
            if cbz_file_name not in current_cbzs:
                continue
            located.append(
                (
                    db_gallery_id,
                    os.path.join(current_cbzs[cbz_file_name], cbz_file_name),
                )
            )
        return located

    def _establish_cbz_hash_baselines(
        self, db_gallery_ids: list[int], pool: Pool
    ) -> None:
        """(Re)hash CBZ files and unconditionally record the result as their baseline.

        Used after a rebuild: the freshly-written file is definitionally
        valid, so there's nothing to compare against, only a new baseline to
        record.
        """
        candidates = self._locate_cbz_paths(db_gallery_ids)
        if not candidates:
            return

        actual_hashes = run_in_parallel(
            pool, cbz_scrub_worker, [(cbz_path,) for _, cbz_path in candidates]
        )
        for (db_gallery_id, _), actual_sha256 in zip(
            candidates, actual_hashes, strict=True
        ):
            if actual_sha256 is None:
                continue
            self.cbz_integrity._upsert_hash(db_gallery_id, actual_sha256)
            self.cbz_integrity._set_verification_to_now(db_gallery_id)

    def _scrub_cbz_files(
        self,
        current_galleries_folders: list[str],
        exclude_hashs: set[bytes],
        pool: Pool,
    ) -> None:
        batch_size = self.config.h2h.cbz_scrub_batch_size
        if batch_size <= 0:
            return

        folder_by_gallery_name = {
            os.path.basename(folder): folder for folder in current_galleries_folders
        }
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                list(folder_by_gallery_name.keys())
            )
        )
        candidate_ids = self.cbz_integrity._get_scrub_candidates(
            list(db_gallery_ids_by_name.values()), batch_size
        )
        candidates = self._locate_cbz_paths(candidate_ids)
        if not candidates:
            return

        self.logger.info(f"Scrubbing {len(candidates)} CBZ file(s) for integrity...")
        actual_hashes = run_in_parallel(
            pool, cbz_scrub_worker, [(cbz_path,) for _, cbz_path in candidates]
        )

        corrupt_db_gallery_ids: list[int] = []
        for (db_gallery_id, _), actual_sha256 in zip(
            candidates, actual_hashes, strict=True
        ):
            expected_sha256 = self.cbz_integrity._get_hash(db_gallery_id)
            if actual_sha256 is None or (
                expected_sha256 is not None and actual_sha256 != expected_sha256
            ):
                corrupt_db_gallery_ids.append(db_gallery_id)
                continue
            self.cbz_integrity._upsert_hash(db_gallery_id, actual_sha256)
            self.cbz_integrity._set_verification_to_now(db_gallery_id)

        if not corrupt_db_gallery_ids:
            return

        self.logger.info(
            f"Rebuilding {len(corrupt_db_gallery_ids)} CBZ file(s) that failed "
            "integrity verification..."
        )
        corrupt_gallery_names_by_id = (
            self.gallery_ids.get_gallery_names_by_db_gallery_ids(corrupt_db_gallery_ids)
        )
        corrupt_folders = [
            folder_by_gallery_name[gallery_name]
            for gallery_name in corrupt_gallery_names_by_id.values()
            if gallery_name in folder_by_gallery_name
        ]
        if not corrupt_folders:
            return
        self.cbz.compress_galleries_to_cbz(corrupt_folders, exclude_hashs, pool)
        self._establish_cbz_hash_baselines(corrupt_db_gallery_ids, pool)

    def insert_h2h_download(self) -> bool:
        self.delete_pending_gallery_removals()

        current_galleries_folders, current_galleries_names = (
            self.scan_current_galleries_folders()
        )

        if self.config.h2h.cbz_path is not None:
            self.cbz._refresh_current_cbz_files(current_galleries_names)

        self.logger.info("Inserting galleries...")
        current_galleries_folders = self._sort_galleries_for_processing(
            current_galleries_folders
        )

        self.logger.info("Getting excluded hash values...")
        exclude_hashes_tracker = _ExcludeHashesTracker(
            self.duplicated_hashes, self.logger
        )
        self.logger.info("Excluded hash values obtained.")

        total_inserted_in_database = 0
        total_created_cbz = 0
        is_insert_limit_reached = False
        chunked_galleries_folders = chunk_list(
            current_galleries_folders, 1000 * POOL_CPU_LIMIT
        )
        total_chunks = len(chunked_galleries_folders)
        total_galleries = len(current_galleries_folders)
        galleries_processed = 0
        self.logger.info(
            f"Inserting {total_galleries} galleries in parallel "
            f"across {total_chunks} chunk(s)..."
        )
        cbz_pool_cm = (
            Pool(POOL_CPU_LIMIT)
            if self.config.h2h.cbz_path is not None
            else contextlib.nullcontext()
        )
        with cbz_pool_cm as cbz_pool:
            for chunk_index, gallery_chunk in enumerate(chunked_galleries_folders, 1):
                galleries_processed += len(gallery_chunk)
                self.logger.info(
                    f"Processing chunk {chunk_index}/{total_chunks} "
                    f"({galleries_processed}/{total_galleries} galleries)..."
                )
                is_insert_list = self._insert_gallery_chunk_with_split_retry(
                    gallery_chunk
                )
                if any(is_insert_list):
                    self.logger.info("There are new galleries inserted in database.")
                    is_insert_limit_reached |= True
                    total_inserted_in_database += sum(is_insert_list)
                    exclude_hashes_tracker.refresh_if_new_duplicates()

                # Gallery-content dedup detection (gallery_content_hashes,
                # gallery_full_content_hashes, gallery_duplicate_warnings) is
                # useful on its own -- e.g. todelete_names -- independently of
                # whether CBZ output is configured, so it always runs. Only
                # actually compressing the filtered result is CBZ-specific.
                gallery_chunk_to_compress = (
                    self._filter_gallery_chunk_for_deduplication(
                        gallery_chunk,
                        exclude_hashes_tracker.exclude_hashs,
                        is_insert_list,
                    )
                )

                if self.config.h2h.cbz_path is not None:
                    is_new_list = self.cbz.compress_galleries_to_cbz(
                        gallery_chunk_to_compress,
                        exclude_hashes_tracker.exclude_hashs,
                        cast(Pool, cbz_pool),
                    )
                    if any(is_new_list):
                        self.logger.info("There are new CBZ files created.")
                        total_created_cbz += sum(is_new_list)
            self.logger.info(
                f"Total galleries inserted in database: {total_inserted_in_database}"
            )
            self.logger.info(f"Total CBZ files created: {total_created_cbz}")

            if self.config.h2h.cbz_path is not None:
                self.logger.info(
                    "Checking for CBZ files made stale by new exclusions..."
                )
                final_exclude_hashs = (
                    self.duplicated_hashes._get_duplicated_hash_values_by_count_artist_ratio()
                )
                stale_galleries = self.cbz.get_stale_cbz_galleries(
                    current_galleries_names, final_exclude_hashs, cast(Pool, cbz_pool)
                )
                if stale_galleries:
                    self.logger.info(
                        f"Recompressing {len(stale_galleries)} CBZ file(s) made stale "
                        "by newly-excluded duplicate files..."
                    )
                    folder_by_gallery_name = {
                        os.path.basename(folder): folder
                        for folder in current_galleries_folders
                    }
                    stale_folders_to_filter = [
                        folder_by_gallery_name[name]
                        for name in stale_galleries
                        if name in folder_by_gallery_name
                    ]
                    stale_folders = self._filter_gallery_chunk_for_deduplication(
                        stale_folders_to_filter,
                        final_exclude_hashs,
                        # These galleries are only here because
                        # get_stale_cbz_galleries flagged newly-excluded
                        # duplicate files, not because their own content
                        # changed -- their full-content hash is unaffected
                        # and already correct from a prior pass.
                        [False] * len(stale_folders_to_filter),
                    )
                    self.cbz.compress_galleries_to_cbz(
                        stale_folders,
                        final_exclude_hashs,
                        cast(Pool, cbz_pool),
                    )

                self._scrub_cbz_files(
                    current_galleries_folders, final_exclude_hashs, cast(Pool, cbz_pool)
                )

        self.logger.info("Cleaning up database...")
        self.refresh_current_files_hashs()

        return is_insert_limit_reached

    def queue_redownload_for_pending_deletions(self) -> None:
        self.todelete_queue._queue_redownload_for_todelete_names()

    def refresh_todelete_names_cache(self) -> None:
        self.todelete_queue.refresh_todelete_names_cache()

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
