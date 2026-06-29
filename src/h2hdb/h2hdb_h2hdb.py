__all__ = ["H2HDB", "GALLERY_INFO_FILE_NAME"]


import os
from itertools import islice
from typing import Any

from h2h_galleryinfo_parser import (
    GalleryInfoParser,
    parse_galleryinfo,
)

from .cbz_files import H2HDBCBZFiles
from .config_loader import H2HDBConfig
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
from .table_database_setting import H2HDBCheckDatabaseSettings
from .table_download_queue import H2HDBDownloadQueue
from .table_files_dbids import H2HDBFiles
from .table_gids import H2HDBGalleriesGIDs, H2HDBGalleriesIDs
from .table_pending_removals import H2HDBPendingGalleryRemovals
from .table_removed_gids import H2HDBRemovedGalleries
from .table_tags import H2HDBGalleriesTags
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles
from .table_uploadaccounts import H2HDBUploadAccounts
from .threading_tools import POOL_CPU_LIMIT, run_in_parallel
from .view_ginfo import H2HDBGalleriesInfos

GALLERY_METADATA_BATCH_SIZE = 500


def get_sorting_base_level(x: int = 20) -> int:
    zero_level = max(x, 1)
    return zero_level


class H2HDB(BaseRepository):
    def __init__(self, config: H2HDBConfig) -> None:
        context = RepositoryContext.from_config(config)
        super().__init__(context)

        self.database_settings = H2HDBCheckDatabaseSettings(context)
        self.download_queue = H2HDBDownloadQueue(context)
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
        self.cbz = H2HDBCBZFiles(context, self.gallery_times)

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

    def _count_duplicated_files_hashs_sha512(self) -> int:
        with self.SQLConnector() as connector:
            table_name = "duplicated_files_hashs_sha512"
            query = f"""
                SELECT COUNT(*)
                FROM {table_name}
            """
            query_result = connector.fetch_one(query)
        return int(query_result[0])

    def _create_duplicated_galleries_tables(self) -> None:
        with self.SQLConnector() as connector:
            query = """
                    CREATE VIEW IF NOT EXISTS duplicated_files_hashs_sha512 AS
                    SELECT db_file_id,
                        db_hash_id
                    FROM files_hashs_sha512
                    GROUP BY db_hash_id
                    HAVING COUNT(*) >= 3
                    """
            connector.execute(query)

        with self.SQLConnector() as connector:
            query = """
                CREATE VIEW IF NOT EXISTS duplicated_hash_values_by_count_artist_ratio AS WITH duplicated_db_dbids AS (
                            SELECT galleries_dbids.db_gallery_id AS db_gallery_id,
                                files_dbids.db_file_id AS db_file_id,
                                duplicated_files_hashs_sha512.db_hash_id AS db_hash_id,
                                galleries_tag_pairs_dbids.tag_value AS artist_value
                            FROM duplicated_files_hashs_sha512
                                LEFT JOIN files_hashs_sha512 ON duplicated_files_hashs_sha512.db_hash_id = files_hashs_sha512.db_hash_id
                                LEFT JOIN files_dbids ON files_hashs_sha512.db_file_id = files_dbids.db_file_id
                                LEFT JOIN galleries_dbids ON files_dbids.db_gallery_id = galleries_dbids.db_gallery_id
                                LEFT JOIN galleries_tags ON galleries_dbids.db_gallery_id = galleries_tags.db_gallery_id
                                LEFT JOIN galleries_tag_pairs_dbids ON galleries_tags.db_tag_pair_id = galleries_tag_pairs_dbids.db_tag_pair_id
                            WHERE galleries_tag_pairs_dbids.tag_name = 'artist'
                        ),
                        duplicated_count_artists_by_db_gallery_id AS(
                            SELECT COUNT(DISTINCT artist_value) AS artist_count,
                                db_gallery_id
                            FROM duplicated_db_dbids
                            GROUP BY db_gallery_id
                        )
                        SELECT files_hashs_sha512_dbids.hash_value AS hash_value
                        FROM duplicated_db_dbids
                            LEFT JOIN duplicated_count_artists_by_db_gallery_id ON duplicated_db_dbids.db_gallery_id = duplicated_count_artists_by_db_gallery_id.db_gallery_id
                            LEFT JOIN files_hashs_sha512_dbids ON duplicated_db_dbids.db_hash_id = files_hashs_sha512_dbids.db_hash_id
                        GROUP BY duplicated_db_dbids.db_hash_id
                        HAVING COUNT(DISTINCT duplicated_db_dbids.artist_value) / MAX(
                                duplicated_count_artists_by_db_gallery_id.artist_count
                            ) > 2
                        """
            connector.execute(query)

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

    def get_pending_download_gids(self) -> list[int]:
        return self.download_queue.get_pending_download_gids()

    def check_todelete_gid(self, gid: int) -> bool:
        return self.download_queue.check_todelete_gid(gid)

    def insert_todelete_gid(self, gid: int) -> None:
        self.download_queue.insert_todelete_gid(gid)

    def check_todownload_gid(self, gid: int, url: str) -> bool:
        return self.download_queue.check_todownload_gid(gid, url)

    def insert_todownload_gid(self, gid: int, url: str) -> None:
        self.download_queue.insert_todownload_gid(gid, url)

    def update_todownload_gid(self, gid: int, url: str) -> None:
        self.download_queue.update_todownload_gid(gid, url)

    def remove_todownload_gid(self, gid: int) -> None:
        self.download_queue.remove_todownload_gid(gid)

    def get_todownload_gids(self) -> list[tuple[int, str]]:
        return self.download_queue.get_todownload_gids()

    def create_main_tables(self) -> None:
        self.logger.debug("Creating main tables...")
        self.download_queue._create_todownload_gids_table()
        self.pending_removals._create_pending_gallery_removals_table()
        self.gallery_ids._create_galleries_names_table()
        self.gallery_gids._create_galleries_gids_table()
        self.download_queue._create_todelete_gids_table()
        self.gallery_times._create_galleries_download_times_table()
        self.gallery_times._create_galleries_redownload_times_table()
        self.gallery_times._create_galleries_upload_times_table()
        self.download_queue._create_pending_download_gids_view()
        self.gallery_times._create_galleries_modified_times_table()
        self.gallery_times._create_galleries_access_times_table()
        self.gallery_titles._create_galleries_titles_table()
        self.upload_accounts._create_upload_account_table()
        self.gallery_comments._create_galleries_comments_table()
        self.files._create_files_names_table()
        self.gallery_infos._create_galleries_infos_view()
        self.download_queue._create_todelete_names_view()
        self.files._create_galleries_files_hashs_tables()
        self.files._create_gallery_image_hash_view()
        self.gallery_infos._create_duplicate_hash_in_gallery_view()
        self.removed_galleries._create_removed_galleries_gids_table()
        self.gallery_tags._create_galleries_tags_table()
        self._create_duplicated_galleries_tables()
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

        db_gallery_ids = {
            galleryinfo_params.gallery_name: (
                self.gallery_ids._get_db_gallery_id_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
            )
            for galleryinfo_params in galleryinfo_params_list
        }
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

    def _check_gallery_info_file_hash(
        self, galleryinfo_params: GalleryInfoParser
    ) -> bool:
        if not self.gallery_ids._check_galleries_dbids_by_gallery_name(
            galleryinfo_params.gallery_name
        ):
            return False
        db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
            galleryinfo_params.gallery_name
        )

        if not self.files._check_db_file_id(db_gallery_id, GALLERY_INFO_FILE_NAME):
            return False
        gallery_info_file_id = self.files._get_db_file_id(
            db_gallery_id, GALLERY_INFO_FILE_NAME
        )
        absolute_file_path = os.path.join(
            galleryinfo_params.gallery_folder, GALLERY_INFO_FILE_NAME
        )

        if not self.files._check_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        ):
            return False
        original_hash_value = self.files.get_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        )
        current_hash_value = hash_function_by_file(
            absolute_file_path, COMPARISON_HASH_ALGORITHM
        )
        issame = original_hash_value == current_hash_value
        return issame

    def _get_duplicated_hash_values_by_count_artist_ratio(self) -> set[bytes]:
        with self.SQLConnector() as connector:
            table_name = "duplicated_hash_values_by_count_artist_ratio"
            select_query = f"""
                SELECT hash_value
                FROM {table_name}
            """

            query_result = connector.fetch_all(select_query)
        return {query[0] for query in query_result}

    def insert_gallery_infos(
        self, galleryinfo_params_list: list[GalleryInfoParser]
    ) -> list[bool]:
        is_insert_list: list[bool] = list()
        to_insert: list[GalleryInfoParser] = list()
        for galleryinfo_params in galleryinfo_params_list:
            is_insert = self._check_gallery_info_file_hash(galleryinfo_params) is False
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

            data: list[tuple[Any, ...]] = list()
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

    def insert_h2h_download(self) -> bool:
        self.delete_pending_gallery_removals()

        current_galleries_folders, current_galleries_names = (
            self.scan_current_galleries_folders()
        )

        self.cbz._refresh_current_cbz_files(current_galleries_names)

        self.logger.info("Inserting galleries...")
        if self.config.h2h.cbz_sort in ["upload_time", "download_time", "gid", "title"]:
            self.logger.info(f"Sorting by {self.config.h2h.cbz_sort}...")
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_galleryinfo(x), self.config.h2h.cbz_sort),
                reverse=True,
            )
        elif "no" in self.config.h2h.cbz_sort:
            self.logger.info("No sorting...")
            pass
        elif "pages" in self.config.h2h.cbz_sort:
            self.logger.info("Sorting by pages...")
            zero_level = (
                max(1, int(self.config.h2h.cbz_sort.split("+")[-1]))
                if "+" in self.config.h2h.cbz_sort
                else 20
            )
            self.logger.info(
                f"Sorting by pages with adjustment based on {zero_level} pages..."
            )
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: abs(getattr(parse_galleryinfo(x), "pages") - zero_level),
            )
        else:
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_galleryinfo(x), "pages"),
            )
        self.logger.info("Galleries sorted.")

        self.logger.info("Getting excluded hash values...")
        exclude_hashs = set[bytes]()
        previously_count_duplicated_files = 0
        self.logger.info("Excluded hash values obtained.")

        def calculate_exclude_hashs(
            previously_count_duplicated_files: int, exclude_hashs: set[bytes]
        ) -> tuple[int, set[bytes]]:
            self.logger.debug("Checking for duplicated files...")
            current_count_duplicated_files = self._count_duplicated_files_hashs_sha512()
            new_exclude_hashs = exclude_hashs
            if current_count_duplicated_files > previously_count_duplicated_files:
                self.logger.debug(
                    "Duplicated files found. Updating excluded hash values..."
                )
                previously_count_duplicated_files = current_count_duplicated_files
                new_exclude_hashs = (
                    self._get_duplicated_hash_values_by_count_artist_ratio()
                )
                self.logger.info("Excluded hash values updated.")
            return previously_count_duplicated_files, new_exclude_hashs

        total_inserted_in_database = 0
        total_created_cbz = 0
        is_insert_limit_reached = False
        chunked_galleries_folders = chunk_list(
            current_galleries_folders, 100 * POOL_CPU_LIMIT
        )
        self.logger.info("Inserting galleries in parallel...")
        for gallery_chunk in chunked_galleries_folders:
            # Insert gallery info to database
            is_insert_list: list[bool] = list()
            try:
                is_insert_list = self.insert_gallery_infos(
                    [
                        parse_galleryinfo(gallery_folder)
                        for gallery_folder in gallery_chunk
                    ]
                )
            except Exception as e:
                self.logger.error(f"Error inserting galleries: {e}")
                self.logger.error("Retrying galleries one by one")
                for x in gallery_chunk:
                    self.logger.error(f"Retrying gallery '{x}'...")
                    is_insert_list.append(
                        self.insert_gallery_infos([parse_galleryinfo(x)])[0]
                    )
            if any(is_insert_list):
                self.logger.info("There are new galleries inserted in database.")
                is_insert_limit_reached |= True
                total_inserted_in_database += sum(is_insert_list)

            # Compress gallery to CBZ file
            if self.config.h2h.cbz_path != "":
                if any(is_insert_list):
                    previously_count_duplicated_files, exclude_hashs = (
                        calculate_exclude_hashs(
                            previously_count_duplicated_files, exclude_hashs
                        )
                    )
                config_data = self.config.model_dump(mode="json")
                is_new_list = run_in_parallel(
                    compress_gallery_to_cbz_worker,
                    [(config_data, x, exclude_hashs) for x in gallery_chunk],
                )
                if any(is_new_list):
                    self.logger.info("There are new CBZ files created.")
                    total_created_cbz += sum(is_new_list)
        self.logger.info(
            f"Total galleries inserted in database: {total_inserted_in_database}"
        )
        self.logger.info(f"Total CBZ files created: {total_created_cbz}")

        if self.config.h2h.cbz_path != "":
            self.logger.info("Checking for CBZ files made stale by new exclusions...")
            final_exclude_hashs = (
                self._get_duplicated_hash_values_by_count_artist_ratio()
            )
            stale_galleries = self.cbz.get_stale_cbz_galleries(
                current_galleries_names, final_exclude_hashs
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
                config_data = self.config.model_dump(mode="json")
                run_in_parallel(
                    compress_gallery_to_cbz_worker,
                    [
                        (config_data, folder_by_gallery_name[name], final_exclude_hashs)
                        for name in stale_galleries
                        if name in folder_by_gallery_name
                    ],
                )

        self.logger.info("Cleaning up database...")
        self.refresh_current_files_hashs()

        return is_insert_limit_reached

    def reset_redownload_times(self) -> None:
        self.gallery_times._reset_redownload_times()

    def get_komga_metadata(
        self, gallery_name: str
    ) -> dict[str, str | list[dict[str, str]]]:
        metadata: dict[str, str | list[dict[str, str]]] = dict()
        metadata["title"] = self.gallery_titles.get_title_by_gallery_name(gallery_name)
        if self.gallery_comments._check_gallery_comment_by_gallery_name(gallery_name):
            metadata["summary"] = self.gallery_comments.get_comment_by_gallery_name(
                gallery_name
            )
        else:
            metadata["summary"] = ""
        upload_time = self.gallery_times.get_upload_time_by_gallery_name(gallery_name)
        metadata["releaseDate"] = "-".join(
            [
                str(upload_time.year),
                f"{upload_time.month:02d}",
                f"{upload_time.day:02d}",
            ]
        )
        tags = self.gallery_tags.get_tag_pairs_by_gallery_name(gallery_name)
        metadata["authors"] = [
            {"name": value, "role": key} for key, value in tags if value != ""
        ]
        return metadata


def compress_gallery_to_cbz_worker(
    config_data: dict[str, Any],
    gallery_folder: str,
    exclude_hashs: set[bytes],
) -> bool:
    config = H2HDBConfig.model_validate(config_data)
    with H2HDB(config=config) as connector:
        return connector.cbz.compress_gallery_to_cbz(gallery_folder, exclude_hashs)
