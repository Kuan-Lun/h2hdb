__all__ = ["H2HDB", "GALLERY_INFO_FILE_NAME"]


import os
from itertools import islice
from time import sleep

from h2h_galleryinfo_parser import (
    parse_galleryinfo,
    GalleryInfoParser,
    GalleryURLParser,
)

from .view_ginfo import H2HDBGalleriesInfos
from .table_tags import H2HDBGalleriesTags
from .table_files_dbids import H2HDBFiles
from .table_removed_gids import H2HDBRemovedGalleries
from .table_comments import H2HDBGalleriesComments
from .information import FileInformation, TagInformation
from .hash_dict import HASH_ALGORITHMS
from .threading_tools import SQLThreadsList, run_in_parallel, POOL_CPU_LIMIT
from .settings import hash_function_by_file, chunk_list
from .settings import (
    FOLDER_NAME_LENGTH_LIMIT,
    FILE_NAME_LENGTH_LIMIT,
    COMPARISON_HASH_ALGORITHM,
    GALLERY_INFO_FILE_NAME,
)


def get_sorting_base_level(x: int = 20) -> int:
    zero_level = max(x, 1)
    return zero_level


class H2HDB(
    H2HDBGalleriesInfos,
    H2HDBGalleriesComments,
    H2HDBGalleriesTags,
    H2HDBFiles,
    H2HDBRemovedGalleries,
):
    def _create_pending_gallery_removals_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_gallery_name_based_on_limit(column_name)
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql},
                            full_name TEXT NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _count_duplicated_files_hashs_sha512(self) -> int:
        with self.SQLConnector() as connector:
            table_name = "duplicated_files_hashs_sha512"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                    """
            query_result = connector.fetch_one(query)
        return query_result[0]

    def _create_duplicated_galleries_tables(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
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
            match self.config.database.sql_type.lower():
                case "mysql":
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
        with self.SQLConnector() as connector:
            if self.check_pending_gallery_removal(gallery_name) is False:
                table_name = "pending_gallery_removals"
                if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
                    self.logger.error(
                        f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
                    )
                    raise ValueError("Gallery name is too long.")
                gallery_name_parts = self._split_gallery_name(gallery_name)

                match self.config.database.sql_type.lower():
                    case "mysql":
                        column_name_parts, _ = (
                            self.mysql_split_gallery_name_based_on_limit("name")
                        )
                        insert_query = f"""
                            INSERT INTO {table_name} ({", ".join(column_name_parts)}, full_name)
                            VALUES ({", ".join(["%s" for _ in column_name_parts])}, %s)
                        """
                connector.execute(
                    insert_query, (*tuple(gallery_name_parts), gallery_name)
                )

    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            gallery_name_parts = self._split_gallery_name(gallery_name)
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    select_query = f"""
                        SELECT full_name
                        FROM {table_name}
                        WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """
            query_result = connector.fetch_one(select_query, tuple(gallery_name_parts))
            return query_result is not None

    def get_pending_gallery_removals(self) -> list[str]:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT full_name
                        FROM {table_name}
                    """

            query_result = connector.fetch_all(select_query)
            pending_gallery_removals = [query[0] for query in query_result]
        return pending_gallery_removals

    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    delete_query = f"""
                        DELETE FROM {table_name} WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(delete_query, tuple(gallery_name_parts))

    def delete_pending_gallery_removals(self) -> None:
        pending_gallery_removals = self.get_pending_gallery_removals()
        for gallery_name in pending_gallery_removals:
            self.delete_gallery_file(gallery_name)
            self.delete_gallery(gallery_name)
            self.delete_pending_gallery_removal(gallery_name)

    def delete_gallery_file(self, gallery_name: str) -> None:
        # self.logger.info(f"Gallery images for '{gallery_name}' deleted.")
        pass

    def delete_gallery(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            if not self._check_galleries_dbids_by_gallery_name(gallery_name):
                self.logger.debug(f"Gallery '{gallery_name}' does not exist.")
                return

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    get_delete_gallery_id_query = f"""
                        DELETE FROM galleries_dbids
                        WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                        """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(get_delete_gallery_id_query, tuple(gallery_name_parts))
            self.logger.info(f"Gallery '{gallery_name}' deleted.")

    def optimize_database(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_table_name_query = f"""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE REFERENCED_TABLE_SCHEMA = '{self.config.database.database}'
                    """
            table_names = connector.fetch_all(select_table_name_query)
            table_names = [t[0] for t in table_names]

            match self.config.database.sql_type.lower():
                case "mysql":
                    get_optimize_query = lambda x: "OPTIMIZE TABLE {x}".format(x=x)

            for table_name in table_names:
                connector.execute(get_optimize_query(table_name))
            self.logger.info("Database optimized.")

    def _create_pending_download_gids_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS pending_download_gids AS
                            SELECT gids.gid AS gid
                            FROM (SELECT *
                                FROM galleries_redownload_times AS grt0
                                WHERE DATE_ADD(grt0.time, INTERVAL 7 DAY) <= NOW()
                                )
                                AS grt
                            INNER JOIN galleries_download_times AS gdt
                                on grt.db_gallery_id = gdt.db_gallery_id
                            INNER JOIN galleries_upload_times AS gut
                                ON grt.db_gallery_id = gut.db_gallery_id
                            INNER JOIN galleries_gids AS gids
                                ON grt.db_gallery_id = gids.db_gallery_id
                            WHERE grt.time <= DATE_ADD(gut.time, INTERVAL 1 YEAR)
                                AND DATE_ADD(gut.time, INTERVAL 7 DAY) <= NOW()
                                OR DATE_ADD(gdt.time, INTERVAL 7 DAY) <= grt.time
                                 ORDER BY gut.`time` DESC
                    """
            connector.execute(query)
            self.logger.info("pending_download_gids view created.")

    def get_pending_download_gids(self) -> list[int]:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        SELECT gid
                        FROM pending_download_gids
                    """
            query_result = connector.fetch_all(query)
            pending_download_gids = [query[0] for query in query_result]
        return pending_download_gids

    def _create_todelete_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todelete_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            FOREIGN KEY (gid) REFERENCES galleries_gids(gid)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            gid          INT UNSIGNED NOT NULL
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _create_todelete_names_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todelete_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE VIEW IF NOT EXISTS {table_name} AS
                            SELECT full_name
                            FROM 
                            (SELECT galleries_names.full_name AS full_name
                            FROM todelete_gids
                            INNER JOIN galleries_gids
                                ON galleries_gids.gid = todelete_gids.gid
                            INNER JOIN galleries_names
                                ON galleries_names.db_gallery_id = galleries_gids.db_gallery_id) AS todelete_names
                            UNION
                                SELECT full_name
                                FROM (
                                    SELECT gi.name AS full_name
                                    FROM galleries_infos gi
                                    JOIN (
                                        SELECT gid, MAX(download_time) AS max_download_time
                                        FROM galleries_infos
                                        GROUP BY gid
                                        HAVING COUNT(*) > 1
                                    ) sub ON gi.gid = sub.gid
                                    WHERE gi.download_time < sub.max_download_time
                                    ) AS duplicated_gids_names
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def check_todelete_gid(self, gid: int) -> bool:
        with self.SQLConnector() as connector:
            table_name = "todelete_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid
                        FROM {table_name}
                        WHERE gid = %s
                    """
                    query_result = connector.fetch_one(select_query, (gid,))
        return query_result is not None

    def insert_todelete_gid(self, gid: int) -> None:
        if not self.check_todelete_gid(gid):
            with self.SQLConnector() as connector:
                table_name = "todelete_gids"
                match self.config.database.sql_type.lower():
                    case "mysql":
                        insert_query = f"""
                            INSERT INTO {table_name} (gid) VALUES (%s)
                        """
                connector.execute(insert_query, (gid,))

    def _create_todownload_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            gid          INT UNSIGNED NOT NULL,
                            url          CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def check_todownload_gid(self, gid: int, url: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    if url != "":
                        select_query = f"""
                            SELECT gid
                            FROM {table_name}
                            WHERE gid = %s AND url = %s
                        """
                        query_result = connector.fetch_one(select_query, (gid, url))
                    else:
                        select_query = f"""
                            SELECT gid
                            FROM {table_name}
                            WHERE gid = %s
                        """
                        query_result = connector.fetch_one(select_query, (gid,))
        return query_result is not None

    def insert_todownload_gid(self, gid: int, url: str) -> None:
        if url != "":
            gallery = GalleryURLParser(url)
            gid = gallery.gid
            if gallery.gid != gid and gid != 0:
                raise ValueError(
                    f"Gallery GID {gid} does not match URL GID {gallery.gid}."
                )
        elif gid <= 0:
            raise ValueError("Gallery GID must be greater than zero.")

        if not self.check_todownload_gid(gid, url):
            if (url == "") or (not self.check_todownload_gid(gid, "")):
                with self.SQLConnector() as connector:
                    table_name = "todownload_gids"
                    match self.config.database.sql_type.lower():
                        case "mysql":
                            insert_query = f"""
                                INSERT INTO {table_name} (gid, url) VALUES (%s, %s)
                            """
                    connector.execute(insert_query, (gid, url))
            else:
                self.update_todownload_gid(gid, url)

    def update_todownload_gid(self, gid: int, url: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET url = %s WHERE gid = %s
                    """
            connector.execute(update_query, (url, gid))

    def remove_todownload_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    delete_query = f"""
                        DELETE FROM {table_name} WHERE gid = %s
                    """
            connector.execute(delete_query, (gid,))

    def get_todownload_gids(self) -> list[tuple[int, str]]:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid, url
                        FROM {table_name}
                    """
            query_result = connector.fetch_all(select_query)
        todownload_gids = [(query[0], query[1]) for query in query_result]
        return todownload_gids

    def create_main_tables(self) -> None:
        self.logger.debug("Creating main tables...")
        self._create_todownload_gids_table()
        self._create_pending_gallery_removals_table()
        self._create_galleries_names_table()
        self._create_galleries_gids_table()
        self._create_todelete_gids_table()
        self._create_galleries_download_times_table()
        self._create_galleries_redownload_times_table()
        self._create_galleries_upload_times_table()
        self._create_pending_download_gids_view()
        self._create_galleries_modified_times_table()
        self._create_galleries_access_times_table()
        self._create_galleries_titles_table()
        self._create_upload_account_table()
        self._create_galleries_comments_table()
        self._create_files_names_table()
        self._create_galleries_infos_view()
        self._create_todelete_names_view()
        self._create_galleries_files_hashs_tables()
        self._create_gallery_image_hash_view()
        self._create_duplicate_hash_in_gallery_view()
        self._create_removed_galleries_gids_table()
        self._create_galleries_tags_table()
        self._create_duplicated_galleries_tables()
        self.logger.info("Main tables created.")

    def update_redownload_time_to_now_by_gid(self, gid: int) -> None:
        db_gallery_id = self._get_db_gallery_id_by_gid(gid)
        table_name = "galleries_redownload_times"
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET time = NOW() WHERE db_gallery_id = %s
                    """
            connector.execute(update_query, (db_gallery_id,))

    def _insert_gallery_info(self, galleryinfo_params: GalleryInfoParser) -> None:
        self.insert_pending_gallery_removal(galleryinfo_params.gallery_name)

        self._insert_gallery_name(galleryinfo_params.gallery_name)
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(
            galleryinfo_params.gallery_name
        )

        with SQLThreadsList() as threads:
            threads.append(
                target=self._insert_gallery_gid,
                args=(db_gallery_id, galleryinfo_params.gid),
            )
            threads.append(
                target=self._insert_gallery_title,
                args=(db_gallery_id, galleryinfo_params.title),
            )
            threads.append(
                target=self._insert_upload_time,
                args=(db_gallery_id, galleryinfo_params.upload_time),
            )
            threads.append(
                target=self._insert_gallery_comment,
                args=(db_gallery_id, galleryinfo_params.galleries_comments),
            )
            threads.append(
                target=self._insert_gallery_upload_account,
                args=(db_gallery_id, galleryinfo_params.upload_account),
            )
            threads.append(
                target=self._insert_download_time,
                args=(db_gallery_id, galleryinfo_params.download_time),
            )
            threads.append(
                target=self._insert_access_time,
                args=(db_gallery_id, galleryinfo_params.download_time),
            )
            threads.append(
                target=self._insert_modified_time,
                args=(db_gallery_id, galleryinfo_params.modified_time),
            )
            threads.append(
                target=self._insert_gallery_files,
                args=(db_gallery_id, galleryinfo_params.files_path),
            )

        file_pairs = list[FileInformation]()
        for file_path in galleryinfo_params.files_path:
            db_file_id = self._get_db_file_id(db_gallery_id, file_path)
            absolute_file_path = os.path.join(
                galleryinfo_params.gallery_folder, file_path
            )
            file_pairs.append(FileInformation(absolute_file_path, db_file_id))
        self._insert_gallery_file_hash_for_db_gallery_id(file_pairs)

        taglist = list[TagInformation]()
        for tag in galleryinfo_params.tags:
            taglist.append(TagInformation(tag[0], tag[1]))
        self._insert_gallery_tags(db_gallery_id, taglist)

        self.delete_pending_gallery_removal(galleryinfo_params.gallery_name)

    def _check_gallery_info_file_hash(
        self, galleryinfo_params: GalleryInfoParser
    ) -> bool:
        if not self._check_galleries_dbids_by_gallery_name(
            galleryinfo_params.gallery_name
        ):
            return False
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(
            galleryinfo_params.gallery_name
        )

        if not self._check_db_file_id(db_gallery_id, GALLERY_INFO_FILE_NAME):
            return False
        gallery_info_file_id = self._get_db_file_id(
            db_gallery_id, GALLERY_INFO_FILE_NAME
        )
        absolute_file_path = os.path.join(
            galleryinfo_params.gallery_folder, GALLERY_INFO_FILE_NAME
        )

        if not self._check_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        ):
            return False
        original_hash_value = self.get_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        )
        current_hash_value = hash_function_by_file(
            absolute_file_path, COMPARISON_HASH_ALGORITHM
        )
        issame = original_hash_value == current_hash_value
        return issame

    def _get_duplicated_hash_values_by_count_artist_ratio(self) -> list[bytes]:
        with self.SQLConnector() as connector:
            table_name = "duplicated_hash_values_by_count_artist_ratio"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT hash_value
                        FROM {table_name}
                    """

            query_result = connector.fetch_all(select_query)
        return [query[0] for query in query_result]

    def insert_gallery_info(self, gallery_folder: str) -> bool:
        galleryinfo_params = parse_galleryinfo(gallery_folder)
        is_thesame = self._check_gallery_info_file_hash(galleryinfo_params)
        is_insert = is_thesame is False
        if is_insert:
            self.logger.debug(
                f"Inserting gallery '{galleryinfo_params.gallery_name}'..."
            )
            self.delete_gallery_file(galleryinfo_params.gallery_name)
            self.delete_gallery(galleryinfo_params.gallery_name)
            self._insert_gallery_info(galleryinfo_params)
            self.logger.debug(f"Gallery '{galleryinfo_params.gallery_name}' inserted.")
        return is_insert

    def compress_gallery_to_cbz(
        self, gallery_folder: str, exclude_hashs: list[bytes]
    ) -> bool:
        from .compress_gallery_to_cbz import (
            compress_images_and_create_cbz,
            calculate_hash_of_file_in_cbz,
        )

        galleryinfo_params = parse_galleryinfo(gallery_folder)
        match self.config.h2h.cbz_grouping:
            case "date-yyyy":
                upload_time = self.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = str(upload_time.year).rjust(4, "0")
            case "date-yyyy-mm":
                upload_time = self.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                )
            case "date-yyyy-mm-dd":
                upload_time = self.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                    str(upload_time.day).rjust(2, "0"),
                )
            case "flat":
                relative_cbz_directory = ""
            case _:
                raise ValueError(
                    f"Invalid cbz_grouping value: {self.config.h2h.cbz_grouping}"
                )
        cbz_directory = os.path.join(self.config.h2h.cbz_path, relative_cbz_directory)
        cbz_log_directory = os.path.join("cbz_path", relative_cbz_directory)
        cbz_tmp_directory = os.path.join(self.config.h2h.cbz_path, "tmp")

        cbz_log_path = os.path.join(
            cbz_log_directory, galleryinfo_params.gallery_name + ".cbz"
        )

        def gallery_name2cbz_file_name(gallery_name: str) -> str:
            while (len(gallery_name.encode("utf-8")) + 4) > FILE_NAME_LENGTH_LIMIT:
                gallery_name = gallery_name[1:]
            return gallery_name + ".cbz"

        cbz_path = os.path.join(
            cbz_directory, gallery_name2cbz_file_name(galleryinfo_params.gallery_name)
        )
        if os.path.exists(cbz_path):
            db_gallery_id = self._get_db_gallery_id_by_gallery_name(
                galleryinfo_params.gallery_name
            )
            gallery_info_file_id = self._get_db_file_id(
                db_gallery_id, GALLERY_INFO_FILE_NAME
            )
            original_hash_value = self.get_hash_value_by_file_id(
                gallery_info_file_id, COMPARISON_HASH_ALGORITHM
            )
            cbz_hash_value = calculate_hash_of_file_in_cbz(
                cbz_path, GALLERY_INFO_FILE_NAME, COMPARISON_HASH_ALGORITHM
            )
            if original_hash_value != cbz_hash_value:
                compress_images_and_create_cbz(
                    gallery_folder,
                    cbz_directory,
                    cbz_tmp_directory,
                    self.config.h2h.cbz_max_size,
                    exclude_hashs,
                )
                result = True
            else:
                result = False
        else:
            compress_images_and_create_cbz(
                gallery_folder,
                cbz_directory,
                cbz_tmp_directory,
                self.config.h2h.cbz_max_size,
                exclude_hashs,
            )
            result = True
        return result

    def scan_current_galleries_folders(self) -> tuple[list[str], list[str]]:
        self.delete_pending_gallery_removals()

        with self.SQLConnector() as connector:
            tmp_table_name = "tmp_current_galleries"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_gallery_name_based_on_limit(column_name)
                    )
                    query = f"""
                        CREATE TEMPORARY TABLE IF NOT EXISTS {tmp_table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql}
                        )
                    """

            connector.execute(query)
            self.logger.info(f"{tmp_table_name} table created.")

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    insert_query = f"""
                        INSERT INTO {tmp_table_name}
                            ({", ".join(column_name_parts)})
                        VALUES ({", ".join(["%s" for _ in column_name_parts])})
                    """

            data = list[tuple]()
            current_galleries_folders = list[str]()
            current_galleries_names = list[str]()
            for root, _, files in os.walk(self.config.h2h.download_path):
                if GALLERY_INFO_FILE_NAME in files:
                    current_galleries_folders.append(root)
                    gallery_name = os.path.basename(current_galleries_folders[-1])
                    current_galleries_names.append(gallery_name)
                    gallery_name_parts = self._split_gallery_name(gallery_name)
                    data.append(tuple(gallery_name_parts))
            group_size = 5000
            it = iter(data)
            for _ in range(0, len(data), group_size):
                connector.execute_many(insert_query, list(islice(it, group_size)))

            match self.config.database.sql_type.lower():
                case "mysql":
                    fetch_query = f"""
                        SELECT CONCAT({",".join(["galleries_dbids."+column_name for column_name in column_name_parts])})
                        FROM galleries_dbids
                        LEFT JOIN {tmp_table_name} USING ({",".join(column_name_parts)})
                        WHERE {tmp_table_name}.{column_name_parts[0]} IS NULL
                    """
            removed_galleries = connector.fetch_all(fetch_query)
            if len(removed_galleries) > 0:
                removed_galleries = [gallery[0] for gallery in removed_galleries]

        for removed_gallery in removed_galleries:
            self.insert_pending_gallery_removal(removed_gallery)

        self.delete_pending_gallery_removals()

        return (current_galleries_folders, current_galleries_names)

    def _refresh_current_cbz_files(self, current_galleries_names: list[str]) -> None:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        current_cbzs = dict[str, str]()
        for root, _, files in os.walk(self.config.h2h.cbz_path):
            for file in files:
                current_cbzs[file] = root
        for key in set(current_cbzs.keys()) - set(
            gallery_name_to_cbz_file_name(name) for name in current_galleries_names
        ):
            os.remove(os.path.join(current_cbzs[key], key))
            self.logger.info(f"CBZ '{key}' removed.")
        self.logger.info("CBZ files refreshed.")

        while True:
            directory_removed = False
            for root, dirs, files in os.walk(self.config.h2h.cbz_path, topdown=False):
                if root == self.config.h2h.cbz_path:
                    continue
                if max([len(dirs), len(files)]) == 0:
                    directory_removed = True
                    os.rmdir(root)
                    self.logger.info(f"Directory '{root}' removed.")
            if not directory_removed:
                break
        self.logger.info("Empty directories removed.")

    def _refresh_current_files_hashs(self, algorithm: str) -> None:
        if algorithm not in HASH_ALGORITHMS:
            raise ValueError(
                f"Invalid hash algorithm: {algorithm} not in {HASH_ALGORITHMS}"
            )

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    get_delete_db_hash_id_query = (
                        lambda x, y: f"""
                        DELETE FROM {y}
                        WHERE db_hash_id IN (
                                SELECT db_hash_id
                                FROM {x}
                                RIGHT JOIN {y} USING (db_hash_id)
                                WHERE {x}.db_hash_id IS NULL
                            )
                        """
                    )
            hash_table_name = f"files_hashs_{algorithm.lower()}"
            db_table_name = f"files_hashs_{algorithm.lower()}_dbids"
            connector.execute(
                get_delete_db_hash_id_query(hash_table_name, db_table_name)
            )

    def refresh_current_files_hashs(self):
        algorithmlist = list(HASH_ALGORITHMS.keys())
        with SQLThreadsList() as threads:
            for algorithm in algorithmlist:
                threads.append(
                    target=self._refresh_current_files_hashs,
                    args=(algorithm,),
                )

    def insert_h2h_download(self) -> None:
        self.delete_pending_gallery_removals()

        current_galleries_folders, current_galleries_names = (
            self.scan_current_galleries_folders()
        )

        self._refresh_current_cbz_files(current_galleries_names)

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
        exclude_hashs = list[bytes]()
        previously_count_duplicated_files = 0
        self.logger.info("Excluded hash values obtained.")

        def calculate_exclude_hashs(
            previously_count_duplicated_files: int, exclude_hashs: list[bytes]
        ) -> tuple[int, list[bytes]]:
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
            is_insert_list = run_in_parallel(
                self.insert_gallery_info,
                [(x,) for x in gallery_chunk],
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
                is_new_list = run_in_parallel(
                    self.compress_gallery_to_cbz,
                    [(x, exclude_hashs) for x in gallery_chunk],
                )
                if any(is_new_list):
                    self.logger.info("There are new CBZ files created.")
                    total_created_cbz += sum(is_new_list)
        self.logger.info(
            f"Total galleries inserted in database: {total_inserted_in_database}"
        )
        self.logger.info(f"Total CBZ files created: {total_created_cbz}")

        self.logger.info("Cleaning up database...")
        self.refresh_current_files_hashs()

        if is_insert_limit_reached:
            self.logger.info("Sleeping for 30 minutes...")
            sleep(1800)
            self.logger.info("Refreshing database...")
            return self.insert_h2h_download()

        self._reset_redownload_times()

    def get_komga_metadata(self, gallery_name: str) -> dict:
        metadata = dict[str, str | list[dict[str, str]]]()
        metadata["title"] = self.get_title_by_gallery_name(gallery_name)
        if self._check_gallery_comment_by_gallery_name(gallery_name):
            metadata["summary"] = self.get_comment_by_gallery_name(gallery_name)
        else:
            metadata["summary"] = ""
        upload_time = self.get_upload_time_by_gallery_name(gallery_name)
        metadata["releaseDate"] = "-".join(
            [
                str(upload_time.year),
                "{m:02d}".format(m=upload_time.month),
                "{d:02d}".format(d=upload_time.day),
            ]
        )
        tags = self.get_tag_pairs_by_gallery_name(gallery_name)
        metadata["authors"] = [
            {"name": value, "role": key} for key, value in tags if value != ""
        ]
        return metadata
