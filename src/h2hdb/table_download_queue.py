from h2h_galleryinfo_parser import GalleryURLParser

from .repository import BaseRepository


class H2HDBDownloadQueue(BaseRepository):
    def _create_pending_download_gids_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
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
                case "sqlite":
                    query = """
                        CREATE VIEW IF NOT EXISTS pending_download_gids AS
                            SELECT gids.gid AS gid
                            FROM (SELECT *
                                FROM galleries_redownload_times AS grt0
                                WHERE datetime(grt0.time, '+7 days') <= datetime('now')
                                )
                                AS grt
                            INNER JOIN galleries_download_times AS gdt
                                on grt.db_gallery_id = gdt.db_gallery_id
                            INNER JOIN galleries_upload_times AS gut
                                ON grt.db_gallery_id = gut.db_gallery_id
                            INNER JOIN galleries_gids AS gids
                                ON grt.db_gallery_id = gids.db_gallery_id
                            WHERE grt.time <= datetime(gut.time, '+1 years')
                                AND datetime(gut.time, '+7 days') <= datetime('now')
                                OR datetime(gdt.time, '+7 days') <= grt.time
                                 ORDER BY gut.time DESC
                    """
            connector.execute(query)
        self.logger.info("pending_download_gids view created.")

    def get_pending_download_gids(self) -> list[int]:
        with self.SQLConnector() as connector:
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
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            FOREIGN KEY (gid) REFERENCES galleries_gids(gid)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            gid          INT UNSIGNED NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            gid INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_gids(gid)
                                ON UPDATE CASCADE ON DELETE CASCADE
                        )
                    """
            connector.execute(query)
        self.logger.info(f"{table_name} table created.")

    def _create_todelete_names_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todelete_names"
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
            select_query = f"""
                SELECT gid
                FROM {table_name}
                WHERE gid = %s
            """
            query_result = connector.fetch_one(select_query, (gid,))
        return len(query_result) != 0

    def insert_todelete_gid(self, gid: int) -> None:
        if not self.check_todelete_gid(gid):
            with self.SQLConnector() as connector:
                table_name = "todelete_gids"
                insert_query = f"""
                    INSERT INTO {table_name} (gid) VALUES (%s)
                """
                connector.execute(insert_query, (gid,))

    def _create_todownload_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            gid          INT UNSIGNED NOT NULL,
                            url          CHAR({self.mariadb_index_prefix_limit}) NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            gid INTEGER NOT NULL PRIMARY KEY,
                            url TEXT NOT NULL
                        )
                    """
            connector.execute(query)
        self.logger.info(f"{table_name} table created.")

    def check_todownload_gid(self, gid: int, url: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
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
        return len(query_result) != 0

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
                    insert_query = f"""
                        INSERT INTO {table_name} (gid, url) VALUES (%s, %s)
                    """
                    connector.execute(insert_query, (gid, url))
            else:
                self.update_todownload_gid(gid, url)

    def update_todownload_gid(self, gid: int, url: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            update_query = f"""
                UPDATE {table_name} SET url = %s WHERE gid = %s
            """
            connector.execute(update_query, (url, gid))

    def remove_todownload_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            delete_query = f"""
                DELETE FROM {table_name} WHERE gid = %s
            """
            connector.execute(delete_query, (gid,))

    def get_todownload_gids(self) -> list[tuple[int, str]]:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            select_query = f"""
                SELECT gid, url
                FROM {table_name}
            """
            query_result = connector.fetch_all(select_query)
        todownload_gids = [(query[0], query[1]) for query in query_result]
        return todownload_gids
