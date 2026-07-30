from dataclasses import dataclass
from uuid import uuid4

from h2h_galleryinfo_parser import GalleryURLParser

from .repository import BaseRepository
from .sql_connector import SQLConnector


@dataclass(frozen=True, slots=True)
class DownloadRequest:
    gid: int
    url: str
    token: str


class H2HDBToDownloadQueue(BaseRepository):
    def _create_pending_download_gids_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = """
                        CREATE VIEW IF NOT EXISTS pending_download_gids AS
                            SELECT gids.gid AS gid
                            FROM (SELECT *
                                FROM galleries_redownload_times AS grt0
                                WHERE grt0.time <= DATE_SUB(NOW(), INTERVAL 7 DAY)
                                )
                                AS grt
                            INNER JOIN galleries_download_times AS gdt
                                on grt.db_gallery_id = gdt.db_gallery_id
                            INNER JOIN galleries_upload_times AS gut
                                ON grt.db_gallery_id = gut.db_gallery_id
                            INNER JOIN galleries_gids AS gids
                                ON grt.db_gallery_id = gids.db_gallery_id
                            WHERE (
                                (
                                    grt.time <= DATE_ADD(gut.time, INTERVAL 1 YEAR)
                                    AND DATE_ADD(gut.time, INTERVAL 7 DAY) <= NOW()
                                )
                                OR DATE_ADD(gdt.time, INTERVAL 7 DAY) <= grt.time
                            )
                                AND NOT EXISTS (
                                    SELECT 1 FROM removed_galleries_gids
                                    WHERE removed_galleries_gids.gid = gids.gid
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM todelete_gids
                                    WHERE todelete_gids.gid = gids.gid
                                )
                                AND NOT EXISTS (
                                    SELECT 1
                                    FROM todelete_galleries
                                        JOIN galleries_gids AS deletion_gids
                                            USING (db_gallery_id)
                                    WHERE deletion_gids.gid = gids.gid
                                )
                            ORDER BY gut.`time` DESC
                    """
                case "sqlite":
                    query = """
                        CREATE VIEW IF NOT EXISTS pending_download_gids AS
                            SELECT gids.gid AS gid
                            FROM (SELECT *
                                FROM galleries_redownload_times AS grt0
                                WHERE grt0.time <= datetime('now', '-7 days')
                                )
                                AS grt
                            INNER JOIN galleries_download_times AS gdt
                                on grt.db_gallery_id = gdt.db_gallery_id
                            INNER JOIN galleries_upload_times AS gut
                                ON grt.db_gallery_id = gut.db_gallery_id
                            INNER JOIN galleries_gids AS gids
                                ON grt.db_gallery_id = gids.db_gallery_id
                            WHERE (
                                (
                                    grt.time <= datetime(gut.time, '+1 years')
                                    AND datetime(gut.time, '+7 days') <= datetime('now')
                                )
                                OR datetime(gdt.time, '+7 days') <= grt.time
                            )
                                AND NOT EXISTS (
                                    SELECT 1 FROM removed_galleries_gids
                                    WHERE removed_galleries_gids.gid = gids.gid
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM todelete_gids
                                    WHERE todelete_gids.gid = gids.gid
                                )
                                AND NOT EXISTS (
                                    SELECT 1
                                    FROM todelete_galleries
                                        JOIN galleries_gids AS deletion_gids
                                            USING (db_gallery_id)
                                    WHERE deletion_gids.gid = gids.gid
                                )
                            ORDER BY gut.time DESC
                    """
            connector.execute(query)
        self.logger.debug("Ensured database view exists: name=pending_download_gids.")

    def get_pending_download_gids(self) -> list[int]:
        with self.SQLConnector() as connector:
            query = """
                SELECT gid
                FROM pending_download_gids
            """
            query_result = connector.fetch_all(query)
            pending_download_gids = [query[0] for query in query_result]
        return pending_download_gids

    def _create_todownload_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            gid          INT UNSIGNED NOT NULL,
                            url          CHAR({self.mariadb_index_prefix_limit}) NOT NULL,
                            request_token CHAR(32) NOT NULL,
                            UNIQUE INDEX (request_token)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            gid INTEGER NOT NULL PRIMARY KEY,
                            url TEXT NOT NULL,
                            request_token TEXT NOT NULL UNIQUE
                        )
                    """
            connector.execute(query)
        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def get_download_request(self, gid: int) -> DownloadRequest | None:
        with self.SQLConnector() as connector:
            row = connector.fetch_one(
                """
                SELECT gid, url, request_token
                FROM todownload_gids
                WHERE gid = %s
                """,
                (gid,),
            )
        if not row:
            return None
        return DownloadRequest(int(row[0]), str(row[1]), str(row[2]))

    @staticmethod
    def _normalized_gid(gid: int, url: str) -> int:
        if url:
            gallery = GalleryURLParser(url)
            if gallery.gid != gid and gid != 0:
                raise ValueError(
                    f"Gallery GID {gid} does not match URL GID {gallery.gid}."
                )
            return gallery.gid
        elif gid <= 0:
            raise ValueError("Gallery GID must be greater than zero.")
        return gid

    def request_download(self, gid: int, url: str = "") -> DownloadRequest:
        with self.SQLConnector() as connector:
            with connector.transaction():
                return self._request_download_with_connector(connector, gid, url)

    def _request_download_with_connector(
        self, connector: SQLConnector, gid: int, url: str = ""
    ) -> DownloadRequest:
        gid = self._normalized_gid(gid, url)
        request = DownloadRequest(gid, url, uuid4().hex)
        self._upsert_download_request(connector, request)
        row = connector.fetch_one(
            """
            SELECT gid, url, request_token
            FROM todownload_gids
            WHERE gid = %s
            """,
            (gid,),
        )
        if not row:
            raise RuntimeError(
                f"Download request for gallery GID {gid} disappeared after upsert."
            )
        return DownloadRequest(int(row[0]), str(row[1]), str(row[2]))

    def _upsert_download_request(
        self, connector: SQLConnector, request: DownloadRequest
    ) -> None:
        match self.config.database.sql_type.lower():
            case "mariadb":
                query = """
                    INSERT INTO todownload_gids (gid, url, request_token)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        url = IF(VALUES(url) = '', url, VALUES(url)),
                        request_token = VALUES(request_token)
                """
            case "sqlite":
                query = """
                    INSERT INTO todownload_gids (gid, url, request_token)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(gid) DO UPDATE SET
                        url = CASE
                            WHEN excluded.url = '' THEN todownload_gids.url
                            ELSE excluded.url
                        END,
                        request_token = excluded.request_token
                """
        connector.execute(query, (request.gid, request.url, request.token))

    def complete_download_request(self, request: DownloadRequest) -> None:
        with self.SQLConnector() as connector:
            connector.execute(
                """
                DELETE FROM todownload_gids
                WHERE gid = %s AND request_token = %s
                """,
                (request.gid, request.token),
            )

    def get_download_requests(self) -> list[DownloadRequest]:
        with self.SQLConnector() as connector:
            query_result = connector.fetch_all("""
                SELECT gid, url, request_token
                FROM todownload_gids
                ORDER BY gid
                """)
        return [
            DownloadRequest(int(gid), str(url), str(request_token))
            for gid, url, request_token in query_result
        ]
