from .repository import BaseRepository

TODELETE_GALLERY_CANDIDATES_SELECT = """
    SELECT live.db_gallery_id
    FROM galleries_dbids AS live
        JOIN galleries_gids ON galleries_gids.db_gallery_id = live.db_gallery_id
        JOIN todelete_gids ON todelete_gids.gid = galleries_gids.gid
    UNION
    SELECT live.db_gallery_id
    FROM galleries_dbids AS live
        JOIN galleries_infos
            ON galleries_infos.db_gallery_id = live.db_gallery_id
        JOIN (
            SELECT gid, MAX(download_time) AS max_download_time
            FROM galleries_infos
                JOIN galleries_dbids
                    ON galleries_dbids.db_gallery_id = galleries_infos.db_gallery_id
            GROUP BY gid
            HAVING COUNT(*) > 1
        ) AS duplicate_gid
            ON duplicate_gid.gid = galleries_infos.gid
    WHERE galleries_infos.download_time < duplicate_gid.max_download_time
    UNION
    SELECT live.db_gallery_id
    FROM galleries_dbids AS live
        JOIN duplicate_hash_in_gallery
            ON duplicate_hash_in_gallery.db_gallery_id = live.db_gallery_id
"""


class H2HDBToDeleteQueue(BaseRepository):
    def _create_todelete_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todelete_gids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            gid INT UNSIGNED NOT NULL PRIMARY KEY
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            gid INTEGER NOT NULL PRIMARY KEY
                        )
                    """
            connector.execute(query)
        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def _create_todelete_gallery_candidates_view(self) -> None:
        with self.SQLConnector() as connector:
            connector.execute(f"""
                CREATE VIEW IF NOT EXISTS todelete_gallery_candidates AS
                {TODELETE_GALLERY_CANDIDATES_SELECT}
                """)
        self.logger.debug(
            "Ensured database view exists: name=todelete_gallery_candidates."
        )

    def _create_todelete_galleries_table(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = """
                        CREATE TABLE IF NOT EXISTS todelete_galleries (
                            db_gallery_id INT UNSIGNED NOT NULL PRIMARY KEY,
                            FOREIGN KEY (db_gallery_id)
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE
                        )
                    """
                case "sqlite":
                    query = """
                        CREATE TABLE IF NOT EXISTS todelete_galleries (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE
                        )
                    """
            connector.execute(query)
        self.logger.debug("Ensured database table exists: name=todelete_galleries.")

    def refresh_todelete_galleries(self) -> int:
        with self.SQLConnector() as connector:
            with connector.transaction():
                connector.execute("DELETE FROM todelete_galleries")
                connector.execute("""
                    INSERT INTO todelete_galleries (db_gallery_id)
                    SELECT db_gallery_id
                    FROM todelete_gallery_candidates
                    """)
                row = connector.fetch_one("SELECT COUNT(*) FROM todelete_galleries")
        candidate_count = int(row[0])
        self.logger.info(
            f"Published gallery deletion candidates: galleries={candidate_count}."
        )
        return candidate_count

    def _create_todelete_rm_commands_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    # MariaDB needs a literal `\\` in the source to produce one
                    # backslash in the shell-quoted output.
                    query = r"""
                        CREATE VIEW IF NOT EXISTS todelete_rm_commands AS
                        SELECT CONCAT(
                            'rm -rf -- ''',
                            REPLACE(galleries_names.full_name, '''', '''\\'''''),
                            ''''
                        ) AS cmd
                        FROM todelete_galleries
                            JOIN galleries_names USING (db_gallery_id)
                    """
                case "sqlite":
                    query = r"""
                        CREATE VIEW IF NOT EXISTS todelete_rm_commands AS
                        SELECT 'rm -rf -- ''' ||
                            REPLACE(galleries_names.full_name, '''', '''\''''') ||
                            '''' AS cmd
                        FROM todelete_galleries
                            JOIN galleries_names USING (db_gallery_id)
                    """
            connector.execute(query)
        self.logger.debug("Ensured database view exists: name=todelete_rm_commands.")

    def is_gallery_deletion_requested(self, gid: int) -> bool:
        with self.SQLConnector() as connector:
            query_result = connector.fetch_one(
                "SELECT gid FROM todelete_gids WHERE gid = %s", (gid,)
            )
        return bool(query_result)

    def request_gallery_deletion(self, gid: int) -> None:
        if gid <= 0:
            raise ValueError("Gallery GID must be greater than zero.")
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = "INSERT IGNORE INTO todelete_gids (gid) VALUES (%s)"
                case "sqlite":
                    query = """
                        INSERT INTO todelete_gids (gid) VALUES (%s)
                        ON CONFLICT(gid) DO NOTHING
                    """
            connector.execute(query, (gid,))
