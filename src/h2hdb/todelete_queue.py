from .repository import BaseRepository


class H2HDBToDeleteQueue(BaseRepository):
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
                    UNION
                        SELECT gallery_name AS full_name
                        FROM duplicate_hash_in_gallery
            """
            connector.execute(query)
        self.logger.info(f"{table_name} table created.")

    def _create_todelete_rm_commands_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todelete_rm_commands"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    # Not an f-string with escaped backslashes: MariaDB needs a literal
                    # `\\` in the source to produce one backslash in the shell-quoted
                    # output, and a regular f-string would collapse it to one first.
                    query = rf"""
                        CREATE VIEW IF NOT EXISTS {table_name} AS
                        SELECT CONCAT(
                            'rm -rf -- ''',
                            REPLACE(full_name, '''', '''\\'''''),
                            ''''
                        ) AS cmd
                        FROM todelete_names
                    """
                case "sqlite":
                    query = rf"""
                        CREATE VIEW IF NOT EXISTS {table_name} AS
                        SELECT 'rm -rf -- ''' || REPLACE(full_name, '''', '''\''''') || '''' AS cmd
                        FROM todelete_names
                    """
            connector.execute(query)
        self.logger.info(f"{table_name} table created.")

    def _queue_redownload_for_todelete_names(self) -> None:
        with self.SQLConnector() as connector:
            query = """
                INSERT INTO todownload_gids (gid, url)
                SELECT galleries_infos.gid, ''
                FROM galleries_infos
                    INNER JOIN todelete_names ON galleries_infos.name = todelete_names.full_name
                WHERE NOT EXISTS (
                    SELECT 1 FROM todownload_gids WHERE todownload_gids.gid = galleries_infos.gid
                )
            """
            connector.execute(query)

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
