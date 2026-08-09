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


_ACTIVE_TODELETE_GALLERY_LOCATORS_SELECT = """
    SELECT discovery.source_locator
    FROM catalog_source_revision AS source_revision
        JOIN catalog_operational_activations AS activation
            ON activation.build_id = source_revision.active_build_id
            AND activation.source_revision = source_revision.current_revision
        JOIN catalog_source_galleries AS source
            ON source.build_id = activation.build_id
        JOIN catalog_build_discoveries AS discovery
            ON discovery.build_id = source.build_id
            AND discovery.gallery_key = source.gallery_key
        LEFT JOIN catalog_build_content_digests AS digest
            ON digest.build_id = source.build_id
            AND digest.gallery_key = source.gallery_key
    WHERE source_revision.singleton_id = 1
        AND (
            EXISTS (
                SELECT 1
                FROM todelete_gids AS marker
                WHERE marker.gid = source.gid
                    AND NOT EXISTS (
                        SELECT 1
                        FROM catalog_build_deletion_consumptions AS consumption
                            JOIN catalog_operational_activations
                                AS consumed_activation
                                ON consumed_activation.build_id =
                                    consumption.build_id
                                AND consumed_activation.preparation_id =
                                    consumption.preparation_id
                        WHERE consumption.gid = marker.gid
                            AND consumption.deletion_request_token =
                                marker.request_token
                    )
            )
            OR EXISTS (
                SELECT 1
                FROM catalog_source_galleries AS newer
                WHERE newer.build_id = source.build_id
                    AND newer.gid = source.gid
                    AND newer.download_time_utc > source.download_time_utc
            )
            OR digest.duplicate_hash_deletion_candidate = 1
        )
    UNION ALL
    SELECT galleries_names.full_name AS source_locator
    FROM todelete_galleries
        JOIN galleries_names USING (db_gallery_id)
    WHERE NOT EXISTS (
        SELECT 1
        FROM catalog_source_revision AS fallback_source_revision
            JOIN catalog_operational_activations AS fallback_activation
                ON fallback_activation.build_id =
                    fallback_source_revision.active_build_id
                AND fallback_activation.source_revision =
                    fallback_source_revision.current_revision
        WHERE fallback_source_revision.singleton_id = 1
    )
"""


def _todelete_rm_commands_query(
    sql_type: str,
    *,
    active_authority: bool,
    if_not_exists: bool,
) -> str:
    locator_select = (
        _ACTIVE_TODELETE_GALLERY_LOCATORS_SELECT
        if active_authority
        else """
            SELECT galleries_names.full_name AS source_locator
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
        """
    )
    create_clause = "CREATE VIEW IF NOT EXISTS" if if_not_exists else "CREATE VIEW"
    match sql_type.lower():
        case "mariadb":
            # MariaDB needs a literal `\\` in the source to produce one
            # backslash in the shell-quoted output.
            return rf"""
                {create_clause} todelete_rm_commands AS
                SELECT CONCAT(
                    'rm -rf -- ''',
                    REPLACE(deletion_path.source_locator, '''', '''\\'''''),
                    ''''
                ) AS cmd
                FROM (
                    {locator_select}
                ) AS deletion_path
            """
        case "sqlite":
            return rf"""
                {create_clause} todelete_rm_commands AS
                SELECT 'rm -rf -- ''' ||
                    REPLACE(deletion_path.source_locator, '''', '''\''''') ||
                    '''' AS cmd
                FROM (
                    {locator_select}
                ) AS deletion_path
            """
        case _:
            raise AssertionError(f"Unsupported SQL type: {sql_type}")


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
            connector.execute(
                _todelete_rm_commands_query(
                    self.config.database.sql_type,
                    active_authority=False,
                    if_not_exists=True,
                )
            )
        self.logger.debug("Ensured database view exists: name=todelete_rm_commands.")

    def _replace_todelete_rm_commands_with_active_authority_view(self) -> None:
        """Install the active-source view, retaining a pre-activation fallback."""

        with self.SQLConnector() as connector:
            connector.execute("DROP VIEW IF EXISTS todelete_rm_commands")
            connector.execute(
                _todelete_rm_commands_query(
                    self.config.database.sql_type,
                    active_authority=True,
                    if_not_exists=False,
                )
            )
        self.logger.debug(
            "Replaced database view with active source authority: "
            "name=todelete_rm_commands."
        )

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

    def get_gallery_deletion_requests(self) -> list[int]:
        with self.SQLConnector() as connector:
            rows = connector.fetch_all("SELECT gid FROM todelete_gids ORDER BY gid")
        return [int(row[0]) for row in rows]
