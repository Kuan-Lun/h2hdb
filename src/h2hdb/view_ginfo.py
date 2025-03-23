from .table_uploadaccounts import H2HDBUploadAccounts
from .table_titles import H2HDBGalleriesTitles
from .table_times import H2HDBTimes
from .table_gids import H2HDBGalleriesIDs, H2HDBGalleriesGIDs
from .table_database_setting import H2HDBCheckDatabaseSettings


class H2HDBGalleriesInfos(
    H2HDBGalleriesTitles,
    H2HDBUploadAccounts,
    H2HDBTimes,
    H2HDBGalleriesGIDs,
    H2HDBGalleriesIDs,
    H2HDBCheckDatabaseSettings,
):
    def _create_galleries_infos_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS galleries_infos AS
                        SELECT galleries_names.db_gallery_id AS db_gallery_id,
                            galleries_names.full_name AS name,
                            galleries_titles.title AS title,
                            galleries_gids.gid AS gid,
                            galleries_upload_accounts.account AS upload_account,
                            galleries_upload_times.time AS upload_time,
                            galleries_download_times.time AS download_time,
                            galleries_modified_times.time AS modified_time,
                            galleries_access_times.time AS access_time
                        FROM galleries_names
                            LEFT JOIN galleries_titles USING (db_gallery_id)
                            LEFT JOIN galleries_gids USING (db_gallery_id)
                            LEFT JOIN galleries_upload_accounts USING (db_gallery_id)
                            LEFT JOIN galleries_upload_times USING (db_gallery_id)
                            LEFT JOIN galleries_download_times USING (db_gallery_id)
                            LEFT JOIN galleries_modified_times USING (db_gallery_id)
                            LEFT JOIN galleries_access_times USING (db_gallery_id)
                    """
            connector.execute(query)
            self.logger.info("galleries_infos view created.")

    def _create_duplicate_hash_in_gallery_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS duplicate_hash_in_gallery AS WITH Files AS (
                            SELECT files_dbids.db_gallery_id AS db_gallery_id,
                                files_hashs_sha512.db_hash_id AS hash_value
                            FROM files_dbids
                                JOIN files_hashs_sha512 ON files_dbids.db_file_id = files_hashs_sha512.db_file_id
                        ),
                        DuplicateCount AS (
                            SELECT db_gallery_id,
                                hash_value
                            FROM Files
                            GROUP BY db_gallery_id,
                                hash_value
                            HAVING COUNT(*) > 1
                        ),
                        TotalCount AS (
                            SELECT db_gallery_id,
                                COUNT(*) AS files_count
                            FROM files_dbids
                            GROUP BY db_gallery_id
                        ),
                        DuplicateGroupCount AS (
                            SELECT db_gallery_id,
                                COUNT(*) AS duplicate_groups
                            FROM DuplicateCount
                            GROUP BY db_gallery_id
                        )
                        SELECT tc.db_gallery_id AS db_gallery_id,
                            gg.gid AS gid,
                            gn.full_name AS gallery_name
                        FROM TotalCount AS tc
                            JOIN DuplicateGroupCount AS dg ON tc.db_gallery_id = dg.db_gallery_id
                            JOIN galleries_gids AS gg ON tc.db_gallery_id = gg.db_gallery_id
                            JOIN galleries_names AS gn ON gg.db_gallery_id = gn.db_gallery_id
                        WHERE CAST(dg.duplicate_groups AS FLOAT) / (
                                tc.files_count - CAST(dg.duplicate_groups AS FLOAT)
                            ) > 0.9
                        ORDER BY gid DESC;
                    """
            connector.execute(query)
            self.logger.info("duplicate_hash_in_gallery view created.")
