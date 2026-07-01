from .repository import BaseRepository


class H2HDBDuplicatedHashes(BaseRepository):
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

    def _count_duplicated_files_hashs_sha512(self) -> int:
        with self.SQLConnector() as connector:
            table_name = "duplicated_files_hashs_sha512"
            query = f"""
                SELECT COUNT(*)
                FROM {table_name}
            """
            query_result = connector.fetch_one(query)
        return int(query_result[0])

    def _get_duplicated_hash_values_by_count_artist_ratio(self) -> set[bytes]:
        with self.SQLConnector() as connector:
            table_name = "duplicated_hash_values_by_count_artist_ratio"
            select_query = f"""
                SELECT hash_value
                FROM {table_name}
            """

            query_result = connector.fetch_all(select_query)
        return {query[0] for query in query_result}
