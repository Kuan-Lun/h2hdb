from .repository import BaseRepository


class H2HDBDuplicatedHashes(BaseRepository):
    def _count_duplicated_files_hashs_sha512(self) -> int:
        with self.SQLConnector() as connector:
            query = """
                SELECT COUNT(*) FROM (
                    SELECT db_hash_id
                    FROM files_hashs_sha512
                    GROUP BY db_hash_id
                    HAVING COUNT(*) >= 3
                ) duplicated_files_hashs_sha512
            """
            query_result = connector.fetch_one(query)
        return int(query_result[0])

    def _get_duplicated_hash_values_by_count_artist_ratio(self) -> set[bytes]:
        # Indexed temp tables (not CTEs) give the optimizer real cardinalities to pick
        # a sane join order; temp tables are connection-scoped, so this all has to run
        # through a single connector.
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    connector.execute("""
                        CREATE TEMPORARY TABLE tmp_duplicated_hash_ids (
                            db_hash_id INT UNSIGNED NOT NULL PRIMARY KEY
                        )
                    """)
                    connector.execute("""
                        CREATE TEMPORARY TABLE tmp_gallery_artist_counts (
                            db_gallery_id INT UNSIGNED NOT NULL PRIMARY KEY,
                            artist_count INT UNSIGNED NOT NULL
                        )
                    """)
                case "sqlite":
                    connector.execute("""
                        CREATE TEMPORARY TABLE tmp_duplicated_hash_ids (
                            db_hash_id INTEGER NOT NULL PRIMARY KEY
                        )
                    """)
                    connector.execute("""
                        CREATE TEMPORARY TABLE tmp_gallery_artist_counts (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY,
                            artist_count INTEGER NOT NULL
                        )
                    """)

            connector.execute("""
                INSERT INTO tmp_duplicated_hash_ids (db_hash_id)
                SELECT db_hash_id
                FROM files_hashs_sha512
                GROUP BY db_hash_id
                HAVING COUNT(*) >= 3
            """)

            connector.execute("""
                INSERT INTO tmp_gallery_artist_counts (db_gallery_id, artist_count)
                SELECT galleries_tags.db_gallery_id,
                    COUNT(DISTINCT galleries_tag_pairs_dbids.tag_value)
                FROM galleries_tags
                    INNER JOIN galleries_tag_pairs_dbids ON galleries_tags.db_tag_pair_id = galleries_tag_pairs_dbids.db_tag_pair_id
                WHERE galleries_tag_pairs_dbids.tag_name = 'artist'
                GROUP BY galleries_tags.db_gallery_id
            """)

            select_query = """
                SELECT files_hashs_sha512_dbids.hash_value AS hash_value
                FROM tmp_duplicated_hash_ids
                    INNER JOIN files_hashs_sha512 ON tmp_duplicated_hash_ids.db_hash_id = files_hashs_sha512.db_hash_id
                    INNER JOIN files_dbids ON files_hashs_sha512.db_file_id = files_dbids.db_file_id
                    INNER JOIN galleries_dbids ON files_dbids.db_gallery_id = galleries_dbids.db_gallery_id
                    INNER JOIN galleries_tags ON galleries_dbids.db_gallery_id = galleries_tags.db_gallery_id
                    INNER JOIN galleries_tag_pairs_dbids ON galleries_tags.db_tag_pair_id = galleries_tag_pairs_dbids.db_tag_pair_id
                    INNER JOIN tmp_gallery_artist_counts ON galleries_dbids.db_gallery_id = tmp_gallery_artist_counts.db_gallery_id
                    INNER JOIN files_hashs_sha512_dbids ON files_hashs_sha512.db_hash_id = files_hashs_sha512_dbids.db_hash_id
                WHERE galleries_tag_pairs_dbids.tag_name = 'artist'
                GROUP BY files_hashs_sha512.db_hash_id
                HAVING COUNT(DISTINCT galleries_tag_pairs_dbids.tag_value) / MAX(tmp_gallery_artist_counts.artist_count) > 2
            """
            query_result = connector.fetch_all(select_query)
        return {query[0] for query in query_result}
