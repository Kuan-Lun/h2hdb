from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list
from .table_gids import H2HDBGalleriesIDs

SOURCE_MANIFEST_BATCH_SIZE = 500


class H2HDBGallerySourceManifests(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_gallery_source_manifests_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_source_manifests"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            sha256        BINARY(32)   NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            sha256 BLOB NOT NULL
                        )
                    """
            connector.execute(query)
        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def _insert_many(self, manifests_by_db_gallery_id: dict[int, bytes]) -> None:
        for manifest in manifests_by_db_gallery_id.values():
            if len(manifest) != 32:
                raise ValueError(
                    f"Source manifest SHA-256 must be exactly 32 bytes, got {len(manifest)}."
                )
        self._insert_rows(
            "gallery_source_manifests",
            ["db_gallery_id", "sha256"],
            list(manifests_by_db_gallery_id.items()),
        )

    def _get_by_db_gallery_ids(self, db_gallery_ids: list[int]) -> dict[int, bytes]:
        if not db_gallery_ids:
            return {}

        manifests = dict[int, bytes]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(db_gallery_ids, SOURCE_MANIFEST_BATCH_SIZE):
                query = f"""
                    SELECT db_gallery_id, sha256
                    FROM gallery_source_manifests
                    WHERE db_gallery_id IN ({", ".join(["%s"] * len(batch))})
                """
                for db_gallery_id, manifest in connector.fetch_all(query, tuple(batch)):
                    manifests[int(db_gallery_id)] = bytes(manifest)
        return manifests

    def _delete_stale_rows(self) -> None:
        with self.SQLConnector() as connector:
            connector.execute("""
                DELETE FROM gallery_source_manifests
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM galleries_dbids
                    WHERE galleries_dbids.db_gallery_id =
                        gallery_source_manifests.db_gallery_id
                )
            """)
