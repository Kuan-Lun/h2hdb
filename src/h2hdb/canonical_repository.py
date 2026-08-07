from dataclasses import dataclass
from datetime import UTC, datetime

from .domain import GallerySourceFile, GallerySourceRecord, GalleryTag
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector

MARIADB_INDEX_PART_LENGTH = 191


@dataclass(frozen=True, slots=True)
class CanonicalSnapshotDiff:
    unchanged: int
    new: int
    changed: int
    removed: int
    removed_gids: tuple[int, ...]
    redownload_gids: tuple[int, ...]


class CanonicalSnapshotRepository(BaseRepository):
    """Persist ingest-owned source facts in the canonical source schema."""

    def __init__(self, context: RepositoryContext) -> None:
        super().__init__(context)

    def _sync_snapshot_with_connector(
        self,
        connector: SQLConnector,
        galleries: tuple[GallerySourceRecord, ...],
    ) -> CanonicalSnapshotDiff:
        desired_by_name = {gallery.gallery_name: gallery for gallery in galleries}
        existing_gids = {
            int(row[0])
            for row in connector.fetch_all("SELECT DISTINCT gid FROM galleries_gids")
        }
        desired_gids = {gallery.gid for gallery in galleries}
        requested_deletion_gids = {
            int(row[0]) for row in connector.fetch_all("SELECT gid FROM todelete_gids")
        }
        removed_gids = tuple(sorted(existing_gids - desired_gids))
        redownload_gids = tuple(
            gid for gid in removed_gids if gid not in requested_deletion_gids
        )
        existing_rows = connector.fetch_all("""
            SELECT
                names.full_name,
                parents.db_gallery_id,
                manifests.sha256
            FROM galleries_dbids AS parents
            JOIN galleries_names AS names
                ON names.db_gallery_id = parents.db_gallery_id
            JOIN gallery_source_manifests AS manifests
                ON manifests.db_gallery_id = parents.db_gallery_id
            """)
        existing_by_name = {
            str(name): (
                int(db_gallery_id),
                bytes(manifest),
            )
            for name, db_gallery_id, manifest in existing_rows
        }

        unchanged_names = {
            name
            for name, gallery in desired_by_name.items()
            if name in existing_by_name
            and existing_by_name[name][1]
            == bytes.fromhex(gallery.source_manifest_sha256)
        }
        new_names = set(desired_by_name).difference(existing_by_name)
        changed_existing_names = (
            set(desired_by_name).intersection(existing_by_name)
        ).difference(unchanged_names)
        removed_names = set(existing_by_name).difference(desired_by_name)
        removed_ids = [existing_by_name[name][0] for name in sorted(removed_names)]
        self._delete_galleries(connector, removed_ids)

        changed_existing_ids = [
            existing_by_name[name][0] for name in sorted(changed_existing_names)
        ]
        self._delete_gallery_facts(connector, changed_existing_ids)

        db_gallery_id_by_name = {
            name: existing_by_name[name][0]
            for name in unchanged_names | changed_existing_names
        }
        for name in sorted(changed_existing_names):
            self._insert_gallery_facts(
                connector,
                db_gallery_id_by_name[name],
                desired_by_name[name],
            )
        for name in sorted(new_names):
            db_gallery_id_by_name[name] = self._insert_gallery(
                connector,
                desired_by_name[name],
            )

        self._delete_orphan_hashes(connector)
        self._consume_orphaned_deletion_requests(connector)
        self._rebuild_content_ownership(
            connector,
            galleries,
            db_gallery_id_by_name,
        )
        self._refresh_deletion_candidates(connector)
        return CanonicalSnapshotDiff(
            unchanged=len(unchanged_names),
            new=len(new_names),
            changed=len(changed_existing_names),
            removed=len(removed_names),
            removed_gids=removed_gids,
            redownload_gids=redownload_gids,
        )

    @staticmethod
    def _delete_galleries(connector: SQLConnector, db_gallery_ids: list[int]) -> None:
        if not db_gallery_ids:
            return
        placeholders = ", ".join("%s" for _ in db_gallery_ids)
        connector.execute(
            f"DELETE FROM galleries_dbids WHERE db_gallery_id IN ({placeholders})",
            tuple(db_gallery_ids),
        )

    @staticmethod
    def _delete_gallery_facts(
        connector: SQLConnector,
        db_gallery_ids: list[int],
    ) -> None:
        """Clear replaceable source facts without changing gallery identity."""
        if not db_gallery_ids:
            return
        placeholders = ", ".join("%s" for _ in db_gallery_ids)
        parameters = tuple(db_gallery_ids)
        for table_name in (
            "gallery_duplicate_warnings",
            "gallery_content_hashes",
            "gallery_source_manifests",
            "galleries_tags",
            "files_dbids",
            "galleries_modified_times",
            "galleries_upload_times",
            "galleries_download_times",
            "galleries_comments",
            "galleries_upload_accounts",
            "galleries_titles",
            "galleries_gids",
            "galleries_names",
        ):
            connector.execute(
                f"DELETE FROM {table_name} WHERE db_gallery_id IN ({placeholders})",
                parameters,
            )

    def _insert_gallery(
        self,
        connector: SQLConnector,
        gallery: GallerySourceRecord,
    ) -> int:
        name_parts = self._name_parts(gallery.gallery_name)
        if self._context.sql_type == "mariadb":
            connector.execute(
                """
                INSERT INTO galleries_dbids (name_part1, name_part2)
                VALUES (%s, %s)
                """,
                name_parts,
            )
            id_row = connector.fetch_one(
                """
                SELECT db_gallery_id
                FROM galleries_dbids
                WHERE name_part1 = %s AND name_part2 = %s
                """,
                name_parts,
            )
        else:
            connector.execute(
                "INSERT INTO galleries_dbids (name) VALUES (%s)",
                (gallery.gallery_name,),
            )
            id_row = connector.fetch_one(
                "SELECT db_gallery_id FROM galleries_dbids WHERE name = %s",
                (gallery.gallery_name,),
            )
        if not id_row:
            raise RuntimeError(
                f"Gallery {gallery.gallery_name!r} disappeared after insertion"
            )
        db_gallery_id = int(id_row[0])

        self._insert_gallery_facts(connector, db_gallery_id, gallery)
        return db_gallery_id

    def _insert_gallery_facts(
        self,
        connector: SQLConnector,
        db_gallery_id: int,
        gallery: GallerySourceRecord,
    ) -> None:

        connector.execute(
            "INSERT INTO galleries_names (db_gallery_id, full_name) VALUES (%s, %s)",
            (db_gallery_id, gallery.gallery_name),
        )
        connector.execute(
            "INSERT INTO galleries_gids (db_gallery_id, gid) VALUES (%s, %s)",
            (db_gallery_id, gallery.gid),
        )
        connector.execute(
            "INSERT INTO galleries_titles (db_gallery_id, title) VALUES (%s, %s)",
            (db_gallery_id, gallery.title),
        )
        connector.execute(
            """
            INSERT INTO galleries_upload_accounts (db_gallery_id, account)
            VALUES (%s, %s)
            """,
            (db_gallery_id, gallery.upload_account),
        )
        if gallery.comment:
            connector.execute(
                """
                INSERT INTO galleries_comments (db_gallery_id, comment)
                VALUES (%s, %s)
                """,
                (db_gallery_id, gallery.comment),
            )

        download_time = self._database_datetime(gallery.download_time)
        for table_name, value in (
            ("galleries_download_times", download_time),
            ("galleries_upload_times", self._database_datetime(gallery.upload_time)),
            (
                "galleries_modified_times",
                self._database_datetime(gallery.modified_time),
            ),
        ):
            connector.execute(
                f"INSERT INTO {table_name} (db_gallery_id, time) VALUES (%s, %s)",
                (db_gallery_id, value),
            )
        for table_name in (
            "galleries_redownload_times",
            "galleries_access_times",
        ):
            self._merge_operational_time(
                connector,
                table_name,
                db_gallery_id,
                download_time,
            )

        self._insert_tags(connector, db_gallery_id, gallery.tags)
        self._insert_files(connector, db_gallery_id, gallery.files)
        connector.execute(
            """
            INSERT INTO gallery_source_manifests (db_gallery_id, sha256)
            VALUES (%s, %s)
            """,
            (db_gallery_id, bytes.fromhex(gallery.source_manifest_sha256)),
        )

    @staticmethod
    def _merge_operational_time(
        connector: SQLConnector,
        table_name: str,
        db_gallery_id: int,
        download_time: datetime,
    ) -> None:
        """Advance mutable runtime state monotonically or initialize it if absent."""

        connector.execute(
            f"""
            UPDATE {table_name}
            SET time = CASE WHEN time < %s THEN %s ELSE time END
            WHERE db_gallery_id = %s
            """,
            (download_time, download_time, db_gallery_id),
        )
        connector.execute(
            f"""
            INSERT INTO {table_name} (db_gallery_id, time)
            SELECT %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} WHERE db_gallery_id = %s
            )
            """,
            (db_gallery_id, download_time, db_gallery_id),
        )

    def _insert_tags(
        self,
        connector: SQLConnector,
        db_gallery_id: int,
        tags: tuple[GalleryTag, ...],
    ) -> None:
        for tag in tags:
            if self._context.sql_type == "mariadb":
                connector.execute(
                    "INSERT IGNORE INTO galleries_tags_names (tag_name) VALUES (%s)",
                    (tag.name,),
                )
                connector.execute(
                    """
                    INSERT IGNORE INTO galleries_tags_values (tag_value) VALUES (%s)
                    """,
                    (tag.value,),
                )
                connector.execute(
                    """
                    INSERT IGNORE INTO galleries_tag_pairs_dbids (
                        tag_name,
                        tag_value
                    ) VALUES (%s, %s)
                    """,
                    (tag.name, tag.value),
                )
            else:
                connector.execute(
                    """
                    INSERT INTO galleries_tags_names (tag_name) VALUES (%s)
                    ON CONFLICT(tag_name) DO NOTHING
                    """,
                    (tag.name,),
                )
                connector.execute(
                    """
                    INSERT INTO galleries_tags_values (tag_value) VALUES (%s)
                    ON CONFLICT(tag_value) DO NOTHING
                    """,
                    (tag.value,),
                )
                connector.execute(
                    """
                    INSERT INTO galleries_tag_pairs_dbids (tag_name, tag_value)
                    VALUES (%s, %s)
                    ON CONFLICT(tag_name, tag_value) DO NOTHING
                    """,
                    (tag.name, tag.value),
                )
            pair_row = connector.fetch_one(
                """
                SELECT db_tag_pair_id
                FROM galleries_tag_pairs_dbids
                WHERE tag_name = %s AND tag_value = %s
                """,
                (tag.name, tag.value),
            )
            if not pair_row:
                raise RuntimeError(f"Gallery tag {(tag.name, tag.value)!r} is missing")
            connector.execute(
                """
                INSERT INTO galleries_tags (db_gallery_id, db_tag_pair_id)
                VALUES (%s, %s)
                """,
                (db_gallery_id, int(pair_row[0])),
            )

    def _insert_files(
        self,
        connector: SQLConnector,
        db_gallery_id: int,
        files: tuple[GallerySourceFile, ...],
    ) -> None:
        for source_file in files:
            name_parts = self._name_parts(source_file.name)
            if self._context.sql_type == "mariadb":
                connector.execute(
                    """
                    INSERT INTO files_dbids (
                        db_gallery_id,
                        name_part1,
                        name_part2
                    ) VALUES (%s, %s, %s)
                    """,
                    (db_gallery_id, *name_parts),
                )
                file_row = connector.fetch_one(
                    """
                    SELECT db_file_id
                    FROM files_dbids
                    WHERE db_gallery_id = %s
                        AND name_part1 = %s
                        AND name_part2 = %s
                    """,
                    (db_gallery_id, *name_parts),
                )
            else:
                connector.execute(
                    """
                    INSERT INTO files_dbids (db_gallery_id, name)
                    VALUES (%s, %s)
                    """,
                    (db_gallery_id, source_file.name),
                )
                file_row = connector.fetch_one(
                    """
                    SELECT db_file_id
                    FROM files_dbids
                    WHERE db_gallery_id = %s AND name = %s
                    """,
                    (db_gallery_id, source_file.name),
                )
            if not file_row:
                raise RuntimeError(
                    f"Source file {source_file.name!r} disappeared after insertion"
                )
            db_file_id = int(file_row[0])
            connector.execute(
                "INSERT INTO files_names (db_file_id, full_name) VALUES (%s, %s)",
                (db_file_id, source_file.name),
            )
            digest = bytes.fromhex(source_file.sha256)
            if self._context.sql_type == "mariadb":
                connector.execute(
                    """
                    INSERT IGNORE INTO files_hashs_sha256_dbids (hash_value)
                    VALUES (%s)
                    """,
                    (digest,),
                )
            else:
                connector.execute(
                    """
                    INSERT INTO files_hashs_sha256_dbids (hash_value)
                    VALUES (%s)
                    ON CONFLICT(hash_value) DO NOTHING
                    """,
                    (digest,),
                )
            hash_row = connector.fetch_one(
                """
                SELECT db_hash_id
                FROM files_hashs_sha256_dbids
                WHERE hash_value = %s
                """,
                (digest,),
            )
            if not hash_row:
                raise RuntimeError(
                    f"Source file hash {source_file.sha256} disappeared after insertion"
                )
            connector.execute(
                """
                INSERT INTO files_hashs_sha256 (db_file_id, db_hash_id)
                VALUES (%s, %s)
                """,
                (db_file_id, int(hash_row[0])),
            )

    @staticmethod
    def _delete_orphan_hashes(connector: SQLConnector) -> None:
        connector.execute("""
            DELETE FROM files_hashs_sha256_dbids
            WHERE NOT EXISTS (
                SELECT 1
                FROM files_hashs_sha256
                WHERE files_hashs_sha256.db_hash_id =
                    files_hashs_sha256_dbids.db_hash_id
            )
            """)

    @staticmethod
    def _consume_orphaned_deletion_requests(connector: SQLConnector) -> None:
        """Consume deletion intent once no canonical source has the GID.

        A crash can leave the marker behind after the filesystem source and
        its canonical row are already gone. A complete snapshot is the
        reconciliation boundary, so it also consumes those pre-existing
        orphan markers instead of requiring the GID to disappear in this
        particular transaction.
        """

        connector.execute(
            """
            DELETE FROM todelete_gids
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM galleries_gids
                    WHERE galleries_gids.gid = todelete_gids.gid
                )
            """,
        )

    @staticmethod
    def _rebuild_content_ownership(
        connector: SQLConnector,
        galleries: tuple[GallerySourceRecord, ...],
        db_gallery_id_by_name: dict[str, int],
    ) -> None:
        connector.execute("DELETE FROM gallery_duplicate_warnings")
        connector.execute("DELETE FROM gallery_content_hashes")
        owner_rows = [
            (
                db_gallery_id_by_name[gallery.gallery_name],
                bytes.fromhex(gallery.content_sha256),
            )
            for gallery in galleries
            if gallery.content_sha256 is not None
            and gallery.duplicate_of_gallery_name is None
        ]
        if owner_rows:
            connector.execute_many(
                """
                INSERT INTO gallery_content_hashes (db_gallery_id, sha256)
                VALUES (%s, %s)
                """,
                owner_rows,
            )
        duplicate_rows = [
            (
                db_gallery_id_by_name[gallery.gallery_name],
                db_gallery_id_by_name[gallery.duplicate_of_gallery_name],
            )
            for gallery in galleries
            if gallery.duplicate_of_gallery_name is not None
        ]
        if duplicate_rows:
            connector.execute_many(
                """
                INSERT INTO gallery_duplicate_warnings (
                    db_gallery_id,
                    duplicate_of_db_gallery_id
                ) VALUES (%s, %s)
                """,
                duplicate_rows,
            )

    @staticmethod
    def _refresh_deletion_candidates(connector: SQLConnector) -> None:
        connector.execute("DELETE FROM todelete_galleries")
        connector.execute("""
            INSERT INTO todelete_galleries (db_gallery_id)
            SELECT db_gallery_id FROM todelete_gallery_candidates
            """)

    @staticmethod
    def _name_parts(value: str) -> tuple[str, str]:
        return (
            value[:MARIADB_INDEX_PART_LENGTH],
            value[MARIADB_INDEX_PART_LENGTH:],
        )

    @staticmethod
    def _database_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
