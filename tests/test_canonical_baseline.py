import sqlite3
from collections.abc import Iterable
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path

from h2hdb import (
    H2HDB,
    CatalogPublicationSelection,
    CatalogSnapshot,
    CoreConfig,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
)


def _digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def _gallery(
    name: str,
    gid: int,
    *,
    version: str,
    title: str,
    comment: str,
    upload_account: str,
    timestamp: datetime,
    tags: tuple[GalleryTag, ...],
    files: tuple[str, ...],
    content_sha256: str,
    duplicate_of: str | None = None,
) -> GallerySourceRecord:
    return GallerySourceRecord(
        gallery_name=name,
        gid=gid,
        title=title,
        comment=comment,
        upload_account=upload_account,
        upload_time=timestamp - timedelta(days=2),
        download_time=timestamp,
        modified_time=timestamp + timedelta(hours=3),
        tags=tags,
        files=tuple(
            GallerySourceFile(
                name=file_name,
                size_bytes=position,
                sha256=_digest(f"file:{version}:{file_name}"),
            )
            for position, file_name in enumerate(files, start=1)
        ),
        source_manifest_sha256=_digest(f"manifest:{version}"),
        content_sha256=content_sha256,
        duplicate_of_gallery_name=duplicate_of,
    )


def _publish(database: H2HDB, galleries: Iterable[GallerySourceRecord]) -> None:
    gallery_tuple = tuple(galleries)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    try:
        database.publish_snapshot(
            CatalogSnapshot(
                galleries=gallery_tuple,
                selections=(
                    CatalogPublicationSelection(
                        source_gallery_name=gallery_tuple[0].gallery_name
                    ),
                ),
            ),
            ingest_turn=turn,
        )
    finally:
        assert database.complete_gallery_ingest(turn)


def _gallery_id(connection: sqlite3.Connection, gallery_name: str) -> int:
    row = connection.execute(
        "SELECT db_gallery_id FROM galleries_dbids WHERE name = ?",
        (gallery_name,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_changed_source_rebuilds_facts_without_replacing_gallery_identity(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    old_content = _digest("old-content")
    shared_content = _digest("shared-content")
    target = _gallery(
        "dedup-target",
        910_001,
        version="target",
        title="Target",
        comment="target comment",
        upload_account="target-account",
        timestamp=datetime(2025, 1, 1, 1, 2, 3),
        tags=(GalleryTag("group", "target"),),
        files=("target.jpg",),
        content_sha256=shared_content,
    )
    before = _gallery(
        "identity-gallery",
        910_002,
        version="before",
        title="oldtitlemarker",
        comment="oldcommentmarker",
        upload_account="old-account",
        timestamp=datetime(2025, 2, 1, 2, 3, 4),
        tags=(GalleryTag("artist", "old-artist"),),
        files=("oldfilemarker.jpg", "old-extra.png"),
        content_sha256=old_content,
    )
    _publish(database, (target, before))

    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        original_id = _gallery_id(connection, before.gallery_name)
        target_id = _gallery_id(connection, target.gallery_name)

    after = _gallery(
        before.gallery_name,
        910_003,
        version="after",
        title="newtitlemarker",
        comment="newcommentmarker",
        upload_account="new-account",
        timestamp=datetime(2026, 3, 4, 5, 6, 7),
        tags=(
            GalleryTag("artist", "new-artist"),
            GalleryTag("language", "english"),
        ),
        files=("newfilemarker.webp", "new-extra.gif"),
        content_sha256=shared_content,
        duplicate_of=target.gallery_name,
    )
    _publish(database, (target, after))

    with sqlite3.connect(
        database_path,
        detect_types=sqlite3.PARSE_DECLTYPES,
    ) as connection:
        assert _gallery_id(connection, after.gallery_name) == original_id
        assert connection.execute(
            "SELECT full_name FROM galleries_names WHERE db_gallery_id = ?",
            (original_id,),
        ).fetchall() == [(after.gallery_name,)]
        assert connection.execute(
            "SELECT gid FROM galleries_gids WHERE db_gallery_id = ?",
            (original_id,),
        ).fetchall() == [(after.gid,)]
        assert connection.execute(
            "SELECT title FROM galleries_titles WHERE db_gallery_id = ?",
            (original_id,),
        ).fetchall() == [(after.title,)]
        assert connection.execute(
            "SELECT comment FROM galleries_comments WHERE db_gallery_id = ?",
            (original_id,),
        ).fetchall() == [(after.comment,)]
        assert connection.execute(
            "SELECT account FROM galleries_upload_accounts WHERE db_gallery_id = ?",
            (original_id,),
        ).fetchall() == [(after.upload_account,)]

        expected_times = {
            "galleries_download_times": after.download_time,
            "galleries_redownload_times": after.download_time,
            "galleries_upload_times": after.upload_time,
            "galleries_modified_times": after.modified_time,
            "galleries_access_times": after.download_time,
        }
        for table_name, expected in expected_times.items():
            assert connection.execute(
                f"SELECT time FROM {table_name} WHERE db_gallery_id = ?",
                (original_id,),
            ).fetchall() == [(expected,)]

        assert (
            connection.execute(
                """
            SELECT pairs.tag_name, pairs.tag_value
            FROM galleries_tags AS tags
            JOIN galleries_tag_pairs_dbids AS pairs USING (db_tag_pair_id)
            WHERE tags.db_gallery_id = ?
            ORDER BY pairs.tag_name, pairs.tag_value
            """,
                (original_id,),
            ).fetchall()
            == [("artist", "new-artist"), ("language", "english")]
        )
        assert (
            connection.execute(
                """
            SELECT names.full_name, lower(hex(hashes.hash_value))
            FROM files_dbids AS files
            JOIN files_names AS names USING (db_file_id)
            JOIN files_hashs_sha256 AS links USING (db_file_id)
            JOIN files_hashs_sha256_dbids AS hashes USING (db_hash_id)
            WHERE files.db_gallery_id = ?
            ORDER BY names.full_name
            """,
                (original_id,),
            ).fetchall()
            == sorted(
                (
                    source_file.name,
                    source_file.sha256,
                )
                for source_file in after.files
            )
        )
        assert (
            connection.execute(
                """
            SELECT lower(hex(sha256))
            FROM gallery_source_manifests
            WHERE db_gallery_id = ?
            """,
                (original_id,),
            ).fetchall()
            == [(after.source_manifest_sha256,)]
        )

        assert connection.execute(
            "SELECT lower(hex(sha256)) FROM gallery_content_hashes",
        ).fetchall() == [(target.content_sha256,)]
        assert connection.execute("""
            SELECT db_gallery_id, duplicate_of_db_gallery_id
            FROM gallery_duplicate_warnings
            """).fetchall() == [(original_id, target_id)]

        for old_file in before.files:
            assert (
                connection.execute(
                    "SELECT 1 FROM files_hashs_sha256_dbids WHERE hash_value = ?",
                    (bytes.fromhex(old_file.sha256),),
                ).fetchone()
                is None
            )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

        fts_expectations = (
            ("galleries_names_fts", "identity", original_id),
            ("galleries_titles_fts", "newtitlemarker", original_id),
            ("galleries_comments_fts", "newcommentmarker", original_id),
            ("files_names_fts", "newfilemarker", None),
        )
        for table_name, query, expected_rowid in fts_expectations:
            rows = connection.execute(
                f"SELECT rowid FROM {table_name} WHERE {table_name} MATCH ?",
                (query,),
            ).fetchall()
            assert len(rows) == 1
            if expected_rowid is not None:
                assert rows == [(expected_rowid,)]
        for table_name, query in (
            ("galleries_titles_fts", "oldtitlemarker"),
            ("galleries_comments_fts", "oldcommentmarker"),
            ("files_names_fts", "oldfilemarker"),
        ):
            assert (
                connection.execute(
                    f"SELECT rowid FROM {table_name} WHERE {table_name} MATCH ?",
                    (query,),
                ).fetchall()
                == []
            )
