import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path

import pytest

from h2hdb import (
    H2HDB,
    CatalogArtifact,
    CatalogPublication,
    CatalogPublicationSelection,
    CatalogPublisher,
    CatalogPublishResult,
    CatalogSnapshot,
    CatalogSubject,
    CoreConfig,
    DownloadTurn,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
    IngestTurnLostError,
)


def _digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def _gallery(
    name: str,
    gid: int,
    *,
    manifest: str,
    title: str | None = None,
    download_time: datetime | None = None,
    content_sha256: str | None = None,
    duplicate_of: str | None = None,
    file_label: str | None = None,
    file_size: int = 1,
    tags: tuple[GalleryTag, ...] = (),
) -> GallerySourceRecord:
    timestamp = download_time or datetime(2025, 1, 2, 3, 4, 5)
    source_file = file_label or name
    return GallerySourceRecord(
        gallery_name=name,
        gid=gid,
        title=f"Title for {name}" if title is None else title,
        comment="",
        upload_account="",
        upload_time=timestamp - timedelta(days=1),
        download_time=timestamp,
        modified_time=timestamp + timedelta(minutes=1),
        tags=tags,
        files=(
            GallerySourceFile(
                name=f"{source_file}.jpg",
                size_bytes=file_size,
                sha256=_digest(f"file:{source_file}"),
            ),
        ),
        source_manifest_sha256=_digest(f"manifest:{manifest}"),
        content_sha256=content_sha256,
        duplicate_of_gallery_name=duplicate_of,
    )


def _publication(gallery: GallerySourceRecord) -> CatalogPublication:
    published_at = gallery.upload_time
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=UTC)
    modified_at = gallery.modified_time
    if modified_at.tzinfo is None:
        modified_at = modified_at.replace(tzinfo=UTC)
    return CatalogPublication(
        publication_id=f"urn:h2h:gallery:{gallery.gid}",
        gid=gallery.gid,
        title=gallery.title or gallery.gallery_name,
        source_title=gallery.title,
        sort_title=(gallery.title or gallery.gallery_name).casefold(),
        summary=gallery.comment,
        language=next(
            (tag.value for tag in gallery.tags if tag.name == "language" and tag.value),
            "und",
        ),
        published_at=published_at,
        modified_at=modified_at,
        subjects=tuple(
            CatalogSubject(
                name=tag.value,
                scheme=f"h2h:tag:{tag.name}",
                code=tag.name,
            )
            for tag in gallery.tags
        ),
        artifacts=(
            CatalogArtifact(
                artifact_id=f"urn:h2hdb:artifact:{gallery.gallery_name}",
                name=f"{gallery.gallery_name}.cbz",
                location=Path("/catalog") / f"{gallery.gallery_name}.cbz",
                media_type="application/vnd.comicbook+zip",
                size_bytes=100,
                sha256=_digest(f"artifact:{gallery.gallery_name}"),
                modified_at=modified_at,
            ),
        ),
        source_gallery_name=gallery.gallery_name,
        content_sha256=gallery.content_sha256,
    )


def _selection(gallery: GallerySourceRecord) -> CatalogPublicationSelection:
    return CatalogPublicationSelection(
        source_gallery_name=gallery.gallery_name,
        artifacts=_publication(gallery).artifacts,
    )


def _publish(
    database: H2HDB,
    snapshot: CatalogSnapshot,
) -> CatalogPublishResult:
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    try:
        return database.publish_snapshot(
            snapshot,
            ingest_turn=turn,
        )
    finally:
        assert database.complete_gallery_ingest(turn)


def _sqlite_path(config: CoreConfig) -> Path:
    return Path(config.database.database)


def test_snapshot_atomically_publishes_canonical_losers_and_projection(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    for gid in (810_001, 810_002):
        missing = database.request_download(gid)
        database.complete_missing_download_request(missing, gid)

    content_sha256 = _digest("shared-content")
    owner = _gallery(
        "owner",
        810_001,
        manifest="owner-v1",
        title="",
        download_time=datetime(2025, 1, 1),
        content_sha256=content_sha256,
        file_size=0,
        tags=(GalleryTag("misc", ""),),
    )
    loser = _gallery(
        "loser",
        810_002,
        manifest="loser-v1",
        content_sha256=content_sha256,
        duplicate_of="owner",
    )
    same_gid = _gallery(
        "newer-copy-of-gid",
        810_001,
        manifest="same-gid-v1",
        download_time=datetime(2025, 2, 1),
    )
    publication = _publication(owner)

    result = _publish(
        database,
        CatalogSnapshot(
            galleries=(owner, loser, same_gid),
            selections=(_selection(owner),),
        ),
    )

    assert isinstance(database, CatalogPublisher)
    assert not hasattr(database, "publish_revision")
    assert result.revision.revision == 1
    assert (
        result.new_galleries,
        result.changed_galleries,
        result.removed_galleries,
    ) == (
        3,
        0,
        0,
    )
    assert database.list_publications(limit=10).publications == (publication,)
    projected = database.get_publication(publication.publication_id)
    assert projected is not None
    assert projected.title == "owner"
    assert projected.source_title == ""
    assert projected.subjects == (
        CatalogSubject(name="", scheme="h2h:tag:misc", code="misc"),
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        canonical_rows = connection.execute("""
            SELECT names.full_name, gids.gid, titles.title, accounts.account
            FROM galleries_names AS names
            JOIN galleries_gids AS gids USING (db_gallery_id)
            JOIN galleries_titles AS titles USING (db_gallery_id)
            JOIN galleries_upload_accounts AS accounts USING (db_gallery_id)
            ORDER BY names.full_name
            """).fetchall()
        assert canonical_rows == [
            ("loser", 810_002, "Title for loser", ""),
            ("newer-copy-of-gid", 810_001, "Title for newer-copy-of-gid", ""),
            ("owner", 810_001, "", ""),
        ]
        assert connection.execute(
            "SELECT tag_name, tag_value FROM galleries_tag_pairs_dbids"
        ).fetchall() == [("misc", "")]
        assert connection.execute("""
            SELECT files_names.full_name, files_hashs_sha256_dbids.hash_value
            FROM files_names
            JOIN files_hashs_sha256 USING (db_file_id)
            JOIN files_hashs_sha256_dbids USING (db_hash_id)
            WHERE files_names.full_name = 'owner.jpg'
            """).fetchone() == ("owner.jpg", bytes.fromhex(_digest("file:owner")))
        assert connection.execute(
            "SELECT duplicate_name, kept_name FROM gallery_duplicate_warnings_names"
        ).fetchall() == [("loser", "owner")]
        assert connection.execute("""
            SELECT names.full_name, hashes.sha256
            FROM gallery_content_hashes AS hashes
            JOIN galleries_names AS names USING (db_gallery_id)
            """).fetchall() == [("owner", bytes.fromhex(content_sha256))]
        owner_times = connection.execute("""
            SELECT download.time, redownload.time, access.time
            FROM galleries_names AS names
            JOIN galleries_download_times AS download USING (db_gallery_id)
            JOIN galleries_redownload_times AS redownload USING (db_gallery_id)
            JOIN galleries_access_times AS access USING (db_gallery_id)
            WHERE names.full_name = 'owner'
            """).fetchone()
        assert owner_times == (
            "2025-01-01 00:00:00",
            "2025-01-01 00:00:00",
            "2025-01-01 00:00:00",
        )
        assert connection.execute(
            "SELECT gid FROM removed_galleries_gids ORDER BY gid"
        ).fetchall() == [(810_001,), (810_002,)]
        deletion_names = connection.execute("""
            SELECT names.full_name
            FROM todelete_galleries
            JOIN galleries_names AS names USING (db_gallery_id)
            ORDER BY names.full_name
            """).fetchall()
        assert deletion_names == [("owner",)]


def test_snapshot_selection_requires_canonical_source_and_unique_artifacts() -> None:
    first = _gallery("first", 811_001, manifest="first")
    second = _gallery("second", 811_002, manifest="second")
    first_selection = _selection(first)

    with pytest.raises(ValueError, match="absent canonical source"):
        CatalogSnapshot(
            galleries=(first,),
            selections=(CatalogPublicationSelection(source_gallery_name="missing"),),
        )

    with pytest.raises(ValueError, match="duplicate artifact ID"):
        CatalogSnapshot(
            galleries=(first, second),
            selections=(
                first_selection,
                CatalogPublicationSelection(
                    source_gallery_name=second.gallery_name,
                    artifacts=first_selection.artifacts,
                ),
            ),
        )


def test_snapshot_diff_preserves_identity_and_rebuilds_changed_facts(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    old_a = _gallery("a", 820_001, manifest="a-v1")
    old_b = _gallery("b", 820_002, manifest="b-v1", file_label="b-old")
    old_c = _gallery("c", 820_003, manifest="c-v1", file_label="c-old")
    first = CatalogSnapshot(
        galleries=(old_a, old_b, old_c),
        selections=tuple(_selection(item) for item in (old_a, old_b, old_c)),
    )
    _publish(database, first)

    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        old_ids = dict(
            connection.execute(
                "SELECT full_name, db_gallery_id FROM galleries_names"
            ).fetchall()
        )
        connection.execute(
            """
            UPDATE galleries_redownload_times SET time = '2031-01-02 03:04:05'
            WHERE db_gallery_id = ?
            """,
            (old_ids["a"],),
        )
        connection.execute(
            """
            UPDATE galleries_access_times SET time = '2032-02-03 04:05:06'
            WHERE db_gallery_id = ?
            """,
            (old_ids["a"],),
        )
        connection.execute(
            """
            UPDATE galleries_redownload_times SET time = '2033-03-04 05:06:07'
            WHERE db_gallery_id = ?
            """,
            (old_ids["b"],),
        )
        connection.execute(
            """
            UPDATE galleries_access_times SET time = '2034-04-05 06:07:08'
            WHERE db_gallery_id = ?
            """,
            (old_ids["b"],),
        )

    unchanged_a = replace(
        old_a,
        title="Manifest says this metadata is unchanged",
        download_time=datetime(2040, 1, 1),
    )
    changed_b = _gallery(
        "b",
        820_002,
        manifest="b-v2",
        title="Changed B",
        download_time=datetime(2026, 3, 4, 5, 6, 7),
        file_label="b-new",
    )
    new_d = _gallery("d", 820_004, manifest="d-v1")
    second = CatalogSnapshot(
        galleries=(unchanged_a, changed_b, new_d),
        selections=tuple(_selection(item) for item in (unchanged_a, changed_b, new_d)),
    )

    result = _publish(database, second)

    assert (
        result.new_galleries,
        result.changed_galleries,
        result.removed_galleries,
    ) == (
        1,
        1,
        1,
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        current_ids = dict(
            connection.execute(
                "SELECT full_name, db_gallery_id FROM galleries_names"
            ).fetchall()
        )
        assert current_ids["a"] == old_ids["a"]
        assert current_ids["b"] == old_ids["b"]
        assert "c" not in current_ids
        assert (
            connection.execute(
                """
            SELECT title FROM galleries_titles
            WHERE db_gallery_id = ?
            """,
                (current_ids["a"],),
            ).fetchone()
            == ("Title for a",)
        )
        assert (
            connection.execute(
                """
            SELECT redownload.time, access.time
            FROM galleries_redownload_times AS redownload
            JOIN galleries_access_times AS access USING (db_gallery_id)
            WHERE redownload.db_gallery_id = ?
            """,
                (current_ids["a"],),
            ).fetchone()
            == ("2031-01-02 03:04:05", "2032-02-03 04:05:06")
        )
        assert (
            connection.execute(
                """
            SELECT titles.title, redownload.time, access.time
            FROM galleries_titles AS titles
            JOIN galleries_redownload_times AS redownload USING (db_gallery_id)
            JOIN galleries_access_times AS access USING (db_gallery_id)
            WHERE titles.db_gallery_id = ?
            """,
                (current_ids["b"],),
            ).fetchone()
            == (
                "Changed B",
                "2033-03-04 05:06:07",
                "2034-04-05 06:07:08",
            )
        )
        remaining_hashes = {
            bytes(row[0]).hex()
            for row in connection.execute(
                "SELECT hash_value FROM files_hashs_sha256_dbids"
            ).fetchall()
        }
        assert _digest("file:b-old") not in remaining_hashes
        assert _digest("file:c-old") not in remaining_hashes
        assert _digest("file:b-new") in remaining_hashes
        assert (
            connection.execute(
                """
            SELECT lower(hex(manifests.sha256)), names.full_name
            FROM gallery_source_manifests AS manifests
            JOIN files_dbids AS files USING (db_gallery_id)
            JOIN files_names AS names USING (db_file_id)
            WHERE manifests.db_gallery_id = ?
            """,
                (current_ids["b"],),
            ).fetchall()
            == [(_digest("manifest:b-v2"), "b-new.jpg")]
        )


def test_changed_snapshot_initializes_missing_operational_times(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    original = _gallery(
        "missing-operational-times",
        824_001,
        manifest="original",
        download_time=datetime(2020, 1, 2, 3, 4, 5),
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=(original,),
            selections=(_selection(original),),
        ),
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        connection.execute("DELETE FROM galleries_redownload_times")
        connection.execute("DELETE FROM galleries_access_times")

    changed = replace(
        original,
        source_manifest_sha256=_digest("manifest:content-hash-v1"),
        download_time=datetime(2026, 7, 8, 9, 10, 11),
    )
    result = _publish(
        database,
        CatalogSnapshot(
            galleries=(changed,),
            selections=(_selection(changed),),
        ),
    )

    assert result.changed_galleries == 1
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert (
            connection.execute("""
            SELECT redownload.time, access.time
            FROM galleries_redownload_times AS redownload
            JOIN galleries_access_times AS access USING (db_gallery_id)
            """).fetchone()
            == (
                "2026-07-08 09:10:11",
                "2026-07-08 09:10:11",
            )
        )


def test_manifest_change_preserves_accepted_operational_state(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    original_galleries = tuple(
        _gallery(
            f"original-manifest-{index}",
            824_100 + index,
            manifest=f"mtime-and-filenames-{index}",
            download_time=datetime(2020, 1, index + 1),
        )
        for index in range(1, 4)
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=original_galleries,
            selections=tuple(_selection(gallery) for gallery in original_galleries),
        ),
    )
    for gallery in original_galleries:
        database.record_accepted_submission(gallery.gid)
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        connection.execute(
            "UPDATE galleries_access_times SET time = '2024-05-06 07:08:09'"
        )
        before = connection.execute("""
            SELECT names.full_name, redownload.time, access.time
            FROM galleries_names AS names
            JOIN galleries_redownload_times AS redownload USING (db_gallery_id)
            JOIN galleries_access_times AS access USING (db_gallery_id)
            ORDER BY names.full_name
            """).fetchall()
    assert database.get_pending_redownload_gids() == []

    content_hash_galleries = tuple(
        replace(
            gallery,
            source_manifest_sha256=_digest(
                f"content-hash-manifest-v1:{gallery.gallery_name}"
            ),
        )
        for gallery in original_galleries
    )
    result = _publish(
        database,
        CatalogSnapshot(
            galleries=content_hash_galleries,
            selections=tuple(_selection(gallery) for gallery in content_hash_galleries),
        ),
    )

    assert result.changed_galleries == len(original_galleries)
    assert database.get_pending_redownload_gids() == []
    states = database.get_candidate_states(
        [gallery.gid for gallery in original_galleries]
    )
    assert all(not state.redownload_required for state in states.values())
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        after = connection.execute("""
            SELECT names.full_name, redownload.time, access.time
            FROM galleries_names AS names
            JOIN galleries_redownload_times AS redownload USING (db_gallery_id)
            JOIN galleries_access_times AS access USING (db_gallery_id)
            ORDER BY names.full_name
            """).fetchall()
    assert after == before


def test_changed_same_gid_sources_merge_operational_times_by_gallery_identity(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    shared_gid = 824_200
    first = _gallery(
        "same-gid-first",
        shared_gid,
        manifest="first-original",
        download_time=datetime(2020, 1, 1),
    )
    second = _gallery(
        "same-gid-second",
        shared_gid,
        manifest="second-original",
        download_time=datetime(2021, 1, 1),
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=(first, second),
            selections=(_selection(first),),
        ),
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        ids = dict(
            connection.execute(
                "SELECT full_name, db_gallery_id FROM galleries_names"
            ).fetchall()
        )
        connection.execute(
            "UPDATE galleries_redownload_times SET time = ? WHERE db_gallery_id = ?",
            ("2019-01-01 00:00:00", ids[first.gallery_name]),
        )
        connection.execute(
            "UPDATE galleries_access_times SET time = ? WHERE db_gallery_id = ?",
            ("2018-01-01 00:00:00", ids[first.gallery_name]),
        )
        connection.execute(
            "UPDATE galleries_redownload_times SET time = ? WHERE db_gallery_id = ?",
            ("2030-01-01 00:00:00", ids[second.gallery_name]),
        )
        connection.execute(
            "UPDATE galleries_access_times SET time = ? WHERE db_gallery_id = ?",
            ("2031-01-01 00:00:00", ids[second.gallery_name]),
        )

    changed_first = replace(
        first,
        source_manifest_sha256=_digest("first-content-manifest"),
        download_time=datetime(2025, 1, 1),
    )
    changed_second = replace(
        second,
        source_manifest_sha256=_digest("second-content-manifest"),
        download_time=datetime(2026, 1, 1),
    )
    result = _publish(
        database,
        CatalogSnapshot(
            galleries=(changed_first, changed_second),
            selections=(_selection(changed_first),),
        ),
    )

    assert result.changed_galleries == 2
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        rows = connection.execute("""
            SELECT names.full_name, redownload.time, access.time
            FROM galleries_names AS names
            JOIN galleries_redownload_times AS redownload USING (db_gallery_id)
            JOIN galleries_access_times AS access USING (db_gallery_id)
            ORDER BY names.full_name
            """).fetchall()
    assert rows == [
        (first.gallery_name, "2025-01-01 00:00:00", "2025-01-01 00:00:00"),
        (second.gallery_name, "2030-01-01 00:00:00", "2031-01-01 00:00:00"),
    ]


def test_identical_snapshot_reuses_current_revision(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    gallery = _gallery("unchanged-revision", 825_103, manifest="stable")
    snapshot = CatalogSnapshot(
        galleries=(gallery,),
        selections=(_selection(gallery),),
    )

    first = _publish(database, snapshot)
    second = _publish(database, snapshot)

    assert second.revision == first.revision
    assert second.new_galleries == 0
    assert second.changed_galleries == 0
    assert second.removed_galleries == 0
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT revision FROM catalog_revision_history ORDER BY revision"
        ).fetchall() == [(0,), (1,)]


def test_remote_missing_marker_survives_initial_and_repeated_local_snapshots(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    gallery = _gallery(
        "remote-missing-with-local-source",
        825_104,
        manifest="remote-missing",
        download_time=datetime(2020, 1, 1),
    )
    missing_request = database.request_download(gallery.gid)
    database.complete_missing_download_request(missing_request, gallery.gid)
    snapshot = CatalogSnapshot(
        galleries=(gallery,),
        selections=(_selection(gallery),),
    )

    _publish(database, snapshot)
    _publish(database, snapshot)

    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT gid FROM removed_galleries_gids"
        ).fetchall() == [(gallery.gid,)]
    assert database.get_pending_redownload_gids() == []

    database.record_gallery_found(gallery.gid)

    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert (
            connection.execute("SELECT gid FROM removed_galleries_gids").fetchall()
            == []
        )
    assert database.get_pending_redownload_gids() == [gallery.gid]


def test_full_snapshot_queues_removed_dedup_loser_absent_from_projection(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    content_sha256 = _digest("dedup-content")
    owner = _gallery(
        "dedup-owner",
        826_001,
        manifest="owner-v1",
        content_sha256=content_sha256,
    )
    loser = _gallery(
        "dedup-loser",
        826_002,
        manifest="loser-v1",
        content_sha256=content_sha256,
        duplicate_of="dedup-owner",
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=(owner, loser),
            selections=(_selection(owner),),
        ),
    )

    _publish(
        database,
        CatalogSnapshot(
            galleries=(owner,),
            selections=(_selection(owner),),
        ),
    )

    assert database.get_download_request(loser.gid) is not None


def test_full_snapshot_does_not_queue_gid_with_another_canonical_source(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    shared_gid = 827_001
    first_source = _gallery("first-source", shared_gid, manifest="first-v1")
    remaining_source = _gallery(
        "remaining-source",
        shared_gid,
        manifest="remaining-v1",
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=(first_source, remaining_source),
            selections=(_selection(first_source),),
        ),
    )

    result = _publish(
        database,
        CatalogSnapshot(
            galleries=(remaining_source,),
            selections=(_selection(remaining_source),),
        ),
    )

    assert result.removed_galleries == 1
    assert database.get_download_request(shared_gid) is None


def test_full_snapshot_consumes_explicit_deletion_without_redownload(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    deleted = _gallery("requested-deletion", 828_001, manifest="delete-v1")
    _publish(
        database,
        CatalogSnapshot(
            galleries=(deleted,),
            selections=(_selection(deleted),),
        ),
    )
    database.request_gallery_deletion(deleted.gid)

    result = _publish(database, CatalogSnapshot(galleries=(), selections=()))

    assert result.removed_galleries == 1
    assert database.get_download_request(deleted.gid) is None
    assert database.get_gallery_deletion_requests() == []


def test_full_snapshot_consumes_preexisting_orphan_deletion_marker(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    reappearing = _gallery("orphan-marker", 828_002, manifest="reappearing")
    database.request_gallery_deletion(reappearing.gid)

    _publish(database, CatalogSnapshot(galleries=(), selections=()))

    assert database.get_gallery_deletion_requests() == []
    _publish(
        database,
        CatalogSnapshot(
            galleries=(reappearing,),
            selections=(_selection(reappearing),),
        ),
    )
    assert database.get_gallery_deletion_requests() == []
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM todelete_galleries"
        ).fetchone() == (0,)


def test_pointer_failure_rolls_back_canonical_projection_and_queue(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    gallery = _gallery("must-roll-back", 830_001, manifest="rollback")
    snapshot = CatalogSnapshot(
        galleries=(gallery,),
        selections=(_selection(gallery),),
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        connection.execute("""
            CREATE TRIGGER reject_catalog_pointer
            BEFORE UPDATE ON catalog_revision
            BEGIN
                SELECT RAISE(ABORT, 'pointer failure');
            END
            """)

    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    with pytest.raises(Exception, match="pointer failure"):
        database.publish_snapshot(
            snapshot,
            ingest_turn=turn,
        )
    assert database.complete_gallery_ingest(turn)

    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM galleries_dbids"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_publications"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_revision_history"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT current_revision FROM catalog_revision"
        ).fetchone() == (0,)


def test_pointer_failure_rolls_back_automatic_removed_gid_queue(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    disappearing = _gallery("disappearing", 835_001, manifest="before-failure")
    publication = _publication(disappearing)
    _publish(
        database,
        CatalogSnapshot(
            galleries=(disappearing,),
            selections=(_selection(disappearing),),
        ),
    )
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        connection.execute("""
            CREATE TRIGGER reject_removed_catalog_pointer
            BEFORE UPDATE ON catalog_revision
            BEGIN
                SELECT RAISE(ABORT, 'removed pointer failure');
            END
            """)

    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    with pytest.raises(Exception, match="removed pointer failure"):
        database.publish_snapshot(
            CatalogSnapshot(galleries=(), selections=()),
            ingest_turn=turn,
        )
    assert database.complete_gallery_ingest(turn)

    assert database.get_catalog_revision().revision == 1
    assert database.list_publications(limit=10).publications == (publication,)
    assert database.get_download_request(disappearing.gid) is None
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT full_name FROM galleries_names"
        ).fetchall() == [("disappearing",)]
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_revision_history"
        ).fetchone() == (2,)


def test_stale_ingest_turn_cannot_mutate_canonical_snapshot(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    original = _gallery("stable", 840_001, manifest="stable-v1")
    _publish(
        database,
        CatalogSnapshot(
            galleries=(original,),
            selections=(_selection(original),),
        ),
    )
    stale = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert stale is not None
    assert database.complete_gallery_ingest(stale)
    changed = _gallery("stable", 840_001, manifest="stable-v2", title="Rejected")

    with pytest.raises(IngestTurnLostError):
        database.publish_snapshot(
            CatalogSnapshot(
                galleries=(changed,),
                selections=(_selection(changed),),
            ),
            ingest_turn=stale,
        )

    assert database.get_catalog_revision().revision == 1
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute("""
            SELECT title FROM galleries_titles
            JOIN galleries_names USING (db_gallery_id)
            WHERE galleries_names.full_name = 'stable'
            """).fetchone() == ("Title for stable",)


def test_accepted_submission_is_token_fenced_and_renew_busy_returns_false(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    gallery = _gallery(
        "accepted",
        850_001,
        manifest="accepted-v1",
        download_time=datetime(2020, 1, 1),
    )
    _publish(
        database,
        CatalogSnapshot(
            galleries=(gallery,),
            selections=(_selection(gallery),),
        ),
    )
    missing_request = database.request_download(gallery.gid)
    database.complete_missing_download_request(missing_request, gallery.gid)
    stale_request = database.request_download(gallery.gid)
    current_request = database.request_download(gallery.gid)
    database.record_accepted_submission(gallery.gid, request=stale_request)
    assert database.get_download_request(gallery.gid) == current_request
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert connection.execute(
            "SELECT gid FROM removed_galleries_gids"
        ).fetchall() == [(gallery.gid,)]
        assert (
            connection.execute(
                """
            SELECT redownload.time
            FROM galleries_redownload_times AS redownload
            JOIN galleries_gids AS gids USING (db_gallery_id)
            WHERE gids.gid = ?
            """,
                (gallery.gid,),
            ).fetchone()
            == ("2020-01-01 00:00:00",)
        )

    database.record_accepted_submission(gallery.gid, request=current_request)
    assert database.get_download_request(gallery.gid) is None
    with sqlite3.connect(_sqlite_path(sqlite_config)) as connection:
        assert (
            connection.execute("SELECT gid FROM removed_galleries_gids").fetchall()
            == []
        )
        redownload_time = connection.execute(
            """
            SELECT redownload.time
            FROM galleries_redownload_times AS redownload
            JOIN galleries_gids AS gids USING (db_gallery_id)
            WHERE gids.gid = ?
            """,
            (gallery.gid,),
        ).fetchone()
    assert redownload_time is not None
    assert redownload_time[0] != "2020-01-01 00:00:00"

    request = database.request_download(850_002)
    turn = database.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def _busy(_turn: DownloadTurn, *, lease_seconds: int) -> bool:
        del _turn, lease_seconds
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(database._gallery_ingest, "renew_download_turn", _busy)
    assert database.renew_download_turn(turn, lease_seconds=60) is False
    assert database.get_download_request(request.gid) == request


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            datetime(2026, 8, 7, 16, 30, tzinfo=timezone(timedelta(hours=8))),
            datetime(2026, 8, 7, 8, 30),
        ),
        (datetime(2026, 8, 7, 16, 30), datetime(2026, 8, 7, 16, 30)),
    ],
)
def test_canonical_datetimes_normalize_aware_but_preserve_naive_calendar(
    sqlite_config: CoreConfig,
    source: datetime,
    expected: datetime,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    gallery = _gallery(
        "datetime-gallery",
        860_001,
        manifest="datetime",
        download_time=source,
    )
    publication = _publication(gallery)
    _publish(
        database,
        CatalogSnapshot(galleries=(gallery,), selections=(_selection(gallery),)),
    )

    with sqlite3.connect(
        _sqlite_path(sqlite_config),
        detect_types=sqlite3.PARSE_DECLTYPES,
    ) as connection:
        stored = connection.execute(
            "SELECT time FROM galleries_download_times"
        ).fetchone()
    assert stored == (expected,)
    projected = database.get_publication(publication.publication_id)
    assert projected is not None
    assert projected.published_at.tzinfo is not None
    assert projected.modified_at.tzinfo is not None
