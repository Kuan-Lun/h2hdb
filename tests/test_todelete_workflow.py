"""Integration tests for the durable gallery-deletion workflow."""

from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pytest

from h2hdb import H2HDB, DownloadRequest, H2HDBConfig, SyncOutcome
from h2hdb import todownload_queue as todownload_queue_module
from h2hdb.hash_dict import FILE_CONTENT_HASH_ALGORITHM
from h2hdb.sql_connector import SQLConnector


@pytest.fixture
def download_path(tmp_path: Path) -> Path:
    path = tmp_path / "download"
    path.mkdir()
    return path


@pytest.fixture
def db(db_config: H2HDBConfig, download_path: Path) -> Iterator[H2HDB]:
    db_config.h2h.download_path = download_path
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _insert_gallery(
    db: H2HDB,
    name: str,
    gid: int,
    *,
    download_time: str | None = None,
) -> int:
    db.gallery_ids._insert_gallery_name(name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid)
    if download_time is not None:
        db.gallery_times._insert_download_time(db_gallery_id, download_time)
    return db_gallery_id


def _insert_galleries(db: H2HDB, galleries: list[tuple[str, int]]) -> None:
    match db.config.database.sql_type.lower():
        case "mariadb":
            column_names, _ = db.mariadb_split_gallery_name_based_on_limit("name")
        case "sqlite":
            column_names, _ = db.sqlite_name_columns("name")

    db._insert_rows(
        "galleries_dbids",
        column_names,
        [tuple(db._split_gallery_name(name)) for name, _gid in galleries],
    )
    db_gallery_ids = db.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
        [name for name, _gid in galleries]
    )
    db._insert_rows(
        "galleries_names",
        ["db_gallery_id", "full_name"],
        [(db_gallery_ids[name], name) for name, _gid in galleries],
    )
    db._insert_rows(
        "galleries_gids",
        ["db_gallery_id", "gid"],
        [(db_gallery_ids[name], gid) for name, gid in galleries],
    )


def _assign_file_hashes(
    db: H2HDB,
    db_gallery_id: int,
    hashes: list[bytes],
) -> None:
    file_ids_by_name = db.files._insert_gallery_files(
        db_gallery_id,
        [f"{index:03d}.jpg" for index in range(len(hashes))],
    )
    db.files.insert_db_hash_id_by_hash_values(set(hashes), FILE_CONTENT_HASH_ALGORITHM)
    hash_ids = db.files._get_db_hash_ids_by_hash_values(
        set(hashes), FILE_CONTENT_HASH_ALGORITHM
    )
    with db.SQLConnector() as connector:
        connector.execute_many(
            """
            INSERT INTO files_hashs_sha256 (db_file_id, db_hash_id)
            VALUES (%s, %s)
            """,
            [
                (file_ids_by_name[f"{index:03d}.jpg"], hash_ids[hash_value])
                for index, hash_value in enumerate(hashes)
            ],
        )


def _candidate_names(db: H2HDB) -> set[str]:
    with db.SQLConnector() as connector:
        rows = connector.fetch_all("""
            SELECT galleries_names.full_name
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
            """)
    return {str(row[0]) for row in rows}


def _write_galleryinfo(gallery_folder: Path, *, title: str) -> None:
    gallery_folder.mkdir()
    (gallery_folder / "galleryinfo.txt").write_text(
        "\n".join(
            [
                f"Title: {title}",
                "Upload Time: 2024-01-01 00:00",
                "Uploaded By: tester",
                "Downloaded: 2024-01-02 00:00",
                "Tags: language:english",
                "Downloaded from E-Hentai Galleries by the "
                "Hentai@Home Downloader <3",
            ]
        ),
        encoding="utf-8",
    )


def test_active_snapshot_contains_exactly_the_three_deletion_sources(
    db: H2HDB,
) -> None:
    explicit_name = "explicit deletion"
    duplicate_gid_old_name = "same gid old"
    duplicate_gid_new_name = "same gid new"
    duplicate_hash_name = "duplicate hashes inside gallery"
    normal_name = "normal gallery"

    _insert_gallery(db, explicit_name, 1001)
    _insert_gallery(
        db,
        duplicate_gid_old_name,
        1002,
        download_time="2024-01-01 00:00:00",
    )
    _insert_gallery(
        db,
        duplicate_gid_new_name,
        1002,
        download_time="2024-02-01 00:00:00",
    )
    duplicate_hash_id = _insert_gallery(db, duplicate_hash_name, 1003)
    _assign_file_hashes(db, duplicate_hash_id, [b"a" * 32, b"a" * 32])
    _insert_gallery(db, normal_name, 1004)

    db.request_gallery_deletion(1001)

    assert db.todelete_queue.refresh_todelete_galleries() == 3
    assert _candidate_names(db) == {
        explicit_name,
        duplicate_gid_old_name,
        duplicate_hash_name,
    }


def test_equal_full_content_with_different_gids_is_not_a_deletion_candidate(
    db: H2HDB,
) -> None:
    first_name = "full duplicate first gid"
    second_name = "full duplicate second gid"
    first_id = _insert_gallery(db, first_name, 1101)
    second_id = _insert_gallery(db, second_name, 1102)
    identical_hashes = [b"a" * 32, b"b" * 32]
    _assign_file_hashes(db, first_id, identical_hashes)
    _assign_file_hashes(db, second_id, identical_hashes)

    # This is the current CBZ-level representation of equal effective content.
    # It must not leak back into raw-gallery deletion candidates.
    db.gallery_deduplication._record_duplicate_warning(first_id, second_id)

    assert db.todelete_queue.refresh_todelete_galleries() == 0
    assert _candidate_names(db) == set()


def test_deletion_is_enqueued_only_after_the_folder_disappears(
    db: H2HDB,
    download_path: Path,
) -> None:
    gid = 1201
    gallery_folder = download_path / str(gid)
    galleryinfo_path = gallery_folder / "galleryinfo.txt"
    _write_galleryinfo(gallery_folder, title="Delayed physical removal")

    assert db.synchronize_once() == SyncOutcome(1, 0, 0)
    db.request_gallery_deletion(gid)

    assert db.synchronize_once() == SyncOutcome(0, 0, 0)
    assert db.get_download_request(gid) is None
    assert _candidate_names(db) == {str(gid)}

    galleryinfo_path.unlink()
    gallery_folder.rmdir()
    outcome = db.synchronize_once()

    assert outcome == SyncOutcome(0, 0, 1)
    assert outcome.has_changes is True
    assert outcome.needs_immediate_rescan is False
    assert db.get_download_request(gid) is not None
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(str(gid)) is False


def test_non_candidate_physical_removal_does_not_enqueue_download(
    db: H2HDB,
) -> None:
    gallery_name = "ordinary missing gallery"
    gid = 1301
    _insert_gallery(db, gallery_name, gid)
    assert db.todelete_queue.refresh_todelete_galleries() == 0

    assert db.pending_removals.delete_confirmed_missing_galleries([gallery_name]) == 1

    assert db.get_download_request(gid) is None
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name) is False


def test_same_gid_deleted_in_one_batch_creates_one_request_token(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gid = 1401
    names = ["same gid first", "same gid second"]
    for name in names:
        _insert_gallery(db, name, gid)
    db.request_gallery_deletion(gid)
    assert db.todelete_queue.refresh_todelete_galleries() == 2

    generated_tokens: list[UUID] = []

    def recording_uuid4() -> UUID:
        token = UUID(int=len(generated_tokens) + 1)
        generated_tokens.append(token)
        return token

    monkeypatch.setattr(todownload_queue_module, "uuid4", recording_uuid4)

    assert db.pending_removals.delete_confirmed_missing_galleries(names) == 2
    assert generated_tokens == [UUID(int=1)]
    assert db.get_download_request(gid) is not None


def test_same_gid_deleted_across_batches_creates_one_request_token(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gid = 1402
    galleries = [(f"same gid batch {index:03d}", gid) for index in range(501)]
    names = [name for name, _gid in galleries]
    _insert_galleries(db, galleries)
    db.request_gallery_deletion(gid)
    assert db.todelete_queue.refresh_todelete_galleries() == len(galleries)

    generated_tokens: list[UUID] = []

    def recording_uuid4() -> UUID:
        token = UUID(int=len(generated_tokens) + 1)
        generated_tokens.append(token)
        return token

    monkeypatch.setattr(todownload_queue_module, "uuid4", recording_uuid4)

    assert db.pending_removals.delete_confirmed_missing_galleries(names) == len(
        galleries
    )
    assert generated_tokens == [UUID(int=1)]
    assert db.get_download_request(gid) is not None


def test_distinct_gids_are_cleaned_up_across_batches(db: H2HDB) -> None:
    galleries = [
        (f"distinct gid batch {index:03d}", 30_000 + index) for index in range(501)
    ]
    names = [name for name, _gid in galleries]
    gids = [gid for _name, gid in galleries]
    _insert_galleries(db, galleries)
    db._insert_rows("todelete_gids", ["gid"], [(gid,) for gid in gids])
    assert db.todelete_queue.refresh_todelete_galleries() == len(galleries)

    assert db.pending_removals.delete_confirmed_missing_galleries(names) == len(
        galleries
    )

    assert {request.gid for request in db.get_download_requests()} == set(gids)
    with db.SQLConnector() as connector:
        assert connector.fetch_one("SELECT COUNT(*) FROM galleries_dbids") == (0,)
        assert connector.fetch_one("SELECT COUNT(*) FROM todelete_gids") == (0,)


def test_deletion_enqueue_with_blank_url_preserves_existing_url(db: H2HDB) -> None:
    gallery_name = "preserve requested url"
    gid = 1501
    url = f"https://e-hentai.org/g/{gid}/abc123def4"
    _insert_gallery(db, gallery_name, gid)
    original_request = db.request_download(gid, url)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()

    assert db.pending_removals.delete_confirmed_missing_galleries([gallery_name]) == 1

    replacement_request = db.get_download_request(gid)
    assert replacement_request is not None
    assert replacement_request.url == url
    assert replacement_request.token != original_request.token


def test_stale_download_acknowledgement_cannot_remove_newer_request(
    db: H2HDB,
) -> None:
    gid = 1601
    stale_request = db.request_download(gid)
    current_request = db.request_download(gid)
    assert current_request.token != stale_request.token

    db.complete_download_request(stale_request)

    assert db.get_download_request(gid) == current_request
    db.complete_download_request(
        DownloadRequest(
            gid, "https://example.invalid/identity-is-the-token", current_request.token
        )
    )
    assert db.get_download_request(gid) is None


def test_blank_download_request_returns_the_effective_preserved_url(
    db: H2HDB,
) -> None:
    gid = 1602
    url = f"https://e-hentai.org/g/{gid}/abc123def4"
    db.request_download(gid, url)

    replacement_request = db.request_download(gid)

    assert replacement_request.url == url
    assert db.get_download_request(gid) == replacement_request


def test_present_pending_gallery_is_refreshed_without_enqueue(db: H2HDB) -> None:
    gallery_name = "present interrupted refresh"
    gid = 1701
    _insert_gallery(db, gallery_name, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()
    db.insert_pending_gallery_removal(gallery_name)

    assert db.pending_removals.recover_pending_gallery_removals({gallery_name}) == 0

    assert db.get_download_request(gid) is None
    assert db.get_pending_gallery_removals() == []
    assert db.is_gallery_deletion_requested(gid) is True


def test_explicit_gid_is_consumed_only_after_its_last_live_gallery(
    db: H2HDB,
) -> None:
    gid = 1801
    first_name = "explicit gid first folder"
    second_name = "explicit gid second folder"
    _insert_gallery(db, first_name, gid)
    _insert_gallery(db, second_name, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()

    assert db.pending_removals.delete_confirmed_missing_galleries([first_name]) == 1
    assert db.is_gallery_deletion_requested(gid) is True
    assert db.get_download_request(gid) is None

    assert db.pending_removals.delete_confirmed_missing_galleries([second_name]) == 1
    assert db.is_gallery_deletion_requested(gid) is False
    assert db.get_download_request(gid) is not None


def test_automatic_same_gid_candidates_enqueue_only_after_the_last_removal(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gid = 1802
    first_old_name = "automatic same gid oldest"
    second_old_name = "automatic same gid middle"
    newest_name = "automatic same gid newest"
    _insert_gallery(
        db,
        first_old_name,
        gid,
        download_time="2024-01-01 00:00:00",
    )
    _insert_gallery(
        db,
        second_old_name,
        gid,
        download_time="2024-02-01 00:00:00",
    )
    _insert_gallery(
        db,
        newest_name,
        gid,
        download_time="2024-03-01 00:00:00",
    )
    assert db.todelete_queue.refresh_todelete_galleries() == 2

    generated_tokens: list[UUID] = []

    def recording_uuid4() -> UUID:
        token = UUID(int=len(generated_tokens) + 1)
        generated_tokens.append(token)
        return token

    monkeypatch.setattr(todownload_queue_module, "uuid4", recording_uuid4)

    assert db.pending_removals.delete_confirmed_missing_galleries([first_old_name]) == 1
    assert db.get_download_request(gid) is None
    assert generated_tokens == []

    assert (
        db.pending_removals.delete_confirmed_missing_galleries([second_old_name]) == 1
    )
    assert db.get_download_request(gid) is not None
    assert generated_tokens == [UUID(int=1)]
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(newest_name) is True


def test_missing_gallery_removal_rolls_back_enqueue_and_delete_together(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gallery_name = "transaction rollback"
    gid = 1901
    _insert_gallery(db, gallery_name, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()
    original_request = db.todownload_queue._request_download_with_connector

    def interrupt_after_enqueue(
        connector: SQLConnector, request_gid: int, url: str = ""
    ) -> DownloadRequest:
        original_request(connector, request_gid, url)
        raise RuntimeError("interrupt removal transaction")

    monkeypatch.setattr(
        db.todownload_queue,
        "_request_download_with_connector",
        interrupt_after_enqueue,
    )

    with pytest.raises(RuntimeError, match="interrupt removal transaction"):
        db.pending_removals.delete_confirmed_missing_galleries([gallery_name])

    assert db.get_download_request(gid) is None
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name) is True
    assert _candidate_names(db) == {gallery_name}
    assert db.is_gallery_deletion_requested(gid) is True


def test_rm_command_shell_quotes_apostrophes(db: H2HDB) -> None:
    gallery_name = "artist's gallery"
    gid = 2001
    _insert_gallery(db, gallery_name, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()

    with db.SQLConnector() as connector:
        row = connector.fetch_one("SELECT cmd FROM todelete_rm_commands")

    assert row == ("rm -rf -- 'artist'\\''s gallery'",)


def test_candidate_publication_exactly_removes_cancelled_requests(db: H2HDB) -> None:
    gallery_name = "cancel explicit deletion"
    gid = 2101
    _insert_gallery(db, gallery_name, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()
    assert _candidate_names(db) == {gallery_name}

    with db.SQLConnector() as connector:
        connector.execute("DELETE FROM todelete_gids WHERE gid = %s", (gid,))

    assert db.todelete_queue.refresh_todelete_galleries() == 0
    assert _candidate_names(db) == set()


def test_missing_partial_parent_without_galleries_names_is_recoverable(
    db: H2HDB,
) -> None:
    gallery_name = "interrupted before galleries names"
    gid = 2201
    match db.config.database.sql_type.lower():
        case "mariadb":
            column_names, _ = db.mariadb_split_gallery_name_based_on_limit("name")
        case "sqlite":
            column_names, _ = db.sqlite_name_columns("name")
    name_parts = db._split_gallery_name(gallery_name)
    with db.SQLConnector() as connector:
        connector.execute(
            f"""
            INSERT INTO galleries_dbids ({", ".join(column_names)})
            VALUES ({", ".join(["%s"] * len(column_names))})
            """,
            tuple(name_parts),
        )
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid)
    db.request_gallery_deletion(gid)
    db.todelete_queue.refresh_todelete_galleries()
    db.insert_pending_gallery_removal(gallery_name)

    assert db.pending_removals.recover_pending_gallery_removals(set()) == 1

    assert db.get_pending_gallery_removals() == []
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name) is False
    assert db.get_download_request(gid) is not None
    assert db.is_gallery_deletion_requested(gid) is False
