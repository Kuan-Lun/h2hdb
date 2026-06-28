from collections.abc import Iterator
from pathlib import Path

import pytest

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.h2hdb_h2hdb import insert_gallery_info_worker
from h2hdb.sql_connector import DatabaseDuplicateKeyError, DatabaseKeyError
from h2hdb.threading_tools import run_in_parallel


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def test_gallery_name_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - gallery title"
    db.gallery_ids._insert_gallery_name(gallery_name)

    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name) is True
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    assert isinstance(db_gallery_id, int)


def test_gallery_gid_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - another gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)

    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=123456)

    assert db.gallery_gids.check_gid_by_gid(123456) is True
    assert db.gallery_gids.get_gid_by_gallery_name(gallery_name) == 123456
    assert db.gallery_gids.get_gids() == [123456]


def test_gallery_gid_duplicate_raises(db: H2HDB) -> None:
    gallery_name = "artist - yet another gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=999)

    with pytest.raises(DatabaseDuplicateKeyError):
        db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=999)


def test_removed_gallery_gid_round_trip(db: H2HDB) -> None:
    db.removed_galleries.insert_removed_gallery_gid(42)

    assert db.removed_galleries._check_removed_gallery_gid(42) is True
    assert db.removed_galleries.select_removed_gallery_gid(42) == 42


def test_removed_gallery_gid_missing_raises(db: H2HDB) -> None:
    with pytest.raises(DatabaseKeyError):
        db.removed_galleries.select_removed_gallery_gid(404)


def test_gallery_comment_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - commented gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)

    db.gallery_comments._insert_gallery_comment(db_gallery_id, "hello world")

    assert (
        db.gallery_comments._check_gallery_comment_by_db_gallery_id(db_gallery_id)
        is True
    )
    assert db.gallery_comments._select_gallery_comment(db_gallery_id) == "hello world"


def test_hash_value_round_trip(db: H2HDB) -> None:
    hash_value = bytes.fromhex("ab" * 64)

    db.files.insert_db_hash_id_by_hash_value(hash_value, "sha512")

    assert db.files._check_db_hash_id_by_hash_value(hash_value, "sha512") is True
    db_hash_id = db.files.get_db_hash_id_by_hash_value(hash_value, "sha512")
    assert isinstance(db_hash_id, int)
    assert db.files.get_hash_value_by_db_hash_id(db_hash_id, "sha512") == hash_value


def test_bulk_hash_insert_handles_stale_duplicate_read(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    hash_value = bytes.fromhex("cd" * 64)
    db.files.insert_db_hash_id_by_hash_value(hash_value, "sha512")

    original_get_hash_ids = db.files._get_db_hash_ids_by_hash_values
    call_count = 0

    def stale_once(hash_values: set[bytes], algorithm: str) -> dict[bytes, int]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}
        return original_get_hash_ids(hash_values, algorithm)

    monkeypatch.setattr(db.files, "_get_db_hash_ids_by_hash_values", stale_once)

    db.files.insert_db_hash_id_by_hash_values({hash_value}, "sha512")

    assert db.files._check_db_hash_id_by_hash_value(hash_value, "sha512") is True


def test_bulk_hash_insert_split_retries_duplicate_batches(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    duplicate_hash = bytes.fromhex("dd" * 64)
    new_hashes = [bytes([index]) * 64 for index in range(4)]
    db.files.insert_db_hash_id_by_hash_value(duplicate_hash, "sha512")

    original_get_hash_ids = db.files._get_db_hash_ids_by_hash_values
    original_insert_hashes = db.files._insert_db_hash_ids_with_split_retry
    call_sizes = list[int]()

    def stale_once(hash_values: set[bytes], algorithm: str) -> dict[bytes, int]:
        if hash_values == {duplicate_hash, *new_hashes}:
            return {}
        return original_get_hash_ids(hash_values, algorithm)

    def recording_insert(hash_values: list[bytes], algorithm: str) -> None:
        call_sizes.append(len(hash_values))
        original_insert_hashes(hash_values, algorithm)

    monkeypatch.setattr(db.files, "_get_db_hash_ids_by_hash_values", stale_once)
    monkeypatch.setattr(
        db.files, "_insert_db_hash_ids_with_split_retry", recording_insert
    )

    db.files.insert_db_hash_id_by_hash_values(
        {duplicate_hash, *new_hashes}, "sha512"
    )

    for hash_value in new_hashes:
        assert db.files._check_db_hash_id_by_hash_value(hash_value, "sha512") is True
    assert any(call_size > 1 for call_size in call_sizes[1:])


def test_get_files_by_gallery_name(db: H2HDB) -> None:
    gallery_name = "artist - gallery with files"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)

    db.files._insert_gallery_files(db_gallery_id, ["page1.jpg", "page2.jpg"])

    assert sorted(db.files.get_files_by_gallery_name(gallery_name)) == [
        "page1.jpg",
        "page2.jpg",
    ]
    db_file_id = db.files._get_db_file_id(db_gallery_id, "page1.jpg")
    assert isinstance(db_file_id, int)


def test_parallel_worker_does_not_pickle_h2hdb_instance(
    db_config: H2HDBConfig, tmp_path: Path
) -> None:
    gallery_folder = tmp_path / "123456"
    gallery_folder.mkdir()
    (gallery_folder / "galleryinfo.txt").write_text(
        "\n".join(
            [
                "Title: Worker Gallery",
                "Upload Time: 2024-01-01 00:00",
                "Uploaded By: tester",
                "Downloaded: 2024-01-02 00:00",
                "Tags: artist:worker, language:english",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            ]
        ),
        encoding="utf-8",
    )

    with H2HDB(config=db_config) as db:
        db.create_main_tables()

    config_data = db_config.model_dump(mode="json")
    result = run_in_parallel(
        insert_gallery_info_worker,
        [(config_data, str(gallery_folder))],
    )

    assert result == [True]
