from collections.abc import Iterator

import pytest

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.sql_connector import DatabaseDuplicateKeyError, DatabaseKeyError


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def test_gallery_name_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - gallery title"
    db._insert_gallery_name(gallery_name)

    assert db._check_galleries_dbids_by_gallery_name(gallery_name) is True
    db_gallery_id = db._get_db_gallery_id_by_gallery_name(gallery_name)
    assert isinstance(db_gallery_id, int)


def test_gallery_gid_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - another gallery"
    db._insert_gallery_name(gallery_name)
    db_gallery_id = db._get_db_gallery_id_by_gallery_name(gallery_name)

    db._insert_gallery_gid(db_gallery_id, gid=123456)

    assert db.check_gid_by_gid(123456) is True
    assert db.get_gid_by_gallery_name(gallery_name) == 123456
    assert db.get_gids() == [123456]


def test_gallery_gid_duplicate_raises(db: H2HDB) -> None:
    gallery_name = "artist - yet another gallery"
    db._insert_gallery_name(gallery_name)
    db_gallery_id = db._get_db_gallery_id_by_gallery_name(gallery_name)
    db._insert_gallery_gid(db_gallery_id, gid=999)

    with pytest.raises(DatabaseDuplicateKeyError):
        db._insert_gallery_gid(db_gallery_id, gid=999)


def test_removed_gallery_gid_round_trip(db: H2HDB) -> None:
    db.insert_removed_gallery_gid(42)

    assert db._check_removed_gallery_gid(42) is True
    assert db.select_removed_gallery_gid(42) == 42


def test_removed_gallery_gid_missing_raises(db: H2HDB) -> None:
    with pytest.raises(DatabaseKeyError):
        db.select_removed_gallery_gid(404)


def test_gallery_comment_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - commented gallery"
    db._insert_gallery_name(gallery_name)
    db_gallery_id = db._get_db_gallery_id_by_gallery_name(gallery_name)

    db._insert_gallery_comment(db_gallery_id, "hello world")

    assert db._check_gallery_comment_by_db_gallery_id(db_gallery_id) is True
    assert db._select_gallery_comment(db_gallery_id) == "hello world"


def test_hash_value_round_trip(db: H2HDB) -> None:
    hash_value = bytes.fromhex("ab" * 64)

    db.insert_db_hash_id_by_hash_value(hash_value, "sha512")

    assert db._check_db_hash_id_by_hash_value(hash_value, "sha512") is True
    db_hash_id = db.get_db_hash_id_by_hash_value(hash_value, "sha512")
    assert isinstance(db_hash_id, int)
    assert db.get_hash_value_by_db_hash_id(db_hash_id, "sha512") == hash_value


def test_get_files_by_gallery_name(db: H2HDB) -> None:
    gallery_name = "artist - gallery with files"
    db._insert_gallery_name(gallery_name)
    db_gallery_id = db._get_db_gallery_id_by_gallery_name(gallery_name)

    db._insert_gallery_files(db_gallery_id, ["page1.jpg", "page2.jpg"])

    assert sorted(db.get_files_by_gallery_name(gallery_name)) == [
        "page1.jpg",
        "page2.jpg",
    ]
    db_file_id = db._get_db_file_id(db_gallery_id, "page1.jpg")
    assert isinstance(db_file_id, int)
