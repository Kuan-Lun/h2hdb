from collections.abc import Iterator
from pathlib import Path

import pytest
from h2h_galleryinfo_parser import parse_galleryinfo

from h2hdb import H2HDB, H2HDBConfig
from h2hdb import h2hdb_h2hdb as h2hdb_h2hdb_module
from h2hdb import table_tags as table_tags_module
from h2hdb.hash_dict import HASH_ALGORITHMS
from h2hdb.sql_connector import DatabaseDuplicateKeyError, DatabaseKeyError


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _write_galleryinfo(
    gallery_folder: Path,
    *,
    title: str,
    upload_time: str = "2024-01-01 00:00",
    uploaded_by: str = "tester",
    downloaded: str = "2024-01-02 00:00",
    tags: str = "language:english",
) -> None:
    gallery_folder.mkdir()
    (gallery_folder / "galleryinfo.txt").write_text(
        "\n".join(
            [
                f"Title: {title}",
                f"Upload Time: {upload_time}",
                f"Uploaded By: {uploaded_by}",
                f"Downloaded: {downloaded}",
                f"Tags: {tags}",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            ]
        ),
        encoding="utf-8",
    )


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

    db.files.insert_db_hash_id_by_hash_values({duplicate_hash, *new_hashes}, "sha512")

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


def test_insert_gallery_infos_batches_multiple_galleries(
    db: H2HDB, tmp_path: Path
) -> None:
    gallery_folders = list[Path]()
    for index in range(2):
        gallery_folder = tmp_path / f"12345{index}"
        gallery_folder.mkdir()
        (gallery_folder / "galleryinfo.txt").write_text(
            "\n".join(
                [
                    f"Title: Batch Gallery {index}",
                    f"Upload Time: 2024-01-0{index + 1} 00:00",
                    f"Uploaded By: tester{index}",
                    f"Downloaded: 2024-01-0{index + 2} 00:00",
                    f"Tags: artist:batch{index}, group:shared, language:english",
                    "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
                ]
            ),
            encoding="utf-8",
        )
        (gallery_folder / "001.jpg").write_bytes(f"image-{index}".encode())
        gallery_folders.append(gallery_folder)

    gallery_infos = [parse_galleryinfo(str(path)) for path in gallery_folders]

    assert db.insert_gallery_infos(gallery_infos) == [True, True]

    for index, gallery_info in enumerate(gallery_infos):
        gallery_name = gallery_info.gallery_name
        assert db.gallery_titles.get_title_by_gallery_name(gallery_name) == (
            f"Batch Gallery {index}"
        )
        assert db.upload_accounts.get_upload_account_by_gallery_name(gallery_name) == (
            f"tester{index}"
        )
        assert sorted(db.files.get_files_by_gallery_name(gallery_name)) == [
            "001.jpg",
            "galleryinfo.txt",
        ]
        db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
        db_file_id = db.files._get_db_file_id(db_gallery_id, "001.jpg")
        assert db.files._check_hash_value_by_file_id(db_file_id, "sha512") is True
        assert sorted(db.gallery_tags.get_tag_pairs_by_gallery_name(gallery_name)) == [
            ("artist", f"batch{index}"),
            ("group", "shared"),
            ("language", "english"),
        ]

    assert db.insert_gallery_infos(gallery_infos) == [False, False]


def test_insert_rows_batches_galleries_across_chunk_boundary(
    db: H2HDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Forces _insert_rows to split a 3-row insert into 3 separate batches of 1,
    # pinning down that chunking doesn't lose or misalign rows at the boundary.
    monkeypatch.setattr(h2hdb_h2hdb_module, "GALLERY_METADATA_BATCH_SIZE", 1)

    gallery_folders = list[Path]()
    for index in range(3):
        gallery_folder = tmp_path / f"50000{index}"
        _write_galleryinfo(gallery_folder, title=f"Chunk Gallery {index}")
        gallery_folders.append(gallery_folder)

    gallery_infos = [parse_galleryinfo(str(path)) for path in gallery_folders]

    assert db.insert_gallery_infos(gallery_infos) == [True, True, True]

    for index, gallery_info in enumerate(gallery_infos):
        assert (
            db.gallery_titles.get_title_by_gallery_name(gallery_info.gallery_name)
            == f"Chunk Gallery {index}"
        )


def test_insert_rows_batches_tags_across_chunk_boundary(
    db: H2HDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Forces table_tags._insert_rows to split tag-pair inserts into batches of 1.
    monkeypatch.setattr(table_tags_module, "TAG_BATCH_SIZE", 1)

    gallery_folder = tmp_path / "600001"
    _write_galleryinfo(
        gallery_folder,
        title="Many Tags Gallery",
        tags="artist:a, artist:b, group:c, language:english",
    )
    gallery_info = parse_galleryinfo(str(gallery_folder))

    assert db.insert_gallery_infos([gallery_info]) == [True]

    assert sorted(
        db.gallery_tags.get_tag_pairs_by_gallery_name(gallery_info.gallery_name)
    ) == sorted(
        [("artist", "a"), ("artist", "b"), ("group", "c"), ("language", "english")]
    )


def test_insert_gallery_files_assigns_distinct_ids_for_each_file(db: H2HDB) -> None:
    gallery_name = "artist - many files gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)

    # 254 chars: under FILE_NAME_LENGTH_LIMIT (255) but over the MariaDB
    # index-prefix limit (191), forcing the name to split across two columns
    # on the MariaDB backend.
    long_file_name = "a" * 250 + ".png"
    file_names = ["001.jpg", "002.jpg", long_file_name]

    db.files._insert_gallery_files(db_gallery_id, file_names)

    db_file_ids = {
        file_name: db.files._get_db_file_id(db_gallery_id, file_name)
        for file_name in file_names
    }
    assert len(set(db_file_ids.values())) == len(file_names)
    assert sorted(db.files.get_files_by_gallery_name(gallery_name)) == sorted(
        file_names
    )


def test_insert_gallery_files_returns_matching_id_mapping(db: H2HDB) -> None:
    gallery_name = "artist - mapping gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    file_names = ["001.jpg", "002.jpg", "003.jpg"]

    db_file_ids_by_name = db.files._insert_gallery_files(db_gallery_id, file_names)

    assert set(db_file_ids_by_name) == set(file_names)
    for file_name, db_file_id in db_file_ids_by_name.items():
        assert db.files._get_db_file_id(db_gallery_id, file_name) == db_file_id


def test_insert_gallery_infos_does_not_issue_per_file_id_lookups(
    db: H2HDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # _insert_gallery_files now returns the name->db_file_id mapping computed
    # from a single batched SELECT, so neither it nor its caller should fall
    # back to looking up file ids one by one.
    gallery_folder = tmp_path / "700001"
    _write_galleryinfo(gallery_folder, title="No N+1 Gallery")
    for index in range(5):
        (gallery_folder / f"{index:03d}.jpg").write_bytes(f"image-{index}".encode())
    gallery_info = parse_galleryinfo(str(gallery_folder))

    call_count = 0
    original_get_db_file_id = db.files._get_db_file_id

    def counting_get_db_file_id(db_gallery_id: int, file_name: str) -> int:
        nonlocal call_count
        call_count += 1
        return original_get_db_file_id(db_gallery_id, file_name)

    monkeypatch.setattr(db.files, "_get_db_file_id", counting_get_db_file_id)

    assert db.insert_gallery_infos([gallery_info]) == [True]

    assert call_count == 0


def test_refresh_current_files_hashs_removes_orphans_for_every_algorithm(
    db: H2HDB,
) -> None:
    for index, algorithm in enumerate(HASH_ALGORITHMS):
        hash_value = bytes([index]) * 64
        db.files.insert_db_hash_id_by_hash_value(hash_value, algorithm)
        assert db.files._check_db_hash_id_by_hash_value(hash_value, algorithm) is True

    db.refresh_current_files_hashs()

    for index, algorithm in enumerate(HASH_ALGORITHMS):
        hash_value = bytes([index]) * 64
        assert db.files._check_db_hash_id_by_hash_value(hash_value, algorithm) is False


def test_refresh_current_files_hashs_propagates_worker_errors(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(algorithm: str) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(db, "_refresh_current_files_hashs", boom)

    with pytest.raises(RuntimeError):
        db.refresh_current_files_hashs()


def test_insert_gallery_file_hash_reads_file_once_for_all_algorithms(
    db: H2HDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gallery_name = "artist - single read gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db_file_ids_by_name = db.files._insert_gallery_files(db_gallery_id, ["page.bin"])
    db_file_id = db_file_ids_by_name["page.bin"]

    file_path = tmp_path / "page.bin"
    file_path.write_bytes(b"page content")

    import h2hdb.table_files_dbids as table_files_dbids_module
    from h2hdb.settings import hash_multiple_by_file

    call_count = 0

    def counting_hash_multiple_by_file(
        file_path_arg: str, algorithms: dict[str, int]
    ) -> dict[str, bytes]:
        nonlocal call_count
        call_count += 1
        return hash_multiple_by_file(file_path_arg, algorithms)

    monkeypatch.setattr(
        table_files_dbids_module,
        "hash_multiple_by_file",
        counting_hash_multiple_by_file,
    )

    db.files._insert_gallery_file_hash(db_file_id, str(file_path))

    assert call_count == 1
    for algorithm in HASH_ALGORITHMS:
        assert db.files._check_hash_value_by_file_id(db_file_id, algorithm) is True


def test_refresh_current_cbz_files_removes_only_orphaned_files(
    sqlite_config: H2HDBConfig, tmp_path: Path
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    (cbz_path / "kept.cbz").write_bytes(b"kept")
    (cbz_path / "orphan.cbz").write_bytes(b"orphan")
    sqlite_config.h2h.cbz_path = str(cbz_path)

    with H2HDB(config=sqlite_config) as db:
        db._refresh_current_cbz_files({"kept"})

    assert (cbz_path / "kept.cbz").exists()
    assert not (cbz_path / "orphan.cbz").exists()
