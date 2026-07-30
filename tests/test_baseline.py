import hashlib
import zipfile
from collections.abc import Iterator
from multiprocessing.pool import Pool
from pathlib import Path

import pytest
from h2h_galleryinfo_parser import GalleryInfoParser, parse_galleryinfo

from h2hdb import H2HDB, H2HDBConfig
from h2hdb import h2hdb_h2hdb as h2hdb_h2hdb_module
from h2hdb import table_files_dbids as table_files_dbids_module
from h2hdb import table_tags as table_tags_module
from h2hdb.gallery_source_manifest import (
    GalleryChange,
    build_gallery_source_manifest,
)
from h2hdb.hash_dict import FILE_CONTENT_HASH_ALGORITHM
from h2hdb.information import FileInformation
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


def test_gallery_source_manifest_is_stable_and_detects_rename(tmp_path: Path) -> None:
    gallery_folder = tmp_path / "700090"
    _write_galleryinfo(gallery_folder, title="Source Manifest")
    original_file = gallery_folder / "001.jpg"
    original_file.write_bytes(b"same file bytes")

    first = build_gallery_source_manifest(parse_galleryinfo(gallery_folder))
    assert first == build_gallery_source_manifest(parse_galleryinfo(gallery_folder))

    original_file.rename(gallery_folder / "renamed.jpg")
    assert first != build_gallery_source_manifest(parse_galleryinfo(gallery_folder))


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
    hash_value = bytes.fromhex("ab" * 32)

    db.files.insert_db_hash_id_by_hash_value(hash_value, FILE_CONTENT_HASH_ALGORITHM)

    assert (
        db.files._check_db_hash_id_by_hash_value(
            hash_value, FILE_CONTENT_HASH_ALGORITHM
        )
        is True
    )
    db_hash_id = db.files.get_db_hash_id_by_hash_value(
        hash_value, FILE_CONTENT_HASH_ALGORITHM
    )
    assert isinstance(db_hash_id, int)
    assert (
        db.files.get_hash_value_by_db_hash_id(db_hash_id, FILE_CONTENT_HASH_ALGORITHM)
        == hash_value
    )
    with pytest.raises(DatabaseDuplicateKeyError):
        db.files.insert_db_hash_id_by_hash_value(
            hash_value, FILE_CONTENT_HASH_ALGORITHM
        )


@pytest.mark.parametrize("digest_size", [31, 33])
def test_hash_value_rejects_non_sha256_digest_lengths(
    db: H2HDB, digest_size: int
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"sha256 digest must be exactly 32 bytes, got {digest_size}",
    ):
        db.files.insert_db_hash_id_by_hash_value(
            bytes(digest_size), FILE_CONTENT_HASH_ALGORITHM
        )


def test_file_hash_schema_uses_only_sha256_tables(db: H2HDB) -> None:
    with db.SQLConnector() as connector:
        match db.config.database.sql_type.lower():
            case "mariadb":
                table_rows = connector.fetch_all(
                    """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    """,
                    (db.config.database.database,),
                )
                hash_column = connector.fetch_one(
                    """
                    SELECT DATA_TYPE, CHARACTER_OCTET_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = 'files_hashs_sha256_dbids'
                        AND COLUMN_NAME = 'hash_value'
                    """,
                    (db.config.database.database,),
                )
                assert hash_column == ("binary", 32)
            case "sqlite":
                table_rows = connector.fetch_all(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
                create_sql = connector.fetch_one("""
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'table'
                        AND name = 'files_hashs_sha256_dbids'
                    """)[0]
                assert "hash_value BLOB NOT NULL" in create_sql
                assert "CHECK (length(hash_value) = 32)" in create_sql

        assert connector.fetch_all("SELECT sha256 FROM files_hashs LIMIT 0") == []
        assert (
            connector.fetch_all("SELECT * FROM duplicate_hash_in_gallery LIMIT 0") == []
        )

    hash_tables = {
        str(table_name)
        for (table_name,) in table_rows
        if str(table_name).startswith("files_hashs_")
    }
    all_tables = {str(table_name) for (table_name,) in table_rows}
    assert hash_tables == {
        "files_hashs_sha256",
        "files_hashs_sha256_dbids",
    }
    assert {"cbz_hashes", "cbz_verifications"}.isdisjoint(all_tables)
    assert {
        "gallery_source_manifests",
        "pending_cbz_rebuilds",
    }.issubset(all_tables)


def test_bulk_hash_insert_handles_stale_duplicate_read(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    hash_value = bytes.fromhex("cd" * 32)
    db.files.insert_db_hash_id_by_hash_value(hash_value, FILE_CONTENT_HASH_ALGORITHM)

    original_get_hash_ids = db.files._get_db_hash_ids_by_hash_values
    call_count = 0

    def stale_once(hash_values: set[bytes], algorithm: str) -> dict[bytes, int]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}
        return original_get_hash_ids(hash_values, algorithm)

    monkeypatch.setattr(db.files, "_get_db_hash_ids_by_hash_values", stale_once)

    db.files.insert_db_hash_id_by_hash_values({hash_value}, FILE_CONTENT_HASH_ALGORITHM)

    assert (
        db.files._check_db_hash_id_by_hash_value(
            hash_value, FILE_CONTENT_HASH_ALGORITHM
        )
        is True
    )


def test_bulk_hash_insert_split_retries_duplicate_batches(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    duplicate_hash = bytes.fromhex("dd" * 32)
    new_hashes = [bytes([index]) * 32 for index in range(4)]
    db.files.insert_db_hash_id_by_hash_value(
        duplicate_hash, FILE_CONTENT_HASH_ALGORITHM
    )

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
        {duplicate_hash, *new_hashes}, FILE_CONTENT_HASH_ALGORITHM
    )

    for hash_value in new_hashes:
        assert (
            db.files._check_db_hash_id_by_hash_value(
                hash_value, FILE_CONTENT_HASH_ALGORITHM
            )
            is True
        )
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

    gallery_infos = [parse_galleryinfo(path) for path in gallery_folders]

    assert db.insert_gallery_infos(gallery_infos) == [
        GalleryChange.new,
        GalleryChange.new,
    ]

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
        assert (
            db.files._check_hash_value_by_file_id(
                db_file_id, FILE_CONTENT_HASH_ALGORITHM
            )
            is True
        )
        assert sorted(db.gallery_tags.get_tag_pairs_by_gallery_name(gallery_name)) == [
            ("artist", f"batch{index}"),
            ("group", "shared"),
            ("language", "english"),
        ]

    assert db.insert_gallery_infos(gallery_infos) == [
        GalleryChange.unchanged,
        GalleryChange.unchanged,
    ]


def test_get_komga_metadata_does_not_cross_mix_galleries(
    db: H2HDB, tmp_path: Path
) -> None:
    gallery_folders = list[Path]()
    for index in range(2):
        gallery_folder = tmp_path / f"70000{index}"
        _write_galleryinfo(
            gallery_folder,
            title=f"Komga Gallery {index}",
            tags=f"artist:komga{index}, group:shared, language:english",
        )
        gallery_folders.append(gallery_folder)

    gallery_infos = [parse_galleryinfo(path) for path in gallery_folders]
    assert db.insert_gallery_infos(gallery_infos) == [
        GalleryChange.new,
        GalleryChange.new,
    ]

    gallery_name_0 = gallery_infos[0].gallery_name
    gallery_name_1 = gallery_infos[1].gallery_name
    db_gallery_id_0 = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name_0)
    db.gallery_comments._insert_gallery_comment(db_gallery_id_0, "hello world")

    metadata = db.get_komga_metadata([gallery_name_0, gallery_name_1])

    assert metadata[gallery_name_0]["title"] == "Komga Gallery 0"
    assert metadata[gallery_name_0]["summary"] == "hello world"
    assert metadata[gallery_name_1]["title"] == "Komga Gallery 1"
    assert metadata[gallery_name_1]["summary"] == ""

    for index, gallery_name in enumerate((gallery_name_0, gallery_name_1)):
        gid = gallery_infos[index].gid
        authors = metadata[gallery_name]["authors"]
        assert {"name": str(gid), "role": "gid"} in authors
        assert {"name": f"komga{index}", "role": "artist"} in authors
        assert {"name": "shared", "role": "group"} in authors
        assert {"name": "english", "role": "language"} in authors
        assert len(authors) == 4


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

    gallery_infos = [parse_galleryinfo(path) for path in gallery_folders]

    assert db.insert_gallery_infos(gallery_infos) == [
        GalleryChange.new,
        GalleryChange.new,
        GalleryChange.new,
    ]

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
    gallery_info = parse_galleryinfo(gallery_folder)

    assert db.insert_gallery_infos([gallery_info]) == [GalleryChange.new]

    assert sorted(
        db.gallery_tags.get_tag_pairs_by_gallery_name(gallery_info.gallery_name)
    ) == sorted(
        [("artist", "a"), ("artist", "b"), ("group", "c"), ("language", "english")]
    )


def test_hash_association_insert_batches_across_boundary(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(table_files_dbids_module, "HASH_ASSOCIATION_BATCH_SIZE", 2)

    gallery_name = "hash association batching"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    file_names = [f"{index}.jpg" for index in range(3)]
    db_file_ids = db.files._insert_gallery_files(db_gallery_id, file_names)

    hash_values = {
        file_name: hashlib.sha256(file_name.encode()).digest()
        for file_name in file_names
    }
    db.files.insert_db_hash_id_by_hash_values(
        set(hash_values.values()), FILE_CONTENT_HASH_ALGORITHM
    )
    db_hash_ids = db.files._get_db_hash_ids_by_hash_values(
        set(hash_values.values()), FILE_CONTENT_HASH_ALGORITHM
    )
    fileinformations = list[FileInformation]()
    for file_name in file_names:
        fileinformation = FileInformation(Path(file_name), db_file_ids[file_name])
        fileinformation.setdb_hash_id(
            FILE_CONTENT_HASH_ALGORITHM,
            db_hash_ids[hash_values[file_name]],
        )
        fileinformations.append(fileinformation)

    debug_messages = list[str]()
    monkeypatch.setattr(db.logger, "debug", debug_messages.append)
    db.files.insert_hash_value_by_db_hash_ids(fileinformations)

    assert any(
        "event=start stage=sql_insert table=files_hashs_sha256 "
        "rows=3 batch_size=2 batches=2" in message
        for message in debug_messages
    )
    for file_name in file_names:
        assert (
            db.files.get_hash_value_by_file_id(
                db_file_ids[file_name], FILE_CONTENT_HASH_ALGORITHM
            )
            == hash_values[file_name]
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
    # _insert_gallery_files returns the name->db_file_id mapping from a single
    # batched SELECT.
    gallery_folder = tmp_path / "700001"
    _write_galleryinfo(gallery_folder, title="No N+1 Gallery")
    for index in range(5):
        (gallery_folder / f"{index:03d}.jpg").write_bytes(f"image-{index}".encode())
    gallery_info = parse_galleryinfo(gallery_folder)

    call_count = 0
    original_get_db_file_id = db.files._get_db_file_id

    def counting_get_db_file_id(db_gallery_id: int, file_name: str) -> int:
        nonlocal call_count
        call_count += 1
        return original_get_db_file_id(db_gallery_id, file_name)

    monkeypatch.setattr(db.files, "_get_db_file_id", counting_get_db_file_id)

    assert db.insert_gallery_infos([gallery_info]) == [GalleryChange.new]

    assert call_count == 0


def test_insert_gallery_chunk_splits_retry_instead_of_one_by_one(
    db: H2HDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gallery_paths = list[Path]()
    for index in range(4):
        gallery_folder = tmp_path / f"80000{index}"
        _write_galleryinfo(gallery_folder, title=f"Split Retry Gallery {index}")
        gallery_paths.append(gallery_folder)

    original_insert_gallery_infos = db.insert_gallery_infos
    call_sizes = list[int]()
    fail_once = True

    def flaky_insert_gallery_infos(
        galleryinfo_params_list: list[GalleryInfoParser],
    ) -> list[GalleryChange]:
        nonlocal fail_once
        call_sizes.append(len(galleryinfo_params_list))
        if fail_once and len(galleryinfo_params_list) == len(gallery_paths):
            fail_once = False
            raise RuntimeError("simulated transient failure")
        return original_insert_gallery_infos(galleryinfo_params_list)

    monkeypatch.setattr(db, "insert_gallery_infos", flaky_insert_gallery_infos)

    result = db._insert_gallery_chunk_with_split_retry(gallery_paths)

    assert result == [GalleryChange.new] * 4
    # The failing full batch is retried as two halves, not four single-item calls.
    assert call_sizes == [4, 2, 2]


def test_refresh_current_files_hashs_removes_orphaned_sha256(
    db: H2HDB,
) -> None:
    hash_value = bytes.fromhex("ee" * 32)
    db.files.insert_db_hash_id_by_hash_value(hash_value, FILE_CONTENT_HASH_ALGORITHM)
    assert (
        db.files._check_db_hash_id_by_hash_value(
            hash_value, FILE_CONTENT_HASH_ALGORITHM
        )
        is True
    )

    db.refresh_current_files_hashs()

    assert (
        db.files._check_db_hash_id_by_hash_value(
            hash_value, FILE_CONTENT_HASH_ALGORITHM
        )
        is False
    )


def test_refresh_current_files_hashs_propagates_worker_errors(
    db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(algorithm: str) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(db, "_refresh_current_files_hashs", boom)

    with pytest.raises(RuntimeError):
        db.refresh_current_files_hashs()


def test_insert_gallery_file_hash_hashes_and_persists_sha256(
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
        file_path_arg: Path, algorithms: dict[str, int]
    ) -> dict[str, bytes]:
        nonlocal call_count
        call_count += 1
        return hash_multiple_by_file(file_path_arg, algorithms)

    monkeypatch.setattr(
        table_files_dbids_module,
        "hash_multiple_by_file",
        counting_hash_multiple_by_file,
    )

    db.files._insert_gallery_file_hash(db_file_id, file_path)

    assert call_count == 1
    assert (
        db.files.get_hash_value_by_file_id(db_file_id, FILE_CONTENT_HASH_ALGORITHM)
        == hashlib.sha256(b"page content").digest()
    )


def test_refresh_current_cbz_files_removes_only_orphaned_files(
    sqlite_config: H2HDBConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    (cbz_path / "kept.cbz").write_bytes(b"kept")
    (cbz_path / "orphan.cbz").write_bytes(b"orphan")
    legacy_directory = cbz_path / "legacy"
    legacy_directory.mkdir()
    (legacy_directory / "kept.cbz").write_bytes(b"duplicate")
    for tmp_name in ("first", "second"):
        tmp_directory = cbz_path / "tmp" / tmp_name
        tmp_directory.mkdir(parents=True)
        (tmp_directory / "001.jpg").write_bytes(b"temporary")
    sqlite_config.h2h.cbz_path = cbz_path

    with H2HDB(config=sqlite_config) as db:
        info_messages: list[str] = []
        monkeypatch.setattr(db.logger, "info", info_messages.append)
        db.cbz._refresh_current_cbz_files({"kept"})

    assert (cbz_path / "kept.cbz").exists()
    assert not (cbz_path / "orphan.cbz").exists()
    assert not legacy_directory.exists()
    assert not (cbz_path / "tmp").exists()
    assert info_messages == [
        "CBZ output reconciliation completed: scanned_files=5 "
        "expected_cbz_files=1 unexpected_files_removed=4 "
        "empty_directories_removed=4."
    ]


def test_get_stale_cbz_galleries_flags_cbz_containing_newly_excluded_file(
    db: H2HDB, tmp_path: Path
) -> None:
    from h2hdb.compress_gallery_to_cbz import gallery_name_to_cbz_file_name

    gallery_folder = tmp_path / "700001"
    _write_galleryinfo(gallery_folder, title="Stale CBZ Gallery")
    image_content = b"some image bytes"
    (gallery_folder / "001.jpg").write_bytes(image_content)
    gallery_info = parse_galleryinfo(gallery_folder)
    db.insert_gallery_infos([gallery_info])
    gallery_name = gallery_info.gallery_name
    image_hash = hashlib.sha256(image_content).digest()

    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path
    cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
    with zipfile.ZipFile(cbz_file, "w") as cbz:
        cbz.writestr("galleryinfo.txt", "stale")
        cbz.writestr("001.jpg", image_content)

    # image_hash is now excluded, but the existing cbz still contains it.
    with Pool(1) as pool:
        assert db.cbz.get_stale_cbz_galleries({gallery_name}, {image_hash}, pool) == {
            gallery_name
        }


def test_get_stale_cbz_galleries_ignores_cbz_that_already_excludes_file(
    db: H2HDB, tmp_path: Path
) -> None:
    from h2hdb.compress_gallery_to_cbz import gallery_name_to_cbz_file_name

    gallery_folder = tmp_path / "700002"
    _write_galleryinfo(gallery_folder, title="Fresh CBZ Gallery")
    image_content = b"some other image bytes"
    (gallery_folder / "001.jpg").write_bytes(image_content)
    gallery_info = parse_galleryinfo(gallery_folder)
    db.insert_gallery_infos([gallery_info])
    gallery_name = gallery_info.gallery_name
    image_hash = hashlib.sha256(image_content).digest()

    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path
    cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
    with zipfile.ZipFile(cbz_file, "w") as cbz:
        cbz.writestr("galleryinfo.txt", "fresh")
        # 001.jpg already absent, matching the exclusion below.

    with Pool(1) as pool:
        assert (
            db.cbz.get_stale_cbz_galleries({gallery_name}, {image_hash}, pool) == set()
        )


def test_update_redownload_time_to_now_by_gid(db: H2HDB) -> None:
    gallery_name = "artist - redownload time gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=777)
    db.gallery_times._insert_download_time(db_gallery_id, "2000-01-01 00:00:00")

    old_time = db.gallery_times._select_time(
        "galleries_redownload_times", db_gallery_id
    )

    db.update_redownload_time_to_now_by_gid(777)

    new_time = db.gallery_times._select_time(
        "galleries_redownload_times", db_gallery_id
    )
    assert new_time != old_time
    assert new_time.year >= 2024


def test_optimize_database_preserves_data(db: H2HDB) -> None:
    gallery_name = "artist - optimize database gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=888)

    db.optimize_database()

    assert (
        db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name) == db_gallery_id
    )
    assert db.gallery_gids.get_gid_by_gallery_name(gallery_name) == 888


def test_analyze_database_preserves_data(db: H2HDB) -> None:
    gallery_name = "artist - analyze database gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=889)

    db.analyze_database()

    assert (
        db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name) == db_gallery_id
    )
    assert db.gallery_gids.get_gid_by_gallery_name(gallery_name) == 889


def test_todownload_gid_round_trip(db: H2HDB) -> None:
    assert db.get_download_request(111) is None

    first_request = db.request_download(111)
    assert db.get_download_request(111) == first_request
    assert db.get_download_requests() == [first_request]

    second_request = db.request_download(111, "https://e-hentai.org/g/111/abc123def4")
    assert second_request.token != first_request.token
    assert db.get_download_requests() == [second_request]

    third_request = db.request_download(111)
    assert third_request.url == second_request.url
    assert third_request.token != second_request.token

    db.complete_download_request(third_request)
    assert db.get_download_requests() == []


def test_request_download_via_url_derives_gid(db: H2HDB) -> None:
    request = db.request_download(0, "https://e-hentai.org/g/222/abc123def4")

    assert request.gid == 222
    assert db.get_download_requests() == [request]


def test_request_download_rejects_non_positive_gid_without_url(
    db: H2HDB,
) -> None:
    with pytest.raises(ValueError):
        db.request_download(0)


def test_todelete_gid_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - todelete gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=333)

    assert db.is_gallery_deletion_requested(333) is False

    db.request_gallery_deletion(333)
    db.todelete_queue.refresh_todelete_galleries()

    assert db.is_gallery_deletion_requested(333) is True
    with db.SQLConnector() as connector:
        query_result = connector.fetch_all("""
            SELECT galleries_names.full_name
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
            """)
    assert query_result == [(gallery_name,)]


def test_get_pending_download_gids_includes_overdue_redownload(db: H2HDB) -> None:
    gallery_name = "artist - pending download gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    db.gallery_gids._insert_gallery_gid(db_gallery_id, gid=444)

    db.gallery_times._insert_download_time(db_gallery_id, "2000-01-01 00:00:00")
    db.gallery_times._insert_upload_time(db_gallery_id, "2000-01-01 00:00:00")
    db.gallery_times.update_redownload_time(db_gallery_id, "2000-02-01 00:00:00")

    assert 444 in db.get_pending_download_gids()

    db.request_gallery_deletion(444)

    assert 444 not in db.get_pending_download_gids()


def test_pending_gallery_removal_round_trip(db: H2HDB) -> None:
    gallery_name = "artist - pending removal gallery"

    assert db.check_pending_gallery_removal(gallery_name) is False

    db.insert_pending_gallery_removal(gallery_name)

    assert db.check_pending_gallery_removal(gallery_name) is True
    assert gallery_name in db.get_pending_gallery_removals()

    db.delete_pending_gallery_removal(gallery_name)

    assert db.check_pending_gallery_removal(gallery_name) is False
    assert gallery_name not in db.get_pending_gallery_removals()


def test_insert_pending_gallery_removal_rejects_long_name(db: H2HDB) -> None:
    with pytest.raises(ValueError):
        db.insert_pending_gallery_removal("a" * 300)


def test_delete_pending_gallery_removals_deletes_gallery_and_clears_queue(
    db: H2HDB,
) -> None:
    gallery_name = "artist - full removal pipeline gallery"
    db.gallery_ids._insert_gallery_name(gallery_name)
    db.insert_pending_gallery_removal(gallery_name)

    removed = db.pending_removals.recover_pending_gallery_removals(set())

    assert removed == 1
    assert db.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name) is False
    assert db.get_pending_gallery_removals() == []
