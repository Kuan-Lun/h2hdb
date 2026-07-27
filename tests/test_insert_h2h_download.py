"""Characterization tests for H2HDB.insert_h2h_download().

These pin today's externally-observable behavior of the pipeline (gallery
scanning/sorting, global content-ownership reconciliation, CBZ compression,
and duplicate-spam-image exclusion) so that extracting pieces of it into
smaller methods doesn't silently change behavior.
"""

import hashlib
import io
import zipfile
from collections.abc import Callable, Iterator
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from h2hdb import H2HDB, H2HDBConfig
from h2hdb import cbz_files as cbz_files_module
from h2hdb import h2hdb_h2hdb as h2hdb_h2hdb_module
from h2hdb.cbz_files import CBZCompressionOutcome, CBZCompressionSummary
from h2hdb.compress_gallery_to_cbz import gallery_name_to_cbz_file_name
from h2hdb.hash_dict import FILE_CONTENT_HASH_ALGORITHM
from h2hdb.settings import CBZ_GROUPING, CBZ_SORT


def _write_galleryinfo(
    gallery_folder: Path,
    *,
    title: str,
    upload_time: str = "2024-01-01 00:00",
    uploaded_by: str = "tester",
    downloaded: str = "2024-01-02 00:00",
    tags: str = "language:english",
    pages: int = 0,
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
    for page in range(pages):
        (gallery_folder / f"{page:03d}.jpg").write_bytes(_make_jpeg_bytes(page))


def _make_jpeg_bytes(seed: int) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), color=(seed % 256, 0, 0)).save(buf, "JPEG")
    return buf.getvalue()


def _effective_content_hash(*file_contents: bytes) -> bytes:
    file_hashes = sorted(hashlib.sha256(content).digest() for content in file_contents)
    return hashlib.sha256(b"".join(file_hashes)).digest()


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


def test_insert_h2h_download_inserts_new_galleries_and_is_idempotent(
    db: H2HDB, download_path: Path
) -> None:
    _write_galleryinfo(download_path / "700001", title="Gallery One")
    _write_galleryinfo(download_path / "700002", title="Gallery Two")

    assert db.insert_h2h_download() is True
    assert sorted(db.gallery_gids.get_gids()) == [700001, 700002]

    # Nothing changed on disk, so the second pass must not find new work.
    assert db.insert_h2h_download() is False
    assert sorted(db.gallery_gids.get_gids()) == [700001, 700002]


def test_insert_h2h_download_reimports_changed_galleryinfo_with_sha256(
    db: H2HDB, download_path: Path
) -> None:
    gallery_folder = download_path / "700003"
    _write_galleryinfo(gallery_folder, title="Original title")
    galleryinfo_path = gallery_folder / "galleryinfo.txt"

    assert db.insert_h2h_download() is True

    updated_text = galleryinfo_path.read_text(encoding="utf-8").replace(
        "Title: Original title", "Title: Updated title"
    )
    galleryinfo_path.write_text(updated_text, encoding="utf-8")

    assert db.insert_h2h_download() is True
    assert db.gallery_titles.get_title_by_gallery_name("700003") == "Updated title"

    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name("700003")
    db_file_id = db.files._get_db_file_id(db_gallery_id, "galleryinfo.txt")
    assert (
        db.files.get_hash_value_by_file_id(db_file_id, FILE_CONTENT_HASH_ALGORITHM)
        == hashlib.sha256(galleryinfo_path.read_bytes()).digest()
    )
    assert db.insert_h2h_download() is False


def test_insert_h2h_download_emits_perf_debug_stages(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sqlite_config.h2h.download_path = download_path
    _write_galleryinfo(download_path / "700099", title="Profiled Gallery")

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        debug_messages: list[str] = []
        monkeypatch.setattr(instance.logger, "debug", debug_messages.append)

        assert instance.insert_h2h_download() is True

    perf_messages = [
        message for message in debug_messages if message.startswith("PERF ")
    ]
    for expected_message in (
        "event=start stage=insert_h2h_download",
        "event=end stage=sql_insert",
        "event=end stage=gallery_files",
        "event=end stage=file_byte_hashing",
        "event=end stage=hash_association_insert",
        "event=end stage=final_dedup",
        "event=end stage=cleanup_file_hashes",
        "event=end stage=insert_h2h_download",
    ):
        assert any(expected_message in message for message in perf_messages)


def test_insert_h2h_download_creates_cbz_files_when_cbz_path_configured(
    db: H2HDB, download_path: Path, tmp_path: Path
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path

    gallery_folder = download_path / "700003"
    _write_galleryinfo(gallery_folder, title="Gallery Three", pages=1)

    assert db.insert_h2h_download() is True

    cbz_file = cbz_path / gallery_name_to_cbz_file_name("700003")
    assert cbz_file.exists()
    with zipfile.ZipFile(cbz_file) as cbz:
        assert set(cbz.namelist()) == {"galleryinfo.txt", "000.jpg"}


def test_compress_gallery_to_cbz_reports_created_rebuilt_and_unchanged(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path

    gallery_folder = download_path / "700030"
    _write_galleryinfo(gallery_folder, title="CBZ Outcome Gallery", pages=1)
    expected_names = frozenset({"galleryinfo.txt", "000.jpg"})
    instance = H2HDB(config=sqlite_config)

    assert (
        instance.cbz.compress_gallery_to_cbz(
            gallery_folder, set(), expected_names, None
        )
        == CBZCompressionOutcome.created
    )
    assert (
        instance.cbz.compress_gallery_to_cbz(
            gallery_folder, set(), expected_names, None
        )
        == CBZCompressionOutcome.unchanged
    )

    cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_folder.name)
    with zipfile.ZipFile(cbz_file, "w") as cbz:
        cbz.writestr("unexpected.txt", "stale")

    assert (
        instance.cbz.compress_gallery_to_cbz(
            gallery_folder, set(), expected_names, None
        )
        == CBZCompressionOutcome.rebuilt
    )
    with zipfile.ZipFile(cbz_file) as cbz:
        assert set(cbz.namelist()) == expected_names


def test_insert_h2h_download_logs_cbz_batch_outcome_counts(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path
    sqlite_config.h2h.cbz_scrub_batch_size = 0
    _write_galleryinfo(download_path / "700031", title="CBZ Log Gallery", pages=1)

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        info_messages: list[str] = []
        monkeypatch.setattr(instance.logger, "info", info_messages.append)

        assert instance.insert_h2h_download() is True

        assert any(
            "Provisional CBZ check for gallery chunk 1/1 completed: "
            "checked=1 created=1 rebuilt=0 unchanged=0" in message
            for message in info_messages
        )
        assert any(
            "Final CBZ chunk 1/1 completed: "
            "checked=1 created=0 rebuilt=0 unchanged=1" in message
            for message in info_messages
        )
        assert any(
            "CBZ processing totals: compression_checks=2 create_operations=1 "
            "rebuild_operations=0 unchanged_checks=1 "
            "write_operations=1 provisional_writes=1 final_writes=0 "
            "integrity_repair_writes=0 "
            "integrity_state_db_gallery_ids_invalidated=0."
            in message
            for message in info_messages
        )


def test_final_cbz_rebuild_invalidates_prior_integrity_state(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path
    sqlite_config.h2h.cbz_scrub_batch_size = 1
    gallery_name = "700033"
    _write_galleryinfo(
        download_path / gallery_name,
        title="CBZ Baseline Invalidation Gallery",
        pages=1,
    )

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        assert instance.insert_h2h_download() is True

        db_gallery_id = instance.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        assert instance.cbz_integrity._get_hash(db_gallery_id) is not None

        cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
        with zipfile.ZipFile(cbz_file) as cbz:
            original_members = {
                member_name: cbz.read(member_name) for member_name in cbz.namelist()
            }
        with zipfile.ZipFile(cbz_file, "w") as cbz:
            for member_name, content in original_members.items():
                cbz.writestr(member_name, content)
            cbz.writestr("unexpected.txt", b"stale")

        info_messages: list[str] = []
        debug_messages: list[str] = []
        monkeypatch.setattr(instance.logger, "info", info_messages.append)
        monkeypatch.setattr(instance.logger, "debug", debug_messages.append)

        assert instance.insert_h2h_download() is False

        assert any(
            "Final CBZ chunk 1/1 completed: "
            "checked=1 created=0 rebuilt=1 unchanged=0" in message
            for message in info_messages
        )
        assert any(
            "CBZ integrity scrub completed: checked=1 verified=0 "
            "baselines_created=1 unreadable=0 hash_mismatches=0 rebuilt=0"
            in message
            for message in info_messages
        )
        assert any(
            "CBZ integrity state invalidated before CBZ write dispatch: "
            "planned_written_db_gallery_ids=1 "
            "prior_state_db_gallery_ids=1." in message
            for message in debug_messages
        )
        assert any(
            "integrity_repair_writes=0 "
            "integrity_state_db_gallery_ids_invalidated=1."
            in message
            for message in info_messages
        )


def test_cbz_write_invalidates_integrity_state_before_a_later_chunk_fails(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path
    sqlite_config.h2h.cbz_scrub_batch_size = 2
    gallery_names = ["700034", "700035"]
    for index, gallery_name in enumerate(gallery_names):
        gallery_folder = download_path / gallery_name
        _write_galleryinfo(
            gallery_folder,
            title=f"Interrupted CBZ {gallery_name}",
            pages=1,
        )
        (gallery_folder / "000.jpg").write_bytes(_make_jpeg_bytes(index * 100))

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        assert instance.insert_h2h_download() is True

        db_gallery_ids_by_name = {
            gallery_name: instance.gallery_ids._get_db_gallery_id_by_gallery_name(
                gallery_name
            )
            for gallery_name in gallery_names
        }
        for db_gallery_id in db_gallery_ids_by_name.values():
            assert instance.cbz_integrity._get_hash(db_gallery_id) is not None

        for gallery_name in gallery_names:
            cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
            with zipfile.ZipFile(cbz_file) as cbz:
                original_members = {
                    member_name: cbz.read(member_name)
                    for member_name in cbz.namelist()
                }
            with zipfile.ZipFile(cbz_file, "w") as cbz:
                for member_name, content in original_members.items():
                    cbz.writestr(member_name, content)
                cbz.writestr("unexpected.txt", b"stale")

        original_chunk_list = h2hdb_h2hdb_module.chunk_list

        def use_one_gallery_per_final_chunk(
            input_list: list[Path], chunk_size: int
        ) -> list[list[Path]]:
            if chunk_size == 1000 * h2hdb_h2hdb_module.POOL_CPU_LIMIT:
                return original_chunk_list(input_list, 1)
            return original_chunk_list(input_list, chunk_size)

        monkeypatch.setattr(
            h2hdb_h2hdb_module,
            "chunk_list",
            use_one_gallery_per_final_chunk,
        )

        first_completed_gallery: str | None = None
        compression_call_count = 0
        original_compress = instance.cbz.compress_galleries_to_cbz

        def interrupt_second_final_chunk(
            gallery_folders: list[Path],
            exclude_hashs: set[bytes],
            pool: Pool,
            *,
            invalidate_integrity_state: Callable[[set[int]], int],
            force_rebuild: bool = False,
        ) -> CBZCompressionSummary:
            nonlocal compression_call_count, first_completed_gallery
            compression_call_count += 1
            if compression_call_count == 2:
                raise RuntimeError("simulated forced termination")
            first_completed_gallery = gallery_folders[0].name
            return original_compress(
                gallery_folders,
                exclude_hashs,
                pool,
                invalidate_integrity_state=invalidate_integrity_state,
                force_rebuild=force_rebuild,
            )

        monkeypatch.setattr(
            instance.cbz,
            "compress_galleries_to_cbz",
            interrupt_second_final_chunk,
        )

        with pytest.raises(RuntimeError, match="simulated forced termination"):
            instance.insert_h2h_download()

        assert first_completed_gallery is not None
        first_completed_id = db_gallery_ids_by_name[first_completed_gallery]
        not_started_name = next(
            name for name in gallery_names if name != first_completed_gallery
        )
        assert instance.cbz_integrity._get_hash(first_completed_id) is None
        assert (
            instance.cbz_integrity._get_hash(db_gallery_ids_by_name[not_started_name])
            is not None
        )


def test_cbz_scrub_force_rebuilds_hash_mismatch_with_unchanged_member_names(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path
    sqlite_config.h2h.cbz_scrub_batch_size = 1
    gallery_name = "700032"
    _write_galleryinfo(
        download_path / gallery_name,
        title="CBZ Scrub Repair Gallery",
        pages=1,
    )

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        assert instance.insert_h2h_download() is True

        cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
        with zipfile.ZipFile(cbz_file) as cbz:
            original_members = {
                member_name: cbz.read(member_name) for member_name in cbz.namelist()
            }

        with zipfile.ZipFile(cbz_file, "w") as cbz:
            for member_name in original_members:
                cbz.writestr(member_name, b"tampered")

        info_messages: list[str] = []
        warning_messages: list[str] = []
        monkeypatch.setattr(instance.logger, "info", info_messages.append)
        monkeypatch.setattr(instance.logger, "warning", warning_messages.append)
        assert instance.insert_h2h_download() is False

        with zipfile.ZipFile(cbz_file) as cbz:
            repaired_members = {
                member_name: cbz.read(member_name) for member_name in cbz.namelist()
            }
        assert repaired_members == original_members

        db_gallery_id = instance.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        assert (
            instance.cbz_integrity._get_hash(db_gallery_id)
            == hashlib.sha256(cbz_file.read_bytes()).digest()
        )
        assert any(
            "CBZ integrity scrub completed: checked=1 verified=0 "
            "baselines_created=0 unreadable=0 hash_mismatches=1 "
            "repair_checked=1 repair_created=0 repair_rebuilt=1" in message
            for message in info_messages
        )
        assert any(
            f"db_gallery_id={db_gallery_id} path={str(cbz_file)!r} "
            "reason=sha256_mismatch expected_sha256=0x" in message
            and " actual_sha256=0x" in message
            for message in warning_messages
        )


def test_interrupted_integrity_repair_keeps_known_good_hash(
    sqlite_config: H2HDBConfig,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    sqlite_config.h2h.download_path = download_path
    sqlite_config.h2h.cbz_path = cbz_path
    sqlite_config.h2h.cbz_scrub_batch_size = 1
    gallery_name = "700036"
    _write_galleryinfo(
        download_path / gallery_name,
        title="Interrupted CBZ Integrity Repair",
        pages=1,
    )

    with H2HDB(config=sqlite_config) as instance:
        instance.create_main_tables()
        assert instance.insert_h2h_download() is True

        db_gallery_id = instance.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        known_good_hash = instance.cbz_integrity._get_hash(db_gallery_id)
        assert known_good_hash is not None

        cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
        with zipfile.ZipFile(cbz_file) as cbz:
            original_members = {
                member_name: cbz.read(member_name) for member_name in cbz.namelist()
            }
        with zipfile.ZipFile(cbz_file, "w") as cbz:
            for member_name in original_members:
                cbz.writestr(member_name, b"tampered")

        original_run_in_parallel = cbz_files_module.run_in_parallel

        def fail_before_integrity_repair_write(
            pool: Pool,
            fun: Callable[..., Any],
            args: list[tuple[Any, ...]],
        ) -> list[Any]:
            if fun is cbz_files_module.compress_gallery_to_cbz_worker and args:
                raise RuntimeError("simulated interruption before repair write")
            return original_run_in_parallel(pool, fun, args)

        monkeypatch.setattr(
            cbz_files_module,
            "run_in_parallel",
            fail_before_integrity_repair_write,
        )
        with pytest.raises(
            RuntimeError,
            match="simulated interruption before repair write",
        ):
            instance.insert_h2h_download()

        assert instance.cbz_integrity._get_hash(db_gallery_id) == known_good_hash
        assert instance.cbz_integrity._get_last_verified_time(db_gallery_id) is None

        monkeypatch.setattr(
            cbz_files_module,
            "run_in_parallel",
            original_run_in_parallel,
        )
        assert instance.insert_h2h_download() is False
        with zipfile.ZipFile(cbz_file) as cbz:
            repaired_members = {
                member_name: cbz.read(member_name) for member_name in cbz.namelist()
            }
        assert repaired_members == original_members


def test_insert_h2h_download_creates_provisional_cbz_between_progress_chunks(
    db: H2HDB,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path
    monkeypatch.setattr(
        "h2hdb.h2hdb_h2hdb.PROGRESSIVE_GALLERY_CHUNK_SIZE",
        1,
    )

    for gallery_name, seed in (("700006", 20), ("700007", 220)):
        folder = download_path / gallery_name
        _write_galleryinfo(folder, title=f"Progressive {gallery_name}")
        (folder / "001.jpg").write_bytes(_make_jpeg_bytes(seed))

    seen_chunks: list[list[str]] = []
    original_insert = db._insert_gallery_chunk_with_split_retry

    def observe_before_inserting_next_chunk(
        gallery_chunk: list[Path],
    ) -> list[bool]:
        if seen_chunks:
            first_gallery_name = seen_chunks[0][0]
            first_cbz = cbz_path / gallery_name_to_cbz_file_name(first_gallery_name)
            assert first_cbz.exists()
            with zipfile.ZipFile(first_cbz) as cbz:
                assert set(cbz.namelist()) == {"galleryinfo.txt", "001.jpg"}
        seen_chunks.append([folder.name for folder in gallery_chunk])
        return original_insert(gallery_chunk)

    monkeypatch.setattr(
        db,
        "_insert_gallery_chunk_with_split_retry",
        observe_before_inserting_next_chunk,
    )

    assert db.insert_h2h_download() is True

    assert len(seen_chunks) == 2
    assert all(len(chunk) == 1 for chunk in seen_chunks)
    assert db.gallery_deduplication.get_duplicate_warning_db_gallery_ids() == []
    for gallery_name in {"700006", "700007"}:
        assert db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name) > 0
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
        assert cbz_file.exists()
        with zipfile.ZipFile(cbz_file) as cbz:
            assert set(cbz.namelist()) == {"galleryinfo.txt", "001.jpg"}


def test_insert_h2h_download_keeps_galleryinfo_only_gallery_eligible_for_cbz(
    db: H2HDB, download_path: Path, tmp_path: Path
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path

    gallery_name = "700005"
    _write_galleryinfo(
        download_path / gallery_name,
        title="Gallery With Metadata Only",
    )

    assert db.insert_h2h_download() is True

    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
    assert db_gallery_id > 0
    assert db.gallery_deduplication._get_all_hashes("gallery_content_hashes") == {}
    assert db.gallery_deduplication._get_all_hashes("gallery_full_content_hashes") == {}
    assert db.gallery_deduplication.get_duplicate_warning_db_gallery_ids() == []

    cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
    assert cbz_file.exists()
    with zipfile.ZipFile(cbz_file) as cbz:
        assert cbz.namelist() == ["galleryinfo.txt"]


def test_insert_h2h_download_groups_cbz_by_upload_time_when_configured(
    db: H2HDB,
    download_path: Path,
    tmp_path: Path,
) -> None:
    # cbz_grouping besides "flat" needs each gallery's upload_time, batched
    # ahead of dispatch to the worker pool in compress_galleries_to_cbz --
    # pins that the batched lookup actually reaches the worker correctly.
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path
    db.config.h2h.cbz_grouping = CBZ_GROUPING.date_yyyy_mm_dd

    gallery_folder = download_path / "700004"
    _write_galleryinfo(
        gallery_folder,
        title="Gallery Four",
        upload_time="2023-05-17 00:00",
        pages=1,
    )

    assert db.insert_h2h_download() is True

    cbz_file = cbz_path / "2023" / "05" / "17" / gallery_name_to_cbz_file_name("700004")
    assert cbz_file.exists()
    with zipfile.ZipFile(cbz_file) as cbz:
        assert set(cbz.namelist()) == {"galleryinfo.txt", "000.jpg"}

    # Changing the grouping must move the expected output rather than retaining
    # an old same-named CBZ that a basename-only lookup could later scrub.
    db.config.h2h.cbz_grouping = CBZ_GROUPING.date_yyyy
    assert db.insert_h2h_download() is False

    regrouped_cbz_file = (
        cbz_path / "2023" / gallery_name_to_cbz_file_name("700004")
    )
    assert not cbz_file.exists()
    assert regrouped_cbz_file.exists()


def test_insert_h2h_download_excludes_and_recovers_duplicate_spam_images(
    db: H2HDB,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path

    # An identical image shared by 3 galleries from 3 different artists trips
    # the duplicate/spam-image exclusion (duplicated_hash_values_by_count_
    # artist_ratio): files_hashs_sha256 sees the same hash >=3 times, and the
    # distinct-artist-count-to-max-artist-count ratio exceeds 2.
    shared_image = _make_jpeg_bytes(0)
    gallery_names: list[str] = []
    for i, artist in enumerate(["artist-a", "artist-b", "artist-c"]):
        folder = download_path / f"70001{i}"
        _write_galleryinfo(folder, title=f"Gallery {i}", tags=f"artist:{artist}")
        (folder / "001.jpg").write_bytes(shared_image)
        gallery_names.append(folder.name)

    saw_unreconciled_provisional_cbzs = False
    original_filter = db._filter_galleries_for_deduplication

    def observe_provisional_cbzs_before_final_reconciliation(
        galleries: list[Path],
        exclude_hashs: set[bytes],
    ) -> list[Path]:
        nonlocal saw_unreconciled_provisional_cbzs
        if not saw_unreconciled_provisional_cbzs:
            for name in gallery_names:
                provisional_cbz = cbz_path / gallery_name_to_cbz_file_name(name)
                assert provisional_cbz.exists()
                with zipfile.ZipFile(provisional_cbz) as cbz:
                    assert set(cbz.namelist()) == {
                        "galleryinfo.txt",
                        "001.jpg",
                    }
            saw_unreconciled_provisional_cbzs = True
        return original_filter(galleries, exclude_hashs)

    monkeypatch.setattr(
        db,
        "_filter_galleries_for_deduplication",
        observe_provisional_cbzs_before_final_reconciliation,
    )

    assert db.insert_h2h_download() is True
    assert saw_unreconciled_provisional_cbzs

    db_gallery_ids = {
        db.gallery_ids._get_db_gallery_id_by_gallery_name(name)
        for name in gallery_names
    }
    assert len(db_gallery_ids) == len(gallery_names)
    assert db.gallery_deduplication._get_all_hashes("gallery_content_hashes") == {}
    assert db.gallery_deduplication.get_duplicate_warning_db_gallery_ids() == []

    for name in gallery_names:
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(name)
        with zipfile.ZipFile(cbz_file) as cbz:
            assert cbz.namelist() == ["galleryinfo.txt"]

    # A no-op pass must use the same final exclusion snapshot and preserve both
    # contentless eligibility and the metadata-only CBZ result.
    assert db.insert_h2h_download() is False

    for name in gallery_names:
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(name)
        with zipfile.ZipFile(cbz_file) as cbz:
            assert cbz.namelist() == ["galleryinfo.txt"]


def test_insert_h2h_download_reconciles_migrated_owner_before_compression(
    db: H2HDB,
    download_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One run must release A's old hash before judging B's claim to it.

    A and B are deliberately collected in separate metadata batches. This
    catches the old snapshot bug where B lost X to the higher-priority A even
    though A's final claim had migrated from X to Y.
    """

    monkeypatch.setattr(
        "h2hdb.h2hdb_h2hdb.GALLERY_METADATA_BATCH_SIZE",
        1,
    )
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path

    content_x = _make_jpeg_bytes(21)
    content_y = _make_jpeg_bytes(220)
    a_name = "700050"
    b_name = "700051"
    a_folder = download_path / a_name
    _write_galleryinfo(
        a_folder,
        title="A title deliberately much longer than B",
    )
    a_page = a_folder / "001.jpg"
    a_page.write_bytes(content_x)

    assert db.insert_h2h_download() is True

    a_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(a_name)
    assert db.gallery_deduplication._get_all_hashes("gallery_content_hashes") == {
        a_id: _effective_content_hash(content_x)
    }

    # The normal ingest contract treats an unchanged galleryinfo.txt as an
    # unchanged gallery. Update A's already-known file hash directly so this
    # setup preserves A's db_gallery_id and its incumbent ownership of X while
    # presenting a final claim for Y to the next full main run.
    a_page.write_bytes(content_y)
    a_page_id = db.files._get_db_file_ids_by_gallery_ids_for_name([a_id], a_page.name)[
        a_id
    ]
    content_y_sha256 = hashlib.sha256(content_y).digest()
    db.files.insert_db_hash_id_by_hash_values(
        {content_y_sha256}, FILE_CONTENT_HASH_ALGORITHM
    )
    db.files._update_gallery_file_hash_by_db_hash_id(
        a_page_id,
        db.files.get_db_hash_id_by_hash_value(
            content_y_sha256, FILE_CONTENT_HASH_ALGORITHM
        ),
        FILE_CONTENT_HASH_ALGORITHM,
    )

    # Force the final compression phase to materialize A from its migrated
    # source bytes rather than reuse the prior same-name CBZ.
    (cbz_path / gallery_name_to_cbz_file_name(a_name)).unlink()

    b_folder = download_path / b_name
    _write_galleryinfo(b_folder, title="B")
    (b_folder / "001.jpg").write_bytes(content_x)

    # This single successful return must be the convergence point; no repair
    # run is allowed or needed.
    assert db.insert_h2h_download() is True

    assert db.gallery_ids._get_db_gallery_id_by_gallery_name(a_name) == a_id
    b_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(b_name)
    assert db.gallery_deduplication._get_all_hashes("gallery_content_hashes") == {
        a_id: _effective_content_hash(content_y),
        b_id: _effective_content_hash(content_x),
    }
    assert db.gallery_deduplication.get_duplicate_warning_db_gallery_ids() == []

    for gallery_name, expected_content in (
        (a_name, content_y),
        (b_name, content_x),
    ):
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(gallery_name)
        assert cbz_file.exists()
        with zipfile.ZipFile(cbz_file) as cbz:
            assert set(cbz.namelist()) == {"galleryinfo.txt", "001.jpg"}
            with (
                Image.open(io.BytesIO(cbz.read("001.jpg"))) as actual_image,
                Image.open(io.BytesIO(expected_content)) as expected_image,
            ):
                actual_red = actual_image.convert("RGB").getpixel((0, 0))[0]
                expected_red = expected_image.convert("RGB").getpixel((0, 0))[0]
                assert abs(actual_red - expected_red) <= 2


def test_insert_h2h_download_resolves_three_way_content_hash_collision(
    db: H2HDB, download_path: Path, tmp_path: Path
) -> None:
    """Three brand-new galleries in one chunk share byte-identical file
    content. Only the highest-priority gallery (longest title, here) should
    end up with a CBZ after a full run; the others must lose the race."""
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = cbz_path

    # Same tags/artist for all three (no "artist:" tag at all) so the
    # unrelated spam-image exclusion mechanism never trips -- this is purely
    # a content-hash race between three otherwise-ordinary galleries.
    shared_image = _make_jpeg_bytes(0)
    entries = [
        ("700040", "short"),
        ("700041", "a somewhat longer title"),
        ("700042", "the longest title of them all here"),
    ]
    for name, title in entries:
        folder = download_path / name
        _write_galleryinfo(folder, title=title)
        (folder / "001.jpg").write_bytes(shared_image)

    assert db.insert_h2h_download() is True

    winner_name, loser_names = "700042", ["700040", "700041"]
    winner_cbz = cbz_path / gallery_name_to_cbz_file_name(winner_name)
    assert winner_cbz.exists()
    with zipfile.ZipFile(winner_cbz) as cbz:
        assert set(cbz.namelist()) == {"galleryinfo.txt", "001.jpg"}

    for loser_name in loser_names:
        loser_cbz = cbz_path / gallery_name_to_cbz_file_name(loser_name)
        assert not loser_cbz.exists()

    winner_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(winner_name)
    loser_ids = {
        db.gallery_ids._get_db_gallery_id_by_gallery_name(name) for name in loser_names
    }
    assert (
        set(db.gallery_deduplication.get_duplicate_warning_db_gallery_ids())
        == loser_ids
    )
    assert winner_id not in loser_ids


def test_insert_h2h_download_sorts_by_upload_time_descending(
    db: H2HDB, download_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.config.h2h.cbz_sort = CBZ_SORT.upload_time
    _write_galleryinfo(
        download_path / "700020", title="Old", upload_time="2024-01-01 00:00"
    )
    _write_galleryinfo(
        download_path / "700021", title="New", upload_time="2024-06-01 00:00"
    )
    _write_galleryinfo(
        download_path / "700022", title="Mid", upload_time="2024-03-01 00:00"
    )

    seen_orders: list[list[str]] = []
    original = db._insert_gallery_chunk_with_split_retry

    def recording(gallery_chunk: list[Path]) -> list[bool]:
        seen_orders.append([folder.name for folder in gallery_chunk])
        return original(gallery_chunk)

    monkeypatch.setattr(db, "_insert_gallery_chunk_with_split_retry", recording)

    db.insert_h2h_download()

    assert seen_orders == [["700021", "700022", "700020"]]


def test_insert_h2h_download_sorts_by_pages_distance_from_adjustment(
    db: H2HDB, download_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.config.h2h.cbz_sort = "pages+5"  # type: ignore[assignment]
    _write_galleryinfo(download_path / "700030", title="Close", pages=5)
    _write_galleryinfo(download_path / "700031", title="Near", pages=1)
    _write_galleryinfo(download_path / "700032", title="Far", pages=10)

    seen_orders: list[list[str]] = []
    original = db._insert_gallery_chunk_with_split_retry

    def recording(gallery_chunk: list[Path]) -> list[bool]:
        seen_orders.append([folder.name for folder in gallery_chunk])
        return original(gallery_chunk)

    monkeypatch.setattr(db, "_insert_gallery_chunk_with_split_retry", recording)

    db.insert_h2h_download()

    assert seen_orders == [["700030", "700031", "700032"]]
