"""Characterization tests for H2HDB.insert_h2h_download().

These pin today's externally-observable behavior of the pipeline (gallery
scanning/sorting, global content-ownership reconciliation, CBZ compression,
and duplicate-spam-image exclusion) so that extracting pieces of it into
smaller methods doesn't silently change behavior.
"""

import hashlib
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from PIL import Image

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.compress_gallery_to_cbz import gallery_name_to_cbz_file_name
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
    file_hashes = sorted(hashlib.sha512(content).digest() for content in file_contents)
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
    db: H2HDB, download_path: Path, tmp_path: Path
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
    # artist_ratio): files_hashs_sha512 sees the same hash >=3 times, and the
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
    content_y_sha512 = hashlib.sha512(content_y).digest()
    db.files.insert_db_hash_id_by_hash_values({content_y_sha512}, "sha512")
    db.files._update_gallery_file_hash_by_db_hash_id(
        a_page_id,
        db.files.get_db_hash_id_by_hash_value(content_y_sha512, "sha512"),
        "sha512",
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
