"""Characterization tests for H2HDB.insert_h2h_download().

These pin today's externally-observable behavior of the pipeline (gallery
scanning/sorting, CBZ compression, and the duplicate-spam-image exclusion +
stale-CBZ recompression safety net) so that extracting pieces of it into
smaller methods doesn't silently change behavior.
"""

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from PIL import Image

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.compress_gallery_to_cbz import gallery_name_to_cbz_file_name
from h2hdb.settings import CBZ_SORT


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


@pytest.fixture
def download_path(tmp_path: Path) -> Path:
    path = tmp_path / "download"
    path.mkdir()
    return path


@pytest.fixture
def db(db_config: H2HDBConfig, download_path: Path) -> Iterator[H2HDB]:
    db_config.h2h.download_path = str(download_path)
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


def test_insert_h2h_download_creates_cbz_files_when_cbz_path_configured(
    db: H2HDB, download_path: Path, tmp_path: Path
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = str(cbz_path)

    gallery_folder = download_path / "700003"
    _write_galleryinfo(gallery_folder, title="Gallery Three", pages=1)

    assert db.insert_h2h_download() is True

    cbz_file = cbz_path / gallery_name_to_cbz_file_name("700003")
    assert cbz_file.exists()
    with zipfile.ZipFile(cbz_file) as cbz:
        assert set(cbz.namelist()) == {"galleryinfo.txt", "000.jpg"}


def test_insert_h2h_download_excludes_and_recovers_duplicate_spam_images(
    db: H2HDB, download_path: Path, tmp_path: Path
) -> None:
    cbz_path = tmp_path / "cbz"
    cbz_path.mkdir()
    db.config.h2h.cbz_path = str(cbz_path)

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

    assert db.insert_h2h_download() is True

    for name in gallery_names:
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(name)
        with zipfile.ZipFile(cbz_file) as cbz:
            assert cbz.namelist() == ["galleryinfo.txt"]

    # No new galleries on the second pass, so the in-loop exclude_hashs
    # recalculation never runs (it's gated on `any(is_insert_list)`) -- the
    # CBZs would get rebuilt *without* the exclusion if not for the
    # end-of-run "stale CBZ" safety net recomputing the exclusion set
    # unconditionally and recompressing anything that drifted from it.
    assert db.insert_h2h_download() is False

    for name in gallery_names:
        cbz_file = cbz_path / gallery_name_to_cbz_file_name(name)
        with zipfile.ZipFile(cbz_file) as cbz:
            assert cbz.namelist() == ["galleryinfo.txt"]


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

    def recording(gallery_chunk: list[str]) -> list[bool]:
        seen_orders.append([Path(folder).name for folder in gallery_chunk])
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

    def recording(gallery_chunk: list[str]) -> list[bool]:
        seen_orders.append([Path(folder).name for folder in gallery_chunk])
        return original(gallery_chunk)

    monkeypatch.setattr(db, "_insert_gallery_chunk_with_split_retry", recording)

    db.insert_h2h_download()

    assert seen_orders == [["700030", "700031", "700032"]]
