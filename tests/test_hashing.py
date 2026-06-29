import hashlib
import os
import zipfile
from pathlib import Path

import pytest

from h2hdb.compress_gallery_to_cbz import calculate_hash_of_file_in_cbz
from h2hdb.hash_dict import HASH_ALGORITHMS
from h2hdb.information import FileInformation
from h2hdb.settings import hash_function_by_file, hash_multiple_by_file, hash_stream

# Larger than the small buffer sizes used below, forcing every streaming call
# in this file to cross multiple chunk boundaries instead of reading in one go.
TEST_CONTENT = os.urandom(10_000)
SMALL_BUFFER_SIZE = 777


def test_hash_stream_matches_reference_digests() -> None:
    chunks = [
        TEST_CONTENT[i : i + SMALL_BUFFER_SIZE]
        for i in range(0, len(TEST_CONTENT), SMALL_BUFFER_SIZE)
    ]

    digests = hash_stream(chunks, ["sha512", "blake2b"])

    assert digests["sha512"] == hashlib.sha512(TEST_CONTENT).digest()
    assert digests["blake2b"] == hashlib.blake2b(TEST_CONTENT).digest()


def test_hash_multiple_by_file_matches_reference_digests(tmp_path: Path) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    digests = hash_multiple_by_file(
        str(file_path), HASH_ALGORITHMS, buffer_size=SMALL_BUFFER_SIZE
    )

    for algorithm in HASH_ALGORITHMS:
        assert digests[algorithm] == hashlib.new(algorithm, TEST_CONTENT).digest()


def test_hash_function_by_file_matches_reference_digest(tmp_path: Path) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    assert (
        hash_function_by_file(str(file_path), "sha512")
        == hashlib.sha512(TEST_CONTENT).digest()
    )


def test_hash_multiple_by_file_reads_file_only_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    open_count = 0
    original_open = open

    def counting_open(*args: object, **kwargs: object) -> object:
        nonlocal open_count
        if args and args[0] == str(file_path):
            open_count += 1
        return original_open(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("builtins.open", counting_open)

    hash_multiple_by_file(str(file_path), HASH_ALGORITHMS)

    assert open_count == 1


def test_file_information_sethash_matches_reference_digests(tmp_path: Path) -> None:
    file_path = tmp_path / "page.bin"
    file_path.write_bytes(TEST_CONTENT)

    finfo = FileInformation(str(file_path), db_file_id=1)
    finfo.sethash()

    for algorithm in HASH_ALGORITHMS:
        assert (
            getattr(finfo, algorithm) == hashlib.new(algorithm, TEST_CONTENT).digest()
        )


def test_calculate_hash_of_file_in_cbz_matches_reference_digest(tmp_path: Path) -> None:
    cbz_path = tmp_path / "gallery.cbz"
    with zipfile.ZipFile(cbz_path, "w") as cbz:
        cbz.writestr("page.bin", TEST_CONTENT)

    digest = calculate_hash_of_file_in_cbz(
        str(cbz_path), "page.bin", "sha512", buffer_size=SMALL_BUFFER_SIZE
    )

    assert digest == hashlib.sha512(TEST_CONTENT).digest()


def test_calculate_hash_of_file_in_cbz_returns_empty_for_non_zip(
    tmp_path: Path,
) -> None:
    not_a_zip = tmp_path / "not-a-zip.cbz"
    not_a_zip.write_bytes(b"definitely not a zip file")

    assert calculate_hash_of_file_in_cbz(str(not_a_zip), "page.bin", "sha512") == bytes(
        0
    )
