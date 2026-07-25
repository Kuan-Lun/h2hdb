import hashlib
import os
import threading
from pathlib import Path
from typing import Any

import pytest

from h2hdb import H2HDB, H2HConfig, H2HDBConfig
from h2hdb.compress_gallery_to_cbz import hash_and_process_file
from h2hdb.hash_dict import (
    FILE_CONTENT_HASH_ALGORITHM,
    FILE_CONTENT_HASH_OUTPUT_BITS,
    HASH_ALGORITHMS,
)
from h2hdb.information import FileInformation
from h2hdb.settings import (
    COMPARISON_HASH_ALGORITHM,
    hash_function_by_file,
    hash_multiple_by_file,
    hash_multiple_by_file_with_size,
    hash_stream,
)
from h2hdb.table_files_dbids import H2HDBFiles

# Larger than the small buffer sizes used below, forcing every streaming call
# in this file to cross multiple chunk boundaries instead of reading in one go.
TEST_CONTENT = os.urandom(10_000)
SMALL_BUFFER_SIZE = 777


def test_canonical_file_content_hash_is_sha256() -> None:
    assert FILE_CONTENT_HASH_ALGORITHM == "sha256"
    assert FILE_CONTENT_HASH_OUTPUT_BITS == 256
    assert HASH_ALGORITHMS == {"sha256": 256}
    assert COMPARISON_HASH_ALGORITHM == FILE_CONTENT_HASH_ALGORITHM


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
        file_path, HASH_ALGORITHMS, buffer_size=SMALL_BUFFER_SIZE
    )

    for algorithm in HASH_ALGORITHMS:
        assert digests[algorithm] == hashlib.new(algorithm, TEST_CONTENT).digest()


def test_hash_multiple_by_file_with_size_reports_bytes_read(tmp_path: Path) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    digests, bytes_read = hash_multiple_by_file_with_size(
        file_path, HASH_ALGORITHMS, buffer_size=SMALL_BUFFER_SIZE
    )

    assert bytes_read == len(TEST_CONTENT)
    for algorithm in HASH_ALGORITHMS:
        assert digests[algorithm] == hashlib.new(algorithm, TEST_CONTENT).digest()


def test_hash_function_by_file_matches_reference_digest(tmp_path: Path) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    assert (
        hash_function_by_file(file_path, FILE_CONTENT_HASH_ALGORITHM)
        == hashlib.sha256(TEST_CONTENT).digest()
    )


def test_hash_multiple_by_file_reads_file_only_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(TEST_CONTENT)

    open_count = 0
    original_open = open

    def counting_open(*args: Any, **kwargs: Any) -> Any:
        nonlocal open_count
        if args and args[0] == file_path:
            open_count += 1
        return original_open(*args, **kwargs)

    monkeypatch.setattr("builtins.open", counting_open)

    hash_multiple_by_file(file_path, HASH_ALGORITHMS)

    assert open_count == 1


def test_file_information_sethash_matches_reference_digests(tmp_path: Path) -> None:
    file_path = tmp_path / "page.bin"
    file_path.write_bytes(TEST_CONTENT)

    finfo = FileInformation(file_path, db_file_id=1)
    assert finfo.sethash() == len(TEST_CONTENT)
    assert finfo.sethash() == len(TEST_CONTENT)

    assert finfo.sha256 == hashlib.sha256(TEST_CONTENT).digest()


def test_file_hash_workers_default_and_bounds() -> None:
    assert H2HConfig().file_hash_workers == min(4, os.process_cpu_count() or 1)

    for invalid_workers in (0, 33):
        with pytest.raises(ValueError):
            H2HConfig(file_hash_workers=invalid_workers)


def test_bounded_file_hashing_matches_serial_results(tmp_path: Path) -> None:
    paths = list[Path]()
    for index in range(6):
        path = tmp_path / f"page-{index}.bin"
        path.write_bytes(TEST_CONTENT + bytes([index]))
        paths.append(path)

    serial = [
        FileInformation(path, db_file_id=index) for index, path in enumerate(paths)
    ]
    parallel = [
        FileInformation(path, db_file_id=index) for index, path in enumerate(paths)
    ]

    assert list(H2HDBFiles._hash_file_informations(serial, 1)) == [
        len(TEST_CONTENT) + 1
    ] * len(paths)
    assert list(H2HDBFiles._hash_file_informations(parallel, 3)) == [
        len(TEST_CONTENT) + 1
    ] * len(paths)

    for serial_info, parallel_info in zip(serial, parallel, strict=True):
        for algorithm in HASH_ALGORITHMS:
            assert getattr(parallel_info, algorithm) == getattr(serial_info, algorithm)


def test_bounded_file_hashing_respects_worker_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_limit = 3
    lock = threading.Lock()
    release_workers = threading.Event()
    started_workers = 0
    active_workers = 0
    peak_workers = 0

    def controlled_sethash(fileinformation: FileInformation) -> int:
        nonlocal started_workers, active_workers, peak_workers
        with lock:
            started_workers += 1
            active_workers += 1
            peak_workers = max(peak_workers, active_workers)
            if started_workers == worker_limit:
                release_workers.set()

        try:
            if not release_workers.wait(timeout=2):
                raise AssertionError("Hash workers did not execute concurrently.")
            return fileinformation.db_file_id
        finally:
            with lock:
                active_workers -= 1

    monkeypatch.setattr(FileInformation, "sethash", controlled_sethash)
    fileinformations = [
        FileInformation(Path(f"unused-{index}"), db_file_id=index)
        for index in range(12)
    ]

    results = list(H2HDBFiles._hash_file_informations(fileinformations, worker_limit))

    assert sorted(results) == list(range(12))
    assert peak_workers == worker_limit


def test_bounded_file_hashing_yields_completed_files_without_head_of_line_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_file_finished = threading.Event()
    release_first_file = threading.Event()

    def finish_second_file_first(fileinformation: FileInformation) -> int:
        if fileinformation.db_file_id == 1:
            if not release_first_file.wait(timeout=2):
                raise AssertionError("First hash worker was not released.")
        else:
            second_file_finished.set()
        return fileinformation.db_file_id

    monkeypatch.setattr(FileInformation, "sethash", finish_second_file_first)
    results = H2HDBFiles._hash_file_informations(
        [
            FileInformation(Path("unused-first"), db_file_id=1),
            FileInformation(Path("unused-second"), db_file_id=2),
        ],
        max_workers=2,
    )

    assert next(results) == 2
    assert second_file_finished.is_set()
    release_first_file.set()
    assert list(results) == [1]


def test_hash_worker_error_skips_database_persistence(
    sqlite_config: H2HDBConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sqlite_config.h2h.file_hash_workers = 2
    db = H2HDB(config=sqlite_config)
    fileinformations = [
        FileInformation(Path("unused-ok"), db_file_id=1),
        FileInformation(Path("unused-failure"), db_file_id=2),
    ]
    persistence_called = False

    def fail_second_file(fileinformation: FileInformation) -> int:
        if fileinformation.db_file_id == 2:
            raise OSError("simulated read failure")
        return 0

    def record_persistence(*args: Any, **kwargs: Any) -> None:
        nonlocal persistence_called
        persistence_called = True

    monkeypatch.setattr(FileInformation, "sethash", fail_second_file)
    monkeypatch.setattr(
        db.files, "insert_db_hash_id_by_hash_values", record_persistence
    )
    monkeypatch.setattr(
        db.files, "insert_hash_value_by_db_hash_ids", record_persistence
    )

    with pytest.raises(OSError, match="simulated read failure"):
        db.files._insert_gallery_file_hash_for_db_gallery_id(fileinformations)

    assert persistence_called is False
    assert not any(
        thread.name.startswith("h2hdb-file-hash") for thread in threading.enumerate()
    )


def test_file_hash_perf_log_reports_workers_and_bytes(
    sqlite_config: H2HDBConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sqlite_config.h2h.file_hash_workers = 2
    file_contents = [b"first page", b"second page is longer"]
    file_paths = list[Path]()
    for index, content in enumerate(file_contents):
        file_path = tmp_path / f"page-{index}.bin"
        file_path.write_bytes(content)
        file_paths.append(file_path)

    with H2HDB(config=sqlite_config) as db:
        db.create_main_tables()
        gallery_name = "hash performance log"
        db.gallery_ids._insert_gallery_name(gallery_name)
        db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(gallery_name)
        db_file_ids = db.files._insert_gallery_files(
            db_gallery_id, [path.name for path in file_paths]
        )
        fileinformations = [
            FileInformation(path, db_file_ids[path.name]) for path in file_paths
        ]
        debug_messages = list[str]()
        monkeypatch.setattr(db.logger, "debug", debug_messages.append)

        db.files._insert_gallery_file_hash_for_db_gallery_id(fileinformations)

    end_message = next(
        message
        for message in debug_messages
        if "event=end stage=file_byte_hashing" in message
    )
    assert f"bytes={sum(map(len, file_contents))}" in end_message
    assert "configured_workers=2" in end_message
    assert "worker_limit=2" in end_message
    assert "rate_files_s=" in end_message
    assert "rate_mib_s=" in end_message
    assert (
        sum(
            "event=start stage=hash_catalog_insert algorithm=sha256" in message
            for message in debug_messages
        )
        == 1
    )
    assert (
        sum(
            "event=start stage=hash_catalog_lookup algorithm=sha256" in message
            for message in debug_messages
        )
        == 1
    )
    assert (
        sum(
            "event=start stage=hash_association_insert algorithm=sha256" in message
            for message in debug_messages
        )
        == 1
    )
    assert any(
        "event=start stage=hash_association_persistence" in message
        and "algorithms=1" in message
        for message in debug_messages
    )


def test_hash_and_process_file_skips_files_with_excluded_hash(tmp_path: Path) -> None:
    input_directory = tmp_path / "input"
    tmp_cbz_directory = tmp_path / "tmp_cbz"
    input_directory.mkdir()
    tmp_cbz_directory.mkdir()

    excluded_content = b"excluded file content"
    kept_content = b"kept file content"
    (input_directory / "excluded.bin").write_bytes(excluded_content)
    (input_directory / "kept.bin").write_bytes(kept_content)

    excluded_hash = hashlib.new(COMPARISON_HASH_ALGORITHM, excluded_content).digest()
    exclude_hashs = {excluded_hash}

    hash_and_process_file(
        input_directory, tmp_cbz_directory, "excluded.bin", exclude_hashs, 0
    )
    hash_and_process_file(
        input_directory, tmp_cbz_directory, "kept.bin", exclude_hashs, 0
    )

    assert not (tmp_cbz_directory / "excluded.bin").exists()
    assert (tmp_cbz_directory / "kept.bin").read_bytes() == kept_content
