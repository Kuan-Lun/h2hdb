__all__ = [
    "FOLDER_NAME_LENGTH_LIMIT",
    "FILE_NAME_LENGTH_LIMIT",
    "COMPARISON_HASH_ALGORITHM",
    "GALLERY_INFO_FILE_NAME",
    "HASH_STREAM_BUFFER_SIZE",
    "iter_file_chunks",
    "hash_stream",
    "hash_function_by_file",
    "hash_multiple_by_file",
]

import hashlib
import logging
from collections.abc import Iterable, Iterator
from enum import Enum, StrEnum
from typing import Any

FOLDER_NAME_LENGTH_LIMIT = 255
FILE_NAME_LENGTH_LIMIT = 255
COMPARISON_HASH_ALGORITHM = "sha512"
GALLERY_INFO_FILE_NAME = "galleryinfo.txt"

# Large scanned pages can be tens of MiB; hashing in fixed-size chunks keeps
# peak memory bounded regardless of file size, instead of loading the whole
# file into one bytes object.
HASH_STREAM_BUFFER_SIZE = 4 * 1024 * 1024


class LOG_LEVEL(int, Enum):
    notset = logging.NOTSET
    debug = logging.DEBUG
    info = logging.INFO
    warning = logging.WARNING
    error = logging.ERROR
    critical = logging.CRITICAL


class CBZ_GROUPING(StrEnum):
    flat = "flat"
    date_yyyy = "date-yyyy"
    date_yyyy_mm = "date-yyyy-mm"
    date_yyyy_mm_dd = "date-yyyy-mm-dd"


class CBZ_SORT(StrEnum):
    no = "no"
    upload_time = "upload_time"
    download_time = "download_time"
    pages = "pages"
    pages_num = "pages+[num]"


def hash_stream(chunks: Iterable[bytes], algorithms: Iterable[str]) -> dict[str, bytes]:
    """Compute one digest per algorithm from a single pass over `chunks`."""
    hashers = {algorithm: hashlib.new(algorithm.lower()) for algorithm in algorithms}
    for chunk in chunks:
        for hasher in hashers.values():
            hasher.update(chunk)
    return {algorithm: hasher.digest() for algorithm, hasher in hashers.items()}


def iter_file_chunks(
    file: Any, buffer_size: int = HASH_STREAM_BUFFER_SIZE
) -> Iterator[bytes]:
    """Yield fixed-size chunks from a binary file-like object until EOF."""
    return iter(lambda: file.read(buffer_size), b"")


def hash_multiple_by_file(
    file_path: str,
    algorithms: Iterable[str],
    buffer_size: int = HASH_STREAM_BUFFER_SIZE,
) -> dict[str, bytes]:
    with open(file_path, "rb") as f:
        return hash_stream(iter_file_chunks(f, buffer_size), algorithms)


def hash_function_by_file(file_path: str, algorithm: str) -> bytes:
    return hash_multiple_by_file(file_path, [algorithm])[algorithm]


def chunk_list[T](input_list: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0.")

    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]
