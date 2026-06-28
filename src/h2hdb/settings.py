__all__ = [
    "FOLDER_NAME_LENGTH_LIMIT",
    "FILE_NAME_LENGTH_LIMIT",
    "COMPARISON_HASH_ALGORITHM",
    "GALLERY_INFO_FILE_NAME",
    "hash_function",
    "hash_function_by_file",
]

import hashlib
import logging
from enum import Enum, StrEnum

FOLDER_NAME_LENGTH_LIMIT = 255
FILE_NAME_LENGTH_LIMIT = 255
COMPARISON_HASH_ALGORITHM = "sha512"
GALLERY_INFO_FILE_NAME = "galleryinfo.txt"


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


def hash_function(x: bytes, algorithm: str) -> bytes:
    return hashlib.new(algorithm.lower(), x).digest()


def hash_function_by_file(file_path: str, algorithm: str) -> bytes:
    with open(file_path, "rb") as f:
        file_content = f.read()
    return hash_function(file_content, algorithm)


def chunk_list[T](input_list: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0.")

    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]
