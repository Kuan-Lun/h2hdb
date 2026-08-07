__all__ = [
    "FILE_NAME_LENGTH_LIMIT",
    "FOLDER_NAME_LENGTH_LIMIT",
    "LOG_LEVEL",
    "chunk_list",
]

import logging
from enum import Enum

FOLDER_NAME_LENGTH_LIMIT = 255
FILE_NAME_LENGTH_LIMIT = 255


class LOG_LEVEL(int, Enum):
    notset = logging.NOTSET
    debug = logging.DEBUG
    info = logging.INFO
    warning = logging.WARNING
    error = logging.ERROR
    critical = logging.CRITICAL


def chunk_list[T](input_list: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0.")
    return [
        input_list[index : index + chunk_size]
        for index in range(0, len(input_list), chunk_size)
    ]
