__all__ = [
    "FOLDER_NAME_LENGTH_LIMIT",
    "FILE_NAME_LENGTH_LIMIT",
    "COMPARISON_HASH_ALGORITHM",
    "GALLERY_INFO_FILE_NAME",
    "hash_function",
    "hash_function_by_file",
]

import hashlib

FOLDER_NAME_LENGTH_LIMIT = 255
FILE_NAME_LENGTH_LIMIT = 255
COMPARISON_HASH_ALGORITHM = "sha512"
GALLERY_INFO_FILE_NAME = "galleryinfo.txt"


def hash_function(x: bytes, algorithm: str) -> bytes:
    return getattr(hashlib, algorithm.lower())(x).digest()


def hash_function_by_file(file_path: str, algorithm: str) -> bytes:
    with open(file_path, "rb") as f:
        file_content = f.read()
    return hash_function(file_content, algorithm)


def chunk_list(input_list: list, chunk_size: int) -> list:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0.")

    # 初始化結果列表
    result = list()
    # 使用列表切片來分組
    for i in range(0, len(input_list), chunk_size):
        result.append(input_list[i : i + chunk_size])
    return result
