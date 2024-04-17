import hashlib

FOLDER_NAME_LENGTH_LIMIT = 255
FILE_NAME_LENGTH_LIMIT = 255
COMPARISON_HASH_ALGORITHM = "sha512"
GALLERY_INFO_FILE_NAME = "galleryinfo.txt"


def hash_function(x: bytes, algorithm: str) -> bytes:
    return getattr(hashlib, algorithm.lower())(x).digest()
