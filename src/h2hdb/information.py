from pathlib import Path

from .hash_dict import HASH_ALGORITHMS
from .settings import hash_multiple_by_file_with_size


class FileInformation:
    def __init__(self, absolute_path: Path, db_file_id: int) -> None:
        self.absolute_path = absolute_path
        self.db_file_id = db_file_id
        self.issethash = False
        self.bytes_read = 0
        self.db_hash_id: dict[str, int] = dict()

    def sethash(self) -> int:
        if not self.issethash:
            digests, self.bytes_read = hash_multiple_by_file_with_size(
                self.absolute_path, HASH_ALGORITHMS
            )
            for algorithm, digest in digests.items():
                setattr(self, algorithm, digest)
            self.issethash = True
        return self.bytes_read

    def setdb_hash_id(self, algorithm: str, db_hash_id: int) -> None:
        self.db_hash_id[algorithm] = db_hash_id


class TagInformation:
    __slots__ = ["tag_name", "tag_value", "db_tag_id"]

    def __init__(self, tag_name: str, tag_value: str) -> None:
        self.tag_name = tag_name
        self.tag_value = tag_value

    def setdb_tag_id(self, db_tag_id: int) -> None:
        self.db_tag_id = db_tag_id
