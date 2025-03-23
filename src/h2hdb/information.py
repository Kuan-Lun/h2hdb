from .hash_dict import HASH_ALGORITHMS
from .settings import hash_function


class FileInformation:
    def __init__(self, absolute_path: str, db_file_id: int) -> None:
        self.absolute_path = absolute_path
        self.db_file_id = db_file_id
        self.issethash = False
        self.db_hash_id = dict[str, int]()

    def sethash(self) -> None:
        if not self.issethash:
            with open(self.absolute_path, "rb") as file:
                file_content = file.read()
            algorithmlist = list(HASH_ALGORITHMS.keys())
            for algorithm in algorithmlist:
                setattr(self, algorithm, hash_function(file_content, algorithm))
            self.issethash = True

    def setdb_hash_id(self, algorithm: str, db_hash_id: int) -> None:
        self.db_hash_id[algorithm] = db_hash_id


class TagInformation:
    __slots__ = ["tag_name", "tag_value", "db_tag_id"]

    def __init__(self, tag_name: str, tag_value: str) -> None:
        self.tag_name = tag_name
        self.tag_value = tag_value

    def setdb_tag_id(self, db_tag_id: int) -> None:
        self.db_tag_id = db_tag_id
