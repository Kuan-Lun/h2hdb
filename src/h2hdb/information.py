class TagInformation:
    __slots__ = ["tag_name", "tag_value", "db_tag_id"]

    def __init__(self, tag_name: str, tag_value: str) -> None:
        self.tag_name = tag_name
        self.tag_value = tag_value

    def setdb_tag_id(self, db_tag_id: int) -> None:
        self.db_tag_id = db_tag_id
