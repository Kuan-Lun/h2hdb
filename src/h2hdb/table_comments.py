from abc import ABCMeta


from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


class H2HDBGalleriesComments(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_comments_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            comment       TEXT         NOT NULL,
                            FULLTEXT (Comment)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_comment(self, db_gallery_id: int, comment: str) -> None:
        if comment != "":
            with self.SQLConnector() as connector:
                table_name = "galleries_comments"
                match self.config.database.sql_type.lower():
                    case "mysql":
                        insert_query = f"""
                            INSERT INTO {table_name} (db_gallery_id, comment) VALUES (%s, %s)
                        """
                connector.execute(insert_query, (db_gallery_id, comment))

    def _update_gallery_comment(self, db_gallery_id: int, comment: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET Comment = %s WHERE db_gallery_id = %s
                    """
            connector.execute(update_query, (comment, db_gallery_id))

    def __get_gallery_comment_by_db_gallery_id(self, db_gallery_id: int) -> tuple:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT Comment
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return query_result

    def _check_gallery_comment_by_db_gallery_id(self, db_gallery_id: int) -> bool:
        query_result = self.__get_gallery_comment_by_db_gallery_id(db_gallery_id)
        return len(query_result) != 0

    def _check_gallery_comment_by_gallery_name(self, gallery_name: str) -> bool:
        ischeck = False
        if self._check_galleries_dbids_by_gallery_name(gallery_name):
            db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
            ischeck = self._check_gallery_comment_by_db_gallery_id(db_gallery_id)
        return ischeck

    def _select_gallery_comment(self, db_gallery_id: int) -> str:
        query_result = self.__get_gallery_comment_by_db_gallery_id(db_gallery_id)
        if query_result:
            comment = query_result[0]
        else:
            msg = (
                f"Uploader comment for gallery name ID {db_gallery_id} does not exist."
            )
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return comment

    def get_comment_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_comment(db_gallery_id)
