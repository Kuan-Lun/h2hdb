from abc import ABCMeta


from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


class H2HDBGalleriesTitles(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_titles_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            title         TEXT         NOT NULL,
                            FULLTEXT (title)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_title(self, db_gallery_id: int, title: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, title) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, title))

    def _get_title_by_db_gallery_id(self, db_gallery_id: int) -> str:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT title
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Title for gallery name ID {db_gallery_id} does not exist."
                self.logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                title = query_result[0]
        return title

    def get_title_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._get_title_by_db_gallery_id(db_gallery_id)
