from .repository import BaseRepository, RepositoryContext
from .sql_connector import DatabaseKeyError
from .table_gids import H2HDBGalleriesIDs


class H2HDBGalleriesTitles(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_galleries_titles_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mariadb":
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
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            title TEXT NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    self._create_sqlite_fts5_sync(
                        connector, table_name, "title", "db_gallery_id"
                    )

            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_title(self, db_gallery_id: int, title: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            insert_query = f"""
                INSERT INTO {table_name} (db_gallery_id, title) VALUES (%s, %s)
            """
            connector.execute(insert_query, (db_gallery_id, title))

    def _get_title_by_db_gallery_id(self, db_gallery_id: int) -> str:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            select_query = f"""
                SELECT title
                FROM {table_name}
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        if query_result:
            title = str(query_result[0])
        else:
            msg = f"Title for gallery name ID {db_gallery_id} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return title

    def get_title_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        return self._get_title_by_db_gallery_id(db_gallery_id)
