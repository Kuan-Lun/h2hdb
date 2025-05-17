from abc import ABCMeta
import datetime


from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


class H2HDBTimes(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_times_table(self, table_name: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            time          DATETIME     NOT NULL,
                            INDEX (time)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _insert_time(self, table_name: str, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, time) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, time))

    def _select_time(self, table_name: str, db_gallery_id: int) -> datetime.datetime:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT time
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        if query_result:
            time = query_result[0]
        else:
            msg = f"Time for gallery name ID {db_gallery_id} does not exist in table '{table_name}'."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return time

    def _update_time(self, table_name: str, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET time = %s WHERE db_gallery_id = %s
                    """
            connector.execute(update_query, (time, db_gallery_id))

    def _create_galleries_download_times_table(self) -> None:
        self._create_times_table("galleries_download_times")

    def _create_galleries_redownload_times_table(self) -> None:
        self._create_times_table("galleries_redownload_times")

    def _insert_download_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_download_times", db_gallery_id, time)
        self._insert_time("galleries_redownload_times", db_gallery_id, time)

    def update_redownload_time(self, db_gallery_id: int, time: str) -> None:
        self._update_time("galleries_redownload_times", db_gallery_id, time)

    def _reset_redownload_times(self) -> None:
        table_name = "galleries_redownload_times"
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name}
                        JOIN galleries_download_times
                        ON {table_name}.db_gallery_id = galleries_download_times.db_gallery_id
                        SET {table_name}.time = galleries_download_times.time
                        WHERE {table_name}.time <> galleries_download_times.time;

                    """
            connector.execute(update_query)

    def _create_galleries_upload_times_table(self) -> None:
        self._create_times_table("galleries_upload_times")

    def _insert_upload_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_upload_times", db_gallery_id, time)

    def get_upload_time_by_gallery_name(self, gallery_name: str) -> datetime.datetime:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_time("galleries_upload_times", db_gallery_id)

    def _create_galleries_modified_times_table(self) -> None:
        self._create_times_table("galleries_modified_times")

    def _insert_modified_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_modified_times", db_gallery_id, time)

    def _create_galleries_access_times_table(self) -> None:
        self._create_times_table("galleries_access_times")

    def _insert_access_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_access_times", db_gallery_id, time)

    def update_access_time(self, gallery_name: str, time: str) -> None:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        self._update_time("galleries_access_times", db_gallery_id, time)
