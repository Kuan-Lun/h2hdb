from abc import ABCMeta

from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


class H2HDBUploadAccounts(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_upload_account_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED                      NOT NULL,
                            account       CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            INDEX (account)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_upload_account(self, db_gallery_id: int, account: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, account) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, account))

    def _select_gallery_upload_account(self, db_gallery_id: int) -> str:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT account
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        if query_result:
            account = query_result[0]
        else:
            msg = f"Upload account for gallery name ID {db_gallery_id} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return account

    def get_upload_account_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_upload_account(db_gallery_id)
