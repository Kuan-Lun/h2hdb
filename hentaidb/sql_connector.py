from abc import ABCMeta, abstractmethod

from mysql.connector import Error as MySQLError
from mysql.connector import connect as MySQLConnect


class SQLConnectorParams:
    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database


class SQLConnector(metaclass=ABCMeta):
    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def close(self) -> bool:
        pass

    def __enter__(self) -> "SQLConnector":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class MySQLConnector(SQLConnector):
    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        super().__init__(host, port, user, password, database)
        self.connection = None

    def connect(self) -> None:
        self.connection = MySQLConnect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def close(self) -> bool:
        try:
            self.connection.close()
            return True
        except MySQLError as e:
            print(e)
            return False

    def execute(self, query: str, data: tuple = ()) -> bool:
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        self.connection.commit()
        cursor.close()

    def fetch(self, query: str, data: tuple = ()) -> list:
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        vlist = cursor.fetchall()
        cursor.close()
        return vlist
