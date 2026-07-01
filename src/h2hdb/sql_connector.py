__all__ = [
    "SQLConnectorParams",
    "SQLConnector",
    "DatabaseConfigurationError",
    "DatabaseKeyError",
    "DatabaseTableError",
]


from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict


class DatabaseConfigurationError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class DatabaseKeyError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class DatabaseDuplicateKeyError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class DatabaseTableError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class SQLConnectorParams(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SQLConnector(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self) -> SQLConnector:
        self.connect()
        return self

    @abstractmethod
    def check_table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.close()

    @abstractmethod
    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        pass

    @abstractmethod
    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        pass

    @abstractmethod
    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        pass

    @abstractmethod
    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        pass
