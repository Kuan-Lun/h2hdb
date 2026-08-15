__all__ = [
    "SQLConnectorParams",
    "SQLConnector",
    "DatabaseConfigurationError",
    "DatabaseKeyError",
    "DatabaseTableError",
    "DatabaseReadOnlyError",
]


from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
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


class DatabaseReadOnlyError(PermissionError):
    pass


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
    def begin(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @contextmanager
    def transaction(self) -> Generator[None]:
        self.begin()
        try:
            yield
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def begin_read(self) -> None:
        self.begin()

    @contextmanager
    def read_transaction(self) -> Generator[None]:
        self.begin_read()
        try:
            yield
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

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

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        """Execute one mutation and return its exact affected-row count.

        vNext compare-and-swap writers require this stronger primitive.  It is
        deliberately not implemented in terms of ``execute`` plus a later
        query because that would lose the statement-local CAS result.  Legacy
        and test connectors that do not participate in vNext writes fail
        closed until they implement the primitive explicitly.
        """

        del query, data
        raise NotImplementedError("this connector cannot report affected rows")

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
