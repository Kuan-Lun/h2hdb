from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from time import struct_time
from types import TracebackType
from typing import Any, cast

from mysql.connector import connect as SQLConnect
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.errors import IntegrityError, ProgrammingError
from mysql.connector.pooling import PooledMySQLConnection
from pydantic import Field

from .sql_connector import (
    DatabaseConfigurationError,
    DatabaseDuplicateKeyError,
    DatabaseReadOnlyError,
    SQLConnector,
    SQLConnectorParams,
)

AUTO_COMMIT_KEYS = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]
MAX_ALLOWED_PACKET_QUERY = "SELECT @@SESSION.max_allowed_packet"
INSERT_PACKET_BUDGET_PERCENT = 80
PACKET_PROTOCOL_RESERVE_BYTES = 1024
PARAMETER_LITERAL_RESERVE_BYTES = 32


class MariaDBDuplicateKeyError(DatabaseDuplicateKeyError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class MariaDBConnectorParams(SQLConnectorParams):
    host: str = Field(
        min_length=1,
        description="Host of the MariaDB database",
    )
    port: int = Field(
        ge=1,
        le=65535,
        description="Port of the MariaDB database",
    )
    user: str = Field(
        min_length=1,
        description="User for the MariaDB database",
    )
    password: str = Field(
        description="Password for the MariaDB database",
    )
    database: str = Field(
        min_length=1,
        description="Database name for the MariaDB database",
    )
    read_only: bool = False


class MariaDBCursor:
    def __init__(
        self, connection: PooledMySQLConnection | MySQLConnectionAbstract
    ) -> None:
        self.connection = connection

    def __enter__(self) -> MySQLCursorAbstract:
        self.cursor = self.connection.cursor(buffered=True)
        return self.cursor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.cursor.close()


class MariaDBConnector(SQLConnector):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        read_only: bool = False,
    ) -> None:
        self.params = MariaDBConnectorParams(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            read_only=read_only,
        )
        self._max_allowed_packet: int | None = None

    def connect(self) -> None:
        # max_allowed_packet is a session value inherited by each physical
        # connection, so a reconnect must never reuse the previous value.
        self._max_allowed_packet = None
        connection_params = self.params.model_dump(exclude={"read_only"})
        self.connection = SQLConnect(**connection_params)
        self._in_transaction = False
        if self.params.read_only:
            with MariaDBCursor(self.connection) as cursor:
                cursor.execute("SET SESSION TRANSACTION READ ONLY")

    def close(self) -> None:
        try:
            self.connection.close()
        finally:
            self._max_allowed_packet = None

    def check_table_exists(self, table_name: str) -> bool:
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self.fetch_one(query)
        return bool(result)

    def commit(self) -> None:
        self.connection.commit()
        self._in_transaction = False

    def begin(self) -> None:
        if self.params.read_only:
            raise DatabaseReadOnlyError(
                "Cannot start a write transaction in read-only mode"
            )
        self.connection.start_transaction()
        self._in_transaction = True

    def begin_read(self) -> None:
        if self.connection.in_transaction:
            raise ProgrammingError("Transaction already in progress")
        # MariaDB 10.x can expose a MySQL-compatible ``5.5.5-`` handshake
        # prefix. Connector/Python consequently misclassifies the server as
        # MySQL 5.5.5 and rejects its ``readonly=True`` option before sending
        # SQL. MariaDB supports both characteristics directly in START
        # TRANSACTION, so bypass that client-side version gate while retaining
        # the database-enforced read-only consistent snapshot.
        with MariaDBCursor(self.connection) as cursor:
            cursor.execute("START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT")
        self._in_transaction = True

    def rollback(self) -> None:
        self.connection.rollback()
        self._in_transaction = False

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self._ensure_writable(query)
        with MariaDBCursor(self.connection) as cursor:
            try:
                cursor.execute(query, data)
            except IntegrityError as e:
                raise MariaDBDuplicateKeyError(str(e))
            except Exception as e:
                raise e
        if not self._in_transaction and any(
            key in query.upper() for key in AUTO_COMMIT_KEYS
        ):
            self.commit()

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        self._ensure_writable(query)
        owns_implicit_write = not self._in_transaction and any(
            key in query.upper() for key in AUTO_COMMIT_KEYS
        )
        try:
            with MariaDBCursor(self.connection) as cursor:
                cursor.execute(query, data)
                affected = cursor.rowcount
        except IntegrityError as error:
            if owns_implicit_write:
                self._rollback_after_failed_implicit_write()
            raise MariaDBDuplicateKeyError(str(error)) from error
        except Exception:
            if owns_implicit_write:
                self._rollback_after_failed_implicit_write()
            raise
        if affected < 0:
            if owns_implicit_write:
                self._rollback_after_failed_implicit_write()
            raise RuntimeError("MariaDB did not report an affected-row count")
        if owns_implicit_write:
            self.commit()
        return affected

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self._ensure_writable(query)
        is_insert = self._statement_keyword(query) == "INSERT"
        batches = (
            self._packet_safe_insert_batches(query, data)
            if is_insert and data
            else [data]
        )
        owns_implicit_write = not self._in_transaction and any(
            key in query.upper() for key in AUTO_COMMIT_KEYS
        )
        try:
            with MariaDBCursor(self.connection) as cursor:
                for batch in batches:
                    cursor.executemany(query, batch)
        except IntegrityError as error:
            if owns_implicit_write:
                self._rollback_after_failed_implicit_write()
            raise MariaDBDuplicateKeyError(str(error)) from error
        except Exception:
            if owns_implicit_write:
                self._rollback_after_failed_implicit_write()
            raise
        if owns_implicit_write:
            self.commit()

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        with MariaDBCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchone()
        if isinstance(vlist, tuple):
            return vlist
        else:
            return tuple()

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        with MariaDBCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchall()
        return cast(list[tuple[Any, ...]], vlist)

    def _ensure_writable(self, query: str) -> None:
        if not self.params.read_only:
            return
        keyword = self._statement_keyword(query)
        if keyword not in {"SELECT", "SHOW", "EXPLAIN", "WITH"}:
            raise DatabaseReadOnlyError(
                f"Statement {keyword or '<empty>'} is not allowed in read-only mode"
            )

    @staticmethod
    def _statement_keyword(query: str) -> str:
        remaining = query.lstrip()
        while remaining.startswith("/*"):
            comment_end = remaining.find("*/", 2)
            if comment_end < 0:
                return ""
            remaining = remaining[comment_end + 2 :].lstrip()
        words = remaining.split(maxsplit=1)
        return words[0].upper() if words else ""

    def _packet_safe_insert_batches(
        self, query: str, data: list[tuple[Any, ...]]
    ) -> list[list[tuple[Any, ...]]]:
        packet_limit = self._get_session_max_allowed_packet()
        packet_budget = packet_limit * INSERT_PACKET_BUDGET_PERCENT // 100
        encoding = self.connection.python_charset
        query_size = len(query.encode(encoding))
        batches: list[list[tuple[Any, ...]]] = []
        batch: list[tuple[Any, ...]] = []
        batch_size = PACKET_PROTOCOL_RESERVE_BYTES

        for row_number, row in enumerate(data, start=1):
            row_size = (
                query_size
                + 1
                + sum(
                    self._parameter_literal_upper_bound(value, encoding)
                    for value in row
                )
            )
            singleton_size = PACKET_PROTOCOL_RESERVE_BYTES + row_size
            if singleton_size > packet_limit:
                raise DatabaseConfigurationError(
                    "MariaDB INSERT row "
                    f"{row_number} has a conservative packet-size estimate of "
                    f"{singleton_size} bytes, exceeding this connection's "
                    f"max_allowed_packet value of {packet_limit} bytes"
                )

            if batch and batch_size + row_size > packet_budget:
                batches.append(batch)
                batch = []
                batch_size = PACKET_PROTOCOL_RESERVE_BYTES

            # A single row may safely exceed the soft 80% target while still
            # fitting below the server's hard limit. Keep it as its own batch.
            if not batch and singleton_size > packet_budget:
                batches.append([row])
                continue

            batch.append(row)
            batch_size += row_size

        if batch:
            batches.append(batch)
        return batches

    def _get_session_max_allowed_packet(self) -> int:
        if self._max_allowed_packet is not None:
            return self._max_allowed_packet

        result = self.fetch_one(MAX_ALLOWED_PACKET_QUERY)
        if len(result) != 1:
            raise DatabaseConfigurationError(
                "MariaDB did not return a valid @@SESSION.max_allowed_packet value"
            )
        raw_value = result[0]
        try:
            packet_limit = int(raw_value)
        except (TypeError, ValueError) as error:
            raise DatabaseConfigurationError(
                "MariaDB did not return a valid @@SESSION.max_allowed_packet value"
            ) from error
        if isinstance(raw_value, bool) or packet_limit <= 0:
            raise DatabaseConfigurationError(
                "MariaDB did not return a positive @@SESSION.max_allowed_packet value"
            )
        self._max_allowed_packet = packet_limit
        return packet_limit

    @classmethod
    def _parameter_literal_upper_bound(cls, value: Any, encoding: str) -> int:
        if isinstance(value, Enum):
            return cls._parameter_literal_upper_bound(value.value, encoding)
        if value is None:
            payload_size = len(b"NULL")
        elif isinstance(value, bool):
            payload_size = 1
        elif isinstance(value, str):
            payload_size = len(value.encode(encoding))
        elif isinstance(value, (bytes, bytearray)):
            payload_size = len(value)
        elif isinstance(
            value,
            (int, float, Decimal, datetime, date, time, timedelta, struct_time),
        ):
            payload_size = len(str(value).encode(encoding))
        else:
            raise DatabaseConfigurationError(
                "Cannot safely estimate a MariaDB batch parameter of type "
                f"{type(value).__name__}"
            )

        # Connector/Python can at most double the encoded payload while SQL
        # escaping it. The fixed reserve covers quotes and type prefixes.
        return PARAMETER_LITERAL_RESERVE_BYTES + 2 * payload_size

    def _rollback_after_failed_implicit_write(self) -> None:
        try:
            self.rollback()
        except Exception:
            # Preserve the write failure if a broken connection cannot roll
            # back; callers must still see the actionable original error.
            pass
