from typing import Any, cast

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.errors import IntegrityError, ProgrammingError

import h2hdb.mariadb_connector as mariadb_connector_module
from h2hdb import CoreConfig
from h2hdb.mariadb_connector import MariaDBConnector, MariaDBDuplicateKeyError
from h2hdb.sql_connector import DatabaseConfigurationError

_MAX_ALLOWED_PACKET_QUERY = "SELECT @@SESSION.max_allowed_packet"
_INSERT_QUERY = "INSERT INTO widgets (id, value) VALUES (%s, %s)"
_PACKET_LIMIT = 4096
_EXPANDING_VALUE = "漢😀'\"\\\0\n\r\x1a" * 24
_INTEGRATION_PACKET_LIMIT = 1024 * 1024
_INTEGRATION_EXPANDING_VALUE = "漢😀'\"\\\0\n\r\x1a" * 12_000


class _RecordingCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        assert data == ()
        self.queries.append(query)

    def close(self) -> None:
        self.closed = True


class _CompatibilityPrefixedConnection:
    server_version = (5, 5, 5)

    def __init__(self, *, in_transaction: bool = False) -> None:
        self.in_transaction = in_transaction
        self.cursor_instance = _RecordingCursor()

    def cursor(self, *, buffered: bool = False) -> _RecordingCursor:
        assert buffered
        return self.cursor_instance

    def start_transaction(self, **kwargs: object) -> None:
        raise AssertionError(
            "begin_read must not use Connector/Python's MySQL-only version gate: "
            f"{kwargs}"
        )


class _PacketRecordingCursor:
    def __init__(self, connection: _PacketRecordingConnection) -> None:
        self.connection = connection
        self.result: tuple[Any, ...] | None = None
        self.closed = False
        self.rowcount = connection.affected_rows

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self.connection.execute_calls.append((query, data))
        if query == _MAX_ALLOWED_PACKET_QUERY:
            self.result = (self.connection.max_allowed_packet,)

    def executemany(
        self,
        query: str,
        data: list[tuple[Any, ...]],
    ) -> None:
        batch = list(data)
        self.connection.execute_many_calls.append((query, batch))
        call_number = len(self.connection.execute_many_calls)
        if self.connection.fail_execute_many_call == call_number:
            raise IntegrityError(msg="duplicate entry", errno=1062)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.result

    def close(self) -> None:
        self.closed = True


class _PacketRecordingConnection:
    server_version = (11, 0, 0)
    python_charset = "utf8"

    def __init__(
        self,
        *,
        max_allowed_packet: int = _PACKET_LIMIT,
        fail_execute_many_call: int | None = None,
        affected_rows: int = 1,
    ) -> None:
        self.max_allowed_packet = max_allowed_packet
        self.fail_execute_many_call = fail_execute_many_call
        self.affected_rows = affected_rows
        self.in_transaction = False
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.execute_many_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.cursors: list[_PacketRecordingCursor] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.start_transaction_calls = 0
        self.closed = False

    def cursor(self, *, buffered: bool = False) -> _PacketRecordingCursor:
        assert buffered
        cursor = _PacketRecordingCursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        self.commit_calls += 1
        self.in_transaction = False

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.in_transaction = False

    def start_transaction(self, **kwargs: object) -> None:
        assert kwargs == {}
        self.start_transaction_calls += 1
        self.in_transaction = True

    def close(self) -> None:
        self.closed = True


def _connector_with(
    connection: _CompatibilityPrefixedConnection,
) -> MariaDBConnector:
    connector = MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
    )
    connector.connection = cast(MySQLConnectionAbstract, connection)
    connector._in_transaction = False
    return connector


def _packet_connector_with(
    connection: _PacketRecordingConnection,
) -> MariaDBConnector:
    connector = MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
    )
    connector.connection = cast(MySQLConnectionAbstract, connection)
    connector._in_transaction = False
    return connector


def _connector_from_config(config: CoreConfig) -> MariaDBConnector:
    return MariaDBConnector(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database,
    )


def _large_rows(*, start: int = 0, count: int = 6) -> list[tuple[Any, ...]]:
    return [(row_id, _EXPANDING_VALUE) for row_id in range(start, start + count)]


def _packet_queries(connection: _PacketRecordingConnection) -> list[str]:
    return [
        query
        for query, _ in connection.execute_calls
        if query == _MAX_ALLOWED_PACKET_QUERY
    ]


def _flatten_batches(
    calls: list[tuple[str, list[tuple[Any, ...]]]],
) -> list[tuple[Any, ...]]:
    return [row for _, batch in calls for row in batch]


def test_begin_read_bypasses_mariadb_compatibility_version_prefix() -> None:
    connection = _CompatibilityPrefixedConnection()
    connector = _connector_with(connection)

    connector.begin_read()

    assert connection.cursor_instance.queries == [
        "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT"
    ]
    assert connection.cursor_instance.closed
    assert connector._in_transaction


def test_begin_read_rejects_an_existing_transaction() -> None:
    connection = _CompatibilityPrefixedConnection(in_transaction=True)
    connector = _connector_with(connection)

    with pytest.raises(ProgrammingError, match="already in progress"):
        connector.begin_read()

    assert connection.cursor_instance.queries == []
    assert not connector._in_transaction


def test_check_table_exists_binds_the_exact_table_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_one(query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        calls.append((query, data))
        return ("catalog_source_builds",)

    monkeypatch.setattr(connector, "fetch_one", fetch_one)

    assert connector.check_table_exists("catalog_source_builds")
    assert calls == [
        (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            ("catalog_source_builds",),
        )
    ]


def test_execute_many_caches_session_packet_limit_for_physical_connection() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)

    connector.execute_many(_INSERT_QUERY, _large_rows())
    connector.execute_many(_INSERT_QUERY, _large_rows(start=100))

    assert _packet_queries(connection) == [_MAX_ALLOWED_PACKET_QUERY]
    assert len(connection.execute_many_calls) > 2
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize("affected_rows", (0, 1))
def test_execute_affected_returns_statement_rowcount_and_commits(
    affected_rows: int,
) -> None:
    connection = _PacketRecordingConnection(affected_rows=affected_rows)
    connector = _packet_connector_with(connection)

    assert (
        connector.execute_affected(
            "UPDATE allocator SET next_id = %s WHERE stream = %s AND next_id = %s",
            (2, "GALLERY", 1),
        )
        == affected_rows
    )
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0


def test_execute_many_does_not_query_packet_limit_for_non_insert() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)
    rows = [("updated", 1), ("updated", 2)]

    connector.execute_many(
        "UPDATE widgets SET value = %s WHERE id = %s",
        rows,
    )

    assert _packet_queries(connection) == []
    assert connection.execute_many_calls == [
        ("UPDATE widgets SET value = %s WHERE id = %s", rows)
    ]


def test_execute_many_splits_insert_by_encoded_bytes_and_preserves_order() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)
    small_rows = [(row_id, "ok") for row_id in range(6)]
    large_rows = _large_rows(start=100)

    connector.execute_many(_INSERT_QUERY, small_rows)
    small_call_count = len(connection.execute_many_calls)
    connector.execute_many(_INSERT_QUERY, large_rows)
    large_calls = connection.execute_many_calls[small_call_count:]

    assert small_call_count == 1
    assert len(large_calls) > 1
    assert all(query == _INSERT_QUERY and batch for query, batch in large_calls)
    assert _flatten_batches(large_calls) == large_rows


def test_execute_many_splits_insert_with_leading_block_comment() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)
    query = f"/* request trace */ {_INSERT_QUERY}"
    rows = _large_rows()

    connector.execute_many(query, rows)

    assert _packet_queries(connection) == [_MAX_ALLOWED_PACKET_QUERY]
    assert len(connection.execute_many_calls) > 1
    assert _flatten_batches(connection.execute_many_calls) == rows


def test_chunked_execute_many_commits_once_after_all_chunks() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)

    connector.execute_many(_INSERT_QUERY, _large_rows())

    assert len(connection.execute_many_calls) > 1
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0


def test_chunked_execute_many_rolls_back_when_second_chunk_fails() -> None:
    connection = _PacketRecordingConnection(fail_execute_many_call=2)
    connector = _packet_connector_with(connection)

    with pytest.raises(MariaDBDuplicateKeyError, match="duplicate entry"):
        connector.execute_many(_INSERT_QUERY, _large_rows())

    assert len(connection.execute_many_calls) == 2
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1


def test_chunked_execute_many_does_not_commit_inside_explicit_transaction() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)

    with connector.transaction():
        connector.execute_many(_INSERT_QUERY, _large_rows())
        assert len(connection.execute_many_calls) > 1
        assert connection.commit_calls == 0

    assert connection.start_transaction_calls == 1
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0


def test_execute_many_rejects_single_row_over_hard_packet_limit_before_send() -> None:
    connection = _PacketRecordingConnection()
    connector = _packet_connector_with(connection)

    with pytest.raises(DatabaseConfigurationError, match="max_allowed_packet"):
        connector.execute_many(_INSERT_QUERY, [(1, "x" * 2048)])

    assert _packet_queries(connection) == [_MAX_ALLOWED_PACKET_QUERY]
    assert connection.execute_many_calls == []
    assert connection.commit_calls == 0


def test_connect_clears_cached_packet_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_connection = _PacketRecordingConnection(max_allowed_packet=4096)
    second_connection = _PacketRecordingConnection(max_allowed_packet=8192)
    connections = iter([first_connection, second_connection])

    def fake_connect(**kwargs: Any) -> MySQLConnectionAbstract:
        assert kwargs["database"] == "h2hdb"
        return cast(MySQLConnectionAbstract, next(connections))

    monkeypatch.setattr(mariadb_connector_module, "SQLConnect", fake_connect)
    connector = MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
    )

    connector.connect()
    connector.execute_many(_INSERT_QUERY, _large_rows())
    connector.close()
    connector.connect()
    connector.execute_many(_INSERT_QUERY, _large_rows(start=100))

    assert _packet_queries(first_connection) == [_MAX_ALLOWED_PACKET_QUERY]
    assert _packet_queries(second_connection) == [_MAX_ALLOWED_PACKET_QUERY]
    assert first_connection.closed


def test_execute_many_splits_insert_below_real_mariadb_packet_limit(
    mariadb_config: CoreConfig,
) -> None:
    rows = [(row_id, _INTEGRATION_EXPANDING_VALUE) for row_id in range(8)]

    with _connector_from_config(mariadb_config) as connector:
        connector.execute(
            "CREATE TABLE packet_rows (id INT PRIMARY KEY, value LONGTEXT NOT NULL)"
        )
        assert connector.fetch_one(_MAX_ALLOWED_PACKET_QUERY) == (
            _INTEGRATION_PACKET_LIMIT,
        )

        connector.execute_many(_INSERT_QUERY.replace("widgets", "packet_rows"), rows)

        count, total_size = connector.fetch_one(
            "SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM packet_rows"
        )
        assert count == len(rows)
        assert int(total_size) > _INTEGRATION_PACKET_LIMIT


def test_chunked_execute_many_rolls_back_real_mariadb_late_failure(
    mariadb_config: CoreConfig,
) -> None:
    insert_query = _INSERT_QUERY.replace("widgets", "packet_rows")
    rows = [(row_id, _INTEGRATION_EXPANDING_VALUE) for row_id in (1, 2, 3, 99)]

    with _connector_from_config(mariadb_config) as connector:
        connector.execute(
            "CREATE TABLE packet_rows (id INT PRIMARY KEY, value LONGTEXT NOT NULL)"
        )
        connector.execute(insert_query, (99, "existing"))

        with pytest.raises(MariaDBDuplicateKeyError):
            connector.execute_many(insert_query, rows)

        connector.execute(insert_query, (100, "after rollback"))
        assert connector.fetch_all("SELECT id FROM packet_rows ORDER BY id") == [
            (99,),
            (100,),
        ]
