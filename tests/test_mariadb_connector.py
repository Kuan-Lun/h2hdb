from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.errors import IntegrityError, ProgrammingError

import h2hdb.mariadb_connector as mariadb_connector_module
from h2hdb import CoreConfig
from h2hdb.mariadb_connector import (
    INNODB_DURABILITY_QUERY,
    MariaDBConnector,
    MariaDBDuplicateKeyError,
)
from h2hdb.mariadb_pool import MariaDBConnectionPool
from h2hdb.repository import RepositoryContext
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
        elif query == INNODB_DURABILITY_QUERY:
            self.result = (self.connection.innodb_flush_log_at_trx_commit,)

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
        innodb_flush_log_at_trx_commit: int = 1,
    ) -> None:
        self.max_allowed_packet = max_allowed_packet
        self.fail_execute_many_call = fail_execute_many_call
        self.affected_rows = affected_rows
        self.innodb_flush_log_at_trx_commit = innodb_flush_log_at_trx_commit
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
    assert not connection.cursor_instance.closed
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
    assert len(connection.cursors) == 1
    assert not connection.cursors[0].closed
    connector.close()
    assert connection.cursors[0].closed


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


def test_connect_rejects_nondurable_innodb_commit_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _PacketRecordingConnection(innodb_flush_log_at_trx_commit=2)

    monkeypatch.setattr(
        mariadb_connector_module,
        "SQLConnect",
        lambda **_kwargs: cast(MySQLConnectionAbstract, connection),
    )
    connector = MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
    )

    with pytest.raises(DatabaseConfigurationError, match="flush_log_at_trx_commit=1"):
        connector.connect()

    assert connection.closed


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


class _PooledRecordingConnection(_PacketRecordingConnection):
    def __init__(self) -> None:
        super().__init__()
        self.reset_calls = 0
        self.fail_reset = False
        self.fail_commit = False

    def cmd_reset_connection(self) -> bool:
        self.reset_calls += 1
        if self.fail_reset:
            raise ConnectionError("injected reset failure")
        return True

    def commit(self) -> None:
        super().commit()
        if self.fail_commit:
            raise ConnectionError("injected committed response loss")


def _pooled_connector(
    pool: MariaDBConnectionPool, *, read_only: bool = False
) -> MariaDBConnector:
    return MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
        read_only=read_only,
        _pool=pool,
    )


def test_pooled_connector_reuses_one_cursor_executes_immediately_and_resets() -> None:
    connection = _PooledRecordingConnection()
    pool = MariaDBConnectionPool(lambda: cast(MySQLConnectionAbstract, connection))
    with _pooled_connector(pool) as connector:
        with connector.transaction():
            connector.execute(_INSERT_QUERY, (1, "first"))
            assert connection.execute_calls[-1] == (_INSERT_QUERY, (1, "first"))
            connector.execute(_INSERT_QUERY, (2, "second"))
            assert connection.commit_calls == 0
        assert connection.commit_calls == 1
        assert len(connection.cursors) == 1
    assert connection.cursors[0].closed
    assert connection.rollback_calls == 1
    assert connection.reset_calls == 1
    assert not connection.closed
    with _pooled_connector(pool) as reopened:
        assert cast(MariaDBConnector, reopened).connection is cast(
            MySQLConnectionAbstract, connection
        )
        assert len(connection.cursors) == 2
    pool.close()
    assert connection.closed


def test_connector_rejects_cross_thread_lease_use_and_close() -> None:
    connection = _PooledRecordingConnection()
    pool = MariaDBConnectionPool(lambda: cast(MySQLConnectionAbstract, connection))
    connector = _pooled_connector(pool)
    with connector:
        calls_before = len(connection.execute_calls)
        with ThreadPoolExecutor(max_workers=1) as executor:
            for operation in (lambda: connector.fetch_one("SELECT 1"), connector.close):
                with pytest.raises(RuntimeError, match="another thread/process"):
                    executor.submit(operation).result(timeout=5)
        assert len(connection.execute_calls) == calls_before
        assert not connection.closed
    pool.close()


def test_commit_response_loss_is_not_retried_and_discards_lease() -> None:
    connection = _PooledRecordingConnection()
    connection.fail_commit = True
    pool = MariaDBConnectionPool(lambda: cast(MySQLConnectionAbstract, connection))
    connector = _pooled_connector(pool)
    with connector:
        with pytest.raises(ConnectionError, match="committed response loss"):
            with connector.transaction():
                connector.execute(_INSERT_QUERY, (1, "durable"))
        with pytest.raises(RuntimeError, match="connection failed"):
            connector.commit()
        assert connection.commit_calls == 1
    assert connection.closed
    assert connection.reset_calls == 0
    pool.close()


def test_failed_reset_discards_session_and_returns_pool_capacity() -> None:
    first, second = _PooledRecordingConnection(), _PooledRecordingConnection()
    first.fail_reset = True
    connections = iter((first, second))
    pool = MariaDBConnectionPool(
        lambda: cast(MySQLConnectionAbstract, next(connections)),
        capacity=1,
    )
    with _pooled_connector(pool):
        pass
    assert first.closed
    with _pooled_connector(pool) as connector:
        assert cast(MariaDBConnector, connector).connection is cast(
            MySQLConnectionAbstract, second
        )
    pool.close()
    assert second.closed


def test_failed_connector_initialization_releases_pool_capacity() -> None:
    first, second = _PooledRecordingConnection(), _PooledRecordingConnection()
    first.innodb_flush_log_at_trx_commit = 2
    connections = iter((first, second))
    pool = MariaDBConnectionPool(
        lambda: cast(MySQLConnectionAbstract, next(connections)),
        capacity=1,
    )
    with pytest.raises(DatabaseConfigurationError, match="flush_log_at_trx_commit=1"):
        with _pooled_connector(pool):
            pass
    assert first.closed
    with _pooled_connector(pool):
        pass
    pool.close()


@pytest.mark.mariadb_smoke
def test_runtime_pool_real_mariadb_resets_read_only_session_and_uncommitted_rows(
    mariadb_config: CoreConfig,
) -> None:
    context = RepositoryContext.from_config(mariadb_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE lease_rows (id INT PRIMARY KEY)")
            identity = connector.fetch_one("SELECT CONNECTION_ID()")
            connector.execute("SET @h2hdb_lease_marker = 42")
            connector.begin()
            connector.execute("INSERT INTO lease_rows VALUES (1)")
            # Explicit close must roll back unfinished writes.
        with context.SQLConnector() as connector:
            assert connector.fetch_one("SELECT CONNECTION_ID()") == identity
            assert connector.fetch_one("SELECT @h2hdb_lease_marker") == (None,)
            assert connector.fetch_all("SELECT id FROM lease_rows") == []
            connector.rollback()  # End the implicit SELECT snapshot.
            connector.execute("SET SESSION TRANSACTION READ ONLY")
            with connector.read_transaction():
                assert connector.fetch_one("SELECT @@SESSION.tx_read_only") == (1,)
        with context.SQLConnector() as connector:
            assert connector.fetch_one("SELECT CONNECTION_ID()") == identity
            assert connector.fetch_one("SELECT @@SESSION.tx_read_only") == (0,)
            with connector.transaction():
                connector.execute("INSERT INTO lease_rows VALUES (2)")
        with context.SQLConnector() as connector:
            assert connector.fetch_all("SELECT id FROM lease_rows") == [(2,)]
    finally:
        context.close()


def test_runtime_pool_real_mariadb_commit_response_loss_reads_durable_outcome(
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = RepositoryContext.from_config(mariadb_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE lease_rows (id INT PRIMARY KEY)")
            identity = connector.fetch_one("SELECT CONNECTION_ID()")
            connection = cast(MariaDBConnector, connector)._require_connection()
            original_commit = connection.commit
            commits = 0

            def commit_then_lose_response() -> None:
                nonlocal commits
                original_commit()
                commits += 1
                raise ConnectionError("injected real committed response loss")

            monkeypatch.setattr(connection, "commit", commit_then_lose_response)
            with pytest.raises(ConnectionError, match="committed response loss"):
                with connector.transaction():
                    connector.execute("INSERT INTO lease_rows VALUES (1)")
                    connector.execute("INSERT INTO lease_rows VALUES (2)")
            assert commits == 1
        with context.SQLConnector() as connector:
            assert connector.fetch_one("SELECT CONNECTION_ID()") != identity
            assert connector.fetch_all("SELECT id FROM lease_rows ORDER BY id") == [
                (1,),
                (2,),
            ]
    finally:
        context.close()


def test_runtime_pool_real_mariadb_close_preserves_active_transaction(
    mariadb_config: CoreConfig,
) -> None:
    context = RepositoryContext.from_config(mariadb_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE lease_rows (id INT PRIMARY KEY)")
            with connector.transaction():
                connector.execute("INSERT INTO lease_rows VALUES (1)")
                context.close()
                with pytest.raises(RuntimeError, match="runtime is closed"):
                    context.SQLConnector()
                connector.execute("INSERT INTO lease_rows VALUES (2)")
    finally:
        context.close()
    restarted = RepositoryContext.from_config(mariadb_config)
    try:
        with restarted.SQLConnector() as connector:
            assert connector.fetch_all("SELECT id FROM lease_rows ORDER BY id") == [
                (1,),
                (2,),
            ]
    finally:
        restarted.close()
