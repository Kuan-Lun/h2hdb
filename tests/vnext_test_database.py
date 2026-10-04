"""Explicit native database allocation for portable repository test contracts.

The fixture owns each database lifetime. Bare generated setup deliberately does
not replace schema-epoch admission or a complete READY audit.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Generator, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, field
from functools import cache, wraps
from itertools import groupby
from typing import Any, Concatenate, Literal
from unittest.mock import patch
from weakref import WeakValueDictionary

from h2hdb import CoreConfig
from h2hdb import mariadb_connector as native_mariadb
from h2hdb import sqlite_connector as native_sqlite
from h2hdb.config_loader import DatabaseAccessMode
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.schema_epoch import SchemaEpochDefinition
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

Backend = Literal["sqlite", "mariadb"]
type _DatabaseKey = tuple[str, str | None, int | None, str]


def _database_key(config: CoreConfig) -> _DatabaseKey:
    value = config.database
    return value.sql_type, value.host, value.port, value.database


@dataclass
class _ConnectionOwner:
    connectors: list[SQLConnector] = field(default_factory=list)

    def close(self) -> None:
        errors: list[BaseException] = []
        for connector in tuple(self.connectors):
            if getattr(connector, "connection", None) is not None:
                try:
                    connector.close()
                except BaseException as error:
                    errors.append(error)
        self.connectors.clear()
        if errors:
            raise BaseExceptionGroup("native test connector cleanup failed", errors)


_OWNERS: WeakValueDictionary[_DatabaseKey, _ConnectionOwner] = WeakValueDictionary()


def _connection_owner(config: CoreConfig) -> _ConnectionOwner:
    key = _database_key(config)
    owner = _OWNERS.get(key)
    if owner is None:
        owner = _ConnectionOwner()
        _OWNERS[key] = owner
    return owner


@contextmanager
def database_connection_lifetime(config: CoreConfig) -> Iterator[None]:
    """Close fixture-owned raw helper connections before database DROP teardown."""

    owner = _connection_owner(config)
    try:
        yield
    finally:
        owner.close()
        key = _database_key(config)
        if _OWNERS.get(key) is owner:
            del _OWNERS[key]


def _needs_fixture_transaction(connector: SQLConnector) -> bool:
    connection = getattr(connector, "connection", None)
    if connection is None:
        # Unopened connector protocol doubles remain local tests; the original
        # method still rejects a real closed connector rather than opening it.
        return False
    if isinstance(connector, MariaDBConnector):
        if connector._in_transaction:
            return False
        if connection.in_transaction:
            raise AssertionError(
                "test inspector found an unmanaged MariaDB transaction; fix its caller boundary"
            )
        return True
    if isinstance(connector, SQLiteConnector):
        return not connection.in_transaction
    return False


@contextmanager
def fixture_transaction(connector: SQLConnector) -> Iterator[None]:
    """Own a seed write transaction, or participate in the caller's managed one."""

    if _needs_fixture_transaction(connector):
        with connector.transaction():
            yield
    else:
        yield


@contextmanager
def inspection_snapshot(connector: SQLConnector) -> Iterator[None]:
    if _needs_fixture_transaction(connector):
        with connector.read_transaction():
            yield
    else:
        yield


def atomic_fixture[**P, R](
    function: Callable[Concatenate[SQLConnector, P], R],
) -> Callable[Concatenate[SQLConnector, P], R]:
    @wraps(function)
    def seeded(connector: SQLConnector, /, *args: P.args, **kwargs: P.kwargs) -> R:
        with fixture_transaction(connector):
            return function(connector, *args, **kwargs)

    return seeded


@contextmanager
def track_managed_transactions(config: CoreConfig) -> Iterator[set[int]]:
    """Observe real native transaction lifetimes, including failed adapter calls."""

    native = (
        SQLiteConnector if config.database.sql_type == "sqlite" else MariaDBConnector
    )
    active: set[int] = set()
    with ExitStack() as patches:
        for method in ("begin", "begin_read", "commit", "rollback", "close"):
            original = getattr(native, method)

            def observed(
                connector: SQLConnector,
                *,
                _original: Callable[[SQLConnector], None] = original,
                _method: str = method,
            ) -> None:
                _original(connector)
                if _method in {"begin", "begin_read"}:
                    active.add(id(connector))
                else:
                    active.discard(id(connector))

            patches.enter_context(patch.object(native, method, observed))
        yield active


def inspect_one(
    connector: SQLConnector, sql: str, data: tuple[Any, ...] = ()
) -> tuple[Any, ...]:
    """Run a test oracle read in its own snapshot unless already transaction-owned."""

    with inspection_snapshot(connector):
        return connector.fetch_one(sql, data)


def inspect_all(
    connector: SQLConnector, sql: str, data: tuple[Any, ...] = ()
) -> list[tuple[Any, ...]]:
    with inspection_snapshot(connector):
        return connector.fetch_all(sql, data)


def assert_indexed_query(
    connector: SQLConnector, sql: str, data: tuple[Any, ...] = ()
) -> list[tuple[Any, ...]]:
    """Require native indexed access or a constant/empty optimizer result."""

    if connector_backend(connector) == "sqlite":
        plan = inspect_all(connector, "EXPLAIN QUERY PLAN " + sql, data)
        details = [str(row[3]) for row in plan]
        assert details and not any("SCAN " in detail for detail in details), plan
        assert any("SEARCH " in detail for detail in details), plan
    else:
        plan = inspect_all(connector, "EXPLAIN " + sql, data)
        physical = [
            row
            for row in plan
            if row[2] is not None and not str(row[2]).startswith("<")
        ]
        assert all(
            row[3] in {"const", "system", "eq_ref", "ref", "range"} for row in physical
        ), plan
        if not physical:
            # MariaDB can prove an empty lookup from its const table before a
            # physical plan exists. This fixture proves zero reads, not the
            # access cost of a larger populated relation.
            assert plan and all(
                row[2] is None
                and ("Impossible" in str(row[-1]) or "optimized away" in str(row[-1]))
                for row in plan
            ), plan
    return plan


@contextmanager
def trace_statements(connector: SQLConnector, statements: list[str]) -> Iterator[None]:
    """Observe production connector calls identically on either native engine."""

    with ExitStack() as patches:
        for name in (
            "execute",
            "execute_many",
            "execute_affected",
            "fetch_one",
            "fetch_all",
        ):
            original = getattr(connector, name)

            def traced(
                sql: str,
                *args: object,
                _original: Callable[..., object] = original,
                **kwargs: object,
            ) -> object:
                statements.append(sql)
                return _original(sql, *args, **kwargs)

            patches.enter_context(patch.object(connector, name, side_effect=traced))
        yield


@dataclass
class DatabaseFactory:
    """Allocate separate databases, retaining same-name identity for restarts."""

    backend: Backend
    allocate: Callable[[str], CoreConfig]
    dispose: Callable[[CoreConfig], None]
    _databases: dict[str, CoreConfig] = field(default_factory=dict, init=False)
    _owners: dict[str, _ConnectionOwner] = field(default_factory=dict, init=False)

    def config(self, name: str = "catalog") -> CoreConfig:
        if not name or len(name) > 256:
            raise ValueError("test database name must contain 1..256 characters")
        if name not in self._databases:
            config = self.allocate(name)
            if config.database.sql_type != self.backend:
                raise ValueError("test database allocator returned another backend")
            self._databases[name] = config
            self._owners[name] = _connection_owner(config)
        return self._databases[name]

    def release(self, name: str = "catalog") -> None:
        """Drop an owned database after every connector using it is closed."""

        config = self._databases[name]
        owner = self._owners[name]
        try:
            owner.close()
        finally:
            self.dispose(config)
            del self._databases[name]
            del self._owners[name]
            key = _database_key(config)
            if _OWNERS.get(key) is owner:
                del _OWNERS[key]

    def close_connections(self, config: CoreConfig) -> None:
        """Close raw fixture handles without dropping an explicitly owned DB.

        Public facades must already have been closed by their caller. This
        method never adopts a path or a database allocated outside this factory.
        """

        for name, owned in self._databases.items():
            if config is owned:
                self._owners[name].close()
                return
        raise ValueError("snapshot requires a database owned by this factory")

    def close(self) -> None:
        errors: list[BaseException] = []
        for name in tuple(self._databases):
            try:
                self.release(name)
            except BaseException as error:
                errors.append(error)
        if errors:
            raise BaseExceptionGroup("native test database cleanup failed", errors)


@contextmanager
def generated_databases(
    factory: DatabaseFactory, count: int
) -> Iterator[Iterator[SQLConnector]]:
    """Provide independent native probe databases using pytest's owned service."""

    def allocated() -> Generator[SQLConnector]:
        for index in range(count):
            name = f"probe-{index}"
            try:
                with closing(
                    open_generated_database(factory.config(name))
                ) as connector:
                    yield connector
            finally:
                factory.release(name)

    with closing(allocated()) as connectors:
        yield connectors


def connector_backend(connector: SQLConnector) -> Backend:
    """Derive dialect from the native connector, never a caller-supplied label."""

    if isinstance(connector, SQLiteConnector):
        return "sqlite"
    if isinstance(connector, MariaDBConnector):
        return "mariadb"
    raise TypeError("portable database tests require a native SQL connector")


def database_connector(config: CoreConfig) -> SQLConnector:
    """Construct an unconnected, unpooled connector; its caller owns close()."""

    database = config.database
    read_only = database.access_mode is DatabaseAccessMode.read_only
    if database.sql_type == "sqlite":
        connector: SQLConnector = native_sqlite.SQLiteConnector(
            database.database, read_only=read_only
        )
    else:
        assert database.host is not None
        assert database.port is not None
        assert database.user is not None
        assert database.password is not None
        connector = native_mariadb.MariaDBConnector(
            host=database.host,
            port=database.port,
            user=database.user,
            password=database.password,
            database=database.database,
            read_only=read_only,
        )
    owner = _OWNERS.get(_database_key(config))
    if owner is not None:
        owner.connectors.append(connector)
        original_close = connector.close

        def close_owned() -> None:
            original_close()
            if connector in owner.connectors:
                owner.connectors.remove(connector)

        # Release ownership only after the real native close succeeds. SQLite
        # retains its closed connection attribute, whose second close on another
        # thread raises even though the owning worker already cleaned up.
        connector.close = close_owned  # type: ignore[method-assign]  # Test-only lifetime observation preserves the native close implementation.
    return connector


def open_database(config: CoreConfig) -> SQLConnector:
    """Return a connected connector, for try/finally or contextlib.closing()."""

    connector = database_connector(config)
    connector.connect()
    return connector


def set_foreign_key_checks(connector: SQLConnector, *, enabled: bool) -> None:
    """Native corruption-fixture control, outside any managed transaction."""

    if connector_backend(connector) == "sqlite":
        connector.execute(f"PRAGMA foreign_keys = {int(enabled)}")
    else:
        connector.execute(f"SET SESSION foreign_key_checks = {int(enabled)}")


def foreign_key_checks_enabled(connector: SQLConnector) -> bool:
    query = (
        "PRAGMA foreign_keys"
        if connector_backend(connector) == "sqlite"
        else "SELECT @@SESSION.foreign_key_checks"
    )
    result = inspect_one(connector, query)
    assert result in ((0,), (1,)), "engine returned an invalid FK enforcement state"
    return result == (1,)


def set_check_constraints(connector: SQLConnector, *, enabled: bool) -> None:
    """Permit intentionally malformed rows through each engine's native switch."""

    if connector_backend(connector) == "sqlite":
        connector.execute(f"PRAGMA ignore_check_constraints = {int(not enabled)}")
    else:
        connector.execute(f"SET SESSION check_constraint_checks = {int(enabled)}")


def _identifier(value: str) -> str:
    if not value or not all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in value
    ):
        raise ValueError("invalid schema identifier in test database")
    return f"`{value}`"


def table_columns(connector: SQLConnector, table: str) -> tuple[str, ...]:
    """Read physical columns in declared order using the actual engine catalog."""

    if connector_backend(connector) == "sqlite":
        return tuple(
            str(row[1])
            for row in inspect_all(
                connector, f"PRAGMA table_info({_identifier(table)})"
            )
        )
    return tuple(
        str(row[0])
        for row in inspect_all(
            connector,
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (table,),
        )
    )


def snapshot_rows(connector: SQLConnector) -> dict[str, tuple[tuple[Any, ...], ...]]:
    """Capture every actual base table's exact row multiset in one native snapshot."""

    with inspection_snapshot(connector):
        if connector_backend(connector) == "sqlite":
            names = connector.fetch_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        else:
            names = connector.fetch_all(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
        return {
            str(name): tuple(
                sorted(
                    connector.fetch_all(f"SELECT * FROM {_identifier(str(name))}"),
                    key=repr,
                )
            )
            for (name,) in names
        }


def assert_foreign_key_integrity(connector: SQLConnector) -> None:
    """Reject orphan tuples, including composite native MariaDB foreign keys."""

    with inspection_snapshot(connector):
        _assert_foreign_key_integrity_in_snapshot(connector)


def _assert_foreign_key_integrity_in_snapshot(connector: SQLConnector) -> None:

    if connector_backend(connector) == "sqlite":
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        return
    constraints: dict[tuple[str, str, str], list[tuple[str, str]]] = defaultdict(list)
    rows = connector.fetch_all(
        "SELECT TABLE_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, "
        "COLUMN_NAME, REFERENCED_COLUMN_NAME "
        "FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL "
        "ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"
    )
    for table, constraint, parent, column, parent_column in rows:
        constraints[(str(table), str(constraint), str(parent))].append(
            (str(column), str(parent_column))
        )
    for (table, constraint, parent), columns in constraints.items():
        present = " AND ".join(
            f"child.{_identifier(column)} IS NOT NULL" for column, _ in columns
        )
        matching = " AND ".join(
            f"parent.{_identifier(parent_column)} = child.{_identifier(column)}"
            for column, parent_column in columns
        )
        orphan = connector.fetch_one(
            f"SELECT 1 FROM {_identifier(table)} AS child WHERE {present} "
            f"AND NOT EXISTS (SELECT 1 FROM {_identifier(parent)} AS parent WHERE {matching}) LIMIT 1"
        )
        assert orphan == (), f"orphan rows violate {table}.{constraint} -> {parent}"


@cache
def _definition(backend: Backend) -> SchemaEpochDefinition:
    return GeneratedVNextSchemaProvider(backend).definition


def open_generated_database(config: CoreConfig) -> SQLConnector:
    """Build native generated DDL and bootstrap facts in a fresh fixture DB."""

    connector = open_database(config)
    backend = connector_backend(connector)
    definition = _definition(backend)

    def ddl() -> None:
        for schema_slice in definition.slices:
            for statement in schema_slice.statements:
                connector.execute(statement.sql)

    def bootstrap() -> None:
        for sql, seeds in groupby(
            definition.bootstrap_seeds, key=lambda seed: seed.sql
        ):
            parameters = [seed.parameters for seed in seeds]
            for offset in range(0, len(parameters), 128):
                connector.execute_many(sql, parameters[offset : offset + 128])

    try:
        if backend == "sqlite":
            with connector.transaction():
                ddl()
                bootstrap()
        else:
            # MariaDB DDL commits implicitly. Only the bounded bootstrap DML is
            # transactional; the fixture owns teardown after any DDL failure.
            ddl()
            with connector.transaction():
                bootstrap()
    except BaseException as error:
        try:
            connector.close()
        except BaseException as close_error:
            error.add_note(f"generated {backend} connector close failed: {close_error}")
        raise
    return connector
