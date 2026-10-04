"""Native database fixtures preserve lifetime, dialect and integrity evidence."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
from vnext_fault_harness import FaultInjector, InjectedFault, fault_injection
from vnext_test_database import (
    DatabaseFactory,
    assert_foreign_key_integrity,
    atomic_fixture,
    connector_backend,
    database_connector,
    foreign_key_checks_enabled,
    inspect_all,
    inspect_one,
    open_database,
    set_foreign_key_checks,
    table_columns,
)


def test_worker_closed_connections_are_not_closed_again_by_the_fixture_owner(
    database_factory: DatabaseFactory,
) -> None:
    config = database_factory.config("worker")

    def use_native_connection() -> None:
        with database_connector(config) as connector:
            assert inspect_one(connector, "SELECT 1") == (1,)

    with ThreadPoolExecutor(max_workers=1) as workers:
        workers.submit(use_native_connection).result(timeout=20)
    database_factory.release("worker")


def test_native_helpers_preserve_installed_statement_fault_injection(
    database_factory: DatabaseFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = database_factory.config("injected")
    with database_connector(config) as connector:
        connector.execute("CREATE TABLE fault_probe (id INTEGER PRIMARY KEY)")
    injector = FaultInjector(fail_before_mutation=1)
    with (
        fault_injection(monkeypatch, injector),
        database_connector(config) as connector,
    ):
        with pytest.raises(InjectedFault), connector.transaction():
            connector.execute("INSERT INTO fault_probe VALUES (1)")
    assert injector.fired == "before_mutation"
    with database_connector(config) as connector:
        assert inspect_one(connector, "SELECT COUNT(*) FROM fault_probe") == (0,)


def test_native_fixture_reopen_isolation_and_explicit_release(
    database_factory: DatabaseFactory,
) -> None:
    first = database_factory.config("first")
    second = database_factory.config("second")
    with closing(open_database(first)) as connector:
        assert connector_backend(connector) == database_factory.backend
        connector.execute(
            "CREATE TABLE fixture_parent (first_key INTEGER NOT NULL, second_key INTEGER NOT NULL, PRIMARY KEY (first_key, second_key))"
        )
        connector.execute(
            "CREATE TABLE fixture_child (first_key INTEGER NOT NULL, second_key INTEGER NOT NULL, PRIMARY KEY (first_key, second_key), FOREIGN KEY (first_key, second_key) REFERENCES fixture_parent(first_key, second_key))"
        )
        assert foreign_key_checks_enabled(connector)
        assert table_columns(connector, "fixture_child") == ("first_key", "second_key")
        with connector.transaction():
            connector.execute("INSERT INTO fixture_parent VALUES (1, 2)")
            connector.execute("INSERT INTO fixture_child VALUES (1, 2)")
        assert_foreign_key_integrity(connector)
        set_foreign_key_checks(connector, enabled=False)
        assert not foreign_key_checks_enabled(connector)
        connector.execute("INSERT INTO fixture_child VALUES (1, 3)")
        set_foreign_key_checks(connector, enabled=True)
        with pytest.raises(AssertionError):
            assert_foreign_key_integrity(connector)
    assert database_factory.config("first") is first
    with closing(open_database(first)) as connector:
        assert inspect_one(connector, "SELECT COUNT(*) FROM fixture_child") == (2,)
    with closing(open_database(second)) as connector:
        assert not connector.check_table_exists("fixture_child")
    database_factory.release("first")
    with closing(open_database(database_factory.config("first"))) as connector:
        assert not connector.check_table_exists("fixture_child")


def test_fixture_owner_closes_abandoned_raw_connections_before_drop(
    database_factory: DatabaseFactory,
) -> None:
    config = database_factory.config("abandoned")
    connector = open_database(config)
    connector.execute("CREATE TABLE fixture_owned (id INTEGER PRIMARY KEY)")
    connector.begin()
    connector.execute("INSERT INTO fixture_owned VALUES (1)")
    database_factory.release("abandoned")
    with closing(open_database(database_factory.config("abandoned"))) as fresh:
        assert not fresh.check_table_exists("fixture_owned")


def test_oracle_reads_and_atomic_seeds_preserve_transaction_boundaries(
    database_factory: DatabaseFactory,
) -> None:
    from h2hdb.sql_connector import SQLConnector

    @atomic_fixture
    def seed(connector: SQLConnector, value: int) -> tuple[object, ...]:
        connector.execute("INSERT INTO fixture_values VALUES (%s)", (value,))
        return connector.fetch_one(
            "SELECT id FROM fixture_values WHERE id = %s", (value,)
        )

    with closing(open_database(database_factory.config("inspection"))) as connector:
        connector.execute("CREATE TABLE fixture_values (id INTEGER PRIMARY KEY)")
        assert seed(connector, 1) == (1,)
        assert inspect_all(connector, "SELECT id FROM fixture_values") == [(1,)]
        with pytest.raises(RuntimeError, match="outer transaction"):
            with connector.transaction():
                assert seed(connector, 2) == (2,)
                assert inspect_one(
                    connector, "SELECT COUNT(*) FROM fixture_values"
                ) == (2,)
                raise RuntimeError("outer transaction")
        assert inspect_one(connector, "SELECT COUNT(*) FROM fixture_values") == (1,)
