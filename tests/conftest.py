import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import CoreConfig, DatabaseConfig

MARIADB_IMAGE = "mariadb:11"
MARIADB_ROOT_PASSWORD = "h2hdb-test-root"
MARIADB_USER = "h2hdb"
MARIADB_PASSWORD = "h2hdb-test-password"


@pytest.fixture(scope="session")
def mariadb_container() -> Iterator[Any]:
    if os.environ.get("H2HDB_TEST_MARIADB") != "1":
        pytest.skip("set H2HDB_TEST_MARIADB=1 to run MariaDB integration tests")
    try:
        from testcontainers.community.mysql import MySqlContainer
    except ImportError as error:
        pytest.fail(
            f"MariaDB tests were enabled but dependencies are unavailable: {error}",
            pytrace=False,
        )
    try:
        container = MySqlContainer(
            image=MARIADB_IMAGE,
            username=MARIADB_USER,
            password=MARIADB_PASSWORD,
            root_password=MARIADB_ROOT_PASSWORD,
            dbname="h2hdb_template",
        )
        started = container.start()
    except Exception as error:
        pytest.fail(
            f"MariaDB tests were enabled but the testcontainer is unavailable: "
            f"{error}",
            pytrace=False,
        )
    try:
        yield started
    finally:
        container.stop()


@pytest.fixture
def mariadb_config(mariadb_container: Any) -> Iterator[CoreConfig]:
    try:
        import mysql.connector
    except ImportError as error:
        pytest.fail(
            f"MariaDB tests were enabled but the connector is unavailable: {error}",
            pytrace=False,
        )
    host = mariadb_container.get_container_host_ip()
    port = int(mariadb_container.get_exposed_port(mariadb_container.port))
    database = f"h2hdb_test_{uuid.uuid4().hex[:12]}"

    admin_connection = mysql.connector.connect(
        host=host, port=port, user="root", password=MARIADB_ROOT_PASSWORD
    )
    try:
        with admin_connection.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
            )
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{database}`.* TO %s",
                (MARIADB_USER,),
            )
        admin_connection.commit()
    finally:
        admin_connection.close()

    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb",
            host=host,
            port=port,
            user=MARIADB_USER,
            password=MARIADB_PASSWORD,
            database=database,
        )
    )
    try:
        yield config
    finally:
        admin_connection = mysql.connector.connect(
            host=host, port=port, user="root", password=MARIADB_ROOT_PASSWORD
        )
        try:
            with admin_connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS `{database}`")
            admin_connection.commit()
        finally:
            admin_connection.close()


@pytest.fixture
def sqlite_config(tmp_path: Path) -> CoreConfig:
    # Must be a real file, not `:memory:`: every H2HDB method opens its own
    # connection, and SQLite's in-memory databases are connection-scoped.
    database_path = tmp_path / "h2hdb_test.sqlite3"
    return CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
    )


@pytest.fixture(params=["sqlite", "mariadb"])
def db_config(request: pytest.FixtureRequest) -> CoreConfig:
    return cast(CoreConfig, request.getfixturevalue(f"{request.param}_config"))
