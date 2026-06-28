import uuid
from collections.abc import Iterator

import mysql.connector
import pytest
from testcontainers.mysql import MySqlContainer

from h2hdb import DatabaseConfig, H2HDBConfig

MARIADB_IMAGE = "mariadb:11"
MARIADB_ROOT_PASSWORD = "h2hdb-test-root"
MARIADB_USER = "h2hdb"
MARIADB_PASSWORD = "h2hdb-test-password"


@pytest.fixture(scope="session")
def mariadb_container() -> Iterator[MySqlContainer]:
    container = MySqlContainer(
        image=MARIADB_IMAGE,
        username=MARIADB_USER,
        password=MARIADB_PASSWORD,
        root_password=MARIADB_ROOT_PASSWORD,
        dbname="h2hdb_template",
    )
    with container as started:
        yield started


@pytest.fixture
def mariadb_config(mariadb_container: MySqlContainer) -> Iterator[H2HDBConfig]:
    host = mariadb_container.get_container_host_ip()
    port = int(mariadb_container.get_exposed_port(mariadb_container.port))
    database = f"h2hdb_test_{uuid.uuid4().hex[:12]}"

    admin_connection = mysql.connector.connect(
        host=host, port=port, user="root", password=MARIADB_ROOT_PASSWORD
    )
    try:
        with admin_connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE `{database}`")
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{database}`.* TO %s",
                (MARIADB_USER,),
            )
        admin_connection.commit()
    finally:
        admin_connection.close()

    config = H2HDBConfig(
        database=DatabaseConfig(
            sql_type="mysql",
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
