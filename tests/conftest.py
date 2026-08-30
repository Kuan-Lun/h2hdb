import os
import uuid
from collections.abc import Collection, Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import CoreConfig, DatabaseConfig

MARIADB_IMAGE = "mariadb:10.11.11"
MARIADB_VERSION_PREFIX = "10.11.11-"
MARIADB_ROOT_PASSWORD = "h2hdb-test-root"
MARIADB_USER = "h2hdb"
MARIADB_PASSWORD = "h2hdb-test-password"
MARIADB_MAX_ALLOWED_PACKET = 1024 * 1024
MARIADB_XDIST_GROUP = "live-mariadb"


def live_mariadb_xdist_group(
    fixture_names: Collection[str],
    parameters: Mapping[str, object],
) -> str | None:
    if (
        "mariadb_container" in fixture_names
        or "mariadb_config" in fixture_names
        or parameters.get("db_config") == "mariadb"
    ):
        return MARIADB_XDIST_GROUP
    return None


def _item_fixture_names(item: pytest.Item) -> tuple[str, ...]:
    raw_fixture_names: object = getattr(item, "fixturenames", ())
    if not isinstance(raw_fixture_names, (list, tuple)):
        return ()
    return tuple(
        fixture_name
        for fixture_name in raw_fixture_names
        if isinstance(fixture_name, str)
    )


def _item_parameters(item: pytest.Item) -> Mapping[str, object]:
    callspec: object | None = getattr(item, "callspec", None)
    raw_parameters: object = getattr(callspec, "params", {})
    if not isinstance(raw_parameters, Mapping):
        return {}
    return cast(Mapping[str, object], raw_parameters)


def _xdist_group_name(marker: pytest.Mark) -> str:
    if marker.args:
        return str(marker.args[0])
    return str(marker.kwargs.get("name", "default"))


def live_mariadb_group_marker_required(
    existing_group_markers: Collection[pytest.Mark],
) -> bool:
    if not existing_group_markers:
        return True
    existing_group_names = {
        _xdist_group_name(marker) for marker in existing_group_markers
    }
    if existing_group_names != {MARIADB_XDIST_GROUP}:
        msg = (
            f"requires xdist group {MARIADB_XDIST_GROUP!r}, but declares "
            f"{sorted(existing_group_names, key=repr)!r}"
        )
        raise ValueError(msg)
    return False


def claim_live_mariadb_container(guard_root: Path, testrun_uid: str) -> Path:
    guard_path = guard_root / f"h2hdb-mariadb-container-{testrun_uid}"
    try:
        guard_path.mkdir()
    except FileExistsError as error:
        msg = "MariaDB integration tests escaped their single xdist worker group"
        raise RuntimeError(msg) from error
    return guard_path


# Xdist consumes group markers during this same hook phase, so this classifier
# must run first instead of relying on plugin registration order.
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        group_name = live_mariadb_xdist_group(
            _item_fixture_names(item),
            _item_parameters(item),
        )
        if group_name is None:
            continue

        existing_group_markers = tuple(item.iter_markers(name="xdist_group"))
        try:
            marker_required = live_mariadb_group_marker_required(existing_group_markers)
        except ValueError as error:
            raise pytest.UsageError(f"{item.nodeid} {error}") from error
        if marker_required:
            item.add_marker(
                pytest.mark.xdist_group(name=group_name),
                append=False,
            )
        item.add_marker(pytest.mark.mariadb, append=False)


@pytest.fixture(scope="session")
def mariadb_container(
    tmp_path_factory: pytest.TempPathFactory,
    testrun_uid: str,
    worker_id: str,
) -> Iterator[Any]:
    if os.environ.get("H2HDB_TEST_MARIADB") != "1":
        pytest.skip("set H2HDB_TEST_MARIADB=1 to run MariaDB integration tests")

    try:
        worker_temp_root = tmp_path_factory.getbasetemp()
        shared_temp_root = (
            worker_temp_root if worker_id == "master" else worker_temp_root.parent
        )
        claim_live_mariadb_container(
            shared_temp_root,
            testrun_uid,
        )
    except RuntimeError as error:
        pytest.fail(
            str(error),
            pytrace=False,
        )
    # Keep the atomic claim for the whole pytest run. The shared pytest temp root
    # owns cleanup; removing it during worker teardown could let a late,
    # misclassified node start a second container.
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
            command=f"--max-allowed-packet={MARIADB_MAX_ALLOWED_PACKET}",
        )
        started = container.start()
    except Exception as error:
        pytest.fail(
            f"MariaDB tests were enabled but the testcontainer is unavailable: {error}",
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
            cursor.execute("SELECT VERSION()")
            version_row = cursor.fetchone()
            assert version_row is not None
            (server_version,) = version_row
            assert str(server_version).startswith(MARIADB_VERSION_PREFIX), (
                server_version
            )
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
    # Must be a real file, not `:memory:`: facade calls open fresh connections,
    # and SQLite's in-memory databases are connection-scoped.
    database_path = tmp_path / "h2hdb_test.sqlite3"
    return CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
    )


@pytest.fixture(
    params=(
        pytest.param("sqlite", id="sqlite"),
        pytest.param("mariadb", id="mariadb"),
    )
)
def db_config(request: pytest.FixtureRequest) -> CoreConfig:
    return cast(CoreConfig, request.getfixturevalue(f"{request.param}_config"))
