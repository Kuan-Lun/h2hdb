import os
import platform
import subprocess
import uuid
from collections.abc import Collection, Iterator, Mapping
from contextlib import ExitStack, closing, contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest
from vnext_test_database import DatabaseFactory, database_connection_lifetime

from h2hdb import CoreConfig, DatabaseConfig

pytest_plugins = ("backend_contract",)

MARIADB_IMAGE = "mariadb:10.11.11"
MARIADB_VERSION_PREFIX = "10.11.11-"
MARIADB_ROOT_PASSWORD = "h2hdb-test-root"
MARIADB_USER = "h2hdb"
MARIADB_PASSWORD = "h2hdb-test-password"
MARIADB_MAX_ALLOWED_PACKET = 1024 * 1024
MARIADB_XDIST_GROUP = "live-mariadb"
AUTO_WORKER_CAP = 10
MARIADB_AUTO_WORKER_CAP = 4
OVERRIDE_WORKER_CAP = 16
PYTEST_WORKER_OVERRIDE = "H2HDB_PYTEST_WORKERS"
MACOS_PERFORMANCE_CORE_SYSCTL = "hw.perflevel0.physicalcpu"
MACOS_SYSCTL_TIMEOUT_SECONDS = 2
DEEP_TEST_FILES = frozenset(
    {
        "test_storage_guest_powercut_harness.py",
        "test_operational_refinement_runtime.py",
        "test_vnext_schema_provider_generation.py",
        "test_vnext_bootstrap_fault_matrix.py",
        "test_vnext_identity_corruption_matrix.py",
        "test_vnext_live_authority_races.py",
        "test_vnext_physical_domain_fault_matrix.py",
        "test_vnext_pipeline_fault_matrix.py",
        "test_vnext_pipeline_stage_authority.py",
        "test_vnext_pipeline_takeover_matrix.py",
        "test_vnext_pipeline_workflows.py",
    }
)


def select_pytest_worker_count(
    *,
    override: str | None,
    mariadb_enabled: bool,
    system: str,
    macos_performance_cores: int | None,
    process_cpus: int | None,
) -> int:
    if override is not None:
        if not override.isascii() or not override.isdigit():
            msg = (
                f"{PYTEST_WORKER_OVERRIDE} must be an integer from 1 through "
                f"{OVERRIDE_WORKER_CAP}"
            )
            raise ValueError(msg)
        worker_count = int(override)
        if not 1 <= worker_count <= OVERRIDE_WORKER_CAP:
            msg = (
                f"{PYTEST_WORKER_OVERRIDE} must be an integer from 1 through "
                f"{OVERRIDE_WORKER_CAP}"
            )
            raise ValueError(msg)
        return worker_count

    detected_count: int | None
    if (
        system == "Darwin"
        and macos_performance_cores is not None
        and macos_performance_cores > 0
    ):
        detected_count = macos_performance_cores
        if process_cpus is not None and process_cpus > 0:
            detected_count = min(detected_count, process_cpus)
    else:
        detected_count = process_cpus
    if detected_count is None or detected_count < 1:
        detected_count = 1
    workload_cap = MARIADB_AUTO_WORKER_CAP if mariadb_enabled else AUTO_WORKER_CAP
    return min(detected_count, workload_cap)


def macos_performance_core_count() -> int | None:
    try:
        completed = subprocess.run(
            ["/usr/sbin/sysctl", "-n", MACOS_PERFORMANCE_CORE_SYSCTL],
            check=False,
            capture_output=True,
            text=True,
            timeout=MACOS_SYSCTL_TIMEOUT_SECONDS,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    if not value.isascii() or not value.isdigit():
        return None
    count = int(value)
    return count if count > 0 else None


def pytest_xdist_auto_num_workers(config: pytest.Config) -> int:
    del config
    override = os.environ.get(PYTEST_WORKER_OVERRIDE)
    mariadb_enabled = os.environ.get("H2HDB_TEST_MARIADB") == "1"
    system = platform.system()
    performance_cores = (
        macos_performance_core_count()
        if override is None and system == "Darwin"
        else None
    )
    process_cpus = os.process_cpu_count() if override is None else None
    try:
        return select_pytest_worker_count(
            override=override,
            mariadb_enabled=mariadb_enabled,
            system=system,
            macos_performance_cores=performance_cores,
            process_cpus=process_cpus,
        )
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error


def live_mariadb_xdist_group(
    fixture_names: Collection[str],
    parameters: Mapping[str, object],
    *,
    declared_live: bool = False,
) -> str | None:
    if (
        declared_live
        or "mariadb_container" in fixture_names
        or "mariadb_config" in fixture_names
        or any(
            parameters.get(name) == "mariadb"
            for name in ("db_config", "database_factory")
        )
    ):
        return MARIADB_XDIST_GROUP
    return None


def item_requires_deep_profile(
    *,
    test_file_name: str,
    marker_names: Collection[str],
    live_mariadb: bool,
) -> bool:
    declared_markers = frozenset(marker_names)
    heavy_file_requires_deep = (
        test_file_name in DEEP_TEST_FILES and "merge_smoke" not in declared_markers
    )
    live_mariadb_requires_deep = (
        live_mariadb and "mariadb_smoke" not in declared_markers
    )
    return heavy_file_requires_deep or live_mariadb_requires_deep


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
            declared_live=item.get_closest_marker("mariadb") is not None,
        )
        live_mariadb = group_name is not None
        if group_name is not None:
            existing_group_markers = tuple(item.iter_markers(name="xdist_group"))
            try:
                marker_required = live_mariadb_group_marker_required(
                    existing_group_markers
                )
            except ValueError as error:
                raise pytest.UsageError(f"{item.nodeid} {error}") from error
            if marker_required:
                item.add_marker(
                    pytest.mark.xdist_group(name=group_name),
                    append=False,
                )
            item.add_marker(pytest.mark.mariadb, append=False)

        if item_requires_deep_profile(
            test_file_name=item.path.name,
            marker_names={marker.name for marker in item.iter_markers()},
            live_mariadb=live_mariadb,
        ):
            item.add_marker(pytest.mark.deep, append=False)


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


@contextmanager
def _mariadb_database(mariadb_container: Any) -> Iterator[CoreConfig]:
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
        with database_connection_lifetime(config):
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
def mariadb_config(mariadb_container: Any) -> Iterator[CoreConfig]:
    with _mariadb_database(mariadb_container) as config:
        yield config


_FACTORY_BACKENDS = (
    pytest.param("sqlite", id="sqlite"),
    pytest.param(
        "mariadb", id="mariadb", marks=(pytest.mark.mariadb, pytest.mark.deep)
    ),
)


@contextmanager
def _owned_database_factory(
    request: pytest.FixtureRequest, tmp_path: Path, backend: str
) -> Iterator[DatabaseFactory]:
    with ExitStack() as cleanup:
        if backend == "sqlite":

            def allocate(name: str) -> CoreConfig:
                key = sha256(name.encode()).hexdigest()
                return CoreConfig(
                    database=DatabaseConfig(
                        sql_type="sqlite", database=str(tmp_path / f"{key}.sqlite3")
                    )
                )

            def dispose_sqlite(config: CoreConfig) -> None:
                path = Path(config.database.database)
                for owned_path in (
                    path,
                    Path(str(path) + "-wal"),
                    Path(str(path) + "-shm"),
                ):
                    owned_path.unlink(missing_ok=True)

            with closing(
                DatabaseFactory("sqlite", allocate, dispose_sqlite)
            ) as factory:
                yield factory
        else:
            container = request.getfixturevalue("mariadb_container")

            databases: dict[str, ExitStack] = {}

            def allocate_mariadb(_name: str) -> CoreConfig:
                lifetime = ExitStack()
                cleanup.callback(lifetime.close)
                config = lifetime.enter_context(_mariadb_database(container))
                databases[config.database.database] = lifetime
                return config

            def dispose_mariadb(config: CoreConfig) -> None:
                databases.pop(config.database.database).close()

            with closing(
                DatabaseFactory("mariadb", allocate_mariadb, dispose_mariadb)
            ) as factory:
                yield factory


@pytest.fixture(params=_FACTORY_BACKENDS)
def database_factory(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[DatabaseFactory]:
    """Own every named native database used by one portable test."""
    with _owned_database_factory(request, tmp_path, request.param) as factory:
        yield factory


@pytest.fixture(scope="module", params=_FACTORY_BACKENDS)
def module_database_factory(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[DatabaseFactory]:
    """Own immutable fault corpora and reusable targets across one module."""
    with _owned_database_factory(
        request, tmp_path_factory.mktemp("fault-corpora"), request.param
    ) as factory:
        yield factory


@pytest.fixture
def mariadb_database_factory(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[DatabaseFactory]:
    """Named native databases for explicitly engine-specific MariaDB contracts.

    The connection guard still requires a backend_specific reason on each case;
    portable cases must instead use the parametrized database_factory.
    """
    with _owned_database_factory(request, tmp_path, "mariadb") as factory:
        yield factory


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
