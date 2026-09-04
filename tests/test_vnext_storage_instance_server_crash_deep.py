from __future__ import annotations

import multiprocessing
import os
import signal
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any, Literal, NoReturn, cast

import pytest
from testcontainers.community.mysql import MySqlContainer
from testcontainers.core.docker_client import DockerClient

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    StorageInstanceBindingMismatchError,
    VNextDatabaseAdminFacade,
)
from h2hdb.mariadb_connector import MariaDBConnector

pytestmark = [
    pytest.mark.deep,
    pytest.mark.mariadb,
    pytest.mark.mariadb_server_crash,
]

_MARIADB_IMAGE = "mariadb:10.11.11"
_MARIADB_VERSION_PREFIX = "10.11.11-"
_MARIADB_ROOT_PASSWORD = "h2hdb-crash-test-root"
_MARIADB_USER = "h2hdb_crash"
_MARIADB_PASSWORD = "h2hdb-crash-test-password"
_MARIADB_TEMPLATE_DATABASE = "h2hdb_crash_template"
_MARIADB_MAX_ALLOWED_PACKET = 1024 * 1024
_DOCKER_API_TIMEOUT_SECONDS = 30
_CONTAINER_STOP_TIMEOUT_SECONDS = 30
_BARRIER_TIMEOUT_SECONDS = 30.0
_CHILD_REAP_TIMEOUT_SECONDS = 10.0
_CHILD_HOLD_TIMEOUT_SECONDS = 120.0

_COMMITTED_UUID = bytes.fromhex("00000000000040008000000000000011")
_ROLLED_BACK_UUID = bytes.fromhex("00000000000040008000000000000022")
_RECOVERY_UUID = bytes.fromhex("00000000000040008000000000000033")

_CommitBoundary = Literal["before-commit", "after-commit"]


def _new_container(*, name: str, volume_name: str) -> Any:
    container = MySqlContainer(
        image=_MARIADB_IMAGE,
        username=_MARIADB_USER,
        password=_MARIADB_PASSWORD,
        root_password=_MARIADB_ROOT_PASSWORD,
        dbname=_MARIADB_TEMPLATE_DATABASE,
        wait_strategy_check_string=(
            r".*: ready for connections\..*socket: .*port: 3306"
        ),
        command=(
            f"--max-allowed-packet={_MARIADB_MAX_ALLOWED_PACKET} "
            "--innodb-flush-log-at-trx-commit=1"
        ),
        name=name,
        labels={
            "h2hdb.test-purpose": "mariadb-server-crash-recovery",
            "h2hdb.test-owner": name,
        },
        docker_client_kw={"timeout": _DOCKER_API_TIMEOUT_SECONDS},
    )
    return container.with_volume_mapping(volume_name, "/var/lib/mysql", "rw")


def _database_config(container: Any, database: str) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb",
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(container.port)),
            user=_MARIADB_USER,
            password=_MARIADB_PASSWORD,
            database=database,
        )
    )


def _provision_database(container: Any, database: str) -> CoreConfig:
    import mysql.connector

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(container.port))
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user="root",
        password=_MARIADB_ROOT_PASSWORD,
        connection_timeout=_DOCKER_API_TIMEOUT_SECONDS,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            row = cast(tuple[object, ...] | None, cursor.fetchone())
            if row is None or not str(row[0]).startswith(_MARIADB_VERSION_PREFIX):
                raise RuntimeError(f"unexpected MariaDB version: {row!r}")
            cursor.execute(
                f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 "
                "COLLATE utf8mb4_bin"
            )
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{database}`.* TO %s",
                (_MARIADB_USER,),
            )
        connection.commit()
    finally:
        connection.close()
    return _database_config(container, database)


def _pause_bind_at_commit(
    config: CoreConfig,
    storage_instance_uuid: bytes,
    boundary: _CommitBoundary,
    reached: Any,
    release: Any,
) -> NoReturn:
    original_commit: Callable[[MariaDBConnector], None] = MariaDBConnector.commit
    intercepted = False

    def commit(connector: MariaDBConnector) -> None:
        nonlocal intercepted
        if intercepted:
            original_commit(connector)
            return
        intercepted = True
        if boundary == "after-commit":
            original_commit(connector)
        reached.set()
        if not release.wait(_CHILD_HOLD_TIMEOUT_SECONDS):
            os._exit(92)
        if boundary == "before-commit":
            original_commit(connector)

    cast(Any, MariaDBConnector).commit = commit
    try:
        VNextDatabaseAdminFacade(config).bind_storage_instance(storage_instance_uuid)
    except BaseException:
        os._exit(90)
    os._exit(91)


def _wait_for_boundary(
    process: Any,
    reached: Any,
    *,
    label: str,
) -> None:
    deadline = time.monotonic() + _BARRIER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if reached.wait(0.05):
            if not process.is_alive():
                pytest.fail(
                    f"{label} child exited after reporting its commit boundary: "
                    f"{process.exitcode}",
                    pytrace=False,
                )
            return
        if not process.is_alive():
            pytest.fail(
                f"{label} child exited before its commit boundary: {process.exitcode}",
                pytrace=False,
            )
    pytest.fail(
        f"{label} child did not reach its commit boundary within "
        f"{_BARRIER_TIMEOUT_SECONDS:.0f} seconds",
        pytrace=False,
    )


def _kill_and_reap(process: Any) -> None:
    if process.is_alive():
        process.kill()
    process.join(_CHILD_REAP_TIMEOUT_SECONDS)
    if process.is_alive():
        raise RuntimeError(
            f"crash-injection child {process.pid} survived kill and bounded join"
        )


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="POSIX SIGKILL only")
def test_live_mariadb_server_sigkill_recreated_container_recovers_named_volume_transactions() -> (  # noqa: E501 -- test name is the exact evidence claim.
    None
):
    """Recover one response-lost commit and roll back one open transaction.

    Docker, its Linux kernel, the host page cache, and physical storage remain
    alive.  This is deliberately server-process crash recovery across a named
    volume, not a host power-loss test.  The production connector has received
    the server's commit response, but the public facade has not returned it to
    its caller when both the server and client process are killed.
    """

    if os.environ.get("H2HDB_TEST_MARIADB") != "1":
        pytest.skip("set H2HDB_TEST_MARIADB=1 to run MariaDB integration tests")

    run_id = uuid.uuid4().hex
    volume_name = f"h2hdb-server-crash-{run_id}"
    container_names = (
        f"h2hdb-server-crash-a-{run_id}",
        f"h2hdb-server-crash-b-{run_id}",
    )
    control = DockerClient(timeout=_DOCKER_API_TIMEOUT_SECONDS)
    volume: Any | None = None
    containers: list[Any] = []
    children: list[Any] = []
    cleanup_errors: list[Exception] = []
    try:
        volume = control.client.volumes.create(
            name=volume_name,
            labels={"h2hdb.test-purpose": "mariadb-server-crash-recovery"},
        )
        first = _new_container(name=container_names[0], volume_name=volume_name)
        containers.append(first)
        first.start()

        committed_database = f"h2hdb_committed_{run_id[:12]}"
        open_database = f"h2hdb_open_{run_id[:12]}"
        committed_config = _provision_database(first, committed_database)
        open_config = _provision_database(first, open_database)
        VNextDatabaseAdminFacade(committed_config).initialize()
        VNextDatabaseAdminFacade(open_config).initialize()

        context = multiprocessing.get_context("spawn")
        committed_reached = context.Event()
        committed_release = context.Event()
        open_reached = context.Event()
        open_release = context.Event()
        committed_child = context.Process(
            target=_pause_bind_at_commit,
            args=(
                committed_config,
                _COMMITTED_UUID,
                "after-commit",
                committed_reached,
                committed_release,
            ),
        )
        open_child = context.Process(
            target=_pause_bind_at_commit,
            args=(
                open_config,
                _ROLLED_BACK_UUID,
                "before-commit",
                open_reached,
                open_release,
            ),
        )
        children.extend((committed_child, open_child))
        committed_child.start()
        open_child.start()
        _wait_for_boundary(
            committed_child,
            committed_reached,
            label="response-lost-commit",
        )
        _wait_for_boundary(open_child, open_reached, label="open-transaction")

        wrapped = first.get_wrapped_container()
        wrapped.kill(signal="SIGKILL")
        stopped = wrapped.wait(timeout=_CONTAINER_STOP_TIMEOUT_SECONDS)
        assert int(stopped["StatusCode"]) != 0
        for child in children:
            _kill_and_reap(child)
            assert child.exitcode == -signal.SIGKILL

        second = _new_container(name=container_names[1], volume_name=volume_name)
        containers.append(second)
        second.start()
        committed_restarted = _database_config(second, committed_database)
        open_restarted = _database_config(second, open_database)

        committed_admin = VNextDatabaseAdminFacade(committed_restarted)
        with pytest.raises(
            StorageInstanceBindingMismatchError,
            match="different storage instance",
        ):
            committed_admin.bind_storage_instance(_RECOVERY_UUID)
        assert (
            committed_admin.bind_storage_instance(_COMMITTED_UUID).storage_instance_uuid
            == _COMMITTED_UUID
        )
        committed_admin.check()

        open_admin = VNextDatabaseAdminFacade(open_restarted)
        assert (
            open_admin.bind_storage_instance(_RECOVERY_UUID).storage_instance_uuid
            == _RECOVERY_UUID
        )
        open_admin.check()
    finally:
        active_exception = sys.exception()
        for child in children:
            try:
                _kill_and_reap(child)
            except Exception as error:
                cleanup_errors.append(error)
        for container in reversed(containers):
            try:
                container.stop(force=True, delete_volume=False)
            except Exception as error:
                cleanup_errors.append(error)
        if volume is not None:
            try:
                volume.remove(force=True)
            except Exception as error:
                cleanup_errors.append(error)
        try:
            control.client.close()
        except Exception as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            if active_exception is not None:
                for cleanup_error in cleanup_errors:
                    active_exception.add_note(
                        f"crash-test cleanup failed: {cleanup_error!r}"
                    )
            else:
                raise ExceptionGroup(
                    "MariaDB server-crash test cleanup failed",
                    cleanup_errors,
                )
