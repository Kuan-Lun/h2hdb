"""Real server-ended idle sessions must not fail the next fenced operation."""

from collections.abc import Callable
from contextlib import closing
from typing import Any, cast

import mysql.connector
import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract

from h2hdb import (
    CoreConfig,
    DatabaseAuditPolicy,
    DatabaseAuditReason,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_transaction import VNextUnitOfWork

_MARIADB_CONTRACT = pytest.mark.backend_specific(
    backend="mariadb",
    reason="MariaDB server KILL CONNECTION and stale pooled-session admission have no SQLite server analogue",
)


def _kill_idle(config: CoreConfig, identity: int) -> None:
    # The disposable test user can kill its own connections. This deliberately
    # bypasses the victim context, whose only session must remain idle.
    with MariaDBConnector(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database,
    ) as killer:
        killer.execute(f"KILL CONNECTION {identity}")


def _record_connections(
    monkeypatch: pytest.MonkeyPatch, *, use_pure: bool
) -> list[MySQLConnectionAbstract]:
    original = mysql.connector.connect
    opened: list[MySQLConnectionAbstract] = []

    def connect(**parameters: Any) -> MySQLConnectionAbstract:
        connection = cast(
            MySQLConnectionAbstract,
            original(**parameters, use_pure=use_pure),
        )
        opened.append(connection)
        return connection

    monkeypatch.setattr(mysql.connector, "connect", connect)
    return opened


@_MARIADB_CONTRACT
@pytest.mark.parametrize("use_pure", [False, True])
def test_server_killed_idle_connection_is_replaced_before_one_write(
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    use_pure: bool,
) -> None:
    opened = _record_connections(monkeypatch, use_pure=use_pure)
    context = RepositoryContext.from_config(mariadb_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE admission_rows (id INT PRIMARY KEY)")
            identity = connector.fetch_one("SELECT CONNECTION_ID()")[0]
        assert type(identity) is int
        assert len(opened) == 1
        _kill_idle(mariadb_config, identity)
        with context.SQLConnector() as connector:
            replacement = connector.fetch_one("SELECT CONNECTION_ID()")[0]
            with connector.transaction():
                connector.execute("INSERT INTO admission_rows VALUES (1)")
        assert replacement != identity
        assert len(opened) == 2
        with context.SQLConnector() as connector:
            assert connector.fetch_one("SELECT CONNECTION_ID()") == (replacement,)
            assert connector.fetch_all("SELECT id FROM admission_rows") == [(1,)]
        assert len(opened) == 2
    finally:
        context.close()


@_MARIADB_CONTRACT
@pytest.mark.mariadb_smoke
def test_server_killed_idle_heartbeat_session_preserves_clean_quick_restart(
    mariadb_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    lease = 300_000_000
    policy = DatabaseAuditPolicy(
        minimum_interval_microseconds=86_400_000_000, duration_multiplier=100
    )
    with closing(VNextDatabaseAdminFacade(mariadb_config)) as admin:
        admin.initialize()
        first = admin.start_ingest_runtime(
            policy=policy, lease_duration_microseconds=lease
        )
        assert first.reason is DatabaseAuditReason.FIRST_RUN
        admin.mark_initial_catchup_complete(first.session)

        with monkeypatch.context() as fault:
            opened = _record_connections(fault, use_pure=False)
            with VNextIngestFacade(mariadb_config) as ingest:
                session = ingest.try_claim_ingest(True, lease)
                assert session is not None
                assert len(opened) == 1
                identity = opened[0].connection_id
                assert type(identity) is int
                _kill_idle(mariadb_config, identity)

                writes: list[str] = []
                original: Callable[..., None] = VNextUnitOfWork.compare_and_swap

                def count_write(
                    work: VNextUnitOfWork,
                    query: str,
                    data: tuple[Any, ...] = (),
                    *,
                    authority: str,
                ) -> None:
                    writes.append(authority)
                    original(work, query, data, authority=authority)

                with monkeypatch.context() as counting:
                    counting.setattr(VNextUnitOfWork, "compare_and_swap", count_write)
                    renewed = ingest.renew_ingest(session, lease)
                assert len(opened) == 2
                assert writes == [
                    "maintenance gate owner lease",
                    "ingest generation owner lease",
                ]
                assert renewed.ingest_generation == session.ingest_generation
                ingest.complete_ingest(renewed)

        admin.finish_ingest_runtime(first.session)

    with closing(VNextDatabaseAdminFacade(mariadb_config)) as restarted:
        following = restarted.start_ingest_runtime(
            policy=policy, lease_duration_microseconds=lease
        )
        assert following.reason is DatabaseAuditReason.RECENT_AUDIT
        assert following.full_audit is None
        assert following.last_full_audit_at == first.last_full_audit_at
        restarted.finish_ingest_runtime(following.session)
