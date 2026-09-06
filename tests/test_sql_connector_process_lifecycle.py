"""Real child termination preserves transaction atomicity across fresh runtimes."""

import multiprocessing
import os
import signal
from multiprocessing.connection import Connection

import pytest

from h2hdb import CoreConfig
from h2hdb.repository import RepositoryContext


def _write_until_checkpoint(
    config: CoreConfig, checkpoint: Connection, committed: bool
) -> None:
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            connector.begin()
            connector.execute("INSERT INTO lifecycle_rows VALUES (1)")
            connector.execute("INSERT INTO lifecycle_rows VALUES (2)")
            if committed:
                connector.commit()
            checkpoint.send("committed" if committed else "uncommitted")
            checkpoint.recv()  # The test terminates this process at the checkpoint.
    finally:
        checkpoint.close()
        context.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGTERM/SIGKILL evidence")
@pytest.mark.parametrize("signal_name", ("SIGTERM", "SIGKILL"))
@pytest.mark.parametrize(
    "committed", (False, True), ids=("before-commit", "after-commit")
)
def test_process_signal_restart_preserves_complete_transaction(
    db_config: CoreConfig, signal_name: str, committed: bool
) -> None:
    context = RepositoryContext.from_config(db_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE lifecycle_rows (id INT PRIMARY KEY)")
    finally:
        context.close()
    processes = multiprocessing.get_context("spawn")
    parent, child = processes.Pipe()
    process = processes.Process(
        target=_write_until_checkpoint, args=(db_config, child, committed)
    )
    process.start()
    child.close()
    try:
        assert parent.poll(15), "writer did not reach its transaction checkpoint"
        assert parent.recv() == ("committed" if committed else "uncommitted")
        signal_number = getattr(signal, signal_name)
        assert process.pid is not None
        os.kill(process.pid, signal_number)
        process.join(10)
        assert process.exitcode == -signal_number
        restarted = RepositoryContext.from_config(db_config)
        try:
            with restarted.SQLConnector() as connector:
                expected = [(1,), (2,)] if committed else []
                assert (
                    connector.fetch_all("SELECT id FROM lifecycle_rows ORDER BY id")
                    == expected
                )
                connector.rollback()  # End any implicit MariaDB SELECT snapshot.
                with connector.transaction():
                    # Recovery derives its outcome from fresh durable authority.
                    rows = connector.fetch_all(
                        "SELECT id FROM lifecycle_rows ORDER BY id"
                    )
                    if rows == []:
                        connector.execute("INSERT INTO lifecycle_rows VALUES (1)")
                        connector.execute("INSERT INTO lifecycle_rows VALUES (2)")
                    else:
                        assert rows == [(1,), (2,)]
                assert connector.fetch_all(
                    "SELECT id FROM lifecycle_rows ORDER BY id"
                ) == [(1,), (2,)]
        finally:
            restarted.close()
    finally:
        parent.close()
        if process.is_alive():
            process.kill()
            process.join(10)
        assert not process.is_alive()
        process.close()
