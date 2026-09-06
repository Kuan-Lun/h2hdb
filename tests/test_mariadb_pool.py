"""Admission/race evidence for the runtime-owned physical connection bound."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import cast

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract

import h2hdb.mariadb_pool as pool_module
from h2hdb.mariadb_pool import MariaDBConnectionPool


class _Connection:
    def __init__(self) -> None:
        self.closed = False
        self.close_started: Event | None = None
        self.close_continue: Event | None = None

    def close(self) -> None:
        if self.close_started is not None:
            self.close_started.set()
        if self.close_continue is not None:
            assert self.close_continue.wait(5)
        self.closed = True


def _physical(connection: _Connection) -> MySQLConnectionAbstract:
    return cast(MySQLConnectionAbstract, connection)


def test_pool_reuses_only_explicitly_returned_sessions() -> None:
    opened: list[_Connection] = []

    def open_connection() -> MySQLConnectionAbstract:
        connection = _Connection()
        opened.append(connection)
        return _physical(connection)

    pool = MariaDBConnectionPool(open_connection, capacity=2)
    first = pool.acquire()
    second = pool.acquire()
    with pytest.raises(TimeoutError, match="capacity exhausted"):
        pool.acquire()  # Nested borrowing cannot wait on its own leases.
    pool.release(first, reusable=True)
    assert pool.acquire() is first
    pool.release(first, reusable=False)
    assert opened[0].closed
    replacement = pool.acquire()
    assert replacement is not first
    assert not opened[1].closed
    pool.close()
    assert not opened[1].closed  # Closing a runtime preserves its active borrower.
    pool.release(second, reusable=True)
    pool.release(replacement, reusable=True)
    assert all(connection.closed for connection in opened)
    with pytest.raises(RuntimeError, match="not leased"):
        pool.release(second, reusable=True)


def test_pending_open_consumes_capacity_and_failed_open_releases_it() -> None:
    opening, finish = Event(), Event()
    fail = True

    def open_connection() -> MySQLConnectionAbstract:
        opening.set()
        assert finish.wait(5)
        if fail:
            raise ConnectionError("injected connect failure")
        return _physical(_Connection())

    pool = MariaDBConnectionPool(open_connection, capacity=1, wait_seconds=0)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert opening.wait(5)
        try:
            with pytest.raises(TimeoutError):
                pool.acquire()
        finally:
            finish.set()
        with pytest.raises(ConnectionError, match="connect failure"):
            future.result(timeout=5)
    fail = False
    connection = pool.acquire()
    pool.release(connection, reusable=False)
    pool.close()


def test_closing_transport_still_counts_against_capacity() -> None:
    connection = _Connection()
    connection.close_started = Event()
    connection.close_continue = Event()
    pool = MariaDBConnectionPool(
        lambda: _physical(connection), capacity=1, wait_seconds=0
    )
    leased = pool.acquire()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.release, leased, reusable=False)
        assert connection.close_started.wait(5)
        try:
            with pytest.raises(TimeoutError):
                pool.acquire()
        finally:
            connection.close_continue.set()
        future.result(timeout=5)
    assert connection.closed
    pool.close()


def test_close_racing_open_discards_new_connection() -> None:
    opening, finish = Event(), Event()
    connection = _Connection()

    def open_connection() -> MySQLConnectionAbstract:
        opening.set()
        assert finish.wait(5)
        return _physical(connection)

    pool = MariaDBConnectionPool(open_connection)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert opening.wait(5)
        pool.close()
        finish.set()
        with pytest.raises(RuntimeError, match="closed while opening"):
            future.result(timeout=5)
    assert connection.closed


def test_waiter_admission_is_bounded_and_close_wakes_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    pool = MariaDBConnectionPool(
        lambda: _physical(connection), capacity=1, waiter_limit=1, wait_seconds=5
    )
    leased = pool.acquire()
    waiting = Event()
    condition_wait = pool._condition.wait

    def record_wait(timeout: float | None = None) -> bool:
        waiting.set()
        return condition_wait(timeout)

    monkeypatch.setattr(pool._condition, "wait", record_wait)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(pool.acquire)
        assert waiting.wait(5)
        second = executor.submit(pool.acquire)
        try:
            with pytest.raises(TimeoutError):
                second.result(timeout=2)
        finally:
            pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            first.result(timeout=5)
    pool.release(leased, reusable=True)
    assert connection.closed


def test_waiter_receives_only_returned_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _Connection()
    pool = MariaDBConnectionPool(lambda: _physical(connection), capacity=1)
    leased = pool.acquire()
    waiting = Event()
    condition_wait = pool._condition.wait

    def record_wait(timeout: float | None = None) -> bool:
        waiting.set()
        return condition_wait(timeout)

    monkeypatch.setattr(pool._condition, "wait", record_wait)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert waiting.wait(5)
        pool.release(leased, reusable=True)
        assert future.result(timeout=5) is leased
    pool.release(leased, reusable=True)
    pool.close()


def test_fork_inherited_pool_rejects_use_and_finalizer_never_quits_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    pool = MariaDBConnectionPool(lambda: _physical(connection))
    leased = pool.acquire()
    pool.release(leased, reusable=True)
    with monkeypatch.context() as child:
        child.setattr(pool_module, "getpid", lambda: pool._pid + 1)
        for operation in (pool.acquire, pool.close):
            with pytest.raises(RuntimeError, match="after fork"):
                operation()
        pool_module._close_idle(pool._idle, pool._pid)
        assert not connection.closed
    pool.close()
    assert connection.closed


def test_failed_transport_close_keeps_its_capacity_reserved() -> None:
    class BrokenClose(_Connection):
        def close(self) -> None:
            raise ConnectionError("cannot prove transport closed")

    connection = BrokenClose()
    pool = MariaDBConnectionPool(
        lambda: _physical(connection), capacity=1, wait_seconds=0
    )
    leased = pool.acquire()
    pool.release(leased, reusable=False)
    with pytest.raises(TimeoutError):
        pool.acquire()
    assert pool._total == 1
    assert pool._quarantined == [_physical(connection)]
    pool.close()
