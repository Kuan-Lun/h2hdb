"""Admission/race evidence for the runtime-owned physical connection bound."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import cast

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.errors import InterfaceError, OperationalError, ProgrammingError

import h2hdb.mariadb_pool as pool_module
from h2hdb.mariadb_pool import MariaDBConnectionPool


class _Cursor:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self) -> None:
        self.closed = False
        self.close_started: Event | None = None
        self.close_continue: Event | None = None
        self.cursor_started: Event | None = None
        self.cursor_continue: Event | None = None
        self.cursor_error: BaseException | None = None
        self.cursor_calls = 0
        self.cursors: list[_Cursor] = []

    def cursor(self, *, buffered: bool = False) -> MySQLCursorAbstract:
        assert buffered
        self.cursor_calls += 1
        if self.cursor_started is not None:
            self.cursor_started.set()
        if self.cursor_continue is not None:
            assert self.cursor_continue.wait(5)
        if self.cursor_error is not None:
            raise self.cursor_error
        result = _Cursor()
        self.cursors.append(result)
        return cast(MySQLCursorAbstract, result)

    def close(self) -> None:
        if self.close_started is not None:
            self.close_started.set()
        if self.close_continue is not None:
            assert self.close_continue.wait(5)
        self.closed = True


def _physical(connection: _Connection) -> MySQLConnectionAbstract:
    return cast(MySQLConnectionAbstract, connection)


def _lease(pool: MariaDBConnectionPool) -> MySQLConnectionAbstract:
    connection, cursor = pool.acquire()
    cursor.close()
    return connection


def test_pool_reuses_only_explicitly_returned_sessions() -> None:
    opened: list[_Connection] = []

    def open_connection() -> MySQLConnectionAbstract:
        connection = _Connection()
        opened.append(connection)
        return _physical(connection)

    pool = MariaDBConnectionPool(open_connection, capacity=2)
    first = _lease(pool)
    second = _lease(pool)
    with pytest.raises(TimeoutError, match="capacity exhausted"):
        _lease(pool)  # Nested borrowing cannot wait on its own leases.
    pool.release(first, reusable=True)
    assert _lease(pool) is first
    pool.release(first, reusable=False)
    assert opened[0].closed
    replacement = _lease(pool)
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
        future = executor.submit(_lease, pool)
        assert opening.wait(5)
        try:
            with pytest.raises(TimeoutError):
                _lease(pool)
        finally:
            finish.set()
        with pytest.raises(ConnectionError, match="connect failure"):
            future.result(timeout=5)
    fail = False
    connection = _lease(pool)
    pool.release(connection, reusable=False)
    pool.close()


def test_closing_transport_still_counts_against_capacity() -> None:
    connection = _Connection()
    connection.close_started = Event()
    connection.close_continue = Event()
    pool = MariaDBConnectionPool(
        lambda: _physical(connection), capacity=1, wait_seconds=0
    )
    leased = _lease(pool)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.release, leased, reusable=False)
        assert connection.close_started.wait(5)
        try:
            with pytest.raises(TimeoutError):
                _lease(pool)
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
        future = executor.submit(_lease, pool)
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
    leased = _lease(pool)
    waiting = Event()
    condition_wait = pool._condition.wait

    def record_wait(timeout: float | None = None) -> bool:
        waiting.set()
        return condition_wait(timeout)

    monkeypatch.setattr(pool._condition, "wait", record_wait)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(_lease, pool)
        assert waiting.wait(5)
        second = executor.submit(_lease, pool)
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
    leased = _lease(pool)
    waiting = Event()
    condition_wait = pool._condition.wait

    def record_wait(timeout: float | None = None) -> bool:
        waiting.set()
        return condition_wait(timeout)

    monkeypatch.setattr(pool._condition, "wait", record_wait)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_lease, pool)
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
    leased = _lease(pool)
    pool.release(leased, reusable=True)
    with monkeypatch.context() as child:
        child.setattr(pool_module, "getpid", lambda: pool._pid + 1)
        for operation in (lambda: _lease(pool), pool.close):
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
    leased = _lease(pool)
    pool.release(leased, reusable=False)
    with pytest.raises(TimeoutError):
        _lease(pool)
    assert pool._total == 1
    assert pool._quarantined == [_physical(connection)]
    pool.close()


@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("MySQL Connection not available."),
        OperationalError("MySQL Connection not available"),
        OperationalError("server gone", errno=2006),
        OperationalError("server lost", errno=2013),
        InterfaceError("server lost", errno=2055),
    ],
)
def test_unavailable_idle_cursor_is_replaced_once_without_extra_health_probe(
    failure: BaseException,
) -> None:
    old, fresh = _Connection(), _Connection()
    unopened = iter((old, fresh))
    pool = MariaDBConnectionPool(lambda: _physical(next(unopened)), capacity=1)
    leased = _lease(pool)
    pool.release(leased, reusable=True)
    old.cursor_error = failure

    connection, cursor = pool.acquire()

    assert connection is _physical(fresh)
    assert old.closed and not fresh.closed
    assert old.cursor_calls == 2 and fresh.cursor_calls == 1
    assert pool._total == 1 and pool._quarantined == []
    cursor.close()
    pool.release(connection, reusable=True)
    # Healthy reuse creates just its required cursor: no second is_connected()
    # or ping() API exists on this fake transport.
    assert _lease(pool) is connection
    assert fresh.cursor_calls == 2
    pool.release(connection, reusable=True)
    pool.close()


@pytest.mark.parametrize("reused", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        ProgrammingError("cursor programming error"),
        OperationalError("unknown admission error"),
        OperationalError("access denied", errno=1045),
        InterfaceError("invalid protocol state"),
        ValueError("invalid cursor configuration"),
        KeyboardInterrupt(),
    ],
)
def test_nontransport_cursor_error_never_opens_a_replacement(
    reused: bool, failure: BaseException
) -> None:
    connection = _Connection()
    opens = 0

    def opener() -> MySQLConnectionAbstract:
        nonlocal opens
        opens += 1
        return _physical(connection)

    pool = MariaDBConnectionPool(opener, capacity=1)
    if reused:
        pool.release(_lease(pool), reusable=True)
    connection.cursor_error = failure
    with pytest.raises(type(failure)) as caught:
        pool.acquire()
    assert caught.value is failure
    assert opens == 1 and connection.closed
    assert pool._total == 0 and pool._leased == {}
    pool.close()


@pytest.mark.parametrize("reused", [False, True])
def test_unavailable_fresh_cursor_does_not_retry(reused: bool) -> None:
    old, fresh = _Connection(), _Connection()
    failure = OperationalError("MySQL Connection not available.")
    fresh.cursor_error = failure
    unopened = iter((old, fresh) if reused else (fresh,))
    opens = 0

    def opener() -> MySQLConnectionAbstract:
        nonlocal opens
        opens += 1
        return _physical(next(unopened))

    pool = MariaDBConnectionPool(opener, capacity=1)
    if reused:
        pool.release(_lease(pool), reusable=True)
        old.cursor_error = failure
    with pytest.raises(OperationalError) as caught:
        pool.acquire()
    assert caught.value is failure
    assert fresh.closed and (not reused or old.closed)
    assert opens == 1 + int(reused)
    assert pool._total == 0 and pool._leased == {}
    pool.close()


def test_replacement_opens_fresh_instead_of_trying_other_idle_sessions() -> None:
    other, stale, fresh = _Connection(), _Connection(), _Connection()
    unopened = iter((other, stale, fresh))
    pool = MariaDBConnectionPool(lambda: _physical(next(unopened)), capacity=2)
    first, second = _lease(pool), _lease(pool)
    pool.release(first, reusable=True)
    pool.release(second, reusable=True)
    stale.cursor_error = OperationalError("MySQL Connection not available.")

    connection, cursor = pool.acquire()

    assert connection is _physical(fresh)
    assert other.cursor_calls == 1 and stale.closed
    assert pool._idle == [_physical(other)] and pool._total == 2
    cursor.close()
    pool.release(connection, reusable=False)
    pool.close()


def test_failed_replacement_open_returns_its_reservation_to_a_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old, final = _Connection(), _Connection()
    opening, finish, waiting = Event(), Event(), Event()
    opens = 0

    def opener() -> MySQLConnectionAbstract:
        nonlocal opens
        opens += 1
        if opens == 1:
            return _physical(old)
        if opens == 2:
            opening.set()
            assert finish.wait(5)
            raise OSError("replacement unavailable")
        return _physical(final)

    pool = MariaDBConnectionPool(opener, capacity=1, wait_seconds=5)
    pool.release(_lease(pool), reusable=True)
    old.cursor_error = OperationalError("MySQL Connection not available.")
    condition_wait = pool._condition.wait

    def wait(timeout: float | None = None) -> bool:
        waiting.set()
        return condition_wait(timeout)

    monkeypatch.setattr(pool._condition, "wait", wait)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(pool.acquire)
        assert opening.wait(5)
        second = executor.submit(pool.acquire)
        try:
            assert waiting.wait(5)
        finally:
            finish.set()
        with pytest.raises(OSError, match="replacement unavailable"):
            first.result(timeout=5)
        connection, cursor = second.result(timeout=5)
    assert connection is _physical(final) and opens == 3
    cursor.close()
    pool.release(connection, reusable=False)
    assert pool._total == 0 and pool._waiters == 0
    pool.close()


@pytest.mark.parametrize("interrupt", [False, True])
def test_failed_idle_close_prevents_replacement_and_quarantines_capacity(
    interrupt: bool,
) -> None:
    close_failure = KeyboardInterrupt() if interrupt else OSError("close failed")

    class BrokenClose(_Connection):
        broken = True

        def close(self) -> None:
            if self.broken:
                raise close_failure
            super().close()

    old = BrokenClose()
    unopened = iter((old,))
    pool = MariaDBConnectionPool(
        lambda: _physical(next(unopened)), capacity=1, wait_seconds=0
    )
    pool.release(_lease(pool), reusable=True)
    admission_failure = OperationalError("MySQL Connection not available.")
    old.cursor_error = admission_failure
    expected = KeyboardInterrupt if interrupt else OperationalError
    with pytest.raises(expected) as caught:
        pool.acquire()
    assert caught.value is (close_failure if interrupt else admission_failure)
    assert pool._quarantined == [_physical(old)]
    assert pool._total == 1 and pool._leased == {}
    with pytest.raises(TimeoutError, match="capacity exhausted"):
        pool.acquire()
    if interrupt:
        with pytest.raises(KeyboardInterrupt):
            pool.close()
    else:
        pool.close()
    old.broken = False
    pool.close()
    assert old.closed and pool._total == 0 and pool._quarantined == []


@pytest.mark.parametrize("reused", [False, True])
def test_pool_close_during_cursor_validation_never_delivers_lease(
    reused: bool,
) -> None:
    connection = _Connection()
    pool = MariaDBConnectionPool(lambda: _physical(connection), capacity=1)
    if reused:
        pool.release(_lease(pool), reusable=True)
    connection.cursor_started, connection.cursor_continue = Event(), Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert connection.cursor_started.wait(5)
        try:
            # close() must remain available while the driver is validating.
            pool.close()
        finally:
            connection.cursor_continue.set()
        with pytest.raises(RuntimeError, match="closed while admitting"):
            future.result(timeout=5)
    assert connection.closed and connection.cursors[-1].closed
    assert pool._total == 0 and pool._leased == {}


@pytest.mark.parametrize("close_pool", [False, True])
def test_replacement_close_keeps_slot_and_observes_pool_shutdown(
    close_pool: bool,
) -> None:
    old, fresh = _Connection(), _Connection()
    unopened = iter((old, fresh))
    opens = 0

    def opener() -> MySQLConnectionAbstract:
        nonlocal opens
        opens += 1
        return _physical(next(unopened))

    pool = MariaDBConnectionPool(opener, capacity=1, wait_seconds=0)
    pool.release(_lease(pool), reusable=True)
    old.cursor_error = OperationalError("MySQL Connection not available.")
    old.close_started, old.close_continue = Event(), Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert old.close_started.wait(5)
        try:
            with pytest.raises(TimeoutError, match="capacity exhausted"):
                pool.acquire()
            if close_pool:
                pool.close()
        finally:
            old.close_continue.set()
        if close_pool:
            with pytest.raises(RuntimeError, match="closed while replacing"):
                future.result(timeout=5)
        else:
            connection, cursor = future.result(timeout=5)
            assert connection is _physical(fresh)
            cursor.close()
            pool.release(connection, reusable=False)
    assert old.closed and opens == (1 if close_pool else 2)
    assert pool._total == 0 and pool._leased == {}
    pool.close()


@pytest.mark.parametrize("failure", [None, OSError("open failed"), KeyboardInterrupt()])
def test_replacement_open_holds_slot_and_cleans_up_after_pool_shutdown(
    failure: BaseException | None,
) -> None:
    old, fresh = _Connection(), _Connection()
    opening, finish = Event(), Event()
    opens = 0

    def opener() -> MySQLConnectionAbstract:
        nonlocal opens
        opens += 1
        if opens == 1:
            return _physical(old)
        opening.set()
        assert finish.wait(5)
        if failure is not None:
            raise failure
        return _physical(fresh)

    pool = MariaDBConnectionPool(opener, capacity=1, wait_seconds=0)
    pool.release(_lease(pool), reusable=True)
    old.cursor_error = OperationalError("MySQL Connection not available.")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pool.acquire)
        assert opening.wait(5)
        try:
            with pytest.raises(TimeoutError, match="capacity exhausted"):
                pool.acquire()
            pool.close()
        finally:
            finish.set()
        with pytest.raises(RuntimeError if failure is None else type(failure)):
            future.result(timeout=5)
    assert old.closed and (failure is not None or fresh.closed)
    assert fresh.cursor_calls == 0
    assert opens == 2 and pool._total == 0 and pool._leased == {}
