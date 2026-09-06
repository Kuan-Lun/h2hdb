"""Runtime-owned bounded MariaDB leases; never retry a database command."""

from collections.abc import Callable
from os import getpid
from threading import Condition, get_ident
from time import monotonic
from weakref import finalize

from mysql.connector.abstracts import MySQLConnectionAbstract

# Bounds apply to one RepositoryContext, including connections being opened or
# closed. They do not depend on gallery/image counts or queue length.
MARIADB_CONTEXT_CONNECTION_LIMIT = 8
MARIADB_CONTEXT_WAITER_LIMIT = 8
MARIADB_POOL_WAIT_SECONDS = 30.0


def _close_idle(connections: list[MySQLConnectionAbstract], owner_pid: int) -> None:
    # COM_QUIT on a fork-inherited socket would terminate the parent's session.
    if getpid() != owner_pid:
        return
    while connections:
        _close_connection(connections.pop())


def _close_connection(connection: MySQLConnectionAbstract) -> bool:
    try:
        connection.close()
    except Exception:
        # A close failure cannot prove the physical transport is gone. Keep its
        # admission slot reserved, and never mask the original SQL exception.
        return False
    return True


class MariaDBConnectionPool:
    """Own physical sessions until context close or final reference release.

    Admission counts pending opens and closes, and limits waiting callers too.
    Reset is the connector's responsibility before releasing a reusable lease.

    Create the runtime after its process starts (spawn is supported). An
    inherited runtime rejects use after fork. The PID guard only prevents our
    cleanup callback from issuing COM_QUIT; it does not make garbage collection
    of fork-inherited driver C objects safe. Do not inherit a live DB runtime.
    """

    def __init__(
        self,
        opener: Callable[[], MySQLConnectionAbstract],
        *,
        capacity: int = MARIADB_CONTEXT_CONNECTION_LIMIT,
        waiter_limit: int = MARIADB_CONTEXT_WAITER_LIMIT,
        wait_seconds: float = MARIADB_POOL_WAIT_SECONDS,
    ) -> None:
        if capacity < 1 or waiter_limit < 0 or wait_seconds < 0:
            raise ValueError("Invalid MariaDB pool bounds")
        self._opener = opener
        self._capacity = capacity
        self._waiter_limit = waiter_limit
        self._wait_seconds = wait_seconds
        self._pid = getpid()
        self._condition = Condition()
        self._idle: list[MySQLConnectionAbstract] = []
        self._quarantined: list[MySQLConnectionAbstract] = []
        self._leased: dict[int, int] = {}
        self._total = 0
        self._waiters = 0
        self._closed = False
        self._finalizer = finalize(self, _close_idle, self._idle, self._pid)
        self._quarantine_finalizer = finalize(
            self, _close_idle, self._quarantined, self._pid
        )

    def _require_process(self) -> None:
        if getpid() != self._pid:
            raise RuntimeError(
                "MariaDB pool cannot be used after fork; create a runtime"
            )

    def acquire(self) -> MySQLConnectionAbstract:
        self._require_process()
        deadline = monotonic() + self._wait_seconds
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("MariaDB pool is closed")
                if self._idle:
                    connection = self._idle.pop()
                    self._leased[id(connection)] = get_ident()
                    return connection
                if self._total < self._capacity:
                    self._total += 1
                    break
                remaining = deadline - monotonic()
                if (
                    remaining <= 0
                    or self._waiters >= self._waiter_limit
                    or get_ident() in self._leased.values()
                ):
                    raise TimeoutError("MariaDB context connection capacity exhausted")
                self._waiters += 1
                try:
                    self._condition.wait(remaining)
                finally:
                    self._waiters -= 1
        try:
            connection = self._opener()
        except BaseException:
            with self._condition:
                self._total -= 1
                self._condition.notify_all()
            raise
        with self._condition:
            if not self._closed:
                self._leased[id(connection)] = get_ident()
                return connection
        self._discard(connection)
        raise RuntimeError("MariaDB pool closed while opening a connection")

    def release(self, connection: MySQLConnectionAbstract, *, reusable: bool) -> None:
        self._require_process()
        with self._condition:
            if id(connection) not in self._leased:
                raise RuntimeError("MariaDB connection is not leased from this pool")
            del self._leased[id(connection)]
            if reusable and not self._closed:
                self._idle.append(connection)
                self._condition.notify_all()
                return
        self._discard(connection)

    def _discard(self, connection: MySQLConnectionAbstract) -> None:
        closed = False
        try:
            closed = _close_connection(connection)
        finally:
            with self._condition:
                if closed:
                    self._total -= 1
                else:
                    self._quarantined.append(connection)
                self._condition.notify_all()

    def close(self) -> None:
        self._require_process()
        with self._condition:
            self._closed = True
            idle = self._idle + self._quarantined
            self._idle.clear()
            self._quarantined.clear()
            self._condition.notify_all()
        # Outstanding leases finish normally, then release discards them.
        for connection in idle:
            self._discard(connection)
