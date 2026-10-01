"""Runtime-owned bounded MariaDB leases; never retry a database command."""

from collections.abc import Callable
from os import getpid
from threading import Condition, get_ident
from time import monotonic
from weakref import finalize

from mysql.connector import errorcode
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.errors import InterfaceError, OperationalError

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

    def acquire(self) -> tuple[MySQLConnectionAbstract, MySQLCursorAbstract]:
        """Admit a buffered cursor before exposing any session to caller SQL.

        Cursor construction already performs the driver's non-reconnecting
        health check. Only an unavailable idle transport can be replaced, once;
        no SQL statement or transaction is replayed here.
        """

        self._require_process()
        deadline = monotonic() + self._wait_seconds
        connection: MySQLConnectionAbstract | None
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("MariaDB pool is closed")
                if self._idle:
                    connection = self._idle.pop()
                    self._leased[id(connection)] = get_ident()
                    break
                if self._total < self._capacity:
                    self._total += 1
                    connection = None
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
        reused = connection is not None
        if connection is None:
            connection = self._open_reserved()
        return self._admit_cursor(connection, reused=reused)

    def _open_reserved(self) -> MySQLConnectionAbstract:
        """Open under an existing capacity reservation, outside the pool lock."""

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

    def _admit_cursor(
        self, connection: MySQLConnectionAbstract, *, reused: bool
    ) -> tuple[MySQLConnectionAbstract, MySQLCursorAbstract]:
        try:
            cursor = connection.cursor(buffered=True)
        except BaseException as error:
            if reused and _unavailable_transport(error):
                replacement = self._replace_reserved(connection)
                if replacement is not None:
                    return self._admit_cursor(replacement, reused=False)
            else:
                self.release(connection, reusable=False)
            raise
        with self._condition:
            if not self._closed:
                return connection, cursor
        try:
            cursor.close()
        finally:
            self.release(connection, reusable=False)
        raise RuntimeError("MariaDB pool closed while admitting a connection")

    def _replace_reserved(
        self, connection: MySQLConnectionAbstract
    ) -> MySQLConnectionAbstract | None:
        # Keep the original slot reserved across both the close and open. A
        # waiting caller must not acquire it between those physical operations.
        with self._condition:
            del self._leased[id(connection)]
        closed = False
        try:
            closed = _close_connection(connection)
        finally:
            with self._condition:
                if not closed:
                    self._quarantined.append(connection)
                    self._condition.notify_all()
        if not closed:
            return None
        with self._condition:
            if self._closed:
                self._total -= 1
                self._condition.notify_all()
                raise RuntimeError("MariaDB pool closed while replacing a connection")
        return self._open_reserved()

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


def _unavailable_transport(error: BaseException) -> bool:
    if not isinstance(error, (InterfaceError, OperationalError)):
        return False
    if error.errno in {
        errorcode.CR_SERVER_GONE_ERROR,
        errorcode.CR_SERVER_LOST,
        errorcode.CR_SERVER_LOST_EXTENDED,
    }:
        return True
    # Connector/Python's C extension and pure-Python cursor factories both
    # report a failed is_connected() without an errno (with/without a period).
    # Restrict this exception to cursor admission, never a user SQL operation.
    return error.errno == -1 and error.msg in {
        "MySQL Connection not available.",
        "MySQL Connection not available",
    }
