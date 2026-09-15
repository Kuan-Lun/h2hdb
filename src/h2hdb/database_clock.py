"""Database-owned UTC timestamps shared by bounded administrative workflows."""

from __future__ import annotations

from .vnext_domains import require_int63
from .vnext_transaction import VNextUnitOfWork


def database_unix_microseconds(work: VNextUnitOfWork) -> int:
    """Read one database-owned timestamp inside the current transaction."""

    if work.backend == "sqlite":
        row = work.connector.fetch_one(
            "SELECT CAST(unixepoch('now') AS INTEGER) * 1000000 + "
            "CAST(substr(strftime('%f', 'now'), 4, 3) AS INTEGER) * 1000"
        )
    else:
        row = work.connector.fetch_one(
            "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6))"
        )
    if len(row) != 1:
        raise ValueError("database clock returned no exact scalar")
    return require_int63(row[0], field="database unix microseconds")
