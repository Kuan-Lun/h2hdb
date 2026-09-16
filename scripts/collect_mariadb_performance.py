"""Manually collect bounded, read-only MariaDB diagnostic snapshots.

Copy this standalone file into an existing H2HDB Python environment. It does
not initialize/check the catalog, enable instrumentation, reset counters, or
execute supplied SQL. The only SET changes this collector's own session.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from mysql.connector import connect
from mysql.connector.abstracts import MySQLConnectionAbstract

from h2hdb.config_loader import CoreConfig
from h2hdb.environment import resolve_environment_placeholders

MAX_DIGESTS = 10000
COUNTERS = (
    "COUNT_STAR",
    "SUM_TIMER_WAIT",
    "SUM_LOCK_TIME",
    "SUM_ROWS_EXAMINED",
    "SUM_ROWS_SENT",
    "SUM_CREATED_TMP_DISK_TABLES",
    "SUM_SELECT_SCAN",
    "SUM_ERRORS",
)
STATUS_NAMES = (
    "Uptime",
    "Questions",
    "Com_select",
    "Com_insert",
    "Com_update",
    "Com_delete",
    "Com_commit",
    "Com_rollback",
    "Bytes_received",
    "Bytes_sent",
    "Handler_read_key",
    "Handler_read_next",
    "Handler_read_rnd_next",
    "Innodb_buffer_pool_reads",
    "Innodb_buffer_pool_read_requests",
    "Innodb_buffer_pool_wait_free",
    "Innodb_data_reads",
    "Innodb_data_writes",
    "Innodb_data_read",
    "Innodb_data_written",
    "Innodb_row_lock_waits",
    "Innodb_row_lock_time",
)
STATUS_SQL = (
    "SHOW GLOBAL STATUS WHERE Variable_name IN ("
    + ",".join("'" + name + "'" for name in STATUS_NAMES)
    + ")"
)
DIGEST_SQL = (
    "SELECT DIGEST, LEFT(DIGEST_TEXT, 2048), "
    + ", ".join(COUNTERS)
    + " FROM performance_schema.events_statements_summary_by_digest "
    "WHERE SCHEMA_NAME = %s AND DIGEST IS NOT NULL ORDER BY DIGEST LIMIT 10001"
)


def query(
    connection: MySQLConnectionAbstract,
    statement: str,
    parameters: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]:
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cast(list[tuple[Any, ...]], cursor.fetchall())


def load_database(path: Path, kind: str) -> CoreConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if kind == "ingest":
        raw = raw["core"]
    config = CoreConfig.model_validate(resolve_environment_placeholders(raw))
    if config.database.sql_type != "mariadb":
        raise ValueError("collector requires MariaDB")
    return config


def open_connection(config: CoreConfig) -> MySQLConnectionAbstract:
    db = config.database
    # No default schema: this collector's statements do not join the target
    # schema's digest aggregates. Global status still includes collector traffic.
    return cast(
        MySQLConnectionAbstract,
        connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            autocommit=True,
            connection_timeout=5,
            read_timeout=10,
            write_timeout=10,
        ),
    )


def snapshot(connection: MySQLConnectionAbstract, schema: str) -> dict[str, Any]:
    started = time.perf_counter()
    result: dict[str, Any] = {
        "utc": datetime.now(UTC).isoformat(),
        "global_status": {str(k): int(v) for k, v in query(connection, STATUS_SQL)},
    }
    enabled = query(connection, "SELECT @@GLOBAL.performance_schema")
    result["performance_schema_enabled"] = bool(int(enabled[0][0]))
    if not result["performance_schema_enabled"]:
        result["digest_status"] = "disabled"
    else:
        try:
            result["consumers"] = {
                str(k): str(v)
                for k, v in query(
                    connection,
                    "SELECT NAME, ENABLED FROM performance_schema.setup_consumers "
                    "WHERE NAME IN ('global_instrumentation', 'thread_instrumentation', "
                    "'statements_digest', 'events_statements_current')",
                )
            }
            result["statement_instruments"] = [
                {"enabled": str(a), "timed": str(b), "count": int(c)}
                for a, b, c in query(
                    connection,
                    "SELECT ENABLED, TIMED, COUNT(*) "
                    "FROM performance_schema.setup_instruments "
                    "WHERE NAME LIKE 'statement/%' GROUP BY ENABLED, TIMED",
                )
            ]
            rows = query(connection, DIGEST_SQL, (schema,))
            result["digest_status"] = (
                "truncated" if len(rows) > MAX_DIGESTS else "available"
            )
            result["digests"] = {
                str(row[0]): {
                    "normalized_sql_prefix": str(row[1]),
                    **{
                        name: None if v is None else int(v)
                        for name, v in zip(COUNTERS, row[2:], strict=True)
                    },
                }
                for row in rows[:MAX_DIGESTS]
            }
            overflow = query(
                connection,
                "SELECT COALESCE(SUM(COUNT_STAR), 0) "
                "FROM performance_schema.events_statements_summary_by_digest "
                "WHERE DIGEST IS NULL",
            )
            result["global_digest_overflow_events"] = int(overflow[0][0])
        except Exception as error:
            result["digest_status"] = "unavailable"
            result["digest_error_type"] = type(error).__name__
            # Exception strings may contain connection/configuration information.
    result["snapshot_seconds"] = time.perf_counter() - started
    return result


def differences(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    for label, snapshot_result in (("before", before), ("after", after)):
        missing = set(STATUS_NAMES) - snapshot_result["global_status"].keys()
        if missing:
            problems.append(
                f"required global counters unavailable ({label}): "
                + ", ".join(sorted(missing))
            )
    if (
        "Uptime" not in before["global_status"]
        or "Uptime" not in after["global_status"]
    ):
        problems.append("server uptime unavailable; restart detection unavailable")
    status: dict[str, int] = {}
    for name, end in after["global_status"].items():
        start = before["global_status"].get(name)
        if start is None:
            problems.append(f"global counter appeared: {name}")
        elif end < start:
            problems.append(f"global counter decreased: {name}")
        else:
            status[name] = end - start
    if before["global_status"].keys() != after["global_status"].keys():
        problems.append("global counter set changed")
    digest_rows: list[dict[str, Any]] = []
    digest_available = before["digest_status"] == after["digest_status"] == "available"
    if not digest_available:
        problems.append("complete digest snapshots unavailable")
    else:
        previous = before["digests"]
        current = after["digests"]
        if previous.keys() - current.keys():
            problems.append("digest disappeared; reset or eviction possible")
        if before.get("consumers") != after.get("consumers") or before.get(
            "statement_instruments"
        ) != after.get("statement_instruments"):
            problems.append("instrumentation configuration changed")
        required = {
            "global_instrumentation",
            "thread_instrumentation",
            "statements_digest",
        }
        consumers = after.get("consumers", {})
        if any(consumers.get(name) != "YES" for name in required):
            problems.append("required statement consumers are disabled or missing")
        instruments = after.get("statement_instruments", [])
        if not instruments or any(
            row["enabled"] != "YES" or row["timed"] != "YES" for row in instruments
        ):
            problems.append("statement instrumentation is incomplete or untimed")
        for digest, row in current.items():
            old = previous.get(digest, dict.fromkeys(COUNTERS, 0))
            values: dict[str, Any] = {
                "digest": digest,
                "normalized_sql_prefix": row["normalized_sql_prefix"],
            }
            valid = True
            for name in COUNTERS:
                start, end = old[name], row[name]
                if start is None or end is None or end < start:
                    problems.append(
                        f"digest counter missing/decreased: {digest}:{name}"
                    )
                    valid = False
                    break
                values[name] = end - start
            if valid and values["COUNT_STAR"]:
                values["server_elapsed_seconds"] = values["SUM_TIMER_WAIT"] / 1e12
                values["server_table_lock_seconds"] = values["SUM_LOCK_TIME"] / 1e12
                digest_rows.append(values)
        overflow = after.get("global_digest_overflow_events", 0) - before.get(
            "global_digest_overflow_events", 0
        )
        if overflow:
            problems.append(
                "global digest overflow changed; attribution may be incomplete"
            )
    return {
        "global_status_delta": status,
        "digest_deltas": sorted(
            digest_rows, key=lambda row: row["SUM_TIMER_WAIT"], reverse=True
        ),
        "limitations_detected": sorted(set(problems)),
        "complete_counter_comparison": not problems,
    }


def collect(config: CoreConfig, seconds: float) -> dict[str, Any]:
    if not 1 <= seconds <= 600:
        raise ValueError("duration must be between 1 and 600 seconds")
    with closing(open_connection(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        version = str(query(connection, "SELECT VERSION()")[0][0])
        if "MariaDB" not in version:
            raise ValueError("collector requires a MariaDB server")
        before = snapshot(connection, config.database.database)
        started = time.perf_counter()
        time.sleep(seconds)
        interval = time.perf_counter() - started
        after = snapshot(connection, config.database.database)
        roundtrips = []
        for _ in range(20):
            started = time.perf_counter()
            if query(connection, "SELECT 1") != [(1,)]:
                raise RuntimeError("latency sample returned an unexpected result")
            roundtrips.append(time.perf_counter() - started)
    return {
        "status": "collected",
        "server_version": version,
        "interval_seconds": interval,
        "before": before,
        "after": after,
        "comparison": differences(before, after),
        "select_one_client_seconds": {
            "samples": roundtrips,
            "median": statistics.median(roundtrips),
        },
        "notes": [
            "Read-only metadata/statistics queries only; no catalog scans, ANALYZE, instrumentation changes, resets or locks requested.",
            "Digest deltas include every account using the configured schema, not only ingest. Match the window with client DEBUG operation logs.",
            "Server times use picoseconds/1e12; concurrent statement durations may overlap and are not wall-clock percentages.",
            "SUM_LOCK_TIME is statement table-lock timing, not a complete InnoDB row-lock attribution. Global InnoDB counters cover all server users.",
            "SELECT1 samples run after the snapshots; they include client/driver/server/transport overhead, not pure network latency.",
            "Global counters include this collector's own small traffic. Snapshots are sequential and not a transactional cut.",
            "Normalized SQL prefixes omit literal values and are capped at2048 characters. Their MariaDB digest is not the application's raw-template SHA fingerprint.",
            "Disabled, missing or incomplete instrumentation cannot prove zero query cost. Hidden reset-and-regrowth can escape counter-decrease detection.",
        ],
        "references": [
            "https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/system-tables/performance-schema/performance-schema-tables/performance-schema-events_statements_summary_by_digest-table",
            "https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/system-tables/performance-schema/performance-schema-overview",
        ],
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-kind", choices=("core", "ingest"), required=True)
    parser.add_argument("--seconds", type=int, choices=range(1, 601), default=60)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        parser.error("output must be a new path")
    if not args.output.parent.is_dir():
        parser.error("output parent must already exist")
    try:
        report = collect(load_database(args.config, args.config_kind), args.seconds)
    except Exception as error:
        report = {
            "status": "failed",
            "error_type": type(error).__name__,
            "message": "No exception/configuration text emitted; verify connectivity, permissions and config locally.",
        }
    write_report(args.output, report)
    print(json.dumps({"status": report["status"], "output": str(args.output)}))
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
