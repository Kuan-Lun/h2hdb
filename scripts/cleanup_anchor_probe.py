#!/usr/bin/env python3
"""Opt-in synthetic AR retirement cost probe; never accepts an existing database."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]

from test_cleanup_anchor_costs import (  # noqa: E402 - repository-local fixture setup
    _PHASE,
    _PLAN,
    _RETAINED,
    _TABLE,
    _file,
    _native_cost,
    _operation,
    _root,
    _seed,
)
from vnext_publication_cleanup_fixtures import (  # noqa: E402 - repository-local fixtures
    partial_publication_setup,
)
from vnext_test_database import (  # noqa: E402 - repository-local fixtures
    connector_backend,
    open_generated_database,
)

from h2hdb import CoreConfig  # noqa: E402 - repository-local source
from h2hdb import (  # noqa: E402 - repository-local source
    vnext_cleanup_repository as cleanup,
)
from h2hdb.config_loader import DatabaseConfig  # noqa: E402 - repository-local source
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - repository-local source
from h2hdb.sql_performance import (  # noqa: E402 - repository-local source
    instrument_connector,
    measure_sql,
)
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - local source


@dataclass
class _SQLCost:
    calls: int = 0
    seconds: float = 0.0

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        _query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.calls += 1
            self.seconds += elapsed


@contextmanager
def _database(mariadb: bool, directory: Path) -> Iterator[CoreConfig]:
    if not mariadb:
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(directory / "probe.sqlite")
            )
        )
        return
    from testcontainers.community.mysql import MySqlContainer

    with MySqlContainer(
        image="mariadb:10.11.11",
        username="probe",
        password="probe-only",
        root_password="probe-root-only",
        dbname="synthetic_ar_cost",
    ) as container:
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb",
                host=container.get_container_host_ip(),
                port=int(container.get_exposed_port(container.port)),
                user="probe",
                password="probe-only",
                database="synthetic_ar_cost",
            )
        )


def _populate(connector: SQLConnector, count: int) -> tuple[int, ...]:
    distribution = (count - 81, 17, 64)
    with partial_publication_setup(connector, backend=connector_backend(connector)):
        for index, size in enumerate(distribution):
            root = _root(index)
            connector.execute(f"DELETE FROM {_TABLE} WHERE analysis_id = %s", (root,))
            for start in range(0, size, 1000):
                connector.execute_many(
                    f"INSERT INTO {_TABLE} (analysis_id, file_sha256) VALUES (%s, %s)",
                    [
                        (root, _file(i + 1))
                        for i in range(start, min(size, start + 1000))
                    ],
                )
    return distribution


def _sample(
    connector: SQLConnector, *, count: int, retained: int, historical: bool
) -> dict[str, Any]:
    distribution = _populate(connector, count)
    backend = connector_backend(connector)
    # These budgets precede measurements. They bound engine work over complete
    # phases, including durable-prefix and terminal proofs, not returned rows.
    native_budget = (
        (1000 * count + 100000) if backend == "sqlite" else (50 * count + 10000)
    )
    sql_budget = 2 * ((count + 63) // 64) + 16 * ((count + 255) // 256 + 1)
    plan = replace(_PLAN, phases={_PHASE: _PLAN.phases[_PHASE]})
    cursor = b""
    count_deleted = 0
    digest = sha256()
    sql = _SQLCost()
    durations: list[float] = []
    seen: set[bytes] = set()
    original = cleanup._analysis_owned_suffix
    selector = (lambda _plan, _spec: None) if historical else original
    with patch.object(cleanup, "_analysis_owned_suffix", selector):
        with (
            _native_cost(connector) as native_cost,
            measure_sql(sql, observe_nested=True),
        ):
            measured = instrument_connector(connector)
            started = perf_counter()
            while True:
                batch_started = perf_counter()
                with measured.transaction():
                    operation = replace(
                        _operation(connector, 3),
                        work=VNextUnitOfWork(measured, backend=backend),
                    )
                    result = cleanup._run_static_phase(operation, cursor, plan, _PHASE)
                durations.append(perf_counter() - batch_started)
                if len(result.row_keys) > 256:
                    raise RuntimeError("transaction exceeded its 256-row contract")
                for key in result.row_keys:
                    if key in seen:
                        raise RuntimeError("cleanup returned a duplicate logical key")
                    seen.add(key)
                    digest.update(len(key).to_bytes(2, "big") + key)
                count_deleted += len(result.row_keys)
                cursor = result.next_cursor
                if not result.row_keys:
                    break
                if len(durations) > (count + 255) // 256 + 1:
                    raise RuntimeError(
                        "cleanup failed to terminate within its batch bound"
                    )
            elapsed = perf_counter() - started
            engine_work = native_cost()
    with connector.read_transaction():
        remaining = connector.fetch_one(f"SELECT COUNT(*) FROM {_TABLE}")
        retained_rows = connector.fetch_one(
            f"SELECT COUNT(*) FROM {_TABLE} WHERE analysis_id = %s", (_RETAINED,)
        )
    if (
        count_deleted != count
        or remaining != (retained,)
        or retained_rows != (retained,)
    ):
        raise RuntimeError("cleanup result or retained-root oracle disagreed")
    return {
        "variant": "historical_range_shape" if historical else "candidate",
        "pending_rows": count,
        "retained_rows": retained,
        "pending_distribution": distribution,
        "retained_fraction": retained / (retained + count),
        "phase_seconds": elapsed,
        "max_transaction_seconds": max(durations),
        "transactions": len(durations),
        "logical_keys_sha256": digest.hexdigest(),
        "native_unit": "sqlite_vm_steps"
        if backend == "sqlite"
        else "mariadb_handler_reads",
        "native_work": engine_work,
        "native_budget": native_budget,
        "sql_calls": sql.calls,
        "sql_seconds": sql.seconds,
        "sql_budget": sql_budget,
        "cost_passed": engine_work <= native_budget and sql.calls <= sql_budget,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mariadb",
        action="store_true",
        help="start an isolated MariaDB 10.11.11 container",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 5:
        parser.error("repetitions must be in 1..5")
    report: dict[str, Any] = {
        "scope": "complete AR_EXCLUSION_ANCHOR phase over partial synthetic graphs; not full ingest or READY audit",
        "retained_rows": 100000,
        "pending_scales": [1000, 10000, 100000],
        "transaction_row_cap": 256,
        "delete_page_cap": 64,
        "repetitions": args.repetitions,
        "samples": [],
    }
    try:
        with TemporaryDirectory(prefix="h2hdb-anchor-cost-") as directory:
            with _database(args.mariadb, Path(directory)) as config:
                with closing(open_generated_database(config)) as connector:
                    report["backend"] = connector_backend(connector)
                    with connector.read_transaction():
                        version_query = (
                            "SELECT VERSION()"
                            if args.mariadb
                            else "SELECT sqlite_version()"
                        )
                        report["engine_version"] = connector.fetch_one(version_query)[0]
                    _seed(connector, (0, 0, 0), retained=100000)
                    for count in report["pending_scales"]:
                        for repetition in range(args.repetitions):
                            variants = (
                                (True, False) if repetition % 2 == 0 else (False, True)
                            )
                            pair = [
                                _sample(
                                    connector,
                                    count=count,
                                    retained=100000,
                                    historical=variant,
                                )
                                for variant in variants
                            ]
                            if (
                                pair[0]["logical_keys_sha256"]
                                != pair[1]["logical_keys_sha256"]
                            ):
                                raise RuntimeError(
                                    "historical and candidate logical keys differ"
                                )
                            report["samples"].extend(pair)
                            print(
                                f"completed pending={count} repetition={repetition + 1}",
                                flush=True,
                            )
        report["median_seconds"] = {
            str(count): {
                variant: statistics.median(
                    item["phase_seconds"]
                    for item in report["samples"]
                    if item["pending_rows"] == count and item["variant"] == variant
                )
                for variant in ("historical_range_shape", "candidate")
            }
            for count in report["pending_scales"]
        }
        report["cost_passed"] = all(
            sample["cost_passed"]
            for sample in report["samples"]
            if sample["variant"] == "candidate"
        )
        report["negative_control_rejected"] = all(
            not sample["cost_passed"]
            for sample in report["samples"]
            if sample["variant"] == "historical_range_shape"
            and sample["pending_rows"] == 100000
        )
        status = (
            0 if report["cost_passed"] and report["negative_control_rejected"] else 1
        )
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        status = 2
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
