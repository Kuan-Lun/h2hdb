"""Public source-to-publication experiment for durable assembly-window selection.

No FK or READY validation is disabled. The optional MariaDB configuration must
refer to a task-owned loopback server; every case creates and drops its own
randomly named database. Credentials never enter the report.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import tempfile
import time
import traceback
from collections.abc import Iterator
from contextlib import closing, contextmanager, nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("H2HDB_SOURCE_PROBE_RUNTIME_ROOT", str(ROOT)))
sys.path[:0] = [str(RUNTIME_ROOT / "src"), str(ROOT / "tests")]

from vnext_pipeline import (  # noqa: E402 - explicit checkout fixture.
    MemoryLibrary,
    MemorySource,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
)
from vnext_source_staging_cost import (  # noqa: E402 - shared native contract.
    HANDLER_LIMITS,
    SQLITE_VM_LIMIT,
    WINDOW,
    SelectionWork,
    budget_failures,
    observe_pending_queries,
    unbounded_pending_control,
)

from h2hdb import (  # noqa: E402 - checkout runtime.
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb.sql_performance import measure_sql  # noqa: E402 - checkout runtime.


@dataclass
class SourceSQL:
    """Completed connector calls, not expanded driver/server statement counts."""

    sql_calls: int = 0
    sql_seconds: float = 0.0
    returned_rows: int = 0
    connection_calls: int = 0
    transaction_calls: int = 0

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        del query
        if category == "sql":
            self.sql_calls += 1
            self.sql_seconds += elapsed
            self.returned_rows += read_rows
        elif category == "connection":
            self.connection_calls += 1
        else:
            self.transaction_calls += 1


def run_case(
    config: CoreConfig,
    galleries: int,
    *,
    degraded: bool,
    instrumented: bool = True,
    publish: bool = True,
) -> dict[str, Any]:
    """Execute the public workflow; evaluate the fixed budget only afterwards."""

    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                gid,
                title=f"Window gallery {gid}",
                locator=(f"gallery-{gid:06d}",),
                pages=[f"page-{gid}".encode()],
                artists=(),
                language=None,
            )
            for gid in range(1, galleries + 1)
        ]
    )
    library = MemoryLibrary(source)
    source_sql = SourceSQL()
    selection = SelectionWork()
    started = time.perf_counter()
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade, lease=10_000_000_000)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        source_started = time.perf_counter()
        with (
            unbounded_pending_control() if degraded else nullcontext(),
            observe_pending_queries(config.database.sql_type)
            if instrumented
            else nullcontext(selection) as selection,
            measure_sql(source_sql, observe_nested=True)
            if instrumented
            else nullcontext(),
        ):
            receipt = run_source(facade, session, policy, source, step_budget=100_000)
        source_seconds = time.perf_counter() - source_started
        assert receipt.sealed
        assert receipt.discovered_galleries == receipt.staged_galleries == galleries
        if publish:
            run_analysis(facade, session, policy, receipt.build_id, step_budget=100_000)
            run_publication(facade, session, policy, library, step_budget=100_000)
        facade.complete_ingest(session)
    if publish:
        with closing(VNextCatalogFacade(config)) as catalog:
            revision = catalog.get_catalog_revision()
            assert revision.publication_count == galleries
            found: set[int] = set()
            cursor = None
            while True:
                page = catalog.discover_publications(revision=revision, after=cursor)
                for publication in page.publications:
                    assert publication.gid not in found
                    found.add(publication.gid)
                cursor = page.next_cursor
                if cursor is None:
                    break
            assert found == set(range(1, galleries + 1))
    assert full_check(config).state == "READY"
    positions = [
        sample.position for sample in selection.samples if sample.position is not None
    ]
    if instrumented:
        assert positions == list(range(galleries))
    failures = (
        budget_failures(selection.samples, config.database.sql_type)
        if instrumented
        else []
    )
    return {
        "backend": config.database.sql_type,
        "galleries": galleries,
        "degraded_unbounded_control": degraded,
        "fixed_budget": {
            "window": WINDOW,
            "sqlite_vm_max": SQLITE_VM_LIMIT,
            "handler_max": HANDLER_LIMITS,
        },
        "runtime_source_root": str(RUNTIME_ROOT),
        "instrumented": instrumented,
        "budget_passed": not failures if instrumented else None,
        "budget_failures": failures,
        "source_sql": asdict(source_sql),
        "source_native": asdict(selection),
        "source_seconds": source_seconds,
        "full_workflow_seconds": time.perf_counter() - started if publish else None,
        "validation_seconds": time.perf_counter() - started,
        "source_sealed": receipt.sealed,
        "validation_scope": "publication" if publish else "sealed_source_only",
        "publication_count": galleries if publish else None,
        "READY": "READY",
        "note": "SQL counts are completed connector calls, not expanded driver/server statement counts. Native counters include all source work after each connector finishes connecting and before close, including checkpoint/receipt reads; connection-initialization PRAGMA/SET work is outside those counters. Pending-query counters cover its real execution once. Instrumented walls include measurement overhead; plain walls cover the complete source phase including connection setup but do not evaluate native budgets. The degraded reader uses the old global-prefix algorithm under the candidate orchestration, not an exact baseline machine. Comparisons require an independently recorded exclusive schedule and do not prove the 24-hour NAS goal.",
    }


@contextmanager
def database(backend: str, private_config: Path | None) -> Iterator[CoreConfig]:
    if backend == "sqlite":
        with tempfile.TemporaryDirectory(prefix="h2h-staging-window-") as temp:
            yield CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite", database=str(Path(temp) / "catalog.sqlite3")
                )
            )
        return
    if private_config is None:
        raise ValueError(
            "MariaDB requires the task-owned private loopback configuration"
        )
    import mysql.connector

    connection_settings = json.loads(private_config.read_text())
    if connection_settings.get("host") not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("this probe accepts only its task-owned loopback server")
    name = "staging_window_" + secrets.token_hex(8)
    with mysql.connector.connect(**connection_settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE " + name)
    try:
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb", **{**connection_settings, "database": name}
            )
        )
    finally:
        with mysql.connector.connect(**connection_settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP DATABASE " + name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    parser.add_argument("--galleries", type=int, choices=range(1, 1026), required=True)
    parser.add_argument("--degraded", action="store_true")
    parser.add_argument("--private-config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must be a new path")
    try:
        with database(args.backend, args.private_config) as config:
            result = run_case(config, args.galleries, degraded=args.degraded)
    except Exception as error:
        # Driver messages may contain connection material. Preserve only the
        # exception class and non-secret inputs; the failed run remains evidence.
        result = {
            "backend": args.backend,
            "galleries": args.galleries,
            "degraded_unbounded_control": args.degraded,
            "budget_passed": None,
            "error_type": type(error).__name__,
            "error_locations": [
                {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(error.__traceback__)
            ],
            "status": "incomplete",
        }
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        raise SystemExit(2) from None
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                name: result[name]
                for name in (
                    "backend",
                    "galleries",
                    "degraded_unbounded_control",
                    "budget_passed",
                    "publication_count",
                    "READY",
                )
            }
        )
    )
    raise SystemExit(0 if result["budget_passed"] else 1)


if __name__ == "__main__":
    main()
