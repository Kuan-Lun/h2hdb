"""Check fixed SQL-work budgets on disposable public ingest pipelines.

This is a manual engineering acceptance, separate from tool-correctness tests.
It may fail on the current implementation. It measures connector calls, returned
rows and observed elapsed time; it cannot certify a NAS wall-clock objective.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import ingest_batch_scaling_probe as batch_probe  # noqa: E402 - checkout-only tool.
import ingest_pipeline_probe as probe  # noqa: E402 - shared exact SQL observer.
from performance_attribution import assess_attribution  # noqa: E402
from vnext_pipeline import (  # noqa: E402 - public protocol fixtures.
    LEASE_MICROSECONDS,
    MemoryLibrary,
    MemorySource,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
)

import h2hdb.vnext_cleanup_repository as cleanup  # noqa: E402 - observation only.
from h2hdb import (  # noqa: E402
    CoreConfig,
    LoggerConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb.sql_performance import measure_sql  # noqa: E402
from h2hdb.vnext_identity import effective_content_digest  # noqa: E402

# Versioned, normative engineering ceilings: these constants are not imported
# from runtime plans, inferred from timings, or fitted to a baseline run. They
# deliberately permit substantial fixed normalized-schema control work, but
# charge variable work only to NEW input. Repeated retained-gallery traversal
# does not earn additional allowance. Passing these finite checks is not proof
# of asymptotic complexity, server rows examined, or a 12/24-hour throughput SLO.
CONTRACT_VERSION = 1
# (fixed calls per turn, calls per new gallery, calls per new file)
PHASE_CEILINGS = {
    "claim": (128, 0, 0),
    "policy": (128, 0, 0),
    "source": (2048, 1024, 16),
    "analysis": (4096, 2048, 16),
    "publication": (4096, 2048, 16),
    "complete": (128, 0, 0),
    "cleanup": (2048, 1024, 16),
}
GO_FILE_MULTIPLICITY = {
    "GO_FILESYSTEM_SEAL": 1,
    "GO_FILESYSTEM_VALUES": 4,
    "GO_FILESYSTEM_ANCHOR": 1,
}
EXIT_CODES = {"satisfied": 0, "violated": 1, "incomplete": 2}


class AcceptanceObserver(probe.Observer):
    """Check exact-query aggregation against a separate delivered-event ledger."""

    def __init__(self) -> None:
        super().__init__(query_budget=32768)
        self.event_calls: Counter[str] = Counter()
        self.event_rows: Counter[str] = Counter()

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        self.event_calls[category] += 1
        self.event_rows[category] += read_rows
        super().record_sql_operation(category, elapsed, query, read_rows)

    def report(self, *, query_limit: int | None = None) -> dict[str, Any]:
        result = super().report(query_limit=query_limit)
        if query_limit is not None:
            raise ValueError("acceptance requires all query groups")
        calls: Counter[str] = Counter()
        rows: Counter[str] = Counter()
        for row in result["queries"]:
            calls[row["category"]] += row["calls"]
            rows[row["category"]] += row["returned_rows"]
        if calls != self.event_calls or rows != self.event_rows:
            raise ValueError("query groups differ from delivered SQL event ledger")
        result["delivered_event_calls"] = dict(self.event_calls)
        result["delivered_event_rows"] = dict(self.event_rows)
        return result


def check(name: str, observed: int, upper_bound: int, rationale: str) -> dict[str, Any]:
    return {
        "name": name,
        "observed": observed,
        "upper_bound": upper_bound,
        "unit": "completed SQL connector calls",
        "rationale": rationale,
        "status": "satisfied" if observed <= upper_bound else "violated",
    }


def phase_budget(phase: str, added: int, pages: int) -> int:
    fixed, per_gallery, per_file = PHASE_CEILINGS[phase]
    return fixed + per_gallery * added + per_file * added * (pages + 1)


def file_page_budget(added: int, pages: int) -> int:
    # 128 files/page is an independently declared contract boundary. Eight
    # calls/file plus 256/page is a batching target, not current-code behavior.
    files = pages + 1  # The neutral source has exactly one metadata file.
    return added * (8 * files + 256 * ((files + 127) // 128))


def retirement_budget(rows: int) -> int:
    # Target SELECT+DELETE per <=64-key group; 16 calls per <=256-row page
    # plus one terminal page covers <=4 physical specs, cursor and empty probes.
    # Expected rows come from fixture input, never from observed mutations.
    return 2 * ((rows + 63) // 64) + 16 * ((rows + 255) // 256 + 1)


def _sql_rows(measurements: Mapping[str, Any]) -> list[dict[str, Any]]:
    attribution = assess_attribution(measurements)
    if attribution["status"] != "complete":
        raise ValueError(f"incomplete SQL attribution: {attribution['reasons']}")
    return [row for row in measurements["queries"] if row["category"] == "sql"]


def assess_turn(turn: dict[str, Any], *, pages: int) -> dict[str, Any]:
    rows = _sql_rows(turn["measurements"])
    by_phase: Counter[str] = Counter()
    for row in rows:
        by_phase[row["pipeline"]] += row["calls"]
    if set(by_phase) != PHASE_CEILINGS.keys() or any(
        value <= 0 for value in by_phase.values()
    ):
        raise ValueError("missing or unclassified SQL workflow phase")
    if set(turn["phases"]) != set(PHASE_CEILINGS) or turn["cleanup"] != "DONE":
        raise ValueError("incomplete public workflow boundaries")
    if any(
        not isinstance(seconds, (int, float))
        or isinstance(seconds, bool)
        or not math.isfinite(seconds)
        or seconds < 0
        for seconds in turn["phases"].values()
    ):
        raise ValueError("invalid workflow elapsed measurements")
    added = turn["added"]
    file_commits = sum(
        row["calls"]
        for row in rows
        if (row["pipeline"], row["phase"], row["operation"])
        == ("source", "commit", "FILE_PAGE")
    )
    if type(added) is not int or added <= 0 or file_commits <= 0:
        raise ValueError("new source files lack measured FILE_PAGE commits")
    checks = [
        check(
            f"{phase}.sql_calls",
            by_phase[phase],
            phase_budget(phase, added, pages),
            "Fixed per-turn control allowance + per-new-gallery/file allowance; "
            "retained galleries do not increase the budget. See PHASE_CEILINGS.",
        )
        for phase in PHASE_CEILINGS
    ]
    checks.append(
        check(
            "source.FILE_PAGE.commit.sql_calls",
            file_commits,
            file_page_budget(added, pages),
            "Batching target: 8 calls/new file + 256 calls/new 128-file page; "
            "one metadata file per gallery is included.",
        )
    )
    return {
        "status": "violated"
        if any(item["status"] == "violated" for item in checks)
        else "satisfied",
        "dimensions": {
            "added_galleries": added,
            "retained_galleries": turn["selected"] - added,
            "new_files": added * (pages + 1),
            "pages_per_gallery": pages,
        },
        "checks": checks,
        "attribution": assess_attribution(turn["measurements"]),
    }


def parse_replacement(value: str) -> tuple[int, int]:
    try:
        pages, cycles = (int(part) for part in value.split(":"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected pages:cycles") from error
    if not (0 <= pages <= 512 and 1 <= cycles <= 8):
        raise argparse.ArgumentTypeError("require 0..512 pages and 1..8 cycles")
    return pages, cycles


@contextmanager
def observe_retirement(samples: list[dict[str, Any]]) -> Iterator[None]:
    """Observe real phase bodies without changing plans, transactions or results."""
    original = cleanup._run_static_phase

    def observed(
        operation: cleanup._CleanupOperation,
        cursor: bytes,
        plan: cleanup._StaticTargetPlan,
        phase: str,
        **kwargs: Any,
    ) -> cleanup._Mutation:
        if phase not in GO_FILE_MULTIPLICITY:
            return original(operation, cursor, plan, phase, **kwargs)
        counter = AcceptanceObserver()
        counter.phase = phase
        with measure_sql(counter, observe_nested=True):
            result = original(operation, cursor, plan, phase, **kwargs)
        samples.append(
            {
                "phase": phase,
                "row_keys": [key.hex() for key in result.row_keys],
                "measurements": counter.report(),
            }
        )
        return result

    with patch.object(cleanup, "_run_static_phase", observed):
        yield


def assess_retirement(samples: list[dict[str, Any]], pages: int) -> dict[str, Any]:
    checks = []
    for phase, multiplicity in GO_FILE_MULTIPLICITY.items():
        selected = [sample for sample in samples if sample["phase"] == phase]
        keys = [key for sample in selected for key in sample["row_keys"]]
        expected_rows = multiplicity * (pages + 1)
        if len(keys) != expected_rows or len(set(keys)) != expected_rows:
            raise ValueError(f"{phase}: retired facts differ from input oracle")
        if any(len(sample["row_keys"]) > 256 for sample in selected):
            raise ValueError("cleanup exceeded independent 256-row page contract")
        calls = sum(
            row["calls"]
            for sample in selected
            for row in _sql_rows(sample["measurements"])
        )
        checks.append(
            check(
                f"{phase}.sql_calls",
                calls,
                retirement_budget(expected_rows),
                "2 calls per 64 expected keys + 16 per 256-row page and terminal "
                "page; at most 4 physical specs. Counts every phase-body query. "
                "Expected retired rows are input files times fixed multiplicity.",
            )
            | {"expected_retired_rows": expected_rows, "retired_rows": len(keys)}
        )
    return {
        "status": "violated"
        if any(item["status"] == "violated" for item in checks)
        else "satisfied",
        "checks": checks,
    }


def replacement_payloads(cycle: int, pages: int) -> list[bytes]:
    return [f"revision-{cycle}-page-{page}".encode() for page in range(pages)]


def verify_replacement(config: CoreConfig, cycle: int, pages: int) -> str:
    expected = effective_content_digest(
        tuple(sha256(value).digest() for value in replacement_payloads(cycle, pages))
    ).hex()
    with closing(VNextCatalogFacade(config)) as facade:
        revision = facade.get_catalog_revision()
        page = facade.discover_publications(revision=revision, limit=128)
        if pages == 0:
            # Metadata-only observations remain source facts but have no
            # publishable content. An empty public catalog is the exact oracle.
            if (
                revision.publication_count != 0
                or revision.artifact_count != 0
                or page.publications
                or page.next_cursor is not None
            ):
                raise AssertionError(
                    "metadata-only source published unexpected content"
                )
            return expected
        if (
            revision.publication_count != 1
            or revision.artifact_count != 0
            or len(page.publications) != 1
            or page.next_cursor is not None
            or page.publications[0].gid != 1001
            or page.publications[0].content_sha256 != expected
        ):
            raise AssertionError("retained public catalog differs from input oracle")
    return expected


def run_replacement(
    backend: str,
    pages: int,
    cycles: int,
    *,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    parse_replacement(f"{pages}:{cycles}")
    with (
        tempfile.TemporaryDirectory(prefix="h2hdb-retirement-acceptance-") as temporary,
        probe.database(backend, Path(temporary)) as original_config,
    ):
        config = original_config.model_copy(
            update={"logger": LoggerConfig.model_validate({"level": "error"})}
        )
        initialize_database(config)
        source = MemorySource()
        library = MemoryLibrary(source)
        turns = []
        for cycle in range(cycles + 1):
            source.put(
                gallery(
                    1001,
                    pages=replacement_payloads(cycle, pages),
                    artists=(),
                    language=None,
                    comment=f"replacement revision {cycle}",
                )
            )
            observer = AcceptanceObserver()
            samples: list[dict[str, Any]] = []
            phases: dict[str, float] = {}

            def measured[T](phase: str, action: Callable[[], T]) -> T:
                observer.phase = phase
                if progress is not None:
                    progress({"event": "phase_started", "cycle": cycle, "phase": phase})
                started = time.perf_counter()
                try:
                    return action()
                finally:
                    phases[phase] = time.perf_counter() - started

            with VNextIngestFacade(config) as facade, observer.installed():
                session = measured("claim", lambda: claim_session(facade))
                policy = measured(
                    "policy",
                    lambda: facade.ensure_policy(
                        session,
                        ingest_policy(
                            artifacts_required=False,
                            spam_occurrence_threshold=77 + cycle,
                        ),
                    ),
                )
                receipt = measured(
                    "source", lambda: run_source(facade, session, policy, source)
                )
                analysis = measured(
                    "analysis",
                    lambda: run_analysis(facade, session, policy, receipt.build_id),
                )
                publication = measured(
                    "publication",
                    lambda: run_publication(facade, session, policy, library),
                )
                if not analysis.terminal or not publication.terminal:
                    raise AssertionError("public pipeline did not finalize")
                measured("complete", lambda: facade.complete_ingest(session))
                with observe_retirement(samples):
                    measured(
                        "cleanup", lambda: drain_maintenance(facade, attempts=4096)
                    )
            oracle = verify_replacement(config, cycle, pages)
            # A direct claim must succeed after DONE; do not hide pending cleanup
            # behind claim_session's retry/drain helper. Release the disposable
            # probe's empty claim so the following cycle starts normally.
            with VNextIngestFacade(config) as facade:
                next_session = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
                if next_session is None:
                    raise AssertionError("cleanup DONE did not admit the next claim")
                facade.complete_ingest(next_session)
            if full_check(config).state != "READY":
                raise AssertionError("replacement full READY audit failed")
            turn = {
                "cycle": cycle,
                "selected": 1,
                "added": 1,
                "phases": phases,
                "measurements": observer.report(),
                "cleanup": "DONE",
                "full_ready_audit": "passed",
                "next_claim": "passed",
                "oracle": oracle,
                "retirement_samples": samples,
            }
            turn["acceptance"] = assess_turn(turn, pages=pages)
            if cycle:
                turn["retirement_acceptance"] = assess_retirement(samples, pages)
            turns.append(turn)
        return {
            "kind": "replacement",
            "backend": backend,
            "pages": pages,
            "replacement_cycles": cycles,
            "turns": turns,
        }


def source_hashes() -> dict[str, str]:
    return batch_probe.experiment_source_hashes() | {
        name: sha256((ROOT / name).read_bytes()).hexdigest()
        for name in (
            "scripts/check-ingest-database-performance.py",
            "scripts/performance_attribution.py",
        )
    }


def imported_sources() -> dict[str, dict[str, str]]:
    """Reject an already-imported wheel when claiming checkout measurements."""
    result = {}
    root = (ROOT / "src" / "h2hdb").resolve()
    for name, module in tuple(sys.modules.items()):
        if name != "h2hdb" and not name.startswith("h2hdb."):
            continue
        filename = getattr(module, "__file__", None)
        if filename is None or not Path(filename).resolve().is_relative_to(root):
            raise RuntimeError(f"imported {name} is not from the measured checkout")
        path = Path(filename).resolve()
        result[name] = {
            "path": str(path),
            "sha256": sha256(path.read_bytes()).hexdigest(),
        }
    if not result:
        raise RuntimeError("Core package was not loaded")
    return result


def acceptance_status(cases: list[dict[str, Any]]) -> str:
    statuses = []
    for case in cases:
        turns = case.get("turns", [])
        kind = case.get("kind")
        if kind == "append":
            expected = list(range(0, case["galleries"], case["batch"]))
            if len(turns) != len(expected) or any(
                turn.get("selected") != min(case["galleries"], lower + case["batch"])
                or turn.get("added") != min(case["batch"], case["galleries"] - lower)
                for turn, lower in zip(turns, expected)
            ):
                return "incomplete"
        elif kind == "replacement":
            if [turn.get("cycle") for turn in turns] != list(
                range(case["replacement_cycles"] + 1)
            ):
                return "incomplete"
        else:
            return "incomplete"
        for turn in turns:
            statuses.append(turn.get("acceptance", {}).get("status"))
            if kind == "replacement" and turn["cycle"] > 0:
                statuses.append(turn.get("retirement_acceptance", {}).get("status"))
    if not statuses or any(
        value not in {"satisfied", "violated"} for value in statuses
    ):
        return "incomplete"
    return "violated" if "violated" in statuses else "satisfied"


def print_progress(row: dict[str, Any]) -> None:
    """Keep full query evidence in the report, never duplicate it to stdout."""
    keys = (
        "event",
        "backend",
        "galleries",
        "batch",
        "pages",
        "artifacts",
        "cycle",
        "selected",
        "added",
        "cleanup",
        "phase",
        "boundary",
        "active_seconds",
        "seconds",
        "sql_calls",
        "sql_seconds",
    )
    print(json.dumps({key: row[key] for key in keys if key in row}), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--allow-mariadb", action="store_true")
    parser.add_argument(
        "--case", type=batch_probe.parse_case, action="append", default=[]
    )
    parser.add_argument(
        "--replacement-case", type=parse_replacement, action="append", default=[]
    )
    parser.add_argument("--artifacts", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.case and not args.replacement_case:
        parser.error("at least one --case or --replacement-case is required")
    if args.backend == "mariadb" and not args.allow_mariadb:
        parser.error(
            "MariaDB starts a disposable 10.11.11 container; add --allow-mariadb"
        )
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "error",
        "acceptance": {"status": "incomplete"},
        "contract_version": CONTRACT_VERSION,
        "scope": "core_public_pipeline_sql_work",
        "backend": args.backend,
        "provenance": probe.source_provenance(),
        "experiment_sources_sha256": source_hashes(),
        "cases": [],
        "limits": [
            "Finite synthetic unique-content fixtures, not a 132046-gallery run.",
            "No raster, CBZ, physical disk I/O, network-latency model or wall-clock SLO.",
            "SQL calls are completed connector methods; rows are returned, not examined.",
            "Whole-phase ceilings are regression targets, not fitted current-code models.",
            "Setup/full READY/catalog oracles and post-cleanup claims are outside phase costs.",
            "Replacement uses neutral no-artifact fixture, including when --artifacts is set.",
        ],
    }
    probe.write_report(args.output, report)
    try:
        report["imported_sources"] = imported_sources()
        for galleries, batch, pages in args.case:
            result = batch_probe.run_case(
                args.backend,
                galleries,
                batch,
                pages,
                artifacts=args.artifacts,
                query_limit=None,
                observer_factory=AcceptanceObserver,
                check_next_claim=True,
                progress=print_progress,
            )
            result["kind"] = "append"
            for turn in result["turns"]:
                turn["acceptance"] = assess_turn(turn, pages=pages)
            report["cases"].append(result)
            probe.write_report(args.output, report)
        for pages, cycles in args.replacement_case:
            report["cases"].append(
                run_replacement(
                    args.backend,
                    pages,
                    cycles,
                    progress=print_progress,
                )
            )
            probe.write_report(args.output, report)
        if (
            report["provenance"] != probe.source_provenance()
            or report["experiment_sources_sha256"] != source_hashes()
        ):
            raise RuntimeError("experiment sources changed during execution")
        report["imported_sources"] = imported_sources()
        report["status"] = "completed"
        report["acceptance"]["status"] = acceptance_status(report["cases"])
    except Exception as error:
        report["error"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        probe.write_report(args.output, report)
    return EXIT_CODES[report["acceptance"]["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
