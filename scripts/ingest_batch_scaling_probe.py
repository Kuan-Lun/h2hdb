"""Measure catch-up batch costs on disposable real database pipelines.

Synthetic immutable source markers prevent old galleries from being deeply read
again. Public source, analysis, publication and cleanup run without semantic
patches. MemoryLibrary artifacts are neutral fixtures, not raster images or CBZ.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import ingest_pipeline_probe as probe  # noqa: E402 - checkout diagnostic helpers.
from test_vnext_source_marker import MarkerSource  # noqa: E402 - exact marker fixture.
from vnext_pipeline import (  # noqa: E402 - public protocol fixture.
    LEASE_MICROSECONDS,
    MemoryLibrary,
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

from h2hdb import (  # noqa: E402 - checkout public facades.
    CoreConfig,
    LoggerConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb.sql_performance import measure_sql  # noqa: E402 - audit observation.
from h2hdb.vnext_identity import effective_content_digest  # noqa: E402 - codec oracle.


def parse_case(value: str) -> tuple[int, int, int]:
    """Bound fixture size independently from the production transaction caps."""
    try:
        galleries, batch, pages = (int(part) for part in value.split(":"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected galleries:batch:pages") from error
    if not (
        1 <= galleries <= 4096
        and 1 <= batch <= galleries
        and 1 <= pages <= 256
        and galleries * pages <= 65536
    ):
        raise argparse.ArgumentTypeError(
            "require 1..4096 galleries, batch <= galleries, 1..256 pages, "
            "and at most 65536 total pages"
        )
    return galleries, batch, pages


def page_bytes(gid: int, page: int) -> bytes:
    return f"gallery-{gid}-page-{page}".encode()


def verify_catalog(
    config: CoreConfig, galleries: int, pages: int, *, artifacts: bool
) -> str:
    """Compare every public result, including keyset boundaries, to input facts."""
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
        actual: dict[int, tuple[str, str]] = {}
        cursor = None
        for _ in range((galleries + 127) // 128 + 1):
            result = catalog.discover_publications(
                revision=revision, limit=128, after=cursor
            )
            for item in result.publications:
                if item.gid in actual:
                    raise AssertionError("public catalog repeated a gallery")
                if item.content_sha256 is None:
                    raise AssertionError("synthetic publication has no content digest")
                actual[item.gid] = (item.title, item.content_sha256)
            cursor = result.next_cursor
            if cursor is None:
                break
        else:
            raise AssertionError("public catalog exceeded the bounded oracle")
        expected = {
            gid: (
                f"Gallery {gid}",
                effective_content_digest(
                    tuple(
                        sha256(page_bytes(gid, page)).digest() for page in range(pages)
                    )
                ).hex(),
            )
            for gid in range(1, galleries + 1)
        }
        if actual != expected:
            raise AssertionError("catalog differs from the independent input oracle")
        if revision.publication_count != galleries or revision.artifact_count != (
            galleries if artifacts else 0
        ):
            raise AssertionError("catalog count or artifact count differs")
        return sha256(json.dumps(sorted(actual.items())).encode()).hexdigest()


def measure_ready_audit(
    config: CoreConfig,
    *,
    observer_factory: Callable[[], probe.Observer] | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Observe a complete production check separately from incremental work.

    The observer receives every completed SQL event, including nested runtime
    audit scopes. No validator or schema check is replaced by the probe.
    """
    observer = (
        probe.Observer(query_budget=32768)
        if observer_factory is None
        else observer_factory()
    )
    observer.phase = "ready_audit"
    if progress is not None:
        progress({"event": "phase_started", "phase": "ready_audit"})
    started = time.perf_counter()
    with measure_sql(observer, observe_nested=True):
        result = full_check(config)
    seconds = time.perf_counter() - started
    if result.state != "READY":
        raise AssertionError("full READY audit failed")
    measurements = observer.report(query_limit=None)
    if progress is not None:
        progress(
            {
                "event": "phase_finished",
                "phase": "ready_audit",
                "seconds": seconds,
                "sql_calls": measurements["sql_calls"],
                "sql_seconds": measurements["sql_seconds"],
            }
        )
    return {
        "state": result.state,
        "wall_seconds": seconds,
        "measurements": measurements,
    }


def run_case(
    backend: str,
    galleries: int,
    batch: int,
    pages: int,
    *,
    artifacts: bool = False,
    progress: Callable[[dict[str, Any]], None] | None = None,
    query_limit: int | None = 8,
    observer_factory: Callable[[], probe.Observer] | None = None,
    check_next_claim: bool = False,
) -> dict[str, Any]:
    parse_case(f"{galleries}:{batch}:{pages}")
    with (
        tempfile.TemporaryDirectory(prefix="h2hdb-batch-scaling-") as temporary,
        probe.database(backend, Path(temporary)) as original_config,
    ):
        config = original_config.model_copy(
            update={"logger": LoggerConfig.model_validate({"level": "error"})}
        )
        initialize_database(config)
        source = MarkerSource()
        library = MemoryLibrary(source)
        turns = []
        for lower in range(0, galleries, batch):
            count = min(galleries, lower + batch)
            source.forbidden_reads = {item.locator for item in source.galleries}
            deep_before, markers_before = len(source.deep_reads), source.marker_calls
            for gid in range(lower + 1, count + 1):
                source.put(
                    gallery(
                        gid,
                        pages=[page_bytes(gid, page) for page in range(pages)],
                        artists=(),
                        language=None,
                    )
                )
            observer = (
                probe.Observer(query_budget=32768)
                if observer_factory is None
                else observer_factory()
            )
            phases: dict[str, float] = {}
            active_phase = "setup"
            phase_started = time.perf_counter()
            last_progress = 0.0

            def pulse(label: str, *, force: bool = False) -> None:
                nonlocal last_progress
                now = time.perf_counter()
                if progress is not None and (force or now - last_progress >= 15):
                    progress(
                        {
                            "event": "phase_progress",
                            "backend": backend,
                            "galleries": galleries,
                            "batch": batch,
                            "pages": pages,
                            "artifacts": artifacts,
                            "selected": count,
                            "phase": active_phase,
                            "boundary": label,
                            "active_seconds": now - phase_started,
                        }
                    )
                    last_progress = now

            def measured[T](label: str, action: Callable[[], T]) -> T:
                nonlocal active_phase, phase_started
                observer.phase = active_phase = label
                started = phase_started = time.perf_counter()
                pulse("started", force=True)
                try:
                    return action()
                finally:
                    phases[label] = time.perf_counter() - started
                    pulse("finished", force=True)

            with VNextIngestFacade(config) as facade, observer.installed():
                session = measured("claim", lambda: claim_session(facade))
                policy = measured(
                    "policy",
                    lambda: facade.ensure_policy(
                        session, ingest_policy(artifacts_required=artifacts)
                    ),
                )
                receipt = measured(
                    "source",
                    lambda: run_source(
                        facade,
                        session,
                        policy,
                        source,
                        step_budget=100_000,
                        boundary=pulse,
                    ),
                )
                analysis = measured(
                    "analysis",
                    lambda: run_analysis(
                        facade,
                        session,
                        policy,
                        receipt.build_id,
                        step_budget=100_000,
                        boundary=pulse,
                    ),
                )
                publication = measured(
                    "publication",
                    lambda: run_publication(
                        facade,
                        session,
                        policy,
                        library,
                        step_budget=100_000,
                        boundary=pulse,
                    ),
                )
                measured("complete", lambda: facade.complete_ingest(session))
                measured(
                    "cleanup",
                    lambda: drain_maintenance(facade, attempts=100_000, boundary=pulse),
                )
            if not analysis.terminal or not publication.terminal:
                raise AssertionError("pipeline did not finalize")
            if len(source.deep_reads) - deep_before != count - lower:
                raise AssertionError("deep source reads exceeded new galleries")
            if library.render_calls != (count if artifacts else 0):
                raise AssertionError("artifact render count differs from new galleries")
            oracle = verify_catalog(config, count, pages, artifacts=artifacts)
            if check_next_claim:
                with VNextIngestFacade(config) as facade:
                    next_session = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
                    if next_session is None:
                        raise AssertionError("cleanup DONE did not admit next claim")
                    facade.complete_ingest(next_session)
            measurements = observer.report(query_limit=query_limit)
            turns.append(
                {
                    "selected": count,
                    "added": count - lower,
                    "deep_reads": len(source.deep_reads) - deep_before,
                    "marker_calls": source.marker_calls - markers_before,
                    "phases": phases,
                    "measurements": measurements,
                    "oracle": oracle,
                    "cleanup": "DONE",
                    "next_claim": "passed" if check_next_claim else "not_checked",
                }
            )
            if progress is not None:
                progress(
                    {
                        "event": "turn_completed",
                        "backend": backend,
                        "galleries": galleries,
                        "batch": batch,
                        "pages": pages,
                        "artifacts": artifacts,
                        "selected": count,
                        "added": count - lower,
                        "cleanup": "DONE",
                        "seconds": sum(phases.values()),
                        "sql_calls": measurements["sql_calls"],
                        "sql_seconds": measurements["sql_seconds"],
                    }
                )
        audit = measure_ready_audit(
            config, observer_factory=observer_factory, progress=progress
        )
        return {
            "backend": backend,
            "galleries": galleries,
            "batch": batch,
            "pages": pages,
            "artifacts": artifacts,
            "turns": turns,
            "ready_audit": audit,
            "full_ready_audit": "passed",
            "oracle": verify_catalog(config, galleries, pages, artifacts=artifacts),
        }


def experiment_source_hashes() -> dict[str, str]:
    """Bind the two experiment drivers and the marker fixture to their output."""
    files = (
        "scripts/ingest_batch_scaling_probe.py",
        "scripts/ingest_locator_reuse_probe.py",
        "tests/test_vnext_source_marker.py",
    )
    return {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in files}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--case", type=parse_case, action="append", required=True)
    parser.add_argument("--artifacts", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report: dict[str, Any] = {
        "status": "incomplete",
        "provenance": probe.source_provenance(),
        "experiment_sources_sha256": experiment_source_hashes(),
        "cases": [],
        "notes": (
            "Real generated database, public source/analysis/publication, old-gallery "
            "deep reads forbidden, public catalog oracle and cleanup DONE each turn, "
            "final full READY. Synthetic unique content; no duplicate/conflict workload, "
            "raster decoder, CBZ encoder, real source-byte inventory, fsync or NAS I/O. "
            "Optional MemoryLibrary artifacts exercise neutral core artifact contracts. "
            "Timings include observation and are not an SLO verdict. SQL events are "
            "counted once; operation query families and phase counts/rows are retained. "
            "Changing publication batch does not change the bounded transaction caps. "
            "Different histories must converge to the same final public oracle."
        ),
    }
    probe.write_report(args.output, report)
    try:
        for galleries, batch, pages in args.case:
            report["cases"].append(
                run_case(
                    args.backend,
                    galleries,
                    batch,
                    pages,
                    artifacts=args.artifacts,
                    progress=lambda row: print(json.dumps(row), flush=True),
                )
            )
            probe.write_report(args.output, report)
        if (
            report["provenance"] != probe.source_provenance()
            or report["experiment_sources_sha256"] != experiment_source_hashes()
        ):
            raise RuntimeError("experiment source changed during the run")
        report["status"] = "completed"
    except BaseException as error:
        report.update(status="failed", error=type(error).__name__)
        raise
    finally:
        probe.write_report(args.output, report)


if __name__ == "__main__":
    main()
