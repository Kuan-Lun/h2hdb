"""Investigate source, full audits and cleanup on disposable synthetic databases.

This manual probe never accepts a database path, DSN, or remote host. SQLite
uses a temporary directory; MariaDB uses a private 10.11.11 Testcontainer.
The memory adapters exclude filesystem scanning and image/archive work.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from compaction_contracts import (  # noqa: E402 - read-only fixture oracle.
    current_compaction_layout,
    retained_compaction_roots,
)
from ingest_growth_cleanup_probe import (  # noqa: E402 - checkout-only fixtures.
    database,
    write_report,
)
from ingest_pipeline_probe import (  # noqa: E402 - shared synthetic shape generator.
    Shape,
    source_for,
)
from vnext_pipeline import (  # noqa: E402 - production public API driver.
    LEASE_MICROSECONDS,
    MemoryGallery,
    MemoryLibrary,
    claim_session,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
)

from h2hdb import (  # noqa: E402 - select this checkout's sources.
    CoreConfig,
    LoggerConfig,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)
from h2hdb.sql_performance import (  # noqa: E402 - independent physical call observer.
    _current_scope,
    _MeasuredConnector,
    measure_sql,
)
from h2hdb.vnext_identity import (  # noqa: E402 - independent publication oracle.
    effective_content_digest,
)


@dataclass(frozen=True)
class Case:
    name: str
    shape: Shape
    revisions: int = 1


def matrix(group: str) -> tuple[Case, ...]:
    """Vary row windows and raw comment lengths near codec sizes independently."""
    groups = {
        "baseline": (Case("baseline", Shape(), 3),),
        "galleries": tuple(
            Case(f"galleries-{n}", Shape(galleries=n)) for n in (8, 32, 127, 128, 129)
        ),
        "pages": tuple(Case(f"pages-{n}", Shape(pages=n)) for n in (127, 128, 129)),
        "canonical": tuple(
            Case(f"canonical-{n}", Shape(metadata_bytes=n))
            for n in (32767, 32768, 32769, 65535, 65536, 65537)
        ),
        "retention": (Case("retention", Shape(galleries=8, tags=32), 9),),
        "depth": (Case("depth", Shape(tags=32), 18),),
        "tags": tuple(
            Case(f"tags-{n}", Shape(galleries=32, pages=16, tags=n))
            for n in (0, 32, 128)
        ),
    }
    if group == "all":
        return tuple(case for cases in groups.values() for case in cases)
    if group not in groups:
        raise ValueError("unknown matrix group")
    return groups[group]


@dataclass
class Query:
    calls: int = 0
    seconds: float = 0.0
    returned_rows: int = 0
    max_seconds: float = 0.0


class PhysicalObserver:
    """Observe one connector API call even when a nested diagnostic owns SQL.

    The wrapper executes the original operation exactly once and does not change
    parameters or results. Query texts contain placeholders; values are never
    retained. Counts include successful and failed calls; rows describe returned
    Python objects, not the server's examined rows. Timings include telemetry.
    """

    def __init__(
        self, *, budget: int = 32768, label: Callable[[], str] | None = None
    ) -> None:
        self.budget = budget
        self.label = label
        self.queries: dict[tuple[str, str, str], Query] = defaultdict(Query)

    def record_sql_operation(
        self, category: str, elapsed: float, query: str, read_rows: int
    ) -> None:
        # This outer recorder activates connector instrumentation. The physical
        # wrapper is the sole counting path; recording again would double count.
        pass

    @contextmanager
    def installed(self) -> Iterator[None]:
        original = _MeasuredConnector._call
        observer = self

        def call[T](
            connector: _MeasuredConnector,
            category: Any,
            action: Callable[[], T],
            query: str = "",
        ) -> T:
            scope = _current_scope()
            recorder = scope.recorder if scope is not None else None
            fallback = observer.label() if observer.label is not None else "outside"
            label = str(getattr(recorder, "operation", fallback))
            stack = getattr(recorder, "stack", ())
            if stack:
                span = stack[-1]
                label = span.name
                if validator := span.labels.get("validator"):
                    label += ":" + str(validator)
            key = (label, category, query)
            if key not in observer.queries and len(observer.queries) >= observer.budget:
                raise RuntimeError("physical SQL observation budget exceeded")
            started = time.perf_counter()
            rows = 0
            try:
                result = original(connector, category, action, query)
                match result:
                    case list():
                        rows = len(result)
                    case tuple() if result:
                        rows = 1
                return result
            finally:
                elapsed = time.perf_counter() - started
                value = observer.queries[key]
                value.calls += 1
                value.seconds += elapsed
                value.returned_rows += rows
                value.max_seconds = max(value.max_seconds, elapsed)

        with patch.object(_MeasuredConnector, "_call", call), measure_sql(self):
            yield

    def report(self) -> dict[str, Any]:
        queries = [
            {
                "operation": operation,
                "category": category,
                "sql": sql,
                "fingerprint": sha256(sql.encode()).hexdigest()[:16],
                **asdict(value),
            }
            for (operation, category, sql), value in self.queries.items()
        ]
        return {
            "sql_calls": sum(q["calls"] for q in queries if q["category"] == "sql"),
            "returned_rows": sum(
                q["returned_rows"] for q in queries if q["category"] == "sql"
            ),
            **{
                category + "_seconds": sum(
                    q["seconds"] for q in queries if q["category"] == category
                )
                for category in ("sql", "connection", "transaction")
            },
            "queries": sorted(queries, key=lambda q: q["seconds"], reverse=True),
        }


class LogCapture(logging.Handler):
    """Capture only diagnostic messages; cap the retained report independently."""

    def __init__(self, *, budget: int = 100000) -> None:
        super().__init__(logging.DEBUG)
        self.budget = budget
        self.messages: list[str] = []
        self.overflowed = False

    def emit(self, record: logging.LogRecord) -> None:
        if len(self.messages) >= self.budget:
            self.overflowed = True
            return
        self.messages.append(record.getMessage())

    @contextmanager
    def installed(self) -> Iterator[None]:
        logger = logging.getLogger("h2hdb")
        previous_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self)
        try:
            yield
        finally:
            logger.removeHandler(self)
            logger.setLevel(previous_level)


def measured[T](
    action: Callable[[], T], *, label: Callable[[], str] | None = None
) -> tuple[T, dict[str, Any]]:
    observer = PhysicalObserver(label=label)
    capture = LogCapture()
    with observer.installed(), capture.installed():
        started = time.perf_counter()
        result = action()
        seconds = time.perf_counter() - started
    if capture.overflowed:
        raise RuntimeError("diagnostic log observation budget exceeded")
    return result, {
        "seconds": seconds,
        **observer.report(),
        "diagnostic_messages": capture.messages,
    }


def verify_diagnostic_counters(
    report: dict[str, Any], *, required: bool, allow_quiet: bool = False
) -> None:
    """Compare production root counters with separately observed physical calls."""
    prefix = "database_performance "
    events = [
        json.loads(message[len(prefix) :])
        for message in report["diagnostic_messages"]
        if message.startswith(prefix)
    ]
    report["database_events"] = events
    terminal = [
        event
        for event in events
        if event["event"] in {"completed", "failed", "interrupted"}
    ]
    if not required:
        report["diagnostic_counter_check"] = "disabled_by_log_level"
        return
    if not terminal and allow_quiet:
        if report.get("outcome") != "DONE" or any(
            query["category"] == "sql"
            and query["sql"]
            .lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            for query in report["queries"]
        ):
            raise RuntimeError("only a read-only DONE probe can omit INFO diagnostics")
        report["diagnostic_counter_check"] = "quiet_readonly_DONE_at_INFO"
        return
    if len(terminal) != 1 or terminal[0]["event"] != "completed":
        raise RuntimeError(
            "measured maintenance call lacks one successful terminal log"
        )
    event = terminal[0]
    if (
        event["sql_calls"] != report["sql_calls"]
        or event["read_rows"] != report["returned_rows"]
    ):
        raise RuntimeError("production diagnostic counters disagree with physical SQL")
    actual: dict[str, int] = defaultdict(int)
    for query in report["queries"]:
        if query["category"] == "sql":
            actual[query["fingerprint"]] += query["calls"]
    report["unmapped_aggregate_fingerprints"] = []
    for query in event.get("query_top", ()):
        if query["fingerprint"] == "other":
            report["unmapped_aggregate_fingerprints"].append(query)
            continue
        if actual[query["fingerprint"]] != query["calls"]:
            raise RuntimeError("production SQL fingerprint count is incorrect")
    report["diagnostic_counter_check"] = "passed"


def snapshot(config: CoreConfig, expected: tuple[MemoryGallery, ...]) -> dict[str, Any]:
    """Independent public oracle across the catalog's hard 128-row pages."""
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
        found = {}
        cursor = None
        for _ in range((len(expected) + 127) // 128 + 1):
            page = catalog.discover_publications(
                revision=revision, limit=128, after=cursor
            )
            for value in page.publications:
                if value.gid in found:
                    raise RuntimeError("duplicate public GID across keyset pages")
                found[value.gid] = value
            cursor = page.next_cursor
            if cursor is None:
                break
        else:
            raise RuntimeError("public snapshot exceeded page budget")
    if set(found) != {item.gid for item in expected}:
        raise RuntimeError("public GID set changed")
    for item in expected:
        digest = effective_content_digest(
            tuple(
                sha256(data).digest()
                for name, data in item.files.items()
                if name.endswith(b".png")
            )
        ).hex()
        value = found[item.gid]
        if (value.title, value.summary, value.content_sha256) != (
            item.title,
            item.comment,
            digest,
        ):
            raise RuntimeError("public content/title/summary disagrees with source")
        if sorted((tag.code, tag.name) for tag in value.subjects) != sorted(item.tags):
            raise RuntimeError("public tags disagree with source")
    return {
        "revision": revision.revision,
        "publications": revision.publication_count,
        "verified": True,
    }


def audit_sequence(config: CoreConfig) -> list[dict[str, Any]]:
    """Fresh facade, then repeated checks on the same facade: no cold-cache claim."""
    reports = []
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        for ordinal in range(3):
            result, report = measured(admin.check)
            if result.state != "READY":
                raise RuntimeError("full audit did not report READY")
            verify_diagnostic_counters(
                report, required=int(config.logger.level) <= logging.INFO
            )
            report.update(label="fresh_facade" if ordinal == 0 else f"repeat_{ordinal}")
            reports.append(report)
    if len({report["sql_calls"] for report in reports}) != 1:
        raise RuntimeError("unchanged full audits executed different SQL counts")
    return reports


def drain(
    facade: VNextIngestFacade, *, diagnostics_required: bool, allow_quiet: bool = False
) -> dict[str, Any]:
    attempts = []
    for _ in range(256):
        outcome, report = measured(
            lambda: facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
        )
        report["outcome"] = str(outcome)
        verify_diagnostic_counters(
            report, required=diagnostics_required, allow_quiet=allow_quiet
        )
        attempts.append(report)
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            return {"outcome": "DONE", "attempts": attempts}
        if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
            raise RuntimeError(f"uncontended cleanup returned {outcome}")
    raise RuntimeError("cleanup exceeded 256-attempt budget")


def run_case(config: CoreConfig, case: Case, level: str) -> dict[str, Any]:
    config = config.model_copy(
        update={"logger": LoggerConfig.model_validate({"level": level})}
    )
    initialize_database(config)
    source = source_for(case.shape)
    library = MemoryLibrary(source)
    records = []
    with VNextIngestFacade(config) as facade:
        for ordinal in range(1, case.revisions + 1):
            # Change one gallery per revision, retaining all other observations.
            # Final policy change forces materialization through the public API.
            if ordinal > 1 and (ordinal < case.revisions or case.name == "depth"):
                item = source.galleries[0]
                source.put(
                    replace(item, title=f"Revision {ordinal}", comment=item.comment)
                )
            session = claim_session(facade)
            policy = facade.ensure_policy(
                session,
                ingest_policy(
                    artifacts_required=False,
                    spam_occurrence_threshold=7
                    if ordinal == case.revisions
                    and ordinal > 1
                    and case.name != "depth"
                    else 3,
                ),
            )
            source_label = "source.prepare"

            def source_boundary(label: str) -> None:
                nonlocal source_label
                source_label = label

            receipt, source_report = measured(
                lambda: run_source(
                    facade, session, policy, source, boundary=source_boundary
                ),
                label=lambda: source_label,
            )
            analysis, analysis_report = measured(
                lambda: run_analysis(facade, session, policy, receipt.build_id)
            )
            publication, publication_report = measured(
                lambda: run_publication(facade, session, policy, library)
            )
            facade.complete_ingest(session)
            if (
                not analysis.terminal
                or not publication.terminal
                or library.render_calls
            ):
                raise RuntimeError("metadata-only pipeline did not complete")
            before = snapshot(config, source.galleries)
            layout = current_compaction_layout(config)
            if case.name == "depth" and layout.depth != (
                ordinal - 1 if ordinal <= 17 else 0
            ):
                raise RuntimeError(
                    "natural compaction did not follow depth 0..16 then 0"
                )
            audits = audit_sequence(config)
            cleanup = drain(
                facade,
                diagnostics_required=level != "warning",
                allow_quiet=level == "info",
            )
            after = snapshot(config, source.galleries)
            analyses, builds = retained_compaction_roots(config)
            if after != before:
                raise RuntimeError("cleanup changed published facts")
            next_session, claim = measured(
                lambda: facade.try_claim_ingest(True, LEASE_MICROSECONDS)
            )
            if next_session is None:
                raise RuntimeError("next ingest claim after DONE was refused")
            facade.complete_ingest(next_session)
            followup = drain(
                facade,
                diagnostics_required=level != "warning",
                allow_quiet=level == "info",
            )
            records.append(
                {
                    "ordinal": ordinal,
                    "overlay_depth": layout.depth,
                    "retained_analysis_count": len(analyses),
                    "retained_source_build_count": len(builds),
                    "source": source_report,
                    "analysis": analysis_report,
                    "publication": publication_report,
                    "audits": audits,
                    "cleanup": cleanup,
                    "next_claim": {"granted": True, **claim},
                    "post_claim_cleanup": followup,
                    "publication_oracle": after,
                }
            )
            print(
                json.dumps(
                    {
                        "case": case.name,
                        "revision": ordinal,
                        "source_seconds": source_report["seconds"],
                        "audit_seconds": [audit["seconds"] for audit in audits],
                        "cleanup_seconds": sum(
                            row["seconds"] for row in cleanup["attempts"]
                        ),
                    }
                ),
                flush=True,
            )
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        final_ready, final_audit = measured(admin.check)
    if final_ready.state != "READY":
        raise RuntimeError("post-cleanup full READY audit failed")
    verify_diagnostic_counters(final_audit, required=level != "warning")
    return {
        "name": case.name,
        "final_full_ready_audit": final_audit,
        "shape": asdict(case.shape),
        "log_level": level,
        "revisions": records,
        "status": "completed",
    }


def provenance(output: Path) -> dict[str, str]:
    """Preserve the observed source bytes, including uncommitted runtime edits."""
    paths = [
        Path(__file__),
        *sorted((ROOT / "src" / "h2hdb").glob("*.py")),
        ROOT / "tests" / "vnext_pipeline.py",
        ROOT / "tests" / "vnext_fault_harness.py",
        ROOT / "tests" / "compaction_contracts.py",
        ROOT / "scripts" / "ingest_pipeline_probe.py",
        ROOT / "scripts" / "ingest_growth_cleanup_probe.py",
        ROOT / "scripts" / "ingest_maintenance_mariadb.py",
    ]
    source_directory = output.with_suffix(".sources")
    source_directory.mkdir(parents=True, exist_ok=True)
    result = {}
    for path in paths:
        relative = path.relative_to(ROOT)
        content = path.read_bytes()
        destination = source_directory / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        result[str(relative)] = sha256(content).hexdigest()
    return result


def compact_query_texts(value: Any, dictionary: dict[str, str]) -> None:
    """Deduplicate SQL text across measurements without truncating any counts."""
    match value:
        case dict():
            if "sql" in value and "fingerprint" in value:
                sql = value.pop("sql")
                fingerprint = value["fingerprint"]
                if fingerprint in dictionary and dictionary[fingerprint] != sql:
                    raise RuntimeError(
                        "SQL fingerprint collision invalidates report mapping"
                    )
                dictionary[fingerprint] = sql
            for child in value.values():
                compact_query_texts(child, dictionary)
        case list():
            for child in value:
                compact_query_texts(child, dictionary)


def verify_repeat_counts(cases: list[dict[str, Any]]) -> None:
    """Finite repeated-input cost contract, not an asymptotic complexity proof."""
    counts: dict[str, tuple[int, ...]] = {}
    for case in cases:
        current = tuple(
            revision["source"]["sql_calls"] for revision in case["revisions"]
        )
        previous = counts.setdefault(case["name"], current)
        if previous != current:
            raise RuntimeError(
                "identical source fixtures executed different SQL counts"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument(
        "--group",
        choices=(
            "all",
            "baseline",
            "galleries",
            "pages",
            "canonical",
            "retention",
            "depth",
            "tags",
        ),
        default="baseline",
    )
    parser.add_argument(
        "--level", choices=("debug", "info", "warning"), default="debug"
    )
    parser.add_argument("--repeats", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "status": "incomplete",
        "backend": args.backend,
        "source_sha256": provenance(args.output),
        "hypotheses": {
            "source": "For each fixed source shape, repeated fresh runs should execute the same SQL count. Canonical writer calls are measured separately; linearity or a lower constant is a hypothesis, not a presumed universal bound.",
            "audit": "Three full checks of unchanged authority must execute equal SQL counts; an extra repeated validator/query violates this finite cost target. Compare the page/cache boundary scale independently. Each full check creates a fresh canonical cache, so warm server caches do not imply zero queries.",
            "cleanup": "Each publication must reach DONE and admit subsequent ingest. A hard row cap is not a constant SQL-count or wall-time claim.",
        },
        "cost_units": [
            "connector SQL calls",
            "returned Python rows",
            "wall seconds",
            "SQL seconds",
            "transaction seconds",
            "connection seconds",
        ],
        "limitations": [
            "Local synthetic data, not NAS timing or throughput.",
            "Memory adapters omit filesystem/image/archive/storage work.",
            "MemorySource exposes no producer completion markers; repeated source runs reobserve galleries and do not benchmark adapter marker reuse.",
            "Comment input lengths are not encoded source canonical lengths. Metadata framing can move leaf boundaries; this probe does not certify cache occupancy or capacity eviction behavior.",
            "Diagnostic messages use an in-memory sink; remote log transport and disk handler latency are excluded.",
            "Fresh facade is not a cold database/OS cache; setup and public oracles warm caches.",
            "Nested diagnostic durations overlap; only physical observer categories are additive.",
            "All statements retained up to a hard budget; a budget overflow invalidates the run.",
            "No performance target is declared achieved merely because correctness passes.",
        ],
        "sql_texts": {},
        "cases": [],
    }
    write_report(args.output, report)
    try:
        for case in matrix(args.group):
            for repeat in range(args.repeats):
                with tempfile.TemporaryDirectory(
                    prefix="h2hdb-maintenance-investigation-"
                ) as directory:
                    with database(args.backend, Path(directory)) as config:
                        result = run_case(config, case, args.level)
                        result["repeat"] = repeat + 1
                        compact_query_texts(result, report["sql_texts"])
                        report["cases"].append(result)
                        verify_repeat_counts(report["cases"])
                write_report(args.output, report)
    except BaseException as error:
        report.update(status="failed", failure=type(error).__name__)
        write_report(args.output, report)
        raise
    report["status"] = "completed"
    write_report(args.output, report)


if __name__ == "__main__":
    main()
