"""Measure a real sealed ingest pipeline on disposable synthetic databases.

SQLite is the default. MariaDB starts a private 10.11.11 Testcontainer; no
existing database or server is accepted. Each shape/repetition starts empty.
Memory adapters deliberately do not decode images or render real CBZ files.
The cooperative POSIX alarm is not a hard deadline for native calls or teardown.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]

from vnext_fault_harness import (  # noqa: E402 - private backend version probe.
    open_connector,
)
from vnext_pipeline import (  # noqa: E402 - select checkout fixtures.
    MemoryGallery,
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

from h2hdb import (  # noqa: E402 - select checkout runtime.
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb import (  # noqa: E402 - checkout projection observer.
    vnext_publication_candidate_repository as projection,
)
from h2hdb.ingest_performance import (  # noqa: E402 - scoped diagnostic observation.
    IngestPerformance,
    PerformanceStep,
)
from h2hdb.sql_performance import measure_sql  # noqa: E402 - checkout source.
from h2hdb.vnext_identity import effective_content_digest  # noqa: E402 - codec oracle.
from h2hdb.vnext_ingest_analysis import (  # noqa: E402 - observer, not workflow driver.
    VNextIngestAnalysisOrchestrator,
)


@dataclass(frozen=True)
class Shape:
    galleries: int = 2
    pages: int = 1
    tags: int = 0
    metadata_bytes: int = 0

    def __post_init__(self) -> None:
        limits = {
            "galleries": (1, 32),
            "pages": (1, 256),
            "tags": (0, 128),
            "metadata_bytes": (0, 262144),
        }
        for name, (low, high) in limits.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer between {low} and {high}")
        if (
            self.galleries * self.pages > 4096
            or self.galleries * self.metadata_bytes > 4 * 1024 * 1024
        ):
            raise ValueError("shape exceeds 4096 total pages or 4 MiB metadata payload")


def shapes(base: Shape, vary: str) -> list[tuple[str, Shape]]:
    if vary not in {"none", "all", "galleries", "pages", "tags", "metadata_bytes"}:
        raise ValueError("unknown shape dimension")
    changes = {
        "galleries": max(8, base.galleries * 2),
        "pages": max(129, base.pages * 2),
        "tags": max(32, base.tags * 2),
        "metadata_bytes": max(65536, base.metadata_bytes * 2),
    }
    return [
        ("baseline", base),
        *[
            (name, replace(base, **{name: value}))
            for name, value in changes.items()
            if vary in {"all", name}
        ],
    ]


def source_for(shape: Shape) -> MemorySource:
    return MemorySource(
        [
            gallery(
                gid,
                title=f"Probe gallery {gid}",
                pages=[
                    f"gallery-{gid}-page-{page}".encode() for page in range(shape.pages)
                ],
                artists=(),
                language=None,
                extra_tags=[("group", f"tag-{tag}") for tag in range(shape.tags)],
                comment="x" * shape.metadata_bytes,
            )
            for gid in range(1, shape.galleries + 1)
        ]
    )


@dataclass
class Measurement:
    calls: int = 0
    seconds: float = 0.0
    returned_rows: int = 0


class Observer:
    """Observe each SQL event once at its innermost performance scope.

    Internal hooks only observe real calls; all mutations use public facades.
    A surrounding measure_sql receives calls outside facade performance scopes.
    Nested stage summaries are never used as additive measurements.
    """

    def __init__(self) -> None:
        self.phase = "outside"
        self.active: PerformanceStep | None = None
        self.source_scope: tuple[str, str, str] | None = None
        self.failure: Exception | None = None
        self.queries: dict[tuple[str, str, str, str, str], Measurement] = defaultdict(
            Measurement
        )
        self.operations: dict[tuple[str, str, str], Measurement] = defaultdict(
            Measurement
        )
        self.subcalls: dict[tuple[str, str, str, str], Measurement] = defaultdict(
            Measurement
        )
        self.pending: dict[int, dict[tuple[str, str], Measurement]] = {}
        self.omitted_query_events = 0

    def record(
        self,
        sample: PerformanceStep | None,
        category: str,
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        if sample is not None:
            pending = self.pending.setdefault(id(sample), {})
            if len(pending) >= 8192 and (category, query) not in pending:
                raise RuntimeError("SQL observation fingerprint budget exceeded")
            value = pending.setdefault((category, query), Measurement())
        else:
            pipeline, phase, operation = self.source_scope or (
                self.phase,
                "outside",
                "outside",
            )
            key = (pipeline, phase, operation, category, query)
            value = self.query_group(key)
        value.calls += 1
        value.seconds += elapsed
        value.returned_rows += rows

    def query_group(self, key: tuple[str, str, str, str, str]) -> Measurement:
        if key not in self.queries and len(self.queries) >= 8192:
            raise RuntimeError("SQL observation fingerprint budget exceeded")
        return self.queries[key]

    def flush(self, sample: PerformanceStep) -> None:
        for (category, query), recorded in self.pending.pop(id(sample), {}).items():
            value = self.query_group(
                (sample.pipeline, sample.phase, sample.operation, category, query)
            )
            value.calls += recorded.calls
            value.seconds += recorded.seconds
            value.returned_rows += recorded.returned_rows

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        self.observe(None, category, elapsed, query, read_rows)

    def observe(
        self,
        sample: PerformanceStep | None,
        category: str,
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        try:
            self.record(sample, category, elapsed, query, rows)
        except Exception as error:
            self.failure = error

    def wrap_source(self, name: str, action: Callable[..., Any]) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            previous = self.source_scope
            phase = (
                "prepare"
                if name.startswith("prepare")
                else "issue"
                if name.startswith("issue")
                else "commit"
            )
            self.source_scope = ("source", phase, name)
            started = time.perf_counter()
            try:
                return action(*args, **kwargs)
            finally:
                value = self.operations[self.source_scope]
                value.calls += 1
                value.seconds += time.perf_counter() - started
                self.source_scope = previous

        return wrapped

    def wrap_subcall(self, name: str, action: Callable[..., Any]) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            sample = self.active
            key = (
                (sample.pipeline, sample.phase, sample.operation, name)
                if sample is not None
                else (self.phase, "outside", "outside", name)
            )
            started = time.perf_counter()
            try:
                return action(*args, **kwargs)
            finally:
                value = self.subcalls[key]
                value.calls += 1
                value.seconds += time.perf_counter() - started

        return wrapped

    @contextmanager
    def installed(self) -> Iterator[None]:
        original_record = PerformanceStep.record_sql_operation
        original_step = IngestPerformance.step
        observer = self

        def record(
            sample: PerformanceStep,
            category: Literal["sql", "connection", "transaction"],
            elapsed: float,
            query: str,
            rows: int,
        ) -> None:
            observer.observe(sample, category, elapsed, query, rows)
            original_record(sample, category, elapsed, query, rows)

        @contextmanager
        def step(
            owner: IngestPerformance,
            pipeline: str,
            phase: str,
            operation: str,
            generation: int,
        ) -> Iterator[PerformanceStep]:
            with original_step(owner, pipeline, phase, operation, generation) as sample:
                previous = observer.active
                observer.active = sample
                try:
                    yield sample
                finally:
                    own_seconds = max(
                        0.0, sample.elapsed(time.perf_counter()) - sample.nested_seconds
                    )
                    value = observer.operations[
                        (sample.pipeline, sample.phase, sample.operation)
                    ]
                    value.calls += 1
                    value.seconds += own_seconds
                    try:
                        observer.flush(sample)
                    except Exception as error:
                        observer.failure = error
                    observer.active = previous

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(PerformanceStep, "record_sql_operation", record)
            )
            stack.enter_context(patch.object(IngestPerformance, "step", step))
            for name in (
                "prepare_source",
                "issue_source_step",
                "prepare_source_step",
                "commit_source_step",
            ):
                stack.enter_context(
                    patch.object(
                        VNextIngestFacade,
                        name,
                        self.wrap_source(name, getattr(VNextIngestFacade, name)),
                    )
                )
            target = VNextIngestAnalysisOrchestrator
            name = "_prepare_gallery_work"
            stack.enter_context(
                patch.object(
                    target, name, self.wrap_subcall(name, getattr(target, name))
                )
            )
            for name in (
                "_prepare_catalog_plan",
                "_prepare_projection_publication",
                "_assign_projection_order",
                "_populate_projection_facets",
                "_populate_projection_tag_orders",
                "_populate_projection_children",
            ):
                stack.enter_context(
                    patch.object(
                        projection,
                        name,
                        self.wrap_subcall(name, getattr(projection, name)),
                    )
                )
            stack.enter_context(measure_sql(self))
            yield

    def report(self) -> dict[str, Any]:
        if self.failure is not None:
            raise RuntimeError(
                "SQL observer failed; measurements are invalid"
            ) from self.failure
        if self.pending:
            raise RuntimeError("SQL observations remain in incomplete scopes")
        queries = [
            {
                "pipeline": pipeline,
                "phase": phase,
                "operation": operation,
                "category": category,
                "sql": sql,
                "fingerprint": sha256(sql.encode()).hexdigest()[:16],
                **asdict(value),
            }
            for (pipeline, phase, operation, category, sql), value in sorted(
                self.queries.items()
            )
        ]
        return {
            "sql_calls": sum(
                row["calls"] for row in queries if row["category"] == "sql"
            ),
            "sql_seconds": sum(
                row["seconds"] for row in queries if row["category"] == "sql"
            ),
            "returned_rows": sum(
                row["returned_rows"] for row in queries if row["category"] == "sql"
            ),
            "connection_seconds": sum(
                row["seconds"] for row in queries if row["category"] == "connection"
            ),
            "transaction_seconds": sum(
                row["seconds"] for row in queries if row["category"] == "transaction"
            ),
            "omitted_query_events": self.omitted_query_events,
            "queries": queries,
            "operations": [
                {
                    "pipeline": pipeline,
                    "phase": phase,
                    "operation": operation,
                    "calls": value.calls,
                    "exclusive_seconds": value.seconds,
                }
                for (pipeline, phase, operation), value in sorted(
                    self.operations.items()
                )
            ],
            "subcalls": [
                {
                    "pipeline": pipeline,
                    "phase": phase,
                    "operation": operation,
                    "name": name,
                    "calls": value.calls,
                    "inclusive_seconds": value.seconds,
                }
                for (pipeline, phase, operation, name), value in sorted(
                    self.subcalls.items()
                )
            ],
        }


def verify_publication(
    config: CoreConfig, expected: tuple[MemoryGallery, ...]
) -> dict[str, Any]:
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
        page = catalog.discover_publications(revision=revision, limit=128)
        if (
            revision.revision != 1
            or revision.publication_count != len(expected)
            or revision.artifact_count != 0
            or page.next_cursor is not None
        ):
            raise RuntimeError("unexpected fresh publication count/revision/artifacts")
        actual = {item.gid: item for item in page.publications}
        if set(actual) != {item.gid for item in expected}:
            raise RuntimeError("published GID set does not match synthetic source")
        content: dict[str, str] = {}
        summary_bytes: dict[str, int] = {}
        summary_sha256: dict[str, str] = {}
        tag_count: dict[str, int] = {}
        tags_sha256: dict[str, str] = {}
        for item in expected:
            expected_hash = effective_content_digest(
                tuple(
                    sha256(data).digest()
                    for name, data in item.files.items()
                    if name.endswith(b".png")
                )
            ).hex()
            found = actual[item.gid]
            if (found.title, found.content_sha256) != (item.title, expected_hash):
                raise RuntimeError(
                    "published title/content does not match synthetic source"
                )
            if found.summary != item.comment:
                raise RuntimeError("published summary does not match synthetic comment")
            observed_tags = sorted(
                ((subject.code, subject.name) for subject in found.subjects),
                key=lambda value: (value[0] or "", value[1]),
            )
            if observed_tags != sorted(item.tags):
                raise RuntimeError(
                    "published tags do not match synthetic namespace/value pairs"
                )
            key = str(item.gid)
            content[key] = expected_hash
            summary = found.summary.encode("utf-8")
            summary_bytes[key] = len(summary)
            summary_sha256[key] = sha256(summary).hexdigest()
            tag_count[key] = len(observed_tags)
            tags_sha256[key] = sha256(
                json.dumps(
                    observed_tags, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest()
        return {
            "revision": revision.revision,
            "publication_count": revision.publication_count,
            "gids": sorted(actual),
            "titles": {str(item.gid): item.title for item in expected},
            "content_sha256": content,
            "summary_bytes": summary_bytes,
            "summary_sha256": summary_sha256,
            "tag_count": tag_count,
            "tags_sha256": tags_sha256,
        }


def run_case(config: CoreConfig, shape: Shape) -> dict[str, Any]:
    source = source_for(shape)
    library = MemoryLibrary(source)
    setup_started = time.perf_counter()
    with closing(open_connector(config)) as connector:
        row = connector.fetch_one(
            "SELECT sqlite_version()"
            if config.database.sql_type == "sqlite"
            else "SELECT VERSION()"
        )
    server_version = str(row[0])
    if config.database.sql_type == "mariadb" and not server_version.startswith(
        "10.11.11-"
    ):
        raise RuntimeError("private MariaDB server version does not match 10.11.11")
    initialize_database(config)
    observer = Observer()
    phases: dict[str, float] = {}

    def measured[T](phase: str, action: Callable[[], T]) -> T:
        observer.phase = phase
        started = time.perf_counter()
        try:
            return action()
        finally:
            phases[phase] = time.perf_counter() - started

    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        setup_seconds = time.perf_counter() - setup_started
        with observer.installed():
            receipt = measured(
                "source", lambda: run_source(facade, session, policy, source)
            )
            analysis = measured(
                "analysis",
                lambda: run_analysis(facade, session, policy, receipt.build_id),
            )
            publication = measured(
                "publication", lambda: run_publication(facade, session, policy, library)
            )
            measured("complete", lambda: facade.complete_ingest(session))
    if not analysis.terminal or not publication.terminal or library.render_calls:
        raise RuntimeError("pipeline did not finalize metadata-only publication")
    oracle = verify_publication(config, source.galleries)
    audit_started = time.perf_counter()
    if full_check(config).state != "READY":
        raise RuntimeError("full READY audit failed")
    audit_seconds = time.perf_counter() - audit_started
    return {
        "shape": asdict(shape),
        "server_version": server_version,
        "setup_seconds": setup_seconds,
        "phase_seconds": phases,
        "measurements": observer.report(),
        "oracle": oracle,
        "full_ready_audit": "passed",
        "full_ready_audit_seconds": audit_seconds,
    }


@contextmanager
def database(backend: str, root: Path) -> Iterator[CoreConfig]:
    if backend == "sqlite":
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(root / "catalog.sqlite3")
            )
        )
        return
    if backend != "mariadb":
        raise ValueError("unknown backend")
    from testcontainers.community.mysql import MySqlContainer

    container = MySqlContainer(
        image="mariadb:10.11.11",
        username="probe",
        password="synthetic-probe-password",
        root_password="synthetic-root-password",
        dbname="pipeline_probe",
    )
    original_error: BaseException | None = None
    try:
        container.start()
        yield CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb",
                host=container.get_container_host_ip(),
                port=int(container.get_exposed_port(container.port)),
                user="probe",
                password="synthetic-probe-password",
                database="pipeline_probe",
            )
        )
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            container.stop()
        except BaseException as error:
            if original_error is None:
                raise
            original_error.add_note(
                f"Testcontainer cleanup also failed: {type(error).__name__}"
            )


def write_report(output: Path, report: dict[str, Any]) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=".pipeline-report-",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def source_provenance() -> dict[str, Any]:
    files = sorted((ROOT / "src" / "h2hdb").rglob("*.py")) + [
        Path(__file__),
        ROOT / "tests" / "vnext_pipeline.py",
        ROOT / "tests" / "vnext_fault_harness.py",
        ROOT / "pyproject.toml",
    ]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=3,
    ).stdout.strip()
    return {
        "git_head": commit,
        "python": sys.version,
        "source_sha256": {
            str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
            for path in files
        },
        "note": "Hashes bind measured checkout sources, including uncommitted edits; SQLite and driver environment can affect timing.",
    }


def new_report(backend: str) -> dict[str, Any]:
    return {
        "status": "incomplete",
        "stage": "starting",
        "backend": backend,
        "cases": [],
        "scope": "Fresh sealed source/analysis/catalog publication/finalization; artifacts_required=False. Memory pages are not raster images. No image decode, real CBZ, source filesystem scan, cleanup, startup audit scheduling, production history or contention is modeled.",
        "measurement_notes": "Each SQL event is attributed once to its innermost scope. Operation seconds exclude nested performance scopes. Subcall inclusive_seconds overlap SQL, operation and phase times; never add these views. returned_rows are connector results, not DB rows examined. Query SQL has no parameter values; raw SQL fingerprints match existing DEBUG telemetry. Single synchronous thread only, no adapter-worker attribution. Direct temporary SQLite plan I/O is inside plan time but outside core SQL counters. Setup/claim/policy and final correctness audit are outside timed pipeline phases. Instrumentation overhead is included; timings are exploratory, not an SLO verdict. The additional exact observer is active at INFO and its overhead is included; no DEBUG logger is required. GIDs/titles stay fixed across page/tag/comment cases; page changes intentionally change content hashes. Metadata bytes denotes ASCII comment payload, not total serialized observation size.",
        "timeout_notes": "Cooperative POSIX SIGALRM, not a hard process deadline. Native calls and container startup/teardown may delay interruption.",
    }


def execute(
    backend: str,
    cases: list[tuple[str, Shape]],
    repeats: int,
    output: Path,
    report: dict[str, Any],
) -> None:
    if not 1 <= repeats <= 3 or not 1 <= len(cases) <= 5:
        raise ValueError("at most three repetitions and five shapes are allowed")
    write_report(output, report)
    try:
        report["source_provenance"] = source_provenance()
        for name, shape in cases:
            for repetition in range(1, repeats + 1):
                report["stage"] = f"{name}:{repetition}:database_startup_and_pipeline"
                write_report(output, report)
                with tempfile.TemporaryDirectory(prefix="h2hdb-pipeline-") as temporary:
                    with database(backend, Path(temporary)) as config:
                        result = run_case(config, shape)
                        result.update(name=name, repetition=repetition)
                        report["cases"].append(result)
                        report["stage"] = f"{name}:{repetition}:database_teardown"
                        write_report(output, report)
                print(
                    json.dumps(
                        {
                            "name": name,
                            "repetition": repetition,
                            "phase_seconds": result["phase_seconds"],
                            "sql_calls": result["measurements"]["sql_calls"],
                        }
                    ),
                    flush=True,
                )
        report.update(status="completed", stage="measurement_and_teardown_completed")
        write_report(output, report)
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = {
            "stage": report["stage"],
            "reason": type(error).__name__,
            "message": "Original exception re-raised; message omitted to avoid configuration secrets.",
        }
        try:
            write_report(output, report)
        except BaseException as write_error:
            error.add_note(
                f"Saving failure report failed: {type(write_error).__name__}"
            )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--galleries", type=int, default=2)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--tags", type=int, default=0)
    parser.add_argument("--metadata-bytes", type=int, default=0)
    parser.add_argument(
        "--vary",
        choices=("none", "all", "galleries", "pages", "tags", "metadata_bytes"),
        default="none",
    )
    parser.add_argument("--repeats", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--timeout", type=int, choices=range(30, 1801), default=300)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        cases = shapes(
            Shape(args.galleries, args.pages, args.tags, args.metadata_bytes), args.vary
        )
    except ValueError as error:
        parser.error(str(error))
    if not hasattr(signal, "SIGALRM"):
        parser.error("this manual probe requires cooperative POSIX SIGALRM")
    if args.output.exists() or args.output.is_symlink():
        parser.error(
            "--output must be a new file; existing paths and symlinks are refused"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = new_report(args.backend)

    def expired(_signum: int, _frame: object) -> None:
        raise TimeoutError("pipeline probe cooperative alarm expired")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(args.timeout)
    try:
        execute(args.backend, cases, args.repeats, args.output, report)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
