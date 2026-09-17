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
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass, replace
from functools import wraps
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import collect_mariadb_performance as mariadb_performance  # noqa: E402 - dev diagnostics.
from mysql.connector.abstracts import (  # noqa: E402 - driver type.
    MySQLConnectionAbstract,
)
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
    CatalogPublication,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
    catalog_refinement,
)
from h2hdb import (  # noqa: E402 - checkout projection observer.
    vnext_publication_candidate_repository as projection,
)
from h2hdb import (  # noqa: E402 - audit observer.
    vnext_schema_provider as schema_provider,
)
from h2hdb.ingest_performance import (  # noqa: E402 - scoped diagnostic observation.
    IngestPerformance,
    PerformanceStep,
)
from h2hdb.schema_admin import VNextSchemaAdmin  # noqa: E402 - observe public audit.
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
            "galleries": (1, 256),
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
                locator=(f"gallery-{gid:06d}",),
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
    max_seconds: float = 0.0


class Observer:
    """Observe each SQL event once at its innermost performance scope.

    Internal hooks only observe real calls; all mutations use public facades.
    A surrounding measure_sql receives calls outside facade performance scopes.
    Nested stage summaries are never used as additive measurements.
    """

    def __init__(self, query_budget: int = 8192) -> None:
        self.query_budget = query_budget
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
            if len(pending) >= self.query_budget and (category, query) not in pending:
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
        value.max_seconds = max(value.max_seconds, elapsed)

    def query_group(self, key: tuple[str, str, str, str, str]) -> Measurement:
        if key not in self.queries and len(self.queries) >= self.query_budget:
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
            value.max_seconds = max(value.max_seconds, recorded.max_seconds)

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

    def report(self, *, query_limit: int | None = None) -> dict[str, Any]:
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

        def operation_detail(
            key: tuple[str, str, str], value: Measurement
        ) -> dict[str, Any]:
            selected = [
                row
                for row in queries
                if (row["pipeline"], row["phase"], row["operation"]) == key
            ]
            breakdown = {
                category + "_seconds": sum(
                    row["seconds"] for row in selected if row["category"] == category
                )
                for category in ("sql", "connection", "transaction")
            }
            sql = [row for row in selected if row["category"] == "sql"]
            return {
                "pipeline": key[0],
                "phase": key[1],
                "operation": key[2],
                "calls": value.calls,
                "exclusive_seconds": value.seconds,
                **breakdown,
                "non_sql_seconds": max(0.0, value.seconds - sum(breakdown.values())),
                "sql_calls": sum(row["calls"] for row in sql),
                "returned_rows": sum(row["returned_rows"] for row in sql),
                "top_queries": sorted(
                    sql, key=lambda row: row["seconds"], reverse=True
                )[:3],
            }

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
            "query_group_count": len(queries),
            "query_group_budget": self.query_budget,
            "query_details_truncated": query_limit is not None
            and len(queries) > query_limit,
            "queries": queries
            if query_limit is None
            else sorted(queries, key=lambda row: row["seconds"], reverse=True)[
                :query_limit
            ],
            "operations": [
                operation_detail(key, value)
                for key, value in sorted(self.operations.items())
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


class AuditObserver(Observer):
    """Time the real wheel-owned validators without changing audit decisions."""

    def __init__(self, progress: Callable[[str], None] | None = None) -> None:
        super().__init__(query_budget=32768)
        self.phase = "ready_audit"
        self.children: list[float] = []
        self.progress = progress
        self.cache_key_budget = 65536
        self.cache_keys: set[tuple[bytes, bytes]] = set()
        self.cache_values: dict[tuple[bytes, bytes], int] = {}
        self.cache_metrics: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

    def cache_key(self, digest: bytes, domain: bytes) -> tuple[bytes, bytes]:
        key = (digest, domain)
        if key not in self.cache_keys and len(self.cache_keys) >= self.cache_key_budget:
            raise RuntimeError("canonical cache observation key budget exceeded")
        self.cache_keys.add(key)
        return key

    def cache_wrappers(self) -> tuple[Callable[..., Any], Callable[..., Any]]:
        owner = catalog_refinement._CanonicalValidationCache
        original_open, original_remember = owner.open, owner.remember

        def measured_open(
            cache: Any, digest: bytes, domain: bytes
        ) -> tuple[Any, int] | None:
            self.cache_key(digest, domain)
            result = original_open(cache, digest, domain)
            metric = self.cache_metrics[domain.decode("ascii")]
            metric["hits" if result is not None else "misses"] += 1
            return result

        def measured_remember(
            cache: Any, digest: bytes, domain: bytes, spool: Any, *, byte_count: int
        ) -> None:
            key = self.cache_key(digest, domain)
            previous_entries = len(cache._values)
            already_resident = key in cache._values
            original_remember(cache, digest, domain, spool, byte_count=byte_count)
            self.cache_values[key] = byte_count
            metric = self.cache_metrics[domain.decode("ascii")]
            admitted = key in cache._values
            metric["admissions" if admitted else "oversize_bypasses"] += 1
            if admitted:
                metric["evictions_caused"] += (
                    previous_entries + int(not already_resident) - len(cache._values)
                )
            metric["max_resident_entries"] = max(
                metric["max_resident_entries"], len(cache._values)
            )
            metric["max_resident_bytes"] = max(
                metric["max_resident_bytes"], cache._byte_count
            )
            metric["max_resident_charged_bytes"] = max(
                metric["max_resident_charged_bytes"], cache._charged_byte_count
            )

        return measured_open, measured_remember

    @contextmanager
    def scope(self, name: str) -> Iterator[None]:
        previous = self.source_scope
        key = ("ready_audit", "audit", name)
        self.source_scope = key
        self.children.append(0.0)
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - started
            child_seconds = self.children.pop()
            measured = self.operations[key]
            measured.calls += 1
            measured.seconds += max(0.0, elapsed - child_seconds)
            self.source_scope = previous
            if self.children:
                self.children[-1] += elapsed
            if self.progress is not None:
                self.progress(name)

    def wrap(self, name: str, action: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(action)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            with self.scope(name):
                return action(*args, **kwargs)

        return wrapped

    @contextmanager
    def installed(self) -> Iterator[None]:
        original_load = schema_provider._load_builtin_semantic_validators

        def registry() -> Mapping[str, Callable[..., Any]]:
            # Preserve the actual wheel registry IDs and callbacks; the wrappers
            # only delimit measurements around each original validator call.
            return MappingProxyType(
                {
                    key: self.wrap("semantic:" + key, validator)
                    for key, validator in original_load().items()
                }
            )

        with ExitStack() as stack:
            cache_open, cache_remember = self.cache_wrappers()
            for name, callback in (("open", cache_open), ("remember", cache_remember)):
                stack.enter_context(
                    patch.object(
                        catalog_refinement._CanonicalValidationCache, name, callback
                    )
                )
            stack.enter_context(
                patch.object(
                    schema_provider, "_load_builtin_semantic_validators", registry
                )
            )
            for owner, name, label in (
                (VNextSchemaAdmin, "_resolve_provider", "provider_resolution"),
                (
                    schema_provider.GeneratedVNextSchemaProvider,
                    "validate_global",
                    "schema_structure",
                ),
                (
                    schema_provider.GeneratedVNextSchemaProvider,
                    "validate_bootstrap_seeds",
                    "bootstrap_seeds",
                ),
                (
                    schema_provider.GeneratedVNextSchemaProvider,
                    "validate_semantics",
                    "semantic_dispatch",
                ),
            ):
                stack.enter_context(
                    patch.object(owner, name, self.wrap(label, getattr(owner, name)))
                )
            stack.enter_context(measure_sql(self))
            yield

    def finish(self, seconds: float) -> dict[str, Any]:
        outside = self.operations[("ready_audit", "outside", "outside")]
        outside.calls = 1
        outside.seconds = max(
            0.0, seconds - sum(value.seconds for value in self.operations.values())
        )
        result = self.report(query_limit=64)
        result["seconds"] = seconds
        result["canonical_cache"] = {
            "observed_distinct_requested_keys": len(self.cache_keys),
            "observed_distinct_validated_keys": len(self.cache_values),
            "observed_distinct_validated_bytes": sum(self.cache_values.values()),
            "observation_key_budget": self.cache_key_budget,
            "runtime_max_entries": cache_entry_capacity(),
            "runtime_entry_charge_bytes": catalog_refinement._CANONICAL_VALIDATION_CACHE_ENTRY_BYTES,
            "runtime_max_value_bytes": catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES,
            "runtime_max_total_bytes": catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES,
            "domains": dict(self.cache_metrics),
            "notes": "Read-only observation of original cache decisions during this full READY audit. Distinct identity is (canonical digest, domain), shared across cache instances; bytes count each successfully validated identity once, including values too large for admission. Total runtime budget includes payload and the fixed per-entry bookkeeping charge; runtime_max_entries is the derived upper bound for empty values, not an independent entry cap. Charged bytes are logical accounting, not process RSS. Domain eviction counts name the insertion causing eviction, not the evicted domain. Resident maxima cover the whole cache at each domain's insertion and must not be added. Instrumentation does not persist cached values or alter admission, LRU order, capacity or validation decisions.",
        }
        result["notes"] = (
            "SQL is attributed once to the innermost active validator. Operation seconds are exclusive of nested validator scopes. Provider resolution and schema structure are separate; outside covers control admission, closed-world inventories and transaction handling. Full READY does not execute BUILDING-only bootstrap validators. Top 64 query details are retained; all event counts and operation totals are complete. These timings include measurement overhead and follow hot pipeline/oracle reads, not a cold startup audit."
        )
        return result


def verify_publication(
    config: CoreConfig, expected: tuple[MemoryGallery, ...]
) -> dict[str, Any]:
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
        actual: dict[int, CatalogPublication] = {}
        cursor = None
        # Public keyset reads retain the real 128-publication page cap.
        for _ in range((len(expected) + 127) // 128 + 1):
            page = catalog.discover_publications(
                revision=revision, limit=128, after=cursor
            )
            for publication in page.publications:
                if publication.gid in actual:
                    raise RuntimeError(
                        "public catalog repeated a publication across pages"
                    )
                actual[publication.gid] = publication
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        else:
            raise RuntimeError("public catalog exceeded bounded oracle pagination")
        if (
            revision.revision != 1
            or revision.publication_count != len(expected)
            or revision.artifact_count != 0
            or len(actual) != len(expected)
        ):
            raise RuntimeError("unexpected fresh publication count/revision/artifacts")
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


class ServerDiagnostics:
    """Separate read-only connection; snapshots are outside phase wall timings."""

    def __init__(self, connection: MySQLConnectionAbstract, schema: str) -> None:
        self.connection = connection
        self.schema = schema
        self.phases: dict[str, dict[str, Any]] = {}

    def start(self, name: str) -> None:
        self.phases[name] = {
            "before": mariadb_performance.snapshot(self.connection, self.schema)
        }

    def finish(self, name: str) -> None:
        row = self.phases[name]
        row["after"] = mariadb_performance.snapshot(self.connection, self.schema)
        row["differences"] = mariadb_performance.differences(
            row["before"], row["after"]
        )

    def finish_preserving(self, name: str, failure: BaseException | None) -> None:
        try:
            self.finish(name)
        except BaseException as error:
            if failure is None:
                raise
            failure.add_note(
                f"Post-phase diagnostics also failed: {type(error).__name__}"
            )

    def report(self) -> dict[str, Any]:
        return {
            "phases": self.phases,
            "snapshot_seconds": sum(
                row[side]["snapshot_seconds"]
                for row in self.phases.values()
                for side in ("before", "after")
                if side in row
            ),
            "notes": "Private MariaDB only, performance_schema explicitly enabled at container startup. Snapshots use a separate read-only root session with no default schema and are outside client phase wall timers and core SQL counters. Global status still includes diagnostic traffic. Server SUM_TIMER_WAIT is elapsed statement time, not CPU, and statements can overlap across connections. Its difference from client time is not pure network latency. Server digests and raw client SHA fingerprints are different identities; inspect normalized SQL before matching. Complete-counter flags and limitations_detected must be checked before interpreting deltas.",
        }


@contextmanager
def diagnostic_session(
    config: CoreConfig, enabled: bool
) -> Iterator[ServerDiagnostics | None]:
    if not enabled:
        yield None
        return
    if config.database.sql_type != "mariadb":
        raise ValueError("server diagnostics require a private MariaDB fixture")
    admin = config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"user": "root", "password": "synthetic-root-password"}
            )
        }
    )
    with closing(mariadb_performance.open_connection(admin)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        yield ServerDiagnostics(connection, config.database.database)


def run_case(
    config: CoreConfig,
    shape: Shape,
    *,
    progress: Callable[[str, dict[str, float]], None] | None = None,
    diagnostics: ServerDiagnostics | None = None,
    audit_cache_control: bool = False,
) -> dict[str, Any]:
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
    last_progress = 0.0
    active_phase = "setup"
    active_started = setup_started

    def pulse(label: str, *, force: bool = False) -> None:
        nonlocal last_progress
        now = time.perf_counter()
        if progress is not None and (force or now - last_progress >= 15):
            progress(
                active_phase + ":" + label,
                {**phases, "active_elapsed": now - active_started},
            )
            last_progress = now

    def measured[T](phase: str, action: Callable[[], T]) -> T:
        nonlocal active_phase, active_started
        observer.phase = phase
        active_phase = phase
        if diagnostics is not None:
            diagnostics.start(phase)
        active_started = time.perf_counter()
        pulse("started", force=True)
        started = active_started = time.perf_counter()
        failure: BaseException | None = None
        try:
            return action()
        except BaseException as error:
            failure = error
            raise
        finally:
            phases[phase] = time.perf_counter() - started
            pulse("finished", force=True)
            if diagnostics is not None:
                diagnostics.finish_preserving(phase, failure)

    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        setup_seconds = time.perf_counter() - setup_started
        with observer.installed():
            receipt = measured(
                "source",
                lambda: run_source(facade, session, policy, source, boundary=pulse),
            )
            analysis = measured(
                "analysis",
                lambda: run_analysis(
                    facade, session, policy, receipt.build_id, boundary=pulse
                ),
            )
            publication = measured(
                "publication",
                lambda: run_publication(
                    facade, session, policy, library, boundary=pulse
                ),
            )
            measured("complete", lambda: facade.complete_ingest(session))
    if not analysis.terminal or not publication.terminal or library.render_calls:
        raise RuntimeError("pipeline did not finalize metadata-only publication")
    oracle = verify_publication(config, source.galleries)

    def measured_audit(label: str) -> dict[str, Any]:
        nonlocal active_phase, active_started
        audit = AuditObserver(pulse)
        active_phase = label
        if diagnostics is not None:
            diagnostics.start(label)
        active_started = time.perf_counter()
        pulse("started", force=True)
        audit_started = active_started = time.perf_counter()
        audit_failure: BaseException | None = None
        try:
            with audit.installed():
                if full_check(config).state != "READY":
                    raise RuntimeError("full READY audit failed")
        except BaseException as error:
            audit_failure = error
            raise
        finally:
            audit_seconds = time.perf_counter() - audit_started
            if diagnostics is not None:
                diagnostics.finish_preserving(label, audit_failure)
        pulse("finished", force=True)
        return audit.finish(audit_seconds)

    audit_report = measured_audit("ready_audit")
    controls = (
        audit_cache_comparison(
            config, source.galleries, oracle, audit_report, measured_audit
        )
        if audit_cache_control
        else None
    )
    return {
        "shape": asdict(shape),
        "server_version": server_version,
        "setup_seconds": setup_seconds,
        "phase_seconds": phases,
        "measurements": observer.report(),
        "oracle": oracle,
        "full_ready_audit": "passed",
        "full_ready_audit_seconds": audit_report["seconds"],
        "full_ready_audit_measurements": audit_report,
        "audit_cache_comparison": controls,
        "server_diagnostics": diagnostics.report() if diagnostics is not None else None,
    }


def cache_entry_capacity() -> int:
    """Derive the empty-value entry bound from the shared charged budget."""
    return (
        catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES
        // catalog_refinement._CANONICAL_VALIDATION_CACHE_ENTRY_BYTES
    )


def audit_cache_comparison(
    config: CoreConfig,
    expected: tuple[MemoryGallery, ...],
    oracle: dict[str, Any],
    baseline: dict[str, Any],
    measure: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    """A/B/A full audits on the same private, already-published fixture."""
    original_charge = catalog_refinement._CANONICAL_VALIDATION_CACHE_ENTRY_BYTES
    original = cache_entry_capacity()
    control_charge = (
        catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES // 512
    )

    def validators(report: dict[str, Any]) -> dict[str, int]:
        return {
            row["operation"]: row["calls"]
            for row in report["operations"]
            if row["operation"].startswith("semantic:")
        }

    expected_validators = validators(baseline)
    audits = [
        {
            "label": "A_baseline",
            "entry_capacity": original,
            "entry_charge_bytes": original_charge,
            "measurements": baseline,
        }
    ]
    for label, charge in (
        ("B_capacity512", control_charge),
        ("A_restored", original_charge),
    ):
        if verify_publication(config, expected) != oracle:
            raise RuntimeError("cache control changed the published oracle")
        with patch.object(
            catalog_refinement, "_CANONICAL_VALIDATION_CACHE_ENTRY_BYTES", charge
        ):
            capacity = cache_entry_capacity()
            report = measure("ready_audit_" + label)
        if validators(report) != expected_validators:
            raise RuntimeError("cache control changed the full READY validator set")
        audits.append(
            {
                "label": label,
                "entry_capacity": capacity,
                "entry_charge_bytes": charge,
                "measurements": report,
            }
        )
    if catalog_refinement._CANONICAL_VALIDATION_CACHE_ENTRY_BYTES != original_charge:
        raise RuntimeError("cache control failed to restore the entry charge")
    if verify_publication(config, expected) != oracle:
        raise RuntimeError("cache control changed the published oracle")
    return {
        "order": [audit["label"] for audit in audits],
        "same_published_oracle": True,
        "same_ready_validator_calls": expected_validators,
        "restored_entry_capacity": original,
        "audits": audits,
        "notes": "Counterfactual dev-only bookkeeping-charge patch on one private published database. Entry capacities are derived empty-value upper bounds; nonempty values reduce actual capacity. All original validators run; per-value and total charged byte budgets remain unchanged. Each full audit creates fresh snapshot-local caches; repeated order can still warm database/file-system caches. Oracle reads and server snapshots are outside each audit timer. No database facts are modified by this experiment.",
    }


@contextmanager
def database(
    backend: str, root: Path, *, diagnostics: bool = False
) -> Iterator[CoreConfig]:
    if diagnostics and backend != "mariadb":
        raise ValueError("server diagnostics require MariaDB")
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
    if diagnostics:
        container.with_command(
            "--performance-schema=ON --performance-schema-digests-size=10000"
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
        Path(__file__).resolve(),
        ROOT / "tests" / "vnext_pipeline.py",
        ROOT / "tests" / "vnext_fault_harness.py",
        ROOT / "pyproject.toml",
        ROOT / "scripts" / "collect_mariadb_performance.py",
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
        "measurement_notes": "Each SQL event is attributed once to its innermost scope. Operation seconds exclude nested performance scopes. Subcall inclusive_seconds overlap SQL, operation and phase times; never add these views. returned_rows are connector results, not DB rows examined. Query SQL has no parameter values; raw SQL fingerprints match existing DEBUG telemetry. Single synchronous thread only, no adapter-worker attribution. Direct temporary SQLite plan I/O is inside plan time but outside core SQL counters. Setup/claim/policy and final correctness audit are outside timed pipeline phases. Instrumentation overhead is included; timings are exploratory, not an SLO verdict. The additional exact observer is active at INFO and its overhead is included; no DEBUG logger is required. GIDs/titles stay fixed across page/tag/comment cases; page changes intentionally change content hashes. Metadata bytes denotes ASCII comment payload, not total serialized observation size. non_sql_seconds subtracts query/connection/transaction measurements from exclusive operation time; it includes Python, local I/O and observer overhead, not a CPU-only metric. Hard caps: 256 galleries, 4096 total pages, 32768 total tags, and 4 MiB total comments. This tool does not assume corpus timing is linear in gallery count.",
        "timeout_notes": "Cooperative POSIX SIGALRM, not a hard process deadline. Native calls and container startup/teardown may delay interruption.",
    }


def execute(
    backend: str,
    cases: list[tuple[str, Shape]],
    repeats: int,
    output: Path,
    report: dict[str, Any],
    *,
    diagnostics: bool = False,
    audit_cache_control: bool = False,
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
                    with database(
                        backend, Path(temporary), diagnostics=diagnostics
                    ) as config:

                        def progress(stage: str, seconds: dict[str, float]) -> None:
                            report["stage"] = f"{name}:{repetition}:{stage}"
                            report["current_case"] = {
                                "name": name,
                                "repetition": repetition,
                                "shape": asdict(shape),
                                "phase_seconds": seconds,
                            }
                            write_report(output, report)

                        diagnostic_started = time.perf_counter()
                        with diagnostic_session(config, diagnostics) as server:
                            diagnostic_setup_seconds = (
                                time.perf_counter() - diagnostic_started
                            )
                            result = run_case(
                                config,
                                shape,
                                progress=progress,
                                diagnostics=server,
                                audit_cache_control=audit_cache_control,
                            )
                        result["diagnostic_setup_seconds"] = (
                            diagnostic_setup_seconds if diagnostics else 0.0
                        )
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
        report.pop("current_case", None)
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
    parser.add_argument(
        "--mariadb-diagnostics",
        action="store_true",
        help="enable performance_schema only in the private MariaDB fixture and collect phase snapshots",
    )
    parser.add_argument("--galleries", type=int, default=2)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--tags", type=int, default=0)
    parser.add_argument("--metadata-bytes", type=int, default=0)
    parser.add_argument(
        "--audit-cache-control",
        action="store_true",
        help="Private fixture only: compare original/512/original derived cache entry ceilings by varying bookkeeping charge; byte budgets stay fixed",
    )
    parser.add_argument(
        "--vary",
        choices=("none", "all", "galleries", "pages", "tags", "metadata_bytes"),
        default="none",
    )
    parser.add_argument("--repeats", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--timeout", type=int, choices=range(30, 1801), default=300)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.mariadb_diagnostics and args.backend != "mariadb":
        parser.error("--mariadb-diagnostics requires --backend mariadb")
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
        execute(
            args.backend,
            cases,
            args.repeats,
            args.output,
            report,
            diagnostics=args.mariadb_diagnostics,
            audit_cache_control=args.audit_cache_control,
        )
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
