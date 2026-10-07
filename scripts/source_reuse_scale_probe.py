"""Public marker-reuse workloads for source-selection scale experiments.

The seed contains both published observations and unpublished marker-backed
observations. Each measured turn starts a fresh generation and uses ordinary
prepare_source, never the sealed-source resume shortcut. No graph rows are
invented here; the separate owned snapshot helper preserves FK enforcement.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import shutil
import sys
import time
import traceback
from collections import Counter
from collections.abc import Sequence
from contextlib import closing, nullcontext
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("H2HDB_SOURCE_PROBE_RUNTIME_ROOT", str(ROOT)))
sys.path[:0] = [str(RUNTIME_ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from public_fixture_snapshot import (  # noqa: E402 - explicit development inputs.
    clone_public_seed,
    owned_database,
    seal_public_seed,
)
from test_vnext_source_marker import (  # noqa: E402 - public adapter fixture.
    MarkerSource,
)
from vnext_pipeline import (  # noqa: E402 - explicit public workflow fixture.
    Clock,
    MemoryGallery,
    MemoryLibrary,
    SessionOwner,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
)
from vnext_source_staging_cost import (  # noqa: E402 - fixed existing native budget.
    HANDLER_LIMITS,
    SQLITE_VM_LIMIT,
    WINDOW,
    SelectionWork,
    budget_failures,
    observe_pending_queries,
    unbounded_pending_control,
)
from vnext_test_database import database_connector  # noqa: E402 - independent oracle.

import h2hdb  # noqa: E402 - actual loaded runtime provenance.
from h2hdb import (  # noqa: E402 - selected runtime.
    CoreConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
    VNextIngestPage,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
)
from h2hdb.sql_performance import SQLCounters, measure_sql  # noqa: E402
from h2hdb.vnext_identity import encode_source_relative_locator  # noqa: E402

RESERVE_BYTES = 32 * 1024**3
LEASE_MICROSECONDS = 600_000_000


@dataclass(frozen=True)
class ReuseShape:
    """Actual selected counts, separate from inventory or pre-existing history."""

    galleries: int
    published: int
    files: int
    changed: int = 4

    def __post_init__(self) -> None:
        if not 0 < self.published < self.galleries:
            raise ValueError("seed needs both published and unpublished galleries")
        if not 0 < self.changed <= self.published:
            raise ValueError("changed galleries must lie within the published baseline")
        if self.files < 2 * self.galleries:
            raise ValueError("each gallery needs metadata and at least one PAGE")

    @property
    def file_counts(self) -> tuple[int, ...]:
        """Balanced controlled density, explicitly not an observed histogram."""
        quotient, remainder = divmod(self.files, self.galleries)
        return tuple(quotient + (index < remainder) for index in range(self.galleries))

    @property
    def steps(self) -> int:
        # A finite protocol guard, not a performance acceptance allowance.
        return 128 + 128 * self.galleries + 8 * self.files

    def values(self, *, changed: bool = False) -> tuple[MemoryGallery, ...]:
        values = []
        for index, count in enumerate(self.file_counts):
            gid = index + 1
            value = gallery(
                gid,
                title=f"Reuse gallery {gid}",
                locator=(f"gallery-{gid:08d}",),
                pages=[f"page-{gid}-{page}".encode() for page in range(count - 1)],
                artists=(),
                language=None,
            )
            if changed and index < self.changed:
                value = replace(
                    value,
                    modified_time=value.modified_time + 1,
                    files={**value.files, b"000.png": f"changed-{gid}".encode()},
                )
            values.append(value)
        return tuple(values)


class IndexedMarkerSource(MarkerSource):
    """The same public marker contract with one sorted locator index per scan."""

    def __init__(self, galleries: Sequence[MemoryGallery]) -> None:
        self._keys: tuple[tuple[str, ...], ...] | None = None
        self._encoded: tuple[bytes, ...] = ()
        super().__init__(galleries)

    def put(self, value: MemoryGallery) -> None:
        super().put(value)
        self._keys = None

    def remove(self, locator: tuple[str, ...]) -> None:
        super().remove(locator)
        self._keys = None

    def list_gallery_locators(
        self, *, after_locator: tuple[str, ...] | None, limit: int
    ) -> VNextIngestPage[tuple[str, ...]]:
        self.page_calls += 1
        if self._keys is None:
            self._keys = tuple(
                sorted(self._galleries, key=encode_source_relative_locator)
            )
            self._encoded = tuple(map(encode_source_relative_locator, self._keys))
        offset = (
            0
            if after_locator is None
            else bisect.bisect_right(
                self._encoded, encode_source_relative_locator(after_locator)
            )
        )
        items = self._keys[offset : offset + limit]
        terminal = offset + len(items) == len(self._keys)
        return VNextIngestPage(items, None if terminal else items[-1], terminal)


@dataclass
class SourceCounters:
    """Inclusive whole-source SQL plus a separate complete STAGING_FIND issue."""

    whole: SQLCounters = field(default_factory=SQLCounters)
    find: SQLCounters = field(default_factory=SQLCounters)
    actions: Counter[str] = field(default_factory=Counter)
    native_find: SelectionWork = field(default_factory=SelectionWork)
    prepared_counts: dict[str, int] = field(default_factory=dict)
    native_issue_ordinals: list[int] = field(default_factory=list)
    native_issues: list[dict[str, Any]] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        read_rows: int,
    ) -> None:
        del query
        counters = self.whole
        if category == "sql":
            counters.sql_calls += 1
            counters.sql_seconds += elapsed
            counters.read_rows += read_rows
        elif category == "connection":
            counters.connection_calls += 1
            counters.connection_seconds += elapsed
        else:
            counters.transaction_calls += 1
            counters.transaction_seconds += elapsed

    def add_native(self, value: SelectionWork) -> None:
        self.native_find.samples.extend(value.samples)
        self.native_find.connections += value.connections
        self.native_find.source_sqlite_vm_steps += value.source_sqlite_vm_steps
        for name, count in value.source_handler_reads.items():
            prior = self.native_find.source_handler_reads.get(name, 0)
            self.native_find.source_handler_reads[name] = prior + count


def check_resources() -> None:
    if shutil.disk_usage(ROOT).free < RESERVE_BYTES:
        raise RuntimeError("local synthetic probe disk reserve reached")


def _owner(facade: VNextIngestFacade, clock: Clock) -> SessionOwner:
    return SessionOwner(
        facade,
        claim_session(facade, lease=LEASE_MICROSECONDS),
        LEASE_MICROSECONDS,
        clock,
    )


def drive_source(
    facade: VNextIngestFacade,
    owner: SessionOwner,
    policy: VNextResolvedIngestPolicy,
    source: IndexedMarkerSource,
    shape: ReuseShape,
    *,
    backend: str,
    counters: SourceCounters,
    native: bool,
    collect_only: bool = False,
    max_new_galleries: int | None = None,
    native_find_ordinals: frozenset[int] | None = None,
    deadline: float,
) -> VNextIngestSourceReceipt | None:
    """Observe private action labels only; public calls retain all authority."""
    with facade.prepare_source(
        source, policy=policy, max_new_galleries=max_new_galleries
    ) as prepared:
        for step in range(shape.steps):
            if step % 128 == 0 and time.monotonic() > deadline:
                raise TimeoutError("source protocol deadline exceeded")
            if collect_only and prepared.observation_complete:
                return None
            owner.heartbeat()
            action = prepared._machine.action.value
            issue_sql = SourceCounters()
            find = action == "STAGING_FIND"
            find_ordinal = counters.actions["STAGING_FIND"]
            native_sample = (
                native
                and find
                and (
                    native_find_ordinals is None or find_ordinal in native_find_ordinals
                )
            )
            with (
                measure_sql(issue_sql, observe_nested=True)
                if native and find
                else nullcontext(),
                observe_pending_queries(backend)
                if native_sample
                else nullcontext(None) as work,
            ):
                issued = facade.issue_source_step(owner.current, policy, prepared)
            assert issued._action.value == action
            if native and find:
                counters.find.add(issue_sql.whole)
                if native_sample:
                    assert work is not None
                    counters.add_native(work)
                    counters.native_issue_ordinals.append(find_ordinal)
                    counters.native_issues.append(
                        {
                            "issue_ordinal": find_ordinal,
                            "issue_sql": asdict(issue_sql.whole),
                            "issue_native": asdict(work),
                        }
                    )
            local = facade.prepare_source_step(prepared, issued)
            owner.heartbeat()
            result = facade.commit_source_step(owner.current, local)
            counters.actions[action] += 1
            if result.terminal:
                receipt = result.source_receipt
                if collect_only or receipt is None or not receipt.sealed:
                    raise AssertionError(
                        "source did not reach the requested public boundary"
                    )
                counters.prepared_counts = {
                    "selected_galleries": prepared.gallery_count,
                    "deferred_galleries": prepared.deferred_gallery_count,
                    "waiting_galleries": prepared.waiting_gallery_count,
                }
                return receipt
    raise RuntimeError("source protocol step guard exhausted")


def graph_facts(config: CoreConfig) -> dict[str, int]:
    """Independent native counts; never used as authority by the workflow."""
    relations = {
        "observations": "catalog_gallery_observations",
        "marker_bindings": "catalog_gallery_observation_completion_marker",
        "file_facts": "catalog_gallery_observation_file_anchors",
        "collection_observations": "catalog_source_collection_observations",
    }
    with database_connector(config) as connector, connector.read_transaction():
        counts = {
            label: int(connector.fetch_one(f"SELECT COUNT(*) FROM {relation}")[0])
            for label, relation in relations.items()
        }
    with closing(VNextCatalogFacade(config)) as catalog:
        counts["publications"] = catalog.get_catalog_revision().publication_count
    return counts


def build_public_seed(
    config: CoreConfig, shape: ReuseShape, *, deadline_seconds: float = 3600
) -> dict[str, Any]:
    """Publish P, then durably observe all G before deliberately ending that turn."""
    check_resources()
    started = time.perf_counter()
    deadline = time.monotonic() + deadline_seconds
    initialize_database(config)
    values = shape.values()
    clock = Clock()
    published_source = IndexedMarkerSource(values[: shape.published])
    library = MemoryLibrary(published_source)
    with VNextIngestFacade(config, clock=clock) as facade:
        owner = _owner(facade, clock)
        policy = facade.ensure_policy(
            owner.current, ingest_policy(artifacts_required=False)
        )
        first = drive_source(
            facade,
            owner,
            policy,
            published_source,
            shape,
            backend=config.database.sql_type,
            counters=SourceCounters(),
            native=False,
            deadline=deadline,
        )
        assert first is not None and first.staged_galleries == shape.published
        later_steps = max(10_000, 2048 * shape.galleries + 8 * shape.files)
        run_analysis(facade, owner, policy, first.build_id, step_budget=later_steps)
        run_publication(facade, owner, policy, library, step_budget=later_steps)
        owner.heartbeat()
        facade.complete_ingest(owner.current)
    assert full_check(config).state == "READY"
    published_seconds = time.perf_counter() - started
    cached_source = IndexedMarkerSource(values)
    cached_source.forbidden_reads.update(
        value.locator for value in values[: shape.published]
    )
    cache_counters = SourceCounters()
    with VNextIngestFacade(config, clock=clock) as facade:
        owner = _owner(facade, clock)
        policy = facade.ensure_policy(
            owner.current, ingest_policy(artifacts_required=False)
        )
        drive_source(
            facade,
            owner,
            policy,
            cached_source,
            shape,
            backend=config.database.sql_type,
            counters=cache_counters,
            native=False,
            collect_only=True,
            deadline=deadline,
        )
        owner.heartbeat()
        facade.complete_ingest(owner.current)
    assert len(cached_source.deep_reads) == shape.galleries - shape.published
    assert full_check(config).state == "READY"
    facts = graph_facts(config)
    assert facts["publications"] == shape.published
    assert facts["observations"] == facts["marker_bindings"] == shape.galleries
    assert facts["file_facts"] == shape.files
    return {
        "shape": asdict(shape),
        "facts": facts,
        "READY": "READY",
        "published_seed_seconds": published_seconds,
        "total_seed_seconds": time.perf_counter() - started,
        "cached_deep_reads": len(cached_source.deep_reads),
        "cache_actions": dict(cache_counters.actions),
        "file_count_distribution": dict(Counter(shape.file_counts)),
        "shape_limits": "Balanced neutral-byte FILE density; not measured NAS per-gallery distribution, image cost, full inventory size, or historical depth.",
    }


def run_measured_source(
    config: CoreConfig,
    shape: ReuseShape,
    *,
    native: bool,
    degraded: bool = False,
    max_new_galleries: int | None = None,
    native_find_ordinals: frozenset[int] | None = None,
    deadline_seconds: float = 3600,
) -> dict[str, Any]:
    """Run a fresh generation from a public seed and validate outside its timer."""
    check_resources()
    before = graph_facts(config)
    admitted = min(
        shape.galleries - shape.published,
        max_new_galleries if max_new_galleries is not None else shape.galleries,
    )
    selected = shape.published + admitted
    values = shape.values(changed=True)
    source = IndexedMarkerSource(values)
    source.forbidden_reads.update(value.locator for value in values[shape.changed :])
    counters = SourceCounters()
    clock = Clock()
    with VNextIngestFacade(config, clock=clock) as facade:
        owner = _owner(facade, clock)
        generation = owner.current.ingest_generation
        policy = facade.ensure_policy(
            owner.current, ingest_policy(artifacts_required=False)
        )
        started = time.perf_counter()
        with (
            measure_sql(counters, observe_nested=True) if native else nullcontext(),
            unbounded_pending_control() if degraded else nullcontext(),
        ):
            receipt = drive_source(
                facade,
                owner,
                policy,
                source,
                shape,
                backend=config.database.sql_type,
                counters=counters,
                native=native,
                max_new_galleries=max_new_galleries,
                native_find_ordinals=native_find_ordinals,
                deadline=time.monotonic() + deadline_seconds,
            )
        source_seconds = time.perf_counter() - started
        assert receipt is not None
        assert receipt.discovered_galleries == receipt.staged_galleries == selected
        assert counters.actions["STAGING_FIND"] >= selected
        assert len(source.deep_reads) == shape.changed
        owner.heartbeat()
        facade.complete_ingest(owner.current)
    validation_started = time.perf_counter()
    assert full_check(config).state == "READY"
    after = graph_facts(config)
    with database_connector(config) as connector, connector.read_transaction():
        selected_files = int(
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_source_build_galleries AS member "
                "JOIN catalog_gallery_observation_file_anchors AS file "
                "ON file.gallery_id = member.gallery_id "
                "AND file.observation_id = member.observation_id "
                "WHERE member.build_id = %s",
                (receipt.build_id,),
            )[0]
        )
    assert counters.prepared_counts == {
        "selected_galleries": selected,
        "deferred_galleries": shape.galleries - selected,
        "waiting_galleries": 0,
    }
    assert selected_files == sum(shape.file_counts[:selected])
    assert after["publications"] == shape.published
    assert after["observations"] == before["observations"] + shape.changed
    failures = (
        budget_failures(counters.native_find.samples, config.database.sql_type)
        if native
        else []
    )
    return {
        "backend": config.database.sql_type,
        "shape": asdict(shape),
        "admission": {
            "max_new_galleries": max_new_galleries,
            "inventory_galleries": shape.galleries,
            "published_galleries": shape.published,
            "cached_unpublished_galleries": shape.galleries - shape.published,
            "admitted_unpublished_galleries": admitted,
            **counters.prepared_counts,
            "selected_file_facts": selected_files,
            "oracle": "Selected/deferred/waiting counts are public prepared-source values; FILE count is independently joined from receipt.build_id memberships after seal.",
        },
        "runtime_source_root": str(RUNTIME_ROOT),
        "runtime_provenance": runtime_provenance(),
        "fresh_generation": generation,
        "instrumented": native,
        "degraded_control": degraded,
        "source_seconds": source_seconds,
        "post_source_validation_seconds": time.perf_counter() - validation_started,
        "scope": "Fresh public source through sealed source; claim/policy before timer and complete_ingest/full READY after it. No analysis/publication in timed turn.",
        "source_sealed": receipt.sealed,
        "READY": "READY",
        "deep_reads": len(source.deep_reads),
        "marker_calls": source.marker_calls,
        "actions": dict(counters.actions),
        "before": before,
        "after": after,
        "whole_source_sql": asdict(counters.whole) if native else None,
        "staging_find_issue_sql": asdict(counters.find) if native else None,
        "staging_find_issue_native": asdict(counters.native_find) if native else None,
        "native_find_issue_ordinals": counters.native_issue_ordinals,
        "native_find_issues": counters.native_issues,
        "native_scope": "Native work covers sampled STAGING_FIND issues, including authority/checkpoint/receipt reads after connector.connect and before close; connection initialization PRAGMA/SET is excluded. Samples isolate the actual selector. SQLite quantum 1 is enabled only in selected issue calls, never all source SQL. Native walls and SQL seconds include instrumentation overhead. SQL call counts cover all source and all STAGING_FIND issues.",
        "fixed_selector_budget": {
            "window": WINDOW,
            "sqlite_vm": SQLITE_VM_LIMIT,
            "handlers": HANDLER_LIMITS,
        },
        "budget_failures": failures,
        "budget_passed": not failures if native else None,
        "main_workload_acceptance": "Not established by one run; requires the separately fixed medium/large comparison contract.",
    }


def runtime_provenance() -> dict[str, Any]:
    """Fail if import caching resolved a different runtime than the selected root."""
    root = (RUNTIME_ROOT / "src" / "h2hdb").resolve()
    actual = Path(h2hdb.__file__).resolve()
    if actual != root / "__init__.py":
        raise AssertionError("loaded runtime differs from the selected snapshot")
    return {
        "module_file": str(actual),
        "runtime_files": {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob("*.py"))
        },
    }


def _require_owned_sample_config(config: CoreConfig, receipt_path: Path | None) -> None:
    database = config.database
    prefix = "h2hdb_public_seed_"
    if receipt_path is None:
        raise ValueError("sample child requires its parent's allocation receipt")
    receipt = json.loads(receipt_path.read_text())
    identity = {
        "backend": database.sql_type,
        "database": database.database,
        "host": database.host,
        "port": database.port,
    }
    if (
        receipt.get("identity") != identity
        or not isinstance(receipt.get("nonce"), str)
        or len(receipt["nonce"]) != 32
    ):
        raise ValueError("sample configuration differs from its allocation receipt")
    if database.sql_type == "mariadb":
        if database.host not in {
            "127.0.0.1",
            "localhost",
            "::1",
        } or not database.database.startswith(prefix):
            raise ValueError("sample child requires its local synthetic owned schema")
    else:
        path = Path(database.database).resolve()
        if not path.parent.name.startswith(prefix) or path.name != "catalog.sqlite3":
            raise ValueError(
                "sample child requires its local synthetic owned SQLite file"
            )


def main() -> None:
    result: dict[str, Any]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    parser.add_argument("--galleries", type=int, required=True)
    parser.add_argument("--published", type=int, required=True)
    parser.add_argument("--files", type=int, required=True)
    parser.add_argument("--changed", type=int, default=4)
    parser.add_argument("--max-new-galleries", type=int)
    parser.add_argument("--private-config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--degraded", action="store_true")
    parser.add_argument("--sample-config-stdin", action="store_true")
    parser.add_argument("--build-seed-only", action="store_true")
    parser.add_argument("--owner-receipt", type=Path)
    parser.add_argument("--plain", action="store_true")
    parser.add_argument("--native-find-ordinals", type=str)
    parser.add_argument("--deadline-seconds", type=float, default=1800)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must be a new path")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        shape = ReuseShape(args.galleries, args.published, args.files, args.changed)
        runtime_provenance()
        ordinals = (
            frozenset(map(int, args.native_find_ordinals.split(",")))
            if args.native_find_ordinals
            else None
        )
        if args.sample_config_stdin:
            config = CoreConfig.model_validate_json(sys.stdin.read())
            _require_owned_sample_config(config, args.owner_receipt)
            if config.database.sql_type != args.backend:
                raise ValueError("child backend differs from its owned database")
            if args.build_seed_only:
                result = {
                    "status": "completed_public_seed",
                    "seed": build_public_seed(
                        config, shape, deadline_seconds=args.deadline_seconds
                    ),
                    "runtime_provenance": runtime_provenance(),
                }
                with args.output.open("x") as output:
                    json.dump(result, output, indent=2)
                    output.write("\n")
                raise SystemExit(0)
            run = run_measured_source(
                config,
                shape,
                native=not args.plain,
                degraded=args.degraded,
                max_new_galleries=args.max_new_galleries,
                native_find_ordinals=ordinals,
                deadline_seconds=args.deadline_seconds,
            )
            result = {"status": "completed_raw_sample", "run": run}
            exit_code = 0 if args.plain or run["budget_passed"] else 1
            with args.output.open("x") as output:
                json.dump(result, output, indent=2)
                output.write("\n")
            raise SystemExit(exit_code)
        with (
            owned_database(args.backend, args.private_config) as seed,
            owned_database(args.backend, args.private_config) as target,
        ):
            setup = build_public_seed(seed.config, shape)
            sealed = seal_public_seed(seed)
            clone = clone_public_seed(seed, target)
            run = run_measured_source(
                target.config,
                shape,
                native=not args.plain,
                degraded=args.degraded,
                max_new_galleries=args.max_new_galleries,
                native_find_ordinals=ordinals,
                deadline_seconds=args.deadline_seconds,
            )
            result = {
                "status": "complete",
                "seed": setup,
                "seal": sealed,
                "clone": clone,
                "run": run,
            }
        exit_code = 0 if args.plain or run["budget_passed"] else 1
    except Exception as error:
        result = {
            "status": "incomplete",
            "error_type": type(error).__name__,
            "error_locations": [
                {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(error.__traceback__)
            ],
        }
        exit_code = 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
