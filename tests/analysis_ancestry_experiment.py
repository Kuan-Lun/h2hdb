"""Paired public issue latency on identical durable source/analysis checkpoints."""

from __future__ import annotations

import sys
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Literal, cast
from unittest.mock import patch

from analysis_ancestry_baseline import historical_validate_ancestry_suffixes
from compaction_contracts import current_compaction_layout
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)

import h2hdb.vnext_analysis_repository as analysis
from h2hdb import (
    CoreConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
    VNextIngestSession,
    VNextIssuedAnalysisStep,
    VNextPreparedAnalysis,
    vnext_identity,
)
from h2hdb.sql_performance import measure_sql
from h2hdb.vnext_ingest_fence_repository import IngestFenceUnavailableError


@dataclass
class Sample:
    variant: str
    elapsed_seconds: float = 0.0
    sql_calls: int = 0
    sql_seconds: float = 0.0
    returned_rows: int = 0
    transaction_seconds: float = 0.0
    connection_seconds: float = 0.0

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        _query: str,
        read_rows: int,
    ) -> None:
        match category:
            case "sql":
                self.sql_calls += 1
                self.sql_seconds += elapsed
                self.returned_rows += read_rows
            case "transaction":
                self.transaction_seconds += elapsed
            case "connection":
                self.connection_seconds += elapsed


def public_ancestry_experiment(
    config: CoreConfig, *, page_count: int = 129, turns: int = 19
) -> dict[str, object]:
    """Fixed corpus and changed-page count; only ancestry/history grows.

    Both A/B variants issue a fresh public preparation handle against the same
    durable checkpoint, without commit. The production issue then proceeds
    through the real prepare/commit/publication/cleanup loop. Alternating AB/BA
    order counters systematic warmth bias; no synthetic sleeps or network
    latency injection are used. Wall-time samples remain evidence, not a flaky
    timing assertion. SQL cost assertions are deterministic negative controls.
    """
    started_at = datetime.now(UTC).isoformat()
    initialize_database(config)
    source = MemorySource([gallery(1), gallery(2)])
    library = MemoryLibrary(source)
    samples: list[dict[str, object]] = []
    layouts: list[int] = []
    target_turns = {1, 2, 9, 16, 17, 18, 19}
    original_issue = VNextIngestFacade.issue_analysis_step
    production = analysis._validate_ancestry_suffixes
    measured_turns: set[int] = set()
    turn = 0

    def paired_issue(
        facade: VNextIngestFacade,
        session: VNextIngestSession,
        prepared: VNextPreparedAnalysis,
    ) -> VNextIssuedAnalysisStep:
        issued = original_issue(facade, session, prepared)
        payload = issued._payload
        if (
            turn not in target_turns
            or turn in measured_turns
            or payload is None
            or payload.stage != b"validate_file_hash_decision"
        ):
            return issued
        measured_turns.add(turn)
        expected = replace(payload, batch_key=b"comparison")
        values: list[dict[str, object]] = []
        for repetition in range(4):
            order: tuple[str, ...] = ("production", "historical")
            if repetition % 2:
                order = tuple(reversed(order))
            for variant in order:
                validator = (
                    production
                    if variant == "production"
                    else historical_validate_ancestry_suffixes
                )
                # Fresh handles exercise begin_and_issue_next_batch; reusing
                # prepared would silently take the active-issue fast path.
                with facade.prepare_analysis(
                    prepared._build_id, prepared._policy, max_rows=prepared._max_rows
                ) as fresh:
                    sample = Sample(variant)
                    with (
                        patch.object(
                            analysis, "_validate_ancestry_suffixes", validator
                        ),
                        measure_sql(sample, observe_nested=True),
                    ):
                        started = perf_counter()
                        compared = original_issue(facade, session, fresh)
                        sample.elapsed_seconds = perf_counter() - started
                    assert compared._payload is not None
                    assert (
                        replace(compared._payload, batch_key=b"comparison") == expected
                    )
                    values.append({"repetition": repetition, **asdict(sample)})
        # A fresh session check must reject a delayed owner before ancestry
        # authority is used, even after valid issue calls on the same checkpoint.
        with facade.prepare_analysis(
            prepared._build_id, prepared._policy, max_rows=prepared._max_rows
        ) as stale:
            try:
                original_issue(
                    facade,
                    replace(session, ingest_owner_token=b"stale-owner-test"),
                    stale,
                )
            except IngestFenceUnavailableError:
                pass
            else:
                raise AssertionError("stale owner issued an analysis batch")
        samples.append(
            {
                "turn": turn,
                "samples": values,
                "elapsed_medians": {
                    variant: median(
                        cast(float, row["elapsed_seconds"])
                        for row in values
                        if row["variant"] == variant
                    )
                    for variant in ("production", "historical")
                },
            }
        )
        return issued

    with (
        VNextIngestFacade(config, clock=Clock()) as facade,
        patch.object(VNextIngestFacade, "issue_analysis_step", paired_issue),
    ):
        for turn in range(1, turns + 1):
            source.put(
                gallery(
                    1, pages=[f"fixed-page-{i}".encode() for i in range(page_count)]
                )
            )
            source.put(gallery(2, pages=[f"changed-page-{turn}".encode()]))
            receipt = run_ingest_turn(
                facade,
                source=source,
                library=library,
                policy=ingest_policy(artifacts_required=False),
            )
            assert receipt.source.sealed and receipt.analysis.terminal
            layouts.append(current_compaction_layout(config).depth)
            with closing(VNextCatalogFacade(config)) as catalog:
                current = catalog.get_catalog_revision()
                assert current.publication_count == 2
                publications = catalog.discover_publications(
                    revision=current, limit=128
                )
                assert {item.gid for item in publications.publications} == {1, 2}
                expected_content = {
                    1: vnext_identity.effective_content_digest(
                        tuple(
                            sha256(f"fixed-page-{i}".encode()).digest()
                            for i in range(page_count)
                        )
                    ).hex(),
                    2: vnext_identity.effective_content_digest(
                        (sha256(f"changed-page-{turn}".encode()).digest(),)
                    ).hex(),
                }
                assert {
                    item.gid: item.content_sha256 for item in publications.publications
                } == expected_content
                assert current.artifact_count == 0
            drain_maintenance(facade)
    assert layouts == [index % 17 for index in range(turns)]
    assert measured_turns == target_turns.intersection(range(1, turns + 1))
    assert full_check(config).state == "READY"
    for item in samples:
        sample_turn = int(str(item["turn"]))
        rows = item["samples"]
        assert isinstance(rows, list)
        production_counts = {
            row["sql_calls"] for row in rows if row["variant"] == "production"
        }
        historical_counts = {
            row["sql_calls"] for row in rows if row["variant"] == "historical"
        }
        assert len(production_counts) == len(historical_counts) == 1
        ancestors = 0 if sample_turn == 1 else (sample_turn - 2) % 17 + 1
        expected_saved = 0 if ancestors == 0 else 26 * ancestors - 6
        assert (
            next(iter(historical_counts)) - next(iter(production_counts))
            == expected_saved
        )
    root = Path(__file__).resolve().parents[1]
    paths = (
        "src/h2hdb/vnext_analysis_repository.py",
        "tests/analysis_ancestry_baseline.py",
        "tests/analysis_ancestry_experiment.py",
        "tests/test_analysis_ancestry_costs.py",
    )
    return {
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "invocation": [sys.executable, *sys.argv],
        "source_sha256": {
            name: sha256((root / name).read_bytes()).hexdigest() for name in paths
        },
        "historical_baseline": "0.39.10 _validate_ancestry_suffixes body; current point loaders and exact scalar checks are shared",
        "limits": [
            "Fixture uses real Core source/analysis/publication paths with in-memory adapters, no image compression or NAS I/O",
            "Elapsed samples compare complete fresh public issue calls; they do not measure an end-to-end historical deployment",
            "A/B queries run sequentially on identical checkpoints; page cache is warm, no simulated latency",
            "Finite deterministic SQL cost bound; no universal latency or server examined-row bound is claimed",
        ],
        "skips": [],
        "correctness_oracle": "identical complete issued payload except fresh batch token; independent content digests from source page bytes; public catalog membership; real cleanup DONE and full READY",
        "backend": config.database.sql_type,
        "fixed_galleries": 2,
        "fixed_pages": page_count + 1,
        "changed_pages_per_turn": 1,
        "complete_public_turns": turns,
        "published_depths": layouts,
        "scope": "whole public issue on identical checkpoints, four paired AB/BA samples; real source seals, analysis prepare/commit, publication, cleanup DONE and full READY",
        "samples": samples,
    }
