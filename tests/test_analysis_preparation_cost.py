"""Finite SQL correspondence for the GID cost model, not a timing SLO.

These fixtures run the actual public source/analysis protocol on fresh SQLite
databases. Observers count real statements inside each marker scan in each GID
preparation stage; they never substitute query results or preparation values.
The current positive tag cost is an identified optimization target, not a
claim that GID preparation is already independent of tags.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pytest
from vnext_pipeline import (
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_source,
)

from h2hdb import CoreConfig, DatabaseConfig, VNextIngestFacade
from h2hdb import vnext_analysis_repository as analysis_repository
from h2hdb.sql_performance import measure_sql
from h2hdb.vnext_analysis_repository import AnalysisStageIssue
from h2hdb.vnext_identity import (
    ANALYSIS_ALREADY_UPLOADED_MARKER,
    CANONICAL_VALUE_CHUNK_BYTES,
)
from h2hdb.vnext_ingest_analysis import (
    VNextIngestAnalysisOrchestrator,
    _LocalAnalysisWork,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "verification" / "lean" / "AnalysisPreparationCost.lean"
GID_STAGES = (b"gid_candidate", b"validate_gid_candidate")


@pytest.fixture(scope="module")
def model_costs() -> dict[tuple[str, int, int], int]:
    lean = shutil.which("lean")
    assert lean is not None, "Lean is required for executable cost correspondence"
    result = subprocess.run(
        [lean, "--error=warning", "--run", str(MODEL)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert not result.stderr
    costs = {}
    for line in result.stdout.splitlines():
        kind, galleries, tags, cost = line.split(",")
        key = kind, int(galleries), int(tags)
        assert key not in costs
        costs[key] = int(cost)
    assert set(costs) == {
        *(
            ("uniform", galleries, tags)
            for galleries in (0, 1, 2)
            for tags in (0, 1, 3, 127, 128, 129)
        ),
        *(("marker", 1, preceding) for preceding in (0, 1, 127, 128)),
    }
    return costs


@dataclass
class _SQLCounter:
    queries: list[str] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        _elapsed: float,
        query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.queries.append(query)


@dataclass(frozen=True)
class _MarkerSample:
    stage: bytes
    gallery_id: int
    matched: bool
    queries: tuple[str, ...]


@contextmanager
def _observe_gid_marker_scans(
    monkeypatch: pytest.MonkeyPatch,
    *,
    extra_query: bool = False,
) -> Iterator[list[_MarkerSample]]:
    samples: list[_MarkerSample] = []
    active_stage: bytes | None = None
    prepare = VNextIngestAnalysisOrchestrator._prepare_gallery_work
    marker = analysis_repository._gallery_has_already_uploaded_marker

    def prepare_gallery(
        owner: VNextIngestAnalysisOrchestrator, issue: AnalysisStageIssue
    ) -> _LocalAnalysisWork:
        nonlocal active_stage
        previous, active_stage = active_stage, issue.stage
        try:
            return prepare(owner, issue)
        finally:
            active_stage = previous

    def inspect_marker(
        work: VNextUnitOfWork, gallery_id: int, observation_id: int
    ) -> bool:
        if active_stage not in GID_STAGES:
            return marker(work, gallery_id, observation_id)
        assert active_stage is not None
        counter = _SQLCounter()
        with measure_sql(counter):
            matched = marker(work, gallery_id, observation_id)
            if extra_query:
                assert work.connector.fetch_one("SELECT 1") == (1,)
        samples.append(
            _MarkerSample(active_stage, gallery_id, matched, tuple(counter.queries))
        )
        return matched

    with monkeypatch.context() as patch:
        patch.setattr(
            VNextIngestAnalysisOrchestrator, "_prepare_gallery_work", prepare_gallery
        )
        patch.setattr(
            analysis_repository, "_gallery_has_already_uploaded_marker", inspect_marker
        )
        yield samples


def _run_source_analysis(
    tmp_path: Path,
    *,
    galleries: int,
    tags: tuple[str, ...],
) -> None:
    source = MemorySource(
        [
            gallery(
                gid,
                title=f"GID cost {gid}",
                pages=[f"distinct page {gid}".encode()],
                artists=(),
                language=None,
                extra_tags=[("group", tag) for tag in tags],
            )
            for gid in range(1, galleries + 1)
        ]
    )
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "analysis.sqlite3")
        )
    )
    initialize_database(config)
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        result = run_analysis(facade, session, policy, receipt.build_id)
        assert result.terminal
        assert result.snapshot_manifest_sha256 is not None


def _assert_cost(
    samples: list[_MarkerSample],
    *,
    galleries: int,
    canonical_cost: int,
    matched: bool,
) -> None:
    assert len(samples) == 2 * galleries
    for stage in GID_STAGES:
        selected = [sample for sample in samples if sample.stage == stage]
        assert len(selected) == galleries
        assert len({sample.gallery_id for sample in selected}) == galleries
        assert all(sample.matched is matched for sample in selected)
        # One tag-list SELECT per gallery is outside the canonical-read model.
        for sample in selected:
            assert sample.queries
            assert (
                "FROM catalog_gallery_observation_tags AS observed"
                in (sample.queries[0])
            ), "marker scan acquired an unmodeled SQL prefix"
        observed = sum(len(sample.queries) - 1 for sample in selected)
        assert observed == canonical_cost, "canonical SQL cost differs from Lean trace"


@pytest.mark.parametrize("galleries", [1, 2])
@pytest.mark.parametrize("tags", [0, 1, 127, 128, 129])
def test_gid_preparation_sql_matches_constructed_lean_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    galleries: int,
    tags: int,
) -> None:
    with _observe_gid_marker_scans(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            galleries=galleries,
            tags=tuple(f"tag-{position}" for position in range(tags)),
        )
    total = model_costs["uniform", galleries, tags]
    assert total % 2 == 0
    _assert_cost(samples, galleries=galleries, canonical_cost=total // 2, matched=False)
    if tags:
        assert total > 0, "current tag-dependent cost must not be called optimized"


@pytest.mark.parametrize("preceding", [0, 1, 127, 128])
def test_marker_early_exit_matches_lean_visited_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    preceding: int,
) -> None:
    tags = (
        *(f"before-{position}" for position in range(preceding)),
        ANALYSIS_ALREADY_UPLOADED_MARKER.decode("ascii"),
        "after-0",
        "after-1",
    )
    with _observe_gid_marker_scans(monkeypatch) as samples:
        _run_source_analysis(tmp_path, galleries=1, tags=tags)
    _assert_cost(
        samples,
        galleries=1,
        canonical_cost=model_costs["marker", 1, preceding],
        matched=True,
    )


def test_sql_cost_correspondence_rejects_an_extra_real_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
) -> None:
    with _observe_gid_marker_scans(monkeypatch, extra_query=True) as samples:
        _run_source_analysis(tmp_path, galleries=1, tags=("tag-0",))
    with pytest.raises(AssertionError, match="canonical SQL cost differs"):
        _assert_cost(
            samples,
            galleries=1,
            canonical_cost=model_costs["uniform", 1, 1] // 2,
            matched=False,
        )


@pytest.mark.parametrize("extra_bytes", [0, 1])
def test_single_leaf_cost_scope_ends_at_canonical_chunk_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    extra_bytes: int,
) -> None:
    with _observe_gid_marker_scans(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            galleries=1,
            tags=("x" * (CANONICAL_VALUE_CHUNK_BYTES + extra_bytes),),
        )
    single_leaf_cost = model_costs["uniform", 1, 1] // 2
    if extra_bytes == 0:
        _assert_cost(
            samples, galleries=1, canonical_cost=single_leaf_cost, matched=False
        )
    else:
        assert all(len(sample.queries) - 1 > single_leaf_cost for sample in samples)
        with pytest.raises(AssertionError, match="canonical SQL cost differs"):
            _assert_cost(
                samples, galleries=1, canonical_cost=single_leaf_cost, matched=False
            )
