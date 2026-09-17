"""Finite SQL correspondence for GID-only preparation, not a timing SLO.

Fresh SQLite fixtures drive the public source/analysis protocol. Both GID
stages independently validate complete metadata streams and qualification, but
must issue no tag or canonical-value query and prepare no content upload.
Metadata chunk reads still scale with metadata bytes; the zero cost claim is
specifically about unnecessary tag canonical validation, not all SQL or I/O.
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
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import measure_sql
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import (
    AnalysisGidPreparation,
    AnalysisPreparationAuthority,
    AnalysisStageIssue,
)
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
class _GidSample:
    stage: bytes
    gallery_id: int
    queries: tuple[str, ...]


@contextmanager
def _observe_gid_preparation(
    monkeypatch: pytest.MonkeyPatch,
    *,
    unnecessary_marker_scan: Literal["helper", "wrapper"] | None = None,
) -> Iterator[list[_GidSample]]:
    samples: list[_GidSample] = []
    active_stage: bytes | None = None
    prepare = VNextIngestAnalysisOrchestrator._prepare_gallery_work
    gid_prepare = analysis_repository.AnalysisRepository.prepare_gid_gallery
    gid_prepare_facts = analysis_repository._prepare_gid_gallery

    def prepare_gallery(
        owner: VNextIngestAnalysisOrchestrator, issue: AnalysisStageIssue
    ) -> _LocalAnalysisWork:
        nonlocal active_stage
        previous, active_stage = active_stage, issue.stage
        try:
            local = prepare(owner, issue)
            if issue.stage in GID_STAGES:
                assert local.plans == (), "GID preparation created a content upload"
            return local
        finally:
            active_stage = previous

    def prepare_gid_facts(
        work: VNextUnitOfWork,
        run: analysis_repository._RunAuthority,
        gallery_id: int,
        authority: AnalysisPreparationAuthority,
    ) -> AnalysisGidPreparation:
        prepared = gid_prepare_facts(work, run, gallery_id, authority)
        if unnecessary_marker_scan == "helper":
            analysis_repository._gallery_has_already_uploaded_marker(
                work, gallery_id, prepared.observation_id
            )
        return prepared

    def inspect_gid(
        connector: SQLConnector,
        *,
        backend: str,
        authority: AnalysisPreparationAuthority,
        gallery_id: int,
    ) -> AnalysisGidPreparation:
        assert active_stage in GID_STAGES
        assert active_stage is not None
        counter = _SQLCounter()
        # Observe the complete repository entry point so a scan moved outside
        # its facts helper cannot escape the same zero-tag-query contract.
        with measure_sql(counter):
            prepared = gid_prepare(
                connector,
                backend=backend,
                authority=authority,
                gallery_id=gallery_id,
            )
            if unnecessary_marker_scan == "wrapper":
                with connector.read_transaction():
                    work = VNextUnitOfWork(connector, backend=backend)
                    analysis_repository._gallery_has_already_uploaded_marker(
                        work, gallery_id, prepared.observation_id
                    )
        samples.append(_GidSample(active_stage, gallery_id, tuple(counter.queries)))
        return prepared

    with monkeypatch.context() as patch:
        patch.setattr(
            VNextIngestAnalysisOrchestrator, "_prepare_gallery_work", prepare_gallery
        )
        patch.setattr(analysis_repository, "_prepare_gid_gallery", prepare_gid_facts)
        patch.setattr(
            analysis_repository.AnalysisRepository,
            "prepare_gid_gallery",
            staticmethod(inspect_gid),
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
    path = tmp_path / "analysis.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        result = run_analysis(facade, session, policy, receipt.build_id)
        assert result.terminal
        assert result.snapshot_manifest_sha256 is not None
    # Independent result oracle: distinct contents and GIDs retain every gallery.
    with SQLiteConnector(database=str(path)) as connector:
        assert connector.fetch_all(
            "SELECT gid, winner_gallery_id FROM catalog_analysis_gid_winner_resolved "
            "WHERE analysis_id = %s ORDER BY gid",
            (result.analysis_id,),
        ) == [(gid, gid) for gid in range(1, galleries + 1)]


def _assert_cost(
    samples: list[_GidSample], *, galleries: int, canonical_cost: int
) -> None:
    assert canonical_cost == 0, "GID Lean trace must have no tag canonical reads"
    assert len(samples) == 2 * galleries
    for stage in GID_STAGES:
        selected = [sample for sample in samples if sample.stage == stage]
        assert len(selected) == galleries
        assert len({sample.gallery_id for sample in selected}) == galleries
        canonical = sum(
            "canonical_value" in query
            for sample in selected
            for query in sample.queries
        )
        assert canonical == canonical_cost, "canonical SQL cost differs from Lean trace"
        assert not any(
            "catalog_gallery_observation_tags" in query
            or "catalog_tag_terms" in query
            or "catalog_analysis_file_hash_decision_resolved" in query
            for sample in selected
            for query in sample.queries
        ), "GID preparation read a content-only relation"


@pytest.mark.parametrize("galleries", [1, 2])
@pytest.mark.parametrize("tags", [0, 1, 127, 128, 129])
def test_gid_preparation_sql_matches_constructed_lean_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    galleries: int,
    tags: int,
) -> None:
    with _observe_gid_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            galleries=galleries,
            tags=tuple(f"tag-{position}" for position in range(tags)),
        )
    _assert_cost(
        samples,
        galleries=galleries,
        canonical_cost=model_costs["uniform", galleries, tags],
    )


@pytest.mark.parametrize("preceding", [0, 1, 127, 128])
def test_gid_preparation_ignores_marker_position(
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
    with _observe_gid_preparation(monkeypatch) as samples:
        _run_source_analysis(tmp_path, galleries=1, tags=tags)
    _assert_cost(samples, galleries=1, canonical_cost=model_costs["uniform", 1, 3])


@pytest.mark.parametrize("location", ["helper", "wrapper"])
def test_sql_cost_correspondence_rejects_an_extra_real_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    location: Literal["helper", "wrapper"],
) -> None:
    with _observe_gid_preparation(
        monkeypatch, unnecessary_marker_scan=location
    ) as samples:
        _run_source_analysis(tmp_path, galleries=1, tags=("tag-0",))
    with pytest.raises(AssertionError, match="canonical SQL cost differs"):
        _assert_cost(samples, galleries=1, canonical_cost=model_costs["uniform", 1, 1])


@pytest.mark.parametrize("extra_bytes", [0, 1])
def test_gid_preparation_ignores_single_and_multi_leaf_tags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int, int], int],
    extra_bytes: int,
) -> None:
    with _observe_gid_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            galleries=1,
            tags=("x" * (CANONICAL_VALUE_CHUNK_BYTES + extra_bytes),),
        )
    _assert_cost(samples, galleries=1, canonical_cost=model_costs["uniform", 1, 1])
