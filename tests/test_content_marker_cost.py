"""Real content-stage SQL costs, including the complete preparation entry point.

These finite, deterministic tests count client statements and returned tag
references. They do not bound server rows examined, RSS or wall-clock latency.
Every content stage retains its own fresh preparation and exact result oracle.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast

import pytest
from test_content_marker_validation import _corrupt_value, _store_value, _TagRows
from vnext_generated_database import open_generated_sqlite_database
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
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import (
    AnalysisGalleryPreparation,
    AnalysisPreparationAuthority,
    AnalysisStageIssue,
)
from h2hdb.vnext_canonical_value_repository import CanonicalValueCollisionError
from h2hdb.vnext_identity import ANALYSIS_ALREADY_UPLOADED_MARKER
from h2hdb.vnext_ingest_analysis import (
    VNextIngestAnalysisOrchestrator,
    _LocalAnalysisWork,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "verification" / "lean" / "AnalysisPreparationCost.lean"
CONTENT_STAGES = (
    b"impacted_content",
    b"content_owner_candidate",
    b"validate_content_owner_candidate",
)
_TAG_RELATION = "catalog_gallery_observation_tags"


@dataclass
class _SQLCounter:
    queries: list[tuple[str, int]] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        _elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        if category == "sql":
            self.queries.append((query, rows))


@dataclass(frozen=True)
class _ContentSample:
    stage: bytes
    gallery_id: int
    prefer_not_already_uploaded: int | None
    queries: tuple[tuple[str, int], ...]


@contextmanager
def _observe_content_preparation(
    monkeypatch: pytest.MonkeyPatch,
    *,
    extra_tag_query: bool = False,
    scalar_only: bool = False,
) -> Iterator[list[_ContentSample]]:
    samples: list[_ContentSample] = []
    active_stage: bytes | None = None
    prepare_work = VNextIngestAnalysisOrchestrator._prepare_gallery_work
    prepare_gallery = analysis_repository.AnalysisRepository.prepare_gallery

    def inspect_work(
        owner: VNextIngestAnalysisOrchestrator, issue: AnalysisStageIssue
    ) -> _LocalAnalysisWork:
        nonlocal active_stage
        previous, active_stage = active_stage, issue.stage
        try:
            return prepare_work(owner, issue)
        finally:
            active_stage = previous

    def inspect_gallery(
        connector: SQLConnector,
        *,
        backend: str,
        authority: AnalysisPreparationAuthority,
        gallery_id: int,
    ) -> AnalysisGalleryPreparation:
        assert active_stage in CONTENT_STAGES
        assert active_stage is not None
        counter = _SQLCounter()
        with measure_sql(counter):
            prepared = prepare_gallery(
                connector,
                backend=backend,
                authority=authority,
                gallery_id=gallery_id,
            )
            if extra_tag_query:
                # A query added outside the marker helper must also be counted.
                with connector.read_transaction():
                    connector.fetch_all(
                        "SELECT position FROM catalog_gallery_observation_tags "
                        "WHERE gallery_id = %s AND observation_id = %s "
                        "ORDER BY position LIMIT 128",
                        (gallery_id, prepared.observation_id),
                    )
        samples.append(
            _ContentSample(
                active_stage,
                gallery_id,
                prepared.content_prefer_not_already_uploaded,
                tuple(counter.queries),
            )
        )
        return prepared

    with monkeypatch.context() as patch:
        if scalar_only:
            # Keep authoritative scalar validation and results, but deliberately
            # remove the optimization so the same positive cost oracle fails.
            patch.setattr(
                analysis_repository,
                "load_and_validate_single_page_canonical_values",
                lambda *_args, **_kwargs: {},
            )
        patch.setattr(
            VNextIngestAnalysisOrchestrator, "_prepare_gallery_work", inspect_work
        )
        patch.setattr(
            analysis_repository.AnalysisRepository,
            "prepare_gallery",
            staticmethod(inspect_gallery),
        )
        yield samples


def _run_source_analysis(
    tmp_path: Path,
    *,
    tags: tuple[str, ...],
    galleries: int = 1,
    marked_gallery: int | None = None,
    same_content: bool = False,
    expected_winners: tuple[int, ...] | None = None,
) -> None:
    source = MemorySource(
        [
            gallery(
                gid,
                title=f"Content cost {gid}",
                pages=[
                    b"shared content"
                    if same_content
                    else f"distinct content {gid}".encode()
                ],
                artists=(),
                language=None,
                extra_tags=[
                    *(
                        [("group", ANALYSIS_ALREADY_UPLOADED_MARKER.decode("ascii"))]
                        if gid == marked_gallery
                        else []
                    ),
                    *(("group", tag) for tag in tags),
                ],
            )
            for gid in range(1, galleries + 1)
        ]
    )
    path = tmp_path / "content.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        result = run_analysis(facade, session, policy, receipt.build_id)
        assert result.terminal
        assert result.snapshot_manifest_sha256 is not None
    with SQLiteConnector(database=str(path)) as connector:
        assert connector.fetch_all(
            "SELECT gid, winner_gallery_id FROM catalog_analysis_gid_winner_resolved "
            "WHERE analysis_id = %s ORDER BY gid",
            (result.analysis_id,),
        ) == [
            (gid, gid)
            for gid in (
                range(1, galleries + 1)
                if expected_winners is None
                else expected_winners
            )
        ]


def _assert_cost(
    samples: list[_ContentSample],
    *,
    galleries: int,
    list_queries: int,
    canonical_queries: int,
    preference: int = 1,
) -> None:
    assert len(samples) == len(CONTENT_STAGES) * galleries
    for stage in CONTENT_STAGES:
        selected = [sample for sample in samples if sample.stage == stage]
        assert len(selected) == galleries
        assert len({sample.gallery_id for sample in selected}) == galleries
        for sample in selected:
            assert sample.prefer_not_already_uploaded == preference
            canonical = sum(
                "canonical_value" in query for query, _rows in sample.queries
            )
            assert canonical == canonical_queries, "content canonical SQL cost differs"
            listings = [
                (query, rows)
                for query, rows in sample.queries
                if _TAG_RELATION in query
            ]
            assert len(listings) == list_queries, "content tag-list SQL cost differs"
            assert all("LIMIT" in query and rows <= 128 for query, rows in listings), (
                "tag enumeration must stay bounded at 128 returned references"
            )


@pytest.fixture(scope="module")
def model_costs() -> dict[tuple[str, int], tuple[int, int]]:
    lean = shutil.which("lean")
    assert lean is not None, "Lean is required for executable cost correspondence"
    result = subprocess.run(
        [lean, "--error=warning", "--run", str(MODEL), "--content"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert not result.stderr
    costs: dict[tuple[str, int], tuple[int, int]] = {}
    for line in result.stdout.splitlines():
        kind, argument, listings, canonical = line.split(",")
        key = kind, int(argument)
        assert key not in costs
        costs[key] = int(listings), int(canonical)
    assert set(costs) == {
        *(("uniform", count) for count in (0, 1, 127, 128, 129, 130, 256, 512, 1024)),
        *(("marker", count) for count in (0, 1, 127, 128, 129)),
        *((kind, size) for kind in ("multileaf", "mixed") for size in (32769, 65536)),
        ("fault-before-marker", 0),
        ("marker-before-fault", 0),
    }
    return costs


def test_content_preparation_rejects_per_tag_query_growth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pre-fix runtime fails this budget while returning the same result."""
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path, tags=tuple(f"tag-{position}" for position in range(129))
        )
    _assert_cost(samples, galleries=1, list_queries=3, canonical_queries=6)


@pytest.mark.parametrize("tags", [0, 1, 127, 128, 129, 130, 256, 512, 1024])
def test_content_preparation_sql_matches_lean_at_page_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int], tuple[int, int]],
    tags: int,
) -> None:
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path, tags=tuple(f"tag-{position}" for position in range(tags))
        )
    listings, canonical = model_costs["uniform", tags]
    _assert_cost(
        samples, galleries=1, list_queries=listings, canonical_queries=canonical
    )


@pytest.mark.parametrize("preceding", [0, 1, 127, 128, 129])
def test_content_marker_early_exit_matches_constructed_lean_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int], tuple[int, int]],
    preceding: int,
) -> None:
    tags = (
        *(f"before-{position}" for position in range(preceding)),
        ANALYSIS_ALREADY_UPLOADED_MARKER.decode("ascii"),
        "after-0",
        "after-1",
    )
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(tmp_path, tags=tags)
    listings, canonical = model_costs["marker", preceding]
    _assert_cost(
        samples,
        galleries=1,
        list_queries=listings,
        canonical_queries=canonical,
        preference=0,
    )


@pytest.mark.parametrize("byte_count", [32769, 65536])
@pytest.mark.parametrize("kind", ["multileaf", "mixed"])
def test_content_long_values_have_a_separate_cost_regime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int], tuple[int, int]],
    byte_count: int,
    kind: str,
) -> None:
    tags: tuple[str, ...] = ("x" * byte_count,)
    if kind == "mixed":
        tags = ("short", *tags)
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(tmp_path, tags=tags)
    listings, canonical = model_costs[kind, byte_count]
    _assert_cost(
        samples, galleries=1, list_queries=listings, canonical_queries=canonical
    )


def test_content_costs_repeat_independently_for_each_gallery_and_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: dict[tuple[str, int], tuple[int, int]],
) -> None:
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            tags=tuple(f"shared-{position}" for position in range(128)),
            galleries=2,
        )
    listings, canonical = model_costs["uniform", 128]
    _assert_cost(
        samples, galleries=2, list_queries=listings, canonical_queries=canonical
    )


@pytest.mark.parametrize("marker_first", [False, True])
def test_content_fault_replay_cost_matches_constructed_lean_trace(
    tmp_path: Path,
    model_costs: dict[tuple[str, int], tuple[int, int]],
    marker_first: bool,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "fault-cost.sqlite3")
    try:
        marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
        corrupt = _corrupt_value(connector, _store_value(connector, b"bad"), "payload")
        ordinary = _store_value(connector, b"ordinary")
        values = (
            (ordinary, marker, corrupt) if marker_first else (ordinary, corrupt, marker)
        )
        counter = _SQLCounter()
        with connector.read_transaction(), measure_sql(counter):
            work = VNextUnitOfWork(
                instrument_connector(cast(SQLConnector, _TagRows(connector, values))),
                backend="sqlite",
            )
            if marker_first:
                assert analysis_repository._gallery_has_already_uploaded_marker(
                    work, 1, 1
                )
            else:
                with pytest.raises(CanonicalValueCollisionError):
                    analysis_repository._gallery_has_already_uploaded_marker(work, 1, 1)
        kind = "marker-before-fault" if marker_first else "fault-before-marker"
        lists, canonical = model_costs[kind, 0]
        assert sum(_TAG_RELATION in query for query, _rows in counter.queries) == lists
        assert (
            sum("canonical_value" in query for query, _rows in counter.queries)
            == canonical
        )
    finally:
        connector.close()


def test_content_marker_preference_selects_the_unmarked_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _observe_content_preparation(monkeypatch) as samples:
        _run_source_analysis(
            tmp_path,
            tags=("ordinary",),
            galleries=2,
            marked_gallery=1,
            same_content=True,
            expected_winners=(2,),
        )
    for stage in CONTENT_STAGES:
        assert {
            sample.gallery_id: sample.prefer_not_already_uploaded
            for sample in samples
            if sample.stage == stage
        } == {1: 0, 2: 1}


@pytest.mark.parametrize("fault", ["wrapper_query", "scalar_only"])
def test_content_cost_oracle_rejects_deliberate_degradation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    with _observe_content_preparation(
        monkeypatch,
        extra_tag_query=fault == "wrapper_query",
        scalar_only=fault == "scalar_only",
    ) as samples:
        _run_source_analysis(tmp_path, tags=("tag-0", "tag-1", "tag-2"))
    expected = "tag-list" if fault == "wrapper_query" else "canonical"
    with pytest.raises(AssertionError, match=f"{expected} SQL cost differs"):
        _assert_cost(samples, galleries=1, list_queries=2, canonical_queries=6)
