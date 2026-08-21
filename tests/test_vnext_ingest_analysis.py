from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path

import pytest
from test_vnext_analysis_repository import (
    _authorities,
    _generated_database,
    _map_working_build,
    _seed_build,
    _seed_gallery,
    _seed_initial_snapshot,
    _seed_preparation_facts,
    _seed_root,
)

import h2hdb
from h2hdb import (
    VNextAnalysisAdvanceResult,
    VNextIngestFacade,
    VNextIssuedAnalysisStep,
    VNextPreparedAnalysis,
    VNextPreparedAnalysisStep,
)
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import (
    VNextArtifactProducer,
    VNextArtifactStoragePolicy,
    VNextIngestPolicy,
    VNextIngestSession,
    VNextResolvedIngestPolicy,
)
from h2hdb.repository import RepositoryContext
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_ingest_analysis import (
    VNextIngestAnalysisOrchestrator,
    _LocalAnalysisWork,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


class _Tokens:
    def __init__(self) -> None:
        self._next = 1

    def __call__(self) -> bytes:
        value = self._next.to_bytes(16, "big")
        self._next += 1
        return value


class _Clock:
    def __init__(self, start: int = 100) -> None:
        self._next = start

    def __call__(self) -> int:
        value = self._next
        self._next += 1
        return value


def _config(path: Path) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(path)),
    )


def _policy() -> VNextResolvedIngestPolicy:
    producer = VNextArtifactProducer(
        b"analysis-test-writer",
        b"cpython-test-abi",
        b"pillow-test-build",
        b"libjpeg-test-build",
        b"zlib-test-build",
    )
    natural = VNextIngestPolicy(
        producer=producer,
        storage=VNextArtifactStoragePolicy(b"analysis-test-storage"),
    )
    return VNextResolvedIngestPolicy(
        natural,
        1,
        1,
        natural.artifact_policy_sha256,
        natural.producer_fingerprint_sha256,
        1,
        1,
        1,
        False,
    )


def _session(gate: GateLease, turn: IngestTurn) -> VNextIngestSession:
    return VNextIngestSession(
        gate.owner_token,
        gate.gate_generation,
        gate.slots[0],
        gate.lease_expires_at,
        turn.generation,
        turn.owner_token,
        turn.lease_expires_at,
        None,
        None,
        None,
        None,
    )


def _orchestrator(path: Path) -> VNextIngestAnalysisOrchestrator:
    return VNextIngestAnalysisOrchestrator(
        RepositoryContext.from_config(_config(path)),
        clock=_Clock(),
        token_factory=_Tokens(),
    )


def _drive(
    orchestrator: VNextIngestAnalysisOrchestrator,
    session: VNextIngestSession,
    build_id: bytes,
    *,
    max_rows: int = 128,
    stop: Callable[[VNextIssuedAnalysisStep], bool] | None = None,
) -> tuple[
    VNextAnalysisAdvanceResult | None,
    VNextPreparedAnalysis,
    VNextIssuedAnalysisStep | None,
]:
    prepared = orchestrator.prepare_analysis(
        build_id,
        _policy(),
        max_rows=max_rows,
    )
    for _index in range(10_000):
        issued = orchestrator.issue_analysis_step(session, prepared)
        if stop is not None and stop(issued):
            return None, prepared, issued
        local = orchestrator.prepare_analysis_step(prepared, issued)
        result = orchestrator.commit_analysis_step(session, local)
        if result.terminal:
            return result, prepared, None
    prepared.close()
    raise AssertionError("analysis orchestration did not converge")


def _seed_empty(path: Path) -> tuple[bytes, GateLease, IngestTurn]:
    connector = _generated_database(path)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            scope = _seed_root(connector)
            build_id = b"e" * 16
            _seed_build(
                connector,
                build_id=build_id,
                scope=scope,
                manifest_byte=7,
                gallery_count=0,
            )
            _map_working_build(
                connector,
                build_id=build_id,
                generation=turn.generation,
            )
        return build_id, gate, turn
    finally:
        connector.close()


def test_empty_build_runs_all_stages_and_snapshot_end_to_end(tmp_path: Path) -> None:
    database = tmp_path / "analysis-empty.sqlite3"
    build_id, gate, turn = _seed_empty(database)

    result, prepared, stopped = _drive(
        _orchestrator(database),
        _session(gate, turn),
        build_id,
    )
    assert result is not None
    assert stopped is None
    assert result.terminal and result.stage == b"snapshot_manifest"
    assert result.snapshot_manifest_sha256 is not None
    analysis_id = result.analysis_id
    prepared.close()

    connector = SQLiteConnector(str(database))
    connector.connect()
    try:
        assert connector.fetch_one(
            "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
            (analysis_id,),
        ) == ("COMPLETE",)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_checkpoint_states "
            "WHERE analysis_id = %s AND state = 'COMPLETE'",
            (analysis_id,),
        ) == (15,)
    finally:
        connector.close()


def test_nonempty_gallery_preparation_uses_exact_issued_memberships(
    tmp_path: Path,
) -> None:
    database = tmp_path / "analysis-nonempty.sqlite3"
    connector = _generated_database(database)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build_id, first, second = _seed_initial_snapshot(connector)
            _seed_preparation_facts(
                connector,
                gallery_id=1,
                observation_id=1,
                file_sha256=first,
            )
            _seed_preparation_facts(
                connector,
                gallery_id=2,
                observation_id=1,
                file_sha256=second,
            )
    finally:
        connector.close()
    orchestrator = _orchestrator(database)

    def at_preparation(issue: VNextIssuedAnalysisStep) -> bool:
        payload = issue._payload
        return payload is not None and payload.stage == b"impacted_content"

    result, prepared, issued = _drive(
        orchestrator,
        _session(gate, turn),
        build_id,
        stop=at_preparation,
    )
    assert result is None
    assert issued is not None
    payload = issued._payload
    assert payload is not None
    assert payload.memberships == ((1, 1), (2, 1))
    step = orchestrator.prepare_analysis_step(prepared, issued)
    local = step._payload
    assert isinstance(local, _LocalAnalysisWork)
    assert (
        tuple(
            None if item is None else (item.gallery_id, item.observation_id)
            for item in local.preparations
        )
        == payload.memberships
    )
    prepared.close()


def test_renewed_receipt_is_accepted_but_foreign_authority_is_rejected(
    tmp_path: Path,
) -> None:
    database = tmp_path / "analysis-renew.sqlite3"
    build_id, gate, turn = _seed_empty(database)
    facade = VNextIngestFacade(_config(database), clock=_Clock())
    prepared = facade.prepare_analysis(build_id, _policy(), max_rows=8)
    assert isinstance(prepared, VNextPreparedAnalysis)
    issued = facade.issue_analysis_step(_session(gate, turn), prepared)
    assert isinstance(issued, VNextIssuedAnalysisStep)
    local = facade.prepare_analysis_step(prepared, issued)
    assert isinstance(local, VNextPreparedAnalysisStep)

    connector = SQLiteConnector(str(database))
    connector.connect()
    try:
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            renewed_gate = MaintenanceGateRepository.renew(
                work,
                gate,
                now=200,
                lease_duration=2_000_000,
            )
            renewed_turn = IngestFenceRepository.renew(
                work,
                turn,
                now=200,
                lease_duration=2_000_000,
            )
    finally:
        connector.close()
    result = facade.commit_analysis_step(
        _session(renewed_gate, renewed_turn),
        local,
    )
    assert isinstance(result, VNextAnalysisAdvanceResult)
    assert result.analysis_id

    next_issue = facade.issue_analysis_step(
        _session(renewed_gate, renewed_turn),
        prepared,
    )
    next_local = facade.prepare_analysis_step(prepared, next_issue)
    foreign = VNextIngestSession(
        renewed_gate.owner_token,
        renewed_gate.gate_generation,
        renewed_gate.slots[0],
        renewed_gate.lease_expires_at,
        renewed_turn.generation,
        b"x" * 16,
        renewed_turn.lease_expires_at,
        None,
        None,
        None,
        None,
    )
    with pytest.raises(ValueError, match="another ingest session"):
        facade.commit_analysis_step(foreign, next_local)
    prepared.close()


def test_analysis_contract_is_top_level_and_has_no_internal_compatibility_aliases() -> (
    None
):
    assert {
        "VNextAnalysisAdvanceResult",
        "VNextIssuedAnalysisStep",
        "VNextPreparedAnalysis",
        "VNextPreparedAnalysisStep",
    } <= set(h2hdb.__all__)
    assert "VNextIngestAnalysisOrchestrator" not in h2hdb.__all__
    assert tuple(inspect.signature(VNextIngestFacade.prepare_analysis).parameters) == (
        "self",
        "build_id",
        "policy",
        "max_rows",
    )
    assert tuple(
        inspect.signature(VNextIngestFacade.issue_analysis_step).parameters
    ) == ("self", "session", "prepared")
    assert tuple(
        inspect.signature(VNextIngestFacade.prepare_analysis_step).parameters
    ) == ("self", "prepared", "issued")
    assert tuple(
        inspect.signature(VNextIngestFacade.commit_analysis_step).parameters
    ) == ("self", "session", "prepared_step")


def test_restart_after_lost_result_resumes_from_durable_checkpoint(
    tmp_path: Path,
) -> None:
    database = tmp_path / "analysis-restart.sqlite3"
    build_id, gate, turn = _seed_empty(database)
    session = _session(gate, turn)
    orchestrator = _orchestrator(database)
    first = orchestrator.prepare_analysis(build_id, _policy(), max_rows=4)
    issued = orchestrator.issue_analysis_step(session, first)
    step = orchestrator.prepare_analysis_step(first, issued)
    lost = orchestrator.commit_analysis_step(session, step)
    analysis_id = lost.analysis_id
    first.close()

    result, restarted, stopped = _drive(
        orchestrator,
        session,
        build_id,
        max_rows=4,
    )
    assert result is not None and result.terminal
    assert stopped is None
    assert result.analysis_id == analysis_id
    restarted.close()


def test_issued_gallery_page_is_hard_capped_at_128(tmp_path: Path) -> None:
    database = tmp_path / "analysis-cap.sqlite3"
    connector = _generated_database(database)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            scope = _seed_root(connector)
            build_id = b"c" * 16
            _seed_build(
                connector,
                build_id=build_id,
                scope=scope,
                manifest_byte=8,
                gallery_count=129,
            )
            for gallery_id in range(1, 130):
                _seed_gallery(
                    connector,
                    build_id=build_id,
                    scope=scope,
                    gallery_id=gallery_id,
                    observation_id=gallery_id,
                    occurrences=(),
                    artists=(),
                    serial=10_000 + gallery_id,
                )
            _map_working_build(
                connector,
                build_id=build_id,
                generation=turn.generation,
            )
    finally:
        connector.close()
    orchestrator = _orchestrator(database)

    def at_preparation(issue: VNextIssuedAnalysisStep) -> bool:
        payload = issue._payload
        return payload is not None and payload.stage == b"impacted_content"

    result, prepared, issued = _drive(
        orchestrator,
        _session(gate, turn),
        build_id,
        max_rows=128,
        stop=at_preparation,
    )
    assert result is None
    assert issued is not None and issued._payload is not None
    assert len(issued._payload.memberships) == 128
    assert issued._payload.memberships[0] == (1, 1)
    assert issued._payload.memberships[-1] == (128, 128)
    prepared.close()
