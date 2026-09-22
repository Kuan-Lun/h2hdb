"""Process-local validation plans preserve the durable ingest lifecycle."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from test_vnext_analysis_repository import (
    _authorities,
    _generated_database,
    _seed_initial_snapshot,
)
from test_vnext_ingest_analysis import _drive, _orchestrator, _session

import h2hdb.vnext_ingest_analysis as orchestration
from h2hdb import VNextIngestSession, VNextIssuedAnalysisStep, VNextPreparedAnalysis
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import (
    AnalysisCorruptionError,
    AnalysisFileDecisionValidationPlan,
    AnalysisRepository,
)
from h2hdb.vnext_ingest_analysis import VNextIngestAnalysisOrchestrator
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_STAGE = b"validate_file_hash_decision"


def _at_validation(issue: VNextIssuedAnalysisStep) -> bool:
    return issue._payload is not None and issue._payload.stage == _STAGE


def _seed(path: Path) -> tuple[bytes, GateLease, IngestTurn]:
    connector = _generated_database(path)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        return build, gate, turn
    finally:
        connector.close()


def _start(
    path: Path,
) -> tuple[
    VNextIngestAnalysisOrchestrator,
    VNextIngestSession,
    VNextPreparedAnalysis,
    VNextIssuedAnalysisStep,
    bytes,
]:
    build, gate, turn = _seed(path)
    driver = _orchestrator(path)
    session = _session(gate, turn)
    _result, prepared, issued = _drive(
        driver, session, build, max_rows=1, stop=_at_validation
    )
    assert issued is not None
    return driver, session, prepared, issued, build


@pytest.fixture
def plans(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    list[AnalysisFileDecisionValidationPlan], list[AnalysisFileDecisionValidationPlan]
]:
    created: list[AnalysisFileDecisionValidationPlan] = []
    closed: list[AnalysisFileDecisionValidationPlan] = []
    original_create = AnalysisRepository.prepare_file_decision_validation_plan
    original_close = AnalysisFileDecisionValidationPlan.close

    def create(*args: Any, **kwargs: Any) -> AnalysisFileDecisionValidationPlan:
        plan = original_create(*args, **kwargs)
        created.append(plan)
        return plan

    def close(plan: AnalysisFileDecisionValidationPlan) -> None:
        closed.append(plan)
        original_close(plan)

    monkeypatch.setattr(
        AnalysisRepository,
        "prepare_file_decision_validation_plan",
        staticmethod(create),
    )
    monkeypatch.setattr(AnalysisFileDecisionValidationPlan, "close", close)
    return created, closed


def test_one_plan_is_reused_across_pages_and_closed_at_next_preparation(
    tmp_path: Path,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
) -> None:
    driver, session, prepared, issued, _build = _start(tmp_path / "reuse.sqlite3")
    created, closed = plans
    with prepared:
        row_counts: list[int] = []
        for _ in range(4):
            step = driver.prepare_analysis_step(prepared, issued)
            assert prepared._machine.validation_plan is created[0]
            assert len(created) == 1 and not closed
            result = driver.commit_analysis_step(session, step)
            row_counts.append(result.processed_rows)
            if result.stage_terminal:
                break
            issued = driver.issue_analysis_step(session, prepared)
        assert row_counts == [1, 1, 0]
        assert prepared._machine.validation_plan is None
        assert not closed
        following = driver.issue_analysis_step(session, prepared)
        assert following._payload is not None and following._payload.stage != _STAGE
        assert not closed
        driver.prepare_analysis_step(prepared, following)
        assert closed == created
    assert closed == created


def test_reused_plan_page_preparation_opens_no_database_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, session, prepared, issued, _build = _start(tmp_path / "local-page.sqlite3")
    with prepared:
        driver.commit_analysis_step(
            session, driver.prepare_analysis_step(prepared, issued)
        )
        next_issue = driver.issue_analysis_step(session, prepared)

        def reject_connection(*_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("local page preparation opened a database connection")

        with monkeypatch.context() as fault:
            fault.setattr(SQLiteConnector, "__init__", reject_connection)
            step = driver.prepare_analysis_step(prepared, next_issue)
        assert driver.commit_analysis_step(session, step).processed_rows == 1


def test_reissued_committed_batch_carries_actual_prefix_from_its_start_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reissued-prefix.sqlite3"
    driver, session, prepared, issued, _build = _start(path)
    assert issued._payload is not None and issued._payload.batch_key is not None
    initial = issued._payload
    with prepared, SQLiteConnector(str(path)) as connector:
        first = driver.commit_analysis_step(
            session, driver.prepare_analysis_step(prepared, issued)
        )
        assert first.processed_rows == 1 and not first.stage_terminal
        plan = prepared._machine.validation_plan
        assert plan is not None
        gate, turn = orchestration._repository_authority(session)
        assert initial.batch_key is not None
        with connector.transaction():
            replay_issue = AnalysisRepository.issue_next_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=initial.analysis_id,
                batch_key=initial.batch_key,
                max_rows=128,
                now=200,
            )
        assert replay_issue.replayed_result is not None
        assert replay_issue.page_limit == initial.page_limit == 1
        assert replay_issue.checkpoint_cursor == initial.checkpoint_cursor
        assert (
            replay_issue.file_decision_actual_keys == initial.file_decision_actual_keys
        )
        page = AnalysisRepository.prepare_file_decision_validation_page(
            issue=replay_issue, plan=plan
        )
        with connector.transaction():
            replay = AnalysisRepository.process_issued_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                issue=replay_issue,
                preparations=(),
                file_decision_validation=page,
                now=201,
            )
        assert replay.replayed and replay.row_count == 1


@pytest.mark.parametrize("tamper_issue", (False, True))
def test_commit_fresh_actual_prefix_rejects_new_or_omitted_issued_orphan(
    tmp_path: Path, *, tamper_issue: bool
) -> None:
    path = tmp_path / "fresh-actual.sqlite3"
    driver, session, prepared, issued, _build = _start(path)
    assert issued._payload is not None
    payload = issued._payload
    extra = bytes(32)
    with prepared, SQLiteConnector(str(path)) as connector:
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 1)",
                (extra,),
            )
            connector.execute(
                "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
                "(analysis_id, file_sha256) VALUES (%s, %s)",
                (payload.analysis_id, extra),
            )
        if tamper_issue:
            gate, turn = orchestration._repository_authority(session)
            assert payload.batch_key is not None
            with connector.transaction():
                exact = AnalysisRepository.issue_next_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=payload.analysis_id,
                    batch_key=payload.batch_key,
                    max_rows=payload.page_limit,
                    now=200,
                )
            assert extra in exact.file_decision_actual_keys
            issued._payload = replace(
                exact,
                file_decision_actual_keys=tuple(
                    key for key in exact.file_decision_actual_keys if key != extra
                ),
            )
        before = connector.fetch_all(
            "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s", (_STAGE,)
        )
        step = driver.prepare_analysis_step(prepared, issued)
        with pytest.raises(AnalysisCorruptionError, match="prefix changed"):
            driver.commit_analysis_step(session, step)
        assert (
            connector.fetch_all(
                "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s", (_STAGE,)
            )
            == before
        )


@pytest.mark.parametrize("terminal", [False, True])
def test_committed_response_loss_reuses_exact_page_and_plan_until_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
    *,
    terminal: bool,
) -> None:
    driver, session, prepared, issued, _build = _start(tmp_path / "lost.sqlite3")
    created, closed = plans
    original = orchestration._apply_commit_outcome
    lost = False

    def lose_result(*args: Any, **kwargs: Any) -> Any:
        nonlocal lost
        outcome = args[4]
        if outcome.stage == _STAGE and outcome.terminal == terminal and not lost:
            lost = True
            raise OSError("lost committed validation response")
        return original(*args, **kwargs)

    with prepared:
        for _ in range(3):
            step = driver.prepare_analysis_step(prepared, issued)
            with monkeypatch.context() as faults:
                faults.setattr(orchestration, "_apply_commit_outcome", lose_result)
                try:
                    driver.commit_analysis_step(session, step)
                except OSError as error:
                    assert str(error) == "lost committed validation response"
                    break
            issued = driver.issue_analysis_step(session, prepared)
        else:
            raise AssertionError("validation response-loss boundary was not reached")
        assert lost and len(created) == 1 and not closed
        assert driver.prepare_analysis_step(prepared, issued) is step
        replay = driver.commit_analysis_step(session, step)
        assert replay.replayed and replay.stage_terminal == terminal
        assert len(created) == 1
        assert not closed
    assert closed == created


def test_restart_discards_plan_and_rebuilds_at_durable_validation_cursor(
    tmp_path: Path,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
) -> None:
    path = tmp_path / "restart.sqlite3"
    driver, session, prepared, issued, build = _start(path)
    created, closed = plans
    first = driver.commit_analysis_step(
        session, driver.prepare_analysis_step(prepared, issued)
    )
    assert first.processed_rows == 1
    prepared.close()
    assert closed == created and len(created) == 1
    restarted_driver = _orchestrator(path)
    _result, restarted, next_issue = _drive(
        restarted_driver, session, build, max_rows=1, stop=_at_validation
    )
    assert next_issue is not None
    with restarted:
        second = restarted_driver.commit_analysis_step(
            session, restarted_driver.prepare_analysis_step(restarted, next_issue)
        )
        assert second.analysis_id == first.analysis_id
        assert second.processed_rows == 1 and not second.replayed
        assert len(created) == 2 and created[0] is not created[1]
        terminal_issue = restarted_driver.issue_analysis_step(session, restarted)
        terminal = restarted_driver.commit_analysis_step(
            session, restarted_driver.prepare_analysis_step(restarted, terminal_issue)
        )
        assert terminal.stage_terminal and terminal.processed_rows == 0
    assert closed == created


def test_failed_page_preparation_discards_plan_and_preserves_original_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
) -> None:
    driver, session, prepared, issued, _build = _start(
        tmp_path / "prepare-failure.sqlite3"
    )
    created, closed = plans

    def fail_page(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("injected local page failure")

    with prepared:
        with monkeypatch.context() as faults:
            faults.setattr(
                AnalysisRepository,
                "prepare_file_decision_validation_page",
                staticmethod(fail_page),
            )
            with pytest.raises(OSError, match="injected local page failure"):
                driver.prepare_analysis_step(prepared, issued)
        assert prepared._machine.validation_plan is None
        assert len(created) == 1 and closed == created
        step = driver.prepare_analysis_step(prepared, issued)
        assert len(created) == 2
        assert driver.commit_analysis_step(session, step).processed_rows == 1
    assert closed == created


def test_removed_source_seal_cannot_commit_a_prepared_validation_page(
    tmp_path: Path,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
) -> None:
    path = tmp_path / "authority.sqlite3"
    driver, session, prepared, issued, build = _start(path)
    created, closed = plans
    with prepared:
        step = driver.prepare_analysis_step(prepared, issued)
        with SQLiteConnector(str(path)) as connector:
            before = connector.fetch_all(
                "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s", (_STAGE,)
            )
            connector.execute(
                "DELETE FROM catalog_source_build_sealed_ats WHERE build_id = %s",
                (build,),
            )
        with pytest.raises(RuntimeError, match="sealed|source|build"):
            driver.commit_analysis_step(session, step)
        with SQLiteConnector(str(path)) as connector:
            assert (
                connector.fetch_all(
                    "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s",
                    (_STAGE,),
                )
                == before
            )
    assert closed == created


def test_replaced_lease_cannot_use_an_earlier_sessions_validation_plan(
    tmp_path: Path,
    plans: tuple[
        list[AnalysisFileDecisionValidationPlan],
        list[AnalysisFileDecisionValidationPlan],
    ],
) -> None:
    path = tmp_path / "replaced-lease.sqlite3"
    driver, session, prepared, issued, _build = _start(path)
    created, closed = plans
    with prepared:
        step = driver.prepare_analysis_step(prepared, issued)
        takeover_now = session.ingest_lease_expires_at + 1
        with SQLiteConnector(str(path)) as connector:
            before = connector.fetch_all(
                "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s", (_STAGE,)
            )
            with connector.transaction():
                gate = MaintenanceGateRepository.claim_shared(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    now=takeover_now,
                    lease_duration=2_000_000,
                )
            with connector.transaction():
                turn = IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=b"replacement-turn",
                    now=takeover_now,
                    lease_duration=2_000_000,
                )
        with pytest.raises(MaintenanceGateUnavailableError):
            driver.commit_analysis_step(session, step)
        with pytest.raises(ValueError, match="another ingest session"):
            driver.commit_analysis_step(_session(gate, turn), step)
        with SQLiteConnector(str(path)) as connector:
            assert (
                connector.fetch_all(
                    "SELECT * FROM catalog_analysis_checkpoints WHERE stage = %s",
                    (_STAGE,),
                )
                == before
            )
    assert closed == created


def test_slow_local_validation_preparation_allows_renewal_and_updated_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "renew.sqlite3"
    build, gate, turn = _seed(path)
    driver = _orchestrator(path)
    session = _session(gate, turn)
    _result, prepared, issued = _drive(
        driver, session, build, max_rows=1, stop=_at_validation
    )
    assert issued is not None
    ready = Event()
    resumed = Event()
    original = AnalysisRepository.prepare_file_decision_validation_plan

    def slow_prepare(*args: Any, **kwargs: Any) -> AnalysisFileDecisionValidationPlan:
        report = kwargs["progress"]

        def pause_at_read_boundary(galleries: int) -> None:
            report(galleries)
            if not ready.is_set():
                ready.set()
                if not resumed.wait(timeout=5):
                    raise AssertionError("independent session renewal did not complete")

        kwargs["progress"] = pause_at_read_boundary
        return original(*args, **kwargs)

    monkeypatch.setattr(
        AnalysisRepository,
        "prepare_file_decision_validation_plan",
        staticmethod(slow_prepare),
    )
    with prepared, ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(driver.prepare_analysis_step, prepared, issued)
        try:
            assert ready.wait(timeout=5)
            with SQLiteConnector(str(path)) as connector:
                with connector.transaction():
                    work = VNextUnitOfWork(connector, backend="sqlite")
                    renewed_gate = MaintenanceGateRepository.renew(
                        work, gate, now=200, lease_duration=2_000_000
                    )
                    renewed_turn = IngestFenceRepository.renew(
                        work, turn, now=200, lease_duration=2_000_000
                    )
        finally:
            resumed.set()
        step = future.result(timeout=5)
        result = driver.commit_analysis_step(_session(renewed_gate, renewed_turn), step)
        assert result.processed_rows == 1 and not result.replayed
