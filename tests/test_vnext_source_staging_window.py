"""Durable assembly windows bound source selection without a second cursor.

All workflow fixtures enter through public ingestion with FKs enabled. Native
oracles and deliberate corruption are test-only; full READY follows successful
publication. Native budgets and the old executed selector are shared with the
standalone manual experiment, not inferred from measured wall time.
"""

from __future__ import annotations

import importlib.util
import json
import multiprocessing
import os
import sys
from collections.abc import Iterator
from contextlib import closing, nullcontext
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest
from test_vnext_pipeline_takeover_matrix import FENCE_ERRORS
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
    takeover_clock,
)
from vnext_source_staging_cost import WINDOW, budget_failures, observe_pending_queries
from vnext_test_database import assert_foreign_key_integrity

from h2hdb import CoreConfig, VNextCatalogFacade, VNextIngestFacade
from h2hdb import vnext_ingest_facade as facade_module
from h2hdb.vnext_source_build_repository import (
    _PENDING_ASSEMBLY_GALLERY_QUERY,
    PendingSourceGallery,
    SourceBuildConflictError,
    SourceBuildRepository,
)

pytestmark = pytest.mark.deep


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "source_staging_window_probe_under_test"
    path = (
        Path(__file__).resolve().parents[1] / "scripts/source_staging_window_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    old_path = sys.path[:]
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = old_path
        del sys.modules[name]


@pytest.mark.performance_acceptance
@pytest.mark.parametrize("count", [255, 256, 257])
def test_public_source_window_capacity_has_fixed_native_budget(
    db_config: CoreConfig, probe: ModuleType, count: int, tmp_path: Path
) -> None:
    result = probe.run_case(db_config, count, degraded=False)
    (tmp_path / "source-cost.json").write_text(json.dumps(result, indent=2) + "\n")
    assert result["budget_passed"]
    samples = result["source_native"]["samples"]
    assert len(samples) == count + (count + WINDOW - 1) // WINDOW + 1
    assert {sample["window_start"] for sample in samples} == {
        *range(0, count, WINDOW),
        count,
    }
    assert all(
        0 <= sample["window_end"] - sample["window_start"] <= WINDOW
        for sample in samples
    )
    assert result["READY"] == "READY"


@pytest.mark.performance_acceptance
@pytest.mark.parametrize("degraded", [False, True])
def test_repeated_windows_reject_the_executed_unbounded_prefix_control(
    db_config: CoreConfig, probe: ModuleType, tmp_path: Path, *, degraded: bool
) -> None:
    result = probe.run_case(db_config, 513, degraded=degraded)
    (tmp_path / "source-cost.json").write_text(json.dumps(result, indent=2) + "\n")
    assert result["READY"] == "READY" and result["publication_count"] == 513
    assert result["budget_passed"] is not degraded
    assert bool(result["budget_failures"]) is degraded


def _source(count: int) -> MemorySource:
    return MemorySource(
        [
            gallery(gid, pages=[f"page-{gid}".encode()], artists=(), language=None)
            for gid in range(1, count + 1)
        ]
    )


def _finish_prepared(facade: Any, session: Any, policy: Any, prepared: Any) -> Any:
    for _ in range(100_000):
        issued = facade.issue_source_step(session, policy, prepared)
        step = facade.prepare_source_step(prepared, issued)
        result = facade.commit_source_step(session, step)
        if result.terminal:
            assert result.source_receipt is not None
            return result.source_receipt
    raise AssertionError("public source did not seal")


def test_a_later_link_does_not_hide_an_earlier_hole_in_the_same_window(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    source = _source(3)
    original = SourceBuildRepository.get_pending_assembly_gallery
    selected: list[int] = []

    def select_later_once(connector: Any, *, build_id: bytes) -> Any:
        pending = original(connector, build_id=build_id)
        if pending is not None and not selected:
            # Out-of-order attachment remains a real, complete public commit.
            # Select one existing expected member; never fabricate graph facts.
            row = connector.fetch_one(_PENDING_ASSEMBLY_GALLERY_QUERY, (build_id, 1, 2))
            assert row and pending.position == 0
            pending = PendingSourceGallery(build_id, *row)
        if pending is not None:
            selected.append(pending.position)
        return pending

    monkeypatch.setattr(
        SourceBuildRepository, "get_pending_assembly_gallery", select_later_once
    )
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        assert selected == [1, 0, 2]
        assert receipt.discovered_galleries == receipt.staged_galleries == 3
        run_analysis(facade, session, policy, receipt.build_id)
        run_publication(facade, session, policy, MemoryLibrary(source))
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


def test_changed_admission_limit_rebuilds_pending_cut_then_preserves_known_galleries(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = _source(3)
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(source, policy=policy, max_new_galleries=2) as cut:
            for _ in range(10_000):
                if cut._machine.action.value == "STAGING_FIND":
                    break
                issued = facade.issue_source_step(session, policy, cut)
                step = facade.prepare_source_step(cut, issued)
                facade.commit_source_step(session, step)
            else:
                pytest.fail("first cut did not enter its assembly window")
            first_build = cut._machine.build_id
            assert first_build is not None and cut.deferred_gallery_count == 1
        facade.complete_ingest(session)

    # A configuration change belongs to the next ingest generation. The old
    # OPEN build remains durable after cooperative session completion; expiry
    # and abandoned-owner takeover are independently covered by the spawn case.
    with VNextIngestFacade(db_config, clock=Clock()) as restarted:
        session = claim_session(restarted)
        policy = restarted.ensure_policy(
            session, ingest_policy(artifacts_required=False)
        )
        with restarted.prepare_source(
            source, policy=policy, max_new_galleries=1
        ) as cut:
            receipt = _finish_prepared(restarted, session, policy, cut)
            assert cut.deferred_gallery_count == 2
        assert receipt.build_id != first_build
        assert receipt.discovered_galleries == receipt.staged_galleries == 1
        run_analysis(restarted, session, policy, receipt.build_id)
        run_publication(restarted, session, policy, library)
        restarted.complete_ingest(session)
        drain_maintenance(restarted)
        session = claim_session(restarted)
        policy = restarted.ensure_policy(
            session, ingest_policy(artifacts_required=False)
        )
        with restarted.prepare_source(
            source, policy=policy, max_new_galleries=2
        ) as cut:
            receipt = _finish_prepared(restarted, session, policy, cut)
            assert cut.deferred_gallery_count == 0
        # The limit constrains newly admitted galleries, not the total cut.
        assert receipt.discovered_galleries == receipt.staged_galleries == 3
        run_analysis(restarted, session, policy, receipt.build_id)
        run_publication(restarted, session, policy, library)
        restarted.complete_ingest(session)
    with closing(VNextCatalogFacade(db_config)) as catalog:
        assert catalog.get_catalog_revision().publication_count == 3
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("degraded", [False, True])
@pytest.mark.parametrize("budget_passed", [False, True])
def test_probe_cli_preserves_the_actual_cost_result(
    probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    degraded: bool,
    budget_passed: bool,
) -> None:
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe",
            "--backend",
            "sqlite",
            "--galleries",
            "513",
            "--output",
            str(output),
            *(["--degraded"] if degraded else []),
        ],
    )
    result = {
        "backend": "sqlite",
        "galleries": 513,
        "degraded_unbounded_control": degraded,
        "budget_passed": budget_passed,
        "publication_count": 513,
        "READY": "READY",
    }
    monkeypatch.setattr(probe, "database", lambda *_: nullcontext(object()))
    monkeypatch.setattr(probe, "run_case", lambda *_, **__: result)
    with pytest.raises(SystemExit) as exit_info:
        probe.main()
    assert exit_info.value.code == (0 if budget_passed else 1)
    assert json.loads(output.read_text()) == result


def test_probe_cli_records_incomplete_without_driver_secrets(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["probe", "--backend", "sqlite", "--galleries", "1", "--output", str(output)],
    )
    monkeypatch.setattr(probe, "database", lambda *_: nullcontext(object()))

    def failed(*_: Any, **__: Any) -> None:
        raise RuntimeError("driver secret must never enter evidence")

    monkeypatch.setattr(probe, "run_case", failed)
    with pytest.raises(SystemExit) as exit_info:
        probe.main()
    assert exit_info.value.code == 2
    result = json.loads(output.read_text())
    assert result["status"] == "incomplete" and result["budget_passed"] is None
    assert result["error_type"] == "RuntimeError"
    assert "driver secret" not in output.read_text()


class _LostResponse(RuntimeError):
    pass


def _commit_assembly_then_exit(
    config: CoreConfig, count: int, terminal: bool, pipe: Any
) -> None:
    # This spawned process owns a different interpreter and no facade/connector
    # state survives its exit. Check the actual child database authority too.
    with closing(open_connector(config)) as connector:
        if config.database.sql_type == "mariadb":
            authority = connector.fetch_one("SELECT DATABASE()")
            assert authority == (config.database.database,)
            assert "MariaDB" in connector.fetch_one("SELECT VERSION()")[0]
        else:
            assert connector.fetch_one("SELECT sqlite_version()")
    source = _source(count)
    apply = facade_module._apply_source_outcome
    lost: Any = None

    def lose_after_commit(prepared: Any, step: Any, outcome: Any) -> tuple[int, bool]:
        nonlocal lost
        if (
            step._action.value == "ASSEMBLY"
            and outcome.terminal is terminal
            and lost is None
        ):
            lost = outcome
            raise _LostResponse
        return apply(prepared, step, outcome)

    with VNextIngestFacade(config, clock=Clock()) as facade:
        old_session = claim_session(facade)
        policy = facade.ensure_policy(
            old_session, ingest_policy(artifacts_required=False)
        )
        with (
            patch.object(facade_module, "_apply_source_outcome", lose_after_commit),
            pytest.raises(_LostResponse),
        ):
            run_source(facade, old_session, policy, source, step_budget=100_000)
    pipe.send((os.getpid(), lost, old_session))
    pipe.close()


@pytest.mark.parametrize("terminal", [False, True])
def test_assembly_response_loss_restarts_from_durable_prefix_and_fences_old_session(
    db_config: CoreConfig, *, terminal: bool
) -> None:
    initialize_database(db_config)
    count = 1 if terminal else 257
    source = _source(count)
    library = MemoryLibrary(source)
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_commit_assembly_then_exit, args=(db_config, count, terminal, send)
    )
    process.start()
    send.close()
    try:
        assert receive.poll(600), "source commit child did not finish in 600 seconds"
        child_pid, lost, old_session = receive.recv()
        process.join(10)
        assert not process.is_alive() and process.exitcode == 0
        assert child_pid != os.getpid()
    finally:
        if process.is_alive():
            process.kill()
            process.join(10)
        receive.close()
    assert lost is not None and lost.terminal is terminal
    if not terminal:
        assert lost.next_gallery_count == WINDOW

    with VNextIngestFacade(db_config) as before_expiry:
        assert before_expiry.try_claim_ingest(True, 10**9) is None

    with VNextIngestFacade(db_config, clock=takeover_clock()) as restarted:
        session = claim_session(restarted)
        policy = restarted.ensure_policy(
            session, ingest_policy(artifacts_required=False)
        )
        with pytest.raises(FENCE_ERRORS):
            restarted.ensure_policy(
                old_session, ingest_policy(artifacts_required=False)
            )
        with observe_pending_queries(db_config.database.sql_type) as work:
            receipt = run_source(
                restarted, session, policy, source, step_budget=100_000
            )
        assert receipt.sealed and receipt.build_id == lost.build_id
        assert receipt.staged_galleries == receipt.discovered_galleries == count
        if terminal:
            assert not work.samples
        else:
            assert work.samples and all(
                sample.window_start is not None and sample.window_start >= WINDOW
                for sample in work.samples
            )
            assert not budget_failures(work.samples, db_config.database.sql_type)
        run_analysis(restarted, session, policy, receipt.build_id, step_budget=100_000)
        run_publication(restarted, session, policy, library, step_budget=100_000)
        restarted.complete_ingest(session)
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("fault", ["genesis", "receipt"])
def test_forged_assembly_frontier_cannot_skip_expected_work(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    initialize_database(db_config)
    source = _source(1)
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(1000):
                if prepared._machine.action.value == "STAGING_FIND":
                    break
                issued = facade.issue_source_step(session, policy, prepared)
                step = facade.prepare_source_step(prepared, issued)
                facade.commit_source_step(session, step)
            else:
                pytest.fail("public source did not reach first assembly window")
            build = prepared._machine.build_id
            assert build is not None
            with closing(open_connector(db_config)) as connector:
                original = connector.fetch_one

                def corrupt(query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
                    row = original(query, data)
                    if (
                        "state, updated_at FROM operational_source_build_assembly_checkpoints"
                        in query
                    ):
                        return (
                            1 if fault == "genesis" else 2,
                            (0).to_bytes(8, "big"),
                            1,
                            *row[3:],
                        )
                    return row

                monkeypatch.setattr(connector, "fetch_one", corrupt)
                with (
                    connector.read_transaction(),
                    pytest.raises(SourceBuildConflictError),
                ):
                    SourceBuildRepository.get_pending_assembly_gallery(
                        connector, build_id=build
                    )
                monkeypatch.setattr(connector, "fetch_one", original)
                assert_foreign_key_integrity(connector)
