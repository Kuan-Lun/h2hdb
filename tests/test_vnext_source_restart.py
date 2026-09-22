"""Recover sealed work independently of a newer filesystem inventory."""

from __future__ import annotations

import json
import logging
from collections import Counter
from collections.abc import Generator
from contextlib import closing, contextmanager
from dataclasses import replace
from typing import Any

import pytest
from test_vnext_source_batches import (
    _publications,
    _publish_batch,
    _source_batch,
    _source_batch_clock,
)
from test_vnext_source_marker import MarkerSource
from test_vnext_source_reread import _InterruptedLibrary
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    MemoryLibrary,
    MemorySource,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
)

from h2hdb import (
    CoreConfig,
    FileContentReceipt,
    VNextIngestFacade,
    VNextSourceChangedError,
    VNextSourceCompletionMarker,
    VNextSourceDeferredError,
)
from h2hdb.sql_connector import SQLConnector


def test_resume_without_work_and_after_publication_requests_fresh_inventory(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        assert facade.prepare_source_resume(source, policy=policy) is None
        receipt, _ = _source_batch(facade, session, policy, source, None)
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        assert (
            facade.commit_source_resume(session, prepared).build_id == receipt.build_id
        )
        run_analysis(facade, session, policy, receipt.build_id)
        run_publication(facade, session, policy, MemoryLibrary(source))
        assert facade.prepare_source_resume(source, policy=policy) is None
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize(
    ("pages_per_gallery", "batch_rows"),
    [(1, 1), pytest.param(43, 128, marks=pytest.mark.deep)],
)
def test_analysis_restart_keeps_committed_cursor_and_ignores_new_gallery(
    db_config: CoreConfig,
    pages_per_gallery: int,
    batch_rows: int,
) -> None:
    initialize_database(db_config)
    values = tuple(
        gallery(
            gid,
            pages=[
                f"page-{page}-of-{gid}".encode() for page in range(pages_per_gallery)
            ],
        )
        for gid in range(1001, 1004)
    )
    source = MarkerSource(values)
    library = MemoryLibrary(source)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _ = _source_batch(facade, session, policy, source, None)
        with facade.prepare_analysis(
            receipt.build_id, policy, max_rows=batch_rows
        ) as analysis:
            for _ in range(100):
                issued = facade.issue_analysis_step(session, analysis)
                local = facade.prepare_analysis_step(analysis, issued)
                result = facade.commit_analysis_step(session, local)
                if result.stage == b"changed_file_hash" and result.processed_rows:
                    assert not result.stage_terminal
                    break
            else:
                pytest.fail("fixture never committed a partial hash-change page")
            original_analysis = result.analysis_id
            issued = facade.issue_analysis_step(session, analysis)
            original_issue = issued._payload
            assert original_issue is not None
            assert original_issue.checkpoint_processed_count == batch_rows
            assert original_issue.checkpoint_cursor
            # Lose the in-memory issued work just as an interrupted worker does.
        facade.complete_ingest(session)
    source.put(gallery(2001))
    source.deep_reads.clear()
    marker_calls = source.marker_calls
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        resumed = facade.commit_source_resume(session, prepared)
        assert resumed.build_id == receipt.build_id
        assert resumed.discovered_galleries == len(values)
        assert facade.commit_source_resume(session, prepared) == resumed
        with facade.prepare_analysis(
            resumed.build_id, policy, max_rows=batch_rows
        ) as analysis:
            issued = facade.issue_analysis_step(session, analysis)
            actual = issued._payload
            assert actual is not None
            assert actual.analysis_id == original_analysis
            assert (
                actual.stage,
                actual.checkpoint_cursor,
                actual.checkpoint_processed_count,
            ) == (
                original_issue.stage,
                original_issue.checkpoint_cursor,
                original_issue.checkpoint_processed_count,
            )
            local = facade.prepare_analysis_step(analysis, issued)
            result = facade.commit_analysis_step(session, local)
            assert result.analysis_id == original_analysis
            assert result.processed_rows == 1 and not result.replayed
        completed = run_analysis(
            facade, session, policy, resumed.build_id, max_rows=batch_rows
        )
        assert completed.analysis_id == original_analysis
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
    assert source.deep_reads == [] and source.marker_calls == marker_calls + len(values)
    assert {value.gid for value in _publications(db_config)} == {
        value.gid for value in values
    }
    assert full_check(db_config).state == "READY"
    fresh_receipt, deferred = _publish_batch(
        db_config,
        source,
        library,
        limit=None,
        artifacts_required=True,
    )
    assert fresh_receipt.build_id != receipt.build_id and deferred == 0
    assert source.deep_reads == [source.get(("gallery-2001",)).locator]
    assert {value.gid for value in _publications(db_config)} == {1001, 1002, 1003, 2001}


def test_artifact_restart_keeps_prepared_bytes_when_inventory_grows(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(gid) for gid in range(1001, 1004))
    source = MarkerSource(values)
    library = _InterruptedLibrary(source)
    library.interrupt_after_first = True
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _ = _source_batch(facade, session, policy, source, None)
        run_analysis(facade, session, policy, receipt.build_id)
        with pytest.raises(VNextSourceChangedError, match="temporarily unavailable"):
            run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
    assert len(library.rendered_gids) == 1
    source.put(gallery(2001))
    source.deep_reads.clear()
    library.interrupt_after_first = False
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        resumed = facade.commit_source_resume(session, prepared)
        assert resumed.build_id == receipt.build_id
        run_analysis(facade, session, policy, resumed.build_id)
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
    assert source.deep_reads == []
    assert Counter(library.rendered_gids) == Counter(value.gid for value in values)
    assert {value.gid for value in _publications(db_config)} == {
        value.gid for value in values
    }
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("change", ["root", "qualification", "analysis", "candidate"])
def test_resume_declines_mismatched_policy_or_root(
    db_config: CoreConfig, change: str
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        requested = ingest_policy()
        policy = facade.ensure_policy(session, requested)
        receipt, _ = _source_batch(facade, session, policy, source, None)
        if change in {"analysis", "candidate"}:
            run_analysis(facade, session, policy, receipt.build_id)
        if change == "candidate":
            issued = facade.issue_publication_step(session, policy)
            library = MemoryLibrary(source)
            local = facade.prepare_publication_step(
                issued,
                artifact_adapters={library.adapter_id: library},
                finalization_adapters={library.adapter_id: library},
                library_activation=library,
            )
            facade.commit_publication_step(session, local)
        if change == "qualification":
            requested = replace(requested, artifacts_required=False)
        elif change == "analysis":
            requested = replace(requested, spam_occurrence_threshold=4)
        elif change == "candidate":
            requested = replace(
                requested,
                artifact=replace(
                    requested.artifact, policy_fingerprint_sha256=b"p" * 32
                ),
            )
        policy = facade.ensure_policy(session, requested)
        if change == "root":
            source._root = ("different", "root")
        assert facade.prepare_source_resume(source, policy=policy) is None
        facade.complete_ingest(session)


@pytest.mark.parametrize(
    "gallery_count",
    [3, *(pytest.param(count, marks=pytest.mark.deep) for count in (127, 128, 129))],
)
def test_resume_qualification_pages_and_commit_cost_do_not_rescan_membership(
    db_config: CoreConfig,
    gallery_count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_database(db_config)
    source = MarkerSource(
        tuple(gallery(1001 + index, pages=[]) for index in range(gallery_count))
    )
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        _source_batch(facade, session, policy, source, None)
        facade.complete_ingest(session)
    pages: list[int] = []
    queries: list[str] = []
    commit_costs: list[int] = []
    with closing(open_connector(db_config)) as connection:
        connector_type = type(connection)
    original_fetch = connector_type.fetch_all
    original_one = connector_type.fetch_one
    original_read = connector_type.read_transaction
    original_marker = source.observe_completion_marker
    expected_locators = {value.locator for value in source.galleries}
    source.put(gallery(9001, pages=[]))
    source.deep_reads.clear()
    discovered_before = source.page_calls
    active_reads = 0
    marker_probes: Counter[tuple[str, ...]] = Counter()

    @contextmanager
    def read_transaction(connector: SQLConnector) -> Generator[None]:
        nonlocal active_reads
        with original_read(connector):
            active_reads += 1
            try:
                yield
            finally:
                active_reads -= 1

    def probe_marker(locator: tuple[str, ...]) -> VNextSourceCompletionMarker:
        assert active_reads == 0, "source marker I/O held a database transaction"
        marker_probes[locator] += 1
        return original_marker(locator)

    def assert_source_cost() -> None:
        assert marker_probes == Counter(dict.fromkeys(expected_locators, 1))
        assert source.deep_reads == []
        assert source.page_calls == discovered_before

    monkeypatch.setattr(connector_type, "read_transaction", read_transaction)
    monkeypatch.setattr(source, "observe_completion_marker", probe_marker)

    def fetch_one(
        connector: SQLConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        queries.append(query)
        return original_one(connector, query, data)

    def fetch_all(
        connector: SQLConnector, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        rows = original_fetch(connector, query, data)
        if (
            "qualification.qualification_policy_sha256" in query
            and "member.gallery_id >" in query
        ):
            pages.append(len(rows))
        return rows

    monkeypatch.setattr(connector_type, "fetch_all", fetch_all)
    monkeypatch.setattr(connector_type, "fetch_one", fetch_one)
    for _ in range(3):
        with (
            _source_batch_clock(db_config) as clock,
            VNextIngestFacade(db_config, clock=clock) as facade,
        ):
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            pages.clear()
            marker_probes.clear()
            prepared = facade.prepare_source_resume(source, policy=policy)
            assert prepared is not None
            assert_source_cost()
            assert sum(pages) == gallery_count
            assert max(pages) <= 128
            assert len(pages) == (gallery_count + 127) // 128 + 1
            pages.clear()
            queries.clear()
            resumed = facade.commit_source_resume(session, prepared)
            assert resumed.discovered_galleries == gallery_count
            assert pages == []
            assert_source_cost()
            commit_costs.append(len(queries))
            # Scalar receipt checks have a fixed query budget independent of
            # gallery count; collection scans belong to preparation only.
            assert len(queries) < 80
            facade.complete_ingest(session)
    assert len(set(commit_costs)) == 1

    def repeated_fetch(
        connector: SQLConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        # Deliberately degrade the real SQL path. The counter oracle must
        # reject a second scan even though returned data remains correct.
        if (
            "qualification.qualification_policy_sha256" in query
            and "member.gallery_id >" in query
        ):
            fetch_all(connector, query, data)
        return fetch_all(connector, query, data)

    monkeypatch.setattr(connector_type, "fetch_all", repeated_fetch)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        pages.clear()
        assert facade.prepare_source_resume(source, policy=policy) is not None
        with pytest.raises(AssertionError):
            assert sum(pages) == gallery_count
        assert sum(pages) == 2 * gallery_count
        facade.complete_ingest(session)
    monkeypatch.setattr(connector_type, "fetch_all", fetch_all)

    def repeated_marker(locator: tuple[str, ...]) -> VNextSourceCompletionMarker:
        # Same correct result, twice the actual adapter I/O: the ordinary cost
        # oracle must reject the deliberately degraded implementation.
        probe_marker(locator)
        return probe_marker(locator)

    monkeypatch.setattr(source, "observe_completion_marker", repeated_marker)
    marker_probes.clear()
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        assert facade.prepare_source_resume(source, policy=policy) is not None
        with pytest.raises(AssertionError):
            assert_source_cost()
        assert marker_probes == Counter(dict.fromkeys(expected_locators, 2))
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


def test_resume_declines_open_build_before_source_seal(db_config: CoreConfig) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(30):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                facade.commit_source_step(session, local)
                if issued._action.value == "ROOT_HANDOFF":
                    break
            else:
                pytest.fail("source fixture did not reserve its working root")
            assert facade.prepare_source_resume(source, policy=policy) is None
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


def test_resume_commit_rejects_replaced_working_cut(db_config: CoreConfig) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        original, _ = _source_batch(facade, session, policy, source, None)
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        facade.complete_ingest(session)
    source.put(gallery(2001))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        replacement, _ = _source_batch(facade, session, policy, source, None)
        assert replacement.build_id != original.build_id
        with pytest.raises(VNextSourceChangedError, match="changed before resume"):
            facade.commit_source_resume(session, prepared)
        fresh = facade.prepare_source_resume(source, policy=policy)
        assert fresh is not None
        assert (
            facade.commit_source_resume(session, fresh).build_id == replacement.build_id
        )
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


def test_resume_commit_rejects_expired_session_authority(db_config: CoreConfig) -> None:
    from h2hdb.vnext_ingest_fence_repository import IngestFenceUnavailableError
    from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateUnavailableError

    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        _source_batch(facade, session, policy, source, None)
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        facade.complete_ingest(session)
        successor = claim_session(facade)
        with pytest.raises(
            (IngestFenceUnavailableError, MaintenanceGateUnavailableError)
        ):
            facade.commit_source_resume(session, prepared)
        current_policy = facade.ensure_policy(successor, ingest_policy())
        fresh = facade.prepare_source_resume(
            source,
            policy=current_policy,
        )
        assert fresh is not None
        assert facade.commit_source_resume(successor, fresh).sealed
        facade.complete_ingest(successor)
    assert full_check(db_config).state == "READY"


def test_resume_rejects_missing_qualification_authority(db_config: CoreConfig) -> None:
    from h2hdb.vnext_source_build_repository import SourceBuildConflictError

    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _ = _source_batch(facade, session, policy, source, None)
        with closing(open_connector(db_config)) as connector, connector.transaction():
            member = connector.fetch_one(
                "SELECT gallery_id, observation_id FROM catalog_source_build_galleries WHERE build_id = %s",
                (receipt.build_id,),
            )
            connector.execute(
                "DELETE FROM catalog_gallery_observation_validation_policies WHERE gallery_id = %s AND observation_id = %s",
                member,
            )
        with pytest.raises(
            SourceBuildConflictError, match="qualification authority is missing"
        ):
            facade.prepare_source_resume(source, policy=policy)
        facade.complete_ingest(session)


def test_resume_proof_cannot_be_rebound_to_another_qualification_policy(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        _source_batch(facade, session, policy, source, None)
        prepared = facade.prepare_source_resume(source, policy=policy)
        assert prepared is not None
        altered = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        # Frozen Python wrappers are not an authority boundary. Replacing the
        # policy must not reuse qualification evidence for the former policy.
        object.__setattr__(prepared, "_policy", altered)
        with pytest.raises(VNextSourceChangedError, match="changed before resume"):
            facade.commit_source_resume(session, prepared)
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize(
    "change",
    [
        "bytes",
        "device",
        "inode",
        "modified_ns",
        "changed_ns",
        "version",
        "none",
        "deferred",
    ],
)
def test_resume_declines_changed_or_unavailable_existing_completion_marker(
    db_config: CoreConfig,
    change: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        _source_batch(facade, session, policy, source, None)
        facade.complete_ingest(session)
    original = source.observe_completion_marker
    source.deep_reads.clear()
    locator_pages = source.page_calls
    probed: list[tuple[str, ...]] = []

    def altered_marker(locator: tuple[str, ...]) -> VNextSourceCompletionMarker | None:
        probed.append(locator)
        if change == "deferred":
            raise VNextSourceDeferredError("producer marker temporarily unavailable")
        if change == "none":
            return None
        marker = original(locator)
        if change == "version":
            return replace(marker, observation_version=marker.observation_version + 1)
        if change == "bytes":
            file = replace(
                marker.file,
                content=FileContentReceipt.from_parts((b"different marker bytes",)),
            )
        else:
            file = replace(marker.file, **{change: getattr(marker.file, change) + 1})
        return replace(marker, file=file)

    monkeypatch.setattr(source, "observe_completion_marker", altered_marker)
    caplog.set_level(logging.INFO, logger="h2hdb.database_performance")
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        assert facade.prepare_source_resume(source, policy=policy) is None
        facade.complete_ingest(session)
    diagnostics = [
        json.loads(record.getMessage().removeprefix("database_performance "))
        for record in caplog.records
        if record.name == "h2hdb.database_performance"
        and record.getMessage().startswith("database_performance ")
    ]
    terminal = next(
        event
        for event in reversed(diagnostics)
        if event["operation"] == "source_resume_prepare"
        and event["event"] == "completed"
    )
    assert terminal["labels"]["resumable"] is False
    assert terminal["labels"]["completion_marker_probes"] == 1
    assert terminal["labels"]["completion_markers_matched"] == 0
    assert terminal["labels"]["resume_reason"] == {
        "deferred": "marker_deferred",
        "none": "marker_absent",
    }.get(change, "marker_mismatch")
    assert probed == [("gallery-1001",)]
    assert source.deep_reads == [] and source.page_calls == locator_pages
    assert full_check(db_config).state == "READY"


def test_resume_declines_markerless_sealed_observation(db_config: CoreConfig) -> None:
    initialize_database(db_config)
    source = MemorySource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(1000):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                result = facade.commit_source_step(session, local)
                if result.terminal:
                    assert result.source_receipt is not None
                    assert result.source_receipt.sealed
                    break
            else:
                pytest.fail("markerless source did not seal")
        assert facade.prepare_source_resume(source, policy=policy) is None
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"
