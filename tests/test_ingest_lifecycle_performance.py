"""Real lifecycle diagnostics reconcile physical calls and reject missing scopes."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pytest
from test_vnext_source_marker import MarkerSource
from vnext_pipeline import (
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
)

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.config_loader import LoggerConfig
from h2hdb.database_performance import DatabasePerformance
from h2hdb.ingest_performance import IngestPerformance, PerformanceStep
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.vnext_gallery_staging_repository import GalleryObservationStagingRepository


@dataclass
class _Timing:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class _PhysicalCalls:
    calls: int = 0
    rows: int = 0

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        del elapsed, query
        if category == "sql":
            self.calls += 1
            self.rows += rows


def _database_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    return [
        json.loads(record.getMessage().removeprefix("database_performance "))
        for record in caplog.records
        if record.name == "h2hdb.database_performance"
        and record.getMessage().startswith("database_performance ")
    ]


def _source_summary(caplog: pytest.LogCaptureFixture) -> str:
    summaries = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
        and record.name == "h2hdb.ingest_performance"
        and record.getMessage().startswith("Ingest source stage finished:")
    ]
    assert len(summaries) == 1
    return summaries[0]


def _require_source_coverage(summary: str, observed: _PhysicalCalls) -> None:
    for action in ("FILE_PAGE", "DIRECTORY_PAGE", "TAG_PAGE", "METADATA_PAGE"):
        for phase in ("issue", "prepare", "commit"):
            assert f"{action}.{phase}:" in summary, (action, phase)
    assert f"; {observed.calls} completed SQL connector calls;" in summary
    assert f"; {observed.rows} rows returned (not rows examined);" in summary


def _transaction_log_violations(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    active = 0
    violations: list[str] = []
    original_handle = logging.Logger.handle

    def handle(logger: logging.Logger, record: logging.LogRecord) -> None:
        if active and record.name in {
            "h2hdb.ingest_performance",
            "h2hdb.database_performance",
        }:
            violations.append(record.getMessage())
        original_handle(logger, record)

    def instrument_transaction(name: str) -> None:
        original = getattr(SQLConnector, name)

        @contextmanager
        def transaction(connector: SQLConnector) -> Iterator[None]:
            nonlocal active
            with original(connector):
                active += 1
                try:
                    yield
                finally:
                    active -= 1

        monkeypatch.setattr(SQLConnector, name, transaction)

    instrument_transaction("transaction")
    instrument_transaction("read_transaction")
    monkeypatch.setattr(logging.Logger, "handle", handle)
    return violations


@pytest.mark.parametrize("remove_tag_scope", [False, True])
def test_real_source_info_reconciles_work_and_rejects_missing_scope(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    remove_tag_scope: bool,
) -> None:
    """A slow TAG commit must be visible without DEBUG or per-gallery chatter.

    The negative control removes the actual TAG commit measurement while
    retaining every database write and result; the same coverage oracle must
    reject it. SQL calls/returned rows are reconciled against an independent
    outer observer, not calculated from the telemetry implementation.
    """
    initialize_database(db_config)
    transaction_logs = _transaction_log_violations(monkeypatch)
    timing = _Timing()
    original_tags = GalleryObservationStagingRepository.put_tags

    def slow_tags(*args: Any, **kwargs: Any) -> Any:
        result = original_tags(*args, **kwargs)
        timing.now += 7.25
        return result

    monkeypatch.setattr(GalleryObservationStagingRepository, "put_tags", slow_tags)
    if remove_tag_scope:
        original_step = IngestPerformance.step

        @contextmanager
        def remove_scope(
            owner: IngestPerformance,
            pipeline: str,
            phase: str,
            operation: str,
            generation: int,
            *,
            correlation_id: str | None = None,
        ) -> Iterator[PerformanceStep]:
            if pipeline == "source" and phase == "TAG_PAGE.commit":
                # The returned sample is never installed as an active observer.
                yield PerformanceStep(
                    owner,
                    pipeline,
                    phase,
                    operation,
                    generation,
                    None,
                    (0, None),
                    None,
                )
                return
            with original_step(
                owner,
                pipeline,
                phase,
                operation,
                generation,
                correlation_id=correlation_id,
            ) as sample:
                yield sample

        monkeypatch.setattr(IngestPerformance, "step", remove_scope)
    observed = _PhysicalCalls()
    source = MarkerSource(
        tuple(
            gallery(
                1000 + index,
                pages=[b"one-page"],
                directories=(b"empty",),
                extra_tags=(("group", "shared"),),
            )
            for index in range(3)
        )
    )
    config = db_config.model_copy(
        update={"logger": LoggerConfig.model_validate({"level": "info"})}
    )
    with caplog.at_level(logging.INFO, logger="h2hdb"):
        with VNextIngestFacade(config) as facade:
            performance = cast(
                IngestPerformance, cast(Any, facade)._VNextIngestFacade__performance
            )
            performance.clock = timing
            session = claim_session(facade)
            policy = facade.ensure_policy(
                session, ingest_policy(artifacts_required=False)
            )
            with facade.prepare_source(source, policy=policy) as prepared:
                with measure_sql(observed, observe_nested=True):
                    for _ in range(1000):
                        issued = facade.issue_source_step(session, policy, prepared)
                        # Replay must retain the same authority while appearing
                        # in diagnostics as reuse, including local preparation.
                        assert (
                            facade.issue_source_step(session, policy, prepared)
                            is issued
                        )
                        local = facade.prepare_source_step(prepared, issued)
                        assert facade.prepare_source_step(prepared, issued) is local
                        result = facade.commit_source_step(session, local)
                        if result.terminal:
                            break
                    else:
                        raise AssertionError(
                            "source did not finish its bounded fixture"
                        )
                assert result.source_receipt is not None
                assert result.source_receipt.staged_galleries == 3
    summary = _source_summary(caplog)
    if remove_tag_scope:
        with pytest.raises(AssertionError):
            _require_source_coverage(summary, observed)
    else:
        _require_source_coverage(summary, observed)
        assert "TAG_PAGE.commit: 21.8s," in summary
        assert "includes reused results" in summary
    source_messages = [
        item
        for item in caplog.records
        if item.name == "h2hdb.ingest_performance"
        and item.getMessage().startswith("Ingest source")
    ]
    assert len(source_messages) == 2
    preparation = next(
        event
        for event in _database_events(caplog)
        if event["operation"] == "source_prepare" and event["event"] == "completed"
    )
    assert preparation["labels"]["admitted_galleries"] == 3
    assert preparation["labels"]["admitted_files"] == 6
    correlation = preparation["labels"]["correlation_id"]
    assert re.fullmatch("[0-9a-f]{32}", correlation)
    assert f"correlation {correlation}" in summary
    assert "shared" not in summary and "gallery-1000" not in caplog.text
    assert transaction_logs == []


@pytest.mark.cleanup_acceptance
def test_idle_cleanup_and_claim_identify_slow_empty_candidate_at_info(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero-result scan is attributed to its target in both lifecycle paths."""
    initialize_database(db_config)
    timing = _Timing()
    original = cleanup._next_static_candidate_shard

    def slow_candidate(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        if args[1].kind is cleanup.CleanupTargetKind.CANONICAL_VALUE:
            timing.now += 61.0
        return result

    monkeypatch.setattr(cleanup, "_next_static_candidate_shard", slow_candidate)
    config = db_config.model_copy(
        update={"logger": LoggerConfig.model_validate({"level": "info"})}
    )
    with caplog.at_level(logging.INFO, logger="h2hdb.database_performance"):
        with VNextIngestFacade(config) as facade:
            performance = cast(
                DatabasePerformance,
                cast(Any, facade)._VNextIngestFacade__database_performance,
            )
            performance.clock = timing
            assert facade.drain_current_only_maintenance(100_000_000) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
            session = facade.try_claim_ingest(True, 100_000_000)
            assert session is not None
    events = [
        event for event in _database_events(caplog) if event["event"] == "completed"
    ]
    for operation in ("current_only_cleanup", "ingest_claim"):
        event = next(item for item in events if item["operation"] == operation)
        assert event["elapsed_seconds"] == 61.0
        target = event["phase_top"][0]
        assert target["phase"] == "maintenance_eligibility"
        assert target["labels"] == {
            "target": "CANONICAL_VALUE",
            "candidate_found": False,
        }
        assert target["exclusive_seconds"] == 61.0
    cleanup_event = next(
        item for item in events if item["operation"] == "current_only_cleanup"
    )
    assert cleanup_event["labels"]["committed_logical_rows"] == 0
    assert cleanup_event["sql_calls"] == 25
    claim_event = next(item for item in events if item["operation"] == "ingest_claim")
    assert claim_event["labels"]["outcome"] == "claimed"
    assert claim_event["labels"]["ingest_generation"] == session.ingest_generation


@pytest.mark.parametrize("file_count", [127, 128, 129])
@pytest.mark.parametrize("remove_derived_scope", [False, True])
def test_role_stream_diagnostics_cover_page_boundaries_and_empty_tail(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    file_count: int,
    remove_derived_scope: bool,
) -> None:
    """These bounds concern measured calls/returned rows, not server scan cost."""
    from test_catalog_refinement_runtime import (
        _generated_catalog_database,
        _insert_retained_file_family,
    )

    from h2hdb import catalog_refinement
    from h2hdb.database_performance import DatabaseSpan, database_phase

    connector = _generated_catalog_database(tmp_path / "role-measurements.sqlite3")
    performance = DatabasePerformance(
        logging.getLogger("h2hdb.database_performance"),
        backend="sqlite",
        level=logging.DEBUG,
    )
    if remove_derived_scope:
        original_phase = database_phase

        @contextmanager
        def remove_scope(name: str, **fields: Any) -> Iterator[DatabaseSpan]:
            if name == "role_scan.derived_content":
                yield DatabaseSpan()
                return
            with original_phase(name, **fields) as sample:
                yield sample

        monkeypatch.setattr(catalog_refinement, "database_phase", remove_scope)
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        for ordinal in range(file_count):
            _insert_retained_file_family(
                connector,
                name_bytes=f"{ordinal:03}.png".encode(),
                file_no=ordinal,
                file_sha256=b"f" * 32,
            )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) "
            "VALUES (1, 1, %s, %s)",
            (b"f" * 32, file_count),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with caplog.at_level(logging.DEBUG, logger="h2hdb.database_performance"):
            with performance.operation("role_probe"):
                catalog_refinement.check_role_derivation_v1(
                    instrument_connector(connector)
                )
    finally:
        connector.close()
    events = _database_events(caplog)
    summary = next(item for item in events if item["event"] == "completed")
    streams = {item["phase"]: item for item in summary["phase_totals"]}
    expected = {
        "role_scan.file_family",
        "role_scan.file_file_nos",
        "role_scan.file_file_sha256s",
        "role_scan.file_artifact_role",
        "role_scan.file_seals",
        "role_scan.derived_content",
        "role_scan.stored_occurrences",
    }

    def require_coverage() -> None:
        assert streams.keys() == expected
        for name, stream in streams.items():
            rows = 1 if name == "role_scan.stored_occurrences" else file_count
            assert stream["calls"] == (rows + 127) // 128 + 1
            assert stream["exclusive_sql_calls"] == stream["calls"]
            assert stream["exclusive_read_rows"] == rows

    if remove_derived_scope:
        with pytest.raises(AssertionError):
            require_coverage()
    else:
        require_coverage()
    pages = [item for item in events if item["event"] == "phase"]
    # Both generators interleave while comparing their results. Closing every
    # SQL page span before yield prevents one stream becoming another's child.
    assert all(item["parent_id"] == 0 for item in pages)
    for name in streams:
        stream_pages = [item for item in pages if item["phase"] == name]
        assert stream_pages[-1]["labels"]["returned_rows"] == 0
        assert stream_pages[-1]["labels"]["continuation"] is True
        assert all(item["labels"]["page_limit"] == 128 for item in stream_pages)


def test_failed_source_commit_keeps_retry_authority_and_reports_no_success(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vnext_fault_harness import snapshot_database

    initialize_database(db_config)
    original = GalleryObservationStagingRepository.put_tags
    injected = False

    def fail_after_write(*args: Any, **kwargs: Any) -> Any:
        nonlocal injected
        result = original(*args, **kwargs)
        if not injected:
            injected = True
            raise RuntimeError("private-tag-payload-must-not-be-logged")
        return result

    monkeypatch.setattr(
        GalleryObservationStagingRepository, "put_tags", fail_after_write
    )
    with caplog.at_level(logging.INFO, logger="h2hdb"):
        with VNextIngestFacade(db_config) as facade:
            session = claim_session(facade)
            policy = facade.ensure_policy(
                session, ingest_policy(artifacts_required=False)
            )
            with facade.prepare_source(
                MarkerSource((gallery(3001),)), policy=policy
            ) as cut:
                for _ in range(200):
                    issued = facade.issue_source_step(session, policy, cut)
                    local = facade.prepare_source_step(cut, issued)
                    if local._action.value == "TAG_PAGE" and not injected:
                        before = snapshot_database(db_config)
                        with pytest.raises(RuntimeError, match="private-tag-payload"):
                            facade.commit_source_step(session, local)
                        assert snapshot_database(db_config) == before
                        assert facade.issue_source_step(session, policy, cut) is issued
                        assert facade.prepare_source_step(cut, issued) is local
                    result = facade.commit_source_step(session, local)
                    if result.terminal:
                        break
                else:
                    raise AssertionError("source retry did not seal")
            assert injected
            assert result.source_receipt is not None and result.source_receipt.sealed
    failures = [
        item
        for item in _database_events(caplog)
        if item["operation"] == "source_step" and item["event"] == "failed"
    ]
    assert len(failures) == 1
    assert failures[0]["labels"]["action"] == "TAG_PAGE"
    assert failures[0]["labels"]["step_phase"] == "commit"
    assert failures[0]["error_type"] == "RuntimeError"
    assert failures[0]["sql_calls"] > 0
    assert "private-tag-payload-must-not-be-logged" not in caplog.text
    assert "Ingest source stage failed:" in caplog.text
    assert "Ingest source stage finished:" in caplog.text
