"""Diagnostics preserve public validation and application logging ownership."""

import logging
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import pytest
from test_vnext_ingest_analysis import _Clock, _policy, _seed_empty, _session

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)
from h2hdb.config_loader import LoggerConfig
from h2hdb.ingest_performance import IngestPerformance


class _DiagnosticAttributeTrap:
    def __getattribute__(self, name: str) -> object:
        if name in {"ingest_generation", "_session", "_issued"}:
            raise AssertionError("diagnostics inspected a foreign handle")
        return super().__getattribute__(name)


def _call_malformed(
    facade: VNextIngestFacade, operation: str, malformed: object
) -> object:
    foreign = cast(Any, malformed)
    if operation == "analysis_issue":
        return facade.issue_analysis_step(foreign, foreign)
    if operation == "analysis_prepare":
        return facade.prepare_analysis_step(foreign, foreign)
    if operation == "analysis_commit":
        return facade.commit_analysis_step(foreign, foreign)
    if operation == "publication_issue":
        return facade.issue_publication_step(foreign, foreign)
    if operation == "publication_prepare":
        return facade.prepare_publication_step(
            foreign,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=foreign,
        )
    if operation == "publication_commit":
        return facade.commit_publication_step(foreign, foreign)
    if operation == "publication_recovery":
        return facade.try_issue_publication_recovery_step(foreign)
    raise AssertionError(f"unexpected test operation: {operation}")


@pytest.mark.parametrize("malformed", [object(), _DiagnosticAttributeTrap()])
@pytest.mark.parametrize(
    ("operation", "message"),
    [
        ("analysis_issue", "prepared must be VNextPreparedAnalysis"),
        ("analysis_prepare", "prepared must be VNextPreparedAnalysis"),
        ("analysis_commit", "prepared_step must be VNextPreparedAnalysisStep"),
        ("publication_issue", "policy must be VNextResolvedIngestPolicy"),
        (
            "publication_prepare",
            "issued must be an orchestrator-issued publication step",
        ),
        (
            "publication_commit",
            "prepared must be an orchestrator-prepared publication step",
        ),
        ("publication_recovery", "session must be VNextIngestSession"),
    ],
)
def test_performance_preserves_malformed_handle_validation(
    tmp_path: Path, operation: str, message: str, malformed: object
) -> None:
    database = tmp_path / "unused.sqlite3"
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database))
    )
    with VNextIngestFacade(config) as facade:
        with pytest.raises(TypeError, match=f"^{message}$"):
            _call_malformed(facade, operation, malformed)
    assert not database.exists()


def test_performance_uses_application_handler_and_preserves_default_info(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "unused.sqlite3")
        )
    )
    assert int(config.logger.level) == logging.INFO
    with caplog.at_level(logging.DEBUG, logger="h2hdb.ingest_performance"):
        with VNextIngestFacade(config) as facade:
            with pytest.raises(TypeError, match="prepared must be"):
                _call_malformed(facade, "publication_commit", object())
    records = [
        record for record in caplog.records if record.name == "h2hdb.ingest_performance"
    ]
    assert records
    assert all(record.levelno == logging.INFO for record in records)
    assert all("sql_calls=0" in record.getMessage() for record in records)
    assert all("query_top=" not in record.getMessage() for record in records)


def test_empty_recovery_probe_finishes_before_uninstrumented_work(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
        )
    )
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        admin.initialize()
    performance_now = 0.0
    with caplog.at_level(logging.INFO, logger="h2hdb.ingest_performance"):
        with VNextIngestFacade(config, clock=lambda: 1000) as facade:
            performance = cast(
                IngestPerformance,
                cast(Any, facade)._VNextIngestFacade__performance,
            )
            performance.clock = lambda: performance_now
            session = facade.try_claim_ingest(True, 100_000)
            assert session is not None
            assert facade.try_issue_publication_recovery_step(session) is None
            records_at_completion = tuple(caplog.records)
            assert records_at_completion
            final = records_at_completion[-1].getMessage()
            assert "event=stage_terminal " in final
            assert "operation=RECOVERY " in final
            assert f"generation={session.ingest_generation} " in final
            assert "wall_seconds=0.000000 " in final
            assert "calls=1 " in final
            # Source observation can take hours outside an instrumented call.
            # Closing later must not attribute that gap to the completed probe.
            performance_now = 3600.0
        assert tuple(caplog.records) == records_at_completion


def test_analysis_owner_labels_fresh_and_replayed_steps_after_validation(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    database = tmp_path / "analysis.sqlite3"
    build_id, gate, turn = _seed_empty(database)
    session = _session(gate, turn)
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database)),
        logger=LoggerConfig.model_validate({"level": "debug"}),
    )
    with caplog.at_level(logging.DEBUG, logger="h2hdb.ingest_performance"):
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            with closing(
                facade.prepare_analysis(build_id, _policy(), max_rows=8)
            ) as plan:
                issued = facade.issue_analysis_step(session, plan)
                assert facade.issue_analysis_step(session, plan) is issued
                prepared = facade.prepare_analysis_step(plan, issued)
                assert facade.prepare_analysis_step(plan, issued) is prepared
                result = facade.commit_analysis_step(session, prepared)
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "h2hdb.ingest_performance"
        and "event=completed " in record.getMessage()
    ]
    assert len(messages) == 5
    for message in messages:
        assert f"operation={result.stage.decode('ascii')} " in message
        assert f"generation={session.ingest_generation} " in message
    assert sum("phase=issue " in message for message in messages) == 2
    assert sum("phase=prepare " in message for message in messages) == 2
    assert sum("phase=commit " in message for message in messages) == 1
