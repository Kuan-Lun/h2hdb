from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.sql_performance import instrument_connector
from h2hdb.sqlite_connector import SQLiteConnector


@pytest.fixture
def probe() -> ModuleType:
    name = "ingest_growth_cleanup_probe_under_test"
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "ingest_growth_cleanup_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous_path = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous_path
    return module


def test_counts_are_net_decreases_without_calling_insertions_deletions(
    probe: ModuleType,
) -> None:
    assert probe.net_decreases(
        {"removed": 3, "smaller": 5, "larger": 2, "same": 1},
        {"smaller": 2, "larger": 8, "same": 1, "created": 9},
    ) == {"removed": 3, "smaller": 3}


def test_recorder_keeps_sql_and_discovery_groups_separate(probe: ModuleType) -> None:
    recorder = probe.Recorder()
    recorder.record_sql_operation("sql", 0.25, " SELECT 1 ", 1)
    recorder.group = "current_only_maintenance_state"
    recorder.record_sql_operation("sql", 0.75, "SELECT 1", 1)
    recorder.record_sql_operation("connection", 0.5, "", 0)
    rows = recorder.report()
    assert len(rows) == 3
    assert sum(row["seconds"] for row in rows) == 1.5
    assert {row["group"] for row in rows} == {
        "operation",
        "current_only_maintenance_state",
    }


def test_actual_sql_call_is_measured_and_measurement_context_is_restored(
    probe: ModuleType, tmp_path: Path
) -> None:
    def read_value() -> tuple[object, ...] | None:
        with instrument_connector(SQLiteConnector(str(tmp_path / "probe.db"))) as db:
            return db.fetch_one("SELECT 42")

    result, record = probe.measure(read_value)
    assert result == (42,)
    assert record["sql_calls"] == 1
    assert record["logical_phase_rows"] == 0
    assert record["advance_count"] == 0
    assert record["seconds"] >= record["sql_seconds"] >= 0
    raw = SQLiteConnector(str(tmp_path / "unmeasured.db"))
    assert instrument_connector(raw) is raw


def test_failed_probe_preserves_original_exception_and_restores_patches(
    probe: ModuleType,
) -> None:
    original = probe.VNextCleanupRepository.current_only_maintenance_state

    def failure() -> None:
        raise ValueError("synthetic measurement failure")

    with pytest.raises(ValueError, match="synthetic measurement failure"):
        probe.measure(failure)
    assert probe.VNextCleanupRepository.current_only_maintenance_state is original


def test_sqlite_progress_sample_measures_real_work_and_unwraps_connector(
    probe: ModuleType, tmp_path: Path
) -> None:
    query = (
        "WITH RECURSIVE numbers(value) AS (VALUES(1) UNION ALL "
        "SELECT value + 1 FROM numbers WHERE value < %s) "
        "SELECT SUM(value) FROM numbers"
    )
    samples = []
    with probe.measure_sql(probe.Recorder()):
        with instrument_connector(SQLiteConnector(str(tmp_path / "sample.db"))) as db:
            for limit in (200, 2000):
                with probe.sample_sqlite_candidate(db) as sample:
                    assert sample is not None
                    assert db.fetch_one(query, (limit,)) == (limit * (limit + 1) // 2,)
                callbacks = sample.callbacks
                assert callbacks > 0
                # A subsequent real query must not invoke the cleared handler.
                db.fetch_one(query, (2000,))
                assert sample.callbacks == callbacks
                samples.append(callbacks)
    # Work grows while query count and returned-row count remain identical.
    # Avoid machine-specific timing or precise opcode thresholds.
    assert samples[1] > samples[0]


def test_sqlite_progress_handler_is_cleared_after_measured_exception(
    probe: ModuleType, tmp_path: Path
) -> None:
    query = (
        "WITH RECURSIVE numbers(value) AS (VALUES(1) UNION ALL "
        "SELECT value + 1 FROM numbers WHERE value < 1000) SELECT SUM(value) FROM numbers"
    )
    with SQLiteConnector(str(tmp_path / "failed-sample.db")) as db:
        with pytest.raises(ValueError, match="synthetic callback scope failure"):
            with probe.sample_sqlite_candidate(db) as sample:
                assert sample is not None
                db.fetch_one(query)
                raise ValueError("synthetic callback scope failure")
        callbacks = sample.callbacks
        assert callbacks > 0
        db.fetch_one(query)
        assert sample.callbacks == callbacks


def test_non_sqlite_progress_sample_explicitly_reports_unmeasured(
    probe: ModuleType,
) -> None:
    with probe.sample_sqlite_candidate(object()) as sample:
        assert sample is None


def test_idle_mode_measures_each_target_and_preserves_real_catalog(
    probe: ModuleType, tmp_path: Path
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(tmp_path / "idle.db"))
    )
    output = tmp_path / "report.json"
    report = probe.run_case(
        config,
        gallery_count=1,
        pages_per_gallery=2,
        revisions=2,
        mode="idle",
        output=output,
    )
    assert report == json.loads(output.read_text())
    assert report["status"] == "completed"
    assert report["full_ready_audit"] == "passed"
    assert report["pages_per_gallery"] == 2
    assert [case["overlay_depth"] for case in report["cases"]] == [0, 1]
    targets = {
        kind.value for kind in probe.cleanup_repository._CURRENT_ONLY_TARGET_PRIORITY
    }
    for case in report["cases"]:
        assert case["idle_catalog_snapshot_unchanged"]
        assert case["idle_retained_roots_unchanged"]
        assert case["idle_claim_completed"]
        first, second, claim = case["idle_sequence"]
        assert first["outcome"] == second["outcome"] == "DONE"
        assert first["advance_count"] == second["advance_count"] == 0
        assert claim["granted"]
        for operation in case["idle_sequence"]:
            candidates = operation["candidate_probes"]
            assert {row["target"] for row in candidates} == targets
            assert all(not row["candidate_found"] for row in candidates)
            assert all(
                row["sqlite_progress_operations_estimate"]
                == row["sqlite_progress_callbacks"] * probe.SQLITE_PROGRESS_QUANTUM
                for row in candidates
            )
            assert (
                sum(row["sqlite_progress_operations_estimate"] for row in candidates)
                > 0
            )
            queries = [
                row
                for row in operation["queries"]
                if row["category"] == "sql" and row["target"] != "unclassified"
            ]
            assert {row["target"] for row in queries} == targets
            assert {row["group"] for row in queries} == {
                "current_only_maintenance_state"
            }
            # Zero returned candidates does not assert zero examined rows.
            assert all(row["returned_rows"] == 0 for row in queries)
            assert sum(row["seconds"] for row in queries) <= operation["sql_seconds"]


@pytest.mark.parametrize(
    "shape",
    [
        {"gallery_count": 0},
        {"gallery_count": 33},
        {"pages_per_gallery": 0},
        {"pages_per_gallery": 33},
        {"gallery_count": 32, "pages_per_gallery": 32},
        {"mode": "invalid"},
        {"mode": "cleanup", "pages_per_gallery": 2},
    ],
)
def test_invalid_idle_shape_is_rejected_before_database_access(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shape: dict[str, object],
) -> None:
    initialize = Mock()
    monkeypatch.setattr(probe, "initialize_database", initialize)
    options = {"gallery_count": 1, "pages_per_gallery": 1, "mode": "idle", **shape}
    with pytest.raises(ValueError):
        probe.run_case(
            CoreConfig(), revisions=1, output=tmp_path / "report.json", **options
        )
    initialize.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    ["drain", "claim", "catalog", "roots", "followup_catalog", "followup_roots"],
)
def test_idle_oracle_rejects_unexpected_work_or_changed_facts(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    done = probe.VNextCurrentOnlyMaintenanceOutcome.DONE
    progressed = probe.VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
    facade = SimpleNamespace(
        drain_current_only_maintenance=Mock(
            return_value=progressed if failure == "drain" else done
        ),
        try_claim_ingest=Mock(return_value=None if failure == "claim" else object()),
        complete_ingest=Mock(),
    )
    monkeypatch.setattr(
        probe,
        "catalog_snapshot",
        Mock(
            side_effect=[
                "original",
                "changed" if failure == "catalog" else "original",
                "changed" if failure == "followup_catalog" else "original",
            ]
        ),
    )
    monkeypatch.setattr(
        probe,
        "retained_compaction_roots",
        Mock(
            side_effect=[
                ({1}, {2}),
                ({1}, {3} if failure == "roots" else {2}),
                ({1}, {3} if failure == "followup_roots" else {2}),
            ]
        ),
    )
    case: dict[str, object] = {}
    with pytest.raises(RuntimeError):
        probe.measure_idle_sequence(
            facade,
            object(),
            CoreConfig(),
            report={"cases": [case]},
            case=case,
            output=tmp_path / "report.json",
        )
    assert "idle_catalog_snapshot_unchanged" not in case
    assert "idle_claim_completed" not in case
    if failure in {"catalog", "roots", "followup_catalog", "followup_roots"}:
        facade.complete_ingest.assert_called_once()
    if failure in {"followup_catalog", "followup_roots"}:
        assert facade.drain_current_only_maintenance.call_count == 3


def test_two_real_sqlite_publications_advance_and_publish_latest_content(
    probe: ModuleType, tmp_path: Path
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "catalog.db")
        )
    )
    output = tmp_path / "report.json"
    report = probe.run_case(config, gallery_count=2, revisions=2, output=output)
    assert report == json.loads(output.read_text())
    assert report["status"] == "completed"
    assert report["stage"] == "measurement_completed"
    assert "not that cleanup efficiency passed" in report["measurement_notes"]
    first, second = report["cases"]
    assert (first["actual_revision"], second["actual_revision"]) == (1, 2)
    assert [case["published_count"] for case in report["cases"]] == [2, 2]
    assert second["gallery_1_title"] == "Gallery 1 revision 2"
    assert first["gallery_1_content_sha256"] != second["gallery_1_content_sha256"]
    assert all(
        case["gallery_1_content_sha256"] == case["expected_content_sha256"]
        and case["steps"][-1]["outcome"] == "DONE"
        for case in report["cases"]
    )
    assert report["source_provenance"]["source_sha256"]
    assert "warm the cache" in report["measurement_notes"]
    assert all(step["advance_transactions"] <= 16 for step in second["steps"])
    assert sum(step["logical_phase_rows"] for step in second["steps"]) > 0
    assert any(
        step["advance_count"] > step["advance_transactions"] for step in second["steps"]
    )


def test_replayed_revision_is_rejected_before_catalog_hydration(
    probe: ModuleType,
) -> None:
    with pytest.raises(RuntimeError, match="replay is not a new revision"):
        probe.verify_publication(
            object(),
            SimpleNamespace(revision=3),
            previous_revision=3,
            gallery_count=2,
            expected_title="new title",
            expected_page=b"new page",
        )


@pytest.mark.cleanup_acceptance
def test_three_round_policy_change_compacts_real_history(
    probe: ModuleType, tmp_path: Path
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "catalog.db")
        )
    )
    report = probe.run_case(
        config,
        gallery_count=2,
        revisions=3,
        policy_change_at=3,
        output=tmp_path / "report.json",
    )
    first, incremental, compacted = report["cases"]
    assert [case["overlay_depth"] for case in report["cases"]] == [0, 1, 0]
    assert first["policy_id"] == incremental["policy_id"] != compacted["policy_id"]
    assert (
        incremental["gallery_1_content_sha256"] == compacted["gallery_1_content_sha256"]
    )
    assert (
        compacted["retained_analysis_count"]
        == compacted["retained_source_build_count"]
        == 1
    )
    assert all(case["steps"][-1]["outcome"] == "DONE" for case in report["cases"])
    assert report["full_ready_audit"] == "passed"


@pytest.mark.parametrize("stale_field", ["title", "content_sha256"])
def test_advancing_revision_with_stale_source_content_is_rejected(
    probe: ModuleType, stale_field: str
) -> None:
    publication = SimpleNamespace(
        gid=1,
        title="new title",
        content_sha256=probe.effective_content_digest(
            (probe.sha256(b"new page").digest(),)
        ).hex(),
    )
    setattr(publication, stale_field, "stale")
    catalog = SimpleNamespace(
        discover_publications=Mock(
            return_value=SimpleNamespace(publications=[publication])
        )
    )
    with pytest.raises(RuntimeError, match="latest synthetic title/content"):
        probe.verify_publication(
            catalog,
            SimpleNamespace(revision=4, publication_count=2, artifact_count=0),
            previous_revision=3,
            gallery_count=2,
            expected_title="new title",
            expected_page=b"new page",
        )


def test_initial_report_survives_failure_without_config_secret(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    original_error = ValueError("database password=do-not-save-this")

    def reject_initialize(_config: CoreConfig) -> None:
        assert json.loads(output.read_text())["status"] == "incomplete"
        raise original_error

    monkeypatch.setattr(probe, "initialize_database", reject_initialize)
    with pytest.raises(ValueError) as caught:
        probe.run_case(CoreConfig(), gallery_count=2, revisions=2, output=output)
    assert caught.value is original_error
    saved = json.loads(output.read_text())
    assert saved["status"] == "failed"
    assert saved["failure"]["stage"] == "database_initialize"
    assert saved["failure"]["reason"] == "ValueError"
    assert "do-not-save-this" not in output.read_text()


def test_interrupted_report_replace_keeps_last_complete_json(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    initial = {"status": "incomplete", "cases": []}
    probe.write_report(output, initial)
    monkeypatch.setattr(probe.os, "replace", Mock(side_effect=TimeoutError("alarm")))
    with pytest.raises(TimeoutError):
        probe.write_report(output, {"status": "completed"})
    assert json.loads(output.read_text()) == initial
    assert not list(tmp_path.glob(".report-*"))


@pytest.mark.parametrize("failure", ["startup", "startup_and_stop", "teardown"])
def test_private_container_stops_after_startup_or_body_failure(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    import testcontainers.community.mysql as mysql_container

    startup_error = RuntimeError("startup failed")
    stop_error = RuntimeError("stop failed")
    container = SimpleNamespace(
        start=Mock(side_effect=None if failure == "teardown" else startup_error),
        stop=Mock(side_effect=None if failure == "startup" else stop_error),
        get_container_host_ip=lambda: "127.0.0.1",
        get_exposed_port=lambda _port: 3306,
        port=3306,
    )
    monkeypatch.setattr(mysql_container, "MySqlContainer", Mock(return_value=container))
    monkeypatch.setattr(
        probe,
        "open_connector",
        Mock(
            return_value=SimpleNamespace(
                fetch_one=lambda _query: ("10.11.11-MariaDB",), close=lambda: None
            )
        ),
    )
    with pytest.raises(RuntimeError) as caught:
        with probe.database("mariadb", tmp_path):
            pass
    assert caught.value is (stop_error if failure == "teardown" else startup_error)
    container.start.assert_called_once_with()
    container.stop.assert_called_once_with()
