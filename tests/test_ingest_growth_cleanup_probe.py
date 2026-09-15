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
