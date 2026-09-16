from __future__ import annotations

import importlib.util
import json
import logging
import sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import instrument_connector
from h2hdb.sqlite_connector import SQLiteConnector


@pytest.fixture
def probe() -> ModuleType:
    name = "ingest_pipeline_probe_under_test"
    script = (
        Path(__file__).resolve().parents[1] / "scripts" / "ingest_pipeline_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


@pytest.mark.parametrize(
    "field,value",
    [
        ("galleries", 0),
        ("galleries", 33),
        ("pages", 0),
        ("pages", 257),
        ("tags", -1),
        ("tags", 129),
        ("metadata_bytes", -1),
        ("metadata_bytes", 262145),
        ("pages", True),
    ],
)
def test_invalid_shape_is_rejected(probe: ModuleType, field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        probe.Shape(**{field: value})


def test_shape_product_budget_and_one_factor_matrix(probe: ModuleType) -> None:
    with pytest.raises(ValueError, match="total pages"):
        probe.Shape(galleries=32, pages=256)
    with pytest.raises(ValueError, match="metadata payload"):
        probe.Shape(galleries=32, metadata_bytes=262144)
    base = probe.Shape()
    matrix = probe.shapes(base, "all")
    assert len(matrix) == 5
    for name, changed in matrix[1:]:
        assert [
            key for key in asdict(base) if getattr(base, key) != getattr(changed, key)
        ] == [name]
    assert dict(matrix)["pages"].pages == 129
    assert dict(matrix)["metadata_bytes"].metadata_bytes == 65536
    assert dict(matrix)["tags"].tags == 32
    with pytest.raises(ValueError, match="unknown shape"):
        probe.shapes(base, "history")


def test_source_is_deterministic_and_dimensions_are_independent(
    probe: ModuleType,
) -> None:
    baseline = probe.source_for(probe.Shape()).galleries
    assert baseline == probe.source_for(probe.Shape()).galleries
    for name, shape in probe.shapes(probe.Shape(), "all")[1:]:
        values = probe.source_for(shape).galleries
        if name != "galleries":
            assert [(v.gid, v.title, v.locator) for v in values] == [
                (v.gid, v.title, v.locator) for v in baseline
            ]
        for value in values:
            assert sum(name.endswith(b".png") for name in value.files) == shape.pages
            assert len(value.tags) == shape.tags
            assert len(value.comment.encode()) == shape.metadata_bytes


def test_nested_sql_is_recorded_once_and_labels_follow_validated_operation(
    probe: ModuleType, tmp_path: Path
) -> None:
    observer = probe.Observer()
    performance = IngestPerformance(logging.getLogger("probe-test"), backend="sqlite")
    with observer.installed():
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "count.db"))
        ) as connector:
            connector.fetch_one("SELECT 1")
            with performance.step("analysis", "issue", "ISSUE", 1) as outer:
                connector.fetch_one("SELECT 2")
                with performance.step("analysis", "prepare", "inner", 1):
                    connector.fetch_one("SELECT 3")
                outer.operation = "validated_operation"
    report = observer.report()
    queries = [query for query in report["queries"] if query["category"] == "sql"]
    assert report["sql_calls"] == 3
    assert report["returned_rows"] == 3
    assert {(query["operation"], query["sql"]) for query in queries} == {
        ("outside", "SELECT 1"),
        ("validated_operation", "SELECT 2"),
        ("inner", "SELECT 3"),
    }
    assert observer.active is None
    raw = SQLiteConnector(str(tmp_path / "later.db"))
    assert instrument_connector(raw) is raw


def test_swallowed_sql_observer_failure_invalidates_report(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer = probe.Observer()

    def fail(*args: Any, **kwargs: Any) -> None:
        raise ValueError("broken observer")

    monkeypatch.setattr(observer, "record", fail)
    with (
        observer.installed(),
        instrument_connector(
            SQLiteConnector(str(tmp_path / "failure.db"))
        ) as connector,
    ):
        assert connector.fetch_one("SELECT 42") == (42,)
    with pytest.raises(RuntimeError, match="observer failed") as error:
        observer.report()
    assert isinstance(error.value.__cause__, ValueError)


def test_measurement_fingerprint_budget_fails_closed(probe: ModuleType) -> None:
    observer = probe.Observer()
    for index in range(8193):
        observer.record_sql_operation("sql", 0.0, f"SELECT {index}", 1)
    with pytest.raises(RuntimeError, match="observer failed"):
        observer.report()


def test_exception_restores_runtime_hooks(probe: ModuleType) -> None:
    original_step = IngestPerformance.step
    original_prepare = probe.VNextIngestFacade.prepare_source
    with pytest.raises(ValueError, match="synthetic"):
        with probe.Observer().installed():
            raise ValueError("synthetic")
    assert IngestPerformance.step is original_step
    assert probe.VNextIngestFacade.prepare_source is original_prepare


def test_real_pipeline_publishes_same_gids_across_tag_shapes(
    probe: ModuleType, tmp_path: Path
) -> None:
    cases = []
    for index, (tags, metadata_bytes) in enumerate(((0, 0), (3, 0), (0, 65))):
        config = CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / f"case-{index}.db")
            )
        )
        cases.append(
            probe.run_case(
                config, probe.Shape(tags=tags, metadata_bytes=metadata_bytes)
            )
        )
    first, second, metadata = cases
    for field in ("revision", "publication_count", "gids", "titles", "content_sha256"):
        assert (
            first["oracle"][field]
            == second["oracle"][field]
            == metadata["oracle"][field]
        )
    assert first["oracle"]["tag_count"] == {"1": 0, "2": 0}
    assert second["oracle"]["tag_count"] == {"1": 3, "2": 3}
    assert first["oracle"]["tags_sha256"] != second["oracle"]["tags_sha256"]
    assert first["oracle"]["summary_bytes"] == {"1": 0, "2": 0}
    assert metadata["oracle"]["summary_bytes"] == {"1": 65, "2": 65}
    assert first["oracle"]["summary_sha256"] != metadata["oracle"]["summary_sha256"]
    assert first["oracle"]["gids"] == [1, 2]
    for case in cases:
        assert case["full_ready_audit"] == "passed"
        assert set(case["phase_seconds"]) == {
            "source",
            "analysis",
            "publication",
            "complete",
        }
        measured = case["measurements"]
        assert measured["sql_calls"] > 0
        assert {
            value["phase"]
            for value in measured["operations"]
            if value["pipeline"] == "source"
        } == {"prepare", "issue", "commit"}
        assert all(value["exclusive_seconds"] >= 0 for value in measured["operations"])
        assert any(
            value["operation"] == "gid_candidate" for value in measured["queries"]
        )
        assert {
            value["operation"]
            for value in measured["subcalls"]
            if value["name"] == "_prepare_catalog_plan"
        } == {"BUILD_CATALOG", "VALIDATE_CATALOG"}


@pytest.mark.deep
def test_real_pipeline_crosses_128_page_source_boundary(
    probe: ModuleType, tmp_path: Path
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "boundary.db")
        )
    )
    case = probe.run_case(config, probe.Shape(galleries=1, pages=129))
    assert case["oracle"]["gids"] == [1]
    assert case["full_ready_audit"] == "passed"


def test_failure_report_is_atomic_and_preserves_exception(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    @contextmanager
    def fail_database(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("synthetic startup failure")
        yield

    monkeypatch.setattr(probe, "database", fail_database)
    monkeypatch.setattr(probe, "source_provenance", dict)
    report = probe.new_report("sqlite")
    output = tmp_path / "report.json"
    with pytest.raises(RuntimeError, match="synthetic startup failure"):
        probe.execute("sqlite", [("baseline", probe.Shape())], 1, output, report)
    saved = json.loads(output.read_text())
    assert saved["status"] == "failed"
    assert saved["failure"]["reason"] == "RuntimeError"
    assert "synthetic startup failure" not in output.read_text()
    assert not list(tmp_path.glob(".pipeline-report-*"))


def test_atomic_write_failure_keeps_previous_report(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    probe.write_report(output, {"status": "incomplete"})

    def fail_replace(*args: Any) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(probe.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        probe.write_report(output, {"status": "completed"})
    assert json.loads(output.read_text()) == {"status": "incomplete"}
    assert not list(tmp_path.glob(".pipeline-report-*"))


@pytest.mark.parametrize("kind", ["file", "symlink", "dangling_symlink", "directory"])
def test_cli_refuses_existing_output_without_changing_it(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    output = tmp_path / "report.json"
    original = b'{"status":"existing evidence"}\n'
    target = tmp_path / "target.json"
    if kind == "file":
        output.write_bytes(original)
    elif kind == "directory":
        output.mkdir()
    else:
        if kind == "symlink":
            target.write_bytes(original)
        output.symlink_to(target)
    execute = Mock()
    monkeypatch.setattr(probe, "execute", execute)
    monkeypatch.setattr(
        sys, "argv", ["ingest_pipeline_probe.py", "--output", str(output)]
    )
    with pytest.raises(SystemExit) as error:
        probe.main()
    assert error.value.code == 2
    execute.assert_not_called()
    if kind == "file":
        assert output.read_bytes() == original
    elif kind == "directory":
        assert output.is_dir()
    else:
        assert output.is_symlink()
        if kind == "symlink":
            assert target.read_bytes() == original
        else:
            assert not target.exists()


@pytest.mark.parametrize(
    "failure", ["startup", "startup_and_stop", "body", "body_and_stop", "teardown"]
)
def test_private_container_cleans_up_and_preserves_original_failure(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    import testcontainers.community.mysql as mysql_container

    original_error = RuntimeError("original failure")
    stop_error = RuntimeError("stop failure")
    container = SimpleNamespace(
        start=Mock(
            side_effect=original_error if failure.startswith("startup") else None
        ),
        stop=Mock(
            side_effect=stop_error
            if failure.endswith("stop") or failure == "teardown"
            else None
        ),
        get_container_host_ip=lambda: "127.0.0.1",
        get_exposed_port=lambda _port: 3306,
        port=3306,
    )
    constructor = Mock(return_value=container)
    monkeypatch.setattr(mysql_container, "MySqlContainer", constructor)
    with pytest.raises(RuntimeError) as caught:
        with probe.database("mariadb", tmp_path) as config:
            assert config.database.host == "127.0.0.1"
            if failure.startswith("body"):
                raise original_error
    assert caught.value is (stop_error if failure == "teardown" else original_error)
    constructor.assert_called_once()
    assert constructor.call_args.kwargs["image"] == "mariadb:10.11.11"
    container.start.assert_called_once_with()
    container.stop.assert_called_once_with()
    if failure.endswith("stop"):
        assert original_error.__notes__ == [
            "Testcontainer cleanup also failed: RuntimeError"
        ]


@pytest.mark.parametrize("missing", ["summary", "tags", "tag_namespace"])
def test_publication_oracle_rejects_lost_metadata_or_tags(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    source = probe.source_for(probe.Shape(galleries=1, tags=2, metadata_bytes=17))
    expected = source.galleries[0]
    content = probe.effective_content_digest(
        tuple(
            probe.sha256(data).digest()
            for name, data in expected.files.items()
            if name.endswith(b".png")
        )
    ).hex()
    publication = SimpleNamespace(
        gid=expected.gid,
        title=expected.title,
        content_sha256=content,
        summary="" if missing == "summary" else expected.comment,
        subjects=()
        if missing == "tags"
        else tuple(
            SimpleNamespace(
                code="wrong" if missing == "tag_namespace" else namespace, name=value
            )
            for namespace, value in expected.tags
        ),
    )
    catalog = SimpleNamespace(
        get_catalog_revision=lambda: SimpleNamespace(
            revision=1, publication_count=1, artifact_count=0
        ),
        discover_publications=lambda **kwargs: SimpleNamespace(
            publications=(publication,), next_cursor=None
        ),
        close=Mock(),
    )
    monkeypatch.setattr(probe, "VNextCatalogFacade", Mock(return_value=catalog))
    with pytest.raises(
        RuntimeError, match="summary" if missing == "summary" else "tags"
    ):
        probe.verify_publication(CoreConfig(), source.galleries)
    catalog.close.assert_called_once_with()
