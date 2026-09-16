from __future__ import annotations

import importlib.util
import json
import logging
import sys
from contextlib import contextmanager
from dataclasses import asdict
from io import BytesIO
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
        ("galleries", 257),
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
    probe.Shape(galleries=256, pages=16, tags=128, metadata_bytes=16384)
    with pytest.raises(ValueError, match="metadata payload"):
        probe.Shape(galleries=256, pages=16, metadata_bytes=16385)
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
        audit = case["full_ready_audit_measurements"]
        assert audit["sql_calls"] > 0
        assert audit["returned_rows"] > 0
        assert audit["query_details_truncated"]
        labels = {value["operation"] for value in audit["operations"]}
        assert "schema_structure" in labels
        assert "provider_resolution" in labels
        assert any(label.startswith("semantic:catalog.") for label in labels)
        expected_semantics = {
            "semantic:" + key
            for key in probe.schema_provider.GeneratedVNextSchemaProvider(
                "sqlite"
            ).definition.ready_semantic_obligation_ids
        }
        assert expected_semantics <= labels
        assert (
            sum(value["sql_calls"] for value in audit["operations"])
            == audit["sql_calls"]
        )
        assert (
            sum(value["returned_rows"] for value in audit["operations"])
            == audit["returned_rows"]
        )
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


def test_forced_start_report_is_outside_phase_and_audit_timers(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_counter = probe.time.perf_counter
    virtual_serialization_seconds = 0.0
    starts = []

    def counter() -> float:
        return float(real_counter()) + virtual_serialization_seconds

    def progress(stage: str, _phases: dict[str, float]) -> None:
        nonlocal virtual_serialization_seconds
        if stage.endswith(":started"):
            starts.append(stage)
            virtual_serialization_seconds += 86400.0

    monkeypatch.setattr(probe.time, "perf_counter", counter)
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(tmp_path / "timer.db"))
    )
    case = probe.run_case(config, probe.Shape(galleries=1), progress=progress)
    assert len(starts) == 5
    # The injected day represents report serialization, not a performance SLO.
    assert all(value < 86400.0 for value in case["phase_seconds"].values())
    assert case["full_ready_audit_seconds"] < 86400.0


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


def test_fixture_locator_order_is_canonical_across_decimal_widths(
    probe: ModuleType,
) -> None:
    from h2hdb.vnext_ingest_facade import _iter_source_locators

    source = probe.source_for(probe.Shape(galleries=256))
    locators = tuple(_iter_source_locators(source))
    assert len(locators) == 256
    assert locators[0] == ("gallery-000001",)
    assert locators[-1] == ("gallery-000256",)
    assert len(set(locators)) == 256


@pytest.mark.parametrize("fault", [None, "duplicate", "missing"])
def test_oracle_reads_multiple_public_pages_and_rejects_invalid_membership(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, fault: str | None
) -> None:
    source = probe.source_for(probe.Shape(galleries=256))
    publications = [
        SimpleNamespace(
            gid=item.gid,
            title=item.title,
            summary=item.comment,
            subjects=(),
            content_sha256=probe.effective_content_digest(
                tuple(
                    probe.sha256(data).digest()
                    for name, data in item.files.items()
                    if name.endswith(b".png")
                )
            ).hex(),
        )
        for item in source.galleries
    ]
    second = (
        publications[:128]
        if fault == "duplicate"
        else publications[128 : 255 if fault == "missing" else 256]
    )
    discover = Mock(
        side_effect=[
            SimpleNamespace(publications=publications[:128], next_cursor="second-page"),
            SimpleNamespace(publications=second, next_cursor=None),
        ]
    )
    catalog = SimpleNamespace(
        get_catalog_revision=lambda: SimpleNamespace(
            revision=1, publication_count=256, artifact_count=0
        ),
        discover_publications=discover,
        close=Mock(),
    )
    monkeypatch.setattr(probe, "VNextCatalogFacade", Mock(return_value=catalog))
    if fault is None:
        oracle = probe.verify_publication(CoreConfig(), source.galleries)
        assert oracle["gids"] == list(range(1, 257))
        assert discover.call_args_list[0].kwargs["after"] is None
        assert discover.call_args_list[1].kwargs["after"] == "second-page"
    else:
        with pytest.raises(RuntimeError):
            probe.verify_publication(CoreConfig(), source.galleries)
    catalog.close.assert_called_once_with()


def test_audit_nested_scopes_are_exclusive_and_sql_is_not_duplicated(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [0.0]
    monkeypatch.setattr(probe.time, "perf_counter", lambda: clock[0])
    observer = probe.AuditObserver()
    with observer.scope("parent"):
        observer.record_sql_operation("sql", 0.1, "SELECT parent", 1)
        clock[0] = 1.0
        with observer.scope("child"):
            observer.record_sql_operation("sql", 0.2, "SELECT child", 2)
            clock[0] = 3.0
        clock[0] = 5.0
    report = observer.finish(6.0)
    measured = {value["operation"]: value for value in report["operations"]}
    assert measured["parent"]["exclusive_seconds"] == 3.0
    assert measured["child"]["exclusive_seconds"] == 2.0
    assert measured["outside"]["exclusive_seconds"] == 1.0
    assert report["sql_calls"] == 2
    assert report["returned_rows"] == 3
    assert measured["parent"]["non_sql_seconds"] == 2.9
    assert measured["child"]["top_queries"][0]["sql"] == "SELECT child"


def test_audit_validator_wrapper_preserves_failure_and_restores_hooks(
    probe: ModuleType,
) -> None:
    original = probe.schema_provider._load_builtin_semantic_validators
    observer = probe.AuditObserver()
    failure = ValueError("validator failure")

    def fail() -> None:
        raise failure

    with pytest.raises(ValueError) as caught:
        with observer.installed():
            observer.wrap("validator", fail)()
    assert caught.value is failure
    assert probe.schema_provider._load_builtin_semantic_validators is original
    assert observer.source_scope is None
    assert not observer.children


def test_audit_cache_observer_preserves_lru_order_and_counts_eviction(
    probe: ModuleType,
) -> None:
    owner = probe.catalog_refinement._CanonicalValidationCache
    original_open = owner.open
    observer = probe.AuditObserver()
    domain = b"title_utf8_v1"
    capacity = probe.catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES
    with observer.installed():
        cache = owner()
        for value in range(capacity + 1):
            digest = value.to_bytes(32)
            assert cache.open(digest, domain) is None
            cache.remember(digest, domain, BytesIO(b"x"), byte_count=1)
        newest = capacity.to_bytes(32)
        opened = cache.open(newest, domain)
        assert opened is not None
        with opened[0] as spool:
            assert spool.read() == b"x"
        assert cache.open(bytes(32), domain) is None
        assert list(cache._values)[-1] == (newest, domain)
    assert owner.open is original_open
    report = observer.finish(0.0)["canonical_cache"]
    metric = report["domains"][domain.decode()]
    assert metric["hits"] == 1
    assert metric["misses"] == capacity + 2
    assert metric["admissions"] == capacity + 1
    assert metric["evictions_caused"] == 1
    assert metric["max_resident_entries"] == capacity
    assert report["observed_distinct_validated_bytes"] == capacity + 1
    assert report["observed_distinct_requested_keys"] == capacity + 1


def test_audit_cache_observer_bounds_keys_and_records_uncached_values(
    probe: ModuleType,
) -> None:
    observer = probe.AuditObserver()
    observer.cache_key_budget = 1
    domain = b"title_utf8_v1"
    size = probe.catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES + 1
    with observer.installed():
        cache = probe.catalog_refinement._CanonicalValidationCache()
        cache.remember(bytes(32), domain, BytesIO(b"x" * size), byte_count=size)
        assert not cache._values
        with pytest.raises(RuntimeError, match="key budget exceeded"):
            cache.open(b"a" * 32, domain)
    report = observer.finish(0.0)["canonical_cache"]
    assert report["domains"][domain.decode()]["oversize_bypasses"] == 1
    assert report["observed_distinct_validated_bytes"] == size


def test_audit_cache_control_preserves_byte_caps_validators_and_restores_capacity(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = probe.catalog_refinement
    original = runtime._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES
    byte_caps = (
        runtime._CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES,
        runtime._CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES,
    )
    oracle = {"gids": [1]}
    verification = Mock(return_value=oracle)
    monkeypatch.setattr(probe, "verify_publication", verification)
    baseline = {"operations": [{"operation": "semantic:real_validator", "calls": 1}]}
    observed = []

    def measure(label: str) -> dict[str, Any]:
        observed.append((label, runtime._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES))
        assert byte_caps == (
            runtime._CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES,
            runtime._CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES,
        )
        return baseline

    result = probe.audit_cache_comparison(CoreConfig(), (), oracle, baseline, measure)
    assert observed == [
        ("ready_audit_B_capacity512", 512),
        ("ready_audit_A_restored", original),
    ]
    assert result["order"] == ["A_baseline", "B_capacity512", "A_restored"]
    assert result["same_published_oracle"]
    assert verification.call_count == 3
    assert runtime._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES == original


@pytest.mark.parametrize("failure", ["measure", "oracle", "validators"])
def test_audit_cache_control_fails_closed_and_restores_capacity(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    runtime = probe.catalog_refinement
    original = runtime._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES
    oracle = {"gids": [1]}
    monkeypatch.setattr(
        probe,
        "verify_publication",
        Mock(return_value={} if failure == "oracle" else oracle),
    )
    baseline = {"operations": [{"operation": "semantic:real_validator", "calls": 1}]}

    def measure(_label: str) -> dict[str, Any]:
        if failure == "measure":
            raise RuntimeError("audit failed")
        return {"operations": []} if failure == "validators" else baseline

    with pytest.raises(RuntimeError):
        probe.audit_cache_comparison(CoreConfig(), (), oracle, baseline, measure)
    assert runtime._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES == original


def test_server_diagnostics_preserve_unavailable_status_and_original_error(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = {"snapshot_seconds": 1.0, "phase": "before"}
    after = {"snapshot_seconds": 2.0, "phase": "after"}
    snapshot = Mock(side_effect=[before, after])
    difference = {
        "complete_counter_comparison": False,
        "limitations_detected": ["disabled"],
    }
    monkeypatch.setattr(probe.mariadb_performance, "snapshot", snapshot)
    monkeypatch.setattr(
        probe.mariadb_performance, "differences", Mock(return_value=difference)
    )
    connection = object()
    diagnostic = probe.ServerDiagnostics(connection, "private_probe")
    diagnostic.start("source")
    diagnostic.finish("source")
    report = diagnostic.report()
    assert report["snapshot_seconds"] == 3.0
    assert report["phases"]["source"] == {
        "before": before,
        "after": after,
        "differences": difference,
    }
    assert all(
        call.args == (connection, "private_probe") for call in snapshot.call_args_list
    )
    monkeypatch.setattr(
        diagnostic, "finish", Mock(side_effect=TimeoutError("diagnostics failed"))
    )
    original = ValueError("original failure")
    diagnostic.finish_preserving("source", original)
    assert original.__notes__ == ["Post-phase diagnostics also failed: TimeoutError"]
    with pytest.raises(TimeoutError, match="diagnostics failed"):
        diagnostic.finish_preserving("source", None)


def test_diagnostic_session_uses_read_only_private_root_and_closes(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    cursor = Mock()
    connection = Mock()
    connection.cursor.return_value = probe.closing(cursor)
    opened = Mock(return_value=connection)
    monkeypatch.setattr(probe.mariadb_performance, "open_connection", opened)
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb",
            host="127.0.0.1",
            port=33333,
            user="probe",
            database="private_probe",
        )
    )
    with pytest.raises(ValueError, match="body"):
        with probe.diagnostic_session(config, True) as diagnostic:
            assert diagnostic.schema == "private_probe"
            raise ValueError("body")
    root = opened.call_args.args[0].database
    assert root.user == "root" and root.host == "127.0.0.1" and root.port == 33333
    assert config.database.user == "probe"
    cursor.execute.assert_called_once_with("SET SESSION TRANSACTION READ ONLY")
    connection.close.assert_called_once_with()


def test_sqlite_refuses_server_diagnostics_before_database_open(
    probe: ModuleType, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="MariaDB"):
        with probe.database("sqlite", tmp_path, diagnostics=True):
            pytest.fail("unexpected database entry")
    assert not (tmp_path / "catalog.sqlite3").exists()
