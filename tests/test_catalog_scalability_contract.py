"""Portable catalog smoke semantics and call budgets, independent of file export."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from vnext_database_snapshot import database_digest
from vnext_test_database import DatabaseFactory, database_connector

from h2hdb import (
    CatalogDiscoveryQuery,
    CoreConfig,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
)
from h2hdb.config_loader import DatabaseAccessMode
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import SQLConnector


@pytest.fixture
def benchmark() -> Iterator[ModuleType]:
    name = "catalog_scalability_contract_under_test"
    path = (
        Path(__file__).resolve().parents[1] / "benchmarks/sqlite_catalog_scalability.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.modules.pop(name, None)


def _counted_facade(
    config: CoreConfig, counters: Any, monkeypatch: pytest.MonkeyPatch
) -> VNextCatalogFacade:
    """Count the same facade-visible calls as the file benchmark on either engine."""

    def counted_connector() -> SQLConnector:
        native = database_connector(config)
        for method, field in (
            ("connect", "connections"),
            ("begin_read", "read_transactions"),
        ):
            original = getattr(native, method)

            def record_control(
                *,
                _original: Callable[[], None] = original,
                _field: str = field,
            ) -> None:
                setattr(counters, _field, getattr(counters, _field) + 1)
                _original()

            monkeypatch.setattr(native, method, record_control)
        for method in ("fetch_one", "fetch_all"):
            query_method = getattr(native, method)

            def record_query(
                query: str,
                data: tuple[Any, ...] = (),
                *,
                _original: Callable[..., Any] = query_method,
            ) -> Any:
                counters.record_query(query)
                return _original(query, data)

            monkeypatch.setattr(native, method, record_query)
        return native

    facade = VNextCatalogFacade(config)
    context = replace(
        RepositoryContext.from_config(config), SQLConnector=counted_connector
    )
    # Match the development benchmark's observation seam; no production injection
    # API or alternative schema provider is introduced.
    object.__setattr__(facade, "_VNextCatalogFacade__context", context)
    return facade


def test_catalog_smoke_bundle_cursor_reference_and_cost_contract(
    database_factory: DatabaseFactory,
    benchmark: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = database_factory.config("catalog-scalability-contract")
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        initialized = admin.initialize()
        assert initialized.state == "READY"
    expected, _tables = benchmark.seed_catalog_fixture(
        config, publication_count=165, seed=benchmark.DEFAULT_SEED
    )
    assert expected["publication_count"] == expected["artifact_count"] == 165
    assert expected["acquisition_descriptor_count"] == 165
    assert expected["artifact_blob_count"] == 1
    assert (
        0 < expected["title_search_posting_count"] <= expected["search_posting_count"]
    )
    assert expected["search"]["publication_count"] == 33
    assert all(expected["facets"].values())
    before = database_digest(config)
    readonly = config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )
    counters = benchmark._ReadCounters()
    query = CatalogDiscoveryQuery(search=benchmark.SEARCH_QUERY)
    with closing(_counted_facade(readonly, counters, monkeypatch)) as facade:
        first, first_metrics = benchmark._measure_bundle(facade, counters, query=query)
        warm, warm_metrics = benchmark._measure_bundle(facade, counters, query=query)
        assert first.page.next_cursor is not None
        cursor, cursor_metrics = benchmark._measure_bundle(
            facade, counters, query=query, after=first.page.next_cursor
        )
        benchmark._validate_measured_results(first, warm, cursor, expected=expected)
        reference, reference_metrics = benchmark._measure_separate_reference(
            facade, counters, query=query
        )
        memory, memory_metrics = benchmark._measure_bundle_memory(
            facade, counters, query=query
        )
    assert first == warm == reference == memory
    assert first_metrics["result_sha256"] == warm_metrics["result_sha256"]
    assert first_metrics["result_sha256"] == reference_metrics["result_sha256"]
    assert first_metrics["result_sha256"] == memory_metrics["result_sha256"]
    assert first_metrics["connection_count"] == 1
    assert first_metrics["read_transaction_count"] == 2
    assert first_metrics["logical_query_count"] <= 64
    assert (
        sum(first_metrics["query_class_counts"].values())
        == first_metrics["logical_query_count"]
    )
    assert (
        sum(shape["count"] for shape in first_metrics["query_shapes"])
        == (first_metrics["logical_query_count"])
    )
    assert cursor_metrics["returned_publication_count"] > 0
    assert reference_metrics["connection_count"] == 4
    assert reference_metrics["read_transaction_count"] == 8
    assert memory_metrics["python_traced_peak_bytes"] > 0
    assert memory_metrics["result_json_bytes"] > 0
    assert database_digest(config) == before
    with closing(VNextDatabaseAdminFacade(readonly)) as admin:
        audited = admin.check()
        assert audited.state == "READY"
        assert audited.manifest_sha256 == initialized.manifest_sha256
