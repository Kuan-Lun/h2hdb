"""Paired native regression for bounded canonical dictionary selection."""

from __future__ import annotations

import importlib
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import initialize_database
from vnext_test_database import DatabaseFactory

from h2hdb import vnext_cleanup_repository as cleanup


@pytest.fixture(scope="module")
def dictionary_probe() -> ModuleType:
    before = sys.path[:]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        return importlib.import_module("cleanup_dictionary_selection_probe")
    finally:
        sys.path[:] = before


def test_dictionary_bounded_selector_preserves_complete_binary_order_and_cursors(
    database_factory: DatabaseFactory, dictionary_probe: ModuleType
) -> None:
    config = database_factory.config()
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        corpus = dictionary_probe.seed(connector, 256, pending_rows=64)
        with connector.read_transaction():
            records = dictionary_probe.compare_selectors(connector, corpus)
        assert len(records) == 14
        assert all(record["candidate_budget_met"] for record in records)
        assert {record["cut"] for record in records} == {
            "initial",
            "continuation-first",
            "continuation-middle",
            "continuation-last",
        }


@pytest.mark.parametrize("batch_dictionary", [False, True])
def test_dictionary_phase_preserves_exact_deletions_and_resumes_after_rollback(
    database_factory: DatabaseFactory,
    dictionary_probe: ModuleType,
    batch_dictionary: bool,
) -> None:
    config = database_factory.config()
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        corpus = dictionary_probe.seed(
            connector,
            128,
            pending_rows=512,
            dual_reference_sorts=batch_dictionary,
        )
        with (
            pytest.raises(RuntimeError, match="synthetic response loss"),
            connector.transaction(),
        ):
            operation = dictionary_probe.operation(
                connector, tuple(dictionary_probe.target(index) for index in range(16))
            )
            mutation = cleanup._run_static_phase(
                operation, b"", dictionary_probe.PLAN, "CV_DICTIONARY"
            )
            assert 0 < len(mutation.row_keys) <= 64
            raise RuntimeError("synthetic response loss before commit")
        baseline = dictionary_probe.run_phase(
            connector, baseline=True, batch_dictionary=batch_dictionary
        )
        dictionary_probe._restore(connector, corpus)
        candidate = dictionary_probe.run_phase(
            connector, baseline=False, batch_dictionary=batch_dictionary
        )
        assert candidate == baseline
        assert sum(len(keys) for _, keys in candidate) == 1024
        assert all(0 < len(keys) <= 64 for _, keys in candidate[:-1])
        assert all(
            previous[0] != current[0]
            for previous, current in zip(candidate, candidate[1:-1], strict=False)
        )
        assert candidate[-1][1] == ()


def test_dictionary_root_admission_is_fresh_and_terminal_retention_stays_exact(
    database_factory: DatabaseFactory, dictionary_probe: ModuleType
) -> None:
    config = database_factory.config()
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        dictionary_probe.seed(connector, 16, pending_rows=16)
        roots = (dictionary_probe.target(0),)
        spec = dictionary_probe.DICTIONARIES[0]
        with connector.transaction():
            initial = dictionary_probe.select(connector, spec, roots)
            assert initial
            connector.execute(
                "INSERT INTO operational_ingest_generations "
                "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                (1, 0, None),
            )
            connector.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (%s, %s)",
                (1, roots[0]),
            )
            assert dictionary_probe.select(connector, spec, roots) == []
            assert dictionary_probe.select(connector, spec, roots, baseline=True) == []
            with pytest.raises(cleanup.CleanupRetentionBlockedError):
                cleanup._run_static_phase(
                    dictionary_probe.operation(connector, roots),
                    b"",
                    dictionary_probe.PLAN,
                    "CV_DICTIONARY",
                )
            connector.execute(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (1, roots[0]),
            )
            assert dictionary_probe.select(connector, spec, roots) == initial


def test_dictionary_metadata_rejects_unproved_sort_and_reference_shapes() -> None:
    spec = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.CANONICAL_VALUE].phases[
        "CV_DICTIONARY"
    ][1]
    with pytest.raises(RuntimeError, match="selection metadata"):
        replace(spec, primary_key=("source_gallery_name",))
    with pytest.raises(RuntimeError, match="selection metadata"):
        replace(spec, canonical_dictionary_columns=("title_sha256", "title_sha256"))


def test_dictionary_work_contract_rejects_retained_dictionary_scan(
    dictionary_probe: ModuleType,
) -> None:
    bound = dictionary_probe.selector_budget(
        "mariadb", roots=256, rows=0, empty_fixture=True
    )
    assert 131_072 > bound
    assert bound == 20_736
