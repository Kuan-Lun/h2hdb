"""Paired engine costs on valid public graphs, including dictionary fanout."""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import LEASE_MICROSECONDS, Clock, full_check

from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import CleanupRetentionBlockedError


@pytest.fixture
def dictionary_probe() -> Iterator[ModuleType]:
    before = list(sys.path)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        yield importlib.import_module("cleanup_dictionary_probe")
    finally:
        sys.path[:] = before


def test_fixed_budget_rejects_old_empty_scan_and_unbounded_hit_work(
    dictionary_probe: ModuleType,
) -> None:
    record: dict[str, Any] = {
        "returned_rows": 0,
        "frozen_roots": 2,
        "baseline_work": 6736,
        "candidate_work": 6736,
    }
    for backend in ("sqlite", "mariadb"):
        with pytest.raises(AssertionError, match="empty dictionary"):
            dictionary_probe.assert_query_budget(record, backend)
        with pytest.raises(AssertionError, match="hit selection"):
            dictionary_probe.assert_query_budget(
                {
                    **record,
                    "returned_rows": 16,
                    "baseline_work": 4965,
                    "candidate_work": 52470,
                },
                backend,
            )
    with pytest.raises(ValueError, match="frozen roots"):
        dictionary_probe.empty_budget("sqlite", 0)


@pytest.mark.deep
@pytest.mark.performance_acceptance
@pytest.mark.parametrize(
    "galleries,replacements,shared_title",
    [
        (64, 64, False),
        (256, 64, False),
        (255, 255, True),
        (256, 256, True),
        (257, 257, True),
        (513, 513, True),
    ],
)
def test_public_cleanup_dictionary_native_budget_and_capacity(
    db_config: CoreConfig,
    dictionary_probe: ModuleType,
    tmp_path: Path,
    galleries: int,
    replacements: int,
    shared_title: bool,
) -> None:
    result = dictionary_probe.run_case(
        db_config,
        galleries=galleries,
        replacements=replacements,
        shared_title=shared_title,
    )
    (tmp_path / "cleanup-dictionary-cost.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    assert result["ready_before"] == result["ready_after"] == "READY"
    assert result["cleanup_done"] and result["next_claim_obtained"]
    assert result["foreign_keys_enabled"]
    assert result["measured_connections_with_foreign_keys_verified"] > 0
    assert len(result["protected_empty_cases"]) == 4
    assert all(
        case["raw_children_present"] and case["returned_rows"] == 0
        for case in result["protected_empty_cases"]
    )
    if db_config.database.sql_type == "sqlite":
        # Keep the failed broader goal visible; measurement completion and
        # narrower warm absence gains must never overwrite this counterexample.
        assert not result["original_per_query_contract_met"]
        assert result["original_per_query_contract_failures"]
    assert (
        result["selection_native_candidate_total"]
        <= result["selection_native_baseline_total"]
    )
    records = result["records"]
    if shared_title:
        # Shared source-title roots own many actual child PKs; this exercises
        # a real full page and resumed durable keyset, not a larger input label.
        assert max(record["returned_rows"] for record in records) >= min(galleries, 256)
        if galleries > 256:
            assert any(
                record["has_after"] and record["returned_rows"] for record in records
            )
        negative = result["fanout_negative_control"]
        assert negative is not None
        # Native oracles differ: SQLite repeats correlated eligibility in the
        # rejected child-dependent CASE, whereas MariaDB may cache it per root.
        if db_config.database.sql_type == "sqlite":
            assert negative["rejected_by_same_hit_budget"]
    else:
        assert max(record["frozen_roots"] for record in records) >= 3
        snapshot = result["same_snapshot_root_subsets"]
        assert snapshot["same_transaction_no_mutations_between_cases"]
        assert {case["frozen_roots"] for case in snapshot["cases"]} == {1, 2, 3}
        rejected = []
        for record in records:
            try:
                dictionary_probe.assert_query_budget(
                    {**record, "candidate_work": record["baseline_work"]},
                    db_config.database.sql_type,
                )
            except AssertionError:
                rejected.append(record)
        assert rejected, "same fixed budget must reject the original empty scan"


@pytest.mark.deep
@pytest.mark.cleanup_acceptance
@pytest.mark.parametrize("fault", ["retention", "lease_expiry"])
def test_public_dictionary_selection_rechecks_fresh_authority_after_selection(
    db_config: CoreConfig,
    dictionary_probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    dictionary_probe.prepare_fixture(
        db_config, galleries=4, replacements=4, shared_title=True
    )
    with closing(open_connector(db_config)) as connector:
        connector_type: type[SQLConnector] = type(connector)
    original = connector_type.fetch_all
    clock = Clock()
    selected: tuple[Any, ...] | None = None
    generation: int | None = None

    def fetch(
        self: SQLConnector, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        nonlocal selected, generation
        rows = original(self, query, data)
        if (
            selected is None
            and rows
            and "FROM catalog_display_title_choices AS c" in query
            and "CASE WHEN (EXISTS" in query
            and "ORDER BY" in query
        ):
            selected = rows[0]
            if fault == "retention":
                generation = int(
                    self.fetch_one(
                        "SELECT MAX(generation) FROM operational_ingest_generations"
                    )[0]
                )
                # Deliberate transaction-local fault between selection and exact
                # lock. This uses existing FK parents and must roll back; it is
                # not presented as a supported concurrent writer under the gate.
                self.execute(
                    "INSERT INTO operational_canonical_value_uploads (generation, value_sha256) VALUES (%s, %s)",
                    (generation, selected[0]),
                )
            else:
                # Simulate time spent between selection and its fresh fence.
                # No database clock, durable lease, or authority row is changed.
                clock._offset += 2 * LEASE_MICROSECONDS
        return rows

    monkeypatch.setattr(connector_type, "fetch_all", fetch)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        if fault == "retention":
            with pytest.raises(CleanupRetentionBlockedError, match="retention root"):
                dictionary_probe._drain(facade)
        else:
            for _ in range(dictionary_probe.MAX_DRAIN_CALLS):
                outcome = facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
                if selected is not None:
                    # A transaction admitted under the locked gate may commit
                    # after wall-clock expiry. The facade must then stop with
                    # PROGRESSED, never reuse that lease or claim DONE.
                    assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
                    break
            else:
                pytest.fail("dictionary selection boundary was not exercised")
    monkeypatch.setattr(connector_type, "fetch_all", original)
    assert selected is not None
    with closing(open_connector(db_config)) as connector, connector.read_transaction():
        if generation is not None:
            assert connector.fetch_one(
                "SELECT title_sha256 FROM catalog_display_title_choices WHERE display_title_policy_id = %s AND source_title_sha256 = %s AND source_gallery_name = %s",
                selected[1:],
            )
            assert (
                connector.fetch_one(
                    "SELECT 1 FROM operational_canonical_value_uploads WHERE generation = %s AND value_sha256 = %s",
                    (generation, selected[0]),
                )
                == ()
            )
    assert full_check(db_config).state == "READY"
    # Reopen after the failed response; fresh admission and the exact selected
    # keys must be recoverable on both native engines.
    with VNextIngestFacade(db_config, clock=clock) as facade:
        dictionary_probe._drain(facade)
        claim = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
        assert claim is not None
        facade.complete_ingest(claim)
    assert full_check(db_config).state == "READY"
