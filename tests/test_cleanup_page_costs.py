"""Canonical-page cost, atomic family deletion and durable retry contracts."""

from __future__ import annotations

import importlib
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
from test_vnext_cleanup_repository import (
    _advance,
    _advance_to_cleanup_phase,
    _begin,
    _cleanup_protocol_snapshot,
    _exclusive,
)
from vnext_test_database import (
    DatabaseFactory,
    assert_foreign_key_integrity,
    inspect_all,
    open_generated_database,
)

from h2hdb.vnext_cleanup_repository import CleanupTargetKind

pytestmark = [pytest.mark.cleanup_acceptance, pytest.mark.deep]


@pytest.fixture
def page_probe() -> ModuleType:
    before = list(sys.path)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        return importlib.import_module("cleanup_page_cost_probe")
    finally:
        sys.path[:] = before


@pytest.mark.parametrize("pending", (1, 63, 64, 65, 257))
def test_page_phase_cost_rejects_scalar_control_and_preserves_exact_trace(
    database_factory: DatabaseFactory,
    tmp_path: Path,
    page_probe: ModuleType,
    pending: int,
) -> None:
    with closing(
        open_generated_database(database_factory.config(str(tmp_path / "pages.db")))
    ) as database:
        roots = page_probe.seed_pages(database, 257, pending)
        original = database.execute_affected

        def bounded_delete(sql: str, data: tuple[object, ...] = ()) -> int:
            assert len(data) <= 900
            return original(sql, data)

        with patch.object(database, "execute_affected", side_effect=bounded_delete):
            scalar = page_probe.measure_phase(database, roots, scalar=True)
            production = page_probe.measure_phase(database, roots, scalar=False)
        assert production["trace"] == scalar["trace"]
        budget = page_probe.call_budget(pending, len(roots))
        assert production["sql_calls"] <= budget
        if pending >= 63:
            assert scalar["sql_calls"] > budget
        assert_foreign_key_integrity(database)


def test_page_compound_delete_faults_roll_back_families_and_durable_checkpoint(
    database_factory: DatabaseFactory, tmp_path: Path, page_probe: ModuleType
) -> None:
    with closing(
        open_generated_database(database_factory.config(str(tmp_path / "faults.db")))
    ) as database:
        page_probe.seed_pages(database, 17, 65)
        gate = _exclusive(database)
        cycle = _begin(
            database, gate, CleanupTargetKind.CANONICAL_VALUE, 129, max_rows=256
        )
        _advance_to_cleanup_phase(database, gate, cycle, "CV_PAGE")
        tables = tuple(table for table, _key in page_probe.FAMILIES)

        def family_rows() -> tuple[list[tuple[object, ...]], ...]:
            return tuple(
                inspect_all(database, f"SELECT * FROM {table} ORDER BY 1, 2")
                if table
                not in {
                    "catalog_canonical_value_allocation_seals",
                    "catalog_canonical_value_page_anchors",
                }
                else inspect_all(database, f"SELECT * FROM {table} ORDER BY 1")
                for table in tables
            )

        before = family_rows()
        checkpoint = _cleanup_protocol_snapshot(database)
        original = database.execute_affected
        for failed_table in (
            "catalog_canonical_value_page_coordinates",
            "catalog_canonical_value_page_payloads",
            "catalog_canonical_value_page_subtree_item_counts",
            "catalog_canonical_value_page_anchors",
        ):
            triggered = False

            def fail_after_delete(sql: str, data: tuple[object, ...] = ()) -> int:
                nonlocal triggered
                affected = original(sql, data)
                if sql.startswith(f"DELETE FROM {failed_table} "):
                    triggered = True
                    raise RuntimeError("injected page family fault after deletion")
                return affected

            with (
                patch.object(
                    database, "execute_affected", side_effect=fail_after_delete
                ),
                pytest.raises(RuntimeError, match="page family fault"),
            ):
                _advance(database, gate, cycle, 1, b"p" * 32, now=80)
            assert triggered
            assert family_rows() == before
            assert _cleanup_protocol_snapshot(database) == checkpoint
            assert_foreign_key_integrity(database)

        committed = _advance(database, gate, cycle, 1, b"p" * 32, now=81)
        after = family_rows()
        # A lost response must replay the committed result, without deleting
        # more keys or advancing the next durable checkpoint.
        replay = _advance(database, gate, cycle, 1, b"p" * 32, now=82)
        assert replay.replayed
        assert replay == replace(committed, replayed=True)
        assert committed.row_count >= 65
        assert family_rows() == after
        assert_foreign_key_integrity(database)


def test_page_batch_allows_independently_missing_optional_children(
    database_factory: DatabaseFactory, tmp_path: Path, page_probe: ModuleType
) -> None:
    with closing(
        open_generated_database(database_factory.config(str(tmp_path / "optional.db")))
    ) as database:
        roots = page_probe.seed_pages(database, 17, 65)
        with database.transaction():
            for table, position in (
                ("catalog_canonical_value_page_payloads", 0),
                ("catalog_canonical_value_page_subtree_item_counts", 1),
            ):
                database.execute(
                    f"DELETE FROM {table} WHERE page_sha256 = %s",
                    (b"\x82" + position.to_bytes(31, "big"),),
                )
        scalar = page_probe.measure_phase(database, roots, scalar=True)
        production = page_probe.measure_phase(database, roots, scalar=False)
        assert production["trace"] == scalar["trace"]
        assert_foreign_key_integrity(database)
