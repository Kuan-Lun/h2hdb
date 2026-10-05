"""Physical-work evidence for the retained publication-history keyset page.

This is an FK-valid history subgraph, not a complete READY database or a claim
that the resident orchestrator retains this many revisions. H is distinct
receipts, each with the one response retained by the real finalization writer.
"""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import pytest
from test_catalog_refinement_runtime import (
    _insert_exact_canonical_payload,
    _insert_title_policies,
    _ReadRecorder,
)
from vnext_test_database import (
    DatabaseFactory,
    assert_foreign_key_integrity,
    connector_backend,
    open_generated_database,
)

from h2hdb import catalog_refinement, vnext_identity
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector

PAGE = 128
HANDLER_BUDGET = 16 * (PAGE + 1) + 64
VM_BUDGET = 128 * (PAGE + 1) + 256


def _identity(prefix: bytes, generation: int) -> bytes:
    return prefix + generation.to_bytes(15, "big")


def _history(connector: SQLConnector, count: int) -> None:
    """Seed exact FK dependencies without disabling checks or adding old responses."""
    with connector.transaction():
        policy = _insert_exact_canonical_payload(
            connector,
            domain="artifact_policy_v3",
            payload=vnext_identity.encode_artifact_policy(2, b"cost-probe", b"p" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_adapter_policy VALUES (%s, %s)",
            (b"p" * 32, b"cost-probe"),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policy_semantics VALUES (%s, 2, %s)",
            (policy, b"p" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policies VALUES (1, %s)", (policy,)
        )
        _insert_title_policies(connector, display_policy_id=1)
        connector.execute(
            "INSERT INTO operational_operational_policys VALUES (1, 1, 1, 128)"
        )
        for generation in range(1, count + 1):
            receipt = _identity(b"r", generation)
            preparation = _identity(b"p", generation)
            connector.execute(
                "INSERT INTO catalog_revision_descriptors VALUES (%s, 0, 0)",
                (generation,),
            )
            connector.execute(
                "INSERT INTO catalog_source_revision_descriptors VALUES (%s, %s, %s)",
                (generation, b"default", b"s" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_publication_generation_nodes VALUES (%s)",
                (generation,),
            )
            connector.execute(
                "INSERT INTO catalog_publication_generation_successors VALUES (%s, %s)",
                (generation, generation - 1),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commit_anchors VALUES (%s)", (receipt,)
            )
            connector.execute(
                "INSERT INTO operational_operational_event_streams VALUES (%s, 1)",
                (preparation,),
            )
            connector.execute(
                "INSERT INTO operational_operational_preparation_effect_seals VALUES (%s, 0, %s, 1)",
                (preparation, b"e" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_publication_finalization_checkpoints "
                "(receipt_id, generation, `cursor`, processed_count, state, updated_at) "
                "VALUES (%s, 2, %s, 0, 'COMPLETE', 2)",
                (receipt, b""),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commits "
                "(receipt_id, candidate_id, revision, source_revision, generation, "
                "preparation_id, operational_policy_id, artifact_policy_id, "
                "display_title_policy_id, new_galleries, changed_galleries, "
                "removed_galleries, duplicate_losers, committed_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, 1, 1, 1, 0, 0, 0, 0, 1)",
                (
                    receipt,
                    _identity(b"c", generation),
                    generation,
                    generation,
                    generation,
                    preparation,
                ),
            )
            connector.execute(
                "INSERT INTO catalog_publication_finalization_batch_stored "
                "(receipt_id, start_generation, batch_key, start_cursor, "
                "start_processed_count, next_cursor, row_count, committed_at) "
                "VALUES (%s, 1, %s, %s, 0, %s, 0, 2)",
                (receipt, b"terminal", b"", b""),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commit_finalizations VALUES (%s)",
                (receipt,),
            )
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts VALUES (%s, %s)",
            (b"default", _identity(b"r", count)),
        )
    assert_foreign_key_integrity(connector)


def _expected(generation: int) -> tuple[Any, ...]:
    receipt = _identity(b"r", generation)
    return (
        receipt,
        _identity(b"c", generation),
        generation,
        generation,
        generation,
        1,
        _identity(b"p", generation),
        None,
        2,
        b"",
        0,
        "COMPLETE",
        2,
        receipt,
        b"",
        0,
        b"",
        0,
        "COMPLETE",
        0,
        1,
        2,
        2,
    )


def _handler_counts(connector: SQLConnector) -> dict[str, int]:
    values = dict(connector.fetch_all("SHOW SESSION STATUS LIKE 'Handler_read_%'"))
    required = {
        "Handler_read_first",
        "Handler_read_key",
        "Handler_read_last",
        "Handler_read_next",
        "Handler_read_prev",
        "Handler_read_rnd",
        "Handler_read_rnd_deleted",
        "Handler_read_rnd_next",
    }
    assert required <= values.keys()
    return {name: int(values[name]) for name in required}


def _measure(
    connector: SQLConnector, query: str, data: tuple[Any, ...]
) -> tuple[list[tuple[Any, ...]], int]:
    if isinstance(connector, SQLiteConnector):
        steps = 0

        def progress() -> int:
            nonlocal steps
            steps += 1
            return 0

        connector.connection.set_progress_handler(progress, 1)
        try:
            rows = connector.fetch_all(query, data)
        finally:
            connector.connection.set_progress_handler(None, 0)
        return rows, steps
    assert isinstance(connector, MariaDBConnector)
    before = _handler_counts(connector)
    assert _handler_counts(connector) == before, "counter observer adds measured work"
    rows = connector.fetch_all(query, data)
    after = _handler_counts(connector)
    assert all(after[name] >= before[name] for name in before)
    return rows, sum(after[name] - before[name] for name in before)


@pytest.mark.deep
@pytest.mark.parametrize("history", (127, 128, 129, 4096))
def test_retained_history_pages_bound_native_work_and_reject_a_full_scan(
    database_factory: DatabaseFactory, history: int, tmp_path: Path
) -> None:
    evidence: list[dict[str, Any]] = []
    with closing(open_generated_database(database_factory.config())) as connector:
        _history(connector, history)
        budget = (
            VM_BUDGET if connector_backend(connector) == "sqlite" else HANDLER_BUDGET
        )
        with connector.read_transaction():
            recorder = _ReadRecorder(connector)
            catalog_refinement._validate_publication_generation_history(
                cast(Any, recorder)
            )
            pages = [
                (sql, data, count)
                for sql, data, count in recorder.reads
                if "ORDER BY committed.generation LIMIT %s" in sql
            ]
            assert sum(count for _sql, _data, count in pages) == history
            sql = pages[0][0]
            # Capture the unchanged runtime query; the independent fixture model
            # supplies every output column and all sampled cursor memberships.
            cursors = sorted(
                {
                    -1,
                    (history // (2 * PAGE)) * PAGE,
                    ((history - 1) // PAGE) * PAGE,
                    history,
                }
            )
            degraded = sql.replace(
                "committed.generation > %s", "committed.generation + 0 > %s"
            ).replace(
                "ORDER BY committed.generation LIMIT",
                "ORDER BY committed.generation + 0 LIMIT",
            )
            assert degraded != sql
            rejected = False
            try:
                for cycle in range(3):
                    for cursor in cursors:
                        expected = [
                            _expected(g)
                            for g in range(
                                max(1, cursor + 1),
                                min(history + 1, max(1, cursor + 1) + PAGE),
                            )
                        ]
                        rows, cost = _measure(connector, sql, (cursor, PAGE))
                        assert rows == expected
                        mutant_rows, mutant_cost = _measure(
                            connector, degraded, (cursor, PAGE)
                        )
                        assert mutant_rows == expected
                        rejected |= mutant_cost > budget
                        evidence.append(
                            {
                                "cycle": cycle,
                                "cursor": cursor,
                                "returned_rows": len(rows),
                                "production": cost,
                                "full_scan": mutant_cost,
                                "budget": budget,
                            }
                        )
                        assert cost <= budget, evidence[-1]
                if history == 4096:
                    assert rejected, "same-result full scan was not rejected"
            finally:
                (tmp_path / "history-work.json").write_text(
                    json.dumps(
                        {
                            "backend": connector_backend(connector),
                            "history": history,
                            "receipts_per_owner": 1,
                            "measurements": evidence,
                        },
                        indent=2,
                    )
                )
