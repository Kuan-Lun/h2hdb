"""Exact batched CP deletion, fencing, rollback, and replay on real backends."""

from __future__ import annotations

import json
import tomllib
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import (
    backend_of,
    open_connector,
    physical_tables,
    snapshot_database,
)
from vnext_publication_cleanup_fixtures import (
    OLD_RECEIPT,
    PUBLICATION_KEY,
    partial_publication_setup,
    seed_publication_cleanup,
)

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig, vnext_identity
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupBatchResult,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupUnavailableError,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_VARIABLE_KEYS = (
    b"",
    b"\0",
    b"a",
    b"a\0",
    b"a" * 16,
    b"a" * 16 + b"\0",
    b"a" * 32,
    b"a" * 32 + b"\0",
    b"a" * 128,
)


class _AbortTransaction(RuntimeError):
    pass


class _LostCommittedResponse(RuntimeError):
    pass


def _advance(
    config: CoreConfig,
    connector: SQLConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int = 3,
) -> tuple[CleanupBatchResult, ...]:
    return VNextCleanupRepository.advance_current_only_cycle(
        VNextUnitOfWork(connector, backend=backend_of(config)),
        gate_lease=gate,
        cycle=cycle,
        now=now,
    )


def _snapshot(config: CoreConfig) -> dict[str, tuple[tuple[Any, ...], ...]]:
    names = tuple(
        name
        for name in physical_tables(backend_of(config))
        if name.startswith(("operational_cleanup_", "operational_maintenance_gate_"))
        or name
        in {
            "catalog_title_search_postings",
            "catalog_subjects",
            "catalog_contributors",
            "catalog_tag_directory_order",
            "catalog_tag_publication_order",
            "catalog_publication_occurrence_identities",
            "catalog_publication_commit_head_receipts",
        }
    )
    return snapshot_database(config, tables=names)


def _finish(config: CoreConfig, gate: GateLease, cycle: CleanupCycle) -> None:
    for now in range(10, 40):
        with closing(open_connector(config)) as connector, connector.transaction():
            results = _advance(config, connector, gate, cycle, now=now)
        assert sum(result.row_count for result in results) <= 256
        if results[-1].cycle_complete:
            break
    else:
        pytest.fail("bounded fixture did not finish cleanup")
    completed = _snapshot(config)
    with closing(open_connector(config)) as connector, connector.transaction():
        replay = _advance(config, connector, gate, cycle, now=50)
    assert len(replay) == 1 and replay[0].replayed and replay[0].cycle_complete
    assert _snapshot(config) == completed
    with closing(open_connector(config)) as connector, connector.read_transaction():
        assert connector.fetch_all(
            "SELECT revision, publication_key "
            "FROM catalog_publication_occurrence_identities ORDER BY revision"
        ) == [(2, PUBLICATION_KEY)]


@pytest.mark.parametrize(
    "change",
    (
        {"delete_sql": ("DELETE FROM catalog_subjects WHERE revision = %s",)},
        {"delete_parameter_indexes": ((0, 1, 2),)},
        {"delete_allowed_affected": (frozenset((0, 1)),)},
    ),
)
def test_batch_opt_in_refuses_partial_key_or_compound_delete_contracts(
    change: dict[str, Any],
) -> None:
    plan = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.CATALOG_PUBLICATION]
    spec = plan.phases["CP_SUBJECT"][0]
    with pytest.raises(RuntimeError, match="one exact primary-key delete"):
        replace(spec, **change)


def test_every_batch_opt_in_matches_the_complete_manifest_primary_key() -> None:
    manifest_path = (
        Path(__file__).resolve().parents[1] / "verification/schema/physical.toml"
    )
    manifest = tomllib.loads(manifest_path.read_text())
    primary_keys = {
        relation["table"]: tuple(relation["primary_key"])
        for relation in manifest["relation"]
        if "table" in relation and "view" not in relation
    }
    targets: set[cleanup.CleanupTargetKind] = set()
    for plan in cleanup._STATIC_PLANS.values():
        for specs in plan.phases.values():
            for spec in specs:
                if spec.batch_exact_primary_keys:
                    targets.add(plan.kind)
                    assert spec.primary_key == primary_keys[spec.table]
    assert targets == {cleanup.CleanupTargetKind.CATALOG_PUBLICATION}


def test_indirect_namespace_grid_preserves_empty_nul_and_variable_width_keys(
    db_config: CoreConfig,
) -> None:
    gate, cycle = seed_publication_cleanup(
        db_config,
        rows=len(_VARIABLE_KEYS),
        phase="CP_ORDER",
        variable_keys=_VARIABLE_KEYS,
    )
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            with patch.object(
                connector, "fetch_all", wraps=connector.fetch_all
            ) as many:
                results = _advance(db_config, connector, gate, cycle)
            assert results[-1].row_count == 2 * len(_VARIABLE_KEYS)
            assert results[-1].phase == "CP_ORDER"
            locks = [
                call for call in many.call_args_list if "cleanup_order" in call.args[0]
            ]
            assert len(locks) == 2
            assert all(len(call.args[1]) <= 900 for call in locks)
        assert connector.fetch_all(
            "SELECT revision, namespace FROM catalog_tag_directory_order "
            "ORDER BY revision, namespace"
        ) == [(2, key) for key in sorted(_VARIABLE_KEYS)]
    _finish(db_config, gate, cycle)


@pytest.mark.deep
def test_contributor_grid_preserves_binary_roles_at_fixed_digest_widths(
    db_config: CoreConfig,
) -> None:
    keys = (*_VARIABLE_KEYS[:-1], b"a" * 64)
    gate, cycle = seed_publication_cleanup(
        db_config,
        rows=len(keys),
        phase="CP_CONTRIBUTOR",
        variable_keys=keys,
    )
    with closing(open_connector(db_config)) as connector, connector.transaction():
        assert _advance(db_config, connector, gate, cycle)[-1].row_count == len(keys)
        assert connector.fetch_all(
            "SELECT revision, role FROM catalog_contributors "
            "ORDER BY revision, position"
        ) == [(2, key) for key in keys]
    _finish(db_config, gate, cycle)


@pytest.mark.deep
@pytest.mark.parametrize(
    "fault", ("missing", "extra", "duplicate", "reordered", "type")
)
def test_exact_locked_set_rejects_changed_results_before_any_delete(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=3, phase="CP_STORAGE")
    before = _snapshot(db_config)
    with closing(open_connector(db_config)) as connector:
        original = connector.fetch_all

        def corrupt(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            rows = original(query, data)
            if "cleanup_order" not in query:
                return rows
            assert len(rows) == 3
            match fault:
                case "missing":
                    return rows[:-1]
                case "extra":
                    return [*rows, (2, *rows[0][1:])]
                case "duplicate":
                    return [rows[0], rows[0], rows[2]]
                case "reordered":
                    return list(reversed(rows))
            return [(str(rows[0][0]), *rows[0][1:]), *rows[1:]]

        monkeypatch.setattr(connector, "fetch_all", corrupt)
        with patch.object(
            connector, "execute_affected", wraps=connector.execute_affected
        ) as affected:
            with pytest.raises(CleanupRetentionBlockedError), connector.transaction():
                _advance(db_config, connector, gate, cycle)
        assert not any(
            call.args[0].lstrip().startswith("DELETE")
            for call in affected.call_args_list
        )
    assert _snapshot(db_config) == before


@pytest.mark.deep
@pytest.mark.parametrize("fault", ("short_count", "extra_count", "checkpoint"))
def test_later_page_or_checkpoint_failure_rolls_back_all_pages_and_receipts(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=65, phase="CP_STORAGE")
    before = _snapshot(db_config)
    deleted_pages: list[int] = []
    with closing(open_connector(db_config)) as connector:
        original = connector.execute_affected

        def affected(query: str, data: tuple[Any, ...] = ()) -> int:
            count = original(query, data)
            if query.startswith("DELETE FROM catalog_title_search_postings "):
                deleted_pages.append(count)
                if len(deleted_pages) == 2 and fault != "checkpoint":
                    return count + (-1 if fault == "short_count" else 1)
            return count

        monkeypatch.setattr(connector, "execute_affected", affected)
        if fault == "checkpoint":
            advance_checkpoint = cleanup._advance_checkpoint

            def abort_checkpoint(*args: Any, **kwargs: Any) -> Any:
                advance_checkpoint(*args, **kwargs)
                raise _AbortTransaction("after actual checkpoint update")

            monkeypatch.setattr(cleanup, "_advance_checkpoint", abort_checkpoint)
        expected = (
            _AbortTransaction if fault == "checkpoint" else CleanupUnavailableError
        )
        with pytest.raises(expected), connector.transaction():
            _advance(db_config, connector, gate, cycle)
    assert deleted_pages == [64, 1]
    assert _snapshot(db_config) == before
    monkeypatch.undo()
    _finish(db_config, gate, cycle)


@pytest.mark.deep
def test_new_retention_between_selection_and_lock_aborts_the_page(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=3, phase="CP_STORAGE")
    before = _snapshot(db_config)
    with closing(open_connector(db_config)) as connector:
        original = connector.fetch_all
        changed = False

        def retain(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            nonlocal changed
            rows = original(query, data)
            if (
                not changed
                and "FROM catalog_title_search_postings AS c " in query
                and "cleanup_order" not in query
                and rows
            ):
                changed = True
                connector.execute(
                    "UPDATE catalog_publication_commit_head_receipts "
                    "SET receipt_id = %s WHERE channel = %s",
                    (OLD_RECEIPT, b"default"),
                )
            return rows

        monkeypatch.setattr(connector, "fetch_all", retain)
        with pytest.raises(CleanupRetentionBlockedError), connector.transaction():
            _advance(db_config, connector, gate, cycle)
        assert changed
    assert _snapshot(db_config) == before


@pytest.mark.deep
def test_duplicate_candidate_join_is_deleted_once_and_advances_sql_cursor(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=3, phase="CP_STORAGE")
    with closing(open_connector(db_config)) as connector:
        original = connector.fetch_all
        injected = False

        def duplicate(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            nonlocal injected
            rows = original(query, data)
            if (
                not injected
                and "FROM catalog_title_search_postings AS c " in query
                and "cleanup_order" not in query
                and rows
            ):
                injected = True
                return [rows[0], *rows]
            return rows

        monkeypatch.setattr(connector, "fetch_all", duplicate)
        with connector.transaction():
            result = _advance(db_config, connector, gate, cycle)[-1]
        assert injected and result.row_count == result.deleted_count == 3
        assert result.cursor == cleanup._encode_static_cursor(
            0, (1, PUBLICATION_KEY, 1, (3).to_bytes(32, "big"), PUBLICATION_KEY)
        )
    _finish(db_config, gate, cycle)


@pytest.mark.deep
def test_new_eligible_root_in_same_shard_is_outside_frozen_batch_authority(
    db_config: CoreConfig,
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=3, phase="CP_STORAGE")
    later_key = bytes((201,)) + (1).to_bytes(31, "big")
    with closing(open_connector(db_config)) as connector:
        with partial_publication_setup(connector, backend=backend_of(db_config)):
            connector.execute(
                "INSERT INTO catalog_publication_identities (publication_key, gid) "
                "VALUES (%s, 8)",
                (later_key,),
            )
            connector.execute(
                "INSERT INTO catalog_publication_occurrence_identities "
                "(catalog_occurrence_sha256, revision, publication_key) "
                "VALUES (%s, 1, %s)",
                (
                    vnext_identity.catalog_publication_occurrence_sha256(1, later_key),
                    later_key,
                ),
            )
            connector.execute(
                "INSERT INTO catalog_title_search_postings "
                "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
                ((1).to_bytes(32, "big"), later_key),
            )
        with connector.transaction():
            assert _advance(db_config, connector, gate, cycle)[-1].row_count == 3
        assert connector.fetch_all(
            "SELECT revision, publication_key FROM catalog_title_search_postings "
            "ORDER BY revision, value_sha256"
        ) == [(1, later_key), *[(2, PUBLICATION_KEY)] * 3]


@pytest.mark.deep
def test_lost_commit_response_replays_without_reissuing_page_deletes(
    db_config: CoreConfig,
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=65, phase="CP_STORAGE")
    command = CleanupBatchCommand(
        b"publication-batch-response-loss".ljust(32, b"\0"), 1
    )
    with pytest.raises(_LostCommittedResponse):
        with closing(open_connector(db_config)) as connector:
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    gate_lease=gate,
                    cycle=cycle,
                    command=command,
                    now=3,
                )
                assert result.row_count == result.deleted_count == 65
            raise _LostCommittedResponse
    persisted = _snapshot(db_config)
    with closing(open_connector(db_config)) as connector, connector.transaction():
        with patch.object(
            connector, "execute_affected", wraps=connector.execute_affected
        ) as mutations:
            replay = VNextCleanupRepository.advance(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                gate_lease=gate,
                cycle=cycle,
                command=command,
                now=4,
            )
        mutations.assert_not_called()
        assert replay == replace(result, replayed=True)
    assert _snapshot(db_config) == persisted
    with closing(open_connector(db_config)) as connector:
        with pytest.raises(CleanupUnavailableError), connector.transaction():
            VNextCleanupRepository.advance(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                gate_lease=gate,
                cycle=cycle,
                command=replace(command, batch_key=b"stale".ljust(32, b"\0")),
                now=5,
            )
    assert _snapshot(db_config) == persisted
    _finish(db_config, gate, cycle)


@pytest.mark.deep
def test_exact_lock_query_uses_indexed_lookups_with_unrelated_rows(
    db_config: CoreConfig,
    tmp_path: Path,
) -> None:
    # All 1,027 old rows share the same eligible frozen root. A lookup using
    # only the revision/publication prefix would visit them for each grid key.
    gate, cycle = seed_publication_cleanup(
        db_config, rows=1027, phase="CP_STORAGE", max_rows=3
    )
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            with patch.object(
                connector, "fetch_all", wraps=connector.fetch_all
            ) as many:
                _advance(db_config, connector, gate, cycle)
            lock_calls = [
                call for call in many.call_args_list if "cleanup_order" in call.args[0]
            ]
            assert len(lock_calls) == 1
            sql, parameters = lock_calls[0].args
            assert len(parameters) <= 900
            sql = sql.removesuffix(" FOR UPDATE")
            if backend_of(db_config) == "sqlite":
                plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
                (tmp_path / "cleanup-lock-plan.json").write_text(json.dumps(plan))
                child = [str(row[3]) for row in plan if " c " in str(row[3])]
                assert child and all("SEARCH c USING" in item for item in child)
                assert all(
                    "revision=? AND value_sha256=? AND publication_key=?" in item
                    for item in child
                ), plan
            else:
                plan = connector.fetch_all("EXPLAIN " + sql, parameters)
                (tmp_path / "cleanup-lock-plan.json").write_text(json.dumps(plan))
                # MariaDB traditional EXPLAIN: table, type, possible_keys, key,
                # key_len, ref, rows. The requested child must be a keyed probe.
                child_rows = [row for row in plan if row[2] == "c"]
                assert child_rows
                assert all(
                    row[3] in {"eq_ref", "ref", "const"} for row in child_rows
                ), plan
                assert all(row[5] == "PRIMARY" for row in child_rows), plan
                assert all(int(row[6]) == 8 + 32 + 32 for row in child_rows), plan
                assert all(int(row[8]) <= 1 for row in child_rows), plan
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_title_search_postings WHERE revision = 1"
        ) == (1024,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_title_search_postings WHERE revision = 2"
        ) == (1027,)
