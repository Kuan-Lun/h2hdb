"""Terminal absence proofs retain every family under bounded SQL work."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from vnext_generated_database import open_generated_sqlite_database

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance


def _cycle(kind: CleanupTargetKind) -> CleanupCycle:
    return CleanupCycle(
        cleanup_id=cleanup._cleanup_id(kind, 1, 1),
        target_kind=kind,
        shard_no=1,
        target_key=cleanup._target_key(kind, 1),
        cycle_generation=1,
        cycle_cutoff_at=100,
        max_rows_per_transaction=256,
        hash_cache_max_age_microseconds=0,
    )


def _query_roots(
    plan: cleanup._StaticTargetPlan, count: int
) -> tuple[tuple[cleanup._StaticScalar, ...], ...]:
    """Produce bind/parse inputs; these are not fabricated durable authority."""

    roots: list[tuple[cleanup._StaticScalar, ...]] = []
    for ordinal in range(count):
        values: list[cleanup._StaticScalar] = []
        for attribute in cleanup._frozen_root_attributes(plan):
            if attribute in cleanup._FROZEN_ROOT_INT_ATTRIBUTES:
                values.append(ordinal + 1)
            else:
                width = 16 if attribute in cleanup._FROZEN_ROOT_UUID_ATTRIBUTES else 32
                values.append(b"\x01" + ordinal.to_bytes(width - 1, "big"))
        roots.append(tuple(values))
    return tuple(roots)


@pytest.mark.parametrize("root_count", [1, 256])
def test_all_terminal_plans_execute_below_a_999_variable_connection_limit(
    tmp_path: Path, root_count: int
) -> None:
    with closing(
        open_generated_sqlite_database(tmp_path / "probe-budget.sqlite3")
    ) as connector:
        connector.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        for kind, plan in cleanup._STATIC_PLANS.items():
            roots = _query_roots(plan, root_count)
            predicate, bindings = cleanup._frozen_root_predicate(plan, roots)
            specs = tuple(spec for phase in plan.phases.values() for spec in phase)
            queries = tuple(
                cleanup._static_terminal_probe_batches(
                    plan=plan,
                    specs=specs,
                    frozen_root_predicate=predicate,
                    frozen_root_parameters=bindings,
                    shard_parameters=cleanup._static_shard_parameters(
                        plan, _cycle(kind)
                    ),
                )
            )
            assert queries
            if root_count == 1 and len(specs) > 1:
                assert len(queries) < len(specs)
            for query, parameters in queries:
                # Record the externally meaningful budget rather than merely
                # comparing the implementation's two constants to themselves.
                assert len(parameters) <= 900
                assert query.count(" LIMIT 1)") <= 8
                assert len(query.encode("utf-8")) < 64 * 1024
                assert connector.fetch_all("EXPLAIN QUERY PLAN " + query, parameters)
                assert connector.fetch_one(query, parameters) == ()


def _claim(connector: SQLiteConnector) -> GateLease:
    with connector.transaction():
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _advance(
    connector: SQLiteConnector, gate: GateLease, cycle: CleanupCycle, *, now: int
) -> None:
    with connector.transaction():
        VNextCleanupRepository.advance(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cycle=cycle,
            command=CleanupBatchCommand(now.to_bytes(32, "big"), 1),
            now=now,
        )


@pytest.mark.parametrize(
    ("table", "key_column", "key"),
    [
        ("catalog_a_file_decision_shadow_seals", "file_sha256", b"f" * 32),
        ("catalog_analysis_content_owner_candidate_tombstones", "gallery_id", 7),
        ("catalog_analysis_gid_winner_tombstones", "gid", 11),
    ],
    ids=["first-overlay-family", "middle-overlay-family", "last-overlay-family"],
)
def test_terminal_probe_rejects_hidden_family_and_preserves_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    key_column: str,
    key: bytes | int,
) -> None:
    analysis_id = b"\x01" + b"a" * 15
    with closing(
        open_generated_sqlite_database(tmp_path / "hidden-family.sqlite3")
    ) as connector:
        # Fault fixture isolates analysis cleanup from unrelated parent planes;
        # the public repository still freezes and seals its own cycle authority.
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO catalog_analysis_run_descriptor "
            "(analysis_id, build_id, policy_id, input_manifest_sha256, started_at) "
            "VALUES (%s, %s, 1, %s, 0)",
            (analysis_id, b"b" * 16, b"m" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_states (analysis_id, state) "
            "VALUES (%s, 'ABANDONED')",
            (analysis_id,),
        )
        connector.execute(
            f"INSERT INTO {table} (analysis_id, {key_column}) VALUES (%s, %s)",
            (analysis_id, key),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        gate = _claim(connector)
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.ANALYSIS_RUN,
                shard_no=1,
                cycle_cutoff_at=100,
                now=2,
            )
        _advance(connector, gate, cycle, now=3)
        _advance(connector, gate, cycle, now=4)
        # The frozen family now becomes ineligible. A raw terminal proof must
        # still find its payload instead of incorrectly completing AR_OVERLAY.
        connector.execute(
            "UPDATE catalog_analysis_run_states SET state = 'OPEN' WHERE analysis_id = %s",
            (analysis_id,),
        )
        before = connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
        probes: list[str] = []
        original = connector.fetch_one

        def observe(sql: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            if sql.startswith("SELECT 1 WHERE EXISTS ("):
                probes.append(sql)
            return original(sql, data)

        monkeypatch.setattr(connector, "fetch_one", observe)
        with pytest.raises(CleanupRetentionBlockedError, match="still owns rows"):
            _advance(connector, gate, cycle, now=5)
        assert probes
        assert any(" OR EXISTS (" in sql for sql in probes)
        assert (
            connector.fetch_all("SELECT * FROM operational_cleanup_checkpoints")
            == before
        )
        assert connector.fetch_one(
            f"SELECT {key_column} FROM {table} WHERE analysis_id = %s", (analysis_id,)
        ) == (key,)
        connector.execute(
            "UPDATE catalog_analysis_run_states SET state = 'ABANDONED' "
            "WHERE analysis_id = %s",
            (analysis_id,),
        )
        _advance(connector, gate, cycle, now=6)
        assert (
            connector.fetch_one(
                f"SELECT {key_column} FROM {table} WHERE analysis_id = %s",
                (analysis_id,),
            )
            == ()
        )
