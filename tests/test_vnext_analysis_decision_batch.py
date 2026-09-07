from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_decision_batch import (
    ensure_file_decision_materialization_page,
    load_file_decision_shadow_page,
    load_file_decision_tombstone_page,
    require_file_decision_page_keys,
)
from h2hdb.vnext_analysis_family import (
    AnalysisExclusionDeltaFamily,
    AnalysisFamilyCollisionError,
    AnalysisFamilyPartialError,
    ensure_analysis_exclusion_delta_family,
)
from h2hdb.vnext_analysis_overlay_family import (
    AnalysisFileHashDecisionShadowFamily,
    ensure_analysis_file_hash_decision_shadow_family,
)

_SHADOWS = (
    "catalog_a_file_decision_shadow_anchors",
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
    "catalog_a_file_decision_shadow_seals",
)
_DELTAS = (
    "catalog_analysis_exclusion_delta_anchors",
    "catalog_analysis_exclusion_delta_old_excluded_flags",
    "catalog_analysis_exclusion_delta_new_excluded_flags",
    "catalog_analysis_exclusion_delta_changes",
    "catalog_analysis_exclusion_delta_seals",
)
_TOMBSTONE = "catalog_analysis_file_hash_decision_tombstone"


def _proposals(
    analysis: bytes, count: int
) -> tuple[
    tuple[AnalysisExclusionDeltaFamily, ...],
    tuple[AnalysisFileHashDecisionShadowFamily, ...],
    tuple[bytes, ...],
]:
    keys = tuple(index.to_bytes(32, "big") for index in range(count))
    return (
        tuple(
            AnalysisExclusionDeltaFamily(analysis, key, index % 2, (index // 2) % 2)
            for index, key in enumerate(keys)
        ),
        tuple(
            AnalysisFileHashDecisionShadowFamily(
                analysis, key, index + 1, index % 7, index % 3
            )
            for index, key in enumerate(keys)
            if index % 3 != 2
        ),
        tuple(key for index, key in enumerate(keys) if index % 3 == 2),
    )


def _apply(connector: SQLConnector, analysis: bytes, count: int) -> None:
    deltas, shadows, tombstones = _proposals(analysis, count)
    ensure_file_decision_materialization_page(
        connector,
        analysis_id=analysis,
        deltas=deltas,
        shadows=shadows,
        tombstones=tombstones,
    )


def _snapshot(
    connector: SQLConnector, analysis: bytes
) -> tuple[tuple[tuple[Any, ...], ...], ...]:
    return tuple(
        tuple(
            connector.fetch_all(
                f"SELECT * FROM {table} WHERE analysis_id = %s ORDER BY file_sha256",
                (analysis,),
            )
        )
        for table in (*_DELTAS, *_SHADOWS, _TOMBSTONE)
    )


def _assert_batch_matches_scalar_reference(connector: SQLConnector) -> None:
    reference, candidate = b"r" * 16, b"b" * 16
    deltas, shadows, tombstones = _proposals(reference, 128)
    with connector.transaction():
        for delta in deltas:
            ensure_analysis_exclusion_delta_family(
                connector,
                analysis_id=reference,
                file_sha256=delta.file_sha256,
                old_excluded=delta.old_excluded,
                new_excluded=delta.new_excluded,
            )
        for shadow in shadows:
            ensure_analysis_file_hash_decision_shadow_family(connector, shadow)
        for key in tombstones:
            connector.execute(
                f"INSERT INTO {_TOMBSTONE} (analysis_id, file_sha256) VALUES (%s, %s)",
                (reference, key),
            )
    with connector.transaction():
        with patch.object(connector, "execute", wraps=connector.execute) as executed:
            _apply(connector, candidate, 128)
        statements = [call.args[0] for call in executed.call_args_list]
        assert len(statements) == 11
        assert all("), (" in statement for statement in statements)
        assert [statement.split()[2] for statement in statements] == [
            *_DELTAS,
            *_SHADOWS,
            _TOMBSTONE,
        ]
    with connector.read_transaction():
        # Ignore only analysis identity; compare every normalized stored fact.
        expected = tuple(
            tuple(row[1:] for row in table) for table in _snapshot(connector, reference)
        )
        actual = tuple(
            tuple(row[1:] for row in table) for table in _snapshot(connector, candidate)
        )
    assert actual == expected
    with (
        connector.transaction(),
        patch.object(
            connector,
            "execute",
            side_effect=AssertionError("exact replay attempted DML"),
        ),
    ):
        _apply(connector, candidate, 128)


def test_batch_matches_independent_scalar_storage_and_exact_replay(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "scalar-batch.sqlite3")
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        _assert_batch_matches_scalar_reference(connector)
    finally:
        connector.close()


@pytest.mark.parametrize("table", (*_SHADOWS, *_DELTAS))
def test_partial_family_orphan_and_collision_fail_before_any_batch_dml(
    tmp_path: Path, table: str
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "partial.sqlite3")
    analysis = b"p" * 16
    key = (0).to_bytes(32, "big")
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            _apply(connector, analysis, 3)
            # Missing any required child is partial; the optional change marker
            # uses a changed key, so its loss must also fail the exact preflight.
            corrupt_key = (1).to_bytes(32, "big") if table == _DELTAS[3] else key
            connector.execute(
                f"DELETE FROM {table} WHERE analysis_id = %s AND file_sha256 = %s",
                (analysis, corrupt_key),
            )
        with (
            pytest.raises(AnalysisFamilyCollisionError),
            connector.transaction(),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("preflight performed DML"),
            ),
        ):
            _apply(connector, analysis, 4)
        # An isolated child without an anchor must not disappear through a view.
        with connector.transaction():
            for shadow_table in _SHADOWS:
                connector.execute(
                    f"DELETE FROM {shadow_table} WHERE analysis_id = %s AND file_sha256 = %s",
                    (analysis, key),
                )
            connector.execute(
                f"INSERT INTO {_SHADOWS[2]} (analysis_id, file_sha256, artist_count) VALUES (%s, %s, %s)",
                (analysis, key, 2),
            )
        with pytest.raises(AnalysisFamilyPartialError):
            load_file_decision_shadow_page(
                connector, analysis_id=analysis, digests=(key,)
            )
    finally:
        connector.close()


@pytest.mark.parametrize("failed_table", (*_DELTAS, *_SHADOWS, _TOMBSTONE))
def test_each_table_failure_rolls_back_the_complete_page(
    tmp_path: Path, failed_table: str
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "rollback.sqlite3")
    analysis = b"f" * 16
    original = connector.execute
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        before = _snapshot(connector, analysis)

        def fail_after_write(statement: str, parameters: Any = None) -> None:
            original(statement, parameters)
            if statement.startswith(f"INSERT INTO {failed_table} "):
                raise RuntimeError("injected after table write")

        with (
            pytest.raises(RuntimeError, match="injected"),
            connector.transaction(),
            patch.object(connector, "execute", side_effect=fail_after_write),
        ):
            _apply(connector, analysis, 4)
        assert _snapshot(connector, analysis) == before
        with connector.transaction():
            _apply(connector, analysis, 4)
        with (
            connector.transaction(),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("lost response replay attempted DML"),
            ),
        ):
            _apply(connector, analysis, 4)
    finally:
        connector.close()


def test_complete_collision_and_unexpected_overlay_are_zero_write(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "collision.sqlite3")
    analysis = b"c" * 16
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            _apply(connector, analysis, 3)
        deltas, shadows, tombstones = _proposals(analysis, 4)
        changed = AnalysisFileHashDecisionShadowFamily(
            analysis, shadows[0].file_sha256, 999, 0, 0
        )
        with (
            pytest.raises(AnalysisFamilyCollisionError),
            connector.transaction(),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("collision performed DML"),
            ),
        ):
            ensure_file_decision_materialization_page(
                connector,
                analysis_id=analysis,
                deltas=deltas,
                shadows=(changed, *shadows[1:]),
                tombstones=tombstones,
            )
        with (
            pytest.raises(AnalysisFamilyCollisionError),
            connector.transaction(),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("extra shadow performed DML"),
            ),
        ):
            ensure_file_decision_materialization_page(
                connector,
                analysis_id=analysis,
                deltas=deltas,
                shadows=shadows[1:],
                tombstones=tombstones,
            )
    finally:
        connector.close()


@pytest.mark.parametrize(
    "digests",
    [tuple(index.to_bytes(32, "big") for index in range(129)), (b"d" * 32,) * 2],
)
def test_page_input_bounds_reject_before_sql(digests: tuple[bytes, ...]) -> None:
    connector = cast(SQLConnector, object())
    for reader in (load_file_decision_shadow_page, load_file_decision_tombstone_page):
        with pytest.raises(ValueError):
            reader(connector, analysis_id=b"a" * 16, digests=digests)
    with pytest.raises(ValueError):
        require_file_decision_page_keys(digests)


def test_shadow_page_uses_bounded_index_searches(tmp_path: Path) -> None:
    connector = open_generated_sqlite_database(tmp_path / "query-plan.sqlite3")
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            _apply(connector, b"a" * 16, 128)
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            loaded = load_file_decision_shadow_page(
                connector,
                analysis_id=b"a" * 16,
                digests=((0).to_bytes(32, "big"), (1).to_bytes(32, "big")),
            )
        assert len(loaded) == 2 and fetched.call_count == 1
        sql, parameters = fetched.call_args.args
        assert sql.count("file_sha256 IN (%s, %s)") == 5
        assert parameters[-1] == 129
        plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
        descriptions = [str(row[3]) for row in plan]
        for table in _SHADOWS:
            assert any(
                f"SEARCH {table} USING" in description
                and "analysis_id=? AND file_sha256=?" in description
                for description in descriptions
            ), descriptions
    finally:
        connector.close()


def test_live_mariadb_scalar_batch_matches_reference_and_query_plan(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as connector:
        # Isolate physical family storage from source parents. Every family PK
        # and scalar domain remains enforced; production E2E below keeps FKs on.
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        _assert_batch_matches_scalar_reference(connector)
        with (
            connector.read_transaction(),
            patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched,
        ):
            load_file_decision_shadow_page(
                connector,
                analysis_id=b"b" * 16,
                digests=((0).to_bytes(32, "big"), (1).to_bytes(32, "big")),
            )
        sql, parameters = fetched.call_args.args
        with connector.read_transaction():
            plan = connector.fetch_all("EXPLAIN " + sql, parameters)
        # UNION inputs must search a primary-key range; a bounded derived union
        # is allowed to scan its <=128 fixed-width keys.
        base_rows = [row for row in plan if row[2] in _SHADOWS]
        assert len(base_rows) == 5, plan
        assert all(
            row[3] in {"range", "ref", "const"}
            and row[5]
            in {
                "PRIMARY",
                "ix_fk_analysis_file_hash_decision_shadow_anchor_2_file_sha256",
            }
            and str(row[6]) == "48"
            for row in base_rows
        ), "\n".join(str(row) for row in plan)


def _exercise_production_page(connector: SQLConnector, backend: str) -> None:
    from test_vnext_analysis_repository import (
        _independent_file_oracle,
        _seed_initial_snapshot,
    )
    from test_vnext_live_mariadb_analysis_repository import _file_decision_snapshot

    import h2hdb.vnext_analysis_repository as analysis_module
    from h2hdb.vnext_analysis_repository import AnalysisRepository
    from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository
    from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
    from h2hdb.vnext_transaction import VNextUnitOfWork

    def work() -> VNextUnitOfWork:
        return VNextUnitOfWork(connector, backend=backend)

    with connector.transaction():
        gate = MaintenanceGateRepository.claim_shared(
            work(), now=10, lease_duration=10000
        )
        turn = IngestFenceRepository.claim(
            work(), owner_token=b"turn".ljust(16, b"!"), now=11, lease_duration=10000
        )
        _scope, build, first, second = _seed_initial_snapshot(cast(Any, connector))
    with connector.transaction():
        run = AnalysisRepository.begin(
            work(),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build,
            policy_id=1,
            proposed_analysis_id=b"b" * 16,
            now=30,
        )
    for stage_index, operation in enumerate(
        (
            AnalysisRepository.process_changed_gallery_batch,
            AnalysisRepository.process_changed_file_hash_batch,
        )
    ):
        for page in range(2):
            with connector.transaction():
                result = operation(
                    work(),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=f"source-{stage_index}-{page}".encode(),
                    max_rows=128,
                    now=100 + stage_index * 10 + page,
                )
        assert result.terminal

    def process() -> Any:
        return AnalysisRepository.process_file_hash_decision_batch(
            work(),
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=run.analysis_id,
            batch_key=b"batch",
            max_rows=128,
            now=200,
        )

    before = _file_decision_snapshot(cast(Any, connector), run.analysis_id)
    execute = connector.execute

    def crash_after_seal(statement: str, parameters: Any = None) -> None:
        execute(statement, parameters)
        if statement.startswith(f"INSERT INTO {_SHADOWS[-1]} "):
            raise RuntimeError("crash after sealed family before checkpoint")

    with (
        pytest.raises(RuntimeError, match="crash after"),
        connector.transaction(),
        patch.object(connector, "execute", side_effect=crash_after_seal),
    ):
        process()
    assert _file_decision_snapshot(cast(Any, connector), run.analysis_id) == before

    with (
        connector.transaction(),
        patch.object(connector, "fetch_one", wraps=connector.fetch_one) as fetched,
    ):
        committed = process()
    aggregate_statements = [
        call.args[0]
        for call in fetched.call_args_list
        if "SUM(occurrence.occurrence_count)" in call.args[0]
        or "COUNT(DISTINCT artist.artist_tag_id)" in call.args[0]
        or "MAX(per_gallery.artist_count)" in call.args[0]
    ]
    assert len(aggregate_statements) == 6
    assert committed.row_count == 2
    # Simulate a lost successful response: a new transaction only replays the
    # receipt, with original source independently re-evaluated and no DML.
    with (
        connector.transaction(),
        patch.object(
            connector,
            "execute",
            side_effect=AssertionError("receipt replay mutated data"),
        ),
        patch.object(
            connector,
            "execute_affected",
            side_effect=AssertionError("receipt replay advanced checkpoint"),
        ),
    ):
        replay = process()
    assert replay.replayed and replay.next_cursor == committed.next_cursor
    with connector.transaction():
        AnalysisRepository.process_file_hash_decision_batch(
            work(),
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=run.analysis_id,
            batch_key=b"terminal",
            max_rows=128,
            now=201,
        )
    for page in range(2):
        with connector.transaction():
            validated = AnalysisRepository.validate_file_hash_decision_batch(
                work(),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=f"validate-{page}".encode(),
                max_rows=128,
                now=300 + page,
            )
    assert validated.component_sealed
    with connector.read_transaction():
        expected = _independent_file_oracle(cast(Any, connector), build)
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            actual = analysis_module._load_resolved_decision_page(
                work(), run.analysis_id, (first, second)
            )
        sql, parameters = fetched.call_args.args
        assert {
            key: (
                value.occurrence_count,
                value.artist_count,
                value.maximum_gallery_artist_count,
            )
            for key, value in actual.items()
        } == expected
        if backend == "mariadb":
            plan = connector.fetch_all("EXPLAIN " + sql, parameters)
            # The nearest-ancestor view must push the analysis/hash predicates
            # to its base tables; the final small result may use a filesort.
            indexed = [
                row
                for row in plan
                if row[2]
                in {
                    "sealed",
                    "anchor",
                    "member_1",
                    "member_2",
                    "member_3",
                    "same_tomb",
                    "near_tomb",
                }
            ]
            assert indexed and all(row[3] != "ALL" for row in indexed), "\n".join(
                str(row) for row in plan
            )


def test_sqlite_production_page_rolls_back_and_replays_receipt(tmp_path: Path) -> None:
    connector = open_generated_sqlite_database(tmp_path / "production-batch.sqlite3")
    try:
        _exercise_production_page(connector, "sqlite")
    finally:
        connector.close()


@pytest.mark.mariadb_smoke
def test_live_mariadb_production_page_rolls_back_and_replays_receipt(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as connector:
        _exercise_production_page(connector, "mariadb")


def test_incremental_page_loads_parent_policy_once_and_preserves_overlay(
    tmp_path: Path,
) -> None:
    from test_vnext_analysis_repository import (
        _authorities,
        _begin,
        _independent_file_oracle,
        _prepare_incremental,
        _run_file_slice,
    )

    import h2hdb.vnext_analysis_repository as analysis_module

    connector = open_generated_sqlite_database(tmp_path / "incremental-batch.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        turn, build, unchanged, removed, added = _prepare_incremental(
            connector, gate, first_turn
        )
        run = _begin(
            connector, gate, turn, build_id=build, analysis_id=b"i" * 16, now=530
        )
        with patch.object(
            analysis_module, "_analysis_policy", wraps=analysis_module._analysis_policy
        ) as policy_loads:
            _run_file_slice(
                connector,
                gate,
                turn,
                run.analysis_id,
                max_rows=128,
                start_now=600,
                replay_each=True,
            )
        # Three different keys share the same parent policy; materialization and
        # its independent replay each reload it once, validation does not use it.
        assert policy_loads.call_count == 2
        assert load_file_decision_tombstone_page(
            connector, analysis_id=run.analysis_id, digests=(unchanged, removed, added)
        ) == {removed}
        assert set(
            load_file_decision_shadow_page(
                connector,
                analysis_id=run.analysis_id,
                digests=(unchanged, removed, added),
            )
        ) == {added}
        actual = {
            row[0]: row[1:]
            for row in connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, maximum_gallery_artist_count FROM catalog_analysis_file_hash_decision_resolved WHERE analysis_id = %s",
                (run.analysis_id,),
            )
        }
        assert actual == _independent_file_oracle(connector, build)
    finally:
        connector.close()
