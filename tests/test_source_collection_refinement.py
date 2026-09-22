"""Corruption controls for collection READY contracts on both SQL backends."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import closing

import pytest
from test_vnext_source_batches import _source_batch, _source_batch_clock
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    MemoryLibrary,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
)

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.schema_epoch import SchemaEpochValidationError
from h2hdb.source_collection_refinement import (
    check_source_collection_consumption_fencing_v1,
    check_source_collection_durable_observations_v1,
    check_source_collection_staging_owner_v1,
)
from h2hdb.sql_connector import SQLConnector


class _Rollback(Exception):
    """Keep each deliberately corrupt state inside an aborted test transaction."""


def _reject_change(
    connector: SQLConnector,
    statement: str,
    parameters: tuple[object, ...],
    check: Callable[[SQLConnector], None],
) -> None:
    with pytest.raises(_Rollback), connector.transaction():
        connector.execute(statement, parameters)
        with pytest.raises(SchemaEpochValidationError):
            check(connector)
        raise _Rollback


def test_collection_ready_checks_reject_descriptor_policy_and_consumption_gaps(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(7101, pages=[b"one", b"two"]),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _ = _source_batch(facade, session, policy, source, None)
        assert full_check(db_config).state == "READY"
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                collection_id = connector.fetch_one(
                    "SELECT collection_id FROM catalog_source_collection_consumptions WHERE build_id = %s",
                    (receipt.build_id,),
                )[0]
            assert isinstance(collection_id, bytes)
            _reject_change(
                connector,
                "DELETE FROM operational_source_collection_claims WHERE collection_id = %s",
                (collection_id,),
                check_source_collection_durable_observations_v1,
            )
            _reject_change(
                connector,
                "UPDATE catalog_source_collection_qualification_policies SET qualification_policy_sha256 = %s WHERE collection_id = %s",
                (b"x" * 32, collection_id),
                check_source_collection_durable_observations_v1,
            )
            _reject_change(
                connector,
                "DELETE FROM catalog_source_collection_consumptions WHERE collection_id = %s",
                (collection_id,),
                check_source_collection_consumption_fencing_v1,
            )
            _reject_change(
                connector,
                "UPDATE operational_source_collection_states SET state = 'ABANDONED' WHERE collection_id = %s",
                (collection_id,),
                check_source_collection_consumption_fencing_v1,
            )
            _reject_change(
                connector,
                "UPDATE catalog_source_build_states SET state = 'OPEN' WHERE build_id = %s",
                (receipt.build_id,),
                check_source_collection_consumption_fencing_v1,
            )
        assert full_check(db_config).state == "READY"
        facade.complete_ingest(session)


def test_collection_staging_rejects_missing_and_dual_owners(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(7201, pages=[b"original"]),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _ = _source_batch(facade, session, policy, source, None)
        run_analysis(facade, session, policy, receipt.build_id)
        run_publication(facade, session, policy, MemoryLibrary(source))
        facade.complete_ingest(session)
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        source.put(gallery(7202, pages=[b"new"]))
        with (
            facade.prepare_source(source, policy=policy) as prepared,
            closing(open_connector(db_config)) as connector,
        ):
            for _ in range(100):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                facade.commit_source_step(session, local)
                with connector.read_transaction():
                    row = connector.fetch_one(
                        "SELECT staging_id FROM operational_gallery_staging_collections LIMIT 1"
                    )
                if row:
                    break
            else:
                pytest.fail("real source workflow never created collection staging")
            staging_id = row[0]
            _reject_change(
                connector,
                "DELETE FROM operational_gallery_observation_staging_claims WHERE staging_id = %s",
                (staging_id,),
                check_source_collection_staging_owner_v1,
            )
            with connector.read_transaction():
                check_source_collection_staging_owner_v1(connector)
            _reject_change(
                connector,
                "DELETE FROM operational_gallery_staging_collections WHERE staging_id = %s",
                (staging_id,),
                check_source_collection_staging_owner_v1,
            )
            _reject_change(
                connector,
                "INSERT INTO operational_gallery_staging_source_builds (staging_id, build_id) VALUES (%s, %s)",
                (staging_id, receipt.build_id),
                check_source_collection_staging_owner_v1,
            )
            with connector.read_transaction():
                check_source_collection_staging_owner_v1(connector)


def test_cleanup_proof_is_once_per_validation_and_rejects_repeated_audit_mutant(
    sqlite_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_source_collection_cleanup import (
        _advance,
        _begin,
        _exclusive,
        _one_gallery,
    )

    import h2hdb.operational_refinement as operational
    from h2hdb.source_collection_refinement import _CleanupAudit
    from h2hdb.vnext_cleanup_repository import CleanupTargetKind

    _one_gallery(sqlite_config)
    with (
        _source_batch_clock(sqlite_config) as clock,
        closing(open_connector(sqlite_config)) as connector,
    ):
        now = clock() + 10**10
        gate = _exclusive(connector, sqlite_config, now)
        collection = connector.fetch_one(
            "SELECT collection_id FROM catalog_source_collections"
        )[0]
        cycle = _begin(
            connector,
            sqlite_config,
            gate,
            CleanupTargetKind.SOURCE_COLLECTION,
            collection[0],
            now=now + 1,
        )
        generation = 1
        for step in range(64):
            result = _advance(
                connector, sqlite_config, gate, cycle, generation, now=now + step + 2
            )
            current = connector.fetch_one(
                "SELECT phase FROM operational_cleanup_checkpoints WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            if current == ("SC_STATE",):
                break
            assert result.generation is not None
            generation = result.generation
        else:
            pytest.fail("cleanup never committed multiple descriptor gaps")

        actual_reachability = operational.check_cleanup_reachability_v1
        actual_roots = operational.check_cleanup_frozen_root_set_v1
        counts = {"reachability": 0, "roots": 0}

        def reachability(sql: SQLConnector) -> None:
            counts["reachability"] += 1
            assert counts["reachability"] <= 1, "repeated complete cleanup audit"
            actual_reachability(sql)

        def roots(sql: SQLConnector) -> None:
            counts["roots"] += 1
            assert counts["roots"] <= 1, "repeated complete frozen-root audit"
            actual_roots(sql)

        monkeypatch.setattr(operational, "check_cleanup_reachability_v1", reachability)
        monkeypatch.setattr(operational, "check_cleanup_frozen_root_set_v1", roots)
        # Each call proves its own read snapshot; no prior call supplies authority.
        for _ in range(3):
            counts.update(reachability=0, roots=0)
            with connector.read_transaction():
                check_source_collection_durable_observations_v1(connector)
            assert counts == {"reachability": 1, "roots": 1}

        actual_ensure = _CleanupAudit.ensure

        def repeated_global_audit(audit: _CleanupAudit) -> None:
            audit.validated = False
            actual_ensure(audit)

        monkeypatch.setattr(_CleanupAudit, "ensure", repeated_global_audit)
        counts.update(reachability=0, roots=0)
        with (
            pytest.raises(AssertionError, match="repeated complete cleanup audit"),
            connector.read_transaction(),
        ):
            check_source_collection_durable_observations_v1(connector)
