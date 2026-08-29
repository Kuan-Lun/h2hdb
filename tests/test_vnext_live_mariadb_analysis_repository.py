from __future__ import annotations

from decimal import Decimal
from typing import Any, cast
from unittest.mock import patch

import pytest
from test_vnext_analysis_repository import (
    _map_working_build,
    _seed_build,
    _seed_gallery,
    _seed_initial_snapshot,
    _seed_root,
)

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_analysis_repository import AnalysisRepository
from h2hdb.vnext_domains import INT63_MAX, DomainValidationError
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    source_build_identity,
    source_build_snapshot_attempt_id,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _connector(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    return MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )


def _work(connector: MariaDBConnector) -> VNextUnitOfWork:
    return VNextUnitOfWork(connector, backend="mariadb")


def _snapshot_build_id(
    connector: MariaDBConnector,
    *,
    scope: bytes,
    summary: SourceBuildManifestSummary,
) -> bytes:
    source_root = connector.fetch_one(
        "SELECT source_root_sha256 FROM catalog_source_scopes WHERE scope_key = %s",
        (scope,),
    )
    assert len(source_root) == 1
    return source_build_identity(
        snapshot_attempt_id=source_build_snapshot_attempt_id(
            source_root[0],
            summary,
        ),
        scope=scope,
        manifest_policy_id=1,
    )


def _authorities(connector: MariaDBConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"analysis-gate-01",
        ):
            gate = MaintenanceGateRepository.claim_shared(
                _work(connector),
                now=10,
                lease_duration=10_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            _work(connector),
            owner_token=b"analysis-turn-01",
            now=11,
            lease_duration=10_000,
        )
    return gate, turn


def _begin(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    build_id: bytes,
    analysis_id: bytes,
) -> Any:
    with connector.transaction():
        return AnalysisRepository.begin(
            _work(connector),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build_id,
            policy_id=1,
            proposed_analysis_id=analysis_id,
            now=30,
        )


def _run_stage_to_completion(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    analysis_id: bytes,
    operation: Any,
    batch_prefix: bytes,
    start_now: int,
) -> tuple[Any, ...]:
    results = []
    for index in range(10):
        with connector.transaction():
            result = operation(
                _work(connector),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=batch_prefix + index.to_bytes(2, "big"),
                max_rows=128,
                now=start_now + index,
            )
        results.append(result)
        if result.next_state == "COMPLETE":
            return tuple(results)
    raise AssertionError("live MariaDB analysis stage did not converge")


def _prepare_file_decision_stage(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
) -> None:
    gallery_results = _run_stage_to_completion(
        connector,
        gate,
        turn,
        analysis_id=analysis_id,
        operation=AnalysisRepository.process_changed_gallery_batch,
        batch_prefix=b"maria-gallery-",
        start_now=100,
    )
    hash_results = _run_stage_to_completion(
        connector,
        gate,
        turn,
        analysis_id=analysis_id,
        operation=AnalysisRepository.process_changed_file_hash_batch,
        batch_prefix=b"maria-hash-",
        start_now=200,
    )
    assert gallery_results[-1].next_state == "COMPLETE"
    assert hash_results[-1].next_state == "COMPLETE"


def _file_decision_snapshot(
    connector: MariaDBConnector,
    analysis_id: bytes,
) -> tuple[object, ...]:
    with connector.read_transaction():
        return (
            connector.fetch_all(
                "SELECT generation, processed_count, state, updated_at "
                "FROM catalog_analysis_checkpoints "
                "WHERE analysis_id = %s AND stage = %s",
                (analysis_id, b"file_hash_decision"),
            ),
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s AND stage = %s",
                (analysis_id, b"file_hash_decision"),
            ),
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_exclusion_delta_anchors "
                "WHERE analysis_id = %s",
                (analysis_id,),
            ),
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_a_file_decision_shadow_anchors "
                "WHERE analysis_id = %s",
                (analysis_id,),
            ),
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_file_hash_decision_tombstone "
                "WHERE analysis_id = %s",
                (analysis_id,),
            ),
            connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_state_component_seals "
                "WHERE analysis_id = %s AND state_component = %s",
                (analysis_id, b"file_hash_decision"),
            ),
        )


def test_live_mariadb_file_decision_handles_decimal_aggregate_and_replay(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as entered_connector:
        connector = cast(MariaDBConnector, entered_connector)
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build_id, first, second = _seed_initial_snapshot(
                cast(Any, connector)
            )
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build_id,
            analysis_id=b"maria-analysis-1",
        )
        _prepare_file_decision_stage(connector, gate, turn, run.analysis_id)

        with connector.read_transaction():
            raw_sum = connector.fetch_one(
                "SELECT SUM(occurrence.occurrence_count) "
                "FROM catalog_source_build_galleries AS member "
                "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
                "ON occurrence.gallery_id = member.gallery_id "
                "AND occurrence.observation_id = member.observation_id "
                "WHERE member.build_id = %s AND occurrence.file_sha256 = %s",
                (build_id, first),
            )[0]
        assert type(raw_sum) is Decimal and raw_sum == Decimal(3)

        with connector.transaction():
            committed = AnalysisRepository.process_file_hash_decision_batch(
                _work(connector),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"maria-decision-page",
                max_rows=128,
                now=300,
            )
        assert committed.next_state == "OPEN" and committed.row_count == 2

        with connector.read_transaction():
            receipt_count = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s AND stage = %s",
                (run.analysis_id, b"file_hash_decision"),
            )
        with connector.transaction():
            replay = AnalysisRepository.process_file_hash_decision_batch(
                _work(connector),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"maria-decision-page",
                max_rows=1,
                now=301,
            )
        assert replay.replayed and replay.row_count == committed.row_count
        assert replay.next_cursor == committed.next_cursor
        with connector.read_transaction():
            post_replay_receipt_count = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s AND stage = %s",
                (run.analysis_id, b"file_hash_decision"),
            )
        assert post_replay_receipt_count == receipt_count

        decision_results = _run_stage_to_completion(
            connector,
            gate,
            turn,
            analysis_id=run.analysis_id,
            operation=AnalysisRepository.process_file_hash_decision_batch,
            batch_prefix=b"maria-decision-terminal-",
            start_now=310,
        )
        validation_results = _run_stage_to_completion(
            connector,
            gate,
            turn,
            analysis_id=run.analysis_id,
            operation=AnalysisRepository.validate_file_hash_decision_batch,
            batch_prefix=b"maria-validate-",
            start_now=400,
        )
        assert decision_results[-1].terminal
        assert validation_results[-1].component_sealed
        with connector.read_transaction():
            resolved_rows = connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, "
                "maximum_gallery_artist_count "
                "FROM catalog_analysis_file_hash_decision_resolved "
                "WHERE analysis_id = %s ORDER BY file_sha256",
                (run.analysis_id,),
            )
        assert resolved_rows == [(first, 3, 3, 2), (second, 1, 2, 2)]


def test_live_mariadb_file_decision_overflow_is_zero_write(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as entered_connector:
        connector = cast(MariaDBConnector, entered_connector)
        gate, turn = _authorities(connector)
        digest = b"overflow-file-digest".ljust(32, b"!")
        with connector.transaction():
            fixture_connector = cast(Any, connector)
            scope = _seed_root(fixture_connector)
            summary = SourceBuildManifestSummary(b"\x09" * 32, 2, 0, 0)
            build_id = _snapshot_build_id(
                connector,
                scope=scope,
                summary=summary,
            )
            _seed_build(
                fixture_connector,
                build_id=build_id,
                scope=scope,
                manifest_byte=9,
                gallery_count=2,
                manifest_sha256=summary.manifest_sha256,
            )
            _seed_gallery(
                fixture_connector,
                build_id=build_id,
                scope=scope,
                gallery_id=1,
                observation_id=1,
                occurrences=((digest, INT63_MAX),),
                artists=(1,),
                serial=900,
            )
            _seed_gallery(
                fixture_connector,
                build_id=build_id,
                scope=scope,
                gallery_id=2,
                observation_id=1,
                occurrences=((digest, 1),),
                artists=(2,),
                serial=901,
            )
            _map_working_build(
                fixture_connector,
                build_id=build_id,
                generation=turn.generation,
            )
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build_id,
            analysis_id=b"maria-overflow!!",
        )
        _prepare_file_decision_stage(connector, gate, turn, run.analysis_id)

        with connector.read_transaction():
            raw_sum = connector.fetch_one(
                "SELECT SUM(occurrence.occurrence_count) "
                "FROM catalog_source_build_galleries AS member "
                "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
                "ON occurrence.gallery_id = member.gallery_id "
                "AND occurrence.observation_id = member.observation_id "
                "WHERE member.build_id = %s AND occurrence.file_sha256 = %s",
                (build_id, digest),
            )[0]
        assert type(raw_sum) is Decimal
        assert raw_sum == Decimal(INT63_MAX + 1)

        before = _file_decision_snapshot(connector, run.analysis_id)
        with pytest.raises(
            DomainValidationError,
            match=rf"decision occurrence_count must be in 1\.\.{INT63_MAX}",
        ):
            with connector.transaction():
                AnalysisRepository.process_file_hash_decision_batch(
                    _work(connector),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"maria-overflow-page",
                    max_rows=128,
                    now=300,
                )
        assert _file_decision_snapshot(connector, run.analysis_id) == before
