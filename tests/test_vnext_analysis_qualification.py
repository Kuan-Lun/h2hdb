from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from test_vnext_analysis_repository import (
    _authorities,
    _generated_database,
    _independent_file_oracle,
    _map_working_build,
    _seed_build,
    _seed_gallery,
    _seed_initial_snapshot,
    _seed_preparation_facts,
    _seed_published_commit,
    _seed_root,
    _source_build_id,
)
from test_vnext_ingest_analysis import _Clock, _config, _drive, _session, _Tokens
from test_vnext_live_mariadb_analysis_repository import (
    _authorities as _mariadb_authorities,
)
from test_vnext_live_mariadb_analysis_repository import (
    _connector as _mariadb_connector,
)

from h2hdb import (
    CoreConfig,
    GalleryObservationMetadata,
    VNextDatabaseAdminFacade,
    VNextSourceQualification,
)
from h2hdb import vnext_analysis_repository as analysis
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_ingest_analysis import VNextIngestAnalysisOrchestrator
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_transaction import VNextUnitOfWork


@pytest.mark.parametrize("shared_content", [False, True])
def test_rejected_source_is_removed_from_analysis_and_repair_restores_same_gid_winner(
    tmp_path: Path, *, shared_content: bool
) -> None:
    database = tmp_path / "qualified-analysis.sqlite3"
    connector = _generated_database(database)
    try:
        gate, turn = _authorities(connector)
        _exercise_qualification_generations(
            connector,
            config=_config(database),
            backend="sqlite",
            gate=gate,
            turn=turn,
            shared_content=shared_content,
        )
    finally:
        connector.close()


@pytest.mark.mariadb_smoke
def test_live_mariadb_qualification_removes_and_restores_same_gid_winner(
    mariadb_config: CoreConfig,
) -> None:
    """Two galleries and three one-row-batch generations exercise native SQL joins."""

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _mariadb_connector(mariadb_config) as entered:
        connector = cast(MariaDBConnector, entered)
        gate, turn = _mariadb_authorities(connector)
        _exercise_qualification_generations(
            connector,
            config=mariadb_config,
            backend="mariadb",
            gate=gate,
            turn=turn,
            shared_content=True,
        )


def _exercise_qualification_generations(
    connector: Any,
    *,
    config: CoreConfig,
    backend: str,
    gate: GateLease,
    turn: IngestTurn,
    shared_content: bool,
) -> None:
    """Real analysis preserves membership and recomputes all five components."""

    clock = _Clock()
    orchestrator = VNextIngestAnalysisOrchestrator(
        RepositoryContext.from_config(config),
        clock=clock,
        token_factory=_Tokens(),
    )
    base_receipt: bytes | None = None
    with connector.transaction():
        scope = _seed_root(connector)
    for generation, accepted in enumerate((True, False, True), start=1):
        qualification = (
            VNextSourceQualification()
            if accepted
            else VNextSourceQualification(
                False, "image_decode_failed", b"content-1.jpg"
            )
        )
        with connector.transaction():
            build_id = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((generation + 10,)) * 32,
                gallery_count=2,
                file_count=2,
                byte_count=2,
            )
            _seed_build(
                connector,
                build_id=build_id,
                scope=scope,
                manifest_byte=generation + 10,
                gallery_count=2,
                file_count=2,
                byte_count=2,
                base_receipt=base_receipt,
                created_at=clock(),
                sealed_at=clock(),
            )
            for gallery_id in (1, 2):
                if gallery_id == 2 and generation > 1:
                    connector.execute(
                        "INSERT INTO catalog_source_build_expected_gallery "
                        "(build_id, position, gallery_id) VALUES (%s, 1, 2)",
                        (build_id,),
                    )
                    connector.execute(
                        "INSERT INTO catalog_source_build_galleries "
                        "(build_id, gallery_id, observation_id) VALUES (%s, 2, 1)",
                        (build_id,),
                    )
                    continue
                member_qualification = (
                    qualification if gallery_id == 1 else VNextSourceQualification()
                )
                observation_id = generation if gallery_id == 1 else 1
                file_sha256 = bytes((1 if shared_content else gallery_id,)) * 32
                _seed_gallery(
                    connector,
                    build_id=build_id,
                    scope=scope,
                    gallery_id=gallery_id,
                    observation_id=observation_id,
                    occurrences=((file_sha256, 1),),
                    artists=(gallery_id,),
                    serial=800 + generation * 10 + gallery_id,
                    qualification=member_qualification,
                )
                metadata = GalleryObservationMetadata(
                    777,
                    "preferred longer title" if gallery_id == 1 else "fallback",
                    "",
                    "fixture",
                    100,
                    101,
                    102,
                    1,
                    1,
                    1,
                    qualification=member_qualification,
                )
                _seed_preparation_facts(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=observation_id,
                    file_sha256=file_sha256,
                    metadata=metadata,
                )
            _map_working_build(
                connector,
                build_id=build_id,
                generation=turn.generation,
                replace=generation > 1,
            )
        result, prepared, stopped = _drive(
            orchestrator, _session(gate, turn), build_id, max_rows=1
        )
        prepared.close()
        assert result is not None and stopped is None and result.terminal
        assert result.snapshot_manifest_sha256 is not None
        with connector.read_transaction():
            assert connector.fetch_one(
                "SELECT gid, winner_gallery_id "
                "FROM catalog_analysis_gid_winner_resolved WHERE analysis_id = %s",
                (result.analysis_id,),
            ) == (777, 1 if accepted else 2)
            assert connector.fetch_all(
                "SELECT gallery_id FROM catalog_source_build_galleries "
                "WHERE build_id = %s ORDER BY gallery_id",
                (build_id,),
            ) == [(1,), (2,)]
            assert connector.fetch_one(
                "SELECT gallery_count, file_count, byte_count "
                "FROM catalog_source_snapshot_manifest_identity "
                "WHERE snapshot_manifest_sha256 = %s",
                (result.snapshot_manifest_sha256,),
            ) == (2, 2, 2)
            decision_rows = connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, "
                "maximum_gallery_artist_count "
                "FROM catalog_analysis_file_hash_decision_resolved "
                "WHERE analysis_id = %s",
                (result.analysis_id,),
            )
            assert {
                bytes(row[0]): tuple(row[1:]) for row in decision_rows
            } == _independent_file_oracle(connector, build_id)
            assert len(decision_rows) == (2 if accepted and not shared_content else 1)
            if not accepted:
                assert connector.fetch_one(
                    "SELECT change_kind FROM catalog_analysis_changed_galleries "
                    "WHERE analysis_id = %s AND gallery_id = 1",
                    (result.analysis_id,),
                ) == ("REMOVED",)
                assert connector.fetch_one(
                    "SELECT gallery_id FROM catalog_analysis_gid_candidate_tombstones "
                    "WHERE analysis_id = %s",
                    (result.analysis_id,),
                ) == (1,)
            elif generation == 3:
                assert connector.fetch_one(
                    "SELECT change_kind FROM catalog_analysis_changed_galleries "
                    "WHERE analysis_id = %s AND gallery_id = 1",
                    (result.analysis_id,),
                ) == ("ADDED",)
        with connector.transaction():
            base_receipt = _seed_published_commit(
                connector,
                build_id=build_id,
                snapshot_manifest_sha256=result.snapshot_manifest_sha256,
                generation=turn.generation,
                committed_at=clock(),
                analysis_id=result.analysis_id,
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend=backend), turn, now=clock()
            )
            if generation < 3:
                turn = IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend=backend),
                    owner_token=bytes((generation + 20,)) * 16,
                    now=clock(),
                    lease_duration=1_000_000,
                )


@pytest.mark.parametrize(
    "corruption",
    ["missing", "policy", "disposition", "unexpected_reason", "unexpected_source"],
)
def test_analysis_metadata_rejects_qualification_not_bound_to_canonical_bytes(
    tmp_path: Path, corruption: str
) -> None:
    connector = _generated_database(tmp_path / "corrupt-qualification.sqlite3")
    try:
        _authorities(connector)
        with connector.transaction():
            _scope, _build, digest, _other = _seed_initial_snapshot(connector)
            _seed_preparation_facts(
                connector, gallery_id=1, observation_id=1, file_sha256=digest
            )
            if corruption == "missing":
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_validation_dispositions "
                    "WHERE gallery_id = 1 AND observation_id = 1"
                )
            elif corruption == "policy":
                connector.execute(
                    "UPDATE catalog_gallery_observation_validation_policies "
                    "SET qualification_policy_sha256 = %s "
                    "WHERE gallery_id = 1 AND observation_id = 1",
                    (b"x" * 32,),
                )
            elif corruption == "disposition":
                connector.execute(
                    "UPDATE catalog_gallery_observation_validation_dispositions "
                    "SET accepted = 0 WHERE gallery_id = 1 AND observation_id = 1"
                )
            elif corruption == "unexpected_reason":
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_validation_reasons "
                    "(gallery_id, observation_id, qualification_reason) "
                    "VALUES (1, 1, %s)",
                    (b"image_decode_failed",),
                )
            else:
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_validation_sources "
                    "(gallery_id, observation_id, qualification_source_name) "
                    "VALUES (1, 1, %s)",
                    (b"content-1.jpg",),
                )
        with pytest.raises(analysis.AnalysisCorruptionError, match="qualification"):
            analysis._metadata_comparator_facts(
                VNextUnitOfWork(connector, backend="sqlite"), 1, 1
            )
    finally:
        connector.close()
