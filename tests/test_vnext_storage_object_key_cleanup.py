from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from vnext_generated_database import open_generated_sqlite_database

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupCycle,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _exclusive(connector: SQLiteConnector) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"storage-key-gc!!",
        ),
    ):
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _drain(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int,
) -> int:
    generation = 1
    for batches in range(1, 128):
        with connector.transaction():
            result = VNextCleanupRepository.advance(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                command=CleanupBatchCommand(batches.to_bytes(32, "big"), generation),
                now=now + batches,
            )
        if result.cycle_complete:
            return batches
        assert result.generation is not None
        generation = result.generation
    raise AssertionError("cleanup did not reach its bounded fixed point")


def test_storage_object_key_cleanup_is_bounded_and_retains_live_key(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "storage-key-gc.sqlite3")
    try:
        orphan = bytes((23,)) + b"o" * 31
        retained = bytes((23,)) + b"r" * 31
        connector.execute_many(
            "INSERT INTO catalog_storage_object_key_identities "
            "(storage_object_key_sha256, key_codec, segment_count) "
            "VALUES (%s, %s, 1)",
            [(orphan, b"test-v2"), (retained, b"test-v2")],
        )
        connector.execute_many(
            "INSERT INTO catalog_storage_object_key_segments "
            "(storage_object_key_sha256, segment_position, key_segment) "
            "VALUES (%s, 0, %s)",
            [(orphan, b"orphan"), (retained, b"retained")],
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key, resource_kind, "
                "storage_object_key_sha256, storage_generation, "
                "protection_token, state) VALUES (%s, %s, %s, %s, 1, %s, %s)",
                (
                    b"c" * 16,
                    b"p" * 32,
                    b"acquisition",
                    retained,
                    b"t" * 32,
                    "COMMITTED",
                ),
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")

        gate = _exclusive(connector)
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.STORAGE_OBJECT_KEY,
                shard_no=23,
                cycle_cutoff_at=100,
                max_rows_per_transaction=1,
                now=2,
            )
        batches = _drain(connector, gate, cycle, now=2)

        assert batches >= 2
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_storage_object_key_identities "
                "WHERE storage_object_key_sha256 = %s",
                (orphan,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_storage_object_key_segments "
                "WHERE storage_object_key_sha256 = %s",
                (orphan,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_storage_object_key_identities "
            "WHERE storage_object_key_sha256 = %s",
            (retained,),
        ) == (1,)
    finally:
        connector.close()


def test_gallery_observation_cleanup_deletes_adapter_role_before_file_anchor(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "gallery-role-gc.sqlite3")
    try:
        file_key = b"f" * 32
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "INSERT INTO catalog_gallery_observation_allocations "
                "(gallery_id, observation_id, allocated_at) VALUES (29, 1, 0)"
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_anchors "
                "(gallery_id, observation_id, file_key) VALUES (29, 1, %s)",
                (file_key,),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_file_nos "
                "(gallery_id, observation_id, file_key, file_no) "
                "VALUES (29, 1, %s, 0)",
                (file_key,),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_file_sha256s "
                "(gallery_id, observation_id, file_key, file_sha256) "
                "VALUES (29, 1, %s, %s)",
                (file_key, b"h" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_artifact_role "
                "(gallery_id, observation_id, file_key, artifact_role) "
                "VALUES (29, 1, %s, %s)",
                (file_key, b"page"),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_seals "
                "(gallery_id, observation_id, file_key) VALUES (29, 1, %s)",
                (file_key,),
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")

        gate = _exclusive(connector)
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.GALLERY_OBSERVATION,
                shard_no=29,
                cycle_cutoff_at=100,
                max_rows_per_transaction=1,
                now=2,
            )
        _drain(connector, gate, cycle, now=2)

        for table in (
            "catalog_gallery_observation_file_seals",
            "catalog_gallery_observation_file_file_sha256s",
            "catalog_gallery_observation_file_file_nos",
            "catalog_gallery_observation_file_artifact_role",
            "catalog_gallery_observation_file_anchors",
        ):
            assert connector.fetch_one(f"SELECT 1 FROM {table} LIMIT 1") == ()
    finally:
        connector.close()
