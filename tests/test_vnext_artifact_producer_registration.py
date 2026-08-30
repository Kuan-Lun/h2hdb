from __future__ import annotations

from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import vnext_identity as identity
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationConflictError,
    ArtifactPreparationNotReadyError,
    ArtifactPreparationRepository,
)
from h2hdb.vnext_domains import DomainValidationError
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_FIELDS = (
    b"writer-v1",
    b"cpython-v1",
    b"pillow-v1",
    b"libjpeg-v1",
    b"zlib-v1",
)


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _authorities(
    connector: SQLiteConnector,
    *,
    gate_duration: int = 100,
    turn_duration: int = 100,
) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        work = VNextUnitOfWork(connector, backend="sqlite")
        gate = MaintenanceGateRepository.claim_shared(
            work,
            now=10,
            lease_duration=gate_duration,
        )
        turn = IngestFenceRepository.claim(
            work,
            owner_token=b"i" * 16,
            now=10,
            lease_duration=turn_duration,
        )
    return gate, turn


def _register(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    fields: tuple[bytes, ...] = _FIELDS,
    *,
    artifact_algorithm_version: int = 1,
    now: int = 11,
) -> Any:
    with connector.transaction():
        return ArtifactPreparationRepository.register_producer(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            now=now,
            artifact_algorithm_version=artifact_algorithm_version,
            writer_id=fields[0],
            python_abi=fields[1],
            pillow_build=fields[2],
            libjpeg_build=fields[3],
            zlib_build=fields[4],
        )


def test_registration_derives_digest_inserts_one_row_and_replays_without_writes(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer.sqlite3")
    try:
        gate, turn = _authorities(connector)
        original_execute = connector.execute
        mutations: list[str] = []

        def recording_execute(query: str, data: tuple[Any, ...] = ()) -> None:
            mutations.append(query)
            original_execute(query, data)

        with patch.object(connector, "execute", side_effect=recording_execute):
            registered = _register(connector, gate, turn)
        expected_digest = identity.artifact_producer_fingerprint_sha256(*_FIELDS)
        expected_equivalence = identity.artifact_producer_equivalence_class(
            expected_digest
        )
        assert registered.producer_fingerprint_sha256 == expected_digest
        assert not registered.replayed
        assert len(mutations) == 1
        assert "catalog_artifact_producer_fingerprints" in mutations[0]
        assert connector.fetch_one(
            "SELECT producer_fingerprint_sha256, artifact_algorithm_version, "
            "producer_equivalence_class, writer_id, python_abi, pillow_build, "
            "libjpeg_build, zlib_build "
            "FROM catalog_artifact_producer_fingerprints"
        ) == (expected_digest, 1, expected_equivalence, *_FIELDS)

        with patch.object(connector, "execute", wraps=connector.execute) as execute:
            replay = _register(connector, gate, turn)
        assert replay.replayed
        assert replay.producer_fingerprint_sha256 == expected_digest
        execute.assert_not_called()
    finally:
        connector.close()


def test_registration_rejects_a_fresh_row_at_the_capacity_ceiling(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer-capacity.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with (
            patch(
                "h2hdb.vnext_artifact_preparation_repository."
                "RECOMPOSED_REGISTRY_MAXIMUM_ROWS",
                0,
            ),
            pytest.raises(
                ArtifactPreparationNotReadyError,
                match="recomposition capacity",
            ),
        ):
            _register(connector, gate, turn)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_artifact_producer_fingerprints"
        ) == (0,)
    finally:
        connector.close()


def test_registration_mutation_fault_rolls_back_the_wide_row(tmp_path: Path) -> None:
    base_path = tmp_path / "producer-fault-base.sqlite3"
    if not base_path.exists():
        base = _database(base_path)
        _authorities(base)
        base.close()
    path = tmp_path / "producer-fault.sqlite3"
    copyfile(base_path, path)
    connector = SQLiteConnector(str(path))
    connector.connect()
    gate = GateLease(b"g" * 16, 0, GateMode.SHARED, (0,), 110)
    gate_row = connector.fetch_one(
        "SELECT owner_token FROM operational_maintenance_gate_owners"
    )
    assert gate_row
    gate = GateLease(gate_row[0], 0, GateMode.SHARED, (0,), 110)
    turn = IngestTurn(1, b"i" * 16, 110)
    original_execute = connector.execute
    mutation = 0

    def failing_execute(query: str, data: tuple[Any, ...] = ()) -> None:
        nonlocal mutation
        mutation += 1
        if mutation == 1:
            raise RuntimeError("producer mutation")
        original_execute(query, data)

    try:
        with (
            patch.object(connector, "execute", side_effect=failing_execute),
            pytest.raises(RuntimeError, match="producer mutation"),
        ):
            _register(connector, gate, turn)
        assert mutation == 1
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_artifact_producer_fingerprints"
        )
    finally:
        connector.close()


def test_registration_rejects_digest_collision_and_mismatched_wide_row(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer-collision.sqlite3")
    try:
        gate, turn = _authorities(connector)
        registered = _register(connector, gate, turn)
        conflicting_fields = (b"other-writer", *_FIELDS[1:])
        with (
            patch(
                "h2hdb.vnext_artifact_preparation_repository.identity."
                "artifact_producer_fingerprint_sha256",
                return_value=registered.producer_fingerprint_sha256,
            ),
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(ArtifactPreparationConflictError, match="collides"),
        ):
            _register(connector, gate, turn, conflicting_fields)
        execute.assert_not_called()

        connector.execute(
            "UPDATE catalog_artifact_producer_fingerprints SET writer_id = %s "
            "WHERE producer_fingerprint_sha256 = %s",
            (b"tampered-writer", registered.producer_fingerprint_sha256),
        )
        with pytest.raises(ArtifactPreparationConflictError, match="collides"):
            _register(connector, gate, turn)
    finally:
        connector.close()


def test_registration_rejects_natural_key_collision_with_different_digest_zero_write(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer-natural-collision.sqlite3")
    try:
        gate, turn = _authorities(connector)
        registered = _register(connector, gate, turn)
        forged_digest = bytes(
            value ^ 0xFF for value in registered.producer_fingerprint_sha256
        )
        with (
            patch(
                "h2hdb.vnext_artifact_preparation_repository.identity."
                "artifact_producer_fingerprint_sha256",
                return_value=forged_digest,
            ),
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(ArtifactPreparationConflictError, match="candidate key"),
        ):
            _register(connector, gate, turn)
        execute.assert_not_called()
    finally:
        connector.close()


def test_registration_reauthorizes_replay_and_rejects_stale_authority_zero_write(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer-stale.sqlite3")
    try:
        gate, turn = _authorities(
            connector,
            gate_duration=1_000,
            turn_duration=10,
        )
        _register(connector, gate, turn)

        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(IngestFenceUnavailableError),
        ):
            _register(connector, gate, turn, now=20)
        execute.assert_not_called()

        stale_gate = GateLease(
            gate.owner_token,
            gate.gate_generation,
            gate.mode,
            gate.slots,
            11,
        )
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(MaintenanceGateUnavailableError),
        ):
            _register(connector, stale_gate, turn, now=11)
        execute.assert_not_called()
    finally:
        connector.close()


def test_registration_requires_the_zip_policy_before_any_producer_write(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "producer-missing-zip-policy.sqlite3")
    try:
        gate, turn = _authorities(connector)
        connector.execute(
            "DELETE FROM catalog_artifact_zip_writer_policies "
            "WHERE artifact_algorithm_version = %s",
            (1,),
        )
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(
                ArtifactPreparationNotReadyError,
                match="algorithm version is not registered",
            ),
        ):
            _register(connector, gate, turn)
        execute.assert_not_called()
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("artifact_algorithm_version", "fields", "error"),
    [
        (1 << 32, _FIELDS, "algorithm version"),
        (1, (b"x" * 129, *_FIELDS[1:]), "writer_id"),
    ],
)
def test_registration_rejects_malformed_command_before_any_sql(
    tmp_path: Path,
    artifact_algorithm_version: int,
    fields: tuple[bytes, ...],
    error: str,
) -> None:
    connector = _database(tmp_path / "producer-uint32.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with (
            patch.object(
                connector, "fetch_one", wraps=connector.fetch_one
            ) as fetch_one,
            patch.object(
                connector, "fetch_all", wraps=connector.fetch_all
            ) as fetch_all,
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(DomainValidationError, match=error),
        ):
            _register(
                connector,
                gate,
                turn,
                fields,
                artifact_algorithm_version=artifact_algorithm_version,
            )
        fetch_one.assert_not_called()
        fetch_all.assert_not_called()
        execute.assert_not_called()
    finally:
        connector.close()


def test_mariadb_registration_uses_plain_immutable_reads_and_one_insert() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.selects: list[str] = []
            self.mutations: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.selects.append(query)
            if "catalog_artifact_zip_writer_policies" in query:
                return (data[0],)
            if query == ("SELECT COUNT(*) FROM catalog_artifact_producer_fingerprints"):
                return (0,)
            return ()

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            del data
            self.mutations.append(query)

    recorder = Recorder()
    with patch(
        "h2hdb.vnext_artifact_preparation_repository._authorize_artifact_mutation",
        return_value=1,
    ):
        registration = ArtifactPreparationRepository.register_producer(
            VNextUnitOfWork(recorder, backend="mariadb"),  # type: ignore[arg-type]
            gate_lease=GateLease(b"g" * 16, 1, GateMode.SHARED, (0,), 100),
            ingest_turn=IngestTurn(1, b"i" * 16, 100),
            now=1,
            artifact_algorithm_version=1,
            writer_id=_FIELDS[0],
            python_abi=_FIELDS[1],
            pillow_build=_FIELDS[2],
            libjpeg_build=_FIELDS[3],
            zlib_build=_FIELDS[4],
        )
    assert not registration.replayed
    locks = [query for query in recorder.selects if query.endswith(" FOR UPDATE")]
    assert not locks
    assert any(
        "FROM catalog_artifact_zip_writer_policies" in query
        for query in recorder.selects
    )
    assert all("?" not in query for query in recorder.selects)
    assert all("%s" in query and "?" not in query for query in recorder.mutations)
    assert len(recorder.mutations) == 1
    assert "catalog_artifact_producer_fingerprints" in recorder.mutations[0]
