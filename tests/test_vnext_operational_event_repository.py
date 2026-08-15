from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_operational_event_repository import (
    ActivationGenerationRaceError,
    DeletionConsumption,
    OperationalActivation,
    OperationalBatchLimitError,
    OperationalBatchReceipt,
    OperationalEffectCorruptionError,
    OperationalEffectRepository,
    OperationalEffectSeal,
    OperationalEffectStateError,
    OperationalPreparation,
    RemovedGid,
)
from h2hdb.vnext_queue_repository import VNextQueueRepository
from h2hdb.vnext_transaction import StaleWriteError, VNextUnitOfWork


def _generated_database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    _seed_catalog_authority(connector)
    return connector


def _seed_catalog_authority(connector: SQLiteConnector) -> None:
    value_sha256 = b"v" * 32
    page_sha256 = b"p" * 32
    connector.execute(
        "INSERT INTO catalog_canonical_digest_policies (digest_domain) VALUES (%s)",
        (b"operational-test-v1",),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, b"operational-test-v1", 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value_sha256, b"x"),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_provider_registry (source_provider) VALUES (%s)",
        (b"operational-test",),
    )
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, %s)",
        (b"s" * 32, b"operational-test", value_sha256, 1),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (b"b" * 16, b"s" * 32, 1, "SEALED", 1, 2),
    )
    connector.execute(
        "INSERT INTO catalog_channel_registry (channel) VALUES (%s)",
        (b"main",),
    )
    connector.execute(
        "INSERT INTO catalog_source_snapshot_manifest_identity "
        "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, 0, 0, 0),
    )
    connector.execute(
        "INSERT INTO catalog_source_revisions "
        "(source_revision, channel, snapshot_manifest_sha256, published_at) "
        "VALUES (%s, %s, %s, %s)",
        (1, b"main", value_sha256, 3),
    )
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 2),
    )
    connector.execute(
        "INSERT INTO operational_operational_consumers "
        "(consumer_id, consumer_name) VALUES (%s, %s)",
        (1, "downloader"),
    )


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=5,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=6,
            lease_duration=1_000_000,
        )
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (b"b" * 16, turn.generation),
        )
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (1, %s, %s)",
            (b"b" * 16, 6),
        )
    return gate, turn


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int = 10,
) -> OperationalPreparation:
    with connector.transaction():
        return OperationalEffectRepository.begin(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=b"b" * 16,
            operational_policy_id=1,
            now=now,
        )


def _terminal_and_seal(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    preparation_id: bytes,
    *,
    now: int,
) -> tuple[OperationalBatchReceipt, OperationalEffectSeal]:
    with connector.transaction():
        terminal = OperationalEffectRepository.append_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=preparation_id,
            effects=(),
            now=now,
        )
    with connector.transaction():
        seal = OperationalEffectRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=preparation_id,
            now=now + 1,
        )
    return terminal, seal


def _activate(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    preparation_id: bytes,
    *,
    now: int = 50,
) -> OperationalActivation:
    with connector.transaction():
        return OperationalEffectRepository.activate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=preparation_id,
            source_revision=1,
            operational_policy_id=1,
            now=now,
        )


def test_two_typed_effects_response_loss_exact_replay_seal_activation_and_ack(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "operational.sqlite3")
    deletion_token = b"d" * 16
    effects = (
        RemovedGid(11, b"r" * 16),
        DeletionConsumption(22, deletion_token),
    )
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=22,
                request_token=deletion_token,
                url=None,
                requested_at=4,
            )
        preparation = _begin(connector, gate, turn)
        replayed_begin = _begin(connector, gate, turn, now=999)
        assert replayed_begin.preparation_id == preparation.preparation_id
        assert replayed_begin.prepared_at == preparation.prepared_at
        assert replayed_begin.replayed is True
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_event_streams"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_preparation_checkpoints"
        ) == (1,)

        with connector.transaction():
            receipt = OperationalEffectRepository.append_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation_id=preparation.preparation_id,
                effects=effects,
                now=20,
            )
        assert (receipt.start_sequence_no, receipt.next_sequence_no) == (0, 2)
        assert receipt.row_count == 2
        before = (
            connector.fetch_one("SELECT COUNT(*) FROM operational_operational_events"),
            connector.fetch_one(
                "SELECT COUNT(*) "
                "FROM operational_operational_preparation_batch_receipts"
            ),
            connector.fetch_all(
                "SELECT generation, cursor_bytes, processed_count, chain_sha256, "
                "state, updated_at "
                "FROM operational_operational_preparation_checkpoints"
            ),
        )
        with connector.transaction():
            replay = OperationalEffectRepository.append_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation_id=preparation.preparation_id,
                effects=effects,
                now=999,
            )
        assert replay.replayed is True
        assert replay.committed_at == 20
        assert before == (
            connector.fetch_one("SELECT COUNT(*) FROM operational_operational_events"),
            connector.fetch_one(
                "SELECT COUNT(*) "
                "FROM operational_operational_preparation_batch_receipts"
            ),
            connector.fetch_all(
                "SELECT generation, cursor_bytes, processed_count, chain_sha256, "
                "state, updated_at "
                "FROM operational_operational_preparation_checkpoints"
            ),
        )
        assert connector.fetch_all(
            "SELECT sequence_no, event_type, length(event_id), length(event_sha256) "
            "FROM operational_operational_events ORDER BY sequence_no"
        ) == [
            (0, "REMOVED_GID", 16, 32),
            (1, "DELETION_CONSUMPTION", 16, 32),
        ]
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_removed_gid_events"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) "
            "FROM operational_operational_deletion_consumption_events"
        ) == (1,)

        removed_event_id = connector.fetch_one(
            "SELECT event_id FROM operational_operational_events "
            "WHERE sequence_no = 0"
        )[0]
        connector.execute(
            "UPDATE operational_operational_removed_gid_events SET gid = %s "
            "WHERE event_id = %s",
            (999, removed_event_id),
        )
        with pytest.raises(
            OperationalEffectCorruptionError, match="complete typed tuple"
        ):
            with connector.transaction():
                OperationalEffectRepository.append_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation_id=preparation.preparation_id,
                    effects=effects,
                    now=21,
                )
        connector.execute(
            "UPDATE operational_operational_removed_gid_events SET gid = %s "
            "WHERE event_id = %s",
            (11, removed_event_id),
        )

        terminal, seal = _terminal_and_seal(
            connector, gate, turn, preparation.preparation_id, now=30
        )
        assert terminal.terminal is True
        assert seal.event_count == 2
        seal_rows = connector.fetch_one(
            "SELECT COUNT(*) " "FROM operational_operational_preparation_effect_seals"
        )
        with connector.transaction():
            seal_replay = OperationalEffectRepository.seal(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation_id=preparation.preparation_id,
                now=999,
            )
        assert seal_replay.replayed is True
        assert seal_replay.sealed_at == 31
        assert (
            connector.fetch_one(
                "SELECT COUNT(*) "
                "FROM operational_operational_preparation_effect_seals"
            )
            == seal_rows
        )
        with connector.transaction():
            post_seal_batch_replay = OperationalEffectRepository.append_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation_id=preparation.preparation_id,
                effects=effects,
                now=1_000,
            )
        assert post_seal_batch_replay.replayed is True
        assert post_seal_batch_replay.committed_at == 20

        statements: list[str] = []
        connector.connection.set_trace_callback(statements.append)
        activation = _activate(connector, gate, turn, preparation.preparation_id)
        connector.connection.set_trace_callback(None)
        assert activation.replayed is False
        assert not any(
            "operational_operational_events" in statement for statement in statements
        )

        with connector.transaction():
            ack = OperationalEffectRepository.acknowledge_through(
                VNextUnitOfWork(connector, backend="sqlite"),
                consumer_id=1,
                source_revision=1,
                through_sequence_no=1,
                now=60,
            )
        assert ack.evidence_count == 2
        before_ack = connector.fetch_all(
            "SELECT consumer_id, event_id, acked_at "
            "FROM operational_operational_event_acks ORDER BY event_id"
        )
        with connector.transaction():
            ack_replay = OperationalEffectRepository.acknowledge_through(
                VNextUnitOfWork(connector, backend="sqlite"),
                consumer_id=1,
                source_revision=1,
                through_sequence_no=1,
                now=999,
            )
        assert ack_replay.replayed is True
        assert ack_replay.updated_at == 60
        assert (
            connector.fetch_all(
                "SELECT consumer_id, event_id, acked_at "
                "FROM operational_operational_event_acks ORDER BY event_id"
            )
            == before_ack
        )
    finally:
        connector.close()


def test_zero_event_seal_and_deletion_generation_activation_race(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "zero-race.sqlite3")
    try:
        gate, turn = _authorities(connector)
        preparation = _begin(connector, gate, turn)
        _terminal, seal = _terminal_and_seal(
            connector, gate, turn, preparation.preparation_id, now=20
        )
        assert seal.event_count == 0
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_events"
        ) == (0,)

        with connector.transaction():
            VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=7,
                request_token=b"q" * 16,
                url=None,
                requested_at=30,
            )
        with pytest.raises(ActivationGenerationRaceError):
            _activate(connector, gate, turn, preparation.preparation_id, now=31)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_activations"
        ) == (0,)

        successor = _begin(connector, gate, turn, now=32)
        assert successor.preparation_id != preparation.preparation_id
        assert successor.deletion_request_generation == 1
    finally:
        connector.close()


def test_batch_checkpoint_stale_cas_rolls_back_every_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "stale-cas.sqlite3")
    try:
        gate, turn = _authorities(connector)
        preparation = _begin(connector, gate, turn)
        original_execute_affected = connector.execute_affected

        def stale_checkpoint(query: str, data: tuple[Any, ...] = ()) -> int:
            if query.startswith(
                "UPDATE operational_operational_preparation_checkpoints"
            ):
                return 0
            return original_execute_affected(query, data)

        monkeypatch.setattr(connector, "execute_affected", stale_checkpoint)
        with pytest.raises(StaleWriteError):
            with connector.transaction():
                OperationalEffectRepository.append_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation_id=preparation.preparation_id,
                    effects=(RemovedGid(1, b"a" * 16),),
                    now=20,
                )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_events"
        ) == (0,)
        assert connector.fetch_one(
            "SELECT COUNT(*) " "FROM operational_operational_preparation_batch_receipts"
        ) == (0,)
        assert connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_count, state "
            "FROM operational_operational_preparation_checkpoints"
        ) == (0, b"\x00" * 8, 0, "OPEN")
    finally:
        connector.close()


def test_every_preparation_mutation_rechecks_the_live_ingest_fence(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "stale-ingest.sqlite3")
    try:
        gate, turn = _authorities(connector)
        preparation = _begin(connector, gate, turn)
        stale = IngestTurn(turn.generation, turn.owner_token, 1)
        before = connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_count, state "
            "FROM operational_operational_preparation_checkpoints"
        )
        with pytest.raises(IngestFenceUnavailableError):
            with connector.transaction():
                OperationalEffectRepository.append_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale,
                    preparation_id=preparation.preparation_id,
                    effects=(RemovedGid(1, b"a" * 16),),
                    now=20,
                )
        assert (
            connector.fetch_one(
                "SELECT generation, cursor_bytes, processed_count, state "
                "FROM operational_operational_preparation_checkpoints"
            )
            == before
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_events"
        ) == (0,)
        assert connector.fetch_one(
            "SELECT COUNT(*) " "FROM operational_operational_preparation_batch_receipts"
        ) == (0,)
    finally:
        connector.close()


def test_database_policy_caps_batches_and_contiguous_ack_evidence(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "ack-bound.sqlite3")
    try:
        gate, turn = _authorities(connector)
        preparation = _begin(connector, gate, turn)
        with pytest.raises(OperationalBatchLimitError):
            with connector.transaction():
                OperationalEffectRepository.append_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation_id=preparation.preparation_id,
                    effects=tuple(
                        RemovedGid(index + 1, bytes([index + 1]) * 16)
                        for index in range(3)
                    ),
                    now=20,
                )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_events"
        ) == (0,)

        for offset in (0, 2):
            with connector.transaction():
                OperationalEffectRepository.append_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation_id=preparation.preparation_id,
                    effects=tuple(
                        RemovedGid(index + 1, bytes([index + 1]) * 16)
                        for index in range(offset, offset + 2)
                    ),
                    now=21 + offset,
                )
        _terminal_and_seal(connector, gate, turn, preparation.preparation_id, now=30)
        _activate(connector, gate, turn, preparation.preparation_id)

        with pytest.raises(OperationalBatchLimitError):
            with connector.transaction():
                OperationalEffectRepository.acknowledge_through(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    consumer_id=1,
                    source_revision=1,
                    through_sequence_no=2,
                    now=60,
                )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_event_acks"
        ) == (0,)

        for target, timestamp in ((1, 61), (3, 62)):
            with connector.transaction():
                receipt = OperationalEffectRepository.acknowledge_through(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    consumer_id=1,
                    source_revision=1,
                    through_sequence_no=target,
                    now=timestamp,
                )
            assert receipt.evidence_count == 2
        before = connector.fetch_all(
            "SELECT through_sequence_no, updated_at "
            "FROM operational_operational_event_ack_heads"
        )
        with pytest.raises(OperationalEffectStateError, match="backward"):
            with connector.transaction():
                OperationalEffectRepository.acknowledge_through(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    consumer_id=1,
                    source_revision=1,
                    through_sequence_no=0,
                    now=63,
                )
        assert (
            connector.fetch_all(
                "SELECT through_sequence_no, updated_at "
                "FROM operational_operational_event_ack_heads"
            )
            == before
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_event_acks"
        ) == (4,)
    finally:
        connector.close()
