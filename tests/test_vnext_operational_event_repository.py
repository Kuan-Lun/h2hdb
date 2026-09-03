from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_registry_fixtures import (
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import seed_sealed_source_build, seed_snapshot_manifest
from vnext_publication_fixtures import seed_publication_commit

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_identity import artifact_policy_digest
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
    DeletionConsumption,
    OperationalBatchLimitError,
    OperationalBatchReceipt,
    OperationalEffectCorruptionError,
    OperationalEffectRepository,
    OperationalEffectSeal,
    OperationalEffectStateError,
    OperationalPreparation,
    RemovedGid,
    SupersededDrainPosition,
    drain_page_sql,
    drain_position_sql,
)
from h2hdb.vnext_queue_repository import VNextQueueRepository
from h2hdb.vnext_transaction import StaleWriteError, VNextUnitOfWork

_ARTIFACT_ADAPTER_ID = b"test-artifact-adapter"
_ARTIFACT_POLICY_FINGERPRINT = b"p" * 32


def _generated_database(path: Path) -> SQLiteConnector:
    connector = open_generated_sqlite_database(path)
    _seed_catalog_authority(connector)
    return connector


def _seed_catalog_authority(connector: SQLiteConnector) -> None:
    value_sha256 = b"v" * 32
    page_sha256 = b"p" * 32
    connector.execute(
        "INSERT INTO catalog_canonical_digest_policies (digest_domain) VALUES (%s)",
        (b"operational-test-v1",),
    )
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=b"operational-test-v1",
        page_sha256=page_sha256,
        page_bytes=b"x",
        subtree_item_count=1,
        allocated_at=1,
    )
    scope = seed_source_scope(
        connector,
        source_root_sha256=value_sha256,
    )
    seed_manifest_policy(connector)
    seed_sealed_source_build(
        connector,
        build_id=b"b" * 16,
        scope_key=scope.scope_key,
        manifest_sha256=b"m" * 32,
        gallery_count=0,
        file_count=0,
        byte_count=0,
        created_at=1,
        sealed_at=2,
    )
    connector.execute(
        "INSERT INTO catalog_channel_registry (channel) VALUES (%s)",
        (b"main",),
    )
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=value_sha256,
        gallery_count=0,
        file_count=0,
        byte_count=0,
    )
    policy_component_sha256 = artifact_policy_digest(
        2,
        _ARTIFACT_ADAPTER_ID,
        _ARTIFACT_POLICY_FINGERPRINT,
    )
    seed_canonical_value(
        connector,
        value_sha256=policy_component_sha256,
        digest_domain=b"artifact_policy_v3",
        page_sha256=b"q" * 32,
        page_bytes=b"p",
        subtree_item_count=1,
        allocated_at=1,
    )
    policy_semantics = seed_artifact_policy_semantics(
        connector,
        artifact_algorithm_version=2,
        adapter_id=_ARTIFACT_ADAPTER_ID,
        policy_fingerprint_sha256=_ARTIFACT_POLICY_FINGERPRINT,
    )
    assert policy_semantics.policy_component_sha256 == policy_component_sha256
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (1, policy_component_sha256),
    )
    seed_title_sort_policy(connector, unicode_data_version=b"test-unicode")
    seed_display_title_policy(connector)
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 2),
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


def _publish_preparation(
    connector: SQLiteConnector,
    preparation_id: bytes,
    *,
    now: int = 50,
) -> tuple[Any, ...]:
    receipt_id = b"u" * 16
    with connector.transaction():
        statements = (
            (
                "INSERT INTO catalog_source_revision_descriptors "
                "(source_revision, channel, snapshot_manifest_sha256) "
                "VALUES (%s, %s, %s)",
                (1, b"main", b"v" * 32),
            ),
            (
                "INSERT INTO catalog_revision_descriptors "
                "(revision, publication_count, artifact_count) VALUES (%s, %s, %s)",
                (1, 0, 0),
            ),
            (
                "INSERT INTO catalog_publication_generation_nodes "
                "(generation) VALUES (%s)",
                (1,),
            ),
            (
                "INSERT INTO catalog_publication_generation_successors "
                "(successor_generation, predecessor_generation) VALUES (%s, %s)",
                (1, 0),
            ),
        )
        for query, parameters in statements:
            connector.execute(query, parameters)
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=b"c" * 16,
            revision=1,
            source_revision=1,
            generation=1,
            preparation_id=preparation_id,
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=0,
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=now,
        )
    row = connector.fetch_one(
        "SELECT source_revision, preparation_id, operational_policy_id, committed_at "
        "FROM catalog_publication_commits WHERE source_revision = %s",
        (1,),
    )
    assert len(row) == 4
    return row


def test_operational_repository_has_no_independent_activation_writer() -> None:
    assert not hasattr(OperationalEffectRepository, "activate")


def test_two_typed_effects_response_loss_exact_replay_seal_and_activation(
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
            "SELECT COUNT(*) FROM operational_operational_deletion_consumption_events"
        ) == (1,)

        removed_event_id = connector.fetch_one(
            "SELECT event_id FROM operational_operational_events WHERE sequence_no = 0"
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
            "SELECT COUNT(*) FROM operational_operational_preparation_effect_seals"
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
                "SELECT COUNT(*) FROM operational_operational_preparation_effect_seals"
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
        activation = _publish_preparation(connector, preparation.preparation_id)
        connector.connection.set_trace_callback(None)
        assert activation == (1, preparation.preparation_id, 1, 50)
        assert not any(
            "operational_operational_events" in statement for statement in statements
        )

        assert not hasattr(OperationalEffectRepository, "acknowledge_through")
    finally:
        connector.close()


def test_zero_event_seal_remains_invisible_without_publication_commit(
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
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_publication_commits"
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
            "SELECT COUNT(*) FROM operational_operational_preparation_batch_receipts"
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
            "SELECT COUNT(*) FROM operational_operational_preparation_batch_receipts"
        ) == (0,)
    finally:
        connector.close()


def test_database_policy_caps_transient_event_batches(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "batch-bound.sqlite3")
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
        activation = _publish_preparation(connector, preparation.preparation_id)
        assert activation == (1, preparation.preparation_id, 1, 50)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_events"
        ) == (4,)
    finally:
        connector.close()


_SUPERSEDED_BUILD = b"b" * 16
_CURRENT_GENERATION = 0
_CURRENT_POLICY = 1
_SEEK_INDEX = "ix_operational_preparation_drain_seek"
_ZERO_UUID = b"\0" * 16


def _seed_superseded_preparations(
    connector: SQLiteConnector,
    *,
    count: int,
    first_generation: int = 1,
    states: tuple[str, ...] = ("OPEN",),
) -> list[bytes]:
    """Seed ``count`` unbound, uncommitted preparations of the build, each
    under a distinct deletion generation other than the current one, so every
    row is superseded by generation.  ``states`` cycles over the rows."""

    ids: list[bytes] = []
    with connector.transaction():
        for offset in range(count):
            generation = first_generation + offset
            preparation_id = f"sup{generation:013d}".encode()
            assert len(preparation_id) == 16
            connector.execute(
                "INSERT INTO operational_deletion_request_generations "
                "(generation, allocated_at) VALUES (%s, %s)",
                (generation, 1),
            )
            connector.execute(
                "INSERT INTO operational_operational_event_streams "
                "(preparation_id, created_at) VALUES (%s, %s)",
                (preparation_id, 1),
            )
            state = states[offset % len(states)]
            connector.execute(
                "INSERT INTO operational_operational_preparations "
                "(preparation_id, build_id, deletion_request_generation, "
                "operational_policy_id, state, prepared_at, completed_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    preparation_id,
                    _SUPERSEDED_BUILD,
                    generation,
                    _CURRENT_POLICY,
                    state,
                    3,
                    5 if state == "COMPLETE" else None,
                ),
            )
            ids.append(preparation_id)
    return ids


def _superseded_position(connector: SQLiteConnector) -> SupersededDrainPosition | None:
    with connector.transaction():
        return OperationalEffectRepository.superseded_drain_position(
            VNextUnitOfWork(connector, backend="sqlite"),
            build_id=_SUPERSEDED_BUILD,
            policy_id=_CURRENT_POLICY,
            deletion_generation=_CURRENT_GENERATION,
        )


def _abandon_one_page(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int,
    position: SupersededDrainPosition | None = None,
) -> int:
    if position is None:
        position = _superseded_position(connector)
        assert position is not None
    with connector.transaction():
        return OperationalEffectRepository.abandon_superseded_preparations(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=_SUPERSEDED_BUILD,
            policy_id=_CURRENT_POLICY,
            deletion_generation=_CURRENT_GENERATION,
            position=position,
            now=now,
        )


def _drain_all(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int,
    page_budget: int,
) -> tuple[list[int], list[SupersededDrainPosition]]:
    """Drive drainage from the durable position until it is None; every
    issued position must strictly advance past the previously committed one."""

    pages: list[int] = []
    positions: list[SupersededDrainPosition] = []
    while (position := _superseded_position(connector)) is not None:
        assert position.advances_past(positions[-1] if positions else None)
        positions.append(position)
        pages.append(
            _abandon_one_page(connector, gate, turn, now=now, position=position)
        )
        now += 1
        assert len(pages) <= page_budget
    return pages, positions


def _non_abandoned_count(connector: SQLiteConnector) -> int:
    row = connector.fetch_one(
        "SELECT COUNT(*) FROM operational_operational_preparations "
        "WHERE build_id = %s AND state IN ('OPEN', 'COMPLETE')",
        (_SUPERSEDED_BUILD,),
    )
    return int(row[0])


@pytest.mark.parametrize("count", (129, 257))
def test_superseded_drainage_is_bounded_seek_paged_and_converges(
    tmp_path: Path,
    count: int,
) -> None:
    """More than one page of superseded preparations drains 128 at a time from
    the durable position, which strictly advances after every committed page,
    until none remain and every row is ABANDONED exactly once."""

    connector = _generated_database(tmp_path / "drain.sqlite3")
    try:
        gate, turn = _authorities(connector)
        ids = _seed_superseded_preparations(connector, count=count)
        pages, positions = _drain_all(
            connector, gate, turn, now=100, page_budget=count // 128 + 1
        )
        assert pages == [128] * (count // 128) + ([count % 128] if count % 128 else [])
        # Each page started at the least surviving row: the seek is exact.
        assert [position.preparation_id for position in positions] == [
            ids[index * 128] for index in range(len(pages))
        ]
        assert all(position.state == "OPEN" for position in positions)
        assert _non_abandoned_count(connector) == 0
        abandoned = connector.fetch_one(
            "SELECT COUNT(*) FROM operational_operational_preparations "
            "WHERE build_id = %s AND state = 'ABANDONED'",
            (_SUPERSEDED_BUILD,),
        )
        assert abandoned == (count,)
        assert _superseded_position(connector) is None
    finally:
        connector.close()


def test_superseded_drainage_drains_each_state_as_one_seek_range(
    tmp_path: Path,
) -> None:
    """Mixed states drain as single-state index ranges in drain order:
    every COMPLETE row before the first OPEN page, each page within one
    state, and the position's (state, preparation_id) key strictly ascending."""

    connector = _generated_database(tmp_path / "states.sqlite3")
    try:
        gate, turn = _authorities(connector)
        _seed_superseded_preparations(connector, count=257, states=("OPEN", "COMPLETE"))
        pages, positions = _drain_all(connector, gate, turn, now=100, page_budget=3)
        assert [(position.state, page) for position, page in zip(positions, pages)] == [
            ("COMPLETE", 128),
            ("OPEN", 128),
            ("OPEN", 1),
        ]
        assert [position.key for position in positions] == sorted(
            position.key for position in positions
        )
        assert _non_abandoned_count(connector) == 0
    finally:
        connector.close()


def test_superseded_drainage_page_rolls_back_exactly_on_an_interrupted_commit(
    tmp_path: Path,
) -> None:
    """A page interrupted before its transaction commits abandons nothing:
    the whole page is atomic and the durable position is unchanged."""

    connector = _generated_database(tmp_path / "fault.sqlite3")
    try:
        gate, turn = _authorities(connector)
        _seed_superseded_preparations(connector, count=200)
        assert _non_abandoned_count(connector) == 200
        position = _superseded_position(connector)
        assert position is not None
        with pytest.raises(RuntimeError, match="interrupted"):
            with connector.transaction():
                OperationalEffectRepository.abandon_superseded_preparations(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    build_id=_SUPERSEDED_BUILD,
                    policy_id=_CURRENT_POLICY,
                    deletion_generation=_CURRENT_GENERATION,
                    position=position,
                    now=100,
                )
                raise RuntimeError("interrupted before commit")
        assert _non_abandoned_count(connector) == 200
        assert _superseded_position(connector) == position
        pages, _positions = _drain_all(connector, gate, turn, now=200, page_budget=2)
        assert pages == [128, 72]
        assert _non_abandoned_count(connector) == 0
    finally:
        connector.close()


def test_superseded_drainage_rejects_a_stale_position_with_zero_writes(
    tmp_path: Path,
) -> None:
    """A page whose commit response was lost is durable; a delayed retry that
    still carries the committed page's position fails closed with zero writes,
    and the driver resumes from the re-read durable position, which is
    strictly past the committed page."""

    connector = _generated_database(tmp_path / "replay.sqlite3")
    try:
        gate, turn = _authorities(connector)
        _seed_superseded_preparations(connector, count=257)
        first = _superseded_position(connector)
        assert first is not None
        assert _abandon_one_page(connector, gate, turn, now=100, position=first) == 128
        with pytest.raises(OperationalEffectStateError, match="stale"):
            _abandon_one_page(connector, gate, turn, now=101, position=first)
        assert _non_abandoned_count(connector) == 129
        second = _superseded_position(connector)
        assert second is not None and second.advances_past(first)
        assert _abandon_one_page(connector, gate, turn, now=102, position=second) == 128
        third = _superseded_position(connector)
        assert third is not None and third.advances_past(second)
        assert _abandon_one_page(connector, gate, turn, now=103, position=third) == 1
        assert _superseded_position(connector) is None
        assert _non_abandoned_count(connector) == 0
    finally:
        connector.close()


def test_superseded_drainage_excludes_bound_and_current_attempts(
    tmp_path: Path,
) -> None:
    """The live build's drainage never touches the current attempt (same
    policy and generation) nor an attempt bound to a candidate; the retiring
    build's drainage abandons the bound attempt too, because its orphaned
    candidate can never publish."""

    connector = _generated_database(tmp_path / "exclusion.sqlite3")
    try:
        gate, turn = _authorities(connector)
        ids = _seed_superseded_preparations(connector, count=3)
        current = b"current-attempt!"
        connector.execute(
            "INSERT INTO operational_operational_event_streams "
            "(preparation_id, created_at) VALUES (%s, %s)",
            (current, 1),
        )
        connector.execute(
            "INSERT INTO operational_operational_preparations "
            "(preparation_id, build_id, deletion_request_generation, "
            "operational_policy_id, state, prepared_at, completed_at) "
            "VALUES (%s, %s, %s, %s, 'OPEN', 3, NULL)",
            (current, _SUPERSEDED_BUILD, _CURRENT_GENERATION, _CURRENT_POLICY),
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO operational_publication_candidate_preparations "
            "(candidate_id, preparation_id, bound_at) VALUES (%s, %s, %s)",
            (b"c" * 16, ids[1], 7),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        pages, positions = _drain_all(connector, gate, turn, now=100, page_budget=1)
        assert pages == [2]
        assert positions[0].preparation_id == ids[0]
        states = dict(
            connector.fetch_all(
                "SELECT preparation_id, state FROM operational_operational_preparations "
                "WHERE build_id = %s",
                (_SUPERSEDED_BUILD,),
            )
        )
        assert states == {
            ids[0]: "ABANDONED",
            ids[1]: "OPEN",
            ids[2]: "ABANDONED",
            current: "OPEN",
        }
        # The retiring build drains the bound attempt as well; the current
        # attempt is not excluded either because the build is being retired.
        work = VNextUnitOfWork(connector, backend="sqlite")
        with connector.transaction():
            position = OperationalEffectRepository.retiring_build_drain_position(
                work, build_id=_SUPERSEDED_BUILD
            )
            assert position is not None
            assert position.preparation_id == min(ids[1], current)
            assert (
                OperationalEffectRepository.abandon_retiring_build_preparations(
                    work, build_id=_SUPERSEDED_BUILD, position=position, now=200
                )
                == 2
            )
            assert (
                OperationalEffectRepository.retiring_build_drain_position(
                    work, build_id=_SUPERSEDED_BUILD
                )
                is None
            )
        assert _non_abandoned_count(connector) == 0
    finally:
        connector.close()


def test_superseded_drainage_sql_seeks_the_drain_index(tmp_path: Path) -> None:
    """Query-plan evidence: both the position probe and the page of either
    drain mode are one range seek on the (build_id, state, preparation_id)
    index, never a table scan, so a page costs its rows regardless of how many
    rows earlier pages already abandoned."""

    connector = _generated_database(tmp_path / "plan.sqlite3")
    try:
        _seed_superseded_preparations(connector, count=8, states=("OPEN", "COMPLETE"))
        probes: list[tuple[str, tuple[object, ...], bool]] = [
            (
                drain_position_sql(exclusion=True),
                (_SUPERSEDED_BUILD, "OPEN", 1, 0),
                False,
            ),
            (
                drain_page_sql(exclusion=True),
                (_SUPERSEDED_BUILD, "OPEN", 1, 0, _ZERO_UUID, 128),
                True,
            ),
            (drain_position_sql(exclusion=False), (_SUPERSEDED_BUILD, "OPEN"), False),
            (
                drain_page_sql(exclusion=False),
                (_SUPERSEDED_BUILD, "OPEN", _ZERO_UUID, 128),
                True,
            ),
        ]
        for sql, data, seeks_preparation in probes:
            with connector.read_transaction():
                plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, data)
            details = [str(row[-1]) for row in plan]
            expected = re.compile(
                rf"^SEARCH p USING (?:COVERING )?INDEX {_SEEK_INDEX} "
                r"\(build_id=\? AND state=\?"
                + (r" AND preparation_id>\?\)$" if seeks_preparation else r"\)$")
            )
            assert any(expected.match(detail) for detail in details), details
            assert not any(detail.startswith("SCAN p") for detail in details), details
            assert not any("TEMP B-TREE" in detail for detail in details), details
    finally:
        connector.close()
