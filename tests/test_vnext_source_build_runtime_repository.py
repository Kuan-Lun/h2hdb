from __future__ import annotations

import inspect
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity
from vnext_catalog_registry_fixtures import (
    seed_manifest_policy,
    seed_source_scope,
)
from vnext_manifest_fixtures import seed_gallery_manifest, seed_snapshot_manifest

import h2hdb.vnext_source_build_repository as source_build_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
    artifact_source_manifest_digest,
    decode_canonical_value_page,
    gallery_key,
    iter_source_relative_locator_payload,
)
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    AssemblyBatchAttempt,
    SourceBuildConflictError,
    SourceBuildNotReadyError,
    SourceBuildRepository,
    SourceDiscoveryPlan,
    SourceDiscoveryPlanError,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


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
    seed_manifest_policy(connector)
    return connector


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=1_000_000,
        )
    return gate, turn


def _upload(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def _open_build(
    connector: SQLiteConnector,
) -> tuple[GateLease, IngestTurn, SourceRootBuildCommand]:
    gate, turn = _authorities(connector)
    command = SourceRootBuildCommand((), b"b" * 16)
    with command.prepare_root_upload() as root:
        _upload(connector, gate, turn, root, now=21)
        with connector.transaction():
            SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=command,
                root_plan=root,
                now=24,
            )
    return gate, turn, command


def test_source_build_database_clock_and_abandonment_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "source-abandon.sqlite3")
    try:
        gate, turn, command = _open_build(connector)
        created = connector.fetch_one(
            "SELECT created.created_at, working.assigned_at "
            "FROM catalog_source_build_created_ats created "
            "JOIN operational_source_working_builds working "
            "ON working.build_id = created.build_id "
            "WHERE created.build_id = %s",
            (command.build_attempt_id,),
        )
        assert created[0] == created[1]
        assert created[0] != 24
        # This must be an absolute Unix-microsecond timestamp, not merely the
        # fractional milliseconds of the current second.  The latter can
        # happen if SQLiteConnector rewrites a strftime('%s', ...) format
        # literal as a repository parameter marker.
        assert created[0] > 1_600_000_000_000_000

        with connector.transaction():
            abandoned = SourceBuildRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=command.build_attempt_id,
                now=25,
            )
        assert not abandoned.replayed
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (command.build_attempt_id,),
        ) == ("ABANDONED",)
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (command.build_attempt_id,)

        before = connector.fetch_all(
            "SELECT build_id, state FROM catalog_source_build_states"
        )
        with connector.transaction():
            replay = SourceBuildRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=command.build_attempt_id,
                now=26,
            )
        assert replay.replayed
        assert (
            connector.fetch_all(
                "SELECT build_id, state FROM catalog_source_build_states"
            )
            == before
        )

        replacement = SourceRootBuildCommand((), b"n" * 16)
        with replacement.prepare_root_upload() as root:
            _upload(connector, gate, turn, root, now=27)
            with (
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="ABANDONED"),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=replacement,
                    root_plan=root,
                    now=30,
                )
    finally:
        connector.close()


def test_source_build_abandonment_rolls_back_slot_delete_on_cas_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "source-abandon-fault.sqlite3")
    try:
        gate, turn, command = _open_build(connector)
        original = connector.execute_affected

        def fail_state_cas(query: str, data: tuple[Any, ...] = ()) -> int:
            if "UPDATE catalog_source_build_states" in query:
                raise RuntimeError("injected abandonment CAS failure")
            return original(query, data)

        with monkeypatch.context() as context:
            context.setattr(connector, "execute_affected", fail_state_cas)
            with pytest.raises(RuntimeError, match="injected"):
                with connector.transaction():
                    SourceBuildRepository.abandon(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=command.build_attempt_id,
                        now=25,
                    )
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (command.build_attempt_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (command.build_attempt_id,)
    finally:
        connector.close()


def _published_commit(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    revision: int,
    source_revision: int,
    generation: int,
    snapshot: bytes,
    committed_at: int,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_channels "
        "(source_revision, channel) VALUES (%s, %s)",
        (source_revision, b"default"),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_snapshot_manifests "
        "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
        (source_revision, snapshot),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptor_seals "
        "(source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (%s, 0)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
        (generation,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (%s, %s)",
        (generation, generation - 1),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    members = (
        ("catalog_publication_commit_candidates", "candidate_id", candidate_id),
        (
            "catalog_publication_commit_catalog_revisions",
            "revision",
            revision,
        ),
        (
            "catalog_publication_commit_source_revisions",
            "source_revision",
            source_revision,
        ),
        ("catalog_publication_commit_generations", "generation", generation),
        (
            "catalog_publication_commit_operational_preparations",
            "preparation_id",
            bytes((generation,)) * 16,
        ),
        (
            "catalog_publication_commit_operational_policies",
            "operational_policy_id",
            1,
        ),
        ("catalog_publication_commit_artifact_policies", "artifact_policy_id", 1),
        (
            "catalog_publication_commit_display_title_policies",
            "display_title_policy_id",
            1,
        ),
        ("catalog_publication_commit_new_galleries", "new_galleries", 0),
        ("catalog_publication_commit_changed_galleries", "changed_galleries", 0),
        ("catalog_publication_commit_removed_galleries", "removed_galleries", 0),
        ("catalog_publication_commit_duplicate_losers", "duplicate_losers", 0),
        (
            "catalog_publication_commit_committed_ats",
            "committed_at",
            committed_at,
        ),
    )
    for table, column, value in members:
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    if generation == 1:
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", receipt_id),
        )
    else:
        connector.execute(
            "UPDATE catalog_publication_commit_head_receipts SET receipt_id = %s "
            "WHERE channel = %s",
            (receipt_id, b"default"),
        )
    connector.execute("PRAGMA foreign_keys = ON")


def test_handoff_pins_common_commit_and_replay_ignores_later_head_advance(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "handoff-base-replay.sqlite3")
    try:
        gate, turn = _authorities(connector)
        command = SourceRootBuildCommand((), b"b" * 16)
        with command.prepare_root_upload() as root:
            _upload(connector, gate, turn, root, now=12)
            seed_snapshot_manifest(
                connector,
                snapshot_manifest_sha256=root.value_sha256,
                gallery_count=0,
                file_count=0,
                byte_count=0,
            )
            _published_commit(
                connector,
                receipt_id=b"1" * 16,
                candidate_id=b"c" * 16,
                revision=1,
                source_revision=1,
                generation=1,
                snapshot=root.value_sha256,
                committed_at=15,
            )

            with connector.transaction():
                first = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=command,
                    root_plan=root,
                    now=24,
                )
            assert not first.replayed
            assert connector.fetch_one(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            ) == (command.build_attempt_id, b"1" * 16)
            assert connector.fetch_one(
                "SELECT build_id, base_source_revision, base_source_generation "
                "FROM catalog_source_build_base_source"
            ) == (command.build_attempt_id, 1, 1)

            _published_commit(
                connector,
                receipt_id=b"2" * 16,
                candidate_id=b"d" * 16,
                revision=2,
                source_revision=2,
                generation=2,
                snapshot=root.value_sha256,
                committed_at=25,
            )

            with command.prepare_root_upload() as replay_root:
                _upload(connector, gate, turn, replay_root, now=26)
                with connector.transaction():
                    replay = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        command=command,
                        root_plan=replay_root,
                        now=29,
                    )
            assert replay.replayed and replay.build_id == first.build_id
            assert connector.fetch_all(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            ) == [(command.build_attempt_id, b"1" * 16)]
    finally:
        connector.close()


def _resolve_batch(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: SourceDiscoveryPlan,
    batch: Any,
    *,
    now: int,
) -> tuple[Any, ...]:
    resolved = []
    for locator in batch.locators:
        with plan.prepare_locator_upload(locator) as upload:
            _upload(connector, gate, turn, upload, now=now)
            with connector.transaction():
                resolved.append(
                    SourceBuildRepository.resolve_discovery_locator(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=batch,
                        locator=locator,
                        upload_plan=upload,
                        now=now + 3,
                    )
                )
    return tuple(resolved)


def _finish_discovery(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: SourceDiscoveryPlan,
    *,
    now: int,
) -> tuple[Any, ...]:
    all_resolved: list[Any] = []
    while True:
        batch = SourceBuildRepository.prepare_discovery_batch(
            connector,
            build_id=b"b" * 16,
            plan=plan,
        )
        resolved = _resolve_batch(
            connector,
            gate,
            turn,
            plan,
            batch,
            now=now,
        )
        with connector.transaction():
            receipt = SourceBuildRepository.commit_discovery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                batch=batch,
                resolved=resolved,
                now=now + 4,
            )
        all_resolved.extend(resolved)
        if receipt.terminal:
            return tuple(all_resolved)
        now += 10


def _insert_observation_stat(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
    file_count: int,
    byte_count: int,
) -> None:
    key = (gallery_id, observation_id)
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat_anchors "
        "(gallery_id, observation_id) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat_file_counts "
        "(gallery_id, observation_id, file_count) VALUES (%s, %s, %s)",
        (*key, file_count),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat_byte_counts "
        "(gallery_id, observation_id, byte_count) VALUES (%s, %s, %s)",
        (*key, byte_count),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat_seals "
        "(gallery_id, observation_id) VALUES (%s, %s)",
        key,
    )


def _insert_source_build_discovery(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scan_attempt: bytes,
    gallery_count: int,
    tree_observation_sha256: bytes,
    completed_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_scan_attempts "
        "(build_id, scan_attempt) VALUES (%s, %s)",
        (build_id, scan_attempt),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_gallery_counts "
        "(build_id, gallery_count) VALUES (%s, %s)",
        (build_id, gallery_count),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_tree_observation_sha256s "
        "(build_id, tree_observation_sha256) VALUES (%s, %s)",
        (build_id, tree_observation_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_completed_ats "
        "(build_id, completed_at) VALUES (%s, %s)",
        (build_id, completed_at),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_seals (build_id) VALUES (%s)",
        (build_id,),
    )


def _source_build_discovery_snapshot(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(sql)
        for sql in (
            "SELECT * FROM catalog_source_build_discovery_anchors",
            "SELECT * FROM catalog_source_build_discovery_scan_attempts",
            "SELECT * FROM catalog_source_build_discovery_gallery_counts",
            "SELECT * FROM catalog_source_build_discovery_tree_observation_sha256s",
            "SELECT * FROM catalog_source_build_discovery_completed_ats",
            "SELECT * FROM catalog_source_build_discovery_seals",
        )
    )


def test_source_discovery_vertical_replay_seal_visibility_and_corruption(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "discovery-vertical-replay.sqlite3")
    try:
        _open_build(connector)
        values = (b"b" * 16, b"d" * 16, 3, b"t" * 32, 40)
        with connector.transaction():
            source_build_module._persist_source_build_discovery(
                connector,
                build_id=values[0],
                scan_attempt=values[1],
                gallery_count=values[2],
                tree_observation_sha256=values[3],
                completed_at=values[4],
            )
        committed = _source_build_discovery_snapshot(connector)
        assert all(rows for rows in committed)
        assert (
            connector.fetch_one(
                "SELECT build_id, scan_attempt, gallery_count, "
                "tree_observation_sha256, completed_at "
                "FROM catalog_source_build_discoveries WHERE build_id = %s",
                (values[0],),
            )
            == values
        )

        with connector.transaction():
            source_build_module._persist_source_build_discovery(
                connector,
                build_id=values[0],
                scan_attempt=values[1],
                gallery_count=values[2],
                tree_observation_sha256=values[3],
                completed_at=values[4],
            )
        assert _source_build_discovery_snapshot(connector) == committed

        connector.execute(
            "DELETE FROM catalog_source_build_discovery_seals WHERE build_id = %s",
            (values[0],),
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_source_build_discoveries WHERE build_id = %s",
                (values[0],),
            )
            == []
        )
    finally:
        connector.close()

    corrupt = _generated_database(tmp_path / "discovery-vertical-corrupt.sqlite3")
    try:
        _open_build(corrupt)
        build_id = b"b" * 16
        corrupt.execute(
            "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
            (build_id,),
        )
        corrupt.execute(
            "INSERT INTO catalog_source_build_discovery_completed_ats "
            "(build_id, completed_at) VALUES (%s, 999)",
            (build_id,),
        )
        before = _source_build_discovery_snapshot(corrupt)
        with pytest.raises(SourceBuildConflictError, match="completed at"):
            with corrupt.transaction():
                source_build_module._persist_source_build_discovery(
                    corrupt,
                    build_id=build_id,
                    scan_attempt=b"d" * 16,
                    gallery_count=3,
                    tree_observation_sha256=b"t" * 32,
                    completed_at=40,
                )
        assert _source_build_discovery_snapshot(corrupt) == before
        assert corrupt.fetch_all("SELECT 1 FROM catalog_source_build_discoveries") == []
    finally:
        corrupt.close()


def test_source_discovery_vertical_mariadb_sql_is_static_and_seal_last() -> None:
    class _MariaDiscoveryRecorder:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            return ()

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    recorder = _MariaDiscoveryRecorder()
    source_build_module._persist_source_build_discovery(
        recorder,
        build_id=b"b" * 16,
        scan_attempt=b"d" * 16,
        gallery_count=3,
        tree_observation_sha256=b"t" * 32,
        completed_at=40,
    )
    ordered_tables = (
        "catalog_source_build_discovery_anchors",
        "catalog_source_build_discovery_scan_attempts",
        "catalog_source_build_discovery_gallery_counts",
        "catalog_source_build_discovery_tree_observation_sha256s",
        "catalog_source_build_discovery_completed_ats",
        "catalog_source_build_discovery_seals",
    )
    assert len(recorder.executions) == len(ordered_tables)
    for (query, _data), table in zip(recorder.executions, ordered_tables, strict=True):
        assert query.lstrip().startswith(f"INSERT INTO {table}")
        assert "%s" in query and "?" not in query
    assert (
        recorder.executions[-1][0]
        .lstrip()
        .startswith("INSERT INTO catalog_source_build_discovery_seals")
    )


def _stage_assembly_inputs(
    connector: SQLiteConnector,
    resolved: tuple[Any, ...],
    *,
    omit_stat: bool = False,
) -> tuple[int, int]:
    total_files = 0
    total_bytes = 0
    for position, evidence in enumerate(resolved):
        file_count = position + 1
        byte_count = (position + 1) * 100
        total_files += file_count
        total_bytes += byte_count
        connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, %s)",
            (evidence.gallery_id, 1, 100),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observations "
            "(gallery_id, observation_id, observation_identity_sha256) "
            "VALUES (%s, %s, %s)",
            (evidence.gallery_id, 1, evidence.locator_sha256),
        )
        if not omit_stat:
            _insert_observation_stat(
                connector,
                gallery_id=evidence.gallery_id,
                observation_id=1,
                file_count=file_count,
                byte_count=byte_count,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            (b"b" * 16, evidence.gallery_id, 1),
        )
        seed_gallery_manifest(
            connector,
            gallery_id=evidence.gallery_id,
            observation_id=1,
            manifest_policy_id=1,
            manifest_sha256=artifact_source_manifest_digest(
                evidence.locator_sha256,
                1,
                1,
            ),
            computed_at=101,
        )
    return total_files, total_bytes


def test_disk_plan_sorts_unsigned_digest_caps_pages_and_rejects_duplicates(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "plan.sqlite3")
    try:
        _open_build(connector)
        locators = tuple((f"gallery-{index:04d}",) for index in reversed(range(257)))
        with SourceDiscoveryPlan.from_locators(locators) as plan:
            first = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            assert len(first.locators) == 256
            digests = [locator.locator_sha256 for locator in first.locators]
            assert digests == sorted(digests)
            assert not first.terminal
        with pytest.raises(SourceDiscoveryPlanError, match="duplicate"):
            SourceDiscoveryPlan.from_locators((("same",), ("same",)))

        with SourceDiscoveryPlan.from_locators(locators) as first_plan:
            first_receipt = (
                first_plan.scan_attempt,
                first_plan.gallery_count,
                first_plan.tree_observation_sha256,
            )
        with SourceDiscoveryPlan.from_locators(locators) as rebuilt:
            assert (
                rebuilt.scan_attempt,
                rebuilt.gallery_count,
                rebuilt.tree_observation_sha256,
            ) == first_receipt

        discovery_parameters = inspect.signature(
            SourceBuildRepository.prepare_discovery_batch
        ).parameters
        assert "cursor" not in discovery_parameters
        assert "count" not in discovery_parameters
        assembly_parameters = inspect.signature(
            SourceBuildRepository.assemble_batch
        ).parameters
        assert "cursor" not in assembly_parameters
        assert "file_count" not in assembly_parameters
    finally:
        connector.close()


def test_pending_source_gallery_is_bounded_pk_driven_and_decodes_plan_position(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "pending-source-gallery.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("gallery",),)) as plan:
            resolved = _finish_discovery(
                connector,
                gate,
                turn,
                plan,
                now=40,
            )
            pending = SourceBuildRepository.get_pending_source_gallery(
                connector,
                build_id=b"b" * 16,
            )
            assert pending is not None
            assert pending.position == 0
            assert pending.gallery_id == resolved[0].gallery_id
            assert pending.locator_sha256 == resolved[0].locator_sha256
            assert plan._decode_locator(
                pending.position,
                pending.locator_sha256,
            ) == ("gallery",)

        query_plan = connector.fetch_all(
            "EXPLAIN QUERY PLAN " + source_build_module._PENDING_SOURCE_GALLERY_QUERY,
            (b"b" * 16,),
        )
        assert query_plan
        details = tuple(str(row[3]) for row in query_plan)
        assert all("SCAN " not in detail for detail in details)
        assert any(
            "catalog_source_build_expected_gallery" in detail
            or "sqlite_autoindex_catalog_source_build_expected_gallery" in detail
            for detail in details
        )
        assert any(
            "catalog_source_build_galleries" in detail
            or "sqlite_autoindex_catalog_source_build_galleries" in detail
            for detail in details
        )
    finally:
        connector.close()


def test_discovery_new_generation_assembly_and_response_loss(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "runtime.sqlite3")
    try:
        gate, turn, root_command = _open_build(connector)
        assert connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_count, state "
            "FROM operational_source_build_discovery_checkpoints"
        ) == (1, b"", 0, "OPEN")
        assert connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_gallery_count, "
            "processed_file_count, processed_byte_count, "
            "manifest_chain_sha256, state "
            "FROM operational_source_build_assembly_checkpoints"
        ) == (
            1,
            b"",
            0,
            0,
            0,
            bytes.fromhex(
                "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
            ),
            "OPEN",
        )

        with SourceDiscoveryPlan.from_locators(
            (("z-last",), ("nested", "畫廊"), ("a-first",))
        ) as plan:
            first = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            resolved = _resolve_batch(
                connector,
                gate,
                turn,
                plan,
                first,
                now=30,
            )
            with connector.transaction():
                committed = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=first,
                    resolved=resolved,
                    now=34,
                )
            with connector.transaction():
                replay = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=first,
                    resolved=resolved,
                    now=35,
                )
            assert replay.replayed
            assert replay.committed_at == committed.committed_at

            # Rebuilding the same complete snapshot produces the same
            # deterministic scan attempt and resumes the exact receipt chain.
            with SourceDiscoveryPlan.from_locators(
                (("z-last",), ("nested", "畫廊"), ("a-first",))
            ) as switched:
                resumed = SourceBuildRepository.prepare_discovery_batch(
                    connector,
                    build_id=b"b" * 16,
                    plan=switched,
                )
                assert resumed.terminal
                assert resumed.start_generation == committed.committed_generation
                assert resumed.scan_attempt == plan.scan_attempt

            with connector.transaction():
                IngestFenceRepository.complete(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    turn,
                    now=40,
                )
            with connector.transaction():
                turn2 = IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=b"j" * 16,
                    now=41,
                    lease_duration=1_000_000,
                )
            with root_command.prepare_root_upload() as root2:
                _upload(connector, gate, turn2, root2, now=42)
                with (
                    patch.object(
                        source_build_module,
                        "database_unix_microseconds",
                        side_effect=AssertionError("clock"),
                    ),
                    connector.transaction(),
                ):
                    handoff2 = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn2,
                        command=root_command,
                        root_plan=root2,
                        now=45,
                    )
            assert handoff2.generation == 2
            terminal = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            assert terminal.terminal
            with connector.transaction():
                terminal_receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    batch=terminal,
                    resolved=(),
                    now=46,
                )
            with connector.transaction():
                terminal_replay = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    batch=terminal,
                    resolved=(),
                    now=47,
                )
            assert terminal_receipt.terminal
            assert terminal_replay.replayed
            assert connector.fetch_one(
                "SELECT scan_attempt, gallery_count, tree_observation_sha256 "
                "FROM catalog_source_build_discoveries"
            ) == (plan.scan_attempt, 3, plan.tree_observation_sha256)
            assert _source_build_discovery_snapshot(connector) == (
                [(b"b" * 16,)],
                [(b"b" * 16, plan.scan_attempt)],
                [(b"b" * 16, 3)],
                [(b"b" * 16, plan.tree_observation_sha256)],
                [(b"b" * 16, 46)],
                [(b"b" * 16,)],
            )

            total_files, total_bytes = _stage_assembly_inputs(connector, resolved)
            attempt = SourceBuildRepository.issue_assembly_batch()
            with connector.transaction():
                assembled = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=attempt,
                    now=50,
                )
            assert assembled.row_count == 3
            assert assembled.next_file_count == total_files
            assert assembled.next_byte_count == total_bytes
            with connector.transaction():
                assembled_replay = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=attempt,
                    now=51,
                )
            assert assembled_replay.replayed
            assert assembled_replay.next_manifest_chain_sha256 == (
                assembled.next_manifest_chain_sha256
            )

            terminal_attempt = SourceBuildRepository.issue_assembly_batch()
            with connector.transaction():
                sealed = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=52,
                )
            assert sealed.terminal
            sealed_source = connector.fetch_one(
                "SELECT state, created_at, sealed_at FROM catalog_source_builds"
            )
            assert sealed_source[0] == "SEALED"
            assert sealed_source[1] <= sealed_source[2]
            assert sealed_source[2] != 52
            assert connector.fetch_one(
                "SELECT manifest_sha256, gallery_count, file_count, byte_count, "
                "computed_at "
                "FROM catalog_build_manifests"
            ) == (
                sealed.next_manifest_chain_sha256,
                3,
                total_files,
                total_bytes,
                sealed_source[2],
            )
            with (
                patch.object(
                    source_build_module,
                    "database_unix_microseconds",
                    side_effect=AssertionError("clock"),
                ),
                connector.transaction(),
            ):
                sealed_replay = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=53,
                )
            assert sealed_replay.replayed

            # A delayed root response retry still resolves the immutable
            # generation mapping after the build has advanced to SEALED.
            with root_command.prepare_root_upload() as sealed_root_retry:
                _upload(connector, gate, turn2, sealed_root_retry, now=55)
                with connector.transaction():
                    root_replay = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn2,
                        command=root_command,
                        root_plan=sealed_root_retry,
                        now=58,
                    )
            assert root_replay.replayed
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_canonical_value_uploads "
                    "WHERE generation = %s AND value_sha256 = %s",
                    (turn2.generation, root_command.source_root_sha256),
                )
                == []
            )

            connector.execute(
                "UPDATE catalog_build_manifest_file_counts SET file_count = %s "
                "WHERE build_id = %s",
                (total_files + 1, b"b" * 16),
            )
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("write"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("write"),
                ),
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="manifest"),
            ):
                SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=59,
                )

            with pytest.raises(IngestFenceUnavailableError):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=terminal_attempt,
                        now=54,
                    )
    finally:
        connector.close()


def test_assembly_missing_dependency_and_scope_corruption_are_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "corruption.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("gallery",),)) as plan:
            resolved = _finish_discovery(
                connector,
                gate,
                turn,
                plan,
                now=30,
            )
            _stage_assembly_inputs(connector, resolved, omit_stat=True)
            attempt = SourceBuildRepository.issue_assembly_batch()
            with pytest.raises(SourceBuildNotReadyError, match="lacks"):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=attempt,
                        now=60,
                    )
            assert connector.fetch_one(
                "SELECT generation, processed_gallery_count "
                "FROM operational_source_build_assembly_checkpoints"
            ) == (1, 0)
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                )
                == []
            )

            _insert_observation_stat(
                connector,
                gallery_id=resolved[0].gallery_id,
                observation_id=1,
                file_count=1,
                byte_count=100,
            )
            other_scope = seed_source_scope(
                connector,
                source_root_sha256=resolved[0].locator_sha256,
            ).scope_key
            connector.execute(
                "UPDATE catalog_gallery_identity_coordinates SET scope_key = %s "
                "WHERE gallery_id = %s",
                (other_scope, resolved[0].gallery_id),
            )
            with pytest.raises(SourceBuildConflictError, match="scope"):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=attempt,
                        now=61,
                    )
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                )
                == []
            )

        source = inspect.getsource(SourceBuildRepository)
        assert "COUNT(" not in source.upper()
        assert "SUM(" not in source.upper()
        with pytest.raises(TypeError):
            AssemblyBatchAttempt(b"x" * 32, object())
    finally:
        connector.close()


def test_discovery_and_assembly_major_statement_faults_roll_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "faults.sqlite3")

    def fail_once(
        method_name: str,
        fragment: str,
        operation: Any,
    ) -> None:
        with monkeypatch.context() as context:
            original = getattr(connector, method_name)

            def fail(
                query: str,
                data: tuple[Any, ...] = (),
                *,
                _original: Any = original,
            ) -> Any:
                if fragment in query:
                    raise RuntimeError("injected source-build statement fault")
                return _original(query, data)

            context.setattr(connector, method_name, fail)
            with pytest.raises(
                RuntimeError,
                match="injected source-build statement fault",
            ):
                operation()

    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("fault-gallery",),)) as plan:
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            resolved = _resolve_batch(
                connector,
                gate,
                turn,
                plan,
                batch,
                now=30,
            )

            def commit_data() -> None:
                with connector.transaction():
                    SourceBuildRepository.commit_discovery_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=batch,
                        resolved=resolved,
                        now=34,
                    )

            for method, fragment in (
                ("execute", "INSERT INTO catalog_source_build_expected_gallery"),
                (
                    "execute",
                    "INSERT INTO operational_source_build_discovery_batch_receipts",
                ),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_discovery_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_data)
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM catalog_source_build_expected_gallery"
                    )
                    == []
                )
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM "
                        "operational_source_build_discovery_batch_receipts"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_count, state FROM "
                    "operational_source_build_discovery_checkpoints"
                ) == (1, 0, "OPEN")
            commit_data()

            terminal = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )

            def commit_terminal_discovery() -> None:
                with connector.transaction():
                    SourceBuildRepository.commit_discovery_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=terminal,
                        resolved=(),
                        now=40,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_discovery_batch_receipts",
                ),
                ("execute", "INSERT INTO catalog_source_build_discovery_anchors"),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_scan_attempts",
                ),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_gallery_counts",
                ),
                (
                    "execute",
                    "INSERT INTO "
                    "catalog_source_build_discovery_tree_observation_sha256s",
                ),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_completed_ats",
                ),
                ("execute", "INSERT INTO catalog_source_build_discovery_seals"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_discovery_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_terminal_discovery)
                assert _source_build_discovery_snapshot(connector) == (
                    [],
                    [],
                    [],
                    [],
                    [],
                    [],
                )
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM catalog_source_build_discoveries"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_count, state FROM "
                    "operational_source_build_discovery_checkpoints"
                ) == (2, 1, "OPEN")
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM "
                    "operational_source_build_discovery_batch_receipts"
                ) == (1,)
            commit_terminal_discovery()

            _stage_assembly_inputs(connector, resolved)
            assembly = SourceBuildRepository.issue_assembly_batch()

            def commit_assembly() -> None:
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=assembly,
                        now=50,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_assembly_batch_receipts",
                ),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_assembly_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_assembly)
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_gallery_count, state FROM "
                    "operational_source_build_assembly_checkpoints"
                ) == (1, 0, "OPEN")
            commit_assembly()

            seal = SourceBuildRepository.issue_assembly_batch()

            def commit_seal() -> None:
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=seal,
                        now=60,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_assembly_batch_receipts",
                ),
                ("execute", "INSERT INTO catalog_source_build_sealed_ats"),
                ("execute", "INSERT INTO catalog_build_manifest_anchors"),
                (
                    "execute",
                    "INSERT INTO catalog_build_manifest_manifest_sha256s",
                ),
                ("execute", "INSERT INTO catalog_build_manifest_file_counts"),
                ("execute", "INSERT INTO catalog_build_manifest_byte_counts"),
                ("execute", "INSERT INTO catalog_build_manifest_seals"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_assembly_checkpoints",
                ),
                ("execute_affected", "UPDATE catalog_source_build_states"),
            ):
                fail_once(method, fragment, commit_seal)
                assert (
                    connector.fetch_all("SELECT 1 FROM catalog_build_manifests") == []
                )
                assert connector.fetch_one(
                    "SELECT state, sealed_at FROM catalog_source_builds"
                ) == ("OPEN", None)
                assert connector.fetch_one(
                    "SELECT generation, processed_gallery_count, state FROM "
                    "operational_source_build_assembly_checkpoints"
                ) == (2, 1, "OPEN")
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM "
                    "operational_source_build_assembly_batch_receipts"
                ) == (1,)
            terminal_trace: list[str] = []
            connector.connection.set_trace_callback(terminal_trace.append)
            try:
                commit_seal()
            finally:
                connector.connection.set_trace_callback(None)
            checkpoint_cas = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "UPDATE OPERATIONAL_SOURCE_BUILD_ASSEMBLY_CHECKPOINTS" in sql.upper()
            )
            state_cas = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "UPDATE CATALOG_SOURCE_BUILD_STATES" in sql.upper()
            )
            manifest_anchor = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "INSERT INTO CATALOG_BUILD_MANIFEST_ANCHORS" in sql.upper()
            )
            manifest_seal = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "INSERT INTO CATALOG_BUILD_MANIFEST_SEALS" in sql.upper()
            )
            assert checkpoint_cas < state_cas < manifest_anchor < manifest_seal
            assert connector.fetch_one("SELECT state FROM catalog_source_builds") == (
                "SEALED",
            )
    finally:
        connector.close()


def test_assembly_pages_257_rows_in_bounded_keyset_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "bounded-assembly.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        scope = connector.fetch_one(
            "SELECT scope_key FROM catalog_source_builds WHERE build_id = %s",
            (b"b" * 16,),
        )[0]
        prepared: list[tuple[bytes, bytes, bytes, int, bytes]] = []
        for index in range(257):
            leaf = f"bounded-{index:04d}".encode()
            with CanonicalValueUploadPlan.from_parts(
                "source_relative_locator_v1",
                iter_source_relative_locator_payload((leaf.decode(),)),
            ) as locator_plan:
                pages = list(locator_plan.iter_pages())
                assert len(pages) == 1
                prepared.append(
                    (
                        locator_plan.value_sha256,
                        pages[0].page_sha256,
                        pages[0].page_bytes,
                        locator_plan.byte_count,
                        leaf,
                    )
                )
        prepared.sort(key=lambda item: item[0])
        audit = sha256(b"h2hdb-vnext-source-build-discovery-audit-v1\0")
        audit.update(len(prepared).to_bytes(8, "big"))
        total_bytes = 0
        with connector.transaction():
            for position, (value, page_sha, page_bytes, byte_count, leaf) in enumerate(
                prepared
            ):
                audit.update(value)
                decoded = decode_canonical_value_page(page_bytes)
                seed_canonical_value(
                    connector,
                    value_sha256=value,
                    digest_domain=b"source_relative_locator_v1",
                    page_sha256=page_sha,
                    page_bytes=page_bytes,
                    subtree_item_count=decoded.subtree_byte_count,
                    allocated_at=70,
                    level=decoded.level,
                    page_position=decoded.page_position,
                )
                connector.execute(
                    "INSERT INTO catalog_source_locator_identity "
                    "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
                    (value, leaf),
                )
                gallery_id = position + 1
                seed_gallery_identity(
                    connector,
                    gallery_id=gallery_id,
                    gallery_key=gallery_key(scope, value),
                    scope_key=scope,
                    locator_sha256=value,
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) "
                    "VALUES (%s, %s, %s)",
                    (gallery_id, 1, 71),
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_observations "
                    "(gallery_id, observation_id, observation_identity_sha256) "
                    "VALUES (%s, %s, %s)",
                    (gallery_id, 1, value),
                )
                row_bytes = position + 1
                total_bytes += row_bytes
                _insert_observation_stat(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=1,
                    file_count=1,
                    byte_count=row_bytes,
                )
                connector.execute(
                    "INSERT INTO catalog_source_build_expected_gallery "
                    "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
                    (b"b" * 16, position, gallery_id),
                )
                connector.execute(
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    (b"b" * 16, gallery_id, 1),
                )
                seed_gallery_manifest(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=1,
                    manifest_policy_id=1,
                    manifest_sha256=artifact_source_manifest_digest(value, 1, 1),
                    computed_at=72,
                )
            _insert_source_build_discovery(
                connector,
                build_id=b"b" * 16,
                scan_attempt=b"d" * 16,
                gallery_count=257,
                tree_observation_sha256=audit.digest(),
                completed_at=73,
            )
            connector.execute_affected(
                "UPDATE operational_source_build_discovery_checkpoints "
                "SET generation = %s, cursor_bytes = %s, processed_count = %s, "
                "state = %s, updated_at = %s WHERE build_id = %s",
                (2, (256).to_bytes(8, "big"), 257, "COMPLETE", 73, b"b" * 16),
            )

        original_fetch_all = connector.fetch_all
        observed_page_sizes: list[int] = []
        observed_queries: list[tuple[str, tuple[Any, ...]]] = []

        def bounded_fetch_all(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            rows = original_fetch_all(query, data)
            if "FROM catalog_source_build_expected_gallery e" in query:
                assert data[-1] == 256
                assert "ORDER BY e.position LIMIT %s" in query
                assert len(rows) <= 256
                observed_page_sizes.append(len(rows))
                observed_queries.append((query, data))
            return rows

        with monkeypatch.context() as context:
            context.setattr(connector, "fetch_all", bounded_fetch_all)
            receipts = []
            for now in (80, 81, 82):
                with connector.transaction():
                    receipts.append(
                        SourceBuildRepository.assemble_batch(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            build_id=b"b" * 16,
                            attempt=SourceBuildRepository.issue_assembly_batch(),
                            now=now,
                        )
                    )
        assert [receipt.row_count for receipt in receipts] == [256, 1, 0]
        assert observed_page_sizes == [256, 1, 0]
        assert receipts[-1].terminal
        assert receipts[-1].next_gallery_count == 257
        assert receipts[-1].next_file_count == 257
        assert receipts[-1].next_byte_count == total_bytes
        assembly_query, assembly_data = next(
            item
            for item in observed_queries
            if "FROM catalog_source_build_expected_gallery e" in item[0]
        )
        query_plan = connector.fetch_all(
            "EXPLAIN QUERY PLAN " + assembly_query,
            assembly_data,
        )
        assert query_plan
        assert all("SCAN " not in str(row[3]) for row in query_plan)
    finally:
        connector.close()


def test_mariadb_shape_locks_authorities_and_uses_full_checkpoint_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_id = b"b" * 16
    scope = b"s" * 32
    empty_chain = bytes.fromhex(
        "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
    )

    class RecordingConnector:
        def __init__(self) -> None:
            self.fetch_one_queries: list[str] = []
            self.fetch_all_queries: list[tuple[str, tuple[Any, ...]]] = []
            self.execute_queries: list[str] = []
            self.cas_queries: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.fetch_one_queries.append(query)
            if "operational_source_build_generations" in query:
                return (build_id,)
            if "operational_source_working_builds" in query:
                return (build_id, 20)
            if query.startswith("WITH family_keys(build_id)") and (
                "catalog_source_build_anchors" in query
            ):
                return (
                    build_id,
                    build_id,
                    build_id,
                    scope,
                    build_id,
                    1,
                    build_id,
                    "OPEN",
                    build_id,
                    20,
                    build_id,
                    None,
                    None,
                )
            if "FROM catalog_source_build_states" in query:
                return ("OPEN",)
            if "operational_source_build_assembly_checkpoints" in query:
                return (1, b"", 0, 0, 0, empty_chain, "OPEN", 20)
            if "operational_source_build_assembly_batch_receipts" in query:
                return ()
            if "catalog_source_build_discovery_seals" in query:
                return (0, b"d" * 32, "COMPLETE")
            if query.startswith("SELECT gallery_count FROM ") and (
                "catalog_source_build_discovery_gallery_counts" in query
            ):
                return (0,)
            if query.startswith("SELECT sealed_at FROM ") and (
                "catalog_source_build_sealed_ats" in query
            ):
                return (30_000_000,)
            if query.startswith("WITH family_keys(build_id)") and (
                "catalog_build_manifest_anchors" in query
            ):
                return ()
            if "UTC_TIMESTAMP(6)" in query:
                return (30_000_000,)
            raise AssertionError((query, data))

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.fetch_all_queries.append((query, data))
            return []

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            del data
            self.execute_queries.append(query)

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            del data
            self.cas_queries.append(query)
            return 1

    recorder = RecordingConnector()
    monkeypatch.setattr(
        "h2hdb.vnext_source_build_repository._authorize",
        lambda *_args, **_kwargs: 1,
    )
    receipt = SourceBuildRepository.assemble_batch(
        VNextUnitOfWork(recorder, backend="mariadb"),  # type: ignore[arg-type]
        gate_lease=None,  # type: ignore[arg-type]
        ingest_turn=None,  # type: ignore[arg-type]
        build_id=build_id,
        attempt=SourceBuildRepository.issue_assembly_batch(),
        now=30,
    )
    assert receipt.terminal
    assert receipt.committed_at == 30_000_000
    locked = [query for query in recorder.fetch_one_queries if " FOR UPDATE" in query]
    assert len(locked) == 4
    assert all(query.endswith(" FOR UPDATE") for query in locked)
    assert any("UTC_TIMESTAMP(6)" in query for query in recorder.fetch_one_queries)
    manifest_writes = [
        query
        for query in recorder.execute_queries
        if "catalog_build_manifest_" in query
    ]
    assert [
        next(
            name
            for name in (
                "anchors",
                "manifest_sha256s",
                "file_counts",
                "byte_counts",
                "seals",
            )
            if f"catalog_build_manifest_{name}" in query
        )
        for query in manifest_writes
    ] == ["anchors", "manifest_sha256s", "file_counts", "byte_counts", "seals"]
    assert any(
        "LIMIT %s" in query and data[-1] == 256
        for query, data in recorder.fetch_all_queries
    )
    checkpoint_cas = next(
        query
        for query in recorder.cas_queries
        if "operational_source_build_assembly_checkpoints" in query
    )
    for predicate in (
        "generation = %s",
        "cursor_bytes = %s",
        "processed_gallery_count = %s",
        "processed_file_count = %s",
        "processed_byte_count = %s",
        "manifest_chain_sha256 = %s",
        "state = %s",
        "updated_at = %s",
    ):
        assert predicate in checkpoint_cas


def test_mariadb_abandonment_locks_exact_root_and_keeps_generation_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_id = b"b" * 16
    scope = b"s" * 32

    class RecordingConnector:
        def __init__(self) -> None:
            self.fetches: list[str] = []
            self.mutations: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            del data
            self.fetches.append(query)
            if "operational_source_build_generations" in query:
                return (build_id,)
            if "operational_source_working_builds" in query:
                return (build_id, 20)
            if query.startswith("WITH family_keys(build_id)"):
                return (
                    build_id,
                    build_id,
                    build_id,
                    scope,
                    build_id,
                    1,
                    build_id,
                    "OPEN",
                    build_id,
                    20,
                    build_id,
                    None,
                    None,
                )
            if "catalog_source_build_states" in query:
                return ("OPEN",)
            raise AssertionError(query)

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            del data
            self.mutations.append(query)
            return 1

    connector = RecordingConnector()
    monkeypatch.setattr(
        "h2hdb.vnext_source_build_repository._authorize",
        lambda *_args, **_kwargs: 1,
    )
    result = SourceBuildRepository.abandon(
        VNextUnitOfWork(connector, backend="mariadb"),  # type: ignore[arg-type]
        gate_lease=None,  # type: ignore[arg-type]
        ingest_turn=None,  # type: ignore[arg-type]
        build_id=build_id,
        now=30,
    )
    assert not result.replayed
    locked = [query for query in connector.fetches if " FOR UPDATE" in query]
    assert len(locked) == 3
    assert all(query.endswith(" FOR UPDATE") for query in locked)
    assert connector.mutations == [
        "DELETE FROM operational_source_working_builds "
        "WHERE slot = %s AND build_id = %s AND assigned_at = %s",
        "UPDATE catalog_source_build_states SET state = %s "
        "WHERE build_id = %s AND state = %s",
    ]
    assert not any("source_build_generations" in query for query in connector.mutations)
