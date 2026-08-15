from __future__ import annotations

import inspect
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
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
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
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
    command = SourceRootBuildCommand((), b"b" * 16, 20)
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
            connector.execute(
                "INSERT INTO catalog_gallery_observation_stat "
                "(gallery_id, observation_id, file_count, byte_count) "
                "VALUES (%s, %s, %s, %s)",
                (evidence.gallery_id, 1, file_count, byte_count),
            )
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            (b"b" * 16, evidence.gallery_id, 1),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_manifests "
            "(gallery_id, observation_id, manifest_policy_id, "
            "manifest_sha256, computed_at) VALUES (%s, %s, %s, %s, %s)",
            (
                evidence.gallery_id,
                1,
                1,
                sha256(b"manifest" + position.to_bytes(8, "big")).digest(),
                101,
            ),
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

            # A new repository-issued scan plan cannot be spliced into an
            # already-bound receipt chain, even if locator bytes are equal.
            with SourceDiscoveryPlan.from_locators(
                (("z-last",), ("nested", "畫廊"), ("a-first",))
            ) as switched:
                with pytest.raises(SourceBuildConflictError, match="prior batches"):
                    SourceBuildRepository.prepare_discovery_batch(
                        connector,
                        build_id=b"b" * 16,
                        plan=switched,
                    )

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
                with connector.transaction():
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
            assert connector.fetch_one(
                "SELECT state, sealed_at FROM catalog_source_builds"
            ) == ("SEALED", 52)
            assert connector.fetch_one(
                "SELECT manifest_sha256, gallery_count, file_count, byte_count "
                "FROM catalog_build_manifests"
            ) == (
                sealed.next_manifest_chain_sha256,
                3,
                total_files,
                total_bytes,
            )
            with connector.transaction():
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

            connector.execute(
                "INSERT INTO catalog_gallery_observation_stat "
                "(gallery_id, observation_id, file_count, byte_count) "
                "VALUES (%s, %s, %s, %s)",
                (resolved[0].gallery_id, 1, 1, 100),
            )
            other_scope = b"s" * 32
            connector.execute(
                "INSERT INTO catalog_source_scopes "
                "(scope_key, source_provider, source_root_sha256, "
                "identity_policy_version) VALUES (%s, %s, %s, %s)",
                (
                    other_scope,
                    b"filesystem",
                    resolved[0].locator_sha256,
                    1,
                ),
            )
            connector.execute(
                "UPDATE catalog_gallery_identities SET scope_key = %s "
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
                ("execute", "INSERT INTO catalog_source_build_discoveries"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_discovery_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_terminal_discovery)
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
                ("execute", "INSERT INTO catalog_build_manifests"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_assembly_checkpoints",
                ),
                ("execute_affected", "UPDATE catalog_source_builds"),
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
            commit_seal()
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
                connector.execute(
                    "INSERT INTO catalog_canonical_value_allocations "
                    "(value_sha256, digest_domain, byte_count, allocated_at) "
                    "VALUES (%s, %s, %s, %s)",
                    (value, b"source_relative_locator_v1", byte_count, 70),
                )
                connector.execute(
                    "INSERT INTO catalog_canonical_value_pages "
                    "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
                    (page_sha, value, page_bytes),
                )
                decoded = decode_canonical_value_page(page_bytes)
                connector.execute(
                    "INSERT INTO catalog_canonical_value_page_descriptors "
                    "(page_sha256, value_sha256, level, page_position, "
                    "subtree_item_count) VALUES (%s, %s, %s, %s, %s)",
                    (
                        page_sha,
                        value,
                        decoded.level,
                        decoded.page_position,
                        decoded.subtree_byte_count,
                    ),
                )
                connector.execute(
                    "INSERT INTO catalog_canonical_value_identities "
                    "(value_sha256, root_page_sha256) VALUES (%s, %s)",
                    (value, page_sha),
                )
                connector.execute(
                    "INSERT INTO catalog_source_locator_identity "
                    "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
                    (value, leaf),
                )
                gallery_id = position + 1
                connector.execute(
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (%s, %s, %s, %s)",
                    (gallery_id, gallery_key(scope, value), scope, value),
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
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_stat "
                    "(gallery_id, observation_id, file_count, byte_count) "
                    "VALUES (%s, %s, %s, %s)",
                    (gallery_id, 1, 1, row_bytes),
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
                connector.execute(
                    "INSERT INTO catalog_gallery_manifests "
                    "(gallery_id, observation_id, manifest_policy_id, "
                    "manifest_sha256, computed_at) VALUES (%s, %s, %s, %s, %s)",
                    (gallery_id, 1, 1, sha256(b"m" + value).digest(), 72),
                )
            connector.execute(
                "INSERT INTO catalog_source_build_discoveries "
                "(build_id, scan_attempt, gallery_count, "
                "tree_observation_sha256, completed_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (b"b" * 16, b"d" * 16, 257, audit.digest(), 73),
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
            if "FROM catalog_source_builds" in query:
                return (scope, 1, "OPEN", 20, None)
            if "operational_source_build_assembly_checkpoints" in query:
                return (1, b"", 0, 0, 0, empty_chain, "OPEN", 20)
            if "operational_source_build_assembly_batch_receipts" in query:
                return ()
            if "catalog_source_build_discoveries" in query:
                return (0, b"d" * 32, "COMPLETE")
            if "catalog_build_manifests" in query:
                return ()
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
    locked = [query for query in recorder.fetch_one_queries if " FOR UPDATE" in query]
    assert len(locked) == 4
    assert all(query.endswith(" FOR UPDATE") for query in locked)
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
