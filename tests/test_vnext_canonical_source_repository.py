from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
    CANONICAL_VALUE_CHUNK_BYTES,
    SOURCE_ROOT_DIGEST_DOMAIN,
    canonical_value_digest,
    decode_canonical_value_page,
    iter_source_root_payload,
    source_scope_key,
)
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
from h2hdb.vnext_source_build_repository import (
    SourceBuildConflictError,
    SourceBuildRepository,
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
                lease_duration=10_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=10_000,
        )
    return gate, turn


def _put_plan(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=20,
        )
    for prepared in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=prepared,
                now=21,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=22,
        )


def test_disk_backed_plan_crosses_leaf_and_branch_boundaries() -> None:
    byte_count = CANONICAL_VALUE_CHUNK_BYTES * 256 + 1
    with CanonicalValueUploadPlan.from_parts(
        "source_title_utf8_v1",
        (b"x" * CANONICAL_VALUE_CHUNK_BYTES for _ in range(256)),
    ) as exact_boundary:
        pages = list(exact_boundary.iter_pages())
        assert len(pages) == 257
        assert exact_boundary.expected_root_level == 1
        assert decode_canonical_value_page(pages[-1].page_bytes).level == 1

    with CanonicalValueUploadPlan.from_parts(
        "source_title_utf8_v1",
        (b"y" * CANONICAL_VALUE_CHUNK_BYTES for _ in range(256)),
    ) as almost:
        # A second deterministic replay proves the page issuer does not retain
        # a corpus-sized in-memory descriptor list.
        assert len(list(almost.iter_pages())) == 257

    parts = (b"z" * CANONICAL_VALUE_CHUNK_BYTES for _ in range(256))
    with CanonicalValueUploadPlan.from_parts(
        "source_title_utf8_v1",
        (*parts, b"q"),
    ) as over_boundary:
        pages = list(over_boundary.iter_pages())
        assert over_boundary.byte_count == byte_count
        assert len(pages) == 260  # 257 leaves, two level-1 pages, one root.
        assert over_boundary.expected_root_level == 2


def test_writer_lookup_queries_use_declared_key_paths(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "query-plans.sqlite3")
    digest = b"d" * 32
    build_id = b"b" * 16
    try:
        lookups = (
            (
                "SELECT digest_domain, byte_count FROM "
                "catalog_canonical_value_allocations WHERE value_sha256 = %s",
                (digest,),
            ),
            (
                "SELECT page_sha256 FROM catalog_canonical_value_page_descriptors "
                "WHERE value_sha256 = %s AND level = %s AND page_position = %s",
                (digest, 0, 0),
            ),
            (
                "SELECT child_sha256 FROM catalog_canonical_value_page_parents "
                "WHERE parent_sha256 = %s AND position = %s",
                (digest, 0),
            ),
            (
                "SELECT manifest_policy_id FROM catalog_manifest_policies "
                "WHERE manifest_algorithm_version = %s AND file_order_version = %s",
                (1, 1),
            ),
            (
                "SELECT scope_key FROM catalog_source_scopes "
                "WHERE source_provider = %s AND source_root_sha256 = %s "
                "AND identity_policy_version = %s",
                (b"filesystem", digest, 1),
            ),
            (
                "SELECT build_id FROM operational_source_build_generations "
                "WHERE generation = %s",
                (1,),
            ),
            (
                "SELECT build_id FROM operational_source_working_builds "
                "WHERE slot = %s",
                (1,),
            ),
            (
                "SELECT channel FROM catalog_source_build_channel "
                "WHERE build_id = %s",
                (build_id,),
            ),
        )
        for sql, parameters in lookups:
            details = [
                str(row[3])
                for row in connector.fetch_all(
                    "EXPLAIN QUERY PLAN " + sql,
                    parameters,
                )
            ]
            assert details
            assert all("SCAN " not in detail for detail in details), details
            assert any("SEARCH " in detail for detail in details), details
    finally:
        connector.close()


def test_canonical_upload_crash_replay_seal_and_streaming_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "canonical.sqlite3")
    components = tuple(f"segment-{index:03d}-" + "a" * 240 for index in range(130))
    payload_parts = tuple(iter_source_root_payload(components))
    payload = b"".join(payload_parts)
    assert len(payload) > CANONICAL_VALUE_CHUNK_BYTES
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        payload_parts,
    )
    try:
        gate, turn = _authorities(connector)
        with pytest.raises(RuntimeError, match="allocation crash"):
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=19,
                )
                raise RuntimeError("allocation crash")
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_canonical_value_allocations")
            == []
        )

        def reject_transactional_payload_scan(
            _plan: CanonicalValueUploadPlan,
        ) -> Any:
            raise AssertionError("payload stream was scanned inside a DB transaction")

        with monkeypatch.context() as patch:
            patch.setattr(
                CanonicalValueUploadPlan,
                "iter_payload_parts",
                reject_transactional_payload_scan,
            )
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=20,
                )

        first = next(plan.iter_pages())
        with pytest.raises(RuntimeError, match="crash"):
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=first,
                    now=21,
                )
                raise RuntimeError("synthetic crash")
        assert connector.fetch_all("SELECT 1 FROM catalog_canonical_value_pages") == []

        for prepared in plan.iter_pages():
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=prepared,
                    now=22,
                )
            # Response-loss replay is a byte-for-byte no-op.
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=prepared,
                    now=23,
                )
        with pytest.raises(RuntimeError, match="seal crash"):
            with connector.transaction():
                CanonicalValueRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=24,
                )
                raise RuntimeError("seal crash")
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_canonical_value_identities")
            == []
        )
        statement_count = {"fetch_one": 0, "execute": 0}
        original_fetch_one = connector.fetch_one
        original_execute = connector.execute

        def bounded_fetch_one(
            query: str, data: tuple[Any, ...] = ()
        ) -> tuple[Any, ...]:
            statement_count["fetch_one"] += 1
            return original_fetch_one(query, data)

        def bounded_execute(query: str, data: tuple[Any, ...] = ()) -> None:
            statement_count["execute"] += 1
            original_execute(query, data)

        with monkeypatch.context() as patch:
            patch.setattr(connector, "fetch_one", bounded_fetch_one)
            patch.setattr(connector, "execute", bounded_execute)
            patch.setattr(
                CanonicalValueUploadPlan,
                "iter_payload_parts",
                reject_transactional_payload_scan,
            )
            with connector.transaction():
                CanonicalValueRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=25,
                )
        assert statement_count["fetch_one"] <= 80
        assert statement_count["execute"] <= 1
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE value_sha256 = %s",
            (plan.value_sha256,),
        ) == (turn.generation,)

        streamed = bytearray()
        with connector.read_transaction():
            receipt = CanonicalValueRepository.stream_and_validate(
                VNextUnitOfWork(connector, backend="sqlite"),
                value_sha256=plan.value_sha256,
                consume_provisional=streamed.extend,
            )
        assert bytes(streamed) == payload
        assert receipt.value_sha256 == canonical_value_digest(
            SOURCE_ROOT_DIGEST_DOMAIN, payload
        )
    finally:
        plan.close()
        connector.close()


def test_source_root_handoff_releases_only_claim_and_recovers_response_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "source-root.sqlite3")
    command = SourceRootBuildCommand(("Volumes", "資料 A"), b"b" * 16, 30)
    plan = command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, plan)

        def reject_transactional_payload_scan(
            _plan: CanonicalValueUploadPlan,
        ) -> Any:
            raise AssertionError("root payload was scanned inside a DB transaction")

        with monkeypatch.context() as patch:
            patch.setattr(
                CanonicalValueUploadPlan,
                "iter_payload_parts",
                reject_transactional_payload_scan,
            )
            with connector.transaction():
                result = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=command,
                    root_plan=plan,
                    now=31,
                )
        assert result.build_id == command.build_attempt_id
        assert not result.replayed
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (command.build_attempt_id,)
        assert (
            connector.fetch_all(
                "SELECT generation FROM operational_canonical_value_uploads "
                "WHERE value_sha256 = %s",
                (plan.value_sha256,),
            )
            == []
        )

        # The attempt token is not generation authority.  After a lost response,
        # an exact mapped generation returns its durable build even if the caller
        # generated a fresh attempt token.
        retry_command = SourceRootBuildCommand(
            command.source_root_components,
            b"c" * 16,
            32,
        )
        # A whole-request retry may have recreated the same generation/root
        # upload claim before discovering that the generation was mapped.
        _put_plan(connector, gate, turn, plan)
        with connector.transaction():
            replay = SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=retry_command,
                root_plan=plan,
                now=33,
            )
        assert replay.replayed
        assert replay.build_id == command.build_attempt_id
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_source_builds") == (1,)
        assert (
            connector.fetch_all(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (turn.generation, plan.value_sha256),
            )
            == []
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn,
                now=34,
            )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (command.build_attempt_id,)
    finally:
        plan.close()
        connector.close()


def test_absolute_slash_source_root_handoff(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "slash-root.sqlite3")
    command = SourceRootBuildCommand((), b"/" * 16, 40)
    plan = command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, plan)
        with connector.transaction():
            handoff = SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=command,
                root_plan=plan,
                now=41,
            )
        assert handoff.source_root_sha256 == plan.value_sha256
        assert connector.fetch_one(
            "SELECT source_provider, source_root_sha256, identity_policy_version "
            "FROM catalog_source_scopes WHERE scope_key = %s",
            (handoff.scope_key,),
        ) == (b"filesystem", plan.value_sha256, 1)
    finally:
        plan.close()
        connector.close()


def test_cross_scope_build_attempt_conflict_rolls_back_and_retains_claim(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "cross-scope.sqlite3")
    build_id = b"s" * 16
    old_command = SourceRootBuildCommand(("old-root",), build_id, 60)
    new_command = SourceRootBuildCommand(("new-root",), build_id, 60)
    old_plan = old_command.prepare_root_upload()
    new_plan = new_command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, old_plan)
        _put_plan(connector, gate, turn, new_plan)
        old_scope = source_scope_key("filesystem", old_plan.value_sha256, 1)
        connector.execute(
            "INSERT INTO catalog_source_scopes "
            "(scope_key, source_provider, source_root_sha256, "
            "identity_policy_version) VALUES (%s, %s, %s, %s)",
            (old_scope, b"filesystem", old_plan.value_sha256, 1),
        )
        connector.execute(
            "INSERT INTO catalog_source_builds "
            "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
            "VALUES (%s, %s, %s, %s, %s, NULL)",
            (build_id, old_scope, 1, "OPEN", 60),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_channel (build_id, channel) "
            "VALUES (%s, %s)",
            (build_id, b"default"),
        )
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
            (1, build_id, 60),
        )

        with pytest.raises(SourceBuildConflictError):
            with connector.transaction():
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=new_command,
                    root_plan=new_plan,
                    now=61,
                )
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_source_scopes") == (1,)
        assert (
            connector.fetch_all("SELECT 1 FROM operational_source_build_generations")
            == []
        )
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (turn.generation, new_plan.value_sha256),
        ) == (turn.generation,)
    finally:
        old_plan.close()
        new_plan.close()
        connector.close()


def test_source_handoff_rolls_back_each_major_statement_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "handoff-faults.sqlite3")
    command = SourceRootBuildCommand(("fault-root",), b"f" * 16, 50)
    plan = command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, plan)
        fault_points = (
            ("execute", "INSERT INTO catalog_source_scopes"),
            ("execute", "INSERT INTO catalog_source_builds"),
            ("execute", "INSERT INTO catalog_source_build_channel"),
            (
                "execute",
                "INSERT INTO operational_source_build_discovery_checkpoints",
            ),
            (
                "execute",
                "INSERT INTO operational_source_build_assembly_checkpoints",
            ),
            ("execute", "INSERT INTO operational_source_build_generations"),
            ("execute", "INSERT INTO operational_source_working_builds"),
            (
                "execute_affected",
                "DELETE FROM operational_canonical_value_uploads",
            ),
        )
        for method_name, failure_fragment in fault_points:
            with monkeypatch.context() as patch:
                original = getattr(connector, method_name)

                def fail_target(
                    query: str,
                    data: tuple[Any, ...] = (),
                    *,
                    _fragment: str = failure_fragment,
                    _original: Any = original,
                ) -> Any:
                    if _fragment in query:
                        raise RuntimeError("injected handoff statement fault")
                    return _original(query, data)

                patch.setattr(connector, method_name, fail_target)
                with pytest.raises(
                    RuntimeError, match="injected handoff statement fault"
                ):
                    with connector.transaction():
                        SourceBuildRepository.handoff_root(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            command=command,
                            root_plan=plan,
                            now=51,
                        )

            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_source_scopes"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_source_builds"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_source_build_generations"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_source_working_builds"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM " "operational_source_build_discovery_checkpoints"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM " "operational_source_build_assembly_checkpoints"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (turn.generation, plan.value_sha256),
            ) == (1,)
    finally:
        plan.close()
        connector.close()


def test_stale_authority_and_page_collision_leave_zero_semantic_writes(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "fenced.sqlite3")
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        (b"\x00\x00\x00\x01\x00\x00\x00\x00",),
    )
    try:
        gate, turn = _authorities(connector)
        stale_gate = GateLease(
            gate.owner_token,
            gate.gate_generation,
            GateMode.SHARED,
            gate.slots,
            gate.lease_expires_at - 1,
        )
        with pytest.raises(MaintenanceGateUnavailableError):
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=stale_gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=20,
                )
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_canonical_value_allocations")
            == []
        )

        stale_turn = IngestTurn(
            turn.generation,
            turn.owner_token,
            turn.lease_expires_at - 1,
        )
        with pytest.raises(IngestFenceUnavailableError):
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    plan=plan,
                    now=20,
                )
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_canonical_value_allocations")
            == []
        )

        connector.execute(
            "INSERT INTO catalog_canonical_value_allocations "
            "(value_sha256, digest_domain, byte_count, allocated_at) "
            "VALUES (%s, %s, %s, %s)",
            (plan.value_sha256, b"source_title_utf8_v1", plan.byte_count, 19),
        )
        with pytest.raises(CanonicalValueCollisionError):
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=20,
                )
        assert (
            connector.fetch_all("SELECT 1 FROM operational_canonical_value_uploads")
            == []
        )
        connector.execute(
            "DELETE FROM catalog_canonical_value_allocations "
            "WHERE value_sha256 = %s",
            (plan.value_sha256,),
        )

        with connector.transaction():
            CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                now=20,
            )
        prepared = next(plan.iter_pages())
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=prepared,
                now=21,
            )
        connector.execute(
            "UPDATE catalog_canonical_value_pages SET page_bytes = %s "
            "WHERE page_sha256 = %s",
            (b"corrupt", prepared.page_sha256),
        )
        with pytest.raises(CanonicalValueCollisionError):
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=prepared,
                    now=22,
                )
    finally:
        plan.close()
        connector.close()
