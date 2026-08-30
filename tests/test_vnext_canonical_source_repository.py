from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import (
    seed_canonical_allocation,
    seed_canonical_page,
)
from vnext_catalog_registry_fixtures import (
    seed_manifest_policy,
    seed_source_scope,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import seed_source_build

import h2hdb.vnext_source_build_repository as source_build_module
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_family import (
    load_allocation_family,
    load_page_family,
    load_sealed_value_identity,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValuePartialFamilyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_catalog_registry_repository import (
    CatalogRegistryConflictError,
    ensure_source_scope,
    load_source_scope,
)
from h2hdb.vnext_identity import (
    CANONICAL_VALUE_CHUNK_BYTES,
    SOURCE_ROOT_DIGEST_DOMAIN,
    GalleryObservationNodeKind,
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
    SourceBuildManifestSummary,
    SourceBuildRepository,
    SourceRootBuildCommand,
    source_build_identity,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _generated_database(path: Path) -> SQLiteConnector:
    connector = open_generated_sqlite_database(path)
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
                "SELECT d.digest_domain, c.byte_count FROM "
                "catalog_canonical_value_allocation_seals s "
                "JOIN catalog_canonical_value_allocation_digest_domains d "
                "ON d.value_sha256 = s.value_sha256 "
                "JOIN catalog_canonical_value_allocation_byte_counts c "
                "ON c.value_sha256 = s.value_sha256 "
                "WHERE s.value_sha256 = %s",
                (digest,),
            ),
            (
                "SELECT c.page_sha256 FROM "
                "catalog_canonical_value_page_coordinates c "
                "JOIN catalog_canonical_value_page_seals s "
                "ON s.page_sha256 = c.page_sha256 "
                "WHERE c.value_sha256 = %s AND c.level = %s "
                "AND c.page_position = %s",
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
                "SELECT channel FROM catalog_source_build_channel WHERE build_id = %s",
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


def test_canonical_family_queries_keep_mariadb_placeholders_and_narrow_tables() -> None:
    class RecordingConnector:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            return ()

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.queries.append((query, data))
            return []

    connector = RecordingConnector()
    digest = b"d" * 32
    assert load_allocation_family(connector, value_sha256=digest) is None
    assert load_page_family(connector, page_sha256=digest) is None
    assert load_sealed_value_identity(connector, value_sha256=digest) is None
    assert len(connector.queries) == 3
    for query, parameters in connector.queries:
        assert "?" not in query
        assert query.count("%s") == len(parameters)
        assert "catalog_canonical_value_allocations" not in query
        assert "catalog_canonical_value_pages " not in query
        assert "catalog_canonical_value_page_descriptors" not in query


def test_mariadb_source_scope_registry_replay_uses_plain_wide_reads() -> None:
    root = b"r" * 32
    scope = source_scope_key("filesystem", root, 1)

    class RecordingConnector:
        def __init__(self) -> None:
            self.selects: list[str] = []
            self.mutations: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.selects.append(query)
            if "FROM catalog_source_scopes WHERE scope_key" in query:
                assert data == (scope,)
                return (scope, b"filesystem", root, 1)
            if "SELECT scope_key FROM catalog_source_scopes" in query:
                assert data == (b"filesystem", root, 1)
                return (scope,)
            raise AssertionError((query, data))

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            del data
            self.mutations.append(query)

    connector = RecordingConnector()
    replay = ensure_source_scope(
        connector,  # type: ignore[arg-type]
        source_provider=b"filesystem",
        source_root_sha256=root,
        identity_policy_version=1,
    )
    assert replay.replayed and replay.record.scope_key == scope
    assert not connector.mutations
    assert len(connector.selects) == 2
    assert all(" FOR UPDATE" not in query.upper() for query in connector.selects)
    assert all("%s" in query and "?" not in query for query in connector.selects)
    assert all("catalog_source_scopes" in query for query in connector.selects)


def test_source_scope_inserts_atomically_and_natural_collision_is_not_repaired(
    tmp_path: Path,
) -> None:
    root = b"r" * 32
    scope = source_scope_key("filesystem", root, 1)
    fresh = _generated_database(tmp_path / "fresh-scope.sqlite3")
    try:
        fresh.execute("PRAGMA foreign_keys = OFF")
        with patch.object(fresh, "execute", wraps=fresh.execute) as execute:
            inserted = ensure_source_scope(
                fresh,
                source_provider=b"filesystem",
                source_root_sha256=root,
                identity_policy_version=1,
            )
        fresh.execute("PRAGMA foreign_keys = ON")
        assert not inserted.replayed and inserted.record.scope_key == scope
        mutations = [
            call.args[0]
            for call in execute.call_args_list
            if call.args[0].startswith("INSERT INTO")
        ]
        assert len(mutations) == 1
        assert "catalog_source_scopes" in mutations[0]
        assert fresh.fetch_one("SELECT COUNT(*) FROM catalog_source_scopes") == (1,)
    finally:
        fresh.close()

    collision = _generated_database(tmp_path / "scope-collision.sqlite3")
    try:
        collision.execute("PRAGMA foreign_keys = OFF")
        collision.execute(
            "INSERT INTO catalog_source_scopes "
            "(scope_key, source_provider, source_root_sha256, "
            "identity_policy_version) VALUES (%s, %s, %s, %s)",
            (b"x" * 32, b"filesystem", root, 1),
        )
        collision.execute("PRAGMA foreign_keys = ON")
        with patch.object(collision, "execute", wraps=collision.execute) as execute:
            with pytest.raises(CatalogRegistryConflictError, match="natural"):
                ensure_source_scope(
                    collision,
                    source_provider=b"filesystem",
                    source_root_sha256=root,
                    identity_policy_version=1,
                )
        execute.assert_not_called()
        assert collision.fetch_one("SELECT COUNT(*) FROM catalog_source_scopes") == (1,)
    finally:
        collision.close()


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
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_canonical_value_page_payloads")
            == []
        )

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
    command = SourceRootBuildCommand(
        ("Volumes", "資料 A"),
        SourceBuildManifestSummary.empty(),
    )
    plan = command.prepare_root_upload()
    expected_scope = source_scope_key(
        "filesystem",
        command.source_root_sha256,
        1,
    )
    expected_build_id = source_build_identity(
        snapshot_attempt_id=command.build_attempt_id,
        scope=expected_scope,
        manifest_policy_id=1,
    )
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
        assert result.build_id == expected_build_id
        assert result.build_id != command.build_attempt_id
        assert result.scope_key == expected_scope
        assert not result.replayed
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (expected_build_id,)
        assert (
            connector.fetch_all(
                "SELECT generation FROM operational_canonical_value_uploads "
                "WHERE value_sha256 = %s",
                (plan.value_sha256,),
            )
            == []
        )

        # After a lost response, the same frozen snapshot deterministically
        # derives the same attempt capability and returns its scope- and
        # policy-bound durable build.
        retry_command = SourceRootBuildCommand(
            command.source_root_components,
            command.manifest_summary,
        )
        # A whole-request retry may have recreated the same generation/root
        # upload claim before discovering that the generation was mapped.
        _put_plan(connector, gate, turn, plan)
        original_execute = connector.execute

        def reject_source_scope_mutation(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> None:
            if query.startswith("INSERT INTO catalog_source_scopes"):
                raise AssertionError("immutable source-scope replay attempted DML")
            original_execute(query, data)

        def reject_database_clock(_work: VNextUnitOfWork) -> int:
            raise AssertionError("source-build replay queried the database clock")

        with monkeypatch.context() as patch:
            patch.setattr(connector, "execute", reject_source_scope_mutation)
            patch.setattr(
                source_build_module,
                "database_unix_microseconds",
                reject_database_clock,
            )
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
        assert replay.build_id == expected_build_id
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
        ) == (expected_build_id,)
    finally:
        plan.close()
        connector.close()


def test_absolute_slash_source_root_handoff(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "slash-root.sqlite3")
    command = SourceRootBuildCommand((), SourceBuildManifestSummary.empty())
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
        scope = load_source_scope(connector, handoff.scope_key)
        assert (
            scope.source_provider,
            scope.source_root_sha256,
            scope.identity_policy_version,
        ) == (b"filesystem", plan.value_sha256, 1)
    finally:
        plan.close()
        connector.close()


def test_cross_scope_build_attempt_conflict_rolls_back_and_retains_claim(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "cross-scope.sqlite3")
    summary = SourceBuildManifestSummary.empty()
    old_command = SourceRootBuildCommand(("old-root",), summary)
    new_command = SourceRootBuildCommand(("new-root",), summary)
    build_id = new_command.build_attempt_id
    old_plan = old_command.prepare_root_upload()
    new_plan = new_command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, old_plan)
        _put_plan(connector, gate, turn, new_plan)
        old_scope = seed_source_scope(
            connector,
            source_root_sha256=old_plan.value_sha256,
        ).scope_key
        seed_source_build(
            connector,
            build_id=build_id,
            scope_key=old_scope,
            state="OPEN",
            created_at=60,
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
    command = SourceRootBuildCommand(
        ("fault-root",),
        SourceBuildManifestSummary.empty(),
    )
    plan = command.prepare_root_upload()
    try:
        gate, turn = _authorities(connector)
        _put_plan(connector, gate, turn, plan)
        fault_points = (
            ("execute", "INSERT INTO catalog_source_scopes"),
            ("execute", "INSERT INTO catalog_source_build_descriptor"),
            ("execute", "INSERT INTO catalog_source_build_states"),
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
                "SELECT COUNT(*) FROM operational_source_build_discovery_checkpoints"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_source_build_assembly_checkpoints"
            ) == (0,)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (turn.generation, plan.value_sha256),
            ) == (1,)
    finally:
        plan.close()
        connector.close()


def test_canonical_family_statement_faults_roll_back_before_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "canonical-family-faults.sqlite3")
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        (b"\x00\x00\x00\x01\x00\x00\x00\x00",),
    )
    allocation_tables = (
        "catalog_canonical_value_allocation_anchors",
        "catalog_canonical_value_allocation_digest_domains",
        "catalog_canonical_value_allocation_byte_counts",
        "catalog_canonical_value_allocation_allocated_ats",
        "catalog_canonical_value_allocation_seals",
    )
    page_tables = (
        "catalog_canonical_value_page_anchors",
        "catalog_canonical_value_page_payloads",
        "catalog_canonical_value_page_coordinates",
        "catalog_canonical_value_page_subtree_item_counts",
        "catalog_canonical_value_page_seals",
    )
    try:
        gate, turn = _authorities(connector)
        for table in allocation_tables:
            with monkeypatch.context() as patch:
                original_execute = connector.execute

                def fail_allocation_member(
                    query: str,
                    data: tuple[Any, ...] = (),
                    *,
                    _fragment: str = f"INSERT INTO {table}",
                    _original: Any = original_execute,
                ) -> None:
                    if _fragment in query:
                        raise RuntimeError("injected allocation family fault")
                    _original(query, data)

                patch.setattr(connector, "execute", fail_allocation_member)
                with pytest.raises(
                    RuntimeError,
                    match="injected allocation family fault",
                ):
                    with connector.transaction():
                        CanonicalValueRepository.allocate(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            plan=plan,
                            now=20,
                        )
            for family_table in allocation_tables:
                assert connector.fetch_one(f"SELECT COUNT(*) FROM {family_table}") == (
                    0,
                )
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_canonical_value_uploads"
            ) == (0,)

        with connector.transaction():
            CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                now=20,
            )
        prepared = next(plan.iter_pages())
        for table in page_tables:
            with monkeypatch.context() as patch:
                original_execute = connector.execute

                def fail_page_member(
                    query: str,
                    data: tuple[Any, ...] = (),
                    *,
                    _fragment: str = f"INSERT INTO {table}",
                    _original: Any = original_execute,
                ) -> None:
                    if _fragment in query:
                        raise RuntimeError("injected page family fault")
                    _original(query, data)

                patch.setattr(connector, "execute", fail_page_member)
                with pytest.raises(RuntimeError, match="injected page family fault"):
                    with connector.transaction():
                        CanonicalValueRepository.put_page(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            plan=plan,
                            prepared_page=prepared,
                            now=21,
                        )
            for family_table in page_tables:
                assert connector.fetch_one(f"SELECT COUNT(*) FROM {family_table}") == (
                    0,
                )
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

        seed_canonical_allocation(
            connector,
            value_sha256=plan.value_sha256,
            digest_domain=b"source_title_utf8_v1",
            byte_count=plan.byte_count,
            allocated_at=19,
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
        for table in (
            "catalog_canonical_value_allocation_seals",
            "catalog_canonical_value_allocation_allocated_ats",
            "catalog_canonical_value_allocation_byte_counts",
            "catalog_canonical_value_allocation_digest_domains",
            "catalog_canonical_value_allocation_anchors",
        ):
            connector.execute(
                f"DELETE FROM {table} WHERE value_sha256 = %s",
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
            "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
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


@pytest.mark.parametrize(
    "missing_table",
    (
        "catalog_canonical_value_allocation_anchors",
        "catalog_canonical_value_allocation_digest_domains",
        "catalog_canonical_value_allocation_byte_counts",
        "catalog_canonical_value_allocation_allocated_ats",
        "catalog_canonical_value_allocation_seals",
    ),
)
def test_allocation_replay_rejects_each_partial_family_member(
    tmp_path: Path,
    missing_table: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"partial-allocation-{missing_table}.sqlite3"
    )
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        (b"\x00\x00\x00\x01\x00\x00\x00\x00",),
    )
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            first = CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                now=20,
            )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            f"DELETE FROM {missing_table} WHERE value_sha256 = %s",
            (plan.value_sha256,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(CanonicalValuePartialFamilyError):
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    now=99,
                )
        assert first.allocated_at == 20
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (turn.generation, plan.value_sha256),
        ) == (1,)
    finally:
        plan.close()
        connector.close()


@pytest.mark.parametrize(
    "missing_table",
    (
        "catalog_canonical_value_page_anchors",
        "catalog_canonical_value_page_payloads",
        "catalog_canonical_value_page_coordinates",
        "catalog_canonical_value_page_subtree_item_counts",
        "catalog_canonical_value_page_seals",
    ),
)
def test_page_replay_rejects_each_partial_family_member(
    tmp_path: Path,
    missing_table: str,
) -> None:
    connector = _generated_database(tmp_path / f"partial-page-{missing_table}.sqlite3")
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        (b"\x00\x00\x00\x01\x00\x00\x00\x00",),
    )
    try:
        gate, turn = _authorities(connector)
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
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            f"DELETE FROM {missing_table} WHERE page_sha256 = %s",
            (prepared.page_sha256,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(CanonicalValuePartialFamilyError):
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


def test_allocation_replay_preserves_first_time_and_branch_edges_are_exact(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "canonical-exact-replay.sqlite3")
    plan = CanonicalValueUploadPlan.from_parts(
        SOURCE_ROOT_DIGEST_DOMAIN,
        iter_source_root_payload(("x" * 255,) * 140),
    )
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            first = CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                now=20,
            )
        with connector.transaction():
            replay = CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                now=99,
            )
        assert first.allocated_at == replay.allocated_at == 20
        assert connector.fetch_one(
            "SELECT allocated_at FROM "
            "catalog_canonical_value_allocation_allocated_ats "
            "WHERE value_sha256 = %s",
            (plan.value_sha256,),
        ) == (20,)

        prepared_pages = list(plan.iter_pages())
        for prepared in prepared_pages:
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=prepared,
                    now=21,
                )
        branch = next(
            prepared
            for prepared in prepared_pages
            if decode_canonical_value_page(prepared.page_bytes).node_kind
            is GalleryObservationNodeKind.BRANCH
        )
        extra_page_sha256 = b"x" * 32
        seed_canonical_page(
            connector,
            page_sha256=extra_page_sha256,
            value_sha256=plan.value_sha256,
            page_bytes=b"x",
            level=8,
            page_position=999,
            subtree_item_count=1,
        )
        connector.execute(
            "INSERT INTO catalog_canonical_value_page_parents "
            "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
            (branch.page_sha256, 255, extra_page_sha256),
        )
        with pytest.raises(CanonicalValueCollisionError):
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=branch,
                    now=22,
                )
        connector.execute(
            "DELETE FROM catalog_canonical_value_page_parents "
            "WHERE parent_sha256 = %s AND position = 255",
            (branch.page_sha256,),
        )
        connector.execute(
            "DELETE FROM catalog_canonical_value_page_parents "
            "WHERE parent_sha256 = %s AND position = 0",
            (branch.page_sha256,),
        )
        with pytest.raises(CanonicalValueCollisionError):
            with connector.transaction():
                CanonicalValueRepository.put_page(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=plan,
                    prepared_page=branch,
                    now=23,
                )
    finally:
        plan.close()
        connector.close()
