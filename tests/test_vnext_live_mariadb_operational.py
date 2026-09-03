from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity
from vnext_catalog_registry_fixtures import (
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_manifest_fixtures import seed_snapshot_manifest, seed_source_build
from vnext_publication_fixtures import seed_publication_finalization_checkpoint

from h2hdb import CoreConfig
from h2hdb import vnext_identity as identity
from h2hdb import vnext_source_build_repository as source_build_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import DatabaseDuplicateKeyError
from h2hdb.vnext_allocator_repository import (
    IdentityStream,
    RevisionStream,
    VNextAllocatorRepository,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_gallery_staging_budget import (
    lock_gallery_staging_request_budget,
    release_gallery_staging_request_budget,
    reserve_gallery_staging_request_budget,
)
from h2hdb.vnext_hash_cache_repository import (
    FileHashCacheConflictError,
    FileHashObservationPlan,
    VNextHashCacheRepository,
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
from h2hdb.vnext_operational_event_repository import (
    DeletionConsumption,
    OperationalEffectRepository,
    RemovedGid,
)
from h2hdb.vnext_queue_repository import (
    QueueIdentityConflictError,
    VNextQueueRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    SourceBuildRepository,
    SourceDiscoveryPlan,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import (
    LockRank,
    VNextUnitOfWork,
    encode_lock_key,
)

_ARTIFACT_ADAPTER_ID = b"test-artifact-adapter"
_ARTIFACT_POLICY_FINGERPRINT = b"p" * 32


class _LiveMariaDBConnector(MariaDBConnector):
    """Real connector that records only the row-lock SQL it actually executes."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        super().__init__(host, port, user, password, database)
        self.for_update_queries: list[str] = []

    def fetch_one(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        if query.rstrip().upper().endswith(" FOR UPDATE"):
            self.for_update_queries.append(query)
        return super().fetch_one(query, data)


def _connector(
    config: CoreConfig,
    *,
    traced: bool = False,
) -> MariaDBConnector:
    database = config.database
    connector_type = _LiveMariaDBConnector if traced else MariaDBConnector
    connector = connector_type(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )
    connector.connect()
    return connector


@pytest.fixture
def generated_mariadb(
    mariadb_config: CoreConfig,
) -> Any:
    connector = _connector(mariadb_config, traced=True)
    payload: Any = ARTIFACT["backends"]
    payload = payload["mariadb"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])

    # Database-owned immutable/configuration facts needed by the writer graph.
    seed_manifest_policy(connector)
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 64),
    )
    try:
        yield connector
    finally:
        connector.close()


def _work(connector: MariaDBConnector) -> VNextUnitOfWork:
    return VNextUnitOfWork(connector, backend="mariadb")


def _read_one(
    connector: MariaDBConnector,
    query: str,
    data: tuple[Any, ...] = (),
) -> tuple[Any, ...]:
    with connector.read_transaction():
        return connector.fetch_one(query, data)


def _read_all(
    connector: MariaDBConnector,
    query: str,
    data: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]:
    with connector.read_transaction():
        return connector.fetch_all(query, data)


def _ingest_snapshot(connector: MariaDBConnector) -> tuple[object, ...]:
    with connector.read_transaction():
        return (
            connector.fetch_all(
                "SELECT generation, started_at, completed_at "
                "FROM operational_ingest_generations ORDER BY generation"
            ),
            connector.fetch_all(
                "SELECT singleton_id, current_generation, completed_generation, phase, "
                "last_transition_at FROM operational_ingest_coordination_heads"
            ),
            connector.fetch_all(
                "SELECT generation, owner_token, claimed_at, lease_expires_at "
                "FROM operational_ingest_generation_owners ORDER BY generation"
            ),
        )


def _gate_snapshot(connector: MariaDBConnector) -> tuple[object, ...]:
    with connector.read_transaction():
        return (
            connector.fetch_all(
                "SELECT gate_generation, mode, created_at "
                "FROM operational_maintenance_gate_generations "
                "ORDER BY gate_generation"
            ),
            connector.fetch_all(
                "SELECT singleton_id, gate_generation, updated_at "
                "FROM operational_maintenance_gate_heads"
            ),
            connector.fetch_all(
                "SELECT owner_token, gate_generation, lease_expires_at "
                "FROM operational_maintenance_gate_owners ORDER BY owner_token"
            ),
            connector.fetch_all(
                "SELECT slot, owner_token FROM operational_maintenance_gate_holders "
                "ORDER BY slot"
            ),
        )


def _claim_shared(
    connector: MariaDBConnector,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> GateLease:
    with patch(
        "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
        return_value=token,
    ):
        with connector.transaction():
            return MaintenanceGateRepository.claim_shared(
                _work(connector),
                now=now,
                lease_duration=duration,
            )


def _claim_exclusive(
    connector: MariaDBConnector,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> GateLease:
    with patch(
        "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
        return_value=token,
    ):
        with connector.transaction():
            return MaintenanceGateRepository.claim_exclusive(
                _work(connector),
                now=now,
                lease_duration=duration,
            )


def _upload(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            _work(connector),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                _work(connector),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            _work(connector),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def _allocate_catalog_concurrently(
    config: CoreConfig,
    barrier: Barrier,
    updated_at: int,
) -> int:
    connector = _connector(config)
    try:
        barrier.wait(timeout=10)
        with connector.transaction():
            return VNextAllocatorRepository.allocate_revision(
                _work(connector),
                RevisionStream.CATALOG,
                updated_at=updated_at,
            )
    finally:
        connector.close()


def _seed_live_canonical_value(
    connector: MariaDBConnector,
    *,
    digest_domain: str,
    payload: bytes,
    now: int,
) -> bytes:
    value_sha256 = identity.canonical_value_digest(digest_domain, payload)
    tree = identity.build_canonical_value_tree(
        value_sha256,
        len(payload),
        (payload,),
    )
    assert len(tree.pages) == 1
    page = tree.pages[0]
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=digest_domain.encode("ascii"),
        page_sha256=page.page_sha256,
        page_bytes=page.page_bytes,
        subtree_item_count=len(payload),
        allocated_at=now,
    )
    return value_sha256


def _reserve_live_staging_request(
    config: CoreConfig,
    barrier: Barrier,
    *,
    staging_id: bytes,
    request_sha256: bytes,
) -> str:
    connector = _connector(config)
    try:
        barrier.wait(timeout=10)
        with connector.transaction():
            work = _work(connector)
            reserve_gallery_staging_request_budget(work)
            row = work.lock_row(
                LockRank.CHILD,
                encode_lock_key("gallery-staging-request", request_sha256),
                "SELECT staging_id FROM "
                "operational_gallery_observation_staging_requests "
                "WHERE request_sha256 = %s",
                (request_sha256,),
            )
            assert row == ()
            connector.execute(
                "INSERT INTO operational_gallery_observation_staging_requests "
                "(request_sha256, staging_id) VALUES (%s, %s)",
                (request_sha256, staging_id),
            )
        return "reserve"
    finally:
        connector.close()


def _release_live_staging_request(
    config: CoreConfig,
    barrier: Barrier,
    *,
    staging_id: bytes,
    request_sha256: bytes,
) -> str:
    connector = _connector(config)
    try:
        barrier.wait(timeout=10)
        with connector.transaction():
            work = _work(connector)
            retained = lock_gallery_staging_request_budget(work)
            row = work.lock_row(
                LockRank.CHILD,
                encode_lock_key("gallery-staging-request", request_sha256),
                "SELECT staging_id FROM "
                "operational_gallery_observation_staging_requests "
                "WHERE request_sha256 = %s",
                (request_sha256,),
            )
            assert row == (staging_id,)
            assert (
                connector.execute_affected(
                    "DELETE FROM operational_gallery_observation_staging_requests "
                    "WHERE request_sha256 = %s",
                    (request_sha256,),
                )
                == 1
            )
            release_gallery_staging_request_budget(
                work,
                retained_request_count=retained,
                deleted_count=1,
            )
        return "release"
    finally:
        connector.close()


def test_live_mariadb_gallery_staging_budget_and_retiring_slot_serialize(
    generated_mariadb: _LiveMariaDBConnector,
    mariadb_config: CoreConfig,
) -> None:
    connector = generated_mariadb
    root_sha256 = _seed_live_canonical_value(
        connector,
        digest_domain="source_root_v1",
        payload=b"root",
        now=10,
    )
    scope_key = seed_source_scope(
        connector,
        source_root_sha256=root_sha256,
    ).scope_key
    build_id = b"b" * 16
    seed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
        state="OPEN",
        created_at=11,
    )
    for gallery_id, name in ((1, "gallery-one"), (2, "gallery-two")):
        locator_payload = identity.encode_source_relative_locator((name,))
        locator_sha256 = _seed_live_canonical_value(
            connector,
            digest_domain="source_relative_locator_v1",
            payload=locator_payload,
            now=11 + gallery_id,
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator_sha256, name.encode("ascii")),
        )
        seed_gallery_identity(
            connector,
            gallery_id=gallery_id,
            gallery_key=identity.gallery_key(scope_key, locator_sha256),
            scope_key=scope_key,
            locator_sha256=locator_sha256,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, 1, 20)",
            (gallery_id,),
        )
    connector.commit()

    staging_id = b"s" * 16
    with connector.transaction():
        connector.execute(
            "INSERT INTO operational_gallery_observation_stagings "
            "(staging_id, build_id, gallery_id, observation_id, state, "
            "created_at, sealed_at, terminal_byte_count) "
            "VALUES (%s, %s, 1, 1, %s, 30, 31, 0)",
            (staging_id, build_id, "RETIRING_SEALED"),
        )
    with pytest.raises(DatabaseDuplicateKeyError):
        with connector.transaction():
            connector.execute(
                "INSERT INTO operational_gallery_observation_stagings "
                "(staging_id, build_id, gallery_id, observation_id, state, "
                "created_at, sealed_at, terminal_byte_count) "
                "VALUES (%s, %s, 2, 1, %s, 32, 33, 0)",
                (b"t" * 16, build_id, "RETIRING_REUSED"),
            )

    retired_request = b"r" * 32
    with connector.transaction():
        connector.execute(
            "INSERT INTO operational_gallery_observation_staging_requests "
            "(request_sha256, staging_id) VALUES (%s, %s)",
            (retired_request, staging_id),
        )
        connector.execute(
            "UPDATE operational_gallery_observation_staging_request_budgets "
            "SET retained_request_count = 1 WHERE singleton_id = 1"
        )

    inserted_request = b"i" * 32
    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        reserve = pool.submit(
            _reserve_live_staging_request,
            mariadb_config,
            barrier,
            staging_id=staging_id,
            request_sha256=inserted_request,
        )
        release = pool.submit(
            _release_live_staging_request,
            mariadb_config,
            barrier,
            staging_id=staging_id,
            request_sha256=retired_request,
        )
        assert {reserve.result(timeout=20), release.result(timeout=20)} == {
            "reserve",
            "release",
        }
    assert _read_one(
        connector,
        "SELECT retained_request_count FROM "
        "operational_gallery_observation_staging_request_budgets "
        "WHERE singleton_id = 1",
    ) == (1,)
    assert _read_all(
        connector,
        "SELECT request_sha256 FROM "
        "operational_gallery_observation_staging_requests "
        "ORDER BY request_sha256",
    ) == [(inserted_request,)]

    before_rollback = _read_all(
        connector,
        "SELECT request_sha256 FROM "
        "operational_gallery_observation_staging_requests "
        "ORDER BY request_sha256",
    )
    with pytest.raises(RuntimeError, match="budget rollback"):
        with connector.transaction():
            work = _work(connector)
            reserve_gallery_staging_request_budget(work)
            connector.execute(
                "INSERT INTO operational_gallery_observation_staging_requests "
                "(request_sha256, staging_id) VALUES (%s, %s)",
                (b"z" * 32, staging_id),
            )
            raise RuntimeError("budget rollback")
    assert (
        _read_all(
            connector,
            "SELECT request_sha256 FROM "
            "operational_gallery_observation_staging_requests "
            "ORDER BY request_sha256",
        )
        == before_rollback
    )
    assert _read_one(
        connector,
        "SELECT retained_request_count FROM "
        "operational_gallery_observation_staging_request_budgets "
        "WHERE singleton_id = 1",
    ) == (1,)


def test_live_mariadb_cleanup_frozen_root_set_and_rollback(
    generated_mariadb: _LiveMariaDBConnector,
) -> None:
    connector = generated_mariadb
    source_identity = _seed_live_canonical_value(
        connector,
        digest_domain="filesystem_source_identity_v1",
        payload=b"frozen-root-source",
        now=10,
    )
    fingerprint = _seed_live_canonical_value(
        connector,
        digest_domain="filesystem_fingerprint_v1",
        payload=b"frozen-root-fingerprint",
        now=11,
    )
    file_sha256 = b"f" * 32
    with connector.transaction():
        connector.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, 7)",
            (file_sha256,),
        )
        connector.execute(
            "INSERT INTO operational_hash_cache_observations "
            "(source_identity_sha256, fingerprint_sha256, observed_at) "
            "VALUES (%s, %s, 12)",
            (source_identity, fingerprint),
        )
        connector.execute(
            "INSERT INTO operational_file_hash_caches "
            "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
            "VALUES (%s, %s, %s, 13)",
            (source_identity, fingerprint, file_sha256),
        )

    gate = _claim_exclusive(
        connector,
        b"cleanup-frozen01",
        now=20,
        duration=100,
    )
    with connector.transaction():
        cycle = VNextCleanupRepository.begin_cycle(
            _work(connector),
            gate_lease=gate,
            target_kind=CleanupTargetKind.HASH_CACHE_OBSERVATION,
            shard_no=source_identity[0],
            cycle_cutoff_at=100,
            max_rows_per_transaction=1,
            hash_cache_max_age_microseconds=0,
            now=21,
        )
    assert _read_one(
        connector,
        "SELECT frozen_root_count FROM operational_cleanup_jobs WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    ) == (1,)
    assert _read_one(
        connector,
        "SELECT COUNT(*) FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    ) == (1,)

    def advance(batch_key: bytes, generation: int, now: int) -> Any:
        with connector.transaction():
            return VNextCleanupRepository.advance(
                _work(connector),
                gate_lease=gate,
                cycle=cycle,
                command=CleanupBatchCommand(batch_key, generation),
                now=now,
            )

    first = advance(b"1" * 32, 1, 22)
    assert first.phase == "HC_FILE" and first.generation == 2
    second = advance(b"2" * 32, 2, 23)
    assert second.phase == "HC_ROOT" and second.generation == 1
    third = advance(b"3" * 32, 1, 24)
    assert third.phase == "HC_ROOT" and third.generation == 2

    command = CleanupBatchCommand(b"4" * 32, 2)
    with pytest.raises(RuntimeError, match="abort frozen completion"):
        with connector.transaction():
            completed = VNextCleanupRepository.advance(
                _work(connector),
                gate_lease=gate,
                cycle=cycle,
                command=command,
                now=25,
            )
            assert completed.cycle_complete
            raise RuntimeError("abort frozen completion")
    assert _read_one(
        connector,
        "SELECT state FROM operational_cleanup_jobs WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    ) == ("OPEN",)
    assert _read_one(
        connector,
        "SELECT COUNT(*) FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    ) == (1,)
    assert _read_one(
        connector,
        "SELECT final_chain_sha256, final_deleted_count "
        "FROM operational_cleanup_jobs WHERE cleanup_id = %s",
        (cycle.cleanup_id,),
    ) == (None, None)

    committed = advance(b"4" * 32, 2, 26)
    assert committed.cycle_complete and not committed.replayed
    replayed = advance(b"4" * 32, 2, 27)
    assert replayed.cycle_complete and replayed.replayed
    assert (
        _read_one(
            connector,
            "SELECT 1 FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        )
        == ()
    )


def test_live_mariadb_canonical_cleanup_retains_contributor_facet_value(
    generated_mariadb: _LiveMariaDBConnector,
) -> None:
    connector = generated_mariadb
    with connector.transaction():
        contributor = _seed_live_canonical_value(
            connector,
            digest_domain="contributor_name_utf8_v1",
            payload=b"retained contributor",
            now=10,
        )
        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (99, 1, 0)"
        )
        connector.execute(
            "INSERT INTO catalog_contributor_facet_order "
            "(revision, position, contributor_name_sha256, role, occurrence_count) "
            "VALUES (99, 0, %s, %s, 1)",
            (contributor, b"author"),
        )

    gate = _claim_exclusive(
        connector,
        b"facet-retention1",
        now=20,
        duration=10_000,
    )

    def drain(cycle: Any, *, now: int) -> Any:
        generation = 1
        for attempt in range(64):
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    _work(connector),
                    gate_lease=gate,
                    cycle=cycle,
                    command=CleanupBatchCommand(
                        attempt.to_bytes(32, "big"),
                        generation,
                    ),
                    now=now + attempt,
                )
            if result.cycle_complete:
                return result
            assert result.generation is not None
            generation = result.generation
        raise AssertionError("canonical cleanup did not terminate")

    with connector.transaction():
        retained_cycle = VNextCleanupRepository.begin_cycle(
            _work(connector),
            gate_lease=gate,
            target_kind=CleanupTargetKind.CANONICAL_VALUE,
            shard_no=contributor[0],
            cycle_cutoff_at=100,
            max_rows_per_transaction=32,
            now=21,
        )
    retained = drain(retained_cycle, now=22)
    assert retained.deleted_count == 0
    assert _read_one(
        connector,
        "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
        "WHERE value_sha256 = %s",
        (contributor,),
    ) == (1,)

    with connector.transaction():
        connector.execute(
            "DELETE FROM catalog_contributor_facet_order WHERE revision = 99"
        )
        released_cycle = VNextCleanupRepository.begin_cycle(
            _work(connector),
            gate_lease=gate,
            target_kind=CleanupTargetKind.CANONICAL_VALUE,
            shard_no=contributor[0],
            cycle_cutoff_at=200,
            max_rows_per_transaction=32,
            now=100,
        )
    released = drain(released_cycle, now=101)
    assert released.cycle_complete
    assert (
        _read_one(
            connector,
            "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
            "WHERE value_sha256 = %s",
            (contributor,),
        )
        == ()
    )


def test_live_mariadb_operational_writer_workflows(
    generated_mariadb: _LiveMariaDBConnector,
    mariadb_config: CoreConfig,
) -> None:
    connector = generated_mariadb

    # Ingest authority: exact response replay, live contention, renewal fencing,
    # expired takeover, stale zero-write rejection, and quiescent completion.
    with connector.transaction():
        first_turn = IngestFenceRepository.claim(
            _work(connector),
            owner_token=b"ingest-owner-001",
            now=100,
            lease_duration=20,
        )
    with connector.transaction():
        assert (
            IngestFenceRepository.claim(
                _work(connector),
                owner_token=b"ingest-owner-001",
                now=101,
                lease_duration=999,
            )
            == first_turn
        )
    before_ingest_contention = _ingest_snapshot(connector)
    with pytest.raises(IngestFenceUnavailableError, match="live lease"):
        with connector.transaction():
            IngestFenceRepository.claim(
                _work(connector),
                owner_token=b"ingest-owner-002",
                now=102,
                lease_duration=20,
            )
    assert _ingest_snapshot(connector) == before_ingest_contention

    with connector.transaction():
        renewed_turn = IngestFenceRepository.renew(
            _work(connector),
            first_turn,
            now=110,
            lease_duration=30,
        )
    assert renewed_turn.lease_expires_at == 140
    with pytest.raises(IngestFenceUnavailableError, match="stale"):
        with connector.transaction():
            IngestFenceRepository.lock_and_require_live(
                _work(connector), first_turn, now=111
            )
    with connector.transaction():
        takeover_turn = IngestFenceRepository.claim(
            _work(connector),
            owner_token=b"ingest-owner-002",
            now=140,
            lease_duration=40,
        )
    assert (renewed_turn.generation, takeover_turn.generation) == (1, 2)
    before_stale_completion = _ingest_snapshot(connector)
    with pytest.raises(IngestFenceUnavailableError, match="stale"):
        with connector.transaction():
            IngestFenceRepository.complete(_work(connector), renewed_turn, now=141)
    assert _ingest_snapshot(connector) == before_stale_completion
    with connector.transaction():
        IngestFenceRepository.complete(_work(connector), takeover_turn, now=150)
    assert _read_one(
        connector,
        "SELECT current_generation, completed_generation, phase "
        "FROM operational_ingest_coordination_heads",
    ) == (2, 2, "READY")
    with connector.transaction():
        current_turn = IngestFenceRepository.claim(
            _work(connector),
            owner_token=b"ingest-owner-003",
            now=160,
            lease_duration=10_000,
        )

    # The fixed 64-slot gate is exercised through its writer API, including
    # the 65th-owner rejection and an expired SHARED -> EXCLUSIVE transition.
    shared_leases = tuple(
        _claim_shared(
            connector,
            slot.to_bytes(16, "big"),
            now=200,
            duration=20,
        )
        for slot in range(1, 65)
    )
    assert tuple(lease.slots for lease in shared_leases) == tuple(
        (slot,) for slot in range(64)
    )
    full_gate = _gate_snapshot(connector)
    with pytest.raises(MaintenanceGateUnavailableError, match="all 64"):
        _claim_shared(
            connector,
            (65).to_bytes(16, "big"),
            now=201,
            duration=20,
        )
    assert _gate_snapshot(connector) == full_gate

    with connector.transaction():
        MaintenanceGateRepository.release(_work(connector), shared_leases[0], now=202)
    exclusive = _claim_exclusive(
        connector,
        b"exclusive-owner1",
        now=220,
        duration=100,
    )
    assert exclusive.mode is GateMode.EXCLUSIVE
    assert exclusive.slots == tuple(range(64))
    assert _read_one(
        connector,
        "SELECT COUNT(*), MIN(slot), MAX(slot), COUNT(DISTINCT owner_token) "
        "FROM operational_maintenance_gate_holders",
    ) == (64, 0, 63, 1)
    before_exclusive_contention = _gate_snapshot(connector)
    with pytest.raises(MaintenanceGateUnavailableError, match="EXCLUSIVE"):
        _claim_shared(
            connector,
            b"blocked-shared01",
            now=221,
            duration=100,
        )
    assert _gate_snapshot(connector) == before_exclusive_contention
    with connector.transaction():
        MaintenanceGateRepository.release(_work(connector), exclusive, now=222)
    current_gate = _claim_shared(
        connector,
        b"current-gate-001",
        now=223,
        duration=10_000,
    )

    # All four allocator streams advance from generated seeds. A deliberate
    # exception demonstrates real MariaDB rollback, then two live connections
    # serialize on the CATALOG row and receive distinct consecutive revisions.
    with connector.transaction():
        source_revision = VNextAllocatorRepository.allocate_revision(
            _work(connector), RevisionStream.SOURCE, updated_at=300
        )
        catalog_revision = VNextAllocatorRepository.allocate_revision(
            _work(connector), RevisionStream.CATALOG, updated_at=301
        )
        gallery_id = VNextAllocatorRepository.allocate_identity(
            _work(connector), IdentityStream.GALLERY, updated_at=302
        )
        tag_id = VNextAllocatorRepository.allocate_identity(
            _work(connector), IdentityStream.TAG, updated_at=303
        )
    assert (source_revision, catalog_revision, gallery_id, tag_id) == (1, 1, 1, 1)
    before_fault = _read_one(
        connector,
        "SELECT next_revision, updated_at FROM operational_revision_allocators "
        "WHERE stream = %s",
        (RevisionStream.CATALOG.value,),
    )
    with pytest.raises(RuntimeError, match="allocator fault"):
        with connector.transaction():
            VNextAllocatorRepository.allocate_revision(
                _work(connector), RevisionStream.CATALOG, updated_at=304
            )
            raise RuntimeError("allocator fault after CAS")
    assert (
        _read_one(
            connector,
            "SELECT next_revision, updated_at FROM operational_revision_allocators "
            "WHERE stream = %s",
            (RevisionStream.CATALOG.value,),
        )
        == before_fault
    )

    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(
            pool.submit(
                _allocate_catalog_concurrently,
                mariadb_config,
                barrier,
                timestamp,
            )
            for timestamp in (305, 306)
        )
        concurrent_revisions = {future.result(timeout=20) for future in futures}
    assert concurrent_revisions == {2, 3}
    assert _read_one(
        connector,
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        (RevisionStream.CATALOG.value,),
    ) == (4,)

    # Deletion requests retain immutable attempt history while rotating the
    # per-GID head and monotonically advancing one global generation.
    deletion_facts = (
        (42, b"delete-request01", None, 400),
        (7, b"delete-request02", "", 401),
        (42, b"delete-request03", "replacement", 402),
    )
    receipts = []
    for gid, token, url, requested_at in deletion_facts:
        with connector.transaction():
            receipts.append(
                VNextQueueRepository.request_deletion(
                    _work(connector),
                    gid=gid,
                    request_token=token,
                    url=url,
                    requested_at=requested_at,
                )
            )
    assert [receipt.observed_generation for receipt in receipts] == [1, 2, 3]
    with connector.transaction():
        replayed_deletion = VNextQueueRepository.request_deletion(
            _work(connector),
            gid=42,
            request_token=b"delete-request03",
            url="replacement",
            requested_at=402,
        )
    assert not replayed_deletion.created
    assert replayed_deletion.current
    before_deletion_collision = _read_one(
        connector,
        "SELECT current_generation FROM operational_deletion_request_generation_heads",
    )
    with pytest.raises(QueueIdentityConflictError):
        with connector.transaction():
            VNextQueueRepository.request_deletion(
                _work(connector),
                gid=99,
                request_token=b"delete-request03",
                url="replacement",
                requested_at=402,
            )
    assert (
        _read_one(
            connector,
            "SELECT current_generation FROM operational_deletion_request_generation_heads",
        )
        == before_deletion_collision
    )
    assert _read_all(
        connector,
        "SELECT gid, request_token FROM operational_deletion_request_heads ORDER BY gid",
    ) == [(7, b"delete-request02"), (42, b"delete-request03")]
    assert _read_one(
        connector, "SELECT COUNT(*) FROM operational_deletion_request_attempts"
    ) == (3,)

    # Build and hash-cache authority is created only through production writer
    # APIs. The empty discovery is a valid exact source snapshot and keeps the
    # integration focused on the operational state machines.
    root_command = SourceRootBuildCommand((), SourceBuildManifestSummary.empty())
    with root_command.prepare_root_upload() as root_plan:
        _upload(connector, current_gate, current_turn, root_plan, now=501)
        with connector.transaction():
            source = SourceBuildRepository.handoff_root(
                _work(connector),
                gate_lease=current_gate,
                ingest_turn=current_turn,
                command=root_command,
                root_plan=root_plan,
                analysis_policy_id=1,
                now=504,
            )
    build_id = source.build_id

    source_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_source_identity_v1",
        (b"source-id-v1\0", b"/source/gallery/file.jpg"),
    )
    fingerprint_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_fingerprint_v1",
        (b"fingerprint-v1\0", b"stat-and-prefix"),
    )
    try:
        _upload(connector, current_gate, current_turn, source_plan, now=510)
        _upload(connector, current_gate, current_turn, fingerprint_plan, now=520)
        file_plan = FileHashObservationPlan.from_parts((b"file-", b"bytes"))
        with connector.transaction():
            cached = VNextHashCacheRepository.handoff(
                _work(connector),
                gate_lease=current_gate,
                ingest_turn=current_turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=file_plan,
                observed_at=530,
                cached_at=531,
                now=532,
            )
        assert not cached.replayed
        with connector.transaction():
            cache_replay = VNextHashCacheRepository.handoff(
                _work(connector),
                gate_lease=current_gate,
                ingest_turn=current_turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=file_plan,
                observed_at=530,
                cached_at=531,
                now=533,
            )
        assert cache_replay.replayed
        with connector.read_transaction():
            assert (
                VNextHashCacheRepository.lookup_exact(
                    _work(connector),
                    source_plan=source_plan,
                    fingerprint_plan=fingerprint_plan,
                )
                == cache_replay
            )
        with pytest.raises(FileHashCacheConflictError, match="exact tuple"):
            with connector.transaction():
                VNextHashCacheRepository.handoff(
                    _work(connector),
                    gate_lease=current_gate,
                    ingest_turn=current_turn,
                    source_plan=source_plan,
                    fingerprint_plan=fingerprint_plan,
                    file_plan=FileHashObservationPlan.from_parts((b"changed",)),
                    observed_at=530,
                    cached_at=531,
                    now=534,
                )
    finally:
        source_plan.close()
        fingerprint_plan.close()

    with SourceDiscoveryPlan.from_locators(()) as discovery_plan:
        with connector.read_transaction():
            discovery_batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=build_id,
                plan=discovery_plan,
            )
        assert discovery_batch.terminal
        with connector.transaction():
            discovery_receipt = SourceBuildRepository.commit_discovery_batch(
                _work(connector),
                gate_lease=current_gate,
                ingest_turn=current_turn,
                batch=discovery_batch,
                resolved=(),
                now=540,
            )
        assert discovery_receipt.terminal
    with connector.transaction():
        assembly_receipt = SourceBuildRepository.assemble_batch(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            build_id=build_id,
            attempt=SourceBuildRepository.issue_assembly_batch(),
            now=550,
        )
    assert assembly_receipt.terminal
    sealed_source = _read_one(
        connector,
        "SELECT state, created_at, sealed_at FROM catalog_source_builds "
        "WHERE build_id = %s",
        (build_id,),
    )
    assert sealed_source is not None
    assert sealed_source[0] == "SEALED"
    assert sealed_source[1] <= sealed_source[2]
    assert sealed_source[2] != 550

    # Only the immutable upstream publication fact is fixture SQL. Revision 1
    # itself came from the real SOURCE allocator above.
    # The root handoff above already established this digest as an exact,
    # sealed canonical identity, satisfying the snapshot manifest's FK.
    snapshot_manifest_sha256 = root_command.source_root_sha256
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot_manifest_sha256,
        gallery_count=0,
        file_count=0,
        byte_count=0,
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptors "
        "(source_revision, channel, snapshot_manifest_sha256) "
        "VALUES (%s, %s, %s)",
        (source_revision, b"default", snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptors "
        "(revision, publication_count, artifact_count) VALUES (%s, %s, %s)",
        (catalog_revision, 0, 0),
    )
    policy_payload = identity.encode_artifact_policy(
        2,
        _ARTIFACT_ADAPTER_ID,
        _ARTIFACT_POLICY_FINGERPRINT,
    )
    with CanonicalValueUploadPlan.from_parts(
        "artifact_policy_v3",
        (policy_payload,),
    ) as policy_plan:
        _upload(connector, current_gate, current_turn, policy_plan, now=551)
        policy_semantics = seed_artifact_policy_semantics(
            connector,
            artifact_algorithm_version=2,
            adapter_id=_ARTIFACT_ADAPTER_ID,
            policy_fingerprint_sha256=_ARTIFACT_POLICY_FINGERPRINT,
        )
        assert policy_semantics.policy_component_sha256 == policy_plan.value_sha256
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (1, policy_semantics.policy_component_sha256),
    )
    seed_title_sort_policy(connector, unicode_data_version=b"test-unicode")
    seed_display_title_policy(connector)
    connector.commit()

    deletion_token = b"effect-delete-01"
    with connector.transaction():
        deletion_for_effect = VNextQueueRepository.request_deletion(
            _work(connector),
            gid=22,
            request_token=deletion_token,
            url=None,
            requested_at=561,
        )
    with connector.transaction():
        preparation = OperationalEffectRepository.begin(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            build_id=build_id,
            operational_policy_id=1,
            now=570,
        )
    assert preparation.deletion_request_generation == (
        deletion_for_effect.observed_generation
    )
    with connector.transaction():
        begin_replay = OperationalEffectRepository.begin(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            build_id=build_id,
            operational_policy_id=1,
            now=571,
        )
    assert begin_replay.replayed
    assert begin_replay.prepared_at == preparation.prepared_at

    effects = (
        RemovedGid(11, b"removed-token-01"),
        DeletionConsumption(22, deletion_token),
    )
    with connector.transaction():
        effect_batch = OperationalEffectRepository.append_batch(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            preparation_id=preparation.preparation_id,
            effects=effects,
            now=580,
        )
    assert (effect_batch.start_sequence_no, effect_batch.next_sequence_no) == (0, 2)
    with connector.transaction():
        effect_replay = OperationalEffectRepository.append_batch(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            preparation_id=preparation.preparation_id,
            effects=effects,
            now=581,
        )
    assert effect_replay.replayed
    assert effect_replay.committed_at == effect_batch.committed_at
    with connector.transaction():
        terminal_batch = OperationalEffectRepository.append_batch(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            preparation_id=preparation.preparation_id,
            effects=(),
            now=590,
        )
    assert terminal_batch.terminal
    with connector.transaction():
        seal = OperationalEffectRepository.seal(
            _work(connector),
            gate_lease=current_gate,
            ingest_turn=current_turn,
            preparation_id=preparation.preparation_id,
            now=591,
        )
    assert seal.event_count == 2
    receipt_id = b"live-receipt-001"
    candidate_id = b"live-candidate01"
    with connector.transaction():
        connector.execute(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
            (1,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            (1, 0),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
            (receipt_id,),
        )
        seed_publication_finalization_checkpoint(
            connector,
            receipt_id=receipt_id,
            updated_at=600,
        )
        connector.execute(
            "INSERT INTO catalog_publication_commits "
            "(receipt_id, candidate_id, revision, source_revision, generation, "
            "preparation_id, operational_policy_id, artifact_policy_id, "
            "display_title_policy_id, new_galleries, changed_galleries, "
            "removed_galleries, duplicate_losers, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                receipt_id,
                candidate_id,
                catalog_revision,
                source_revision,
                1,
                preparation.preparation_id,
                1,
                1,
                1,
                0,
                0,
                0,
                0,
                600,
            ),
        )
    assert _read_one(
        connector,
        "SELECT candidate_id, revision, source_revision, generation, preparation_id, "
        "operational_policy_id, artifact_policy_id, display_title_policy_id, "
        "new_galleries, changed_galleries, removed_galleries, duplicate_losers, "
        "committed_at FROM catalog_publication_commits WHERE receipt_id = %s",
        (receipt_id,),
    ) == (
        candidate_id,
        catalog_revision,
        source_revision,
        1,
        preparation.preparation_id,
        1,
        1,
        1,
        0,
        0,
        0,
        0,
        600,
    )
    activation = _read_one(
        connector,
        "SELECT source_revision, preparation_id, operational_policy_id, "
        "committed_at FROM catalog_publication_commits "
        "WHERE source_revision = %s",
        (source_revision,),
    )
    assert activation == (source_revision, preparation.preparation_id, 1, 600)
    assert _read_all(
        connector,
        "SELECT sequence_no, event_type FROM operational_operational_events "
        "ORDER BY sequence_no",
    ) == [(0, "REMOVED_GID"), (1, "DELETION_CONSUMPTION")]
    assert not hasattr(OperationalEffectRepository, "acknowledge_through")

    # These were real MariaDB SELECT ... FOR UPDATE statements, not a recorder.
    locked_sql = "\n".join(connector.for_update_queries)
    for required_table in (
        "operational_ingest_coordination_heads",
        "operational_maintenance_gate_heads",
        "operational_revision_allocators",
        "operational_deletion_request_generation_heads",
        "operational_hash_cache_observations",
        "operational_operational_preparations",
    ):
        assert required_table in locked_sql
    assert all(
        query.rstrip().upper().endswith(" FOR UPDATE")
        for query in connector.for_update_queries
    )


_DRAIN_BUILD = b"b" * 16


def _seed_mariadb_drain_preparations(
    connector: MariaDBConnector,
    *,
    count: int,
    policy_id: int,
    current_generation: int,
) -> None:
    """Seed ``count`` unbound, uncommitted OPEN/COMPLETE preparations of the
    build, each under a distinct deletion generation, for bounded drainage.

    Foreign key checks are disabled only for this focused injection; the live
    physical matrix exercises FK integrity."""

    connector.execute("SET FOREIGN_KEY_CHECKS = 0")
    connector.execute(
        "INSERT INTO catalog_source_build_descriptor "
        "(build_id, scope_key, manifest_policy_id, created_at) "
        "VALUES (%s, %s, %s, %s)",
        (_DRAIN_BUILD, b"s" * 32, 1, 5),
    )
    connector.execute(
        "INSERT INTO operational_source_build_generations "
        "(build_id, generation) VALUES (%s, %s)",
        (_DRAIN_BUILD, current_generation),
    )
    connector.execute(
        "INSERT INTO operational_source_working_builds "
        "(slot, build_id, assigned_at) VALUES (1, %s, %s)",
        (_DRAIN_BUILD, 6),
    )
    with connector.transaction():
        for offset in range(count):
            generation = current_generation + 1 + offset
            preparation_id = f"prp{generation:013d}".encode()
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
            if offset % 2:
                state, completed = "COMPLETE", 6
            else:
                state, completed = "OPEN", None
            connector.execute(
                "INSERT INTO operational_operational_preparations "
                "(preparation_id, build_id, deletion_request_generation, "
                "operational_policy_id, state, prepared_at, completed_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    preparation_id,
                    _DRAIN_BUILD,
                    generation,
                    policy_id,
                    state,
                    4,
                    completed,
                ),
            )
    connector.execute("SET FOREIGN_KEY_CHECKS = 1")
    # SET opens an implicit transaction that no auto-commit statement
    # closes; commit it so the caller can start its own transactions.
    connector.commit()


def _mariadb_non_abandoned(connector: MariaDBConnector) -> int:
    return int(
        _read_one(
            connector,
            "SELECT COUNT(*) FROM operational_operational_preparations "
            "WHERE build_id = %s AND state IN ('OPEN', 'COMPLETE')",
            (_DRAIN_BUILD,),
        )[0]
    )


def test_live_mariadb_stale_build_preparation_drainage_is_bounded_and_replayable(
    generated_mariadb: _LiveMariaDBConnector,
) -> None:
    """The source-side stale-build preparation drainage abandons at most 128
    rows per transaction on live MariaDB, converges over 257 rows, rolls back
    an interrupted page, and replays a lost page response idempotently."""

    connector = generated_mariadb
    _seed_mariadb_drain_preparations(
        connector, count=257, policy_id=1, current_generation=0
    )
    # Interrupted page: nothing is abandoned.
    with pytest.raises(RuntimeError, match="interrupted"):
        with connector.transaction():
            source_build_module._abandon_build_preparations_page(
                _work(connector), build_id=_DRAIN_BUILD, now=100
            )
            raise RuntimeError("interrupted before commit")
    assert _mariadb_non_abandoned(connector) == 257

    # Bounded, keyset-paged drainage; the lost-response replay resumes.
    def stale_pending() -> bool:
        with connector.read_transaction():
            return source_build_module._build_superseded_preparations_pending(
                connector, build_id=_DRAIN_BUILD
            )

    pages: list[int] = []
    now = 200
    while stale_pending():
        with connector.transaction():
            pages.append(
                source_build_module._abandon_build_preparations_page(
                    _work(connector), build_id=_DRAIN_BUILD, now=now
                )
            )
        now += 1
    assert pages == [128, 128, 1]
    assert _mariadb_non_abandoned(connector) == 0


def test_live_mariadb_superseded_preparation_drainage_is_bounded_and_replayable(
    generated_mariadb: _LiveMariaDBConnector,
) -> None:
    """The operational superseded-preparation drainage abandons at most 128
    rows per authorized transaction on live MariaDB and converges over 129
    rows."""

    connector = generated_mariadb
    _seed_mariadb_drain_preparations(
        connector, count=129, policy_id=2, current_generation=0
    )
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (2, 1, 1, 63),
    )
    gate = _claim_shared(connector, b"gate-drain-00001", now=5, duration=1_000_000)
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            _work(connector),
            owner_token=b"ingest-drain-001",
            now=6,
            lease_duration=1_000_000,
        )
    # Map the live generation to the build so the writer authorizes.
    connector.execute(
        "UPDATE operational_source_build_generations SET generation = %s "
        "WHERE build_id = %s",
        (turn.generation, _DRAIN_BUILD),
    )

    def superseded_pending() -> bool:
        with connector.read_transaction():
            return OperationalEffectRepository.superseded_preparations_pending(
                _work(connector),
                build_id=_DRAIN_BUILD,
                policy_id=1,
                deletion_generation=0,
            )

    pages: list[int] = []
    now = 300
    while superseded_pending():
        with connector.transaction():
            pages.append(
                OperationalEffectRepository.abandon_superseded_preparations(
                    _work(connector),
                    gate_lease=gate,
                    ingest_turn=turn,
                    build_id=_DRAIN_BUILD,
                    policy_id=1,
                    deletion_generation=0,
                    now=now,
                )
            )
        now += 1
    assert pages == [128, 1]
    assert _mariadb_non_abandoned(connector) == 0
