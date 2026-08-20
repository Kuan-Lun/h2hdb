from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from unittest.mock import patch

import pytest
from vnext_catalog_registry_fixtures import (
    seed_artifact_policy_semantics,
    seed_artifact_producer_fingerprint,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_title_sort_policy,
)
from vnext_manifest_fixtures import seed_snapshot_manifest
from vnext_publication_fixtures import seed_publication_finalization_checkpoint

from h2hdb import CoreConfig
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_allocator_repository import (
    IdentityStream,
    RevisionStream,
    VNextAllocatorRepository,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
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
    SourceBuildRepository,
    SourceDiscoveryPlan,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


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
    connector.execute(
        "INSERT INTO operational_operational_consumers "
        "(consumer_id, consumer_name) VALUES (%s, %s)",
        (1, "live-mariadb-downloader"),
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
                "SELECT generation, owner_token, claimed_at "
                "FROM operational_ingest_generation_owners ORDER BY generation"
            ),
            connector.fetch_all(
                "SELECT generation, lease_expires_at "
                "FROM operational_ingest_generation_leases ORDER BY generation"
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
        "SELECT next_revision FROM operational_revision_allocators "
        "WHERE stream = %s",
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
    build_id = b"mariadb-build001"
    root_command = SourceRootBuildCommand((), build_id)
    with root_command.prepare_root_upload() as root_plan:
        _upload(connector, current_gate, current_turn, root_plan, now=501)
        with connector.transaction():
            SourceBuildRepository.handoff_root(
                _work(connector),
                gate_lease=current_gate,
                ingest_turn=current_turn,
                command=root_command,
                root_plan=root_plan,
                now=504,
            )

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
        "INSERT INTO catalog_source_revision_anchors " "(source_revision) VALUES (%s)",
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
        (source_revision, snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptor_seals "
        "(source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
        (catalog_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (%s, %s)",
        (catalog_revision, 0),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals " "(revision) VALUES (%s)",
        (catalog_revision,),
    )
    producer = seed_artifact_producer_fingerprint(
        connector,
        artifact_algorithm_version=1,
        writer_id=b"writer",
        python_abi=b"abi",
        pillow_build=b"pillow",
        libjpeg_build=b"jpeg",
        zlib_build=b"zlib",
    )
    # Exact registry replay can be SELECT-only and therefore opens an implicit
    # Connector/Python transaction even though no fixture mutation was needed.
    connector.commit()
    policy_payload = identity.encode_artifact_policy(
        1,
        2048,
        producer.producer_fingerprint_sha256,
    )
    with CanonicalValueUploadPlan.from_parts(
        "artifact_policy_v2",
        (policy_payload,),
    ) as policy_plan:
        _upload(connector, current_gate, current_turn, policy_plan, now=551)
        policy_semantics = seed_artifact_policy_semantics(
            connector,
            artifact_algorithm_version=1,
            max_image_short_side=2048,
            producer_fingerprint_sha256=producer.producer_fingerprint_sha256,
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
    commit_members = (
        (
            "catalog_publication_commit_candidates",
            "candidate_id",
            b"live-candidate01",
        ),
        (
            "catalog_publication_commit_catalog_revisions",
            "revision",
            catalog_revision,
        ),
        (
            "catalog_publication_commit_source_revisions",
            "source_revision",
            source_revision,
        ),
        ("catalog_publication_commit_generations", "generation", 1),
        (
            "catalog_publication_commit_operational_preparations",
            "preparation_id",
            preparation.preparation_id,
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
        (
            "catalog_publication_commit_duplicate_losers",
            "duplicate_losers",
            0,
        ),
        ("catalog_publication_commit_committed_ats", "committed_at", 600),
    )
    with connector.transaction():
        connector.execute(
            "INSERT INTO catalog_publication_generation_nodes "
            "(generation) VALUES (%s)",
            (1,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            (1, 0),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_anchors "
            "(receipt_id) VALUES (%s)",
            (receipt_id,),
        )
        for table, value_column, value in commit_members:
            connector.execute(
                f"INSERT INTO {table} (receipt_id, {value_column}) " "VALUES (%s, %s)",
                (receipt_id, value),
            )
        seed_publication_finalization_checkpoint(
            connector,
            receipt_id=receipt_id,
            updated_at=600,
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_seals " "(receipt_id) VALUES (%s)",
            (receipt_id,),
        )
    activation = _read_one(
        connector,
        "SELECT source_revision, preparation_id, operational_policy_id, "
        "activated_at FROM operational_operational_activations "
        "WHERE source_revision = %s",
        (source_revision,),
    )
    assert activation == (source_revision, preparation.preparation_id, 1, 600)
    with connector.transaction():
        acknowledgement = OperationalEffectRepository.acknowledge_through(
            _work(connector),
            consumer_id=1,
            source_revision=source_revision,
            through_sequence_no=1,
            now=610,
        )
    assert acknowledgement.evidence_count == 2
    with connector.transaction():
        acknowledgement_replay = OperationalEffectRepository.acknowledge_through(
            _work(connector),
            consumer_id=1,
            source_revision=source_revision,
            through_sequence_no=1,
            now=611,
        )
    assert acknowledgement_replay.replayed
    assert _read_all(
        connector,
        "SELECT sequence_no, event_type FROM operational_operational_events "
        "ORDER BY sequence_no",
    ) == [(0, "REMOVED_GID"), (1, "DELETION_CONSUMPTION")]
    assert _read_one(
        connector, "SELECT COUNT(*) FROM operational_operational_event_acks"
    ) == (2,)

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
