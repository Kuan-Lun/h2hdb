"""Authority exhaustion, expiry takeover, response loss and policy
replacement through the public facades, on SQLite and (opt-in) live MariaDB.

Every case drives real production code paths: allocator, deletion-request,
download-generation and cleanup-cycle exhaustion sentinels are installed in
the durable authority rows and the next production write must fail closed
with its typed error and zero durable effect; an expired download owner is
taken over and its late completion is refused; a lost commit response is
replayed exactly; and a policy replaced between operational preparation and
commit supersedes the stale attempt.
"""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import patch

import pytest
from vnext_catalog_registry_fixtures import seed_manifest_policy
from vnext_fault_harness import (
    MAINTENANCE_GATE_TABLES,
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
    snapshot_database,
    snapshot_difference,
)
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    Clock,
    MemoryLibrary,
    MemorySource,
    catalog_view,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    populate_catalog,
    run_ingest_turn,
    takeover_clock,
)

from h2hdb import (
    CoreConfig,
    VNextDownloadQueueFacade,
    VNextIngestFacade,
)
from h2hdb.vnext_allocator_repository import (
    AllocatorExhaustedError,
    IdentityStream,
    RevisionStream,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_cleanup_repository import (
    CleanupCycleExhaustedError,
    CleanupTargetKind,
    _cleanup_id,
    _target_key,
)
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_download_ingest_repository import (
    DownloadGenerationExhaustedError,
    DownloadIngestReplayMismatchError,
    DownloadIngestUnavailableError,
    HandoffKind,
)
from h2hdb.vnext_hash_cache_repository import (
    FileHashCacheConflictError,
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_queue_repository import DeletionGenerationExhaustedError
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    SourceBuildRepository,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _corpus() -> tuple[MemorySource, MemoryLibrary]:
    source = MemorySource(
        [
            gallery(1001, pages=[b"p0-a", b"p1-a"], artists=["alice"]),
            gallery(1002, pages=[b"p0-b"], artists=["bob"]),
        ]
    )
    return source, MemoryLibrary(source)


def _execute(config: CoreConfig, sql: str, data: tuple[Any, ...] = ()) -> None:
    connector = open_connector(config)
    try:
        with connector.transaction():
            connector.execute(sql, data)
    finally:
        connector.close()


def _fetch_one(config: CoreConfig, sql: str, data: tuple[Any, ...] = ()) -> Any:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return connector.fetch_one(sql, data)
    finally:
        connector.close()


@pytest.fixture
def populated(db_config: CoreConfig) -> tuple[CoreConfig, MemorySource, MemoryLibrary]:
    initialize_database(db_config)
    source, library = _corpus()
    facade = VNextIngestFacade(db_config, clock=Clock())
    try:
        run_ingest_turn(facade, source=source, library=library)
        drain_maintenance(facade)
    finally:
        facade.close()
    return db_config, source, library


def test_deletion_generation_exhaustion_fails_closed_through_the_queue_facade(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
) -> None:
    config, _source, _library = populated
    _execute(
        config,
        "INSERT INTO operational_deletion_request_generations "
        "(generation, allocated_at) VALUES (%s, %s)",
        (INT63_MAX, 1),
    )
    _execute(
        config,
        "UPDATE operational_deletion_request_generation_heads "
        "SET current_generation = %s, updated_at = %s",
        (INT63_MAX, 1),
    )
    before = snapshot_database(config)
    queue = VNextDownloadQueueFacade(config, clock=Clock())
    with pytest.raises(DeletionGenerationExhaustedError):
        queue.request_deletion(1001, url=None)
    assert snapshot_difference(before, snapshot_database(config)) == {}


def test_download_generation_exhaustion_fails_closed_through_the_queue_facade(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
) -> None:
    config, _source, _library = populated
    head = _fetch_one(
        config,
        "SELECT current_generation, completed_generation "
        "FROM operational_download_coordination_heads",
    )
    assert head is not None
    _execute(
        config,
        "INSERT INTO operational_download_generations "
        "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
        (INT63_MAX, 1, 2),
    )
    _execute(
        config,
        "UPDATE operational_download_coordination_heads "
        "SET current_generation = %s, completed_generation = %s",
        (INT63_MAX, INT63_MAX),
    )
    before = snapshot_database(config)
    queue = VNextDownloadQueueFacade(config, clock=Clock())
    with pytest.raises(DownloadGenerationExhaustedError):
        queue.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
    assert snapshot_difference(before, snapshot_database(config)) == {}


@pytest.mark.parametrize(
    ("stream", "boundary"),
    (
        (RevisionStream.SOURCE, "publication.commit:COMMIT_PUBLICATION"),
        (RevisionStream.CATALOG, "publication.commit:BEGIN"),
    ),
    ids=("SOURCE", "CATALOG"),
)
def test_revision_allocator_exhaustion_rolls_back_the_publication_commit_exactly(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
    stream: RevisionStream,
    boundary: str,
) -> None:
    config, source, library = populated
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"]))
    snapshots: list[dict[str, Any]] = []
    saved: list[Any] = []

    def exhaust(label: str) -> None:
        if label == boundary and not saved:
            saved.append(
                _fetch_one(
                    config,
                    "SELECT next_revision, updated_at "
                    "FROM operational_revision_allocators WHERE stream = %s",
                    (stream.value,),
                )
            )
            _execute(
                config,
                "UPDATE operational_revision_allocators SET next_revision = %s "
                "WHERE stream = %s",
                (INT63_MAX, stream.value),
            )
            snapshots.append(snapshot_database(config))

    facade = VNextIngestFacade(config, clock=Clock())
    try:
        session = claim_session(facade)
        with pytest.raises(AllocatorExhaustedError):
            run_ingest_turn(
                facade,
                source=source,
                library=library,
                session=session,
                boundary=exhaust,
            )
        assert saved and snapshots
        assert snapshot_difference(snapshots[0], snapshot_database(config)) == {}
        # Restoring the authority lets the very same session commit.
        _execute(
            config,
            "UPDATE operational_revision_allocators SET next_revision = %s "
            "WHERE stream = %s",
            (saved[0][0], stream.value),
        )
        run_ingest_turn(facade, source=source, library=library, session=session)
        drain_maintenance(facade)
    finally:
        facade.close()
    assert full_check(config).state == "READY"
    assert catalog_view(config)["publication_count"] == 2


@pytest.mark.parametrize(
    "stream", (IdentityStream.POLICY, IdentityStream.GALLERY, IdentityStream.TAG)
)
def test_identity_allocator_exhaustion_fails_closed_and_recovers(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
    stream: IdentityStream,
) -> None:
    config, source, library = populated
    saved = _fetch_one(
        config,
        "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
        (stream.value,),
    )
    _execute(
        config,
        "UPDATE operational_identity_allocators SET next_id = %s WHERE stream = %s",
        (INT63_MAX, stream.value),
    )
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        session = claim_session(facade)
        if stream is IdentityStream.POLICY:
            before = snapshot_database(config)
            with pytest.raises(AllocatorExhaustedError):
                facade.ensure_policy(
                    session, ingest_policy(spam_occurrence_threshold=9)
                )
            assert snapshot_difference(before, snapshot_database(config)) == {}
        else:
            # A brand-new gallery with a brand-new tag needs both identities.
            source.put(
                gallery(
                    1003,
                    pages=[b"p0-c"],
                    artists=["carol"],
                    extra_tags=[("female", "brand-new-tag")],
                )
            )
            with pytest.raises(AllocatorExhaustedError):
                run_ingest_turn(facade, source=source, library=library, session=session)
            # Every committed transaction before the refused allocation is a
            # consistent durable prefix.
            assert full_check(config).state == "READY"
        _execute(
            config,
            "UPDATE operational_identity_allocators SET next_id = %s WHERE stream = %s",
            (saved[0], stream.value),
        )
        run_ingest_turn(facade, source=source, library=library, session=session)
        drain_maintenance(facade)
    finally:
        facade.close()
    assert full_check(config).state == "READY"
    assert catalog_view(config)["publication_count"] == len(source.galleries)


def test_cleanup_cycle_exhaustion_fails_closed_with_zero_writes(
    db_config: CoreConfig,
) -> None:
    """Every COMPLETE cleanup job is moved to the last representable cycle
    (with its exact derived cleanup_id, so the shard identity stays valid);
    the next drain that must reuse one of them fails closed with zero writes."""

    initialize_database(db_config)
    populate_catalog(db_config)
    connector = open_connector(db_config)
    try:
        with connector.transaction():
            jobs = connector.fetch_all(
                "SELECT job.cleanup_id, job.target_key, target.target_kind "
                "FROM operational_cleanup_jobs AS job "
                "JOIN operational_cleanup_sweep_targets AS target "
                "ON target.target_key = job.target_key "
                "WHERE job.state = 'COMPLETE'"
            )
            assert jobs
            for cleanup_id, target_key, raw_kind in jobs:
                kind = CleanupTargetKind(
                    raw_kind.decode("ascii")
                    if isinstance(raw_kind, bytes)
                    else raw_kind
                )
                shard = next(
                    number
                    for number in range(256)
                    if _target_key(kind, number) == bytes(target_key)
                )
                assert (
                    connector.execute_affected(
                        "UPDATE operational_cleanup_jobs "
                        "SET cleanup_id = %s, cycle_generation = %s "
                        "WHERE cleanup_id = %s",
                        (_cleanup_id(kind, shard, INT63_MAX), INT63_MAX, cleanup_id),
                    )
                    == 1
                )
    finally:
        connector.close()
    # Give the same strategies work again so an exhausted cycle must be reused.
    source, library = _corpus()
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-again"], artists=["alice"]))
    facade = VNextIngestFacade(db_config, clock=Clock())
    try:
        run_ingest_turn(facade, source=source, library=library)
        before = snapshot_database(db_config)
        with pytest.raises(CleanupCycleExhaustedError):
            drain_maintenance(facade)
        # Only the EXCLUSIVE gate acquisition and its compensating release
        # are durable; no cleanup job, checkpoint, root or data row changed.
        assert set(snapshot_difference(before, snapshot_database(db_config))) <= set(
            MAINTENANCE_GATE_TABLES
        )
    finally:
        facade.close()


def test_expired_download_owner_is_taken_over_and_its_late_finish_is_refused(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
) -> None:
    """The ingest side consumes an expired download owner's generation as an
    EXPIRED_TAKEOVER handoff; the late downloader's completion and renewal are
    refused with zero durable effect."""

    config, source, library = populated
    early = VNextDownloadQueueFacade(config, clock=Clock())
    request = early.request_download(5001, url="https://example.invalid/g/5001")
    turn = early.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
    taker = VNextIngestFacade(config, clock=takeover_clock())
    try:
        session = claim_session(taker, periodic=False)
        takeover = _fetch_one(
            config,
            "SELECT handoff_kind FROM operational_download_ingest_handoffs "
            "WHERE download_generation = %s",
            (turn.generation,),
        )
        assert takeover == (HandoffKind.EXPIRED_TAKEOVER.value,)
        before = snapshot_database(config)
        # The late owner presents a turn whose generation was handed off by
        # the takeover: completion is a replay mismatch, renewal is refused.
        with pytest.raises(
            (DownloadIngestUnavailableError, DownloadIngestReplayMismatchError)
        ):
            early.finish_download_turn(turn, request)
        with pytest.raises(DownloadIngestUnavailableError):
            early.renew_download_turn(
                turn, lease_duration_microseconds=LEASE_MICROSECONDS
            )
        assert snapshot_difference(before, snapshot_database(config)) == {}
        # The taken-over generation links to this ingest turn and completes.
        run_ingest_turn(
            taker, source=source, library=library, session=session, periodic=False
        )
        drain_maintenance(taker)
    finally:
        taker.close()
    assert full_check(config).state == "READY"


def test_lost_download_finish_response_replays_the_same_handoff(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _source, _library = populated
    injector = FaultInjector()
    with fault_injection(monkeypatch, injector):
        queue = VNextDownloadQueueFacade(config, clock=Clock())
        request = queue.request_download(5002, url="https://example.invalid/g/5002")
        turn = queue.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
        injector.fail_after_commit = injector.commits + 1
        with pytest.raises(InjectedFault):
            queue.finish_download_turn(turn, request)
        assert injector.fired == "after_commit"
        committed = snapshot_database(config)
        mutations = injector.mutations
        handoff = queue.finish_download_turn(turn, request)
        assert injector.mutations == mutations
        assert snapshot_difference(committed, snapshot_database(config)) == {}
        assert handoff.download_generation == turn.generation
        assert queue.is_download_handoff_complete(handoff) is False


def test_policy_replacement_between_preparation_and_commit_supersedes_the_attempt(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
) -> None:
    config, source, library = populated
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"]))

    class _Stop(Exception):
        pass

    def stop(label: str) -> None:
        if label == "publication.commit:SEAL_OPERATIONAL":
            raise _Stop(label)

    first = VNextIngestFacade(config, clock=Clock())
    try:
        with pytest.raises(_Stop):
            run_ingest_turn(first, source=source, library=library, boundary=stop)
    finally:
        first.close()
    replaced = dataclasses.replace(ingest_policy(), operational_max_batch_rows=64)
    second = VNextIngestFacade(config, clock=takeover_clock())
    try:
        run_ingest_turn(second, source=source, library=library, policy=replaced)
        connector = open_connector(config)
        try:
            with connector.read_transaction():
                attempts = connector.fetch_all(
                    "SELECT preparation.state, policy.max_batch_rows "
                    "FROM operational_operational_preparations AS preparation "
                    "JOIN operational_operational_policys AS policy "
                    "ON policy.operational_policy_id = "
                    "preparation.operational_policy_id "
                    "ORDER BY preparation.state"
                )
        finally:
            connector.close()
        # The replaced policy is a new attempt; the stale one is abandoned and
        # reclaimed by generic cleanup, while the first revision's committed
        # attempt under the old policy stays as publication lineage.
        assert sorted(attempts) == [
            ("ABANDONED", 128),
            ("COMPLETE", 64),
            ("COMPLETE", 128),
        ]
        drain_maintenance(second)
        connector = open_connector(config)
        try:
            with connector.read_transaction():
                remaining = connector.fetch_all(
                    "SELECT state FROM operational_operational_preparations "
                    "ORDER BY state"
                )
        finally:
            connector.close()
        assert remaining == [("COMPLETE",), ("COMPLETE",)]
    finally:
        second.close()
    assert full_check(config).state == "READY"
    assert catalog_view(config)["publication_count"] == 2


def test_lost_publication_commit_response_allocates_each_revision_once(
    populated: tuple[CoreConfig, MemorySource, MemoryLibrary],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, source, library = populated
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"]))
    injector = FaultInjector()

    def lose(label: str) -> None:
        if label == "publication.commit:COMMIT_PUBLICATION" and injector.fired is None:
            injector.fail_after_commit = injector.commits + 1

    with fault_injection(monkeypatch, injector):
        facade = VNextIngestFacade(config, clock=Clock())
        try:
            session = claim_session(facade)
            with pytest.raises(InjectedFault):
                run_ingest_turn(
                    facade,
                    source=source,
                    library=library,
                    session=session,
                    boundary=lose,
                )
            assert injector.fired == "after_commit"
            injector.fired = None
            run_ingest_turn(facade, source=source, library=library, session=session)
            drain_maintenance(facade)
        finally:
            facade.close()
    assert full_check(config).state == "READY"
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            revisions = connector.fetch_all(
                "SELECT source_revision, revision FROM catalog_publication_commits "
                "ORDER BY revision"
            )
            allocators = dict(
                connector.fetch_all(
                    "SELECT stream, next_revision FROM operational_revision_allocators"
                )
            )
    finally:
        connector.close()
    sources = [int(row[0]) for row in revisions]
    catalogs = [int(row[1]) for row in revisions]
    assert sources == list(range(sources[0], sources[0] + len(sources)))
    assert catalogs == list(range(catalogs[0], catalogs[0] + len(catalogs)))
    next_by_stream = {str(key): int(value) for key, value in allocators.items()}
    assert next_by_stream["SOURCE"] == sources[-1] + 1
    assert next_by_stream["CATALOG"] == catalogs[-1] + 1


def _repository_authorities(
    connector: Any, backend: str
) -> tuple[GateLease, IngestTurn]:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ),
    ):
        gate = MaintenanceGateRepository.claim_shared(
            VNextUnitOfWork(connector, backend=backend),
            now=10,
            lease_duration=10_000,
        )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend=backend),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=10_000,
        )
    return gate, turn


def _upload(
    connector: Any,
    backend: str,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    start: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=start,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=start + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=start + 2,
        )


def test_hash_cache_handoff_replay_and_lookup_on_each_backend(
    db_config: CoreConfig,
) -> None:
    """The hash-cache handoff (canonical source identity and fingerprint,
    exact file digest and byte count) is replayed exactly and looked up
    exactly on SQLite and on live MariaDB, with a same-key conflict refused."""

    initialize_database(db_config)
    backend = db_config.database.sql_type
    connector = open_connector(db_config)
    source_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_source_identity_v1", (b"source-id-v1\0", b"/gallery/file.jpg")
    )
    fingerprint_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_fingerprint_v1", (b"fingerprint-v1\0", b"stat")
    )
    root_command = SourceRootBuildCommand(
        ("source",), SourceBuildManifestSummary.empty()
    )
    root_plan = root_command.prepare_root_upload()
    try:
        with connector.transaction():
            seed_manifest_policy(connector)
        gate, turn = _repository_authorities(connector, backend)
        _upload(connector, backend, gate, turn, root_plan, start=20)
        with connector.transaction():
            SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                command=root_command,
                root_plan=root_plan,
                now=23,
            )
        _upload(connector, backend, gate, turn, source_plan, start=30)
        _upload(connector, backend, gate, turn, fingerprint_plan, start=40)
        file_plan = FileHashObservationPlan.from_parts((b"file-", b"bytes"))
        with connector.transaction():
            first = VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=file_plan,
                observed_at=50,
                cached_at=51,
                now=52,
            )
        assert not first.replayed
        with connector.transaction():
            replay = VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=file_plan,
                observed_at=50,
                cached_at=51,
                now=53,
            )
        assert replay.replayed and replay.file_sha256 == first.file_sha256
        forged = FileHashObservationPlan.from_parts((b"file-", b"bytes!!"))
        before = snapshot_database(db_config)
        with pytest.raises(FileHashCacheConflictError):
            with connector.transaction():
                VNextHashCacheRepository.handoff(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    ingest_turn=turn,
                    source_plan=source_plan,
                    fingerprint_plan=fingerprint_plan,
                    file_plan=forged,
                    observed_at=50,
                    cached_at=51,
                    now=54,
                )
        assert snapshot_difference(before, snapshot_database(db_config)) == {}
    finally:
        source_plan.close()
        fingerprint_plan.close()
        root_plan.close()
        connector.close()
