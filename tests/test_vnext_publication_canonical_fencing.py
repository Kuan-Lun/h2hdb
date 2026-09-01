from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest
import test_vnext_publication_candidate_repository as candidate_fixtures

import h2hdb.vnext_ingest_publication as publication
from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateNotReadyError,
    PublicationCandidateRepository,
    PublicationCatalogProjectionPlan,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_CANDIDATE = candidate_fixtures._CANDIDATE


def _test_authorities() -> tuple[GateLease, IngestTurn]:
    return (
        GateLease(b"g" * 16, 7, GateMode.SHARED, (0,), 1_000),
        IngestTurn(7, b"i" * 16, 1_000),
    )


def _canonical_work(
    plan: CanonicalValueUploadPlan,
) -> publication._CanonicalWork:
    return publication._CanonicalWork(
        plan,
        object(),
        stage_fence=publication._CanonicalStageFence(
            _CANDIDATE,
            publication._Action.BUILD_CATALOG,
            b"first-consumer",
            7,
        ),
    )


def test_canonical_allocate_authorizes_then_locks_fresh_fence_before_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = CanonicalValueUploadPlan.from_parts("catalog_summary_utf8_v1", (b"x",))
    gate, turn = _test_authorities()
    work = cast(VNextUnitOfWork, SimpleNamespace())
    calls: list[str] = []
    allocation = object()

    def authorize(
        actual_work: VNextUnitOfWork,
        actual_gate: GateLease,
        actual_turn: IngestTurn,
        *,
        now: int,
    ) -> int:
        assert (actual_work, actual_gate, actual_turn, now) == (work, gate, turn, 50)
        calls.append("authorize")
        return 7

    def lock_fence(
        actual_work: VNextUnitOfWork,
        *,
        candidate_id: bytes,
        stage: bytes,
        first_consumer_cursor: bytes,
    ) -> None:
        assert actual_work is work
        assert candidate_id == _CANDIDATE
        assert stage == b"BUILD_CATALOG_PROJECTION"
        assert first_consumer_cursor == b"first-consumer"
        calls.append("fence")

    def allocate(
        actual_work: VNextUnitOfWork,
        *,
        generation: int,
        plan: CanonicalValueUploadPlan,
        now: int,
    ) -> object:
        assert actual_work is work
        assert generation == 7
        assert plan is canonical.plan
        assert now == 50
        calls.append("allocate")
        return allocation

    monkeypatch.setattr(publication, "_authorize_canonical_write", authorize)
    monkeypatch.setattr(
        PublicationCandidateRepository,
        "_lock_canonical_allocation_fence_authorized",
        staticmethod(lock_fence),
    )
    monkeypatch.setattr(publication, "_allocate_authorized", allocate)
    canonical = _canonical_work(plan)
    try:
        result = publication._commit_canonical_work(
            work,
            action=publication._Action.CANONICAL_ALLOCATE,
            canonical=canonical,
            gate=gate,
            turn=turn,
            now=50,
        )
    finally:
        plan.close()

    assert result is allocation
    assert calls == ["authorize", "fence", "allocate"]


def test_canonical_allocate_stale_fence_performs_zero_allocation_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = CanonicalValueUploadPlan.from_parts("catalog_summary_utf8_v1", (b"x",))
    gate, turn = _test_authorities()
    work = cast(VNextUnitOfWork, SimpleNamespace())
    calls: list[str] = []

    def authorize(
        _work: VNextUnitOfWork,
        _gate: GateLease,
        _turn: IngestTurn,
        *,
        now: int,
    ) -> int:
        assert now == 50
        calls.append("authorize")
        return 7

    def reject_stale_fence(
        _work: VNextUnitOfWork,
        *,
        candidate_id: bytes,
        stage: bytes,
        first_consumer_cursor: bytes,
    ) -> None:
        assert candidate_id == _CANDIDATE
        assert stage == b"BUILD_CATALOG_PROJECTION"
        assert first_consumer_cursor == b"first-consumer"
        calls.append("fence")
        raise PublicationCandidateNotReadyError(
            "canonical allocation first consumer already advanced"
        )

    def unexpected_allocate(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("stale allocation reached _allocate_authorized")

    monkeypatch.setattr(publication, "_authorize_canonical_write", authorize)
    monkeypatch.setattr(
        PublicationCandidateRepository,
        "_lock_canonical_allocation_fence_authorized",
        staticmethod(reject_stale_fence),
    )
    monkeypatch.setattr(publication, "_allocate_authorized", unexpected_allocate)
    canonical = _canonical_work(plan)
    try:
        with pytest.raises(
            PublicationCandidateNotReadyError,
            match="first consumer already advanced",
        ):
            publication._commit_canonical_work(
                work,
                action=publication._Action.CANONICAL_ALLOCATE,
                canonical=canonical,
                gate=gate,
                turn=turn,
                now=50,
            )
    finally:
        plan.close()

    assert calls == ["authorize", "fence"]


@contextmanager
def _generated_catalog_plan(
    database_path: Path,
) -> Iterator[
    tuple[
        SQLiteConnector,
        GateLease,
        IngestTurn,
        PublicationCatalogProjectionPlan,
    ]
]:
    connector = candidate_fixtures._generated_database(database_path)
    gate, turn = candidate_fixtures._authorities(connector)
    candidate_fixtures._seed_completed_analysis(connector, turn, with_base=False)
    candidate_fixtures._seed_selected_galleries(connector, count=1)
    candidate_fixtures._seed_projection_metadata(connector, count=1, with_tags=True)
    candidate_fixtures._begin(
        connector,
        gate,
        turn,
        artifacts_required=True,
    )
    candidate_fixtures._complete_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    try:
        with PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="sqlite",
            authority=authority,
        ) as plan:
            yield connector, gate, turn, plan
    finally:
        connector.close()


def _canonical_consumers(
    plan: PublicationCatalogProjectionPlan,
) -> tuple[tuple[bytes, bytes], ...]:
    result: list[tuple[bytes, bytes]] = []
    for upload in plan.iter_canonical_value_plans():
        try:
            result.append(
                (
                    upload.value_sha256,
                    plan._canonical_consumer_cursor(upload.value_sha256),
                )
            )
        finally:
            upload.close()
    return tuple(result)


def test_generated_sqlite_fence_rejects_consumed_first_consumer_without_claim_rebuild(
    tmp_path: Path,
) -> None:
    with _generated_catalog_plan(tmp_path / "canonical-fence.sqlite3") as (
        connector,
        gate,
        turn,
        plan,
    ):
        consumers = _canonical_consumers(plan)
        assert consumers
        value_sha256, first_consumer_cursor = min(
            consumers,
            key=lambda item: item[1],
        )
        candidate_fixtures._upload_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )
        claim_parameters = (turn.generation, value_sha256)
        assert (
            connector.fetch_one(
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                claim_parameters,
            )
            == claim_parameters
        )

        with connector.transaction():
            PublicationCandidateRepository._lock_canonical_allocation_fence_authorized(
                VNextUnitOfWork(connector, backend="sqlite"),
                candidate_id=_CANDIDATE,
                stage=b"BUILD_CATALOG_PROJECTION",
                first_consumer_cursor=first_consumer_cursor,
            )
        with connector.transaction():
            batch = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"consume-first-canonical",
                now=112,
            )

        assert batch.next_cursor >= first_consumer_cursor
        assert (
            connector.fetch_one(
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                claim_parameters,
            )
            == ()
        )
        with pytest.raises(
            PublicationCandidateNotReadyError,
            match="first consumer already advanced",
        ):
            with connector.transaction():
                PublicationCandidateRepository._lock_canonical_allocation_fence_authorized(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    candidate_id=_CANDIDATE,
                    stage=b"BUILD_CATALOG_PROJECTION",
                    first_consumer_cursor=first_consumer_cursor,
                )
        assert (
            connector.fetch_one(
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                claim_parameters,
            )
            == ()
        )


def _mariadb_connector(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    connector = MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )
    connector.connect()
    return connector


def _mariadb_authorities(
    connector: MariaDBConnector,
) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"mariadb-gate-001",
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="mariadb"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="mariadb"),
            owner_token=b"mariadb-turn-001",
            now=11,
            lease_duration=1_000_000,
        )
    return gate, turn


def _complete_mariadb_selection(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
) -> None:
    timestamp = 101
    for method, batch_prefix in (
        (
            PublicationCandidateRepository.process_selection_batch,
            b"mariadb-build-selection-",
        ),
        (
            PublicationCandidateRepository.validate_selection_batch,
            b"mariadb-validate-selection-",
        ),
    ):
        for index in range(10):
            with connector.transaction():
                batch = method(
                    VNextUnitOfWork(connector, backend="mariadb"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=batch_prefix + index.to_bytes(2, "big"),
                    now=timestamp,
                )
            timestamp += 1
            if batch.terminal:
                break
        else:
            raise AssertionError("MariaDB selection stage did not converge")


def _upload_mariadb_projection_canonical_values(
    connector: MariaDBConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: PublicationCatalogProjectionPlan,
    *,
    now: int,
) -> None:
    for upload in plan.iter_canonical_value_plans():
        try:
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="mariadb"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=upload,
                    now=now,
                )
            for page in upload.iter_pages():
                with connector.transaction():
                    CanonicalValueRepository.put_page(
                        VNextUnitOfWork(connector, backend="mariadb"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        plan=upload,
                        prepared_page=page,
                        now=now,
                    )
            with connector.transaction():
                CanonicalValueRepository.seal(
                    VNextUnitOfWork(connector, backend="mariadb"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=upload,
                    now=now,
                )
        finally:
            upload.close()


@contextmanager
def _generated_mariadb_catalog_plan(
    config: CoreConfig,
) -> Iterator[
    tuple[
        MariaDBConnector,
        GateLease,
        IngestTurn,
        PublicationCatalogProjectionPlan,
    ]
]:
    VNextDatabaseAdminFacade(config).initialize()
    connector = _mariadb_connector(config)
    gate, turn = _mariadb_authorities(connector)
    fixture_connector = cast(Any, connector)
    with connector.transaction():
        candidate_fixtures._seed_completed_analysis(
            fixture_connector,
            turn,
            with_base=False,
        )
        candidate_fixtures._seed_selected_galleries(fixture_connector, count=1)
        candidate_fixtures._seed_projection_metadata(
            fixture_connector,
            count=1,
            with_tags=True,
        )
    with connector.transaction():
        with patch(
            "h2hdb.vnext_publication_candidate_repository._new_candidate_id",
            return_value=_CANDIDATE,
        ):
            PublicationCandidateRepository.begin(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=candidate_fixtures._ANALYSIS,
                artifact_policy_id=1,
                display_title_policy_id=1,
                artifacts_required=True,
                now=100,
            )
    _complete_mariadb_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="mariadb"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    try:
        with PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="mariadb",
            authority=authority,
        ) as plan:
            yield connector, gate, turn, plan
    finally:
        connector.close()


def _mariadb_claim(
    connector: MariaDBConnector,
    *,
    generation: int,
    value_sha256: bytes,
) -> tuple[Any, ...]:
    with connector.read_transaction():
        return connector.fetch_one(
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value_sha256),
        )


def test_live_mariadb_fence_rejects_consumed_first_consumer_without_claim_rebuild(
    mariadb_config: CoreConfig,
) -> None:
    with _generated_mariadb_catalog_plan(mariadb_config) as (
        connector,
        gate,
        turn,
        plan,
    ):
        consumers = _canonical_consumers(plan)
        assert consumers
        value_sha256, first_consumer_cursor = min(
            consumers,
            key=lambda item: item[1],
        )
        _upload_mariadb_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )
        claim = (turn.generation, value_sha256)
        assert (
            _mariadb_claim(
                connector,
                generation=turn.generation,
                value_sha256=value_sha256,
            )
            == claim
        )

        with connector.transaction():
            PublicationCandidateRepository._lock_canonical_allocation_fence_authorized(
                VNextUnitOfWork(connector, backend="mariadb"),
                candidate_id=_CANDIDATE,
                stage=b"BUILD_CATALOG_PROJECTION",
                first_consumer_cursor=first_consumer_cursor,
            )
        with connector.transaction():
            batch = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"mariadb-consume-first-canonical",
                now=112,
            )

        assert batch.next_cursor >= first_consumer_cursor
        assert (
            _mariadb_claim(
                connector,
                generation=turn.generation,
                value_sha256=value_sha256,
            )
            == ()
        )
        uploads = tuple(plan.iter_canonical_value_plans())
        try:
            delayed_upload = next(
                upload for upload in uploads if upload.value_sha256 == value_sha256
            )
            delayed = publication._CanonicalWork(
                delayed_upload,
                object(),
                stage_fence=publication._CanonicalStageFence(
                    _CANDIDATE,
                    publication._Action.BUILD_CATALOG,
                    first_consumer_cursor,
                    turn.generation,
                ),
            )
            with pytest.raises(
                PublicationCandidateNotReadyError,
                match="first consumer already advanced",
            ):
                with connector.transaction():
                    publication._commit_canonical_work(
                        VNextUnitOfWork(connector, backend="mariadb"),
                        action=publication._Action.CANONICAL_ALLOCATE,
                        canonical=delayed,
                        gate=gate,
                        turn=turn,
                        now=113,
                    )
        finally:
            for upload in uploads:
                upload.close()
        assert (
            _mariadb_claim(
                connector,
                generation=turn.generation,
                value_sha256=value_sha256,
            )
            == ()
        )


def _projection_fingerprint(
    plan: PublicationCatalogProjectionPlan,
) -> tuple[tuple[bytes, ...], tuple[tuple[bytes, bytes], ...]]:
    children = tuple(child.cursor for child in plan._page_after(b""))
    return children, _canonical_consumers(plan)


def test_disk_projection_plan_supports_sequential_cross_thread_reads(
    tmp_path: Path,
) -> None:
    with _generated_catalog_plan(tmp_path / "projection-thread.sqlite3") as (
        _connector,
        _gate,
        _turn,
        plan,
    ):
        expected = _projection_fingerprint(plan)
        with ThreadPoolExecutor(max_workers=1) as executor:
            observed = executor.submit(_projection_fingerprint, plan).result()

        assert observed == expected
        assert _projection_fingerprint(plan) == expected
