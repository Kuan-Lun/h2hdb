from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import test_vnext_publication_candidate_repository as candidate_fixtures
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    inspect_one,
)

import h2hdb.vnext_ingest_publication as publication
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
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
    config: CoreConfig,
) -> Iterator[
    tuple[
        SQLConnector,
        GateLease,
        IngestTurn,
        PublicationCatalogProjectionPlan,
    ]
]:
    connector = candidate_fixtures._generated_database(config)
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
            VNextUnitOfWork(connector, backend=connector_backend(connector)),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    try:
        with PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend=connector_backend(connector),
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
    database_factory: DatabaseFactory,
    tmp_path: Path,
) -> None:
    with _generated_catalog_plan(
        database_factory.config(str(tmp_path / "canonical-fence.sqlite3"))
    ) as (
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
            inspect_one(
                connector,
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                claim_parameters,
            )
            == claim_parameters
        )

        with connector.transaction():
            PublicationCandidateRepository._lock_canonical_allocation_fence_authorized(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                candidate_id=_CANDIDATE,
                stage=b"BUILD_CATALOG_PROJECTION",
                first_consumer_cursor=first_consumer_cursor,
            )
        with connector.transaction():
            batch = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"consume-first-canonical",
                now=112,
            )

        assert batch.next_cursor >= first_consumer_cursor
        assert (
            inspect_one(
                connector,
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
                    VNextUnitOfWork(connector, backend=connector_backend(connector)),
                    candidate_id=_CANDIDATE,
                    stage=b"BUILD_CATALOG_PROJECTION",
                    first_consumer_cursor=first_consumer_cursor,
                )
        assert (
            inspect_one(
                connector,
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                claim_parameters,
            )
            == ()
        )


def _mariadb_claim(
    connector: SQLConnector,
    *,
    generation: int,
    value_sha256: bytes,
) -> tuple[Any, ...]:
    with connector.read_transaction():
        return inspect_one(
            connector,
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value_sha256),
        )


def test_live_mariadb_fence_rejects_consumed_first_consumer_without_claim_rebuild(
    db_config: CoreConfig,
) -> None:
    with _generated_catalog_plan(db_config) as (
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
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                candidate_id=_CANDIDATE,
                stage=b"BUILD_CATALOG_PROJECTION",
                first_consumer_cursor=first_consumer_cursor,
            )
        with connector.transaction():
            batch = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
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
                        VNextUnitOfWork(
                            connector, backend=connector_backend(connector)
                        ),
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
    database_factory: DatabaseFactory,
    tmp_path: Path,
) -> None:
    with _generated_catalog_plan(
        database_factory.config(str(tmp_path / "projection-thread.sqlite3"))
    ) as (
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
