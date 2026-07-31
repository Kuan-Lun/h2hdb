from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from h2hdb import (
    H2HDB,
    DownloadTurn,
    EnsureDownloadRequestResult,
    GalleryIngestPhase,
    H2HDBConfig,
)
from h2hdb.sql_connector import DatabaseDuplicateKeyError


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _complete_baseline_ingest(db: H2HDB) -> None:
    turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert turn is not None
    assert turn.generation == 0
    assert turn.claimed_from_phase == GalleryIngestPhase.ingest_requested
    assert db._complete_gallery_ingest(turn) is True


def _expire_active_lease(db: H2HDB) -> None:
    with db.SQLConnector() as connector:
        connector.execute("""
            UPDATE gallery_ingest_state
            SET lease_expires_at = 0
            WHERE state_id = 1
            """)


def test_initial_state_requires_baseline_ingest_before_downloads(
    db: H2HDB,
) -> None:
    state = db.get_gallery_ingest_state()

    assert state.phase == GalleryIngestPhase.ingest_requested
    assert state.generation == 0
    assert state.completed_generation == 0
    assert state.owner_token is None
    assert state.lease_expires_at is None
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None
    assert db.claim_download_turn(lease_seconds=60) is None

    _complete_baseline_ingest(db)

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == 0
    assert db.claim_download_turn(lease_seconds=60) is not None


def test_create_main_tables_preserves_coordination_state(db: H2HDB) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True
    expected_state = db.get_gallery_ingest_state()

    db.create_main_tables()
    db.create_main_tables()

    assert db.get_gallery_ingest_state() == expected_state
    with db.SQLConnector() as connector:
        assert connector.fetch_one("SELECT COUNT(*) FROM gallery_ingest_state") == (1,)


def test_coordination_schema_rejects_a_second_singleton_row(db: H2HDB) -> None:
    with pytest.raises(DatabaseDuplicateKeyError):
        with db.SQLConnector() as connector:
            connector.execute(
                """
                INSERT INTO gallery_ingest_state (
                    state_id,
                    phase,
                    generation,
                    completed_generation,
                    owner_token,
                    lease_expires_at,
                    last_transition_at
                )
                VALUES (2, %s, 0, 0, NULL, NULL, 0)
                """,
                (GalleryIngestPhase.ready.value,),
            )


def test_download_turn_handoff_is_fenced_and_idempotent(db: H2HDB) -> None:
    _complete_baseline_ingest(db)

    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    assert turn.generation == 1
    assert db.claim_download_turn(lease_seconds=60) is None
    assert db.renew_download_turn(turn, lease_seconds=120) is True

    stale_turn = DownloadTurn(
        generation=turn.generation,
        owner_token="stale-owner",
        lease_expires_at=turn.lease_expires_at,
    )
    assert db.renew_download_turn(stale_turn, lease_seconds=120) is False
    assert db.request_gallery_ingest(stale_turn) is False

    assert db.request_gallery_ingest(turn) is True
    assert db.request_gallery_ingest(turn) is True
    assert db.renew_download_turn(turn, lease_seconds=120) is False
    requested_state = db.get_gallery_ingest_state()
    assert requested_state.handoff_generation == turn.generation
    assert requested_state.handoff_owner_token == turn.owner_token

    ingest_turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    assert ingest_turn.generation == turn.generation
    assert db.request_gallery_ingest(turn) is True
    assert db._complete_gallery_ingest(ingest_turn) is True

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == turn.generation
    assert state.handoff_generation == turn.generation
    assert state.handoff_owner_token == turn.owner_token
    assert db.request_gallery_ingest(turn) is True

    next_turn = db.claim_download_turn(lease_seconds=60)
    assert next_turn is not None
    next_state = db.get_gallery_ingest_state()
    assert next_state.handoff_generation is None
    assert next_state.handoff_owner_token is None
    assert db.request_gallery_ingest(turn) is False


def test_ensure_download_request_creates_then_preserves_token_and_enriches_url(
    db: H2HDB,
) -> None:
    gid = 844990
    url = f"https://e-hentai.org/g/{gid}/abc123def4"

    created = db.ensure_download_request(gid)
    assert isinstance(created, EnsureDownloadRequestResult)
    assert created.created is True
    assert created.request.gid == gid
    assert created.request.url == ""

    enriched = db.ensure_download_request(gid, url)
    assert enriched.created is False
    assert enriched.request.token == created.request.token
    assert enriched.request.url == url

    preserved = db.ensure_download_request(
        gid,
        f"https://e-hentai.org/g/{gid}/ffffffffff",
    )
    assert preserved.created is False
    assert preserved.request == enriched.request
    assert db.get_download_request(gid) == enriched.request


def test_ensure_download_request_is_atomic_for_concurrent_creators(
    db: H2HDB,
) -> None:
    gid = 844991
    url = f"https://e-hentai.org/g/{gid}/abc123def4"
    barrier = Barrier(2)

    def ensure() -> EnsureDownloadRequestResult:
        barrier.wait()
        return db.ensure_download_request(gid, url)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(ensure), executor.submit(ensure)]
        results = [future.result() for future in futures]

    assert sum(result.created for result in results) == 1
    assert results[0].request == results[1].request
    assert db.get_download_request(gid) == results[0].request


def test_complete_download_request_in_turn_keeps_live_turn_and_is_idempotent(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844992)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert db.complete_download_request_in_turn(turn, request) is True
    assert db.complete_download_request_in_turn(turn, request) is True

    assert db.get_download_request(request.gid) is None
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.generation == turn.generation
    assert state.owner_token == turn.owner_token
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_complete_download_request_in_turn_preserves_a_newer_request(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    stale_request = db.request_download(844993)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    newer_request = db.request_download(
        stale_request.gid,
        "https://e-hentai.org/g/844993/abc123def4",
    )

    assert db.complete_download_request_in_turn(turn, stale_request) is True

    assert db.get_download_request(stale_request.gid) == newer_request
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading


def test_complete_download_request_in_turn_rejects_stale_and_expired_turns(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844994)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    stale_turn = DownloadTurn(
        generation=turn.generation,
        owner_token="stale-owner",
        lease_expires_at=turn.lease_expires_at,
    )

    assert db.complete_download_request_in_turn(stale_turn, request) is False
    assert db.get_download_request(request.gid) == request

    _expire_active_lease(db)
    assert db.complete_download_request_in_turn(turn, request) is False
    assert db.get_download_request(request.gid) == request
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading


def test_complete_download_request_in_turn_rejects_after_handoff(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844995)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    assert db.request_gallery_ingest(turn) is True

    assert db.complete_download_request_in_turn(turn, request) is False

    assert db.get_download_request(request.gid) == request
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.ingest_requested


def test_complete_download_request_in_turn_propagates_delete_failure_without_handoff(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845018)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def fail_delete(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected batch root delete failure")

    monkeypatch.setattr(
        db.todownload_queue,
        "_complete_download_request_with_connector",
        fail_delete,
    )

    with pytest.raises(RuntimeError, match="injected batch root delete failure"):
        db.complete_download_request_in_turn(turn, request)

    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_complete_missing_download_request_in_turn_marks_and_is_idempotent(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844996)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert (
        db.complete_missing_download_request_in_turn(turn, request, request.gid) is True
    )
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is True
    assert db.get_download_request(request.gid) is None
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading

    db.clear_removed_gallery_gid(request.gid)
    assert (
        db.complete_missing_download_request_in_turn(turn, request, request.gid) is True
    )
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False


def test_complete_missing_download_request_in_turn_preserves_newer_token(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    stale_request = db.request_download(844997)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    newer_request = db.request_download(
        stale_request.gid,
        "https://e-hentai.org/g/844997/abc123def4",
    )

    assert (
        db.complete_missing_download_request_in_turn(
            turn,
            stale_request,
            stale_request.gid,
        )
        is True
    )

    assert db.get_download_request(stale_request.gid) == newer_request
    assert db.removed_galleries._check_removed_gallery_gid(stale_request.gid) is False
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading


def test_complete_missing_download_request_in_turn_rejects_stale_or_expired_turn(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844998)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    stale_turn = DownloadTurn(
        generation=turn.generation,
        owner_token="stale-owner",
        lease_expires_at=turn.lease_expires_at,
    )

    assert (
        db.complete_missing_download_request_in_turn(
            stale_turn,
            request,
            request.gid,
        )
        is False
    )
    assert db.get_download_request(request.gid) == request
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False

    _expire_active_lease(db)
    assert (
        db.complete_missing_download_request_in_turn(turn, request, request.gid)
        is False
    )
    assert db.get_download_request(request.gid) == request
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False


def test_complete_missing_download_request_in_turn_rolls_back_delete_on_marker_failure(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(844999)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def fail_marker(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected batch missing marker failure")

    monkeypatch.setattr(
        db.removed_galleries,
        "_insert_removed_gallery_gid_with_connector",
        fail_marker,
    )

    with pytest.raises(RuntimeError, match="injected batch missing marker failure"):
        db.complete_missing_download_request_in_turn(turn, request, request.gid)

    assert db.get_download_request(request.gid) == request
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading


def test_settled_roots_survive_expired_turn_recovery_and_unfinished_root_remains(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    completed_request = db.request_download(845000)
    missing_request = db.request_download(845016)
    unfinished_request = db.request_download(845017)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert db.complete_download_request_in_turn(turn, completed_request) is True
    assert (
        db.complete_missing_download_request_in_turn(
            turn,
            missing_request,
            missing_request.gid,
        )
        is True
    )
    _expire_active_lease(db)

    ingest_turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    assert ingest_turn.claimed_from_phase == GalleryIngestPhase.downloading
    assert db.get_download_request(completed_request.gid) is None
    assert db.get_download_request(missing_request.gid) is None
    assert db.removed_galleries._check_removed_gallery_gid(missing_request.gid) is True
    assert db.get_download_request(unfinished_request.gid) == unfinished_request
    assert db._complete_gallery_ingest(ingest_turn) is True


def test_finish_download_turn_atomically_hands_off_and_deletes_root(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845001)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert db.finish_download_turn(turn, request) is True
    assert db.get_download_request(request.gid) is None
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingest_requested
    assert state.handoff_generation == turn.generation
    assert state.handoff_owner_token == turn.owner_token
    assert db.request_gallery_ingest(turn) is True


def test_prior_generic_handoff_prevents_later_finish_mutations(db: H2HDB) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845013)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    assert db.request_gallery_ingest(turn) is True

    assert db.finish_download_turn(turn, request) is True
    assert db.finish_missing_download_turn(turn, request, request.gid) is True

    assert db.get_download_request(request.gid) == request
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False


def test_finish_download_turn_preserves_newer_root_request_while_handing_off(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    stale_request = db.request_download(845002)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    newer_request = db.request_download(
        stale_request.gid,
        "https://e-hentai.org/g/845002/abc123def4",
    )
    assert newer_request.token != stale_request.token

    assert db.finish_download_turn(turn, stale_request) is True
    assert db.get_download_request(stale_request.gid) == newer_request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingest_requested
    assert state.handoff_generation == turn.generation
    assert state.handoff_owner_token == turn.owner_token


def test_finish_download_turn_rolls_back_handoff_when_delete_fails(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845003)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def fail_delete(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected root delete failure")

    monkeypatch.setattr(
        db.todownload_queue,
        "_complete_download_request_with_connector",
        fail_delete,
    )

    with pytest.raises(RuntimeError, match="injected root delete failure"):
        db.finish_download_turn(turn, request)

    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_finish_missing_download_turn_atomically_hands_off_marks_removed_and_deletes_root(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845004)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert db.finish_missing_download_turn(turn, request, request.gid) is True

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is True
    assert db.get_download_request(request.gid) is None
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingest_requested
    assert state.handoff_generation == turn.generation
    assert state.handoff_owner_token == turn.owner_token


def test_finish_missing_download_turn_preserves_newer_root_request(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    stale_request = db.request_download(845005)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    newer_request = db.request_download(
        stale_request.gid,
        "https://e-hentai.org/g/845005/abc123def4",
    )

    assert db.finish_missing_download_turn(
        turn,
        stale_request,
        stale_request.gid,
    )

    assert db.removed_galleries._check_removed_gallery_gid(stale_request.gid) is False
    assert db.get_download_request(stale_request.gid) == newer_request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingest_requested
    assert state.handoff_generation == turn.generation
    assert state.handoff_owner_token == turn.owner_token


def test_finished_missing_turn_replay_cannot_restore_a_cleared_marker(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845012)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    assert db.finish_missing_download_turn(turn, request, request.gid) is True
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is True

    db.clear_removed_gallery_gid(request.gid)
    assert db.finish_missing_download_turn(turn, request, request.gid) is True

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) is None


def test_finish_missing_download_turn_is_fenced_before_missing_mutations(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845006)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    stale_turn = DownloadTurn(
        generation=turn.generation,
        owner_token="stale-owner",
        lease_expires_at=turn.lease_expires_at,
    )

    assert (
        db.finish_missing_download_turn(
            stale_turn,
            request,
            request.gid,
        )
        is False
    )

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_finish_missing_download_turn_rolls_back_all_writes_when_delete_fails(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845007)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def fail_delete(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected missing-root delete failure")

    monkeypatch.setattr(
        db.todownload_queue,
        "_complete_download_request_with_connector",
        fail_delete,
    )

    with pytest.raises(RuntimeError, match="injected missing-root delete failure"):
        db.finish_missing_download_turn(turn, request, request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_finish_missing_download_turn_rolls_back_handoff_and_delete_when_marker_fails(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845014)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    def fail_marker(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected missing-root marker failure")

    monkeypatch.setattr(
        db.removed_galleries,
        "_insert_removed_gallery_gid_with_connector",
        fail_marker,
    )

    with pytest.raises(RuntimeError, match="injected missing-root marker failure"):
        db.finish_missing_download_turn(turn, request, request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_complete_missing_download_request_marks_removed_and_exact_deletes(
    db: H2HDB,
) -> None:
    request = db.request_download(845008)

    db.complete_missing_download_request(request, request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is True
    assert db.get_download_request(request.gid) is None

    db.clear_removed_gallery_gid(request.gid)
    db.clear_removed_gallery_gid(request.gid)
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False

    db.complete_missing_download_request(request, request.gid)
    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False


def test_complete_missing_download_request_preserves_newer_token_without_marking(
    db: H2HDB,
) -> None:
    stale_request = db.request_download(845009)
    newer_request = db.request_download(
        stale_request.gid,
        "https://e-hentai.org/g/845009/abc123def4",
    )

    db.complete_missing_download_request(stale_request, stale_request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(stale_request.gid) is False
    assert db.get_download_request(stale_request.gid) == newer_request


def test_complete_missing_download_request_rolls_back_removed_when_delete_fails(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = db.request_download(845010)

    def fail_delete(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected direct missing delete failure")

    monkeypatch.setattr(
        db.todownload_queue,
        "_complete_download_request_with_connector",
        fail_delete,
    )

    with pytest.raises(RuntimeError, match="injected direct missing delete failure"):
        db.complete_missing_download_request(request, request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) == request


def test_complete_missing_download_request_rolls_back_delete_when_marker_fails(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = db.request_download(845015)

    def fail_marker(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected direct missing marker failure")

    monkeypatch.setattr(
        db.removed_galleries,
        "_insert_removed_gallery_gid_with_connector",
        fail_marker,
    )

    with pytest.raises(RuntimeError, match="injected direct missing marker failure"):
        db.complete_missing_download_request(request, request.gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.get_download_request(request.gid) == request


def test_missing_download_completion_rejects_a_mismatched_gid_without_writes(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(845011)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None
    mismatched_gid = request.gid + 1

    with pytest.raises(ValueError, match="does not match"):
        db.complete_missing_download_request(request, mismatched_gid)
    with pytest.raises(ValueError, match="does not match"):
        db.complete_missing_download_request_in_turn(
            turn,
            request,
            mismatched_gid,
        )
    with pytest.raises(ValueError, match="does not match"):
        db.finish_missing_download_turn(turn, request, mismatched_gid)

    assert db.removed_galleries._check_removed_gallery_gid(request.gid) is False
    assert db.removed_galleries._check_removed_gallery_gid(mismatched_gid) is False
    assert db.get_download_request(request.gid) == request
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.downloading
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None


def test_downloader_and_periodic_scan_atomically_compete_for_ready(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    barrier = Barrier(2)

    def claim_download() -> object:
        barrier.wait()
        return db.claim_download_turn(lease_seconds=60)

    def claim_periodic_ingest() -> object:
        barrier.wait()
        return db._claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=True,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            executor.submit(claim_download),
            executor.submit(claim_periodic_ingest),
        ]

    assert sum(result.result() is not None for result in results) == 1
    assert db.get_gallery_ingest_state().phase in {
        GalleryIngestPhase.downloading,
        GalleryIngestPhase.ingesting,
    }


def test_fresh_download_blocks_periodic_ingest_and_expired_lease_is_recovered(
    db: H2HDB,
) -> None:
    _complete_baseline_ingest(db)
    request = db.request_download(812345)
    turn = db.claim_download_turn(lease_seconds=60)
    assert turn is not None

    assert (
        db._claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=True,
        )
        is None
    )

    _expire_active_lease(db)

    assert db.renew_download_turn(turn, lease_seconds=60) is False
    assert db.request_gallery_ingest(turn) is False
    ingest_turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    assert ingest_turn.claimed_from_phase == GalleryIngestPhase.downloading
    assert db.request_gallery_ingest(turn) is False
    assert db.finish_download_turn(turn, request) is False

    # A hard-killed downloader leaves its durable root request available for a
    # later retry; lease recovery only hands its on-disk output to ingestion.
    assert db.get_download_request(request.gid) == request
    assert db._complete_gallery_ingest(ingest_turn) is True
    state = db.get_gallery_ingest_state()
    assert state.completed_generation == turn.generation
    assert state.handoff_generation is None
    assert state.handoff_owner_token is None
    assert db.request_gallery_ingest(turn) is False


def test_fresh_ingest_owner_cannot_be_replaced_but_expired_owner_can(
    db: H2HDB,
) -> None:
    first_turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert first_turn is not None

    assert (
        db._claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=True,
        )
        is None
    )

    _expire_active_lease(db)
    assert db._renew_gallery_ingest(first_turn, lease_seconds=60) is False
    assert db._complete_gallery_ingest(first_turn) is False
    replacement_turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert replacement_turn is not None
    assert replacement_turn.claimed_from_phase == GalleryIngestPhase.ingesting
    assert replacement_turn.owner_token != first_turn.owner_token
    assert db._complete_gallery_ingest(first_turn) is False
    assert db._complete_gallery_ingest(replacement_turn) is True


@pytest.mark.parametrize("lease_seconds", [0, -1])
def test_download_turn_rejects_non_positive_lease(
    db: H2HDB,
    lease_seconds: int,
) -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        db.claim_download_turn(lease_seconds=lease_seconds)
