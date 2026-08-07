from datetime import UTC, datetime
from hashlib import sha256

import pytest

from h2hdb import (
    H2HDB,
    CatalogPublicationSelection,
    CatalogSnapshot,
    CoreConfig,
    DownloadCoordinator,
    GalleryIngestPhase,
    GallerySourceRecord,
)


@pytest.fixture
def coordinator(sqlite_config: CoreConfig) -> H2HDB:
    database = H2HDB(sqlite_config)
    database.migrate()
    assert isinstance(database, DownloadCoordinator)
    return database


def _complete_baseline_ingest(database: H2HDB) -> None:
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=False)
    assert turn is not None
    assert turn.claimed_from_phase is GalleryIngestPhase.ingest_requested
    assert database.complete_gallery_ingest(turn) is True


def _selection(gid: int, *, redownload_required: bool) -> CatalogPublicationSelection:
    return CatalogPublicationSelection(
        source_gallery_name=f"canonical-gallery-{gid}",
        redownload_required=redownload_required,
    )


def _source_record(gid: int, *, timestamp: datetime) -> GallerySourceRecord:
    return GallerySourceRecord(
        gallery_name=f"canonical-gallery-{gid}",
        gid=gid,
        title=f"Gallery {gid}",
        comment="",
        upload_account="",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=(),
        files=(),
        source_manifest_sha256=sha256(f"manifest:{gid}".encode()).hexdigest(),
    )


def test_durable_request_replacement_is_token_fenced(
    coordinator: H2HDB,
) -> None:
    original = coordinator.request_download(700_001, "https://example/old")
    replacement = coordinator.request_download(700_001)

    assert replacement.token != original.token
    assert replacement.url == original.url
    assert coordinator.get_download_requests() == [replacement]

    coordinator.complete_download_request(original)
    assert coordinator.get_download_request(original.gid) == replacement

    coordinator.complete_download_request(replacement)
    assert coordinator.get_download_request(replacement.gid) is None


def test_ensure_request_preserves_token_and_enriches_blank_url(
    coordinator: H2HDB,
) -> None:
    created = coordinator.ensure_download_request(700_002)
    ensured = coordinator.ensure_download_request(700_002, "https://example/gallery")

    assert created.created is True
    assert ensured.created is False
    assert ensured.request.token == created.request.token
    assert ensured.request.url == "https://example/gallery"


def test_missing_completion_cannot_acknowledge_a_newer_request(
    coordinator: H2HDB,
) -> None:
    stale = coordinator.request_download(700_003)
    current = coordinator.request_download(700_003, "https://example/current")

    coordinator.complete_missing_download_request(stale, stale.gid)
    assert coordinator.get_download_request(current.gid) == current

    coordinator.complete_missing_download_request(current, current.gid)
    assert coordinator.get_download_request(current.gid) is None
    coordinator.record_gallery_found(current.gid)


def test_candidate_state_is_one_public_snapshot(
    coordinator: H2HDB,
) -> None:
    ingest_turn = coordinator.claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    coordinator.publish_snapshot(
        CatalogSnapshot(
            galleries=(
                _source_record(710_001, timestamp=datetime.now(UTC)),
                _source_record(710_002, timestamp=datetime(2000, 1, 1)),
            ),
            selections=(
                # Deliberately opposite: downloader state comes from canonical
                # tables/views, not the revisioned OPDS projection.
                _selection(710_001, redownload_required=True),
                _selection(710_002, redownload_required=False),
            ),
        ),
        ingest_turn=ingest_turn,
    )
    assert coordinator.complete_gallery_ingest(ingest_turn)
    requested = coordinator.request_download(710_003)
    coordinator.request_download(710_002)

    states = coordinator.get_candidate_states(
        [710_001, 710_002, requested.gid, 710_004, 710_001]
    )

    assert list(states) == [710_001, 710_002, 710_003, 710_004]
    assert states[710_001].cataloged is True
    assert states[710_001].redownload_required is False
    assert states[710_001].requested is False
    assert states[710_002].cataloged is True
    assert states[710_002].redownload_required is True
    assert states[710_002].requested is True
    assert states[710_003].cataloged is False
    assert states[710_003].requested is True
    assert states[710_004].cataloged is False
    assert states[710_004].redownload_required is False
    assert states[710_004].requested is False
    assert coordinator.get_pending_redownload_gids() == [710_002]


def test_download_and_ingest_turns_preserve_generation_and_token_fences(
    coordinator: H2HDB,
) -> None:
    initial = coordinator.get_gallery_ingest_state()
    assert initial.phase is GalleryIngestPhase.ingest_requested
    assert initial.generation == 0
    assert coordinator.claim_download_turn(lease_seconds=60) is None

    _complete_baseline_ingest(coordinator)
    baseline = coordinator.get_gallery_ingest_state()
    assert baseline.phase is GalleryIngestPhase.ready
    assert baseline.completed_generation == 0

    request = coordinator.request_download(720_001)
    first_turn = coordinator.claim_download_turn(lease_seconds=60)
    assert first_turn is not None
    assert first_turn.generation == 1
    assert coordinator.renew_download_turn(first_turn, lease_seconds=60) is True
    assert coordinator.finish_download_turn(first_turn, request) is True
    assert coordinator.finish_download_turn(first_turn, request) is True
    assert coordinator.get_download_request(request.gid) is None

    requested = coordinator.get_gallery_ingest_state()
    assert requested.phase is GalleryIngestPhase.ingest_requested
    assert requested.generation == first_turn.generation
    ingest_turn = coordinator.claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    assert ingest_turn.generation == first_turn.generation
    renewed_until = coordinator.renew_gallery_ingest(
        ingest_turn,
        lease_seconds=60,
    )
    assert renewed_until is not None
    assert coordinator.complete_gallery_ingest(ingest_turn) is True
    assert coordinator.get_gallery_ingest_state().completed_generation == 1

    second_request = coordinator.request_download(720_002)
    second_turn = coordinator.claim_download_turn(lease_seconds=60)
    assert second_turn is not None
    assert second_turn.generation == 2
    assert coordinator.renew_download_turn(first_turn, lease_seconds=60) is False
    assert coordinator.request_gallery_ingest(first_turn) is False
    assert coordinator.finish_download_turn(second_turn, second_request) is True


def test_in_turn_completion_preserves_a_newer_request_token(
    coordinator: H2HDB,
) -> None:
    _complete_baseline_ingest(coordinator)
    stale = coordinator.request_download(730_001)
    turn = coordinator.claim_download_turn(lease_seconds=60)
    assert turn is not None
    current = coordinator.request_download(stale.gid, "https://example/current")

    assert coordinator.complete_download_request_in_turn(turn, stale) is True
    assert coordinator.get_download_request(stale.gid) == current
    assert coordinator.complete_download_request_in_turn(turn, current) is True
    assert coordinator.get_download_request(current.gid) is None
    assert coordinator.request_gallery_ingest(turn) is True


def test_deletion_requests_are_durable_and_idempotent(coordinator: H2HDB) -> None:
    coordinator.request_gallery_deletion(740_002)
    coordinator.request_gallery_deletion(740_001)
    coordinator.request_gallery_deletion(740_002)

    assert coordinator.get_gallery_deletion_requests() == [740_001, 740_002]


@pytest.mark.parametrize("lease_seconds", [0, -1])
def test_turn_claim_rejects_non_positive_lease(
    coordinator: H2HDB,
    lease_seconds: int,
) -> None:
    with pytest.raises(ValueError, match="lease_seconds"):
        coordinator.claim_download_turn(lease_seconds=lease_seconds)
