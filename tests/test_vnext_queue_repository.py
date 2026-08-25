from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

import h2hdb.vnext_queue_repository as queue_repository_module
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_queue_repository import (
    DeletionGenerationExhaustedError,
    PendingRedownloadCursor,
    PendingRedownloadCursorError,
    QueueIdentityConflictError,
    VNextQueueRepository,
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
    return connector


class _MariaQueueRecorder:
    def __init__(self) -> None:
        self.selects: list[tuple[str, tuple[object, ...]]] = []
        self.mutations: list[tuple[str, tuple[object, ...]]] = []

    def fetch_one(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> tuple[object, ...]:
        self.selects.append((query, data))
        return ()

    def execute(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> None:
        self.mutations.append((query, data))


def _seed_current_catalog_candidates(
    connector: SQLiteConnector,
    rows: tuple[tuple[int, int, int], ...],
    *,
    unmapped_redownloads: tuple[tuple[int, int], ...] = (),
) -> None:
    """Seed only the sealed authority graph needed by candidate reads."""

    receipt_id = b"r" * 16
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
            (receipt_id,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", receipt_id),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_catalog_revisions "
            "(receipt_id, revision) VALUES (%s, %s)",
            (receipt_id, 1),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_source_revisions "
            "(receipt_id, source_revision) VALUES (%s, %s)",
            (receipt_id, 1),
        )
        for gid, gallery_id, redownload_at in rows:
            publication_key = identity.publication_key(gid)
            occurrence = identity.catalog_publication_occurrence_sha256(
                1, publication_key
            )
            source_name = f"gallery-{gallery_id}".encode()
            connector.execute(
                "INSERT INTO catalog_publication_identities "
                "(publication_key, gid) VALUES (%s, %s)",
                (publication_key, gid),
            )
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids "
                "(source_gallery_name, gid) VALUES (%s, %s)",
                (source_name, gid),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_source_name_accesses "
                "(gallery_id, source_gallery_name) VALUES (%s, %s)",
                (gallery_id, source_name),
            )
            connector.execute(
                "INSERT INTO catalog_publication_occurrence_identities "
                "(catalog_occurrence_sha256, revision, publication_key) "
                "VALUES (%s, %s, %s)",
                (occurrence, 1, publication_key),
            )
            connector.execute(
                "INSERT INTO catalog_publication_storage "
                "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
                "language_sha256, modified_at, source_title_sha256) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    occurrence,
                    gallery_id,
                    b"s" * 32,
                    b"l" * 32,
                    redownload_at,
                    b"t" * 32,
                ),
            )
            connector.execute(
                "INSERT INTO operational_gallery_redownload_states "
                "(gallery_id, redownload_at, through_source_revision, updated_at) "
                "VALUES (%s, %s, %s, %s)",
                (gallery_id, redownload_at, 1, redownload_at),
            )
        for gallery_id, redownload_at in unmapped_redownloads:
            connector.execute(
                "INSERT INTO operational_gallery_redownload_states "
                "(gallery_id, redownload_at, through_source_revision, updated_at) "
                "VALUES (%s, %s, %s, %s)",
                (gallery_id, redownload_at, 1, redownload_at),
            )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def test_download_request_replace_is_fresh_token_fenced_and_preserves_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "download-replace.sqlite3")
    tokens = iter((b"a" * 16, b"b" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: next(tokens),
    )
    try:
        with connector.transaction():
            first = VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                url="https://example.invalid/gallery/42",
                requested_at=10,
            )
        with connector.transaction():
            replacement = VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                url="",
                requested_at=11,
            )

        assert first.request_token == b"a" * 16
        assert replacement.request_token == b"b" * 16
        assert replacement.url == first.url
        assert replacement.requested_at == 11
        assert connector.fetch_one(
            "SELECT url, request_token, requested_at "
            "FROM operational_download_requests WHERE gid = 42"
        ) == (replacement.url, replacement.request_token, 11)

        with connector.transaction():
            assert not VNextQueueRepository.complete_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                request=first,
            )
        with connector.transaction():
            assert VNextQueueRepository.complete_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                request=replacement,
            )
        assert (
            connector.fetch_one(
                "SELECT gid FROM operational_download_requests WHERE gid = 42"
            )
            == ()
        )
    finally:
        connector.close()


def test_ensure_download_request_fills_only_empty_url_and_pages_by_gid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "download-ensure.sqlite3")
    tokens = iter((b"a" * 16, b"b" * 16, b"c" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: next(tokens),
    )
    try:
        with connector.transaction():
            created = VNextQueueRepository.ensure_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                requested_at=10,
            )
        assert created.created
        assert created.request.url == ""

        with connector.transaction():
            filled = VNextQueueRepository.ensure_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                url="https://example.invalid/42",
                requested_at=99,
            )
        assert not filled.created
        assert filled.request.request_token == created.request.request_token
        assert filled.request.requested_at == 10
        assert filled.request.url == "https://example.invalid/42"

        with connector.transaction():
            preserved = VNextQueueRepository.ensure_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                url="https://forged.invalid/42",
                requested_at=100,
            )
            second = VNextQueueRepository.ensure_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=100,
                requested_at=12,
            )
        assert preserved.request == filled.request
        assert second.created

        with connector.read_transaction():
            page_one = VNextQueueRepository.list_download_requests(
                VNextUnitOfWork(connector, backend="sqlite"),
                limit=1,
            )
            page_two = VNextQueueRepository.list_download_requests(
                VNextUnitOfWork(connector, backend="sqlite"),
                after_gid=page_one[-1].gid,
                limit=1,
            )
            fetched = VNextQueueRepository.get_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
            )
        assert tuple(value.gid for value in page_one + page_two) == (42, 100)
        assert fetched == filled.request
    finally:
        connector.close()


def test_candidate_states_use_current_revision_scoped_redownload_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "download-candidates.sqlite3")
    _seed_current_catalog_candidates(
        connector,
        (
            (42, 7, 90),
            (43, 8, 110),
        ),
    )
    try:
        with connector.transaction():
            request = VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=44,
                requested_at=1,
            )
        with connector.read_transaction():
            states = VNextQueueRepository.get_candidate_states(
                VNextUnitOfWork(connector, backend="sqlite"),
                gids=(44, 42, 43, 42),
                now=100,
            )

        assert tuple(states) == (44, 42, 43)
        assert states[42].cataloged
        assert states[42].redownload_required
        assert not states[42].requested
        assert states[43].cataloged
        assert not states[43].redownload_required
        assert not states[43].requested
        assert not states[44].cataloged
        assert not states[44].redownload_required
        assert states[44].requested
        assert request.gid == 44

        connector.execute(
            "INSERT INTO operational_removed_gids (gid) VALUES (%s)",
            (42,),
        )
        with connector.read_transaction():
            removed = VNextQueueRepository.get_candidate_states(
                VNextUnitOfWork(connector, backend="sqlite"),
                gids=(42,),
                now=100,
            )
        assert not removed[42].redownload_required

        with connector.transaction():
            assert (
                VNextQueueRepository.record_galleries_found(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gids=(42, 42),
                )
                == 1
            )
        with connector.read_transaction():
            found = VNextQueueRepository.get_candidate_states(
                VNextUnitOfWork(connector, backend="sqlite"),
                gids=(42,),
                now=100,
            )
        assert found[42].redownload_required

        with connector.transaction():
            VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                request_token=b"d" * 16,
                url=None,
                requested_at=101,
            )
        with connector.read_transaction():
            deleting = VNextQueueRepository.get_candidate_states(
                VNextUnitOfWork(connector, backend="sqlite"),
                gids=(42,),
                now=100,
            )
        assert not deleting[42].redownload_required
    finally:
        connector.close()


def test_candidate_states_are_hard_bounded_before_sql(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "download-candidate-bound.sqlite3")
    try:
        with pytest.raises(ValueError, match="must not exceed 256"):
            with connector.read_transaction():
                VNextQueueRepository.get_candidate_states(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gids=tuple(range(1, 258)),
                    now=1,
                )
    finally:
        connector.close()


def test_pending_redownload_pages_pin_snapshot_and_advance_by_scanned_row(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "pending-redownload-page.sqlite3")
    _seed_current_catalog_candidates(
        connector,
        (
            (42, 7, 90),
            (43, 8, 100),
            (44, 10, 105),
            (45, 11, 110),
        ),
        unmapped_redownloads=((9, 95),),
    )
    try:
        connector.execute(
            "INSERT INTO operational_removed_gids (gid) VALUES (%s)",
            (43,),
        )
        with connector.transaction():
            VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=44,
                request_token=b"d" * 16,
                url=None,
                requested_at=106,
            )

        with connector.read_transaction():
            first = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                limit=1,
                now=106,
            )
        assert first.catalog_revision == 1
        assert first.source_revision == 1
        assert first.cutoff_at == 106
        assert first.gids == (42,)
        assert first.next_cursor == PendingRedownloadCursor(1, 1, 106, 90, 7)
        assert not first.terminal

        with connector.read_transaction():
            second = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                cursor=first.next_cursor,
                limit=1,
            )
        assert second.gids == ()
        assert second.next_cursor == PendingRedownloadCursor(1, 1, 106, 95, 9)
        assert not second.terminal

        with connector.read_transaction():
            third = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                cursor=second.next_cursor,
                limit=1,
            )
        assert third.gids == ()
        assert third.next_cursor == PendingRedownloadCursor(1, 1, 106, 100, 8)
        assert not third.terminal

        with connector.read_transaction():
            terminal = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                cursor=third.next_cursor,
                limit=1,
            )
        assert terminal.gids == ()
        assert terminal.next_cursor is None
        assert terminal.terminal
        assert terminal.cutoff_at == 106
    finally:
        connector.close()


def test_pending_redownload_page_without_current_head_is_empty_and_terminal(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "pending-redownload-empty.sqlite3")
    try:
        with connector.read_transaction():
            page = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=123,
            )
        assert page.catalog_revision == 0
        assert page.source_revision == 0
        assert page.cutoff_at == 123
        assert page.gids == ()
        assert page.next_cursor is None
        assert page.terminal
    finally:
        connector.close()


def test_pending_redownload_cursor_is_exact_and_page_size_is_bounded(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "pending-redownload-cursor.sqlite3")
    _seed_current_catalog_candidates(connector, ((42, 7, 90), (43, 8, 91)))
    try:
        with connector.read_transaction():
            first = VNextQueueRepository.list_pending_redownloads(
                VNextUnitOfWork(connector, backend="sqlite"),
                limit=1,
                now=100,
            )
        assert first.next_cursor is not None

        forged_revision = PendingRedownloadCursor(2, 1, 100, 90, 7)
        with pytest.raises(
            PendingRedownloadCursorError,
            match="sealed publication commit",
        ):
            with connector.read_transaction():
                VNextQueueRepository.list_pending_redownloads(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    cursor=forged_revision,
                )

        forged_position = PendingRedownloadCursor(1, 1, 100, 90, 999)
        with pytest.raises(
            PendingRedownloadCursorError,
            match="durable schedule position",
        ):
            with connector.read_transaction():
                VNextQueueRepository.list_pending_redownloads(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    cursor=forged_position,
                )

        with pytest.raises(ValueError, match="must not exceed 256"):
            with connector.read_transaction():
                VNextQueueRepository.list_pending_redownloads(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    limit=257,
                    now=100,
                )
    finally:
        connector.close()


@pytest.mark.parametrize("after", [None, (90, 7)])
def test_pending_redownload_sqlite_plan_uses_composite_schedule_index(
    tmp_path: Path,
    after: tuple[int, int] | None,
) -> None:
    connector = _generated_database(tmp_path / f"pending-plan-{after is None}.sqlite3")
    try:
        query, parameters = queue_repository_module._pending_redownload_scan_query(
            source_revision=1,
            cutoff=100,
            catalog_revision=1,
            scan_limit=257,
            after=after,
        )
        plan = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", parameters)
        details = "\n".join(str(row[-1]) for row in plan)
        assert (
            "operational_gallery_redownload_states USING COVERING INDEX "
            "ix_gallery_redownload_state_fk_2" in details
        ), details
    finally:
        connector.close()


def test_confirmed_missing_is_exact_request_fenced_and_reversible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "download-missing.sqlite3")
    tokens = iter((b"a" * 16, b"b" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: next(tokens),
    )
    try:
        with connector.transaction():
            stale = VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=404,
                requested_at=1,
            )
        with connector.transaction():
            current = VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=404,
                requested_at=2,
            )
        with connector.transaction():
            assert not VNextQueueRepository.complete_missing_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                request=stale,
                missing_gid=404,
            )
        assert (
            connector.fetch_one(
                "SELECT gid FROM operational_removed_gids WHERE gid = %s",
                (404,),
            )
            == ()
        )

        with connector.transaction():
            assert VNextQueueRepository.complete_missing_download_request(
                VNextUnitOfWork(connector, backend="sqlite"),
                request=current,
                missing_gid=404,
            )
        assert connector.fetch_one(
            "SELECT gid FROM operational_removed_gids WHERE gid = %s",
            (404,),
        ) == (404,)

        with connector.transaction():
            assert (
                VNextQueueRepository.record_galleries_found(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gids=(404,),
                )
                == 1
            )
        assert (
            connector.fetch_one(
                "SELECT gid FROM operational_removed_gids WHERE gid = %s",
                (404,),
            )
            == ()
        )
    finally:
        connector.close()


def test_generated_download_token_collision_is_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "download-token-collision.sqlite3")
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: b"a" * 16,
    )
    try:
        with connector.transaction():
            VNextQueueRepository.request_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=1,
                requested_at=1,
            )
        with pytest.raises(QueueIdentityConflictError, match="already durable"):
            with connector.transaction():
                VNextQueueRepository.request_download(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gid=2,
                    requested_at=2,
                )
        assert connector.fetch_all(
            "SELECT gid, request_token FROM operational_download_requests"
        ) == [(1, b"a" * 16)]
    finally:
        connector.close()


def test_download_request_mariadb_locks_gid_and_token_before_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _MariaQueueRecorder()
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: b"a" * 16,
    )
    receipt = VNextQueueRepository.request_download(
        VNextUnitOfWork(cast(Any, recorder), backend="mariadb"),
        gid=42,
        url="https://example.invalid/42",
        requested_at=10,
    )
    assert receipt.request_token == b"a" * 16
    assert len(recorder.selects) == 2
    assert all(query.endswith(" FOR UPDATE") for query, _data in recorder.selects)
    assert "WHERE gid = %s FOR UPDATE" in recorder.selects[0][0]
    assert "WHERE request_token = %s FOR UPDATE" in recorder.selects[1][0]
    assert len(recorder.mutations) == 1
    assert "INSERT INTO operational_download_requests" in recorder.mutations[0][0]


def test_deletion_request_advances_global_generation_and_replays_exactly(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "queue.sqlite3")
    token = b"a" * 16
    try:
        with connector.transaction():
            receipt = VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                request_token=token,
                url="https://example.invalid/gallery/42",
                requested_at=10,
            )
        assert receipt.created is True
        assert receipt.observed_generation == 1

        before = {
            table: connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0]
            for table in (
                "operational_deletion_request_generations",
                "operational_deletion_request_attempts",
                "operational_deletion_request_urls",
                "operational_deletion_request_heads",
            )
        }
        with connector.transaction():
            replay = VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                request_token=token,
                url="https://example.invalid/gallery/42",
                requested_at=10,
            )
        assert replay.created is False
        assert replay.current is True
        assert replay.observed_generation == 1
        assert before == {
            table: connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0]
            for table in before
        }
    finally:
        connector.close()


def test_request_rotation_and_global_cross_gid_generation(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "queue-rotation.sqlite3")
    try:
        for gid, token, url, timestamp in (
            (42, b"a" * 16, None, 10),
            (7, b"b" * 16, "", 11),
            (42, b"c" * 16, "replacement", 12),
        ):
            with connector.transaction():
                receipt = VNextQueueRepository.request_deletion(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gid=gid,
                    request_token=token,
                    url=url,
                    requested_at=timestamp,
                )
        assert receipt.observed_generation == 3
        assert connector.fetch_all(
            "SELECT generation FROM operational_deletion_request_generations "
            "ORDER BY generation"
        ) == [(0,), (1,), (2,), (3,)]
        assert connector.fetch_all(
            "SELECT gid, request_token FROM operational_deletion_request_heads "
            "ORDER BY gid"
        ) == [(7, b"b" * 16), (42, b"c" * 16)]
        assert connector.fetch_all(
            "SELECT request_token, url FROM operational_deletion_request_urls "
            "ORDER BY request_token"
        ) == [(b"b" * 16, ""), (b"c" * 16, "replacement")]
    finally:
        connector.close()


def test_token_collision_rolls_back_without_advancing_generation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "queue-collision.sqlite3")
    token = b"a" * 16
    try:
        with connector.transaction():
            VNextQueueRepository.request_deletion(
                VNextUnitOfWork(connector, backend="sqlite"),
                gid=42,
                request_token=token,
                url=None,
                requested_at=10,
            )
        with pytest.raises(QueueIdentityConflictError):
            with connector.transaction():
                VNextQueueRepository.request_deletion(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gid=43,
                    request_token=token,
                    url=None,
                    requested_at=10,
                )
        assert connector.fetch_one(
            "SELECT current_generation "
            "FROM operational_deletion_request_generation_heads"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_deletion_request_attempts"
        ) == (1,)
    finally:
        connector.close()


def test_generation_exhaustion_is_zero_write(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "queue-exhausted.sqlite3")
    try:
        connector.execute(
            "INSERT INTO operational_deletion_request_generations VALUES (%s, %s)",
            (INT63_MAX, 1),
        )
        connector.execute(
            "UPDATE operational_deletion_request_generation_heads "
            "SET current_generation = %s, updated_at = %s",
            (INT63_MAX, 1),
        )
        with pytest.raises(DeletionGenerationExhaustedError):
            with connector.transaction():
                VNextQueueRepository.request_deletion(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gid=42,
                    request_token=b"a" * 16,
                    url=None,
                    requested_at=2,
                )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_deletion_request_attempts"
        ) == (0,)
        assert connector.fetch_one(
            "SELECT current_generation, updated_at "
            "FROM operational_deletion_request_generation_heads"
        ) == (INT63_MAX, 1)
    finally:
        connector.close()
