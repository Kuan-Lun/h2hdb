from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_queue_repository import (
    DeletionGenerationExhaustedError,
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
