from __future__ import annotations

import inspect
from collections.abc import Callable, Generator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from vnext_publication_fixtures import (
    seed_catalog_publication,
    seed_catalog_publication_title,
    seed_publication_commit,
    seed_publication_identity,
)

import h2hdb
from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextCurrentProjectionContinuationError,
    VNextCurrentProjectionItem,
    VNextCurrentProjectionPage,
    VNextCurrentProjectionUnavailableError,
    VNextIngestFacade,
)
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.repository import RepositoryContext
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    CatalogArtifactFamily,
    ensure_catalog_artifact_family,
)
from h2hdb.vnext_current_projection_repository import (
    CurrentProjectionArtifactRepository,
    CurrentProjectionCursorError,
    CurrentProjectionReadError,
)

_CHANNEL = b"default"


def _database(path: Path) -> SQLiteConnector:
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


def _insert_finalization_batch(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    start_generation: int,
    batch_key: bytes,
    start_cursor: bytes,
    start_count: int,
    next_cursor: bytes,
    row_count: int,
    committed_at: int,
) -> None:
    key = (receipt_id, start_generation)
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_anchors "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_coordinates "
        "(receipt_id, batch_key, start_generation) VALUES (%s, %s, %s)",
        (receipt_id, batch_key, start_generation),
    )
    for table, column, value in (
        (
            "catalog_publication_finalization_batch_start_cursors",
            "start_cursor",
            start_cursor,
        ),
        (
            "catalog_publication_finalization_batch_start_counts",
            "start_processed_count",
            start_count,
        ),
        (
            "catalog_publication_finalization_batch_next_cursors",
            "next_cursor",
            next_cursor,
        ),
        (
            "catalog_publication_finalization_batch_row_counts",
            "row_count",
            row_count,
        ),
        (
            "catalog_publication_finalization_batch_committed_ats",
            "committed_at",
            committed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(receipt_id, start_generation, {column}) VALUES (%s, %s, %s)",
            (*key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_seals "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        key,
    )


def _mark_projection_finalized(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    publication_keys: tuple[bytes, ...],
    committed_at: int,
) -> None:
    cursor = max(publication_keys, default=b"")
    if publication_keys:
        _insert_finalization_batch(
            connector,
            receipt_id=receipt_id,
            start_generation=1,
            batch_key=b"artifact-items",
            start_cursor=b"",
            start_count=0,
            next_cursor=cursor,
            row_count=len(publication_keys),
            committed_at=committed_at + 1,
        )
        terminal_generation = 2
        checkpoint_generation = 3
    else:
        terminal_generation = 1
        checkpoint_generation = 2
    _insert_finalization_batch(
        connector,
        receipt_id=receipt_id,
        start_generation=terminal_generation,
        batch_key=b"artifact-terminal",
        start_cursor=cursor,
        start_count=len(publication_keys),
        next_cursor=cursor,
        row_count=0,
        committed_at=committed_at + 2,
    )
    for table, column, value in (
        (
            "catalog_publication_finalization_checkpoint_generations",
            "generation",
            checkpoint_generation,
        ),
        (
            "catalog_publication_finalization_checkpoint_cursors",
            "cursor",
            cursor,
        ),
        (
            "catalog_publication_finalization_checkpoint_counts",
            "processed_count",
            len(publication_keys),
        ),
        (
            "catalog_publication_finalization_checkpoint_states",
            "state",
            "COMPLETE",
        ),
        (
            "catalog_publication_finalization_checkpoint_updated_ats",
            "updated_at",
            committed_at + 2,
        ),
    ):
        sql_column = f"`{column}`" if column == "cursor" else column
        connector.execute(
            f"UPDATE {table} SET {sql_column} = %s WHERE receipt_id = %s",
            (value, receipt_id),
        )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) "
        "VALUES (%s)",
        (receipt_id,),
    )


def _seed_projection(
    connector: SQLiteConnector,
    *,
    revision: int,
    gids: tuple[int, ...],
    receipt_id: bytes,
    finalized: bool = False,
    channel: bytes = _CHANNEL,
) -> dict[bytes, tuple[int, str, int, tuple[str, ...], bytes, int]]:
    source_revision = revision
    committed_at = 1_000_000 + revision
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
            (revision,),
        )
        connector.execute(
            "INSERT INTO catalog_revision_publication_counts "
            "(revision, publication_count) VALUES (%s, %s)",
            (revision, len(gids)),
        )
        connector.execute(
            "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (%s)",
            (revision,),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (%s)",
            (source_revision,),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_channels "
            "(source_revision, channel) VALUES (%s, %s)",
            (source_revision, channel),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_snapshot_manifests "
            "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
            (source_revision, sha256(f"snapshot-{revision}".encode()).digest()),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptor_seals "
            "(source_revision) VALUES (%s)",
            (source_revision,),
        )
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=revision.to_bytes(16, "big"),
            revision=revision,
            source_revision=source_revision,
            generation=revision,
            preparation_id=(revision + 100).to_bytes(16, "big"),
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=len(gids),
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=committed_at,
        )

        expected: dict[
            bytes,
            tuple[int, str, int, tuple[str, ...], bytes, int],
        ] = {}
        for position, gid in enumerate(gids):
            publication_key = identity.publication_key(gid)
            upload_time = 2_000_000 + gid
            source_name = f"gallery-{gid}"
            artifact_payload = f"artifact-{revision}-{gid}".encode()
            artifact_sha256 = sha256(artifact_payload).digest()
            locator_components = identity.artifact_locator_components(artifact_sha256)
            locator_sha256 = identity.artifact_locator_digest(locator_components)
            semantics_sha256 = sha256(f"semantics-{revision}-{gid}".encode()).digest()
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, %s)",
                (gid, upload_time),
            )
            seed_publication_identity(connector, gid=gid)
            seed_catalog_publication(
                connector,
                revision=revision,
                publication_key=publication_key,
                gallery_id=revision * 10_000 + position + 1,
                summary_sha256=sha256(f"summary-{revision}-{gid}".encode()).digest(),
                language_sha256=sha256(b"language-zh").digest(),
                modified_at=3_000_000 + gid,
            )
            seed_catalog_publication_title(
                connector,
                revision=revision,
                publication_key=publication_key,
                source_title_sha256=sha256(
                    f"source-title-{revision}-{gid}".encode()
                ).digest(),
                source_gallery_name=source_name.encode(),
            )
            connector.execute(
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                "VALUES (%s, %s, %s)",
                (artifact_sha256, len(artifact_payload), locator_sha256),
            )
            ensure_catalog_artifact_family(
                connector,
                CatalogArtifactFamily(
                    revision,
                    publication_key,
                    artifact_sha256,
                    semantics_sha256,
                ),
            )
            expected[publication_key] = (
                gid,
                source_name,
                upload_time,
                locator_components,
                artifact_sha256,
                len(artifact_payload),
            )
        publication_keys = tuple(sorted(expected))
        if finalized:
            _mark_projection_finalized(
                connector,
                receipt_id=receipt_id,
                publication_keys=publication_keys,
                committed_at=committed_at,
            )
        connector.execute(
            "INSERT OR REPLACE INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (channel, receipt_id),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    expected_state = "PROJECTION_FINALIZED" if finalized else "DB_COMMITTED"
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (receipt_id,),
    ) == (expected_state,)
    return expected


def test_db_committed_projection_pages_are_bounded_and_empty_terminal(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "bounded.sqlite3")
    try:
        receipt_id = b"r" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(101, 102, 103, 104),
            receipt_id=receipt_id,
        )
        ordered_keys = tuple(sorted(expected))

        first = CurrentProjectionArtifactRepository.list_page(
            connector,
            page_limit=2,
        )
        assert (first.receipt_id, first.catalog_revision) == (receipt_id, 1)
        assert tuple(item.publication_key for item in first.items) == ordered_keys[:2]
        assert first.next_cursor == ordered_keys[1]
        assert not first.terminal

        second = CurrentProjectionArtifactRepository.list_page(
            connector,
            receipt_id=first.receipt_id,
            cursor=first.next_cursor,
            page_limit=2,
        )
        assert tuple(item.publication_key for item in second.items) == ordered_keys[2:]
        assert not second.terminal
        for item in (*first.items, *second.items):
            assert (
                item.gid,
                item.source_gallery_name,
                item.upload_time,
                item.artifact_locator_components,
                item.artifact_sha256,
                item.size_bytes,
            ) == expected[item.publication_key]

        terminal = CurrentProjectionArtifactRepository.list_page(
            connector,
            receipt_id=second.receipt_id,
            cursor=second.next_cursor,
            page_limit=2,
        )
        assert terminal.receipt_id == receipt_id
        assert terminal.catalog_revision == 1
        assert terminal.items == ()
        assert terminal.next_cursor is None
        assert terminal.terminal
    finally:
        connector.close()


def test_continuation_stays_on_receipt_after_current_head_advances(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "head-advance.sqlite3")
    try:
        old_receipt = b"o" * 16
        old_expected = _seed_projection(
            connector,
            revision=1,
            gids=(201, 202),
            receipt_id=old_receipt,
        )
        first = CurrentProjectionArtifactRepository.list_page(
            connector,
            page_limit=1,
        )
        new_receipt = b"n" * 16
        new_expected = _seed_projection(
            connector,
            revision=2,
            gids=(301,),
            receipt_id=new_receipt,
        )

        continued = CurrentProjectionArtifactRepository.list_page(
            connector,
            receipt_id=first.receipt_id,
            cursor=first.next_cursor,
            page_limit=1,
        )
        assert continued.receipt_id == old_receipt
        assert continued.catalog_revision == 1
        assert (
            tuple(item.publication_key for item in continued.items)
            == tuple(sorted(old_expected))[1:]
        )

        current = CurrentProjectionArtifactRepository.list_page(connector)
        assert current.receipt_id == new_receipt
        assert current.catalog_revision == 2
        assert tuple(item.publication_key for item in current.items) == tuple(
            sorted(new_expected)
        )
    finally:
        connector.close()


def test_finalized_receipt_remains_replayable(tmp_path: Path) -> None:
    connector = _database(tmp_path / "finalized.sqlite3")
    try:
        receipt_id = b"f" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(401,),
            receipt_id=receipt_id,
            finalized=True,
        )
        page = CurrentProjectionArtifactRepository.list_page(
            connector,
            receipt_id=receipt_id,
        )
        assert tuple(item.publication_key for item in page.items) == tuple(expected)
    finally:
        connector.close()


def test_cursor_and_page_bounds_fail_closed(tmp_path: Path) -> None:
    connector = _database(tmp_path / "cursor.sqlite3")
    try:
        receipt_id = b"r" * 16
        _seed_projection(
            connector,
            revision=1,
            gids=(501,),
            receipt_id=receipt_id,
        )
        with pytest.raises(ValueError, match="must not exceed 128"):
            CurrentProjectionArtifactRepository.list_page(connector, page_limit=129)
        with pytest.raises(ValueError, match="must be in 1"):
            CurrentProjectionArtifactRepository.list_page(connector, page_limit=0)
        with pytest.raises(ValueError, match="exactly 32 bytes|contain 32"):
            CurrentProjectionArtifactRepository.list_page(
                connector,
                receipt_id=receipt_id,
                cursor=b"short",
            )
        with pytest.raises(CurrentProjectionCursorError, match="initial"):
            CurrentProjectionArtifactRepository.list_page(
                connector,
                cursor=b"x" * 32,
            )
        with pytest.raises(CurrentProjectionCursorError, match="exact artifact"):
            CurrentProjectionArtifactRepository.list_page(
                connector,
                receipt_id=receipt_id,
                cursor=b"x" * 32,
            )
        with pytest.raises(CurrentProjectionCursorError, match="another channel"):
            CurrentProjectionArtifactRepository.list_page(
                connector,
                channel=b"other",
                receipt_id=receipt_id,
            )
    finally:
        connector.close()


@pytest.mark.parametrize("corruption", ("title-seal", "locator"))
def test_sealed_projection_corruption_is_not_silently_omitted(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _database(tmp_path / f"corrupt-{corruption}.sqlite3")
    try:
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(601,),
            receipt_id=b"r" * 16,
        )
        publication_key = next(iter(expected))
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            if corruption == "title-seal":
                connector.execute(
                    "DELETE FROM catalog_publication_title_seals "
                    "WHERE revision = 1 AND publication_key = %s",
                    (publication_key,),
                )
            else:
                artifact_sha256 = expected[publication_key][4]
                connector.execute(
                    "UPDATE catalog_artifact_blobs "
                    "SET artifact_locator_sha256 = %s WHERE artifact_sha256 = %s",
                    (b"z" * 32, artifact_sha256),
                )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(CurrentProjectionReadError, match="corrupt durable facts"):
            CurrentProjectionArtifactRepository.list_page(connector)
    finally:
        connector.close()


def test_sqlite_page_scan_uses_revision_publication_key_primary_index(
    tmp_path: Path,
) -> None:
    import h2hdb.vnext_current_projection_repository as module

    connector = _database(tmp_path / "explain.sqlite3")
    try:
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(701, 702),
            receipt_id=b"r" * 16,
        )
        first_key = min(expected)
        for cursor, expected_constraint in (
            (None, "(revision=?)"),
            (first_key, "(revision=? AND publication_key>?)"),
        ):
            query, parameters = module._artifact_page_query(
                revision=1,
                cursor=cursor,
                page_limit=128,
            )
            plan = connector.fetch_all("EXPLAIN QUERY PLAN " + query, parameters)
            details = tuple(str(row[3]) for row in plan)
            artifact_scan = tuple(
                detail
                for detail in details
                if "artifact" in detail and "SEARCH" in detail
            )
            assert any(
                "USING INDEX sqlite_autoindex_catalog_artifacts_1" in detail
                and expected_constraint in detail
                for detail in artifact_scan
            ), details
    finally:
        connector.close()


def test_public_facade_pins_continuation_across_head_advance_and_maps_all_fields(
    tmp_path: Path,
) -> None:
    path = tmp_path / "facade-head-advance.sqlite3"
    connector = _database(path)
    try:
        old_receipt = b"o" * 16
        old_expected = _seed_projection(
            connector,
            revision=1,
            gids=(801, 802),
            receipt_id=old_receipt,
            channel=b"special",
        )
    finally:
        connector.close()
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )

    first = facade.list_current_projection_artifacts(
        channel=b"special",
        page_limit=1,
    )

    assert isinstance(first, VNextCurrentProjectionPage)
    assert type(first.items[0]) is VNextCurrentProjectionItem
    assert first.receipt_id == old_receipt
    assert first.catalog_revision == 1
    assert first.next_cursor is not None
    first_item = first.items[0]
    assert (
        first_item.gid,
        first_item.source_gallery_name,
        first_item.upload_time,
        first_item.artifact_locator_components,
        first_item.artifact_sha256,
        first_item.size_bytes,
    ) == old_expected[first_item.publication_key]

    head_connector = SQLiteConnector(str(path))
    head_connector.connect()
    try:
        new_receipt = b"n" * 16
        new_expected = _seed_projection(
            head_connector,
            revision=2,
            gids=(901,),
            receipt_id=new_receipt,
            channel=b"special",
        )
    finally:
        head_connector.close()

    continued = facade.continue_current_projection_artifacts(
        first.receipt_id,
        first.next_cursor,
    )
    assert continued.receipt_id == old_receipt
    assert continued.catalog_revision == 1
    assert (
        tuple(item.publication_key for item in continued.items)
        == tuple(sorted(old_expected))[1:]
    )

    current = facade.list_current_projection_artifacts(channel=b"special")
    assert current.receipt_id == new_receipt
    assert current.catalog_revision == 2
    assert tuple(item.publication_key for item in current.items) == tuple(
        sorted(new_expected)
    )


def test_public_facade_projection_authority_fails_closed_and_is_top_level(
    tmp_path: Path,
) -> None:
    path = tmp_path / "facade-forgery.sqlite3"
    connector = _database(path)
    try:
        receipt_id = b"r" * 16
        _seed_projection(
            connector,
            revision=1,
            gids=(1_001,),
            receipt_id=receipt_id,
        )
    finally:
        connector.close()
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )

    with pytest.raises(
        VNextCurrentProjectionContinuationError,
        match="exact artifact",
    ):
        facade.continue_current_projection_artifacts(receipt_id, b"x" * 32)
    with pytest.raises(
        VNextCurrentProjectionUnavailableError,
        match="unavailable",
    ):
        facade.continue_current_projection_artifacts(b"z" * 16, b"x" * 32)

    assert {
        "VNextCurrentProjectionItem",
        "VNextCurrentProjectionPage",
        "VNextCurrentProjectionContinuationError",
        "VNextCurrentProjectionUnavailableError",
    } <= set(h2hdb.__all__)
    assert tuple(
        inspect.signature(
            VNextIngestFacade.list_current_projection_artifacts
        ).parameters
    ) == ("self", "channel", "page_limit")
    assert tuple(
        inspect.signature(
            VNextIngestFacade.continue_current_projection_artifacts
        ).parameters
    ) == ("self", "receipt_id", "next_cursor")


class _ProjectionReadRecorder:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.selects: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> _ProjectionReadRecorder:
        self.events.append("connect")
        return self

    def __exit__(self, *args: object) -> None:
        self.events.append("close")

    @contextmanager
    def read_transaction(self) -> Generator[None]:
        self.events.append("begin-read-only-snapshot")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    def fetch_one(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> tuple[object, ...]:
        self.selects.append((query, data))
        return (b"r" * 16, 1, b"special", "DB_COMMITTED")

    def fetch_all(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> list[tuple[object, ...]]:
        self.selects.append((query, data))
        return []


class _ProjectionFacadeContext:
    def __init__(self, recorder: _ProjectionReadRecorder) -> None:
        self.sql_type = "mariadb"
        self.SQLConnector: Callable[[], _ProjectionReadRecorder] = lambda: recorder


def test_public_projection_facade_owns_mariadb_read_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _ProjectionReadRecorder()
    config = CoreConfig(database=DatabaseConfig(sql_type="mariadb", database="unused"))
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: _ProjectionFacadeContext(recorder)),
    )

    page = VNextIngestFacade(config).list_current_projection_artifacts(
        channel=b"special",
        page_limit=7,
    )

    assert page == VNextCurrentProjectionPage(b"r" * 16, 1, (), None, True)
    assert recorder.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "close",
    ]
    assert len(recorder.selects) == 2
    assert recorder.selects[0][1] == (b"special",)
    assert recorder.selects[1][1] == (1, 7)
    assert all("FOR UPDATE" not in query for query, _data in recorder.selects)
