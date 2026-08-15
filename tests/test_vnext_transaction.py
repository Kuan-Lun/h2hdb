from __future__ import annotations

from pathlib import Path

import pytest

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import (
    LockOrderViolationError,
    LockRank,
    StaleWriteError,
    VNextUnitOfWork,
    encode_lock_key,
)


def test_lock_keys_are_typed_length_framed_and_orderable() -> None:
    assert encode_lock_key("head", 1, b"a") == (
        b"s\x00\x00\x00\x04head"
        b"i\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01"
        b"b\x00\x00\x00\x01a"
    )
    assert encode_lock_key(b"a") != encode_lock_key("a")
    with pytest.raises(TypeError):
        encode_lock_key(True)


def test_lock_order_rejects_rank_and_same_rank_inversions(tmp_path: Path) -> None:
    connector = SQLiteConnector(database=str(tmp_path / "locks.sqlite3"))
    with connector:
        connector.execute("CREATE TABLE authority (id INTEGER PRIMARY KEY)")
        connector.execute("INSERT INTO authority VALUES (%s)", (1,))
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            assert work.lock_row(
                LockRank.INGEST_FENCE,
                encode_lock_key(1),
                "SELECT id FROM authority WHERE id = %s",
                (1,),
            ) == (1,)
            work.lock_row(
                LockRank.HEAD,
                encode_lock_key("b"),
                "SELECT id FROM authority WHERE id = %s",
                (1,),
            )
            with pytest.raises(LockOrderViolationError):
                work.lock_row(
                    LockRank.CHECKPOINT,
                    encode_lock_key(1),
                    "SELECT id FROM authority WHERE id = %s",
                    (1,),
                )

        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            work.lock_row(
                LockRank.HEAD,
                encode_lock_key("b"),
                "SELECT id FROM authority WHERE id = %s",
                (1,),
            )
            with pytest.raises(LockOrderViolationError):
                work.lock_row(
                    LockRank.HEAD,
                    encode_lock_key("a"),
                    "SELECT id FROM authority WHERE id = %s",
                    (1,),
                )


def test_compare_and_swap_is_exact_and_transactional(tmp_path: Path) -> None:
    connector = SQLiteConnector(database=str(tmp_path / "cas.sqlite3"))
    with connector:
        connector.execute(
            "CREATE TABLE allocator (stream TEXT PRIMARY KEY, next_id INTEGER)"
        )
        connector.execute("INSERT INTO allocator VALUES (%s, %s)", ("GALLERY", 1))
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            work.compare_and_swap(
                "UPDATE allocator SET next_id = %s WHERE stream = %s AND next_id = %s",
                (2, "GALLERY", 1),
                authority="gallery allocator",
            )
        assert connector.fetch_one("SELECT next_id FROM allocator") == (2,)

        with pytest.raises(StaleWriteError):
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend="sqlite")
                work.compare_and_swap(
                    "UPDATE allocator SET next_id = %s "
                    "WHERE stream = %s AND next_id = %s",
                    (3, "GALLERY", 1),
                    authority="gallery allocator",
                )
        assert connector.fetch_one("SELECT next_id FROM allocator") == (2,)


def test_mariadb_lock_query_is_emitted_with_for_update() -> None:
    class RecordingConnector(SQLiteConnector):
        def __init__(self) -> None:
            self.query = ""

        def fetch_one(
            self, query: str, data: tuple[object, ...] = ()
        ) -> tuple[object, ...]:
            self.query = query
            return (data[0],)

    connector = RecordingConnector()
    work = VNextUnitOfWork(connector, backend="mariadb")
    assert work.lock_row(
        LockRank.HEAD,
        encode_lock_key("SOURCE"),
        "SELECT next_revision FROM allocator WHERE stream = %s;",
        ("SOURCE",),
    ) == ("SOURCE",)
    assert connector.query.endswith(" FOR UPDATE")
