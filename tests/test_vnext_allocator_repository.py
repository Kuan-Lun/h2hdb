from __future__ import annotations

from pathlib import Path

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_allocator_repository import (
    AllocatorExhaustedError,
    IdentityStream,
    RevisionStream,
    VNextAllocatorRepository,
)
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_transaction import VNextUnitOfWork


def _generated_database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def test_revision_and_identity_allocators_advance_exact_seed_rows(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "allocators.sqlite3")
    try:
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            assert (
                VNextAllocatorRepository.allocate_identity(
                    work, IdentityStream.TAG, updated_at=13
                )
                == 1
            )
            assert (
                VNextAllocatorRepository.allocate_identity(
                    work, IdentityStream.POLICY, updated_at=14
                )
                == 1
            )
            assert (
                VNextAllocatorRepository.allocate_identity(
                    work, IdentityStream.GALLERY, updated_at=12
                )
                == 1
            )
            assert (
                VNextAllocatorRepository.allocate_revision(
                    work, RevisionStream.SOURCE, updated_at=10
                )
                == 1
            )
            assert (
                VNextAllocatorRepository.allocate_revision(
                    work, RevisionStream.CATALOG, updated_at=11
                )
                == 1
            )

        assert connector.fetch_all(
            "SELECT stream, next_revision, updated_at "
            "FROM operational_revision_allocators ORDER BY stream"
        ) == [("CATALOG", 2, 11), ("SOURCE", 2, 10)]
        assert connector.fetch_all(
            "SELECT stream, next_id, updated_at "
            "FROM operational_identity_allocators ORDER BY stream"
        ) == [("GALLERY", 2, 12), ("POLICY", 2, 14), ("TAG", 2, 13)]
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("table", "column", "stream", "allocator"),
    (
        (
            "operational_revision_allocators",
            "next_revision",
            RevisionStream.SOURCE,
            "revision",
        ),
        (
            "operational_identity_allocators",
            "next_id",
            IdentityStream.GALLERY,
            "identity",
        ),
    ),
)
def test_exhaustion_sentinel_fails_without_mutation(
    tmp_path: Path,
    table: str,
    column: str,
    stream: RevisionStream | IdentityStream,
    allocator: str,
) -> None:
    connector = _generated_database(tmp_path / f"{allocator}-exhausted.sqlite3")
    try:
        connector.execute(
            f"UPDATE {table} SET {column} = %s WHERE stream = %s",
            (INT63_MAX, stream.value),
        )
        with pytest.raises(AllocatorExhaustedError):
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend="sqlite")
                if isinstance(stream, RevisionStream):
                    VNextAllocatorRepository.allocate_revision(
                        work,
                        stream,
                        updated_at=99,
                    )
                else:
                    VNextAllocatorRepository.allocate_identity(
                        work,
                        stream,
                        updated_at=99,
                    )
        assert connector.fetch_one(
            f"SELECT {column}, updated_at FROM {table} WHERE stream = %s",
            (stream.value,),
        ) == (INT63_MAX, 0)
    finally:
        connector.close()


def test_allocator_requires_exact_seed_authority(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "allocator-missing.sqlite3")
    try:
        connector.execute(
            "DELETE FROM operational_revision_allocators WHERE stream = %s",
            (RevisionStream.SOURCE.value,),
        )
        with pytest.raises(RuntimeError, match="required allocator row"):
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend="sqlite")
                VNextAllocatorRepository.allocate_revision(
                    work,
                    RevisionStream.SOURCE,
                    updated_at=1,
                )
    finally:
        connector.close()
