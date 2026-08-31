from __future__ import annotations

import sqlite3
from hashlib import sha256

from h2hdb import vnext_identity as identity
from h2hdb.vnext_artifact_preparation_repository import (
    _iter_planned_member_entries,
)


def test_planned_member_plan_preserves_late_metadata_source_position() -> None:
    database = sqlite3.connect(":memory:")
    database.execute(
        "CREATE TABLE members (publication_key BLOB NOT NULL, "
        "source_position INTEGER NOT NULL, source_name_bytes BLOB NOT NULL, "
        "source_file_sha256 BLOB NOT NULL, source_size_bytes INTEGER NOT NULL, "
        "source_role INTEGER NOT NULL)"
    )
    publication_key = sha256(b"publication").digest()
    page = b"page"
    metadata = b"metadata"
    database.executemany(
        "INSERT INTO members VALUES (?, ?, ?, ?, ?, ?)",
        (
            (
                publication_key,
                0,
                b"001.jpg",
                sha256(page).digest(),
                len(page),
                int(identity.ArtifactMemberSourceRole.PAGE),
            ),
            (
                publication_key,
                1,
                b"galleryinfo.txt",
                sha256(metadata).digest(),
                len(metadata),
                int(identity.ArtifactMemberSourceRole.METADATA),
            ),
        ),
    )

    entries = tuple(_iter_planned_member_entries(database, publication_key))

    assert tuple(entry.source_position for entry in entries) == (0, 1)
    assert tuple(entry.source_role for entry in entries) == (
        identity.ArtifactMemberSourceRole.PAGE,
        identity.ArtifactMemberSourceRole.METADATA,
    )
    assert (
        identity.decode_artifact_member_plan(
            identity.encode_artifact_member_plan(entries)
        )
        == entries
    )
