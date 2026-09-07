from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from test_vnext_gallery_staging_repository import (
    _directory_observation,
    _file_observation,
    _seed_working_gallery,
)
from test_vnext_live_mariadb_analysis_repository import _connector
from vnext_generated_database import open_generated_sqlite_database

import h2hdb.vnext_gallery_staging_repository as staging
from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_gallery_staging_repository import (
    BatchAttempt,
    DirectoryBatchCommand,
    FileBatchCommand,
    GalleryObservationStagingRepository,
    GalleryStagingConflictError,
    GalleryStagingHandle,
    GalleryStagingNotReadyError,
    MatchBatchCommand,
    MatchBatchReceipt,
)
from h2hdb.vnext_identity import (
    GalleryObservationComponent,
    decode_gallery_observation_page,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork


def _stage(
    connector: SQLConnector, backend: str
) -> tuple[GateLease, IngestTurn, GalleryStagingHandle]:
    with connector.transaction():
        gate = MaintenanceGateRepository.claim_shared(
            VNextUnitOfWork(connector, backend=backend), now=10, lease_duration=100_000
        )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend=backend),
            owner_token=b"directory-test01",
            now=11,
            lease_duration=100_000,
        )
    with connector.transaction():
        build, gallery = _seed_working_gallery(cast(Any, connector), turn)
    with connector.transaction():
        handle = GalleryObservationStagingRepository.begin(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build,
            gallery_id=gallery,
            now=20,
        )
    # FILE ordinals deliberately disagree with DIRECTORY byte-name order. Long
    # names also exercise the 255-byte name domain across the 192-entry boundary.
    files = tuple(
        replace(
            _file_observation(index),
            name_bytes=f"{index:04d}".encode() + b"x" * 247 + b".jpg",
        )
        for index in reversed(range(193))
    )
    with connector.transaction():
        GalleryObservationStagingRepository.put_files(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=FileBatchCommand(files, True, BatchAttempt(b"f" * 16, None)),
            now=21,
        )
    directories = tuple(
        sorted(map(_directory_observation, files), key=lambda entry: entry.name_bytes)
    )
    for index, batch in enumerate((directories[:192], directories[192:])):
        with connector.transaction():
            GalleryObservationStagingRepository.put_directories(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                command=DirectoryBatchCommand(
                    batch,
                    index == 1,
                    BatchAttempt(
                        bytes([index + 1]) * 16, None if index == 0 else b"\x01" * 16
                    ),
                ),
                now=22 + index,
            )
    return gate, turn, handle


def _match(
    connector: SQLConnector,
    backend: str,
    authorities: tuple[GateLease, IngestTurn, GalleryStagingHandle],
) -> MatchBatchReceipt:
    gate, turn, handle = authorities
    return GalleryObservationStagingRepository.match_files_to_directory(
        VNextUnitOfWork(connector, backend=backend),
        gate_lease=gate,
        ingest_turn=turn,
        handle=handle,
        command=MatchBatchCommand(b"m" * 16, None),
        now=30,
    )


def _state(connector: SQLConnector) -> tuple[object, ...]:
    with connector.read_transaction():
        return (
            connector.fetch_all(
                "SELECT * FROM operational_gallery_observation_staging_match_checkpoints"
            ),
            connector.fetch_all(
                "SELECT * FROM operational_gallery_observation_staging_match_receipts"
            ),
            connector.fetch_all(
                "SELECT request_sha256 FROM operational_gallery_observation_staging_requests ORDER BY request_sha256"
            ),
        )


@contextmanager
def _rollback(connector: SQLConnector) -> Iterator[None]:
    class InjectedRollback(Exception):
        pass

    with pytest.raises(InjectedRollback), connector.transaction():
        yield
        raise InjectedRollback


def _exercise_directory_batch(connector: SQLConnector, backend: str) -> None:
    authorities = _stage(connector, backend)
    with connector.read_transaction():
        root, _count = staging._component_root(
            connector, authorities[2], GalleryObservationComponent.DIRECTORY
        )
        children = connector.fetch_all(
            "SELECT child_sha256 FROM catalog_gallery_observation_page_children WHERE parent_sha256 = %s ORDER BY position",
            (root,),
        )
    assert len(children) == 2
    leaf = children[0][0]
    before = _state(connector)
    corruptions: tuple[tuple[str, tuple[object, ...]], ...] = (
        (
            "UPDATE catalog_gallery_observation_pages SET page_bytes = %s WHERE page_sha256 = %s",
            (b"invalid", root),
        ),
        (
            "UPDATE catalog_gallery_observation_page_descriptor_components SET component = %s WHERE page_sha256 = %s",
            (b"FILE", root),
        ),
        (
            "UPDATE catalog_gallery_observation_page_descriptor_levels SET level = 2 WHERE page_sha256 = %s",
            (root,),
        ),
        (
            "UPDATE catalog_gallery_observation_page_descriptor_subtree_item_counts SET subtree_item_count = 194 WHERE page_sha256 = %s",
            (root,),
        ),
        (
            "DELETE FROM catalog_gallery_observation_page_key_bounds_seals WHERE page_sha256 = %s",
            (leaf,),
        ),
        (
            "UPDATE catalog_gallery_observation_page_key_bounds_last_keys SET last_key = %s WHERE page_sha256 = %s",
            (b"z" * 255, leaf),
        ),
        (
            "UPDATE catalog_gallery_observation_page_descriptor_levels SET level = 1 WHERE page_sha256 = %s",
            (leaf,),
        ),
        (
            "UPDATE catalog_gallery_observation_page_children SET position = 2 WHERE parent_sha256 = %s AND position = 1",
            (root,),
        ),
        (
            "INSERT INTO catalog_gallery_observation_page_children (parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
            (leaf, children[1][0]),
        ),
    )
    for sql, parameters in corruptions:
        with _rollback(connector):
            connector.execute(sql, parameters)
            with pytest.raises(
                (GalleryStagingConflictError, GalleryStagingNotReadyError)
            ):
                _match(connector, backend, authorities)
        assert _state(connector) == before

    with _rollback(connector):
        completed = _match(connector, backend, authorities)
        assert completed.matched_count == 193 and completed.state == "COMPLETE"
    assert _state(connector) == before

    with (
        patch.object(
            staging,
            "decode_gallery_observation_page",
            wraps=decode_gallery_observation_page,
        ) as decoded,
        patch.object(
            staging, "_component_root", wraps=staging._component_root
        ) as roots,
        patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched,
        connector.transaction(),
    ):
        committed = _match(connector, backend, authorities)
    assert not committed.replayed and committed.matched_count == 193
    assert roots.call_count == 1
    assert (
        decoded.call_count == 3
    )  # One branch, two leaves, including exact-byte validation.
    child_calls = [
        call
        for call in fetched.call_args_list
        if "FROM catalog_gallery_observation_page_children c " in call.args[0]
    ]
    assert len(child_calls) == 3
    assert Counter(call.args[1][0] for call in child_calls) == Counter(
        (root, leaf, children[1][0])
    )
    assert all(
        "LEFT JOIN" in call.args[0] and "LIMIT 257" in call.args[0]
        for call in child_calls
    )
    after = _state(connector)

    # A fresh transaction must reread exact page bytes even for response-loss replay.
    with _rollback(connector):
        connector.execute(
            "UPDATE catalog_gallery_observation_pages SET page_bytes = %s WHERE page_sha256 = %s",
            (b"invalid", leaf),
        )
        with pytest.raises(GalleryStagingConflictError, match="digest differs"):
            _match(connector, backend, authorities)
    assert _state(connector) == after
    with connector.transaction():
        replay = _match(connector, backend, authorities)
    assert replay.replayed and replay.request_sha256 == committed.request_sha256
    assert _state(connector) == after


def test_directory_grouped_batch_sqlite_exact_corruption_rollback_and_replay(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "directory.sqlite3")
    try:
        _exercise_directory_batch(connector, "sqlite")
    finally:
        connector.close()


@pytest.mark.mariadb_smoke
def test_directory_grouped_batch_live_mariadb_exact_corruption_rollback_and_replay(
    mariadb_config: CoreConfig,
) -> None:
    admin = VNextDatabaseAdminFacade(mariadb_config)
    admin.initialize()
    admin.close()
    connector = _connector(mariadb_config)
    connector.connect()
    try:
        _exercise_directory_batch(connector, "mariadb")
    finally:
        connector.close()


@pytest.mark.parametrize("depth", (1, 2, 8))
def test_directory_grouped_traversal_reads_each_touched_page_once(depth: int) -> None:
    from vnext_directory_batch_fixtures import DirectoryGraphConnector

    connector = DirectoryGraphConnector(depth=depth)
    original = list(connector.rows)
    with patch.object(
        staging,
        "decode_gallery_observation_page",
        wraps=decode_gallery_observation_page,
    ) as decoded:
        staging._match_directory_rows(connector, connector.handle(), connector.rows)
    assert connector.rows == original
    assert connector.root_reads == 1
    assert set(connector.page_reads) == set(connector.nodes)
    assert set(connector.page_reads.values()) == {1}
    assert set(connector.child_reads.values()) == {1}
    assert decoded.call_count == len(connector.nodes)


def test_directory_grouped_traversal_skips_unrequested_subtrees() -> None:
    from vnext_directory_batch_fixtures import DirectoryGraphConnector

    connector = DirectoryGraphConnector(depth=8)
    staging._match_directory_rows(connector, connector.handle(), connector.rows[:1])
    assert len(connector.page_reads) == 9
    assert set(connector.page_reads.values()) == {1}
    assert connector.root_reads == 1


def test_directory_grouped_traversal_rejects_target_overflow_and_duplicates() -> None:
    from vnext_directory_batch_fixtures import DirectoryGraphConnector

    connector = DirectoryGraphConnector(leaves=1, depth=1)
    with pytest.raises(GalleryStagingConflictError, match="exceeds 256"):
        staging._match_directory_rows(
            connector, connector.handle(), connector.rows * 257
        )
    with pytest.raises(GalleryStagingConflictError, match="not unique"):
        staging._match_directory_rows(connector, connector.handle(), connector.rows * 2)
    staging._match_directory_rows(connector, connector.handle(), ())
    assert connector.root_reads == 0


def test_directory_grouped_traversal_caps_a_wide_depth_eight_active_path() -> None:
    from vnext_directory_batch_fixtures import DirectoryGraphConnector

    connector = DirectoryGraphConnector(depth=8, wide_path=True)
    staging._match_directory_rows(connector, connector.handle(), connector.rows)
    assert len(connector.page_reads) == 8 + 256
    assert set(connector.page_reads.values()) == {1}
    assert (
        sum(
            len(connector.nodes[digest].children)
            for digest in connector.page_reads
            if connector.nodes[digest].level > 0
        )
        == 8 * 256
    )
    assert connector.root_reads == 1
