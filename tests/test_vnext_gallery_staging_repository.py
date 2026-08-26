from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import (
    seed_canonical_allocation,
    seed_canonical_page,
)
from vnext_catalog_identity_fixtures import (
    seed_file_name_identity,
    seed_gallery_identity,
    seed_gallery_observation_file,
    seed_tag_term,
)
from vnext_catalog_registry_fixtures import (
    seed_manifest_policy,
    seed_source_scope,
)
from vnext_manifest_fixtures import seed_source_build

import h2hdb.domain as domain_module
import h2hdb.vnext_gallery_staging_repository as staging_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_allocator_repository import (
    IdentityStream,
    VNextAllocatorRepository,
)
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_gallery_identity_repository import GalleryIdentityHandoff
from h2hdb.vnext_gallery_staging_repository import (
    BatchAttempt,
    DirectoryBatchCommand,
    DirectoryObservation,
    FileBatchCommand,
    FileContentReceipt,
    FileObservation,
    GalleryObservationStagingRepository,
    GalleryStagingConflictError,
    GalleryStagingHandle,
    GalleryStagingNotReadyError,
    GalleryStagingReceipt,
    GalleryStagingSeal,
    MatchBatchCommand,
    MatchBatchReceipt,
    MetadataBatchCommand,
    TagBatchCommand,
    TagObservation,
)
from h2hdb.vnext_identity import (
    GALLERY_OBSERVATION_DURABLE_PARSER_PHASES,
    CanonicalValueBranchEntry,
    GalleryObservationComponent,
    GalleryObservationDirectoryEntry,
    GalleryObservationDirectoryFileType,
    GalleryObservationFileEntry,
    GalleryObservationMetadata,
    GalleryObservationMetadataDecoder,
    GalleryObservationNodeKind,
    artifact_source_manifest_digest,
    build_canonical_value_tree,
    build_gallery_observation_tree,
    canonical_value_digest,
    decode_canonical_value_page,
    encode_gallery_observation_metadata,
    file_key,
    gallery_directory_audit_digest,
    gallery_key,
    gallery_metadata_audit_digest,
    gallery_scan_audit_digest,
)
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from h2hdb.vnext_manifest_family import ensure_gallery_manifest_family
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


def _seed_canonical_identity(
    connector: SQLiteConnector,
    *,
    digest_domain: str,
    payload: bytes,
    now: int,
) -> bytes:
    value_sha256 = canonical_value_digest(digest_domain, payload)
    tree = build_canonical_value_tree(value_sha256, len(payload), (payload,))
    seed_canonical_allocation(
        connector,
        value_sha256=value_sha256,
        digest_domain=digest_domain.encode("ascii"),
        byte_count=len(payload),
        allocated_at=now,
    )
    for encoded in tree.pages:
        page = decode_canonical_value_page(encoded.page_bytes)
        seed_canonical_page(
            connector,
            page_sha256=encoded.page_sha256,
            value_sha256=value_sha256,
            page_bytes=encoded.page_bytes,
            level=page.level,
            page_position=page.page_position,
            subtree_item_count=page.subtree_byte_count,
        )
        if page.node_kind is GalleryObservationNodeKind.BRANCH:
            for position, entry in enumerate(page.entries):
                assert isinstance(entry, CanonicalValueBranchEntry)
                connector.execute(
                    "INSERT INTO catalog_canonical_value_page_parents "
                    "(child_sha256, parent_sha256, position) VALUES (%s, %s, %s)",
                    (entry.child_page_sha256, encoded.page_sha256, position),
                )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, tree.root_page_sha256),
    )
    return value_sha256


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=100_000,
        )
    return gate, turn


def _seed_working_gallery(
    connector: SQLiteConnector,
    turn: IngestTurn,
    *,
    build_id: bytes = b"b" * 16,
) -> tuple[bytes, int]:
    root = _seed_canonical_identity(
        connector,
        digest_domain="source_root_v1",
        payload=b"root",
        now=12,
    )
    locator = _seed_canonical_identity(
        connector,
        digest_domain="source_relative_locator_v1",
        payload=b"gallery",
        now=12,
    )
    seed_manifest_policy(connector)
    scope = seed_source_scope(
        connector,
        source_root_sha256=root,
    ).scope_key
    seed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope,
        state="OPEN",
        created_at=12,
    )
    connector.execute(
        "INSERT INTO operational_source_build_generations "
        "(build_id, generation) VALUES (%s, %s)",
        (build_id, turn.generation),
    )
    connector.execute(
        "INSERT INTO operational_source_working_builds "
        "(slot, build_id, assigned_at) VALUES (1, %s, %s)",
        (build_id, 12),
    )
    connector.execute(
        "INSERT INTO catalog_source_locator_identity "
        "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
        (locator, b"gallery"),
    )
    seed_gallery_identity(
        connector,
        gallery_id=1,
        gallery_key=gallery_key(scope, locator),
        scope_key=scope,
        locator_sha256=locator,
    )
    connector.execute(
        "INSERT INTO operational_gallery_observation_allocators "
        "(gallery_id, next_observation_id, updated_at) VALUES (1, 1, 12)"
    )
    connector.execute(
        "INSERT INTO catalog_source_build_expected_gallery "
        "(build_id, position, gallery_id) VALUES (%s, 0, 1)",
        (build_id,),
    )
    return build_id, 1


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    build_id: bytes,
    gallery_id: int,
    *,
    now: int,
) -> GalleryStagingHandle:
    with connector.transaction():
        return GalleryObservationStagingRepository.begin(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build_id,
            gallery_id=gallery_id,
            now=now,
        )


def _file_observation(index: int) -> FileObservation:
    name = f"file-{index:04d}.bin".encode("ascii")
    payload = (f"content-{index}-" * (1 + index % 3)).encode("ascii")
    return FileObservation(
        name,
        FileContentReceipt.from_parts((payload[:3], payload[3:])),
        100 + index,
        1000 + index,
        -50 + index,
        -40 + index,
    )


def _directory_observation(file: FileObservation) -> DirectoryObservation:
    return DirectoryObservation(
        file.name_bytes,
        file.content.size_bytes,
        file.device,
        file.inode,
        file.modified_ns,
        file.changed_ns,
        GalleryObservationDirectoryFileType.REGULAR,
    )


def _put_files(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    handle: Any,
    command: FileBatchCommand,
    *,
    now: int,
) -> GalleryStagingReceipt:
    with connector.transaction():
        return GalleryObservationStagingRepository.put_files(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=command,
            now=now,
        )


def _put_directories(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    handle: Any,
    command: DirectoryBatchCommand,
    *,
    now: int,
) -> GalleryStagingReceipt:
    with connector.transaction():
        return GalleryObservationStagingRepository.put_directories(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=command,
            now=now,
        )


def _put_tags(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    handle: Any,
    command: TagBatchCommand,
    *,
    now: int,
) -> GalleryStagingReceipt:
    with connector.transaction():
        return GalleryObservationStagingRepository.put_tags(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=command,
            now=now,
        )


def _put_metadata(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    handle: Any,
    command: MetadataBatchCommand,
    *,
    now: int,
) -> GalleryStagingReceipt:
    with connector.transaction():
        return GalleryObservationStagingRepository.put_metadata(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=command,
            now=now,
        )


def _match(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    handle: Any,
    command: MatchBatchCommand,
    *,
    now: int,
) -> MatchBatchReceipt:
    with connector.transaction():
        return GalleryObservationStagingRepository.match_files_to_directory(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            handle=handle,
            command=command,
            now=now,
        )


def _request_snapshot(connector: SQLiteConnector) -> tuple[object, ...]:
    return (
        connector.fetch_one(
            "SELECT COUNT(*) FROM operational_gallery_observation_staging_requests"
        ),
        connector.fetch_one(
            "SELECT COUNT(*) FROM operational_gallery_observation_staging_request_chunks"
        ),
        connector.fetch_one("SELECT COUNT(*) FROM catalog_gallery_observation_pages"),
        connector.fetch_one("SELECT COUNT(*) FROM catalog_gallery_observation_files"),
        connector.fetch_all(
            "SELECT component, level, cursor, regular_count, "
            "processed_byte_count, state "
            "FROM operational_gallery_observation_staging_checkpoints "
            "ORDER BY component, level"
        ),
        connector.fetch_all(
            "SELECT component, level, request_sha256, "
            "start_processed_byte_count, next_processed_byte_count "
            "FROM operational_gallery_observation_staging_receipts "
            "ORDER BY component, level"
        ),
    )


_VERTICAL_FAMILY_TABLES = {
    "directory": ("catalog_gallery_observation_directories",),
    "stat": ("catalog_gallery_observation_stat",),
    "scan": ("catalog_gallery_observation_scans",),
    "filesystem": (
        "catalog_gallery_observation_file_filesystem_anchors",
        "catalog_gallery_observation_file_filesystem_devices",
        "catalog_gallery_observation_file_filesystem_inodes",
        "catalog_gallery_observation_file_filesystem_modified_nses",
        "catalog_gallery_observation_file_filesystem_changed_nses",
        "catalog_gallery_observation_file_filesystem_seals",
    ),
}

_PAGE_FAMILY_TABLES = (
    "catalog_gallery_observation_page_descriptor_anchors",
    "catalog_gallery_observation_pages",
    "catalog_gallery_observation_page_descriptor_components",
    "catalog_gallery_observation_page_descriptor_levels",
    "catalog_gallery_observation_page_descriptor_subtree_item_counts",
    "catalog_gallery_observation_page_descriptor_seals",
    "catalog_gallery_observation_page_key_bounds_anchors",
    "catalog_gallery_observation_page_key_bounds_first_keys",
    "catalog_gallery_observation_page_key_bounds_last_keys",
    "catalog_gallery_observation_page_key_bounds_seals",
)


def _page_family_snapshot(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(f"SELECT * FROM {table}") for table in _PAGE_FAMILY_TABLES
    )


def _seed_vertical_family_parents(
    connector: SQLiteConnector,
) -> GalleryStagingHandle:
    gate, turn = _authorities(connector)
    build_id, gallery_id = _seed_working_gallery(connector, turn)
    handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
    observation_identity = _seed_canonical_identity(
        connector,
        digest_domain="gallery_observation_v1",
        payload=b"vertical-family-observation",
        now=21,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observations "
        "(gallery_id, observation_id, observation_identity_sha256) "
        "VALUES (%s, %s, %s)",
        (gallery_id, handle.observation_id, observation_identity),
    )
    seed_file_name_identity(
        connector,
        file_key=b"k" * 32,
        name_bytes=b"file.bin",
        file_role=b"CONTENT",
    )
    connector.execute(
        "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (%s, 9)",
        (b"f" * 32,),
    )
    seed_gallery_observation_file(
        connector,
        gallery_id=gallery_id,
        observation_id=handle.observation_id,
        file_no=0,
        file_key=b"k" * 32,
        file_sha256=b"f" * 32,
    )
    return handle


def _persist_vertical_family(
    connector: Any,
    handle: GalleryStagingHandle,
    family: str,
) -> None:
    if family == "directory":
        staging_module._persist_directory_fact(
            connector,
            gallery_id=handle.gallery_id,
            observation_id=handle.observation_id,
            directory_entry_count=3,
            directory_observation_sha256=b"d" * 32,
        )
    elif family == "stat":
        staging_module._persist_stat_fact(
            connector,
            gallery_id=handle.gallery_id,
            observation_id=handle.observation_id,
            file_count=3,
            byte_count=9,
        )
    elif family == "scan":
        roots = {
            component: (bytes((int(component) + 1,)) * 32, int(component) + 3)
            for component in GalleryObservationComponent
        }
        staging_module._persist_scan_fact(
            connector,
            handle,
            roots,
            scan_observation_version=2,
            source_file_count=3,
        )
    elif family == "filesystem":
        staging_module._persist_file_filesystem_fact(
            connector,
            gallery_id=handle.gallery_id,
            observation_id=handle.observation_id,
            file_key=b"k" * 32,
            device=b"\x01" * 8,
            inode=b"\x02" * 8,
            modified_ns=b"\x03" * 8,
            changed_ns=b"\x04" * 8,
        )
    else:  # pragma: no cover - the test matrix is closed above.
        raise AssertionError(family)


def _vertical_family_snapshot(
    connector: SQLiteConnector,
    family: str,
) -> tuple[list[tuple[Any, ...]], ...]:
    queries: tuple[str, ...]
    if family == "directory":
        queries = ("SELECT * FROM catalog_gallery_observation_directories",)
    elif family == "stat":
        queries = ("SELECT * FROM catalog_gallery_observation_stat",)
    elif family == "scan":
        queries = ("SELECT * FROM catalog_gallery_observation_scans",)
    elif family == "filesystem":
        queries = (
            "SELECT * FROM catalog_gallery_observation_file_filesystem_anchors",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_devices",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_inodes",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_modified_nses",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_changed_nses",
            "SELECT * FROM catalog_gallery_observation_file_filesystem_seals",
        )
    else:  # pragma: no cover - the test matrix is closed above.
        raise AssertionError(family)
    return tuple(connector.fetch_all(query) for query in queries)


def _vertical_family_view(
    connector: SQLiteConnector,
    family: str,
) -> list[tuple[Any, ...]]:
    if family == "directory":
        return connector.fetch_all(
            "SELECT * FROM catalog_gallery_observation_directories"
        )
    if family == "stat":
        return connector.fetch_all("SELECT * FROM catalog_gallery_observation_stat")
    if family == "scan":
        return connector.fetch_all("SELECT * FROM catalog_gallery_observation_scans")
    if family == "filesystem":
        return connector.fetch_all(
            "SELECT * FROM catalog_gallery_observation_file_filesystem"
        )
    raise AssertionError(family)  # pragma: no cover - closed test matrix.


def test_file_content_receipt_is_stream_derived_and_not_forgeable() -> None:
    receipt = FileContentReceipt.from_parts((b"abc", b"", b"def"))
    assert receipt.size_bytes == 6
    assert receipt.file_sha256 == sha256(b"abcdef").digest()
    assert (
        FileContentReceipt.from_parts((b"abcdeg",)).file_sha256 != receipt.file_sha256
    )

    with pytest.raises(TypeError, match="from_parts"):
        FileContentReceipt(sha256(b"abcdef").digest(), 6, object())
    with pytest.raises(TypeError, match="FileContentReceipt"):
        FileObservation(b"x", object(), 1, 1, 1, 1)  # type: ignore[arg-type]


def test_file_response_loss_replay_rejects_normalized_leaf_corruption_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "file-replay-leaf-corruption.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        command = FileBatchCommand(
            (_file_observation(0),),
            True,
            BatchAttempt(b"f" * 16, None),
        )
        committed = _put_files(connector, gate, turn, handle, command, now=21)
        assert connector.fetch_one(
            "SELECT file_no FROM catalog_gallery_observation_file_file_nos "
            "WHERE gallery_id = %s AND observation_id = %s",
            (handle.gallery_id, handle.observation_id),
        ) == (0,)
        assert _put_files(connector, gate, turn, handle, command, now=22).replayed

        connector.execute(
            "UPDATE catalog_gallery_observation_file_file_nos SET file_no = 1 "
            "WHERE gallery_id = %s AND observation_id = %s",
            (handle.gallery_id, handle.observation_id),
        )
        before = _request_snapshot(connector)
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(GalleryStagingConflictError, match="normalized observation"),
        ):
            _put_files(connector, gate, turn, handle, command, now=23)
        assert _request_snapshot(connector) == before
        assert committed.cursor == 1
    finally:
        connector.close()


def test_filesystem_replay_query_is_driven_by_the_binary_anchor() -> None:
    query = staging_module._filesystem_family_query(2)

    assert "WITH expected_keys" not in query
    assert "SELECT %s AS file_key" not in query
    assert "FROM catalog_gallery_observation_file_filesystem_anchors AS a" in query
    assert "a.file_key IN (%s, %s)" in query


def test_file_replay_reads_raw_binary_key_and_rejects_partial_filesystem_family(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "binary-filesystem-replay.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        source = FileObservation(
            b"directory-0000",
            FileContentReceipt.from_parts(()),
            100,
            1_000,
            0,
            0,
        )
        key = file_key(source.name_bytes)
        assert key == bytes.fromhex(
            "e8394e0eee04798de0bf3632768e6bbbc887b3e5dc3ed93de6935b7ed6db7b54"
        )
        command = FileBatchCommand(
            (source,),
            True,
            BatchAttempt(b"f" * 16, None),
        )
        _put_files(connector, gate, turn, handle, command, now=21)
        assert _put_files(connector, gate, turn, handle, command, now=22).replayed

        connector.execute(
            "DELETE FROM catalog_gallery_observation_file_filesystem_seals "
            "WHERE gallery_id = %s AND observation_id = %s AND file_key = %s",
            (handle.gallery_id, handle.observation_id, key),
        )
        before = _request_snapshot(connector)
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(
                GalleryStagingConflictError,
                match="filesystem family is incomplete",
            ),
        ):
            _put_files(connector, gate, turn, handle, command, now=23)
        assert _request_snapshot(connector) == before
    finally:
        connector.close()


def test_tag_response_loss_replay_validates_exact_canonical_payload_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "tag-replay-payload-corruption.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        source = TagObservation("artist", "Alice")
        command = TagBatchCommand(
            (source,),
            True,
            BatchAttempt(b"t" * 16, None),
        )
        _put_tags(connector, gate, turn, handle, command, now=21)
        assert _put_tags(connector, gate, turn, handle, command, now=22).replayed
        root = connector.fetch_one(
            "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
            "WHERE value_sha256 = %s",
            (source._value_sha256,),
        )[0]
        connector.execute(
            "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
            "WHERE page_sha256 = %s",
            (b"corrupt-canonical-page", root),
        )
        before = _request_snapshot(connector)
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(GalleryStagingConflictError),
        ):
            _put_tags(connector, gate, turn, handle, command, now=23)
        assert _request_snapshot(connector) == before
        assert connector.fetch_one(
            "SELECT page_bytes FROM catalog_canonical_value_page_payloads "
            "WHERE page_sha256 = %s",
            (root,),
        ) == (b"corrupt-canonical-page",)
    finally:
        connector.close()


def test_file_pages_materialize_content_hash_counts_and_replay_exact_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "file-hash-materialization.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        shared = FileContentReceipt.from_parts((b"same-content",))

        first_entries = tuple(
            FileObservation(
                f"page-one-{index:03d}.bin".encode("ascii"),
                shared,
                index,
                1_000 + index,
                index,
                index,
            )
            for index in range(256)
        )
        first = FileBatchCommand(
            first_entries,
            False,
            BatchAttempt(b"a" * 16, None),
        )
        _put_files(connector, gate, turn, handle, first, now=21)
        assert connector.fetch_one(
            "SELECT occurrence_count "
            "FROM catalog_gallery_observation_file_hash_occurrences "
            "WHERE gallery_id = %s AND observation_id = %s "
            "AND file_sha256 = %s",
            (handle.gallery_id, handle.observation_id, shared.file_sha256),
        ) == (256,)

        final = FileBatchCommand(
            (
                FileObservation(b"last-content.bin", shared, 300, 1_300, 300, 300),
                FileObservation(b"galleryinfo.txt", shared, 301, 1_301, 301, 301),
            ),
            True,
            BatchAttempt(b"b" * 16, b"a" * 16),
        )
        _put_files(connector, gate, turn, handle, final, now=22)
        assert connector.fetch_all(
            "SELECT file_sha256, occurrence_count "
            "FROM catalog_gallery_observation_file_hash_occurrences "
            "WHERE gallery_id = %s AND observation_id = %s",
            (handle.gallery_id, handle.observation_id),
        ) == [(shared.file_sha256, 257)]

        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
        ):
            assert _put_files(
                connector,
                gate,
                turn,
                handle,
                final,
                now=23,
            ).replayed

        connector.execute(
            "UPDATE catalog_gallery_observation_file_hash_occurrences "
            "SET occurrence_count = 256 WHERE gallery_id = %s "
            "AND observation_id = %s AND file_sha256 = %s",
            (handle.gallery_id, handle.observation_id, shared.file_sha256),
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(GalleryStagingConflictError),
        ):
            _put_files(connector, gate, turn, handle, final, now=24)
    finally:
        connector.close()


def test_file_hash_occurrence_int63_overflow_fails_before_page_writes(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "file-hash-overflow.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        shared = FileContentReceipt.from_parts((b"same-content",))
        first = FileBatchCommand(
            tuple(
                FileObservation(
                    f"page-one-{index:03d}.bin".encode("ascii"),
                    shared,
                    index,
                    1_000 + index,
                    index,
                    index,
                )
                for index in range(256)
            ),
            False,
            BatchAttempt(b"a" * 16, None),
        )
        _put_files(connector, gate, turn, handle, first, now=21)
        connector.execute(
            "UPDATE catalog_gallery_observation_file_hash_occurrences "
            "SET occurrence_count = %s WHERE gallery_id = %s "
            "AND observation_id = %s AND file_sha256 = %s",
            (
                INT63_MAX,
                handle.gallery_id,
                handle.observation_id,
                shared.file_sha256,
            ),
        )
        final = FileBatchCommand(
            (FileObservation(b"last-content.bin", shared, 300, 1_300, 300, 300),),
            True,
            BatchAttempt(b"b" * 16, b"a" * 16),
        )
        before = _request_snapshot(connector)
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(OverflowError, match="occurrence count"),
        ):
            _put_files(connector, gate, turn, handle, final, now=22)
        assert _request_snapshot(connector) == before
        assert connector.fetch_one(
            "SELECT occurrence_count "
            "FROM catalog_gallery_observation_file_hash_occurrences "
            "WHERE gallery_id = %s AND observation_id = %s "
            "AND file_sha256 = %s",
            (handle.gallery_id, handle.observation_id, shared.file_sha256),
        ) == (INT63_MAX,)
    finally:
        connector.close()


def test_tag_page_materializes_only_exact_artist_namespace_and_replays_exact(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "tag-artist-materialization.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        command = TagBatchCommand(
            (
                TagObservation("artist", "Alice"),
                TagObservation("Artist", "Uppercase"),
                TagObservation("group", "Circle"),
            ),
            True,
            BatchAttempt(b"t" * 16, None),
        )
        _put_tags(connector, gate, turn, handle, command, now=21)
        artist_tag_id = connector.fetch_one(
            "SELECT tag_id FROM catalog_gallery_observation_tags "
            "WHERE gallery_id = %s AND observation_id = %s AND position = 0",
            (handle.gallery_id, handle.observation_id),
        )[0]
        assert connector.fetch_all(
            "SELECT artist_tag_id FROM catalog_gallery_observation_artists "
            "WHERE gallery_id = %s AND observation_id = %s",
            (handle.gallery_id, handle.observation_id),
        ) == [(artist_tag_id,)]

        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
        ):
            assert _put_tags(
                connector,
                gate,
                turn,
                handle,
                command,
                now=22,
            ).replayed

        connector.execute(
            "DELETE FROM catalog_gallery_observation_artists "
            "WHERE gallery_id = %s AND observation_id = %s "
            "AND artist_tag_id = %s",
            (handle.gallery_id, handle.observation_id, artist_tag_id),
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            pytest.raises(
                GalleryStagingConflictError,
                match="artist materialization",
            ),
        ):
            _put_tags(connector, gate, turn, handle, command, now=23)
    finally:
        connector.close()


def test_tag_allocator_lock_rereads_natural_identity_before_insert(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "tag-allocator-reread.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        source = TagObservation("artist", "Alice")

        def install_concurrent_term(
            work: VNextUnitOfWork,
            stream: Any,
            *,
            updated_at: int,
        ) -> int:
            assert stream is IdentityStream.TAG
            assert updated_at == 21
            seed_tag_term(
                work.connector,
                tag_id=7,
                namespace=source._namespace_bytes,
                tag_value_sha256=source._value_sha256,
            )
            return 1

        with patch.object(
            VNextAllocatorRepository,
            "allocate_identity",
            side_effect=install_concurrent_term,
        ):
            receipt = _put_tags(
                connector,
                gate,
                turn,
                handle,
                TagBatchCommand(
                    (source,),
                    True,
                    BatchAttempt(b"t" * 16, None),
                ),
                now=21,
            )
        assert receipt.cursor == 1
        assert connector.fetch_one(
            "SELECT tag_id FROM catalog_gallery_observation_tags "
            "WHERE gallery_id = %s AND observation_id = %s AND position = 0",
            (handle.gallery_id, handle.observation_id),
        ) == (7,)
        assert connector.fetch_all(
            "SELECT tag_id FROM catalog_tag_term_anchors ORDER BY tag_id"
        ) == [(7,)]
    finally:
        connector.close()


def test_file_and_tag_identity_families_are_fault_atomic_seal_last_and_replay_exact(
    tmp_path: Path,
) -> None:
    file_tables = (
        "catalog_file_name_identity_anchors",
        "catalog_file_name_identity_name_bytes",
        "catalog_file_name_identity_file_roles",
        "catalog_file_name_identity_seals",
        "catalog_gallery_observation_file_anchors",
        "catalog_gallery_observation_file_file_nos",
        "catalog_gallery_observation_file_file_sha256s",
        "catalog_gallery_observation_file_seals",
        "catalog_gallery_observation_file_hash_occurrences",
    )
    connector = _generated_database(tmp_path / "file-family-faults.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        file_command = FileBatchCommand(
            (_file_observation(0),),
            True,
            BatchAttempt(b"f" * 16, None),
        )
        baseline = _request_snapshot(connector)
        for target in file_tables:
            original_execute = connector.execute
            triggered = False

            def fail_target(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                failed_table: str = target,
            ) -> None:
                nonlocal triggered
                if not triggered and sql.lstrip().startswith(
                    f"INSERT INTO {failed_table}"
                ):
                    triggered = True
                    raise RuntimeError(f"injected {failed_table}")
                original_execute(sql, data)

            with (
                patch.object(connector, "execute", side_effect=fail_target),
                pytest.raises(RuntimeError, match="injected catalog_"),
            ):
                _put_files(connector, gate, turn, handle, file_command, now=21)
            assert triggered
            assert _request_snapshot(connector) == baseline
            assert all(
                connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)
                for table in file_tables
            )
        committed = _put_files(connector, gate, turn, handle, file_command, now=21)
        assert committed.cursor == 1
        assert all(
            connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (1,)
            for table in file_tables
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
        ):
            assert _put_files(
                connector, gate, turn, handle, file_command, now=22
            ).replayed
    finally:
        connector.close()

    tag_tables = (
        "catalog_tag_term_anchors",
        "catalog_tag_term_identities",
        "catalog_tag_term_seals",
        "catalog_gallery_observation_tags",
        "catalog_gallery_observation_artists",
    )
    connector = _generated_database(tmp_path / "tag-family-faults.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        tag_command = TagBatchCommand(
            (TagObservation("artist", "Alice"),),
            True,
            BatchAttempt(b"t" * 16, None),
        )
        baseline = _request_snapshot(connector)
        for target in tag_tables:
            original_execute = connector.execute
            triggered = False

            def fail_target(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                failed_table: str = target,
            ) -> None:
                nonlocal triggered
                if not triggered and sql.lstrip().startswith(
                    f"INSERT INTO {failed_table}"
                ):
                    triggered = True
                    raise RuntimeError(f"injected {failed_table}")
                original_execute(sql, data)

            with (
                patch.object(connector, "execute", side_effect=fail_target),
                pytest.raises(RuntimeError, match="injected catalog_"),
            ):
                _put_tags(connector, gate, turn, handle, tag_command, now=21)
            assert triggered
            assert _request_snapshot(connector) == baseline
            assert all(
                connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)
                for table in tag_tables
            )
            assert (
                connector.fetch_all("SELECT 1 FROM catalog_gallery_observation_tags")
                == []
            )
        committed = _put_tags(connector, gate, turn, handle, tag_command, now=21)
        assert committed.cursor == 1
        assert all(
            connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (1,)
            for table in tag_tables
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
        ):
            assert _put_tags(
                connector, gate, turn, handle, tag_command, now=22
            ).replayed
    finally:
        connector.close()


def test_equal_content_digest_with_different_stream_size_conflicts_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SameDigest:
        def update(self, _part: bytes) -> None:
            return None

        def digest(self) -> bytes:
            return b"h" * 32

    with monkeypatch.context() as context:
        context.setattr(domain_module, "sha256", lambda _value=b"": _SameDigest())
        short = FileContentReceipt.from_parts((b"a",))
        long = FileContentReceipt.from_parts((b"bb",))

    connector = _generated_database(tmp_path / "gallery-content-size.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        before = _request_snapshot(connector)
        with pytest.raises(GalleryStagingConflictError, match="content blob differs"):
            _put_files(
                connector,
                gate,
                turn,
                handle,
                FileBatchCommand(
                    (
                        FileObservation(b"a.bin", short, 1, 2, 3, 4),
                        FileObservation(b"b.bin", long, 5, 6, 7, 8),
                    ),
                    True,
                    BatchAttempt(b"a" * 16, None),
                ),
                now=21,
            )
        assert _request_snapshot(connector) == before
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_content_blobs") == (0,)
        assert connector.fetch_one(
            "SELECT cursor, processed_byte_count, state "
            "FROM operational_gallery_observation_staging_checkpoints "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"FILE"),
        ) == (0, 0, "OPEN")
    finally:
        connector.close()


def test_processed_byte_authority_overflow_terminal_empty_and_nonfile_corruption(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gallery-byte-authority.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        connector.execute(
            "UPDATE operational_gallery_observation_staging_checkpoints "
            "SET processed_byte_count = %s "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (INT63_MAX, handle.staging_id, b"FILE"),
        )
        overflow_snapshot = _request_snapshot(connector)
        with pytest.raises(OverflowError, match="processed byte count"):
            _put_files(
                connector,
                gate,
                turn,
                handle,
                FileBatchCommand(
                    (_file_observation(1),),
                    True,
                    BatchAttempt(b"o" * 16, None),
                ),
                now=21,
            )
        assert _request_snapshot(connector) == overflow_snapshot
        connector.execute(
            "UPDATE operational_gallery_observation_staging_checkpoints "
            "SET processed_byte_count = 0 "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"FILE"),
        )
        empty = _put_files(
            connector,
            gate,
            turn,
            handle,
            FileBatchCommand((), True, BatchAttempt(b"e" * 16, None)),
            now=22,
        )
        assert (empty.cursor, empty.processed_byte_count) == (0, 0)
        assert connector.fetch_one(
            "SELECT start_processed_byte_count, next_processed_byte_count "
            "FROM operational_gallery_observation_staging_receipts "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"FILE"),
        ) == (0, 0)

        connector.execute("PRAGMA ignore_check_constraints = ON")
        connector.execute(
            "UPDATE operational_gallery_observation_staging_checkpoints "
            "SET processed_byte_count = 1 "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"DIRECTORY"),
        )
        connector.execute("PRAGMA ignore_check_constraints = OFF")
        corrupt_snapshot = _request_snapshot(connector)
        with pytest.raises(GalleryStagingConflictError, match="non-FILE DIRECTORY"):
            _put_directories(
                connector,
                gate,
                turn,
                handle,
                DirectoryBatchCommand((), True, BatchAttempt(b"d" * 16, None)),
                now=23,
            )
        assert _request_snapshot(connector) == corrupt_snapshot
    finally:
        connector.close()


def test_parser_phase_mapping_and_audit_digest_goldens_are_closed() -> None:
    runtime_phases = {
        "PREFIX",
        "VERSION",
        "GID",
        "TITLE_TAG",
        "TITLE_LENGTH",
        "TITLE_TEXT",
        "COMMENT_TAG",
        "COMMENT_LENGTH",
        "COMMENT_TEXT",
        "ACCOUNT_TAG",
        "ACCOUNT_LENGTH",
        "ACCOUNT_TEXT",
        "UPLOAD_TIME",
        "DOWNLOAD_TIME",
        "MODIFIED_TIME",
        "SCAN_VERSION",
        "SOURCE_FILE_COUNT",
        "PAGE_COUNT_PRESENCE",
        "PAGE_COUNT",
        "DONE",
    }
    for phase in runtime_phases:
        assert (
            staging_module._runtime_parser_phase(
                staging_module._durable_parser_phase(phase)
            )
            == phase
        )
    assert {
        staging_module._durable_parser_phase(phase) for phase in runtime_phases
    } == set(GALLERY_OBSERVATION_DURABLE_PARSER_PHASES)
    with pytest.raises(GalleryStagingConflictError, match="unknown runtime"):
        staging_module._durable_parser_phase("unknown")
    with pytest.raises(GalleryStagingConflictError, match="unknown durable"):
        staging_module._runtime_parser_phase("unknown")
    for alias in (
        "TITLE_TEXT",
        "COMMENT_TEXT",
        "ACCOUNT_TAG",
        "ACCOUNT_LENGTH",
        "ACCOUNT_TEXT",
        "LENGTH",
    ):
        with pytest.raises(GalleryStagingConflictError, match="unknown durable"):
            staging_module._runtime_parser_phase(alias)
    assert "UPLOAD_ACCOUNT_LENGTH" in GALLERY_OBSERVATION_DURABLE_PARSER_PHASES
    assert staging_module._runtime_parser_phase("UPLOAD_ACCOUNT_LENGTH") == (
        "ACCOUNT_LENGTH"
    )

    root = bytes(range(32))
    assert gallery_directory_audit_digest(root, 7).hex() == (
        "75cc50ba337da95a237aaecf984016af9ea667a80492aba5b0cadda10a16b9d7"
    )
    assert gallery_metadata_audit_digest(root, 11).hex() == (
        "70bdb1d71ac3f6daabf19ac16334acd825149f65054bbe3384602b7e8a1d6d06"
    )
    roots = {
        component: (bytes((int(component) + 1,)) * 32, int(component) + 3)
        for component in GalleryObservationComponent
    }
    assert gallery_scan_audit_digest(roots).hex() == (
        "b04306743e234c19165a72e2e4bc7831b6a90bb397f6c7a69fdee6fb03ab7042"
    )


def test_mariadb_staging_authority_locks_use_for_update_in_global_order() -> None:
    class _MariaLockFake:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            if "operational_gallery_observation_stagings" in query:
                return (b"b" * 16, 1, 2, "OPEN", 3, None)
            if "operational_gallery_observation_staging_claims" in query:
                return (4, 5, 6)
            raise AssertionError(query)

    connector = _MariaLockFake()
    work = VNextUnitOfWork(cast(Any, connector), backend="mariadb")
    header, claim = staging_module._lock_header_and_claim(work, b"s" * 16)
    assert (header.observation_id, claim.claim_generation) == (2, 5)
    assert len(connector.queries) == 2
    assert all(query.endswith(" FOR UPDATE") for query, _data in connector.queries)
    assert "stagings" in connector.queries[0][0]
    assert "claims" in connector.queries[1][0]

    class _MariaCheckpointFake:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            del data
            self.queries.append(query)
            return (7, 0, 99, "OPEN", 8)

    checkpoint_connector = _MariaCheckpointFake()
    checkpoint = staging_module._lock_checkpoint(
        VNextUnitOfWork(cast(Any, checkpoint_connector), backend="mariadb"),
        b"s" * 16,
        GalleryObservationComponent.FILE,
        0,
    )
    assert checkpoint.processed_byte_count == 99
    assert len(checkpoint_connector.queries) == 1
    assert "processed_byte_count" in checkpoint_connector.queries[0]
    assert checkpoint_connector.queries[0].endswith(" FOR UPDATE")

    class _MariaRequestFake:
        def __init__(self, staging_id: bytes, predecessor: bytes) -> None:
            self.staging_id = staging_id
            self.predecessor = predecessor
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            if "request_owners" in query:
                return (self.staging_id,) if data == (self.predecessor,) else ()
            if "request_predecessors" in query:
                return ()
            if "staging_requests" in query:
                return ()
            raise AssertionError(query)

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    staging_id = b"s" * 16
    predecessor = b"p" * 32
    request_connector = _MariaRequestFake(staging_id, predecessor)
    handle = GalleryStagingHandle(staging_id, b"b" * 16, 1, 1, 1, 0)
    frame = staging_module._bounded_request_frame(b"B", handle, (b"body",))
    request_sha256 = staging_module._persist_request_identity(
        VNextUnitOfWork(cast(Any, request_connector), backend="mariadb"),
        handle,
        frame,
        predecessor=predecessor,
        owner_lock_level=0,
    )
    assert request_sha256 == sha256(frame).digest()
    locked = [
        query for query, _data in request_connector.queries if "FOR UPDATE" in query
    ]
    assert len(locked) == 3
    assert all(query.endswith(" FOR UPDATE") for query in locked)
    assert sum("request_owners" in query for query in locked) == 2
    assert sum("request_predecessors" in query for query in locked) == 1


def test_mariadb_gallery_manifest_policy_is_plain_read_and_writer_is_atomic() -> None:
    observation_identity = b"o" * 32

    class RecordingConnector:
        def __init__(self) -> None:
            self.fetches: list[str] = []
            self.writes: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            del data
            self.fetches.append(query)
            if "FROM catalog_gallery_observations" in query:
                return (observation_identity,)
            if "FROM catalog_manifest_policy_seals" in query:
                return (1, 1)
            if "FROM catalog_gallery_manifests" in query:
                return ()
            if "UTC_TIMESTAMP(6)" in query:
                return (123_000_000,)
            raise AssertionError(query)

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            del data
            self.writes.append(query)

    connector = RecordingConnector()
    result = ensure_gallery_manifest_family(
        VNextUnitOfWork(connector, backend="mariadb"),  # type: ignore[arg-type]
        gallery_id=1,
        observation_id=2,
        manifest_policy_id=1,
    )
    assert result.manifest_sha256 == artifact_source_manifest_digest(
        observation_identity,
        1,
        1,
    )
    observation_query = next(
        query for query in connector.fetches if "catalog_gallery_observations" in query
    )
    policy_query = next(
        query for query in connector.fetches if "catalog_manifest_policy_seals" in query
    )
    assert observation_query.endswith(" FOR UPDATE")
    assert "FOR UPDATE" not in policy_query
    assert any("UTC_TIMESTAMP(6)" in query for query in connector.fetches)
    assert "catalog_gallery_manifests" in connector.writes[-1]


def test_mariadb_gallery_page_family_sql_is_static_and_seal_last() -> None:
    class _MariaPageFake:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            if "FROM catalog_gallery_observation_page_descriptor_anchors" in query:
                return ()
            if "FROM catalog_gallery_observation_page_key_bounds_anchors" in query:
                return ()
            if "gallery_observation_allocation_pages" in query:
                return ()
            raise AssertionError(query)

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.queries.append((query, data))
            if "gallery_observation_page_children" in query:
                return []
            raise AssertionError(query)

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    connector = _MariaPageFake()
    prepared = staging_module._prepare_leaf(
        GalleryObservationComponent.FILE,
        0,
        (_file_observation(0),),
    )
    handle = GalleryStagingHandle(b"s" * 16, b"b" * 16, 1, 1, 1, 0)
    staging_module._persist_observation_page(connector, handle, prepared)
    inserted_tables = tuple(
        table
        for query, _data in connector.executions
        for table in (
            *_PAGE_FAMILY_TABLES,
            "catalog_gallery_observation_allocation_pages",
        )
        if query.lstrip().startswith(f"INSERT INTO {table}")
    )
    assert inserted_tables == (
        *_PAGE_FAMILY_TABLES,
        "catalog_gallery_observation_allocation_pages",
    )
    assert all("%s" in query and "?" not in query for query, _ in connector.queries)
    assert all("%s" in query and "?" not in query for query, _ in connector.executions)
    assert not any("FROM (SELECT %s" in query for query, _ in connector.queries)
    assert not any("FOR UPDATE" in query for query, _ in connector.queries)


def test_mariadb_metadata_shared_fact_writes_are_serialized_by_ingest_head() -> None:
    class _MariaIngestFenceFake:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            if "operational_ingest_coordination_heads" in query:
                return (1, 0, "INGESTING", 1)
            if "operational_ingest_generations" in query:
                return (1, None)
            if "operational_ingest_generation_owners" in query:
                return (b"i" * 16,)
            if "operational_ingest_generation_leases" in query:
                return (100,)
            raise AssertionError(query)

    gate = GateLease(b"g" * 16, 1, GateMode.SHARED, (0,), 100)
    turn = IngestTurn(1, b"i" * 16, 100)
    connector = _MariaIngestFenceFake()
    with patch.object(
        MaintenanceGateRepository,
        "lock_and_require_live",
        return_value=gate,
    ) as gate_lock:
        generation = staging_module._authorize_outer(
            VNextUnitOfWork(cast(Any, connector), backend="mariadb"),
            gate,
            turn,
            now=2,
        )

    assert generation == 1
    gate_lock.assert_called_once()
    assert len(connector.queries) == 4
    assert "operational_ingest_coordination_heads" in connector.queries[0][0]
    assert all(query.endswith(" FOR UPDATE") for query, _data in connector.queries)


def _metadata_vertical_snapshot(connector: SQLiteConnector) -> tuple[object, ...]:
    return (
        connector.fetch_all(
            "SELECT gid, upload_time FROM catalog_gallery_upload_times ORDER BY gid"
        ),
        connector.fetch_all(
            "SELECT source_gallery_name, gid FROM catalog_source_gallery_name_gids "
            "ORDER BY source_gallery_name"
        ),
        connector.fetch_all(
            "SELECT gallery_id, source_gallery_name "
            "FROM catalog_gallery_source_name_accesses ORDER BY gallery_id"
        ),
        connector.fetch_all(
            "SELECT gallery_id, observation_id, download_time, modified_time "
            "FROM catalog_gallery_observation_metadata_locals "
            "ORDER BY gallery_id, observation_id"
        ),
    )


def test_metadata_vertical_writer_derives_narrow_facts_and_replays(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "metadata-vertical.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        metadata = GalleryObservationMetadata(
            12_345,
            "title",
            "comment",
            "uploader",
            100,
            101,
            102,
            1,
            0,
            None,
        )
        command = MetadataBatchCommand(
            encode_gallery_observation_metadata(metadata),
            True,
            BatchAttempt(b"m" * 16, None),
        )

        committed = _put_metadata(
            connector,
            gate,
            turn,
            handle,
            command,
            now=21,
        )
        assert committed.state == "COMPLETE"
        expected = (
            [(12_345, 100)],
            [(b"gallery", 12_345)],
            [(gallery_id, b"gallery")],
            [(gallery_id, handle.observation_id, 101, 102)],
        )
        assert _metadata_vertical_snapshot(connector) == expected
        assert connector.fetch_one(
            "SELECT gallery_id, observation_id, gid, upload_time, download_time, "
            "modified_time FROM catalog_gallery_observation_metadata"
        ) == (gallery_id, handle.observation_id, 12_345, 100, 101, 102)

        before = (_request_snapshot(connector), _metadata_vertical_snapshot(connector))
        replayed = _put_metadata(
            connector,
            gate,
            turn,
            handle,
            command,
            now=22,
        )
        assert replayed.replayed
        assert (
            _request_snapshot(connector),
            _metadata_vertical_snapshot(connector),
        ) == before
    finally:
        connector.close()


@pytest.mark.parametrize(
    "failed_table",
    (
        "catalog_gallery_upload_times",
        "catalog_source_gallery_name_gids",
        "catalog_gallery_source_name_accesses",
        "catalog_gallery_observation_metadata_locals",
        "catalog_gallery_observation_metadata_digests",
        "catalog_gallery_observation_page_counts",
    ),
)
def test_metadata_vertical_insert_fault_rolls_back_every_fact(
    tmp_path: Path,
    failed_table: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"metadata-vertical-fault-{failed_table}.sqlite3"
    )
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        command = MetadataBatchCommand(
            encode_gallery_observation_metadata(
                GalleryObservationMetadata(7, "", "", "", 10, 11, 12, 1, 0, 3)
            ),
            True,
            BatchAttempt(b"f" * 16, None),
        )
        before = _request_snapshot(connector)
        original_execute = connector.execute

        def fail_one_fact(sql: str, data: tuple[Any, ...] = ()) -> None:
            if sql.lstrip().startswith(f"INSERT INTO {failed_table}"):
                raise RuntimeError("injected metadata-fact fault")
            original_execute(sql, data)

        with (
            patch.object(connector, "execute", side_effect=fail_one_fact),
            pytest.raises(RuntimeError, match="metadata-fact fault"),
        ):
            _put_metadata(connector, gate, turn, handle, command, now=21)
        assert _request_snapshot(connector) == before
        assert _metadata_vertical_snapshot(connector) == (
            [],
            [],
            [],
            [],
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_gallery_observation_metadata_digests"
            )
            == []
        )
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_gallery_observation_page_counts")
            == []
        )

        committed = _put_metadata(connector, gate, turn, handle, command, now=22)
        assert committed.state == "COMPLETE"
        assert connector.fetch_one(
            "SELECT gid, upload_time, download_time, modified_time "
            "FROM catalog_gallery_observation_metadata"
        ) == (7, 10, 11, 12)
    finally:
        connector.close()


def test_gallery_page_families_are_seal_last_fault_atomic_and_replay_exact(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gallery-page-vertical.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        command = FileBatchCommand(
            (_file_observation(0),),
            True,
            BatchAttempt(b"p" * 16, None),
        )
        empty: tuple[list[tuple[Any, ...]], ...] = tuple(
            [] for _table in _PAGE_FAMILY_TABLES
        )
        before_request = _request_snapshot(connector)
        assert _page_family_snapshot(connector) == empty

        for failed_table in _PAGE_FAMILY_TABLES:
            original_execute = connector.execute
            triggered = False

            def fail_one_insert(
                sql: str,
                data: tuple[Any, ...] = (),
                *,
                target: str = failed_table,
            ) -> None:
                nonlocal triggered
                if not triggered and sql.lstrip().startswith(f"INSERT INTO {target}"):
                    triggered = True
                    raise RuntimeError("injected gallery-page family fault")
                original_execute(sql, data)

            with (
                patch.object(connector, "execute", side_effect=fail_one_insert),
                pytest.raises(RuntimeError, match="gallery-page family fault"),
            ):
                _put_files(connector, gate, turn, handle, command, now=21)
            assert triggered, failed_table
            assert _request_snapshot(connector) == before_request
            assert _page_family_snapshot(connector) == empty

        inserted: list[str] = []
        original_execute = connector.execute

        def record_insert(sql: str, data: tuple[Any, ...] = ()) -> None:
            inserted.append(sql)
            original_execute(sql, data)

        with patch.object(connector, "execute", side_effect=record_insert):
            receipt = _put_files(connector, gate, turn, handle, command, now=22)
        inserted_tables = tuple(
            table
            for sql in inserted
            for table in _PAGE_FAMILY_TABLES
            if sql.lstrip().startswith(f"INSERT INTO {table}")
        )
        assert inserted_tables == _PAGE_FAMILY_TABLES
        assert receipt.root_page_sha256 is not None
        committed = _page_family_snapshot(connector)
        assert all(rows for rows in committed)

        replay_inserts: list[str] = []
        original_execute = connector.execute

        def record_replay(sql: str, data: tuple[Any, ...] = ()) -> None:
            if sql.lstrip().startswith("INSERT"):
                replay_inserts.append(sql)
            original_execute(sql, data)

        with patch.object(connector, "execute", side_effect=record_replay):
            replayed = _put_files(connector, gate, turn, handle, command, now=23)
        assert replayed.replayed
        assert _page_family_snapshot(connector) == committed
        assert replay_inserts == []

        for seal_table, message in (
            (
                "catalog_gallery_observation_page_key_bounds_seals",
                "bounds family is partial",
            ),
            (
                "catalog_gallery_observation_page_descriptor_seals",
                "page family is partial",
            ),
        ):
            connector.execute("PRAGMA foreign_keys = OFF")
            try:
                connector.execute(
                    f"DELETE FROM {seal_table} WHERE page_sha256 = %s",
                    (receipt.root_page_sha256,),
                )
            finally:
                connector.execute("PRAGMA foreign_keys = ON")
            corrupt_pages = _page_family_snapshot(connector)
            corrupt_request = _request_snapshot(connector)
            with pytest.raises(GalleryStagingConflictError, match=message):
                _put_files(connector, gate, turn, handle, command, now=24)
            assert _page_family_snapshot(connector) == corrupt_pages
            assert _request_snapshot(connector) == corrupt_request
            connector.execute(
                f"INSERT INTO {seal_table} (page_sha256) VALUES (%s)",
                (receipt.root_page_sha256,),
            )
    finally:
        connector.close()


def test_metadata_vertical_corruption_mismatch_has_zero_partial_writes(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "metadata-vertical-conflict.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "INSERT INTO catalog_gallery_observation_metadata_locals "
                "(gallery_id, observation_id, download_time, modified_time) "
                "VALUES (%s, %s, %s, %s)",
                (gallery_id, handle.observation_id, 21, 999),
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        before = _request_snapshot(connector)
        command = MetadataBatchCommand(
            encode_gallery_observation_metadata(
                GalleryObservationMetadata(8, "", "", "", 20, 21, 22, 1, 0, None)
            ),
            True,
            BatchAttempt(b"c" * 16, None),
        )

        with pytest.raises(
            GalleryStagingConflictError,
            match="gallery observation metadata local differs",
        ):
            _put_metadata(connector, gate, turn, handle, command, now=21)
        assert _request_snapshot(connector) == before
        assert _metadata_vertical_snapshot(connector) == (
            [],
            [],
            [],
            [(gallery_id, handle.observation_id, 21, 999)],
        )
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_gallery_observation_metadata")
            == []
        )
    finally:
        connector.close()


def test_metadata_vertical_mariadb_sql_shape_uses_server_derived_name() -> None:
    class _MariaMetadataRecorder:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            if "FROM catalog_gallery_identities AS identity" in query:
                return (b"l" * 32, b"server-gallery")
            return ()

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.queries.append((query, data))
            if "FROM catalog_gallery_observation_tree_roots" in query:
                return [(b"r" * 32, 1)]
            raise AssertionError(query)

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    decoder = GalleryObservationMetadataDecoder()
    decoder.feed(
        encode_gallery_observation_metadata(
            GalleryObservationMetadata(77, "", "", "", 100, 101, 102, 1, 0, None)
        )
    )
    recorder = _MariaMetadataRecorder()
    staging_module._persist_metadata_facts(
        cast(Any, recorder),
        GalleryStagingHandle(b"s" * 16, b"b" * 16, 1, 1, 1, 0),
        decoder.state,
        b"r" * 32,
    )

    derived_query, derived_data = recorder.queries[0]
    assert "FROM catalog_gallery_identities AS identity" in derived_query
    assert "JOIN catalog_source_locator_identity AS locator" in derived_query
    assert "identity.gallery_id = %s" in derived_query
    assert derived_data == (1,)
    ordered_tables = (
        "catalog_gallery_upload_times",
        "catalog_source_gallery_name_gids",
        "catalog_gallery_source_name_accesses",
        "catalog_gallery_observation_metadata_locals",
    )
    pilot_executions = tuple(
        (query, data)
        for query, data in recorder.executions
        if any(
            query.lstrip().startswith(f"INSERT INTO {table}")
            for table in ordered_tables
        )
    )
    assert len(pilot_executions) == len(ordered_tables)
    for (query, _data), table in zip(pilot_executions, ordered_tables, strict=True):
        assert query.lstrip().startswith(f"INSERT INTO {table}")
        assert "%s" in query and "?" not in query
    assert pilot_executions[1][1] == (b"server-gallery", 77)
    assert pilot_executions[2][1] == (1, b"server-gallery")
    assert (
        recorder.executions.index(pilot_executions[-1]) < len(recorder.executions) - 1
    )
    assert "catalog_gallery_observation_metadata_digests" in recorder.executions[-1][0]


def test_four_vertical_family_writers_fault_replay_and_seal_visibility(
    tmp_path: Path,
) -> None:
    assert sum(len(tables) for tables in _VERTICAL_FAMILY_TABLES.values()) == 9
    for family, tables in _VERTICAL_FAMILY_TABLES.items():
        connector = _generated_database(tmp_path / f"{family}-vertical-fault.sqlite3")
        try:
            handle = _seed_vertical_family_parents(connector)
            empty: tuple[list[tuple[Any, ...]], ...] = tuple([] for _table in tables)
            assert _vertical_family_snapshot(connector, family) == empty
            assert _vertical_family_view(connector, family) == []

            for failed_table in tables:
                original_execute = connector.execute
                triggered = False

                def fail_one_insert(
                    sql: str,
                    data: tuple[Any, ...] = (),
                    *,
                    target: str = failed_table,
                ) -> None:
                    nonlocal triggered
                    if not triggered and sql.lstrip().startswith(
                        f"INSERT INTO {target}"
                    ):
                        triggered = True
                        raise RuntimeError(f"injected {target} fault")
                    original_execute(sql, data)

                with (
                    patch.object(connector, "execute", side_effect=fail_one_insert),
                    pytest.raises(RuntimeError, match="injected catalog_"),
                ):
                    with connector.transaction():
                        _persist_vertical_family(connector, handle, family)
                assert triggered, failed_table
                assert _vertical_family_snapshot(connector, family) == empty
                assert _vertical_family_view(connector, family) == []

            executed: list[str] = []
            original_execute = connector.execute

            def record_execute(sql: str, data: tuple[Any, ...] = ()) -> None:
                executed.append(sql)
                original_execute(sql, data)

            with patch.object(connector, "execute", side_effect=record_execute):
                with connector.transaction():
                    _persist_vertical_family(connector, handle, family)
            inserted_tables = tuple(
                table
                for sql in executed
                for table in tables
                if sql.lstrip().startswith(f"INSERT INTO {table}")
            )
            assert inserted_tables == tables
            if family == "filesystem":
                assert inserted_tables[-1].endswith("_seals")
            committed = _vertical_family_snapshot(connector, family)
            assert all(rows for rows in committed)
            assert len(_vertical_family_view(connector, family)) == 1

            replayed_sql: list[str] = []
            original_execute = connector.execute

            def record_replay(sql: str, data: tuple[Any, ...] = ()) -> None:
                replayed_sql.append(sql)
                original_execute(sql, data)

            with patch.object(connector, "execute", side_effect=record_replay):
                with connector.transaction():
                    _persist_vertical_family(connector, handle, family)
            assert _vertical_family_snapshot(connector, family) == committed
            assert not any(
                sql.lstrip().startswith("INSERT INTO catalog_gallery_observation_")
                for sql in replayed_sql
            )

            key = (handle.gallery_id, handle.observation_id)
            if family == "directory":
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_directories "
                    "WHERE gallery_id = %s AND observation_id = %s",
                    key,
                )
            elif family == "stat":
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_stat "
                    "WHERE gallery_id = %s AND observation_id = %s",
                    key,
                )
            elif family == "scan":
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_scans "
                    "WHERE gallery_id = %s AND observation_id = %s",
                    key,
                )
            else:
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_file_filesystem_seals "
                    "WHERE gallery_id = %s AND observation_id = %s AND file_key = %s",
                    (*key, b"k" * 32),
                )
            assert _vertical_family_view(connector, family) == []
        finally:
            connector.close()


@pytest.mark.parametrize("family", tuple(_VERTICAL_FAMILY_TABLES))
def test_four_vertical_family_corruption_is_zero_partial(
    tmp_path: Path,
    family: str,
) -> None:
    connector = _generated_database(tmp_path / f"{family}-vertical-corrupt.sqlite3")
    try:
        handle = _seed_vertical_family_parents(connector)
        key = (handle.gallery_id, handle.observation_id)
        if family == "directory":
            connector.execute(
                "INSERT INTO catalog_gallery_observation_directories "
                "(gallery_id, observation_id, directory_entry_count, "
                "directory_observation_sha256) VALUES (%s, %s, %s, %s)",
                (*key, 999, b"x" * 32),
            )
        elif family == "stat":
            connector.execute(
                "INSERT INTO catalog_gallery_observation_stat "
                "(gallery_id, observation_id, file_count, byte_count) "
                "VALUES (%s, %s, %s, %s)",
                (*key, 999, 999),
            )
        elif family == "scan":
            connector.execute(
                "INSERT INTO catalog_gallery_observation_scans "
                "(gallery_id, observation_id, scan_observation_sha256, "
                "scan_observation_version, source_file_count) "
                "VALUES (%s, %s, %s, %s, %s)",
                (*key, b"x" * 32, 999, 999),
            )
        else:
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_filesystem_anchors "
                "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
                (*key, b"k" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_filesystem_changed_nses "
                "(gallery_id, observation_id, file_key, changed_ns) "
                "VALUES (%s, %s, %s, %s)",
                (*key, b"k" * 32, b"x" * 8),
            )
        before = _vertical_family_snapshot(connector, family)

        with pytest.raises(GalleryStagingConflictError, match="differs"):
            with connector.transaction():
                _persist_vertical_family(connector, handle, family)
        assert _vertical_family_snapshot(connector, family) == before
        if family == "filesystem":
            assert _vertical_family_view(connector, family) == []
        else:
            assert len(_vertical_family_view(connector, family)) == 1
    finally:
        connector.close()


def test_four_vertical_family_mariadb_sql_is_static_and_seal_last() -> None:
    class _MariaVerticalRecorder:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            return ()

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    recorder = _MariaVerticalRecorder()
    handle = GalleryStagingHandle(b"s" * 16, b"b" * 16, 1, 1, 1, 0)
    for family in _VERTICAL_FAMILY_TABLES:
        _persist_vertical_family(recorder, handle, family)

    ordered_tables = tuple(
        table for tables in _VERTICAL_FAMILY_TABLES.values() for table in tables
    )
    insertions = tuple(
        (query, data)
        for query, data in recorder.executions
        if query.lstrip().startswith("INSERT INTO catalog_gallery_observation_")
    )
    assert len(insertions) == len(ordered_tables) == 9
    for (query, _data), table in zip(insertions, ordered_tables, strict=True):
        assert query.lstrip().startswith(f"INSERT INTO {table}")
        assert "%s" in query and "?" not in query
    offset = 0
    for tables in _VERTICAL_FAMILY_TABLES.values():
        assert (
            insertions[offset + len(tables) - 1][0]
            .lstrip()
            .startswith(f"INSERT INTO {tables[-1]}")
        )
        offset += len(tables)


def test_begin_rolls_back_replays_and_large_vertical_slice_seals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "gallery-staging.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)

        with pytest.raises(RuntimeError, match="begin crash"):
            with connector.transaction():
                GalleryObservationStagingRepository.begin(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    build_id=build_id,
                    gallery_id=gallery_id,
                    now=20,
                )
                raise RuntimeError("begin crash")
        assert connector.fetch_one(
            "SELECT next_observation_id FROM operational_gallery_observation_allocators "
            "WHERE gallery_id = 1"
        ) == (1,)
        assert (
            connector.fetch_all(
                "SELECT staging_id FROM operational_gallery_observation_stagings"
            )
            == []
        )

        gallery_key_value, scope_key, locator_sha256 = connector.fetch_one(
            "SELECT gallery_key, scope_key, locator_sha256 "
            "FROM catalog_gallery_identities WHERE gallery_id = %s",
            (gallery_id,),
        )
        handoff = GalleryIdentityHandoff(
            build_id,
            gallery_id,
            gallery_key_value,
            scope_key,
            locator_sha256,
            False,
        )
        with pytest.raises(GalleryStagingConflictError, match="handoff differs"):
            with connector.transaction():
                GalleryObservationStagingRepository.begin_from_identity(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    identity=GalleryIdentityHandoff(
                        build_id,
                        gallery_id,
                        b"x" * 32,
                        scope_key,
                        locator_sha256,
                        False,
                    ),
                    now=21,
                )
        assert connector.fetch_one(
            "SELECT next_observation_id "
            "FROM operational_gallery_observation_allocators WHERE gallery_id = %s",
            (gallery_id,),
        ) == (1,)
        with connector.transaction():
            handle = GalleryObservationStagingRepository.begin_from_identity(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                identity=handoff,
                now=21,
            )
        assert _begin(connector, gate, turn, build_id, gallery_id, now=22) == handle
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_gallery_observation_staging_checkpoints"
        ) == (4,)
        before_resume = _request_snapshot(connector)
        with connector.transaction():
            resumed = GalleryObservationStagingRepository.resume(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                now=23,
            )
        assert resumed == handle
        assert _request_snapshot(connector) == before_resume

        files = tuple(_file_observation(index) for index in reversed(range(257)))
        first_byte_count = sum(file.content.size_bytes for file in files[:256])
        total_byte_count = sum(file.content.size_bytes for file in files)
        file_a = FileBatchCommand(
            files[:256],
            False,
            BatchAttempt(b"a" * 16, None),
        )
        first = _put_files(connector, gate, turn, handle, file_a, now=30)
        assert first.processed_byte_count == first_byte_count
        before_replay = _request_snapshot(connector)
        replay = _put_files(connector, gate, turn, handle, file_a, now=31)
        assert replay.replayed and replay.request_sha256 == first.request_sha256
        assert replay.processed_byte_count == first_byte_count
        assert _request_snapshot(connector) == before_replay
        connector.execute(
            "UPDATE operational_gallery_observation_staging_receipts "
            "SET next_processed_byte_count = %s "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (first_byte_count + 1, handle.staging_id, b"FILE"),
        )
        corrupt_replay = _request_snapshot(connector)
        with pytest.raises(GalleryStagingConflictError, match="checkpoint poststate"):
            _put_files(connector, gate, turn, handle, file_a, now=31)
        assert _request_snapshot(connector) == corrupt_replay
        connector.execute(
            "UPDATE operational_gallery_observation_staging_receipts "
            "SET next_processed_byte_count = %s "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (first_byte_count, handle.staging_id, b"FILE"),
        )

        file_b = FileBatchCommand(
            files[256:],
            True,
            BatchAttempt(b"b" * 16, b"a" * 16),
        )

        class _FixedHash:
            def digest(self) -> bytes:
                return first.request_sha256

        with monkeypatch.context() as context:
            context.setattr(staging_module, "sha256", lambda _value=b"": _FixedHash())
            with pytest.raises(GalleryStagingConflictError, match="collision"):
                _put_files(connector, gate, turn, handle, file_b, now=32)
        assert connector.fetch_one(
            "SELECT cursor, processed_byte_count "
            "FROM operational_gallery_observation_staging_checkpoints "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"FILE"),
        ) == (256, first_byte_count)

        file_done = _put_files(connector, gate, turn, handle, file_b, now=33)
        assert file_done.state == "COMPLETE" and file_done.cursor == 257
        assert file_done.processed_byte_count == total_byte_count
        assert file_done.root_page_sha256 is not None
        assert connector.fetch_one(
            "SELECT start_processed_byte_count, next_processed_byte_count "
            "FROM operational_gallery_observation_staging_receipts "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (handle.staging_id, b"FILE"),
        ) == (first_byte_count, total_byte_count)
        with pytest.raises(
            GalleryStagingConflictError,
            match="acknowledge the latest",
        ):
            _put_files(connector, gate, turn, handle, file_a, now=34)

        expected_file_tree = build_gallery_observation_tree(
            GalleryObservationComponent.FILE,
            tuple(
                GalleryObservationFileEntry(
                    position,
                    file_key(file.name_bytes),
                    file.content.file_sha256,
                    file.content.size_bytes,
                    file.device,
                    file.inode,
                    file.modified_ns,
                    file.changed_ns,
                )
                for position, file in enumerate(files)
            ),
        )
        assert file_done.root_page_sha256 == expected_file_tree.root_page_sha256

        directories = tuple(
            sorted(
                (_directory_observation(file) for file in files),
                key=lambda entry: entry.name_bytes,
            )
        )
        directory_a = DirectoryBatchCommand(
            directories[:192],
            False,
            BatchAttempt(b"c" * 16, None),
        )
        directory_b = DirectoryBatchCommand(
            directories[192:],
            True,
            BatchAttempt(b"d" * 16, b"c" * 16),
        )
        _put_directories(connector, gate, turn, handle, directory_a, now=40)
        directory_done = _put_directories(
            connector, gate, turn, handle, directory_b, now=41
        )
        expected_directory_tree = build_gallery_observation_tree(
            GalleryObservationComponent.DIRECTORY,
            tuple(
                GalleryObservationDirectoryEntry(
                    position,
                    entry.name_bytes,
                    entry.size_bytes,
                    entry.device,
                    entry.inode,
                    entry.modified_ns,
                    entry.changed_ns,
                    entry.file_type,
                )
                for position, entry in enumerate(directories)
            ),
        )
        assert (
            directory_done.root_page_sha256 == expected_directory_tree.root_page_sha256
        )

        tag_done = _put_tags(
            connector,
            gate,
            turn,
            handle,
            TagBatchCommand(
                (
                    TagObservation("artist", "Alice"),
                    TagObservation("language", "zh-Hant"),
                ),
                True,
                BatchAttempt(b"t" * 16, None),
            ),
            now=42,
        )
        assert tag_done.cursor == 2 and tag_done.root_page_sha256 is not None
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_tag_terms") == (2,)
        assert (
            connector.fetch_all(
                "SELECT value_sha256 FROM operational_canonical_value_uploads"
            )
            == []
        )

        metadata = GalleryObservationMetadata(
            12345,
            "A" * 33_000,
            "comment 資料",
            "uploader",
            100,
            101,
            102,
            1,
            257,
            257,
        )
        metadata_bytes = encode_gallery_observation_metadata(metadata)
        metadata_a = MetadataBatchCommand(
            metadata_bytes[:32_768],
            False,
            BatchAttempt(b"m" * 16, None),
        )
        metadata_first = _put_metadata(
            connector,
            gate,
            turn,
            handle,
            MetadataBatchCommand(
                metadata_bytes[:32_768],
                False,
                BatchAttempt(b"m" * 16, None),
            ),
            now=43,
        )
        assert metadata_first.cursor == 32_768
        assert connector.fetch_one(
            "SELECT phase FROM operational_gallery_observation_staging_metadata_parsers "
            "WHERE staging_id = %s",
            (handle.staging_id,),
        ) == ("TITLE",)
        assert _put_metadata(
            connector,
            gate,
            turn,
            handle,
            metadata_a,
            now=44,
        ).replayed
        metadata_done = _put_metadata(
            connector,
            gate,
            turn,
            handle,
            MetadataBatchCommand(
                metadata_bytes[32_768:],
                True,
                BatchAttempt(b"n" * 16, b"m" * 16),
            ),
            now=45,
        )
        assert metadata_done.cursor == len(metadata_bytes)

        match_a = _match(
            connector,
            gate,
            turn,
            handle,
            MatchBatchCommand(b"1" * 16, None),
            now=50,
        )
        assert (match_a.matched_count, match_a.state) == (256, "OPEN")
        match_before = _request_snapshot(connector)
        match_replay = _match(
            connector,
            gate,
            turn,
            handle,
            MatchBatchCommand(b"1" * 16, None),
            now=51,
        )
        assert match_replay.replayed
        assert _request_snapshot(connector) == match_before
        match_done = _match(
            connector,
            gate,
            turn,
            handle,
            MatchBatchCommand(b"2" * 16, b"1" * 16),
            now=52,
        )
        assert (match_done.matched_count, match_done.state) == (257, "COMPLETE")
        with pytest.raises(GalleryStagingConflictError, match="acknowledge the latest"):
            _match(
                connector,
                gate,
                turn,
                handle,
                MatchBatchCommand(b"1" * 16, None),
                now=53,
            )

        assert connector.fetch_all(
            "SELECT component, processed_byte_count "
            "FROM operational_gallery_observation_staging_checkpoints "
            "WHERE staging_id = %s AND component != %s AND level = 0 "
            "ORDER BY component",
            (handle.staging_id, b"FILE"),
        ) == [(b"DIRECTORY", 0), (b"METADATA", 0), (b"TAG", 0)]
        connector.execute(
            "UPDATE operational_gallery_observation_staging_receipts "
            "SET next_processed_byte_count = %s "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (total_byte_count + 1, handle.staging_id, b"FILE"),
        )
        with pytest.raises(GalleryStagingConflictError, match="FILE byte"):
            with connector.transaction():
                GalleryObservationStagingRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    handle=handle,
                    now=59,
                )
        assert (
            connector.fetch_all(
                "SELECT gallery_id FROM catalog_gallery_observation_stat"
            )
            == []
        )
        connector.execute(
            "UPDATE operational_gallery_observation_staging_receipts "
            "SET next_processed_byte_count = %s "
            "WHERE staging_id = %s AND component = %s AND level = 0",
            (total_byte_count, handle.staging_id, b"FILE"),
        )

        with pytest.raises(RuntimeError, match="seal crash"):
            with connector.transaction():
                GalleryObservationStagingRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    handle=handle,
                    now=60,
                )
                raise RuntimeError("seal crash")
        assert (
            connector.fetch_all("SELECT build_id FROM catalog_source_build_galleries")
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT gallery_id FROM catalog_gallery_observation_stat"
            )
            == []
        )

        seal_sql: list[str] = []
        connector.connection.set_trace_callback(seal_sql.append)
        try:
            with connector.transaction():
                sealed = GalleryObservationStagingRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    handle=handle,
                    now=61,
                )
        finally:
            connector.connection.set_trace_callback(None)
        assert sealed.state == "SEALED" and sealed.observation_id == 1
        authority_queries = [
            sql
            for sql in seal_sql
            if "OPERATIONAL_GALLERY_OBSERVATION_STAGING_RECEIPTS" in sql.upper()
        ]
        assert len(authority_queries) == 1
        assert not any(
            "COUNT(" in sql.upper() or "SUM(" in sql.upper() for sql in seal_sql
        )
        assert not any(
            "SELECT" in sql.upper()
            and "FROM CATALOG_GALLERY_OBSERVATION_FILE_ANCHORS" in sql.upper()
            for sql in seal_sql
        )
        assert connector.fetch_one(
            "SELECT file_count, byte_count FROM catalog_gallery_observation_stat "
            "WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, sealed.observation_id),
        ) == (257, total_byte_count)
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = 1",
            (build_id,),
        ) == (1,)
        expected_manifest = artifact_source_manifest_digest(
            sealed.observation_identity_sha256,
            1,
            1,
        )
        manifest = connector.fetch_one(
            "SELECT manifest_sha256, computed_at FROM catalog_gallery_manifests "
            "WHERE gallery_id = %s AND observation_id = %s "
            "AND manifest_policy_id = 1",
            (gallery_id, sealed.observation_id),
        )
        assert manifest[0] == expected_manifest
        assert manifest[1] != 61
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            patch(
                "h2hdb.vnext_manifest_family.database_unix_microseconds",
                side_effect=AssertionError("clock"),
            ),
            connector.transaction(),
        ):
            replayed_seal = GalleryObservationStagingRepository.seal(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                now=62,
            )
        assert replayed_seal.replayed and replayed_seal == sealed.__class__(
            sealed.build_id,
            sealed.gallery_id,
            sealed.observation_id,
            sealed.observation_identity_sha256,
            sealed.state,
            True,
        )
        connector.execute(
            "UPDATE catalog_gallery_manifests "
            "SET manifest_sha256 = %s WHERE gallery_id = %s "
            "AND observation_id = %s AND manifest_policy_id = 1",
            (b"x" * 32, gallery_id, sealed.observation_id),
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            connector.transaction(),
            pytest.raises(GalleryStagingConflictError, match="exact sealed"),
        ):
            GalleryObservationStagingRepository.seal(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                now=63,
            )
        connector.execute(
            "UPDATE catalog_gallery_manifests "
            "SET manifest_sha256 = %s WHERE gallery_id = %s "
            "AND observation_id = %s AND manifest_policy_id = 1",
            (expected_manifest, gallery_id, sealed.observation_id),
        )
        after_seal = _request_snapshot(connector)
        with pytest.raises(GalleryStagingNotReadyError, match="only OPEN"):
            _put_files(
                connector,
                gate,
                turn,
                handle,
                FileBatchCommand(
                    (),
                    True,
                    BatchAttempt(b"z" * 16, b"b" * 16),
                ),
                now=64,
            )
        assert _request_snapshot(connector) == after_seal

        # Assembly may seal the source build after the gallery response was
        # lost.  The exact terminal receipt remains replayable and read-only;
        # only fresh staging work is fenced to an OPEN build.
        connector.execute(
            "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
            "VALUES (%s, %s)",
            (build_id, 100),
        )
        connector.execute(
            "UPDATE catalog_source_build_states SET state = %s WHERE build_id = %s",
            ("SEALED", build_id),
        )
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            patch(
                "h2hdb.vnext_manifest_family.database_unix_microseconds",
                side_effect=AssertionError("clock"),
            ),
            connector.transaction(),
        ):
            sealed_build_replay = GalleryObservationStagingRepository.seal(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                now=65,
            )
        assert sealed_build_replay.replayed
        assert sealed_build_replay == replayed_seal
    finally:
        connector.close()


def test_metadata_256_leaf_boundary_carries_to_one_minimal_root(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "gallery-frontier-carry.sqlite3")
    try:
        gate, turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, turn)
        handle = _begin(connector, gate, turn, build_id, gallery_id, now=20)
        encoded = encode_gallery_observation_metadata(
            GalleryObservationMetadata(
                1,
                "x" * (255 * 32_768),
                "",
                "",
                1,
                2,
                3,
                1,
                0,
                0,
            )
        )
        chunks = tuple(
            encoded[offset : offset + 32_768]
            for offset in range(0, len(encoded), 32_768)
        )
        assert len(chunks) == 256

        previous: bytes | None = None
        final_command: MetadataBatchCommand | None = None
        final_receipt: GalleryStagingReceipt | None = None
        for position, chunk in enumerate(chunks):
            operation = (position + 1).to_bytes(16, "big")
            final_command = MetadataBatchCommand(
                chunk,
                position == len(chunks) - 1,
                BatchAttempt(operation, previous),
            )
            final_receipt = _put_metadata(
                connector,
                gate,
                turn,
                handle,
                final_command,
                now=30 + position,
            )
            previous = operation

        assert final_command is not None and final_receipt is not None
        assert final_receipt.state == "COMPLETE"
        assert final_receipt.cursor == len(encoded)
        assert final_receipt.root_page_sha256 is not None
        assert connector.fetch_one(
            "SELECT level, subtree_item_count "
            "FROM catalog_gallery_observation_page_descriptors "
            "WHERE page_sha256 = %s",
            (final_receipt.root_page_sha256,),
        ) == (1, len(encoded))
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_gallery_observation_page_children "
            "WHERE parent_sha256 = %s",
            (final_receipt.root_page_sha256,),
        ) == (256,)
        assert connector.fetch_all(
            "SELECT q.level, f.position "
            "FROM operational_gallery_observation_staging_frontiers f "
            "JOIN operational_gallery_observation_staging_page_requests q "
            "ON q.request_sha256 = f.request_sha256 "
            "WHERE q.staging_id = %s AND q.component = %s",
            (handle.staging_id, b"METADATA"),
        ) == [(1, 0)]
        before_replay = _request_snapshot(connector)
        replayed = _put_metadata(
            connector,
            gate,
            turn,
            handle,
            final_command,
            now=300,
        )
        assert replayed.replayed and replayed == GalleryStagingReceipt(
            final_receipt.request_sha256,
            final_receipt.component,
            final_receipt.cursor,
            final_receipt.processed_byte_count,
            final_receipt.state,
            final_receipt.root_page_sha256,
            True,
        )
        assert _request_snapshot(connector) == before_replay
    finally:
        connector.close()


def test_identical_observation_on_a_later_build_reuses_canonical_identity(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gallery-reuse.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        first_build, gallery_id = _seed_working_gallery(connector, first_turn)
        metadata_bytes = encode_gallery_observation_metadata(
            GalleryObservationMetadata(1, "", "", "", 1, 2, 3, 1, 0, 0)
        )

        def stage_empty_observation(
            turn: IngestTurn,
            build_id: bytes,
            *,
            now: int,
        ) -> GalleryStagingHandle:
            handle = _begin(
                connector,
                gate,
                turn,
                build_id,
                gallery_id,
                now=now,
            )
            _put_files(
                connector,
                gate,
                turn,
                handle,
                FileBatchCommand((), True, BatchAttempt(b"f" * 16, None)),
                now=now + 1,
            )
            _put_directories(
                connector,
                gate,
                turn,
                handle,
                DirectoryBatchCommand((), True, BatchAttempt(b"d" * 16, None)),
                now=now + 2,
            )
            _put_tags(
                connector,
                gate,
                turn,
                handle,
                TagBatchCommand((), True, BatchAttempt(b"t" * 16, None)),
                now=now + 3,
            )
            _put_metadata(
                connector,
                gate,
                turn,
                handle,
                MetadataBatchCommand(
                    metadata_bytes,
                    True,
                    BatchAttempt(b"m" * 16, None),
                ),
                now=now + 4,
            )
            _match(
                connector,
                gate,
                turn,
                handle,
                MatchBatchCommand(b"v" * 16, None),
                now=now + 5,
            )
            return handle

        def seal_observation(
            turn: IngestTurn,
            handle: GalleryStagingHandle,
            *,
            now: int,
        ) -> GalleryStagingSeal:
            with connector.transaction():
                return GalleryObservationStagingRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    handle=handle,
                    now=now,
                )

        first_handle = stage_empty_observation(
            first_turn,
            first_build,
            now=20,
        )
        first_seal = seal_observation(first_turn, first_handle, now=26)
        assert first_seal.state == "SEALED"
        assert first_seal.observation_id == first_handle.observation_id == 1

        with connector.transaction():
            second_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"j" * 16,
                now=100_012,
                lease_duration=100_000,
            )
        second_build = b"c" * 16
        scope_key, manifest_policy_id = connector.fetch_one(
            "SELECT scope.scope_key, policy.manifest_policy_id "
            "FROM catalog_source_build_descriptor_seals seal "
            "JOIN catalog_source_build_scope_keys scope "
            "ON scope.build_id = seal.build_id "
            "JOIN catalog_source_build_manifest_policy_ids policy "
            "ON policy.build_id = seal.build_id WHERE seal.build_id = %s",
            (first_build,),
        )
        seed_source_build(
            connector,
            build_id=second_build,
            scope_key=scope_key,
            manifest_policy_id=manifest_policy_id,
            state="OPEN",
            created_at=100_013,
        )
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (second_build, second_turn.generation),
        )
        connector.execute(
            "UPDATE operational_source_working_builds "
            "SET build_id = %s, assigned_at = %s WHERE slot = 1",
            (second_build, 100_013),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_expected_gallery "
            "(build_id, position, gallery_id) VALUES (%s, 0, %s)",
            (second_build, gallery_id),
        )

        second_handle = stage_empty_observation(
            second_turn,
            second_build,
            now=100_020,
        )
        connector.execute(
            "UPDATE catalog_gallery_observation_stat SET byte_count = 1 "
            "WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, first_seal.observation_id),
        )
        with pytest.raises(GalleryStagingConflictError, match="stat"):
            seal_observation(second_turn, second_handle, now=100_026)
        assert connector.fetch_one(
            "SELECT state FROM operational_gallery_observation_stagings "
            "WHERE staging_id = %s",
            (second_handle.staging_id,),
        ) == ("OPEN",)
        connector.execute(
            "UPDATE catalog_gallery_observation_stat SET byte_count = 0 "
            "WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, first_seal.observation_id),
        )
        reused = seal_observation(second_turn, second_handle, now=100_028)
        assert second_handle.observation_id == 2
        assert reused.state == "REUSED"
        assert reused.observation_id == first_seal.observation_id
        assert (
            reused.observation_identity_sha256 == first_seal.observation_identity_sha256
        )
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = %s",
            (second_build, gallery_id),
        ) == (first_seal.observation_id,)
        assert connector.fetch_one(
            "SELECT state FROM operational_gallery_observation_stagings "
            "WHERE staging_id = %s",
            (second_handle.staging_id,),
        ) == ("REUSED",)
        before_replay = _request_snapshot(connector)
        with connector.transaction():
            replayed = GalleryObservationStagingRepository.seal(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=second_turn,
                handle=second_handle,
                now=100_030,
            )
        assert replayed.replayed and replayed == GalleryStagingSeal(
            reused.build_id,
            reused.gallery_id,
            reused.observation_id,
            reused.observation_identity_sha256,
            reused.state,
            True,
        )
        assert _request_snapshot(connector) == before_replay
    finally:
        connector.close()


def test_stale_turn_is_zero_write_and_explicit_takeover_changes_only_claim(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gallery-takeover.sqlite3")
    try:
        gate, old_turn = _authorities(connector)
        build_id, gallery_id = _seed_working_gallery(connector, old_turn)
        handle = _begin(connector, gate, old_turn, build_id, gallery_id, now=20)
        before = _request_snapshot(connector)

        with connector.transaction():
            new_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=100_011,
                lease_duration=100_000,
            )
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (build_id, new_turn.generation),
        )

        with pytest.raises(IngestFenceUnavailableError, match="stale"):
            _put_files(
                connector,
                gate,
                old_turn,
                handle,
                FileBatchCommand((), True, BatchAttempt(b"a" * 16, None)),
                now=100_012,
            )
        assert _request_snapshot(connector) == before

        with connector.transaction():
            taken = GalleryObservationStagingRepository.takeover(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=new_turn,
                handle=handle,
                now=100_013,
            )
        assert taken.ingest_generation == new_turn.generation
        assert taken.claim_generation == handle.claim_generation + 1
        assert connector.fetch_one(
            "SELECT ingest_generation, claim_generation "
            "FROM operational_gallery_observation_staging_claims "
            "WHERE staging_id = %s",
            (handle.staging_id,),
        ) == (new_turn.generation, 1)
    finally:
        connector.close()
