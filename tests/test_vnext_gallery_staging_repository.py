from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

import h2hdb.vnext_gallery_staging_repository as staging_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
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
    GalleryObservationNodeKind,
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
    MaintenanceGateRepository,
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


def _seed_canonical_identity(
    connector: SQLiteConnector,
    *,
    digest_domain: str,
    payload: bytes,
    now: int,
) -> bytes:
    value_sha256 = canonical_value_digest(digest_domain, payload)
    tree = build_canonical_value_tree(value_sha256, len(payload), (payload,))
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, digest_domain.encode("ascii"), len(payload), now),
    )
    for encoded in tree.pages:
        page = decode_canonical_value_page(encoded.page_bytes)
        connector.execute(
            "INSERT INTO catalog_canonical_value_pages "
            "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
            (encoded.page_sha256, value_sha256, encoded.page_bytes),
        )
        connector.execute(
            "INSERT INTO catalog_canonical_value_page_descriptors "
            "(page_sha256, value_sha256, level, page_position, "
            "subtree_item_count) VALUES (%s, %s, %s, %s, %s)",
            (
                encoded.page_sha256,
                value_sha256,
                page.level,
                page.page_position,
                page.subtree_byte_count,
            ),
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
    scope = sha256(b"test scope").digest()
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, %s)",
        (scope, b"filesystem", root, 1),
    )
    connector.execute(
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, NULL)",
        (build_id, scope, 1, "OPEN", 12),
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
    connector.execute(
        "INSERT INTO catalog_gallery_identities "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (1, gallery_key(scope, locator), scope, locator),
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
        context.setattr(staging_module, "sha256", lambda _value=b"": _SameDigest())
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
        with connector.transaction():
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
                now=63,
            )
        assert _request_snapshot(connector) == after_seal
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
            "SELECT scope_key, manifest_policy_id FROM catalog_source_builds "
            "WHERE build_id = %s",
            (first_build,),
        )
        connector.execute(
            "INSERT INTO catalog_source_builds "
            "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
            "VALUES (%s, %s, %s, %s, %s, NULL)",
            (second_build, scope_key, manifest_policy_id, "OPEN", 100_013),
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
        connector.execute(
            "DELETE FROM catalog_gallery_observation_stat "
            "WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, first_seal.observation_id),
        )
        with pytest.raises(GalleryStagingConflictError, match="stat"):
            seal_observation(second_turn, second_handle, now=100_027)
        connector.execute(
            "INSERT INTO catalog_gallery_observation_stat "
            "(gallery_id, observation_id, file_count, byte_count) "
            "VALUES (%s, %s, 0, 0)",
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
