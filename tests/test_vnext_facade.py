from __future__ import annotations

import inspect
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity
from vnext_catalog_registry_fixtures import (
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import seed_snapshot_manifest
from vnext_publication_fixtures import (
    seed_catalog_publication,
    seed_catalog_publication_title,
    seed_publication_commit,
    seed_publication_identity,
)

from h2hdb import (
    CatalogDiscoveryPage,
    CatalogRecentOrder,
    StorageObjectKey,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextDownloadQueueFacade,
    open_database,
)
from h2hdb.catalog_search import iter_search_lexemes
from h2hdb.config_loader import (
    CoreConfig,
    DatabaseAccessMode,
    DatabaseConfig,
)
from h2hdb.domain import CatalogRevision
from h2hdb.ports import CatalogReader
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import SchemaEpochAdmissionError, SchemaEpochDefinition
from h2hdb.sql_connector import DatabaseReadOnlyError, SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    ArtifactSemanticInputFamily,
    CatalogArtifactFamily,
    ensure_artifact_semantic_input_family,
    ensure_catalog_artifact_family,
)
from h2hdb.vnext_catalog_reader_repository import VNextCatalogReadError
from h2hdb.vnext_download_ingest_repository import DownloadIngestRepository
from h2hdb.vnext_identity import (
    CanonicalValueChunk,
    CanonicalValuePage,
    GalleryObservationNodeKind,
    artifact_id,
    artifact_policy_digest,
    artifact_semantics_digest,
    artifact_storage_key_digest,
    canonical_value_digest,
    canonical_value_page_digest,
    encode_artifact_policy,
    encode_artifact_semantics,
    encode_canonical_value_page,
    encode_source_relative_locator,
    gallery_key,
    publication_id,
    publication_key,
)
from h2hdb.vnext_queue_repository import VNextDownloadRequest, VNextQueueRepository
from h2hdb.vnext_schema_provider import VNextSchemaProviderUnavailableError
from h2hdb.vnext_transaction import VNextUnitOfWork

if TYPE_CHECKING:
    _catalog_reader: CatalogReader = VNextCatalogFacade(CoreConfig())
    _admin_facade = VNextDatabaseAdminFacade(CoreConfig())
    _queue_request: VNextDownloadRequest = VNextDownloadQueueFacade(
        CoreConfig()
    ).request_download(1)


def _config(
    path: Path,
    *,
    read_only: bool = False,
    backend: str = "sqlite",
) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type=backend,
            database=str(path) if backend == "sqlite" else "h2hdb-test",
            access_mode=(
                DatabaseAccessMode.read_only
                if read_only
                else DatabaseAccessMode.read_write
            ),
        )
    )


def _generated_database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def test_database_admin_facade_initializes_retries_and_fully_checks_fresh_epoch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin-facade.sqlite3"
    facade = VNextDatabaseAdminFacade(_config(path))

    initialized = facade.initialize()
    retried = facade.initialize()
    checked = facade.check()

    assert initialized.state == "READY"
    assert initialized.transitioned_to_ready
    assert not initialized.resumed_build
    assert initialized.bootstrap_seed_ids
    assert retried.state == "READY"
    assert retried.resumed_build
    assert not retried.transitioned_to_ready
    assert checked.state == "READY"
    assert not checked.transitioned_to_ready


def test_database_admin_facade_fully_checks_through_read_only_config(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin-read-only-check.sqlite3"
    VNextDatabaseAdminFacade(_config(path)).initialize()

    checked = VNextDatabaseAdminFacade(_config(path, read_only=True)).check()

    assert checked.state == "READY"
    assert checked.resumed_build
    assert not checked.transitioned_to_ready


def test_public_open_database_fully_checks_epoch_before_returning_catalog_facade(
    tmp_path: Path,
) -> None:
    path = tmp_path / "public-open.sqlite3"
    VNextDatabaseAdminFacade(_config(path)).initialize()

    reader = open_database(_config(path, read_only=True))

    assert isinstance(reader, VNextCatalogFacade)
    assert isinstance(reader, CatalogReader)

    with SQLiteConnector(str(path)) as connector:
        connector.execute("CREATE TABLE unexpected_schema_drift (id INTEGER)")

    with pytest.raises(SchemaEpochAdmissionError, match="outside this epoch manifest"):
        open_database(_config(path, read_only=True))


def test_database_admin_facade_readiness_is_read_only_and_constant_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-readiness.sqlite3"
    initialized = VNextDatabaseAdminFacade(_config(path)).initialize()
    statements: list[str] = []

    class CountingReadOnlySQLiteConnector(SQLiteConnector):
        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            statements.append(query)
            return super().fetch_one(query, data)

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            statements.append(query)
            return super().fetch_all(query, data)

    config = _config(path, read_only=True)
    context = replace(
        RepositoryContext.from_config(config),
        SQLConnector=lambda: CountingReadOnlySQLiteConnector(
            database=str(path),
            read_only=True,
        ),
    )
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: context),
    )

    readiness = VNextDatabaseAdminFacade(config).check_readiness()

    assert readiness.state == "READY"
    assert readiness.manifest_sha256 == initialized.manifest_sha256
    assert len(statements) == 2
    assert "sqlite_master" in statements[0]
    assert "FROM h2hdb_schema_epoch" in statements[1]


def test_database_admin_facade_blocked_default_never_opens_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-blocked.sqlite3"
    config = _config(path)
    connector_opens = 0

    def forbidden_connector() -> SQLConnector:
        nonlocal connector_opens
        connector_opens += 1
        raise AssertionError("blocked provider must fail before opening the database")

    context = replace(
        RepositoryContext.from_config(config),
        SQLConnector=forbidden_connector,
    )

    class BlockedGeneratedProvider:
        def __init__(self, backend: str) -> None:
            assert backend == "sqlite"

        @property
        def definition(self) -> SchemaEpochDefinition:
            raise VNextSchemaProviderUnavailableError("generated provider is blocked")

    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: context),
    )
    monkeypatch.setattr(
        "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider",
        BlockedGeneratedProvider,
    )
    facade = VNextDatabaseAdminFacade(config)

    for operation in (facade.initialize, facade.check, facade.check_readiness):
        with pytest.raises(VNextSchemaProviderUnavailableError, match="blocked"):
            operation()

    assert connector_opens == 0
    assert not path.exists()


def _canonical(connector: SQLiteConnector, domain: str, payload: bytes) -> bytes:
    value_sha256 = canonical_value_digest(domain, payload)
    chunks = () if not payload else (CanonicalValueChunk(0, payload),)
    page = CanonicalValuePage(
        value_sha256,
        GalleryObservationNodeKind.LEAF,
        0,
        0,
        len(payload),
        chunks,
    )
    page_bytes = encode_canonical_value_page(page)
    page_sha256 = canonical_value_page_digest(page_bytes)
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain.encode("ascii"),
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        subtree_item_count=len(payload),
        allocated_at=1,
    )
    return value_sha256


def _publication_commit_fixture(
    connector: SQLiteConnector,
    *,
    snapshot_manifest_sha256: bytes,
) -> None:
    """Install the sealed physical commit graph consumed by the reader."""

    receipt_id = b"r" * 16
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        statements: tuple[tuple[str, tuple[object, ...]], ...] = (
            (
                "INSERT INTO catalog_source_revision_descriptors "
                "(source_revision, channel, snapshot_manifest_sha256) "
                "VALUES (%s, %s, %s)",
                (1, b"default", snapshot_manifest_sha256),
            ),
            (
                "INSERT INTO catalog_revision_descriptors "
                "(revision, publication_count, artifact_count) "
                "VALUES (%s, %s, %s)",
                (1, 1, 1),
            ),
            (
                "INSERT INTO catalog_publication_generation_nodes "
                "(generation) VALUES (%s)",
                (1,),
            ),
            (
                "INSERT INTO catalog_publication_generation_successors "
                "(successor_generation, predecessor_generation) VALUES (%s, %s)",
                (1, 0),
            ),
        )
        for query, parameters in statements:
            connector.execute(query, parameters)
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=b"c" * 16,
            revision=1,
            source_revision=1,
            generation=1,
            preparation_id=b"p" * 16,
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=1,
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=1_000_000,
            channel=b"default",
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _catalog_fixture(connector: SQLiteConnector) -> dict[str, object]:
    source_root = _canonical(
        connector,
        "source_root_v1",
        b"\x00\x00\x00\x01\x00\x00\x00\x00",
    )
    scope_key = seed_source_scope(
        connector,
        source_root_sha256=source_root,
    ).scope_key
    locator = _canonical(
        connector,
        "source_relative_locator_v1",
        encode_source_relative_locator(("gallery-one",)),
    )
    connector.execute(
        "INSERT INTO catalog_source_locator_identity "
        "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
        (locator, b"gallery-one"),
    )
    gallery_key_value = gallery_key(scope_key, locator)
    seed_gallery_identity(
        connector,
        gallery_id=1,
        gallery_key=gallery_key_value,
        scope_key=scope_key,
        locator_sha256=locator,
    )

    source_title = _canonical(connector, "source_title_utf8_v1", b"Source")
    display_title = _canonical(connector, "display_title_utf8_v1", b"Display")
    sort_title = _canonical(connector, "title_sort_utf8_v1", b"display")
    summary = _canonical(connector, "catalog_summary_utf8_v1", b"Summary")
    language = _canonical(connector, "catalog_language_utf8_v1", b"en")
    seed_title_sort_policy(connector, unicode_data_version=b"15.0.0")
    seed_display_title_policy(connector)
    connector.execute(
        "INSERT INTO catalog_display_title_choices "
        "(display_title_policy_id, source_title_sha256, source_gallery_name, "
        "title_sha256) VALUES (1, %s, %s, %s)",
        (source_title, b"gallery-one", display_title),
    )
    connector.execute(
        "INSERT INTO catalog_title_sorts "
        "(title_sort_policy_id, title_sha256, sort_title_sha256) "
        "VALUES (1, %s, %s)",
        (display_title, sort_title),
    )

    gid = 123
    publication_key_value = publication_key(gid)
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (gid, 2_000_000),
    )
    assert seed_publication_identity(connector, gid=gid).publication_key == (
        publication_key_value
    )
    connector.execute(
        "INSERT INTO catalog_source_gallery_name_gids "
        "(source_gallery_name, gid) VALUES (%s, %s)",
        (b"gallery-one", gid),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_source_name_accesses "
        "(gallery_id, source_gallery_name) VALUES (1, %s)",
        (b"gallery-one",),
    )
    snapshot_manifest = _canonical(
        connector,
        "source_snapshot_manifest_v1",
        b"facade-source-snapshot",
    )
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot_manifest,
        gallery_count=1,
        file_count=1,
        byte_count=1,
    )
    _publication_commit_fixture(
        connector,
        snapshot_manifest_sha256=snapshot_manifest,
    )
    seed_catalog_publication(
        connector,
        revision=1,
        publication_key=publication_key_value,
        gallery_id=1,
        summary_sha256=summary,
        language_sha256=language,
        modified_at=3_000_000,
        source_title_sha256=source_title,
    )
    connector.execute(
        "INSERT INTO catalog_publication_order "
        "(revision, position, publication_key) VALUES (1, 0, %s)",
        (publication_key_value,),
    )
    seed_catalog_publication_title(
        connector,
        revision=1,
        publication_key=publication_key_value,
        source_title_sha256=source_title,
        source_gallery_name=b"gallery-one",
    )
    search_lexemes = tuple(dict.fromkeys(iter_search_lexemes((b"Display", b"Source"))))
    connector.execute(
        "INSERT INTO catalog_search_documents "
        "(revision, publication_key, row_count) VALUES (1, %s, %s)",
        (publication_key_value, len(search_lexemes)),
    )
    for lexeme in search_lexemes:
        lexeme_sha256 = _canonical(connector, "search_lexeme_utf8_v1", lexeme)
        connector.execute(
            "INSERT INTO catalog_search_lexemes (value_sha256) VALUES (%s)",
            (lexeme_sha256,),
        )
        connector.execute(
            "INSERT INTO catalog_search_postings "
            "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
            (lexeme_sha256, publication_key_value),
        )
    connector.execute(
        "INSERT INTO catalog_language_facet_order "
        "(revision, position, language_sha256, occurrence_count) "
        "VALUES (1, 0, %s, 1)",
        (language,),
    )
    connector.execute(
        "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (1, 1)"
    )
    adapter_id = b"facade-test-adapter"
    policy_fingerprint = sha256(b"facade-test-artifact-policy").digest()
    policy = _canonical(
        connector,
        "artifact_policy_v3",
        encode_artifact_policy(2, adapter_id, policy_fingerprint),
    )
    assert policy == artifact_policy_digest(2, adapter_id, policy_fingerprint)
    semantics_record = seed_artifact_policy_semantics(
        connector,
        artifact_algorithm_version=2,
        adapter_id=adapter_id,
        policy_fingerprint_sha256=policy_fingerprint,
    )
    assert semantics_record.policy_component_sha256 == policy
    component_specs = (
        ("artifact_source_manifest_v1", b"source"),
        ("artifact_effective_content_v1", b"members"),
        ("artifact_effective_content_v1", b"content"),
        ("artifact_selected_v1", b"selected"),
        ("artifact_owner_v1", b"owner"),
    )
    components = tuple(
        _canonical(connector, domain, payload) for domain, payload in component_specs
    )
    source_manifest, member_plan, effective_content, selected, owner = components
    semantics_payload = encode_artifact_semantics(
        source_manifest,
        member_plan,
        effective_content,
        selected,
        owner,
        policy,
    )
    semantics = _canonical(
        connector,
        "artifact_semantics_v1",
        semantics_payload,
    )
    assert semantics == artifact_semantics_digest(
        source_manifest,
        member_plan,
        effective_content,
        selected,
        owner,
        policy,
    )
    ensure_artifact_semantic_input_family(
        connector,
        ArtifactSemanticInputFamily(
            artifact_semantics_sha256=semantics,
            source_manifest_component_sha256=source_manifest,
            member_plan_component_sha256=member_plan,
            effective_content_component_sha256=effective_content,
            selected_component_sha256=selected,
            owner_component_sha256=owner,
            policy_component_sha256=policy,
        ),
    )
    artifact_bytes = b"facade-artifact"
    artifact_sha256 = sha256(artifact_bytes).digest()
    artifact_identifier = artifact_id(gid, artifact_sha256)
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes) VALUES (%s, %s)",
        (artifact_sha256, len(artifact_bytes)),
    )
    ensure_catalog_artifact_family(
        connector,
        CatalogArtifactFamily(
            1,
            publication_key_value,
            artifact_sha256,
            semantics,
            b"download-123.bin",
            b"application/octet-stream",
            0,
        ),
    )
    storage_key = StorageObjectKey("facade-v2", ("acquisition", "123"))
    storage_key_sha256 = artifact_storage_key_digest(
        storage_key.codec,
        storage_key.segments,
    )
    connector.execute(
        "INSERT INTO catalog_storage_object_key_identities "
        "(storage_object_key_sha256, key_codec, segment_count) VALUES (%s, %s, 2)",
        (storage_key_sha256, storage_key.codec.encode("ascii")),
    )
    for position, segment in enumerate(storage_key.segments):
        connector.execute(
            "INSERT INTO catalog_storage_object_key_segments "
            "(storage_object_key_sha256, segment_position, key_segment) "
            "VALUES (%s, %s, %s)",
            (storage_key_sha256, position, segment.encode("utf-8")),
        )
    connector.execute(
        "INSERT INTO catalog_storage_objects "
        "(revision, publication_key, resource_kind, storage_object_key_sha256, "
        "storage_object_sha256, size_bytes, modified_at) "
        "VALUES (1, %s, %s, %s, %s, %s, 3000000)",
        (
            publication_key_value,
            b"acquisition",
            storage_key_sha256,
            artifact_sha256,
            len(artifact_bytes),
        ),
    )
    return {
        "artifact_id": artifact_identifier.decode("ascii"),
        "artifact_sha256": artifact_sha256,
        "publication_id": publication_id(gid).decode("ascii"),
    }


def test_catalog_facade_reads_every_public_shape_from_read_only_generated_sqlite(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog-facade.sqlite3"
    connector = _generated_database(path)
    try:
        values = _catalog_fixture(connector)
        assert not connector.check_table_exists("database_maintenance")
        assert not connector.check_table_exists("h2hdb_database_maintenance")
    finally:
        connector.close()

    facade = VNextCatalogFacade(_config(path, read_only=True))
    revision = facade.get_catalog_revision()
    page = facade.discover_publications(revision=revision, limit=10)
    recent = facade.list_recent_publications(
        order=CatalogRecentOrder.DOWNLOADED,
        revision=revision,
    )
    publication = facade.get_publication(
        cast(str, values["publication_id"]),
        revision=revision,
    )
    by_name = facade.get_publications_by_artifact_names(
        ["download-123.bin", "missing.bin", "download-123.bin"],
        revision=revision,
    )
    artifact = facade.get_artifact(
        cast(str, values["artifact_id"]),
        revision=revision,
    )

    assert revision == CatalogRevision(
        revision.revision,
        revision.published_at,
        1,
        1,
    )
    assert isinstance(page, CatalogDiscoveryPage)
    assert page.publications == (publication,)
    assert recent.order is CatalogRecentOrder.DOWNLOADED
    assert recent.publications == (publication,)
    assert by_name == {"download-123.bin": publication}
    assert publication is not None
    assert publication.artifacts == (artifact,)
    assert artifact is not None
    assert (
        artifact.storage_object.sha256
        == cast(
            bytes,
            values["artifact_sha256"],
        ).hex()
    )


class _CountingClock:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)
        self.calls = 0

    def __call__(self) -> int:
        self.calls += 1
        return next(self._values)


def test_download_queue_facade_owns_short_transactions_and_bounded_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "queue-facade.sqlite3"
    connector = _generated_database(path)
    connector.close()
    tokens = iter((b"a" * 16, b"b" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: next(tokens),
    )
    clock = _CountingClock(10, 11)
    facade = VNextDownloadQueueFacade(_config(path), clock=clock)

    first = facade.request_download(42, "https://example.invalid/42")
    second = facade.ensure_download_request(100)
    page_one = facade.list_download_requests(limit=1)
    page_two = facade.list_download_requests(after_gid=page_one[-1].gid, limit=1)

    assert clock.calls == 2
    assert first.requested_at == 10
    assert second.request.requested_at == 11
    assert tuple(request.gid for request in page_one + page_two) == (42, 100)
    assert facade.get_download_request(42) == first
    assert facade.complete_download_request(first)
    assert facade.get_download_request(42) is None

    read_only = VNextDownloadQueueFacade(
        _config(path, read_only=True),
        clock=_CountingClock(12),
    )
    assert read_only.get_download_request(100) == second.request
    assert read_only.list_download_requests(limit=1) == (second.request,)
    with pytest.raises(DatabaseReadOnlyError):
        read_only.request_download(200)


def test_download_queue_facade_atomically_finishes_and_recovers_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "queue-finish-facade.sqlite3"
    connector = _generated_database(path)
    connector.close()
    request_tokens = iter((b"a" * 16, b"b" * 16, b"c" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: next(request_tokens),
    )
    download_tokens = iter((b"d" * 16, b"e" * 16))
    monkeypatch.setattr(
        "h2hdb.vnext_download_ingest_repository._new_download_owner_token",
        lambda: next(download_tokens),
    )
    clock = _CountingClock(10, 20, 30, 31, 32, 60, 70, 80)
    facade = VNextDownloadQueueFacade(_config(path), clock=clock)

    original = facade.request_download(42, "https://example.invalid/42")
    turn = facade.claim_download_turn(lease_duration_microseconds=100)
    handoff = facade.finish_download_turn(turn, original)
    assert handoff.requested_at == 30
    assert facade.get_download_request(42) is None
    assert not facade.is_download_handoff_complete(handoff)

    replacement = facade.request_download(42, "https://example.invalid/new-42")
    assert facade.finish_download_turn(turn, original) == handoff
    assert facade.get_download_request(42) == replacement

    connector = SQLiteConnector(str(path))
    connector.connect()
    try:
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with connector.transaction():
            ingest_turn = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=40,
                lease_duration=100,
            )
        with connector.transaction():
            DownloadIngestRepository.complete_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                ingest_turn,
                now=50,
            )
    finally:
        connector.close()
    assert facade.is_download_handoff_complete(handoff)

    missing = facade.request_download(404)
    missing_turn = facade.claim_download_turn(lease_duration_microseconds=100)
    facade.finish_missing_download_turn(missing_turn, missing, 404)
    assert facade.get_download_request(404) is None

    connector = SQLiteConnector(str(path))
    connector.connect()
    try:
        assert connector.fetch_one(
            "SELECT gid FROM operational_removed_gids WHERE gid = %s",
            (404,),
        ) == (404,)
    finally:
        connector.close()
    assert facade.record_gallery_found(404) == 1


def test_download_queue_facade_rolls_back_repository_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "queue-rollback.sqlite3"
    connector = _generated_database(path)
    connector.close()

    def insert_then_fail(
        work: object,
        *,
        gid: int,
        url: str,
        requested_at: int,
    ) -> object:
        unit = cast(Any, work)
        unit.connector.execute(
            "INSERT INTO operational_download_requests "
            "(gid, url, request_token, requested_at) VALUES (%s, %s, %s, %s)",
            (gid, url, b"r" * 16, requested_at),
        )
        raise RuntimeError("injected failure")

    monkeypatch.setattr(
        VNextQueueRepository,
        "ensure_download_request",
        insert_then_fail,
    )
    facade = VNextDownloadQueueFacade(_config(path), clock=lambda: 99)
    with pytest.raises(RuntimeError, match="injected failure"):
        facade.ensure_download_request(77, "https://example.invalid/77")

    connector = SQLiteConnector(str(path))
    connector.connect()
    try:
        assert (
            connector.fetch_one(
                "SELECT gid FROM operational_download_requests WHERE gid = %s",
                (77,),
            )
            == ()
        )
    finally:
        connector.close()


class _MariaRecorder:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = iter(rows)
        self.events: list[str] = []
        self.selects: list[tuple[str, tuple[object, ...]]] = []
        self.mutations: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> _MariaRecorder:
        self.events.append("connect")
        return self

    def __exit__(self, *args: object) -> None:
        self.events.append("close")

    @contextmanager
    def transaction(self) -> Generator[None]:
        self.events.append("begin-write")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

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
        return next(self._rows)

    def fetch_all(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> list[tuple[object, ...]]:
        self.selects.append((query, data))
        return []

    def execute(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> None:
        self.mutations.append((query, data))

    def execute_affected(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> int:
        self.mutations.append((query, data))
        return 1


class _FacadeContext:
    def __init__(self, *recorders: _MariaRecorder) -> None:
        self.sql_type = "mariadb"
        remaining = iter(recorders)
        self.SQLConnector: Callable[[], _MariaRecorder] = lambda: next(remaining)


def test_facades_preserve_mariadb_read_snapshot_and_for_update_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(Path("unused"), backend="mariadb")
    catalog_snapshot = _MariaRecorder(
        [
            (7, 0, 0, 1_000_000, 1),
            (7, 0, 0, 1_000_000, 1),
        ]
    )
    catalog_fence = _MariaRecorder([(7, 0, 0, 1_000_000, 1)])
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: _FacadeContext(catalog_snapshot, catalog_fence)),
    )
    revision = VNextCatalogFacade(config).get_catalog_revision()

    assert revision.revision == 7
    assert catalog_snapshot.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "close",
    ]
    assert catalog_fence.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "close",
    ]
    catalog_selects = (*catalog_snapshot.selects, *catalog_fence.selects)
    assert all(" FOR UPDATE" not in query for query, _data in catalog_selects)
    assert len(catalog_selects) == 3
    assert all(
        "catalog_publication_commit_head_receipts" in query
        for query, _data in catalog_selects
    )
    assert not any(
        "maintenance" in query.casefold() for query, _data in catalog_selects
    )

    queue_recorder = _MariaRecorder([(), ()])
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: _FacadeContext(queue_recorder)),
    )
    monkeypatch.setattr(
        "h2hdb.vnext_queue_repository.secrets.token_bytes",
        lambda size: b"q" * 16,
    )
    request = VNextDownloadQueueFacade(config, clock=lambda: 55).request_download(42)

    assert request.requested_at == 55
    assert queue_recorder.events == ["connect", "begin-write", "commit", "close"]
    assert len(queue_recorder.selects) == 2
    assert all(query.endswith(" FOR UPDATE") for query, _data in queue_recorder.selects)
    assert len(queue_recorder.mutations) == 1
    assert "INSERT INTO operational_download_requests" in queue_recorder.mutations[0][0]


def test_catalog_facade_rejects_a_head_change_seen_by_the_fresh_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(Path("unused"), backend="mariadb")
    snapshot = _MariaRecorder(
        [
            (7, 0, 0, 1_000_000, 1),
            (7, 0, 0, 1_000_000, 1),
        ]
    )
    advanced = _MariaRecorder([(8, 0, 0, 2_000_000, 2)])
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: _FacadeContext(snapshot, advanced)),
    )

    with pytest.raises(VNextCatalogReadError, match="head advanced"):
        VNextCatalogFacade(config).get_catalog_revision()

    assert snapshot.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "close",
    ]
    assert advanced.events == [
        "connect",
        "begin-read-only-snapshot",
        "rollback",
        "close",
    ]


@pytest.mark.parametrize(
    "method_name",
    (
        "get_catalog_revision",
        "discover_publications",
        "list_publication_facets",
        "list_recent_publications",
        "get_publication",
        "get_publication_presentation",
        "get_publication_page",
        "get_publications_by_artifact_names",
        "get_artifact",
    ),
)
def test_every_catalog_facade_read_uses_the_shared_fresh_head_fence(
    method_name: str,
) -> None:
    source = inspect.getsource(getattr(VNextCatalogFacade, method_name))

    assert "return self.__read(" in source


@pytest.mark.parametrize(
    "facade_type",
    [
        VNextCatalogFacade,
        VNextDatabaseAdminFacade,
        VNextDownloadQueueFacade,
    ],
)
def test_facade_public_surface_does_not_expose_database_infrastructure(
    facade_type: (
        type[VNextCatalogFacade]
        | type[VNextDatabaseAdminFacade]
        | type[VNextDownloadQueueFacade]
    ),
    tmp_path: Path,
) -> None:
    facade = facade_type(_config(tmp_path / "surface.sqlite3"))
    public_names = {name for name in dir(facade) if not name.startswith("_")}

    assert not {"connector", "context", "repository", "reader", "work"} & public_names
    for name in public_names:
        member = getattr(facade_type, name, None)
        if not callable(member):
            continue
        parameters = inspect.signature(member).parameters
        assert (
            not {
                "connector",
                "context",
                "provider",
                "repository",
                "work",
            }
            & parameters.keys()
        )

    assert "provider" not in inspect.signature(open_database).parameters

    with pytest.raises(TypeError, match="CoreConfig"):
        facade_type(object())  # type: ignore[arg-type]
