from __future__ import annotations

import inspect
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from h2hdb import (
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextDownloadQueueFacade,
    open_database,
)
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.config_loader import (
    CoreConfig,
    DatabaseAccessMode,
    DatabaseConfig,
)
from h2hdb.domain import CatalogPage, CatalogRevision
from h2hdb.ports import CatalogReader
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    SchemaCreateStatement,
    SchemaEpochDefinition,
    SchemaObject,
    SchemaObjectKind,
    SchemaSeedStatement,
    SchemaSemanticValidationPhase,
    SchemaSlice,
)
from h2hdb.sql_connector import DatabaseReadOnlyError, SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_identity import (
    CanonicalValueChunk,
    CanonicalValuePage,
    GalleryObservationNodeKind,
    artifact_id,
    artifact_locator_components,
    artifact_name,
    artifact_policy_digest,
    artifact_producer_fingerprint_sha256,
    artifact_semantics_digest,
    canonical_value_digest,
    canonical_value_page_digest,
    encode_artifact_locator,
    encode_artifact_policy,
    encode_artifact_semantics,
    encode_canonical_value_page,
    encode_source_relative_locator,
    gallery_key,
    publication_id,
    publication_key,
    source_scope_key,
)
from h2hdb.vnext_queue_repository import VNextDownloadRequest, VNextQueueRepository
from h2hdb.vnext_schema_provider import VNextSchemaProviderUnavailableError

if TYPE_CHECKING:
    _catalog_reader: CatalogReader = VNextCatalogFacade(CoreConfig())
    _admin_facade = VNextDatabaseAdminFacade(CoreConfig())
    _queue_request: VNextDownloadRequest = VNextDownloadQueueFacade(
        CoreConfig()
    ).request_download(1)


_ADMIN_PROBE = SchemaObject(SchemaObjectKind.TABLE, "vnext_facade_admin_probe")
_ADMIN_DDL = SchemaCreateStatement(
    "create:vnext_facade_admin_probe",
    """
    CREATE TABLE IF NOT EXISTS vnext_facade_admin_probe (
        probe_id INTEGER NOT NULL PRIMARY KEY CHECK (probe_id = 1),
        payload BLOB NOT NULL CHECK (typeof(payload) = 'blob')
    )
    """,
    _ADMIN_PROBE,
)
_ADMIN_SEED = SchemaSeedStatement(
    "seed:vnext_facade_admin_probe",
    "vnext_facade_admin_probe",
    """
    INSERT INTO vnext_facade_admin_probe (probe_id, payload)
    VALUES (%s, %s)
    ON CONFLICT(probe_id) DO NOTHING
    """,
    (1, b"facade"),
)


def _admin_definition() -> SchemaEpochDefinition:
    return SchemaEpochDefinition(
        epoch=2,
        schema_version=1,
        ddl_manifest_sha256="11" * 32,
        seed_manifest_sha256="22" * 32,
        obligation_manifest_sha256="33" * 32,
        expected_objects=frozenset({_ADMIN_PROBE}),
        slices=(SchemaSlice("facade-admin", (_ADMIN_DDL,)),),
        bootstrap_seeds=(_ADMIN_SEED,),
        activation_semantic_obligation_ids=("facade-admin-integrity",),
        ready_semantic_obligation_ids=("facade-admin-integrity",),
    )


@dataclass
class _AdminProvider:
    definition: SchemaEpochDefinition

    def validate_slice(
        self,
        connector: SQLConnector,
        schema_slice: SchemaSlice,
    ) -> None:
        assert schema_slice.slice_id == "facade-admin"
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = %s",
            (_ADMIN_PROBE.name,),
        ) == (1,)

    def validate_global(self, connector: SQLConnector) -> None:
        assert connector.fetch_one(
            "SELECT probe_id, payload FROM vnext_facade_admin_probe"
        ) == (1, b"facade")

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        self.validate_global(connector)
        return (_ADMIN_SEED.seed_id,)

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        del phase
        self.validate_global(connector)
        return ("facade-admin-integrity",)


@dataclass
class _ReadinessOnlyProvider:
    definition: SchemaEpochDefinition

    def validate_slice(
        self, connector: SQLConnector, schema_slice: SchemaSlice
    ) -> None:
        del connector, schema_slice
        raise AssertionError("readiness must not validate a schema slice")

    def validate_global(self, connector: SQLConnector) -> None:
        del connector
        raise AssertionError("readiness must not perform global validation")

    def validate_bootstrap_seeds(self, connector: SQLConnector) -> Sequence[str]:
        del connector
        raise AssertionError("readiness must not validate bootstrap seeds")

    def validate_semantics(
        self,
        connector: SQLConnector,
        phase: SchemaSemanticValidationPhase,
    ) -> Sequence[str]:
        del connector, phase
        raise AssertionError("readiness must not validate semantics")


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


def test_database_admin_facade_initializes_retries_and_fully_checks_fresh_epoch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin-facade.sqlite3"
    provider = _AdminProvider(_admin_definition())
    facade = VNextDatabaseAdminFacade(_config(path))

    initialized = facade.initialize(provider)
    retried = facade.initialize(provider)
    checked = facade.check(provider)

    assert initialized.state == "READY"
    assert initialized.transitioned_to_ready
    assert not initialized.resumed_build
    assert initialized.bootstrap_seed_ids == (_ADMIN_SEED.seed_id,)
    assert retried.state == "READY"
    assert retried.resumed_build
    assert not retried.transitioned_to_ready
    assert checked.state == "READY"
    assert not checked.transitioned_to_ready


def test_database_admin_facade_fully_checks_through_read_only_config(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin-read-only-check.sqlite3"
    provider = _AdminProvider(_admin_definition())
    VNextDatabaseAdminFacade(_config(path)).initialize(provider)

    checked = VNextDatabaseAdminFacade(_config(path, read_only=True)).check(provider)

    assert checked.state == "READY"
    assert checked.resumed_build
    assert not checked.transitioned_to_ready


def test_public_open_database_fully_checks_epoch_before_returning_catalog_facade(
    tmp_path: Path,
) -> None:
    path = tmp_path / "public-open.sqlite3"
    provider = _AdminProvider(_admin_definition())
    VNextDatabaseAdminFacade(_config(path)).initialize(provider)

    reader = open_database(_config(path, read_only=True), provider=provider)

    assert isinstance(reader, VNextCatalogFacade)
    assert isinstance(reader, CatalogReader)

    with SQLiteConnector(str(path)) as connector:
        connector.execute(
            "UPDATE vnext_facade_admin_probe SET payload = %s WHERE probe_id = 1",
            (b"drifted",),
        )

    with pytest.raises(AssertionError):
        open_database(_config(path, read_only=True), provider=provider)


def test_database_admin_facade_readiness_is_read_only_and_constant_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-readiness.sqlite3"
    definition = _admin_definition()
    VNextDatabaseAdminFacade(_config(path)).initialize(_AdminProvider(definition))
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

    readiness = VNextDatabaseAdminFacade(config).check_readiness(
        _ReadinessOnlyProvider(definition)
    )

    assert readiness.state == "READY"
    assert readiness.manifest_sha256 == definition.manifest_sha256
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
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, 1)",
        (value_sha256, domain.encode("ascii"), len(payload)),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value_sha256, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_descriptors "
        "(page_sha256, value_sha256, level, page_position, subtree_item_count) "
        "VALUES (%s, %s, 0, 0, %s)",
        (page_sha256, value_sha256, len(payload)),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )
    return value_sha256


def _catalog_fixture(connector: SQLiteConnector) -> dict[str, object]:
    source_root = _canonical(
        connector,
        "source_root_v1",
        b"\x00\x00\x00\x01\x00\x00\x00\x00",
    )
    scope_key = source_scope_key("filesystem", source_root, 1)
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, 1)",
        (scope_key, b"filesystem", source_root),
    )
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
    connector.execute(
        "INSERT INTO catalog_gallery_identities "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (1, %s, %s, %s)",
        (gallery_key_value, scope_key, locator),
    )

    source_title = _canonical(connector, "source_title_utf8_v1", b"Source")
    display_title = _canonical(connector, "display_title_utf8_v1", b"Display")
    sort_title = _canonical(connector, "title_sort_utf8_v1", b"display")
    summary = _canonical(connector, "catalog_summary_utf8_v1", b"Summary")
    language = _canonical(connector, "catalog_language_utf8_v1", b"en")
    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, "
        "unicode_data_version) VALUES (1, 1, %s)",
        (b"15.0.0",),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (1, 1, 1)"
    )
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
        "INSERT INTO catalog_publication_identities "
        "(publication_key, publication_id, gid, artifact_name) "
        "VALUES (%s, %s, %s, %s)",
        (
            publication_key_value,
            publication_id(gid),
            gid,
            artifact_name(gid),
        ),
    )
    connector.execute(
        "INSERT INTO catalog_revisions (revision, publication_count, published_at) "
        "VALUES (1, 1, 1000000)"
    )
    connector.execute(
        "INSERT INTO catalog_publications "
        "(revision, gallery_id, publication_key, summary_sha256, language_sha256, "
        "published_at, modified_at, item_sha256) "
        "VALUES (1, 1, %s, %s, %s, 2000000, 3000000, %s)",
        (publication_key_value, summary, language, b"i" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_publication_order "
        "(revision, position, publication_key) VALUES (1, 0, %s)",
        (publication_key_value,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_titles "
        "(revision, publication_key, display_title_policy_id, "
        "source_title_sha256, source_gallery_name) VALUES (1, %s, 1, %s, %s)",
        (publication_key_value, source_title, b"gallery-one"),
    )
    connector.execute(
        "INSERT INTO catalog_publication_heads "
        "(channel, revision, generation, advanced_at) VALUES (%s, 1, 1, 4000000)",
        (b"default",),
    )

    producer_tuple = (b"writer", b"python", b"pillow", b"jpeg", b"zlib")
    producer = artifact_producer_fingerprint_sha256(*producer_tuple)
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprints "
        "(producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build) VALUES (%s, 1, %s, %s, %s, %s, %s, %s)",
        (producer, b"facade", *producer_tuple),
    )
    policy = _canonical(
        connector,
        "artifact_policy_v2",
        encode_artifact_policy(1, 1600, producer),
    )
    assert policy == artifact_policy_digest(1, 1600, producer)
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "max_image_short_side, producer_fingerprint_sha256) "
        "VALUES (%s, 1, 1600, %s)",
        (policy, producer),
    )
    component_specs = (
        ("artifact_source_manifest_v1", b"source"),
        ("artifact_member_plan_v1", b"members"),
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
    connector.execute(
        "INSERT INTO catalog_artifact_semantic_input "
        "(artifact_semantics_sha256, source_manifest_component_sha256, "
        "member_plan_component_sha256, effective_content_component_sha256, "
        "selected_component_sha256, owner_component_sha256, "
        "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (semantics, *components, policy),
    )
    artifact_bytes = b"facade-artifact"
    artifact_sha256 = sha256(artifact_bytes).digest()
    artifact_identifier = artifact_id(gid, artifact_sha256)
    artifact_locator = _canonical(
        connector,
        "artifact_locator_bytes_v1",
        encode_artifact_locator(artifact_locator_components(artifact_sha256)),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_blobs (artifact_sha256, size_bytes) "
        "VALUES (%s, %s)",
        (artifact_sha256, len(artifact_bytes)),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_identity "
        "(artifact_id, publication_key, artifact_sha256) VALUES (%s, %s, %s)",
        (artifact_identifier, publication_key_value, artifact_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_location "
        "(artifact_sha256, artifact_locator_sha256) VALUES (%s, %s)",
        (artifact_sha256, artifact_locator),
    )
    connector.execute(
        "INSERT INTO catalog_artifacts "
        "(revision, artifact_id, artifact_semantics_sha256, modified_at) "
        "VALUES (1, %s, %s, 3000000)",
        (artifact_identifier, semantics),
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
    page = facade.list_publications(revision=revision, limit=10)
    publication = facade.get_publication(
        cast(str, values["publication_id"]),
        revision=revision,
    )
    by_name = facade.get_publications_by_artifact_names(
        ["h2h-123.cbz", "missing.cbz", "h2h-123.cbz"],
        revision=revision,
    )
    artifact = facade.get_artifact(
        cast(str, values["artifact_id"]),
        revision=revision,
    )

    assert revision == CatalogRevision(revision.revision, revision.published_at, 1)
    assert isinstance(page, CatalogPage)
    assert page.publications == (publication,)
    assert by_name == {"h2h-123.cbz": publication}
    assert publication is not None
    assert publication.artifacts == (artifact,)
    assert artifact is not None
    assert artifact.sha256 == cast(bytes, values["artifact_sha256"]).hex()


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
    def __init__(self, recorder: _MariaRecorder) -> None:
        self.sql_type = "mariadb"
        self.SQLConnector: Callable[[], _MariaRecorder] = lambda: recorder


def test_facades_preserve_mariadb_read_snapshot_and_for_update_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(Path("unused"), backend="mariadb")
    catalog_recorder = _MariaRecorder([(7,), (7, 0, 1_000_000)])
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, value: _FacadeContext(catalog_recorder)),
    )
    revision = VNextCatalogFacade(config).get_catalog_revision()

    assert revision.revision == 7
    assert catalog_recorder.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "close",
    ]
    assert all(" FOR UPDATE" not in query for query, _data in catalog_recorder.selects)
    assert not any(
        "maintenance" in query.casefold() for query, _data in catalog_recorder.selects
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
        assert not {"connector", "context", "repository", "work"} & parameters.keys()

    with pytest.raises(TypeError, match="CoreConfig"):
        facade_type(object())  # type: ignore[arg-type]
