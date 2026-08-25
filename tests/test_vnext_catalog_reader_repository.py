from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity, seed_tag_term
from vnext_catalog_registry_fixtures import (
    seed_artifact_policy_semantics,
    seed_artifact_producer_fingerprint,
    seed_display_title_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_manifest_fixtures import seed_snapshot_manifest
from vnext_publication_fixtures import (
    clone_catalog_publication_families,
    seed_catalog_contributor,
    seed_catalog_publication,
    seed_catalog_publication_title,
    seed_publication_commit,
    seed_publication_identity,
)

import h2hdb.vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.catalog_errors import CatalogRevisionNotFoundError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    ArtifactSemanticInputFamily,
    CatalogArtifactFamily,
    ensure_artifact_semantic_input_family,
    ensure_catalog_artifact_family,
)
from h2hdb.vnext_catalog_reader_repository import (
    VNextCatalogIdentifierError,
    VNextCatalogReaderRepository,
    VNextCatalogReadError,
    _selected_keys_cte,
)
from h2hdb.vnext_identity import (
    CanonicalValueChunk,
    CanonicalValuePage,
    GalleryObservationNodeKind,
    artifact_id,
    artifact_locator_components,
    artifact_policy_digest,
    artifact_producer_fingerprint_sha256,
    artifact_semantics_digest,
    canonical_value_digest,
    canonical_value_page_digest,
    encode_artifact_locator,
    encode_artifact_policy,
    encode_artifact_semantics,
    encode_canonical_value_page,
    encode_effective_content,
    encode_source_relative_locator,
    gallery_key,
    publication_key,
    source_relative_locator_digest,
    source_scope_key,
)


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


def test_selected_key_cte_uses_backend_binary_types() -> None:
    sqlite_cte = _selected_keys_cte(128, backend="sqlite")
    mariadb_cte = _selected_keys_cte(128, backend="mariadb")

    assert sqlite_cte.count("%s") == 128
    assert "CAST(" not in sqlite_cte
    assert mariadb_cte.count("CAST(%s AS BINARY(32))") == 128
    assert mariadb_cte.count(" UNION ALL ") == 127
    with pytest.raises(ValueError, match="must be positive"):
        _selected_keys_cte(0, backend="sqlite")
    with pytest.raises(ValueError, match="unsupported SQL backend"):
        _selected_keys_cte(1, backend="postgresql")


def _canonical(
    connector: SQLiteConnector,
    domain: str,
    payload: bytes,
    *,
    allocated_at: int = 1,
) -> bytes:
    value = canonical_value_digest(domain, payload)
    entries = () if not payload else (CanonicalValueChunk(0, payload),)
    page = CanonicalValuePage(
        value,
        GalleryObservationNodeKind.LEAF,
        0,
        0,
        len(payload),
        entries,
    )
    page_bytes = encode_canonical_value_page(page)
    page_sha256 = canonical_value_page_digest(page_bytes)
    seed_canonical_value(
        connector,
        value_sha256=value,
        digest_domain=domain.encode("ascii"),
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        subtree_item_count=len(payload),
        allocated_at=allocated_at,
    )
    return value


def _publication_commit(
    connector: SQLiteConnector,
    *,
    snapshot_manifest_sha256: bytes,
    revision: int = 1,
    source_revision: int = 1,
    generation: int = 1,
    publication_count: int = 1,
    committed_at: int = 1000000,
    receipt_id: bytes = b"r" * 16,
    candidate_id: bytes = b"c" * 16,
    preparation_id: bytes = b"p" * 16,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_channels "
        "(source_revision, channel) VALUES (%s, %s)",
        (source_revision, b"default"),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_snapshot_manifests "
        "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
        (source_revision, snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptor_seals "
        "(source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (%s, %s)",
        (revision, publication_count),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
        (generation,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (%s, %s)",
        (generation, generation - 1),
    )
    seed_publication_commit(
        connector,
        receipt_id=receipt_id,
        candidate_id=candidate_id,
        revision=revision,
        source_revision=source_revision,
        generation=generation,
        preparation_id=preparation_id,
        operational_policy_id=1,
        artifact_policy_id=1,
        display_title_policy_id=1,
        new_galleries=publication_count,
        changed_galleries=0,
        removed_galleries=0,
        duplicate_losers=0,
        committed_at=committed_at,
        channel=None,
    )
    connector.execute(
        "INSERT OR REPLACE INTO catalog_publication_commit_head_receipts "
        "(channel, receipt_id) VALUES (%s, %s)",
        (b"default", receipt_id),
    )
    connector.execute("PRAGMA foreign_keys = ON")


def _published_fixture(connector: SQLiteConnector) -> dict[str, bytes]:
    root = _canonical(connector, "source_root_v1", b"\x00\x00\x00\x01\x00\x00\x00\x00")
    scope = source_scope_key("filesystem", root, 1)
    assert seed_source_scope(connector, source_root_sha256=root).scope_key == scope
    locator_payload = encode_source_relative_locator(("gallery-one",))
    locator = _canonical(connector, "source_relative_locator_v1", locator_payload)
    assert locator == source_relative_locator_digest(
        "source_relative_locator_v1", ("gallery-one",)
    )
    connector.execute(
        "INSERT INTO catalog_source_locator_identity "
        "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
        (locator, b"gallery-one"),
    )
    gallery = gallery_key(scope, locator)
    seed_gallery_identity(
        connector,
        gallery_id=1,
        gallery_key=gallery,
        scope_key=scope,
        locator_sha256=locator,
    )
    snapshot = _canonical(
        connector,
        "source_snapshot_manifest_v1",
        b"reader-source-snapshot",
    )
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot,
        gallery_count=1,
        file_count=1,
        byte_count=1,
    )

    source_title = _canonical(connector, "source_title_utf8_v1", "原始標題".encode())
    display_title = _canonical(connector, "display_title_utf8_v1", "顯示標題".encode())
    sort_title = _canonical(
        connector, "title_sort_utf8_v1", "顯示標題".casefold().encode()
    )
    summary = _canonical(connector, "catalog_summary_utf8_v1", "摘要".encode())
    language = _canonical(connector, "catalog_language_utf8_v1", b"zh")
    content = _canonical(
        connector,
        "effective_content_v1",
        encode_effective_content((b"f" * 32,)),
    )
    contributor = _canonical(connector, "contributor_name_utf8_v1", "作者".encode())
    tag_value = _canonical(connector, "tag_value_utf8_v1", "測試".encode())

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
    key = publication_key(gid)
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
        "VALUES (%s, %s)",
        (gid, 2000000),
    )
    assert seed_publication_identity(connector, gid=gid).publication_key == key
    _publication_commit(
        connector,
        snapshot_manifest_sha256=snapshot,
    )
    seed_catalog_publication(
        connector,
        revision=1,
        publication_key=key,
        gallery_id=1,
        summary_sha256=summary,
        language_sha256=language,
        modified_at=3000000,
    )
    connector.execute(
        "INSERT INTO catalog_publication_order "
        "(revision, position, publication_key) VALUES (1, 0, %s)",
        (key,),
    )
    seed_catalog_publication_title(
        connector,
        revision=1,
        publication_key=key,
        source_title_sha256=source_title,
        source_gallery_name=b"gallery-one",
    )
    connector.execute(
        "INSERT INTO catalog_publication_contents "
        "(revision, publication_key, content_sha256) VALUES (1, %s, %s)",
        (key, content),
    )
    seed_catalog_contributor(
        connector,
        revision=1,
        publication_key=key,
        position=0,
        contributor_name_sha256=contributor,
        role=b"author",
    )
    seed_tag_term(
        connector,
        tag_id=1,
        namespace=b"artist",
        tag_value_sha256=tag_value,
    )
    connector.execute(
        "INSERT INTO catalog_subjects "
        "(revision, publication_key, position, tag_id) VALUES (1, %s, 0, 1)",
        (key,),
    )
    return {
        "publication_key": key,
        "summary": summary,
        "content": content,
        "snapshot_manifest": snapshot,
    }


def _artifact_fixture(
    connector: SQLiteConnector,
    *,
    publication_key_value: bytes,
    gid: int = 123,
) -> dict[str, bytes]:
    producer_tuple = (
        b"reader-writer",
        b"cpython-test",
        b"pillow-test",
        b"jpeg-test",
        b"zlib-test",
    )
    producer = artifact_producer_fingerprint_sha256(*producer_tuple)
    registered = seed_artifact_producer_fingerprint(
        connector,
        artifact_algorithm_version=1,
        writer_id=producer_tuple[0],
        python_abi=producer_tuple[1],
        pillow_build=producer_tuple[2],
        libjpeg_build=producer_tuple[3],
        zlib_build=producer_tuple[4],
    )
    assert registered.producer_fingerprint_sha256 == producer
    policy_payload = encode_artifact_policy(1, 1600, producer)
    policy = _canonical(connector, "artifact_policy_v2", policy_payload)
    assert policy == artifact_policy_digest(1, 1600, producer)
    registered_policy = seed_artifact_policy_semantics(
        connector,
        max_image_short_side=1600,
        producer_fingerprint_sha256=producer,
    )
    assert registered_policy.policy_component_sha256 == policy

    components = tuple(
        _canonical(connector, domain, payload)
        for domain, payload in (
            ("artifact_source_manifest_v1", b"source-manifest"),
            ("artifact_member_plan_v1", b"member-plan"),
            ("artifact_effective_content_v1", b"effective-content"),
            ("artifact_selected_v1", b"selected"),
            ("artifact_owner_v1", b"owner"),
        )
    )
    (
        source_manifest,
        member_plan,
        effective_content,
        selected,
        owner,
    ) = components
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
    _, inserted = ensure_artifact_semantic_input_family(
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
    assert inserted

    artifact_bytes = b"reader-artifact"
    artifact_sha256 = sha256(artifact_bytes).digest()
    locator_components = artifact_locator_components(artifact_sha256)
    locator = _canonical(
        connector,
        "artifact_locator_bytes_v1",
        encode_artifact_locator(locator_components),
    )
    identifier = artifact_id(gid, artifact_sha256)
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes, artifact_locator_sha256) "
        "VALUES (%s, %s, %s)",
        (artifact_sha256, len(artifact_bytes), locator),
    )
    _, inserted = ensure_catalog_artifact_family(
        connector,
        CatalogArtifactFamily(
            revision=1,
            publication_key=publication_key_value,
            artifact_sha256=artifact_sha256,
            artifact_semantics_sha256=semantics,
        ),
    )
    assert inserted
    return {
        "artifact_id": identifier,
        "artifact_sha256": artifact_sha256,
        "locator": locator,
    }


def test_pinned_reader_hydrates_normalized_publication_and_canonical_values(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            revision = reader.get_catalog_revision(connector)
            page = reader.list_publications(
                connector, revision=revision, offset=0, limit=10
            )
            artifact_page = reader.list_publications(
                connector,
                revision=revision,
                offset=0,
                limit=10,
                require_artifact=True,
            )
            by_id = reader.get_publication(
                connector, "urn:h2h:gallery:123", revision=revision
            )
            artifact = reader.get_artifact(
                connector,
                artifact_values["artifact_id"].decode("ascii"),
                revision=revision,
            )
            by_artifact_name = reader.get_publications_by_artifact_names(
                connector,
                ["h2h-123.cbz", "h2h-999.cbz", "h2h-123.cbz"],
                revision=revision,
            )
        assert revision.revision == 1
        assert revision.publication_count == 1
        assert page.total == 1
        assert page.publications == (by_id,)
        assert artifact_page.total == 1
        assert artifact_page.publications == (by_id,)
        assert by_artifact_name == {"h2h-123.cbz": by_id}
        publication = page.publications[0]
        assert publication.title == "顯示標題"
        assert publication.source_title == "原始標題"
        assert publication.summary == "摘要"
        assert publication.language == "zh"
        assert publication.source_gallery_name == "gallery-one"
        # Mutable redownload state is operational authority and is not copied
        # into an immutable, historically pinned catalog publication.
        assert not publication.redownload_required
        assert publication.content_sha256 == values["content"].hex()
        assert publication.contributors[0].name == "作者"
        assert publication.subjects[0].scheme == "h2h:tag:artist"
        assert publication.subjects[0].name == "測試"
        assert artifact is not None
        assert artifact.name == "h2h-123.cbz"
        assert artifact.location == Path(
            "sha256",
            artifact_values["artifact_sha256"].hex()[:2],
            f"{artifact_values['artifact_sha256'].hex()}.cbz",
        )
        assert artifact.sha256 == artifact_values["artifact_sha256"].hex()
    finally:
        connector.close()


def test_artifact_lookup_uses_only_the_strict_codec_and_exact_digest(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-artifact-codec.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        identifier = artifact_values["artifact_id"].decode("ascii")
        malformed = (
            identifier[:-64] + identifier[-64:].upper(),
            identifier.replace(":123:sha256:", ":0123:sha256:"),
            "urn:h2h:artifact:cbz:１２３:sha256:" + "0" * 64,
        )
        with connector.read_transaction():
            for value in malformed:
                with pytest.raises(
                    VNextCatalogReadError,
                    match="exact registered identity",
                ):
                    reader.get_artifact(connector, value, revision=1)
            missing = reader.get_artifact(
                connector,
                artifact_id(123, b"x" * 32).decode("ascii"),
                revision=1,
            )
        assert missing is None
    finally:
        connector.close()


def test_strict_lookup_rejects_publication_key_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-public-collision.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        original_publication_key = identity.publication_key

        def colliding_publication_key(
            gid: int,
            *,
            algorithm_version: int = identity.PUBLICATION_KEY_ALGORITHM_VERSION,
        ) -> bytes:
            if gid in {123, 999}:
                return values["publication_key"]
            return original_publication_key(
                gid,
                algorithm_version=algorithm_version,
            )

        monkeypatch.setattr(identity, "publication_key", colliding_publication_key)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        colliding_artifact_id = artifact_id(
            999,
            artifact_values["artifact_sha256"],
        ).decode("ascii")
        with connector.read_transaction():
            with pytest.raises(VNextCatalogReadError, match="collides"):
                reader.get_publication(
                    connector,
                    "urn:h2h:gallery:999",
                    revision=1,
                )
            with pytest.raises(VNextCatalogReadError, match="identity set"):
                reader.get_publications_by_artifact_names(
                    connector,
                    ["h2h-999.cbz"],
                    revision=1,
                )
            with pytest.raises(VNextCatalogReadError, match="collides"):
                reader.get_artifact(
                    connector,
                    colliding_artifact_id,
                    revision=1,
                )
    finally:
        connector.close()


def test_publication_and_artifact_name_lookups_reject_noncanonical_inputs_before_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-public-codecs.sqlite3")
    reader = VNextCatalogReaderRepository(backend="sqlite")
    queries: list[str] = []
    original_fetch_one = SQLiteConnector.fetch_one
    original_fetch_all = SQLiteConnector.fetch_all

    def counted_fetch_one(
        current: SQLiteConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        queries.append(query)
        return original_fetch_one(current, query, data)

    def counted_fetch_all(
        current: SQLiteConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        queries.append(query)
        return original_fetch_all(current, query, data)

    monkeypatch.setattr(SQLiteConnector, "fetch_one", counted_fetch_one)
    monkeypatch.setattr(SQLiteConnector, "fetch_all", counted_fetch_all)
    try:
        for value in (
            "urn:h2h:gallery:0",
            "urn:h2h:gallery:0123",
            "urn:h2h:gallery:１２３",
            "urn:h2h:gallery:123 ",
        ):
            with pytest.raises(VNextCatalogIdentifierError):
                reader.get_publication(connector, value)
        for value in (
            "missing.cbz",
            "h2h-0.cbz",
            "h2h-0123.cbz",
            "h2h-１２３.cbz",
            "h2h-123.CBZ",
        ):
            with pytest.raises(VNextCatalogIdentifierError):
                reader.get_publications_by_artifact_names(connector, [value])
        with pytest.raises(TypeError):
            reader.get_publications_by_artifact_names(connector, "h2h-123.cbz")
        with pytest.raises(ValueError, match="at most 128"):
            reader.get_publications_by_artifact_names(
                connector,
                ["h2h-1.cbz"] * 129,
            )
        assert queries == []
    finally:
        connector.close()


def test_artifact_name_lookup_uses_one_bounded_set_query_per_publication_family(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-name-page.sqlite3")
    try:
        values = _published_fixture(connector)
        _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        queries: list[str] = []
        original_fetch_all = SQLiteConnector.fetch_all

        def counted_fetch_all(
            current: SQLiteConnector,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            queries.append(query)
            return original_fetch_all(current, query, data)

        monkeypatch.setattr(SQLiteConnector, "fetch_all", counted_fetch_all)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        names = [f"h2h-{gid}.cbz" for gid in range(1, 129)]
        with connector.read_transaction():
            publications = reader.get_publications_by_artifact_names(
                connector,
                names,
                revision=1,
            )
        assert tuple(publications) == ("h2h-123.cbz",)
        assert (
            sum(
                "FROM catalog_publication_anchors AS anchor" in query
                for query in queries
            )
            == 1
        )
        assert (
            sum(
                "FROM catalog_contributor_anchors AS a JOIN selected" in query
                for query in queries
            )
            == 1
        )
        assert sum("FROM catalog_subjects AS s" in query for query in queries) == 1
        assert (
            sum("FROM catalog_artifacts AS artifact" in query for query in queries) == 1
        )
    finally:
        connector.close()


def test_single_publication_lookup_rejects_partial_scalar_and_title_families(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-partial-publication.sqlite3")
    try:
        values = _published_fixture(connector)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_publication_title_seals "
            "WHERE revision = 1 AND publication_key = %s",
            (values["publication_key"],),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="partial or noncongruent"),
        ):
            reader.get_publication(connector, "urn:h2h:gallery:123", revision=1)
    finally:
        connector.close()


def test_artifact_reader_rejects_a_safe_but_noncanonical_locator(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-artifact-locator.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        wrong_locator = _canonical(
            connector,
            "artifact_locator_bytes_v1",
            encode_artifact_locator(("safe", "wrong.cbz")),
        )
        connector.execute(
            "UPDATE catalog_artifact_blobs SET artifact_locator_sha256 = %s "
            "WHERE artifact_sha256 = %s",
            (wrong_locator, artifact_values["artifact_sha256"]),
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="content-addressed identity"),
        ):
            reader.get_artifact(
                connector,
                artifact_values["artifact_id"].decode("ascii"),
                revision=1,
            )
    finally:
        connector.close()


def test_reader_rejects_missing_order_row_and_wrong_canonical_domain(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-corrupt.sqlite3")
    try:
        values = _published_fixture(connector)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        connector.execute("DELETE FROM catalog_publication_order")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="publication_count"),
        ):
            reader.list_publications(connector)

        connector.execute(
            "INSERT INTO catalog_publication_order "
            "(revision, position, publication_key) VALUES (1, 0, %s)",
            (values["publication_key"],),
        )
        connector.execute(
            "UPDATE catalog_canonical_value_allocation_digest_domains "
            "SET digest_domain = %s WHERE value_sha256 = %s",
            (b"source_title_utf8_v1", values["summary"]),
        )
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="exact page-tree validation"),
        ):
            reader.list_publications(connector)
    finally:
        connector.close()


def test_reader_uses_public_revision_not_found_error(tmp_path: Path) -> None:
    connector = _database(tmp_path / "reader-missing-revision.sqlite3")
    try:
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(CatalogRevisionNotFoundError) as missing_current,
        ):
            reader.get_catalog_revision(connector)
        assert missing_current.value.revision == 0

        with (
            connector.read_transaction(),
            pytest.raises(CatalogRevisionNotFoundError) as missing_historical,
        ):
            reader.get_catalog_revision(connector, 99)
        assert missing_historical.value.revision == 99

        connector.execute("INSERT INTO catalog_revision_anchors (revision) VALUES (1)")
        connector.execute(
            "INSERT INTO catalog_revision_publication_counts "
            "(revision, publication_count) VALUES (1, 0)"
        )
        connector.execute(
            "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (1)"
        )
        with (
            connector.read_transaction(),
            pytest.raises(CatalogRevisionNotFoundError) as unpublished_descriptor,
        ):
            reader.get_catalog_revision(connector, 1)
        assert unpublished_descriptor.value.revision == 1
    finally:
        connector.close()


def test_reader_fails_closed_for_search_until_revision_index_exists(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-search.sqlite3")
    try:
        _published_fixture(connector)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(
                VNextCatalogReadError,
                match="normalized revision-pinned search index",
            ),
        ):
            reader.list_publications(connector, query="顯示")

        with connector.read_transaction():
            page = reader.list_publications(connector, query="  ")
        assert page.total == 1
    finally:
        connector.close()


def test_reader_pins_historical_revision_through_read_only_connection(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "reader-history.sqlite3"
    connector = _database(database_path)
    try:
        values = _published_fixture(connector)
        _publication_commit(
            connector,
            snapshot_manifest_sha256=values["snapshot_manifest"],
            revision=2,
            source_revision=2,
            generation=2,
            publication_count=1,
            committed_at=5000000,
            receipt_id=b"s" * 16,
            candidate_id=b"d" * 16,
            preparation_id=b"q" * 16,
        )
        clone_catalog_publication_families(
            connector,
            source_revision=1,
            target_revision=2,
        )
        for table, columns in (
            (
                "catalog_publication_order",
                "position, publication_key",
            ),
            (
                "catalog_publication_contents",
                "publication_key, content_sha256",
            ),
            (
                "catalog_subjects",
                "publication_key, position, tag_id",
            ),
        ):
            connector.execute(
                f"INSERT INTO {table} (revision, {columns}) "
                f"SELECT 2, {columns} FROM {table} WHERE revision = 1"
            )
    finally:
        connector.close()

    read_only = SQLiteConnector(str(database_path), read_only=True)
    read_only.connect()
    try:
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with read_only.read_transaction():
            current = reader.get_catalog_revision(read_only)
            historical = reader.get_catalog_revision(read_only, 1)
            current_page = reader.list_publications(read_only, revision=current)
            historical_page = reader.list_publications(
                read_only,
                revision=historical,
            )
        assert current.revision == 2
        assert historical.revision == 1
        assert current_page.revision == current
        assert historical_page.revision == historical
        assert current_page.publications == historical_page.publications
    finally:
        read_only.close()
