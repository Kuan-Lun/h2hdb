from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.catalog_errors import CatalogRevisionNotFoundError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_catalog_reader_repository import (
    VNextCatalogReaderRepository,
    VNextCatalogReadError,
)
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
    encode_effective_content,
    encode_source_relative_locator,
    gallery_key,
    publication_id,
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
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value, domain.encode("ascii"), len(payload), allocated_at),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_descriptors "
        "(page_sha256, value_sha256, level, page_position, subtree_item_count) "
        "VALUES (%s, %s, 0, 0, %s)",
        (page_sha256, value, len(payload)),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value, page_sha256),
    )
    return value


def _published_fixture(connector: SQLiteConnector) -> dict[str, bytes]:
    root = _canonical(connector, "source_root_v1", b"\x00\x00\x00\x01\x00\x00\x00\x00")
    scope = source_scope_key("filesystem", root, 1)
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, 1)",
        (scope, b"filesystem", root),
    )
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
    connector.execute(
        "INSERT INTO catalog_gallery_identities "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (1, %s, %s, %s)",
        (gallery, scope, locator),
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
    contributor_sort = _canonical(connector, "contributor_sort_as_utf8_v1", b"author")
    tag_value = _canonical(connector, "tag_value_utf8_v1", "測試".encode())

    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, unicode_data_version) "
        "VALUES (1, 1, %s)",
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
    key = publication_key(gid)
    connector.execute(
        "INSERT INTO catalog_publication_identities "
        "(publication_key, publication_id, gid, artifact_name) "
        "VALUES (%s, %s, %s, %s)",
        (key, publication_id(gid), gid, artifact_name(gid)),
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
        (key, summary, language, b"i" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_publication_order "
        "(revision, position, publication_key) VALUES (1, 0, %s)",
        (key,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_titles "
        "(revision, publication_key, display_title_policy_id, "
        "source_title_sha256, source_gallery_name) VALUES (1, %s, 1, %s, %s)",
        (key, source_title, b"gallery-one"),
    )
    connector.execute(
        "INSERT INTO catalog_publication_contents "
        "(revision, publication_key, content_sha256) VALUES (1, %s, %s)",
        (key, content),
    )
    connector.execute(
        "INSERT INTO catalog_contributors "
        "(revision, publication_key, position, contributor_name_sha256, role) "
        "VALUES (1, %s, 0, %s, %s)",
        (key, contributor, b"author"),
    )
    connector.execute(
        "INSERT INTO catalog_contributor_sort_as "
        "(revision, publication_key, position, sort_as_sha256) "
        "VALUES (1, %s, 0, %s)",
        (key, contributor_sort),
    )
    connector.execute(
        "INSERT INTO catalog_tag_terms "
        "(tag_id, namespace, tag_value_sha256) VALUES (1, %s, %s)",
        (b"artist", tag_value),
    )
    connector.execute(
        "INSERT INTO catalog_subjects "
        "(revision, publication_key, position, tag_id) VALUES (1, %s, 0, 1)",
        (key,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_heads "
        "(channel, revision, generation, advanced_at) VALUES (%s, 1, 1, 4000000)",
        (b"default",),
    )
    return {"publication_key": key, "summary": summary, "content": content}


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
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprints "
        "(producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build) VALUES (%s, 1, %s, %s, %s, %s, %s, %s)",
        (producer, b"reader-fixture", *producer_tuple),
    )
    policy_payload = encode_artifact_policy(1, 1600, producer)
    policy = _canonical(connector, "artifact_policy_v2", policy_payload)
    assert policy == artifact_policy_digest(1, 1600, producer)
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "max_image_short_side, producer_fingerprint_sha256) "
        "VALUES (%s, 1, 1600, %s)",
        (policy, producer),
    )

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
    connector.execute(
        "INSERT INTO catalog_artifact_semantic_input "
        "(artifact_semantics_sha256, source_manifest_component_sha256, "
        "member_plan_component_sha256, effective_content_component_sha256, "
        "selected_component_sha256, owner_component_sha256, "
        "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (semantics, *components, policy),
    )

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
        "INSERT INTO catalog_artifact_blobs (artifact_sha256, size_bytes) "
        "VALUES (%s, %s)",
        (artifact_sha256, len(artifact_bytes)),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_identity "
        "(artifact_id, publication_key, artifact_sha256) VALUES (%s, %s, %s)",
        (identifier, publication_key_value, artifact_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_location "
        "(artifact_sha256, artifact_locator_sha256) VALUES (%s, %s)",
        (artifact_sha256, locator),
    )
    connector.execute(
        "INSERT INTO catalog_artifacts "
        "(revision, artifact_id, artifact_semantics_sha256, modified_at) "
        "VALUES (1, %s, %s, 3000000)",
        (identifier, semantics),
    )
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
                ["h2h-123.cbz", "missing.cbz", "h2h-123.cbz"],
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
        assert publication.contributors[0].sort_as == "author"
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
            "UPDATE catalog_canonical_value_allocations SET digest_domain = %s "
            "WHERE value_sha256 = %s",
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
        _published_fixture(connector)
        connector.execute(
            "INSERT INTO catalog_revisions "
            "(revision, publication_count, published_at) VALUES (2, 1, 5000000)"
        )
        for table, columns in (
            (
                "catalog_publications",
                "gallery_id, publication_key, summary_sha256, language_sha256, "
                "published_at, modified_at, item_sha256",
            ),
            (
                "catalog_publication_order",
                "position, publication_key",
            ),
            (
                "catalog_publication_titles",
                "publication_key, display_title_policy_id, source_title_sha256, "
                "source_gallery_name",
            ),
            (
                "catalog_publication_contents",
                "publication_key, content_sha256",
            ),
            (
                "catalog_contributors",
                "publication_key, position, contributor_name_sha256, role",
            ),
            (
                "catalog_contributor_sort_as",
                "publication_key, position, sort_as_sha256",
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
        connector.execute(
            "UPDATE catalog_publication_heads SET revision = 2, generation = 2, "
            "advanced_at = 5000000 WHERE channel = %s",
            (b"default",),
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
