from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any
from unicodedata import unidata_version
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import seed_analysis_component, seed_analysis_run
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity, seed_tag_term
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import seed_sealed_source_build, seed_snapshot_manifest
from vnext_publication_fixtures import (
    clone_catalog_publication_families,
    seed_catalog_contributor,
    seed_catalog_publication,
    seed_catalog_publication_title,
    seed_publication_commit,
    seed_publication_finalization,
    seed_publication_identity,
)

import h2hdb.vnext_catalog_reader_repository as catalog_reader_repository
import h2hdb.vnext_identity as identity
from h2hdb import (
    CatalogContributorFilter,
    CatalogDiscoveryCursor,
    CatalogDiscoveryQuery,
    CatalogFacetCursor,
    CatalogFacetKind,
    CatalogRecentOrder,
    CatalogSubjectFilter,
    CoreConfig,
    DatabaseConfig,
    StorageObjectKey,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)
from h2hdb.catalog_errors import CatalogCursorError, CatalogRevisionNotFoundError
from h2hdb.catalog_search import iter_search_lexemes
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
    _discovery_filter_sql,
    _discovery_page_sql,
    _discovery_query_sha256,
    _facet_identity_sha256,
    _selected_keys_cte,
)
from h2hdb.vnext_identity import (
    CanonicalValueChunk,
    CanonicalValuePage,
    GalleryObservationNodeKind,
    artifact_id,
    artifact_policy_digest,
    artifact_semantics_digest,
    canonical_value_digest,
    canonical_value_page_digest,
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

_EMPTY_EVENT_CHAIN = sha256(b"h2hdb-operational-event-chain-v1\0").digest()
_READER_ADAPTER_ID = b"reader-test-adapter"
_READER_POLICY_FINGERPRINT = sha256(b"reader-test-artifact-policy").digest()
_CATALOG_PUBLICATION_PAYLOAD_TABLES = (
    "catalog_contributors",
    "catalog_publication_order",
    "catalog_publication_titles",
    "catalog_publication_contents",
    "catalog_subjects",
    "catalog_artifacts",
    "catalog_publications",
)


def _canonical_root_page(
    connector: SQLiteConnector,
    value_sha256: bytes,
) -> bytes:
    row = connector.fetch_one(
        "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (value_sha256,),
    )
    assert len(row) == 1
    root_page_sha256 = row[0]
    assert isinstance(root_page_sha256, bytes)
    return root_page_sha256


def _assert_canonical_value_storage(
    connector: SQLiteConnector,
    *,
    value_sha256: bytes,
    root_page_sha256: bytes,
    present: bool,
) -> None:
    """Assert the complete one-page canonical fixture is retained or reclaimed."""

    expected = (1,) if present else (0,)
    for table in (
        "catalog_canonical_value_allocation_anchors",
        "catalog_canonical_value_allocation_digest_domains",
        "catalog_canonical_value_allocation_byte_counts",
        "catalog_canonical_value_allocation_allocated_ats",
        "catalog_canonical_value_allocation_seals",
        "catalog_canonical_value_identities",
    ):
        assert (
            connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE value_sha256 = %s",
                (value_sha256,),
            )
            == expected
        )
    assert (
        connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_canonical_value_page_coordinates "
            "WHERE value_sha256 = %s AND page_sha256 = %s",
            (value_sha256, root_page_sha256),
        )
        == expected
    )
    for table in (
        "catalog_canonical_value_page_anchors",
        "catalog_canonical_value_page_payloads",
        "catalog_canonical_value_page_subtree_item_counts",
        "catalog_canonical_value_page_seals",
    ):
        assert (
            connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE page_sha256 = %s",
                (root_page_sha256,),
            )
            == expected
        )


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


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
    artifact_count: int | None = None,
    committed_at: int = 1000000,
    receipt_id: bytes = b"r" * 16,
    candidate_id: bytes = b"c" * 16,
    preparation_id: bytes = b"p" * 16,
) -> None:
    analysis_id = _seed_commit_authorities(
        connector,
        preparation_id=preparation_id,
        snapshot_manifest_sha256=snapshot_manifest_sha256,
        generation=generation,
        gallery_count=publication_count,
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptors "
        "(source_revision, channel, snapshot_manifest_sha256) "
        "VALUES (%s, %s, %s)",
        (source_revision, b"default", snapshot_manifest_sha256),
    )
    sealed_artifact_count = (
        publication_count if artifact_count is None else artifact_count
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptors "
        "(revision, publication_count, artifact_count) VALUES (%s, %s, %s)",
        (revision, publication_count, sealed_artifact_count),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance "
        "(source_revision, analysis_id) VALUES (%s, %s)",
        (source_revision, analysis_id),
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


def _seed_commit_authorities(
    connector: SQLiteConnector,
    *,
    preparation_id: bytes,
    snapshot_manifest_sha256: bytes,
    generation: int,
    gallery_count: int,
) -> bytes:
    """Seed complete FK-on parents for the immutable common commit fixture."""

    seed_manifest_policy(connector)
    seed_analysis_policy(connector)
    seed_display_title_policy(connector)
    policy_payload = encode_artifact_policy(
        2,
        _READER_ADAPTER_ID,
        _READER_POLICY_FINGERPRINT,
    )
    policy_component = artifact_policy_digest(
        2,
        _READER_ADAPTER_ID,
        _READER_POLICY_FINGERPRINT,
    )
    if not connector.fetch_one(
        "SELECT 1 FROM catalog_canonical_value_identities WHERE value_sha256 = %s",
        (policy_component,),
    ):
        assert (
            _canonical(
                connector,
                "artifact_policy_v3",
                policy_payload,
            )
            == policy_component
        )
    semantics = seed_artifact_policy_semantics(
        connector,
        artifact_algorithm_version=2,
        adapter_id=_READER_ADAPTER_ID,
        policy_fingerprint_sha256=_READER_POLICY_FINGERPRINT,
    )
    assert semantics.policy_component_sha256 == policy_component
    connector.execute(
        "INSERT OR IGNORE INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (1, %s)",
        (semantics.policy_component_sha256,),
    )
    connector.execute(
        "INSERT OR IGNORE INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, "
        "algorithm_version, max_batch_rows) VALUES (1, 1, 1, 128)"
    )
    if connector.fetch_one(
        "SELECT 1 FROM operational_operational_preparation_effect_seals "
        "WHERE preparation_id = %s",
        (preparation_id,),
    ):
        row = connector.fetch_one(
            "SELECT run.analysis_id "
            "FROM operational_operational_preparations AS preparation "
            "JOIN catalog_analysis_run_descriptor AS run "
            "ON run.build_id = preparation.build_id "
            "WHERE preparation.preparation_id = %s",
            (preparation_id,),
        )
        assert len(row) == 1
        analysis_id = row[0]
        assert isinstance(analysis_id, bytes)
        return analysis_id
    scope = connector.fetch_one(
        "SELECT scope_key FROM catalog_source_scopes ORDER BY scope_key LIMIT 1"
    )
    assert len(scope) == 1
    build_id = sha256(b"reader-commit-build\0" + preparation_id).digest()[:16]
    build_manifest_sha256 = sha256(b"reader-commit-manifest\0" + build_id).digest()
    seed_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope[0],
        manifest_sha256=build_manifest_sha256,
        gallery_count=gallery_count,
        file_count=0,
        byte_count=0,
        created_at=1,
        sealed_at=2,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (build_id, b"default"),
    )
    if generation > 1:
        base = connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits WHERE generation = %s",
            (generation - 1,),
        )
        assert len(base) == 1
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_id, base[0]),
        )
    analysis_id = sha256(b"reader-commit-analysis\0" + preparation_id).digest()[:16]
    input_payload = bytearray(b"h2hdb-vnext-analysis-input\0")
    input_payload.extend(build_manifest_sha256)
    for count in (gallery_count, 0, 0):
        input_payload.extend(count.to_bytes(8, "big"))
    input_payload.extend((1).to_bytes(4, "big"))
    input_payload.extend((1).to_bytes(8, "big"))
    input_payload.extend((3).to_bytes(8, "big"))
    input_payload.extend((1).to_bytes(4, "big"))
    input_payload.extend((1).to_bytes(4, "big"))
    seed_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        policy_id=1,
        input_manifest_sha256=sha256(input_payload).digest(),
        started_at=2,
        state="COMPLETE",
        completed_at=3,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) "
        "VALUES (%s, 0, %s)",
        (analysis_id, analysis_id),
    )
    for component in (
        b"content_owner",
        b"content_owner_candidate",
        b"file_hash_decision",
        b"gid_candidate",
        b"gid_winner",
    ):
        seed_analysis_component(
            connector,
            analysis_id=analysis_id,
            state_component=component,
            row_count=0,
            sealed_at=3,
            terminal_receipt=True,
        )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest "
        "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
        (analysis_id, snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO operational_operational_event_streams "
        "(preparation_id, created_at) VALUES (%s, 2)",
        (preparation_id,),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparations "
        "(preparation_id, build_id, deletion_request_generation, "
        "operational_policy_id, state, prepared_at, completed_at) "
        "VALUES (%s, %s, 0, 1, 'COMPLETE', 2, 3)",
        (preparation_id, build_id),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparation_effect_seals "
        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
        "VALUES (%s, 0, %s, 3)",
        (preparation_id, _EMPTY_EVENT_CHAIN),
    )
    return analysis_id


def _published_fixture(
    connector: SQLiteConnector,
    *,
    artifact_count: int | None = None,
) -> dict[str, bytes]:
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

    seed_title_sort_policy(
        connector,
        unicode_data_version=unidata_version.encode("ascii"),
    )
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
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (gid, 2000000),
    )
    assert seed_publication_identity(connector, gid=gid).publication_key == key
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
    _publication_commit(
        connector,
        snapshot_manifest_sha256=snapshot,
        artifact_count=artifact_count,
    )
    seed_catalog_publication(
        connector,
        revision=1,
        publication_key=key,
        gallery_id=1,
        summary_sha256=summary,
        language_sha256=language,
        modified_at=3000000,
        source_title_sha256=source_title,
        download_time=2500000,
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
        "source_root": root,
        "locator": locator,
        "source_title": source_title,
        "display_title": display_title,
        "sort_title": sort_title,
        "summary": summary,
        "language": language,
        "content": content,
        "contributor": contributor,
        "tag_value": tag_value,
        "snapshot_manifest": snapshot,
    }


def _add_recent_artifact_publication(
    connector: SQLiteConnector,
    *,
    values: dict[str, bytes],
    semantics_sha256: bytes,
    gid: int,
    position: int,
    upload_time: int,
    download_time: int,
) -> bytes:
    """Add one exact reader publication while reusing bounded canonical payloads."""

    key = publication_key(gid)
    gallery_name = f"gallery-{gid}".encode("ascii")
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (gid, upload_time),
    )
    seed_publication_identity(connector, gid=gid)
    connector.execute(
        "INSERT INTO catalog_source_gallery_name_gids "
        "(source_gallery_name, gid) VALUES (%s, %s)",
        (gallery_name, gid),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_source_name_accesses "
        "(gallery_id, source_gallery_name) VALUES (%s, %s)",
        (gid, gallery_name),
    )
    seed_catalog_publication(
        connector,
        revision=1,
        publication_key=key,
        gallery_id=gid,
        summary_sha256=values["summary"],
        language_sha256=values["language"],
        modified_at=3000000 + gid,
        source_title_sha256=values["source_title"],
        download_time=download_time,
    )
    seed_catalog_publication_title(
        connector,
        revision=1,
        publication_key=key,
        source_title_sha256=values["source_title"],
        source_gallery_name=gallery_name,
    )
    connector.execute(
        "INSERT INTO catalog_display_title_choices "
        "(display_title_policy_id, source_title_sha256, source_gallery_name, "
        "title_sha256) VALUES (1, %s, %s, %s)",
        (values["source_title"], gallery_name, values["display_title"]),
    )
    connector.execute(
        "INSERT INTO catalog_publication_order "
        "(revision, position, publication_key) VALUES (1, %s, %s)",
        (position, key),
    )
    artifact_sha256 = sha256(
        b"recent-reader-artifact\0" + gid.to_bytes(8, "big")
    ).digest()
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes) VALUES (%s, %s)",
        (artifact_sha256, gid),
    )
    artifact_name = f"download-{gid}.bin".encode("ascii")
    ensure_catalog_artifact_family(
        connector,
        CatalogArtifactFamily(
            revision=1,
            publication_key=key,
            artifact_sha256=artifact_sha256,
            artifact_semantics_sha256=semantics_sha256,
            artifact_name=artifact_name,
            media_type=b"application/octet-stream",
            page_count=0,
        ),
    )
    _seed_catalog_acquisition(
        connector,
        publication_key_value=key,
        gid=gid,
        artifact_sha256=artifact_sha256,
        size_bytes=gid,
    )
    return key


def _seed_catalog_acquisition(
    connector: SQLiteConnector,
    *,
    publication_key_value: bytes,
    gid: int,
    artifact_sha256: bytes,
    size_bytes: int,
) -> StorageObjectKey:
    storage_key = StorageObjectKey("reader-v2", ("acquisition", str(gid)))
    key_sha256 = identity.artifact_storage_key_digest(
        storage_key.codec,
        storage_key.segments,
    )
    connector.execute(
        "INSERT INTO catalog_storage_object_key_identities "
        "(storage_object_key_sha256, key_codec, segment_count) VALUES (%s, %s, %s)",
        (key_sha256, storage_key.codec.encode("ascii"), len(storage_key.segments)),
    )
    for position, segment in enumerate(storage_key.segments):
        connector.execute(
            "INSERT INTO catalog_storage_object_key_segments "
            "(storage_object_key_sha256, segment_position, key_segment) "
            "VALUES (%s, %s, %s)",
            (key_sha256, position, segment.encode("utf-8")),
        )
    connector.execute(
        "INSERT INTO catalog_storage_objects "
        "(revision, publication_key, resource_kind, storage_object_key_sha256, "
        "storage_object_sha256, size_bytes, modified_at) "
        "VALUES (1, %s, %s, %s, %s, %s, %s)",
        (
            publication_key_value,
            b"acquisition",
            key_sha256,
            artifact_sha256,
            size_bytes,
            3_000_000 + gid,
        ),
    )
    return storage_key


def _artifact_fixture(
    connector: SQLiteConnector,
    *,
    publication_key_value: bytes,
    gid: int = 123,
) -> dict[str, Any]:
    policy = artifact_policy_digest(
        2,
        _READER_ADAPTER_ID,
        _READER_POLICY_FINGERPRINT,
    )
    assert connector.fetch_one(
        "SELECT value_sha256 FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (policy,),
    ) == (policy,)
    assert policy == artifact_policy_digest(
        2,
        _READER_ADAPTER_ID,
        _READER_POLICY_FINGERPRINT,
    )
    registered_policy = seed_artifact_policy_semantics(
        connector,
        artifact_algorithm_version=2,
        adapter_id=_READER_ADAPTER_ID,
        policy_fingerprint_sha256=_READER_POLICY_FINGERPRINT,
    )
    assert registered_policy.policy_component_sha256 == policy

    components = tuple(
        _canonical(connector, domain, payload)
        for domain, payload in (
            ("artifact_source_manifest_v1", b"source-manifest"),
            ("artifact_effective_content_v1", b"member-plan"),
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
    identifier = artifact_id(gid, artifact_sha256)
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes) VALUES (%s, %s)",
        (artifact_sha256, len(artifact_bytes)),
    )
    _, inserted = ensure_catalog_artifact_family(
        connector,
        CatalogArtifactFamily(
            revision=1,
            publication_key=publication_key_value,
            artifact_sha256=artifact_sha256,
            artifact_semantics_sha256=semantics,
            artifact_name=f"download-{gid}.bin".encode("ascii"),
            media_type=b"application/octet-stream",
            page_count=0,
        ),
    )
    assert inserted
    storage_key = _seed_catalog_acquisition(
        connector,
        publication_key_value=publication_key_value,
        gid=gid,
        artifact_sha256=artifact_sha256,
        size_bytes=len(artifact_bytes),
    )
    return {
        "artifact_id": identifier,
        "artifact_sha256": artifact_sha256,
        "storage_key": storage_key,
        "semantics": semantics,
        "source_manifest": source_manifest,
        "member_plan": member_plan,
        "effective_content": effective_content,
        "selected": selected,
        "owner": owner,
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
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            revision = reader.get_catalog_revision(connector)
            page = reader.discover_publications(
                connector,
                revision=revision,
                limit=10,
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
                ["download-123.bin", "missing.bin", "download-123.bin"],
                revision=revision,
            )
        assert revision.revision == 1
        assert revision.publication_count == 1
        assert page.total == 1
        assert page.publications == (by_id,)
        assert by_artifact_name == {"download-123.bin": by_id}
        publication = page.publications[0]
        assert publication.title == "顯示標題"
        assert publication.source_title == "原始標題"
        assert publication.summary == "摘要"
        assert publication.language == "zh"
        assert publication.downloaded_at.timestamp() == 2.5
        assert publication.source_gallery_name == "gallery-one"
        # Mutable redownload state is operational authority and is not copied
        # into an immutable, historically pinned catalog publication.
        assert not publication.redownload_required
        assert publication.content_sha256 == values["content"].hex()
        assert publication.contributors[0].name == "作者"
        assert publication.subjects[0].scheme == "h2h:tag:artist"
        assert publication.subjects[0].name == "測試"
        assert artifact is not None
        assert artifact.name == "download-123.bin"
        assert artifact.storage_object.key == artifact_values["storage_key"]
        assert (
            artifact.storage_object.sha256 == artifact_values["artifact_sha256"].hex()
        )
    finally:
        connector.close()


def _seed_discovery_authority(
    connector: SQLiteConnector,
    *,
    values: dict[str, bytes],
) -> None:
    general_subject = _canonical(
        connector,
        "tag_value_utf8_v1",
        "科幻".encode(),
    )
    seed_tag_term(
        connector,
        tag_id=2,
        namespace=b"genre",
        tag_value_sha256=general_subject,
    )
    connector.execute(
        "INSERT INTO catalog_subjects "
        "(revision, publication_key, position, tag_id) VALUES (1, %s, 1, 2)",
        (values["publication_key"],),
    )
    fields = (
        "顯示標題".encode(),
        "原始標題".encode(),
        "作者".encode(),
        "測試".encode(),
        "科幻".encode(),
    )
    lexemes = tuple(dict.fromkeys(iter_search_lexemes(fields)))
    connector.execute(
        "INSERT INTO catalog_search_documents "
        "(revision, publication_key, row_count) VALUES (1, %s, %s)",
        (values["publication_key"], len(lexemes)),
    )
    for lexeme in lexemes:
        value_sha256 = _canonical(
            connector,
            "search_lexeme_utf8_v1",
            lexeme,
        )
        connector.execute(
            "INSERT INTO catalog_search_lexemes (value_sha256) VALUES (%s)",
            (value_sha256,),
        )
        connector.execute(
            "INSERT INTO catalog_search_postings "
            "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
            (value_sha256, values["publication_key"]),
        )
    connector.execute(
        "INSERT INTO catalog_language_facet_order "
        "(revision, position, language_sha256, occurrence_count) "
        "VALUES (1, 0, %s, 1)",
        (values["language"],),
    )
    connector.execute(
        "INSERT INTO catalog_subject_facet_order "
        "(revision, position, tag_id, occurrence_count) VALUES (1, 0, 2, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_contributor_facet_order "
        "(revision, position, contributor_name_sha256, role, occurrence_count) "
        "VALUES (1, 0, %s, %s, 1)",
        (values["contributor"], b"author"),
    )
    connector.execute(
        "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (1, 1)"
    )


def _seed_publication_child_cardinality(
    connector: SQLiteConnector,
    *,
    publication_key_value: bytes,
    total: int,
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    contributor_names = ["作者"]
    subjects = [("測試", "artist")]
    for position in range(1, total):
        contributor_name = f"作者-{position:03d}"
        contributor = _canonical(
            connector,
            "contributor_name_utf8_v1",
            contributor_name.encode(),
        )
        seed_catalog_contributor(
            connector,
            revision=1,
            publication_key=publication_key_value,
            position=position,
            contributor_name_sha256=contributor,
            role=b"author",
        )
        namespace = f"topic-{position:03d}"
        subject_name = f"主題-{position:03d}"
        tag_value = _canonical(
            connector,
            "tag_value_utf8_v1",
            subject_name.encode(),
        )
        seed_tag_term(
            connector,
            tag_id=position + 1,
            namespace=namespace.encode("ascii"),
            tag_value_sha256=tag_value,
        )
        connector.execute(
            "INSERT INTO catalog_subjects "
            "(revision, publication_key, position, tag_id) "
            "VALUES (1, %s, %s, %s)",
            (publication_key_value, position, position + 1),
        )
        contributor_names.append(contributor_name)
        subjects.append((subject_name, namespace))
    return tuple(contributor_names), tuple(subjects)


@pytest.mark.deep
def test_publication_children_stream_in_hard_capped_keyset_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-child-keyset-pages.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        total = catalog_reader_repository._CHILD_HYDRATION_PAGE_LIMIT + 1
        expected_contributors, expected_subjects = _seed_publication_child_cardinality(
            connector,
            publication_key_value=values["publication_key"],
            total=total,
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            monkeypatch.context() as reference_patch,
            connector.read_transaction(),
        ):
            reference_patch.setattr(
                catalog_reader_repository,
                "_CHILD_HYDRATION_PAGE_LIMIT",
                total + 1,
            )
            reference = reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )

        page_rows: dict[str, list[int]] = {"contributors": [], "subjects": []}
        prefetch_sizes: list[int] = []
        original_fetch_all = connector.fetch_all
        original_prefetch = catalog_reader_repository._CanonicalLoader.prefetch

        def record_fetch_all(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            rows = original_fetch_all(query, data)
            if "FROM catalog_contributors AS contributor JOIN selected" in query:
                page_rows["contributors"].append(len(rows))
            elif "FROM catalog_subjects AS s JOIN selected" in query:
                page_rows["subjects"].append(len(rows))
            return rows

        def record_prefetch(
            loader: catalog_reader_repository._CanonicalLoader,
            references: tuple[tuple[object, bytes], ...],
        ) -> None:
            prefetch_sizes.append(len(references))
            original_prefetch(loader, references)

        monkeypatch.setattr(connector, "fetch_all", record_fetch_all)
        monkeypatch.setattr(
            catalog_reader_repository._CanonicalLoader,
            "prefetch",
            record_prefetch,
        )
        with connector.read_transaction():
            paged = reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )

        assert paged == reference
        assert paged is not None
        assert (
            tuple(value.name for value in paged.contributors) == expected_contributors
        )
        assert tuple(value.role for value in paged.contributors) == ("author",) * total
        assert (
            tuple((value.name, value.code) for value in paged.subjects)
            == expected_subjects
        )
        assert page_rows == {
            "contributors": [catalog_reader_repository._CHILD_HYDRATION_PAGE_LIMIT, 1],
            "subjects": [catalog_reader_repository._CHILD_HYDRATION_PAGE_LIMIT, 1],
        }
        assert prefetch_sizes
        assert (
            max(prefetch_sizes) <= catalog_reader_repository._CHILD_HYDRATION_PAGE_LIMIT
        )
    finally:
        connector.close()


@pytest.mark.deep
def test_publication_child_keyset_pages_reject_cross_page_position_gaps(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-child-keyset-gap.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        boundary = catalog_reader_repository._CHILD_HYDRATION_PAGE_LIMIT
        _seed_publication_child_cardinality(
            connector,
            publication_key_value=values["publication_key"],
            total=boundary + 1,
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")

        connector.execute(
            "UPDATE catalog_contributors SET position = %s "
            "WHERE revision = 1 AND publication_key = %s AND position = %s",
            (boundary + 1, values["publication_key"], boundary),
        )
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="contributor positions"),
        ):
            reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )
        connector.execute(
            "UPDATE catalog_contributors SET position = %s "
            "WHERE revision = 1 AND publication_key = %s AND position = %s",
            (boundary, values["publication_key"], boundary + 1),
        )
        connector.execute(
            "UPDATE catalog_subjects SET position = %s "
            "WHERE revision = 1 AND publication_key = %s AND position = %s",
            (boundary + 1, values["publication_key"], boundary),
        )
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="subject positions"),
        ):
            reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )
    finally:
        connector.close()


def test_discovery_uses_sealed_sql_postings_and_exact_cross_filters(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "discovery-reader.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        marker = object()
        query = CatalogDiscoveryQuery(
            search="顯示",
            language="zh",
            subject=CatalogSubjectFilter(namespace="genre", value="科幻"),
            contributor=CatalogContributorFilter(name="作者", role="author"),
        )
        with (
            connector.read_transaction(),
            patch.object(
                reader,
                "_hydrate_publications",
                return_value={values["publication_key"]: marker},
            ) as hydrate,
        ):
            page = reader.discover_publications(
                connector,
                query=query,
                limit=1,
            )
        assert page.publications == (marker,)
        assert page.next_cursor is None
        hydrate.assert_called_once()

        with connector.read_transaction():
            no_match = reader.discover_publications(
                connector,
                query=CatalogDiscoveryQuery(search="不存在"),
                limit=1,
            )
        assert no_match.publications == ()
        forged_query = CatalogDiscoveryQuery(search="不存在")
        forged = CatalogDiscoveryCursor(
            revision=1,
            query_sha256=_discovery_query_sha256(forged_query),
            position=0,
            publication_id=identity.publication_id(123).decode("ascii"),
        )
        with (
            connector.read_transaction(),
            pytest.raises(
                CatalogCursorError,
                match="not a member",
            ),
        ):
            reader.discover_publications(
                connector,
                query=forged_query,
                after=forged,
                limit=1,
            )
    finally:
        connector.close()


def test_discovery_and_facets_include_a_zero_acquisition_catalog(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "discovery-metadata-only.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            page = reader.discover_publications(
                connector,
                query=CatalogDiscoveryQuery(search="顯示"),
                limit=1,
            )
            default_page = reader.discover_publications(connector, limit=1)
            languages = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.LANGUAGE,
            )
            recent = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
            )

        assert [publication.gid for publication in page.publications] == [123]
        assert page.publications[0].artifacts == ()
        assert page.total is None
        assert default_page.total == 1
        assert [
            (value.value, value.publication_count) for value in languages.values
        ] == [("zh", 1)]
        assert recent.publications == ()
    finally:
        connector.close()


def test_batched_hydration_matches_streaming_reference_with_fewer_queries(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-hydration-differential.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")

        with (
            patch(
                "h2hdb.vnext_catalog_reader_repository._CanonicalLoader.prefetch",
                autospec=True,
                return_value=None,
            ),
            patch.object(connector, "fetch_one", wraps=connector.fetch_one) as one,
            patch.object(connector, "fetch_all", wraps=connector.fetch_all) as all_rows,
            connector.read_transaction(),
        ):
            streaming_reference = reader.discover_publications(connector, limit=1)
            streaming_query_count = one.call_count + all_rows.call_count

        with (
            patch.object(connector, "fetch_one", wraps=connector.fetch_one) as one,
            patch.object(connector, "fetch_all", wraps=connector.fetch_all) as all_rows,
            connector.read_transaction(),
        ):
            batched = reader.discover_publications(connector, limit=1)
            batched_query_count = one.call_count + all_rows.call_count

        assert batched == streaming_reference
        assert batched_query_count < streaming_query_count
    finally:
        connector.close()


def test_metadata_only_publication_rejects_every_stray_presentation_family(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "metadata-only-orphan-presentation.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        insertions = (
            (
                "catalog_artifacts",
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256, artifact_name, media_type, page_count) "
                "VALUES (1, %s, %s, %s, %s, %s, 0)",
                (
                    values["publication_key"],
                    b"a" * 32,
                    b"s" * 32,
                    b"download.bin",
                    b"application/octet-stream",
                ),
            ),
            (
                "catalog_storage_objects",
                "INSERT INTO catalog_storage_objects "
                "(revision, publication_key, resource_kind, "
                "storage_object_key_sha256, storage_object_sha256, size_bytes, "
                "modified_at) VALUES (1, %s, %s, %s, %s, 1, 1)",
                (
                    values["publication_key"],
                    b"acquisition",
                    b"k" * 32,
                    b"o" * 32,
                ),
            ),
            (
                "catalog_pages",
                "INSERT INTO catalog_pages "
                "(revision, publication_key, resource_kind, page_index, "
                "extent_offset, extent_length, media_type, image_sha256, width, "
                "height) VALUES (1, %s, %s, 0, 0, 1, %s, %s, 1, 1)",
                (
                    values["publication_key"],
                    b"acquisition",
                    b"image/jpeg",
                    b"p" * 32,
                ),
            ),
            (
                "catalog_thumbnails",
                "INSERT INTO catalog_thumbnails "
                "(revision, publication_key, resource_kind, extent_offset, "
                "extent_length, media_type, image_sha256, width, height) "
                "VALUES (1, %s, %s, 0, 1, %s, %s, 1, 1)",
                (
                    values["publication_key"],
                    b"thumbnail",
                    b"image/jpeg",
                    b"t" * 32,
                ),
            ),
        )
        for table, statement, parameters in insertions:
            connector.execute("PRAGMA foreign_keys = OFF")
            connector.execute(statement, parameters)
            connector.execute("PRAGMA foreign_keys = ON")
            with (
                connector.read_transaction(),
                pytest.raises(
                    VNextCatalogReadError,
                    match="zero-artifact revision retains artifact or presentation authority",
                ),
            ):
                reader.get_publication_presentation(
                    connector,
                    "urn:h2h:gallery:123",
                    revision=1,
                )
            connector.execute("PRAGMA foreign_keys = OFF")
            connector.execute(
                f"DELETE FROM {table} WHERE revision = 1 AND publication_key = %s",
                (values["publication_key"],),
            )
            connector.execute("PRAGMA foreign_keys = ON")
    finally:
        connector.close()


def test_artifact_bearing_revision_rejects_a_missing_publication_artifact(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-bearing-missing-artifact.sqlite3")
    try:
        _published_fixture(connector, artifact_count=1)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(
                VNextCatalogReadError,
                match="artifact-bearing revision lacks a publication artifact",
            ),
        ):
            reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )
        with (
            connector.read_transaction(),
            pytest.raises(
                VNextCatalogReadError,
                match="artifact-bearing revision lacks a publication artifact",
            ),
        ):
            reader.get_publication_presentation(
                connector,
                "urn:h2h:gallery:123",
                revision=1,
            )
    finally:
        connector.close()


def test_search_discovery_plan_starts_from_the_lexeme_index(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "discovery-search-plan.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        query = CatalogDiscoveryQuery(search="顯示 原始")
        sql_filter = _discovery_filter_sql(query, revision=1, backend="sqlite")
        sql = _discovery_page_sql(sql_filter)
        parameters = (
            *sql_filter.cte_parameters,
            1,
            1,
            1,
            -1,
            *sql_filter.parameters,
            2,
        )
        plan_rows = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
        plan = " ".join(str(row[3]) for row in plan_rows)

        assert sql.startswith("WITH matched_search(publication_key) AS (")
        assert "FROM catalog_search_postings AS posting" in sql
        assert "SELECT COUNT(*) FROM catalog_search_postings" not in sql
        assert "posting USING COVERING INDEX" in plan
        assert "revision=? AND value_sha256=?" in plan
    finally:
        connector.close()


def test_reader_revalidates_forged_query_cursor_and_revision_domains(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "discovery-forged-domains.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        query = CatalogDiscoveryQuery(search="顯示")
        valid_discovery_cursor = CatalogDiscoveryCursor(
            revision=1,
            query_sha256=_discovery_query_sha256(query),
            position=0,
            publication_id="urn:h2h:gallery:123",
        )
        discovery_mutations: tuple[tuple[str, object], ...] = (
            ("revision", True),
            ("position", -1),
            ("position", 1 << 63),
            ("query_sha256", valid_discovery_cursor.query_sha256.upper()),
            ("publication_id", "urn:h2h:gallery:0123"),
        )
        for field, forged_value in discovery_mutations:
            discovery_cursor = CatalogDiscoveryCursor(
                revision=valid_discovery_cursor.revision,
                query_sha256=valid_discovery_cursor.query_sha256,
                position=valid_discovery_cursor.position,
                publication_id=valid_discovery_cursor.publication_id,
            )
            object.__setattr__(discovery_cursor, field, forged_value)
            with (
                connector.read_transaction(),
                pytest.raises(CatalogCursorError, match="public domain"),
            ):
                reader.discover_publications(
                    connector,
                    query=query,
                    after=discovery_cursor,
                    limit=1,
                )

        effective = CatalogDiscoveryQuery()
        valid_facet_cursor = CatalogFacetCursor(
            revision=1,
            query_sha256=_discovery_query_sha256(effective),
            facet=CatalogFacetKind.LANGUAGE,
            position=0,
            value_sha256=_facet_identity_sha256(
                CatalogFacetKind.LANGUAGE,
                values["language"],
            ),
        )
        facet_mutations: tuple[tuple[str, object], ...] = (
            ("revision", True),
            ("position", -1),
            ("position", 1 << 63),
            ("query_sha256", valid_facet_cursor.query_sha256.upper()),
            ("value_sha256", valid_facet_cursor.value_sha256.upper()),
            ("facet", "language"),
        )
        for field, forged_value in facet_mutations:
            facet_cursor = CatalogFacetCursor(
                revision=valid_facet_cursor.revision,
                query_sha256=valid_facet_cursor.query_sha256,
                facet=valid_facet_cursor.facet,
                position=valid_facet_cursor.position,
                value_sha256=valid_facet_cursor.value_sha256,
            )
            object.__setattr__(facet_cursor, field, forged_value)
            with (
                connector.read_transaction(),
                pytest.raises(CatalogCursorError, match="public domain"),
            ):
                reader.list_publication_facets(
                    connector,
                    facet=CatalogFacetKind.LANGUAGE,
                    after=facet_cursor,
                    limit=1,
                )

        forged_query = CatalogDiscoveryQuery(search="顯示")
        object.__setattr__(forged_query, "search_lexemes", (b"forged",))
        with pytest.raises(ValueError, match="search_lexemes are not canonical"):
            reader.discover_publications(connector, query=forged_query)

        subject = CatalogSubjectFilter(namespace="genre", value="科幻")
        forged_subject_query = CatalogDiscoveryQuery(subject=subject)
        object.__setattr__(subject, "namespace", 1)
        with pytest.raises(ValueError):
            reader.discover_publications(connector, query=forged_subject_query)

        contributor = CatalogContributorFilter(name="作者", role="author")
        forged_contributor_query = CatalogDiscoveryQuery(contributor=contributor)
        object.__setattr__(contributor, "role", "foreign")
        with pytest.raises(ValueError, match="role is not registered"):
            reader.discover_publications(connector, query=forged_contributor_query)

        with connector.read_transaction():
            forged_revision = reader.get_catalog_revision(connector)
        object.__setattr__(forged_revision, "publication_count", True)
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="descriptor is malformed"),
        ):
            reader.get_publication(
                connector,
                "urn:h2h:gallery:123",
                revision=forged_revision,
            )
    finally:
        connector.close()


def test_facets_exclude_the_active_family_and_specialized_subject_namespaces(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "facet-reader.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            languages = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.LANGUAGE,
                query=CatalogDiscoveryQuery(language="not-active"),
            )
            subjects = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.SUBJECT,
                query=CatalogDiscoveryQuery(
                    subject=CatalogSubjectFilter(
                        namespace="missing",
                        value="missing",
                    )
                ),
            )
            contributors = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.CONTRIBUTOR,
                query=CatalogDiscoveryQuery(
                    contributor=CatalogContributorFilter(
                        name="missing",
                        role="author",
                    )
                ),
            )
        assert [
            (value.value, value.publication_count) for value in languages.values
        ] == [("zh", 1)]
        assert [
            (value.namespace, value.value, value.publication_count)
            for value in subjects.values
        ] == [("genre", "科幻", 1)]
        assert [
            (value.role, value.value, value.publication_count)
            for value in contributors.values
        ] == [("author", "作者", 1)]

        filtered_query = CatalogDiscoveryQuery(
            subject=CatalogSubjectFilter(namespace="genre", value="不存在")
        )
        forged = CatalogFacetCursor(
            revision=1,
            query_sha256=_discovery_query_sha256(filtered_query),
            facet=CatalogFacetKind.LANGUAGE,
            position=0,
            value_sha256=_facet_identity_sha256(
                CatalogFacetKind.LANGUAGE,
                values["language"],
            ),
        )
        with (
            connector.read_transaction(),
            pytest.raises(
                CatalogCursorError,
                match="not a member",
            ),
        ):
            reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.LANGUAGE,
                query=filtered_query,
                after=forged,
            )
    finally:
        connector.close()


def test_facet_counts_and_pages_are_exact_and_seekable(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "facet-distinct-reader.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        seed_tag_term(
            connector,
            tag_id=3,
            namespace=b"topic",
            tag_value_sha256=values["tag_value"],
        )
        connector.execute(
            "INSERT INTO catalog_subjects "
            "(revision, publication_key, position, tag_id) VALUES (1, %s, 2, 3)",
            (values["publication_key"],),
        )
        connector.execute(
            "INSERT INTO catalog_subject_facet_order "
            "(revision, position, tag_id, occurrence_count) VALUES (1, 1, 3, 1)"
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            first = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.SUBJECT,
                limit=1,
            )
            assert first.next_cursor is not None
            second = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.SUBJECT,
                after=first.next_cursor,
                limit=1,
            )
            contributors = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.CONTRIBUTOR,
            )

        assert [
            (value.namespace, value.value, value.publication_count)
            for value in (*first.values, *second.values)
        ] == [("genre", "科幻", 1), ("topic", "測試", 1)]
        assert second.next_cursor is None
        assert [
            (value.role, value.value, value.publication_count)
            for value in contributors.values
        ] == [("author", "作者", 1)]
    finally:
        connector.close()


def test_recent_artifact_windows_sort_by_authoritative_times_and_gid_ties(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-order.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        _add_recent_artifact_publication(
            connector,
            values=values,
            semantics_sha256=artifact_values["semantics"],
            gid=124,
            position=1,
            upload_time=2000000,
            download_time=5000000,
        )
        _add_recent_artifact_publication(
            connector,
            values=values,
            semantics_sha256=artifact_values["semantics"],
            gid=125,
            position=2,
            upload_time=1000000,
            download_time=6000000,
        )
        connector.execute(
            "UPDATE catalog_revision_descriptors "
            "SET publication_count = 3, artifact_count = 3 WHERE revision = 1"
        )
        connector.execute("PRAGMA foreign_keys = ON")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            uploaded = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
                revision=1,
            )
            downloaded = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.DOWNLOADED,
                revision=1,
            )
        assert uploaded.order is CatalogRecentOrder.UPLOADED
        assert [publication.gid for publication in uploaded.publications] == [
            124,
            123,
            125,
        ]
        assert downloaded.order is CatalogRecentOrder.DOWNLOADED
        assert [publication.gid for publication in downloaded.publications] == [
            125,
            124,
            123,
        ]
        assert all(
            len(publication.artifacts) == 1 for publication in downloaded.publications
        )
    finally:
        connector.close()


@pytest.mark.deep
def test_recent_artifact_window_is_fixed_to_top_128(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-128.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        for position, gid in enumerate(range(1000, 1128), start=1):
            _add_recent_artifact_publication(
                connector,
                values=values,
                semantics_sha256=artifact_values["semantics"],
                gid=gid,
                position=position,
                upload_time=3000000 + gid,
                download_time=3000000 + gid,
            )
        connector.execute(
            "UPDATE catalog_revision_descriptors "
            "SET publication_count = 129, artifact_count = 129 WHERE revision = 1"
        )
        connector.execute("PRAGMA foreign_keys = ON")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            window = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
                revision=1,
            )
        assert len(window.publications) == 128
        assert [publication.gid for publication in window.publications] == list(
            range(1127, 999, -1)
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        excluded_key = values["publication_key"]
        connector.execute(
            "DELETE FROM catalog_publication_download_times WHERE "
            "catalog_occurrence_sha256 = ("
            "SELECT catalog_occurrence_sha256 "
            "FROM catalog_publication_occurrence_identities "
            "WHERE revision = 1 AND publication_key = %s)",
            (excluded_key,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="authority is incomplete"),
        ):
            reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
                revision=1,
            )
    finally:
        connector.close()


def test_recent_artifact_window_handles_artifactless_revision(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-empty.sqlite3")
    try:
        _published_fixture(connector, artifact_count=0)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            window = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.DOWNLOADED,
                revision=1,
            )
        assert window.publications == ()
    finally:
        connector.close()


def test_recent_artifact_window_rejects_artifact_count_mismatch(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-count.sqlite3")
    try:
        values = _published_fixture(connector)
        _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        connector.execute(
            "UPDATE catalog_revision_descriptors "
            "SET publication_count = 2, artifact_count = 2 WHERE revision = 1"
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="artifact_count disagrees"),
        ):
            reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
                revision=1,
            )
    finally:
        connector.close()


def test_recent_artifact_window_rejects_untyped_order_before_sql(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-invalid-order.sqlite3")
    try:
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with pytest.raises(TypeError, match="must be CatalogRecentOrder"):
            reader.list_recent_publications(
                connector,
                order="uploaded",  # type: ignore[arg-type]  # Runtime boundary evidence.
            )
    finally:
        connector.close()


def test_recent_artifact_window_rejects_missing_download_authority(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-recent-corrupt.sqlite3")
    try:
        values = _published_fixture(connector)
        _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_publication_download_times WHERE "
            "catalog_occurrence_sha256 = ("
            "SELECT catalog_occurrence_sha256 "
            "FROM catalog_publication_occurrence_identities "
            "WHERE revision = 1 AND publication_key = %s)",
            (values["publication_key"],),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="authority is incomplete"),
        ):
            reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.DOWNLOADED,
                revision=1,
            )
    finally:
        connector.close()


def test_recent_artifact_window_rechecks_head_after_hydration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-recent-head-race.sqlite3")
    try:
        values = _published_fixture(connector)
        _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        _publication_commit(
            connector,
            snapshot_manifest_sha256=values["snapshot_manifest"],
            revision=2,
            source_revision=2,
            generation=2,
            publication_count=0,
            artifact_count=0,
            committed_at=5000000,
            receipt_id=b"s" * 16,
            candidate_id=b"d" * 16,
            preparation_id=b"q" * 16,
        )
        connector.execute(
            "UPDATE catalog_publication_commit_head_receipts "
            "SET receipt_id = %s WHERE channel = %s",
            (b"r" * 16, b"default"),
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        hydrate = reader._hydrate_publications

        def hydrate_then_advance(*args: Any, **kwargs: Any) -> Any:
            result = hydrate(*args, **kwargs)
            connector.execute(
                "UPDATE catalog_publication_commit_head_receipts "
                "SET receipt_id = %s WHERE channel = %s",
                (b"s" * 16, b"default"),
            )
            return result

        monkeypatch.setattr(reader, "_hydrate_publications", hydrate_then_advance)
        with pytest.raises(VNextCatalogReadError, match="head advanced"):
            reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
            )
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
            "urn:h2h:artifact:acquisition:１２３:sha256:" + "0" * 64,
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
            "",
            ".",
            "..",
            "nested/name.bin",
            "nested\\name.bin",
            "control\nname.bin",
            "x" * 256,
        ):
            with pytest.raises(VNextCatalogIdentifierError):
                reader.get_publications_by_artifact_names(connector, [value])
        with pytest.raises(TypeError):
            reader.get_publications_by_artifact_names(connector, "download-123.bin")
        with pytest.raises(ValueError, match="at most 128"):
            reader.get_publications_by_artifact_names(
                connector,
                ["download.bin"] * 129,
            )
        assert queries == []
    finally:
        connector.close()


def test_artifact_name_lookup_uses_bounded_set_queries_per_publication_family(
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
        names = [f"download-{gid}.bin" for gid in range(1, 129)]
        with connector.read_transaction():
            publications = reader.get_publications_by_artifact_names(
                connector,
                names,
                revision=1,
            )
        assert tuple(publications) == ("download-123.bin",)
        assert (
            sum(
                "FROM catalog_publications AS publication" in query for query in queries
            )
            == 1
        )
        assert (
            sum(
                "FROM catalog_contributors AS contributor JOIN selected" in query
                for query in queries
            )
            == 1
        )
        assert sum("FROM catalog_subjects AS s" in query for query in queries) == 1
        assert (
            sum("FROM catalog_artifacts AS artifact" in query for query in queries) == 2
        )
    finally:
        connector.close()


def test_single_publication_lookup_rejects_missing_atomic_title_row(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-partial-publication.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_publication_storage WHERE "
            "catalog_occurrence_sha256 = ("
            "SELECT catalog_occurrence_sha256 "
            "FROM catalog_publication_occurrence_identities "
            "WHERE revision = 1 AND publication_key = %s)",
            (values["publication_key"],),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="missing or noncongruent"),
        ):
            reader.get_publication(connector, "urn:h2h:gallery:123", revision=1)
    finally:
        connector.close()


def test_artifact_reader_hydrates_the_exact_opaque_storage_key(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-artifact-storage-key.sqlite3")
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            artifact = reader.get_artifact(
                connector,
                artifact_values["artifact_id"].decode("ascii"),
                revision=1,
            )
        assert artifact is not None
        assert artifact.storage_object.key == artifact_values["storage_key"]
        assert artifact.storage_object.key.segments == ("acquisition", "123")
    finally:
        connector.close()


def test_recent_feed_rejects_a_missing_tail_row_without_counting(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-artifact-missing-tail.sqlite3")
    try:
        values = _published_fixture(connector)
        _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        connector.execute(
            "DELETE FROM catalog_artifacts WHERE revision = 1 AND publication_key = %s",
            (values["publication_key"],),
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="artifact_count"),
        ):
            reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.UPLOADED,
            )
    finally:
        connector.close()


def test_reader_rejects_missing_order_row_and_wrong_canonical_domain(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-corrupt.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _seed_discovery_authority(connector, values=values)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        connector.execute("DELETE FROM catalog_publication_order")
        with (
            connector.read_transaction(),
            pytest.raises(VNextCatalogReadError, match="publication_count"),
        ):
            reader.discover_publications(connector)

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
            reader.discover_publications(connector)
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

        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (1, 0, 0)"
        )
        with (
            connector.read_transaction(),
            pytest.raises(CatalogRevisionNotFoundError) as unpublished_descriptor,
        ):
            reader.get_catalog_revision(connector, 1)
        assert unpublished_descriptor.value.revision == 1
    finally:
        connector.close()


def test_reader_requires_a_discovery_seal_and_query_constructor_fails_early(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "reader-search.sqlite3")
    try:
        _published_fixture(connector, artifact_count=0)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with (
            connector.read_transaction(),
            pytest.raises(
                VNextCatalogReadError,
                match="lacks its exact discovery seal",
            ),
        ):
            reader.discover_publications(
                connector,
                query=CatalogDiscoveryQuery(search="顯示"),
            )

        with pytest.raises(ValueError, match="no searchable lexemes"):
            CatalogDiscoveryQuery(search="  ")
        with pytest.raises(ValueError, match="no searchable lexemes"):
            CatalogDiscoveryQuery(search="---")
    finally:
        connector.close()


def test_reader_rejects_explicit_and_pinned_revision_after_head_advances(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "reader-history.sqlite3"
    connector = _database(database_path)
    try:
        values = _published_fixture(connector, artifact_count=0)
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with connector.read_transaction():
            prior_head = reader.get_catalog_revision(connector)
        _publication_commit(
            connector,
            snapshot_manifest_sha256=values["snapshot_manifest"],
            revision=2,
            source_revision=2,
            generation=2,
            publication_count=1,
            artifact_count=0,
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
            current_publication = reader.get_publication(
                read_only,
                "urn:h2h:gallery:123",
                revision=current,
            )
            with pytest.raises(CatalogRevisionNotFoundError) as explicit_history:
                reader.get_catalog_revision(read_only, 1)
            with pytest.raises(CatalogRevisionNotFoundError) as pinned_history:
                reader.get_publication(
                    read_only,
                    "urn:h2h:gallery:123",
                    revision=prior_head,
                )
            with pytest.raises(CatalogRevisionNotFoundError) as empty_history:
                reader.get_publications_by_artifact_names(
                    read_only,
                    [],
                    revision=prior_head,
                )
            with pytest.raises(CatalogRevisionNotFoundError) as empty_history_int:
                reader.get_publications_by_artifact_names(
                    read_only,
                    [],
                    revision=1,
                )
        assert current.revision == 2
        assert explicit_history.value.revision == 1
        assert pinned_history.value.revision == 1
        assert empty_history.value.revision == 1
        assert empty_history_int.value.revision == 1
        assert current_publication is not None
    finally:
        read_only.close()


def test_reader_rechecks_head_after_hydration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _database(tmp_path / "reader-head-race.sqlite3")
    try:
        values = _published_fixture(connector, artifact_count=0)
        _publication_commit(
            connector,
            snapshot_manifest_sha256=values["snapshot_manifest"],
            revision=2,
            source_revision=2,
            generation=2,
            publication_count=1,
            artifact_count=0,
            committed_at=5000000,
            receipt_id=b"s" * 16,
            candidate_id=b"d" * 16,
            preparation_id=b"q" * 16,
        )
        connector.execute(
            "UPDATE catalog_publication_commit_head_receipts "
            "SET receipt_id = %s WHERE channel = %s",
            (b"r" * 16, b"default"),
        )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        hydrate = reader._hydrate_publications

        def hydrate_then_advance(*args: Any, **kwargs: Any) -> Any:
            result = hydrate(*args, **kwargs)
            connector.execute(
                "UPDATE catalog_publication_commit_head_receipts "
                "SET receipt_id = %s WHERE channel = %s",
                (b"s" * 16, b"default"),
            )
            return result

        monkeypatch.setattr(reader, "_hydrate_publications", hydrate_then_advance)
        with pytest.raises(VNextCatalogReadError, match="head advanced"):
            reader.get_publication(connector, "urn:h2h:gallery:123")
    finally:
        connector.close()


def test_fk_on_current_only_facade_drains_all_payload_and_keeps_ready(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "current-only-facade.sqlite3"
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
    )
    admin = VNextDatabaseAdminFacade(config)
    assert admin.initialize().state == "READY"
    connector = SQLiteConnector(str(database_path))
    connector.connect()
    try:
        values = _published_fixture(connector)
        artifact_values = _artifact_fixture(
            connector,
            publication_key_value=values["publication_key"],
        )
        old_only_canonical_values = (
            values["locator"],
            values["snapshot_manifest"],
            values["source_title"],
            values["display_title"],
            values["sort_title"],
            values["summary"],
            values["language"],
            values["content"],
            values["contributor"],
            values["tag_value"],
            artifact_values["semantics"],
            artifact_values["source_manifest"],
            artifact_values["member_plan"],
            artifact_values["effective_content"],
            artifact_values["selected"],
            artifact_values["owner"],
        )
        old_only_root_pages = {
            value: _canonical_root_page(connector, value)
            for value in old_only_canonical_values
        }
        current_source_root_page = _canonical_root_page(
            connector,
            values["source_root"],
        )
        seed_publication_finalization(
            connector,
            receipt_id=b"r" * 16,
            cursor=values["publication_key"],
            processed_count=1,
            finalized_at=4_000_000,
        )
        current_snapshot = _canonical(
            connector,
            "source_snapshot_manifest_v1",
            b"reader-current-empty-snapshot",
        )
        seed_snapshot_manifest(
            connector,
            snapshot_manifest_sha256=current_snapshot,
            gallery_count=0,
            file_count=0,
            byte_count=0,
        )
        current_snapshot_root_page = _canonical_root_page(connector, current_snapshot)
        _publication_commit(
            connector,
            snapshot_manifest_sha256=current_snapshot,
            revision=2,
            source_revision=2,
            generation=2,
            publication_count=0,
            committed_at=5_000_000,
            receipt_id=b"s" * 16,
            candidate_id=b"d" * 16,
            preparation_id=b"q" * 16,
        )
        connector.execute(
            "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (2, 1)"
        )

        assert connector.fetch_one(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (b"s" * 16,),
        ) == ("DB_COMMITTED", None)
        blocked_facade = VNextIngestFacade(config, clock=lambda: 5_500_000)
        for _attempt in range(32):
            before_finalization = blocked_facade.drain_current_only_maintenance(
                30_000_000
            )
            assert connector.fetch_one("PRAGMA foreign_keys") == (1,)
            assert connector.fetch_all("PRAGMA foreign_key_check") == []
            if before_finalization is VNextCurrentOnlyMaintenanceOutcome.BLOCKED:
                break
            assert before_finalization is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        assert before_finalization is VNextCurrentOnlyMaintenanceOutcome.BLOCKED
        # Unrelated orphan families may already be reclaimed while external
        # finalization is pending, but the current head's snapshot stays live.
        _assert_canonical_value_storage(
            connector,
            value_sha256=current_snapshot,
            root_page_sha256=current_snapshot_root_page,
            present=True,
        )
        # Both publication builds use the same source scope.  Its root is the
        # current metadata baseline and must outlive historical payload GC.
        _assert_canonical_value_storage(
            connector,
            value_sha256=values["source_root"],
            root_page_sha256=current_source_root_page,
            present=True,
        )
        generation_count = connector.fetch_one(
            "SELECT COUNT(*) FROM operational_maintenance_gate_generations"
        )
        for _attempt in range(3):
            assert (
                blocked_facade.drain_current_only_maintenance(30_000_000)
                is VNextCurrentOnlyMaintenanceOutcome.BLOCKED
            )
            assert connector.fetch_one("PRAGMA foreign_keys") == (1,)
            assert connector.fetch_all("PRAGMA foreign_key_check") == []
        assert (
            connector.fetch_one(
                "SELECT COUNT(*) FROM operational_maintenance_gate_generations"
            )
            == generation_count
        )

        seed_publication_finalization(
            connector,
            receipt_id=b"s" * 16,
            cursor=b"",
            processed_count=0,
            finalized_at=6_000_000,
        )
        assert connector.fetch_one("PRAGMA foreign_keys") == (1,)
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES:
            assert (
                connector.fetch_one(f"SELECT COUNT(*) FROM {table} WHERE revision = 1")[
                    0
                ]
                > 0
            )
            assert (
                connector.fetch_one(f"SELECT COUNT(*) FROM {table} WHERE revision = 2")[
                    0
                ]
                == 0
            )
    finally:
        connector.close()

    assert admin.check().state == "READY"
    facade = VNextIngestFacade(config, clock=lambda: 7_000_000)
    with SQLiteConnector(str(database_path)) as inspection:
        assert inspection.fetch_one(
            "SELECT COUNT(*) FROM operational_cleanup_jobs WHERE state = 'OPEN'"
        ) == (0,)
    # Actionable old payload fences a new ingest even before the first shard
    # job opens; otherwise a bounded attempt that just completed one shard
    # could recreate a predecessor pin to a partially reclaimed revision.
    assert facade.try_claim_ingest(True, 30_000_000) is None
    with SQLiteConnector(str(database_path)) as inspection:
        for table in (
            "operational_maintenance_gate_holders",
            "operational_maintenance_gate_owners",
            "operational_ingest_generation_owners",
        ):
            assert inspection.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)

    attempts = [facade.drain_current_only_maintenance(30_000_000)]
    assert attempts == [VNextCurrentOnlyMaintenanceOutcome.PROGRESSED]

    # A bounded attempt releases EXCLUSIVE after either retaining an OPEN
    # checkpoint or completing an empty phase. A tentative SHARED ingest claim
    # must atomically roll back while any actionable predecessor payload remains.
    assert facade.try_claim_ingest(True, 30_000_000) is None
    with SQLiteConnector(str(database_path)) as inspection:
        for table in (
            "operational_maintenance_gate_holders",
            "operational_maintenance_gate_owners",
            "operational_ingest_generation_owners",
        ):
            assert inspection.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)

    for _attempt in range(64):
        attempts.append(facade.drain_current_only_maintenance(30_000_000))
        with SQLiteConnector(str(database_path)) as inspection:
            assert inspection.fetch_one("PRAGMA foreign_keys") == (1,)
            assert inspection.fetch_all("PRAGMA foreign_key_check") == []
        if attempts[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE:
            break
    assert attempts[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE
    session = facade.try_claim_ingest(True, 30_000_000)
    assert session is not None
    facade.complete_ingest(session)

    catalog = VNextCatalogFacade(config)
    current = catalog.get_catalog_revision()
    assert current.revision == 2
    assert catalog.discover_publications(revision=current).total == 0
    with pytest.raises(CatalogRevisionNotFoundError):
        catalog.discover_publications(revision=1)
    assert admin.check().state == "READY"

    connector = SQLiteConnector(str(database_path))
    connector.connect()
    try:
        assert connector.fetch_one("PRAGMA foreign_keys") == (1,)
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        assert (
            connector.fetch_one(
                "SELECT candidate_id FROM catalog_publication_candidates "
                "WHERE candidate_id = %s",
                (b"d" * 16,),
            )
            == ()
        )
        for table in _CATALOG_PUBLICATION_PAYLOAD_TABLES:
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE revision = 1"
            ) == (0,)
            assert (
                connector.fetch_one(f"SELECT COUNT(*) FROM {table} WHERE revision = 2")[
                    0
                ]
                == 0
            )
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_source_revision_descriptors "
            "WHERE source_revision = 1"
        ) == (values["snapshot_manifest"],)
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_source_revision_descriptors "
            "WHERE source_revision = 2"
        ) == (current_snapshot,)
        for value in old_only_canonical_values:
            _assert_canonical_value_storage(
                connector,
                value_sha256=value,
                root_page_sha256=old_only_root_pages[value],
                present=False,
            )
        _assert_canonical_value_storage(
            connector,
            value_sha256=current_snapshot,
            root_page_sha256=current_snapshot_root_page,
            present=True,
        )
        assert (
            connector.fetch_one(
                "SELECT snapshot_manifest_sha256 "
                "FROM catalog_source_snapshot_manifest_identity "
                "WHERE snapshot_manifest_sha256 = %s",
                (values["snapshot_manifest"],),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_source_snapshot_manifest_identity "
            "WHERE snapshot_manifest_sha256 = %s",
            (current_snapshot,),
        ) == (current_snapshot,)
        assert connector.fetch_all("SELECT * FROM catalog_display_title_choices") == []
        assert connector.fetch_all("SELECT * FROM catalog_title_sorts") == []
        assert (
            connector.fetch_one(
                "SELECT artifact_sha256 FROM catalog_artifact_blobs "
                "WHERE artifact_sha256 = %s",
                (artifact_values["artifact_sha256"],),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT artifact_semantics_sha256 "
                "FROM catalog_artifact_semantic_inputs "
                "WHERE artifact_semantics_sha256 = %s",
                (artifact_values["semantics"],),
            )
            == ()
        )
        for table in (
            "catalog_publication_identities",
            "catalog_gallery_identities",
            "catalog_gallery_source_name_accesses",
            "catalog_source_gallery_name_gids",
            "catalog_gallery_upload_times",
            "catalog_source_locator_identity",
        ):
            assert connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_source_scopes") == (1,)
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_tag_terms") == (0,)
        assert connector.fetch_one(
            "SELECT publication_count FROM catalog_revision_descriptors "
            "WHERE revision = 1"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_source_revision_descriptors "
            "WHERE source_revision = 1"
        ) == (values["snapshot_manifest"],)
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_source_revision_descriptors "
            "WHERE source_revision = 2"
        ) == (current_snapshot,)
        assert connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commit_head_receipts "
            "WHERE channel = %s",
            (b"default",),
        ) == (b"s" * 16,)
        assert connector.fetch_one(
            "SELECT revision FROM catalog_publication_commits WHERE receipt_id = %s",
            (b"r" * 16,),
        ) == (1,)
    finally:
        connector.close()
