from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import vnext_identity as identity
from h2hdb.domain import (
    ArtifactArchiveRenderEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactStorageEvidence,
    ByteExtent,
    CatalogResourceKind,
    PreparedPublicationPresentation,
    PreparedThumbnailResource,
    StorageObjectDescriptor,
    StorageObjectKey,
)
from h2hdb.ports import ArtifactStorageAdapter
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_preparation_repository import ArtifactPersistenceReceipt

_CANDIDATE = b"c" * 16
_PUBLICATION = b"p" * 32
_MODIFIED_AT = datetime(2026, 8, 31, tzinfo=UTC)


class _NeutralAdapter:
    adapter_id = b"neutral-fixture-v2"
    policy_fingerprint_sha256 = b"f" * 32

    def storage_key(
        self,
        gid: int,
        resource_kind: CatalogResourceKind,
    ) -> StorageObjectKey:
        return StorageObjectKey("fixture-v2", (str(gid), resource_kind.value))

    def open_source(
        self,
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        del source_root_components, gallery_locator_components
        return BytesIO(source_name)

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        del members, gid
        payload = b"neutral archive"
        assert destination.write(payload) == len(payload)
        return ArtifactArchiveRenderEvidence(
            sha256(payload).digest(),
            len(payload),
            "application/octet-stream",
            "publication.bin",
            (),
        )

    def protect(
        self,
        archive: BinaryIO,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        modified_at: datetime,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        payload = archive.read()
        assert sha256(payload).digest() == expected_sha256
        assert len(payload) == expected_size_bytes
        assert len(protection_token) == 32
        return ArtifactStorageEvidence(
            True,
            StorageObjectDescriptor(
                storage_key,
                expected_size_bytes,
                expected_sha256.hex(),
                modified_at,
            ),
        )

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        del archive, thumbnail_destination, rendered_pages
        return ArtifactPresentationRenderEvidence((), None)


def _descriptor(kind: CatalogResourceKind) -> StorageObjectDescriptor:
    payload = kind.value.encode("ascii")
    return StorageObjectDescriptor(
        StorageObjectKey("fixture-v2", ("gid-7", kind.value)),
        len(payload),
        sha256(payload).hexdigest(),
        _MODIFIED_AT,
    )


def test_storage_adapter_contract_is_neutral_and_complete() -> None:
    adapter = _NeutralAdapter()

    assert isinstance(adapter, ArtifactStorageAdapter)
    assert adapter.storage_key(7, CatalogResourceKind.ACQUISITION) != (
        adapter.storage_key(7, CatalogResourceKind.THUMBNAIL)
    )


def test_prepared_resource_bundle_uses_distinct_kinds_and_digest32_tokens() -> None:
    acquisition_key = identity.artifact_storage_key_digest(
        "fixture-v2",
        ("gid-7", "acquisition"),
    )
    thumbnail_key = identity.artifact_storage_key_digest(
        "fixture-v2",
        ("gid-7", "thumbnail"),
    )
    acquisition_token = identity.encode_artifact_protection_token(
        _CANDIDATE,
        _PUBLICATION,
        CatalogResourceKind.ACQUISITION.value,
        acquisition_key,
        9,
    )
    thumbnail_token = identity.encode_artifact_protection_token(
        _CANDIDATE,
        _PUBLICATION,
        CatalogResourceKind.THUMBNAIL.value,
        thumbnail_key,
        9,
    )

    receipt = ArtifactPersistenceReceipt(
        candidate_id=_CANDIDATE,
        publication_key=_PUBLICATION,
        artifact_sha256=b"a" * 32,
        resources=(
            (CatalogResourceKind.ACQUISITION, acquisition_token, "PREPARED"),
            (CatalogResourceKind.THUMBNAIL, thumbnail_token, "PREPARED"),
        ),
        replayed=False,
    )

    assert len(acquisition_token) == len(thumbnail_token) == 32
    assert acquisition_token != thumbnail_token
    assert tuple(kind for kind, _token, _state in receipt.resources) == (
        CatalogResourceKind.ACQUISITION,
        CatalogResourceKind.THUMBNAIL,
    )


def test_persistence_receipt_rejects_noncanonical_resource_bundle() -> None:
    token = b"t" * 32
    with pytest.raises(ValueError, match="strictly ordered"):
        ArtifactPersistenceReceipt(
            candidate_id=_CANDIDATE,
            publication_key=_PUBLICATION,
            artifact_sha256=b"a" * 32,
            resources=(
                (CatalogResourceKind.THUMBNAIL, token, "PREPARED"),
                (CatalogResourceKind.ACQUISITION, token, "PREPARED"),
            ),
            replayed=False,
        )
    with pytest.raises(ValueError, match="32 bytes"):
        ArtifactPersistenceReceipt(
            candidate_id=_CANDIDATE,
            publication_key=_PUBLICATION,
            artifact_sha256=b"a" * 32,
            resources=((CatalogResourceKind.ACQUISITION, b"short", "PREPARED"),),
            replayed=False,
        )


def test_descriptor_presentation_totality_rejects_thumbnail_without_pages() -> None:
    descriptor = _descriptor(CatalogResourceKind.THUMBNAIL)
    thumbnail = PreparedThumbnailResource(
        storage_object=descriptor,
        extent=ByteExtent(0, descriptor.size_bytes),
        media_type="image/example",
        sha256=descriptor.sha256,
        width=1,
        height=1,
    )

    with pytest.raises(ValueError, match="exactly when pages exist"):
        PreparedPublicationPresentation((), thumbnail)


def _foreign_keys(
    connector: SQLiteConnector,
    table: str,
) -> set[tuple[str, str, str]]:
    rows = connector.fetch_all(f"PRAGMA foreign_key_list({table})")
    return {(str(row[2]), str(row[3]), str(row[4])) for row in rows}


def test_prepared_resource_blob_schema_binds_before_storage_confirmation(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "prepared-v2.sqlite3")
    try:
        columns = tuple(
            str(row[1])
            for row in connector.fetch_all(
                "PRAGMA table_info(catalog_prepared_resource_blob)"
            )
        )

        assert columns == (
            "candidate_id",
            "publication_key",
            "resource_kind",
            "storage_object_sha256",
        )
        assert (
            "catalog_prepared_artifacts",
            "candidate_id",
            "candidate_id",
        ) in _foreign_keys(connector, "catalog_prepared_resource_blob")
        assert (
            "catalog_artifact_blobs",
            "storage_object_sha256",
            "artifact_sha256",
        ) in _foreign_keys(connector, "catalog_prepared_resource_blob")
        assert (
            "catalog_prepared_resource_blob",
            "candidate_id",
            "candidate_id",
        ) in _foreign_keys(connector, "catalog_prepared_storage_objects")
    finally:
        connector.close()


def test_catalog_storage_object_retains_neutral_verified_blob(tmp_path: Path) -> None:
    connector = open_generated_sqlite_database(tmp_path / "catalog-v2.sqlite3")
    try:
        assert (
            "catalog_artifact_blobs",
            "storage_object_sha256",
            "artifact_sha256",
        ) in _foreign_keys(connector, "catalog_storage_objects")
    finally:
        connector.close()
