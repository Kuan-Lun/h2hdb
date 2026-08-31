from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from h2hdb import vnext_identity as identity
from h2hdb.domain import CatalogResourceKind, StorageObjectKey
from h2hdb.vnext_catalog_reader_repository import (
    VNextCatalogReaderRepository,
    VNextCatalogReadError,
    _ArtifactFacts,
)


class _PresentationConnector:
    def __init__(self, *, thumbnail_offset: int = 0) -> None:
        self.publication_key = identity.publication_key(7)
        self.artifact_sha256 = bytes.fromhex("11" * 32)
        self.thumbnail_sha256 = bytes.fromhex("22" * 32)
        self.acquisition_key = StorageObjectKey("test-v2", ("acquisition", "7"))
        self.thumbnail_key = StorageObjectKey("test-v2", ("thumbnail", "7"))
        self.thumbnail_offset = thumbnail_offset

    def fetch_all(
        self,
        query: str,
        data: Sequence[object] = (),
    ) -> list[tuple[Any, ...]]:
        del data
        if "FROM catalog_storage_objects AS object" in query:
            return [
                self._storage_row(
                    CatalogResourceKind.ACQUISITION,
                    self.acquisition_key,
                    self.artifact_sha256,
                    16,
                ),
                self._storage_row(
                    CatalogResourceKind.THUMBNAIL,
                    self.thumbnail_key,
                    self.thumbnail_sha256,
                    8,
                ),
            ]
        if "FROM catalog_storage_object_key_segments" in query:
            rows: list[tuple[Any, ...]] = []
            for key in (self.acquisition_key, self.thumbnail_key):
                digest = identity.artifact_storage_key_digest(key.codec, key.segments)
                rows.extend(
                    (digest, position, segment.encode("utf-8"))
                    for position, segment in enumerate(key.segments)
                )
            return sorted(rows)
        if "COUNT(*), MIN(page.page_index)" in query:
            return [(self.publication_key, 2, 0, 1)]
        if "page.page_index = 0" in query:
            return [
                (
                    self.publication_key,
                    b"acquisition",
                    0,
                    4,
                    b"image/jpeg",
                    bytes.fromhex("33" * 32),
                    10,
                    20,
                )
            ]
        if "FROM catalog_thumbnails AS thumbnail" in query:
            return [
                (
                    self.publication_key,
                    b"thumbnail",
                    self.thumbnail_offset,
                    8 - self.thumbnail_offset,
                    b"image/jpeg",
                    self.thumbnail_sha256,
                    5,
                    10,
                )
            ]
        raise AssertionError(query)

    def _storage_row(
        self,
        kind: CatalogResourceKind,
        key: StorageObjectKey,
        object_sha256: bytes,
        size_bytes: int,
    ) -> tuple[object, ...]:
        key_digest = identity.artifact_storage_key_digest(key.codec, key.segments)
        return (
            self.publication_key,
            kind.value.encode("ascii"),
            key_digest,
            object_sha256,
            size_bytes,
            0,
            key_digest,
            key.codec.encode("ascii"),
            len(key.segments),
        )


def _artifact(connector: _PresentationConnector) -> _ArtifactFacts:
    return _ArtifactFacts(
        artifact_sha256=connector.artifact_sha256,
        size_bytes=16,
        artifact_semantics_sha256=bytes.fromhex("44" * 32),
        artifact_name=b"artifact.bin",
        media_type=b"application/octet-stream",
        page_count=2,
    )


def test_reader_hydrates_generic_cover_and_whole_object_thumbnail() -> None:
    connector = _PresentationConnector()
    repository = VNextCatalogReaderRepository(backend="sqlite")

    result = repository._published_resources_for_artifacts(  # noqa: SLF001
        connector,  # type: ignore[arg-type]  # deterministic repository fake
        revision=3,
        artifacts={connector.publication_key: _artifact(connector)},
    )[connector.publication_key]

    assert result.acquisition.key == connector.acquisition_key
    assert result.cover is not None
    assert result.cover.storage_object == result.acquisition
    assert result.thumbnail is not None
    assert result.thumbnail.extent.offset == 0
    assert result.thumbnail.extent.length == 8
    assert result.thumbnail.sha256 == result.thumbnail.storage_object.sha256


def test_reader_rejects_thumbnail_that_is_not_the_complete_object() -> None:
    connector = _PresentationConnector(thumbnail_offset=1)
    repository = VNextCatalogReaderRepository(backend="sqlite")

    with pytest.raises(VNextCatalogReadError, match="complete sealed"):
        repository._published_resources_for_artifacts(  # noqa: SLF001
            connector,  # type: ignore[arg-type]  # deterministic repository fake
            revision=3,
            artifacts={connector.publication_key: _artifact(connector)},
        )
