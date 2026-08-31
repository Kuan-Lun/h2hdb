"""Core verification of adapter-rendered generic page and thumbnail facts."""

from __future__ import annotations

__all__ = [
    "PreparedPresentationArtifact",
    "prepare_presentation",
]

from datetime import datetime
from hashlib import sha256
from io import UnsupportedOperation
from tempfile import TemporaryFile
from typing import BinaryIO, cast

from .domain import (
    ArtifactPagePresentationEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactRenderedPage,
    ByteExtent,
    PreparedPageResource,
    PreparedPublicationPresentation,
    PreparedThumbnailResource,
    StorageObjectDescriptor,
    StorageObjectKey,
)
from .ports import ArtifactStorageAdapter
from .vnext_artifact_render import (
    ArtifactRenderConflictError,
    ArtifactRenderNotReadyError,
)

_COPY_CHUNK_BYTES = 64 * 1024
_MAXIMUM_AUXILIARY_RESOURCE_BYTES = 64 * 1024 * 1024


class PreparedPresentationArtifact:
    """Owned verified thumbnail bytes plus persistable generic presentation."""

    __slots__ = ("_closed", "presentation", "thumbnail")

    def __init__(
        self,
        *,
        presentation: PreparedPublicationPresentation,
        thumbnail: BinaryIO,
    ) -> None:
        self.presentation = presentation
        self.thumbnail = thumbnail
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self.thumbnail.close()

    def __enter__(self) -> PreparedPresentationArtifact:
        if self._closed:
            raise ValueError("prepared presentation artifact is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class _ReadOnlyArchive:
    __slots__ = ("_closed", "_delegate", "_size")

    def __init__(self, delegate: BinaryIO, size: int) -> None:
        self._delegate = delegate
        self._size = size
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, size: int = -1) -> bytes:
        self._require_open()
        result = self._delegate.read(size)
        if not isinstance(result, bytes):
            raise ArtifactRenderConflictError(
                "acquisition archive returned a non-bytes chunk"
            )
        return result

    def seek(self, offset: int, whence: int = 0) -> int:
        self._require_open()
        if whence == 0:
            target = offset
        elif whence == 1:
            target = self._delegate.tell() + offset
        elif whence == 2:
            target = self._size + offset
        else:
            raise ValueError("invalid seek whence")
        if target < 0 or target > self._size:
            raise ValueError("acquisition archive seek is outside the sealed object")
        return self._delegate.seek(target)

    def tell(self) -> int:
        self._require_open()
        return self._delegate.tell()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def write(self, _data: bytes) -> int:
        raise UnsupportedOperation("acquisition archive is read-only")

    def truncate(self, _size: int | None = None) -> int:
        raise UnsupportedOperation("acquisition archive is read-only")

    def close(self) -> None:
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("acquisition archive facade is closed")


class _BoundedThumbnailWriter:
    __slots__ = ("_delegate",)

    def __init__(self, delegate: BinaryIO) -> None:
        self._delegate = delegate

    def write(self, data: bytes | bytearray | memoryview) -> int:
        payload = bytes(data)
        position = self._delegate.tell()
        if position < 0 or len(payload) > _MAXIMUM_AUXILIARY_RESOURCE_BYTES - position:
            raise ArtifactRenderNotReadyError(
                "presentation auxiliary resource exceeds the core byte bound"
            )
        written = self._delegate.write(payload)
        if written != len(payload):
            raise ArtifactRenderConflictError(
                "presentation destination accepted a partial write"
            )
        return written

    def seek(self, offset: int, whence: int = 0) -> int:
        position = self._delegate.seek(offset, whence)
        if position < 0 or position > _MAXIMUM_AUXILIARY_RESOURCE_BYTES:
            raise ArtifactRenderNotReadyError(
                "presentation destination sought outside the core byte bound"
            )
        return position

    def tell(self) -> int:
        return self._delegate.tell()

    def truncate(self, size: int | None = None) -> int:
        target = self._delegate.tell() if size is None else size
        if target < 0 or target > _MAXIMUM_AUXILIARY_RESOURCE_BYTES:
            raise ArtifactRenderNotReadyError(
                "presentation destination exceeds the core byte bound"
            )
        return self._delegate.truncate(target)

    def flush(self) -> None:
        self._delegate.flush()

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True


def prepare_presentation(
    adapter: ArtifactStorageAdapter,
    *,
    archive: BinaryIO,
    acquisition: StorageObjectDescriptor,
    rendered_pages: tuple[ArtifactRenderedPage, ...],
    thumbnail_key: StorageObjectKey | None,
    modified_at: datetime,
) -> PreparedPresentationArtifact:
    """Render bounded auxiliary bytes and independently verify every extent."""

    if not isinstance(acquisition, StorageObjectDescriptor):
        raise TypeError("acquisition must be StorageObjectDescriptor")
    rendered = tuple(rendered_pages)
    if tuple(page.page_index for page in rendered) != tuple(range(len(rendered))):
        raise ArtifactRenderConflictError("rendered page authority is not dense")
    if (thumbnail_key is None) != (not rendered):
        raise ArtifactRenderConflictError(
            "thumbnail key must exist exactly when rendered pages exist"
        )
    expected_archive = (bytes.fromhex(acquisition.sha256), acquisition.size_bytes)
    if _hash_stream(archive) != expected_archive:
        raise ArtifactRenderConflictError(
            "acquisition descriptor disagrees with rendered archive bytes"
        )
    archive.seek(0)
    thumbnail = cast(BinaryIO, TemporaryFile(mode="w+b"))
    owned = True
    try:
        try:
            evidence = adapter.render_presentation(
                cast(BinaryIO, _ReadOnlyArchive(archive, acquisition.size_bytes)),
                cast(BinaryIO, _BoundedThumbnailWriter(thumbnail)),
                rendered_pages=rendered,
            )
        except ArtifactRenderConflictError, ArtifactRenderNotReadyError:
            raise
        except (OSError, ValueError) as error:
            raise ArtifactRenderNotReadyError(
                "artifact adapter could not render presentation resources"
            ) from error
        if not isinstance(evidence, ArtifactPresentationRenderEvidence):
            raise ArtifactRenderConflictError(
                "artifact adapter returned invalid presentation evidence"
            )
        evidence.__post_init__()
        if _hash_stream(archive) != expected_archive:
            raise ArtifactRenderConflictError(
                "artifact adapter changed the acquisition archive"
            )
        pages = _verify_pages(
            archive,
            acquisition=acquisition,
            rendered=rendered,
            evidence=evidence.pages,
        )
        thumbnail_resource = _verify_thumbnail(
            thumbnail,
            thumbnail_key=thumbnail_key,
            modified_at=modified_at,
            evidence=evidence,
        )
        thumbnail.seek(0)
        result = PreparedPresentationArtifact(
            presentation=PreparedPublicationPresentation(
                pages,
                thumbnail_resource,
            ),
            thumbnail=thumbnail,
        )
        owned = False
        return result
    finally:
        if owned:
            thumbnail.close()


def _verify_pages(
    archive: BinaryIO,
    *,
    acquisition: StorageObjectDescriptor,
    rendered: tuple[ArtifactRenderedPage, ...],
    evidence: tuple[ArtifactPagePresentationEvidence, ...],
) -> tuple[PreparedPageResource, ...]:
    if len(evidence) != len(rendered):
        raise ArtifactRenderConflictError(
            "presentation pages do not exactly cover rendered page authority"
        )
    prepared: list[PreparedPageResource] = []
    previous_end = 0
    for authority, page in zip(rendered, evidence, strict=True):
        if page.page_index != authority.page_index or page.locator != authority.locator:
            raise ArtifactRenderConflictError(
                "presentation page does not match its opaque render locator"
            )
        extent = page.extent
        end = extent.offset + extent.length
        if end > acquisition.size_bytes:
            raise ArtifactRenderConflictError(
                "presentation page extent exceeds the acquisition object"
            )
        if extent.offset < previous_end:
            raise ArtifactRenderConflictError(
                "presentation page extents overlap or are out of order"
            )
        digest, size = _hash_extent(archive, extent)
        if digest != page.sha256 or size != extent.length:
            raise ArtifactRenderConflictError(
                "presentation page digest differs from acquisition extent bytes"
            )
        prepared.append(
            PreparedPageResource(
                page.page_index,
                acquisition,
                extent,
                page.media_type,
                digest.hex(),
                page.width,
                page.height,
            )
        )
        previous_end = end
    return tuple(prepared)


def _verify_thumbnail(
    thumbnail: BinaryIO,
    *,
    thumbnail_key: StorageObjectKey | None,
    modified_at: datetime,
    evidence: ArtifactPresentationRenderEvidence,
) -> PreparedThumbnailResource | None:
    digest, size = _hash_stream(thumbnail)
    thumbnail_evidence = evidence.thumbnail
    if not evidence.pages:
        if thumbnail_key is not None or thumbnail_evidence is not None or size != 0:
            raise ArtifactRenderConflictError(
                "empty presentation unexpectedly rendered a thumbnail"
            )
        return None
    if thumbnail_key is None or thumbnail_evidence is None:
        raise ArtifactRenderConflictError(
            "nonempty presentation lacks complete thumbnail authority"
        )
    if (digest, size) != (
        thumbnail_evidence.sha256,
        thumbnail_evidence.size_bytes,
    ):
        raise ArtifactRenderConflictError(
            "thumbnail evidence disagrees with destination bytes"
        )
    descriptor = StorageObjectDescriptor(
        thumbnail_key,
        size,
        digest.hex(),
        modified_at,
    )
    return PreparedThumbnailResource(
        descriptor,
        ByteExtent(0, size),
        thumbnail_evidence.media_type,
        digest.hex(),
        thumbnail_evidence.width,
        thumbnail_evidence.height,
    )


def _hash_extent(stream: BinaryIO, extent: ByteExtent) -> tuple[bytes, int]:
    stream.seek(extent.offset)
    remaining = extent.length
    digest = sha256()
    while remaining:
        part = stream.read(min(remaining, _COPY_CHUNK_BYTES))
        if not isinstance(part, bytes) or not part:
            raise ArtifactRenderConflictError("presentation page extent is truncated")
        if len(part) > remaining:
            raise ArtifactRenderConflictError(
                "presentation page read exceeded its declared extent"
            )
        digest.update(part)
        remaining -= len(part)
    return digest.digest(), extent.length


def _hash_stream(stream: BinaryIO) -> tuple[bytes, int]:
    stream.seek(0)
    digest = sha256()
    count = 0
    while True:
        part = stream.read(_COPY_CHUNK_BYTES)
        if not isinstance(part, bytes):
            raise ArtifactRenderConflictError("resource stream returned non-bytes")
        if not part:
            break
        digest.update(part)
        count += len(part)
    return digest.digest(), count
