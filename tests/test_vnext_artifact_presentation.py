from __future__ import annotations

from datetime import UTC, datetime
from errno import ENOSPC
from hashlib import sha256
from io import BytesIO
from typing import BinaryIO, cast

import pytest

import h2hdb.vnext_artifact_presentation as presentation_module
from h2hdb.domain import (
    ArtifactPagePresentationEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactRenderedPage,
    ArtifactThumbnailPresentationEvidence,
    ByteExtent,
    StorageObjectDescriptor,
    StorageObjectKey,
)
from h2hdb.ports import ArtifactStorageAdapter
from h2hdb.vnext_artifact_preparation_repository import ArtifactPreparationReceipt
from h2hdb.vnext_artifact_presentation import (
    PreparedPresentationArtifact,
    prepare_presentation,
)
from h2hdb.vnext_artifact_render import (
    ArtifactRenderConflictError,
    ArtifactRenderNotReadyError,
)

_MODIFIED = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)


class _Adapter:
    adapter_id = b"fixture-adapter"
    policy_fingerprint_sha256 = b"p" * 32

    def __init__(
        self,
        *,
        pages: tuple[ArtifactPagePresentationEvidence, ...],
        thumbnail: bytes | None,
        thumbnail_digest: bytes | None = None,
    ) -> None:
        self.pages = pages
        self.thumbnail = thumbnail
        self.thumbnail_digest = thumbnail_digest

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        assert archive.read() == _ARCHIVE
        assert tuple(page.locator for page in rendered_pages) == tuple(
            page.locator for page in self.pages
        )
        if self.thumbnail is None:
            evidence = None
        else:
            assert thumbnail_destination.write(self.thumbnail) == len(self.thumbnail)
            evidence = ArtifactThumbnailPresentationEvidence(
                len(self.thumbnail),
                "image/jpeg",
                (
                    sha256(self.thumbnail).digest()
                    if self.thumbnail_digest is None
                    else self.thumbnail_digest
                ),
                200,
                300,
            )
        return ArtifactPresentationRenderEvidence(self.pages, evidence)


_PAGE_0 = b"page-zero"
_PAGE_1 = b"page-one!"
_ARCHIVE = b"head" + _PAGE_0 + b"gap" + _PAGE_1 + b"tail"
_RENDERED = (
    ArtifactRenderedPage(0, 1, "opaque-0"),
    ArtifactRenderedPage(1, 2, "opaque-1"),
)


def _page_evidence(
    page_index: int,
    locator: str,
    offset: int,
    payload: bytes,
    *,
    digest: bytes | None = None,
) -> ArtifactPagePresentationEvidence:
    return ArtifactPagePresentationEvidence(
        page_index,
        locator,
        ByteExtent(offset, len(payload)),
        "image/jpeg",
        sha256(payload).digest() if digest is None else digest,
        1200,
        1600,
    )


def _pages() -> tuple[ArtifactPagePresentationEvidence, ...]:
    return (
        _page_evidence(0, "opaque-0", 4, _PAGE_0),
        _page_evidence(1, "opaque-1", 4 + len(_PAGE_0) + 3, _PAGE_1),
    )


def _acquisition() -> StorageObjectDescriptor:
    return StorageObjectDescriptor(
        StorageObjectKey("fixture-v2", ("acquisition",)),
        len(_ARCHIVE),
        sha256(_ARCHIVE).hexdigest(),
        _MODIFIED,
    )


def test_prepare_presentation_rehashes_pages_and_whole_thumbnail() -> None:
    thumbnail = b"thumbnail"
    adapter = _Adapter(pages=_pages(), thumbnail=thumbnail)

    with prepare_presentation(
        adapter,  # type: ignore[arg-type]
        archive=BytesIO(_ARCHIVE),
        acquisition=_acquisition(),
        rendered_pages=_RENDERED,
        thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
        modified_at=_MODIFIED,
    ) as prepared:
        assert tuple(page.sha256 for page in prepared.presentation.pages) == (
            sha256(_PAGE_0).hexdigest(),
            sha256(_PAGE_1).hexdigest(),
        )
        assert prepared.presentation.pages[0].extent == ByteExtent(4, len(_PAGE_0))
        resource = prepared.presentation.thumbnail
        assert resource is not None
        assert resource.extent == ByteExtent(0, len(thumbnail))
        assert resource.sha256 == resource.storage_object.sha256
        assert resource.extent.length == resource.storage_object.size_bytes
        assert prepared.thumbnail.read() == thumbnail


def test_prepare_presentation_rejects_page_digest_not_backed_by_archive() -> None:
    pages = (
        _page_evidence(
            0,
            "opaque-0",
            4,
            _PAGE_0,
            digest=b"x" * 32,
        ),
        _pages()[1],
    )
    adapter = _Adapter(pages=pages, thumbnail=b"thumbnail")

    with pytest.raises(ArtifactRenderConflictError, match="page digest"):
        prepare_presentation(
            adapter,  # type: ignore[arg-type]
            archive=BytesIO(_ARCHIVE),
            acquisition=_acquisition(),
            rendered_pages=_RENDERED,
            thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
            modified_at=_MODIFIED,
        )


def test_prepare_presentation_rejects_overlapping_page_extents() -> None:
    pages = (
        _pages()[0],
        _page_evidence(1, "opaque-1", 5, _PAGE_1),
    )
    adapter = _Adapter(pages=pages, thumbnail=b"thumbnail")

    with pytest.raises(ArtifactRenderConflictError, match="overlap"):
        prepare_presentation(
            adapter,  # type: ignore[arg-type]
            archive=BytesIO(_ARCHIVE),
            acquisition=_acquisition(),
            rendered_pages=_RENDERED,
            thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
            modified_at=_MODIFIED,
        )


def test_prepare_presentation_rejects_thumbnail_digest_evidence() -> None:
    adapter = _Adapter(
        pages=_pages(),
        thumbnail=b"thumbnail",
        thumbnail_digest=b"x" * 32,
    )

    with pytest.raises(ArtifactRenderConflictError, match="thumbnail evidence"):
        prepare_presentation(
            adapter,  # type: ignore[arg-type]
            archive=BytesIO(_ARCHIVE),
            acquisition=_acquisition(),
            rendered_pages=_RENDERED,
            thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
            modified_at=_MODIFIED,
        )


class _ArchiveMutationAdapter(_Adapter):
    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        del thumbnail_destination, rendered_pages
        archive.write(b"mutation")
        raise AssertionError("read-only acquisition unexpectedly allowed mutation")


def test_prepare_presentation_exposes_read_only_acquisition() -> None:
    adapter = _ArchiveMutationAdapter(pages=_pages(), thumbnail=b"thumbnail")

    with pytest.raises(ArtifactRenderNotReadyError, match="could not render"):
        prepare_presentation(
            adapter,  # type: ignore[arg-type]
            archive=BytesIO(_ARCHIVE),
            acquisition=_acquisition(),
            rendered_pages=_RENDERED,
            thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
            modified_at=_MODIFIED,
        )


def test_prepare_empty_presentation_requires_no_thumbnail_bytes_or_key() -> None:
    adapter = _Adapter(pages=(), thumbnail=None)

    with prepare_presentation(
        adapter,  # type: ignore[arg-type]
        archive=BytesIO(_ARCHIVE),
        acquisition=_acquisition(),
        rendered_pages=(),
        thumbnail_key=None,
        modified_at=_MODIFIED,
    ) as prepared:
        assert prepared.presentation.pages == ()
        assert prepared.presentation.thumbnail is None
        assert prepared.thumbnail.read() == b""


class _CloseFailure(BytesIO):
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1
        super().close()
        raise self.failure


def test_presentation_enospc_keeps_original_error_when_thumbnail_close_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError(ENOSPC, "thumbnail write failed")
    thumbnail = _CloseFailure(OSError(ENOSPC, "thumbnail flush failed"))
    adapter = _Adapter(pages=_pages(), thumbnail=b"thumbnail")

    def fail_render(
        *_args: object, **_kwargs: object
    ) -> ArtifactPresentationRenderEvidence:
        raise failure

    monkeypatch.setattr(adapter, "render_presentation", fail_render)
    monkeypatch.setattr(
        presentation_module, "TemporaryFile", lambda **_kwargs: thumbnail
    )
    archive = BytesIO(_ARCHIVE)
    with pytest.raises(ArtifactRenderNotReadyError) as caught:
        prepare_presentation(
            cast(ArtifactStorageAdapter, adapter),
            archive=archive,
            acquisition=_acquisition(),
            rendered_pages=_RENDERED,
            thumbnail_key=StorageObjectKey("fixture-v2", ("thumbnail",)),
            modified_at=_MODIFIED,
        )
    assert caught.value.__cause__ is failure
    assert any("thumbnail flush failed" in note for note in caught.value.__notes__)
    assert thumbnail.closed and thumbnail.close_count == 1
    assert not archive.closed  # Acquisition ownership remains with the caller.


def test_presentation_creation_failure_leaves_caller_archive_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError(ENOSPC, "cannot create thumbnail")

    def temporary(**_kwargs: object) -> BinaryIO:
        raise failure

    monkeypatch.setattr(presentation_module, "TemporaryFile", temporary)
    archive = BytesIO(_ARCHIVE)
    adapter = _Adapter(pages=(), thumbnail=None)
    with pytest.raises(OSError) as caught:
        prepare_presentation(
            cast(ArtifactStorageAdapter, adapter),
            archive=archive,
            acquisition=_acquisition(),
            rendered_pages=(),
            thumbnail_key=None,
            modified_at=_MODIFIED,
        )
    assert caught.value is failure
    assert not archive.closed


def _empty_presentation(thumbnail: BinaryIO) -> PreparedPresentationArtifact:
    adapter = _Adapter(pages=(), thumbnail=None)
    with prepare_presentation(
        cast(ArtifactStorageAdapter, adapter),
        archive=BytesIO(_ARCHIVE),
        acquisition=_acquisition(),
        rendered_pages=(),
        thumbnail_key=None,
        modified_at=_MODIFIED,
    ) as original:
        presentation = original.presentation
    return PreparedPresentationArtifact(presentation=presentation, thumbnail=thumbnail)


def test_presentation_context_does_not_replace_body_error_with_close_error() -> None:
    thumbnail = _CloseFailure(OSError(ENOSPC, "thumbnail close failed"))
    presentation = _empty_presentation(thumbnail)
    failure = RuntimeError("protection failed")
    with pytest.raises(RuntimeError) as caught, presentation:
        raise failure
    assert caught.value is failure
    assert any("thumbnail close failed" in note for note in failure.__notes__)
    presentation.close()
    assert thumbnail.closed and thumbnail.close_count == 1


@pytest.mark.parametrize("body_fails", [False, True])
def test_preparation_receipt_releases_both_resources_when_each_close_fails(
    *, body_fails: bool
) -> None:
    archive_error = OSError(ENOSPC, "archive close failed")
    archive = _CloseFailure(archive_error)
    thumbnail = _CloseFailure(OSError(ENOSPC, "thumbnail close failed"))
    # The receipt's database authority is irrelevant to its owned-stream cleanup;
    # construct only that ownership state without forging a usable receipt.
    receipt = object.__new__(ArtifactPreparationReceipt)
    receipt._archive = archive
    receipt._presentation_artifact = _empty_presentation(thumbnail)
    receipt._closed = False
    body_error = RuntimeError("storage protection failed")
    expected = body_error if body_fails else archive_error
    with pytest.raises(type(expected)) as caught, receipt:
        if body_fails:
            raise body_error
    assert caught.value is expected
    assert archive.closed and thumbnail.closed
    assert archive.close_count == thumbnail.close_count == 1
    assert any("thumbnail close failed" in note for note in archive_error.__notes__)
    if body_fails:
        assert any("archive close failed" in note for note in body_error.__notes__)
        assert any("thumbnail close failed" in note for note in body_error.__notes__)
    receipt.close()
    assert archive.close_count == thumbnail.close_count == 1
