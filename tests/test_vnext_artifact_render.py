from __future__ import annotations

from collections.abc import Buffer, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from errno import ENOSPC
from hashlib import sha256
from io import BytesIO, RawIOBase
from pathlib import Path
from tempfile import TemporaryFile
from threading import Barrier, Lock
from time import sleep
from typing import BinaryIO, cast

import pytest

import h2hdb.vnext_artifact_render as render_module
from h2hdb import (
    ArtifactFailureContext,
    VNextSourceChangedError,
    get_artifact_failure_context,
)
from h2hdb.domain import (
    ArtifactArchiveRenderEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactSourceRole,
    ArtifactStorageEvidence,
    CatalogResourceKind,
    StorageObjectKey,
)
from h2hdb.vnext_artifact_render import (
    ArtifactRenderConflictError,
    ArtifactRenderNotReadyError,
    ArtifactSourceReference,
    RenderedArtifact,
    _BoundedArtifactStream,
    _ReadOnlySlice,
    render_artifact,
    verify_artifact_sources,
)


class _Adapter:
    adapter_id = b"fixture-adapter"
    policy_fingerprint_sha256 = b"p" * 32

    def __init__(self, sources: dict[bytes, bytes | BinaryIO]) -> None:
        self.sources = sources
        self.opened: list[bytes] = []
        self.rendered: list[tuple[int, bytes]] = []

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
        assert source_root_components == ("root",)
        assert gallery_locator_components == ("gallery",)
        self.opened.append(source_name)
        source = self.sources[source_name]
        return BytesIO(source) if isinstance(source, bytes) else source

    def render_archive(
        self,
        members: Iterable[ArtifactSourceMember],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        assert gid == 7
        digest = sha256()
        size = 0
        pages: list[ArtifactRenderedPage] = []
        for member in members:
            payload = member.source.read()
            assert isinstance(payload, bytes)
            self.rendered.append((member.position, payload))
            assert destination.write(payload) == len(payload)
            digest.update(payload)
            size += len(payload)
            if member.role is ArtifactSourceRole.PAGE:
                pages.append(
                    ArtifactRenderedPage(
                        len(pages),
                        member.position,
                        f"opaque-{member.position}",
                    )
                )
        return ArtifactArchiveRenderEvidence(
            digest.digest(),
            size,
            "application/octet-stream",
            "artifact.bin",
            tuple(pages),
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
        del (
            archive,
            storage_key,
            expected_sha256,
            expected_size_bytes,
            modified_at,
            protection_token,
        )
        raise AssertionError("render-only fixture must not protect storage")

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        del archive, thumbnail_destination, rendered_pages
        raise AssertionError("render-only fixture must not render presentation")


def _reference(
    position: int,
    role: ArtifactSourceRole,
    name: bytes,
    payload: bytes,
    *,
    size: int | None = None,
    digest: bytes | None = None,
) -> ArtifactSourceReference:
    return ArtifactSourceReference(
        position,
        role,
        name,
        sha256(payload).digest() if digest is None else digest,
        len(payload) if size is None else size,
    )


def _render(
    adapter: _Adapter,
    references: tuple[ArtifactSourceReference, ...],
) -> RenderedArtifact:
    return render_artifact(
        adapter,
        gid=7,
        source_root_components=("root",),
        gallery_locator_components=("gallery",),
        references=references,
    )


def _verify(
    adapter: _Adapter,
    references: tuple[ArtifactSourceReference, ...],
) -> None:
    verify_artifact_sources(
        adapter,
        gid=7,
        source_root_components=("root",),
        gallery_locator_components=("gallery",),
        references=references,
    )


@pytest.mark.parametrize("cached", [False, True])
def test_source_failure_keeps_type_and_exact_gallery_and_member_context(
    *, cached: bool
) -> None:
    adapter = _Adapter({b"metadata.txt": b"metadata", b"page.jpg": b"changed"})
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),
        _reference(1, ArtifactSourceRole.PAGE, b"page.jpg", b"original"),
    )
    with pytest.raises(VNextSourceChangedError) as caught:
        if cached:
            _verify(adapter, references)
        else:
            _render(adapter, references)
    assert type(caught.value) is VNextSourceChangedError
    assert get_artifact_failure_context(caught.value) == ArtifactFailureContext(
        7, ("root",), ("gallery",), b"page.jpg", len(b"original")
    )


@pytest.mark.parametrize(
    "failure",
    [
        ArtifactRenderConflictError("sealed evidence changed"),
        ArtifactRenderNotReadyError("adapter rejected source"),
        RuntimeError("storage failed"),
        VNextSourceChangedError("source changed"),
    ],
)
def test_renderer_failure_keeps_original_object_and_gallery_context(
    monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    adapter = _Adapter({b"metadata.txt": b"metadata"})

    def fail_render(
        _members: Iterable[ArtifactSourceMember],
        _destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        assert gid == 7
        raise failure

    monkeypatch.setattr(adapter, "render_archive", fail_render)
    with pytest.raises(type(failure)) as caught:
        _render(
            adapter,
            (_reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),),
        )
    assert caught.value is failure
    assert get_artifact_failure_context(caught.value) == ArtifactFailureContext(
        7, ("root",), ("gallery",)
    )


def test_adapter_value_error_retains_cause_and_public_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _Adapter({b"metadata.txt": b"metadata"})
    failure = ValueError("decoder rejected image")

    def fail_render(
        _members: Iterable[ArtifactSourceMember],
        _destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        assert gid == 7
        raise failure

    monkeypatch.setattr(adapter, "render_archive", fail_render)
    with pytest.raises(ArtifactRenderNotReadyError) as caught:
        _render(
            adapter,
            (_reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),),
        )
    assert caught.value.__cause__ is failure
    assert get_artifact_failure_context(caught.value) == ArtifactFailureContext(
        7, ("root",), ("gallery",)
    )


@pytest.mark.parametrize("cached", [False, True])
def test_open_source_failure_reports_the_exact_member_before_rendering(
    monkeypatch: pytest.MonkeyPatch, *, cached: bool
) -> None:
    adapter = _Adapter({})
    original = OSError("source cannot be opened")

    def fail_open(
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        assert source_root_components == ("root",)
        assert gallery_locator_components == ("gallery",)
        assert source_name == b"metadata.txt"
        raise original

    monkeypatch.setattr(adapter, "open_source", fail_open)
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),
    )
    with pytest.raises(ArtifactRenderNotReadyError) as caught:
        if cached:
            _verify(adapter, references)
        else:
            _render(adapter, references)
    assert caught.value.__cause__ is original
    assert get_artifact_failure_context(caught.value) == ArtifactFailureContext(
        7, ("root",), ("gallery",), b"metadata.txt", len(b"metadata")
    )
    assert not adapter.rendered


class _OverlapDetectingSpool(BytesIO):
    """Expose a shared physical seek/read overlap deterministically."""

    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self._state_lock = Lock()
        self._seek_pending = False
        self.overlapped = False

    def seek(self, offset: int, whence: int = 0) -> int:
        with self._state_lock:
            if self._seek_pending:
                self.overlapped = True
            self._seek_pending = True
        sleep(0.02)
        return super().seek(offset, whence)

    def read(self, size: int | None = -1) -> bytes:
        result = super().read(size)
        with self._state_lock:
            self._seek_pending = False
        return result


def test_verified_source_slices_serialize_shared_spool_seek_and_read() -> None:
    first = b"first-extent" * 1_024
    second = b"second-extent" * 1_024
    spool = _OverlapDetectingSpool(first + second)
    spool_lock = Lock()
    slices = (
        _ReadOnlySlice(spool, delegate_lock=spool_lock, offset=0, length=len(first)),
        _ReadOnlySlice(
            spool,
            delegate_lock=spool_lock,
            offset=len(first),
            length=len(second),
        ),
    )
    ready = Barrier(2)

    def consume(source: _ReadOnlySlice) -> bytes:
        ready.wait()
        return source.read()

    with ThreadPoolExecutor(max_workers=2) as executor:
        actual = tuple(executor.map(consume, slices))

    assert actual == (first, second)
    assert not spool.overlapped


def test_render_spools_exact_selected_sources_and_never_opens_other() -> None:
    metadata = b"metadata"
    page = b"page"
    adapter = _Adapter({b"metadata.txt": metadata, b"page.jpg": page})
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", metadata),
        _reference(1, ArtifactSourceRole.PAGE, b"page.jpg", page),
        ArtifactSourceReference(
            2,
            ArtifactSourceRole.OTHER,
            b"irrelevant.bin",
            b"x" * 32,
            33 * 1024 * 1024,
        ),
    )

    with _render(adapter, references) as rendered:
        assert rendered.archive.read() == metadata + page
        assert tuple(page.source_position for page in rendered.evidence.pages) == (1,)

    assert adapter.opened == [b"metadata.txt", b"page.jpg"]
    assert adapter.rendered == [(0, metadata), (1, page)]


def test_source_revalidation_reads_exact_selected_sources_without_rendering() -> None:
    metadata = b"metadata"
    page = b"page"
    adapter = _Adapter({b"metadata.txt": metadata, b"page.jpg": page})
    references = (
        _reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", metadata),
        _reference(1, ArtifactSourceRole.PAGE, b"page.jpg", page),
        ArtifactSourceReference(
            2,
            ArtifactSourceRole.OTHER,
            b"irrelevant.bin",
            b"x" * 32,
            33 * 1024 * 1024,
        ),
    )

    _verify(adapter, references)

    assert adapter.opened == [b"metadata.txt", b"page.jpg"]
    assert adapter.rendered == []


def test_source_revalidation_uses_reference_digest_error_boundary() -> None:
    adapter = _Adapter({b"metadata.txt": b"change"})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"sealed",
    )

    with pytest.raises(VNextSourceChangedError, match="digest differs"):
        _verify(adapter, (reference,))

    assert adapter.rendered == []


def test_render_preserves_sparse_original_positions_with_late_metadata() -> None:
    first_page = b"first"
    metadata = b"metadata"
    second_page = b"second"
    adapter = _Adapter(
        {
            b"first.page": first_page,
            b"gallery.meta": metadata,
            b"second.page": second_page,
        }
    )
    references = (
        _reference(3, ArtifactSourceRole.PAGE, b"first.page", first_page),
        ArtifactSourceReference(
            4,
            ArtifactSourceRole.OTHER,
            b"ignored.bin",
            b"x" * 32,
            33 * 1024 * 1024,
        ),
        _reference(7, ArtifactSourceRole.METADATA, b"gallery.meta", metadata),
        _reference(9, ArtifactSourceRole.PAGE, b"second.page", second_page),
    )

    with _render(adapter, references) as rendered:
        assert rendered.archive.read() == first_page + metadata + second_page
        assert tuple(page.source_position for page in rendered.evidence.pages) == (
            3,
            9,
        )

    assert adapter.opened == [b"first.page", b"gallery.meta", b"second.page"]
    assert adapter.rendered == [
        (3, first_page),
        (7, metadata),
        (9, second_page),
    ]


@pytest.mark.parametrize(
    ("sealed_size", "source", "message"),
    [
        (5, b"tiny", "ended before"),
        (4, b"extra", "beyond its sealed size"),
    ],
)
def test_render_rejects_size_mismatch_and_trailing_byte(
    sealed_size: int,
    source: bytes,
    message: str,
) -> None:
    adapter = _Adapter({b"metadata.txt": source})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        source[:sealed_size],
        size=sealed_size,
    )

    with pytest.raises(VNextSourceChangedError, match=message):
        _render(adapter, (reference,))


def test_render_rejects_digest_mismatch() -> None:
    adapter = _Adapter({b"metadata.txt": b"actual"})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"actual",
        digest=sha256(b"different").digest(),
    )

    with pytest.raises(VNextSourceChangedError, match="digest differs"):
        _render(adapter, (reference,))


def test_render_rejects_page_over_core_member_bound_before_open() -> None:
    metadata = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"",
    )
    pages = tuple(
        _reference(
            position,
            ArtifactSourceRole.PAGE,
            f"p{position}".encode(),
            b"",
        )
        for position in range(1, 4_098)
    )
    adapter = _Adapter({})

    with pytest.raises(ArtifactRenderNotReadyError, match="page bound"):
        _render(adapter, (metadata, *pages))

    assert adapter.opened == []


def test_render_accepts_source_authority_above_four_gib_before_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    references = tuple(
        ArtifactSourceReference(
            position,
            ArtifactSourceRole.METADATA if position == 0 else ArtifactSourceRole.PAGE,
            f"source-{position}".encode(),
            b"x" * 32,
            64 * 1024 * 1024 + 1,
        )
        for position in range(65)
    )
    adapter = _Adapter({})
    failure = VNextSourceChangedError("source no longer exists")

    def changed(**_kwargs: object) -> BinaryIO:
        raise failure

    monkeypatch.setattr(adapter, "open_source", changed)
    with pytest.raises(VNextSourceChangedError) as caught:
        _render(adapter, references)
    assert caught.value is failure


class _RepeatingSource(RawIOBase):
    def __init__(self, size: int) -> None:
        self.remaining = size
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        assert 0 < size <= 64 * 1024
        self.read_sizes.append(size)
        count = min(size, self.remaining)
        self.remaining -= count
        return b"x" * count


def test_render_streams_source_larger_than_64_mib_to_real_disk_and_closes_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    size = 64 * 1024 * 1024 + 1
    digest = sha256()
    for _ in range(1024):
        digest.update(b"x" * (64 * 1024))
    digest.update(b"x")
    source = _RepeatingSource(size)
    adapter = _NonConsumingAdapter({b"metadata.txt": cast(BinaryIO, source)})
    spools: list[BinaryIO] = []

    def temporary(**_kwargs: object) -> BinaryIO:
        spool = TemporaryFile(mode="w+b", dir=tmp_path)
        spools.append(spool)
        return spool

    monkeypatch.setattr(render_module, "TemporaryFile", temporary)
    reference = ArtifactSourceReference(
        0, ArtifactSourceRole.METADATA, b"metadata.txt", digest.digest(), size
    )
    with _render(adapter, (reference,)) as rendered:
        assert rendered.archive.read() == b"rendered"
        assert spools[0].closed
        assert not spools[1].closed
    assert source.remaining == 0
    assert source.closed
    assert max(source.read_sizes) == 64 * 1024
    assert source.read_sizes[-1] == 1  # Exact trailing-byte probe.
    assert all(spool.closed for spool in spools)
    assert not tuple(tmp_path.iterdir())


class _AlternatingSeekableSource(BytesIO):
    """Returns different bytes after seek; core must never seek/re-read it."""

    def __init__(self, first: bytes, second: bytes) -> None:
        super().__init__(first)
        self._second = second
        self._switched = False

    def seek(self, offset: int, whence: int = 0) -> int:
        if not self._switched:
            self._switched = True
            super().seek(0)
            super().truncate(0)
            super().write(self._second)
        return super().seek(offset, whence)


def test_render_uses_one_verified_spool_for_mutating_seekable_source() -> None:
    verified = b"verified"
    source = _AlternatingSeekableSource(verified, b"mutated!")
    adapter = _Adapter({b"metadata.txt": source})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        verified,
    )

    with _render(adapter, (reference,)) as rendered:
        assert rendered.archive.read() == verified

    assert adapter.opened == [b"metadata.txt"]
    assert adapter.rendered == [(0, verified)]
    assert not source._switched


class _WrongEvidenceAdapter(_Adapter):
    def render_archive(
        self,
        members: Iterable[ArtifactSourceMember],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        evidence = super().render_archive(members, destination, gid=gid)
        return ArtifactArchiveRenderEvidence(
            b"z" * 32,
            evidence.size_bytes,
            evidence.media_type,
            evidence.download_name,
            evidence.pages,
        )


def test_render_rehashes_destination_instead_of_trusting_adapter_evidence() -> None:
    adapter = _WrongEvidenceAdapter({b"metadata.txt": b"metadata"})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"metadata",
    )

    with pytest.raises(ArtifactRenderConflictError, match="destination bytes"):
        _render(adapter, (reference,))


class _SourceMutationAdapter(_Adapter):
    def render_archive(
        self,
        members: Iterable[ArtifactSourceMember],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        member = next(iter(members))
        member.source.seek(0)
        member.source.write(b"mutation")
        raise AssertionError("the verified source unexpectedly allowed mutation")


def test_render_exposes_verified_source_spools_through_read_only_facade() -> None:
    adapter = _SourceMutationAdapter({b"metadata.txt": b"metadata"})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"metadata",
    )

    with pytest.raises(ArtifactRenderNotReadyError, match="could not render"):
        _render(adapter, (reference,))


class _NonConsumingAdapter(_Adapter):
    def render_archive(
        self,
        members: Iterable[ArtifactSourceMember],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        del members, gid
        assert destination.write(b"rendered") == len(b"rendered")
        return ArtifactArchiveRenderEvidence(
            sha256(b"rendered").digest(),
            len(b"rendered"),
            "application/octet-stream",
            "artifact.bin",
            (),
        )


def test_render_fully_verifies_sources_before_nonconsuming_renderer_runs() -> None:
    adapter = _NonConsumingAdapter({b"metadata.txt": b"actual"})
    reference = _reference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"sealed",
    )

    with pytest.raises(VNextSourceChangedError, match="digest differs"):
        _render(adapter, (reference,))

    assert adapter.opened == [b"metadata.txt"]
    assert adapter.rendered == []


class _FaultingStream(BytesIO):
    def __init__(
        self,
        payload: bytes = b"",
        *,
        write_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        super().__init__(payload)
        self.write_error = write_error
        self.close_error = close_error
        self.close_count = 0

    def write(self, data: Buffer, /) -> int:
        if self.write_error is not None:
            raise self.write_error
        return super().write(data)

    def close(self) -> None:
        self.close_count += 1
        super().close()
        if self.close_error is not None:
            raise self.close_error


def _metadata_reference() -> tuple[ArtifactSourceReference, ...]:
    return (_reference(0, ArtifactSourceRole.METADATA, b"metadata.txt", b"metadata"),)


def test_stage_enospc_survives_source_and_spool_close_errors_with_member_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError(ENOSPC, "source spool is full")
    source = _FaultingStream(
        b"metadata", close_error=OSError("source close also failed")
    )
    spool = _FaultingStream(
        write_error=failure, close_error=OSError(ENOSPC, "spool flush also failed")
    )
    monkeypatch.setattr(render_module, "TemporaryFile", lambda **_kwargs: spool)
    with pytest.raises(OSError) as caught:
        _render(_Adapter({b"metadata.txt": source}), _metadata_reference())
    assert caught.value is failure
    assert get_artifact_failure_context(failure) == ArtifactFailureContext(
        7, ("root",), ("gallery",), b"metadata.txt", len(b"metadata")
    )
    assert source.closed and spool.closed
    assert source.close_count == spool.close_count == 1
    assert len(failure.__notes__) == 3  # Two cleanup errors and source context.
    assert "source close also failed" in failure.__notes__[0]
    assert any("spool flush also failed" in note for note in failure.__notes__)


def test_archive_creation_failure_releases_staged_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged = _FaultingStream(close_error=OSError("staged close also failed"))
    failure = OSError(ENOSPC, "cannot create archive")
    count = 0

    def temporary(**_kwargs: object) -> BinaryIO:
        nonlocal count
        count += 1
        if count == 2:
            raise failure
        return staged

    monkeypatch.setattr(render_module, "TemporaryFile", temporary)
    with pytest.raises(OSError) as caught:
        _render(_Adapter({b"metadata.txt": b"metadata"}), _metadata_reference())
    assert caught.value is failure
    assert staged.closed
    assert staged.close_count == 1
    assert any("staged close also failed" in note for note in failure.__notes__)


@pytest.mark.parametrize("renderer_fails", [False, True])
def test_render_closes_every_resource_when_source_close_fails(
    monkeypatch: pytest.MonkeyPatch, *, renderer_fails: bool
) -> None:
    stage_error = OSError(ENOSPC, "source close failed")
    staged = _FaultingStream(close_error=stage_error)
    archive = _FaultingStream(close_error=OSError("archive close failed"))
    allocated = iter((staged, archive))
    monkeypatch.setattr(
        render_module, "TemporaryFile", lambda **_kwargs: next(allocated)
    )
    adapter = _Adapter({b"metadata.txt": b"metadata"})
    renderer_error = RuntimeError("rendering failed")

    def failed_render(
        *_args: object, **_kwargs: object
    ) -> ArtifactArchiveRenderEvidence:
        raise renderer_error

    if renderer_fails:
        monkeypatch.setattr(adapter, "render_archive", failed_render)
    expected = renderer_error if renderer_fails else stage_error
    with pytest.raises(type(expected)) as caught:
        _render(adapter, _metadata_reference())
    assert caught.value is expected
    assert staged.closed and archive.closed
    assert staged.close_count == archive.close_count == 1
    assert any("archive close failed" in note for note in expected.__notes__)


def test_failed_slice_close_does_not_skip_other_slices_or_aggregate_spool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spools = (_FaultingStream(), _FaultingStream())
    allocated = iter(spools)
    monkeypatch.setattr(
        render_module, "TemporaryFile", lambda **_kwargs: next(allocated)
    )
    closed: list[_ReadOnlySlice] = []
    original = _ReadOnlySlice.close
    failure = OSError("first slice close failed")

    def close_slice(source: _ReadOnlySlice) -> None:
        original(source)
        closed.append(source)
        if len(closed) == 1:
            raise failure

    monkeypatch.setattr(_ReadOnlySlice, "close", close_slice)
    adapter = _Adapter({b"metadata.txt": b"metadata", b"page": b"page"})
    references = (
        *_metadata_reference(),
        _reference(1, ArtifactSourceRole.PAGE, b"page", b"page"),
    )
    with pytest.raises(OSError) as caught:
        _render(adapter, references)
    assert caught.value is failure
    assert len(closed) == 2
    assert all(source.closed for source in closed)
    assert all(spool.closed and spool.close_count == 1 for spool in spools)


@pytest.mark.parametrize("cached", [False, True])
def test_source_digest_failure_is_not_replaced_by_source_close_failure(
    *, cached: bool
) -> None:
    source = _FaultingStream(b"changed!", close_error=OSError("source close failed"))
    adapter = _Adapter({b"metadata.txt": source})
    with pytest.raises(VNextSourceChangedError, match="digest differs") as caught:
        if cached:
            _verify(adapter, _metadata_reference())
        else:
            _render(adapter, _metadata_reference())
    assert source.closed
    assert any("source close failed" in note for note in caught.value.__notes__)
    assert get_artifact_failure_context(caught.value) == ArtifactFailureContext(
        7, ("root",), ("gallery",), b"metadata.txt", len(b"metadata")
    )


def test_rendered_archive_context_preserves_body_failure_when_close_fails() -> None:
    with _render(
        _Adapter({b"metadata.txt": b"metadata"}), _metadata_reference()
    ) as original:
        evidence = original.evidence
    stream = _FaultingStream(close_error=OSError(ENOSPC, "archive close failed"))
    rendered = RenderedArtifact(archive=stream, evidence=evidence)
    failure = RuntimeError("protect failed")
    with pytest.raises(RuntimeError) as caught, rendered:
        raise failure
    assert caught.value is failure
    assert stream.closed
    assert any("archive close failed" in note for note in failure.__notes__)
    rendered.close()
    assert stream.close_count == 1


def test_adapter_can_inspect_and_rewrite_the_same_bounded_scratch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _Adapter({b"metadata.txt": b"metadata"})
    original_render = adapter.render_archive

    def inspect_render(
        members: Iterable[ArtifactSourceMember], destination: BinaryIO, *, gid: int
    ) -> ArtifactArchiveRenderEvidence:
        evidence = original_render(members, destination, gid=gid)
        assert (
            destination.readable() and destination.seekable() and destination.writable()
        )
        destination.seek(0)
        assert destination.read(4) == b"meta"
        assert destination.read() == b"data"
        destination.seek(0)
        assert destination.write(b"metadata") == len(b"metadata")
        destination.truncate()
        return evidence

    monkeypatch.setattr(adapter, "render_archive", inspect_render)
    with _render(adapter, _metadata_reference()) as rendered:
        assert rendered.archive.read() == b"metadata"


@pytest.mark.parametrize("operation", ["write", "seek", "truncate"])
def test_readable_scratch_retains_output_growth_bounds(operation: str) -> None:
    delegate = BytesIO(b"four")
    scratch = _BoundedArtifactStream(delegate, 4)
    assert scratch.read() == b"four"
    with pytest.raises(ArtifactRenderNotReadyError, match="core resource bound"):
        if operation == "write":
            scratch.write(b"x")
        elif operation == "seek":
            scratch.seek(5)
        else:
            scratch.truncate(5)
    assert delegate.getvalue() == b"four"
