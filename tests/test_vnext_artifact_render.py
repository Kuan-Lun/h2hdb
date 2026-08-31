from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from hashlib import sha256
from io import BytesIO
from typing import BinaryIO

import pytest

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
    render_artifact,
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

    with pytest.raises(ArtifactRenderConflictError, match=message):
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

    with pytest.raises(ArtifactRenderConflictError, match="digest differs"):
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


def test_render_rejects_selected_member_byte_bound_before_open() -> None:
    reference = ArtifactSourceReference(
        0,
        ArtifactSourceRole.METADATA,
        b"metadata.txt",
        b"x" * 32,
        64 * 1024 * 1024 + 1,
    )
    adapter = _Adapter({})

    with pytest.raises(ArtifactRenderNotReadyError, match="member byte bound"):
        _render(adapter, (reference,))

    assert adapter.opened == []


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

    with pytest.raises(ArtifactRenderConflictError, match="digest differs"):
        _render(adapter, (reference,))

    assert adapter.opened == [b"metadata.txt"]
    assert adapter.rendered == []
