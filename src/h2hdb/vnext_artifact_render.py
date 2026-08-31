"""Neutral, bounded artifact rendering across the consumer adapter boundary.

Core owns verification and resource bounds.  It never interprets filenames,
image formats, archive layouts, or filesystem paths.  The adapter opens one
sealed source leaf and consumes immutable verified spools synchronously.
"""

from __future__ import annotations

__all__ = [
    "ArtifactRenderConflictError",
    "ArtifactRenderNotReadyError",
    "ArtifactSourceReference",
    "RenderedArtifact",
    "render_artifact",
]

from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from io import UnsupportedOperation
from tempfile import TemporaryFile
from typing import BinaryIO, cast

from .domain import (
    ArtifactArchiveRenderEvidence,
    ArtifactSourceMember,
    ArtifactSourceRole,
)
from .ports import ArtifactStorageAdapter
from .vnext_domains import (
    INT63_MAX,
    require_digest32,
    require_int63,
    require_positive_int63,
)

_COPY_CHUNK_BYTES = 64 * 1024
_MAXIMUM_SOURCE_MEMBERS = 65_536
_MAXIMUM_RENDERED_PAGES = 4_096
_MAXIMUM_SELECTED_MEMBER_BYTES = 64 * 1024 * 1024
_MAXIMUM_SELECTED_SOURCE_BYTES = (1 << 32) - 1
_MAXIMUM_RENDERED_ARTIFACT_BYTES = (1 << 32) - 1


class ArtifactRenderNotReadyError(RuntimeError):
    """The adapter cannot safely render the sealed source observation."""


class ArtifactRenderConflictError(RuntimeError):
    """Source bytes or adapter evidence disagree with sealed authority."""


@dataclass(frozen=True, slots=True)
class ArtifactSourceReference:
    """One bounded sealed observation fact; no path or image semantics."""

    position: int
    role: ArtifactSourceRole
    source_name: bytes
    expected_sha256: bytes
    expected_size_bytes: int

    def __post_init__(self) -> None:
        require_int63(self.position, field="artifact source reference position")
        if not isinstance(self.role, ArtifactSourceRole):
            raise TypeError("artifact source reference role is invalid")
        # ArtifactSourceMember performs the canonical filename validation.  A
        # closed in-memory stream avoids weakening that shared public domain.
        from io import BytesIO

        ArtifactSourceMember(
            self.position,
            self.role,
            self.source_name,
            self.expected_sha256,
            self.expected_size_bytes,
            BytesIO(),
        )


class RenderedArtifact:
    """Owned verified artifact bytes and exact untrusted-adapter evidence."""

    __slots__ = ("_closed", "archive", "evidence")

    def __init__(
        self,
        *,
        archive: BinaryIO,
        evidence: ArtifactArchiveRenderEvidence,
    ) -> None:
        self.archive = archive
        self.evidence = evidence
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self.archive.close()

    def __enter__(self) -> RenderedArtifact:
        if self._closed:
            raise ValueError("rendered artifact is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def detach_archive(self) -> BinaryIO:
        """Transfer the verified archive stream to the next core-owned stage."""

        if self._closed:
            raise ValueError("rendered artifact is closed")
        self._closed = True
        return self.archive


class _BoundedRandomAccessWriter:
    """Seekable delegate that prevents sparse or sequential growth past a cap."""

    __slots__ = ("_delegate", "_maximum")

    def __init__(self, delegate: BinaryIO, maximum: int) -> None:
        self._delegate = delegate
        self._maximum = require_positive_int63(
            maximum,
            field="bounded artifact destination maximum",
        )

    def write(self, data: bytes | bytearray | memoryview) -> int:
        payload = bytes(data)
        position = self._delegate.tell()
        if position < 0 or len(payload) > self._maximum - position:
            raise ArtifactRenderNotReadyError(
                "rendered artifact exceeds the core resource bound"
            )
        written = self._delegate.write(payload)
        if written != len(payload):
            raise ArtifactRenderConflictError(
                "artifact renderer destination accepted a partial write"
            )
        return written

    def seek(self, offset: int, whence: int = 0) -> int:
        position = self._delegate.seek(offset, whence)
        if position < 0 or position > self._maximum:
            raise ArtifactRenderNotReadyError(
                "artifact renderer sought outside the core resource bound"
            )
        return position

    def truncate(self, size: int | None = None) -> int:
        target = self._delegate.tell() if size is None else size
        if target < 0 or target > self._maximum:
            raise ArtifactRenderNotReadyError(
                "artifact renderer truncated outside the core resource bound"
            )
        return self._delegate.truncate(target)

    def tell(self) -> int:
        return self._delegate.tell()

    def flush(self) -> None:
        self._delegate.flush()

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False


class _ReadOnlySlice:
    """Read/seek facade for one immutable extent in an aggregate verified spool."""

    __slots__ = ("_closed", "_delegate", "_length", "_offset", "_position")

    def __init__(self, delegate: BinaryIO, *, offset: int, length: int) -> None:
        self._delegate = delegate
        self._offset = require_int63(offset, field="verified source spool offset")
        self._length = require_int63(length, field="verified source spool length")
        self._position = 0
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, size: int = -1) -> bytes:
        self._require_open()
        remaining = self._length - self._position
        requested = remaining if size < 0 else min(size, remaining)
        self._delegate.seek(self._offset + self._position)
        result = self._delegate.read(requested)
        if not isinstance(result, bytes):
            raise ArtifactRenderConflictError(
                "verified artifact spool returned a non-bytes chunk"
            )
        if len(result) != requested:
            raise ArtifactRenderConflictError(
                "verified artifact spool ended within a sealed extent"
            )
        self._position += len(result)
        return result

    def seek(self, offset: int, whence: int = 0) -> int:
        self._require_open()
        if whence == 0:
            position = offset
        elif whence == 1:
            position = self._position + offset
        elif whence == 2:
            position = self._length + offset
        else:
            raise ValueError("invalid seek whence")
        if position < 0 or position > self._length:
            raise ValueError("verified artifact source seek is outside its extent")
        self._position = position
        return position

    def tell(self) -> int:
        self._require_open()
        return self._position

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def write(self, _data: bytes) -> int:
        raise UnsupportedOperation("verified artifact source is read-only")

    def truncate(self, _size: int | None = None) -> int:
        raise UnsupportedOperation("verified artifact source is read-only")

    def close(self) -> None:
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("verified artifact source facade is closed")


def render_artifact(
    adapter: ArtifactStorageAdapter,
    *,
    gid: int,
    source_root_components: tuple[str, ...],
    gallery_locator_components: tuple[str, ...],
    references: Iterable[ArtifactSourceReference],
) -> RenderedArtifact:
    """Verify selected sources once, render bounded bytes, and check evidence."""

    exact_gid = require_positive_int63(gid, field="artifact render gid")
    source_rows = tuple(references)
    selected, page_positions = _preflight_references(source_rows)
    staged = _stage_verified_members(
        adapter,
        selected,
        source_root_components=source_root_components,
        gallery_locator_components=gallery_locator_components,
    )
    archive = cast(BinaryIO, TemporaryFile(mode="w+b"))
    archive_owned = True
    try:
        destination = cast(
            BinaryIO,
            _BoundedRandomAccessWriter(
                archive,
                _MAXIMUM_RENDERED_ARTIFACT_BYTES,
            ),
        )
        try:
            evidence = adapter.render_archive(
                staged.members,
                destination,
                gid=exact_gid,
            )
        except ArtifactRenderConflictError, ArtifactRenderNotReadyError:
            raise
        except (OSError, ValueError) as error:
            raise ArtifactRenderNotReadyError(
                "artifact adapter could not render the sealed sources"
            ) from error
        if not isinstance(evidence, ArtifactArchiveRenderEvidence):
            raise ArtifactRenderConflictError(
                "artifact adapter returned an invalid render evidence type"
            )
        evidence.__post_init__()
        staged.verify_unchanged()
        actual_digest, actual_size = _hash_exact_stream(archive)
        if (evidence.artifact_sha256, evidence.size_bytes) != (
            actual_digest,
            actual_size,
        ):
            raise ArtifactRenderConflictError(
                "artifact render evidence disagrees with destination bytes"
            )
        if tuple(page.source_position for page in evidence.pages) != page_positions:
            raise ArtifactRenderConflictError(
                "rendered pages do not exactly cover sealed PAGE sources"
            )
        archive.seek(0)
        result = RenderedArtifact(archive=archive, evidence=evidence)
        archive_owned = False
        return result
    finally:
        staged.close()
        if archive_owned:
            archive.close()


def _preflight_references(
    rows: tuple[ArtifactSourceReference, ...],
) -> tuple[tuple[ArtifactSourceReference, ...], tuple[int, ...]]:
    if len(rows) > _MAXIMUM_SOURCE_MEMBERS:
        raise ArtifactRenderNotReadyError(
            "artifact source observation exceeds the core member bound"
        )
    positions = tuple(row.position for row in rows)
    if positions != tuple(sorted(set(positions))):
        raise ArtifactRenderConflictError(
            "artifact source positions must be strictly increasing"
        )
    metadata = tuple(row for row in rows if row.role is ArtifactSourceRole.METADATA)
    pages = tuple(row for row in rows if row.role is ArtifactSourceRole.PAGE)
    if len(metadata) != 1:
        raise ArtifactRenderConflictError(
            "artifact source observation must contain exactly one METADATA member"
        )
    if len(pages) > _MAXIMUM_RENDERED_PAGES:
        raise ArtifactRenderNotReadyError(
            "artifact source observation exceeds the core page bound"
        )
    selected_positions = {metadata[0].position, *(row.position for row in pages)}
    selected = tuple(row for row in rows if row.position in selected_positions)
    aggregate = 0
    for row in selected:
        size = require_int63(
            row.expected_size_bytes,
            field="selected artifact source size",
        )
        if size > _MAXIMUM_SELECTED_MEMBER_BYTES:
            raise ArtifactRenderNotReadyError(
                "selected artifact source exceeds the core member byte bound"
            )
        if aggregate > _MAXIMUM_SELECTED_SOURCE_BYTES - size:
            raise ArtifactRenderNotReadyError(
                "selected artifact sources exceed the core aggregate byte bound"
            )
        aggregate += size
    return selected, tuple(row.position for row in pages)


class _StagedMembers:
    __slots__ = ("_closed", "_rows", "_spool", "members")

    def __init__(
        self,
        *,
        spool: BinaryIO,
        rows: tuple[ArtifactSourceReference, ...],
        members: tuple[ArtifactSourceMember, ...],
    ) -> None:
        self._spool = spool
        self._rows = rows
        self.members = members
        self._closed = False

    def verify_unchanged(self) -> None:
        offset = 0
        for row in self._rows:
            actual = _hash_extent(self._spool, offset, row.expected_size_bytes)
            if actual != (row.expected_sha256, row.expected_size_bytes):
                raise ArtifactRenderConflictError(
                    "artifact renderer changed a verified source spool"
                )
            offset += row.expected_size_bytes

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            for member in self.members:
                member.source.close()
            self._spool.close()


def _stage_verified_members(
    adapter: ArtifactStorageAdapter,
    rows: tuple[ArtifactSourceReference, ...],
    *,
    source_root_components: tuple[str, ...],
    gallery_locator_components: tuple[str, ...],
) -> _StagedMembers:
    spool = cast(BinaryIO, TemporaryFile(mode="w+b"))
    members: list[ArtifactSourceMember] = []
    owned = True
    try:
        offset = 0
        for row in rows:
            try:
                source = adapter.open_source(
                    source_root_components=source_root_components,
                    gallery_locator_components=gallery_locator_components,
                    source_name=row.source_name,
                )
            except (OSError, RuntimeError, ValueError) as error:
                raise ArtifactRenderNotReadyError(
                    "artifact adapter could not open a sealed source member"
                ) from error
            try:
                _copy_verified_source(row, source, spool)
            finally:
                source.close()
            read_only = cast(
                BinaryIO,
                _ReadOnlySlice(
                    spool,
                    offset=offset,
                    length=row.expected_size_bytes,
                ),
            )
            members.append(
                ArtifactSourceMember(
                    row.position,
                    row.role,
                    row.source_name,
                    row.expected_sha256,
                    row.expected_size_bytes,
                    read_only,
                )
            )
            offset += row.expected_size_bytes
        staged = _StagedMembers(
            spool=spool,
            rows=rows,
            members=tuple(members),
        )
        owned = False
        return staged
    finally:
        if owned:
            for member in members:
                member.source.close()
            spool.close()


def _copy_verified_source(
    row: ArtifactSourceReference,
    source: BinaryIO,
    destination: BinaryIO,
) -> None:
    remaining = row.expected_size_bytes
    digest = sha256()
    while remaining:
        part = source.read(min(remaining, _COPY_CHUNK_BYTES))
        if not isinstance(part, bytes):
            raise ArtifactRenderConflictError(
                "artifact source returned a non-bytes chunk"
            )
        if not part:
            raise ArtifactRenderConflictError(
                "artifact source ended before its sealed size"
            )
        if len(part) > remaining:
            raise ArtifactRenderConflictError(
                "artifact source read exceeded its sealed size"
            )
        written = destination.write(part)
        if written != len(part):
            raise ArtifactRenderConflictError(
                "artifact source spool accepted a partial write"
            )
        digest.update(part)
        remaining -= len(part)
    trailing = source.read(1)
    if not isinstance(trailing, bytes):
        raise ArtifactRenderConflictError(
            "artifact source returned a non-bytes EOF probe"
        )
    if trailing:
        raise ArtifactRenderConflictError(
            "artifact source contains bytes beyond its sealed size"
        )
    if digest.digest() != require_digest32(
        row.expected_sha256,
        field="artifact source expected digest",
    ):
        raise ArtifactRenderConflictError(
            "artifact source digest differs from sealed authority"
        )


def _hash_extent(stream: BinaryIO, offset: int, length: int) -> tuple[bytes, int]:
    stream.seek(offset)
    remaining = length
    digest = sha256()
    while remaining:
        part = stream.read(min(remaining, _COPY_CHUNK_BYTES))
        if not isinstance(part, bytes) or not part:
            raise ArtifactRenderConflictError(
                "verified artifact spool extent is truncated"
            )
        digest.update(part)
        remaining -= len(part)
    return digest.digest(), length


def _hash_exact_stream(stream: BinaryIO) -> tuple[bytes, int]:
    stream.seek(0)
    digest = sha256()
    count = 0
    while True:
        part = stream.read(_COPY_CHUNK_BYTES)
        if not isinstance(part, bytes):
            raise ArtifactRenderConflictError(
                "rendered artifact returned a non-bytes chunk"
            )
        if not part:
            break
        if count > INT63_MAX - len(part):
            raise ArtifactRenderNotReadyError("rendered artifact size exceeds int63")
        count += len(part)
        digest.update(part)
    return digest.digest(), count
