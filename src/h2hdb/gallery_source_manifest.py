import hashlib
from collections.abc import Iterable
from enum import StrEnum
from typing import Protocol

from h2h_galleryinfo_parser import GalleryInfoParser

SOURCE_MANIFEST_VERSION = b"h2hdb-gallery-source-manifest-v1"
CBZ_INPUT_MANIFEST_VERSION = b"h2hdb-cbz-input-manifest-v1"
CBZ_INPUT_MANIFEST_COMMENT_PREFIX = b"h2hdb-cbz-input-manifest-sha256-v1:"


class GalleryChange(StrEnum):
    new = "new"
    changed = "changed"
    unchanged = "unchanged"


class _Hasher(Protocol):
    def update(self, value: bytes) -> None: ...


def _update_length_prefixed(hasher: _Hasher, value: bytes) -> None:
    hasher.update(len(value).to_bytes(8, "big"))
    hasher.update(value)


def build_gallery_source_manifest(galleryinfo_params: GalleryInfoParser) -> bytes:
    """Fingerprint source filenames without rereading any file contents.

    The directory mtime also provides a conservative change marker for
    operations such as swapping two existing filenames, where the final
    filename set is unchanged. Ordinary add/delete/rename operations are
    detected directly by the sorted raw filenames.
    """
    hasher = hashlib.sha256()
    _update_length_prefixed(hasher, SOURCE_MANIFEST_VERSION)
    _update_length_prefixed(
        hasher,
        str(galleryinfo_params.gallery_folder.stat().st_mtime_ns).encode("ascii"),
    )
    for file_name in sorted(path.name for path in galleryinfo_params.files_path):
        _update_length_prefixed(hasher, file_name.encode("utf-8"))
    return hasher.digest()


def build_cbz_input_manifest(files: Iterable[tuple[str, bytes]]) -> bytes:
    """Fingerprint raw source filename-to-content mappings for a CBZ."""
    hasher = hashlib.sha256()
    _update_length_prefixed(hasher, CBZ_INPUT_MANIFEST_VERSION)
    for file_name, content_hash in sorted(files):
        _update_length_prefixed(hasher, file_name.encode("utf-8"))
        _update_length_prefixed(hasher, content_hash)
    return hasher.digest()


def cbz_input_manifest_to_comment(input_manifest: bytes) -> bytes:
    """Encode a CBZ input manifest as self-identifying ZIP metadata."""
    if len(input_manifest) != 32:
        raise ValueError(
            "CBZ input manifest SHA-256 must be exactly 32 bytes, "
            f"got {len(input_manifest)}."
        )
    return CBZ_INPUT_MANIFEST_COMMENT_PREFIX + input_manifest.hex().encode("ascii")
