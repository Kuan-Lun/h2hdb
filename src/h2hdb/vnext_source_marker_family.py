"""Immutable completion-marker facts below staging and workflow repositories."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from .domain import (
    ArtifactSourceRole,
    FileObservation,
    VNextSourceCompletionMarker,
    _file_content_receipt_from_frozen_facts,
)
from .source_errors import VNextSourceChangedError
from .vnext_canonical_value_family import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
)
from .vnext_canonical_value_repository import (
    load_and_validate_single_page_canonical_values,
    stream_and_validate_canonical_value,
)
from .vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
)
from .vnext_identity import (
    GalleryObservationDescriptor,
    decode_gallery_observation_descriptor,
    file_key,
    gallery_observation_descriptor_digest,
)

_BINDING = "catalog_gallery_observation_completion_marker"


class SourceMarkerConflictError(RuntimeError):
    """A retained marker or observation disagrees with its sealed authority."""


class SourceMarkerUnavailableError(VNextSourceChangedError):
    """A previously issued cached observation was evicted; prepare it again."""


@dataclass(frozen=True, slots=True)
class CachedSourceObservation:
    gallery_id: int
    observation_id: int
    observation_identity_sha256: bytes
    descriptor: GalleryObservationDescriptor
    file_count: int
    byte_count: int
    marker: VNextSourceCompletionMarker

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="cached gallery_id")
        require_positive_int63(self.observation_id, field="cached observation_id")
        require_digest32(self.observation_identity_sha256, field="cached identity")
        require_int63(self.file_count, field="cached file_count")
        require_int63(self.byte_count, field="cached byte_count")
        if type(self.descriptor) is not GalleryObservationDescriptor:
            raise TypeError(
                "cached descriptor must be an exact GalleryObservationDescriptor"
            )
        if type(self.marker) is not VNextSourceCompletionMarker:
            raise TypeError(
                "cached marker must be an exact VNextSourceCompletionMarker"
            )
        self.descriptor.__post_init__()
        self.marker.__post_init__()


def _load_cached(
    connector: Any,
    gallery_id: int,
    observation_id: int,
    *,
    marker_key: bytes | None = None,
) -> CachedSourceObservation | None:
    gallery = require_positive_int63(gallery_id, field="marker gallery_id")
    observation = require_positive_int63(observation_id, field="marker observation_id")
    if marker_key is None:
        bound = connector.fetch_one(
            f"SELECT file_key FROM {_BINDING} WHERE gallery_id = %s AND observation_id = %s",
            (gallery, observation),
        )
        if not bound:
            return None
        marker_key = require_digest32(bound[0], field="marker file_key")
    return _load_cached_batch(
        connector, bindings=((gallery, observation, marker_key),)
    )[(gallery, observation)]


def _load_cached_batch(
    connector: Any, *, bindings: tuple[tuple[int, int, bytes], ...]
) -> dict[tuple[int, int], CachedSourceObservation]:
    """Reload scalar authority for at most 128 immutable bindings in bulk."""

    if type(bindings) is not tuple or len(bindings) > 128:
        raise ValueError("source marker binding batch is limited to 128")
    if not bindings:
        return {}
    expected = {
        (
            require_positive_int63(gallery, field="marker gallery_id"),
            require_positive_int63(observation, field="marker observation_id"),
        ): require_digest32(key, field="marker file_key")
        for gallery, observation, key in bindings
    }
    if len(expected) != len(bindings):
        raise SourceMarkerConflictError("source marker binding batch repeats identity")
    selectors = " OR ".join(
        "(file.gallery_id = %s AND file.observation_id = %s AND file.file_key = %s)"
        for _binding in bindings
    )
    rows = connector.fetch_all(
        "SELECT observation.gallery_id, observation.observation_id, "
        "observation.observation_identity_sha256, stat.file_count, stat.byte_count, "
        "scan.scan_observation_version, name.name_bytes, content.file_sha256, content_blob.size_bytes, "
        "fs.device, fs.inode, fs.modified_ns, fs.changed_ns, role.artifact_role "
        "FROM catalog_gallery_observation_file_seals AS file "
        "LEFT JOIN catalog_gallery_observations AS observation "
        "ON observation.gallery_id = file.gallery_id "
        "AND observation.observation_id = file.observation_id "
        "LEFT JOIN catalog_gallery_observation_stat AS stat ON stat.gallery_id = observation.gallery_id "
        "AND stat.observation_id = observation.observation_id "
        "LEFT JOIN catalog_gallery_observation_scans AS scan ON scan.gallery_id = observation.gallery_id "
        "AND scan.observation_id = observation.observation_id "
        "LEFT JOIN catalog_gallery_observation_file_file_sha256s AS content ON content.gallery_id = file.gallery_id "
        "AND content.observation_id = file.observation_id AND content.file_key = file.file_key "
        "LEFT JOIN catalog_file_name_identities AS name ON name.file_key = file.file_key "
        "LEFT JOIN catalog_content_blobs AS content_blob ON content_blob.file_sha256 = content.file_sha256 "
        "LEFT JOIN catalog_gallery_observation_file_filesystem AS fs ON fs.gallery_id = file.gallery_id "
        "AND fs.observation_id = file.observation_id AND fs.file_key = file.file_key "
        "LEFT JOIN catalog_gallery_observation_file_artifact_role AS role ON role.gallery_id = file.gallery_id "
        "AND role.observation_id = file.observation_id AND role.file_key = file.file_key "
        f"WHERE {selectors}",
        tuple(value for binding in bindings for value in binding),
    )
    if len(rows) != len(expected) or any(
        len(row) != 14 or any(value is None for value in row) for row in rows
    ):
        raise SourceMarkerConflictError(
            "completion marker lacks its sealed observation facts"
        )
    root_selectors = " OR ".join(
        "(root.gallery_id = %s AND root.observation_id = %s)" for _binding in bindings
    )
    root_rows = connector.fetch_all(
        "SELECT root.gallery_id, root.observation_id, component.component, "
        "root.root_page_sha256, count.subtree_item_count "
        "FROM catalog_gallery_observation_tree_roots AS root "
        "JOIN catalog_gallery_observation_page_descriptor_seals AS page "
        "ON page.page_sha256 = root.root_page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_components AS component "
        "ON component.page_sha256 = page.page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_subtree_item_counts AS count "
        "ON count.page_sha256 = page.page_sha256 "
        f"WHERE {root_selectors} LIMIT 513",
        tuple(
            value
            for gallery, observation, _key in bindings
            for value in (gallery, observation)
        ),
    )
    roots: dict[tuple[int, int], set[tuple[bytes, bytes, int]]] = {}
    for gallery, observation, component, root, count in root_rows:
        roots.setdefault((gallery, observation), set()).add((component, root, count))
    try:
        descriptors = load_and_validate_single_page_canonical_values(
            connector,
            references=tuple((row[2], b"gallery_observation_v1") for row in rows),
        )
        result: dict[tuple[int, int], CachedSourceObservation] = {}
        for row in rows:
            gallery, observation = row[:2]
            key = (gallery, observation)
            if key not in expected or key in result:
                raise SourceMarkerConflictError(
                    "source marker returned an unexpected observation"
                )
            payload = descriptors.get((row[2], b"gallery_observation_v1"))
            if payload is None or len(payload) != 164:
                raise SourceMarkerConflictError(
                    "cached observation descriptor is unsealed"
                )
            descriptor = decode_gallery_observation_descriptor(payload)
            content = _file_content_receipt_from_frozen_facts(row[7], row[8])
            marker = VNextSourceCompletionMarker(
                FileObservation(
                    row[6],
                    content,
                    ArtifactSourceRole(row[13].decode("ascii")),
                    _stat_integer(row[9]),
                    _stat_integer(row[10]),
                    _stat_integer(row[11], signed=True),
                    _stat_integer(row[12], signed=True),
                ),
                row[5],
            )
            if file_key(marker.file.name_bytes) != expected[key]:
                raise SourceMarkerConflictError(
                    "completion marker file name identity differs"
                )
            expected_roots = {
                (
                    b"METADATA",
                    descriptor.metadata_root_sha256,
                    descriptor.metadata_byte_count,
                ),
                (
                    b"FILE",
                    descriptor.file_root_sha256,
                    descriptor.file_item_count,
                ),
                (
                    b"TAG",
                    descriptor.tag_root_sha256,
                    descriptor.tag_item_count,
                ),
                (
                    b"DIRECTORY",
                    descriptor.directory_root_sha256,
                    descriptor.directory_item_count,
                ),
            }
            if (
                roots.get(key) != expected_roots
                or gallery_observation_descriptor_digest(descriptor) != row[2]
                or row[3] != descriptor.file_item_count
            ):
                raise SourceMarkerConflictError(
                    "cached observation typed root/count authority differs"
                )
            result[key] = CachedSourceObservation(
                gallery, observation, row[2], descriptor, row[3], row[4], marker
            )
        return result
    except (
        TypeError,
        ValueError,
        OverflowError,
        CanonicalValueCollisionError,
        CanonicalValueNotReadyError,
    ) as error:
        raise SourceMarkerConflictError(
            "cached completion marker facts are malformed"
        ) from error


def _stat_integer(value: object, *, signed: bool = False) -> int:
    return int.from_bytes(
        require_bounded_bytes(value, field="marker stat scalar", minimum=8, maximum=8),
        "big",
        signed=signed,
    )


def _compare_canonical(
    connector: Any, digest: bytes, domain: bytes, parts: Iterable[bytes]
) -> None:
    expected: Iterator[bytes] = iter(parts)
    pending = b""

    def compare(chunk: bytes) -> None:
        nonlocal pending
        offset = 0
        while offset < len(chunk):
            while not pending:
                pending = next(expected, b"")
                if not pending:
                    raise SourceMarkerConflictError(
                        "source canonical preimage exceeds the supplied identity"
                    )
            size = min(len(pending), len(chunk) - offset)
            if pending[:size] != chunk[offset : offset + size]:
                raise SourceMarkerConflictError("source canonical preimage differs")
            pending = pending[size:]
            offset += size

    receipt = stream_and_validate_canonical_value(
        connector, value_sha256=digest, consume_provisional=compare
    )
    if receipt.digest_domain != domain or pending or next(expected, b""):
        raise SourceMarkerConflictError("source canonical domain or exact EOF differs")


def bind_completion_marker(
    connector: Any,
    *,
    build_id: bytes,
    gallery_id: int,
    observation_id: int,
    observation_identity_sha256: bytes,
    marker: VNextSourceCompletionMarker,
) -> None:
    """Write at the final child stage after the caller locked and sealed its build."""
    if connector.fetch_one(
        "SELECT observation_id FROM catalog_source_build_galleries WHERE build_id = %s AND gallery_id = %s",
        (build_id, gallery_id),
    ) != (observation_id,):
        raise SourceMarkerConflictError("marker seal lacks its exact build link")
    key = file_key(marker.file.name_bytes)
    cached = _load_cached(connector, gallery_id, observation_id, marker_key=key)
    if (
        cached is None
        or cached.marker != marker
        or cached.observation_identity_sha256 != observation_identity_sha256
    ):
        raise SourceMarkerConflictError(
            "completion marker differs from the sealed observation"
        )
    existing = connector.fetch_one(
        f"SELECT file_key FROM {_BINDING} WHERE gallery_id = %s AND observation_id = %s",
        (gallery_id, observation_id),
    )
    if existing:
        if existing != (key,):
            raise SourceMarkerConflictError(
                "immutable completion marker binding differs"
            )
        return
    connector.execute(
        f"INSERT INTO {_BINDING} (gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
        (gallery_id, observation_id, key),
    )
