"""Bounded immutable observation receipts independent of a completion marker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .domain import VNextSourceCompletionMarker
from .vnext_canonical_value_repository import (
    load_and_validate_single_page_canonical_values,
)
from .vnext_domains import require_digest32, require_int63, require_positive_int63
from .vnext_identity import (
    GalleryObservationDescriptor,
    decode_gallery_observation_descriptor,
    gallery_observation_descriptor_digest,
)
from .vnext_source_marker_family import (
    CachedSourceObservation,
    SourceMarkerConflictError,
    SourceMarkerUnavailableError,
    _load_cached,
)


@dataclass(frozen=True, slots=True)
class SealedSourceObservation:
    """Exact sealed authority; an absent marker does not authorize external reuse."""

    gallery_id: int
    observation_id: int
    observation_identity_sha256: bytes
    descriptor: GalleryObservationDescriptor
    file_count: int
    byte_count: int
    marker: VNextSourceCompletionMarker | None

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="sealed gallery_id")
        require_positive_int63(self.observation_id, field="sealed observation_id")
        require_digest32(self.observation_identity_sha256, field="sealed identity")
        require_int63(self.file_count, field="sealed file_count")
        require_int63(self.byte_count, field="sealed byte_count")
        if type(self.descriptor) is not GalleryObservationDescriptor:
            raise TypeError("sealed descriptor must be GalleryObservationDescriptor")
        self.descriptor.__post_init__()
        if self.marker is not None:
            if type(self.marker) is not VNextSourceCompletionMarker:
                raise TypeError("sealed marker must be VNextSourceCompletionMarker")
            self.marker.__post_init__()

    @classmethod
    def from_cached(cls, value: CachedSourceObservation) -> SealedSourceObservation:
        value.__post_init__()
        return cls(
            value.gallery_id,
            value.observation_id,
            value.observation_identity_sha256,
            value.descriptor,
            value.file_count,
            value.byte_count,
            value.marker,
        )


def load_sealed_source_observation(
    connector: Any,
    *,
    gallery_id: int,
    observation_id: int,
) -> SealedSourceObservation:
    """Read one descriptor, four typed roots and optional bounded marker facts."""
    gallery = require_positive_int63(gallery_id, field="gallery_id")
    observation = require_positive_int63(observation_id, field="observation_id")
    cached = _load_cached(connector, gallery, observation)
    if cached is not None:
        return SealedSourceObservation.from_cached(cached)
    row = connector.fetch_one(
        "SELECT observation.observation_identity_sha256, stat.file_count, stat.byte_count "
        "FROM catalog_gallery_observations AS observation "
        "JOIN catalog_gallery_observation_stat AS stat ON stat.gallery_id = observation.gallery_id "
        "AND stat.observation_id = observation.observation_id "
        "WHERE observation.gallery_id = %s AND observation.observation_id = %s",
        (gallery, observation),
    )
    if not row:
        raise SourceMarkerUnavailableError(
            "sealed source observation is no longer retained"
        )
    if len(row) != 3:
        raise SourceMarkerConflictError(
            "sealed source observation scalar authority differs"
        )
    payloads = load_and_validate_single_page_canonical_values(
        connector,
        references=((row[0], b"gallery_observation_v1"),),
    )
    payload = payloads.get((row[0], b"gallery_observation_v1"))
    if payload is None or len(payload) != 164:
        raise SourceMarkerConflictError("sealed observation descriptor is absent")
    descriptor = decode_gallery_observation_descriptor(payload)
    roots = connector.fetch_all(
        "SELECT component.component, root.root_page_sha256, count.subtree_item_count "
        "FROM catalog_gallery_observation_tree_roots AS root "
        "JOIN catalog_gallery_observation_page_descriptor_seals AS page "
        "ON page.page_sha256 = root.root_page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_components AS component "
        "ON component.page_sha256 = page.page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_subtree_item_counts AS count "
        "ON count.page_sha256 = page.page_sha256 "
        "WHERE root.gallery_id = %s AND root.observation_id = %s LIMIT 5",
        (gallery, observation),
    )
    expected = {
        (b"METADATA", descriptor.metadata_root_sha256, descriptor.metadata_byte_count),
        (b"FILE", descriptor.file_root_sha256, descriptor.file_item_count),
        (b"TAG", descriptor.tag_root_sha256, descriptor.tag_item_count),
        (
            b"DIRECTORY",
            descriptor.directory_root_sha256,
            descriptor.directory_item_count,
        ),
    }
    if (
        len(roots) != 4
        or set(roots) != expected
        or gallery_observation_descriptor_digest(descriptor) != row[0]
        or descriptor.file_item_count != row[1]
    ):
        raise SourceMarkerConflictError(
            "sealed observation typed roots or counts differ"
        )
    return SealedSourceObservation(
        gallery, observation, row[0], descriptor, row[1], row[2], None
    )
