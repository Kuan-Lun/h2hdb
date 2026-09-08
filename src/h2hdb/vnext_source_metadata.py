"""Independent bounded traversal of sealed canonical source metadata pages."""

from __future__ import annotations

from collections.abc import Iterator

from .sql_connector import SQLConnector
from .vnext_domains import require_bounded_bytes, require_digest32, require_int63
from .vnext_identity import (
    GalleryObservationBranchEntry,
    GalleryObservationComponent,
    GalleryObservationMetadataChunk,
    GalleryObservationNodeKind,
    decode_gallery_observation_page,
    gallery_observation_page_digest,
)


class SourceMetadataConflictError(ValueError):
    """Stored metadata page bytes or scalar descriptors are inconsistent."""


def iter_metadata_chunks(
    connector: SQLConnector,
    gallery_id: int,
    observation_id: int,
) -> Iterator[bytes]:
    roots = connector.fetch_all(
        "SELECT root.root_page_sha256 "
        "FROM catalog_gallery_observation_tree_roots AS root "
        "JOIN catalog_gallery_observation_page_descriptor_seals AS seal "
        "ON seal.page_sha256 = root.root_page_sha256 "
        "JOIN catalog_gallery_observation_page_descriptor_components AS descriptor "
        "ON descriptor.page_sha256 = seal.page_sha256 "
        "WHERE root.gallery_id = %s AND root.observation_id = %s "
        "AND descriptor.component = %s LIMIT 2",
        (gallery_id, observation_id, b"METADATA"),
    )
    if len(roots) != 1:
        raise SourceMetadataConflictError("sealed observation lacks one METADATA root")
    root = require_digest32(roots[0][0], field="metadata root_page_sha256")
    expected_offset = 0

    def visit(page_sha256: bytes, expected_level: int | None) -> Iterator[bytes]:
        nonlocal expected_offset
        row = connector.fetch_one(
            "SELECT page.page_bytes, descriptor.component, level.level, "
            "count.subtree_item_count "
            "FROM catalog_gallery_observation_page_descriptor_seals AS seal "
            "JOIN catalog_gallery_observation_pages AS page "
            "ON page.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_components "
            "AS descriptor ON descriptor.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_levels AS level "
            "ON level.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_subtree_item_counts "
            "AS count ON count.page_sha256 = seal.page_sha256 "
            "WHERE seal.page_sha256 = %s",
            (page_sha256,),
        )
        if len(row) != 4:
            raise SourceMetadataConflictError("metadata page or descriptor is missing")
        page_bytes = require_bounded_bytes(
            row[0],
            field="metadata page_bytes",
            minimum=1,
            maximum=64 * 1024,
        )
        if gallery_observation_page_digest(page_bytes) != page_sha256:
            raise SourceMetadataConflictError("metadata page digest differs from bytes")
        page = decode_gallery_observation_page(page_bytes)
        level = require_int63(row[2], field="metadata page level")
        count = require_int63(row[3], field="metadata page subtree count")
        if (
            row[1] != b"METADATA"
            or page.component is not GalleryObservationComponent.METADATA
            or page.level != level
            or page.subtree_item_count != count
            or (expected_level is not None and level != expected_level)
        ):
            raise SourceMetadataConflictError(
                "metadata page descriptor differs from bytes"
            )
        if page.node_kind is GalleryObservationNodeKind.LEAF:
            for entry in page.entries:
                if not isinstance(entry, GalleryObservationMetadataChunk):
                    raise SourceMetadataConflictError(
                        "metadata leaf has a non-chunk record"
                    )
                if entry.byte_offset != expected_offset:
                    raise SourceMetadataConflictError(
                        "metadata leaf offsets are not exactly contiguous"
                    )
                expected_offset = _sum_int63(
                    expected_offset,
                    len(entry.chunk_bytes),
                    field="metadata stream byte offset",
                )
                yield entry.chunk_bytes
            return
        normalized = connector.fetch_all(
            "SELECT position, child_sha256 "
            "FROM catalog_gallery_observation_page_children "
            "WHERE parent_sha256 = %s ORDER BY position LIMIT 257",
            (page_sha256,),
        )
        encoded = []
        for position, entry in enumerate(page.entries):
            if not isinstance(entry, GalleryObservationBranchEntry):
                raise SourceMetadataConflictError("metadata branch has a leaf record")
            encoded.append((position, entry.child_sha256))
        if normalized != encoded:
            raise SourceMetadataConflictError(
                "normalized metadata child edges differ from exact page bytes"
            )
        for _position, child in normalized:
            yield from visit(
                require_digest32(child, field="metadata child_sha256"),
                level - 1,
            )

    yield from visit(root, None)


def _sum_int63(left: int, right: int, *, field: str) -> int:
    return require_int63(
        require_int63(left, field=field) + require_int63(right, field=field),
        field=field,
    )
