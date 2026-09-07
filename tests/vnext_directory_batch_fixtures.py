"""Exact bounded page graphs for DIRECTORY traversal differential probes.

These graphs obey page, digest, descriptor, bound and parent-level contracts.
Their small leaves intentionally avoid allocating a fully packed height-eight
production tree. They exercise traversal shape, not full-tree packing/READY.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from h2hdb.vnext_gallery_staging_repository import GalleryStagingHandle
from h2hdb.vnext_identity import (
    GalleryObservationBranchEntry,
    GalleryObservationComponent,
    GalleryObservationDirectoryEntry,
    GalleryObservationDirectoryFileType,
    GalleryObservationNodeKind,
    GalleryObservationPage,
    encode_gallery_observation_page,
    file_key,
)


@dataclass(frozen=True, slots=True)
class _Node:
    payload: bytes
    level: int
    count: int
    first: bytes
    last: bytes
    children: tuple[bytes, ...]


class DirectoryGraphConnector:
    """Fresh driver-shaped allocations over deterministic sealed exact pages."""

    def __init__(
        self,
        *,
        leaves: int = 256,
        leaf_entries: int = 1,
        depth: int = 2,
        wide_path: bool = False,
    ) -> None:
        self.nodes: dict[bytes, _Node] = {}
        self.page_reads: dict[bytes, int] = {}
        self.child_reads: dict[bytes, int] = {}
        self.root_reads = 0
        self.rows: list[tuple[Any, ...]] = []
        leaves_sha: list[bytes] = []
        for leaf in range(leaves):
            entries = tuple(
                self._entry(leaf * leaf_entries + offset)
                for offset in range(leaf_entries)
            )
            page = GalleryObservationPage(
                GalleryObservationComponent.DIRECTORY,
                GalleryObservationNodeKind.LEAF,
                0,
                len(entries),
                entries,
            )
            leaves_sha.append(
                self._add(page, entries[0].name_bytes, entries[-1].name_bytes, ())
            )
            selected = entries[-1]
            self.rows.append(
                (
                    leaf,
                    file_key(selected.name_bytes),
                    selected.name_bytes,
                    bytes(32),
                    selected.size_bytes,
                    selected.device.to_bytes(8, "big"),
                    selected.inode.to_bytes(8, "big"),
                    selected.modified_ns.to_bytes(8, "big", signed=True),
                    selected.changed_ns.to_bytes(8, "big", signed=True),
                )
            )
        if wide_path:
            self.root = self._branch(tuple(leaves_sha), 1)
            next_ordinal = leaves * leaf_entries
            for level in range(2, depth + 1):
                siblings = [self.root]
                for _sibling in range(255):
                    entry = self._entry(next_ordinal)
                    next_ordinal += 1
                    page = GalleryObservationPage(
                        GalleryObservationComponent.DIRECTORY,
                        GalleryObservationNodeKind.LEAF,
                        0,
                        1,
                        (entry,),
                    )
                    child = self._add(page, entry.name_bytes, entry.name_bytes, ())
                    for child_level in range(1, level):
                        child = self._branch((child,), child_level)
                    siblings.append(child)
                self.root = self._branch(tuple(siblings), level)
        else:
            frontier = leaves_sha
            level = 0
            while len(frontier) > 1 or level < depth:
                level += 1
                group_size = 16 if level == 1 and depth > 1 else 256
                frontier = [
                    self._branch(tuple(frontier[start : start + group_size]), level)
                    for start in range(0, len(frontier), group_size)
                ]
            self.root = frontier[0]
        self.rows.reverse()
        self.rows = [(position, *row[1:]) for position, row in enumerate(self.rows)]

    def _branch(self, children: tuple[bytes, ...], level: int) -> bytes:
        entries = tuple(
            GalleryObservationBranchEntry(digest, self.nodes[digest].count)
            for digest in children
        )
        page = GalleryObservationPage(
            GalleryObservationComponent.DIRECTORY,
            GalleryObservationNodeKind.BRANCH,
            level,
            sum(entry.child_subtree_item_count for entry in entries),
            entries,
        )
        return self._add(
            page, self.nodes[children[0]].first, self.nodes[children[-1]].last, children
        )

    @staticmethod
    def _entry(index: int) -> GalleryObservationDirectoryEntry:
        name = f"{index:08d}".encode() + b"x" * 243 + b".jpg"
        return GalleryObservationDirectoryEntry(
            index,
            name,
            4096,
            1,
            index,
            -7,
            9,
            GalleryObservationDirectoryFileType.REGULAR,
        )

    def _add(
        self,
        page: GalleryObservationPage,
        first: bytes,
        last: bytes,
        children: tuple[bytes, ...],
    ) -> bytes:
        payload = encode_gallery_observation_page(page)
        digest = sha256(payload).digest()
        self.nodes[digest] = _Node(
            payload, page.level, page.subtree_item_count, first, last, children
        )
        return digest

    @staticmethod
    def handle() -> GalleryStagingHandle:
        return GalleryStagingHandle(b"s" * 16, b"b" * 16, 1, 1, 1, 0)

    def fetch_one(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        digest = parameters[0]
        node = self.nodes[digest]
        if (
            "FROM catalog_gallery_observation_page_descriptor_anchors AS anchor"
            in query
        ):
            self.page_reads[digest] = self.page_reads.get(digest, 0) + 1
            return (
                digest,
                memoryview(node.payload).tobytes(),
                b"DIRECTORY",
                node.level,
                node.count,
                digest,
            )
        if (
            "FROM catalog_gallery_observation_page_key_bounds_anchors AS anchor"
            in query
        ):
            return (
                digest,
                memoryview(node.first).tobytes(),
                memoryview(node.last).tobytes(),
                digest,
            )
        raise AssertionError(query)

    def fetch_all(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        if "SELECT r.root_page_sha256, d.subtree_item_count" in query:
            self.root_reads += 1
            return [(self.root, self.nodes[self.root].count)]
        if "FROM catalog_gallery_observation_page_children c " in query:
            assert "LIMIT 257" in query
            digest = parameters[0]
            self.child_reads[digest] = self.child_reads.get(digest, 0) + 1
            return [
                (
                    position,
                    child,
                    child,
                    child,
                    b"DIRECTORY",
                    self.nodes[child].level,
                    self.nodes[child].count,
                    child,
                    child,
                    memoryview(self.nodes[child].first).tobytes(),
                    memoryview(self.nodes[child].last).tobytes(),
                )
                for position, child in enumerate(self.nodes[digest].children)
            ]
        raise AssertionError(query)
