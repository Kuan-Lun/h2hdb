"""Core-owned disk snapshot of exact source-observation pages.

The consumer adapter is read exactly once while this spool is built.  Later
source staging replays only versioned, receipt-checked records from the private
temporary SQLite database.  One bounded adapter page is materialized at a
time; the full source corpus is never retained in memory.
"""

from __future__ import annotations

__all__ = ["FrozenGalleryObservation", "FrozenSourceObservationSpool"]

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from .domain import (
    DirectoryObservation,
    FileObservation,
    TagObservation,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    _file_content_receipt_from_frozen_facts,
)
from .ports import VNextIngestSourceAdapter
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
)
from .vnext_gallery_staging_repository import (
    GalleryObservationComponentRoot,
    GalleryObservationComponentRootBuilder,
)
from .vnext_identity import (
    GalleryObservationComponent,
    GalleryObservationDescriptor,
    GalleryObservationDirectoryFileType,
    artifact_source_manifest_digest,
    encode_source_relative_locator,
    gallery_key,
    gallery_observation_descriptor_digest,
    iter_gallery_observation_metadata_stream,
    source_relative_locator_digest,
    source_root_digest,
    source_scope_key,
)
from .vnext_source_build_repository import (
    SourceBuildManifestSummary,
    SourceDiscoveryPlan,
    source_manifest_chain_step,
)

_CONSTRUCTOR_TOKEN = object()
_PAGE_MAGIC = b"h2hdb-vnext-frozen-source-observation-page-v1\0"
_PAGE_CODEC_VERSION = 1
_METADATA_CHUNK_BYTES = 32_768
_LOCATOR_DOMAIN = "source_relative_locator_v1"
_COMPONENT_CAPACITY = {
    GalleryObservationComponent.FILE: 256,
    GalleryObservationComponent.DIRECTORY: 192,
    GalleryObservationComponent.TAG: 256,
    GalleryObservationComponent.METADATA: 1,
}


class FrozenSourceObservationError(ValueError):
    """A private spool record, cursor, or observation receipt is malformed."""


@dataclass(frozen=True, slots=True)
class FrozenGalleryObservation:
    """Process-local capability for one exact gallery in a frozen spool."""

    position: int
    locator_components: tuple[str, ...]
    locator_sha256: bytes
    observation_identity_sha256: bytes
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_int63(self.position, field="frozen gallery position")
        if not isinstance(self.locator_components, tuple):
            raise TypeError("frozen gallery locator must be an exact tuple")
        encode_source_relative_locator(self.locator_components)
        require_digest32(self.locator_sha256, field="frozen gallery locator_sha256")
        require_digest32(
            self.observation_identity_sha256,
            field="frozen gallery observation_identity_sha256",
        )


@dataclass(frozen=True, slots=True)
class _DecodedPage:
    page_index: int
    next_cursor: bytes | int | None
    terminal: bool
    semantic_item_count: int
    items: tuple[Any, ...]


class FrozenSourceObservationSpool:
    """Disk-backed exact source snapshot consumed by source staging."""

    __slots__ = (
        "_capability",
        "_closed",
        "_directory",
        "_index",
        "_temporary",
        "manifest_summary",
        "source_root_components",
    )

    def __init__(
        self,
        *,
        temporary: TemporaryDirectory[str],
        index: sqlite3.Connection,
        source_root_components: tuple[str, ...],
        manifest_summary: SourceBuildManifestSummary,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _CONSTRUCTOR_TOKEN:
            raise TypeError("use FrozenSourceObservationSpool.freeze")
        self._temporary = temporary
        self._directory = Path(temporary.name)
        self._index = index
        self._capability = object()
        self._closed = False
        self.source_root_components = source_root_components
        self.manifest_summary = manifest_summary

    @classmethod
    def freeze(
        cls,
        adapter: VNextIngestSourceAdapter,
        *,
        plan: SourceDiscoveryPlan,
        source_root_components: tuple[str, ...],
    ) -> FrozenSourceObservationSpool:
        """Consume the live adapter once and seal every observation page."""

        if not isinstance(adapter, VNextIngestSourceAdapter):
            raise TypeError("adapter must implement VNextIngestSourceAdapter")
        if not isinstance(plan, SourceDiscoveryPlan):
            raise TypeError("plan must be SourceDiscoveryPlan")
        plan._require_open()
        if not isinstance(source_root_components, tuple):
            raise TypeError("source_root_components must be an exact tuple")
        source_root_digest(source_root_components)

        temporary = TemporaryDirectory(prefix="h2hdb-source-observations-")
        index = sqlite3.connect(Path(temporary.name) / "observations.sqlite3")
        spool = cls(
            temporary=temporary,
            index=index,
            source_root_components=source_root_components,
            manifest_summary=SourceBuildManifestSummary.empty(),
            _constructor_token=_CONSTRUCTOR_TOKEN,
        )
        try:
            spool._create_schema()
            spool.manifest_summary = spool._freeze_adapter(adapter, plan)
            index.commit()
            return spool
        except BaseException:
            spool.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._index.close()
            self._temporary.cleanup()

    def open_gallery(
        self,
        *,
        position: int,
        locator_sha256: bytes,
        locator_components: tuple[str, ...],
    ) -> FrozenGalleryObservation:
        """Open one exact locator and validate its frozen observation receipt."""

        self._require_open()
        expected_position = require_int63(position, field="frozen gallery position")
        expected_locator = require_digest32(
            locator_sha256,
            field="frozen gallery locator_sha256",
        )
        if not isinstance(locator_components, tuple):
            raise TypeError("locator_components must be an exact tuple")
        payload = encode_source_relative_locator(locator_components)
        if (
            source_relative_locator_digest(_LOCATOR_DOMAIN, locator_components)
            != expected_locator
        ):
            raise FrozenSourceObservationError(
                "frozen gallery locator digest differs from its components"
            )
        row = self._index.execute(
            "SELECT locator_sha256, locator_payload, locator_payload_sha256, "
            "metadata_root_sha256, metadata_item_count, file_root_sha256, "
            "file_item_count, tag_root_sha256, tag_item_count, "
            "directory_root_sha256, directory_item_count, "
            "observation_identity_sha256 FROM galleries WHERE position = ?",
            (expected_position,),
        ).fetchone()
        if row is None or len(row) != 12:
            raise FrozenSourceObservationError("frozen gallery receipt is absent")
        if row[0] != expected_locator or row[1] != payload:
            raise FrozenSourceObservationError("frozen gallery locator receipt changed")
        payload_sha256 = require_digest32(
            row[2],
            field="frozen locator payload_sha256",
        )
        if sha256(payload).digest() != payload_sha256:
            raise FrozenSourceObservationError("frozen locator payload digest changed")
        descriptor = GalleryObservationDescriptor(
            require_digest32(row[3], field="frozen metadata root"),
            require_int63(row[4], field="frozen metadata item_count"),
            require_digest32(row[5], field="frozen FILE root"),
            require_int63(row[6], field="frozen FILE item_count"),
            require_digest32(row[7], field="frozen TAG root"),
            require_int63(row[8], field="frozen TAG item_count"),
            require_digest32(row[9], field="frozen DIRECTORY root"),
            require_int63(row[10], field="frozen DIRECTORY item_count"),
        )
        observation_identity = require_digest32(
            row[11],
            field="frozen observation_identity_sha256",
        )
        if gallery_observation_descriptor_digest(descriptor) != observation_identity:
            raise FrozenSourceObservationError(
                "frozen gallery descriptor receipt changed"
            )
        return FrozenGalleryObservation(
            expected_position,
            locator_components,
            expected_locator,
            observation_identity,
            self._capability,
        )

    def list_file_observations(
        self,
        observation: FrozenGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        if limit != 256:
            raise ValueError("frozen FILE page limit must be 256")
        page = self._load_page(
            observation,
            GalleryObservationComponent.FILE,
            after_name_bytes,
        )
        items = _require_exact_items(page.items, FileObservation, label="FILE")
        return VNextIngestPage(items, page.next_cursor, page.terminal)

    def list_directory_observations(
        self,
        observation: FrozenGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        if limit != 192:
            raise ValueError("frozen DIRECTORY page limit must be 192")
        page = self._load_page(
            observation,
            GalleryObservationComponent.DIRECTORY,
            after_name_bytes,
        )
        items = _require_exact_items(
            page.items,
            DirectoryObservation,
            label="DIRECTORY",
        )
        return VNextIngestPage(items, page.next_cursor, page.terminal)

    def list_tag_observations(
        self,
        observation: FrozenGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]:
        if limit != 256:
            raise ValueError("frozen TAG page limit must be 256")
        page = self._load_page(
            observation,
            GalleryObservationComponent.TAG,
            after_ordinal,
        )
        items = _require_exact_items(page.items, TagObservation, label="TAG")
        return VNextIngestPage(items, page.next_cursor, page.terminal)

    def iter_metadata_chunks(
        self,
        observation: FrozenGalleryObservation,
        *,
        start_offset: int,
    ) -> Iterator[tuple[bytes, bool]]:
        """Replay exact receipt-checked METADATA chunks from one byte cursor."""

        offset = require_int63(start_offset, field="frozen METADATA start_offset")
        while True:
            page = self._load_page(
                observation,
                GalleryObservationComponent.METADATA,
                offset,
            )
            if len(page.items) != 1 or not isinstance(page.items[0], bytes):
                raise FrozenSourceObservationError(
                    "frozen METADATA page has an invalid chunk"
                )
            chunk = require_bounded_bytes(
                page.items[0],
                field="frozen METADATA chunk",
                minimum=1,
                maximum=_METADATA_CHUNK_BYTES,
            )
            if page.semantic_item_count != len(chunk):
                raise FrozenSourceObservationError(
                    "frozen METADATA chunk count changed"
                )
            yield chunk, page.terminal
            if page.terminal:
                return
            if not isinstance(page.next_cursor, int):
                raise FrozenSourceObservationError(
                    "frozen METADATA page lost its byte cursor"
                )
            if page.next_cursor != offset + len(chunk):
                raise FrozenSourceObservationError(
                    "frozen METADATA byte cursor is not contiguous"
                )
            offset = page.next_cursor

    def _create_schema(self) -> None:
        self._index.execute(
            "CREATE TABLE galleries ("
            "position INTEGER PRIMARY KEY, locator_sha256 BLOB UNIQUE NOT NULL, "
            "locator_payload BLOB NOT NULL, locator_payload_sha256 BLOB NOT NULL, "
            "metadata_root_sha256 BLOB NOT NULL, metadata_item_count INTEGER NOT NULL, "
            "file_root_sha256 BLOB NOT NULL, file_item_count INTEGER NOT NULL, "
            "tag_root_sha256 BLOB NOT NULL, tag_item_count INTEGER NOT NULL, "
            "directory_root_sha256 BLOB NOT NULL, "
            "directory_item_count INTEGER NOT NULL, "
            "observation_identity_sha256 BLOB NOT NULL)"
        )
        self._index.execute(
            "CREATE TABLE component_pages ("
            "gallery_position INTEGER NOT NULL, component INTEGER NOT NULL, "
            "page_index INTEGER NOT NULL, after_cursor BLOB NOT NULL, "
            "next_cursor BLOB NOT NULL, terminal INTEGER NOT NULL, "
            "semantic_item_count INTEGER NOT NULL, record_byte_count INTEGER NOT NULL, "
            "record_sha256 BLOB NOT NULL, record_bytes BLOB NOT NULL, "
            "PRIMARY KEY (gallery_position, component, after_cursor), "
            "UNIQUE (gallery_position, component, page_index))"
        )

    def _freeze_adapter(
        self,
        adapter: VNextIngestSourceAdapter,
        plan: SourceDiscoveryPlan,
    ) -> SourceBuildManifestSummary:
        scope = source_scope_key(
            "filesystem",
            source_root_digest(self.source_root_components),
            1,
        )
        summary = SourceBuildManifestSummary.empty()
        position = 0
        while position < plan.gallery_count:
            locators = plan._page(position)
            if not locators:
                raise FrozenSourceObservationError(
                    "source discovery plan ended before gallery_count"
                )
            for locator in locators:
                if locator.position != position:
                    raise FrozenSourceObservationError(
                        "source discovery positions are not contiguous"
                    )
                components = plan._decode_locator(
                    locator.position,
                    locator.locator_sha256,
                )
                observation = adapter.observe_gallery(components)
                if not isinstance(observation, VNextIngestGalleryObservation):
                    raise TypeError(
                        "observe_gallery must return VNextIngestGalleryObservation"
                    )
                observation.__post_init__()
                if observation.locator_components != components:
                    raise FrozenSourceObservationError(
                        "gallery observation locator differs from its plan"
                    )
                roots = self._freeze_gallery_pages(
                    adapter,
                    position=position,
                    locator_sha256=locator.locator_sha256,
                    observation=observation,
                )
                descriptor = GalleryObservationDescriptor(
                    roots[GalleryObservationComponent.METADATA].root_page_sha256,
                    roots[GalleryObservationComponent.METADATA].item_count,
                    roots[GalleryObservationComponent.FILE].root_page_sha256,
                    roots[GalleryObservationComponent.FILE].item_count,
                    roots[GalleryObservationComponent.TAG].root_page_sha256,
                    roots[GalleryObservationComponent.TAG].item_count,
                    roots[GalleryObservationComponent.DIRECTORY].root_page_sha256,
                    roots[GalleryObservationComponent.DIRECTORY].item_count,
                )
                observation_identity = gallery_observation_descriptor_digest(descriptor)
                locator_payload = encode_source_relative_locator(components)
                if (
                    source_relative_locator_digest(_LOCATOR_DOMAIN, components)
                    != locator.locator_sha256
                    or sha256(locator_payload).digest() != locator.payload_sha256
                    or len(locator_payload) != locator.payload_byte_count
                ):
                    raise FrozenSourceObservationError(
                        "frozen locator differs from its discovery receipt"
                    )
                self._index.execute(
                    "INSERT INTO galleries (position, locator_sha256, "
                    "locator_payload, locator_payload_sha256, "
                    "metadata_root_sha256, metadata_item_count, "
                    "file_root_sha256, file_item_count, tag_root_sha256, "
                    "tag_item_count, directory_root_sha256, "
                    "directory_item_count, observation_identity_sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        position,
                        locator.locator_sha256,
                        locator_payload,
                        locator.payload_sha256,
                        descriptor.metadata_root_sha256,
                        descriptor.metadata_byte_count,
                        descriptor.file_root_sha256,
                        descriptor.file_item_count,
                        descriptor.tag_root_sha256,
                        descriptor.tag_item_count,
                        descriptor.directory_root_sha256,
                        descriptor.directory_item_count,
                        observation_identity,
                    ),
                )
                file_root = roots[GalleryObservationComponent.FILE]
                summary = SourceBuildManifestSummary(
                    source_manifest_chain_step(
                        summary.manifest_sha256,
                        position=position,
                        gallery_key_bytes=gallery_key(
                            scope,
                            locator.locator_sha256,
                        ),
                        observation_identity_sha256=observation_identity,
                        gallery_manifest_sha256=artifact_source_manifest_digest(
                            observation_identity,
                            1,
                            1,
                        ),
                        file_count=file_root.item_count,
                        byte_count=file_root.byte_count,
                    ),
                    _checked_add(summary.gallery_count, 1, field="gallery_count"),
                    _checked_add(
                        summary.file_count,
                        file_root.item_count,
                        field="file_count",
                    ),
                    _checked_add(
                        summary.byte_count,
                        file_root.byte_count,
                        field="byte_count",
                    ),
                )
                position += 1
        if position != plan.gallery_count:
            raise FrozenSourceObservationError(
                "frozen observation count differs from discovery plan"
            )
        return summary

    def _freeze_gallery_pages(
        self,
        adapter: VNextIngestSourceAdapter,
        *,
        position: int,
        locator_sha256: bytes,
        observation: VNextIngestGalleryObservation,
    ) -> dict[GalleryObservationComponent, GalleryObservationComponentRoot]:
        roots: dict[GalleryObservationComponent, GalleryObservationComponentRoot] = {}
        roots[GalleryObservationComponent.FILE] = self._freeze_named_component(
            adapter,
            position=position,
            locator_sha256=locator_sha256,
            observation=observation,
            component=GalleryObservationComponent.FILE,
        )
        roots[GalleryObservationComponent.DIRECTORY] = self._freeze_named_component(
            adapter,
            position=position,
            locator_sha256=locator_sha256,
            observation=observation,
            component=GalleryObservationComponent.DIRECTORY,
        )
        roots[GalleryObservationComponent.TAG] = self._freeze_tag_component(
            adapter,
            position=position,
            locator_sha256=locator_sha256,
            observation=observation,
        )
        roots[GalleryObservationComponent.METADATA] = self._freeze_metadata_component(
            position=position,
            locator_sha256=locator_sha256,
            observation=observation,
        )
        return roots

    def _freeze_named_component(
        self,
        adapter: VNextIngestSourceAdapter,
        *,
        position: int,
        locator_sha256: bytes,
        observation: VNextIngestGalleryObservation,
        component: GalleryObservationComponent,
    ) -> GalleryObservationComponentRoot:
        builder = GalleryObservationComponentRootBuilder(component)
        after: bytes | None = None
        page_index = 0
        capacity = _COMPONENT_CAPACITY[component]
        while True:
            page: VNextIngestPage[Any]
            if component is GalleryObservationComponent.FILE:
                page = adapter.list_file_observations(
                    observation,
                    after_name_bytes=after,
                    limit=capacity,
                )
            elif component is GalleryObservationComponent.DIRECTORY:
                page = adapter.list_directory_observations(
                    observation,
                    after_name_bytes=after,
                    limit=capacity,
                )
            else:  # pragma: no cover - private caller is closed above
                raise AssertionError("named component must be FILE or DIRECTORY")
            next_after = _require_named_page(
                page,
                after=after,
                capacity=capacity,
                label=component.name,
            )
            builder.append_page(page.items, terminal=page.terminal)
            self._store_page(
                position=position,
                locator_sha256=locator_sha256,
                component=component,
                page_index=page_index,
                after_cursor=after,
                next_cursor=next_after,
                terminal=page.terminal,
                items=page.items,
                semantic_item_count=len(page.items),
            )
            if page.terminal:
                return builder.finish()
            after = next_after
            page_index += 1

    def _freeze_tag_component(
        self,
        adapter: VNextIngestSourceAdapter,
        *,
        position: int,
        locator_sha256: bytes,
        observation: VNextIngestGalleryObservation,
    ) -> GalleryObservationComponentRoot:
        component = GalleryObservationComponent.TAG
        builder = GalleryObservationComponentRootBuilder(component)
        after: int | None = None
        page_index = 0
        while True:
            page = adapter.list_tag_observations(
                observation,
                after_ordinal=after,
                limit=256,
            )
            next_after = _require_tag_page(page, after=after)
            builder.append_page(page.items, terminal=page.terminal)
            self._store_page(
                position=position,
                locator_sha256=locator_sha256,
                component=component,
                page_index=page_index,
                after_cursor=after,
                next_cursor=next_after,
                terminal=page.terminal,
                items=page.items,
                semantic_item_count=len(page.items),
            )
            if page.terminal:
                return builder.finish()
            after = next_after
            page_index += 1

    def _freeze_metadata_component(
        self,
        *,
        position: int,
        locator_sha256: bytes,
        observation: VNextIngestGalleryObservation,
    ) -> GalleryObservationComponentRoot:
        component = GalleryObservationComponent.METADATA
        builder = GalleryObservationComponentRootBuilder(component)
        offset = 0
        for page_index, (chunk, terminal) in enumerate(
            _iter_metadata_chunks(observation)
        ):
            next_offset = None if terminal else offset + len(chunk)
            builder.append_page((chunk,), terminal=terminal)
            self._store_page(
                position=position,
                locator_sha256=locator_sha256,
                component=component,
                page_index=page_index,
                after_cursor=offset,
                next_cursor=next_offset,
                terminal=terminal,
                items=(chunk,),
                semantic_item_count=len(chunk),
            )
            offset += len(chunk)
        return builder.finish()

    def _store_page(
        self,
        *,
        position: int,
        locator_sha256: bytes,
        component: GalleryObservationComponent,
        page_index: int,
        after_cursor: bytes | int | None,
        next_cursor: bytes | int | None,
        terminal: bool,
        items: Sequence[Any],
        semantic_item_count: int,
    ) -> None:
        after_frame = _encode_cursor(after_cursor, component=component)
        next_frame = _encode_cursor(next_cursor, component=component)
        record = _encode_page_record(
            position=position,
            locator_sha256=locator_sha256,
            component=component,
            page_index=page_index,
            after_cursor=after_frame,
            next_cursor=next_frame,
            terminal=terminal,
            semantic_item_count=semantic_item_count,
            items=items,
        )
        self._index.execute(
            "INSERT INTO component_pages (gallery_position, component, "
            "page_index, after_cursor, next_cursor, terminal, "
            "semantic_item_count, record_byte_count, record_sha256, "
            "record_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                position,
                int(component),
                page_index,
                after_frame,
                next_frame,
                int(terminal),
                semantic_item_count,
                len(record),
                sha256(record).digest(),
                record,
            ),
        )

    def _load_page(
        self,
        observation: FrozenGalleryObservation,
        component: GalleryObservationComponent,
        after_cursor: bytes | int | None,
    ) -> _DecodedPage:
        self._require_observation(observation)
        after_frame = _encode_cursor(after_cursor, component=component)
        row = self._index.execute(
            "SELECT page_index, next_cursor, terminal, semantic_item_count, "
            "record_byte_count, record_sha256, record_bytes "
            "FROM component_pages WHERE gallery_position = ? AND component = ? "
            "AND after_cursor = ?",
            (observation.position, int(component), after_frame),
        ).fetchone()
        if row is None or len(row) != 7:
            raise FrozenSourceObservationError(
                f"frozen {component.name} page is absent at its exact cursor"
            )
        page_index = require_int63(row[0], field="frozen page_index")
        next_frame = require_bounded_bytes(
            row[1],
            field="frozen next cursor frame",
            maximum=260,
        )
        if row[2] not in (0, 1):
            raise FrozenSourceObservationError("frozen page terminal is not a bit")
        terminal = bool(row[2])
        semantic_item_count = require_int63(
            row[3],
            field="frozen semantic_item_count",
        )
        record_byte_count = require_int63(
            row[4],
            field="frozen page record_byte_count",
        )
        record_sha256 = require_digest32(
            row[5],
            field="frozen page record_sha256",
        )
        record = require_bounded_bytes(
            row[6],
            field="frozen page record",
            maximum=INT63_MAX,
        )
        if len(record) != record_byte_count or sha256(record).digest() != record_sha256:
            raise FrozenSourceObservationError("frozen page record receipt changed")
        decoded = _decode_page_record(
            record,
            expected_position=observation.position,
            expected_locator_sha256=observation.locator_sha256,
            expected_component=component,
            expected_page_index=page_index,
            expected_after_cursor=after_frame,
        )
        if (
            decoded.next_cursor != _decode_cursor(next_frame, component=component)
            or decoded.terminal is not terminal
            or decoded.semantic_item_count != semantic_item_count
        ):
            raise FrozenSourceObservationError("frozen page index receipt changed")
        if terminal != (decoded.next_cursor is None):
            raise FrozenSourceObservationError(
                "frozen page terminal and next cursor disagree"
            )
        return decoded

    def _require_observation(self, observation: FrozenGalleryObservation) -> None:
        self._require_open()
        if type(observation) is not FrozenGalleryObservation:
            raise TypeError("observation must be a FrozenGalleryObservation")
        observation.__post_init__()
        if observation._capability is not self._capability:
            raise FrozenSourceObservationError(
                "frozen gallery observation belongs to another spool"
            )
        row = self._index.execute(
            "SELECT locator_sha256, observation_identity_sha256 FROM galleries "
            "WHERE position = ?",
            (observation.position,),
        ).fetchone()
        if row != (
            observation.locator_sha256,
            observation.observation_identity_sha256,
        ):
            raise FrozenSourceObservationError(
                "frozen gallery observation receipt changed"
            )

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("frozen source observation spool is closed")


def _require_named_page(
    page: VNextIngestPage[Any],
    *,
    after: bytes | None,
    capacity: int,
    label: str,
) -> bytes | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError(f"{label} adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != capacity:
        raise ValueError(f"nonterminal {label} page must contain {capacity} items")
    if page.terminal and not page.items and after is not None:
        raise ValueError(f"nonempty {label} streams cannot end with an empty page")
    prior = after
    for item in page.items:
        name = getattr(item, "name_bytes", None)
        if not isinstance(name, bytes):
            raise TypeError(f"{label} observation must expose bytes name_bytes")
        if prior is not None and name <= prior:
            raise ValueError(f"{label} page keys must be strictly increasing")
        prior = name
    if page.terminal:
        return None
    if not isinstance(page.next_after, bytes):
        raise TypeError(f"{label} next_after must be bytes")
    if not page.items or page.next_after != getattr(page.items[-1], "name_bytes"):
        raise ValueError(f"{label} next_after must equal the last item key")
    return page.next_after


def _require_tag_page(
    page: VNextIngestPage[Any],
    *,
    after: int | None,
) -> int | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError("TAG adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != 256:
        raise ValueError("nonterminal TAG page must contain 256 items")
    if page.terminal and not page.items and after is not None:
        raise ValueError("nonempty TAG streams cannot end with an empty page")
    if page.terminal:
        return None
    if not isinstance(page.next_after, int):
        raise TypeError("TAG next_after must be an ordinal")
    start = 0 if after is None else after + 1
    if page.next_after != start + len(page.items) - 1:
        raise ValueError("TAG next_after must equal the last page ordinal")
    return page.next_after


def _iter_metadata_chunks(
    observation: VNextIngestGalleryObservation,
) -> Iterator[tuple[bytes, bool]]:
    def raw_chunks() -> Iterator[bytes]:
        buffer = bytearray()
        for part in iter_gallery_observation_metadata_stream(observation.metadata):
            offset = 0
            while offset < len(part):
                consumed = min(
                    _METADATA_CHUNK_BYTES - len(buffer),
                    len(part) - offset,
                )
                buffer.extend(part[offset : offset + consumed])
                offset += consumed
                if len(buffer) == _METADATA_CHUNK_BYTES:
                    yield bytes(buffer)
                    buffer.clear()
        if buffer:
            yield bytes(buffer)

    chunks = raw_chunks()
    try:
        current = next(chunks)
    except StopIteration as error:  # pragma: no cover - metadata has fixed fields
        raise RuntimeError("gallery metadata stream is unexpectedly empty") from error
    for following in chunks:
        yield current, False
        current = following
    yield current, True


def _encode_cursor(
    cursor: bytes | int | None,
    *,
    component: GalleryObservationComponent,
) -> bytes:
    if cursor is None:
        return b"\x00"
    if component in {
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.DIRECTORY,
    }:
        value = require_bounded_bytes(
            cursor,
            field=f"frozen {component.name} cursor",
            minimum=1,
            maximum=255,
        )
        return b"\x01" + len(value).to_bytes(2, "big") + value
    if component in {
        GalleryObservationComponent.TAG,
        GalleryObservationComponent.METADATA,
    }:
        int_value = require_int63(
            cursor,
            field=f"frozen {component.name} cursor",
        )
        return b"\x02" + int_value.to_bytes(8, "big")
    raise AssertionError("unknown frozen component")  # pragma: no cover


def _decode_cursor(
    frame: bytes,
    *,
    component: GalleryObservationComponent,
) -> bytes | int | None:
    exact = require_bounded_bytes(
        frame,
        field="frozen cursor frame",
        minimum=1,
        maximum=258,
    )
    if exact == b"\x00":
        return None
    if exact[:1] == b"\x01" and component in {
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.DIRECTORY,
    }:
        if len(exact) < 3:
            raise FrozenSourceObservationError("frozen named cursor is truncated")
        size = int.from_bytes(exact[1:3], "big")
        if not 1 <= size <= 255 or len(exact) != size + 3:
            raise FrozenSourceObservationError("frozen named cursor length changed")
        return exact[3:]
    if exact[:1] == b"\x02" and component in {
        GalleryObservationComponent.TAG,
        GalleryObservationComponent.METADATA,
    }:
        if len(exact) != 9:
            raise FrozenSourceObservationError("frozen integer cursor length changed")
        return require_int63(
            int.from_bytes(exact[1:], "big"),
            field="frozen integer cursor",
        )
    raise FrozenSourceObservationError("frozen cursor kind differs from component")


def _encode_page_record(
    *,
    position: int,
    locator_sha256: bytes,
    component: GalleryObservationComponent,
    page_index: int,
    after_cursor: bytes,
    next_cursor: bytes,
    terminal: bool,
    semantic_item_count: int,
    items: Sequence[Any],
) -> bytes:
    payload = bytearray(_PAGE_MAGIC)
    payload.extend(_PAGE_CODEC_VERSION.to_bytes(4, "big"))
    payload.extend(
        require_int63(position, field="frozen page position").to_bytes(8, "big")
    )
    payload.extend(require_digest32(locator_sha256, field="frozen page locator"))
    payload.extend(int(component).to_bytes(1, "big"))
    payload.extend(
        require_int63(page_index, field="frozen page_index").to_bytes(8, "big")
    )
    _append_bytes(payload, after_cursor)
    _append_bytes(payload, next_cursor)
    if type(terminal) is not bool:
        raise TypeError("frozen page terminal must be bool")
    payload.extend(bytes((int(terminal),)))
    payload.extend(
        require_int63(
            semantic_item_count,
            field="frozen semantic_item_count",
        ).to_bytes(8, "big")
    )
    if len(items) > 256:
        raise ValueError("frozen page cannot exceed 256 entries")
    payload.extend(len(items).to_bytes(4, "big"))
    for item in items:
        _encode_page_item(payload, component, item)
    return bytes(payload)


def _encode_page_item(
    payload: bytearray,
    component: GalleryObservationComponent,
    item: Any,
) -> None:
    if component is GalleryObservationComponent.FILE:
        if not isinstance(item, FileObservation):
            raise TypeError("frozen FILE page contains a foreign item")
        item.__post_init__()
        _append_bytes(payload, item.name_bytes)
        payload.extend(item.content.file_sha256)
        payload.extend(item.content.size_bytes.to_bytes(8, "big"))
        payload.extend(item.device.to_bytes(8, "big"))
        payload.extend(item.inode.to_bytes(8, "big"))
        payload.extend(item.modified_ns.to_bytes(8, "big", signed=True))
        payload.extend(item.changed_ns.to_bytes(8, "big", signed=True))
        return
    if component is GalleryObservationComponent.DIRECTORY:
        if not isinstance(item, DirectoryObservation):
            raise TypeError("frozen DIRECTORY page contains a foreign item")
        item.__post_init__()
        _append_bytes(payload, item.name_bytes)
        payload.extend(item.size_bytes.to_bytes(8, "big"))
        payload.extend(item.device.to_bytes(8, "big"))
        payload.extend(item.inode.to_bytes(8, "big"))
        payload.extend(item.modified_ns.to_bytes(8, "big", signed=True))
        payload.extend(item.changed_ns.to_bytes(8, "big", signed=True))
        payload.extend(int(item.file_type).to_bytes(1, "big"))
        return
    if component is GalleryObservationComponent.TAG:
        if not isinstance(item, TagObservation):
            raise TypeError("frozen TAG page contains a foreign item")
        item.__post_init__()
        _append_bytes(payload, item._namespace_bytes)
        _append_bytes(payload, item._value_bytes)
        return
    if component is GalleryObservationComponent.METADATA:
        chunk = require_bounded_bytes(
            item,
            field="frozen METADATA chunk",
            minimum=1,
            maximum=_METADATA_CHUNK_BYTES,
        )
        _append_bytes(payload, chunk)
        return
    raise AssertionError("unknown frozen component")  # pragma: no cover


def _decode_page_record(
    record: bytes,
    *,
    expected_position: int,
    expected_locator_sha256: bytes,
    expected_component: GalleryObservationComponent,
    expected_page_index: int,
    expected_after_cursor: bytes,
) -> _DecodedPage:
    reader = _RecordReader(record)
    if reader.take(len(_PAGE_MAGIC)) != _PAGE_MAGIC:
        raise FrozenSourceObservationError("frozen page magic changed")
    if reader.uint(4) != _PAGE_CODEC_VERSION:
        raise FrozenSourceObservationError("frozen page codec version changed")
    if reader.uint(8) != expected_position:
        raise FrozenSourceObservationError("frozen page gallery position changed")
    if reader.take(32) != expected_locator_sha256:
        raise FrozenSourceObservationError("frozen page locator digest changed")
    try:
        component = GalleryObservationComponent(reader.uint(1))
    except ValueError as error:
        raise FrozenSourceObservationError(
            "frozen page component is unknown"
        ) from error
    if component is not expected_component:
        raise FrozenSourceObservationError("frozen page component changed")
    page_index = reader.uint(8)
    if page_index != expected_page_index:
        raise FrozenSourceObservationError("frozen page index changed")
    after_cursor = reader.bytes_field(maximum=258)
    if after_cursor != expected_after_cursor:
        raise FrozenSourceObservationError("frozen page start cursor changed")
    next_cursor_frame = reader.bytes_field(maximum=258)
    terminal_raw = reader.uint(1)
    if terminal_raw not in (0, 1):
        raise FrozenSourceObservationError("frozen page terminal is not a bit")
    semantic_item_count = require_int63(
        reader.uint(8),
        field="frozen semantic_item_count",
    )
    entry_count = reader.uint(4)
    if entry_count > 256:
        raise FrozenSourceObservationError("frozen page entry count exceeds 256")
    items = tuple(_decode_page_item(reader, component) for _index in range(entry_count))
    reader.require_eof()
    if component is GalleryObservationComponent.METADATA:
        if (
            entry_count != 1
            or not isinstance(items[0], bytes)
            or semantic_item_count != len(items[0])
        ):
            raise FrozenSourceObservationError("frozen METADATA page count changed")
    elif semantic_item_count != entry_count:
        raise FrozenSourceObservationError("frozen page item count changed")
    next_cursor = _decode_cursor(next_cursor_frame, component=component)
    return _DecodedPage(
        page_index,
        next_cursor,
        bool(terminal_raw),
        semantic_item_count,
        items,
    )


def _decode_page_item(
    reader: _RecordReader,
    component: GalleryObservationComponent,
) -> FileObservation | DirectoryObservation | TagObservation | bytes:
    if component is GalleryObservationComponent.FILE:
        name = reader.bytes_field(maximum=255)
        digest = reader.take(32)
        size = reader.uint(8)
        return FileObservation(
            name,
            _file_content_receipt_from_frozen_facts(digest, size),
            reader.uint(8),
            reader.uint(8),
            reader.sint64(),
            reader.sint64(),
        )
    if component is GalleryObservationComponent.DIRECTORY:
        name = reader.bytes_field(maximum=255)
        size = reader.uint(8)
        device = reader.uint(8)
        inode = reader.uint(8)
        modified_ns = reader.sint64()
        changed_ns = reader.sint64()
        try:
            file_type = GalleryObservationDirectoryFileType(reader.uint(1))
        except ValueError as error:
            raise FrozenSourceObservationError(
                "frozen DIRECTORY file_type is unknown"
            ) from error
        return DirectoryObservation(
            name,
            size,
            device,
            inode,
            modified_ns,
            changed_ns,
            file_type,
        )
    if component is GalleryObservationComponent.TAG:
        namespace_bytes = reader.bytes_field(maximum=255)
        value_bytes = reader.bytes_field(maximum=65_536)
        try:
            namespace = namespace_bytes.decode("utf-8", errors="strict")
            value = value_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise FrozenSourceObservationError(
                "frozen TAG bytes are not strict UTF-8"
            ) from error
        return TagObservation(namespace, value)
    if component is GalleryObservationComponent.METADATA:
        return reader.bytes_field(
            minimum=1,
            maximum=_METADATA_CHUNK_BYTES,
        )
    raise AssertionError("unknown frozen component")  # pragma: no cover


class _RecordReader:
    __slots__ = ("_offset", "_payload")

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def take(self, size: int) -> bytes:
        end = self._offset + size
        if size < 0 or end > len(self._payload):
            raise FrozenSourceObservationError("frozen page record is truncated")
        value = self._payload[self._offset : end]
        self._offset = end
        return value

    def uint(self, size: int) -> int:
        return int.from_bytes(self.take(size), "big")

    def sint64(self) -> int:
        return int.from_bytes(self.take(8), "big", signed=True)

    def bytes_field(self, *, maximum: int, minimum: int = 0) -> bytes:
        size = self.uint(4)
        if not minimum <= size <= maximum:
            raise FrozenSourceObservationError(
                "frozen page byte field length is outside its domain"
            )
        return self.take(size)

    def require_eof(self) -> None:
        if self._offset != len(self._payload):
            raise FrozenSourceObservationError(
                "frozen page record contains trailing bytes"
            )


def _append_bytes(payload: bytearray, value: bytes) -> None:
    exact = require_bounded_bytes(
        value,
        field="frozen page byte field",
        maximum=(1 << 32) - 1,
    )
    payload.extend(len(exact).to_bytes(4, "big"))
    payload.extend(exact)


def _require_exact_items[ItemT](
    items: tuple[Any, ...],
    item_type: type[ItemT],
    *,
    label: str,
) -> tuple[ItemT, ...]:
    if any(not isinstance(item, item_type) for item in items):
        raise FrozenSourceObservationError(
            f"frozen {label} page contains a foreign item"
        )
    return cast(tuple[ItemT, ...], items)


def _checked_add(left: int, right: int, *, field: str) -> int:
    value = require_int63(left, field=field) + require_int63(right, field=field)
    if value > INT63_MAX:
        raise OverflowError(f"frozen source {field} exceeds signed int63")
    return value
