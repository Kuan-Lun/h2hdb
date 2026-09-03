"""Backend-neutral production pipeline driver over the public vNext facades.

This module contains no production code and no SQL.  It drives exactly the
public ``VNextIngestFacade`` issue/prepare/commit protocol that the shipped
ingest service uses, with in-memory source, storage, release and library
activation adapters, so the same source-to-publication workflow can execute on
generated SQLite and on live MariaDB 10.11.11, under statement fault injection,
and across process-restart simulations.
"""

from __future__ import annotations

import dataclasses
import struct
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from enum import Enum
from hashlib import sha256
from io import BytesIO
from typing import Any, BinaryIO, cast

from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactPagePresentationEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactReleaseStorageEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactSourceRole,
    ArtifactStorageEvidence,
    ArtifactThumbnailPresentationEvidence,
    ByteExtent,
    CatalogFacetKind,
    CatalogRecentOrder,
    CatalogResourceKind,
    CoreConfig,
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    LibraryActivationCheckpoint,
    LibraryActivationStatus,
    SchemaEpochReport,
    StorageObjectDescriptor,
    StorageObjectKey,
    TagObservation,
    VNextAnalysisAdvanceResult,
    VNextArtifactAdapterPolicy,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextDownloadQueueFacade,
    VNextIngestAdvanceResult,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextIngestPhase,
    VNextIngestPolicy,
    VNextIngestSession,
    VNextIngestSourceReceipt,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
    VNextResolvedIngestPolicy,
)

LEASE_MICROSECONDS = 10**9
ADAPTER_ID = b"memory-library-v1"
POLICY_FINGERPRINT = sha256(b"memory-library-policy-v1").digest()
METADATA_NAME = b"galleryinfo.txt"
ARCHIVE_MEDIA_TYPE = "application/vnd.comicbook+zip"
PAGE_MEDIA_TYPE = "image/png"
THUMBNAIL_MEDIA_TYPE = "image/jpeg"
_ARCHIVE_MAGIC = b"H2HMEMARCH1\0"
_PAGE_SUFFIXES = (b".png", b".jpg", b".jpeg", b".gif")


class Clock:
    """Strictly increasing wall-clock microseconds.

    Sealed prerequisites (build seal, analysis completion) are stamped by the
    database's own clock, so facade timestamps must track real time; the clock
    only guarantees strict monotonicity on top of ``time.time_ns``.
    """

    def __init__(self, step: int = 1, *, offset: int = 0) -> None:
        self._last = 0
        self._step = step
        self._offset = offset
        self.calls = 0

    def __call__(self) -> int:
        self.calls += 1
        value = max(time.time_ns() // 1_000 + self._offset, self._last + self._step)
        self._last = value
        return value


def takeover_clock() -> Clock:
    """A clock past every lease issued by an earlier real-time clock, so a
    fresh facade performs the expired-lease takeover a restarted process sees."""

    return Clock(offset=LEASE_MICROSECONDS + 1_000_000)


def ingest_policy(
    *,
    spam_occurrence_threshold: int = 3,
    artifacts_required: bool = True,
) -> VNextIngestPolicy:
    return VNextIngestPolicy(
        artifact=VNextArtifactAdapterPolicy(
            adapter_id=ADAPTER_ID,
            policy_fingerprint_sha256=POLICY_FINGERPRINT,
        ),
        spam_occurrence_threshold=spam_occurrence_threshold,
        artifacts_required=artifacts_required,
    )


def file_role(name: bytes) -> ArtifactSourceRole:
    if name == METADATA_NAME:
        return ArtifactSourceRole.METADATA
    if name.lower().endswith(_PAGE_SUFFIXES):
        return ArtifactSourceRole.PAGE
    return ArtifactSourceRole.OTHER


@dataclasses.dataclass(frozen=True)
class MemoryGallery:
    """One exact in-memory gallery observation."""

    locator: tuple[str, ...]
    gid: int
    title: str
    files: Mapping[bytes, bytes]
    tags: tuple[tuple[str, str], ...] = ()
    directories: tuple[bytes, ...] = ()
    upload_account: str = "uploader"
    upload_time: int = 1_700_000_000
    download_time: int = 1_700_000_100
    modified_time: int = 1_700_000_200
    comment: str = ""

    def __post_init__(self) -> None:
        if METADATA_NAME not in self.files:
            raise ValueError("a memory gallery must contain galleryinfo.txt")
        if any(not isinstance(name, bytes) for name in self.files):
            raise TypeError("memory gallery file names must be bytes")

    def metadata(self) -> GalleryObservationMetadata:
        pages = sum(file_role(name) is ArtifactSourceRole.PAGE for name in self.files)
        return GalleryObservationMetadata(
            gid=self.gid,
            title=self.title,
            comment=self.comment,
            upload_account=self.upload_account,
            upload_time=self.upload_time,
            download_time=self.download_time,
            modified_time=self.modified_time,
            scan_observation_version=1,
            source_file_count=len(self.files),
            page_count=pages,
        )


def gallery(
    gid: int,
    *,
    title: str | None = None,
    pages: Sequence[bytes] | None = None,
    artists: Sequence[str] = ("artist-a",),
    language: str | None = "english",
    extra_tags: Sequence[tuple[str, str]] = (),
    locator: tuple[str, ...] | None = None,
    other_files: Mapping[bytes, bytes] | None = None,
    directories: tuple[bytes, ...] = (),
    **metadata: Any,
) -> MemoryGallery:
    """Build one deterministic gallery with dense PNG-named pages."""

    files: dict[bytes, bytes] = {
        METADATA_NAME: (f"Title: {title or f'Gallery {gid}'}\nGID: {gid}\n".encode())
    }
    if pages is None:
        pages = (f"page-0-of-{gid}".encode(),)
    for index, content in enumerate(pages):
        files[f"{index:03d}.png".encode("ascii")] = content
    if other_files:
        files.update(other_files)
    tags: list[tuple[str, str]] = [("artist", artist) for artist in artists]
    if language is not None:
        tags.append(("language", language))
    tags.extend(extra_tags)
    return MemoryGallery(
        locator=locator or (f"gallery-{gid}",),
        gid=gid,
        title=title or f"Gallery {gid}",
        files=files,
        tags=tuple(tags),
        directories=directories,
        **metadata,
    )


class MemorySource:
    """Replayable, keyset-paged implementation of VNextIngestSourceAdapter."""

    def __init__(
        self,
        galleries: Sequence[MemoryGallery] = (),
        *,
        root: tuple[str, ...] = ("memory", "source"),
    ) -> None:
        self._root = root
        self._galleries: dict[tuple[str, ...], MemoryGallery] = {}
        self.page_calls = 0
        for value in galleries:
            self.put(value)

    @property
    def source_root_components(self) -> tuple[str, ...]:
        return self._root

    @property
    def galleries(self) -> tuple[MemoryGallery, ...]:
        return tuple(self._galleries[key] for key in sorted(self._galleries))

    def put(self, value: MemoryGallery) -> None:
        if not isinstance(value, MemoryGallery):
            raise TypeError("memory source accepts MemoryGallery only")
        self._galleries[value.locator] = value

    def remove(self, locator: tuple[str, ...]) -> None:
        del self._galleries[locator]

    def get(self, locator: tuple[str, ...]) -> MemoryGallery:
        return self._galleries[locator]

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> VNextIngestPage[tuple[str, ...]]:
        self.page_calls += 1
        keys = sorted(self._galleries)
        if after_locator is not None:
            keys = [key for key in keys if key > after_locator]
        items = tuple(keys[:limit])
        terminal = len(keys) <= limit
        return VNextIngestPage(items, None if terminal else items[-1], terminal)

    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextIngestGalleryObservation:
        self.page_calls += 1
        return VNextIngestGalleryObservation(
            locator_components,
            self._galleries[locator_components].metadata(),
        )

    @staticmethod
    def _stat(
        value: MemoryGallery, name: bytes, *, directory: bool
    ) -> tuple[int, int, int, int]:
        seed = sha256((b"dir:" if directory else b"file:") + name).digest()
        return (
            1,
            int.from_bytes(seed[:6], "big"),
            value.modified_time * 1_000_000_000,
            value.modified_time * 1_000_000_000,
        )

    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        self.page_calls += 1
        value = self._galleries[observation.locator_components]
        names = sorted(value.files)
        if after_name_bytes is not None:
            names = [name for name in names if name > after_name_bytes]
        selected = names[:limit]
        items: list[FileObservation] = []
        for name in selected:
            device, inode, modified_ns, changed_ns = self._stat(
                value, name, directory=False
            )
            items.append(
                FileObservation(
                    name_bytes=name,
                    content=FileContentReceipt.from_parts((value.files[name],)),
                    artifact_role=file_role(name),
                    device=device,
                    inode=inode,
                    modified_ns=modified_ns,
                    changed_ns=changed_ns,
                )
            )
        terminal = len(names) <= limit
        return VNextIngestPage(
            tuple(items),
            None if terminal else items[-1].name_bytes,
            terminal,
        )

    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        """List every direct child: files as REGULAR, subdirectories as DIRECTORY."""

        self.page_calls += 1
        value = self._galleries[observation.locator_components]
        names = sorted(set(value.files) | set(value.directories))
        if after_name_bytes is not None:
            names = [name for name in names if name > after_name_bytes]
        selected = names[:limit]
        items: list[DirectoryObservation] = []
        for name in selected:
            is_directory = name not in value.files
            device, inode, modified_ns, changed_ns = self._stat(
                value, name, directory=is_directory
            )
            items.append(
                DirectoryObservation(
                    name_bytes=name,
                    size_bytes=64 if is_directory else len(value.files[name]),
                    device=device,
                    inode=inode,
                    modified_ns=modified_ns,
                    changed_ns=changed_ns,
                    file_type=(
                        GalleryObservationDirectoryFileType.DIRECTORY
                        if is_directory
                        else GalleryObservationDirectoryFileType.REGULAR
                    ),
                )
            )
        terminal = len(names) <= limit
        return VNextIngestPage(
            tuple(items),
            None if terminal else items[-1].name_bytes,
            terminal,
        )

    def list_tag_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]:
        self.page_calls += 1
        value = self._galleries[observation.locator_components]
        start = 0 if after_ordinal is None else after_ordinal + 1
        selected = value.tags[start : start + limit]
        items = tuple(TagObservation(namespace, tag) for namespace, tag in selected)
        terminal = start + len(selected) >= len(value.tags)
        return VNextIngestPage(
            items,
            None if terminal else start + len(items) - 1,
            terminal,
        )


@dataclasses.dataclass
class _Activation:
    receipt_id: bytes
    status: LibraryActivationStatus
    cursor: VNextLibraryActivationCursor | None
    items: list[VNextLibraryActivationItem]


def storage_key(gid: int, resource_kind: CatalogResourceKind) -> StorageObjectKey:
    return StorageObjectKey(
        "memory-v1",
        (f"{gid % 256:02x}", f"h2h-{gid}", resource_kind.value),
    )


def encode_archive(members: Sequence[tuple[ArtifactSourceRole, int, bytes]]) -> bytes:
    """Deterministic self-describing archive: header + member bytes."""

    pages = [
        (position, body)
        for role, position, body in members
        if role is ArtifactSourceRole.PAGE
    ]
    header = _ARCHIVE_MAGIC + struct.pack(">I", len(pages))
    header_length = len(header) + 16 * len(pages)
    body = b"".join(body for _role, _position, body in members)
    offset = header_length + sum(
        len(body)
        for role, _position, body in members
        if role is not ArtifactSourceRole.PAGE
    )
    extents: list[bytes] = []
    for _position, page in pages:
        extents.append(struct.pack(">QQ", offset, len(page)))
        offset += len(page)
    non_pages = b"".join(
        body for role, _position, body in members if role is not ArtifactSourceRole.PAGE
    )
    page_bytes = b"".join(page for _position, page in pages)
    del body
    return header + b"".join(extents) + non_pages + page_bytes


def decode_archive_extents(archive: bytes) -> tuple[ByteExtent, ...]:
    if not archive.startswith(_ARCHIVE_MAGIC):
        raise ValueError("not a memory archive")
    (count,) = struct.unpack(
        ">I", archive[len(_ARCHIVE_MAGIC) : len(_ARCHIVE_MAGIC) + 4]
    )
    extents: list[ByteExtent] = []
    cursor = len(_ARCHIVE_MAGIC) + 4
    for _ in range(count):
        offset, length = struct.unpack(">QQ", archive[cursor : cursor + 16])
        extents.append(ByteExtent(offset, length))
        cursor += 16
    return tuple(extents)


class MemoryLibrary:
    """In-memory storage, release, and library-activation adapter.

    The object is deliberately kept alive across facade restarts in tests: it
    plays the role of the durable filesystem journal that the real adapter
    keeps below ``.h2hdb-state``.  ``objects`` holds every protected byte
    string by storage key, ``tokens`` the protection token that owns each
    key, ``tombstones`` every terminally released token, and ``current`` the
    reader-visible resources of the last completed activation.
    """

    adapter_id = ADAPTER_ID
    policy_fingerprint_sha256 = POLICY_FINGERPRINT

    def __init__(self, source: MemorySource) -> None:
        self.source = source
        # Private staging: protected but not yet activated bytes by key.
        self.staging: dict[StorageObjectKey, tuple[bytes, bytes]] = {}
        # Reader-visible current tree bytes by key.
        self.objects: dict[StorageObjectKey, bytes] = {}
        self.tombstones: set[bytes] = set()
        self.activations: dict[int, _Activation] = {}
        self.current: dict[
            tuple[int, CatalogResourceKind], StorageObjectDescriptor
        ] = {}
        self.render_calls = 0
        self.protect_calls = 0
        self.release_calls: list[tuple[StorageObjectKey, bytes]] = []
        self.activation_calls: list[str] = []

    # -- ArtifactStorageAdapter -------------------------------------------

    def storage_key(
        self,
        gid: int,
        resource_kind: CatalogResourceKind,
    ) -> StorageObjectKey:
        return storage_key(gid, resource_kind)

    def open_source(
        self,
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        if source_root_components != self.source.source_root_components:
            raise ValueError("foreign source root")
        return BytesIO(self.source.get(gallery_locator_components).files[source_name])

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        self.render_calls += 1
        staged: list[tuple[ArtifactSourceRole, int, bytes]] = []
        for member in members:
            member.source.seek(0)
            payload = member.source.read()
            if (
                sha256(payload).digest() != member.expected_sha256
                or len(payload) != member.expected_size_bytes
            ):
                raise ValueError("member bytes disagree with sealed authority")
            staged.append((member.role, member.position, payload))
        archive = encode_archive(staged)
        destination.write(archive)
        page_members = [
            entry for entry in staged if entry[0] is ArtifactSourceRole.PAGE
        ]
        pages = tuple(
            ArtifactRenderedPage(index, position, f"pages/{index:04d}.png")
            for index, (_role, position, _payload) in enumerate(page_members)
        )
        return ArtifactArchiveRenderEvidence(
            sha256(archive).digest(),
            len(archive),
            ARCHIVE_MEDIA_TYPE,
            f"h2h-{gid}.cbz",
            pages,
        )

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        archive.seek(0)
        payload = archive.read()
        extents = decode_archive_extents(payload)
        if len(extents) != len(rendered_pages):
            raise ValueError("archive extents do not match rendered pages")
        pages = tuple(
            ArtifactPagePresentationEvidence(
                page_index=page.page_index,
                locator=page.locator,
                extent=extent,
                media_type=PAGE_MEDIA_TYPE,
                sha256=sha256(
                    payload[extent.offset : extent.offset + extent.length]
                ).digest(),
                width=64 + page.page_index,
                height=96,
            )
            for page, extent in zip(rendered_pages, extents, strict=True)
        )
        thumbnail: ArtifactThumbnailPresentationEvidence | None = None
        if pages:
            first = extents[0]
            thumbnail_bytes = (
                b"THUMB" + payload[first.offset : first.offset + first.length]
            )
            thumbnail_destination.write(thumbnail_bytes)
            thumbnail = ArtifactThumbnailPresentationEvidence(
                len(thumbnail_bytes),
                THUMBNAIL_MEDIA_TYPE,
                sha256(thumbnail_bytes).digest(),
                320,
                480,
            )
        return ArtifactPresentationRenderEvidence(pages, thumbnail)

    def protect(
        self,
        archive: BinaryIO,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        modified_at: datetime,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        self.protect_calls += 1
        archive.seek(0)
        payload = archive.read()
        if sha256(payload).digest() != expected_sha256 or len(payload) != (
            expected_size_bytes
        ):
            raise ValueError("protect received bytes that disagree with authority")
        if protection_token in self.tombstones:
            # A delayed protect can never resurrect a released token.
            return ArtifactStorageEvidence(False, None)
        staged = self.staging.get(storage_key)
        if staged is not None and staged[1] == protection_token:
            if sha256(staged[0]).digest() != expected_sha256:
                raise ValueError("protection token already staged different bytes")
        self.staging[storage_key] = (payload, protection_token)
        return ArtifactStorageEvidence(
            True,
            StorageObjectDescriptor(
                storage_key,
                expected_size_bytes,
                expected_sha256.hex(),
                modified_at,
            ),
        )

    # -- ArtifactReleaseAdapter -------------------------------------------

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        """Terminal, idempotent tombstone of one exact protection token.

        Only the private staging entry owned by that exact token is removed;
        bytes already activated into the current tree are never touched.
        """

        self.release_calls.append((storage_key, protection_token))
        self.tombstones.add(protection_token)
        staged = self.staging.get(storage_key)
        if staged is not None and staged[1] == protection_token:
            if (
                sha256(staged[0]).digest() != expected_sha256
                or len(staged[0]) != expected_size_bytes
            ):
                raise ValueError("release authority disagrees with staged object")
            del self.staging[storage_key]
        return ArtifactReleaseStorageEvidence(True)

    # -- VNextLibraryActivationAdapter ------------------------------------

    def _checkpoint(self, revision: int) -> LibraryActivationCheckpoint:
        state = self.activations[revision]
        return LibraryActivationCheckpoint(
            revision,
            state.receipt_id,
            state.status,
            state.cursor,
        )

    def begin(self, revision: int, receipt_id: bytes) -> LibraryActivationCheckpoint:
        self.activation_calls.append("begin")
        state = self.activations.get(revision)
        if state is None:
            state = _Activation(receipt_id, LibraryActivationStatus.SPOOL, None, [])
            self.activations[revision] = state
        elif state.receipt_id != receipt_id:
            raise RuntimeError("library activation receipt differs for revision")
        return self._checkpoint(revision)

    def activate_page(
        self,
        revision: int,
        items: Sequence[VNextLibraryActivationItem],
    ) -> None:
        """Move each exact staged resource into current (atomic no-replace)."""

        self.activation_calls.append("activate_page")
        state = self.activations[revision]
        if state.status is not LibraryActivationStatus.SPOOL:
            raise RuntimeError("activate_page outside SPOOL")
        for item in items:
            key = item.storage_object.key
            staged = self.staging.get(key)
            if (
                staged is not None
                and sha256(staged[0]).hexdigest() == item.storage_object.sha256
                and len(staged[0]) == item.storage_object.size_bytes
            ):
                self.objects[key] = staged[0]
                del self.staging[key]
            else:
                payload = self.objects.get(key)
                if payload is None:
                    raise RuntimeError("activation item is neither staged nor current")
                if (
                    sha256(payload).hexdigest() != item.storage_object.sha256
                    or len(payload) != item.storage_object.size_bytes
                ):
                    raise RuntimeError("activation item disagrees with current bytes")
            state.items.append(item)
        if items:
            state.cursor = VNextLibraryActivationCursor(
                items[-1].publication_key,
                items[-1].resource_kind,
            )

    def seal(self, revision: int) -> None:
        self.activation_calls.append("seal")
        state = self.activations[revision]
        state.status = LibraryActivationStatus.RECONCILE
        state.cursor = None

    def reconcile_page(
        self,
        revision: int,
        receipt_id: bytes,
        *,
        limit: int,
    ) -> LibraryActivationCheckpoint:
        """Exact stale removal of every resource the revision no longer references."""

        self.activation_calls.append("reconcile_page")
        state = self.activations[revision]
        if state.receipt_id != receipt_id or limit != 128:
            raise RuntimeError("reconcile authority mismatch")
        if state.status is LibraryActivationStatus.RECONCILE:
            live = {item.storage_object.key for item in state.items}
            for key in list(self.objects):
                if key not in live:
                    del self.objects[key]
            self.current = {
                (item.gid, item.resource_kind): item.storage_object
                for item in state.items
            }
            state.status = LibraryActivationStatus.READY
        return self._checkpoint(revision)

    def complete(self, revision: int, receipt_id: bytes) -> None:
        self.activation_calls.append("complete")
        state = self.activations[revision]
        if state.receipt_id != receipt_id:
            raise RuntimeError("complete authority mismatch")
        if state.status is LibraryActivationStatus.COMPLETE:
            return
        if state.status is not LibraryActivationStatus.READY:
            raise RuntimeError("complete outside READY")
        state.status = LibraryActivationStatus.COMPLETE


@dataclasses.dataclass(frozen=True)
class IngestTurnReceipts:
    session: VNextIngestSession
    policy: VNextResolvedIngestPolicy
    source: VNextIngestSourceReceipt
    analysis: VNextAnalysisAdvanceResult
    publication: VNextIngestAdvanceResult
    completion: VNextIngestCompletionReceipt


Boundary = Callable[[str], None] | None


def _notify(boundary: Boundary, label: str) -> None:
    if boundary is not None:
        boundary(label)


def run_source(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    policy: VNextResolvedIngestPolicy,
    source: MemorySource,
    *,
    step_budget: int = 10_000,
    boundary: Boundary = None,
) -> VNextIngestSourceReceipt:
    with facade.prepare_source(source) as prepared:
        for _ in range(step_budget):
            _notify(boundary, "source.issue")
            issued = facade.issue_source_step(session, policy, prepared)
            local = facade.prepare_source_step(prepared, issued)
            _notify(boundary, f"source.commit:{issued._action.value}")
            result = facade.commit_source_step(session, local)
            if result.phase is not VNextIngestPhase.SOURCE:
                raise RuntimeError("source advancement returned another phase")
            if result.terminal:
                receipt = result.source_receipt
                if receipt is None or not receipt.sealed:
                    raise RuntimeError("terminal source step lacks a sealed receipt")
                return receipt
    raise RuntimeError("source synchronization exceeded its step budget")


def run_analysis(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    policy: VNextResolvedIngestPolicy,
    build_id: bytes,
    *,
    max_rows: int = 128,
    step_budget: int = 10_000,
    boundary: Boundary = None,
) -> VNextAnalysisAdvanceResult:
    prepared = facade.prepare_analysis(build_id, policy, max_rows=max_rows)
    with prepared:
        for _ in range(step_budget):
            _notify(boundary, "analysis.issue")
            issued = facade.issue_analysis_step(session, prepared)
            local = facade.prepare_analysis_step(prepared, issued)
            payload = issued._payload
            stage = (
                payload.stage.decode("ascii")
                if payload is not None and payload.stage is not None
                else "none"
            )
            _notify(boundary, f"analysis.commit:{stage}")
            result = facade.commit_analysis_step(session, local)
            if result.terminal:
                if (
                    not result.stage_terminal
                    or result.stage != b"snapshot_manifest"
                    or result.snapshot_manifest_sha256 is None
                ):
                    raise RuntimeError("terminal analysis lacks a sealed snapshot")
                return result
    raise RuntimeError("analysis synchronization exceeded its step budget")


def run_publication(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    policy: VNextResolvedIngestPolicy,
    library: MemoryLibrary,
    *,
    step_budget: int = 10_000,
    boundary: Boundary = None,
) -> VNextIngestAdvanceResult:
    adapters = {library.adapter_id: library}
    for _ in range(step_budget):
        _notify(boundary, "publication.issue")
        issued = facade.issue_publication_step(session, policy)
        prepared = facade.prepare_publication_step(
            issued,
            artifact_adapters=adapters,
            finalization_adapters=adapters,
            library_activation=library,
        )
        with prepared:
            _notify(boundary, f"publication.commit:{issued.operation}")
            result = facade.commit_publication_step(session, prepared)
        if result.phase not in {
            VNextIngestPhase.PUBLICATION,
            VNextIngestPhase.FINALIZATION,
        }:
            raise RuntimeError("publication advancement returned another phase")
        if result.terminal:
            if result.phase is not VNextIngestPhase.FINALIZATION:
                raise RuntimeError("terminal publication is not finalized")
            return result
    raise RuntimeError("publication synchronization exceeded its step budget")


def run_publication_recovery(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    library: MemoryLibrary,
    *,
    step_budget: int = 10_000,
    boundary: Boundary = None,
) -> VNextIngestAdvanceResult | None:
    """Reconcile one durable commit without binding a source build."""

    adapters = {library.adapter_id: library}
    for _ in range(step_budget):
        _notify(boundary, "publication-recovery.issue")
        issued = facade.try_issue_publication_recovery_step(session)
        if issued is None:
            return None
        prepared = facade.prepare_publication_step(
            issued,
            artifact_adapters=adapters,
            finalization_adapters=adapters,
            library_activation=library,
        )
        with prepared:
            _notify(boundary, f"publication-recovery.commit:{issued.operation}")
            result = facade.commit_publication_step(session, prepared)
        if result.phase is not VNextIngestPhase.FINALIZATION:
            raise RuntimeError("publication recovery returned another phase")
        if result.terminal:
            return result
    raise RuntimeError("publication recovery exceeded its step budget")


def claim_session(
    facade: VNextIngestFacade,
    *,
    periodic: bool = True,
    lease: int = LEASE_MICROSECONDS,
    attempts: int = 64,
) -> VNextIngestSession:
    """Claim like the resident does: drain pending current-only maintenance
    between refused claims instead of treating a refusal as failure."""

    for _ in range(attempts):
        session = facade.try_claim_ingest(periodic, lease)
        if session is not None:
            return session
        drain_maintenance(facade)
    raise RuntimeError("ingest session was not available after maintenance")


def run_ingest_turn(
    facade: VNextIngestFacade,
    *,
    source: MemorySource,
    library: MemoryLibrary,
    policy: VNextIngestPolicy | None = None,
    periodic: bool = True,
    max_rows: int = 128,
    session: VNextIngestSession | None = None,
    boundary: Boundary = None,
) -> IngestTurnReceipts:
    """Drive one complete source-to-finalization turn and complete ingest.

    ``boundary`` is invoked with a label immediately before every facade call
    that opens a fenced write transaction; tests use it to inject concurrent
    authority changes at every mutation boundary.
    """

    if session is None:
        _notify(boundary, "claim")
        session = claim_session(facade, periodic=periodic)
    _notify(boundary, "ensure_policy")
    resolved = facade.ensure_policy(session, policy or ingest_policy())
    run_publication_recovery(
        facade,
        session,
        library,
        boundary=boundary,
    )
    source_receipt = run_source(facade, session, resolved, source, boundary=boundary)
    analysis = run_analysis(
        facade,
        session,
        resolved,
        source_receipt.build_id,
        max_rows=max_rows,
        boundary=boundary,
    )
    publication = run_publication(facade, session, resolved, library, boundary=boundary)
    _notify(boundary, "complete")
    completion = facade.complete_ingest(session)
    return IngestTurnReceipts(
        session,
        resolved,
        source_receipt,
        analysis,
        publication,
        completion,
    )


def drain_maintenance(
    facade: VNextIngestFacade,
    *,
    attempts: int = 256,
    boundary: Boundary = None,
) -> int:
    """Advance current-only maintenance to DONE; return progressed cycles."""

    progressed = 0
    for _ in range(attempts):
        _notify(boundary, "maintenance")
        outcome = facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            return progressed
        if outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
            progressed += 1
            continue
        raise RuntimeError(f"maintenance did not progress: {outcome}")
    raise RuntimeError("maintenance did not reach DONE within its attempt budget")


def initialize_database(config: CoreConfig) -> SchemaEpochReport:
    report = VNextDatabaseAdminFacade(config).initialize()
    if report.state != "READY":
        raise RuntimeError("epoch initialization did not reach READY")
    return report


def full_check(config: CoreConfig) -> SchemaEpochReport:
    """Run the complete production READY audit (every wheel validator)."""

    return VNextDatabaseAdminFacade(config).check()


def _canonical(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _canonical(getattr(value, item.name))
            for item in dataclasses.fields(value)
        }
    if isinstance(value, datetime):
        return "<datetime>"
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    return value


def catalog_view(config: CoreConfig) -> dict[str, Any]:
    """Public observable catalog state, timestamp-free, for equivalence checks."""

    facade = VNextCatalogFacade(config)
    revision = facade.get_catalog_revision()
    page = facade.discover_publications(revision=revision, limit=128)
    publications: list[dict[str, Any]] = []
    for publication in page.publications:
        presentation = facade.get_publication_presentation(
            publication.publication_id,
            revision=revision,
        )
        publications.append(
            {
                "publication": _canonical(publication),
                "presentation": _canonical(presentation),
                "artifacts": [
                    _canonical(facade.get_artifact(item.artifact_id, revision=revision))
                    for item in publication.artifacts
                ],
            }
        )
    facets = {
        facet.value: _canonical(
            facade.list_publication_facets(facet=facet, revision=revision, limit=128)
        )
        for facet in CatalogFacetKind
    }
    recent = {
        order.value: _canonical(
            facade.list_recent_publications(order=order, revision=revision)
        )
        for order in CatalogRecentOrder
    }
    return {
        "revision": revision.revision,
        "publication_count": revision.publication_count,
        "artifact_count": revision.artifact_count,
        "publications": publications,
        "facets": facets,
        "recent": recent,
        "next_cursor": _canonical(page.next_cursor),
    }


def library_view(library: MemoryLibrary) -> dict[str, Any]:
    return {
        f"{gid}:{kind.value}": (descriptor.sha256, descriptor.size_bytes)
        for (gid, kind), descriptor in sorted(
            library.current.items(),
            key=lambda item: (item[0][0], item[0][1].value),
        )
    }


def stored_objects(library: MemoryLibrary) -> dict[str, tuple[str, int]]:
    return {
        "/".join(key.segments): (sha256(payload).hexdigest(), len(payload))
        for key, payload in sorted(
            library.objects.items(),
            key=lambda item: item[0].segments,
        )
    }


def cast_config(value: object) -> CoreConfig:
    return cast(CoreConfig, value)


def populate_catalog(
    config: CoreConfig,
    *,
    with_download: bool = True,
) -> tuple[MemorySource, MemoryLibrary]:
    """Build a representative populated catalog through the public facades.

    Two revisions (fresh publish, then modify/add/remove with a consumed
    deletion request), a linked download handoff, a downloader-reported missing
    gallery, and maintenance drained to its fixed point.  The result is READY.
    """

    source = MemorySource(
        [
            gallery(
                1001,
                pages=[b"p0-a", b"p1-a"],
                artists=["alice"],
                extra_tags=[("female", "glasses")],
            ),
            gallery(1002, pages=[b"p0-b"], artists=["bob"], language="japanese"),
            gallery(
                1003,
                pages=[b"p0-c", b"p1-c", b"p2-c"],
                artists=["alice", "carol"],
                other_files={b"notes.txt": b"not a page"},
                directories=(b"extras",),
            ),
        ]
    )
    library = MemoryLibrary(source)
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        run_ingest_turn(facade, source=source, library=library)
        drain_maintenance(facade)
    finally:
        facade.close()
    queue = VNextDownloadQueueFacade(config, clock=Clock())
    queue.request_deletion(1003, url="https://example.invalid/g/1003")
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"]))
    source.put(gallery(1004, pages=[b"p0-d"], artists=["dave"]))
    source.remove(("gallery-1002",))
    source.remove(("gallery-1003",))
    if with_download:
        request = queue.request_download(2001, url="https://example.invalid/g/2001")
        turn = queue.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
        source.put(gallery(2001, pages=[b"p0-e"], artists=["erin"]))
        missing = queue.request_download(2002, url="https://example.invalid/g/2002")
        queue.complete_missing_download_request_in_turn(turn, missing, 2002)
        queue.finish_download_turn(turn, request)
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            periodic=not with_download,
        )
        drain_maintenance(facade)
    finally:
        facade.close()
    if full_check(config).state != "READY":
        raise RuntimeError("populated catalog is not READY")
    return source, library
