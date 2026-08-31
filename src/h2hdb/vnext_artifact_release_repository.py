"""Bounded reconciliation of orphaned generic resource protections.

The database transaction and the external storage call are deliberately split:

* ``issue_page`` reconstructs one opaque immutable page from durable resource,
  storage-key, byte-identity, and policy facts while holding the exact
  EXCLUSIVE maintenance fence.
* ``release_page`` revalidates that fence and every page fact in a short
  transaction, commits, and only then invokes idempotent external releases.
* ``commit_page`` accepts only the acknowledgement issued after every external
  call succeeded and atomically fences each exact resource coordinate to
  ``COMMITTED``.

A lost response is safe to retry: protection tokens are opaque deterministic
32-byte digests, page cursors are exact ``(candidate, publication, kind)``
keysets, and every storage call receives the same neutral object facts.
"""

from __future__ import annotations

__all__ = [
    "ArtifactReleaseAcknowledgement",
    "ArtifactReleaseAdapter",
    "ArtifactReleaseCommitReceipt",
    "ArtifactReleaseConflictError",
    "ArtifactReleaseItem",
    "ArtifactReleasePage",
    "ArtifactReleaseRepository",
    "ArtifactReleaseRepositoryError",
    "ArtifactReleaseStorageEvidence",
    "ArtifactReleaseUnavailableError",
]

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from . import vnext_identity as identity
from .domain import (
    ArtifactReleaseStorageEvidence,
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationCursor,
)
from .ports import ArtifactReleaseAdapter
from .sql_connector import SQLConnector
from .vnext_domains import (
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_utf8_bytes,
    require_uuid16,
)
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_MAX_PAGE_ROWS = 128
_RESOURCE_CURSOR_BYTES = 33
_CURSOR_BYTES = 16 + _RESOURCE_CURSOR_BYTES
_PAGE_CAPABILITY = object()
_ACK_CAPABILITY = object()
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ArtifactReleaseRepositoryError(RuntimeError):
    """Base class for orphan-protection reconciliation failures."""


class ArtifactReleaseUnavailableError(ArtifactReleaseRepositoryError):
    """The release page, adapter, or maintenance authority is unavailable."""


class ArtifactReleaseConflictError(ArtifactReleaseRepositoryError):
    """Durable release authority differs from the repository-issued facts."""


@dataclass(frozen=True, slots=True)
class ArtifactReleaseItem:
    """Every exact durable fact needed to release one generic resource."""

    candidate_id: bytes
    publication_key: bytes
    resource_kind: CatalogResourceKind
    storage_object_key_sha256: bytes
    storage_object: StorageObjectDescriptor
    storage_generation: int
    protection_token: bytes
    adapter_id: bytes
    state: str

    def __post_init__(self) -> None:
        candidate = require_uuid16(
            self.candidate_id,
            field="artifact release candidate_id",
        )
        publication = require_digest32(
            self.publication_key,
            field="artifact release publication_key",
        )
        if type(self.resource_kind) is not CatalogResourceKind:
            raise TypeError("artifact release resource_kind is not registered")
        key_digest = require_digest32(
            self.storage_object_key_sha256,
            field="artifact release storage_object_key_sha256",
        )
        if not isinstance(self.storage_object, StorageObjectDescriptor):
            raise TypeError("artifact release storage_object is not registered")
        self.storage_object.__post_init__()
        if (
            identity.artifact_storage_key_digest(
                self.storage_object.key.codec,
                self.storage_object.key.segments,
            )
            != key_digest
        ):
            raise ValueError("artifact release storage-key digest disagrees")
        generation = require_int63(
            self.storage_generation,
            field="artifact release storage_generation",
        )
        token = require_digest32(
            self.protection_token,
            field="artifact release protection_token",
        )
        try:
            decoded = identity.decode_artifact_protection_token(token)
            expected = identity.encode_artifact_protection_token(
                candidate,
                publication,
                self.resource_kind.value,
                key_digest,
                generation,
            )
        except identity.VNextIdentityError as error:
            raise ValueError(
                "artifact release protection token is malformed"
            ) from error
        if decoded != token or token != expected:
            raise ValueError("artifact release token disagrees with durable facts")
        require_ascii_bytes(
            self.adapter_id,
            field="artifact release adapter_id",
            minimum=1,
            maximum=64,
        )
        _require_state(self.state)

    @property
    def coordinate(self) -> VNextLibraryActivationCursor:
        return VNextLibraryActivationCursor(
            self.publication_key,
            self.resource_kind,
        )

    @property
    def cursor(self) -> bytes:
        return self.candidate_id + self.coordinate.to_bytes()

    @property
    def immutable_facts(self) -> tuple[object, ...]:
        return (
            self.candidate_id,
            self.publication_key,
            self.resource_kind,
            self.storage_object_key_sha256,
            self.storage_object,
            self.storage_generation,
            self.protection_token,
            self.adapter_id,
        )


@dataclass(frozen=True, slots=True)
class ArtifactReleasePage:
    """Opaque bounded tx1 result, reproducible from sealed resource facts."""

    gate_lease: GateLease
    start_cursor: bytes
    next_cursor: bytes
    page_limit: int
    items: tuple[ArtifactReleaseItem, ...]
    terminal: bool
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _PAGE_CAPABILITY:
            raise TypeError("artifact release pages are repository-issued")
        _require_exclusive_lease_shape(self.gate_lease)
        start = _require_cursor(self.start_cursor)
        next_cursor = _require_cursor(self.next_cursor)
        limit = _require_page_limit(self.page_limit)
        if not isinstance(self.items, tuple):
            raise TypeError("artifact release page items must be a tuple")
        if len(self.items) > limit:
            raise ValueError("artifact release page exceeds its fixed bound")
        cursors: list[bytes] = []
        for item in self.items:
            if not isinstance(item, ArtifactReleaseItem):
                raise TypeError("artifact release page contains a foreign item")
            item.__post_init__()
            if item.state not in {"PENDING", "PREPARED"}:
                raise ValueError("artifact release page contains a terminal item")
            cursors.append(item.cursor)
        ordered = tuple(cursors)
        if ordered != tuple(sorted(set(ordered))):
            raise ValueError("artifact release page keys must be unique and ordered")
        if ordered and start and ordered[0] <= start:
            raise ValueError("artifact release page did not advance its cursor")
        expected_next = start if not ordered else ordered[-1]
        if next_cursor != expected_next:
            raise ValueError("artifact release page next cursor is not server-derived")
        if type(self.terminal) is not bool or self.terminal != (not ordered):
            raise ValueError("artifact release page terminal marker disagrees")


@dataclass(frozen=True, slots=True)
class ArtifactReleaseAcknowledgement:
    """Opaque repository acknowledgement issued after all external releases."""

    page: ArtifactReleasePage
    released_tokens: tuple[bytes, ...]
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _ACK_CAPABILITY:
            raise TypeError("artifact release acknowledgements are repository-issued")
        page = _require_page(self.page)
        expected = tuple(item.protection_token for item in page.items)
        if self.released_tokens != expected:
            raise ValueError("artifact release acknowledgement token set disagrees")


@dataclass(frozen=True, slots=True)
class ArtifactReleaseCommitReceipt:
    """tx2 outcome for one acknowledged release page."""

    start_cursor: bytes
    next_cursor: bytes
    row_count: int
    transitioned_count: int
    replayed: bool

    def __post_init__(self) -> None:
        _require_cursor(self.start_cursor)
        _require_cursor(self.next_cursor)
        rows = require_int63(self.row_count, field="artifact release row_count")
        transitioned = require_int63(
            self.transitioned_count,
            field="artifact release transitioned_count",
        )
        if transitioned > rows:
            raise ValueError("artifact release transition count exceeds its page")
        if type(self.replayed) is not bool:
            raise TypeError("artifact release replayed must be bool")
        if self.replayed != (transitioned == 0):
            raise ValueError("artifact release replay marker disagrees")


@dataclass(frozen=True, slots=True)
class _ReleaseHeader:
    candidate_id: bytes
    publication_key: bytes
    resource_kind: CatalogResourceKind
    storage_object_key_sha256: bytes
    storage_generation: int
    protection_token: bytes
    state: str
    storage_object_sha256: bytes
    size_bytes: int
    modified_at: int
    key_codec: str
    segment_count: int
    adapter_id: bytes


class ArtifactReleaseRepository:
    """Reconcile active protection tokens owned by inactive unpublished work."""

    @staticmethod
    def issue_page(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cursor: bytes = b"",
        page_limit: int = _MAX_PAGE_ROWS,
        now: int,
    ) -> ArtifactReleasePage:
        """Return one bounded page without calling storage or writing state."""

        start = _require_cursor(cursor)
        bound = _require_page_limit(page_limit)
        timestamp = require_int63(now, field="artifact release issue now")
        _require_exclusive_gate(work, gate_lease, now=timestamp)

        cursor_clause = ""
        parameters: tuple[object, ...] = ()
        if start:
            after_candidate, after_resource = _decode_cursor(start)
            _require_cursor_authority(
                work.connector,
                candidate_id=after_candidate,
                resource=after_resource,
            )
            after_kind = after_resource.resource_kind.value.encode("ascii")
            cursor_clause = (
                "AND (prepared.candidate_id > %s OR "
                "(prepared.candidate_id = %s AND ("
                "prepared.publication_key > %s OR "
                "(prepared.publication_key = %s "
                "AND prepared.resource_kind > %s)))) "
            )
            parameters = (
                after_candidate,
                after_candidate,
                after_resource.publication_key,
                after_resource.publication_key,
                after_kind,
            )
        rows = work.connector.fetch_all(
            _RELEASE_SELECT
            + _RELEASE_JOINS
            + "WHERE prepared.state IN ('PENDING', 'PREPARED') "
            + _CANDIDATE_ELIGIBILITY
            + cursor_clause
            + "ORDER BY prepared.candidate_id, prepared.publication_key, "
            "prepared.resource_kind LIMIT %s",
            (*parameters, bound),
        )
        try:
            items = _items_from_rows(work.connector, rows)
        except (TypeError, UnicodeError, ValueError) as error:
            raise ArtifactReleaseConflictError(
                "artifact release page contains incomplete or corrupt durable facts"
            ) from error
        next_cursor = start if not items else items[-1].cursor
        try:
            return ArtifactReleasePage(
                gate_lease,
                start,
                next_cursor,
                bound,
                items,
                not items,
                _PAGE_CAPABILITY,
            )
        except (TypeError, ValueError) as error:
            raise ArtifactReleaseConflictError(
                "artifact release query returned inconsistent resource coordinates"
            ) from error

    @staticmethod
    def release_page(
        connector: SQLConnector,
        *,
        backend: str,
        page: ArtifactReleasePage,
        adapters: Mapping[bytes, ArtifactReleaseAdapter],
        now: int,
    ) -> ArtifactReleaseAcknowledgement:
        """Call terminal storage releases only after committed revalidation."""

        requested = _require_page(page)
        if backend not in {"sqlite", "mariadb"}:
            raise ValueError("artifact release backend is not registered")
        timestamp = require_int63(now, field="artifact release external now")
        resolved = _resolve_adapters(adapters, requested.items)

        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _require_exclusive_gate(work, requested.gate_lease, now=timestamp)
            _revalidate_page(connector, requested)

        released: list[bytes] = []
        for item in requested.items:
            descriptor = item.storage_object
            raw = resolved[item.adapter_id].release(
                descriptor.key,
                bytes.fromhex(descriptor.sha256),
                descriptor.size_bytes,
                item.protection_token,
            )
            if type(raw) is not ArtifactReleaseStorageEvidence or not raw.released:
                raise ArtifactReleaseUnavailableError(
                    "artifact storage adapter did not acknowledge terminal release"
                )
            released.append(item.protection_token)
        return ArtifactReleaseAcknowledgement(
            requested,
            tuple(released),
            _ACK_CAPABILITY,
        )

    @staticmethod
    def commit_page(
        work: VNextUnitOfWork,
        *,
        acknowledgement: ArtifactReleaseAcknowledgement,
        now: int,
    ) -> ArtifactReleaseCommitReceipt:
        """CAS one fully acknowledged page to COMMITTED, or replay with no DML."""

        ack = _require_acknowledgement(acknowledgement)
        page = ack.page
        timestamp = require_int63(now, field="artifact release commit now")
        _require_exclusive_gate(work, page.gate_lease, now=timestamp)

        current_items: list[ArtifactReleaseItem] = []
        candidates: set[bytes] = set()
        for expected in page.items:
            kind = expected.resource_kind.value.encode("ascii")
            state_row = work.lock_row(
                LockRank.CHILD,
                encode_lock_key(
                    "artifact-release-state",
                    expected.candidate_id,
                    expected.coordinate.to_bytes(),
                ),
                "SELECT state FROM catalog_prepared_artifacts "
                "WHERE candidate_id = %s AND publication_key = %s "
                "AND resource_kind = %s",
                (expected.candidate_id, expected.publication_key, kind),
            )
            if len(state_row) != 1:
                raise ArtifactReleaseConflictError(
                    "acknowledged prepared-resource state is absent"
                )
            current = _load_exact_item(
                work.connector,
                candidate_id=expected.candidate_id,
                publication_key=expected.publication_key,
                resource_kind=expected.resource_kind,
            )
            if current is None or current.immutable_facts != expected.immutable_facts:
                raise ArtifactReleaseConflictError(
                    "acknowledged artifact release facts changed"
                )
            if state_row[0] != current.state:
                raise ArtifactReleaseConflictError(
                    "locked artifact release state disagrees with its family"
                )
            candidates.add(expected.candidate_id)
            current_items.append(current)

        for candidate_id in sorted(candidates):
            if not _candidate_is_eligible(work.connector, candidate_id):
                raise ArtifactReleaseUnavailableError(
                    "candidate became active or published before release commit"
                )

        states = tuple(item.state for item in current_items)
        if states and all(state == "COMMITTED" for state in states):
            return _commit_receipt(page, transitioned_count=0)
        if any(state == "COMMITTED" for state in states):
            raise ArtifactReleaseConflictError(
                "artifact release page is only partially COMMITTED"
            )
        for expected, current in zip(page.items, current_items, strict=True):
            if current.state != expected.state:
                raise ArtifactReleaseConflictError(
                    "artifact release state changed after external acknowledgement"
                )

        for item in current_items:
            work.compare_and_swap(
                "UPDATE catalog_prepared_artifacts SET state = 'COMMITTED' "
                "WHERE candidate_id = %s AND publication_key = %s "
                "AND resource_kind = %s AND state = %s",
                (
                    item.candidate_id,
                    item.publication_key,
                    item.resource_kind.value.encode("ascii"),
                    item.state,
                ),
                authority="orphan resource protection release",
            )
        for item in current_items:
            updated = _load_exact_item(
                work.connector,
                candidate_id=item.candidate_id,
                publication_key=item.publication_key,
                resource_kind=item.resource_kind,
            )
            if (
                updated is None
                or updated.immutable_facts != item.immutable_facts
                or updated.state != "COMMITTED"
            ):
                raise ArtifactReleaseConflictError(
                    "artifact release state did not refine COMMITTED"
                )
        return _commit_receipt(page, transitioned_count=len(current_items))


_RELEASE_SELECT = (
    "SELECT prepared.candidate_id, prepared.publication_key, "
    "prepared.resource_kind, prepared.storage_object_key_sha256, "
    "prepared.storage_generation, prepared.protection_token, prepared.state, "
    "resource_blob.storage_object_sha256, blob_row.size_bytes, "
    "publication.modified_at, key_row.storage_object_key_sha256, "
    "key_row.key_codec, key_row.segment_count, adapter.adapter_id "
)

_RELEASE_JOINS = (
    "FROM catalog_prepared_artifacts AS prepared "
    "LEFT JOIN catalog_publication_candidates AS candidate_row "
    "ON candidate_row.candidate_id = prepared.candidate_id "
    "LEFT JOIN catalog_prepared_resource_blob AS resource_blob "
    "ON resource_blob.candidate_id = prepared.candidate_id "
    "AND resource_blob.publication_key = prepared.publication_key "
    "AND resource_blob.resource_kind = prepared.resource_kind "
    "LEFT JOIN catalog_artifact_blobs AS blob_row "
    "ON blob_row.artifact_sha256 = resource_blob.storage_object_sha256 "
    "LEFT JOIN catalog_publication_occurrence_identities AS occurrence "
    "ON occurrence.revision = candidate_row.reserved_revision "
    "AND occurrence.publication_key = prepared.publication_key "
    "LEFT JOIN catalog_publication_storage AS publication "
    "ON publication.catalog_occurrence_sha256 = "
    "occurrence.catalog_occurrence_sha256 "
    "LEFT JOIN catalog_storage_object_key_identities AS key_row "
    "ON key_row.storage_object_key_sha256 = prepared.storage_object_key_sha256 "
    "LEFT JOIN catalog_artifact_policies AS policy "
    "ON policy.artifact_policy_id = candidate_row.artifact_policy_id "
    "LEFT JOIN catalog_artifact_policy_semantics AS semantics "
    "ON semantics.policy_component_sha256 = policy.policy_component_sha256 "
    "LEFT JOIN catalog_artifact_adapter_policy AS adapter "
    "ON adapter.policy_fingerprint_sha256 = semantics.policy_fingerprint_sha256 "
)

_CANDIDATE_ELIGIBILITY = (
    "AND NOT EXISTS ("
    "SELECT 1 FROM operational_catalog_working_candidates AS working_row "
    "WHERE working_row.candidate_id = prepared.candidate_id) "
    "AND NOT EXISTS ("
    "SELECT 1 FROM catalog_publication_commits AS commit_row "
    "WHERE commit_row.candidate_id = prepared.candidate_id) "
)


def _items_from_rows(
    connector: SQLConnector,
    rows: list[tuple[Any, ...]],
) -> tuple[ArtifactReleaseItem, ...]:
    headers: list[_ReleaseHeader] = []
    key_counts: dict[bytes, int] = {}
    for row in rows:
        if len(row) != 14 or any(value is None for value in row):
            raise ValueError("artifact release family row has an invalid shape")
        candidate = require_uuid16(row[0], field="artifact release candidate_id")
        publication = require_digest32(
            row[1],
            field="artifact release publication_key",
        )
        kind = _resource_kind(row[2])
        key_digest = require_digest32(
            row[3],
            field="artifact release storage_object_key_sha256",
        )
        if (
            require_digest32(row[10], field="artifact release key identity digest")
            != key_digest
        ):
            raise ValueError("artifact release storage key identity is noncongruent")
        codec = require_ascii_bytes(
            row[11],
            field="artifact release storage key codec",
            minimum=1,
            maximum=64,
        ).decode("ascii")
        segment_count = require_positive_int63(
            row[12],
            field="artifact release storage key segment_count",
        )
        if segment_count > 16:
            raise ValueError("artifact release storage key has too many segments")
        previous = key_counts.setdefault(key_digest, segment_count)
        if previous != segment_count:
            raise ValueError("artifact release storage key counts conflict")
        state = _require_state(row[6])
        headers.append(
            _ReleaseHeader(
                candidate,
                publication,
                kind,
                key_digest,
                require_int63(
                    row[4],
                    field="artifact release storage_generation",
                ),
                require_digest32(
                    row[5],
                    field="artifact release protection_token",
                ),
                state,
                require_digest32(
                    row[7],
                    field="artifact release storage object sha256",
                ),
                require_positive_int63(
                    row[8],
                    field="artifact release storage object size",
                ),
                require_int63(
                    row[9],
                    field="artifact release storage object modified_at",
                ),
                codec,
                segment_count,
                require_ascii_bytes(
                    row[13],
                    field="artifact release adapter_id",
                    minimum=1,
                    maximum=64,
                ),
            )
        )

    segments = _load_key_segments(connector, key_counts)
    items: list[ArtifactReleaseItem] = []
    for header in headers:
        exact_segments = segments.get(header.storage_object_key_sha256, ())
        if len(exact_segments) != header.segment_count:
            raise ValueError("artifact release storage key family is incomplete")
        key = StorageObjectKey(header.key_codec, exact_segments)
        if (
            identity.artifact_storage_key_digest(key.codec, key.segments)
            != header.storage_object_key_sha256
        ):
            raise ValueError("artifact release storage key digest disagrees")
        try:
            modified_at = _EPOCH + timedelta(microseconds=header.modified_at)
        except OverflowError as error:
            raise ValueError(
                "artifact release storage object modified_at is out of range"
            ) from error
        descriptor = StorageObjectDescriptor(
            key,
            header.size_bytes,
            header.storage_object_sha256.hex(),
            modified_at,
        )
        items.append(
            ArtifactReleaseItem(
                header.candidate_id,
                header.publication_key,
                header.resource_kind,
                header.storage_object_key_sha256,
                descriptor,
                header.storage_generation,
                header.protection_token,
                header.adapter_id,
                header.state,
            )
        )
    return tuple(items)


def _load_key_segments(
    connector: SQLConnector,
    key_counts: dict[bytes, int],
) -> dict[bytes, tuple[str, ...]]:
    if not key_counts:
        return {}
    digests = tuple(sorted(key_counts))
    placeholders = ", ".join("%s" for _digest in digests)
    rows = connector.fetch_all(
        "SELECT storage_object_key_sha256, segment_position, key_segment "
        "FROM catalog_storage_object_key_segments "
        f"WHERE storage_object_key_sha256 IN ({placeholders}) "
        "ORDER BY storage_object_key_sha256, segment_position",
        digests,
    )
    grouped: dict[bytes, list[str]] = {}
    for row in rows:
        if len(row) != 3:
            raise ValueError("artifact release storage key segment is malformed")
        digest = require_digest32(row[0], field="artifact release key segment digest")
        if digest not in key_counts:
            raise ValueError("artifact release storage key segment is unexpected")
        position = require_int63(
            row[1],
            field="artifact release key segment position",
        )
        current = grouped.setdefault(digest, [])
        if position != len(current):
            raise ValueError("artifact release storage key segments are not dense")
        current.append(
            require_utf8_bytes(
                row[2],
                field="artifact release storage key segment",
                minimum=1,
                maximum=255,
                reject_nul=True,
            ).decode("utf-8")
        )
    return {digest: tuple(values) for digest, values in grouped.items()}


def _load_exact_item(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
) -> ArtifactReleaseItem | None:
    candidate = require_uuid16(candidate_id, field="artifact release candidate_id")
    publication = require_digest32(
        publication_key,
        field="artifact release publication_key",
    )
    if type(resource_kind) is not CatalogResourceKind:
        raise TypeError("artifact release resource_kind is not registered")
    rows = connector.fetch_all(
        _RELEASE_SELECT
        + _RELEASE_JOINS
        + "WHERE prepared.candidate_id = %s AND prepared.publication_key = %s "
        "AND prepared.resource_kind = %s LIMIT 2",
        (candidate, publication, resource_kind.value.encode("ascii")),
    )
    if not rows:
        return None
    try:
        items = _items_from_rows(connector, rows)
    except (TypeError, UnicodeError, ValueError) as error:
        raise ArtifactReleaseConflictError(
            "prepared artifact release family is partial or corrupt"
        ) from error
    if len(items) != 1:
        raise ArtifactReleaseConflictError(
            "prepared artifact release coordinate is duplicated"
        )
    return items[0]


def _revalidate_page(connector: SQLConnector, page: ArtifactReleasePage) -> None:
    candidates: set[bytes] = set()
    for item in page.items:
        current = _load_exact_item(
            connector,
            candidate_id=item.candidate_id,
            publication_key=item.publication_key,
            resource_kind=item.resource_kind,
        )
        if current != item:
            raise ArtifactReleaseConflictError(
                "artifact release page changed before external release"
            )
        candidates.add(item.candidate_id)
    for candidate_id in sorted(candidates):
        if not _candidate_is_eligible(connector, candidate_id):
            raise ArtifactReleaseUnavailableError(
                "candidate became active or published before external release"
            )


def _require_cursor_authority(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    resource: VNextLibraryActivationCursor,
) -> None:
    current = _load_exact_item(
        connector,
        candidate_id=candidate_id,
        publication_key=resource.publication_key,
        resource_kind=resource.resource_kind,
    )
    if current is None or current.state != "COMMITTED":
        raise ArtifactReleaseConflictError(
            "artifact release cursor does not name one committed resource"
        )
    kind = resource.resource_kind.value.encode("ascii")
    skipped = connector.fetch_one(
        "SELECT prepared.candidate_id FROM catalog_prepared_artifacts AS prepared "
        "WHERE prepared.state IN ('PENDING', 'PREPARED') "
        + _CANDIDATE_ELIGIBILITY
        + "AND (prepared.candidate_id < %s OR "
        "(prepared.candidate_id = %s AND ("
        "prepared.publication_key < %s OR "
        "(prepared.publication_key = %s "
        "AND prepared.resource_kind <= %s)))) LIMIT 1",
        (
            candidate_id,
            candidate_id,
            resource.publication_key,
            resource.publication_key,
            kind,
        ),
    )
    if skipped:
        raise ArtifactReleaseConflictError(
            "artifact release cursor skips an active earlier resource"
        )


def _candidate_is_eligible(connector: SQLConnector, candidate_id: bytes) -> bool:
    candidate = require_uuid16(candidate_id, field="artifact release candidate_id")
    row = connector.fetch_one(
        "SELECT candidate_row.candidate_id "
        "FROM catalog_publication_candidates AS candidate_row "
        "WHERE candidate_row.candidate_id = %s "
        "AND NOT EXISTS ("
        "SELECT 1 FROM operational_catalog_working_candidates AS working_row "
        "WHERE working_row.candidate_id = candidate_row.candidate_id) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM catalog_publication_commits AS commit_row "
        "WHERE commit_row.candidate_id = candidate_row.candidate_id)",
        (candidate,),
    )
    return row == (candidate,)


def _resolve_adapters(
    adapters: Mapping[bytes, ArtifactReleaseAdapter],
    items: tuple[ArtifactReleaseItem, ...],
) -> dict[bytes, ArtifactReleaseAdapter]:
    if not isinstance(adapters, Mapping):
        raise TypeError("artifact release adapters must be a mapping")
    resolved: dict[bytes, ArtifactReleaseAdapter] = {}
    for adapter_id in sorted({item.adapter_id for item in items}):
        try:
            adapter = adapters[adapter_id]
        except KeyError as error:
            raise ArtifactReleaseUnavailableError(
                "artifact release adapter is not installed"
            ) from error
        actual_id = require_ascii_bytes(
            adapter.adapter_id,
            field="artifact release adapter_id",
            minimum=1,
            maximum=64,
        )
        if actual_id != adapter_id or not callable(getattr(adapter, "release", None)):
            raise ArtifactReleaseUnavailableError(
                "artifact release adapter differs from the sealed policy registry"
            )
        resolved[adapter_id] = adapter
    return resolved


def _commit_receipt(
    page: ArtifactReleasePage,
    *,
    transitioned_count: int,
) -> ArtifactReleaseCommitReceipt:
    return ArtifactReleaseCommitReceipt(
        page.start_cursor,
        page.next_cursor,
        len(page.items),
        transitioned_count,
        transitioned_count == 0,
    )


def _require_page(page: ArtifactReleasePage) -> ArtifactReleasePage:
    if not isinstance(page, ArtifactReleasePage):
        raise TypeError("page must be an ArtifactReleasePage")
    page.__post_init__()
    return page


def _require_acknowledgement(
    acknowledgement: ArtifactReleaseAcknowledgement,
) -> ArtifactReleaseAcknowledgement:
    if not isinstance(acknowledgement, ArtifactReleaseAcknowledgement):
        raise TypeError("acknowledgement must be an ArtifactReleaseAcknowledgement")
    acknowledgement.__post_init__()
    return acknowledgement


def _require_exclusive_gate(
    work: VNextUnitOfWork,
    lease: GateLease,
    *,
    now: int,
) -> None:
    current = MaintenanceGateRepository.lock_and_require_live(work, lease, now=now)
    if current.mode != GateMode.EXCLUSIVE or current.slots != tuple(range(64)):
        raise ArtifactReleaseUnavailableError(
            "artifact release requires the exact EXCLUSIVE maintenance gate"
        )


def _require_exclusive_lease_shape(lease: GateLease) -> None:
    if not isinstance(lease, GateLease):
        raise TypeError("artifact release gate lease must be a GateLease")
    lease.__post_init__()
    if lease.mode != GateMode.EXCLUSIVE or lease.slots != tuple(range(64)):
        raise ValueError("artifact release page lacks an EXCLUSIVE gate lease")


def _require_cursor(cursor: object) -> bytes:
    value = require_bounded_bytes(
        cursor,
        field="artifact release cursor",
        maximum=_CURSOR_BYTES,
    )
    if not value:
        return value
    if len(value) != _CURSOR_BYTES:
        raise ValueError("artifact release cursor must be empty or exactly 49 bytes")
    require_uuid16(value[:16], field="artifact release cursor candidate_id")
    resource = VNextLibraryActivationCursor.from_bytes(value[16:])
    if value != value[:16] + resource.to_bytes():
        raise ValueError("artifact release cursor is not canonical")
    return value


def _decode_cursor(
    cursor: bytes,
) -> tuple[bytes, VNextLibraryActivationCursor]:
    value = _require_cursor(cursor)
    if not value:
        raise ValueError("the initial artifact release cursor has no key")
    return value[:16], VNextLibraryActivationCursor.from_bytes(value[16:])


def _require_page_limit(page_limit: object) -> int:
    value = require_positive_int63(page_limit, field="artifact release page_limit")
    if value > _MAX_PAGE_ROWS:
        raise ValueError(f"artifact release pages are capped at {_MAX_PAGE_ROWS} rows")
    return value


def _resource_kind(value: object) -> CatalogResourceKind:
    raw = require_ascii_bytes(
        value,
        field="artifact release resource_kind",
        minimum=1,
        maximum=11,
    )
    return CatalogResourceKind(raw.decode("ascii"))


def _require_state(state: object) -> str:
    if not isinstance(state, str):
        raise TypeError("artifact release state must be exact text")
    if state not in {"PENDING", "PREPARED", "COMMITTED"}:
        raise ValueError("artifact release state is not registered")
    return state
