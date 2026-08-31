"""Permanent receipt-owned publication artifact finalization.

The storage operation is deliberately outside every database transaction:

* :meth:`PublicationFinalizationRepository.issue_page` owns one read-only
  snapshot and returns an opaque page reconstructed from the commit-owned
  checkpoint and immutable PREPARED token facts.
* :meth:`PublicationFinalizationRepository.release_page` revalidates the exact
  page in a short transaction, commits, and only then calls terminal,
  idempotent storage release operations.
* :meth:`PublicationFinalizationRepository.commit_page` consumes only the
  repository-issued acknowledgement.  It locks and revalidates the checkpoint,
  advances the complete page atomically, and retains the current batch receipt.

There is intentionally no issuance relation.  A lost issue response is rebuilt
from the unchanged checkpoint, while the current lost commit response is
returned from ``(receipt_id, batch_key)`` before any transient authority,
candidate fact, or live lease is consulted.  Once an exact successor is
durable, its safely acknowledged predecessor is pruned and becomes stale.
"""

from __future__ import annotations

__all__ = [
    "PublicationFinalizationAcknowledgement",
    "PublicationFinalizationAdapter",
    "PublicationFinalizationBatchReceipt",
    "PublicationFinalizationConflictError",
    "PublicationFinalizationCorruptionError",
    "PublicationFinalizationItem",
    "PublicationFinalizationPage",
    "PublicationFinalizationRepository",
    "PublicationFinalizationRepositoryError",
    "PublicationFinalizationStorageEvidence",
    "PublicationFinalizationUnavailableError",
]

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from . import vnext_identity as identity
from .domain import (
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationCursor,
)
from .sql_connector import SQLConnector
from .vnext_artifact_release_repository import (
    ArtifactReleaseAdapter,
    ArtifactReleaseStorageEvidence,
)
from .vnext_domains import (
    INT63_MAX,
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
    MaintenanceGateCorruptionError,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from .vnext_transaction import (
    LockRank,
    StaleWriteError,
    VNextUnitOfWork,
    encode_lock_key,
)

PublicationFinalizationAdapter = ArtifactReleaseAdapter
PublicationFinalizationStorageEvidence = ArtifactReleaseStorageEvidence

_MAX_PAGE_ROWS = 128
_MAX_CURSOR_BYTES = 33
_PAGE_CAPABILITY = object()
_ACK_CAPABILITY = object()
_PRELOCKED_GATE_CAPABILITY = object()
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_CHECKPOINT_TABLE = "catalog_publication_finalization_checkpoints"

_BATCH_TABLE = "catalog_publication_finalization_batch_stored"
_BATCH_VIEW = "catalog_publication_finalization_batch_receipts"

_COMMIT_TABLE = "catalog_publication_commits"
_PUBLICATION_RECEIPT_VIEW = "catalog_publication_receipts"
_FINALIZATION_MARKER = "catalog_publication_commit_finalizations"


class PublicationFinalizationRepositoryError(RuntimeError):
    """Base class for publication-finalization protocol failures."""


class PublicationFinalizationUnavailableError(PublicationFinalizationRepositoryError):
    """A live fence, open checkpoint, storage adapter, or commit is unavailable."""


class PublicationFinalizationConflictError(PublicationFinalizationRepositoryError):
    """Durable finalization facts differ from an opaque repository result."""


class PublicationFinalizationCorruptionError(PublicationFinalizationRepositoryError):
    """A normalized finalization family is partial or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class PublicationFinalizationItem:
    """Exact immutable storage facts for one published PREPARED resource."""

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
            field="publication finalization candidate_id",
        )
        publication = require_digest32(
            self.publication_key,
            field="publication finalization publication_key",
        )
        if type(self.resource_kind) is not CatalogResourceKind:
            raise TypeError("publication finalization resource_kind is not registered")
        storage_key_sha256 = require_digest32(
            self.storage_object_key_sha256,
            field="publication finalization storage_object_key_sha256",
        )
        if not isinstance(self.storage_object, StorageObjectDescriptor):
            raise TypeError("publication finalization storage_object is not registered")
        self.storage_object.__post_init__()
        if (
            identity.artifact_storage_key_digest(
                self.storage_object.key.codec,
                self.storage_object.key.segments,
            )
            != storage_key_sha256
        ):
            raise ValueError("publication finalization storage key disagrees")
        generation = require_int63(
            self.storage_generation,
            field="publication finalization storage_generation",
        )
        token = require_digest32(
            self.protection_token,
            field="publication finalization protection_token",
        )
        require_ascii_bytes(
            self.adapter_id,
            field="publication finalization adapter_id",
            minimum=1,
            maximum=64,
        )
        if self.state != "PREPARED":
            raise ValueError("publication finalization item is not PREPARED")
        try:
            decoded = identity.decode_artifact_protection_token(token)
            expected = identity.encode_artifact_protection_token(
                candidate,
                publication,
                self.resource_kind.value,
                storage_key_sha256,
                generation,
            )
        except identity.VNextIdentityError as error:
            raise ValueError("publication finalization token is malformed") from error
        if decoded != token or token != expected:
            raise ValueError(
                "publication finalization token disagrees with durable facts"
            )

    @property
    def coordinate(self) -> VNextLibraryActivationCursor:
        return VNextLibraryActivationCursor(
            self.publication_key,
            self.resource_kind,
        )

    @property
    def cursor(self) -> bytes:
        return self.coordinate.to_bytes()

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
class PublicationFinalizationPage:
    """Opaque bounded issue result reconstructed without an issuance row."""

    gate_lease: GateLease
    receipt_id: bytes
    candidate_id: bytes
    batch_key: bytes
    start_generation: int
    start_cursor: bytes
    start_processed_count: int
    checkpoint_updated_at: int
    publication_committed_at: int
    page_limit: int
    items: tuple[PublicationFinalizationItem, ...]
    next_cursor: bytes
    terminal: bool
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _PAGE_CAPABILITY:
            raise TypeError("publication finalization pages are repository-issued")
        _require_shared_lease_shape(self.gate_lease)
        require_uuid16(
            self.receipt_id,
            field="publication finalization receipt_id",
        )
        candidate = require_uuid16(
            self.candidate_id,
            field="publication finalization candidate_id",
        )
        _require_batch_key(self.batch_key)
        require_positive_int63(
            self.start_generation,
            field="publication finalization start_generation",
        )
        start = _require_cursor(self.start_cursor, field="start_cursor")
        next_cursor = _require_cursor(self.next_cursor, field="next_cursor")
        require_int63(
            self.start_processed_count,
            field="publication finalization start_processed_count",
        )
        require_int63(
            self.checkpoint_updated_at,
            field="publication finalization checkpoint_updated_at",
        )
        require_int63(
            self.publication_committed_at,
            field="publication finalization publication_committed_at",
        )
        limit = _require_page_limit(self.page_limit)
        if not isinstance(self.items, tuple):
            raise TypeError("publication finalization page items must be a tuple")
        if len(self.items) > limit:
            raise ValueError("publication finalization page exceeds its hard bound")
        keys: list[bytes] = []
        for item in self.items:
            if not isinstance(item, PublicationFinalizationItem):
                raise TypeError("publication finalization page contains a foreign item")
            item.__post_init__()
            if item.candidate_id != candidate:
                raise ValueError("publication finalization page mixes candidates")
            keys.append(item.cursor)
        ordered = tuple(keys)
        if ordered != tuple(sorted(set(ordered))):
            raise ValueError("publication finalization page keys are not ordered")
        if ordered and start and ordered[0] <= start:
            raise ValueError("publication finalization page did not advance its cursor")
        expected_next = start if not ordered else ordered[-1]
        if next_cursor != expected_next:
            raise ValueError("publication finalization next cursor is not derived")
        if type(self.terminal) is not bool or self.terminal != (not ordered):
            raise ValueError("publication finalization terminal marker disagrees")


@dataclass(frozen=True, slots=True)
class PublicationFinalizationAcknowledgement:
    """Opaque acknowledgement issued only after every external release."""

    page: PublicationFinalizationPage
    released_tokens: tuple[bytes, ...]
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _ACK_CAPABILITY:
            raise TypeError(
                "publication finalization acknowledgements are repository-issued"
            )
        page = _require_page(self.page)
        expected = tuple(item.protection_token for item in page.items)
        if self.released_tokens != expected:
            raise ValueError("publication finalization acknowledgement disagrees")


@dataclass(frozen=True, slots=True)
class PublicationFinalizationBatchReceipt:
    """Current response reconstructed from one sealed commit-owned batch."""

    receipt_id: bytes
    batch_key: bytes
    start_generation: int
    start_cursor: bytes
    start_processed_count: int
    next_cursor: bytes
    next_processed_count: int
    next_state: str
    row_count: int
    terminal: bool
    committed_generation: int
    committed_at: int

    def __post_init__(self) -> None:
        require_uuid16(
            self.receipt_id,
            field="publication finalization receipt_id",
        )
        _require_batch_key(self.batch_key)
        start_generation = require_positive_int63(
            self.start_generation,
            field="publication finalization start_generation",
        )
        start_cursor = _require_cursor(self.start_cursor, field="start_cursor")
        next_cursor = _require_cursor(self.next_cursor, field="next_cursor")
        start_count = require_int63(
            self.start_processed_count,
            field="publication finalization start_processed_count",
        )
        next_count = require_int63(
            self.next_processed_count,
            field="publication finalization next_processed_count",
        )
        rows = require_int63(
            self.row_count,
            field="publication finalization row_count",
        )
        committed_generation = require_positive_int63(
            self.committed_generation,
            field="publication finalization committed_generation",
        )
        require_int63(
            self.committed_at,
            field="publication finalization committed_at",
        )
        if type(self.terminal) is not bool:
            raise TypeError("publication finalization terminal must be bool")
        expected_terminal = rows == 0
        expected_state = "COMPLETE" if expected_terminal else "OPEN"
        if (
            self.terminal != expected_terminal
            or self.next_state != expected_state
            or next_count != start_count + rows
            or committed_generation != start_generation + 1
        ):
            raise ValueError("publication finalization receipt is not monotone")
        if self.terminal:
            if next_cursor != start_cursor:
                raise ValueError("terminal finalization receipt changed its cursor")
        elif not next_cursor or (start_cursor and next_cursor <= start_cursor):
            raise ValueError("nonterminal finalization receipt did not advance")

    @property
    def publication_state(self) -> str:
        """Stable publication state immediately after this exact batch."""

        return "PUBLISHED" if self.terminal else "DB_COMMITTED"


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    generation: int
    cursor: bytes
    processed_count: int
    state: str
    updated_at: int


@dataclass(frozen=True, slots=True)
class _CommitContext:
    candidate_id: bytes
    committed_at: int
    state: str


class PublicationFinalizationRepository:
    """Finalize one sealed publication commit in current bounded pages."""

    @staticmethod
    def issue_page(
        connector: SQLConnector,
        *,
        backend: str,
        gate_lease: GateLease,
        receipt_id: bytes,
        batch_key: bytes,
        page_limit: int = _MAX_PAGE_ROWS,
        now: int,
    ) -> PublicationFinalizationPage:
        """Own one read transaction and issue an exact, mutation-free page."""

        _require_backend(backend)
        receipt = require_uuid16(
            receipt_id,
            field="publication finalization receipt_id",
        )
        attempt = _require_batch_key(batch_key)
        bound = _require_page_limit(page_limit)
        timestamp = require_int63(now, field="publication finalization issue now")
        with connector.read_transaction():
            _require_shared_gate_snapshot(
                connector,
                gate_lease,
                now=timestamp,
            )
            if _load_batch_by_key(connector, receipt, attempt) is not None:
                raise PublicationFinalizationConflictError(
                    "publication finalization batch_key is already committed"
                )
            context = _load_open_commit_context(connector, receipt)
            checkpoint = _load_checkpoint(connector, receipt)
            _require_open_checkpoint(checkpoint)
            if timestamp < max(context.committed_at, checkpoint.updated_at):
                raise PublicationFinalizationUnavailableError(
                    "publication finalization issue time precedes durable authority"
                )
            items = _load_page_items(
                connector,
                candidate_id=context.candidate_id,
                cursor=checkpoint.cursor,
                page_limit=bound,
            )
            next_cursor = checkpoint.cursor if not items else items[-1].cursor
            try:
                return PublicationFinalizationPage(
                    gate_lease,
                    receipt,
                    context.candidate_id,
                    attempt,
                    checkpoint.generation,
                    checkpoint.cursor,
                    checkpoint.processed_count,
                    checkpoint.updated_at,
                    context.committed_at,
                    bound,
                    items,
                    next_cursor,
                    not items,
                    _PAGE_CAPABILITY,
                )
            except (TypeError, ValueError) as error:
                raise PublicationFinalizationCorruptionError(
                    "publication finalization issue facts are inconsistent"
                ) from error

    @staticmethod
    def release_page(
        connector: SQLConnector,
        *,
        backend: str,
        page: PublicationFinalizationPage,
        adapters: Mapping[bytes, PublicationFinalizationAdapter],
        now: int,
    ) -> PublicationFinalizationAcknowledgement:
        """Revalidate, commit the DB transaction, then call storage releases."""

        _require_backend(backend)
        requested = _require_page(page)
        timestamp = require_int63(now, field="publication finalization release now")
        resolved = _resolve_adapters(adapters, requested.items)

        with connector.transaction():
            stored = _load_batch_by_key(
                connector,
                requested.receipt_id,
                requested.batch_key,
            )
            if stored is not None:
                _require_receipt_matches_page(stored, requested)
                return _acknowledge(requested)
            work = VNextUnitOfWork(connector, backend=backend)
            _require_shared_gate_locked(work, requested.gate_lease, now=timestamp)
            _revalidate_fresh_page(work, requested, now=timestamp)

        released: list[bytes] = []
        for item in requested.items:
            descriptor = item.storage_object
            evidence = resolved[item.adapter_id].release(
                descriptor.key,
                bytes.fromhex(descriptor.sha256),
                descriptor.size_bytes,
                item.protection_token,
            )
            if (
                type(evidence) is not ArtifactReleaseStorageEvidence
                or not evidence.released
            ):
                raise PublicationFinalizationUnavailableError(
                    "storage did not acknowledge terminal protection release"
                )
            released.append(item.protection_token)
        return PublicationFinalizationAcknowledgement(
            requested,
            tuple(released),
            _ACK_CAPABILITY,
        )

    @staticmethod
    def commit_page(
        work: VNextUnitOfWork,
        *,
        acknowledgement: PublicationFinalizationAcknowledgement,
        now: int,
        _prelocked_gate_capability: object | None = None,
    ) -> PublicationFinalizationBatchReceipt:
        """Commit one acknowledged page, replaying the current receipt first."""

        ack = _require_acknowledgement(acknowledgement)
        page = ack.page
        timestamp = require_int63(now, field="publication finalization commit now")

        # Current response-loss safety: no live gate, candidate row, or mutable
        # checkpoint is needed while this exact response remains the head.
        stored = _load_batch_by_key(
            work.connector,
            page.receipt_id,
            page.batch_key,
        )
        if stored is not None:
            _require_receipt_matches_page(stored, page)
            return stored
        generation_collision = _load_batch_by_generation(
            work.connector,
            page.receipt_id,
            page.start_generation,
        )
        if generation_collision is not None:
            raise PublicationFinalizationConflictError(
                "publication finalization generation already has another batch_key"
            )

        if _prelocked_gate_capability is not _PRELOCKED_GATE_CAPABILITY:
            _require_shared_gate_locked(work, page.gate_lease, now=timestamp)
        context = _load_open_commit_context(work.connector, page.receipt_id)
        if context.candidate_id != page.candidate_id:
            raise PublicationFinalizationConflictError(
                "publication finalization commit candidate changed"
            )
        checkpoint = _lock_checkpoint(work, page.receipt_id)
        _require_checkpoint_matches_page(checkpoint, page)
        if timestamp < max(
            page.publication_committed_at,
            checkpoint.updated_at,
        ):
            raise PublicationFinalizationUnavailableError(
                "publication finalization commit time precedes durable authority"
            )
        current_items = _lock_current_items(work, page)
        row_count = len(current_items)
        next_count = checkpoint.processed_count + row_count
        terminal = row_count == 0
        if terminal != page.terminal:
            raise PublicationFinalizationConflictError(
                "publication finalization page coverage changed"
            )
        if checkpoint.generation == INT63_MAX:
            raise PublicationFinalizationUnavailableError(
                "publication finalization generation is exhausted"
            )

        try:
            for item in current_items:
                work.compare_and_swap(
                    "UPDATE catalog_prepared_artifacts SET state = 'COMMITTED' "
                    "WHERE candidate_id = %s AND publication_key = %s "
                    "AND resource_kind = %s AND state = 'PREPARED'",
                    (
                        item.candidate_id,
                        item.publication_key,
                        item.resource_kind.value.encode("ascii"),
                    ),
                    authority="published artifact protection release",
                )
            _insert_batch_receipt(
                work.connector,
                page=page,
                committed_at=timestamp,
            )
            _advance_checkpoint(
                work,
                receipt_id=page.receipt_id,
                checkpoint=checkpoint,
                next_cursor=page.next_cursor,
                next_processed_count=next_count,
                next_state="COMPLETE" if terminal else "OPEN",
                updated_at=timestamp,
            )
            if checkpoint.generation > 1:
                deleted = work.connector.execute_affected(
                    f"DELETE FROM {_BATCH_TABLE} "
                    "WHERE receipt_id = %s AND start_generation = %s",
                    (page.receipt_id, checkpoint.generation - 1),
                )
                if deleted != 1:
                    raise PublicationFinalizationCorruptionError(
                        "publication finalization predecessor receipt is missing "
                        "before safe acknowledgement"
                    )
            if terminal:
                work.connector.execute(
                    f"INSERT INTO {_FINALIZATION_MARKER} (receipt_id) VALUES (%s)",
                    (page.receipt_id,),
                )
                prunable_baseline = _published_depth_zero_working_baseline(
                    work,
                    receipt_id=page.receipt_id,
                    candidate_id=page.candidate_id,
                )
                if prunable_baseline is not None:
                    analysis_id, baseline_analysis_id = prunable_baseline
                    deleted = work.connector.execute_affected(
                        "DELETE FROM catalog_analysis_baselines "
                        "WHERE analysis_id = %s AND base_analysis_id = %s",
                        (analysis_id, baseline_analysis_id),
                    )
                    if deleted != 1:
                        raise PublicationFinalizationCorruptionError(
                            "published depth-zero analysis baseline changed before "
                            "safe handoff"
                        )
        except StaleWriteError as error:
            raise PublicationFinalizationConflictError(
                "publication finalization authority changed during commit"
            ) from error

        committed = _load_batch_by_key(
            work.connector,
            page.receipt_id,
            page.batch_key,
        )
        if committed is None:
            raise PublicationFinalizationCorruptionError(
                "sealed publication finalization receipt is not visible"
            )
        _require_receipt_matches_page(committed, page)
        return committed

    @staticmethod
    def get_batch_receipt(
        connector: SQLConnector,
        *,
        receipt_id: bytes,
        batch_key: bytes | None = None,
        start_generation: int | None = None,
    ) -> PublicationFinalizationBatchReceipt | None:
        """Read the current response by either of its exact coordinates."""

        receipt = require_uuid16(
            receipt_id,
            field="publication finalization receipt_id",
        )
        if (batch_key is None) == (start_generation is None):
            raise ValueError("provide exactly one finalization receipt coordinate")
        if batch_key is not None:
            return _load_batch_by_key(
                connector,
                receipt,
                _require_batch_key(batch_key),
            )
        return _load_batch_by_generation(
            connector,
            receipt,
            require_positive_int63(
                start_generation,
                field="publication finalization start_generation",
            ),
        )


@dataclass(frozen=True, slots=True)
class _FinalizationHeader:
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


_ITEM_SELECT = (
    "SELECT prepared.candidate_id, prepared.publication_key, "
    "prepared.resource_kind, prepared.storage_object_key_sha256, "
    "prepared.storage_generation, prepared.protection_token, prepared.state, "
    "stored.storage_object_sha256, stored.size_bytes, stored.modified_at, "
    "resource_blob.storage_object_sha256, blob_row.size_bytes, "
    "key_row.storage_object_key_sha256, key_row.key_codec, "
    "key_row.segment_count, adapter.adapter_id "
)

_ITEM_JOINS = (
    "FROM catalog_prepared_artifacts AS prepared "
    "LEFT JOIN catalog_prepared_storage_objects AS stored "
    "ON stored.candidate_id = prepared.candidate_id "
    "AND stored.publication_key = prepared.publication_key "
    "AND stored.resource_kind = prepared.resource_kind "
    "LEFT JOIN catalog_prepared_resource_blob AS resource_blob "
    "ON resource_blob.candidate_id = prepared.candidate_id "
    "AND resource_blob.publication_key = prepared.publication_key "
    "AND resource_blob.resource_kind = prepared.resource_kind "
    "LEFT JOIN catalog_artifact_blobs AS blob_row "
    "ON blob_row.artifact_sha256 = resource_blob.storage_object_sha256 "
    "LEFT JOIN catalog_storage_object_key_identities AS key_row "
    "ON key_row.storage_object_key_sha256 = prepared.storage_object_key_sha256 "
    "LEFT JOIN catalog_publication_candidates AS candidate "
    "ON candidate.candidate_id = prepared.candidate_id "
    "LEFT JOIN catalog_artifact_policies AS policy "
    "ON policy.artifact_policy_id = candidate.artifact_policy_id "
    "LEFT JOIN catalog_artifact_policy_semantics AS semantics "
    "ON semantics.policy_component_sha256 = policy.policy_component_sha256 "
    "LEFT JOIN catalog_artifact_adapter_policy AS adapter "
    "ON adapter.policy_fingerprint_sha256 = semantics.policy_fingerprint_sha256 "
)


def _initialize_finalization_checkpoint(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    initialized_at: int,
) -> None:
    """Insert the total OPEN checkpoint before the publication commit row."""

    receipt = require_uuid16(
        receipt_id,
        field="publication finalization receipt_id",
    )
    timestamp = require_int63(
        initialized_at,
        field="publication finalization initialized_at",
    )
    connector.execute(
        f"INSERT INTO {_CHECKPOINT_TABLE} "
        "(receipt_id, generation, `cursor`, processed_count, state, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (receipt, 1, b"", 0, "OPEN", timestamp),
    )


def _load_page_items(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    cursor: bytes,
    page_limit: int,
) -> tuple[PublicationFinalizationItem, ...]:
    start = _require_cursor(cursor, field="page cursor")
    if start:
        coordinate = VNextLibraryActivationCursor.from_bytes(start)
        predicate = (
            "WHERE prepared.candidate_id = %s AND ("
            "prepared.publication_key > %s OR "
            "(prepared.publication_key = %s AND prepared.resource_kind > %s)) "
        )
        parameters: tuple[object, ...] = (
            candidate_id,
            coordinate.publication_key,
            coordinate.publication_key,
            coordinate.resource_kind.value.encode("ascii"),
            page_limit,
        )
    else:
        predicate = "WHERE prepared.candidate_id = %s "
        parameters = (candidate_id, page_limit)
    rows = connector.fetch_all(
        _ITEM_SELECT
        + _ITEM_JOINS
        + predicate
        + "ORDER BY prepared.publication_key, prepared.resource_kind LIMIT %s",
        parameters,
    )
    try:
        return _items_from_rows(connector, rows)
    except (TypeError, UnicodeError, ValueError) as error:
        raise PublicationFinalizationCorruptionError(
            "prepared resource finalization family is partial or invalid"
        ) from error


def _load_exact_item(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
) -> PublicationFinalizationItem | None:
    if type(resource_kind) is not CatalogResourceKind:
        raise TypeError("publication finalization resource_kind is not registered")
    rows = connector.fetch_all(
        _ITEM_SELECT
        + _ITEM_JOINS
        + "WHERE prepared.candidate_id = %s AND prepared.publication_key = %s "
        "AND prepared.resource_kind = %s LIMIT 2",
        (
            candidate_id,
            publication_key,
            resource_kind.value.encode("ascii"),
        ),
    )
    if not rows:
        return None
    try:
        items = _items_from_rows(connector, rows)
    except (TypeError, UnicodeError, ValueError) as error:
        raise PublicationFinalizationCorruptionError(
            "prepared resource finalization family is partial or invalid"
        ) from error
    if len(items) != 1:
        raise PublicationFinalizationCorruptionError(
            "prepared resource finalization coordinate is duplicated"
        )
    return items[0]


def _items_from_rows(
    connector: SQLConnector,
    rows: list[tuple[Any, ...]],
) -> tuple[PublicationFinalizationItem, ...]:
    headers: list[_FinalizationHeader] = []
    key_counts: dict[bytes, int] = {}
    for row in rows:
        if len(row) != 16 or any(value is None for value in row):
            raise ValueError("publication finalization item row has an invalid shape")
        candidate = require_uuid16(
            row[0],
            field="publication finalization candidate_id",
        )
        publication = require_digest32(
            row[1],
            field="publication finalization publication_key",
        )
        kind = _resource_kind(row[2])
        key_digest = require_digest32(
            row[3],
            field="publication finalization storage_object_key_sha256",
        )
        object_digest = require_digest32(
            row[7],
            field="publication finalization storage object sha256",
        )
        size_bytes = require_positive_int63(
            row[8],
            field="publication finalization storage object size",
        )
        if (
            require_digest32(
                row[10],
                field="publication finalization resource blob sha256",
            )
            != object_digest
            or require_positive_int63(
                row[11],
                field="publication finalization resource blob size",
            )
            != size_bytes
        ):
            raise ValueError(
                "publication finalization storage object disagrees with its blob"
            )
        if (
            require_digest32(
                row[12],
                field="publication finalization key identity digest",
            )
            != key_digest
        ):
            raise ValueError(
                "publication finalization storage key identity is noncongruent"
            )
        codec = require_ascii_bytes(
            row[13],
            field="publication finalization storage key codec",
            minimum=1,
            maximum=64,
        ).decode("ascii")
        segment_count = require_positive_int63(
            row[14],
            field="publication finalization storage key segment_count",
        )
        if segment_count > 16:
            raise ValueError(
                "publication finalization storage key has too many segments"
            )
        previous = key_counts.setdefault(key_digest, segment_count)
        if previous != segment_count:
            raise ValueError("publication finalization storage key counts conflict")
        state = row[6]
        if not isinstance(state, str):
            raise TypeError("publication finalization state must be exact text")
        headers.append(
            _FinalizationHeader(
                candidate,
                publication,
                kind,
                key_digest,
                require_int63(
                    row[4],
                    field="publication finalization storage_generation",
                ),
                require_digest32(
                    row[5],
                    field="publication finalization protection_token",
                ),
                state,
                object_digest,
                size_bytes,
                require_int63(
                    row[9],
                    field="publication finalization storage object modified_at",
                ),
                codec,
                segment_count,
                require_ascii_bytes(
                    row[15],
                    field="publication finalization adapter_id",
                    minimum=1,
                    maximum=64,
                ),
            )
        )

    segments = _load_key_segments(connector, key_counts)
    items: list[PublicationFinalizationItem] = []
    for header in headers:
        exact_segments = segments.get(header.storage_object_key_sha256, ())
        if len(exact_segments) != header.segment_count:
            raise ValueError(
                "publication finalization storage key family is incomplete"
            )
        key = StorageObjectKey(header.key_codec, exact_segments)
        if (
            identity.artifact_storage_key_digest(key.codec, key.segments)
            != header.storage_object_key_sha256
        ):
            raise ValueError("publication finalization storage key digest disagrees")
        try:
            modified_at = _EPOCH + timedelta(microseconds=header.modified_at)
        except OverflowError as error:
            raise ValueError(
                "publication finalization storage object modified_at is out of range"
            ) from error
        descriptor = StorageObjectDescriptor(
            key,
            header.size_bytes,
            header.storage_object_sha256.hex(),
            modified_at,
        )
        items.append(
            PublicationFinalizationItem(
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
            raise ValueError(
                "publication finalization storage key segment is malformed"
            )
        digest = require_digest32(
            row[0],
            field="publication finalization key segment digest",
        )
        if digest not in key_counts:
            raise ValueError(
                "publication finalization storage key segment is unexpected"
            )
        position = require_int63(
            row[1],
            field="publication finalization key segment position",
        )
        current = grouped.setdefault(digest, [])
        if position != len(current):
            raise ValueError(
                "publication finalization storage key segments are not dense"
            )
        current.append(
            require_utf8_bytes(
                row[2],
                field="publication finalization storage key segment",
                minimum=1,
                maximum=255,
                reject_nul=True,
            ).decode("utf-8")
        )
    return {digest: tuple(values) for digest, values in grouped.items()}


def _resource_kind(value: object) -> CatalogResourceKind:
    raw = require_ascii_bytes(
        value,
        field="publication finalization resource_kind",
        minimum=1,
        maximum=11,
    )
    return CatalogResourceKind(raw.decode("ascii"))


def _revalidate_fresh_page(
    work: VNextUnitOfWork,
    page: PublicationFinalizationPage,
    *,
    now: int,
) -> None:
    context = _load_open_commit_context(work.connector, page.receipt_id)
    if context.candidate_id != page.candidate_id:
        raise PublicationFinalizationConflictError(
            "publication finalization candidate changed"
        )
    checkpoint = _lock_checkpoint(work, page.receipt_id)
    _require_checkpoint_matches_page(checkpoint, page)
    if now < max(context.committed_at, checkpoint.updated_at):
        raise PublicationFinalizationUnavailableError(
            "publication finalization release time precedes durable authority"
        )
    _lock_current_items(work, page)


def _lock_current_items(
    work: VNextUnitOfWork,
    page: PublicationFinalizationPage,
) -> tuple[PublicationFinalizationItem, ...]:
    if not page.items:
        remaining_items = _load_page_items(
            work.connector,
            candidate_id=page.candidate_id,
            cursor=page.start_cursor,
            page_limit=1,
        )
        if remaining_items:
            raise PublicationFinalizationConflictError(
                "terminal publication finalization page is no longer empty"
            )
        state_row = work.connector.fetch_one(
            "SELECT state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND state <> 'COMMITTED' LIMIT 1",
            (page.candidate_id,),
        )
        if state_row:
            raise PublicationFinalizationConflictError(
                "terminal publication finalization has an unreleased resource"
            )
        return ()
    predicates = " OR ".join(
        "(publication_key = %s AND resource_kind = %s)" for _item in page.items
    )
    lock_keys = tuple(
        encode_lock_key(
            "publication-finalization-item",
            item.cursor,
        )
        for item in page.items
    )
    state_rows = work.lock_rows(
        LockRank.CHILD,
        lock_keys,
        "SELECT publication_key, resource_kind, state "
        "FROM catalog_prepared_artifacts "
        f"WHERE candidate_id = %s AND ({predicates}) "
        "ORDER BY publication_key, resource_kind",
        (
            page.candidate_id,
            *(
                value
                for item in page.items
                for value in (
                    item.publication_key,
                    item.resource_kind.value.encode("ascii"),
                )
            ),
        ),
    )
    expected_states = tuple(
        (
            item.publication_key,
            item.resource_kind.value.encode("ascii"),
            "PREPARED",
        )
        for item in page.items
    )
    if tuple(state_rows) != expected_states:
        raise PublicationFinalizationConflictError(
            "publication finalization page has mixed or changed artifact state"
        )
    current: list[PublicationFinalizationItem] = []
    for expected in page.items:
        item = _load_exact_item(
            work.connector,
            candidate_id=page.candidate_id,
            publication_key=expected.publication_key,
            resource_kind=expected.resource_kind,
        )
        if item is None or item.immutable_facts != expected.immutable_facts:
            raise PublicationFinalizationConflictError(
                "publication finalization immutable artifact facts changed"
            )
        current.append(item)
    return tuple(current)


def _load_open_commit_context(
    connector: SQLConnector,
    receipt_id: bytes,
) -> _CommitContext:
    row = connector.fetch_one(
        f"SELECT committed.candidate_id, committed.committed_at, receipt.state "
        f"FROM {_COMMIT_TABLE} AS committed "
        f"JOIN {_PUBLICATION_RECEIPT_VIEW} AS receipt "
        "ON receipt.receipt_id = committed.receipt_id "
        "WHERE committed.receipt_id = %s",
        (receipt_id,),
    )
    if len(row) != 3:
        raise PublicationFinalizationUnavailableError(
            "publication commit is absent or has an inconsistent receipt"
        )
    state = row[2]
    if state != "DB_COMMITTED":
        raise PublicationFinalizationUnavailableError(
            "publication artifact finalization is already complete"
        )
    return _CommitContext(
        require_uuid16(row[0], field="publication finalization candidate_id"),
        require_int63(row[1], field="publication finalization committed_at"),
        state,
    )


def _published_depth_zero_working_baseline(
    work: VNextUnitOfWork,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
) -> tuple[bytes, bytes] | None:
    """Return the exact disposable working baseline of one finalized compaction.

    A depth-zero result is fully materialized behind its self-only ancestry.  Its
    baseline remains necessary while the run or publication is still working,
    but retaining it after the terminal projection handoff would permanently
    pin the predecessor depth-16 chain.  This check derives every identity from
    the current commit and its durable source provenance; no caller value is
    lineage authority.
    """

    row = work.connector.fetch_one(
        "SELECT committed.candidate_id, committed.generation, "
        "candidate.analysis_id, provenance.analysis_id, run.build_id, "
        "run.policy_id, run.state, build.state, anchor.anchor_analysis_id, "
        "anchor.overlay_depth, source.snapshot_manifest_sha256, "
        "snapshot.snapshot_manifest_sha256, baseline.base_analysis_id "
        "FROM catalog_publication_commits AS committed "
        "JOIN catalog_publication_candidates AS candidate "
        "ON candidate.candidate_id = committed.candidate_id "
        "JOIN catalog_source_revision_provenance AS provenance "
        "ON provenance.source_revision = committed.source_revision "
        "JOIN catalog_analysis_runs AS run "
        "ON run.analysis_id = candidate.analysis_id "
        "JOIN catalog_source_builds AS build ON build.build_id = run.build_id "
        "JOIN catalog_analysis_state_anchors AS anchor "
        "ON anchor.analysis_id = run.analysis_id "
        "JOIN catalog_source_revision_descriptors AS source "
        "ON source.source_revision = committed.source_revision "
        "JOIN catalog_analysis_snapshot_manifest AS snapshot "
        "ON snapshot.analysis_id = run.analysis_id "
        "LEFT JOIN catalog_analysis_baselines AS baseline "
        "ON baseline.analysis_id = run.analysis_id "
        "WHERE committed.receipt_id = %s",
        (receipt_id,),
    )
    if len(row) != 13:
        raise PublicationFinalizationCorruptionError(
            "terminal publication lacks one complete analysis lineage"
        )
    committed_candidate = require_uuid16(
        row[0],
        field="published baseline commit candidate_id",
    )
    generation = require_positive_int63(
        row[1],
        field="published baseline commit generation",
    )
    analysis_id = require_uuid16(
        row[2],
        field="published baseline candidate analysis_id",
    )
    provenance_analysis_id = require_uuid16(
        row[3],
        field="published baseline provenance analysis_id",
    )
    build_id = require_uuid16(row[4], field="published baseline build_id")
    policy_id = require_positive_int63(
        row[5],
        field="published baseline policy_id",
    )
    anchor_analysis_id = require_uuid16(
        row[8],
        field="published baseline anchor_analysis_id",
    )
    overlay_depth = require_int63(
        row[9],
        field="published baseline overlay_depth",
    )
    source_snapshot = require_digest32(
        row[10],
        field="published baseline source snapshot",
    )
    analysis_snapshot = require_digest32(
        row[11],
        field="published baseline analysis snapshot",
    )
    if (
        committed_candidate != candidate_id
        or analysis_id != provenance_analysis_id
        or row[6] != "COMPLETE"
        or row[7] != "SEALED"
        or overlay_depth > 16
        or source_snapshot != analysis_snapshot
    ):
        raise PublicationFinalizationCorruptionError(
            "terminal publication analysis lineage is not one sealed bounded result"
        )
    current_ancestry = _require_exact_published_analysis_ancestry(
        work,
        analysis_id=analysis_id,
        expected_depth=overlay_depth,
    )
    if anchor_analysis_id != current_ancestry[-1]:
        raise PublicationFinalizationCorruptionError(
            "terminal publication analysis anchor differs from its exact ancestry"
        )
    _require_exact_published_analysis_components(work, analysis_id=analysis_id)

    if overlay_depth > 0:
        if row[12] is None:
            raise PublicationFinalizationCorruptionError(
                "positive-depth publication lost its ancestry baseline"
            )
        baseline_analysis_id = require_uuid16(
            row[12],
            field="published positive-depth base_analysis_id",
        )
        if baseline_analysis_id != current_ancestry[1]:
            raise PublicationFinalizationCorruptionError(
                "positive-depth publication baseline is not its immediate ancestor"
            )
        return None

    if anchor_analysis_id != analysis_id:
        raise PublicationFinalizationCorruptionError(
            "published depth-zero analysis is not its own anchor"
        )

    if row[12] is None:
        base_rows = work.connector.fetch_all(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s LIMIT 2",
            (build_id,),
        )
        if generation != 1 or base_rows:
            raise PublicationFinalizationCorruptionError(
                "non-genesis depth-zero publication lost its working baseline"
            )
        return None

    baseline_analysis_id = require_uuid16(
        row[12],
        field="published baseline base_analysis_id",
    )
    if baseline_analysis_id == analysis_id:
        raise PublicationFinalizationCorruptionError(
            "published depth-zero baseline cycles to itself"
        )
    baseline_row = work.connector.fetch_one(
        "SELECT run.policy_id, run.state, anchor.anchor_analysis_id, "
        "anchor.overlay_depth FROM catalog_analysis_runs AS run "
        "JOIN catalog_analysis_state_anchors AS anchor "
        "ON anchor.analysis_id = run.analysis_id "
        "WHERE run.analysis_id = %s",
        (baseline_analysis_id,),
    )
    if len(baseline_row) != 4:
        raise PublicationFinalizationCorruptionError(
            "published depth-zero baseline analysis is incomplete"
        )
    baseline_policy_id = require_positive_int63(
        baseline_row[0],
        field="published baseline predecessor policy_id",
    )
    baseline_anchor = require_uuid16(
        baseline_row[2],
        field="published baseline predecessor anchor",
    )
    baseline_depth = require_int63(
        baseline_row[3],
        field="published baseline predecessor depth",
    )
    if (
        baseline_row[1] != "COMPLETE"
        or baseline_depth > 16
        or (baseline_policy_id == policy_id and baseline_depth != 16)
    ):
        raise PublicationFinalizationCorruptionError(
            "published depth-zero baseline is not one legal sealed compaction parent"
        )
    ancestry = _require_exact_published_analysis_ancestry(
        work,
        analysis_id=baseline_analysis_id,
        expected_depth=baseline_depth,
    )
    if baseline_anchor != ancestry[-1] or analysis_id in ancestry:
        raise PublicationFinalizationCorruptionError(
            "published depth-zero baseline ancestry is inconsistent"
        )
    _require_exact_published_analysis_components(
        work,
        analysis_id=baseline_analysis_id,
    )
    return analysis_id, baseline_analysis_id


def _require_exact_published_analysis_ancestry(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    expected_depth: int,
) -> tuple[bytes, ...]:
    rows = work.connector.fetch_all(
        "SELECT ancestor_depth, ancestor_analysis_id "
        "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
        "ORDER BY ancestor_depth LIMIT 18",
        (analysis_id,),
    )
    ancestry = tuple(
        require_uuid16(row[1], field="published baseline ancestor_analysis_id")
        for row in rows
    )
    if (
        len(rows) != expected_depth + 1
        or not ancestry
        or ancestry[0] != analysis_id
        or len(set(ancestry)) != len(ancestry)
        or any(
            require_int63(row[0], field="published baseline ancestor_depth") != depth
            for depth, row in enumerate(rows)
        )
    ):
        raise PublicationFinalizationCorruptionError(
            "published analysis ancestry is not one exact bounded suffix"
        )
    return ancestry


def _require_exact_published_analysis_components(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
) -> None:
    rows = work.connector.fetch_all(
        "SELECT state_component FROM catalog_analysis_state_component_seals "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT 6",
        (analysis_id,),
    )
    expected = frozenset(
        component.encode("ascii") for component in identity.ANALYSIS_STATE_COMPONENTS
    )
    if len(rows) != len(expected) or {row[0] for row in rows} != expected:
        raise PublicationFinalizationCorruptionError(
            "published analysis lacks its exact five component seals"
        )


def _load_checkpoint(connector: SQLConnector, receipt_id: bytes) -> _Checkpoint:
    row = connector.fetch_one(
        f"SELECT generation, `cursor`, processed_count, state, updated_at "
        f"FROM {_CHECKPOINT_TABLE} WHERE receipt_id = %s",
        (receipt_id,),
    )
    return _checkpoint_from_row(row)


def _lock_checkpoint(work: VNextUnitOfWork, receipt_id: bytes) -> _Checkpoint:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication-finalization-checkpoint", receipt_id),
        "SELECT generation, `cursor`, processed_count, state, updated_at "
        f"FROM {_CHECKPOINT_TABLE} WHERE receipt_id = %s",
        (receipt_id,),
    )
    return _checkpoint_from_row(row)


def _checkpoint_from_row(row: tuple[object, ...]) -> _Checkpoint:
    if len(row) != 5:
        raise PublicationFinalizationCorruptionError(
            "publication finalization checkpoint is absent or partial"
        )
    generation = require_positive_int63(
        row[0],
        field="publication finalization checkpoint generation",
    )
    cursor = _require_cursor(row[1], field="checkpoint cursor")
    count = require_int63(
        row[2],
        field="publication finalization checkpoint processed_count",
    )
    state = row[3]
    if state not in {"OPEN", "COMPLETE"}:
        raise PublicationFinalizationCorruptionError(
            "publication finalization checkpoint state is not registered"
        )
    updated_at = require_int63(
        row[4],
        field="publication finalization checkpoint updated_at",
    )
    return _Checkpoint(generation, cursor, count, state, updated_at)


def _require_open_checkpoint(checkpoint: _Checkpoint) -> None:
    if checkpoint.state != "OPEN":
        raise PublicationFinalizationUnavailableError(
            "publication artifact finalization checkpoint is COMPLETE"
        )
    if checkpoint.generation == INT63_MAX:
        raise PublicationFinalizationUnavailableError(
            "publication finalization generation is exhausted"
        )


def _require_checkpoint_matches_page(
    checkpoint: _Checkpoint,
    page: PublicationFinalizationPage,
) -> None:
    _require_open_checkpoint(checkpoint)
    expected = (
        page.start_generation,
        page.start_cursor,
        page.start_processed_count,
        "OPEN",
        page.checkpoint_updated_at,
    )
    actual = (
        checkpoint.generation,
        checkpoint.cursor,
        checkpoint.processed_count,
        checkpoint.state,
        checkpoint.updated_at,
    )
    if actual != expected:
        raise PublicationFinalizationConflictError(
            "publication finalization checkpoint advanced after issue"
        )


def _insert_batch_receipt(
    connector: SQLConnector,
    *,
    page: PublicationFinalizationPage,
    committed_at: int,
) -> None:
    connector.execute(
        f"INSERT INTO {_BATCH_TABLE} "
        "(receipt_id, start_generation, batch_key, start_cursor, "
        "start_processed_count, next_cursor, row_count, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            page.receipt_id,
            page.start_generation,
            page.batch_key,
            page.start_cursor,
            page.start_processed_count,
            page.next_cursor,
            len(page.items),
            committed_at,
        ),
    )


def _advance_checkpoint(
    work: VNextUnitOfWork,
    *,
    receipt_id: bytes,
    checkpoint: _Checkpoint,
    next_cursor: bytes,
    next_processed_count: int,
    next_state: str,
    updated_at: int,
) -> None:
    work.compare_and_swap(
        f"UPDATE {_CHECKPOINT_TABLE} SET generation = %s, `cursor` = %s, "
        "processed_count = %s, state = %s, updated_at = %s "
        "WHERE receipt_id = %s AND generation = %s AND `cursor` = %s "
        "AND processed_count = %s AND state = %s AND updated_at = %s",
        (
            checkpoint.generation + 1,
            next_cursor,
            next_processed_count,
            next_state,
            updated_at,
            receipt_id,
            checkpoint.generation,
            checkpoint.cursor,
            checkpoint.processed_count,
            checkpoint.state,
            checkpoint.updated_at,
        ),
        authority="publication finalization checkpoint tuple",
    )


def _load_batch_by_key(
    connector: SQLConnector,
    receipt_id: bytes,
    batch_key: bytes,
) -> PublicationFinalizationBatchReceipt | None:
    row = connector.fetch_one(
        "SELECT receipt_id, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at "
        f"FROM {_BATCH_VIEW} WHERE receipt_id = %s AND batch_key = %s",
        (receipt_id, batch_key),
    )
    return _batch_from_row(row)


def _load_batch_by_generation(
    connector: SQLConnector,
    receipt_id: bytes,
    start_generation: int,
) -> PublicationFinalizationBatchReceipt | None:
    row = connector.fetch_one(
        "SELECT receipt_id, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at "
        f"FROM {_BATCH_VIEW} WHERE receipt_id = %s AND start_generation = %s",
        (receipt_id, start_generation),
    )
    return _batch_from_row(row)


def _batch_from_row(
    row: tuple[object, ...],
) -> PublicationFinalizationBatchReceipt | None:
    if not row:
        return None
    if len(row) != 12:
        raise PublicationFinalizationCorruptionError(
            "publication finalization receipt row has an invalid shape"
        )
    terminal = require_int63(
        row[9],
        field="publication finalization terminal",
    )
    if terminal not in {0, 1}:
        raise PublicationFinalizationCorruptionError(
            "publication finalization terminal is not boolean"
        )
    try:
        next_state = row[7]
        if not isinstance(next_state, str):
            raise TypeError("publication finalization next_state must be exact text")
        return PublicationFinalizationBatchReceipt(
            require_uuid16(row[0], field="publication finalization receipt_id"),
            _require_batch_key(row[1]),
            require_positive_int63(
                row[2],
                field="publication finalization start_generation",
            ),
            _require_cursor(row[3], field="start_cursor"),
            require_int63(
                row[4],
                field="publication finalization start_processed_count",
            ),
            _require_cursor(row[5], field="next_cursor"),
            require_int63(
                row[6],
                field="publication finalization next_processed_count",
            ),
            next_state,
            require_int63(row[8], field="publication finalization row_count"),
            bool(terminal),
            require_positive_int63(
                row[10],
                field="publication finalization committed_generation",
            ),
            require_int63(row[11], field="publication finalization committed_at"),
        )
    except (TypeError, ValueError) as error:
        raise PublicationFinalizationCorruptionError(
            "publication finalization receipt has invalid facts"
        ) from error


def _require_receipt_matches_page(
    receipt: PublicationFinalizationBatchReceipt,
    page: PublicationFinalizationPage,
) -> None:
    expected = (
        page.receipt_id,
        page.batch_key,
        page.start_generation,
        page.start_cursor,
        page.start_processed_count,
        page.next_cursor,
        page.start_processed_count + len(page.items),
        "COMPLETE" if page.terminal else "OPEN",
        len(page.items),
        page.terminal,
        page.start_generation + 1,
    )
    actual = (
        receipt.receipt_id,
        receipt.batch_key,
        receipt.start_generation,
        receipt.start_cursor,
        receipt.start_processed_count,
        receipt.next_cursor,
        receipt.next_processed_count,
        receipt.next_state,
        receipt.row_count,
        receipt.terminal,
        receipt.committed_generation,
    )
    if actual != expected:
        raise PublicationFinalizationConflictError(
            "current finalization receipt conflicts with issued page"
        )


def _resolve_adapters(
    adapters: Mapping[bytes, PublicationFinalizationAdapter],
    items: tuple[PublicationFinalizationItem, ...],
) -> dict[bytes, PublicationFinalizationAdapter]:
    if not isinstance(adapters, Mapping):
        raise TypeError("publication finalization adapters must be a mapping")
    resolved: dict[bytes, PublicationFinalizationAdapter] = {}
    for adapter_id in sorted({item.adapter_id for item in items}):
        try:
            adapter = adapters[adapter_id]
        except KeyError as error:
            raise PublicationFinalizationUnavailableError(
                "publication finalization storage adapter is not installed"
            ) from error
        actual_id = require_ascii_bytes(
            adapter.adapter_id,
            field="publication finalization adapter_id",
            minimum=1,
            maximum=64,
        )
        if actual_id != adapter_id or not callable(getattr(adapter, "release", None)):
            raise PublicationFinalizationUnavailableError(
                "publication finalization adapter differs from codec registry"
            )
        resolved[adapter_id] = adapter
    return resolved


def _require_shared_gate_locked(
    work: VNextUnitOfWork,
    lease: GateLease,
    *,
    now: int,
) -> None:
    try:
        current = MaintenanceGateRepository.lock_and_require_live(
            work,
            lease,
            now=now,
        )
    except (MaintenanceGateUnavailableError, MaintenanceGateCorruptionError) as error:
        raise PublicationFinalizationUnavailableError(
            "publication finalization maintenance gate is unavailable"
        ) from error
    if current.mode is not GateMode.SHARED:
        raise PublicationFinalizationUnavailableError(
            "publication finalization requires a SHARED maintenance gate"
        )


def _require_shared_gate_snapshot(
    connector: SQLConnector,
    lease: GateLease,
    *,
    now: int,
) -> None:
    requested = _require_shared_lease_shape(lease)
    timestamp = require_int63(now, field="publication finalization gate snapshot now")
    rows = connector.fetch_all(
        "SELECT head.gate_generation, generation.mode, owner.gate_generation, "
        "owner.lease_expires_at, holder.slot "
        "FROM operational_maintenance_gate_heads AS head "
        "JOIN operational_maintenance_gate_generations AS generation "
        "ON generation.gate_generation = head.gate_generation "
        "JOIN operational_maintenance_gate_owners AS owner "
        "ON owner.owner_token = %s "
        "JOIN operational_maintenance_gate_holders AS holder "
        "ON holder.owner_token = owner.owner_token "
        "WHERE head.singleton_id = 1 ORDER BY holder.slot LIMIT 65",
        (requested.owner_token,),
    )
    expected = [
        (
            requested.gate_generation,
            GateMode.SHARED.value,
            requested.gate_generation,
            requested.lease_expires_at,
            requested.slots[0],
        )
    ]
    if rows != expected or requested.lease_expires_at <= timestamp:
        raise PublicationFinalizationUnavailableError(
            "publication finalization gate snapshot is stale or expired"
        )


def _acknowledge(
    page: PublicationFinalizationPage,
) -> PublicationFinalizationAcknowledgement:
    return PublicationFinalizationAcknowledgement(
        page,
        tuple(item.protection_token for item in page.items),
        _ACK_CAPABILITY,
    )


def _require_page(
    page: PublicationFinalizationPage,
) -> PublicationFinalizationPage:
    if not isinstance(page, PublicationFinalizationPage):
        raise TypeError("page must be a PublicationFinalizationPage")
    page.__post_init__()
    return page


def _require_acknowledgement(
    acknowledgement: PublicationFinalizationAcknowledgement,
) -> PublicationFinalizationAcknowledgement:
    if not isinstance(acknowledgement, PublicationFinalizationAcknowledgement):
        raise TypeError(
            "acknowledgement must be a PublicationFinalizationAcknowledgement"
        )
    acknowledgement.__post_init__()
    return acknowledgement


def _require_shared_lease_shape(lease: GateLease) -> GateLease:
    if not isinstance(lease, GateLease):
        raise TypeError("publication finalization gate lease must be a GateLease")
    lease.__post_init__()
    if lease.mode is not GateMode.SHARED or len(lease.slots) != 1:
        raise ValueError("publication finalization requires one SHARED gate slot")
    return lease


def _require_backend(backend: str) -> None:
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError("publication finalization backend is not registered")


def _require_batch_key(batch_key: object) -> bytes:
    return require_bounded_bytes(
        batch_key,
        field="publication finalization batch_key",
        minimum=1,
        maximum=512,
    )


def _require_cursor(cursor: object, *, field: str) -> bytes:
    value = require_bounded_bytes(
        cursor,
        field=f"publication finalization {field}",
        maximum=_MAX_CURSOR_BYTES,
    )
    if value and len(value) != _MAX_CURSOR_BYTES:
        raise ValueError(f"publication finalization {field} must be empty or raw32")
    return value


def _require_page_limit(page_limit: object) -> int:
    value = require_positive_int63(
        page_limit,
        field="publication finalization page_limit",
    )
    if value > _MAX_PAGE_ROWS:
        raise ValueError(
            f"publication finalization pages are capped at {_MAX_PAGE_ROWS} rows"
        )
    return value
