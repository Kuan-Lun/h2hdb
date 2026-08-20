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
  advances the complete page atomically, and seals a permanent batch receipt.

There is intentionally no issuance relation.  A lost issue response is rebuilt
from the unchanged checkpoint, while a lost commit response is returned from
the permanent ``(receipt_id, batch_key)`` receipt before any transient
authority, candidate fact, or live lease is consulted.
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

from . import vnext_identity as identity
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
_MAX_CURSOR_BYTES = 32
_PREPARED_STAGE = b"VALIDATE_PREPARED_ARTIFACT"
_PAGE_CAPABILITY = object()
_ACK_CAPABILITY = object()

_CHECKPOINT_ANCHOR = "catalog_publication_finalization_checkpoint_anchors"
_CHECKPOINT_GENERATION = "catalog_publication_finalization_checkpoint_generations"
_CHECKPOINT_CURSOR = "catalog_publication_finalization_checkpoint_cursors"
_CHECKPOINT_COUNT = "catalog_publication_finalization_checkpoint_counts"
_CHECKPOINT_STATE = "catalog_publication_finalization_checkpoint_states"
_CHECKPOINT_UPDATED_AT = "catalog_publication_finalization_checkpoint_updated_ats"
_CHECKPOINT_SEAL = "catalog_publication_finalization_checkpoint_seals"
_CHECKPOINT_VIEW = "catalog_publication_finalization_checkpoints"

_BATCH_ANCHOR = "catalog_publication_finalization_batch_anchors"
_BATCH_COORDINATE = "catalog_publication_finalization_batch_coordinates"
_BATCH_START_CURSOR = "catalog_publication_finalization_batch_start_cursors"
_BATCH_START_COUNT = "catalog_publication_finalization_batch_start_counts"
_BATCH_NEXT_CURSOR = "catalog_publication_finalization_batch_next_cursors"
_BATCH_ROW_COUNT = "catalog_publication_finalization_batch_row_counts"
_BATCH_COMMITTED_AT = "catalog_publication_finalization_batch_committed_ats"
_BATCH_SEAL = "catalog_publication_finalization_batch_seals"
_BATCH_VIEW = "catalog_publication_finalization_batch_receipts"

_COMMIT_VIEW = "catalog_publication_commits"
_PUBLICATION_RECEIPT_VIEW = "catalog_publication_receipts"
_FINALIZATION_MARKER = "catalog_publication_commit_finalizations"


class PublicationFinalizationRepositoryError(RuntimeError):
    """Base class for permanent publication-finalization failures."""


class PublicationFinalizationUnavailableError(PublicationFinalizationRepositoryError):
    """A live fence, open checkpoint, storage adapter, or commit is unavailable."""


class PublicationFinalizationConflictError(PublicationFinalizationRepositoryError):
    """Durable finalization facts differ from an opaque repository result."""


class PublicationFinalizationCorruptionError(PublicationFinalizationRepositoryError):
    """A normalized finalization family is partial or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class PublicationFinalizationItem:
    """Exact immutable storage facts for one published PREPARED artifact."""

    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    artifact_locator_sha256: bytes
    locator_components: tuple[str, ...]
    size_bytes: int
    storage_codec_version: int
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
        artifact = require_digest32(
            self.artifact_sha256,
            field="publication finalization artifact_sha256",
        )
        locator = require_digest32(
            self.artifact_locator_sha256,
            field="publication finalization artifact_locator_sha256",
        )
        components = identity.artifact_locator_components(artifact)
        if tuple(self.locator_components) != components:
            raise ValueError(
                "publication finalization locator is not content-addressed"
            )
        if identity.artifact_locator_digest(components) != locator:
            raise ValueError("publication finalization locator digest disagrees")
        size = require_int63(
            self.size_bytes,
            field="publication finalization size_bytes",
        )
        codec = require_positive_int63(
            self.storage_codec_version,
            field="publication finalization storage_codec_version",
        )
        generation = require_int63(
            self.storage_generation,
            field="publication finalization storage_generation",
        )
        token = require_bounded_bytes(
            self.protection_token,
            field="publication finalization protection_token",
            minimum=184,
            maximum=184,
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
        except identity.VNextIdentityError as error:
            raise ValueError("publication finalization token is malformed") from error
        if (
            decoded.candidate_id != candidate
            or decoded.publication_key != publication
            or decoded.artifact_sha256 != artifact
            or decoded.artifact_locator_sha256 != locator
            or decoded.storage_codec_version != codec
            or decoded.storage_generation != generation
            or decoded.size_bytes != size
        ):
            raise ValueError(
                "publication finalization token disagrees with durable facts"
            )

    @property
    def immutable_facts(self) -> tuple[object, ...]:
        return (
            self.candidate_id,
            self.publication_key,
            self.artifact_sha256,
            self.artifact_locator_sha256,
            self.locator_components,
            self.size_bytes,
            self.storage_codec_version,
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
    expected_prepared_count: int
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
        processed = require_int63(
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
        expected = require_int63(
            self.expected_prepared_count,
            field="publication finalization expected_prepared_count",
        )
        limit = _require_page_limit(self.page_limit)
        if processed > expected:
            raise ValueError(
                "publication finalization checkpoint exceeds expected count"
            )
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
            keys.append(item.publication_key)
        ordered = tuple(keys)
        if ordered != tuple(sorted(set(ordered))):
            raise ValueError("publication finalization page keys are not ordered")
        if ordered and start and ordered[0] <= start:
            raise ValueError("publication finalization page did not advance its cursor")
        expected_next = start if not ordered else ordered[-1]
        if next_cursor != expected_next:
            raise ValueError("publication finalization next cursor is not derived")
        if processed + len(ordered) > expected:
            raise ValueError("publication finalization page exceeds expected coverage")
        if type(self.terminal) is not bool or self.terminal != (not ordered):
            raise ValueError("publication finalization terminal marker disagrees")
        if self.terminal and processed != expected:
            raise ValueError("terminal publication finalization count disagrees")


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
    """Permanent response reconstructed from one sealed commit-owned batch."""

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

        return "PROJECTION_FINALIZED" if self.terminal else "DB_COMMITTED"


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
    """Finalize one sealed publication commit in permanent bounded pages."""

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
            expected_count = _load_expected_prepared_count(
                connector,
                context.candidate_id,
            )
            items = _load_page_items(
                connector,
                candidate_id=context.candidate_id,
                cursor=checkpoint.cursor,
                page_limit=bound,
            )
            next_cursor = checkpoint.cursor if not items else items[-1].publication_key
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
                    expected_count,
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
            evidence = resolved[item.adapter_id].release(
                item.locator_components,
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
    ) -> PublicationFinalizationBatchReceipt:
        """Commit one acknowledged page, replaying permanent receipts first."""

        ack = _require_acknowledgement(acknowledgement)
        page = ack.page
        timestamp = require_int63(now, field="publication finalization commit now")

        # Unconditional response-loss safety: no live gate, candidate row, or
        # mutable checkpoint is needed once this exact response is sealed.
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
        expected_count = _load_expected_prepared_count(
            work.connector,
            page.candidate_id,
        )
        if expected_count != page.expected_prepared_count:
            raise PublicationFinalizationConflictError(
                "terminal prepared-artifact receipt changed"
            )
        current_items = _lock_current_items(work, page)
        row_count = len(current_items)
        next_count = checkpoint.processed_count + row_count
        terminal = row_count == 0
        if terminal != page.terminal or next_count > expected_count:
            raise PublicationFinalizationConflictError(
                "publication finalization page coverage changed"
            )
        if terminal and next_count != expected_count:
            raise PublicationFinalizationCorruptionError(
                "terminal finalization count disagrees with validation receipt"
            )
        if checkpoint.generation == INT63_MAX:
            raise PublicationFinalizationUnavailableError(
                "publication finalization generation is exhausted"
            )

        try:
            for item in current_items:
                work.compare_and_swap(
                    "UPDATE catalog_prepared_artifact_states SET state = 'COMMITTED' "
                    "WHERE candidate_id = %s AND publication_key = %s "
                    "AND state = 'PREPARED'",
                    (item.candidate_id, item.publication_key),
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
            if terminal:
                work.connector.execute(
                    f"INSERT INTO {_FINALIZATION_MARKER} (receipt_id) VALUES (%s)",
                    (page.receipt_id,),
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
        """Read one permanent response by either of its exact coordinates."""

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


_ITEM_SELECT = (
    "SELECT seal.candidate_id, seal.publication_key, digest.artifact_sha256, "
    "codec.storage_codec_version, generation.storage_generation, "
    "token.protection_token, state.state, blob.size_bytes, "
    "location.artifact_locator_sha256, codec_seal.storage_codec_version, "
    "adapter.adapter_id "
)

_ITEM_JOINS = (
    "FROM catalog_prepared_artifact_seals AS seal "
    "LEFT JOIN catalog_prepared_artifact_sha256s AS digest "
    "ON digest.candidate_id = seal.candidate_id "
    "AND digest.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_storage_codec_versions AS codec "
    "ON codec.candidate_id = seal.candidate_id "
    "AND codec.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_storage_generations AS generation "
    "ON generation.candidate_id = seal.candidate_id "
    "AND generation.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_protection_tokens AS token "
    "ON token.candidate_id = seal.candidate_id "
    "AND token.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_states AS state "
    "ON state.candidate_id = seal.candidate_id "
    "AND state.publication_key = seal.publication_key "
    "LEFT JOIN catalog_artifact_blobs AS blob "
    "ON blob.artifact_sha256 = digest.artifact_sha256 "
    "LEFT JOIN catalog_artifact_location AS location "
    "ON location.artifact_sha256 = digest.artifact_sha256 "
    "LEFT JOIN catalog_artifact_storage_codec_seals AS codec_seal "
    "ON codec_seal.storage_codec_version = codec.storage_codec_version "
    "LEFT JOIN catalog_artifact_storage_codec_adapter_ids AS adapter "
    "ON adapter.storage_codec_version = codec.storage_codec_version "
)


def _initialize_finalization_checkpoint(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    initialized_at: int,
) -> None:
    """Insert the total OPEN checkpoint before the publication commit seal."""

    receipt = require_uuid16(
        receipt_id,
        field="publication finalization receipt_id",
    )
    timestamp = require_int63(
        initialized_at,
        field="publication finalization initialized_at",
    )
    connector.execute(
        f"INSERT INTO {_CHECKPOINT_ANCHOR} (receipt_id) VALUES (%s)",
        (receipt,),
    )
    for table, column, value in (
        (_CHECKPOINT_GENERATION, "generation", 1),
        (_CHECKPOINT_CURSOR, "cursor", b""),
        (_CHECKPOINT_COUNT, "processed_count", 0),
        (_CHECKPOINT_STATE, "state", "OPEN"),
        (_CHECKPOINT_UPDATED_AT, "updated_at", timestamp),
    ):
        sql_column = f"`{column}`" if column == "cursor" else column
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {sql_column}) VALUES (%s, %s)",
            (receipt, value),
        )
    connector.execute(
        f"INSERT INTO {_CHECKPOINT_SEAL} (receipt_id) VALUES (%s)",
        (receipt,),
    )


def _load_page_items(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    cursor: bytes,
    page_limit: int,
) -> tuple[PublicationFinalizationItem, ...]:
    rows = connector.fetch_all(
        _ITEM_SELECT
        + _ITEM_JOINS
        + "WHERE seal.candidate_id = %s AND seal.publication_key > %s "
        "ORDER BY seal.publication_key LIMIT %s",
        (candidate_id, cursor, page_limit),
    )
    try:
        return tuple(_item_from_row(row) for row in rows)
    except (TypeError, ValueError) as error:
        raise PublicationFinalizationCorruptionError(
            "prepared artifact finalization family is partial or invalid"
        ) from error


def _load_exact_item(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
) -> PublicationFinalizationItem | None:
    row = connector.fetch_one(
        _ITEM_SELECT
        + _ITEM_JOINS
        + "WHERE seal.candidate_id = %s AND seal.publication_key = %s",
        (candidate_id, publication_key),
    )
    if not row:
        return None
    try:
        return _item_from_row(row)
    except (TypeError, ValueError) as error:
        raise PublicationFinalizationCorruptionError(
            "prepared artifact finalization family is partial or invalid"
        ) from error


def _item_from_row(row: tuple[object, ...]) -> PublicationFinalizationItem:
    if len(row) != 11:
        raise ValueError("publication finalization item row has an invalid shape")
    codec = require_positive_int63(
        row[3],
        field="publication finalization storage codec",
    )
    if row[9] != codec:
        raise ValueError("publication finalization codec registry is incomplete")
    artifact = require_digest32(
        row[2],
        field="publication finalization artifact_sha256",
    )
    state = row[6]
    if not isinstance(state, str):
        raise TypeError("publication finalization state must be exact text")
    return PublicationFinalizationItem(
        require_uuid16(row[0], field="publication finalization candidate_id"),
        require_digest32(row[1], field="publication finalization publication_key"),
        artifact,
        require_digest32(
            row[8],
            field="publication finalization artifact_locator_sha256",
        ),
        identity.artifact_locator_components(artifact),
        require_int63(row[7], field="publication finalization size_bytes"),
        codec,
        require_int63(
            row[4],
            field="publication finalization storage_generation",
        ),
        require_bounded_bytes(
            row[5],
            field="publication finalization protection_token",
            minimum=184,
            maximum=184,
        ),
        require_ascii_bytes(
            row[10],
            field="publication finalization adapter_id",
            minimum=1,
            maximum=64,
        ),
        state,
    )


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
    if (
        _load_expected_prepared_count(work.connector, page.candidate_id)
        != page.expected_prepared_count
    ):
        raise PublicationFinalizationConflictError(
            "terminal prepared-artifact receipt changed"
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
        return ()
    placeholders = ", ".join("%s" for _item in page.items)
    lock_keys = tuple(
        encode_lock_key("publication-finalization-item", item.publication_key)
        for item in page.items
    )
    state_rows = work.lock_rows(
        LockRank.CHILD,
        lock_keys,
        "SELECT publication_key, state FROM catalog_prepared_artifact_states "
        f"WHERE candidate_id = %s AND publication_key IN ({placeholders}) "
        "ORDER BY publication_key",
        (page.candidate_id, *(item.publication_key for item in page.items)),
    )
    expected_states = tuple((item.publication_key, "PREPARED") for item in page.items)
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
        f"FROM {_COMMIT_VIEW} AS committed "
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


def _load_expected_prepared_count(
    connector: SQLConnector,
    candidate_id: bytes,
) -> int:
    row = connector.fetch_one(
        "SELECT terminal.next_processed_count "
        "FROM catalog_publication_checkpoints AS checkpoint "
        "JOIN catalog_publication_batch_receipts AS terminal "
        "ON terminal.candidate_id = checkpoint.candidate_id "
        "AND terminal.stage = checkpoint.stage "
        "AND terminal.committed_generation = checkpoint.generation "
        "AND terminal.next_cursor = checkpoint.`cursor` "
        "AND terminal.next_cursor = terminal.start_cursor "
        "AND terminal.next_processed_count = checkpoint.processed_count "
        "AND terminal.committed_at = checkpoint.updated_at "
        "AND terminal.terminal = 1 "
        "AND terminal.next_state = checkpoint.state "
        "WHERE checkpoint.candidate_id = %s AND checkpoint.stage = %s "
        "AND checkpoint.state = 'COMPLETE'",
        (candidate_id, _PREPARED_STAGE),
    )
    if len(row) != 1:
        raise PublicationFinalizationCorruptionError(
            "VALIDATE_PREPARED_ARTIFACT lacks one exact terminal receipt"
        )
    return require_int63(
        row[0],
        field="terminal prepared-artifact count",
    )


def _load_checkpoint(connector: SQLConnector, receipt_id: bytes) -> _Checkpoint:
    row = connector.fetch_one(
        f"SELECT generation, `cursor`, processed_count, state, updated_at "
        f"FROM {_CHECKPOINT_VIEW} WHERE receipt_id = %s",
        (receipt_id,),
    )
    return _checkpoint_from_row(row)


def _lock_checkpoint(work: VNextUnitOfWork, receipt_id: bytes) -> _Checkpoint:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication-finalization-checkpoint", receipt_id),
        f"SELECT generation.generation, checkpoint_cursor.`cursor`, "
        "count.processed_count, "
        "state.state, updated.updated_at "
        f"FROM {_CHECKPOINT_SEAL} AS seal "
        f"JOIN {_CHECKPOINT_GENERATION} AS generation "
        "ON generation.receipt_id = seal.receipt_id "
        f"JOIN {_CHECKPOINT_CURSOR} AS checkpoint_cursor "
        "ON checkpoint_cursor.receipt_id = seal.receipt_id "
        f"JOIN {_CHECKPOINT_COUNT} AS count "
        "ON count.receipt_id = seal.receipt_id "
        f"JOIN {_CHECKPOINT_STATE} AS state "
        "ON state.receipt_id = seal.receipt_id "
        f"JOIN {_CHECKPOINT_UPDATED_AT} AS updated "
        "ON updated.receipt_id = seal.receipt_id "
        "WHERE seal.receipt_id = %s",
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
    key = (page.receipt_id, page.start_generation)
    connector.execute(
        f"INSERT INTO {_BATCH_ANCHOR} "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        f"INSERT INTO {_BATCH_COORDINATE} "
        "(receipt_id, batch_key, start_generation) VALUES (%s, %s, %s)",
        (page.receipt_id, page.batch_key, page.start_generation),
    )
    for table, column, value in (
        (_BATCH_START_CURSOR, "start_cursor", page.start_cursor),
        (
            _BATCH_START_COUNT,
            "start_processed_count",
            page.start_processed_count,
        ),
        (_BATCH_NEXT_CURSOR, "next_cursor", page.next_cursor),
        (_BATCH_ROW_COUNT, "row_count", len(page.items)),
        (_BATCH_COMMITTED_AT, "committed_at", committed_at),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(receipt_id, start_generation, {column}) VALUES (%s, %s, %s)",
            (*key, value),
        )
    connector.execute(
        f"INSERT INTO {_BATCH_SEAL} " "(receipt_id, start_generation) VALUES (%s, %s)",
        key,
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
    changes = (
        (_CHECKPOINT_CURSOR, "cursor", checkpoint.cursor, next_cursor),
        (
            _CHECKPOINT_COUNT,
            "processed_count",
            checkpoint.processed_count,
            next_processed_count,
        ),
        (_CHECKPOINT_STATE, "state", checkpoint.state, next_state),
        (
            _CHECKPOINT_UPDATED_AT,
            "updated_at",
            checkpoint.updated_at,
            updated_at,
        ),
    )
    for table, column, old_value, new_value in changes:
        if old_value == new_value:
            continue
        sql_column = f"`{column}`" if column == "cursor" else column
        work.compare_and_swap(
            f"UPDATE {table} SET {sql_column} = %s "
            f"WHERE receipt_id = %s AND {sql_column} = %s",
            (new_value, receipt_id, old_value),
            authority=f"publication finalization checkpoint {column}",
        )
    work.compare_and_swap(
        f"UPDATE {_CHECKPOINT_GENERATION} SET generation = %s "
        "WHERE receipt_id = %s AND generation = %s",
        (checkpoint.generation + 1, receipt_id, checkpoint.generation),
        authority="publication finalization checkpoint generation",
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
            "permanent finalization receipt conflicts with issued page"
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
