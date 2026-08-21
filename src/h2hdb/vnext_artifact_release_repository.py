"""Bounded reconciliation of orphaned artifact protection tokens.

The database transaction and the external storage call are deliberately split:

* ``issue_page`` reconstructs one opaque, immutable page from sealed durable
  protection-token facts while holding the exact EXCLUSIVE maintenance fence.
* ``release_page`` revalidates that fence and every page fact in a short
  transaction, commits that transaction, and only then invokes the idempotent
  external tombstone operation.
* ``commit_page`` accepts only the opaque acknowledgement issued after all
  external calls succeeded.  It revalidates and locks the complete page before
  changing every active state to ``COMMITTED`` in one transaction.

No release receipt relation is required for this protocol: a lost tx1 response
is reconstructed byte-for-byte from the immutable token facts, and a lost
external response is retried with the same terminal, idempotent tokens.  A tx2
response retry retains its acknowledgement and performs no repair writes once
the complete page is already ``COMMITTED``.
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

from . import vnext_identity as identity
from .domain import ArtifactReleaseStorageEvidence
from .ports import ArtifactReleaseAdapter
from .sql_connector import SQLConnector
from .vnext_domains import (
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
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_MAX_PAGE_ROWS = 128
_CURSOR_BYTES = 48
_PAGE_CAPABILITY = object()
_ACK_CAPABILITY = object()


class ArtifactReleaseRepositoryError(RuntimeError):
    """Base class for orphan-protection reconciliation failures."""


class ArtifactReleaseUnavailableError(ArtifactReleaseRepositoryError):
    """The release page, adapter, or maintenance authority is unavailable."""


class ArtifactReleaseConflictError(ArtifactReleaseRepositoryError):
    """Durable release authority differs from the repository-issued facts."""


@dataclass(frozen=True, slots=True)
class ArtifactReleaseItem:
    """Every exact durable fact needed to release one protection token."""

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
            field="artifact release candidate_id",
        )
        publication = require_digest32(
            self.publication_key,
            field="artifact release publication_key",
        )
        artifact = require_digest32(
            self.artifact_sha256,
            field="artifact release artifact_sha256",
        )
        locator = require_digest32(
            self.artifact_locator_sha256,
            field="artifact release artifact_locator_sha256",
        )
        expected_components = identity.artifact_locator_components(artifact)
        if tuple(self.locator_components) != expected_components:
            raise ValueError("artifact release locator is not content-addressed")
        if identity.artifact_locator_digest(expected_components) != locator:
            raise ValueError("artifact release locator digest disagrees")
        size = require_int63(self.size_bytes, field="artifact release size_bytes")
        codec = require_positive_int63(
            self.storage_codec_version,
            field="artifact release storage_codec_version",
        )
        generation = require_int63(
            self.storage_generation,
            field="artifact release storage_generation",
        )
        token_bytes = require_bounded_bytes(
            self.protection_token,
            field="artifact release protection_token",
            minimum=184,
            maximum=184,
        )
        require_ascii_bytes(
            self.adapter_id,
            field="artifact release adapter_id",
            minimum=1,
            maximum=64,
        )
        if self.state not in {"PENDING", "PREPARED", "COMMITTED"}:
            raise ValueError("artifact release state is not registered")
        try:
            decoded = identity.decode_artifact_protection_token(token_bytes)
        except identity.VNextIdentityError as error:
            raise ValueError(
                "artifact release protection token is malformed"
            ) from error
        if (
            decoded.candidate_id != candidate
            or decoded.publication_key != publication
            or decoded.artifact_sha256 != artifact
            or decoded.artifact_locator_sha256 != locator
            or decoded.storage_codec_version != codec
            or decoded.storage_generation != generation
            or decoded.size_bytes != size
        ):
            raise ValueError("artifact release token disagrees with durable facts")

    @property
    def cursor(self) -> bytes:
        return self.candidate_id + self.publication_key

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
class ArtifactReleasePage:
    """Opaque bounded tx1 result, reproducible from sealed token facts."""

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
        _require_page(self.page)
        expected = tuple(item.protection_token for item in self.page.items)
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
            after_candidate, after_publication = _decode_cursor(start)
            cursor_clause = (
                "AND (seal.candidate_id > %s OR "
                "(seal.candidate_id = %s AND seal.publication_key > %s)) "
            )
            parameters = (after_candidate, after_candidate, after_publication)
        rows = work.connector.fetch_all(
            _RELEASE_SELECT
            + _RELEASE_JOINS
            + "WHERE state_row.state IN ('PENDING', 'PREPARED') "
            + _CANDIDATE_ELIGIBILITY
            + cursor_clause
            + "ORDER BY seal.candidate_id, seal.publication_key LIMIT %s",
            (*parameters, bound),
        )
        try:
            items = tuple(_item_from_row(row) for row in rows)
        except (TypeError, ValueError) as error:
            raise ArtifactReleaseConflictError(
                "artifact release page contains incomplete or corrupt durable facts"
            ) from error
        next_cursor = start if not items else items[-1].cursor
        return ArtifactReleasePage(
            gate_lease,
            start,
            next_cursor,
            bound,
            items,
            not items,
            _PAGE_CAPABILITY,
        )

    @staticmethod
    def release_page(
        connector: SQLConnector,
        *,
        backend: str,
        page: ArtifactReleasePage,
        adapters: Mapping[bytes, ArtifactReleaseAdapter],
        now: int,
    ) -> ArtifactReleaseAcknowledgement:
        """Call terminal storage tombstones only after a committed revalidation."""

        requested = _require_page(page)
        if backend not in {"sqlite", "mariadb"}:
            raise ValueError("artifact release backend is not registered")
        timestamp = require_int63(now, field="artifact release external now")
        resolved = _resolve_adapters(adapters, requested.items)

        # This transaction contains no external call.  Its gate locks fence all
        # ordinary writers while the exact page is revalidated, and it commits
        # before the first adapter invocation below.
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _require_exclusive_gate(work, requested.gate_lease, now=timestamp)
            _revalidate_page(connector, requested)

        released: list[bytes] = []
        for item in requested.items:
            raw = resolved[item.adapter_id].release(
                item.locator_components,
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
        for item in page.items:
            state_row = work.lock_row(
                LockRank.CHILD,
                encode_lock_key(
                    "artifact-release-state",
                    item.candidate_id,
                    item.publication_key,
                ),
                "SELECT state FROM catalog_prepared_artifact_states "
                "WHERE candidate_id = %s AND publication_key = %s",
                (item.candidate_id, item.publication_key),
            )
            if len(state_row) != 1:
                raise ArtifactReleaseConflictError(
                    "acknowledged prepared-artifact state is absent"
                )
            current = _load_exact_item(
                work.connector,
                candidate_id=item.candidate_id,
                publication_key=item.publication_key,
            )
            if current is None or current.immutable_facts != item.immutable_facts:
                raise ArtifactReleaseConflictError(
                    "acknowledged artifact release facts changed"
                )
            if state_row[0] != current.state:
                raise ArtifactReleaseConflictError(
                    "locked artifact release state disagrees with its family"
                )
            if not _candidate_is_eligible(work.connector, item.candidate_id):
                raise ArtifactReleaseUnavailableError(
                    "candidate became active or published before release commit"
                )
            current_items.append(current)

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
                "UPDATE catalog_prepared_artifact_states SET state = 'COMMITTED' "
                "WHERE candidate_id = %s AND publication_key = %s AND state = %s",
                (item.candidate_id, item.publication_key, item.state),
                authority="orphan artifact protection release",
            )
        for item in current_items:
            updated = _load_exact_item(
                work.connector,
                candidate_id=item.candidate_id,
                publication_key=item.publication_key,
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
    "SELECT seal.candidate_id, seal.publication_key, digest.artifact_sha256, "
    "codec_row.storage_codec_version, generation_row.storage_generation, "
    "token_row.protection_token, state_row.state, blob_row.size_bytes, "
    "location_row.artifact_locator_sha256, codec_seal.storage_codec_version, "
    "adapter_row.storage_codec_version, adapter_row.adapter_id "
)

_RELEASE_JOINS = (
    "FROM catalog_prepared_artifact_seals AS seal "
    "JOIN catalog_publication_candidate_definition_seals AS candidate_row "
    "ON candidate_row.candidate_id = seal.candidate_id "
    "LEFT JOIN catalog_prepared_artifact_sha256s AS digest "
    "ON digest.candidate_id = seal.candidate_id "
    "AND digest.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_storage_codec_versions AS codec_row "
    "ON codec_row.candidate_id = seal.candidate_id "
    "AND codec_row.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_storage_generations AS generation_row "
    "ON generation_row.candidate_id = seal.candidate_id "
    "AND generation_row.publication_key = seal.publication_key "
    "LEFT JOIN catalog_prepared_artifact_protection_tokens AS token_row "
    "ON token_row.candidate_id = seal.candidate_id "
    "AND token_row.publication_key = seal.publication_key "
    "JOIN catalog_prepared_artifact_states AS state_row "
    "ON state_row.candidate_id = seal.candidate_id "
    "AND state_row.publication_key = seal.publication_key "
    "LEFT JOIN catalog_artifact_blobs AS blob_row "
    "ON blob_row.artifact_sha256 = digest.artifact_sha256 "
    "LEFT JOIN catalog_artifact_location AS location_row "
    "ON location_row.artifact_sha256 = digest.artifact_sha256 "
    "LEFT JOIN catalog_artifact_storage_codec_seals AS codec_seal "
    "ON codec_seal.storage_codec_version = codec_row.storage_codec_version "
    "LEFT JOIN catalog_artifact_storage_codec_adapter_ids AS adapter_row "
    "ON adapter_row.storage_codec_version = codec_row.storage_codec_version "
)

_CANDIDATE_ELIGIBILITY = (
    "AND NOT EXISTS ("
    "SELECT 1 FROM operational_catalog_working_candidates AS working_row "
    "WHERE working_row.candidate_id = seal.candidate_id) "
    "AND NOT EXISTS ("
    "SELECT 1 FROM catalog_publication_commit_candidates AS commit_row "
    "WHERE commit_row.candidate_id = seal.candidate_id) "
)


def _item_from_row(row: tuple[object, ...]) -> ArtifactReleaseItem:
    if len(row) != 12:
        raise ValueError("artifact release family row has an invalid shape")
    codec = require_positive_int63(row[3], field="artifact release codec")
    if row[9] != codec or row[10] != codec:
        raise ValueError("artifact release storage codec family is partial")
    artifact = require_digest32(row[2], field="artifact release artifact_sha256")
    components = identity.artifact_locator_components(artifact)
    return ArtifactReleaseItem(
        candidate_id=require_uuid16(row[0], field="artifact release candidate_id"),
        publication_key=require_digest32(
            row[1],
            field="artifact release publication_key",
        ),
        artifact_sha256=artifact,
        artifact_locator_sha256=require_digest32(
            row[8],
            field="artifact release artifact_locator_sha256",
        ),
        locator_components=components,
        size_bytes=require_int63(row[7], field="artifact release size_bytes"),
        storage_codec_version=codec,
        storage_generation=require_int63(
            row[4],
            field="artifact release storage_generation",
        ),
        protection_token=require_bounded_bytes(
            row[5],
            field="artifact release protection_token",
            minimum=184,
            maximum=184,
        ),
        adapter_id=require_ascii_bytes(
            row[11],
            field="artifact release adapter_id",
            minimum=1,
            maximum=64,
        ),
        state=_require_state(row[6]),
    )


def _load_exact_item(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
) -> ArtifactReleaseItem | None:
    candidate = require_uuid16(candidate_id, field="artifact release candidate_id")
    publication = require_digest32(
        publication_key,
        field="artifact release publication_key",
    )
    row = connector.fetch_one(
        _RELEASE_SELECT
        + _RELEASE_JOINS
        + "WHERE seal.candidate_id = %s AND seal.publication_key = %s",
        (candidate, publication),
    )
    if not row:
        return None
    try:
        return _item_from_row(row)
    except (TypeError, ValueError) as error:
        raise ArtifactReleaseConflictError(
            "prepared artifact release family is partial or corrupt"
        ) from error


def _revalidate_page(connector: SQLConnector, page: ArtifactReleasePage) -> None:
    candidates: set[bytes] = set()
    for item in page.items:
        current = _load_exact_item(
            connector,
            candidate_id=item.candidate_id,
            publication_key=item.publication_key,
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


def _candidate_is_eligible(connector: SQLConnector, candidate_id: bytes) -> bool:
    candidate = require_uuid16(candidate_id, field="artifact release candidate_id")
    row = connector.fetch_one(
        "SELECT candidate_row.candidate_id "
        "FROM catalog_publication_candidate_definition_seals AS candidate_row "
        "WHERE candidate_row.candidate_id = %s "
        "AND NOT EXISTS ("
        "SELECT 1 FROM operational_catalog_working_candidates AS working_row "
        "WHERE working_row.candidate_id = candidate_row.candidate_id) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM catalog_publication_commit_candidates AS commit_row "
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
                "artifact release adapter differs from the sealed codec registry"
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
    if value and len(value) != _CURSOR_BYTES:
        raise ValueError("artifact release cursor must be empty or exactly 48 bytes")
    return value


def _decode_cursor(cursor: bytes) -> tuple[bytes, bytes]:
    value = _require_cursor(cursor)
    if not value:
        raise ValueError("the initial artifact release cursor has no key")
    return value[:16], value[16:]


def _require_page_limit(page_limit: object) -> int:
    value = require_positive_int63(page_limit, field="artifact release page_limit")
    if value > _MAX_PAGE_ROWS:
        raise ValueError(f"artifact release pages are capped at {_MAX_PAGE_ROWS} rows")
    return value


def _require_state(state: object) -> str:
    if not isinstance(state, str):
        raise TypeError("artifact release state must be exact text")
    if state not in {"PENDING", "PREPARED", "COMMITTED"}:
        raise ValueError("artifact release state is not registered")
    return state
