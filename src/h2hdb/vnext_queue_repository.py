"""Generation-backed vNext operational queue history."""

from __future__ import annotations

__all__ = [
    "DeletionGenerationExhaustedError",
    "DeletionRequestReceipt",
    "DownloadRequestCorruptionError",
    "EnsureDownloadRequestReceipt",
    "PendingRedownloadCursor",
    "PendingRedownloadCursorError",
    "PendingRedownloadPage",
    "QueueIdentityConflictError",
    "VNextQueueRepository",
    "VNextDownloadRequest",
]

import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .domain import DownloadCandidateState
from .vnext_domains import (
    INT63_MAX,
    require_int63,
    require_positive_int63,
    require_text,
    require_uuid16,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key


class QueueIdentityConflictError(RuntimeError):
    """An idempotency token already names different immutable request facts."""


class DeletionGenerationExhaustedError(OverflowError):
    """The deletion-generation head cannot advance beyond portable int63."""


class DownloadRequestCorruptionError(RuntimeError):
    """The durable download queue violates its declared BCNF row shape."""


class PendingRedownloadCursorError(ValueError):
    """A pending-redownload cursor no longer names exact durable authority."""


@dataclass(frozen=True, slots=True)
class DeletionRequestReceipt:
    request_token: bytes
    gid: int
    url: str | None
    requested_at: int
    created: bool
    current: bool
    observed_generation: int


@dataclass(frozen=True, slots=True)
class VNextDownloadRequest:
    """One exact token-fenced row in the operational download queue."""

    gid: int
    url: str
    request_token: bytes
    requested_at: int

    def __post_init__(self) -> None:
        require_positive_int63(self.gid, field="download request gid")
        require_text(self.url, field="download request URL")
        require_uuid16(self.request_token, field="download request token")
        require_int63(self.requested_at, field="download request requested_at")


@dataclass(frozen=True, slots=True)
class EnsureDownloadRequestReceipt:
    """Result of creating or preserving the current request for one GID."""

    request: VNextDownloadRequest
    created: bool

    def __post_init__(self) -> None:
        if not isinstance(self.request, VNextDownloadRequest):
            raise TypeError("ensure receipt request must be VNextDownloadRequest")
        if not isinstance(self.created, bool):
            raise TypeError("ensure receipt created must be bool")


@dataclass(frozen=True, order=True, slots=True)
class PendingRedownloadCursor:
    """One exact, snapshot-pinned keyset position in the redownload schedule."""

    catalog_revision: int
    source_revision: int
    cutoff_at: int
    redownload_at: int
    gallery_id: int

    def __post_init__(self) -> None:
        require_positive_int63(
            self.catalog_revision,
            field="pending-redownload cursor catalog_revision",
        )
        require_positive_int63(
            self.source_revision,
            field="pending-redownload cursor source_revision",
        )
        cutoff = require_int63(
            self.cutoff_at,
            field="pending-redownload cursor cutoff_at",
        )
        scheduled = require_int63(
            self.redownload_at,
            field="pending-redownload cursor redownload_at",
        )
        require_positive_int63(
            self.gallery_id,
            field="pending-redownload cursor gallery_id",
        )
        if scheduled > cutoff:
            raise ValueError(
                "pending-redownload cursor cannot advance beyond its cutoff"
            )


@dataclass(frozen=True, slots=True)
class PendingRedownloadPage:
    """One hard-bounded page from a pinned redownload schedule scan."""

    catalog_revision: int
    source_revision: int
    cutoff_at: int
    gids: tuple[int, ...]
    next_cursor: PendingRedownloadCursor | None
    terminal: bool

    def __post_init__(self) -> None:
        catalog_revision = require_int63(
            self.catalog_revision,
            field="pending-redownload page catalog_revision",
        )
        source_revision = require_int63(
            self.source_revision,
            field="pending-redownload page source_revision",
        )
        cutoff = require_int63(
            self.cutoff_at,
            field="pending-redownload page cutoff_at",
        )
        if not isinstance(self.gids, tuple):
            raise TypeError("pending-redownload page gids must be an exact tuple")
        if len(self.gids) > 256:
            raise ValueError(
                "pending-redownload page cannot contain more than 256 GIDs"
            )
        validated_gids = tuple(
            require_positive_int63(gid, field="pending-redownload page gid")
            for gid in self.gids
        )
        if len(set(validated_gids)) != len(validated_gids):
            raise ValueError("pending-redownload page GIDs must be unique")
        if type(self.terminal) is not bool:
            raise TypeError("pending-redownload page terminal must be bool")
        if self.terminal != (self.next_cursor is None):
            raise ValueError(
                "pending-redownload page terminal state and cursor disagree"
            )
        if (catalog_revision == 0) != (source_revision == 0):
            raise ValueError(
                "pending-redownload page revisions must both be zero or positive"
            )
        if catalog_revision == 0:
            if self.gids or not self.terminal:
                raise ValueError(
                    "a headless pending-redownload page must be empty and terminal"
                )
            return
        require_positive_int63(
            catalog_revision,
            field="pending-redownload page catalog_revision",
        )
        require_positive_int63(
            source_revision,
            field="pending-redownload page source_revision",
        )
        if self.next_cursor is not None:
            if not isinstance(self.next_cursor, PendingRedownloadCursor):
                raise TypeError(
                    "pending-redownload page next_cursor must be "
                    "PendingRedownloadCursor"
                )
            self.next_cursor.__post_init__()
            if (
                self.next_cursor.catalog_revision != catalog_revision
                or self.next_cursor.source_revision != source_revision
                or self.next_cursor.cutoff_at != cutoff
            ):
                raise ValueError(
                    "pending-redownload page cursor does not share its snapshot pin"
                )


class VNextQueueRepository:
    @staticmethod
    def request_download(
        work: VNextUnitOfWork,
        *,
        gid: int,
        url: str = "",
        requested_at: int,
    ) -> VNextDownloadRequest:
        """Replace one GID's request with a fresh repository-owned token."""

        gallery_id = require_positive_int63(gid, field="download request gid")
        exact_url = _download_url(url)
        timestamp = require_int63(
            requested_at,
            field="download request requested_at",
        )
        token = require_uuid16(
            secrets.token_bytes(16),
            field="generated download request token",
        )
        current = _lock_download_gid(work, gallery_id)
        collision = _lock_download_token(work, token)
        if collision:
            raise QueueIdentityConflictError(
                "generated download request token is already durable"
            )
        if current:
            durable = _download_request_from_row((gallery_id, *current))
            replacement_url = exact_url if exact_url else durable.url
            work.compare_and_swap(
                "UPDATE operational_download_requests "
                "SET url = %s, request_token = %s, requested_at = %s "
                "WHERE gid = %s AND request_token = %s",
                (
                    replacement_url,
                    token,
                    timestamp,
                    gallery_id,
                    durable.request_token,
                ),
                authority=f"download request for gid {gallery_id}",
            )
        else:
            replacement_url = exact_url
            work.connector.execute(
                "INSERT INTO operational_download_requests "
                "(gid, url, request_token, requested_at) VALUES (%s, %s, %s, %s)",
                (gallery_id, replacement_url, token, timestamp),
            )
        return VNextDownloadRequest(
            gallery_id,
            replacement_url,
            token,
            timestamp,
        )

    @staticmethod
    def ensure_download_request(
        work: VNextUnitOfWork,
        *,
        gid: int,
        url: str = "",
        requested_at: int,
    ) -> EnsureDownloadRequestReceipt:
        """Create a request once, filling only an existing empty URL."""

        gallery_id = require_positive_int63(gid, field="download request gid")
        exact_url = _download_url(url)
        timestamp = require_int63(
            requested_at,
            field="download request requested_at",
        )
        current = _lock_download_gid(work, gallery_id)
        if current:
            durable = _download_request_from_row((gallery_id, *current))
            if not durable.url and exact_url:
                work.compare_and_swap(
                    "UPDATE operational_download_requests SET url = %s "
                    "WHERE gid = %s AND request_token = %s AND url = %s",
                    (exact_url, gallery_id, durable.request_token, ""),
                    authority=f"download request URL for gid {gallery_id}",
                )
                durable = VNextDownloadRequest(
                    durable.gid,
                    exact_url,
                    durable.request_token,
                    durable.requested_at,
                )
            return EnsureDownloadRequestReceipt(durable, False)

        token = require_uuid16(
            secrets.token_bytes(16),
            field="generated download request token",
        )
        collision = _lock_download_token(work, token)
        if collision:
            raise QueueIdentityConflictError(
                "generated download request token is already durable"
            )
        work.connector.execute(
            "INSERT INTO operational_download_requests "
            "(gid, url, request_token, requested_at) VALUES (%s, %s, %s, %s)",
            (gallery_id, exact_url, token, timestamp),
        )
        return EnsureDownloadRequestReceipt(
            VNextDownloadRequest(gallery_id, exact_url, token, timestamp),
            True,
        )

    @staticmethod
    def get_download_request(
        work: VNextUnitOfWork,
        *,
        gid: int,
    ) -> VNextDownloadRequest | None:
        gallery_id = require_positive_int63(gid, field="download request gid")
        row = work.connector.fetch_one(
            "SELECT gid, url, request_token, requested_at "
            "FROM operational_download_requests WHERE gid = %s",
            (gallery_id,),
        )
        if not row:
            return None
        return _download_request_from_row(row)

    @staticmethod
    def list_download_requests(
        work: VNextUnitOfWork,
        *,
        after_gid: int = 0,
        limit: int = 1000,
    ) -> tuple[VNextDownloadRequest, ...]:
        cursor = require_int63(after_gid, field="download request after_gid")
        page_limit = require_positive_int63(limit, field="download request limit")
        if page_limit > 1000:
            raise ValueError("download request page limit must not exceed 1000")
        rows = work.connector.fetch_all(
            "SELECT gid, url, request_token, requested_at "
            "FROM operational_download_requests WHERE gid > %s "
            "ORDER BY gid LIMIT %s",
            (cursor, page_limit),
        )
        if len(rows) > page_limit:
            raise DownloadRequestCorruptionError(
                "download request page exceeded its hard limit"
            )
        result = tuple(_download_request_from_row(row) for row in rows)
        if any(left.gid >= right.gid for left, right in zip(result, result[1:])):
            raise DownloadRequestCorruptionError(
                "download request page is not in strict GID order"
            )
        return result

    @staticmethod
    def get_candidate_states(
        work: VNextUnitOfWork,
        *,
        gids: Sequence[int],
        now: int,
    ) -> Mapping[int, DownloadCandidateState]:
        """Read one bounded set of current download decisions.

        ``redownload_required`` is derived only from the current sealed
        publication's durable, source-revision-scoped redownload authority. It
        is never inferred from mutable source observations or catalog payload
        joins.
        """

        ordered_gids = tuple(
            dict.fromkeys(
                require_positive_int63(gid, field="download candidate gid")
                for gid in gids
            )
        )
        if len(ordered_gids) > 256:
            raise ValueError("download candidate lookup must not exceed 256 GIDs")
        if not ordered_gids:
            return {}
        timestamp = require_int63(now, field="download candidate now")
        placeholders = _sql_placeholders(len(ordered_gids))
        catalog_rows = work.connector.fetch_all(
            "SELECT identity.gid, redownload.gallery_id, removed.gid, deletion.gid "
            "FROM catalog_publication_commit_head_receipts AS head "
            "JOIN catalog_publication_commit_seals AS commit_seal "
            "ON commit_seal.receipt_id = head.receipt_id "
            "JOIN catalog_publication_commit_catalog_revisions AS catalog "
            "ON catalog.receipt_id = commit_seal.receipt_id "
            "JOIN catalog_publication_commit_source_revisions AS source "
            "ON source.receipt_id = commit_seal.receipt_id "
            "JOIN catalog_publications AS publication "
            "ON publication.revision = catalog.revision "
            "JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = publication.publication_key "
            "LEFT JOIN operational_gallery_redownload_states AS redownload "
            "ON redownload.gallery_id = publication.gallery_id "
            "AND redownload.through_source_revision = source.source_revision "
            "AND redownload.redownload_at <= %s "
            "LEFT JOIN operational_removed_gids AS removed "
            "ON removed.gid = identity.gid "
            "LEFT JOIN operational_deletion_request_heads AS deletion "
            "ON deletion.gid = identity.gid "
            "WHERE head.channel = %s "
            f"AND identity.gid IN ({placeholders}) ORDER BY identity.gid",
            (timestamp, b"default", *ordered_gids),
        )
        cataloged: set[int] = set()
        redownload_required: set[int] = set()
        requested_rows = work.connector.fetch_all(
            "SELECT gid FROM operational_download_requests "
            f"WHERE gid IN ({placeholders}) ORDER BY gid",
            ordered_gids,
        )
        requested = _validated_gid_rows(
            requested_rows,
            expected=frozenset(ordered_gids),
            authority="download request candidate",
        )
        expected = frozenset(ordered_gids)
        for row in catalog_rows:
            if len(row) != 4:
                raise DownloadRequestCorruptionError(
                    "download catalog candidate row has an invalid shape"
                )
            try:
                gid = require_positive_int63(row[0], field="catalog candidate gid")
            except (TypeError, ValueError) as error:
                raise DownloadRequestCorruptionError(
                    "download catalog candidate row violates its physical domain"
                ) from error
            if gid not in expected or gid in cataloged:
                raise DownloadRequestCorruptionError(
                    "download catalog candidate row is duplicate or unrequested"
                )
            cataloged.add(gid)
            if row[1] is not None:
                try:
                    require_positive_int63(
                        row[1], field="candidate redownload gallery_id"
                    )
                except (TypeError, ValueError) as error:
                    raise DownloadRequestCorruptionError(
                        "download redownload candidate violates its physical domain"
                    ) from error
                if row[2] is None and row[3] is None:
                    redownload_required.add(gid)

        return {
            gid: DownloadCandidateState(
                gid=gid,
                cataloged=gid in cataloged,
                redownload_required=gid in redownload_required,
                requested=gid in requested,
            )
            for gid in ordered_gids
        }

    @staticmethod
    def list_pending_redownloads(
        work: VNextUnitOfWork,
        *,
        cursor: PendingRedownloadCursor | None = None,
        limit: int = 256,
        now: int | None = None,
    ) -> PendingRedownloadPage:
        """Scan one bounded keyset page of durable due-redownload authority.

        The initial page pins the current sealed default-channel publication
        and a caller-supplied time cutoff. Continuations carry those exact pins
        and advance by the last *scanned* schedule row, including rows that do
        not map to an eligible external GID.
        """

        page_limit = require_positive_int63(limit, field="pending-redownload limit")
        if page_limit > 256:
            raise ValueError("pending-redownload page limit must not exceed 256")
        if cursor is None:
            if now is None:
                raise TypeError("initial pending-redownload page requires now")
            cutoff = require_int63(now, field="pending-redownload cutoff_at")
            pin = _current_publication_pin(work)
            if pin is None:
                return PendingRedownloadPage(0, 0, cutoff, (), None, True)
            catalog_revision, source_revision = pin
            after: tuple[int, int] | None = None
        else:
            exact_cursor = _require_pending_redownload_cursor(cursor)
            cutoff = exact_cursor.cutoff_at
            if now is not None:
                supplied_cutoff = require_int63(
                    now,
                    field="pending-redownload continuation cutoff_at",
                )
                if supplied_cutoff != cutoff:
                    raise PendingRedownloadCursorError(
                        "continuation cutoff differs from its pinned cursor"
                    )
            _validate_pending_redownload_cursor(work, exact_cursor)
            catalog_revision = exact_cursor.catalog_revision
            source_revision = exact_cursor.source_revision
            after = (exact_cursor.redownload_at, exact_cursor.gallery_id)

        scan_limit = page_limit + 1
        query, parameters = _pending_redownload_scan_query(
            source_revision=source_revision,
            cutoff=cutoff,
            catalog_revision=catalog_revision,
            scan_limit=scan_limit,
            after=after,
        )
        rows = work.connector.fetch_all(query, parameters)
        if len(rows) > scan_limit:
            raise DownloadRequestCorruptionError(
                "pending-redownload scan exceeded its physical hard limit"
            )

        parsed = tuple(
            _pending_redownload_row(
                row,
                after=after,
                cutoff=cutoff,
            )
            for row in rows
        )
        coordinates = tuple((row[0], row[1]) for row in parsed)
        if any(left >= right for left, right in zip(coordinates, coordinates[1:])):
            raise DownloadRequestCorruptionError(
                "pending-redownload scan is not in strict keyset order"
            )
        window = parsed[:page_limit]
        gids = tuple(
            gid
            for _redownload_at, _gallery_id, gid, removed, deleting in window
            if gid is not None and removed is None and deleting is None
        )
        if len(set(gids)) != len(gids):
            raise DownloadRequestCorruptionError(
                "pending-redownload page contains duplicate external GIDs"
            )
        terminal = len(parsed) <= page_limit
        next_cursor = None
        if not terminal:
            redownload_at, gallery_id, _gid, _removed, _deleting = window[-1]
            next_cursor = PendingRedownloadCursor(
                catalog_revision,
                source_revision,
                cutoff,
                redownload_at,
                gallery_id,
            )
        return PendingRedownloadPage(
            catalog_revision,
            source_revision,
            cutoff,
            gids,
            next_cursor,
            terminal,
        )

    @staticmethod
    def complete_download_request(
        work: VNextUnitOfWork,
        *,
        request: VNextDownloadRequest,
    ) -> bool:
        exact = _require_download_request(request)
        current = _lock_download_gid(work, exact.gid)
        if not current:
            return False
        durable = _download_request_from_row((exact.gid, *current))
        if durable.request_token != exact.request_token:
            return False
        work.compare_and_swap(
            "DELETE FROM operational_download_requests "
            "WHERE gid = %s AND request_token = %s",
            (exact.gid, exact.request_token),
            authority=f"download request completion for gid {exact.gid}",
        )
        return True

    @staticmethod
    def complete_missing_download_request(
        work: VNextUnitOfWork,
        *,
        request: VNextDownloadRequest,
        missing_gid: int,
    ) -> bool:
        """Complete an exact request and publish its confirmed-missing marker."""

        exact = _require_download_request(request)
        missing = require_positive_int63(
            missing_gid,
            field="confirmed-missing gid",
        )
        if exact.gid != missing:
            raise ValueError(
                f"download request GID {exact.gid} does not match missing GID {missing}"
            )
        if not VNextQueueRepository.complete_download_request(work, request=exact):
            return False
        current = _lock_removed_gid(work, missing)
        if not current:
            work.connector.execute(
                "INSERT INTO operational_removed_gids (gid) VALUES (%s)",
                (missing,),
            )
        return True

    @staticmethod
    def record_galleries_found(
        work: VNextUnitOfWork,
        *,
        gids: Sequence[int],
    ) -> int:
        """Clear stale confirmed-missing markers for one bounded GID set."""

        gallery_ids = tuple(
            sorted(
                {require_positive_int63(gid, field="found gallery gid") for gid in gids}
            )
        )
        if len(gallery_ids) > 256:
            raise ValueError("found gallery update must not exceed 256 GIDs")
        deleted = 0
        for gid in gallery_ids:
            if not _lock_removed_gid(work, gid):
                continue
            affected = work.connector.execute_affected(
                "DELETE FROM operational_removed_gids WHERE gid = %s",
                (gid,),
            )
            if affected != 1:
                raise DownloadRequestCorruptionError(
                    "confirmed-missing marker changed after its exact lock"
                )
            deleted += 1
        return deleted

    @staticmethod
    def request_deletion(
        work: VNextUnitOfWork,
        *,
        gid: int,
        request_token: bytes,
        url: str | None,
        requested_at: int,
    ) -> DeletionRequestReceipt:
        gallery_id = require_positive_int63(gid, field="deletion request gid")
        token = require_uuid16(request_token, field="deletion request token")
        timestamp = require_int63(requested_at, field="deletion request requested_at")
        exact_url = (
            None if url is None else require_text(url, field="deletion request URL")
        )

        generation_row = work.lock_row(
            LockRank.HEAD,
            encode_lock_key("deletion-generation-head", 1),
            "SELECT current_generation FROM "
            "operational_deletion_request_generation_heads "
            "WHERE singleton_id = %s",
            (1,),
        )
        if len(generation_row) != 1:
            raise RuntimeError("deletion-request generation head is missing")
        current_generation = require_int63(
            generation_row[0],
            field="deletion request current_generation",
        )

        attempt = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(0, token),
            "SELECT gid, requested_at FROM operational_deletion_request_attempts "
            "WHERE request_token = %s",
            (token,),
        )
        head = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(1, gallery_id),
            "SELECT request_token FROM operational_deletion_request_heads "
            "WHERE gid = %s",
            (gallery_id,),
        )
        url_row = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(2, token),
            "SELECT url FROM operational_deletion_request_urls "
            "WHERE request_token = %s",
            (token,),
        )

        if attempt:
            VNextQueueRepository._validate_replay(
                attempt=attempt,
                url_row=url_row,
                gid=gallery_id,
                url=exact_url,
                requested_at=timestamp,
            )
            return DeletionRequestReceipt(
                request_token=token,
                gid=gallery_id,
                url=exact_url,
                requested_at=timestamp,
                created=False,
                current=bool(head and head[0] == token),
                observed_generation=current_generation,
            )
        if url_row:
            raise QueueIdentityConflictError(
                "deletion request URL exists without its immutable attempt"
            )
        if current_generation == INT63_MAX:
            raise DeletionGenerationExhaustedError(
                "deletion-request generation is exhausted"
            )
        successor = require_positive_int63(
            current_generation + 1,
            field="deletion request successor generation",
        )

        work.connector.execute(
            "INSERT INTO operational_deletion_request_generations "
            "(generation, allocated_at) VALUES (%s, %s)",
            (successor, timestamp),
        )
        work.connector.execute(
            "INSERT INTO operational_deletion_request_attempts "
            "(request_token, gid, requested_at) VALUES (%s, %s, %s)",
            (token, gallery_id, timestamp),
        )
        if exact_url is not None:
            work.connector.execute(
                "INSERT INTO operational_deletion_request_urls "
                "(request_token, url) VALUES (%s, %s)",
                (token, exact_url),
            )
        if head:
            old_token = require_uuid16(head[0], field="prior deletion request token")
            work.compare_and_swap(
                "UPDATE operational_deletion_request_heads SET request_token = %s "
                "WHERE gid = %s AND request_token = %s",
                (token, gallery_id, old_token),
                authority=f"deletion request head for gid {gallery_id}",
            )
        else:
            work.connector.execute(
                "INSERT INTO operational_deletion_request_heads "
                "(gid, request_token) VALUES (%s, %s)",
                (gallery_id, token),
            )
        work.compare_and_swap(
            "UPDATE operational_deletion_request_generation_heads "
            "SET current_generation = %s, updated_at = %s "
            "WHERE singleton_id = %s AND current_generation = %s",
            (successor, timestamp, 1, current_generation),
            authority="deletion-request generation head",
        )
        return DeletionRequestReceipt(
            request_token=token,
            gid=gallery_id,
            url=exact_url,
            requested_at=timestamp,
            created=True,
            current=True,
            observed_generation=successor,
        )

    @staticmethod
    def _validate_replay(
        *,
        attempt: tuple[object, ...],
        url_row: tuple[object, ...],
        gid: int,
        url: str | None,
        requested_at: int,
    ) -> None:
        if len(attempt) != 2 or attempt != (gid, requested_at):
            raise QueueIdentityConflictError(
                "deletion request token names different immutable attempt facts"
            )
        if url is None:
            matches = not url_row
        else:
            matches = len(url_row) == 1 and url_row[0] == url
        if not matches:
            raise QueueIdentityConflictError(
                "deletion request token names different URL presence or bytes"
            )


def _download_url(value: object) -> str:
    return require_text(value, field="download request URL")


def _sql_placeholders(count: int) -> str:
    if count <= 0:
        raise ValueError("SQL placeholder count must be positive")
    return ", ".join("%s" for _ in range(count))


def _validated_gid_rows(
    rows: Sequence[tuple[object, ...]],
    *,
    expected: frozenset[int],
    authority: str,
) -> frozenset[int]:
    gids: set[int] = set()
    for row in rows:
        if len(row) != 1:
            raise DownloadRequestCorruptionError(
                f"{authority} row has an invalid shape"
            )
        try:
            gid = require_positive_int63(row[0], field=f"{authority} gid")
        except (TypeError, ValueError) as error:
            raise DownloadRequestCorruptionError(
                f"{authority} row violates its physical domain"
            ) from error
        if gid not in expected or gid in gids:
            raise DownloadRequestCorruptionError(
                f"{authority} row is duplicate or unrequested"
            )
        gids.add(gid)
    return frozenset(gids)


def _pending_redownload_scan_query(
    *,
    source_revision: int,
    cutoff: int,
    catalog_revision: int,
    scan_limit: int,
    after: tuple[int, int] | None,
) -> tuple[str, tuple[object, ...]]:
    """Build the shared bounded schedule query used by runtime and plan tests."""

    if after is None:
        keyset_predicate = ""
        parameters: tuple[object, ...] = (
            source_revision,
            cutoff,
            scan_limit,
            catalog_revision,
        )
    else:
        keyset_predicate = (
            "AND (redownload_at > %s OR (redownload_at = %s AND gallery_id > %s)) "
        )
        parameters = (
            source_revision,
            cutoff,
            after[0],
            after[0],
            after[1],
            scan_limit,
            catalog_revision,
        )
    return (
        "SELECT scheduled.redownload_at, scheduled.gallery_id, "
        "identity.gid, removed.gid, deletion.gid FROM ("
        "SELECT redownload_at, gallery_id "
        "FROM operational_gallery_redownload_states "
        "WHERE through_source_revision = %s AND redownload_at <= %s "
        f"{keyset_predicate}"
        "ORDER BY redownload_at, gallery_id LIMIT %s"
        ") AS scheduled "
        "LEFT JOIN catalog_publications AS publication "
        "ON publication.revision = %s "
        "AND publication.gallery_id = scheduled.gallery_id "
        "LEFT JOIN catalog_publication_identities AS identity "
        "ON identity.publication_key = publication.publication_key "
        "LEFT JOIN operational_removed_gids AS removed "
        "ON removed.gid = identity.gid "
        "LEFT JOIN operational_deletion_request_heads AS deletion "
        "ON deletion.gid = identity.gid "
        "ORDER BY scheduled.redownload_at, scheduled.gallery_id",
        parameters,
    )


def _current_publication_pin(
    work: VNextUnitOfWork,
) -> tuple[int, int] | None:
    rows = work.connector.fetch_all(
        "SELECT catalog.revision, source.source_revision "
        "FROM catalog_publication_commit_head_receipts AS head "
        "JOIN catalog_publication_commit_seals AS commit_seal "
        "ON commit_seal.receipt_id = head.receipt_id "
        "JOIN catalog_publication_commit_catalog_revisions AS catalog "
        "ON catalog.receipt_id = commit_seal.receipt_id "
        "JOIN catalog_publication_commit_source_revisions AS source "
        "ON source.receipt_id = commit_seal.receipt_id "
        "WHERE head.channel = %s",
        (b"default",),
    )
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 2:
        raise DownloadRequestCorruptionError(
            "current publication pin has an invalid cardinality or shape"
        )
    try:
        return (
            require_positive_int63(
                rows[0][0],
                field="current publication catalog_revision",
            ),
            require_positive_int63(
                rows[0][1],
                field="current publication source_revision",
            ),
        )
    except (TypeError, ValueError) as error:
        raise DownloadRequestCorruptionError(
            "current publication pin violates its physical domain"
        ) from error


def _require_pending_redownload_cursor(value: object) -> PendingRedownloadCursor:
    if not isinstance(value, PendingRedownloadCursor):
        raise TypeError("cursor must be PendingRedownloadCursor")
    # Frozen dataclasses remain forgeable through ``object.__setattr__``.
    value.__post_init__()
    return value


def _validate_pending_redownload_cursor(
    work: VNextUnitOfWork,
    cursor: PendingRedownloadCursor,
) -> None:
    pin_rows = work.connector.fetch_all(
        "SELECT commit_seal.receipt_id "
        "FROM catalog_publication_commit_seals AS commit_seal "
        "JOIN catalog_publication_commit_catalog_revisions AS catalog "
        "ON catalog.receipt_id = commit_seal.receipt_id "
        "JOIN catalog_publication_commit_source_revisions AS source "
        "ON source.receipt_id = commit_seal.receipt_id "
        "WHERE catalog.revision = %s AND source.source_revision = %s",
        (cursor.catalog_revision, cursor.source_revision),
    )
    if len(pin_rows) != 1 or len(pin_rows[0]) != 1:
        raise PendingRedownloadCursorError(
            "cursor does not name one exact sealed publication commit"
        )
    try:
        require_uuid16(
            pin_rows[0][0],
            field="pending-redownload cursor publication receipt",
        )
    except (TypeError, ValueError) as error:
        raise DownloadRequestCorruptionError(
            "pending-redownload publication receipt violates its physical domain"
        ) from error
    coordinate_rows = work.connector.fetch_all(
        "SELECT gallery_id FROM operational_gallery_redownload_states "
        "WHERE gallery_id = %s AND through_source_revision = %s "
        "AND redownload_at = %s AND redownload_at <= %s",
        (
            cursor.gallery_id,
            cursor.source_revision,
            cursor.redownload_at,
            cursor.cutoff_at,
        ),
    )
    if coordinate_rows != [(cursor.gallery_id,)]:
        raise PendingRedownloadCursorError(
            "cursor no longer names its exact durable schedule position"
        )


def _pending_redownload_row(
    row: tuple[object, ...],
    *,
    after: tuple[int, int] | None,
    cutoff: int,
) -> tuple[int, int, int | None, int | None, int | None]:
    if len(row) != 5:
        raise DownloadRequestCorruptionError(
            "pending-redownload row has an invalid shape"
        )
    try:
        redownload_at = require_int63(
            row[0],
            field="pending-redownload row redownload_at",
        )
        gallery_id = require_positive_int63(
            row[1],
            field="pending-redownload row gallery_id",
        )
        gid = (
            None
            if row[2] is None
            else require_positive_int63(row[2], field="pending-redownload row gid")
        )
        removed = (
            None
            if row[3] is None
            else require_positive_int63(
                row[3],
                field="pending-redownload row removed gid",
            )
        )
        deleting = (
            None
            if row[4] is None
            else require_positive_int63(
                row[4],
                field="pending-redownload row deletion gid",
            )
        )
    except (TypeError, ValueError) as error:
        raise DownloadRequestCorruptionError(
            "pending-redownload row violates its physical domain"
        ) from error
    coordinate = (redownload_at, gallery_id)
    if redownload_at > cutoff or (after is not None and coordinate <= after):
        raise DownloadRequestCorruptionError(
            "pending-redownload row lies outside its pinned keyset window"
        )
    if gid is None and (removed is not None or deleting is not None):
        raise DownloadRequestCorruptionError(
            "pending-redownload eligibility marker lacks an external GID"
        )
    if removed is not None and removed != gid:
        raise DownloadRequestCorruptionError(
            "pending-redownload removed marker names a different GID"
        )
    if deleting is not None and deleting != gid:
        raise DownloadRequestCorruptionError(
            "pending-redownload deletion marker names a different GID"
        )
    return redownload_at, gallery_id, gid, removed, deleting


def _require_download_request(value: object) -> VNextDownloadRequest:
    if not isinstance(value, VNextDownloadRequest):
        raise TypeError("request must be VNextDownloadRequest")
    # Frozen dataclasses remain forgeable through ``object.__setattr__``.
    value.__post_init__()
    return value


def _lock_download_gid(
    work: VNextUnitOfWork,
    gid: int,
) -> tuple[object, ...]:
    return work.lock_row(
        LockRank.CHILD,
        encode_lock_key(0, gid),
        "SELECT url, request_token, requested_at "
        "FROM operational_download_requests WHERE gid = %s",
        (gid,),
    )


def _lock_download_token(
    work: VNextUnitOfWork,
    token: bytes,
) -> tuple[object, ...]:
    return work.lock_row(
        LockRank.CHILD,
        encode_lock_key(1, token),
        "SELECT gid, url, requested_at "
        "FROM operational_download_requests WHERE request_token = %s",
        (token,),
    )


def _lock_removed_gid(
    work: VNextUnitOfWork,
    gid: int,
) -> tuple[object, ...]:
    return work.lock_row(
        LockRank.CHILD,
        encode_lock_key(2, gid),
        "SELECT gid FROM operational_removed_gids WHERE gid = %s",
        (gid,),
    )


def _download_request_from_row(row: tuple[object, ...]) -> VNextDownloadRequest:
    if len(row) != 4:
        raise DownloadRequestCorruptionError(
            "download request row has an invalid shape"
        )
    try:
        gid = require_positive_int63(row[0], field="download request gid")
        url = _download_url(row[1])
        token = require_uuid16(row[2], field="download request token")
        requested_at = require_int63(
            row[3],
            field="download request requested_at",
        )
    except (TypeError, ValueError) as error:
        raise DownloadRequestCorruptionError(
            "download request row violates its physical domain"
        ) from error
    return VNextDownloadRequest(gid, url, token, requested_at)
