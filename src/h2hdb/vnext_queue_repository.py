"""Generation-backed vNext operational queue history."""

from __future__ import annotations

__all__ = [
    "DeletionGenerationExhaustedError",
    "DeletionRequestReceipt",
    "DownloadRequestCorruptionError",
    "EnsureDownloadRequestReceipt",
    "QueueIdentityConflictError",
    "VNextQueueRepository",
    "VNextDownloadRequest",
]

import secrets
from dataclasses import dataclass

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
