"""Exact global request budget for bounded gallery staging.

The singleton is an emergency backpressure authority, not the normal
retention mechanism.  The ingest-fenced terminal staging retirement protocol
keeps steady-state request history to one gallery; this counter prevents an
abandoned or corrupt workflow from growing the request identity family without
bound.
"""

from __future__ import annotations

__all__ = [
    "GALLERY_STAGING_REQUEST_LIMIT",
    "GalleryStagingBudgetCorruptionError",
    "GalleryStagingCapacityError",
    "lock_gallery_staging_request_budget",
    "release_gallery_staging_request_budget",
    "reserve_gallery_staging_request_budget",
]

from dataclasses import dataclass

from .vnext_domains import require_int63, require_positive_int63
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

GALLERY_STAGING_REQUEST_LIMIT = 1_500_000
_BUDGET_TABLE = "operational_gallery_observation_staging_request_budgets"


class GalleryStagingCapacityError(RuntimeError):
    """A fresh request must wait for terminal staging retirement/cleanup."""

    def __init__(self, retained_request_count: int) -> None:
        self.retained_request_count = require_int63(
            retained_request_count,
            field="retained gallery staging request count",
        )
        self.request_limit = GALLERY_STAGING_REQUEST_LIMIT
        super().__init__(
            "gallery staging request capacity is exhausted; clean stale terminal "
            "staging, or abandon an oversized OPEN staging and release its ingest "
            "lease before maintenance cleanup, then retry"
        )


class GalleryStagingBudgetCorruptionError(RuntimeError):
    """The singleton budget row is absent or disagrees with a mutation."""


@dataclass(slots=True)
class GalleryStagingRequestBudgetReservation:
    """One transaction's already-locked budget authority and current value."""

    _work: VNextUnitOfWork
    retained_request_count: int

    def reserve(self, work: VNextUnitOfWork) -> None:
        """Reserve one more identity without trying to reacquire the HEAD lock."""

        if work is not self._work:
            raise GalleryStagingBudgetCorruptionError(
                "gallery staging request reservation belongs to another transaction"
            )
        retained = require_int63(
            self.retained_request_count,
            field="locked retained gallery staging request count",
        )
        if retained > GALLERY_STAGING_REQUEST_LIMIT:
            raise GalleryStagingBudgetCorruptionError(
                "gallery staging request budget exceeds its hard limit"
            )
        if retained == GALLERY_STAGING_REQUEST_LIMIT:
            raise GalleryStagingCapacityError(retained)
        affected = work.connector.execute_affected(
            f"UPDATE {_BUDGET_TABLE} "
            "SET retained_request_count = retained_request_count + 1 "
            "WHERE singleton_id = 1 AND retained_request_count = %s",
            (retained,),
        )
        if affected != 1:
            raise GalleryStagingBudgetCorruptionError(
                "gallery staging request budget changed after its row lock"
            )
        self.retained_request_count = retained + 1


def lock_gallery_staging_request_budget(work: VNextUnitOfWork) -> int:
    """Lock and return the singleton before any request-identity CHILD lock."""

    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("gallery-staging-request-budget", 1),
        f"SELECT retained_request_count FROM {_BUDGET_TABLE} WHERE singleton_id = 1",
    )
    return _decode_request_budget(row)


def reserve_gallery_staging_request_budget(
    work: VNextUnitOfWork,
) -> GalleryStagingRequestBudgetReservation:
    """Lock the singleton once and reserve the transaction's first identity."""

    reservation = GalleryStagingRequestBudgetReservation(
        work, lock_gallery_staging_request_budget(work)
    )
    reservation.reserve(work)
    return reservation


def release_gallery_staging_request_budget(
    work: VNextUnitOfWork,
    *,
    retained_request_count: int,
    deleted_count: int,
) -> None:
    """Subtract exact request identities deleted in the caller transaction."""

    count = require_positive_int63(
        deleted_count,
        field="deleted gallery staging request count",
    )
    if count > 256:
        raise GalleryStagingBudgetCorruptionError(
            "one request-budget release exceeds the transaction row bound"
        )
    retained = require_int63(
        retained_request_count,
        field="locked retained gallery staging request count",
    )
    if retained > GALLERY_STAGING_REQUEST_LIMIT or retained < count:
        raise GalleryStagingBudgetCorruptionError(
            "gallery staging request budget would underflow"
        )
    affected = work.connector.execute_affected(
        f"UPDATE {_BUDGET_TABLE} "
        "SET retained_request_count = retained_request_count - %s "
        "WHERE singleton_id = 1 AND retained_request_count = %s",
        (count, retained),
    )
    if affected == 1:
        return
    raise GalleryStagingBudgetCorruptionError(
        "gallery staging request budget changed after its row lock"
    )


def _decode_request_budget(row: tuple[object, ...]) -> int:
    if len(row) != 1:
        raise GalleryStagingBudgetCorruptionError(
            "gallery staging request budget singleton is missing"
        )
    retained = require_int63(row[0], field="retained gallery staging request count")
    if retained > GALLERY_STAGING_REQUEST_LIMIT:
        raise GalleryStagingBudgetCorruptionError(
            "gallery staging request budget exceeds its hard limit"
        )
    return retained
