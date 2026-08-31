"""Public-safe facades for greenfield vNext administration and applications.

The repository layer deliberately accepts an already-open connector or unit of
work.  These facades are the small application boundary that owns bounded
transactions and exposes only domain objects to consumers.  Catalog reads add
a second fresh transaction that fences the completed snapshot against the
current publication head.
"""

from __future__ import annotations

__all__ = [
    "VNextCatalogFacade",
    "VNextDatabaseAdminFacade",
    "VNextDownloadQueueFacade",
    "open_database",
]

import secrets
from collections.abc import Callable, Mapping, Sequence
from time import time_ns
from typing import TypeVar

from .config_loader import CoreConfig
from .domain import (
    DEFAULT_CATALOG_DISCOVERY_QUERY,
    CatalogArtifact,
    CatalogDiscoveryCursor,
    CatalogDiscoveryPage,
    CatalogDiscoveryQuery,
    CatalogFacetCursor,
    CatalogFacetKind,
    CatalogFacetPage,
    CatalogImageResource,
    CatalogPublication,
    CatalogPublicationPresentation,
    CatalogRecentOrder,
    CatalogRecentWindow,
    CatalogRevision,
    DownloadCandidateState,
)
from .repository import RepositoryContext
from .schema_admin import SchemaEpochReadiness, VNextSchemaAdmin
from .schema_epoch import SchemaEpochReport
from .sql_connector import SQLConnector
from .vnext_catalog_reader_repository import (
    VNextCatalogReaderRepository,
    VNextCatalogReadError,
)
from .vnext_download_ingest_repository import (
    DownloadHandoff,
    DownloadIngestRepository,
    DownloadTurn,
)
from .vnext_queue_repository import (
    DeletionRequestReceipt,
    EnsureDownloadRequestReceipt,
    PendingRedownloadCursor,
    PendingRedownloadPage,
    VNextDownloadRequest,
    VNextQueueRepository,
)
from .vnext_transaction import VNextUnitOfWork

_ResultT = TypeVar("_ResultT")


def _now_microseconds() -> int:
    """Return one portable nonnegative UTC timestamp for durable queue facts."""

    return time_ns() // 1_000


class VNextDatabaseAdminFacade:
    """Expose only greenfield schema-epoch administration to applications."""

    __slots__ = ("__admin",)

    def __init__(self, config: CoreConfig) -> None:
        if not isinstance(config, CoreConfig):
            raise TypeError("config must be CoreConfig")
        self.__admin = VNextSchemaAdmin(RepositoryContext.from_config(config))

    def initialize(self) -> SchemaEpochReport:
        return self.__admin.initialize()

    def check(self) -> SchemaEpochReport:
        return self.__admin.check()

    def check_readiness(self) -> SchemaEpochReadiness:
        return self.__admin.check_readiness()


class VNextCatalogFacade:
    """Read one pinned snapshot, then fence it against a fresh current head."""

    __slots__ = ("__backend", "__context", "__reader")

    def __init__(self, config: CoreConfig) -> None:
        if not isinstance(config, CoreConfig):
            raise TypeError("config must be CoreConfig")
        context = RepositoryContext.from_config(config)
        self.__context = context
        self.__backend = context.sql_type
        self.__reader = VNextCatalogReaderRepository(backend=self.__backend)

    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision:
        return self.__read(
            lambda connector: self.__reader.get_catalog_revision(
                connector,
                revision,
            )
        )

    def discover_publications(
        self,
        *,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogDiscoveryCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogDiscoveryPage:
        return self.__read(
            lambda connector: self.__reader.discover_publications(
                connector,
                query=query,
                after=after,
                revision=revision,
                limit=limit,
            )
        )

    def list_publication_facets(
        self,
        *,
        facet: CatalogFacetKind,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogFacetCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogFacetPage:
        return self.__read(
            lambda connector: self.__reader.list_publication_facets(
                connector,
                facet=facet,
                query=query,
                after=after,
                limit=limit,
                revision=revision,
            )
        )

    def list_recent_publications(
        self,
        *,
        order: CatalogRecentOrder,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogRecentWindow:
        return self.__read(
            lambda connector: self.__reader.list_recent_publications(
                connector,
                order=order,
                revision=revision,
            )
        )

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None:
        return self.__read(
            lambda connector: self.__reader.get_publication(
                connector,
                publication_id,
                revision=revision,
            )
        )

    def get_publication_presentation(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublicationPresentation | None:
        return self.__read(
            lambda connector: self.__reader.get_publication_presentation(
                connector,
                publication_id,
                revision=revision,
            )
        )

    def get_publication_page(
        self,
        publication_id: str,
        page_index: int,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogImageResource | None:
        return self.__read(
            lambda connector: self.__reader.get_publication_page(
                connector,
                publication_id,
                page_index,
                revision=revision,
            )
        )

    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | int | None = None,
    ) -> Mapping[str, CatalogPublication]:
        return self.__read(
            lambda connector: self.__reader.get_publications_by_artifact_names(
                connector,
                names,
                revision=revision,
            )
        )

    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifact | None:
        return self.__read(
            lambda connector: self.__reader.get_artifact(
                connector,
                artifact_id,
                revision=revision,
            )
        )

    def __read(self, operation: Callable[[SQLConnector], _ResultT]) -> _ResultT:
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                pinned = self.__reader.get_catalog_revision(connector)
                result = operation(connector)
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                current = self.__reader.get_catalog_revision(connector)
                if current != pinned:
                    raise VNextCatalogReadError(
                        "catalog publication head advanced during the read"
                    )
        return result


class VNextDownloadQueueFacade:
    """Own one short transaction per public download-queue operation."""

    __slots__ = ("__backend", "__clock", "__context")

    def __init__(
        self,
        config: CoreConfig,
        *,
        clock: Callable[[], int] = _now_microseconds,
    ) -> None:
        if not isinstance(config, CoreConfig):
            raise TypeError("config must be CoreConfig")
        if not callable(clock):
            raise TypeError("clock must be callable")
        context = RepositoryContext.from_config(config)
        self.__context = context
        self.__backend = context.sql_type
        self.__clock = clock

    def request_download(self, gid: int, url: str = "") -> VNextDownloadRequest:
        requested_at = self.__clock()
        return self.__write(
            lambda work: VNextQueueRepository.request_download(
                work,
                gid=gid,
                url=url,
                requested_at=requested_at,
            )
        )

    def ensure_download_request(
        self,
        gid: int,
        url: str = "",
    ) -> EnsureDownloadRequestReceipt:
        requested_at = self.__clock()
        return self.__write(
            lambda work: VNextQueueRepository.ensure_download_request(
                work,
                gid=gid,
                url=url,
                requested_at=requested_at,
            )
        )

    def get_download_request(self, gid: int) -> VNextDownloadRequest | None:
        return self.__read(
            lambda work: VNextQueueRepository.get_download_request(work, gid=gid)
        )

    def list_download_requests(
        self,
        *,
        after_gid: int = 0,
        limit: int = 1000,
    ) -> tuple[VNextDownloadRequest, ...]:
        return self.__read(
            lambda work: VNextQueueRepository.list_download_requests(
                work,
                after_gid=after_gid,
                limit=limit,
            )
        )

    def complete_download_request(self, request: VNextDownloadRequest) -> bool:
        return self.__write(
            lambda work: VNextQueueRepository.complete_download_request(
                work,
                request=request,
            )
        )

    def complete_missing_download_request(
        self,
        request: VNextDownloadRequest,
        missing_gid: int,
    ) -> bool:
        return self.__write(
            lambda work: VNextQueueRepository.complete_missing_download_request(
                work,
                request=request,
                missing_gid=missing_gid,
            )
        )

    def record_gallery_found(self, *gids: int) -> int:
        return self.__write(
            lambda work: VNextQueueRepository.record_galleries_found(
                work,
                gids=gids,
            )
        )

    def request_deletion(
        self,
        gid: int,
        url: str | None = None,
    ) -> DeletionRequestReceipt:
        request_token = secrets.token_bytes(16)
        requested_at = self.__clock()
        return self.__write(
            lambda work: VNextQueueRepository.request_deletion(
                work,
                gid=gid,
                request_token=request_token,
                url=url,
                requested_at=requested_at,
            )
        )

    def get_candidate_states(
        self,
        gids: Sequence[int],
    ) -> Mapping[int, DownloadCandidateState]:
        now = self.__clock()
        return self.__read(
            lambda work: VNextQueueRepository.get_candidate_states(
                work,
                gids=gids,
                now=now,
            )
        )

    def list_pending_redownloads(
        self,
        *,
        cursor: PendingRedownloadCursor | None = None,
        limit: int = 256,
    ) -> PendingRedownloadPage:
        now = self.__clock() if cursor is None else None
        return self.__read(
            lambda work: VNextQueueRepository.list_pending_redownloads(
                work,
                cursor=cursor,
                limit=limit,
                now=now,
            )
        )

    def claim_download_turn(
        self,
        *,
        lease_duration_microseconds: int,
    ) -> DownloadTurn:
        now = self.__clock()
        return self.__write(
            lambda work: DownloadIngestRepository.claim_download(
                work,
                now=now,
                lease_duration=lease_duration_microseconds,
            )
        )

    def renew_download_turn(
        self,
        turn: DownloadTurn,
        *,
        lease_duration_microseconds: int,
    ) -> DownloadTurn:
        now = self.__clock()
        return self.__write(
            lambda work: DownloadIngestRepository.renew_download(
                work,
                turn,
                now=now,
                lease_duration=lease_duration_microseconds,
            )
        )

    def handoff_download_turn(self, turn: DownloadTurn) -> DownloadHandoff:
        now = self.__clock()
        return self.__write(
            lambda work: DownloadIngestRepository.handoff_download(
                work,
                turn,
                now=now,
                recover_existing=True,
            )
        )

    def complete_download_request_in_turn(
        self,
        turn: DownloadTurn,
        request: VNextDownloadRequest,
    ) -> bool:
        now = self.__clock()

        def operation(work: VNextUnitOfWork) -> bool:
            DownloadIngestRepository.resume_download(work, turn, now=now)
            return VNextQueueRepository.complete_download_request(
                work,
                request=request,
            )

        return self.__write(operation)

    def complete_missing_download_request_in_turn(
        self,
        turn: DownloadTurn,
        request: VNextDownloadRequest,
        missing_gid: int,
    ) -> bool:
        now = self.__clock()

        def operation(work: VNextUnitOfWork) -> bool:
            DownloadIngestRepository.resume_download(work, turn, now=now)
            return VNextQueueRepository.complete_missing_download_request(
                work,
                request=request,
                missing_gid=missing_gid,
            )

        return self.__write(operation)

    def finish_download_turn(
        self,
        turn: DownloadTurn,
        request: VNextDownloadRequest,
    ) -> DownloadHandoff:
        now = self.__clock()

        def operation(work: VNextUnitOfWork) -> DownloadHandoff:
            transition = DownloadIngestRepository.ensure_download_handoff(
                work,
                turn,
                now=now,
            )
            if transition.created:
                VNextQueueRepository.complete_download_request(
                    work,
                    request=request,
                )
            return transition.handoff

        return self.__write(operation)

    def finish_missing_download_turn(
        self,
        turn: DownloadTurn,
        request: VNextDownloadRequest,
        missing_gid: int,
    ) -> DownloadHandoff:
        now = self.__clock()

        def operation(work: VNextUnitOfWork) -> DownloadHandoff:
            transition = DownloadIngestRepository.ensure_download_handoff(
                work,
                turn,
                now=now,
            )
            if transition.created:
                VNextQueueRepository.complete_missing_download_request(
                    work,
                    request=request,
                    missing_gid=missing_gid,
                )
            return transition.handoff

        return self.__write(operation)

    def is_download_handoff_complete(self, handoff: DownloadHandoff) -> bool:
        return self.__read(
            lambda work: DownloadIngestRepository.is_download_handoff_complete(
                work,
                handoff,
            )
        )

    def __read(
        self,
        operation: Callable[[VNextUnitOfWork], _ResultT],
    ) -> _ResultT:
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                return operation(VNextUnitOfWork(connector, backend=self.__backend))

    def __write(
        self,
        operation: Callable[[VNextUnitOfWork], _ResultT],
    ) -> _ResultT:
        with self.__context.SQLConnector() as connector:
            with connector.transaction():
                return operation(VNextUnitOfWork(connector, backend=self.__backend))


def open_database(config: CoreConfig) -> VNextCatalogFacade:
    """Open the public catalog only after a complete epoch-READY audit.

    The audit always resolves the exact wheel-resident generated provider before
    the catalog facade is constructed.  Callers cannot substitute schema DDL or
    validation behavior through this public boundary.
    """

    VNextDatabaseAdminFacade(config).check()
    return VNextCatalogFacade(config)
