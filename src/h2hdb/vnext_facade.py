"""Public-safe facades for greenfield vNext administration and applications.

The repository layer deliberately accepts an already-open connector or unit of
work.  These facades are the small application boundary that owns one fresh
connection and one bounded transaction per call while exposing only domain
objects to consumers.
"""

from __future__ import annotations

__all__ = [
    "VNextCatalogFacade",
    "VNextDatabaseAdminFacade",
    "VNextDownloadQueueFacade",
    "open_database",
]

from collections.abc import Callable, Mapping, Sequence
from time import time_ns
from typing import TypeVar

from .config_loader import CoreConfig
from .domain import CatalogArtifact, CatalogPage, CatalogPublication, CatalogRevision
from .repository import RepositoryContext
from .schema_admin import SchemaEpochReadiness, VNextSchemaAdmin
from .schema_epoch import SchemaEpochReport
from .sql_connector import SQLConnector
from .vnext_catalog_reader_repository import VNextCatalogReaderRepository
from .vnext_queue_repository import (
    EnsureDownloadRequestReceipt,
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
    """Open a pinned read transaction for each normalized catalog operation."""

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

    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
        require_artifact: bool = False,
    ) -> CatalogPage:
        return self.__read(
            lambda connector: self.__reader.list_publications(
                connector,
                query=query,
                revision=revision,
                offset=offset,
                limit=limit,
                require_artifact=require_artifact,
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
                return operation(connector)


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
