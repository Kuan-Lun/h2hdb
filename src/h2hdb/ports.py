"""Public application protocols for the greenfield catalog."""

from __future__ import annotations

__all__ = ["CatalogReader"]

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from .domain import (
    CatalogArtifact,
    CatalogPage,
    CatalogPublication,
    CatalogRevision,
)


@runtime_checkable
class CatalogReader(Protocol):
    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision: ...

    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
        require_artifact: bool = False,
    ) -> CatalogPage: ...

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None: ...

    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | int | None = None,
    ) -> Mapping[str, CatalogPublication]: ...

    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifact | None: ...
