"""Schema-independent public catalog errors."""

from __future__ import annotations

__all__ = ["CatalogIdentifierError", "CatalogRevisionNotFoundError"]


class CatalogIdentifierError(RuntimeError):
    """A public catalog identifier is not in its exact canonical grammar."""


class CatalogRevisionNotFoundError(LookupError):
    """The requested immutable catalog revision does not exist."""

    def __init__(self, revision: int) -> None:
        self.revision = revision
        super().__init__(f"Catalog revision {revision} does not exist")
