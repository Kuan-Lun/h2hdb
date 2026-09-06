"""Recoverable source observation failures at the public ingest boundary."""

from __future__ import annotations

__all__ = ["VNextSourceChangedError"]


class VNextSourceChangedError(RuntimeError):
    """The source changed during preparation and must be observed afresh."""
