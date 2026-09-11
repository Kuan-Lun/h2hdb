"""Recoverable source observation failures at the public ingest boundary."""

from __future__ import annotations

__all__ = ["VNextSourceChangedError", "VNextSourceDeferredError"]


class VNextSourceChangedError(RuntimeError):
    """The source changed during preparation and must be observed afresh."""


class VNextSourceDeferredError(VNextSourceChangedError):
    """One gallery is incomplete or changing; retry it in a later source turn.

    Discovery must retain the locator of an existing, incomplete gallery. The
    facade preserves its last published observation and skips new incomplete
    galleries without discarding other galleries' preparation.
    """
