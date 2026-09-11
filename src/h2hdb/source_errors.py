"""Recoverable source observation failures at the public ingest boundary."""

from __future__ import annotations

__all__ = ["VNextSourceChangedError", "VNextSourceDeferredError"]


class VNextSourceChangedError(RuntimeError):
    """The source changed during preparation and must be observed afresh."""


class VNextSourceDeferredError(VNextSourceChangedError):
    """One gallery is incomplete or changing; retry it in a later source turn.

    The facade independently probes published locators omitted by discovery;
    only confirmed absence permits deletion. For a present incomplete gallery it
    preserves the last published observation, and it skips new incomplete
    galleries without discarding other galleries' preparation.
    """
