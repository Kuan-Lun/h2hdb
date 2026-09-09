"""Deterministic artifact cleanup without replacing the original failure."""

from collections.abc import Callable


def close_artifact_resources(
    *close: Callable[[], None], primary_error: BaseException | None = None
) -> None:
    """Attempt every release, preserving the first failure and its context."""

    failure = primary_error
    for release in close:
        try:
            release()
        except BaseException as error:
            if failure is None:
                failure = error
            elif error is not failure:
                failure.add_note(f"Artifact resource cleanup also failed: {error!r}")
                # An owned bundle may already have attempted multiple closes.
                # Keep those diagnostics when its first error joins an earlier
                # work failure, without replacing that work failure's identity.
                for note in getattr(error, "__notes__", ()):
                    failure.add_note(note)
    if primary_error is None and failure is not None:
        raise failure
