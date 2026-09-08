"""Structured diagnostics that preserve original artifact error classification."""

from __future__ import annotations

__all__ = ["get_artifact_failure_context"]

from collections.abc import Iterator
from contextlib import contextmanager

from .domain import ArtifactFailureContext

_CONTEXT_ATTRIBUTE = "_h2hdb_artifact_failure_context"
_MAXIMUM_ERROR_CHAIN = 32


def get_artifact_failure_context(
    error: BaseException,
) -> ArtifactFailureContext | None:
    """Read bounded diagnostics through an explicit cause chain, without parsing text."""

    current: BaseException | None = error
    for _ in range(_MAXIMUM_ERROR_CHAIN):
        if current is None:
            break
        context = getattr(current, _CONTEXT_ATTRIBUTE, None)
        if isinstance(context, ArtifactFailureContext):
            return context
        current = current.__cause__
    return None


@contextmanager
def artifact_failure_scope(context: ArtifactFailureContext) -> Iterator[None]:
    """Add diagnostic identity without wrapping source-change or storage errors."""

    try:
        yield
    except Exception as error:
        # The innermost scope can identify an exact source leaf. An outer
        # gallery scope must not replace it with less specific information.
        if get_artifact_failure_context(error) is None:
            setattr(error, _CONTEXT_ATTRIBUTE, context)
            error.add_note(
                f"Artifact source context: gid={context.gid}; "
                f"source_root_components={ascii(context.source_root_components)[:4096]}; "
                "gallery_locator_components="
                f"{ascii(context.gallery_locator_components)[:4096]}"
            )
        raise
