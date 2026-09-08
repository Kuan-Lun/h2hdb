"""Best-effort local preparation observations, independent of durable authority."""

from .domain import VNextSourcePreparationOperation, VNextSourcePreparationProgress
from .ports import VNextSourcePreparationObserver


def report_source_progress(
    observer: VNextSourcePreparationObserver | None,
    operation: VNextSourcePreparationOperation,
    completed: int,
    total: int | None = None,
) -> None:
    if observer is None:
        return
    progress = VNextSourcePreparationProgress(operation, completed, total)
    try:
        observer(progress)
    except Exception:
        # Observers never control source validation or the ingest result.
        pass
