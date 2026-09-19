"""Frozen 0.39.10 per-ancestor read path, solely a performance negative control.

The control deliberately invokes the original point loaders. It is not a
production fallback; public-path experiments patch it only during read-only
issuance on disposable fixture databases.
"""

from __future__ import annotations

from h2hdb.vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    load_analysis_run_family,
)
from h2hdb.vnext_analysis_repository import (
    AnalysisCorruptionError,
    _load_layout,
    _require_exact_component_seals,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def historical_validate_ancestry_suffixes(
    work: VNextUnitOfWork,
    *,
    ancestry: tuple[bytes, ...],
    anchor_analysis_id: bytes,
    policy_id: int,
) -> None:
    """Require every inherited ancestor to materialize its exact sealed suffix."""

    for offset, ancestor in enumerate(ancestry):
        suffix = ancestry[offset:]
        try:
            run = load_analysis_run_family(
                work.connector,
                analysis_id=ancestor,
            )
        except AnalysisFamilyCollisionError as error:
            raise AnalysisCorruptionError(str(error)) from error
        if run is None:
            raise AnalysisCorruptionError("inherited analysis anchor is missing")
        _baseline, derived_anchor, derived_depth, materialized = _load_layout(
            work,
            ancestor,
        )
        if (
            run.policy_id != policy_id
            or run.state != "COMPLETE"
            or derived_anchor != anchor_analysis_id
            or derived_depth != len(suffix) - 1
        ):
            raise AnalysisCorruptionError(
                "inherited analysis does not match the complete policy suffix"
            )
        if materialized != suffix:
            raise AnalysisCorruptionError(
                "inherited analysis ancestry is not the complete parent suffix"
            )
        baseline_row = work.connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (ancestor,),
        )
        if len(suffix) > 1 and baseline_row != (suffix[1],):
            raise AnalysisCorruptionError(
                "inherited analysis baseline is not its immediate parent"
            )
        _require_exact_component_seals(work, ancestor)
