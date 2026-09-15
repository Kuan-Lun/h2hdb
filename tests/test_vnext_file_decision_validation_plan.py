from __future__ import annotations

from dataclasses import replace

import pytest

from h2hdb.vnext_analysis_repository import (
    _PREPARATION_TOKEN,
    _STAGE_ISSUE_TOKEN,
    AnalysisPreparationAuthority,
    AnalysisStageIssue,
)
from h2hdb.vnext_file_decision_validation_plan import (
    FileDecisionSourceGallery,
    build_file_decision_validation_plan,
)


def _authority() -> AnalysisPreparationAuthority:
    return AnalysisPreparationAuthority(
        b"a" * 16, b"b" * 16, 1, 1, b"m" * 32, (), _PREPARATION_TOKEN
    )


def _issue(authority: AnalysisPreparationAuthority) -> AnalysisStageIssue:
    return AnalysisStageIssue(
        authority.analysis_id,
        authority.build_id,
        b"validate_file_hash_decision",
        b"batch",
        128,
        1,
        b"cursor",
        0,
        (),
        authority,
        None,
        None,
        _STAGE_ISSUE_TOKEN,
    )


def _key(value: int) -> bytes:
    return value.to_bytes(32, "big")


def test_independent_plan_aggregates_occurrences_artist_union_and_gallery_max() -> None:
    authority = _authority()
    galleries = (
        FileDecisionSourceGallery(1, 10, (1, 2), ((_key(1), 3), (_key(2), 1))),
        FileDecisionSourceGallery(2, 20, (2, 3, 4), ((_key(1), 2),)),
        FileDecisionSourceGallery(3, 30, (), ((_key(2), 4), (_key(3), 1))),
    )
    plan = build_file_decision_validation_plan(authority, galleries)
    try:
        assert plan.row_count == 3
        assert plan.source_page(after=None, limit=129) == (
            (_key(1), (5, 4, 3)),
            (_key(2), (5, 2, 2)),
            (_key(3), (1, 0, 0)),
        )
        assert plan.source_page(after=_key(1), limit=1) == ((_key(2), (5, 2, 2)),)
        assert plan.source_page(after=_key(3), limit=128) == ()
    finally:
        plan.close()
    plan.close()
    with pytest.raises(ValueError, match="closed"):
        plan.source_page(after=None, limit=1)


@pytest.mark.parametrize(
    "fault", ("count_and_prefix", "authority", "record", "extra_bytes")
)
def test_plan_rejects_mutated_metadata_and_payload(fault: str) -> None:
    plan = build_file_decision_validation_plan(
        _authority(),
        (FileDecisionSourceGallery(1, 1, (), ((_key(1), 1), (_key(2), 1))),),
    )
    try:
        if fault == "count_and_prefix":
            plan.row_count = 1
            plan._payload.truncate(
                88
            )  # A still-authentic prefix cannot change authority.
        elif fault == "authority":
            plan.authority = replace(plan.authority, generation=2)
        elif fault == "record":
            plan._payload.seek(0)
            plan._payload.write(b"x")
        else:
            plan._payload.seek(0, 2)
            plan._payload.write(b"x")
        with pytest.raises(ValueError, match="modified|length changed"):
            plan.source_page(after=None, limit=1)
    finally:
        plan.close()


@pytest.mark.parametrize(
    "field", ("entries", "batch_key", "source_count", "checkpoint_cursor")
)
def test_prepared_page_rejects_forged_expected_values_and_coordinates(
    field: str,
) -> None:
    authority = _authority()
    plan = build_file_decision_validation_plan(
        authority, (FileDecisionSourceGallery(1, 1, (), ((_key(1), 1),)),)
    )
    try:
        page = plan._prepare_page(_issue(authority), ((_key(1), (1, 0, 0)),))
        if field == "entries":
            corrupted = replace(page, entries=((_key(1), (2, 0, 0)),))
        elif field == "batch_key":
            corrupted = replace(page, batch_key=b"another-batch")
        elif field == "source_count":
            corrupted = replace(page, source_count=2)
        else:
            corrupted = replace(page, checkpoint_cursor=b"another-cursor")
        with pytest.raises(ValueError, match="changed|modified"):
            corrupted.verify()
    finally:
        plan.close()


def test_empty_plan_is_a_valid_independent_empty_source() -> None:
    plan = build_file_decision_validation_plan(_authority(), ())
    try:
        assert plan.row_count == 0
        assert plan.source_page(after=None, limit=128) == ()
    finally:
        plan.close()
