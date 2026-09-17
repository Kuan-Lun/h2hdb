from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from h2hdb.vnext_analysis_repository import (
    _PREPARATION_TOKEN,
    _STAGE_ISSUE_TOKEN,
    AnalysisPreparationAuthority,
    AnalysisRepository,
    AnalysisStageIssue,
    _encode_cursor,
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
        _encode_cursor(b"D", None, live_count=0),
        0,
        (),
        (),
        authority,
        None,
        None,
        _STAGE_ISSUE_TOKEN,
    )


def _key(value: int) -> bytes:
    return value.to_bytes(32, "big")


@pytest.mark.parametrize(
    "keys",
    (
        [_key(1)],
        (b"short",),
        (_key(1), _key(1)),
        (_key(2), _key(1)),
        tuple(_key(value) for value in range(130)),
    ),
)
def test_issued_actual_keys_reject_invalid_shape_domain_order_and_bound(
    keys: Any,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(_issue(_authority()), file_decision_actual_keys=keys)


def test_issued_actual_keys_require_validation_stage_and_exact_cursor_prefix() -> None:
    issue = replace(_issue(_authority()), file_decision_actual_keys=(_key(1),))
    with pytest.raises(ValueError, match="non-validation"):
        replace(issue, stage=b"file_hash_decision")
    with pytest.raises(ValueError, match="after its cursor"):
        replace(issue, checkpoint_cursor=_encode_cursor(b"D", _key(1), live_count=0))
    with pytest.raises(ValueError, match="lookahead"):
        replace(
            issue, page_limit=1, file_decision_actual_keys=(_key(1), _key(2), _key(3))
        )


def test_local_validation_page_merges_bounded_source_and_issued_actual_prefix() -> None:
    authority = _authority()
    plan = build_file_decision_validation_plan(
        authority,
        (FileDecisionSourceGallery(1, 1, (), ((_key(2), 1), (_key(4), 1))),),
    )
    try:
        issue = replace(
            _issue(authority),
            page_limit=2,
            file_decision_actual_keys=(_key(1), _key(3), _key(5)),
        )
        page = AnalysisRepository.prepare_file_decision_validation_page(
            issue=issue, plan=plan
        )
        assert page.entries == ((_key(1), None), (_key(2), (1, 0, 0)))
        page.verify()
        with pytest.raises(RuntimeError, match="another authority"):
            AnalysisRepository.prepare_file_decision_validation_page(
                issue=replace(
                    issue,
                    preparation_authority=replace(authority, generation=2),
                ),
                plan=plan,
            )
    finally:
        plan.close()


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
        match fault:
            case "count_and_prefix":
                plan.row_count = 1
                plan._payload.truncate(
                    88
                )  # A still-authentic prefix cannot change authority.
            case "authority":
                plan.authority = replace(plan.authority, generation=2)
            case "record":
                plan._payload.seek(0)
                plan._payload.write(b"x")
            case _:
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
        match field:
            case "entries":
                corrupted = replace(page, entries=((_key(1), (2, 0, 0)),))
            case "batch_key":
                corrupted = replace(page, batch_key=b"another-batch")
            case "source_count":
                corrupted = replace(page, source_count=2)
            case _:
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


def test_scratch_inputs_use_one_local_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlite3

    statements: list[str] = []
    original = sqlite3.connect

    def connect(database: str, *, isolation_level: None) -> sqlite3.Connection:
        connection = original(database, isolation_level=isolation_level)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    plan = build_file_decision_validation_plan(
        _authority(),
        (FileDecisionSourceGallery(1, 1, (1, 2), ((_key(i), 1) for i in range(260))),),
    )
    plan.close()
    assert statements.count("BEGIN") == 1
    assert statements.count("COMMIT") == 1
    assert statements.index("BEGIN") < next(
        i for i, sql in enumerate(statements) if sql.startswith("INSERT")
    )
    assert statements.index("COMMIT") < next(
        i for i, sql in enumerate(statements) if sql.startswith("SELECT occurrence.")
    )
