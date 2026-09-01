from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "CanonicalPlanCursor.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "CanonicalPlanCursor.tla"
SMALL_PROFILE = ROOT / "verification" / "tla" / "CanonicalPlanCursorSmall.cfg"


def test_lean_model_names_cursor_refinement_and_linear_work_theorems() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())
    required_definitions = {
        "ConsumerCommitted",
        "inspectEntry",
        "referenceScan",
        "optimizedScan",
        "CompletePrefix",
        "CompletionMonotone",
        "FullStreamValidationRequired",
        "inspectEntryWithObservedReceipt",
        "commitPreparedAllocate",
    }
    required_theorems = {
        "inspect_entry_complete_iff",
        "complete_prefix_survives_authorized_progress",
        "reference_scan_equals_optimized_scan",
        "next_action_is_unchanged",
        "terminal_result_is_unchanged",
        "rejection_result_is_unchanged",
        "scan_crosses_only_exact_complete_entries",
        "consecutive_cursor_scans_do_not_revalidate_crossed_prefix",
        "composed_cursor_work_is_linear",
        "exact_retry_requires_full_stream_again",
        "same_receipt_conflicting_preimage_rejects",
        "changed_or_missing_observed_receipt_rejects",
        "delayed_allocate_after_consumer_is_zero_write_rejected",
        "changed_allocate_authority_is_zero_write_rejected",
        "build_consumed_value_does_not_reallocate_claim",
        "validation_does_not_recreate_consumed_claim",
        "successful_progress_validation_work_is_linear",
    }

    for definition in required_definitions:
        assert f"def {definition}" in model
    for theorem in required_theorems:
        assert f"theorem {theorem}" in model

    assert "durable first-consumer checkpoint" in prose
    assert "full stream validation again" in prose
    assert "zero publication writes" in prose
    assert "arbitrary retry storms" in prose
    assert "do not prove that Python iteration" in prose


def test_tla_model_covers_cursor_cache_transaction_and_fence_failures() -> None:
    model = TLA_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())
    required_actions = {
        "IssueAction",
        "CaptureConcurrentPreparedAllocate",
        "StageTransaction",
        "CommitAction",
        "CommitDelayedAllocate",
        "RejectDelayedAllocateAfterConsumer",
        "RejectDelayedAllocate",
        "ReplayLost",
        "AdvanceFence",
        "RejectStaleFence",
        "AbortTransaction",
        "CorruptActivePreimageSameReceipt",
        "Crash",
        "Restart",
    }
    required_invariants = {
        "DurableWorldsAgree",
        "ReferenceAndCursorSelectSameNextAction",
        "ReferenceAndCursorHaveSameTerminalResult",
        "CursorPrefixIsFreshlyJustified",
        "ValidatedPositionsAreOneMonotonicPrefix",
        "SuccessfulProgressValidationWorkIsLinear",
        "SuccessfulRequiredAllocationsAreBounded",
        "EveryPreparedSealedAllocateWasFreshlyStreamed",
        "SameReceiptChangedPreimageIsRejected",
        "CurrentConsumerClaimDeletionAndCheckpointAreAtomic",
        "StatementPrefixCrashRollsBack",
        "ResponseLossReplayIsObservational",
        "StaleFenceHasZeroPublicationWrites",
        "DelayedConsumedOrStaleAllocateHasZeroWrites",
        "DelayedAllocateAfterConsumerFenceIsExplicit",
    }

    for action in required_actions:
        assert f"{action} ==" in model or f"{action}(" in model
    for invariant in required_invariants:
        assert f"{invariant} ==" in model

    assert "SafetyView == vars" in prose
    assert "The model has no stream-skipping memo" in prose
    assert "arbitrary aborted retry work is explicitly outside that bound" in prose
    assert "bounded safety" in prose
    assert "not an unbounded proof" in prose
    assert "does not establish liveness" in prose


def test_tla_small_profile_is_finite_exhaustive_safety_wiring() -> None:
    profile = SMALL_PROFILE.read_text(encoding="utf-8")

    assert "SPECIFICATION Spec" in profile
    assert "VIEW SafetyView" in profile
    assert "FirstValue = value1" in profile
    assert "SecondValue = value2" in profile
    assert "MaxGeneration = 2" in profile
    assert "TxStatementCount = 2" in profile
    assert "ReferenceAndCursorSelectSameNextAction" in profile
    assert "EveryPreparedSealedAllocateWasFreshlyStreamed" in profile
    assert "SameReceiptChangedPreimageIsRejected" in profile
    assert "DelayedConsumedOrStaleAllocateHasZeroWrites" in profile
    assert "DelayedAllocateAfterConsumerFenceIsExplicit" in profile
    assert "StaleFenceHasZeroPublicationWrites" in profile
    assert "not an unbounded proof" in profile
    assert "liveness" in profile
    assert "PROPERTY" not in profile
