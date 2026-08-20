from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "verification" / "tla" / "MutableVerticalCAS.tla"
MINIMAL_PROFILE = ROOT / "verification" / "tla" / "MutableVerticalCASMinimal.cfg"
SMALL_PROFILE = ROOT / "verification" / "tla" / "MutableVerticalCASSmall.cfg"


def test_mutable_vertical_cas_model_names_the_atomic_safety_contract() -> None:
    model = MODEL.read_text(encoding="utf-8")
    required_actions = {
        "CreateCheckpoint",
        "CommitBatch",
        "ReplayBatch",
        "Crash",
        "BeginCleanup",
        "CleanupReceiptMember",
        "CleanupCheckpointMember",
    }
    required_invariants = {
        "TypeOK",
        "VisibleTuplesAreTotal",
        "CheckpointEqualsLatestReceiptPoststate",
        "ReceiptGenerationsAreUnique",
        "ReceiptEquations",
        "AtomicCommitHasNoPublishedHalf",
        "ReplayIsObservational",
        "ReplayUsesStoredPageLimit",
        "CrashPreservesDurableState",
        "CrashLeavesNoTornTuple",
        "CleanupIsChildFirst",
    }

    for action in required_actions:
        assert f"{action}(" in model or f"{action} ==" in model
    for invariant in required_invariants:
        assert f"{invariant} ==" in model

    assert "single atomic database transitions" in model
    assert "not an unbounded proof" in model
    assert "does not establish liveness" in model
    assert "stored immutable page limit" in model
    assert "retry caller limit" in model


def test_mutable_vertical_cas_profiles_are_explicitly_bounded_safety() -> None:
    minimal = MINIMAL_PROFILE.read_text(encoding="utf-8")
    small = SMALL_PROFILE.read_text(encoding="utf-8")

    for profile in (minimal, small):
        assert "SPECIFICATION Spec" in profile
        assert "VIEW SafetyView" in profile
        assert "INVARIANTS" in profile
        assert "not an unbounded proof" in profile
        assert "liveness" in profile
        assert "PROPERTY" not in profile

    assert "RowCounts = {0}" in minimal
    assert "PageLimits = {1}" in minimal
    assert "RowCounts = {0, 1}" in small
    assert "PageLimits = {1, 2}" in small
    assert "MaxGeneration = 2" in small
