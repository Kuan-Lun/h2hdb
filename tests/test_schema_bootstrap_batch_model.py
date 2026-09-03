from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "SchemaBootstrapBatch.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "SchemaBootstrapBatch.tla"
SMALL_PROFILE = ROOT / "verification" / "tla" / "SchemaBootstrapBatchSmall.cfg"


def test_lean_model_names_batch_equivalence_and_replay_theorems() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for definition in {
        "durableInsert",
        "applyReference",
        "applyBatched",
        "DurablyEquivalent",
        "ExactOrderedRows",
        "ExactSeedMultiset",
    }:
        assert f"def {definition}" in model
    for theorem in {
        "batched_result_equals_row_reference",
        "exact_seed_multiset_rejects_count_difference",
        "exact_singleton_rejects_changed_row",
        "exact_singleton_rejects_duplicate",
        "exact_singleton_rejects_missing",
        "replay_after_generated_subset_equals_clean_execution",
        "replay_after_generated_prefix_equals_clean_execution",
        "batched_replay_after_generated_subset_equals_reference",
    }:
        assert f"theorem {theorem}" in model

    assert "unbounded over seed-list length" in prose
    assert "idempotent no-op conflict semantics" in prose
    assert "do not prove Python grouping" in prose


def test_tla_model_covers_bounded_batch_crash_and_response_loss() -> None:
    model = TLA_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for action in {
        "PrepareBatch",
        "CommitDelivered",
        "CommitResponseLost",
        "AbortPrepared",
        "ReplayLost",
        "Crash",
        "PublishReady",
    }:
        assert f"{action} ==" in model
    for invariant in {
        "PreparedBatchIsBounded",
        "DurableFactsAreAnOrderedPrefix",
        "ReadyHasExactGeneratedFacts",
        "ReadyIsTerminal",
    }:
        assert f"{invariant} ==" in model

    assert "Finite crash/replay model" in prose
    assert "does not establish Python" in prose


def test_tla_small_profile_wires_finite_safety_invariants() -> None:
    profile = SMALL_PROFILE.read_text(encoding="utf-8")

    assert "SPECIFICATION Spec" in profile
    assert "SeedCount = 4" in profile
    assert "BatchLimit = 2" in profile
    assert "StatementSplit = 3" in profile
    assert "PreparedBatchIsBounded" in profile
    assert "DurableFactsAreAnOrderedPrefix" in profile
    assert "ReadyHasExactGeneratedFacts" in profile
    assert "PROPERTY" not in profile
