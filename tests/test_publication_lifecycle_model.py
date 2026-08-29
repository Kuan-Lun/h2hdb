from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "verification" / "tla" / "PublicationLifecycle.tla"
MINIMAL_PROFILE = ROOT / "verification" / "tla" / "PublicationLifecycleMinimal.cfg"
SMALL_PROFILE = ROOT / "verification" / "tla" / "PublicationLifecycleSmall.cfg"


def test_publication_lifecycle_model_names_the_atomic_safety_contract() -> None:
    model = MODEL.read_text(encoding="utf-8")
    required_actions = {
        "PrepareCatalogDescriptor",
        "SealCandidate",
        "BeginPublish",
        "StagePublishStatement",
        "CommitPublish",
        "RejectStaleHeadCAS",
        "ReplayPublication",
        "CrashDuringPublish",
        "IssueFinalization",
        "ReleaseFinalization",
        "CommitFinalization",
        "ReplayFinalization",
        "RejectStaleFinalizationRetry",
        "CleanupCandidate",
    }
    required_invariants = {
        "TypeOK",
        "CommitSealIsTotal",
        "CommitHasThirteenMandatoryMembers",
        "CommitEquivalentKeysAreUnique",
        "GenerationChainIsExact",
        "CommonHeadIsMaximumTip",
        "PublishedCandidateIffSealedCommonCommit",
        "CandidateLifecycleIsGraphDerived",
        "CommitOwnsPermanentCheckpoint",
        "DescriptorIsNotPublishedBeforeCommitSeal",
        "DerivedActivationIffSealedCommit",
        "NoTornPublicVisibility",
        "ReplayIsObservational",
        "StaleHeadCASHasZeroDurableWrites",
        "TransactionPrefixIsRollbackOnly",
        "StatementPrefixCrashRollsBack",
        "FinalizationBatchEquations",
        "ExternalReleasePrecedesCommittedRow",
        "CurrentFinalizationReceiptIsUnique",
        "SuccessorCASAndPredecessorDeleteAreAtomic",
        "FinalizationMarkerAndCurrentReceiptAgree",
        "FinalizationCoherence",
        "TerminalCurrentReceiptReplaySurvivesCandidateCleanup",
        "ResponseLossHasCurrentReplayAuthority",
        "StalePredecessorRetryHasZeroDurableWrites",
        "TerminalCurrentReceiptIsFinalizedAtAuthority",
    }

    for action in required_actions:
        assert f"{action}(" in model or f"{action} ==" in model
    for invariant in required_invariants:
        assert f"{invariant} ==" in model

    assert "INSERT_SOURCE_DESCRIPTOR" in model
    assert "INSERT_GENERATION_NODE" in model
    assert "INSERT_GENERATION_SUCCESSOR" in model
    assert "INSERT_COMMIT_ANCHOR" in model
    assert "INSERT_13_COMMIT_MEMBERS" in model
    assert "INSERT_FINALIZATION_CHECKPOINT_FAMILY" in model
    assert "INSERT_COMMIT_SEAL_LAST" in model
    assert "CAS_COMMON_HEAD_RECEIPT" in model
    assert "operational activation are derived views" in model
    assert "CandidatePhase(c) ==" in model
    assert "ReceiptFinalizedAt(c) ==" in model
    assert "candidatePhase" not in model
    assert "receiptFinalizedAt" not in model
    assert "FINALIZE_EXTERNAL_RELEASE" in model
    assert "STALE_FINALIZATION_RETRY" in model
    assert "(finalizationBatchReceipts \\ FinalizationBatchesFor(c))" in model
    assert "Cardinality(FinalizationBatchesFor(c)) <= 1" in model
    assert "ResponseLossHasPermanentReplayAuthority" not in model
    assert "FinalizationIsAppendOnlyAndDerived" not in model
    assert "A prefix crash" in model
    assert "not an unbounded proof" in model
    assert "does not establish liveness" in model


def test_publication_lifecycle_profiles_are_explicitly_bounded_safety() -> None:
    minimal = MINIMAL_PROFILE.read_text(encoding="utf-8")
    small = SMALL_PROFILE.read_text(encoding="utf-8")

    for profile in (minimal, small):
        assert "SPECIFICATION Spec" in profile
        assert "VIEW SafetyView" in profile
        assert "INVARIANTS" in profile
        assert "CurrentFinalizationReceiptIsUnique" in profile
        assert "SuccessorCASAndPredecessorDeleteAreAtomic" in profile
        assert "ResponseLossHasCurrentReplayAuthority" in profile
        assert "StalePredecessorRetryHasZeroDurableWrites" in profile
        assert "TerminalCurrentReceiptIsFinalizedAtAuthority" in profile
        assert "not an unbounded proof" in profile
        assert "liveness" in profile
        assert "PROPERTY" not in profile

    assert "Candidates = {candidate1}" in minimal
    assert "Candidates = {candidate1, candidate2}" in small
    assert "BatchKeys = {batch1, batch2}" in small
    assert "MaxGeneration = 2" in small
    assert "MaxFinalizationGeneration = 3" in small
