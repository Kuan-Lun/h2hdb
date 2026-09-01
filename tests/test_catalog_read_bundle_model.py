from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "CatalogReadBundle.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "CatalogReadBundle.tla"
SMALL_PROFILE = ROOT / "verification" / "tla" / "CatalogReadBundleSmall.cfg"


def test_lean_model_proves_layout_bundle_and_fresh_fence_equivalence() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for definition in {
        "executeRead",
        "singleSnapshotBundle",
        "fourIndependentReads",
        "bundledRead",
    }:
        assert f"def {definition}" in model
    for theorem in {
        "reused_connector_equals_two_connectors_under_same_observations",
        "sequential_fresh_transactions_preserve_reference_result",
        "single_snapshot_bundle_equals_four_reads_at_stable_head",
        "stable_bundle_succeeds_with_exact_reference",
        "advanced_head_has_zero_stale_success",
        "successful_read_implies_exact_fresh_head",
        "successful_bundle_cannot_return_stale_payload",
    }:
        assert f"theorem {theorem}" in model

    assert "two transaction observations remain distinct" in prose
    assert "LANGUAGE, SUBJECT, and CONTRIBUTOR" in prose
    assert "do not prove connector reset" in prose
    assert "SQLite/MariaDB refinement" in prose


def test_tla_model_explores_layout_reuse_bundle_reads_and_head_advance() -> None:
    model = TLA_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for action in {
        "BeginRead",
        "ReadBundle",
        "ReadBundleLanguage",
        "ReadBundleSubject",
        "ReadBundleContributor",
        "ReadIndependentPage",
        "ReadIndependentLanguage",
        "ReadIndependentSubject",
        "ReadIndependentContributor",
        "ReadFreshFence",
        "AdvanceHead",
    }:
        assert f"{action} ==" in model
    for invariant in {
        "ConnectorLayoutsAgree",
        "BundleUsesOnePinnedSnapshot",
        "StableIndependentReadsMatchPinned",
        "SuccessfulBundleEqualsFourStableReads",
        "FreshFenceControlsOutcome",
        "HeadAdvancementHasZeroStaleSuccess",
        "SuccessfulReadHasExactFreshHead",
        "SuccessfulReadHadStableHead",
    }:
        assert f"{invariant} ==" in model

    assert "same pinned and fresh-fence observations" in prose
    assert "staleSuccesses remains zero" in prose
    assert "does not establish Python connector lifecycle" in prose
    assert "SQLite/MariaDB refinement" in prose


def test_tla_small_profile_wires_finite_head_and_bundle_safety() -> None:
    profile = SMALL_PROFILE.read_text(encoding="utf-8")

    assert "SPECIFICATION Spec" in profile
    assert "MaxRevision = 3" in profile
    assert "ConnectorLayoutsAgree" in profile
    assert "SuccessfulBundleEqualsFourStableReads" in profile
    assert "HeadAdvancementHasZeroStaleSuccess" in profile
    assert "SuccessfulReadHasExactFreshHead" in profile
    assert "not connector, Python, SQL, SQLite, or MariaDB refinement" in profile
    assert "PROPERTY" not in profile
