from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "ArtifactReceiptCache.lean"


def test_lean_model_covers_equivalence_work_and_disposal_contracts() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())
    required_definitions = {
        "CacheValid",
        "reference",
        "optimized",
        "secondPhaseRenderCount",
        "retainAfterPersist",
        "retireMismatched",
        "discardCache",
        "referenceAfterSourceProbe",
        "optimizedAfterSourceProbe",
        "retainBoundedAfterPersist",
    }
    required_theorems = {
        "optional_cache_observationally_equals_reference",
        "exact_cache_hit_renders_once_over_both_phases",
        "missing_cache_rerenders_but_preserves_observation",
        "mismatched_cache_rerenders_but_preserves_observation",
        "response_loss_retains_no_cache",
        "non_pending_reply_retains_no_cache",
        "authority_or_family_drift_evicts",
        "discard_rerenders_with_reference_observation",
        "source_probe_preserves_optional_cache_equivalence",
        "rejected_source_probe_never_consumes_cache",
        "oversized_receipt_retains_no_cache",
    }

    for definition in required_definitions:
        assert f"def {definition}" in model
    for theorem in required_theorems:
        assert f"theorem {theorem}" in model

    assert "exact authority and ordered resource-family key" in prose
    assert "durable/public confirmation result" in prose
    assert "reopens and hashes the same selected sealed source members" in prose
    assert "do not prove that Python ownership" in prose
