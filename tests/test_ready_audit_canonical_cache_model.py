from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "verification" / "lean" / "ReadyAuditCanonicalCache.lean"


def test_ready_audit_cache_model_proves_snapshot_equivalence_and_bounds() -> None:
    model = MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for definition in {
        "SnapshotCache",
        "readWithCache",
        "readSequence",
        "cacheAfterValidation",
    }:
        assert f"def {definition}" in model or f"structure {definition}" in model
    for theorem in {
        "cache_hit_equals_snapshot",
        "cache_miss_revalidates_snapshot",
        "every_cached_read_equals_snapshot",
        "arbitrary_access_sequence_equals_revalidation",
        "failed_validation_does_not_change_cache",
        "cache_capacity_is_hard_bounded",
    }:
        assert f"theorem {theorem}" in model

    assert "immutable canonical-value result" in prose
    assert "retain or evict any entries" in prose
    assert "do not prove that Python's LRU implementation enforces the bounds" in prose
    assert "SQL/Python canonical validation is correct" in prose
    assert "production code refines this model" in prose
