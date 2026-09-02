from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "CatalogChildHydration.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "CatalogChildHydration.tla"
TLA_SMALL = ROOT / "verification" / "tla" / "CatalogChildHydrationSmall.cfg"


def test_formal_models_cover_bounded_ordered_child_hydration() -> None:
    lean = LEAN_MODEL.read_text(encoding="utf-8")
    tla = TLA_MODEL.read_text(encoding="utf-8")
    small = TLA_SMALL.read_text(encoding="utf-8")

    for theorem in (
        "next_page_is_hard_bounded",
        "page_local_canonical_batch_is_hard_bounded",
        "page_then_tail_preserves_exact_decoded_order",
        "bounded_keyset_step_preserves_reference",
        "empty_tail_is_exact_completion",
    ):
        assert f"theorem {theorem}" in lean
    for invariant in (
        "EveryFetchIsHardBounded",
        "EmittedRowsAreExactOrderedPrefix",
        "CursorEqualsEmittedLength",
        "CompletionIsExact",
        "CompletedResultEqualsReference",
    ):
        assert invariant in tla
        assert invariant in small
    assert "suffix strictly after" in " ".join(tla.split())
    assert "TLC exhausts only" in tla
