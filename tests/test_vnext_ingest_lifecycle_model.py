from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "IngestFacadeLifecycle.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "IngestFacadeLifecycle.tla"
TLA_SMALL = ROOT / "verification" / "tla" / "IngestFacadeLifecycleSmall.cfg"


def test_formal_models_cover_ingest_facade_resource_lifecycle() -> None:
    lean = LEAN_MODEL.read_text(encoding="utf-8")
    tla = TLA_MODEL.read_text(encoding="utf-8")
    small = TLA_SMALL.read_text(encoding="utf-8")

    for theorem in (
        "close_is_idempotent",
        "call_after_close_is_rejected",
        "close_wins_install_and_releases_once",
        "install_then_close_releases_once",
        "take_then_close_preserves_borrower_ownership",
    ):
        assert f"theorem {theorem}" in lean
    for invariant in (
        "ExactlyOneOwner",
        "AtMostOneRelease",
        "ClosedFacadeOwnsNoCache",
        "ResourceConserved",
    ):
        assert invariant in tla
        assert invariant in small
    assert "TLC explores only" in tla
    assert "Runtime barrier-driven race tests" in tla
