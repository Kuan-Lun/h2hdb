from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "SchemaArtifactDecode.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "SchemaArtifactDecode.tla"
TLA_SMALL = ROOT / "verification" / "tla" / "SchemaArtifactDecodeSmall.cfg"


def test_formal_models_state_the_exact_decode_boundary() -> None:
    lean = LEAN_MODEL.read_text(encoding="utf-8")
    tla = TLA_MODEL.read_text(encoding="utf-8")
    configuration = TLA_SMALL.read_text(encoding="utf-8")

    assert "reference_codec_roundtrip" in lean
    assert "list_and_tuple_wires_are_distinct" in lean
    assert "successful_exact_decode_preserves_projection" in lean
    assert "admitted_decode_preserves_schema_projection" in lean
    assert "pinned_runtime_decode_preserves_schema_projection" in lean
    assert "ExactPinnedResource" in lean
    assert "do not prove that Python's pickle implementation" in lean
    assert "InvalidNeverAccepted" in tla
    assert "AcceptanceMatchesChecks" in tla
    assert "DigestOK" in tla
    assert "OpcodeSurfaceOK" in tla
    assert "NodeBoundOK" in tla
    assert '"memo-dag-overflow"' in tla
    assert '"dictionary-key-invalid"' in tla
    assert "ClosedTypeSurface" in tla
    assert "FullPreflightChecksPass" in tla
    assert "RuntimeAcceptanceRequiresPinnedCohort" in tla
    assert "production runtime may omit" in tla
    assert "does not prove Python" in tla
    assert "pickletools, SHA-256" in tla
    assert "RuntimeAcceptanceRequiresPinnedCohort" in configuration
    assert "ValidAcceptedAtCompletion" in configuration
