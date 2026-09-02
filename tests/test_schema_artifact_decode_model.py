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
    assert "list_and_tuple_tags_are_distinct" in lean
    assert "successful_exact_decode_preserves_projection" in lean
    assert "admitted_decode_preserves_schema_projection" in lean
    assert "do not prove that Python's JSON parser, zlib" in lean
    assert "InvalidNeverAccepted" in tla
    assert "AcceptanceMatchesChecks" in tla
    assert "RawDigestOK" in tla
    assert "NodeBoundOK" in tla
    assert "ClosedTypeSurface" in tla
    assert "does not prove Python, JSON, SHA-256, zlib" in tla
    assert "ValidAcceptedAtCompletion" in configuration
