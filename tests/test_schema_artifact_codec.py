from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import zipfile
import zlib
from pathlib import Path
from typing import Any, TypedDict, cast

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from h2hdb import _schema_artifact_codec as codec

ROOT = Path(__file__).resolve().parents[1]


class _Metadata(TypedDict):
    compressed_size: int
    compressed_sha256: str
    raw_size: int
    raw_sha256: str


def _metadata(compressed: bytes, raw: bytes) -> _Metadata:
    return {
        "compressed_size": len(compressed),
        "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
        "raw_size": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _decode_wire(raw: bytes) -> dict[str, Any]:
    compressed = zlib.compress(raw, level=9)
    return codec.decode_schema_artifact(compressed, **_metadata(compressed, raw))


def _canonical_value(value: object) -> object:
    if type(value) is dict:
        mapping = cast(dict[str, object], value)
        return {key: _canonical_value(mapping[key]) for key in sorted(mapping)}
    if type(value) is list:
        return [_canonical_value(item) for item in value]
    if type(value) is tuple:
        return tuple(_canonical_value(item) for item in value)
    return value


def _assert_structurally_identical(actual: object, expected: object) -> None:
    assert type(actual) is type(expected)
    if type(expected) is dict:
        actual_mapping = cast(dict[object, object], actual)
        expected_mapping = cast(dict[object, object], expected)
        assert tuple(actual_mapping) == tuple(expected_mapping)
        for key in expected_mapping:
            _assert_structurally_identical(actual_mapping[key], expected_mapping[key])
        return
    if type(expected) in {list, tuple}:
        actual_sequence = cast(list[object] | tuple[object, ...], actual)
        expected_sequence = cast(list[object] | tuple[object, ...], expected)
        assert len(actual_sequence) == len(expected_sequence)
        for actual_item, expected_item in zip(actual_sequence, expected_sequence):
            _assert_structurally_identical(actual_item, expected_item)
        return
    assert actual == expected


def test_codec_preserves_types_values_and_canonical_dictionary_order() -> None:
    value = {
        "z": [True, 1, (b"\x00\xff", None)],
        "a": {"z": ["\x00tuple", []], "a": (False, 0)},
        "tags_are_plain_values": ["\x00bytes", "\x00tuple"],
    }

    decoded = codec.decode_schema_artifact_raw(codec.encode_schema_artifact(value))

    _assert_structurally_identical(decoded, _canonical_value(value))
    assert type(decoded["z"][0]) is bool
    assert type(decoded["z"][1]) is int
    assert type(decoded["z"][2]) is tuple
    assert type(decoded["z"][2][0]) is bytes


_key_strategy = st.text(min_size=1, max_size=8).filter(
    lambda value: not value.startswith("\x00")
)
_scalar_strategy = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**80), max_value=2**80),
    st.text(max_size=16),
    st.binary(max_size=16),
)
_value_strategy = st.recursive(
    _scalar_strategy,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.lists(children, max_size=5).map(tuple),
        st.dictionaries(_key_strategy, children, max_size=5),
    ),
    max_leaves=40,
)


@given(st.dictionaries(_key_strategy, _value_strategy, max_size=6))
@settings(max_examples=100, deadline=None)
def test_codec_property_roundtrip(value: dict[str, object]) -> None:
    decoded = codec.decode_schema_artifact_raw(codec.encode_schema_artifact(value))

    _assert_structurally_identical(decoded, _canonical_value(value))


@pytest.mark.parametrize(
    "value, match",
    [
        ({"\x00tuple": []}, "reserved"),
        ({1: "value"}, "string keys"),
        ({"value": 1.5}, "unsupported type"),
        ({"value": "\ud800"}, "surrogate"),
    ],
)
def test_encoder_rejects_values_outside_the_closed_type_surface(
    value: object, match: str
) -> None:
    with pytest.raises(codec.SchemaArtifactCodecError, match=match):
        codec.encode_schema_artifact(value)


def test_encoder_rejects_container_cycles() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    with pytest.raises(codec.SchemaArtifactCodecError, match="cycle"):
        codec.encode_schema_artifact({"value": cyclic})


@pytest.mark.parametrize(
    "raw, match",
    [
        (b'{"a":1,"a":2}', "duplicate object key"),
        (b'{"b":1,"a":2}', "strictly canonical"),
        (b'{"a":NaN}', "forbidden constant"),
        (b'{"a":1.5}', "cannot contain a float"),
        (b'{"\\u0061":1}', "noncanonical Unicode escape"),
        (b'{"a":"\\/"}', "noncanonical escape"),
        (b'{"a":"\\u000B"}', "noncanonical Unicode escape"),
        (b'{"a":1} ', "canonical compact"),
        (b'{"a":1}x', "trailing data"),
        (b'{"\\u0000future":1}', "unknown tag"),
        (b'{"\\u0000tuple":{}}', "tuple tag"),
        (b'{"a":{"\\u0000bytes":"AA"}}', "lowercase hex"),
        (b'{"a":{"\\u0000bytes":"a"}}', "lowercase hex"),
        (b"[]", "root must be a dictionary"),
    ],
)
def test_decoder_rejects_noncanonical_or_invalid_json(raw: bytes, match: str) -> None:
    with pytest.raises(codec.SchemaArtifactCodecError, match=match):
        _decode_wire(raw)


def test_decoder_rejects_invalid_utf8() -> None:
    with pytest.raises(codec.SchemaArtifactCodecError, match="UTF-8"):
        _decode_wire(b'{"a":"\xff"}')


def test_decoder_rejects_compressed_digest_drift() -> None:
    raw = b"{}"
    compressed = zlib.compress(raw)
    metadata = _metadata(compressed, raw)
    metadata["compressed_sha256"] = "0" * 64

    with pytest.raises(codec.SchemaArtifactCodecError, match="compressed digest"):
        codec.decode_schema_artifact(compressed, **metadata)


def test_decoder_rejects_raw_digest_drift() -> None:
    raw = b"{}"
    compressed = zlib.compress(raw)
    metadata = _metadata(compressed, raw)
    metadata["raw_sha256"] = "0" * 64

    with pytest.raises(codec.SchemaArtifactCodecError, match="raw digest"):
        codec.decode_schema_artifact(compressed, **metadata)


def test_decoder_rejects_a_corrupted_zlib_stream_with_fresh_outer_digest() -> None:
    raw = codec.encode_schema_artifact({"value": tuple(range(128))})
    compressed = bytearray(zlib.compress(raw))
    compressed[len(compressed) // 2] ^= 0x80
    corrupted = bytes(compressed)

    with pytest.raises(codec.SchemaArtifactCodecError, match="zlib|raw digest"):
        codec.decode_schema_artifact(
            corrupted,
            **_metadata(corrupted, raw),
        )


def test_decoder_rejects_a_truncated_zlib_stream() -> None:
    raw = codec.encode_schema_artifact({"value": "present"})
    compressed = zlib.compress(raw)[:-1]

    with pytest.raises(codec.SchemaArtifactCodecError, match="truncated|invalid"):
        codec.decode_schema_artifact(compressed, **_metadata(compressed, raw))


def test_decoder_rejects_trailing_zlib_data() -> None:
    raw = codec.encode_schema_artifact({"value": "present"})
    compressed = zlib.compress(raw) + b"trailing"

    with pytest.raises(codec.SchemaArtifactCodecError, match="trailing data"):
        codec.decode_schema_artifact(compressed, **_metadata(compressed, raw))


def test_decoder_rejects_compressed_input_beyond_the_hard_cap() -> None:
    compressed = b"x" * (codec.MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES + 1)

    with pytest.raises(codec.SchemaArtifactCodecError, match="hard bound"):
        codec.decode_schema_artifact(
            compressed,
            compressed_size=len(compressed),
            compressed_sha256=hashlib.sha256(compressed).hexdigest(),
            raw_size=0,
            raw_sha256=hashlib.sha256(b"").hexdigest(),
        )


def test_decoder_rejects_a_small_compressed_bomb() -> None:
    oversized_raw = b"0" * (codec.MAX_SCHEMA_ARTIFACT_RAW_BYTES + 1)
    compressed = zlib.compress(oversized_raw, level=9)

    with pytest.raises(codec.SchemaArtifactCodecError, match="expands beyond"):
        codec.decode_schema_artifact(
            compressed,
            compressed_size=len(compressed),
            compressed_sha256=hashlib.sha256(compressed).hexdigest(),
            raw_size=codec.MAX_SCHEMA_ARTIFACT_RAW_BYTES,
            raw_sha256="0" * 64,
        )


def test_decoder_enforces_the_semantic_node_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = codec.encode_schema_artifact({"value": [1, 2, 3, 4]})
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_NODES", 4)

    with pytest.raises(codec.SchemaArtifactCodecError, match="node count"):
        codec.decode_schema_artifact_raw(raw)


def test_decoder_enforces_the_semantic_depth_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = codec.encode_schema_artifact({"value": [[[[1]]]]})
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_DEPTH", 3)

    with pytest.raises(codec.SchemaArtifactCodecError, match="semantic depth"):
        codec.decode_schema_artifact_raw(raw)


def test_resource_loader_rejects_nonfixed_or_missing_resources() -> None:
    raw = b"{}"
    compressed = zlib.compress(raw)
    metadata = _metadata(compressed, raw)

    with pytest.raises(codec.SchemaArtifactCodecError, match="identity"):
        codec.load_schema_artifact_resource(
            package="h2hdb",
            resource_name="../_generated_vnext_schema.bin",
            **metadata,
        )
    with pytest.raises(codec.SchemaArtifactCodecError, match="cannot be read"):
        codec.load_schema_artifact_resource(
            package="h2hdb_missing_package_for_codec_test",
            resource_name="_generated_vnext_schema.bin",
            **metadata,
        )


def test_generated_artifact_resource_supports_direct_zipimport(tmp_path: Path) -> None:
    package_zip = tmp_path / "schema-package.zip"
    with zipfile.ZipFile(package_zip, "w") as archive:
        archive.writestr("h2hdb/__init__.py", "")
        for name in (
            "_schema_artifact_codec.py",
            "_generated_vnext_schema.py",
            "_generated_vnext_schema.bin",
        ):
            archive.write(ROOT / "src" / "h2hdb" / name, f"h2hdb/{name}")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(package_zip)
    probe = """
import h2hdb
from h2hdb._generated_vnext_schema import ARTIFACT
assert ARTIFACT["epoch"] == 3
assert ARTIFACT["backends"]["sqlite"]["relations"]
assert ".zip/h2hdb/__init__.py" in h2hdb.__file__
"""

    subprocess.run(
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
