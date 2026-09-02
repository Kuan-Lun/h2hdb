from __future__ import annotations

import ast
import hashlib
import io
import os
import pickle
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any, TypedDict, cast

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from h2hdb import _schema_artifact_codec as codec

ROOT = Path(__file__).resolve().parents[1]
GENERATED_LOADER = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
GENERATED_RESOURCE = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.bin"


class _Metadata(TypedDict):
    pickle_protocol: int
    raw_size: int
    raw_sha256: str


def _metadata(raw: bytes) -> _Metadata:
    return {
        "pickle_protocol": codec.SCHEMA_ARTIFACT_PICKLE_PROTOCOL,
        "raw_size": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _decode(raw: bytes) -> dict[str, Any]:
    return codec.decode_schema_artifact(raw, **_metadata(raw))


def _generated_metadata() -> _Metadata:
    tree = ast.parse(GENERATED_LOADER.read_text(encoding="utf-8"))
    values = {
        target.id: ast.literal_eval(statement.value)
        for statement in tree.body
        if isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance((target := statement.targets[0]), ast.Name)
        and target.id in {"_PICKLE_PROTOCOL", "_RAW_SIZE", "_RAW_SHA256"}
    }
    return {
        "pickle_protocol": cast(int, values["_PICKLE_PROTOCOL"]),
        "raw_size": cast(int, values["_RAW_SIZE"]),
        "raw_sha256": cast(str, values["_RAW_SHA256"]),
    }


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
        "\x00plain_key": ["\x00bytes", "\x00tuple"],
    }

    decoded = _decode(codec.encode_schema_artifact(value))

    _assert_structurally_identical(decoded, _canonical_value(value))
    assert type(decoded["z"][0]) is bool
    assert type(decoded["z"][1]) is int
    assert type(decoded["z"][2]) is tuple
    assert type(decoded["z"][2][0]) is bytes


def test_tuple_interning_does_not_conflate_nested_bool_and_int_values() -> None:
    value = {
        "bool": ((True,),),
        "int": ((1,),),
        "false": ((False,),),
        "zero": ((0,),),
    }

    decoded = _decode(codec.encode_schema_artifact(value))

    _assert_structurally_identical(decoded, _canonical_value(value))
    assert type(decoded["bool"][0][0]) is bool
    assert type(decoded["int"][0][0]) is int
    assert type(decoded["false"][0][0]) is bool
    assert type(decoded["zero"][0][0]) is int


_key_strategy = st.text(min_size=1, max_size=8)
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
    decoded = _decode(codec.encode_schema_artifact(value))

    _assert_structurally_identical(decoded, _canonical_value(value))


@given(st.binary(max_size=256))
@settings(max_examples=200, deadline=None)
def test_authenticated_malformed_bytes_never_escape_as_parser_errors(
    raw: bytes,
) -> None:
    try:
        decoded = _decode(raw)
    except codec.SchemaArtifactCodecError:
        return
    assert type(decoded) is dict


@pytest.mark.parametrize(
    "value, match",
    [
        ({1: "value"}, "string keys"),
        ({"value": 1.5}, "unsupported type"),
        ({"value": {1, 2}}, "unsupported type"),
        ({"value": bytearray(b"x")}, "unsupported type"),
        ({"value": "\ud800"}, "surrogate"),
        ({"value": 1 << 500}, "integer"),
        (["not", "a", "mapping"], "root must be a dictionary"),
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


def test_decoder_rejects_digest_drift_before_pickle_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = codec.encode_schema_artifact({"value": [1, 2, 3]})
    metadata = _metadata(raw)
    metadata["raw_sha256"] = "0" * 64
    monkeypatch.setattr(
        codec,
        "_preflight_pickle",
        lambda _raw: pytest.fail("pickle parser ran before digest authentication"),
    )

    with pytest.raises(codec.SchemaArtifactCodecError, match="digest does not match"):
        codec.decode_schema_artifact(raw, **metadata)


@pytest.mark.parametrize("digest", ["", "A" * 64, "0" * 63, 1])
def test_decoder_rejects_noncanonical_digest_metadata(digest: object) -> None:
    raw = codec.encode_schema_artifact({})
    metadata = cast(dict[str, object], _metadata(raw))
    metadata["raw_sha256"] = digest
    decoder = cast(Any, codec.decode_schema_artifact)

    with pytest.raises(codec.SchemaArtifactCodecError, match="canonical SHA-256"):
        decoder(raw, **metadata)


@pytest.mark.parametrize("protocol", [4, 6, True, "5"])
def test_decoder_rejects_nonfixed_protocol_metadata(protocol: object) -> None:
    raw = codec.encode_schema_artifact({})
    metadata = cast(dict[str, object], _metadata(raw))
    metadata["pickle_protocol"] = protocol
    decoder = cast(Any, codec.decode_schema_artifact)

    with pytest.raises(codec.SchemaArtifactCodecError, match="protocol is unsupported"):
        decoder(raw, **metadata)


def test_decoder_rejects_protocol_four_payload_under_protocol_five_contract() -> None:
    raw = pickle.dumps({}, protocol=4)

    with pytest.raises(codec.SchemaArtifactCodecError, match="protocol 5"):
        _decode(raw)


def test_decoder_rejects_truncated_and_trailing_pickle_data() -> None:
    raw = codec.encode_schema_artifact({"value": "present"})

    with pytest.raises(codec.SchemaArtifactCodecError, match="truncated|malformed"):
        _decode(raw[:-1])
    with pytest.raises(codec.SchemaArtifactCodecError, match="trailing data"):
        _decode(raw + b"trailing")


def test_decoder_rejects_a_corrupt_payload_with_stale_digest() -> None:
    raw = codec.encode_schema_artifact({"value": tuple(range(128))})
    corrupted = bytearray(raw)
    corrupted[len(corrupted) // 2] ^= 0x80

    with pytest.raises(codec.SchemaArtifactCodecError, match="digest does not match"):
        codec.decode_schema_artifact(bytes(corrupted), **_metadata(raw))


@pytest.mark.parametrize(
    "raw, opcode",
    [
        (b"\x80\x05cbuiltins\nlist\n.", "GLOBAL"),
        (pickle.dumps(len, protocol=5), "STACK_GLOBAL"),
        (b"\x80\x05N)R.", "REDUCE"),
        (b"\x80\x05NNb.", "BUILD"),
        (b"\x80\x05N)\x81.", "NEWOBJ"),
        (b"\x80\x05N)}\x92.", "NEWOBJ_EX"),
        (b"\x80\x05Pexternal\n.", "PERSID"),
        (b"\x80\x05NQ.", "BINPERSID"),
        (b"\x80\x05\x82\x01.", "EXT1"),
        (b"\x80\x05\x83\x01\x00.", "EXT2"),
        (b"\x80\x05\x84\x01\x00\x00\x00.", "EXT4"),
        (b"\x80\x05\x97.", "NEXT_BUFFER"),
        (pickle.dumps(1.5, protocol=5), "BINFLOAT"),
        (pickle.dumps({1, 2}, protocol=5), "EMPTY_SET"),
        (pickle.dumps(bytearray(b"x"), protocol=5), "BYTEARRAY8"),
    ],
)
def test_decoder_rejects_every_nonprimitive_or_executable_opcode(
    raw: bytes, opcode: str
) -> None:
    with pytest.raises(codec.SchemaArtifactCodecError, match=opcode):
        _decode(raw)


def test_restricted_unpickler_independently_rejects_external_hooks() -> None:
    unpickler = codec._RestrictedUnpickler(io.BytesIO(b""))

    with pytest.raises(codec.SchemaArtifactCodecError, match="global"):
        unpickler.find_class("builtins", "list")
    with pytest.raises(codec.SchemaArtifactCodecError, match="persistent"):
        unpickler.persistent_load("external")


def test_decoder_rejects_noncanonical_dictionary_order() -> None:
    raw = pickle.dumps({"b": 1, "a": 2}, protocol=5)

    with pytest.raises(codec.SchemaArtifactCodecError, match="strictly canonical"):
        _decode(raw)


def test_decoder_rejects_shared_mutable_containers_and_cycles() -> None:
    shared: list[object] = []
    shared_raw = pickle.dumps({"a": shared, "b": shared}, protocol=5)
    cyclic: list[object] = []
    cyclic.append(cyclic)
    cyclic_raw = pickle.dumps({"value": cyclic}, protocol=5)

    with pytest.raises(codec.SchemaArtifactCodecError, match="mutable container"):
        _decode(shared_raw)
    with pytest.raises(codec.SchemaArtifactCodecError, match="mutable container"):
        _decode(cyclic_raw)


def test_preflight_rejects_an_exponentially_unfolded_immutable_memo_dag() -> None:
    shared: object = (None,)
    for _ in range(30):
        shared = (shared, shared)
    raw = pickle.dumps({"value": shared}, protocol=5)

    assert len(raw) < 1024
    with pytest.raises(codec.SchemaArtifactCodecError, match="expanded tree"):
        _decode(raw)


def test_preflight_rejects_tuple_dictionary_key_before_unpickling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = bytearray(b"\x80\x05}\x94N\x85\x94")
    memo_index = 1
    for _ in range(18):
        raw.extend((ord("h"), memo_index, 0x86, 0x94))
        memo_index += 1
    raw.extend(b"Ns.")
    monkeypatch.setattr(
        codec,
        "_RestrictedUnpickler",
        lambda _stream: pytest.fail("unpickler ran before dictionary-key validation"),
    )

    assert len(raw) < 128
    with pytest.raises(codec.SchemaArtifactCodecError, match="key is not"):
        _decode(bytes(raw))


def test_post_validation_independently_bounds_an_unfolded_tuple_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared: object = (None,)
    for _ in range(8):
        shared = (shared, shared)
    raw = pickle.dumps({"value": shared}, protocol=5)
    monkeypatch.setattr(codec, "_preflight_pickle", lambda _raw: None)
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_NODES", 128)

    with pytest.raises(codec.SchemaArtifactCodecError, match="semantic node count"):
        _decode(raw)


def test_post_validation_independently_rejects_mutable_state_below_tuple_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutable: list[object] = []
    shared = (mutable,)
    raw = pickle.dumps({"a": shared, "z": shared}, protocol=5)
    monkeypatch.setattr(codec, "_preflight_pickle", lambda _raw: None)

    with pytest.raises(codec.SchemaArtifactCodecError, match="shared mutable"):
        _decode(raw)


def test_decoder_rejects_root_and_postdecode_type_drift() -> None:
    with pytest.raises(codec.SchemaArtifactCodecError, match="root must"):
        _decode(pickle.dumps([], protocol=5))
    with pytest.raises(codec.SchemaArtifactCodecError, match="unsupported decoded"):
        codec._DecodedValidator().validate({"value": 1.5})


def test_decoder_enforces_encoded_node_and_opcode_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = (1,)
    raw = codec.encode_schema_artifact({"value": [shared] * 8})

    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_NODES", 8)
    with pytest.raises(codec.SchemaArtifactCodecError, match="bound|node count"):
        _decode(raw)

    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_NODES", 1_000_000)
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_OPCODES", 8)
    with pytest.raises(codec.SchemaArtifactCodecError, match="opcode count"):
        _decode(raw)


def test_decoder_enforces_semantic_depth_before_python_recursion_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nested: object = None
    for _ in range(100):
        nested = [nested]
    raw = pickle.dumps({"value": nested}, protocol=5)
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_DEPTH", 16)

    with pytest.raises(codec.SchemaArtifactCodecError, match="semantic depth"):
        _decode(raw)


def test_decoder_rechecks_depth_for_a_shared_tuple_at_each_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = (((1,),),)
    raw = codec.encode_schema_artifact({"a": shared, "z": [[[shared]]]})
    monkeypatch.setattr(codec, "MAX_SCHEMA_ARTIFACT_DEPTH", 5)

    with pytest.raises(codec.SchemaArtifactCodecError, match="semantic depth"):
        _decode(raw)


def test_decoder_rejects_frame_and_raw_size_bombs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    framed = codec.encode_schema_artifact({"value": "x" * 70_000})
    monkeypatch.setattr(codec, "_MAX_SCHEMA_ARTIFACT_FRAMES", 0)
    with pytest.raises(codec.SchemaArtifactCodecError, match="frame"):
        _decode(framed)

    oversized = b"x" * (codec.MAX_SCHEMA_ARTIFACT_BYTES + 1)
    with pytest.raises(codec.SchemaArtifactCodecError, match="hard bound"):
        codec.decode_schema_artifact(
            oversized,
            pickle_protocol=codec.SCHEMA_ARTIFACT_PICKLE_PROTOCOL,
            raw_size=len(oversized),
            raw_sha256=hashlib.sha256(oversized).hexdigest(),
        )


def test_decoder_rejects_a_truncated_huge_length_before_unpickling() -> None:
    raw = b"\x80\x05\x8d" + ((1 << 63) - 1).to_bytes(8, byteorder="little") + b"."

    with pytest.raises(codec.SchemaArtifactCodecError, match="malformed"):
        _decode(raw)


def test_pinned_resource_loader_is_private_and_rejects_nonfixed_identity() -> None:
    raw = codec.encode_schema_artifact({})
    metadata = _metadata(raw)

    assert "_load_pinned_schema_artifact_resource" not in codec.__all__
    with pytest.raises(codec.SchemaArtifactCodecError, match="identity"):
        codec._load_pinned_schema_artifact_resource(
            package="h2hdb",
            resource_name="../_generated_vnext_schema.bin",
            **metadata,
        )
    with pytest.raises(codec.SchemaArtifactCodecError, match="identity"):
        codec._load_pinned_schema_artifact_resource(
            package="h2hdb_missing_package_for_codec_test",
            resource_name="_generated_vnext_schema.bin",
            **metadata,
        )


def test_pinned_resource_loader_wraps_package_resource_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable_files(_package: str) -> Any:
        raise ModuleNotFoundError("fixture package unavailable")

    monkeypatch.setattr(codec, "files", unavailable_files)
    with pytest.raises(codec.SchemaArtifactCodecError, match="cannot be read"):
        codec._load_pinned_schema_artifact_resource(
            package="h2hdb",
            resource_name="_generated_vnext_schema.bin",
            **_metadata(codec.encode_schema_artifact({})),
        )


def test_pinned_resource_authenticates_before_unpickling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = _generated_metadata()
    metadata["raw_sha256"] = "0" * 64
    monkeypatch.setattr(
        codec,
        "_unpickle_and_validate_schema_artifact",
        lambda _raw: pytest.fail("unpickler ran before pinned digest authentication"),
    )

    with pytest.raises(codec.SchemaArtifactCodecError, match="digest does not match"):
        codec._load_pinned_schema_artifact_resource(
            package="h2hdb",
            resource_name="_generated_vnext_schema.bin",
            **metadata,
        )


def test_pinned_resource_skips_only_the_redundant_abstract_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        codec,
        "_preflight_pickle",
        lambda _raw: pytest.fail("pinned resource repeated its build-time preflight"),
    )

    decoded = codec._load_pinned_schema_artifact_resource(
        package="h2hdb",
        resource_name="_generated_vnext_schema.bin",
        **_generated_metadata(),
    )

    assert decoded["epoch"] == 3
    assert decoded["backends"]["sqlite"]["relations"]


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
