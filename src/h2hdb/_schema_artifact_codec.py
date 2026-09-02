"""Bounded canonical codec for the wheel-resident schema artifact.

Only primitive immutable/schema-description values cross this boundary.  The
wire form is canonical JSON with native dictionaries/lists and collision-free
reserved tags for tuples/bytes, then zlib compression.  Decoding is
deliberately fail closed and does not import any verification-only package.
"""

from __future__ import annotations

__all__ = [
    "MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES",
    "MAX_SCHEMA_ARTIFACT_DEPTH",
    "MAX_SCHEMA_ARTIFACT_NODES",
    "MAX_SCHEMA_ARTIFACT_RAW_BYTES",
    "SchemaArtifactCodecError",
    "compress_schema_artifact",
    "decode_schema_artifact",
    "decode_schema_artifact_raw",
    "encode_schema_artifact",
    "load_schema_artifact_resource",
]

import hashlib
import hmac
import json
import re
import zlib
from importlib.resources import files
from typing import Any, Final, cast

MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES: Final = 2 * 1024 * 1024
MAX_SCHEMA_ARTIFACT_RAW_BYTES: Final = 32 * 1024 * 1024
MAX_SCHEMA_ARTIFACT_NODES: Final = 1_000_000
MAX_SCHEMA_ARTIFACT_DEPTH: Final = 64

_MAX_JSON_DEPTH: Final = 2 * MAX_SCHEMA_ARTIFACT_DEPTH + 8
_MAX_INTEGER_DIGITS: Final = 128
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_BYTE_HEX_PATTERN = re.compile(r"(?:[0-9a-f]{2})*")
_INTEGER_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)")
_UNICODE_ESCAPE_PATTERN = re.compile(r"[0-9a-f]{4}")
_SCHEMA_ARTIFACT_RESOURCE: Final = "_generated_vnext_schema.bin"
_RESERVED_KEY_PREFIX: Final = "\x00"
_BYTES_TAG: Final = f"{_RESERVED_KEY_PREFIX}bytes"
_TUPLE_TAG: Final = f"{_RESERVED_KEY_PREFIX}tuple"


class SchemaArtifactCodecError(ValueError):
    """The generated artifact resource is malformed or exceeds its bounds."""


class _Budget:
    __slots__ = ("nodes",)

    def __init__(self) -> None:
        self.nodes = 0

    def visit(self, depth: int) -> None:
        if depth > MAX_SCHEMA_ARTIFACT_DEPTH:
            raise SchemaArtifactCodecError(
                "schema artifact exceeds the maximum semantic depth"
            )
        self.nodes += 1
        if self.nodes > MAX_SCHEMA_ARTIFACT_NODES:
            raise SchemaArtifactCodecError(
                "schema artifact exceeds the maximum semantic node count"
            )


class _CanonicalEncoder:
    __slots__ = ("_active_containers", "_budget", "_output")

    def __init__(self) -> None:
        self._active_containers: set[int] = set()
        self._budget = _Budget()
        self._output = bytearray()

    def encode(self, value: object) -> bytes:
        self._node(value, depth=0)
        return bytes(self._output)

    def _write(self, value: bytes) -> None:
        if len(self._output) + len(value) > MAX_SCHEMA_ARTIFACT_RAW_BYTES:
            raise SchemaArtifactCodecError(
                "schema artifact exceeds the maximum uncompressed size"
            )
        self._output.extend(value)

    def _container(self, value: object) -> int:
        identity = id(value)
        if identity in self._active_containers:
            raise SchemaArtifactCodecError("schema artifact contains a container cycle")
        self._active_containers.add(identity)
        return identity

    def _node(self, value: object, *, depth: int) -> None:
        self._budget.visit(depth)
        value_type = type(value)
        if value is None:
            self._write(b"null")
            return
        if value_type is bool:
            self._write(b"true" if value else b"false")
            return
        if value_type is int:
            rendered = str(value)
            if len(rendered.removeprefix("-")) > _MAX_INTEGER_DIGITS:
                raise SchemaArtifactCodecError(
                    "schema artifact integer exceeds the digit bound"
                )
            self._write(rendered.encode("ascii"))
            return
        if value_type is str:
            string = cast(str, value)
            _validate_unicode(string)
            rendered = json.dumps(string, ensure_ascii=False, separators=(",", ":"))
            self._write(rendered.encode("utf-8"))
            return
        if value_type is bytes:
            byte_value = cast(bytes, value)
            self._write(b'{"\\u0000bytes":"')
            self._write(byte_value.hex().encode("ascii"))
            self._write(b'"}')
            return
        if value_type not in {dict, list, tuple}:
            raise SchemaArtifactCodecError(
                f"schema artifact contains unsupported type {value_type.__name__!r}"
            )

        identity = self._container(value)
        try:
            if value_type is dict:
                mapping = cast(dict[object, object], value)
                if not all(type(key) is str for key in mapping):
                    raise SchemaArtifactCodecError(
                        "schema artifact dictionaries require string keys"
                    )
                string_mapping = cast(dict[str, object], mapping)
                if any(key.startswith(_RESERVED_KEY_PREFIX) for key in string_mapping):
                    raise SchemaArtifactCodecError(
                        "schema artifact dictionary uses a reserved codec key"
                    )
                self._write(b"{")
                for index, key in enumerate(sorted(string_mapping)):
                    if index:
                        self._write(b",")
                    self._node(key, depth=depth + 1)
                    self._write(b":")
                    self._node(string_mapping[key], depth=depth + 1)
                self._write(b"}")
                return

            sequence = cast(list[object] | tuple[object, ...], value)
            if value_type is tuple:
                self._write(b'{"\\u0000tuple":')
            self._write(b"[")
            for index, item in enumerate(sequence):
                if index:
                    self._write(b",")
                self._node(item, depth=depth + 1)
            self._write(b"]")
            if value_type is tuple:
                self._write(b"}")
        finally:
            self._active_containers.remove(identity)


def _validate_unicode(value: str) -> None:
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise SchemaArtifactCodecError(
            "schema artifact contains a surrogate code point"
        )


def _validate_sha256(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise SchemaArtifactCodecError(f"{field} is not a canonical SHA-256 digest")
    return value


def _validate_size(value: object, *, field: str, maximum: int) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        raise SchemaArtifactCodecError(f"{field} is outside its hard bound")
    return value


def encode_schema_artifact(value: object) -> bytes:
    """Encode supported primitive containers into bounded canonical JSON."""

    return _CanonicalEncoder().encode(value)


def compress_schema_artifact(raw: bytes) -> bytes:
    """Compress one already-canonical bounded artifact payload."""

    if type(raw) is not bytes or len(raw) > MAX_SCHEMA_ARTIFACT_RAW_BYTES:
        raise SchemaArtifactCodecError(
            "schema artifact raw payload is outside its bound"
        )
    compressed = zlib.compress(raw, level=9)
    if len(compressed) > MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES:
        raise SchemaArtifactCodecError(
            "schema artifact compressed payload is outside its bound"
        )
    return compressed


def _reject_float(_value: str) -> float:
    raise SchemaArtifactCodecError("schema artifact JSON cannot contain a float")


def _reject_constant(value: str) -> object:
    raise SchemaArtifactCodecError(
        f"schema artifact JSON contains forbidden constant {value!r}"
    )


def _parse_integer(value: str) -> int:
    if (
        _INTEGER_PATTERN.fullmatch(value) is None
        or len(value.removeprefix("-")) > _MAX_INTEGER_DIGITS
    ):
        raise SchemaArtifactCodecError("schema artifact JSON integer is not canonical")
    return int(value)


def _object_pairs(pairs: list[tuple[str, object]]) -> object:
    result: dict[str, object] = {}
    previous_key: str | None = None
    for key, value in pairs:
        if key in result:
            raise SchemaArtifactCodecError(
                f"schema artifact JSON contains duplicate object key {key!r}"
            )
        _validate_unicode(key)
        if previous_key is not None and key <= previous_key:
            raise SchemaArtifactCodecError(
                "schema artifact dictionary keys are not strictly canonical"
            )
        result[key] = value
        previous_key = key
    reserved_keys = tuple(key for key in result if key.startswith(_RESERVED_KEY_PREFIX))
    if not reserved_keys:
        return result
    if len(result) != 1 or len(reserved_keys) != 1:
        raise SchemaArtifactCodecError(
            "schema artifact dictionary uses a reserved codec key"
        )
    tag = reserved_keys[0]
    payload = result[tag]
    if tag == _TUPLE_TAG:
        if type(payload) is not list:
            raise SchemaArtifactCodecError(
                "schema artifact tuple tag does not contain an array"
            )
        decoded_tuple = tuple(payload)
        payload.clear()
        return decoded_tuple
    if tag == _BYTES_TAG:
        if type(payload) is not str or _BYTE_HEX_PATTERN.fullmatch(payload) is None:
            raise SchemaArtifactCodecError(
                "schema artifact byte value is not canonical lowercase hex"
            )
        return bytes.fromhex(payload)
    raise SchemaArtifactCodecError(f"schema artifact contains unknown tag {tag!r}")


def _validate_json_string_spelling(text: str, *, start: int, end: int) -> None:
    cursor = text.find("\\", start + 1, end)
    while cursor >= 0:
        if cursor + 1 >= end:
            raise SchemaArtifactCodecError(
                "schema artifact JSON string escape is truncated"
            )
        escape = text[cursor + 1]
        if escape in {'"', "\\", "b", "f", "n", "r", "t"}:
            cursor = text.find("\\", cursor + 2, end)
            continue
        if escape == "u":
            digits = text[cursor + 2 : cursor + 6]
            if len(digits) != 4 or _UNICODE_ESCAPE_PATTERN.fullmatch(digits) is None:
                raise SchemaArtifactCodecError(
                    "schema artifact JSON string has a noncanonical Unicode escape"
                )
            code_point = int(digits, 16)
            if code_point > 0x1F or code_point in {0x08, 0x09, 0x0A, 0x0C, 0x0D}:
                raise SchemaArtifactCodecError(
                    "schema artifact JSON string has a noncanonical Unicode escape"
                )
            cursor = text.find("\\", cursor + 6, end)
            continue
        raise SchemaArtifactCodecError(
            "schema artifact JSON string has a noncanonical escape"
        )


def _preflight_json(text: str) -> None:
    depth = 0
    index = 0
    while index < len(text):
        character = text[index]
        if character == '"':
            candidate = text.find('"', index + 1)
            while candidate >= 0:
                backslashes = 0
                cursor = candidate - 1
                while cursor > index and text[cursor] == "\\":
                    backslashes += 1
                    cursor -= 1
                if backslashes % 2 == 0:
                    break
                candidate = text.find('"', candidate + 1)
            if candidate < 0:
                raise SchemaArtifactCodecError("schema artifact JSON is truncated")
            _validate_json_string_spelling(text, start=index, end=candidate)
            index = candidate + 1
            continue
        if character in "[{":
            depth += 1
            if depth > _MAX_JSON_DEPTH:
                raise SchemaArtifactCodecError(
                    "schema artifact exceeds the maximum JSON nesting depth"
                )
        elif character in "]}":
            depth -= 1
            if depth < 0:
                raise SchemaArtifactCodecError(
                    "schema artifact JSON closes an unopened container"
                )
        elif character in " \t\r\n":
            raise SchemaArtifactCodecError(
                "schema artifact JSON is not in canonical compact form"
            )
        index += 1
    if depth != 0:
        raise SchemaArtifactCodecError("schema artifact JSON is truncated")


def _validate_decoded_node(value: object, *, budget: _Budget, depth: int) -> None:
    budget.visit(depth)
    value_type = type(value)
    if value is None or value_type in {bool, int, bytes}:
        return
    if value_type is str:
        string = cast(str, value)
        _validate_unicode(string)
        return
    if value_type is dict:
        mapping = cast(dict[object, object], value)
        previous_key: str | None = None
        for key, item in mapping.items():
            if type(key) is not str:
                raise SchemaArtifactCodecError(
                    "schema artifact dictionary contains a non-string key"
                )
            if previous_key is not None and key <= previous_key:
                raise SchemaArtifactCodecError(
                    "schema artifact dictionary keys are not strictly canonical"
                )
            previous_key = key
            _validate_decoded_node(key, budget=budget, depth=depth + 1)
            _validate_decoded_node(item, budget=budget, depth=depth + 1)
        return
    if value_type in {list, tuple}:
        sequence = cast(list[object] | tuple[object, ...], value)
        for item in sequence:
            _validate_decoded_node(item, budget=budget, depth=depth + 1)
        return
    raise SchemaArtifactCodecError(
        f"schema artifact contains unsupported decoded type {value_type.__name__!r}"
    )


def _decode_schema_artifact_text(text: str) -> dict[str, Any]:
    _preflight_json(text)
    decoder = json.JSONDecoder(
        object_pairs_hook=_object_pairs,
        parse_float=_reject_float,
        parse_int=_parse_integer,
        parse_constant=_reject_constant,
        strict=True,
    )
    try:
        parsed, end = decoder.raw_decode(text)
    except (RecursionError, json.JSONDecodeError) as error:
        raise SchemaArtifactCodecError("schema artifact JSON is malformed") from error
    if end != len(text):
        raise SchemaArtifactCodecError("schema artifact JSON contains trailing data")
    del text
    _validate_decoded_node(parsed, budget=_Budget(), depth=0)
    if type(parsed) is not dict or not all(type(key) is str for key in parsed):
        raise SchemaArtifactCodecError(
            "schema artifact root must be a dictionary with string keys"
        )
    return cast(dict[str, Any], parsed)


def _decode_utf8(raw: bytes) -> str:
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise SchemaArtifactCodecError(
            "schema artifact JSON is not valid UTF-8"
        ) from error


def decode_schema_artifact_raw(raw: bytes) -> dict[str, Any]:
    """Decode one bounded canonical JSON payload without decompression."""

    if type(raw) is not bytes or len(raw) > MAX_SCHEMA_ARTIFACT_RAW_BYTES:
        raise SchemaArtifactCodecError(
            "schema artifact raw payload is outside its bound"
        )
    return _decode_schema_artifact_text(_decode_utf8(raw))


def decode_schema_artifact(
    compressed: bytes,
    *,
    compressed_size: int,
    compressed_sha256: str,
    raw_size: int,
    raw_sha256: str,
) -> dict[str, Any]:
    """Authenticate, decompress, and decode one bounded artifact resource."""

    expected_compressed_size = _validate_size(
        compressed_size,
        field="schema artifact compressed size",
        maximum=MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES,
    )
    expected_raw_size = _validate_size(
        raw_size,
        field="schema artifact raw size",
        maximum=MAX_SCHEMA_ARTIFACT_RAW_BYTES,
    )
    expected_compressed_digest = _validate_sha256(
        compressed_sha256, field="schema artifact compressed digest"
    )
    expected_raw_digest = _validate_sha256(
        raw_sha256, field="schema artifact raw digest"
    )
    if type(compressed) is not bytes:
        raise SchemaArtifactCodecError("schema artifact resource must contain bytes")
    if len(compressed) != expected_compressed_size:
        raise SchemaArtifactCodecError("schema artifact compressed size does not match")
    if not hmac.compare_digest(
        hashlib.sha256(compressed).hexdigest(), expected_compressed_digest
    ):
        raise SchemaArtifactCodecError(
            "schema artifact compressed digest does not match"
        )

    decompressor = zlib.decompressobj()
    try:
        raw = decompressor.decompress(compressed, MAX_SCHEMA_ARTIFACT_RAW_BYTES + 1)
    except zlib.error as error:
        raise SchemaArtifactCodecError(
            "schema artifact zlib stream is invalid"
        ) from error
    if len(raw) > MAX_SCHEMA_ARTIFACT_RAW_BYTES or decompressor.unconsumed_tail:
        raise SchemaArtifactCodecError(
            "schema artifact expands beyond the uncompressed hard bound"
        )
    if not decompressor.eof:
        raise SchemaArtifactCodecError("schema artifact zlib stream is truncated")
    if decompressor.unused_data:
        raise SchemaArtifactCodecError("schema artifact zlib stream has trailing data")
    if len(raw) != expected_raw_size:
        raise SchemaArtifactCodecError("schema artifact raw size does not match")
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_raw_digest):
        raise SchemaArtifactCodecError("schema artifact raw digest does not match")
    text = _decode_utf8(raw)
    del raw
    return _decode_schema_artifact_text(text)


def load_schema_artifact_resource(
    *,
    package: str,
    resource_name: str,
    compressed_size: int,
    compressed_sha256: str,
    raw_size: int,
    raw_sha256: str,
) -> dict[str, Any]:
    """Read one package resource without ever buffering beyond the hard cap."""

    expected_size = _validate_size(
        compressed_size,
        field="schema artifact compressed size",
        maximum=MAX_SCHEMA_ARTIFACT_COMPRESSED_BYTES,
    )
    if (
        type(package) is not str
        or not package
        or type(resource_name) is not str
        or resource_name != _SCHEMA_ARTIFACT_RESOURCE
    ):
        raise SchemaArtifactCodecError("schema artifact resource identity is invalid")
    try:
        resource = files(package).joinpath(resource_name)
        with resource.open("rb") as stream:
            compressed = stream.read(expected_size + 1)
            has_more = bool(stream.read(1))
    except (ImportError, LookupError, OSError, TypeError) as error:
        raise SchemaArtifactCodecError(
            "schema artifact resource cannot be read"
        ) from error
    if has_more or len(compressed) > expected_size:
        raise SchemaArtifactCodecError(
            "schema artifact resource exceeds its declared size"
        )
    return decode_schema_artifact(
        compressed,
        compressed_size=compressed_size,
        compressed_sha256=compressed_sha256,
        raw_size=raw_size,
        raw_sha256=raw_sha256,
    )
