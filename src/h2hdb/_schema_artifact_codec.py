"""Authenticated, bounded codec for the wheel-resident schema artifact.

The generated payload uses protocol-5 pickle only as a compact Python-native
value encoding. Generic decoding first authenticates caller-supplied bytes and
then runs a bounded abstract stack/memo interpreter before restricted
unpickling. The interpreter admits only primitive values and containers,
rejects every executable or externally resolved operation, and saturates the
unfolded tree's resource bounds.

The private production loader has a narrower trust boundary: it reads only the
fixed resource in the ``h2hdb`` package and authenticates it against size and
SHA-256 literals shipped in the same generated loader. Build, drift,
schema-surface, and distribution gates have already applied the full abstract
preflight to that exact digest, so production avoids repeating it on every
short-lived readiness process. Restricted unpickling and closed post-decode
validation remain mandatory at runtime. This is not a generic untrusted-pickle
API.
"""

from __future__ import annotations

__all__ = [
    "MAX_SCHEMA_ARTIFACT_BYTES",
    "MAX_SCHEMA_ARTIFACT_DEPTH",
    "MAX_SCHEMA_ARTIFACT_EXPANDED_BYTES",
    "MAX_SCHEMA_ARTIFACT_NODES",
    "MAX_SCHEMA_ARTIFACT_OPCODES",
    "SCHEMA_ARTIFACT_PICKLE_PROTOCOL",
    "SchemaArtifactCodecError",
    "decode_schema_artifact",
    "encode_schema_artifact",
]

import hashlib
import hmac
import io
import pickle
import re
from importlib.resources import files
from typing import Any, Final, NoReturn, cast

MAX_SCHEMA_ARTIFACT_BYTES: Final = 8 * 1024 * 1024
MAX_SCHEMA_ARTIFACT_EXPANDED_BYTES: Final = 32 * 1024 * 1024
MAX_SCHEMA_ARTIFACT_NODES: Final = 1_000_000
MAX_SCHEMA_ARTIFACT_DEPTH: Final = 64
MAX_SCHEMA_ARTIFACT_OPCODES: Final = 2_000_000
SCHEMA_ARTIFACT_PICKLE_PROTOCOL: Final = 5

_MAX_SCHEMA_ARTIFACT_FRAMES: Final = 256
_MAX_SCHEMA_ARTIFACT_SCALAR_BYTES: Final = 256 * 1024
_MAX_SCHEMA_ARTIFACT_WORK_BYTES: Final = 128 * 1024 * 1024
_MAX_INTEGER_BITS: Final = 426
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_SCHEMA_ARTIFACT_PACKAGE: Final = "h2hdb"
_SCHEMA_ARTIFACT_RESOURCE: Final = "_generated_vnext_schema.bin"

# No opcode in this set can resolve a global, invoke a callable, construct a
# class, load persistent state, or consume an out-of-band buffer. The list is
# deliberately the exact primitive/container surface emitted by CPython's
# protocol-5 pickler for the supported logical tree.
_ALLOWED_PICKLE_OPCODES: Final = frozenset(
    {
        "APPEND",
        "APPENDS",
        "BINGET",
        "BINBYTES",
        "BINBYTES8",
        "BININT",
        "BININT1",
        "BININT2",
        "BINUNICODE",
        "BINUNICODE8",
        "EMPTY_DICT",
        "EMPTY_LIST",
        "EMPTY_TUPLE",
        "FRAME",
        "LONG1",
        "LONG4",
        "LONG_BINGET",
        "MARK",
        "MEMOIZE",
        "NEWFALSE",
        "NEWTRUE",
        "NONE",
        "PROTO",
        "SETITEM",
        "SETITEMS",
        "SHORT_BINBYTES",
        "SHORT_BINUNICODE",
        "STOP",
        "TUPLE",
        "TUPLE1",
        "TUPLE2",
        "TUPLE3",
    }
)
type _ExactInternKey = tuple[str, object]


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
        self.add(1)

    def add(self, count: int) -> None:
        self.nodes += count
        if self.nodes > MAX_SCHEMA_ARTIFACT_NODES:
            raise SchemaArtifactCodecError(
                "schema artifact exceeds the maximum semantic node count"
            )


def _validate_unicode(value: str) -> None:
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise SchemaArtifactCodecError(
            "schema artifact contains a surrogate code point"
        )


def _validate_integer(value: int) -> None:
    if value.bit_length() > _MAX_INTEGER_BITS:
        raise SchemaArtifactCodecError("schema artifact integer exceeds its bound")


class _Canonicalizer:
    __slots__ = (
        "_active_containers",
        "_budget",
        "_bytes",
        "_strings",
        "_tuples",
    )

    def __init__(self) -> None:
        self._active_containers: set[int] = set()
        self._budget = _Budget()
        self._bytes: dict[bytes, bytes] = {}
        self._strings: dict[str, str] = {}
        self._tuples: dict[_ExactInternKey, tuple[object, ...]] = {}

    def normalize(self, value: object) -> object:
        normalized, _key = self._node(value, depth=0)
        if type(normalized) is not dict:
            raise SchemaArtifactCodecError(
                "schema artifact root must be a dictionary with string keys"
            )
        return normalized

    def _enter_container(self, value: object) -> int:
        identity = id(value)
        if identity in self._active_containers:
            raise SchemaArtifactCodecError("schema artifact contains a container cycle")
        self._active_containers.add(identity)
        return identity

    def _node(
        self, value: object, *, depth: int
    ) -> tuple[object, _ExactInternKey | None]:
        self._budget.visit(depth)
        value_type = type(value)
        if value is None:
            return None, ("none", None)
        if value_type is bool:
            return value, ("bool", value)
        if value_type is int:
            integer = cast(int, value)
            _validate_integer(integer)
            return integer, ("int", integer)
        if value_type is str:
            string = cast(str, value)
            _validate_unicode(string)
            canonical = self._strings.setdefault(string, string)
            return canonical, ("str", canonical)
        if value_type is bytes:
            byte_value = cast(bytes, value)
            canonical_bytes = self._bytes.setdefault(byte_value, byte_value)
            return canonical_bytes, ("bytes", canonical_bytes)
        if value_type not in {dict, list, tuple}:
            raise SchemaArtifactCodecError(
                f"schema artifact contains unsupported type {value_type.__name__!r}"
            )

        identity = self._enter_container(value)
        try:
            if value_type is dict:
                mapping = cast(dict[object, object], value)
                if not all(type(key) is str for key in mapping):
                    raise SchemaArtifactCodecError(
                        "schema artifact dictionaries require string keys"
                    )
                string_mapping = cast(dict[str, object], mapping)
                normalized_mapping: dict[str, object] = {}
                for key in sorted(string_mapping):
                    normalized_key, _ = self._node(key, depth=depth + 1)
                    normalized_value, _ = self._node(
                        string_mapping[key], depth=depth + 1
                    )
                    normalized_mapping[cast(str, normalized_key)] = normalized_value
                return normalized_mapping, None

            sequence = cast(list[object] | tuple[object, ...], value)
            normalized_items: list[object] = []
            item_keys: list[_ExactInternKey] = []
            hashable_tuple = value_type is tuple
            for item in sequence:
                normalized_item, item_key = self._node(item, depth=depth + 1)
                normalized_items.append(normalized_item)
                if item_key is None:
                    hashable_tuple = False
                else:
                    item_keys.append(item_key)
            if value_type is list:
                return normalized_items, None
            candidate = tuple(normalized_items)
            if not hashable_tuple:
                return candidate, None
            exact_key: _ExactInternKey = ("tuple", tuple(item_keys))
            canonical_tuple = self._tuples.setdefault(exact_key, candidate)
            return canonical_tuple, exact_key
        finally:
            self._active_containers.remove(identity)


class _DecodedValidator:
    __slots__ = (
        "_active_containers",
        "_budget",
        "_completed_tuples",
        "_mutable_containers",
        "_validated_strings",
    )

    def __init__(self) -> None:
        self._active_containers: set[int] = set()
        self._budget = _Budget()
        self._completed_tuples: dict[int, tuple[int, int, bool]] = {}
        self._mutable_containers: set[int] = set()
        self._validated_strings: set[int] = set()

    def validate(self, value: object) -> dict[str, Any]:
        self._node(value, depth=0)
        if type(value) is not dict or not all(type(key) is str for key in value):
            raise SchemaArtifactCodecError(
                "schema artifact root must be a dictionary with string keys"
            )
        return cast(dict[str, Any], value)

    def _node(self, value: object, *, depth: int) -> tuple[int, int, bool]:
        self._budget.visit(depth)
        value_type = type(value)
        if value is None or value_type is bool:
            return 0, 1, False
        if value_type is int:
            _validate_integer(cast(int, value))
            return 0, 1, False
        if value_type is str:
            identity = id(value)
            if identity not in self._validated_strings:
                _validate_unicode(cast(str, value))
                self._validated_strings.add(identity)
            return 0, 1, False
        if value_type is bytes:
            return 0, 1, False
        if value_type not in {dict, list, tuple}:
            raise SchemaArtifactCodecError(
                f"schema artifact contains unsupported decoded type "
                f"{value_type.__name__!r}"
            )

        identity = id(value)
        if identity in self._active_containers:
            raise SchemaArtifactCodecError("schema artifact contains a container cycle")
        if value_type is tuple and identity in self._completed_tuples:
            height, nodes, contains_mutable = self._completed_tuples[identity]
            if depth + height > MAX_SCHEMA_ARTIFACT_DEPTH:
                raise SchemaArtifactCodecError(
                    "schema artifact exceeds the maximum semantic depth"
                )
            if contains_mutable:
                raise SchemaArtifactCodecError(
                    "schema artifact contains a shared mutable container"
                )
            self._budget.add(nodes - 1)
            return height, nodes, False
        if value_type in {dict, list}:
            if identity in self._mutable_containers:
                raise SchemaArtifactCodecError(
                    "schema artifact contains a shared mutable container"
                )
            self._mutable_containers.add(identity)

        self._active_containers.add(identity)
        try:
            maximum_child_height = -1
            subtree_nodes = 1
            contains_mutable = value_type in {dict, list}
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
                    key_height, key_nodes, key_mutable = self._node(
                        key, depth=depth + 1
                    )
                    item_height, item_nodes, item_mutable = self._node(
                        item, depth=depth + 1
                    )
                    subtree_nodes += key_nodes + item_nodes
                    contains_mutable = contains_mutable or key_mutable or item_mutable
                    maximum_child_height = max(
                        maximum_child_height, key_height, item_height
                    )
            else:
                sequence = cast(list[object] | tuple[object, ...], value)
                for item in sequence:
                    child_height, child_nodes, child_mutable = self._node(
                        item, depth=depth + 1
                    )
                    subtree_nodes += child_nodes
                    contains_mutable = contains_mutable or child_mutable
                    maximum_child_height = max(
                        maximum_child_height,
                        child_height,
                    )
            height = 1 + maximum_child_height
            if value_type is tuple:
                self._completed_tuples[identity] = (
                    height,
                    subtree_nodes,
                    contains_mutable,
                )
            return height, subtree_nodes, contains_mutable
        finally:
            self._active_containers.remove(identity)


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> NoReturn:
        raise SchemaArtifactCodecError(
            f"schema artifact cannot resolve pickle global {module}.{name}"
        )

    def persistent_load(self, pid: object) -> NoReturn:
        raise SchemaArtifactCodecError(
            f"schema artifact cannot load persistent pickle id {pid!r}"
        )


def _validate_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise SchemaArtifactCodecError(
            "schema artifact digest is not a canonical SHA-256 value"
        )
    return value


def _validate_size(value: object) -> int:
    if type(value) is not int or value < 0 or value > MAX_SCHEMA_ARTIFACT_BYTES:
        raise SchemaArtifactCodecError("schema artifact size is outside its hard bound")
    return value


def _validate_protocol(value: object) -> int:
    if type(value) is not int or value != SCHEMA_ARTIFACT_PICKLE_PROTOCOL:
        raise SchemaArtifactCodecError("schema artifact pickle protocol is unsupported")
    return value


class _AbstractMark:
    __slots__ = ()


_ABSTRACT_MARK = _AbstractMark()


class _AbstractValue:
    __slots__ = (
        "contains_mutable",
        "depth",
        "kind",
        "last_key",
        "nodes",
        "string_value",
        "weight",
    )

    def __init__(
        self,
        kind: str,
        *,
        nodes: int = 1,
        weight: int = 1,
        depth: int = 0,
        contains_mutable: bool = False,
        string_value: str | None = None,
    ) -> None:
        self.kind = kind
        self.nodes = nodes
        self.weight = weight
        self.depth = depth
        self.contains_mutable = contains_mutable
        self.string_value = string_value
        self.last_key: str | None = None


type _AbstractStackItem = _AbstractMark | _AbstractValue


def _require_abstract_value(
    item: _AbstractStackItem, *, context: str
) -> _AbstractValue:
    if not isinstance(item, _AbstractValue):
        raise SchemaArtifactCodecError(
            f"schema artifact pickle has a misplaced MARK in {context}"
        )
    return item


def _pop_abstract_values(
    stack: list[_AbstractStackItem], count: int, *, context: str
) -> list[_AbstractValue]:
    if len(stack) < count:
        raise SchemaArtifactCodecError(
            f"schema artifact pickle stack underflow in {context}"
        )
    items = stack[-count:] if count else []
    if count:
        del stack[-count:]
    return [_require_abstract_value(item, context=context) for item in items]


def _pop_abstract_mark(
    stack: list[_AbstractStackItem], *, context: str
) -> list[_AbstractValue]:
    cursor = len(stack) - 1
    while cursor >= 0 and stack[cursor] is not _ABSTRACT_MARK:
        cursor -= 1
    if cursor < 0:
        raise SchemaArtifactCodecError(
            f"schema artifact pickle has no MARK for {context}"
        )
    items = stack[cursor + 1 :]
    del stack[cursor:]
    return [_require_abstract_value(item, context=context) for item in items]


def _extend_abstract_container(
    container: _AbstractValue, children: list[_AbstractValue]
) -> int:
    added_weight = 0
    for child in children:
        container.nodes += child.nodes
        container.weight += child.weight
        added_weight += child.weight
        container.depth = max(container.depth, child.depth + 1)
        container.contains_mutable = (
            container.contains_mutable or child.contains_mutable
        )
        if container.nodes > MAX_SCHEMA_ARTIFACT_NODES:
            raise SchemaArtifactCodecError(
                "schema artifact expanded tree exceeds the maximum node count"
            )
        if container.weight > MAX_SCHEMA_ARTIFACT_EXPANDED_BYTES:
            raise SchemaArtifactCodecError(
                "schema artifact expanded tree exceeds the maximum byte weight"
            )
        if container.depth > MAX_SCHEMA_ARTIFACT_DEPTH:
            raise SchemaArtifactCodecError(
                "schema artifact exceeds the maximum semantic depth"
            )
    return added_weight


def _new_abstract_tuple(children: list[_AbstractValue]) -> tuple[_AbstractValue, int]:
    result = _AbstractValue("tuple")
    added_weight = _extend_abstract_container(result, children)
    return result, added_weight


def _append_abstract_dictionary_item(
    target: _AbstractValue, key: _AbstractValue, value: _AbstractValue
) -> int:
    if key.kind != "str" or key.string_value is None:
        raise SchemaArtifactCodecError(
            "schema artifact pickle dictionary key is not an exact string"
        )
    if target.last_key is not None and key.string_value <= target.last_key:
        raise SchemaArtifactCodecError(
            "schema artifact dictionary keys are not strictly canonical"
        )
    target.last_key = key.string_value
    return _extend_abstract_container(target, [key, value])


def _preflight_pickle(raw: bytes) -> None:
    # Kept lazy so the already-authenticated wheel resource does not pay the
    # development/audit parser's import cost on every short-lived READY probe.
    import pickletools

    opcode_count = 0
    constructed_nodes = 0
    frame_count = 0
    stop_position: int | None = None
    work_bytes = 0
    stack: list[_AbstractStackItem] = []
    memo: list[_AbstractValue] = []
    try:
        for opcode, argument, position in pickletools.genops(raw):
            opcode_count += 1
            if opcode_count > MAX_SCHEMA_ARTIFACT_OPCODES:
                raise SchemaArtifactCodecError(
                    "schema artifact exceeds the maximum pickle opcode count"
                )
            name = opcode.name
            if name not in _ALLOWED_PICKLE_OPCODES:
                raise SchemaArtifactCodecError(
                    f"schema artifact contains forbidden pickle opcode {name!r}"
                )
            if opcode_count == 1:
                if (
                    name != "PROTO"
                    or position != 0
                    or argument != SCHEMA_ARTIFACT_PICKLE_PROTOCOL
                ):
                    raise SchemaArtifactCodecError(
                        "schema artifact does not begin with pickle protocol 5"
                    )
            elif name == "PROTO":
                raise SchemaArtifactCodecError(
                    "schema artifact contains a repeated pickle protocol opcode"
                )
            if name == "PROTO":
                continue
            if name == "FRAME":
                frame_count += 1
                if (
                    frame_count > _MAX_SCHEMA_ARTIFACT_FRAMES
                    or type(argument) is not int
                    or argument < 0
                    or argument > MAX_SCHEMA_ARTIFACT_BYTES
                ):
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle frame is outside its bound"
                    )
                continue
            if name == "MARK":
                stack.append(_ABSTRACT_MARK)
                if len(stack) > MAX_SCHEMA_ARTIFACT_NODES:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle stack exceeds its bound"
                    )
                continue
            if name in {"NONE", "NEWFALSE", "NEWTRUE"}:
                stack.append(_AbstractValue(name.casefold()))
                constructed_nodes += 1
            elif name in {"BININT", "BININT1", "BININT2", "LONG1", "LONG4"}:
                if type(argument) is not int:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle integer argument is invalid"
                    )
                _validate_integer(argument)
                stack.append(_AbstractValue("int"))
                constructed_nodes += 1
            elif name in {"SHORT_BINUNICODE", "BINUNICODE", "BINUNICODE8"}:
                if type(argument) is not str:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle string argument is invalid"
                    )
                _validate_unicode(argument)
                encoded_size = len(argument.encode("utf-8"))
                if encoded_size > _MAX_SCHEMA_ARTIFACT_SCALAR_BYTES:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle string exceeds its bound"
                    )
                stack.append(
                    _AbstractValue(
                        "str", weight=encoded_size + 1, string_value=argument
                    )
                )
                constructed_nodes += 1
            elif name in {"SHORT_BINBYTES", "BINBYTES", "BINBYTES8"}:
                if type(argument) is not bytes:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle bytes argument is invalid"
                    )
                if len(argument) > _MAX_SCHEMA_ARTIFACT_SCALAR_BYTES:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle bytes value exceeds its bound"
                    )
                stack.append(_AbstractValue("bytes", weight=len(argument) + 1))
                constructed_nodes += 1
            elif name == "EMPTY_LIST":
                stack.append(_AbstractValue("list", contains_mutable=True))
                constructed_nodes += 1
            elif name == "EMPTY_DICT":
                stack.append(_AbstractValue("dict", contains_mutable=True))
                constructed_nodes += 1
            elif name == "EMPTY_TUPLE":
                stack.append(_AbstractValue("tuple"))
                constructed_nodes += 1
            elif name in {"TUPLE1", "TUPLE2", "TUPLE3"}:
                item_count = int(name[-1])
                result, added = _new_abstract_tuple(
                    _pop_abstract_values(stack, item_count, context=name)
                )
                stack.append(result)
                work_bytes += added
                constructed_nodes += 1
            elif name == "TUPLE":
                result, added = _new_abstract_tuple(
                    _pop_abstract_mark(stack, context=name)
                )
                stack.append(result)
                work_bytes += added
                constructed_nodes += 1
            elif name == "MEMOIZE":
                if not stack:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle stack underflow in MEMOIZE"
                    )
                if len(memo) >= MAX_SCHEMA_ARTIFACT_NODES:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle memo exceeds its bound"
                    )
                memo.append(_require_abstract_value(stack[-1], context=name))
            elif name in {"BINGET", "LONG_BINGET"}:
                if type(argument) is not int or not 0 <= argument < len(memo):
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle memo reference is invalid"
                    )
                referenced = memo[argument]
                if referenced.contains_mutable:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle aliases a mutable container"
                    )
                stack.append(referenced)
            elif name == "APPEND":
                child = _pop_abstract_values(stack, 1, context=name)[0]
                if not stack:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle stack underflow in APPEND"
                    )
                target = _require_abstract_value(stack[-1], context=name)
                if target.kind != "list":
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle APPEND target is not a list"
                    )
                work_bytes += _extend_abstract_container(target, [child])
            elif name == "APPENDS":
                children = _pop_abstract_mark(stack, context=name)
                if not stack:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle stack underflow in APPENDS"
                    )
                target = _require_abstract_value(stack[-1], context=name)
                if target.kind != "list":
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle APPENDS target is not a list"
                    )
                work_bytes += _extend_abstract_container(target, children)
            elif name == "SETITEM":
                key, value = _pop_abstract_values(stack, 2, context=name)
                if not stack:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle stack underflow in SETITEM"
                    )
                target = _require_abstract_value(stack[-1], context=name)
                if target.kind != "dict":
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle SETITEM target is not a dictionary"
                    )
                work_bytes += _append_abstract_dictionary_item(target, key, value)
            elif name == "SETITEMS":
                items = _pop_abstract_mark(stack, context=name)
                if not stack or len(items) % 2:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle SETITEMS stack is malformed"
                    )
                target = _require_abstract_value(stack[-1], context=name)
                if target.kind != "dict":
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle SETITEMS target is not a dictionary"
                    )
                for index in range(0, len(items), 2):
                    work_bytes += _append_abstract_dictionary_item(
                        target, items[index], items[index + 1]
                    )
            elif name == "STOP":
                if len(stack) != 1:
                    raise SchemaArtifactCodecError(
                        "schema artifact pickle does not leave exactly one root"
                    )
                root = _require_abstract_value(stack[0], context=name)
                if root.kind != "dict":
                    raise SchemaArtifactCodecError(
                        "schema artifact root must be a dictionary with string keys"
                    )
                stop_position = position
            else:  # pragma: no cover - the allowlist and VM must evolve together
                raise SchemaArtifactCodecError(
                    f"schema artifact pickle opcode {name!r} has no abstract rule"
                )
            if constructed_nodes > MAX_SCHEMA_ARTIFACT_NODES:
                raise SchemaArtifactCodecError(
                    "schema artifact exceeds the maximum encoded node count"
                )
            if len(stack) > MAX_SCHEMA_ARTIFACT_NODES:
                raise SchemaArtifactCodecError(
                    "schema artifact pickle stack exceeds its bound"
                )
            if work_bytes > _MAX_SCHEMA_ARTIFACT_WORK_BYTES:
                raise SchemaArtifactCodecError(
                    "schema artifact pickle construction work exceeds its bound"
                )
    except SchemaArtifactCodecError:
        raise
    except (UnicodeError, ValueError) as error:
        raise SchemaArtifactCodecError("schema artifact pickle is malformed") from error
    if stop_position is None:
        raise SchemaArtifactCodecError("schema artifact pickle is truncated")
    if stop_position + 1 != len(raw):
        raise SchemaArtifactCodecError("schema artifact pickle has trailing data")


def encode_schema_artifact(value: object) -> bytes:
    """Encode one logical schema tree as deterministic protocol-5 pickle."""

    normalized = _Canonicalizer().normalize(value)
    try:
        raw = pickle.dumps(
            normalized,
            protocol=SCHEMA_ARTIFACT_PICKLE_PROTOCOL,
            fix_imports=False,
        )
    except (OverflowError, pickle.PicklingError, RecursionError, ValueError) as error:
        raise SchemaArtifactCodecError("schema artifact cannot be encoded") from error
    if len(raw) > MAX_SCHEMA_ARTIFACT_BYTES:
        raise SchemaArtifactCodecError("schema artifact size is outside its hard bound")
    _preflight_pickle(raw)
    return raw


def _unpickle_and_validate_schema_artifact(raw: bytes) -> dict[str, Any]:
    stream = io.BytesIO(raw)
    try:
        decoded = _RestrictedUnpickler(stream).load()
    except SchemaArtifactCodecError:
        raise
    except (
        EOFError,
        IndexError,
        MemoryError,
        OverflowError,
        TypeError,
        ValueError,
        pickle.UnpicklingError,
    ) as error:
        raise SchemaArtifactCodecError(
            "schema artifact pickle cannot be decoded"
        ) from error
    if stream.tell() != len(raw):
        raise SchemaArtifactCodecError("schema artifact pickle has trailing data")
    try:
        return _DecodedValidator().validate(decoded)
    except SchemaArtifactCodecError:
        raise
    except RecursionError as error:  # defensive if interpreter limits are unusual
        raise SchemaArtifactCodecError(
            "schema artifact exceeds the maximum semantic depth"
        ) from error


def _authenticate_schema_artifact(
    raw: bytes, *, raw_size: int, raw_sha256: str
) -> None:
    expected_size = _validate_size(raw_size)
    expected_digest = _validate_sha256(raw_sha256)
    if type(raw) is not bytes:
        raise SchemaArtifactCodecError("schema artifact resource must contain bytes")
    if len(raw) != expected_size:
        raise SchemaArtifactCodecError("schema artifact size does not match")
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_digest):
        raise SchemaArtifactCodecError("schema artifact digest does not match")


def decode_schema_artifact(
    raw: bytes,
    *,
    pickle_protocol: int,
    raw_size: int,
    raw_sha256: str,
) -> dict[str, Any]:
    """Authenticate and decode one generated artifact resource."""

    _validate_protocol(pickle_protocol)
    _authenticate_schema_artifact(raw, raw_size=raw_size, raw_sha256=raw_sha256)
    _preflight_pickle(raw)
    return _unpickle_and_validate_schema_artifact(raw)


def _load_pinned_schema_artifact_resource(
    *,
    package: str,
    resource_name: str,
    pickle_protocol: int,
    raw_size: int,
    raw_sha256: str,
) -> dict[str, Any]:
    """Decode the generated loader's exact wheel-owned resource.

    This private fast path is valid only because the generated caller pins the
    metadata literals and every build/distribution gate fully preflights the
    corresponding digest. Generic callers must use :func:`decode_schema_artifact`.
    """

    _validate_protocol(pickle_protocol)
    expected_size = _validate_size(raw_size)
    if (
        type(package) is not str
        or package != _SCHEMA_ARTIFACT_PACKAGE
        or type(resource_name) is not str
        or resource_name != _SCHEMA_ARTIFACT_RESOURCE
    ):
        raise SchemaArtifactCodecError("schema artifact resource identity is invalid")
    try:
        resource = files(package).joinpath(resource_name)
        with resource.open("rb") as stream:
            raw = stream.read(expected_size + 1)
            has_more = bool(stream.read(1))
    except (ImportError, LookupError, OSError, TypeError) as error:
        raise SchemaArtifactCodecError(
            "schema artifact resource cannot be read"
        ) from error
    if has_more or len(raw) > expected_size:
        raise SchemaArtifactCodecError(
            "schema artifact resource exceeds its declared size"
        )
    _authenticate_schema_artifact(raw, raw_size=raw_size, raw_sha256=raw_sha256)
    # This exact resource/digest pair was produced by the generator's full
    # abstract opcode preflight and is rechecked by drift, schema-surface, and
    # distribution gates. Once its digest matches, reparsing the same opcode
    # proof on every process start adds no authority; an actor able to replace
    # both this loader and its digest can already replace arbitrary wheel code.
    return _unpickle_and_validate_schema_artifact(raw)
