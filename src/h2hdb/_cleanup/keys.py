"""Canonical cleanup keys, cursor codecs, and bounded shard coordinates."""

from __future__ import annotations

from collections.abc import Sequence

from h2hdb._cleanup.model import (
    CleanupCorruptionError,
    CleanupCycle,
    CleanupTargetKind,
    _StaticScalar,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan
from h2hdb.vnext_domains import require_bounded_bytes, require_int63


def _hash_cache_cutoff(cycle: CleanupCycle) -> int:
    if cycle.hash_cache_max_age_microseconds > cycle.cycle_cutoff_at:
        raise CleanupCorruptionError("hash-cache cleanup cutoff underflows")
    return cycle.cycle_cutoff_at - cycle.hash_cache_max_age_microseconds


def _encode_static_scalar(value: _StaticScalar) -> bytes:
    match value:
        case bool():
            raise CleanupCorruptionError("cleanup cursor contains a boolean key")
        case int():
            integer = require_int63(value, field="cleanup static cursor integer")
            return b"i" + integer.to_bytes(8, "big")
        case bytes():
            payload = require_bounded_bytes(
                value, field="cleanup static cursor bytes", maximum=1024
            )
            return b"b" + len(payload).to_bytes(2, "big") + payload
        case str():
            payload = value.encode("utf-8", errors="strict")
            if len(payload) > 1024:
                raise CleanupCorruptionError("cleanup static text cursor is too large")
            return b"s" + len(payload).to_bytes(2, "big") + payload
    raise CleanupCorruptionError("cleanup cursor contains an unsupported key type")


def _decode_static_scalars(
    payload: bytes,
    *,
    offset: int,
    count: int,
    field: str,
) -> tuple[_StaticScalar, ...]:
    values: list[_StaticScalar] = []
    for _ in range(count):
        if offset >= len(payload):
            raise CleanupCorruptionError(f"{field} is truncated")
        tag = payload[offset : offset + 1]
        offset += 1
        if tag == b"i":
            if offset + 8 > len(payload):
                raise CleanupCorruptionError(f"{field} integer is truncated")
            values.append(int.from_bytes(payload[offset : offset + 8], "big"))
            offset += 8
            continue
        if tag not in {b"b", b"s"} or offset + 2 > len(payload):
            raise CleanupCorruptionError(f"{field} tag is invalid")
        size = int.from_bytes(payload[offset : offset + 2], "big")
        offset += 2
        if offset + size > len(payload):
            raise CleanupCorruptionError(f"{field} bytes are truncated")
        value = payload[offset : offset + size]
        offset += size
        try:
            values.append(
                value if tag == b"b" else value.decode("utf-8", errors="strict")
            )
        except UnicodeDecodeError as error:
            raise CleanupCorruptionError(f"{field} text is not strict UTF-8") from error
    if offset != len(payload):
        raise CleanupCorruptionError(f"{field} has trailing bytes")
    return tuple(values)


def _encode_static_cursor(index: int, values: Sequence[_StaticScalar]) -> bytes:
    if not 0 <= index <= 65535 or len(values) > 255:
        raise CleanupCorruptionError("cleanup static cursor header is invalid")
    encoded = bytearray(b"\x01" + index.to_bytes(2, "big") + bytes((len(values),)))
    for value in values:
        encoded.extend(_encode_static_scalar(value))
    return require_bounded_bytes(
        bytes(encoded), field="cleanup static cursor", maximum=2048
    )


def _decode_static_cursor(
    cursor: bytes, specs: Sequence[_StaticDeleteSpec], root_arity: int
) -> tuple[int, tuple[_StaticScalar, ...] | None]:
    if not cursor:
        return 0, None
    payload = require_bounded_bytes(
        cursor, field="cleanup static cursor", minimum=4, maximum=2048
    )
    if payload[0] != 1:
        raise CleanupCorruptionError("cleanup static cursor version is unknown")
    index = int.from_bytes(payload[1:3], "big")
    if index >= len(specs):
        raise CleanupCorruptionError("cleanup static cursor relation is unknown")
    count = payload[3]
    if count != root_arity + len(specs[index].primary_key):
        raise CleanupCorruptionError("cleanup static cursor arity is invalid")
    return index, _decode_static_scalars(
        payload,
        offset=4,
        count=count,
        field="cleanup static cursor",
    )


def _keyset_predicate(columns: Sequence[str]) -> str:
    branches: list[str] = []
    for index, column in enumerate(columns):
        equal = " AND ".join(f"{prior} = %s" for prior in columns[:index])
        greater = f"{column} > %s"
        branches.append(f"({equal + ' AND ' if equal else ''}{greater})")
    return " OR ".join(branches)


def _keyset_parameters(values: Sequence[_StaticScalar]) -> tuple[_StaticScalar, ...]:
    parameters: list[_StaticScalar] = []
    for index, value in enumerate(values):
        parameters.extend(values[:index])
        parameters.append(value)
    return tuple(parameters)


def _static_policy_parameters(
    plan: _StaticTargetPlan, cycle: CleanupCycle
) -> tuple[object, ...]:
    if plan.kind is CleanupTargetKind.HASH_CACHE_OBSERVATION:
        return (_hash_cache_cutoff(cycle),)
    return (cycle.cycle_cutoff_at,) if plan.uses_cutoff else ()


def _static_shard_sql(plan: _StaticTargetPlan) -> str:
    if plan.contiguous_integer_prefix:
        return "1 = 1"
    if plan.shard_width is None:
        return f"MOD(r.{plan.shard_column}, 256) = %s"
    return f"r.{plan.shard_column} >= %s AND (%s = 1 OR r.{plan.shard_column} < %s)"


def _static_shard_parameters(
    plan: _StaticTargetPlan, cycle: CleanupCycle
) -> tuple[object, ...]:
    if plan.contiguous_integer_prefix:
        return ()
    if plan.shard_width is None:
        return (cycle.shard_no,)
    lower = bytes((cycle.shard_no,))
    if not plan.variable_width_shard:
        lower += bytes(plan.shard_width - 1)
    if cycle.shard_no == 255:
        return (lower, 1, b"")
    upper = bytes((cycle.shard_no + 1,))
    if not plan.variable_width_shard:
        upper += bytes(plan.shard_width - 1)
    return (lower, 0, upper)


def _static_values(row: Sequence[object]) -> tuple[_StaticScalar, ...]:
    values: list[_StaticScalar] = []
    for value in row:
        if isinstance(value, bool) or not isinstance(value, (bytes, int, str)):
            raise CleanupCorruptionError("cleanup selected an invalid key value")
        match value:
            case int():
                require_int63(value, field="cleanup selected integer key")
            case bytes():
                require_bounded_bytes(
                    value, field="cleanup selected byte key", maximum=1024
                )
            case _:
                if len(value.encode("utf-8", errors="strict")) > 1024:
                    raise CleanupCorruptionError(
                        "cleanup selected text key is too large"
                    )
        values.append(value)
    return tuple(values)
