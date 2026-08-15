"""Strict values at the vNext repository/SQL boundary.

SQLite's dynamic typing and Python's ``bool``-is-an-``int`` relationship make
it unsafe to rely on generated DDL checks alone.  Production repositories use
these functions both when constructing commands and immediately before every
SQL bind.  Database identities stay binary; hexadecimal text belongs only at
an explicit public adapter boundary.
"""

from __future__ import annotations

__all__ = [
    "INT63_MAX",
    "UINT32_MAX",
    "DomainValidationError",
    "microseconds_from_datetime",
    "require_ascii_bytes",
    "require_bool_byte",
    "require_bounded_bytes",
    "require_digest32",
    "require_enum_bytes",
    "require_int63",
    "require_positive_int63",
    "require_text",
    "require_uint32",
    "require_utf8_bytes",
    "require_uuid16",
]

from collections.abc import Collection
from datetime import UTC, datetime

INT63_MAX = (1 << 63) - 1
UINT32_MAX = (1 << 32) - 1
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class DomainValidationError(ValueError):
    """A value cannot be represented by its declared vNext SQL domain."""


def _require_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainValidationError(
            f"{field} must be an integer, not {type(value).__name__}"
        )
    return value


def require_int63(value: object, *, field: str) -> int:
    result = _require_int(value, field=field)
    if result < 0 or result > INT63_MAX:
        raise DomainValidationError(f"{field} must be in 0..{INT63_MAX}")
    return result


def require_positive_int63(value: object, *, field: str) -> int:
    result = _require_int(value, field=field)
    if result < 1 or result > INT63_MAX:
        raise DomainValidationError(f"{field} must be in 1..{INT63_MAX}")
    return result


def require_uint32(value: object, *, field: str) -> int:
    result = _require_int(value, field=field)
    if result < 0 or result > UINT32_MAX:
        raise DomainValidationError(f"{field} must be in 0..{UINT32_MAX}")
    return result


def require_bool_byte(value: object, *, field: str) -> int:
    result = _require_int(value, field=field)
    if result not in (0, 1):
        raise DomainValidationError(f"{field} must be the integer byte 0 or 1")
    return result


def require_bounded_bytes(
    value: object,
    *,
    field: str,
    maximum: int,
    minimum: int = 0,
) -> bytes:
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
        raise ValueError("maximum must be a nonnegative integer")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 0:
        raise ValueError("minimum must be a nonnegative integer")
    if minimum > maximum:
        raise ValueError("minimum cannot exceed maximum")
    if not isinstance(value, bytes):
        raise DomainValidationError(
            f"{field} must be bytes, not {type(value).__name__}"
        )
    if not minimum <= len(value) <= maximum:
        raise DomainValidationError(
            f"{field} must contain {minimum}..{maximum} bytes; got {len(value)}"
        )
    return value


def require_digest32(value: object, *, field: str) -> bytes:
    return require_bounded_bytes(value, field=field, minimum=32, maximum=32)


def require_uuid16(value: object, *, field: str) -> bytes:
    return require_bounded_bytes(value, field=field, minimum=16, maximum=16)


def require_ascii_bytes(
    value: object,
    *,
    field: str,
    maximum: int,
    minimum: int = 0,
) -> bytes:
    result = require_bounded_bytes(
        value,
        field=field,
        minimum=minimum,
        maximum=maximum,
    )
    try:
        result.decode("ascii", errors="strict")
    except UnicodeDecodeError as error:
        raise DomainValidationError(
            f"{field} must contain exact ASCII bytes"
        ) from error
    return result


def require_utf8_bytes(
    value: object,
    *,
    field: str,
    maximum: int,
    minimum: int = 0,
    reject_nul: bool = False,
) -> bytes:
    result = require_bounded_bytes(
        value,
        field=field,
        minimum=minimum,
        maximum=maximum,
    )
    try:
        result.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise DomainValidationError(
            f"{field} must contain exact UTF-8 bytes"
        ) from error
    if reject_nul and b"\x00" in result:
        raise DomainValidationError(f"{field} must not contain NUL")
    return result


def require_enum_bytes(
    value: object,
    *,
    field: str,
    allowed: Collection[bytes],
) -> bytes:
    if not allowed:
        raise ValueError("allowed must not be empty")
    if not isinstance(value, bytes):
        raise DomainValidationError(
            f"{field} must be bytes, not {type(value).__name__}"
        )
    if value not in allowed:
        raise DomainValidationError(f"{field} is not a registered binary enum value")
    return value


def require_text(value: object, *, field: str) -> str:
    """Require an exact SQL text value without applying an implicit coercion.

    Some vNext relations deliberately use an unbounded ``TEXT``/``LONGTEXT``
    domain (for example queue URLs).  Those values still need a production
    Python boundary: accepting an object with a string conversion would make
    SQLite and MariaDB bind different semantic values.
    """

    if not isinstance(value, str):
        raise DomainValidationError(
            f"{field} must be exact text, not {type(value).__name__}"
        )
    return value


def microseconds_from_datetime(value: object, *, field: str) -> int:
    if not isinstance(value, datetime):
        raise DomainValidationError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field} must be timezone-aware")
    utc_value = value.astimezone(UTC)
    delta = utc_value - _UNIX_EPOCH
    microseconds = (
        delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    )
    return require_int63(microseconds, field=field)
