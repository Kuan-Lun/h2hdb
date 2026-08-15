from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from h2hdb.vnext_domains import (
    INT63_MAX,
    UINT32_MAX,
    DomainValidationError,
    microseconds_from_datetime,
    require_ascii_bytes,
    require_bool_byte,
    require_bounded_bytes,
    require_digest32,
    require_enum_bytes,
    require_int63,
    require_positive_int63,
    require_uint32,
    require_utf8_bytes,
    require_uuid16,
)


@pytest.mark.parametrize("value", (False, True, 1.0, "1", b"1", None))
def test_integer_domains_reject_non_exact_integers(value: object) -> None:
    with pytest.raises(DomainValidationError):
        require_int63(value, field="generation")


def test_integer_domains_enforce_portable_bounds() -> None:
    assert require_int63(0, field="generation") == 0
    assert require_int63(INT63_MAX, field="generation") == INT63_MAX
    assert require_positive_int63(1, field="revision") == 1
    assert require_positive_int63(INT63_MAX, field="revision") == INT63_MAX
    assert require_uint32(UINT32_MAX, field="version") == UINT32_MAX
    assert require_bool_byte(0, field="flag") == 0
    assert require_bool_byte(1, field="flag") == 1

    for malformed in (-1, INT63_MAX + 1):
        with pytest.raises(DomainValidationError):
            require_int63(malformed, field="generation")
    for malformed in (0, -1, INT63_MAX + 1):
        with pytest.raises(DomainValidationError):
            require_positive_int63(malformed, field="revision")
    for malformed in (-1, UINT32_MAX + 1):
        with pytest.raises(DomainValidationError):
            require_uint32(malformed, field="version")
    for malformed in (-1, 2, False, True):
        with pytest.raises(DomainValidationError):
            require_bool_byte(malformed, field="flag")


def test_binary_domains_do_not_coerce_text_or_mutable_buffers() -> None:
    digest = bytes(range(32))
    identity = bytes(range(16))
    assert require_digest32(digest, field="digest") is digest
    assert require_uuid16(identity, field="identity") is identity
    assert require_bounded_bytes(b"abc", field="payload", maximum=3) == b"abc"

    for malformed in ("a" * 32, bytearray(32), memoryview(bytes(32))):
        with pytest.raises(DomainValidationError):
            require_digest32(malformed, field="digest")
    for malformed_identity in (bytes(15), bytes(17)):
        with pytest.raises(DomainValidationError):
            require_uuid16(malformed_identity, field="identity")


def test_text_byte_domains_validate_encoding_length_and_nul_policy() -> None:
    assert (
        require_ascii_bytes(
            b"16.0.0", field="unicode_data_version", minimum=1, maximum=32
        )
        == b"16.0.0"
    )
    assert (
        require_utf8_bytes(
            "目錄".encode(), field="segment", minimum=1, maximum=32, reject_nul=True
        )
        == "目錄".encode()
    )

    with pytest.raises(DomainValidationError):
        require_ascii_bytes(b"\xff", field="ascii", maximum=1)
    with pytest.raises(DomainValidationError):
        require_utf8_bytes(b"\xff", field="utf8", maximum=1)
    with pytest.raises(DomainValidationError):
        require_utf8_bytes(b"a\x00b", field="utf8", maximum=3, reject_nul=True)


def test_binary_enum_is_closed_world() -> None:
    allowed = frozenset({b"OPEN", b"COMPLETE"})
    assert require_enum_bytes(b"OPEN", field="state", allowed=allowed) == b"OPEN"
    for malformed in (b"open", b"FAILED", "OPEN"):
        with pytest.raises(DomainValidationError):
            require_enum_bytes(malformed, field="state", allowed=allowed)


def test_aware_datetime_converts_exactly_to_utc_microseconds() -> None:
    value = datetime(2026, 8, 15, 12, 34, 56, 789012, tzinfo=UTC)
    offset_value = value.astimezone(timezone(timedelta(hours=8)))
    expected = 1_786_797_296_789_012
    assert microseconds_from_datetime(value, field="created_at") == expected
    assert microseconds_from_datetime(offset_value, field="created_at") == expected

    with pytest.raises(DomainValidationError):
        microseconds_from_datetime(value.replace(tzinfo=None), field="created_at")
    with pytest.raises(DomainValidationError):
        microseconds_from_datetime(
            datetime(1969, 12, 31, tzinfo=UTC), field="created_at"
        )
