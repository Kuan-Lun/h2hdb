"""Pinned, bounded tokenizer shared by catalog writers and discovery readers."""

from __future__ import annotations

import codecs
import unicodedata
from collections.abc import Iterable, Iterator

from .catalog_errors import CatalogSearchQueryTooComplexError

SEARCH_LEXEME_DOMAIN = b"search_lexeme_utf8_v1"
SEARCH_MAX_FIELD_NFD_BYTES = 65_536
SEARCH_MAX_LEXEME_BYTES = 64
SEARCH_MAX_QUERY_NFD_BYTES = 1_024
SEARCH_MAX_QUERY_LEXEMES = 16
SEARCH_POLICY_ID = 1
SEARCH_ALGORITHM_VERSION = 2
SEARCH_UNICODE_DATA_VERSION = "16.0.0"

_UTF8_DECODE_CHUNK_BYTES = 4_096
_CJK_IDEOGRAPH_RANGES = (
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xF900, 0xFA6D),  # CJK Compatibility Ideographs
    (0xFA70, 0xFAD9),
    (0x20000, 0x2A6DF),  # CJK Unified Ideographs Extension B
    (0x2A700, 0x2B739),  # Extension C
    (0x2B740, 0x2B81D),  # Extension D
    (0x2B820, 0x2CEA1),  # Extension E
    (0x2CEB0, 0x2EBE0),  # Extension F
    (0x2EBF0, 0x2EE5D),  # Extension I
    (0x2F800, 0x2FA1D),  # CJK Compatibility Ideographs Supplement
    (0x30000, 0x3134A),  # CJK Unified Ideographs Extension G
    (0x31350, 0x323AF),  # Extension H
)


def require_search_runtime_policy() -> None:
    """Reject a runtime whose Unicode tables differ from the indexed policy."""

    observed = unicodedata.unidata_version
    if observed != SEARCH_UNICODE_DATA_VERSION:
        raise RuntimeError(
            "catalog search requires Unicode data "
            f"{SEARCH_UNICODE_DATA_VERSION}, observed {observed}"
        )


def catalog_search_field_lexemes(value: str) -> tuple[bytes, ...]:
    """Return unique membership lexemes for one public metadata field.

    The field is omitted as a whole when its canonical NFD form exceeds the
    policy budget.  It is never truncated into a different searchable value.
    """

    if not isinstance(value, str):
        raise TypeError("catalog search field must be str")
    require_search_runtime_policy()
    normalized = _bounded_nfc_text(
        (value,),
        maximum_nfd_bytes=SEARCH_MAX_FIELD_NFD_BYTES,
    )
    if normalized is None:
        return ()
    return tuple(dict.fromkeys(_iter_normalized_lexemes(normalized)))


def iter_search_lexemes(parts: Iterable[bytes]) -> Iterator[bytes]:
    """Yield tokens for distinct fields without joining adjacent boundaries."""

    require_search_runtime_policy()
    for part in parts:
        if not isinstance(part, bytes):
            raise TypeError("search tokenizer fields must be bytes")
        yield from iter_search_field_lexemes((part,))


def iter_search_field_lexemes(parts: Iterable[bytes]) -> Iterator[bytes]:
    """Tokenize strict UTF-8 chunks belonging to one logical metadata field.

    Decoding always continues through the exact field boundary, including when
    the canonical NFD byte budget has already been exceeded.  Consequently an
    oversized field is omitted, but malformed trailing UTF-8 still fails.
    """

    require_search_runtime_policy()
    normalized = _bounded_nfc_text(
        _iter_strict_utf8(parts),
        maximum_nfd_bytes=SEARCH_MAX_FIELD_NFD_BYTES,
    )
    if normalized is None:
        return
    yield from _iter_normalized_lexemes(normalized)


def canonical_query_lexemes(value: str) -> tuple[bytes, ...]:
    """Return the unique AND operands for one bounded catalog query."""

    if not isinstance(value, str):
        raise TypeError("catalog search query must be str")
    require_search_runtime_policy()
    normalized = _bounded_nfc_text(
        (value,),
        maximum_nfd_bytes=SEARCH_MAX_QUERY_NFD_BYTES,
    )
    if normalized is None:
        raise ValueError("catalog search query exceeds 1024 canonical NFD UTF-8 bytes")
    unique = tuple(dict.fromkeys(_iter_normalized_lexemes(normalized)))
    if not unique:
        raise ValueError("catalog search query has no searchable lexemes")
    if len(unique) > SEARCH_MAX_QUERY_LEXEMES:
        raise CatalogSearchQueryTooComplexError(
            "catalog search query exceeds 16 unique lexemes"
        )
    return unique


def _iter_strict_utf8(parts: Iterable[bytes]) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    for part in parts:
        if not isinstance(part, bytes):
            raise TypeError("search tokenizer field chunks must be bytes")
        for offset in range(0, len(part), _UTF8_DECODE_CHUNK_BYTES):
            yield decoder.decode(
                part[offset : offset + _UTF8_DECODE_CHUNK_BYTES],
                final=False,
            )
    yield decoder.decode(b"", final=True)


def _bounded_nfc_text(
    parts: Iterable[str],
    *,
    maximum_nfd_bytes: int,
) -> str | None:
    """Collect one canonically bounded field, then normalize it exactly once.

    Canonically equivalent strings have identical NFD and therefore the same
    budget outcome.  Before the limit is crossed, the raw UTF-8 representation
    is itself bounded by four times the NFD byte count: every input scalar has
    at least one scalar in its canonical decomposition and UTF-8 scalars use at
    most four bytes.  Once crossed, buffered pieces are released while the
    remaining input is still consumed and checked for surrogate code points.
    """

    pieces: list[str] = []
    nfd_byte_count = 0
    omitted = False
    for part in parts:
        if not isinstance(part, str):
            raise TypeError("decoded search tokenizer chunks must be str")
        if omitted:
            _require_scalar_text(part)
            continue
        for character in part:
            if omitted:
                _require_scalar_character(character)
                continue
            decomposition = unicodedata.normalize("NFD", character)
            nfd_byte_count += len(decomposition.encode("utf-8", errors="strict"))
            if nfd_byte_count > maximum_nfd_bytes:
                omitted = True
                pieces.clear()
        if not omitted:
            pieces.append(part)
    if omitted:
        return None
    return unicodedata.normalize("NFC", "".join(pieces))


def _require_scalar_text(value: str) -> None:
    for character in value:
        _require_scalar_character(character)


def _require_scalar_character(character: str) -> None:
    if 0xD800 <= ord(character) <= 0xDFFF:
        character.encode("utf-8", errors="strict")


def _iter_normalized_lexemes(value: str) -> Iterator[bytes]:
    word = bytearray()
    word_overflow = False
    cjk_previous: str | None = None
    cjk_count = 0

    def flush_word() -> Iterator[bytes]:
        nonlocal word_overflow
        if word and not word_overflow:
            yield bytes(word)
        word.clear()
        word_overflow = False

    def flush_cjk() -> Iterator[bytes]:
        nonlocal cjk_count, cjk_previous
        if cjk_count == 1 and cjk_previous is not None:
            yield cjk_previous.encode("utf-8")
        cjk_previous = None
        cjk_count = 0

    for character in value.casefold():
        if _is_cjk(character):
            yield from flush_word()
            if cjk_previous is not None:
                yield (cjk_previous + character).encode("utf-8")
            cjk_previous = character
            cjk_count += 1
            continue
        yield from flush_cjk()
        if character.isalnum() and character != "_":
            encoded = character.encode("utf-8")
            if len(word) + len(encoded) <= SEARCH_MAX_LEXEME_BYTES:
                word.extend(encoded)
            else:
                word_overflow = True
            continue
        yield from flush_word()
    yield from flush_cjk()
    yield from flush_word()


def _is_cjk(character: str) -> bool:
    value = ord(character)
    return any(lower <= value <= upper for lower, upper in _CJK_IDEOGRAPH_RANGES)
