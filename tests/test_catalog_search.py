from __future__ import annotations

import unicodedata
from typing import Literal

import pytest

from h2hdb import CatalogSearchQueryTooComplexError
from h2hdb.catalog_search import (
    SEARCH_MAX_FIELD_NFD_BYTES,
    SEARCH_MAX_QUERY_NFD_BYTES,
    canonical_query_lexemes,
    catalog_search_field_lexemes,
    iter_search_field_lexemes,
    iter_search_lexemes,
)


def test_logical_fields_never_form_cross_field_tokens() -> None:
    assert tuple(iter_search_lexemes((b"title", b"author"))) == (
        b"title",
        b"author",
    )
    assert tuple(iter_search_lexemes(("中".encode(), "文".encode()))) == (
        "中".encode(),
        "文".encode(),
    )
    assert tuple(iter_search_field_lexemes((b"ti", b"tle"))) == (b"title",)
    assert tuple(iter_search_field_lexemes(("中".encode(), "文".encode()))) == (
        "中文".encode(),
    )
    assert catalog_search_field_lexemes("foo foo") == (b"foo",)


def test_unicode_v2_tokens_pin_nfc_casefold_and_cjk_edges() -> None:
    assert tuple(iter_search_lexemes(("e\u0301".encode(),))) == ("é".encode(),)
    assert tuple(iter_search_lexemes(("é".encode(),))) == ("é".encode(),)
    assert tuple(iter_search_lexemes(("İ".encode(),))) == (b"i",)
    assert tuple(iter_search_lexemes(("中".encode(),))) == ("中".encode(),)
    assert tuple(iter_search_lexemes(("中文測".encode(),))) == (
        "中文".encode(),
        "文測".encode(),
    )


@pytest.mark.parametrize(
    ("composed", "decomposed"),
    (
        ("\u0b94", "\u0b92\u0bd7"),
        ("\U00016d68", "\U00016d67\U00016d67"),
    ),
)
def test_canonical_starter_pairs_are_chunk_independent(
    composed: str,
    decomposed: str,
) -> None:
    expected = (composed.encode(),)
    for value in (composed, decomposed):
        encoded = value.encode()
        for split in range(len(encoded) + 1):
            assert (
                tuple(iter_search_field_lexemes((encoded[:split], encoded[split:])))
                == expected
            )
        assert (
            tuple(iter_search_field_lexemes(tuple(bytes((byte,)) for byte in encoded)))
            == expected
        )


def test_nfd_field_budget_is_canonical_and_omits_the_whole_field() -> None:
    padding = "_" * (SEARCH_MAX_FIELD_NFD_BYTES - 4)
    composed = f"{padding}éb"
    decomposed = f"{padding}e\u0301b"

    assert catalog_search_field_lexemes(composed) == ("éb".encode(),)
    assert catalog_search_field_lexemes(decomposed) == ("éb".encode(),)
    assert catalog_search_field_lexemes(composed + "_") == ()
    assert catalog_search_field_lexemes(decomposed + "_") == ()


def test_nfd_query_budget_has_canonical_boundary_outcomes() -> None:
    padding = "_" * (SEARCH_MAX_QUERY_NFD_BYTES - 4)
    composed = f"{padding}éb"
    decomposed = f"{padding}e\u0301b"

    assert canonical_query_lexemes(composed) == ("éb".encode(),)
    assert canonical_query_lexemes(decomposed) == ("éb".encode(),)
    for value in (composed + "_", decomposed + "_"):
        with pytest.raises(ValueError, match="1024 canonical NFD"):
            canonical_query_lexemes(value)


def test_oversized_combining_run_is_not_normalized_monolithically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_normalize = unicodedata.normalize
    largest_input = 0

    def tracked_normalize(
        form: Literal["NFC", "NFD", "NFKC", "NFKD"],
        value: str,
    ) -> str:
        nonlocal largest_input
        largest_input = max(largest_input, len(value))
        return real_normalize(form, value)

    monkeypatch.setattr(unicodedata, "normalize", tracked_normalize)
    combining_count = SEARCH_MAX_FIELD_NFD_BYTES // 2 + 1
    oversized = ("a" + "\u0301" * combining_count).encode()

    assert tuple(iter_search_field_lexemes((oversized,))) == ()
    assert largest_input == 1
    with pytest.raises(UnicodeDecodeError):
        tuple(iter_search_field_lexemes((oversized + b"\xff",)))
    with pytest.raises(UnicodeEncodeError):
        catalog_search_field_lexemes("a" * (SEARCH_MAX_FIELD_NFD_BYTES + 1) + "\ud800")


def test_cjk_policy_excludes_unicode_16_unassigned_gaps() -> None:
    assigned_neighbors = (
        0xFA6D,
        0xFA70,
        0x2A6DF,
        0x2A700,
        0x2B739,
        0x2B740,
        0x2EE5D,
        0x2F800,
        0x3134A,
        0x31350,
    )
    unassigned_gaps = (
        0xFA6E,
        0xFA6F,
        0x2A6E0,
        0x2B73A,
        0x2B81E,
        0x2CEA2,
        0x2EBE1,
        0x2EE5E,
        0x2FA1E,
        0x3134B,
    )

    for value in assigned_neighbors:
        assert catalog_search_field_lexemes(chr(value))
    for value in unassigned_gaps:
        assert unicodedata.category(chr(value)) == "Cn"
        assert catalog_search_field_lexemes(chr(value)) == ()


def test_unicode_runtime_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(unicodedata, "unidata_version", "17.0.0")

    with pytest.raises(RuntimeError, match="requires Unicode data 16.0.0"):
        canonical_query_lexemes("query")
    with pytest.raises(RuntimeError, match="requires Unicode data 16.0.0"):
        catalog_search_field_lexemes("field")
    with pytest.raises(RuntimeError, match="requires Unicode data 16.0.0"):
        tuple(iter_search_field_lexemes((b"field",)))


def test_query_lexemes_are_unique_bounded_and_never_truncated() -> None:
    assert canonical_query_lexemes(" foo  foo BAR ") == (b"foo", b"bar")
    with pytest.raises(ValueError, match="1024 canonical NFD"):
        canonical_query_lexemes("x" * (SEARCH_MAX_QUERY_NFD_BYTES + 1))
    with pytest.raises(ValueError, match="no searchable"):
        canonical_query_lexemes("___")
    query = " ".join(f"token{index}" for index in range(17))
    with pytest.raises(CatalogSearchQueryTooComplexError, match="16 unique"):
        canonical_query_lexemes(query)
