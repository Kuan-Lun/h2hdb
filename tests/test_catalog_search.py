from __future__ import annotations

import unicodedata
from datetime import UTC, datetime, timedelta, timezone
from typing import Literal, cast

import pytest

from h2hdb import (
    CatalogDiscoveryQuery,
    CatalogPageCountRange,
    CatalogSearchQueryTooComplexError,
    CatalogSubjectFilter,
    CatalogTimestampRange,
)
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


def test_discovery_ranges_preserve_exact_inclusive_and_exclusive_bounds() -> None:
    offset = timezone(timedelta(hours=8))
    start = datetime(2026, 9, 5, 8, 0, 0, 1, tzinfo=offset)
    end = start + timedelta(microseconds=1)
    timestamps = CatalogTimestampRange(start=start, end=end)

    assert timestamps.start == datetime(2026, 9, 5, 0, 0, 0, 1, tzinfo=UTC)
    assert timestamps.end == datetime(2026, 9, 5, 0, 0, 0, 2, tzinfo=UTC)
    assert timestamps.start.tzinfo is UTC
    assert timestamps.end.tzinfo is UTC
    assert CatalogTimestampRange(start=start).end is None
    assert CatalogTimestampRange(end=end).start is None
    assert CatalogPageCountRange(minimum=0, maximum=0).minimum == 0
    assert CatalogPageCountRange(minimum=4096, maximum=4096).maximum == 4096
    assert CatalogPageCountRange(minimum=1).maximum is None
    assert CatalogPageCountRange(maximum=4096).minimum is None


@pytest.mark.parametrize(
    ("start", "end"),
    (
        (None, None),
        (datetime(2026, 9, 5), None),
        (None, datetime(2026, 9, 5)),
        (datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC), None),
        (None, datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC)),
        (datetime(2026, 9, 5, tzinfo=UTC), datetime(2026, 9, 5, tzinfo=UTC)),
        (datetime(2026, 9, 6, tzinfo=UTC), datetime(2026, 9, 5, tzinfo=UTC)),
        (1, None),
        (None, "2026-09-05"),
    ),
)
def test_discovery_timestamp_ranges_reject_invalid_authority_bounds(
    start: object,
    end: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        CatalogTimestampRange(
            start=cast("datetime | None", start),
            end=cast("datetime | None", end),
        )


@pytest.mark.parametrize(
    ("minimum", "maximum"),
    (
        (None, None),
        (-1, None),
        (None, -1),
        (4097, None),
        (None, 4097),
        (2, 1),
        (True, None),
        (None, False),
        (1.0, None),
        (None, "1"),
    ),
)
def test_discovery_page_ranges_reject_invalid_authority_bounds(
    minimum: object,
    maximum: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        CatalogPageCountRange(
            minimum=cast("int | None", minimum),
            maximum=cast("int | None", maximum),
        )


@pytest.mark.parametrize("gid", (0, -1, 1 << 63, True, 1.0, "1834943"))
def test_discovery_gid_requires_a_canonical_positive_int63(gid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        CatalogDiscoveryQuery(gid=cast("int", gid))


def test_discovery_query_keeps_exact_subjects_and_both_lexeme_scopes() -> None:
    genre = CatalogSubjectFilter(namespace="genre", value=" Café ")
    topic = CatalogSubjectFilter(namespace="topic", value="café")
    query = CatalogDiscoveryQuery(
        search="  Alpha ALPHA  ",
        title="Beta ALPHA",
        gid=(1 << 63) - 1,
        subjects=(topic, genre, topic),
    )

    assert query.search_lexemes == (b"alpha",)
    assert query.title_lexemes == (b"beta", b"alpha")
    assert query.subjects == (genre, topic)
    assert query.subjects[0].value == " Café "
    assert query.gid == (1 << 63) - 1


def test_discovery_query_bounds_lexemes_across_search_and_title_scopes() -> None:
    eight = " ".join(f"token{index}" for index in range(8))
    valid = CatalogDiscoveryQuery(search=eight, title=eight)
    assert len(valid.search_lexemes) + len(valid.title_lexemes) == 16

    with pytest.raises(CatalogSearchQueryTooComplexError):
        CatalogDiscoveryQuery(search=eight, title=eight + " overflow")


@pytest.mark.parametrize("title", ("", " \t\n", "---", 1))
def test_discovery_title_rejects_unsearchable_or_nontext_values(title: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        CatalogDiscoveryQuery(title=cast("str", title))


def test_discovery_subject_input_is_bounded_before_canonical_deduplication() -> None:
    subjects = tuple(
        CatalogSubjectFilter(namespace="genre", value=f"value-{index}")
        for index in range(16)
    )
    assert len(CatalogDiscoveryQuery(subjects=subjects).subjects) == 16
    assert CatalogDiscoveryQuery(subjects=(subjects[0],) * 16).subjects == (
        subjects[0],
    )

    with pytest.raises((TypeError, ValueError)):
        CatalogDiscoveryQuery(subjects=(*subjects, subjects[0]))
    with pytest.raises((TypeError, ValueError)):
        CatalogDiscoveryQuery(subjects=(subjects[0],) * 17)
    with pytest.raises(TypeError):
        CatalogDiscoveryQuery(subjects=cast("tuple[CatalogSubjectFilter, ...]", []))
    with pytest.raises(TypeError):
        CatalogDiscoveryQuery(
            subjects=cast("tuple[CatalogSubjectFilter, ...]", ("genre:science",))
        )
