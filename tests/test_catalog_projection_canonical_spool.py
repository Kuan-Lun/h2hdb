from __future__ import annotations

import sqlite3
from collections.abc import Buffer, Iterator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryFile
from typing import BinaryIO

import pytest
from test_catalog_plan_preparation_performance import _source

from h2hdb import vnext_canonical_value_repository as canonical
from h2hdb import vnext_identity as identity
from h2hdb import vnext_publication_candidate_repository as projection


@contextmanager
def _scratch() -> Iterator[tuple[sqlite3.Connection, BytesIO]]:
    database = sqlite3.connect(":memory:")
    payload = BytesIO()
    try:
        projection._initialize_projection_plan_database(database)
        yield database, payload
    finally:
        database.close()
        payload.close()


@pytest.mark.parametrize("byte_count", [0, 64, 65_536])
def test_bounded_scalar_and_one_pass_stream_share_exact_canonical_identity(
    byte_count: int,
) -> None:
    value = bytes(index % 251 for index in range(byte_count))
    domain = "catalog_summary_utf8_v1"
    consumed = 0

    def parts() -> Iterator[bytes]:
        nonlocal consumed
        for offset in range(0, byte_count, 997):
            consumed += 1
            yield value[offset : offset + 997]

    with _scratch() as (database, payload):
        streamed = projection._register_projection_canonical_value(
            database, payload, domain=domain, parts=parts()
        )
        bounded = projection._register_projection_canonical_bytes(
            database, payload, domain=domain, value=value
        )
        assert consumed == (byte_count + 996) // 997
        assert streamed == bounded == identity.canonical_value_digest(domain, value)
        assert database.execute("SELECT COUNT(*) FROM canonical_values").fetchone() == (
            1,
        )
        assert payload.getvalue() == value


def test_search_membership_deduplicates_each_field_and_preserves_title_subset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered: list[bytes] = []
    original = projection._register_projection_canonical_bytes

    def register(
        database: sqlite3.Connection,
        payload: BinaryIO,
        *,
        domain: str,
        value: bytes,
    ) -> bytes:
        registered.append(value)
        return original(database, payload, domain=domain, value=value)

    monkeypatch.setattr(projection, "_register_projection_canonical_bytes", register)
    with _scratch() as (database, payload):
        publication_key = b"p" * 32
        for field, title in ((b"shared shared tag", False), (b"shared shared", True)):
            projection._register_projection_search_field(
                database,
                payload,
                publication_key=publication_key,
                parts=(field,),
                title=title,
            )
        assert registered == [b"shared", b"tag", b"shared"]
        shared_digest = identity.canonical_value_digest(
            "search_lexeme_utf8_v1", b"shared"
        )
        tag_digest = identity.canonical_value_digest("search_lexeme_utf8_v1", b"tag")
        assert set(database.execute("SELECT * FROM search_postings")) == {
            (publication_key, shared_digest),
            (publication_key, tag_digest),
        }
        assert database.execute("SELECT * FROM title_search_postings").fetchall() == [
            (publication_key, shared_digest)
        ]
        assert payload.getvalue() == b"sharedtag"


@pytest.mark.parametrize("corruption", ["domain", "length", "bytes", "truncated"])
def test_bounded_scalar_reuse_rejects_digest_collision_or_truncated_spool(
    monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    with _scratch() as (database, payload):
        domain = "search_lexeme_utf8_v1"
        digest = projection._register_projection_canonical_bytes(
            database, payload, domain=domain, value=b"first"
        )
        monkeypatch.setattr(identity, "canonical_value_digest", lambda *_: digest)
        value = b"first"
        match corruption:
            case "domain":
                domain = "contributor_name_utf8_v1"
            case "length":
                value = b"longer"
            case "bytes":
                value = b"other"
            case _:
                payload.truncate(1)
        with pytest.raises(
            projection.PublicationCandidateConflictError,
            match="collides|truncated",
        ):
            projection._register_projection_canonical_bytes(
                database, payload, domain=domain, value=value
            )


def test_bounded_scalar_rejects_oversized_input_before_registration() -> None:
    with _scratch() as (database, payload):
        with pytest.raises(ValueError, match="65536"):
            projection._register_projection_canonical_bytes(
                database,
                payload,
                domain="contributor_name_utf8_v1",
                value=b"x" * 65_537,
            )
        assert payload.getvalue() == b""
        assert not database.execute("SELECT 1 FROM canonical_values").fetchall()


def test_unknown_length_metadata_stream_remains_larger_than_scalar_budget() -> None:
    value = b"metadata" * 32_768
    with _scratch() as (database, payload):
        digest = projection._register_projection_canonical_value(
            database,
            payload,
            domain="catalog_summary_utf8_v1",
            parts=(
                value[offset : offset + 8191] for offset in range(0, len(value), 8191)
            ),
        )
        assert digest == identity.canonical_value_digest(
            "catalog_summary_utf8_v1", value
        )
        assert payload.getvalue() == value


@pytest.mark.parametrize("invalid_tail", [False, True])
def test_search_dedup_keeps_full_field_budget_and_trailing_utf8_validation(
    invalid_tail: bool,
) -> None:
    with _scratch() as (database, payload):
        parts = (b"word " * 20_000, b"\xff" if invalid_tail else b"tail")
        if invalid_tail:
            with pytest.raises(UnicodeDecodeError):
                projection._register_projection_search_field(
                    database, payload, publication_key=b"p" * 32, parts=parts
                )
        else:
            projection._register_projection_search_field(
                database, payload, publication_key=b"p" * 32, parts=parts
            )
        assert not database.execute("SELECT 1 FROM search_postings").fetchall()
        assert not database.execute("SELECT 1 FROM canonical_values").fetchall()
        assert payload.getvalue() == b""


def test_projection_uses_upload_spools_only_for_unbounded_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _source(tmp_path, tags=(b"english", b"Shared Shared Artist")) as (
        connector,
        authority,
        _digests,
    ):
        uploaded_domains: list[str] = []
        original = canonical.CanonicalValueUploadPlan.from_parts

        def from_parts(
            cls: type[canonical.CanonicalValueUploadPlan],
            domain: str,
            parts: Iterator[bytes],
        ) -> canonical.CanonicalValueUploadPlan:
            uploaded_domains.append(domain)
            return original(domain, parts)

        monkeypatch.setattr(
            canonical.CanonicalValueUploadPlan, "from_parts", classmethod(from_parts)
        )
        for prepare in (
            projection.PublicationCandidateRepository.prepare_catalog_projection,
            projection.PublicationCandidateRepository.prepare_catalog_projection_validation,
        ):
            uploaded_domains.clear()
            with prepare(connector, backend="sqlite", authority=authority) as plan:
                assert plan.publication_count == 3
                assert len(uploaded_domains) == 4 * plan.publication_count
                assert set(uploaded_domains) == {
                    "source_title_utf8_v1",
                    "display_title_utf8_v1",
                    "title_sort_utf8_v1",
                    "catalog_summary_utf8_v1",
                }


def test_failed_stream_registration_closes_private_upload_spool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spools: list[BinaryIO] = []

    def temporary(*, mode: str) -> BinaryIO:
        assert mode == "w+b"
        spool = TemporaryFile(mode="w+b")
        spools.append(spool)
        return spool

    def parts() -> Iterator[bytes]:
        yield b"x" * 65_536
        raise OSError("source stream failed")

    monkeypatch.setattr(canonical, "TemporaryFile", temporary)
    with _scratch() as (database, payload):
        with pytest.raises(OSError, match="source stream failed"):
            projection._register_projection_canonical_value(
                database, payload, domain="catalog_summary_utf8_v1", parts=parts()
            )
        assert len(spools) == 1 and spools[0].closed
        assert payload.getvalue() == b""
        assert not database.execute("SELECT 1 FROM canonical_values").fetchall()


def test_partial_shared_payload_write_does_not_register_canonical_value() -> None:
    class PartialWriter(BytesIO):
        def write(self, data: Buffer, /) -> int:
            return super().write(bytes(data)[:1])

    with _scratch() as (database, _payload), PartialWriter() as partial:
        with pytest.raises(OSError, match="partial write"):
            projection._register_projection_canonical_bytes(
                database,
                partial,
                domain="contributor_name_utf8_v1",
                value=b"artist",
            )
        assert not database.execute("SELECT 1 FROM canonical_values").fetchall()
