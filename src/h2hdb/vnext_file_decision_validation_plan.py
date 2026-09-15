"""Private disposable source snapshots for file-decision validation.

Source facts are supplied only by the analysis repository's bounded immutable
reads. SQLite sorts the temporary inputs once; the retained plan is an anonymous
fixed-width file whose records and prepared pages carry process-local MACs.
Nothing here is a persistent database authority or accepts a caller file path.
"""

from __future__ import annotations

import hmac
import secrets
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from contextlib import closing
from dataclasses import dataclass
from itertools import groupby
from tempfile import TemporaryFile
from typing import TYPE_CHECKING, BinaryIO, cast

from .vnext_domains import require_digest32, require_int63, require_positive_int63

if TYPE_CHECKING:
    from .vnext_analysis_repository import (
        AnalysisPreparationAuthority,
        AnalysisStageIssue,
    )

_VALUES_BYTES = 24
_PAYLOAD_BYTES = 32 + _VALUES_BYTES
_RECORD_BYTES = _PAYLOAD_BYTES + 32
_PLAN_TOKEN = object()
_PAGE_TOKEN = object()
FileDecisionValues = tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class FileDecisionSourceGallery:
    gallery_id: int
    observation_id: int
    artists: Iterable[int]
    occurrences: Iterable[tuple[bytes, int]]


@dataclass(frozen=True, slots=True)
class AnalysisFileDecisionValidationPage:
    """One authenticated, bounded expected page prepared outside a DB write."""

    authority: AnalysisPreparationAuthority
    batch_key: bytes
    checkpoint_generation: int
    checkpoint_cursor: bytes
    checkpoint_processed_count: int
    page_limit: int
    entries: tuple[tuple[bytes, FileDecisionValues | None], ...]
    source_count: int
    _owner: AnalysisFileDecisionValidationPlan
    _proof: bytes
    _capability: object

    def verify(self) -> None:
        if self._capability is not _PAGE_TOKEN:
            raise TypeError("file-decision validation pages are repository-issued")
        self._owner._require_open()
        if self.authority != self._owner.authority:
            raise ValueError("file-decision page authority changed")
        if self.source_count != self._owner.row_count:
            raise ValueError("file-decision page source count changed")
        if not 1 <= self.page_limit <= 128 or len(self.entries) > self.page_limit:
            raise ValueError("file-decision validation page exceeds its bound")
        previous: bytes | None = None
        for key, values in self.entries:
            require_digest32(key, field="file-decision validation page key")
            if previous is not None and key <= previous:
                raise ValueError("file-decision validation page keys are not canonical")
            if values is not None:
                _require_values(values)
            previous = key
        if not hmac.compare_digest(self._proof, self._owner._page_proof(self)):
            raise ValueError("file-decision validation page was modified")


class AnalysisFileDecisionValidationPlan:
    """Closeable expected values tied to one repository-issued source authority."""

    def __init__(
        self,
        *,
        authority: AnalysisPreparationAuthority,
        payload: BinaryIO,
        signing_key: bytes,
        row_count: int,
        capability: object,
    ) -> None:
        if capability is not _PLAN_TOKEN:
            raise TypeError("file-decision validation plans are repository-issued")
        self.authority = authority
        self.row_count = require_int63(row_count, field="file-decision plan row count")
        self._payload = payload
        self._signing_key = signing_key
        self._closed = False
        self._capability = capability
        self._metadata_proof = self._metadata_mac()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._payload.close()
            self._signing_key = b""

    def _require_open(self) -> None:
        if self._capability is not _PLAN_TOKEN:
            raise TypeError("file-decision plan is not repository-issued")
        if self._closed:
            raise ValueError("file-decision validation plan is closed")
        self.authority.__post_init__()
        if not hmac.compare_digest(self._metadata_proof, self._metadata_mac()):
            raise ValueError("file-decision plan metadata was modified")

    def _metadata_mac(self) -> bytes:
        authority = self.authority
        data = bytearray(b"h2hdb-file-decision-plan-metadata-v1\0")
        data.extend(
            authority.analysis_id + authority.build_id + authority.input_manifest_sha256
        )
        for value in (authority.generation, authority.policy_id, self.row_count):
            data.extend(
                require_int63(value, field="file-decision plan metadata").to_bytes(
                    8, "big"
                )
            )
        for component, count, sealed_at in authority.component_seals:
            data.extend(len(component).to_bytes(8, "big") + component)
            data.extend(count.to_bytes(8, "big") + sealed_at.to_bytes(8, "big"))
        return hmac.digest(self._signing_key, data, "sha256")

    def _record(self, position: int) -> tuple[bytes, FileDecisionValues]:
        self._require_open()
        if not 0 <= position < self.row_count:
            raise ValueError("file-decision plan position is out of range")
        self._payload.seek(position * _RECORD_BYTES)
        record = self._payload.read(_RECORD_BYTES)
        if len(record) != _RECORD_BYTES:
            raise ValueError("file-decision plan is truncated")
        payload, proof = record[:_PAYLOAD_BYTES], record[_PAYLOAD_BYTES:]
        expected = hmac.digest(
            self._signing_key, position.to_bytes(8, "big") + payload, "sha256"
        )
        if not hmac.compare_digest(proof, expected):
            raise ValueError("file-decision plan record was modified")
        values = cast(
            FileDecisionValues,
            tuple(
                int.from_bytes(payload[offset : offset + 8], "big")
                for offset in (32, 40, 48)
            ),
        )
        _require_values(values)
        return require_digest32(payload[:32], field="file-decision plan key"), values

    def source_page(
        self, *, after: bytes | None, limit: int
    ) -> tuple[tuple[bytes, FileDecisionValues], ...]:
        self._require_open()
        if not 1 <= limit <= 129:
            raise ValueError("file-decision source page exceeds 129")
        if after is not None:
            require_digest32(after, field="file-decision plan cursor")
        self._payload.seek(0, 2)
        if self._payload.tell() != self.row_count * _RECORD_BYTES:
            raise ValueError("file-decision plan length changed")
        lower, upper = 0, self.row_count
        while lower < upper:
            middle = (lower + upper) // 2
            if after is not None and self._record(middle)[0] <= after:
                lower = middle + 1
            else:
                upper = middle
        rows = tuple(
            self._record(position)
            for position in range(lower, min(self.row_count, lower + limit))
        )
        if any(left[0] >= right[0] for left, right in zip(rows, rows[1:])):
            raise ValueError("file-decision plan records are not ordered")
        return rows

    def _page_proof(self, page: AnalysisFileDecisionValidationPage) -> bytes:
        data = bytearray(b"h2hdb-file-decision-validation-page-v1\0")
        authority = self.authority
        data.extend(
            authority.analysis_id + authority.build_id + authority.input_manifest_sha256
        )
        for value in (
            authority.generation,
            authority.policy_id,
            page.checkpoint_generation,
            page.checkpoint_processed_count,
            page.page_limit,
            page.source_count,
        ):
            data.extend(
                require_int63(value, field="file-decision page coordinate").to_bytes(
                    8, "big"
                )
            )
        for raw in (page.batch_key, page.checkpoint_cursor):
            data.extend(len(raw).to_bytes(8, "big"))
            data.extend(raw)
        for key, values in page.entries:
            data.extend(key)
            data.append(int(values is not None))
            if values is not None:
                data.extend(_values_bytes(values))
        return hmac.digest(self._signing_key, data, "sha256")

    def _prepare_page(
        self,
        issue: AnalysisStageIssue,
        entries: tuple[tuple[bytes, FileDecisionValues | None], ...],
    ) -> AnalysisFileDecisionValidationPage:
        self._require_open()
        if (
            issue.batch_key is None
            or issue.checkpoint_generation is None
            or issue.checkpoint_cursor is None
            or issue.checkpoint_processed_count is None
        ):
            raise ValueError("file-decision validation issue lacks a checkpoint")
        page = AnalysisFileDecisionValidationPage(
            self.authority,
            issue.batch_key,
            issue.checkpoint_generation,
            issue.checkpoint_cursor,
            issue.checkpoint_processed_count,
            issue.page_limit,
            entries,
            self.row_count,
            self,
            b"",
            _PAGE_TOKEN,
        )
        object.__setattr__(page, "_proof", self._page_proof(page))
        page.verify()
        return page


def _require_values(values: FileDecisionValues) -> None:
    if len(values) != 3:
        raise ValueError("file-decision values have the wrong shape")
    require_positive_int63(values[0], field="decision occurrence_count")
    require_int63(values[1], field="decision artist_count")
    require_int63(values[2], field="decision maximum_gallery_artist_count")


def _values_bytes(values: FileDecisionValues) -> bytes:
    _require_values(values)
    return b"".join(value.to_bytes(8, "big") for value in values)


def _source_totals(database: sqlite3.Connection) -> Iterator[tuple[bytes, int, int]]:
    rows = database.execute(
        "SELECT occurrence.file_sha256, occurrence.occurrence_count, gallery.artist_count "
        "FROM occurrences AS occurrence JOIN galleries AS gallery "
        "ON gallery.gallery_id = occurrence.gallery_id "
        "ORDER BY occurrence.file_sha256, occurrence.gallery_id"
    )
    for key, group in groupby(rows, key=lambda row: row[0]):
        count = 0
        maximum = 0
        for _digest, occurrences, artists in group:
            count = require_positive_int63(
                count + occurrences, field="decision occurrence_count"
            )
            maximum = max(maximum, artists)
        yield key, count, maximum


def build_file_decision_validation_plan(
    authority: AnalysisPreparationAuthority,
    galleries: Iterable[FileDecisionSourceGallery],
    *,
    progress: Callable[[int], None] | None = None,
) -> AnalysisFileDecisionValidationPlan:
    """Construct independent expected values; discard all sorting inputs."""
    authority.__post_init__()
    payload = cast(BinaryIO, TemporaryFile(mode="w+b"))
    signing_key = secrets.token_bytes(32)
    row_count = 0
    galleries_read = 0
    try:
        with closing(sqlite3.connect("", isolation_level=None)) as database:
            database.execute("PRAGMA temp_store = FILE")
            database.execute("PRAGMA journal_mode = OFF")
            database.execute("PRAGMA synchronous = OFF")
            database.execute("PRAGMA cache_size = -2048")
            database.executescript("""
                CREATE TABLE galleries (
                    gallery_id INTEGER PRIMARY KEY, artist_count INTEGER NOT NULL
                );
                CREATE TABLE artists (
                    gallery_id INTEGER NOT NULL, artist_id INTEGER NOT NULL,
                    PRIMARY KEY (gallery_id, artist_id)
                ) WITHOUT ROWID;
                CREATE TABLE occurrences (
                    file_sha256 BLOB NOT NULL, gallery_id INTEGER NOT NULL,
                    occurrence_count INTEGER NOT NULL,
                    PRIMARY KEY (file_sha256, gallery_id)
                ) WITHOUT ROWID;
            """)
            for gallery in galleries:
                galleries_read += 1
                gallery_id = require_positive_int63(
                    gallery.gallery_id, field="source gallery_id"
                )
                require_positive_int63(
                    gallery.observation_id, field="source observation_id"
                )
                artist_count = 0
                for artist in gallery.artists:
                    database.execute(
                        "INSERT INTO artists VALUES (?, ?)",
                        (
                            gallery_id,
                            require_positive_int63(
                                artist, field="source artist tag_id"
                            ),
                        ),
                    )
                    artist_count = require_int63(
                        artist_count + 1, field="source artist count"
                    )
                database.execute(
                    "INSERT INTO galleries VALUES (?, ?)", (gallery_id, artist_count)
                )
                for key, count in gallery.occurrences:
                    database.execute(
                        "INSERT INTO occurrences VALUES (?, ?, ?)",
                        (
                            require_digest32(key, field="source file_sha256"),
                            gallery_id,
                            require_positive_int63(
                                count, field="source occurrence_count"
                            ),
                        ),
                    )
            artists = database.execute(
                "SELECT occurrence.file_sha256, COUNT(DISTINCT artist.artist_id) "
                "FROM occurrences AS occurrence LEFT JOIN artists AS artist "
                "ON artist.gallery_id = occurrence.gallery_id "
                "GROUP BY occurrence.file_sha256 ORDER BY occurrence.file_sha256"
            )
            for total, artist in zip(_source_totals(database), artists, strict=True):
                key, count, maximum = total
                if artist[0] != key:
                    raise ValueError("file-decision sort aggregates disagree on keys")
                values = (
                    count,
                    require_int63(artist[1], field="decision artist_count"),
                    maximum,
                )
                record = key + _values_bytes(values)
                payload.write(record)
                payload.write(
                    hmac.digest(
                        signing_key, row_count.to_bytes(8, "big") + record, "sha256"
                    )
                )
                row_count = require_int63(
                    row_count + 1, field="file-decision plan row count"
                )
                if progress is not None and row_count % 128 == 0:
                    progress(galleries_read)
        payload.flush()
        return AnalysisFileDecisionValidationPlan(
            authority=authority,
            payload=payload,
            signing_key=signing_key,
            row_count=row_count,
            capability=_PLAN_TOKEN,
        )
    except BaseException:
        payload.close()
        raise
