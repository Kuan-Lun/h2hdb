"""Disposable authenticated key pages for immutable analysis source keys.

Only the repository supplies source hashes. A disk B-tree deduplicates and sorts
once, then an anonymous fixed-width stream serves bounded pages without retaining
source rows in memory. The plan has no persistent authority or caller path.
"""

from __future__ import annotations

import hmac
import secrets
import sqlite3
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass
from tempfile import TemporaryFile
from typing import TYPE_CHECKING, BinaryIO, cast

from .vnext_domains import require_digest32, require_int63, require_positive_int63

if TYPE_CHECKING:
    from .vnext_analysis_repository import (
        AnalysisPreparationAuthority,
        AnalysisStageIssue,
    )

_RECORD_BYTES = 64
_PLAN_TOKEN = object()
_PAGE_TOKEN = object()


@dataclass(frozen=True, slots=True)
class AnalysisHashKeyPage:
    authority: AnalysisPreparationAuthority
    stage: bytes
    input_binding: bytes
    batch_key: bytes
    checkpoint_generation: int
    checkpoint_cursor: bytes
    checkpoint_processed_count: int
    page_limit: int
    keys: tuple[bytes, ...]
    source_count: int
    _owner: AnalysisHashKeyPlan
    _proof: bytes
    _capability: object

    def verify(self) -> None:
        if self._capability is not _PAGE_TOKEN:
            raise TypeError("analysis hash-key pages are repository-issued")
        self._owner._require_open()
        if (
            self.authority != self._owner.authority
            or self.stage != self._owner.stage
            or self.input_binding != self._owner.input_binding
            or self.source_count != self._owner.row_count
        ):
            raise ValueError("analysis hash-key page authority changed")
        if not 1 <= self.page_limit <= 128 or len(self.keys) > self.page_limit:
            raise ValueError("analysis hash-key page exceeds its bound")
        previous: bytes | None = None
        for key in self.keys:
            require_digest32(key, field="analysis hash-key page key")
            if previous is not None and key <= previous:
                raise ValueError("analysis hash-key page keys are not canonical")
            previous = key
        if not hmac.compare_digest(self._proof, self._owner._page_proof(self)):
            raise ValueError("analysis hash-key page was modified")


class AnalysisHashKeyPlan:
    def __init__(
        self,
        *,
        authority: AnalysisPreparationAuthority,
        stage: bytes,
        input_binding: bytes,
        payload: BinaryIO,
        signing_key: bytes,
        row_count: int,
        capability: object,
    ) -> None:
        if capability is not _PLAN_TOKEN:
            raise TypeError("analysis hash-key plans are repository-issued")
        if stage not in {b"changed_file_hash", b"file_hash_decision"}:
            raise ValueError("unknown analysis hash-key stage")
        self.authority = authority
        self.stage = stage
        self.input_binding = require_digest32(
            input_binding, field="analysis hash-key input"
        )
        self.row_count = require_int63(
            row_count, field="analysis hash-key plan row count"
        )
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
            raise TypeError("analysis hash-key plan is not repository-issued")
        if self._closed:
            raise ValueError("analysis hash-key plan is closed")
        self.authority.__post_init__()
        if not hmac.compare_digest(self._metadata_proof, self._metadata_mac()):
            raise ValueError("analysis hash-key plan metadata was modified")

    def _metadata_mac(self) -> bytes:
        authority = self.authority
        data = bytearray(b"h2hdb-analysis-hash-key-plan-v1\0")
        data.extend(len(self.stage).to_bytes(8, "big") + self.stage)
        data.extend(
            authority.analysis_id + authority.build_id + authority.input_manifest_sha256
        )
        data.extend(
            require_digest32(self.input_binding, field="analysis hash-key input")
        )
        for value in (authority.generation, authority.policy_id, self.row_count):
            data.extend(
                require_int63(value, field="analysis hash-key metadata").to_bytes(
                    8, "big"
                )
            )
        for component, count, sealed_at in authority.component_seals:
            data.extend(len(component).to_bytes(8, "big") + component)
            data.extend(count.to_bytes(8, "big") + sealed_at.to_bytes(8, "big"))
        return hmac.digest(self._signing_key, data, "sha256")

    def _record(self, position: int) -> bytes:
        self._require_open()
        if not 0 <= position < self.row_count:
            raise ValueError("analysis hash-key plan position is out of range")
        self._payload.seek(position * _RECORD_BYTES)
        record = self._payload.read(_RECORD_BYTES)
        if len(record) != _RECORD_BYTES:
            raise ValueError("analysis hash-key plan is truncated")
        key, proof = record[:32], record[32:]
        expected = hmac.digest(
            self._signing_key, position.to_bytes(8, "big") + key, "sha256"
        )
        if not hmac.compare_digest(proof, expected):
            raise ValueError("analysis hash-key plan record was modified")
        return key

    def source_page(self, *, after: bytes | None, limit: int) -> tuple[bytes, ...]:
        self._require_open()
        require_positive_int63(limit, field="analysis hash-key page limit")
        if limit > 128:
            raise ValueError("analysis hash-key source page exceeds 128")
        if after is not None:
            require_digest32(after, field="analysis hash-key cursor")
        self._payload.seek(0, 2)
        if self._payload.tell() != self.row_count * _RECORD_BYTES:
            raise ValueError("analysis hash-key plan length changed")
        lower, upper = 0, self.row_count
        while lower < upper:
            middle = (lower + upper) // 2
            if after is not None and self._record(middle) <= after:
                lower = middle + 1
            else:
                upper = middle
        keys = tuple(
            self._record(position)
            for position in range(lower, min(self.row_count, lower + limit))
        )
        if any(left >= right for left, right in zip(keys, keys[1:])):
            raise ValueError("analysis hash-key plan records are not ordered")
        return keys

    def _page_proof(self, page: AnalysisHashKeyPage) -> bytes:
        data = bytearray(b"h2hdb-analysis-hash-key-page-v1\0" + self._metadata_mac())
        for value in (
            page.checkpoint_generation,
            page.checkpoint_processed_count,
            page.page_limit,
            page.source_count,
        ):
            data.extend(
                require_int63(value, field="analysis hash-key coordinate").to_bytes(
                    8, "big"
                )
            )
        for raw in (page.batch_key, page.checkpoint_cursor):
            data.extend(len(raw).to_bytes(8, "big") + raw)
        data.extend(b"".join(page.keys))
        return hmac.digest(self._signing_key, data, "sha256")

    def _prepare_page(
        self, issue: AnalysisStageIssue, keys: tuple[bytes, ...]
    ) -> AnalysisHashKeyPage:
        self._require_open()
        if (
            issue.stage != self.stage
            or issue.batch_key is None
            or issue.checkpoint_generation is None
            or issue.checkpoint_cursor is None
            or issue.checkpoint_processed_count is None
        ):
            raise ValueError("analysis hash-key issue lacks its checkpoint")
        page = AnalysisHashKeyPage(
            self.authority,
            self.stage,
            self.input_binding,
            issue.batch_key,
            issue.checkpoint_generation,
            issue.checkpoint_cursor,
            issue.checkpoint_processed_count,
            issue.page_limit,
            keys,
            self.row_count,
            self,
            b"",
            _PAGE_TOKEN,
        )
        object.__setattr__(page, "_proof", self._page_proof(page))
        page.verify()
        return page


def build_analysis_hash_key_plan(
    authority: AnalysisPreparationAuthority,
    input_binding: bytes,
    hashes: Iterable[bytes],
    *,
    stage: bytes,
    progress: Callable[[], None] | None = None,
) -> AnalysisHashKeyPlan:
    authority.__post_init__()
    payload = cast(BinaryIO, TemporaryFile(mode="w+b"))
    signing_key = secrets.token_bytes(32)
    row_count = 0
    try:
        with closing(sqlite3.connect("", isolation_level=None)) as database:
            database.execute("PRAGMA temp_store = FILE")
            database.execute("PRAGMA journal_mode = OFF")
            database.execute("PRAGMA synchronous = OFF")
            database.execute("PRAGMA cache_size = -2048")
            database.execute(
                "CREATE TABLE hashes (digest BLOB PRIMARY KEY) WITHOUT ROWID"
            )
            database.execute("BEGIN")
            for count, key in enumerate(hashes, 1):
                database.execute(
                    "INSERT OR IGNORE INTO hashes VALUES (?)",
                    (require_digest32(key, field="analysis source hash"),),
                )
                if progress is not None and count % 128 == 0:
                    progress()
            database.commit()
            for (key,) in database.execute("SELECT digest FROM hashes ORDER BY digest"):
                payload.write(
                    key
                    + hmac.digest(
                        signing_key, row_count.to_bytes(8, "big") + key, "sha256"
                    )
                )
                row_count = require_int63(
                    row_count + 1, field="analysis hash-key row count"
                )
                if progress is not None and row_count % 128 == 0:
                    progress()
        payload.flush()
        return AnalysisHashKeyPlan(
            authority=authority,
            stage=stage,
            input_binding=input_binding,
            payload=payload,
            signing_key=signing_key,
            row_count=row_count,
            capability=_PLAN_TOKEN,
        )
    except BaseException:
        payload.close()
        raise
