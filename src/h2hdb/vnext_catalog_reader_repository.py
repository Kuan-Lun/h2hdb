"""Pinned readers for the normalized greenfield catalog.

The reader never reconstructs a value from a digest alone.  Every long value
is reached through ``canonical_value_identity`` and its complete page tree,
then domain checked before any decoded value is exposed.  Revision ordering is
read from the immutable ``catalog_publication_order`` relation; callers never
sort a revision-sized result in memory.
"""

from __future__ import annotations

__all__ = [
    "VNextCatalogReadError",
    "VNextCatalogReaderRepository",
]

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from .catalog_errors import CatalogRevisionNotFoundError
from .domain import (
    CatalogArtifact,
    CatalogContributor,
    CatalogPage,
    CatalogPublication,
    CatalogRevision,
    CatalogSubject,
)
from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
)
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_utf8_bytes,
)
from .vnext_identity import artifact_name, decode_artifact_locator
from .vnext_transaction import VNextUnitOfWork

_DEFAULT_CHANNEL = b"default"
_CBZ_MEDIA_TYPE = "application/vnd.comicbook+zip"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EFFECTIVE_CONTENT_PREFIX = b"h2hdb-vnext-effective-content\0"
_EFFECTIVE_CONTENT_HEADER_BYTES = len(_EFFECTIVE_CONTENT_PREFIX) + 12


class VNextCatalogReadError(RuntimeError):
    """A supposedly immutable published revision is incomplete or corrupt."""


class _CanonicalLoader:
    def __init__(self, connector: SQLConnector, *, backend: str) -> None:
        self._work = VNextUnitOfWork(connector, backend=backend)
        self._cache: dict[tuple[bytes, bytes], bytes] = {}

    def load(self, value_sha256: object, *, domain: bytes) -> bytes:
        value = require_digest32(value_sha256, field="canonical value_sha256")
        expected_domain = require_ascii_bytes(
            domain,
            field="canonical digest domain",
            minimum=1,
            maximum=64,
        )
        key = (value, expected_domain)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        parts: list[bytes] = []
        self.validate(value, domain=expected_domain, consume=parts.append)
        payload = b"".join(parts)
        self._cache[key] = payload
        return payload

    def validate(
        self,
        value_sha256: object,
        *,
        domain: bytes,
        consume: Callable[[bytes], None],
    ) -> int:
        value = require_digest32(value_sha256, field="canonical value_sha256")
        expected_domain = require_ascii_bytes(
            domain,
            field="canonical digest domain",
            minimum=1,
            maximum=64,
        )
        try:
            receipt = CanonicalValueRepository.stream_and_validate(
                self._work,
                value_sha256=value,
                consume_provisional=consume,
            )
        except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
            raise VNextCatalogReadError(
                "canonical reference failed exact page-tree validation"
            ) from error
        if receipt.digest_domain != expected_domain:
            raise VNextCatalogReadError(
                "canonical reference uses the wrong registered digest domain"
            )
        return receipt.byte_count

    def validate_effective_content(self, value_sha256: object) -> None:
        carry = bytearray()
        expected_count: int | None = None
        emitted = 0
        previous: bytes | None = None

        def consume(part: bytes) -> None:
            nonlocal expected_count, emitted, previous
            carry.extend(part)
            if expected_count is None and len(carry) >= _EFFECTIVE_CONTENT_HEADER_BYTES:
                prefix = bytes(carry[: len(_EFFECTIVE_CONTENT_PREFIX)])
                version_offset = len(_EFFECTIVE_CONTENT_PREFIX)
                version = int.from_bytes(
                    carry[version_offset : version_offset + 4], "big"
                )
                count = int.from_bytes(
                    carry[version_offset + 4 : _EFFECTIVE_CONTENT_HEADER_BYTES],
                    "big",
                )
                del carry[:_EFFECTIVE_CONTENT_HEADER_BYTES]
                if prefix != _EFFECTIVE_CONTENT_PREFIX or version != 1:
                    raise VNextCatalogReadError(
                        "effective-content frame has an unknown prefix or version"
                    )
                expected_count = require_int63(count, field="effective file_count")
            while expected_count is not None and len(carry) >= 32:
                digest = bytes(carry[:32])
                del carry[:32]
                if previous is not None and digest < previous:
                    raise VNextCatalogReadError(
                        "effective-content file digests are not ordered"
                    )
                previous = digest
                emitted += 1
                if emitted > expected_count:
                    raise VNextCatalogReadError(
                        "effective-content exceeds its declared file_count"
                    )

        byte_count = self.validate(
            value_sha256,
            domain=b"effective_content_v1",
            consume=consume,
        )
        if expected_count is None or carry or emitted != expected_count:
            raise VNextCatalogReadError("effective-content frame is truncated")
        if byte_count != _EFFECTIVE_CONTENT_HEADER_BYTES + 32 * expected_count:
            raise VNextCatalogReadError(
                "effective-content byte_count disagrees with file_count"
            )

    def text(self, value_sha256: object, *, domain: bytes, field: str) -> str:
        payload = self.load(value_sha256, domain=domain)
        return require_utf8_bytes(
            payload,
            field=field,
            maximum=(1 << 63) - 1,
        ).decode("utf-8", errors="strict")


class VNextCatalogReaderRepository:
    """Read immutable publications from one explicit or current revision."""

    def __init__(self, *, backend: str) -> None:
        if backend not in {"sqlite", "mariadb"}:
            raise ValueError(f"unsupported SQL backend {backend!r}")
        self._backend = backend

    def get_catalog_revision(
        self,
        connector: SQLConnector,
        revision: int | None = None,
        *,
        channel: bytes = _DEFAULT_CHANNEL,
    ) -> CatalogRevision:
        exact_channel = require_ascii_bytes(
            channel,
            field="catalog channel",
            minimum=1,
            maximum=64,
        )
        if revision is None:
            head = connector.fetch_one(
                "SELECT revision FROM catalog_publication_heads WHERE channel = %s",
                (exact_channel,),
            )
            if len(head) != 1:
                raise CatalogRevisionNotFoundError(0)
            selected = require_positive_int63(head[0], field="catalog head revision")
        else:
            selected = require_positive_int63(revision, field="catalog revision")
        row = connector.fetch_one(
            "SELECT revision, publication_count, published_at "
            "FROM catalog_revisions WHERE revision = %s",
            (selected,),
        )
        if len(row) != 3:
            raise CatalogRevisionNotFoundError(selected)
        if require_positive_int63(row[0], field="catalog revision") != selected:
            raise VNextCatalogReadError("catalog revision lookup returned another key")
        return CatalogRevision(
            selected,
            _datetime_from_microseconds(row[2], field="catalog published_at"),
            require_int63(row[1], field="catalog publication_count"),
        )

    def list_publications(
        self,
        connector: SQLConnector,
        *,
        query: str | None = None,
        revision: CatalogRevision | int | None = None,
        offset: int = 0,
        limit: int = 50,
        require_artifact: bool = False,
    ) -> CatalogPage:
        if query is not None:
            if not isinstance(query, str):
                raise TypeError("catalog query must be str or None")
            if query.strip():
                raise VNextCatalogReadError(
                    "catalog search is unavailable until the normalized "
                    "revision-pinned search index is built"
                )
        page_offset = require_int63(offset, field="catalog page offset")
        page_limit = require_positive_int63(limit, field="catalog page limit")
        if page_limit > 1000:
            raise ValueError("catalog page limit must not exceed 1000")
        if not isinstance(require_artifact, bool):
            raise TypeError("require_artifact must be bool")
        pinned = self._pin(connector, revision)
        total = pinned.publication_count
        if require_artifact:
            total_row = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_publication_order AS o "
                "WHERE o.revision = %s AND EXISTS ("
                "SELECT 1 FROM catalog_artifacts AS a "
                "JOIN catalog_artifact_identity AS i "
                "ON i.artifact_id = a.artifact_id "
                "WHERE a.revision = o.revision "
                "AND i.publication_key = o.publication_key)",
                (pinned.revision,),
            )
            if len(total_row) != 1:
                raise VNextCatalogReadError(
                    "artifact-filtered catalog count has an invalid shape"
                )
            total = require_int63(total_row[0], field="filtered publication_count")
            if total > pinned.publication_count:
                raise VNextCatalogReadError(
                    "artifact-filtered count exceeds the revision publication_count"
                )
        if page_offset > total:
            page_offset = total
        if require_artifact:
            rows = connector.fetch_all(
                "SELECT o.position, o.publication_key "
                "FROM catalog_publication_order AS o "
                "WHERE o.revision = %s AND EXISTS ("
                "SELECT 1 FROM catalog_artifacts AS a "
                "JOIN catalog_artifact_identity AS i "
                "ON i.artifact_id = a.artifact_id "
                "WHERE a.revision = o.revision "
                "AND i.publication_key = o.publication_key) "
                "ORDER BY o.position LIMIT %s OFFSET %s",
                (pinned.revision, page_limit, page_offset),
            )
        else:
            rows = connector.fetch_all(
                "SELECT o.position, o.publication_key "
                "FROM catalog_publication_order AS o "
                "WHERE o.revision = %s AND o.position >= %s "
                "ORDER BY o.position LIMIT %s",
                (pinned.revision, page_offset, page_limit),
            )
        expected_count = min(page_limit, total - page_offset)
        if len(rows) != expected_count:
            raise VNextCatalogReadError(
                "catalog publication_count disagrees with its immutable order rows"
            )
        keys: list[bytes] = []
        previous_position: int | None = None
        for index, row in enumerate(rows):
            if len(row) != 2:
                raise VNextCatalogReadError("catalog order row has an invalid shape")
            position = require_int63(row[0], field="catalog publication position")
            if not require_artifact and position != page_offset + index:
                raise VNextCatalogReadError(
                    "catalog publication order is not zero-based and contiguous"
                )
            if previous_position is not None and position <= previous_position:
                raise VNextCatalogReadError(
                    "artifact-filtered publication order is not strictly increasing"
                )
            previous_position = position
            keys.append(require_digest32(row[1], field="publication_key"))
        loader = _CanonicalLoader(connector, backend=self._backend)
        publications = tuple(
            self._hydrate_publication(
                connector,
                loader,
                revision=pinned.revision,
                publication_key=key,
            )
            for key in keys
        )
        return CatalogPage(
            revision=pinned,
            publications=publications,
            offset=page_offset,
            limit=page_limit,
            total=total,
        )

    def get_publication(
        self,
        connector: SQLConnector,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None:
        identifier = require_ascii_bytes(
            publication_id.encode("ascii", errors="strict"),
            field="publication_id",
            minimum=1,
            maximum=64,
        )
        pinned = self._pin(connector, revision)
        row = connector.fetch_one(
            "SELECT i.publication_key "
            "FROM catalog_publication_identities AS i "
            "JOIN catalog_publications AS p ON p.publication_key = i.publication_key "
            "AND p.revision = %s WHERE i.publication_id = %s",
            (pinned.revision, identifier),
        )
        if not row:
            return None
        if len(row) != 1:
            raise VNextCatalogReadError("publication lookup has an invalid shape")
        return self._hydrate_publication(
            connector,
            _CanonicalLoader(connector, backend=self._backend),
            revision=pinned.revision,
            publication_key=require_digest32(row[0], field="publication_key"),
        )

    def get_artifact(
        self,
        connector: SQLConnector,
        artifact_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifact | None:
        identifier = require_ascii_bytes(
            artifact_id.encode("ascii", errors="strict"),
            field="artifact_id",
            minimum=1,
            maximum=128,
        )
        pinned = self._pin(connector, revision)
        row = connector.fetch_one(
            "SELECT artifact_id FROM catalog_artifacts "
            "WHERE revision = %s AND artifact_id = %s",
            (pinned.revision, identifier),
        )
        if not row:
            return None
        return self._hydrate_artifact(
            connector,
            _CanonicalLoader(connector, backend=self._backend),
            revision=pinned.revision,
            artifact_id=identifier,
        )

    def get_publications_by_artifact_names(
        self,
        connector: SQLConnector,
        names: Sequence[str],
        *,
        revision: CatalogRevision | int | None = None,
    ) -> Mapping[str, CatalogPublication]:
        pinned = self._pin(connector, revision)
        encoded: list[tuple[str, bytes]] = []
        seen: set[bytes] = set()
        for name in names:
            raw = require_utf8_bytes(
                name.encode("utf-8", errors="strict"),
                field="artifact_name",
                minimum=1,
                maximum=255,
                reject_nul=True,
            )
            if raw in seen:
                continue
            seen.add(raw)
            encoded.append((name, raw))
        loader = _CanonicalLoader(connector, backend=self._backend)
        result: dict[str, CatalogPublication] = {}
        hydrated: dict[bytes, CatalogPublication] = {}
        for name, raw in encoded:
            rows = connector.fetch_all(
                "SELECT i.publication_key FROM catalog_publication_identities AS p "
                "JOIN catalog_artifact_identity AS i "
                "ON i.publication_key = p.publication_key "
                "JOIN catalog_artifacts AS a ON a.artifact_id = i.artifact_id "
                "WHERE a.revision = %s AND p.artifact_name = %s "
                "ORDER BY i.publication_key LIMIT 2",
                (pinned.revision, raw),
            )
            if not rows:
                continue
            if len(rows) != 1:
                raise VNextCatalogReadError(
                    "artifact name is ambiguous within a published revision"
                )
            key = require_digest32(rows[0][0], field="publication_key")
            publication = hydrated.get(key)
            if publication is None:
                publication = self._hydrate_publication(
                    connector,
                    loader,
                    revision=pinned.revision,
                    publication_key=key,
                )
                hydrated[key] = publication
            result[name] = publication
        return result

    def _pin(
        self,
        connector: SQLConnector,
        revision: CatalogRevision | int | None,
    ) -> CatalogRevision:
        if isinstance(revision, CatalogRevision):
            persisted = self.get_catalog_revision(connector, revision.revision)
            if persisted != revision:
                raise VNextCatalogReadError(
                    "caller-pinned catalog revision descriptor disagrees"
                )
            return revision
        return self.get_catalog_revision(connector, revision)

    def _hydrate_publication(
        self,
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_key: bytes,
    ) -> CatalogPublication:
        row = connector.fetch_one(
            "SELECT i.publication_id, i.gid, p.summary_sha256, "
            "p.language_sha256, p.published_at, p.modified_at, "
            "t.source_title_sha256, t.source_gallery_name, d.title_sha256, "
            "s.sort_title_sha256, c.content_sha256 "
            "FROM catalog_publications AS p "
            "JOIN catalog_publication_identities AS i "
            "ON i.publication_key = p.publication_key "
            "JOIN catalog_publication_titles AS t "
            "ON t.revision = p.revision AND t.publication_key = p.publication_key "
            "JOIN catalog_display_title_choices AS d "
            "ON d.display_title_policy_id = t.display_title_policy_id "
            "AND d.source_title_sha256 = t.source_title_sha256 "
            "AND d.source_gallery_name = t.source_gallery_name "
            "JOIN catalog_display_title_policies AS dp "
            "ON dp.display_title_policy_id = t.display_title_policy_id "
            "JOIN catalog_title_sorts AS s "
            "ON s.title_sort_policy_id = dp.title_sort_policy_id "
            "AND s.title_sha256 = d.title_sha256 "
            "LEFT JOIN catalog_publication_contents AS c "
            "ON c.revision = p.revision AND c.publication_key = p.publication_key "
            "WHERE p.revision = %s AND p.publication_key = %s",
            (revision, publication_key),
        )
        if len(row) != 11:
            raise VNextCatalogReadError(
                "published item lacks its exact identity/title projection"
            )
        publication_id = require_ascii_bytes(
            row[0], field="publication_id", minimum=1, maximum=64
        ).decode("ascii")
        gid = require_positive_int63(row[1], field="publication gid")
        expected_id = f"urn:h2h:gallery:{gid}"
        if publication_id != expected_id:
            raise VNextCatalogReadError("publication ID does not encode its GID")
        summary = loader.text(
            row[2], domain=b"catalog_summary_utf8_v1", field="catalog summary"
        )
        language = loader.text(
            row[3], domain=b"catalog_language_utf8_v1", field="catalog language"
        )
        source_title = loader.text(
            row[6], domain=b"source_title_utf8_v1", field="source title"
        )
        source_gallery_name = require_utf8_bytes(
            row[7],
            field="source_gallery_name",
            minimum=1,
            maximum=255,
            reject_nul=True,
        ).decode("utf-8")
        title = loader.text(
            row[8], domain=b"display_title_utf8_v1", field="display title"
        )
        sort_title = loader.text(
            row[9], domain=b"title_sort_utf8_v1", field="sort title"
        )
        content_sha256 = (
            None
            if row[10] is None
            else require_digest32(row[10], field="content_sha256").hex()
        )
        if row[10] is not None:
            loader.validate_effective_content(row[10])
        contributors = self._contributors(
            connector,
            loader,
            revision=revision,
            publication_key=publication_key,
        )
        subjects = self._subjects(
            connector,
            loader,
            revision=revision,
            publication_key=publication_key,
        )
        artifact_rows = connector.fetch_all(
            "SELECT a.artifact_id FROM catalog_artifacts AS a "
            "JOIN catalog_artifact_identity AS i ON i.artifact_id = a.artifact_id "
            "WHERE a.revision = %s AND i.publication_key = %s "
            "ORDER BY a.artifact_id",
            (revision, publication_key),
        )
        artifacts = tuple(
            self._hydrate_artifact(
                connector,
                loader,
                revision=revision,
                artifact_id=require_ascii_bytes(
                    artifact_row[0],
                    field="artifact_id",
                    minimum=1,
                    maximum=128,
                ),
            )
            for artifact_row in artifact_rows
        )
        return CatalogPublication(
            publication_id=publication_id,
            gid=gid,
            title=title,
            source_title=source_title,
            sort_title=sort_title,
            summary=summary,
            language=language,
            published_at=_datetime_from_microseconds(
                row[4], field="publication published_at"
            ),
            modified_at=_datetime_from_microseconds(
                row[5], field="publication modified_at"
            ),
            source_gallery_name=source_gallery_name,
            contributors=contributors,
            subjects=subjects,
            artifacts=artifacts,
            content_sha256=content_sha256,
        )

    @staticmethod
    def _contributors(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_key: bytes,
    ) -> tuple[CatalogContributor, ...]:
        rows = connector.fetch_all(
            "SELECT c.position, c.contributor_name_sha256, c.role, s.sort_as_sha256 "
            "FROM catalog_contributors AS c "
            "LEFT JOIN catalog_contributor_sort_as AS s "
            "ON s.revision = c.revision AND s.publication_key = c.publication_key "
            "AND s.position = c.position "
            "WHERE c.revision = %s AND c.publication_key = %s "
            "ORDER BY c.position",
            (revision, publication_key),
        )
        result: list[CatalogContributor] = []
        for expected_position, row in enumerate(rows):
            if require_int63(row[0], field="contributor position") != expected_position:
                raise VNextCatalogReadError("contributor positions are not contiguous")
            name = loader.text(
                row[1],
                domain=b"contributor_name_utf8_v1",
                field="contributor name",
            )
            role = require_utf8_bytes(
                row[2], field="contributor role", minimum=1, maximum=64
            ).decode("utf-8")
            sort_as = (
                None
                if row[3] is None
                else loader.text(
                    row[3],
                    domain=b"contributor_sort_as_utf8_v1",
                    field="contributor sort_as",
                )
            )
            result.append(CatalogContributor(name=name, role=role, sort_as=sort_as))
        return tuple(result)

    @staticmethod
    def _subjects(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_key: bytes,
    ) -> tuple[CatalogSubject, ...]:
        rows = connector.fetch_all(
            "SELECT s.position, t.namespace, t.tag_value_sha256 "
            "FROM catalog_subjects AS s JOIN catalog_tag_terms AS t "
            "ON t.tag_id = s.tag_id "
            "WHERE s.revision = %s AND s.publication_key = %s "
            "ORDER BY s.position",
            (revision, publication_key),
        )
        result: list[CatalogSubject] = []
        for expected_position, row in enumerate(rows):
            if require_int63(row[0], field="subject position") != expected_position:
                raise VNextCatalogReadError("subject positions are not contiguous")
            namespace = require_utf8_bytes(
                row[1], field="tag namespace", maximum=128
            ).decode("utf-8")
            value = loader.text(row[2], domain=b"tag_value_utf8_v1", field="tag value")
            result.append(
                CatalogSubject(
                    name=value,
                    scheme=f"h2h:tag:{namespace}",
                    code=namespace,
                )
            )
        return tuple(result)

    @staticmethod
    def _hydrate_artifact(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        artifact_id: bytes,
    ) -> CatalogArtifact:
        row = connector.fetch_one(
            "SELECT p.artifact_name, i.artifact_sha256, b.size_bytes, "
            "l.artifact_locator_sha256, a.modified_at, p.gid "
            "FROM catalog_artifacts AS a "
            "JOIN catalog_artifact_identity AS i ON i.artifact_id = a.artifact_id "
            "JOIN catalog_artifact_blobs AS b ON b.artifact_sha256 = i.artifact_sha256 "
            "JOIN catalog_artifact_location AS l "
            "ON l.artifact_sha256 = i.artifact_sha256 "
            "JOIN catalog_publication_identities AS p "
            "ON p.publication_key = i.publication_key "
            "WHERE a.revision = %s AND a.artifact_id = %s",
            (revision, artifact_id),
        )
        if len(row) != 6:
            raise VNextCatalogReadError("catalog artifact lacks identity or location")
        identifier = require_ascii_bytes(
            artifact_id, field="artifact_id", minimum=1, maximum=128
        ).decode("ascii")
        name_bytes = require_utf8_bytes(
            row[0],
            field="artifact_name",
            minimum=1,
            maximum=255,
            reject_nul=True,
        )
        name = name_bytes.decode("utf-8")
        if name in {".", ".."} or "/" in name or "\\" in name:
            raise VNextCatalogReadError("artifact name is not a safe leaf")
        artifact_sha256 = require_digest32(row[1], field="artifact_sha256")
        gid_marker = identifier.split(":")
        if (
            len(gid_marker) != 7
            or gid_marker[:4] != ["urn", "h2h", "artifact", "cbz"]
            or gid_marker[5] != "sha256"
            or gid_marker[6] != artifact_sha256.hex()
        ):
            raise VNextCatalogReadError(
                "artifact ID does not encode its exact byte identity"
            )
        try:
            encoded_gid = require_positive_int63(
                int(gid_marker[4]), field="artifact GID"
            )
        except ValueError as error:
            raise VNextCatalogReadError("artifact ID has an invalid GID") from error
        publication_gid = require_positive_int63(row[5], field="publication GID")
        if encoded_gid != publication_gid:
            raise VNextCatalogReadError("artifact ID belongs to another publication")
        if name_bytes != artifact_name(publication_gid):
            raise VNextCatalogReadError(
                "artifact name is not the canonical GID-derived leaf"
            )
        locator_payload = loader.load(row[3], domain=b"artifact_locator_bytes_v1")
        if len(locator_payload) > 4096:
            raise VNextCatalogReadError("artifact locator exceeds its v1 bound")
        try:
            components = decode_artifact_locator(locator_payload)
        except ValueError as error:
            raise VNextCatalogReadError(
                "artifact locator framing is invalid"
            ) from error
        if not components:
            raise VNextCatalogReadError("artifact locator must not be empty")
        relative = PurePosixPath(*components)
        if relative.is_absolute() or any(
            part in {"", ".", ".."} for part in relative.parts
        ):
            raise VNextCatalogReadError("artifact locator is not a safe relative path")
        return CatalogArtifact(
            artifact_id=identifier,
            name=name,
            location=Path(*components),
            media_type=_CBZ_MEDIA_TYPE,
            size_bytes=require_int63(row[2], field="artifact size_bytes"),
            sha256=artifact_sha256.hex(),
            modified_at=_datetime_from_microseconds(
                row[4], field="artifact modified_at"
            ),
        )


def _datetime_from_microseconds(value: object, *, field: str) -> datetime:
    microseconds = require_int63(value, field=field)
    try:
        return _EPOCH + timedelta(microseconds=microseconds)
    except OverflowError as error:
        raise VNextCatalogReadError(f"{field} exceeds Python datetime") from error
