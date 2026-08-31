"""Current-head readers for the normalized greenfield catalog.

The reader never reconstructs a value from a digest alone.  Every long value
is reached through ``canonical_value_identity`` and its complete page tree,
then domain checked before any decoded value is exposed.  Revision ordering is
read from the immutable ``catalog_publication_order`` relation; callers never
sort a revision-sized result in memory.  A returned descriptor remains usable
only while it still names the current publication head; head advancement makes
every explicit or pinned historical revision fail closed.
"""

from __future__ import annotations

__all__ = [
    "VNextCatalogIdentifierError",
    "VNextCatalogReadError",
    "VNextCatalogReaderRepository",
]

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from unicodedata import category

from . import vnext_identity as identity
from .catalog_errors import (
    CatalogCursorError,
    CatalogIdentifierError,
    CatalogRevisionNotFoundError,
)
from .catalog_search import (
    SEARCH_LEXEME_DOMAIN,
    SEARCH_POLICY_ID,
)
from .domain import (
    DEFAULT_CATALOG_DISCOVERY_QUERY,
    ByteExtent,
    CatalogArtifact,
    CatalogContributor,
    CatalogContributorFilter,
    CatalogDiscoveryCursor,
    CatalogDiscoveryPage,
    CatalogDiscoveryQuery,
    CatalogFacetCursor,
    CatalogFacetKind,
    CatalogFacetPage,
    CatalogFacetValue,
    CatalogImageResource,
    CatalogPublication,
    CatalogPublicationPresentation,
    CatalogRecentOrder,
    CatalogRecentWindow,
    CatalogResourceKind,
    CatalogRevision,
    CatalogSubject,
    CatalogSubjectFilter,
    StorageObjectDescriptor,
    StorageObjectKey,
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
from .vnext_transaction import VNextUnitOfWork

_DEFAULT_CHANNEL = b"default"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EFFECTIVE_CONTENT_PREFIX = b"h2hdb-vnext-effective-content\0"
_EFFECTIVE_CONTENT_HEADER_BYTES = len(_EFFECTIVE_CONTENT_PREFIX) + 12


class VNextCatalogReadError(RuntimeError):
    """A supposedly immutable published revision is incomplete or corrupt."""


class VNextCatalogIdentifierError(VNextCatalogReadError, CatalogIdentifierError):
    """A caller supplied a noncanonical public catalog identifier."""


@dataclass(frozen=True, slots=True)
class _ArtifactFacts:
    artifact_sha256: bytes
    size_bytes: int
    artifact_semantics_sha256: bytes
    artifact_name: bytes
    media_type: bytes
    page_count: int


@dataclass(frozen=True, slots=True)
class _PublishedResources:
    acquisition: StorageObjectDescriptor
    cover: CatalogImageResource | None
    thumbnail: CatalogImageResource | None


@dataclass(frozen=True, slots=True)
class _DiscoverySQLFilter:
    cte: str
    join: str
    cte_parameters: tuple[object, ...]
    clauses: tuple[str, ...]
    parameters: tuple[object, ...]


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
    """Read publications only from the exact current catalog head."""

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
        requested = (
            None
            if revision is None
            else require_positive_int63(revision, field="catalog revision")
        )
        row = connector.fetch_one(
            "SELECT committed.revision, descriptor.publication_count, "
            "descriptor.artifact_count, "
            "committed.committed_at, committed.generation "
            "FROM catalog_channel_registry AS registry "
            "JOIN catalog_publication_commit_head_receipts AS head "
            "ON head.channel = registry.channel "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = head.receipt_id "
            "JOIN catalog_source_revision_descriptors AS source "
            "ON source.source_revision = committed.source_revision "
            "AND source.channel = registry.channel "
            "JOIN catalog_revision_descriptors AS descriptor "
            "ON descriptor.revision = committed.revision "
            "WHERE registry.channel = %s",
            (exact_channel,),
        )
        missing = 0 if requested is None else requested
        if len(row) != 5:
            raise CatalogRevisionNotFoundError(missing)
        selected = require_positive_int63(row[0], field="catalog head revision")
        if requested is not None and requested != selected:
            raise CatalogRevisionNotFoundError(requested)
        require_positive_int63(row[4], field="catalog revision generation")
        try:
            return CatalogRevision(
                selected,
                _datetime_from_microseconds(row[3], field="catalog published_at"),
                require_int63(row[1], field="catalog publication_count"),
                require_int63(row[2], field="catalog artifact_count"),
            )
        except ValueError as error:
            raise VNextCatalogReadError(
                "catalog revision violates the all-or-none artifact-count contract"
            ) from error

    def discover_publications(
        self,
        connector: SQLConnector,
        *,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogDiscoveryCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogDiscoveryPage:
        """Seek one title-ordered, bounded discovery page."""

        if not isinstance(query, CatalogDiscoveryQuery):
            raise TypeError("query must be CatalogDiscoveryQuery")
        query = _validated_discovery_query(query)
        if after is not None and not isinstance(after, CatalogDiscoveryCursor):
            raise TypeError("after must be CatalogDiscoveryCursor or None")
        if after is not None:
            after = _validated_discovery_cursor(after)
        page_limit = require_positive_int63(limit, field="discovery page limit")
        if page_limit > 128:
            raise ValueError("discovery page limit must not exceed 128")
        query_sha256 = _discovery_query_sha256(query)
        requested_revision = revision
        if after is not None and revision is None:
            requested_revision = after.revision
        pinned = self._pin(connector, requested_revision)
        after_position = -1
        cursor_key: bytes | None = None
        if after is not None:
            if after.revision != pinned.revision:
                raise CatalogCursorError(
                    "discovery cursor revision differs from the pinned revision"
                )
            if after.query_sha256 != query_sha256:
                raise CatalogCursorError("discovery cursor belongs to another query")
            if after.position >= pinned.publication_count:
                raise CatalogCursorError(
                    "discovery cursor position lies outside the sealed catalog"
                )
            try:
                gid = identity.decode_publication_id(
                    after.publication_id.encode("ascii", errors="strict")
                )
            except (UnicodeError, identity.VNextIdentityError) as error:
                raise CatalogCursorError(
                    "discovery cursor publication_id is not canonical"
                ) from error
            cursor_key = identity.publication_key(gid)
            after_position = after.position

        seal = connector.fetch_one(
            "SELECT policy_id FROM catalog_discovery_seals WHERE revision = %s",
            (pinned.revision,),
        )
        if seal != (SEARCH_POLICY_ID,):
            raise VNextCatalogReadError(
                "catalog revision lacks its exact discovery seal"
            )
        sql_filter = _discovery_filter_sql(
            query,
            revision=pinned.revision,
            backend=self._backend,
        )
        if after is not None and (
            cursor_key is None
            or not _discovery_cursor_matches(
                connector,
                revision=pinned.revision,
                position=after.position,
                publication_key=cursor_key,
                sql_filter=sql_filter,
            )
        ):
            raise CatalogCursorError(
                "discovery cursor is not a member of its exact query"
            )
        parameters: list[object] = [
            *sql_filter.cte_parameters,
            pinned.revision,
            pinned.revision,
            pinned.revision,
            after_position,
            *sql_filter.parameters,
        ]
        parameters.append(page_limit + 1)
        rows = connector.fetch_all(
            _discovery_page_sql(sql_filter),
            tuple(parameters),
        )
        if query == DEFAULT_CATALOG_DISCOVERY_QUERY:
            expected_row_count = min(
                page_limit + 1,
                max(0, pinned.publication_count - (after_position + 1)),
            )
            if len(rows) != expected_row_count:
                raise VNextCatalogReadError(
                    "catalog publication_count disagrees with its discovery order"
                )
        parsed: list[tuple[int, bytes, int]] = []
        previous = after_position
        for row in rows:
            if len(row) != 3:
                raise VNextCatalogReadError("discovery order row has an invalid shape")
            position = require_int63(row[0], field="discovery order position")
            key = require_digest32(row[1], field="discovery publication_key")
            gid = require_positive_int63(row[2], field="discovery publication GID")
            if (
                position <= previous
                or position >= pinned.publication_count
                or (
                    query == DEFAULT_CATALOG_DISCOVERY_QUERY
                    and position != previous + 1
                )
                or identity.publication_key(gid) != key
            ):
                raise VNextCatalogReadError(
                    "discovery order is not strict and congruent"
                )
            parsed.append((position, key, gid))
            previous = position

        visible = parsed[:page_limit]
        loader = _CanonicalLoader(connector, backend=self._backend)
        hydrated = (
            self._hydrate_publications(
                connector,
                loader,
                revision=pinned.revision,
                publication_keys=tuple(item[1] for item in visible),
                artifacts_required=pinned.artifact_count > 0,
            )
            if visible
            else {}
        )
        publications = tuple(hydrated[item[1]] for item in visible)
        next_cursor = None
        if len(parsed) > page_limit:
            position, _key, gid = visible[-1]
        else:
            position = gid = -1
        if position >= 0:
            next_cursor = CatalogDiscoveryCursor(
                revision=pinned.revision,
                query_sha256=query_sha256,
                position=position,
                publication_id=identity.publication_id(gid).decode("ascii"),
            )
        self._assert_still_current(connector, pinned)
        return CatalogDiscoveryPage(
            revision=pinned,
            publications=publications,
            next_cursor=next_cursor,
            limit=page_limit,
            total=(
                pinned.publication_count
                if query == DEFAULT_CATALOG_DISCOVERY_QUERY
                else None
            ),
        )

    def list_publication_facets(
        self,
        connector: SQLConnector,
        *,
        facet: CatalogFacetKind,
        query: CatalogDiscoveryQuery = DEFAULT_CATALOG_DISCOVERY_QUERY,
        after: CatalogFacetCursor | None = None,
        limit: int = 50,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogFacetPage:
        """Return one seek page of exact facet values under cross-filters."""

        if type(facet) is not CatalogFacetKind:
            raise TypeError("facet must be CatalogFacetKind")
        if not isinstance(query, CatalogDiscoveryQuery):
            raise TypeError("query must be CatalogDiscoveryQuery")
        query = _validated_discovery_query(query)
        if after is not None and not isinstance(after, CatalogFacetCursor):
            raise TypeError("after must be CatalogFacetCursor or None")
        if after is not None:
            after = _validated_facet_cursor(after)
        page_limit = require_positive_int63(limit, field="facet page limit")
        if page_limit > 128:
            raise ValueError("facet page limit must not exceed 128")
        effective = CatalogDiscoveryQuery(
            search=query.search,
            language=None if facet is CatalogFacetKind.LANGUAGE else query.language,
            subject=None if facet is CatalogFacetKind.SUBJECT else query.subject,
            contributor=(
                None if facet is CatalogFacetKind.CONTRIBUTOR else query.contributor
            ),
        )
        query_sha256 = _discovery_query_sha256(effective)
        requested_revision = revision
        if after is not None and revision is None:
            requested_revision = after.revision
        pinned = self._pin(connector, requested_revision)
        seal = connector.fetch_one(
            "SELECT policy_id FROM catalog_discovery_seals WHERE revision = %s",
            (pinned.revision,),
        )
        if seal != (SEARCH_POLICY_ID,):
            raise VNextCatalogReadError(
                "catalog revision lacks its exact discovery seal"
            )
        sql_filter = _discovery_filter_sql(
            effective,
            revision=pinned.revision,
            backend=self._backend,
        )
        loader = _CanonicalLoader(connector, backend=self._backend)
        after_position = -1
        if after is not None:
            if (
                after.revision != pinned.revision
                or after.facet is not facet
                or after.query_sha256 != query_sha256
            ):
                raise CatalogCursorError("facet cursor belongs to another query")
            cursor_row = connector.fetch_one(
                _facet_filtered_sql(
                    facet,
                    sql_filter=sql_filter,
                    position_operator="=",
                    include_limit=False,
                ),
                (
                    *sql_filter.cte_parameters,
                    pinned.revision,
                    after.position,
                    *sql_filter.parameters,
                ),
            )
            if not cursor_row:
                raise CatalogCursorError(
                    "facet cursor is not a member of the filtered facet"
                )
            cursor_position, _cursor_value, actual_digest = _catalog_facet_value(
                loader,
                facet=facet,
                row=cursor_row,
            )
            if cursor_position != after.position or actual_digest != after.value_sha256:
                raise CatalogCursorError(
                    "facet cursor does not match an exact filtered facet row"
                )
            after_position = after.position

        rows = connector.fetch_all(
            _facet_filtered_sql(
                facet,
                sql_filter=sql_filter,
                position_operator=">",
                include_limit=True,
            ),
            (
                *sql_filter.cte_parameters,
                pinned.revision,
                after_position,
                *sql_filter.parameters,
                page_limit + 1,
            ),
        )
        parsed = tuple(
            _catalog_facet_value(loader, facet=facet, row=row) for row in rows
        )
        previous_position = after_position
        for position, _value, _digest in parsed:
            if position <= previous_position or (
                effective == DEFAULT_CATALOG_DISCOVERY_QUERY
                and position != previous_position + 1
            ):
                raise VNextCatalogReadError("facet order is not strict and congruent")
            previous_position = position
        visible = parsed[:page_limit]
        next_cursor = None
        if len(parsed) > page_limit:
            position, _value, value_sha256 = visible[-1]
            next_cursor = CatalogFacetCursor(
                revision=pinned.revision,
                query_sha256=query_sha256,
                facet=facet,
                position=position,
                value_sha256=value_sha256,
            )
        self._assert_still_current(connector, pinned)
        return CatalogFacetPage(
            revision=pinned,
            facet=facet,
            values=tuple(value for _position, value, _digest in visible),
            next_cursor=next_cursor,
            limit=page_limit,
        )

    def list_recent_publications(
        self,
        connector: SQLConnector,
        *,
        order: CatalogRecentOrder,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogRecentWindow:
        """Dynamically sort the complete artifact set and return its fixed top 128."""

        if type(order) is not CatalogRecentOrder:
            raise TypeError("recent artifact order must be CatalogRecentOrder")
        pinned = self._pin(connector, revision)
        order_expression = (
            "upload.upload_time"
            if order is CatalogRecentOrder.UPLOADED
            else "downloaded.download_time"
        )
        rows = connector.fetch_all(
            "SELECT artifact.publication_key, occurrence.publication_key, "
            "identity.publication_key, identity.gid, upload.upload_time, "
            "downloaded.download_time, COUNT(*) OVER (), "
            "MAX(CASE WHEN occurrence.publication_key IS NULL "
            "OR identity.publication_key IS NULL OR upload.upload_time IS NULL "
            "OR downloaded.download_time IS NULL THEN 1 ELSE 0 END) OVER () "
            "FROM catalog_artifacts AS artifact "
            "LEFT JOIN catalog_publication_occurrence_identities AS occurrence "
            "ON occurrence.revision = artifact.revision "
            "AND occurrence.publication_key = artifact.publication_key "
            "LEFT JOIN catalog_publication_download_times AS downloaded "
            "ON downloaded.catalog_occurrence_sha256 = "
            "occurrence.catalog_occurrence_sha256 "
            "LEFT JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = artifact.publication_key "
            "LEFT JOIN catalog_gallery_upload_times AS upload "
            "ON upload.gid = identity.gid "
            "WHERE artifact.revision = %s "
            f"ORDER BY {order_expression} DESC, identity.gid DESC LIMIT 128",
            (pinned.revision,),
        )
        expected_count = min(128, pinned.artifact_count)
        if len(rows) != expected_count:
            raise VNextCatalogReadError(
                "catalog artifact_count disagrees with the recent artifact set"
            )
        keys: list[bytes] = []
        gids: dict[bytes, int] = {}
        for row in rows:
            if len(row) != 8 or any(value is None for value in row):
                raise VNextCatalogReadError(
                    "recent artifact ordering authority is incomplete"
                )
            key = require_digest32(row[0], field="recent artifact publication_key")
            occurrence_key = require_digest32(
                row[1], field="recent occurrence publication_key"
            )
            identity_key = require_digest32(
                row[2], field="recent identity publication_key"
            )
            gid = require_positive_int63(row[3], field="recent publication GID")
            require_int63(row[4], field="recent upload_time")
            require_int63(row[5], field="recent download_time")
            total = require_int63(row[6], field="recent artifact total")
            incomplete = require_int63(
                row[7], field="recent incomplete authority count"
            )
            if incomplete != 0:
                raise VNextCatalogReadError(
                    "recent artifact ordering authority is incomplete"
                )
            if (
                occurrence_key != key
                or identity_key != key
                or identity.publication_key(gid) != key
                or key in gids
                or total != pinned.artifact_count
            ):
                raise VNextCatalogReadError(
                    "recent artifact ordering authority is noncongruent"
                )
            gids[key] = gid
            keys.append(key)
        loader = _CanonicalLoader(connector, backend=self._backend)
        hydrated = self._hydrate_publications(
            connector,
            loader,
            revision=pinned.revision,
            publication_keys=keys,
            artifacts_required=True,
        )
        publications = tuple(hydrated[key] for key in keys)
        if any(
            publication.gid != gids[key] or len(publication.artifacts) != 1
            for key, publication in zip(keys, publications, strict=True)
        ):
            raise VNextCatalogReadError(
                "recent artifact window hydrated a noncongruent publication"
            )
        self._assert_still_current(connector, pinned)
        return CatalogRecentWindow(
            revision=pinned,
            order=order,
            publications=publications,
        )

    def get_publication(
        self,
        connector: SQLConnector,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublication | None:
        if not isinstance(publication_id, str):
            raise TypeError("publication_id must be str")
        try:
            gid = identity.decode_publication_id(
                publication_id.encode("ascii", errors="strict")
            )
        except (UnicodeError, identity.VNextIdentityError) as error:
            raise VNextCatalogIdentifierError(
                "publication ID is not an exact registered identity"
            ) from error
        publication_key = identity.publication_key(gid)
        pinned = self._pin(connector, revision)
        publications = self._hydrate_publications(
            connector,
            _CanonicalLoader(connector, backend=self._backend),
            revision=pinned.revision,
            publication_keys=(publication_key,),
            artifacts_required=pinned.artifact_count > 0,
            require_all=False,
        )
        publication = publications.get(publication_key)
        if publication is not None and publication.gid != gid:
            raise VNextCatalogReadError(
                "publication identity collides with the requested GID"
            )
        self._assert_still_current(connector, pinned)
        return publication

    def get_publication_presentation(
        self,
        connector: SQLConnector,
        publication_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogPublicationPresentation | None:
        gid, publication_key = _decode_publication_identifier(publication_id)
        pinned = self._pin(connector, revision)
        if not self._publication_identity_is_current(
            connector,
            revision=pinned.revision,
            publication_key=publication_key,
            gid=gid,
        ):
            self._assert_still_current(connector, pinned)
            return None
        artifact = self._artifact_facts_for_publications(
            connector,
            revision=pinned.revision,
            publication_keys=(publication_key,),
        ).get(publication_key)
        if artifact is None:
            if pinned.artifact_count > 0:
                raise VNextCatalogReadError(
                    "artifact-bearing revision lacks a publication artifact"
                )
            self._assert_no_presentation_without_artifact(
                connector,
                revision=pinned.revision,
                publication_keys=(publication_key,),
            )
            presentation = CatalogPublicationPresentation(
                publication_id=publication_id,
                page_count=0,
                cover=None,
                thumbnail=None,
            )
        else:
            resources = self._published_resources_for_artifacts(
                connector,
                revision=pinned.revision,
                artifacts={publication_key: artifact},
            )[publication_key]
            presentation = CatalogPublicationPresentation(
                publication_id=publication_id,
                page_count=artifact.page_count,
                cover=resources.cover,
                thumbnail=resources.thumbnail,
            )
        self._assert_still_current(connector, pinned)
        return presentation

    def get_publication_page(
        self,
        connector: SQLConnector,
        publication_id: str,
        page_index: int,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogImageResource | None:
        index = require_int63(page_index, field="catalog page_index")
        if index >= 4096:
            raise ValueError("catalog page_index must be in 0..4095")
        gid, publication_key = _decode_publication_identifier(publication_id)
        pinned = self._pin(connector, revision)
        if not self._publication_identity_is_current(
            connector,
            revision=pinned.revision,
            publication_key=publication_key,
            gid=gid,
        ):
            self._assert_still_current(connector, pinned)
            return None
        artifact = self._artifact_facts_for_publications(
            connector,
            revision=pinned.revision,
            publication_keys=(publication_key,),
        ).get(publication_key)
        if artifact is None:
            if pinned.artifact_count > 0:
                raise VNextCatalogReadError(
                    "artifact-bearing revision lacks a publication artifact"
                )
            self._assert_no_presentation_without_artifact(
                connector,
                revision=pinned.revision,
                publication_keys=(publication_key,),
            )
            self._assert_still_current(connector, pinned)
            return None
        if index >= artifact.page_count:
            self._assert_still_current(connector, pinned)
            return None
        resources = self._published_resources_for_artifacts(
            connector,
            revision=pinned.revision,
            artifacts={publication_key: artifact},
        )[publication_key]
        row = connector.fetch_one(
            "SELECT resource_kind, extent_offset, extent_length, media_type, "
            "image_sha256, width, height FROM catalog_pages "
            "WHERE revision = %s AND publication_key = %s AND page_index = %s",
            (pinned.revision, publication_key, index),
        )
        if len(row) != 7:
            raise VNextCatalogReadError(
                "catalog page row is missing from its sealed dense presentation"
            )
        page = _catalog_image_resource(
            row,
            storage_object=resources.acquisition,
            expected_kind=CatalogResourceKind.ACQUISITION,
        )
        self._assert_still_current(connector, pinned)
        return page

    @staticmethod
    def _publication_identity_is_current(
        connector: SQLConnector,
        *,
        revision: int,
        publication_key: bytes,
        gid: int,
    ) -> bool:
        row = connector.fetch_one(
            "SELECT identity.gid FROM catalog_publication_order AS ordering "
            "JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = ordering.publication_key "
            "WHERE ordering.revision = %s AND ordering.publication_key = %s",
            (revision, publication_key),
        )
        if not row:
            return False
        if len(row) != 1:
            raise VNextCatalogReadError(
                "catalog publication identity has an invalid shape"
            )
        stored_gid = require_positive_int63(row[0], field="publication GID")
        if stored_gid != gid or identity.publication_key(stored_gid) != publication_key:
            raise VNextCatalogReadError(
                "catalog publication identity collides with its canonical GID"
            )
        return True

    def get_artifact(
        self,
        connector: SQLConnector,
        artifact_id: str,
        *,
        revision: CatalogRevision | int | None = None,
    ) -> CatalogArtifact | None:
        if not isinstance(artifact_id, str):
            raise TypeError("artifact_id must be str")
        try:
            identifier = artifact_id.encode("ascii", errors="strict")
            gid, artifact_sha256 = identity.decode_artifact_id(identifier)
        except (UnicodeError, identity.VNextIdentityError) as error:
            raise VNextCatalogIdentifierError(
                "artifact ID is not an exact registered identity"
            ) from error
        publication_key = identity.publication_key(gid)
        pinned = self._pin(connector, revision)
        artifact = self._hydrate_artifact(
            connector,
            _CanonicalLoader(connector, backend=self._backend),
            revision=pinned.revision,
            publication_key=publication_key,
            expected_gid=gid,
            expected_artifact_sha256=artifact_sha256,
            artifact_required=pinned.artifact_count > 0,
        )
        self._assert_still_current(connector, pinned)
        return artifact

    def get_publications_by_artifact_names(
        self,
        connector: SQLConnector,
        names: Sequence[str],
        *,
        revision: CatalogRevision | int | None = None,
    ) -> Mapping[str, CatalogPublication]:
        if isinstance(names, (str, bytes)) or not isinstance(names, Sequence):
            raise TypeError("artifact names must be a sequence of str")
        if len(names) > 128:
            raise ValueError("artifact name lookup accepts at most 128 names")
        encoded: list[tuple[str, bytes]] = []
        seen: set[bytes] = set()
        for name in names:
            if not isinstance(name, str):
                raise TypeError("each artifact name must be str")
            try:
                raw = name.encode("utf-8", errors="strict")
            except UnicodeError as error:
                raise VNextCatalogIdentifierError(
                    "artifact name is not strict UTF-8"
                ) from error
            if (
                not 1 <= len(raw) <= 255
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
                or any(category(character).startswith("C") for character in name)
            ):
                raise VNextCatalogIdentifierError(
                    "artifact name is not a bounded safe leaf"
                )
            if raw in seen:
                continue
            seen.add(raw)
            encoded.append((name, raw))
        pinned = self._pin(connector, revision)
        if not encoded:
            self._assert_still_current(connector, pinned)
            return {}
        requested_names = tuple(raw for _name, raw in encoded)
        rows = connector.fetch_all(
            "SELECT artifact.artifact_name, artifact.publication_key, identity.gid "
            "FROM catalog_artifacts AS artifact "
            "JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = artifact.publication_key "
            f"WHERE artifact.revision = %s AND artifact.artifact_name IN "
            f"({_sql_placeholders(len(requested_names))}) "
            "ORDER BY artifact.artifact_name",
            (pinned.revision, *requested_names),
        )
        found: dict[bytes, tuple[bytes, int]] = {}
        for row in rows:
            if len(row) != 3:
                raise VNextCatalogReadError(
                    "artifact-name lookup returned an invalid row shape"
                )
            artifact_name = require_utf8_bytes(
                row[0],
                field="artifact download name",
                minimum=1,
                maximum=255,
                reject_nul=True,
            )
            key = require_digest32(row[1], field="publication_key")
            gid = require_positive_int63(row[2], field="publication GID")
            if (
                artifact_name not in seen
                or artifact_name in found
                or identity.publication_key(gid) != key
            ):
                raise VNextCatalogReadError(
                    "artifact-name lookup is not an exact unique identity set"
                )
            found[artifact_name] = (key, gid)
        loader = _CanonicalLoader(connector, backend=self._backend)
        result: dict[str, CatalogPublication] = {}
        publication_keys = tuple(sorted({value[0] for value in found.values()}))
        hydrated = self._hydrate_publications(
            connector,
            loader,
            revision=pinned.revision,
            publication_keys=publication_keys,
            artifacts_required=pinned.artifact_count > 0,
        )
        for name, raw in encoded:
            found_identity = found.get(raw)
            if found_identity is None:
                continue
            key, gid = found_identity
            publication = hydrated[key]
            if publication.gid != gid:
                raise VNextCatalogReadError(
                    "artifact-name identity collides with the requested GID"
                )
            result[name] = publication
        self._assert_still_current(connector, pinned)
        return result

    def _pin(
        self,
        connector: SQLConnector,
        revision: CatalogRevision | int | None,
    ) -> CatalogRevision:
        if isinstance(revision, CatalogRevision):
            try:
                if type(revision.published_at) is not datetime:
                    raise TypeError("catalog published_at must be datetime")
                if (
                    revision.published_at.tzinfo is not UTC
                    or revision.published_at.fold
                ):
                    raise ValueError("catalog published_at must be canonical UTC")
                canonical = CatalogRevision(
                    revision=revision.revision,
                    published_at=revision.published_at,
                    publication_count=revision.publication_count,
                    artifact_count=revision.artifact_count,
                )
            except (TypeError, ValueError) as error:
                raise VNextCatalogReadError(
                    "caller-pinned catalog revision descriptor is malformed"
                ) from error
            persisted = self.get_catalog_revision(connector, canonical.revision)
            if not _catalog_revisions_match(persisted, canonical):
                raise VNextCatalogReadError(
                    "caller-pinned catalog revision descriptor disagrees"
                )
            pinned = canonical
        else:
            pinned = self.get_catalog_revision(connector, revision)
        if pinned.artifact_count == 0:
            self._assert_revision_has_no_artifact_authority(
                connector,
                revision=pinned.revision,
            )
        return pinned

    @staticmethod
    def _assert_revision_has_no_artifact_authority(
        connector: SQLConnector,
        *,
        revision: int,
    ) -> None:
        row = connector.fetch_one(
            "SELECT publication_key FROM ("
            "SELECT publication_key FROM catalog_artifacts WHERE revision = %s "
            "UNION ALL SELECT publication_key FROM catalog_storage_objects "
            "WHERE revision = %s UNION ALL SELECT publication_key FROM catalog_pages "
            "WHERE revision = %s UNION ALL SELECT publication_key "
            "FROM catalog_thumbnails WHERE revision = %s"
            ") AS artifact_authority LIMIT 1",
            (revision, revision, revision, revision),
        )
        if row:
            if len(row) != 1:
                raise VNextCatalogReadError(
                    "zero-artifact authority probe has an invalid shape"
                )
            require_digest32(row[0], field="stray artifact publication_key")
            raise VNextCatalogReadError(
                "zero-artifact revision retains artifact or presentation authority"
            )

    def _assert_still_current(
        self,
        connector: SQLConnector,
        pinned: CatalogRevision,
    ) -> None:
        if self.get_catalog_revision(connector) != pinned:
            raise VNextCatalogReadError(
                "catalog publication head advanced during the read"
            )

    def _hydrate_publications(
        self,
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_keys: Sequence[bytes],
        artifacts_required: bool,
        require_all: bool = True,
    ) -> dict[bytes, CatalogPublication]:
        selected = tuple(
            require_digest32(value, field="publication_key")
            for value in publication_keys
        )
        if not selected:
            return {}
        if len(selected) > 128 or len(set(selected)) != len(selected):
            raise VNextCatalogReadError(
                "publication hydration keys must be unique and bounded"
            )
        selected_cte = _selected_keys_cte(len(selected), backend=self._backend)
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}), "
            "family_keys(publication_key) AS ("
            "SELECT ordering.publication_key FROM catalog_publication_order AS ordering "
            "JOIN selected AS chosen ON chosen.publication_key = ordering.publication_key "
            "WHERE ordering.revision = %s UNION "
            "SELECT publication.publication_key FROM catalog_publications AS publication "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = publication.publication_key "
            "WHERE publication.revision = %s UNION "
            "SELECT title.publication_key FROM catalog_publication_titles AS title "
            "JOIN selected AS chosen ON chosen.publication_key = title.publication_key "
            "WHERE title.revision = %s UNION "
            "SELECT content.publication_key FROM catalog_publication_contents AS content "
            "JOIN selected AS chosen ON chosen.publication_key = content.publication_key "
            "WHERE content.revision = %s) "
            "SELECT family.publication_key, ordering.publication_key, "
            "publication.publication_key, publication.gallery_id, "
            "publication.summary_sha256, publication.language_sha256, "
            "publication.modified_at, publication.download_time, "
            "identity.gid, upload.upload_time, "
            "title.publication_key, title.source_title_sha256, "
            "title.source_gallery_name, "
            "committed.receipt_id, committed.display_title_policy_id, "
            "policy.display_title_policy_id, choice.title_sha256, "
            "policy.title_sort_policy_id, "
            "title_sort.sort_title_sha256, content.content_sha256 "
            "FROM family_keys AS family "
            "LEFT JOIN catalog_publication_order AS ordering "
            "ON ordering.revision = %s AND ordering.publication_key = family.publication_key "
            "LEFT JOIN catalog_publications AS publication "
            "ON publication.revision = %s "
            "AND publication.publication_key = family.publication_key "
            "LEFT JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = publication.publication_key "
            "LEFT JOIN catalog_gallery_upload_times AS upload ON upload.gid = identity.gid "
            "LEFT JOIN catalog_publication_titles AS title "
            "ON title.revision = publication.revision "
            "AND title.publication_key = publication.publication_key "
            "LEFT JOIN catalog_publication_commits AS committed "
            "ON committed.revision = publication.revision "
            "LEFT JOIN catalog_display_title_policies AS policy "
            "ON policy.display_title_policy_id = committed.display_title_policy_id "
            "LEFT JOIN catalog_display_title_choices AS choice "
            "ON choice.display_title_policy_id = policy.display_title_policy_id "
            "AND choice.source_title_sha256 = title.source_title_sha256 "
            "AND choice.source_gallery_name = title.source_gallery_name "
            "LEFT JOIN catalog_title_sorts AS title_sort "
            "ON title_sort.title_sort_policy_id = policy.title_sort_policy_id "
            "AND title_sort.title_sha256 = choice.title_sha256 "
            "LEFT JOIN catalog_publication_contents AS content "
            "ON content.revision = publication.revision "
            "AND content.publication_key = publication.publication_key "
            "ORDER BY family.publication_key",
            (*selected, *(revision for _ in range(6))),
        )
        expected = set(selected)
        scalar_by_key: dict[bytes, tuple[object, ...]] = {}
        for row in rows:
            if len(row) != 20:
                raise VNextCatalogReadError(
                    "published item scalar query returned an invalid shape"
                )
            key = require_digest32(row[0], field="publication_key")
            if key not in expected or key in scalar_by_key:
                raise VNextCatalogReadError(
                    "published item scalar query is not one-to-one"
                )
            if any(value is None for value in row[1:19]):
                raise VNextCatalogReadError(
                    "published item scalar/title row is missing or noncongruent"
                )
            if row[1] != key or row[2] != key or row[10] != key:
                raise VNextCatalogReadError(
                    "published item scalar/title keys are noncongruent"
                )
            if row[14] != row[15]:
                raise VNextCatalogReadError(
                    "publication display-title policy is noncongruent"
                )
            scalar_by_key[key] = (
                key,
                row[8],
                row[4],
                row[5],
                row[9],
                row[6],
                row[7],
                row[11],
                row[12],
                row[16],
                row[18],
                row[19],
            )
        if require_all and set(scalar_by_key) != expected:
            raise VNextCatalogReadError(
                "published item lacks its exact identity/title projection"
            )

        visible = tuple(sorted(scalar_by_key))
        if not visible:
            return {}
        contributors = self._contributors_for_publications(
            connector, loader, revision=revision, publication_keys=visible
        )
        subjects = self._subjects_for_publications(
            connector, loader, revision=revision, publication_keys=visible
        )
        artifact_facts = self._artifact_facts_for_publications(
            connector, revision=revision, publication_keys=visible
        )
        if artifacts_required and set(artifact_facts) != set(visible):
            raise VNextCatalogReadError(
                "artifact-bearing revision lacks a publication artifact"
            )
        if not artifacts_required and artifact_facts:
            raise VNextCatalogReadError(
                "zero-artifact revision retains a publication artifact"
            )
        self._assert_no_presentation_without_artifact(
            connector,
            revision=revision,
            publication_keys=tuple(key for key in visible if key not in artifact_facts),
        )
        published_resources = self._published_resources_for_artifacts(
            connector,
            revision=revision,
            artifacts=artifact_facts,
        )

        result: dict[bytes, CatalogPublication] = {}
        for key in visible:
            row = scalar_by_key[key]
            gid = require_positive_int63(row[1], field="publication gid")
            if identity.publication_key(gid) != key:
                raise VNextCatalogReadError(
                    "publication key disagrees with its immutable GID"
                )
            source_gallery_name = require_utf8_bytes(
                row[8],
                field="source_gallery_name",
                minimum=1,
                maximum=255,
                reject_nul=True,
            ).decode("utf-8")
            content_sha256 = None
            if row[11] is not None:
                content = require_digest32(row[11], field="content_sha256")
                loader.validate_effective_content(content)
                content_sha256 = content.hex()
            modified_at = _datetime_from_microseconds(
                row[5], field="publication modified_at"
            )
            artifact = artifact_facts.get(key)
            resources = published_resources.get(key)
            artifacts = (
                ()
                if artifact is None
                else (
                    _catalog_artifact_from_facts(
                        publication_key=key,
                        gid=gid,
                        facts=artifact,
                        storage_object=_require_published_resources(
                            resources
                        ).acquisition,
                    ),
                )
            )
            result[key] = CatalogPublication(
                publication_id=identity.publication_id(gid).decode("ascii"),
                gid=gid,
                title=loader.text(
                    row[9], domain=b"display_title_utf8_v1", field="display title"
                ),
                source_title=loader.text(
                    row[7], domain=b"source_title_utf8_v1", field="source title"
                ),
                sort_title=loader.text(
                    row[10], domain=b"title_sort_utf8_v1", field="sort title"
                ),
                summary=loader.text(
                    row[2], domain=b"catalog_summary_utf8_v1", field="catalog summary"
                ),
                language=loader.text(
                    row[3],
                    domain=b"catalog_language_utf8_v1",
                    field="catalog language",
                ),
                published_at=_datetime_from_microseconds(
                    row[4], field="publication published_at"
                ),
                modified_at=modified_at,
                downloaded_at=_datetime_from_microseconds(
                    row[6], field="publication downloaded_at"
                ),
                source_gallery_name=source_gallery_name,
                page_count=0 if artifact is None else artifact.page_count,
                cover=None if resources is None else resources.cover,
                thumbnail=None if resources is None else resources.thumbnail,
                contributors=contributors.get(key, ()),
                subjects=subjects.get(key, ()),
                artifacts=artifacts,
                content_sha256=content_sha256,
            )
        return result

    def _contributors_for_publications(
        self,
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[bytes, tuple[CatalogContributor, ...]]:
        selected_cte = _selected_keys_cte(len(publication_keys), backend=self._backend)
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT contributor.publication_key, contributor.position, "
            "contributor.contributor_name_sha256, contributor.role, roles.role "
            "FROM catalog_contributors AS contributor JOIN selected AS chosen "
            "ON chosen.publication_key = contributor.publication_key "
            "LEFT JOIN catalog_contributor_role_registry AS roles "
            "ON roles.role = contributor.role WHERE contributor.revision = %s "
            "ORDER BY contributor.publication_key, contributor.position",
            (*publication_keys, revision),
        )
        grouped: dict[bytes, list[CatalogContributor]] = {}
        for row in rows:
            if len(row) != 5 or any(value is None for value in row[2:]):
                raise VNextCatalogReadError(
                    "contributor occurrence has invalid or unregistered facts"
                )
            key = require_digest32(row[0], field="publication_key")
            position = require_int63(row[1], field="contributor position")
            current = grouped.setdefault(key, [])
            if position != len(current):
                raise VNextCatalogReadError("contributor positions are not contiguous")
            name = loader.text(
                row[2],
                domain=b"contributor_name_utf8_v1",
                field="contributor name",
            )
            role_bytes = require_ascii_bytes(
                row[3], field="contributor role", minimum=1, maximum=64
            )
            if (
                require_ascii_bytes(
                    row[4], field="registered contributor role", minimum=1, maximum=64
                )
                != role_bytes
            ):
                raise VNextCatalogReadError("contributor role registry disagrees")
            current.append(
                CatalogContributor(name=name, role=role_bytes.decode("ascii"))
            )
        return {key: tuple(values) for key, values in grouped.items()}

    @staticmethod
    def _subjects_for_publications(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[bytes, tuple[CatalogSubject, ...]]:
        placeholders = _sql_placeholders(len(publication_keys))
        rows = connector.fetch_all(
            "SELECT s.publication_key, s.position, s.tag_id, "
            "t.tag_id, t.namespace, t.tag_value_sha256 "
            "FROM catalog_subjects AS s "
            "LEFT JOIN catalog_tag_terms AS t ON t.tag_id = s.tag_id "
            f"WHERE s.revision = %s AND s.publication_key IN ({placeholders}) "
            "ORDER BY s.publication_key, s.position",
            (revision, *publication_keys),
        )
        grouped: dict[bytes, list[CatalogSubject]] = {}
        for row in rows:
            if len(row) != 6 or any(value is None for value in row[2:]):
                raise VNextCatalogReadError("subject row lacks its tag identity")
            key = require_digest32(row[0], field="publication_key")
            position = require_int63(row[1], field="subject position")
            if require_positive_int63(
                row[2], field="subject tag_id"
            ) != require_positive_int63(row[3], field="stored subject tag_id"):
                raise VNextCatalogReadError("subject tag identity disagrees")
            current = grouped.setdefault(key, [])
            if position != len(current):
                raise VNextCatalogReadError("subject positions are not contiguous")
            namespace = require_utf8_bytes(
                row[4], field="tag namespace", maximum=128
            ).decode("utf-8")
            value = loader.text(row[5], domain=b"tag_value_utf8_v1", field="tag value")
            current.append(
                CatalogSubject(
                    name=value,
                    scheme=f"h2h:tag:{namespace}",
                    code=namespace,
                )
            )
        return {key: tuple(values) for key, values in grouped.items()}

    def _artifact_facts_for_publications(
        self,
        connector: SQLConnector,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[bytes, _ArtifactFacts]:
        selected_cte = _selected_keys_cte(len(publication_keys), backend=self._backend)
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT artifact.publication_key, artifact.artifact_sha256, "
            "artifact.artifact_semantics_sha256, artifact_blob_row.size_bytes, "
            "artifact.artifact_name, artifact.media_type, artifact.page_count "
            "FROM catalog_artifacts AS artifact "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = artifact.publication_key "
            "LEFT JOIN catalog_artifact_blobs AS artifact_blob_row "
            "ON artifact_blob_row.artifact_sha256 = artifact.artifact_sha256 "
            "WHERE artifact.revision = %s ORDER BY artifact.publication_key",
            (*publication_keys, revision),
        )
        result: dict[bytes, _ArtifactFacts] = {}
        for row in rows:
            if len(row) != 7 or any(value is None for value in row[1:]):
                raise VNextCatalogReadError(
                    "catalog artifact lacks total storage facts"
                )
            key = require_digest32(row[0], field="artifact publication_key")
            if key in result:
                raise VNextCatalogReadError("catalog artifact key is duplicated")
            page_count = require_int63(row[6], field="artifact page_count")
            if page_count > 4096:
                raise VNextCatalogReadError("catalog artifact exceeds 4096 pages")
            result[key] = _ArtifactFacts(
                artifact_sha256=require_digest32(row[1], field="artifact_sha256"),
                size_bytes=require_positive_int63(row[3], field="artifact size_bytes"),
                artifact_semantics_sha256=require_digest32(
                    row[2], field="artifact_semantics_sha256"
                ),
                artifact_name=require_utf8_bytes(
                    row[4],
                    field="artifact download name",
                    minimum=1,
                    maximum=255,
                    reject_nul=True,
                ),
                media_type=require_ascii_bytes(
                    row[5], field="artifact media_type", minimum=1, maximum=127
                ),
                page_count=page_count,
            )
        return result

    def _assert_no_presentation_without_artifact(
        self,
        connector: SQLConnector,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> None:
        if not publication_keys:
            return
        selected = tuple(
            require_digest32(value, field="publication_key")
            for value in publication_keys
        )
        selected_cte = _selected_keys_cte(len(selected), backend=self._backend)
        row = connector.fetch_one(
            f"WITH selected(publication_key) AS ({selected_cte}), "
            "presentation_authority(publication_key) AS ("
            "SELECT object.publication_key FROM catalog_storage_objects AS object "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = object.publication_key "
            "WHERE object.revision = %s UNION ALL "
            "SELECT page.publication_key FROM catalog_pages AS page "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = page.publication_key "
            "WHERE page.revision = %s UNION ALL "
            "SELECT thumbnail.publication_key FROM catalog_thumbnails AS thumbnail "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = thumbnail.publication_key "
            "WHERE thumbnail.revision = %s) "
            "SELECT publication_key FROM presentation_authority "
            "ORDER BY publication_key LIMIT 1",
            (*selected, revision, revision, revision),
        )
        if row:
            if len(row) != 1:
                raise VNextCatalogReadError(
                    "orphan catalog presentation probe has an invalid shape"
                )
            require_digest32(row[0], field="orphan presentation publication_key")
            raise VNextCatalogReadError(
                "metadata-only publication retains presentation authority"
            )

    def _published_resources_for_artifacts(
        self,
        connector: SQLConnector,
        *,
        revision: int,
        artifacts: Mapping[bytes, _ArtifactFacts],
    ) -> dict[bytes, _PublishedResources]:
        publication_keys = tuple(sorted(artifacts))
        if not publication_keys:
            return {}
        storage_objects = self._storage_objects_for_publications(
            connector,
            revision=revision,
            publication_keys=publication_keys,
        )
        selected_cte = _selected_keys_cte(len(publication_keys), backend=self._backend)
        count_rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT page.publication_key, COUNT(*), MIN(page.page_index), "
            "MAX(page.page_index) FROM catalog_pages AS page "
            "JOIN selected AS chosen "
            "ON chosen.publication_key = page.publication_key "
            "WHERE page.revision = %s GROUP BY page.publication_key "
            "ORDER BY page.publication_key",
            (*publication_keys, revision),
        )
        page_counts: dict[bytes, tuple[int, int, int]] = {}
        for row in count_rows:
            if len(row) != 4 or any(value is None for value in row[1:]):
                raise VNextCatalogReadError(
                    "catalog page coverage aggregate has an invalid shape"
                )
            key = require_digest32(row[0], field="page publication_key")
            if key not in artifacts or key in page_counts:
                raise VNextCatalogReadError(
                    "catalog page coverage is duplicated or unexpected"
                )
            page_counts[key] = (
                require_int63(row[1], field="catalog page row count"),
                require_int63(row[2], field="catalog minimum page_index"),
                require_int63(row[3], field="catalog maximum page_index"),
            )

        cover_rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT page.publication_key, page.resource_kind, "
            "page.extent_offset, page.extent_length, page.media_type, "
            "page.image_sha256, page.width, page.height "
            "FROM catalog_pages AS page JOIN selected AS chosen "
            "ON chosen.publication_key = page.publication_key "
            "WHERE page.revision = %s AND page.page_index = 0 "
            "ORDER BY page.publication_key",
            (*publication_keys, revision),
        )
        covers: dict[bytes, tuple[object, ...]] = {}
        for row in cover_rows:
            if len(row) != 8:
                raise VNextCatalogReadError("catalog cover row has an invalid shape")
            key = require_digest32(row[0], field="cover publication_key")
            if key not in artifacts or key in covers:
                raise VNextCatalogReadError(
                    "catalog cover row is duplicated or unexpected"
                )
            covers[key] = tuple(row[1:])

        thumbnail_rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT thumbnail.publication_key, thumbnail.resource_kind, "
            "thumbnail.extent_offset, thumbnail.extent_length, "
            "thumbnail.media_type, thumbnail.image_sha256, "
            "thumbnail.width, thumbnail.height "
            "FROM catalog_thumbnails AS thumbnail JOIN selected AS chosen "
            "ON chosen.publication_key = thumbnail.publication_key "
            "WHERE thumbnail.revision = %s ORDER BY thumbnail.publication_key",
            (*publication_keys, revision),
        )
        thumbnails: dict[bytes, tuple[object, ...]] = {}
        for row in thumbnail_rows:
            if len(row) != 8:
                raise VNextCatalogReadError(
                    "catalog thumbnail row has an invalid shape"
                )
            key = require_digest32(row[0], field="thumbnail publication_key")
            if key not in artifacts or key in thumbnails:
                raise VNextCatalogReadError(
                    "catalog thumbnail row is duplicated or unexpected"
                )
            thumbnails[key] = tuple(row[1:])

        result: dict[bytes, _PublishedResources] = {}
        for key, artifact in artifacts.items():
            acquisition = storage_objects.get((key, CatalogResourceKind.ACQUISITION))
            if acquisition is None:
                raise VNextCatalogReadError(
                    "catalog artifact lacks its acquisition storage object"
                )
            if (
                acquisition.sha256 != artifact.artifact_sha256.hex()
                or acquisition.size_bytes != artifact.size_bytes
            ):
                raise VNextCatalogReadError(
                    "catalog acquisition storage object disagrees with its artifact"
                )
            expected_kinds = {CatalogResourceKind.ACQUISITION}
            if artifact.page_count == 0:
                if key in page_counts or key in covers or key in thumbnails:
                    raise VNextCatalogReadError(
                        "empty catalog presentation has image rows"
                    )
                cover = thumbnail = None
            else:
                expected_kinds.add(CatalogResourceKind.THUMBNAIL)
                coverage = page_counts.get(key)
                if coverage != (artifact.page_count, 0, artifact.page_count - 1):
                    raise VNextCatalogReadError(
                        "catalog page rows do not exactly cover page_count"
                    )
                cover_row = covers.get(key)
                thumbnail_row = thumbnails.get(key)
                thumbnail_object = storage_objects.get(
                    (key, CatalogResourceKind.THUMBNAIL)
                )
                if (
                    cover_row is None
                    or thumbnail_row is None
                    or thumbnail_object is None
                ):
                    raise VNextCatalogReadError(
                        "catalog presentation lacks cover or thumbnail authority"
                    )
                cover = _catalog_image_resource(
                    cover_row,
                    storage_object=acquisition,
                    expected_kind=CatalogResourceKind.ACQUISITION,
                )
                thumbnail = _catalog_image_resource(
                    thumbnail_row,
                    storage_object=thumbnail_object,
                    expected_kind=CatalogResourceKind.THUMBNAIL,
                )
                if (
                    thumbnail.extent.offset != 0
                    or thumbnail.extent.length != thumbnail_object.size_bytes
                    or thumbnail.sha256 != thumbnail_object.sha256
                ):
                    raise VNextCatalogReadError(
                        "catalog thumbnail is not its complete sealed storage object"
                    )
            actual_kinds = {
                kind for publication, kind in storage_objects if publication == key
            }
            if actual_kinds != expected_kinds:
                raise VNextCatalogReadError(
                    "catalog storage objects do not exactly match presentation roles"
                )
            result[key] = _PublishedResources(acquisition, cover, thumbnail)
        return result

    def _storage_objects_for_publications(
        self,
        connector: SQLConnector,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[tuple[bytes, CatalogResourceKind], StorageObjectDescriptor]:
        selected_cte = _selected_keys_cte(len(publication_keys), backend=self._backend)
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}) "
            "SELECT object.publication_key, object.resource_kind, "
            "object.storage_object_key_sha256, object.storage_object_sha256, "
            "object.size_bytes, object.modified_at, key_row.storage_object_key_sha256, "
            "key_row.key_codec, key_row.segment_count "
            "FROM catalog_storage_objects AS object JOIN selected AS chosen "
            "ON chosen.publication_key = object.publication_key "
            "LEFT JOIN catalog_storage_object_key_identities AS key_row "
            "ON key_row.storage_object_key_sha256 = "
            "object.storage_object_key_sha256 "
            "WHERE object.revision = %s "
            "ORDER BY object.publication_key, object.resource_kind",
            (*publication_keys, revision),
        )
        headers: dict[
            tuple[bytes, CatalogResourceKind],
            tuple[bytes, bytes, int, datetime, str, int],
        ] = {}
        key_counts: dict[bytes, int] = {}
        for row in rows:
            if len(row) != 9 or any(value is None for value in row[1:]):
                raise VNextCatalogReadError(
                    "catalog storage object lacks its complete key identity"
                )
            publication_key = require_digest32(
                row[0], field="storage object publication_key"
            )
            kind = _catalog_resource_kind(row[1])
            object_key_digest = require_digest32(
                row[2], field="storage_object_key_sha256"
            )
            if (
                require_digest32(row[6], field="registered storage_object_key_sha256")
                != object_key_digest
            ):
                raise VNextCatalogReadError(
                    "storage object key identity is noncongruent"
                )
            codec = require_ascii_bytes(
                row[7], field="storage key codec", minimum=1, maximum=64
            ).decode("ascii")
            segment_count = require_positive_int63(
                row[8], field="storage key segment_count"
            )
            if segment_count > 16:
                raise VNextCatalogReadError(
                    "storage key exceeds the public segment bound"
                )
            coordinate = (publication_key, kind)
            if coordinate in headers:
                raise VNextCatalogReadError("catalog storage object is duplicated")
            headers[coordinate] = (
                object_key_digest,
                require_digest32(row[3], field="storage object sha256"),
                require_positive_int63(row[4], field="storage object size_bytes"),
                _datetime_from_microseconds(row[5], field="storage object modified_at"),
                codec,
                segment_count,
            )
            previous = key_counts.setdefault(object_key_digest, segment_count)
            if previous != segment_count:
                raise VNextCatalogReadError(
                    "storage key identity has conflicting segment counts"
                )

        key_digests = tuple(sorted(key_counts))
        if not key_digests:
            return {}
        segments_rows = connector.fetch_all(
            "SELECT storage_object_key_sha256, segment_position, key_segment "
            "FROM catalog_storage_object_key_segments "
            f"WHERE storage_object_key_sha256 IN ({_sql_placeholders(len(key_digests))}) "
            "ORDER BY storage_object_key_sha256, segment_position",
            key_digests,
        )
        segment_values: dict[bytes, list[str]] = {}
        for row in segments_rows:
            if len(row) != 3:
                raise VNextCatalogReadError("storage key segment has invalid shape")
            digest = require_digest32(row[0], field="storage key segment digest")
            if digest not in key_counts:
                raise VNextCatalogReadError("unexpected storage key segment")
            position = require_int63(row[1], field="storage key segment position")
            current = segment_values.setdefault(digest, [])
            if position != len(current):
                raise VNextCatalogReadError(
                    "storage key segments are not zero-based and contiguous"
                )
            current.append(
                require_utf8_bytes(
                    row[2],
                    field="storage key segment",
                    minimum=1,
                    maximum=255,
                    reject_nul=True,
                ).decode("utf-8")
            )

        result: dict[tuple[bytes, CatalogResourceKind], StorageObjectDescriptor] = {}
        for coordinate, header in headers.items():
            key_digest, object_digest, size, modified_at, codec, segment_count = header
            segments = tuple(segment_values.get(key_digest, ()))
            if len(segments) != segment_count:
                raise VNextCatalogReadError("storage key segment family is incomplete")
            try:
                key = StorageObjectKey(codec, segments)
            except (TypeError, ValueError, UnicodeError) as error:
                raise VNextCatalogReadError(
                    "stored storage object key violates the public domain"
                ) from error
            if identity.artifact_storage_key_digest(codec, segments) != key_digest:
                raise VNextCatalogReadError(
                    "storage object key digest disagrees with its exact segments"
                )
            result[coordinate] = StorageObjectDescriptor(
                key=key,
                size_bytes=size,
                sha256=object_digest.hex(),
                modified_at=modified_at,
            )
        return result

    def _hydrate_artifact(
        self,
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_key: bytes,
        expected_gid: int | None = None,
        expected_artifact_sha256: bytes | None = None,
        artifact_required: bool = False,
    ) -> CatalogArtifact | None:
        key = require_digest32(publication_key, field="publication_key")
        facts = self._artifact_facts_for_publications(
            connector,
            revision=revision,
            publication_keys=(key,),
        ).get(key)
        if facts is None:
            if artifact_required and expected_gid is not None:
                if self._publication_identity_is_current(
                    connector,
                    revision=revision,
                    publication_key=key,
                    gid=expected_gid,
                ):
                    raise VNextCatalogReadError(
                        "artifact-bearing revision lacks a publication artifact"
                    )
            self._assert_no_presentation_without_artifact(
                connector,
                revision=revision,
                publication_keys=(key,),
            )
            return None
        if (
            expected_artifact_sha256 is not None
            and facts.artifact_sha256
            != require_digest32(
                expected_artifact_sha256,
                field="expected artifact_sha256",
            )
        ):
            return None
        row = connector.fetch_one(
            "SELECT identity.gid, publication.modified_at "
            "FROM catalog_publication_order AS ordering "
            "JOIN catalog_publications AS publication "
            "ON publication.revision = ordering.revision "
            "AND publication.publication_key = ordering.publication_key "
            "JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = publication.publication_key "
            "WHERE ordering.revision = %s AND ordering.publication_key = %s",
            (revision, key),
        )
        if not row:
            raise VNextCatalogReadError(
                "catalog artifact refers to an incomplete publication"
            )
        if len(row) != 2:
            raise VNextCatalogReadError(
                "catalog artifact identity has an invalid shape"
            )
        gid = require_positive_int63(row[0], field="publication GID")
        if expected_gid is not None and gid != require_positive_int63(
            expected_gid,
            field="expected publication GID",
        ):
            raise VNextCatalogReadError(
                "artifact identity collides with the requested GID"
            )
        resources = self._published_resources_for_artifacts(
            connector,
            revision=revision,
            artifacts={key: facts},
        )[key]
        return _catalog_artifact_from_facts(
            publication_key=key,
            gid=gid,
            facts=facts,
            storage_object=resources.acquisition,
        )


def _catalog_artifact_from_facts(
    *,
    publication_key: bytes,
    gid: int,
    facts: _ArtifactFacts,
    storage_object: StorageObjectDescriptor,
) -> CatalogArtifact:
    exact_key = require_digest32(publication_key, field="publication_key")
    exact_gid = require_positive_int63(gid, field="publication GID")
    if identity.publication_key(exact_gid) != exact_key:
        raise VNextCatalogReadError(
            "artifact publication key disagrees with its immutable GID"
        )
    require_digest32(facts.artifact_sha256, field="artifact_sha256")
    require_digest32(facts.artifact_semantics_sha256, field="artifact_semantics_sha256")
    if (
        storage_object.sha256 != facts.artifact_sha256.hex()
        or storage_object.size_bytes != facts.size_bytes
    ):
        raise VNextCatalogReadError(
            "artifact storage descriptor disagrees with immutable artifact facts"
        )
    try:
        return CatalogArtifact(
            artifact_id=identity.artifact_id(exact_gid, facts.artifact_sha256).decode(
                "ascii"
            ),
            name=facts.artifact_name.decode("utf-8", errors="strict"),
            storage_object=storage_object,
            media_type=facts.media_type.decode("ascii", errors="strict"),
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise VNextCatalogReadError(
            "catalog artifact violates its public domain"
        ) from error


def _require_published_resources(
    resources: _PublishedResources | None,
) -> _PublishedResources:
    if resources is None:
        raise VNextCatalogReadError(
            "catalog artifact lacks its published resource family"
        )
    return resources


def _catalog_resource_kind(value: object) -> CatalogResourceKind:
    raw = require_ascii_bytes(
        value,
        field="catalog resource_kind",
        minimum=1,
        maximum=11,
    )
    try:
        return CatalogResourceKind(raw.decode("ascii"))
    except ValueError as error:
        raise VNextCatalogReadError(
            "catalog resource_kind is not registered"
        ) from error


def _catalog_image_resource(
    row: tuple[object, ...],
    *,
    storage_object: StorageObjectDescriptor,
    expected_kind: CatalogResourceKind,
) -> CatalogImageResource:
    if len(row) != 7:
        raise VNextCatalogReadError("catalog image row has an invalid shape")
    kind = _catalog_resource_kind(row[0])
    if kind is not expected_kind:
        raise VNextCatalogReadError(
            "catalog image row references the wrong storage resource kind"
        )
    try:
        return CatalogImageResource(
            storage_object=storage_object,
            extent=ByteExtent(
                require_int63(row[1], field="catalog image extent offset"),
                require_positive_int63(row[2], field="catalog image extent length"),
            ),
            media_type=require_ascii_bytes(
                row[3], field="catalog image media_type", minimum=1, maximum=127
            ).decode("ascii"),
            sha256=require_digest32(row[4], field="catalog image sha256").hex(),
            width=require_positive_int63(row[5], field="catalog image width"),
            height=require_positive_int63(row[6], field="catalog image height"),
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise VNextCatalogReadError(
            "catalog image row violates its public domain"
        ) from error


def _decode_publication_identifier(publication_id: str) -> tuple[int, bytes]:
    if not isinstance(publication_id, str):
        raise TypeError("publication_id must be str")
    try:
        encoded = publication_id.encode("ascii", errors="strict")
        gid = identity.decode_publication_id(encoded)
    except (UnicodeError, identity.VNextIdentityError) as error:
        raise VNextCatalogIdentifierError(
            "publication ID is not an exact registered identity"
        ) from error
    if identity.publication_id(gid) != encoded:
        raise VNextCatalogIdentifierError(
            "publication ID is not in canonical encoded form"
        )
    return gid, identity.publication_key(gid)


def _validated_discovery_query(
    query: CatalogDiscoveryQuery,
) -> CatalogDiscoveryQuery:
    """Reconstruct caller-controlled query state and reject forged cache fields."""

    if type(query) is not CatalogDiscoveryQuery:
        raise TypeError("query must be CatalogDiscoveryQuery")
    subject = None
    if query.subject is not None:
        if type(query.subject) is not CatalogSubjectFilter:
            raise TypeError("discovery subject must be CatalogSubjectFilter")
        subject = CatalogSubjectFilter(
            namespace=query.subject.namespace,
            value=query.subject.value,
        )
    contributor = None
    if query.contributor is not None:
        if type(query.contributor) is not CatalogContributorFilter:
            raise TypeError("discovery contributor must be CatalogContributorFilter")
        contributor = CatalogContributorFilter(
            name=query.contributor.name,
            role=query.contributor.role,
        )
    canonical = CatalogDiscoveryQuery(
        search=query.search,
        language=query.language,
        subject=subject,
        contributor=contributor,
    )
    if query.search_lexemes != canonical.search_lexemes:
        raise ValueError("discovery query search_lexemes are not canonical")
    return canonical


def _validated_discovery_cursor(
    cursor: CatalogDiscoveryCursor,
) -> CatalogDiscoveryCursor:
    try:
        return CatalogDiscoveryCursor(
            revision=cursor.revision,
            query_sha256=cursor.query_sha256,
            position=cursor.position,
            publication_id=cursor.publication_id,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise CatalogCursorError(
            "discovery cursor violates its public domain"
        ) from error


def _validated_facet_cursor(cursor: CatalogFacetCursor) -> CatalogFacetCursor:
    try:
        return CatalogFacetCursor(
            revision=cursor.revision,
            query_sha256=cursor.query_sha256,
            facet=cursor.facet,
            position=cursor.position,
            value_sha256=cursor.value_sha256,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise CatalogCursorError("facet cursor violates its public domain") from error


def _catalog_revisions_match(left: CatalogRevision, right: CatalogRevision) -> bool:
    """Compare one public descriptor without Python's bool/int equality aliasing."""

    return (
        type(left.revision) is int
        and type(right.revision) is int
        and left.revision == right.revision
        and type(left.published_at) is datetime
        and type(right.published_at) is datetime
        and left.published_at == right.published_at
        and type(left.publication_count) is int
        and type(right.publication_count) is int
        and left.publication_count == right.publication_count
        and type(left.artifact_count) is int
        and type(right.artifact_count) is int
        and left.artifact_count == right.artifact_count
    )


def _discovery_query_sha256(query: CatalogDiscoveryQuery) -> str:
    frame = bytearray(b"h2hdb-catalog-discovery-query\0\x02")

    def append(value: bytes | None) -> None:
        if value is None:
            frame.extend(b"\x00")
        else:
            frame.extend(b"\x01" + len(value).to_bytes(4, "big") + value)

    if query.search is None:
        append(None)
    else:
        token_frame = bytearray()
        for token in sorted(query.search_lexemes):
            token_frame.extend(len(token).to_bytes(2, "big") + token)
        append(bytes(token_frame))
    append(None if query.language is None else query.language.encode("utf-8"))
    append(None if query.subject is None else query.subject.namespace.encode("utf-8"))
    append(None if query.subject is None else query.subject.value.encode("utf-8"))
    append(
        None if query.contributor is None else query.contributor.role.encode("ascii")
    )
    append(
        None if query.contributor is None else query.contributor.name.encode("utf-8")
    )
    return sha256(frame).hexdigest()


def _discovery_filter_sql(
    query: CatalogDiscoveryQuery,
    *,
    revision: int,
    backend: str,
) -> _DiscoverySQLFilter:
    exact_revision = require_positive_int63(revision, field="catalog revision")
    if backend == "sqlite":
        posting_index = "INDEXED BY sqlite_autoindex_catalog_search_postings_1 "
    elif backend == "mariadb":
        posting_index = "FORCE INDEX (PRIMARY) "
    else:
        raise ValueError("discovery SQL backend is not registered")
    cte = ""
    join = ""
    cte_parameters: tuple[object, ...] = ()
    clauses: list[str] = []
    parameters: list[object] = []
    if query.search is not None:
        value_sha256s = tuple(
            identity.canonical_value_digest(
                SEARCH_LEXEME_DOMAIN.decode("ascii"),
                lexeme,
            )
            for lexeme in sorted(query.search_lexemes)
        )
        cte = (
            "WITH matched_search(publication_key) AS ("
            "SELECT posting.publication_key "
            f"FROM catalog_search_postings AS posting {posting_index}"
            "WHERE posting.revision = %s "
            f"AND posting.value_sha256 IN ({_sql_placeholders(len(value_sha256s))}) "
            "GROUP BY posting.publication_key HAVING COUNT(*) = %s) "
        )
        join = (
            "JOIN matched_search AS search_match "
            "ON search_match.publication_key = publication.publication_key "
        )
        cte_parameters = (exact_revision, *value_sha256s, len(value_sha256s))
    if query.language is not None:
        clauses.append("AND publication.language_sha256 = %s ")
        parameters.append(
            identity.canonical_value_digest(
                "catalog_language_utf8_v1",
                query.language.encode("utf-8"),
            )
        )
    if query.subject is not None:
        clauses.append(
            "AND EXISTS (SELECT 1 FROM catalog_subjects AS filtered_subject "
            "JOIN catalog_tag_terms AS filtered_term "
            "ON filtered_term.tag_id = filtered_subject.tag_id "
            "WHERE filtered_subject.revision = publication.revision "
            "AND filtered_subject.publication_key = publication.publication_key "
            "AND filtered_term.namespace = %s "
            "AND filtered_term.tag_value_sha256 = %s) "
        )
        parameters.extend(
            (
                query.subject.namespace.encode("utf-8"),
                identity.canonical_value_digest(
                    "tag_value_utf8_v1",
                    query.subject.value.encode("utf-8"),
                ),
            )
        )
    if query.contributor is not None:
        clauses.append(
            "AND EXISTS (SELECT 1 FROM catalog_contributors AS filtered_contributor "
            "WHERE filtered_contributor.revision = publication.revision "
            "AND filtered_contributor.publication_key = publication.publication_key "
            "AND filtered_contributor.contributor_name_sha256 = %s "
            "AND filtered_contributor.role = %s) "
        )
        parameters.extend(
            (
                identity.canonical_value_digest(
                    "contributor_name_utf8_v1",
                    query.contributor.name.encode("utf-8"),
                ),
                query.contributor.role.encode("ascii"),
            )
        )
    return _DiscoverySQLFilter(
        cte=cte,
        join=join,
        cte_parameters=cte_parameters,
        clauses=tuple(clauses),
        parameters=tuple(parameters),
    )


def _discovery_page_sql(sql_filter: _DiscoverySQLFilter) -> str:
    return (
        sql_filter.cte
        + "SELECT ordering.position, ordering.publication_key, identity.gid "
        "FROM catalog_publication_order AS ordering "
        "JOIN catalog_publication_identities AS identity "
        "ON identity.publication_key = ordering.publication_key "
        "JOIN catalog_search_documents AS document "
        "ON document.revision = %s "
        "AND document.publication_key = ordering.publication_key "
        "JOIN catalog_publications AS publication "
        "ON publication.revision = %s "
        "AND publication.publication_key = ordering.publication_key "
        + sql_filter.join
        + "WHERE ordering.revision = %s AND ordering.position > %s "
        + "".join(sql_filter.clauses)
        + "ORDER BY ordering.position LIMIT %s"
    )


def _discovery_cursor_matches(
    connector: SQLConnector,
    *,
    revision: int,
    position: int,
    publication_key: bytes,
    sql_filter: _DiscoverySQLFilter,
) -> bool:
    row = connector.fetch_one(
        sql_filter.cte + "SELECT ordering.publication_key "
        "FROM catalog_publication_order AS ordering "
        "JOIN catalog_search_documents AS document "
        "ON document.revision = ordering.revision "
        "AND document.publication_key = ordering.publication_key "
        "JOIN catalog_publications AS publication "
        "ON publication.revision = ordering.revision "
        "AND publication.publication_key = ordering.publication_key "
        + sql_filter.join
        + "WHERE ordering.revision = %s AND ordering.position = %s "
        "AND ordering.publication_key = %s " + "".join(sql_filter.clauses),
        (
            *sql_filter.cte_parameters,
            revision,
            position,
            publication_key,
            *sql_filter.parameters,
        ),
    )
    return row == (publication_key,)


def _facet_query_shape(facet: CatalogFacetKind) -> tuple[str, str, str]:
    if facet is CatalogFacetKind.LANGUAGE:
        return (
            "facet.position, facet.language_sha256, "
            "COUNT(DISTINCT publication.publication_key)",
            "FROM catalog_language_facet_order AS facet "
            "JOIN catalog_publications AS publication "
            "ON publication.revision = facet.revision "
            "AND publication.language_sha256 = facet.language_sha256",
            "facet.position, facet.language_sha256",
        )
    if facet is CatalogFacetKind.SUBJECT:
        return (
            "facet.position, facet.tag_id, term.namespace, "
            "term.tag_value_sha256, "
            "COUNT(DISTINCT publication.publication_key)",
            "FROM catalog_subject_facet_order AS facet "
            "JOIN catalog_tag_terms AS term ON term.tag_id = facet.tag_id "
            "JOIN catalog_subjects AS membership "
            "ON membership.revision = facet.revision "
            "AND membership.tag_id = facet.tag_id "
            "JOIN catalog_publications AS publication "
            "ON publication.revision = membership.revision "
            "AND publication.publication_key = membership.publication_key",
            "facet.position, facet.tag_id, term.namespace, term.tag_value_sha256",
        )
    return (
        "facet.position, facet.contributor_name_sha256, facet.role, "
        "COUNT(DISTINCT publication.publication_key)",
        "FROM catalog_contributor_facet_order AS facet "
        "JOIN catalog_contributors AS membership "
        "ON membership.revision = facet.revision "
        "AND membership.contributor_name_sha256 = facet.contributor_name_sha256 "
        "AND membership.role = facet.role "
        "JOIN catalog_publications AS publication "
        "ON publication.revision = membership.revision "
        "AND publication.publication_key = membership.publication_key",
        "facet.position, facet.contributor_name_sha256, facet.role",
    )


def _facet_filtered_sql(
    facet: CatalogFacetKind,
    *,
    sql_filter: _DiscoverySQLFilter,
    position_operator: str,
    include_limit: bool,
) -> str:
    if position_operator not in {"=", ">"}:
        raise ValueError("facet position operator is not registered")
    select, joins, group_by = _facet_query_shape(facet)
    query = (
        sql_filter.cte + f"SELECT {select} {joins} "
        "JOIN catalog_search_documents AS document "
        "ON document.revision = publication.revision "
        "AND document.publication_key = publication.publication_key "
        + sql_filter.join
        + f"WHERE facet.revision = %s AND facet.position {position_operator} %s "
        + "".join(sql_filter.clauses)
        + f"GROUP BY {group_by} HAVING COUNT(*) > 0 "
        "ORDER BY facet.position"
    )
    return query + (" LIMIT %s" if include_limit else "")


def _facet_identity_sha256(
    facet: CatalogFacetKind,
    *parts: bytes,
) -> str:
    frame = bytearray(b"h2hdb-catalog-facet-value\0\x01")
    frame.extend(facet.value.encode("ascii") + b"\0")
    for part in parts:
        frame.extend(len(part).to_bytes(4, "big") + part)
    return sha256(frame).hexdigest()


def _catalog_facet_value(
    loader: _CanonicalLoader,
    *,
    facet: CatalogFacetKind,
    row: tuple[object, ...],
) -> tuple[int, CatalogFacetValue, str]:
    if facet is CatalogFacetKind.LANGUAGE:
        if len(row) != 3:
            raise VNextCatalogReadError("language facet row is malformed")
        position = require_int63(row[0], field="facet position")
        value_sha256 = require_digest32(row[1], field="language facet value")
        count = require_positive_int63(row[2], field="language facet count")
        value = loader.text(
            value_sha256,
            domain=b"catalog_language_utf8_v1",
            field="language facet value",
        )
        return (
            position,
            CatalogFacetValue(value=value, label=value, publication_count=count),
            _facet_identity_sha256(facet, value_sha256),
        )
    if facet is CatalogFacetKind.SUBJECT:
        if len(row) != 5:
            raise VNextCatalogReadError("subject facet row is malformed")
        position = require_int63(row[0], field="facet position")
        tag_id = require_positive_int63(row[1], field="subject facet tag_id")
        namespace_bytes = require_utf8_bytes(
            row[2],
            field="subject facet namespace",
            maximum=128,
        )
        value_sha256 = require_digest32(row[3], field="subject facet value")
        count = require_positive_int63(row[4], field="subject facet count")
        namespace = namespace_bytes.decode("utf-8")
        value = loader.text(
            value_sha256,
            domain=b"tag_value_utf8_v1",
            field="subject facet value",
        )
        return (
            position,
            CatalogFacetValue(
                value=value,
                label=value,
                publication_count=count,
                namespace=namespace,
            ),
            _facet_identity_sha256(
                facet,
                tag_id.to_bytes(8, "big"),
                namespace_bytes,
                value_sha256,
            ),
        )
    if len(row) != 4:
        raise VNextCatalogReadError("contributor facet row is malformed")
    position = require_int63(row[0], field="facet position")
    value_sha256 = require_digest32(row[1], field="contributor facet value")
    role_bytes = require_ascii_bytes(
        row[2],
        field="contributor facet role",
        minimum=1,
        maximum=64,
    )
    count = require_positive_int63(row[3], field="contributor facet count")
    role = role_bytes.decode("ascii")
    value = loader.text(
        value_sha256,
        domain=b"contributor_name_utf8_v1",
        field="contributor facet value",
    )
    return (
        position,
        CatalogFacetValue(
            value=value,
            label=value,
            publication_count=count,
            role=role,
        ),
        _facet_identity_sha256(facet, value_sha256, role_bytes),
    )


def _datetime_from_microseconds(value: object, *, field: str) -> datetime:
    microseconds = require_int63(value, field=field)
    try:
        return _EPOCH + timedelta(microseconds=microseconds)
    except OverflowError as error:
        raise VNextCatalogReadError(f"{field} exceeds Python datetime") from error


def _sql_placeholders(count: int) -> str:
    if count <= 0:
        raise ValueError("SQL placeholder count must be positive")
    return ", ".join("%s" for _ in range(count))


def _selected_keys_cte(count: int, *, backend: str) -> str:
    if count <= 0:
        raise ValueError("selected-key count must be positive")
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError(f"unsupported SQL backend {backend!r}")
    parameter = "CAST(%s AS BINARY(32))" if backend == "mariadb" else "%s"
    return " UNION ALL ".join(
        (
            f"SELECT {parameter} AS publication_key",
            *(f"SELECT {parameter}" for _ in range(count - 1)),
        )
    )
