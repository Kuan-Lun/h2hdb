"""Pinned readers for the normalized greenfield catalog.

The reader never reconstructs a value from a digest alone.  Every long value
is reached through ``canonical_value_identity`` and its complete page tree,
then domain checked before any decoded value is exposed.  Revision ordering is
read from the immutable ``catalog_publication_order`` relation; callers never
sort a revision-sized result in memory.
"""

from __future__ import annotations

__all__ = [
    "VNextCatalogIdentifierError",
    "VNextCatalogReadError",
    "VNextCatalogReaderRepository",
]

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from . import vnext_identity as identity
from .catalog_errors import CatalogIdentifierError, CatalogRevisionNotFoundError
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
from .vnext_transaction import VNextUnitOfWork

_DEFAULT_CHANNEL = b"default"
_CBZ_MEDIA_TYPE = "application/vnd.comicbook+zip"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EFFECTIVE_CONTENT_PREFIX = b"h2hdb-vnext-effective-content\0"
_EFFECTIVE_CONTENT_HEADER_BYTES = len(_EFFECTIVE_CONTENT_PREFIX) + 12


class VNextCatalogReadError(RuntimeError):
    """A supposedly immutable published revision is incomplete or corrupt."""


class VNextCatalogIdentifierError(VNextCatalogReadError, CatalogIdentifierError):
    """A caller supplied a noncanonical public catalog identifier."""


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
            row = connector.fetch_one(
                "SELECT catalog.revision, count.publication_count, "
                "committed.committed_at, generation.generation "
                "FROM catalog_channel_registry AS registry "
                "JOIN catalog_publication_commit_head_receipts AS head "
                "ON head.channel = registry.channel "
                "JOIN catalog_publication_commit_seals AS commit_seal "
                "ON commit_seal.receipt_id = head.receipt_id "
                "JOIN catalog_publication_commit_catalog_revisions AS catalog "
                "ON catalog.receipt_id = head.receipt_id "
                "JOIN catalog_publication_commit_source_revisions AS source "
                "ON source.receipt_id = head.receipt_id "
                "JOIN catalog_source_revision_descriptor_seals AS source_seal "
                "ON source_seal.source_revision = source.source_revision "
                "JOIN catalog_source_revision_channels AS source_channel "
                "ON source_channel.source_revision = source.source_revision "
                "AND source_channel.channel = registry.channel "
                "JOIN catalog_publication_commit_generations AS generation "
                "ON generation.receipt_id = head.receipt_id "
                "JOIN catalog_publication_commit_committed_ats AS committed "
                "ON committed.receipt_id = head.receipt_id "
                "JOIN catalog_revision_descriptor_seals AS descriptor_seal "
                "ON descriptor_seal.revision = catalog.revision "
                "JOIN catalog_revision_publication_counts AS count "
                "ON count.revision = descriptor_seal.revision "
                "WHERE registry.channel = %s",
                (exact_channel,),
            )
            if len(row) != 4:
                raise CatalogRevisionNotFoundError(0)
            selected = require_positive_int63(row[0], field="catalog head revision")
        else:
            selected = require_positive_int63(revision, field="catalog revision")
            row = connector.fetch_one(
                "SELECT catalog.revision, count.publication_count, "
                "committed.committed_at, generation.generation "
                "FROM catalog_publication_commit_catalog_revisions AS catalog "
                "JOIN catalog_publication_commit_seals AS commit_seal "
                "ON commit_seal.receipt_id = catalog.receipt_id "
                "JOIN catalog_publication_commit_source_revisions AS source "
                "ON source.receipt_id = catalog.receipt_id "
                "JOIN catalog_source_revision_descriptor_seals AS source_seal "
                "ON source_seal.source_revision = source.source_revision "
                "JOIN catalog_source_revision_channels AS source_channel "
                "ON source_channel.source_revision = source.source_revision "
                "AND source_channel.channel = %s "
                "JOIN catalog_publication_commit_generations AS generation "
                "ON generation.receipt_id = catalog.receipt_id "
                "JOIN catalog_publication_commit_committed_ats AS committed "
                "ON committed.receipt_id = catalog.receipt_id "
                "JOIN catalog_revision_descriptor_seals AS descriptor_seal "
                "ON descriptor_seal.revision = catalog.revision "
                "JOIN catalog_revision_publication_counts AS count "
                "ON count.revision = descriptor_seal.revision "
                "WHERE catalog.revision = %s",
                (exact_channel, selected),
            )
        if len(row) != 4:
            raise CatalogRevisionNotFoundError(selected)
        if require_positive_int63(row[0], field="catalog revision") != selected:
            raise VNextCatalogReadError("catalog revision lookup returned another key")
        require_positive_int63(row[3], field="catalog revision generation")
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
        if page_limit > 128:
            raise ValueError("catalog page limit must not exceed 128")
        if not isinstance(require_artifact, bool):
            raise TypeError("require_artifact must be bool")
        pinned = self._pin(connector, revision)
        total = pinned.publication_count
        if require_artifact:
            total_row = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_publication_order AS o "
                "WHERE o.revision = %s AND EXISTS ("
                "SELECT 1 FROM catalog_artifact_seals AS artifact "
                "WHERE artifact.revision = o.revision "
                "AND artifact.publication_key = o.publication_key)",
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
                "SELECT 1 FROM catalog_artifact_seals AS artifact "
                "WHERE artifact.revision = o.revision "
                "AND artifact.publication_key = o.publication_key) "
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
        hydrated = self._hydrate_publications(
            connector,
            loader,
            revision=pinned.revision,
            publication_keys=keys,
        )
        publications = tuple(hydrated[key] for key in keys)
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
            require_all=False,
        )
        publication = publications.get(publication_key)
        if publication is not None and publication.gid != gid:
            raise VNextCatalogReadError(
                "publication identity collides with the requested GID"
            )
        return publication

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
        return self._hydrate_artifact(
            connector,
            _CanonicalLoader(connector, backend=self._backend),
            revision=pinned.revision,
            publication_key=publication_key,
            expected_gid=gid,
            expected_artifact_sha256=artifact_sha256,
        )

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
        encoded: list[tuple[str, bytes, int]] = []
        seen: set[bytes] = set()
        requested_gid_by_key: dict[bytes, int] = {}
        for name in names:
            if not isinstance(name, str):
                raise TypeError("each artifact name must be str")
            try:
                raw = name.encode("ascii", errors="strict")
                gid = identity.decode_artifact_name(raw)
            except (UnicodeError, identity.VNextIdentityError) as error:
                raise VNextCatalogIdentifierError(
                    "artifact name is not an exact registered identity"
                ) from error
            if raw in seen:
                continue
            seen.add(raw)
            key = identity.publication_key(gid)
            previous_gid = requested_gid_by_key.setdefault(key, gid)
            if previous_gid != gid:
                raise VNextCatalogReadError(
                    "artifact-name request contains a publication-key collision"
                )
            encoded.append((name, key, gid))
        if not encoded:
            return {}
        pinned = self._pin(connector, revision)
        requested_keys = tuple(key for _name, key, _gid in encoded)
        rows = connector.fetch_all(
            "SELECT identity.publication_key, identity.gid "
            "FROM catalog_publication_identities AS identity "
            "JOIN catalog_artifact_seals AS artifact "
            "ON artifact.publication_key = identity.publication_key "
            f"WHERE artifact.revision = %s AND identity.publication_key IN "
            f"({_sql_placeholders(len(requested_keys))}) "
            "ORDER BY identity.publication_key",
            (pinned.revision, *requested_keys),
        )
        found: dict[bytes, int] = {}
        for row in rows:
            if len(row) != 2:
                raise VNextCatalogReadError(
                    "artifact-name lookup returned an invalid row shape"
                )
            key = require_digest32(row[0], field="publication_key")
            gid = require_positive_int63(row[1], field="publication GID")
            if (
                requested_gid_by_key.get(key) != gid
                or key in found
                or identity.publication_key(gid) != key
            ):
                raise VNextCatalogReadError(
                    "artifact-name lookup is not an exact identity set"
                )
            found[key] = gid
        loader = _CanonicalLoader(connector, backend=self._backend)
        result: dict[str, CatalogPublication] = {}
        hydrated = self._hydrate_publications(
            connector,
            loader,
            revision=pinned.revision,
            publication_keys=tuple(sorted(found)),
        )
        for name, key, gid in encoded:
            if found.get(key) != gid:
                continue
            publication = hydrated[key]
            if publication.gid != gid:
                raise VNextCatalogReadError(
                    "artifact-name identity collides with the requested GID"
                )
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

    def _hydrate_publications(
        self,
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_keys: Sequence[bytes],
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
        selected_cte = _selected_keys_cte(len(selected))
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}), "
            "family_keys(publication_key) AS ("
            "SELECT ordering.publication_key FROM catalog_publication_order AS ordering "
            "JOIN selected AS chosen ON chosen.publication_key = ordering.publication_key "
            "WHERE ordering.revision = %s UNION "
            "SELECT anchor.publication_key FROM catalog_publication_anchors AS anchor "
            "JOIN selected AS chosen ON chosen.publication_key = anchor.publication_key "
            "WHERE anchor.revision = %s UNION "
            "SELECT gallery.publication_key FROM catalog_publication_gallery_ids AS gallery "
            "JOIN selected AS chosen ON chosen.publication_key = gallery.publication_key "
            "WHERE gallery.revision = %s UNION "
            "SELECT summary.publication_key FROM catalog_publication_summary_sha256s AS summary "
            "JOIN selected AS chosen ON chosen.publication_key = summary.publication_key "
            "WHERE summary.revision = %s UNION "
            "SELECT language.publication_key FROM catalog_publication_language_sha256s AS language "
            "JOIN selected AS chosen ON chosen.publication_key = language.publication_key "
            "WHERE language.revision = %s UNION "
            "SELECT modified.publication_key FROM catalog_publication_modified_ats AS modified "
            "JOIN selected AS chosen ON chosen.publication_key = modified.publication_key "
            "WHERE modified.revision = %s UNION "
            "SELECT seal.publication_key FROM catalog_publication_seals AS seal "
            "JOIN selected AS chosen ON chosen.publication_key = seal.publication_key "
            "WHERE seal.revision = %s UNION "
            "SELECT anchor.publication_key FROM catalog_publication_title_anchors AS anchor "
            "JOIN selected AS chosen ON chosen.publication_key = anchor.publication_key "
            "WHERE anchor.revision = %s UNION "
            "SELECT source.publication_key "
            "FROM catalog_publication_title_source_title_sha256s AS source "
            "JOIN selected AS chosen ON chosen.publication_key = source.publication_key "
            "WHERE source.revision = %s UNION "
            "SELECT name.publication_key "
            "FROM catalog_publication_title_source_gallery_names AS name "
            "JOIN selected AS chosen ON chosen.publication_key = name.publication_key "
            "WHERE name.revision = %s UNION "
            "SELECT seal.publication_key FROM catalog_publication_title_seals AS seal "
            "JOIN selected AS chosen ON chosen.publication_key = seal.publication_key "
            "WHERE seal.revision = %s UNION "
            "SELECT content.publication_key FROM catalog_publication_contents AS content "
            "JOIN selected AS chosen ON chosen.publication_key = content.publication_key "
            "WHERE content.revision = %s) "
            "SELECT family.publication_key, ordering.publication_key, "
            "publication_anchor.publication_key, gallery.gallery_id, "
            "summary.summary_sha256, language.language_sha256, modified.modified_at, "
            "publication_seal.publication_key, identity.gid, upload.upload_time, "
            "title_anchor.publication_key, title_source.source_title_sha256, "
            "title_name.source_gallery_name, title_seal.publication_key, "
            "committed_revision.receipt_id, commit_seal.receipt_id, "
            "commit_policy.display_title_policy_id, policy_seal.display_title_policy_id, "
            "choice.title_sha256, policy_sort.title_sort_policy_id, "
            "title_sort.sort_title_sha256, content.content_sha256 "
            "FROM family_keys AS family "
            "LEFT JOIN catalog_publication_order AS ordering "
            "ON ordering.revision = %s AND ordering.publication_key = family.publication_key "
            "LEFT JOIN catalog_publication_anchors AS publication_anchor "
            "ON publication_anchor.revision = ordering.revision "
            "AND publication_anchor.publication_key = ordering.publication_key "
            "LEFT JOIN catalog_publication_gallery_ids AS gallery "
            "ON gallery.revision = publication_anchor.revision "
            "AND gallery.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_summary_sha256s AS summary "
            "ON summary.revision = publication_anchor.revision "
            "AND summary.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_language_sha256s AS language "
            "ON language.revision = publication_anchor.revision "
            "AND language.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_modified_ats AS modified "
            "ON modified.revision = publication_anchor.revision "
            "AND modified.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_seals AS publication_seal "
            "ON publication_seal.revision = publication_anchor.revision "
            "AND publication_seal.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_gallery_upload_times AS upload ON upload.gid = identity.gid "
            "LEFT JOIN catalog_publication_title_anchors AS title_anchor "
            "ON title_anchor.revision = publication_anchor.revision "
            "AND title_anchor.publication_key = publication_anchor.publication_key "
            "LEFT JOIN catalog_publication_title_source_title_sha256s AS title_source "
            "ON title_source.revision = title_anchor.revision "
            "AND title_source.publication_key = title_anchor.publication_key "
            "LEFT JOIN catalog_publication_title_source_gallery_names AS title_name "
            "ON title_name.revision = title_anchor.revision "
            "AND title_name.publication_key = title_anchor.publication_key "
            "LEFT JOIN catalog_publication_title_seals AS title_seal "
            "ON title_seal.revision = title_anchor.revision "
            "AND title_seal.publication_key = title_anchor.publication_key "
            "LEFT JOIN catalog_publication_commit_catalog_revisions AS committed_revision "
            "ON committed_revision.revision = publication_anchor.revision "
            "LEFT JOIN catalog_publication_commit_seals AS commit_seal "
            "ON commit_seal.receipt_id = committed_revision.receipt_id "
            "LEFT JOIN catalog_publication_commit_display_title_policies AS commit_policy "
            "ON commit_policy.receipt_id = commit_seal.receipt_id "
            "LEFT JOIN catalog_display_title_policy_seals AS policy_seal "
            "ON policy_seal.display_title_policy_id = "
            "commit_policy.display_title_policy_id "
            "LEFT JOIN catalog_display_title_choices AS choice "
            "ON choice.display_title_policy_id = policy_seal.display_title_policy_id "
            "AND choice.source_title_sha256 = title_source.source_title_sha256 "
            "AND choice.source_gallery_name = title_name.source_gallery_name "
            "LEFT JOIN catalog_display_title_policy_title_sort_policy_ids AS policy_sort "
            "ON policy_sort.display_title_policy_id = "
            "policy_seal.display_title_policy_id "
            "LEFT JOIN catalog_title_sorts AS title_sort "
            "ON title_sort.title_sort_policy_id = policy_sort.title_sort_policy_id "
            "AND title_sort.title_sha256 = choice.title_sha256 "
            "LEFT JOIN catalog_publication_contents AS content "
            "ON content.revision = publication_anchor.revision "
            "AND content.publication_key = publication_anchor.publication_key "
            "ORDER BY family.publication_key",
            (*selected, *(revision for _ in range(12)), revision),
        )
        expected = set(selected)
        scalar_by_key: dict[bytes, tuple[object, ...]] = {}
        for row in rows:
            if len(row) != 22:
                raise VNextCatalogReadError(
                    "published item scalar query returned an invalid shape"
                )
            key = require_digest32(row[0], field="publication_key")
            if key not in expected or key in scalar_by_key:
                raise VNextCatalogReadError(
                    "published item scalar query is not one-to-one"
                )
            if any(value is None for value in row[1:21]):
                raise VNextCatalogReadError(
                    "published item scalar/title family is partial or noncongruent"
                )
            scalar_by_key[key] = (
                key,
                row[8],
                row[4],
                row[5],
                row[9],
                row[6],
                row[11],
                row[12],
                row[18],
                row[20],
                row[21],
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

        result: dict[bytes, CatalogPublication] = {}
        for key in visible:
            row = scalar_by_key[key]
            gid = require_positive_int63(row[1], field="publication gid")
            if identity.publication_key(gid) != key:
                raise VNextCatalogReadError(
                    "publication key disagrees with its immutable GID"
                )
            source_gallery_name = require_utf8_bytes(
                row[7],
                field="source_gallery_name",
                minimum=1,
                maximum=255,
                reject_nul=True,
            ).decode("utf-8")
            content_sha256 = None
            if row[10] is not None:
                content = require_digest32(row[10], field="content_sha256")
                loader.validate_effective_content(content)
                content_sha256 = content.hex()
            modified_at = _datetime_from_microseconds(
                row[5], field="publication modified_at"
            )
            artifact = artifact_facts.get(key)
            artifacts = (
                ()
                if artifact is None
                else (
                    _catalog_artifact_from_facts(
                        loader,
                        publication_key=key,
                        gid=gid,
                        modified_at=modified_at,
                        artifact_sha256=artifact[0],
                        size_bytes=artifact[1],
                        locator_sha256=artifact[2],
                        artifact_semantics_sha256=artifact[3],
                    ),
                )
            )
            result[key] = CatalogPublication(
                publication_id=identity.publication_id(gid).decode("ascii"),
                gid=gid,
                title=loader.text(
                    row[8], domain=b"display_title_utf8_v1", field="display title"
                ),
                source_title=loader.text(
                    row[6], domain=b"source_title_utf8_v1", field="source title"
                ),
                sort_title=loader.text(
                    row[9], domain=b"title_sort_utf8_v1", field="sort title"
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
                source_gallery_name=source_gallery_name,
                contributors=contributors.get(key, ()),
                subjects=subjects.get(key, ()),
                artifacts=artifacts,
                content_sha256=content_sha256,
            )
        return result

    @staticmethod
    def _contributors_for_publications(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[bytes, tuple[CatalogContributor, ...]]:
        selected_cte = _selected_keys_cte(len(publication_keys))
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}), "
            "family_keys(revision, publication_key, position) AS ("
            "SELECT a.revision, a.publication_key, a.position "
            "FROM catalog_contributor_anchors AS a JOIN selected AS chosen "
            "ON chosen.publication_key = a.publication_key WHERE a.revision = %s "
            "UNION SELECT n.revision, n.publication_key, n.position "
            "FROM catalog_contributor_name_sha256s AS n JOIN selected AS chosen "
            "ON chosen.publication_key = n.publication_key WHERE n.revision = %s "
            "UNION SELECT r.revision, r.publication_key, r.position "
            "FROM catalog_contributor_roles AS r JOIN selected AS chosen "
            "ON chosen.publication_key = r.publication_key WHERE r.revision = %s "
            "UNION SELECT i.revision, i.publication_key, i.position "
            "FROM catalog_contributor_identities AS i JOIN selected AS chosen "
            "ON chosen.publication_key = i.publication_key WHERE i.revision = %s "
            "UNION SELECT s.revision, s.publication_key, s.position "
            "FROM catalog_contributor_seals AS s JOIN selected AS chosen "
            "ON chosen.publication_key = s.publication_key WHERE s.revision = %s) "
            "SELECT family.publication_key, family.position, a.position, "
            "n.contributor_name_sha256, r.role, i.position, s.position, roles.role "
            "FROM family_keys AS family "
            "LEFT JOIN catalog_contributor_anchors AS a "
            "ON a.revision = family.revision AND a.publication_key = family.publication_key "
            "AND a.position = family.position "
            "LEFT JOIN catalog_contributor_name_sha256s AS n "
            "ON n.revision = family.revision AND n.publication_key = family.publication_key "
            "AND n.position = family.position "
            "LEFT JOIN catalog_contributor_roles AS r "
            "ON r.revision = family.revision AND r.publication_key = family.publication_key "
            "AND r.position = family.position "
            "LEFT JOIN catalog_contributor_identities AS i "
            "ON i.revision = family.revision AND i.publication_key = family.publication_key "
            "AND i.position = family.position "
            "AND i.contributor_name_sha256 = n.contributor_name_sha256 "
            "AND i.role = r.role "
            "LEFT JOIN catalog_contributor_seals AS s "
            "ON s.revision = family.revision AND s.publication_key = family.publication_key "
            "AND s.position = family.position "
            "LEFT JOIN catalog_contributor_role_registry AS roles ON roles.role = r.role "
            "ORDER BY family.publication_key, family.position",
            (*publication_keys, revision, revision, revision, revision, revision),
        )
        grouped: dict[bytes, list[CatalogContributor]] = {}
        for row in rows:
            if len(row) != 8 or any(value is None for value in row[2:]):
                raise VNextCatalogReadError(
                    "contributor family is partial or noncongruent"
                )
            key = require_digest32(row[0], field="publication_key")
            position = require_int63(row[1], field="contributor position")
            if any(
                require_int63(value, field="contributor family position") != position
                for value in (row[2], row[5], row[6])
            ):
                raise VNextCatalogReadError("contributor family keys disagree")
            current = grouped.setdefault(key, [])
            if position != len(current):
                raise VNextCatalogReadError("contributor positions are not contiguous")
            name = loader.text(
                row[3],
                domain=b"contributor_name_utf8_v1",
                field="contributor name",
            )
            role_bytes = require_ascii_bytes(
                row[4], field="contributor role", minimum=1, maximum=64
            )
            if (
                require_ascii_bytes(
                    row[7], field="registered contributor role", minimum=1, maximum=64
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
            "tag_seal.tag_id, t.namespace, t.tag_value_sha256 "
            "FROM catalog_subjects AS s "
            "LEFT JOIN catalog_tag_term_seals AS tag_seal ON tag_seal.tag_id = s.tag_id "
            "LEFT JOIN catalog_tag_term_identities AS t "
            "ON t.tag_id = tag_seal.tag_id "
            f"WHERE s.revision = %s AND s.publication_key IN ({placeholders}) "
            "ORDER BY s.publication_key, s.position",
            (revision, *publication_keys),
        )
        grouped: dict[bytes, list[CatalogSubject]] = {}
        for row in rows:
            if len(row) != 6 or any(value is None for value in row[2:]):
                raise VNextCatalogReadError("subject row lacks its sealed tag identity")
            key = require_digest32(row[0], field="publication_key")
            position = require_int63(row[1], field="subject position")
            if require_positive_int63(
                row[2], field="subject tag_id"
            ) != require_positive_int63(row[3], field="sealed subject tag_id"):
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

    @staticmethod
    def _artifact_facts_for_publications(
        connector: SQLConnector,
        *,
        revision: int,
        publication_keys: tuple[bytes, ...],
    ) -> dict[bytes, tuple[bytes, int, bytes, bytes]]:
        selected_cte = _selected_keys_cte(len(publication_keys))
        rows = connector.fetch_all(
            f"WITH selected(publication_key) AS ({selected_cte}), "
            "family_keys(revision, publication_key) AS ("
            "SELECT a.revision, a.publication_key FROM catalog_artifact_anchors AS a "
            "JOIN selected AS chosen ON chosen.publication_key = a.publication_key "
            "WHERE a.revision = %s "
            "UNION SELECT d.revision, d.publication_key FROM catalog_artifact_sha256s AS d "
            "JOIN selected AS chosen ON chosen.publication_key = d.publication_key "
            "WHERE d.revision = %s "
            "UNION SELECT m.revision, m.publication_key "
            "FROM catalog_artifact_semantics_sha256s AS m "
            "JOIN selected AS chosen ON chosen.publication_key = m.publication_key "
            "WHERE m.revision = %s "
            "UNION SELECT s.revision, s.publication_key FROM catalog_artifact_seals AS s "
            "JOIN selected AS chosen ON chosen.publication_key = s.publication_key "
            "WHERE s.revision = %s) "
            "SELECT family.publication_key, a.publication_key, d.artifact_sha256, "
            "m.artifact_semantics_sha256, s.publication_key, artifact_blob.size_bytes, "
            "location.artifact_locator_sha256 "
            "FROM family_keys AS family "
            "LEFT JOIN catalog_artifact_anchors AS a "
            "ON a.revision = family.revision AND a.publication_key = family.publication_key "
            "LEFT JOIN catalog_artifact_sha256s AS d "
            "ON d.revision = family.revision AND d.publication_key = family.publication_key "
            "LEFT JOIN catalog_artifact_semantics_sha256s AS m "
            "ON m.revision = family.revision AND m.publication_key = family.publication_key "
            "LEFT JOIN catalog_artifact_seals AS s "
            "ON s.revision = family.revision AND s.publication_key = family.publication_key "
            "LEFT JOIN catalog_artifact_blobs AS artifact_blob "
            "ON artifact_blob.artifact_sha256 = d.artifact_sha256 "
            "LEFT JOIN catalog_artifact_location AS location "
            "ON location.artifact_sha256 = d.artifact_sha256 "
            "ORDER BY family.publication_key",
            (*publication_keys, revision, revision, revision, revision),
        )
        result: dict[bytes, tuple[bytes, int, bytes, bytes]] = {}
        for row in rows:
            if len(row) != 7 or any(value is None for value in row[1:]):
                raise VNextCatalogReadError(
                    "catalog artifact family is partial or lacks storage facts"
                )
            key = require_digest32(row[0], field="artifact publication_key")
            if key in result or any(
                require_digest32(value, field="artifact family publication_key") != key
                for value in (row[1], row[4])
            ):
                raise VNextCatalogReadError("catalog artifact family keys disagree")
            result[key] = (
                require_digest32(row[2], field="artifact_sha256"),
                require_int63(row[5], field="artifact size_bytes"),
                require_digest32(row[6], field="artifact_locator_sha256"),
                require_digest32(row[3], field="artifact_semantics_sha256"),
            )
        return result

    @staticmethod
    def _hydrate_artifact(
        connector: SQLConnector,
        loader: _CanonicalLoader,
        *,
        revision: int,
        publication_key: bytes,
        expected_gid: int | None = None,
        expected_artifact_sha256: bytes | None = None,
    ) -> CatalogArtifact | None:
        key = require_digest32(publication_key, field="publication_key")
        facts = VNextCatalogReaderRepository._artifact_facts_for_publications(
            connector,
            revision=revision,
            publication_keys=(key,),
        ).get(key)
        if facts is None:
            return None
        if expected_artifact_sha256 is not None and facts[0] != require_digest32(
            expected_artifact_sha256,
            field="expected artifact_sha256",
        ):
            return None
        row = connector.fetch_one(
            "SELECT identity.gid, modified.modified_at "
            "FROM catalog_publication_order AS ordering "
            "JOIN catalog_publication_anchors AS anchor "
            "ON anchor.revision = ordering.revision "
            "AND anchor.publication_key = ordering.publication_key "
            "JOIN catalog_publication_modified_ats AS modified "
            "ON modified.revision = anchor.revision "
            "AND modified.publication_key = anchor.publication_key "
            "JOIN catalog_publication_seals AS seal "
            "ON seal.revision = anchor.revision "
            "AND seal.publication_key = anchor.publication_key "
            "JOIN catalog_publication_identities AS identity "
            "ON identity.publication_key = anchor.publication_key "
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
        return _catalog_artifact_from_facts(
            loader,
            publication_key=key,
            gid=gid,
            modified_at=_datetime_from_microseconds(
                row[1], field="artifact modified_at"
            ),
            artifact_sha256=facts[0],
            size_bytes=facts[1],
            locator_sha256=facts[2],
            artifact_semantics_sha256=facts[3],
        )


def _catalog_artifact_from_facts(
    loader: _CanonicalLoader,
    *,
    publication_key: bytes,
    gid: int,
    modified_at: datetime,
    artifact_sha256: bytes,
    size_bytes: int,
    locator_sha256: bytes,
    artifact_semantics_sha256: bytes,
) -> CatalogArtifact:
    exact_key = require_digest32(publication_key, field="publication_key")
    exact_gid = require_positive_int63(gid, field="publication GID")
    digest = require_digest32(artifact_sha256, field="artifact_sha256")
    if identity.publication_key(exact_gid) != exact_key:
        raise VNextCatalogReadError(
            "artifact publication key disagrees with its immutable GID"
        )
    require_digest32(
        artifact_semantics_sha256,
        field="artifact_semantics_sha256",
    )
    name_bytes = identity.artifact_name(exact_gid)
    name = name_bytes.decode("ascii")
    locator_payload = loader.load(
        require_digest32(locator_sha256, field="artifact_locator_sha256"),
        domain=b"artifact_locator_bytes_v1",
    )
    if len(locator_payload) > 4096:
        raise VNextCatalogReadError("artifact locator exceeds its v1 bound")
    try:
        components = identity.decode_artifact_locator(locator_payload)
    except ValueError as error:
        raise VNextCatalogReadError("artifact locator framing is invalid") from error
    if not components or components != identity.artifact_locator_components(digest):
        raise VNextCatalogReadError(
            "artifact locator disagrees with its content-addressed identity"
        )
    relative = PurePosixPath(*components)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise VNextCatalogReadError("artifact locator is not a safe relative path")
    return CatalogArtifact(
        artifact_id=identity.artifact_id(exact_gid, digest).decode("ascii"),
        name=name,
        location=Path(*components),
        media_type=_CBZ_MEDIA_TYPE,
        size_bytes=require_int63(size_bytes, field="artifact size_bytes"),
        sha256=digest.hex(),
        modified_at=modified_at,
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


def _selected_keys_cte(count: int) -> str:
    if count <= 0:
        raise ValueError("selected-key count must be positive")
    return " UNION ALL ".join(
        ("SELECT %s AS publication_key", *("SELECT %s" for _ in range(count - 1)))
    )
