"""Read-only publication authority for bounded admission of new source galleries.

A completion-marker cache can outlive an interrupted ingest. Only membership in
the current published source build proves that a gallery was already admitted.
Callers own the short read transaction for each lookup and recheck the head in a
fresh transaction after preparing their complete local source snapshot.
"""

from __future__ import annotations

from .domain import SourceBatchBaseline
from .source_errors import VNextSourceChangedError
from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    load_and_validate_single_page_canonical_values,
    stream_and_validate_canonical_value,
)
from .vnext_catalog_identity_family import GalleryIdentity
from .vnext_catalog_registry_repository import (
    CatalogRegistryConflictError,
    CatalogRegistryNotReadyError,
    load_source_scope,
)
from .vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_identity import (
    decode_source_relative_locator,
    iter_source_relative_locator_payload,
    iter_source_root_payload,
    source_relative_locator_digest,
    source_root_digest,
    source_scope_key,
)
from .vnext_manifest_family import (
    ManifestFamilyCollisionError,
    SourceBuildFamily,
    load_source_build_family,
)
from .vnext_source_build_repository import (
    SourceBuildConflictError,
    _load_finalized_source_publication_by_receipt,
)
from .vnext_source_marker_family import (
    CachedSourceObservation,
    SourceMarkerConflictError,
    _compare_canonical,
    _load_cached_batch,
)

__all__ = [
    "SourceBatchBaseline",
    "SourceBatchChangedError",
    "SourceBatchConflictError",
    "SourceBatchRepository",
]

_CHANNEL = b"default"
_LOCATOR_DOMAIN = b"source_relative_locator_v1"
_MAX_LOCATORS = 128


class SourceBatchConflictError(RuntimeError):
    """The published source baseline contains inconsistent or incomplete facts."""


class SourceBatchChangedError(VNextSourceChangedError):
    """A valid successor publication requires a fresh admission snapshot."""


class SourceBatchRepository:
    @staticmethod
    def load_baseline(
        connector: SQLConnector,
        *,
        source_root_components: tuple[str, ...],
    ) -> SourceBatchBaseline:
        """Pin the published build and the requested root's membership scope.

        Replacing the source root makes every locator new. The previous build
        still owns the head fence; its scope must not be confused with the
        requested root's independently derived scope.
        """

        root = source_root_digest(source_root_components)
        scope = source_scope_key("filesystem", root, 1)
        receipt = _head_receipt(connector)
        if receipt is None:
            return SourceBatchBaseline(None, None, scope)
        build = _published_build(connector, receipt)
        if build.scope_key != scope:
            return SourceBatchBaseline(receipt, build.build_id, scope)
        try:
            durable_scope = load_source_scope(connector, scope)
            if (
                durable_scope.source_provider != b"filesystem"
                or durable_scope.source_root_sha256 != root
                or durable_scope.identity_policy_version != 1
            ):
                raise SourceBatchConflictError("source batch scope differs")
            _compare_canonical(
                connector,
                root,
                b"source_root_v1",
                iter_source_root_payload(source_root_components),
            )
        except (
            CatalogRegistryConflictError,
            CatalogRegistryNotReadyError,
            SourceMarkerConflictError,
        ) as error:
            raise SourceBatchConflictError(str(error)) from error
        return SourceBatchBaseline(receipt, build.build_id, scope)

    @staticmethod
    def lookup_members(
        connector: SQLConnector,
        baseline: SourceBatchBaseline,
        locators: tuple[tuple[str, ...], ...],
    ) -> tuple[bool, ...]:
        """Resolve at most 128 exact locators against the pinned source build."""

        if type(locators) is not tuple or len(locators) > _MAX_LOCATORS:
            raise ValueError("source batch membership accepts at most 128 locators")
        digests = tuple(
            source_relative_locator_digest(_LOCATOR_DOMAIN.decode("ascii"), parts)
            for parts in locators
        )
        SourceBatchRepository.require_current(connector, baseline)
        if not locators or baseline.build_id is None:
            return (False,) * len(locators)
        keys = tuple(dict.fromkeys(digests))
        placeholders = ", ".join("%s" for _ in keys)
        rows = connector.fetch_all(
            "SELECT identity.gallery_id, identity.gallery_key, identity.scope_key, "
            "identity.locator_sha256, member.observation_id "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity "
            "ON identity.gallery_id = member.gallery_id "
            "WHERE member.build_id = %s AND identity.scope_key = %s "
            f"AND identity.locator_sha256 IN ({placeholders}) LIMIT 129",
            (baseline.build_id, baseline.scope_key, *keys),
        )
        members: set[bytes] = set()
        for row in rows:
            if len(row) != 5:
                raise SourceBatchConflictError("source batch member shape differs")
            try:
                identity = GalleryIdentity(*row[:4])
                require_positive_int63(row[4], field="source batch observation_id")
            except (TypeError, ValueError) as error:
                raise SourceBatchConflictError(
                    "source batch member identity differs"
                ) from error
            digest = identity.locator_sha256
            if (
                identity.scope_key != baseline.scope_key
                or digest not in keys
                or digest in members
            ):
                raise SourceBatchConflictError(
                    "source batch member key repeats or differs"
                )
            members.add(digest)
        try:
            payloads = load_and_validate_single_page_canonical_values(
                connector,
                references=tuple(
                    (digest, _LOCATOR_DOMAIN) for digest in sorted(members)
                ),
            )
        except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
            raise SourceBatchConflictError(str(error)) from error
        for digest, parts in zip(digests, locators, strict=True):
            if digest not in members:
                continue
            expected = iter_source_relative_locator_payload(parts)
            payload = payloads.get((digest, _LOCATOR_DOMAIN))
            if payload is None:
                try:
                    _compare_canonical(connector, digest, _LOCATOR_DOMAIN, expected)
                except SourceMarkerConflictError as error:
                    raise SourceBatchConflictError(str(error)) from error
            elif payload != b"".join(expected):
                raise SourceBatchConflictError("source batch locator preimage differs")
        return tuple(digest in members for digest in digests)

    @staticmethod
    def retain_published_observations(
        connector: SQLConnector,
        baseline: SourceBatchBaseline,
        candidates: tuple[CachedSourceObservation | None, ...],
    ) -> tuple[CachedSourceObservation | None, ...]:
        """Require fresh preparation of unpublished artifact source after restart.

        An unpublished source seal does not prove that external artifact inputs
        survived the process that observed it. Only candidates belonging to the
        pinned published source can take the no-observation reuse path.
        """

        if type(candidates) is not tuple or len(candidates) > _MAX_LOCATORS:
            raise ValueError("published reuse accepts at most 128 candidates")
        SourceBatchRepository.require_current(connector, baseline)
        if baseline.build_id is None:
            return (None,) * len(candidates)
        keys = tuple(
            dict.fromkeys(
                candidate.gallery_id
                for candidate in candidates
                if candidate is not None
            )
        )
        if not keys:
            return candidates
        for candidate in candidates:
            if candidate is not None:
                candidate.__post_init__()
        slots = ", ".join("%s" for _key in keys)
        rows = connector.fetch_all(
            "SELECT member.gallery_id, member.observation_id "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity ON identity.gallery_id = member.gallery_id "
            "WHERE member.build_id = %s AND identity.scope_key = %s "
            f"AND member.gallery_id IN ({slots}) LIMIT 129",
            (baseline.build_id, baseline.scope_key, *keys),
        )
        identities = set(rows)
        if len(identities) != len(rows) or any(
            len(row) != 2 or row[0] not in keys for row in rows
        ):
            raise SourceBatchConflictError("published reuse membership differs")
        return tuple(
            candidate
            if candidate is not None
            and (candidate.gallery_id, candidate.observation_id) in identities
            else None
            for candidate in candidates
        )

    @staticmethod
    def list_locators(
        connector: SQLConnector,
        baseline: SourceBatchBaseline,
        *,
        after_gallery_id: int,
    ) -> tuple[tuple[int, tuple[str, ...]], ...]:
        """Page published inventory to independently confirm apparent deletions."""

        after_gallery_id = require_int63(
            after_gallery_id, field="published gallery cursor"
        )
        SourceBatchRepository.require_current(connector, baseline)
        if baseline.build_id is None:
            return ()
        rows = connector.fetch_all(
            "SELECT identity.gallery_id, identity.gallery_key, identity.scope_key, "
            "identity.locator_sha256 FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity ON identity.gallery_id = member.gallery_id "
            "WHERE member.build_id = %s AND identity.scope_key = %s AND identity.gallery_id > %s "
            "ORDER BY identity.gallery_id LIMIT 128",
            (baseline.build_id, baseline.scope_key, after_gallery_id),
        )
        try:
            payloads = load_and_validate_single_page_canonical_values(
                connector,
                references=tuple((row[3], _LOCATOR_DOMAIN) for row in rows),
            )
            result: list[tuple[int, tuple[str, ...]]] = []
            for row in rows:
                identity = GalleryIdentity(*row)
                if (
                    identity.scope_key != baseline.scope_key
                    or identity.gallery_id <= after_gallery_id
                ):
                    raise SourceBatchConflictError(
                        "published locator scope or order differs"
                    )
                digest = identity.locator_sha256
                payload = payloads.get((digest, _LOCATOR_DOMAIN))
                if payload is None:
                    parts: list[bytes] = []
                    receipt = stream_and_validate_canonical_value(
                        connector,
                        value_sha256=digest,
                        consume_provisional=parts.append,
                    )
                    if receipt.digest_domain != _LOCATOR_DOMAIN:
                        raise SourceBatchConflictError(
                            "published locator domain differs"
                        )
                    payload = b"".join(parts)
                locator = decode_source_relative_locator(payload)
                if (
                    source_relative_locator_digest(
                        _LOCATOR_DOMAIN.decode("ascii"), locator
                    )
                    != digest
                ):
                    raise SourceBatchConflictError("published locator preimage differs")
                result.append((identity.gallery_id, locator))
                after_gallery_id = identity.gallery_id
        except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
            raise SourceBatchConflictError(str(error)) from error
        return tuple(result)

    @staticmethod
    def lookup_observations(
        connector: SQLConnector,
        baseline: SourceBatchBaseline,
        locators: tuple[tuple[str, ...], ...],
        *,
        qualification_policy_sha256: bytes,
    ) -> tuple[CachedSourceObservation | None, ...]:
        """Load fallback authority from the published build, never newest cache.

        Each locator and immutable observation is independently revalidated.
        An incompatible qualification policy cannot preserve an old observation
        while claiming it was verified under the replacement policy.
        """

        policy = require_digest32(
            qualification_policy_sha256, field="qualification_policy_sha256"
        )
        membership = SourceBatchRepository.lookup_members(connector, baseline, locators)
        if not any(membership):
            return (None,) * len(locators)
        digests = tuple(
            source_relative_locator_digest(_LOCATOR_DOMAIN.decode("ascii"), locator)
            for locator, known in zip(locators, membership, strict=True)
            if known
        )
        slots = ", ".join("%s" for _digest in digests)
        rows = connector.fetch_all(
            "SELECT identity.locator_sha256, member.gallery_id, member.observation_id, "
            "marker.file_key, qualification.qualification_policy_sha256 "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_identities AS identity ON identity.gallery_id = member.gallery_id "
            "LEFT JOIN catalog_gallery_observation_completion_marker AS marker "
            "ON marker.gallery_id = member.gallery_id AND marker.observation_id = member.observation_id "
            "LEFT JOIN catalog_gallery_observation_validation_policies AS qualification "
            "ON qualification.gallery_id = member.gallery_id AND qualification.observation_id = member.observation_id "
            "WHERE member.build_id = %s AND identity.scope_key = %s "
            f"AND identity.locator_sha256 IN ({slots}) LIMIT 129",
            (baseline.build_id, baseline.scope_key, *digests),
        )
        by_digest = {row[0]: row for row in rows}
        if len(by_digest) != len(rows) or set(by_digest) != set(digests):
            raise SourceBatchConflictError("published fallback membership differs")
        for row in rows:
            if len(row) != 5 or row[4] is None:
                raise SourceBatchConflictError(
                    "published fallback qualification is absent"
                )
            if row[3] is None or row[4] != policy:
                raise SourceBatchChangedError(
                    "deferred published gallery needs a fresh observation under the current completion policy"
                )
        try:
            cached = _load_cached_batch(
                connector,
                bindings=tuple((row[1], row[2], row[3]) for row in rows),
            )
        except SourceMarkerConflictError as error:
            raise SourceBatchConflictError(str(error)) from error
        result: list[CachedSourceObservation | None] = []
        for locator, known in zip(locators, membership, strict=True):
            if not known:
                result.append(None)
                continue
            digest = source_relative_locator_digest(
                _LOCATOR_DOMAIN.decode("ascii"), locator
            )
            row = by_digest[digest]
            result.append(cached[(row[1], row[2])])
        return tuple(result)

    @staticmethod
    def require_current(
        connector: SQLConnector,
        baseline: SourceBatchBaseline,
    ) -> None:
        """Reject changed heads and revalidate the published build's own scope."""

        if type(baseline) is not SourceBatchBaseline:
            raise TypeError("baseline must be a SourceBatchBaseline")
        baseline.__post_init__()
        receipt = _head_receipt(connector)
        if receipt != baseline.receipt_id:
            if receipt is None:
                raise SourceBatchConflictError(
                    "source batch publication head disappeared"
                )
            _published_build(connector, receipt)
            raise SourceBatchChangedError("source batch publication head changed")
        if receipt is not None and (
            _published_build(connector, receipt).build_id != baseline.build_id
        ):
            raise SourceBatchConflictError("source batch published build differs")


def _head_receipt(connector: SQLConnector) -> bytes | None:
    row = connector.fetch_one(
        "SELECT registry.channel, head.receipt_id "
        "FROM catalog_channel_registry AS registry "
        "LEFT JOIN catalog_publication_commit_head_receipts AS head "
        "ON head.channel = registry.channel WHERE registry.channel = %s",
        (_CHANNEL,),
    )
    if len(row) != 2 or row[0] != _CHANNEL:
        raise SourceBatchConflictError("source batch channel registry is incomplete")
    if row[1] is None:
        finalized = connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commit_finalizations AS marker "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = marker.receipt_id "
            "JOIN catalog_source_revision_descriptors AS descriptor "
            "ON descriptor.source_revision = committed.source_revision "
            "WHERE descriptor.channel = %s LIMIT 1",
            (_CHANNEL,),
        )
        if finalized:
            raise SourceBatchConflictError(
                "source batch publication head is absent despite finalized source"
            )
        return None
    try:
        return require_uuid16(row[1], field="source batch head receipt_id")
    except ValueError as error:
        raise SourceBatchConflictError(
            "source batch head identity is invalid"
        ) from error


def _published_build(connector: SQLConnector, receipt: bytes) -> SourceBuildFamily:
    try:
        published = _load_finalized_source_publication_by_receipt(
            connector, receipt_id=receipt, expected_build_id=None
        )
        build = load_source_build_family(connector, build_id=published.build_id)
    except (SourceBuildConflictError, ManifestFamilyCollisionError) as error:
        raise SourceBatchConflictError(str(error)) from error
    if build is None or build.state != "SEALED":
        raise SourceBatchConflictError(
            "source batch published build is absent or unsealed"
        )
    try:
        scope = load_source_scope(connector, build.scope_key)
    except (CatalogRegistryConflictError, CatalogRegistryNotReadyError) as error:
        raise SourceBatchConflictError(str(error)) from error
    if scope.source_provider != b"filesystem" or scope.identity_policy_version != 1:
        raise SourceBatchConflictError("source batch published build scope differs")
    return build
