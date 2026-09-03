"""Exact physical protocols for publication-owned immutable facts.

Publication candidates and contributor occurrences are stored as atomic BCNF
rows. Each catalog occurrence is split losslessly into a collision-checked
revision/publication identity and one complete gallery/scalar/title payload.
The historical publication and title row shapes are read-only projections.
"""

from __future__ import annotations

__all__ = [
    "CatalogContributorFamily",
    "CatalogPublicationDownloadTimeFamily",
    "CatalogPublicationFamily",
    "CatalogPublicationTitleFamily",
    "PublicationCandidateFamily",
    "PublicationFamilyCollisionError",
    "PublicationFamilyPartialError",
    "PublicationIdentityFamily",
    "PublicationSelectionFamily",
    "ensure_catalog_contributor_family",
    "ensure_catalog_publication_download_time_family",
    "ensure_catalog_publication_family",
    "ensure_catalog_publication_title_family",
    "ensure_publication_candidate_family",
    "ensure_publication_identity_family",
    "ensure_publication_selection_family",
    "load_catalog_contributor_family",
    "load_catalog_publication_download_time_family",
    "load_catalog_publication_family",
    "load_catalog_publication_title_family",
    "load_publication_candidate_family",
    "load_publication_identity_family",
    "load_publication_selection_family",
]

from dataclasses import dataclass
from typing import Any

from . import vnext_identity as identity
from .sql_connector import DatabaseDuplicateKeyError
from .vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_state_machine_contract import require_catalog_state_mutation

_CANDIDATE = "catalog_publication_candidates"

_PUBLICATION_IDENTITY = "catalog_publication_identities"

_SELECTION_OCCURRENCE_IDENTITY = "catalog_publication_selection_occurrence_identities"
_SELECTION_STORAGE = "catalog_publication_selection_storage"

_PUBLICATION_OCCURRENCE_IDENTITY = "catalog_publication_occurrence_identities"
_PUBLICATION_STORAGE = "catalog_publication_storage"
_PUBLICATION_DOWNLOAD_TIME = "catalog_publication_download_times"
_PUBLICATION = "catalog_publications"
_TITLE = "catalog_publication_titles"

_CONTRIBUTOR = "catalog_contributors"


class PublicationFamilyCollisionError(RuntimeError):
    """A complete immutable family disagrees with expected exact facts."""


class PublicationFamilyPartialError(PublicationFamilyCollisionError):
    """At least one physical member exists without one complete family."""


@dataclass(frozen=True, slots=True)
class PublicationCandidateFamily:
    candidate_id: bytes
    analysis_id: bytes
    reserved_revision: int
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    created_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication candidate_id")
        require_uuid16(self.analysis_id, field="publication candidate analysis_id")
        require_positive_int63(
            self.reserved_revision,
            field="publication candidate reserved_revision",
        )
        require_positive_int63(
            self.artifact_policy_id,
            field="publication candidate artifact_policy_id",
        )
        require_positive_int63(
            self.display_title_policy_id,
            field="publication candidate display_title_policy_id",
        )
        if type(self.artifacts_required) is not bool:
            raise TypeError("publication candidate artifacts_required must be bool")
        require_int63(self.created_at, field="publication candidate created_at")


@dataclass(frozen=True, slots=True)
class PublicationIdentityFamily:
    publication_key: bytes
    gid: int

    def __post_init__(self) -> None:
        publication = require_digest32(
            self.publication_key,
            field="publication identity publication_key",
        )
        gallery_gid = require_positive_int63(self.gid, field="publication identity gid")
        if identity.publication_key(gallery_gid) != publication:
            raise ValueError("publication identity key does not match its GID frame")


@dataclass(frozen=True, slots=True)
class PublicationSelectionFamily:
    candidate_id: bytes
    gallery_id: int
    publication_key: bytes

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication selection candidate_id")
        require_positive_int63(
            self.gallery_id,
            field="publication selection gallery_id",
        )
        require_digest32(
            self.publication_key,
            field="publication selection publication_key",
        )


@dataclass(frozen=True, slots=True)
class CatalogPublicationFamily:
    revision: int
    publication_key: bytes
    gallery_id: int
    summary_sha256: bytes
    language_sha256: bytes
    modified_at: int
    source_title_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog publication revision")
        require_digest32(
            self.publication_key,
            field="catalog publication publication_key",
        )
        require_positive_int63(
            self.gallery_id,
            field="catalog publication gallery_id",
        )
        require_digest32(self.summary_sha256, field="catalog publication summary")
        require_digest32(self.language_sha256, field="catalog publication language")
        require_int63(self.modified_at, field="catalog publication modified_at")
        require_digest32(
            self.source_title_sha256,
            field="catalog publication source_title_sha256",
        )


@dataclass(frozen=True, slots=True)
class CatalogPublicationDownloadTimeFamily:
    revision: int
    publication_key: bytes
    download_time: int

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog download-time revision")
        require_digest32(
            self.publication_key,
            field="catalog download-time publication_key",
        )
        require_int63(
            self.download_time,
            field="catalog publication download_time",
        )


@dataclass(frozen=True, slots=True)
class CatalogPublicationTitleFamily:
    revision: int
    publication_key: bytes
    source_title_sha256: bytes
    source_gallery_name: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog title revision")
        require_digest32(
            self.publication_key,
            field="catalog title publication_key",
        )
        require_digest32(
            self.source_title_sha256,
            field="catalog title source_title_sha256",
        )
        require_bounded_bytes(
            self.source_gallery_name,
            field="catalog title source_gallery_name",
            minimum=1,
            maximum=255,
        )


@dataclass(frozen=True, slots=True)
class CatalogContributorFamily:
    revision: int
    publication_key: bytes
    position: int
    contributor_name_sha256: bytes
    role: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog contributor revision")
        require_digest32(
            self.publication_key,
            field="catalog contributor publication_key",
        )
        require_int63(self.position, field="catalog contributor position")
        require_digest32(
            self.contributor_name_sha256,
            field="catalog contributor name",
        )
        require_bounded_bytes(
            self.role,
            field="catalog contributor role",
            minimum=1,
            maximum=64,
        )


def _locking_suffix(*, backend: str, locking: bool) -> str:
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError("publication family backend is not registered")
    return " FOR UPDATE" if backend == "mariadb" and locking else ""


def _require_gallery_publication_key(
    connector: Any,
    *,
    gallery_id: int,
    publication_key: bytes,
) -> None:
    """Prove the immutable gallery identity chain reaches the expected key."""

    rows = connector.fetch_all(
        "SELECT identity.publication_key "
        "FROM catalog_gallery_source_name_accesses AS access "
        "JOIN catalog_source_gallery_name_gids AS name_gid "
        "ON name_gid.source_gallery_name = access.source_gallery_name "
        "JOIN catalog_publication_identities AS identity "
        "ON identity.gid = name_gid.gid "
        "WHERE access.gallery_id = %s LIMIT 2",
        (gallery_id,),
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise PublicationFamilyPartialError(
            "gallery has no complete immutable publication identity chain"
        )
    if rows[0][0] != publication_key:
        raise PublicationFamilyCollisionError(
            "gallery derives a different publication_key"
        )


def _candidate_family_row(
    connector: Any,
    candidate_id: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    row = connector.fetch_one(
        "SELECT candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, created_at "
        f"FROM {_CANDIDATE} WHERE candidate_id = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate_id,),
    )
    return tuple(row)


def load_publication_candidate_family(
    connector: Any,
    *,
    candidate_id: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> PublicationCandidateFamily | None:
    candidate = require_uuid16(candidate_id, field="publication candidate_id")
    row = _candidate_family_row(
        connector,
        candidate,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    if len(row) != 7 or row[0] != candidate:
        raise PublicationFamilyPartialError("publication candidate row is malformed")
    if row[5] not in {0, 1}:
        raise PublicationFamilyCollisionError(
            "publication candidate artifacts_required is not boolean"
        )
    try:
        return PublicationCandidateFamily(
            candidate,
            row[1],
            row[2],
            row[3],
            row[4],
            bool(row[5]),
            row[6],
        )
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "publication candidate family contains invalid facts"
        ) from error


def ensure_publication_candidate_family(
    connector: Any,
    family: PublicationCandidateFamily,
    *,
    backend: str = "sqlite",
) -> tuple[PublicationCandidateFamily, bool]:
    if not isinstance(family, PublicationCandidateFamily):
        raise TypeError("family must be PublicationCandidateFamily")
    _locking_suffix(backend=backend, locking=False)
    existing = load_publication_candidate_family(
        connector,
        candidate_id=family.candidate_id,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise PublicationFamilyCollisionError(
                "publication candidate replay changed exact facts"
            )
        return existing, False
    candidate = family.candidate_id
    require_catalog_state_mutation(
        "publication-candidate.initialize",
        previous_state=None,
        next_state="OPEN",
        timestamp=None,
    )
    try:
        connector.execute(
            f"INSERT INTO {_CANDIDATE} "
            "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
            "display_title_policy_id, artifacts_required, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                candidate,
                family.analysis_id,
                family.reserved_revision,
                family.artifact_policy_id,
                family.display_title_policy_id,
                int(family.artifacts_required),
                family.created_at,
            ),
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_publication_candidate_family(
                connector,
                candidate_id=candidate,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "publication candidate concurrent replay left partial facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "publication candidate concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True


def load_publication_identity_family(
    connector: Any,
    *,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> PublicationIdentityFamily | None:
    publication = require_digest32(
        publication_key,
        field="publication identity publication_key",
    )
    row = connector.fetch_one(
        f"SELECT publication_key, gid FROM {_PUBLICATION_IDENTITY} "
        "WHERE publication_key = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (publication,),
    )
    if not row:
        return None
    if len(row) != 2 or row[0] != publication:
        raise PublicationFamilyCollisionError("publication identity row is malformed")
    try:
        return PublicationIdentityFamily(publication, row[1])
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "publication identity contains invalid facts"
        ) from error


def ensure_publication_identity_family(
    connector: Any,
    family: PublicationIdentityFamily,
    *,
    backend: str = "sqlite",
) -> tuple[PublicationIdentityFamily, bool]:
    if not isinstance(family, PublicationIdentityFamily):
        raise TypeError("family must be PublicationIdentityFamily")
    _locking_suffix(backend=backend, locking=False)
    rows = connector.fetch_all(
        f"SELECT publication_key, gid FROM {_PUBLICATION_IDENTITY} "
        "WHERE publication_key = %s OR gid = %s LIMIT 2",
        (family.publication_key, family.gid),
    )
    if rows:
        if tuple(rows) != ((family.publication_key, family.gid),):
            raise PublicationFamilyCollisionError(
                "publication identity collides with another key/GID pair"
            )
        return family, False
    try:
        connector.execute(
            f"INSERT INTO {_PUBLICATION_IDENTITY} (publication_key, gid) "
            "VALUES (%s, %s)",
            (family.publication_key, family.gid),
        )
    except DatabaseDuplicateKeyError as error:
        rows = connector.fetch_all(
            f"SELECT publication_key, gid FROM {_PUBLICATION_IDENTITY} "
            "WHERE publication_key = %s OR gid = %s LIMIT 2"
            + _locking_suffix(backend=backend, locking=True),
            (family.publication_key, family.gid),
        )
        if tuple(rows) != ((family.publication_key, family.gid),):
            raise PublicationFamilyCollisionError(
                "publication identity concurrent replay changed exact facts"
            ) from error
        return family, False
    return family, True


def load_publication_selection_family(
    connector: Any,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> PublicationSelectionFamily | None:
    candidate = require_uuid16(candidate_id, field="publication selection candidate_id")
    publication = require_digest32(
        publication_key,
        field="publication selection publication_key",
    )
    occurrence = identity.publication_selection_occurrence_sha256(
        candidate, publication
    )
    rows = connector.fetch_all(
        "SELECT occurrence.selection_occurrence_sha256, occurrence.candidate_id, "
        "occurrence.publication_key, stored.selection_occurrence_sha256, "
        "stored.gallery_id, derived.publication_key "
        f"FROM {_SELECTION_OCCURRENCE_IDENTITY} AS occurrence "
        f"LEFT JOIN {_SELECTION_STORAGE} AS stored "
        "ON stored.selection_occurrence_sha256 = "
        "occurrence.selection_occurrence_sha256 "
        "LEFT JOIN catalog_gallery_source_name_accesses AS access "
        "ON access.gallery_id = stored.gallery_id "
        "LEFT JOIN catalog_source_gallery_name_gids AS name_gid "
        "ON name_gid.source_gallery_name = access.source_gallery_name "
        "LEFT JOIN catalog_publication_identities AS derived "
        "ON derived.gid = name_gid.gid "
        "WHERE occurrence.selection_occurrence_sha256 = %s OR "
        "(occurrence.candidate_id = %s AND occurrence.publication_key = %s) LIMIT 2"
        + _locking_suffix(backend=backend, locking=locking),
        (occurrence, candidate, publication),
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise PublicationFamilyCollisionError(
            "publication selection occurrence resolves to multiple rows"
        )
    row = tuple(rows[0])
    if len(row) != 6 or tuple(row[:4]) != (
        occurrence,
        candidate,
        publication,
        occurrence,
    ):
        if len(row) == 6 and tuple(row[:3]) == (
            occurrence,
            candidate,
            publication,
        ):
            raise PublicationFamilyPartialError(
                "publication selection occurrence has no gallery payload"
            )
        raise PublicationFamilyCollisionError(
            "publication selection occurrence has an invalid shape or collision"
        )
    if row[5] != publication:
        raise PublicationFamilyCollisionError(
            "publication selection gallery derives a different publication_key"
        )
    try:
        return PublicationSelectionFamily(candidate, row[4], publication)
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "publication selection occurrence contains invalid facts"
        ) from error


def ensure_publication_selection_family(
    connector: Any,
    family: PublicationSelectionFamily,
    *,
    backend: str = "sqlite",
) -> tuple[PublicationSelectionFamily, bool]:
    if not isinstance(family, PublicationSelectionFamily):
        raise TypeError("family must be PublicationSelectionFamily")
    _locking_suffix(backend=backend, locking=False)
    _require_gallery_publication_key(
        connector,
        gallery_id=family.gallery_id,
        publication_key=family.publication_key,
    )
    existing = load_publication_selection_family(
        connector,
        candidate_id=family.candidate_id,
        publication_key=family.publication_key,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise PublicationFamilyCollisionError(
                "publication selection replay changed exact facts"
            )
        return existing, False
    occurrence = identity.publication_selection_occurrence_sha256(
        family.candidate_id, family.publication_key
    )
    try:
        connector.execute(
            f"INSERT INTO {_SELECTION_OCCURRENCE_IDENTITY} "
            "(selection_occurrence_sha256, candidate_id, publication_key) "
            "VALUES (%s, %s, %s)",
            (occurrence, family.candidate_id, family.publication_key),
        )
        connector.execute(
            f"INSERT INTO {_SELECTION_STORAGE} "
            "(selection_occurrence_sha256, gallery_id) VALUES (%s, %s)",
            (occurrence, family.gallery_id),
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_publication_selection_family(
                connector,
                candidate_id=family.candidate_id,
                publication_key=family.publication_key,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "publication selection concurrent replay left partial facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "publication selection concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True


def _publication_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    occurrence = identity.catalog_publication_occurrence_sha256(
        revision, publication_key
    )
    rows = connector.fetch_all(
        "SELECT occurrence.catalog_occurrence_sha256, occurrence.revision, "
        "occurrence.publication_key, stored.catalog_occurrence_sha256, "
        "stored.gallery_id, stored.summary_sha256, stored.language_sha256, "
        "stored.modified_at, stored.source_title_sha256, derived.publication_key "
        f"FROM {_PUBLICATION_OCCURRENCE_IDENTITY} AS occurrence "
        f"LEFT JOIN {_PUBLICATION_STORAGE} AS stored "
        "ON stored.catalog_occurrence_sha256 = occurrence.catalog_occurrence_sha256 "
        "LEFT JOIN catalog_gallery_source_name_accesses AS access "
        "ON access.gallery_id = stored.gallery_id "
        "LEFT JOIN catalog_source_gallery_name_gids AS name_gid "
        "ON name_gid.source_gallery_name = access.source_gallery_name "
        "LEFT JOIN catalog_publication_identities AS derived "
        "ON derived.gid = name_gid.gid "
        "WHERE occurrence.catalog_occurrence_sha256 = %s OR "
        "(occurrence.revision = %s AND occurrence.publication_key = %s) LIMIT 2"
        + _locking_suffix(backend=backend, locking=locking),
        (occurrence, revision, publication_key),
    )
    if not rows:
        return ()
    if len(rows) != 1:
        raise PublicationFamilyCollisionError(
            "catalog occurrence identity resolves to multiple rows"
        )
    return tuple(rows[0])


def load_catalog_publication_family(
    connector: Any,
    *,
    revision: int,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> CatalogPublicationFamily | None:
    catalog_revision = require_positive_int63(revision, field="catalog revision")
    publication = require_digest32(publication_key, field="catalog publication_key")
    row = _publication_family_row(
        connector,
        catalog_revision,
        publication,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    occurrence = identity.catalog_publication_occurrence_sha256(
        catalog_revision, publication
    )
    if len(row) != 10 or tuple(row[:4]) != (
        occurrence,
        catalog_revision,
        publication,
        occurrence,
    ):
        if len(row) == 10 and tuple(row[:3]) == (
            occurrence,
            catalog_revision,
            publication,
        ):
            raise PublicationFamilyPartialError(
                "catalog occurrence identity has no complete payload"
            )
        raise PublicationFamilyCollisionError(
            "catalog occurrence identity has an invalid shape or collision"
        )
    if row[9] != publication:
        raise PublicationFamilyCollisionError(
            "catalog occurrence gallery derives a different publication_key"
        )
    try:
        return CatalogPublicationFamily(
            catalog_revision,
            publication,
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
        )
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "catalog publication family contains invalid facts"
        ) from error


def ensure_catalog_publication_family(
    connector: Any,
    family: CatalogPublicationFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogPublicationFamily, bool]:
    if not isinstance(family, CatalogPublicationFamily):
        raise TypeError("family must be CatalogPublicationFamily")
    _locking_suffix(backend=backend, locking=False)
    _require_gallery_publication_key(
        connector,
        gallery_id=family.gallery_id,
        publication_key=family.publication_key,
    )
    existing = load_catalog_publication_family(
        connector,
        revision=family.revision,
        publication_key=family.publication_key,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise PublicationFamilyCollisionError(
                "catalog publication replay changed exact facts"
            )
        return existing, False
    occurrence = identity.catalog_publication_occurrence_sha256(
        family.revision, family.publication_key
    )
    try:
        connector.execute(
            f"INSERT INTO {_PUBLICATION_OCCURRENCE_IDENTITY} "
            "(catalog_occurrence_sha256, revision, publication_key) "
            "VALUES (%s, %s, %s)",
            (occurrence, family.revision, family.publication_key),
        )
        connector.execute(
            f"INSERT INTO {_PUBLICATION_STORAGE} "
            "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
            "language_sha256, modified_at, source_title_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                occurrence,
                family.gallery_id,
                family.summary_sha256,
                family.language_sha256,
                family.modified_at,
                family.source_title_sha256,
            ),
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_catalog_publication_family(
                connector,
                revision=family.revision,
                publication_key=family.publication_key,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "catalog publication concurrent replay did not leave the exact row"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog publication concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True


def _download_time_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    occurrence = identity.catalog_publication_occurrence_sha256(
        revision, publication_key
    )
    rows = connector.fetch_all(
        "SELECT occurrence.catalog_occurrence_sha256, occurrence.revision, "
        "occurrence.publication_key, downloaded.download_time "
        f"FROM {_PUBLICATION_OCCURRENCE_IDENTITY} AS occurrence "
        f"LEFT JOIN {_PUBLICATION_DOWNLOAD_TIME} AS downloaded "
        "ON downloaded.catalog_occurrence_sha256 = "
        "occurrence.catalog_occurrence_sha256 "
        "WHERE occurrence.catalog_occurrence_sha256 = %s OR "
        "(occurrence.revision = %s AND occurrence.publication_key = %s) LIMIT 2"
        + _locking_suffix(backend=backend, locking=locking),
        (occurrence, revision, publication_key),
    )
    if not rows:
        return ()
    if len(rows) != 1:
        raise PublicationFamilyCollisionError(
            "catalog download-time occurrence resolves to multiple rows"
        )
    return tuple(rows[0])


def load_catalog_publication_download_time_family(
    connector: Any,
    *,
    revision: int,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> CatalogPublicationDownloadTimeFamily | None:
    catalog_revision = require_positive_int63(revision, field="catalog revision")
    publication = require_digest32(publication_key, field="catalog publication_key")
    row = _download_time_family_row(
        connector,
        catalog_revision,
        publication,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    occurrence = identity.catalog_publication_occurrence_sha256(
        catalog_revision, publication
    )
    if len(row) != 4 or tuple(row[:3]) != (
        occurrence,
        catalog_revision,
        publication,
    ):
        raise PublicationFamilyCollisionError(
            "catalog download-time occurrence has an invalid shape or collision"
        )
    if row[3] is None:
        raise PublicationFamilyPartialError(
            "catalog publication occurrence has no download-time fact"
        )
    try:
        return CatalogPublicationDownloadTimeFamily(
            catalog_revision,
            publication,
            row[3],
        )
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "catalog publication download time contains invalid facts"
        ) from error


def ensure_catalog_publication_download_time_family(
    connector: Any,
    family: CatalogPublicationDownloadTimeFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogPublicationDownloadTimeFamily, bool]:
    if not isinstance(family, CatalogPublicationDownloadTimeFamily):
        raise TypeError("family must be CatalogPublicationDownloadTimeFamily")
    row = _download_time_family_row(
        connector,
        family.revision,
        family.publication_key,
        backend=backend,
        locking=False,
    )
    if not row:
        raise PublicationFamilyPartialError(
            "catalog download time has no publication occurrence identity"
        )
    occurrence = identity.catalog_publication_occurrence_sha256(
        family.revision, family.publication_key
    )
    if len(row) != 4 or tuple(row[:3]) != (
        occurrence,
        family.revision,
        family.publication_key,
    ):
        raise PublicationFamilyCollisionError(
            "catalog download-time occurrence has an invalid shape or collision"
        )
    if row[3] is not None:
        existing = load_catalog_publication_download_time_family(
            connector,
            revision=family.revision,
            publication_key=family.publication_key,
            backend=backend,
        )
        if existing != family:
            raise PublicationFamilyCollisionError(
                "catalog download-time replay changed exact facts"
            )
        return family, False
    try:
        connector.execute(
            f"INSERT INTO {_PUBLICATION_DOWNLOAD_TIME} "
            "(catalog_occurrence_sha256, download_time) VALUES (%s, %s)",
            (occurrence, family.download_time),
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_catalog_publication_download_time_family(
                connector,
                revision=family.revision,
                publication_key=family.publication_key,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "catalog download-time concurrent replay left conflicting facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog download-time concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True


def _title_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    row = connector.fetch_one(
        "SELECT revision, publication_key, source_title_sha256, "
        f"source_gallery_name FROM {_TITLE} "
        "WHERE revision = %s AND publication_key = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key),
    )
    return tuple(row)


def load_catalog_publication_title_family(
    connector: Any,
    *,
    revision: int,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> CatalogPublicationTitleFamily | None:
    catalog_revision = require_positive_int63(revision, field="catalog revision")
    publication = require_digest32(publication_key, field="catalog publication_key")
    row = _title_family_row(
        connector,
        catalog_revision,
        publication,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    if len(row) != 4 or (row[0], row[1]) != (catalog_revision, publication):
        raise PublicationFamilyCollisionError(
            "catalog title row has an invalid shape or key"
        )
    try:
        return CatalogPublicationTitleFamily(
            catalog_revision,
            publication,
            row[2],
            row[3],
        )
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "catalog title family contains invalid facts"
        ) from error


def ensure_catalog_publication_title_family(
    connector: Any,
    family: CatalogPublicationTitleFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogPublicationTitleFamily, bool]:
    if not isinstance(family, CatalogPublicationTitleFamily):
        raise TypeError("family must be CatalogPublicationTitleFamily")
    _locking_suffix(backend=backend, locking=False)
    existing = load_catalog_publication_title_family(
        connector,
        revision=family.revision,
        publication_key=family.publication_key,
        backend=backend,
    )
    if existing is None:
        raise PublicationFamilyPartialError(
            "catalog title projection has no complete occurrence payload"
        )
    if existing != family:
        raise PublicationFamilyCollisionError(
            "catalog title replay changed exact facts"
        )
    return existing, False


def _contributor_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    position: int,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    row = connector.fetch_one(
        "SELECT revision, publication_key, position, "
        f"contributor_name_sha256, role FROM {_CONTRIBUTOR} "
        "WHERE revision = %s AND publication_key = %s AND position = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key, position),
    )
    return tuple(row)


def load_catalog_contributor_family(
    connector: Any,
    *,
    revision: int,
    publication_key: bytes,
    position: int,
    backend: str = "sqlite",
    locking: bool = False,
) -> CatalogContributorFamily | None:
    catalog_revision = require_positive_int63(revision, field="catalog revision")
    publication = require_digest32(publication_key, field="catalog publication_key")
    occurrence = require_int63(position, field="catalog contributor position")
    row = _contributor_family_row(
        connector,
        catalog_revision,
        publication,
        occurrence,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    expected = (catalog_revision, publication, occurrence)
    if len(row) != 5 or tuple(row[:3]) != expected:
        raise PublicationFamilyCollisionError(
            "catalog contributor has an invalid physical shape"
        )
    try:
        return CatalogContributorFamily(
            catalog_revision,
            publication,
            occurrence,
            row[3],
            row[4],
        )
    except (TypeError, ValueError) as error:
        raise PublicationFamilyCollisionError(
            "catalog contributor family contains invalid facts"
        ) from error


def ensure_catalog_contributor_family(
    connector: Any,
    family: CatalogContributorFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogContributorFamily, bool]:
    if not isinstance(family, CatalogContributorFamily):
        raise TypeError("family must be CatalogContributorFamily")
    _locking_suffix(backend=backend, locking=False)
    existing = load_catalog_contributor_family(
        connector,
        revision=family.revision,
        publication_key=family.publication_key,
        position=family.position,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise PublicationFamilyCollisionError(
                "catalog contributor replay changed exact facts"
            )
        return existing, False
    try:
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR} "
            "(revision, publication_key, contributor_name_sha256, role, position) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                family.revision,
                family.publication_key,
                family.contributor_name_sha256,
                family.role,
                family.position,
            ),
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_catalog_contributor_family(
                connector,
                revision=family.revision,
                publication_key=family.publication_key,
                position=family.position,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "catalog contributor concurrent replay left invalid facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog contributor concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True
