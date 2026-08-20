"""Exact physical protocols for publication-owned sealed families.

The public candidate, publication, title, and contributor relations are
read-only projections.  This module owns the corresponding narrow physical
write protocols: every member is validated as one exact immutable tuple,
partial families fail closed, and the PK-only completion seal is always the
last insert.  Publication identifiers and artifact names remain derived from
the collision-checked publication-key/GID pair and are never stored here.
"""

from __future__ import annotations

__all__ = [
    "CatalogContributorFamily",
    "CatalogPublicationFamily",
    "CatalogPublicationTitleFamily",
    "PublicationCandidateFamily",
    "PublicationFamilyCollisionError",
    "PublicationFamilyPartialError",
    "PublicationIdentityFamily",
    "ensure_catalog_contributor_family",
    "ensure_catalog_publication_family",
    "ensure_catalog_publication_title_family",
    "ensure_publication_candidate_family",
    "ensure_publication_identity_family",
    "load_catalog_contributor_family",
    "load_catalog_publication_family",
    "load_catalog_publication_title_family",
    "load_publication_candidate_family",
    "load_publication_identity_family",
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

_CANDIDATE_ANCHOR = "catalog_publication_candidate_anchors"
_CANDIDATE_ANALYSIS = "catalog_publication_candidate_analysis_ids"
_CANDIDATE_REVISION = "catalog_publication_candidate_reserved_revisions"
_CANDIDATE_ARTIFACT_POLICY = "catalog_publication_candidate_artifact_policy_ids"
_CANDIDATE_DISPLAY_POLICY = "catalog_publication_candidate_display_title_policy_ids"
_CANDIDATE_ARTIFACTS_REQUIRED = "catalog_publication_candidate_artifacts_required"
_CANDIDATE_CREATED_AT = "catalog_publication_candidate_created_ats"
_CANDIDATE_SEAL = "catalog_publication_candidate_definition_seals"

_PUBLICATION_IDENTITY = "catalog_publication_identities"

_PUBLICATION_ANCHOR = "catalog_publication_anchors"
_PUBLICATION_GALLERY = "catalog_publication_gallery_ids"
_PUBLICATION_SUMMARY = "catalog_publication_summary_sha256s"
_PUBLICATION_LANGUAGE = "catalog_publication_language_sha256s"
_PUBLICATION_MODIFIED = "catalog_publication_modified_ats"
_PUBLICATION_SEAL = "catalog_publication_seals"

_TITLE_ANCHOR = "catalog_publication_title_anchors"
_TITLE_SOURCE_TITLE = "catalog_publication_title_source_title_sha256s"
_TITLE_SOURCE_GALLERY = "catalog_publication_title_source_gallery_names"
_TITLE_SEAL = "catalog_publication_title_seals"

_CONTRIBUTOR_ANCHOR = "catalog_contributor_anchors"
_CONTRIBUTOR_NAME = "catalog_contributor_name_sha256s"
_CONTRIBUTOR_ROLE = "catalog_contributor_roles"
_CONTRIBUTOR_IDENTITY = "catalog_contributor_identities"
_CONTRIBUTOR_SEAL = "catalog_contributor_seals"


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
class CatalogPublicationFamily:
    revision: int
    publication_key: bytes
    gallery_id: int
    summary_sha256: bytes
    language_sha256: bytes
    modified_at: int

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


def _candidate_family_row(
    connector: Any,
    candidate_id: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    members = (
        _CANDIDATE_ANCHOR,
        _CANDIDATE_ANALYSIS,
        _CANDIDATE_REVISION,
        _CANDIDATE_ARTIFACT_POLICY,
        _CANDIDATE_DISPLAY_POLICY,
        _CANDIDATE_ARTIFACTS_REQUIRED,
        _CANDIDATE_CREATED_AT,
        _CANDIDATE_SEAL,
    )
    key_union = " UNION ".join(
        f"SELECT candidate_id FROM {table} WHERE candidate_id = %s" for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(candidate_id) AS ("
        + key_union
        + ") SELECT anchor.candidate_id, analysis.candidate_id, "
        "analysis.analysis_id, revision.candidate_id, revision.reserved_revision, "
        "artifact_policy.candidate_id, artifact_policy.artifact_policy_id, "
        "display_policy.candidate_id, display_policy.display_title_policy_id, "
        "required.candidate_id, required.artifacts_required, created.candidate_id, "
        "created.created_at, seal.candidate_id FROM family_keys AS family_key "
        f"LEFT JOIN {_CANDIDATE_ANCHOR} AS anchor USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_ANALYSIS} AS analysis USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_REVISION} AS revision USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_ARTIFACT_POLICY} AS artifact_policy "
        "USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_DISPLAY_POLICY} AS display_policy "
        "USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_ARTIFACTS_REQUIRED} AS required "
        "USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_CREATED_AT} AS created USING (candidate_id) "
        f"LEFT JOIN {_CANDIDATE_SEAL} AS seal USING (candidate_id)"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate_id,) * len(members),
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
    key_indexes = (0, 1, 3, 5, 7, 9, 11, 13)
    if len(row) != 14 or any(row[index] != candidate for index in key_indexes):
        raise PublicationFamilyPartialError("publication candidate family is partial")
    if row[10] not in {0, 1}:
        raise PublicationFamilyCollisionError(
            "publication candidate artifacts_required is not boolean"
        )
    try:
        return PublicationCandidateFamily(
            candidate,
            row[2],
            row[4],
            row[6],
            row[8],
            bool(row[10]),
            row[12],
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
    try:
        connector.execute(
            f"INSERT INTO {_CANDIDATE_ANCHOR} (candidate_id) VALUES (%s)",
            (candidate,),
        )
        for table, column, value in (
            (_CANDIDATE_ANALYSIS, "analysis_id", family.analysis_id),
            (_CANDIDATE_REVISION, "reserved_revision", family.reserved_revision),
            (
                _CANDIDATE_ARTIFACT_POLICY,
                "artifact_policy_id",
                family.artifact_policy_id,
            ),
            (
                _CANDIDATE_DISPLAY_POLICY,
                "display_title_policy_id",
                family.display_title_policy_id,
            ),
            (
                _CANDIDATE_ARTIFACTS_REQUIRED,
                "artifacts_required",
                int(family.artifacts_required),
            ),
            (_CANDIDATE_CREATED_AT, "created_at", family.created_at),
        ):
            connector.execute(
                f"INSERT INTO {table} (candidate_id, {column}) VALUES (%s, %s)",
                (candidate, value),
            )
        connector.execute(
            f"INSERT INTO {_CANDIDATE_SEAL} (candidate_id) VALUES (%s)",
            (candidate,),
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


def _publication_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    members = (
        _PUBLICATION_ANCHOR,
        _PUBLICATION_GALLERY,
        _PUBLICATION_SUMMARY,
        _PUBLICATION_LANGUAGE,
        _PUBLICATION_MODIFIED,
        _PUBLICATION_SEAL,
    )
    key_union = " UNION ".join(
        f"SELECT revision, publication_key FROM {table} "
        "WHERE revision = %s AND publication_key = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(revision, publication_key) AS ("
        + key_union
        + ") SELECT anchor.revision, anchor.publication_key, gallery.revision, "
        "gallery.publication_key, gallery.gallery_id, summary.revision, "
        "summary.publication_key, summary.summary_sha256, language.revision, "
        "language.publication_key, language.language_sha256, modified.revision, "
        "modified.publication_key, modified.modified_at, seal.revision, "
        "seal.publication_key FROM family_keys AS family_key "
        f"LEFT JOIN {_PUBLICATION_ANCHOR} AS anchor "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_PUBLICATION_GALLERY} AS gallery "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_PUBLICATION_SUMMARY} AS summary "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_PUBLICATION_LANGUAGE} AS language "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_PUBLICATION_MODIFIED} AS modified "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_PUBLICATION_SEAL} AS seal USING (revision, publication_key)"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key) * len(members),
    )
    return tuple(row)


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
    key_pairs = ((0, 1), (2, 3), (5, 6), (8, 9), (11, 12), (14, 15))
    if len(row) != 16 or any(
        (row[left], row[right]) != (catalog_revision, publication)
        for left, right in key_pairs
    ):
        raise PublicationFamilyPartialError("catalog publication family is partial")
    try:
        return CatalogPublicationFamily(
            catalog_revision,
            publication,
            row[4],
            row[7],
            row[10],
            row[13],
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
    key = (family.revision, family.publication_key)
    try:
        connector.execute(
            f"INSERT INTO {_PUBLICATION_ANCHOR} (revision, publication_key) "
            "VALUES (%s, %s)",
            key,
        )
        for table, column, value in (
            (_PUBLICATION_GALLERY, "gallery_id", family.gallery_id),
            (_PUBLICATION_SUMMARY, "summary_sha256", family.summary_sha256),
            (_PUBLICATION_LANGUAGE, "language_sha256", family.language_sha256),
            (_PUBLICATION_MODIFIED, "modified_at", family.modified_at),
        ):
            connector.execute(
                f"INSERT INTO {table} (revision, publication_key, {column}) "
                "VALUES (%s, %s, %s)",
                (*key, value),
            )
        connector.execute(
            f"INSERT INTO {_PUBLICATION_SEAL} (revision, publication_key) "
            "VALUES (%s, %s)",
            key,
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
                "catalog publication concurrent replay left partial facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog publication concurrent replay changed exact facts"
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
    members = (_TITLE_ANCHOR, _TITLE_SOURCE_TITLE, _TITLE_SOURCE_GALLERY, _TITLE_SEAL)
    key_union = " UNION ".join(
        f"SELECT revision, publication_key FROM {table} "
        "WHERE revision = %s AND publication_key = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(revision, publication_key) AS ("
        + key_union
        + ") SELECT anchor.revision, anchor.publication_key, title.revision, "
        "title.publication_key, title.source_title_sha256, gallery.revision, "
        "gallery.publication_key, gallery.source_gallery_name, seal.revision, "
        "seal.publication_key FROM family_keys AS family_key "
        f"LEFT JOIN {_TITLE_ANCHOR} AS anchor USING (revision, publication_key) "
        f"LEFT JOIN {_TITLE_SOURCE_TITLE} AS title "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_TITLE_SOURCE_GALLERY} AS gallery "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_TITLE_SEAL} AS seal USING (revision, publication_key)"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key) * len(members),
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
    key_pairs = ((0, 1), (2, 3), (5, 6), (8, 9))
    if len(row) != 10 or any(
        (row[left], row[right]) != (catalog_revision, publication)
        for left, right in key_pairs
    ):
        raise PublicationFamilyPartialError("catalog title family is partial")
    try:
        return CatalogPublicationTitleFamily(
            catalog_revision,
            publication,
            row[4],
            row[7],
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
    if existing is not None:
        if existing != family:
            raise PublicationFamilyCollisionError(
                "catalog title replay changed exact facts"
            )
        return existing, False
    key = (family.revision, family.publication_key)
    try:
        connector.execute(
            f"INSERT INTO {_TITLE_ANCHOR} (revision, publication_key) "
            "VALUES (%s, %s)",
            key,
        )
        connector.execute(
            f"INSERT INTO {_TITLE_SOURCE_TITLE} "
            "(revision, publication_key, source_title_sha256) "
            "VALUES (%s, %s, %s)",
            (*key, family.source_title_sha256),
        )
        connector.execute(
            f"INSERT INTO {_TITLE_SOURCE_GALLERY} "
            "(revision, publication_key, source_gallery_name) "
            "VALUES (%s, %s, %s)",
            (*key, family.source_gallery_name),
        )
        connector.execute(
            f"INSERT INTO {_TITLE_SEAL} (revision, publication_key) " "VALUES (%s, %s)",
            key,
        )
    except DatabaseDuplicateKeyError as error:
        try:
            raced = load_catalog_publication_title_family(
                connector,
                revision=family.revision,
                publication_key=family.publication_key,
                backend=backend,
                locking=True,
            )
        except PublicationFamilyCollisionError:
            raise PublicationFamilyCollisionError(
                "catalog title concurrent replay left partial facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog title concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True


def _contributor_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    position: int,
    *,
    backend: str,
    locking: bool,
) -> tuple[Any, ...]:
    members = (
        _CONTRIBUTOR_ANCHOR,
        _CONTRIBUTOR_NAME,
        _CONTRIBUTOR_ROLE,
        _CONTRIBUTOR_IDENTITY,
        _CONTRIBUTOR_SEAL,
    )
    key_union = " UNION ".join(
        f"SELECT revision, publication_key, position FROM {table} "
        "WHERE revision = %s AND publication_key = %s AND position = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(revision, publication_key, position) AS ("
        + key_union
        + ") SELECT anchor.revision, anchor.publication_key, anchor.position, "
        "name.revision, name.publication_key, name.position, "
        "name.contributor_name_sha256, role.revision, role.publication_key, "
        "role.position, role.role, identity_row.revision, "
        "identity_row.publication_key, identity_row.position, "
        "identity_row.contributor_name_sha256, identity_row.role, seal.revision, "
        "seal.publication_key, seal.position FROM family_keys AS family_key "
        f"LEFT JOIN {_CONTRIBUTOR_ANCHOR} AS anchor "
        "USING (revision, publication_key, position) "
        f"LEFT JOIN {_CONTRIBUTOR_NAME} AS name "
        "USING (revision, publication_key, position) "
        f"LEFT JOIN {_CONTRIBUTOR_ROLE} AS role "
        "USING (revision, publication_key, position) "
        f"LEFT JOIN {_CONTRIBUTOR_IDENTITY} AS identity_row "
        "USING (revision, publication_key, position) "
        f"LEFT JOIN {_CONTRIBUTOR_SEAL} AS seal "
        "USING (revision, publication_key, position)"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key, position) * len(members),
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
    keys = (
        (0, 1, 2),
        (3, 4, 5),
        (7, 8, 9),
        (11, 12, 13),
        (16, 17, 18),
    )
    expected = (catalog_revision, publication, occurrence)
    if len(row) != 19 or any(
        tuple(row[index] for index in key) != expected for key in keys
    ):
        raise PublicationFamilyPartialError("catalog contributor family is partial")
    if (row[6], row[10]) != (row[14], row[15]):
        raise PublicationFamilyCollisionError(
            "catalog contributor identity is not congruent"
        )
    try:
        return CatalogContributorFamily(
            catalog_revision,
            publication,
            occurrence,
            row[6],
            row[10],
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
    key = (family.revision, family.publication_key, family.position)
    try:
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_ANCHOR} "
            "(revision, publication_key, position) VALUES (%s, %s, %s)",
            key,
        )
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_NAME} "
            "(revision, publication_key, position, contributor_name_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (*key, family.contributor_name_sha256),
        )
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_ROLE} "
            "(revision, publication_key, position, role) "
            "VALUES (%s, %s, %s, %s)",
            (*key, family.role),
        )
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_IDENTITY} "
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
        connector.execute(
            f"INSERT INTO {_CONTRIBUTOR_SEAL} "
            "(revision, publication_key, position) VALUES (%s, %s, %s)",
            key,
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
                "catalog contributor concurrent replay left partial facts"
            ) from error
        if raced != family:
            raise PublicationFamilyCollisionError(
                "catalog contributor concurrent replay changed exact facts"
            ) from error
        return raced, False
    return family, True
