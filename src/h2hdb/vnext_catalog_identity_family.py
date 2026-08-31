"""Exact storage protocol for the small catalog identity families.

Gallery, file-name, and tag identities are complete BCNF base relations.
Observation-file occurrences remain sealed vertical families. Production
writers use base relations directly, insert completion seals last where
applicable, and treat an existing incomplete vertical family as corruption
rather than repairing it in place. Batch loaders use one set query per family
so a page never turns into one query per atomic fact.
"""

from __future__ import annotations

__all__ = [
    "CatalogIdentityCollisionError",
    "CatalogIdentityPartialFamilyError",
    "FileNameIdentity",
    "GalleryIdentity",
    "GalleryObservationFile",
    "TagTerm",
    "ensure_file_name_identities",
    "ensure_gallery_identity",
    "ensure_gallery_observation_files",
    "ensure_tag_term",
    "load_file_name_identities",
    "load_gallery_identities",
    "load_gallery_identity_candidates",
    "load_gallery_observation_files",
    "load_tag_terms",
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
)
from .vnext_identity import (
    ByteDomainError,
    file_key,
    file_role,
    validate_namespace,
)
from .vnext_identity import (
    gallery_key as derive_gallery_key,
)

_BATCH_LIMIT = 256

_GALLERY_IDENTITY = "catalog_gallery_identities"

_FILE_NAME_IDENTITY = "catalog_file_name_identities"

_OBSERVATION_FILE_ANCHOR = "catalog_gallery_observation_file_anchors"
_OBSERVATION_FILE_NO = "catalog_gallery_observation_file_file_nos"
_OBSERVATION_FILE_SHA256 = "catalog_gallery_observation_file_file_sha256s"
_OBSERVATION_FILE_ARTIFACT_ROLE = "catalog_gallery_observation_file_artifact_role"
_OBSERVATION_FILE_SEAL = "catalog_gallery_observation_file_seals"

_TAG_TERM = "catalog_tag_terms"


class CatalogIdentityCollisionError(RuntimeError):
    """A complete immutable family disagrees with its exact authority."""


class CatalogIdentityPartialFamilyError(CatalogIdentityCollisionError):
    """At least one family member exists without one complete sealed tuple."""


@dataclass(frozen=True, slots=True)
class GalleryIdentity:
    gallery_id: int
    gallery_key: bytes
    scope_key: bytes
    locator_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="gallery_id")
        stable = require_digest32(self.gallery_key, field="gallery_key")
        scope = require_digest32(self.scope_key, field="scope_key")
        locator = require_digest32(self.locator_sha256, field="locator_sha256")
        if stable != derive_gallery_key(scope, locator):
            raise ValueError(
                "gallery identity stable key does not match its exact coordinate"
            )


@dataclass(frozen=True, slots=True)
class FileNameIdentity:
    file_key: bytes
    name_bytes: bytes
    file_role: bytes

    def __post_init__(self) -> None:
        key = require_digest32(self.file_key, field="file_key")
        name = require_bounded_bytes(
            self.name_bytes,
            field="name_bytes",
            minimum=1,
            maximum=255,
        )
        role = require_bounded_bytes(
            self.file_role,
            field="file_role",
            minimum=7,
            maximum=8,
        )
        try:
            expected_key = file_key(name)
            expected_role = file_role(name)
        except ByteDomainError as error:
            raise ValueError(
                "file-name identity contains invalid exact bytes"
            ) from error
        if key != expected_key or role != expected_role:
            raise ValueError(
                "file-name identity does not match its derived key and role"
            )


@dataclass(frozen=True, slots=True)
class GalleryObservationFile:
    gallery_id: int
    observation_id: int
    file_no: int
    file_key: bytes
    file_sha256: bytes
    artifact_role: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_positive_int63(self.observation_id, field="observation_id")
        require_int63(self.file_no, field="file_no")
        require_digest32(self.file_key, field="file_key")
        require_digest32(self.file_sha256, field="file_sha256")
        role = require_bounded_bytes(
            self.artifact_role,
            field="artifact_role",
            minimum=4,
            maximum=8,
        )
        if role not in {b"metadata", b"page", b"other"}:
            raise ValueError("artifact_role is not registered")


@dataclass(frozen=True, slots=True)
class TagTerm:
    tag_id: int
    namespace: bytes
    tag_value_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.tag_id, field="tag_id")
        namespace = require_bounded_bytes(
            self.namespace,
            field="namespace",
            maximum=128,
        )
        try:
            decoded = namespace.decode("utf-8", errors="strict")
            if validate_namespace(decoded) != namespace:
                raise ValueError("tag namespace changed during UTF-8 validation")
        except (UnicodeDecodeError, ByteDomainError) as error:
            raise ValueError("tag namespace is not exact strict UTF-8") from error
        require_digest32(self.tag_value_sha256, field="tag_value_sha256")


def _bounded_unique(values: Sequence[Any], *, label: str) -> tuple[Any, ...]:
    if len(values) > _BATCH_LIMIT:
        raise ValueError(f"{label} batch is limited to {_BATCH_LIMIT} values")
    return tuple(dict.fromkeys(values))


def _placeholders(count: int) -> str:
    return ", ".join("%s" for _ in range(count))


def load_gallery_identities(
    connector: Any,
    *,
    gallery_ids: Sequence[int],
) -> dict[int, GalleryIdentity]:
    ids = _bounded_unique(
        tuple(
            require_positive_int63(value, field="gallery_id") for value in gallery_ids
        ),
        label="gallery identity",
    )
    if not ids:
        return {}
    clause = f"gallery_id IN ({_placeholders(len(ids))})"
    return _gallery_rows(
        connector,
        candidate_sql=f"SELECT gallery_id FROM {_GALLERY_IDENTITY} WHERE {clause}",
        parameters=ids,
    )


def load_gallery_identity_candidates(
    connector: Any,
    *,
    scope_key: bytes,
    locator_sha256: bytes,
    gallery_key: bytes,
    gallery_id: int | None = None,
) -> tuple[GalleryIdentity, ...]:
    scope = require_digest32(scope_key, field="scope_key")
    locator = require_digest32(locator_sha256, field="locator_sha256")
    stable = require_digest32(gallery_key, field="gallery_key")
    branches = [
        f"SELECT gallery_id FROM {_GALLERY_IDENTITY} "
        "WHERE scope_key = %s AND locator_sha256 = %s",
        f"SELECT gallery_id FROM {_GALLERY_IDENTITY} WHERE gallery_key = %s",
    ]
    parameters: tuple[Any, ...] = (scope, locator, stable)
    if gallery_id is not None:
        exact_id = require_positive_int63(gallery_id, field="gallery_id")
        branches.append(
            f"SELECT gallery_id FROM {_GALLERY_IDENTITY} WHERE gallery_id = %s"
        )
        parameters += (exact_id,)
    return tuple(
        _gallery_rows(
            connector,
            candidate_sql=" UNION ".join(branches),
            parameters=parameters,
        ).values()
    )


def _gallery_rows(
    connector: Any,
    *,
    candidate_sql: str,
    parameters: tuple[Any, ...],
) -> dict[int, GalleryIdentity]:
    rows = connector.fetch_all(
        "WITH candidate_ids(gallery_id) AS (" + candidate_sql + ") "
        "SELECT i.gallery_id, i.gallery_key, i.scope_key, i.locator_sha256 "
        "FROM candidate_ids AS k "
        f"JOIN {_GALLERY_IDENTITY} AS i ON i.gallery_id = k.gallery_id "
        "ORDER BY i.gallery_id",
        parameters,
    )
    result: dict[int, GalleryIdentity] = {}
    for row in rows:
        if len(row) != 4:
            raise CatalogIdentityCollisionError(
                "gallery identity has an invalid physical shape"
            )
        gallery_id = require_positive_int63(row[0], field="gallery_id")
        try:
            identity = GalleryIdentity(gallery_id, row[1], row[2], row[3])
        except (TypeError, ValueError) as error:
            raise CatalogIdentityCollisionError(
                "gallery identity contains invalid immutable facts"
            ) from error
        if gallery_id in result:
            raise CatalogIdentityCollisionError(
                "gallery identity candidate is duplicated"
            )
        result[gallery_id] = identity
    return result


def ensure_gallery_identity(
    connector: Any,
    *,
    identity: GalleryIdentity,
) -> bool:
    if type(identity) is not GalleryIdentity:
        raise TypeError("identity must be an exact GalleryIdentity")
    identity.__post_init__()
    existing = load_gallery_identity_candidates(
        connector,
        scope_key=identity.scope_key,
        locator_sha256=identity.locator_sha256,
        gallery_key=identity.gallery_key,
        gallery_id=identity.gallery_id,
    )
    if existing:
        if existing != (identity,):
            raise CatalogIdentityCollisionError(
                "gallery surrogate, stable key, or natural coordinate collides"
            )
        return False
    connector.execute(
        f"INSERT INTO {_GALLERY_IDENTITY} "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            identity.gallery_id,
            identity.gallery_key,
            identity.scope_key,
            identity.locator_sha256,
        ),
    )
    return True


def load_file_name_identities(
    connector: Any,
    *,
    file_keys: Sequence[bytes] = (),
    name_bytes: Sequence[bytes] = (),
) -> dict[bytes, FileNameIdentity]:
    keys = _bounded_unique(
        tuple(require_digest32(value, field="file_key") for value in file_keys),
        label="file-name key",
    )
    names = _bounded_unique(
        tuple(
            require_bounded_bytes(
                value,
                field="name_bytes",
                minimum=1,
                maximum=255,
            )
            for value in name_bytes
        ),
        label="file-name bytes",
    )
    if not keys and not names:
        return {}
    clauses: list[str] = []
    parameters: tuple[Any, ...] = ()
    if keys:
        clauses.append(f"file_key IN ({_placeholders(len(keys))})")
        parameters += keys
    if names:
        clauses.append(f"name_bytes IN ({_placeholders(len(names))})")
        parameters += names
    rows = connector.fetch_all(
        f"SELECT file_key, name_bytes FROM {_FILE_NAME_IDENTITY} WHERE "
        + " OR ".join(f"({clause})" for clause in clauses)
        + " ORDER BY file_key",
        parameters,
    )
    result: dict[bytes, FileNameIdentity] = {}
    for row in rows:
        if len(row) != 2:
            raise CatalogIdentityCollisionError(
                "file-name identity has an invalid physical shape"
            )
        key = require_digest32(row[0], field="file_key")
        try:
            name = require_bounded_bytes(
                row[1],
                field="name_bytes",
                minimum=1,
                maximum=255,
            )
            identity = FileNameIdentity(key, name, file_role(name))
        except (TypeError, ValueError) as error:
            raise CatalogIdentityCollisionError(
                "file-name identity contains invalid derived facts"
            ) from error
        result[key] = identity
    return result


def ensure_file_name_identities(
    connector: Any,
    *,
    identities: Sequence[FileNameIdentity],
) -> dict[bytes, FileNameIdentity]:
    proposed = _bounded_unique(tuple(identities), label="file-name identity")
    for identity in proposed:
        if type(identity) is not FileNameIdentity:
            raise TypeError("identities must contain exact FileNameIdentity values")
        identity.__post_init__()
    by_key: dict[bytes, FileNameIdentity] = {}
    by_name: dict[bytes, FileNameIdentity] = {}
    for identity in proposed:
        if identity.file_key in by_key or identity.name_bytes in by_name:
            raise CatalogIdentityCollisionError(
                "file-name batch repeats a key or exact name"
            )
        by_key[identity.file_key] = identity
        by_name[identity.name_bytes] = identity
    existing = load_file_name_identities(
        connector,
        file_keys=tuple(by_key),
        name_bytes=tuple(by_name),
    )
    existing_by_name = {value.name_bytes: value for value in existing.values()}
    for identity in proposed:
        by_stored_key = existing.get(identity.file_key)
        by_stored_name = existing_by_name.get(identity.name_bytes)
        if by_stored_key is not None or by_stored_name is not None:
            if by_stored_key != identity or by_stored_name != identity:
                raise CatalogIdentityCollisionError(
                    "file-name digest or exact-name candidate key collides"
                )
            continue
        connector.execute(
            f"INSERT INTO {_FILE_NAME_IDENTITY} (file_key, name_bytes) VALUES (%s, %s)",
            (identity.file_key, identity.name_bytes),
        )
        existing[identity.file_key] = identity
        existing_by_name[identity.name_bytes] = identity
    return {identity.file_key: identity for identity in proposed}


def load_gallery_observation_files(
    connector: Any,
    *,
    gallery_id: int,
    observation_id: int,
    file_keys: Sequence[bytes],
    file_nos: Sequence[int],
) -> dict[bytes, GalleryObservationFile]:
    gallery = require_positive_int63(gallery_id, field="gallery_id")
    observation = require_positive_int63(observation_id, field="observation_id")
    keys = _bounded_unique(
        tuple(require_digest32(value, field="file_key") for value in file_keys),
        label="observation file key",
    )
    numbers = _bounded_unique(
        tuple(require_int63(value, field="file_no") for value in file_nos),
        label="observation file number",
    )
    if not keys and not numbers:
        return {}
    branches: list[str] = []
    parameters: tuple[Any, ...] = ()
    if keys:
        key_clause = f"file_key IN ({_placeholders(len(keys))})"
        for table in (
            _OBSERVATION_FILE_ANCHOR,
            _OBSERVATION_FILE_NO,
            _OBSERVATION_FILE_SHA256,
            _OBSERVATION_FILE_ARTIFACT_ROLE,
            _OBSERVATION_FILE_SEAL,
        ):
            branches.append(
                f"SELECT gallery_id, observation_id, file_key FROM {table} "
                f"WHERE gallery_id = %s AND observation_id = %s AND {key_clause}"
            )
            parameters += (gallery, observation, *keys)
    if numbers:
        branches.append(
            f"SELECT gallery_id, observation_id, file_key FROM {_OBSERVATION_FILE_NO} "
            "WHERE gallery_id = %s AND observation_id = %s "
            f"AND file_no IN ({_placeholders(len(numbers))})"
        )
        parameters += (gallery, observation, *numbers)
    rows = connector.fetch_all(
        "WITH candidate_keys(gallery_id, observation_id, file_key) AS ("
        + " UNION ".join(branches)
        + ") SELECT k.gallery_id, k.observation_id, k.file_key, "
        "a.gallery_id, a.observation_id, a.file_key, "
        "n.gallery_id, n.observation_id, n.file_key, n.file_no, "
        "h.gallery_id, h.observation_id, h.file_key, h.file_sha256, "
        "r.gallery_id, r.observation_id, r.file_key, r.artifact_role, "
        "s.gallery_id, s.observation_id, s.file_key FROM candidate_keys AS k "
        f"LEFT JOIN {_OBSERVATION_FILE_ANCHOR} AS a "
        "ON a.gallery_id = k.gallery_id AND a.observation_id = k.observation_id "
        "AND a.file_key = k.file_key "
        f"LEFT JOIN {_OBSERVATION_FILE_NO} AS n "
        "ON n.gallery_id = k.gallery_id AND n.observation_id = k.observation_id "
        "AND n.file_key = k.file_key "
        f"LEFT JOIN {_OBSERVATION_FILE_SHA256} AS h "
        "ON h.gallery_id = k.gallery_id AND h.observation_id = k.observation_id "
        "AND h.file_key = k.file_key "
        f"LEFT JOIN {_OBSERVATION_FILE_ARTIFACT_ROLE} AS r "
        "ON r.gallery_id = k.gallery_id AND r.observation_id = k.observation_id "
        "AND r.file_key = k.file_key "
        f"LEFT JOIN {_OBSERVATION_FILE_SEAL} AS s "
        "ON s.gallery_id = k.gallery_id AND s.observation_id = k.observation_id "
        "AND s.file_key = k.file_key ORDER BY k.file_key",
        parameters,
    )
    result: dict[bytes, GalleryObservationFile] = {}
    expected_key_prefix = (gallery, observation)
    for row in rows:
        if len(row) != 21:
            raise CatalogIdentityPartialFamilyError(
                "gallery observation file family has an invalid physical shape"
            )
        key = require_digest32(row[2], field="file_key")
        expected = (*expected_key_prefix, key)
        if any(
            tuple(row[index : index + 3]) != expected for index in (0, 3, 6, 10, 14, 18)
        ):
            raise CatalogIdentityPartialFamilyError(
                "gallery observation file has an incomplete sealed family"
            )
        try:
            identity = GalleryObservationFile(
                gallery, observation, row[9], key, row[13], row[17]
            )
        except (TypeError, ValueError) as error:
            raise CatalogIdentityCollisionError(
                "gallery observation file contains invalid immutable facts"
            ) from error
        result[key] = identity
    return result


def ensure_gallery_observation_files(
    connector: Any,
    *,
    identities: Sequence[GalleryObservationFile],
) -> dict[bytes, GalleryObservationFile]:
    proposed = _bounded_unique(tuple(identities), label="gallery observation file")
    if not proposed:
        return {}
    for identity in proposed:
        if type(identity) is not GalleryObservationFile:
            raise TypeError(
                "identities must contain exact GalleryObservationFile values"
            )
        identity.__post_init__()
    gallery = proposed[0].gallery_id
    observation = proposed[0].observation_id
    if any(
        (identity.gallery_id, identity.observation_id) != (gallery, observation)
        for identity in proposed
    ):
        raise ValueError("one observation-file batch must share one observation")
    by_key: dict[bytes, GalleryObservationFile] = {}
    by_number: dict[int, GalleryObservationFile] = {}
    for identity in proposed:
        if identity.file_key in by_key or identity.file_no in by_number:
            raise CatalogIdentityCollisionError(
                "observation-file batch repeats a file key or file number"
            )
        by_key[identity.file_key] = identity
        by_number[identity.file_no] = identity
    existing = load_gallery_observation_files(
        connector,
        gallery_id=gallery,
        observation_id=observation,
        file_keys=tuple(by_key),
        file_nos=tuple(by_number),
    )
    existing_by_number = {value.file_no: value for value in existing.values()}
    for identity in proposed:
        by_stored_key = existing.get(identity.file_key)
        by_stored_number = existing_by_number.get(identity.file_no)
        if by_stored_key is not None or by_stored_number is not None:
            if by_stored_key != identity or by_stored_number != identity:
                raise CatalogIdentityCollisionError(
                    "observation file-name or ordinal candidate key collides"
                )
            continue
        key = (identity.gallery_id, identity.observation_id, identity.file_key)
        connector.execute(
            f"INSERT INTO {_OBSERVATION_FILE_ANCHOR} "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            key,
        )
        connector.execute(
            f"INSERT INTO {_OBSERVATION_FILE_NO} "
            "(gallery_id, observation_id, file_key, file_no) "
            "VALUES (%s, %s, %s, %s)",
            (*key, identity.file_no),
        )
        connector.execute(
            f"INSERT INTO {_OBSERVATION_FILE_SHA256} "
            "(gallery_id, observation_id, file_key, file_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (*key, identity.file_sha256),
        )
        connector.execute(
            f"INSERT INTO {_OBSERVATION_FILE_ARTIFACT_ROLE} "
            "(gallery_id, observation_id, file_key, artifact_role) "
            "VALUES (%s, %s, %s, %s)",
            (*key, identity.artifact_role),
        )
        connector.execute(
            f"INSERT INTO {_OBSERVATION_FILE_SEAL} "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            key,
        )
        existing[identity.file_key] = identity
        existing_by_number[identity.file_no] = identity
    return {identity.file_key: identity for identity in proposed}


def load_tag_terms(
    connector: Any,
    *,
    tag_ids: Sequence[int] = (),
    identities: Sequence[tuple[bytes, bytes]] = (),
) -> dict[int, TagTerm]:
    ids = _bounded_unique(
        tuple(require_positive_int63(value, field="tag_id") for value in tag_ids),
        label="tag id",
    )
    naturals = _bounded_unique(tuple(identities), label="tag natural identity")
    normalized_naturals: tuple[tuple[bytes, bytes], ...] = tuple(
        (
            require_bounded_bytes(namespace, field="namespace", maximum=128),
            require_digest32(value, field="tag_value_sha256"),
        )
        for namespace, value in naturals
    )
    if not ids and not normalized_naturals:
        return {}
    branches: list[str] = []
    parameters: tuple[Any, ...] = ()
    if ids:
        clause = f"tag_id IN ({_placeholders(len(ids))})"
        branches.append(f"SELECT tag_id FROM {_TAG_TERM} WHERE {clause}")
        parameters += ids
    for namespace, value in normalized_naturals:
        branches.append(
            f"SELECT tag_id FROM {_TAG_TERM} "
            "WHERE namespace = %s AND tag_value_sha256 = %s"
        )
        parameters += (namespace, value)
    rows = connector.fetch_all(
        "WITH candidate_ids(tag_id) AS (" + " UNION ".join(branches) + ") "
        "SELECT k.tag_id, t.namespace, t.tag_value_sha256 "
        "FROM candidate_ids AS k "
        f"JOIN {_TAG_TERM} AS t ON t.tag_id = k.tag_id ORDER BY k.tag_id",
        parameters,
    )
    result: dict[int, TagTerm] = {}
    for row in rows:
        if len(row) != 3:
            raise CatalogIdentityCollisionError(
                "tag term has an invalid physical shape"
            )
        tag_id = require_positive_int63(row[0], field="tag_id")
        try:
            term = TagTerm(tag_id, row[1], row[2])
        except (TypeError, ValueError) as error:
            raise CatalogIdentityCollisionError(
                "tag term contains invalid immutable facts"
            ) from error
        result[tag_id] = term
    return result


def ensure_tag_term(connector: Any, *, term: TagTerm) -> bool:
    if type(term) is not TagTerm:
        raise TypeError("term must be an exact TagTerm")
    term.__post_init__()
    existing = load_tag_terms(
        connector,
        tag_ids=(term.tag_id,),
        identities=((term.namespace, term.tag_value_sha256),),
    )
    if existing:
        if tuple(existing.values()) != (term,):
            raise CatalogIdentityCollisionError(
                "tag surrogate or natural identity candidate key collides"
            )
        return False
    connector.execute(
        f"INSERT INTO {_TAG_TERM} "
        "(tag_id, namespace, tag_value_sha256) VALUES (%s, %s, %s)",
        (term.tag_id, term.namespace, term.tag_value_sha256),
    )
    return True
