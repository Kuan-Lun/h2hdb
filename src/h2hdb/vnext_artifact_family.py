"""Exact physical storage protocols for vNext artifact families.

The historical artifact semantic, prepared-artifact, and catalog occurrence
relations are read-only views.  This module is the sole narrow-family protocol:
it loads every physical member in one set query, rejects partial families,
inserts immutable members with the completion seal last, and isolates the only
mutable prepared-artifact state behind an exact compare-and-swap.
"""

from __future__ import annotations

__all__ = [
    "ArtifactFamilyCollisionError",
    "ArtifactFamilyPartialError",
    "ArtifactSemanticInputFamily",
    "CatalogArtifactFamily",
    "PreparedArtifactFamily",
    "cas_prepared_artifact_state",
    "ensure_artifact_semantic_input_family",
    "ensure_catalog_artifact_family",
    "ensure_prepared_artifact_family",
    "load_artifact_semantic_input_family",
    "load_artifact_semantic_input_family_by_identity",
    "load_catalog_artifact_family",
    "load_prepared_artifact_family",
    "load_prepared_artifact_family_by_token",
]

from dataclasses import dataclass, replace
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
from .vnext_transaction import VNextUnitOfWork

_SEMANTIC_ANCHOR = "catalog_artifact_semantic_input_anchors"
_SEMANTIC_SOURCE = "catalog_artifact_semantic_source_manifest_sha256s"
_SEMANTIC_MEMBER = "catalog_artifact_semantic_member_plan_sha256s"
_SEMANTIC_EFFECTIVE = "catalog_artifact_semantic_effective_content_sha256s"
_SEMANTIC_SELECTED = "catalog_artifact_semantic_selected_sha256s"
_SEMANTIC_OWNER = "catalog_artifact_semantic_owner_sha256s"
_SEMANTIC_POLICY = "catalog_artifact_semantic_policy_sha256s"
_SEMANTIC_IDENTITY = "catalog_artifact_semantic_input_identities"
_SEMANTIC_SEAL = "catalog_artifact_semantic_input_seals"

_PREPARED_ANCHOR = "catalog_prepared_artifact_anchors"
_PREPARED_SHA = "catalog_prepared_artifact_sha256s"
_PREPARED_CODEC = "catalog_prepared_artifact_storage_codec_versions"
_PREPARED_GENERATION = "catalog_prepared_artifact_storage_generations"
_PREPARED_TOKEN = "catalog_prepared_artifact_protection_tokens"
_PREPARED_STATE = "catalog_prepared_artifact_states"
_PREPARED_SEAL = "catalog_prepared_artifact_seals"

_CATALOG_ANCHOR = "catalog_artifact_anchors"
_CATALOG_SHA = "catalog_artifact_sha256s"
_CATALOG_SEMANTICS = "catalog_artifact_semantics_sha256s"
_CATALOG_SEAL = "catalog_artifact_seals"


class ArtifactFamilyCollisionError(RuntimeError):
    """A complete artifact family disagrees with exact durable authority."""


class ArtifactFamilyPartialError(ArtifactFamilyCollisionError):
    """At least one physical member exists without one complete family."""


@dataclass(frozen=True, slots=True)
class ArtifactSemanticInputFamily:
    artifact_semantics_sha256: bytes
    source_manifest_component_sha256: bytes
    member_plan_component_sha256: bytes
    effective_content_component_sha256: bytes
    selected_component_sha256: bytes
    owner_component_sha256: bytes
    policy_component_sha256: bytes

    def __post_init__(self) -> None:
        for label, value in self.component_items:
            require_digest32(value, field=f"artifact semantic {label}")
        expected = identity.artifact_semantics_digest(*self.components)
        if expected != self.artifact_semantics_sha256:
            raise ValueError("artifact semantic digest does not match its exact frame")

    @property
    def components(self) -> tuple[bytes, bytes, bytes, bytes, bytes, bytes]:
        return (
            self.source_manifest_component_sha256,
            self.member_plan_component_sha256,
            self.effective_content_component_sha256,
            self.selected_component_sha256,
            self.owner_component_sha256,
            self.policy_component_sha256,
        )

    @property
    def component_items(self) -> tuple[tuple[str, bytes], ...]:
        return (
            ("artifact_semantics_sha256", self.artifact_semantics_sha256),
            (
                "source_manifest_component_sha256",
                self.source_manifest_component_sha256,
            ),
            ("member_plan_component_sha256", self.member_plan_component_sha256),
            (
                "effective_content_component_sha256",
                self.effective_content_component_sha256,
            ),
            ("selected_component_sha256", self.selected_component_sha256),
            ("owner_component_sha256", self.owner_component_sha256),
            ("policy_component_sha256", self.policy_component_sha256),
        )


@dataclass(frozen=True, slots=True)
class PreparedArtifactFamily:
    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    storage_codec_version: int
    storage_generation: int
    protection_token: bytes
    state: str

    def __post_init__(self) -> None:
        candidate = require_uuid16(self.candidate_id, field="prepared candidate_id")
        publication = require_digest32(
            self.publication_key,
            field="prepared publication_key",
        )
        artifact = require_digest32(
            self.artifact_sha256,
            field="prepared artifact_sha256",
        )
        codec = require_positive_int63(
            self.storage_codec_version,
            field="prepared storage_codec_version",
        )
        generation = require_int63(
            self.storage_generation,
            field="prepared storage_generation",
        )
        token_bytes = require_bounded_bytes(
            self.protection_token,
            field="prepared protection_token",
            minimum=184,
            maximum=184,
        )
        if self.state not in {"PENDING", "PREPARED", "COMMITTED"}:
            raise ValueError("prepared artifact state is not registered")
        try:
            token = identity.decode_artifact_protection_token(token_bytes)
        except (TypeError, ValueError) as error:
            raise ValueError("prepared artifact protection token is invalid") from error
        if (
            token.candidate_id != candidate
            or token.publication_key != publication
            or token.artifact_sha256 != artifact
            or token.storage_codec_version != codec
            or token.storage_generation != generation
        ):
            raise ValueError("prepared artifact token disagrees with family facts")


@dataclass(frozen=True, slots=True)
class CatalogArtifactFamily:
    revision: int
    publication_key: bytes
    artifact_sha256: bytes
    artifact_semantics_sha256: bytes

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog artifact revision")
        require_digest32(self.publication_key, field="catalog artifact publication_key")
        require_digest32(self.artifact_sha256, field="catalog artifact_sha256")
        require_digest32(
            self.artifact_semantics_sha256,
            field="catalog artifact_semantics_sha256",
        )


def _locking_suffix(*, backend: str, locking: bool) -> str:
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError("artifact family backend is not registered")
    return " FOR UPDATE" if locking and backend == "mariadb" else ""


def _semantic_family_row(
    connector: Any,
    digest: bytes,
    *,
    backend: str = "sqlite",
    locking: bool = False,
) -> tuple[Any, ...]:
    members = (
        _SEMANTIC_ANCHOR,
        _SEMANTIC_SOURCE,
        _SEMANTIC_MEMBER,
        _SEMANTIC_EFFECTIVE,
        _SEMANTIC_SELECTED,
        _SEMANTIC_OWNER,
        _SEMANTIC_POLICY,
        _SEMANTIC_IDENTITY,
        _SEMANTIC_SEAL,
    )
    key_union = " UNION ".join(
        f"SELECT artifact_semantics_sha256 FROM {table} "
        "WHERE artifact_semantics_sha256 = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(artifact_semantics_sha256) AS ("
        + key_union
        + ") SELECT anchor.artifact_semantics_sha256, "
        "source.artifact_semantics_sha256, source.source_manifest_component_sha256, "
        "member.artifact_semantics_sha256, member.member_plan_component_sha256, "
        "effective.artifact_semantics_sha256, "
        "effective.effective_content_component_sha256, "
        "selected.artifact_semantics_sha256, selected.selected_component_sha256, "
        "owner.artifact_semantics_sha256, owner.owner_component_sha256, "
        "policy.artifact_semantics_sha256, policy.policy_component_sha256, "
        "identity_row.artifact_semantics_sha256, "
        "identity_row.source_manifest_component_sha256, "
        "identity_row.member_plan_component_sha256, "
        "identity_row.effective_content_component_sha256, "
        "identity_row.selected_component_sha256, "
        "identity_row.owner_component_sha256, "
        "identity_row.policy_component_sha256, seal.artifact_semantics_sha256 "
        "FROM family_keys AS family_key "
        f"LEFT JOIN {_SEMANTIC_ANCHOR} AS anchor USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_SOURCE} AS source USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_MEMBER} AS member USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_EFFECTIVE} AS effective USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_SELECTED} AS selected USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_OWNER} AS owner USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_POLICY} AS policy USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_IDENTITY} AS identity_row "
        "USING (artifact_semantics_sha256) "
        f"LEFT JOIN {_SEMANTIC_SEAL} AS seal USING (artifact_semantics_sha256)"
        + _locking_suffix(backend=backend, locking=locking),
        (digest,) * len(members),
    )
    return tuple(row)


def load_artifact_semantic_input_family(
    connector: Any,
    *,
    artifact_semantics_sha256: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> ArtifactSemanticInputFamily | None:
    digest = require_digest32(
        artifact_semantics_sha256,
        field="artifact_semantics_sha256",
    )
    row = _semantic_family_row(
        connector,
        digest,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    key_indexes = (0, 1, 3, 5, 7, 9, 11, 13, 20)
    if len(row) != 21 or any(row[index] != digest for index in key_indexes):
        raise ArtifactFamilyPartialError("artifact semantic input family is partial")
    facts = (row[2], row[4], row[6], row[8], row[10], row[12])
    if tuple(row[14:20]) != facts:
        raise ArtifactFamilyCollisionError(
            "artifact semantic natural identity disagrees with direct facts"
        )
    try:
        return ArtifactSemanticInputFamily(digest, *facts)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "artifact semantic input family contains invalid facts"
        ) from error


def load_artifact_semantic_input_family_by_identity(
    connector: Any,
    *,
    components: tuple[bytes, bytes, bytes, bytes, bytes, bytes],
) -> ArtifactSemanticInputFamily | None:
    exact = tuple(
        require_digest32(value, field=f"artifact semantic component {index}")
        for index, value in enumerate(components)
    )
    row = connector.fetch_one(
        f"SELECT artifact_semantics_sha256 FROM {_SEMANTIC_IDENTITY} WHERE "
        "source_manifest_component_sha256 = %s "
        "AND member_plan_component_sha256 = %s "
        "AND effective_content_component_sha256 = %s "
        "AND selected_component_sha256 = %s "
        "AND owner_component_sha256 = %s "
        "AND policy_component_sha256 = %s",
        exact,
    )
    if not row:
        return None
    if len(row) != 1:
        raise ArtifactFamilyCollisionError("artifact semantic identity is malformed")
    family = load_artifact_semantic_input_family(
        connector,
        artifact_semantics_sha256=row[0],
    )
    if family is None or family.components != exact:
        raise ArtifactFamilyPartialError(
            "artifact semantic natural identity has no congruent sealed family"
        )
    return family


def ensure_artifact_semantic_input_family(
    connector: Any,
    family: ArtifactSemanticInputFamily,
    *,
    backend: str = "sqlite",
) -> tuple[ArtifactSemanticInputFamily, bool]:
    if not isinstance(family, ArtifactSemanticInputFamily):
        raise TypeError("family must be ArtifactSemanticInputFamily")
    _locking_suffix(backend=backend, locking=False)
    existing_digest = load_artifact_semantic_input_family(
        connector,
        artifact_semantics_sha256=family.artifact_semantics_sha256,
        backend=backend,
    )
    existing_identity = load_artifact_semantic_input_family_by_identity(
        connector,
        components=family.components,
    )
    for existing in (existing_digest, existing_identity):
        if existing is not None and existing != family:
            raise ArtifactFamilyCollisionError(
                "artifact semantic replay collides with different exact facts"
            )
    if existing_digest is not None or existing_identity is not None:
        if existing_digest != existing_identity:
            raise ArtifactFamilyPartialError(
                "artifact semantic digest and natural identity visibility differ"
            )
        return family, False
    digest = family.artifact_semantics_sha256
    try:
        connector.execute(
            f"INSERT INTO {_SEMANTIC_ANCHOR} "
            "(artifact_semantics_sha256) VALUES (%s)",
            (digest,),
        )
    except DatabaseDuplicateKeyError:
        raced = load_artifact_semantic_input_family(
            connector,
            artifact_semantics_sha256=digest,
            backend=backend,
            locking=True,
        )
        if raced != family:
            raise ArtifactFamilyCollisionError(
                "artifact semantic concurrent replay changed exact facts"
            )
        return raced, False
    facts = (
        (_SEMANTIC_SOURCE, "source_manifest_component_sha256", family.components[0]),
        (_SEMANTIC_MEMBER, "member_plan_component_sha256", family.components[1]),
        (
            _SEMANTIC_EFFECTIVE,
            "effective_content_component_sha256",
            family.components[2],
        ),
        (_SEMANTIC_SELECTED, "selected_component_sha256", family.components[3]),
        (_SEMANTIC_OWNER, "owner_component_sha256", family.components[4]),
        (_SEMANTIC_POLICY, "policy_component_sha256", family.components[5]),
    )
    for table, column, value in facts:
        connector.execute(
            f"INSERT INTO {table} (artifact_semantics_sha256, {column}) "
            "VALUES (%s, %s)",
            (digest, value),
        )
    connector.execute(
        f"INSERT INTO {_SEMANTIC_IDENTITY} "
        "(source_manifest_component_sha256, member_plan_component_sha256, "
        "effective_content_component_sha256, selected_component_sha256, "
        "owner_component_sha256, policy_component_sha256, "
        "artifact_semantics_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (*family.components, digest),
    )
    connector.execute(
        f"INSERT INTO {_SEMANTIC_SEAL} (artifact_semantics_sha256) VALUES (%s)",
        (digest,),
    )
    return family, True


def _prepared_family_row(
    connector: Any,
    candidate_id: bytes,
    publication_key: bytes,
    *,
    backend: str = "sqlite",
    locking: bool = False,
) -> tuple[Any, ...]:
    members = (
        _PREPARED_ANCHOR,
        _PREPARED_SHA,
        _PREPARED_CODEC,
        _PREPARED_GENERATION,
        _PREPARED_TOKEN,
        _PREPARED_STATE,
        _PREPARED_SEAL,
    )
    key_union = " UNION ".join(
        f"SELECT candidate_id, publication_key FROM {table} "
        "WHERE candidate_id = %s AND publication_key = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(candidate_id, publication_key) AS ("
        + key_union
        + ") SELECT anchor.candidate_id, anchor.publication_key, "
        "digest.candidate_id, digest.publication_key, digest.artifact_sha256, "
        "codec.candidate_id, codec.publication_key, codec.storage_codec_version, "
        "generation.candidate_id, generation.publication_key, "
        "generation.storage_generation, token.candidate_id, token.publication_key, "
        "token.protection_token, state.candidate_id, state.publication_key, "
        "state.state, seal.candidate_id, seal.publication_key, "
        "artifact_blob.size_bytes, "
        "location.artifact_locator_sha256 FROM family_keys AS family_key "
        f"LEFT JOIN {_PREPARED_ANCHOR} AS anchor USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_SHA} AS digest USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_CODEC} AS codec USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_GENERATION} AS generation "
        "USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_TOKEN} AS token USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_STATE} AS state USING (candidate_id, publication_key) "
        f"LEFT JOIN {_PREPARED_SEAL} AS seal USING (candidate_id, publication_key) "
        "LEFT JOIN catalog_artifact_blobs AS artifact_blob "
        "ON artifact_blob.artifact_sha256 = digest.artifact_sha256 "
        "LEFT JOIN catalog_artifact_location AS location "
        "ON location.artifact_sha256 = digest.artifact_sha256"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate_id, publication_key) * len(members),
    )
    return tuple(row)


def load_prepared_artifact_family(
    connector: Any,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> PreparedArtifactFamily | None:
    candidate = require_uuid16(candidate_id, field="prepared candidate_id")
    publication = require_digest32(publication_key, field="prepared publication_key")
    row = _prepared_family_row(
        connector,
        candidate,
        publication,
        backend=backend,
        locking=locking,
    )
    if not row:
        return None
    key_pairs = ((0, 1), (2, 3), (5, 6), (8, 9), (11, 12), (14, 15), (17, 18))
    if len(row) != 21 or any(
        (row[left], row[right]) != (candidate, publication) for left, right in key_pairs
    ):
        raise ArtifactFamilyPartialError("prepared artifact family is partial")
    try:
        family = PreparedArtifactFamily(
            candidate,
            publication,
            row[4],
            row[7],
            row[10],
            row[13],
            row[16],
        )
        size_bytes = require_int63(row[19], field="prepared artifact size_bytes")
        locator = require_digest32(
            row[20],
            field="prepared artifact locator",
        )
        token = identity.decode_artifact_protection_token(family.protection_token)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "prepared artifact family contains invalid facts"
        ) from error
    if token.size_bytes != size_bytes or token.artifact_locator_sha256 != locator:
        raise ArtifactFamilyCollisionError(
            "prepared artifact token disagrees with blob or locator authority"
        )
    return family


def load_prepared_artifact_family_by_token(
    connector: Any,
    *,
    protection_token: bytes,
) -> PreparedArtifactFamily | None:
    token = require_bounded_bytes(
        protection_token,
        field="prepared protection_token",
        minimum=184,
        maximum=184,
    )
    row = connector.fetch_one(
        f"SELECT candidate_id, publication_key FROM {_PREPARED_TOKEN} "
        "WHERE protection_token = %s",
        (token,),
    )
    if not row:
        return None
    if len(row) != 2:
        raise ArtifactFamilyCollisionError("prepared token identity is malformed")
    family = load_prepared_artifact_family(
        connector,
        candidate_id=row[0],
        publication_key=row[1],
    )
    if family is None or family.protection_token != token:
        raise ArtifactFamilyPartialError(
            "prepared token identity has no congruent sealed family"
        )
    return family


def ensure_prepared_artifact_family(
    connector: Any,
    family: PreparedArtifactFamily,
    *,
    backend: str = "sqlite",
) -> tuple[PreparedArtifactFamily, bool]:
    if not isinstance(family, PreparedArtifactFamily):
        raise TypeError("family must be PreparedArtifactFamily")
    if family.state != "PENDING":
        raise ValueError("a new prepared artifact family must begin PENDING")
    _locking_suffix(backend=backend, locking=False)
    existing_key = load_prepared_artifact_family(
        connector,
        candidate_id=family.candidate_id,
        publication_key=family.publication_key,
        backend=backend,
    )
    existing_token = load_prepared_artifact_family_by_token(
        connector,
        protection_token=family.protection_token,
    )
    for existing in (existing_key, existing_token):
        if existing is not None and replace(existing, state="PENDING") != family:
            raise ArtifactFamilyCollisionError(
                "prepared artifact replay collides with different immutable facts"
            )
    if existing_key is not None or existing_token is not None:
        if existing_key != existing_token:
            raise ArtifactFamilyPartialError(
                "prepared artifact key and token visibility differ"
            )
        assert existing_key is not None
        return existing_key, False
    key = (family.candidate_id, family.publication_key)
    try:
        connector.execute(
            f"INSERT INTO {_PREPARED_ANCHOR} (candidate_id, publication_key) "
            "VALUES (%s, %s)",
            key,
        )
    except DatabaseDuplicateKeyError:
        raced = load_prepared_artifact_family(
            connector,
            candidate_id=family.candidate_id,
            publication_key=family.publication_key,
            backend=backend,
            locking=True,
        )
        if raced is None or replace(raced, state="PENDING") != family:
            raise ArtifactFamilyCollisionError(
                "prepared artifact concurrent replay changed exact facts"
            )
        return raced, False
    facts = (
        (_PREPARED_SHA, "artifact_sha256", family.artifact_sha256),
        (
            _PREPARED_CODEC,
            "storage_codec_version",
            family.storage_codec_version,
        ),
        (
            _PREPARED_GENERATION,
            "storage_generation",
            family.storage_generation,
        ),
        (_PREPARED_TOKEN, "protection_token", family.protection_token),
        (_PREPARED_STATE, "state", family.state),
    )
    for table, column, value in facts:
        connector.execute(
            f"INSERT INTO {table} (candidate_id, publication_key, {column}) "
            "VALUES (%s, %s, %s)",
            (*key, value),
        )
    connector.execute(
        f"INSERT INTO {_PREPARED_SEAL} (candidate_id, publication_key) "
        "VALUES (%s, %s)",
        key,
    )
    return family, True


def cas_prepared_artifact_state(
    work: VNextUnitOfWork,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    expected_state: str,
    next_state: str,
) -> PreparedArtifactFamily:
    candidate = require_uuid16(candidate_id, field="prepared candidate_id")
    publication = require_digest32(publication_key, field="prepared publication_key")
    allowed = {("PENDING", "PREPARED"), ("PREPARED", "COMMITTED")}
    if (expected_state, next_state) not in allowed:
        raise ValueError("prepared artifact state transition is not registered")
    current = load_prepared_artifact_family(
        work.connector,
        candidate_id=candidate,
        publication_key=publication,
    )
    if current is None:
        raise ArtifactFamilyPartialError("prepared artifact family is absent")
    if current.state == next_state:
        return current
    if current.state != expected_state:
        raise ArtifactFamilyCollisionError(
            "prepared artifact state does not match transition authority"
        )
    work.compare_and_swap(
        f"UPDATE {_PREPARED_STATE} SET state = %s "
        "WHERE candidate_id = %s AND publication_key = %s AND state = %s",
        (next_state, candidate, publication, expected_state),
        authority="prepared artifact state",
    )
    updated = load_prepared_artifact_family(
        work.connector,
        candidate_id=candidate,
        publication_key=publication,
    )
    if updated is None or updated.state != next_state:
        raise ArtifactFamilyCollisionError(
            "prepared artifact state vanished after compare-and-swap"
        )
    return updated


def _catalog_family_row(
    connector: Any,
    revision: int,
    publication_key: bytes,
    *,
    backend: str = "sqlite",
    locking: bool = False,
) -> tuple[Any, ...]:
    members = (_CATALOG_ANCHOR, _CATALOG_SHA, _CATALOG_SEMANTICS, _CATALOG_SEAL)
    key_union = " UNION ".join(
        f"SELECT revision, publication_key FROM {table} "
        "WHERE revision = %s AND publication_key = %s"
        for table in members
    )
    row = connector.fetch_one(
        "WITH family_keys(revision, publication_key) AS ("
        + key_union
        + ") SELECT anchor.revision, anchor.publication_key, "
        "digest.revision, digest.publication_key, digest.artifact_sha256, "
        "semantics.revision, semantics.publication_key, "
        "semantics.artifact_semantics_sha256, seal.revision, seal.publication_key "
        "FROM family_keys AS family_key "
        f"LEFT JOIN {_CATALOG_ANCHOR} AS anchor USING (revision, publication_key) "
        f"LEFT JOIN {_CATALOG_SHA} AS digest USING (revision, publication_key) "
        f"LEFT JOIN {_CATALOG_SEMANTICS} AS semantics "
        "USING (revision, publication_key) "
        f"LEFT JOIN {_CATALOG_SEAL} AS seal USING (revision, publication_key)"
        + _locking_suffix(backend=backend, locking=locking),
        (revision, publication_key) * len(members),
    )
    return tuple(row)


def load_catalog_artifact_family(
    connector: Any,
    *,
    revision: int,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> CatalogArtifactFamily | None:
    catalog_revision = require_positive_int63(revision, field="catalog revision")
    publication = require_digest32(publication_key, field="catalog publication_key")
    row = _catalog_family_row(
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
        raise ArtifactFamilyPartialError("catalog artifact family is partial")
    try:
        return CatalogArtifactFamily(catalog_revision, publication, row[4], row[7])
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "catalog artifact family contains invalid facts"
        ) from error


def ensure_catalog_artifact_family(
    connector: Any,
    family: CatalogArtifactFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogArtifactFamily, bool]:
    if not isinstance(family, CatalogArtifactFamily):
        raise TypeError("family must be CatalogArtifactFamily")
    _locking_suffix(backend=backend, locking=False)
    existing = load_catalog_artifact_family(
        connector,
        revision=family.revision,
        publication_key=family.publication_key,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise ArtifactFamilyCollisionError(
                "catalog artifact replay changed exact occurrence facts"
            )
        return existing, False
    key = (family.revision, family.publication_key)
    try:
        connector.execute(
            f"INSERT INTO {_CATALOG_ANCHOR} (revision, publication_key) "
            "VALUES (%s, %s)",
            key,
        )
    except DatabaseDuplicateKeyError:
        raced = load_catalog_artifact_family(
            connector,
            revision=family.revision,
            publication_key=family.publication_key,
            backend=backend,
            locking=True,
        )
        if raced != family:
            raise ArtifactFamilyCollisionError(
                "catalog artifact concurrent replay changed exact facts"
            )
        return raced, False
    connector.execute(
        f"INSERT INTO {_CATALOG_SHA} "
        "(revision, publication_key, artifact_sha256) VALUES (%s, %s, %s)",
        (*key, family.artifact_sha256),
    )
    connector.execute(
        f"INSERT INTO {_CATALOG_SEMANTICS} "
        "(revision, publication_key, artifact_semantics_sha256) "
        "VALUES (%s, %s, %s)",
        (*key, family.artifact_semantics_sha256),
    )
    connector.execute(
        f"INSERT INTO {_CATALOG_SEAL} (revision, publication_key) VALUES (%s, %s)",
        key,
    )
    return family, True
