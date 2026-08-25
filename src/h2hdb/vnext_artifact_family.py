"""Exact physical storage protocols for the wide BCNF artifact relations.

Artifact semantic inputs, prepared artifacts, and catalog occurrences are each
stored as one authoritative row.  Their candidate keys are enforced by the
database, inserts are atomic, and the only mutable attribute is the prepared
artifact state guarded by an exact compare-and-swap.
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

_SEMANTIC_TABLE = "catalog_artifact_semantic_inputs"
_PREPARED_TABLE = "catalog_prepared_artifacts"
_CATALOG_TABLE = "catalog_artifacts"


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
    row = connector.fetch_one(
        f"SELECT artifact_semantics_sha256, source_manifest_component_sha256, "
        "member_plan_component_sha256, effective_content_component_sha256, "
        "selected_component_sha256, owner_component_sha256, "
        f"policy_component_sha256 FROM {_SEMANTIC_TABLE} "
        "WHERE artifact_semantics_sha256 = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (digest,),
    )
    if not row:
        return None
    if len(row) != 7 or row[0] != digest:
        raise ArtifactFamilyCollisionError("artifact semantic input row is malformed")
    try:
        return ArtifactSemanticInputFamily(*row)
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
        f"SELECT artifact_semantics_sha256, source_manifest_component_sha256, "
        "member_plan_component_sha256, effective_content_component_sha256, "
        "selected_component_sha256, owner_component_sha256, "
        f"policy_component_sha256 FROM {_SEMANTIC_TABLE} WHERE "
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
    if len(row) != 7:
        raise ArtifactFamilyCollisionError("artifact semantic identity is malformed")
    try:
        family = ArtifactSemanticInputFamily(*row)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "artifact semantic input row contains invalid facts"
        ) from error
    if family.components != exact:
        raise ArtifactFamilyCollisionError("artifact semantic identity is incongruent")
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
            f"INSERT INTO {_SEMANTIC_TABLE} "
            "(artifact_semantics_sha256, source_manifest_component_sha256, "
            "member_plan_component_sha256, effective_content_component_sha256, "
            "selected_component_sha256, owner_component_sha256, "
            "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (digest, *family.components),
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
    return family, True


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
    row = connector.fetch_one(
        f"SELECT prepared.candidate_id, prepared.publication_key, "
        "prepared.artifact_sha256, prepared.storage_codec_version, "
        "prepared.storage_generation, prepared.protection_token, prepared.state, "
        "artifact_blob.size_bytes, artifact_blob.artifact_locator_sha256 "
        f"FROM {_PREPARED_TABLE} AS prepared "
        "LEFT JOIN catalog_artifact_blobs AS artifact_blob "
        "ON artifact_blob.artifact_sha256 = prepared.artifact_sha256 "
        "WHERE prepared.candidate_id = %s AND prepared.publication_key = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate, publication),
    )
    if not row:
        return None
    if len(row) != 9 or tuple(row[:2]) != (candidate, publication):
        raise ArtifactFamilyCollisionError("prepared artifact row is malformed")
    try:
        family = PreparedArtifactFamily(*row[:7])
        size_bytes = require_int63(row[7], field="prepared artifact size_bytes")
        locator = require_digest32(
            row[8],
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
        f"SELECT candidate_id, publication_key "
        f"FROM {_PREPARED_TABLE} WHERE protection_token = %s",
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
        raise ArtifactFamilyCollisionError(
            "prepared token identity has no congruent storage authority"
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
            f"INSERT INTO {_PREPARED_TABLE} "
            "(candidate_id, publication_key, artifact_sha256, "
            "storage_codec_version, storage_generation, protection_token, state) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                *key,
                family.artifact_sha256,
                family.storage_codec_version,
                family.storage_generation,
                family.protection_token,
                family.state,
            ),
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
        f"UPDATE {_PREPARED_TABLE} SET state = %s "
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
    row = connector.fetch_one(
        f"SELECT revision, publication_key, artifact_sha256, "
        f"artifact_semantics_sha256 FROM {_CATALOG_TABLE} "
        "WHERE revision = %s AND publication_key = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (catalog_revision, publication),
    )
    if not row:
        return None
    if len(row) != 4 or tuple(row[:2]) != (catalog_revision, publication):
        raise ArtifactFamilyCollisionError("catalog artifact row is malformed")
    try:
        return CatalogArtifactFamily(*row)
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
            f"INSERT INTO {_CATALOG_TABLE} "
            "(revision, publication_key, artifact_sha256, "
            "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
            (*key, family.artifact_sha256, family.artifact_semantics_sha256),
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
    return family, True
