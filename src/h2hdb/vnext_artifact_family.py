"""Exact narrow-family persistence for neutral artifact resources."""

from __future__ import annotations

__all__ = [
    "ArtifactFamilyCollisionError",
    "ArtifactFamilyPartialError",
    "ArtifactSemanticInputFamily",
    "CatalogArtifactFamily",
    "PreparedArtifactFamily",
    "PreparedStorageObjectFamily",
    "cas_prepared_artifact_state",
    "ensure_artifact_semantic_input_family",
    "ensure_catalog_artifact_family",
    "ensure_prepared_artifact_family",
    "ensure_prepared_storage_object_family",
    "load_artifact_semantic_input_family",
    "load_artifact_semantic_input_family_by_identity",
    "load_catalog_artifact_family",
    "load_prepared_artifact_families",
    "load_prepared_artifact_family",
    "load_prepared_artifact_family_by_token",
    "load_prepared_storage_object_family",
]

from dataclasses import dataclass, replace
from typing import Any

from . import vnext_identity as identity
from .domain import ArtifactArchiveRenderEvidence, CatalogResourceKind
from .sql_connector import DatabaseDuplicateKeyError
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_utf8_bytes,
    require_uuid16,
)
from .vnext_state_machine_contract import require_catalog_state_mutation
from .vnext_transaction import VNextUnitOfWork

_SEMANTIC_TABLE = "catalog_artifact_semantic_inputs"
_PREPARED_TABLE = "catalog_prepared_artifacts"
_PREPARED_OBJECT_TABLE = "catalog_prepared_storage_objects"
_CATALOG_TABLE = "catalog_artifacts"


class ArtifactFamilyCollisionError(RuntimeError):
    """A complete artifact family disagrees with durable authority."""


class ArtifactFamilyPartialError(ArtifactFamilyCollisionError):
    """A physical family is absent, incomplete, or only partly visible."""


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
        if identity.artifact_semantics_digest(*self.components) != (
            self.artifact_semantics_sha256
        ):
            raise ValueError("artifact semantic digest does not match its frame")

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
            ("source_manifest_component_sha256", self.source_manifest_component_sha256),
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
    resource_kind: CatalogResourceKind
    storage_object_key_sha256: bytes
    storage_generation: int
    protection_token: bytes
    state: str

    def __post_init__(self) -> None:
        candidate = require_uuid16(self.candidate_id, field="prepared candidate_id")
        publication = require_digest32(
            self.publication_key, field="prepared publication_key"
        )
        if type(self.resource_kind) is not CatalogResourceKind:
            raise TypeError("prepared resource_kind is not registered")
        key_digest = require_digest32(
            self.storage_object_key_sha256,
            field="prepared storage_object_key_sha256",
        )
        generation = require_int63(
            self.storage_generation, field="prepared storage_generation"
        )
        token = require_digest32(
            self.protection_token, field="prepared protection_token"
        )
        expected = identity.encode_artifact_protection_token(
            candidate,
            publication,
            self.resource_kind.value,
            key_digest,
            generation,
        )
        if token != expected:
            raise ValueError("prepared protection token disagrees with exact facts")
        if self.state not in {"PENDING", "PREPARED", "COMMITTED"}:
            raise ValueError("prepared artifact state is not registered")


@dataclass(frozen=True, slots=True)
class PreparedStorageObjectFamily:
    candidate_id: bytes
    publication_key: bytes
    resource_kind: CatalogResourceKind
    storage_object_sha256: bytes
    size_bytes: int
    modified_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="prepared object candidate_id")
        require_digest32(self.publication_key, field="prepared object publication_key")
        if type(self.resource_kind) is not CatalogResourceKind:
            raise TypeError("prepared object resource_kind is not registered")
        require_digest32(
            self.storage_object_sha256,
            field="prepared storage_object_sha256",
        )
        require_positive_int63(
            self.size_bytes, field="prepared storage object size_bytes"
        )
        require_int63(self.modified_at, field="prepared storage object modified_at")


@dataclass(frozen=True, slots=True)
class CatalogArtifactFamily:
    revision: int
    publication_key: bytes
    artifact_sha256: bytes
    artifact_semantics_sha256: bytes
    artifact_name: bytes
    media_type: bytes
    page_count: int

    def __post_init__(self) -> None:
        require_positive_int63(self.revision, field="catalog artifact revision")
        require_digest32(self.publication_key, field="catalog artifact publication_key")
        artifact = require_digest32(
            self.artifact_sha256, field="catalog artifact_sha256"
        )
        require_digest32(
            self.artifact_semantics_sha256,
            field="catalog artifact_semantics_sha256",
        )
        name = require_utf8_bytes(
            self.artifact_name,
            field="catalog artifact_name",
            minimum=1,
            maximum=255,
            reject_nul=True,
        )
        media = require_ascii_bytes(
            self.media_type,
            field="catalog artifact media_type",
            minimum=1,
            maximum=127,
        )
        page_count = require_int63(self.page_count, field="catalog artifact page_count")
        if page_count > 4096:
            raise ValueError("catalog artifact page_count exceeds 4096")
        ArtifactArchiveRenderEvidence(
            artifact,
            1,
            media.decode("ascii"),
            name.decode("utf-8"),
            (),
        )


def _locking_suffix(*, backend: str, locking: bool) -> str:
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError("artifact family backend is not registered")
    return " FOR UPDATE" if locking and backend == "mariadb" else ""


def _resource_kind(value: object) -> CatalogResourceKind:
    raw = require_ascii_bytes(
        value, field="artifact resource_kind", minimum=1, maximum=11
    )
    return CatalogResourceKind(raw.decode("ascii"))


def load_artifact_semantic_input_family(
    connector: Any,
    *,
    artifact_semantics_sha256: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> ArtifactSemanticInputFamily | None:
    digest = require_digest32(
        artifact_semantics_sha256, field="artifact_semantics_sha256"
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
        raise ArtifactFamilyCollisionError("artifact semantic row is malformed")
    try:
        return ArtifactSemanticInputFamily(*row)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "artifact semantic family contains invalid facts"
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
        "source_manifest_component_sha256 = %s AND "
        "member_plan_component_sha256 = %s AND "
        "effective_content_component_sha256 = %s AND "
        "selected_component_sha256 = %s AND owner_component_sha256 = %s AND "
        "policy_component_sha256 = %s",
        exact,
    )
    if not row:
        return None
    try:
        family = ArtifactSemanticInputFamily(*row)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "artifact semantic identity contains invalid facts"
        ) from error
    if family.components != exact:
        raise ArtifactFamilyCollisionError("artifact semantic identity disagrees")
    return family


def ensure_artifact_semantic_input_family(
    connector: Any,
    family: ArtifactSemanticInputFamily,
    *,
    backend: str = "sqlite",
) -> tuple[ArtifactSemanticInputFamily, bool]:
    if not isinstance(family, ArtifactSemanticInputFamily):
        raise TypeError("family must be ArtifactSemanticInputFamily")
    existing_digest = load_artifact_semantic_input_family(
        connector,
        artifact_semantics_sha256=family.artifact_semantics_sha256,
        backend=backend,
    )
    existing_identity = load_artifact_semantic_input_family_by_identity(
        connector, components=family.components
    )
    for existing in (existing_digest, existing_identity):
        if existing is not None and existing != family:
            raise ArtifactFamilyCollisionError(
                "artifact semantic replay collides with different facts"
            )
    if existing_digest is not None or existing_identity is not None:
        if existing_digest != existing_identity:
            raise ArtifactFamilyPartialError(
                "artifact semantic candidate-key visibility differs"
            )
        return family, False
    try:
        connector.execute(
            f"INSERT INTO {_SEMANTIC_TABLE} "
            "(artifact_semantics_sha256, source_manifest_component_sha256, "
            "member_plan_component_sha256, effective_content_component_sha256, "
            "selected_component_sha256, owner_component_sha256, "
            "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (family.artifact_semantics_sha256, *family.components),
        )
    except DatabaseDuplicateKeyError:
        raced = load_artifact_semantic_input_family(
            connector,
            artifact_semantics_sha256=family.artifact_semantics_sha256,
            backend=backend,
            locking=True,
        )
        if raced != family:
            raise ArtifactFamilyCollisionError(
                "artifact semantic concurrent replay changed facts"
            )
        return raced, False
    return family, True


def load_prepared_artifact_family(
    connector: Any,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
    backend: str = "sqlite",
    locking: bool = False,
) -> PreparedArtifactFamily | None:
    candidate = require_uuid16(candidate_id, field="prepared candidate_id")
    publication = require_digest32(publication_key, field="prepared publication_key")
    if type(resource_kind) is not CatalogResourceKind:
        raise TypeError("prepared resource_kind is not registered")
    row = connector.fetch_one(
        f"SELECT candidate_id, publication_key, resource_kind, "
        "storage_object_key_sha256, storage_generation, protection_token, state "
        f"FROM {_PREPARED_TABLE} WHERE candidate_id = %s "
        "AND publication_key = %s AND resource_kind = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate, publication, resource_kind.value.encode("ascii")),
    )
    if not row:
        return None
    try:
        family = PreparedArtifactFamily(
            row[0], row[1], _resource_kind(row[2]), *row[3:]
        )
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "prepared artifact family contains invalid facts"
        ) from error
    if (
        family.candidate_id != candidate
        or family.publication_key != publication
        or family.resource_kind is not resource_kind
    ):
        raise ArtifactFamilyCollisionError("prepared resource lookup is incongruent")
    return family


def load_prepared_artifact_families(
    connector: Any,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    backend: str = "sqlite",
    locking: bool = False,
) -> tuple[PreparedArtifactFamily, ...]:
    candidate = require_uuid16(candidate_id, field="prepared candidate_id")
    publication = require_digest32(publication_key, field="prepared publication_key")
    rows = connector.fetch_all(
        f"SELECT candidate_id, publication_key, resource_kind, "
        "storage_object_key_sha256, storage_generation, protection_token, state "
        f"FROM {_PREPARED_TABLE} WHERE candidate_id = %s "
        "AND publication_key = %s ORDER BY resource_kind"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate, publication),
    )
    result: list[PreparedArtifactFamily] = []
    for row in rows:
        try:
            result.append(
                PreparedArtifactFamily(row[0], row[1], _resource_kind(row[2]), *row[3:])
            )
        except (TypeError, ValueError) as error:
            raise ArtifactFamilyCollisionError(
                "prepared artifact family contains invalid facts"
            ) from error
    coordinates = tuple(item.resource_kind.value for item in result)
    if coordinates != tuple(sorted(set(coordinates))):
        raise ArtifactFamilyCollisionError("prepared resource family is duplicated")
    return tuple(result)


def load_prepared_artifact_family_by_token(
    connector: Any,
    *,
    protection_token: bytes,
    backend: str = "sqlite",
) -> PreparedArtifactFamily | None:
    token = require_digest32(protection_token, field="prepared protection_token")
    row = connector.fetch_one(
        f"SELECT candidate_id, publication_key, resource_kind "
        f"FROM {_PREPARED_TABLE} WHERE protection_token = %s",
        (token,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise ArtifactFamilyCollisionError("prepared token identity is malformed")
    family = load_prepared_artifact_family(
        connector,
        candidate_id=row[0],
        publication_key=row[1],
        resource_kind=_resource_kind(row[2]),
        backend=backend,
    )
    if family is None or family.protection_token != token:
        raise ArtifactFamilyCollisionError("prepared token identity is incongruent")
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
        raise ValueError("a new prepared resource must begin PENDING")
    existing_key = load_prepared_artifact_family(
        connector,
        candidate_id=family.candidate_id,
        publication_key=family.publication_key,
        resource_kind=family.resource_kind,
        backend=backend,
    )
    existing_token = load_prepared_artifact_family_by_token(
        connector,
        protection_token=family.protection_token,
        backend=backend,
    )
    for existing in (existing_key, existing_token):
        if existing is not None and replace(existing, state="PENDING") != family:
            raise ArtifactFamilyCollisionError(
                "prepared resource replay collides with different facts"
            )
    if existing_key is not None or existing_token is not None:
        if existing_key != existing_token:
            raise ArtifactFamilyPartialError(
                "prepared resource key and token visibility differ"
            )
        assert existing_key is not None
        return existing_key, False
    transition = require_catalog_state_mutation(
        "prepared-artifact.initialize",
        previous_state=None,
        next_state=family.state,
        timestamp=None,
    )
    try:
        connector.execute(
            f"INSERT INTO {_PREPARED_TABLE} "
            "(candidate_id, publication_key, resource_kind, "
            "storage_object_key_sha256, storage_generation, protection_token, state) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                family.candidate_id,
                family.publication_key,
                family.resource_kind.value.encode("ascii"),
                family.storage_object_key_sha256,
                family.storage_generation,
                family.protection_token,
                transition.next_state,
            ),
        )
    except DatabaseDuplicateKeyError:
        raced = load_prepared_artifact_family(
            connector,
            candidate_id=family.candidate_id,
            publication_key=family.publication_key,
            resource_kind=family.resource_kind,
            backend=backend,
            locking=True,
        )
        if raced is None or replace(raced, state="PENDING") != family:
            raise ArtifactFamilyCollisionError(
                "prepared resource concurrent replay changed facts"
            )
        return raced, False
    return family, True


def cas_prepared_artifact_state(
    work: VNextUnitOfWork,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
    expected_state: str,
    next_state: str,
) -> PreparedArtifactFamily:
    transition = require_catalog_state_mutation(
        "prepared-artifact.transition",
        previous_state=expected_state,
        next_state=next_state,
        timestamp=None,
    )
    current = load_prepared_artifact_family(
        work.connector,
        candidate_id=candidate_id,
        publication_key=publication_key,
        resource_kind=resource_kind,
        backend=work.backend,
        locking=True,
    )
    if current is None:
        raise ArtifactFamilyPartialError("prepared resource family is absent")
    if current.state == transition.next_state:
        return current
    if current.state != transition.previous_state:
        raise ArtifactFamilyCollisionError(
            "prepared resource state does not match transition authority"
        )
    work.compare_and_swap(
        f"UPDATE {_PREPARED_TABLE} SET state = %s WHERE candidate_id = %s "
        "AND publication_key = %s AND resource_kind = %s AND state = %s",
        (
            transition.next_state,
            current.candidate_id,
            current.publication_key,
            current.resource_kind.value.encode("ascii"),
            transition.previous_state,
        ),
        authority="prepared resource state",
    )
    updated = load_prepared_artifact_family(
        work.connector,
        candidate_id=current.candidate_id,
        publication_key=current.publication_key,
        resource_kind=current.resource_kind,
        backend=work.backend,
    )
    if updated is None or updated.state != transition.next_state:
        raise ArtifactFamilyCollisionError(
            "prepared resource vanished after compare-and-swap"
        )
    return updated


def load_prepared_storage_object_family(
    connector: Any,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
    backend: str = "sqlite",
    locking: bool = False,
) -> PreparedStorageObjectFamily | None:
    candidate = require_uuid16(candidate_id, field="prepared object candidate_id")
    publication = require_digest32(
        publication_key, field="prepared object publication_key"
    )
    row = connector.fetch_one(
        f"SELECT candidate_id, publication_key, resource_kind, "
        "storage_object_sha256, size_bytes, modified_at "
        f"FROM {_PREPARED_OBJECT_TABLE} WHERE candidate_id = %s "
        "AND publication_key = %s AND resource_kind = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (candidate, publication, resource_kind.value.encode("ascii")),
    )
    if not row:
        return None
    try:
        family = PreparedStorageObjectFamily(
            row[0], row[1], _resource_kind(row[2]), *row[3:]
        )
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "prepared storage object contains invalid facts"
        ) from error
    if (
        family.candidate_id != candidate
        or family.publication_key != publication
        or family.resource_kind is not resource_kind
    ):
        raise ArtifactFamilyCollisionError("prepared object lookup is incongruent")
    return family


def ensure_prepared_storage_object_family(
    connector: Any,
    family: PreparedStorageObjectFamily,
    *,
    backend: str = "sqlite",
) -> tuple[PreparedStorageObjectFamily, bool]:
    if not isinstance(family, PreparedStorageObjectFamily):
        raise TypeError("family must be PreparedStorageObjectFamily")
    existing = load_prepared_storage_object_family(
        connector,
        candidate_id=family.candidate_id,
        publication_key=family.publication_key,
        resource_kind=family.resource_kind,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise ArtifactFamilyCollisionError(
                "prepared storage object replay changed facts"
            )
        return existing, False
    try:
        connector.execute(
            f"INSERT INTO {_PREPARED_OBJECT_TABLE} "
            "(candidate_id, publication_key, resource_kind, "
            "storage_object_sha256, size_bytes, modified_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                family.candidate_id,
                family.publication_key,
                family.resource_kind.value.encode("ascii"),
                family.storage_object_sha256,
                family.size_bytes,
                family.modified_at,
            ),
        )
    except DatabaseDuplicateKeyError:
        raced = load_prepared_storage_object_family(
            connector,
            candidate_id=family.candidate_id,
            publication_key=family.publication_key,
            resource_kind=family.resource_kind,
            backend=backend,
            locking=True,
        )
        if raced != family:
            raise ArtifactFamilyCollisionError(
                "prepared storage object concurrent replay changed facts"
            )
        return raced, False
    return family, True


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
        "artifact_semantics_sha256, artifact_name, media_type, page_count "
        f"FROM {_CATALOG_TABLE} WHERE revision = %s AND publication_key = %s"
        + _locking_suffix(backend=backend, locking=locking),
        (catalog_revision, publication),
    )
    if not row:
        return None
    try:
        family = CatalogArtifactFamily(*row)
    except (TypeError, ValueError) as error:
        raise ArtifactFamilyCollisionError(
            "catalog artifact family contains invalid facts"
        ) from error
    if family.revision != catalog_revision or family.publication_key != publication:
        raise ArtifactFamilyCollisionError("catalog artifact lookup is incongruent")
    return family


def ensure_catalog_artifact_family(
    connector: Any,
    family: CatalogArtifactFamily,
    *,
    backend: str = "sqlite",
) -> tuple[CatalogArtifactFamily, bool]:
    if not isinstance(family, CatalogArtifactFamily):
        raise TypeError("family must be CatalogArtifactFamily")
    existing = load_catalog_artifact_family(
        connector,
        revision=family.revision,
        publication_key=family.publication_key,
        backend=backend,
    )
    if existing is not None:
        if existing != family:
            raise ArtifactFamilyCollisionError(
                "catalog artifact replay changed occurrence facts"
            )
        return existing, False
    try:
        connector.execute(
            f"INSERT INTO {_CATALOG_TABLE} "
            "(revision, publication_key, artifact_sha256, "
            "artifact_semantics_sha256, artifact_name, media_type, page_count) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                family.revision,
                family.publication_key,
                family.artifact_sha256,
                family.artifact_semantics_sha256,
                family.artifact_name,
                family.media_type,
                family.page_count,
            ),
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
                "catalog artifact concurrent replay changed facts"
            )
        return raced, False
    return family, True
