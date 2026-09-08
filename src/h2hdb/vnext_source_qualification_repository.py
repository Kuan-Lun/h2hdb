"""Atomic normalized qualification facts sealed by the metadata byte stream."""

from __future__ import annotations

from .sql_connector import SQLConnector
from .vnext_identity import (
    GalleryObservationMetadataScalarReceipt,
    VNextSourceQualification,
)

_TABLES = (
    ("catalog_gallery_observation_validation_policies", "qualification_policy_sha256"),
    ("catalog_gallery_observation_validation_dispositions", "accepted"),
    ("catalog_gallery_observation_validation_reasons", "qualification_reason"),
    ("catalog_gallery_observation_validation_sources", "qualification_source_name"),
)


class SourceQualificationConflictError(ValueError):
    """Qualification is incomplete or differs from its canonical observation."""


def _facts(
    receipt: GalleryObservationMetadataScalarReceipt,
) -> tuple[bytes, int, bytes | None, bytes | None]:
    receipt.__post_init__()
    qualification = receipt.qualification
    return (
        receipt.qualification_policy_sha256,
        int(qualification.accepted),
        None
        if qualification.reason_code is None
        else qualification.reason_code.encode("ascii"),
        qualification.source_name,
    )


def persist_source_qualification(
    connector: SQLConnector,
    gallery_id: int,
    observation_id: int,
    receipt: GalleryObservationMetadataScalarReceipt,
) -> None:
    """Insert the complete scalar family in its caller-owned metadata transaction."""

    key = (gallery_id, observation_id)
    for (table, column), value in zip(_TABLES, _facts(receipt), strict=True):
        row = connector.fetch_one(
            f"SELECT {column} FROM {table} WHERE gallery_id = %s AND observation_id = %s",
            key,
        )
        if value is None:
            if row:
                raise SourceQualificationConflictError(
                    "accepted source has rejection facts"
                )
        elif row:
            if row != (value,):
                raise SourceQualificationConflictError(
                    "source qualification scalar changed"
                )
        else:
            connector.execute(
                f"INSERT INTO {table} (gallery_id, observation_id, {column}) VALUES (%s, %s, %s)",
                (*key, value),
            )


def require_source_qualification(
    connector: SQLConnector,
    gallery_id: int,
    observation_id: int,
    receipt: GalleryObservationMetadataScalarReceipt,
) -> None:
    """Independently compare all decoded scalars and the rejected FILE authority."""

    key = (gallery_id, observation_id)
    for (table, column), value in zip(_TABLES, _facts(receipt), strict=True):
        row = connector.fetch_one(
            f"SELECT {column} FROM {table} WHERE gallery_id = %s AND observation_id = %s",
            key,
        )
        if row != (() if value is None else (value,)):
            raise SourceQualificationConflictError(
                "source qualification differs from canonical metadata"
            )
    qualification: VNextSourceQualification = receipt.qualification
    if qualification.source_name is not None:
        source = connector.fetch_one(
            "SELECT source.artifact_role FROM catalog_gallery_observation_file_artifact_role AS source "
            "JOIN catalog_gallery_observation_file_seals AS sealed "
            "ON sealed.gallery_id = source.gallery_id AND sealed.observation_id = source.observation_id "
            "AND sealed.file_key = source.file_key "
            "JOIN catalog_file_name_identities AS name ON name.file_key = source.file_key "
            "WHERE source.gallery_id = %s AND source.observation_id = %s AND name.name_bytes = %s",
            (*key, qualification.source_name),
        )
        if source != (b"page",):
            raise SourceQualificationConflictError(
                "rejection does not identify one observed PAGE"
            )
