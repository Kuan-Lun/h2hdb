"""Shared exact seed helpers for publication-owned immutable families.

These helpers deliberately use the production family protocols.  Tests that
need malformed or partial state must insert the selected physical member
directly so the corruption remains explicit at the callsite.
"""

from __future__ import annotations

from h2hdb import vnext_identity as identity
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_publication_family import (
    CatalogContributorFamily,
    CatalogPublicationDownloadTimeFamily,
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationCandidateFamily,
    PublicationIdentityFamily,
    ensure_catalog_contributor_family,
    ensure_catalog_publication_download_time_family,
    ensure_catalog_publication_family,
    ensure_catalog_publication_title_family,
    ensure_publication_candidate_family,
    ensure_publication_identity_family,
)
from h2hdb.vnext_publication_finalization_repository import (
    _initialize_finalization_checkpoint,
)


def seed_publication_candidate(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    analysis_id: bytes,
    reserved_revision: int,
    artifact_policy_id: int,
    display_title_policy_id: int,
    artifacts_required: bool,
    created_at: int,
    backend: str = "sqlite",
) -> PublicationCandidateFamily:
    family = PublicationCandidateFamily(
        candidate_id,
        analysis_id,
        reserved_revision,
        artifact_policy_id,
        display_title_policy_id,
        artifacts_required,
        created_at,
    )
    ensure_publication_candidate_family(connector, family, backend=backend)
    return family


def seed_publication_projection_certification(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
) -> None:
    existing = connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (candidate_id,),
    )
    if existing:
        assert existing == (candidate_id,)
        return
    connector.execute(
        "INSERT INTO catalog_publication_candidate_projection_seals "
        "(candidate_id) VALUES (%s)",
        (candidate_id,),
    )


def seed_publication_finalization_checkpoint(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    updated_at: int,
) -> None:
    """Seed the mandatory OPEN permanent-finalization family, seal last."""
    _initialize_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
        initialized_at=updated_at,
    )


def seed_publication_finalization(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    cursor: bytes,
    processed_count: int,
    finalized_at: int,
) -> None:
    """Complete an existing commit's exact terminal finalization family."""

    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_stored "
        "(receipt_id, start_generation, batch_key, start_cursor, "
        "start_processed_count, next_cursor, row_count, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            receipt_id,
            1,
            b"terminal",
            cursor,
            processed_count,
            cursor,
            0,
            finalized_at,
        ),
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoints "
        "SET generation = %s, `cursor` = %s, processed_count = %s, "
        "state = %s, updated_at = %s WHERE receipt_id = %s",
        (2, cursor, processed_count, "COMPLETE", finalized_at, receipt_id),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def seed_publication_commit(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    revision: int,
    source_revision: int,
    generation: int,
    preparation_id: bytes,
    operational_policy_id: int,
    artifact_policy_id: int,
    display_title_policy_id: int,
    new_galleries: int,
    changed_galleries: int,
    removed_galleries: int,
    duplicate_losers: int,
    committed_at: int,
    channel: bytes | None = None,
) -> None:
    """Seed one immutable wide commit after its prerequisite checkpoint."""

    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    seed_publication_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
        updated_at=committed_at,
    )
    connector.execute(
        "INSERT INTO catalog_publication_commits "
        "(receipt_id, candidate_id, revision, source_revision, generation, "
        "preparation_id, operational_policy_id, artifact_policy_id, "
        "display_title_policy_id, new_galleries, changed_galleries, "
        "removed_galleries, duplicate_losers, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            receipt_id,
            candidate_id,
            revision,
            source_revision,
            generation,
            preparation_id,
            operational_policy_id,
            artifact_policy_id,
            display_title_policy_id,
            new_galleries,
            changed_galleries,
            removed_galleries,
            duplicate_losers,
            committed_at,
        ),
    )
    if channel is not None:
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (channel, receipt_id),
        )


def seed_publication_identity(
    connector: SQLConnector,
    *,
    gid: int,
    backend: str = "sqlite",
) -> PublicationIdentityFamily:
    family = PublicationIdentityFamily(identity.publication_key(gid), gid)
    ensure_publication_identity_family(connector, family, backend=backend)
    return family


def seed_catalog_publication(
    connector: SQLConnector,
    *,
    revision: int,
    publication_key: bytes,
    gallery_id: int,
    summary_sha256: bytes,
    language_sha256: bytes,
    modified_at: int,
    source_title_sha256: bytes,
    download_time: int = 0,
    backend: str = "sqlite",
) -> CatalogPublicationFamily:
    family = CatalogPublicationFamily(
        revision,
        publication_key,
        gallery_id,
        summary_sha256,
        language_sha256,
        modified_at,
        source_title_sha256,
    )
    ensure_catalog_publication_family(connector, family, backend=backend)
    ensure_catalog_publication_download_time_family(
        connector,
        CatalogPublicationDownloadTimeFamily(
            revision,
            publication_key,
            download_time,
        ),
        backend=backend,
    )
    return family


def seed_catalog_publication_title(
    connector: SQLConnector,
    *,
    revision: int,
    publication_key: bytes,
    source_title_sha256: bytes,
    source_gallery_name: bytes,
    backend: str = "sqlite",
) -> CatalogPublicationTitleFamily:
    family = CatalogPublicationTitleFamily(
        revision,
        publication_key,
        source_title_sha256,
        source_gallery_name,
    )
    ensure_catalog_publication_title_family(connector, family, backend=backend)
    return family


def seed_catalog_contributor(
    connector: SQLConnector,
    *,
    revision: int,
    publication_key: bytes,
    position: int,
    contributor_name_sha256: bytes,
    role: bytes,
    backend: str = "sqlite",
) -> CatalogContributorFamily:
    family = CatalogContributorFamily(
        revision,
        publication_key,
        position,
        contributor_name_sha256,
        role,
    )
    ensure_catalog_contributor_family(connector, family, backend=backend)
    return family


def clone_catalog_publication_families(
    connector: SQLConnector,
    *,
    source_revision: int,
    target_revision: int,
    backend: str = "sqlite",
) -> None:
    """Clone exact scalar/title/contributor families into a seeded revision."""

    publication_rows = connector.fetch_all(
        "SELECT publication.publication_key, publication.gallery_id, "
        "publication.summary_sha256, publication.language_sha256, "
        "publication.modified_at, publication.download_time, "
        "title.source_title_sha256 "
        "FROM catalog_publications AS publication "
        "JOIN catalog_publication_titles AS title "
        "ON title.revision = publication.revision "
        "AND title.publication_key = publication.publication_key "
        "WHERE publication.revision = %s ORDER BY publication.publication_key",
        (source_revision,),
    )
    for (
        publication_key,
        gallery_id,
        summary,
        language,
        modified_at,
        download_time,
        source_title,
    ) in publication_rows:
        seed_catalog_publication(
            connector,
            revision=target_revision,
            publication_key=publication_key,
            gallery_id=gallery_id,
            summary_sha256=summary,
            language_sha256=language,
            modified_at=modified_at,
            source_title_sha256=source_title,
            download_time=download_time,
            backend=backend,
        )
    title_rows = connector.fetch_all(
        "SELECT publication_key, source_title_sha256, source_gallery_name "
        "FROM catalog_publication_titles WHERE revision = %s "
        "ORDER BY publication_key",
        (source_revision,),
    )
    for publication_key, source_title, source_gallery_name in title_rows:
        seed_catalog_publication_title(
            connector,
            revision=target_revision,
            publication_key=publication_key,
            source_title_sha256=source_title,
            source_gallery_name=source_gallery_name,
            backend=backend,
        )
    contributor_rows = connector.fetch_all(
        "SELECT publication_key, position, contributor_name_sha256, role "
        "FROM catalog_contributors WHERE revision = %s "
        "ORDER BY publication_key, position",
        (source_revision,),
    )
    for publication_key, position, contributor_name, role in contributor_rows:
        seed_catalog_contributor(
            connector,
            revision=target_revision,
            publication_key=publication_key,
            position=position,
            contributor_name_sha256=contributor_name,
            role=role,
            backend=backend,
        )
