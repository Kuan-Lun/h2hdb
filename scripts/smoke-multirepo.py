#!/usr/bin/env python3
"""Exercise the greenfield epoch and public consumers in one SQLite slice."""

from __future__ import annotations

import asyncio
import importlib.metadata
import sqlite3
import tempfile
from pathlib import Path

import h2h_galleryinfo_parser
import h2hdb_downloader
import h2hdb_ingest
import h2hdb_komga
import hbrowser
from fastapi import FastAPI
from h2hdb_opds import OPDSConfig, create_app
from httpx import ASGITransport, AsyncClient

from h2hdb import (
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseAccessMode,
    DatabaseConfig,
    VNextDatabaseAdminFacade,
    VNextDownloadQueueFacade,
    open_database,
)


async def _exercise_empty_opds(application: FastAPI) -> None:
    transport = ASGITransport(app=application)
    async with AsyncClient(
        transport=transport,
        base_url="http://integration.test",
    ) as client:
        health = await client.get("/health")
        assert health.status_code == 200
        feed = await client.get("/opds/v2/publications")
        assert feed.status_code == 404
        assert feed.json() == {"detail": "Catalog revision current not found"}


def _assert_no_legacy_migration_ledger(database_path: Path) -> None:
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'h2hdb_schema_migrations'"
        ).fetchone()
    assert row is None


def _assert_wide_bcnf_recompositions(database_path: Path) -> None:
    required = {
        "catalog_analysis_batch_receipt_stored",
        "catalog_analysis_checkpoints",
        "catalog_gallery_identities",
        "catalog_analysis_policies",
        "catalog_analysis_run_descriptor",
        "catalog_analysis_stages",
        "catalog_analysis_state_component_seals",
        "catalog_artifact_semantic_inputs",
        "catalog_artifact_policy_semantics",
        "catalog_artifact_producer_fingerprints",
        "catalog_artifact_storage_codecs",
        "catalog_artifact_zip_writer_policies",
        "catalog_prepared_artifacts",
        "catalog_artifacts",
        "catalog_artifact_blobs",
        "catalog_build_manifest_core",
        "catalog_display_title_policies",
        "catalog_gallery_observation_metadata_locals",
        "catalog_gallery_observation_scans",
        "catalog_gallery_observation_directories",
        "catalog_gallery_observation_stat",
        "catalog_gallery_manifests",
        "catalog_manifest_policies",
        "catalog_publications",
        "catalog_publication_titles",
        "catalog_publication_batch_receipt_stored",
        "catalog_publication_candidates",
        "catalog_publication_checkpoints",
        "catalog_publication_commits",
        "catalog_publication_finalization_batch_stored",
        "catalog_publication_finalization_checkpoints",
        "catalog_publication_stages",
        "catalog_analysis_content_owner_candidate_shadows",
        "catalog_analysis_content_owner_shadows",
        "catalog_analysis_impacted_content",
        "catalog_analysis_impacted_gid",
        "catalog_source_build_descriptor",
        "catalog_source_build_discoveries",
        "catalog_source_revision_descriptors",
        "catalog_source_scopes",
        "catalog_source_snapshot_manifest_identity",
        "catalog_title_sort_policy",
    }
    removed = {
        "catalog_gallery_identity_anchors",
        "catalog_gallery_identity_coordinates",
        "catalog_gallery_identity_gallery_keys",
        "catalog_gallery_identity_seals",
        "catalog_artifact_semantic_input_anchors",
        "catalog_artifact_semantic_source_manifest_sha256s",
        "catalog_artifact_semantic_member_plan_sha256s",
        "catalog_artifact_semantic_effective_content_sha256s",
        "catalog_artifact_semantic_selected_sha256s",
        "catalog_artifact_semantic_owner_sha256s",
        "catalog_artifact_semantic_policy_sha256s",
        "catalog_artifact_semantic_input_identities",
        "catalog_artifact_semantic_input_seals",
        "catalog_prepared_artifact_anchors",
        "catalog_prepared_artifact_sha256s",
        "catalog_prepared_artifact_storage_codec_versions",
        "catalog_prepared_artifact_storage_generations",
        "catalog_prepared_artifact_protection_tokens",
        "catalog_prepared_artifact_states",
        "catalog_prepared_artifact_seals",
        "catalog_artifact_anchors",
        "catalog_artifact_sha256s",
        "catalog_artifact_semantics_sha256s",
        "catalog_artifact_seals",
        "catalog_artifact_location",
        "catalog_gallery_observation_metadata_anchors",
        "catalog_gallery_observation_metadata_seals",
        "catalog_gallery_observation_download_times",
        "catalog_gallery_observation_modified_times",
        "catalog_gallery_observation_scan_anchors",
        "catalog_gallery_observation_scan_observation_sha256s",
        "catalog_gallery_observation_scan_observation_versions",
        "catalog_gallery_observation_scan_source_file_counts",
        "catalog_gallery_observation_scan_seals",
        "catalog_gallery_observation_directory_anchors",
        "catalog_gallery_observation_directory_entry_counts",
        "catalog_gallery_observation_directory_observation_sha256s",
        "catalog_gallery_observation_directory_seals",
        "catalog_gallery_observation_stat_anchors",
        "catalog_gallery_observation_stat_file_counts",
        "catalog_gallery_observation_stat_byte_counts",
        "catalog_gallery_observation_stat_seals",
        "catalog_gallery_manifest_anchors",
        "catalog_gallery_manifest_manifest_sha256s",
        "catalog_gallery_manifest_computed_ats",
        "catalog_gallery_manifest_seals",
        "catalog_publication_anchors",
        "catalog_publication_gallery_ids",
        "catalog_publication_summary_sha256s",
        "catalog_publication_language_sha256s",
        "catalog_publication_modified_ats",
        "catalog_publication_seals",
        "catalog_publication_title_anchors",
        "catalog_publication_title_source_title_sha256s",
        "catalog_publication_title_source_gallery_names",
        "catalog_publication_title_seals",
        "catalog_a_content_candidate_shadow_anchors",
        "catalog_a_content_candidate_shadow_contents",
        "catalog_a_content_candidate_shadow_not_uploaded",
        "catalog_a_content_candidate_shadow_title_counts",
        "catalog_a_content_candidate_shadow_download_times",
        "catalog_a_content_candidate_shadow_seals",
        "catalog_a_content_owner_shadow_anchors",
        "catalog_a_content_owner_shadow_galleries",
        "catalog_a_content_owner_shadow_seals",
        "catalog_a_impacted_content_anchors",
        "catalog_a_impacted_content_witnesses",
        "catalog_a_impacted_content_seals",
        "catalog_a_impacted_gid_anchors",
        "catalog_a_impacted_gid_witnesses",
        "catalog_a_impacted_gid_seals",
        "operational_operational_consumers",
        "operational_operational_event_acks",
        "operational_operational_event_ack_heads",
        "operational_removed_gid_acks",
    }
    with sqlite3.connect(database_path) as connection:
        relation_types = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }
    assert sum(kind == "table" for kind in relation_types.values()) == 230
    assert sum(kind == "view" for kind in relation_types.values()) == 50
    assert required <= relation_types.keys()
    assert removed.isdisjoint(relation_types)
    assert relation_types["catalog_analysis_impacted_gid"] == "table"


def main() -> None:
    distributions = (
        "h2hdb",
        "h2h-galleryinfo-parser",
        "h2hdb-downloader",
        "h2hdb-ingest",
        "h2hdb-komga",
        "h2hdb-opds",
        "hbrowser",
    )
    resolved = ", ".join(
        f"{name}=={importlib.metadata.version(name)}" for name in distributions
    )
    print(f"resolved integration distributions: {resolved}")

    with tempfile.TemporaryDirectory(prefix="h2hdb-multirepo-") as temporary:
        root = Path(temporary).resolve()
        database_path = root / "catalog.sqlite3"
        writer_config = CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(database_path),
            )
        )

        admin = VNextDatabaseAdminFacade(writer_config)
        initialized = admin.initialize()
        assert initialized.state == "READY"
        replayed = admin.initialize()
        checked = admin.check()
        assert replayed.state == checked.state == "READY"
        assert replayed.manifest_sha256 == initialized.manifest_sha256
        assert checked.manifest_sha256 == initialized.manifest_sha256
        readiness = admin.check_readiness()
        assert readiness.state == "READY"
        _assert_no_legacy_migration_ledger(database_path)
        _assert_wide_bcnf_recompositions(database_path)

        queue = VNextDownloadQueueFacade(writer_config, clock=lambda: 1)
        request = queue.request_download(101, "https://example.invalid/g/101")
        assert queue.get_download_request(101) == request
        assert queue.list_download_requests() == (request,)
        assert queue.complete_download_request(request)
        assert queue.list_download_requests() == ()

        opds_config = OPDSConfig(
            core=writer_config,
            artifact_root=root / "artifacts",
            public_base_url="http://integration.test",
        )
        assert opds_config.core.database.access_mode is DatabaseAccessMode.read_only
        reader = open_database(opds_config.core)
        try:
            reader.get_catalog_revision()
        except CatalogRevisionNotFoundError:
            pass
        else:
            raise AssertionError("fresh catalog unexpectedly has a current revision")

        asyncio.run(_exercise_empty_opds(create_app(opds_config, catalog=reader)))

        # Imports prove that every independently installed consumer resolves
        # the same public core package without a retired compatibility API.
        assert h2h_galleryinfo_parser is not None
        assert h2hdb_downloader is not None
        assert h2hdb_ingest is not None
        assert h2hdb_komga is not None
        assert hbrowser is not None

    print("multi-repo greenfield epoch and public-consumer smoke: ok")


if __name__ == "__main__":
    main()
