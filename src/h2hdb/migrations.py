__all__ = [
    "LATEST_SCHEMA_VERSION",
    "MINIMUM_SCHEMA_VERSION",
    "MigrationRunner",
    "SchemaCompatibilityError",
]

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

from .catalog_repository import CatalogProjectionRepository
from .domain import SchemaCompatibility
from .gallery_deduplication import H2HDBGalleryDeduplication
from .repository import RepositoryContext
from .sql_connector import SQLConnector
from .table_comments import H2HDBGalleriesComments
from .table_database_maintenance import H2HDBDatabaseMaintenance
from .table_database_setting import H2HDBCheckDatabaseSettings
from .table_files_dbids import H2HDBFiles
from .table_gallery_ingest_coordination import H2HDBGalleryIngestCoordination
from .table_gallery_source_manifests import H2HDBGallerySourceManifests
from .table_gids import H2HDBGalleriesGIDs, H2HDBGalleriesIDs
from .table_removed_gids import H2HDBRemovedGalleries
from .table_tags import H2HDBGalleriesTags
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles
from .table_uploadaccounts import H2HDBUploadAccounts
from .todelete_queue import H2HDBToDeleteQueue
from .todownload_queue import H2HDBToDownloadQueue
from .view_ginfo import H2HDBGalleriesInfos

SCHEMA_MIGRATIONS_TABLE = "h2hdb_schema_migrations"


@dataclass(frozen=True, slots=True)
class _SchemaMigration:
    version: int
    name: str


# This is the fresh-database schema baseline. Append future forward-only
# migrations here; do not teach the runtime how to adopt an older or
# unversioned schema.
_MIGRATION_REGISTRY = (
    _SchemaMigration(1, "current-schema-baseline"),
    _SchemaMigration(2, "durable-catalog-source-builds"),
    _SchemaMigration(3, "durable-catalog-build-analysis"),
    _SchemaMigration(4, "durable-catalog-build-projections"),
    _SchemaMigration(5, "catalog-operational-authority"),
    _SchemaMigration(6, "active-source-deletion-command-view"),
    _SchemaMigration(7, "durable-file-spam-scan"),
)
LATEST_SCHEMA_VERSION = _MIGRATION_REGISTRY[-1].version
MINIMUM_SCHEMA_VERSION = LATEST_SCHEMA_VERSION

_V1_REQUIRED_TABLES = frozenset(
    {
        "catalog_artifacts",
        "catalog_contributors",
        "catalog_publications",
        "catalog_revision",
        "catalog_revision_history",
        "catalog_subjects",
        "database_maintenance_state",
        "files_dbids",
        "files_hashs_sha256",
        "files_hashs_sha256_dbids",
        "files_names",
        "galleries_access_times",
        "galleries_comments",
        "galleries_dbids",
        "galleries_download_times",
        "galleries_gids",
        "galleries_modified_times",
        "galleries_names",
        "galleries_redownload_times",
        "galleries_tag_pairs_dbids",
        "galleries_tags",
        "galleries_tags_names",
        "galleries_tags_values",
        "galleries_titles",
        "galleries_upload_accounts",
        "galleries_upload_times",
        "gallery_content_hashes",
        "gallery_duplicate_warnings",
        "gallery_ingest_state",
        "gallery_source_manifests",
        "removed_galleries_gids",
        "todelete_galleries",
        "todelete_gids",
        "todownload_gids",
    }
)

_V1_REQUIRED_VIEWS = frozenset(
    {
        "duplicate_hash_in_gallery",
        "files_hashs",
        "galleries_infos",
        "gallery_duplicate_warnings_names",
        "pending_download_gids",
        "todelete_gallery_candidates",
        "todelete_rm_commands",
    }
)

_V1_REQUIRED_COLUMNS = {
    "catalog_artifacts": frozenset(
        {
            "revision",
            "artifact_key",
            "artifact_name_key",
            "publication_key",
            "artifact_id",
            "name",
            "location",
            "media_type",
            "size_bytes",
            "sha256",
            "modified_at",
        }
    ),
    "catalog_contributors": frozenset(
        {"revision", "publication_key", "position", "name", "role", "sort_as"}
    ),
    "catalog_publications": frozenset(
        {
            "revision",
            "publication_key",
            "publication_id",
            "gid",
            "title",
            "source_title",
            "source_gallery_name",
            "content_sha256",
            "sort_title",
            "summary",
            "language",
            "published_at",
            "modified_at",
            "redownload_required",
        }
    ),
    "catalog_revision": frozenset(
        {"singleton_id", "current_revision", "published_at", "publication_count"}
    ),
    "catalog_revision_history": frozenset(
        {"revision", "published_at", "publication_count"}
    ),
    "catalog_subjects": frozenset(
        {"revision", "publication_key", "position", "name", "scheme", "code"}
    ),
    "database_maintenance_state": frozenset(
        {
            "state_id",
            "accumulated_work",
            "last_evaluated_at",
            "last_optimized_at",
        }
    ),
    "files_dbids": frozenset({"db_file_id", "db_gallery_id"}),
    "files_hashs_sha256": frozenset({"db_file_id", "db_hash_id"}),
    "files_hashs_sha256_dbids": frozenset({"db_hash_id", "hash_value"}),
    "files_names": frozenset({"db_file_id", "full_name"}),
    "galleries_access_times": frozenset({"db_gallery_id", "time"}),
    "galleries_comments": frozenset({"db_gallery_id", "comment"}),
    "galleries_dbids": frozenset({"db_gallery_id"}),
    "galleries_download_times": frozenset({"db_gallery_id", "time"}),
    "galleries_gids": frozenset({"db_gallery_id", "gid"}),
    "galleries_modified_times": frozenset({"db_gallery_id", "time"}),
    "galleries_names": frozenset({"db_gallery_id", "full_name"}),
    "galleries_redownload_times": frozenset({"db_gallery_id", "time"}),
    "galleries_tag_pairs_dbids": frozenset({"db_tag_pair_id", "tag_name", "tag_value"}),
    "galleries_tags": frozenset({"db_gallery_id", "db_tag_pair_id"}),
    "galleries_tags_names": frozenset({"tag_name"}),
    "galleries_tags_values": frozenset({"tag_value"}),
    "galleries_titles": frozenset({"db_gallery_id", "title"}),
    "galleries_upload_accounts": frozenset({"db_gallery_id", "account"}),
    "galleries_upload_times": frozenset({"db_gallery_id", "time"}),
    "gallery_content_hashes": frozenset({"db_gallery_id", "sha256"}),
    "gallery_duplicate_warnings": frozenset(
        {"db_gallery_id", "duplicate_of_db_gallery_id"}
    ),
    "gallery_ingest_state": frozenset(
        {
            "state_id",
            "phase",
            "generation",
            "completed_generation",
            "owner_token",
            "lease_expires_at",
            "handoff_generation",
            "handoff_owner_token",
            "last_transition_at",
        }
    ),
    "gallery_source_manifests": frozenset({"db_gallery_id", "sha256"}),
    "removed_galleries_gids": frozenset({"gid"}),
    "todelete_galleries": frozenset({"db_gallery_id"}),
    "todelete_gids": frozenset({"gid"}),
    "todownload_gids": frozenset({"gid", "url", "request_token"}),
}
_V1_BACKEND_REQUIRED_COLUMNS = {
    "mariadb": {
        "files_dbids": frozenset({"name_part1", "name_part2"}),
        "galleries_dbids": frozenset({"name_part1", "name_part2"}),
    },
    "sqlite": {
        "files_dbids": frozenset({"name"}),
        "galleries_dbids": frozenset({"name"}),
    },
}

_V2_REQUIRED_TABLES = frozenset(
    {
        "catalog_build_batches",
        "catalog_build_control",
        "catalog_build_discoveries",
        "catalog_builds",
        "catalog_file_hash_cache",
        "catalog_source_files",
        "catalog_source_galleries",
        "catalog_source_revision",
        "catalog_source_revision_history",
        "catalog_source_tags",
    }
)

_V2_REQUIRED_COLUMNS = {
    "catalog_builds": frozenset(
        {
            "build_id",
            "scope_key",
            "discovery_epoch",
            "discovery_tree_sha256",
            "phase",
            "ingest_generation",
            "owner_token",
            "base_source_revision",
            "base_active_build_id",
            "discovered_gallery_count",
            "expected_gallery_count",
            "staged_gallery_count",
            "staged_file_count",
            "analyzed_gallery_count",
            "created_at",
            "updated_at",
            "published_source_revision",
            "seal_sha256",
        }
    ),
    "catalog_build_control": frozenset({"singleton_id", "working_build_id"}),
    "catalog_build_discoveries": frozenset(
        {
            "build_id",
            "gallery_key",
            "gallery_name",
            "source_locator",
            "metadata_fingerprint",
        }
    ),
    "catalog_build_batches": frozenset(
        {
            "build_id",
            "batch_kind",
            "batch_id",
            "payload_sha256",
            "item_count",
            "file_count",
        }
    ),
    "catalog_source_galleries": frozenset(
        {
            "build_id",
            "gallery_key",
            "gallery_name",
            "gid",
            "title",
            "comment",
            "upload_account",
            "upload_time",
            "download_time",
            "modified_time",
            "source_manifest_sha256",
            "source_manifest_version",
            "scan_observation_sha256",
            "scan_observation_version",
            "metadata_sha256",
            "page_count",
            "directory_entry_count",
            "directory_observation_sha256",
            "raw_content_sha256",
            "content_sha256",
            "duplicate_of_gallery_name",
            "duplicate_of_gallery_key",
            "expected_file_count",
            "staged_file_count",
            "source_complete",
            "analysis_complete",
            "selected",
        }
    ),
    "catalog_source_files": frozenset(
        {
            "build_id",
            "gallery_key",
            "file_key",
            "file_sort_key",
            "file_name",
            "relative_locator",
            "device",
            "inode",
            "modified_ns",
            "changed_ns",
            "size_bytes",
            "sha256",
        }
    ),
    "catalog_source_tags": frozenset(
        {"build_id", "gallery_key", "position", "tag_name", "tag_value"}
    ),
    "catalog_file_hash_cache": frozenset(
        {"cache_key", "source_key", "fingerprint", "sha256", "cached_at"}
    ),
    "catalog_source_revision": frozenset(
        {
            "singleton_id",
            "current_revision",
            "active_build_id",
            "published_at",
            "gallery_count",
            "file_count",
        }
    ),
    "catalog_source_revision_history": frozenset(
        {"revision", "build_id", "published_at", "gallery_count", "file_count"}
    ),
}

_V2_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "catalog_builds": ("build_id",),
    "catalog_build_control": ("singleton_id",),
    "catalog_build_discoveries": ("build_id", "gallery_key"),
    "catalog_build_batches": ("build_id", "batch_kind", "batch_id"),
    "catalog_source_galleries": ("build_id", "gallery_key"),
    "catalog_source_files": ("build_id", "gallery_key", "file_key"),
    "catalog_source_tags": ("build_id", "gallery_key", "position"),
    "catalog_file_hash_cache": ("cache_key",),
    "catalog_source_revision_history": ("revision",),
    "catalog_source_revision": ("singleton_id",),
}

_V2_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "catalog_source_galleries": (
        (("build_id", "gid"), False),
        (("build_id", "content_sha256"), False),
    ),
    "catalog_source_files": (
        (("build_id", "gallery_key", "file_name"), True),
        (
            ("build_id", "gallery_key", "file_sort_key", "file_name", "file_key"),
            False,
        ),
        (("build_id", "sha256", "gallery_key", "file_key"), False),
    ),
    "catalog_source_revision_history": ((("build_id",), True),),
}

_V2_REQUIRED_FOREIGN_KEYS = {
    "catalog_build_control": (
        (
            ("working_build_id",),
            "catalog_builds",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
    "catalog_build_discoveries": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "CASCADE"),
    ),
    "catalog_build_batches": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "CASCADE"),
    ),
    "catalog_source_galleries": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "CASCADE"),
    ),
    "catalog_source_files": (
        (
            ("build_id", "gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_source_tags": (
        (
            ("build_id", "gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_source_revision_history": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "NO ACTION"),
    ),
    "catalog_source_revision": (
        (
            ("current_revision",),
            "catalog_source_revision_history",
            ("revision",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("active_build_id",),
            "catalog_builds",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
}

_V3_REQUIRED_TABLES = frozenset(
    {
        "catalog_build_analysis_phases",
        "catalog_build_content_digests",
        "catalog_build_content_owners",
        "catalog_build_excluded_file_hashes",
        "catalog_build_gid_winners",
    }
)

_V3_REQUIRED_COLUMNS = {
    "catalog_build_analysis_phases": frozenset({"build_id", "phase", "completed_at"}),
    "catalog_build_excluded_file_hashes": frozenset({"build_id", "sha256"}),
    "catalog_build_content_digests": frozenset(
        {"build_id", "gallery_key", "gallery_name", "content_sha256"}
    ),
    "catalog_build_content_owners": frozenset(
        {
            "build_id",
            "content_sha256",
            "owner_gallery_key",
            "owner_gallery_name",
        }
    ),
    "catalog_build_gid_winners": frozenset(
        {"build_id", "gid", "winner_gallery_key", "winner_gallery_name"}
    ),
}

_V3_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "catalog_build_analysis_phases": ("build_id", "phase"),
    "catalog_build_excluded_file_hashes": ("build_id", "sha256"),
    "catalog_build_content_digests": ("build_id", "gallery_key"),
    "catalog_build_content_owners": ("build_id", "content_sha256"),
    "catalog_build_gid_winners": ("build_id", "gid"),
}

_V3_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "catalog_source_galleries": (
        (("build_id", "gallery_name", "gallery_key"), False),
        (("build_id", "gid", "gallery_name", "gallery_key"), False),
    ),
    "catalog_source_files": (
        (("build_id", "gallery_key", "sha256", "file_key"), False),
    ),
    "catalog_build_content_digests": (
        (("build_id", "content_sha256", "gallery_name"), False),
    ),
    "catalog_build_content_owners": ((("build_id", "owner_gallery_key"), False),),
    "catalog_build_gid_winners": ((("build_id", "winner_gallery_key"), False),),
}

_V3_REQUIRED_FOREIGN_KEYS = {
    "catalog_build_analysis_phases": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "CASCADE"),
    ),
    "catalog_build_excluded_file_hashes": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "CASCADE"),
    ),
    "catalog_build_content_digests": (
        (
            ("build_id", "gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_build_content_owners": (
        (
            ("build_id", "owner_gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_build_gid_winners": (
        (
            ("build_id", "winner_gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
}

_V4_REQUIRED_TABLES = frozenset(
    {
        "catalog_revision_allocator",
        "catalog_build_projections",
        "catalog_build_prepared_artifacts",
        "catalog_build_projection_items",
        "catalog_build_projection_batches",
        "catalog_projection_publication_receipts",
    }
)

_V4_REQUIRED_COLUMNS = {
    "catalog_revision_allocator": frozenset({"singleton_id", "next_revision"}),
    "catalog_build_projections": frozenset(
        {
            "build_id",
            "reserved_revision",
            "base_catalog_revision",
            "artifacts_required",
            "phase",
            "artifact_after_gallery_key",
            "selection_after_gallery_key",
            "selected_gallery_count",
            "protected_artifact_count",
            "staged_selection_count",
            "projection_chain_sha256",
            "projection_xor_sha256",
            "projection_sum_sha256",
            "projection_sha256",
            "new_galleries",
            "changed_galleries",
            "removed_galleries",
            "duplicate_losers",
            "published_catalog_revision",
            "created_at",
            "updated_at",
            "sealed_at",
            "published_at",
        }
    ),
    "catalog_build_prepared_artifacts": frozenset(
        {
            "build_id",
            "gallery_key",
            "payload_sha256",
            "artifact_key",
            "artifact_name_key",
            "artifact_id",
            "name",
            "location",
            "media_type",
            "size_bytes",
            "sha256",
            "modified_at",
            "protected",
        }
    ),
    "catalog_build_projection_items": frozenset(
        {"build_id", "gallery_key", "publication_key", "item_sha256"}
    ),
    "catalog_build_projection_batches": frozenset(
        {"build_id", "batch_kind", "batch_id", "payload_sha256", "item_count"}
    ),
    "catalog_projection_publication_receipts": frozenset(
        {
            "build_id",
            "source_revision",
            "catalog_revision",
            "projection_sha256",
            "state",
            "new_galleries",
            "changed_galleries",
            "removed_galleries",
            "duplicate_losers",
            "selected_galleries",
            "committed_at",
            "finalized_at",
        }
    ),
    "catalog_revision_history": frozenset({"projection_sha256"}),
}

_V4_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "catalog_revision_allocator": ("singleton_id",),
    "catalog_build_projections": ("build_id",),
    "catalog_build_prepared_artifacts": ("build_id", "gallery_key"),
    "catalog_build_projection_items": ("build_id", "gallery_key"),
    "catalog_build_projection_batches": ("build_id", "batch_kind", "batch_id"),
    "catalog_projection_publication_receipts": ("build_id",),
}

_V4_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "catalog_build_projections": ((("reserved_revision",), True),),
    "catalog_build_prepared_artifacts": (
        (("build_id", "artifact_key"), True),
        (("build_id", "artifact_name_key"), True),
    ),
    "catalog_build_projection_items": ((("build_id", "publication_key"), True),),
    "catalog_artifacts": ((("revision", "artifact_name_key"), True),),
    "catalog_publications": (
        (("revision", "content_sha256", "source_gallery_name"), False),
    ),
    "catalog_build_content_digests": (
        (("build_id", "content_sha256", "gallery_key"), False),
    ),
    "catalog_source_galleries": ((("build_id", "gid", "gallery_key"), False),),
}

_V4_REQUIRED_FOREIGN_KEYS = {
    "catalog_build_projections": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "NO ACTION"),
        (
            ("base_catalog_revision",),
            "catalog_revision_history",
            ("revision",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("published_catalog_revision",),
            "catalog_revision_history",
            ("revision",),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
    "catalog_build_prepared_artifacts": (
        (
            ("build_id",),
            "catalog_build_projections",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("build_id", "gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
    "catalog_build_projection_items": (
        (
            ("build_id",),
            "catalog_build_projections",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("build_id", "gallery_key"),
            "catalog_source_galleries",
            ("build_id", "gallery_key"),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
    "catalog_build_projection_batches": (
        (
            ("build_id",),
            "catalog_build_projections",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
    "catalog_projection_publication_receipts": (
        (
            ("build_id",),
            "catalog_builds",
            ("build_id",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("source_revision",),
            "catalog_source_revision_history",
            ("revision",),
            "NO ACTION",
            "NO ACTION",
        ),
        (
            ("catalog_revision",),
            "catalog_revision_history",
            ("revision",),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
}

_V5_REQUIRED_TABLES = frozenset(
    {
        "catalog_build_operational_state",
        "catalog_build_removed_gid_requests",
        "catalog_build_deletion_consumptions",
        "catalog_operational_activations",
        "catalog_removed_gid_request_acks",
        "catalog_gallery_redownload_times",
    }
)

_V5_REQUIRED_COLUMNS = {
    "catalog_build_operational_state": frozenset(
        {
            "build_id",
            "preparation_id",
            "operational_schema_version",
            "phase",
            "deletion_request_generation",
            "after_gallery_key",
            "after_gid",
            "normalized_gallery_count",
            "removed_gid_request_count",
            "deletion_consumption_count",
            "prepared_at",
            "completed_at",
        }
    ),
    "catalog_build_removed_gid_requests": frozenset(
        {"build_id", "preparation_id", "gid", "url", "request_token"}
    ),
    "catalog_build_deletion_consumptions": frozenset(
        {
            "build_id",
            "preparation_id",
            "gid",
            "deletion_request_token",
        }
    ),
    "catalog_operational_activations": frozenset(
        {
            "build_id",
            "source_revision",
            "preparation_id",
            "operational_schema_version",
            "activated_at",
        }
    ),
    "catalog_removed_gid_request_acks": frozenset({"gid", "through_source_revision"}),
    "catalog_gallery_redownload_times": frozenset(
        {"gallery_key", "gallery_name", "redownload_time_utc"}
    ),
    "catalog_source_revision": frozenset({"deletion_request_generation"}),
    "catalog_source_galleries": frozenset({"upload_time_utc", "download_time_utc"}),
    "catalog_build_content_digests": frozenset({"duplicate_hash_deletion_candidate"}),
    "todelete_gids": frozenset({"request_token"}),
}

_V5_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "catalog_build_operational_state": ("build_id",),
    "catalog_build_removed_gid_requests": ("build_id", "preparation_id", "gid"),
    "catalog_build_deletion_consumptions": ("build_id", "preparation_id", "gid"),
    "catalog_operational_activations": ("build_id",),
    "catalog_removed_gid_request_acks": ("gid",),
    "catalog_gallery_redownload_times": ("gallery_key",),
}

_V5_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "catalog_build_removed_gid_requests": (
        (("request_token",), True),
        (("gid", "build_id", "preparation_id"), False),
    ),
    "catalog_build_deletion_consumptions": (
        (("gid", "deletion_request_token", "build_id", "preparation_id"), False),
    ),
    "catalog_operational_activations": ((("source_revision",), True),),
    "catalog_gallery_redownload_times": ((("gallery_name",), False),),
    "catalog_source_galleries": (
        (("build_id", "gid", "download_time_utc", "gallery_key"), False),
    ),
    "todelete_gids": ((("request_token",), True),),
}

_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    SCHEMA_MIGRATIONS_TABLE: ("version",),
    "database_maintenance_state": ("state_id",),
    "gallery_ingest_state": ("state_id",),
    "todownload_gids": ("gid",),
    "galleries_dbids": ("db_gallery_id",),
    "galleries_names": ("db_gallery_id",),
    "galleries_gids": ("db_gallery_id",),
    "todelete_gids": ("gid",),
    "galleries_download_times": ("db_gallery_id",),
    "galleries_redownload_times": ("db_gallery_id",),
    "galleries_upload_times": ("db_gallery_id",),
    "removed_galleries_gids": ("gid",),
    "galleries_modified_times": ("db_gallery_id",),
    "galleries_access_times": ("db_gallery_id",),
    "galleries_titles": ("db_gallery_id",),
    "galleries_upload_accounts": ("db_gallery_id",),
    "galleries_comments": ("db_gallery_id",),
    "files_dbids": ("db_file_id",),
    "files_names": ("db_file_id",),
    "gallery_source_manifests": ("db_gallery_id",),
    "files_hashs_sha256_dbids": ("db_hash_id",),
    "files_hashs_sha256": ("db_file_id",),
    "galleries_tags_names": ("tag_name",),
    "galleries_tags_values": ("tag_value",),
    "galleries_tag_pairs_dbids": ("db_tag_pair_id",),
    "galleries_tags": ("db_gallery_id", "db_tag_pair_id"),
    "gallery_content_hashes": ("db_gallery_id",),
    "gallery_duplicate_warnings": ("db_gallery_id",),
    "todelete_galleries": ("db_gallery_id",),
    "catalog_revision": ("singleton_id",),
    "catalog_revision_history": ("revision",),
    "catalog_publications": ("revision", "publication_key"),
    "catalog_contributors": ("revision", "publication_key", "position"),
    "catalog_subjects": ("revision", "publication_key", "position"),
    "catalog_artifacts": ("revision", "artifact_key"),
}
_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "todownload_gids": ((("request_token",), True),),
    "galleries_gids": ((("gid",), False),),
    "files_hashs_sha256_dbids": ((("hash_value",), True),),
    "files_hashs_sha256": ((("db_hash_id", "db_file_id"), True),),
    "galleries_tag_pairs_dbids": (
        (("tag_name", "tag_value"), True),
        (("tag_value",), False),
    ),
    "galleries_tags": ((("db_tag_pair_id", "db_gallery_id"), True),),
    "gallery_content_hashes": ((("sha256",), True),),
    "catalog_publications": ((("revision", "gid"), True),),
    "catalog_artifacts": ((("revision", "artifact_name_key"), False),),
}
_BACKEND_REQUIRED_INDEXES: dict[
    str,
    dict[str, tuple[tuple[tuple[str, ...], bool], ...]],
] = {
    "mariadb": {
        "galleries_dbids": ((("name_part1", "name_part2"), True),),
        "files_dbids": (
            (("db_gallery_id", "name_part1", "name_part2"), True),
            (("db_file_id", "db_gallery_id"), True),
        ),
    },
    "sqlite": {
        "galleries_dbids": ((("name",), True),),
        "files_dbids": (
            (("db_gallery_id", "name"), True),
            (("db_file_id", "db_gallery_id"), True),
        ),
    },
}

_REQUIRED_FOREIGN_KEYS = {
    "galleries_names": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_gids": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_download_times": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_redownload_times": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_upload_times": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_modified_times": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_access_times": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_titles": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_upload_accounts": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "galleries_comments": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "files_dbids": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "files_names": (
        (("db_file_id",), "files_dbids", ("db_file_id",), "CASCADE", "CASCADE"),
    ),
    "gallery_source_manifests": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "files_hashs_sha256": (
        (("db_file_id",), "files_dbids", ("db_file_id",), "CASCADE", "CASCADE"),
        (
            ("db_hash_id",),
            "files_hashs_sha256_dbids",
            ("db_hash_id",),
            "CASCADE",
            "NO ACTION",
        ),
    ),
    "galleries_tag_pairs_dbids": (
        (("tag_name",), "galleries_tags_names", ("tag_name",), "CASCADE", "CASCADE"),
        (("tag_value",), "galleries_tags_values", ("tag_value",), "CASCADE", "CASCADE"),
    ),
    "galleries_tags": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
        (
            ("db_tag_pair_id",),
            "galleries_tag_pairs_dbids",
            ("db_tag_pair_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "gallery_content_hashes": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "gallery_duplicate_warnings": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
        (
            ("duplicate_of_db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "todelete_galleries": (
        (
            ("db_gallery_id",),
            "galleries_dbids",
            ("db_gallery_id",),
            "CASCADE",
            "CASCADE",
        ),
    ),
    "catalog_contributors": (
        (
            ("revision", "publication_key"),
            "catalog_publications",
            ("revision", "publication_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_subjects": (
        (
            ("revision", "publication_key"),
            "catalog_publications",
            ("revision", "publication_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
    "catalog_artifacts": (
        (
            ("revision", "publication_key"),
            "catalog_publications",
            ("revision", "publication_key"),
            "NO ACTION",
            "CASCADE",
        ),
    ),
}
_REQUIRED_VIEW_DEFINITION_FRAGMENTS = {
    "files_hashs": ("files_hashs_sha256", "files_hashs_sha256_dbids"),
    "duplicate_hash_in_gallery": ("files_hashs", "db_gallery_id", "hash_value"),
    "galleries_infos": (
        "galleries_names",
        "galleries_gids",
        "galleries_download_times",
        "galleries_upload_times",
    ),
    "pending_download_gids": (
        "galleries_redownload_times",
        "removed_galleries_gids",
        "todelete_gids",
        "todelete_galleries",
    ),
    "todelete_gallery_candidates": (
        "todelete_gids",
        "galleries_infos",
        "duplicate_hash_in_gallery",
    ),
    "gallery_duplicate_warnings_names": (
        "gallery_duplicate_warnings",
        "galleries_names",
        "duplicate_name",
        "kept_name",
    ),
    "todelete_rm_commands": ("todelete_galleries", "galleries_names", "rm -rf"),
}

_V6_TODELETE_VIEW_DEFINITION_FRAGMENTS = (
    "catalog_source_revision",
    "catalog_operational_activations",
    "activation.build_id = source_revision.active_build_id",
    "activation.source_revision = source_revision.current_revision",
    "catalog_source_galleries",
    "catalog_build_discoveries",
    "source_locator",
    "catalog_build_deletion_consumptions",
    "consumption.deletion_request_token = marker.request_token",
    "newer.download_time_utc > source.download_time_utc",
    "digest.duplicate_hash_deletion_candidate = 1",
    "fallback_activation.build_id = fallback_source_revision.active_build_id",
    "fallback_activation.source_revision = "
    "fallback_source_revision.current_revision",
    "todelete_galleries",
    "galleries_names",
    "rm -rf",
)

_V7_REQUIRED_TABLES = frozenset(
    {
        "catalog_build_analysis_scan_checkpoints",
        "catalog_build_analysis_scan_receipts",
    }
)

_V7_REQUIRED_COLUMNS = {
    "catalog_build_analysis_scan_checkpoints": frozenset(
        {
            "build_id",
            "phase",
            "generation",
            "minimum_occurrences",
            "cursor_sha256",
            "output_sha256",
            "state",
            "updated_at",
        }
    ),
    "catalog_build_analysis_scan_receipts": frozenset(
        {
            "build_id",
            "phase",
            "batch_key",
            "start_cursor_sha256",
            "next_cursor_sha256",
            "minimum_occurrences",
            "page_limit",
            "input_sha256",
            "output_sha256",
            "row_count",
            "committed_generation",
            "committed_at",
        }
    ),
}

_V7_REQUIRED_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "catalog_build_analysis_scan_checkpoints": ("build_id", "phase"),
    "catalog_build_analysis_scan_receipts": ("build_id", "phase", "batch_key"),
}

_V7_REQUIRED_INDEXES: dict[str, tuple[tuple[tuple[str, ...], bool], ...]] = {
    "catalog_source_galleries": (
        (("build_id", "source_complete", "staged_file_count", "gallery_key"), False),
    ),
    "catalog_build_analysis_scan_receipts": (
        (("build_id", "phase", "start_cursor_sha256"), True),
        (("build_id", "phase", "committed_at", "batch_key"), False),
    ),
}

_V7_REQUIRED_FOREIGN_KEYS = {
    "catalog_build_analysis_scan_checkpoints": (
        (("build_id",), "catalog_builds", ("build_id",), "NO ACTION", "NO ACTION"),
    ),
    "catalog_build_analysis_scan_receipts": (
        (
            ("build_id", "phase"),
            "catalog_build_analysis_scan_checkpoints",
            ("build_id", "phase"),
            "NO ACTION",
            "NO ACTION",
        ),
    ),
}


class SchemaCompatibilityError(RuntimeError):
    pass


class MigrationRunner:
    """Apply numbered forward-only migrations and validate their ledger."""

    def __init__(self, context: RepositoryContext) -> None:
        self._context = context
        settings = H2HDBCheckDatabaseSettings(context)
        self._maintenance = H2HDBDatabaseMaintenance(context, settings)
        self._coordination = H2HDBGalleryIngestCoordination(context)
        self._queue = H2HDBToDownloadQueue(context)
        self._removed = H2HDBRemovedGalleries(context)
        self._gallery_ids = H2HDBGalleriesIDs(context)
        self._gallery_gids = H2HDBGalleriesGIDs(context, self._gallery_ids)
        self._gallery_times = H2HDBTimes(context, self._gallery_ids)
        self._gallery_titles = H2HDBGalleriesTitles(context, self._gallery_ids)
        self._upload_accounts = H2HDBUploadAccounts(context, self._gallery_ids)
        self._gallery_comments = H2HDBGalleriesComments(context, self._gallery_ids)
        self._gallery_tags = H2HDBGalleriesTags(context, self._gallery_ids)
        self._files = H2HDBFiles(context, self._gallery_ids)
        self._source_manifests = H2HDBGallerySourceManifests(context, self._gallery_ids)
        self._to_delete = H2HDBToDeleteQueue(context)
        self._gallery_infos = H2HDBGalleriesInfos(context)
        self._deduplication = H2HDBGalleryDeduplication(
            context,
            self._gallery_ids,
            self._gallery_times,
            self._gallery_titles,
        )

    def migrate(self) -> int:
        if not self._migration_ledger_exists():
            existing_objects = self._schema_object_names()
            if existing_objects:
                raise SchemaCompatibilityError(
                    "Refusing h2hdb migration for an unversioned, "
                    "non-empty database: "
                    f"objects={sorted(existing_objects)}. The runtime migrator "
                    "only initializes an empty database or advances a database "
                    "that already has a schema-version ledger."
                )
            self._create_migration_ledger()

        applied = self._applied_migrations()
        self._validate_applied_migrations(applied)
        for migration in _MIGRATION_REGISTRY:
            if migration.version in applied:
                continue
            if migration.version > 1:
                predecessor = _MIGRATION_REGISTRY[migration.version - 2]
                self._validate_migration_result(predecessor)
            self._apply_migration(migration)
            self._validate_migration_result(migration)
            self._record_migration(migration)
        return LATEST_SCHEMA_VERSION

    def current_version(self) -> int:
        if not self._migration_ledger_exists():
            return 0
        applied = self._applied_migrations()
        return max(applied, default=0)

    def check_readiness(self) -> SchemaCompatibility:
        """Validate only the migration ledger used as the schema commit marker."""

        applied = self._applied_migrations() if self._migration_ledger_exists() else {}
        self._validate_applied_migrations(applied)
        version = max(applied, default=0)
        compatibility = SchemaCompatibility(
            database_version=version,
            minimum_supported=MINIMUM_SCHEMA_VERSION,
            maximum_supported=LATEST_SCHEMA_VERSION,
        )
        if applied != self._expected_migrations():
            raise SchemaCompatibilityError(
                "Incompatible h2hdb schema version: "
                f"database={version} recorded={sorted(applied.items())} "
                f"supported={LATEST_SCHEMA_VERSION}."
            )
        return compatibility

    def check_compatibility(self) -> SchemaCompatibility:
        compatibility = self.check_readiness()
        for migration in _MIGRATION_REGISTRY:
            self._validate_migration_result(migration)
        return compatibility

    @staticmethod
    def _expected_migrations() -> dict[int, str]:
        return {migration.version: migration.name for migration in _MIGRATION_REGISTRY}

    def _migration_ledger_exists(self) -> bool:
        with self._context.SQLConnector() as connector:
            return connector.check_table_exists(SCHEMA_MIGRATIONS_TABLE)

    def _create_migration_ledger(self) -> None:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                self._create_migration_ledger_with_connector(connector)

    def _create_migration_ledger_with_connector(
        self,
        connector: SQLConnector,
    ) -> None:
        match self._context.sql_type:
            case "mariadb":
                connector.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                        version INT UNSIGNED NOT NULL PRIMARY KEY,
                        name VARCHAR(191) NOT NULL,
                        applied_at VARCHAR(40) NOT NULL
                    )
                    """)
            case "sqlite":
                connector.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                        version INTEGER NOT NULL PRIMARY KEY,
                        name TEXT NOT NULL,
                        applied_at TEXT NOT NULL
                    )
                    """)

    def _applied_migrations(self) -> dict[int, str]:
        with self._context.SQLConnector() as connector:
            rows = connector.fetch_all(
                f"SELECT version, name FROM {SCHEMA_MIGRATIONS_TABLE} "
                "ORDER BY version"
            )
        return {int(row[0]): str(row[1]) for row in rows}

    def _validate_applied_migrations(self, applied: dict[int, str]) -> None:
        expected = self._expected_migrations()
        recorded_versions = sorted(applied)
        unknown_versions = sorted(set(applied) - set(expected))
        if unknown_versions:
            raise SchemaCompatibilityError(
                "The h2hdb migration ledger contains unsupported versions: "
                f"database={max(recorded_versions)} recorded={recorded_versions} "
                f"unknown={unknown_versions} "
                f"supported_through={LATEST_SCHEMA_VERSION}."
            )

        expected_prefix = [
            migration.version
            for migration in _MIGRATION_REGISTRY[: len(recorded_versions)]
        ]
        if recorded_versions != expected_prefix:
            raise SchemaCompatibilityError(
                "The h2hdb migration ledger is not a contiguous forward-only "
                f"history: recorded={recorded_versions} expected_prefix="
                f"{expected_prefix}."
            )

        mismatched_names = {
            version: (applied[version], expected[version])
            for version in recorded_versions
            if applied[version] != expected[version]
        }
        if mismatched_names:
            raise SchemaCompatibilityError(
                "The h2hdb migration ledger contains mismatched migration "
                f"names: {mismatched_names}."
            )

    def _record_migration(self, migration: _SchemaMigration) -> None:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                connector.execute(
                    f"""
                    INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (
                        version,
                        name,
                        applied_at
                    ) VALUES (%s, %s, %s)
                    """,
                    (
                        migration.version,
                        migration.name,
                        datetime.now(UTC).isoformat(),
                    ),
                )

    def _schema_object_names(self) -> set[str]:
        with self._context.SQLConnector() as connector:
            match self._context.sql_type:
                case "mariadb":
                    rows = connector.fetch_all(
                        """
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s
                        """,
                        (self._context.config.database.database,),
                    )
                case "sqlite":
                    rows = connector.fetch_all("""
                        SELECT name
                        FROM sqlite_master
                        WHERE name NOT LIKE 'sqlite_%'
                            AND type IN ('table', 'view', 'trigger', 'index')
                        """)
        return {str(row[0]) for row in rows}

    def _table_and_view_names(self) -> tuple[set[str], set[str]]:
        with self._context.SQLConnector() as connector:
            match self._context.sql_type:
                case "mariadb":
                    rows = connector.fetch_all(
                        """
                        SELECT TABLE_NAME, TABLE_TYPE
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s
                        """,
                        (self._context.config.database.database,),
                    )
                    tables = {
                        str(name)
                        for name, table_type in rows
                        if str(table_type) == "BASE TABLE"
                    }
                    views = {
                        str(name)
                        for name, table_type in rows
                        if str(table_type) == "VIEW"
                    }
                case "sqlite":
                    rows = connector.fetch_all("""
                        SELECT name, type
                        FROM sqlite_master
                        WHERE type IN ('table', 'view')
                        """)
                    tables = {
                        str(name)
                        for name, object_type in rows
                        if object_type == "table"
                    }
                    views = {
                        str(name) for name, object_type in rows if object_type == "view"
                    }
        return tables, views

    def _table_columns(self, table_name: str) -> set[str]:
        with self._context.SQLConnector() as connector:
            match self._context.sql_type:
                case "mariadb":
                    rows = connector.fetch_all(
                        """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        """,
                        (self._context.config.database.database, table_name),
                    )
                    return {str(row[0]) for row in rows}
                case "sqlite":
                    rows = connector.fetch_all(f"PRAGMA table_info({table_name})")
                    return {str(row[1]) for row in rows}
                case _:
                    raise AssertionError(
                        f"Unsupported SQL type: {self._context.sql_type}"
                    )

    def _validate_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }

        required_indexes = {
            **_REQUIRED_INDEXES,
            **_BACKEND_REQUIRED_INDEXES[self._context.sql_type],
        }
        actual_indexes = {
            table_name: self._indexes(table_name) for table_name in required_indexes
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in required_indexes.items():
            for expected_columns, unique in expected_indexes:
                if not any(
                    actual_columns == expected_columns and actual_unique == unique
                    for actual_columns, actual_unique in actual_indexes[table_name]
                ):
                    missing_indexes.setdefault(table_name, []).append(expected_columns)

        missing_foreign_keys: dict[
            str,
            list[tuple[tuple[str, ...], str, tuple[str, ...], str, str]],
        ] = {}
        for table_name, expected_keys in _REQUIRED_FOREIGN_KEYS.items():
            actual_keys = self._foreign_keys(table_name)
            for expected in expected_keys:
                if expected not in actual_keys:
                    missing_foreign_keys.setdefault(table_name, []).append(expected)

        invalid_checks: dict[str, tuple[str, ...]] = {}
        check_fragments: dict[str, tuple[str, ...]] = {
            "database_maintenance_state": ("checkstate_id=1",),
            "gallery_ingest_state": (
                "checkstate_id=1",
                "completed_generation<=generation",
                "handoff_generationisnullandhandoff_owner_tokenisnull",
                "phase=ready",
                "phase=downloading",
                "phase=ingest_requested",
                "phase=ingesting",
            ),
            "catalog_revision": ("checksingleton_id=1",),
        }
        if self._context.sql_type == "sqlite":
            check_fragments.update(
                {
                    "database_maintenance_state": (
                        "checkstate_id=1",
                        "checkaccumulated_work>=0",
                    ),
                    "files_hashs_sha256_dbids": ("checklengthhash_value=32",),
                    "catalog_revision": (
                        "checksingleton_id=1",
                        "checkcurrent_revision>=0",
                        "checkpublication_count>=0",
                    ),
                    "catalog_revision_history": (
                        "checkrevision>=0",
                        "checkpublication_count>=0",
                    ),
                    "catalog_contributors": ("checkposition>=0",),
                    "catalog_subjects": ("checkposition>=0",),
                    "catalog_artifacts": ("checksize_bytes>=0",),
                    "catalog_publications": (
                        "checkgid>0",
                        "checkcontent_sha256isnullorlengthcontent_sha256=64",
                    ),
                }
            )

        nullable_catalog_columns = self._nullable_columns("catalog_publications")
        invalid_nullability = (
            {"catalog_publications": ("source_gallery_name",)}
            if "source_gallery_name" in nullable_catalog_columns
            else {}
        )
        for table_name, fragments in check_fragments.items():
            definition = self._normalized_definition(table_name, object_type="table")
            missing_check_fragments = tuple(
                fragment for fragment in fragments if fragment not in definition
            )
            if missing_check_fragments:
                invalid_checks[table_name] = missing_check_fragments

        invalid_views: dict[str, tuple[str, ...]] = {}
        for view_name, fragments in _REQUIRED_VIEW_DEFINITION_FRAGMENTS.items():
            definition = self._normalized_definition(view_name, object_type="view")
            missing_view_fragments = tuple(
                self._normalize_sql(fragment)
                for fragment in fragments
                if self._normalize_sql(fragment) not in definition
            )
            if missing_view_fragments:
                invalid_views[view_name] = missing_view_fragments

        if (
            invalid_primary_keys
            or missing_indexes
            or missing_foreign_keys
            or invalid_checks
            or invalid_views
            or invalid_nullability
        ):
            raise SchemaCompatibilityError(
                "The h2hdb schema objects do not satisfy their structural "
                "contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"foreign_keys={missing_foreign_keys} "
                f"checks={invalid_checks} "
                f"views={invalid_views} "
                f"nullability={invalid_nullability}."
            )

    def _validate_catalog_build_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _V2_REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in _V2_REQUIRED_INDEXES.items():
            actual_indexes = self._indexes(table_name)
            for expected_columns, unique in expected_indexes:
                if not any(
                    actual_columns == expected_columns and actual_unique == unique
                    for actual_columns, actual_unique in actual_indexes
                ):
                    missing_indexes.setdefault(table_name, []).append(expected_columns)
        missing_foreign_keys: dict[
            str,
            list[tuple[tuple[str, ...], str, tuple[str, ...], str, str]],
        ] = {}
        for table_name, expected_keys in _V2_REQUIRED_FOREIGN_KEYS.items():
            actual_keys = self._foreign_keys(table_name)
            for expected in expected_keys:
                if expected not in actual_keys:
                    missing_foreign_keys.setdefault(table_name, []).append(expected)
        required_nonnullable = {
            "catalog_builds": (
                "build_id",
                "scope_key",
                "discovery_epoch",
                "phase",
                "ingest_generation",
                "owner_token",
                "base_source_revision",
                "discovered_gallery_count",
                "staged_gallery_count",
                "staged_file_count",
                "analyzed_gallery_count",
                "created_at",
                "updated_at",
            ),
            "catalog_build_discoveries": (
                "build_id",
                "gallery_key",
                "gallery_name",
                "source_locator",
            ),
            "catalog_build_batches": (
                "build_id",
                "batch_kind",
                "batch_id",
                "payload_sha256",
                "item_count",
                "file_count",
            ),
            "catalog_source_galleries": (
                "build_id",
                "gallery_key",
                "gallery_name",
                "gid",
                "staged_file_count",
                "source_complete",
                "analysis_complete",
                "selected",
            ),
            "catalog_source_files": (
                "build_id",
                "gallery_key",
                "file_key",
                "file_sort_key",
                "file_name",
                "size_bytes",
                "sha256",
            ),
        }
        invalid_nullability = {
            table_name: tuple(
                column
                for column in columns
                if column in self._nullable_columns(table_name)
            )
            for table_name, columns in required_nonnullable.items()
        }
        invalid_nullability = {
            table_name: columns
            for table_name, columns in invalid_nullability.items()
            if columns
        }
        invalid_checks: dict[str, tuple[str, ...]] = {}
        if self._context.sql_type == "sqlite":
            check_fragments = {
                "catalog_builds": (
                    "checklengthbuild_id=32",
                    "phaseindiscovering",
                    "checkdiscovered_gallery_count>=0",
                    "checkstaged_file_count>=0",
                ),
                "catalog_build_control": ("checksingleton_id=1",),
                "catalog_build_batches": (
                    "checklengthpayload_sha256=64",
                    "checkitem_count>=0",
                    "checkfile_count>=0",
                ),
                "catalog_source_galleries": (
                    "checkgid>0",
                    "checkstaged_file_count>=0",
                    "checksource_completein0,1",
                    "checkanalysis_completein0,1",
                    "checkselectedin0,1",
                ),
                "catalog_source_files": (
                    "checksize_bytes>=0",
                    "checklengthsha256=64",
                ),
                "catalog_source_revision": ("checksingleton_id=1",),
            }
            for table_name, fragments in check_fragments.items():
                definition = self._normalized_definition(
                    table_name,
                    object_type="table",
                )
                missing = tuple(
                    fragment for fragment in fragments if fragment not in definition
                )
                if missing:
                    invalid_checks[table_name] = missing
        if (
            invalid_primary_keys
            or missing_indexes
            or missing_foreign_keys
            or invalid_nullability
            or invalid_checks
        ):
            raise SchemaCompatibilityError(
                "The durable catalog build schema does not satisfy its structural "
                "contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"foreign_keys={missing_foreign_keys} "
                f"nullability={invalid_nullability} "
                f"checks={invalid_checks}."
            )

    def _validate_catalog_analysis_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _V3_REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in _V3_REQUIRED_INDEXES.items():
            actual_indexes = self._indexes(table_name)
            for expected_columns, unique in expected_indexes:
                if not any(
                    actual_columns == expected_columns and actual_unique == unique
                    for actual_columns, actual_unique in actual_indexes
                ):
                    missing_indexes.setdefault(table_name, []).append(expected_columns)
        missing_foreign_keys: dict[
            str,
            list[tuple[tuple[str, ...], str, tuple[str, ...], str, str]],
        ] = {}
        for table_name, expected_keys in _V3_REQUIRED_FOREIGN_KEYS.items():
            actual_keys = self._foreign_keys(table_name)
            for expected in expected_keys:
                if expected not in actual_keys:
                    missing_foreign_keys.setdefault(table_name, []).append(expected)
        required_nonnullable = {
            table_name: tuple(columns)
            for table_name, columns in _V3_REQUIRED_COLUMNS.items()
        }
        # Null is a meaningful content digest for an empty/effectively empty gallery.
        required_nonnullable["catalog_build_content_digests"] = (
            "build_id",
            "gallery_key",
            "gallery_name",
        )
        invalid_nullability = {
            table_name: tuple(
                column
                for column in columns
                if column in self._nullable_columns(table_name)
            )
            for table_name, columns in required_nonnullable.items()
        }
        invalid_nullability = {
            table_name: columns
            for table_name, columns in invalid_nullability.items()
            if columns
        }
        invalid_checks: dict[str, tuple[str, ...]] = {}
        if self._context.sql_type == "sqlite":
            check_fragments = {
                "catalog_build_analysis_phases": ("phaseinsource_manifests",),
                "catalog_build_excluded_file_hashes": ("checklengthsha256=64",),
                "catalog_build_content_digests": (
                    "content_sha256isnullorlengthcontent_sha256=64",
                ),
                "catalog_build_content_owners": ("checklengthcontent_sha256=64",),
                "catalog_build_gid_winners": ("checkgid>0",),
            }
            for table_name, fragments in check_fragments.items():
                definition = self._normalized_definition(
                    table_name,
                    object_type="table",
                )
                missing = tuple(
                    fragment for fragment in fragments if fragment not in definition
                )
                if missing:
                    invalid_checks[table_name] = missing
        if (
            invalid_primary_keys
            or missing_indexes
            or missing_foreign_keys
            or invalid_nullability
            or invalid_checks
        ):
            raise SchemaCompatibilityError(
                "The durable catalog analysis schema does not satisfy its structural "
                "contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"foreign_keys={missing_foreign_keys} "
                f"nullability={invalid_nullability} "
                f"checks={invalid_checks}."
            )

    def _validate_catalog_projection_build_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _V4_REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in _V4_REQUIRED_INDEXES.items():
            actual_indexes = self._indexes(table_name)
            for expected_columns, unique in expected_indexes:
                if (expected_columns, unique) not in actual_indexes:
                    missing_indexes.setdefault(table_name, []).append(expected_columns)
        missing_foreign_keys: dict[
            str,
            list[tuple[tuple[str, ...], str, tuple[str, ...], str, str]],
        ] = {}
        for table_name, expected_keys in _V4_REQUIRED_FOREIGN_KEYS.items():
            actual_keys = self._foreign_keys(table_name)
            for expected in expected_keys:
                if expected not in actual_keys:
                    missing_foreign_keys.setdefault(table_name, []).append(expected)
        required_nonnullable = {
            "catalog_revision_allocator": ("singleton_id", "next_revision"),
            "catalog_build_projections": (
                "build_id",
                "reserved_revision",
                "base_catalog_revision",
                "artifacts_required",
                "phase",
                "selected_gallery_count",
                "protected_artifact_count",
                "staged_selection_count",
                "projection_chain_sha256",
                "projection_xor_sha256",
                "projection_sum_sha256",
                "new_galleries",
                "changed_galleries",
                "removed_galleries",
                "duplicate_losers",
                "created_at",
                "updated_at",
            ),
            "catalog_build_prepared_artifacts": tuple(
                _V4_REQUIRED_COLUMNS["catalog_build_prepared_artifacts"]
            ),
            "catalog_build_projection_items": tuple(
                _V4_REQUIRED_COLUMNS["catalog_build_projection_items"]
            ),
            "catalog_build_projection_batches": tuple(
                _V4_REQUIRED_COLUMNS["catalog_build_projection_batches"]
            ),
            "catalog_projection_publication_receipts": tuple(
                _V4_REQUIRED_COLUMNS["catalog_projection_publication_receipts"]
                - {"finalized_at"}
            ),
        }
        invalid_nullability = {
            table_name: tuple(
                column
                for column in columns
                if column in self._nullable_columns(table_name)
            )
            for table_name, columns in required_nonnullable.items()
        }
        invalid_nullability = {
            table_name: columns
            for table_name, columns in invalid_nullability.items()
            if columns
        }
        if (
            invalid_primary_keys
            or missing_indexes
            or missing_foreign_keys
            or invalid_nullability
        ):
            raise SchemaCompatibilityError(
                "The durable catalog projection schema does not satisfy its "
                "structural contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"foreign_keys={missing_foreign_keys} "
                f"nullability={invalid_nullability}."
            )

    def _validate_catalog_operational_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _V5_REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in _V5_REQUIRED_INDEXES.items():
            actual_indexes = self._indexes(table_name)
            for expected_columns, unique in expected_indexes:
                if (expected_columns, unique) not in actual_indexes:
                    missing_indexes.setdefault(table_name, []).append(expected_columns)
        required_nonnullable = {
            "catalog_build_operational_state": tuple(
                _V5_REQUIRED_COLUMNS["catalog_build_operational_state"]
                - {"after_gallery_key", "after_gid", "completed_at"}
            ),
            "catalog_build_removed_gid_requests": tuple(
                _V5_REQUIRED_COLUMNS["catalog_build_removed_gid_requests"]
            ),
            "catalog_build_deletion_consumptions": tuple(
                _V5_REQUIRED_COLUMNS["catalog_build_deletion_consumptions"]
            ),
            "catalog_operational_activations": tuple(
                _V5_REQUIRED_COLUMNS["catalog_operational_activations"]
            ),
            "catalog_removed_gid_request_acks": tuple(
                _V5_REQUIRED_COLUMNS["catalog_removed_gid_request_acks"]
            ),
            "catalog_gallery_redownload_times": tuple(
                _V5_REQUIRED_COLUMNS["catalog_gallery_redownload_times"]
            ),
        }
        invalid_nullability = {
            table_name: tuple(
                column
                for column in columns
                if column in self._nullable_columns(table_name)
            )
            for table_name, columns in required_nonnullable.items()
        }
        invalid_nullability = {
            table_name: columns
            for table_name, columns in invalid_nullability.items()
            if columns
        }
        with self._context.SQLConnector() as connector:
            null_token = connector.fetch_one(
                "SELECT gid FROM todelete_gids WHERE request_token IS NULL LIMIT 1"
            )
        if invalid_primary_keys or missing_indexes or invalid_nullability or null_token:
            raise SchemaCompatibilityError(
                "The catalog operational schema does not satisfy its structural "
                "contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"nullability={invalid_nullability} "
                f"null_deletion_token={bool(null_token)}."
            )

    def _validate_catalog_analysis_scan_schema_contracts(self) -> None:
        invalid_primary_keys = {
            table_name: (expected, self._primary_key(table_name))
            for table_name, expected in _V7_REQUIRED_PRIMARY_KEYS.items()
            if self._primary_key(table_name) != expected
        }
        missing_indexes: dict[str, list[tuple[str, ...]]] = {}
        for table_name, expected_indexes in _V7_REQUIRED_INDEXES.items():
            actual_indexes = self._indexes(table_name)
            for expected_columns, unique in expected_indexes:
                if (expected_columns, unique) not in actual_indexes:
                    missing_indexes.setdefault(table_name, []).append(expected_columns)
        missing_foreign_keys: dict[
            str,
            list[tuple[tuple[str, ...], str, tuple[str, ...], str, str]],
        ] = {}
        for table_name, expected_keys in _V7_REQUIRED_FOREIGN_KEYS.items():
            actual_keys = self._foreign_keys(table_name)
            for expected in expected_keys:
                if expected not in actual_keys:
                    missing_foreign_keys.setdefault(table_name, []).append(expected)
        invalid_nullability = {
            table_name: tuple(
                column
                for column in required_columns
                if column in self._nullable_columns(table_name)
            )
            for table_name, required_columns in _V7_REQUIRED_COLUMNS.items()
        }
        invalid_nullability = {
            table_name: columns
            for table_name, columns in invalid_nullability.items()
            if columns
        }
        check_fragments = {
            "catalog_build_analysis_scan_checkpoints": (
                "phase=file_spam",
                "generation>0",
                "minimum_occurrences>0",
                "lengthcursor_sha256in0,64",
                "lengthoutput_sha256=64",
                "stateinopen,complete",
            ),
            "catalog_build_analysis_scan_receipts": (
                "phase=file_spam",
                "minimum_occurrences>0",
                "page_limit>0",
                "row_count>=0",
                "row_count<=page_limit",
                "committed_generation>0",
                "lengthinput_sha256=64",
                "lengthoutput_sha256=64",
            ),
        }
        invalid_checks = {
            table_name: tuple(
                fragment
                for fragment in fragments
                if fragment
                not in self._normalized_definition(table_name, object_type="table")
            )
            for table_name, fragments in check_fragments.items()
        }
        invalid_checks = {
            table_name: fragments
            for table_name, fragments in invalid_checks.items()
            if fragments
        }
        if (
            invalid_primary_keys
            or missing_indexes
            or missing_foreign_keys
            or invalid_nullability
            or invalid_checks
        ):
            raise SchemaCompatibilityError(
                "The durable analysis scan schema does not satisfy its structural "
                "contracts: "
                f"primary_keys={invalid_primary_keys} "
                f"indexes={missing_indexes} "
                f"foreign_keys={missing_foreign_keys} "
                f"nullability={invalid_nullability} "
                f"checks={invalid_checks}."
            )

    def _nullable_columns(self, table_name: str) -> set[str]:
        with self._context.SQLConnector() as connector:
            if self._context.sql_type == "mariadb":
                rows = connector.fetch_all(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND IS_NULLABLE = 'YES'
                    """,
                    (self._context.config.database.database, table_name),
                )
                return {str(row[0]) for row in rows}
            rows = connector.fetch_all(f"PRAGMA table_info({table_name})")
            return {str(row[1]) for row in rows if not bool(row[3])}

    def _primary_key(self, table_name: str) -> tuple[str, ...]:
        with self._context.SQLConnector() as connector:
            if self._context.sql_type == "mariadb":
                rows = connector.fetch_all(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND CONSTRAINT_NAME = 'PRIMARY'
                    ORDER BY ORDINAL_POSITION
                    """,
                    (self._context.config.database.database, table_name),
                )
                return tuple(str(row[0]) for row in rows)
            rows = connector.fetch_all(f"PRAGMA table_info({table_name})")
            return tuple(
                str(row[1])
                for row in sorted(rows, key=lambda item: int(item[5]))
                if int(row[5]) > 0
            )

    def _indexes(self, table_name: str) -> set[tuple[tuple[str, ...], bool]]:
        with self._context.SQLConnector() as connector:
            if self._context.sql_type == "mariadb":
                rows = connector.fetch_all(
                    """
                    SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY INDEX_NAME, SEQ_IN_INDEX
                    """,
                    (self._context.config.database.database, table_name),
                )
                grouped: dict[str, tuple[list[str], bool]] = {}
                for index_name, column_name, non_unique, _position in rows:
                    columns, unique = grouped.setdefault(
                        str(index_name),
                        ([], not bool(non_unique)),
                    )
                    columns.append(str(column_name))
                    grouped[str(index_name)] = (columns, unique)
                return {
                    (tuple(columns), unique) for columns, unique in grouped.values()
                }

            index_rows = connector.fetch_all(f"PRAGMA index_list({table_name})")
            indexes: set[tuple[tuple[str, ...], bool]] = set()
            for row in index_rows:
                index_name = str(row[1])
                column_rows = connector.fetch_all(f"PRAGMA index_info({index_name})")
                sqlite_columns = tuple(
                    str(column[2])
                    for column in sorted(column_rows, key=lambda item: int(item[0]))
                )
                indexes.add((sqlite_columns, bool(row[2])))
            return indexes

    def _foreign_keys(
        self,
        table_name: str,
    ) -> set[tuple[tuple[str, ...], str, tuple[str, ...], str, str]]:
        with self._context.SQLConnector() as connector:
            if self._context.sql_type == "mariadb":
                rows = connector.fetch_all(
                    """
                    SELECT
                        kcu.CONSTRAINT_NAME,
                        kcu.COLUMN_NAME,
                        kcu.REFERENCED_TABLE_NAME,
                        kcu.REFERENCED_COLUMN_NAME,
                        kcu.ORDINAL_POSITION,
                        rules.UPDATE_RULE,
                        rules.DELETE_RULE
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                    JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rules
                        ON rules.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                        AND rules.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    WHERE kcu.TABLE_SCHEMA = %s
                        AND kcu.TABLE_NAME = %s
                        AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                    ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                    """,
                    (self._context.config.database.database, table_name),
                )
                grouped: dict[
                    str,
                    tuple[list[str], str, list[str], str, str],
                ] = {}
                for (
                    name,
                    column,
                    target,
                    target_column,
                    _position,
                    update_rule,
                    delete_rule,
                ) in rows:
                    columns, _target, target_columns, _update, _delete = (
                        grouped.setdefault(
                            str(name),
                            (
                                [],
                                str(target),
                                [],
                                self._normalize_referential_rule(update_rule),
                                self._normalize_referential_rule(delete_rule),
                            ),
                        )
                    )
                    columns.append(str(column))
                    target_columns.append(str(target_column))
                    grouped[str(name)] = (
                        columns,
                        str(target),
                        target_columns,
                        self._normalize_referential_rule(update_rule),
                        self._normalize_referential_rule(delete_rule),
                    )
                return {
                    (
                        tuple(columns),
                        target,
                        tuple(target_columns),
                        update_rule,
                        delete_rule,
                    )
                    for (
                        columns,
                        target,
                        target_columns,
                        update_rule,
                        delete_rule,
                    ) in grouped.values()
                }

            rows = connector.fetch_all(f"PRAGMA foreign_key_list({table_name})")
            grouped_sqlite: dict[
                int,
                tuple[list[str], str, list[str], str, str],
            ] = {}
            for row in sorted(rows, key=lambda item: (int(item[0]), int(item[1]))):
                columns, _target, target_columns, _update, _delete = (
                    grouped_sqlite.setdefault(
                        int(row[0]),
                        (
                            [],
                            str(row[2]),
                            [],
                            self._normalize_referential_rule(row[5]),
                            self._normalize_referential_rule(row[6]),
                        ),
                    )
                )
                columns.append(str(row[3]))
                target_columns.append(str(row[4]))
                grouped_sqlite[int(row[0])] = (
                    columns,
                    str(row[2]),
                    target_columns,
                    self._normalize_referential_rule(row[5]),
                    self._normalize_referential_rule(row[6]),
                )
            return {
                (
                    tuple(columns),
                    target,
                    tuple(target_columns),
                    update_rule,
                    delete_rule,
                )
                for (
                    columns,
                    target,
                    target_columns,
                    update_rule,
                    delete_rule,
                ) in grouped_sqlite.values()
            }

    def _normalized_definition(self, name: str, *, object_type: str) -> str:
        with self._context.SQLConnector() as connector:
            if self._context.sql_type == "mariadb":
                if object_type == "table":
                    row = connector.fetch_one(f"SHOW CREATE TABLE `{name}`")
                else:
                    row = connector.fetch_one(f"SHOW CREATE VIEW `{name}`")
                definition = str(row[1]) if row else ""
            else:
                row = connector.fetch_one(
                    "SELECT sql FROM sqlite_master WHERE type = %s AND name = %s",
                    (object_type, name),
                )
                definition = str(row[0]) if row and row[0] is not None else ""
        return self._normalize_sql(definition)

    @staticmethod
    def _normalize_sql(value: str) -> str:
        return re.sub(r"[\s`\"'()]", "", value.casefold())

    @staticmethod
    def _normalize_referential_rule(value: object) -> str:
        rule = str(value).upper()
        # MariaDB reports its ``NO ACTION`` default as ``RESTRICT``. Both
        # reject changes at statement completion and are interchangeable for
        # the non-deferrable foreign keys used by h2hdb.
        return "NO ACTION" if rule == "RESTRICT" else rule

    def _apply_migration(self, migration: _SchemaMigration) -> None:
        match migration.version:
            case 1:
                self._apply_current_schema()
            case 2:
                self._apply_catalog_build_schema()
            case 3:
                self._apply_catalog_analysis_schema()
            case 4:
                self._apply_catalog_projection_build_schema()
            case 5:
                self._apply_catalog_operational_schema()
            case 6:
                self._apply_active_source_deletion_command_view()
            case 7:
                self._apply_catalog_analysis_scan_schema()
            case _:
                raise AssertionError(
                    f"No migration implementation for version {migration.version}."
                )

    def _validate_migration_result(self, migration: _SchemaMigration) -> None:
        match migration.version:
            case 1:
                tables, views = self._table_and_view_names()
                missing_tables = sorted(_V1_REQUIRED_TABLES - tables)
                missing_views = sorted(_V1_REQUIRED_VIEWS - views)
                missing_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V1_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    backend_columns = _V1_BACKEND_REQUIRED_COLUMNS[
                        self._context.sql_type
                    ].get(table_name, frozenset())
                    missing = sorted(
                        (required_columns | backend_columns)
                        - self._table_columns(table_name)
                    )
                    if missing:
                        missing_columns[table_name] = missing
                if missing_tables or missing_views or missing_columns:
                    raise SchemaCompatibilityError(
                        "The current schema migration did not create its complete "
                        "baseline: "
                        f"missing_tables={missing_tables} "
                        f"missing_views={missing_views} "
                        f"missing_columns={missing_columns}."
                    )
                self._validate_schema_contracts()
            case 2:
                tables, _views = self._table_and_view_names()
                missing_tables = sorted(_V2_REQUIRED_TABLES - tables)
                v2_missing_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V2_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    missing = sorted(required_columns - self._table_columns(table_name))
                    if missing:
                        v2_missing_columns[table_name] = missing
                if missing_tables or v2_missing_columns:
                    raise SchemaCompatibilityError(
                        "The durable catalog build migration is incomplete: "
                        f"missing_tables={missing_tables} "
                        f"missing_columns={v2_missing_columns}."
                    )
                self._validate_catalog_build_schema_contracts()
            case 3:
                tables, _views = self._table_and_view_names()
                missing_tables = sorted(_V3_REQUIRED_TABLES - tables)
                missing_analysis_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V3_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    missing = sorted(required_columns - self._table_columns(table_name))
                    if missing:
                        missing_analysis_columns[table_name] = missing
                if missing_tables or missing_analysis_columns:
                    raise SchemaCompatibilityError(
                        "The durable catalog analysis schema is incomplete: "
                        f"tables={missing_tables} columns={missing_analysis_columns}."
                    )
                self._validate_catalog_analysis_schema_contracts()
            case 4:
                tables, _views = self._table_and_view_names()
                missing_tables = sorted(_V4_REQUIRED_TABLES - tables)
                missing_projection_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V4_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    missing = sorted(required_columns - self._table_columns(table_name))
                    if missing:
                        missing_projection_columns[table_name] = missing
                if missing_tables or missing_projection_columns:
                    raise SchemaCompatibilityError(
                        "The durable catalog projection schema is incomplete: "
                        f"tables={missing_tables} columns={missing_projection_columns}."
                    )
                self._validate_catalog_projection_build_schema_contracts()
            case 5:
                tables, _views = self._table_and_view_names()
                missing_tables = sorted(_V5_REQUIRED_TABLES - tables)
                missing_operational_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V5_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    missing = sorted(required_columns - self._table_columns(table_name))
                    if missing:
                        missing_operational_columns[table_name] = missing
                if missing_tables or missing_operational_columns:
                    raise SchemaCompatibilityError(
                        "The catalog operational authority schema is incomplete: "
                        f"tables={missing_tables} "
                        f"columns={missing_operational_columns}."
                    )
                self._validate_catalog_operational_schema_contracts()
            case 6:
                _tables, views = self._table_and_view_names()
                missing_views = (
                    [] if "todelete_rm_commands" in views else ["todelete_rm_commands"]
                )
                definition = (
                    ""
                    if missing_views
                    else self._normalized_definition(
                        "todelete_rm_commands",
                        object_type="view",
                    )
                )
                missing_fragments = tuple(
                    self._normalize_sql(fragment)
                    for fragment in _V6_TODELETE_VIEW_DEFINITION_FRAGMENTS
                    if self._normalize_sql(fragment) not in definition
                )
                if missing_views or missing_fragments:
                    raise SchemaCompatibilityError(
                        "The active-source deletion command view is incomplete: "
                        f"missing_views={missing_views} "
                        f"missing_fragments={missing_fragments}."
                    )
            case 7:
                tables, _views = self._table_and_view_names()
                missing_tables = sorted(_V7_REQUIRED_TABLES - tables)
                missing_scan_columns: dict[str, list[str]] = {}
                for table_name, required_columns in _V7_REQUIRED_COLUMNS.items():
                    if table_name not in tables:
                        continue
                    missing = sorted(required_columns - self._table_columns(table_name))
                    if missing:
                        missing_scan_columns[table_name] = missing
                if missing_tables or missing_scan_columns:
                    raise SchemaCompatibilityError(
                        "The durable analysis scan schema is incomplete: "
                        f"tables={missing_tables} columns={missing_scan_columns}."
                    )
                self._validate_catalog_analysis_scan_schema_contracts()
            case _:
                raise AssertionError(
                    f"No migration validation for version {migration.version}."
                )

    def _apply_catalog_analysis_schema(self) -> None:
        with self._context.SQLConnector() as connector:
            for statement in self._catalog_analysis_statements():
                connector.execute(statement)

    def _apply_catalog_analysis_scan_schema(self) -> None:
        """Install a server-owned contiguous FILE_SPAM scan authority.

        A legacy ANALYZING build may already have accepted the former
        caller-constructed terminal token.  No forward migration can recover
        which ranges were actually observed, so require that build to be
        abandoned and rebuilt rather than inventing receipts.
        """

        with self._context.SQLConnector() as connector:
            unfinished = connector.fetch_one(
                "SELECT build_id, phase FROM catalog_builds "
                "WHERE phase IN ('ANALYZING', 'ARTIFACTS', 'SEALED') LIMIT 1"
            )
            if unfinished:
                raise SchemaCompatibilityError(
                    "Cannot install durable FILE_SPAM scan authority while legacy "
                    f"build {unfinished[0]!s} is {unfinished[1]!s}; abandon and "
                    "rebuild it so contiguous scan receipts can be established"
                )
            for statement in self._catalog_analysis_scan_statements():
                connector.execute(statement)

    def _apply_catalog_projection_build_schema(self) -> None:
        """Create resumable projection state after safely extending history.

        MariaDB and SQLite both auto-commit some DDL paths.  Every step is
        therefore independently rerunnable after an interrupted migration.
        """

        with self._context.SQLConnector() as connector:
            if "projection_sha256" not in self._table_columns(
                "catalog_revision_history"
            ):
                connector.execute(
                    "ALTER TABLE catalog_revision_history "
                    "ADD COLUMN projection_sha256 VARCHAR(64) NULL"
                    if self._context.sql_type == "mariadb"
                    else "ALTER TABLE catalog_revision_history "
                    "ADD COLUMN projection_sha256 TEXT NULL"
                )
            for statement in self._catalog_projection_build_statements():
                connector.execute(statement)
            empty_projection_sha256 = (
                "1837ec8c05bd8de86daee2888f575d68c04306633946aeac22a07316b607ae0e"
            )
            connector.execute(
                """
                UPDATE catalog_revision_history
                SET projection_sha256 = %s
                WHERE revision = 0 AND projection_sha256 IS NULL
                """,
                (empty_projection_sha256,),
            )
            current_row = connector.fetch_one("""
                SELECT current_revision
                FROM catalog_revision
                WHERE singleton_id = 1
                """)
            if current_row:
                CatalogProjectionRepository(
                    self._context
                )._revision_projection_sha256_with_connector(
                    connector,
                    int(current_row[0]),
                )
            max_row = connector.fetch_one("""
                SELECT MAX(revision)
                FROM (
                    SELECT revision FROM catalog_revision_history
                    UNION ALL
                    SELECT current_revision AS revision FROM catalog_revision
                ) AS revisions
                """)
            next_revision = max(1, int(max_row[0] or 0) + 1) if max_row else 1
            match self._context.sql_type:
                case "mariadb":
                    connector.execute(
                        """
                        INSERT IGNORE INTO catalog_revision_allocator (
                            singleton_id, next_revision
                        ) VALUES (1, %s)
                        """,
                        (next_revision,),
                    )
                case "sqlite":
                    connector.execute(
                        """
                        INSERT OR IGNORE INTO catalog_revision_allocator (
                            singleton_id, next_revision
                        ) VALUES (1, %s)
                        """,
                        (next_revision,),
                    )

    def _apply_catalog_operational_schema(self) -> None:
        """Install operational cutover storage without materializing source rows."""

        with self._context.SQLConnector() as connector:
            extensions = (
                (
                    "catalog_source_revision",
                    "deletion_request_generation",
                    (
                        "BIGINT UNSIGNED NOT NULL DEFAULT 0"
                        if self._context.sql_type == "mariadb"
                        else "INTEGER NOT NULL DEFAULT 0"
                    ),
                ),
                (
                    "catalog_source_galleries",
                    "upload_time_utc",
                    (
                        "DATETIME NULL"
                        if self._context.sql_type == "mariadb"
                        else "TEXT NULL"
                    ),
                ),
                (
                    "catalog_source_galleries",
                    "download_time_utc",
                    (
                        "DATETIME NULL"
                        if self._context.sql_type == "mariadb"
                        else "TEXT NULL"
                    ),
                ),
                (
                    "catalog_build_content_digests",
                    "duplicate_hash_deletion_candidate",
                    (
                        "BOOLEAN NOT NULL DEFAULT FALSE"
                        if self._context.sql_type == "mariadb"
                        else "INTEGER NOT NULL DEFAULT 0"
                    ),
                ),
                (
                    "todelete_gids",
                    "request_token",
                    (
                        "CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL"
                        if self._context.sql_type == "mariadb"
                        else "TEXT NULL"
                    ),
                ),
            )
            for table_name, column_name, definition in extensions:
                if column_name not in self._table_columns(table_name):
                    connector.execute(
                        f"ALTER TABLE {table_name} ADD COLUMN "
                        f"{column_name} {definition}"
                    )

            # Explicit deletion requests are normally tiny.  Still perform the
            # legacy token adoption in bounded statements so migration never
            # constructs or updates an unbounded in-memory batch.
            while True:
                rows = connector.fetch_all("""
                    SELECT gid
                    FROM todelete_gids
                    WHERE request_token IS NULL
                    ORDER BY gid
                    LIMIT 1000
                    """)
                if not rows:
                    break
                connector.execute_many(
                    """
                    UPDATE todelete_gids
                    SET request_token = %s
                    WHERE gid = %s AND request_token IS NULL
                    """,
                    [
                        (
                            sha256(
                                f"legacy-deletion:{int(row[0])}".encode()
                            ).hexdigest()[:32],
                            int(row[0]),
                        )
                        for row in rows
                    ],
                )
            for statement in self._catalog_operational_statements():
                connector.execute(statement)

    def _apply_active_source_deletion_command_view(self) -> None:
        """Replace the legacy-only deletion view after v5 tables exist."""

        self._to_delete._replace_todelete_rm_commands_with_active_authority_view()

    def _catalog_operational_statements(self) -> tuple[str, ...]:
        phases = (
            "'NORMALIZING_TIMES', 'REMOVED_GID_REQUESTS', "
            "'DELETION_CONSUMPTIONS', 'COMPLETE'"
        )
        if self._context.sql_type == "mariadb":
            return (
                f"""
                CREATE TABLE IF NOT EXISTS catalog_build_operational_state (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    preparation_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    operational_schema_version INT UNSIGNED NOT NULL,
                    phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    deletion_request_generation BIGINT UNSIGNED NOT NULL,
                    after_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    after_gid INT UNSIGNED NULL,
                    normalized_gallery_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    removed_gid_request_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    deletion_consumption_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    prepared_at VARCHAR(40) NOT NULL,
                    completed_at VARCHAR(40) NULL,
                    PRIMARY KEY (build_id),
                    CONSTRAINT catalog_build_operational_phase
                        CHECK (phase IN ({phases}))
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_removed_gid_requests (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    preparation_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gid INT UNSIGNED NOT NULL,
                    url TEXT NOT NULL,
                    request_token CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    PRIMARY KEY (build_id, preparation_id, gid),
                    UNIQUE INDEX catalog_removed_gid_request_token (request_token),
                    INDEX catalog_removed_gid_request_lookup (
                        gid, build_id, preparation_id
                    )
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_deletion_consumptions (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    preparation_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gid INT UNSIGNED NOT NULL,
                    deletion_request_token CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    PRIMARY KEY (build_id, preparation_id, gid),
                    INDEX catalog_deletion_consumption_lookup (
                        gid, deletion_request_token, build_id, preparation_id
                    )
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_operational_activations (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    source_revision BIGINT UNSIGNED NOT NULL,
                    preparation_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    operational_schema_version INT UNSIGNED NOT NULL,
                    activated_at VARCHAR(40) NOT NULL,
                    PRIMARY KEY (build_id),
                    UNIQUE INDEX catalog_operational_activation_revision (
                        source_revision
                    )
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_removed_gid_request_acks (
                    gid INT UNSIGNED NOT NULL PRIMARY KEY,
                    through_source_revision BIGINT UNSIGNED NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_gallery_redownload_times (
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
                    gallery_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    redownload_time_utc DATETIME NOT NULL,
                    INDEX catalog_gallery_redownload_name (gallery_name(191))
                )
                """,
                """
                CREATE UNIQUE INDEX IF NOT EXISTS todelete_gids_request_token
                ON todelete_gids (request_token)
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_download
                ON catalog_source_galleries (
                    build_id, gid, download_time_utc, gallery_key
                )
                """,
            )
        return (
            f"""
            CREATE TABLE IF NOT EXISTS catalog_build_operational_state (
                build_id TEXT NOT NULL PRIMARY KEY,
                preparation_id TEXT NOT NULL CHECK (length(preparation_id) = 32),
                operational_schema_version INTEGER NOT NULL
                    CHECK (operational_schema_version > 0),
                phase TEXT NOT NULL CHECK (phase IN ({phases})),
                deletion_request_generation INTEGER NOT NULL
                    CHECK (deletion_request_generation >= 0),
                after_gallery_key TEXT NULL,
                after_gid INTEGER NULL CHECK (after_gid > 0),
                normalized_gallery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (normalized_gallery_count >= 0),
                removed_gid_request_count INTEGER NOT NULL DEFAULT 0
                    CHECK (removed_gid_request_count >= 0),
                deletion_consumption_count INTEGER NOT NULL DEFAULT 0
                    CHECK (deletion_consumption_count >= 0),
                prepared_at TEXT NOT NULL,
                completed_at TEXT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_removed_gid_requests (
                build_id TEXT NOT NULL,
                preparation_id TEXT NOT NULL,
                gid INTEGER NOT NULL CHECK (gid > 0),
                url TEXT NOT NULL,
                request_token TEXT NOT NULL,
                PRIMARY KEY (build_id, preparation_id, gid),
                UNIQUE (request_token)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_removed_gid_request_lookup
            ON catalog_build_removed_gid_requests (
                gid, build_id, preparation_id
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_deletion_consumptions (
                build_id TEXT NOT NULL,
                preparation_id TEXT NOT NULL,
                gid INTEGER NOT NULL CHECK (gid > 0),
                deletion_request_token TEXT NOT NULL,
                PRIMARY KEY (build_id, preparation_id, gid)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_deletion_consumption_lookup
            ON catalog_build_deletion_consumptions (
                gid, deletion_request_token, build_id, preparation_id
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_operational_activations (
                build_id TEXT NOT NULL PRIMARY KEY,
                source_revision INTEGER NOT NULL UNIQUE CHECK (source_revision > 0),
                preparation_id TEXT NOT NULL,
                operational_schema_version INTEGER NOT NULL
                    CHECK (operational_schema_version > 0),
                activated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_removed_gid_request_acks (
                gid INTEGER NOT NULL PRIMARY KEY CHECK (gid > 0),
                through_source_revision INTEGER NOT NULL
                    CHECK (through_source_revision > 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_gallery_redownload_times (
                gallery_key TEXT NOT NULL PRIMARY KEY,
                gallery_name TEXT NOT NULL,
                redownload_time_utc TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_gallery_redownload_name
            ON catalog_gallery_redownload_times (gallery_name)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS todelete_gids_request_token
            ON todelete_gids (request_token)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_download
            ON catalog_source_galleries (
                build_id, gid, download_time_utc, gallery_key
            )
            """,
        )

    def _catalog_projection_build_statements(self) -> tuple[str, ...]:
        phases = (
            "'PREPARING_ARTIFACTS', 'STAGING_SELECTIONS', 'COMPLETE', "
            "'SEALED', 'PUBLISHED'"
        )
        states = "'DB_COMMITTED', 'PROJECTION_FINALIZED'"
        if self._context.sql_type == "mariadb":
            return (
                """
                CREATE TABLE IF NOT EXISTS catalog_revision_allocator (
                    singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
                    next_revision BIGINT UNSIGNED NOT NULL,
                    CONSTRAINT catalog_revision_allocator_singleton
                        CHECK (singleton_id = 1)
                )
                """,
                f"""
                CREATE TABLE IF NOT EXISTS catalog_build_projections (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    reserved_revision BIGINT UNSIGNED NOT NULL,
                    base_catalog_revision BIGINT UNSIGNED NOT NULL,
                    artifacts_required BOOLEAN NOT NULL,
                    phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_after_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    selection_after_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    selected_gallery_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    protected_artifact_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    staged_selection_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    projection_chain_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    projection_xor_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    projection_sum_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    projection_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    new_galleries BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    changed_galleries BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    removed_galleries BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    duplicate_losers BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    published_catalog_revision BIGINT UNSIGNED NULL,
                    created_at VARCHAR(40) NOT NULL,
                    updated_at VARCHAR(40) NOT NULL,
                    sealed_at VARCHAR(40) NULL,
                    published_at VARCHAR(40) NULL,
                    PRIMARY KEY (build_id),
                    UNIQUE INDEX catalog_build_projection_revision (
                        reserved_revision
                    ),
                    CONSTRAINT catalog_build_projection_phase
                        CHECK (phase IN ({phases})),
                    CONSTRAINT catalog_build_projection_build_fk
                        FOREIGN KEY (build_id) REFERENCES catalog_builds (build_id)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_build_projection_base_revision_fk
                        FOREIGN KEY (base_catalog_revision)
                        REFERENCES catalog_revision_history (revision)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_build_projection_published_revision_fk
                        FOREIGN KEY (published_catalog_revision)
                        REFERENCES catalog_revision_history (revision)
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_prepared_artifacts (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    payload_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_name_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    media_type VARCHAR(191) NOT NULL,
                    size_bytes BIGINT UNSIGNED NOT NULL,
                    sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    modified_at VARCHAR(40) NOT NULL,
                    protected BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (build_id, gallery_key),
                    UNIQUE INDEX catalog_build_prepared_artifact_id (
                        build_id, artifact_key
                    ),
                    UNIQUE INDEX catalog_build_prepared_artifact_name (
                        build_id, artifact_name_key
                    ),
                    CONSTRAINT catalog_build_prepared_artifacts_projection_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_build_projections (build_id)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_build_prepared_artifacts_source_fk
                        FOREIGN KEY (build_id, gallery_key)
                        REFERENCES catalog_source_galleries (build_id, gallery_key)
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_projection_items (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    publication_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    item_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    PRIMARY KEY (build_id, gallery_key),
                    UNIQUE INDEX catalog_build_projection_publication (
                        build_id, publication_key
                    ),
                    CONSTRAINT catalog_build_projection_items_projection_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_build_projections (build_id)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_build_projection_items_source_fk
                        FOREIGN KEY (build_id, gallery_key)
                        REFERENCES catalog_source_galleries (build_id, gallery_key)
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_projection_batches (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    batch_kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    batch_id VARCHAR(191) COLLATE utf8mb4_bin NOT NULL,
                    payload_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    item_count BIGINT UNSIGNED NOT NULL,
                    PRIMARY KEY (build_id, batch_kind, batch_id),
                    CONSTRAINT catalog_build_projection_batches_projection_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_build_projections (build_id)
                        ON DELETE RESTRICT
                )
                """,
                f"""
                CREATE TABLE IF NOT EXISTS catalog_projection_publication_receipts (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
                    source_revision BIGINT UNSIGNED NOT NULL,
                    catalog_revision BIGINT UNSIGNED NOT NULL,
                    projection_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    state VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    new_galleries BIGINT UNSIGNED NOT NULL,
                    changed_galleries BIGINT UNSIGNED NOT NULL,
                    removed_galleries BIGINT UNSIGNED NOT NULL,
                    duplicate_losers BIGINT UNSIGNED NOT NULL,
                    selected_galleries BIGINT UNSIGNED NOT NULL,
                    committed_at VARCHAR(40) NOT NULL,
                    finalized_at VARCHAR(40) NULL,
                    CONSTRAINT catalog_projection_receipt_state
                        CHECK (state IN ({states})),
                    CONSTRAINT catalog_projection_receipt_build_fk
                        FOREIGN KEY (build_id) REFERENCES catalog_builds (build_id)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_projection_receipt_source_revision_fk
                        FOREIGN KEY (source_revision)
                        REFERENCES catalog_source_revision_history (revision)
                        ON DELETE RESTRICT,
                    CONSTRAINT catalog_projection_receipt_catalog_revision_fk
                        FOREIGN KEY (catalog_revision)
                        REFERENCES catalog_revision_history (revision)
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE UNIQUE INDEX IF NOT EXISTS catalog_artifacts_revision_name_unique
                ON catalog_artifacts (revision, artifact_name_key)
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_publications_revision_content_source
                ON catalog_publications (
                    revision, content_sha256, source_gallery_name(191)
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_build_content_digest_key_order
                ON catalog_build_content_digests (
                    build_id, content_sha256, gallery_key
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_key_order
                ON catalog_source_galleries (build_id, gid, gallery_key)
                """,
            )
        return (
            """
            CREATE TABLE IF NOT EXISTS catalog_revision_allocator (
                singleton_id INTEGER NOT NULL PRIMARY KEY CHECK (singleton_id = 1),
                next_revision INTEGER NOT NULL CHECK (next_revision > 0)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS catalog_build_projections (
                build_id TEXT NOT NULL PRIMARY KEY,
                reserved_revision INTEGER NOT NULL UNIQUE CHECK (reserved_revision > 0),
                base_catalog_revision INTEGER NOT NULL CHECK (base_catalog_revision >= 0),
                artifacts_required INTEGER NOT NULL CHECK (artifacts_required IN (0, 1)),
                phase TEXT NOT NULL CHECK (phase IN ({phases})),
                artifact_after_gallery_key TEXT NULL,
                selection_after_gallery_key TEXT NULL,
                selected_gallery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (selected_gallery_count >= 0),
                protected_artifact_count INTEGER NOT NULL DEFAULT 0
                    CHECK (protected_artifact_count >= 0),
                staged_selection_count INTEGER NOT NULL DEFAULT 0
                    CHECK (staged_selection_count >= 0),
                projection_chain_sha256 TEXT NOT NULL
                    CHECK (length(projection_chain_sha256) = 64),
                projection_xor_sha256 TEXT NOT NULL
                    CHECK (length(projection_xor_sha256) = 64),
                projection_sum_sha256 TEXT NOT NULL
                    CHECK (length(projection_sum_sha256) = 64),
                projection_sha256 TEXT NULL CHECK (
                    projection_sha256 IS NULL OR length(projection_sha256) = 64
                ),
                new_galleries INTEGER NOT NULL DEFAULT 0 CHECK (new_galleries >= 0),
                changed_galleries INTEGER NOT NULL DEFAULT 0 CHECK (changed_galleries >= 0),
                removed_galleries INTEGER NOT NULL DEFAULT 0 CHECK (removed_galleries >= 0),
                duplicate_losers INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_losers >= 0),
                published_catalog_revision INTEGER NULL CHECK (published_catalog_revision >= 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                sealed_at TEXT NULL,
                published_at TEXT NULL,
                FOREIGN KEY (build_id) REFERENCES catalog_builds (build_id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (base_catalog_revision)
                    REFERENCES catalog_revision_history (revision)
                    ON DELETE RESTRICT,
                FOREIGN KEY (published_catalog_revision)
                    REFERENCES catalog_revision_history (revision)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_prepared_artifacts (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                artifact_key TEXT NOT NULL,
                artifact_name_key TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                name TEXT NOT NULL,
                location TEXT NOT NULL,
                media_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                modified_at TEXT NOT NULL,
                protected INTEGER NOT NULL DEFAULT 0 CHECK (protected IN (0, 1)),
                PRIMARY KEY (build_id, gallery_key),
                UNIQUE (build_id, artifact_key),
                UNIQUE (build_id, artifact_name_key),
                FOREIGN KEY (build_id) REFERENCES catalog_build_projections (build_id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (build_id, gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_projection_items (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                publication_key TEXT NOT NULL,
                item_sha256 TEXT NOT NULL CHECK (length(item_sha256) = 64),
                PRIMARY KEY (build_id, gallery_key),
                UNIQUE (build_id, publication_key),
                FOREIGN KEY (build_id) REFERENCES catalog_build_projections (build_id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (build_id, gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_projection_batches (
                build_id TEXT NOT NULL,
                batch_kind TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                item_count INTEGER NOT NULL CHECK (item_count >= 0),
                PRIMARY KEY (build_id, batch_kind, batch_id),
                FOREIGN KEY (build_id) REFERENCES catalog_build_projections (build_id)
                    ON DELETE RESTRICT
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS catalog_projection_publication_receipts (
                build_id TEXT NOT NULL PRIMARY KEY,
                source_revision INTEGER NOT NULL CHECK (source_revision > 0),
                catalog_revision INTEGER NOT NULL CHECK (catalog_revision >= 0),
                projection_sha256 TEXT NOT NULL CHECK (length(projection_sha256) = 64),
                state TEXT NOT NULL CHECK (state IN ({states})),
                new_galleries INTEGER NOT NULL CHECK (new_galleries >= 0),
                changed_galleries INTEGER NOT NULL CHECK (changed_galleries >= 0),
                removed_galleries INTEGER NOT NULL CHECK (removed_galleries >= 0),
                duplicate_losers INTEGER NOT NULL CHECK (duplicate_losers >= 0),
                selected_galleries INTEGER NOT NULL CHECK (selected_galleries >= 0),
                committed_at TEXT NOT NULL,
                finalized_at TEXT NULL,
                FOREIGN KEY (build_id) REFERENCES catalog_builds (build_id)
                    ON DELETE RESTRICT,
                FOREIGN KEY (source_revision)
                    REFERENCES catalog_source_revision_history (revision)
                    ON DELETE RESTRICT,
                FOREIGN KEY (catalog_revision)
                    REFERENCES catalog_revision_history (revision)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS catalog_artifacts_revision_name_unique
            ON catalog_artifacts (revision, artifact_name_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_publications_revision_content_source
            ON catalog_publications (revision, content_sha256, source_gallery_name)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_build_content_digest_key_order
            ON catalog_build_content_digests (build_id, content_sha256, gallery_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_key_order
            ON catalog_source_galleries (build_id, gid, gallery_key)
            """,
        )

    def _catalog_analysis_scan_statements(self) -> tuple[str, ...]:
        if self._context.sql_type == "mariadb":
            return (
                """
                CREATE TABLE IF NOT EXISTS catalog_build_analysis_scan_checkpoints (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    phase VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    generation BIGINT UNSIGNED NOT NULL,
                    minimum_occurrences BIGINT UNSIGNED NOT NULL,
                    cursor_sha256 VARCHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    output_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    state VARCHAR(16)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    updated_at VARCHAR(40) NOT NULL,
                    PRIMARY KEY (build_id, phase),
                    CONSTRAINT catalog_analysis_scan_checkpoint_phase
                        CHECK (phase = 'FILE_SPAM'),
                    CONSTRAINT catalog_analysis_scan_checkpoint_generation
                        CHECK (generation > 0),
                    CONSTRAINT catalog_analysis_scan_checkpoint_minimum
                        CHECK (minimum_occurrences > 0),
                    CONSTRAINT catalog_analysis_scan_checkpoint_cursor
                        CHECK (CHAR_LENGTH(cursor_sha256) IN (0, 64)),
                    CONSTRAINT catalog_analysis_scan_checkpoint_output
                        CHECK (CHAR_LENGTH(output_sha256) = 64),
                    CONSTRAINT catalog_analysis_scan_checkpoint_state
                        CHECK (state IN ('OPEN', 'COMPLETE')),
                    CONSTRAINT catalog_analysis_scan_checkpoint_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_analysis_scan_receipts (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    phase VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    batch_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    start_cursor_sha256 VARCHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    next_cursor_sha256 VARCHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    minimum_occurrences BIGINT UNSIGNED NOT NULL,
                    page_limit INT UNSIGNED NOT NULL,
                    input_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    output_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    row_count BIGINT UNSIGNED NOT NULL,
                    committed_generation BIGINT UNSIGNED NOT NULL,
                    committed_at VARCHAR(40) NOT NULL,
                    PRIMARY KEY (build_id, phase, batch_key),
                    UNIQUE INDEX catalog_analysis_scan_receipt_start (
                        build_id, phase, start_cursor_sha256
                    ),
                    INDEX catalog_analysis_scan_receipt_chronology (
                        build_id, phase, committed_at, batch_key
                    ),
                    CONSTRAINT catalog_analysis_scan_receipt_phase
                        CHECK (phase = 'FILE_SPAM'),
                    CONSTRAINT catalog_analysis_scan_receipt_minimum
                        CHECK (minimum_occurrences > 0),
                    CONSTRAINT catalog_analysis_scan_receipt_page_limit
                        CHECK (page_limit > 0),
                    CONSTRAINT catalog_analysis_scan_receipt_rows
                        CHECK (row_count >= 0 AND row_count <= page_limit),
                    CONSTRAINT catalog_analysis_scan_receipt_generation
                        CHECK (committed_generation > 0),
                    CONSTRAINT catalog_analysis_scan_receipt_cursors
                        CHECK (
                            CHAR_LENGTH(start_cursor_sha256) IN (0, 64)
                            AND CHAR_LENGTH(next_cursor_sha256) IN (0, 64)
                        ),
                    CONSTRAINT catalog_analysis_scan_receipt_input
                        CHECK (CHAR_LENGTH(input_sha256) = 64),
                    CONSTRAINT catalog_analysis_scan_receipt_output
                        CHECK (CHAR_LENGTH(output_sha256) = 64),
                    CONSTRAINT catalog_analysis_scan_receipt_checkpoint_fk
                        FOREIGN KEY (build_id, phase)
                        REFERENCES catalog_build_analysis_scan_checkpoints (
                            build_id, phase
                        )
                        ON DELETE RESTRICT
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_galleries_empty_order
                ON catalog_source_galleries (
                    build_id, source_complete, staged_file_count, gallery_key
                )
                """,
            )
        return (
            """
            CREATE TABLE IF NOT EXISTS catalog_build_analysis_scan_checkpoints (
                build_id TEXT COLLATE BINARY NOT NULL,
                phase TEXT COLLATE BINARY NOT NULL CHECK (phase = 'FILE_SPAM'),
                generation INTEGER NOT NULL CHECK (generation > 0),
                minimum_occurrences INTEGER NOT NULL
                    CHECK (minimum_occurrences > 0),
                cursor_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(cursor_sha256) IN (0, 64)),
                output_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(output_sha256) = 64),
                state TEXT COLLATE BINARY NOT NULL
                    CHECK (state IN ('OPEN', 'COMPLETE')),
                updated_at TEXT NOT NULL,
                PRIMARY KEY (build_id, phase),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_analysis_scan_receipts (
                build_id TEXT COLLATE BINARY NOT NULL,
                phase TEXT COLLATE BINARY NOT NULL CHECK (phase = 'FILE_SPAM'),
                batch_key TEXT COLLATE BINARY NOT NULL
                    CHECK (length(batch_key) = 64),
                start_cursor_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(start_cursor_sha256) IN (0, 64)),
                next_cursor_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(next_cursor_sha256) IN (0, 64)),
                minimum_occurrences INTEGER NOT NULL
                    CHECK (minimum_occurrences > 0),
                page_limit INTEGER NOT NULL CHECK (page_limit > 0),
                input_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(input_sha256) = 64),
                output_sha256 TEXT COLLATE BINARY NOT NULL
                    CHECK (length(output_sha256) = 64),
                row_count INTEGER NOT NULL
                    CHECK (row_count >= 0 AND row_count <= page_limit),
                committed_generation INTEGER NOT NULL
                    CHECK (committed_generation > 0),
                committed_at TEXT NOT NULL,
                PRIMARY KEY (build_id, phase, batch_key),
                UNIQUE (build_id, phase, start_cursor_sha256),
                FOREIGN KEY (build_id, phase)
                    REFERENCES catalog_build_analysis_scan_checkpoints (
                        build_id, phase
                    )
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_analysis_scan_receipt_chronology
            ON catalog_build_analysis_scan_receipts (
                build_id, phase, committed_at, batch_key
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_empty_order
            ON catalog_source_galleries (
                build_id, source_complete, staged_file_count, gallery_key
            )
            """,
        )

    def _catalog_analysis_statements(self) -> tuple[str, ...]:
        phases = (
            "'SOURCE_MANIFESTS', 'FILE_SPAM', 'CONTENT_DIGESTS', "
            "'CONTENT_OWNERS', 'GID_WINNERS', 'FINAL_ANALYSES'"
        )
        if self._context.sql_type == "mariadb":
            return (
                f"""
                CREATE TABLE IF NOT EXISTS catalog_build_analysis_phases (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    phase VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    completed_at VARCHAR(40) NOT NULL,
                    PRIMARY KEY (build_id, phase),
                    CONSTRAINT catalog_build_analysis_phase_value
                        CHECK (phase IN ({phases})),
                    CONSTRAINT catalog_build_analysis_phases_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_excluded_file_hashes (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    PRIMARY KEY (build_id, sha256),
                    CONSTRAINT catalog_build_excluded_hashes_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_content_digests (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    content_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    PRIMARY KEY (build_id, gallery_key),
                    INDEX catalog_build_content_digest_order (
                        build_id, content_sha256, gallery_name
                    ),
                    CONSTRAINT catalog_build_content_digests_gallery_fk
                        FOREIGN KEY (build_id, gallery_key)
                        REFERENCES catalog_source_galleries (
                            build_id, gallery_key
                        )
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_content_owners (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    content_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    owner_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    owner_gallery_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    PRIMARY KEY (build_id, content_sha256),
                    INDEX catalog_build_content_owner_gallery (
                        build_id, owner_gallery_key
                    ),
                    CONSTRAINT catalog_build_content_owners_gallery_fk
                        FOREIGN KEY (build_id, owner_gallery_key)
                        REFERENCES catalog_source_galleries (
                            build_id, gallery_key
                        )
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_gid_winners (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gid INT UNSIGNED NOT NULL,
                    winner_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    winner_gallery_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    PRIMARY KEY (build_id, gid),
                    INDEX catalog_build_gid_winner_gallery (
                        build_id, winner_gallery_key
                    ),
                    CONSTRAINT catalog_build_gid_winners_gallery_fk
                        FOREIGN KEY (build_id, winner_gallery_key)
                        REFERENCES catalog_source_galleries (
                            build_id, gallery_key
                        )
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_galleries_name_order
                ON catalog_source_galleries (
                    build_id, gallery_name(255), gallery_key
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_name_order
                ON catalog_source_galleries (
                    build_id, gid, gallery_name(255), gallery_key
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS catalog_source_files_gallery_hash_order
                ON catalog_source_files (
                    build_id, gallery_key, sha256, file_key
                )
                """,
            )
        return (
            f"""
            CREATE TABLE IF NOT EXISTS catalog_build_analysis_phases (
                build_id TEXT NOT NULL,
                phase TEXT NOT NULL CHECK (phase IN ({phases})),
                completed_at TEXT NOT NULL,
                PRIMARY KEY (build_id, phase),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_excluded_file_hashes (
                build_id TEXT NOT NULL,
                sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                PRIMARY KEY (build_id, sha256),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_content_digests (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                gallery_name TEXT NOT NULL,
                content_sha256 TEXT NULL CHECK (
                    content_sha256 IS NULL OR length(content_sha256) = 64
                ),
                PRIMARY KEY (build_id, gallery_key),
                FOREIGN KEY (build_id, gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_build_content_digest_order
            ON catalog_build_content_digests (
                build_id, content_sha256, gallery_name
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_content_owners (
                build_id TEXT NOT NULL,
                content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
                owner_gallery_key TEXT NOT NULL,
                owner_gallery_name TEXT NOT NULL,
                PRIMARY KEY (build_id, content_sha256),
                FOREIGN KEY (build_id, owner_gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_build_content_owner_gallery
            ON catalog_build_content_owners (build_id, owner_gallery_key)
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_gid_winners (
                build_id TEXT NOT NULL,
                gid INTEGER NOT NULL CHECK (gid > 0),
                winner_gallery_key TEXT NOT NULL,
                winner_gallery_name TEXT NOT NULL,
                PRIMARY KEY (build_id, gid),
                FOREIGN KEY (build_id, winner_gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_build_gid_winner_gallery
            ON catalog_build_gid_winners (build_id, winner_gallery_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_name_order
            ON catalog_source_galleries (build_id, gallery_name, gallery_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid_name_order
            ON catalog_source_galleries (
                build_id, gid, gallery_name, gallery_key
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_files_gallery_hash_order
            ON catalog_source_files (build_id, gallery_key, sha256, file_key)
            """,
        )

    def _apply_catalog_build_schema(self) -> None:
        with self._context.SQLConnector() as connector:
            for statement in self._catalog_build_statements():
                connector.execute(statement)
            if self._context.sql_type == "mariadb":
                connector.execute("""
                    INSERT IGNORE INTO catalog_build_control (
                        singleton_id, working_build_id
                    ) VALUES (1, NULL)
                    """)
                connector.execute("""
                    INSERT IGNORE INTO catalog_source_revision_history (
                        revision, build_id, published_at, gallery_count, file_count
                    ) VALUES (0, NULL, '1970-01-01T00:00:00+00:00', 0, 0)
                    """)
                connector.execute("""
                    INSERT IGNORE INTO catalog_source_revision (
                        singleton_id,
                        current_revision,
                        active_build_id,
                        published_at,
                        gallery_count,
                        file_count
                    ) VALUES (
                        1, 0, NULL, '1970-01-01T00:00:00+00:00', 0, 0
                    )
                    """)
            else:
                connector.execute("""
                    INSERT OR IGNORE INTO catalog_build_control (
                        singleton_id, working_build_id
                    ) VALUES (1, NULL)
                    """)
                connector.execute("""
                    INSERT OR IGNORE INTO catalog_source_revision_history (
                        revision, build_id, published_at, gallery_count, file_count
                    ) VALUES (0, NULL, '1970-01-01T00:00:00+00:00', 0, 0)
                    """)
                connector.execute("""
                    INSERT OR IGNORE INTO catalog_source_revision (
                        singleton_id,
                        current_revision,
                        active_build_id,
                        published_at,
                        gallery_count,
                        file_count
                    ) VALUES (
                        1, 0, NULL, '1970-01-01T00:00:00+00:00', 0, 0
                    )
                    """)

    def _catalog_build_statements(self) -> tuple[str, ...]:
        phases = (
            "'DISCOVERING', 'STAGING', 'ANALYZING', "
            "'ARTIFACTS', 'SEALED', 'PUBLISHED', 'ABANDONED'"
        )
        if self._context.sql_type == "mariadb":
            return (
                f"""
                CREATE TABLE IF NOT EXISTS catalog_builds (
                    build_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin
                        NOT NULL PRIMARY KEY,
                    scope_key TEXT NOT NULL,
                    discovery_epoch VARCHAR(191) COLLATE utf8mb4_bin NOT NULL,
                    discovery_tree_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    ingest_generation BIGINT UNSIGNED NOT NULL,
                    owner_token VARCHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    base_source_revision BIGINT UNSIGNED NOT NULL,
                    base_active_build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    discovered_gallery_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    expected_gallery_count BIGINT UNSIGNED NULL,
                    staged_gallery_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    staged_file_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    analyzed_gallery_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    created_at VARCHAR(40) NOT NULL,
                    updated_at VARCHAR(40) NOT NULL,
                    published_source_revision BIGINT UNSIGNED NULL,
                    seal_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    CONSTRAINT catalog_build_phase CHECK (phase IN ({phases})),
                    CONSTRAINT catalog_build_seal_length CHECK (
                        seal_sha256 IS NULL OR CHAR_LENGTH(seal_sha256) = 64
                    )
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_control (
                    singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
                    working_build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    CONSTRAINT catalog_build_control_singleton
                        CHECK (singleton_id = 1),
                    CONSTRAINT catalog_build_control_working_fk
                        FOREIGN KEY (working_build_id)
                        REFERENCES catalog_builds (build_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_discoveries (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_name TEXT NOT NULL,
                    source_locator LONGTEXT NOT NULL,
                    metadata_fingerprint LONGTEXT NULL,
                    PRIMARY KEY (build_id, gallery_key),
                    CONSTRAINT catalog_build_discoveries_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_build_batches (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    batch_kind VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    batch_id VARCHAR(191) COLLATE utf8mb4_bin NOT NULL,
                    payload_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    item_count BIGINT UNSIGNED NOT NULL,
                    file_count BIGINT UNSIGNED NOT NULL,
                    PRIMARY KEY (build_id, batch_kind, batch_id),
                    CONSTRAINT catalog_build_batches_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_source_galleries (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_name TEXT NOT NULL,
                    gid INT UNSIGNED NOT NULL,
                    title TEXT NOT NULL,
                    comment LONGTEXT NOT NULL,
                    upload_account TEXT NOT NULL,
                    upload_time VARCHAR(40) NOT NULL,
                    download_time VARCHAR(40) NOT NULL,
                    modified_time VARCHAR(40) NOT NULL,
                    source_manifest_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    source_manifest_version INT UNSIGNED NULL,
                    scan_observation_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    scan_observation_version INT UNSIGNED NULL,
                    metadata_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    page_count BIGINT UNSIGNED NULL,
                    directory_entry_count BIGINT UNSIGNED NULL,
                    directory_observation_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    raw_content_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    content_sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    duplicate_of_gallery_name TEXT NULL,
                    duplicate_of_gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    expected_file_count BIGINT UNSIGNED NULL,
                    staged_file_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                    source_complete BOOLEAN NOT NULL DEFAULT FALSE,
                    analysis_complete BOOLEAN NOT NULL DEFAULT FALSE,
                    selected BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (build_id, gallery_key),
                    INDEX catalog_source_galleries_gid (build_id, gid),
                    INDEX catalog_source_galleries_content (
                        build_id, content_sha256
                    ),
                    CONSTRAINT catalog_source_galleries_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_source_files (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    file_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    file_sort_key VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    file_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
                    relative_locator LONGTEXT NULL,
                    device VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
                    inode VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
                    modified_ns VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    changed_ns VARCHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    size_bytes BIGINT UNSIGNED NOT NULL,
                    sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    PRIMARY KEY (build_id, gallery_key, file_key),
                    UNIQUE INDEX catalog_source_files_name (
                        build_id, gallery_key, file_name
                    ),
                    INDEX catalog_source_files_order (
                        build_id, gallery_key, file_sort_key, file_name, file_key
                    ),
                    INDEX catalog_source_files_hash_order (
                        build_id, sha256, gallery_key, file_key
                    ),
                    CONSTRAINT catalog_source_files_gallery_fk
                        FOREIGN KEY (build_id, gallery_key)
                        REFERENCES catalog_source_galleries (
                            build_id, gallery_key
                        )
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_source_tags (
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    gallery_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    position INT UNSIGNED NOT NULL,
                    tag_name TEXT NOT NULL,
                    tag_value TEXT NOT NULL,
                    PRIMARY KEY (build_id, gallery_key, position),
                    CONSTRAINT catalog_source_tags_gallery_fk
                        FOREIGN KEY (build_id, gallery_key)
                        REFERENCES catalog_source_galleries (
                            build_id, gallery_key
                        )
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_file_hash_cache (
                    cache_key CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
                    source_key LONGTEXT NOT NULL,
                    fingerprint LONGTEXT NOT NULL,
                    sha256 CHAR(64)
                        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    cached_at VARCHAR(40) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_source_revision_history (
                    revision BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    published_at VARCHAR(40) NOT NULL,
                    gallery_count BIGINT UNSIGNED NOT NULL,
                    file_count BIGINT UNSIGNED NOT NULL,
                    UNIQUE INDEX catalog_source_revision_build (build_id),
                    CONSTRAINT catalog_source_revision_history_build_fk
                        FOREIGN KEY (build_id)
                        REFERENCES catalog_builds (build_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_source_revision (
                    singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
                    current_revision BIGINT UNSIGNED NOT NULL,
                    active_build_id CHAR(32)
                        CHARACTER SET ascii COLLATE ascii_bin NULL,
                    published_at VARCHAR(40) NOT NULL,
                    gallery_count BIGINT UNSIGNED NOT NULL,
                    file_count BIGINT UNSIGNED NOT NULL,
                    CONSTRAINT catalog_source_revision_singleton
                        CHECK (singleton_id = 1),
                    CONSTRAINT catalog_source_revision_history_fk
                        FOREIGN KEY (current_revision)
                        REFERENCES catalog_source_revision_history (revision),
                    CONSTRAINT catalog_source_revision_build_fk
                        FOREIGN KEY (active_build_id)
                        REFERENCES catalog_builds (build_id)
                )
                """,
            )
        return (
            f"""
            CREATE TABLE IF NOT EXISTS catalog_builds (
                build_id TEXT NOT NULL PRIMARY KEY CHECK (length(build_id) = 32),
                scope_key TEXT NOT NULL CHECK (length(scope_key) > 0),
                discovery_epoch TEXT NOT NULL CHECK (length(discovery_epoch) > 0),
                discovery_tree_sha256 TEXT NULL CHECK (
                    discovery_tree_sha256 IS NULL
                    OR length(discovery_tree_sha256) = 64
                ),
                phase TEXT NOT NULL CHECK (phase IN ({phases})),
                ingest_generation INTEGER NOT NULL CHECK (ingest_generation >= 0),
                owner_token TEXT NOT NULL,
                base_source_revision INTEGER NOT NULL
                    CHECK (base_source_revision >= 0),
                base_active_build_id TEXT NULL,
                discovered_gallery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (discovered_gallery_count >= 0),
                expected_gallery_count INTEGER NULL
                    CHECK (expected_gallery_count >= 0),
                staged_gallery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (staged_gallery_count >= 0),
                staged_file_count INTEGER NOT NULL DEFAULT 0
                    CHECK (staged_file_count >= 0),
                analyzed_gallery_count INTEGER NOT NULL DEFAULT 0
                    CHECK (analyzed_gallery_count >= 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                published_source_revision INTEGER NULL
                    CHECK (published_source_revision > 0),
                seal_sha256 TEXT NULL
                    CHECK (seal_sha256 IS NULL OR length(seal_sha256) = 64)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_control (
                singleton_id INTEGER NOT NULL PRIMARY KEY
                    CHECK (singleton_id = 1),
                working_build_id TEXT NULL,
                FOREIGN KEY (working_build_id)
                    REFERENCES catalog_builds (build_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_discoveries (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                gallery_name TEXT NOT NULL,
                source_locator TEXT NOT NULL,
                metadata_fingerprint TEXT NULL,
                PRIMARY KEY (build_id, gallery_key),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_build_batches (
                build_id TEXT NOT NULL,
                batch_kind TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
                    CHECK (length(payload_sha256) = 64),
                item_count INTEGER NOT NULL CHECK (item_count >= 0),
                file_count INTEGER NOT NULL CHECK (file_count >= 0),
                PRIMARY KEY (build_id, batch_kind, batch_id),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_source_galleries (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                gallery_name TEXT NOT NULL,
                gid INTEGER NOT NULL CHECK (gid > 0),
                title TEXT NOT NULL,
                comment TEXT NOT NULL,
                upload_account TEXT NOT NULL,
                upload_time TEXT NOT NULL,
                download_time TEXT NOT NULL,
                modified_time TEXT NOT NULL,
                source_manifest_sha256 TEXT NULL CHECK (
                    source_manifest_sha256 IS NULL
                    OR length(source_manifest_sha256) = 64
                ),
                source_manifest_version INTEGER NULL
                    CHECK (source_manifest_version > 0),
                scan_observation_sha256 TEXT NULL CHECK (
                    scan_observation_sha256 IS NULL
                    OR length(scan_observation_sha256) = 64
                ),
                scan_observation_version INTEGER NULL
                    CHECK (scan_observation_version > 0),
                metadata_sha256 TEXT NULL CHECK (
                    metadata_sha256 IS NULL OR length(metadata_sha256) = 64
                ),
                page_count INTEGER NULL CHECK (page_count >= 0),
                directory_entry_count INTEGER NULL
                    CHECK (directory_entry_count >= 0),
                directory_observation_sha256 TEXT NULL CHECK (
                    directory_observation_sha256 IS NULL
                    OR length(directory_observation_sha256) = 64
                ),
                raw_content_sha256 TEXT NULL CHECK (
                    raw_content_sha256 IS NULL
                    OR length(raw_content_sha256) = 64
                ),
                content_sha256 TEXT NULL CHECK (
                    content_sha256 IS NULL OR length(content_sha256) = 64
                ),
                duplicate_of_gallery_name TEXT NULL,
                duplicate_of_gallery_key TEXT NULL,
                expected_file_count INTEGER NULL
                    CHECK (expected_file_count >= 0),
                staged_file_count INTEGER NOT NULL DEFAULT 0
                    CHECK (staged_file_count >= 0),
                source_complete INTEGER NOT NULL DEFAULT 0
                    CHECK (source_complete IN (0, 1)),
                analysis_complete INTEGER NOT NULL DEFAULT 0
                    CHECK (analysis_complete IN (0, 1)),
                selected INTEGER NOT NULL DEFAULT 0
                    CHECK (selected IN (0, 1)),
                PRIMARY KEY (build_id, gallery_key),
                FOREIGN KEY (build_id)
                    REFERENCES catalog_builds (build_id)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_gid
            ON catalog_source_galleries (build_id, gid)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_galleries_content
            ON catalog_source_galleries (build_id, content_sha256)
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_source_files (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                file_key TEXT NOT NULL,
                file_sort_key TEXT NOT NULL,
                file_name TEXT NOT NULL,
                relative_locator TEXT NULL,
                device TEXT NULL,
                inode TEXT NULL,
                modified_ns TEXT NULL,
                changed_ns TEXT NULL,
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                PRIMARY KEY (build_id, gallery_key, file_key),
                FOREIGN KEY (build_id, gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS catalog_source_files_name
            ON catalog_source_files (build_id, gallery_key, file_name)
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_files_order
            ON catalog_source_files (
                build_id, gallery_key, file_sort_key, file_name, file_key
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_source_files_hash_order
            ON catalog_source_files (build_id, sha256, gallery_key, file_key)
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_source_tags (
                build_id TEXT NOT NULL,
                gallery_key TEXT NOT NULL,
                position INTEGER NOT NULL CHECK (position >= 0),
                tag_name TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                PRIMARY KEY (build_id, gallery_key, position),
                FOREIGN KEY (build_id, gallery_key)
                    REFERENCES catalog_source_galleries (build_id, gallery_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_file_hash_cache (
                cache_key TEXT NOT NULL PRIMARY KEY
                    CHECK (length(cache_key) = 64),
                source_key TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                cached_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_source_revision_history (
                revision INTEGER NOT NULL PRIMARY KEY CHECK (revision >= 0),
                build_id TEXT NULL UNIQUE,
                published_at TEXT NOT NULL,
                gallery_count INTEGER NOT NULL CHECK (gallery_count >= 0),
                file_count INTEGER NOT NULL CHECK (file_count >= 0),
                FOREIGN KEY (build_id) REFERENCES catalog_builds (build_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_source_revision (
                singleton_id INTEGER NOT NULL PRIMARY KEY
                    CHECK (singleton_id = 1),
                current_revision INTEGER NOT NULL CHECK (current_revision >= 0),
                active_build_id TEXT NULL,
                published_at TEXT NOT NULL,
                gallery_count INTEGER NOT NULL CHECK (gallery_count >= 0),
                file_count INTEGER NOT NULL CHECK (file_count >= 0),
                FOREIGN KEY (current_revision)
                    REFERENCES catalog_source_revision_history (revision),
                FOREIGN KEY (active_build_id)
                    REFERENCES catalog_builds (build_id)
            )
            """,
        )

    def _apply_current_schema(self) -> None:
        self._maintenance._create_database_maintenance_state_table()
        self._coordination._create_gallery_ingest_state_table()
        self._queue._create_todownload_gids_table()
        self._gallery_ids._create_galleries_names_table()
        self._gallery_gids._create_galleries_gids_table()
        self._to_delete._create_todelete_gids_table()
        self._gallery_times._create_galleries_download_times_table()
        self._gallery_times._create_galleries_redownload_times_table()
        self._gallery_times._create_galleries_upload_times_table()
        self._removed._create_removed_galleries_gids_table()
        self._gallery_times._create_galleries_modified_times_table()
        self._gallery_times._create_galleries_access_times_table()
        self._gallery_titles._create_galleries_titles_table()
        self._upload_accounts._create_upload_account_table()
        self._gallery_comments._create_galleries_comments_table()
        self._files._create_files_names_table()
        self._source_manifests._create_gallery_source_manifests_table()
        self._gallery_infos._create_galleries_infos_view()
        self._files._create_galleries_files_hashs_tables()
        self._files._create_gallery_image_hash_view()
        self._gallery_infos._create_duplicate_hash_in_gallery_view()
        self._deduplication._create_gallery_content_hashes_table()
        self._deduplication._create_gallery_duplicate_warnings_table()
        self._deduplication._create_gallery_duplicate_warnings_names_view()
        self._to_delete._create_todelete_gallery_candidates_view()
        self._to_delete._create_todelete_galleries_table()
        self._to_delete._create_todelete_rm_commands_view()
        self._queue._create_pending_download_gids_view()
        self._gallery_tags._create_galleries_tags_table()
        self._apply_catalog_projection()
        self._apply_catalog_revision_history()

    def _apply_catalog_projection(self) -> None:
        with self._context.SQLConnector() as connector:
            for statement in self._catalog_projection_statements():
                connector.execute(statement)
            match self._context.sql_type:
                case "mariadb":
                    connector.execute("""
                        INSERT IGNORE INTO catalog_revision (
                            singleton_id,
                            current_revision,
                            published_at,
                            publication_count
                        ) VALUES (1, 0, '1970-01-01T00:00:00+00:00', 0)
                        """)
                case "sqlite":
                    connector.execute("""
                        INSERT OR IGNORE INTO catalog_revision (
                            singleton_id,
                            current_revision,
                            published_at,
                            publication_count
                        ) VALUES (1, 0, '1970-01-01T00:00:00+00:00', 0)
                        """)

    def _apply_catalog_revision_history(self) -> None:
        with self._context.SQLConnector() as connector:
            match self._context.sql_type:
                case "mariadb":
                    connector.execute("""
                        CREATE TABLE IF NOT EXISTS catalog_revision_history (
                            revision BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                            published_at VARCHAR(40) NOT NULL,
                            publication_count BIGINT UNSIGNED NOT NULL
                        )
                        """)
                    connector.execute("""
                        INSERT IGNORE INTO catalog_revision_history (
                            revision,
                            published_at,
                            publication_count
                        ) VALUES (0, '1970-01-01T00:00:00+00:00', 0)
                        """)
                case "sqlite":
                    connector.execute("""
                        CREATE TABLE IF NOT EXISTS catalog_revision_history (
                            revision INTEGER NOT NULL PRIMARY KEY
                                CHECK (revision >= 0),
                            published_at TEXT NOT NULL,
                            publication_count INTEGER NOT NULL
                                CHECK (publication_count >= 0)
                        )
                        """)
                    connector.execute("""
                        INSERT OR IGNORE INTO catalog_revision_history (
                            revision,
                            published_at,
                            publication_count
                        ) VALUES (0, '1970-01-01T00:00:00+00:00', 0)
                        """)

    def _catalog_projection_statements(self) -> tuple[str, ...]:
        if self._context.sql_type == "mariadb":
            return (
                """
                CREATE TABLE IF NOT EXISTS catalog_revision (
                    singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
                    current_revision BIGINT UNSIGNED NOT NULL,
                    published_at VARCHAR(40) NOT NULL,
                    publication_count BIGINT UNSIGNED NOT NULL,
                    CONSTRAINT catalog_revision_singleton CHECK (singleton_id = 1)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_publications (
                    revision BIGINT UNSIGNED NOT NULL,
                    publication_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    publication_id TEXT NOT NULL,
                    gid INT UNSIGNED NOT NULL,
                    title TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_gallery_name TEXT NOT NULL,
                    content_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
                    sort_title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    language VARCHAR(64) NOT NULL,
                    published_at VARCHAR(40) NOT NULL,
                    modified_at VARCHAR(40) NOT NULL,
                    redownload_required BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (revision, publication_key),
                    UNIQUE INDEX catalog_publications_revision_gid (revision, gid)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_contributors (
                    revision BIGINT UNSIGNED NOT NULL,
                    publication_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    position INT UNSIGNED NOT NULL,
                    name TEXT NOT NULL,
                    role VARCHAR(64) NOT NULL,
                    sort_as TEXT NULL,
                    PRIMARY KEY (revision, publication_key, position),
                    CONSTRAINT catalog_contributors_publication_fk
                        FOREIGN KEY (revision, publication_key)
                        REFERENCES catalog_publications (revision, publication_key)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_subjects (
                    revision BIGINT UNSIGNED NOT NULL,
                    publication_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    position INT UNSIGNED NOT NULL,
                    name TEXT NOT NULL,
                    scheme TEXT NULL,
                    code TEXT NULL,
                    PRIMARY KEY (revision, publication_key, position),
                    CONSTRAINT catalog_subjects_publication_fk
                        FOREIGN KEY (revision, publication_key)
                        REFERENCES catalog_publications (revision, publication_key)
                        ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS catalog_artifacts (
                    revision BIGINT UNSIGNED NOT NULL,
                    artifact_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_name_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    publication_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    artifact_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    media_type VARCHAR(191) NOT NULL,
                    size_bytes BIGINT UNSIGNED NOT NULL,
                    sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    modified_at VARCHAR(40) NOT NULL,
                    PRIMARY KEY (revision, artifact_key),
                    INDEX catalog_artifacts_name (revision, artifact_name_key),
                    CONSTRAINT catalog_artifacts_publication_fk
                        FOREIGN KEY (revision, publication_key)
                        REFERENCES catalog_publications (revision, publication_key)
                        ON DELETE CASCADE
                )
                """,
            )
        return (
            """
            CREATE TABLE IF NOT EXISTS catalog_revision (
                singleton_id INTEGER NOT NULL PRIMARY KEY CHECK (singleton_id = 1),
                current_revision INTEGER NOT NULL CHECK (current_revision >= 0),
                published_at TEXT NOT NULL,
                publication_count INTEGER NOT NULL CHECK (publication_count >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_publications (
                revision INTEGER NOT NULL,
                publication_key TEXT NOT NULL,
                publication_id TEXT NOT NULL,
                gid INTEGER NOT NULL CHECK (gid > 0),
                title TEXT NOT NULL,
                source_title TEXT NOT NULL,
                source_gallery_name TEXT NOT NULL,
                content_sha256 TEXT NULL CHECK (
                    content_sha256 IS NULL OR length(content_sha256) = 64
                ),
                sort_title TEXT NOT NULL,
                summary TEXT NOT NULL,
                language TEXT NOT NULL,
                published_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                redownload_required INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (revision, publication_key),
                UNIQUE (revision, gid)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_contributors (
                revision INTEGER NOT NULL,
                publication_key TEXT NOT NULL,
                position INTEGER NOT NULL CHECK (position >= 0),
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                sort_as TEXT NULL,
                PRIMARY KEY (revision, publication_key, position),
                FOREIGN KEY (revision, publication_key)
                    REFERENCES catalog_publications (revision, publication_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_subjects (
                revision INTEGER NOT NULL,
                publication_key TEXT NOT NULL,
                position INTEGER NOT NULL CHECK (position >= 0),
                name TEXT NOT NULL,
                scheme TEXT NULL,
                code TEXT NULL,
                PRIMARY KEY (revision, publication_key, position),
                FOREIGN KEY (revision, publication_key)
                    REFERENCES catalog_publications (revision, publication_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_artifacts (
                revision INTEGER NOT NULL,
                artifact_key TEXT NOT NULL,
                artifact_name_key TEXT NOT NULL,
                publication_key TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                name TEXT NOT NULL,
                location TEXT NOT NULL,
                media_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                sha256 TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                PRIMARY KEY (revision, artifact_key),
                FOREIGN KEY (revision, publication_key)
                    REFERENCES catalog_publications (revision, publication_key)
                    ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS catalog_artifacts_name
            ON catalog_artifacts (revision, artifact_name_key)
            """,
        )
