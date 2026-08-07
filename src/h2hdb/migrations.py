__all__ = [
    "LATEST_SCHEMA_VERSION",
    "MINIMUM_SCHEMA_VERSION",
    "MigrationRunner",
    "SchemaCompatibilityError",
]

import re
from dataclasses import dataclass
from datetime import UTC, datetime

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
_MIGRATION_REGISTRY = (_SchemaMigration(1, "current-schema-baseline"),)
MINIMUM_SCHEMA_VERSION = _MIGRATION_REGISTRY[0].version
LATEST_SCHEMA_VERSION = _MIGRATION_REGISTRY[-1].version

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
            case _:
                raise AssertionError(
                    f"No migration validation for version {migration.version}."
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
