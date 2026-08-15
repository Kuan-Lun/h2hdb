import sqlite3
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from h2hdb import (
    H2HDB,
    CatalogArtifact,
    CatalogPublication,
    CatalogPublicationSelection,
    CatalogSnapshot,
    CoreConfig,
    DatabaseAccessMode,
    DatabaseConfig,
    GallerySourceFile,
    GallerySourceRecord,
    IngestTurnLostError,
    SchemaCompatibilityError,
)

EXPECTED_CANONICAL_TABLES = {
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

EXPECTED_PROJECTION_TABLES = {
    "catalog_artifacts",
    "catalog_contributors",
    "catalog_publications",
    "catalog_revision",
    "catalog_revision_history",
    "catalog_subjects",
}

EXPECTED_BUILD_TABLES = {
    "catalog_build_analysis_scan_checkpoints",
    "catalog_build_analysis_scan_receipts",
    "catalog_build_analysis_phases",
    "catalog_build_batches",
    "catalog_build_content_digests",
    "catalog_build_content_owners",
    "catalog_build_control",
    "catalog_build_discoveries",
    "catalog_build_excluded_file_hashes",
    "catalog_build_gid_winners",
    "catalog_builds",
    "catalog_build_prepared_artifacts",
    "catalog_build_projection_batches",
    "catalog_build_projection_items",
    "catalog_build_projections",
    "catalog_file_hash_cache",
    "catalog_source_files",
    "catalog_source_galleries",
    "catalog_source_revision",
    "catalog_source_revision_history",
    "catalog_source_tags",
    "catalog_projection_publication_receipts",
    "catalog_revision_allocator",
}

CATALOG_BUILD_DROP_ORDER = (
    "catalog_build_prepared_artifacts",
    "catalog_build_projection_items",
    "catalog_build_projection_batches",
    "catalog_build_projections",
    "catalog_projection_publication_receipts",
    "catalog_revision_allocator",
    "catalog_build_analysis_scan_receipts",
    "catalog_build_analysis_scan_checkpoints",
    "catalog_source_revision",
    "catalog_source_revision_history",
    "catalog_build_analysis_phases",
    "catalog_build_excluded_file_hashes",
    "catalog_build_content_owners",
    "catalog_build_gid_winners",
    "catalog_build_content_digests",
    "catalog_source_files",
    "catalog_source_tags",
    "catalog_source_galleries",
    "catalog_build_batches",
    "catalog_build_discoveries",
    "catalog_build_control",
    "catalog_file_hash_cache",
    "catalog_builds",
)

V4_DROP_ORDER = (
    "catalog_build_prepared_artifacts",
    "catalog_build_projection_items",
    "catalog_build_projection_batches",
    "catalog_projection_publication_receipts",
    "catalog_build_projections",
    "catalog_revision_allocator",
)

V3_DROP_ORDER = (
    "catalog_build_content_owners",
    "catalog_build_gid_winners",
    "catalog_build_content_digests",
    "catalog_build_excluded_file_hashes",
    "catalog_build_analysis_phases",
)

V7_DROP_ORDER = (
    "catalog_build_analysis_scan_receipts",
    "catalog_build_analysis_scan_checkpoints",
)

EXPECTED_SCHEMA_VIEWS = {
    "duplicate_hash_in_gallery",
    "files_hashs",
    "galleries_infos",
    "gallery_duplicate_warnings_names",
    "pending_download_gids",
    "todelete_gallery_candidates",
    "todelete_rm_commands",
}


def _read_only_config(config: CoreConfig) -> CoreConfig:
    database = config.database.model_copy(
        update={"access_mode": DatabaseAccessMode.read_only}
    )
    return config.model_copy(update={"database": database})


def _sqlite_tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            str(name)
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
            if not str(name).startswith("sqlite_")
        }


def _source_record(publication: CatalogPublication) -> GallerySourceRecord:
    timestamp = publication.modified_at
    digest = sha256(publication.publication_id.encode()).hexdigest()
    return GallerySourceRecord(
        gallery_name=f"canonical-gallery-{publication.gid}",
        gid=publication.gid,
        title=publication.source_title,
        comment=publication.summary,
        upload_account="",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=(),
        files=(GallerySourceFile("source.jpg", 0, digest),),
        source_manifest_sha256=sha256(
            f"manifest:{publication.publication_id}".encode()
        ).hexdigest(),
    )


def _sqlite_views(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            str(name)
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'view'"
            )
        }


def test_sqlite_current_schema_is_complete_and_idempotent(
    sqlite_config: CoreConfig,
) -> None:
    database_path = Path(sqlite_config.database.database)
    database = H2HDB(sqlite_config)

    assert database.migrate() == 7
    first = database.check_compatibility()
    assert first.database_version == 7
    assert first.minimum_supported <= first.database_version
    assert first.database_version <= first.maximum_supported
    assert database.check_readiness() == first
    tables = _sqlite_tables(database_path)
    assert EXPECTED_CANONICAL_TABLES <= tables
    assert EXPECTED_PROJECTION_TABLES <= tables
    assert EXPECTED_BUILD_TABLES <= tables
    assert "h2hdb_schema_migrations" in tables
    assert {"pending_cbz_rebuilds", "pending_gallery_removals"}.isdisjoint(tables)
    assert EXPECTED_SCHEMA_VIEWS <= _sqlite_views(database_path)

    assert database.migrate() == first.database_version
    assert database.check_compatibility() == first
    with sqlite3.connect(database_path) as connection:
        empty_gallery_index = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'index' "
            "AND name = 'catalog_source_galleries_empty_order'"
        ).fetchone()
        assert empty_gallery_index is not None
        normalized_empty_gallery_index = "".join(
            str(empty_gallery_index[0]).casefold().split()
        )
        assert (
            "(build_id,source_complete,staged_file_count,gallery_key)"
            in normalized_empty_gallery_index
        )
        assert connection.execute(
            "SELECT version, name FROM h2hdb_schema_migrations ORDER BY version"
        ).fetchall() == [
            (1, "current-schema-baseline"),
            (2, "durable-catalog-source-builds"),
            (3, "durable-catalog-build-analysis"),
            (4, "durable-catalog-build-projections"),
            (5, "catalog-operational-authority"),
            (6, "active-source-deletion-command-view"),
            (7, "durable-file-spam-scan"),
        ]
        assert connection.execute(
            "SELECT current_revision, publication_count FROM catalog_revision"
        ).fetchone() == (0, 0)
        assert connection.execute(
            "SELECT revision, publication_count FROM catalog_revision_history"
        ).fetchall() == [(0, 0)]
        columns = {
            str(row[1]): row
            for row in connection.execute("PRAGMA table_info(catalog_publications)")
        }
        assert columns["source_gallery_name"][3] == 1
        assert columns["content_sha256"][3] == 0


def test_read_only_compatibility_check_does_not_create_schema_objects(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "unmigrated.sqlite3"
    with sqlite3.connect(database_path):
        pass
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(database_path),
            access_mode=DatabaseAccessMode.read_only,
        )
    )

    database = H2HDB(config)
    with pytest.raises(SchemaCompatibilityError, match="database=0"):
        database.check_readiness()
    with pytest.raises(SchemaCompatibilityError, match="database=0"):
        database.check_compatibility()

    assert _sqlite_tables(database_path) == set()


def test_migrate_rejects_nonempty_unversioned_database(tmp_path: Path) -> None:
    database_path = tmp_path / "unversioned.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE operator_data (value TEXT NOT NULL)")
        connection.execute("INSERT INTO operator_data VALUES ('keep-me')")
    config = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
    )

    with pytest.raises(
        SchemaCompatibilityError,
        match="unversioned, non-empty database",
    ):
        H2HDB(config).migrate()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT value FROM operator_data").fetchall() == [
            ("keep-me",)
        ]
    assert "h2hdb_schema_migrations" not in _sqlite_tables(database_path)


def test_compatibility_rejects_obsolete_migration_identity(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("""
            UPDATE h2hdb_schema_migrations
            SET name = 'additive-canonical-catalog-schema'
            WHERE version = 1
            """)

    with pytest.raises(SchemaCompatibilityError, match="mismatched migration names"):
        database.check_compatibility()


@pytest.mark.parametrize("database_version", [8, 999])
def test_read_only_compatibility_rejects_unsupported_schema_versions(
    sqlite_config: CoreConfig,
    database_version: int,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("DELETE FROM h2hdb_schema_migrations")
        connection.execute(
            """
            INSERT INTO h2hdb_schema_migrations (version, name, applied_at)
            VALUES (?, ?, '2026-08-07T00:00:00+00:00')
            """,
            (database_version, "unsupported-schema"),
        )

    with pytest.raises(
        SchemaCompatibilityError,
        match=rf"database={database_version}",
    ):
        H2HDB(_read_only_config(sqlite_config)).check_compatibility()


def test_migrated_sqlite_opens_with_read_only_credentials(
    sqlite_config: CoreConfig,
) -> None:
    writable = H2HDB(sqlite_config)
    writable.migrate()

    reader = H2HDB(_read_only_config(sqlite_config))

    assert reader.check_compatibility().database_version == 7
    assert reader.get_catalog_revision().revision == 0


def test_compatibility_rejects_missing_critical_catalog_index(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP INDEX catalog_artifacts_name")

    assert database.check_readiness().database_version == 7
    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.check_compatibility()


def test_compatibility_rejects_missing_catalog_build_index(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP INDEX catalog_source_galleries_gid")

    assert database.check_readiness().database_version == 7
    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.check_compatibility()


def test_compatibility_rejects_missing_catalog_analysis_index(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP INDEX catalog_build_content_digest_order")

    assert database.check_readiness().database_version == 7
    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.check_compatibility()


def test_compatibility_rejects_catalog_source_files_without_foreign_key(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP TABLE catalog_source_files")
        connection.execute("""
            CREATE TABLE catalog_source_files (
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
                PRIMARY KEY (build_id, gallery_key, file_key)
            )
            """)
        connection.execute("""
            CREATE UNIQUE INDEX catalog_source_files_name
            ON catalog_source_files (build_id, gallery_key, file_name)
            """)
        connection.execute("""
            CREATE INDEX catalog_source_files_order
            ON catalog_source_files (
                build_id, gallery_key, file_sort_key, file_name, file_key
            )
            """)
        connection.execute("""
            CREATE INDEX catalog_source_files_hash_order
            ON catalog_source_files (build_id, sha256, gallery_key, file_key)
            """)

    with pytest.raises(SchemaCompatibilityError, match="foreign_keys="):
        database.check_compatibility()


def test_catalog_build_migration_recovers_from_partial_ddl(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 2")
        for table_name in CATALOG_BUILD_DROP_ORDER[:-1]:
            connection.execute(f"DROP TABLE {table_name}")

    assert database.migrate() == 7
    assert EXPECTED_BUILD_TABLES <= _sqlite_tables(database_path)
    assert database.check_compatibility().database_version == 7


def test_catalog_build_forward_migrates_a_v1_ledger(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 2")
        for table_name in CATALOG_BUILD_DROP_ORDER:
            connection.execute(f"DROP TABLE {table_name}")
        assert connection.execute(
            "SELECT version, name FROM h2hdb_schema_migrations"
        ).fetchall() == [(1, "current-schema-baseline")]

    assert database.migrate() == 7
    assert EXPECTED_BUILD_TABLES <= _sqlite_tables(database_path)
    assert database.check_compatibility().database_version == 7


@pytest.mark.parametrize("ledger_version", [2, 3])
def test_catalog_projection_forward_migrates_v2_and_v3_ledgers(
    sqlite_config: CoreConfig,
    ledger_version: int,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "DELETE FROM h2hdb_schema_migrations WHERE version > ?",
            (ledger_version,),
        )
        for table_name in V4_DROP_ORDER:
            connection.execute(f"DROP TABLE {table_name}")
        if ledger_version == 2:
            for table_name in V3_DROP_ORDER:
                connection.execute(f"DROP TABLE {table_name}")

    assert database.migrate() == 7
    assert EXPECTED_BUILD_TABLES <= _sqlite_tables(database_path)
    assert database.check_compatibility().database_version == 7


def test_v4_migration_backfills_the_current_legacy_projection_digest(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    timestamp = datetime(2026, 8, 9, tzinfo=UTC)
    gallery = GallerySourceRecord(
        gallery_name="legacy-backfill",
        gid=808,
        title="Legacy",
        comment="",
        upload_account="",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=(),
        files=(),
        source_manifest_sha256=sha256(b"legacy-manifest").hexdigest(),
    )
    turn = database.claim_gallery_ingest(lease_seconds=30, periodic_scan=False)
    assert turn is not None
    revision = database.publish_snapshot(
        CatalogSnapshot(
            galleries=(gallery,),
            selections=(CatalogPublicationSelection("legacy-backfill"),),
        ),
        ingest_turn=turn,
    ).revision.revision
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 4")
        for table_name in V4_DROP_ORDER:
            connection.execute(f"DROP TABLE {table_name}")
        connection.execute(
            "ALTER TABLE catalog_revision_history DROP COLUMN projection_sha256"
        )

    assert database.migrate() == 7
    with sqlite3.connect(database_path) as connection:
        digest = connection.execute(
            "SELECT projection_sha256 FROM catalog_revision_history WHERE revision = ?",
            (revision,),
        ).fetchone()
        assert digest is not None
        assert len(str(digest[0])) == 64
        assert connection.execute(
            "SELECT next_revision FROM catalog_revision_allocator"
        ).fetchone() == (revision + 1,)


def test_v5_forward_migration_adopts_legacy_deletion_markers(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("INSERT INTO todelete_gids (gid) VALUES (909)")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 5")
        connection.execute("DROP VIEW todelete_rm_commands")
        connection.execute("""
            CREATE VIEW todelete_rm_commands AS
            SELECT 'rm -rf -- ' || galleries_names.full_name AS cmd
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
            """)
        connection.execute("DROP INDEX todelete_gids_request_token")
        connection.execute("DROP INDEX catalog_source_galleries_gid_download")
        for table_name in (
            "catalog_build_operational_state",
            "catalog_build_removed_gid_requests",
            "catalog_build_deletion_consumptions",
            "catalog_operational_activations",
            "catalog_removed_gid_request_acks",
            "catalog_gallery_redownload_times",
        ):
            connection.execute(f"DROP TABLE {table_name}")
        connection.execute(
            "ALTER TABLE catalog_source_revision "
            "DROP COLUMN deletion_request_generation"
        )
        connection.execute(
            "ALTER TABLE catalog_source_galleries DROP COLUMN upload_time_utc"
        )
        connection.execute(
            "ALTER TABLE catalog_source_galleries DROP COLUMN download_time_utc"
        )
        connection.execute(
            "ALTER TABLE catalog_build_content_digests "
            "DROP COLUMN duplicate_hash_deletion_candidate"
        )
        connection.execute("ALTER TABLE todelete_gids DROP COLUMN request_token")

    assert database.migrate() == 7
    assert database.check_compatibility().database_version == 7
    with sqlite3.connect(database_path) as connection:
        token = connection.execute(
            "SELECT request_token FROM todelete_gids WHERE gid = 909"
        ).fetchone()
    assert token is not None and len(str(token[0])) == 32


def test_v6_forward_migration_replaces_the_legacy_deletion_command_view(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 6")
        connection.execute("DROP VIEW todelete_rm_commands")
        connection.execute("""
            CREATE VIEW todelete_rm_commands AS
            SELECT 'rm -rf -- ' || galleries_names.full_name AS cmd
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
            """)
        legacy_definition = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'view' AND name = 'todelete_rm_commands'"
        ).fetchone()
    assert legacy_definition is not None
    assert "catalog_operational_activations" not in str(legacy_definition[0])

    assert database.migrate() == 7
    assert database.check_compatibility().database_version == 7
    with sqlite3.connect(database_path) as connection:
        active_definition = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'view' AND name = 'todelete_rm_commands'"
        ).fetchone()
    assert active_definition is not None
    assert "catalog_operational_activations" in str(active_definition[0])
    assert "catalog_build_discoveries" in str(active_definition[0])


@pytest.mark.parametrize("legacy_phase", ["ANALYZING", "ARTIFACTS", "SEALED"])
def test_v7_rejects_unproven_legacy_build_then_migrates_after_abandonment(
    sqlite_config: CoreConfig,
    legacy_phase: str,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = database.claim_gallery_ingest(lease_seconds=120, periodic_scan=False)
    assert turn is not None
    build = database.begin_catalog_build(
        scope_key="legacy-file-spam-proof",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version = 7")
        for table_name in V7_DROP_ORDER:
            connection.execute(f"DROP TABLE {table_name}")
        connection.execute(
            "UPDATE catalog_builds SET phase = ? WHERE build_id = ?",
            (legacy_phase, build.build_id),
        )

    with pytest.raises(
        SchemaCompatibilityError,
        match="Cannot install durable FILE_SPAM.*abandon and rebuild",
    ):
        database.migrate()
    with sqlite3.connect(database_path) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM h2hdb_schema_migrations WHERE version = 7"
            ).fetchone()
            is None
        )
        assert all(
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            is None
            for table_name in V7_DROP_ORDER
        )
        # A pre-v7 operator may explicitly abandon the unverifiable build.
        connection.execute(
            "UPDATE catalog_builds SET phase = 'ABANDONED' WHERE build_id = ?",
            (build.build_id,),
        )
        connection.execute(
            "UPDATE catalog_build_control SET working_build_id = NULL "
            "WHERE working_build_id = ?",
            (build.build_id,),
        )

    assert database.migrate() == 7
    assert database.check_compatibility().database_version == 7
    assert EXPECTED_BUILD_TABLES <= _sqlite_tables(database_path)


def test_compatibility_rejects_legacy_only_deletion_command_view_at_v6(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP VIEW todelete_rm_commands")
        connection.execute("""
            CREATE VIEW todelete_rm_commands AS
            SELECT 'rm -rf -- ' || galleries_names.full_name AS cmd
            FROM todelete_galleries
                JOIN galleries_names USING (db_gallery_id)
            """)

    assert database.check_readiness().database_version == 7
    with pytest.raises(
        SchemaCompatibilityError,
        match="active-source deletion command view",
    ):
        database.check_compatibility()


def test_catalog_build_forward_migration_refuses_a_corrupt_v1_schema(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    database_path = Path(sqlite_config.database.database)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM h2hdb_schema_migrations WHERE version >= 2")
        for table_name in CATALOG_BUILD_DROP_ORDER:
            connection.execute(f"DROP TABLE {table_name}")
        connection.execute("DROP INDEX catalog_artifacts_name")

    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.migrate()
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT version FROM h2hdb_schema_migrations ORDER BY version"
        ).fetchall() == [(1,)]


def test_compatibility_rejects_canonical_table_without_foreign_key(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP TABLE galleries_gids")
        connection.execute("""
            CREATE TABLE galleries_gids (
                db_gallery_id INTEGER NOT NULL PRIMARY KEY,
                gid INTEGER NOT NULL
            )
            """)
        connection.execute(
            "CREATE INDEX idx_galleries_gids_gid ON galleries_gids (gid)"
        )

    with pytest.raises(SchemaCompatibilityError, match="foreign_keys="):
        database.check_compatibility()


def test_compatibility_rejects_unique_only_canonical_gid_lookup(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP INDEX idx_galleries_gids_gid")
        connection.execute(
            "CREATE UNIQUE INDEX invalid_unique_gid ON galleries_gids (gid)"
        )

    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.check_compatibility()


def test_compatibility_rejects_coordination_table_without_checks(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP TABLE gallery_ingest_state")
        connection.execute("""
            CREATE TABLE gallery_ingest_state (
                state_id INTEGER NOT NULL PRIMARY KEY,
                phase TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                completed_generation INTEGER NOT NULL DEFAULT 0,
                owner_token TEXT NULL,
                lease_expires_at INTEGER NULL,
                handoff_generation INTEGER NULL,
                handoff_owner_token TEXT NULL,
                last_transition_at INTEGER NOT NULL
            )
            """)
        connection.execute("""
            INSERT INTO gallery_ingest_state (
                state_id,
                phase,
                generation,
                completed_generation,
                last_transition_at
            ) VALUES (1, 'READY', 0, 0, 0)
            """)

    with pytest.raises(SchemaCompatibilityError, match="checks="):
        database.check_compatibility()


def test_compatibility_rejects_download_queue_without_token_uniqueness(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP TABLE todownload_gids")
        connection.execute("""
            CREATE TABLE todownload_gids (
                gid INTEGER NOT NULL PRIMARY KEY,
                url TEXT NOT NULL,
                request_token TEXT NOT NULL
            )
            """)

    with pytest.raises(SchemaCompatibilityError, match="indexes="):
        database.check_compatibility()


def test_compatibility_rejects_rewritten_critical_view(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    with sqlite3.connect(Path(sqlite_config.database.database)) as connection:
        connection.execute("DROP VIEW pending_download_gids")
        connection.execute("""
            CREATE VIEW pending_download_gids AS
            SELECT gid FROM todownload_gids
            """)

    with pytest.raises(SchemaCompatibilityError, match="views="):
        database.check_compatibility()


def test_public_backend_contract_smoke(
    db_config: CoreConfig,
    tmp_path: Path,
) -> None:
    database = H2HDB(db_config)
    timestamp = datetime(2026, 8, 6, tzinfo=UTC)
    artifact = CatalogArtifact(
        artifact_id="urn:h2hdb:test:artifact:1",
        name="backend-contract.cbz",
        location=tmp_path / "backend-contract.cbz",
        media_type="application/vnd.comicbook+zip",
        size_bytes=123,
        sha256="ab" * 32,
        modified_at=timestamp,
    )
    publication = CatalogPublication(
        publication_id="urn:h2h:gallery:101",
        gid=101,
        title="Backend Contract",
        source_title="Backend Contract",
        sort_title="backend contract",
        summary="Shared SQLite and MariaDB projection smoke test",
        language="und",
        published_at=timestamp,
        modified_at=timestamp,
        artifacts=(artifact,),
        source_gallery_name="canonical-gallery-101",
    )

    version = database.migrate()
    ingest_turn = database.claim_gallery_ingest(
        lease_seconds=30,
        periodic_scan=False,
    )
    assert ingest_turn is not None
    publish_result = database.publish_snapshot(
        CatalogSnapshot(
            galleries=(_source_record(publication),),
            selections=(
                CatalogPublicationSelection(
                    source_gallery_name="canonical-gallery-101",
                    artifacts=(artifact,),
                ),
            ),
        ),
        ingest_turn=ingest_turn,
    )
    revision = publish_result.revision
    assert database.complete_gallery_ingest(ingest_turn)

    assert version == 7
    assert database.check_compatibility().database_version == version
    assert revision.revision == 1
    assert database.list_publications(limit=10).publications == (publication,)
    assert database.get_publication(publication.publication_id) == publication
    assert database.get_artifact(artifact.artifact_id) == artifact
    # One snapshot transaction publishes canonical source facts and the OPDS
    # projection together.
    assert database.get_candidate_states([101])[101].cataloged

    turn = database.claim_gallery_ingest(lease_seconds=30, periodic_scan=True)
    assert turn is not None
    current_revision = database.publish_snapshot(
        CatalogSnapshot(galleries=(), selections=()),
        ingest_turn=turn,
    ).revision
    assert database.complete_gallery_ingest(turn)
    assert current_revision.revision == revision.revision + 1
    assert database.get_catalog_revision(revision.revision) == revision
    assert database.list_publications(limit=10).publications == ()
    assert database.list_publications(
        limit=10,
        revision=revision,
    ).publications == (publication,)
    assert database.get_download_request(101) is not None

    with pytest.raises(IngestTurnLostError):
        database.publish_snapshot(
            CatalogSnapshot(
                galleries=(_source_record(publication),),
                selections=(
                    CatalogPublicationSelection(
                        source_gallery_name="canonical-gallery-101",
                        artifacts=(artifact,),
                    ),
                ),
            ),
            ingest_turn=turn,
        )
    assert database.get_catalog_revision() == current_revision

    reader = H2HDB(_read_only_config(db_config))
    assert reader.list_publications(limit=10).publications == ()
    assert reader.list_publications(
        limit=10,
        revision=reader.get_catalog_revision(revision.revision),
    ).publications == (publication,)
    turn = database.claim_gallery_ingest(lease_seconds=30, periodic_scan=True)
    assert turn is not None
    try:
        with pytest.raises(PermissionError, match="read-only"):
            reader.publish_snapshot(
                CatalogSnapshot(galleries=(), selections=()),
                ingest_turn=turn,
            )
    finally:
        assert database.complete_gallery_ingest(turn)
