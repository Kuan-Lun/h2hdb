#!/usr/bin/env python3
"""Exercise the greenfield epoch and public consumers in one SQLite slice."""

from __future__ import annotations

import asyncio
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


def main() -> None:
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
