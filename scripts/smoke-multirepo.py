#!/usr/bin/env python3
"""Exercise all editable packages through one temporary SQLite vertical slice."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import h2h_galleryinfo_parser
import h2hdb_downloader
import hbrowser
from fastapi import FastAPI
from h2hdb_ingest import (
    CBZReconciler,
    DeduplicationPolicy,
    FilesystemScanner,
    IngestService,
)
from h2hdb_komga.metadata import publication_to_komga_metadata
from h2hdb_opds import OPDSConfig, create_app
from httpx import ASGITransport, AsyncClient
from PIL import Image

from h2hdb import H2HDB, CoreConfig, DatabaseConfig, open_database


def _write_gallery(root: Path) -> None:
    folder = root / "Integration Gallery [101]"
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Integration Gallery",
                "Upload Time: 2026-08-07 10:00",
                "Uploaded By: integration-user",
                "Downloaded: 2026-08-07 11:00",
                "Tags: artist:Example Artist, language:english",
                "Uploader's Comments:",
                "Cross-repository SQLite smoke test",
                "Downloaded from E-Hentai Galleries by the Hentai@Home "
                "Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    Image.new("RGB", (32, 24), (20, 40, 60)).save(folder / "001.png")


async def _exercise_opds_http(application: FastAPI, expected_content: bytes) -> None:
    transport = ASGITransport(app=application)
    async with AsyncClient(
        transport=transport,
        base_url="http://integration.test",
    ) as client:
        navigation = await client.get("/opds/v2?revision=1")
        assert navigation.status_code == 200
        feed = await client.get("/opds/v2/publications?revision=1")
        assert feed.status_code == 200
        assert feed.json()["publications"][0]["metadata"]["title"] == (
            "Integration Gallery"
        )
        acquisition = await client.get(
            "/opds/v2/acquisitions/urn:h2h:artifact:cbz:101?revision=1"
        )
        assert acquisition.status_code == 200
        assert acquisition.content == expected_content
        assert (
            "Integration%20Gallery%20%5B101%5D.cbz"
            in acquisition.headers["content-disposition"]
        )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="h2hdb-multirepo-") as temporary:
        # macOS exposes /var through the /private/var symlink. Canonicalize the
        # shared root once so OPDS containment checks and catalog artifact
        # locations compare the same path representation.
        root = Path(temporary).resolve()
        gallery_root = root / "galleries"
        _write_gallery(gallery_root)
        config = CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(root / "catalog.sqlite3"),
            )
        )
        database = H2HDB(config)
        assert database.migrate() == 1

        service = IngestService(
            scanner=FilesystemScanner(gallery_root, hash_workers=1),
            deduplication=DeduplicationPolicy(),
            cbz=CBZReconciler(
                artifact_store_path=root / "artifacts",
                cbz_path=root / "komga",
                max_image_short_side=16,
            ),
            catalog_reader=database,
            catalog_publisher=database,
            database_admin=database,
        )
        turn = database.claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=True,
        )
        assert turn is not None
        outcome = service.synchronize_once(turn)
        assert database.complete_gallery_ingest(turn)
        assert outcome.revision == 1
        assert outcome.new == 1

        opds_config = OPDSConfig(
            core=config,
            artifact_root=root / "artifacts",
            public_base_url="http://integration.test",
        )
        assert opds_config.core.database.access_mode.value == "read-only"
        reader = open_database(opds_config.core)
        publication = reader.get_publication("urn:h2h:gallery:101")
        assert publication is not None
        assert publication.artifacts[0].name == "Integration Gallery [101].cbz"
        assert (
            publication.artifacts[0].location.parent == (root / "artifacts").resolve()
        )

        Image.new("RGB", (32, 24), (60, 40, 20)).save(
            gallery_root / "Integration Gallery [101]" / "001.png"
        )
        next_turn = database.claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=True,
        )
        assert next_turn is not None
        next_outcome = service.synchronize_once(next_turn)
        assert database.complete_gallery_ingest(next_turn)
        assert next_outcome.revision == 2
        assert next_outcome.changed == 1

        current_publication = reader.get_publication("urn:h2h:gallery:101")
        assert current_publication is not None
        assert current_publication.artifacts[0].location != (
            publication.artifacts[0].location
        )
        assert publication.artifacts[0].location.is_file()
        assert current_publication.artifacts[0].location.is_file()
        current_cbz = root / "komga" / "Integration Gallery [101].cbz"
        assert list((root / "komga").rglob("*.cbz")) == [current_cbz]
        assert current_cbz.read_bytes() == (
            current_publication.artifacts[0].location.read_bytes()
        )

        metadata = publication_to_komga_metadata(current_publication)
        assert metadata["title"] == current_publication.title
        assert any(author["role"] == "gid" for author in metadata["authors"])

        application = create_app(opds_config, catalog=reader)
        openapi_paths = application.openapi()["paths"]
        assert "/opds/v2" in openapi_paths
        assert set(openapi_paths["/opds/v2/acquisitions/{artifact_id}"]) == {
            "get",
            "head",
        }
        asyncio.run(
            _exercise_opds_http(
                application,
                publication.artifacts[0].location.read_bytes(),
            )
        )

        # Imports above ensure the remaining independently installed packages
        # resolve in the exact same environment as the exercised vertical slice.
        assert h2h_galleryinfo_parser is not None
        assert h2hdb_downloader is not None
        assert hbrowser is not None

    print("multi-repo SQLite public-API vertical slice: ok")


if __name__ == "__main__":
    main()
