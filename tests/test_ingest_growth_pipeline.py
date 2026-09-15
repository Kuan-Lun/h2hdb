"""A real source-seal/publication bridge across the 128-row analysis boundary."""

from __future__ import annotations

import logging
import re
from contextlib import closing
from hashlib import sha256
from pathlib import Path

import pytest
from vnext_pipeline import (
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    LoggerConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb.vnext_identity import effective_content_digest


def test_two_publications_seal_real_sources_and_validate_across_bounded_pages(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "catalog.db")
        ),
        logger=LoggerConfig.model_validate({"level": "debug"}),
    )
    initialize_database(config)
    pages = {
        gid: [f"gallery-{gid}-page-{number}-v1".encode() for number in range(65)]
        for gid in (1, 2)
    }
    source = MemorySource([gallery(gid, pages=pages[gid]) for gid in (1, 2)])
    library = MemoryLibrary(source)
    previous_revision = 0
    previous_content: str | None = None
    with (
        VNextIngestFacade(config) as facade,
        closing(VNextCatalogFacade(config)) as catalog,
        caplog.at_level(logging.DEBUG, logger="h2hdb.ingest_performance"),
    ):
        for ordinal in (1, 2):
            if ordinal == 2:
                pages[1][0] = b"gallery-1-page-0-v2"
            title = f"Gallery 1 revision {ordinal}"
            source.put(gallery(1, title=title, pages=pages[1]))
            assert [item.metadata().page_count for item in source.galleries] == [65, 65]
            caplog.clear()
            receipts = run_ingest_turn(
                facade,
                source=source,
                library=library,
                policy=ingest_policy(artifacts_required=False),
            )
            assert receipts.source.sealed and not receipts.source.replayed
            assert receipts.source.discovered_galleries == 2
            assert receipts.source.staged_galleries == 2
            assert not receipts.completion.replayed
            current = catalog.get_catalog_revision()
            assert current.revision > previous_revision
            previous_revision = current.revision
            assert current.publication_count == 2
            assert current.artifact_count == 0
            publications = catalog.discover_publications(
                revision=current, limit=128
            ).publications
            latest = next(item for item in publications if item.gid == 1)
            expected_content = effective_content_digest(
                tuple(sha256(page).digest() for page in pages[1])
            ).hex()
            assert latest.title == title
            assert latest.content_sha256 == expected_content
            assert latest.content_sha256 != previous_content
            previous_content = latest.content_sha256

            evidence = [
                dict(re.findall(r"(\w+)=([^\s]+)", record.getMessage()))
                for record in caplog.records
                if record.getMessage().startswith("ingest_db_performance ")
                and "operation=validate_file_hash_decision " in record.getMessage()
            ]
            terminals = [
                row
                for row in evidence
                if row["event"] in {"stage_terminal", "stage_transition"}
            ]
            assert len(terminals) == 1
            completed_batches = [
                int(row["processed_rows"])
                for row in evidence
                if row["event"] == "completed" and int(row["processed_rows"]) > 0
            ]
            assert completed_batches
            assert all(count <= 128 for count in completed_batches)
            if ordinal == 1:
                # The first full validation contains 130 distinct PAGE hashes;
                # correctness must survive at least two hard-capped windows.
                assert sum(completed_batches) == 130
                assert len(completed_batches) >= 2
            # These are measurements, not a requirement to preserve today's
            # high query counts or full-set work on an incremental revision.
            assert int(terminals[0]["sql_calls"]) > 0
            assert float(terminals[0]["sql_seconds"]) >= 0
            drain_maintenance(facade)
    assert library.render_calls == 0
