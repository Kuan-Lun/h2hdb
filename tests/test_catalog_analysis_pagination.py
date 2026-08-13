from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from h2hdb import (
    H2HDB,
    CatalogAnalysisPhase,
    CatalogBuild,
    CatalogGalleryFileHashRow,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryHeader,
    CatalogSourceManifest,
    CatalogSourceManifestRow,
    CoreConfig,
    GalleryIngestTurn,
    GallerySourceFile,
    GalleryTag,
)
from h2hdb.mariadb_connector import MariaDBConnector


def _digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _claim(database: H2HDB) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(lease_seconds=120, periodic_scan=False)
    assert turn is not None
    return turn


def _header(
    name: str,
    gid: int,
    tags: Sequence[GalleryTag] = (),
) -> CatalogSourceGalleryHeader:
    timestamp = datetime(2026, 8, 12, 1, 2, 3, tzinfo=UTC)
    return CatalogSourceGalleryHeader(
        gallery_name=name,
        gid=gid,
        title=f"Title {name}",
        comment="pagination fixture",
        upload_account="fixture",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=tuple(tags),
    )


def _stage_build(
    database: H2HDB,
    turn: GalleryIngestTurn,
    files_by_gallery: Mapping[str, Sequence[GallerySourceFile]],
    tags_by_gallery: Mapping[str, Sequence[GalleryTag]] | None = None,
) -> CatalogBuild:
    build = database.begin_catalog_build(
        scope_key=f"analysis-pagination:{_digest(repr(tuple(files_by_gallery)))}",
        ingest_turn=turn,
    )
    names = tuple(files_by_gallery)
    database.discover_catalog_galleries(
        build,
        names,
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.stage_catalog_gallery_headers(
        build,
        tuple(
            _header(
                name,
                index,
                () if tags_by_gallery is None else tags_by_gallery.get(name, ()),
            )
            for index, name in enumerate(names, start=1)
        ),
        batch_id="headers",
        ingest_turn=turn,
    )
    for gallery_index, (name, source_files) in enumerate(files_by_gallery.items()):
        for chunk_index, start in enumerate(range(0, len(source_files), 500)):
            database.stage_catalog_file_chunk(
                build,
                name,
                source_files[start : start + 500],
                batch_id=f"files:{gallery_index}:{chunk_index}",
                ingest_turn=turn,
            )
    database.complete_catalog_galleries(
        build,
        tuple(
            CatalogSourceGalleryCompletion(
                gallery_name=name,
                expected_file_count=len(source_files),
                scan_observation_sha256=_digest(f"scan:{name}"),
                scan_observation_version=2,
                metadata_sha256=_digest(f"metadata:{name}"),
            )
            for name, source_files in files_by_gallery.items()
        ),
        batch_id="completions",
        ingest_turn=turn,
    )
    return database.complete_catalog_source_staging(build, ingest_turn=turn)


def _complete_file_spam(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
) -> None:
    while True:
        page = database.get_catalog_file_spam_page(
            build,
            minimum_occurrences=3,
            limit=37,
            ingest_turn=turn,
        )
        database.apply_catalog_file_spam_page(
            build,
            page,
            (),
            ingest_turn=turn,
        )
        if page.terminal:
            database.complete_catalog_analysis_phase(
                build,
                CatalogAnalysisPhase.file_spam,
                ingest_turn=turn,
            )
            return


def _advance_to_gallery_file_hashes(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
    gallery_names: Sequence[str],
) -> None:
    database.stage_catalog_source_manifests(
        build,
        tuple(
            CatalogSourceManifest(name, _digest(f"manifest:{name}"))
            for name in gallery_names
        ),
        batch_id="manifests",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    )
    _complete_file_spam(database, build, turn)


def _collect_pages[Row](
    fetch_page: Callable[[Any | None, int], Any],
    *,
    limit: int,
) -> tuple[tuple[Row, ...], tuple[tuple[Row, ...], ...]]:
    after: Any | None = None
    rows: list[Row] = []
    pages: list[tuple[Row, ...]] = []
    for _iteration in range(10_000):
        page = fetch_page(after, limit)
        items = tuple(page.items)
        assert len(items) <= limit
        if not items:
            return tuple(rows), tuple(pages)
        pages.append(items)
        rows.extend(items)
        after = items[-1].cursor
    raise AssertionError("Keyset pagination did not reach an empty terminal page")


def _pagination_files() -> dict[str, tuple[GallerySourceFile, ...]]:
    shared = _digest("shared-tail")
    return {
        "Alpha": (
            GallerySourceFile("one.jpg", 1, _digest("alpha:one")),
            GallerySourceFile("two.jpg", 2, _digest("alpha:two")),
        ),
        "alpha": (GallerySourceFile("single.jpg", 3, _digest("lower")),),
        "日本語 Gallery ": (
            GallerySourceFile("A.jpg", 4, shared),
            GallerySourceFile("a.jpg", 5, shared),
            GallerySourceFile("STRASSE.jpg", 6, shared),
            GallerySourceFile("Straße.jpg", 7, shared),
            GallerySourceFile("É.jpg", 8, _digest("accent:upper")),
            GallerySourceFile("é.jpg", 9, _digest("accent:lower")),
            GallerySourceFile("末尾.jpg ", 10, _digest("trailing-space")),
            GallerySourceFile("z.jpg", 11, _digest("z")),
        ),
        "empty": (),
        "Ωmega": (
            GallerySourceFile("first.bin", 12, _digest("omega:first")),
            GallerySourceFile("last.bin", 13, _digest("omega:last")),
        ),
    }


def _expected_manifest_rows(
    files_by_gallery: Mapping[str, Sequence[GallerySourceFile]],
) -> tuple[CatalogSourceManifestRow, ...]:
    rows: list[CatalogSourceManifestRow] = []
    for gallery_name, source_files in files_by_gallery.items():
        gallery_key = _digest(gallery_name)
        if not source_files:
            rows.append(
                CatalogSourceManifestRow(
                    gallery_name=gallery_name,
                    gallery_key=gallery_key,
                    file_sort_key="",
                    file_name=None,
                    file_key="",
                    size_bytes=0,
                    file_sha256="",
                    empty_gallery_metadata_sha256=_digest(f"metadata:{gallery_name}"),
                )
            )
            continue
        rows.extend(
            CatalogSourceManifestRow(
                gallery_name=gallery_name,
                gallery_key=gallery_key,
                file_sort_key=source_file.name.casefold(),
                file_name=source_file.name,
                file_key=_digest(source_file.name),
                size_bytes=source_file.size_bytes,
                file_sha256=source_file.sha256,
            )
            for source_file in source_files
        )
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                row.gallery_key,
                row.file_sort_key,
                row.file_name or "",
                row.file_key,
            ),
        )
    )


def _expected_hash_rows(
    files_by_gallery: Mapping[str, Sequence[GallerySourceFile]],
) -> tuple[CatalogGalleryFileHashRow, ...]:
    rows: list[CatalogGalleryFileHashRow] = []
    for gallery_name, source_files in files_by_gallery.items():
        gallery_key = _digest(gallery_name)
        if not source_files:
            rows.append(
                CatalogGalleryFileHashRow(
                    gallery_name=gallery_name,
                    gallery_key=gallery_key,
                    file_key="",
                    file_sha256="",
                    metadata_file=False,
                    excluded_as_spam=False,
                )
            )
            continue
        rows.extend(
            CatalogGalleryFileHashRow(
                gallery_name=gallery_name,
                gallery_key=gallery_key,
                file_key=_digest(source_file.name),
                file_sha256=source_file.sha256,
                metadata_file=source_file.name == "galleryinfo.txt",
                excluded_as_spam=False,
            )
            for source_file in source_files
        )
    return tuple(
        sorted(
            rows,
            key=lambda row: (row.gallery_key, row.file_sha256, row.file_key),
        )
    )


def _assert_exact_keyset_result[Row](
    actual: Sequence[Row],
    expected: Sequence[Row],
) -> None:
    assert tuple(actual) == tuple(expected)
    assert len(actual) == len(expected)
    assert len(set(actual)) == len(actual)


def test_analysis_source_pages_are_exact_across_backends(
    db_config: CoreConfig,
) -> None:
    database = H2HDB(db_config)
    database.migrate()
    turn = _claim(database)
    files_by_gallery = _pagination_files()
    build = _stage_build(database, turn, files_by_gallery)
    expected_manifests = _expected_manifest_rows(files_by_gallery)

    for limit in (1, 2, 3):
        manifest_rows: tuple[CatalogSourceManifestRow, ...]
        manifest_pages: tuple[tuple[CatalogSourceManifestRow, ...], ...]
        manifest_rows, manifest_pages = _collect_pages(
            lambda after, page_limit: database.list_catalog_source_manifest_rows(
                build.build_id,
                after=after,
                limit=page_limit,
            ),
            limit=limit,
        )
        _assert_exact_keyset_result(manifest_rows, expected_manifests)
        if limit == 2:
            long_page_numbers = {
                page_number
                for page_number, page in enumerate(manifest_pages)
                if any(row.gallery_name == "日本語 Gallery " for row in page)
            }
            assert len(long_page_numbers) >= 4

    _advance_to_gallery_file_hashes(
        database,
        build,
        turn,
        tuple(files_by_gallery),
    )
    expected_hashes = _expected_hash_rows(files_by_gallery)
    for limit in (1, 2, 3):
        hash_rows: tuple[CatalogGalleryFileHashRow, ...]
        hash_pages: tuple[tuple[CatalogGalleryFileHashRow, ...], ...]
        hash_rows, hash_pages = _collect_pages(
            lambda after, page_limit: database.list_catalog_gallery_file_hashes(
                build.build_id,
                after=after,
                limit=page_limit,
            ),
            limit=limit,
        )
        _assert_exact_keyset_result(hash_rows, expected_hashes)
        if limit == 2:
            long_page_numbers = {
                page_number
                for page_number, page in enumerate(hash_pages)
                if any(row.gallery_name == "日本語 Gallery " for row in page)
            }
            assert len(long_page_numbers) >= 4

    empty_manifest = next(row for row in expected_manifests if row.file_name is None)
    empty_hash = next(row for row in expected_hashes if row.empty_gallery_sentinel)
    assert empty_manifest.gallery_name == empty_hash.gallery_name == "empty"


def test_file_spam_checkpoint_and_artist_namespace_are_exact_across_backends(
    db_config: CoreConfig,
) -> None:
    database = H2HDB(db_config)
    database.migrate()
    if db_config.database.sql_type == "mariadb":
        # Exercise the explicit BINARY predicate rather than inheriting the
        # test database's utf8mb4_bin default.  Production may use a
        # case-insensitive table/database default.
        config = db_config.database
        with MariaDBConnector(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        ) as connector:
            connector.execute(
                "ALTER TABLE catalog_source_tags MODIFY tag_name TEXT "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL"
            )
    turn = _claim(database)
    shared = _digest("shared-spam-candidate")
    files_by_gallery = {
        name: (GallerySourceFile("001.jpg", 1, shared),)
        for name in ("exact-artist", "mixed-case-artist", "no-artist")
    }
    build = _stage_build(
        database,
        turn,
        files_by_gallery,
        tags_by_gallery={
            "exact-artist": (GalleryTag("artist", "exact"),),
            # MariaDB text equality is commonly case-insensitive.  FILE_SPAM
            # must nevertheless follow the ingest policy's exact namespace.
            "mixed-case-artist": (
                GalleryTag("Artist", "wrong-a"),
                GalleryTag("ARTIST", "wrong-b"),
            ),
        },
    )
    database.stage_catalog_source_manifests(
        build,
        tuple(
            CatalogSourceManifest(name, _digest(f"manifest:{name}"))
            for name in files_by_gallery
        ),
        batch_id="manifests",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    )

    page = database.get_catalog_file_spam_page(
        build,
        minimum_occurrences=3,
        limit=1,
        ingest_turn=turn,
    )
    assert len(page.items) == 1
    assert page.items[0].file_sha256 == shared
    assert page.items[0].occurrence_count == 3
    assert page.items[0].distinct_artist_count == 1
    assert page.items[0].maximum_gallery_artist_count == 1
    applied = database.apply_catalog_file_spam_page(
        build,
        page,
        (),
        ingest_turn=turn,
    )
    assert applied.applied and not applied.complete

    terminal = database.get_catalog_file_spam_page(
        build,
        minimum_occurrences=3,
        limit=1,
        ingest_turn=turn,
    )
    assert terminal.terminal
    terminal_result = database.apply_catalog_file_spam_page(
        build,
        terminal,
        (),
        ingest_turn=turn,
    )
    assert terminal_result.applied and terminal_result.complete
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.file_spam,
        ingest_turn=turn,
    )
    assert database.is_catalog_analysis_phase_complete(
        build.build_id,
        CatalogAnalysisPhase.file_spam,
    )


def _walk_json(value: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    if isinstance(value, dict):
        nodes.append(value)
        for child in value.values():
            nodes.extend(_walk_json(child))
    elif isinstance(value, list):
        for child in value:
            nodes.extend(_walk_json(child))
    return nodes


def _assert_bounded_indexed_derived_page(
    plan: Mapping[str, Any],
    *,
    index_name: str,
    limit: int,
    covering: bool = False,
) -> None:
    nodes = _walk_json(plan)
    source_nodes = [node for node in nodes if node.get("table_name") == "source_file"]
    assert source_nodes
    assert any(node.get("key") == index_name for node in source_nodes), json.dumps(
        plan,
        indent=2,
        sort_keys=True,
    )
    if covering:
        assert any(
            node.get("key") == index_name and node.get("using_index") is True
            for node in source_nodes
        ), json.dumps(plan, indent=2, sort_keys=True)
    derived_nodes = [
        node for node in nodes if str(node.get("table_name", "")).startswith("<derived")
    ]
    assert derived_nodes
    estimates = [int(node["rows"]) for node in derived_nodes if "rows" in node]
    assert estimates
    assert min(estimates) <= limit * 2


def test_sqlite_gallery_hash_page_uses_covering_index(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    files_by_gallery = _pagination_files()
    build = _stage_build(database, turn, files_by_gallery)
    _advance_to_gallery_file_hashes(database, build, turn, tuple(files_by_gallery))
    galleryinfo_file_key = _digest("galleryinfo.txt")

    with sqlite3.connect(sqlite_config.database.database) as connection:
        empty_gallery_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT gallery_key
            FROM catalog_source_galleries
            WHERE build_id = ?
              AND source_complete = 1
              AND staged_file_count = 0
              AND gallery_key > ?
            ORDER BY gallery_key
            LIMIT 7
            """,
            (build.build_id, ""),
        ).fetchall()
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT
                gallery.gallery_name,
                source_file.gallery_key,
                source_file.file_key,
                source_file.sha256,
                CASE WHEN source_file.file_key = ? THEN 1 ELSE 0 END,
                CASE WHEN excluded.sha256 IS NULL THEN 0 ELSE 1 END
            FROM (
                SELECT build_id, gallery_key, file_key, sha256
                FROM catalog_source_files AS source_file
                WHERE source_file.build_id = ?
                ORDER BY source_file.gallery_key, source_file.sha256,
                    source_file.file_key
                LIMIT 7
            ) AS source_file
            JOIN catalog_source_galleries AS gallery
              ON gallery.build_id = source_file.build_id
             AND gallery.gallery_key = source_file.gallery_key
            LEFT JOIN catalog_build_excluded_file_hashes AS excluded
              ON excluded.build_id = source_file.build_id
             AND excluded.sha256 = source_file.sha256
            WHERE gallery.source_complete = 1
            ORDER BY source_file.gallery_key, source_file.sha256,
                source_file.file_key
            """,
            (galleryinfo_file_key, build.build_id),
        ).fetchall()

    details = tuple(str(row[3]) for row in plan)
    empty_gallery_details = tuple(str(row[3]) for row in empty_gallery_plan)
    assert any(
        "USING COVERING INDEX catalog_source_galleries_empty_order" in detail
        for detail in empty_gallery_details
    ), empty_gallery_details
    assert any(
        "USING COVERING INDEX catalog_source_files_gallery_hash_order" in detail
        for detail in details
    ), details


def test_mariadb_analysis_inner_pages_use_bounded_ordered_indexes(
    mariadb_config: CoreConfig,
) -> None:
    database = H2HDB(mariadb_config)
    database.migrate()
    turn = _claim(database)
    files_by_gallery = {
        f"gallery-{gallery_index:02}": tuple(
            GallerySourceFile(
                f"{file_index:06}.jpg",
                file_index,
                _digest(f"payload:{gallery_index}:{file_index:06}"),
            )
            for file_index in range(250)
        )
        for gallery_index in range(8)
    }
    build = _stage_build(database, turn, files_by_gallery)
    _advance_to_gallery_file_hashes(
        database,
        build,
        turn,
        tuple(files_by_gallery),
    )
    config = mariadb_config.database
    limit = 7
    with MariaDBConnector(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
    ) as connector:
        manifest_plan_row = connector.fetch_one(
            f"""
            EXPLAIN FORMAT=JSON
            SELECT source_file.gallery_key
            FROM (
                SELECT build_id, gallery_key, file_sort_key, file_name,
                    file_key, size_bytes, sha256
                FROM catalog_source_files AS source_file
                WHERE source_file.build_id = %s
                ORDER BY source_file.gallery_key, source_file.file_sort_key,
                    source_file.file_name, source_file.file_key
                LIMIT {limit}
            ) AS source_file
            JOIN catalog_source_galleries AS gallery
              ON gallery.build_id = source_file.build_id
             AND gallery.gallery_key = source_file.gallery_key
            WHERE gallery.source_complete = 1
            ORDER BY source_file.gallery_key, source_file.file_sort_key,
                source_file.file_name, source_file.file_key
            """,
            (build.build_id,),
        )
        hash_plan_row = connector.fetch_one(
            f"""
            EXPLAIN FORMAT=JSON
            SELECT source_file.gallery_key,
                CASE WHEN source_file.file_key = %s THEN 1 ELSE 0 END
            FROM (
                SELECT build_id, gallery_key, file_key, sha256
                FROM catalog_source_files AS source_file
                WHERE source_file.build_id = %s
                ORDER BY source_file.gallery_key, source_file.sha256,
                    source_file.file_key
                LIMIT {limit}
            ) AS source_file
            JOIN catalog_source_galleries AS gallery
              ON gallery.build_id = source_file.build_id
             AND gallery.gallery_key = source_file.gallery_key
            LEFT JOIN catalog_build_excluded_file_hashes AS excluded
              ON excluded.build_id = source_file.build_id
             AND excluded.sha256 = source_file.sha256
            WHERE gallery.source_complete = 1
            ORDER BY source_file.gallery_key, source_file.sha256,
                source_file.file_key
            """,
            (_digest("galleryinfo.txt"), build.build_id),
        )
        empty_gallery_plan_row = connector.fetch_one(
            f"""
            EXPLAIN FORMAT=JSON
            SELECT gallery_key
            FROM catalog_source_galleries AS gallery
            WHERE gallery.build_id = %s
              AND gallery.source_complete = 1
              AND gallery.staged_file_count = 0
              AND gallery.gallery_key > %s
            ORDER BY gallery.gallery_key
            LIMIT {limit}
            """,
            (build.build_id, ""),
        )

    assert manifest_plan_row is not None
    assert hash_plan_row is not None
    assert empty_gallery_plan_row is not None
    _assert_bounded_indexed_derived_page(
        json.loads(str(manifest_plan_row[0])),
        index_name="catalog_source_files_order",
        limit=limit,
    )
    _assert_bounded_indexed_derived_page(
        json.loads(str(hash_plan_row[0])),
        index_name="catalog_source_files_gallery_hash_order",
        limit=limit,
        covering=True,
    )
    empty_gallery_nodes = _walk_json(json.loads(str(empty_gallery_plan_row[0])))
    assert any(
        node.get("table_name") == "gallery"
        and node.get("key") == "catalog_source_galleries_empty_order"
        for node in empty_gallery_nodes
    ), json.dumps(json.loads(str(empty_gallery_plan_row[0])), indent=2, sort_keys=True)
