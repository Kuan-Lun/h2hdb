from __future__ import annotations

__all__ = ["CatalogAnalysisRepository"]

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256

from .catalog_build_repository import (
    CatalogBuildBatchConflictError,
    CatalogBuildRepository,
    CatalogBuildStateError,
)
from .domain import (
    CatalogAnalysisPhase,
    CatalogAnalysisPhaseCheckpoint,
    CatalogAnalysisScanCompletion,
    CatalogBuild,
    CatalogBuildBatchResult,
    CatalogBuildPhase,
    CatalogContentCandidateCursor,
    CatalogContentCandidatePage,
    CatalogContentCandidateRow,
    CatalogContentDigest,
    CatalogContentOwner,
    CatalogDeduplicationCandidate,
    CatalogFileHashAggregate,
    CatalogFileHashAggregateCursor,
    CatalogFileHashAggregatePage,
    CatalogFinalAnalysisCursor,
    CatalogFinalAnalysisPage,
    CatalogGalleryFileHashCursor,
    CatalogGalleryFileHashPage,
    CatalogGalleryFileHashRow,
    CatalogGidCandidateCursor,
    CatalogGidCandidatePage,
    CatalogGidCandidateRow,
    CatalogGidWinner,
    CatalogSourceGalleryAnalysis,
    CatalogSourceManifest,
    CatalogSourceManifestCursor,
    CatalogSourceManifestPage,
    CatalogSourceManifestRow,
    GalleryTag,
)
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector
from .table_gallery_ingest_coordination import GalleryIngestTurn

MAX_ANALYSIS_PAGE_SIZE = 1_000
MAX_ANALYSIS_WRITE_BATCH_SIZE = 1_000
LOOKUP_CHUNK_SIZE = 400
_PHASES = tuple(CatalogAnalysisPhase)


def _stable_key(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _payload_sha256(value: object) -> str:
    return sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _scan_completion_token(
    build_id: str,
    phase: CatalogAnalysisPhase,
    after_value: str,
) -> str:
    return _payload_sha256(("analysis-scan-v1", build_id, phase.value, after_value))


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


class CatalogAnalysisRepository(BaseRepository):
    """Bounded, durable database facts for ingest-owned deduplication policy."""

    def __init__(
        self,
        context: RepositoryContext,
        builds: CatalogBuildRepository,
    ) -> None:
        super().__init__(context)
        self._builds = builds

    @staticmethod
    def _validate_page_limit(limit: int) -> None:
        if not 1 <= limit <= MAX_ANALYSIS_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_ANALYSIS_PAGE_SIZE}")

    @staticmethod
    def _validate_write_batch(values: Sequence[object]) -> None:
        if len(values) > MAX_ANALYSIS_WRITE_BATCH_SIZE:
            raise ValueError(
                "analysis write batch must contain at most "
                f"{MAX_ANALYSIS_WRITE_BATCH_SIZE} items"
            )

    def _require_readable_build(
        self,
        connector: SQLConnector,
        build_id: str,
    ) -> CatalogBuild:
        build = self._builds._require_build(
            connector,
            build_id,
            for_update=False,
        )
        if build.phase not in {
            CatalogBuildPhase.analyzing,
            CatalogBuildPhase.artifacts,
            CatalogBuildPhase.sealed,
            CatalogBuildPhase.published,
        }:
            raise CatalogBuildStateError(
                f"Catalog build {build_id} is not available for analysis"
            )
        return build

    def _require_mutable_build(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        build = self._builds._require_owned_build(connector, build_id, turn)
        self._builds._require_phase(build, CatalogBuildPhase.analyzing)
        return build

    @staticmethod
    def _phase_index(phase: CatalogAnalysisPhase) -> int:
        return _PHASES.index(phase)

    def _completed_phases(
        self,
        connector: SQLConnector,
        build_id: str,
    ) -> set[CatalogAnalysisPhase]:
        return {
            CatalogAnalysisPhase(str(row[0]))
            for row in connector.fetch_all(
                """
                SELECT phase
                FROM catalog_build_analysis_phases
                WHERE build_id = %s
                """,
                (build_id,),
            )
        }

    def _require_preceding_phases(
        self,
        connector: SQLConnector,
        build_id: str,
        phase: CatalogAnalysisPhase,
    ) -> set[CatalogAnalysisPhase]:
        completed = self._completed_phases(connector, build_id)
        required = set(_PHASES[: self._phase_index(phase)])
        missing = required - completed
        if missing:
            names = ", ".join(item.value for item in _PHASES if item in missing)
            raise CatalogBuildStateError(
                f"Catalog analysis phases must complete in order; missing {names}"
            )
        return completed

    def _require_open_phase(
        self,
        connector: SQLConnector,
        build_id: str,
        phase: CatalogAnalysisPhase,
    ) -> None:
        completed = self._require_preceding_phases(connector, build_id, phase)
        if phase in completed:
            raise CatalogBuildStateError(
                f"Catalog analysis phase {phase.value} is already complete"
            )

    def is_phase_complete(
        self,
        build_id: str,
        phase: CatalogAnalysisPhase,
    ) -> bool:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                row = connector.fetch_one(
                    """
                    SELECT 1
                    FROM catalog_build_analysis_phases
                    WHERE build_id = %s AND phase = %s
                    """,
                    (build_id, phase.value),
                )
        return bool(row)

    def list_source_manifest_rows(
        self,
        build_id: str,
        *,
        after: CatalogSourceManifestCursor | None,
        limit: int,
    ) -> CatalogSourceManifestPage:
        self._validate_page_limit(limit)
        regular_keyset = ""
        regular_parameters: list[object] = [build_id]
        sentinel_keyset = ""
        sentinel_parameters: list[object] = [build_id]
        if after is not None:
            regular_keyset = """
                AND (
                    gallery.gallery_key > %s
                    OR (
                        gallery.gallery_key = %s
                        AND (
                            source_file.file_sort_key > %s
                            OR (
                                source_file.file_sort_key = %s
                                AND (
                                    source_file.file_name > %s
                                    OR (
                                        source_file.file_name = %s
                                        AND source_file.file_key > %s
                                    )
                                )
                            )
                        )
                    )
                )
            """
            regular_parameters.extend(
                (
                    after.gallery_key,
                    after.gallery_key,
                    after.file_sort_key,
                    after.file_sort_key,
                    after.file_name,
                    after.file_name,
                    after.file_key,
                )
            )
            # The empty-gallery sentinel is the first possible tuple for a
            # gallery, so after any valid cursor only later gallery keys apply.
            sentinel_keyset = "AND gallery.gallery_key > %s"
            sentinel_parameters.append(after.gallery_key)
        regular_parameters.append(limit)
        sentinel_parameters.append(limit)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.source_manifests,
                )
                regular_rows = connector.fetch_all(
                    f"""
                    SELECT
                        gallery.gallery_name,
                        gallery.gallery_key,
                        source_file.file_sort_key,
                        source_file.file_name,
                        source_file.file_key,
                        source_file.size_bytes,
                        source_file.sha256,
                        NULL
                    FROM catalog_source_galleries AS gallery
                    JOIN catalog_source_files AS source_file
                        ON source_file.build_id = gallery.build_id
                        AND source_file.gallery_key = gallery.gallery_key
                    WHERE gallery.build_id = %s
                        AND gallery.source_complete = 1
                        {regular_keyset}
                    ORDER BY
                        gallery.gallery_key,
                        source_file.file_sort_key,
                        source_file.file_name,
                        source_file.file_key
                    LIMIT %s
                    """,
                    tuple(regular_parameters),
                )
                sentinel_rows = connector.fetch_all(
                    f"""
                    SELECT gallery.gallery_name, gallery.gallery_key,
                        '', NULL, '', 0, '',
                        gallery.metadata_sha256
                    FROM catalog_source_galleries AS gallery
                    WHERE gallery.build_id = %s
                        AND gallery.source_complete = 1
                        AND NOT EXISTS (
                            SELECT 1
                            FROM catalog_source_files AS source_file
                            WHERE source_file.build_id = gallery.build_id
                                AND source_file.gallery_key = gallery.gallery_key
                        )
                        {sentinel_keyset}
                    ORDER BY gallery.gallery_key
                    LIMIT %s
                    """,
                    tuple(sentinel_parameters),
                )
        rows = sorted(
            (*regular_rows, *sentinel_rows),
            key=lambda row: (str(row[1]), str(row[2]), str(row[3] or ""), str(row[4])),
        )[:limit]
        return CatalogSourceManifestPage(
            tuple(
                CatalogSourceManifestRow(
                    gallery_name=str(row[0]),
                    gallery_key=str(row[1]),
                    file_sort_key=str(row[2]),
                    file_name=None if row[3] is None else str(row[3]),
                    file_key=str(row[4]),
                    size_bytes=int(row[5]),
                    file_sha256=str(row[6]),
                    empty_gallery_metadata_sha256=(
                        None if row[7] is None else str(row[7])
                    ),
                )
                for row in rows
            ),
            limit,
        )

    def list_file_hash_aggregates(
        self,
        build_id: str,
        *,
        after: CatalogFileHashAggregateCursor | None,
        limit: int,
    ) -> CatalogFileHashAggregatePage:
        self._validate_page_limit(limit)
        after_sha = "" if after is None else after.file_sha256
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.file_spam,
                )
                exact_artist = (
                    "HEX(CONVERT(tag.tag_value USING utf8mb4))"
                    if self._context.sql_type == "mariadb"
                    else "hex(CAST(tag.tag_value AS BLOB))"
                )
                rows = connector.fetch_all(
                    f"""
                    WITH hash_page AS (
                        SELECT sha256, COUNT(*) AS occurrence_count
                        FROM catalog_source_files
                        WHERE build_id = %s
                            AND file_name <> 'galleryinfo.txt'
                            AND sha256 > %s
                        GROUP BY sha256
                        ORDER BY sha256
                        LIMIT %s
                    ),
                    hash_galleries AS (
                        SELECT DISTINCT source_file.sha256, source_file.gallery_key
                        FROM catalog_source_files AS source_file
                        JOIN hash_page
                            ON hash_page.sha256 = source_file.sha256
                        WHERE source_file.build_id = %s
                    ),
                    artist_counts AS (
                        SELECT
                            hash_gallery.sha256,
                            hash_gallery.gallery_key,
                            COUNT(DISTINCT {exact_artist}) AS artist_count
                        FROM hash_galleries AS hash_gallery
                        LEFT JOIN catalog_source_tags AS tag
                            ON tag.build_id = %s
                            AND tag.gallery_key = hash_gallery.gallery_key
                            AND tag.tag_name = 'artist'
                        GROUP BY hash_gallery.sha256, hash_gallery.gallery_key
                    ),
                    artist_maximums AS (
                        SELECT sha256, MAX(artist_count) AS maximum_artist_count
                        FROM artist_counts
                        GROUP BY sha256
                    ),
                    artist_unions AS (
                        SELECT
                            hash_gallery.sha256,
                            COUNT(DISTINCT {exact_artist}) AS distinct_artist_count
                        FROM hash_galleries AS hash_gallery
                        LEFT JOIN catalog_source_tags AS tag
                            ON tag.build_id = %s
                            AND tag.gallery_key = hash_gallery.gallery_key
                            AND tag.tag_name = 'artist'
                        GROUP BY hash_gallery.sha256
                    )
                    SELECT
                        hash_page.sha256,
                        hash_page.occurrence_count,
                        artist_unions.distinct_artist_count,
                        artist_maximums.maximum_artist_count
                    FROM hash_page
                    JOIN artist_unions
                        ON artist_unions.sha256 = hash_page.sha256
                    JOIN artist_maximums
                        ON artist_maximums.sha256 = hash_page.sha256
                    ORDER BY hash_page.sha256
                    """,
                    (
                        build_id,
                        after_sha,
                        limit,
                        build_id,
                        build_id,
                        build_id,
                    ),
                )
        items = tuple(
            CatalogFileHashAggregate(
                file_sha256=str(row[0]),
                occurrence_count=int(row[1]),
                distinct_artist_count=int(row[2]),
                maximum_gallery_artist_count=int(row[3]),
            )
            for row in rows
        )
        completion = (
            CatalogAnalysisScanCompletion(
                build_id=build_id,
                phase=CatalogAnalysisPhase.file_spam,
                after_value=after_sha,
                token_sha256=_scan_completion_token(
                    build_id,
                    CatalogAnalysisPhase.file_spam,
                    after_sha,
                ),
            )
            if not items
            else None
        )
        return CatalogFileHashAggregatePage(items, limit, completion)

    def list_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: CatalogGalleryFileHashCursor | None,
        limit: int,
    ) -> CatalogGalleryFileHashPage:
        self._validate_page_limit(limit)
        regular_keyset = ""
        regular_parameters: list[object] = [build_id]
        sentinel_keyset = ""
        sentinel_parameters: list[object] = [build_id]
        if after is not None:
            regular_keyset = """
                AND (
                    gallery.gallery_key > %s
                    OR (
                        gallery.gallery_key = %s
                        AND (
                            source_file.sha256 > %s
                            OR (
                                source_file.sha256 = %s
                                AND source_file.file_key > %s
                            )
                        )
                    )
                )
            """
            regular_parameters.extend(
                (
                    after.gallery_key,
                    after.gallery_key,
                    after.file_sha256,
                    after.file_sha256,
                    after.file_key,
                )
            )
            sentinel_keyset = "AND gallery.gallery_key > %s"
            sentinel_parameters.append(after.gallery_key)
        regular_parameters.append(limit)
        sentinel_parameters.append(limit)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.content_digests,
                )
                regular_rows = connector.fetch_all(
                    f"""
                    SELECT
                        gallery.gallery_name,
                        gallery.gallery_key,
                        source_file.file_key,
                        source_file.file_name,
                        source_file.sha256,
                        CASE WHEN excluded.sha256 IS NULL THEN 0 ELSE 1 END
                    FROM catalog_source_galleries AS gallery
                    JOIN catalog_source_files AS source_file
                        ON source_file.build_id = gallery.build_id
                        AND source_file.gallery_key = gallery.gallery_key
                    LEFT JOIN catalog_build_excluded_file_hashes AS excluded
                        ON excluded.build_id = source_file.build_id
                        AND excluded.sha256 = source_file.sha256
                    WHERE gallery.build_id = %s
                        AND gallery.source_complete = 1
                        {regular_keyset}
                    ORDER BY
                        gallery.gallery_key,
                        source_file.sha256,
                        source_file.file_key
                    LIMIT %s
                    """,
                    tuple(regular_parameters),
                )
                sentinel_rows = connector.fetch_all(
                    f"""
                    SELECT gallery.gallery_name, gallery.gallery_key,
                        '', NULL, '', 0
                    FROM catalog_source_galleries AS gallery
                    WHERE gallery.build_id = %s
                        AND gallery.source_complete = 1
                        AND NOT EXISTS (
                            SELECT 1
                            FROM catalog_source_files AS source_file
                            WHERE source_file.build_id = gallery.build_id
                                AND source_file.gallery_key = gallery.gallery_key
                        )
                        {sentinel_keyset}
                    ORDER BY gallery.gallery_key
                    LIMIT %s
                    """,
                    tuple(sentinel_parameters),
                )
        rows = sorted(
            (*regular_rows, *sentinel_rows),
            key=lambda row: (str(row[1]), str(row[4]), str(row[2])),
        )[:limit]
        return CatalogGalleryFileHashPage(
            tuple(
                CatalogGalleryFileHashRow(
                    gallery_name=str(row[0]),
                    gallery_key=str(row[1]),
                    file_key=str(row[2]),
                    file_name=None if row[3] is None else str(row[3]),
                    file_sha256=str(row[4]),
                    excluded_as_spam=bool(row[5]),
                )
                for row in rows
            ),
            limit,
        )

    def _tags_by_gallery_key(
        self,
        connector: SQLConnector,
        build_id: str,
        gallery_keys: Sequence[str],
    ) -> dict[str, tuple[GalleryTag, ...]]:
        tags: dict[str, list[GalleryTag]] = {}
        for start in range(0, len(gallery_keys), LOOKUP_CHUNK_SIZE):
            chunk = tuple(gallery_keys[start : start + LOOKUP_CHUNK_SIZE])
            if not chunk:
                continue
            placeholders = ", ".join("%s" for _ in chunk)
            for gallery_key, name, value in connector.fetch_all(
                f"""
                SELECT gallery_key, tag_name, tag_value
                FROM catalog_source_tags
                WHERE build_id = %s AND gallery_key IN ({placeholders})
                ORDER BY gallery_key, position
                """,
                (build_id, *chunk),
            ):
                tags.setdefault(str(gallery_key), []).append(
                    GalleryTag(str(name), str(value))
                )
        return {key: tuple(values) for key, values in tags.items()}

    @staticmethod
    def _active_source_build_id(connector: SQLConnector) -> str | None:
        row = connector.fetch_one("""
            SELECT active_build_id
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """)
        if not row:
            raise RuntimeError("catalog_source_revision singleton is missing")
        return None if row[0] is None else str(row[0])

    def list_content_candidates(
        self,
        build_id: str,
        *,
        after: CatalogContentCandidateCursor | None,
        limit: int,
    ) -> CatalogContentCandidatePage:
        self._validate_page_limit(limit)
        keyset = ""
        parameters: list[object] = [build_id]
        if after is not None:
            keyset = """
                AND (
                    digest.content_sha256 > %s
                    OR (
                        digest.content_sha256 = %s
                        AND digest.gallery_key > %s
                    )
                )
            """
            parameters.extend(
                (
                    after.content_sha256,
                    after.content_sha256,
                    after.gallery_key,
                )
            )
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.content_owners,
                )
                active_build_id = self._active_source_build_id(connector)
                if active_build_id is None:
                    incumbent_sql = """
                        (
                            SELECT publication.source_gallery_name
                            FROM catalog_revision AS current_revision
                            JOIN catalog_publications AS publication
                                ON publication.revision =
                                    current_revision.current_revision
                            WHERE current_revision.singleton_id = 1
                                AND publication.content_sha256 =
                                    digest.content_sha256
                            ORDER BY publication.publication_key
                            LIMIT 1
                        )
                    """
                else:
                    incumbent_sql = """
                        (
                            SELECT incumbent.gallery_name
                            FROM catalog_source_galleries AS incumbent
                            WHERE incumbent.build_id = %s
                                AND incumbent.selected = 1
                                AND incumbent.content_sha256 =
                                    digest.content_sha256
                            ORDER BY incumbent.gallery_key
                            LIMIT 1
                        )
                    """
                    parameters.insert(0, active_build_id)
                parameters.append(limit)
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        gallery.gallery_key,
                        gallery.gallery_name,
                        gallery.gid,
                        gallery.title,
                        gallery.download_time,
                        digest.content_sha256,
                        {incumbent_sql} AS incumbent_gallery_name
                    FROM catalog_build_content_digests AS digest
                    JOIN catalog_source_galleries AS gallery
                        ON gallery.build_id = digest.build_id
                        AND gallery.gallery_key = digest.gallery_key
                    WHERE digest.build_id = %s
                        AND digest.content_sha256 IS NOT NULL
                        {keyset}
                    ORDER BY digest.content_sha256, digest.gallery_key
                    LIMIT %s
                    """,
                    tuple(parameters),
                )
                tags = self._tags_by_gallery_key(
                    connector,
                    build_id,
                    tuple(str(row[0]) for row in rows),
                )
        return CatalogContentCandidatePage(
            tuple(
                CatalogContentCandidateRow(
                    CatalogDeduplicationCandidate(
                        gallery_name=str(row[1]),
                        gid=int(row[2]),
                        title=str(row[3]),
                        download_time=_parse_datetime(row[4]),
                        content_sha256=str(row[5]),
                        tags=tags.get(str(row[0]), ()),
                    ),
                    None if row[6] is None else str(row[6]),
                    str(row[0]),
                )
                for row in rows
            ),
            limit,
        )

    def list_gid_candidates(
        self,
        build_id: str,
        *,
        after: CatalogGidCandidateCursor | None,
        limit: int,
    ) -> CatalogGidCandidatePage:
        self._validate_page_limit(limit)
        keyset = ""
        parameters: list[object] = [build_id]
        if after is not None:
            keyset = """
                AND (
                    gallery.gid > %s
                    OR (
                        gallery.gid = %s
                        AND gallery.gallery_key > %s
                    )
                )
            """
            parameters.extend((after.gid, after.gid, after.gallery_key))
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.gid_winners,
                )
                active_build_id = self._active_source_build_id(connector)
                if active_build_id is None:
                    incumbent_sql = """
                        (
                            SELECT publication.source_gallery_name
                            FROM catalog_revision AS current_revision
                            JOIN catalog_publications AS publication
                                ON publication.revision =
                                    current_revision.current_revision
                            WHERE current_revision.singleton_id = 1
                                AND publication.gid = gallery.gid
                            ORDER BY publication.publication_key
                            LIMIT 1
                        )
                    """
                else:
                    incumbent_sql = """
                        (
                            SELECT incumbent.gallery_name
                            FROM catalog_source_galleries AS incumbent
                            WHERE incumbent.build_id = %s
                                AND incumbent.selected = 1
                                AND incumbent.gid = gallery.gid
                            ORDER BY incumbent.gallery_key
                            LIMIT 1
                        )
                    """
                    parameters.insert(0, active_build_id)
                parameters.append(limit)
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        gallery.gallery_key,
                        gallery.gallery_name,
                        gallery.gid,
                        gallery.title,
                        gallery.download_time,
                        digest.content_sha256,
                        {incumbent_sql} AS incumbent_gallery_name
                    FROM catalog_source_galleries AS gallery
                    JOIN catalog_build_content_digests AS digest
                        ON digest.build_id = gallery.build_id
                        AND digest.gallery_key = gallery.gallery_key
                    LEFT JOIN catalog_build_content_owners AS owner
                        ON owner.build_id = digest.build_id
                        AND owner.content_sha256 = digest.content_sha256
                    WHERE gallery.build_id = %s
                        AND (
                            digest.content_sha256 IS NULL
                            OR owner.owner_gallery_key = gallery.gallery_key
                        )
                        {keyset}
                    ORDER BY gallery.gid, gallery.gallery_key
                    LIMIT %s
                    """,
                    tuple(parameters),
                )
                tags = self._tags_by_gallery_key(
                    connector,
                    build_id,
                    tuple(str(row[0]) for row in rows),
                )
        return CatalogGidCandidatePage(
            tuple(
                CatalogGidCandidateRow(
                    CatalogDeduplicationCandidate(
                        gallery_name=str(row[1]),
                        gid=int(row[2]),
                        title=str(row[3]),
                        download_time=_parse_datetime(row[4]),
                        content_sha256=None if row[5] is None else str(row[5]),
                        tags=tags.get(str(row[0]), ()),
                    ),
                    None if row[6] is None else str(row[6]),
                    str(row[0]),
                )
                for row in rows
            ),
            limit,
        )

    def list_final_analyses(
        self,
        build_id: str,
        *,
        after: CatalogFinalAnalysisCursor | None,
        limit: int,
    ) -> CatalogFinalAnalysisPage:
        self._validate_page_limit(limit)
        parameters: list[object] = [build_id]
        keyset = ""
        if after is not None:
            keyset = "AND gallery.gallery_key > %s"
            parameters.append(after.gallery_key)
        parameters.append(limit)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_readable_build(connector, build_id)
                self._require_preceding_phases(
                    connector,
                    build_id,
                    CatalogAnalysisPhase.final_analyses,
                )
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        gallery.gallery_key,
                        gallery.gallery_name,
                        digest.content_sha256,
                        CASE
                            WHEN winner.winner_gallery_key = gallery.gallery_key
                                THEN 1
                            ELSE 0
                        END AS selected,
                        CASE
                            WHEN digest.content_sha256 IS NULL
                                OR owner.owner_gallery_key = gallery.gallery_key
                                THEN NULL
                            ELSE owner.owner_gallery_name
                        END AS duplicate_of_gallery_name
                    FROM catalog_source_galleries AS gallery
                    JOIN catalog_build_content_digests AS digest
                        ON digest.build_id = gallery.build_id
                        AND digest.gallery_key = gallery.gallery_key
                    LEFT JOIN catalog_build_content_owners AS owner
                        ON owner.build_id = digest.build_id
                        AND owner.content_sha256 = digest.content_sha256
                    LEFT JOIN catalog_build_gid_winners AS winner
                        ON winner.build_id = gallery.build_id
                        AND winner.gid = gallery.gid
                    WHERE gallery.build_id = %s
                        {keyset}
                    ORDER BY gallery.gallery_key
                    LIMIT %s
                    """,
                    tuple(parameters),
                )
        return CatalogFinalAnalysisPage(
            tuple(
                CatalogSourceGalleryAnalysis(
                    gallery_name=str(row[1]),
                    content_sha256=None if row[2] is None else str(row[2]),
                    selected=bool(row[3]),
                    duplicate_of_gallery_name=None if row[4] is None else str(row[4]),
                    gallery_key=str(row[0]),
                )
                for row in rows
            ),
            limit,
        )

    def _begin_batch(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
        phase: CatalogAnalysisPhase,
        batch_kind: str,
        batch_id: str,
        payload_sha256: str,
    ) -> CatalogBuildBatchResult | None:
        self._builds._validate_batch_id(batch_id)
        self._require_mutable_build(connector, build_id, turn)
        previous = self._builds._existing_batch(
            connector,
            build_id,
            batch_kind,
            batch_id,
        )
        if previous is not None:
            return self._builds._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )
        self._require_open_phase(connector, build_id, phase)
        return None

    def _finish_batch(
        self,
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
        batch_id: str,
        payload_sha256: str,
        item_count: int,
    ) -> CatalogBuildBatchResult:
        self._builds._record_batch(
            connector,
            build_id,
            batch_kind,
            batch_id,
            payload_sha256,
            item_count,
            0,
        )
        return CatalogBuildBatchResult(
            build_id=build_id,
            batch_id=batch_id,
            applied=True,
            item_count=item_count,
        )

    def stage_source_manifests(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[CatalogSourceManifest],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(
            tuple(item.gallery_name for item in items),
            "source manifest gallery",
        )
        payload = _payload_sha256(
            tuple(
                (
                    item.gallery_name,
                    item.source_manifest_sha256,
                    item.source_manifest_version,
                )
                for item in items
            )
        )
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.source_manifests,
            "AN_SOURCE_MANIFESTS",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for item in items:
            gallery_key = _stable_key(item.gallery_name)
            row = connector.fetch_one(
                """
                SELECT
                    gallery_name,
                    source_complete,
                    source_manifest_sha256,
                    source_manifest_version
                FROM catalog_source_galleries
                WHERE build_id = %s AND gallery_key = %s
                """,
                (build_id, gallery_key),
            )
            if not row or str(row[0]) != item.gallery_name or not bool(row[1]):
                raise CatalogBuildStateError(
                    "A canonical manifest references an incomplete source gallery"
                )
            persisted = (
                None if row[2] is None else str(row[2]),
                None if row[3] is None else int(row[3]),
            )
            requested = (
                item.source_manifest_sha256,
                item.source_manifest_version,
            )
            if persisted == requested:
                continue
            if persisted != (None, None):
                raise CatalogBuildBatchConflictError(
                    "A gallery canonical manifest was retried with different data"
                )
            connector.execute(
                """
                UPDATE catalog_source_galleries
                SET source_manifest_sha256 = %s,
                    source_manifest_version = %s
                WHERE build_id = %s AND gallery_key = %s
                """,
                (*requested, build_id, gallery_key),
            )
            applied += 1
        return self._finish_batch(
            connector,
            build_id,
            "AN_SOURCE_MANIFESTS",
            batch_id,
            payload,
            applied,
        )

    def stage_excluded_file_hashes(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[str],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(items, "excluded file SHA-256")
        for digest in items:
            self._validate_sha256(digest, "Excluded file SHA-256")
        payload = _payload_sha256(items)
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.file_spam,
            "AN_FILE_SPAM",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for digest in items:
            if connector.fetch_one(
                """
                SELECT 1
                FROM catalog_build_excluded_file_hashes
                WHERE build_id = %s AND sha256 = %s
                """,
                (build_id, digest),
            ):
                continue
            connector.execute(
                """
                INSERT INTO catalog_build_excluded_file_hashes (build_id, sha256)
                VALUES (%s, %s)
                """,
                (build_id, digest),
            )
            applied += 1
        return self._finish_batch(
            connector,
            build_id,
            "AN_FILE_SPAM",
            batch_id,
            payload,
            applied,
        )

    def stage_content_digests(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[CatalogContentDigest],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(
            tuple(item.gallery_name for item in items),
            "content digest gallery",
        )
        payload = _payload_sha256(
            tuple(
                (
                    item.gallery_name,
                    item.content_sha256,
                    item.duplicate_hash_deletion_candidate,
                )
                for item in items
            )
        )
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.content_digests,
            "AN_CONTENT_DIGESTS",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for item in items:
            gallery_key = self._require_source_gallery(
                connector,
                build_id,
                item.gallery_name,
            )[0]
            previous = connector.fetch_one(
                """
                SELECT
                    gallery_name,
                    content_sha256,
                    duplicate_hash_deletion_candidate
                FROM catalog_build_content_digests
                WHERE build_id = %s AND gallery_key = %s
                """,
                (build_id, gallery_key),
            )
            if previous:
                persisted = None if previous[1] is None else str(previous[1])
                if (
                    str(previous[0]) != item.gallery_name
                    or persisted != item.content_sha256
                    or bool(previous[2]) != item.duplicate_hash_deletion_candidate
                ):
                    raise CatalogBuildBatchConflictError(
                        "A gallery content digest was retried with different data"
                    )
                continue
            connector.execute(
                """
                INSERT INTO catalog_build_content_digests (
                    build_id,
                    gallery_key,
                    gallery_name,
                    content_sha256,
                    duplicate_hash_deletion_candidate
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    build_id,
                    gallery_key,
                    item.gallery_name,
                    item.content_sha256,
                    item.duplicate_hash_deletion_candidate,
                ),
            )
            applied += 1
        return self._finish_batch(
            connector,
            build_id,
            "AN_CONTENT_DIGESTS",
            batch_id,
            payload,
            applied,
        )

    def stage_content_owners(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[CatalogContentOwner],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(
            tuple(item.content_sha256 for item in items),
            "owned content SHA-256",
        )
        payload = _payload_sha256(
            tuple((item.content_sha256, item.owner_gallery_name) for item in items)
        )
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.content_owners,
            "AN_CONTENT_OWNERS",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for item in items:
            owner_key = _stable_key(item.owner_gallery_name)
            candidate = connector.fetch_one(
                """
                SELECT digest.gallery_name, digest.content_sha256
                FROM catalog_build_content_digests AS digest
                WHERE digest.build_id = %s AND digest.gallery_key = %s
                """,
                (build_id, owner_key),
            )
            if (
                not candidate
                or str(candidate[0]) != item.owner_gallery_name
                or candidate[1] is None
                or str(candidate[1]) != item.content_sha256
            ):
                raise CatalogBuildStateError(
                    "A content owner must be a candidate for the same digest"
                )
            previous = connector.fetch_one(
                """
                SELECT owner_gallery_key, owner_gallery_name
                FROM catalog_build_content_owners
                WHERE build_id = %s AND content_sha256 = %s
                """,
                (build_id, item.content_sha256),
            )
            requested = (owner_key, item.owner_gallery_name)
            if previous:
                if (str(previous[0]), str(previous[1])) != requested:
                    raise CatalogBuildBatchConflictError(
                        "A content owner was retried with a different winner"
                    )
                continue
            connector.execute(
                """
                INSERT INTO catalog_build_content_owners (
                    build_id,
                    content_sha256,
                    owner_gallery_key,
                    owner_gallery_name
                ) VALUES (%s, %s, %s, %s)
                """,
                (build_id, item.content_sha256, *requested),
            )
            applied += 1
        return self._finish_batch(
            connector,
            build_id,
            "AN_CONTENT_OWNERS",
            batch_id,
            payload,
            applied,
        )

    def stage_gid_winners(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[CatalogGidWinner],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(tuple(item.gid for item in items), "winner GID")
        payload = _payload_sha256(
            tuple((item.gid, item.winner_gallery_name) for item in items)
        )
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.gid_winners,
            "AN_GID_WINNERS",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for item in items:
            winner_key, gid = self._require_source_gallery(
                connector,
                build_id,
                item.winner_gallery_name,
            )
            if gid != item.gid:
                raise CatalogBuildStateError("A GID winner must have the selected GID")
            owner = connector.fetch_one(
                """
                SELECT digest.content_sha256, content_owner.owner_gallery_key
                FROM catalog_build_content_digests AS digest
                LEFT JOIN catalog_build_content_owners AS content_owner
                    ON content_owner.build_id = digest.build_id
                    AND content_owner.content_sha256 = digest.content_sha256
                WHERE digest.build_id = %s AND digest.gallery_key = %s
                """,
                (build_id, winner_key),
            )
            if not owner or (owner[0] is not None and str(owner[1]) != winner_key):
                raise CatalogBuildStateError("A GID winner must be a content owner")
            previous = connector.fetch_one(
                """
                SELECT winner_gallery_key, winner_gallery_name
                FROM catalog_build_gid_winners
                WHERE build_id = %s AND gid = %s
                """,
                (build_id, item.gid),
            )
            requested = (winner_key, item.winner_gallery_name)
            if previous:
                if (str(previous[0]), str(previous[1])) != requested:
                    raise CatalogBuildBatchConflictError(
                        "A GID winner was retried with a different winner"
                    )
                continue
            connector.execute(
                """
                INSERT INTO catalog_build_gid_winners (
                    build_id, gid, winner_gallery_key, winner_gallery_name
                ) VALUES (%s, %s, %s, %s)
                """,
                (build_id, item.gid, *requested),
            )
            applied += 1
        return self._finish_batch(
            connector,
            build_id,
            "AN_GID_WINNERS",
            batch_id,
            payload,
            applied,
        )

    def stage_final_analyses(
        self,
        connector: SQLConnector,
        build_id: str,
        values: Sequence[CatalogSourceGalleryAnalysis],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        items = tuple(values)
        self._validate_write_batch(items)
        self._validate_unique(
            tuple(item.gallery_name for item in items),
            "final analysis gallery",
        )
        payload = _payload_sha256(
            tuple(
                (
                    item.gallery_name,
                    item.content_sha256,
                    item.selected,
                    item.duplicate_of_gallery_name,
                    item.source_manifest_sha256,
                    item.source_manifest_version,
                )
                for item in items
            )
        )
        replay = self._begin_batch(
            connector,
            build_id,
            turn,
            CatalogAnalysisPhase.final_analyses,
            "AN_FINAL_ANALYSES",
            batch_id,
            payload,
        )
        if replay is not None:
            return replay
        applied = 0
        for item in items:
            gallery_key, gid = self._require_source_gallery(
                connector,
                build_id,
                item.gallery_name,
            )
            derived = connector.fetch_one(
                """
                SELECT
                    digest.content_sha256,
                    CASE
                        WHEN digest.content_sha256 IS NULL
                            OR owner.owner_gallery_key = digest.gallery_key
                            THEN NULL
                        ELSE owner.owner_gallery_name
                    END,
                    CASE
                        WHEN winner.winner_gallery_key = digest.gallery_key THEN 1
                        ELSE 0
                    END
                FROM catalog_build_content_digests AS digest
                LEFT JOIN catalog_build_content_owners AS owner
                    ON owner.build_id = digest.build_id
                    AND owner.content_sha256 = digest.content_sha256
                LEFT JOIN catalog_build_gid_winners AS winner
                    ON winner.build_id = digest.build_id
                    AND winner.gid = %s
                WHERE digest.build_id = %s AND digest.gallery_key = %s
                """,
                (gid, build_id, gallery_key),
            )
            requested = (
                item.content_sha256,
                item.duplicate_of_gallery_name,
                item.selected,
            )
            if (
                not derived
                or (
                    None if derived[0] is None else str(derived[0]),
                    None if derived[1] is None else str(derived[1]),
                    bool(derived[2]),
                )
                != requested
            ):
                raise CatalogBuildStateError(
                    "Final analysis does not match staged owner and winner decisions"
                )
            row = connector.fetch_one(
                """
                SELECT
                    analysis_complete,
                    content_sha256,
                    duplicate_of_gallery_name,
                    selected,
                    source_manifest_sha256,
                    source_manifest_version
                FROM catalog_source_galleries
                WHERE build_id = %s AND gallery_key = %s
                """,
                (build_id, gallery_key),
            )
            if not row or row[4] is None or row[5] is None:
                raise CatalogBuildStateError(
                    "Final analysis requires a canonical source manifest"
                )
            manifest = (str(row[4]), int(row[5]))
            requested_manifest = (
                item.source_manifest_sha256,
                item.source_manifest_version,
            )
            if requested_manifest != (None, None) and requested_manifest != manifest:
                raise CatalogBuildBatchConflictError(
                    "Final analysis disagrees with the canonical source manifest"
                )
            persisted = (
                None if row[1] is None else str(row[1]),
                None if row[2] is None else str(row[2]),
                bool(row[3]),
            )
            if bool(row[0]):
                if persisted != requested:
                    raise CatalogBuildBatchConflictError(
                        "A final gallery analysis was retried with different data"
                    )
                continue
            connector.execute(
                """
                UPDATE catalog_source_galleries
                SET content_sha256 = %s,
                    duplicate_of_gallery_name = %s,
                    duplicate_of_gallery_key = %s,
                    selected = %s,
                    analysis_complete = 1
                WHERE build_id = %s AND gallery_key = %s
                """,
                (
                    item.content_sha256,
                    item.duplicate_of_gallery_name,
                    (
                        None
                        if item.duplicate_of_gallery_name is None
                        else _stable_key(item.duplicate_of_gallery_name)
                    ),
                    item.selected,
                    build_id,
                    gallery_key,
                ),
            )
            applied += 1
        if applied:
            now = self._builds._database_datetime(connector)
            connector.execute(
                """
                UPDATE catalog_builds
                SET analyzed_gallery_count = analyzed_gallery_count + %s,
                    updated_at = %s
                WHERE build_id = %s
                """,
                (applied, now.isoformat(), build_id),
            )
        return self._finish_batch(
            connector,
            build_id,
            "AN_FINAL_ANALYSES",
            batch_id,
            payload,
            applied,
        )

    def complete_phase(
        self,
        connector: SQLConnector,
        build_id: str,
        phase: CatalogAnalysisPhase,
        *,
        turn: GalleryIngestTurn,
        scan_completion: CatalogAnalysisScanCompletion | None = None,
    ) -> CatalogAnalysisPhaseCheckpoint:
        self._require_mutable_build(connector, build_id, turn)
        completed = self._require_preceding_phases(connector, build_id, phase)
        if phase in completed:
            row = connector.fetch_one(
                """
                SELECT completed_at
                FROM catalog_build_analysis_phases
                WHERE build_id = %s AND phase = %s
                """,
                (build_id, phase.value),
            )
            if not row:
                raise RuntimeError("Catalog analysis checkpoint disappeared")
            return CatalogAnalysisPhaseCheckpoint(
                build_id,
                phase,
                _parse_datetime(row[0]),
                False,
            )
        self._validate_phase_complete(
            connector,
            build_id,
            phase,
            scan_completion=scan_completion,
        )
        completed_at = self._builds._database_datetime(connector)
        connector.execute(
            """
            INSERT INTO catalog_build_analysis_phases (
                build_id, phase, completed_at
            ) VALUES (%s, %s, %s)
            """,
            (build_id, phase.value, completed_at.isoformat()),
        )
        return CatalogAnalysisPhaseCheckpoint(
            build_id,
            phase,
            completed_at,
            True,
        )

    def _validate_phase_complete(
        self,
        connector: SQLConnector,
        build_id: str,
        phase: CatalogAnalysisPhase,
        *,
        scan_completion: CatalogAnalysisScanCompletion | None,
    ) -> None:
        query: str | None
        message: str
        match phase:
            case CatalogAnalysisPhase.source_manifests:
                query = """
                    SELECT gallery_name
                    FROM catalog_source_galleries
                    WHERE build_id = %s
                        AND source_complete = 1
                        AND (
                            source_manifest_sha256 IS NULL
                            OR source_manifest_version IS NULL
                        )
                    LIMIT 1
                """
                message = "A source gallery has no canonical manifest"
            case CatalogAnalysisPhase.file_spam:
                if (
                    scan_completion is None
                    or scan_completion.build_id != build_id
                    or scan_completion.phase is not phase
                    or scan_completion.token_sha256
                    != _scan_completion_token(
                        build_id,
                        phase,
                        scan_completion.after_value,
                    )
                ):
                    raise CatalogBuildStateError(
                        "FILE_SPAM requires an explicit terminal aggregate scan token"
                    )
                terminal = connector.fetch_one(
                    """
                    SELECT 1
                    FROM catalog_source_files
                    WHERE build_id = %s
                        AND file_name <> 'galleryinfo.txt'
                        AND sha256 > %s
                    LIMIT 1
                    """,
                    (build_id, scan_completion.after_value),
                )
                if terminal:
                    raise CatalogBuildStateError(
                        "FILE_SPAM aggregate scan token is not terminal"
                    )
                query = None
                message = ""
            case CatalogAnalysisPhase.content_digests:
                query = """
                    SELECT gallery.gallery_name
                    FROM catalog_source_galleries AS gallery
                    LEFT JOIN catalog_build_content_digests AS digest
                        ON digest.build_id = gallery.build_id
                        AND digest.gallery_key = gallery.gallery_key
                    WHERE gallery.build_id = %s
                        AND gallery.source_complete = 1
                        AND digest.gallery_key IS NULL
                    LIMIT 1
                """
                message = "A source gallery has no effective content digest row"
            case CatalogAnalysisPhase.content_owners:
                query = """
                    SELECT digest.gallery_name
                    FROM catalog_build_content_digests AS digest
                    LEFT JOIN catalog_build_content_owners AS owner
                        ON owner.build_id = digest.build_id
                        AND owner.content_sha256 = digest.content_sha256
                    WHERE digest.build_id = %s
                        AND digest.content_sha256 IS NOT NULL
                        AND owner.content_sha256 IS NULL
                    LIMIT 1
                """
                message = "A non-null content group has no owner"
            case CatalogAnalysisPhase.gid_winners:
                query = """
                    SELECT gallery.gallery_name
                    FROM catalog_source_galleries AS gallery
                    JOIN catalog_build_content_digests AS digest
                        ON digest.build_id = gallery.build_id
                        AND digest.gallery_key = gallery.gallery_key
                    LEFT JOIN catalog_build_content_owners AS owner
                        ON owner.build_id = digest.build_id
                        AND owner.content_sha256 = digest.content_sha256
                    LEFT JOIN catalog_build_gid_winners AS winner
                        ON winner.build_id = gallery.build_id
                        AND winner.gid = gallery.gid
                    WHERE gallery.build_id = %s
                        AND (
                            digest.content_sha256 IS NULL
                            OR owner.owner_gallery_key = gallery.gallery_key
                        )
                        AND winner.gid IS NULL
                    LIMIT 1
                """
                message = "A GID candidate group has no winner"
            case CatalogAnalysisPhase.final_analyses:
                query = """
                    SELECT gallery_name
                    FROM catalog_source_galleries
                    WHERE build_id = %s AND analysis_complete = 0
                    LIMIT 1
                """
                message = "A source gallery has no final analysis"
        if query is not None and connector.fetch_one(query, (build_id,)):
            raise CatalogBuildStateError(message)

    def _require_source_gallery(
        self,
        connector: SQLConnector,
        build_id: str,
        gallery_name: str,
    ) -> tuple[str, int]:
        gallery_key = _stable_key(gallery_name)
        row = connector.fetch_one(
            """
            SELECT gallery_name, gid, source_complete
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key = %s
            """,
            (build_id, gallery_key),
        )
        if not row or str(row[0]) != gallery_name or not bool(row[2]):
            raise CatalogBuildStateError(
                "Analysis references a source gallery that is not complete"
            )
        return gallery_key, int(row[1])

    @staticmethod
    def _validate_unique(values: Sequence[object], label: str) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"A batch contains a duplicate {label}")

    @staticmethod
    def _validate_sha256(value: str, label: str) -> None:
        if len(value) != 64:
            raise ValueError(f"{label} must contain 64 hexadecimal characters")
        try:
            bytes.fromhex(value)
        except ValueError as error:
            raise ValueError(f"{label} is not hexadecimal") from error
