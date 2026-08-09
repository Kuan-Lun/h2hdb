from __future__ import annotations

__all__ = [
    "CatalogBuildProjectionRepository",
    "CatalogProjectionBatchConflictError",
    "CatalogProjectionStateError",
]

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from .catalog_build_repository import CatalogBuildRepository
from .catalog_repository import (
    CatalogProjectionRepository,
    _projection_accumulator_add,
    _projection_datetime,
    _projection_sha256_from_accumulator,
    _publication_item_sha256,
    _stable_key,
)
from .domain import (
    CatalogArtifact,
    CatalogBuild,
    CatalogBuildPhase,
    CatalogBuildProjection,
    CatalogBuildProjectionBatchResult,
    CatalogBuildProjectionPhase,
    CatalogBuildProjectionPruneResult,
    CatalogBuildProjectionPublishResult,
    CatalogContributor,
    CatalogPreparedArtifact,
    CatalogProjectionArtifactCursor,
    CatalogProjectionArtifactPage,
    CatalogProjectionCheckpoint,
    CatalogProjectionPublicationReceipt,
    CatalogProjectionPublicationState,
    CatalogProjectionSelectedFile,
    CatalogProjectionSelectedFileCursor,
    CatalogProjectionSelectedFilePage,
    CatalogProjectionSelectedGallery,
    CatalogProjectionSelectedGalleryCursor,
    CatalogProjectionSelectedGalleryPage,
    CatalogProjectionSelection,
    CatalogProjectionSelectionCursor,
    CatalogProjectionSelectionPage,
    CatalogPublication,
    CatalogPublishedArtifact,
    CatalogRevision,
    CatalogSubject,
    GalleryTag,
)
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector
from .table_gallery_ingest_coordination import GalleryIngestTurn

MAX_PROJECTION_PAGE_SIZE = 200
EMPTY_PROJECTION_SHA256 = (
    "1837ec8c05bd8de86daee2888f575d68c04306633946aeac22a07316b607ae0e"
)
INITIAL_STAGE_CHAIN_SHA256 = sha256(b"h2hdb-projection-stage-v1\0").hexdigest()
_CONTRIBUTOR_TAGS = frozenset(
    {"artist", "author", "cosplayer", "group", "illustrator", "uploader"}
)


class CatalogProjectionStateError(RuntimeError):
    pass


class CatalogProjectionBatchConflictError(RuntimeError):
    pass


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _payload_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _artifact_payload(artifact: CatalogArtifact) -> tuple[object, ...]:
    return (
        artifact.artifact_id,
        artifact.name,
        str(artifact.location),
        artifact.media_type,
        artifact.size_bytes,
        artifact.sha256,
        _projection_datetime(artifact.modified_at),
    )


def _deduplicate_contributors(
    contributors: Sequence[CatalogContributor],
) -> tuple[CatalogContributor, ...]:
    result: list[CatalogContributor] = []
    seen: set[tuple[str, str]] = set()
    for contributor in contributors:
        identity = (contributor.role, contributor.name)
        if identity in seen:
            continue
        seen.add(identity)
        result.append(contributor)
    return tuple(result)


class CatalogBuildProjectionRepository(BaseRepository):
    """Stage an invisible catalog revision and jointly activate both pointers."""

    def __init__(
        self,
        context: RepositoryContext,
        builds: CatalogBuildRepository,
        catalog: CatalogProjectionRepository,
    ) -> None:
        super().__init__(context)
        self._builds = builds
        self._catalog = catalog

    @staticmethod
    def _lock_clause(sql_type: str) -> str:
        return " FOR UPDATE" if sql_type == "mariadb" else ""

    @staticmethod
    def _validate_page_limit(limit: int) -> None:
        if not 1 <= limit <= MAX_PROJECTION_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PROJECTION_PAGE_SIZE}")

    @staticmethod
    def _validate_batch_id(batch_id: str) -> None:
        if not batch_id or len(batch_id) > 191:
            raise ValueError("batch_id must contain between 1 and 191 characters")

    def _allocate_revision_with_connector(self, connector: SQLConnector) -> int:
        lock = self._lock_clause(self._context.sql_type)
        row = connector.fetch_one("""
            SELECT next_revision
            FROM catalog_revision_allocator
            WHERE singleton_id = 1
            """ + lock)
        if not row:
            raise RuntimeError("catalog_revision_allocator singleton is missing")
        revision = int(row[0])
        while connector.fetch_one(
            """
            SELECT 1 FROM catalog_revision_history WHERE revision = %s
            UNION ALL
            SELECT 1 FROM catalog_build_projections WHERE reserved_revision = %s
            LIMIT 1
            """,
            (revision, revision),
        ):
            revision += 1
        connector.execute(
            """
            UPDATE catalog_revision_allocator
            SET next_revision = %s
            WHERE singleton_id = 1
            """,
            (revision + 1,),
        )
        return revision

    def _begin_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        artifacts_required: bool,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        build = self._builds._require_owned_build(connector, build_id, turn)
        existing = self._select_projection(connector, build_id, for_update=True)
        if existing:
            projection = self._projection_from_row(existing)
            if projection.artifacts_required != artifacts_required:
                raise CatalogProjectionStateError(
                    "Catalog projection was resumed with a different artifact policy"
                )
            return projection
        if build.phase is not CatalogBuildPhase.artifacts:
            raise CatalogProjectionStateError(
                "Catalog projection can begin only after source analysis completes"
            )
        lock = self._lock_clause(self._context.sql_type)
        pointer = connector.fetch_one("""
            SELECT current_revision
            FROM catalog_revision
            WHERE singleton_id = 1
            """ + lock)
        if not pointer:
            raise RuntimeError("catalog_revision singleton is missing")
        selected_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries
            WHERE build_id = %s AND analysis_complete = 1 AND selected = 1
            """,
            (build_id,),
        )
        selected_count = int(selected_row[0]) if selected_row else 0
        revision = self._allocate_revision_with_connector(connector)
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            INSERT INTO catalog_build_projections (
                build_id,
                reserved_revision,
                base_catalog_revision,
                artifacts_required,
                phase,
                artifact_after_gallery_key,
                selection_after_gallery_key,
                selected_gallery_count,
                protected_artifact_count,
                staged_selection_count,
                projection_chain_sha256,
                projection_xor_sha256,
                projection_sum_sha256,
                projection_sha256,
                new_galleries,
                changed_galleries,
                removed_galleries,
                duplicate_losers,
                published_catalog_revision,
                created_at,
                updated_at,
                sealed_at,
                published_at
            ) VALUES (
                %s, %s, %s, %s, %s, NULL, NULL, %s, 0, 0, %s, %s, %s,
                NULL, 0, 0, 0, 0, NULL, %s, %s, NULL, NULL
            )
            """,
            (
                build_id,
                revision,
                int(pointer[0]),
                artifacts_required,
                CatalogBuildProjectionPhase.preparing_artifacts.value,
                selected_count,
                INITIAL_STAGE_CHAIN_SHA256,
                "0" * 64,
                "0" * 64,
                now,
                now,
            ),
        )
        return self._require_projection(connector, build_id, for_update=False)

    def get_projection(self, build_id: str) -> CatalogBuildProjection | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                row = self._select_projection(connector, build_id, for_update=False)
        return None if not row else self._projection_from_row(row)

    def get_checkpoint(self, build_id: str) -> CatalogProjectionCheckpoint:
        projection = self.get_projection(build_id)
        if projection is None:
            raise LookupError(f"Catalog build {build_id!r} has no projection")
        return CatalogProjectionCheckpoint(
            build_id=build_id,
            phase=projection.phase,
            artifact_after_gallery_key=projection.artifact_after_gallery_key,
            selection_after_gallery_key=projection.selection_after_gallery_key,
        )

    def page_selected_galleries(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectedGalleryCursor | None,
        limit: int,
    ) -> CatalogProjectionSelectedGalleryPage:
        self._validate_page_limit(limit)
        after_clause = ""
        parameters: tuple[object, ...] = (build_id,)
        if after is not None:
            after_clause = "AND source.gallery_key > %s"
            parameters += (after.gallery_key,)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_projection(connector, build_id, for_update=False)
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        source.gallery_key,
                        source.gallery_name,
                        discovery.source_locator,
                        source.gid,
                        source.title,
                        source.comment,
                        source.upload_account,
                        source.upload_time,
                        source.download_time,
                        source.modified_time,
                        source.page_count,
                        source.metadata_sha256,
                        source.source_manifest_sha256,
                        source.content_sha256
                    FROM catalog_source_galleries AS source
                    JOIN catalog_build_discoveries AS discovery
                        ON discovery.build_id = source.build_id
                        AND discovery.gallery_key = source.gallery_key
                    WHERE source.build_id = %s
                        AND source.analysis_complete = 1
                        AND source.selected = 1
                        {after_clause}
                    ORDER BY source.gallery_key
                    LIMIT %s
                    """,
                    (*parameters, limit),
                )
                tags = self._tags_for_gallery_keys(
                    connector,
                    build_id,
                    tuple(str(row[0]) for row in rows),
                )
        items: list[CatalogProjectionSelectedGallery] = []
        for row in rows:
            if row[10] is None or row[11] is None or row[12] is None:
                raise CatalogProjectionStateError(
                    f"Selected gallery {row[1]!r} lacks sealed source metadata"
                )
            items.append(
                CatalogProjectionSelectedGallery(
                    gallery_key=str(row[0]),
                    gallery_name=str(row[1]),
                    source_locator=str(row[2]),
                    gid=int(row[3]),
                    title=str(row[4]),
                    comment=str(row[5]),
                    upload_account=str(row[6]),
                    upload_time=_parse_datetime(row[7]),
                    download_time=_parse_datetime(row[8]),
                    modified_time=_parse_datetime(row[9]),
                    page_count=int(row[10]),
                    tags=tags.get(str(row[0]), ()),
                    metadata_sha256=str(row[11]),
                    source_manifest_sha256=str(row[12]),
                    content_sha256=None if row[13] is None else str(row[13]),
                )
            )
        next_cursor = items[-1].cursor if items else None
        return CatalogProjectionSelectedGalleryPage(tuple(items), next_cursor, limit)

    def page_selected_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: CatalogProjectionSelectedFileCursor | None,
        limit: int,
    ) -> CatalogProjectionSelectedFilePage:
        self._validate_page_limit(limit)
        after_clause = ""
        parameters: tuple[object, ...] = (build_id, gallery_key)
        if after is not None:
            after_clause = """
                AND (
                    file.file_sort_key > %s
                    OR (file.file_sort_key = %s AND file.file_name > %s)
                    OR (
                        file.file_sort_key = %s AND file.file_name = %s
                        AND file.file_key > %s
                    )
                )
            """
            parameters += (
                after.file_sort_key,
                after.file_sort_key,
                after.file_name,
                after.file_sort_key,
                after.file_name,
                after.file_key,
            )
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_projection(connector, build_id, for_update=False)
                selected = connector.fetch_one(
                    """
                    SELECT 1 FROM catalog_source_galleries
                    WHERE build_id = %s AND gallery_key = %s
                        AND analysis_complete = 1 AND selected = 1
                    """,
                    (build_id, gallery_key),
                )
                if not selected:
                    raise LookupError("Gallery is not selected by this source build")
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        file.file_key,
                        file.file_sort_key,
                        file.file_name,
                        file.relative_locator,
                        file.device,
                        file.inode,
                        file.modified_ns,
                        file.changed_ns,
                        file.size_bytes,
                        file.sha256,
                        CASE WHEN excluded.sha256 IS NULL THEN 0 ELSE 1 END
                    FROM catalog_source_files AS file
                    LEFT JOIN catalog_build_excluded_file_hashes AS excluded
                        ON excluded.build_id = file.build_id
                        AND excluded.sha256 = file.sha256
                    WHERE file.build_id = %s AND file.gallery_key = %s
                        {after_clause}
                    ORDER BY file.file_sort_key, file.file_name, file.file_key
                    LIMIT %s
                    """,
                    (*parameters, limit),
                )
        items: list[CatalogProjectionSelectedFile] = []
        for row in rows:
            if any(value is None for value in row[3:8]):
                raise CatalogProjectionStateError(
                    "Selected source file lacks its durable locator/stat signature"
                )
            items.append(
                CatalogProjectionSelectedFile(
                    gallery_key=gallery_key,
                    file_key=str(row[0]),
                    file_sort_key=str(row[1]),
                    file_name=str(row[2]),
                    relative_locator=str(row[3]),
                    device=int(row[4]),
                    inode=int(row[5]),
                    modified_ns=int(row[6]),
                    changed_ns=int(row[7]),
                    size_bytes=int(row[8]),
                    sha256=str(row[9]),
                    excluded=bool(row[10]),
                )
            )
        next_cursor = items[-1].cursor if items else None
        return CatalogProjectionSelectedFilePage(tuple(items), next_cursor, limit)

    @staticmethod
    def _tags_for_gallery_keys(
        connector: SQLConnector,
        build_id: str,
        gallery_keys: tuple[str, ...],
    ) -> dict[str, tuple[GalleryTag, ...]]:
        if not gallery_keys:
            return {}
        placeholders = ", ".join("%s" for _ in gallery_keys)
        rows = connector.fetch_all(
            f"""
            SELECT gallery_key, tag_name, tag_value
            FROM catalog_source_tags
            WHERE build_id = %s AND gallery_key IN ({placeholders})
            ORDER BY gallery_key, position
            """,
            (build_id, *gallery_keys),
        )
        mutable: dict[str, list[GalleryTag]] = {}
        for gallery_key, name, value in rows:
            mutable.setdefault(str(gallery_key), []).append(
                GalleryTag(str(name), str(value))
            )
        return {key: tuple(values) for key, values in mutable.items()}

    def record_prepared_artifacts(
        self,
        connector: SQLConnector,
        build_id: str,
        prepared_artifacts: Sequence[CatalogPreparedArtifact],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(prepared_artifacts)
        if not values or len(values) > MAX_PROJECTION_PAGE_SIZE:
            raise ValueError(
                "Prepared artifact batch must contain between 1 and "
                f"{MAX_PROJECTION_PAGE_SIZE} items"
            )
        gallery_keys = tuple(value.gallery_key for value in values)
        if len(gallery_keys) != len(set(gallery_keys)):
            raise ValueError("Prepared artifact batch contains duplicate galleries")
        self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        if projection.phase is not CatalogBuildProjectionPhase.preparing_artifacts:
            raise CatalogProjectionStateError(
                "Prepared artifacts can be recorded only during artifact preparation"
            )
        payload = tuple(
            (prepared.gallery_key, _artifact_payload(prepared.artifact))
            for prepared in values
        )
        payload_sha256 = _payload_sha256(payload)
        previous = self._existing_batch(
            connector,
            build_id,
            "PREPARED_ARTIFACTS",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch(build_id, batch_id, payload_sha256, previous)
        placeholders = ", ".join("%s" for _ in gallery_keys)
        selected_rows = connector.fetch_all(
            f"""
            SELECT gallery_key FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key IN ({placeholders})
                AND analysis_complete = 1 AND selected = 1
            """,
            (build_id, *gallery_keys),
        )
        if {str(row[0]) for row in selected_rows} != set(gallery_keys):
            raise CatalogProjectionStateError(
                "Prepared artifacts must belong to selected galleries"
            )
        existing_rows = connector.fetch_all(
            f"""
            SELECT gallery_key, payload_sha256
            FROM catalog_build_prepared_artifacts
            WHERE build_id = %s AND gallery_key IN ({placeholders})
            """,
            (build_id, *gallery_keys),
        )
        existing = {str(row[0]): str(row[1]) for row in existing_rows}
        rows_to_insert: list[tuple[object, ...]] = []
        for prepared in values:
            artifact = prepared.artifact
            item_sha256 = _payload_sha256(
                (prepared.gallery_key, _artifact_payload(artifact))
            )
            previous_item = existing.get(prepared.gallery_key)
            if previous_item is not None and previous_item != item_sha256:
                raise CatalogProjectionBatchConflictError(
                    "A gallery artifact was retried with different data"
                )
            if previous_item is not None:
                continue
            rows_to_insert.append(
                (
                    build_id,
                    prepared.gallery_key,
                    item_sha256,
                    _stable_key(artifact.artifact_id),
                    _stable_key(artifact.name),
                    artifact.artifact_id,
                    artifact.name,
                    str(artifact.location),
                    artifact.media_type,
                    artifact.size_bytes,
                    artifact.sha256,
                    _projection_datetime(artifact.modified_at),
                )
            )
        if rows_to_insert:
            connector.execute_many(
                """
                INSERT INTO catalog_build_prepared_artifacts (
                    build_id,
                    gallery_key,
                    payload_sha256,
                    artifact_key,
                    artifact_name_key,
                    artifact_id,
                    name,
                    location,
                    media_type,
                    size_bytes,
                    sha256,
                    modified_at,
                    protected
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                """,
                rows_to_insert,
            )
        self._record_batch(
            connector,
            build_id,
            "PREPARED_ARTIFACTS",
            batch_id,
            payload_sha256,
            len(values),
        )
        return CatalogBuildProjectionBatchResult(
            build_id,
            batch_id,
            bool(rows_to_insert),
            len(values),
        )

    def record_prepared_artifact(
        self,
        connector: SQLConnector,
        build_id: str,
        prepared: CatalogPreparedArtifact,
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        return self.record_prepared_artifacts(
            connector,
            build_id,
            (prepared,),
            batch_id=batch_id,
            turn=turn,
        )

    def advance_artifact_checkpoint(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        after: CatalogProjectionSelectedGalleryCursor,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        self._validate_batch_id(batch_id)
        self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        payload_sha256 = _payload_sha256(
            (
                None if expected_after is None else expected_after.gallery_key,
                after.gallery_key,
            )
        )
        previous = self._existing_batch(
            connector,
            build_id,
            "ARTIFACT_PAGE",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch(build_id, batch_id, payload_sha256, previous)
        if projection.phase is not CatalogBuildProjectionPhase.preparing_artifacts:
            raise CatalogProjectionStateError(
                "Artifact checkpoint can advance only during artifact preparation"
            )
        expected_key = None if expected_after is None else expected_after.gallery_key
        if projection.artifact_after_gallery_key != expected_key:
            raise CatalogProjectionBatchConflictError(
                "Artifact checkpoint compare-and-swap cursor did not match"
            )
        range_clause = "source.gallery_key <= %s"
        parameters: tuple[object, ...] = (build_id, after.gallery_key)
        if expected_key is not None:
            range_clause = "source.gallery_key > %s AND source.gallery_key <= %s"
            parameters = (build_id, expected_key, after.gallery_key)
        rows = connector.fetch_all(
            f"""
            SELECT source.gallery_key, artifact.protected
            FROM catalog_source_galleries AS source
            LEFT JOIN catalog_build_prepared_artifacts AS artifact
                ON artifact.build_id = source.build_id
                AND artifact.gallery_key = source.gallery_key
            WHERE source.build_id = %s
                AND source.analysis_complete = 1 AND source.selected = 1
                AND {range_clause}
            ORDER BY source.gallery_key
            LIMIT {MAX_PROJECTION_PAGE_SIZE + 1}
            """,
            parameters,
        )
        if len(rows) > MAX_PROJECTION_PAGE_SIZE:
            raise CatalogProjectionStateError(
                "Artifact checkpoint range exceeds the bounded page limit"
            )
        if not rows or str(rows[-1][0]) != after.gallery_key:
            raise CatalogProjectionStateError(
                "Artifact checkpoint does not end on a selected gallery"
            )
        if projection.artifacts_required and any(row[1] is None for row in rows):
            raise CatalogProjectionStateError(
                "Artifact page is missing a prepared artifact for a selected gallery"
            )
        protected_count = 0
        if projection.artifacts_required:
            keys = tuple(str(row[0]) for row in rows)
            placeholders = ", ".join("%s" for _ in keys)
            unprotected = sum(not bool(row[1]) for row in rows)
            connector.execute(
                f"""
                UPDATE catalog_build_prepared_artifacts
                SET protected = 1
                WHERE build_id = %s AND gallery_key IN ({placeholders})
                """,
                (build_id, *keys),
            )
            protected_count = unprotected
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET artifact_after_gallery_key = %s,
                protected_artifact_count = protected_artifact_count + %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (after.gallery_key, protected_count, now, build_id),
        )
        self._record_batch(
            connector,
            build_id,
            "ARTIFACT_PAGE",
            batch_id,
            payload_sha256,
            len(rows),
        )
        return CatalogBuildProjectionBatchResult(
            build_id,
            batch_id,
            True,
            len(rows),
        )

    def complete_artifact_preparation(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        expected_after: CatalogProjectionSelectedGalleryCursor | None,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        if projection.phase is CatalogBuildProjectionPhase.staging_selections:
            return projection
        if projection.phase is not CatalogBuildProjectionPhase.preparing_artifacts:
            raise CatalogProjectionStateError(
                "Artifact preparation is not the active projection phase"
            )
        expected_key = None if expected_after is None else expected_after.gallery_key
        if projection.artifact_after_gallery_key != expected_key:
            raise CatalogProjectionBatchConflictError(
                "Artifact completion compare-and-swap cursor did not match"
            )
        if projection.artifacts_required:
            remaining = connector.fetch_one(
                """
                SELECT 1
                FROM catalog_source_galleries
                WHERE build_id = %s AND analysis_complete = 1 AND selected = 1
                    AND (%s IS NULL OR gallery_key > %s)
                LIMIT 1
                """,
                (build_id, expected_key, expected_key),
            )
            if remaining:
                raise CatalogProjectionStateError(
                    "Artifact preparation cannot complete before all selected galleries"
                )
            completed_after = expected_key
        else:
            last_row = connector.fetch_one(
                """
                SELECT MAX(gallery_key)
                FROM catalog_source_galleries
                WHERE build_id = %s AND analysis_complete = 1 AND selected = 1
                """,
                (build_id,),
            )
            completed_after = (
                None if not last_row or last_row[0] is None else str(last_row[0])
            )
        if (
            projection.artifacts_required
            and projection.protected_artifact_count != projection.selected_gallery_count
        ):
            raise CatalogProjectionStateError(
                "Not every selected gallery has a protected artifact"
            )
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET phase = %s,
                artifact_after_gallery_key = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildProjectionPhase.staging_selections.value,
                completed_after,
                now,
                build_id,
            ),
        )
        return self._require_projection(connector, build_id, for_update=False)

    def page_projection_selections(
        self,
        build_id: str,
        *,
        after: CatalogProjectionSelectionCursor | None,
        limit: int,
    ) -> CatalogProjectionSelectionPage:
        self._validate_page_limit(limit)
        after_clause = ""
        parameters: tuple[object, ...] = (build_id,)
        if after is not None:
            after_clause = "AND source.gallery_key > %s"
            parameters += (after.gallery_key,)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                projection = self._require_projection(
                    connector,
                    build_id,
                    for_update=False,
                )
                if projection.phase not in {
                    CatalogBuildProjectionPhase.staging_selections,
                    CatalogBuildProjectionPhase.complete,
                }:
                    raise CatalogProjectionStateError(
                        "Projection selections are not ready to page"
                    )
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        source.gallery_key,
                        artifact.artifact_id,
                        artifact.name,
                        artifact.location,
                        artifact.media_type,
                        artifact.size_bytes,
                        artifact.sha256,
                        artifact.modified_at,
                        artifact.protected
                    FROM catalog_source_galleries AS source
                    LEFT JOIN catalog_build_prepared_artifacts AS artifact
                        ON artifact.build_id = source.build_id
                        AND artifact.gallery_key = source.gallery_key
                    WHERE source.build_id = %s
                        AND source.analysis_complete = 1 AND source.selected = 1
                        {after_clause}
                    ORDER BY source.gallery_key
                    LIMIT %s
                    """,
                    (*parameters, limit),
                )
        items: list[CatalogProjectionSelection] = []
        for row in rows:
            artifact: CatalogArtifact | None = None
            if row[1] is not None:
                if not bool(row[8]):
                    raise CatalogProjectionStateError(
                        "A provisional artifact is not eligible for projection"
                    )
                artifact = CatalogArtifact(
                    artifact_id=str(row[1]),
                    name=str(row[2]),
                    location=Path(str(row[3])),
                    media_type=str(row[4]),
                    size_bytes=int(row[5]),
                    sha256=str(row[6]),
                    modified_at=_parse_datetime(row[7]),
                )
            if projection.artifacts_required and artifact is None:
                raise CatalogProjectionStateError(
                    "Selected gallery has no eligible protected artifact"
                )
            items.append(CatalogProjectionSelection(str(row[0]), artifact, False))
        next_cursor = items[-1].cursor if items else None
        return CatalogProjectionSelectionPage(tuple(items), next_cursor, limit)

    def stage_projection_selections(
        self,
        connector: SQLConnector,
        build_id: str,
        selections: Sequence[CatalogProjectionSelection],
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        after: CatalogProjectionSelectionCursor,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(selections)
        if not values or len(values) > MAX_PROJECTION_PAGE_SIZE:
            raise ValueError(
                "Projection selection batch must contain between 1 and "
                f"{MAX_PROJECTION_PAGE_SIZE} items"
            )
        keys = tuple(value.gallery_key for value in values)
        if tuple(sorted(keys)) != keys or len(set(keys)) != len(keys):
            raise ValueError("Projection selections must be strictly keyset ordered")
        if keys[-1] != after.gallery_key:
            raise ValueError("Projection batch cursor must identify its final item")
        payload_sha256 = _payload_sha256(
            (
                None if expected_after is None else expected_after.gallery_key,
                after.gallery_key,
                tuple(
                    (
                        value.gallery_key,
                        (
                            None
                            if value.artifact is None
                            else _artifact_payload(value.artifact)
                        ),
                        value.redownload_required,
                    )
                    for value in values
                ),
            )
        )
        self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        previous = self._existing_batch(
            connector,
            build_id,
            "PROJECTION_SELECTIONS",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch(build_id, batch_id, payload_sha256, previous)
        if projection.phase is not CatalogBuildProjectionPhase.staging_selections:
            raise CatalogProjectionStateError(
                "Projection selections can be staged only in the selection phase"
            )
        expected_key = None if expected_after is None else expected_after.gallery_key
        if projection.selection_after_gallery_key != expected_key:
            raise CatalogProjectionBatchConflictError(
                "Projection selection compare-and-swap cursor did not match"
            )
        range_clause = "gallery_key <= %s"
        range_parameters: tuple[object, ...] = (build_id, after.gallery_key)
        if expected_key is not None:
            range_clause = "gallery_key > %s AND gallery_key <= %s"
            range_parameters = (build_id, expected_key, after.gallery_key)
        expected_rows = connector.fetch_all(
            f"""
            SELECT gallery_key
            FROM catalog_source_galleries
            WHERE build_id = %s AND analysis_complete = 1 AND selected = 1
                AND {range_clause}
            ORDER BY gallery_key
            LIMIT {MAX_PROJECTION_PAGE_SIZE + 1}
            """,
            range_parameters,
        )
        if len(expected_rows) > MAX_PROJECTION_PAGE_SIZE:
            raise CatalogProjectionStateError(
                "Projection selection range exceeds the bounded page limit"
            )
        expected_keys = tuple(str(row[0]) for row in expected_rows)
        if expected_keys != keys:
            raise CatalogProjectionStateError(
                "Projection batch skipped or substituted selected galleries"
            )
        publications = self._derive_publication_batch(
            connector,
            build_id,
            values,
            artifacts_required=projection.artifacts_required,
        )
        self._catalog._insert_publications(
            connector,
            projection.reserved_revision,
            publications,
        )
        item_rows = []
        for selection, publication in zip(values, publications, strict=True):
            item_rows.append(
                (
                    build_id,
                    selection.gallery_key,
                    _stable_key(publication.publication_id),
                    _publication_item_sha256(publication),
                )
            )
        connector.execute_many(
            """
            INSERT INTO catalog_build_projection_items (
                build_id, gallery_key, publication_key, item_sha256
            ) VALUES (%s, %s, %s, %s)
            """,
            item_rows,
        )
        chain = sha256(
            bytes.fromhex(projection.projection_chain_sha256)
            + bytes.fromhex(payload_sha256)
        ).hexdigest()
        xor_hex = projection.projection_xor_sha256
        sum_hex = projection.projection_sum_sha256
        for publication in publications:
            xor_hex, sum_hex = _projection_accumulator_add(
                xor_hex,
                sum_hex,
                publication,
            )
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET selection_after_gallery_key = %s,
                staged_selection_count = staged_selection_count + %s,
                projection_chain_sha256 = %s,
                projection_xor_sha256 = %s,
                projection_sum_sha256 = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                after.gallery_key,
                len(values),
                chain,
                xor_hex,
                sum_hex,
                now,
                build_id,
            ),
        )
        self._record_batch(
            connector,
            build_id,
            "PROJECTION_SELECTIONS",
            batch_id,
            payload_sha256,
            len(values),
        )
        return CatalogBuildProjectionBatchResult(
            build_id,
            batch_id,
            True,
            len(values),
        )

    def _derive_publication_batch(
        self,
        connector: SQLConnector,
        build_id: str,
        selections: tuple[CatalogProjectionSelection, ...],
        *,
        artifacts_required: bool,
    ) -> tuple[CatalogPublication, ...]:
        keys = tuple(value.gallery_key for value in selections)
        placeholders = ", ".join("%s" for _ in keys)
        source_rows = connector.fetch_all(
            f"""
            SELECT
                gallery_key,
                gallery_name,
                gid,
                title,
                comment,
                upload_account,
                upload_time,
                modified_time,
                content_sha256
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key IN ({placeholders})
                AND analysis_complete = 1 AND selected = 1
            """,
            (build_id, *keys),
        )
        sources = {str(row[0]): row for row in source_rows}
        tags = self._tags_for_gallery_keys(connector, build_id, keys)
        prepared_rows = connector.fetch_all(
            f"""
            SELECT
                gallery_key,
                artifact_id,
                name,
                location,
                media_type,
                size_bytes,
                sha256,
                modified_at,
                protected
            FROM catalog_build_prepared_artifacts
            WHERE build_id = %s AND gallery_key IN ({placeholders})
            """,
            (build_id, *keys),
        )
        persisted_artifacts = {str(row[0]): row[1:] for row in prepared_rows}
        publications: list[CatalogPublication] = []
        for selection in selections:
            source = sources.get(selection.gallery_key)
            if source is None:
                raise CatalogProjectionStateError(
                    "Projection selection references a non-selected source"
                )
            persisted = persisted_artifacts.get(selection.gallery_key)
            artifact = selection.artifact
            if artifact is None:
                if artifacts_required:
                    raise CatalogProjectionStateError(
                        "Projection selection omitted its required artifact"
                    )
                if persisted is not None:
                    raise CatalogProjectionBatchConflictError(
                        "Projection selection omitted a persisted artifact"
                    )
            else:
                if persisted is None or not bool(persisted[7]):
                    raise CatalogProjectionStateError(
                        "Projection artifact is absent or not protected"
                    )
                persisted_payload = (
                    str(persisted[0]),
                    str(persisted[1]),
                    str(persisted[2]),
                    str(persisted[3]),
                    int(persisted[4]),
                    str(persisted[5]),
                    _projection_datetime(_parse_datetime(persisted[6])),
                )
                if persisted_payload != _artifact_payload(artifact):
                    raise CatalogProjectionBatchConflictError(
                        "Projection artifact differs from its protected receipt"
                    )
            gallery_tags = tags.get(selection.gallery_key, ())
            contributors: list[CatalogContributor] = []
            upload_account = str(source[5])
            if upload_account:
                contributors.append(CatalogContributor(upload_account, role="uploader"))
            contributors.extend(
                CatalogContributor(tag.value, role=tag.name)
                for tag in gallery_tags
                if tag.name in _CONTRIBUTOR_TAGS and tag.value
            )
            language = next(
                (
                    tag.value
                    for tag in gallery_tags
                    if tag.name == "language" and tag.value
                ),
                "und",
            )
            modified_at = _as_utc(_parse_datetime(source[7]))
            if artifact is not None:
                modified_at = max(modified_at, _as_utc(artifact.modified_at))
            gallery_name = str(source[1])
            source_title = str(source[3])
            display_title = source_title or gallery_name
            publications.append(
                CatalogPublication(
                    publication_id=f"urn:h2h:gallery:{int(source[2])}",
                    gid=int(source[2]),
                    title=display_title,
                    source_title=source_title,
                    sort_title=display_title.casefold(),
                    summary=str(source[4]),
                    language=language,
                    published_at=_as_utc(_parse_datetime(source[6])),
                    modified_at=modified_at,
                    source_gallery_name=gallery_name,
                    contributors=_deduplicate_contributors(contributors),
                    subjects=tuple(
                        CatalogSubject(
                            name=tag.value,
                            scheme=f"h2h:tag:{tag.name}",
                            code=tag.name,
                        )
                        for tag in gallery_tags
                    ),
                    artifacts=() if artifact is None else (artifact,),
                    redownload_required=selection.redownload_required,
                    content_sha256=(None if source[8] is None else str(source[8])),
                )
            )
        return tuple(publications)

    def complete_projection_staging(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        if projection.phase is CatalogBuildProjectionPhase.complete:
            return projection
        if projection.phase is not CatalogBuildProjectionPhase.staging_selections:
            raise CatalogProjectionStateError(
                "Projection staging is not the active phase"
            )
        expected_key = None if expected_after is None else expected_after.gallery_key
        if projection.selection_after_gallery_key != expected_key:
            raise CatalogProjectionBatchConflictError(
                "Projection completion compare-and-swap cursor did not match"
            )
        remaining = connector.fetch_one(
            """
            SELECT 1 FROM catalog_source_galleries
            WHERE build_id = %s AND analysis_complete = 1 AND selected = 1
                AND (%s IS NULL OR gallery_key > %s)
            LIMIT 1
            """,
            (build_id, expected_key, expected_key),
        )
        if remaining:
            raise CatalogProjectionStateError(
                "Projection staging cannot complete before every selected gallery"
            )
        if projection.staged_selection_count != projection.selected_gallery_count:
            raise CatalogProjectionStateError(
                "Projection selection count does not match the selected source count"
            )
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET phase = %s, updated_at = %s
            WHERE build_id = %s
            """,
            (CatalogBuildProjectionPhase.complete.value, now, build_id),
        )
        return self._require_projection(connector, build_id, for_update=False)

    def seal_projection(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        build = self._builds._require_owned_build(connector, build_id, turn)
        projection = self._require_projection(connector, build_id, for_update=True)
        if projection.phase is CatalogBuildProjectionPhase.sealed:
            return projection
        if projection.phase is not CatalogBuildProjectionPhase.complete:
            raise CatalogProjectionStateError(
                "Only a complete staged projection can be sealed"
            )
        if build.phase not in {CatalogBuildPhase.artifacts, CatalogBuildPhase.sealed}:
            raise CatalogProjectionStateError(
                "Source build is not ready for projection sealing"
            )
        publication_count = self._count_revision_rows(
            connector,
            "catalog_publications",
            projection.reserved_revision,
        )
        item_count_row = connector.fetch_one(
            """
            SELECT COUNT(*) FROM catalog_build_projection_items
            WHERE build_id = %s
            """,
            (build_id,),
        )
        item_count = int(item_count_row[0]) if item_count_row else 0
        if (
            publication_count != projection.selected_gallery_count
            or item_count != projection.selected_gallery_count
            or projection.staged_selection_count != projection.selected_gallery_count
        ):
            raise CatalogProjectionStateError(
                "Projection row counts do not match the selected source count"
            )
        artifact_count = self._count_revision_rows(
            connector,
            "catalog_artifacts",
            projection.reserved_revision,
        )
        if projection.artifacts_required and artifact_count != publication_count:
            raise CatalogProjectionStateError(
                "Sealed projection requires one artifact per selected publication"
            )
        if not projection.artifacts_required and artifact_count:
            raise CatalogProjectionStateError(
                "Artifact-disabled projection unexpectedly contains artifacts"
            )
        projection_sha256 = _projection_sha256_from_accumulator(
            projection.selected_gallery_count,
            projection.projection_xor_sha256,
            projection.projection_sum_sha256,
        )
        self._ensure_revision_projection_sha256(
            connector,
            projection.base_catalog_revision,
        )
        new_galleries, changed_galleries, removed_galleries = self._source_diff_counts(
            connector,
            build,
        )
        loser_row = connector.fetch_one(
            """
            SELECT COUNT(*) FROM catalog_source_galleries
            WHERE build_id = %s AND analysis_complete = 1 AND selected = 0
            """,
            (build_id,),
        )
        duplicate_losers = int(loser_row[0]) if loser_row else 0
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET phase = %s,
                projection_sha256 = %s,
                new_galleries = %s,
                changed_galleries = %s,
                removed_galleries = %s,
                duplicate_losers = %s,
                sealed_at = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildProjectionPhase.sealed.value,
                projection_sha256,
                new_galleries,
                changed_galleries,
                removed_galleries,
                duplicate_losers,
                now,
                now,
                build_id,
            ),
        )
        return self._require_projection(connector, build_id, for_update=False)

    @staticmethod
    def _count_revision_rows(
        connector: SQLConnector,
        table_name: str,
        revision: int,
    ) -> int:
        row = connector.fetch_one(
            f"SELECT COUNT(*) FROM {table_name} WHERE revision = %s",
            (revision,),
        )
        return int(row[0]) if row else 0

    def _ensure_revision_projection_sha256(
        self,
        connector: SQLConnector,
        revision: int,
    ) -> str:
        row = connector.fetch_one(
            """
            SELECT projection_sha256
            FROM catalog_revision_history
            WHERE revision = %s
            """,
            (revision,),
        )
        if not row:
            raise CatalogProjectionStateError(
                f"Base catalog revision {revision} no longer exists"
            )
        if row[0] is None:
            raise CatalogProjectionStateError(
                "Base catalog digest was not backfilled by schema migration"
            )
        return str(row[0])

    def _source_diff_counts(
        self,
        connector: SQLConnector,
        build: CatalogBuild,
    ) -> tuple[int, int, int]:
        if build.base_active_build_id is None:
            exact_name = (
                "BINARY legacy_name.full_name = BINARY candidate.gallery_name"
                if self._context.sql_type == "mariadb"
                else "CAST(legacy_name.full_name AS BLOB) = "
                "CAST(candidate.gallery_name AS BLOB)"
            )
            new_row = connector.fetch_one(
                f"""
                SELECT COUNT(DISTINCT candidate.gallery_key)
                FROM catalog_source_galleries AS candidate
                LEFT JOIN galleries_names AS legacy_name
                    ON {exact_name}
                WHERE candidate.build_id = %s
                    AND legacy_name.db_gallery_id IS NULL
                """,
                (build.build_id,),
            )
            changed_row = connector.fetch_one(
                f"""
                SELECT COUNT(DISTINCT candidate.gallery_key)
                FROM catalog_source_galleries AS candidate
                JOIN galleries_names AS legacy_name
                    ON {exact_name}
                LEFT JOIN gallery_source_manifests AS legacy_manifest
                    ON legacy_manifest.db_gallery_id = legacy_name.db_gallery_id
                WHERE candidate.build_id = %s
                    AND (
                        legacy_manifest.db_gallery_id IS NULL
                        OR LOWER(HEX(legacy_manifest.sha256)) <>
                            candidate.source_manifest_sha256
                    )
                """,
                (build.build_id,),
            )
            removed_row = connector.fetch_one(
                f"""
                SELECT COUNT(*)
                FROM galleries_names AS legacy_name
                LEFT JOIN catalog_source_galleries AS candidate
                    ON candidate.build_id = %s
                    AND {exact_name}
                WHERE candidate.gallery_key IS NULL
                """,
                (build.build_id,),
            )
            return (
                int(new_row[0]) if new_row else 0,
                int(changed_row[0]) if changed_row else 0,
                int(removed_row[0]) if removed_row else 0,
            )
        new_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries AS candidate
            LEFT JOIN catalog_source_galleries AS base
                ON base.build_id = %s
                AND base.gallery_key = candidate.gallery_key
            WHERE candidate.build_id = %s AND base.gallery_key IS NULL
            """,
            (build.base_active_build_id, build.build_id),
        )
        changed_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries AS candidate
            JOIN catalog_source_galleries AS base
                ON base.build_id = %s
                AND base.gallery_key = candidate.gallery_key
            WHERE candidate.build_id = %s
                AND (
                    base.source_manifest_sha256 <> candidate.source_manifest_sha256
                    OR base.source_manifest_version <> candidate.source_manifest_version
                )
            """,
            (build.base_active_build_id, build.build_id),
        )
        removed_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries AS base
            LEFT JOIN catalog_source_galleries AS candidate
                ON candidate.build_id = %s
                AND candidate.gallery_key = base.gallery_key
            WHERE base.build_id = %s AND candidate.gallery_key IS NULL
            """,
            (build.build_id, build.base_active_build_id),
        )
        return (
            int(new_row[0]) if new_row else 0,
            int(changed_row[0]) if changed_row else 0,
            int(removed_row[0]) if removed_row else 0,
        )

    def publish_with_projection(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildProjectionPublishResult:
        """Perform the O(1) source/catalog pointer commit.

        All row-heavy validation and digest work is deliberately completed by
        :meth:`seal_projection`.  This transaction reads only fenced state,
        pointer singletons, and immutable descriptors.
        """

        self._builds._require_owned_build(connector, build_id, turn)
        existing_receipt = self._select_receipt(connector, build_id)
        if existing_receipt is not None:
            build = self._builds._require_build(
                connector,
                build_id,
                for_update=False,
            )
            projection = self._require_projection(
                connector,
                build_id,
                for_update=False,
            )
            return CatalogBuildProjectionPublishResult(
                build,
                projection,
                self._receipt_from_row(existing_receipt),
            )
        build = self._builds._require_build(connector, build_id, for_update=True)
        projection = self._require_projection(connector, build_id, for_update=True)
        if build.phase is not CatalogBuildPhase.sealed:
            raise CatalogProjectionStateError(
                "Source build must be sealed before joint publication"
            )
        if projection.phase is not CatalogBuildProjectionPhase.sealed:
            raise CatalogProjectionStateError(
                "Catalog projection must be sealed before joint publication"
            )
        if projection.projection_sha256 is None:
            raise CatalogProjectionStateError("Sealed projection digest is missing")
        lock = self._lock_clause(self._context.sql_type)
        source_pointer = connector.fetch_one("""
            SELECT current_revision, active_build_id
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + lock)
        catalog_pointer = connector.fetch_one("""
            SELECT current_revision
            FROM catalog_revision
            WHERE singleton_id = 1
            """ + lock)
        control = connector.fetch_one("""
            SELECT working_build_id
            FROM catalog_build_control
            WHERE singleton_id = 1
            """ + lock)
        if not source_pointer or not catalog_pointer or not control:
            raise RuntimeError("Catalog pointer singletons are missing")
        active_build_id = None if source_pointer[1] is None else str(source_pointer[1])
        if (
            int(source_pointer[0]) != build.base_source_revision
            or active_build_id != build.base_active_build_id
            or int(catalog_pointer[0]) != projection.base_catalog_revision
        ):
            raise CatalogProjectionStateError(
                "Source or catalog base revision changed before joint publication"
            )
        if control[0] is None or str(control[0]) != build_id:
            raise CatalogProjectionStateError(
                "Sealed source build is not the current working build"
            )
        if (
            build.seal_sha256 is None
            or build.expected_gallery_count is None
            or build.staged_gallery_count != build.expected_gallery_count
            or build.analyzed_gallery_count != build.expected_gallery_count
        ):
            raise CatalogProjectionStateError("Sealed source descriptor is incomplete")
        if projection.staged_selection_count != projection.selected_gallery_count or (
            projection.artifacts_required
            and projection.protected_artifact_count != projection.selected_gallery_count
        ):
            raise CatalogProjectionStateError(
                "Sealed projection descriptor is incomplete"
            )
        current_digest_row = connector.fetch_one(
            """
            SELECT projection_sha256
            FROM catalog_revision_history
            WHERE revision = %s
            """,
            (projection.base_catalog_revision,),
        )
        if not current_digest_row or current_digest_row[0] is None:
            raise CatalogProjectionStateError(
                "Base catalog revision has no canonical projection digest"
            )
        now = self._builds._database_datetime(connector)
        source_revision = int(source_pointer[0]) + 1
        connector.execute(
            """
            INSERT INTO catalog_source_revision_history (
                revision, build_id, published_at, gallery_count, file_count
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                source_revision,
                build_id,
                now.isoformat(),
                build.staged_gallery_count,
                build.staged_file_count,
            ),
        )
        if str(current_digest_row[0]) == projection.projection_sha256:
            catalog_revision = projection.base_catalog_revision
            catalog_created = False
        else:
            catalog_revision = projection.reserved_revision
            catalog_created = True
            connector.execute(
                """
                INSERT INTO catalog_revision_history (
                    revision,
                    published_at,
                    publication_count,
                    projection_sha256
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    catalog_revision,
                    now.isoformat(),
                    projection.selected_gallery_count,
                    projection.projection_sha256,
                ),
            )
        connector.execute(
            """
            UPDATE catalog_source_revision
            SET current_revision = %s,
                active_build_id = %s,
                published_at = %s,
                gallery_count = %s,
                file_count = %s
            WHERE singleton_id = 1
            """,
            (
                source_revision,
                build_id,
                now.isoformat(),
                build.staged_gallery_count,
                build.staged_file_count,
            ),
        )
        if catalog_created:
            connector.execute(
                """
                UPDATE catalog_revision
                SET current_revision = %s,
                    published_at = %s,
                    publication_count = %s
                WHERE singleton_id = 1
                """,
                (
                    catalog_revision,
                    now.isoformat(),
                    projection.selected_gallery_count,
                ),
            )
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s,
                published_source_revision = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildPhase.published.value,
                source_revision,
                now.isoformat(),
                build_id,
            ),
        )
        connector.execute(
            """
            UPDATE catalog_build_projections
            SET phase = %s,
                published_catalog_revision = %s,
                published_at = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildProjectionPhase.published.value,
                catalog_revision,
                now.isoformat(),
                now.isoformat(),
                build_id,
            ),
        )
        connector.execute(
            """
            UPDATE catalog_build_control
            SET working_build_id = NULL
            WHERE singleton_id = 1 AND working_build_id = %s
            """,
            (build_id,),
        )
        connector.execute(
            """
            INSERT INTO catalog_projection_publication_receipts (
                build_id,
                source_revision,
                catalog_revision,
                projection_sha256,
                state,
                new_galleries,
                changed_galleries,
                removed_galleries,
                duplicate_losers,
                selected_galleries,
                committed_at,
                finalized_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
            """,
            (
                build_id,
                source_revision,
                catalog_revision,
                projection.projection_sha256,
                CatalogProjectionPublicationState.database_committed.value,
                projection.new_galleries,
                projection.changed_galleries,
                projection.removed_galleries,
                projection.duplicate_losers,
                projection.selected_gallery_count,
                now.isoformat(),
            ),
        )
        published_build = self._builds._require_build(
            connector,
            build_id,
            for_update=False,
        )
        published_projection = self._require_projection(
            connector,
            build_id,
            for_update=False,
        )
        receipt_row = self._select_receipt(connector, build_id)
        assert receipt_row is not None
        return CatalogBuildProjectionPublishResult(
            published_build,
            published_projection,
            self._receipt_from_row(receipt_row),
        )

    def get_publication_receipt(
        self,
        build_id: str | None = None,
        *,
        pending_only: bool = False,
    ) -> CatalogProjectionPublicationReceipt | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                if build_id is not None:
                    row = self._select_receipt(connector, build_id)
                else:
                    state_clause = (
                        "WHERE state = 'DB_COMMITTED'" if pending_only else ""
                    )
                    row = connector.fetch_one(f"""
                        SELECT
                            build_id, source_revision, catalog_revision,
                            projection_sha256, state, new_galleries,
                            changed_galleries, removed_galleries,
                            duplicate_losers, selected_galleries,
                            committed_at, finalized_at
                        FROM catalog_projection_publication_receipts
                        {state_clause}
                        ORDER BY committed_at DESC, build_id DESC
                        LIMIT 1
                        """)
        return None if not row else self._receipt_from_row(row)

    def page_published_artifacts(
        self,
        build_id: str,
        *,
        after: CatalogProjectionArtifactCursor | None,
        limit: int,
    ) -> CatalogProjectionArtifactPage:
        self._validate_page_limit(limit)
        after_clause = ""
        parameters: tuple[object, ...]
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                receipt_row = self._select_receipt(connector, build_id)
                if receipt_row is None:
                    raise LookupError(f"Catalog build {build_id!r} is not published")
                receipt = self._receipt_from_row(receipt_row)
                parameters = (receipt.catalog_revision.revision,)
                if after is not None:
                    after_clause = "AND artifact.artifact_key > %s"
                    parameters += (after.artifact_key,)
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        artifact.artifact_key,
                        publication.source_gallery_name,
                        publication.gid,
                        publication.published_at,
                        artifact.artifact_id,
                        artifact.name,
                        artifact.location,
                        artifact.media_type,
                        artifact.size_bytes,
                        artifact.sha256,
                        artifact.modified_at
                    FROM catalog_artifacts AS artifact
                    JOIN catalog_publications AS publication
                        ON publication.revision = artifact.revision
                        AND publication.publication_key = artifact.publication_key
                    WHERE artifact.revision = %s {after_clause}
                    ORDER BY artifact.artifact_key
                    LIMIT %s
                    """,
                    (*parameters, limit),
                )
        items = tuple(
            CatalogPublishedArtifact(
                artifact=CatalogArtifact(
                    artifact_id=str(row[4]),
                    name=str(row[5]),
                    location=Path(str(row[6])),
                    media_type=str(row[7]),
                    size_bytes=int(row[8]),
                    sha256=str(row[9]),
                    modified_at=_parse_datetime(row[10]),
                ),
                gallery_name=str(row[1]),
                gid=int(row[2]),
                upload_time=_parse_datetime(row[3]),
            )
            for row in rows
        )
        next_cursor = (
            CatalogProjectionArtifactCursor(str(rows[-1][0])) if rows else None
        )
        return CatalogProjectionArtifactPage(
            receipt.catalog_revision,
            items,
            next_cursor,
            limit,
        )

    def acknowledge_finalized(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        catalog_revision: int,
        turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt:
        build = self._builds._require_build(connector, build_id, for_update=True)
        if build.phase is not CatalogBuildPhase.published:
            raise CatalogProjectionStateError(
                "Only a published source build can finalize a projection receipt"
            )
        row = self._select_receipt(connector, build_id, for_update=True)
        if row is None:
            raise CatalogProjectionStateError(
                "Projection publication receipt is missing"
            )
        receipt = self._receipt_from_row(row)
        if receipt.catalog_revision.revision != catalog_revision:
            raise CatalogProjectionStateError(
                "Projection finalization targets a different catalog revision"
            )
        projection = self._require_projection(connector, build_id, for_update=True)
        if (
            projection.phase is not CatalogBuildProjectionPhase.published
            or projection.published_catalog_revision != catalog_revision
        ):
            raise CatalogProjectionStateError(
                "Projection receipt does not match its published descriptor"
            )
        if receipt.state is CatalogProjectionPublicationState.projection_finalized:
            return receipt
        source_pointer = connector.fetch_one("""
            SELECT current_revision, active_build_id
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + self._lock_clause(self._context.sql_type))
        catalog_pointer = connector.fetch_one("""
            SELECT current_revision
            FROM catalog_revision
            WHERE singleton_id = 1
            """ + self._lock_clause(self._context.sql_type))
        if (
            not source_pointer
            or not catalog_pointer
            or source_pointer[1] is None
            or int(source_pointer[0]) != receipt.source_revision
            or str(source_pointer[1]) != build_id
            or int(catalog_pointer[0]) != catalog_revision
        ):
            raise CatalogProjectionStateError(
                "Projection receipt no longer matches the active published pointers"
            )
        now = self._builds._database_datetime(connector).isoformat()
        connector.execute(
            """
            UPDATE catalog_projection_publication_receipts
            SET state = %s, finalized_at = %s
            WHERE build_id = %s AND state = %s
            """,
            (
                CatalogProjectionPublicationState.projection_finalized.value,
                now,
                build_id,
                CatalogProjectionPublicationState.database_committed.value,
            ),
        )
        updated = self._select_receipt(connector, build_id)
        assert updated is not None
        return self._receipt_from_row(updated)

    def prune_projection(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        max_rows: int,
    ) -> CatalogBuildProjectionPruneResult:
        """Boundedly delete an unpublished candidate revision child-first."""

        if not 1 <= max_rows <= 10_000:
            raise ValueError("max_rows must be between 1 and 10000")
        projection_row = self._select_projection(
            connector,
            build_id,
            for_update=True,
        )
        if projection_row is None:
            return CatalogBuildProjectionPruneResult(build_id, 0, True)
        descriptor = self._projection_from_row(projection_row)
        build = self._builds._require_build(
            connector,
            build_id,
            for_update=True,
        )
        receipt_row = self._select_receipt(connector, build_id)
        reused_published = (
            build.phase is CatalogBuildPhase.published
            and descriptor.published_catalog_revision is not None
            and descriptor.published_catalog_revision != descriptor.reserved_revision
            and receipt_row is not None
            and self._receipt_from_row(receipt_row).state
            is CatalogProjectionPublicationState.projection_finalized
        )
        pointer = connector.fetch_one("""
            SELECT active_build_id
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + self._lock_clause(self._context.sql_type))
        if not pointer:
            raise RuntimeError("catalog_source_revision singleton is missing")
        active_build_id = None if pointer[0] is None else str(pointer[0])
        finalized_published = (
            build.phase is CatalogBuildPhase.published
            and descriptor.phase is CatalogBuildProjectionPhase.published
            and receipt_row is not None
            and self._receipt_from_row(receipt_row).state
            is CatalogProjectionPublicationState.projection_finalized
        )
        cleanup_activation = connector.fetch_one(
            """
            SELECT 1
            FROM catalog_operational_activations
            WHERE build_id = %s
            LIMIT 1
            """,
            (build_id,),
        )
        cleanup_in_progress = (
            build.phase is CatalogBuildPhase.published
            and descriptor.phase is CatalogBuildProjectionPhase.published
            and receipt_row is None
            and cleanup_activation is not None
            and active_build_id != build_id
        )
        inactive_finalized = (
            finalized_published and active_build_id != build_id
        ) or cleanup_in_progress
        if (
            build.phase is not CatalogBuildPhase.abandoned
            and not reused_published
            and not inactive_finalized
        ):
            raise CatalogProjectionStateError(
                "Only an abandoned candidate, a finalized reused candidate, "
                "or an inactive finalized publication can be projection-pruned"
            )
        if (
            descriptor.phase is CatalogBuildProjectionPhase.published
            and not reused_published
            and not inactive_finalized
        ):
            raise CatalogProjectionStateError(
                "A published catalog projection cannot be candidate-pruned"
            )
        revision = descriptor.reserved_revision
        revision_is_published = bool(
            connector.fetch_one(
                "SELECT 1 FROM catalog_revision_history WHERE revision = %s",
                (revision,),
            )
        )
        if revision_is_published and not inactive_finalized:
            raise CatalogProjectionStateError(
                "A catalog revision referenced by history cannot be candidate-pruned"
            )
        candidate_targets: tuple[
            tuple[str, tuple[str, ...], str, object],
            ...,
        ] = (
            ("catalog_artifacts", ("artifact_key",), "revision", revision),
            (
                "catalog_contributors",
                ("publication_key", "position"),
                "revision",
                revision,
            ),
            (
                "catalog_subjects",
                ("publication_key", "position"),
                "revision",
                revision,
            ),
            ("catalog_publications", ("publication_key",), "revision", revision),
        )
        build_targets: tuple[
            tuple[str, tuple[str, ...], str, object],
            ...,
        ] = (
            (
                "catalog_build_projection_items",
                ("gallery_key",),
                "build_id",
                build_id,
            ),
            (
                "catalog_build_projection_batches",
                ("batch_kind", "batch_id"),
                "build_id",
                build_id,
            ),
            (
                "catalog_build_prepared_artifacts",
                ("gallery_key",),
                "build_id",
                build_id,
            ),
        )
        # A historical published catalog revision is immutable and retained.
        # Only an unused reserved candidate may delete revision-keyed rows.
        targets = (() if revision_is_published else candidate_targets) + build_targets
        deleted = 0
        for table_name, key_columns, where_column, where_value in targets:
            remaining = max_rows - deleted
            if remaining <= 0:
                break
            rows = connector.fetch_all(
                f"""
                SELECT {', '.join(key_columns)}
                FROM {table_name}
                WHERE {where_column} = %s
                ORDER BY {', '.join(key_columns)}
                LIMIT %s
                """,
                (where_value, remaining),
            )
            if not rows:
                continue
            predicates = " AND ".join(f"{column} = %s" for column in key_columns)
            connector.execute_many(
                f"""
                DELETE FROM {table_name}
                WHERE {where_column} = %s AND {predicates}
                """,
                [(where_value, *row) for row in rows],
            )
            deleted += len(rows)
        candidate_remaining = any(
            connector.fetch_one(
                f"SELECT 1 FROM {table_name} " f"WHERE {where_column} = %s LIMIT 1",
                (where_value,),
            )
            for table_name, _keys, where_column, where_value in targets
        )
        if candidate_remaining or deleted >= max_rows:
            return CatalogBuildProjectionPruneResult(build_id, deleted, False)
        if reused_published and not inactive_finalized:
            return CatalogBuildProjectionPruneResult(build_id, deleted, True)
        if inactive_finalized:
            if deleted >= max_rows:
                return CatalogBuildProjectionPruneResult(build_id, deleted, False)
            if receipt_row is not None:
                connector.execute(
                    """
                    DELETE FROM catalog_projection_publication_receipts
                    WHERE build_id = %s AND state = %s
                    """,
                    (
                        build_id,
                        CatalogProjectionPublicationState.projection_finalized.value,
                    ),
                )
                deleted += 1
            if deleted >= max_rows:
                return CatalogBuildProjectionPruneResult(build_id, deleted, False)
        connector.execute(
            "DELETE FROM catalog_build_projections WHERE build_id = %s",
            (build_id,),
        )
        deleted += 1
        complete = True
        return CatalogBuildProjectionPruneResult(build_id, deleted, complete)

    @staticmethod
    def _existing_batch(
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
        batch_id: str,
    ) -> tuple[Any, ...] | None:
        row = connector.fetch_one(
            """
            SELECT payload_sha256, item_count
            FROM catalog_build_projection_batches
            WHERE build_id = %s AND batch_kind = %s AND batch_id = %s
            """,
            (build_id, batch_kind, batch_id),
        )
        return row or None

    @staticmethod
    def _record_batch(
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
        batch_id: str,
        payload_sha256: str,
        item_count: int,
    ) -> None:
        connector.execute(
            """
            INSERT INTO catalog_build_projection_batches (
                build_id, batch_kind, batch_id, payload_sha256, item_count
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (build_id, batch_kind, batch_id, payload_sha256, item_count),
        )

    @staticmethod
    def _replayed_batch(
        build_id: str,
        batch_id: str,
        payload_sha256: str,
        previous: tuple[Any, ...],
    ) -> CatalogBuildProjectionBatchResult:
        if str(previous[0]) != payload_sha256:
            raise CatalogProjectionBatchConflictError(
                f"Projection batch {batch_id!r} was retried with different data"
            )
        return CatalogBuildProjectionBatchResult(
            build_id,
            batch_id,
            False,
            int(previous[1]),
        )

    def _select_projection(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool,
    ) -> tuple[Any, ...] | None:
        lock = self._lock_clause(self._context.sql_type) if for_update else ""
        row = connector.fetch_one(
            """
            SELECT
                build_id,
                reserved_revision,
                base_catalog_revision,
                artifacts_required,
                phase,
                artifact_after_gallery_key,
                selection_after_gallery_key,
                selected_gallery_count,
                protected_artifact_count,
                staged_selection_count,
                projection_chain_sha256,
                projection_xor_sha256,
                projection_sum_sha256,
                projection_sha256,
                new_galleries,
                changed_galleries,
                removed_galleries,
                duplicate_losers,
                published_catalog_revision,
                created_at,
                updated_at
            FROM catalog_build_projections
            WHERE build_id = %s
            """ + lock,
            (build_id,),
        )
        return row or None

    def _require_projection(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool,
    ) -> CatalogBuildProjection:
        row = self._select_projection(
            connector,
            build_id,
            for_update=for_update,
        )
        if row is None:
            raise LookupError(f"Catalog build {build_id!r} has no projection")
        return self._projection_from_row(row)

    @staticmethod
    def _projection_from_row(row: tuple[Any, ...]) -> CatalogBuildProjection:
        return CatalogBuildProjection(
            build_id=str(row[0]),
            reserved_revision=int(row[1]),
            base_catalog_revision=int(row[2]),
            artifacts_required=bool(row[3]),
            phase=CatalogBuildProjectionPhase(str(row[4])),
            artifact_after_gallery_key=None if row[5] is None else str(row[5]),
            selection_after_gallery_key=None if row[6] is None else str(row[6]),
            selected_gallery_count=int(row[7]),
            protected_artifact_count=int(row[8]),
            staged_selection_count=int(row[9]),
            projection_chain_sha256=str(row[10]),
            projection_xor_sha256=str(row[11]),
            projection_sum_sha256=str(row[12]),
            projection_sha256=None if row[13] is None else str(row[13]),
            new_galleries=int(row[14]),
            changed_galleries=int(row[15]),
            removed_galleries=int(row[16]),
            duplicate_losers=int(row[17]),
            published_catalog_revision=None if row[18] is None else int(row[18]),
            created_at=_parse_datetime(row[19]),
            updated_at=_parse_datetime(row[20]),
        )

    def _select_receipt(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool = False,
    ) -> tuple[Any, ...] | None:
        lock = self._lock_clause(self._context.sql_type) if for_update else ""
        row = connector.fetch_one(
            """
            SELECT
                build_id, source_revision, catalog_revision,
                projection_sha256, state, new_galleries,
                changed_galleries, removed_galleries, duplicate_losers,
                selected_galleries, committed_at, finalized_at
            FROM catalog_projection_publication_receipts
            WHERE build_id = %s
            """ + lock,
            (build_id,),
        )
        return row or None

    @staticmethod
    def _receipt_from_row(row: tuple[Any, ...]) -> CatalogProjectionPublicationReceipt:
        committed_at = _parse_datetime(row[10])
        return CatalogProjectionPublicationReceipt(
            build_id=str(row[0]),
            source_revision=int(row[1]),
            catalog_revision=CatalogRevision(
                revision=int(row[2]),
                published_at=committed_at,
                publication_count=int(row[9]),
            ),
            projection_sha256=str(row[3]),
            state=CatalogProjectionPublicationState(str(row[4])),
            new_galleries=int(row[5]),
            changed_galleries=int(row[6]),
            removed_galleries=int(row[7]),
            duplicate_losers=int(row[8]),
            selected_galleries=int(row[9]),
            committed_at=committed_at,
            finalized_at=None if row[11] is None else _parse_datetime(row[11]),
        )
