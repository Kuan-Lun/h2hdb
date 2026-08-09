from __future__ import annotations

__all__ = [
    "CatalogBuildAlreadyActiveError",
    "CatalogBuildBatchConflictError",
    "CatalogBuildNotFoundError",
    "CatalogBuildRepository",
    "CatalogBuildStateError",
]

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import uuid4

from .domain import (
    CatalogBuild,
    CatalogBuildBatchResult,
    CatalogBuildPhase,
    CatalogBuildPruneResult,
    CatalogBuildPublishResult,
    CatalogBuildSourcePage,
    CatalogGalleryStageProgress,
    CatalogPendingGalleryPage,
    CatalogSourceDiscoveryCompletion,
    CatalogSourceFileChunk,
    CatalogSourceFileCursor,
    CatalogSourceFilePage,
    CatalogSourceGalleryAnalysis,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryDiscovery,
    CatalogSourceGalleryHeader,
    CatalogSourceGalleryRecord,
    CatalogSourcePage,
    CatalogSourceRevision,
    FileHashCacheEntry,
    FileHashCacheKey,
    GallerySourceFile,
    GalleryTag,
)
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector
from .table_gallery_ingest_coordination import GalleryIngestTurn

MAX_SOURCE_PAGE_SIZE = 200
LOOKUP_CHUNK_SIZE = 400


class CatalogBuildNotFoundError(LookupError):
    def __init__(self, build_id: str) -> None:
        self.build_id = build_id
        super().__init__(f"Catalog build {build_id!r} does not exist")


class CatalogBuildAlreadyActiveError(RuntimeError):
    pass


class CatalogBuildStateError(RuntimeError):
    pass


class CatalogBuildBatchConflictError(RuntimeError):
    pass


def _stable_key(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _joined_stable_key(*values: str) -> str:
    return _stable_key("\0".join(values))


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _serialize_datetime(value: datetime) -> str:
    aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value
    return aware.isoformat()


def _serialize_database_datetime(value: datetime) -> str:
    aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return aware.strftime("%Y-%m-%d %H:%M:%S")


def _payload_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=False,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def _file_payload(source_file: GallerySourceFile) -> object:
    return (
        source_file.name,
        source_file.size_bytes,
        source_file.sha256,
        source_file.relative_locator,
        source_file.device,
        source_file.inode,
        source_file.modified_ns,
        source_file.changed_ns,
    )


def _header_payload(header: CatalogSourceGalleryHeader) -> object:
    return (
        header.gallery_name,
        header.gid,
        header.title,
        header.comment,
        header.upload_account,
        _serialize_datetime(header.upload_time),
        _serialize_datetime(header.download_time),
        _serialize_datetime(header.modified_time),
        tuple((tag.name, tag.value) for tag in header.tags),
    )


def _completion_payload(completion: CatalogSourceGalleryCompletion) -> object:
    return (
        completion.gallery_name,
        completion.expected_file_count,
        completion.scan_observation_sha256,
        completion.scan_observation_version,
        completion.raw_content_sha256,
        completion.metadata_sha256,
        completion.page_count,
        completion.directory_entry_count,
        completion.directory_observation_sha256,
    )


class CatalogBuildRepository(BaseRepository):
    """Durable, incrementally staged immutable catalog source snapshots.

    Activation only publishes the source revision. It deliberately does not
    imply that the legacy/user-facing publication projection was prepared.
    """

    def __init__(self, context: RepositoryContext) -> None:
        super().__init__(context)

    def _database_datetime(self, connector: SQLConnector) -> datetime:
        match self._context.sql_type:
            case "mariadb":
                row = connector.fetch_one("SELECT UNIX_TIMESTAMP()")
            case "sqlite":
                row = connector.fetch_one("SELECT unixepoch()")
            case _:
                raise AssertionError(f"Unsupported SQL type: {self._context.sql_type}")
        if not row:
            raise RuntimeError("The database did not return its current time")
        return datetime.fromtimestamp(int(row[0]), UTC)

    @staticmethod
    def _lock_clause(sql_type: str) -> str:
        return " FOR UPDATE" if sql_type == "mariadb" else ""

    def _begin_with_connector(
        self,
        connector: SQLConnector,
        turn: GalleryIngestTurn,
        scope_key: str,
    ) -> CatalogBuild:
        if not scope_key:
            raise ValueError("scope_key must not be blank")
        lock = self._lock_clause(self._context.sql_type)
        control = connector.fetch_one("""
            SELECT working_build_id
            FROM catalog_build_control
            WHERE singleton_id = 1
            """ + lock)
        if not control:
            raise RuntimeError("catalog_build_control singleton is missing")
        if control[0] is not None:
            raise CatalogBuildAlreadyActiveError(
                f"Catalog build {control[0]!s} is unfinished; resume it instead"
            )
        source_pointer = connector.fetch_one("""
            SELECT current_revision, active_build_id
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + lock)
        if not source_pointer:
            raise RuntimeError("catalog_source_revision singleton is missing")

        build_id = uuid4().hex
        now = self._database_datetime(connector)
        connector.execute(
            """
            INSERT INTO catalog_builds (
                build_id,
                scope_key,
                discovery_epoch,
                discovery_tree_sha256,
                phase,
                ingest_generation,
                owner_token,
                base_source_revision,
                base_active_build_id,
                discovered_gallery_count,
                expected_gallery_count,
                staged_gallery_count,
                staged_file_count,
                analyzed_gallery_count,
                created_at,
                updated_at,
                published_source_revision,
                seal_sha256
            ) VALUES (
                %s, %s, %s, NULL, %s, %s, %s, %s, %s, 0, NULL, 0, 0, 0,
                %s, %s, NULL, NULL
            )
            """,
            (
                build_id,
                scope_key,
                build_id,
                CatalogBuildPhase.discovering.value,
                turn.generation,
                turn.owner_token,
                int(source_pointer[0]),
                None if source_pointer[1] is None else str(source_pointer[1]),
                now.isoformat(),
                now.isoformat(),
            ),
        )
        connector.execute(
            """
            UPDATE catalog_build_control
            SET working_build_id = %s
            WHERE singleton_id = 1
            """,
            (build_id,),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _resume_with_connector(
        self,
        connector: SQLConnector,
        turn: GalleryIngestTurn,
        scope_key: str,
    ) -> CatalogBuild | None:
        if not scope_key:
            raise ValueError("scope_key must not be blank")
        lock = self._lock_clause(self._context.sql_type)
        row = connector.fetch_one("""
            SELECT working_build_id
            FROM catalog_build_control
            WHERE singleton_id = 1
            """ + lock)
        if not row:
            raise RuntimeError("catalog_build_control singleton is missing")
        if row[0] is None:
            return None
        build_id = str(row[0])
        build = self._require_build(connector, build_id, for_update=True)
        if build.scope_key != scope_key:
            raise CatalogBuildStateError(
                "Unfinished catalog build belongs to a different source scope"
            )
        if build.phase is CatalogBuildPhase.published:
            raise CatalogBuildStateError(
                "Published catalog build is incorrectly retained as the working build"
            )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET ingest_generation = %s,
                owner_token = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (turn.generation, turn.owner_token, now.isoformat(), build_id),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _discover_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        discoveries: Sequence[CatalogSourceGalleryDiscovery],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(discoveries)
        names = tuple(value.gallery_name for value in values)
        if len(names) != len(set(names)):
            raise CatalogBuildStateError(
                "A discovery batch contains duplicate gallery names"
            )
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.discovering)
        payload_sha256 = _payload_sha256(
            tuple(
                (
                    value.gallery_name,
                    value.source_locator,
                    value.metadata_fingerprint,
                )
                for value in values
            )
        )
        previous = self._existing_batch(
            connector,
            build_id,
            "DISCOVERY",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )

        keyed_values = tuple(
            (_stable_key(value.gallery_name), value) for value in values
        )
        existing_rows: list[tuple[Any, ...]] = []
        keys = tuple(key for key, _value in keyed_values)
        for start in range(0, len(keys), LOOKUP_CHUNK_SIZE):
            chunk = keys[start : start + LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join("%s" for _ in chunk)
            existing_rows.extend(
                connector.fetch_all(
                    f"""
                    SELECT
                        gallery_key,
                        gallery_name,
                        source_locator,
                        metadata_fingerprint
                    FROM catalog_build_discoveries
                    WHERE build_id = %s AND gallery_key IN ({placeholders})
                    """,
                    (build_id, *chunk),
                )
            )
        existing = {
            str(key): (
                str(name),
                str(locator),
                None if fingerprint is None else str(fingerprint),
            )
            for key, name, locator, fingerprint in existing_rows
        }
        requested = {
            key: (
                value.gallery_name,
                value.source_locator,
                value.metadata_fingerprint,
            )
            for key, value in keyed_values
        }
        for key, persisted in existing.items():
            if requested.get(key) != persisted:
                raise CatalogBuildBatchConflictError(
                    "A gallery discovery key was retried with a different source"
                )
        missing_names = tuple(
            (key, value) for key, value in keyed_values if key not in existing
        )
        if missing_names:
            connector.execute_many(
                """
                INSERT INTO catalog_build_discoveries (
                    build_id,
                    gallery_key,
                    gallery_name,
                    source_locator,
                    metadata_fingerprint
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (
                        build_id,
                        key,
                        value.gallery_name,
                        value.source_locator,
                        value.metadata_fingerprint,
                    )
                    for key, value in missing_names
                ],
            )
        self._record_batch(
            connector,
            build_id,
            "DISCOVERY",
            batch_id,
            payload_sha256,
            len(missing_names),
            0,
        )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET discovered_gallery_count = discovered_gallery_count + %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (len(missing_names), now.isoformat(), build_id),
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            len(missing_names),
        )

    def _complete_discovery_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
        completion: CatalogSourceDiscoveryCompletion | None = None,
    ) -> CatalogBuild:
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase is CatalogBuildPhase.staging:
            if completion is not None and (
                completion.scan_attempt != build.discovery_epoch
                or completion.gallery_count != build.expected_gallery_count
                or completion.tree_observation_sha256 != build.discovery_tree_sha256
            ):
                raise CatalogBuildStateError(
                    "Catalog discovery was already completed with different data"
                )
            return build
        self._require_phase(build, CatalogBuildPhase.discovering)
        actual_count = self._count(connector, "catalog_build_discoveries", build_id)
        batch_count, _ = self._batch_totals(connector, build_id, "DISCOVERY")
        if (
            actual_count != build.discovered_gallery_count
            or batch_count != actual_count
        ):
            raise CatalogBuildStateError(
                "Catalog discovery counters do not match durable discovery rows"
            )
        if completion is not None:
            if completion.scan_attempt != build.discovery_epoch:
                raise CatalogBuildStateError(
                    "Catalog discovery completion belongs to a different scan attempt"
                )
            if completion.gallery_count != actual_count:
                raise CatalogBuildStateError(
                    "Catalog discovery completion count does not match durable rows"
                )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s,
                expected_gallery_count = %s,
                discovery_tree_sha256 = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildPhase.staging.value,
                actual_count,
                (None if completion is None else completion.tree_observation_sha256),
                now.isoformat(),
                build_id,
            ),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _begin_gallery_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        header: CatalogSourceGalleryHeader,
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(_header_payload(header))
        previous = self._existing_batch(connector, build_id, "HEADER", batch_id)
        if previous is not None:
            return self._replayed_batch_result(
                build_id, batch_id, payload_sha256, previous
            )
        keyed_name = ((_stable_key(header.gallery_name), header.gallery_name),)
        self._ensure_discovered(connector, build_id, keyed_name)
        gallery_key = keyed_name[0][0]
        existing_header = connector.fetch_one(
            """
            SELECT
                gallery_name,
                gid,
                title,
                comment,
                upload_account,
                upload_time,
                download_time,
                modified_time
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key = %s
            """,
            (build_id, gallery_key),
        )
        if existing_header:
            expected_header = (
                header.gallery_name,
                header.gid,
                header.title,
                header.comment,
                header.upload_account,
                _serialize_datetime(header.upload_time),
                _serialize_datetime(header.download_time),
                _serialize_datetime(header.modified_time),
            )
            tag_rows = connector.fetch_all(
                """
                SELECT tag_name, tag_value
                FROM catalog_source_tags
                WHERE build_id = %s AND gallery_key = %s
                ORDER BY position
                """,
                (build_id, gallery_key),
            )
            persisted_tags = tuple((str(name), str(value)) for name, value in tag_rows)
            expected_tags = tuple((tag.name, tag.value) for tag in header.tags)
            normalized_existing = (
                str(existing_header[0]),
                int(existing_header[1]),
                str(existing_header[2]),
                str(existing_header[3]),
                str(existing_header[4]),
                _serialize_datetime(_parse_datetime(existing_header[5])),
                _serialize_datetime(_parse_datetime(existing_header[6])),
                _serialize_datetime(_parse_datetime(existing_header[7])),
            )
            if (
                normalized_existing != expected_header
                or persisted_tags != expected_tags
            ):
                raise CatalogBuildBatchConflictError(
                    "Gallery header was retried with different source metadata"
                )
            self._record_batch(
                connector,
                build_id,
                "HEADER",
                batch_id,
                payload_sha256,
                0,
                0,
            )
            return CatalogBuildBatchResult(build_id, batch_id, True, 0)
        connector.execute(
            """
            INSERT INTO catalog_source_galleries (
                build_id,
                gallery_key,
                gallery_name,
                gid,
                title,
                comment,
                upload_account,
                upload_time,
                download_time,
                modified_time,
                upload_time_utc,
                download_time_utc
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                build_id,
                gallery_key,
                header.gallery_name,
                header.gid,
                header.title,
                header.comment,
                header.upload_account,
                _serialize_datetime(header.upload_time),
                _serialize_datetime(header.download_time),
                _serialize_datetime(header.modified_time),
                _serialize_database_datetime(header.upload_time),
                _serialize_database_datetime(header.download_time),
            ),
        )
        if header.tags:
            connector.execute_many(
                """
                INSERT INTO catalog_source_tags (
                    build_id, gallery_key, position, tag_name, tag_value
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (build_id, gallery_key, position, tag.name, tag.value)
                    for position, tag in enumerate(header.tags)
                ],
            )
        self._record_batch(
            connector,
            build_id,
            "HEADER",
            batch_id,
            payload_sha256,
            1,
            0,
        )
        return CatalogBuildBatchResult(build_id, batch_id, True, 1)

    def _stage_gallery_headers_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        headers: Sequence[CatalogSourceGalleryHeader],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        """Stage a bounded scanner header batch in one fenced transaction."""

        self._validate_batch_id(batch_id)
        values = tuple(headers)
        names = tuple(value.gallery_name for value in values)
        if len(names) != len(set(names)):
            raise ValueError("A gallery header batch contains duplicate gallery names")
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(
            tuple(_header_payload(value) for value in values)
        )
        previous = self._existing_batch(
            connector,
            build_id,
            "HEADER_GROUP",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )
        item_count = 0
        for index, header in enumerate(values):
            result = self._begin_gallery_with_connector(
                connector,
                build_id,
                header,
                batch_id=_joined_stable_key(
                    "HEADER_GROUP",
                    batch_id,
                    str(index),
                    header.gallery_name,
                ),
                turn=turn,
            )
            item_count += result.item_count
        self._record_batch(
            connector,
            build_id,
            "HEADER_GROUP",
            batch_id,
            payload_sha256,
            item_count,
            0,
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            item_count,
        )

    def _stage_file_chunk_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        gallery_name: str,
        files: Sequence[GallerySourceFile],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(files)
        names = tuple(source_file.name for source_file in values)
        if len(names) != len(set(names)):
            raise ValueError("A source file chunk contains duplicate file names")
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(
            (
                gallery_name,
                tuple(_file_payload(item) for item in values),
            )
        )
        previous = self._existing_batch(connector, build_id, "FILES", batch_id)
        if previous is not None:
            return self._replayed_batch_result(
                build_id, batch_id, payload_sha256, previous
            )
        gallery_key = _stable_key(gallery_name)
        row = connector.fetch_one(
            """
            SELECT gallery_name, source_complete
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key = %s
            """,
            (build_id, gallery_key),
        )
        if not row or str(row[0]) != gallery_name:
            raise CatalogBuildStateError(
                "Source file chunk references a gallery header that is not staged"
            )
        if bool(row[1]):
            raise CatalogBuildStateError(
                "Cannot append source files after gallery completion"
            )
        keyed_files = tuple(
            (_stable_key(source_file.name), source_file) for source_file in values
        )
        file_keys = tuple(key for key, _source_file in keyed_files)
        existing_files: dict[
            str,
            tuple[
                str,
                int,
                str,
                str | None,
                int | None,
                int | None,
                int | None,
                int | None,
            ],
        ] = {}
        for start in range(0, len(file_keys), LOOKUP_CHUNK_SIZE):
            chunk = file_keys[start : start + LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join("%s" for _ in chunk)
            existing_rows = connector.fetch_all(
                f"""
                SELECT
                    file_key,
                    file_name,
                    size_bytes,
                    sha256,
                    relative_locator,
                    device,
                    inode,
                    modified_ns,
                    changed_ns
                FROM catalog_source_files
                WHERE build_id = %s
                    AND gallery_key = %s
                    AND file_key IN ({placeholders})
                """,
                (build_id, gallery_key, *chunk),
            )
            existing_files.update(
                {
                    str(key): (
                        str(name),
                        int(size_bytes),
                        str(digest),
                        None if locator is None else str(locator),
                        None if device is None else int(device),
                        None if inode is None else int(inode),
                        None if modified_ns is None else int(modified_ns),
                        None if changed_ns is None else int(changed_ns),
                    )
                    for (
                        key,
                        name,
                        size_bytes,
                        digest,
                        locator,
                        device,
                        inode,
                        modified_ns,
                        changed_ns,
                    ) in existing_rows
                }
            )
        missing_files: list[tuple[str, GallerySourceFile]] = []
        for file_key, source_file in keyed_files:
            previous_file = existing_files.get(file_key)
            expected_file = (
                source_file.name,
                source_file.size_bytes,
                source_file.sha256,
                source_file.relative_locator,
                source_file.device,
                source_file.inode,
                source_file.modified_ns,
                source_file.changed_ns,
            )
            if previous_file is None:
                missing_files.append((file_key, source_file))
            elif previous_file != expected_file:
                raise CatalogBuildBatchConflictError(
                    "A source file identity was retried with different metadata"
                )
        if missing_files:
            connector.execute_many(
                """
                INSERT INTO catalog_source_files (
                    build_id,
                    gallery_key,
                    file_key,
                    file_sort_key,
                    file_name,
                    relative_locator,
                    device,
                    inode,
                    modified_ns,
                    changed_ns,
                    size_bytes,
                    sha256
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                [
                    (
                        build_id,
                        gallery_key,
                        file_key,
                        source_file.name.casefold(),
                        source_file.name,
                        source_file.relative_locator,
                        (
                            None
                            if source_file.device is None
                            else str(source_file.device)
                        ),
                        None if source_file.inode is None else str(source_file.inode),
                        (
                            None
                            if source_file.modified_ns is None
                            else str(source_file.modified_ns)
                        ),
                        (
                            None
                            if source_file.changed_ns is None
                            else str(source_file.changed_ns)
                        ),
                        source_file.size_bytes,
                        source_file.sha256,
                    )
                    for file_key, source_file in missing_files
                ],
            )
        self._record_batch(
            connector,
            build_id,
            "FILES",
            batch_id,
            payload_sha256,
            len(missing_files),
            len(missing_files),
        )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET staged_file_count = staged_file_count + %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (len(missing_files), now.isoformat(), build_id),
        )
        connector.execute(
            """
            UPDATE catalog_source_galleries
            SET staged_file_count = staged_file_count + %s
            WHERE build_id = %s AND gallery_key = %s
            """,
            (len(missing_files), build_id, gallery_key),
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            len(missing_files),
            len(missing_files),
        )

    def _stage_file_chunks_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        chunks: Sequence[CatalogSourceFileChunk],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        """Stage bounded chunks for multiple galleries in one transaction."""

        self._validate_batch_id(batch_id)
        values = tuple(chunks)
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(
            tuple(
                (
                    chunk.gallery_name,
                    tuple(_file_payload(source_file) for source_file in chunk.files),
                )
                for chunk in values
            )
        )
        previous = self._existing_batch(
            connector,
            build_id,
            "FILES_GROUP",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )
        item_count = 0
        file_count = 0
        for index, chunk in enumerate(values):
            result = self._stage_file_chunk_with_connector(
                connector,
                build_id,
                chunk.gallery_name,
                chunk.files,
                batch_id=_joined_stable_key(
                    "FILES_GROUP",
                    batch_id,
                    str(index),
                    chunk.gallery_name,
                ),
                turn=turn,
            )
            item_count += result.item_count
            file_count += result.file_count
        self._record_batch(
            connector,
            build_id,
            "FILES_GROUP",
            batch_id,
            payload_sha256,
            item_count,
            file_count,
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            item_count,
            file_count,
        )

    def _complete_gallery_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        completion: CatalogSourceGalleryCompletion,
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(_completion_payload(completion))
        previous = self._existing_batch(connector, build_id, "COMPLETE", batch_id)
        if previous is not None:
            return self._replayed_batch_result(
                build_id, batch_id, payload_sha256, previous
            )
        gallery_key = _stable_key(completion.gallery_name)
        row = connector.fetch_one(
            """
            SELECT
                gallery_name,
                source_complete,
                expected_file_count,
                staged_file_count,
                scan_observation_sha256,
                scan_observation_version,
                raw_content_sha256,
                metadata_sha256,
                page_count,
                directory_entry_count,
                directory_observation_sha256
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key = %s
            """,
            (build_id, gallery_key),
        )
        if not row or str(row[0]) != completion.gallery_name:
            raise CatalogBuildStateError(
                "Gallery completion references a header that is not staged"
            )
        if bool(row[1]):
            persisted = (
                int(row[2]),
                str(row[4]),
                int(row[5]),
                None if row[6] is None else str(row[6]),
                None if row[7] is None else str(row[7]),
                None if row[8] is None else int(row[8]),
                None if row[9] is None else int(row[9]),
                None if row[10] is None else str(row[10]),
            )
            requested = (
                completion.expected_file_count,
                completion.scan_observation_sha256,
                completion.scan_observation_version,
                completion.raw_content_sha256,
                completion.metadata_sha256,
                completion.page_count,
                completion.directory_entry_count,
                completion.directory_observation_sha256,
            )
            if persisted != requested:
                raise CatalogBuildBatchConflictError(
                    "Gallery was completed with different source data"
                )
            self._record_batch(
                connector,
                build_id,
                "COMPLETE",
                batch_id,
                payload_sha256,
                0,
                0,
            )
            return CatalogBuildBatchResult(build_id, batch_id, True, 0, 0)
        actual_count = int(row[3])
        if actual_count != completion.expected_file_count:
            raise CatalogBuildStateError(
                "Gallery source file chunks do not match the expected unique count"
            )
        connector.execute(
            """
            UPDATE catalog_source_galleries
            SET scan_observation_sha256 = %s,
                scan_observation_version = %s,
                raw_content_sha256 = %s,
                metadata_sha256 = %s,
                page_count = %s,
                directory_entry_count = %s,
                directory_observation_sha256 = %s,
                expected_file_count = %s,
                source_complete = 1
            WHERE build_id = %s AND gallery_key = %s
            """,
            (
                completion.scan_observation_sha256,
                completion.scan_observation_version,
                completion.raw_content_sha256,
                completion.metadata_sha256,
                completion.page_count,
                completion.directory_entry_count,
                completion.directory_observation_sha256,
                completion.expected_file_count,
                build_id,
                gallery_key,
            ),
        )
        self._record_batch(
            connector,
            build_id,
            "COMPLETE",
            batch_id,
            payload_sha256,
            1,
            actual_count,
        )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET staged_gallery_count = staged_gallery_count + 1,
                updated_at = %s
            WHERE build_id = %s
            """,
            (now.isoformat(), build_id),
        )
        return CatalogBuildBatchResult(build_id, batch_id, True, 1, actual_count)

    def _complete_galleries_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        completions: Sequence[CatalogSourceGalleryCompletion],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        """Complete multiple galleries in one bounded fenced transaction."""

        self._validate_batch_id(batch_id)
        values = tuple(completions)
        names = tuple(value.gallery_name for value in values)
        if len(names) != len(set(names)):
            raise ValueError("A gallery completion batch contains duplicate galleries")
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.staging)
        payload_sha256 = _payload_sha256(
            tuple(_completion_payload(value) for value in values)
        )
        previous = self._existing_batch(
            connector,
            build_id,
            "COMPLETE_GROUP",
            batch_id,
        )
        if previous is not None:
            return self._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )
        item_count = 0
        file_count = 0
        for index, completion in enumerate(values):
            result = self._complete_gallery_with_connector(
                connector,
                build_id,
                completion,
                batch_id=_joined_stable_key(
                    "COMPLETE_GROUP",
                    batch_id,
                    str(index),
                    completion.gallery_name,
                ),
                turn=turn,
            )
            item_count += result.item_count
            file_count += result.file_count
        self._record_batch(
            connector,
            build_id,
            "COMPLETE_GROUP",
            batch_id,
            payload_sha256,
            item_count,
            file_count,
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            item_count,
            file_count,
        )

    def _complete_source_staging_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase is CatalogBuildPhase.analyzing:
            return build
        self._require_phase(build, CatalogBuildPhase.staging)
        self._validate_raw_source_complete(connector, build)
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s, updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildPhase.analyzing.value,
                now.isoformat(),
                build_id,
            ),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _stage_analysis_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        analyses: Sequence[CatalogSourceGalleryAnalysis],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(analyses)
        names = tuple(value.gallery_name for value in values)
        if len(names) != len(set(names)):
            raise ValueError("An analysis batch contains duplicate gallery names")
        build = self._require_owned_build(connector, build_id, turn)
        self._require_phase(build, CatalogBuildPhase.analyzing)
        payload_sha256 = _payload_sha256(
            tuple(
                (
                    value.gallery_name,
                    value.content_sha256,
                    value.selected,
                    value.duplicate_of_gallery_name,
                    value.source_manifest_sha256,
                    value.source_manifest_version,
                )
                for value in values
            )
        )
        previous = self._existing_batch(connector, build_id, "ANALYSIS", batch_id)
        if previous is not None:
            return self._replayed_batch_result(
                build_id, batch_id, payload_sha256, previous
            )
        applied_values: list[CatalogSourceGalleryAnalysis] = []
        for value in values:
            gallery_key = _stable_key(value.gallery_name)
            row = connector.fetch_one(
                """
                SELECT
                    gallery_name,
                    analysis_complete,
                    content_sha256,
                    selected,
                    duplicate_of_gallery_name,
                    source_manifest_sha256,
                    source_manifest_version
                FROM catalog_source_galleries
                WHERE build_id = %s AND gallery_key = %s
                """,
                (build_id, gallery_key),
            )
            if not row or str(row[0]) != value.gallery_name:
                raise CatalogBuildStateError(
                    "Analysis references a gallery that is not staged"
                )
            if bool(row[1]):
                persisted = (
                    None if row[2] is None else str(row[2]),
                    bool(row[3]),
                    None if row[4] is None else str(row[4]),
                    None if row[5] is None else str(row[5]),
                    None if row[6] is None else int(row[6]),
                )
                requested = (
                    value.content_sha256,
                    value.selected,
                    value.duplicate_of_gallery_name,
                    value.source_manifest_sha256,
                    value.source_manifest_version,
                )
                if persisted != requested:
                    raise CatalogBuildBatchConflictError(
                        "Gallery was analyzed with different canonical data"
                    )
                continue
            connector.execute(
                """
                UPDATE catalog_source_galleries
                SET content_sha256 = %s,
                    selected = %s,
                    duplicate_of_gallery_name = %s,
                    duplicate_of_gallery_key = %s,
                    source_manifest_sha256 = %s,
                    source_manifest_version = %s,
                    analysis_complete = 1
                WHERE build_id = %s AND gallery_key = %s
                """,
                (
                    value.content_sha256,
                    value.selected,
                    value.duplicate_of_gallery_name,
                    (
                        None
                        if value.duplicate_of_gallery_name is None
                        else _stable_key(value.duplicate_of_gallery_name)
                    ),
                    value.source_manifest_sha256,
                    value.source_manifest_version,
                    build_id,
                    gallery_key,
                ),
            )
            applied_values.append(value)
        self._record_batch(
            connector,
            build_id,
            "ANALYSIS",
            batch_id,
            payload_sha256,
            len(applied_values),
            0,
        )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET analyzed_gallery_count = analyzed_gallery_count + %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (len(applied_values), now.isoformat(), build_id),
        )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            True,
            len(applied_values),
        )

    def _complete_analysis_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase is CatalogBuildPhase.artifacts:
            return build
        self._require_phase(build, CatalogBuildPhase.analyzing)
        staged_phase = connector.fetch_one(
            """
            SELECT phase
            FROM catalog_build_analysis_phases
            WHERE build_id = %s
            LIMIT 1
            """,
            (build_id,),
        )
        if staged_phase and not connector.fetch_one(
            """
            SELECT 1
            FROM catalog_build_analysis_phases
            WHERE build_id = %s AND phase = 'FINAL_ANALYSES'
            """,
            (build_id,),
        ):
            raise CatalogBuildStateError(
                "Durable catalog analysis has not completed FINAL_ANALYSES"
            )
        self._validate_complete_source(connector, build)
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s, updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildPhase.artifacts.value,
                now.isoformat(),
                build_id,
            ),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _cache_hashes_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        entries: Sequence[FileHashCacheEntry],
        *,
        batch_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildBatchResult:
        self._validate_batch_id(batch_id)
        values = tuple(entries)
        keys = tuple(entry.key for entry in values)
        if len(keys) != len(set(keys)):
            raise ValueError("A file hash cache batch contains duplicate keys")
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase not in {
            CatalogBuildPhase.discovering,
            CatalogBuildPhase.staging,
        }:
            raise CatalogBuildStateError(
                f"Cannot cache file hashes while build is {build.phase.value}"
            )
        payload_sha256 = _payload_sha256(
            tuple(
                (entry.key.source_key, entry.key.fingerprint, entry.sha256)
                for entry in values
            )
        )
        previous = self._existing_batch(connector, build_id, "HASH_CACHE", batch_id)
        if previous is not None:
            return self._replayed_batch_result(
                build_id,
                batch_id,
                payload_sha256,
                previous,
            )

        cache_ids = tuple(
            _joined_stable_key(entry.key.source_key, entry.key.fingerprint)
            for entry in values
        )
        existing = self._cache_rows_by_ids(connector, cache_ids)
        rows: list[tuple[Any, ...]] = []
        now = self._database_datetime(connector).isoformat()
        for cache_id, entry in zip(cache_ids, values, strict=True):
            previous_entry = existing.get(cache_id)
            identity = (
                entry.key.source_key,
                entry.key.fingerprint,
                entry.sha256,
            )
            if previous_entry is None:
                rows.append((cache_id, *identity, now))
            elif previous_entry != identity:
                raise CatalogBuildBatchConflictError(
                    "A file hash cache identity collided with different persisted data"
                )
        if rows:
            connector.execute_many(
                """
                INSERT INTO catalog_file_hash_cache (
                    cache_key,
                    source_key,
                    fingerprint,
                    sha256,
                    cached_at
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
        self._record_batch(
            connector,
            build_id,
            "HASH_CACHE",
            batch_id,
            payload_sha256,
            len(values),
            0,
        )
        return CatalogBuildBatchResult(build_id, batch_id, True, len(values))

    def get_file_hashes(
        self,
        keys: Sequence[FileHashCacheKey],
    ) -> Mapping[FileHashCacheKey, str]:
        ordered = tuple(dict.fromkeys(keys))
        if not ordered:
            return {}
        cache_ids = tuple(
            _joined_stable_key(key.source_key, key.fingerprint) for key in ordered
        )
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                existing = self._cache_rows_by_ids(connector, cache_ids)
        result: dict[FileHashCacheKey, str] = {}
        for cache_id, key in zip(cache_ids, ordered, strict=True):
            row = existing.get(cache_id)
            if row is None:
                continue
            if row[:2] != (key.source_key, key.fingerprint):
                raise CatalogBuildBatchConflictError(
                    "A file hash cache key collided with a different source identity"
                )
            result[key] = row[2]
        return result

    def _abandon_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase is CatalogBuildPhase.abandoned:
            return build
        if build.phase is CatalogBuildPhase.published:
            raise CatalogBuildStateError(
                "A published catalog build cannot be abandoned"
            )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s, updated_at = %s
            WHERE build_id = %s
            """,
            (CatalogBuildPhase.abandoned.value, now.isoformat(), build_id),
        )
        connector.execute(
            """
            UPDATE catalog_build_control
            SET working_build_id = NULL
            WHERE singleton_id = 1 AND working_build_id = %s
            """,
            (build_id,),
        )
        return self._require_build(connector, build_id, for_update=False)

    def prune_build(
        self,
        build_id: str,
        *,
        max_rows: int,
    ) -> CatalogBuildPruneResult:
        """Delete one inactive source build in bounded child-first batches.

        Old source revisions intentionally stop being readable once their
        retained build is pruned. The current active and working builds are
        always protected. Repeated calls continue from durable database state.
        """

        if not 1 <= max_rows <= 10_000:
            raise ValueError("max_rows must be between 1 and 10000")
        deleted = 0
        with self.SQLConnector() as connector:
            with connector.transaction():
                lock = self._lock_clause(self._context.sql_type)
                pointer = connector.fetch_one("""
                    SELECT active_build_id
                    FROM catalog_source_revision
                    WHERE singleton_id = 1
                    """ + lock)
                control = connector.fetch_one("""
                    SELECT working_build_id
                    FROM catalog_build_control
                    WHERE singleton_id = 1
                    """ + lock)
                if not pointer or not control:
                    raise RuntimeError("Catalog build pointer singletons are missing")
                protected = {
                    str(value)
                    for value in (pointer[0], control[0])
                    if value is not None
                }
                if build_id in protected:
                    raise CatalogBuildStateError(
                        "The active or working catalog build cannot be pruned"
                    )
                build_row = self._select_build(
                    connector,
                    build_id,
                    for_update=True,
                )
                if not build_row:
                    return CatalogBuildPruneResult(build_id, 0, True)
                build = self._build_from_row(build_row)
                if build.phase not in {
                    CatalogBuildPhase.published,
                    CatalogBuildPhase.abandoned,
                }:
                    raise CatalogBuildStateError(
                        "Only an inactive published or abandoned build can be pruned"
                    )
                if (
                    connector.fetch_one(
                        """
                    SELECT 1
                    FROM catalog_build_projections
                    WHERE build_id = %s
                    LIMIT 1
                    """,
                        (build_id,),
                    )
                    or connector.fetch_one(
                        """
                    SELECT 1
                    FROM catalog_projection_publication_receipts
                    WHERE build_id = %s
                    LIMIT 1
                    """,
                        (build_id,),
                    )
                ):
                    raise CatalogBuildStateError(
                        "Catalog projection cleanup must complete before source cleanup"
                    )

                deletion_specs = (
                    (
                        "catalog_build_excluded_file_hashes",
                        ("sha256",),
                    ),
                    (
                        "catalog_build_content_owners",
                        ("content_sha256",),
                    ),
                    (
                        "catalog_build_gid_winners",
                        ("gid",),
                    ),
                    (
                        "catalog_build_content_digests",
                        ("gallery_key",),
                    ),
                    (
                        "catalog_build_analysis_phases",
                        ("phase",),
                    ),
                    (
                        "catalog_build_operational_state",
                        ("preparation_id",),
                    ),
                    (
                        "catalog_source_files",
                        ("gallery_key", "file_key"),
                    ),
                    (
                        "catalog_source_tags",
                        ("gallery_key", "position"),
                    ),
                    (
                        "catalog_build_batches",
                        ("batch_kind", "batch_id"),
                    ),
                    ("catalog_build_discoveries", ("gallery_key",)),
                    ("catalog_source_galleries", ("gallery_key",)),
                )
                for table_name, key_columns in deletion_specs:
                    remaining = max_rows - deleted
                    if remaining <= 0:
                        break
                    rows = connector.fetch_all(
                        f"""
                        SELECT {", ".join(key_columns)}
                        FROM {table_name}
                        WHERE build_id = %s
                        ORDER BY {", ".join(key_columns)}
                        LIMIT %s
                        """,
                        (build_id, remaining),
                    )
                    if not rows:
                        continue
                    predicates = " AND ".join(
                        f"{column} = %s" for column in key_columns
                    )
                    connector.execute_many(
                        f"""
                        DELETE FROM {table_name}
                        WHERE build_id = %s AND {predicates}
                        """,
                        [(build_id, *row) for row in rows],
                    )
                    deleted += len(rows)
                    if deleted >= max_rows:
                        break

                if deleted < max_rows:
                    history = connector.fetch_one(
                        """
                        SELECT revision
                        FROM catalog_source_revision_history
                        WHERE build_id = %s
                        """,
                        (build_id,),
                    )
                    if history:
                        connector.execute(
                            """
                            DELETE FROM catalog_source_revision_history
                            WHERE revision = %s AND build_id = %s
                            """,
                            (int(history[0]), build_id),
                        )
                        deleted += 1
                if deleted < max_rows:
                    connector.execute(
                        "DELETE FROM catalog_builds WHERE build_id = %s",
                        (build_id,),
                    )
                    deleted += 1
                    complete = True
                else:
                    complete = False
        return CatalogBuildPruneResult(build_id, deleted, complete)

    def _seal_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        build = self._require_owned_build(connector, build_id, turn)
        if build.phase is CatalogBuildPhase.sealed:
            return build
        self._require_phase(build, CatalogBuildPhase.artifacts)
        gallery_count, file_count = self._validate_complete_source(connector, build)
        seal_sha256 = _payload_sha256(
            (
                build.build_id,
                build.scope_key,
                build.discovery_epoch,
                build.discovery_tree_sha256,
                build.base_source_revision,
                build.base_active_build_id,
                gallery_count,
                file_count,
                build.analyzed_gallery_count,
            )
        )
        now = self._database_datetime(connector)
        connector.execute(
            """
            UPDATE catalog_builds
            SET phase = %s,
                staged_gallery_count = %s,
                staged_file_count = %s,
                seal_sha256 = %s,
                updated_at = %s
            WHERE build_id = %s
            """,
            (
                CatalogBuildPhase.sealed.value,
                gallery_count,
                file_count,
                seal_sha256,
                now.isoformat(),
                build_id,
            ),
        )
        return self._require_build(connector, build_id, for_update=False)

    def _publish_with_connector(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildPublishResult:
        build = self._require_owned_build(connector, build_id, turn)
        projection = connector.fetch_one(
            """
            SELECT phase
            FROM catalog_build_projections
            WHERE build_id = %s
            """,
            (build_id,),
        )
        if projection and str(projection[0]) != "PUBLISHED":
            raise CatalogBuildStateError(
                "A staged catalog projection exists; publish both source and "
                "catalog pointers with publish_catalog_build_with_projection"
            )
        lock = self._lock_clause(self._context.sql_type)
        pointer = connector.fetch_one("""
            SELECT
                current_revision,
                active_build_id,
                gallery_count,
                file_count
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + lock)
        if not pointer:
            raise RuntimeError("catalog_source_revision singleton is missing")
        current_revision = int(pointer[0])
        active_build_id = None if pointer[1] is None else str(pointer[1])

        if build.phase is CatalogBuildPhase.published:
            if (
                build.published_source_revision is None
                or current_revision != build.published_source_revision
                or active_build_id != build.build_id
            ):
                raise CatalogBuildStateError(
                    "Published build result no longer matches the active source pointer"
                )
            return CatalogBuildPublishResult(
                build,
                build.published_source_revision,
                build.base_active_build_id,
            )

        self._require_phase(build, CatalogBuildPhase.sealed)
        control = connector.fetch_one("""
            SELECT working_build_id
            FROM catalog_build_control
            WHERE singleton_id = 1
            """ + lock)
        if not control or control[0] is None or str(control[0]) != build.build_id:
            raise CatalogBuildStateError(
                "Sealed catalog build is not the current working build"
            )
        if (
            current_revision != build.base_source_revision
            or active_build_id != build.base_active_build_id
        ):
            raise CatalogBuildStateError(
                "Active source revision changed after this catalog build began"
            )
        if (
            build.seal_sha256 is None
            or build.expected_gallery_count is None
            or build.staged_gallery_count != build.expected_gallery_count
            or build.analyzed_gallery_count != build.expected_gallery_count
        ):
            raise CatalogBuildStateError(
                "Sealed catalog build descriptor is incomplete"
            )
        gallery_count = build.staged_gallery_count
        file_count = build.staged_file_count
        next_revision = current_revision + 1
        now = self._database_datetime(connector)
        connector.execute(
            """
            INSERT INTO catalog_source_revision_history (
                revision,
                build_id,
                published_at,
                gallery_count,
                file_count
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                next_revision,
                build_id,
                now.isoformat(),
                gallery_count,
                file_count,
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
                next_revision,
                now.isoformat(),
                build_id,
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
                next_revision,
                build_id,
                now.isoformat(),
                gallery_count,
                file_count,
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
        published = self._require_build(connector, build_id, for_update=False)
        return CatalogBuildPublishResult(
            published,
            next_revision,
            build.base_active_build_id,
        )

    def get_build(self, build_id: str) -> CatalogBuild | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                row = self._select_build(connector, build_id, for_update=False)
        return None if not row else self._build_from_row(row)

    def get_working_build(self) -> CatalogBuild | None:
        """Inspect the unfinished build, including its exact resumable scope."""

        with self.SQLConnector() as connector:
            with connector.read_transaction():
                row = connector.fetch_one("""
                    SELECT working_build_id
                    FROM catalog_build_control
                    WHERE singleton_id = 1
                    """)
                if not row:
                    raise RuntimeError("catalog_build_control singleton is missing")
                if row[0] is None:
                    return None
                build_row = self._select_build(
                    connector,
                    str(row[0]),
                    for_update=False,
                )
        if not build_row:
            raise RuntimeError("The working pointer references a missing build")
        return self._build_from_row(build_row)

    def list_cleanup_candidates(self, *, limit: int) -> tuple[CatalogBuild, ...]:
        """List builds eligible for bounded projection/source cleanup."""

        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                rows = connector.fetch_all(
                    """
                    SELECT build.build_id
                    FROM catalog_builds AS build
                    CROSS JOIN catalog_source_revision AS source_pointer
                    CROSS JOIN catalog_build_control AS control
                    WHERE source_pointer.singleton_id = 1
                        AND control.singleton_id = 1
                        AND (
                            source_pointer.active_build_id IS NULL
                            OR build.build_id <> source_pointer.active_build_id
                        )
                        AND (
                            control.working_build_id IS NULL
                            OR build.build_id <> control.working_build_id
                        )
                        AND (
                            build.phase = %s
                            OR (
                                build.phase = %s
                                AND (
                                    NOT EXISTS (
                                        SELECT 1
                                        FROM catalog_build_projections AS projection
                                        WHERE projection.build_id = build.build_id
                                    )
                                    OR EXISTS (
                                        SELECT 1
                                        FROM catalog_projection_publication_receipts
                                            AS receipt
                                        WHERE receipt.build_id = build.build_id
                                            AND receipt.state = %s
                                    )
                                    OR (
                                        EXISTS (
                                            SELECT 1
                                            FROM catalog_build_projections AS projection
                                            WHERE projection.build_id = build.build_id
                                                AND projection.phase = 'PUBLISHED'
                                        )
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM catalog_projection_publication_receipts
                                                AS receipt
                                            WHERE receipt.build_id = build.build_id
                                        )
                                        AND EXISTS (
                                            SELECT 1
                                            FROM catalog_operational_activations
                                                AS activation
                                            WHERE activation.build_id = build.build_id
                                        )
                                    )
                                )
                            )
                        )
                    ORDER BY build.updated_at, build.build_id
                    LIMIT %s
                    """,
                    (
                        CatalogBuildPhase.abandoned.value,
                        CatalogBuildPhase.published.value,
                        "PROJECTION_FINALIZED",
                        limit,
                    ),
                )
                return tuple(
                    self._require_build(
                        connector,
                        str(row[0]),
                        for_update=False,
                    )
                    for row in rows
                )

    def get_active_build(self) -> CatalogBuild | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                row = connector.fetch_one("""
                    SELECT active_build_id
                    FROM catalog_source_revision
                    WHERE singleton_id = 1
                    """)
                if not row:
                    raise RuntimeError("catalog_source_revision singleton is missing")
                if row[0] is None:
                    return None
                build_row = self._select_build(
                    connector,
                    str(row[0]),
                    for_update=False,
                )
        if not build_row:
            raise RuntimeError("The active source pointer references a missing build")
        return self._build_from_row(build_row)

    def get_source_revision(
        self,
        revision: int | None = None,
    ) -> CatalogSourceRevision:
        if revision is not None and revision < 0:
            raise ValueError("revision must not be negative")
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                return self._get_source_revision(connector, revision)

    def list_sources(
        self,
        *,
        offset: int,
        limit: int,
        revision: CatalogSourceRevision | None,
    ) -> CatalogSourcePage:
        self._validate_page(offset, limit)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                selected = self._get_source_revision(
                    connector,
                    None if revision is None else revision.revision,
                )
                galleries, total = self._list_source_records(
                    connector,
                    selected.build_id,
                    offset,
                    limit,
                )
        return CatalogSourcePage(selected, galleries, offset, limit, total)

    def list_build_sources(
        self,
        build_id: str,
        *,
        offset: int,
        limit: int,
    ) -> CatalogBuildSourcePage:
        self._validate_page(offset, limit)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                build = self._require_build(connector, build_id, for_update=False)
                galleries, total = self._list_source_records(
                    connector,
                    build_id,
                    offset,
                    limit,
                )
        return CatalogBuildSourcePage(build, galleries, offset, limit, total)

    def list_pending_galleries(
        self,
        build_id: str,
        *,
        after_gallery_name: str | None,
        limit: int,
    ) -> CatalogPendingGalleryPage:
        if not 1 <= limit <= MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                build = self._require_build(connector, build_id, for_update=False)
                after_clause = ""
                parameters: tuple[Any, ...] = (build_id,)
                if after_gallery_name is not None:
                    after_clause = "AND discovery.gallery_name > %s"
                    parameters += (after_gallery_name,)
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        discovery.gallery_name,
                        discovery.source_locator,
                        CASE WHEN source.gallery_key IS NULL THEN 0 ELSE 1 END,
                        COALESCE(source.staged_file_count, 0)
                    FROM catalog_build_discoveries AS discovery
                    LEFT JOIN catalog_source_galleries AS source
                        ON source.build_id = discovery.build_id
                        AND source.gallery_key = discovery.gallery_key
                    WHERE discovery.build_id = %s
                        AND (
                            source.gallery_key IS NULL
                            OR source.source_complete = 0
                        )
                        {after_clause}
                    ORDER BY discovery.gallery_name, discovery.gallery_key
                    LIMIT %s
                    """,
                    (*parameters, limit),
                )
        return CatalogPendingGalleryPage(
            build=build,
            galleries=tuple(
                CatalogGalleryStageProgress(
                    gallery_name=str(name),
                    source_locator=str(source_locator),
                    header_staged=bool(header_staged),
                    staged_file_count=int(staged_file_count),
                )
                for name, source_locator, header_staged, staged_file_count in rows
            ),
            after_gallery_name=after_gallery_name,
            limit=limit,
        )

    def list_build_files(
        self,
        build_id: str,
        gallery_name: str,
        *,
        after: CatalogSourceFileCursor | None,
        limit: int,
    ) -> CatalogSourceFilePage:
        if not 1 <= limit <= MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        gallery_key = _stable_key(gallery_name)
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                self._require_build(connector, build_id, for_update=False)
                gallery = connector.fetch_one(
                    """
                    SELECT gallery_name
                    FROM catalog_source_galleries
                    WHERE build_id = %s AND gallery_key = %s
                    """,
                    (build_id, gallery_key),
                )
                if not gallery or str(gallery[0]) != gallery_name:
                    raise LookupError(
                        f"Gallery {gallery_name!r} is not staged in build {build_id}"
                    )
                after_clause = ""
                parameters: tuple[Any, ...] = (build_id, gallery_key)
                if after is not None:
                    after_clause = """
                        AND (
                            file_sort_key > %s
                            OR (
                                file_sort_key = %s
                                AND file_name > %s
                            )
                            OR (
                                file_sort_key = %s
                                AND file_name = %s
                                AND file_key > %s
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
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        file_key,
                        file_sort_key,
                        file_name,
                        size_bytes,
                        sha256,
                        relative_locator,
                        device,
                        inode,
                        modified_ns,
                        changed_ns
                    FROM catalog_source_files
                    WHERE build_id = %s AND gallery_key = %s
                        {after_clause}
                    ORDER BY file_sort_key, file_name, file_key
                    LIMIT %s
                    """,
                    (*parameters, limit + 1),
                )
        has_more = len(rows) > limit
        selected_rows = rows[:limit]
        next_cursor = (
            CatalogSourceFileCursor(
                file_sort_key=str(selected_rows[-1][1]),
                file_name=str(selected_rows[-1][2]),
                file_key=str(selected_rows[-1][0]),
            )
            if has_more and selected_rows
            else None
        )
        return CatalogSourceFilePage(
            build_id=build_id,
            gallery_name=gallery_name,
            files=tuple(
                GallerySourceFile(
                    name=str(row[2]),
                    size_bytes=int(row[3]),
                    sha256=str(row[4]),
                    relative_locator=None if row[5] is None else str(row[5]),
                    device=None if row[6] is None else int(row[6]),
                    inode=None if row[7] is None else int(row[7]),
                    modified_ns=None if row[8] is None else int(row[8]),
                    changed_ns=None if row[9] is None else int(row[9]),
                )
                for row in selected_rows
            ),
            after=after,
            next_cursor=next_cursor,
            limit=limit,
            has_more=has_more,
        )

    @staticmethod
    def _validate_batch_id(batch_id: str) -> None:
        if not batch_id or len(batch_id) > 191:
            raise ValueError("batch_id must contain between 1 and 191 characters")

    @staticmethod
    def _require_phase(build: CatalogBuild, expected: CatalogBuildPhase) -> None:
        if build.phase is not expected:
            raise CatalogBuildStateError(
                f"Catalog build {build.build_id} is {build.phase.value}; "
                f"expected {expected.value}"
            )

    def _require_owned_build(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        row = self._select_build(connector, build_id, for_update=True)
        if not row:
            raise CatalogBuildNotFoundError(build_id)
        build = self._build_from_row(row)
        if int(row[3]) != turn.generation or str(row[4]) != turn.owner_token:
            raise CatalogBuildStateError(
                "Catalog build is not bound to the supplied ingest turn"
            )
        return build

    def _require_build(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool,
    ) -> CatalogBuild:
        row = self._select_build(connector, build_id, for_update=for_update)
        if not row:
            raise CatalogBuildNotFoundError(build_id)
        return self._build_from_row(row)

    def _select_build(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool,
    ) -> tuple[Any, ...]:
        lock = self._lock_clause(self._context.sql_type) if for_update else ""
        return connector.fetch_one(
            """
            SELECT
                build_id,
                scope_key,
                phase,
                ingest_generation,
                owner_token,
                base_source_revision,
                base_active_build_id,
                discovered_gallery_count,
                expected_gallery_count,
                staged_gallery_count,
                staged_file_count,
                analyzed_gallery_count,
                created_at,
                updated_at,
                published_source_revision,
                seal_sha256,
                discovery_epoch,
                discovery_tree_sha256
            FROM catalog_builds
            WHERE build_id = %s
            """ + lock,
            (build_id,),
        )

    @staticmethod
    def _build_from_row(row: tuple[Any, ...]) -> CatalogBuild:
        return CatalogBuild(
            build_id=str(row[0]),
            scope_key=str(row[1]),
            phase=CatalogBuildPhase(str(row[2])),
            ingest_generation=int(row[3]),
            base_source_revision=int(row[5]),
            base_active_build_id=None if row[6] is None else str(row[6]),
            discovered_gallery_count=int(row[7]),
            expected_gallery_count=None if row[8] is None else int(row[8]),
            staged_gallery_count=int(row[9]),
            staged_file_count=int(row[10]),
            analyzed_gallery_count=int(row[11]),
            created_at=_parse_datetime(row[12]),
            updated_at=_parse_datetime(row[13]),
            published_source_revision=None if row[14] is None else int(row[14]),
            seal_sha256=None if row[15] is None else str(row[15]),
            discovery_epoch=None if row[16] is None else str(row[16]),
            discovery_tree_sha256=None if row[17] is None else str(row[17]),
        )

    @staticmethod
    def _existing_batch(
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
        batch_id: str,
    ) -> tuple[Any, ...] | None:
        row = connector.fetch_one(
            """
            SELECT payload_sha256, item_count, file_count
            FROM catalog_build_batches
            WHERE build_id = %s AND batch_kind = %s AND batch_id = %s
            """,
            (build_id, batch_kind, batch_id),
        )
        return row or None

    @staticmethod
    def _replayed_batch_result(
        build_id: str,
        batch_id: str,
        payload_sha256: str,
        previous: tuple[Any, ...],
    ) -> CatalogBuildBatchResult:
        if str(previous[0]) != payload_sha256:
            raise CatalogBuildBatchConflictError(
                f"Catalog build batch {batch_id!r} was retried with different data"
            )
        return CatalogBuildBatchResult(
            build_id,
            batch_id,
            False,
            int(previous[1]),
            int(previous[2]),
        )

    @staticmethod
    def _record_batch(
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
        batch_id: str,
        payload_sha256: str,
        item_count: int,
        file_count: int,
    ) -> None:
        connector.execute(
            """
            INSERT INTO catalog_build_batches (
                build_id,
                batch_kind,
                batch_id,
                payload_sha256,
                item_count,
                file_count
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                build_id,
                batch_kind,
                batch_id,
                payload_sha256,
                item_count,
                file_count,
            ),
        )

    def _ensure_discovery_keys_absent(
        self,
        connector: SQLConnector,
        build_id: str,
        keyed_names: Sequence[tuple[str, str]],
    ) -> None:
        existing = self._names_by_keys(
            connector,
            "catalog_build_discoveries",
            build_id,
            keyed_names,
        )
        if existing:
            raise CatalogBuildBatchConflictError(
                "A gallery was discovered by more than one batch"
            )

    def _ensure_source_keys_absent(
        self,
        connector: SQLConnector,
        build_id: str,
        keyed_names: Sequence[tuple[str, str]],
    ) -> None:
        existing = self._names_by_keys(
            connector,
            "catalog_source_galleries",
            build_id,
            keyed_names,
        )
        if existing:
            raise CatalogBuildBatchConflictError(
                "A gallery source was staged by more than one batch"
            )

    def _ensure_discovered(
        self,
        connector: SQLConnector,
        build_id: str,
        keyed_names: Sequence[tuple[str, str]],
    ) -> None:
        existing = self._names_by_keys(
            connector,
            "catalog_build_discoveries",
            build_id,
            keyed_names,
        )
        expected = {key: name for key, name in keyed_names}
        if existing != expected:
            raise CatalogBuildStateError(
                "Source staging batch contains a gallery absent from discovery"
            )

    def _names_by_keys(
        self,
        connector: SQLConnector,
        table_name: str,
        build_id: str,
        keyed_names: Sequence[tuple[str, str]],
    ) -> dict[str, str]:
        keys = tuple(key for key, _name in keyed_names)
        rows: list[tuple[Any, ...]] = []
        for start in range(0, len(keys), LOOKUP_CHUNK_SIZE):
            chunk = keys[start : start + LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join("%s" for _ in chunk)
            rows.extend(
                connector.fetch_all(
                    f"""
                    SELECT gallery_key, gallery_name
                    FROM {table_name}
                    WHERE build_id = %s AND gallery_key IN ({placeholders})
                    """,
                    (build_id, *chunk),
                )
            )
        result = {str(key): str(name) for key, name in rows}
        requested = {key: name for key, name in keyed_names}
        for key, name in result.items():
            if requested.get(key) != name:
                raise CatalogBuildBatchConflictError(
                    "A gallery name SHA-256 key collision was detected"
                )
        return result

    def _cache_rows_by_ids(
        self,
        connector: SQLConnector,
        cache_ids: Sequence[str],
    ) -> dict[str, tuple[str, str, str]]:
        rows: list[tuple[Any, ...]] = []
        for start in range(0, len(cache_ids), LOOKUP_CHUNK_SIZE):
            chunk = cache_ids[start : start + LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join("%s" for _ in chunk)
            rows.extend(
                connector.fetch_all(
                    f"""
                    SELECT cache_key, source_key, fingerprint, sha256
                    FROM catalog_file_hash_cache
                    WHERE cache_key IN ({placeholders})
                    """,
                    tuple(chunk),
                )
            )
        return {
            str(cache_id): (str(source_key), str(fingerprint), str(digest))
            for cache_id, source_key, fingerprint, digest in rows
        }

    @staticmethod
    def _count(connector: SQLConnector, table_name: str, build_id: str) -> int:
        row = connector.fetch_one(
            f"SELECT COUNT(*) FROM {table_name} WHERE build_id = %s",
            (build_id,),
        )
        return int(row[0]) if row else 0

    @staticmethod
    def _batch_totals(
        connector: SQLConnector,
        build_id: str,
        batch_kind: str,
    ) -> tuple[int, int]:
        row = connector.fetch_one(
            """
            SELECT COALESCE(SUM(item_count), 0), COALESCE(SUM(file_count), 0)
            FROM catalog_build_batches
            WHERE build_id = %s AND batch_kind = %s
            """,
            (build_id, batch_kind),
        )
        return (int(row[0]), int(row[1])) if row else (0, 0)

    def _validate_raw_source_complete(
        self,
        connector: SQLConnector,
        build: CatalogBuild,
    ) -> tuple[int, int]:
        if build.expected_gallery_count is None:
            raise CatalogBuildStateError("Catalog build discovery is not complete")
        discovery_count = self._count(
            connector,
            "catalog_build_discoveries",
            build.build_id,
        )
        gallery_count = self._count(
            connector,
            "catalog_source_galleries",
            build.build_id,
        )
        # File rows are deliberately not counted here: that would scan the
        # multi-million-row source table while holding the seal write
        # transaction. Every file-chunk insert advances the durable build
        # counter in the same transaction, and each gallery completion already
        # verifies its own unique file count.
        file_count = build.staged_file_count
        discovery_batch_count, _ = self._batch_totals(
            connector,
            build.build_id,
            "DISCOVERY",
        )
        header_batch_count, _ = self._batch_totals(
            connector,
            build.build_id,
            "HEADER",
        )
        complete_batch_count, complete_batch_files = self._batch_totals(
            connector,
            build.build_id,
            "COMPLETE",
        )
        _, file_batch_count = self._batch_totals(
            connector,
            build.build_id,
            "FILES",
        )
        expected = build.expected_gallery_count
        if not (
            discovery_count
            == discovery_batch_count
            == gallery_count
            == header_batch_count
            == complete_batch_count
            == build.discovered_gallery_count
            == build.staged_gallery_count
            == expected
        ) or not (
            file_count
            == complete_batch_files
            == file_batch_count
            == build.staged_file_count
        ):
            raise CatalogBuildStateError(
                "Catalog build source rows, batches, and counters are incomplete"
            )
        missing = connector.fetch_one(
            """
            SELECT discovery.gallery_name
            FROM catalog_build_discoveries AS discovery
            LEFT JOIN catalog_source_galleries AS source
                ON source.build_id = discovery.build_id
                AND source.gallery_key = discovery.gallery_key
            WHERE discovery.build_id = %s
                AND (
                    source.gallery_key IS NULL
                    OR source.source_complete = 0
                    OR source.expected_file_count IS NULL
                    OR source.staged_file_count <> source.expected_file_count
                    OR source.scan_observation_sha256 IS NULL
                    OR source.scan_observation_version IS NULL
                )
            LIMIT 1
            """,
            (build.build_id,),
        )
        if missing:
            raise CatalogBuildStateError(
                f"Discovered gallery {missing[0]!s} has no staged source record"
            )
        return gallery_count, file_count

    def _validate_complete_source(
        self,
        connector: SQLConnector,
        build: CatalogBuild,
    ) -> tuple[int, int]:
        gallery_count, file_count = self._validate_raw_source_complete(
            connector,
            build,
        )
        legacy_analysis_batch_count, _ = self._batch_totals(
            connector,
            build.build_id,
            "ANALYSIS",
        )
        staged_analysis_batch_count, _ = self._batch_totals(
            connector,
            build.build_id,
            "AN_FINAL_ANALYSES",
        )
        analysis_batch_count = legacy_analysis_batch_count + staged_analysis_batch_count
        analyzed_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries
            WHERE build_id = %s AND analysis_complete = 1
            """,
            (build.build_id,),
        )
        analyzed_count = int(analyzed_row[0]) if analyzed_row else 0
        if not (
            analyzed_count
            == analysis_batch_count
            == build.analyzed_gallery_count
            == gallery_count
        ):
            raise CatalogBuildStateError(
                "Catalog analysis rows, batches, and counters are incomplete"
            )
        missing_manifest = connector.fetch_one(
            """
            SELECT gallery_name
            FROM catalog_source_galleries
            WHERE build_id = %s
                AND (
                    source_manifest_sha256 IS NULL
                    OR source_manifest_version IS NULL
                )
            LIMIT 1
            """,
            (build.build_id,),
        )
        if missing_manifest:
            raise CatalogBuildStateError(
                f"Gallery {missing_manifest[0]!s} has no canonical source manifest"
            )
        invalid_duplicate = connector.fetch_one(
            """
            SELECT duplicate.gallery_name
            FROM catalog_source_galleries AS duplicate
            LEFT JOIN catalog_source_galleries AS owner
                ON owner.build_id = duplicate.build_id
                AND owner.gallery_key = duplicate.duplicate_of_gallery_key
            WHERE duplicate.build_id = %s
                AND duplicate.duplicate_of_gallery_name IS NOT NULL
                AND (
                    duplicate.gallery_key = duplicate.duplicate_of_gallery_key
                    OR owner.gallery_key IS NULL
                    OR owner.gallery_name <> duplicate.duplicate_of_gallery_name
                    OR owner.duplicate_of_gallery_name IS NOT NULL
                    OR owner.content_sha256 IS NULL
                    OR owner.content_sha256 <> duplicate.content_sha256
                    OR duplicate.selected <> 0
                )
            LIMIT 1
            """,
            (build.build_id,),
        )
        if invalid_duplicate:
            raise CatalogBuildStateError(
                f"Gallery {invalid_duplicate[0]!s} has an invalid duplicate owner"
            )
        duplicate_owner = connector.fetch_one(
            """
            SELECT content_sha256
            FROM catalog_source_galleries
            WHERE build_id = %s
                AND duplicate_of_gallery_name IS NULL
                AND content_sha256 IS NOT NULL
            GROUP BY content_sha256
            HAVING COUNT(*) > 1
            LIMIT 1
            """,
            (build.build_id,),
        )
        if duplicate_owner:
            raise CatalogBuildStateError(
                "Catalog build has multiple owners for one content SHA-256"
            )
        duplicate_selected_gid = connector.fetch_one(
            """
            SELECT gid
            FROM catalog_source_galleries
            WHERE build_id = %s AND selected = 1
            GROUP BY gid
            HAVING COUNT(*) > 1
            LIMIT 1
            """,
            (build.build_id,),
        )
        if duplicate_selected_gid:
            raise CatalogBuildStateError(
                "Catalog build selected more than one gallery for a GID"
            )
        return gallery_count, file_count

    @staticmethod
    def _validate_page(offset: int, limit: int) -> None:
        if offset < 0:
            raise ValueError("offset must not be negative")
        if not 1 <= limit <= MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_SOURCE_PAGE_SIZE}")

    @staticmethod
    def _get_source_revision(
        connector: SQLConnector,
        revision: int | None,
    ) -> CatalogSourceRevision:
        if revision is None:
            row = connector.fetch_one("""
                SELECT
                    history.revision,
                    history.build_id,
                    history.published_at,
                    history.gallery_count,
                    history.file_count
                FROM catalog_source_revision AS current
                JOIN catalog_source_revision_history AS history
                    ON history.revision = current.current_revision
                WHERE current.singleton_id = 1
                """)
        else:
            row = connector.fetch_one(
                """
                SELECT revision, build_id, published_at, gallery_count, file_count
                FROM catalog_source_revision_history
                WHERE revision = %s
                """,
                (revision,),
            )
        if not row:
            if revision is None:
                raise RuntimeError("The active catalog source revision is missing")
            raise LookupError(f"Catalog source revision {revision} does not exist")
        return CatalogSourceRevision(
            revision=int(row[0]),
            build_id=None if row[1] is None else str(row[1]),
            published_at=_parse_datetime(row[2]),
            gallery_count=int(row[3]),
            file_count=int(row[4]),
        )

    def _list_source_records(
        self,
        connector: SQLConnector,
        build_id: str | None,
        offset: int,
        limit: int,
    ) -> tuple[tuple[CatalogSourceGalleryRecord, ...], int]:
        if build_id is None:
            return (), 0
        total_row = connector.fetch_one(
            """
            SELECT COUNT(*)
            FROM catalog_source_galleries
            WHERE build_id = %s AND source_complete = 1
            """,
            (build_id,),
        )
        total = int(total_row[0]) if total_row else 0
        rows = connector.fetch_all(
            """
            SELECT
                source.gallery_key,
                source.gallery_name,
                discovery.source_locator,
                discovery.metadata_fingerprint,
                source.metadata_sha256,
                source.gid,
                source.title,
                source.comment,
                source.upload_account,
                source.upload_time,
                source.download_time,
                source.modified_time,
                source.expected_file_count,
                source.source_manifest_sha256,
                source.source_manifest_version,
                source.scan_observation_sha256,
                source.scan_observation_version,
                source.page_count,
                source.directory_entry_count,
                source.directory_observation_sha256,
                source.content_sha256,
                source.duplicate_of_gallery_name
            FROM catalog_source_galleries AS source
            JOIN catalog_build_discoveries AS discovery
                ON discovery.build_id = source.build_id
                AND discovery.gallery_key = source.gallery_key
            WHERE source.build_id = %s AND source.source_complete = 1
            ORDER BY source.gallery_name, source.gallery_key
            LIMIT %s OFFSET %s
            """,
            (build_id, limit, offset),
        )
        if not rows:
            return (), total
        gallery_keys = tuple(str(row[0]) for row in rows)
        placeholders = ", ".join("%s" for _ in gallery_keys)
        parameters: tuple[Any, ...] = (build_id, *gallery_keys)
        tag_rows = connector.fetch_all(
            f"""
            SELECT gallery_key, tag_name, tag_value
            FROM catalog_source_tags
            WHERE build_id = %s AND gallery_key IN ({placeholders})
            ORDER BY gallery_key, position
            """,
            parameters,
        )
        tags: dict[str, list[GalleryTag]] = {}
        for gallery_key, tag_name, tag_value in tag_rows:
            tags.setdefault(str(gallery_key), []).append(
                GalleryTag(str(tag_name), str(tag_value))
            )
        records = tuple(
            CatalogSourceGalleryRecord(
                gallery_name=str(row[1]),
                source_locator=str(row[2]),
                metadata_fingerprint=None if row[3] is None else str(row[3]),
                metadata_sha256=None if row[4] is None else str(row[4]),
                gid=int(row[5]),
                title=str(row[6]),
                comment=str(row[7]),
                upload_account=str(row[8]),
                upload_time=_parse_datetime(row[9]),
                download_time=_parse_datetime(row[10]),
                modified_time=_parse_datetime(row[11]),
                tags=tuple(tags.get(str(row[0]), ())),
                source_file_count=int(row[12]),
                source_manifest_sha256=None if row[13] is None else str(row[13]),
                source_manifest_version=None if row[14] is None else int(row[14]),
                scan_observation_sha256=(None if row[15] is None else str(row[15])),
                scan_observation_version=(None if row[16] is None else int(row[16])),
                page_count=None if row[17] is None else int(row[17]),
                directory_entry_count=None if row[18] is None else int(row[18]),
                directory_observation_sha256=(
                    None if row[19] is None else str(row[19])
                ),
                content_sha256=None if row[20] is None else str(row[20]),
                duplicate_of_gallery_name=None if row[21] is None else str(row[21]),
            )
            for row in rows
        )
        return records, total
