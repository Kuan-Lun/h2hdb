from __future__ import annotations

__all__ = [
    "CatalogOperationalGenerationStaleError",
    "CatalogOperationalRepository",
    "CatalogOperationalStateError",
    "MAX_OPERATIONAL_PREPARE_PAGE_SIZE",
]

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .catalog_build_repository import CatalogBuildRepository, CatalogBuildStateError
from .domain import (
    CatalogBuildOperationalPhase,
    CatalogBuildOperationalState,
    CatalogBuildPhase,
)
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector
from .table_gallery_ingest_coordination import GalleryIngestTurn
from .todownload_queue import (
    DownloadRequest,
    EnsureDownloadRequestResult,
    H2HDBToDownloadQueue,
)

MAX_OPERATIONAL_PREPARE_PAGE_SIZE = 1000
OPERATIONAL_SCHEMA_VERSION = 1


class CatalogOperationalStateError(RuntimeError):
    pass


class CatalogOperationalGenerationStaleError(CatalogOperationalStateError):
    """A deletion request raced a completed operational preparation.

    This error is deliberately public and retryable.  A caller must rerun the
    bounded operational preparation on the same build, then retry publication.
    """


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _operational_datetime(value: object) -> str:
    return _parse_datetime(value).strftime("%Y-%m-%d %H:%M:%S")


class CatalogOperationalRepository(BaseRepository):
    """Build-scoped operational authority activated by the source pointer.

    Preparation is a keyset-paged state machine.  Rows from an obsolete
    preparation carry a different ``preparation_id`` and therefore remain
    invisible without requiring an unbounded cleanup transaction.
    """

    def __init__(
        self,
        context: RepositoryContext,
        builds: CatalogBuildRepository,
        download_queue: H2HDBToDownloadQueue,
    ) -> None:
        super().__init__(context)
        self._builds = builds
        self._download_queue = download_queue

    @staticmethod
    def _lock_clause(sql_type: str) -> str:
        return " FOR UPDATE" if sql_type == "mariadb" else ""

    def _source_pointer(
        self,
        connector: SQLConnector,
        *,
        for_update: bool,
    ) -> tuple[int, str | None, int]:
        lock = self._lock_clause(self._context.sql_type) if for_update else ""
        row = connector.fetch_one("""
            SELECT
                current_revision,
                active_build_id,
                deletion_request_generation
            FROM catalog_source_revision
            WHERE singleton_id = 1
            """ + lock)
        if not row:
            raise RuntimeError("catalog_source_revision singleton is missing")
        return (
            int(row[0]),
            None if row[1] is None else str(row[1]),
            int(row[2]),
        )

    @staticmethod
    def _state_from_row(row: tuple[Any, ...]) -> CatalogBuildOperationalState:
        return CatalogBuildOperationalState(
            build_id=str(row[0]),
            preparation_id=str(row[1]),
            phase=CatalogBuildOperationalPhase(str(row[2])),
            deletion_request_generation=int(row[3]),
            after_gallery_key=None if row[4] is None else str(row[4]),
            after_gid=None if row[5] is None else int(row[5]),
            normalized_gallery_count=int(row[6]),
            removed_gid_request_count=int(row[7]),
            deletion_consumption_count=int(row[8]),
            prepared_at=_parse_datetime(row[9]),
            completed_at=None if row[10] is None else _parse_datetime(row[10]),
        )

    @staticmethod
    def _select_state(
        connector: SQLConnector,
        build_id: str,
        *,
        for_update: bool,
        sql_type: str,
    ) -> tuple[Any, ...] | None:
        lock = " FOR UPDATE" if for_update and sql_type == "mariadb" else ""
        row = connector.fetch_one(
            """
            SELECT
                build_id,
                preparation_id,
                phase,
                deletion_request_generation,
                after_gallery_key,
                after_gid,
                normalized_gallery_count,
                removed_gid_request_count,
                deletion_consumption_count,
                prepared_at,
                completed_at
            FROM catalog_build_operational_state
            WHERE build_id = %s
            """ + lock,
            (build_id,),
        )
        return row or None

    def get_state(self, build_id: str) -> CatalogBuildOperationalState | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                row = self._select_state(
                    connector,
                    build_id,
                    for_update=False,
                    sql_type=self._context.sql_type,
                )
        return None if row is None else self._state_from_row(row)

    def _replace_state(
        self,
        connector: SQLConnector,
        state: CatalogBuildOperationalState,
    ) -> None:
        connector.execute(
            """
            UPDATE catalog_build_operational_state
            SET preparation_id = %s,
                operational_schema_version = %s,
                phase = %s,
                deletion_request_generation = %s,
                after_gallery_key = %s,
                after_gid = %s,
                normalized_gallery_count = %s,
                removed_gid_request_count = %s,
                deletion_consumption_count = %s,
                prepared_at = %s,
                completed_at = %s
            WHERE build_id = %s
            """,
            (
                state.preparation_id,
                OPERATIONAL_SCHEMA_VERSION,
                state.phase.value,
                state.deletion_request_generation,
                state.after_gallery_key,
                state.after_gid,
                state.normalized_gallery_count,
                state.removed_gid_request_count,
                state.deletion_consumption_count,
                state.prepared_at.isoformat(),
                None if state.completed_at is None else state.completed_at.isoformat(),
                state.build_id,
            ),
        )

    def _new_state(
        self,
        connector: SQLConnector,
        build_id: str,
        generation: int,
        *,
        phase: CatalogBuildOperationalPhase = (
            CatalogBuildOperationalPhase.normalizing_times
        ),
        after_gallery_key: str | None = None,
        normalized_gallery_count: int = 0,
    ) -> CatalogBuildOperationalState:
        now = self._builds._database_datetime(connector)
        state = CatalogBuildOperationalState(
            build_id=build_id,
            preparation_id=uuid4().hex,
            phase=phase,
            deletion_request_generation=generation,
            after_gallery_key=after_gallery_key,
            after_gid=None,
            normalized_gallery_count=normalized_gallery_count,
            removed_gid_request_count=0,
            deletion_consumption_count=0,
            prepared_at=now,
            completed_at=None,
        )
        existing = self._select_state(
            connector,
            build_id,
            for_update=True,
            sql_type=self._context.sql_type,
        )
        if existing is None:
            connector.execute(
                """
                INSERT INTO catalog_build_operational_state (
                    build_id,
                    preparation_id,
                    operational_schema_version,
                    phase,
                    deletion_request_generation,
                    after_gallery_key,
                    after_gid,
                    normalized_gallery_count,
                    removed_gid_request_count,
                    deletion_consumption_count,
                    prepared_at,
                    completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, 0, 0, %s, NULL)
                """,
                (
                    build_id,
                    state.preparation_id,
                    OPERATIONAL_SCHEMA_VERSION,
                    state.phase.value,
                    generation,
                    state.after_gallery_key,
                    state.normalized_gallery_count,
                    now.isoformat(),
                ),
            )
        else:
            self._replace_state(connector, state)
        return state

    def _require_preparable_build(
        self,
        connector: SQLConnector,
        build_id: str,
        turn: GalleryIngestTurn,
    ) -> tuple[Any, int]:
        build = self._builds._require_owned_build(connector, build_id, turn)
        if build.phase not in {CatalogBuildPhase.artifacts, CatalogBuildPhase.sealed}:
            raise CatalogOperationalStateError(
                "Operational preparation requires an ARTIFACTS or SEALED source build"
            )
        projection = connector.fetch_one(
            """
            SELECT phase
            FROM catalog_build_projections
            WHERE build_id = %s
            """,
            (build_id,),
        )
        if projection and str(projection[0]) not in {"COMPLETE", "SEALED"}:
            raise CatalogOperationalStateError(
                "Operational preparation requires a COMPLETE or SEALED projection"
            )
        current_revision, active_build_id, generation = self._source_pointer(
            connector,
            for_update=True,
        )
        if (
            current_revision != build.base_source_revision
            or active_build_id != build.base_active_build_id
        ):
            raise CatalogBuildStateError(
                "Active source revision changed before operational preparation"
            )
        return build, generation

    def prepare(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        max_rows: int,
        turn: GalleryIngestTurn,
    ) -> CatalogBuildOperationalState:
        if not 1 <= max_rows <= MAX_OPERATIONAL_PREPARE_PAGE_SIZE:
            raise ValueError(
                "max_rows must be between 1 and " f"{MAX_OPERATIONAL_PREPARE_PAGE_SIZE}"
            )
        build, generation = self._require_preparable_build(
            connector,
            build_id,
            turn,
        )
        row = self._select_state(
            connector,
            build_id,
            for_update=True,
            sql_type=self._context.sql_type,
        )
        state = (
            self._new_state(connector, build_id, generation)
            if row is None
            else self._state_from_row(row)
        )
        if state.deletion_request_generation != generation:
            normalizing = state.phase is CatalogBuildOperationalPhase.normalizing_times
            state = self._new_state(
                connector,
                build_id,
                generation,
                phase=(
                    CatalogBuildOperationalPhase.normalizing_times
                    if normalizing
                    else CatalogBuildOperationalPhase.removed_gid_requests
                ),
                after_gallery_key=(state.after_gallery_key if normalizing else None),
                normalized_gallery_count=state.normalized_gallery_count,
            )
        if state.complete:
            return state
        match state.phase:
            case CatalogBuildOperationalPhase.normalizing_times:
                state = self._prepare_normalized_times(
                    connector,
                    state,
                    max_rows=max_rows,
                )
            case CatalogBuildOperationalPhase.removed_gid_requests:
                state = self._prepare_removed_gid_requests(
                    connector,
                    state,
                    base_active_build_id=build.base_active_build_id,
                    max_rows=max_rows,
                )
            case CatalogBuildOperationalPhase.deletion_consumptions:
                state = self._prepare_deletion_consumptions(
                    connector,
                    state,
                    max_rows=max_rows,
                )
            case CatalogBuildOperationalPhase.complete:
                return state
        self._replace_state(connector, state)
        return state

    def _prepare_normalized_times(
        self,
        connector: SQLConnector,
        state: CatalogBuildOperationalState,
        *,
        max_rows: int,
    ) -> CatalogBuildOperationalState:
        after = state.after_gallery_key or ""
        rows = connector.fetch_all(
            """
            SELECT gallery_key, upload_time, download_time
            FROM catalog_source_galleries
            WHERE build_id = %s AND gallery_key > %s
            ORDER BY gallery_key
            LIMIT %s
            """,
            (state.build_id, after, max_rows),
        )
        if rows:
            connector.execute_many(
                """
                UPDATE catalog_source_galleries
                SET upload_time_utc = %s, download_time_utc = %s
                WHERE build_id = %s AND gallery_key = %s
                """,
                [
                    (
                        _operational_datetime(upload_time),
                        _operational_datetime(download_time),
                        state.build_id,
                        str(gallery_key),
                    )
                    for gallery_key, upload_time, download_time in rows
                ],
            )
        terminal = len(rows) < max_rows
        return CatalogBuildOperationalState(
            state.build_id,
            state.preparation_id,
            (
                CatalogBuildOperationalPhase.removed_gid_requests
                if terminal
                else state.phase
            ),
            state.deletion_request_generation,
            None if terminal else str(rows[-1][0]),
            None,
            state.normalized_gallery_count + len(rows),
            state.removed_gid_request_count,
            state.deletion_consumption_count,
            state.prepared_at,
        )

    @staticmethod
    def _effective_deletion_filter(marker_alias: str) -> str:
        return f"""
            NOT EXISTS (
                SELECT 1
                FROM catalog_build_deletion_consumptions AS consumed
                JOIN catalog_operational_activations AS consumed_activation
                    ON consumed_activation.build_id = consumed.build_id
                    AND consumed_activation.preparation_id = consumed.preparation_id
                WHERE consumed.gid = {marker_alias}.gid
                    AND consumed.deletion_request_token =
                        {marker_alias}.request_token
            )
        """

    def _prepare_removed_gid_requests(
        self,
        connector: SQLConnector,
        state: CatalogBuildOperationalState,
        *,
        base_active_build_id: str | None,
        max_rows: int,
    ) -> CatalogBuildOperationalState:
        after = state.after_gid or 0
        deletion_filter = self._effective_deletion_filter("marker")
        if base_active_build_id is None:
            query = f"""
                SELECT base.gid
                FROM galleries_gids AS base
                WHERE base.gid > %s
                    AND NOT EXISTS (
                        SELECT 1 FROM catalog_source_galleries AS candidate
                        WHERE candidate.build_id = %s
                            AND candidate.gid = base.gid
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM todelete_gids AS marker
                        WHERE marker.gid = base.gid AND {deletion_filter}
                    )
                GROUP BY base.gid
                ORDER BY base.gid
                LIMIT %s
            """
            parameters: tuple[Any, ...] = (after, state.build_id, max_rows)
        else:
            query = f"""
                SELECT base.gid
                FROM catalog_source_galleries AS base
                WHERE base.build_id = %s
                    AND base.gid > %s
                    AND NOT EXISTS (
                        SELECT 1 FROM catalog_source_galleries AS candidate
                        WHERE candidate.build_id = %s
                            AND candidate.gid = base.gid
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM todelete_gids AS marker
                        WHERE marker.gid = base.gid AND {deletion_filter}
                    )
                GROUP BY base.gid
                ORDER BY base.gid
                LIMIT %s
            """
            parameters = (
                base_active_build_id,
                after,
                state.build_id,
                max_rows,
            )
        rows = connector.fetch_all(query, parameters)
        if rows:
            connector.execute_many(
                """
                INSERT INTO catalog_build_removed_gid_requests (
                    build_id,
                    preparation_id,
                    gid,
                    url,
                    request_token
                ) VALUES (%s, %s, %s, '', %s)
                """,
                [
                    (state.build_id, state.preparation_id, int(row[0]), uuid4().hex)
                    for row in rows
                ],
            )
        terminal = len(rows) < max_rows
        return CatalogBuildOperationalState(
            state.build_id,
            state.preparation_id,
            (
                CatalogBuildOperationalPhase.deletion_consumptions
                if terminal
                else state.phase
            ),
            state.deletion_request_generation,
            None,
            None if terminal else int(rows[-1][0]),
            state.normalized_gallery_count,
            state.removed_gid_request_count + len(rows),
            state.deletion_consumption_count,
            state.prepared_at,
        )

    def _prepare_deletion_consumptions(
        self,
        connector: SQLConnector,
        state: CatalogBuildOperationalState,
        *,
        max_rows: int,
    ) -> CatalogBuildOperationalState:
        after = state.after_gid or 0
        deletion_filter = self._effective_deletion_filter("marker")
        rows = connector.fetch_all(
            f"""
            SELECT marker.gid, marker.request_token
            FROM todelete_gids AS marker
            WHERE marker.gid > %s
                AND {deletion_filter}
                AND NOT EXISTS (
                    SELECT 1 FROM catalog_source_galleries AS candidate
                    WHERE candidate.build_id = %s
                        AND candidate.gid = marker.gid
                )
            ORDER BY marker.gid
            LIMIT %s
            """,
            (after, state.build_id, max_rows),
        )
        if rows:
            connector.execute_many(
                """
                INSERT INTO catalog_build_deletion_consumptions (
                    build_id,
                    preparation_id,
                    gid,
                    deletion_request_token
                ) VALUES (%s, %s, %s, %s)
                """,
                [
                    (
                        state.build_id,
                        state.preparation_id,
                        int(gid),
                        str(token),
                    )
                    for gid, token in rows
                ],
            )
        terminal = len(rows) < max_rows
        completed_at = self._builds._database_datetime(connector) if terminal else None
        return CatalogBuildOperationalState(
            state.build_id,
            state.preparation_id,
            CatalogBuildOperationalPhase.complete if terminal else state.phase,
            state.deletion_request_generation,
            None,
            None if terminal else int(rows[-1][0]),
            state.normalized_gallery_count,
            state.removed_gid_request_count,
            state.deletion_consumption_count + len(rows),
            state.prepared_at,
            completed_at,
        )

    def require_ready_for_activation(
        self,
        connector: SQLConnector,
        build_id: str,
    ) -> CatalogBuildOperationalState:
        _revision, _active, generation = self._source_pointer(
            connector,
            for_update=True,
        )
        row = self._select_state(
            connector,
            build_id,
            for_update=True,
            sql_type=self._context.sql_type,
        )
        if row is None:
            raise CatalogOperationalStateError(
                "Catalog build operational preparation is missing"
            )
        state = self._state_from_row(row)
        if state.deletion_request_generation != generation:
            raise CatalogOperationalGenerationStaleError(
                "Deletion request generation changed; refresh operational preparation"
            )
        if not state.complete:
            raise CatalogOperationalStateError(
                "Catalog build operational preparation is incomplete"
            )
        return state

    def activate(
        self,
        connector: SQLConnector,
        state: CatalogBuildOperationalState,
        *,
        source_revision: int,
        activated_at: datetime,
    ) -> None:
        existing = connector.fetch_one(
            """
            SELECT source_revision, preparation_id
            FROM catalog_operational_activations
            WHERE build_id = %s
            """,
            (state.build_id,),
        )
        if existing:
            if (
                int(existing[0]) != source_revision
                or str(existing[1]) != state.preparation_id
            ):
                raise CatalogOperationalStateError(
                    "Catalog operational activation conflicts with its persisted build"
                )
            return
        connector.execute(
            """
            INSERT INTO catalog_operational_activations (
                build_id,
                source_revision,
                preparation_id,
                operational_schema_version,
                activated_at
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                state.build_id,
                source_revision,
                state.preparation_id,
                OPERATIONAL_SCHEMA_VERSION,
                activated_at.isoformat(),
            ),
        )

    def active_authority(
        self,
        connector: SQLConnector,
        *,
        for_update: bool = False,
    ) -> tuple[str, int, str] | None:
        revision, build_id, _generation = self._source_pointer(
            connector,
            for_update=for_update,
        )
        if build_id is None:
            return None
        row = connector.fetch_one(
            """
            SELECT preparation_id
            FROM catalog_operational_activations
            WHERE build_id = %s AND source_revision = %s
            """,
            (build_id, revision),
        )
        return None if not row else (build_id, revision, str(row[0]))

    def _effective_auto_request(
        self,
        connector: SQLConnector,
        gid: int,
    ) -> DownloadRequest | None:
        row = connector.fetch_one(
            """
            SELECT event.gid, event.url, event.request_token
            FROM catalog_build_removed_gid_requests AS event
            JOIN catalog_operational_activations AS activation
                ON activation.build_id = event.build_id
                AND activation.preparation_id = event.preparation_id
            LEFT JOIN catalog_removed_gid_request_acks AS ack
                ON ack.gid = event.gid
            WHERE event.gid = %s
                AND activation.source_revision >
                    COALESCE(ack.through_source_revision, 0)
                AND NOT EXISTS (
                    SELECT 1 FROM todownload_gids AS manual
                    WHERE manual.gid = event.gid
                )
            ORDER BY activation.source_revision
            LIMIT 1
            """,
            (gid,),
        )
        if not row:
            return None
        return DownloadRequest(int(row[0]), str(row[1]), str(row[2]))

    def get_download_request_with_connector(
        self,
        connector: SQLConnector,
        gid: int,
    ) -> DownloadRequest | None:
        row = connector.fetch_one(
            """
            SELECT gid, url, request_token
            FROM todownload_gids
            WHERE gid = %s
            """,
            (gid,),
        )
        if row:
            return DownloadRequest(int(row[0]), str(row[1]), str(row[2]))
        if self.active_authority(connector) is None:
            return None
        return self._effective_auto_request(connector, gid)

    def get_download_requests_with_connector(
        self,
        connector: SQLConnector,
    ) -> list[DownloadRequest]:
        manual_rows = connector.fetch_all("""
            SELECT gid, url, request_token
            FROM todownload_gids
            ORDER BY gid
            """)
        requests = {
            int(gid): DownloadRequest(int(gid), str(url), str(token))
            for gid, url, token in manual_rows
        }
        if self.active_authority(connector) is not None:
            auto_rows = connector.fetch_all("""
                SELECT event.gid, event.url, event.request_token,
                    activation.source_revision
                FROM catalog_build_removed_gid_requests AS event
                JOIN catalog_operational_activations AS activation
                    ON activation.build_id = event.build_id
                    AND activation.preparation_id = event.preparation_id
                LEFT JOIN catalog_removed_gid_request_acks AS ack
                    ON ack.gid = event.gid
                WHERE activation.source_revision >
                        COALESCE(ack.through_source_revision, 0)
                    AND NOT EXISTS (
                        SELECT 1 FROM todownload_gids AS manual
                        WHERE manual.gid = event.gid
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM catalog_build_removed_gid_requests AS older_event
                        JOIN catalog_operational_activations AS older_activation
                            ON older_activation.build_id = older_event.build_id
                            AND older_activation.preparation_id =
                                older_event.preparation_id
                        WHERE older_event.gid = event.gid
                            AND older_activation.source_revision >
                                COALESCE(ack.through_source_revision, 0)
                            AND older_activation.source_revision <
                                activation.source_revision
                    )
                ORDER BY event.gid, activation.source_revision
                """)
            for gid, url, token, _revision in auto_rows:
                requests.setdefault(
                    int(gid),
                    DownloadRequest(int(gid), str(url), str(token)),
                )
        return [requests[gid] for gid in sorted(requests)]

    def request_download_with_connector(
        self,
        connector: SQLConnector,
        gid: int,
        url: str = "",
    ) -> DownloadRequest:
        self._source_pointer(connector, for_update=True)
        return self._download_queue._request_download_with_connector(
            connector,
            gid,
            url,
        )

    def ensure_download_request_with_connector(
        self,
        connector: SQLConnector,
        gid: int,
        url: str = "",
    ) -> EnsureDownloadRequestResult:
        self._source_pointer(connector, for_update=True)
        existing = self.get_download_request_with_connector(connector, gid)
        if existing is not None:
            if url and not existing.url:
                connector.execute(
                    """
                    UPDATE catalog_build_removed_gid_requests
                    SET url = %s
                    WHERE request_token = %s AND url = ''
                    """,
                    (url, existing.token),
                )
                connector.execute(
                    """
                    UPDATE todownload_gids
                    SET url = %s
                    WHERE request_token = %s AND url = ''
                    """,
                    (url, existing.token),
                )
                existing = DownloadRequest(existing.gid, url, existing.token)
            return EnsureDownloadRequestResult(existing, False)
        return self._download_queue._ensure_download_request_with_connector(
            connector,
            gid,
            url,
        )

    def _ack_removed_gid(
        self,
        connector: SQLConnector,
        gid: int,
        source_revision: int,
    ) -> None:
        if self._context.sql_type == "mariadb":
            connector.execute(
                """
                INSERT INTO catalog_removed_gid_request_acks (
                    gid, through_source_revision
                ) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    through_source_revision = GREATEST(
                        through_source_revision,
                        VALUES(through_source_revision)
                    )
                """,
                (gid, source_revision),
            )
        else:
            connector.execute(
                """
                INSERT INTO catalog_removed_gid_request_acks (
                    gid, through_source_revision
                ) VALUES (%s, %s)
                ON CONFLICT(gid) DO UPDATE SET
                    through_source_revision = MAX(
                        catalog_removed_gid_request_acks.through_source_revision,
                        excluded.through_source_revision
                    )
                """,
                (gid, source_revision),
            )

    def complete_download_request_with_connector(
        self,
        connector: SQLConnector,
        request: DownloadRequest,
    ) -> bool:
        authority = self.active_authority(connector, for_update=True)
        if self._download_queue._complete_download_request_with_connector(
            connector,
            request,
        ):
            if authority is not None:
                self._ack_removed_gid(connector, request.gid, authority[1])
            return True
        if authority is None:
            return False
        auto = self._effective_auto_request(connector, request.gid)
        if auto is None or auto.token != request.token:
            return False
        self._ack_removed_gid(connector, request.gid, authority[1])
        return True

    def requested_gids(
        self,
        connector: SQLConnector,
        gids: Sequence[int],
    ) -> set[int]:
        result: set[int] = set()
        for gid in gids:
            if self.get_download_request_with_connector(connector, gid) is not None:
                result.add(gid)
        return result

    def request_gallery_deletion_with_connector(
        self,
        connector: SQLConnector,
        gid: int,
    ) -> None:
        if gid <= 0:
            raise ValueError("Gallery GID must be greater than zero.")
        self._source_pointer(connector, for_update=True)
        lock = self._lock_clause(self._context.sql_type)
        row = connector.fetch_one(
            "SELECT request_token FROM todelete_gids WHERE gid = %s" + lock,
            (gid,),
        )
        if row and row[0] is not None:
            token = str(row[0])
            consumed = connector.fetch_one(
                """
                SELECT 1
                FROM catalog_build_deletion_consumptions AS consumption
                JOIN catalog_operational_activations AS activation
                    ON activation.build_id = consumption.build_id
                    AND activation.preparation_id = consumption.preparation_id
                WHERE consumption.gid = %s
                    AND consumption.deletion_request_token = %s
                LIMIT 1
                """,
                (gid, token),
            )
            if not consumed:
                return
        token = uuid4().hex
        if row:
            connector.execute(
                "UPDATE todelete_gids SET request_token = %s WHERE gid = %s",
                (token, gid),
            )
        else:
            connector.execute(
                "INSERT INTO todelete_gids (gid, request_token) VALUES (%s, %s)",
                (gid, token),
            )
        connector.execute("""
            UPDATE catalog_source_revision
            SET deletion_request_generation = deletion_request_generation + 1
            WHERE singleton_id = 1
            """)

    def effective_deletion_gids(self, connector: SQLConnector) -> list[int]:
        if self.active_authority(connector) is None:
            rows = connector.fetch_all("SELECT gid FROM todelete_gids ORDER BY gid")
        else:
            deletion_filter = self._effective_deletion_filter("marker")
            rows = connector.fetch_all(f"""
                SELECT marker.gid
                FROM todelete_gids AS marker
                WHERE {deletion_filter}
                ORDER BY marker.gid
                """)
        return [int(row[0]) for row in rows]

    def cataloged_gids(
        self,
        connector: SQLConnector,
        gids: Sequence[int],
    ) -> set[int]:
        authority = self.active_authority(connector)
        if authority is None:
            placeholders = ", ".join("%s" for _ in gids)
            rows = connector.fetch_all(
                f"SELECT gid FROM galleries_gids WHERE gid IN ({placeholders})",
                tuple(gids),
            )
        else:
            placeholders = ", ".join("%s" for _ in gids)
            rows = connector.fetch_all(
                f"""
                SELECT DISTINCT gid
                FROM catalog_source_galleries
                WHERE build_id = %s AND gid IN ({placeholders})
                """,
                (authority[0], *gids),
            )
        return {int(row[0]) for row in rows}

    def pending_redownload_gids(
        self,
        connector: SQLConnector,
        gids: Sequence[int] | None = None,
    ) -> list[int]:
        authority = self.active_authority(connector)
        if authority is None:
            if gids is None:
                rows = connector.fetch_all("SELECT gid FROM pending_download_gids")
            else:
                placeholders = ", ".join("%s" for _ in gids)
                rows = connector.fetch_all(
                    f"SELECT gid FROM pending_download_gids "
                    f"WHERE gid IN ({placeholders})",
                    tuple(gids),
                )
            return [int(row[0]) for row in rows]
        return self._active_pending_redownload_gids(
            connector,
            authority[0],
            gids=gids,
        )

    def _active_pending_redownload_gids(
        self,
        connector: SQLConnector,
        build_id: str,
        *,
        gids: Sequence[int] | None,
    ) -> list[int]:
        gid_filter = ""
        parameters: tuple[Any, ...] = (build_id,)
        if gids is not None:
            placeholders = ", ".join("%s" for _ in gids)
            gid_filter = f" AND source.gid IN ({placeholders})"
            parameters += tuple(gids)
        exact_name = (
            "BINARY legacy_name.full_name = BINARY source.gallery_name"
            if self._context.sql_type == "mariadb"
            else "CAST(legacy_name.full_name AS BLOB) = "
            "CAST(source.gallery_name AS BLOB)"
        )
        deletion_filter = self._effective_deletion_filter("marker")
        if self._context.sql_type == "mariadb":
            effective_time = """
                GREATEST(
                    source.download_time_utc,
                    COALESCE(runtime.redownload_time_utc, '1970-01-01 00:00:00'),
                    COALESCE(legacy_runtime.time, '1970-01-01 00:00:00')
                )
            """
            temporal = f"""
                {effective_time} <= DATE_SUB(NOW(), INTERVAL 7 DAY)
                AND (
                    (
                        {effective_time} <=
                            DATE_ADD(source.upload_time_utc, INTERVAL 1 YEAR)
                        AND DATE_ADD(source.upload_time_utc, INTERVAL 7 DAY) <= NOW()
                    )
                    OR DATE_ADD(source.download_time_utc, INTERVAL 7 DAY) <=
                        {effective_time}
                )
            """
        else:
            effective_time = """
                MAX(
                    source.download_time_utc,
                    COALESCE(runtime.redownload_time_utc, '1970-01-01 00:00:00'),
                    COALESCE(legacy_runtime.time, '1970-01-01 00:00:00')
                )
            """
            temporal = f"""
                datetime({effective_time}) <= datetime('now', '-7 days')
                AND (
                    (
                        datetime({effective_time}) <=
                            datetime(source.upload_time_utc, '+1 years')
                        AND datetime(source.upload_time_utc, '+7 days') <=
                            datetime('now')
                    )
                    OR datetime(source.download_time_utc, '+7 days') <=
                        datetime({effective_time})
                )
            """
        rows = connector.fetch_all(
            f"""
            SELECT source.gid
            FROM catalog_source_galleries AS source
            LEFT JOIN catalog_gallery_redownload_times AS runtime
                ON runtime.gallery_key = source.gallery_key
            LEFT JOIN galleries_names AS legacy_name
                ON {exact_name}
            LEFT JOIN galleries_redownload_times AS legacy_runtime
                ON legacy_runtime.db_gallery_id = legacy_name.db_gallery_id
            WHERE source.build_id = %s
                {gid_filter}
                AND {temporal}
                AND NOT EXISTS (
                    SELECT 1 FROM removed_galleries_gids AS removed
                    WHERE removed.gid = source.gid
                )
                AND NOT EXISTS (
                    SELECT 1 FROM todelete_gids AS marker
                    WHERE marker.gid = source.gid AND {deletion_filter}
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM catalog_source_galleries AS flagged_source
                    JOIN catalog_build_content_digests AS digest
                        ON digest.build_id = flagged_source.build_id
                        AND digest.gallery_key = flagged_source.gallery_key
                    WHERE flagged_source.build_id = source.build_id
                        AND flagged_source.gid = source.gid
                        AND digest.duplicate_hash_deletion_candidate = 1
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM catalog_source_galleries AS older
                    JOIN catalog_source_galleries AS newer
                        ON newer.build_id = older.build_id
                        AND newer.gid = older.gid
                        AND newer.download_time_utc > older.download_time_utc
                    WHERE older.build_id = source.build_id
                        AND older.gid = source.gid
                )
            GROUP BY source.gid
            ORDER BY MAX(source.upload_time_utc) DESC
            """,
            parameters,
        )
        return [int(row[0]) for row in rows]

    def record_accepted_runtime(
        self,
        connector: SQLConnector,
        gid: int,
    ) -> bool:
        authority = self.active_authority(connector, for_update=True)
        if authority is None:
            return False
        rows = connector.fetch_all(
            """
            SELECT gallery_key, gallery_name
            FROM catalog_source_galleries
            WHERE build_id = %s AND gid = %s
            """,
            (authority[0], gid),
        )
        if not rows:
            return True
        now = self._builds._database_datetime(connector).strftime("%Y-%m-%d %H:%M:%S")
        if self._context.sql_type == "mariadb":
            query = """
                INSERT INTO catalog_gallery_redownload_times (
                    gallery_key, gallery_name, redownload_time_utc
                ) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    gallery_name = VALUES(gallery_name),
                    redownload_time_utc = VALUES(redownload_time_utc)
            """
        else:
            query = """
                INSERT INTO catalog_gallery_redownload_times (
                    gallery_key, gallery_name, redownload_time_utc
                ) VALUES (%s, %s, %s)
                ON CONFLICT(gallery_key) DO UPDATE SET
                    gallery_name = excluded.gallery_name,
                    redownload_time_utc = excluded.redownload_time_utc
            """
        connector.execute_many(
            query,
            [(str(key), str(name), now) for key, name in rows],
        )
        return True
