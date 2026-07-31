__all__ = [
    "DownloadTurn",
    "GalleryIngestPhase",
    "GalleryIngestState",
]


from dataclasses import dataclass
from enum import Enum, StrEnum, auto
from uuid import uuid4

from .repository import BaseRepository
from .sql_connector import DatabaseConfigurationError, SQLConnector

GALLERY_INGEST_STATE_ID = 1
GALLERY_INGEST_STATE_TABLE = "gallery_ingest_state"


class GalleryIngestPhase(StrEnum):
    ready = "READY"
    downloading = "DOWNLOADING"
    ingest_requested = "INGEST_REQUESTED"
    ingesting = "INGESTING"


class _DownloadHandoffResult(Enum):
    accepted = auto()
    already_accepted = auto()
    rejected = auto()


@dataclass(frozen=True, slots=True)
class DownloadTurn:
    generation: int
    owner_token: str
    lease_expires_at: int


@dataclass(frozen=True, slots=True)
class GalleryIngestState:
    phase: GalleryIngestPhase
    generation: int
    completed_generation: int
    owner_token: str | None
    lease_expires_at: int | None
    handoff_generation: int | None
    handoff_owner_token: str | None
    last_transition_at: int


@dataclass(frozen=True, slots=True)
class GalleryIngestTurn:
    generation: int
    owner_token: str
    lease_expires_at: int
    claimed_from_phase: GalleryIngestPhase


class H2HDBGalleryIngestCoordination(BaseRepository):
    def _create_gallery_ingest_state_table(self) -> None:
        match self.config.database.sql_type.lower():
            case "mariadb":
                create_query = f"""
                    CREATE TABLE IF NOT EXISTS {GALLERY_INGEST_STATE_TABLE} (
                        state_id TINYINT UNSIGNED NOT NULL,
                        phase VARCHAR(32) NOT NULL,
                        generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        completed_generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        owner_token VARCHAR(64)
                            CHARACTER SET ascii COLLATE ascii_bin NULL,
                        lease_expires_at BIGINT UNSIGNED NULL,
                        handoff_generation BIGINT UNSIGNED NULL,
                        handoff_owner_token VARCHAR(64)
                            CHARACTER SET ascii COLLATE ascii_bin NULL,
                        last_transition_at BIGINT UNSIGNED NOT NULL,
                        PRIMARY KEY (state_id),
                        CONSTRAINT gallery_ingest_state_singleton
                            CHECK (state_id = {GALLERY_INGEST_STATE_ID}),
                        CONSTRAINT gallery_ingest_state_phase
                            CHECK (
                                phase IN (
                                    '{GalleryIngestPhase.ready.value}',
                                    '{GalleryIngestPhase.downloading.value}',
                                    '{GalleryIngestPhase.ingest_requested.value}',
                                    '{GalleryIngestPhase.ingesting.value}'
                                )
                            ),
                        CONSTRAINT gallery_ingest_generation_order
                            CHECK (completed_generation <= generation),
                        CONSTRAINT gallery_ingest_handoff_pair
                            CHECK (
                                (
                                    handoff_generation IS NULL
                                    AND handoff_owner_token IS NULL
                                )
                                OR (
                                    handoff_generation IS NOT NULL
                                    AND handoff_generation = generation
                                    AND handoff_owner_token IS NOT NULL
                                )
                            ),
                        CONSTRAINT gallery_ingest_phase_fields
                            CHECK (
                                (
                                    phase = '{GalleryIngestPhase.ready.value}'
                                    AND completed_generation = generation
                                    AND owner_token IS NULL
                                    AND lease_expires_at IS NULL
                                )
                                OR (
                                    phase = '{GalleryIngestPhase.downloading.value}'
                                    AND generation = completed_generation + 1
                                    AND owner_token IS NOT NULL
                                    AND lease_expires_at IS NOT NULL
                                    AND handoff_generation IS NULL
                                    AND handoff_owner_token IS NULL
                                )
                                OR (
                                    phase = '{GalleryIngestPhase.ingest_requested.value}'
                                    AND lease_expires_at IS NULL
                                    AND (
                                        (
                                            generation = 0
                                            AND completed_generation = 0
                                            AND owner_token IS NULL
                                            AND handoff_generation IS NULL
                                            AND handoff_owner_token IS NULL
                                        )
                                        OR (
                                            generation = completed_generation + 1
                                            AND owner_token IS NOT NULL
                                            AND handoff_generation = generation
                                            AND handoff_owner_token = owner_token
                                        )
                                    )
                                )
                                OR (
                                    phase = '{GalleryIngestPhase.ingesting.value}'
                                    AND owner_token IS NOT NULL
                                    AND lease_expires_at IS NOT NULL
                                )
                            )
                    )
                """
                seed_query = f"""
                    INSERT IGNORE INTO {GALLERY_INGEST_STATE_TABLE} (
                        state_id,
                        phase,
                        generation,
                        completed_generation,
                        last_transition_at
                    )
                    VALUES (%s, %s, 0, 0, %s)
                """
            case "sqlite":
                create_query = f"""
                    CREATE TABLE IF NOT EXISTS {GALLERY_INGEST_STATE_TABLE} (
                        state_id INTEGER NOT NULL PRIMARY KEY
                            CHECK (state_id = {GALLERY_INGEST_STATE_ID}),
                        phase TEXT NOT NULL
                            CHECK (
                                phase IN (
                                    '{GalleryIngestPhase.ready.value}',
                                    '{GalleryIngestPhase.downloading.value}',
                                    '{GalleryIngestPhase.ingest_requested.value}',
                                    '{GalleryIngestPhase.ingesting.value}'
                                )
                            ),
                        generation INTEGER NOT NULL DEFAULT 0
                            CHECK (generation >= 0),
                        completed_generation INTEGER NOT NULL DEFAULT 0
                            CHECK (
                                completed_generation >= 0
                                AND completed_generation <= generation
                            ),
                        owner_token TEXT NULL,
                        lease_expires_at INTEGER NULL,
                        handoff_generation INTEGER NULL,
                        handoff_owner_token TEXT NULL,
                        last_transition_at INTEGER NOT NULL,
                        CHECK (
                            (
                                handoff_generation IS NULL
                                AND handoff_owner_token IS NULL
                            )
                            OR (
                                handoff_generation IS NOT NULL
                                AND handoff_generation = generation
                                AND handoff_owner_token IS NOT NULL
                            )
                        ),
                        CHECK (
                            (
                                phase = '{GalleryIngestPhase.ready.value}'
                                AND completed_generation = generation
                                AND owner_token IS NULL
                                AND lease_expires_at IS NULL
                            )
                            OR (
                                phase = '{GalleryIngestPhase.downloading.value}'
                                AND generation = completed_generation + 1
                                AND owner_token IS NOT NULL
                                AND lease_expires_at IS NOT NULL
                                AND handoff_generation IS NULL
                                AND handoff_owner_token IS NULL
                            )
                            OR (
                                phase = '{GalleryIngestPhase.ingest_requested.value}'
                                AND lease_expires_at IS NULL
                                AND (
                                    (
                                        generation = 0
                                        AND completed_generation = 0
                                        AND owner_token IS NULL
                                        AND handoff_generation IS NULL
                                        AND handoff_owner_token IS NULL
                                    )
                                    OR (
                                        generation = completed_generation + 1
                                        AND owner_token IS NOT NULL
                                        AND handoff_generation = generation
                                        AND handoff_owner_token = owner_token
                                    )
                                )
                            )
                            OR (
                                phase = '{GalleryIngestPhase.ingesting.value}'
                                AND owner_token IS NOT NULL
                                AND lease_expires_at IS NOT NULL
                            )
                        )
                    )
                """
                seed_query = f"""
                    INSERT OR IGNORE INTO {GALLERY_INGEST_STATE_TABLE} (
                        state_id,
                        phase,
                        generation,
                        completed_generation,
                        last_transition_at
                    )
                    VALUES (%s, %s, 0, 0, %s)
                """
            case _:
                raise ValueError("Unsupported SQL type")

        with self.SQLConnector() as connector:
            connector.execute(create_query)
            now = self._database_time(connector)
            connector.execute(
                seed_query,
                (
                    GALLERY_INGEST_STATE_ID,
                    GalleryIngestPhase.ingest_requested.value,
                    now,
                ),
            )
        self.logger.debug(
            f"Ensured database table exists: name={GALLERY_INGEST_STATE_TABLE}."
        )

    def _database_time(self, connector: SQLConnector) -> int:
        match self.config.database.sql_type.lower():
            case "mariadb":
                row = connector.fetch_one("SELECT UNIX_TIMESTAMP()")
            case "sqlite":
                row = connector.fetch_one("SELECT unixepoch()")
            case _:
                raise ValueError("Unsupported SQL type")
        if not row:
            raise DatabaseConfigurationError(
                "The database did not return its current time."
            )
        return int(row[0])

    @staticmethod
    def _validate_lease_seconds(lease_seconds: int) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero.")

    def _select_state(
        self,
        connector: SQLConnector,
        *,
        for_update: bool = False,
    ) -> GalleryIngestState:
        lock_clause = (
            " FOR UPDATE"
            if for_update and self.config.database.sql_type.lower() == "mariadb"
            else ""
        )
        row = connector.fetch_one(
            f"""
            SELECT
                phase,
                generation,
                completed_generation,
                owner_token,
                lease_expires_at,
                handoff_generation,
                handoff_owner_token,
                last_transition_at
            FROM {GALLERY_INGEST_STATE_TABLE}
            WHERE state_id = %s{lock_clause}
            """,
            (GALLERY_INGEST_STATE_ID,),
        )
        if not row:
            raise DatabaseConfigurationError(
                "Gallery ingest coordination state is missing; "
                "run create_main_tables()."
            )
        return GalleryIngestState(
            phase=GalleryIngestPhase(str(row[0])),
            generation=int(row[1]),
            completed_generation=int(row[2]),
            owner_token=str(row[3]) if row[3] is not None else None,
            lease_expires_at=int(row[4]) if row[4] is not None else None,
            handoff_generation=int(row[5]) if row[5] is not None else None,
            handoff_owner_token=str(row[6]) if row[6] is not None else None,
            last_transition_at=int(row[7]),
        )

    def get_state(self) -> GalleryIngestState:
        with self.SQLConnector() as connector:
            return self._select_state(connector)

    def claim_download_turn(self, *, lease_seconds: int) -> DownloadTurn | None:
        self._validate_lease_seconds(lease_seconds)
        owner_token = uuid4().hex

        with self.SQLConnector() as connector:
            with connector.transaction():
                state = self._select_state(connector, for_update=True)
                if state.phase != GalleryIngestPhase.ready:
                    return None

                now = self._database_time(connector)
                lease_expires_at = now + lease_seconds
                generation = state.generation + 1
                connector.execute(
                    f"""
                    UPDATE {GALLERY_INGEST_STATE_TABLE}
                    SET phase = %s,
                        generation = %s,
                        owner_token = %s,
                        lease_expires_at = %s,
                        handoff_generation = NULL,
                        handoff_owner_token = NULL,
                        last_transition_at = %s
                    WHERE state_id = %s
                    """,
                    (
                        GalleryIngestPhase.downloading.value,
                        generation,
                        owner_token,
                        lease_expires_at,
                        now,
                        GALLERY_INGEST_STATE_ID,
                    ),
                )

        return DownloadTurn(
            generation=generation,
            owner_token=owner_token,
            lease_expires_at=lease_expires_at,
        )

    def renew_download_turn(
        self,
        turn: DownloadTurn,
        *,
        lease_seconds: int,
    ) -> bool:
        self._validate_lease_seconds(lease_seconds)

        with self.SQLConnector() as connector:
            with connector.transaction():
                state = self._select_state(connector, for_update=True)
                now = self._database_time(connector)
                if (
                    state.phase != GalleryIngestPhase.downloading
                    or state.generation != turn.generation
                    or state.owner_token != turn.owner_token
                    or state.lease_expires_at is None
                    or state.lease_expires_at <= now
                ):
                    return False

                connector.execute(
                    f"""
                    UPDATE {GALLERY_INGEST_STATE_TABLE}
                    SET lease_expires_at = %s
                    WHERE state_id = %s
                    """,
                    (now + lease_seconds, GALLERY_INGEST_STATE_ID),
                )
        return True

    def request_gallery_ingest(self, turn: DownloadTurn) -> bool:
        with self.SQLConnector() as connector:
            with connector.transaction():
                result = self._handoff_download_turn_with_connector(connector, turn)
        return result is not _DownloadHandoffResult.rejected

    @staticmethod
    def _handoff_matches(
        state: GalleryIngestState,
        turn: DownloadTurn,
    ) -> bool:
        return (
            state.handoff_generation == turn.generation
            and state.handoff_owner_token == turn.owner_token
        )

    def _handoff_download_turn_with_connector(
        self,
        connector: SQLConnector,
        turn: DownloadTurn,
    ) -> _DownloadHandoffResult:
        state = self._select_state(connector, for_update=True)
        if self._handoff_matches(state, turn):
            return _DownloadHandoffResult.already_accepted
        if (
            state.phase != GalleryIngestPhase.downloading
            or state.generation != turn.generation
            or state.owner_token != turn.owner_token
        ):
            return _DownloadHandoffResult.rejected

        now = self._database_time(connector)
        if state.lease_expires_at is None or state.lease_expires_at <= now:
            return _DownloadHandoffResult.rejected
        connector.execute(
            f"""
            UPDATE {GALLERY_INGEST_STATE_TABLE}
            SET phase = %s,
                lease_expires_at = NULL,
                handoff_generation = %s,
                handoff_owner_token = %s,
                last_transition_at = %s
            WHERE state_id = %s
            """,
            (
                GalleryIngestPhase.ingest_requested.value,
                turn.generation,
                turn.owner_token,
                now,
                GALLERY_INGEST_STATE_ID,
            ),
        )
        return _DownloadHandoffResult.accepted

    @staticmethod
    def _lease_is_expired(state: GalleryIngestState, *, now: int) -> bool:
        return state.lease_expires_at is None or state.lease_expires_at <= now

    def claim_gallery_ingest(
        self,
        *,
        lease_seconds: int,
        periodic_scan: bool,
    ) -> GalleryIngestTurn | None:
        self._validate_lease_seconds(lease_seconds)
        owner_token = uuid4().hex

        with self.SQLConnector() as connector:
            with connector.transaction():
                state = self._select_state(connector, for_update=True)
                now = self._database_time(connector)
                claimable = (
                    state.phase == GalleryIngestPhase.ingest_requested
                    or (
                        state.phase == GalleryIngestPhase.downloading
                        and self._lease_is_expired(state, now=now)
                    )
                    or (
                        state.phase == GalleryIngestPhase.ingesting
                        and self._lease_is_expired(state, now=now)
                    )
                    or (state.phase == GalleryIngestPhase.ready and periodic_scan)
                )
                if not claimable:
                    return None

                lease_expires_at = now + lease_seconds
                connector.execute(
                    f"""
                    UPDATE {GALLERY_INGEST_STATE_TABLE}
                    SET phase = %s,
                        owner_token = %s,
                        lease_expires_at = %s,
                        last_transition_at = %s
                    WHERE state_id = %s
                    """,
                    (
                        GalleryIngestPhase.ingesting.value,
                        owner_token,
                        lease_expires_at,
                        now,
                        GALLERY_INGEST_STATE_ID,
                    ),
                )

        return GalleryIngestTurn(
            generation=state.generation,
            owner_token=owner_token,
            lease_expires_at=lease_expires_at,
            claimed_from_phase=state.phase,
        )

    def renew_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
    ) -> bool:
        return (
            self.renew_gallery_ingest_lease(
                turn,
                lease_seconds=lease_seconds,
            )
            is not None
        )

    def renew_gallery_ingest_lease(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int | None:
        self._validate_lease_seconds(lease_seconds)
        if sqlite_busy_timeout_ms is not None and sqlite_busy_timeout_ms < 0:
            raise ValueError("sqlite_busy_timeout_ms must not be negative.")
        with self.SQLConnector() as connector:
            if (
                sqlite_busy_timeout_ms is not None
                and self.config.database.sql_type.lower() == "sqlite"
            ):
                connector.execute(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms:d}")
            with connector.transaction():
                state = self._select_state(connector, for_update=True)
                now = self._database_time(connector)
                if (
                    state.phase != GalleryIngestPhase.ingesting
                    or state.generation != turn.generation
                    or state.owner_token != turn.owner_token
                    or state.lease_expires_at is None
                    or state.lease_expires_at <= now
                ):
                    return None
                lease_expires_at = now + lease_seconds
                connector.execute(
                    f"""
                    UPDATE {GALLERY_INGEST_STATE_TABLE}
                    SET lease_expires_at = %s
                    WHERE state_id = %s
                    """,
                    (lease_expires_at, GALLERY_INGEST_STATE_ID),
                )
        return lease_expires_at

    def complete_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        allow_expired_sqlite_lease: bool = False,
    ) -> bool:
        with self.SQLConnector() as connector:
            with connector.transaction():
                state = self._select_state(connector, for_update=True)
                now = self._database_time(connector)
                lease_is_live = (
                    state.lease_expires_at is not None and state.lease_expires_at > now
                )
                sqlite_exclusive_maintenance_fenced = (
                    allow_expired_sqlite_lease
                    and self.config.database.sql_type.lower() == "sqlite"
                )
                if (
                    state.phase != GalleryIngestPhase.ingesting
                    or state.generation != turn.generation
                    or state.owner_token != turn.owner_token
                    or (not lease_is_live and not sqlite_exclusive_maintenance_fenced)
                ):
                    return False
                connector.execute(
                    f"""
                    UPDATE {GALLERY_INGEST_STATE_TABLE}
                    SET phase = %s,
                        completed_generation = %s,
                        owner_token = NULL,
                        lease_expires_at = NULL,
                        last_transition_at = %s
                    WHERE state_id = %s
                    """,
                    (
                        GalleryIngestPhase.ready.value,
                        turn.generation,
                        now,
                        GALLERY_INGEST_STATE_ID,
                    ),
                )
        return True
