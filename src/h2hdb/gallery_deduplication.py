import datetime
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list
from .table_gids import H2HDBGalleriesIDs
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles

ALREADY_UPLOADED_TAG_VALUE = "already uploaded"
ALREADY_UPLOADED_BATCH_SIZE = 500
HASH_OWNER_BATCH_SIZE = 500

PriorityKey = tuple[bool, int, datetime.datetime]


@dataclass(frozen=True)
class ContentClaim:
    """A gallery's final effective-content state for one reconciliation pass.

    ``sha256`` is ``None`` when no files remain after applying the final
    exclusion set. Such a gallery does not participate in content ownership
    and remains eligible for a CBZ.
    """

    db_gallery_id: int
    sha256: bytes | None
    priority_key: PriorityKey


@dataclass(frozen=True)
class ReconciliationResult:
    """Stable content ownership and its direct loser-to-winner relation."""

    owner_hash_by_db_gallery_id: dict[int, bytes]
    duplicate_of_by_db_gallery_id: dict[int, int]
    eligible_db_gallery_ids: frozenset[int]

    @property
    def losing_db_gallery_ids(self) -> frozenset[int]:
        return frozenset(self.duplicate_of_by_db_gallery_id)


def select_reconciliation(
    claims: Sequence[ContentClaim],
    existing_owner_hash_by_db_gallery_id: Mapping[int, bytes],
) -> ReconciliationResult:
    """Purely select the stable owner of every effective content hash.

    Highest ``priority_key`` wins. At an exact priority tie, the incumbent
    remains owner only when it still claims that same hash; otherwise the
    greatest ``db_gallery_id`` wins. Every contentful non-winner points
    directly to its group's final winner. Contentless galleries own no hash,
    have no duplicate warning, and remain CBZ-eligible.
    """

    claim_by_id: dict[int, ContentClaim] = {}
    claims_by_hash: defaultdict[bytes, list[ContentClaim]] = defaultdict(list)
    for claim in claims:
        if claim.db_gallery_id in claim_by_id:
            raise ValueError(
                f"Duplicate content claim for gallery {claim.db_gallery_id}."
            )
        claim_by_id[claim.db_gallery_id] = claim
        if claim.sha256 is not None:
            claims_by_hash[claim.sha256].append(claim)

    valid_incumbent_by_hash: dict[bytes, int] = {}
    for db_gallery_id, sha256 in existing_owner_hash_by_db_gallery_id.items():
        existing_claim = claim_by_id.get(db_gallery_id)
        if existing_claim is None or existing_claim.sha256 != sha256:
            continue
        if sha256 in valid_incumbent_by_hash:
            raise ValueError(f"Multiple existing owners for hash {sha256.hex()}.")
        valid_incumbent_by_hash[sha256] = db_gallery_id

    owner_hash_by_id: dict[int, bytes] = {}
    duplicate_of_by_id: dict[int, int] = {}
    for sha256, hash_claims in claims_by_hash.items():
        highest_priority = max(claim.priority_key for claim in hash_claims)
        top_claims = [
            claim for claim in hash_claims if claim.priority_key == highest_priority
        ]
        incumbent_id = valid_incumbent_by_hash.get(sha256)
        if incumbent_id is not None and any(
            claim.db_gallery_id == incumbent_id for claim in top_claims
        ):
            winner_id = incumbent_id
        else:
            winner_id = max(claim.db_gallery_id for claim in top_claims)

        owner_hash_by_id[winner_id] = sha256
        for claim in hash_claims:
            if claim.db_gallery_id != winner_id:
                duplicate_of_by_id[claim.db_gallery_id] = winner_id

    eligible_ids = frozenset(claim_by_id).difference(duplicate_of_by_id)
    return ReconciliationResult(
        owner_hash_by_db_gallery_id=owner_hash_by_id,
        duplicate_of_by_db_gallery_id=duplicate_of_by_id,
        eligible_db_gallery_ids=eligible_ids,
    )


class H2HDBGalleryDeduplication(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        gallery_ids: H2HDBGalleriesIDs,
        gallery_times: H2HDBTimes,
        gallery_titles: H2HDBGalleriesTitles,
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids
        self.gallery_times = gallery_times
        self.gallery_titles = gallery_titles

    def _create_gallery_content_hashes_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_content_hashes"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            sha256        BINARY(32)   NOT NULL,
                            UNIQUE INDEX (sha256)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            sha256        BLOB    NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    connector.execute(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_sha256 "
                        f"ON {table_name}(sha256)"
                    )

        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def _create_gallery_duplicate_warnings_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_duplicate_warnings"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            FOREIGN KEY (duplicate_of_db_gallery_id)
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id              INT UNSIGNED NOT NULL,
                            duplicate_of_db_gallery_id INT UNSIGNED NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            duplicate_of_db_gallery_id INTEGER NOT NULL
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE
                        )
                    """
            connector.execute(query)
        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def _create_gallery_duplicate_warnings_names_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_duplicate_warnings_names"
            query = f"""
                CREATE VIEW IF NOT EXISTS {table_name} AS
                SELECT
                    duplicate_names.full_name AS duplicate_name,
                    kept_names.full_name AS kept_name
                FROM gallery_duplicate_warnings
                    JOIN galleries_names AS duplicate_names
                        ON duplicate_names.db_gallery_id = gallery_duplicate_warnings.db_gallery_id
                    JOIN galleries_names AS kept_names
                        ON kept_names.db_gallery_id
                            = gallery_duplicate_warnings.duplicate_of_db_gallery_id
            """
            connector.execute(query)
        self.logger.debug(f"Ensured database view exists: name={table_name}.")

    def _get_hash_owner(self, sha256: bytes) -> int | None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT db_gallery_id FROM gallery_content_hashes WHERE sha256 = %s
            """
            query_result = connector.fetch_one(select_query, (sha256,))
        return int(query_result[0]) if query_result else None

    def _upsert_hash(self, table_name: str, db_gallery_id: int, sha256: bytes) -> None:
        with self.SQLConnector() as connector:
            select_query = f"SELECT 1 FROM {table_name} WHERE db_gallery_id = %s"
            already_has_a_hash = connector.fetch_one(select_query, (db_gallery_id,))
            if already_has_a_hash:
                query = f"UPDATE {table_name} SET sha256 = %s WHERE db_gallery_id = %s"
                connector.execute(query, (sha256, db_gallery_id))
            else:
                query = f"""
                    INSERT INTO {table_name} (db_gallery_id, sha256) VALUES (%s, %s)
                """
                connector.execute(query, (db_gallery_id, sha256))

    def _claim_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        self._upsert_hash("gallery_content_hashes", db_gallery_id, sha256)

    def get_duplicate_warning_db_gallery_ids(self) -> list[int]:
        """db_gallery_id of every gallery currently losing its content-hash
        race to another gallery -- i.e. galleries that shouldn't have a CBZ."""
        with self.SQLConnector() as connector:
            query = "SELECT db_gallery_id FROM gallery_duplicate_warnings"
            query_result = connector.fetch_all(query)
        return [int(row[0]) for row in query_result]

    def _record_duplicate_warning(
        self, db_gallery_id: int, duplicate_of_db_gallery_id: int
    ) -> None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT 1 FROM gallery_duplicate_warnings WHERE db_gallery_id = %s
            """
            already_warned = connector.fetch_one(select_query, (db_gallery_id,))
            if already_warned:
                return

            insert_query = """
                INSERT INTO gallery_duplicate_warnings
                    (db_gallery_id, duplicate_of_db_gallery_id)
                VALUES (%s, %s)
            """
            connector.execute(insert_query, (db_gallery_id, duplicate_of_db_gallery_id))

    def get_already_uploaded_flags_by_db_gallery_ids(
        self, db_gallery_ids: list[int]
    ) -> dict[int, bool]:
        if not db_gallery_ids:
            return {}

        flagged_ids: set[int] = set()
        with self.SQLConnector() as connector:
            for batch in chunk_list(db_gallery_ids, ALREADY_UPLOADED_BATCH_SIZE):
                select_query = f"""
                    SELECT DISTINCT galleries_tags.db_gallery_id
                    FROM galleries_tags
                        JOIN galleries_tag_pairs_dbids
                            ON galleries_tags.db_tag_pair_id
                                = galleries_tag_pairs_dbids.db_tag_pair_id
                    WHERE galleries_tag_pairs_dbids.tag_value = %s
                        AND galleries_tags.db_gallery_id IN (
                            {", ".join(["%s"] * len(batch))}
                        )
                """
                query_result = connector.fetch_all(
                    select_query, (ALREADY_UPLOADED_TAG_VALUE, *batch)
                )
                flagged_ids.update(int(row[0]) for row in query_result)
        return {
            db_gallery_id: db_gallery_id in flagged_ids
            for db_gallery_id in db_gallery_ids
        }

    def _get_priority_key(self, db_gallery_id: int) -> PriorityKey:
        """Rank a gallery against a hash-collision opponent.

        Lexicographic: a gallery tagged "already uploaded" always loses
        regardless of the other two fields; otherwise the longer title wins;
        only when both tie does the more recent download_time decide.
        """
        has_already_uploaded_tag = self.get_already_uploaded_flags_by_db_gallery_ids(
            [db_gallery_id]
        )[db_gallery_id]
        title = self.gallery_titles.get_titles_by_db_gallery_ids([db_gallery_id])[
            db_gallery_id
        ]
        download_time = self.gallery_times.get_download_times_by_db_gallery_ids(
            [db_gallery_id]
        )[db_gallery_id]
        return (not has_already_uploaded_tag, len(title), download_time)

    def _get_all_hashes(self, table_name: str) -> dict[int, bytes]:
        with self.SQLConnector() as connector:
            query_result = connector.fetch_all(
                f"SELECT db_gallery_id, sha256 FROM {table_name}"
            )
        return {
            int(db_gallery_id): bytes(sha256) for db_gallery_id, sha256 in query_result
        }

    def _get_all_duplicate_warnings(self) -> dict[int, int]:
        with self.SQLConnector() as connector:
            query_result = connector.fetch_all("""
                SELECT db_gallery_id, duplicate_of_db_gallery_id
                FROM gallery_duplicate_warnings
                """)
        return {
            int(db_gallery_id): int(duplicate_of_db_gallery_id)
            for db_gallery_id, duplicate_of_db_gallery_id in query_result
        }

    def _delete_rows_by_db_gallery_ids(
        self, table_name: str, db_gallery_ids: list[int]
    ) -> None:
        if not db_gallery_ids:
            return

        with self.SQLConnector() as connector:
            for batch in chunk_list(db_gallery_ids, HASH_OWNER_BATCH_SIZE):
                connector.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE db_gallery_id IN ({", ".join(["%s"] * len(batch))})
                    """,
                    tuple(batch),
                )

    def _sync_hashes(
        self,
        table_name: str,
        existing: Mapping[int, bytes],
        desired: Mapping[int, bytes],
    ) -> None:
        changed_ids = sorted(
            db_gallery_id
            for db_gallery_id in existing.keys() | desired.keys()
            if existing.get(db_gallery_id) != desired.get(db_gallery_id)
        )
        self._delete_rows_by_db_gallery_ids(table_name, changed_ids)
        self._insert_rows(
            table_name,
            ["db_gallery_id", "sha256"],
            [
                (db_gallery_id, desired[db_gallery_id])
                for db_gallery_id in changed_ids
                if db_gallery_id in desired
            ],
        )

    def _sync_duplicate_warnings(
        self,
        existing: Mapping[int, int],
        desired: Mapping[int, int],
    ) -> None:
        changed_ids = sorted(
            db_gallery_id
            for db_gallery_id in existing.keys() | desired.keys()
            if existing.get(db_gallery_id) != desired.get(db_gallery_id)
        )
        self._delete_rows_by_db_gallery_ids("gallery_duplicate_warnings", changed_ids)
        self._insert_rows(
            "gallery_duplicate_warnings",
            ["db_gallery_id", "duplicate_of_db_gallery_id"],
            [
                (db_gallery_id, desired[db_gallery_id])
                for db_gallery_id in changed_ids
                if db_gallery_id in desired
            ],
        )

    def reconcile_many(self, claims: list[ContentClaim]) -> ReconciliationResult:
        """Globally reconcile content ownership and duplicate warnings.

        ``claims`` must contain exactly one entry for every live gallery;
        contentless galleries use ``sha256=None``.

        Writes are exact, not upserts: stale and orphaned rows are removed,
        changed warning targets are replaced, and all conflicting content-hash
        rows are deleted before their stable replacements are inserted.
        Consequently a later successful call repairs any partially-applied
        state left by an interrupted earlier call.
        """

        claim_ids = {claim.db_gallery_id for claim in claims}
        if len(claim_ids) != len(claims):
            raise ValueError("Each gallery must have exactly one content claim.")

        existing_content_hashes = self._get_all_hashes("gallery_content_hashes")
        result = select_reconciliation(claims, existing_content_hashes)

        existing_warnings = self._get_all_duplicate_warnings()

        self._sync_hashes(
            "gallery_content_hashes",
            existing_content_hashes,
            result.owner_hash_by_db_gallery_id,
        )
        self._sync_duplicate_warnings(
            existing_warnings,
            result.duplicate_of_by_db_gallery_id,
        )
        return result
