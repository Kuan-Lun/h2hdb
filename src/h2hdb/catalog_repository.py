__all__ = ["CatalogRevisionNotFoundError"]

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from .domain import (
    CatalogArtifact,
    CatalogContributor,
    CatalogPage,
    CatalogPublication,
    CatalogRevision,
    CatalogSubject,
)
from .repository import BaseRepository, RepositoryContext
from .sql_connector import SQLConnector

MAX_PAGE_SIZE = 200
LOOKUP_CHUNK_SIZE = 500


class CatalogRevisionNotFoundError(LookupError):
    def __init__(self, revision: int) -> None:
        self.revision = revision
        super().__init__(f"Catalog revision {revision} does not exist")


@dataclass(frozen=True, slots=True)
class _PreparedRevision:
    revision: CatalogRevision
    created: bool


def _stable_key(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _projection_datetime(value: datetime) -> str:
    """Serialize every projection timestamp with explicit timezone context."""

    aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value
    return aware.isoformat()


class CatalogProjectionRepository(BaseRepository):
    def __init__(self, context: RepositoryContext) -> None:
        super().__init__(context)

    def _prepare_revision_with_connector(
        self,
        connector: SQLConnector,
        publications: Sequence[CatalogPublication],
    ) -> _PreparedRevision:
        snapshot = tuple(publications)
        self._validate_snapshot(snapshot)
        published_at = datetime.now(UTC)
        lock_clause = " FOR UPDATE" if self._context.sql_type == "mariadb" else ""
        row = connector.fetch_one("""
            SELECT current_revision
            FROM catalog_revision
            WHERE singleton_id = 1
            """ + lock_clause)
        if not row:
            raise RuntimeError("catalog_revision singleton is missing")
        current_revision = self._get_revision(connector, int(row[0]))
        if self._revision_matches_snapshot(
            connector,
            current_revision.revision,
            snapshot,
        ):
            return _PreparedRevision(current_revision, created=False)
        revision = current_revision.revision + 1
        self._insert_publications(connector, revision, snapshot)
        connector.execute(
            """
            INSERT INTO catalog_revision_history (
                revision,
                published_at,
                publication_count
            ) VALUES (%s, %s, %s)
            """,
            (revision, published_at.isoformat(), len(snapshot)),
        )
        return _PreparedRevision(
            CatalogRevision(revision, published_at, len(snapshot)),
            created=True,
        )

    @staticmethod
    def _advance_revision_pointer_with_connector(
        connector: SQLConnector,
        revision: CatalogRevision,
    ) -> None:
        connector.execute(
            """
            UPDATE catalog_revision
            SET current_revision = %s,
                published_at = %s,
                publication_count = %s
            WHERE singleton_id = 1
            """,
            (
                revision.revision,
                revision.published_at.isoformat(),
                revision.publication_count,
            ),
        )

    def get_catalog_revision(self, revision: int | None = None) -> CatalogRevision:
        if revision is not None and revision < 0:
            raise ValueError("revision must not be negative")
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                return self._get_revision(connector, revision)

    def list_publications(
        self,
        *,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        revision: CatalogRevision | None = None,
        require_artifact: bool = False,
    ) -> CatalogPage:
        if offset < 0:
            raise ValueError("offset must not be negative")
        if not 1 <= limit <= MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        normalized_query = query.strip() if query is not None else ""
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                selected_revision = self._get_revision(
                    connector,
                    None if revision is None else revision.revision,
                )
                where, parameters = self._search_where(
                    selected_revision.revision,
                    normalized_query,
                    require_artifact=require_artifact,
                )
                total_row = connector.fetch_one(
                    f"SELECT COUNT(*) FROM catalog_publications AS p WHERE {where}",
                    parameters,
                )
                total = int(total_row[0]) if total_row else 0
                rows = connector.fetch_all(
                    f"""
                    SELECT
                        p.publication_key,
                        p.publication_id,
                        p.gid,
                        p.title,
                        p.source_title,
                        p.source_gallery_name,
                        p.content_sha256,
                        p.sort_title,
                        p.summary,
                        p.language,
                        p.published_at,
                        p.modified_at,
                        p.redownload_required
                    FROM catalog_publications AS p
                    WHERE {where}
                    ORDER BY p.sort_title, p.publication_key
                    LIMIT %s OFFSET %s
                    """,
                    (*parameters, limit, offset),
                )
                publications = self._hydrate_publications(
                    connector,
                    selected_revision.revision,
                    rows,
                )
        return CatalogPage(selected_revision, publications, offset, limit, total)

    def get_publication(
        self,
        publication_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogPublication | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                selected_revision = self._get_revision(
                    connector,
                    None if revision is None else revision.revision,
                )
                rows = connector.fetch_all(
                    """
                    SELECT
                        publication_key,
                        publication_id,
                        gid,
                        title,
                        source_title,
                        source_gallery_name,
                        content_sha256,
                        sort_title,
                        summary,
                        language,
                        published_at,
                        modified_at,
                        redownload_required
                    FROM catalog_publications
                    WHERE revision = %s AND publication_key = %s
                    """,
                    (selected_revision.revision, _stable_key(publication_id)),
                )
                publications = self._hydrate_publications(
                    connector,
                    selected_revision.revision,
                    rows,
                )
        return publications[0] if publications else None

    def get_publications_by_artifact_names(
        self,
        names: Sequence[str],
        *,
        revision: CatalogRevision | None = None,
    ) -> Mapping[str, CatalogPublication]:
        ordered_names = tuple(dict.fromkeys(names))
        if not ordered_names:
            return {}
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                selected_revision = self._get_revision(
                    connector,
                    None if revision is None else revision.revision,
                )
                artifact_rows: list[tuple[object, ...]] = []
                for start in range(0, len(ordered_names), LOOKUP_CHUNK_SIZE):
                    chunk = ordered_names[start : start + LOOKUP_CHUNK_SIZE]
                    keys = tuple(_stable_key(name) for name in chunk)
                    placeholders = ", ".join("%s" for _ in keys)
                    artifact_rows.extend(
                        connector.fetch_all(
                            f"""
                            SELECT name, publication_key
                            FROM catalog_artifacts
                            WHERE revision = %s
                                AND artifact_name_key IN ({placeholders})
                            """,
                            (selected_revision.revision, *keys),
                        )
                    )
                publication_keys = tuple(
                    dict.fromkeys(str(row[1]) for row in artifact_rows)
                )
                publication_rows = self._publication_rows_by_keys(
                    connector,
                    selected_revision.revision,
                    publication_keys,
                )
                publications = self._hydrate_publications(
                    connector,
                    selected_revision.revision,
                    publication_rows,
                )
        by_key = {
            _stable_key(publication.publication_id): publication
            for publication in publications
        }
        return {
            str(name): by_key[str(publication_key)]
            for name, publication_key in artifact_rows
            if str(publication_key) in by_key
        }

    def get_artifact(
        self,
        artifact_id: str,
        *,
        revision: CatalogRevision | None = None,
    ) -> CatalogArtifact | None:
        with self.SQLConnector() as connector:
            with connector.read_transaction():
                selected_revision = self._get_revision(
                    connector,
                    None if revision is None else revision.revision,
                )
                row = connector.fetch_one(
                    """
                    SELECT
                        artifact_id,
                        name,
                        location,
                        media_type,
                        size_bytes,
                        sha256,
                        modified_at
                    FROM catalog_artifacts
                    WHERE revision = %s AND artifact_key = %s
                    """,
                    (selected_revision.revision, _stable_key(artifact_id)),
                )
        return self._artifact_from_row(row) if row else None

    @staticmethod
    def _validate_snapshot(publications: tuple[CatalogPublication, ...]) -> None:
        publication_ids = [publication.publication_id for publication in publications]
        gids = [publication.gid for publication in publications]
        artifact_ids = [
            artifact.artifact_id
            for publication in publications
            for artifact in publication.artifacts
        ]
        artifact_names = [
            artifact.name
            for publication in publications
            for artifact in publication.artifacts
        ]
        for label, values in (
            ("publication ID", publication_ids),
            ("GID", gids),
            ("artifact ID", artifact_ids),
            ("artifact name", artifact_names),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"Catalog snapshot contains a duplicate {label}")

    def _insert_publications(
        self,
        connector: SQLConnector,
        revision: int,
        publications: tuple[CatalogPublication, ...],
    ) -> None:
        publication_rows: list[tuple[object, ...]] = []
        contributor_rows: list[tuple[object, ...]] = []
        subject_rows: list[tuple[object, ...]] = []
        artifact_rows: list[tuple[object, ...]] = []
        for publication in publications:
            publication_key = _stable_key(publication.publication_id)
            publication_rows.append(
                (
                    revision,
                    publication_key,
                    publication.publication_id,
                    publication.gid,
                    publication.title,
                    publication.source_title,
                    publication.source_gallery_name,
                    publication.content_sha256,
                    publication.sort_title,
                    publication.summary,
                    publication.language,
                    _projection_datetime(publication.published_at),
                    _projection_datetime(publication.modified_at),
                    publication.redownload_required,
                )
            )
            contributor_rows.extend(
                (
                    revision,
                    publication_key,
                    position,
                    contributor.name,
                    contributor.role,
                    contributor.sort_as,
                )
                for position, contributor in enumerate(publication.contributors)
            )
            subject_rows.extend(
                (
                    revision,
                    publication_key,
                    position,
                    subject.name,
                    subject.scheme,
                    subject.code,
                )
                for position, subject in enumerate(publication.subjects)
            )
            artifact_rows.extend(
                (
                    revision,
                    _stable_key(artifact.artifact_id),
                    _stable_key(artifact.name),
                    publication_key,
                    artifact.artifact_id,
                    artifact.name,
                    str(artifact.location),
                    artifact.media_type,
                    artifact.size_bytes,
                    artifact.sha256,
                    _projection_datetime(artifact.modified_at),
                )
                for artifact in publication.artifacts
            )
        if publication_rows:
            connector.execute_many(
                """
                INSERT INTO catalog_publications (
                    revision,
                    publication_key,
                    publication_id,
                    gid,
                    title,
                    source_title,
                    source_gallery_name,
                    content_sha256,
                    sort_title,
                    summary,
                    language,
                    published_at,
                    modified_at,
                    redownload_required
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                publication_rows,
            )
        if contributor_rows:
            connector.execute_many(
                """
                INSERT INTO catalog_contributors (
                    revision, publication_key, position, name, role, sort_as
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                contributor_rows,
            )
        if subject_rows:
            connector.execute_many(
                """
                INSERT INTO catalog_subjects (
                    revision, publication_key, position, name, scheme, code
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                subject_rows,
            )
        if artifact_rows:
            connector.execute_many(
                """
                INSERT INTO catalog_artifacts (
                    revision,
                    artifact_key,
                    artifact_name_key,
                    publication_key,
                    artifact_id,
                    name,
                    location,
                    media_type,
                    size_bytes,
                    sha256,
                    modified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                artifact_rows,
            )

    @staticmethod
    def _get_revision(
        connector: SQLConnector,
        revision: int | None = None,
    ) -> CatalogRevision:
        if revision is None:
            row = connector.fetch_one("""
                SELECT
                    history.revision,
                    history.published_at,
                    history.publication_count
                FROM catalog_revision AS current
                JOIN catalog_revision_history AS history
                    ON history.revision = current.current_revision
                WHERE current.singleton_id = 1
                """)
        else:
            row = connector.fetch_one(
                """
                SELECT revision, published_at, publication_count
                FROM catalog_revision_history
                WHERE revision = %s
                """,
                (revision,),
            )
        if not row:
            if revision is not None:
                raise CatalogRevisionNotFoundError(revision)
            raise RuntimeError(
                "catalog_revision singleton or its history entry is missing"
            )
        return CatalogRevision(int(row[0]), _parse_datetime(row[1]), int(row[2]))

    @staticmethod
    def _search_where(
        revision: int,
        query: str,
        *,
        require_artifact: bool,
    ) -> tuple[str, tuple[object, ...]]:
        parameters: tuple[object, ...] = (revision,)
        where = "p.revision = %s"
        if query:
            escaped = (
                query.casefold()
                .replace("!", "!!")
                .replace("%", "!%")
                .replace("_", "!_")
            )
            pattern = f"%{escaped}%"
            where += """ AND (
                LOWER(p.title) LIKE %s ESCAPE '!'
                OR LOWER(p.summary) LIKE %s ESCAPE '!'
                OR LOWER(p.source_title) LIKE %s ESCAPE '!'
                OR LOWER(p.publication_id) LIKE %s ESCAPE '!'
                OR EXISTS (
                    SELECT 1 FROM catalog_contributors AS c
                    WHERE c.revision = p.revision
                        AND c.publication_key = p.publication_key
                        AND LOWER(c.name) LIKE %s ESCAPE '!'
                )
                OR EXISTS (
                    SELECT 1 FROM catalog_subjects AS s
                    WHERE s.revision = p.revision
                        AND s.publication_key = p.publication_key
                        AND LOWER(s.name) LIKE %s ESCAPE '!'
                )
            )"""
            parameters += (pattern, pattern, pattern, pattern, pattern, pattern)
        if require_artifact:
            where += """ AND EXISTS (
                SELECT 1 FROM catalog_artifacts AS required_artifact
                WHERE required_artifact.revision = p.revision
                    AND required_artifact.publication_key = p.publication_key
            )"""
        return where, parameters

    def _publication_rows_by_keys(
        self,
        connector: SQLConnector,
        revision: int,
        publication_keys: tuple[str, ...],
    ) -> list[tuple[object, ...]]:
        if not publication_keys:
            return []
        rows: list[tuple[object, ...]] = []
        for start in range(0, len(publication_keys), LOOKUP_CHUNK_SIZE):
            chunk = publication_keys[start : start + LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join("%s" for _ in chunk)
            rows.extend(
                connector.fetch_all(
                    f"""
                    SELECT
                        publication_key,
                        publication_id,
                        gid,
                        title,
                        source_title,
                        source_gallery_name,
                        content_sha256,
                        sort_title,
                        summary,
                        language,
                        published_at,
                        modified_at,
                        redownload_required
                    FROM catalog_publications
                    WHERE revision = %s
                        AND publication_key IN ({placeholders})
                    """,
                    (revision, *chunk),
                )
            )
        return rows

    def _hydrate_publications(
        self,
        connector: SQLConnector,
        revision: int,
        rows: Sequence[tuple[object, ...]],
    ) -> tuple[CatalogPublication, ...]:
        if not rows:
            return ()
        keys = tuple(str(row[0]) for row in rows)
        placeholders = ", ".join("%s" for _ in keys)
        parameters: tuple[object, ...] = (revision, *keys)
        contributor_rows = connector.fetch_all(
            f"""
            SELECT publication_key, name, role, sort_as
            FROM catalog_contributors
            WHERE revision = %s AND publication_key IN ({placeholders})
            ORDER BY publication_key, position
            """,
            parameters,
        )
        subject_rows = connector.fetch_all(
            f"""
            SELECT publication_key, name, scheme, code
            FROM catalog_subjects
            WHERE revision = %s AND publication_key IN ({placeholders})
            ORDER BY publication_key, position
            """,
            parameters,
        )
        artifact_rows = connector.fetch_all(
            f"""
            SELECT
                publication_key,
                artifact_id,
                name,
                location,
                media_type,
                size_bytes,
                sha256,
                modified_at
            FROM catalog_artifacts
            WHERE revision = %s AND publication_key IN ({placeholders})
            ORDER BY publication_key, artifact_id
            """,
            parameters,
        )
        contributors: dict[str, list[CatalogContributor]] = {}
        for key, name, role, sort_as in contributor_rows:
            contributors.setdefault(str(key), []).append(
                CatalogContributor(
                    name=str(name),
                    role=str(role),
                    sort_as=None if sort_as is None else str(sort_as),
                )
            )
        subjects: dict[str, list[CatalogSubject]] = {}
        for key, name, scheme, code in subject_rows:
            subjects.setdefault(str(key), []).append(
                CatalogSubject(
                    name=str(name),
                    scheme=None if scheme is None else str(scheme),
                    code=None if code is None else str(code),
                )
            )
        artifacts: dict[str, list[CatalogArtifact]] = {}
        for row in artifact_rows:
            artifacts.setdefault(str(row[0]), []).append(
                self._artifact_from_row(row[1:])
            )
        return tuple(
            CatalogPublication(
                publication_id=str(row[1]),
                gid=int(str(row[2])),
                title=str(row[3]),
                source_title=str(row[4]),
                source_gallery_name=str(row[5]),
                content_sha256=None if row[6] is None else str(row[6]),
                sort_title=str(row[7]),
                summary=str(row[8]),
                language=str(row[9]),
                published_at=_parse_datetime(row[10]),
                modified_at=_parse_datetime(row[11]),
                contributors=tuple(contributors.get(str(row[0]), ())),
                subjects=tuple(subjects.get(str(row[0]), ())),
                artifacts=tuple(artifacts.get(str(row[0]), ())),
                redownload_required=bool(row[12]),
            )
            for row in rows
        )

    def _revision_matches_snapshot(
        self,
        connector: SQLConnector,
        revision: int,
        publications: tuple[CatalogPublication, ...],
    ) -> bool:
        key_rows = connector.fetch_all(
            """
            SELECT publication_key
            FROM catalog_publications
            WHERE revision = %s
            """,
            (revision,),
        )
        keys = tuple(str(row[0]) for row in key_rows)
        if len(keys) != len(publications):
            return False

        persisted: list[CatalogPublication] = []
        for start in range(0, len(keys), LOOKUP_CHUNK_SIZE):
            chunk = keys[start : start + LOOKUP_CHUNK_SIZE]
            rows = self._publication_rows_by_keys(connector, revision, chunk)
            persisted.extend(self._hydrate_publications(connector, revision, rows))
        return self._snapshot_identity(persisted) == self._snapshot_identity(
            publications
        )

    @staticmethod
    def _snapshot_identity(
        publications: Sequence[CatalogPublication],
    ) -> tuple[object, ...]:
        def publication_identity(publication: CatalogPublication) -> tuple[object, ...]:
            return (
                publication.publication_id,
                publication.gid,
                publication.title,
                publication.source_title,
                publication.source_gallery_name,
                publication.content_sha256,
                publication.sort_title,
                publication.summary,
                publication.language,
                _projection_datetime(publication.published_at),
                _projection_datetime(publication.modified_at),
                tuple(
                    (item.name, item.role, item.sort_as)
                    for item in publication.contributors
                ),
                tuple(
                    (item.name, item.scheme, item.code) for item in publication.subjects
                ),
                tuple(
                    sorted(
                        (
                            item.artifact_id,
                            item.name,
                            str(item.location),
                            item.media_type,
                            item.size_bytes,
                            item.sha256,
                            _projection_datetime(item.modified_at),
                        )
                        for item in publication.artifacts
                    )
                ),
                publication.redownload_required,
            )

        return tuple(
            sorted(
                (publication_identity(publication) for publication in publications),
                key=lambda item: str(item[0]),
            )
        )

    @staticmethod
    def _artifact_from_row(row: Sequence[object]) -> CatalogArtifact:
        return CatalogArtifact(
            artifact_id=str(row[0]),
            name=str(row[1]),
            location=Path(str(row[2])),
            media_type=str(row[3]),
            size_bytes=int(str(row[4])),
            sha256=str(row[5]),
            modified_at=_parse_datetime(row[6]),
        )
