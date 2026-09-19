"""Exact descriptor-copy costs and public unchanged-artifact integration.

Kernel fixtures temporarily expand the pages of real published parents. They
are FK-valid isolated writer inputs, not complete READY states: the surrounding
artifact page count and byte extents intentionally remain outside this kernel
experiment. The original public rows are restored and full READY is checked.
The separate lifecycle test uses only valid source-to-publication facts.
"""

from __future__ import annotations

import inspect
import json
import statistics
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    full_check,
    gallery,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import CoreConfig, VNextCatalogFacade, VNextIngestFacade
from h2hdb import vnext_artifact_preparation_repository as artifacts
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.performance_acceptance

_COLUMNS = (
    "revision",
    "publication_key",
    "resource_kind",
    "page_index",
    "extent_offset",
    "extent_length",
    "media_type",
    "image_sha256",
    "width",
    "height",
)
_COPY = artifacts._copy_unchanged_catalog_pages


def _evidence_metadata() -> dict[str, Any]:
    source = Path(artifacts.__file__)
    return {
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "invocation": sys.argv,
        "source_sha256": sha256(source.read_bytes()).hexdigest(),
        "test_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
        "historical_copy_sha256": sha256(
            inspect.getsource(_historical_copy).encode()
        ).hexdigest(),
        "shared_exact_insert_comparison_sha256": sha256(
            inspect.getsource(artifacts._insert_or_compare).encode()
        ).hexdigest(),
        "python": sys.version,
    }


@dataclass(frozen=True)
class _Fixture:
    config: CoreConfig
    publication: bytes
    base_revision: int
    revision: int
    pages: tuple[tuple[Any, ...], ...]

    def work(self, connector: SQLConnector) -> VNextUnitOfWork:
        return VNextUnitOfWork(connector, backend=self.config.database.sql_type)

    def copy_arguments(self) -> dict[str, Any]:
        return {
            "base_revision": self.base_revision,
            "revision": self.revision,
            "publication_key": self.publication,
        }


def _rows(connector: SQLConnector, fixture: _Fixture) -> list[tuple[Any, ...]]:
    return connector.fetch_all(
        f"SELECT {', '.join(_COLUMNS)} FROM catalog_pages "
        "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
        (fixture.revision, fixture.publication),
    )


def _insert(connector: SQLConnector, rows: tuple[tuple[Any, ...], ...]) -> None:
    for start in range(0, len(rows), 64):
        connector.execute_many(
            f"INSERT INTO catalog_pages ({', '.join(_COLUMNS)}) "
            f"VALUES ({', '.join('%s' for _ in _COLUMNS)})",
            list(rows[start : start + 64]),
        )


def _replace(connector: SQLConnector, fixture: _Fixture, pages: int) -> None:
    with connector.transaction():
        connector.execute(
            "DELETE FROM catalog_pages WHERE revision IN (%s, %s) "
            "AND publication_key = %s",
            (fixture.base_revision, fixture.revision, fixture.publication),
        )
        _insert(connector, _expected(fixture, pages, revision=fixture.base_revision))


def _expected(
    fixture: _Fixture, pages: int, *, revision: int | None = None
) -> tuple[tuple[Any, ...], ...]:
    selected = fixture.revision if revision is None else revision
    return tuple(
        (
            selected,
            fixture.publication,
            b"acquisition",
            index,
            64 + 32 * index,
            32,
            b"image/png",
            index.to_bytes(32, "big"),
            64 + index,
            96,
        )
        for index in range(pages)
    )


def _historical_copy(
    work: VNextUnitOfWork,
    *,
    base_revision: int,
    revision: int,
    publication_key: bytes,
) -> None:
    """Frozen 0.39.10 page-copy algorithm; diagnostic negative control only."""
    pages = work.connector.fetch_all(
        "SELECT resource_kind, page_index, extent_offset, extent_length, "
        "media_type, image_sha256, width, height FROM catalog_pages "
        "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
        (base_revision, publication_key),
    )
    for row in pages:
        artifacts._insert_or_compare(
            work,
            "catalog_pages",
            _COLUMNS,
            (revision, publication_key, *row),
            key_where="revision = %s AND publication_key = %s AND page_index = %s",
            key_parameters=(revision, publication_key, row[1]),
            conflict_label="unchanged catalog page",
        )


@dataclass
class _Meter:
    sql_calls: int = 0
    fetched_rows: list[int] = field(default_factory=list)
    inserted_rows: list[int] = field(default_factory=list)
    max_parameters: int = 0

    def observe(self, query: str, parameters: tuple[Any, ...]) -> None:
        assert "catalog_pages" in query
        self.sql_calls += 1
        self.max_parameters = max(self.max_parameters, len(parameters))


@contextmanager
def _measure(connector: SQLConnector) -> Iterator[_Meter]:
    result = _Meter()
    fetch = connector.fetch_all
    execute = connector.execute

    def fetch_all(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        result.observe(query, data)
        rows = fetch(query, data)
        result.fetched_rows.append(len(rows))
        return rows

    def execute_recorded(query: str, data: tuple[Any, ...] = ()) -> None:
        result.observe(query, data)
        result.inserted_rows.append(len(data) // len(_COLUMNS))
        execute(query, data)

    with (
        patch.object(connector, "fetch_all", fetch_all),
        patch.object(connector, "execute", execute_recorded),
    ):
        yield result


def _assert_bound(meter: _Meter, pages: int) -> None:
    windows = (pages + 127) // 128
    assert meter.sql_calls <= 4 * windows + 1
    assert max(meter.fetched_rows, default=0) <= 128
    assert max(meter.inserted_rows, default=0) <= 64
    assert meter.max_parameters <= 640


@pytest.fixture
def published(db_config: CoreConfig) -> Iterator[_Fixture]:
    initialize_database(db_config)
    source = MemorySource([gallery(1)])
    library = MemoryLibrary(source)
    with (
        VNextIngestFacade(db_config) as facade,
        closing(VNextCatalogFacade(db_config)) as catalog,
    ):
        run_ingest_turn(facade, source=source, library=library)
        base = catalog.get_catalog_revision().revision
        source.put(gallery(2))
        run_ingest_turn(facade, source=source, library=library)
        revision = catalog.get_catalog_revision().revision
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            rows = connector.fetch_all(
                f"SELECT {', '.join(_COLUMNS)} FROM catalog_pages "
                "WHERE revision IN (%s, %s) ORDER BY revision, publication_key, page_index",
                (base, revision),
            )
    publications = {bytes(row[1]) for row in rows if row[0] == base}
    assert len(publications) == 1
    publication = publications.pop()
    saved = tuple(row for row in rows if row[1] == publication)
    fixture = _Fixture(db_config, publication, base, revision, saved)
    try:
        yield fixture
    finally:
        with closing(open_connector(db_config)) as connector:
            with connector.transaction():
                connector.execute(
                    "DELETE FROM catalog_pages WHERE revision IN (%s, %s) "
                    "AND publication_key = %s",
                    (base, revision, publication),
                )
                _insert(connector, saved)
        assert full_check(db_config).state == "READY"


def _run_measured(
    connector: SQLConnector,
    fixture: _Fixture,
    copier: Callable[..., None],
) -> tuple[_Meter, float]:
    started = time.perf_counter()
    with connector.transaction(), _measure(connector) as meter:
        copier(fixture.work(connector), **fixture.copy_arguments())
    return meter, time.perf_counter() - started


@pytest.mark.deep
def test_page_copy_windows_exact_replay_and_historical_cost_control(
    published: _Fixture, tmp_path: Path
) -> None:
    """Time committed transactions; validate costs without flaky wall-time gates."""
    report: list[dict[str, Any]] = []
    with closing(open_connector(published.config)) as connector:
        for pages in (0, 1, 63, 64, 65, 127, 128, 129, 4096):
            expected = _expected(published, pages)
            samples: dict[str, list[dict[str, Any]]] = {
                "production": [],
                "historical": [],
            }
            for cycle in range(3):
                variants = (
                    (("historical", _historical_copy), ("production", _COPY))
                    if cycle % 2 == 0
                    else (("production", _COPY), ("historical", _historical_copy))
                )
                for label, copier in variants:
                    _replace(connector, published, pages)
                    meter, seconds = _run_measured(connector, published, copier)
                    with connector.read_transaction():
                        assert tuple(_rows(connector, published)) == expected
                    replay_meter, replay_seconds = _run_measured(
                        connector, published, copier
                    )
                    with connector.read_transaction():
                        assert tuple(_rows(connector, published)) == expected
                    if label == "production":
                        _assert_bound(meter, pages)
                        _assert_bound(replay_meter, pages)
                    elif pages >= 63:
                        with pytest.raises(AssertionError):
                            _assert_bound(meter, pages)
                    samples[label].append(
                        {
                            "cycle": cycle,
                            "seconds": seconds,
                            "replay_seconds": replay_seconds,
                            "sql_calls": meter.sql_calls,
                            "replay_sql_calls": replay_meter.sql_calls,
                            "max_fetched_rows": max(meter.fetched_rows, default=0),
                            "max_inserted_rows": max(meter.inserted_rows, default=0),
                            "exact_output": True,
                        }
                    )
            report.append(
                {
                    "pages": pages,
                    "samples": samples,
                    "median_seconds": {
                        label: statistics.median(sample["seconds"] for sample in values)
                        for label, values in samples.items()
                    },
                }
            )
    target = tmp_path / "artifact-copy-kernel.json"
    target.write_text(
        json.dumps(
            {
                "backend": published.config.database.sql_type,
                **_evidence_metadata(),
                "scope": "FK-valid writer kernel, surrounding descriptor seals excluded",
                "clock": "perf_counter includes transaction begin and commit; local warm caches",
                "cost_bound": "at most 4*ceil(pages/128)+1 SQL calls, 128-row read, 64-row insert",
                "negative_control": "same-output historical algorithm rejected for pages >= 63",
                "wall_time_is_measured_not_asserted": True,
                "cases": report,
            },
            indent=2,
        )
    )


class _InjectedFailure(RuntimeError):
    pass


def test_page_copy_rejects_collisions_and_rolls_back_every_insert_boundary(
    published: _Fixture,
) -> None:
    with closing(open_connector(published.config)) as connector:
        _replace(connector, published, 129)
        expected = _expected(published, 129)
        with connector.transaction():
            _insert(connector, expected[::3])
        _run_measured(connector, published, _COPY)
        with connector.read_transaction():
            assert tuple(_rows(connector, published)) == expected
        for column, replacement in (
            ("resource_kind", b"thumbnail"),
            ("extent_offset", 777),
            ("extent_length", 31),
            ("media_type", b"image/other"),
            ("image_sha256", b"x" * 32),
            ("width", 23),
            ("height", 24),
        ):
            _replace(connector, published, 129)
            with connector.transaction():
                _insert(connector, (expected[128],))
                connector.execute(
                    f"UPDATE catalog_pages SET {column} = %s WHERE revision = %s "
                    "AND publication_key = %s AND page_index = 128",
                    (replacement, published.revision, published.publication),
                )
            with connector.read_transaction():
                before = _rows(connector, published)
            with pytest.raises(
                artifacts.ArtifactPreparationConflictError, match="exact facts"
            ):
                _run_measured(connector, published, _COPY)
            with connector.read_transaction():
                assert _rows(connector, published) == before
        for failed_insert in (1, 2, 3):
            _replace(connector, published, 129)
            original = connector.execute
            seen = 0

            def fail(query: str, data: tuple[Any, ...] = ()) -> None:
                nonlocal seen
                if query.startswith("INSERT INTO catalog_pages"):
                    seen += 1
                    if seen == failed_insert:
                        raise _InjectedFailure("write boundary")
                original(query, data)

            with (
                patch.object(connector, "execute", fail),
                pytest.raises(_InjectedFailure, match="write boundary"),
            ):
                _run_measured(connector, published, _COPY)
            with connector.read_transaction():
                assert _rows(connector, published) == []
            _run_measured(connector, published, _COPY)
            with connector.read_transaction():
                assert tuple(_rows(connector, published)) == expected


def test_page_copy_duplicate_race_is_exact_and_other_errors_propagate(
    published: _Fixture,
) -> None:
    with closing(open_connector(published.config)) as connector:
        expected = _expected(published, 65)
        for mismatch in (False, True):
            _replace(connector, published, 65)
            original = connector.execute
            fired = False

            def race(query: str, data: tuple[Any, ...] = ()) -> None:
                nonlocal fired
                if query.startswith("INSERT INTO catalog_pages") and not fired:
                    fired = True
                    row = list(expected[0])
                    if mismatch:
                        row[-1] = 555
                    original(
                        f"INSERT INTO catalog_pages ({', '.join(_COLUMNS)}) "
                        f"VALUES ({', '.join('%s' for _ in _COLUMNS)})",
                        tuple(row),
                    )
                    raise DatabaseDuplicateKeyError("competing partial insert")
                original(query, data)

            with patch.object(connector, "execute", race):
                if mismatch:
                    with pytest.raises(artifacts.ArtifactPreparationConflictError):
                        _run_measured(connector, published, _COPY)
                else:
                    _run_measured(connector, published, _COPY)
            with connector.read_transaction():
                assert tuple(_rows(connector, published)) == (
                    () if mismatch else expected
                )
        _replace(connector, published, 65)
        with (
            patch.object(
                connector, "execute", side_effect=_InjectedFailure("not duplicate")
            ),
            pytest.raises(_InjectedFailure, match="not duplicate"),
        ):
            _run_measured(connector, published, _COPY)
        with connector.read_transaction():
            assert _rows(connector, published) == []
        _replace(connector, published, 65)
        original_commit = connector.commit

        def lost_commit_response() -> None:
            original_commit()
            raise _InjectedFailure("commit response lost")

        with (
            patch.object(connector, "commit", lost_commit_response),
            pytest.raises(_InjectedFailure, match="commit response lost"),
        ):
            _run_measured(connector, published, _COPY)
        with closing(open_connector(published.config)) as recovered:
            with recovered.read_transaction():
                assert tuple(_rows(recovered, published)) == expected
            _run_measured(recovered, published, _COPY)
            with recovered.read_transaction():
                assert tuple(_rows(recovered, published)) == expected


@pytest.mark.deep
def test_public_pipeline_reuses_unchanged_pages_across_three_revisions(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MemorySource(
        [gallery(1, pages=[f"image-{i}".encode() for i in range(129)])]
    )
    library = MemoryLibrary(source)
    previous: tuple[tuple[Any, ...], ...] | None = None
    copy_calls = 0

    def recorded(work: VNextUnitOfWork, **kwargs: Any) -> None:
        nonlocal copy_calls
        copy_calls += 1
        with _measure(work.connector) as meter:
            _COPY(work, **kwargs)
        # Every gallery has <=129 pages in this fixture. The 129-page gallery
        # crosses both insertion and read-window boundaries.
        _assert_bound(meter, 129)

    with (
        VNextIngestFacade(db_config) as facade,
        closing(VNextCatalogFacade(db_config)) as catalog,
    ):
        for ordinal in (1, 2, 3):
            if ordinal > 1:
                source.put(gallery(ordinal))
            with patch.object(artifacts, "_copy_unchanged_catalog_pages", recorded):
                run_ingest_turn(facade, source=source, library=library)
            revision = catalog.get_catalog_revision()
            assert revision.artifact_count == ordinal
            assert library.render_calls == ordinal
            with closing(open_connector(db_config)) as connector:
                with connector.read_transaction():
                    current = tuple(
                        connector.fetch_all(
                            "SELECT p.resource_kind, p.page_index, p.extent_offset, "
                            "p.extent_length, p.media_type, p.image_sha256, p.width, p.height "
                            "FROM catalog_pages p JOIN catalog_publication_identities i "
                            "ON i.publication_key = p.publication_key "
                            "WHERE p.revision = %s AND i.gid = 1 ORDER BY p.page_index",
                            (revision.revision,),
                        )
                    )
            assert len(current) == 129
            if previous is not None:
                assert current == previous
            previous = current
            drain_maintenance(facade)
            assert full_check(db_config).state == "READY"
    assert copy_calls == 3


@pytest.mark.deep
@pytest.mark.parametrize("retained", [8, 32])
def test_public_fixed_new_gallery_growth_times_copy_variants(
    db_config: CoreConfig, tmp_path: Path, retained: int
) -> None:
    """Run both writers inside real fenced publication calls, then publish/clean."""
    initialize_database(db_config)
    source = MemorySource(
        [
            gallery(
                gid,
                locator=(f"gallery-{gid:06d}",),
                pages=[f"retained-{gid}-{index}".encode() for index in range(17)],
            )
            for gid in range(1, retained + 1)
        ]
    )
    library = MemoryLibrary(source)
    samples: list[dict[str, Any]] = []
    copied = 0

    def compare_inside_public_transaction(work: VNextUnitOfWork, **kwargs: Any) -> None:
        nonlocal copied
        copied += 1
        source_rows = work.connector.fetch_all(
            f"SELECT {', '.join(_COLUMNS[2:])} FROM catalog_pages "
            "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
            (kwargs["base_revision"], kwargs["publication_key"]),
        )
        expected = [
            (kwargs["revision"], kwargs["publication_key"], *row) for row in source_rows
        ]
        for cycle in range(3):
            variants = (
                (("historical", _historical_copy), ("production", _COPY))
                if (cycle + copied) % 2 == 0
                else (("production", _COPY), ("historical", _historical_copy))
            )
            for label, copier in variants:
                work.connector.execute("SAVEPOINT descriptor_copy_experiment")
                try:
                    started = time.perf_counter()
                    with _measure(work.connector) as meter:
                        copier(work, **kwargs)
                    elapsed = time.perf_counter() - started
                    actual = work.connector.fetch_all(
                        f"SELECT {', '.join(_COLUMNS)} FROM catalog_pages "
                        "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
                        (kwargs["revision"], kwargs["publication_key"]),
                    )
                    assert actual == expected
                    if label == "production":
                        _assert_bound(meter, len(source_rows))
                    else:
                        with pytest.raises(AssertionError):
                            _assert_bound(meter, len(source_rows))
                    samples.append(
                        {
                            "publication_key": kwargs["publication_key"].hex(),
                            "pages": len(source_rows),
                            "cycle": cycle,
                            "variant": label,
                            "seconds": elapsed,
                            "sql_calls": meter.sql_calls,
                            "exact_output": True,
                        }
                    )
                finally:
                    work.connector.execute(
                        "ROLLBACK TO SAVEPOINT descriptor_copy_experiment"
                    )
                    work.connector.execute(
                        "RELEASE SAVEPOINT descriptor_copy_experiment"
                    )
        _COPY(work, **kwargs)

    with (
        VNextIngestFacade(db_config) as facade,
        closing(VNextCatalogFacade(db_config)) as catalog,
    ):
        run_ingest_turn(facade, source=source, library=library)
        assert library.render_calls == retained
        for gid in range(retained + 1, retained + 101):
            source.put(
                gallery(
                    gid,
                    locator=(f"gallery-{gid:06d}",),
                    pages=[f"new-{gid}".encode()],
                )
            )
        with patch.object(
            artifacts,
            "_copy_unchanged_catalog_pages",
            compare_inside_public_transaction,
        ):
            run_ingest_turn(facade, source=source, library=library)
        revision = catalog.get_catalog_revision()
        assert revision.publication_count == retained + 100
        assert revision.artifact_count == retained + 100
        assert library.render_calls == retained + 100
        assert copied == retained
        seen = 0
        cursor = None
        while True:
            page = catalog.discover_publications(
                revision=revision, after=cursor, limit=128
            )
            for publication in page.publications:
                presentation = catalog.get_publication_presentation(
                    publication.publication_id, revision=revision
                )
                assert presentation is not None
                assert presentation.page_count == (
                    17 if publication.gid <= retained else 1
                )
                seen += 1
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        assert seen == retained + 100
        drain_maintenance(facade)
        assert full_check(db_config).state == "READY"
    totals = {
        label: [
            sum(
                sample["seconds"]
                for sample in samples
                if sample["variant"] == label and sample["cycle"] == cycle
            )
            for cycle in range(3)
        ]
        for label in ("historical", "production")
    }
    (tmp_path / "artifact-copy-public-growth.json").write_text(
        json.dumps(
            {
                "backend": db_config.database.sql_type,
                **_evidence_metadata(),
                "scope": "actual public source-analysis-publication flow; page-copy A/B inside fenced transaction",
                "artifact_adapter": "MemoryLibrary neutral deterministic bytes, not image encoding or real CBZ",
                "retained_galleries": retained,
                "retained_pages_each": 17,
                "new_galleries": 100,
                "new_pages_each": 1,
                "repetitions": 3,
                "clock": "perf_counter of copy kernel inside publication transaction; excludes outer transaction begin/commit",
                "warm_cache": "alternating historical/production under rollback savepoints; not cold disk or NAS",
                "oracle": "exact independent SELECT of full source facts with substituted target revision",
                "public_publication_cleanup_and_ready": "passed",
                "render_calls": library.render_calls,
                "samples": samples,
                "aggregate_seconds_per_cycle": totals,
                "median_aggregate_seconds": {
                    label: statistics.median(values) for label, values in totals.items()
                },
            },
            indent=2,
        )
    )
