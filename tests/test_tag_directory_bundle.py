from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
from test_vnext_catalog_reader_repository import (
    _canonical,
    _database,
    _published_fixture,
    _seed_tag_browse_fixture,
)
from vnext_catalog_identity_fixtures import seed_tag_term

from h2hdb import (
    CatalogTagBundle,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
)
from h2hdb.vnext_catalog_reader_repository import (
    VNextCatalogReaderRepository,
    VNextCatalogReadError,
)
from h2hdb.vnext_identity import publication_key


def test_tag_bundle_pages_exact_first_publications_without_individual_reads(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "tag-bundle.sqlite3"
    connector = _database(database_path)
    try:
        _seed_tag_browse_fixture(connector)
        facade = VNextCatalogFacade(
            CoreConfig(
                database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
            )
        )
        first_apple = min((202, 206), key=publication_key)
        expected = [
            ("amber", first_apple),
            ("beta", 202),
            ("alpha", 204),
            ("測試", 123),
            ("uncensored", 205),
        ]
        actual: list[tuple[str, int]] = []
        after = None
        with (
            patch.object(VNextCatalogReaderRepository, "get_publication") as individual,
            patch.object(VNextCatalogReaderRepository, "list_tag_publications") as tags,
        ):
            while True:
                bundle = facade.list_tag_values_with_publications(
                    namespace="artist", after=after, limit=2
                )
                assert bundle.page == facade.list_tag_values(
                    namespace="artist", after=after, limit=2
                )
                assert len(bundle.publications) <= 2
                actual.extend(
                    (value.value, publication.gid)
                    for value, publication in zip(
                        bundle.page.values, bundle.publications, strict=True
                    )
                )
                # The first ranked publication remains selected without images.
                assert all(item.thumbnail is None for item in bundle.publications)
                after = bundle.page.next_cursor
                if after is None:
                    break
            individual.assert_not_called()
            tags.assert_not_called()
        assert actual == expected
        empty = facade.list_tag_values_with_publications(namespace="missing")
        assert empty.page.values == ()
        assert empty.publications == ()
        assert empty.page.next_cursor is None
    finally:
        connector.close()


def test_tag_bundle_deduplicates_shared_publications_at_the_hard_page_bound(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "tag-bundle-bound.sqlite3")
    try:
        with connector.transaction():
            values = _published_fixture(connector, artifact_count=0)
            connector.execute(
                "DELETE FROM catalog_tag_directory_order WHERE revision = 1"
            )
            for position in range(130):
                digest = _canonical(
                    connector, "tag_value_utf8_v1", f"tag-{position:03}".encode()
                )
                tag_id = position + 2
                seed_tag_term(
                    connector,
                    tag_id=tag_id,
                    namespace=b"artist",
                    tag_value_sha256=digest,
                )
                connector.execute(
                    "INSERT INTO catalog_subjects "
                    "(revision, publication_key, position, tag_id) VALUES (1, %s, %s, %s)",
                    (values["publication_key"], position + 1, tag_id),
                )
                connector.execute(
                    "INSERT INTO catalog_tag_publication_order "
                    "(revision, tag_id, position, publication_key) VALUES (1, %s, 0, %s)",
                    (tag_id, values["publication_key"]),
                )
                connector.execute(
                    "INSERT INTO catalog_tag_directory_order "
                    "(revision, namespace, position, tag_value_sha256) VALUES (1, %s, %s, %s)",
                    (b"artist", position, digest),
                )
            connector.execute(
                "INSERT INTO catalog_tag_directory_order "
                "(revision, namespace, position, tag_value_sha256) VALUES (1, %s, 130, %s)",
                (b"artist", values["tag_value"]),
            )
            connector.execute(
                "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (1, 1)"
            )
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with patch.object(
            reader, "_hydrate_publications", wraps=reader._hydrate_publications
        ) as hydrate:
            first = reader.list_tag_values_with_publications(
                connector, namespace="artist", limit=128
            )
            hydrate.assert_called_once()
            assert hydrate.call_args.kwargs["publication_keys"] == (
                values["publication_key"],
            )
        assert len(first.page.values) == len(first.publications) == 128
        assert len({id(publication) for publication in first.publications}) == 1
        assert first.page.values[0].value == "tag-000"
        assert first.page.values[-1].value == "tag-127"
        assert first.page.next_cursor is not None
        last = reader.list_tag_values_with_publications(
            connector, namespace="artist", after=first.page.next_cursor, limit=128
        )
        assert [value.value for value in last.page.values] == [
            "tag-128",
            "tag-129",
            "測試",
        ]
        assert [publication.gid for publication in last.publications] == [123] * 3
        assert last.page.next_cursor is None
        with pytest.raises(ValueError, match="one publication per tag"):
            replace(last, publications=())
        assert CatalogTagBundle(page=last.page, publications=last.publications) == last
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        connector.close()


def test_tag_bundle_rejects_missing_publication_hydration_authority(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "tag-bundle-missing.sqlite3")
    try:
        _published_fixture(connector, artifact_count=0)
        connector.execute(
            "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (1, 1)"
        )
        connector.execute("DELETE FROM catalog_publication_order WHERE revision = 1")
        reader = VNextCatalogReaderRepository(backend="sqlite")
        with pytest.raises(VNextCatalogReadError):
            reader.list_tag_values_with_publications(connector, namespace="artist")
    finally:
        connector.close()
