from __future__ import annotations

from pathlib import Path

import pytest
from test_vnext_facade import _config, _FacadeContext, _MariaRecorder

from h2hdb import (
    CatalogSubjectFilter,
    CatalogTagFilter,
    CatalogTagValue,
    VNextCatalogFacade,
)
from h2hdb.catalog_search import SEARCH_POLICY_ID
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_catalog_reader_repository import VNextCatalogReadError


@pytest.mark.parametrize("publications", [False, True])
@pytest.mark.parametrize("advance", [False, True])
def test_tag_browse_rechecks_head_in_a_fresh_transaction(
    monkeypatch: pytest.MonkeyPatch,
    publications: bool,
    advance: bool,
) -> None:
    old_head = (7, 0, 0, 1_000_000, 1)
    new_head = (8, 0, 0, 2_000_000, 2) if advance else old_head
    rows: list[tuple[object, ...]] = [old_head, old_head, (), (SEARCH_POLICY_ID,)]
    if publications:
        rows.append(())  # The exact tag is absent in this empty catalog.
    rows.extend((old_head, new_head))
    snapshot = _MariaRecorder(rows)
    monkeypatch.setattr(
        RepositoryContext,
        "from_config",
        classmethod(lambda cls, config: _FacadeContext(snapshot)),
    )
    facade = VNextCatalogFacade(_config(Path("unused"), backend="mariadb"))

    def read() -> None:
        if publications:
            page = facade.list_tag_publications(
                subject=CatalogTagFilter(namespace="artist", value="Example"),
            )
            assert page.publications == ()
            assert page.revision.revision == 7
        else:
            tags = facade.list_tag_values(namespace="artist")
            assert tags.values == ()
            assert tags.revision.revision == 7

    if advance:
        with pytest.raises(VNextCatalogReadError, match="head advanced"):
            read()
    else:
        read()
    assert snapshot.events == [
        "connect",
        "begin-read-only-snapshot",
        "commit",
        "begin-read-only-snapshot",
        "rollback" if advance else "commit",
        "close",
    ]
    assert not snapshot.mutations


@pytest.mark.parametrize("value", ["", "a" * 65_536, "圖" * 21_845 + "a"])
def test_tag_browse_accepts_the_complete_source_value_domain(value: str) -> None:
    assert CatalogTagFilter(namespace="artist", value=value).value == value
    assert CatalogTagValue(value=value, latest_uploaded_time=0).value == value
    with pytest.raises(ValueError):
        CatalogSubjectFilter(namespace="artist", value=value)


@pytest.mark.parametrize("value", ["a" * 65_537, "圖" * 21_846])
def test_tag_browse_value_bound_counts_utf8_bytes(value: str) -> None:
    with pytest.raises(ValueError):
        CatalogTagFilter(namespace="artist", value=value)
    with pytest.raises(ValueError):
        CatalogTagValue(value=value, latest_uploaded_time=0)
