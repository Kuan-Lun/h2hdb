"""Admission uses published source membership and survives process-local loss."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from unittest.mock import create_autospec, patch

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_ingest_turn,
    run_publication,
    run_source,
)

from h2hdb import CoreConfig, VNextIngestFacade, VNextSourceChangedError
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_catalog_registry_repository import CatalogRegistryNotReadyError
from h2hdb.vnext_identity import source_root_digest, source_scope_key
from h2hdb.vnext_source_batch_repository import (
    SourceBatchBaseline,
    SourceBatchChangedError,
    SourceBatchConflictError,
    SourceBatchRepository,
)
from h2hdb.vnext_source_build_repository import SourceBuildConflictError


def test_source_batch_uses_published_members_including_duplicate_losers(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    first = gallery(1001, title="First", pages=(b"shared-page",))
    duplicate = gallery(1002, title="Longer title", pages=(b"shared-page",))
    added = gallery(1003)
    source = MemorySource((first, duplicate))
    library = MemoryLibrary(source)
    policy = ingest_policy(artifacts_required=False)
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            empty = SourceBatchRepository.load_baseline(
                connector, source_root_components=source.source_root_components
            )
            assert empty.receipt_id is None
            assert empty.build_id is None
            assert SourceBatchRepository.lookup_members(
                connector, empty, (first.locator,)
            ) == (False,)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        published = run_ingest_turn(
            facade, source=source, library=library, policy=policy
        )
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                baseline = SourceBatchRepository.load_baseline(
                    connector, source_root_components=source.source_root_components
                )
                assert baseline.build_id == published.source.build_id
                assert connector.fetch_one(
                    "SELECT publication_count FROM catalog_publication_receipts "
                    "WHERE receipt_id = %s",
                    (baseline.receipt_id,),
                ) == (1,)
                assert SourceBatchRepository.lookup_members(
                    connector,
                    baseline,
                    (
                        duplicate.locator,
                        added.locator,
                        first.locator,
                        duplicate.locator,
                    ),
                ) == (True, False, True, True)
                page = (first.locator,) + tuple(
                    (f"unknown-{index}",) for index in range(127)
                )
                assert (
                    SourceBatchRepository.lookup_members(connector, baseline, page)
                    == (True,) + (False,) * 127
                )
                with pytest.raises(SourceBatchChangedError, match="head changed"):
                    SourceBatchRepository.require_current(connector, empty)
                with pytest.raises(SourceBatchConflictError, match="build differs"):
                    SourceBatchRepository.require_current(
                        connector, replace(baseline, build_id=b"x" * 16)
                    )
                replacement_root = SourceBatchRepository.load_baseline(
                    connector, source_root_components=("replacement", "root")
                )
                assert replacement_root.receipt_id == baseline.receipt_id
                assert replacement_root.build_id == baseline.build_id
                assert replacement_root.scope_key != baseline.scope_key
                assert SourceBatchRepository.lookup_members(
                    connector, replacement_root, (first.locator, duplicate.locator)
                ) == (False, False)
                SourceBatchRepository.require_current(connector, replacement_root)
                with patch(
                    "h2hdb.vnext_source_batch_repository.load_source_scope",
                    side_effect=CatalogRegistryNotReadyError(
                        "published scope is absent"
                    ),
                ):
                    with pytest.raises(SourceBatchConflictError, match="scope"):
                        SourceBatchRepository.load_baseline(
                            connector,
                            source_root_components=("replacement", "root"),
                        )

        # Root replacement must keep the normal admission cap even when all
        # relative locators occurred under the previous source root.
        replacement_source = MemorySource(
            (first, duplicate), root=("replacement", "root")
        )
        with facade.prepare_source(replacement_source, max_new_galleries=1) as cut:
            assert cut.deferred_gallery_count == 1

        source.put(added)
        session = claim_session(facade)
        resolved = facade.ensure_policy(session, policy)
        staged = run_source(facade, session, resolved, source)
        # Sealed newer observations and a complete working build are not proof
        # that the gallery has entered the current publication.
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                restarted = SourceBatchRepository.load_baseline(
                    connector, source_root_components=source.source_root_components
                )
                assert restarted == baseline
                assert SourceBatchRepository.lookup_members(
                    connector, restarted, (added.locator,)
                ) == (False,)
        run_analysis(facade, session, resolved, staged.build_id)
        run_publication(facade, session, resolved, library)
        facade.complete_ingest(session)
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            successor = SourceBatchRepository.load_baseline(
                connector, source_root_components=source.source_root_components
            )
            assert successor.build_id == staged.build_id
            assert SourceBatchRepository.lookup_members(
                connector, successor, (added.locator,)
            ) == (True,)
            with pytest.raises(SourceBatchChangedError, match="head changed"):
                SourceBatchRepository.require_current(connector, baseline)
            with pytest.raises(SourceBatchChangedError, match="head changed"):
                SourceBatchRepository.require_current(connector, replacement_root)


def test_source_batch_rejects_oversized_lookup_before_database_reads() -> None:
    connector = create_autospec(SQLConnector, instance=True)
    baseline = SourceBatchBaseline(None, None, b"s" * 32)
    with pytest.raises(ValueError, match="at most 128"):
        SourceBatchRepository.lookup_members(
            connector, baseline, tuple((f"gallery-{index}",) for index in range(129))
        )
    connector.fetch_one.assert_not_called()
    connector.fetch_all.assert_not_called()


@pytest.mark.parametrize("row", [(), (b"default",), (b"foreign", None)])
def test_source_batch_requires_complete_default_registry(
    row: tuple[bytes | None, ...],
) -> None:
    connector = create_autospec(SQLConnector, instance=True)
    connector.fetch_one.return_value = row
    with pytest.raises(SourceBatchConflictError, match="registry"):
        SourceBatchRepository.load_baseline(
            connector, source_root_components=("source",)
        )


def test_source_batch_missing_head_is_corruption_not_retry() -> None:
    connector = create_autospec(SQLConnector, instance=True)
    connector.fetch_one.side_effect = [(b"default", None), ()]
    baseline = SourceBatchBaseline(b"r" * 16, b"b" * 16, b"s" * 32)
    with pytest.raises(SourceBatchConflictError, match="disappeared") as failure:
        SourceBatchRepository.require_current(connector, baseline)
    assert not isinstance(failure.value, VNextSourceChangedError)


def test_source_batch_missing_head_cannot_turn_published_source_into_genesis() -> None:
    connector = create_autospec(SQLConnector, instance=True)
    connector.fetch_one.side_effect = [(b"default", None), (1,)]
    with pytest.raises(SourceBatchConflictError, match="head is absent") as failure:
        SourceBatchRepository.load_baseline(
            connector, source_root_components=("replacement", "root")
        )
    assert not isinstance(failure.value, VNextSourceChangedError)


def test_source_batch_invalid_successor_is_corruption_not_retry() -> None:
    connector = create_autospec(SQLConnector, instance=True)
    connector.fetch_one.return_value = (b"default", b"n" * 16)
    baseline = SourceBatchBaseline(
        b"r" * 16,
        b"b" * 16,
        source_scope_key("filesystem", source_root_digest(("source",)), 1),
    )
    with patch(
        "h2hdb.vnext_source_batch_repository._load_finalized_source_publication_by_receipt",
        side_effect=SourceBuildConflictError("publication provenance is incomplete"),
    ):
        with pytest.raises(SourceBatchConflictError, match="provenance") as failure:
            SourceBatchRepository.require_current(connector, baseline)
    assert not isinstance(failure.value, VNextSourceChangedError)


def test_source_batch_baseline_rejects_partial_identity() -> None:
    with pytest.raises(ValueError, match="incomplete"):
        SourceBatchBaseline(b"r" * 16, None, b"s" * 32)
