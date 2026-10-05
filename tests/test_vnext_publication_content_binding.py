"""Exact cross-revision content corruption and public restart/cleanup semantics."""

from __future__ import annotations

from contextlib import closing

import pytest
from vnext_corpora import Corpus, _stopped, _turn
from vnext_database_snapshot import ReusableDatabaseSnapshot, database_digest
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    catalog_view,
    full_check,
    gallery,
    initialize_database,
    run_ingest_turn,
)
from vnext_test_database import (
    DatabaseFactory,
    foreign_key_checks_enabled,
    set_foreign_key_checks,
)

from h2hdb import CoreConfig, VNextCatalogFacade, VNextIngestFacade
from h2hdb.catalog_refinement import CatalogSemanticValidationError
from h2hdb.vnext_identity import publication_key


def _move_content_child(
    config: CoreConfig, revision: int, original: bytes, replacement: bytes
) -> None:
    """Fault injection only; fresh consumers retain their native FK enforcement."""

    with closing(open_connector(config)) as connector:
        set_foreign_key_checks(connector, enabled=False)
        with connector.transaction():
            assert (
                connector.execute_affected(
                    "UPDATE catalog_publication_contents SET publication_key = %s "
                    "WHERE revision = %s AND publication_key = %s",
                    (replacement, revision, original),
                )
                == 1
            )
    with closing(open_connector(config)) as fresh:
        assert foreign_key_checks_enabled(fresh)


def _assert_new_revision_parent_is_published(config: CoreConfig) -> None:
    with closing(VNextCatalogFacade(config)) as reader:
        revision = reader.get_catalog_revision()
        page = reader.discover_publications(revision=revision, limit=128)
        added = [item for item in page.publications if item.gid == 1003]
        assert len(added) == 1
        assert added[0].content_sha256 is not None


@pytest.mark.deep
@pytest.mark.parametrize(
    ("cut", "historical"),
    (
        ("COMMIT_PUBLICATION", False),
        ("FINALIZE", False),
        ("completed_without_cleanup", False),
        ("completed_without_cleanup", True),
    ),
)
def test_ready_rejects_cross_revision_publication_content_binding(
    database_factory: DatabaseFactory, cut: str, historical: bool
) -> None:
    """One portable body chooses exact coordinates, never engine row ordering.

    Candidate/active orphans are rejected even after the scalar resume path;
    historical orphans cannot escape through current-head-only validation.
    Exact test-only repair permits normal subsequent consumption and cleanup;
    the audit does not claim to repair arbitrary corruption by itself.
    """

    config = database_factory.config("content-binding-baseline")
    initialize_database(config)
    source = MemorySource(
        [
            gallery(1001, pages=[b"p0", b"p1"]),
            gallery(1002, pages=[b"q0"], artists=["bob"], language="japanese"),
        ]
    )
    library = MemoryLibrary(source)
    _turn(config, source, library)
    source.put(gallery(1001, pages=[b"p0", b"p1", b"p-extra"]))
    # A distinct positive content owner creates a publication key only in the
    # second revision. A metadata-only source is not a GID candidate in this
    # public pipeline; optional child absence has a separate native unit case.
    source.put(gallery(1003, pages=[b"new-only-parent"], artists=["carol"]))
    source.remove(("gallery-1002",))
    stop = None if cut == "completed_without_cleanup" else "publication.commit:" + cut
    if stop is None:
        with VNextIngestFacade(config, clock=Clock()) as facade:
            run_ingest_turn(facade, source=source, library=library)
    else:
        _stopped(config, source, library, stop=stop)
    corpus = Corpus(cut, config, source, library, stop)
    with closing(open_connector(config)) as connector, connector.read_transaction():
        revisions = connector.fetch_all(
            "SELECT revision FROM catalog_publication_contents "
            "WHERE publication_key = %s ORDER BY revision",
            (publication_key(1001),),
        )
        assert len(revisions) == 2
        oldest, newest = int(revisions[0][0]), int(revisions[1][0])
        assert oldest < newest
        target_revision = oldest if historical else newest
        replacement_key = publication_key(1003 if historical else 1002)
        other_revision = newest if historical else oldest
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_publication_occurrence_identities "
                "WHERE revision = %s AND publication_key = %s",
                (target_revision, replacement_key),
            )
            == []
        )
        assert connector.fetch_all(
            "SELECT 1 FROM catalog_publication_occurrence_identities "
            "WHERE revision = %s AND publication_key = %s",
            (other_revision, replacement_key),
        ) == [(1,)]
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_publication_contents "
                "WHERE revision = %s AND publication_key = %s",
                (target_revision, replacement_key),
            )
            == []
        )
    assert full_check(config).state == "READY"
    before = database_digest(config)
    snapshot = ReusableDatabaseSnapshot(
        database_factory, config, database_factory.config("content-binding-replay")
    )

    def finish(target: CoreConfig) -> None:
        # Both helpers require maintenance to reach DONE, rather than treating
        # publication alone as completion. At rest, consume adds another GID.
        if corpus.mid_flight:
            corpus.resume(target)
        else:
            corpus.consume(target)

    finish(snapshot.target)
    assert full_check(snapshot.target).state == "READY"
    _assert_new_revision_parent_is_published(snapshot.target)
    reference = catalog_view(snapshot.target)
    target = snapshot.restore()
    _move_content_child(target, target_revision, publication_key(1001), replacement_key)
    with pytest.raises(
        CatalogSemanticValidationError, match="no occurrence in its revision"
    ):
        full_check(target)

    resume_rejected = False
    if corpus.mid_flight:
        try:
            finish(target)
        except CatalogSemanticValidationError as error:
            # A future earlier audit may refuse the same corruption while
            # resuming; unrelated errors must never count as this rejection.
            assert "no occurrence in its revision" in str(error)
            resume_rejected = True
        # Commit/finalize remain scalar operations. The subsequent complete
        # READY audit must still reject the orphan instead of certifying a
        # catalog whose former positive content silently became None.
        with pytest.raises(
            CatalogSemanticValidationError, match="no occurrence in its revision"
        ):
            full_check(target)
    _move_content_child(target, target_revision, replacement_key, publication_key(1001))
    if not corpus.mid_flight or resume_rejected:
        finish(target)
    assert full_check(target).state == "READY"
    _assert_new_revision_parent_is_published(target)
    assert catalog_view(target) == reference
    assert database_digest(config) == before
