"""Small-fixture compaction evidence, independent of retained history rows."""

from __future__ import annotations

import copy
from contextlib import closing
from dataclasses import dataclass, fields, is_dataclass
from hashlib import sha256
from pathlib import Path

from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    file_role,
    full_check,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import (
    ArtifactSourceRole,
    CatalogArtifact,
    CatalogFacetKind,
    CatalogImageResource,
    CatalogPublication,
    CatalogPublicationPresentation,
    CatalogRecentOrder,
    CatalogTagFilter,
    CoreConfig,
    DatabaseConfig,
    StorageObjectDescriptor,
    VNextCatalogFacade,
    VNextIngestFacade,
    VNextIngestPolicy,
)


@dataclass(frozen=True)
class CompactionLayout:
    revision: int
    analysis_id: bytes
    build_id: bytes
    policy_id: int
    publication_state: str
    ancestry: tuple[bytes, ...]
    baseline: bytes | None

    @property
    def depth(self) -> int:
        return len(self.ancestry) - 1


def current_compaction_layout(config: CoreConfig) -> CompactionLayout:
    """Observe the current lineage now; later GC must not erase this evidence."""

    return _compaction_layout(config, pending=False)


def pending_compaction_layout(config: CoreConfig) -> CompactionLayout:
    """Observe the one not-yet-activated commit in this isolated test corpus."""

    return _compaction_layout(config, pending=True)


def _compaction_layout(config: CoreConfig, *, pending: bool) -> CompactionLayout:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        receipts = connector.fetch_all(
            "SELECT receipt_id FROM catalog_publication_receipts "
            "WHERE state = 'DB_COMMITTED' LIMIT 2"
            if pending
            else "SELECT receipt_id FROM catalog_publication_commit_head_receipts LIMIT 2"
        )
        assert len(receipts) == 1
        row = connector.fetch_one(
            "SELECT receipt.revision, run.analysis_id, run.build_id, "
            "run.policy_id, receipt.state "
            "FROM catalog_publication_receipts AS receipt "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = receipt.receipt_id "
            "JOIN catalog_source_revision_provenance AS provenance "
            "ON provenance.source_revision = committed.source_revision "
            "JOIN catalog_analysis_runs AS run "
            "ON run.analysis_id = provenance.analysis_id "
            "WHERE receipt.receipt_id = %s",
            (receipts[0][0],),
        )
        assert len(row) == 5
        analysis_id = bytes(row[1])
        ancestors = connector.fetch_all(
            "SELECT ancestor_depth, ancestor_analysis_id "
            "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
            "ORDER BY ancestor_depth",
            (analysis_id,),
        )
        assert [int(item[0]) for item in ancestors] == list(range(len(ancestors)))
        ancestry = tuple(bytes(item[1]) for item in ancestors)
        assert ancestry and ancestry[0] == analysis_id
        assert len(set(ancestry)) == len(ancestry)
        baseline = connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (analysis_id,),
        )
    return CompactionLayout(
        int(row[0]),
        analysis_id,
        bytes(row[2]),
        int(row[3]),
        str(row[4]),
        ancestry,
        bytes(baseline[0]) if baseline else None,
    )


def retained_compaction_roots(config: CoreConfig) -> tuple[set[bytes], set[bytes]]:
    """Exact live analysis/build identities in an isolated, bounded fixture."""

    with closing(open_connector(config)) as connector, connector.read_transaction():
        analyses = connector.fetch_all(
            "SELECT analysis_id FROM catalog_analysis_run_descriptor"
        )
        builds = connector.fetch_all(
            "SELECT build_id FROM catalog_source_build_descriptor"
        )
    return {bytes(row[0]) for row in analyses}, {bytes(row[0]) for row in builds}


def _semantic(value: object) -> object:
    """Keep every semantic field, excluding only identified opaque locators."""

    omitted: set[str] = set()
    if isinstance(value, CatalogArtifact):
        omitted = {"artifact_id"}
    elif isinstance(value, StorageObjectDescriptor):
        omitted = {"key"}
    elif isinstance(value, (CatalogPublication, CatalogPublicationPresentation)):
        omitted = {"publication_id"}
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _semantic(getattr(value, item.name))
            for item in fields(value)
            if item.name not in omitted
        }
    if isinstance(value, tuple):
        return tuple(_semantic(item) for item in value)
    return value


def _image_bytes(library: MemoryLibrary, image: CatalogImageResource) -> bytes:
    payload = library.objects[image.storage_object.key]
    assert len(payload) == image.storage_object.size_bytes
    assert sha256(payload).hexdigest() == image.storage_object.sha256
    end = image.extent.offset + image.extent.length
    assert end <= len(payload)
    image_bytes = payload[image.extent.offset : end]
    assert sha256(image_bytes).hexdigest() == image.sha256
    return image_bytes


def compaction_semantics(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
) -> dict[str, object]:
    """Full public semantics and resolved bytes for a small, non-spam corpus.

    Revision numbers and publication commit timestamps intentionally differ
    between accumulated and fresh histories. Source timestamps, content hashes,
    all metadata, resource facts, facet counts and page order remain exact.
    The fixture must fit one catalog/facet/tag page; fail rather than truncate.
    """

    with closing(VNextCatalogFacade(config)) as facade:
        return _read_compaction_semantics(facade, source, library)


def _read_compaction_semantics(
    facade: VNextCatalogFacade,
    source: MemorySource,
    library: MemoryLibrary,
) -> dict[str, object]:
    current = facade.get_catalog_revision()
    discovered = facade.discover_publications(revision=current, limit=128)
    assert discovered.next_cursor is None
    assert discovered.total == current.publication_count == len(discovered.publications)
    source_by_gid = {item.gid: item for item in source.galleries}
    publications: dict[int, object] = {}
    namespaces = {namespace for item in source.galleries for namespace, _ in item.tags}
    for publication in discovered.publications:
        assert (
            facade.get_publication(publication.publication_id, revision=current)
            == publication
        )
        presentation = facade.get_publication_presentation(
            publication.publication_id, revision=current
        )
        assert presentation is not None
        images = tuple(
            facade.get_publication_page(
                publication.publication_id, index, revision=current
            )
            for index in range(publication.page_count)
        )
        assert all(image is not None for image in images)
        page_bytes = tuple(
            _image_bytes(library, image) for image in images if image is not None
        )
        expected_pages = tuple(
            payload
            for name, payload in sorted(source_by_gid[publication.gid].files.items())
            if file_role(name) is ArtifactSourceRole.PAGE
        )
        assert page_bytes == expected_pages
        if presentation.thumbnail is not None:
            _image_bytes(library, presentation.thumbnail)
        artifacts = []
        for artifact in publication.artifacts:
            assert (
                facade.get_artifact(artifact.artifact_id, revision=current) == artifact
            )
            payload = library.objects[artifact.storage_object.key]
            assert len(payload) == artifact.storage_object.size_bytes
            assert sha256(payload).hexdigest() == artifact.storage_object.sha256
            artifacts.append((_semantic(artifact), payload))
        publications[publication.gid] = (
            _semantic(publication),
            _semantic(presentation),
            _semantic(images),
            page_bytes,
            tuple(artifacts),
        )
    facets: dict[str, object] = {}
    for facet in CatalogFacetKind:
        page = facade.list_publication_facets(facet=facet, revision=current, limit=128)
        assert page.next_cursor is None
        facets[facet.value] = _semantic(page.values)
    tags: dict[str, object] = {}
    for namespace in sorted(namespaces):
        tag_page = facade.list_tag_values(
            namespace=namespace, revision=current, limit=128
        )
        assert tag_page.next_cursor is None
        memberships = []
        for value in tag_page.values:
            members = facade.list_tag_publications(
                subject=CatalogTagFilter(namespace, value.value),
                revision=current,
                limit=128,
            )
            assert members.next_cursor is None
            memberships.append(
                (value, tuple(item.gid for item in members.publications))
            )
        tags[namespace] = tuple(memberships)
    resources = []
    for (gid, kind), descriptor in sorted(
        library.current.items(), key=lambda item: (item[0][0], item[0][1].value)
    ):
        payload = library.objects[descriptor.key]
        assert len(payload) == descriptor.size_bytes
        assert sha256(payload).hexdigest() == descriptor.sha256
        resources.append((gid, kind, _semantic(descriptor), payload))
    assert {item.key for item in library.current.values()} == set(library.objects)
    return {
        "counts": (current.publication_count, current.artifact_count),
        "publication_order": tuple(item.gid for item in discovered.publications),
        "publications": publications,
        "facets": facets,
        "tags": tags,
        "recent": {
            order.value: _semantic(
                facade.list_recent_publications(
                    order=order, revision=current
                ).publications
            )
            for order in CatalogRecentOrder
        },
        "resources": tuple(resources),
    }


def fresh_compaction_semantics(
    source: MemorySource,
    *,
    directory: Path,
    policy: VNextIngestPolicy | None = None,
) -> dict[str, object]:
    """Independent fresh SQLite reference for either subject backend."""

    directory.mkdir()
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(directory / "fresh.sqlite3")
        )
    )
    initialize_database(config)
    fresh_source = copy.deepcopy(source)
    library = MemoryLibrary(fresh_source)
    with VNextIngestFacade(config, clock=Clock()) as facade:
        run_ingest_turn(facade, source=fresh_source, library=library, policy=policy)
        drain_maintenance(facade)
    assert full_check(config).state == "READY"
    return compaction_semantics(config, fresh_source, library)
