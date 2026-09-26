"""Upload timestamp corrections preserve immutable observations and publications."""

from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import patch

import pytest
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    METADATA_NAME,
    Clock,
    MemoryLibrary,
    drain_maintenance,
    full_check,
    gallery,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import (
    CatalogDiscoveryQuery,
    CatalogRecentOrder,
    CatalogTagFilter,
    CatalogTimestampRange,
    CoreConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
)
from h2hdb import catalog_refinement as refinement
from h2hdb.catalog_refinement import CatalogSemanticValidationError
from h2hdb.vnext_cleanup_repository import _encode_static_cursor
from h2hdb.vnext_identity import (
    encode_gallery_observation_metadata,
    validate_gallery_observation_metadata_parts,
)
from h2hdb.vnext_publication_family import (
    CatalogPublicationUploadTimeFamily,
    PublicationFamilyCollisionError,
    PublicationFamilyPartialError,
    compare_catalog_publication_upload_time_families,
    ensure_catalog_publication_upload_time_family,
    load_catalog_publication_family,
)
from h2hdb.vnext_source_metadata import iter_metadata_chunks

_GID = 1_322_802
_OLD_UPLOAD = 1_543_686_780_000_000
_NEW_UPLOAD = 1_543_686_540_000_000
_MIDDLE_UPLOAD = (_OLD_UPLOAD + _NEW_UPLOAD) // 2
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _instant(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def _assert_catalog_order(config: CoreConfig, *, corrected: bool) -> None:
    reader = VNextCatalogFacade(config)
    expected = [_GID + 1, _GID] if corrected else [_GID, _GID + 1]
    recent = reader.list_recent_publications(order=CatalogRecentOrder.UPLOADED)
    assert [item.gid for item in recent.publications] == expected
    tagged = reader.list_tag_publications(
        subject=CatalogTagFilter(namespace="artist", value="shared")
    )
    assert [item.gid for item in tagged.publications] == expected
    directory = reader.list_tag_values(namespace="rank")
    assert [item.value for item in directory.values] == (
        ["neighbor", "target"] if corrected else ["target", "neighbor"]
    )
    filtered = reader.discover_publications(
        query=CatalogDiscoveryQuery(
            uploaded=CatalogTimestampRange(start=_instant(_MIDDLE_UPLOAD))
        )
    )
    assert {item.gid for item in filtered.publications} == (
        {_GID + 1} if corrected else {_GID, _GID + 1}
    )
    target = next(
        item for item in reader.discover_publications().publications if item.gid == _GID
    )
    assert target.published_at == _instant(_NEW_UPLOAD if corrected else _OLD_UPLOAD)


def test_upload_correction_preserves_old_observation_and_publication_until_commit(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(
        _GID,
        upload_time=_OLD_UPLOAD,
        artists=("shared",),
        extra_tags=(("rank", "target"),),
        other_files={METADATA_NAME: b"Upload Time: 2018-12-01 17:53\n"},
    )
    neighbor = gallery(
        _GID + 1,
        upload_time=_MIDDLE_UPLOAD,
        artists=("shared",),
        extra_tags=(("rank", "neighbor"),),
    )
    source = MarkerSource((original, neighbor))
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        first = run_ingest_turn(facade, source=source, library=library)
    _assert_catalog_order(db_config, corrected=False)
    with closing(open_connector(db_config)) as connector:
        old_row = connector.fetch_one(
            "SELECT gallery_id, observation_id FROM catalog_gallery_observation_metadata "
            "WHERE gid = %s",
            (_GID,),
        )
        old_gallery, old_observation = old_row
        old_metadata = tuple(
            iter_metadata_chunks(connector, old_gallery, old_observation)
        )
        assert validate_gallery_observation_metadata_parts(
            old_metadata
        ).upload_time == (_OLD_UPLOAD)
    changed = gallery(
        _GID,
        upload_time=_NEW_UPLOAD,
        modified_time=original.modified_time + 1,
        artists=("shared",),
        extra_tags=(("rank", "target"),),
        other_files={METADATA_NAME: b"Upload Time: 2018-12-01 17:49\n"},
    )
    source.put(changed)
    checked_before_publication = False

    def before_publication(label: str) -> None:
        nonlocal checked_before_publication
        if label != "analysis.issue" or checked_before_publication:
            return
        checked_before_publication = True
        _assert_catalog_order(db_config, corrected=False)
        with closing(open_connector(db_config)) as connector:
            assert connector.fetch_all(
                "SELECT upload_time FROM catalog_gallery_observation_upload_times "
                "WHERE gallery_id = %s ORDER BY observation_id",
                (old_gallery,),
            ) == [(_OLD_UPLOAD,), (_NEW_UPLOAD,)]
            assert (
                tuple(iter_metadata_chunks(connector, old_gallery, old_observation))
                == old_metadata
            )

    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        second = run_ingest_turn(
            facade,
            source=source,
            library=library,
            boundary=before_publication,
        )
        assert checked_before_publication
        assert second.source.build_id != first.source.build_id
        _assert_catalog_order(db_config, corrected=True)
        assert full_check(db_config).state == "READY"
        drain_maintenance(facade)
        assert full_check(db_config).state == "READY"


def test_same_gid_locations_keep_distinct_times_and_selected_winner_time(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    # The existing GID winner comparator prefers the longer title, independently
    # of upload time. Equal page bytes keep both locations in the same group.
    source = MarkerSource(
        (
            gallery(_GID, title="Short", locator=("a",), upload_time=_OLD_UPLOAD),
            gallery(
                _GID,
                title="A considerably longer source title",
                locator=("b",),
                upload_time=_NEW_UPLOAD,
            ),
        )
    )
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        run_ingest_turn(facade, source=source, library=library)
    publications = VNextCatalogFacade(db_config).discover_publications().publications
    assert len(publications) == 1
    assert publications[0].source_gallery_name == "b"
    assert publications[0].published_at == _instant(_NEW_UPLOAD)
    with closing(open_connector(db_config)) as connector:
        assert connector.fetch_all(
            "SELECT upload_time FROM catalog_gallery_observation_metadata "
            "WHERE gid = %s ORDER BY upload_time",
            (_GID,),
        ) == [(_NEW_UPLOAD,), (_OLD_UPLOAD,)]
    assert full_check(db_config).state == "READY"


def test_publication_upload_time_family_replays_exactly_and_rejects_mutation(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(_GID, upload_time=_OLD_UPLOAD),))
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        run_ingest_turn(facade, source=source, library=library)
    with closing(open_connector(db_config)) as connector:
        revision, key, value = connector.fetch_one(
            "SELECT occurrence.revision, occurrence.publication_key, uploaded.upload_time "
            "FROM catalog_publication_occurrence_identities AS occurrence "
            "JOIN catalog_publication_upload_times AS uploaded "
            "ON uploaded.catalog_occurrence_sha256 = occurrence.catalog_occurrence_sha256"
        )
        family = CatalogPublicationUploadTimeFamily(revision, bytes(key), value)
        connector.rollback()
        with connector.transaction():
            assert ensure_catalog_publication_upload_time_family(
                connector, family, backend=db_config.database.sql_type
            ) == (family, False)
            compare_catalog_publication_upload_time_families(connector, (family,))
            with pytest.raises(PublicationFamilyCollisionError, match="replay changed"):
                ensure_catalog_publication_upload_time_family(
                    connector,
                    CatalogPublicationUploadTimeFamily(
                        revision, bytes(key), _NEW_UPLOAD
                    ),
                    backend=db_config.database.sql_type,
                )
        assert connector.fetch_one(
            "SELECT upload_time FROM catalog_publication_upload_times"
        ) == (_OLD_UPLOAD,)
        connector.rollback()
        connector.begin()
        try:
            connector.execute("DELETE FROM catalog_publication_storage")
            with pytest.raises(
                PublicationFamilyPartialError, match="no complete payload"
            ):
                load_catalog_publication_family(
                    connector,
                    revision=revision,
                    publication_key=bytes(key),
                    backend=db_config.database.sql_type,
                )
            assert connector.fetch_one(
                "SELECT upload_time FROM catalog_publication_upload_times"
            ) == (_OLD_UPLOAD,)
        finally:
            connector.rollback()


def test_upload_audit_rejects_changed_observation_scalar(db_config: CoreConfig) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(_GID, upload_time=_OLD_UPLOAD),))
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        run_ingest_turn(facade, source=source, library=MemoryLibrary(source))
    with closing(open_connector(db_config)) as connector:
        connector.execute(
            "UPDATE catalog_gallery_observation_upload_times SET upload_time = %s",
            (_NEW_UPLOAD,),
        )
    with pytest.raises(
        CatalogSemanticValidationError,
        match="source observation upload time differs from canonical metadata",
    ):
        full_check(db_config)


@pytest.mark.parametrize("observation_count", (0, 127, 128, 129))
def test_upload_audit_batches_timestamps_without_rescanning_metadata(
    observation_count: int,
) -> None:
    """Adding upload validation must not add per-observation SQL or tree walks."""

    class BoundedObservations:
        def __init__(self) -> None:
            self.calls = 0

        def primary_key_table_reference(self, relation: str) -> str:
            return relation

        def fetch_all(
            self, query: str, parameters: tuple[object, ...]
        ) -> list[tuple[int, int, int]]:
            self.calls += 1
            assert "catalog_gallery_observations" in query
            assert "catalog_gallery_observation_upload_times" in query
            after = int(cast(int, parameters[0]))
            return [
                (gallery_id, 1, _OLD_UPLOAD)
                for gallery_id in range(
                    after + 1, min(after + 128, observation_count) + 1
                )
            ]

        def fetch_one(self, *_args: object, **_kwargs: object) -> None:
            pytest.fail("timestamp audit issued an extra per-observation SQL read")

    connector = BoundedObservations()
    metadata = encode_gallery_observation_metadata(
        gallery(_GID, upload_time=_OLD_UPLOAD).metadata()
    )
    with (
        patch.object(
            refinement, "_validated_open_observation_retirement", return_value=None
        ),
        patch.object(refinement, "require_source_qualification"),
        patch.object(
            refinement,
            "iter_metadata_chunks",
            side_effect=lambda *_args: iter((metadata,)),
        ) as read_metadata,
    ):
        refinement.check_source_qualification_v1(cast(Any, connector))
    assert connector.calls == (observation_count + 127) // 128 + 1
    assert read_metadata.call_count == observation_count


def test_observation_retirement_accepts_last_shifted_metadata_relation() -> None:
    root = (41, 2)
    values = (*root, *root)
    assert refinement._observation_retirement_cursor(
        13, _encode_static_cursor(8, values), frozenset((root,))
    ) == (8, values)
    with pytest.raises(CatalogSemanticValidationError, match="relation is invalid"):
        refinement._observation_retirement_cursor(
            13, _encode_static_cursor(9, values), frozenset((root,))
        )
