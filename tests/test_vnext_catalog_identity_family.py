from __future__ import annotations

from typing import Any

import pytest

from h2hdb.vnext_catalog_identity_family import (
    CatalogIdentityCollisionError,
    CatalogIdentityPartialFamilyError,
    FileNameIdentity,
    GalleryIdentity,
    GalleryObservationFile,
    TagTerm,
    ensure_file_name_identities,
    ensure_gallery_identity,
    ensure_gallery_observation_files,
    ensure_tag_term,
    load_file_name_identities,
    load_gallery_identities,
    load_gallery_observation_files,
    load_tag_terms,
)
from h2hdb.vnext_identity import file_key, gallery_key


class _Recorder:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executions: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        self.fetches.append((query, data))
        return self.rows

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self.executions.append((query, data))


def _assert_one_set_read(recorder: _Recorder, relations: tuple[str, ...]) -> None:
    assert len(recorder.fetches) == 1
    query, parameters = recorder.fetches[0]
    assert parameters
    assert "FOR UPDATE" not in query.upper()
    assert "?" not in query
    assert "%s" in query
    for relation in relations:
        assert relation in query


def test_four_family_loaders_are_one_set_read_and_validate_exact_tuples() -> None:
    scope = b"s" * 32
    locator = b"l" * 32
    stable = gallery_key(scope, locator)
    gallery = _Recorder([(1, 1, scope, locator, 1, 1, stable, 1)])
    assert load_gallery_identities(gallery, gallery_ids=(1,)) == {
        1: GalleryIdentity(1, stable, scope, locator)
    }
    _assert_one_set_read(
        gallery,
        (
            "catalog_gallery_identity_anchors",
            "catalog_gallery_identity_coordinates",
            "catalog_gallery_identity_gallery_keys",
            "catalog_gallery_identity_seals",
        ),
    )

    name = b"001.jpg"
    name_key = file_key(name)
    file_name = _Recorder(
        [(name_key, name_key, name_key, name, name_key, b"CONTENT", name_key)]
    )
    assert load_file_name_identities(file_name, file_keys=(name_key,)) == {
        name_key: FileNameIdentity(name_key, name, b"CONTENT")
    }
    _assert_one_set_read(
        file_name,
        (
            "catalog_file_name_identity_anchors",
            "catalog_file_name_identity_name_bytes",
            "catalog_file_name_identity_file_roles",
            "catalog_file_name_identity_seals",
        ),
    )

    file_sha256 = b"f" * 32
    observation_file = _Recorder(
        [
            (
                1,
                2,
                name_key,
                1,
                2,
                name_key,
                1,
                2,
                name_key,
                0,
                1,
                2,
                name_key,
                file_sha256,
                1,
                2,
                name_key,
            )
        ]
    )
    assert load_gallery_observation_files(
        observation_file,
        gallery_id=1,
        observation_id=2,
        file_keys=(name_key,),
        file_nos=(0,),
    ) == {name_key: GalleryObservationFile(1, 2, 0, name_key, file_sha256)}
    _assert_one_set_read(
        observation_file,
        (
            "catalog_gallery_observation_file_anchors",
            "catalog_gallery_observation_file_file_nos",
            "catalog_gallery_observation_file_file_sha256s",
            "catalog_gallery_observation_file_seals",
        ),
    )

    value = b"v" * 32
    tag = _Recorder([(3, 3, b"artist", value, 3, 3)])
    assert load_tag_terms(tag, tag_ids=(3,)) == {3: TagTerm(3, b"artist", value)}
    _assert_one_set_read(
        tag,
        (
            "catalog_tag_term_anchors",
            "catalog_tag_term_identities",
            "catalog_tag_term_seals",
        ),
    )


def test_four_family_loaders_fail_closed_on_any_partial_family() -> None:
    scope = b"s" * 32
    locator = b"l" * 32
    stable = gallery_key(scope, locator)
    with pytest.raises(CatalogIdentityPartialFamilyError):
        load_gallery_identities(
            _Recorder([(1, 1, scope, locator, 1, 1, stable, None)]),
            gallery_ids=(1,),
        )

    name = b"001.jpg"
    key = file_key(name)
    with pytest.raises(CatalogIdentityPartialFamilyError):
        load_file_name_identities(
            _Recorder([(key, key, key, name, key, b"CONTENT", None)]),
            file_keys=(key,),
        )

    with pytest.raises(CatalogIdentityPartialFamilyError):
        load_gallery_observation_files(
            _Recorder(
                [
                    (
                        1,
                        2,
                        key,
                        1,
                        2,
                        key,
                        1,
                        2,
                        key,
                        0,
                        1,
                        2,
                        key,
                        b"f" * 32,
                        None,
                        None,
                        None,
                    )
                ]
            ),
            gallery_id=1,
            observation_id=2,
            file_keys=(key,),
            file_nos=(0,),
        )

    with pytest.raises(CatalogIdentityPartialFamilyError):
        load_tag_terms(
            _Recorder([(3, 3, b"artist", b"v" * 32, 3, None)]),
            tag_ids=(3,),
        )


def test_gallery_loader_recomputes_stable_key_and_rejects_valid_width_corruption() -> (
    None
):
    scope = b"s" * 32
    locator = b"l" * 32
    with pytest.raises(CatalogIdentityCollisionError, match="invalid immutable"):
        load_gallery_identities(
            _Recorder([(1, 1, scope, locator, 1, 1, b"x" * 32, 1)]),
            gallery_ids=(1,),
        )


def test_four_family_writers_reject_candidate_collisions_without_writes() -> None:
    scope = b"s" * 32
    locator = b"l" * 32
    other_scope = b"t" * 32
    other_locator = b"m" * 32
    gallery = _Recorder(
        [
            (
                1,
                1,
                other_scope,
                other_locator,
                1,
                1,
                gallery_key(other_scope, other_locator),
                1,
            )
        ]
    )
    with pytest.raises(CatalogIdentityCollisionError, match="collides"):
        ensure_gallery_identity(
            gallery,
            identity=GalleryIdentity(1, gallery_key(scope, locator), scope, locator),
        )
    assert gallery.executions == []

    name = b"001.jpg"
    key = file_key(name)
    file_name = _Recorder([(key, key, key, name, key, b"METADATA", key)])
    with pytest.raises(CatalogIdentityCollisionError, match="derived facts"):
        ensure_file_name_identities(
            file_name,
            identities=(FileNameIdentity(key, name, b"CONTENT"),),
        )
    assert file_name.executions == []

    observation_file = _Recorder(
        [
            (
                1,
                2,
                key,
                1,
                2,
                key,
                1,
                2,
                key,
                0,
                1,
                2,
                key,
                b"x" * 32,
                1,
                2,
                key,
            )
        ]
    )
    with pytest.raises(CatalogIdentityCollisionError, match="candidate key collides"):
        ensure_gallery_observation_files(
            observation_file,
            identities=(GalleryObservationFile(1, 2, 0, key, b"f" * 32),),
        )
    assert observation_file.executions == []

    tag = _Recorder([(3, 3, b"language", b"w" * 32, 3, 3)])
    with pytest.raises(CatalogIdentityCollisionError, match="candidate key collides"):
        ensure_tag_term(tag, term=TagTerm(3, b"artist", b"v" * 32))
    assert tag.executions == []


def test_four_family_writers_use_one_candidate_read_and_insert_seal_last() -> None:
    scope = b"s" * 32
    locator = b"l" * 32
    gallery = _Recorder([])
    assert ensure_gallery_identity(
        gallery,
        identity=GalleryIdentity(1, gallery_key(scope, locator), scope, locator),
    )
    _assert_one_set_read(gallery, ("catalog_gallery_identity_seals",))
    assert "catalog_gallery_identity_seals" in gallery.executions[-1][0]

    first_name = b"001.jpg"
    second_name = b"002.jpg"
    names = _Recorder([])
    ensure_file_name_identities(
        names,
        identities=(
            FileNameIdentity(file_key(first_name), first_name, b"CONTENT"),
            FileNameIdentity(file_key(second_name), second_name, b"CONTENT"),
        ),
    )
    _assert_one_set_read(names, ("catalog_file_name_identity_seals",))
    assert len(names.fetches) == 1
    assert "catalog_file_name_identity_seals" in names.executions[-1][0]

    files = _Recorder([])
    ensure_gallery_observation_files(
        files,
        identities=(
            GalleryObservationFile(1, 2, 0, file_key(first_name), b"a" * 32),
            GalleryObservationFile(1, 2, 1, file_key(second_name), b"b" * 32),
        ),
    )
    _assert_one_set_read(files, ("catalog_gallery_observation_file_seals",))
    assert len(files.fetches) == 1
    assert "catalog_gallery_observation_file_seals" in files.executions[-1][0]

    tag = _Recorder([])
    assert ensure_tag_term(
        tag,
        term=TagTerm(3, b"artist", b"v" * 32),
    )
    _assert_one_set_read(tag, ("catalog_tag_term_seals",))
    assert "catalog_tag_term_seals" in tag.executions[-1][0]
