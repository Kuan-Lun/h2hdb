"""Executable SHA-256 collision-resistance boundary.

SHA-256 collisions cannot be produced, so their impossibility cannot be
tested.  The catalog therefore does not rely on it wherever an exact preimage
is retained: every stored identity that carries its preimage is accepted only
after full byte comparison.  This module installs a *collision fixture*: a
digest that is exactly SHA-256 except that two equal-length markers are
identified, so two different payloads share one digest.  It then proves the
production writers fail closed at every retained-preimage identity, and pins
the one place where identity is digest-plus-size only (file content blobs),
which is the explicit collision-resistance assumption.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from test_vnext_hash_cache_repository import _authorities, _database, _put, _ready_build
from vnext_fault_harness import (
    EPOCH_CONTROL_TABLE,
    physical_tables,
    snapshot_database,
    snapshot_difference,
)
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    gallery,
    initialize_database,
    run_ingest_turn,
)

import h2hdb.domain as domain_module
import h2hdb.vnext_artifact_presentation as presentation_module
import h2hdb.vnext_artifact_render as render_module
import h2hdb.vnext_gallery_staging_repository as staging_module
import h2hdb.vnext_hash_cache_repository as hash_cache_module
import h2hdb.vnext_identity as identity_module
import h2hdb.vnext_source_observation_spool as spool_module
from h2hdb import CoreConfig, DatabaseConfig, VNextIngestFacade
from h2hdb.vnext_canonical_value_family import load_sealed_value_identity
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_catalog_identity_family import (
    CatalogIdentityCollisionError,
    FileNameIdentity,
    ensure_file_name_identities,
)
from h2hdb.vnext_gallery_staging_repository import GalleryStagingConflictError
from h2hdb.vnext_hash_cache_repository import (
    FileHashCacheConflictError,
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

MARKER_A = b"COLLIDE-A"
MARKER_B = b"COLLIDE-B"
_DIGEST_MODULES = (
    identity_module,
    domain_module,
    staging_module,
    hash_cache_module,
    spool_module,
    render_module,
    presentation_module,
)


class CollidingSHA256:
    """SHA-256 composed with the identification of MARKER_B with MARKER_A."""

    name = "sha256"
    digest_size = 32
    block_size = 64

    def __init__(self, data: bytes = b"") -> None:
        self._buffer = bytearray(data)

    def update(self, data: bytes) -> None:
        self._buffer.extend(data)

    def digest(self) -> bytes:
        return hashlib.sha256(bytes(self._buffer).replace(MARKER_B, MARKER_A)).digest()

    def hexdigest(self) -> str:
        return self.digest().hex()

    def copy(self) -> CollidingSHA256:
        return CollidingSHA256(bytes(self._buffer))


@pytest.fixture
def colliding_digest() -> Iterator[None]:
    patches = [
        patch.object(module, "sha256", CollidingSHA256) for module in _DIGEST_MODULES
    ]
    for active in patches:
        active.start()
    try:
        yield
    finally:
        for active in reversed(patches):
            active.stop()


def test_collision_fixture_identifies_exactly_the_two_markers(
    colliding_digest: None,
) -> None:
    assert len(MARKER_A) == len(MARKER_B)
    same = identity_module.canonical_value_digest("tag_value_utf8_v1", MARKER_A)
    assert identity_module.canonical_value_digest("tag_value_utf8_v1", MARKER_B) == same
    assert identity_module.canonical_value_digest("tag_value_utf8_v1", b"other") != same
    # Anything not containing MARKER_B is exactly SHA-256.
    plain = CollidingSHA256(b"plain")
    assert plain.digest() == hashlib.sha256(b"plain").digest()
    marked = CollidingSHA256(b"x" + MARKER_B + b"y")
    assert marked.digest() == hashlib.sha256(b"x" + MARKER_A + b"y").digest()


def _config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


_DATA_TABLES = tuple(
    table for table in physical_tables("sqlite") if table != EPOCH_CONTROL_TABLE
)


def _snapshot(path: Path) -> dict[str, tuple[tuple[Any, ...], ...]]:
    return snapshot_database(_config(path), tables=_DATA_TABLES)


def test_canonical_value_upload_fails_closed_on_a_digest_collision(
    tmp_path: Path,
    colliding_digest: None,
) -> None:
    path = tmp_path / "canonical-collision.sqlite3"
    connector = _database(path)
    plan_a = CanonicalValueUploadPlan.from_parts(
        "tag_value_utf8_v1", (b"tag-", MARKER_A, b"-value")
    )
    plan_b = CanonicalValueUploadPlan.from_parts(
        "tag_value_utf8_v1", (b"tag-", MARKER_B, b"-value")
    )
    try:
        assert plan_a.value_sha256 == plan_b.value_sha256
        assert plan_a.byte_count == plan_b.byte_count
        gate, turn = _authorities(connector)
        _ready_build(connector, gate, turn)
        _put(connector, gate, turn, plan_a, start=40)
        sealed = load_sealed_value_identity(connector, value_sha256=plan_a.value_sha256)
        assert sealed is not None
        before = _snapshot(path)
        with pytest.raises(CanonicalValueCollisionError):
            _put(connector, gate, turn, plan_b, start=50)
        assert snapshot_difference(before, _snapshot(path)) == {}
        # Streaming the sealed identity back returns exactly payload A.
        parts: list[bytes] = []
        with connector.read_transaction():
            receipt = CanonicalValueRepository.stream_and_validate(
                VNextUnitOfWork(connector, backend="sqlite"),
                value_sha256=plan_a.value_sha256,
                consume_provisional=parts.append,
            )
        assert receipt.value_sha256 == plan_a.value_sha256
        assert b"".join(parts) == b"tag-" + MARKER_A + b"-value"
    finally:
        plan_a.close()
        plan_b.close()
        connector.close()


def test_file_name_identity_fails_closed_on_a_file_key_collision(
    tmp_path: Path,
    colliding_digest: None,
) -> None:
    path = tmp_path / "file-name-collision.sqlite3"
    connector = _database(path)
    try:
        name_a = MARKER_A + b".png"
        name_b = MARKER_B + b".png"
        key = identity_module.file_key(name_a)
        assert identity_module.file_key(name_b) == key
        first = FileNameIdentity(key, name_a, identity_module.file_role(name_a))
        second = FileNameIdentity(key, name_b, identity_module.file_role(name_b))
        with connector.transaction():
            ensure_file_name_identities(connector, identities=(first,))
        before = _snapshot(path)
        with pytest.raises(CatalogIdentityCollisionError):
            with connector.transaction():
                ensure_file_name_identities(connector, identities=(second,))
        assert snapshot_difference(before, _snapshot(path)) == {}
        with pytest.raises(CatalogIdentityCollisionError):
            ensure_file_name_identities(connector, identities=(first, second))
    finally:
        connector.close()


def test_gallery_observation_page_fails_closed_on_a_page_digest_collision(
    tmp_path: Path,
    colliding_digest: None,
) -> None:
    """Two galleries whose FILE pages differ only in the colliding marker share
    a page digest; staging the second gallery must refuse to alias the page."""

    config = _config(tmp_path / "page-collision.sqlite3")
    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                5001,
                title="Title " + MARKER_A.decode(),
                pages=[b"shared-page"],
                artists=["one"],
                locator=("first",),
            ),
            gallery(
                5001,
                title="Title " + MARKER_B.decode(),
                pages=[b"shared-page"],
                artists=["one"],
                locator=("second",),
            ),
        ]
    )
    library = MemoryLibrary(source)
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        with pytest.raises(GalleryStagingConflictError, match="collision"):
            run_ingest_turn(facade, source=source, library=library)
    finally:
        facade.close()


def test_file_content_identity_is_digest_plus_size_by_explicit_assumption(
    tmp_path: Path,
    colliding_digest: None,
) -> None:
    """Content blobs retain no preimage: a colliding digest with a different
    size is rejected, while equal size cannot be distinguished.  That is the
    documented SHA-256 collision-resistance assumption of the file content
    identity; everything else in this module compares preimages."""

    path = tmp_path / "hash-cache-collision.sqlite3"
    connector = _database(path)
    source_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_source_identity_v1", (b"source-id-v1\0", b"/gallery/file.jpg")
    )
    fingerprint_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_fingerprint_v1", (b"fingerprint-v1\0", b"stat")
    )
    other_source = CanonicalValueUploadPlan.from_parts(
        "filesystem_source_identity_v1", (b"source-id-v1\0", b"/gallery/other.jpg")
    )
    other_fingerprint = CanonicalValueUploadPlan.from_parts(
        "filesystem_fingerprint_v1", (b"fingerprint-v1\0", b"stat-other")
    )
    try:
        gate, turn = _authorities(connector)
        _ready_build(connector, gate, turn)
        for now, plan in enumerate(
            (source_plan, fingerprint_plan, other_source, other_fingerprint), start=4
        ):
            _put(connector, gate, turn, plan, start=now * 10)
        file_a = FileHashObservationPlan.from_parts((b"bytes-", MARKER_A))
        file_b = FileHashObservationPlan.from_parts((b"bytes-", MARKER_B))
        file_longer = FileHashObservationPlan.from_parts((b"bytes-", MARKER_B, b"!"))
        assert file_a.file_sha256 == file_b.file_sha256
        assert file_longer.file_sha256 != file_a.file_sha256
        with connector.transaction():
            VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=file_a,
                observed_at=100,
                cached_at=101,
                now=102,
            )
        # A colliding digest with another byte count is rejected closed.
        forged = FileHashObservationPlan.from_parts((b"bytes-", MARKER_B, b"!!"))
        forged_same_digest = type(file_a)(
            file_a.file_sha256, forged.size_bytes, file_a._constructor_token
        )
        before = _snapshot(path)
        with pytest.raises(FileHashCacheConflictError, match="byte count"):
            with connector.transaction():
                VNextHashCacheRepository.handoff(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    source_plan=other_source,
                    fingerprint_plan=other_fingerprint,
                    file_plan=forged_same_digest,
                    observed_at=200,
                    cached_at=201,
                    now=202,
                )
        assert snapshot_difference(before, _snapshot(path)) == {}
        # Equal size and colliding digest are indistinguishable by design.
        with connector.transaction():
            hit = VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=other_source,
                fingerprint_plan=other_fingerprint,
                file_plan=file_b,
                observed_at=200,
                cached_at=201,
                now=203,
            )
        assert hit.file_sha256 == file_a.file_sha256
        assert hit.size_bytes == file_a.size_bytes
    finally:
        for plan in (source_plan, fingerprint_plan, other_source, other_fingerprint):
            plan.close()
        connector.close()
