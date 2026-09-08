"""Qualification is immutable source authority, including cache and cleanup."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace

import pytest
from test_vnext_source_batches import _publications
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
    run_source,
)

from h2hdb import (
    CoreConfig,
    GalleryObservationMetadata,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextSourceQualification,
)
from h2hdb.catalog_refinement import CatalogSemanticValidationError
from h2hdb.vnext_identity import (
    GalleryObservationMetadataDecoder,
    decode_gallery_observation_metadata,
    encode_gallery_observation_metadata,
)
from h2hdb.vnext_source_qualification_repository import SourceQualificationConflictError


class QualifiedSource(MarkerSource):
    rejected: set[tuple[str, ...]]

    def __init__(self) -> None:
        super().__init__((gallery(1001), gallery(1002)))
        self.rejected = {("gallery-1001",)}

    def observe_gallery(
        self, locator_components: tuple[str, ...]
    ) -> VNextIngestGalleryObservation:
        observed = super().observe_gallery(locator_components)
        return replace(
            observed,
            qualification=(
                VNextSourceQualification(False, "image_decode_failed", b"000.png")
                if locator_components in self.rejected
                else VNextSourceQualification()
            ),
        )


def test_qualification_metadata_roundtrips_every_checkpoint() -> None:
    qualification = VNextSourceQualification(
        False, "image_decode_failed", "壞圖.png".encode()
    )
    metadata = GalleryObservationMetadata(
        123,
        "title",
        "",
        "uploader",
        1,
        2,
        3,
        1,
        1,
        1,
        qualification_policy_sha256=b"p" * 32,
        qualification=qualification,
    )
    payload = encode_gallery_observation_metadata(metadata)
    assert decode_gallery_observation_metadata(payload) == metadata
    for position in range(len(payload) + 1):
        decoder = GalleryObservationMetadataDecoder()
        decoder.feed(payload[:position])
        resumed = GalleryObservationMetadataDecoder(decoder.state)
        resumed.feed(payload[position:])
        receipt = resumed.finish()
        assert receipt.qualification == qualification
        assert receipt.qualification_policy_sha256 == b"p" * 32


def test_rejection_cache_rechecks_policy_and_preserves_retained_source_authority(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = QualifiedSource()
    library = MemoryLibrary(source)
    policy = ingest_policy(artifacts_required=False)

    def publish() -> None:
        with VNextIngestFacade(db_config, clock=Clock()) as facade:
            receipts = run_ingest_turn(
                facade, source=source, library=library, policy=policy
            )
            drain_maintenance(facade)
        assert full_check(db_config).state == "READY"
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_source_build_galleries WHERE build_id = %s",
                    (receipts.source.build_id,),
                ) == (len(source.galleries),)

    publish()
    assert {item.gid for item in _publications(db_config)} == {1002}
    source.deep_reads.clear()
    source.forbidden_reads.update(value.locator for value in source.galleries)
    publish()
    assert not source.deep_reads

    # Same source marker, new decoder/presentation semantics: both receipts must
    # be re-qualified. Previously rejected input can now become accepted.
    source.forbidden_reads.clear()
    source.rejected.clear()
    policy = replace(
        policy, artifact=replace(policy.artifact, policy_fingerprint_sha256=b"q" * 32)
    )
    publish()
    assert set(source.deep_reads) == {value.locator for value in source.galleries}
    assert {item.gid for item in _publications(db_config)} == {1001, 1002}

    source.remove(("gallery-1001",))
    publish()
    assert {item.gid for item in _publications(db_config)} == {1002}
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            retained = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_gallery_observations"
            )
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_gallery_observation_validation_policies"
                )
                == retained
            )
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_gallery_observation_validation_dispositions"
                )
                == retained
            )
            rejected = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_gallery_observation_validation_dispositions WHERE accepted = 0"
            )
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_gallery_observation_validation_reasons"
                )
                == rejected
            )
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_gallery_observation_validation_sources"
                )
                == rejected
            )


@pytest.mark.parametrize(
    "corruption", ("missing", "policy", "accepted", "reason", "source")
)
def test_ready_rejects_qualification_disagreeing_with_canonical_metadata(
    db_config: CoreConfig,
    corruption: str,
) -> None:
    initialize_database(db_config)
    source = QualifiedSource()
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        run_source(facade, session, policy, source)
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            key = connector.fetch_one(
                "SELECT gallery_id, observation_id FROM catalog_gallery_observation_validation_dispositions WHERE accepted = 0"
            )
            assert key
            if corruption == "missing":
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_validation_dispositions WHERE gallery_id = %s AND observation_id = %s",
                    key,
                )
            else:
                table, column, value = {
                    "policy": ("policies", "qualification_policy_sha256", b"q" * 32),
                    "accepted": ("dispositions", "accepted", 1),
                    "reason": ("reasons", "qualification_reason", b"other_reason"),
                    "source": ("sources", "qualification_source_name", b"missing.png"),
                }[corruption]
                connector.execute(
                    f"UPDATE catalog_gallery_observation_validation_{table} SET {column} = %s WHERE gallery_id = %s AND observation_id = %s",
                    (value, *key),
                )
    with pytest.raises(CatalogSemanticValidationError, match="qualification"):
        full_check(db_config)


def test_rejection_must_identify_an_observed_page(db_config: CoreConfig) -> None:
    initialize_database(db_config)

    class WrongSource(QualifiedSource):
        def observe_gallery(
            self, locator_components: tuple[str, ...]
        ) -> VNextIngestGalleryObservation:
            observed = super().observe_gallery(locator_components)
            return replace(
                observed,
                qualification=VNextSourceQualification(
                    False, "image_decode_failed", b"galleryinfo.txt"
                ),
            )

    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with pytest.raises(SourceQualificationConflictError, match="observed PAGE"):
            run_source(facade, session, policy, WrongSource())


def test_qualification_enforces_bounded_neutral_facts() -> None:
    maximum = VNextSourceQualification(False, "r" * 64, b"s" * 255)
    assert maximum.reason_code == "r" * 64
    for accepted, reason, source in (
        (True, "unexpected", None),
        (True, None, b"page.png"),
        (False, "", b"page.png"),
        (False, "r" * 65, b"page.png"),
        (False, "Error", b"page.png"),
        (False, "bad\nreason", b"page.png"),
        (False, "bad", b""),
        (False, "bad", b"s" * 256),
        (False, "bad", b"nested/page.png"),
    ):
        with pytest.raises(ValueError):
            VNextSourceQualification(accepted, reason, source)


def test_qualification_checkpoint_rejects_incoherent_atomic_facts() -> None:
    metadata = GalleryObservationMetadata(
        123,
        "title",
        "",
        "uploader",
        1,
        2,
        3,
        1,
        1,
        1,
        qualification_policy_sha256=b"p" * 32,
        qualification=VNextSourceQualification(False, "decode_error", b"page.png"),
    )
    decoder = GalleryObservationMetadataDecoder()
    decoder.feed(encode_gallery_observation_metadata(metadata))
    for changes in (
        {"qualification_policy_sha256": b"short"},
        {"qualification_reason": b"bad\nreason"},
        {"qualification_reason": b""},
        {"qualification_source_name": b"a" * 256},
        {"qualification_source_name": b""},
        {"accepted": True},
    ):
        with pytest.raises(ValueError):
            replace(decoder.state, **changes)
