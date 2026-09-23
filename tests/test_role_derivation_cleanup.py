"""READY accepts exact interrupted observation retirement, never general drift."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    Clock,
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
    takeover_clock,
)

from h2hdb import (
    CoreConfig,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextSourceQualification,
    catalog_refinement,
)
from h2hdb.sql_connector import SQLConnector

pytestmark = pytest.mark.cleanup_acceptance


@dataclass(frozen=True)
class _RetirementFixture:
    retired_hashes: tuple[bytes, ...]
    current_hash: bytes


class _QualificationSource(MemorySource):
    reject = False

    def observe_gallery(
        self, locator_components: tuple[str, ...]
    ) -> VNextIngestGalleryObservation:
        observed = super().observe_gallery(locator_components)
        return replace(
            observed,
            qualification=(
                VNextSourceQualification(False, "image_decode_failed", b"000.png")
                if self.reject
                else VNextSourceQualification()
            ),
        )


def _prepare_retirement(
    config: CoreConfig, *, pages: int, rejected: bool = False
) -> _RetirementFixture:
    """Publish a real policy-compacted replacement with no active old references."""

    initialize_database(config)
    payloads = tuple(f"retired-page-{number}".encode() for number in range(pages))
    source = _QualificationSource(
        [gallery(1001, pages=payloads, artists=[], language=None)]
    )
    source.reject = rejected
    library = MemoryLibrary(source)
    with VNextIngestFacade(config, clock=Clock()) as facade:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
        drain_maintenance(facade)
    source.reject = False
    source.put(gallery(1001, pages=[b"current-page"], artists=[], language=None))
    with VNextIngestFacade(config, clock=Clock()) as facade:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(
                artifacts_required=False,
                spam_occurrence_threshold=77,
            ),
        )
    assert full_check(config).state == "READY"
    return _RetirementFixture(
        tuple(sorted(sha256(payload).digest() for payload in payloads)),
        sha256(b"current-page").digest(),
    )


def _open_checkpoint(connector: SQLConnector) -> tuple[str, str, bytes] | None:
    with connector.read_transaction():
        rows = connector.fetch_all(
            "SELECT sweep.target_kind, checkpoint.phase, checkpoint.cursor_bytes "
            "FROM operational_cleanup_jobs AS job "
            "JOIN operational_cleanup_sweep_targets AS sweep "
            "ON sweep.target_key = job.target_key "
            "JOIN operational_cleanup_checkpoints AS checkpoint "
            "ON checkpoint.cleanup_id = job.cleanup_id "
            "WHERE job.state = 'OPEN' AND checkpoint.state = 'OPEN'"
        )
    assert len(rows) <= 1
    if not rows:
        return None
    return str(rows[0][0]), str(rows[0][1]), bytes(rows[0][2])


def _advance_one(config: CoreConfig) -> VNextCurrentOnlyMaintenanceOutcome:
    # Each call closes and reopens its public facade. Reducing only the number
    # of transactions per call exposes genuine committed crash boundaries;
    # neither the 256-row transaction limit nor a writer is replaced.
    with (
        patch("h2hdb.vnext_ingest_facade._CURRENT_ONLY_BATCHES_PER_ATTEMPT", 1),
        VNextIngestFacade(config, clock=takeover_clock()) as facade,
    ):
        return facade.drain_current_only_maintenance(LEASE_MICROSECONDS)


def _reach_fact_gap(config: CoreConfig) -> None:
    with closing(open_connector(config)) as connector:
        for _ in range(160):
            outcome = _advance_one(config)
            checkpoint = _open_checkpoint(connector)
            if checkpoint is not None and checkpoint[:2] == (
                "GALLERY_OBSERVATION",
                "GO_FACTS",
            ):
                assert checkpoint[2]
                return
            assert outcome is not VNextCurrentOnlyMaintenanceOutcome.DONE
    raise AssertionError("real cleanup never reached the observation fact boundary")


@pytest.mark.parametrize(
    ("pages", "rejected"),
    (
        (1, False),
        (1, True),
        pytest.param(127, False, marks=pytest.mark.deep),
        pytest.param(128, False, marks=pytest.mark.deep),
        pytest.param(129, False, marks=pytest.mark.deep),
        pytest.param(257, False, marks=pytest.mark.deep),
        pytest.param(257, True, marks=pytest.mark.deep),
    ),
)
def test_full_ready_accepts_every_committed_observation_retirement_checkpoint(
    db_config: CoreConfig,
    pages: int,
    rejected: bool,
) -> None:
    fixture = _prepare_retirement(db_config, pages=pages, rejected=rejected)
    _reach_fact_gap(db_config)
    seen: set[str] = set()
    partial_hashes = False
    partial_files = False
    with closing(open_connector(db_config)) as connector:
        for _ in range(160):
            checkpoint = _open_checkpoint(connector)
            if "GO_ROOT" in seen and (
                checkpoint is None or checkpoint[0] != "GALLERY_OBSERVATION"
            ):
                break
            if checkpoint is not None and checkpoint[0] == "GALLERY_OBSERVATION":
                phase = checkpoint[1]
                seen.add(phase)
                assert full_check(db_config).state == "READY"
                if phase == "GO_OBSERVATION_FACTS":
                    _assert_qualification_retirement_is_exact(connector)
                    _assert_retired_file_family_cannot_reappear(connector)
                with connector.read_transaction():
                    occurrences = connector.fetch_all(
                        "SELECT file_sha256 FROM "
                        "catalog_gallery_observation_file_hash_occurrences "
                        "WHERE gallery_id = 1 AND observation_id = 1"
                    )
                    files = connector.fetch_all(
                        "SELECT file_key FROM catalog_gallery_observation_file_anchors "
                        "WHERE gallery_id = 1 AND observation_id = 1"
                    )
                partial_hashes |= 0 < len(occurrences) < pages
                partial_files |= 0 < len(files) < pages + 1
            outcome = _advance_one(db_config)
            if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
                break
        else:
            raise AssertionError("restarted public cleanup did not finish")
        with VNextIngestFacade(db_config, clock=takeover_clock()) as facade:
            drain_maintenance(facade)
        assert {"GO_FACTS", "GO_FILES", "GO_OBSERVATION_FACTS", "GO_ROOT"} <= seen
        if pages == 257:
            assert partial_hashes and partial_files
        with connector.read_transaction():
            assert connector.fetch_all(
                "SELECT file_sha256, occurrence_count "
                "FROM catalog_gallery_observation_file_hash_occurrences"
            ) == [(fixture.current_hash, 1)]
    assert full_check(db_config).state == "READY"


class _RollbackMutation(Exception):
    pass


@contextmanager
def _rolled_back(connector: SQLConnector) -> Iterator[None]:
    try:
        with connector.transaction():
            yield
            raise _RollbackMutation
    except _RollbackMutation:
        pass


@pytest.mark.parametrize("pages", (1, pytest.param(257, marks=pytest.mark.deep)))
def test_observation_cleanup_authority_does_not_hide_corruption(
    db_config: CoreConfig,
    pages: int,
) -> None:
    fixture = _prepare_retirement(db_config, pages=pages)
    _reach_fact_gap(db_config)
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            catalog_refinement.check_role_derivation_v1(connector)
        mutations: tuple[tuple[str, tuple[Any, ...]], ...] = (
            (
                "UPDATE catalog_gallery_observation_file_hash_occurrences "
                "SET occurrence_count = 2 WHERE gallery_id = 1 AND observation_id = 2",
                (),
            ),
            (
                "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
                "(gallery_id, observation_id, file_sha256, occurrence_count) "
                "VALUES (1, 1, %s, 1)",
                (fixture.retired_hashes[0],),
            ),
            (
                "DELETE FROM catalog_gallery_observation_file_hash_occurrences "
                "WHERE gallery_id = 1 AND observation_id = 2",
                (),
            ),
            (
                "DELETE FROM operational_cleanup_cycle_roots",
                (),
            ),
            (
                "UPDATE operational_cleanup_checkpoints SET cursor_bytes = %s "
                "WHERE phase = 'GO_FACTS' AND state = 'OPEN'",
                (b"forged",),
            ),
            (
                "UPDATE catalog_source_build_galleries SET observation_id = 1 "
                "WHERE gallery_id = 1 AND observation_id = 2",
                (),
            ),
        )
        if pages == 257:
            # The first 256-row cleanup transaction has not reached the largest
            # digest. Its absence cannot be authorized by the earlier cursor.
            with connector.read_transaction():
                assert connector.fetch_one(
                    "SELECT occurrence_count FROM "
                    "catalog_gallery_observation_file_hash_occurrences "
                    "WHERE gallery_id = 1 AND observation_id = 1 AND file_sha256 = %s",
                    (fixture.retired_hashes[-1],),
                ) == (1,)
            mutations += (
                (
                    "DELETE FROM catalog_gallery_observation_file_hash_occurrences "
                    "WHERE gallery_id = 1 AND observation_id = 1 AND file_sha256 = %s",
                    (fixture.retired_hashes[-1],),
                ),
                (
                    "UPDATE catalog_gallery_observation_file_hash_occurrences "
                    "SET occurrence_count = 2 WHERE gallery_id = 1 "
                    "AND observation_id = 1 AND file_sha256 = %s",
                    (fixture.retired_hashes[-1],),
                ),
            )
        for query, parameters in mutations:
            with _rolled_back(connector):
                connector.execute(query, parameters)
                with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
                    catalog_refinement.check_role_derivation_v1(connector)
            with connector.read_transaction():
                catalog_refinement.check_role_derivation_v1(connector)
        assert full_check(db_config).state == "READY"


def _assert_qualification_retirement_is_exact(connector: SQLConnector) -> None:
    mutations = (
        "DELETE FROM catalog_gallery_observation_validation_dispositions "
        "WHERE gallery_id = 1 AND observation_id = 2",
        "INSERT INTO catalog_gallery_observation_validation_dispositions "
        "(gallery_id, observation_id, accepted) VALUES (1, 1, 1)",
    )
    for query in mutations:
        with _rolled_back(connector):
            connector.execute(query)
            with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
                catalog_refinement.check_source_qualification_v1(connector)
        with connector.read_transaction():
            catalog_refinement.check_source_qualification_v1(connector)


def _assert_retired_file_family_cannot_reappear(connector: SQLConnector) -> None:
    # Reintroduce a complete METADATA family, not a broken orphan. CONTENT-only
    # comparison would miss it, so this exercises the exact GO_FILES authority.
    with connector.read_transaction():
        row = connector.fetch_one(
            "SELECT file_key FROM catalog_file_name_identities WHERE name_bytes = %s",
            (b"galleryinfo.txt",),
        )
    assert len(row) == 1
    file_key = bytes(row[0])
    with _rolled_back(connector):
        for suffix, extra in (
            ("anchors", ""),
            ("file_nos", ", file_no"),
            ("file_sha256s", ", file_sha256"),
            ("artifact_role", ", artifact_role"),
            ("seals", ""),
        ):
            connector.execute(
                f"INSERT INTO catalog_gallery_observation_file_{suffix} "
                f"(gallery_id, observation_id, file_key{extra}) "
                f"SELECT gallery_id, 1, file_key{extra} "
                f"FROM catalog_gallery_observation_file_{suffix} "
                "WHERE gallery_id = 1 AND observation_id = 2 AND file_key = %s",
                (file_key,),
            )
        with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
            catalog_refinement.check_role_derivation_v1(connector)
    with connector.read_transaction():
        catalog_refinement.check_role_derivation_v1(connector)
