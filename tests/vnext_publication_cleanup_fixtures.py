"""Focused publication cleanup graphs using the real schema and both connectors.

Unrelated publication/source families are deliberately absent. Setup disables
foreign keys, then restores and verifies enforcement before cleanup runs. This
is SQL/transaction evidence, not a claim that a complete READY audit passes.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager

from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database
from vnext_publication_fixtures import (
    seed_publication_commit,
    seed_publication_finalization,
)

from h2hdb import CoreConfig, vnext_identity
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupCycle,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

PUBLICATION_KEY = bytes((201,)) + bytes(31)
OLD_RECEIPT = b"o" * 16
CURRENT_RECEIPT = b"n" * 16


@contextmanager
def partial_publication_setup(
    connector: SQLConnector, *, backend: str
) -> Iterator[None]:
    """Only setup may omit unrelated families; tested mutations enforce FKs."""
    if backend == "sqlite":
        disable, enable, probe = (
            "PRAGMA foreign_keys = OFF",
            "PRAGMA foreign_keys = ON",
            "PRAGMA foreign_keys",
        )
    else:
        disable, enable, probe = (
            "SET FOREIGN_KEY_CHECKS = 0",
            "SET FOREIGN_KEY_CHECKS = 1",
            "SELECT @@FOREIGN_KEY_CHECKS",
        )
    connector.execute(disable)
    try:
        with connector.transaction():
            yield
    finally:
        connector.execute(enable)
    assert connector.fetch_one(probe) == (1,)


def seed_publication_cleanup(
    config: CoreConfig,
    *,
    rows: int = 65,
    phase: str = "CP_SUBJECT",
    max_rows: int = 256,
    variable_keys: tuple[bytes, ...] | None = None,
    root_count: int = 1,
) -> tuple[GateLease, CleanupCycle]:
    """Seed old/current projections and freeze only old publication roots.

    The first root owns ``rows`` children. Additional roots each own one
    contributor, providing the maximum real frozen-predicate bind footprint.
    """
    initialize_database(config)
    backend = backend_of(config)
    assert 1 <= root_count <= 256
    assert root_count == 1 or phase == "CP_CONTRIBUTOR"
    if variable_keys is not None:
        assert len(variable_keys) == rows
    with closing(open_connector(config)) as connector:
        with partial_publication_setup(connector, backend=backend):
            connector.execute(
                "INSERT INTO catalog_publication_identities (publication_key, gid) "
                "VALUES (%s, 7)",
                (PUBLICATION_KEY,),
            )
            for revision, receipt in ((1, OLD_RECEIPT), (2, CURRENT_RECEIPT)):
                connector.execute(
                    "INSERT INTO catalog_revision_descriptors "
                    "(revision, publication_count, artifact_count) VALUES (%s, 1, 0)",
                    (revision,),
                )
                connector.execute(
                    "INSERT INTO catalog_source_revision_descriptors "
                    "(source_revision, channel, snapshot_manifest_sha256) "
                    "VALUES (%s, %s, %s)",
                    (revision, b"default", bytes((revision,)) * 32),
                )
                connector.execute(
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (
                        vnext_identity.catalog_publication_occurrence_sha256(
                            revision, PUBLICATION_KEY
                        ),
                        revision,
                        PUBLICATION_KEY,
                    ),
                )
                seed_publication_commit(
                    connector,
                    receipt_id=receipt,
                    candidate_id=bytes((revision,)) * 16,
                    revision=revision,
                    source_revision=revision,
                    generation=revision,
                    preparation_id=bytes((revision + 2,)) * 16,
                    operational_policy_id=1,
                    artifact_policy_id=1,
                    display_title_policy_id=1,
                    new_galleries=1,
                    changed_galleries=0,
                    removed_galleries=0,
                    duplicate_losers=0,
                    committed_at=revision,
                )
                seed_publication_finalization(
                    connector,
                    receipt_id=receipt,
                    cursor=PUBLICATION_KEY,
                    processed_count=1,
                    finalized_at=revision + 10,
                )
                for position in range(rows):
                    value = (position + 1).to_bytes(32, "big")
                    variable = (
                        variable_keys[position]
                        if variable_keys is not None
                        else b"artist"
                    )
                    if phase == "CP_STORAGE":
                        connector.execute(
                            "INSERT INTO catalog_title_search_postings "
                            "(revision, value_sha256, publication_key) "
                            "VALUES (%s, %s, %s)",
                            (revision, value, PUBLICATION_KEY),
                        )
                    elif phase == "CP_SUBJECT":
                        connector.execute(
                            "INSERT INTO catalog_subjects "
                            "(revision, publication_key, position, tag_id) "
                            "VALUES (%s, %s, %s, %s)",
                            (revision, PUBLICATION_KEY, position, position + 1),
                        )
                    elif phase == "CP_CONTRIBUTOR":
                        connector.execute(
                            "INSERT INTO catalog_contributors "
                            "(revision, publication_key, contributor_name_sha256, "
                            "role, position) VALUES (%s, %s, %s, %s, %s)",
                            (revision, PUBLICATION_KEY, value, variable, position),
                        )
                    elif phase == "CP_ORDER":
                        if revision == 1:
                            connector.execute(
                                "INSERT INTO catalog_tag_terms "
                                "(tag_id, namespace, tag_value_sha256) "
                                "VALUES (%s, %s, %s)",
                                (position + 1, variable, value),
                            )
                        connector.execute(
                            "INSERT INTO catalog_tag_directory_order "
                            "(revision, namespace, position, tag_value_sha256) "
                            "VALUES (%s, %s, %s, %s)",
                            (revision, variable, position, value),
                        )
                        connector.execute(
                            "INSERT INTO catalog_tag_publication_order "
                            "(revision, tag_id, position, publication_key) "
                            "VALUES (%s, %s, 0, %s)",
                            (revision, position + 1, PUBLICATION_KEY),
                        )
                    else:
                        raise AssertionError(f"unsupported test phase {phase}")
            for ordinal in range(1, root_count):
                publication_key = bytes((201,)) + ordinal.to_bytes(31, "big")
                connector.execute(
                    "INSERT INTO catalog_publication_identities "
                    "(publication_key, gid) VALUES (%s, %s)",
                    (publication_key, 7 + ordinal),
                )
                connector.execute(
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, 1, %s)",
                    (
                        vnext_identity.catalog_publication_occurrence_sha256(
                            1, publication_key
                        ),
                        publication_key,
                    ),
                )
                connector.execute(
                    "INSERT INTO catalog_contributors "
                    "(revision, publication_key, contributor_name_sha256, role, position) "
                    "VALUES (1, %s, %s, %s, 0)",
                    (publication_key, (1).to_bytes(32, "big"), b"artist"),
                )
            connector.execute(
                "INSERT INTO catalog_publication_commit_head_receipts "
                "(channel, receipt_id) VALUES (%s, %s)",
                (b"default", CURRENT_RECEIPT),
            )
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend),
                now=1,
                lease_duration=100_000,
            )
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                target_kind=CleanupTargetKind.CATALOG_PUBLICATION,
                shard_no=PUBLICATION_KEY[0],
                cycle_cutoff_at=100,
                max_rows_per_transaction=max_rows,
                now=2,
            )
    return gate, cycle
