"""Public, valid seeds remain reusable without disabling native constraints."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from types import ModuleType

import pytest
from mysql.connector.errors import DatabaseError as MariaDBError
from test_vnext_source_marker import MarkerSource
from vnext_pipeline import (
    MemoryLibrary,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
)
from vnext_test_database import DatabaseFactory, database_connector, inspect_one

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.schema_epoch import SCHEMA_EPOCH_CONTROL_TABLE


@pytest.fixture
def snapshots() -> Iterator[ModuleType]:
    name = "public_fixture_snapshot_under_test"
    path = Path(__file__).resolve().parents[1] / "scripts/public_fixture_snapshot.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        del sys.modules[name]


def _publish(config: CoreConfig, source: MarkerSource, library: MemoryLibrary) -> None:
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        assert receipt.sealed
        run_analysis(facade, session, policy, receipt.build_id)
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
    assert full_check(config).state == "READY"


def test_public_marker_seed_copies_restores_and_reuses_with_fk_enforcement(
    database_factory: DatabaseFactory, snapshots: ModuleType, tmp_path: Path
) -> None:
    values = [gallery(1001, pages=[b"one", b"two"]), gallery(1002, pages=[b"three"])]
    with (
        snapshots.owned_database_from_factory(database_factory) as seed,
        snapshots.owned_database_from_factory(database_factory) as target,
    ):
        initialize_database(seed.config)
        source = MarkerSource(values)
        seed_library = MemoryLibrary(source)
        _publish(seed.config, source, seed_library)
        sealed = snapshots.seal_public_seed(seed)
        assert sealed["READY"] == "READY"
        marker = next(
            row
            for row in sealed["tables"]
            if row["table"] == "catalog_gallery_observation_completion_marker"
        )
        assert marker["rows"] == 2
        with pytest.raises(ValueError, match="another writer"):
            _ = seed.config
        receipts = [sealed]
        for _ in range(2):
            copied = snapshots.clone_public_seed(seed, target)
            assert copied == sealed
            receipts.append(copied)
            changed = gallery(1002, title="Changed title", pages=[b"three"])
            reused = MarkerSource([values[0], changed])
            reused.forbidden_reads.add(values[0].locator)
            # The DB copy intentionally excludes adapter state. Even without
            # artifact bytes, activation journal authority must accompany it.
            library = deepcopy(seed_library)
            library.source = reused
            _publish(target.config, reused, library)
            assert reused.deep_reads == [changed.locator]
            with database_connector(target.config) as reader:
                assert inspect_one(
                    reader, "SELECT COUNT(*) FROM catalog_publication_commits"
                ) == (2,)
        # Repeated clone restores the original public graph, including rows
        # deleted or added by the previous complete generation.
        restored = snapshots.clone_public_seed(seed, target)
        assert restored == sealed
        receipts.append(restored)
        (tmp_path / "public-snapshot-proof.json").write_text(
            json.dumps({"seed_and_repeated_clone_receipts": receipts}, indent=2) + "\n"
        )
        with database_connector(target.config) as writer:
            writer.execute(
                "CREATE TABLE unexpected_fixture_table (id INTEGER PRIMARY KEY)"
            )
        with pytest.raises(ValueError, match="schema drift"):
            snapshots.clone_public_seed(seed, target)


def test_public_snapshot_rejects_unowned_alias_and_unsealed_source(
    database_factory: DatabaseFactory, snapshots: ModuleType
) -> None:
    with (
        snapshots.owned_database_from_factory(database_factory) as seed,
        snapshots.owned_database_from_factory(database_factory) as target,
    ):
        with pytest.raises(ValueError, match="separate owned"):
            snapshots.clone_public_seed(seed, seed)
        with pytest.raises(ValueError, match="sealed source"):
            snapshots.clone_public_seed(seed, target)
        forged = snapshots.OwnedDatabase(seed.config)
        with pytest.raises(ValueError, match="live owned"):
            snapshots.clone_public_seed(forged, target)
    with pytest.raises(ValueError, match="live owned"):
        _ = target.config


def test_public_snapshot_rejects_a_seed_changed_after_sealing(
    database_factory: DatabaseFactory, snapshots: ModuleType
) -> None:
    with (
        snapshots.owned_database_from_factory(database_factory) as seed,
        snapshots.owned_database_from_factory(database_factory) as target,
    ):
        stale_config = seed.config
        initialize_database(stale_config)
        source = MarkerSource([gallery(1001, pages=[b"one"])])
        library = MemoryLibrary(source)
        _publish(stale_config, source, library)
        snapshots.seal_public_seed(seed)
        # A saved config is not a new authority: even a semantically valid
        # public successor invalidates the sealed byte/row identity.
        source.put(gallery(1001, title="Later title", pages=[b"one"]))
        _publish(stale_config, source, library)
        with pytest.raises(ValueError, match="seed content changed"):
            snapshots.clone_public_seed(seed, target)


def test_public_seed_sealing_rejects_an_active_native_writer(
    database_factory: DatabaseFactory, snapshots: ModuleType
) -> None:
    with snapshots.owned_database_from_factory(database_factory) as seed:
        config = seed.config
        initialize_database(config)
        with database_connector(config) as writer, writer.transaction():
            # Acquire a real row/write lock without changing valid public data.
            writer.execute(
                f"UPDATE {SCHEMA_EPOCH_CONTROL_TABLE} SET ready_at = ready_at"
            )
            with pytest.raises(
                (sqlite3.OperationalError, MariaDBError),
                match="locked|Lock wait timeout",
            ):
                snapshots.seal_public_seed(seed)
        assert snapshots.seal_public_seed(seed)["READY"] == "READY"
