"""Fixed FILE-page SQL budgets through real source facades on both backends.

These finite counts cover 128- and 256-file boundaries and real scalar negative
controls. They do not establish complete ingest or NAS wall-clock acceptance.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any, Literal

import pytest
from vnext_fault_harness import (
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
    snapshot_database,
)
from vnext_pipeline import (
    MemoryGallery,
    MemorySource,
    claim_session,
    ingest_policy,
    initialize_database,
)

import h2hdb.vnext_gallery_staging_repository as staging
from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.sql_performance import measure_sql
from h2hdb.vnext_catalog_identity_family import (
    ensure_file_name_identities,
    ensure_gallery_observation_files,
)
from h2hdb.vnext_identity import file_key

pytestmark = pytest.mark.deep


@dataclass
class _Counter:
    queries: list[str] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        _elapsed: float,
        query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.queries.append(" ".join(query.split()))


def _budget(files: int, galleries: int = 1) -> int:
    # The prior acceptance contract, fixed independently of these results.
    return galleries * (8 * files + 256 * ((files + 127) // 128))


def _source(files: int, *, galleries: int = 1) -> MemorySource:
    records = []
    for gid in range(1, galleries + 1):
        values = {
            f"{position:04d}.png".encode(): (
                b"repeated-across-pages" if position % 2 else b"second-content"
            )
            for position in range(files - 2)
        }
        # This name hashes to a non-UTF-8 key, exercising native binary binds.
        values[b"directory-0000"] = b"opaque non-image file"
        values[b"galleryinfo.txt"] = f"gallery-{gid}".encode()
        assert len(values) == files
        records.append(
            MemoryGallery(
                locator=(f"gallery-{gid}",),
                gid=gid,
                title=f"Gallery {gid}",
                files=values,
            )
        )
    return MemorySource(records)


def _collect(
    facade: VNextIngestFacade,
    source: MemorySource,
    *,
    before_file: Callable[[Any, Any], None] | None = None,
) -> tuple[int, ...]:
    session = claim_session(facade)
    policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
    counts = []
    with facade.prepare_source(source, policy=policy) as prepared:
        for _ in range(10000):
            issued = facade.issue_source_step(session, policy, prepared)
            local = facade.prepare_source_step(prepared, issued)
            if issued._action.value == "FILE_PAGE":
                if before_file is not None:
                    before_file(session, local)
                counter = _Counter()
                with measure_sql(counter, observe_nested=True):
                    result = facade.commit_source_step(session, local)
                counts.append(len(counter.queries))
            else:
                result = facade.commit_source_step(session, local)
            if result.terminal:
                assert result.source_receipt is not None
                assert result.source_receipt.sealed
                return tuple(counts)
    pytest.fail("source did not reach its sealed receipt")


def _assert_facts(config: CoreConfig, *, files: int, galleries: int = 1) -> None:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_gallery_observation_file_anchors"
        ) == (files * galleries,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_gallery_observation_file_filesystem_seals"
        ) == (files * galleries,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_gallery_observation_file_filesystem_anchors "
            "WHERE file_key = %s",
            (file_key(b"directory-0000"),),
        ) == (galleries,)
        rows = connector.fetch_all(
            "SELECT gallery_id, SUM(occurrence_count) "
            "FROM catalog_gallery_observation_file_hash_occurrences "
            "GROUP BY gallery_id ORDER BY gallery_id"
        )
        assert [(row[0], int(row[1])) for row in rows] == [
            (gid, files - 1) for gid in range(1, galleries + 1)
        ]


@pytest.mark.parametrize("files", [127, 128, 129, 255, 256, 257])
def test_file_page_cost_stays_within_fixed_budget_across_repeated_pages(
    db_config: CoreConfig, files: int
) -> None:
    initialize_database(db_config)
    with VNextIngestFacade(db_config) as facade:
        counts = _collect(facade, _source(files, galleries=2))
    assert counts and all(count > 0 for count in counts)
    assert sum(counts) <= _budget(files, galleries=2)
    _assert_facts(db_config, files=files, galleries=2)


def _scalar_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute the real current writes one entry at a time, preserving results."""

    names = ensure_file_name_identities
    occurrences = ensure_gallery_observation_files
    blobs = staging._persist_content_blobs
    filesystem = staging._persist_file_filesystem_facts

    def scalar_names(connector: Any, *, identities: Sequence[Any]) -> Any:
        for identity in identities:
            names(connector, identities=(identity,))

    def scalar_occurrences(connector: Any, *, identities: Sequence[Any]) -> Any:
        for identity in identities:
            occurrences(connector, identities=(identity,))

    def scalar_blobs(connector: Any, sources: Sequence[Any]) -> None:
        for source in sources:
            blobs(connector, (source,))

    def scalar_filesystem(
        connector: Any,
        *,
        gallery_id: int,
        observation_id: int,
        facts: dict[bytes, tuple[bytes, bytes, bytes, bytes]],
    ) -> None:
        for key, value in facts.items():
            filesystem(
                connector,
                gallery_id=gallery_id,
                observation_id=observation_id,
                facts={key: value},
            )

    monkeypatch.setattr(staging, "ensure_file_name_identities", scalar_names)
    monkeypatch.setattr(staging, "ensure_gallery_observation_files", scalar_occurrences)
    monkeypatch.setattr(staging, "_persist_content_blobs", scalar_blobs)
    monkeypatch.setattr(staging, "_persist_file_filesystem_facts", scalar_filesystem)


def test_real_scalar_persistence_is_rejected_by_unchanged_file_page_budget(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    _scalar_helpers(monkeypatch)
    with VNextIngestFacade(db_config) as facade:
        counts = _collect(facade, _source(257))
    assert sum(counts) > _budget(257)
    _assert_facts(db_config, files=257)


@pytest.mark.parametrize(
    "fault",
    [
        "catalog_file_name_identities",
        "catalog_content_blobs",
        "catalog_gallery_observation_file_seals",
        "catalog_gallery_observation_file_filesystem_seals",
        "catalog_gallery_observation_file_hash_occurrences",
        "response_loss",
    ],
)
def test_file_batch_fault_retries_preserve_exact_facts(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    initialize_database(db_config)
    injector = FaultInjector()
    fired = False
    with (
        fault_injection(monkeypatch, injector),
        VNextIngestFacade(db_config) as facade,
    ):

        def interrupt(session: Any, local: Any) -> None:
            nonlocal fired
            if fired:
                return
            fired = True
            before = snapshot_database(db_config)
            family_written = False
            if fault != "response_loss":

                def fail_after_family(query: str) -> None:
                    nonlocal family_written
                    if family_written:
                        raise InjectedFault(f"after {fault} write")
                    if query.startswith(f"INSERT INTO {fault} "):
                        family_written = True

                injector.on_before_mutation = fail_after_family
            else:
                injector.fail_after_commit = injector.commits + 1
            with pytest.raises(InjectedFault):
                facade.commit_source_step(session, local)
            injector.on_before_mutation = None
            injector.fail_after_commit = None
            if fault != "response_loss":
                assert family_written
                assert snapshot_database(db_config) == before
            else:
                assert snapshot_database(db_config) != before
            # _collect retries the exact still-pending public source step.

        _collect(facade, _source(257), before_file=interrupt)
    assert fired
    _assert_facts(db_config, files=257)


def test_response_loss_replay_rejects_partial_binary_filesystem_family(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    injector = FaultInjector()
    fired = False
    with (
        fault_injection(monkeypatch, injector),
        VNextIngestFacade(db_config) as facade,
    ):

        def corrupt(session: Any, local: Any) -> None:
            nonlocal fired
            if fired:
                return
            fired = True
            injector.fail_after_commit = injector.commits + 1
            with pytest.raises(InjectedFault):
                facade.commit_source_step(session, local)
            injector.fail_after_commit = None
            with (
                closing(open_connector(db_config)) as connector,
                connector.transaction(),
            ):
                key = file_key(b"directory-0000")
                assert connector.fetch_one(
                    "SELECT file_key "
                    "FROM catalog_gallery_observation_file_filesystem_anchors "
                    "WHERE file_key = %s",
                    (key,),
                ) == (key,)
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_file_filesystem_seals "
                    "WHERE file_key = %s",
                    (key,),
                )
            before = snapshot_database(db_config)
            with pytest.raises(staging.GalleryStagingConflictError):
                facade.commit_source_step(session, local)
            assert snapshot_database(db_config) == before
            raise InjectedFault("validated partial-family rejection")

        with pytest.raises(InjectedFault, match="validated partial-family rejection"):
            _collect(facade, _source(127), before_file=corrupt)
    assert fired
