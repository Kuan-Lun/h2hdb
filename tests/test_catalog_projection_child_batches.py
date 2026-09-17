from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import test_vnext_publication_candidate_repository as fixture
from vnext_catalog_identity_fixtures import seed_tag_term

from h2hdb import vnext_identity as identity
from h2hdb import vnext_publication_candidate_repository as module
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateConflictError,
    PublicationCatalogProjectionPlan,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateRepository as Repository,
)
from h2hdb.vnext_publication_family import (
    CatalogPublicationFamily,
    compare_catalog_publication_families,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


@contextmanager
def _catalog(
    path: Path,
    *,
    count: int = 1,
) -> Iterator[
    tuple[
        SQLiteConnector,
        PublicationCatalogProjectionPlan,
        PublicationCatalogProjectionPlan,
        GateLease,
        IngestTurn,
    ]
]:
    connector = fixture._generated_database(path)
    try:
        gate, turn = fixture._authorities(connector)
        with connector.transaction():
            fixture._seed_completed_analysis(connector, turn, with_base=False)
            fixture._seed_selected_galleries(connector, count=count)
            fixture._seed_projection_metadata(connector, count=count)
            for tag_id, (namespace, value) in enumerate(
                ((b"language", b"english"), (b"artist", b"Artist")), 1
            ):
                value_digest = identity.canonical_value_digest(
                    "tag_value_utf8_v1", value
                )
                fixture._canonical_identity(
                    connector,
                    value_digest,
                    domain=b"tag_value_utf8_v1",
                    serial=10_000 + tag_id,
                    payload=value,
                )
                seed_tag_term(
                    connector,
                    tag_id=tag_id,
                    namespace=namespace,
                    tag_value_sha256=value_digest,
                )
            connector.execute_many(
                "INSERT INTO catalog_gallery_observation_tags (gallery_id, observation_id, position, tag_id) VALUES (%s, %s, %s, %s)",
                [
                    (gallery, 1, position, position + 1)
                    for gallery in range(1, count + 1)
                    for position in range(2)
                ],
            )
        fixture._begin(connector, gate, turn, artifacts_required=True)
        fixture._complete_selection(connector, gate, turn)
        with connector.transaction():
            authority = Repository.issue_projection_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=fixture._CANDIDATE,
                now=110,
            )
        with ExitStack() as stack:
            plan = stack.enter_context(
                Repository.prepare_catalog_projection(
                    connector, backend="sqlite", authority=authority
                )
            )
            validation = stack.enter_context(
                Repository.prepare_catalog_projection_validation(
                    connector, backend="sqlite", authority=authority
                )
            )
            fixture._upload_projection_canonical_values(
                connector, gate, turn, plan, now=111
            )
            for index in range(plan.child_count // 128 + 2):
                with connector.transaction():
                    batch = Repository.process_catalog_projection_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=fixture._CANDIDATE,
                        plan=plan,
                        batch_key=b"build" + index.to_bytes(4, "big"),
                        now=112 + index,
                    )
                assert batch.row_count <= 128
                if batch.terminal:
                    break
            else:
                raise AssertionError("catalog build did not terminate")
            yield connector, plan, validation, gate, turn
    finally:
        connector.close()


def _children(
    plan: PublicationCatalogProjectionPlan,
) -> tuple[module._ProjectionChild, ...]:
    result: list[module._ProjectionChild] = []
    after = b""
    while rows := plan._page_after(after):
        result.extend(rows)
        after = rows[-1].cursor
    return tuple(result)


def test_subject_128_child_comparison_and_persistence_each_use_one_statement(
    tmp_path: Path,
) -> None:
    with _catalog(tmp_path / "bounded.sqlite3", count=64) as (
        connector,
        plan,
        validation,
        _,
        _,
    ):
        subjects = tuple(
            child
            for child in _children(validation)
            if child.kind == module._CatalogChildKind.SUBJECT
        )
        assert len(subjects) == 128
        work = VNextUnitOfWork(connector, backend="sqlite")
        with connector.transaction():
            authority = module._load_projection_authority(work, plan.authority)
            publications = tuple(
                child
                for child in _children(validation)
                if child.kind == module._CatalogChildKind.PUBLICATION
            )
            with patch.object(
                connector, "fetch_all", wraps=connector.fetch_all
            ) as family_reads:
                module._compare_projection_children(
                    work, authority, validation, publications
                )
            assert family_reads.call_count == 2
            with patch.object(
                connector, "fetch_all", wraps=connector.fetch_all
            ) as reads:
                module._compare_projection_children(
                    work, authority, validation, subjects
                )
            assert reads.call_count == 1
            assert "catalog_subjects" in reads.call_args.args[0]
            assert reads.call_args.args[1][-1] == 129
            connector.execute(
                "DELETE FROM catalog_tag_publication_order WHERE revision = 1"
            )
            connector.execute("DELETE FROM catalog_subjects WHERE revision = 1")
            canonical = module._prepare_projection_canonical_batch(work, plan, subjects)
            with patch.object(
                connector, "execute_many", wraps=connector.execute_many
            ) as writes:
                module._insert_projection_children(
                    work, authority, plan, subjects, canonical
                )
            assert writes.call_count == 1
            assert len(writes.call_args.args[1]) == 128
            module._compare_projection_children(work, authority, validation, subjects)
            with pytest.raises(ValueError, match="exceeds 128"):
                module._compare_projection_children(
                    work, authority, validation, (*subjects, subjects[0])
                )
            with pytest.raises(ValueError, match="exceeds 128"):
                module._insert_projection_children(
                    work, authority, plan, (*subjects, subjects[0]), canonical
                )


def test_physical_publication_batch_rejects_129_rows_before_query(
    tmp_path: Path,
) -> None:
    connector = fixture._generated_database(tmp_path / "family-bound.sqlite3")
    try:
        family = CatalogPublicationFamily(
            1, b"p" * 32, 1, b"s" * 32, b"l" * 32, 1, b"t" * 32
        )
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as reads:
            with pytest.raises(ValueError, match="exceeds 128"):
                compare_catalog_publication_families(connector, (family,) * 129)
        assert reads.call_count == 0
    finally:
        connector.close()


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE catalog_publication_storage SET modified_at = modified_at + 1",
        "DELETE FROM catalog_publication_storage",
        "UPDATE catalog_publication_download_times SET download_time = download_time + 1",
        "UPDATE catalog_gallery_upload_times SET upload_time = upload_time + 1",
        "DELETE FROM catalog_gallery_upload_times",
        "UPDATE catalog_publication_occurrence_identities SET revision = revision + 1",
        "UPDATE catalog_subjects SET tag_id = 3 WHERE position = 0",
        "UPDATE catalog_contributors SET role = X'77726f6e67'",
        "UPDATE catalog_display_title_choices SET title_sha256 = source_title_sha256",
        "UPDATE catalog_title_sorts SET sort_title_sha256 = title_sha256",
        "UPDATE catalog_search_documents SET row_count = row_count + 1",
        "DELETE FROM catalog_search_postings WHERE value_sha256 IN (SELECT value_sha256 FROM catalog_title_search_postings)",
        "UPDATE catalog_language_facet_order SET occurrence_count = occurrence_count + 1",
        "UPDATE catalog_tag_publication_order SET position = position + 1",
        "UPDATE catalog_tag_directory_order SET position = position + 1",
    ],
)
def test_batched_comparison_rejects_corruption_without_checkpoint_progress(
    tmp_path: Path, corruption: str
) -> None:
    with _catalog(tmp_path / "corruption.sqlite3") as (
        connector,
        _,
        validation,
        gate,
        turn,
    ):
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(corruption)
        with pytest.raises(PublicationCandidateConflictError), connector.transaction():
            Repository.validate_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=fixture._CANDIDATE,
                validation=validation,
                batch_key=b"corrupt",
                now=10_000,
            )
        assert connector.fetch_one(
            "SELECT generation, cursor, processed_count, state FROM catalog_publication_checkpoints WHERE candidate_id = %s AND stage = %s",
            (fixture._CANDIDATE, b"VALIDATE_CATALOG_PROJECTION"),
        ) == (1, b"", 0, "OPEN")


def test_search_canonical_domain_and_partial_family_are_checked_in_batches(
    tmp_path: Path,
) -> None:
    with _catalog(tmp_path / "canonical.sqlite3") as (connector, _, validation, _, _):
        postings = tuple(
            child
            for child in _children(validation)
            if child.kind == module._CatalogChildKind.SEARCH_POSTING
        )
        assert len(postings) > 1
        work = VNextUnitOfWork(connector, backend="sqlite")
        with connector.transaction():
            authority = module._load_projection_authority(work, validation.authority)
            queries: list[str] = []
            original = connector.fetch_all

            def record(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
                queries.append(query)
                return original(query, data)

            with patch.object(connector, "fetch_all", side_effect=record):
                module._compare_projection_children(
                    work, authority, validation, postings
                )
            assert len(queries) == 3
            assert (
                sum(
                    "catalog_canonical_value_allocation_anchors" in query
                    for query in queries
                )
                == 1
            )
        value = postings[0].subkey
        connector.execute(
            "UPDATE catalog_canonical_value_allocation_digest_domains SET digest_domain = %s WHERE value_sha256 = %s",
            (b"source_title_utf8_v1", value),
        )
        with (
            pytest.raises(PublicationCandidateConflictError, match="wrong domain"),
            connector.transaction(),
        ):
            module._compare_projection_children(work, authority, validation, postings)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_canonical_value_allocation_digest_domains WHERE value_sha256 = %s",
            (value,),
        )
        with (
            pytest.raises(
                PublicationCandidateConflictError, match="partial or corrupt"
            ),
            connector.transaction(),
        ):
            module._compare_projection_children(work, authority, validation, postings)


@pytest.mark.parametrize(
    "backend",
    ["sqlite", pytest.param("mariadb", marks=[pytest.mark.mariadb, pytest.mark.deep])],
)
def test_real_backend_child_batches_replay_and_reject_physical_corruption(
    backend: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    from test_vnext_publication_canonical_batch import _database

    from h2hdb.vnext_canonical_value_repository import CanonicalValueRepository

    with _database(backend, tmp_path, request) as (connector, gate, turn, plan):
        for upload in plan.iter_canonical_value_plans():
            try:
                with connector.transaction():
                    CanonicalValueRepository.allocate(
                        VNextUnitOfWork(connector, backend=backend),
                        gate_lease=gate,
                        ingest_turn=turn,
                        plan=upload,
                        now=111,
                    )
                for page in upload.iter_pages():
                    with connector.transaction():
                        CanonicalValueRepository.put_page(
                            VNextUnitOfWork(connector, backend=backend),
                            gate_lease=gate,
                            ingest_turn=turn,
                            plan=upload,
                            prepared_page=page,
                            now=111,
                        )
                with connector.transaction():
                    CanonicalValueRepository.seal(
                        VNextUnitOfWork(connector, backend=backend),
                        gate_lease=gate,
                        ingest_turn=turn,
                        plan=upload,
                        now=111,
                    )
            finally:
                upload.close()
        with Repository.prepare_catalog_projection_validation(
            connector, backend=backend, authority=plan.authority
        ) as validation:
            with connector.transaction():
                built = Repository.process_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=fixture._CANDIDATE,
                    plan=plan,
                    batch_key=b"build",
                    now=120,
                )
            assert 1 < built.row_count <= 128
            with connector.transaction():
                replay = Repository.process_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=fixture._CANDIDATE,
                    plan=plan,
                    batch_key=b"build",
                    now=121,
                )
            assert replay.replayed and replay.committed_at == built.committed_at
            with connector.transaction():
                terminal = Repository.process_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=fixture._CANDIDATE,
                    plan=plan,
                    batch_key=b"build-terminal",
                    now=122,
                )
            assert (
                terminal.terminal and terminal.next_processed_count == plan.child_count
            )
            with connector.transaction():
                connector.execute(
                    "UPDATE catalog_publication_download_times SET download_time = download_time + 1"
                )
            with (
                pytest.raises(
                    PublicationCandidateConflictError, match="occurrence family"
                ),
                connector.transaction(),
            ):
                Repository.validate_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=fixture._CANDIDATE,
                    validation=validation,
                    batch_key=b"bad-download",
                    now=123,
                )
            with connector.read_transaction():
                assert connector.fetch_one(
                    "SELECT processed_count FROM catalog_publication_checkpoints WHERE candidate_id = %s AND stage = %s",
                    (fixture._CANDIDATE, b"VALIDATE_CATALOG_PROJECTION"),
                ) == (0,)
            with connector.transaction():
                connector.execute(
                    "UPDATE catalog_publication_download_times SET download_time = download_time - 1"
                )
            for index in range(2):
                with connector.transaction():
                    checked = Repository.validate_catalog_projection_batch(
                        VNextUnitOfWork(connector, backend=backend),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=fixture._CANDIDATE,
                        validation=validation,
                        batch_key=b"validate" + bytes((index,)),
                        now=124 + index,
                    )
            assert checked.terminal and checked.next_processed_count == plan.child_count
