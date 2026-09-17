"""GID-only capability preserves immutable authority and commit fences."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from vnext_pipeline import (
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_source,
)

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextIngestFacade,
    VNextIngestSession,
    VNextIssuedAnalysisStep,
    VNextPreparedAnalysis,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import (
    AnalysisCorruptionError,
    AnalysisGalleryPreparation,
    AnalysisGidPreparation,
    AnalysisNotReadyError,
    AnalysisRepository,
    AnalysisRepositoryError,
    _require_preparation_kind,
)
from h2hdb.vnext_ingest_analysis import _LocalAnalysisWork


@dataclass(frozen=True)
class _GidIssue:
    facade: VNextIngestFacade
    session: VNextIngestSession
    analysis: VNextPreparedAnalysis
    issued: VNextIssuedAnalysisStep
    path: Path


@contextmanager
def _issue(tmp_path: Path, stage: bytes) -> Iterator[_GidIssue]:
    path = tmp_path / "gid.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    source = MemorySource(
        [
            gallery(
                1,
                title="GID authority",
                pages=[b"one unique PAGE"],
                extra_tags=[("group", "tag")],
            )
        ]
    )
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        with facade.prepare_analysis(receipt.build_id, policy, max_rows=1) as analysis:
            for _ in range(1_000):
                issued = facade.issue_analysis_step(session, analysis)
                payload = issued._payload
                if (
                    payload is not None
                    and payload.stage == stage
                    and payload.memberships
                ):
                    yield _GidIssue(facade, session, analysis, issued, path)
                    return
                prepared = facade.prepare_analysis_step(analysis, issued)
                result = facade.commit_analysis_step(session, prepared)
                assert not result.terminal, "target GID issue was never reached"
    pytest.fail("GID issue exceeded its bounded test budget")


def _change_normalized_gid(connector: SQLConnector) -> None:
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (2, 0)"
    )
    connector.execute(
        "UPDATE catalog_source_gallery_name_gids SET gid = 2 WHERE gid = 1"
    )


@pytest.mark.parametrize("stage", [b"gid_candidate", b"validate_gid_candidate"])
@pytest.mark.parametrize("fault", ["metadata_bytes", "normalized_gid", "qualification"])
def test_gid_preparation_revalidates_stream_and_qualification_in_each_stage(
    tmp_path: Path, stage: bytes, fault: str
) -> None:
    with _issue(tmp_path, stage) as case:
        with SQLiteConnector(str(case.path)) as connector, connector.transaction():
            if fault == "normalized_gid":
                _change_normalized_gid(connector)
                message = "normalized GID differs"
            elif fault == "qualification":
                connector.execute(
                    "UPDATE catalog_gallery_observation_validation_policies "
                    "SET qualification_policy_sha256 = %s",
                    (b"x" * 32,),
                )
                message = "qualification differs"
            else:
                root = connector.fetch_one(
                    "SELECT root.root_page_sha256, page.page_bytes "
                    "FROM catalog_gallery_observation_tree_roots AS root "
                    "JOIN catalog_gallery_observation_page_descriptor_components AS descriptor "
                    "ON descriptor.page_sha256 = root.root_page_sha256 "
                    "JOIN catalog_gallery_observation_pages AS page "
                    "ON page.page_sha256 = root.root_page_sha256 "
                    "WHERE descriptor.component = %s",
                    (b"METADATA",),
                )
                assert len(root) == 2
                damaged = root[1][:-1] + bytes([root[1][-1] ^ 1])
                connector.execute(
                    "UPDATE catalog_gallery_observation_pages SET page_bytes = %s "
                    "WHERE page_sha256 = %s",
                    (damaged, root[0]),
                )
                message = "metadata page digest differs"
        with pytest.raises(AnalysisCorruptionError, match=message):
            case.facade.prepare_analysis_step(case.analysis, case.issued)


@pytest.mark.parametrize("stage", [b"gid_candidate", b"validate_gid_candidate"])
@pytest.mark.parametrize(
    "fault", ["membership", "gid", "generation", "content_capability"]
)
def test_gid_commit_rejects_changed_authority_and_wrong_capability(
    tmp_path: Path, stage: bytes, fault: str
) -> None:
    with _issue(tmp_path, stage) as case:
        prepared = case.facade.prepare_analysis_step(case.analysis, case.issued)
        local = prepared._payload
        assert isinstance(local, _LocalAnalysisWork)
        assert len(local.preparations) == 1
        gid = local.preparations[0]
        assert isinstance(gid, AnalysisGidPreparation)
        error: type[AnalysisRepositoryError]
        if fault == "membership":
            local.preparations = (replace(gid, observation_id=gid.observation_id + 1),)
            error, message = AnalysisNotReadyError, "current membership"
        elif fault == "generation":
            local.preparations = (
                replace(
                    gid,
                    authority=replace(
                        gid.authority, generation=gid.authority.generation + 1
                    ),
                ),
            )
            error, message = AnalysisNotReadyError, "generation is stale"
        elif fault == "content_capability":
            with SQLiteConnector(str(case.path)) as connector:
                full = AnalysisRepository.prepare_gallery(
                    connector,
                    backend="sqlite",
                    authority=gid.authority,
                    gallery_id=gid.gallery_id,
                )
            local.preparations = (full,)
            error, message = AnalysisNotReadyError, "another stage family"
        else:
            with SQLiteConnector(str(case.path)) as connector, connector.transaction():
                _change_normalized_gid(connector)
            error, message = (
                AnalysisCorruptionError,
                "GID preparation changed metadata group",
            )
        with pytest.raises(error, match=message):
            case.facade.commit_analysis_step(case.session, prepared)


def test_gid_only_capability_cannot_authorize_content_and_requires_current_seals(
    tmp_path: Path,
) -> None:
    with _issue(tmp_path, b"gid_candidate") as case:
        prepared = case.facade.prepare_analysis_step(case.analysis, case.issued)
        local = prepared._payload
        assert isinstance(local, _LocalAnalysisWork)
        gid = local.preparations[0]
        assert isinstance(gid, AnalysisGidPreparation)
        with pytest.raises(AnalysisNotReadyError, match="another stage family"):
            _require_preparation_kind((gid,), AnalysisGalleryPreparation)
        with pytest.raises(TypeError, match="repository-issued"):
            replace(gid, _capability=object())
        with SQLiteConnector(str(case.path)) as connector:
            missing_owner = replace(
                gid.authority,
                component_seals=tuple(
                    row
                    for row in gid.authority.component_seals
                    if row[0] != b"content_owner"
                ),
            )
            with pytest.raises(
                AnalysisNotReadyError, match="sealed file decisions and content owners"
            ):
                AnalysisRepository.prepare_gid_gallery(
                    connector,
                    backend="sqlite",
                    authority=missing_owner,
                    gallery_id=gid.gallery_id,
                )
            with pytest.raises(
                AnalysisNotReadyError, match="generation mapping changed"
            ):
                AnalysisRepository.prepare_gid_gallery(
                    connector,
                    backend="sqlite",
                    authority=replace(
                        gid.authority, generation=gid.authority.generation + 1
                    ),
                    gallery_id=gid.gallery_id,
                )
