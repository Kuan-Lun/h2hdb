from __future__ import annotations

import sqlite3
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO
from unicodedata import unidata_version

import pytest

from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactReleaseStorageEvidence,
    ArtifactStorageAdapter,
    ArtifactStorageEvidence,
    ArtifactTransformKind,
    CoreConfig,
    DatabaseConfig,
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    TagObservation,
    VNextArtifactProducer,
    VNextArtifactStoragePolicy,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextIngestPolicy,
    VNextIngestSession,
    VNextIngestSourceAdapter,
    VNextIssuedSourceStep,
    VNextPreparedSource,
    VNextPreparedSourceStep,
    artifact_producer_fingerprint_sha256,
)
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_ingest_policy_repository import VNextIngestPolicyConflictError


def _metadata() -> GalleryObservationMetadata:
    return GalleryObservationMetadata(
        gid=1,
        title="title",
        comment="",
        upload_account="uploader",
        upload_time=1,
        download_time=2,
        modified_time=3,
        scan_observation_version=1,
        source_file_count=1,
        page_count=1,
    )


def _generated_database(path: Path) -> None:
    connector = SQLiteConnector(str(path))
    connector.connect()
    try:
        payload: Any = ARTIFACT["backends"]
        payload = payload["sqlite"]
        for _slice_id, statements in payload["slices"]:
            for _statement_id, _kind, _name, sql in statements:
                connector.execute(sql)
        for seed in payload["bootstrap_seeds"]:
            connector.execute(seed["sql"], seed["parameters"])
    finally:
        connector.close()


def test_public_policy_contains_natural_facts_and_derives_fingerprints() -> None:
    producer = VNextArtifactProducer(
        writer_id=b"writer",
        python_abi=b"cp313",
        pillow_build=b"pillow-11",
        libjpeg_build=b"libjpeg-turbo-3",
        zlib_build=b"zlib-1.3",
    )
    policy = VNextIngestPolicy(
        producer=producer,
        storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
    )

    assert policy.unicode_data_version == unidata_version.encode("ascii")
    assert policy.producer_fingerprint_sha256 == (
        artifact_producer_fingerprint_sha256(
            producer.writer_id,
            producer.python_abi,
            producer.pillow_build,
            producer.libjpeg_build,
            producer.zlib_build,
        )
    )
    assert len(policy.artifact_policy_sha256) == 32


def test_public_source_page_is_keyset_addressed_and_bounded() -> None:
    page = VNextIngestPage(items=(("a",),), next_after=("a",), terminal=False)
    terminal = VNextIngestPage[tuple[str, ...]](
        items=(),
        next_after=None,
        terminal=True,
    )

    assert page.next_after == ("a",)
    assert terminal.terminal
    with pytest.raises(ValueError, match="requires next_after"):
        VNextIngestPage(items=(("a",),), next_after=None, terminal=False)
    with pytest.raises(ValueError, match="cannot expose next_after"):
        VNextIngestPage(items=(), next_after=("a",), terminal=True)
    with pytest.raises(ValueError, match="more than 256"):
        VNextIngestPage(
            items=tuple(range(257)),
            next_after=256,
            terminal=False,
        )


def test_public_observations_and_source_adapter_are_repository_independent() -> None:
    receipt = FileContentReceipt.from_parts((b"abc",))
    file = FileObservation(b"001.jpg", receipt, 1, 2, 3, 4)
    directory = DirectoryObservation(
        b"nested",
        0,
        1,
        2,
        3,
        4,
        GalleryObservationDirectoryFileType.DIRECTORY,
    )
    tag = TagObservation("artist", "name")
    observation = VNextIngestGalleryObservation(("gallery",), _metadata())

    class Source:
        source_root_components = ("root",)

        def list_gallery_locators(
            self,
            *,
            after_locator: tuple[str, ...] | None,
            limit: int,
        ) -> VNextIngestPage[tuple[str, ...]]:
            assert after_locator is None and limit == 256
            return VNextIngestPage((("gallery",),), None, True)

        def observe_gallery(
            self,
            locator_components: tuple[str, ...],
        ) -> VNextIngestGalleryObservation:
            assert locator_components == ("gallery",)
            return observation

        def list_file_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[FileObservation]:
            return VNextIngestPage((file,), None, True)

        def list_directory_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[DirectoryObservation]:
            return VNextIngestPage((directory,), None, True)

        def list_tag_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_ordinal: int | None,
            limit: int,
        ) -> VNextIngestPage[TagObservation]:
            return VNextIngestPage((tag,), None, True)

    assert isinstance(Source(), VNextIngestSourceAdapter)

    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=":memory:"))
    with VNextIngestFacade(config).prepare_source(Source()) as prepared:
        assert isinstance(prepared, VNextPreparedSource)
        assert not hasattr(prepared, "plan")


def test_prepare_source_uses_canonical_locator_key_order() -> None:
    observation = VNextIngestGalleryObservation(("b",), _metadata())

    class Source:
        source_root_components = ("root",)

        def list_gallery_locators(
            self,
            *,
            after_locator: tuple[str, ...] | None,
            limit: int,
        ) -> VNextIngestPage[tuple[str, ...]]:
            assert after_locator is None and limit == 256
            # Canonical framing sorts the one-byte component before the
            # two-byte component even though Python string order does not.
            return VNextIngestPage((("b",), ("aa",), ("a", "nested")), None, True)

        def observe_gallery(
            self,
            locator_components: tuple[str, ...],
        ) -> VNextIngestGalleryObservation:
            return observation

        def list_file_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[FileObservation]:
            return VNextIngestPage((), None, True)

        def list_directory_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[DirectoryObservation]:
            return VNextIngestPage((), None, True)

        def list_tag_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_ordinal: int | None,
            limit: int,
        ) -> VNextIngestPage[TagObservation]:
            return VNextIngestPage((), None, True)

    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=":memory:"))
    with VNextIngestFacade(config).prepare_source(Source()):
        pass


def test_public_session_is_a_neutral_primitive_receipt() -> None:
    session = VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=2,
        gate_lease_expires_at=100,
        ingest_generation=3,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=100,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )

    assert session.gate_slot == 2
    assert not hasattr(session, "gate")
    assert not hasattr(session, "turn")


def test_public_artifact_adapter_evidence_is_neutral() -> None:
    class Storage:
        adapter_id = b"managed-filesystem"
        producer_fingerprint_sha256 = b"p" * 32

        def render_member(
            self,
            source: BinaryIO,
            transform_kind: ArtifactTransformKind,
            destination: BinaryIO,
        ) -> None:
            destination.write(source.read())

        def protect(
            self,
            archive: BinaryIO,
            locator_components: tuple[str, ...],
            protection_token: bytes,
        ) -> ArtifactStorageEvidence:
            return ArtifactStorageEvidence(True)

    class Release:
        adapter_id = b"managed-filesystem"

        def release(
            self,
            locator_components: tuple[str, ...],
            protection_token: bytes,
        ) -> ArtifactReleaseStorageEvidence:
            return ArtifactReleaseStorageEvidence(True)

    source = BytesIO(b"bytes")
    destination = BytesIO()
    Storage().render_member(source, ArtifactTransformKind.RAW_COPY, destination)

    assert destination.getvalue() == b"bytes"
    assert isinstance(Storage(), ArtifactStorageAdapter)
    assert isinstance(Release(), ArtifactReleaseAdapter)


def test_ingest_facade_resolves_fresh_policy_and_replays_by_natural_key(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ingest-policy.sqlite3"
    _generated_database(path)
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    facade = VNextIngestFacade(config, clock=lambda: 100)
    session = facade.try_claim_ingest(True, 1_000)
    assert session is not None
    policy = VNextIngestPolicy(
        producer=VNextArtifactProducer(
            writer_id=b"writer",
            python_abi=b"cp313",
            pillow_build=b"pillow-11",
            libjpeg_build=b"libjpeg-turbo-3",
            zlib_build=b"zlib-1.3",
        ),
        storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
    )

    created = facade.ensure_policy(session, policy)
    replayed = facade.ensure_policy(session, policy)

    assert not created.replayed
    assert replayed == type(created)(
        policy=created.policy,
        manifest_policy_id=created.manifest_policy_id,
        analysis_policy_id=created.analysis_policy_id,
        artifact_policy_sha256=created.artifact_policy_sha256,
        producer_fingerprint_sha256=created.producer_fingerprint_sha256,
        display_title_policy_id=created.display_title_policy_id,
        title_sort_policy_id=created.title_sort_policy_id,
        operational_policy_id=created.operational_policy_id,
        replayed=True,
    )
    assert {
        created.manifest_policy_id,
        created.analysis_policy_id,
        created.title_sort_policy_id,
        created.display_title_policy_id,
        created.operational_policy_id,
    } == {2, 3, 4, 5, 6}
    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT artifact_policy_id, policy_component_sha256 "
            "FROM catalog_artifact_policies WHERE policy_component_sha256 = %s",
            (created.artifact_policy_sha256,),
        ) == (1, created.artifact_policy_sha256)
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators " "WHERE stream = %s",
            ("POLICY",),
        ) == (7,)


def test_ingest_policy_compact_id_collision_fails_closed_and_rolls_back(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ingest-policy-collision.sqlite3"
    _generated_database(path)
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    facade = VNextIngestFacade(config, clock=lambda: 100)
    session = facade.try_claim_ingest(True, 1_000)
    assert session is not None
    policy = VNextIngestPolicy(
        producer=VNextArtifactProducer(
            writer_id=b"writer",
            python_abi=b"cp313",
            pillow_build=b"pillow-11",
            libjpeg_build=b"libjpeg-turbo-3",
            zlib_build=b"zlib-1.3",
        ),
        storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
    )
    foreign = facade.ensure_policy(
        session,
        replace(policy, max_image_short_side=4096),
    )
    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT artifact_policy_id FROM catalog_artifact_policies "
            "WHERE policy_component_sha256 = %s",
            (foreign.artifact_policy_sha256,),
        ) == (1,)
        connector.execute(
            "UPDATE operational_identity_allocators SET next_id = %s "
            "WHERE stream = %s",
            (1, "POLICY"),
        )

    with pytest.raises(
        VNextIngestPolicyConflictError,
        match="artifact policy_id is already registered",
    ):
        facade.ensure_policy(session, policy)

    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("POLICY",),
        ) == (1,)
        assert not connector.fetch_one(
            "SELECT policy_component_sha256 "
            "FROM catalog_artifact_policy_semantics_seals "
            "WHERE policy_component_sha256 = %s",
            (policy.artifact_policy_sha256,),
        )


def test_ingest_facade_try_claim_and_completion_are_public_and_replayable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ingest-completion.sqlite3"
    _generated_database(path)
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    now = 100
    facade = VNextIngestFacade(config, clock=lambda: now)

    session = facade.try_claim_ingest(True, 1_000)
    assert session is not None
    assert facade.try_claim_ingest(True, 1_000) is None
    assert facade.resume_ingest(session) == session

    renewed = facade.renew_ingest(session, 2_000)
    assert renewed.ingest_lease_expires_at == 2_100
    assert renewed.gate_lease_expires_at == 2_100

    completed = facade.complete_ingest(renewed)
    assert isinstance(completed, VNextIngestCompletionReceipt)
    assert completed.ingest_generation == renewed.ingest_generation
    assert completed.owner_token == renewed.ingest_owner_token
    assert completed.completed_at == now
    assert completed.download_generation is None
    assert not completed.replayed

    now = 150
    replayed = facade.complete_ingest(renewed)
    assert replayed == VNextIngestCompletionReceipt(
        ingest_generation=completed.ingest_generation,
        owner_token=completed.owner_token,
        completed_at=completed.completed_at,
        download_generation=None,
        replayed=True,
    )


def test_source_step_commit_accepts_renewed_same_authority_and_rejects_forgery(
    tmp_path: Path,
) -> None:
    class EmptySource:
        source_root_components = ("root",)

        def list_gallery_locators(
            self,
            *,
            after_locator: tuple[str, ...] | None,
            limit: int,
        ) -> VNextIngestPage[tuple[str, ...]]:
            assert after_locator is None and limit == 256
            return VNextIngestPage((), None, True)

        def observe_gallery(
            self,
            locator_components: tuple[str, ...],
        ) -> VNextIngestGalleryObservation:
            raise AssertionError("empty source has no gallery")

        def list_file_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[FileObservation]:
            raise AssertionError("empty source has no FILE stream")

        def list_directory_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[DirectoryObservation]:
            raise AssertionError("empty source has no DIRECTORY stream")

        def list_tag_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_ordinal: int | None,
            limit: int,
        ) -> VNextIngestPage[TagObservation]:
            raise AssertionError("empty source has no TAG stream")

    path = tmp_path / "ingest-source-step.sqlite3"
    _generated_database(path)
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    now = 100
    facade = VNextIngestFacade(config, clock=lambda: now)
    session = facade.try_claim_ingest(True, 100)
    assert session is not None
    policy = facade.ensure_policy(
        session,
        VNextIngestPolicy(
            producer=VNextArtifactProducer(
                writer_id=b"writer",
                python_abi=b"cp313",
                pillow_build=b"pillow-11",
                libjpeg_build=b"libjpeg-turbo-3",
                zlib_build=b"zlib-1.3",
            ),
            storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
        ),
    )

    with facade.prepare_source(EmptySource()) as source:
        issued = facade.issue_source_step(session, policy, source)
        assert isinstance(issued, VNextIssuedSourceStep)
        local = facade.prepare_source_step(source, issued)
        assert isinstance(local, VNextPreparedSourceStep)

        now = 110
        renewed = facade.renew_ingest(session, 1_000)
        assert renewed.ingest_lease_expires_at == 1_110
        with pytest.raises(ValueError, match="another ingest session"):
            facade.commit_source_step(
                replace(renewed, ingest_owner_token=b"x" * 16),
                local,
            )

        result = facade.commit_source_step(renewed, local)
        assert result.source_receipt is not None
        assert len(result.source_receipt.build_id) == 16
        assert not result.terminal

        # Drive the remaining root/discovery/empty-assembly steps.  Every
        # adapter operation is outside issue/commit, even though this empty
        # source needs no hashing.
        for _ in range(30):
            issued = facade.issue_source_step(renewed, policy, source)
            local = facade.prepare_source_step(source, issued)
            result = facade.commit_source_step(renewed, local)
            if result.terminal:
                break
        else:
            pytest.fail("empty source did not reach its sealed build")
        assert result.source_receipt is not None
        assert result.source_receipt.discovered_galleries == 0
        assert result.source_receipt.staged_galleries == 0
        assert result.source_receipt.sealed


def test_source_three_stage_flow_discovers_stages_and_seals_one_empty_gallery(
    tmp_path: Path,
) -> None:
    metadata = GalleryObservationMetadata(
        gid=7,
        title="empty",
        comment="",
        upload_account="uploader",
        upload_time=1,
        download_time=2,
        modified_time=3,
        scan_observation_version=1,
        source_file_count=0,
        page_count=0,
    )

    class EmptyGallerySource:
        source_root_components = ("root",)

        def list_gallery_locators(
            self,
            *,
            after_locator: tuple[str, ...] | None,
            limit: int,
        ) -> VNextIngestPage[tuple[str, ...]]:
            assert after_locator is None and limit == 256
            return VNextIngestPage((("gallery",),), None, True)

        def observe_gallery(
            self,
            locator_components: tuple[str, ...],
        ) -> VNextIngestGalleryObservation:
            assert locator_components == ("gallery",)
            return VNextIngestGalleryObservation(locator_components, metadata)

        def list_file_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[FileObservation]:
            assert after_name_bytes is None and limit == 256
            return VNextIngestPage((), None, True)

        def list_directory_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[DirectoryObservation]:
            assert after_name_bytes is None and limit == 192
            return VNextIngestPage((), None, True)

        def list_tag_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_ordinal: int | None,
            limit: int,
        ) -> VNextIngestPage[TagObservation]:
            assert after_ordinal is None and limit == 256
            return VNextIngestPage((), None, True)

    path = tmp_path / "ingest-source-complete.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 100_000)
    assert session is not None
    policy = facade.ensure_policy(
        session,
        VNextIngestPolicy(
            producer=VNextArtifactProducer(
                writer_id=b"writer",
                python_abi=b"cp313",
                pillow_build=b"pillow-11",
                libjpeg_build=b"libjpeg-turbo-3",
                zlib_build=b"zlib-1.3",
            ),
            storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
        ),
    )

    with facade.prepare_source(EmptyGallerySource()) as source:
        for _ in range(40):
            issued = facade.issue_source_step(session, policy, source)
            local = facade.prepare_source_step(source, issued)
            result = facade.commit_source_step(session, local)
            if result.terminal:
                break
        else:
            pytest.fail("source state machine did not reach a bounded terminal step")

    assert result.source_receipt is not None
    assert result.source_receipt.discovered_galleries == 1
    assert result.source_receipt.staged_galleries == 1
    assert result.source_receipt.sealed
    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (result.source_receipt.build_id,),
        ) == ("SEALED",)


def test_source_staging_crash_resume_uses_durable_component_and_match_cursors(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ingest-source-crash-resume.sqlite3"
    _generated_database(path)
    file_names = tuple(f"f{index:04d}.jpg".encode() for index in range(300))
    files = tuple(
        FileObservation(
            name,
            FileContentReceipt.from_parts((name,)),
            index + 1,
            index + 2,
            index + 3,
            index + 4,
        )
        for index, name in enumerate(file_names)
    )
    directories = tuple(
        DirectoryObservation(
            item.name_bytes,
            item.content.size_bytes,
            item.device,
            item.inode,
            item.modified_ns,
            item.changed_ns,
            GalleryObservationDirectoryFileType.REGULAR,
        )
        for item in files
    )
    tags = tuple(TagObservation("tag", f"value-{index:04d}") for index in range(300))
    observations: dict[tuple[str, ...], VNextIngestGalleryObservation] = {
        ("gallery-a",): VNextIngestGalleryObservation(
            ("gallery-a",),
            GalleryObservationMetadata(
                gid=7,
                title="x" * 40_000,
                comment="",
                upload_account="uploader",
                upload_time=1,
                download_time=2,
                modified_time=3,
                scan_observation_version=1,
                source_file_count=len(files),
                page_count=len(files),
            ),
        ),
        ("gallery-b",): VNextIngestGalleryObservation(
            ("gallery-b",),
            GalleryObservationMetadata(
                gid=8,
                title="empty",
                comment="",
                upload_account="uploader",
                upload_time=1,
                download_time=2,
                modified_time=3,
                scan_observation_version=1,
                source_file_count=0,
                page_count=0,
            ),
        ),
    }

    def prove_no_database_write_transaction() -> None:
        connection = sqlite3.connect(path, timeout=0.05)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.rollback()
        finally:
            connection.close()

    class RestartableSource:
        source_root_components = ("root",)

        def __init__(self) -> None:
            self.file_afters: list[bytes | None] = []

        def list_gallery_locators(
            self,
            *,
            after_locator: tuple[str, ...] | None,
            limit: int,
        ) -> VNextIngestPage[tuple[str, ...]]:
            prove_no_database_write_transaction()
            assert after_locator is None and limit == 256
            return VNextIngestPage(tuple(observations), None, True)

        def observe_gallery(
            self,
            locator_components: tuple[str, ...],
        ) -> VNextIngestGalleryObservation:
            prove_no_database_write_transaction()
            return observations[locator_components]

        def list_file_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[FileObservation]:
            prove_no_database_write_transaction()
            if observation.locator_components == ("gallery-b",):
                return VNextIngestPage((), None, True)
            self.file_afters.append(after_name_bytes)
            start = (
                0
                if after_name_bytes is None
                else file_names.index(after_name_bytes) + 1
            )
            items = files[start : start + limit]
            terminal = start + len(items) == len(files)
            return VNextIngestPage(
                items,
                None if terminal else items[-1].name_bytes,
                terminal,
            )

        def list_directory_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_name_bytes: bytes | None,
            limit: int,
        ) -> VNextIngestPage[DirectoryObservation]:
            prove_no_database_write_transaction()
            if observation.locator_components == ("gallery-b",):
                return VNextIngestPage((), None, True)
            start = (
                0
                if after_name_bytes is None
                else file_names.index(after_name_bytes) + 1
            )
            items = directories[start : start + limit]
            terminal = start + len(items) == len(directories)
            return VNextIngestPage(
                items,
                None if terminal else items[-1].name_bytes,
                terminal,
            )

        def list_tag_observations(
            self,
            observation: VNextIngestGalleryObservation,
            *,
            after_ordinal: int | None,
            limit: int,
        ) -> VNextIngestPage[TagObservation]:
            prove_no_database_write_transaction()
            if observation.locator_components == ("gallery-b",):
                return VNextIngestPage((), None, True)
            start = 0 if after_ordinal is None else after_ordinal + 1
            items = tags[start : start + limit]
            terminal = start + len(items) == len(tags)
            return VNextIngestPage(
                items,
                None if terminal else start + len(items) - 1,
                terminal,
            )

    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 100_000)
    assert session is not None
    policy = facade.ensure_policy(
        session,
        VNextIngestPolicy(
            producer=VNextArtifactProducer(
                writer_id=b"writer",
                python_abi=b"cp313",
                pillow_build=b"pillow-11",
                libjpeg_build=b"libjpeg-turbo-3",
                zlib_build=b"zlib-1.3",
            ),
            storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
        ),
    )
    adapter = RestartableSource()

    def checkpoint(component: bytes) -> tuple[int, str] | None:
        with SQLiteConnector(str(path)) as connector:
            row = connector.fetch_one(
                "SELECT cursor, state FROM "
                "operational_gallery_observation_staging_checkpoints "
                "WHERE component = %s AND level = 0 ORDER BY staging_id LIMIT 1",
                (component,),
            )
        return None if not row else (row[0], row[1])

    def match_checkpoint() -> tuple[int, str] | None:
        with SQLiteConnector(str(path)) as connector:
            row = connector.fetch_one(
                "SELECT matched_count, state FROM "
                "operational_gallery_observation_staging_match_checkpoints "
                "ORDER BY staging_id LIMIT 1"
            )
        return None if not row else (row[0], row[1])

    def drive_until(predicate: Any) -> None:
        with facade.prepare_source(adapter) as source:
            for _ in range(160):
                issued = facade.issue_source_step(session, policy, source)
                local = facade.prepare_source_step(source, issued)
                result = facade.commit_source_step(session, local)
                if predicate(result):
                    return
        pytest.fail("source state machine did not reach the requested checkpoint")

    # Each context exit simulates losing every process-local cursor and attempt
    # token.  The replacement source snapshot must recover only from durable
    # checkpoint/receipt authority.
    drive_until(lambda _result: checkpoint(b"FILE") == (256, "OPEN"))
    drive_until(lambda _result: checkpoint(b"METADATA") == (32_768, "OPEN"))
    drive_until(lambda _result: match_checkpoint() == (256, "OPEN"))
    drive_until(lambda result: result.terminal)

    assert adapter.file_afters == [None, file_names[255]]
    with SQLiteConnector(str(path)) as connector:
        build = connector.fetch_one(
            "SELECT state FROM catalog_source_build_states ORDER BY build_id LIMIT 1"
        )
        galleries = connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_source_build_galleries"
        )
    assert build == ("SEALED",)
    assert galleries == (2,)
