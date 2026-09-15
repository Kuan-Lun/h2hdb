from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from time import monotonic, sleep
from types import ModuleType

import pytest

from h2hdb import CoreConfig, VNextDatabaseAdminFacade, VNextDownloadQueueFacade
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    SchemaEpochAdmissionError,
    SchemaEpochDriftError,
    SchemaEpochValidationError,
)
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "upgrade-audit-schema.py"


@pytest.fixture
def converter() -> ModuleType:
    spec = importlib.util.spec_from_file_location("audit_schema_upgrade_test", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=["sqlite", pytest.param("mariadb", marks=pytest.mark.mariadb)])
def conversion_config(request: pytest.FixtureRequest) -> CoreConfig:
    config: CoreConfig = request.getfixturevalue(f"{request.param}_config")
    return config


def _old_populated_database(
    config: CoreConfig, converter: ModuleType
) -> tuple[object, ...]:
    admin = VNextDatabaseAdminFacade(config)
    try:
        admin.initialize()
    finally:
        admin.close()
    queue = VNextDownloadQueueFacade(config)
    try:
        queue.ensure_download_request(12345, "https://example.invalid/g/12345/")
        before = tuple(queue.list_download_requests())
    finally:
        queue.close()
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if config.database.sql_type == "sqlite" else "mariadb"
    )
    retained, addition = converter._conversion_slices(provider)
    # The converter pins all retained SQL byte-for-byte against the actual
    # schema-6 release. Removing the sole addition yields that exact shape;
    # the queue fact makes this a populated database, not a genesis fixture.
    assert converter._slices_digest(retained) == converter._OLD_SLICES[provider.backend]
    assert len(addition.statements) == 1
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            connector.execute(f"DROP TABLE {addition.statements[0].creates.name}")
            with connector.transaction():
                connector.execute(
                    "UPDATE h2hdb_schema_epoch SET schema_version = 6, "
                    "manifest_sha256 = %s WHERE singleton_id = 1",
                    (bytes.fromhex(converter._OLD_MANIFESTS[provider.backend]),),
                )
    finally:
        context.close()
    return before


def _requests(config: CoreConfig) -> tuple[object, ...]:
    queue = VNextDownloadQueueFacade(config)
    try:
        return tuple(queue.list_download_requests())
    finally:
        queue.close()


def test_populated_schema_conversion_preserves_facts_and_replays(
    conversion_config: CoreConfig,
    converter: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _old_populated_database(conversion_config, converter)
    progress: list[str] = []
    assert converter.upgrade(conversion_config, progress=progress.append) == "converted"
    assert progress[-1] == "ready_committed"
    assert _requests(conversion_config) == before
    admin = VNextDatabaseAdminFacade(conversion_config)
    try:
        assert admin.check().schema_version == 7
    finally:
        admin.close()
    assert (
        converter.upgrade(conversion_config, progress=progress.append)
        == "already_converted"
    )
    assert _requests(conversion_config) == before

    def forbid_duplicate_audit(_admin: VNextSchemaAdmin) -> None:
        pytest.fail("converter already completed the full audit")

    admin = VNextDatabaseAdminFacade(conversion_config)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(VNextSchemaAdmin, "check", forbid_duplicate_audit)
            startup = admin.start_ingest_runtime(
                lease_duration_microseconds=300_000_000
            )
        assert startup.full_audit is None
        admin.finish_ingest_runtime(startup.session)
    finally:
        admin.close()


@pytest.mark.parametrize(
    "interruption",
    [
        "conversion_marked",
        "object_created",
        "addition_validated",
        "full_audit_completed",
        "ready_committed",
    ],
)
def test_conversion_interruption_preserves_data_and_resumes(
    conversion_config: CoreConfig, converter: ModuleType, interruption: str
) -> None:
    before = _old_populated_database(conversion_config, converter)

    def interrupt(checkpoint: str) -> None:
        if checkpoint == interruption:
            raise RuntimeError("simulated converter termination")

    with pytest.raises(RuntimeError, match="simulated converter termination"):
        converter.upgrade(conversion_config, progress=interrupt)
    assert _requests(conversion_config) == before
    # SQLite rolls its offline transaction back. MariaDB's DDL commits leave
    # either the conversion-only marker or a fully committed READY marker.
    context = RepositoryContext.from_config(conversion_config)
    try:
        with context.SQLConnector() as connector:
            row = converter._control(connector)
    finally:
        context.close()
    if row[3] == "BUILDING":
        admin = VNextDatabaseAdminFacade(conversion_config)
        try:
            for call in (admin.check_readiness, admin.check):
                with pytest.raises(SchemaEpochAdmissionError):
                    call()
            with pytest.raises(SchemaEpochDriftError, match="manifest differs"):
                admin.initialize()
        finally:
            admin.close()
        context = RepositoryContext.from_config(conversion_config)
        try:
            with context.SQLConnector() as connector:
                assert converter._control(connector) == row
        finally:
            context.close()
    assert converter.upgrade(conversion_config, progress=lambda _: None) in {
        "converted",
        "already_converted",
    }
    assert _requests(conversion_config) == before


def test_foreign_marker_and_extra_object_are_rejected_without_changes(
    sqlite_config: CoreConfig, converter: ModuleType
) -> None:
    before = _old_populated_database(sqlite_config, converter)
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE foreign_object (value INTEGER)")
            control_before = converter._control(connector)
        with pytest.raises(SchemaEpochValidationError, match="inventory"):
            converter.upgrade(sqlite_config, progress=lambda _: None)
        with context.SQLConnector() as connector:
            assert converter._control(connector) == control_before
            connector.execute("DROP TABLE foreign_object")
            with connector.transaction():
                connector.execute(
                    "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s WHERE singleton_id = 1",
                    (b"x" * 32,),
                )
            control_before = converter._control(connector)
        with pytest.raises(SchemaEpochAdmissionError, match="Only the exact"):
            converter.upgrade(sqlite_config, progress=lambda _: None)
        with context.SQLConnector() as connector:
            assert converter._control(connector) == control_before
    finally:
        context.close()
    assert _requests(sqlite_config) == before


def test_failed_final_audit_never_publishes_ready(
    conversion_config: CoreConfig,
    converter: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _old_populated_database(conversion_config, converter)

    def reject(*_args: object, **_kwargs: object) -> None:
        raise SchemaEpochValidationError("semantic corruption detected")

    with monkeypatch.context() as patch:
        patch.setattr(GeneratedVNextSchemaProvider, "validate_semantics", reject)
        with pytest.raises(SchemaEpochValidationError, match="semantic corruption"):
            converter.upgrade(conversion_config, progress=lambda _: None)
    context = RepositoryContext.from_config(conversion_config)
    try:
        with context.SQLConnector() as connector:
            row = converter._control(connector)
            assert row[2] == 6 or row[3] == "BUILDING"
    finally:
        context.close()
    assert _requests(conversion_config) == before
    assert converter.upgrade(conversion_config, progress=lambda _: None) == "converted"


@pytest.mark.deep
def test_real_process_kill_recovers_conversion_without_losing_facts(
    conversion_config: CoreConfig, converter: ModuleType, tmp_path: Path
) -> None:
    before = _old_populated_database(conversion_config, converter)
    config_path = tmp_path / "local-test-config.json"
    config_path.write_text(conversion_config.model_dump_json())
    reached = tmp_path / "addition-validated"
    program = """
import importlib.util
import sys
from pathlib import Path
from threading import Event
from h2hdb import load_config
spec = importlib.util.spec_from_file_location('offline_converter_child', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
def checkpoint(name):
    if name == 'addition_validated':
        Path(sys.argv[3]).write_text(name)
        Event().wait(120)
module.upgrade(load_config(sys.argv[2]), progress=checkpoint)
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(_SCRIPT.parents[1] / "src")
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(_SCRIPT), str(config_path), str(reached)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = monotonic() + 90
        while (
            not reached.exists() and process.poll() is None and monotonic() < deadline
        ):
            sleep(0.01)
        if not reached.exists():
            process.kill()
            stdout, stderr = process.communicate(timeout=10)
            pytest.fail(f"converter never reached kill checkpoint: {stdout} {stderr}")
        process.kill()
        process.communicate(timeout=10)
        assert process.returncode != 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)
    assert _requests(conversion_config) == before
    assert converter.upgrade(conversion_config, progress=lambda _: None) == "converted"
    assert _requests(conversion_config) == before
