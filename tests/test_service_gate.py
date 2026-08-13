from collections.abc import Generator
from contextlib import contextmanager

import pytest

from h2hdb import H2HDB, CoreConfig


def test_public_operations_acquire_one_reentrant_maintenance_gate(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    acquisitions = 0
    active = 0

    @contextmanager
    def observed_gate(*, timeout_seconds: int | None = None) -> Generator[None]:
        nonlocal acquisitions, active
        del timeout_seconds
        acquisitions += 1
        active += 1
        assert active == 1
        try:
            yield
        finally:
            active -= 1

    monkeypatch.setattr(
        database._database_maintenance,
        "database_gate",
        observed_gate,
    )

    with database.database_gate():
        assert database.get_catalog_revision().revision == 0
        database.request_download(990_001)
        assert database.get_download_request(990_001) is not None

    # The explicit outer gate is reused by all nested public operations.
    assert acquisitions == 1
    assert active == 0


def test_database_touching_facade_methods_are_guarded() -> None:
    intentionally_self_gated = {
        "analyze_database",
        "check_schema_epoch_v2",
        "check_schema_epoch_v2_readiness",
        "initialize_schema_epoch_v2",
        "migrate",
        "optimize_database",
        "run_scheduled_database_maintenance",
    }
    intentionally_ungated_database_methods = {"check_readiness"}
    non_database_methods = {"database_gate"}
    methods = {
        name: member
        for name, member in vars(H2HDB).items()
        if callable(member)
        and not name.startswith("_")
        and name
        not in (
            intentionally_self_gated
            | intentionally_ungated_database_methods
            | non_database_methods
        )
    }

    assert methods
    assert {
        name for name, method in methods.items() if not hasattr(method, "__wrapped__")
    } == set()


def test_schema_readiness_check_bypasses_maintenance_gate(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    acquisitions = 0

    @contextmanager
    def observed_gate(*, timeout_seconds: int | None = None) -> Generator[None]:
        nonlocal acquisitions
        del timeout_seconds
        acquisitions += 1
        yield

    monkeypatch.setattr(
        database._database_maintenance,
        "database_gate",
        observed_gate,
    )

    assert database.check_readiness().database_version == 7
    assert acquisitions == 0


def test_scheduled_maintenance_readiness_check_uses_shared_gate(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    acquisitions = 0

    @contextmanager
    def observed_gate(*, timeout_seconds: int | None = None) -> Generator[None]:
        nonlocal acquisitions
        del timeout_seconds
        acquisitions += 1
        yield

    monkeypatch.setattr(
        database._database_maintenance,
        "database_gate",
        observed_gate,
    )

    result = database.run_scheduled_database_maintenance()

    assert not result.evaluated
    assert acquisitions == 1
