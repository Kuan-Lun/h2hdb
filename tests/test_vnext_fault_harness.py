"""Native regression contracts for the fault injector and its independent oracle."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from typing import Literal

import mysql.connector
import pytest
import vnext_fault_harness as harness
from vnext_fault_harness import (
    FaultInjector,
    assert_exact_rollback,
    count_mutations,
    fault_injection,
    fault_points,
    open_connector,
    run_fault_point,
    snapshot_database,
)
from vnext_test_database import DatabaseFactory

from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector

TABLES = ("fault_fixture",)
INSERT = "INSERT INTO fault_fixture (row_id, value) VALUES (%s, %s)"
UPDATE = (
    "UPDATE fault_fixture SET value = %s WHERE row_id = %s /* "
    + "same-prefix-" * 12
    + "original */"
)


@pytest.fixture
def fault_config(database_factory: DatabaseFactory) -> CoreConfig:
    config = database_factory.config("fault-contract")
    with closing(open_connector(config)) as connector, connector.transaction():
        connector.execute(
            "CREATE TABLE fault_fixture (row_id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
        )
    return config


def _clear(config: CoreConfig) -> None:
    with closing(open_connector(config)) as connector, connector.transaction():
        connector.execute("DELETE FROM fault_fixture")


def _write(
    config: CoreConfig,
    *,
    observer: Callable[[], None] | None = None,
    variant: str = "original",
) -> None:
    with closing(open_connector(config)) as connector:
        if variant == "split":
            with connector.transaction():
                connector.execute(INSERT, (1, 1))
            with connector.transaction():
                connector.execute(UPDATE, (2, 1))
            return
        with connector.transaction():
            if variant == "reordered":
                connector.execute(UPDATE, (2, 1))
                connector.execute(INSERT, (1, 1))
                return
            connector.execute(INSERT, (1, 1))
            if observer is not None:
                observer()
            update = (
                UPDATE.replace("original", "different")
                if variant == "suffix"
                else UPDATE
            )
            connector.execute(update, (2, 1))


@pytest.mark.parametrize(
    ("kind", "statement_index"),
    (("before_mutation", 0), ("before_mutation", 1), ("after_commit", 2)),
)
def test_exact_native_fault_target_preserves_rollback_and_replay(
    fault_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    statement_index: int,
) -> None:
    dry = count_mutations(monkeypatch, lambda: _write(fault_config))
    assert dry.mutations == 2 and dry.commits == 1
    assert len(dry.transactions) == 1
    point = next(
        p
        for p in fault_points(dry)
        if p.kind == kind and p.statement_index == statement_index
    )
    _clear(fault_config)
    injector, before = run_fault_point(
        monkeypatch,
        config=fault_config,
        point=point,
        workflow=lambda: _write(fault_config),
        snapshot_tables=TABLES,
    )
    assert injector.fired == kind
    if kind == "before_mutation":
        assert_exact_rollback(fault_config, before, snapshot_tables=TABLES)
        assert snapshot_database(fault_config, tables=TABLES) == {"fault_fixture": ()}
        _write(fault_config)
    assert snapshot_database(fault_config, tables=TABLES) == {
        "fault_fixture": ((1, 2),)
    }


@pytest.mark.parametrize("variant", ("split", "reordered", "suffix"))
@pytest.mark.parametrize("kind", ("before_mutation", "after_commit"))
def test_changed_native_transaction_cannot_pass_as_recorded_fault_target(
    fault_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
    kind: str,
) -> None:
    dry = count_mutations(monkeypatch, lambda: _write(fault_config))
    assert dry.mutations == 2 and dry.commits == 1
    point = next(
        p
        for p in fault_points(dry)
        if p.kind == kind
        and p.statement_index == (1 if kind == "before_mutation" else 2)
    )
    _clear(fault_config)
    if variant == "suffix":
        assert UPDATE[:96] == UPDATE.replace("original", "different")[:96]
    with pytest.raises(AssertionError, match="fault target .* drift"):
        run_fault_point(
            monkeypatch,
            config=fault_config,
            point=point,
            workflow=lambda: _write(fault_config, variant=variant),
            snapshot_tables=TABLES,
        )


def test_compensation_cannot_replace_the_interrupted_transaction_snapshot(
    fault_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dry = count_mutations(monkeypatch, lambda: _write(fault_config))
    point = next(
        p
        for p in fault_points(dry)
        if p.kind == "before_mutation" and p.statement_index == 1
    )
    _clear(fault_config)

    def broken_rollback() -> None:
        # Deliberately deficient worker: commit a partial write instead of
        # rolling it back, then execute a harmless compensating transaction.
        # An oracle captured after the fault would hide this partial commit.
        with closing(open_connector(fault_config)) as connector:
            connector.begin()
            try:
                connector.execute(INSERT, (1, 1))
                connector.execute(UPDATE, (2, 1))
            except harness.InjectedFault:
                connector.commit()
                with connector.transaction():
                    connector.execute(
                        "DELETE FROM fault_fixture WHERE row_id = %s", (2,)
                    )
                raise

    injector, before = run_fault_point(
        monkeypatch,
        config=fault_config,
        point=point,
        workflow=broken_rollback,
        snapshot_tables=TABLES,
        capture_every_transaction=True,
        targeting="ordinal_only",
    )
    assert injector.fired == "before_mutation"
    assert before == {"fault_fixture": ()}
    assert snapshot_database(fault_config, tables=TABLES) == {
        "fault_fixture": ((1, 1),)
    }
    with pytest.raises(AssertionError):
        assert_exact_rollback(fault_config, before, snapshot_tables=TABLES)


@pytest.mark.parametrize("mode", ("success", "select_failure", "nested"))
@pytest.mark.parametrize("kind", ("before_mutation", "after_commit"))
def test_native_snapshot_observer_preserves_writer_and_resets_after_failure(
    fault_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    kind: Literal["before_mutation", "after_commit"],
) -> None:
    injector = FaultInjector(
        fail_before_mutation=2 if kind == "before_mutation" else None,
        fail_after_commit=1 if kind == "after_commit" else None,
    )
    nested_calls = 0
    original_open = harness.open_connector

    def nested_open(config: CoreConfig) -> SQLConnector:
        nonlocal nested_calls
        if nested_calls == 0:
            nested_calls += 1
            assert snapshot_database(config, tables=TABLES) == {"fault_fixture": ()}
        return original_open(config)

    def observer() -> None:
        before = (
            injector.mutations,
            injector.commits,
            injector.transaction_mutations,
            injector._current_first,
            tuple(injector._current),
        )
        assert before[:3] == (1, 0, 1)
        if mode == "select_failure":
            with pytest.raises(
                (sqlite3.OperationalError, mysql.connector.Error)
            ) as rejected:
                snapshot_database(fault_config, tables=("missing_fault_fixture",))
            if isinstance(rejected.value, sqlite3.OperationalError):
                assert "no such table: missing_fault_fixture" in str(rejected.value)
            else:
                assert isinstance(rejected.value, mysql.connector.Error)
                assert rejected.value.errno == 1146
        else:
            with monkeypatch.context() as patch:
                if mode == "nested":
                    patch.setattr(harness, "open_connector", nested_open)
                assert snapshot_database(fault_config, tables=TABLES) == {
                    "fault_fixture": ()
                }
        after = (
            injector.mutations,
            injector.commits,
            injector.transaction_mutations,
            injector._current_first,
            tuple(injector._current),
        )
        assert after == before

    with fault_injection(monkeypatch, injector), pytest.raises(harness.InjectedFault):
        _write(fault_config, observer=observer)
    assert injector.fired == kind
    assert injector.mutations == 2
    assert injector.commits == (1 if kind == "after_commit" else 0)
    assert injector.fired_statements == (INSERT, UPDATE)
    assert nested_calls == (1 if mode == "nested" else 0)
    expected = ((1, 2),) if kind == "after_commit" else ()
    assert snapshot_database(fault_config, tables=TABLES) == {"fault_fixture": expected}
