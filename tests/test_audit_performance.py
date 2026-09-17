"""Full audits retain per-validator evidence without logging under a transaction."""

from __future__ import annotations

import json
import logging
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest
from vnext_fault_harness import backend_of, open_connector

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.config_loader import LoggerConfig
from h2hdb.database_performance import DatabasePerformance
from h2hdb.domain import SchemaProvisioningOutcome
from h2hdb.operational_refinement import OperationalSemanticValidationError
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import SchemaEpochAdmissionError, SchemaEpochValidationError
from h2hdb.sql_performance import instrument_connector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
from h2hdb.vnext_transaction import VNextUnitOfWork

_LOGGER = "h2hdb.database_performance"
_PREFIX = "database_performance "


def _debug(config: CoreConfig) -> CoreConfig:
    return config.model_copy(
        update={"logger": LoggerConfig.model_validate({"level": "debug"})}
    )


def _records(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], json.loads(record.getMessage()[len(_PREFIX) :]))
        for record in caplog.records
        if record.name == _LOGGER and record.getMessage().startswith(_PREFIX)
    ]


def _phases(records: list[dict[str, Any]], phase: str) -> list[dict[str, Any]]:
    return [
        record
        for record in records
        if record["event"] == "phase" and record["phase"] == phase
    ]


def _terminal(records: list[dict[str, Any]]) -> dict[str, Any]:
    terminals = [
        record
        for record in records
        if record["event"] in {"completed", "failed", "interrupted"}
    ]
    assert terminals
    return terminals[-1]


def test_full_ready_audit_reports_every_real_validator_and_sql(
    db_config: CoreConfig, caplog: pytest.LogCaptureFixture
) -> None:
    with closing(VNextDatabaseAdminFacade(_debug(db_config))) as admin:
        admin.initialize()
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            report = admin.check()

    records = _records(caplog)
    validators = _phases(records, "semantic_validator")
    assert tuple(record["labels"]["validator"] for record in validators) == (
        report.semantic_obligation_ids
    )
    assert all(
        record["labels"]["lifecycle"] == "READY_REVALIDATION"
        and record["status"] == "completed"
        for record in validators
    )
    assert all(record["sql_calls"] > 0 for record in validators)
    assert all(record["sql_seconds"] >= 0 for record in validators)
    structure = _phases(records, "global_structure")
    assert len(structure) == 1
    assert structure[0]["sql_calls"] > 0
    semantics = _phases(records, "semantics")
    assert len(semantics) == 1
    assert semantics[0]["sql_calls"] == sum(
        record["sql_calls"] for record in validators
    )
    terminal = _terminal(records)
    assert terminal["operation"] == "schema_check"
    assert terminal["labels"]["audit"] == "ready"
    assert terminal["event"] == "completed"
    assert terminal["sql_calls"] > (
        structure[0]["sql_calls"] + semantics[0]["sql_calls"]
    )
    assert (
        terminal["sql_calls"]
        == sum(
            record["exclusive_sql_calls"]
            for record in records
            if record["event"] == "phase"
        )
        + terminal["exclusive_sql_calls"]
    )
    assert terminal["connection_calls"] == 2
    assert terminal["transaction_calls"] == 2
    assert terminal["query_top"]


def test_initialize_distinguishes_activation_from_ready_marker_replay(
    db_config: CoreConfig, caplog: pytest.LogCaptureFixture
) -> None:
    with closing(VNextDatabaseAdminFacade(_debug(db_config))) as admin:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            created = admin.initialize()
        activation = _records(caplog)
        assert created.outcome is SchemaProvisioningOutcome.CREATED
        assert created.activation_audit is not None
        assert (
            tuple(
                record["labels"]["validator"]
                for record in _phases(activation, "semantic_validator")
            )
            == created.activation_audit.semantic_obligation_ids
        )
        assert all(
            record["labels"]["lifecycle"] == "BUILDING_TO_READY"
            for record in _phases(activation, "semantic_validator")
        )
        assert _terminal(activation)["labels"] == {
            "result": "created",
            "audit": "activation",
        }
        for phase in (
            "schema_construction",
            "bootstrap_install",
            "bootstrap_before",
            "bootstrap_after",
        ):
            assert len(_phases(activation, phase)) == 1

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            replayed = admin.initialize()
        replay_records = _records(caplog)
        assert replayed.outcome is SchemaProvisioningOutcome.ALREADY_READY
        assert replayed.activation_audit is None
        assert not _phases(replay_records, "semantic_validator")
        assert not _phases(replay_records, "global_structure")
        assert _terminal(replay_records)["labels"] == {
            "result": "already_ready",
            "audit": "not_performed",
        }

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            assert admin.check_readiness().state == "READY"
        assert not _records(caplog)


def test_real_semantic_corruption_reports_failed_validator_without_message_or_token(
    db_config: CoreConfig, caplog: pytest.LogCaptureFixture
) -> None:
    with closing(VNextDatabaseAdminFacade(_debug(db_config))) as admin:
        admin.initialize()
        with closing(open_connector(db_config)) as connector:
            with connector.transaction():
                lease = MaintenanceGateRepository.claim_exclusive(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    now=1,
                    lease_duration=1_000,
                )
                connector.execute(
                    "DELETE FROM operational_maintenance_gate_holders WHERE slot = 0"
                )
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            with pytest.raises(OperationalSemanticValidationError, match="slots 0..63"):
                admin.check()

    records = _records(caplog)
    failed = [
        record
        for record in _phases(records, "semantic_validator")
        if record["status"] == "failed"
    ]
    assert len(failed) == 1
    assert failed[0]["labels"]["validator"] == ("h2hdb.operational.maintenance-gate.v1")
    assert failed[0]["sql_calls"] > 0
    assert _terminal(records)["event"] == "failed"
    messages = "\n".join(
        record.getMessage() for record in caplog.records if record.name == _LOGGER
    )
    assert "OperationalSemanticValidationError" in messages
    assert "exactly slots 0..63" not in messages
    assert lease.owner_token.hex() not in messages
    assert repr(lease.owner_token) not in messages


@pytest.mark.parametrize("level", ["info", "debug"])
def test_audit_logging_level_selects_summary_or_validator_details(
    sqlite_config: CoreConfig, caplog: pytest.LogCaptureFixture, level: str
) -> None:
    config = sqlite_config.model_copy(
        update={"logger": LoggerConfig.model_validate({"level": level})}
    )
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        admin.initialize()
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            admin.check()
    records = _records(caplog)
    assert _terminal(records)["event"] == "completed"
    assert bool(_phases(records, "semantic_validator")) is (level == "debug")
    if level == "info":
        assert all(
            record.levelno == logging.INFO
            for record in caplog.records
            if record.name == _LOGGER
        )


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.mark.parametrize("fail", [False, True])
def test_actual_validator_delay_is_attributed_and_emitted_only_after_connection_close(
    sqlite_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    fail: bool,
) -> None:
    with closing(VNextDatabaseAdminFacade(sqlite_config)) as admin:
        admin.initialize()
    context = RepositoryContext.from_config(_debug(sqlite_config))
    clock = _Clock()
    open_connections: list[SQLiteConnector] = []
    emitted_while_open: list[logging.LogRecord] = []
    delayed_queries = 0

    class DelayedConnector(SQLiteConnector):
        def connect(self) -> None:
            super().connect()
            open_connections.append(self)

        def close(self) -> None:
            super().close()
            open_connections.remove(self)

        def fetch_all(
            self, query: str, data: tuple[Any, ...] = ()
        ) -> list[tuple[Any, ...]]:
            nonlocal delayed_queries
            if "FROM operational_maintenance_gate_heads" in query:
                clock.now += 2.5
                delayed_queries += 1
                if fail:
                    raise RuntimeError("private SQL failure detail")
            return super().fetch_all(query, data)

    class TransactionGuard(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if open_connections:
                emitted_while_open.append(record)

    monitored = replace(
        context,
        SQLConnector=lambda: instrument_connector(
            DelayedConnector(database=sqlite_config.database.database)
        ),
    )
    schema_admin = VNextSchemaAdmin(monitored)
    schema_admin._performance = DatabasePerformance(
        logging.getLogger(_LOGGER),
        backend="sqlite",
        level=logging.DEBUG,
        clock=clock,
    )
    guard = TransactionGuard()
    logger = logging.getLogger(_LOGGER)
    logger.addHandler(guard)
    caplog.clear()
    try:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            if fail:
                with pytest.raises(SchemaEpochValidationError, match="unreadable"):
                    schema_admin.check()
            else:
                schema_admin.check()
    finally:
        logger.removeHandler(guard)
        context.close()
    assert not emitted_while_open
    assert not open_connections
    assert delayed_queries > 0
    records = _records(caplog)
    delayed = [
        record
        for record in _phases(records, "semantic_validator")
        if record["sql_seconds"] > 0
    ]
    assert delayed
    assert sum(record["sql_seconds"] for record in delayed) == delayed_queries * 2.5
    assert all(record["sql_seconds"] == record["elapsed_seconds"] for record in delayed)
    terminal = _terminal(records)
    assert (
        terminal["sql_seconds"]
        == terminal["elapsed_seconds"]
        == (delayed_queries * 2.5)
    )
    assert terminal["event"] == ("failed" if fail else "completed")
    assert "private SQL failure detail" not in caplog.text


def test_building_marker_rejected_before_full_validator_records(
    sqlite_config: CoreConfig, caplog: pytest.LogCaptureFixture
) -> None:
    with closing(VNextDatabaseAdminFacade(_debug(sqlite_config))) as admin:
        admin.initialize()
        with closing(open_connector(sqlite_config)) as connector:
            connector.execute(
                "UPDATE h2hdb_schema_epoch SET state = 'BUILDING', ready_at = NULL"
            )
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            with pytest.raises(SchemaEpochAdmissionError, match="not READY"):
                admin.check()
    records = _records(caplog)
    assert _terminal(records)["event"] == "failed"
    assert not _phases(records, "semantic_validator")
    assert not _phases(records, "global_structure")
    assert _phases(records, "readiness_marker")[0]["status"] == "failed"

    caplog.clear()
    with closing(VNextDatabaseAdminFacade(_debug(sqlite_config))) as admin:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            resumed = admin.initialize()
    assert resumed.outcome is SchemaProvisioningOutcome.RESUMED
    assert resumed.activation_audit is not None
    records = _records(caplog)
    assert _terminal(records)["labels"] == {
        "result": "resumed",
        "audit": "activation",
    }
    assert (
        tuple(
            record["labels"]["validator"]
            for record in _phases(records, "semantic_validator")
        )
        == resumed.activation_audit.semantic_obligation_ids
    )


def test_provider_blocker_reports_failure_before_database_open(
    sqlite_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pathlib import Path

    from h2hdb.vnext_schema_provider import VNextSchemaProviderUnavailableError

    def blocked(provider: GeneratedVNextSchemaProvider) -> None:
        raise VNextSchemaProviderUnavailableError("private provider failure detail")

    monkeypatch.setattr(GeneratedVNextSchemaProvider, "_require_available", blocked)
    with closing(VNextDatabaseAdminFacade(_debug(sqlite_config))) as admin:
        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            with pytest.raises(VNextSchemaProviderUnavailableError):
                admin.check()
    records = _records(caplog)
    assert _terminal(records)["event"] == "failed"
    assert _terminal(records)["connection_calls"] == 0
    assert _phases(records, "provider_resolution")[0]["status"] == "failed"
    assert not Path(sqlite_config.database.database).exists()
    assert "private provider failure detail" not in caplog.text
