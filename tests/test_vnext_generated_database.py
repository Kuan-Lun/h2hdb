from __future__ import annotations

from itertools import groupby
from pathlib import Path
from typing import Any, ClassVar, cast
from unittest.mock import Mock

import pytest
import test_vnext_catalog_reader_mariadb as mariadb_reader_tests
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider


def test_generated_mariadb_reader_fixture_batches_every_fact_in_exact_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = Mock(spec=MariaDBConnector)
    monkeypatch.setattr(
        mariadb_reader_tests, "MariaDBConnector", Mock(return_value=connector)
    )
    assert mariadb_reader_tests._generated_mariadb(CoreConfig()) is connector
    connector.connect.assert_called_once_with()
    payload: Any = ARTIFACT["backends"]["mariadb"]
    assert [call.args for call in connector.execute.call_args_list] == [
        (sql,)
        for _slice_id, statements in payload["slices"]
        for _statement_id, _kind, _name, sql in statements
    ]
    batches = [
        (call.args[0], call.args[1]) for call in connector.execute_many.call_args_list
    ]
    assert batches
    assert all(1 <= len(parameters) <= 128 for _sql, parameters in batches)
    assert [(sql, parameters) for sql, batch in batches for parameters in batch] == [
        (seed["sql"], seed["parameters"]) for seed in payload["bootstrap_seeds"]
    ]


class _RecordingSQLiteConnector(SQLiteConnector):
    def __init__(self, database: str) -> None:
        super().__init__(database)
        self.begin_count = 0
        self.commit_count = 0
        self.rollback_count = 0
        self.batches: list[tuple[str, int]] = []

    def begin(self) -> None:
        self.begin_count += 1
        super().begin()

    def commit(self) -> None:
        self.commit_count += 1
        super().commit()

    def rollback(self) -> None:
        self.rollback_count += 1
        super().rollback()

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self.batches.append((query, len(data)))
        super().execute_many(query, data)


class _FailingBatchConnector(SQLiteConnector):
    rollback_calls: ClassVar[int] = 0
    close_calls: ClassVar[int] = 0

    def __init__(self, database: str) -> None:
        super().__init__(database)
        self.batch_attempt = 0

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self.batch_attempt += 1
        if self.batch_attempt == 2:
            raise RuntimeError("injected generated bootstrap batch failure")
        super().execute_many(query, data)

    def rollback(self) -> None:
        type(self).rollback_calls += 1
        super().rollback()

    def close(self) -> None:
        type(self).close_calls += 1
        super().close()


def test_generated_sqlite_setup_is_one_exact_batched_transaction(
    tmp_path: Path,
) -> None:
    connector = cast(
        _RecordingSQLiteConnector,
        open_generated_sqlite_database(
            tmp_path / "generated.sqlite3",
            connector_type=_RecordingSQLiteConnector,
        ),
    )
    provider = GeneratedVNextSchemaProvider("sqlite")
    definition = provider.definition
    expected_batches = tuple(
        (sql, len(tuple(seeds)))
        for sql, seeds in groupby(
            definition.bootstrap_seeds,
            key=lambda seed: seed.sql,
        )
    )
    try:
        assert connector.begin_count == 1
        assert connector.commit_count == 1
        assert connector.rollback_count == 0
        assert tuple(connector.batches) == expected_batches
        assert len(expected_batches) == 17
        assert sum(row_count for _sql, row_count in expected_batches) == 6_094
        provider.validate_global(connector)
        assert tuple(provider.validate_bootstrap_seeds(connector)) == tuple(
            seed.seed_id for seed in definition.bootstrap_seeds
        )
    finally:
        connector.close()


def test_generated_sqlite_setup_rolls_back_and_closes_on_batch_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "failed.sqlite3"
    _FailingBatchConnector.rollback_calls = 0
    _FailingBatchConnector.close_calls = 0

    with pytest.raises(
        RuntimeError,
        match="injected generated bootstrap batch failure",
    ):
        open_generated_sqlite_database(
            path,
            connector_type=_FailingBatchConnector,
        )

    assert _FailingBatchConnector.rollback_calls == 1
    assert _FailingBatchConnector.close_calls == 1
    connector = SQLiteConnector(str(path))
    connector.connect()
    try:
        assert (
            connector.fetch_all(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
            == []
        )
    finally:
        connector.close()
