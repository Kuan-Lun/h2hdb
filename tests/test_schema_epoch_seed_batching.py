from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast
from unittest.mock import Mock

import pytest

from h2hdb.schema_epoch import (
    _SCHEMA_SEED_BATCH_ROWS,
    SchemaSeedStatement,
    _execute_bootstrap_seeds,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider


def _seed(table: str, seed_id: str, value: int) -> SchemaSeedStatement:
    return SchemaSeedStatement(
        seed_id,
        table,
        f"INSERT INTO {table} (value) VALUES (%s) ON CONFLICT(value) DO NOTHING",
        (value,),
    )


def _recorded_batches(
    seeds: Sequence[SchemaSeedStatement],
) -> list[tuple[str, list[tuple[Any, ...]]]]:
    connector = Mock(spec=SQLConnector)
    _execute_bootstrap_seeds(cast(SQLConnector, connector), seeds)
    return [
        (str(call.args[0]), list(call.args[1]))
        for call in connector.execute_many.call_args_list
    ]


def test_bootstrap_seed_batches_preserve_order_sql_boundaries_and_hard_cap() -> None:
    seeds = (
        *(_seed("seed_a", f"a-{index}", index) for index in range(257)),
        _seed("seed_b", "b-0", 1000),
        _seed("seed_b", "b-1", 1001),
        _seed("seed_a", "a-tail", 2000),
    )

    batches = _recorded_batches(seeds)

    assert [len(parameters) for _sql, parameters in batches] == [256, 1, 2, 1]
    assert all(len(parameters) <= _SCHEMA_SEED_BATCH_ROWS for _, parameters in batches)
    assert [(sql, parameters) for sql, batch in batches for parameters in batch] == [
        (seed.sql, seed.parameters) for seed in seeds
    ]


def test_empty_bootstrap_seed_sequence_executes_no_statement() -> None:
    assert _recorded_batches(()) == []


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_bootstrap_contract_uses_39_bounded_batches(
    backend: Literal["sqlite", "mariadb"],
) -> None:
    seeds = GeneratedVNextSchemaProvider(backend).definition.bootstrap_seeds

    batches = _recorded_batches(seeds)

    assert len(seeds) == 6_094
    assert len(batches) == 39
    assert max(len(parameters) for _sql, parameters in batches) == (
        _SCHEMA_SEED_BATCH_ROWS
    )
    assert [(sql, parameters) for sql, batch in batches for parameters in batch] == [
        (seed.sql, seed.parameters) for seed in seeds
    ]


class _ResponseLossConnector:
    def __init__(self, *, fail_after_batch: int, response_lost: bool) -> None:
        self.fail_after_batch = fail_after_batch
        self.response_lost = response_lost
        self.batch_number = 0
        self.failed = False
        self.rows: set[tuple[str, tuple[Any, ...]]] = set()

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self.batch_number += 1
        should_fail = not self.failed and self.batch_number == self.fail_after_batch
        if should_fail and not self.response_lost:
            self.failed = True
            raise RuntimeError("injected statement failure")
        self.rows.update((query, row) for row in data)
        if should_fail:
            self.failed = True
            raise RuntimeError("injected response loss")


@pytest.mark.parametrize("response_lost", [False, True])
@pytest.mark.parametrize("failed_batch", [1, 2, 3])
def test_interrupted_batched_bootstrap_replay_matches_row_reference(
    response_lost: bool,
    failed_batch: int,
) -> None:
    seeds = tuple(
        _seed("seed_a", f"a-{index}", index)
        for index in range(_SCHEMA_SEED_BATCH_ROWS + 1)
    ) + tuple(_seed("seed_b", f"b-{index}", index) for index in range(3))
    connector = _ResponseLossConnector(
        fail_after_batch=failed_batch,
        response_lost=response_lost,
    )

    with pytest.raises(RuntimeError, match="injected"):
        _execute_bootstrap_seeds(cast(SQLConnector, connector), seeds)

    _execute_bootstrap_seeds(cast(SQLConnector, connector), seeds)

    assert connector.rows == {(seed.sql, seed.parameters) for seed in seeds}
