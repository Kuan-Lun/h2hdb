"""Native, fixture-owned generated databases for dev-only SQL probes."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, closing, contextmanager
from itertools import count
from pathlib import Path
from typing import Any

import pytest
from vnext_test_database import (
    DatabaseFactory,
    inspect_one,
    open_database,
    open_generated_database,
)

from h2hdb import CoreConfig, VNextIngestFacade, VNextIngestSession
from h2hdb.sql_connector import SQLConnector


@contextmanager
def generated_probe_databases(
    factory: DatabaseFactory, count: int
) -> Iterator[Iterator[SQLConnector]]:
    """Replace only disposable allocation; execute the probe unchanged."""
    with ExitStack() as stack:
        connections = [
            stack.enter_context(
                closing(open_generated_database(factory.config(f"probe-{index}")))
            )
            for index in range(count)
        ]
        yield iter(connections)


def owned_probe_database_factory(
    factory: DatabaseFactory,
    *,
    created: list[CoreConfig] | None = None,
) -> Callable[[str, Path], AbstractContextManager[CoreConfig]]:
    """Allocate empty owned databases for probes that initialize via public APIs."""
    ordinal = count()

    @contextmanager
    def database(backend: str, _temporary: Path) -> Iterator[CoreConfig]:
        assert backend == factory.backend
        config = factory.config(f"public-probe-{next(ordinal)}")
        if created is not None:
            created.append(config)
        yield config

    return database


def observe_probe_claims(monkeypatch: pytest.MonkeyPatch) -> list[int | None]:
    """Record actual facade calls and native generations without changing them."""
    original = VNextIngestFacade.try_claim_ingest
    claims: list[int | None] = []

    def observed(
        facade: VNextIngestFacade,
        periodic: bool,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession | None:
        session = original(facade, periodic, lease_duration_microseconds)
        claims.append(None if session is None else session.ingest_generation)
        return session

    monkeypatch.setattr(VNextIngestFacade, "try_claim_ingest", observed)
    return claims


def assert_probe_claim_sequence(
    result: dict[str, Any], claims: list[int | None], config: CoreConfig
) -> None:
    """Reject extra empty turns or hidden maintenance between measured turns."""
    assert result["measurement_protocol"] == "consecutive-work-generations-v1"
    turns = result["turns"]
    expected = list(range(1, len(turns) + 2))
    assert claims == expected
    assert [turn["ingest_generation"] for turn in turns] == expected[:-1]
    assert [turn["next_claim_generation"] for turn in turns] == expected[1:]
    assert [turn["next_claim_kind"] for turn in turns] == [
        *(["next_pipeline_turn"] * (len(turns) - 1)),
        "post_measurement_probe",
    ]
    for turn in turns:
        assert turn["cleanup"] == "DONE"
        assert turn["next_claim"] == "passed"
        queries = [
            query
            for query in turn["measurements"]["queries"]
            if query["pipeline"] == "claim" and query["category"] == "sql"
        ]
        assert queries
        # This unique fixture needs no DELETE to grant a direct next claim.
        # A cleanup drain would issue DELETEs and belongs to cleanup instead.
        assert not any(
            query["sql"].lstrip().upper().startswith("DELETE") for query in queries
        )
    assert result["post_measurement_next_claim"] == {
        "status": "passed",
        "ingest_generation": expected[-1],
        "completed": True,
        "included_in_phase_costs": False,
        "state_changed": True,
        "cleanup_after_probe": "not_checked",
        "ready_audit_after_probe": "not_checked",
    }
    with closing(open_database(config)) as connector:
        assert inspect_one(
            connector,
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads WHERE singleton_id = 1",
        ) == (expected[-1], expected[-1], "READY")
