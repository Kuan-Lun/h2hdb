"""Exercise issued file-validation preparation outside managed write transactions."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_repository import AnalysisRepository
from h2hdb.vnext_file_decision_validation_plan import (
    AnalysisFileDecisionValidationPage,
    AnalysisFileDecisionValidationPlan,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_transaction import VNextUnitOfWork


@contextmanager
def file_validation_pages(
    connector: SQLConnector,
    *,
    backend: str,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
) -> Iterator[Callable[[bytes, int, int], AnalysisFileDecisionValidationPage]]:
    plan: AnalysisFileDecisionValidationPlan | None = None

    def prepare(
        batch_key: bytes, max_rows: int, now: int
    ) -> AnalysisFileDecisionValidationPage:
        nonlocal plan
        with connector.transaction():
            issue = AnalysisRepository.issue_next_batch(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=batch_key,
                max_rows=max_rows,
                now=now,
            )
        if plan is None:
            plan = AnalysisRepository.prepare_file_decision_validation_plan(
                connector, backend=backend, authority=issue.preparation_authority
            )
        return AnalysisRepository.prepare_file_decision_validation_page(
            issue=issue, plan=plan
        )

    try:
        yield prepare
    finally:
        if plan is not None:
            plan.close()
