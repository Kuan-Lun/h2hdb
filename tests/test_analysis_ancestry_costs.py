"""Bounded ancestry authority and public-path cost regression evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from analysis_ancestry_experiment import public_ancestry_experiment

from h2hdb import CoreConfig

pytestmark = pytest.mark.performance_acceptance


@pytest.mark.deep
@pytest.mark.parametrize("total_pages", (127, 128, 129))
def test_public_ancestry_cost_and_latency_across_compaction(
    db_config: CoreConfig, tmp_path: Path, total_pages: int
) -> None:
    report = public_ancestry_experiment(db_config, page_count=total_pages - 1)
    (tmp_path / "ancestry-public-cost.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
