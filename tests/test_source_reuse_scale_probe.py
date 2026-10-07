"""Warm published and unpublished marker observations enter a fresh source cut."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from vnext_test_database import (
    DatabaseFactory,
    assert_foreign_key_integrity,
    database_connector,
)

pytestmark = pytest.mark.deep


@pytest.fixture
def reuse_probe() -> Iterator[ModuleType]:
    name = "source_reuse_scale_probe_under_test"
    path = Path(__file__).resolve().parents[1] / "scripts/source_reuse_scale_probe.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    old_path = sys.path[:]
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = old_path
        del sys.modules[name]


def test_public_seed_restored_twice_then_fresh_generation_reuses_unpublished(
    database_factory: DatabaseFactory, reuse_probe: ModuleType, tmp_path: Path
) -> None:
    from public_fixture_snapshot import (
        clone_public_seed,
        owned_database_from_factory,
        seal_public_seed,
    )

    shape = reuse_probe.ReuseShape(galleries=5, published=2, files=15, changed=1)
    evidence = {}
    try:
        with (
            owned_database_from_factory(database_factory) as seed,
            owned_database_from_factory(database_factory) as target,
        ):
            evidence["seed"] = reuse_probe.build_public_seed(seed.config, shape)
            evidence["sealed"] = sealed = seal_public_seed(seed)
            runs: list[dict[str, Any]] = []
            evidence["runs"] = runs
            for quota in (1, 3):
                restored = clone_public_seed(seed, target)
                assert restored == sealed
                result = reuse_probe.run_measured_source(
                    target.config, shape, native=True, max_new_galleries=quota
                )
                runs.append(result)
                assert result["before"]["publications"] == 2
                assert result["before"]["marker_bindings"] == 5
                assert result["before"]["file_facts"] == 15
                assert result["after"]["publications"] == 2
                assert result["deep_reads"] == 1
                assert result["actions"]["STAGING_FIND"] >= 3
                assert result["admission"]["selected_galleries"] == 2 + quota
                assert result["admission"]["deferred_galleries"] == 3 - quota
                assert result["admission"]["selected_file_facts"] == 3 * (2 + quota)
                assert result["source_sealed"] and result["READY"] == "READY"
                assert result["budget_passed"]
                assert len(result["native_find_issues"]) == len(
                    result["native_find_issue_ordinals"]
                )
                assert all(
                    row["issue_sql"]["sql_calls"] > 0
                    for row in result["native_find_issues"]
                )
                assert (
                    result["whole_source_sql"]["sql_calls"]
                    > result["staging_find_issue_sql"]["sql_calls"]
                    > 0
                )
                with database_connector(target.config) as reader:
                    assert_foreign_key_integrity(reader)
            assert runs[0]["fresh_generation"] == runs[1]["fresh_generation"]
    finally:
        (tmp_path / "reuse-pilot.json").write_text(
            json.dumps(evidence, indent=2) + "\n"
        )
