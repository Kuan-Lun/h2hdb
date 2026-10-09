"""The local cost oracle must require the actual requested ancestry union."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "ingest_file_validation_cost_probe_under_test"
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts/ingest_file_validation_cost_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = previous
        sys.modules.pop(name, None)


def test_same_grid_size_cannot_replace_an_expected_layer(probe: ModuleType) -> None:
    expected = (b"a" * 16, b"b" * 16)
    wrong = (b"a" * 16, b"c" * 16)
    correct = [(kind, expected, 256) for kind in ("shadow", "tombstone")]
    probe.check_unique_reads(
        correct, expected_layers=frozenset(expected), key_count=128
    )
    degraded = [(kind, wrong, 256) for kind in ("shadow", "tombstone")]
    with pytest.raises(AssertionError, match="family layer identity differs"):
        probe.check_unique_reads(
            degraded, expected_layers=frozenset(expected), key_count=128
        )
