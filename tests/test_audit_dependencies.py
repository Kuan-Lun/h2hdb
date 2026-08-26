from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
AUDIT_DEPENDENCIES = ROOT / "scripts" / "audit-dependencies.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


audit = _load_module("h2hdb_audit_dependencies", AUDIT_DEPENDENCIES)


@pytest.mark.parametrize(
    ("satisfying", "expected"),
    [
        (("0.18.1", "0.23.2"), True),
        (("0.18.1", "0.18.2"), False),
    ],
)
def test_node_audit_reports_latest_outside_declared_range(
    monkeypatch: pytest.MonkeyPatch,
    satisfying: tuple[str, ...],
    expected: bool,
) -> None:
    def npm_versions(specification: str) -> tuple[str, ...]:
        if specification == "markdownlint-cli2":
            return ("0.23.2",)
        assert specification == "markdownlint-cli2@>=0.18.1,<0.20"
        return satisfying

    monkeypatch.setattr(audit, "_npm_view_versions", npm_versions)

    result = audit._audit_node("markdownlint-cli2", ">=0.18.1,<0.20")

    assert result["latest"] == "0.23.2"
    assert result["latest_satisfies"] is expected
