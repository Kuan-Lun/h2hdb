from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def manifest() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/dependency_manifest.py"
    specification = importlib.util.spec_from_file_location("manifest_under_test", path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_manifest_preserves_receipt_bytes(manifest: ModuleType) -> None:
    document = {
        "build-system": {"requires": ["hatchling>=1"]},
        "project": {
            "version": "9.9.9",
            "dependencies": ["runtime>=2"],
            "optional-dependencies": {"dev": ["pytest>=8"]},
        },
        "dependency-groups": {"檢查": ["ruff>=1"]},
        "tool": {"unrelated": True},
    }
    package = {"devDependencies": {"markdownlint-cli2": ">=1"}}
    expected = (
        '{"build":["hatchling>=1"],"dependency-groups":{"檢查":["ruff>=1"]},'
        '"node-dev":{"markdownlint-cli2":">=1"},"optional":{"dev":["pytest>=8"]},'
        '"runtime":["runtime>=2"]}'
    ).encode()
    assert manifest.dependency_manifest(document, package) == expected
    assert (
        manifest.dependency_manifest(dict(reversed(document.items())), package)
        == expected
    )


@pytest.mark.parametrize(
    "document",
    [
        {"build-system": {"requires": ["new>=1"]}},
        {"project": {"dependencies": ["new>=1"]}},
        {"project": {"optional-dependencies": {"dev": ["new>=1"]}}},
        {"dependency-groups": {"test": ["new>=1"]}},
    ],
)
def test_each_python_dependency_surface_changes_receipt(
    manifest: ModuleType, document: dict[str, object]
) -> None:
    assert manifest.dependency_manifest(document, {}) != manifest.dependency_manifest(
        {}, {}
    )


def test_node_dependency_changes_receipt(manifest: ModuleType) -> None:
    assert manifest.dependency_manifest(
        {}, {"devDependencies": {"tool": ">=1"}}
    ) != manifest.dependency_manifest({}, {})
