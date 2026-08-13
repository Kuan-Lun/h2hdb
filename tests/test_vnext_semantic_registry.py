from __future__ import annotations

import ast
from pathlib import Path

import pytest

from h2hdb import catalog_refinement, catalog_writer
from h2hdb._generated_vnext_schema import ARTIFACT


def _manifest() -> tuple[dict[str, object], ...]:
    by_id = {
        hook.obligation_id: (hook.name, hook.version)
        for hook in catalog_writer.BUILTIN_WRITER_HOOKS
    }
    return tuple(
        {
            "id": obligation_id,
            "contract": {
                "lifecycle": lifecycle,
                "ready_check": ready_check,
                "writer_hook": by_id[obligation_id][0],
                "writer_hook_version": by_id[obligation_id][1],
            },
        }
        for obligation_id, lifecycle, ready_check in catalog_refinement._SPECS
    )


def test_wheel_registry_requires_the_exact_generated_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(ARTIFACT, "semantic_obligations", _manifest())
    catalog_refinement.validate_builtin_semantic_manifest()

    wrong = list(_manifest())
    first = wrong[0]
    contract = first["contract"]
    assert isinstance(contract, dict)
    wrong[0] = {
        "id": first["id"],
        "contract": {**contract, "writer_hook_version": 2},
    }
    monkeypatch.setitem(ARTIFACT, "semantic_obligations", tuple(wrong))
    with pytest.raises(catalog_refinement.BuiltinSemanticRegistryError):
        catalog_refinement.validate_builtin_semantic_manifest()


def test_semantic_and_writer_dispatch_remain_fail_closed_until_wired() -> None:
    with pytest.raises(
        catalog_refinement.BuiltinSemanticRegistryError,
        match="not wired",
    ):
        catalog_refinement.builtin_semantic_validators()

    hook = catalog_writer.BUILTIN_WRITER_HOOKS[0]
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="known but not wired",
    ):
        catalog_writer.resolve_writer_hook(hook.name, hook.version)
    with pytest.raises(catalog_writer.WriterHookUnavailableError, match="unknown"):
        catalog_writer.resolve_writer_hook("caller.noop", 1)


def test_runtime_semantic_registry_does_not_import_verification() -> None:
    root = Path(__file__).resolve().parents[1]
    for filename in ("catalog_refinement.py", "catalog_writer.py"):
        tree = ast.parse((root / "src" / "h2hdb" / filename).read_text())
        names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        names.update(
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert not any(
            name == "verification" or name.startswith("verification.") for name in names
        )
