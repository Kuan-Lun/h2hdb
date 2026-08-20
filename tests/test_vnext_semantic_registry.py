from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import catalog_refinement, catalog_writer
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.vnext_analysis_repository import AnalysisRepository


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


def test_catalog_semantic_registry_and_writer_bindings_are_closed_world() -> None:
    validators = catalog_refinement.builtin_semantic_validators()
    expected = tuple(
        obligation_id
        for obligation_id, lifecycle, _ready_check in catalog_refinement._SPECS
        if obligation_id.startswith("catalog.") and lifecycle == "ready_and_runtime"
    )
    assert tuple(validators) == expected
    assert len(validators) == 11
    assert "catalog.bootstrap.v1" not in validators
    assert tuple(validator.__name__ for validator in validators.values()) == tuple(
        ready_check.rsplit(".", 1)[1]
        for obligation_id, lifecycle, ready_check in catalog_refinement._SPECS
        if obligation_id.startswith("catalog.") and lifecycle == "ready_and_runtime"
    )
    with pytest.raises(TypeError):
        validators["catalog.bootstrap.v1"] = catalog_refinement.check_bootstrap_v1  # type: ignore[index]

    assert len(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS) == 25
    hook = catalog_writer.BUILTIN_WRITER_HOOKS[0]
    binding = catalog_writer.resolve_writer_hook(
        hook.obligation_id,
        hook.name,
        hook.version,
    )
    assert binding is catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS[hook.obligation_id]
    assert binding.entrypoints
    assert binding.families
    assert binding.authority_relations
    assert binding.mutation_relations
    assert binding.mutation_relations <= binding.authority_relations
    assert all(callable(entrypoint) for entrypoint in binding.entrypoints)

    building_only = next(
        value
        for value in catalog_writer.BUILTIN_WRITER_HOOKS
        if value.obligation_id == "catalog.bootstrap.v1"
    )
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="known but not wired",
    ):
        catalog_writer.resolve_writer_hook(
            building_only.obligation_id,
            building_only.name,
            building_only.version,
        )
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="unknown obligation ID",
    ):
        catalog_writer.resolve_writer_hook("caller.noop.v1", "caller.noop", 1)
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="wrong name/version",
    ):
        catalog_writer.resolve_writer_hook(hook.obligation_id, "caller.noop", 1)

    with pytest.raises(FrozenInstanceError):
        binding.version = 2  # type: ignore[misc]
    with pytest.raises(TypeError):
        cast(dict[str, object], catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS)[
            hook.obligation_id
        ] = binding


def test_analysis_abandon_is_bound_to_every_fenced_state_machine_contract() -> None:
    for obligation_id in (
        "catalog.physical-domains.v1",
        "catalog.state-machines.v1",
        "h2hdb.operational.fencing.v1",
        "h2hdb.operational.maintenance-gate.v1",
    ):
        binding = catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS[obligation_id]
        assert binding.entrypoints.count(AnalysisRepository.abandon) == 1


def test_writer_binding_rejects_empty_duplicate_and_callback_markers() -> None:
    binding = next(iter(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS.values()))
    family = binding.families[0]

    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="at least one transaction family",
    ):
        replace(binding, families=())
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="repeats an entrypoint",
    ):
        replace(family, entrypoints=(family.entrypoints[0],) * 2)
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="public method",
    ):
        replace(family, entrypoints=(lambda *_args, **_kwargs: None,))

    def noop(*_args: object, **_kwargs: object) -> None:
        return None

    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="public method",
    ):
        replace(family, entrypoints=(noop,))
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="public method",
    ):
        replace(
            family,
            entrypoints=(catalog_refinement.check_identity_codecs_v1,),
        )
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="unknown obligation ID",
    ):
        replace(binding, obligation_id="caller.noop.v1")
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="authority_relations must be non-empty",
    ):
        replace(binding, authority_relations=frozenset())
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="mutation_relations must be non-empty",
    ):
        replace(family, mutation_relations=frozenset())


def test_physical_domain_bindings_preserve_guards_and_transaction_owners() -> None:
    from h2hdb.vnext_physical_domains import (
        CATALOG_PHYSICAL_DOMAIN_GUARDS,
        CATALOG_PHYSICAL_DOMAIN_WRITERS,
        OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
        OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
        OPERATIONAL_SCHEMA_EPOCH_WRITERS,
    )

    catalog = catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS["catalog.physical-domains.v1"]
    assert len(catalog.families) == 1
    assert catalog.families[0].entrypoints == CATALOG_PHYSICAL_DOMAIN_WRITERS
    assert catalog.families[0].transaction_owner is (
        catalog_writer.WriterTransactionOwner.CALLER_UOW
    )
    assert catalog.domain_guards == CATALOG_PHYSICAL_DOMAIN_GUARDS

    operational = catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS[
        "h2hdb.operational.physical-domains.v1"
    ]
    assert len(operational.families) == 2
    caller, epoch = operational.families
    assert caller.entrypoints == OPERATIONAL_PHYSICAL_DOMAIN_WRITERS
    assert caller.transaction_owner is catalog_writer.WriterTransactionOwner.CALLER_UOW
    assert "schema_epoch_control" not in caller.mutation_relations
    assert epoch.entrypoints == OPERATIONAL_SCHEMA_EPOCH_WRITERS
    assert epoch.transaction_owner is (
        catalog_writer.WriterTransactionOwner.SCHEMA_EPOCH_RUNNER
    )
    assert epoch.mutation_relations == frozenset({"schema_epoch_control"})
    assert operational.domain_guards == OPERATIONAL_PHYSICAL_DOMAIN_GUARDS
    assert operational.transaction_owners == frozenset(
        {
            catalog_writer.WriterTransactionOwner.CALLER_UOW,
            catalog_writer.WriterTransactionOwner.SCHEMA_EPOCH_RUNNER,
        }
    )

    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="require installed domain guards",
    ):
        replace(catalog, domain_guards=())
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="installed vnext_domains symbol",
    ):
        replace(catalog, domain_guards=(lambda value: value,))
    with pytest.raises(
        catalog_writer.WriterHookUnavailableError,
        match="repeats a domain guard",
    ):
        replace(catalog, domain_guards=(catalog.domain_guards[0],) * 2)


def test_catalog_registry_fails_closed_without_the_receipt_count_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backends = dict(cast(dict[str, Any], ARTIFACT["backends"]))
    sqlite = dict(backends["sqlite"])
    relations = list(sqlite["relations"])
    receipt_index = next(
        index
        for index, relation in enumerate(relations)
        if relation["relation"] == "publication_receipt"
    )
    receipt = dict(relations[receipt_index])
    receipt["columns"] = tuple(
        column for column in receipt["columns"] if column[0] != "publication_count"
    )
    relations[receipt_index] = receipt
    sqlite["relations"] = tuple(relations)
    backends["sqlite"] = sqlite
    monkeypatch.setitem(ARTIFACT, "backends", backends)

    with pytest.raises(
        catalog_refinement.BuiltinSemanticRegistryError,
        match="bounded authoritative publication_count scalar",
    ):
        catalog_refinement.builtin_semantic_validators()


def test_catalog_registry_fails_closed_without_bcnf_publication_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backends = dict(cast(dict[str, Any], ARTIFACT["backends"]))
    sqlite = dict(backends["sqlite"])
    relations = tuple(
        relation
        for relation in sqlite["relations"]
        if relation["relation"] != "catalog_publication_order"
    )
    sqlite["relations"] = relations
    backends["sqlite"] = sqlite
    monkeypatch.setitem(ARTIFACT, "backends", backends)

    with pytest.raises(
        catalog_refinement.BuiltinSemanticRegistryError,
        match="catalog_publication_order is not singular",
    ):
        catalog_refinement.builtin_semantic_validators()


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
