from __future__ import annotations

import ast
import hashlib
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import vnext_schema_provider as provider_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.schema_epoch import SchemaEpochValidationError, SchemaSeedStatement
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import (
    GeneratedVNextSchemaProvider,
    VNextSchemaProviderUnavailableError,
)

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "generate-vnext-schema-provider.py"
GENERATED = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
DATA_PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
OPERATIONAL_PHYSICAL = ROOT / "verification" / "schema" / "operational_physical.toml"
ARTIFACT_DATA = cast(dict[str, Any], ARTIFACT)


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def test_generated_provider_artifact_is_deterministic_and_current() -> None:
    subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for relative_path, expected_sha256 in ARTIFACT_DATA["source_provenance"]:
        assert (
            hashlib.sha256((ROOT / relative_path).read_bytes()).hexdigest()
            == expected_sha256
        )


def test_runtime_provider_modules_do_not_import_verification() -> None:
    for path in (
        GENERATED,
        ROOT / "src" / "h2hdb" / "vnext_schema_provider.py",
    ):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            str(node.module)
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert not any(
            name == "verification" or name.startswith("verification.")
            for name in imported
        )

    probe = """
import importlib.abc
import sys

class RejectVerification(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'verification' or fullname.startswith('verification.'):
            raise AssertionError(f'runtime imported {fullname}')
        return None

sys.meta_path.insert(0, RejectVerification())
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
provider = GeneratedVNextSchemaProvider('sqlite')
assert provider.generated_definition_data['relations']
assert not any(name == 'verification' or name.startswith('verification.') for name in sys.modules)
"""
    subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_generated_coverage_is_exact_and_excludes_control_and_stubs() -> None:
    data = _load(DATA_PHYSICAL)
    operational = _load(OPERATIONAL_PHYSICAL)
    expected_data = tuple(data["source_slice"])
    expected_operational = tuple(operational["source_slice"])

    assert ARTIFACT_DATA["data_relations"] == expected_data
    assert ARTIFACT_DATA["operational_relations"] == expected_operational
    assert "schema_epoch_control" not in ARTIFACT_DATA["relation_order"]
    assert set(ARTIFACT_DATA["relation_order"]) == set(expected_data) | set(
        expected_operational
    )
    assert len(ARTIFACT_DATA["relation_order"]) == len(expected_data) + len(
        expected_operational
    )

    stub_tables = {value["table"] for value in operational["external_stub"]}
    data_tables = {value["table"] for value in data["relation"]}
    assert stub_tables <= data_tables
    for backend_payload in ARTIFACT_DATA["backends"].values():
        generated_tables = {value["table"] for value in backend_payload["relations"]}
        assert generated_tables == data_tables | {
            value["table"]
            for value in operational["relation"]
            if value["name"] != "schema_epoch_control"
        }
        assert len(generated_tables) == len(backend_payload["relations"])
        assert backend_payload["epoch_control"]["table"] == "h2hdb_schema_epoch"
        assert "h2hdb_schema_epoch" not in generated_tables


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_dependency_order_and_fk_targets_are_closed(backend: str) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relations = {value["relation"]: value for value in payload["relations"]}
    by_table = {value["table"]: value for value in payload["relations"]}
    position = {
        relation_name: index
        for index, relation_name in enumerate(ARTIFACT_DATA["relation_order"])
    }

    for relation_name, relation in relations.items():
        for _name, _columns, target_table, target_columns in relation["foreign_keys"]:
            target = by_table[target_table]
            assert position[target["relation"]] < position[relation_name]
            sql_keys = {target["primary_key"], *target["unique_keys"]}
            assert target_columns in sql_keys
        for dependency in relation["view_dependencies"]:
            assert position[dependency] < position[relation_name]

    slices = payload["slices"]
    assert tuple(value[0].removeprefix("relation:") for value in slices) == tuple(
        ARTIFACT_DATA["relation_order"]
    )
    declared_objects = tuple(
        (kind, name)
        for _slice_id, statements in slices
        for _statement_id, kind, name, _sql in statements
    )
    assert tuple(sorted(declared_objects)) == payload["expected_objects"]
    assert len(declared_objects) == len(set(declared_objects))
    if backend == "mariadb":
        assert all(kind != "index" for kind, _name in declared_objects)
    else:
        assert any(kind == "index" for kind, _name in declared_objects)


def test_formal_seed_and_obligation_contracts_are_machine_bound() -> None:
    from h2hdb import catalog_writer

    data = _load(DATA_PHYSICAL)
    operational = _load(OPERATIONAL_PHYSICAL)
    expected_obligation_ids = tuple(
        value["id"]
        for document in (data, operational)
        for value in document.get("semantic_obligation", ())
    )
    expected_recurring_obligation_ids = tuple(
        value["id"]
        for document in (data, operational)
        for value in document.get("semantic_obligation", ())
        if value["lifecycle"] != "building_only"
    )
    expected_seed_ids = tuple(
        seed_id
        for document in (data, operational)
        for seed_id in (
            *(value["id"] for value in document.get("bootstrap_seed", ())),
            *(
                f"{value['id']}.shard-{shard_no:03d}"
                for value in document.get("bootstrap_seed_range", ())
                for shard_no in range(256)
            ),
        )
    )

    assert (
        tuple(value["id"] for value in ARTIFACT_DATA["semantic_obligations"])
        == expected_obligation_ids
    )
    assert tuple(value["id"] for value in ARTIFACT_DATA["bootstrap_seeds"]) == (
        expected_seed_ids
    )
    assert expected_obligation_ids
    assert expected_seed_ids
    assert len(ARTIFACT_DATA["obligation_manifest_sha256"]) == 64
    cleanup_seeds = tuple(
        value
        for value in ARTIFACT_DATA["bootstrap_seeds"]
        if value["relation"] == "cleanup_sweep_target"
    )
    cleanup_ranges = tuple(operational.get("bootstrap_seed_range", ()))
    assert cleanup_ranges
    assert len(cleanup_seeds) == len(cleanup_ranges) * 256
    cleanup_keys = tuple(bytes.fromhex(value["value"][2][3]) for value in cleanup_seeds)
    assert len(set(cleanup_keys)) == len(cleanup_keys)
    assert all(len(value) == 32 for value in cleanup_keys)

    for backend in ("sqlite", "mariadb"):
        provider = GeneratedVNextSchemaProvider(backend)
        payload = ARTIFACT_DATA["backends"][backend]
        assert tuple(value["seed_id"] for value in payload["bootstrap_seeds"]) == (
            expected_seed_ids
        )
        assert len(payload["seed_manifest_sha256"]) == 64
        assert tuple(provider.semantic_validators) == (
            expected_recurring_obligation_ids
        )
        assert tuple(provider.writer_hook_bindings) == tuple(
            catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS
        )
        assert tuple(provider.writer_hook_bindings) == (
            expected_recurring_obligation_ids
        )
        assert len(provider.writer_hook_bindings) == 25
        assert not provider.blockers
        assert not any("validators are missing" in value for value in provider.blockers)
        assert not any("undeclared IDs" in value for value in provider.blockers)
        assert provider.definition.epoch == ARTIFACT_DATA["epoch"]


def test_generated_provider_rejects_caller_supplied_semantic_validators() -> None:
    with pytest.raises(TypeError):
        GeneratedVNextSchemaProvider(  # type: ignore[call-arg]
            "sqlite",
            {"catalog.identity-codecs.v1": lambda _connector: None},
        )

    provider = GeneratedVNextSchemaProvider("sqlite")
    expected_ids = tuple(
        value["id"]
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    assert tuple(provider.semantic_validators) == expected_ids
    with pytest.raises(TypeError):
        cast(dict[str, Any], provider.semantic_validators)[
            "catalog.identity-codecs.v1"
        ] = lambda _connector: None
    with pytest.raises(TypeError):
        cast(dict[str, Any], provider.writer_hook_bindings)[
            "catalog.identity-codecs.v1"
        ] = object()
    assert provider.definition.schema_version == ARTIFACT_DATA["schema_version"]


def test_generated_provider_reports_every_recurring_writer_hook_exactly() -> None:
    from h2hdb import catalog_writer

    provider = GeneratedVNextSchemaProvider("sqlite")
    recurring = tuple(
        value
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    building_only_ids = tuple(
        value["id"]
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] == "building_only"
    )
    writer_blockers = tuple(
        blocker
        for blocker in provider.blockers
        if blocker.startswith("semantic obligation ") and " writer hook " in blocker
    )

    installed_ids = frozenset(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS)
    unresolved = tuple(
        obligation for obligation in recurring if obligation["id"] not in installed_ids
    )

    assert len(recurring) == 25
    assert len(installed_ids) == 25
    assert len(writer_blockers) == len(unresolved) == 0
    assert installed_ids == frozenset(value["id"] for value in recurring)
    assert installed_ids.isdisjoint(building_only_ids)


def test_generated_provider_derives_writer_blockers_from_wheel_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_writer

    recurring = tuple(
        value
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    expected_calls = tuple(
        (
            value["id"],
            value["contract"]["writer_hook"],
            value["contract"]["writer_hook_version"],
        )
        for value in recurring
    )
    target_id = next(iter(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS))
    unavailable_id, unavailable_name, unavailable_version = next(
        value for value in expected_calls if value[0] == target_id
    )
    calls: list[tuple[str, str, int]] = []
    original_resolver = catalog_writer.resolve_writer_hook

    def resolve(obligation_id: str, name: str, version: int) -> object:
        calls.append((obligation_id, name, version))
        if (obligation_id, name, version) == (
            unavailable_id,
            unavailable_name,
            unavailable_version,
        ):
            raise catalog_writer.WriterHookUnavailableError("probe unavailable")
        return original_resolver(obligation_id, name, version)

    monkeypatch.setattr(catalog_writer, "resolve_writer_hook", resolve)
    provider = GeneratedVNextSchemaProvider("sqlite")
    writer_blockers = tuple(
        blocker
        for blocker in provider.blockers
        if blocker.startswith("semantic obligation ") and " writer hook " in blocker
    )

    assert tuple(calls) == expected_calls
    assert len(writer_blockers) == 1
    probe_blocker = next(
        blocker for blocker in writer_blockers if "probe unavailable" in blocker
    )
    assert unavailable_id in probe_blocker
    assert unavailable_name in probe_blocker
    assert f"v{unavailable_version}" in probe_blocker
    with pytest.raises(VNextSchemaProviderUnavailableError, match="probe unavailable"):
        _ = provider.definition


def test_generated_provider_rejects_a_noncanonical_writer_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_writer

    target_id, canonical = next(
        iter(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS.items())
    )
    original_resolver = catalog_writer.resolve_writer_hook

    def resolve(obligation_id: str, name: str, version: int) -> object:
        binding = original_resolver(obligation_id, name, version)
        return replace(binding) if obligation_id == target_id else binding

    monkeypatch.setattr(catalog_writer, "resolve_writer_hook", resolve)
    provider = GeneratedVNextSchemaProvider("sqlite")

    assert target_id not in provider.writer_hook_bindings
    assert len(provider.writer_hook_bindings) == 24
    assert any(
        repr(target_id) in blocker and "non-canonical binding" in blocker
        for blocker in provider.blockers
    )
    assert canonical is catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS[target_id]
    with pytest.raises(
        VNextSchemaProviderUnavailableError,
        match="non-canonical binding",
    ):
        _ = provider.definition


def test_generated_provider_rejects_registry_omission_and_noop_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_refinement

    installed = dict(catalog_refinement.builtin_semantic_validators())
    missing_id = next(iter(installed))
    monkeypatch.setattr(
        catalog_refinement,
        "builtin_semantic_validators",
        lambda: {
            obligation_id: validator
            for obligation_id, validator in installed.items()
            if obligation_id != missing_id
        },
    )
    missing_provider = GeneratedVNextSchemaProvider("sqlite")
    assert any(
        "validators are missing" in blocker and repr(missing_id) in blocker
        for blocker in missing_provider.blockers
    )
    with pytest.raises(VNextSchemaProviderUnavailableError):
        _ = missing_provider.definition

    unexpected_id = "caller.noop.v1"
    monkeypatch.setattr(
        catalog_refinement,
        "builtin_semantic_validators",
        lambda: {**installed, unexpected_id: lambda _connector: None},
    )
    extra_provider = GeneratedVNextSchemaProvider("sqlite")
    assert any(
        "undeclared IDs" in blocker and repr(unexpected_id) in blocker
        for blocker in extra_provider.blockers
    )
    with pytest.raises(VNextSchemaProviderUnavailableError):
        _ = extra_provider.definition


def test_generated_ready_validation_never_scans_all_foreign_key_rows() -> None:
    source = (ROOT / "src" / "h2hdb" / "vnext_schema_provider.py").read_text()
    assert "PRAGMA foreign_key_check" not in source.replace(
        "Do not run PRAGMA foreign_key_check", ""
    )


def test_generated_mariadb_constraint_names_use_portable_identifier_codec() -> None:
    value = "uk_operational_gallery_observation_staging_request_predecessors_1"
    expected = f"{value[:50]}_{hashlib.sha256(value.encode('ascii')).hexdigest()[:12]}"
    assert len(expected.encode("ascii")) == 63
    assert provider_module._ddl_identifier(value) == expected


def test_generated_seed_statements_are_backend_specific_and_idempotent() -> None:
    sqlite_seeds = ARTIFACT_DATA["backends"]["sqlite"]["bootstrap_seeds"]
    mariadb_seeds = ARTIFACT_DATA["backends"]["mariadb"]["bootstrap_seeds"]
    assert tuple(value["parameters"] for value in sqlite_seeds) == tuple(
        value["parameters"] for value in mariadb_seeds
    )
    assert all(" ON CONFLICT " in value["sql"] for value in sqlite_seeds)
    assert all(value["sql"].endswith(" DO NOTHING") for value in sqlite_seeds)
    assert all(" ON DUPLICATE KEY UPDATE " in value["sql"] for value in mariadb_seeds)
    assert all(
        SchemaSeedStatement(
            seed_id=value["seed_id"],
            target_table=value["target_table"],
            sql=value["sql"],
            parameters=value["parameters"],
        )
        for value in (*sqlite_seeds, *mariadb_seeds)
    )


def test_sqlite_bootstrap_validation_is_exact(tmp_path: Path) -> None:
    payload = ARTIFACT_DATA["backends"]["sqlite"]
    connector = SQLiteConnector(str(tmp_path / "generated-seeds.sqlite3"))
    connector.connect()
    try:
        for _slice_id, statements in payload["slices"]:
            for _statement_id, _kind, _name, sql in statements:
                connector.execute(sql)
        for seed in payload["bootstrap_seeds"]:
            connector.execute(seed["sql"], seed["parameters"])
            connector.execute(seed["sql"], seed["parameters"])

        expected_ids = tuple(value["seed_id"] for value in payload["bootstrap_seeds"])
        assert (
            provider_module._validate_bootstrap_seed_records(connector, payload)
            == expected_ids
        )

        connector.execute(
            "UPDATE operational_revision_allocators "
            "SET next_revision = next_revision + 1 WHERE stream = %s",
            ("SOURCE",),
        )
        with pytest.raises(SchemaEpochValidationError, match="exact generated row"):
            provider_module._validate_bootstrap_seed_records(connector, payload)
    finally:
        connector.close()


def test_generated_manifests_are_backend_specific_and_well_formed() -> None:
    assert ARTIFACT_DATA["artifact_version"] == 1
    assert ARTIFACT_DATA["epoch"] == 2
    assert ARTIFACT_DATA["schema_version"] == 1
    assert len(ARTIFACT_DATA["source_manifest_sha256"]) == 64
    sqlite_manifest = ARTIFACT_DATA["backends"]["sqlite"]["ddl_manifest_sha256"]
    mariadb_manifest = ARTIFACT_DATA["backends"]["mariadb"]["ddl_manifest_sha256"]
    assert len(sqlite_manifest) == len(mariadb_manifest) == 64
    assert sqlite_manifest != mariadb_manifest
    sqlite_seed_manifest = ARTIFACT_DATA["backends"]["sqlite"]["seed_manifest_sha256"]
    mariadb_seed_manifest = ARTIFACT_DATA["backends"]["mariadb"]["seed_manifest_sha256"]
    assert len(sqlite_seed_manifest) == len(mariadb_seed_manifest) == 64
    assert sqlite_seed_manifest != mariadb_seed_manifest


def test_mariadb_view_body_normalization_preserves_semantics() -> None:
    expected = """
        CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS `resolved_value`
            (`analysis_id`, `value`) AS
        SELECT path.`analysis_id` AS `analysis_id`, shadow.`value` AS `value`
        FROM `analysis_path` AS path
        JOIN `analysis_shadow` AS shadow
          ON shadow.`analysis_id` = path.`ancestor_analysis_id`
        WHERE NOT EXISTS (
          SELECT 1 FROM `analysis_tombstone` AS tomb
          WHERE tomb.`analysis_id` = path.`ancestor_analysis_id`
            AND tomb.`value` = shadow.`value`
        )
    """
    stored = """
        select `path`.`analysis_id` AS `analysis_id`,
               `shadow`.`value` AS `value`
        from (`catalog_test`.`analysis_path` `path`
        join `catalog_test`.`analysis_shadow` `shadow`
          on (`shadow`.`analysis_id` = `path`.`ancestor_analysis_id`))
        where !exists (
          select 1 from `catalog_test`.`analysis_tombstone` `tomb`
          where `tomb`.`analysis_id` = `path`.`ancestor_analysis_id`
            and `tomb`.`value` = `shadow`.`value`
          limit 1
        )
    """

    expected_tokens = provider_module._mariadb_view_body_tokens(
        provider_module._mariadb_expected_view_body(expected),
        database_name="catalog_test",
    )
    actual_tokens = provider_module._mariadb_view_body_tokens(
        stored,
        database_name="catalog_test",
    )
    assert actual_tokens == expected_tokens

    wrong = stored.replace("and `tomb`.`value`", "or `tomb`.`value`")
    assert (
        provider_module._mariadb_view_body_tokens(
            wrong,
            database_name="catalog_test",
        )
        != expected_tokens
    )

    # LIMIT 1 is semantics-neutral only at the tail of EXISTS.  It must remain
    # authoritative in a scalar subquery or at the outer SELECT level.
    scalar_limited = "SELECT (SELECT 1 LIMIT 1) AS `value`"
    scalar_unlimited = "SELECT (SELECT 1) AS `value`"
    assert provider_module._mariadb_view_body_tokens(
        scalar_limited,
        database_name="catalog_test",
    ) != provider_module._mariadb_view_body_tokens(
        scalar_unlimited,
        database_name="catalog_test",
    )

    with pytest.raises(
        SchemaEpochValidationError,
        match="unbalanced parentheses",
    ):
        provider_module._mariadb_view_body_tokens(
            stored + "(",
            database_name="catalog_test",
        )


def test_mariadb_check_normalization_preserves_not_equal_literals() -> None:
    assert provider_module._normalize_check(
        "component != X'46494C45' AND label = 'literal != value'"
    ) == provider_module._normalize_check(
        "`component` <> x'46494c45' and `label` = 'literal != value'"
    )
    assert provider_module._normalize_check(
        "label = 'literal != value'"
    ) != provider_module._normalize_check("label = 'literal <> value'")
