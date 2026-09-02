"""Closed bootstrap fault matrix over every generated seed row.

For every one of the generated bootstrap seed rows (catalog registries and
operational genesis/shard seeds) the matrix omits the row, corrupts one of its
values, and adds a foreign row to its registry, and proves the production
BUILDING-phase exact-seed validator rejects each mutation with zero surviving
writes.  A second, sampled matrix proves the READY-phase full audit rejects
registry corruption that READY still owns, and records the one designed
exception: advanced allocator values are ordinary runtime state after READY.

Partial commits cannot happen on SQLite (construction is one transaction), so
the resume matrix seeds every seed-group prefix as a committed BUILDING
residue and proves the production runner converges to READY with the exact
seed set; the live MariaDB variant interrupts the real batched seed writer.
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from vnext_fault_harness import (
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
    physical_tables,
)
from vnext_pipeline import full_check, initialize_database, populate_catalog

from h2hdb import CoreConfig, DatabaseConfig, VNextDatabaseAdminFacade
from h2hdb.schema_epoch import (
    _SCHEMA_SEED_BATCH_ROWS,
    SchemaEpochDefinition,
    SchemaEpochValidationError,
    SchemaSeedStatement,
    SQLiteSchemaEpochCatalog,
)
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

_INSERT_COLUMNS = re.compile(r"INSERT INTO\s+(\w+)\s*\(([^)]*)\)", re.IGNORECASE)


def _columns(seed: SchemaSeedStatement) -> tuple[str, ...]:
    match = _INSERT_COLUMNS.search(seed.sql)
    assert match is not None, seed.sql
    assert match.group(1) == seed.target_table
    return tuple(column.strip().strip('"`') for column in match.group(2).split(","))


def _where(
    columns: tuple[str, ...], values: tuple[Any, ...]
) -> tuple[str, tuple[Any, ...]]:
    clauses: list[str] = []
    bound: list[Any] = []
    for column, value in zip(columns, values, strict=True):
        if value is None:
            clauses.append(f"{column} IS NULL")
        else:
            clauses.append(f"{column} = %s")
            bound.append(value)
    return " AND ".join(clauses), tuple(bound)


def _corrupted(value: Any) -> Any:
    if isinstance(value, bool):
        return int(not value)
    if isinstance(value, int):
        return value + 1
    if isinstance(value, bytes):
        return bytes([value[0] ^ 0xFF]) + value[1:] if value else b"\x01"
    if isinstance(value, str):
        return value + "x"
    raise TypeError(type(value))


def _foreign_row(seed: SchemaSeedStatement) -> tuple[Any, ...]:
    """A row whose every value differs from the generated seed row."""

    return tuple(
        None
        if value is None
        else (
            value + 100_000
            if isinstance(value, int) and not isinstance(value, bool)
            else _corrupted(value)
        )
        for value in seed.parameters
    )


@pytest.fixture(scope="module")
def sqlite_definition() -> SchemaEpochDefinition:
    return GeneratedVNextSchemaProvider("sqlite").definition


@pytest.fixture(scope="module")
def ready_sqlite(tmp_path_factory: pytest.TempPathFactory) -> CoreConfig:
    path = tmp_path_factory.mktemp("bootstrap") / "ready.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    return config


def _seed_tables(definition: SchemaEpochDefinition) -> list[str]:
    return sorted({seed.target_table for seed in definition.bootstrap_seeds})


def _mutations(
    seed: SchemaSeedStatement,
) -> Iterator[tuple[str, str, tuple[Any, ...]]]:
    columns = _columns(seed)
    where, bound = _where(columns, seed.parameters)
    yield "omit", f"DELETE FROM {seed.target_table} WHERE {where}", bound
    for index, value in enumerate(seed.parameters):
        if value is None or (isinstance(value, int) and columns[index].endswith("_id")):
            continue
        yield (
            f"corrupt:{columns[index]}",
            f"UPDATE {seed.target_table} SET {columns[index]} = %s WHERE {where}",
            (_corrupted(value), *bound),
        )
        break
    placeholders = ", ".join("%s" for _ in columns)
    yield (
        "foreign",
        f"INSERT INTO {seed.target_table} ({', '.join(columns)}) VALUES ({placeholders})",
        _foreign_row(seed),
    )


@pytest.mark.parametrize(
    "table",
    sorted(
        {
            seed.target_table
            for seed in GeneratedVNextSchemaProvider(
                "sqlite"
            ).definition.bootstrap_seeds
        }
    ),
)
def test_building_validator_rejects_every_seed_row_omission_corruption_and_foreign_row(
    ready_sqlite: CoreConfig,
    sqlite_definition: SchemaEpochDefinition,
    table: str,
) -> None:
    provider = GeneratedVNextSchemaProvider("sqlite")
    seeds = [
        seed for seed in sqlite_definition.bootstrap_seeds if seed.target_table == table
    ]
    assert seeds
    connector = open_connector(ready_sqlite)
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.read_transaction():
            baseline = tuple(provider.validate_bootstrap_seeds(connector))
        assert baseline == tuple(
            seed.seed_id for seed in sqlite_definition.bootstrap_seeds
        )
        checked = 0
        rejected_by_schema = 0
        for seed in seeds:
            for kind, sql, bound in _mutations(seed):
                connector.begin()
                try:
                    try:
                        affected = connector.execute_affected(sql, bound)
                    except DatabaseDuplicateKeyError:
                        # The generated UNIQUE/CHECK constraints reject the
                        # mutation before any validator runs; that is also
                        # fail-closed and is counted separately.
                        assert kind == "foreign" or kind.startswith("corrupt:"), kind
                        rejected_by_schema += 1
                        continue
                    assert affected == 1, (seed.seed_id, kind, affected)
                    with pytest.raises(SchemaEpochValidationError):
                        provider.validate_bootstrap_seeds(connector)
                    checked += 1
                finally:
                    # Every mutation is discarded; the READY database never
                    # carries a corrupted seed forward.
                    connector.rollback()
        with connector.read_transaction():
            assert tuple(provider.validate_bootstrap_seeds(connector)) == baseline
    finally:
        connector.close()
    # Every row was omitted (validator), and every foreign/corrupt attempt was
    # refused either by the schema or by the validator.
    assert checked >= len(seeds)
    assert checked + rejected_by_schema >= 2 * len(seeds)


READY_OWNED_REGISTRIES = {
    # Catalog registries are exact READY authority.
    "catalog_canonical_digest_policies",
    "catalog_channel_registry",
    "catalog_source_provider_registry",
    "catalog_search_policies",
    "catalog_resource_kinds",
    "catalog_contributor_role_registry",
    "catalog_analysis_stages",
    "catalog_publication_stages",
    # Operational cleanup registries are exact READY authority.
    "operational_cleanup_target_kinds",
    "operational_cleanup_phases",
    "operational_cleanup_sweep_targets",
    # Deletion generation genesis and the publication generation genesis node
    # are immutable, FK-protected history.
    "operational_deletion_request_generations",
    "catalog_publication_generation_nodes",
    # The request budget singleton must stay congruent with retained requests.
    "operational_gallery_observation_staging_request_budgets",
}
RUNTIME_ADVANCING_REGISTRIES = {
    # READY validation accepts advanced allocator and head values by design.
    "operational_revision_allocators",
    "operational_identity_allocators",
    "operational_deletion_request_generation_heads",
}
# Registry mutations that neither the schema nor the READY audit rejects on a
# populated catalog.  Each is a reviewed design boundary, not a gap: an extra
# unreferenced registry row is inert because every consumer pins the exact
# registered row it uses and READY revalidates those pinned rows.
READY_INERT_BY_DESIGN = {
    ("catalog_contributor_role_registry", "foreign"),
    ("operational_deletion_request_generations", "foreign"),
}


def test_every_seed_registry_is_classified_for_ready_audit_scope(
    sqlite_definition: SchemaEpochDefinition,
) -> None:
    tables = set(_seed_tables(sqlite_definition))
    assert tables == READY_OWNED_REGISTRIES | RUNTIME_ADVANCING_REGISTRIES
    assert not READY_OWNED_REGISTRIES & RUNTIME_ADVANCING_REGISTRIES
    assert {table for table, _kind in READY_INERT_BY_DESIGN} <= READY_OWNED_REGISTRIES


@pytest.fixture(scope="module")
def populated_sqlite(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("bootstrap-populated") / "populated.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    populate_catalog(config)
    return path


def _apply_or_schema_reject(
    config: CoreConfig, sql: str, bound: tuple[Any, ...]
) -> bool:
    """Apply one corruption with referential integrity enforced; True when the
    schema itself refused it (a fail-closed outcome that needs no audit)."""

    connector = open_connector(config)
    try:
        try:
            with connector.transaction():
                assert connector.execute_affected(sql, bound) == 1
        except DatabaseDuplicateKeyError:
            return True
        return False
    finally:
        connector.close()


@pytest.mark.parametrize("table", sorted(READY_OWNED_REGISTRIES))
def test_ready_full_audit_rejects_registry_omission_corruption_and_foreign_rows(
    tmp_path: Path,
    populated_sqlite: Path,
    sqlite_definition: SchemaEpochDefinition,
    table: str,
) -> None:
    """On a populated READY catalog, every registry seed omission and value
    corruption is refused by referential integrity or by the full audit; the
    only accepted mutations are the reviewed inert extra rows."""

    seeds = [
        seed for seed in sqlite_definition.bootstrap_seeds if seed.target_table == table
    ]
    # One representative seed row per registry keeps the full audit affordable;
    # the BUILDING matrix above covers every row exactly.
    seed = seeds[0]
    outcomes: dict[str, str] = {}
    for kind, sql, bound in _mutations(seed):
        path = tmp_path / f"{table}-{kind.split(':')[0]}.sqlite3"
        shutil.copyfile(populated_sqlite, path)
        config = CoreConfig(
            database=DatabaseConfig(sql_type="sqlite", database=str(path))
        )
        if _apply_or_schema_reject(config, sql, bound):
            outcomes[kind] = "schema"
            continue
        try:
            VNextDatabaseAdminFacade(config).check()
        except Exception as error:
            assert type(error).__module__.startswith("h2hdb."), (table, kind, error)
            outcomes[kind] = "audit"
        else:
            outcomes[kind] = "accepted"
    accepted = {kind for kind, outcome in outcomes.items() if outcome == "accepted"}
    assert accepted == {
        kind for registry, kind in READY_INERT_BY_DESIGN if registry == table
    }, outcomes
    assert outcomes["omit"] in {"schema", "audit"}, outcomes


@pytest.mark.parametrize("table", sorted(RUNTIME_ADVANCING_REGISTRIES))
def test_ready_audit_accepts_advanced_runtime_registry_values_but_building_rejects_them(
    tmp_path: Path,
    sqlite_definition: SchemaEpochDefinition,
    table: str,
) -> None:
    seed = next(s for s in sqlite_definition.bootstrap_seeds if s.target_table == table)
    columns = _columns(seed)
    where, bound = _where(columns, seed.parameters)
    advancing = [
        column
        for column, value in zip(columns, seed.parameters, strict=True)
        if isinstance(value, int)
        and column in {"next_revision", "next_id", "current_generation"}
    ]
    assert advancing, columns
    column = advancing[0]
    path = tmp_path / f"{table}.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    provider = GeneratedVNextSchemaProvider("sqlite")
    connector = open_connector(config)
    try:
        with connector.transaction():
            if table == "operational_deletion_request_generation_heads":
                # A head may only advance onto retained immutable history.
                connector.execute(
                    "INSERT INTO operational_deletion_request_generations "
                    "(generation, allocated_at) VALUES (1, 1)"
                )
            assert (
                connector.execute_affected(
                    f"UPDATE {table} SET {column} = {column} + 1 WHERE {where}", bound
                )
                == 1
            )
        with connector.read_transaction():
            with pytest.raises(SchemaEpochValidationError):
                provider.validate_bootstrap_seeds(connector)
    finally:
        connector.close()
    assert full_check(config).state == "READY"


def _seed_group_boundaries(definition: SchemaEpochDefinition) -> list[int]:
    boundaries: list[int] = [0]
    seeds = definition.bootstrap_seeds
    for index in range(1, len(seeds)):
        if seeds[index].sql != seeds[index - 1].sql:
            boundaries.append(index)
    boundaries.append(len(seeds))
    return boundaries


def _seed_building_prefix(
    connector: SQLConnector,
    definition: SchemaEpochDefinition,
    *,
    seeded: int,
) -> None:
    with connector.transaction():
        SQLiteSchemaEpochCatalog().create_control_table(connector)
        connector.execute(
            "INSERT INTO h2hdb_schema_epoch (singleton_id, epoch, schema_version, "
            "state, manifest_sha256, started_at, ready_at) "
            "VALUES (1, 3, 2, 'BUILDING', %s, 1, NULL)",
            (bytes.fromhex(definition.manifest_sha256),),
        )
        for schema_slice in definition.slices:
            for statement in schema_slice.statements:
                connector.execute(statement.sql)
        for seed in definition.bootstrap_seeds[:seeded]:
            connector.execute(seed.sql, seed.parameters)


@pytest.mark.parametrize(
    "boundary",
    _seed_group_boundaries(GeneratedVNextSchemaProvider("sqlite").definition),
)
def test_sqlite_committed_seed_prefix_resumes_to_the_exact_ready_seed_set(
    tmp_path: Path,
    sqlite_definition: SchemaEpochDefinition,
    boundary: int,
) -> None:
    path = tmp_path / f"prefix-{boundary}.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    connector = open_connector(config)
    try:
        _seed_building_prefix(connector, sqlite_definition, seeded=boundary)
    finally:
        connector.close()
    report = VNextDatabaseAdminFacade(config).initialize()
    assert (
        report.state == "READY"
        and report.resumed_build
        and report.transitioned_to_ready
    )
    assert report.bootstrap_seed_ids == tuple(
        seed.seed_id for seed in sqlite_definition.bootstrap_seeds
    )
    provider = GeneratedVNextSchemaProvider("sqlite")
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            assert (
                tuple(provider.validate_bootstrap_seeds(connector))
                == report.bootstrap_seed_ids
            )
    finally:
        connector.close()
    assert full_check(config).state == "READY"


def _seed_batches(definition: SchemaEpochDefinition) -> int:
    """Replicate the production writer's bounded, SQL-aligned batching."""

    batches = 0
    current_sql: str | None = None
    rows = 0
    for seed in definition.bootstrap_seeds:
        if current_sql is not None and (
            seed.sql != current_sql or rows == _SCHEMA_SEED_BATCH_ROWS
        ):
            batches += 1
            current_sql = None
            rows = 0
        if current_sql is None:
            current_sql = seed.sql
        rows += 1
    return batches + 1


_MARIADB_DEFINITION = GeneratedVNextSchemaProvider("mariadb").definition
_MARIADB_SEED_SQL = frozenset(seed.sql for seed in _MARIADB_DEFINITION.bootstrap_seeds)


class _SeedBatchStop(InjectedFault):
    pass


@pytest.mark.parametrize(
    "batch_ordinal", range(1, _seed_batches(_MARIADB_DEFINITION) + 1)
)
def test_live_mariadb_interrupted_seed_batch_resumes_to_the_exact_ready_seed_set(
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    batch_ordinal: int,
) -> None:
    """Interrupt the real batched seed writer before its ``batch_ordinal``-th
    seed batch, leaving a committed prefix, then resume through the public
    administration facade.

    MariaDB commits every bounded seed batch (DDL and INSERT batches are not
    atomic across the epoch), so this is the only backend where a partially
    committed bootstrap residue can exist.
    """

    injector = FaultInjector()
    original = injector.before_mutation
    seed_batches = 0

    def stop_before_batch(sql: str) -> None:
        nonlocal seed_batches
        if sql in _MARIADB_SEED_SQL:
            seed_batches += 1
            if seed_batches == batch_ordinal:
                raise _SeedBatchStop(f"interrupted before seed batch {batch_ordinal}")
        original(sql)

    injector.before_mutation = stop_before_batch  # type: ignore[method-assign]  # test seam
    with fault_injection(monkeypatch, injector):
        with pytest.raises(_SeedBatchStop):
            VNextDatabaseAdminFacade(mariadb_config).initialize()
    assert seed_batches == batch_ordinal
    connector = open_connector(mariadb_config)
    try:
        assert connector.fetch_one(
            "SELECT state FROM h2hdb_schema_epoch WHERE singleton_id = 1"
        ) == ("BUILDING",)
    finally:
        connector.close()
    resumed = VNextDatabaseAdminFacade(mariadb_config).initialize()
    assert resumed.state == "READY" and resumed.resumed_build
    assert resumed.transitioned_to_ready
    assert resumed.bootstrap_seed_ids == tuple(
        seed.seed_id for seed in _MARIADB_DEFINITION.bootstrap_seeds
    )
    assert full_check(mariadb_config).state == "READY"


def test_generated_seed_manifest_is_closed_over_physical_tables(
    sqlite_definition: SchemaEpochDefinition,
) -> None:
    tables = set(physical_tables("sqlite"))
    assert {seed.target_table for seed in sqlite_definition.bootstrap_seeds} <= tables
    assert len(sqlite_definition.bootstrap_seeds) == 6_094
