"""Bootstrap fault evidence without a database-per-generated-row matrix.

The bounded default-profile matrix exercises the production exact comparison
helpers over every row and every cell in both generated backend payloads.  A
single fake-connector traversal connects that pure closed matrix to the public
provider implementation.  Backend integration remains deliberately small:
sampled READY corruption and committed-prefix resume cases cover the SQL and
transaction boundary without rebuilding a database for every generated row.
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Literal, cast

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
from h2hdb import vnext_schema_provider as provider_module
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


def _payload_records(
    payload: Mapping[str, Any], key: str
) -> tuple[dict[str, Any], ...]:
    records = payload[key]
    assert isinstance(records, tuple)
    assert all(isinstance(record, dict) for record in records)
    return cast(tuple[dict[str, Any], ...], records)


def _different_canonical_value(value: Any) -> bytes | int | str:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return value + b"\x00"
    if isinstance(value, int) and not isinstance(value, bool):
        return value + 1
    if isinstance(value, str):
        return value + "\x00"
    raise TypeError(type(value))


@pytest.mark.merge_smoke
@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_closed_generated_bootstrap_seed_matrix_rejects_every_row_and_cell_difference(
    backend: Literal["sqlite", "mariadb"],
) -> None:
    """Every generated keyed seed is an exact singleton, in pure O(total cells)."""

    payload = GeneratedVNextSchemaProvider(backend).generated_definition_data
    seeds = _payload_records(payload, "bootstrap_seeds")
    assert seeds
    checked_rows = 0
    checked_cells = 0
    for seed in seeds:
        seed_id = cast(str, seed["seed_id"])
        expected = cast(tuple[bytes | int | str | None, ...], seed["expected_row"])
        provider_module._validate_exact_bootstrap_seed_row(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
            seed_id,
            [expected],
            expected,
        )
        with pytest.raises(SchemaEpochValidationError, match="exact generated row"):
            provider_module._validate_exact_bootstrap_seed_row(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
                seed_id,
                [],
                expected,
            )
        with pytest.raises(SchemaEpochValidationError, match="exact generated row"):
            provider_module._validate_exact_bootstrap_seed_row(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
                seed_id,
                [expected, expected],
                expected,
            )
        for index, value in enumerate(expected):
            corrupted = (
                *expected[:index],
                _different_canonical_value(value),
                *expected[index + 1 :],
            )
            assert corrupted != expected
            with pytest.raises(
                SchemaEpochValidationError,
                match="exact generated row",
            ):
                provider_module._validate_exact_bootstrap_seed_row(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
                    seed_id,
                    [corrupted],
                    expected,
                )
            checked_cells += 1
        checked_rows += 1

    assert checked_rows == len(seeds)
    assert checked_cells == sum(len(seed["expected_row"]) for seed in seeds)


@pytest.mark.merge_smoke
@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_closed_generated_bootstrap_relation_matrix_rejects_count_differences(
    backend: Literal["sqlite", "mariadb"],
) -> None:
    payload = GeneratedVNextSchemaProvider(backend).generated_definition_data
    relations = _payload_records(payload, "bootstrap_seeded_relations")
    for relation in relations:
        relation_name = cast(str, relation["relation"])
        expected = cast(
            tuple[tuple[bytes | int | str | None, ...], ...],
            relation["expected_rows"],
        )
        assert expected
        assert cast(str, relation["validation_sql"]).endswith(
            f" LIMIT {len(expected) + 1}"
        )
        provider_module._validate_exact_bootstrap_relation_rows(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
            relation_name,
            expected,
            expected,
        )
        provider_module._validate_exact_bootstrap_relation_rows(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
            relation_name,
            tuple(reversed(expected)),
            expected,
        )
        foreign = tuple(_different_canonical_value(value) for value in expected[0])
        assert foreign not in expected
        for actual in (
            expected[1:],
            (*expected, expected[0]),
            (*expected, foreign),
        ):
            with pytest.raises(
                SchemaEpochValidationError,
                match="generated genesis rows",
            ):
                provider_module._validate_exact_bootstrap_relation_rows(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
                    relation_name,
                    actual,
                    expected,
                )

    absent_relations = _payload_records(payload, "bootstrap_absent_relations")
    for relation in absent_relations:
        relation_name = cast(str, relation["relation"])
        provider_module._validate_empty_bootstrap_relation(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
            relation_name,
            [],
        )
        with pytest.raises(SchemaEpochValidationError, match="contains data"):
            provider_module._validate_empty_bootstrap_relation(  # noqa: SLF001 -- production trust boundary under closed-matrix verification
                relation_name,
                [(1,)],
            )


class _GeneratedBootstrapConnector:
    """Data-only connector double for one complete production traversal."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._responses: dict[tuple[str, tuple[Any, ...]], list[tuple[Any, ...]]] = {}
        for seed in _payload_records(payload, "bootstrap_seeds"):
            self._responses[
                (
                    cast(str, seed["validation_sql"]),
                    cast(tuple[Any, ...], seed["validation_parameters"]),
                )
            ] = [cast(tuple[Any, ...], seed["expected_row"])]
        for relation in _payload_records(payload, "bootstrap_seeded_relations"):
            self._responses[(cast(str, relation["validation_sql"]), ())] = list(
                cast(tuple[tuple[Any, ...], ...], relation["expected_rows"])
            )
        for relation in _payload_records(payload, "bootstrap_absent_relations"):
            self._responses[(cast(str, relation["validation_sql"]), ())] = []

    def fetch_all(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        key = (query, data)
        self.calls.append(key)
        return list(self._responses[key])


@pytest.mark.merge_smoke
@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_bootstrap_validator_traverses_the_exact_fake_connector_manifest(
    backend: Literal["sqlite", "mariadb"],
) -> None:
    provider = GeneratedVNextSchemaProvider(backend)
    payload = provider.generated_definition_data
    connector = _GeneratedBootstrapConnector(payload)

    completed = provider_module._validate_bootstrap_seed_records(  # noqa: SLF001 -- production traversal is the subject of this integration test
        cast(SQLConnector, connector),
        payload,
    )

    seeds = _payload_records(payload, "bootstrap_seeds")
    assert completed == tuple(cast(str, seed["seed_id"]) for seed in seeds)
    assert len(connector.calls) == sum(
        len(_payload_records(payload, key))
        for key in (
            "bootstrap_seeds",
            "bootstrap_seeded_relations",
            "bootstrap_absent_relations",
        )
    )


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


@pytest.fixture(scope="module")
def empty_ready_sqlite(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Keep bootstrap-only runtime seeds present for their READY negatives."""

    path = tmp_path_factory.mktemp("bootstrap-empty") / "empty.sqlite3"
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
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
    empty_ready_sqlite: Path,
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
        # Generation zero is required by an empty READY catalog but is
        # legitimately compacted after the first publication.  Exercise its
        # bootstrap authority before that runtime transition; every other
        # registry benefits from the populated fixture's FK coverage.
        source = (
            empty_ready_sqlite
            if table == "catalog_publication_generation_nodes"
            else populated_sqlite
        )
        shutil.copyfile(source, path)
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
            "VALUES (1, %s, %s, 'BUILDING', %s, 1, NULL)",
            (
                definition.epoch,
                definition.schema_version,
                bytes.fromhex(definition.manifest_sha256),
            ),
        )
        for schema_slice in definition.slices:
            for statement in schema_slice.statements:
                connector.execute(statement.sql)
        for seed in definition.bootstrap_seeds[:seeded]:
            connector.execute(seed.sql, seed.parameters)


@pytest.mark.merge_smoke
def test_sqlite_committed_seed_prefix_resumes_to_the_exact_ready_seed_set(
    tmp_path: Path,
    sqlite_definition: SchemaEpochDefinition,
) -> None:
    # One prefix just beyond the batch hard-cap represents arbitrary committed
    # generated prefixes; the unbounded Lean theorem covers every prefix size.
    boundary = _SCHEMA_SEED_BATCH_ROWS + 1
    path = tmp_path / "representative-prefix.sqlite3"
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
    assert VNextDatabaseAdminFacade(config).check_readiness().state == "READY"


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


def test_live_mariadb_interrupted_seed_batch_resumes_to_the_exact_ready_seed_set(
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interrupt one representative real batch, then resume through the facade.

    MariaDB commits every bounded seed batch (DDL and INSERT batches are not
    atomic across the epoch). The data-only matrix and unbounded replay theorem
    cover all other batch positions without repeatedly rebuilding MariaDB.
    """

    batch_ordinal = _seed_batches(_MARIADB_DEFINITION) // 2
    injector = FaultInjector()
    seed_batches = 0

    def stop_before_batch(sql: str) -> None:
        nonlocal seed_batches
        if sql in _MARIADB_SEED_SQL:
            seed_batches += 1
            if seed_batches == batch_ordinal:
                raise _SeedBatchStop(f"interrupted before seed batch {batch_ordinal}")

    injector.on_before_mutation = stop_before_batch
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
    seeds = sqlite_definition.bootstrap_seeds
    assert seeds
    assert {seed.target_table for seed in seeds} <= tables
    assert len({seed.seed_id for seed in seeds}) == len(seeds)
