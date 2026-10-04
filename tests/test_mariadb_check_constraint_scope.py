"""MariaDB CHECK names are scoped to their table, including offline coexistence."""

from __future__ import annotations

from contextlib import closing

import pytest
from vnext_fault_harness import open_connector

from h2hdb import CoreConfig
from h2hdb.schema_epoch import MariaDBSchemaEpochCatalog, SchemaEpochValidationError
from h2hdb.vnext_schema_provider import (
    GeneratedVNextSchemaProvider,
    _validate_mariadb_relation,
)

pytestmark = pytest.mark.backend_specific(
    backend="mariadb",
    reason="MariaDB INFORMATION_SCHEMA CHECK constraint names are table-scoped and use native catalog validators",
)


@pytest.mark.mariadb
def test_same_check_name_on_distinct_tables_keeps_exact_relation_validation(
    mariadb_config: CoreConfig,
) -> None:
    provider = GeneratedVNextSchemaProvider("mariadb")
    relation = next(
        item
        for item in provider.generated_definition_data["relations"]
        if item["relation"] == "gallery_gid_identity"
    )
    schema_slice = next(
        item
        for item in provider.definition.slices
        if item.slice_id == "relation:gallery_gid_identity"
    )
    other_table = "check_scope_other_gallery_identity"
    name = relation["checks"][0][0]
    other = dict(relation, table=other_table, checks=((name, "gid > 7"),))
    with closing(open_connector(mariadb_config)) as connector:
        for statement in schema_slice.statements:
            connector.execute(statement.sql)
        connector.execute(
            f"CREATE TABLE {other_table} (gid BIGINT UNSIGNED NOT NULL, "
            f"PRIMARY KEY (gid), CONSTRAINT {name} CHECK (gid > 7)) "
            "ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_nopad_bin"
        )
        connector.commit()
        _validate_mariadb_relation(connector, relation)
        _validate_mariadb_relation(connector, other)
        with pytest.raises(SchemaEpochValidationError, match="CHECK constraints drift"):
            _validate_mariadb_relation(
                connector, dict(other, checks=relation["checks"])
            )


@pytest.mark.mariadb
def test_control_checks_ignore_same_names_on_another_table(
    mariadb_config: CoreConfig,
) -> None:
    catalog = MariaDBSchemaEpochCatalog()
    with closing(open_connector(mariadb_config)) as connector:
        catalog.create_control_table(connector)
        connector.execute(
            "CREATE TABLE check_scope_other_control (singleton_id INT NOT NULL, "
            "CONSTRAINT ck_schema_epoch_control_singleton CHECK (singleton_id = 2))"
        )
        connector.commit()
        catalog.validate_control_table(connector)
