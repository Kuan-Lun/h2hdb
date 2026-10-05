"""Bounded evidence for the closed generated SQL storage-domain contract."""

from __future__ import annotations

import re
import sqlite3
import tomllib
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    inspect_all,
    inspect_one,
    open_generated_database,
    set_foreign_key_checks,
)

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector

ROOT = Path(__file__).resolve().parents[1]
PHYSICAL_MANIFESTS = (
    ROOT / "verification" / "schema" / "physical.toml",
    ROOT / "verification" / "schema" / "operational_physical.toml",
)

# These relations deliberately have no production population path today.  The
# fault evidence therefore exercises their generated DDL directly instead of
# pretending that a facade-produced corpus can contain them.
SCHEMA_ONLY_TABLES = frozenset(
    {
        "catalog_gallery_observation_discovery_fingerprints",
        "catalog_gallery_observation_raw_content",
        "operational_gallery_redownload_states",
    }
)


def _base_relations() -> Iterator[Mapping[str, Any]]:
    for path in PHYSICAL_MANIFESTS:
        with path.open("rb") as stream:
            document = tomllib.load(stream)
        for relation in document["relation"]:
            if (
                relation.get("status") == "implemented"
                and relation.get("kind", "table") == "table"
            ):
                yield cast(Mapping[str, Any], relation)


def _sqlite_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = OFF")
    backend = cast(Mapping[str, Any], ARTIFACT["backends"])["sqlite"]
    for _slice_name, statements in backend["slices"]:
        for _statement_id, _kind, _object_name, statement in statements:
            connection.execute(statement)
    connection.commit()
    return connection


def test_every_generated_base_column_has_a_closed_sqlite_storage_domain() -> None:
    """Statically close every column, including relations absent from corpora."""

    checked_columns = 0
    checked_unsigned = 0
    provider_relations = {
        relation["table"]: relation
        for relation in cast(Mapping[str, Any], ARTIFACT["backends"])["sqlite"][
            "relations"
        ]
        if relation["kind"] == "table"
    }
    epoch_control = cast(Mapping[str, Any], ARTIFACT["backends"])["sqlite"][
        "epoch_control"
    ]
    provider_relations[str(epoch_control["table"])] = epoch_control
    manifest_tables: set[str] = set()

    for relation in _base_relations():
        table = str(relation["table"])
        manifest_tables.add(table)
        sqlite_expression = " AND ".join(
            str(check["sqlite_expression"]) for check in relation.get("check", ())
        )
        provider = provider_relations[table]
        assert (
            tuple(
                (str(check["name"]), str(check["sqlite_expression"]))
                for check in relation.get("check", ())
            )
            == provider["checks"]
        )

        for column in relation["column"]:
            name = str(column["name"])
            sqlite_type = str(column["sqlite"]["type"]).lower()
            assert re.search(
                rf"\btypeof\({re.escape(name)}\) = '{re.escape(sqlite_type)}'",
                sqlite_expression,
            ), f"{table}.{name} has no exact SQLite storage-class predicate"
            checked_columns += 1

            if "UNSIGNED" in str(column["mariadb"]["type"]).upper():
                assert re.search(rf"\b{re.escape(name)} >= 0\b", sqlite_expression), (
                    f"{table}.{name} loses its MariaDB UNSIGNED lower bound on SQLite"
                )
                checked_unsigned += 1

    assert checked_columns > 800
    assert checked_unsigned > 400
    assert SCHEMA_ONLY_TABLES < manifest_tables


_VALID_SCHEMA_ONLY_ROWS: tuple[tuple[str, tuple[object, ...]], ...] = (
    ("catalog_gallery_observation_discovery_fingerprints", (1, 1, b"f" * 40)),
    ("catalog_gallery_observation_raw_content", (1, 1, b"r" * 32)),
    ("operational_gallery_redownload_states", (1, 2, 3, 4)),
)


def _insert_row(connection: SQLConnector, table: str, row: tuple[object, ...]) -> None:
    placeholders = ", ".join("%s" for _value in row)
    connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)


def _assert_schema_only_tables_empty(connection: SQLConnector) -> None:
    for table in sorted(SCHEMA_ONLY_TABLES):
        assert inspect_one(connection, f"SELECT COUNT(*) FROM {table}") == (0,), (
            f"schema-only fixture is not empty: {table}"
        )


def _verify_valid_rows_then_rollback(connection: SQLConnector) -> None:
    # Native execute autocommits without this explicit transaction. Verify the
    # rows were genuinely accepted, then independently prove rollback removed all
    # of them before any invalid INSERT can encounter an unrelated primary key.
    connection.begin()
    try:
        for table, row in _VALID_SCHEMA_ONLY_ROWS:
            _insert_row(connection, table, row)
            assert inspect_all(connection, f"SELECT * FROM {table}") == [row]
    finally:
        connection.rollback()
    _assert_schema_only_tables_empty(connection)


def _assert_domain_rejection(
    connection: SQLConnector,
    table: str,
    row: tuple[object, ...],
    *,
    mariadb_errno: int,
    column: str,
) -> None:
    with pytest.raises(Exception) as failure:
        _insert_row(connection, table, row)
    error = failure.value
    native = (
        (error.__cause__ or error.__context__ or error)
        if isinstance(error, DatabaseDuplicateKeyError)
        else error
    )
    message = f"not a target domain rejection for {table}.{column}: {native}"
    if connector_backend(connection) == "sqlite":
        assert isinstance(native, sqlite3.IntegrityError), message
        assert native.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_CHECK, message
    else:
        assert type(native).__module__.startswith("mysql."), message
        assert getattr(native, "errno", None) == mariadb_errno, message
        assert f"'{column}'" in str(native), message


def test_schema_only_relations_enforce_domains_without_a_production_writer(
    database_factory: DatabaseFactory,
) -> None:
    """The three intentionally unpopulated relations still reject bad rows."""

    connection = open_generated_database(database_factory.config())
    set_foreign_key_checks(connection, enabled=False)
    try:
        _verify_valid_rows_then_rollback(connection)
        invalid_rows: tuple[tuple[str, tuple[object, ...], int, str], ...] = (
            (
                "catalog_gallery_observation_discovery_fingerprints",
                (2, 2, b"f" * 41),
                1406,
                "metadata_fingerprint",
            ),
            (
                "catalog_gallery_observation_raw_content",
                (3, 3, b"r" * 33),
                1406,
                "raw_content_sha256",
            ),
            (
                "operational_gallery_redownload_states",
                (-1, 2, 3, 4),
                1264,
                "gallery_id",
            ),
            (
                "operational_gallery_redownload_states",
                (4, 2, 3, -1),
                1264,
                "updated_at",
            ),
        )
        for table, row, errno, column in invalid_rows:
            _assert_schema_only_tables_empty(connection)
            connection.begin()
            try:
                _assert_domain_rejection(
                    connection, table, row, mariadb_errno=errno, column=column
                )
            finally:
                connection.rollback()
            _assert_schema_only_tables_empty(connection)
    finally:
        connection.close()


def test_schema_only_domain_oracle_rejects_committed_fixture_and_duplicate_key(
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = open_generated_database(database_factory.config())
    set_foreign_key_checks(connection, enabled=False)
    try:
        with monkeypatch.context() as mutation:
            mutation.setattr(connection, "rollback", connection.commit)
            with pytest.raises(
                AssertionError, match="schema-only fixture is not empty"
            ):
                _verify_valid_rows_then_rollback(connection)
        # The faulty rollback genuinely left committed rows. Their duplicate key
        # errors must not masquerade as a domain refusal, even if the empty-table
        # assertion is later removed. This disposable database owns these rows.
        table, row = _VALID_SCHEMA_ONLY_ROWS[0]
        assert inspect_all(connection, f"SELECT * FROM {table}") == [row]
        connection.begin()
        try:
            with pytest.raises(AssertionError, match="not a target domain rejection"):
                _assert_domain_rejection(
                    connection,
                    table,
                    row,
                    mariadb_errno=1406,
                    column="metadata_fingerprint",
                )
        finally:
            connection.rollback()
    finally:
        connection.close()


@pytest.mark.backend_specific(
    backend="sqlite",
    reason="SQLite storage-class CHECKs reject non-BLOB values; MariaDB typed columns normalize binding types and portable stored-domain violations are tested separately",
)
def test_schema_only_relations_enforce_sqlite_binding_storage_classes() -> None:
    connection = _sqlite_connection()
    try:
        for table, row in (
            ("catalog_gallery_observation_discovery_fingerprints", (1, 1, "f" * 40)),
            ("catalog_gallery_observation_raw_content", (1, 1, "r" * 32)),
            ("operational_gallery_redownload_states", (1, 2, 3, b"4")),
        ):
            placeholders = ", ".join("?" for _ in row)
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)
            connection.rollback()
    finally:
        connection.close()


@pytest.mark.backend_specific(
    backend="sqlite",
    reason="SQLite dynamic TEXT affinity losslessly normalizes integer bindings; native MariaDB typed-column domain cases are paired separately",
)
def test_sqlite_text_affinity_preserves_the_declared_storage_domain() -> None:
    """Lossless SQLite affinity conversion stores TEXT, never a foreign class."""

    connection = _sqlite_connection()
    try:
        request_token = b"r" * 16
        connection.execute(
            "INSERT INTO operational_deletion_request_urls "
            "(request_token, url) VALUES (?, ?)",
            (request_token, 7),
        )
        assert connection.execute(
            "SELECT typeof(url), url FROM operational_deletion_request_urls "
            "WHERE request_token = ?",
            (request_token,),
        ).fetchone() == ("text", "7")
    finally:
        connection.close()
