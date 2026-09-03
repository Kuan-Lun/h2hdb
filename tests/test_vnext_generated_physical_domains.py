"""Bounded evidence for the closed generated SQL storage-domain contract."""

from __future__ import annotations

import re
import sqlite3
import tomllib
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT

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


def test_schema_only_relations_enforce_domains_without_a_production_writer() -> None:
    """The three intentionally unpopulated relations still reject bad rows."""

    connection = _sqlite_connection()
    try:
        valid_rows: tuple[tuple[str, tuple[object, ...]], ...] = (
            (
                "catalog_gallery_observation_discovery_fingerprints",
                (1, 1, b"f" * 40),
            ),
            ("catalog_gallery_observation_raw_content", (1, 1, b"r" * 32)),
            ("operational_gallery_redownload_states", (1, 2, 3, 4)),
        )
        for table, row in valid_rows:
            placeholders = ", ".join("?" for _value in row)
            connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)
        connection.rollback()

        invalid_rows: tuple[tuple[str, tuple[object, ...]], ...] = (
            (
                "catalog_gallery_observation_discovery_fingerprints",
                (1, 1, "f" * 40),
            ),
            ("catalog_gallery_observation_raw_content", (1, 1, "r" * 32)),
            ("operational_gallery_redownload_states", (-1, 2, 3, 4)),
            ("operational_gallery_redownload_states", (1, 2, 3, b"4")),
        )
        for table, row in invalid_rows:
            placeholders = ", ".join("?" for _value in row)
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    f"INSERT INTO {table} VALUES ({placeholders})",
                    row,
                )
            connection.rollback()
    finally:
        connection.close()


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
