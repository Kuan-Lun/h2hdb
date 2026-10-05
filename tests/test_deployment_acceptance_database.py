"""Pure controls for deployment backend admission; no database engine is run."""

from __future__ import annotations

import copy
import importlib.util
import sqlite3
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

from h2hdb import CoreConfig, DatabaseAccessMode, DatabaseConfig
from h2hdb.sql_connector import DatabaseReadOnlyError


def _load_module() -> ModuleType:
    source = (
        Path(__file__).resolve().parents[1]
        / "scripts/deployment_acceptance/database.py"
    )
    spec = importlib.util.spec_from_file_location(
        "acceptance_database_under_test", source
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


database = _load_module()


def _authority(backend: str, role: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "passed",
        "backend": backend,
        "role": role,
        "revision": 7,
        "publications": 4,
    }
    if backend == "sqlite":
        result.update(
            database=str(database.SQLITE_DATABASE),
            device=9,
            inode=13,
            journal_mode="wal",
            foreign_keys=1,
            synchronous=2,
            query_only=int(role == "opds"),
            native_write_refused=role == "opds",
        )
    else:
        result.update(
            database=database.DATABASE_NAME,
            server_version="10.11-MariaDB",
            transaction_read_only=int(role == "opds"),
        )
    if role == "opds":
        result["write_transaction_refused"] = True
    return result


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
@pytest.mark.parametrize(
    "change",
    [
        None,
        "missing-role",
        "backend",
        "revision",
        "count",
        "database",
        "reader-refusal",
    ],
)
def test_role_receipts_require_same_native_database_and_exact_public_oracle(
    backend: str,
    change: str | None,
) -> None:
    values = {role: _authority(backend, role) for role in ("ingest", "opds")}
    if change is None:
        database.validate_role_authorities(
            values, backend=backend, revision=7, publications=4
        )
        return
    match change:
        case "missing-role":
            values.pop("opds")
        case "backend":
            values["opds"]["backend"] = "wrong"
        case "revision":
            values["opds"]["revision"] = 8
        case "count":
            values["opds"]["publications"] = 0
        case "database":
            values["opds"]["database"] = "wrong"
        case _:
            values["opds"]["write_transaction_refused"] = False
    with pytest.raises(AssertionError):
        database.validate_role_authorities(
            values, backend=backend, revision=7, publications=4
        )


@pytest.mark.parametrize("field", ["device", "inode"])
@pytest.mark.parametrize("value", [None, 0, 999])
def test_sqlite_config_string_is_not_shared_file_authority(
    field: str, value: object
) -> None:
    values = {role: _authority("sqlite", role) for role in ("ingest", "opds")}
    values["opds"][field] = value
    with pytest.raises(AssertionError):
        database.validate_role_authorities(
            values, backend="sqlite", revision=7, publications=4
        )


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
@pytest.mark.parametrize("role", ["ingest", "opds"])
@pytest.mark.parametrize(
    "bad",
    [None, "native-target", "read-only", "engine", "config-backend", "config-mode"],
)
def test_native_authority_checks_engine_target_and_access_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    role: str,
    bad: str | None,
) -> None:
    path = tmp_path / "catalog.sqlite3"
    path.touch()
    monkeypatch.setattr(database, "SQLITE_DATABASE", path)
    readonly = int(role == "opds")
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type=("sqlite" if backend == "mariadb" else "mariadb")
            if bad == "config-backend"
            else backend,
            database=str(path) if backend == "sqlite" else database.DATABASE_NAME,
            access_mode=DatabaseAccessMode.read_only
            if bool(readonly) != (bad == "config-mode")
            else DatabaseAccessMode.read_write,
        )
    )
    connector = Mock()
    if readonly:
        connector.begin.side_effect = DatabaseReadOnlyError("read only")
    connector.fetch_all.return_value = [
        (0, "main", str(path) if bad != "native-target" else "/other.sqlite3")
    ]
    pragmas = {
        "journal_mode": "wal" if bad != "engine" else "off",
        "query_only": readonly if bad != "read-only" else 1 - readonly,
        "foreign_keys": 1,
        "synchronous": 2,
    }

    def fetch_one(sql: str) -> tuple[object, ...]:
        if sql == "PRAGMA user_version = 0":
            error = sqlite3.OperationalError("attempt to write a readonly database")
            error.sqlite_errorcode = sqlite3.SQLITE_READONLY
            raise error
        if sql.startswith("PRAGMA "):
            return (pragmas[sql.removeprefix("PRAGMA ")],)
        return (
            database.DATABASE_NAME if bad != "native-target" else "wrong",
            "10.11-MariaDB" if bad != "engine" else "SQLite",
            readonly if bad != "read-only" else 1 - readonly,
        )

    connector.fetch_one.side_effect = fetch_one
    if bad is None:
        result = database._native_authority(
            connector, config, backend=backend, role=role
        )
        assert result["backend"] == backend and result["status"] == "passed"
        assert result.get("write_transaction_refused", False) is bool(readonly)
    else:
        with pytest.raises(AssertionError):
            database._native_authority(connector, config, backend=backend, role=role)


def test_volume_initializer_refuses_existing_data_without_connecting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "catalog.sqlite3"
    marker.write_bytes(b"retained")
    monkeypatch.setattr(database, "SQLITE_DIRECTORY", tmp_path)
    monkeypatch.setattr(database, "SQLITE_DATABASE", marker)
    monkeypatch.setattr(database.os, "geteuid", lambda: 0)
    connect = Mock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr(database.sqlite3, "connect", connect)
    with pytest.raises(ValueError, match="not empty"):
        database.initialize_sqlite(uid=65534, gid=65534)
    connect.assert_not_called()
    assert marker.read_bytes() == b"retained"


def test_volume_initializer_leaves_empty_wal_for_ingest_schema_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(database, "SQLITE_DIRECTORY", tmp_path)
    monkeypatch.setattr(database, "SQLITE_DATABASE", tmp_path / "catalog.sqlite3")
    monkeypatch.setattr(database.os, "geteuid", lambda: 0)
    chown = Mock()
    monkeypatch.setattr(database.os, "chown", chown)
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = ("wal",)
    connection.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(database.sqlite3, "connect", Mock(return_value=connection))
    assert database.initialize_sqlite(uid=65534, gid=65534)["status"] == "passed"
    assert connection.execute.call_args_list[0].args == ("PRAGMA journal_mode=WAL",)
    assert connection.execute.call_args_list[1].args == (
        "SELECT name FROM sqlite_master",
    )
    assert chown.call_count == 2
    connection.close.assert_called_once()
    assert (tmp_path / "catalog.sqlite3").stat().st_mode & 0o777 == 0o600
    assert tmp_path.stat().st_mode & 0o777 == 0o700


def test_forged_equal_missing_native_identity_is_rejected() -> None:
    values = {role: _authority("sqlite", role) for role in ("ingest", "opds")}
    for value in values.values():
        value.pop("inode")
    before = copy.deepcopy(values)
    with pytest.raises(AssertionError):
        database.validate_role_authorities(
            values, backend="sqlite", revision=7, publications=4
        )
    assert values == before


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_missing_raw_receipt_fields_cannot_pass_by_equal_none(backend: str) -> None:
    values = {role: _authority(backend, role) for role in ("ingest", "opds")}
    for field in values["ingest"]:
        damaged = copy.deepcopy(values)
        for row in damaged.values():
            row.pop(field, None)
        with pytest.raises(AssertionError):
            database.validate_role_authorities(
                damaged, backend=backend, revision=7, publications=4
            )


@pytest.mark.parametrize("version", [None, "", 123, {"MariaDB": True}])
def test_mariadb_receipt_requires_a_real_server_version(version: object) -> None:
    values = {role: _authority("mariadb", role) for role in ("ingest", "opds")}
    for row in values.values():
        row["server_version"] = version
    with pytest.raises(AssertionError):
        database.validate_role_authorities(
            values, backend="mariadb", revision=7, publications=4
        )
