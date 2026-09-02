"""Closed physical-domain fault matrix derived from the physical manifests.

For every column of every generated physical base table the matrix derives
invalid values from the manifest's own CHECK expression and declared types:
wrong binary width, wrong storage class, out-of-range counter or timestamp,
unregistered enum value, and a collation-sensitive case variant of an enum
value.  Each invalid value is applied to a real row of a facade-produced
corpus (see ``vnext_corpora``: catalogs at rest and abandoned turns that hold
the transient relations) inside a transaction, and the matrix proves the
backend rejects it and the transaction rolls back to the exact prior row, or
pins exactly which undeclared storage checks let the backend accept it.

The same matrix runs on SQLite and, opt-in, on live MariaDB 10.11.11; a
separate matrix feeds every production writer-boundary guard the same invalid
classes and proves it raises before any SQL is issued.
"""

from __future__ import annotations

import re
import sqlite3
import tomllib
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from vnext_corpora import NEVER_AT_REST, NO_PRODUCTION_WRITER, build_corpora
from vnext_fault_harness import EPOCH_CONTROL_TABLE, open_connector

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector
from h2hdb.vnext_domains import (
    INT63_MAX,
    UINT32_MAX,
    DomainValidationError,
    require_ascii_bytes,
    require_bool_byte,
    require_bounded_bytes,
    require_digest32,
    require_enum_bytes,
    require_int63,
    require_positive_int63,
    require_text,
    require_uint32,
    require_utf8_bytes,
    require_uuid16,
)
from h2hdb.vnext_physical_domains import (
    CATALOG_PHYSICAL_DOMAIN_GUARDS,
    OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
)

ROOT = Path(__file__).resolve().parents[1]
_MANIFESTS = (
    ROOT / "verification" / "schema" / "physical.toml",
    ROOT / "verification" / "schema" / "operational_physical.toml",
)


@dataclass(frozen=True)
class Column:
    table: str
    name: str
    sqlite_type: str
    mariadb_type: str
    nullable: bool
    checks: tuple[str, ...]


@dataclass(frozen=True)
class Candidate:
    column: Column
    kind: str
    value: object
    backends: frozenset[str]

    @property
    def label(self) -> str:
        return f"{self.column.table}.{self.column.name}:{self.kind}"


def manifest_columns() -> list[Column]:
    columns: list[Column] = []
    for path in _MANIFESTS:
        with path.open("rb") as stream:
            document = tomllib.load(stream)
        for relation in document["relation"]:
            if relation.get("status") != "implemented":
                continue
            checks = tuple(
                str(check["sqlite_expression"]) for check in relation.get("check", [])
            )
            for column in relation["column"]:
                columns.append(
                    Column(
                        str(relation["table"]),
                        str(column["name"]),
                        str(column["sqlite"]["type"]),
                        str(column["mariadb"]["type"]),
                        bool(column["sqlite"]["nullable"]),
                        checks,
                    )
                )
    return columns


_BOTH = frozenset({"sqlite", "mariadb"})
_SQLITE_ONLY = frozenset({"sqlite"})


def _enum_values(expression: str, column: str) -> list[object]:
    match = re.search(rf"\b{re.escape(column)} IN \(([^)]*)\)", expression)
    if match is None:
        return []
    values: list[object] = []
    for token in match.group(1).split(","):
        token = token.strip()
        if token.startswith("X'"):
            values.append(bytes.fromhex(token[2:-1]))
        elif token.startswith("'"):
            values.append(token[1:-1])
        else:
            values.append(int(token))
    return values


def candidates(column: Column) -> Iterator[Candidate]:
    """Derive invalid values for one column from its own manifest checks."""

    name = re.escape(column.name)
    expression = " AND ".join(column.checks)
    typed = re.search(rf"typeof\({name}\) = '(\w+)'", expression)
    storage = typed.group(1) if typed else None
    exact = re.search(rf"length\({name}\) = (\d+)", expression)
    upper = re.search(rf"length\({name}\) <= (\d+)", expression)
    lower = re.search(rf"length\({name}\) >= (\d+)", expression)
    positive = re.search(rf"length\({name}\) > 0", expression)
    between = re.search(rf"\b{name} BETWEEN (-?\d+) AND (-?\d+)", expression)
    at_least = re.search(rf"\b{name} >= (-?\d+)", expression)
    greater = re.search(rf"\b{name} > (-?\d+)", expression)
    at_most = re.search(rf"\b{name} <= (-?\d+)", expression)
    less = re.search(rf"\b{name} < (-?\d+)", expression)
    singleton = re.search(rf"\b{name} = (\d+)\b", expression)
    enums = _enum_values(expression, column.name)

    if storage == "blob" or column.sqlite_type == "BLOB":
        if exact is not None:
            width = int(exact.group(1))
            yield Candidate(column, "width-short", b"\x01" * (width - 1), _BOTH)
            yield Candidate(column, "width-long", b"\x01" * (width + 1), _BOTH)
            yield Candidate(column, "storage-text", "t" * width, _SQLITE_ONLY)
            yield Candidate(column, "storage-integer", 7, _SQLITE_ONLY)
        else:
            if upper is not None:
                yield Candidate(
                    column, "width-long", b"\x01" * (int(upper.group(1)) + 1), _BOTH
                )
            if positive is not None or (lower is not None and int(lower.group(1)) > 0):
                yield Candidate(column, "width-empty", b"", _BOTH)
            yield Candidate(column, "storage-text", "text", _SQLITE_ONLY)
            yield Candidate(column, "storage-integer", 7, _SQLITE_ONLY)
    elif storage == "integer" or column.sqlite_type == "INTEGER":
        yield Candidate(column, "storage-real", 1.5, _SQLITE_ONLY)
        yield Candidate(column, "storage-text", "text", _SQLITE_ONLY)
        yield Candidate(column, "storage-blob", b"\x01", _SQLITE_ONLY)
        if between is not None:
            low, high = int(between.group(1)), int(between.group(2))
            yield Candidate(column, "range-low", low - 1, _BOTH)
            yield Candidate(column, "range-high", high + 1, _BOTH)
        else:
            if at_least is not None:
                yield Candidate(column, "range-low", int(at_least.group(1)) - 1, _BOTH)
            elif greater is not None:
                yield Candidate(column, "range-low", int(greater.group(1)), _BOTH)
            if at_most is not None:
                yield Candidate(column, "range-high", int(at_most.group(1)) + 1, _BOTH)
            elif less is not None:
                yield Candidate(column, "range-high", int(less.group(1)), _BOTH)
        if singleton is not None and not enums:
            yield Candidate(column, "singleton", int(singleton.group(1)) + 1, _BOTH)
        if "UNSIGNED" in column.mariadb_type:
            yield Candidate(column, "range-negative", -1, _BOTH)
    elif storage == "text" or column.sqlite_type == "TEXT":
        yield Candidate(column, "storage-blob", b"\x01\x02", _SQLITE_ONLY)
        yield Candidate(column, "storage-integer", 7, _SQLITE_ONLY)
        if upper is not None:
            yield Candidate(
                column, "width-long", "t" * (int(upper.group(1)) + 1), _BOTH
            )
    if enums:
        sample = enums[0]
        if isinstance(sample, bytes):
            yield Candidate(column, "enum-unregistered", b"bogus", _BOTH)
            if sample.upper() != sample:
                yield Candidate(column, "enum-collation", sample.upper(), _BOTH)
            elif sample.lower() != sample:
                yield Candidate(column, "enum-collation", sample.lower(), _BOTH)
        elif isinstance(sample, str):
            yield Candidate(column, "enum-unregistered", "BOGUS", _BOTH)
            if sample.lower() != sample:
                yield Candidate(column, "enum-collation", sample.lower(), _BOTH)
            elif sample.upper() != sample:
                yield Candidate(column, "enum-collation", sample.upper(), _BOTH)
        else:
            yield Candidate(
                column,
                "enum-unregistered",
                max(int(v) for v in enums if isinstance(v, int)) + 1,
                _BOTH,
            )


def _sqlite_config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


def _rows_by_table(config: CoreConfig, tables: set[str]) -> dict[str, tuple[Any, ...]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            result: dict[str, tuple[Any, ...]] = {}
            for table in tables:
                rows = connector.fetch_all(f"SELECT * FROM {table} LIMIT 1")
                if rows:
                    result[table] = rows[0]
            return result
    finally:
        connector.close()


def _key_columns(connector: SQLConnector, backend: str, table: str) -> list[str]:
    """The primary-key columns of ``table`` (every manifest relation has one)."""

    if backend == "sqlite":
        rows = connector.fetch_all(f"PRAGMA table_info({table})")
        keyed = sorted((int(row[5]), str(row[1])) for row in rows if int(row[5]) > 0)
        return [name for _position, name in keyed]
    return [
        str(row[0])
        for row in connector.fetch_all(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
            (table,),
        )
    ]


def _column_names(connector: SQLConnector, backend: str, table: str) -> list[str]:
    if backend == "sqlite":
        return [
            str(row[1]) for row in connector.fetch_all(f"PRAGMA table_info({table})")
        ]
    return [
        str(row[0])
        for row in connector.fetch_all(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (table,),
        )
    ]


_REJECTIONS: tuple[type[BaseException], ...] = (
    DatabaseDuplicateKeyError,
    sqlite3.IntegrityError,
    sqlite3.OperationalError,
)


def _key_predicate(
    names: list[str], keys: list[str], row: tuple[Any, ...]
) -> tuple[str, tuple[Any, ...]]:
    """Address the sampled row by its primary key (all columns when none)."""

    selected = keys or names
    pairs = [
        (name, value)
        for name, value in zip(names, row, strict=True)
        if name in selected
    ]
    where = " AND ".join(
        f"{name} IS NULL" if value is None else f"{name} = %s" for name, value in pairs
    )
    return where, tuple(value for _name, value in pairs if value is not None)


def _apply_candidate(
    connector: SQLConnector,
    backend: str,
    candidate: Candidate,
    row: tuple[Any, ...],
    names: list[str],
    keys: list[str],
) -> str:
    """UPDATE the sampled row's column to the candidate; return the outcome."""

    where, bound = _key_predicate(names, keys, row)
    table = candidate.column.table
    connector.begin()
    try:
        try:
            affected = connector.execute_affected(
                f"UPDATE {table} SET {candidate.column.name} = %s WHERE {where}",
                (candidate.value, *bound),
            )
        except _REJECTIONS:
            return "rejected"
        except OverflowError:
            # SQLite cannot even bind an integer outside signed 64 bits; the
            # driver refuses before SQL, which is also fail-closed.
            return "rejected"
        except Exception as error:  # MariaDB driver errors are backend classes
            if backend == "mariadb" and type(error).__module__.startswith("mysql."):
                return "rejected"
            raise
        if affected != 1:
            return "no-op"
        return "accepted"
    finally:
        connector.rollback()


def _run_matrix(
    configs: list[CoreConfig], backend: str
) -> tuple[dict[str, str], set[str]]:
    columns = manifest_columns()
    all_tables = {column.table for column in columns} - {EPOCH_CONTROL_TABLE}
    sampled: dict[str, tuple[Any, ...]] = {}
    sampled_config: dict[str, CoreConfig] = {}
    for config in configs:
        for table, row in _rows_by_table(config, all_tables - set(sampled)).items():
            sampled[table] = row
            sampled_config[table] = config
    outcomes: dict[str, str] = {}
    by_config: dict[int, list[Candidate]] = {}
    for column in columns:
        if column.table not in sampled or column.table == EPOCH_CONTROL_TABLE:
            continue
        for candidate in candidates(column):
            if backend in candidate.backends:
                by_config.setdefault(id(sampled_config[column.table]), []).append(
                    candidate
                )
    for config in configs:
        selected = by_config.get(id(config), [])
        if not selected:
            continue
        connector = open_connector(config)
        try:
            if backend == "sqlite":
                connector.execute("PRAGMA foreign_keys = OFF")
            names_cache: dict[str, tuple[list[str], list[str]]] = {}
            for candidate in selected:
                table = candidate.column.table
                cached = names_cache.get(table)
                if cached is None:
                    with connector.read_transaction():
                        cached = (
                            _column_names(connector, backend, table),
                            _key_columns(connector, backend, table),
                        )
                    names_cache[table] = cached
                names, keys = cached
                before = sampled[table]
                outcome = _apply_candidate(
                    connector, backend, candidate, before, names, keys
                )
                outcomes[candidate.label] = outcome
                # Rollback exactness: the sampled row is unchanged.
                where, bound = _key_predicate(names, keys, before)
                with connector.read_transaction():
                    after = connector.fetch_all(
                        f"SELECT * FROM {table} WHERE {where}", bound
                    )
                assert after == [before], candidate.label
        finally:
            connector.close()
    return outcomes, all_tables - set(sampled)


def _leniency(candidate: Candidate) -> str | None:
    """Name the check the SQLite DDL does not declare for an accepted value.

    SQLite enforces only the CHECK expressions the manifest renders; it has no
    strict storage classes and no unsigned integers.  An acceptance is
    explained exactly when the rendered checks of that column declare no
    constraint of the injected class.  Anything else is a real hole."""

    name = re.escape(candidate.column.name)
    checks = candidate.column.checks
    expression = " AND ".join(checks)
    if candidate.kind.startswith("storage-"):
        if re.search(rf"typeof\({name}\)", expression) is None and not _enum_values(
            expression, candidate.column.name
        ):
            return "no-typeof-check"
        return None
    if candidate.kind in {"range-negative", "range-low"}:
        bounded = [
            check
            for check in checks
            if re.search(rf"\b{name} (>=|>|BETWEEN) ", check) is not None
        ]
        if not bounded:
            return "no-lower-bound"
        if all(" OR " in check for check in bounded):
            # The bound holds only in one disjunct; the sampled row satisfies
            # another (for example a depth-zero self ancestor).
            return "conditional-bound"
        return None
    if candidate.kind == "singleton":
        if any(
            re.search(rf"\b{name} = \d+\b", check) and " OR " in check
            for check in checks
        ):
            return "conditional-equality"
        return None
    return None


# Exact inventory of invalid classes the rendered SQLite DDL does not reject,
# keyed by injected class and the undeclared check that explains it.  Every one
# of these values is refused by the covered writer-binding guards before a
# production writer binds it; this fence pins that storage itself does not.
SQLITE_UNDECLARED_CHECK_INVENTORY: dict[str, int] = {
    "storage-real:no-typeof-check": 112,
    "storage-text:no-typeof-check": 105,
    "storage-integer:no-typeof-check": 34,
    "storage-blob:no-typeof-check": 9,
    "range-negative:no-lower-bound": 18,
    "range-low:conditional-bound": 1,
    "singleton:conditional-equality": 3,
}


def test_sqlite_every_manifest_column_rejects_every_invalid_class_and_rolls_back(
    tmp_path: Path,
) -> None:
    configs = [
        corpus.config
        for corpus in build_corpora(
            tmp_path, lambda name: _sqlite_config(tmp_path / f"{name}.sqlite3")
        )
    ]
    outcomes, unsampled = _run_matrix(configs, "sqlite")
    assert unsampled <= NEVER_AT_REST | NO_PRODUCTION_WRITER, sorted(unsampled)
    by_label = {
        candidate.label: candidate
        for column in manifest_columns()
        for candidate in candidates(column)
    }
    accepted = sorted(
        label for label, outcome in outcomes.items() if outcome != "rejected"
    )
    explained = {label: _leniency(by_label[label]) for label in accepted}
    unexplained = sorted(label for label, reason in explained.items() if reason is None)
    assert unexplained == [], unexplained
    inventory = Counter(
        f"{by_label[label].kind}:{reason}" for label, reason in explained.items()
    )
    assert dict(inventory) == SQLITE_UNDECLARED_CHECK_INVENTORY, dict(inventory)
    assert len(outcomes) - len(accepted) > 4000
    kinds = {label.rsplit(":", 1)[1] for label in outcomes}
    assert {
        "width-short",
        "width-long",
        "storage-text",
        "storage-integer",
        "storage-real",
        "storage-blob",
        "range-low",
        "range-high",
        "enum-unregistered",
        "enum-collation",
    } <= kinds


def test_live_mariadb_every_manifest_column_rejects_every_invalid_class_and_rolls_back(
    mariadb_config: CoreConfig,
    tmp_path: Path,
) -> None:
    """Every manifest column of the populated corpus receives every invalid
    class its own manifest checks declare on live MariaDB 10.11.11; the
    rendered CHECK constraints and strict column types own width, range, enum
    and binary collation, storage-class coercions are owned by the writer
    guards, and the only storage leniency (fixed-width BINARY padding, plus
    the conditional bounds) is pinned as an exact inventory."""

    del tmp_path
    configs = [
        corpus.config
        for corpus in build_corpora(
            Path("."), lambda name: mariadb_config, names={"ready-populated"}
        )
    ]
    outcomes, _unsampled = _run_matrix(configs, "mariadb")
    by_label = {
        candidate.label: candidate
        for column in manifest_columns()
        for candidate in candidates(column)
    }
    accepted = sorted(
        label for label, outcome in outcomes.items() if outcome != "rejected"
    )
    explained = {label: _mariadb_leniency(by_label[label]) for label in accepted}
    unexplained = sorted(label for label, reason in explained.items() if reason is None)
    assert unexplained == [], unexplained
    inventory = Counter(
        f"{by_label[label].kind}:{reason}" for label, reason in explained.items()
    )
    # InnoDB samples a different row per run (clustered by random identities),
    # so the exact counts move by a few columns; the classes are exact.
    assert set(inventory) == MARIADB_UNDECLARED_CHECK_CLASSES, dict(inventory)
    assert 30 <= inventory["width-short:binary-padding"] <= 50, dict(inventory)
    assert len(outcomes) > 500


def _mariadb_leniency(candidate: Candidate) -> str | None:
    """Name the storage behaviour that lets live MariaDB accept a value.

    MariaDB enforces the rendered CHECK constraints and strict column types,
    but a ``BINARY(n)`` column pads a shorter value with zero bytes before any
    check runs, so a short fixed-width identity is stored padded.  Conditional
    bounds and equalities are explained as on SQLite."""

    if (
        candidate.kind == "width-short"
        and candidate.column.mariadb_type.upper().startswith("BINARY(")
    ):
        return "binary-padding"
    if candidate.kind in {"range-negative", "range-low", "singleton"}:
        return _leniency(candidate)
    return None


# The exact classes live MariaDB storage does not reject: fixed-width BINARY
# columns padded from a shorter value (about forty columns, depending on the
# sampled row), and the conditional bounds and equalities that hold only in
# one disjunct.  The writer-binding guards refuse them before a production
# writer binds them.
MARIADB_UNDECLARED_CHECK_CLASSES = frozenset(
    {
        "width-short:binary-padding",
        "range-low:conditional-bound",
        "singleton:conditional-equality",
    }
)


@pytest.mark.parametrize(
    ("guard", "invalid"),
    [
        (require_int63, (-1, INT63_MAX + 1, 1.5, "1", True, None)),
        (require_positive_int63, (0, -1, INT63_MAX + 1, 1.5, True)),
        (require_uint32, (-1, UINT32_MAX + 1, 1.0, False)),
        (require_bool_byte, (2, -1, True, "1")),
        (require_digest32, (b"\x01" * 31, b"\x01" * 33, "a" * 32, 32, None)),
        (require_uuid16, (b"\x01" * 15, b"\x01" * 17, "a" * 16, bytearray(16))),
        (require_text, (b"text", 1, None, 1.0)),
    ],
)
def test_every_writer_boundary_guard_rejects_each_invalid_class(
    guard: Callable[..., object],
    invalid: tuple[object, ...],
) -> None:
    for value in invalid:
        with pytest.raises(DomainValidationError):
            guard(value, field="probe")


def test_bounded_ascii_utf8_and_enum_guards_reject_width_encoding_and_registry() -> (
    None
):
    with pytest.raises(DomainValidationError):
        require_bounded_bytes(b"\x01" * 5, field="p", maximum=4)
    with pytest.raises(DomainValidationError):
        require_bounded_bytes(b"", field="p", minimum=1, maximum=4)
    with pytest.raises(DomainValidationError):
        require_bounded_bytes("text", field="p", maximum=4)
    with pytest.raises(DomainValidationError):
        require_ascii_bytes("\xe9".encode(), field="p", maximum=8)
    with pytest.raises(DomainValidationError):
        require_ascii_bytes(b"a" * 9, field="p", maximum=8)
    with pytest.raises(DomainValidationError):
        require_utf8_bytes(b"\xff", field="p", maximum=8)
    with pytest.raises(DomainValidationError):
        require_utf8_bytes(b"a\x00", field="p", maximum=8, reject_nul=True)
    with pytest.raises(DomainValidationError):
        require_enum_bytes(b"open", field="p", allowed=(b"OPEN", b"SEALED"))
    with pytest.raises(DomainValidationError):
        require_enum_bytes("OPEN", field="p", allowed=(b"OPEN",))
    assert require_enum_bytes(b"OPEN", field="p", allowed=(b"OPEN",)) == b"OPEN"


def test_guard_matrix_covers_every_installed_physical_domain_guard() -> None:
    covered = {
        require_int63,
        require_positive_int63,
        require_uint32,
        require_bool_byte,
        require_digest32,
        require_uuid16,
        require_text,
        require_bounded_bytes,
        require_ascii_bytes,
        require_utf8_bytes,
        require_enum_bytes,
    }
    assert set(CATALOG_PHYSICAL_DOMAIN_GUARDS) <= covered
    assert set(OPERATIONAL_PHYSICAL_DOMAIN_GUARDS) <= covered
