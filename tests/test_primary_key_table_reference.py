"""Dialect-specific primary-key references remain pure through SQL observers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast

import pytest

from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector


@pytest.fixture(params=("sqlite", "mariadb"))
def connector(request: pytest.FixtureRequest, tmp_path: Path) -> SQLConnector:
    if request.param == "sqlite":
        return SQLiteConnector(str(tmp_path / "not-opened.sqlite3"))
    # Construction does not connect; these contracts never start a live service.
    return MariaDBConnector(
        host="unused.invalid",
        port=3306,
        user="acceptance",
        password="unused",
        database="acceptance",
    )


@dataclass
class _Recorder:
    calls: list[tuple[str, float, str, int]] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        elapsed: float,
        query: str,
        rows: int,
    ) -> None:
        self.calls.append((category, elapsed, query, rows))


@pytest.mark.parametrize(
    "relation",
    ("catalog_a_file_decision_shadow_anchors", "widgets", "_keyset_128", "Widget2"),
)
def test_primary_reference_preserves_identifier_and_backend_access_path(
    connector: SQLConnector, relation: str
) -> None:
    assert SQLConnector.primary_key_table_reference(connector, relation) == relation
    expected = relation + (
        " FORCE INDEX (PRIMARY)" if isinstance(connector, MariaDBConnector) else ""
    )
    assert connector.primary_key_table_reference(relation) == expected


@pytest.mark.parametrize(
    "relation",
    (
        "",
        "123widgets",
        "schema.widgets",
        "widgets AS row",
        "widgets FORCE INDEX (PRIMARY)",
        "widgets; DROP TABLE widgets",
        "(SELECT * FROM widgets)",
        "`widgets`",
        "widgets--comment",
        "widgets\n",
        "gallery資料",
        "ｗidgets",
        None,
        True,
        b"widgets",
    ),
)
def test_primary_reference_rejects_non_ascii_identifier_or_sql_expression(
    connector: SQLConnector, relation: object
) -> None:
    recorder = _Recorder()
    with measure_sql(recorder):
        wrapped = instrument_connector(connector)
        assert wrapped is not connector
        for reference in (connector, wrapped):
            with pytest.raises(ValueError, match="one SQL identifier"):
                reference.primary_key_table_reference(cast(str, relation))
    assert recorder.calls == []


def test_measured_reference_preserves_dialect_without_recording_database_work(
    connector: SQLConnector,
) -> None:
    relation = "catalog_a_file_decision_shadow_anchors"
    expected = connector.primary_key_table_reference(relation)
    recorder = _Recorder()
    assert instrument_connector(connector) is connector
    with measure_sql(recorder):
        wrapped = instrument_connector(connector)
        assert wrapped is not connector
        assert instrument_connector(wrapped) is wrapped
        assert wrapped.primary_key_table_reference(relation) == expected
    # Retaining a decorated connector does not erase its dialect outside a scope.
    assert wrapped.primary_key_table_reference(relation) == expected
    assert recorder.calls == []


def test_sqlite_primary_reference_executes_composite_keyset_query(
    tmp_path: Path,
) -> None:
    with SQLiteConnector(str(tmp_path / "keyset.sqlite3")) as connector:
        connector.execute(
            "CREATE TABLE keyset_rows (owner INTEGER, position INTEGER, "
            "PRIMARY KEY (owner, position))"
        )
        connector.execute_many(
            "INSERT INTO keyset_rows VALUES (%s, %s)",
            [(1, 1), (1, 2), (1, 3), (2, 1)],
        )
        recorder = _Recorder()
        with measure_sql(recorder):
            wrapped = instrument_connector(connector)
            relation = wrapped.primary_key_table_reference("keyset_rows")
            assert recorder.calls == []
            rows = wrapped.fetch_all(
                f"SELECT position FROM {relation} "
                "WHERE owner = %s AND position > %s ORDER BY position LIMIT %s",
                (1, 1, 1),
            )
        assert rows == [(2,)]
        assert len(recorder.calls) == 1
        assert recorder.calls[0][0] == "sql"
        assert recorder.calls[0][3] == 1


@pytest.mark.parametrize("width", (16, 32))
def test_binary_parameter_has_explicit_backend_width_without_observer_work(
    connector: SQLConnector, width: int
) -> None:
    expected = (
        f"CAST(%s AS BINARY({width}))"
        if isinstance(connector, MariaDBConnector)
        else "%s"
    )
    recorder = _Recorder()
    with measure_sql(recorder):
        wrapped = instrument_connector(connector)
        assert SQLConnector.binary_parameter_expression(connector, width) == "%s"
        assert connector.binary_parameter_expression(width) == expected
        assert wrapped.binary_parameter_expression(width) == expected
    assert recorder.calls == []


@pytest.mark.parametrize("width", (0, 1, 15, 17, 31, 33, True, 16.0, "16", None))
def test_binary_parameter_rejects_unrecognized_or_non_integer_width(
    connector: SQLConnector, width: object
) -> None:
    recorder = _Recorder()
    with measure_sql(recorder):
        for reference in (connector, instrument_connector(connector)):
            with pytest.raises(ValueError):
                reference.binary_parameter_expression(cast(int, width))
    assert recorder.calls == []
