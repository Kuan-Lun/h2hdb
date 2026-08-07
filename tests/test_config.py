import json
from pathlib import Path

import pytest

from h2hdb import (
    EnvironmentPlaceholderError,
    load_config,
    resolve_environment_placeholders,
)


def test_environment_placeholders_resolve_recursively_without_inline_expansion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("H2HDB_TEST_HOST", "database.internal")
    monkeypatch.setenv("H2HDB_TEST_PASSWORD", "database-secret")
    document = {
        "database": {
            "host": "${H2HDB_TEST_HOST}",
            "password": "${H2HDB_TEST_PASSWORD}",
        },
        "ordinary": ["prefix-${H2HDB_TEST_HOST}", "${UNFINISHED"],
    }

    resolved = resolve_environment_placeholders(document)

    assert resolved == {
        "database": {
            "host": "database.internal",
            "password": "database-secret",
        },
        "ordinary": ["prefix-${H2HDB_TEST_HOST}", "${UNFINISHED"],
    }
    assert document == {
        "database": {
            "host": "${H2HDB_TEST_HOST}",
            "password": "${H2HDB_TEST_PASSWORD}",
        },
        "ordinary": ["prefix-${H2HDB_TEST_HOST}", "${UNFINISHED"],
    }


def test_core_loader_resolves_nested_database_environment_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("H2HDB_TEST_DATABASE", str(tmp_path / "catalog.sqlite3"))
    config_path = tmp_path / "core.json"
    config_path.write_text(
        json.dumps(
            {
                "database": {
                    "sql_type": "sqlite",
                    "database": "${H2HDB_TEST_DATABASE}",
                }
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.database.database == str(tmp_path / "catalog.sqlite3")


def test_missing_environment_variable_fails_with_its_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("H2HDB_TEST_MISSING", raising=False)

    with pytest.raises(
        EnvironmentPlaceholderError,
        match="H2HDB_TEST_MISSING.*not set",
    ):
        resolve_environment_placeholders("${H2HDB_TEST_MISSING}")


def test_invalid_environment_variable_name_does_not_expose_its_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "must-not-appear-in-error"
    monkeypatch.setenv("H2HDB-INVALID", secret)

    with pytest.raises(EnvironmentPlaceholderError) as captured:
        resolve_environment_placeholders("${H2HDB-INVALID}")

    assert "H2HDB-INVALID" in str(captured.value)
    assert secret not in str(captured.value)
