"""Historical log conversion preserves evidence without recovering lost identity."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest


def _tool() -> Any:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts/normalize-database-performance-log.py"
    )
    spec = importlib.util.spec_from_file_location("historical_log_conversion", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _record() -> dict[str, Any]:
    return {
        "schema": 1,
        "sql_calls": 120,
        "sql_seconds": 12.0,
        "read_rows": 240,
        "query_top_scope": "first_64_fingerprints_plus_other",
        "query_top": [
            {
                "fingerprint": "a" * 16,
                "calls": 20,
                "seconds": 2.0,
                "max_seconds": 0.1,
                "returned_rows": 40,
            },
            {
                "fingerprint": "other",
                "calls": 60,
                "seconds": 6.0,
                "max_seconds": 0.1,
                "returned_rows": 120,
            },
        ],
        "query_overflow": {"calls": 60, "seconds": 6.0, "returned_rows": 120},
    }


def test_conversion_preserves_nested_totals_and_unknown_identity() -> None:
    record = _record()
    record["phase_top"] = [_record()]
    converted = _tool().normalize(record)
    assert converted["schema"] == 2
    assert converted["sql_calls"] == 120
    assert converted["sql_seconds"] == 12.0
    assert converted["read_rows"] == 240
    for scope in (converted, converted["phase_top"][0]):
        summary = scope["query_attribution"]
        assert summary["top"][0]["complete"] is True
        assert summary["top"][0]["observed_calls"] == 20
        assert (
            summary["top"][0]["seconds_lower"]
            == summary["top"][0]["seconds_upper"]
            == 2
        )
        assert summary["missing_key_seconds_upper"] == 10
        assert summary["unrecoverable_identity_seconds"] == 10
        assert summary["historical_overflow"] == record["query_overflow"]
        assert "query_top" not in scope
    assert record["schema"] == 1 and "query_top" in record


@pytest.mark.parametrize(
    "kind",
    ["version", "duration", "calls", "duplicate", "fingerprint", "overflow", "missing"],
)
def test_invalid_input_does_not_invent_evidence(kind: str) -> None:
    record = _record()
    if kind == "version":
        record["schema"] = 2
    elif kind == "duration":
        record["sql_seconds"] = 1
    elif kind == "calls":
        record["sql_calls"] = 1
    elif kind == "duplicate":
        record["query_top"].append(record["query_top"][0])
    elif kind == "fingerprint":
        record["query_top"][0]["fingerprint"] = "private sql"
    elif kind == "overflow":
        record["query_overflow"]["seconds"] = 99
    else:
        del record["query_top"]
    with pytest.raises(ValueError):
        _tool().normalize(record)


def test_cli_never_overwrites_input_or_existing_output(tmp_path: Path) -> None:
    tool = _tool()
    source, output = tmp_path / "old.jsonl", tmp_path / "new.jsonl"
    original = "INFO database_performance " + json.dumps(_record()) + "\n"
    source.write_text(original)
    assert tool.main([str(source), str(output)]) == 0
    assert json.loads(output.read_text())["schema"] == 2
    with pytest.raises(SystemExit) as stopped:
        tool.main([str(source), str(output)])
    assert stopped.value.code == 2
    assert source.read_text() == original


def test_real_started_envelope_has_no_completed_measurements() -> None:
    record = {
        "backend": "mariadb",
        "event": "started",
        "labels": {"audit": "ready"},
        "operation": "schema_check",
        "operation_id": "e" * 32,
        "query_fingerprint_algorithm": "sha256-in-placeholder-list-v1",
        "query_slowest_scope": "five_slowest_completed_calls",
        "query_top_scope": "first_64_fingerprints_plus_other",
        "read_rows_unit": "returned_rows_not_examined_rows",
        "schema": 1,
        "sequence": 1,
        "sql_calls_unit": "completed_connector_method_calls",
    }
    converted = _tool().normalize(record)
    assert converted["schema"] == 2 and converted["event"] == "started"
    assert "query_attribution" not in converted
    assert "sql_calls" not in converted
    assert "query_top_scope" not in converted
    record["sql_calls"] = 1
    with pytest.raises(ValueError):
        _tool().normalize(record)


def test_raw_json_label_cannot_be_mistaken_for_log_prefix(tmp_path: Path) -> None:
    source, output = tmp_path / "raw.jsonl", tmp_path / "converted.jsonl"
    record = _record()
    record["labels"] = {"text": "database_performance embedded in JSON"}
    source.write_text("  " + json.dumps(record) + "\n")
    assert _tool().main([str(source), str(output)]) == 0
    converted = json.loads(output.read_text())
    assert converted["labels"] == record["labels"]
    assert converted["sql_calls"] == 120
