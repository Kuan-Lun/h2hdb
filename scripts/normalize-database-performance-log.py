"""One-time conversion of complete schema-1 database diagnostic JSONL to schema 2.

This is an offline log tool, never a runtime compatibility reader. It preserves
known exact totals and first-admitted fingerprints. Historical `other` identities
and top-five omissions cannot be recovered. It accepts raw JSONL or lines with a
`database_performance ` prefix; HTML exports must first be reconstructed as lines.
The input is never overwritten and the output must not already exist.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError("invalid historical query duration")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("invalid historical query duration")
    return result


def _count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("invalid historical query count")
    return value


def _scope(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    if "query_top" in result:
        original = result.pop("query_top")
        if not isinstance(original, list) or len(original) > 5:
            raise ValueError("invalid historical query top")
        top: list[dict[str, Any]] = []
        keys: set[str] = set()
        for row in original:
            if not isinstance(row, dict):
                raise ValueError("invalid historical query row")
            key = row.get("fingerprint")
            if key == "other":
                continue
            if not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{16}", key) is None:
                raise ValueError("invalid historical query fingerprint")
            if key in keys:
                raise ValueError("duplicate historical query fingerprint")
            keys.add(key)
            seconds = _number(row.get("seconds"))
            maximum = _number(row.get("max_seconds"))
            if maximum > seconds:
                raise ValueError("invalid historical query maximum")
            top.append(
                {
                    "fingerprint": key,
                    "observed_calls": _count(row.get("calls")),
                    "seconds_lower": seconds,
                    "seconds_upper": seconds,
                    "observed_returned_rows": _count(row.get("returned_rows")),
                    "observed_max_seconds": maximum,
                    "complete": True,
                }
            )
        known_seconds = sum(row["seconds_lower"] for row in top)
        total_seconds = _number(record.get("sql_seconds"))
        missing = total_seconds - known_seconds
        # Group totals may differ from event-order addition by rounding alone.
        if missing < 0 and not math.isclose(missing, 0, abs_tol=1e-9):
            raise ValueError("historical query totals exceed scope")
        if sum(row["observed_calls"] for row in top) > _count(record.get("sql_calls")):
            raise ValueError("historical query counts exceed scope")
        if sum(row["observed_returned_rows"] for row in top) > _count(
            record.get("read_rows")
        ):
            raise ValueError("historical query rows exceed scope")
        overflow = result.pop("query_overflow", None)
        if not isinstance(overflow, dict):
            raise ValueError("historical overflow evidence is missing")
        overflow_seconds = _number(overflow.get("seconds"))
        overflow_calls = _count(overflow.get("calls"))
        overflow_rows = _count(overflow.get("returned_rows"))
        if (
            overflow_seconds > max(0.0, missing) + 1e-9
            or overflow_calls + sum(row["observed_calls"] for row in top)
            > _count(record.get("sql_calls"))
            or overflow_rows + sum(row["observed_returned_rows"] for row in top)
            > _count(record.get("read_rows"))
        ):
            raise ValueError("historical overflow exceeds unidentified scope totals")
        result["query_attribution"] = {
            "algorithm": "historical-first64-import-v1",
            "capacity": 64,
            "retained_families": len(top),
            "replacements": None,
            "missing_key_seconds_upper": max(0.0, missing),
            "retained_seconds_lower": known_seconds,
            "top": top,
            "historical_overflow": overflow,
            "unrecoverable_identity_seconds": max(0.0, missing),
            "conversion_note": "Only displayed exact families survive; other and undisplayed identities are unavailable.",
        }
    if "phase_top" in result:
        phases = result["phase_top"]
        if not isinstance(phases, list) or any(
            not isinstance(item, dict) for item in phases
        ):
            raise ValueError("invalid historical phases")
        result["phase_top"] = [_scope(item) for item in phases]
    return result


def normalize(record: dict[str, Any]) -> dict[str, Any]:
    """Reject other versions rather than provide a permanent dual-format path."""
    if record.get("schema") != 1 or type(record.get("schema")) is not int:
        raise ValueError("conversion requires a schema-1 database performance record")
    if "query_top" not in record and not (
        record.get("event") == "started"
        and not {
            "sql_calls",
            "sql_seconds",
            "read_rows",
            "query_overflow",
        }.intersection(record)
    ):
        raise ValueError("historical query attribution is missing")
    result = _scope(record)
    result.pop("query_top_scope", None)
    result["schema"] = 2
    result["converted_from_schema"] = 1
    result["query_attribution_scope"] = "inclusive_scope"
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    options = parser.parse_args(argv)
    try:
        with (
            options.input.open(encoding="utf-8") as source,
            options.output.open("x", encoding="utf-8") as destination,
        ):
            for line in source:
                if not line.strip():
                    continue
                payload = (
                    line.partition("database_performance ")[2]
                    if not line.lstrip().startswith("{")
                    and "database_performance " in line
                    else line
                )
                record = json.loads(payload)
                if not isinstance(record, dict):
                    raise ValueError("historical record must be an object")
                destination.write(json.dumps(normalize(record), allow_nan=False) + "\n")
    except (OSError, ValueError, OverflowError) as error:
        parser.exit(
            2, f"Conversion failed ({type(error).__name__}); output may be partial.\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
