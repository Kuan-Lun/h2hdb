"""Assess measured source work against a constructed static catch-up reference.

This development-only consumer accepts counters from the Ingest backlog probe,
not production database credentials. Inputs describe unchanged, accepted source
galleries with qualification enabled. The producer must independently validate
its input facts and raw measurements; this module checks their accounting, not
filesystem content or instrumentation correctness.

The reference admits up to B new galleries each round, visits N inventory rows,
reads newly observed PAGE bytes once, and qualifies each new page once. Those
are engineering targets, not a proven achievable optimum of the runtime. No
wall-clock, physical-I/O or SQL-query-plan bound is inferred. History depth and
SQL calls stay visible, without silently earning a larger PAGE work allowance.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

_MAX_ROUNDS = 100_000
_MAX_INPUT_BYTES = 16 * 1024 * 1024
_LIMITS = [
    "Static accepted input; no source mutation, rejection, policy change or retry.",
    "Counts are logical work, never physical disk traffic or NAS elapsed time.",
    "One PAGE pass is an engineering reference, not a proved runtime optimum.",
    "SQL calls and history depth are diagnostic; no SQL engine-work bound is proved.",
    "Inventory is charged every round: K*N is not an O(N) whole-catch-up claim.",
    "The input producer owns content-oracle and independent-meter validation.",
]


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _object(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _check(name: str, observed: int, reference: int) -> dict[str, Any]:
    return {
        "name": name,
        "observed": observed,
        "reference": reference,
        "excess": max(0, observed - reference),
        "amplification": observed / reference if reference else None,
        "status": "satisfied" if observed <= reference else "violated",
    }


def project_catchup(
    *, inventory: int, batch: int, initial_retained: int, pages_per_gallery: int
) -> dict[str, Any]:
    """Project the static admission trace, not measured runtime or future bytes.

    Every pre-final transition admits B galleries; the last admits the remainder.
    Summing their starting retained counts gives K*R + B*K*(K-1)/2, while
    selected counts add exactly N-R admissions. Tests compare this closed form
    with independently constructed transitions, including short last batches.
    """
    _integer(inventory, "inventory", minimum=1)
    _integer(batch, "batch", minimum=1)
    _integer(initial_retained, "initial_retained")
    _integer(pages_per_gallery, "pages_per_gallery", minimum=1)
    if initial_retained >= inventory:
        raise ValueError("initial retained must leave catch-up work")
    new = inventory - initial_retained
    rounds = (new + batch - 1) // batch
    retained_visits = rounds * initial_retained + batch * rounds * (rounds - 1) // 2
    return {
        "kind": "engineering_reference_projection_not_measurement",
        "rounds": rounds,
        "new_galleries": new,
        "inventory_rows_if_one_pass_per_round": rounds * inventory,
        "qualification_decodes_if_once_per_new_page": new * pages_per_gallery,
        "retained_members_if_reattached_each_round": retained_visits,
        "selected_members_if_reattached_each_round": retained_visits + new,
        "future_page_bytes": None,
        "wall_seconds": None,
        "limits": "Fixed inventory and policy; every round admits min(B, remaining). "
        "Reattachment counters are conditional work, not an unavoidable lower bound. "
        "Future encoded bytes, SQL work and device time are not inferred.",
    }


def _round(
    item: Mapping[str, Any],
    *,
    number: int,
    inventory: int,
    batch: int,
    retained: int,
    pages: int,
) -> dict[str, Any]:
    """Construct the next admission transition before inspecting its costs."""
    expected_new = min(batch, inventory - retained)
    fields = (
        "new_admitted",
        "retained_before",
        "new_page_bytes",
        "inventory_rows",
        "page_read_bytes",
        "retained_page_read_bytes",
        "decode_calls",
        "source_sql_calls",
    )
    values = {name: _integer(item.get(name), name) for name in fields}
    history = item.get("history_depth")
    if history is not None:
        history = _integer(history, "history_depth")
    if not expected_new:
        raise ValueError("rounds continue after catch-up completed")
    if values["new_admitted"] != expected_new or values["retained_before"] != retained:
        raise ValueError("admission/retained sequence differs from static input")
    new_pages = expected_new * pages
    if values["new_page_bytes"] < new_pages:
        raise ValueError("new PAGE byte oracle must cover every nonempty page")
    if (
        values["inventory_rows"] < inventory
        or values["page_read_bytes"] < values["new_page_bytes"]
        or values["decode_calls"] < new_pages
        or values["retained_page_read_bytes"] > values["page_read_bytes"]
    ):
        raise ValueError("measured work omits required input or contradicts totals")
    checks = [
        _check("inventory_rows", values["inventory_rows"], inventory),
        _check("page_read_bytes", values["page_read_bytes"], values["new_page_bytes"]),
        _check("retained_page_read_bytes", values["retained_page_read_bytes"], 0),
        _check("decode_calls", values["decode_calls"], new_pages),
    ]
    return {
        "number": number,
        "retained_before": retained,
        "new_admitted": expected_new,
        "selected_after": retained + expected_new,
        "remaining_after": inventory - retained - expected_new,
        "history_depth": history,
        "history_depth_scope": "unknown" if history is None else "producer_measured",
        "source_sql_calls_unbounded": values["source_sql_calls"],
        "checks": checks,
        "status": "violated"
        if any(c["status"] == "violated" for c in checks)
        else "satisfied",
    }


def assess_catchup(
    report: Mapping[str, Any],
    *,
    require_complete: bool = False,
) -> dict[str, Any]:
    """Return satisfied/violated/incomplete; never fill missing evidence with zero.

    A satisfied measured prefix does not establish complete catch-up or an
    arbitrary-size bound. Reference totals are folded from actual admission
    transitions, including a short final batch, rather than assuming K*B work.
    """
    result: dict[str, Any] = {
        "schema_version": 1,
        "scope": "static_source_catchup_logical_work",
        "status": "incomplete",
        "limits": list(_LIMITS),
    }
    try:
        if (
            type(report.get("schema_version")) is not int
            or report["schema_version"] != 1
        ):
            raise ValueError("unsupported source catch-up ledger schema")
        manifest = report.get("input_manifest_sha256")
        if manifest is not None:
            if (
                not isinstance(manifest, str)
                or re.fullmatch(r"[0-9a-f]{64}", manifest) is None
            ):
                raise ValueError("input_manifest_sha256 must be a SHA-256 hex digest")
            result["input_manifest_sha256"] = manifest
        dimensions = _object(report.get("dimensions"), "dimensions")
        inventory = _integer(dimensions.get("inventory"), "inventory", minimum=1)
        batch = _integer(dimensions.get("batch"), "batch", minimum=1)
        retained = _integer(dimensions.get("initial_retained"), "initial_retained")
        pages = _integer(
            dimensions.get("pages_per_gallery"), "pages_per_gallery", minimum=1
        )
        if retained >= inventory:
            raise ValueError("initial retained must leave catch-up work")
        raw_rounds = report.get("rounds")
        if not isinstance(raw_rounds, list) or not 1 <= len(raw_rounds) <= _MAX_ROUNDS:
            raise ValueError("rounds must contain 1..100000 measured transitions")
        rows = []
        for number, raw in enumerate(raw_rounds, start=1):
            row = _round(
                _object(raw, "round"),
                number=number,
                inventory=inventory,
                batch=batch,
                retained=retained,
                pages=pages,
            )
            rows.append(row)
            retained = row["selected_after"]
        totals = []
        for position in range(4):
            checks = [row["checks"][position] for row in rows]
            totals.append(
                _check(
                    checks[0]["name"],
                    sum(check["observed"] for check in checks),
                    sum(check["reference"] for check in checks),
                )
            )
        result.update(
            {
                "status": "violated"
                if any(row["status"] == "violated" for row in rows)
                else "satisfied",
                "dimensions": dict(dimensions),
                "coverage": "complete_catchup"
                if retained == inventory
                else "measured_prefix",
                "measured_rounds": len(rows),
                "remaining_galleries": inventory - retained,
                "newly_admitted": sum(row["new_admitted"] for row in rows),
                "revisited_retained_members_if_reattached": sum(
                    row["retained_before"] for row in rows
                ),
                "selected_members_if_reattached": sum(
                    row["selected_after"] for row in rows
                ),
                "source_sql_calls_unbounded": sum(
                    row["source_sql_calls_unbounded"] for row in rows
                ),
                "engineering_projection": project_catchup(
                    inventory=inventory,
                    batch=batch,
                    initial_retained=dimensions["initial_retained"],
                    pages_per_gallery=pages,
                ),
                "rounds": rows,
                "aggregate_checks": totals,
            }
        )
        result["cost_status"] = result["status"]
        if require_complete and retained != inventory:
            result["status"] = "incomplete"
            result["reason"] = (
                "full catch-up required but only a measured prefix is present"
            )
    except (ValueError, OverflowError) as error:
        result["reason"] = str(error)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        parser.error("--output must be a new file")
    try:
        with args.input.open("rb") as source:
            data = source.read(_MAX_INPUT_BYTES + 1)
        if len(data) > _MAX_INPUT_BYTES:
            raise ValueError("source ledger exceeds 16 MiB")
        result = assess_catchup(
            _object(json.loads(data), "report"),
            require_complete=args.require_complete,
        )
        result["input_sha256"] = sha256(data).hexdigest()
        result["model_sha256"] = sha256(Path(__file__).read_bytes()).hexdigest()
    except (OSError, ValueError) as error:
        result = {"schema_version": 1, "status": "incomplete", "reason": str(error)}
    try:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps(result, indent=2) + "\n")
    except OSError as error:
        print(json.dumps({"status": "incomplete", "reason": str(error)}))
        return 2
    print(json.dumps({"status": result["status"], "output": str(args.output)}))
    return {"satisfied": 0, "violated": 1, "incomplete": 2}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
