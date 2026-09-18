"""Historical differential and engine-work control for private cleanup probes.

Only the synthetic growth probe calls this module, inside its existing read cut.
The historical predicate is a test artifact, never a production fallback.
"""

from __future__ import annotations

import math
from hashlib import sha256
from pathlib import Path
from typing import Any

from h2hdb import vnext_cleanup_repository as cleanup
from h2hdb.mariadb_connector import MariaDBConnector

BASELINE_COMMIT = "fe32ba7186daf52561fd183ae3ac70298ebbd07b"
BASELINE_SHA256 = "be2e121bd70af0432654970505fde8c6da24ca54803ff216d5db7da2debb0129"
REPETITIONS = 3
_ROOT = "catalog_canonical_value_allocation_anchors"


def historical_query(query: str) -> str:
    """Replace exactly the predicate captured from the real runtime call."""
    path = (
        Path(__file__).resolve().parents[1]
        / "tests/fixtures/canonical_eligibility_0_39_9.sql"
    )
    payload = path.read_bytes()
    if sha256(payload).hexdigest() != BASELINE_SHA256:
        raise RuntimeError("historical cleanup control fingerprint changed")
    predicate = cleanup._CANONICAL_VALUE_ELIGIBILITY
    if _ROOT not in query or query.count(predicate) != 1:
        raise ValueError("comparison requires one captured canonical predicate")
    return query.replace(predicate, payload.decode("utf-8"))


def cache_visits(nodes: list[dict[str, Any]]) -> float:
    """Count table row visits, retaining loops rather than just result rows."""
    total = 0.0
    cache_nodes = 0
    for node in nodes:
        if node["table_name"] not in {"choice", "title_sort"}:
            continue
        cache_nodes += 1
        if "index_condition" in node or "rowid_filter" in node:
            raise RuntimeError("filtered cache access needs an expanded cost model")
        loops = node.get("r_loops")
        rows = node.get("r_rows")
        if (
            isinstance(loops, bool)
            or not isinstance(loops, int | float)
            or not math.isfinite(loops)
            or loops < 0
        ):
            raise RuntimeError("executed title-cache node lacks valid runtime counts")
        if loops == 0:
            continue
        if (
            isinstance(rows, bool)
            or not isinstance(rows, int | float)
            or not math.isfinite(rows)
            or rows < 0
        ):
            raise RuntimeError("executed title-cache node lacks valid runtime counts")
        total += loops * rows
    if not cache_nodes:
        raise RuntimeError("canonical plan omitted every expected title-cache node")
    return total


def title_cache_budget(cardinalities: dict[str, int]) -> int:
    """Linear target for this unique-title, one-policy pipeline fixture.

    There are two hash edges into each cache and a matching live-reference check.
    Allow four visits per allocation/choice/sort row, including one-time cache
    materialization. This is a finite fixture target, not a bound for arbitrary
    title multiplicities, policy counts or every SQL plan.
    """
    required = {"allocations", "display_choices", "title_sorts"}
    if cardinalities.keys() != required or any(
        type(value) is not int or value < 0 for value in cardinalities.values()
    ):
        raise ValueError("canonical cardinalities must be complete nonnegative counts")
    return 4 * sum(cardinalities.values())


def compare_eligibility(
    connector: MariaDBConnector, query: str, parameters: tuple[Any, ...]
) -> dict[str, Any]:
    # Imported at call time because the existing diagnostic owns this opt-in
    # entry point and is already initialized when its canonical query arrives.
    from ingest_maintenance_mariadb import (
        HANDLER_STATUS,
        counter_delta,
        counters,
        decode_plan,
        plan_table_nodes,
        validate_candidate_select,
    )

    validate_candidate_select(query)
    if not query.endswith(" LIMIT 1"):
        raise ValueError("canonical candidate must retain its one-row bound")
    variants = {"production": query, "historical_0_39_9": historical_query(query)}
    if variants["production"] == variants["historical_0_39_9"]:
        raise RuntimeError("canonical optimization is absent from the measured SQL")
    cardinalities = {
        name: int(connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
        for name, table in (
            ("allocations", _ROOT),
            ("display_choices", "catalog_display_title_choices"),
            ("title_sorts", "catalog_title_sorts"),
        )
    }
    bound = title_cache_budget(cardinalities)
    eligible = {
        name: connector.fetch_all(sql.removesuffix(" LIMIT 1"), parameters)
        for name, sql in variants.items()
    }
    if eligible["production"] != eligible["historical_0_39_9"]:
        raise RuntimeError("canonical eligibility changed relative to historical SQL")
    expected = eligible["production"][:1]
    records: dict[str, Any] = {
        name: {"sql": sql, "handler_repetitions": []} for name, sql in variants.items()
    }
    # Alternate order so one variant does not always get the warmer execution.
    for repetition in range(REPETITIONS):
        order = tuple(variants) if repetition % 2 == 0 else tuple(reversed(variants))
        for name in order:
            first = counters(connector, HANDLER_STATUS)
            before = counters(connector, HANDLER_STATUS)
            if any(counter_delta(first, before).values()):
                raise RuntimeError("SHOW STATUS disturbed canonical handler counts")
            rows = connector.fetch_all(variants[name], parameters)
            after = counters(connector, HANDLER_STATUS)
            if rows != expected:
                raise RuntimeError("canonical replay changed ordered candidate result")
            delta = counter_delta(before, after)
            records[name]["handler_repetitions"].append(
                {"delta": delta, "access_requests": sum(delta.values())}
            )
    for name, sql in variants.items():
        analyzed_raw = connector.fetch_one("ANALYZE FORMAT=JSON " + sql, parameters)
        analyzed = decode_plan(analyzed_raw)
        nodes = plan_table_nodes(analyzed)
        visits = cache_visits(nodes)
        records[name].update(
            analyze_json=analyzed,
            analyze_raw=analyzed_raw[0],
            table_nodes=nodes,
            title_cache_row_visits=visits,
            within_title_cache_budget=visits <= bound,
        )
    return {
        "baseline_commit": BASELINE_COMMIT,
        "baseline_predicate_sha256": BASELINE_SHA256,
        "ordered_full_eligible_sets_equal": True,
        "eligible_rows": len(eligible["production"]),
        "cardinalities": cardinalities,
        "title_cache_visit_budget": bound,
        "variants": records,
        "production_target_met": records["production"]["within_title_cache_budget"],
        "historical_negative_control_rejected": not records["historical_0_39_9"][
            "within_title_cache_budget"
        ],
        "units": "Handler engine access requests; ANALYZE r_loops*r_rows table row visits, not unique examined rows or physical disk reads.",
        "scope": "Same transaction, synthetic unique-title single-policy pipeline, after cleanup DONE; three alternating SELECT repetitions. Full-set oracle and ANALYZE are outside earlier facade baseline timings. No wall-time gate, arbitrary-distribution or NAS speedup claim.",
    }
