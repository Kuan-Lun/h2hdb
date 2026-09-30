"""Catch-up work accounting separates engineering targets from completion."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def model() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/source_catchup_cost_model.py"
    spec = importlib.util.spec_from_file_location("catchup_model_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ledger() -> dict[str, Any]:
    # Distinct independent input byte sizes prevent K*B or uniform-byte shortcuts.
    return {
        "schema_version": 1,
        "input_manifest_sha256": "b" * 64,
        "dimensions": {
            "inventory": 25,
            "initial_retained": 5,
            "batch": 8,
            "pages_per_gallery": 3,
        },
        "rounds": [
            {
                "new_admitted": new,
                "retained_before": retained,
                "new_page_bytes": size,
                "inventory_rows": 25,
                "page_read_bytes": size,
                "retained_page_read_bytes": 0,
                "decode_calls": new * 3,
                "source_sql_calls": sql,
                "history_depth": history,
            }
            for new, retained, size, sql, history in (
                (8, 5, 2400, 999, 3),
                (8, 13, 2560, 1199, None),
                (4, 21, 1300, 699, 1),
            )
        ],
    }


def test_constructed_short_final_batch_and_retained_revisits(model: ModuleType) -> None:
    result = model.assess_catchup(_ledger())
    assert result["status"] == "satisfied"
    assert result["coverage"] == "complete_catchup"
    assert result["remaining_galleries"] == 0
    assert result["newly_admitted"] == 20
    assert result["selected_members_if_reattached"] == 59
    assert result["revisited_retained_members_if_reattached"] == 39
    assert result["source_sql_calls_unbounded"] == 2897
    assert {
        item["name"]: (item["observed"], item["reference"])
        for item in result["aggregate_checks"]
    } == {
        "inventory_rows": (75, 75),
        "page_read_bytes": (6260, 6260),
        "retained_page_read_bytes": (0, 0),
        "decode_calls": (60, 60),
    }
    assert result["rounds"][1]["history_depth"] is None
    assert result["rounds"][1]["history_depth_scope"] == "unknown"


@pytest.mark.parametrize("inventory", [1, 2, 127, 128, 129])
@pytest.mark.parametrize("batch", [1, 2, 128])
def test_projection_equals_constructed_admission_events(
    model: ModuleType,
    inventory: int,
    batch: int,
) -> None:
    for initial in {0, inventory - 1}:
        retained = list(range(initial))
        pending = list(range(initial, inventory))
        events: list[tuple[str, int | tuple[int, int]]] = []
        while pending:
            admitted, pending = pending[:batch], pending[batch:]
            events.extend(("inventory", key) for key in range(inventory))
            events.extend(("reattach_old", key) for key in retained)
            retained.extend(admitted)
            events.extend(("attach_selected", key) for key in retained)
            events.extend(
                ("decode", (key, page)) for key in admitted for page in range(3)
            )
        counts = {
            name: sum(event == name for event, _ in events)
            for name in (
                "inventory",
                "reattach_old",
                "attach_selected",
                "decode",
            )
        }
        projection = model.project_catchup(
            inventory=inventory,
            batch=batch,
            initial_retained=initial,
            pages_per_gallery=3,
        )
        assert projection["inventory_rows_if_one_pass_per_round"] == counts["inventory"]
        assert (
            projection["retained_members_if_reattached_each_round"]
            == counts["reattach_old"]
        )
        assert (
            projection["selected_members_if_reattached_each_round"]
            == counts["attach_selected"]
        )
        assert (
            projection["qualification_decodes_if_once_per_new_page"] == counts["decode"]
        )


def test_full_inventory_projection_is_not_a_wall_time_or_byte_forecast(
    model: ModuleType,
) -> None:
    projection = model.project_catchup(
        inventory=132105,
        batch=10000,
        initial_retained=24900,
        pages_per_gallery=1,
    )
    assert projection["rounds"] == 11
    assert projection["inventory_rows_if_one_pass_per_round"] == 1_453_155
    assert projection["selected_members_if_reattached_each_round"] == 931_105
    assert projection["future_page_bytes"] is None
    assert projection["wall_seconds"] is None


@pytest.mark.parametrize("violation", [False, True])
def test_partial_measurement_is_not_whole_catchup(
    model: ModuleType, violation: bool
) -> None:
    report = _ledger()
    report["rounds"] = report["rounds"][:1]
    if violation:
        report["rounds"][0]["page_read_bytes"] += 1
    result = model.assess_catchup(report)
    assert result["coverage"] == "measured_prefix"
    assert result["status"] == ("violated" if violation else "satisfied")
    assert result["remaining_galleries"] == 12
    required = model.assess_catchup(report, require_complete=True)
    assert required["status"] == "incomplete"
    assert required["cost_status"] == result["status"]


@pytest.mark.parametrize("retained,history", [(1, None), (128, 17), (24900, 2)])
def test_page_allowance_does_not_grow_with_retained_history(
    model: ModuleType,
    retained: int,
    history: int | None,
) -> None:
    report = _ledger()
    report["dimensions"].update(initial_retained=retained, inventory=retained + 8)
    report["rounds"] = report["rounds"][:1]
    report["rounds"][0].update(
        retained_before=retained,
        history_depth=history,
        inventory_rows=retained + 8,
        page_read_bytes=2401,
        source_sql_calls=10_000_000,
    )
    result = model.assess_catchup(report)
    assert result["status"] == "violated"
    page = result["aggregate_checks"][1]
    assert (page["reference"], page["excess"]) == (2400, 1)
    # SQL is deliberately exposed as unbounded, not a fabricated cost proof.
    assert result["source_sql_calls_unbounded"] == 10_000_000


@pytest.mark.parametrize(
    "mutant", ["none", "extra_new_read", "old_read", "inventory", "decode"]
)
def test_independent_file_read_trace_rejects_extra_work(
    model: ModuleType,
    tmp_path: Path,
    mutant: str,
) -> None:
    # Measure real logical byte reads independently of the cost model. This is
    # a meter/model negative control, not a replacement for the Ingest probe's
    # production adapter and content-oracle correspondence evidence.
    old = tmp_path / "retained.page"
    new = tmp_path / "new.page"
    old.write_bytes(b"old-page")
    new.write_bytes(b"new-page-data")
    events = []

    def read(path: Path) -> None:
        with path.open("rb") as source:
            content = source.read()
        events.append((path, len(content)))

    read(new)
    if mutant == "extra_new_read":
        read(new)
    if mutant == "old_read":
        read(old)
    entries = list(tmp_path.iterdir())
    if mutant == "inventory":
        entries += list(tmp_path.iterdir())
    report = {
        "schema_version": 1,
        "dimensions": {
            "inventory": 2,
            "initial_retained": 1,
            "batch": 1,
            "pages_per_gallery": 1,
        },
        "rounds": [
            {
                "new_admitted": 1,
                "retained_before": 1,
                "history_depth": None,
                "new_page_bytes": new.stat().st_size,
                "inventory_rows": len(entries),
                "page_read_bytes": sum(size for _, size in events),
                "retained_page_read_bytes": sum(
                    size for path, size in events if path == old
                ),
                "decode_calls": 2 if mutant == "decode" else 1,
                "source_sql_calls": 0,
            }
        ],
    }
    result = model.assess_catchup(report)
    assert result["status"] == ("satisfied" if mutant == "none" else "violated")
    if mutant == "old_read":
        assert result["aggregate_checks"][2]["excess"] == 8
        assert result["aggregate_checks"][2]["amplification"] is None


@pytest.mark.parametrize(
    "field",
    [
        "inventory_rows",
        "page_read_bytes",
        "retained_page_read_bytes",
        "decode_calls",
        "source_sql_calls",
        "new_page_bytes",
    ],
)
@pytest.mark.parametrize("invalid", [None, True, -1, 1.5])
def test_missing_or_invalid_measurements_are_not_zero(
    model: ModuleType,
    field: str,
    invalid: object,
) -> None:
    report = _ledger()
    report["rounds"][0][field] = invalid
    assert model.assess_catchup(report)["status"] == "incomplete"


@pytest.mark.parametrize(
    "mutation",
    [
        "underread",
        "missing_inventory",
        "missing_decode",
        "wrong_retained",
        "wrong_admitted",
        "round_after_completion",
        "invalid_manifest",
        "bool_history",
    ],
)
def test_inconsistent_facts_fail_closed(model: ModuleType, mutation: str) -> None:
    report = _ledger()
    first = report["rounds"][0]
    match mutation:
        case "underread":
            first["page_read_bytes"] = 2399
        case "missing_inventory":
            first["inventory_rows"] = 24
        case "missing_decode":
            first["decode_calls"] = 23
        case "wrong_retained":
            first["retained_before"] = 4
        case "wrong_admitted":
            first["new_admitted"] = 7
        case "round_after_completion":
            report["rounds"].append(deepcopy(first))
        case "invalid_manifest":
            report["input_manifest_sha256"] = "incomplete"
        case "bool_history":
            first["history_depth"] = False
    assert model.assess_catchup(report)["status"] == "incomplete"


def test_cli_consumes_ledger_with_exact_input_hash_and_exit_codes(
    tmp_path: Path,
) -> None:
    script = (
        Path(__file__).resolve().parents[1] / "scripts/source_catchup_cost_model.py"
    )
    source = tmp_path / "ledger.json"
    report = _ledger()
    report["rounds"] = report["rounds"][:1]
    report["rounds"][0]["page_read_bytes"] *= 2
    source.write_text(json.dumps(report))
    for required, code in [(False, 1), (True, 2)]:
        output = tmp_path / f"assessment-{required}.json"
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--input",
                str(source),
                "--output",
                str(output),
            ]
            + (["--require-complete"] if required else []),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        assert result.returncode == code, result.stderr
        assessment = json.loads(output.read_text())
        assert assessment["cost_status"] == "violated"
        assert len(assessment["input_sha256"]) == 64
        assert len(assessment["model_sha256"]) == 64
