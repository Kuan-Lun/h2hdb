from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tomllib
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "verification" / "schema" / "measure_capacity_mariadb.py"
RECEIPT = ROOT / "verification" / "schema" / "generate_capacity_measurement_receipt.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


benchmark = _load_module("measure_capacity_mariadb", BENCHMARK)
receipt = _load_module("generate_capacity_measurement_receipt", RECEIPT)


def _plan() -> dict[str, Any]:
    with receipt.CATALOG_PATH.open("rb") as stream:
        document = tomllib.load(stream)
    return cast(dict[str, Any], document["capacity_plan"])


def _measurement() -> dict[str, Any]:
    plan = _plan()
    shapes = receipt._physical_shapes()
    storage = {
        "engine": benchmark.EXPECTED_ENGINE,
        "row_format": benchmark.EXPECTED_ROW_FORMAT,
        "table_collation": benchmark.EXPECTED_TABLE_COLLATION,
    }
    staging_common = {
        "relation": plan["staging_measurement_relation"],
        "row_generator": benchmark.STAGING_ROW_GENERATOR,
        "key_distribution": benchmark.STAGING_KEY_DISTRIBUTION,
        "physical_shape_sha256": shapes["staging_request"],
        **storage,
    }
    return {
        "measurement_version": 1,
        "execution_mode": benchmark.EXECUTION_MODE,
        "mariadb_version": "10.11.11-MariaDB-capacity-test",
        "mariadb_image": benchmark.MARIADB_IMAGE,
        "innodb_page_size": benchmark.EXPECTED_INNODB_PAGE_SIZE,
        "benchmark_script_sha256": hashlib.sha256(
            receipt.BENCHMARK_PATH.read_bytes()
        ).hexdigest(),
        "seed": benchmark.MEASUREMENT_SEED,
        "insert_batch_size": benchmark.INSERT_BATCH_SIZE,
        "insertion_order": benchmark.INSERTION_ORDER,
        "registry": {
            "relation": plan["registry_measurement_relation"],
            "row_generator": benchmark.REGISTRY_ROW_GENERATOR,
            "key_distribution": benchmark.REGISTRY_KEY_DISTRIBUTION,
            "physical_shape_sha256": shapes["registry"],
            "row_count": plan["registry_measurement_row_count"],
            "actual_rows": plan["registry_measurement_row_count"],
            "information_schema_estimated_rows": plan["registry_measurement_row_count"],
            "data_bytes": 65_000_000,
            "index_bytes": 31_223_232,
            "total_bytes": 96_223_232,
            **storage,
        },
        "source_scope": {
            "relation": plan["source_scope_measurement_relation"],
            "row_generator": benchmark.SOURCE_SCOPE_ROW_GENERATOR,
            "key_distribution": benchmark.SOURCE_SCOPE_KEY_DISTRIBUTION,
            "physical_shape_sha256": shapes["source_scope"],
            "row_count": plan["source_scope_measurement_row_count"],
            "actual_rows": plan["source_scope_measurement_row_count"],
            "information_schema_estimated_rows": plan[
                "source_scope_measurement_row_count"
            ],
            "data_bytes": 55_000_000,
            "index_bytes": 95_000_000,
            "total_bytes": 150_000_000,
            **storage,
        },
        "staging_churn_full": {
            **staging_common,
            "row_count": plan["staging_measurement_accepted_rows"],
            "actual_rows": plan["staging_measurement_accepted_rows"],
            "information_schema_estimated_rows": plan[
                "staging_measurement_accepted_rows"
            ],
            "data_bytes": 156_237_824,
            "index_bytes": 145_752_064,
            "total_bytes": 301_989_888,
        },
        "staging_churn_empty": {
            **staging_common,
            "inserted_rows": plan["staging_measurement_accepted_rows"],
            "deleted_rows": plan["staging_measurement_accepted_rows"],
            "insert_commit_count": (
                plan["staging_measurement_accepted_rows"] // benchmark.INSERT_BATCH_SIZE
            ),
            "delete_commit_count": (
                plan["staging_measurement_accepted_rows"] // benchmark.INSERT_BATCH_SIZE
            ),
            "residual_live_rows": 0,
            "actual_rows": 0,
            "information_schema_estimated_rows": 0,
            "data_bytes": 16_777_216,
            "index_bytes": 16_777_216,
            "total_bytes": 33_554_432,
        },
        "staging_accepted": {
            **staging_common,
            "staging_id_count": plan["staging_measurement_distinct_staging_ids"],
            "requests_per_staging_id": plan[
                "staging_measurement_synthetic_rows_per_staging_id"
            ],
            "row_count": plan["staging_measurement_accepted_rows"],
            "actual_rows": plan["staging_measurement_accepted_rows"],
            "information_schema_estimated_rows": plan[
                "staging_measurement_accepted_rows"
            ],
            "data_bytes": 156_237_824,
            "index_bytes": 145_752_064,
            "total_bytes": 301_989_888,
        },
        "staging_over_capacity_diagnostic": {
            **staging_common,
            "staging_id_count": plan["staging_measurement_distinct_staging_ids"],
            "requests_per_staging_id": plan[
                "staging_over_capacity_diagnostic_rows_per_staging_id"
            ],
            "row_count": plan["staging_over_capacity_diagnostic_rows"],
            "actual_rows": plan["staging_over_capacity_diagnostic_rows"],
            "information_schema_estimated_rows": plan[
                "staging_over_capacity_diagnostic_rows"
            ],
            "data_bytes": 186_646_528,
            "index_bytes": 170_917_888,
            "total_bytes": 357_564_416,
        },
    }


def _write_measurement(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, sort_keys=True))


def test_manual_capacity_benchmark_matches_exact_physical_storage_shapes() -> None:
    benchmark._assert_measured_shapes()


def test_staging_churn_and_refill_use_independent_complete_key_domains() -> None:
    plan = _plan()
    assert plan["staging_measurement_row_generator"] == benchmark.STAGING_ROW_GENERATOR
    assert (
        plan["staging_measurement_key_distribution"]
        == benchmark.STAGING_KEY_DISTRIBUTION
    )
    assert plan["staging_measurement_insertion_order"] == benchmark.INSERTION_ORDER
    churn = next(
        benchmark._staging_rows(
            staging_id_count=1,
            request_indexes=range(1),
            request_domain=b"staging-request-churn",
            staging_id_domain=b"staging-id-churn",
        )
    )
    churn_reconstructed_for_delete = next(
        benchmark._staging_rows(
            staging_id_count=1,
            request_indexes=range(1),
            request_domain=b"staging-request-churn",
            staging_id_domain=b"staging-id-churn",
        )
    )
    refill = next(
        benchmark._staging_rows(
            staging_id_count=1,
            request_indexes=range(1),
            request_domain=b"staging-request-accepted",
            staging_id_domain=b"staging-id-accepted",
        )
    )
    assert churn == churn_reconstructed_for_delete
    assert churn[0] != refill[0]
    assert churn[1] != refill[1]


def test_capacity_receipt_binds_shapes_manifests_and_safety_peak() -> None:
    measurement = _measurement()
    rendered = tomllib.loads(receipt.render_receipt(measurement, input_sha256="a" * 64))
    assert (
        rendered["benchmark_script_sha256"]
        == hashlib.sha256(receipt.BENCHMARK_PATH.read_bytes()).hexdigest()
    )
    assert rendered["innodb_page_size"] == benchmark.EXPECTED_INNODB_PAGE_SIZE
    assert rendered["benchmark_seed"] == benchmark.MEASUREMENT_SEED
    assert rendered["benchmark_insert_batch_size"] == benchmark.INSERT_BATCH_SIZE
    assert rendered["benchmark_insertion_order"] == benchmark.INSERTION_ORDER
    assert (
        rendered["catalog_physical_manifest_sha256"]
        == hashlib.sha256(receipt.PHYSICAL_PATH.read_bytes()).hexdigest()
    )
    assert (
        rendered["operational_physical_manifest_sha256"]
        == hashlib.sha256(receipt.OPERATIONAL_PHYSICAL_PATH.read_bytes()).hexdigest()
    )
    staging = rendered["staging_accepted_measurement"]
    assert staging["total_bytes"] == 301_989_888
    assert staging["safety_peak_bytes"] == 377_487_360
    assert staging["headroom_bytes"] == 22_512_640
    assert rendered["staging_over_capacity_diagnostic"]["accepted"] is False
    assert rendered["acceptance"] == {
        "largest_relation": "gallery_observation_staging_request",
        "largest_safety_peak_bytes": 377_487_360,
        "headroom_bytes": 22_512_640,
        "all_affected_relations_below_soft_limit": True,
    }


def test_staging_peak_uses_larger_full_fill_or_refill_measurement() -> None:
    plan = _plan()
    value = {
        "registry": {"total_bytes": 1},
        "source_scope": {"total_bytes": 1},
        "staging_churn_full": {"total_bytes": 280_000_000},
        "staging_accepted": {"total_bytes": 260_000_000},
    }
    assert (
        receipt._measurement_peaks(value, plan)["gallery_observation_staging_request"]
        == 350_000_000
    )

    value["staging_churn_full"]["total_bytes"] = 240_000_000
    value["staging_accepted"]["total_bytes"] = 290_000_000
    assert (
        receipt._measurement_peaks(value, plan)["gallery_observation_staging_request"]
        == 362_500_000
    )


def test_registry_measurement_relation_dominates_closed_bounded_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measured, measured_score, scored = receipt._registry_capacity_dominance(_plan())
    assert measured == "analysis_policy"
    assert len(scored) == 8
    assert all(score <= measured_score for _name, score in scored)

    original_load = receipt._load
    widened = deepcopy(original_load(receipt.PHYSICAL_PATH))
    relation = next(
        item for item in widened["relation"] if item["name"] == "manifest_policy"
    )
    relation["column"].append(
        {
            "attribute": "unmeasured_payload",
            "mariadb": {
                "type": "VARBINARY(4096)",
                "nullable": False,
                "collation": "NONE",
            },
        }
    )

    def load_with_widening(path: Path) -> dict[str, Any]:
        return (
            widened
            if path == receipt.PHYSICAL_PATH
            else cast(dict[str, Any], original_load(path))
        )

    monkeypatch.setattr(receipt, "_load", load_with_widening)
    with pytest.raises(RuntimeError, match="unmeasured bounded registry exceeds"):
        receipt._registry_capacity_dominance(_plan())


@pytest.mark.parametrize(
    ("relation_name", "widened_bytes"),
    (
        ("cleanup_job", 8_000),
        ("cleanup_cycle_root", 1_024),
        ("cleanup_checkpoint", 20_000),
    ),
)
def test_bounded_cleanup_protocol_width_guard_rejects_each_widening(
    monkeypatch: pytest.MonkeyPatch,
    relation_name: str,
    widened_bytes: int,
) -> None:
    scores = receipt._bounded_protocol_width_guard(_plan())
    assert {name for name, _score, _accounted in scores} == {
        "cleanup_job",
        "cleanup_cycle_root",
        "cleanup_checkpoint",
    }
    assert all(score <= accounted for _name, score, accounted in scores)

    original_load = receipt._load
    widened = deepcopy(original_load(receipt.OPERATIONAL_PHYSICAL_PATH))
    relation = next(
        item for item in widened["relation"] if item["name"] == relation_name
    )
    relation["column"].append(
        {
            "attribute": "unaccounted_protocol_payload",
            "mariadb": {
                "type": f"VARBINARY({widened_bytes})",
                "nullable": True,
                "collation": "NONE",
            },
        }
    )

    def load_with_widening(path: Path) -> dict[str, Any]:
        if path == receipt.OPERATIONAL_PHYSICAL_PATH:
            return cast(dict[str, Any], widened)
        return cast(dict[str, Any], original_load(path))

    monkeypatch.setattr(receipt, "_load", load_with_widening)
    with pytest.raises(RuntimeError, match="bounded cleanup protocol relation"):
        receipt._bounded_protocol_width_guard(_plan())


def test_capacity_receipt_generation_requires_exact_benchmark_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurement_path = tmp_path / "measurement.json"
    receipt_path = tmp_path / "capacity_measurement.toml"
    raw_measurement_path = tmp_path / "capacity_measurement.json"
    _write_measurement(measurement_path, _measurement())
    monkeypatch.setattr(receipt, "RECEIPT_PATH", receipt_path)
    monkeypatch.setattr(receipt, "RAW_MEASUREMENT_PATH", raw_measurement_path)

    assert receipt.main(["--measurement", str(measurement_path)]) == 0
    assert receipt.main(["--check"]) == 0
    assert tomllib.loads(receipt_path.read_text())["receipt_version"] == 2
    assert json.loads(raw_measurement_path.read_text()) == _measurement()
    assert raw_measurement_path.read_bytes() == measurement_path.read_bytes()

    raw_measurement_path.write_bytes(raw_measurement_path.read_bytes() + b"\n")
    with pytest.raises(RuntimeError, match="does not exactly match"):
        receipt.main(["--check"])


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda value: value.__setitem__("seed", 0),
            "capacity measurement.seed must be",
        ),
        (
            lambda value: value.__setitem__("benchmark_script_sha256", "0" * 64),
            "capacity measurement.benchmark_script_sha256 must be",
        ),
        (
            lambda value: value["staging_accepted"].__setitem__(
                "physical_shape_sha256", "0" * 64
            ),
            "staging_accepted.physical_shape_sha256 must be",
        ),
        (
            lambda value: value["staging_accepted"].__setitem__("total_bytes", 1),
            "staging_accepted DATA plus index bytes",
        ),
    ),
)
def test_capacity_receipt_rejects_unbound_measurement(
    tmp_path: Path,
    mutate: Any,
    message: str,
) -> None:
    value = _measurement()
    mutate(value)
    path = tmp_path / "invalid.json"
    _write_measurement(path, value)
    with pytest.raises(RuntimeError, match=message):
        receipt.validate_measurement(path)


def test_capacity_receipt_applies_fragmentation_factor_to_acceptance(
    tmp_path: Path,
) -> None:
    value = _measurement()
    value["staging_accepted"].update(
        data_bytes=200_000_000,
        index_bytes=120_000_001,
        total_bytes=320_000_001,
    )
    value["staging_over_capacity_diagnostic"].update(
        data_bytes=210_000_000,
        index_bytes=120_000_001,
        total_bytes=330_000_001,
    )
    path = tmp_path / "over-soft-limit.json"
    _write_measurement(path, value)
    with pytest.raises(RuntimeError, match="decimal 400 MB soft limit"):
        receipt.validate_measurement(path)
