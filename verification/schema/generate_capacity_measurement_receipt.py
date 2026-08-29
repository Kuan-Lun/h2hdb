"""Generate and validate the hash-bound MariaDB capacity receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

if __package__:
    from .measure_capacity_mariadb import (
        ACCEPTED_REQUESTS_PER_STAGING,
        EXECUTION_MODE,
        EXPECTED_ENGINE,
        EXPECTED_INNODB_PAGE_SIZE,
        EXPECTED_ROW_FORMAT,
        EXPECTED_TABLE_COLLATION,
        INSERT_BATCH_SIZE,
        INSERTION_ORDER,
        MARIADB_IMAGE,
        MEASUREMENT_SEED,
        OVER_CAPACITY_REQUESTS_PER_STAGING,
        REGISTRY_KEY_DISTRIBUTION,
        REGISTRY_ROW_GENERATOR,
        SOURCE_SCOPE_KEY_DISTRIBUTION,
        SOURCE_SCOPE_ROW_COUNT,
        SOURCE_SCOPE_ROW_GENERATOR,
        STAGING_ID_COUNT,
        STAGING_KEY_DISTRIBUTION,
        STAGING_ROW_GENERATOR,
    )
else:
    from measure_capacity_mariadb import (
        ACCEPTED_REQUESTS_PER_STAGING,
        EXECUTION_MODE,
        EXPECTED_ENGINE,
        EXPECTED_INNODB_PAGE_SIZE,
        EXPECTED_ROW_FORMAT,
        EXPECTED_TABLE_COLLATION,
        INSERT_BATCH_SIZE,
        INSERTION_ORDER,
        MARIADB_IMAGE,
        MEASUREMENT_SEED,
        OVER_CAPACITY_REQUESTS_PER_STAGING,
        REGISTRY_KEY_DISTRIBUTION,
        REGISTRY_ROW_GENERATOR,
        SOURCE_SCOPE_KEY_DISTRIBUTION,
        SOURCE_SCOPE_ROW_COUNT,
        SOURCE_SCOPE_ROW_GENERATOR,
        STAGING_ID_COUNT,
        STAGING_KEY_DISTRIBUTION,
        STAGING_ROW_GENERATOR,
    )

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT / "verification" / "schema"
CATALOG_PATH = SCHEMA_ROOT / "catalog.toml"
PHYSICAL_PATH = SCHEMA_ROOT / "physical.toml"
OPERATIONAL_PHYSICAL_PATH = SCHEMA_ROOT / "operational_physical.toml"
BENCHMARK_PATH = SCHEMA_ROOT / "measure_capacity_mariadb.py"
GENERATOR_PATH = Path(__file__)
RECEIPT_PATH = SCHEMA_ROOT / "capacity_measurement.toml"
RAW_MEASUREMENT_PATH = SCHEMA_ROOT / "capacity_measurement.json"

_STORAGE_BYTE_FIELDS = ("data_bytes", "index_bytes", "total_bytes")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _relation(document: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    relations = document.get("relation")
    if not isinstance(relations, list):
        raise RuntimeError(f"{name}: physical manifest has no relation array")
    matches = [item for item in relations if item.get("name") == name]
    if len(matches) != 1 or not isinstance(matches[0], Mapping):
        raise RuntimeError(f"{name}: expected one physical relation")
    return matches[0]


def _storage_shape(relation: Mapping[str, Any]) -> dict[str, object]:
    columns = relation.get("column")
    indexes = relation.get("required_index", [])
    if not isinstance(columns, list) or not isinstance(indexes, list):
        raise RuntimeError("physical relation columns or indexes are malformed")
    return {
        "primary_key": relation.get("primary_key"),
        "unique_keys": relation.get("unique_keys", []),
        "referential_unique_keys": relation.get("referential_unique_keys", []),
        "columns": [
            {
                "attribute": column.get("attribute"),
                "mariadb": column.get("mariadb"),
            }
            for column in columns
        ],
        "required_indexes": [
            {
                "attributes": index.get("attributes"),
                "unique": index.get("unique"),
            }
            for index in indexes
        ],
    }


def _shape_sha256(relation: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _storage_shape(relation),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return _sha256_bytes(payload)


def _maximum_mariadb_column_bytes(column: Mapping[str, Any]) -> int:
    mariadb = column.get("mariadb")
    if not isinstance(mariadb, Mapping):
        raise RuntimeError("capacity dominance column has no MariaDB domain")
    sql_type = mariadb.get("type")
    if not isinstance(sql_type, str):
        raise RuntimeError("capacity dominance MariaDB type is missing")
    fixed_widths = {
        "TINYINT": 1,
        "TINYINT UNSIGNED": 1,
        "SMALLINT": 2,
        "SMALLINT UNSIGNED": 2,
        "INT": 4,
        "INT UNSIGNED": 4,
        "BIGINT": 8,
        "BIGINT UNSIGNED": 8,
    }
    if sql_type in fixed_widths:
        return fixed_widths[sql_type]
    binary = re.fullmatch(r"(BINARY|VARBINARY)\(([1-9][0-9]*)\)", sql_type)
    if binary is not None:
        width = int(binary.group(2))
        if binary.group(1) == "VARBINARY":
            width += 1 if width <= 255 else 2
        return width
    text = re.fullmatch(r"(CHAR|VARCHAR)\(([1-9][0-9]*)\)", sql_type)
    if text is not None:
        collation = mariadb.get("collation")
        if not isinstance(collation, str):
            raise RuntimeError("capacity dominance text collation is missing")
        # The current generated domains use ASCII or utf8mb4. Unknown future
        # text collations are deliberately charged at four bytes per character
        # instead of silently assuming a narrower encoding.
        bytes_per_character = 1 if collation.lower().startswith("ascii") else 4
        payload_width = int(text.group(2)) * bytes_per_character
        if text.group(1) == "VARCHAR":
            payload_width += 1 if payload_width <= 255 else 2
        return payload_width
    raise RuntimeError(f"capacity dominance does not support MariaDB type {sql_type!r}")


def _secondary_index_keys(relation: Mapping[str, Any]) -> set[tuple[str, ...]]:
    secondary_keys: set[tuple[str, ...]] = set()
    for field in ("unique_keys", "referential_unique_keys"):
        raw_keys = relation.get(field, [])
        if not isinstance(raw_keys, list):
            raise RuntimeError(f"capacity dominance {field} is malformed")
        secondary_keys.update(tuple(str(item) for item in key) for key in raw_keys)
    raw_indexes = relation.get("required_index", [])
    if not isinstance(raw_indexes, list):
        raise RuntimeError("capacity dominance required indexes are malformed")
    for index in raw_indexes:
        attributes = index.get("attributes")
        if not isinstance(attributes, list):
            raise RuntimeError("capacity dominance index attributes are malformed")
        secondary_keys.add(tuple(str(item) for item in attributes))
    return secondary_keys


def _registry_width_score(relation: Mapping[str, Any]) -> int:
    """Return a conservative monotone row-plus-index width score."""

    columns = relation.get("column")
    primary_key = relation.get("primary_key")
    if not isinstance(columns, list) or not isinstance(primary_key, list):
        raise RuntimeError("capacity dominance relation shape is malformed")
    width_by_attribute = {
        str(column.get("attribute")): _maximum_mariadb_column_bytes(column)
        for column in columns
    }
    if set(primary_key) - width_by_attribute.keys():
        raise RuntimeError("capacity dominance primary key names an unknown column")
    # Sixty-four bytes for a clustered-record envelope and 32 per secondary
    # record deliberately dominate ordinary InnoDB record bookkeeping.  The
    # score is comparative, not an allocation estimate; the widest score is
    # the relation measured empirically at the common 50,000-row ceiling.
    score = 64 + sum(width_by_attribute.values())
    secondary_keys = _secondary_index_keys(relation)
    primary = tuple(str(item) for item in primary_key)
    secondary_keys.discard(primary)
    for key in secondary_keys:
        missing = (set(key) | set(primary)) - width_by_attribute.keys()
        if missing:
            raise RuntimeError(
                f"capacity dominance index names unknown columns: {sorted(missing)!r}"
            )
        stored_attributes = tuple(dict.fromkeys((*key, *primary)))
        score += 32 + sum(width_by_attribute[name] for name in stored_attributes)
    return score


def _conservative_record_and_index_bytes(relation: Mapping[str, Any]) -> int:
    """Bound maximum encoded records, independently of page high-water state.

    The 256-byte clustered and 128-byte secondary envelopes are intentionally
    much larger than ordinary InnoDB record headers. Column maxima include
    variable-length prefixes, and NULL bitmap bytes are charged explicitly.
    Page fragmentation, MVCC history, and an unbounded ``.ibd`` high-water mark
    are outside this soft-cap record-width guard and remain covered by the
    retained-row protocol plus the empirical staging measurements.
    """

    columns = relation.get("column")
    primary_key = relation.get("primary_key")
    if not isinstance(columns, list) or not isinstance(primary_key, list):
        raise RuntimeError("capacity protocol relation shape is malformed")
    width_by_attribute: dict[str, int] = {}
    nullable_attributes: set[str] = set()
    for column in columns:
        attribute = str(column.get("attribute"))
        width_by_attribute[attribute] = _maximum_mariadb_column_bytes(column)
        mariadb = column.get("mariadb")
        if not isinstance(mariadb, Mapping):
            raise RuntimeError("capacity protocol column has no MariaDB domain")
        nullable = mariadb.get("nullable")
        if not isinstance(nullable, bool):
            raise RuntimeError("capacity protocol column nullable flag is missing")
        if nullable:
            nullable_attributes.add(attribute)
    primary = tuple(str(item) for item in primary_key)
    if set(primary) - width_by_attribute.keys():
        raise RuntimeError("capacity protocol primary key names an unknown column")
    clustered_null_bitmap = (len(nullable_attributes) + 7) // 8
    total = 256 + clustered_null_bitmap + sum(width_by_attribute.values())
    secondary_keys = _secondary_index_keys(relation)
    secondary_keys.discard(primary)
    for key in secondary_keys:
        missing = (set(key) | set(primary)) - width_by_attribute.keys()
        if missing:
            raise RuntimeError(
                f"capacity protocol index names unknown columns: {sorted(missing)!r}"
            )
        stored_attributes = tuple(dict.fromkeys((*key, *primary)))
        secondary_null_bitmap = (
            len(set(stored_attributes) & nullable_attributes) + 7
        ) // 8
        total += (
            128
            + secondary_null_bitmap
            + sum(width_by_attribute[name] for name in stored_attributes)
        )
    return total


def _bounded_protocol_width_guard(
    plan: Mapping[str, Any],
) -> tuple[tuple[str, int, int], ...]:
    operational = _load(OPERATIONAL_PHYSICAL_PATH)
    specifications = (
        (
            "cleanup_job",
            _require_int(
                plan,
                "cleanup_job_accounted_bytes_per_row",
                "capacity_plan",
            ),
        ),
        (
            "cleanup_cycle_root",
            _require_int(
                plan,
                "cleanup_cycle_root_accounted_bytes_per_row",
                "capacity_plan",
            ),
        ),
        (
            "cleanup_checkpoint",
            _require_int(
                plan,
                "cleanup_checkpoint_accounted_bytes_per_row",
                "capacity_plan",
            ),
        ),
    )
    scored = tuple(
        (
            name,
            _conservative_record_and_index_bytes(_relation(operational, name)),
            accounted,
        )
        for name, accounted in specifications
    )
    failures = {
        name: {"width_score": score, "accounted_bytes_per_row": accounted}
        for name, score, accounted in scored
        if score > accounted
    }
    if failures:
        raise RuntimeError(
            "a bounded cleanup protocol relation exceeds its conservative "
            f"per-row physical-width account: {failures!r}"
        )
    return scored


def _registry_capacity_dominance(
    plan: Mapping[str, Any],
) -> tuple[str, int, tuple[tuple[str, int], ...]]:
    physical = _load(PHYSICAL_PATH)
    relation_names = plan.get("bounded_registry_relations")
    if not isinstance(relation_names, list) or not relation_names:
        raise RuntimeError("capacity_plan bounded_registry_relations is malformed")
    measured = _require_str(
        plan,
        "registry_measurement_relation",
        "capacity_plan",
    )
    scored = tuple(
        sorted(
            (
                str(name),
                _registry_width_score(_relation(physical, str(name))),
            )
            for name in relation_names
        )
    )
    score_by_name = dict(scored)
    if measured not in score_by_name:
        raise RuntimeError("measured registry is outside the bounded registry set")
    measured_score = score_by_name[measured]
    wider = {name: score for name, score in scored if score > measured_score}
    if wider:
        raise RuntimeError(
            "an unmeasured bounded registry exceeds the measured relation's "
            f"conservative physical width/index score: {wider!r}"
        )
    return measured, measured_score, scored


def _quoted(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _require_int(value: Mapping[str, Any], name: str, context: str) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int):
        raise RuntimeError(f"{context}.{name} must be an integer")
    return item


def _require_str(value: Mapping[str, Any], name: str, context: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise RuntimeError(f"{context}.{name} must be a nonempty string")
    return item


def _require_bool(value: Mapping[str, Any], name: str, context: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise RuntimeError(f"{context}.{name} must be a boolean")
    return item


def _section(value: Mapping[str, Any], name: str, context: str) -> Mapping[str, Any]:
    section = value.get(name)
    if not isinstance(section, Mapping):
        raise RuntimeError(f"{context}.{name} section is missing")
    return section


def _plan() -> Mapping[str, Any]:
    plan = _load(CATALOG_PATH).get("capacity_plan")
    if not isinstance(plan, Mapping):
        raise RuntimeError("catalog.toml must declare capacity_plan")
    return plan


def _canonical_plan_sha256(plan: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(plan),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return _sha256_bytes(payload)


def _ceil_ratio(value: int, numerator: int, denominator: int) -> int:
    if value < 0 or numerator <= 0 or denominator <= 0:
        raise RuntimeError("capacity safety ratio inputs must be positive")
    return (value * numerator + denominator - 1) // denominator


def _validate_exact(
    section: Mapping[str, Any],
    expected: Mapping[str, object],
    context: str,
) -> None:
    for name, wanted in expected.items():
        if section.get(name) != wanted:
            raise RuntimeError(
                f"{context}.{name} must be {wanted!r}, got {section.get(name)!r}"
            )


def _validate_storage_bytes(section: Mapping[str, Any], context: str) -> int:
    measured: dict[str, int] = {}
    for name in _STORAGE_BYTE_FIELDS:
        item = _require_int(section, name, context)
        if item < 0:
            raise RuntimeError(f"{context}.{name} must be nonnegative")
        measured[name] = item
    if measured["data_bytes"] + measured["index_bytes"] != measured["total_bytes"]:
        raise RuntimeError(f"{context} DATA plus index bytes must equal total bytes")
    estimated = _require_int(
        section,
        "information_schema_estimated_rows",
        context,
    )
    if estimated < 0:
        raise RuntimeError(
            f"{context}.information_schema_estimated_rows must be nonnegative"
        )
    return measured["total_bytes"]


def _physical_shapes() -> dict[str, str]:
    physical = _load(PHYSICAL_PATH)
    operational = _load(OPERATIONAL_PHYSICAL_PATH)
    return {
        "registry": _shape_sha256(_relation(physical, "artifact_producer_fingerprint")),
        "source_scope": _shape_sha256(_relation(physical, "source_scope")),
        "staging_request": _shape_sha256(
            _relation(operational, "gallery_observation_staging_request")
        ),
        "staging_root": _shape_sha256(
            _relation(operational, "gallery_observation_staging")
        ),
        "staging_budget": _shape_sha256(
            _relation(
                operational,
                "gallery_observation_staging_request_budget",
            )
        ),
        "cleanup_job": _shape_sha256(_relation(operational, "cleanup_job")),
        "cleanup_root": _shape_sha256(_relation(operational, "cleanup_cycle_root")),
        "cleanup_checkpoint": _shape_sha256(
            _relation(operational, "cleanup_checkpoint")
        ),
    }


def validate_measurement(path: Path) -> Mapping[str, Any]:
    """Require benchmark JSON to match the current plan, script, and shapes."""

    value = json.loads(path.read_text())
    if not isinstance(value, Mapping):
        raise RuntimeError("capacity measurement input must be a JSON object")
    plan = _plan()
    _registry_capacity_dominance(plan)
    _bounded_protocol_width_guard(plan)
    shapes = _physical_shapes()
    expected_top: Mapping[str, object] = {
        "measurement_version": 1,
        "execution_mode": EXECUTION_MODE,
        "mariadb_image": MARIADB_IMAGE,
        "innodb_page_size": EXPECTED_INNODB_PAGE_SIZE,
        "benchmark_script_sha256": _sha256_bytes(BENCHMARK_PATH.read_bytes()),
        "seed": MEASUREMENT_SEED,
        "insert_batch_size": INSERT_BATCH_SIZE,
        "insertion_order": INSERTION_ORDER,
    }
    _validate_exact(value, expected_top, "capacity measurement")
    version = _require_str(value, "mariadb_version", "capacity measurement")
    if not version.startswith(
        _require_str(plan, "mariadb_measurement_version", "capacity_plan")
    ):
        raise RuntimeError("capacity measurement used the wrong MariaDB version")

    registry = _section(value, "registry", "capacity measurement")
    registry_rows = _require_int(
        plan,
        "registry_measurement_row_count",
        "capacity_plan",
    )
    _validate_exact(
        registry,
        {
            "relation": _require_str(
                plan,
                "registry_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": REGISTRY_ROW_GENERATOR,
            "key_distribution": REGISTRY_KEY_DISTRIBUTION,
            "physical_shape_sha256": shapes["registry"],
            "row_count": registry_rows,
            "actual_rows": registry_rows,
            "engine": EXPECTED_ENGINE,
            "row_format": EXPECTED_ROW_FORMAT,
            "table_collation": EXPECTED_TABLE_COLLATION,
        },
        "capacity measurement.registry",
    )
    _validate_storage_bytes(registry, "capacity measurement.registry")

    source_scope = _section(value, "source_scope", "capacity measurement")
    _validate_exact(
        source_scope,
        {
            "relation": _require_str(
                plan,
                "source_scope_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": SOURCE_SCOPE_ROW_GENERATOR,
            "key_distribution": SOURCE_SCOPE_KEY_DISTRIBUTION,
            "physical_shape_sha256": shapes["source_scope"],
            "row_count": SOURCE_SCOPE_ROW_COUNT,
            "actual_rows": SOURCE_SCOPE_ROW_COUNT,
            "engine": EXPECTED_ENGINE,
            "row_format": EXPECTED_ROW_FORMAT,
            "table_collation": EXPECTED_TABLE_COLLATION,
        },
        "capacity measurement.source_scope",
    )
    _validate_storage_bytes(source_scope, "capacity measurement.source_scope")

    staging_common: Mapping[str, object] = {
        "relation": _require_str(
            plan,
            "staging_measurement_relation",
            "capacity_plan",
        ),
        "row_generator": STAGING_ROW_GENERATOR,
        "key_distribution": STAGING_KEY_DISTRIBUTION,
        "physical_shape_sha256": shapes["staging_request"],
        "engine": EXPECTED_ENGINE,
        "row_format": EXPECTED_ROW_FORMAT,
        "table_collation": EXPECTED_TABLE_COLLATION,
    }
    churn_full = _section(value, "staging_churn_full", "capacity measurement")
    churn_empty = _section(value, "staging_churn_empty", "capacity measurement")
    accepted_rows = _require_int(
        plan,
        "staging_measurement_accepted_rows",
        "capacity_plan",
    )
    _validate_exact(
        churn_full,
        {
            **staging_common,
            "row_count": accepted_rows,
            "actual_rows": accepted_rows,
        },
        "capacity measurement.staging_churn_full",
    )
    _validate_storage_bytes(
        churn_full,
        "capacity measurement.staging_churn_full",
    )
    _validate_exact(
        churn_empty,
        {
            **staging_common,
            "inserted_rows": accepted_rows,
            "deleted_rows": accepted_rows,
            "insert_commit_count": accepted_rows // INSERT_BATCH_SIZE,
            "delete_commit_count": accepted_rows // INSERT_BATCH_SIZE,
            "residual_live_rows": 0,
            "actual_rows": 0,
        },
        "capacity measurement.staging_churn_empty",
    )
    _validate_storage_bytes(
        churn_empty,
        "capacity measurement.staging_churn_empty",
    )

    accepted = _section(value, "staging_accepted", "capacity measurement")
    _validate_exact(
        accepted,
        {
            **staging_common,
            "staging_id_count": STAGING_ID_COUNT,
            "requests_per_staging_id": ACCEPTED_REQUESTS_PER_STAGING,
            "row_count": accepted_rows,
            "actual_rows": accepted_rows,
        },
        "capacity measurement.staging_accepted",
    )
    accepted_total = _validate_storage_bytes(
        accepted,
        "capacity measurement.staging_accepted",
    )

    diagnostic = _section(
        value,
        "staging_over_capacity_diagnostic",
        "capacity measurement",
    )
    diagnostic_rows = _require_int(
        plan,
        "staging_over_capacity_diagnostic_rows",
        "capacity_plan",
    )
    _validate_exact(
        diagnostic,
        {
            **staging_common,
            "staging_id_count": STAGING_ID_COUNT,
            "requests_per_staging_id": OVER_CAPACITY_REQUESTS_PER_STAGING,
            "row_count": diagnostic_rows,
            "actual_rows": diagnostic_rows,
        },
        "capacity measurement.staging_over_capacity_diagnostic",
    )
    diagnostic_total = _validate_storage_bytes(
        diagnostic,
        "capacity measurement.staging_over_capacity_diagnostic",
    )
    if diagnostic_total < accepted_total:
        raise RuntimeError(
            "over-capacity append diagnostic cannot allocate fewer bytes than the "
            "accepted post-churn fill"
        )

    _require_accepted_measurements_fit(value, plan)
    return value


def _measurement_peaks(
    measurement: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> dict[str, int]:
    registry = _section(measurement, "registry", "capacity measurement")
    source_scope = _section(measurement, "source_scope", "capacity measurement")
    staging_churn_full = _section(
        measurement,
        "staging_churn_full",
        "capacity measurement",
    )
    staging = _section(measurement, "staging_accepted", "capacity measurement")
    return {
        "artifact_producer_fingerprint": _ceil_ratio(
            _require_int(registry, "total_bytes", "capacity measurement.registry"),
            _require_int(
                plan,
                "registry_measurement_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "registry_measurement_safety_denominator",
                "capacity_plan",
            ),
        ),
        "source_scope": _ceil_ratio(
            _require_int(
                source_scope,
                "total_bytes",
                "capacity measurement.source_scope",
            ),
            _require_int(
                plan,
                "source_scope_measurement_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "source_scope_measurement_safety_denominator",
                "capacity_plan",
            ),
        ),
        "gallery_observation_staging_request": _ceil_ratio(
            max(
                _require_int(
                    staging_churn_full,
                    "total_bytes",
                    "capacity measurement.staging_churn_full",
                ),
                _require_int(
                    staging,
                    "total_bytes",
                    "capacity measurement.staging_accepted",
                ),
            ),
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_denominator",
                "capacity_plan",
            ),
        ),
        "bounded_nonmeasured_relations": _require_int(
            plan,
            "bounded_nonmeasured_conservative_peak_bytes",
            "capacity_plan",
        ),
        "cleanup_job": _require_int(
            plan,
            "cleanup_job_conservative_peak_bytes",
            "capacity_plan",
        ),
        "cleanup_cycle_root": _require_int(
            plan,
            "cleanup_cycle_root_conservative_peak_bytes",
            "capacity_plan",
        ),
        "cleanup_checkpoint": _require_int(
            plan,
            "cleanup_checkpoint_conservative_peak_bytes",
            "capacity_plan",
        ),
    }


def _require_accepted_measurements_fit(
    measurement: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> None:
    soft_limit = _require_int(
        plan,
        "newly_recomposed_relation_soft_limit_bytes",
        "capacity_plan",
    )
    peaks = _measurement_peaks(measurement, plan)
    failures = {name: value for name, value in peaks.items() if value >= soft_limit}
    if failures:
        raise RuntimeError(
            "accepted capacity evidence reaches or exceeds the decimal 400 MB "
            f"soft limit: {failures!r}"
        )


def _measurement_lines(
    name: str,
    section: Mapping[str, Any],
    *,
    safety_numerator: int,
    safety_denominator: int,
    soft_limit: int,
) -> list[str]:
    context = f"measurement.{name}"
    total = _require_int(section, "total_bytes", context)
    safety_peak = _ceil_ratio(total, safety_numerator, safety_denominator)
    return [
        f"[{name}]",
        f"relation = {_quoted(_require_str(section, 'relation', context))}",
        (
            "physical_shape_sha256 = "
            f"{_quoted(_require_str(section, 'physical_shape_sha256', context))}"
        ),
        f"row_generator = {_quoted(_require_str(section, 'row_generator', context))}",
        (
            "key_distribution = "
            f"{_quoted(_require_str(section, 'key_distribution', context))}"
        ),
        f"row_count = {_require_int(section, 'row_count', context)}",
        f"actual_rows = {_require_int(section, 'actual_rows', context)}",
        (
            "information_schema_estimated_rows = "
            f"{_require_int(section, 'information_schema_estimated_rows', context)}"
        ),
        f"engine = {_quoted(_require_str(section, 'engine', context))}",
        f"row_format = {_quoted(_require_str(section, 'row_format', context))}",
        (
            "table_collation = "
            f"{_quoted(_require_str(section, 'table_collation', context))}"
        ),
        f"data_bytes = {_require_int(section, 'data_bytes', context)}",
        f"index_bytes = {_require_int(section, 'index_bytes', context)}",
        f"total_bytes = {total}",
        f"safety_numerator = {safety_numerator}",
        f"safety_denominator = {safety_denominator}",
        f"safety_peak_bytes = {safety_peak}",
        f"headroom_bytes = {soft_limit - safety_peak}",
    ]


def render_receipt(measurement: Mapping[str, Any], *, input_sha256: str) -> str:
    """Render reviewed benchmark output into the committed receipt format."""

    plan = _plan()
    measured_registry, measured_registry_score, registry_scores = (
        _registry_capacity_dominance(plan)
    )
    protocol_widths = _bounded_protocol_width_guard(plan)
    protocol_width_by_name = {
        name: (score, accounted) for name, score, accounted in protocol_widths
    }
    shapes = _physical_shapes()
    physical_bytes = PHYSICAL_PATH.read_bytes()
    operational_physical_bytes = OPERATIONAL_PHYSICAL_PATH.read_bytes()
    soft_limit = _require_int(
        plan,
        "newly_recomposed_relation_soft_limit_bytes",
        "capacity_plan",
    )
    registry = _section(measurement, "registry", "capacity measurement")
    source_scope = _section(measurement, "source_scope", "capacity measurement")
    churn_full = _section(
        measurement,
        "staging_churn_full",
        "capacity measurement",
    )
    churn_empty = _section(
        measurement,
        "staging_churn_empty",
        "capacity measurement",
    )
    staging = _section(measurement, "staging_accepted", "capacity measurement")
    diagnostic = _section(
        measurement,
        "staging_over_capacity_diagnostic",
        "capacity measurement",
    )
    peaks = _measurement_peaks(measurement, plan)
    largest_relation, largest_peak = max(peaks.items(), key=lambda item: item[1])

    lines = [
        "receipt_version = 2",
        'generator = "verification/schema/generate_capacity_measurement_receipt.py"',
        'benchmark = "verification/schema/measure_capacity_mariadb.py"',
        'raw_measurement = "verification/schema/capacity_measurement.json"',
        f"generator_script_sha256 = {_quoted(_sha256_bytes(GENERATOR_PATH.read_bytes()))}",
        f"benchmark_script_sha256 = {_quoted(_sha256_bytes(BENCHMARK_PATH.read_bytes()))}",
        f"measurement_input_sha256 = {_quoted(input_sha256)}",
        f"execution_mode = {_quoted(EXECUTION_MODE)}",
        f"benchmark_image = {_quoted(MARIADB_IMAGE)}",
        (
            "mariadb_version = "
            f"{_quoted(_require_str(measurement, 'mariadb_version', 'capacity measurement'))}"
        ),
        f"innodb_page_size = {EXPECTED_INNODB_PAGE_SIZE}",
        f"benchmark_seed = {MEASUREMENT_SEED}",
        f"benchmark_insert_batch_size = {INSERT_BATCH_SIZE}",
        f"benchmark_insertion_order = {_quoted(INSERTION_ORDER)}",
        (
            "measurement_scope = "
            f"{_quoted(_require_str(plan, 'measurement_scope', 'capacity_plan'))}"
        ),
        f"soft_limit_bytes = {soft_limit}",
        f"capacity_plan_sha256 = {_quoted(_canonical_plan_sha256(plan))}",
        (
            "catalog_physical_manifest_sha256 = "
            f"{_quoted(_sha256_bytes(physical_bytes))}"
        ),
        (
            "operational_physical_manifest_sha256 = "
            f"{_quoted(_sha256_bytes(operational_physical_bytes))}"
        ),
        "",
        *_measurement_lines(
            "registry_measurement",
            registry,
            safety_numerator=_require_int(
                plan,
                "registry_measurement_safety_numerator",
                "capacity_plan",
            ),
            safety_denominator=_require_int(
                plan,
                "registry_measurement_safety_denominator",
                "capacity_plan",
            ),
            soft_limit=soft_limit,
        ),
        "",
        "[registry_dominance]",
        f"measured_relation = {_quoted(measured_registry)}",
        f"measured_conservative_width_score = {measured_registry_score}",
        f"bounded_relation_count = {len(registry_scores)}",
        "dominated_relations = [",
        *[
            f"  {_quoted(f'{name}:{score}')},"
            for name, score in registry_scores
            if name != measured_registry
        ],
        "]",
        'rule = "every unmeasured 50000-row registry has a no-greater conservative clustered-row plus complete-secondary-index width score"',
        "",
        *_measurement_lines(
            "source_scope_measurement",
            source_scope,
            safety_numerator=_require_int(
                plan,
                "source_scope_measurement_safety_numerator",
                "capacity_plan",
            ),
            safety_denominator=_require_int(
                plan,
                "source_scope_measurement_safety_denominator",
                "capacity_plan",
            ),
            soft_limit=soft_limit,
        ),
        "",
        *_measurement_lines(
            "staging_churn_full_measurement",
            churn_full,
            safety_numerator=_require_int(
                plan,
                "staging_measurement_fragmentation_safety_numerator",
                "capacity_plan",
            ),
            safety_denominator=_require_int(
                plan,
                "staging_measurement_fragmentation_safety_denominator",
                "capacity_plan",
            ),
            soft_limit=soft_limit,
        ),
        'rule = "fill the untruncated table to the complete runtime budget before any bounded delete"',
        "",
        "[staging_churn_empty]",
        (
            "relation = "
            f"{_quoted(_require_str(churn_empty, 'relation', 'measurement.staging_churn_empty'))}"
        ),
        f"physical_shape_sha256 = {_quoted(shapes['staging_request'])}",
        (
            "inserted_rows = "
            f"{_require_int(churn_empty, 'inserted_rows', 'measurement.staging_churn_empty')}"
        ),
        (
            "deleted_rows = "
            f"{_require_int(churn_empty, 'deleted_rows', 'measurement.staging_churn_empty')}"
        ),
        (
            "insert_commit_count = "
            f"{_require_int(churn_empty, 'insert_commit_count', 'measurement.staging_churn_empty')}"
        ),
        (
            "delete_commit_count = "
            f"{_require_int(churn_empty, 'delete_commit_count', 'measurement.staging_churn_empty')}"
        ),
        (
            "residual_live_rows = "
            f"{_require_int(churn_empty, 'residual_live_rows', 'measurement.staging_churn_empty')}"
        ),
        (
            "residual_data_bytes = "
            f"{_require_int(churn_empty, 'data_bytes', 'measurement.staging_churn_empty')}"
        ),
        (
            "residual_index_bytes = "
            f"{_require_int(churn_empty, 'index_bytes', 'measurement.staging_churn_empty')}"
        ),
        (
            "residual_total_bytes = "
            f"{_require_int(churn_empty, 'total_bytes', 'measurement.staging_churn_empty')}"
        ),
        'rule = "after the full-cap fill, fixed 5000-row commits child-free delete all 1.5m random keys without TRUNCATE before different-domain refill"',
        "",
        *_measurement_lines(
            "staging_accepted_measurement",
            staging,
            safety_numerator=_require_int(
                plan,
                "staging_measurement_fragmentation_safety_numerator",
                "capacity_plan",
            ),
            safety_denominator=_require_int(
                plan,
                "staging_measurement_fragmentation_safety_denominator",
                "capacity_plan",
            ),
            soft_limit=soft_limit,
        ),
        (
            "staging_id_count = "
            f"{_require_int(staging, 'staging_id_count', 'measurement.staging_accepted')}"
        ),
        (
            "synthetic_rows_per_staging_id = "
            f"{_require_int(staging, 'requests_per_staging_id', 'measurement.staging_accepted')}"
        ),
        (
            "budget_relation = "
            f"{_quoted(_require_str(plan, 'staging_budget_relation', 'capacity_plan'))}"
        ),
        (
            "budget_maximum_rows = "
            f"{_require_int(plan, 'staging_budget_maximum_rows', 'capacity_plan')}"
        ),
        (
            "merge_capacity_neutral = "
            + str(
                _require_bool(
                    plan,
                    "staging_measurement_merge_capacity_neutral",
                    "capacity_plan",
                )
            ).lower()
        ),
        'distribution_note = "300000 IDs times 5 is a synthetic random-key layout at the hard row budget, not a requests-per-gallery workload claim"',
        "",
        "[staging_over_capacity_diagnostic]",
        (
            "relation = "
            f"{_quoted(_require_str(diagnostic, 'relation', 'measurement.staging_over_capacity_diagnostic'))}"
        ),
        f"physical_shape_sha256 = {_quoted(shapes['staging_request'])}",
        (
            "row_count = "
            f"{_require_int(diagnostic, 'row_count', 'measurement.staging_over_capacity_diagnostic')}"
        ),
        (
            "actual_rows = "
            f"{_require_int(diagnostic, 'actual_rows', 'measurement.staging_over_capacity_diagnostic')}"
        ),
        (
            "data_bytes = "
            f"{_require_int(diagnostic, 'data_bytes', 'measurement.staging_over_capacity_diagnostic')}"
        ),
        (
            "index_bytes = "
            f"{_require_int(diagnostic, 'index_bytes', 'measurement.staging_over_capacity_diagnostic')}"
        ),
        (
            "total_bytes = "
            f"{_require_int(diagnostic, 'total_bytes', 'measurement.staging_over_capacity_diagnostic')}"
        ),
        "accepted = false",
        'reason = "row count exceeds the 1500000 runtime budget; this append is diagnostic only"',
        "",
        "[staging_retention]",
        (
            "staging_relation = "
            f"{_quoted(_require_str(plan, 'staging_retirement_relation', 'capacity_plan'))}"
        ),
        f"staging_physical_shape_sha256 = {_quoted(shapes['staging_root'])}",
        (
            "request_relation = "
            f"{_quoted(_require_str(plan, 'staging_measurement_relation', 'capacity_plan'))}"
        ),
        f"request_physical_shape_sha256 = {_quoted(shapes['staging_request'])}",
        (
            "budget_relation = "
            f"{_quoted(_require_str(plan, 'staging_budget_relation', 'capacity_plan'))}"
        ),
        f"budget_physical_shape_sha256 = {_quoted(shapes['staging_budget'])}",
        (
            "budget_maximum_rows = "
            f"{_require_int(plan, 'staging_budget_maximum_rows', 'capacity_plan')}"
        ),
        (
            "retire_maximum_rows_per_transaction = "
            f"{_require_int(plan, 'staging_in_band_retire_maximum_rows_per_transaction', 'capacity_plan')}"
        ),
        (
            "normal_terminal_staging_maximum = "
            f"{_require_int(plan, 'staging_normal_retained_terminal_gallery_maximum', 'capacity_plan')}"
        ),
        (
            "obligation_id = "
            f"{_quoted(_require_str(plan, 'staging_budget_obligation_id', 'capacity_plan'))}"
        ),
        'rule = "after acknowledged seal, the live shared ingest fence retires the terminal staging child-first before the next gallery; the global budget is an emergency fail-closed backstop"',
        "",
        "[bounded_nonmeasured_relations]",
        (
            "peak_rows = "
            f"{_require_int(plan, 'bounded_nonmeasured_peak_rows', 'capacity_plan')}"
        ),
        (
            "accounted_bytes_per_row = "
            f"{_require_int(plan, 'bounded_nonmeasured_accounted_bytes_per_row', 'capacity_plan')}"
        ),
        (
            "conservative_peak_bytes = "
            f"{_require_int(plan, 'bounded_nonmeasured_conservative_peak_bytes', 'capacity_plan')}"
        ),
        "",
        "[bounded_cleanup_protocol]",
        'job_relation = "cleanup_job"',
        f"job_physical_shape_sha256 = {_quoted(shapes['cleanup_job'])}",
        (
            "job_conservative_record_and_index_bytes = "
            f"{protocol_width_by_name['cleanup_job'][0]}"
        ),
        (f"job_accounted_bytes_per_row = {protocol_width_by_name['cleanup_job'][1]}"),
        (
            "job_width_headroom_bytes = "
            f"{protocol_width_by_name['cleanup_job'][1] - protocol_width_by_name['cleanup_job'][0]}"
        ),
        (
            "job_row_count = "
            f"{_require_int(plan, 'cleanup_job_peak_rows', 'capacity_plan')}"
        ),
        (
            "job_conservative_peak_bytes = "
            f"{_require_int(plan, 'cleanup_job_conservative_peak_bytes', 'capacity_plan')}"
        ),
        'root_relation = "cleanup_cycle_root"',
        f"root_physical_shape_sha256 = {_quoted(shapes['cleanup_root'])}",
        (
            "root_conservative_record_and_index_bytes = "
            f"{protocol_width_by_name['cleanup_cycle_root'][0]}"
        ),
        (
            "root_accounted_bytes_per_row = "
            f"{protocol_width_by_name['cleanup_cycle_root'][1]}"
        ),
        (
            "root_width_headroom_bytes = "
            f"{protocol_width_by_name['cleanup_cycle_root'][1] - protocol_width_by_name['cleanup_cycle_root'][0]}"
        ),
        (
            "root_row_count = "
            f"{_require_int(plan, 'cleanup_cycle_root_peak_rows', 'capacity_plan')}"
        ),
        (
            "root_key_maximum_bytes = "
            f"{_require_int(plan, 'cleanup_frozen_root_key_maximum_bytes', 'capacity_plan')}"
        ),
        (
            "root_conservative_peak_bytes = "
            f"{_require_int(plan, 'cleanup_cycle_root_conservative_peak_bytes', 'capacity_plan')}"
        ),
        'checkpoint_relation = "cleanup_checkpoint"',
        f"checkpoint_physical_shape_sha256 = {_quoted(shapes['cleanup_checkpoint'])}",
        (
            "checkpoint_conservative_record_and_index_bytes = "
            f"{protocol_width_by_name['cleanup_checkpoint'][0]}"
        ),
        (
            "checkpoint_accounted_bytes_per_row = "
            f"{protocol_width_by_name['cleanup_checkpoint'][1]}"
        ),
        (
            "checkpoint_width_headroom_bytes = "
            f"{protocol_width_by_name['cleanup_checkpoint'][1] - protocol_width_by_name['cleanup_checkpoint'][0]}"
        ),
        (
            "checkpoint_row_count = "
            f"{_require_int(plan, 'cleanup_checkpoint_peak_rows', 'capacity_plan')}"
        ),
        (
            "checkpoint_conservative_peak_bytes = "
            f"{_require_int(plan, 'cleanup_checkpoint_conservative_peak_bytes', 'capacity_plan')}"
        ),
        (
            "obligation_id = "
            f"{_quoted(_require_str(plan, 'cleanup_frozen_root_obligation_id', 'capacity_plan'))}"
        ),
        "",
        "[scenario_context]",
        (
            "planning_gallery_count = "
            f"{_require_int(plan, 'planning_gallery_count', 'capacity_plan')}"
        ),
        (
            "planning_average_files_per_gallery = "
            f"{_require_int(plan, 'planning_average_files_per_gallery', 'capacity_plan')}"
        ),
        (
            "stress_gallery_count = "
            f"{_require_int(plan, 'stress_gallery_count', 'capacity_plan')}"
        ),
        "stress_is_accepted_sizing_bound = false",
        'rule = "file and gallery counts are scenario context; neither derives the staging request hard bound"',
        "",
        "[acceptance]",
        f"largest_relation = {_quoted(largest_relation)}",
        f"largest_safety_peak_bytes = {largest_peak}",
        f"headroom_bytes = {soft_limit - largest_peak}",
        "all_affected_relations_below_soft_limit = true",
    ]
    return "\n".join(lines) + "\n"


def validate_receipt(path: Path) -> None:
    """Validate committed evidence against current code, plan, and manifests."""

    if not RAW_MEASUREMENT_PATH.is_file():
        raise RuntimeError(
            f"committed raw capacity measurement is missing: {RAW_MEASUREMENT_PATH}"
        )
    raw_measurement = validate_measurement(RAW_MEASUREMENT_PATH)
    expected_receipt = render_receipt(
        raw_measurement,
        input_sha256=_sha256_bytes(RAW_MEASUREMENT_PATH.read_bytes()),
    )
    if path.read_text() != expected_receipt:
        raise RuntimeError(
            "capacity receipt does not exactly match the committed raw measurement"
        )
    document = _load(path)
    plan = _plan()
    measured_registry, measured_registry_score, registry_scores = (
        _registry_capacity_dominance(plan)
    )
    protocol_widths = _bounded_protocol_width_guard(plan)
    protocol_width_by_name = {
        name: (score, accounted) for name, score, accounted in protocol_widths
    }
    shapes = _physical_shapes()
    soft_limit = _require_int(
        plan,
        "newly_recomposed_relation_soft_limit_bytes",
        "capacity_plan",
    )
    expected_top: Mapping[str, object] = {
        "receipt_version": 2,
        "generator": "verification/schema/generate_capacity_measurement_receipt.py",
        "benchmark": "verification/schema/measure_capacity_mariadb.py",
        "raw_measurement": "verification/schema/capacity_measurement.json",
        "generator_script_sha256": _sha256_bytes(GENERATOR_PATH.read_bytes()),
        "benchmark_script_sha256": _sha256_bytes(BENCHMARK_PATH.read_bytes()),
        "execution_mode": EXECUTION_MODE,
        "benchmark_image": MARIADB_IMAGE,
        "innodb_page_size": EXPECTED_INNODB_PAGE_SIZE,
        "benchmark_seed": MEASUREMENT_SEED,
        "benchmark_insert_batch_size": INSERT_BATCH_SIZE,
        "benchmark_insertion_order": INSERTION_ORDER,
        "measurement_scope": _require_str(
            plan,
            "measurement_scope",
            "capacity_plan",
        ),
        "soft_limit_bytes": soft_limit,
        "capacity_plan_sha256": _canonical_plan_sha256(plan),
        "catalog_physical_manifest_sha256": _sha256_bytes(PHYSICAL_PATH.read_bytes()),
        "operational_physical_manifest_sha256": _sha256_bytes(
            OPERATIONAL_PHYSICAL_PATH.read_bytes()
        ),
    }
    _validate_exact(document, expected_top, "capacity receipt")
    input_sha256 = _require_str(
        document,
        "measurement_input_sha256",
        "capacity receipt",
    )
    if len(input_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in input_sha256
    ):
        raise RuntimeError("capacity receipt measurement_input_sha256 is invalid")
    version = _require_str(document, "mariadb_version", "capacity receipt")
    if not version.startswith(
        _require_str(plan, "mariadb_measurement_version", "capacity_plan")
    ):
        raise RuntimeError("capacity receipt used the wrong MariaDB version")

    measurement_sections = {
        "registry_measurement": (
            "registry",
            _require_int(
                plan,
                "registry_measurement_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "registry_measurement_safety_denominator",
                "capacity_plan",
            ),
            shapes["registry"],
        ),
        "source_scope_measurement": (
            "source_scope",
            _require_int(
                plan,
                "source_scope_measurement_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "source_scope_measurement_safety_denominator",
                "capacity_plan",
            ),
            shapes["source_scope"],
        ),
        "staging_churn_full_measurement": (
            "gallery_observation_staging_request",
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_denominator",
                "capacity_plan",
            ),
            shapes["staging_request"],
        ),
        "staging_accepted_measurement": (
            "gallery_observation_staging_request",
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_numerator",
                "capacity_plan",
            ),
            _require_int(
                plan,
                "staging_measurement_fragmentation_safety_denominator",
                "capacity_plan",
            ),
            shapes["staging_request"],
        ),
    }
    measurement_expected: Mapping[str, Mapping[str, object]] = {
        "registry_measurement": {
            "relation": _require_str(
                plan,
                "registry_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": REGISTRY_ROW_GENERATOR,
            "key_distribution": REGISTRY_KEY_DISTRIBUTION,
            "row_count": _require_int(
                plan,
                "registry_measurement_row_count",
                "capacity_plan",
            ),
            "actual_rows": _require_int(
                plan,
                "registry_measurement_row_count",
                "capacity_plan",
            ),
        },
        "source_scope_measurement": {
            "relation": _require_str(
                plan,
                "source_scope_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": SOURCE_SCOPE_ROW_GENERATOR,
            "key_distribution": SOURCE_SCOPE_KEY_DISTRIBUTION,
            "row_count": SOURCE_SCOPE_ROW_COUNT,
            "actual_rows": SOURCE_SCOPE_ROW_COUNT,
        },
        "staging_churn_full_measurement": {
            "relation": _require_str(
                plan,
                "staging_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "row_count": _require_int(
                plan,
                "staging_measurement_accepted_rows",
                "capacity_plan",
            ),
            "actual_rows": _require_int(
                plan,
                "staging_measurement_accepted_rows",
                "capacity_plan",
            ),
        },
        "staging_accepted_measurement": {
            "relation": _require_str(
                plan,
                "staging_measurement_relation",
                "capacity_plan",
            ),
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "row_count": _require_int(
                plan,
                "staging_measurement_accepted_rows",
                "capacity_plan",
            ),
            "actual_rows": _require_int(
                plan,
                "staging_measurement_accepted_rows",
                "capacity_plan",
            ),
            "staging_id_count": STAGING_ID_COUNT,
            "synthetic_rows_per_staging_id": ACCEPTED_REQUESTS_PER_STAGING,
        },
    }
    accepted_peaks: dict[str, int] = {}
    for section_name, (
        accepted_name,
        numerator,
        denominator,
        shape_sha256,
    ) in measurement_sections.items():
        section = _section(document, section_name, "capacity receipt")
        total = _validate_storage_bytes(section, f"capacity receipt.{section_name}")
        safety_peak = _ceil_ratio(total, numerator, denominator)
        _validate_exact(
            section,
            {
                **measurement_expected[section_name],
                "physical_shape_sha256": shape_sha256,
                "engine": EXPECTED_ENGINE,
                "row_format": EXPECTED_ROW_FORMAT,
                "table_collation": EXPECTED_TABLE_COLLATION,
                "safety_numerator": numerator,
                "safety_denominator": denominator,
                "safety_peak_bytes": safety_peak,
                "headroom_bytes": soft_limit - safety_peak,
            },
            f"capacity receipt.{section_name}",
        )
        if safety_peak >= soft_limit:
            raise RuntimeError(f"capacity receipt {section_name} exceeds soft limit")
        accepted_peaks[accepted_name] = max(
            accepted_peaks.get(accepted_name, 0),
            safety_peak,
        )

    dominance = _section(document, "registry_dominance", "capacity receipt")
    _validate_exact(
        dominance,
        {
            "measured_relation": measured_registry,
            "measured_conservative_width_score": measured_registry_score,
            "bounded_relation_count": len(registry_scores),
            "dominated_relations": [
                f"{name}:{score}"
                for name, score in registry_scores
                if name != measured_registry
            ],
        },
        "capacity receipt.registry_dominance",
    )

    churn = _section(document, "staging_churn_empty", "capacity receipt")
    accepted_rows = _require_int(
        plan,
        "staging_measurement_accepted_rows",
        "capacity_plan",
    )
    _validate_exact(
        churn,
        {
            "relation": _require_str(
                plan,
                "staging_measurement_relation",
                "capacity_plan",
            ),
            "physical_shape_sha256": shapes["staging_request"],
            "inserted_rows": accepted_rows,
            "deleted_rows": accepted_rows,
            "insert_commit_count": accepted_rows // INSERT_BATCH_SIZE,
            "delete_commit_count": accepted_rows // INSERT_BATCH_SIZE,
            "residual_live_rows": 0,
        },
        "capacity receipt.staging_churn_empty",
    )
    if _require_int(
        churn, "residual_data_bytes", "capacity receipt.staging_churn_empty"
    ) + _require_int(
        churn,
        "residual_index_bytes",
        "capacity receipt.staging_churn_empty",
    ) != _require_int(
        churn,
        "residual_total_bytes",
        "capacity receipt.staging_churn_empty",
    ):
        raise RuntimeError("capacity receipt staging churn bytes are inconsistent")

    retention = _section(document, "staging_retention", "capacity receipt")
    _validate_exact(
        retention,
        {
            "staging_physical_shape_sha256": shapes["staging_root"],
            "request_physical_shape_sha256": shapes["staging_request"],
            "budget_physical_shape_sha256": shapes["staging_budget"],
            "budget_maximum_rows": _require_int(
                plan,
                "staging_budget_maximum_rows",
                "capacity_plan",
            ),
            "retire_maximum_rows_per_transaction": _require_int(
                plan,
                "staging_in_band_retire_maximum_rows_per_transaction",
                "capacity_plan",
            ),
            "normal_terminal_staging_maximum": _require_int(
                plan,
                "staging_normal_retained_terminal_gallery_maximum",
                "capacity_plan",
            ),
            "obligation_id": _require_str(
                plan,
                "staging_budget_obligation_id",
                "capacity_plan",
            ),
        },
        "capacity receipt.staging_retention",
    )
    cleanup = _section(document, "bounded_cleanup_protocol", "capacity receipt")
    _validate_exact(
        cleanup,
        {
            "job_physical_shape_sha256": shapes["cleanup_job"],
            "job_conservative_record_and_index_bytes": protocol_width_by_name[
                "cleanup_job"
            ][0],
            "job_accounted_bytes_per_row": protocol_width_by_name["cleanup_job"][1],
            "job_width_headroom_bytes": (
                protocol_width_by_name["cleanup_job"][1]
                - protocol_width_by_name["cleanup_job"][0]
            ),
            "root_physical_shape_sha256": shapes["cleanup_root"],
            "root_conservative_record_and_index_bytes": protocol_width_by_name[
                "cleanup_cycle_root"
            ][0],
            "root_accounted_bytes_per_row": protocol_width_by_name[
                "cleanup_cycle_root"
            ][1],
            "root_width_headroom_bytes": (
                protocol_width_by_name["cleanup_cycle_root"][1]
                - protocol_width_by_name["cleanup_cycle_root"][0]
            ),
            "root_key_maximum_bytes": _require_int(
                plan,
                "cleanup_frozen_root_key_maximum_bytes",
                "capacity_plan",
            ),
            "checkpoint_physical_shape_sha256": shapes["cleanup_checkpoint"],
            "checkpoint_conservative_record_and_index_bytes": (
                protocol_width_by_name["cleanup_checkpoint"][0]
            ),
            "checkpoint_accounted_bytes_per_row": protocol_width_by_name[
                "cleanup_checkpoint"
            ][1],
            "checkpoint_width_headroom_bytes": (
                protocol_width_by_name["cleanup_checkpoint"][1]
                - protocol_width_by_name["cleanup_checkpoint"][0]
            ),
        },
        "capacity receipt.bounded_cleanup_protocol",
    )
    accepted_peaks.update(
        {
            "bounded_nonmeasured_relations": _require_int(
                plan,
                "bounded_nonmeasured_conservative_peak_bytes",
                "capacity_plan",
            ),
            "cleanup_job": _require_int(
                plan,
                "cleanup_job_conservative_peak_bytes",
                "capacity_plan",
            ),
            "cleanup_cycle_root": _require_int(
                plan,
                "cleanup_cycle_root_conservative_peak_bytes",
                "capacity_plan",
            ),
            "cleanup_checkpoint": _require_int(
                plan,
                "cleanup_checkpoint_conservative_peak_bytes",
                "capacity_plan",
            ),
        }
    )
    if any(peak >= soft_limit for peak in accepted_peaks.values()):
        raise RuntimeError("capacity receipt conservative bound exceeds soft limit")
    largest_relation, largest_peak = max(
        accepted_peaks.items(),
        key=lambda item: item[1],
    )
    acceptance = _section(document, "acceptance", "capacity receipt")
    _validate_exact(
        acceptance,
        {
            "largest_relation": largest_relation,
            "largest_safety_peak_bytes": largest_peak,
            "headroom_bytes": soft_limit - largest_peak,
            "all_affected_relations_below_soft_limit": True,
        },
        "capacity receipt.acceptance",
    )
    diagnostic = _section(
        document,
        "staging_over_capacity_diagnostic",
        "capacity receipt",
    )
    _validate_exact(
        diagnostic,
        {
            "relation": _require_str(
                plan,
                "staging_measurement_relation",
                "capacity_plan",
            ),
            "physical_shape_sha256": shapes["staging_request"],
            "row_count": _require_int(
                plan,
                "staging_over_capacity_diagnostic_rows",
                "capacity_plan",
            ),
            "actual_rows": _require_int(
                plan,
                "staging_over_capacity_diagnostic_rows",
                "capacity_plan",
            ),
            "accepted": False,
        },
        "capacity receipt.staging_over_capacity_diagnostic",
    )
    diagnostic_total = _require_int(
        diagnostic,
        "data_bytes",
        "capacity receipt.staging_over_capacity_diagnostic",
    ) + _require_int(
        diagnostic,
        "index_bytes",
        "capacity receipt.staging_over_capacity_diagnostic",
    )
    if diagnostic_total != _require_int(
        diagnostic,
        "total_bytes",
        "capacity receipt.staging_over_capacity_diagnostic",
    ):
        raise RuntimeError("capacity receipt staging diagnostic bytes are inconsistent")
    accepted_total = _require_int(
        _section(
            document,
            "staging_accepted_measurement",
            "capacity receipt",
        ),
        "total_bytes",
        "capacity receipt.staging_accepted_measurement",
    )
    if diagnostic_total < accepted_total:
        raise RuntimeError(
            "capacity receipt staging append diagnostic allocated fewer bytes than "
            "the accepted post-churn fill"
        )
    if _require_bool(diagnostic, "accepted", "capacity receipt diagnostic"):
        raise RuntimeError("staging over-capacity diagnostic cannot be accepted")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the committed receipt instead of writing it",
    )
    parser.add_argument(
        "--measurement",
        type=Path,
        help="JSON output produced by measure_capacity_mariadb.py",
    )
    arguments = parser.parse_args(argv)
    if arguments.check:
        if arguments.measurement is not None:
            parser.error("--check and --measurement are mutually exclusive")
        if not RECEIPT_PATH.is_file():
            raise SystemExit(
                "verification/schema/capacity_measurement.toml is missing; run the "
                "manual benchmark and receipt generator"
            )
        validate_receipt(RECEIPT_PATH)
        return 0
    if arguments.measurement is None:
        parser.error("writing a receipt requires --measurement benchmark JSON")
    input_bytes = arguments.measurement.read_bytes()
    validate_measurement(arguments.measurement)
    RAW_MEASUREMENT_PATH.write_bytes(input_bytes)
    measurement = validate_measurement(RAW_MEASUREMENT_PATH)
    rendered = render_receipt(
        measurement,
        input_sha256=_sha256_bytes(input_bytes),
    )
    RECEIPT_PATH.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
