"""R01 disposable local family-read comparison; no existing database is opened."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import time
import tracemalloc
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from dataclasses import field as dataclass_field
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ("src", "scripts", "tests")]
from ingest_growth_hash_probe import (  # noqa: E402 - checkout source.
    databases,
    profile_mariadb,
    record,
    require_point_work_bound,
    write_report,
)
from vnext_test_database import (  # noqa: E402 - checkout fixtures.
    set_foreign_key_checks,
)

import h2hdb.vnext_analysis_repository as analysis  # noqa: E402 - checkout source.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402 - checkout source.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout source.
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - checkout source.

type HistoricalLoader = Callable[
    [VNextUnitOfWork, bytes | None, Sequence[bytes]], analysis._FileDecisionEvidence
]
type Roots = tuple[bytes | None, bytes]
type Observation = tuple[str, tuple[bytes, ...], int]


@dataclass
class ReadObservations:
    families: list[Observation] = dataclass_field(default_factory=list)
    layout_calls: int = 0


def historical_loader(ref: str) -> tuple[HistoricalLoader, dict[str, str]]:
    source = subprocess.check_output(
        [
            "git",
            "-C",
            str(ROOT),
            "show",
            ref + ":src/h2hdb/vnext_analysis_repository.py",
        ]
    )
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_load_file_decision_evidence"
    )
    if [argument.arg for argument in node.args.args] != [
        "work",
        "analysis_id",
        "digests",
    ]:
        raise ValueError(
            "baseline ref does not contain the historical single-root helper"
        )
    namespace = dict(vars(analysis))
    exec(
        compile(
            ast.Module(body=[node], type_ignores=[]),
            "<historical-file-decision-evidence>",
            "exec",
        ),
        namespace,
    )
    segment = ast.get_source_segment(source.decode(), node)
    assert segment is not None
    return cast(HistoricalLoader, namespace[node.name]), {
        "ref": ref,
        "resolved_ref": subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", ref], text=True
        ).strip(),
        "source_sha256": sha256(source).hexdigest(),
        "function_sha256": sha256(segment.encode()).hexdigest(),
    }


CONTRACT: dict[str, Any] = {
    "version": 1,
    "sql": "fresh layout 2 per non-null root; family 2*ceil(U/17)",
    "returned_family_grid_rows": "2*K*U",
    "max_family_primary_key_probes": "6*K*U",
    "cpu_seconds_per_key": 0.001,
    "wall_seconds_per_key": 0.005,
    "python_peak_bytes_per_call": 8 * 1024 * 1024,
    "main_acceptance": "overlapping main cases candidate median wall < baseline median wall",
    "repetitions": 3,
    "page_keys": 128,
}
SHADOWS = (
    ("catalog_a_file_decision_shadow_anchors", None),
    ("catalog_a_file_decision_shadow_occurrences", "occurrence_count"),
    ("catalog_a_file_decision_shadow_artists", "artist_count"),
    (
        "catalog_a_file_decision_shadow_gallery_artist_max",
        "maximum_gallery_artist_count",
    ),
    ("catalog_a_file_decision_shadow_seals", None),
)


def hash_source() -> dict[str, str]:
    return {
        str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name: sha256(
            path.read_bytes()
        ).hexdigest()
        for path in (
            ROOT / "src/h2hdb/vnext_analysis_repository.py",
            ROOT / "src/h2hdb/vnext_analysis_decision_batch.py",
            ROOT / "scripts/ingest_growth_hash_probe.py",
            Path(__file__),
        )
    }


def scalar(index: int, changed: bool = False) -> tuple[int, int, int]:
    # Already-aggregated decision values: 80% singleton, 10% moderately shared,
    # 10% high multiplicity; this does not reconstruct source gallery artists.
    occurrence, artists, maximum = (
        (1, 1, 1) if index % 10 < 8 else (4, 3, 2) if index % 10 == 8 else (32, 17, 8)
    )
    return occurrence + int(changed), artists, maximum


def batches[T](rows: Sequence[T], limit: int = 256) -> Iterator[list[T]]:
    for offset in range(0, len(rows), limit):
        yield list(rows[offset : offset + limit])


def seed(
    connector: SQLConnector,
    *,
    retained: int,
    baseline_layers: int,
    mode: str,
    reuse_percent: int,
    dense: bool = False,
) -> tuple[
    Roots, tuple[bytes, ...], tuple[bytes, ...], tuple[bytes, ...], set[bytes], int
]:
    current = (100).to_bytes(16, "big")
    ancestors = tuple(
        index.to_bytes(16, "big") for index in range(1, baseline_layers + 1)
    )
    baseline = ancestors[0] if ancestors else None
    current_ancestry = (
        tuple((100 + depth).to_bytes(16, "big") for depth in range(17))
        if mode == "disjoint"
        else (current, *ancestors)
        if mode == "overlay"
        else (current,)
    )
    keys = tuple(index.to_bytes(32, "big") for index in range(retained))
    set_foreign_key_checks(connector, enabled=False)
    facts: list[tuple[bytes, bytes, tuple[int, int, int]]] = []
    current_own: set[bytes] = set()
    with connector.transaction():
        for index, owner in enumerate(ancestors):
            connector.execute_many(
                "INSERT INTO catalog_analysis_state_ancestry "
                "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s,%s,%s)",
                [
                    (owner, depth, ancestor)
                    for depth, ancestor in enumerate(ancestors[index:])
                ],
            )
            if index + 1 < len(ancestors):
                connector.execute(
                    "INSERT INTO catalog_analysis_baselines "
                    "(analysis_id, base_analysis_id) VALUES (%s,%s)",
                    (owner, ancestors[index + 1]),
                )
        connector.execute_many(
            "INSERT INTO catalog_analysis_state_ancestry "
            "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s,%s,%s)",
            [(current, depth, owner) for depth, owner in enumerate(current_ancestry)],
        )
        if baseline:
            connector.execute(
                "INSERT INTO catalog_analysis_baselines "
                "(analysis_id, base_analysis_id) VALUES (%s,%s)",
                (current, baseline),
            )
        for index, key in enumerate(keys):
            if baseline:
                if dense:
                    occurrence, artists, maximum = scalar(index)
                    facts.extend(
                        (owner, key, (occurrence + depth, artists, maximum))
                        for depth, owner in enumerate(ancestors)
                    )
                else:
                    facts.append(
                        (ancestors[index % len(ancestors)], key, scalar(index))
                    )
            changed = mode == "overlay" and index % 100 >= reuse_percent
            if mode != "overlay" or changed:
                occurrence, artists, maximum = scalar(index, changed)
                occurrence += 100 if mode == "disjoint" else 0
                own_layers = current_ancestry if mode == "disjoint" else (current,)
                facts.extend(
                    (owner, key, (occurrence + depth, artists, maximum))
                    for depth, owner in enumerate(own_layers)
                )
                current_own.add(key)
        for table, column in SHADOWS:
            rows: list[tuple[Any, ...]] = [
                (owner, key)
                if column is None
                else (
                    owner,
                    key,
                    values[
                        (
                            "occurrence_count",
                            "artist_count",
                            "maximum_gallery_artist_count",
                        ).index(column)
                    ],
                )
                for owner, key, values in facts
            ]
            columns = "analysis_id,file_sha256" + (
                "" if column is None else "," + column
            )
            placeholders = "%s,%s" + ("" if column is None else ",%s")
            for chunk in batches(rows):
                connector.execute_many(
                    f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", chunk
                )
    return (
        (baseline, current),
        keys,
        ancestors,
        current_ancestry,
        current_own,
        len(facts),
    )


def invoke(
    work: VNextUnitOfWork,
    roots: Roots,
    keys: Sequence[bytes],
    variant: str,
    historical_helper: HistoricalLoader,
) -> tuple[analysis._FileDecisionEvidence, ...]:
    if variant == "candidate":
        return analysis._load_file_decision_evidence(work, roots, keys)
    return tuple(historical_helper(work, owner, keys) for owner in roots)


def oracle(
    pair: tuple[analysis._FileDecisionEvidence, ...],
    keys: Sequence[bytes],
    roots: Roots,
    mode: str,
    reuse_percent: int,
    current_own: set[bytes],
) -> None:
    parents, current = pair
    expected_parent = (
        {}
        if roots[0] is None
        else {key: scalar(int.from_bytes(key, "big")) for key in keys}
    )
    expected_current = {}
    for key in keys:
        index = int.from_bytes(key, "big")
        occurrence, artists, maximum = scalar(
            index, mode == "overlay" and index % 100 >= reuse_percent
        )
        expected_current[key] = (
            occurrence + (100 if mode == "disjoint" else 0),
            artists,
            maximum,
        )

    def values(
        result: dict[bytes, analysis._Decision],
    ) -> dict[bytes, tuple[int, int, int]]:
        return {
            key: (
                value.occurrence_count,
                value.artist_count,
                value.maximum_gallery_artist_count,
            )
            for key, value in result.items()
        }

    assert values(parents.resolved) == expected_parent
    assert values(current.resolved) == expected_current
    assert values(current.own_shadows) == {
        key: value for key, value in expected_current.items() if key in current_own
    }
    assert not current.own_tombstones and not parents.own_tombstones


@contextmanager
def observe(connector: SQLConnector) -> Iterator[ReadObservations]:
    observations = ReadObservations()
    original = connector.fetch_all
    original_one = connector.fetch_one

    def fetch(sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        rows = original(sql, parameters)
        if sql.startswith(
            "SELECT ancestor_depth, ancestor_analysis_id FROM catalog_analysis_state_ancestry "
        ):
            observations.layout_calls += 1
        if sql.startswith("WITH requested_analyses(analysis_id) AS ("):
            kind = (
                "tombstone"
                if "LEFT JOIN catalog_analysis_file_hash_decision_tombstone " in sql
                else "shadow"
            )
            layers = tuple(
                value
                for value in parameters[:-1]
                if isinstance(value, bytes) and len(value) == 16
            )
            keys = tuple(
                value
                for value in parameters[:-1]
                if isinstance(value, bytes) and len(value) == 32
            )
            assert len(layers) <= 17 and len(keys) <= 128
            assert len(rows) == len(layers) * len(keys)
            observations.families.append((kind, layers, len(rows)))
        return rows

    def fetch_one(sql: str, parameters: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        row = original_one(sql, parameters)
        if sql.startswith("SELECT base_analysis_id FROM catalog_analysis_baselines "):
            observations.layout_calls += 1
        return row

    with (
        patch.object(connector, "fetch_all", fetch),
        patch.object(connector, "fetch_one", fetch_one),
    ):
        yield observations


def check_unique_reads(
    observed: list[Observation], *, expected_layers: frozenset[bytes], key_count: int
) -> None:
    unique_layers = len(expected_layers)
    assert len(observed) == 2 * ((unique_layers + 16) // 17), (
        "family query budget exceeded"
    )
    assert sum(rows for _, _, rows in observed) == 2 * unique_layers * key_count, (
        "family row budget exceeded"
    )
    for kind in ("shadow", "tombstone"):
        counted = Counter(
            owner for found, layers, _ in observed if found == kind for owner in layers
        )
        assert set(counted) == expected_layers, (
            "family layer identity differs from expected union"
        )
        assert set(counted.values()) == {1}, "repeated family layer read"


def measure_case(
    connector: SQLConnector,
    shape: dict[str, Any],
    *,
    backend: str,
    historical_helper: HistoricalLoader,
) -> dict[str, Any]:
    roots, all_keys, baseline_ancestry, current_ancestry, own, family_count = seed(
        connector, **{k: v for k, v in shape.items() if k != "requested"}
    )
    keys = all_keys[: shape["requested"]]
    work = VNextUnitOfWork(connector, backend=backend)
    expected_layers = frozenset(baseline_ancestry + current_ancestry)
    unique_layers = len(expected_layers)
    samples: list[dict[str, Any]] = []
    plans = []
    negative_rejected = False
    # Per-call ownership/work is checked outside timing; neither candidate nor
    # baseline gains a result/layout cache between calls or cycles.
    structural = {
        "candidate_queries": 0,
        "candidate_grid_rows": 0,
        "baseline_queries": 0,
        "baseline_grid_rows": 0,
        "candidate_layout_calls": 0,
        "baseline_layout_calls": 0,
    }
    page: Sequence[bytes]
    for page in batches(keys, 128):
        for variant in ("candidate", "baseline"):
            with connector.read_transaction(), observe(connector) as observed:
                pair = invoke(work, roots, page, variant, historical_helper)
            oracle(pair, page, roots, shape["mode"], shape["reuse_percent"], own)
            structural[variant + "_queries"] += len(observed.families)
            structural[variant + "_layout_calls"] += observed.layout_calls
            assert observed.layout_calls == 2 * sum(root is not None for root in roots)
            structural[variant + "_grid_rows"] += sum(
                value[2] for value in observed.families
            )
            if variant == "candidate":
                check_unique_reads(
                    observed.families,
                    expected_layers=expected_layers,
                    key_count=len(page),
                )
            elif set(baseline_ancestry) & set(current_ancestry):
                try:
                    check_unique_reads(
                        observed.families,
                        expected_layers=expected_layers,
                        key_count=len(page),
                    )
                except AssertionError:
                    negative_rejected = True
                else:
                    raise AssertionError("original double reads were not rejected")
    if set(baseline_ancestry) & set(current_ancestry):
        assert negative_rejected
    # Native MariaDB work is profiled on the real first-page queries after the
    # structural pass, with EXPLAIN/ANALYZE outside timed measurements.
    for variant in ("candidate", "baseline"):
        with connector.read_transaction(), record(connector) as recorder:
            pair = invoke(work, roots, keys[:128], variant, historical_helper)
        oracle(pair, keys[:128], roots, shape["mode"], shape["reuse_percent"], own)
        queries = []
        for query in recorder.queries:
            if query.kind not in ("shadow_family_points", "tombstone_points"):
                continue
            if backend == "mariadb":
                with connector.read_transaction():
                    query.plan = profile_mariadb(
                        cast(MariaDBConnector, connector), query
                    )
                require_point_work_bound(query)
            else:
                with connector.read_transaction():
                    query.plan = connector.fetch_all(
                        "EXPLAIN QUERY PLAN " + query.sql, query.parameters
                    )
            row = asdict(query)
            row["parameters"] = [
                item.hex() if isinstance(item, bytes) else item
                for item in query.parameters
            ]
            queries.append(row)
        plans.append({"variant": variant, "queries": queries})
    # Fixed per-call peaks, reported separately so tracemalloc does not distort
    # the three paired elapsed/CPU sample cycles.
    peaks = {}
    for variant in ("candidate", "baseline"):
        peaks[variant] = 0
        for page in (keys[:128], keys[-128:]):
            with connector.read_transaction():
                tracemalloc.start()
                try:
                    pair = invoke(work, roots, page, variant, historical_helper)
                    _, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
            oracle(pair, page, roots, shape["mode"], shape["reuse_percent"], own)
            peaks[variant] = max(peaks[variant], peak)
        assert peaks[variant] <= CONTRACT["python_peak_bytes_per_call"]
    for repetition in range(CONTRACT["repetitions"]):
        order = (
            ("baseline", "candidate")
            if repetition % 2 == 0
            else ("candidate", "baseline")
        )
        for variant in order:
            elapsed = cpu = 0.0
            for page in batches(keys, 128):
                with connector.read_transaction():
                    wall_start, cpu_start = time.perf_counter(), time.process_time()
                    pair = invoke(work, roots, page, variant, historical_helper)
                    cpu += time.process_time() - cpu_start
                    elapsed += time.perf_counter() - wall_start
                oracle(pair, page, roots, shape["mode"], shape["reuse_percent"], own)
            samples.append(
                {
                    "variant": variant,
                    "cycle": repetition,
                    "seconds": elapsed,
                    "cpu_seconds": cpu,
                    "keys": len(keys),
                    "pages": (len(keys) + 127) // 128,
                }
            )
    medians = {
        variant: {
            field: median(
                sample[field] for sample in samples if sample["variant"] == variant
            )
            for field in ("seconds", "cpu_seconds")
        }
        for variant in ("baseline", "candidate")
    }
    for field, budget in (
        ("seconds", CONTRACT["wall_seconds_per_key"]),
        ("cpu_seconds", CONTRACT["cpu_seconds_per_key"]),
    ):
        assert medians["candidate"][field] <= budget * len(keys), (
            f"fixed {field} budget exceeded"
        )
    verdict = None
    if (
        shape["retained"] >= 4096
        and shape["requested"] == shape["retained"]
        and shape["mode"] == "overlay"
    ):
        verdict = medians["candidate"]["seconds"] < medians["baseline"]["seconds"]
        assert verdict, "main overlap workload did not produce net local wall benefit"
    return {
        "shape": shape,
        "baseline_layers": len(baseline_ancestry),
        "current_layers": len(current_ancestry),
        "unique_layers": unique_layers,
        "stored_shadow_families": family_count,
        "physical_shadow_rows": 5 * family_count,
        "base_scalar_pattern": "index mod 10: 0..7=(1,1,1), 8=(4,3,2), 9=(32,17,8); dense older layers add depth to occurrence count",
        "retained_base_scalar_counts": dict(
            Counter(str(scalar(index)) for index in range(shape["retained"]))
        ),
        "current_own_family_count": len(own),
        "current_inherited_value_count": shape["retained"] - len(own),
        "oracle_matches": True,
        "negative_control_rejected": negative_rejected,
        "structural": structural,
        "samples": samples,
        "medians": medians,
        "unit_costs": {
            variant: {
                "cpu_seconds_per_key": medians[variant]["cpu_seconds"] / len(keys),
                "wall_seconds_per_key": medians[variant]["seconds"] / len(keys),
                "wall_seconds_per_page": medians[variant]["seconds"]
                / ((len(keys) + 127) // 128),
            }
            for variant in ("baseline", "candidate")
        },
        "python_peak_bytes_per_call": peaks,
        "main_net_wall_benefit": verdict,
        "representative_native_queries": plans,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    parser.add_argument("--allow-mariadb", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--suite",
        choices=("sanity", "main", "dense", "boundary", "smoke"),
        default="sanity",
    )
    parser.add_argument("--baseline-ref", default="041f707")
    args = parser.parse_args()
    if args.backend == "mariadb" and not args.allow_mariadb:
        parser.error("MariaDB creates a disposable test container; add --allow-mariadb")
    if args.output.exists():
        parser.error("output must be a new file")
    historical_helper, baseline_evidence = historical_loader(args.baseline_ref)
    cases: list[dict[str, Any]] = (
        (
            [
                {
                    "retained": k,
                    "requested": k,
                    "baseline_layers": 1,
                    "mode": "overlay",
                    "reuse_percent": 95,
                }
                for k in (1, 127, 128, 129, 257)
            ]
            + [
                {
                    "retained": 129,
                    "requested": 129,
                    "baseline_layers": b,
                    "mode": mode,
                    "reuse_percent": 95,
                }
                for b, mode in (
                    (0, "genesis"),
                    (8, "policy_change"),
                    (17, "compaction"),
                )
            ]
        )
        if args.suite == "sanity"
        else [
            {
                "retained": retained,
                "requested": requested,
                "baseline_layers": depth,
                "mode": "overlay",
                "reuse_percent": reuse,
            }
            for retained, requested, depth, reuse in (
                (4096, 4096, 8, 95),
                (32768, 32768, 16, 95),
                (4096, 4096, 8, 0),
                (32768, 32768, 16, 0),
                (4096, 128, 8, 95),
                (32768, 128, 8, 95),
            )
        ]
    )
    if args.suite in {"dense", "boundary"}:
        cases = [
            {
                "retained": 4096,
                "requested": 4096,
                "baseline_layers": 8,
                "mode": "overlay",
                "reuse_percent": reuse,
                "dense": True,
            }
            for reuse in (95, 0)
        ] + [
            {
                "retained": 128,
                "requested": 128,
                "baseline_layers": 17,
                "mode": "disjoint",
                "reuse_percent": 0,
                "dense": True,
            }
        ]
    if args.suite == "boundary":
        cases = cases[-1:]
    if args.suite == "smoke":
        cases = [
            {
                "retained": 129,
                "requested": 129,
                "baseline_layers": 1,
                "mode": "overlay",
                "reuse_percent": 95,
            }
        ]
    report: dict[str, Any] = {
        "status": "incomplete",
        "backend": args.backend,
        "contract": CONTRACT,
        "invocation": [sys.executable, *sys.argv],
        "git_head": subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip(),
        "source_sha256": hash_source(),
        "baseline": baseline_evidence,
        "cases": [],
        "limits": [
            "Synthetic scalar family/layout fixture, foreign keys disabled; no complete source or run authority. Disjoint 34-layer case is a loader bound, not a valid public overlay layout.",
            "Artist and occurrence values are aggregate scalars, not an inferred source artist/gallery distribution.",
            "Baseline executes the exact historical private helper AST with current shared layout/family dependencies; their unchanged status requires separate source comparison. This is not an end-to-end historical deployment.",
            "Only pair-loader local work; issue/prepare/commit checkpoints, full source aggregation, publication, cleanup and READY are outside scope.",
            "Physical PK probe budget uses accepted native MariaDB query plans; handler operations are not exact rows examined.",
            "SQLite VM values are 100-opcode samples; no universal wall or NAS/pipeline SLO.",
            "8 MiB peak applies to per-call traced Python allocations, not database server or process RSS.",
            "K=1 measures fixed plus one-key cost together; per-key/page fields are normalized measurements, not a fitted zero-key intercept or unmeasured breakeven prediction.",
        ],
    }
    write_report(args.output, report)
    try:
        with databases(args.backend, len(cases)) as connections:
            for connector, shape in zip(connections, cases, strict=True):
                result = measure_case(
                    connector,
                    shape,
                    backend=args.backend,
                    historical_helper=historical_helper,
                )
                report["cases"].append(result)
                write_report(args.output, report)
                print(
                    json.dumps(
                        {
                            "shape": shape,
                            "structural": result["structural"],
                            "medians": result["medians"],
                            "peak": result["python_peak_bytes_per_call"],
                        }
                    ),
                    flush=True,
                )
        assert report["source_sha256"] == hash_source(), (
            "sources changed during measurement"
        )
        report["status"] = "completed"
    except BaseException as error:
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        write_report(args.output, report)
        raise
    write_report(args.output, report)


if __name__ == "__main__":
    main()
