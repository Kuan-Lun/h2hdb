"""Measure complete cleanup with unchanged retained file/content populations.

Only disposable generated databases are accepted. Twenty observations come from
the public pipeline; native file-family extensions keep foreign keys enabled,
but deliberately do not update observation manifests, so this is a cost fixture,
not full READY semantic evidence. Each trial adds four abandoned canonical
allocations in distinct shards and drains them through the public facade.

Fixed budgets, chosen before this experiment: medium (100k) candidate median
<=75% of baseline, large (1m) <=60%; <=50% of the two eligibility probe calls.
At 1k, <=25ms additional whole-drain time is acceptable. Baseline deliberately
discards prior evidence and must fail the reduced-query-work budget. The
million-root case is explicitly opt-in because FK-valid fixture seeding is slow.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from collections.abc import Callable
from contextlib import ExitStack, closing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

from ingest_growth_cleanup_probe import database  # noqa: E402 - local fixture paths
from vnext_canonical_value_fixtures import (  # noqa: E402 - local fixture paths
    seed_canonical_allocation,
)
from vnext_fault_harness import open_connector  # noqa: E402 - local fixture paths
from vnext_pipeline import (  # noqa: E402 - local fixture paths
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    gallery,
    initialize_database,
    run_ingest_turn,
)

from h2hdb import VNextIngestFacade  # noqa: E402 - checkout source
from h2hdb._cleanup.eligibility import (  # noqa: E402 - checkout source
    CurrentOnlyEligibilityProof,
)
from h2hdb._cleanup.targets import resources  # noqa: E402 - checkout source
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout source
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - checkout source

_TABLES = (
    "catalog_content_blobs",
    "catalog_file_name_identities",
    "catalog_gallery_observation_file_anchors",
    "catalog_gallery_observation_file_file_sha256s",
)
_PROBES = (
    "_next_content_blob_candidate_shard",
    "_next_file_name_candidate_shard",
)


def _counts(connector: SQLConnector) -> dict[str, int]:
    with connector.read_transaction():
        return {
            table: int(connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
            for table in _TABLES
        }


def _seed(
    connector: SQLConnector,
    owners: list[tuple[Any, ...]],
    start: int,
    end: int,
) -> None:
    for first in range(start, end, 2000):
        content: list[tuple[Any, ...]] = []
        names: list[tuple[Any, ...]] = []
        anchors: list[tuple[Any, ...]] = []
        hashes: list[tuple[Any, ...]] = []
        for number in range(first, min(first + 2000, end)):
            digest = sha256(number.to_bytes(8, "big")).digest()
            content.append((digest, 65536))
            names.append((digest, str(number).encode()))
            copies = 1 if number % 100 < 80 else (3 if number % 100 < 98 else 20)
            for owner in owners[:copies]:
                anchors.append((*owner, digest))
                hashes.append((*owner, digest, digest))
        with connector.transaction():
            connector.execute_many(
                "INSERT INTO catalog_content_blobs VALUES (%s, %s)", content
            )
            connector.execute_many(
                "INSERT INTO catalog_file_name_identities VALUES (%s, %s)", names
            )
            connector.execute_many(
                "INSERT INTO catalog_gallery_observation_file_anchors "
                "VALUES (%s, %s, %s)",
                anchors,
            )
            connector.execute_many(
                "INSERT INTO catalog_gallery_observation_file_file_sha256s "
                "VALUES (%s, %s, %s, %s)",
                hashes,
            )
        if (first + 2000) % 100_000 == 0:
            print(json.dumps({"seeded_roots": first + 2000}), flush=True)


def _pending(connector: SQLConnector) -> tuple[bytes, ...]:
    roots = tuple(bytes((32 + index * 48,)) + bytes(31) for index in range(4))
    for root in roots:
        seed_canonical_allocation(
            connector,
            value_sha256=root,
            digest_domain=b"source_root_v1",
            byte_count=1,
            allocated_at=1,
        )
    return roots


def _trial(
    connector: SQLConnector, facade: VNextIngestFacade, variant: str
) -> dict[str, Any]:
    roots = _pending(connector)
    retained = _counts(connector)
    calls: Counter[str] = Counter()

    def measured(
        name: str, original: Callable[[VNextUnitOfWork], int | None]
    ) -> Callable[[VNextUnitOfWork], int | None]:
        def probe(work: VNextUnitOfWork) -> int | None:
            calls[name] += 1
            return original(work)

        return probe

    with ExitStack() as patches:
        for name in _PROBES:
            patches.enter_context(
                patch.object(resources, name, measured(name, getattr(resources, name)))
            )
        if variant == "baseline":
            original = CurrentOnlyEligibilityProof.under_validated_gate
            patches.enter_context(
                patch.object(
                    CurrentOnlyEligibilityProof,
                    "under_validated_gate",
                    side_effect=lambda lease, cutoff, _prior: original(
                        lease, cutoff, None
                    ),
                )
            )
        started = perf_counter()
        attempts = drain_maintenance(facade)
        elapsed = perf_counter() - started
    if _counts(connector) != retained:
        raise RuntimeError("cleanup changed retained file/content cardinalities")
    with connector.read_transaction():
        for root in roots:
            if connector.fetch_one(
                "SELECT value_sha256 FROM catalog_canonical_value_allocation_anchors "
                "WHERE value_sha256 = %s",
                (root,),
            ):
                raise RuntimeError("cleanup DONE retained an abandoned root")
    return {
        "variant": variant,
        "seconds": elapsed,
        "eligibility_calls": dict(calls),
        "total_eligibility_calls": sum(calls.values()),
        "progressed_attempts": attempts,
        "outcome": "DONE",
        "pending_roots_deleted": len(roots),
        "retained_cardinalities_unchanged": True,
    }


def run(backend: str, sizes: list[int], repetitions: int, output: Path) -> None:
    report: dict[str, Any] = {
        "contract": __doc__,
        "backend": backend,
        "reference_multiplicities": {"80_percent": 1, "18_percent": 3, "2_percent": 20},
        "parent_observations": 20,
        "cases": [],
    }
    with TemporaryDirectory(prefix="h2hdb-eligibility-cost-") as directory:
        with database(backend, Path(directory)) as config:
            initialize_database(config)
            source = MemorySource([gallery(gid) for gid in range(1, 21)])
            with (
                VNextIngestFacade(config) as facade,
                closing(open_connector(config)) as connector,
            ):
                run_ingest_turn(facade, source=source, library=MemoryLibrary(source))
                drain_maintenance(facade)
                with connector.read_transaction():
                    owners = connector.fetch_all(
                        "SELECT gallery_id, observation_id "
                        "FROM catalog_gallery_observation_allocations "
                        "ORDER BY gallery_id, observation_id LIMIT 20"
                    )
                if len(owners) != 20:
                    raise RuntimeError("fixture omitted observation parents")
                previous = 0
                for size in sizes:
                    started = perf_counter()
                    _seed(connector, owners, previous, size)
                    previous = size
                    case: dict[str, Any] = {
                        "native_retained_roots_per_target": size,
                        "reference_rows_added_per_target": size * 174 // 100,
                        "pending_canonical_roots_per_trial": 4,
                        "seed_seconds": perf_counter() - started,
                        "actual_cardinalities": _counts(connector),
                        "samples": [],
                    }
                    for repetition in range(repetitions):
                        order = ("baseline", "candidate")
                        for variant in order if repetition % 2 == 0 else order[::-1]:
                            case["samples"].append(_trial(connector, facade, variant))
                    medians = {
                        variant: statistics.median(
                            sample["seconds"]
                            for sample in case["samples"]
                            if sample["variant"] == variant
                        )
                        for variant in ("baseline", "candidate")
                    }
                    query_medians = {
                        variant: statistics.median(
                            sample["total_eligibility_calls"]
                            for sample in case["samples"]
                            if sample["variant"] == variant
                        )
                        for variant in ("baseline", "candidate")
                    }
                    ratio = medians["candidate"] / medians["baseline"]
                    time_passed = (
                        ratio <= (0.60 if size >= 1_000_000 else 0.75)
                        if size >= 100_000
                        else medians["candidate"] <= medians["baseline"] + 0.025
                    )
                    case.update(
                        medians=medians,
                        ratio=ratio,
                        query_medians=query_medians,
                        time_budget_passed=time_passed,
                        query_budget_passed=(
                            query_medians["candidate"] <= 4
                            and query_medians["candidate"]
                            <= query_medians["baseline"] / 2
                        ),
                        negative_cost_control_rejected=query_medians["baseline"] > 4,
                    )
                    report["cases"].append(case)
                    output.write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps(case), flush=True)
    if any(
        not case["time_budget_passed"]
        or not case["query_budget_passed"]
        or not case["negative_cost_control_rejected"]
        for case in report["cases"]
    ):
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--million-roots", action="store_true")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    sizes = [1000, 100_000, 1_000_000] if args.million_roots else [1000, 100_000]
    run(args.backend, sizes, args.repetitions, args.output)


if __name__ == "__main__":
    main()
