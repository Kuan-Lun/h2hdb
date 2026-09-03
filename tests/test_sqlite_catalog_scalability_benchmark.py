from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_BENCHMARK = _ROOT / "benchmarks" / "sqlite_catalog_scalability.py"
_SMOKE_PUBLICATION_COUNT = 165


def _run_benchmark(
    database_path: Path,
    receipt_path: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(_BENCHMARK),
            "--profile",
            "smoke",
            "--database",
            str(database_path),
            "--receipt",
            str(receipt_path),
        ],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_sqlite_catalog_scalability_smoke_profile(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    receipt_path = tmp_path / "receipt.json"

    completed = _run_benchmark(database_path, receipt_path)

    assert completed.returncode == 0, completed.stderr
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["fixture_mode"] == "manifest-bound-sql"
    assert receipt["fixture_refines_complete_ingest_state_machine"] is False
    assert receipt["profile"] == "smoke"
    assert receipt["schema"]["state"] == "READY"
    assert receipt["schema"]["full_ready_audit_passed"] is True
    assert receipt["fixture"]["creates_cbz_or_artwork_bytes"] is False
    assert receipt["expected"]["publication_count"] == _SMOKE_PUBLICATION_COUNT
    assert receipt["expected"]["artifact_count"] == _SMOKE_PUBLICATION_COUNT
    assert (
        receipt["expected"]["acquisition_descriptor_count"] == _SMOKE_PUBLICATION_COUNT
    )
    assert receipt["expected"]["artifact_blob_count"] == 1
    assert receipt["actual_database_counts"] == {
        label: receipt["expected"][label] for label in receipt["actual_database_counts"]
    }
    assert receipt["expected"]["search"]["publication_count"] > 0
    assert receipt["expected"]["search"]["cursor_page_gids"]
    assert all(receipt["expected"]["facets"].values())

    first = receipt["timing"]["catalog_bundle_first_after_build"]
    warm = receipt["timing"]["catalog_bundle_warm"]
    cursor = receipt["timing"]["catalog_bundle_cursor_page"]
    reference = receipt["timing"]["catalog_separate_facade_reference"]
    memory = receipt["timing"]["catalog_bundle_memory_probe"]
    assert first["result_sha256"] == warm["result_sha256"]
    assert first["result_sha256"] == reference["result_sha256"]
    assert first["result_sha256"] == memory["result_sha256"]
    assert first["connection_count"] == 1
    assert first["read_transaction_count"] == 2
    assert first["logical_query_count"] <= 64
    assert sum(first["query_class_counts"].values()) == first["logical_query_count"]
    assert (
        sum(shape["count"] for shape in first["query_shapes"])
        == first["logical_query_count"]
    )
    assert cursor["returned_publication_count"] > 0
    assert reference["connection_count"] == 4
    assert reference["read_transaction_count"] == 8
    assert memory["python_traced_peak_bytes"] > 0
    assert memory["result_json_bytes"] > 0

    assert receipt["source_provenance"]["project_version"] == receipt["core_version"]
    assert len(receipt["source_provenance"]["source_manifest_sha256"]) == 64
    assert len(receipt["fixture_contract_sha256"]) == 64
    assert receipt["comparability"]["fixture_contract_digest_includes_paths"] is False
    assert database_path.stat().st_size == receipt["database"]["size_bytes"]
    assert (
        hashlib.sha256(database_path.read_bytes()).hexdigest()
        == receipt["database"]["sha256"]
    )
    with sqlite3.connect(
        f"file:{database_path}?mode=ro",
        uri=True,
    ) as connection:
        allocators = dict(
            connection.execute(
                "SELECT stream, next_revision FROM operational_revision_allocators"
            )
        )
        source_revision = connection.execute(
            "SELECT source_revision FROM catalog_source_revision_descriptors"
        ).fetchone()
        catalog_revision = connection.execute(
            "SELECT revision FROM catalog_revision_descriptors"
        ).fetchone()
    assert allocators == {"CATALOG": 2, "SOURCE": 2}
    assert source_revision == (1,)
    assert catalog_revision == (1,)


def test_sqlite_catalog_scalability_rejects_existing_targets(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    receipt_path = tmp_path / "receipt.json"
    database_path.write_bytes(b"caller-owned")

    completed = _run_benchmark(database_path, receipt_path)

    assert completed.returncode != 0
    assert "FileExistsError" in completed.stderr
    assert database_path.read_bytes() == b"caller-owned"
    assert not receipt_path.exists()


def test_sqlite_catalog_scalability_does_not_create_target_parents(
    tmp_path: Path,
) -> None:
    target_parent = tmp_path / "missing"

    completed = _run_benchmark(
        target_parent / "catalog.sqlite3",
        target_parent / "receipt.json",
    )

    assert completed.returncode != 0
    assert "FileNotFoundError" in completed.stderr
    assert not target_parent.exists()
