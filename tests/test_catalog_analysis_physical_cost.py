"""Native selectivity with fixed active inputs and unrelated analysis history."""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import pytest
from test_catalog_refinement_physical_cost import _identity, _measure
from test_catalog_refinement_runtime import (
    _insert_analysis_policy,
    _insert_exact_canonical_payload,
    _ReadRecorder,
)
from vnext_test_database import (
    DatabaseFactory,
    assert_foreign_key_integrity,
    connector_backend,
    open_generated_database,
)

from h2hdb import catalog_refinement, vnext_identity
from h2hdb.sql_connector import SQLConnector


def _seed(
    connector: SQLConnector, depth: int, unrelated: int, *, impact: bool
) -> bytes:
    """Legal relation subgraph; deliberately no claim of complete READY state."""
    active = list(range(1, depth + 2))
    ids = [*active, *range(100, 100 + unrelated)]
    with connector.transaction():
        root = _insert_exact_canonical_payload(
            connector, domain="source_root_v1", payload=b"physical-cost"
        )
        scope = vnext_identity.source_scope_key("filesystem", root, 1)
        connector.execute(
            "INSERT INTO catalog_source_scopes VALUES (%s, %s, %s, 1)",
            (scope, b"filesystem", root),
        )
        connector.execute("INSERT INTO catalog_manifest_policies VALUES (1, 1, 1)")
        _insert_analysis_policy(connector, 1)
        connector.execute_many(
            "INSERT INTO catalog_source_build_descriptor VALUES (%s, %s, 1, 0)",
            [(_identity(b"b", i), scope) for i in ids],
        )
        connector.execute_many(
            "INSERT INTO catalog_analysis_run_descriptor VALUES (%s, %s, 1, %s, 0)",
            [(_identity(b"a", i), _identity(b"b", i), b"m" * 32) for i in ids],
        )
        connector.execute_many(
            "INSERT INTO catalog_analysis_run_states VALUES (%s, 'COMPLETE')",
            [(_identity(b"a", i),) for i in ids],
        )
        connector.execute_many(
            "INSERT INTO catalog_analysis_run_completed_ats VALUES (%s, 1)",
            [(_identity(b"a", i),) for i in ids],
        )
        ancestry = [
            (_identity(b"a", i), j, _identity(b"a", k))
            for offset, i in enumerate(active)
            for j, k in enumerate(active[offset:])
        ]
        ancestry += [
            (_identity(b"a", i), 0, _identity(b"a", i)) for i in ids[len(active) :]
        ]
        connector.execute_many(
            "INSERT INTO catalog_analysis_state_ancestry VALUES (%s, %s, %s)", ancestry
        )
        if impact:
            content = _insert_exact_canonical_payload(
                connector, domain="effective_content_v1", payload=b"physical-content"
            )
            connector.execute("INSERT INTO catalog_gallery_gid_identities VALUES (17)")
            for gallery in (1, 2):
                name = f"gallery-{gallery}".encode()
                locator = _insert_exact_canonical_payload(
                    connector, domain="source_relative_locator_v1", payload=name
                )
                connector.execute(
                    "INSERT INTO catalog_source_locator_identity VALUES (%s, %s)",
                    (locator, name),
                )
                connector.execute(
                    "INSERT INTO catalog_source_gallery_name_gids VALUES (%s, 17)",
                    (name,),
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_identities VALUES (%s, %s, %s, %s)",
                    (gallery, bytes((gallery,)) * 32, scope, locator),
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_source_name_accesses VALUES (%s, %s)",
                    (gallery, name),
                )
            pairs = [(_identity(b"a", i), gallery) for i in ids for gallery in (1, 2)]
            connector.execute_many(
                "INSERT INTO catalog_analysis_impacted_galleries VALUES (%s, %s)", pairs
            )
            connector.execute_many(
                "INSERT INTO catalog_a_impacted_content_provenance VALUES (%s, %s, %s)",
                [(a, g, content) for a, g in pairs],
            )
            connector.execute_many(
                "INSERT INTO catalog_analysis_impacted_content VALUES (%s, %s, 1)",
                [(_identity(b"a", i), content) for i in ids],
            )
            connector.execute_many(
                "INSERT INTO catalog_a_impacted_gid_provenance_storage VALUES (%s, %s)",
                pairs,
            )
            connector.execute_many(
                "INSERT INTO catalog_analysis_impacted_gid_storage VALUES (%s, 17)",
                [(_identity(b"a", i),) for i in ids],
            )
    assert_foreign_key_integrity(connector)
    return _identity(b"a", 1)


def _check(
    connector: SQLConnector,
    sql: str,
    data: tuple[Any, ...],
    expected: list[tuple[Any, ...]],
    *,
    budget: int,
    unrelated: int,
) -> dict[str, Any]:
    # Exact 16-byte operands: HEX equality preserves binary equality without
    # allowing the outer analysis-id index seek. Correlated joins stay intact.
    prefix, suffix = sql.split("analysis_id = %s", 1)
    alias, before = prefix.rsplit(" ", 1)
    assert before.endswith(".")
    degraded = alias + " HEX(" + before + "analysis_id) = HEX(%s)" + suffix
    production_rows, production = _measure(connector, sql, data)
    mutant_rows, mutant = _measure(connector, degraded, data)
    assert production_rows == mutant_rows == expected
    result = {"production": production, "full_scan": mutant, "budget": budget}
    assert production <= budget, result
    if unrelated:
        assert mutant > budget, result
    return result


@pytest.mark.deep
@pytest.mark.parametrize("depth", (0, 15, 16))
@pytest.mark.parametrize("unrelated", (0, 4096))
def test_ancestry_native_work_depends_on_depth_not_unrelated_owners(
    database_factory: DatabaseFactory, depth: int, unrelated: int, tmp_path: Path
) -> None:
    evidence = []
    with closing(open_generated_database(database_factory.config())) as connector:
        analysis = _seed(connector, depth, unrelated, impact=False)
        budget = 2688 if connector_backend(connector) == "sqlite" else 184
        with connector.read_transaction():
            recorder = _ReadRecorder(connector)
            assert catalog_refinement._analysis_ancestry(
                cast(Any, recorder), analysis, policy_id=1, detail="native-cost"
            ) == tuple(_identity(b"a", i) for i in range(1, depth + 2))
            assert len(recorder.reads) == 1
            sql, data, _ = recorder.reads[0]
            expected = [
                (i, _identity(b"a", i + 1), 1, "COMPLETE") for i in range(depth + 1)
            ]
            for _ in range(3):
                evidence.append(
                    _check(
                        connector,
                        sql,
                        data,
                        expected,
                        budget=budget,
                        unrelated=unrelated,
                    )
                )
    (tmp_path / "ancestry-work.json").write_text(json.dumps(evidence))


@pytest.mark.deep
@pytest.mark.parametrize("unrelated", (0, 4096))
def test_minimum_witness_native_work_uses_active_key_and_provenance(
    database_factory: DatabaseFactory, unrelated: int, tmp_path: Path
) -> None:
    evidence = []
    with closing(open_generated_database(database_factory.config())) as connector:
        analysis = _seed(connector, 0, unrelated, impact=True)
        budget = 1280 if connector_backend(connector) == "sqlite" else 128
        with connector.read_transaction():
            recorder = _ReadRecorder(connector)
            catalog_refinement._validate_impacted_key_families(
                cast(Any, recorder), analysis
            )
            queries = [
                (sql, data)
                for sql, data, _ in recorder.reads
                if "smaller.gallery_id < candidate.gallery_id" in sql
            ]
            assert len(queries) == 2
            for _ in range(3):
                for sql, data in queries:
                    evidence.append(
                        _check(
                            connector, sql, data, [], budget=budget, unrelated=unrelated
                        )
                    )
    (tmp_path / "impact-work.json").write_text(json.dumps(evidence))
