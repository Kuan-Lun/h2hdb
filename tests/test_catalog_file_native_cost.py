"""MariaDB counterpart to the existing SQLite FILE stream VM budget test."""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from test_ingest_role_cost_probe import probe as probe
from vnext_test_database import open_generated_database

from h2hdb import CoreConfig
from h2hdb.mariadb_connector import MariaDBConnector


@pytest.mark.deep
@pytest.mark.mariadb
@pytest.mark.backend_specific(
    backend="mariadb",
    reason="Measures native session Handler counters and ANALYZE row visits; SQLite has the corresponding VM budget test.",
)
@pytest.mark.parametrize("files", (4096, 32768))
@pytest.mark.parametrize("regime", ("distinct", "duplicate", "metadata"))
def test_file_pages_bound_native_handler_work_and_reject_full_order_scan(
    probe: ModuleType,
    mariadb_config: CoreConfig,
    files: int,
    regime: str,
    tmp_path: Path,
) -> None:
    facts = probe.file_facts(probe.Shape(files, regime))
    expected = probe.expected_stream_rows(facts)
    evidence: list[dict[str, Any]] = []
    with closing(open_generated_database(mariadb_config)) as connector:
        assert isinstance(connector, MariaDBConnector)
        probe.seed_fixture(connector, facts)
        with connector.read_transaction():
            try:
                for cycle in range(3):
                    captured, _ = probe.capture_validator(connector, facts)
                    for kind, queries in captured.items():
                        rejected = False
                        budget = probe.seek_budget(kind, facts)
                        assert budget == 1064
                        for index in set(probe.sample_positions(queries).values()):
                            query = queries[index]
                            page = expected[kind][index * 128 : (index + 1) * 128]
                            production = probe.profile_query(
                                connector,
                                query.sql,
                                query.parameters,
                                page,
                                repetitions=1,
                                budget=budget,
                            )
                            sql, parameters = probe.degraded_order(
                                query.sql, query.parameters
                            )
                            mutant = probe.profile_query(
                                connector,
                                sql,
                                parameters,
                                page,
                                repetitions=1,
                                budget=budget,
                            )
                            rejected |= mutant["verdict"] == "violated"
                            evidence.append(
                                {
                                    "cycle": cycle,
                                    "stream": kind,
                                    "page": index,
                                    "production": production,
                                    "full_order_scan": mutant,
                                }
                            )
                            # profile_query fails closed on hidden ICP/rowid filters
                            # instead of treating filtered row counters as a bound.
                            assert production["verdict"] == "observed_within_budget", (
                                evidence[-1]
                            )
                        if len(expected[kind]) > 128:
                            assert rejected, (kind, files, regime)
            finally:
                (tmp_path / "file-native-work.json").write_text(
                    json.dumps(
                        {"files": files, "regime": regime, "measurements": evidence},
                        default=repr,
                        indent=2,
                    )
                )
