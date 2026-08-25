from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    AnalysisFamilyPartialError,
)
from h2hdb.vnext_analysis_overlay_family import (
    AnalysisContentOwnerCandidateShadowFamily,
    AnalysisContentOwnerShadowFamily,
    AnalysisFileHashDecisionShadowFamily,
    ensure_analysis_content_owner_candidate_shadow_family,
    ensure_analysis_content_owner_shadow_family,
    ensure_analysis_file_hash_decision_shadow_family,
    load_analysis_impacted_content_key_family,
    load_analysis_impacted_gid_key_family,
    record_analysis_impacted_content_provenance,
    record_analysis_impacted_content_provenance_page,
    record_analysis_impacted_gid_provenance_page,
    require_complete_analysis_impacted_content_keyspace,
    require_exact_analysis_impacted_content_provenance_page,
)


def _database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    connector.execute("PRAGMA foreign_keys = OFF")
    return connector


def _insert_tables(trace: list[str], registered: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        table
        for statement in trace
        for table in registered
        if statement.startswith(f"INSERT INTO {table} ")
    )


def test_shadow_families_insert_narrow_file_family_and_atomic_wide_rows(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-shadow-families.sqlite3")
    try:
        analysis = b"a" * 16
        file_sha256 = b"f" * 32
        content_sha256 = b"c" * 32
        traced: list[str] = []
        connector.connection.set_trace_callback(traced.append)
        ensure_analysis_file_hash_decision_shadow_family(
            connector,
            AnalysisFileHashDecisionShadowFamily(
                analysis,
                file_sha256,
                7,
                3,
                2,
            ),
        )
        ensure_analysis_content_owner_candidate_shadow_family(
            connector,
            AnalysisContentOwnerCandidateShadowFamily(
                analysis,
                11,
                content_sha256,
                1,
                19,
                23,
            ),
        )
        ensure_analysis_content_owner_shadow_family(
            connector,
            AnalysisContentOwnerShadowFamily(analysis, content_sha256, 11),
        )
        connector.connection.set_trace_callback(None)
        expected = (
            "catalog_a_file_decision_shadow_anchors",
            "catalog_a_file_decision_shadow_occurrences",
            "catalog_a_file_decision_shadow_artists",
            "catalog_a_file_decision_shadow_gallery_artist_max",
            "catalog_a_file_decision_shadow_seals",
            "catalog_analysis_content_owner_candidate_shadows",
            "catalog_analysis_content_owner_shadows",
        )
        assert _insert_tables(traced, expected) == expected
    finally:
        connector.close()


def test_provenance_page_uses_one_bounded_preflight_and_one_replay_query(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-provenance-page.sqlite3")
    try:
        analysis = b"p" * 16
        contents = tuple(index.to_bytes(32, "big") for index in range(1, 257))
        first, second = contents[:2]
        entries = tuple(enumerate(contents, start=1))
        with patch.object(
            connector,
            "fetch_all",
            wraps=connector.fetch_all,
        ) as reads:
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=entries,
            )
        assert reads.call_count == 1
        preflight_query, preflight_parameters = reads.call_args.args
        assert preflight_query.count("%s") == 259
        assert len(preflight_parameters) == 259
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_a_impacted_content_provenance "
            "WHERE analysis_id = %s",
            (analysis,),
        ) == (256,)
        first_family = load_analysis_impacted_content_key_family(
            connector,
            analysis_id=analysis,
            content_sha256=first,
        )
        assert first_family is not None
        assert first_family.witness_gallery_id == 1

        with patch.object(
            connector,
            "fetch_all",
            wraps=connector.fetch_all,
        ) as reads:
            require_exact_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                after_gallery_id=None,
                through_gallery_id=256,
                expected=entries,
            )
        assert reads.call_count == 1

        later = ((257, first), (257, second))
        with patch.object(
            connector,
            "fetch_all",
            wraps=connector.fetch_all,
        ) as reads:
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=later,
            )
        assert reads.call_count == 1
        second_family = load_analysis_impacted_content_key_family(
            connector,
            analysis_id=analysis,
            content_sha256=second,
        )
        assert second_family is not None
        assert second_family.witness_gallery_id == 2
    finally:
        connector.close()


def test_provenance_preflight_candidates_are_driven_by_typed_storage(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-provenance-typed-keys.sqlite3")
    try:
        analysis = b"q" * 16
        content = bytes(range(0x80, 0xA0))
        with patch.object(
            connector,
            "fetch_all",
            wraps=connector.fetch_all,
        ) as reads:
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((1, content),),
            )
        assert reads.call_count == 1
        content_query, content_parameters = reads.call_args.args
        assert "FROM catalog_analysis_impacted_content AS impacted" in content_query
        assert "SELECT %s AS key_value" not in content_query
        assert content_query.count("impacted.content_sha256 IN (%s)") == 1
        assert content_parameters[1:-1] == (analysis, content)

        connector.execute(
            "INSERT INTO catalog_gallery_source_name_accesses "
            "(gallery_id, source_gallery_name) VALUES (1, %s)",
            (b"gallery-1",),
        )
        connector.execute(
            "INSERT INTO catalog_source_gallery_name_gids "
            "(source_gallery_name, gid) VALUES (%s, 17)",
            (b"gallery-1",),
        )
        with patch.object(
            connector,
            "fetch_all",
            wraps=connector.fetch_all,
        ) as reads:
            record_analysis_impacted_gid_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((1, 17),),
            )
        assert reads.call_count == 2
        gid_query, gid_parameters = reads.call_args.args
        assert "FROM catalog_analysis_impacted_gid AS impacted" in gid_query
        assert "SELECT %s AS key_value" not in gid_query
        assert gid_query.count("impacted.gid IN (%s)") == 1
        assert gid_parameters[1:-1] == (analysis, 17)

        content_family = load_analysis_impacted_content_key_family(
            connector,
            analysis_id=analysis,
            content_sha256=content,
        )
        gid_family = load_analysis_impacted_gid_key_family(
            connector,
            analysis_id=analysis,
            gid=17,
        )
        assert content_family is not None and content_family.witness_gallery_id == 1
        assert gid_family is not None and gid_family.witness_gallery_id == 1
    finally:
        connector.close()


def test_exact_provenance_replay_is_zero_dml_and_missing_witness_tuple_fails(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "analysis-provenance-corruption.sqlite3")
    try:
        analysis = b"r" * 16
        content = b"c" * 32
        record_analysis_impacted_content_provenance(
            connector,
            analysis_id=analysis,
            gallery_id=1,
            content_sha256=content,
        )
        with patch.object(
            connector,
            "execute",
            side_effect=AssertionError("exact provenance replay attempted DML"),
        ):
            family, created = record_analysis_impacted_content_provenance(
                connector,
                analysis_id=analysis,
                gallery_id=1,
                content_sha256=content,
            )
        assert not created and family.witness_gallery_id == 1

        connector.execute(
            "INSERT INTO catalog_a_impacted_content_provenance "
            "(analysis_id, gallery_id, content_sha256) VALUES (%s, 2, %s)",
            (analysis, content),
        )
        connector.execute(
            "DELETE FROM catalog_a_impacted_content_provenance "
            "WHERE analysis_id = %s AND gallery_id = 1 AND content_sha256 = %s",
            (analysis, content),
        )
        with pytest.raises(AnalysisFamilyPartialError):
            require_exact_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                after_gallery_id=None,
                through_gallery_id=2,
                expected=((2, content),),
            )
    finally:
        connector.close()


def test_fresh_page_rejects_existing_or_future_provenance(tmp_path: Path) -> None:
    connector = _database(tmp_path / "analysis-provenance-future.sqlite3")
    try:
        analysis = b"s" * 16
        content = b"d" * 32
        record_analysis_impacted_content_provenance(
            connector,
            analysis_id=analysis,
            gallery_id=1,
            content_sha256=content,
        )
        connector.execute(
            "INSERT INTO catalog_a_impacted_content_provenance "
            "(analysis_id, gallery_id, content_sha256) VALUES (%s, 9, %s)",
            (analysis, content),
        )
        with pytest.raises(AnalysisFamilyCollisionError, match="future provenance"):
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((2, content),),
            )
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("table", "columns", "values"),
    (
        (
            "catalog_analysis_impacted_content",
            "analysis_id, content_sha256, witness_gallery_id",
            lambda analysis, content: (analysis, content, 1),
        ),
        (
            "catalog_a_impacted_content_provenance",
            "analysis_id, gallery_id, content_sha256",
            lambda analysis, content: (analysis, 1, content),
        ),
    ),
)
def test_terminal_keyspace_rejects_orphan_atomic_or_provenance_row(
    tmp_path: Path,
    table: str,
    columns: str,
    values: Any,
) -> None:
    connector = _database(tmp_path / f"analysis-orphan-{table}.sqlite3")
    try:
        analysis = b"t" * 16
        content = b"k" * 32
        connector.execute(
            f"INSERT INTO {table} ({columns}) VALUES ("
            + ", ".join("%s" for _column in columns.split(", "))
            + ")",
            values(analysis, content),
        )
        with pytest.raises(AnalysisFamilyPartialError, match="terminal keyspace"):
            require_complete_analysis_impacted_content_keyspace(
                connector,
                analysis_id=analysis,
            )
    finally:
        connector.close()


def test_terminal_keyspace_rejects_nonminimum_witness(tmp_path: Path) -> None:
    connector = _database(tmp_path / "analysis-nonminimum-witness.sqlite3")
    try:
        analysis = b"u" * 16
        content = b"m" * 32
        record_analysis_impacted_content_provenance(
            connector,
            analysis_id=analysis,
            gallery_id=2,
            content_sha256=content,
        )
        connector.execute(
            "INSERT INTO catalog_a_impacted_content_provenance "
            "(analysis_id, gallery_id, content_sha256) VALUES (%s, 1, %s)",
            (analysis, content),
        )
        with pytest.raises(AnalysisFamilyPartialError, match="nonminimum"):
            require_complete_analysis_impacted_content_keyspace(
                connector,
                analysis_id=analysis,
            )
    finally:
        connector.close()


def test_exact_provenance_page_exposes_the_257th_extra_row(tmp_path: Path) -> None:
    connector = _database(tmp_path / "analysis-provenance-257.sqlite3")
    try:
        analysis = b"v" * 16
        content = b"n" * 32
        first = tuple((gallery, content) for gallery in range(1, 257))
        record_analysis_impacted_content_provenance_page(
            connector,
            analysis_id=analysis,
            entries=first,
        )
        record_analysis_impacted_content_provenance_page(
            connector,
            analysis_id=analysis,
            entries=((257, content),),
        )
        with pytest.raises(AnalysisFamilyCollisionError, match="exact page"):
            require_exact_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                after_gallery_id=None,
                through_gallery_id=257,
                expected=first,
            )
    finally:
        connector.close()
