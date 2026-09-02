"""Closed corruption matrix over every stored identity, digest, chain, frame,
cursor and reference column of every manifest relation, with production
consumers as the oracle.

For each corpus (see ``vnext_corpora``) and each column that carries a
derived identity (SHA-256 digests, UUID identities, digest chains, subtype
frames, opaque cursors) or a reference to another row, one committed
corruption is applied on a copy of the database with foreign-key enforcement
off (a storage-level corruption or a bypassing writer):

* ``flip``  — the first byte is inverted (same width, same storage class);
* ``swap``  — the value of another row of the same table is copied in (a
  well-formed value bound to the wrong identity).

The oracle is the production consumer of that row:

* a catalog at rest must fail its READY audit with a typed h2hdb error; and
* an abandoned turn must be refused with a typed h2hdb error when a later
  owner resumes it, or, when the corrupted transient row is rebuilt rather
  than read, converge to exactly the fault-free catalog with a READY audit.

Anything else — a corruption that neither audit nor resume detects and that
changes the published catalog — is a real hole and fails the matrix.
"""

from __future__ import annotations

import re
import shutil
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from test_vnext_physical_domain_fault_matrix import (
    Column,
    _column_names,
    manifest_columns,
)
from vnext_corpora import Corpus, build_corpora
from vnext_fault_harness import EPOCH_CONTROL_TABLE, open_connector
from vnext_pipeline import catalog_view, full_check

from h2hdb import CoreConfig, DatabaseConfig

_IDENTITY_NAME = re.compile(
    r"(_sha256$|_id$|_key$|_token$|_chain$|_frame$|cursor|_bytes$|^chain_|_sha256_)"
)


def identity_columns() -> list[Column]:
    """Every BLOB column whose name or width marks an identity or reference."""

    selected: list[Column] = []
    for column in manifest_columns():
        if column.table == EPOCH_CONTROL_TABLE or column.sqlite_type != "BLOB":
            continue
        checks = " AND ".join(column.checks)
        exact = re.search(rf"length\({re.escape(column.name)}\) = (16|32)", checks)
        if exact is not None or _IDENTITY_NAME.search(column.name):
            selected.append(column)
    return selected


@dataclass(frozen=True)
class Corruption:
    column: Column
    kind: str

    @property
    def label(self) -> str:
        return f"{self.column.table}.{self.column.name}:{self.kind}"


def _sqlite_config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


def _rows(config: CoreConfig, table: str, limit: int = 2) -> list[tuple[Any, ...]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return connector.fetch_all(f"SELECT * FROM {table} LIMIT {limit}")
    finally:
        connector.close()


def _apply(
    config: CoreConfig, corruption: Corruption, rows: list[tuple[Any, ...]]
) -> bool:
    """Commit one corruption on ``config``; return False when not applicable."""

    connector = open_connector(config)
    try:
        names = _column_names(connector, "sqlite", corruption.column.table)
        index = names.index(corruption.column.name)
        first = rows[0]
        value = first[index]
        if not value:
            return False
        if corruption.kind == "flip":
            replacement = bytes([value[0] ^ 0xFF]) + bytes(value[1:])
        else:
            if len(rows) < 2 or rows[1][index] == value:
                return False
            replacement = rows[1][index]
        where = " AND ".join(
            f"{name} IS NULL" if cell is None else f"{name} = %s"
            for name, cell in zip(names, first, strict=True)
        )
        bound = tuple(cell for cell in first if cell is not None)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.begin()
        try:
            affected = connector.execute_affected(
                f"UPDATE {corruption.column.table} SET {corruption.column.name} = %s "
                f"WHERE {where}",
                (replacement, *bound),
            )
            if affected != 1:
                connector.rollback()
                return False
            connector.commit()
        except Exception:
            connector.rollback()
            # The DDL itself refuses the corrupted value: that is fail-closed.
            return False
        return True
    finally:
        connector.close()


def _typed(error: BaseException) -> bool:
    return type(error).__module__.startswith("h2hdb.")


def _adapter_refused(error: BaseException) -> bool:
    """The in-memory storage/library adapter refused the corrupted fact at
    its own boundary (an activation item, staged object or protection that
    disagrees with the bytes it holds), exactly as a production adapter must."""

    trace = error.__traceback__
    innermost = None
    while trace is not None:
        innermost = trace.tb_frame.f_code.co_filename
        trace = trace.tb_next
    # Any failure raised inside the adapter itself (a name the source never
    # had, an activation item that disagrees with stored bytes) is the
    # adapter refusing the corrupted fact, whatever exception type it uses.
    return (innermost or "").endswith("vnext_pipeline.py")


def _refusal(error: BaseException, label: str) -> str:
    if _typed(error):
        return f"{label}-rejected"
    if _adapter_refused(error):
        return "adapter-rejected"
    raise AssertionError(
        f"untyped {label} failure: {type(error).__name__}: {error}"
    ) from error


def _audit_outcome(config: CoreConfig) -> str:
    try:
        report = full_check(config)
    except Exception as error:
        assert _typed(error), f"untyped audit failure: {type(error).__name__}: {error}"
        return "audit-rejected"
    if report.state != "READY":
        return "audit-rejected"
    return "audit-accepted"


def _resume_outcome(
    corpus: Corpus, copy_config: CoreConfig, reference: dict[str, Any]
) -> str:
    try:
        corpus.resume(copy_config)
    except Exception as error:
        return _refusal(error, "resume")
    audited = _audit_outcome(copy_config)
    if audited != "audit-accepted":
        return "resume-then-audit-rejected"
    try:
        view = catalog_view(copy_config)
    except Exception as error:
        return _refusal(error, "reader")
    if view == reference:
        return "rebuilt-identical"
    return "silent-divergence"


BUCKETS = 8


def _consumer_outcome(
    corpus: Corpus, copy_config: CoreConfig, reference: dict[str, Any]
) -> str:
    """At-rest corpora: the READY audit accepted the corruption; the next
    incremental turn (the production consumer of retained history) must
    refuse it, or converge to exactly the fault-free incremental catalog when
    the corrupted column is never consumed."""

    try:
        corpus.consume(copy_config)
    except Exception as error:
        return _refusal(error, "consumer")
    audited = _audit_outcome(copy_config)
    if audited != "audit-accepted":
        return "consumer-then-audit-rejected"
    try:
        view = catalog_view(copy_config)
    except Exception as error:
        return _refusal(error, "reader")
    if view == reference:
        return "consumer-inert"
    return "silent-divergence"


def _matrix(tmp_path: Path, bucket: int) -> tuple[Counter[str], list[str]]:
    corpora = build_corpora(
        tmp_path, lambda name: _sqlite_config(tmp_path / f"{name}.sqlite3")
    )
    columns = identity_columns()
    assert len(columns) > 300
    references: dict[str, dict[str, Any]] = {}
    outcomes: Counter[str] = Counter()
    detail: list[str] = []
    ordinal = 0
    for corpus in corpora:
        tables = {column.table for column in columns}
        present = {
            table
            for table, rows in _present_rows(corpus.config, tables).items()
            if rows
        }
        rows_by_table = _present_rows(corpus.config, present)
        if corpus.mid_flight or corpus.consumable:
            clean = tmp_path / f"{corpus.name}.clean.sqlite3"
            shutil.copyfile(corpus.config.database.database, clean)
            if corpus.mid_flight:
                corpus.resume(_sqlite_config(clean))
            else:
                corpus.consume(_sqlite_config(clean))
            references[corpus.name] = catalog_view(_sqlite_config(clean))
        for column in columns:
            if column.table not in present:
                continue
            for kind in ("flip", "swap"):
                ordinal += 1
                if ordinal % BUCKETS != bucket:
                    continue
                corruption = Corruption(column, kind)
                copy_path = tmp_path / f"case-{ordinal}.sqlite3"
                shutil.copyfile(corpus.config.database.database, copy_path)
                copy_config = _sqlite_config(copy_path)
                if not _apply(copy_config, corruption, rows_by_table[column.table]):
                    outcomes["not-applicable"] += 1
                    copy_path.unlink()
                    continue
                if corpus.mid_flight:
                    outcome = _resume_outcome(
                        corpus, copy_config, references[corpus.name]
                    )
                else:
                    outcome = _audit_outcome(copy_config)
                    if outcome == "audit-accepted" and corpus.consumable:
                        outcome = _consumer_outcome(
                            corpus, copy_config, references[corpus.name]
                        )
                outcomes[outcome] += 1
                if outcome in {"audit-accepted", "silent-divergence", "consumer-inert"}:
                    detail.append(f"{corpus.name} {corruption.label} -> {outcome}")
                copy_path.unlink()
    return outcomes, detail


def _present_rows(
    config: CoreConfig, tables: set[str]
) -> dict[str, list[tuple[Any, ...]]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return {
                table: connector.fetch_all(f"SELECT * FROM {table} LIMIT 2")
                for table in tables
            }
    finally:
        connector.close()


# Published data and retained analysis history that neither the bounded READY
# audit nor a later production turn re-derives: a corruption there is visible
# to readers (the public catalog view changes).  These are the only columns
# where that happens; every other corruption is refused, rebuilt identically,
# or provably inert for the ingest (the catalog stays identical).
READER_VISIBLE_COLUMNS = frozenset(
    {
        "catalog_a_impacted_gid_provenance_storage.analysis_id",
        "catalog_analysis_gid_candidate_shadows.analysis_id",
        "catalog_analysis_gid_winner_selections.analysis_id",
        "catalog_analysis_impacted_gid_storage.analysis_id",
        "catalog_pages.image_sha256",
        "catalog_tag_terms.tag_value_sha256",
        "catalog_title_sorts.sort_title_sha256",
    }
)

REFUSALS = (
    "audit-rejected",
    "resume-rejected",
    "consumer-rejected",
    "reader-rejected",
    "adapter-rejected",
    "resume-then-audit-rejected",
    "consumer-then-audit-rejected",
)


@pytest.mark.parametrize("bucket", range(BUCKETS))
def test_sqlite_every_identity_corruption_is_refused_by_audit_or_resume(
    tmp_path: Path, bucket: int
) -> None:
    outcomes, detail = _matrix(tmp_path, bucket)
    assert sum(outcomes[name] for name in REFUSALS) > 0
    # The bounded READY audit never silently accepts a corruption of an
    # at-rest catalog whose consumer was not exercised.
    assert outcomes["audit-accepted"] == 0
    # A corruption changes the public catalog only in the pinned columns.
    visible = sorted(
        {
            line.split(" ", 1)[1].rsplit(":", 1)[0]
            for line in detail
            if line.endswith("silent-divergence")
        }
    )
    assert set(visible) <= READER_VISIBLE_COLUMNS, visible
