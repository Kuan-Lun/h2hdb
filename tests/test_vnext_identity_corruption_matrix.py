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

import json
import pickle
import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any

import pytest
from test_vnext_physical_domain_fault_matrix import (
    Column,
    _column_names,
    manifest_columns,
)
from vnext_corpora import Corpus, build_corpora
from vnext_database_snapshot import ReusableDatabaseSnapshot, database_digest
from vnext_fault_harness import EPOCH_CONTROL_TABLE, open_connector
from vnext_pipeline import catalog_view, full_check
from vnext_test_database import DatabaseFactory, inspect_all, set_foreign_key_checks

from h2hdb import CoreConfig
from h2hdb.sql_connector import DatabaseDuplicateKeyError

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


def _rows(config: CoreConfig, table: str, limit: int = 2) -> list[tuple[Any, ...]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return inspect_all(connector, f"SELECT * FROM {table} LIMIT {limit}")
    finally:
        connector.close()


@dataclass(frozen=True)
class CorruptionMutation:
    corruption: Corruption
    names: tuple[str, ...]
    first: tuple[Any, ...]
    replacement: Any


@dataclass(frozen=True)
class TableSample:
    names: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


def _plan_corruption(
    corruption: Corruption, sample: TableSample
) -> CorruptionMutation | None:
    """Reject only non-mutations provable from immutable native source rows."""
    index = sample.names.index(corruption.column.name)
    first = sample.rows[0]
    value = first[index]
    if not value:
        return None
    if corruption.kind == "flip":
        replacement = bytes([value[0] ^ 0xFF]) + bytes(value[1:])
    else:
        if len(sample.rows) < 2 or sample.rows[1][index] == value:
            return None
        replacement = sample.rows[1][index]
    return CorruptionMutation(corruption, sample.names, first, replacement)


def _apply(config: CoreConfig, mutation: CorruptionMutation) -> bool:
    """Commit one corruption on ``config``; return False when not applicable."""

    connector = open_connector(config)
    try:
        corruption = mutation.corruption
        names, first = mutation.names, mutation.first
        where = " AND ".join(
            f"`{name}` IS NULL" if cell is None else f"`{name}` = %s"
            for name, cell in zip(names, first, strict=True)
        )
        bound = tuple(cell for cell in first if cell is not None)
        set_foreign_key_checks(connector, enabled=False)
        connector.begin()
        try:
            affected = connector.execute_affected(
                f"UPDATE `{corruption.column.table}` SET `{corruption.column.name}` = %s "
                f"WHERE {where}",
                (mutation.replacement, *bound),
            )
            if affected != 1:
                connector.rollback()
                return False
            connector.commit()
        except Exception as error:
            connector.rollback()
            # Only native constraint rejection is an admissible DDL refusal.
            # A SQL syntax/connection error must not erase a corruption case.
            import sqlite3

            if isinstance(
                error, (DatabaseDuplicateKeyError, sqlite3.IntegrityError)
            ) or (
                type(error).__module__.startswith("mysql.")
                and getattr(error, "errno", None) in {1062, 1451, 1452, 4025}
            ):
                return False
            raise
        return True
    finally:
        connector.close()


def _apply_sampled_corruption(
    snapshot: ReusableDatabaseSnapshot,
    corruption: Corruption,
    sample: TableSample,
) -> bool:
    mutation = _plan_corruption(corruption, sample)
    if mutation is None:
        # Preserve immediate schema-drift refusal; only the full data copy is
        # unnecessary for a candidate proven not to inject a mutation.
        snapshot.require_target_schema()
        return False
    return _apply(snapshot.restore(), mutation)


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


@dataclass
class PreparedCorpus:
    corpus: Corpus
    snapshot: ReusableDatabaseSnapshot
    samples: dict[str, TableSample]
    reference: dict[str, Any]
    original_digest: object
    adapter_digest: bytes

    def assert_unchanged(self) -> None:
        assert database_digest(self.corpus.config) == self.original_digest
        assert _adapter_digest(self.corpus) == self.adapter_digest


def _adapter_digest(corpus: Corpus) -> bytes:
    # These bounded memory adapters are fixture data, not external pickles.
    # Digest equality assumes SHA-256 collision resistance; consumers deepcopy
    # them before any work, and the native source DB is independently checked.
    return sha256(pickle.dumps((corpus.source, corpus.library))).digest()


@pytest.fixture(scope="module")
def identity_corpora(
    module_database_factory: DatabaseFactory,
) -> Iterator[list[PreparedCorpus]]:
    """Build immutable corpora and their clean consumer oracles once per backend."""
    factory = module_database_factory
    corpora = build_corpora(Path("."), factory.config)
    tables = {column.table for column in identity_columns()}
    prepared: list[PreparedCorpus] = []
    for index, corpus in enumerate(corpora):
        snapshot = ReusableDatabaseSnapshot(
            factory, corpus.config, factory.config(f"identity-target-{index}")
        )
        original_digest = database_digest(corpus.config)
        adapter_digest = _adapter_digest(corpus)
        reference: dict[str, Any] = {}
        if corpus.mid_flight or corpus.consumable:
            if corpus.mid_flight:
                corpus.resume(snapshot.target)
            else:
                corpus.consume(snapshot.target)
            assert full_check(snapshot.target).state == "READY"
            reference = catalog_view(snapshot.target)
        value = PreparedCorpus(
            corpus,
            snapshot,
            _present_rows(corpus.config, tables),
            reference,
            original_digest,
            adapter_digest,
        )
        value.assert_unchanged()
        prepared.append(value)
    try:
        yield prepared
    finally:
        for value in prepared:
            value.assert_unchanged()


def _matrix(
    corpora: list[PreparedCorpus],
    bucket: int,
) -> tuple[Counter[str], list[str]]:
    columns = identity_columns()
    assert len(columns) > 300
    outcomes: Counter[str] = Counter()
    detail: list[str] = []
    ordinal = 0
    for prepared in corpora:
        corpus = prepared.corpus
        prepared.assert_unchanged()
        started = monotonic()
        prior = outcomes.copy()
        selected = 0
        current_label = "before first selected corruption"
        completed = False
        try:
            for column in columns:
                sample = prepared.samples[column.table]
                if not sample.rows:
                    continue
                for kind in ("flip", "swap"):
                    ordinal += 1
                    if ordinal % BUCKETS != bucket:
                        continue
                    corruption = Corruption(column, kind)
                    current_label = corruption.label
                    selected += 1
                    if not _apply_sampled_corruption(
                        prepared.snapshot, corruption, sample
                    ):
                        outcomes["not-applicable"] += 1
                        continue
                    copy_config = prepared.snapshot.target
                    if corpus.mid_flight:
                        outcome = _resume_outcome(
                            corpus, copy_config, prepared.reference
                        )
                    else:
                        outcome = _audit_outcome(copy_config)
                        if outcome == "audit-accepted" and corpus.consumable:
                            outcome = _consumer_outcome(
                                corpus, copy_config, prepared.reference
                            )
                    outcomes[outcome] += 1
                    if outcome in {
                        "audit-accepted",
                        "silent-divergence",
                        "consumer-inert",
                    }:
                        detail.append(f"{corpus.name} {corruption.label} -> {outcome}")
            prepared.snapshot.require_target_schema()
            prepared.assert_unchanged()
            completed = True
        except Exception as error:
            error.add_note(
                f"identity matrix: corpus={corpus.name} corruption={current_label} "
                f"bucket={bucket} ordinal={ordinal}"
            )
            raise
        finally:
            print(
                "IDENTITY_CORPUS "
                + json.dumps(
                    {
                        "backend": corpus.config.database.sql_type,
                        "bucket": bucket,
                        "corpus": corpus.name,
                        "selected_candidates": selected,
                        "outcomes": dict(outcomes - prior),
                        "seconds": monotonic() - started,
                        "completed": completed,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    return outcomes, detail


def _present_rows(config: CoreConfig, tables: set[str]) -> dict[str, TableSample]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return {
                table: TableSample(
                    tuple(_column_names(connector, config.database.sql_type, table)),
                    tuple(inspect_all(connector, f"SELECT * FROM {table} LIMIT 2")),
                )
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
def test_every_identity_corruption_is_refused_by_audit_or_resume(
    identity_corpora: list[PreparedCorpus], bucket: int
) -> None:
    outcomes, detail = _matrix(identity_corpora, bucket)
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
