"""Native equivalence and copy-count bounds for immutable fault planning."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
import test_vnext_identity_corruption_matrix as matrix
from test_vnext_identity_corruption_matrix import (
    Corruption,
    TableSample,
    _apply,
    _apply_sampled_corruption,
    _plan_corruption,
    _present_rows,
)
from test_vnext_physical_domain_fault_matrix import Column
from vnext_database_snapshot import ReusableDatabaseSnapshot, database_digest
from vnext_test_database import DatabaseFactory, database_connector

from h2hdb import CoreConfig

_ROWS: tuple[tuple[object, ...], ...] = (
    (1, b"\x01", None),
    (2, b"\x02", b"same"),
    (3, b"\x03", b"same"),
    (4, b"", b""),
)
_NAMES = ("id", "identity_value", "payload")


@dataclass(frozen=True)
class _Case:
    name: str
    column: str
    kind: str
    sampled_ids: tuple[int, ...]
    planned: bool
    applied: bool
    changed_row: tuple[object, ...] | None


_CASES = (
    _Case(
        "valid-flip", "identity_value", "flip", (1, 2), True, True, (1, b"\xfe", None)
    ),
    _Case("empty", "identity_value", "flip", (4,), False, False, None),
    _Case(
        "native-unique-rejection", "identity_value", "swap", (1, 2), True, False, None
    ),
    _Case("null", "payload", "flip", (1,), False, False, None),
    _Case(
        "payload-flip", "payload", "flip", (2, 3), True, True, (2, b"\x02", b"\x8came")
    ),
    _Case("one-row-swap", "identity_value", "swap", (1,), False, False, None),
    _Case("payload-swap", "payload", "swap", (2, 4), True, True, (2, b"\x02", b"")),
    _Case("equal-swap", "payload", "swap", (2, 3), False, False, None),
)


class _CountedSnapshot(ReusableDatabaseSnapshot):
    copies = 0
    schema_checks = 0

    def require_target_schema(self) -> None:
        self.schema_checks += 1
        super().require_target_schema()

    def restore(self) -> CoreConfig:
        self.copies += 1
        return super().restore()


def _snapshot(factory: DatabaseFactory) -> _CountedSnapshot:
    source = factory.config("immutable-source")
    binary = "BLOB" if factory.backend == "sqlite" else "VARBINARY(32)"
    with database_connector(source) as connector:
        connector.execute(
            f"CREATE TABLE fault_values (id INTEGER PRIMARY KEY, identity_value {binary} UNIQUE, payload {binary})"
        )
        with connector.transaction():
            connector.execute_many(
                "INSERT INTO fault_values VALUES (%s, %s, %s)", list(_ROWS)
            )
    sampled = _present_rows(source, {"fault_values"})["fault_values"]
    assert sampled.names == _NAMES
    assert len(sampled.rows) == 2 and set(sampled.rows) <= set(_ROWS)
    return _CountedSnapshot(factory, source, factory.config("reusable-target"))


def _read_rows(snapshot: ReusableDatabaseSnapshot) -> tuple[tuple[object, ...], ...]:
    # A new connector proves each fault commit is reader-visible.
    with database_connector(snapshot.target) as connector, connector.read_transaction():
        return tuple(connector.fetch_all("SELECT * FROM fault_values ORDER BY id"))


def _exercise(
    snapshot: _CountedSnapshot,
    *,
    early: bool,
) -> tuple[tuple[str, bool], ...]:
    previous = _ROWS
    outcomes = []
    for case in _CASES:
        sample = TableSample(
            _NAMES, tuple(_ROWS[index - 1] for index in case.sampled_ids)
        )
        corruption = Corruption(
            Column("fault_values", case.column, "BLOB", "VARBINARY(32)", True, ()),
            case.kind,
        )
        mutation = _plan_corruption(corruption, sample)
        assert (mutation is not None) == case.planned
        if early:
            applied = _apply_sampled_corruption(snapshot, corruption, sample)
        else:
            config = snapshot.restore()
            applied = False if mutation is None else _apply(config, mutation)
        if early and not case.planned:
            expected = previous
        else:
            expected = tuple(
                case.changed_row
                if case.changed_row is not None and row[0] == case.changed_row[0]
                else row
                for row in _ROWS
            )
        assert applied == case.applied, case.name
        observed = _read_rows(snapshot)
        if case.planned:
            assert observed == expected, case.name
        else:
            # An inapplicable candidate has no consumer. Baseline restores
            # pristine rows; the optimized arm retains the last unused copy.
            assert observed in (previous, _ROWS), case.name
        previous = observed
        outcomes.append((case.name, applied))
    # A final no-op must not hide schema mutations by the preceding consumer.
    snapshot.require_target_schema()
    return tuple(outcomes)


def _assert_copy_bound(snapshot: _CountedSnapshot) -> None:
    original = database_digest(snapshot.source)
    reference = None
    # Fixed ABBA order, four complete repeated cycles per arm. Wall time is
    # deliberately not a performance oracle under concurrent correctness work.
    for early in (False, True, True, False):
        copy_start, schema_start = snapshot.copies, snapshot.schema_checks
        for _ in range(4):
            outcomes = _exercise(snapshot, early=early)
            if reference is None:
                reference = outcomes
            assert outcomes == reference
        assert snapshot.copies - copy_start == 4 * (4 if early else 8), (
            "copies exceed applicable candidate bound"
        )
        assert snapshot.schema_checks - schema_start == 4 * (8 + 1)
        assert len(outcomes) == 8
        assert sum(applied for _, applied in outcomes) == 3
        snapshot.restore()
        assert _read_rows(snapshot) == _ROWS
        assert database_digest(snapshot.source) == original


def test_identity_preflight_preserves_candidates_native_constraints_and_copy_bound(
    database_factory: DatabaseFactory,
) -> None:
    _assert_copy_bound(_snapshot(database_factory))


def test_identity_preflight_copy_bound_rejects_unconditional_copy(
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys

    def always_copy(
        snapshot: ReusableDatabaseSnapshot,
        corruption: Corruption,
        sample: TableSample,
    ) -> bool:
        config = snapshot.restore()
        mutation = _plan_corruption(corruption, sample)
        return False if mutation is None else _apply(config, mutation)

    monkeypatch.setattr(sys.modules[__name__], "_apply_sampled_corruption", always_copy)
    with pytest.raises(
        AssertionError, match="copies exceed applicable candidate bound"
    ):
        _assert_copy_bound(_snapshot(database_factory))


def test_identity_preflight_negative_control_cannot_skip_a_real_mutation(
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(database_factory)
    monkeypatch.setattr(matrix, "_plan_corruption", lambda *_args: None)
    with pytest.raises(AssertionError, match="valid-flip"):
        _exercise(snapshot, early=True)


def test_identity_preflight_false_candidate_still_rejects_schema_drift(
    database_factory: DatabaseFactory,
) -> None:
    snapshot = _snapshot(database_factory)
    with database_connector(snapshot.target) as connector:
        connector.execute("ALTER TABLE fault_values ADD COLUMN foreign_value INTEGER")
    empty = TableSample(_NAMES, (_ROWS[3],))
    corruption = Corruption(
        Column("fault_values", "identity_value", "BLOB", "VARBINARY(32)", True, ()),
        "flip",
    )
    assert _plan_corruption(corruption, empty) is None
    with pytest.raises(ValueError, match="schema drift"):
        _apply_sampled_corruption(snapshot, corruption, empty)
    assert snapshot.copies == 0
