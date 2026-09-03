"""Exact physical storage protocols for the analysis lifecycle families.

Immutable descriptors and component receipts are stored as complete BCNF rows.
Mutable lifecycle state remains isolated from the descriptor, and completion
timestamps are optional immutable facts constrained by that state.
"""

from __future__ import annotations

__all__ = [
    "AnalysisExclusionDeltaFamily",
    "AnalysisFamilyCollisionError",
    "AnalysisFamilyPartialError",
    "AnalysisRunFamily",
    "AnalysisStateComponentFamily",
    "cas_analysis_run_state",
    "ensure_analysis_exclusion_delta_family",
    "ensure_analysis_run_family",
    "ensure_analysis_state_component_family",
    "insert_analysis_run_completed_at",
    "insert_analysis_exclusion_delta_family",
    "load_analysis_exclusion_delta_families",
    "load_analysis_exclusion_delta_family",
    "load_analysis_run_family",
    "load_analysis_run_family_by_identity",
    "load_analysis_state_component_families",
    "load_analysis_state_component_family",
    "require_exact_analysis_state_components",
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_state_machine_contract import require_catalog_state_mutation
from .vnext_transaction import VNextUnitOfWork

_RUN_DESCRIPTOR = "catalog_analysis_run_descriptor"
_RUN_STATE = "catalog_analysis_run_states"
_RUN_COMPLETED = "catalog_analysis_run_completed_ats"
_SOURCE_BUILD_SEALED = "catalog_source_build_sealed_ats"

_COMPONENT_SEAL = "catalog_analysis_state_component_seals"

_EXCLUSION_ANCHOR = "catalog_analysis_exclusion_delta_anchors"
_EXCLUSION_OLD = "catalog_analysis_exclusion_delta_old_excluded_flags"
_EXCLUSION_NEW = "catalog_analysis_exclusion_delta_new_excluded_flags"
_EXCLUSION_CHANGE = "catalog_analysis_exclusion_delta_changes"
_EXCLUSION_SEAL = "catalog_analysis_exclusion_delta_seals"

_COMPONENT_STAGE = {
    b"file_hash_decision": (b"validate_file_hash_decision", b"D", 32),
    b"content_owner_candidate": (b"validate_content_owner_candidate", b"G", 8),
    b"content_owner": (b"validate_content_owner", b"D", 32),
    b"gid_candidate": (b"validate_gid_candidate", b"G", 8),
    b"gid_winner": (b"validate_gid_winner", b"I", 8),
}


class AnalysisFamilyCollisionError(RuntimeError):
    """A complete analysis family disagrees with exact durable authority."""


class AnalysisFamilyPartialError(AnalysisFamilyCollisionError):
    """At least one physical member exists without one complete family."""


@dataclass(frozen=True, slots=True)
class AnalysisRunFamily:
    analysis_id: bytes
    build_id: bytes
    policy_id: int
    input_manifest_sha256: bytes
    started_at: int
    state: str
    completed_at: int | None

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="analysis_id")
        require_uuid16(self.build_id, field="analysis build_id")
        require_positive_int63(self.policy_id, field="analysis policy_id")
        require_digest32(
            self.input_manifest_sha256,
            field="analysis input_manifest_sha256",
        )
        started = require_int63(self.started_at, field="analysis started_at")
        if self.state not in {"OPEN", "COMPLETE", "ABANDONED"}:
            raise ValueError("analysis state is not registered")
        if self.state == "COMPLETE":
            completed = require_int63(
                self.completed_at,
                field="analysis completed_at",
            )
            if completed < started:
                raise ValueError("analysis completed_at precedes started_at")
        elif self.completed_at is not None:
            raise ValueError("OPEN or ABANDONED analysis has completed_at")


@dataclass(frozen=True, slots=True)
class AnalysisStateComponentFamily:
    analysis_id: bytes
    state_component: bytes
    row_count: int
    sealed_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="component analysis_id")
        require_bounded_bytes(
            self.state_component,
            field="state_component",
            minimum=1,
            maximum=64,
        )
        require_int63(self.row_count, field="component row_count")
        require_int63(self.sealed_at, field="component sealed_at")


@dataclass(frozen=True, slots=True)
class AnalysisExclusionDeltaFamily:
    analysis_id: bytes
    file_sha256: bytes
    old_excluded: int
    new_excluded: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="exclusion analysis_id")
        require_digest32(self.file_sha256, field="exclusion file_sha256")
        if self.old_excluded not in {0, 1} or self.new_excluded not in {0, 1}:
            raise ValueError("exclusion flags must be boolean integers")

    @property
    def changed(self) -> bool:
        return self.old_excluded != self.new_excluded


def _run_family_row(connector: Any, analysis_id: bytes) -> tuple[Any, ...]:
    analysis = require_uuid16(analysis_id, field="analysis_id")
    row = connector.fetch_one(
        "WITH family_keys(analysis_id) AS ("
        f"SELECT analysis_id FROM {_RUN_DESCRIPTOR} WHERE analysis_id = %s UNION "
        f"SELECT analysis_id FROM {_RUN_STATE} WHERE analysis_id = %s UNION "
        f"SELECT analysis_id FROM {_RUN_COMPLETED} WHERE analysis_id = %s) "
        "SELECT k.analysis_id, descriptor.analysis_id, descriptor.build_id, "
        "descriptor.policy_id, descriptor.input_manifest_sha256, "
        "descriptor.started_at, state.analysis_id, state.state, "
        "completed.analysis_id, completed.completed_at, source.sealed_at "
        "FROM family_keys AS k "
        f"LEFT JOIN {_RUN_DESCRIPTOR} AS descriptor "
        "ON descriptor.analysis_id = k.analysis_id "
        f"LEFT JOIN {_RUN_STATE} AS state ON state.analysis_id = k.analysis_id "
        f"LEFT JOIN {_RUN_COMPLETED} AS completed "
        "ON completed.analysis_id = k.analysis_id "
        f"LEFT JOIN {_SOURCE_BUILD_SEALED} AS source "
        "ON source.build_id = descriptor.build_id",
        (analysis,) * 3,
    )
    return tuple(row)


def load_analysis_run_family(
    connector: Any,
    *,
    analysis_id: bytes,
) -> AnalysisRunFamily | None:
    """Load one total run family, including sole-build congruence."""

    analysis = require_uuid16(analysis_id, field="analysis_id")
    row = _run_family_row(connector, analysis)
    if not row:
        return None
    mandatory_key_indexes = (0, 1, 6)
    if len(row) != 11 or any(row[index] != analysis for index in mandatory_key_indexes):
        raise AnalysisFamilyPartialError(
            "analysis run has an existing incomplete descriptor family"
        )
    if row[8] not in {None, analysis}:
        raise AnalysisFamilyPartialError("analysis completed_at key differs")
    if row[10] is None:
        raise AnalysisFamilyCollisionError("analysis source build is not sealed")
    try:
        family = AnalysisRunFamily(
            analysis,
            row[2],
            row[3],
            row[4],
            row[5],
            row[7],
            row[9],
        )
        source_sealed_at = require_int63(
            row[10],
            field="analysis source build sealed_at",
        )
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "analysis run contains invalid descriptor facts"
        ) from error
    if family.started_at < source_sealed_at:
        raise AnalysisFamilyCollisionError(
            "analysis started_at precedes source build sealed_at"
        )
    build_rows = connector.fetch_all(
        f"SELECT analysis_id FROM {_RUN_DESCRIPTOR} "
        "WHERE build_id = %s ORDER BY analysis_id LIMIT 2",
        (family.build_id,),
    )
    if build_rows != [(analysis,)]:
        raise AnalysisFamilyCollisionError(
            "analysis build has multiple or incongruent run descriptors"
        )
    return family


def load_analysis_run_family_by_identity(
    connector: Any,
    *,
    build_id: bytes,
    policy_id: int,
) -> AnalysisRunFamily | None:
    """Load the sole analysis for a build and exact-check its policy."""

    build = require_uuid16(build_id, field="analysis build_id")
    policy = require_positive_int63(policy_id, field="analysis policy_id")
    rows = connector.fetch_all(
        f"SELECT analysis_id FROM {_RUN_DESCRIPTOR} "
        "WHERE build_id = %s ORDER BY analysis_id LIMIT 2",
        (build,),
    )
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 1:
        raise AnalysisFamilyCollisionError(
            "analysis build identity is duplicated or malformed"
        )
    family = load_analysis_run_family(connector, analysis_id=rows[0][0])
    if family is None or family.build_id != build:
        raise AnalysisFamilyPartialError(
            "analysis build identity has no congruent sealed descriptor"
        )
    if family.policy_id != policy:
        raise AnalysisFamilyCollisionError(
            "analysis build already belongs to a different policy"
        )
    return family


def ensure_analysis_run_family(
    connector: Any,
    *,
    analysis_id: bytes,
    build_id: bytes,
    policy_id: int,
    input_manifest_sha256: bytes,
    started_at: int,
) -> tuple[AnalysisRunFamily, bool]:
    """Insert a new OPEN descriptor and state or exact-compare its replay."""

    proposed = AnalysisRunFamily(
        analysis_id,
        build_id,
        policy_id,
        input_manifest_sha256,
        started_at,
        "OPEN",
        None,
    )
    existing = load_analysis_run_family_by_identity(
        connector,
        build_id=proposed.build_id,
        policy_id=proposed.policy_id,
    )
    if existing is not None:
        if existing.input_manifest_sha256 != proposed.input_manifest_sha256:
            raise AnalysisFamilyCollisionError(
                "analysis build replay changed its server-derived input"
            )
        return existing, False
    by_id = load_analysis_run_family(
        connector,
        analysis_id=proposed.analysis_id,
    )
    if by_id is not None:
        raise AnalysisFamilyCollisionError(
            "proposed analysis_id belongs to another natural identity"
        )
    source = connector.fetch_one(
        f"SELECT sealed_at FROM {_SOURCE_BUILD_SEALED} WHERE build_id = %s",
        (proposed.build_id,),
    )
    if len(source) != 1 or proposed.started_at < require_int63(
        source[0], field="analysis source build sealed_at"
    ):
        raise AnalysisFamilyCollisionError(
            "analysis database start time precedes its sealed source build"
        )
    transition = require_catalog_state_mutation(
        "analysis-run.initialize",
        previous_state=None,
        next_state="OPEN",
        timestamp=None,
    )
    connector.execute(
        f"INSERT INTO {_RUN_DESCRIPTOR} "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, started_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        (
            proposed.analysis_id,
            proposed.build_id,
            proposed.policy_id,
            proposed.input_manifest_sha256,
            proposed.started_at,
        ),
    )
    connector.execute(
        f"INSERT INTO {_RUN_STATE} (analysis_id, state) VALUES (%s, %s)",
        (proposed.analysis_id, transition.next_state),
    )
    return proposed, True


def insert_analysis_run_completed_at(
    connector: Any,
    *,
    analysis_id: bytes,
    completed_at: int,
) -> None:
    analysis = require_uuid16(analysis_id, field="analysis_id")
    timestamp = require_int63(completed_at, field="analysis completed_at")
    transition = require_catalog_state_mutation(
        "analysis-run.complete-timestamp",
        previous_state="OPEN",
        next_state="COMPLETE",
        timestamp=timestamp,
    )
    connector.execute(
        f"INSERT INTO {_RUN_COMPLETED} (analysis_id, completed_at) VALUES (%s, %s)",
        (analysis, transition.timestamp),
    )


def cas_analysis_run_state(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    previous: str,
    successor: str,
    timestamp: int | None,
    authority: str,
) -> None:
    analysis = require_uuid16(analysis_id, field="analysis_id")
    transition = require_catalog_state_mutation(
        "analysis-run.transition",
        previous_state=previous,
        next_state=successor,
        timestamp=timestamp,
    )
    work.compare_and_swap(
        f"UPDATE {_RUN_STATE} SET state = %s WHERE analysis_id = %s AND state = %s",
        (transition.next_state, analysis, transition.previous_state),
        authority=authority,
    )


def _component_family_row(
    connector: Any,
    analysis_id: bytes,
    state_component: bytes,
) -> tuple[Any, ...]:
    row = connector.fetch_one(
        f"SELECT analysis_id, state_component, row_count, sealed_at "
        f"FROM {_COMPONENT_SEAL} "
        "WHERE analysis_id = %s AND state_component = %s",
        (analysis_id, state_component),
    )
    return tuple(row)


def load_analysis_state_component_family(
    connector: Any,
    *,
    analysis_id: bytes,
    state_component: bytes,
) -> AnalysisStateComponentFamily | None:
    analysis = require_uuid16(analysis_id, field="component analysis_id")
    component = require_bounded_bytes(
        state_component,
        field="state_component",
        minimum=1,
        maximum=64,
    )
    row = _component_family_row(connector, analysis, component)
    if not row:
        return None
    expected = (analysis, component)
    if len(row) != 4 or row[:2] != expected:
        raise AnalysisFamilyPartialError("analysis component row is malformed")
    try:
        return AnalysisStateComponentFamily(analysis, component, row[2], row[3])
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "analysis component contains invalid receipt facts"
        ) from error


def load_analysis_state_component_families(
    connector: Any,
    *,
    analysis_id: bytes,
    limit: int = 6,
) -> tuple[AnalysisStateComponentFamily, ...]:
    analysis = require_uuid16(analysis_id, field="component analysis_id")
    bound = require_positive_int63(limit, field="component family limit")
    keys = connector.fetch_all(
        f"SELECT state_component FROM {_COMPONENT_SEAL} "
        "WHERE analysis_id = %s ORDER BY state_component LIMIT %s",
        (analysis, bound),
    )
    result: list[AnalysisStateComponentFamily] = []
    for row in keys:
        if len(row) != 1:
            raise AnalysisFamilyCollisionError("analysis component key is malformed")
        family = load_analysis_state_component_family(
            connector,
            analysis_id=analysis,
            state_component=row[0],
        )
        if family is None:
            raise AnalysisFamilyPartialError("analysis component key disappeared")
        result.append(family)
    return tuple(result)


def ensure_analysis_state_component_family(
    connector: Any,
    *,
    analysis_id: bytes,
    state_component: bytes,
    row_count: int,
    sealed_at: int,
) -> tuple[AnalysisStateComponentFamily, bool]:
    proposed = AnalysisStateComponentFamily(
        analysis_id,
        state_component,
        row_count,
        sealed_at,
    )
    existing = load_analysis_state_component_family(
        connector,
        analysis_id=proposed.analysis_id,
        state_component=proposed.state_component,
    )
    if existing is not None:
        if existing != proposed:
            raise AnalysisFamilyCollisionError(
                "analysis component replay differs from terminal receipt"
            )
        return existing, False
    connector.execute(
        f"INSERT INTO {_COMPONENT_SEAL} "
        "(analysis_id, state_component, row_count, sealed_at) "
        "VALUES (%s, %s, %s, %s)",
        (
            proposed.analysis_id,
            proposed.state_component,
            proposed.row_count,
            proposed.sealed_at,
        ),
    )
    return proposed, True


def require_exact_analysis_state_components(
    connector: Any,
    *,
    analysis_id: bytes,
    state_components: frozenset[bytes],
) -> tuple[AnalysisStateComponentFamily, ...]:
    """Require the exact component set and each terminal receipt/checkpoint."""

    analysis = require_uuid16(analysis_id, field="component analysis_id")
    expected = frozenset(
        require_bounded_bytes(
            component,
            field="state_component",
            minimum=1,
            maximum=64,
        )
        for component in state_components
    )
    if not expected or not expected.issubset(_COMPONENT_STAGE):
        raise ValueError("component set contains an unregistered value")
    families = load_analysis_state_component_families(
        connector,
        analysis_id=analysis,
        limit=len(expected) + 1,
    )
    if (
        len(families) != len(expected)
        or {family.state_component for family in families} != expected
    ):
        raise AnalysisFamilyCollisionError(
            "analysis does not have the exact terminal component set"
        )
    stages = tuple(sorted(_COMPONENT_STAGE[component][0] for component in expected))
    placeholders = ", ".join("%s" for _stage in stages)
    receipt_rows = connector.fetch_all(
        "SELECT stage, start_cursor, start_processed_count, page_limit, "
        "next_cursor, next_processed_count, next_state, row_count, terminal, "
        "committed_at FROM catalog_analysis_batch_receipts "
        f"WHERE analysis_id = %s AND stage IN ({placeholders}) AND row_count = %s "
        "ORDER BY stage LIMIT %s",
        (analysis, *stages, 0, len(stages) + 1),
    )
    checkpoint_rows = connector.fetch_all(
        "SELECT stage, `cursor`, processed_count, state, updated_at "
        "FROM catalog_analysis_checkpoints "
        f"WHERE analysis_id = %s AND stage IN ({placeholders}) "
        "ORDER BY stage LIMIT %s",
        (analysis, *stages, len(stages) + 1),
    )
    if len(receipt_rows) != len(stages) or len(checkpoint_rows) != len(stages):
        raise AnalysisFamilyCollisionError(
            "analysis component set lacks exact terminal receipts or checkpoints"
        )
    family_by_stage = {
        _COMPONENT_STAGE[family.state_component][0]: family for family in families
    }
    checkpoint_by_stage = {row[0]: row[1:] for row in checkpoint_rows}
    if set(checkpoint_by_stage) != set(stages):
        raise AnalysisFamilyCollisionError(
            "analysis component checkpoint stage set differs"
        )
    for row in receipt_rows:
        if len(row) != 10 or row[0] not in family_by_stage:
            raise AnalysisFamilyCollisionError(
                "analysis terminal receipt stage set differs"
            )
        family = family_by_stage[row[0]]
        try:
            start_count = require_int63(
                row[2],
                field="terminal start_processed_count",
            )
            page_limit = require_positive_int63(
                row[3],
                field="terminal page_limit",
            )
            next_count = require_int63(
                row[5],
                field="terminal next_processed_count",
            )
            terminal = require_int63(row[8], field="terminal flag")
            committed_at = require_int63(
                row[9],
                field="terminal committed_at",
            )
            _stage, kind, key_size = _COMPONENT_STAGE[family.state_component]
            cursor = require_bounded_bytes(
                row[4],
                field="terminal next_cursor",
                minimum=3 + key_size + 8,
                maximum=3 + key_size + 8,
            )
        except (TypeError, ValueError) as error:
            raise AnalysisFamilyCollisionError(
                "analysis terminal receipt has invalid facts"
            ) from error
        key_present = cursor[2]
        key = cursor[3 : 3 + key_size]
        if (
            cursor[0] != 1
            or cursor[1:2] != kind
            or key_present not in {0, 1}
            or (key_present == 0 and key != bytes(key_size))
        ):
            raise AnalysisFamilyCollisionError(
                "analysis terminal receipt has the wrong stage cursor codec"
            )
        if key_present == 1 and kind in {b"G", b"I"}:
            try:
                require_positive_int63(
                    int.from_bytes(key, "big"),
                    field="terminal cursor integer key",
                )
            except (TypeError, ValueError) as error:
                raise AnalysisFamilyCollisionError(
                    "analysis terminal receipt has an invalid integer cursor key"
                ) from error
        live_count = int.from_bytes(cursor[-8:], "big")
        if (
            page_limit > 128
            or row[1] != row[4]
            or start_count != next_count
            or row[6] != "COMPLETE"
            or row[7] != 0
            or terminal != 1
            or live_count != family.row_count
            or committed_at != family.sealed_at
            or checkpoint_by_stage[row[0]]
            != (row[4], next_count, "COMPLETE", committed_at)
        ):
            raise AnalysisFamilyCollisionError(
                "analysis component differs from terminal receipt/checkpoint"
            )
    return families


def load_analysis_exclusion_delta_family(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256: bytes,
) -> AnalysisExclusionDeltaFamily | None:
    analysis = require_uuid16(analysis_id, field="exclusion analysis_id")
    digest = require_digest32(file_sha256, field="exclusion file_sha256")
    parameters = (analysis, digest) * 5
    row = connector.fetch_one(
        "WITH family_keys(analysis_id, file_sha256) AS ("
        f"SELECT analysis_id, file_sha256 FROM {_EXCLUSION_ANCHOR} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_EXCLUSION_OLD} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_EXCLUSION_NEW} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_EXCLUSION_CHANGE} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_EXCLUSION_SEAL} "
        "WHERE analysis_id = %s AND file_sha256 = %s) "
        "SELECT k.analysis_id, k.file_sha256, anchor.analysis_id, "
        "anchor.file_sha256, old.analysis_id, old.file_sha256, old.old_excluded, "
        "new.analysis_id, new.file_sha256, new.new_excluded, "
        "exclusion_change.analysis_id, exclusion_change.file_sha256, "
        "seal.analysis_id, seal.file_sha256 "
        "FROM family_keys AS k "
        f"LEFT JOIN {_EXCLUSION_ANCHOR} AS anchor "
        "ON anchor.analysis_id = k.analysis_id AND anchor.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_OLD} AS old "
        "ON old.analysis_id = k.analysis_id AND old.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_NEW} AS new "
        "ON new.analysis_id = k.analysis_id AND new.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_CHANGE} AS exclusion_change "
        "ON exclusion_change.analysis_id = k.analysis_id "
        "AND exclusion_change.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_SEAL} AS seal "
        "ON seal.analysis_id = k.analysis_id AND seal.file_sha256 = k.file_sha256",
        parameters,
    )
    if not row:
        return None
    return _analysis_exclusion_delta_from_wide_row(
        analysis=analysis,
        digest=digest,
        row=tuple(row),
    )


def load_analysis_exclusion_delta_families(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256s: Sequence[bytes],
) -> tuple[AnalysisExclusionDeltaFamily, ...]:
    """Load at most one analysis page of delta families with one set query."""

    analysis = require_uuid16(analysis_id, field="exclusion analysis_id")
    digests = tuple(
        require_digest32(value, field="exclusion file_sha256") for value in file_sha256s
    )
    if len(digests) > 128:
        raise ValueError("exclusion delta family load exceeds one analysis page")
    if len(set(digests)) != len(digests):
        raise ValueError("exclusion delta family load contains duplicate keys")
    if not digests:
        return ()
    placeholders = ", ".join("%s" for _digest in digests)
    selects: list[str] = []
    parameters: list[Any] = []
    for table in (
        _EXCLUSION_ANCHOR,
        _EXCLUSION_OLD,
        _EXCLUSION_NEW,
        _EXCLUSION_CHANGE,
        _EXCLUSION_SEAL,
    ):
        selects.append(
            f"SELECT analysis_id, file_sha256 FROM {table} "
            f"WHERE analysis_id = %s AND file_sha256 IN ({placeholders})"
        )
        parameters.extend((analysis, *digests))
    rows = connector.fetch_all(
        "WITH family_keys(analysis_id, file_sha256) AS ("
        + " UNION ".join(selects)
        + ") SELECT k.analysis_id, k.file_sha256, anchor.analysis_id, "
        "anchor.file_sha256, old.analysis_id, old.file_sha256, old.old_excluded, "
        "new.analysis_id, new.file_sha256, new.new_excluded, "
        "exclusion_change.analysis_id, exclusion_change.file_sha256, "
        "seal.analysis_id, seal.file_sha256 "
        "FROM family_keys AS k "
        f"LEFT JOIN {_EXCLUSION_ANCHOR} AS anchor "
        "ON anchor.analysis_id = k.analysis_id AND anchor.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_OLD} AS old "
        "ON old.analysis_id = k.analysis_id AND old.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_NEW} AS new "
        "ON new.analysis_id = k.analysis_id AND new.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_CHANGE} AS exclusion_change "
        "ON exclusion_change.analysis_id = k.analysis_id "
        "AND exclusion_change.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {_EXCLUSION_SEAL} AS seal "
        "ON seal.analysis_id = k.analysis_id AND seal.file_sha256 = k.file_sha256 "
        "ORDER BY k.file_sha256",
        tuple(parameters),
    )
    requested = set(digests)
    families: list[AnalysisExclusionDeltaFamily] = []
    for raw_row in rows:
        row = tuple(raw_row)
        if len(row) != 14 or row[0] != analysis or row[1] not in requested:
            raise AnalysisFamilyCollisionError(
                "analysis exclusion delta set load returned an unexpected key"
            )
        families.append(
            _analysis_exclusion_delta_from_wide_row(
                analysis=analysis,
                digest=row[1],
                row=row,
            )
        )
    return tuple(families)


def _analysis_exclusion_delta_from_wide_row(
    *,
    analysis: bytes,
    digest: bytes,
    row: tuple[Any, ...],
) -> AnalysisExclusionDeltaFamily:
    expected = (analysis, digest)
    if len(row) != 14 or any(
        row[index : index + 2] != expected for index in (0, 2, 4, 7, 12)
    ):
        raise AnalysisFamilyPartialError(
            "analysis exclusion delta has an existing incomplete sealed family"
        )
    marker_present = row[10:12] == expected
    if row[10:12] not in {(None, None), expected}:
        raise AnalysisFamilyPartialError("analysis exclusion change marker is partial")
    try:
        family = AnalysisExclusionDeltaFamily(analysis, digest, row[6], row[9])
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "analysis exclusion delta contains invalid facts"
        ) from error
    if marker_present != family.changed:
        raise AnalysisFamilyCollisionError(
            "analysis exclusion change marker differs from flag inequality"
        )
    return family


def ensure_analysis_exclusion_delta_family(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256: bytes,
    old_excluded: int,
    new_excluded: int,
) -> tuple[AnalysisExclusionDeltaFamily, bool]:
    proposed = AnalysisExclusionDeltaFamily(
        analysis_id,
        file_sha256,
        old_excluded,
        new_excluded,
    )
    existing = load_analysis_exclusion_delta_family(
        connector,
        analysis_id=proposed.analysis_id,
        file_sha256=proposed.file_sha256,
    )
    if existing is not None:
        if existing != proposed:
            raise AnalysisFamilyCollisionError(
                "analysis exclusion delta replay differs"
            )
        return existing, False
    key = (proposed.analysis_id, proposed.file_sha256)
    connector.execute(
        f"INSERT INTO {_EXCLUSION_ANCHOR} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_OLD} "
        "(analysis_id, file_sha256, old_excluded) VALUES (%s, %s, %s)",
        (*key, proposed.old_excluded),
    )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_NEW} "
        "(analysis_id, file_sha256, new_excluded) VALUES (%s, %s, %s)",
        (*key, proposed.new_excluded),
    )
    if proposed.changed:
        connector.execute(
            f"INSERT INTO {_EXCLUSION_CHANGE} "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            key,
        )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_SEAL} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    return proposed, True


def insert_analysis_exclusion_delta_family(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256: bytes,
    old_excluded: int,
    new_excluded: int,
) -> AnalysisExclusionDeltaFamily:
    """Insert one fresh delta seal-last without an N-per-page existence read.

    Analysis checkpoints allocate every decision key at most once.  Response
    replay uses a bounded set validator, so the fresh mutation path can avoid a
    point lookup for every row while still rolling back on any conflicting key.
    """

    proposed = AnalysisExclusionDeltaFamily(
        analysis_id,
        file_sha256,
        old_excluded,
        new_excluded,
    )
    key = (proposed.analysis_id, proposed.file_sha256)
    connector.execute(
        f"INSERT INTO {_EXCLUSION_ANCHOR} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_OLD} "
        "(analysis_id, file_sha256, old_excluded) VALUES (%s, %s, %s)",
        (*key, proposed.old_excluded),
    )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_NEW} "
        "(analysis_id, file_sha256, new_excluded) VALUES (%s, %s, %s)",
        (*key, proposed.new_excluded),
    )
    if proposed.changed:
        connector.execute(
            f"INSERT INTO {_EXCLUSION_CHANGE} "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            key,
        )
    connector.execute(
        f"INSERT INTO {_EXCLUSION_SEAL} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    return proposed
