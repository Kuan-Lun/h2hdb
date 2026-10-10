"""Publication-commit cleanup and its compound preparation/event recovery."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from h2hdb._cleanup import keys as _keys
from h2hdb._cleanup import model as _model
from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup import static as _static
from h2hdb._cleanup.model import (
    _CHECKPOINT_TABLE,
    _EMPTY_CURSOR,
    CleanupCorruptionError,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    _CleanupOperation,
    _Mutation,
    _Mutator,
    _StaticScalar,
)
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan
from h2hdb.vnext_domains import require_int63, require_positive_int63, require_uuid16
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key


def _publication_commit_mutator(phase: str) -> _Mutator:
    def mutate(operation: _CleanupOperation, cursor: bytes) -> _Mutation:
        cycle = operation.cycle
        if cycle.target_kind is not CleanupTargetKind.PUBLICATION_COMMIT:
            raise CleanupCorruptionError("publication-commit cleanup kind drifted")
        plan = _PUBLICATION_COMMIT_PLAN
        if phase in {
            "PCOM_RELEASE_BUILD_BASE",
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
            "PCOM_FINALIZATION_MARKER",
            "PCOM_FINALIZATION_BATCH",
        }:
            _require_publication_commit_frozen_preparation_mapping(operation)
        if phase == "PCOM_RELEASE_BUILD_BASE":
            return _static._run_static_phase(operation, cursor, plan, phase)
        if phase in {
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
        }:
            _validate_publication_commit_preparation_authority(
                operation,
                phase=phase,
                cursor=cursor,
            )
        match phase:
            case "PCOM_EVENT":
                return _run_publication_commit_event_phase(operation, cursor)
            case "PCOM_COMMIT_EFFECT_ROOT":
                return _run_publication_commit_effect_root_phase(operation, cursor)
            case "PCOM_PREPARATION_BINDING":
                eligibility = _PUBLICATION_COMMIT_AFTER_BUILD_BASE_ELIGIBILITY
            case "PCOM_PREPARATION_BATCH":
                eligibility = _PUBLICATION_COMMIT_AFTER_PREPARATION_BINDING_ELIGIBILITY
            case "PCOM_PREPARATION_CHECKPOINT":
                eligibility = _PUBLICATION_COMMIT_AFTER_PREPARATION_BATCH_ELIGIBILITY
            case "PCOM_PREPARATION":
                eligibility = (
                    _PUBLICATION_COMMIT_AFTER_PREPARATION_CHECKPOINT_ELIGIBILITY
                )
            case "PCOM_FINALIZATION_MARKER":
                eligibility = _PUBLICATION_COMMIT_AFTER_EVENT_ELIGIBILITY
            case "PCOM_FINALIZATION_BATCH":
                eligibility = _PUBLICATION_COMMIT_AFTER_MARKER_ELIGIBILITY
            case "PCOM_FINALIZATION_CHECKPOINT":
                eligibility = _PUBLICATION_COMMIT_AFTER_COMPOUND_ROOT_ELIGIBILITY
            case _:
                eligibility = _PUBLICATION_COMMIT_AFTER_CHECKPOINT_ELIGIBILITY
        return _static._run_static_phase(
            operation,
            cursor,
            plan,
            phase,
            eligibility=eligibility,
            policy_parameters=(cycle.cleanup_id,),
        )

    return mutate


def _require_publication_commit_frozen_preparation_mapping(
    operation: _CleanupOperation,
) -> None:
    work = operation.work
    for root in operation.frozen_roots:
        receipt_id = require_uuid16(
            root[0],
            field="publication-commit frozen receipt_id",
        )
        preparation_id = require_uuid16(
            root[1],
            field="publication-commit frozen preparation_id",
        )
        if work.connector.fetch_one(
            "SELECT preparation_id FROM catalog_publication_commits "
            "WHERE receipt_id = %s",
            (receipt_id,),
        ) != (preparation_id,):
            raise CleanupCorruptionError(
                "publication-commit preparation differs from its frozen authority"
            )


def _validate_publication_commit_preparation_authority(
    operation: _CleanupOperation,
    *,
    phase: str,
    cursor: bytes,
) -> None:
    """Validate the exact commit-owned preparation control family before DML."""

    work = operation.work
    plan = _PUBLICATION_COMMIT_PLAN
    specs = plan.phases[phase]
    relation_index, cursor_values = _keys._decode_static_cursor(
        cursor,
        specs,
        len(plan.root_key),
    )
    frozen_roots = operation.frozen_roots
    for root in frozen_roots:
        if len(root) != 2:
            raise CleanupCorruptionError(
                "publication-commit frozen root has an invalid shape"
            )
        receipt_id = require_uuid16(
            root[0],
            field="publication-commit cleanup receipt_id",
        )
        frozen_preparation_id = require_uuid16(
            root[1],
            field="publication-commit cleanup preparation_id",
        )
        row = work.connector.fetch_one(
            "SELECT committed.candidate_id, committed.preparation_id, "
            "committed.operational_policy_id, preparation.preparation_id, "
            "preparation.state, preparation.operational_policy_id, "
            "by_candidate.candidate_id, by_candidate.preparation_id, "
            "by_preparation.candidate_id, by_preparation.preparation_id "
            "FROM catalog_publication_commit_anchors AS anchor "
            "LEFT JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = anchor.receipt_id "
            "LEFT JOIN operational_operational_preparations AS preparation "
            "ON preparation.preparation_id = committed.preparation_id "
            "LEFT JOIN operational_publication_candidate_preparations AS "
            "by_candidate ON by_candidate.candidate_id = committed.candidate_id "
            "LEFT JOIN operational_publication_candidate_preparations AS "
            "by_preparation "
            "ON by_preparation.preparation_id = committed.preparation_id "
            "WHERE anchor.receipt_id = %s",
            (receipt_id,),
        )
        if not row or len(row) != 10:
            raise CleanupCorruptionError(
                "publication-commit preparation authority is incomplete"
            )
        try:
            candidate_id = require_uuid16(
                row[0], field="publication-commit preparation candidate_id"
            )
            preparation_id = require_uuid16(
                row[1], field="publication-commit preparation_id"
            )
            operational_policy_id = require_positive_int63(
                row[2], field="publication-commit operational_policy_id"
            )
        except (TypeError, ValueError) as error:
            raise CleanupCorruptionError(
                "publication-commit preparation identity is malformed"
            ) from error
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "publication-commit preparation differs from its frozen authority"
            )

        candidate_binding = row[6:8]
        preparation_binding = row[8:10]
        absent_binding = (None, None)
        exact_binding = (candidate_id, preparation_id)
        if candidate_binding not in {absent_binding, exact_binding} or (
            preparation_binding not in {absent_binding, exact_binding}
        ):
            raise CleanupCorruptionError(
                "publication-commit candidate/preparation binding differs"
            )
        if candidate_binding != preparation_binding:
            raise CleanupCorruptionError(
                "publication-commit candidate/preparation binding is partial"
            )
        if phase == "PCOM_PREPARATION_BINDING":
            if candidate_binding == absent_binding and not (
                _publication_commit_preparation_cursor_covers_root(
                    receipt_id=receipt_id,
                    candidate_id=candidate_id,
                    preparation_id=preparation_id,
                    phase=phase,
                    relation_index=relation_index,
                    cursor_values=cursor_values,
                )
            ):
                raise CleanupCorruptionError(
                    "publication-commit preparation binding disappeared before its phase"
                )
        elif candidate_binding != absent_binding:
            raise CleanupCorruptionError(
                "publication-commit preparation binding survived its cleanup phase"
            )

        if row[3:6] == (preparation_id, "COMPLETE", operational_policy_id):
            continue
        if any(value is not None for value in row[3:6]):
            raise CleanupCorruptionError(
                "publication-commit preparation state or identity differs"
            )
        if not _publication_commit_preparation_cursor_covers_root(
            receipt_id=receipt_id,
            candidate_id=candidate_id,
            preparation_id=preparation_id,
            phase=phase,
            relation_index=relation_index,
            cursor_values=cursor_values,
        ):
            raise CleanupCorruptionError(
                "publication-commit preparation disappeared before its root phase"
            )


def _publication_commit_preparation_cursor_covers_root(
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    preparation_id: bytes,
    phase: str,
    relation_index: int,
    cursor_values: tuple[_StaticScalar, ...] | None,
) -> bool:
    if cursor_values is None:
        return False
    if relation_index != 0:
        raise CleanupCorruptionError(
            "publication-commit preparation ROOT cursor is malformed"
        )
    expected_arity = 3 if phase == "PCOM_PREPARATION_BINDING" else 2
    if (
        phase not in {"PCOM_PREPARATION_BINDING", "PCOM_PREPARATION"}
        or len(cursor_values) != expected_arity
    ):
        raise CleanupCorruptionError(
            "publication-commit preparation ROOT cursor is malformed"
        )
    cursor_receipt = require_uuid16(
        cursor_values[0], field="publication-commit preparation cursor receipt_id"
    )
    if phase == "PCOM_PREPARATION_BINDING":
        cursor_candidate = require_uuid16(
            cursor_values[1],
            field="publication-commit preparation cursor candidate_id",
        )
        cursor_preparation = require_uuid16(
            cursor_values[2],
            field="publication-commit preparation cursor preparation_id",
        )
        cursor_identity_matches = (
            candidate_id == cursor_candidate and preparation_id == cursor_preparation
        )
    else:
        cursor_preparation = require_uuid16(
            cursor_values[1],
            field="publication-commit preparation cursor preparation_id",
        )
        cursor_identity_matches = preparation_id == cursor_preparation
    return receipt_id < cursor_receipt or (
        receipt_id == cursor_receipt and cursor_identity_matches
    )


@dataclass(frozen=True, slots=True)
class _PublicationCommitEventCandidate:
    receipt_id: bytes
    preparation_id: bytes
    sequence_no: int
    event_id: bytes
    event_type: str

    @property
    def cursor_values(self) -> tuple[_StaticScalar, ...]:
        return (self.receipt_id, self.preparation_id, self.sequence_no)


def _publication_commit_event_cursor(
    cursor: bytes,
    *,
    plan: _StaticTargetPlan,
) -> tuple[bytes, bytes, int] | None:
    spec = plan.phases["PCOM_EVENT"]
    relation_index, values = _keys._decode_static_cursor(
        cursor,
        spec,
        len(plan.root_key),
    )
    if values is None:
        return None
    if relation_index != 0 or len(values) != 3:
        raise CleanupCorruptionError("publication-commit EVENT cursor is malformed")
    return (
        require_uuid16(values[0], field="PCOM EVENT cursor receipt_id"),
        require_uuid16(values[1], field="PCOM EVENT cursor preparation_id"),
        require_int63(values[2], field="PCOM EVENT cursor sequence_no"),
    )


def _publication_commit_event_authority(
    work: VNextUnitOfWork,
    *,
    cleanup_id: bytes,
    receipt_id: bytes,
) -> tuple[bytes, int]:
    row = work.connector.fetch_one(
        "SELECT committed.preparation_id, seal.event_count "
        "FROM catalog_publication_commit_anchors AS r "
        "JOIN catalog_publication_commits AS committed "
        "ON committed.receipt_id = r.receipt_id "
        "JOIN operational_operational_preparation_effect_seals AS seal "
        "ON seal.preparation_id = committed.preparation_id "
        "JOIN operational_operational_event_streams AS stream "
        "ON stream.preparation_id = committed.preparation_id "
        f"WHERE ({_PUBLICATION_COMMIT_EVENT_ELIGIBILITY}) "
        "AND r.receipt_id = %s",
        (cleanup_id, receipt_id),
    )
    if not row or len(row) != 2:
        raise CleanupRetentionBlockedError("PUBLICATION_COMMIT EVENT authority changed")
    return (
        require_uuid16(row[0], field="PCOM EVENT preparation_id"),
        require_int63(row[1], field="PCOM EVENT event_count"),
    )


def _validate_publication_commit_event_row(
    row: Sequence[object],
    *,
    receipt_id: bytes,
    preparation_id: bytes,
    expected_sequence_no: int,
) -> _PublicationCommitEventCandidate:
    if len(row) != 5:
        raise CleanupCorruptionError("PCOM EVENT row shape is invalid")
    sequence_no = require_int63(row[0], field="PCOM EVENT sequence_no")
    if sequence_no != expected_sequence_no:
        raise CleanupCorruptionError(
            "PCOM EVENT sequence coordinates are not contiguous"
        )
    event_id = require_uuid16(row[1], field="PCOM EVENT event_id")
    event_type = _model._as_text(row[2], field="PCOM EVENT event_type")
    removed_event_id = row[3]
    deletion_event_id = row[4]
    match event_type:
        case "REMOVED_GID":
            if removed_event_id != event_id or deletion_event_id is not None:
                raise CleanupCorruptionError(
                    "PCOM EVENT lacks its exact REMOVED_GID subtype"
                )
        case "DELETION_CONSUMPTION":
            if deletion_event_id != event_id or removed_event_id is not None:
                raise CleanupCorruptionError(
                    "PCOM EVENT lacks its exact DELETION_CONSUMPTION subtype"
                )
        case _:
            raise CleanupCorruptionError(
                "PCOM EVENT type is outside the closed registry"
            )
    return _PublicationCommitEventCandidate(
        receipt_id,
        preparation_id,
        sequence_no,
        event_id,
        event_type,
    )


def _run_publication_commit_event_phase(
    operation: _CleanupOperation,
    cursor: bytes,
) -> _Mutation:
    """Atomically retire each exact subtype/base-event coordinate."""

    work, cycle = operation.work, operation.cycle
    plan = _PUBLICATION_COMMIT_PLAN
    frozen_roots = operation.frozen_roots
    event_cursor = _publication_commit_event_cursor(cursor, plan=plan)
    frozen_authorities = tuple(
        (
            require_uuid16(root[0], field="PCOM EVENT frozen receipt_id"),
            require_uuid16(root[1], field="PCOM EVENT frozen preparation_id"),
        )
        for root in frozen_roots
    )
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    if event_cursor is not None and event_cursor[0] not in frozen_receipts:
        raise CleanupCorruptionError("PCOM EVENT cursor is outside its frozen roots")

    candidates: list[_PublicationCommitEventCandidate] = []
    remaining = cycle.max_rows_per_transaction
    for receipt_id, frozen_preparation_id in frozen_authorities:
        preparation_id, event_count = _publication_commit_event_authority(
            work,
            cleanup_id=cycle.cleanup_id,
            receipt_id=receipt_id,
        )
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "PCOM EVENT preparation differs from its frozen authority"
            )
        start_sequence = 0
        if event_cursor is not None:
            cursor_receipt, cursor_preparation, cursor_sequence = event_cursor
            if receipt_id < cursor_receipt:
                start_sequence = event_count
            elif receipt_id == cursor_receipt:
                if (
                    cursor_preparation != preparation_id
                    or cursor_sequence >= event_count
                ):
                    raise CleanupCorruptionError(
                        "PCOM EVENT cursor identity exceeds its sealed authority"
                    )
                start_sequence = cursor_sequence + 1

        if start_sequence:
            covered = work.connector.fetch_one(
                "SELECT 1 FROM operational_operational_events "
                "WHERE preparation_id = %s AND sequence_no < %s LIMIT 1",
                (preparation_id, start_sequence),
            )
            if covered:
                if covered != (1,):
                    raise CleanupCorruptionError(
                        "PCOM EVENT covered-coordinate probe is malformed"
                    )
                raise CleanupCorruptionError(
                    "PCOM EVENT cursor-covered coordinate reappeared"
                )

        if start_sequence == event_count:
            extra = work.connector.fetch_one(
                "SELECT 1 FROM operational_operational_events "
                "WHERE preparation_id = %s AND sequence_no >= %s LIMIT 1",
                (preparation_id, event_count),
            )
            if extra:
                raise CleanupCorruptionError(
                    "PCOM EVENT row exceeds the immutable seal count"
                )
            continue
        if start_sequence > event_count:
            raise CleanupCorruptionError("PCOM EVENT cursor exceeds the seal count")
        if remaining == 0:
            continue

        rows = work.connector.fetch_all(
            "SELECT event.sequence_no, event.event_id, event.event_type, "
            "removed.event_id, consumed.event_id "
            "FROM operational_operational_events AS event "
            "LEFT JOIN operational_operational_removed_gid_events AS removed "
            "ON removed.event_id = event.event_id "
            "LEFT JOIN operational_operational_deletion_consumption_events AS consumed "
            "ON consumed.event_id = event.event_id "
            "WHERE event.preparation_id = %s AND event.sequence_no >= %s "
            "ORDER BY event.sequence_no LIMIT %s",
            (preparation_id, start_sequence, remaining),
        )
        if not rows:
            raise CleanupCorruptionError(
                "PCOM EVENT has an uncovered sealed sequence gap"
            )
        expected = start_sequence
        for row in rows:
            candidate = _validate_publication_commit_event_row(
                row,
                receipt_id=receipt_id,
                preparation_id=preparation_id,
                expected_sequence_no=expected,
            )
            if candidate.sequence_no >= event_count:
                raise CleanupCorruptionError(
                    "PCOM EVENT row exceeds the immutable seal count"
                )
            candidates.append(candidate)
            expected += 1
        if len(rows) < remaining and expected != event_count:
            raise CleanupCorruptionError(
                "PCOM EVENT has an uncovered sealed sequence gap"
            )
        remaining -= len(rows)

    for candidate in sorted(
        candidates,
        key=lambda value: encode_lock_key(
            "cleanup-static",
            CleanupTargetKind.PUBLICATION_COMMIT.value,
            "PCOM_EVENT",
            0,
            *value.cursor_values,
        ),
    ):
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "cleanup-static",
                CleanupTargetKind.PUBLICATION_COMMIT.value,
                "PCOM_EVENT",
                0,
                *candidate.cursor_values,
            ),
            "SELECT event.sequence_no, event.event_id, event.event_type, "
            "removed.event_id, consumed.event_id "
            "FROM operational_operational_events AS event "
            "LEFT JOIN operational_operational_removed_gid_events AS removed "
            "ON removed.event_id = event.event_id "
            "LEFT JOIN operational_operational_deletion_consumption_events AS consumed "
            "ON consumed.event_id = event.event_id "
            "WHERE event.preparation_id = %s AND event.sequence_no = %s",
            (candidate.preparation_id, candidate.sequence_no),
        )
        exact = _validate_publication_commit_event_row(
            locked,
            receipt_id=candidate.receipt_id,
            preparation_id=candidate.preparation_id,
            expected_sequence_no=candidate.sequence_no,
        )
        if exact != candidate:
            raise CleanupRetentionBlockedError("PCOM EVENT coordinate changed")
        subtype_table = (
            "operational_operational_removed_gid_events"
            if candidate.event_type == "REMOVED_GID"
            else "operational_operational_deletion_consumption_events"
        )
        if (
            work.connector.execute_affected(
                f"DELETE FROM {subtype_table} WHERE event_id = %s",
                (candidate.event_id,),
            )
            != 1
        ):
            raise CleanupUnavailableError("PCOM EVENT subtype changed")
        if (
            work.connector.execute_affected(
                "DELETE FROM operational_operational_events "
                "WHERE event_id = %s AND preparation_id = %s AND sequence_no = %s",
                (
                    candidate.event_id,
                    candidate.preparation_id,
                    candidate.sequence_no,
                ),
            )
            != 1
        ):
            raise CleanupUnavailableError("PCOM EVENT base row changed")

    row_keys = tuple(
        _keys._encode_static_cursor(0, candidate.cursor_values)
        for candidate in candidates
    )
    next_cursor = row_keys[-1] if row_keys else cursor
    return _Mutation(next_cursor, row_keys)


def _run_publication_commit_effect_root_phase(
    operation: _CleanupOperation,
    cursor: bytes,
) -> _Mutation:
    """Delete every frozen commit/seal/stream triple in one bounded transaction."""

    work, cycle = operation.work, operation.cycle
    plan = _PUBLICATION_COMMIT_PLAN
    specs = plan.phases["PCOM_COMMIT_EFFECT_ROOT"]
    relation_index, cursor_values = _keys._decode_static_cursor(
        cursor,
        specs,
        len(plan.root_key),
    )
    frozen_roots = operation.frozen_roots
    frozen_authorities = tuple(
        (
            require_uuid16(root[0], field="PCOM compound frozen receipt_id"),
            require_uuid16(root[1], field="PCOM compound frozen preparation_id"),
        )
        for root in frozen_roots
    )
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    spec = specs[0]
    if cursor_values is not None:
        if relation_index != 0 or len(cursor_values) != 3:
            raise CleanupCorruptionError("PCOM compound cursor is malformed")
        cursor_root = require_uuid16(
            cursor_values[0], field="PCOM compound cursor root receipt_id"
        )
        cursor_receipt = require_uuid16(
            cursor_values[1], field="PCOM compound cursor commit receipt_id"
        )
        cursor_preparation = require_uuid16(
            cursor_values[2], field="PCOM compound cursor preparation_id"
        )
        if (
            not frozen_receipts
            or cursor_root != frozen_receipts[-1]
            or cursor_receipt != cursor_root
            or cursor_preparation != frozen_authorities[-1][1]
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor does not cover the exact frozen root set"
            )
        receipt = work.connector.fetch_one(
            f"SELECT receipt_start_cursor, cursor_bytes, receipt_row_count "
            f"FROM {_CHECKPOINT_TABLE} "
            "WHERE cleanup_id = %s AND phase = 'PCOM_COMMIT_EFFECT_ROOT'",
            (cycle.cleanup_id,),
        )
        if receipt != (_EMPTY_CURSOR, cursor, len(frozen_receipts)):
            raise CleanupCorruptionError(
                "PCOM compound cursor lacks its exact one-batch receipt proof"
            )
        for receipt_id, preparation_id in frozen_authorities:
            if work.connector.fetch_one(
                "SELECT 1 FROM catalog_publication_commits "
                "WHERE receipt_id = %s LIMIT 1",
                (receipt_id,),
            ):
                raise CleanupCorruptionError(
                    "PCOM compound cursor-covered commit reappeared"
                )
            _require_publication_commit_compound_authority_absent(
                work,
                preparation_id=preparation_id,
            )
        return _Mutation(cursor, ())

    candidates: list[tuple[_StaticScalar, ...]] = []
    for receipt_id, frozen_preparation_id in frozen_authorities:
        row = work.connector.fetch_one(
            "SELECT r.receipt_id, committed.receipt_id, committed.preparation_id "
            "FROM catalog_publication_commit_anchors AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id "
            "JOIN operational_operational_preparation_effect_seals AS seal "
            "ON seal.preparation_id = committed.preparation_id "
            "JOIN operational_operational_event_streams AS stream "
            "ON stream.preparation_id = committed.preparation_id "
            f"WHERE ({_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY}) "
            "AND r.receipt_id = %s",
            (cycle.cleanup_id, receipt_id),
        )
        if not row or len(row) != 3:
            raise CleanupCorruptionError(
                "PCOM compound root is missing or only partially present"
            )
        candidate = _keys._static_values(row)
        if candidate[0] != receipt_id or candidate[1] != receipt_id:
            raise CleanupCorruptionError("PCOM compound receipt identity differs")
        preparation_id = require_uuid16(
            candidate[2], field="PCOM compound preparation_id"
        )
        if preparation_id != frozen_preparation_id:
            raise CleanupCorruptionError(
                "PCOM compound preparation differs from its frozen authority"
            )
        candidates.append(candidate)

    for candidate in sorted(
        candidates,
        key=lambda value: encode_lock_key(
            "cleanup-static",
            CleanupTargetKind.PUBLICATION_COMMIT.value,
            "PCOM_COMMIT_EFFECT_ROOT",
            0,
            *value,
        ),
    ):
        locked = work.lock_row(
            LockRank.CHILD,
            encode_lock_key(
                "cleanup-static",
                CleanupTargetKind.PUBLICATION_COMMIT.value,
                "PCOM_COMMIT_EFFECT_ROOT",
                0,
                *candidate,
            ),
            "SELECT r.receipt_id, committed.receipt_id, committed.preparation_id "
            "FROM catalog_publication_commit_anchors AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id "
            "JOIN operational_operational_preparation_effect_seals AS seal "
            "ON seal.preparation_id = committed.preparation_id "
            "JOIN operational_operational_event_streams AS stream "
            "ON stream.preparation_id = committed.preparation_id "
            f"WHERE ({_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY}) "
            "AND r.receipt_id = %s AND committed.receipt_id = %s "
            "AND committed.preparation_id = %s",
            (cycle.cleanup_id, *candidate),
        )
        if _keys._static_values(locked) != candidate:
            raise CleanupRetentionBlockedError("PCOM compound root changed")
        primary = candidate[1:]
        for statement_index, statement in enumerate(spec.delete_sql):
            assert spec.delete_parameter_indexes is not None
            indexes = spec.delete_parameter_indexes[statement_index]
            parameters = tuple(primary[index] for index in indexes)
            if work.connector.execute_affected(statement, parameters) != 1:
                raise CleanupUnavailableError("PCOM compound root changed")

    for _receipt_id, preparation_id in frozen_authorities:
        _require_publication_commit_compound_authority_absent(
            work,
            preparation_id=preparation_id,
        )
    row_keys = tuple(
        _keys._encode_static_cursor(0, candidate) for candidate in candidates
    )
    return _Mutation(row_keys[-1] if row_keys else cursor, row_keys)


def _require_publication_commit_compound_authority_absent(
    work: VNextUnitOfWork,
    *,
    preparation_id: bytes,
) -> None:
    for table in (
        "operational_publication_candidate_preparations",
        "operational_operational_preparation_batch_receipts",
        "operational_operational_preparation_checkpoints",
        "operational_operational_preparations",
        "operational_operational_events",
        "operational_operational_preparation_effect_seals",
        "operational_operational_event_streams",
    ):
        if work.connector.fetch_one(
            f"SELECT 1 FROM {table} WHERE preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor-covered preparation authority reappeared"
            )
    for subtype_table in (
        "operational_operational_removed_gid_events",
        "operational_operational_deletion_consumption_events",
    ):
        if work.connector.fetch_one(
            f"SELECT 1 FROM {subtype_table} AS subtype "
            "JOIN operational_operational_events AS event "
            "ON event.event_id = subtype.event_id "
            "WHERE event.preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM compound cursor-covered typed event reappeared"
            )


def _require_publication_commit_post_compound_transition(
    operation: _CleanupOperation,
    *,
    phase: str,
    cursor: bytes,
) -> None:
    work = operation.work
    plan = _PUBLICATION_COMMIT_PLAN
    if phase not in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}:
        raise CleanupCorruptionError("PCOM post-compound phase is invalid")
    relation_index, cursor_values = _keys._decode_static_cursor(
        cursor,
        plan.phases[phase],
        len(plan.root_key),
    )
    cursor_receipt: bytes | None = None
    if cursor_values is not None:
        if relation_index != 0 or len(cursor_values) != 2:
            raise CleanupCorruptionError("PCOM post-compound cursor is malformed")
        cursor_receipt = require_uuid16(
            cursor_values[0], field="PCOM post-compound cursor root receipt_id"
        )
        if (
            require_uuid16(
                cursor_values[1],
                field="PCOM post-compound cursor row receipt_id",
            )
            != cursor_receipt
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound cursor receipt identity differs"
            )

    frozen_roots = operation.frozen_roots
    frozen_receipts = tuple(
        require_uuid16(root[0], field="PCOM post-compound frozen receipt_id")
        for root in frozen_roots
    )
    if cursor_receipt is not None and cursor_receipt not in frozen_receipts:
        raise CleanupCorruptionError(
            "PCOM post-compound cursor is outside its frozen roots"
        )
    for root in frozen_roots:
        receipt_id = require_uuid16(
            root[0], field="PCOM post-compound frozen receipt_id"
        )
        preparation_id = require_uuid16(
            root[1], field="PCOM post-compound frozen preparation_id"
        )
        if work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound commit authority reappeared"
            )
        if work.connector.fetch_one(
            "SELECT 1 FROM catalog_source_build_base_publication_commits "
            "WHERE base_receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise CleanupCorruptionError(
                "PCOM post-compound source-build base pin reappeared"
            )
        _require_publication_commit_compound_authority_absent(
            work,
            preparation_id=preparation_id,
        )
        for table, label in (
            (
                "catalog_publication_commit_finalizations",
                "finalization marker",
            ),
            (
                "catalog_publication_finalization_batch_stored",
                "finalization batch",
            ),
        ):
            if work.connector.fetch_one(
                f"SELECT 1 FROM {table} WHERE receipt_id = %s LIMIT 1",
                (receipt_id,),
            ):
                raise CleanupCorruptionError(f"PCOM post-compound {label} reappeared")

        checkpoint_row = work.connector.fetch_one(
            "SELECT state FROM catalog_publication_finalization_checkpoints "
            "WHERE receipt_id = %s",
            (receipt_id,),
        )
        anchor_row = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_commit_anchors WHERE receipt_id = %s",
            (receipt_id,),
        )
        covered = cursor_receipt is not None and receipt_id <= cursor_receipt
        if phase == "PCOM_FINALIZATION_CHECKPOINT":
            if (checkpoint_row == ()) != covered:
                raise CleanupCorruptionError(
                    "PCOM finalization-checkpoint cursor coverage differs"
                )
            if not covered and checkpoint_row != ("COMPLETE",):
                raise CleanupCorruptionError(
                    "PCOM uncovered finalization checkpoint is not COMPLETE"
                )
            if anchor_row != (1,):
                raise CleanupCorruptionError(
                    "PCOM anchor disappeared before its cleanup phase"
                )
            continue
        if checkpoint_row:
            raise CleanupCorruptionError(
                "PCOM finalization checkpoint reappeared after its phase"
            )
        if (anchor_row == ()) != covered:
            raise CleanupCorruptionError("PCOM anchor cursor coverage differs")
        if not covered and anchor_row != (1,):
            raise CleanupCorruptionError("PCOM uncovered anchor authority differs")


_PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY = """
EXISTS (
    SELECT 1
    FROM catalog_publication_commits committed
    JOIN catalog_publication_finalization_checkpoints checkpoint
      ON checkpoint.receipt_id = committed.receipt_id
     AND checkpoint.state = 'COMPLETE'
    JOIN catalog_source_revision_descriptors source_revision
      ON source_revision.source_revision = committed.source_revision
    JOIN catalog_publication_commit_head_receipts head
      ON head.channel = source_revision.channel
    JOIN catalog_publication_commits replacement
      ON replacement.receipt_id = head.receipt_id
    JOIN catalog_publication_receipts replacement_receipt
      ON replacement_receipt.receipt_id = replacement.receipt_id
    WHERE committed.receipt_id = r.receipt_id
      AND replacement.revision > committed.revision
      AND replacement_receipt.state = 'PUBLISHED'
      AND replacement_receipt.finalized_at IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM catalog_publication_commit_head_receipts retained_head
          WHERE retained_head.receipt_id = committed.receipt_id)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_publication_candidate_base_publication_commits base
          WHERE base.base_receipt_id = committed.receipt_id)
      AND NOT EXISTS (
          SELECT 1 FROM operational_gallery_redownload_states protected
          WHERE protected.through_source_revision = committed.source_revision)
      AND NOT EXISTS (
          SELECT 1 FROM catalog_prepared_artifacts protected
          WHERE protected.candidate_id = committed.candidate_id
            AND protected.state IN ('PENDING', 'PREPARED'))
      AND NOT EXISTS (
          SELECT 1
          FROM operational_operational_preparations preparation
          JOIN operational_source_working_builds working
            ON working.build_id = preparation.build_id
          WHERE preparation.preparation_id = committed.preparation_id)
      AND NOT EXISTS (
          SELECT 1 FROM operational_catalog_working_candidates working
          WHERE working.candidate_id = committed.candidate_id))
"""

_PUBLICATION_COMMIT_SAFE_BUILD_BASE_RELEASE = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_source_build_base_publication_commits base
    LEFT JOIN catalog_source_build_states build_state
      ON build_state.build_id = base.build_id
    WHERE base.base_receipt_id = r.receipt_id
      AND (
        build_state.build_id IS NULL
        OR build_state.state = 'OPEN'
        OR EXISTS (
            SELECT 1 FROM operational_source_working_builds working
            WHERE working.build_id = base.build_id)
        OR NOT EXISTS (
            SELECT 1
            FROM catalog_analysis_run_descriptor completed_analysis
            JOIN catalog_analysis_run_states completed_state
              ON completed_state.analysis_id = completed_analysis.analysis_id
            WHERE completed_analysis.build_id = base.build_id
              AND completed_state.state = 'COMPLETE')
        OR EXISTS (
            SELECT 1
            FROM catalog_analysis_run_descriptor analysis
            LEFT JOIN catalog_analysis_run_states analysis_state
              ON analysis_state.analysis_id = analysis.analysis_id
            WHERE analysis.build_id = base.build_id
              AND (
                analysis_state.analysis_id IS NULL
                OR analysis_state.state NOT IN ('COMPLETE', 'ABANDONED')
                OR (
                  analysis_state.state = 'COMPLETE'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM catalog_source_revision_provenance provenance
                      JOIN catalog_publication_commits handoff
                        ON handoff.source_revision = provenance.source_revision
                      JOIN catalog_publication_commit_head_receipts handoff_head
                        ON handoff_head.receipt_id = handoff.receipt_id
                      JOIN catalog_publication_receipts handoff_receipt
                        ON handoff_receipt.receipt_id = handoff.receipt_id
                      JOIN catalog_source_revision_descriptors handoff_source
                        ON handoff_source.source_revision = handoff.source_revision
                      JOIN catalog_publication_commits base_commit
                        ON base_commit.receipt_id = base.base_receipt_id
                      JOIN catalog_source_revision_descriptors base_source
                        ON base_source.source_revision = base_commit.source_revision
                      WHERE provenance.analysis_id = analysis.analysis_id
                        AND handoff_head.channel = base_source.channel
                        AND handoff_source.channel = handoff_head.channel
                        AND handoff.revision > base_commit.revision
                        AND handoff_receipt.state = 'PUBLISHED'
                        AND handoff_receipt.finalized_at IS NOT NULL))))))
"""

_PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY = """
AND EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparations preparation
      ON preparation.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id
      AND preparation.state = 'COMPLETE'
      AND preparation.operational_policy_id =
          preparation_owner.operational_policy_id)
"""

_PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY = """
AND EXISTS (
    SELECT 1 FROM catalog_publication_commits preparation_owner
    JOIN operational_publication_candidate_preparations binding
      ON binding.candidate_id = preparation_owner.candidate_id
     AND binding.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id
)
"""

_PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_publication_candidate_preparations binding
      ON (binding.candidate_id = preparation_owner.candidate_id
          OR binding.preparation_id = preparation_owner.preparation_id)
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparation_batch_receipts batch
      ON batch.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparation_checkpoints checkpoint
      ON checkpoint.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_PREPARATION_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_preparations preparation
      ON preparation.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_EVENTS_ABSENT = """
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    JOIN operational_operational_removed_gid_events removed
      ON removed.event_id = event.event_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_publication_commits preparation_owner
    JOIN operational_operational_events event
      ON event.preparation_id = preparation_owner.preparation_id
    JOIN operational_operational_deletion_consumption_events consumed
      ON consumed.event_id = event.event_id
    WHERE preparation_owner.receipt_id = r.receipt_id)
"""

_PUBLICATION_COMMIT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_SAFE_BUILD_BASE_RELEASE
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY
    + """
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
"""
)

_PUBLICATION_COMMIT_AFTER_BUILD_BASE_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_BINDING_AUTHORITY
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_RELEASE_BUILD_BASE'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_BINDING_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_BINDING'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_BATCH_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_BATCH'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_PREPARATION_CHECKPOINT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_EXACT_PREPARATION_AUTHORITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION_CHECKPOINT'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_EVENT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_PREPARATION'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_EVENT_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_EVENT'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_MARKER_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_MARKER'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_BATCH_ELIGIBILITY = (
    _PUBLICATION_COMMIT_REPLACEMENT_ELIGIBILITY
    + _PUBLICATION_COMMIT_PREPARATION_BINDING_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_BATCH_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_CHECKPOINT_ABSENT
    + _PUBLICATION_COMMIT_PREPARATION_ABSENT
    + _PUBLICATION_COMMIT_EVENTS_ABSENT
    + """
AND NOT EXISTS (
    SELECT 1 FROM catalog_source_build_base_publication_commits base
    WHERE base.base_receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_BATCH'
      AND completed.state = 'COMPLETE')
"""
)

_PUBLICATION_COMMIT_AFTER_COMPOUND_ROOT_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_COMMIT_EFFECT_ROOT'
      AND completed.state = 'COMPLETE')
"""

_PUBLICATION_COMMIT_AFTER_CHECKPOINT_ELIGIBILITY = """
NOT EXISTS (
    SELECT 1 FROM catalog_publication_commits committed
    WHERE committed.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_commit_finalizations finalized
    WHERE finalized.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_batch_stored batch
    WHERE batch.receipt_id = r.receipt_id)
AND NOT EXISTS (
    SELECT 1 FROM catalog_publication_finalization_checkpoints checkpoint
    WHERE checkpoint.receipt_id = r.receipt_id)
AND EXISTS (
    SELECT 1 FROM operational_cleanup_checkpoints completed
    WHERE completed.cleanup_id = %s
      AND completed.phase = 'PCOM_FINALIZATION_CHECKPOINT'
      AND completed.state = 'COMPLETE')
"""


def _publication_commit_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "catalog_publication_commit_anchors"
    key = ("receipt_id",)
    return {
        "PCOM_RELEASE_BUILD_BASE": (
            _plan._owned_spec(
                "catalog_source_build_base_publication_commits",
                ("build_id",),
                root,
                key,
                ("base_receipt_id",),
            ),
        ),
        "PCOM_PREPARATION_BINDING": (
            _plan._indirect_spec(
                "operational_publication_candidate_preparations",
                ("candidate_id", "preparation_id"),
                "operational_publication_candidate_preparations AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.candidate_id = c.candidate_id "
                "AND committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION_BATCH": (
            _plan._indirect_spec(
                "operational_operational_preparation_batch_receipts",
                ("preparation_id", "phase", "batch_key"),
                "operational_operational_preparation_batch_receipts AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION_CHECKPOINT": (
            _plan._indirect_spec(
                "operational_operational_preparation_checkpoints",
                ("preparation_id", "phase"),
                "operational_operational_preparation_checkpoints AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_PREPARATION": (
            _plan._indirect_spec(
                "operational_operational_preparations",
                ("preparation_id",),
                "operational_operational_preparations AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
                extra_predicate="c.state = 'COMPLETE'",
            ),
        ),
        "PCOM_EVENT": (
            _plan._indirect_spec(
                "operational_operational_events",
                ("preparation_id", "sequence_no"),
                "operational_operational_events AS c "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = committed.receipt_id",
            ),
        ),
        "PCOM_FINALIZATION_MARKER": (
            _plan._owned_spec(
                "catalog_publication_commit_finalizations",
                key,
                root,
                key,
            ),
        ),
        "PCOM_FINALIZATION_BATCH": (
            _plan._owned_spec(
                "catalog_publication_finalization_batch_stored",
                ("receipt_id", "start_generation"),
                root,
                key,
            ),
        ),
        "PCOM_COMMIT_EFFECT_ROOT": (
            _plan._indirect_spec(
                "catalog_publication_commits",
                ("receipt_id", "preparation_id"),
                "catalog_publication_commits AS c "
                "JOIN operational_operational_preparation_effect_seals AS seal "
                "ON seal.preparation_id = c.preparation_id "
                "JOIN operational_operational_event_streams AS stream "
                "ON stream.preparation_id = c.preparation_id "
                "JOIN catalog_publication_commit_anchors AS r "
                "ON r.receipt_id = c.receipt_id",
                delete_sql=(
                    "DELETE FROM catalog_publication_commits "
                    "WHERE receipt_id = %s AND preparation_id = %s",
                    "DELETE FROM operational_operational_preparation_effect_seals "
                    "WHERE preparation_id = %s",
                    "DELETE FROM operational_operational_event_streams "
                    "WHERE preparation_id = %s",
                ),
                delete_parameter_indexes=((0, 1), (1,), (1,)),
                delete_allowed_affected=(
                    frozenset((1,)),
                    frozenset((1,)),
                    frozenset((1,)),
                ),
            ),
        ),
        "PCOM_FINALIZATION_CHECKPOINT": (
            _plan._owned_spec(
                "catalog_publication_finalization_checkpoints",
                key,
                root,
                key,
            ),
        ),
        "PCOM_ANCHOR": (_plan._owned_spec(root, key, root, key),),
    }


_PUBLICATION_COMMIT_PLAN = _StaticTargetPlan(
    CleanupTargetKind.PUBLICATION_COMMIT,
    "catalog_publication_commit_anchors",
    ("receipt_id",),
    "receipt_id",
    16,
    _PUBLICATION_COMMIT_ELIGIBILITY,
    _publication_commit_phases(),
)
