"""Sealed frozen-root ownership for a single durable cleanup cycle."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from h2hdb._cleanup import keys as _keys
from h2hdb._cleanup.model import (
    _CLEANUP_ALGORITHM_VERSION,
    _FROZEN_ROOT_TABLE,
    _JOB_TABLE,
    _MAX_BATCH_ROWS,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupTargetKind,
    CleanupUnavailableError,
    _StaticScalar,
)
from h2hdb._cleanup.plan import _StaticTargetPlan
from h2hdb.vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_uuid16,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_FROZEN_ROOT_SET_DOMAIN = b"h2hdb-cleanup-frozen-root-set-v1\0"

_MAX_FROZEN_ROOT_KEY_BYTES = 260

_FROZEN_ROOT_INT_ATTRIBUTES = frozenset(
    {
        "gallery_id",
        "generation",
        "gid",
        "observation_id",
        "revision",
        "source_revision",
    }
)

_FROZEN_ROOT_UUID_ATTRIBUTES = frozenset(
    {
        "analysis_id",
        "build_id",
        "collection_id",
        "candidate_id",
        "preparation_id",
        "receipt_id",
        "staging_id",
    }
)

_FROZEN_ROOT_DIGEST_ATTRIBUTES = frozenset(
    {
        "artifact_sha256",
        "file_key",
        "file_sha256",
        "fingerprint_sha256",
        "page_sha256",
        "publication_key",
        "source_identity_sha256",
        "storage_object_key_sha256",
        "value_sha256",
    }
)


def _frozen_root_attributes(plan: _StaticTargetPlan) -> tuple[str, ...]:
    """Return the immutable authority carried by one frozen protocol root."""

    if plan.kind is CleanupTargetKind.PUBLICATION_COMMIT:
        return (*plan.root_key, "preparation_id")
    return plan.root_key


def _validate_frozen_root_values(
    plan: _StaticTargetPlan,
    root: Sequence[_StaticScalar],
) -> None:
    attributes = _frozen_root_attributes(plan)
    if len(root) != len(attributes):
        raise CleanupCorruptionError("cleanup frozen root arity drifted")
    for attribute, value in zip(attributes, root, strict=True):
        if attribute in _FROZEN_ROOT_INT_ATTRIBUTES:
            if isinstance(value, bool) or not isinstance(value, int):
                raise CleanupCorruptionError("cleanup frozen root integer type drifted")
            require_int63(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute in _FROZEN_ROOT_UUID_ATTRIBUTES:
            require_uuid16(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute in _FROZEN_ROOT_DIGEST_ATTRIBUTES:
            require_digest32(value, field=f"cleanup frozen root {attribute}")
            continue
        if attribute == "source_gallery_name":
            require_bounded_bytes(
                value,
                field="cleanup frozen root source_gallery_name",
                minimum=1,
                maximum=255,
            )
            continue
        raise CleanupCorruptionError(
            f"cleanup root attribute {attribute!r} lacks a registered codec"
        )


def _encode_frozen_root_key(values: Sequence[_StaticScalar]) -> bytes:
    if not values or len(values) > 255:
        raise CleanupCorruptionError("cleanup frozen root key arity is invalid")
    encoded = bytearray(b"\x01" + bytes((len(values),)))
    for value in values:
        encoded.extend(_keys._encode_static_scalar(value))
    return require_bounded_bytes(
        bytes(encoded),
        field="cleanup frozen root key",
        maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
    )


def _decode_frozen_root_key(
    value: object,
    *,
    root_arity: int,
) -> tuple[_StaticScalar, ...]:
    payload = require_bounded_bytes(
        value,
        field="cleanup frozen root key",
        minimum=3,
        maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
    )
    if payload[0] != 1 or payload[1] != root_arity:
        raise CleanupCorruptionError("cleanup frozen root key codec is invalid")
    return _keys._decode_static_scalars(
        payload,
        offset=2,
        count=root_arity,
        field="cleanup frozen root key",
    )


def _require_frozen_root_count(value: object) -> int:
    count = require_int63(value, field="cleanup frozen_root_count")
    if count > _MAX_BATCH_ROWS:
        raise CleanupCorruptionError("cleanup frozen root set exceeds its hard cap")
    return count


def _frozen_root_set_sha256(
    cleanup_id: bytes,
    encoded_roots: Sequence[bytes],
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_FROZEN_ROOT_SET_DOMAIN)
    digest.update(require_uuid16(cleanup_id, field="cleanup frozen root cleanup_id"))
    ordered = tuple(sorted(encoded_roots))
    digest.update(len(ordered).to_bytes(2, "big"))
    for encoded in ordered:
        value = require_bounded_bytes(
            encoded,
            field="cleanup frozen root key",
            minimum=3,
            maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
        )
        digest.update(len(value).to_bytes(2, "big"))
        digest.update(value)
    return digest.digest()


def _frozen_root_predicate(
    plan: _StaticTargetPlan,
    roots: Sequence[tuple[_StaticScalar, ...]],
) -> tuple[str, tuple[_StaticScalar, ...]]:
    if not roots:
        return "0 = 1", ()
    branches: list[str] = []
    parameters: list[_StaticScalar] = []
    for root in roots:
        if len(root) != len(_frozen_root_attributes(plan)):
            raise CleanupCorruptionError("cleanup frozen root arity drifted")
        branches.append(
            "(" + " AND ".join(f"r.{column} = %s" for column in plan.root_key) + ")"
        )
        parameters.extend(root[: len(plan.root_key)])
    return " OR ".join(branches), tuple(parameters)


def _freeze_static_cycle_roots(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    plan: _StaticTargetPlan | None,
) -> None:
    if plan is None:
        return
    root_columns = ", ".join(f"r.{column}" for column in plan.root_key)
    if plan.kind is CleanupTargetKind.PUBLICATION_COMMIT:
        selected_columns = f"{root_columns}, committed.preparation_id"
        root_source = (
            f"{plan.root_table} AS r "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = r.receipt_id"
        )
    else:
        selected_columns = root_columns
        root_source = f"{plan.root_table} AS r"
    query = (
        f"SELECT {selected_columns} FROM {root_source} "
        f"WHERE ({plan.eligibility}) AND ({_keys._static_shard_sql(plan)}) "
        f"ORDER BY {selected_columns} LIMIT %s"
    )
    rows = work.connector.fetch_all(
        query,
        _keys._static_policy_parameters(plan, cycle)
        + _keys._static_shard_parameters(plan, cycle)
        + (cycle.max_rows_per_transaction,),
    )
    roots = tuple(_keys._static_values(row) for row in rows)
    if plan.contiguous_integer_prefix and roots:
        first_generation = require_int63(
            roots[0][0], field="publication-generation prefix floor"
        )
        if cycle.shard_no != first_generation % 256:
            raise CleanupUnavailableError(
                "publication-generation cycle slot does not match its oldest prefix"
            )
    for root in roots:
        _validate_frozen_root_values(plan, root)
    encoded_roots = tuple(_encode_frozen_root_key(root) for root in roots)
    if len(set(encoded_roots)) != len(encoded_roots):
        raise CleanupCorruptionError("cleanup frozen root query returned duplicates")
    for encoded in encoded_roots:
        work.connector.execute(
            f"INSERT INTO {_FROZEN_ROOT_TABLE} (cleanup_id, frozen_root_key) "
            "VALUES (%s, %s)",
            (cycle.cleanup_id, encoded),
        )
    if not encoded_roots:
        return
    expected_empty_digest = _frozen_root_set_sha256(cycle.cleanup_id, ())
    affected = work.connector.execute_affected(
        f"UPDATE {_JOB_TABLE} "
        "SET frozen_root_count = %s, frozen_root_set_sha256 = %s "
        "WHERE cleanup_id = %s AND target_key = %s "
        "AND cycle_generation = %s AND algorithm_version = %s "
        "AND frozen_root_count = 0 AND frozen_root_set_sha256 = %s "
        "AND state = 'OPEN'",
        (
            len(encoded_roots),
            _frozen_root_set_sha256(cycle.cleanup_id, encoded_roots),
            cycle.cleanup_id,
            cycle.target_key,
            cycle.cycle_generation,
            _CLEANUP_ALGORITHM_VERSION,
            expected_empty_digest,
        ),
    )
    if affected != 1:
        raise CleanupUnavailableError("cleanup frozen root seal changed")


def _load_frozen_roots(
    work: VNextUnitOfWork,
    cycle: CleanupCycle,
    plan: _StaticTargetPlan | None,
    *,
    expected_count: int | None = None,
    expected_digest: bytes | None = None,
) -> tuple[tuple[_StaticScalar, ...], ...]:
    if expected_count is None or expected_digest is None:
        job = work.connector.fetch_one(
            f"SELECT frozen_root_count, frozen_root_set_sha256 "
            f"FROM {_JOB_TABLE} WHERE cleanup_id = %s AND target_key = %s "
            "AND cycle_generation = %s AND state = 'OPEN'",
            (cycle.cleanup_id, cycle.target_key, cycle.cycle_generation),
        )
        if not job or len(job) != 2:
            raise CleanupUnavailableError("OPEN cleanup frozen root seal is missing")
        expected_count = _require_frozen_root_count(job[0])
        expected_digest = require_digest32(
            job[1], field="cleanup frozen_root_set_sha256"
        )
    else:
        expected_count = _require_frozen_root_count(expected_count)
        expected_digest = require_digest32(
            expected_digest, field="cleanup frozen_root_set_sha256"
        )
    if expected_count > cycle.max_rows_per_transaction:
        raise CleanupCorruptionError("cleanup frozen root count exceeds cycle policy")
    rows = work.connector.fetch_all(
        f"SELECT frozen_root_key FROM {_FROZEN_ROOT_TABLE} "
        "WHERE cleanup_id = %s ORDER BY frozen_root_key LIMIT %s",
        (cycle.cleanup_id, cycle.max_rows_per_transaction + 1),
    )
    if len(rows) != expected_count:
        raise CleanupCorruptionError("cleanup frozen root membership count drifted")
    encoded_roots: list[bytes] = []
    roots: list[tuple[_StaticScalar, ...]] = []
    for row in rows:
        if len(row) != 1:
            raise CleanupCorruptionError("cleanup frozen root row shape is invalid")
        encoded = require_bounded_bytes(
            row[0],
            field="cleanup frozen root key",
            minimum=3,
            maximum=_MAX_FROZEN_ROOT_KEY_BYTES,
        )
        encoded_roots.append(encoded)
        if plan is None:
            raise CleanupCorruptionError(
                "non-static cleanup cycle retained a frozen root"
            )
        root = _decode_frozen_root_key(
            encoded,
            root_arity=len(_frozen_root_attributes(plan)),
        )
        _validate_frozen_root_values(plan, root)
        if _encode_frozen_root_key(root) != encoded:
            raise CleanupCorruptionError("cleanup frozen root key is non-canonical")
        roots.append(root)
    if len(set(encoded_roots)) != expected_count or len(set(roots)) != expected_count:
        raise CleanupCorruptionError("cleanup frozen root membership is duplicated")
    if len(roots) != expected_count:
        raise CleanupCorruptionError("cleanup decoded frozen root count drifted")
    if _frozen_root_set_sha256(cycle.cleanup_id, encoded_roots) != expected_digest:
        raise CleanupCorruptionError("cleanup frozen root seal digest drifted")
    if plan is None and expected_count != 0:
        raise CleanupCorruptionError("non-static cleanup has frozen root membership")
    return tuple(roots)


def _require_no_frozen_roots(work: VNextUnitOfWork, cleanup_id: bytes) -> None:
    row = work.connector.fetch_one(
        f"SELECT 1 FROM {_FROZEN_ROOT_TABLE} WHERE cleanup_id = %s LIMIT 1",
        (cleanup_id,),
    )
    if not row:
        return
    if row != (1,):
        raise CleanupCorruptionError("cleanup frozen root probe is malformed")
    raise CleanupCorruptionError("COMPLETE cleanup retains frozen root membership")
