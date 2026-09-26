#!/usr/bin/env python3
"""Offline conversion of exact schema 8 to observation-owned upload timestamps.

All database consumers must remain stopped until READY commits. The source
artifact is extracted from an explicitly supplied Core 0.42.2 wheel by the
bundle builder; it is checksum-pinned data. Only a separate historical worker
loads the pinned old wheel to finish retained OPEN cleanup, when necessary.
MariaDB DDL commits separately and is resumed by exact physical shape. Data
copy SQL is keyset-paged; SQLite constraint replacement additionally needs one
atomic transaction per rebuilt table, whose duration depends on that table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
from email.parser import BytesParser
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from time import perf_counter_ns
from typing import Any, cast

from h2hdb import CoreConfig, DatabaseAccessMode, DatabaseAuditPolicy, load_config
from h2hdb._schema_artifact_codec import decode_schema_artifact
from h2hdb.database_audit import (
    DatabaseAuditStateRepository,
    _State,
    _validator_version,
    read_database_audit_state,
)
from h2hdb.database_clock import database_unix_microseconds
from h2hdb.database_performance import DatabasePerformance
from h2hdb.logger import HentaiDBLogger, _route_database_diagnostics
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    MariaDBAdvisorySchemaEpochGate,
    MariaDBSchemaEpochCatalog,
    SchemaCreateStatement,
    SchemaEpochAdmissionError,
    SchemaEpochCatalog,
    SchemaEpochDefinition,
    SchemaEpochRunner,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaObjectKind,
    SchemaSemanticValidationPhase,
    SchemaSlice,
    SQLiteSchemaEpochCatalog,
    mariadb_schema_epoch_gate_name,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
from h2hdb.vnext_transaction import VNextUnitOfWork

SOURCE_SCHEMA_SIZE = 4_429_649
SOURCE_SCHEMA_SHA256 = (
    "a8a425f4d697368035f9ead3256c972560ec73cebf6b03b2047e7d65708e1828"
)
_SOURCE_MANIFESTS = {
    "sqlite": "0d1f17682f461388b159df7d53b017a8fb8bc7af7dafaf395946513c4be7717b",
    "mariadb": "09ff699ec5d63785cda128a709d30fbe5e3edab3e0e685475d941b890c6e2426",
}
_PAGE_ROWS = 128
_ADDITIONS = frozenset(
    {
        "gallery_gid_identity",
        "gallery_observation_upload_time",
        "catalog_publication_upload_time",
    }
)
_REMOVALS = frozenset({"gallery_upload_time"})
_AFFECTED_CLEANUP_KINDS = (
    "GALLERY_UPLOAD_TIME",
    "CATALOG_PUBLICATION",
    "GALLERY_OBSERVATION",
    "PUBLICATION_CANDIDATE",
)


class _Phase(StrEnum):
    COPY = "copy"
    COPIED = "copied"
    STRUCTURE = "structure"
    AUDITED = "audited"


@dataclass(frozen=True)
class _Snapshot:
    payload: Mapping[str, Any]
    slices: Mapping[str, SchemaSlice]
    relations: Mapping[str, Mapping[str, Any]]

    @property
    def objects(self) -> frozenset[SchemaObject]:
        return frozenset(
            statement.creates
            for part in self.slices.values()
            for statement in part.statements
        )


def _snapshot(payload: Mapping[str, Any]) -> _Snapshot:
    slices = {}
    for identifier, statements in payload["slices"]:
        part = SchemaSlice(
            identifier,
            tuple(
                SchemaCreateStatement(
                    identifier, sql, SchemaObject(SchemaObjectKind(kind), name)
                )
                for identifier, kind, name, sql in statements
            ),
        )
        slices[part.slice_id.removeprefix("relation:")] = part
    return _Snapshot(
        payload,
        slices,
        {relation["relation"]: relation for relation in payload["relations"]},
    )


def _load_source(path: Path, backend: str) -> _Snapshot:
    with path.open("rb") as stream:
        raw = stream.read(SOURCE_SCHEMA_SIZE + 1)
    artifact = decode_schema_artifact(
        raw,
        pickle_protocol=5,
        raw_size=SOURCE_SCHEMA_SIZE,
        raw_sha256=SOURCE_SCHEMA_SHA256,
    )
    if artifact["epoch"] != 3 or artifact["schema_version"] != 8:
        raise SchemaEpochAdmissionError("Source artifact is not schema 8")
    return _snapshot(artifact["backends"][backend])


@dataclass(frozen=True)
class _Plan:
    provider: GeneratedVNextSchemaProvider
    definition: SchemaEpochDefinition
    source: _Snapshot
    target: _Snapshot
    validator_version: str
    source_wheel_sha256: str = ""

    @property
    def backend(self) -> str:
        return self.provider.backend

    def marker(self, phase: _Phase, audit: _State | None = None) -> bytes:
        content = (
            "h2hdb-offline-observation-upload-time-8-to-9/1",
            self.backend,
            _SOURCE_MANIFESTS[self.backend],
            self.definition.manifest_sha256,
            self.validator_version,
            self.source_wheel_sha256,
            phase.value,
            None if audit is None else audit.values(),
        )
        return hashlib.sha256(
            json.dumps(
                content,
                separators=(",", ":"),
                default=lambda value: value.hex(),
            ).encode("ascii")
        ).digest()


def _plan(
    provider: GeneratedVNextSchemaProvider,
    source_path: Path,
    *,
    source_wheel_sha256: str = "",
) -> _Plan:
    definition = provider.definition
    if (definition.epoch, definition.schema_version) != (3, 9):
        raise SchemaEpochAdmissionError("This converter requires schema 9 software")
    source = _load_source(source_path, provider.backend)
    target = _snapshot(provider.generated_definition_data)
    if set(target.slices) - set(source.slices) != _ADDITIONS:
        raise SchemaEpochAdmissionError(
            "Target schema additions differ from this conversion"
        )
    if set(source.slices) - set(target.slices) != _REMOVALS:
        raise SchemaEpochAdmissionError(
            "Target schema removals differ from this conversion"
        )
    return _Plan(
        provider, definition, source, target, _validator_version(), source_wheel_sha256
    )


def _control(connector: SQLConnector) -> tuple[object, ...]:
    rows = connector.fetch_all(
        "SELECT singleton_id, epoch, schema_version, state, manifest_sha256, started_at, ready_at "
        "FROM h2hdb_schema_epoch LIMIT 2"
    )
    if len(rows) != 1 or len(rows[0]) != 7:
        raise SchemaEpochAdmissionError("Expected one exact epoch control row")
    row = tuple(rows[0])
    if (
        type(row[0]) is not int
        or row[0] != 1
        or type(row[1]) is not int
        or row[1] != 3
        or type(row[2]) is not int
        or not isinstance(row[4], bytes)
        or type(row[5]) is not int
        or row[5] < 0
    ):
        raise SchemaEpochAdmissionError("Invalid conversion control values")
    return row


def _change_marker(connector: SQLConnector, previous: bytes, following: bytes) -> None:
    affected = connector.execute_affected(
        "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s "
        "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 9 "
        "AND state = 'BUILDING' AND ready_at IS NULL AND manifest_sha256 = %s",
        (following, previous),
    )
    if affected != 1:
        raise SchemaEpochAdmissionError("Conversion lost its exact phase control")


def _retire_audit_owner(connector: SQLConnector, plan: _Plan) -> None:
    previous = read_database_audit_state(connector)
    if previous is None:
        return
    if previous.generation >= INT63_MAX:
        raise SchemaEpochAdmissionError("Audit generation is exhausted")
    following = replace(
        previous,
        generation=previous.generation + 1,
        owner_token=secrets.token_bytes(16),
        lease_expires_at=0,
        audit_pending=1,
    )
    DatabaseAuditStateRepository.save(
        VNextUnitOfWork(connector, backend=plan.backend), previous, following
    )


def _record_audit(
    connector: SQLConnector, plan: _Plan, previous: _State | None, elapsed: int
) -> _State:
    actual = read_database_audit_state(connector)
    if actual != previous:
        raise SchemaEpochAdmissionError(
            "Audit scheduling authority changed during conversion"
        )
    now = database_unix_microseconds(VNextUnitOfWork(connector, backend=plan.backend))
    if actual is None:
        policy = DatabaseAuditPolicy()
        actual = _State(
            1,
            secrets.token_bytes(16),
            0,
            60_000_000,
            policy.minimum_interval_microseconds,
            policy.duration_multiplier,
            None,
            None,
            None,
            None,
            None,
            1,
        )
    interval = min(
        INT63_MAX,
        max(actual.minimum_interval_microseconds, elapsed * actual.duration_multiplier),
    )
    following = replace(
        actual,
        lease_expires_at=None,
        last_audit_at=now,
        audit_duration_microseconds=elapsed,
        validator_version=plan.validator_version,
        next_audit_at=min(INT63_MAX, now + interval),
        audit_pending=0,
    )
    DatabaseAuditStateRepository.save(
        VNextUnitOfWork(connector, backend=plan.backend), previous, following
    )
    _change_marker(
        connector, plan.marker(_Phase.STRUCTURE), plan.marker(_Phase.AUDITED, following)
    )
    return following


@contextmanager
def _exclusive_conversion(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "mariadb":
        database = connector.fetch_one("SELECT DATABASE()")
        connector.commit()
        if len(database) != 1 or not isinstance(database[0], str):
            raise SchemaEpochAdmissionError("Cannot identify conversion database")
        with MariaDBAdvisorySchemaEpochGate().acquire_named(
            connector, mariadb_schema_epoch_gate_name(database[0])
        ):
            yield
        return
    mode = connector.fetch_one("PRAGMA locking_mode = EXCLUSIVE")
    if mode != ("exclusive",):
        raise SchemaEpochAdmissionError(
            "Cannot acquire SQLite exclusive conversion mode"
        )
    with connector.transaction():
        connector.fetch_one("SELECT singleton_id FROM h2hdb_schema_epoch")
    # EXCLUSIVE connection locking retains ownership over page commits. Only
    # closing this connection releases it; each data page still has its own txn.
    yield


def _validate_slice(
    connector: SQLConnector,
    plan: _Plan,
    snapshot: _Snapshot,
    name: str,
    *,
    inspect_view_columns: bool = True,
) -> None:
    for statement in snapshot.slices[name].statements:
        plan.provider._validate_object_sql(
            connector,
            kind=statement.creates.kind.value,
            name=statement.creates.name,
            expected_sql=statement.sql,
        )
    if inspect_view_columns or snapshot.relations[name]["kind"] != "view":
        plan.provider._validate_relation_shape(connector, snapshot.relations[name])


def _keyset_query(
    source_query: str,
    columns: tuple[str, ...],
    keys: tuple[str, ...],
    cursor: tuple[object, ...],
    limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Bound each disjoint PK range before merging a composite-key page.

    An OR over key prefixes may scan the entire retained prefix on either
    backend. Separate range seeks also bound the final merge to key_width *
    limit rows, independent of the already copied prefix.
    """
    if not keys or len(keys) > 2 or not 1 <= limit <= _PAGE_ROWS + 1:
        raise ValueError("Unsupported offline copy keyset bound")
    selected, ordered = ", ".join(columns), ", ".join(keys)
    base = f"SELECT {selected} FROM ({source_query}) source_rows"
    if not cursor:
        return f"{base} ORDER BY {ordered} LIMIT {limit}", ()
    branches: list[str] = []
    values: list[object] = []
    for index, key in enumerate(keys):
        condition = " AND ".join(
            [*(f"{prior} = %s" for prior in keys[:index]), f"{key} > %s"]
        )
        branches.append(
            f"SELECT {selected} FROM ({base} WHERE {condition} "
            f"ORDER BY {ordered} LIMIT {limit}) range_{index}"
        )
        values.extend(cursor[: index + 1])
    return (
        f"SELECT {selected} FROM ({' UNION ALL '.join(branches)}) keyset_page "
        f"ORDER BY {ordered} LIMIT {limit}",
        tuple(values),
    )


def _copy_rows(
    connector: SQLConnector,
    *,
    source_query: str,
    destination: str,
    columns: tuple[str, ...],
    key_width: int,
    checkpoint: Callable[[str], None],
    manage_transactions: bool = True,
) -> int:
    """Exactly copy immutable rows with bounded statements and page commits.

    The destination itself supplies replay evidence, never an assumed MAX-key
    cursor. A restart rechecks already copied pages; it cannot skip a missing
    prefix or silently accept destination-only keys.
    """
    keys = columns[:key_width]
    cursor: tuple[object, ...] = ()
    total = 0
    while True:
        with connector.transaction() if manage_transactions else nullcontext():
            rows = connector.fetch_all(
                *_keyset_query(source_query, columns, keys, cursor, _PAGE_ROWS)
            )
            destination_page = connector.fetch_all(
                *_keyset_query(
                    f"SELECT {', '.join(columns)} FROM {destination}",
                    columns,
                    keys,
                    cursor,
                    _PAGE_ROWS + 1,
                )
            )
            if not rows:
                if destination_page:
                    raise SchemaEpochValidationError(
                        "Destination has unexpected copied keys"
                    )
                checkpoint(f"copy_complete relation={destination} rows={total}")
                return total
            if any(any(value is None for value in row) for row in rows):
                raise SchemaEpochValidationError(
                    "Source upload timestamp authority is incomplete"
                )
            ending = tuple(rows[-1][:key_width])
            existing = [
                row for row in destination_page if tuple(row[:key_width]) <= ending
            ]
            expected = {tuple(row[:key_width]): tuple(row) for row in rows}
            if any(
                expected.get(tuple(row[:key_width])) != tuple(row) for row in existing
            ):
                raise SchemaEpochValidationError(
                    "Previously copied row conflicts with source authority"
                )
            found = {tuple(row[:key_width]) for row in existing}
            missing = [
                tuple(row) for row in rows if tuple(row[:key_width]) not in found
            ]
            if missing:
                connector.execute_many(
                    f"INSERT INTO {destination} ({', '.join(columns)}) "
                    f"VALUES ({', '.join('%s' for _ in columns)})",
                    missing,
                )
            checkpoint("copy_page_before_commit")
        total += len(rows)
        cursor = ending
        checkpoint("copy_page_committed" if manage_transactions else "copy_page_staged")
        checkpoint(
            f"copy_progress relation={destination} rows={total} "
            f"page_rows={len(rows)} inserted_rows={len(missing)} "
            f"durability={'committed' if manage_transactions else 'staged'}"
        )


def _copy_upload_times(
    connector: SQLConnector, *, checkpoint: Callable[[str], None]
) -> None:
    _copy_rows(
        connector,
        source_query="SELECT gid FROM catalog_gallery_upload_times",
        destination="catalog_gallery_gid_identities",
        columns=("gid",),
        key_width=1,
        checkpoint=checkpoint,
    )

    _copy_rows(
        connector,
        source_query=(
            "SELECT m.gallery_id, m.observation_id, u.upload_time "
            "FROM catalog_gallery_observation_metadata_locals m "
            "LEFT JOIN catalog_gallery_source_name_accesses a ON a.gallery_id = m.gallery_id "
            "LEFT JOIN catalog_source_gallery_name_gids g ON g.source_gallery_name = a.source_gallery_name "
            "LEFT JOIN catalog_gallery_upload_times u ON u.gid = g.gid"
        ),
        destination="catalog_gallery_observation_upload_times",
        columns=("gallery_id", "observation_id", "upload_time"),
        key_width=2,
        checkpoint=checkpoint,
    )
    _copy_rows(
        connector,
        source_query=(
            "SELECT o.catalog_occurrence_sha256, u.upload_time "
            "FROM catalog_publication_occurrence_identities o "
            "LEFT JOIN catalog_publication_identities p ON p.publication_key = o.publication_key "
            "LEFT JOIN catalog_gallery_upload_times u ON u.gid = p.gid"
        ),
        destination="catalog_publication_upload_times",
        columns=("catalog_occurrence_sha256", "upload_time"),
        key_width=1,
        checkpoint=checkpoint,
    )


def _rebuild_sqlite(
    connector: SQLConnector, plan: _Plan, name: str, checkpoint: Callable[[str], None]
) -> None:
    """One crash-atomic table replacement using connection-local TEMP scratch."""
    relation = plan.source.relations[name]
    target = plan.target.relations[name]
    if relation["columns"] != target["columns"]:
        raise SchemaEpochAdmissionError("Unsupported SQLite table column conversion")
    table = relation["table"]
    columns = tuple(column[0] for column in relation["columns"])
    keys = tuple(relation["primary_key"])
    # Put key columns first for the shared keyset copy implementation. The
    # actual destination INSERT still names every column explicitly.
    ordered = (*keys, *(column for column in columns if column not in keys))
    scratch = "h2hdb_offline_rebuild"
    declarations = ", ".join(
        f'"{column[0]}" {column[2]}' for column in relation["columns"]
    )
    connector.execute(
        f'CREATE TEMP TABLE "{scratch}" ({declarations}, PRIMARY KEY ({", ".join(keys)}))'
    )
    try:
        _copy_rows(
            connector,
            source_query=f"SELECT {', '.join(ordered)} FROM {table}",
            destination=f"temp.{scratch}",
            columns=ordered,
            key_width=len(keys),
            checkpoint=checkpoint,
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            with connector.transaction():
                connector.execute(f'DROP TABLE "{table}"')
                for statement in plan.target.slices[name].statements:
                    connector.execute(statement.sql)
                _copy_rows(
                    connector,
                    source_query=f"SELECT {', '.join(ordered)} FROM temp.{scratch}",
                    destination=table,
                    columns=ordered,
                    key_width=len(keys),
                    checkpoint=checkpoint,
                    manage_transactions=False,
                )
                _validate_slice(connector, plan, plan.target, name)
                checkpoint("sqlite_table_before_commit")
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        checkpoint("sqlite_table_committed")
    finally:
        connector.execute(f'DROP TABLE temp."{scratch}"')


def _mariadb_steps(plan: _Plan, name: str) -> list[tuple[str, dict[str, Any]]]:
    previous = dict(plan.source.relations[name])
    following = dict(plan.target.relations[name])
    table = previous["table"]
    steps: list[tuple[str, dict[str, Any]]] = []
    shape = dict(previous)
    for family, drop_kind, add_kind in (
        ("foreign_keys", "FOREIGN KEY", "FOREIGN KEY"),
        ("checks", "CONSTRAINT", "CHECK"),
    ):
        old = {item[0]: item for item in previous[family]}
        new = {item[0]: item for item in following[family]}
        for key, value in old.items():
            if new.get(key) != value:
                shape = {
                    **shape,
                    family: tuple(item for item in shape[family] if item[0] != key),
                }
                steps.append((f"ALTER TABLE {table} DROP {drop_kind} {key}", shape))
        for key, value in new.items():
            if old.get(key) != value:
                shape = {**shape, family: (*shape[family], value)}
                if add_kind == "CHECK":
                    clause = f"CHECK ({value[1]})"
                else:
                    clause = f"FOREIGN KEY ({', '.join(value[1])}) REFERENCES {value[2]} ({', '.join(value[3])})"
                steps.append(
                    (f"ALTER TABLE {table} ADD CONSTRAINT {key} {clause}", shape)
                )
    if not steps:
        raise SchemaEpochAdmissionError("Unsupported MariaDB table conversion")
    return steps


def _mariadb_shape_index(connector: SQLConnector, plan: _Plan, name: str) -> int:
    shapes = [
        dict(plan.source.relations[name]),
        *(shape for _, shape in _mariadb_steps(plan, name)),
    ]
    start = None
    for index, shape in reversed(list(enumerate(shapes))):
        try:
            plan.provider._validate_relation_shape(connector, shape)
        except SchemaEpochValidationError:
            continue
        start = index
        break
    if start is None:
        raise SchemaEpochValidationError(
            f"Unsupported intermediate relation shape: {name}"
        )
    return start


def _alter_mariadb(
    connector: SQLConnector, plan: _Plan, name: str, checkpoint: Callable[[str], None]
) -> None:
    """Admit each before/after shape; no implicit DDL commit is a data txn."""
    start = _mariadb_shape_index(connector, plan, name)
    for sql, shape in _mariadb_steps(plan, name)[start:]:
        connector.execute(sql)
        checkpoint("mariadb_ddl_response")
        plan.provider._validate_relation_shape(connector, shape)
        checkpoint("mariadb_ddl_validated")
    _validate_slice(connector, plan, plan.target, name)


def _retire_old_cleanup_registry(
    connector: SQLConnector, plan: _Plan, checkpoint: Callable[[str], None]
) -> None:
    """Retire only completed operational GUT jobs; never rewrite their chains."""
    seeds = plan.source.payload["bootstrap_seeds"]
    old_sweeps = [
        row for row in seeds if ".cleanup-sweep.gallery-upload-time." in row["seed_id"]
    ]
    for seed in old_sweeps:
        with connector.transaction():
            target_key = seed["parameters"][2]
            row = connector.fetch_one(
                "SELECT cleanup_id, state, completed_at, final_chain_sha256, final_deleted_count "
                "FROM operational_cleanup_jobs WHERE target_key = %s",
                (target_key,),
            )
            if row:
                raise SchemaEpochAdmissionError(
                    "Old GID cleanup job survived the validated BUILDING admission"
                )
            actual = connector.fetch_one(
                seed["validation_sql"], seed["validation_parameters"]
            )
            if actual and actual != seed["expected_row"]:
                raise SchemaEpochValidationError("Old cleanup sweep seed differs")
            connector.execute(
                "DELETE FROM operational_cleanup_sweep_targets WHERE target_key = %s",
                (target_key,),
            )
        checkpoint("old_gid_cleanup_target_retired")
    with connector.transaction():
        if connector.fetch_one(
            "SELECT 1 FROM operational_cleanup_checkpoints WHERE phase = 'GUT_ROOT' LIMIT 1"
        ):
            raise SchemaEpochAdmissionError("Old GID cleanup checkpoint remains")
        connector.execute(
            "DELETE FROM operational_cleanup_phases WHERE phase = 'GUT_ROOT'"
        )
        connector.execute(
            "DELETE FROM operational_cleanup_target_kinds WHERE target_kind = 'GALLERY_UPLOAD_TIME'"
        )
        row = connector.fetch_one(
            "SELECT target_kind, phase_order FROM operational_cleanup_phases WHERE phase = 'CP_ROOT'"
        )
        if row not in (("CATALOG_PUBLICATION", 8), ("CATALOG_PUBLICATION", 9)):
            raise SchemaEpochValidationError("Catalog cleanup root phase differs")
        connector.execute(
            "UPDATE operational_cleanup_phases SET phase_order = 9 WHERE phase = 'CP_ROOT' AND phase_order = 8"
        )
    checkpoint("cleanup_registry_rows_converted")


def _changed(plan: _Plan) -> tuple[str, ...]:
    return tuple(
        name
        for name in plan.target.slices
        if name in plan.source.slices
        and plan.source.slices[name] != plan.target.slices[name]
    )


def _catalog(plan: _Plan) -> SchemaEpochCatalog:
    return (
        SQLiteSchemaEpochCatalog()
        if plan.backend == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )


def _validate_inventory(
    connector: SQLConnector, plan: _Plan, *, original: bool
) -> None:
    catalog = _catalog(plan)
    actual = catalog.list_objects(connector)
    control = {catalog.control_object}
    if original:
        if actual != plan.source.objects | control:
            raise SchemaEpochValidationError("Original schema inventory differs")
    elif not actual <= plan.source.objects | plan.target.objects | control:
        raise SchemaEpochValidationError("Conversion contains unknown schema objects")
    changes = set(_changed(plan))
    missing_view = any(
        name in changes
        and plan.source.relations[name]["kind"] == "view"
        and part.statements[0].creates not in actual
        for name, part in plan.source.slices.items()
    )
    for name, part in plan.source.slices.items():
        exists = part.statements[0].creates in actual
        if not exists:
            if not original and (
                name in _REMOVALS
                or (name in changes and plan.source.relations[name]["kind"] == "view")
            ):
                continue
            raise SchemaEpochValidationError(
                f"Required retained relation is missing: {name}"
            )
        if original or name not in changes:
            _validate_slice(
                connector,
                plan,
                plan.source,
                name,
                inspect_view_columns=not missing_view,
            )
        else:
            try:
                _validate_slice(
                    connector,
                    plan,
                    plan.target,
                    name,
                    inspect_view_columns=not missing_view,
                )
            except SchemaEpochValidationError:
                if (
                    plan.backend == "mariadb"
                    and plan.source.relations[name]["kind"] == "table"
                ):
                    _mariadb_shape_index(connector, plan, name)
                else:
                    _validate_slice(
                        connector,
                        plan,
                        plan.source,
                        name,
                        inspect_view_columns=not missing_view,
                    )
    for name in _ADDITIONS:
        if plan.target.slices[name].statements[0].creates in actual:
            _validate_slice(connector, plan, plan.target, name)


def _convert_shapes(
    connector: SQLConnector, plan: _Plan, checkpoint: Callable[[str], None]
) -> None:
    _retire_old_cleanup_registry(connector, plan, checkpoint)
    for name in _changed(plan):
        relation = plan.target.relations[name]
        try:
            _validate_slice(connector, plan, plan.target, name)
        except SchemaEpochValidationError:
            if relation["kind"] == "view":
                part = plan.target.slices[name]
                view = part.statements[0].creates.name
                with (
                    connector.transaction()
                    if plan.backend == "sqlite"
                    else nullcontext()
                ):
                    connector.execute(f"DROP VIEW IF EXISTS {view}")
                    checkpoint("view_dropped")
                    connector.execute(part.statements[0].sql)
                    checkpoint("view_created")
            elif plan.backend == "sqlite":
                _validate_slice(connector, plan, plan.source, name)
                _rebuild_sqlite(connector, plan, name, checkpoint)
            else:
                _alter_mariadb(connector, plan, name, checkpoint)
    if plan.backend == "mariadb":
        connector.commit()
    for seed in plan.target.payload["bootstrap_seeds"]:
        if (
            ".gallery-gid-identity." in seed["seed_id"]
            or ".ggi-root." in seed["seed_id"]
            or ".cp-upload-time." in seed["seed_id"]
        ):
            with connector.transaction():
                connector.execute(seed["sql"], seed["parameters"])
                if (
                    connector.fetch_one(
                        seed["validation_sql"], seed["validation_parameters"]
                    )
                    != seed["expected_row"]
                ):
                    raise SchemaEpochValidationError("Converted cleanup seed differs")
    connector.execute("DROP TABLE IF EXISTS catalog_gallery_upload_times")
    checkpoint("retired_global_timestamp_relation_dropped")
    if plan.backend == "mariadb":
        connector.commit()
    with connector.read_transaction():
        plan.provider.validate_global(connector)
        # READY retains data; activation's bootstrap-absent relations are not
        # required to be empty during an offline conversion of a populated DB.
        for seed in plan.target.payload["bootstrap_seeds"]:
            if seed["target_relation"] not in {
                "cleanup_target_kind",
                "cleanup_sweep_target",
                "cleanup_phase",
            }:
                continue
            if (
                connector.fetch_one(
                    seed["validation_sql"], seed["validation_parameters"]
                )
                != seed["expected_row"]
            ):
                raise SchemaEpochValidationError(
                    "Converted static bootstrap facts differ"
                )
        if _catalog(plan).list_objects(
            connector
        ) != plan.definition.expected_objects | {_catalog(plan).control_object}:
            raise SchemaEpochValidationError("Final converted inventory differs")
        if plan.backend == "sqlite" and connector.fetch_one("PRAGMA foreign_key_check"):
            raise SchemaEpochValidationError("Converted foreign keys are inconsistent")


def _affected_cleanup_exists(connector: SQLConnector) -> bool:
    return bool(
        connector.fetch_one(
            "SELECT 1 FROM operational_cleanup_jobs j "
            "JOIN operational_cleanup_sweep_targets s ON s.target_key = j.target_key "
            f"WHERE j.state = 'OPEN' AND s.target_kind IN ({', '.join('%s' for _ in _AFFECTED_CLEANUP_KINDS)}) LIMIT 1",
            _AFFECTED_CLEANUP_KINDS,
        )
    )


def _completed_gid_jobs(connector: SQLConnector) -> list[tuple[Any, ...]]:
    rows = connector.fetch_all(
        "SELECT s.shard_no,s.target_key,j.cleanup_id,j.cycle_generation,"
        "j.cycle_cutoff_at,j.algorithm_version,j.max_rows_per_transaction,"
        "j.hash_cache_max_age_microseconds,j.frozen_root_count,"
        "j.frozen_root_set_sha256,j.state,j.created_at,j.completed_at,"
        "j.final_chain_sha256,j.final_deleted_count "
        "FROM operational_cleanup_sweep_targets s JOIN operational_cleanup_jobs j "
        "ON j.target_key=s.target_key WHERE s.target_kind='GALLERY_UPLOAD_TIME' "
        "ORDER BY s.shard_no LIMIT 257"
    )
    if len(rows) > 256:
        raise SchemaEpochAdmissionError("Old GID cleanup jobs exceed their fixed slots")
    return rows


def _retire_validated_gid_jobs(
    connector: SQLConnector,
    proof: Mapping[str, Any] | None,
) -> None:
    """The old worker proves old semantics; this txn binds and retires its rows."""
    rows = _completed_gid_jobs(connector)
    if not rows and proof is None:
        return
    digest = hashlib.sha256(
        b"h2hdb-schema8-completed-gid-jobs-v1\0"
        + json.dumps(
            rows, separators=(",", ":"), default=lambda value: value.hex()
        ).encode("ascii")
    ).hexdigest()
    if (
        proof is None
        or type(proof.get("completed_gid_job_count")) is not int
        or proof["completed_gid_job_count"] != len(rows)
        or proof.get("completed_gid_jobs_sha256") != digest
    ):
        raise SchemaEpochAdmissionError(
            "Completed GID jobs differ from the isolated validation"
        )
    for row in rows:
        if row[10] != "COMPLETE":
            raise SchemaEpochAdmissionError("A GID cleanup cycle remains unfinished")
        for table in (
            "operational_cleanup_cycle_roots",
            "operational_cleanup_checkpoints",
        ):
            if connector.fetch_one(
                f"SELECT 1 FROM {table} WHERE cleanup_id=%s LIMIT 1", (row[2],)
            ):
                raise SchemaEpochValidationError(
                    "Completed GID cleanup retains children"
                )
        affected = connector.execute_affected(
            "DELETE FROM operational_cleanup_jobs WHERE cleanup_id=%s "
            "AND target_key=%s AND cycle_generation=%s AND state='COMPLETE'",
            (row[2], row[1], row[3]),
        )
        if affected != 1:
            raise SchemaEpochAdmissionError("Validated GID cleanup job changed")


@contextmanager
def _sqlite_scratch(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend != "sqlite":
        yield
        return
    previous = connector.fetch_one("PRAGMA temp_store_directory")
    with tempfile.TemporaryDirectory(prefix="h2hdb-schema9-tables-") as temporary:
        quoted = temporary.replace("'", "''")
        connector.execute("PRAGMA temp_store = FILE")
        connector.execute(f"PRAGMA temp_store_directory = '{quoted}'")
        if connector.fetch_one("PRAGMA temp_store_directory") != (temporary,):
            raise SchemaEpochAdmissionError(
                "SQLite cannot use the owned TEMP directory"
            )
        try:
            yield
        finally:
            original = str(previous[0]).replace("'", "''") if previous else ""
            connector.execute(f"PRAGMA temp_store_directory = '{original}'")


def _convert(
    connector: SQLConnector,
    plan: _Plan,
    *,
    checkpoint: Callable[[str], None],
    cleanup_proof: Mapping[str, Any] | None = None,
) -> str:
    catalog = _catalog(plan)
    with (
        _exclusive_conversion(connector, plan.backend),
        _sqlite_scratch(connector, plan.backend),
    ):
        with connector.read_transaction():
            catalog.validate_control_table(connector)
            row = _control(connector)
            _, _, version, state, manifest, started_at, ready_at = row
            audit = read_database_audit_state(connector)
            is_ready = (
                state == "READY"
                and type(ready_at) is int
                and ready_at >= cast(int, started_at)
            )
            if (
                version == 9
                and is_ready
                and manifest == bytes.fromhex(plan.definition.manifest_sha256)
            ):
                return "already_converted"
            old_ready = (
                version == 8
                and is_ready
                and manifest == bytes.fromhex(_SOURCE_MANIFESTS[plan.backend])
            )
            if old_ready:
                _validate_inventory(connector, plan, original=True)
                for seed in plan.source.payload["bootstrap_seeds"]:
                    if seed["target_relation"] not in {
                        "cleanup_target_kind",
                        "cleanup_sweep_target",
                        "cleanup_phase",
                    }:
                        continue
                    if (
                        connector.fetch_one(
                            seed["validation_sql"], seed["validation_parameters"]
                        )
                        != seed["expected_row"]
                    ):
                        raise SchemaEpochValidationError(
                            "Original cleanup bootstrap facts differ"
                        )
                if _affected_cleanup_exists(connector):
                    raise SchemaEpochAdmissionError(
                        "Finish the existing affected schema-8 cleanup cycle before conversion"
                    )
                phase = _Phase.COPY
            else:
                admitted: dict[bytes, _Phase] = {
                    plan.marker(part): part for part in _Phase if part != _Phase.AUDITED
                }
                if (
                    audit is not None
                    and audit.validator_version == plan.validator_version
                    and audit.audit_pending == 0
                    and audit.lease_expires_at is None
                ):
                    admitted[plan.marker(_Phase.AUDITED, audit)] = _Phase.AUDITED
                if (
                    version != 9
                    or state != "BUILDING"
                    or ready_at is not None
                    or manifest not in admitted
                ):
                    raise SchemaEpochAdmissionError(
                        "Database is not this exact manifest-bound conversion"
                    )
                phase = admitted[manifest]
                _validate_inventory(connector, plan, original=False)
        if old_ready:
            with connector.transaction():
                _retire_validated_gid_jobs(connector, cleanup_proof)
                checkpoint("source_jobs_retired_before_marker")
                _retire_audit_owner(connector, plan)
                affected = connector.execute_affected(
                    "UPDATE h2hdb_schema_epoch SET schema_version = 9, state = 'BUILDING', manifest_sha256 = %s, ready_at = NULL "
                    "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 8 AND state = 'READY' AND manifest_sha256 = %s",
                    (plan.marker(_Phase.COPY), manifest),
                )
                if affected != 1:
                    raise SchemaEpochAdmissionError(
                        "Original schema control changed before conversion"
                    )
                checkpoint("source_jobs_marker_written")
            checkpoint("conversion_marked")
        if phase == _Phase.COPY:
            for name in _ADDITIONS:
                for statement in plan.target.slices[name].statements:
                    connector.execute(statement.sql)
                    checkpoint("new_relation_ddl_response")
                _validate_slice(connector, plan, plan.target, name)
            if plan.backend == "mariadb":
                connector.commit()
            _copy_upload_times(connector, checkpoint=checkpoint)
            with connector.transaction():
                _change_marker(
                    connector, plan.marker(_Phase.COPY), plan.marker(_Phase.COPIED)
                )
            phase = _Phase.COPIED
            checkpoint("upload_time_copies_committed")
        if phase == _Phase.COPIED:
            _convert_shapes(connector, plan, checkpoint)
            with connector.transaction():
                _change_marker(
                    connector, plan.marker(_Phase.COPIED), plan.marker(_Phase.STRUCTURE)
                )
            phase = _Phase.STRUCTURE
            checkpoint("converted_structure_committed")
        if phase == _Phase.STRUCTURE:
            with connector.read_transaction():
                audit = read_database_audit_state(connector)
            checkpoint("full_audit_started")
            started = perf_counter_ns()
            diagnostics = DatabasePerformance(
                logging.getLogger("h2hdb.database_performance"),
                backend=plan.backend,
                level=logging.INFO,
            )
            with diagnostics.operation("schema_check", reason="offline_schema8_to9"):
                measured = instrument_connector(connector)
                audit = _audit_and_record(measured, plan, catalog, audit, started)
            checkpoint("full_audit_durably_recorded")
        assert audit is not None
        with connector.transaction():
            if read_database_audit_state(connector) != audit:
                raise SchemaEpochAdmissionError(
                    "Completed audit baseline changed before READY"
                )
            now = database_unix_microseconds(
                VNextUnitOfWork(connector, backend=plan.backend)
            )
            affected = connector.execute_affected(
                "UPDATE h2hdb_schema_epoch SET state = 'READY', manifest_sha256 = %s, ready_at = %s "
                "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 9 AND state = 'BUILDING' "
                "AND ready_at IS NULL AND manifest_sha256 = %s",
                (
                    bytes.fromhex(plan.definition.manifest_sha256),
                    max(now, cast(int, started_at)),
                    plan.marker(_Phase.AUDITED, audit),
                ),
            )
            if affected != 1:
                raise SchemaEpochAdmissionError(
                    "Completed audit phase changed before READY"
                )
            checkpoint("ready_before_commit")
        checkpoint("ready_committed")
    return "converted"


def _audit_and_record(
    connector: SQLConnector,
    plan: _Plan,
    catalog: SchemaEpochCatalog,
    audit: _State | None,
    started: int,
) -> _State:
    with connector.transaction():
        # Existing semantic validators require the real generated
        # manifest in their pinned snapshot. This uncommitted value is
        # never visible as a resumable runtime BUILDING incarnation.
        target_marker = bytes.fromhex(plan.definition.manifest_sha256)
        _change_marker(connector, plan.marker(_Phase.STRUCTURE), target_marker)
        SchemaEpochRunner(gate=None, catalog=catalog)._validate_ready_schema(
            connector,
            plan.provider,
            plan.definition,
            validate_genesis=False,
            semantic_phase=SchemaSemanticValidationPhase.READY,
        )
        elapsed = max(0, (perf_counter_ns() - started) // 1_000)
        # No callback/READY action intervenes between successful audit
        # and its baseline-bound AUDITED commit. Failures roll back to
        # STRUCTURE, never persist the temporary generated marker.
        _change_marker(connector, target_marker, plan.marker(_Phase.STRUCTURE))
        audit = _record_audit(connector, plan, audit, elapsed)
    return audit


def upgrade(
    config: CoreConfig,
    *,
    source_schema: Path,
    source_wheel: Path | None = None,
    source_wheel_sha256: str | None = None,
    config_path: Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> str:
    if config.database.access_mode is not DatabaseAccessMode.read_write:
        raise ValueError("Offline conversion needs a read-write database configuration")
    config = config.model_copy(
        update={"logger": config.logger.model_copy(update={"file": None})}
    )
    backend = config.database.sql_type
    if backend not in ("sqlite", "mariadb"):
        raise ValueError("Unsupported conversion backend")
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if backend == "sqlite" else "mariadb"
    )
    digest = ""
    if source_wheel is not None:
        if source_wheel_sha256 is None:
            raise ValueError("An explicit source wheel checksum is required")
        digest = _verify_source_wheel(source_wheel, source_wheel_sha256)
    plan = _plan(provider, source_schema, source_wheel_sha256=digest)
    checkpoint = progress or _Progress()
    checkpoint("preflight_started")
    needs_cleanup = _preflight_cleanup(config, plan)
    checkpoint("preflight_completed")
    report = None
    if needs_cleanup:
        if source_wheel is None or config_path is None:
            raise SchemaEpochAdmissionError(
                "Retained cleanup requires the isolated schema-8 wheel and configuration path"
            )
        report = _run_source_cleanup(config_path, source_wheel, digest)
        checkpoint(
            "source_cleanup_completed " + json.dumps(report, separators=(",", ":"))
        )
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            return _convert(
                connector, plan, checkpoint=checkpoint, cleanup_proof=report
            )
    finally:
        context.close()


def _preflight_cleanup(config: CoreConfig, plan: _Plan) -> bool:
    context = RepositoryContext.from_config(config)
    try:
        with (
            context.SQLConnector() as connector,
            _exclusive_conversion(connector, plan.backend),
        ):
            with connector.read_transaction():
                _catalog(plan).validate_control_table(connector)
                row = _control(connector)
                if (
                    row[2] != 8
                    or row[3] != "READY"
                    or row[4] != bytes.fromhex(_SOURCE_MANIFESTS[plan.backend])
                ):
                    return False  # The ordinary converter performs exact admission.
                _validate_inventory(connector, plan, original=True)
                read_database_audit_state(connector)
                return _affected_cleanup_exists(connector) or bool(
                    _completed_gid_jobs(connector)
                )
    finally:
        context.close()


def _verify_source_wheel(path: Path, expected: str) -> str:
    if len(expected) != 64 or any(
        value not in "0123456789abcdef" for value in expected
    ):
        raise ValueError("Source wheel checksum is invalid")
    if not path.is_file() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("Source wheel exceeds its file bound")
    with path.open("rb") as stream:
        raw = stream.read(32 * 1024 * 1024 + 1)
    if len(raw) > 32 * 1024 * 1024:
        raise ValueError("Source wheel exceeds its file bound")
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Source wheel checksum differs")
    with zipfile.ZipFile(BytesIO(raw)) as archive:
        if len(archive.namelist()) != len(set(archive.namelist())):
            raise ValueError("Source wheel contains duplicate members")
        metadata_path = "h2hdb-0.42.2.dist-info/METADATA"
        if archive.getinfo(metadata_path).file_size > 128 * 1024:
            raise ValueError("Source wheel metadata exceeds its bound")
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
        if metadata["Name"] != "h2hdb" or metadata["Version"] != "0.42.2":
            raise ValueError("The isolated worker requires Core 0.42.2")
        resource = "h2hdb/_generated_vnext_schema.bin"
        if (
            archive.getinfo(resource).file_size != SOURCE_SCHEMA_SIZE
            or hashlib.sha256(archive.read(resource)).hexdigest()
            != SOURCE_SCHEMA_SHA256
        ):
            raise ValueError("Source wheel schema artifact differs")
    return expected


def _run_source_cleanup(config_path: Path, wheel: Path, digest: str) -> dict[str, Any]:
    if os.name != "posix":
        raise SchemaEpochAdmissionError(
            "Historical cleanup requires POSIX owner pipes; use the Docker bundle"
        )
    helper = Path(__file__).resolve().with_name("finish-schema8-cleanup.py")
    bootstrap = (
        "import hashlib,pathlib,runpy,sys\n"
        "wheel,expected,helper,config,owner=sys.argv[1:]\n"
        "path=pathlib.Path(wheel)\n"
        "if not path.is_file() or path.stat().st_size>33554432: raise SystemExit('source wheel size changed')\n"
        "if hashlib.sha256(path.read_bytes()).hexdigest()!=expected: raise SystemExit('source wheel checksum changed')\n"
        "sys.path.insert(0,wheel)\n"
        "sys.argv=[helper,'--config',config,'--consumers-stopped','--owner-fd',owner]\n"
        "runpy.run_path(helper,run_name='__main__')\n"
    )
    reader, writer = os.pipe()
    process: subprocess.Popen[bytes] | None = None
    try:
        with tempfile.TemporaryFile() as output:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    bootstrap,
                    str(wheel.resolve()),
                    digest,
                    str(helper),
                    str(config_path.resolve()),
                    str(reader),
                ],
                stdin=subprocess.DEVNULL,
                stdout=output,
                pass_fds=(reader,),
            )
            process.wait()
            output.seek(0)
            raw = output.read(65_537)
            if len(raw) > 65_536:
                raise SchemaEpochAdmissionError(
                    "Schema-8 cleanup report exceeds its bound"
                )
            try:
                report = json.loads(raw)
            except (ValueError, UnicodeError) as error:
                raise SchemaEpochAdmissionError(
                    "Schema-8 cleanup report is invalid"
                ) from error
            if (
                not isinstance(report, dict)
                or process.returncode != 0
                or report.get("status") not in {"complete", "no_affected_open_cycle"}
            ):
                blocker = (
                    report.get("blocker", report.get("code", "worker_failed"))
                    if isinstance(report, dict)
                    else "invalid_report"
                )
                raise SchemaEpochAdmissionError(
                    f"Schema-8 cleanup did not complete: {blocker}"
                )
            return cast(dict[str, Any], report)
    finally:
        os.close(writer)
        os.close(reader)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class _Progress:
    """INFO milestones plus throttled row counts, without per-row output."""

    def __init__(self) -> None:
        self._started = perf_counter_ns()
        self._last_page = self._started

    def __call__(self, event: str) -> None:
        now = perf_counter_ns()
        kind = event.split(" ", 1)[0]
        if kind in {
            "copy_page_before_commit",
            "copy_page_committed",
            "copy_page_staged",
            "old_gid_cleanup_target_retired",
            "sqlite_table_before_commit",
            "ready_before_commit",
            "source_jobs_retired_before_marker",
            "source_jobs_marker_written",
        }:
            return
        if kind == "copy_progress":
            if now - self._last_page < 1_000_000_000:
                return
            self._last_page = now
        print(
            f"INFO {event} elapsed_seconds={(now - self._started) / 1_000_000_000:.3f}",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--source-schema",
        type=Path,
        default=Path(__file__).resolve().with_name("schema8-provider.bin"),
    )
    parser.add_argument("--consumers-stopped", action="store_true", required=True)
    parser.add_argument(
        "--source-wheel",
        type=Path,
        default=Path(__file__).resolve().with_name("h2hdb-0.42.2-source.whl"),
    )
    parser.add_argument(
        "--source-wheel-checksum",
        type=Path,
        default=Path(__file__).resolve().with_name("schema8-wheel.sha256"),
    )
    args = parser.parse_args()
    with args.source_wheel_checksum.open("r", encoding="ascii") as stream:
        digest = stream.read(66).strip()
    # The offline CLI owns console output; it does not create configured log
    # files in a read-only deployment mount or alter the consumer log level.
    logger = HentaiDBLogger(level=logging.INFO)
    try:
        with _route_database_diagnostics(logger, level=logging.INFO):
            result = upgrade(
                load_config(args.config),
                source_schema=args.source_schema,
                source_wheel=args.source_wheel,
                source_wheel_sha256=digest,
                config_path=Path(args.config),
            )
    finally:
        logger.removeHandlers()
    print(
        f"Observation upload time conversion: {result}; retained database and external resources preserved"
    )


if __name__ == "__main__":
    main()
