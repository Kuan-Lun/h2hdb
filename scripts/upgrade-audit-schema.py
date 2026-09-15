#!/usr/bin/env python3
"""Convert the exact schema-6 database to schema 7 with all consumers stopped.

This one-use offline tool only adds the audit scheduler relation. It never
rewrites catalog/source/queue facts or touches external archives. Runtime schema
provisioning intentionally does not import or call this converter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from time import perf_counter_ns, time_ns
from typing import cast

from h2hdb import CoreConfig, DatabaseAccessMode, load_config
from h2hdb.database_audit import record_offline_audit_completion
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    MariaDBAdvisorySchemaEpochGate,
    MariaDBSchemaEpochCatalog,
    SchemaEpochAdmissionError,
    SchemaEpochCatalog,
    SchemaEpochRunner,
    SchemaEpochValidationError,
    SchemaObject,
    SchemaSemanticValidationPhase,
    SchemaSlice,
    SQLiteSchemaEpochCatalog,
    mariadb_schema_epoch_gate_name,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
from h2hdb.vnext_transaction import VNextUnitOfWork

_ADDITION = "relation:database_audit_state"
_OLD_MANIFESTS = {
    "sqlite": "ababf15d3ed95142a571292aef3f40da9225431b1057b6ec7e984537529cfde8",
    "mariadb": "ce71099443e3c51e83090e1c2a20ae02a2b80d452412633032cddeb28acf8f09",
}
_OLD_SLICES = {
    "sqlite": "132fbbb3f0aa99dc17b602bd2205060a1631364de4eeac279585c148c70be764",
    "mariadb": "a5d08c9f62c87cb41a6b08e079d451e8562623f973c4838ba08ce6602ca833ea",
}


def _slices_digest(slices: Sequence[SchemaSlice]) -> str:
    records = [
        (
            part.slice_id,
            [
                (
                    statement.statement_id,
                    statement.creates.kind.value,
                    statement.creates.name,
                    statement.sql,
                )
                for statement in part.statements
            ],
        )
        for part in slices
    ]
    return hashlib.sha256(
        json.dumps(records, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _conversion_slices(
    provider: GeneratedVNextSchemaProvider,
) -> tuple[tuple[SchemaSlice, ...], SchemaSlice]:
    definition = provider.definition
    if definition.epoch != 3 or definition.schema_version != 7:
        raise SchemaEpochAdmissionError("This converter requires schema-7 software")
    retained = tuple(s for s in definition.slices if s.slice_id != _ADDITION)
    additions = tuple(s for s in definition.slices if s.slice_id == _ADDITION)
    if len(additions) != 1 or _slices_digest(retained) != _OLD_SLICES[provider.backend]:
        raise SchemaEpochAdmissionError(
            "The installed provider is not the exact additive schema-6 to 7 change"
        )
    return retained, additions[0]


@contextmanager
def _exclusive_conversion(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "sqlite":
        # Offline conversion may hold a write transaction for the complete audit.
        # It is not a runtime ingest/publication transaction.
        with connector.transaction():
            yield
        return
    database = connector.fetch_one("SELECT DATABASE()")
    if len(database) != 1 or not isinstance(database[0], str):
        raise SchemaEpochAdmissionError("Cannot identify the conversion database")
    with MariaDBAdvisorySchemaEpochGate().acquire_named(
        connector, mariadb_schema_epoch_gate_name(database[0])
    ):
        yield


@contextmanager
def _control_transaction(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "sqlite":
        yield
    else:
        with connector.transaction():
            yield


@contextmanager
def _inspection_transaction(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "sqlite":
        yield
    else:
        with connector.read_transaction():
            yield


def _control(connector: SQLConnector) -> tuple[object, ...]:
    rows = connector.fetch_all(
        "SELECT singleton_id, epoch, schema_version, state, manifest_sha256, "
        "started_at, ready_at FROM h2hdb_schema_epoch LIMIT 2"
    )
    if len(rows) != 1 or len(rows[0]) != 7:
        raise SchemaEpochAdmissionError("Conversion requires one exact epoch control")
    row = rows[0]
    if (
        type(row[0]) is not int
        or row[0] != 1
        or type(row[1]) is not int
        or row[1] != 3
        or type(row[2]) is not int
        or not isinstance(row[4], (bytes, bytearray))
        or type(row[5]) is not int
        or row[5] < 0
    ):
        raise SchemaEpochAdmissionError("Invalid conversion control values")
    return tuple(row)


def _objects(slices: Sequence[SchemaSlice]) -> frozenset[SchemaObject]:
    return frozenset(statement.creates for s in slices for statement in s.statements)


def _convert(
    connector: SQLConnector,
    provider: GeneratedVNextSchemaProvider,
    *,
    checkpoint: Callable[[str], None],
) -> str:
    retained, addition = _conversion_slices(provider)
    backend = provider.backend
    catalog: SchemaEpochCatalog = (
        SQLiteSchemaEpochCatalog()
        if backend == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )
    definition = provider.definition
    expected = definition.expected_objects | {catalog.control_object}
    old_objects = _objects(retained) | {catalog.control_object}
    new_objects = _objects((addition,))
    target_manifest = bytes.fromhex(definition.manifest_sha256)
    conversion_manifest = hashlib.sha256(
        b"h2hdb-offline-audit-schema-6-to-7\0"
        + bytes.fromhex(_OLD_MANIFESTS[backend])
        + target_manifest
    ).digest()

    with _exclusive_conversion(connector, backend):
        with _inspection_transaction(connector, backend):
            catalog.validate_control_table(connector)
            row = _control(connector)
            _, _, version, state, manifest, started_at, ready_at = row
            actual = catalog.list_objects(connector)
            old_ready = (
                version == 6
                and state == "READY"
                and manifest == bytes.fromhex(_OLD_MANIFESTS[backend])
                and type(ready_at) is int
                and type(started_at) is int
                and ready_at >= started_at
            )
            converting = (
                version == 7
                and state == "BUILDING"
                and manifest == conversion_manifest
                and ready_at is None
            )
            target_ready = (
                version == 7
                and state == "READY"
                and manifest == target_manifest
                and type(ready_at) is int
                and type(started_at) is int
                and ready_at >= started_at
            )
            if not (old_ready or converting or target_ready):
                raise SchemaEpochAdmissionError(
                    "Only the exact READY schema 6 or this schema-7 conversion is supported"
                )
            if old_ready and actual != old_objects:
                raise SchemaEpochValidationError("Schema-6 object inventory differs")
            if not old_objects <= actual or not actual <= expected:
                raise SchemaEpochValidationError("Conversion object inventory differs")
            # Check every retained physical object before changing any control/data.
            for part in retained:
                provider.validate_slice(connector, part)
            checkpoint("retained_schema_validated")
        if target_ready:
            with _inspection_transaction(connector, backend):
                checkpoint("full_audit_started")
                SchemaEpochRunner(gate=None, catalog=catalog).validate_ready(
                    connector, provider
                )
            return "already_converted"
        if old_ready:
            with _control_transaction(connector, backend):
                changed = connector.execute_affected(
                    "UPDATE h2hdb_schema_epoch SET schema_version = 7, "
                    "state = 'BUILDING', manifest_sha256 = %s, ready_at = NULL "
                    "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 6 "
                    "AND state = 'READY' AND manifest_sha256 = %s",
                    (conversion_manifest, bytes.fromhex(_OLD_MANIFESTS[backend])),
                )
                if changed != 1:
                    raise SchemaEpochValidationError(
                        "Schema control changed during conversion"
                    )
            checkpoint("conversion_marked")
        for statement in addition.statements:
            if statement.creates not in actual:
                connector.execute(statement.sql)
                checkpoint("object_created")
        with _inspection_transaction(connector, backend):
            provider.validate_slice(connector, addition)
            if catalog.list_objects(connector) != old_objects | new_objects:
                raise SchemaEpochValidationError(
                    "Converted schema object inventory differs"
                )
            checkpoint("addition_validated")
        with _control_transaction(connector, backend):
            # The conversion-only checksum makes normal migrate/check/ready
            # reject an interrupted conversion, including all implicit DDL
            # commits on MariaDB. Only this final transaction temporarily
            # admits the current manifest for full semantic validation.
            changed = connector.execute_affected(
                "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s "
                "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 7 "
                "AND state = 'BUILDING' AND manifest_sha256 = %s AND ready_at IS NULL",
                (target_manifest, conversion_manifest),
            )
            if changed != 1:
                raise SchemaEpochValidationError("Conversion lost its pending control")
            # Populated databases require READY obligations, not genesis checks.
            checkpoint("full_audit_started")
            audit_started = perf_counter_ns()
            SchemaEpochRunner(gate=None, catalog=catalog)._validate_ready_schema(
                connector,
                provider,
                definition,
                validate_genesis=False,
                semantic_phase=SchemaSemanticValidationPhase.READY,
            )
            checkpoint("full_audit_completed")
            record_offline_audit_completion(
                VNextUnitOfWork(connector, backend=backend),
                duration_microseconds=max(
                    0, (perf_counter_ns() - audit_started) // 1_000
                ),
            )
            changed = connector.execute_affected(
                "UPDATE h2hdb_schema_epoch SET state = 'READY', ready_at = %s "
                "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 7 "
                "AND state = 'BUILDING' AND manifest_sha256 = %s AND ready_at IS NULL",
                (max(time_ns() // 1_000, cast(int, started_at)), target_manifest),
            )
            if changed != 1:
                raise SchemaEpochValidationError("Conversion lost its epoch control")
    checkpoint("ready_committed")
    return "converted"


def _show_progress(checkpoint: str) -> None:
    messages = {
        "retained_schema_validated": "The retained database structure matches schema 6.",
        "conversion_marked": "Conversion recorded; normal consumers will reject startup until it completes.",
        "object_created": "Added the generated audit scheduling table.",
        "addition_validated": "The new scheduling table passed structural validation.",
        "full_audit_started": "Running the complete database audit; this may take as long as a manual check.",
        "full_audit_completed": "The complete database audit passed; recording its result.",
        "ready_committed": "Schema 7 and its completed audit baseline are durably committed.",
    }
    print(messages[checkpoint], flush=True)


def upgrade(
    config: CoreConfig, *, progress: Callable[[str], None] = _show_progress
) -> str:
    """Convert one offline database; failures leave a resumable conversion."""

    if config.database.access_mode is not DatabaseAccessMode.read_write:
        raise ValueError(
            "Offline conversion requires a read-write database configuration"
        )
    backend = config.database.sql_type
    if backend not in ("sqlite", "mariadb"):
        raise ValueError("Unsupported conversion database backend")
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if backend == "sqlite" else "mariadb"
    )
    _conversion_slices(provider)  # Reject unsupported software before DB access.
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            return _convert(connector, provider, checkpoint=progress)
    finally:
        context.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Core database configuration")
    parser.add_argument(
        "--consumers-stopped",
        action="store_true",
        required=True,
        help="Confirm ingest, downloader, readers and other DB writers are stopped",
    )
    args = parser.parse_args()
    result = upgrade(load_config(args.config))
    print(
        f"Audit schema conversion: {result}; existing database facts and CBZs retained"
    )


if __name__ == "__main__":
    main()
