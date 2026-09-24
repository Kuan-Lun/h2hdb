#!/usr/bin/env python3
"""Convert the exact schema-7 database to schema 8 with all consumers stopped.

This offline tool adds durable source-collection relations and separates the
existing staging owner from its shared header. It preserves all existing facts,
staging children and external archives. Only this tool resumes its BUILDING state. Runtime schema
provisioning intentionally does not import or call this converter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from time import perf_counter_ns, time_ns
from typing import Any, cast

from h2hdb import CoreConfig, DatabaseAccessMode, load_config
from h2hdb.database_audit import record_offline_audit_completion
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    MariaDBAdvisorySchemaEpochGate,
    MariaDBSchemaEpochCatalog,
    SchemaCreateStatement,
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

_ADDITIONS = frozenset(
    {
        "source_collection",
        "source_collection_manifest_policy",
        "source_collection_qualification_policy",
        "source_collection_created_at",
        "source_collection_observation",
        "source_collection_consumption",
        "source_collection_state",
        "source_collection_claim",
        "source_working_collection",
        "gallery_staging_source_build",
        "gallery_staging_collection",
    }
)
_HEADER = "gallery_observation_staging"
_CLAIM = "gallery_observation_staging_claim"
_CLEANUP = ("cleanup_target_kind", "cleanup_phase")
_CHANGED = frozenset({_HEADER, _CLAIM, *_CLEANUP})
_COLLECTION_PHASES = "'SC_MEMBERS', 'SC_CLAIM', 'SC_METADATA', 'SC_STATE', 'SC_ROOT', "
_OLD_SEEDS = {
    "sqlite": "920ee64f5e8e73852dd9308f42cabbc998f713fac5341edfe78176e532996d6a",
    "mariadb": "7c5497d24debd8a59453e15483f70e54f33394de60371526c2e79a2f552947e2",
}
_HEADER_TABLE = "operational_gallery_observation_stagings"
_BINDING_TABLE = "operational_gallery_staging_source_builds"
_OLD_MANIFESTS = {
    "sqlite": "eec961165a0883d92b66c4a83c23723d1b6c98db1e8f843b3d0ae901cb31a65a",
    "mariadb": "a523898f26ccc9cfbb0aa8afc34e53e790048aea7dea2623bb9e45d028898484",
}
_OLD_SLICES = {
    "sqlite": "55af2ecfcf824829d9d660a6075d3ea600fefab4ef1cb0fd2312c336b49b5503",
    "mariadb": "a85fa1f7025ef255782970bdbe0f99d34b2d40e26b8c709054ea84b9f96e9a71",
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
        for part in sorted(slices, key=lambda part: part.slice_id)
    ]
    return hashlib.sha256(
        json.dumps(records, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _replace_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise SchemaEpochAdmissionError("Unexpected generated staging SQL shape")
    return text.replace(old, new, 1)


def _old_slice(
    provider: GeneratedVNextSchemaProvider, part: SchemaSlice
) -> SchemaSlice:
    """Reconstruct only the four retired shapes; the complete old DDL is pinned."""
    relation = part.slice_id.removeprefix("relation:")
    if relation not in _CHANGED:
        return part
    statement = part.statements[0]
    sql = statement.sql
    if relation in _CLEANUP:
        sql = _replace_once(
            sql,
            "'SOURCE_COLLECTION', "
            if relation == "cleanup_target_kind"
            else _COLLECTION_PHASES,
            "",
        )
    elif relation == _CLAIM:
        sql = _replace_once(
            sql,
            "operational_ingest_generations",
            "operational_source_build_generations",
        )
    else:
        quote = '"' if provider.backend == "sqlite" else "`"
        binary = "BLOB" if provider.backend == "sqlite" else "BINARY(16)"

        def q(name: str) -> str:
            return quote + name + quote

        sql = _replace_once(
            sql,
            f"  {q('staging_id')} {binary} NOT NULL,",
            f"  {q('staging_id')} {binary} NOT NULL,\n  {q('build_id')} {binary} NOT NULL,",
        )
        unique = f"  CONSTRAINT {q('uk_' + _HEADER_TABLE + '_1')} UNIQUE ({q('gallery_id')}, {q('observation_id')}),"
        sql = _replace_once(
            sql,
            unique,
            f"  CONSTRAINT {q('uk_' + _HEADER_TABLE + '_1')} UNIQUE ({q('build_id')}),\n"
            + unique.replace("stagings_1", "stagings_2"),
        )
        foreign = f"  CONSTRAINT {q('fk_gallery_observation_staging_1')} FOREIGN KEY ({q('gallery_id')}, {q('observation_id')}) REFERENCES {q('catalog_gallery_observation_allocations')} ({q('gallery_id')}, {q('observation_id')}),"
        sql = _replace_once(
            sql,
            foreign,
            f"  CONSTRAINT {q('fk_gallery_observation_staging_1')} FOREIGN KEY ({q('build_id')}) REFERENCES {q('catalog_source_build_descriptor')} ({q('build_id')}),\n"
            + foreign.replace("staging_1", "staging_2"),
        )
        prefix = (
            "typeof(build_id) = 'blob' AND "
            if provider.backend == "sqlite"
            else "build_id IS NOT NULL AND "
        )
        storage = (
            f"  CONSTRAINT {q('ck_gallery_observation_staging_storage_domain')} CHECK ("
        )
        sql = _replace_once(sql, storage, storage + prefix)
        length = "length" if provider.backend == "sqlite" else "octet_length"
        identity = f"  CONSTRAINT {q('ck_gallery_observation_staging_staging_id_len')}"
        sql = _replace_once(
            sql,
            identity,
            f"  CONSTRAINT {q('ck_gallery_observation_staging_build_id_len')} CHECK ({length}(build_id) = 16),\n"
            + identity,
        )
    return SchemaSlice(
        part.slice_id,
        (
            SchemaCreateStatement(statement.statement_id, sql, statement.creates),
            *part.statements[1:],
        ),
    )


def _conversion_slices(
    provider: GeneratedVNextSchemaProvider,
) -> tuple[tuple[SchemaSlice, ...], tuple[SchemaSlice, ...]]:
    definition = provider.definition
    if definition.epoch != 3 or definition.schema_version != 8:
        raise SchemaEpochAdmissionError("This converter requires schema-8 software")
    additions = tuple(
        part
        for part in definition.slices
        if part.slice_id.removeprefix("relation:") in _ADDITIONS
    )
    retained = tuple(
        _old_slice(provider, part)
        for part in definition.slices
        if part not in additions
    )
    if (
        len(additions) != len(_ADDITIONS)
        or _slices_digest(retained) != _OLD_SLICES[provider.backend]
    ):
        raise SchemaEpochAdmissionError(
            "The installed provider is not the exact supported schema-7 to 8 change"
        )
    _conversion_seeds(provider)
    return retained, additions


def _conversion_seeds(
    provider: GeneratedVNextSchemaProvider,
) -> tuple[dict[str, Any], ...]:
    records = provider.generated_definition_data["bootstrap_seeds"]
    additions = tuple(
        record
        for record in records
        if "source-collection.v1" in record["seed_id"]
        or ".cleanup-phase.sc-" in record["seed_id"]
    )
    retained = tuple(record for record in records if record not in additions)
    checksum = hashlib.sha256(
        json.dumps(
            sorted(retained, key=lambda row: row["seed_id"]),
            sort_keys=True,
            separators=(",", ":"),
            default=lambda value: value.hex(),
        ).encode()
    ).hexdigest()
    if checksum != _OLD_SEEDS[provider.backend] or len(additions) != 262:
        raise SchemaEpochAdmissionError(
            "Bootstrap facts differ from the exact schema-7 conversion"
        )
    return additions


def _relation(provider: GeneratedVNextSchemaProvider, name: str) -> dict[str, Any]:
    return next(
        dict(item)
        for item in provider.generated_definition_data["relations"]
        if item["relation"] == name
    )


def _old_relation(provider: GeneratedVNextSchemaProvider, name: str) -> dict[str, Any]:
    relation = _relation(provider, name)
    foreign = list(relation["foreign_keys"])
    if name in _CLEANUP:
        fragment = (
            "'SOURCE_COLLECTION', "
            if name == "cleanup_target_kind"
            else _COLLECTION_PHASES
        )
        relation["checks"] = tuple(
            (key, expression.replace(fragment, ""))
            for key, expression in relation["checks"]
        )
    elif name == _CLAIM:
        foreign[1] = (
            *foreign[1][:2],
            "operational_source_build_generations",
            foreign[1][3],
        )
    elif name == _HEADER:
        binary = "BLOB" if provider.backend == "sqlite" else "BINARY(16)"
        columns = list(relation["columns"])
        columns.insert(1, ("build_id", "build_id", binary, False, None))
        relation["columns"] = tuple(columns)
        relation["unique_keys"] = (("build_id",), *relation["unique_keys"])
        foreign = [
            (
                "fk_gallery_observation_staging_1",
                ("build_id",),
                "catalog_source_build_descriptor",
                ("build_id",),
            ),
            ("fk_gallery_observation_staging_2", *foreign[0][1:]),
        ]
        checks = list(relation["checks"])
        prefix = (
            "typeof(build_id) = 'blob' AND "
            if provider.backend == "sqlite"
            else "build_id IS NOT NULL AND "
        )
        checks[0] = (checks[0][0], prefix + checks[0][1])
        length = "length" if provider.backend == "sqlite" else "octet_length"
        checks.insert(
            1,
            ("ck_gallery_observation_staging_build_id_len", f"{length}(build_id) = 16"),
        )
        relation["checks"] = tuple(checks)
    relation["foreign_keys"] = tuple(foreign)
    return relation


def _validate_old_slice(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, part: SchemaSlice
) -> None:
    name = part.slice_id.removeprefix("relation:")
    if name not in _CHANGED:
        provider.validate_slice(connector, part)
        return
    statement = part.statements[0]
    provider._validate_object_sql(
        connector,
        kind=statement.creates.kind.value,
        name=statement.creates.name,
        expected_sql=statement.sql,
    )
    provider._validate_relation_shape(connector, _old_relation(provider, name))
    for extra in part.statements[1:]:
        provider._validate_object_sql(
            connector,
            kind=extra.creates.kind.value,
            name=extra.creates.name,
            expected_sql=extra.sql,
        )


@contextmanager
def _exclusive_conversion(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "sqlite":
        # Each phase owns BEGIN IMMEDIATE; callers explicitly stop all consumers.
        yield
        return
    database = connector.fetch_one("SELECT DATABASE()")
    if len(database) != 1 or not isinstance(database[0], str):
        raise SchemaEpochAdmissionError("Cannot identify the conversion database")
    with MariaDBAdvisorySchemaEpochGate().acquire_named(
        connector, mariadb_schema_epoch_gate_name(database[0])
    ):
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


def _part(provider: GeneratedVNextSchemaProvider, name: str) -> SchemaSlice:
    return next(
        part
        for part in provider.definition.slices
        if part.slice_id == "relation:" + name
    )


def _is_old(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, name: str
) -> bool:
    target = _part(provider, name)
    try:
        provider.validate_slice(connector, target)
    except SchemaEpochValidationError:
        try:
            _validate_old_slice(connector, provider, _old_slice(provider, target))
        except SchemaEpochValidationError:
            if provider.backend != "mariadb" or name not in {_HEADER, _CLAIM}:
                raise
            _validate_detached_mariadb(connector, provider, name)
        return True
    return False


def _detached_foreign(name: str) -> str:
    return (
        "fk_gallery_observation_staging_1"
        if name == _HEADER
        else "fk_gallery_observation_staging_claim_2"
    )


def _validate_detached_mariadb(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, name: str
) -> None:
    relation = _old_relation(provider, name)
    relation["foreign_keys"] = tuple(
        row for row in relation["foreign_keys"] if row[0] != _detached_foreign(name)
    )
    provider._validate_relation_shape(connector, relation)


def _detach_mariadb_foreign(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, name: str
) -> None:
    try:
        _validate_old_slice(
            connector, provider, _old_slice(provider, _part(provider, name))
        )
    except SchemaEpochValidationError:
        _validate_detached_mariadb(connector, provider, name)
        return
    table = _part(provider, name).statements[0].creates.name
    connector.execute(f"ALTER TABLE {table} DROP FOREIGN KEY {_detached_foreign(name)}")
    _validate_detached_mariadb(connector, provider, name)


def _copy_owners(connector: SQLConnector) -> None:
    if connector.fetch_one(
        f"SELECT 1 FROM {_BINDING_TABLE} b LEFT JOIN {_HEADER_TABLE} s "
        "ON s.staging_id = b.staging_id WHERE s.staging_id IS NULL "
        "OR s.build_id <> b.build_id LIMIT 1"
    ):
        raise SchemaEpochValidationError("Staging owner copy conflicts with schema 7")
    connector.execute(
        f"INSERT INTO {_BINDING_TABLE} (staging_id, build_id) "
        f"SELECT s.staging_id, s.build_id FROM {_HEADER_TABLE} s "
        f"LEFT JOIN {_BINDING_TABLE} b ON b.staging_id = s.staging_id "
        "WHERE b.staging_id IS NULL"
    )
    if connector.fetch_one(
        f"SELECT 1 FROM {_HEADER_TABLE} s LEFT JOIN {_BINDING_TABLE} b "
        "ON s.staging_id = b.staging_id WHERE b.staging_id IS NULL "
        "OR s.build_id <> b.build_id LIMIT 1"
    ):
        raise SchemaEpochValidationError("Staging owner copy is incomplete")


def _rebuild_sqlite(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, name: str
) -> None:
    statement = _part(provider, name).statements[0]
    original = statement.creates.name
    temporary = "offline_conversion_" + original
    sql = _replace_once(
        statement.sql,
        f'CREATE TABLE IF NOT EXISTS "{original}"',
        f'CREATE TABLE IF NOT EXISTS "{temporary}"',
    )
    columns = ", ".join(
        '"' + column[0] + '"' for column in _relation(provider, name)["columns"]
    )
    connector.execute(sql)
    connector.execute(
        f'INSERT INTO "{temporary}" ({columns}) SELECT {columns} FROM "{original}"'
    )
    for left, right in ((original, temporary), (temporary, original)):
        if connector.fetch_one(
            f'SELECT {columns} FROM "{left}" EXCEPT SELECT {columns} FROM "{right}" LIMIT 1'
        ):
            raise SchemaEpochValidationError("Staging copy differs from its original")
    connector.execute(f'DROP TABLE "{original}"')
    connector.execute(f'ALTER TABLE "{temporary}" RENAME TO "{original}"')
    for extra in _part(provider, name).statements[1:]:
        connector.execute(extra.sql)
    provider.validate_slice(connector, _part(provider, name))


def _alter_mariadb(
    connector: SQLConnector, provider: GeneratedVNextSchemaProvider, name: str
) -> None:
    table = _part(provider, name).statements[0].creates.name
    target = _relation(provider, name)
    clauses: list[str] = []
    foreigns: tuple[Any, ...]
    if name in _CLEANUP:
        old = dict(_old_relation(provider, name)["checks"])
        for key, expression in target["checks"]:
            if old[key] != expression:
                clauses.extend(
                    (
                        f"DROP CONSTRAINT {key}",
                        f"ADD CONSTRAINT {key} CHECK ({expression})",
                    )
                )
        foreigns = ()
    elif name == _HEADER:
        clauses.extend(
            [
                "DROP FOREIGN KEY fk_gallery_observation_staging_2",
                "DROP INDEX uk_operational_gallery_observation_stagings_1",
                "DROP INDEX uk_operational_gallery_observation_stagings_2",
                "DROP CONSTRAINT ck_gallery_observation_staging_storage_domain",
                "DROP CONSTRAINT ck_gallery_observation_staging_build_id_len",
                "DROP COLUMN build_id",
                "ADD CONSTRAINT uk_operational_gallery_observation_stagings_1 UNIQUE (gallery_id, observation_id)",
            ]
        )
        check_name, expression = target["checks"][0]
        clauses.append(f"ADD CONSTRAINT {check_name} CHECK ({expression})")
        foreigns = target["foreign_keys"]
    else:
        foreigns = target["foreign_keys"][1:]
    for key, columns, reference, references in foreigns:
        clauses.append(
            f"ADD CONSTRAINT {key} FOREIGN KEY ({', '.join(columns)}) REFERENCES {reference} ({', '.join(references)})"
        )
    # Each ALTER has an exact old, detached-reference or final shape;
    # MariaDB cannot reuse the removed FK name in the same ALTER operation.
    connector.execute(f"ALTER TABLE {table} " + ", ".join(clauses))
    provider.validate_slice(connector, _part(provider, name))


def _convert_structure(
    connector: SQLConnector,
    provider: GeneratedVNextSchemaProvider,
    *,
    checkpoint: Callable[[str], None],
    catalog: SchemaEpochCatalog,
    retained: tuple[SchemaSlice, ...],
    additions: tuple[SchemaSlice, ...],
    conversion_manifest: bytes,
) -> tuple[bool, int]:
    catalog.validate_control_table(connector)
    _, _, version, state, manifest, started_at, ready_at = _control(connector)
    target = bytes.fromhex(provider.definition.manifest_sha256)
    old_ready = (
        version == 7
        and state == "READY"
        and manifest == bytes.fromhex(_OLD_MANIFESTS[provider.backend])
        and type(ready_at) is int
        and ready_at >= cast(int, started_at)
    )
    converting = (
        version == 8
        and state == "BUILDING"
        and manifest == conversion_manifest
        and ready_at is None
    )
    target_ready = (
        version == 8
        and state == "READY"
        and manifest == target
        and type(ready_at) is int
        and ready_at >= cast(int, started_at)
    )
    if not (old_ready or converting or target_ready):
        raise SchemaEpochAdmissionError(
            "Only the exact READY schema 7 or this schema-8 conversion is supported"
        )
    if target_ready:
        return True, cast(int, started_at)
    actual = catalog.list_objects(connector)
    old_objects = _objects(retained) | {catalog.control_object}
    expected = provider.definition.expected_objects | {catalog.control_object}
    if (
        (old_ready and actual != old_objects)
        or not old_objects <= actual
        or not actual <= expected
    ):
        raise SchemaEpochValidationError("Conversion object inventory differs")
    for part in retained:
        name = part.slice_id.removeprefix("relation:")
        if converting and name in _CHANGED:
            _is_old(connector, provider, name)
        else:
            _validate_old_slice(connector, provider, part)
    checkpoint("retained_schema_validated")
    if old_ready:
        connector.execute(
            "UPDATE h2hdb_schema_epoch SET schema_version = 8, state = 'BUILDING', "
            "manifest_sha256 = %s, ready_at = NULL WHERE singleton_id = 1",
            (conversion_manifest,),
        )
        checkpoint("conversion_marked")
    for part in additions:
        for statement in part.statements:
            if statement.creates not in actual:
                connector.execute(statement.sql)
                checkpoint("object_created")
        provider.validate_slice(connector, part)
    if _is_old(connector, provider, _HEADER):
        _copy_owners(connector)
        checkpoint("owners_copied")
        if provider.backend == "sqlite":
            _rebuild_sqlite(connector, provider, _HEADER)
        else:
            _detach_mariadb_foreign(connector, provider, _HEADER)
            checkpoint("header_foreign_detached")
            _alter_mariadb(connector, provider, _HEADER)
        checkpoint("header_rebuilt")
    if _is_old(connector, provider, _CLAIM):
        if provider.backend == "sqlite":
            _rebuild_sqlite(connector, provider, _CLAIM)
        else:
            _detach_mariadb_foreign(connector, provider, _CLAIM)
            checkpoint("claim_foreign_detached")
            _alter_mariadb(connector, provider, _CLAIM)
        checkpoint("claim_rebuilt")
    for name in _CLEANUP:
        if _is_old(connector, provider, name):
            if provider.backend == "sqlite":
                _rebuild_sqlite(connector, provider, name)
            else:
                _alter_mariadb(connector, provider, name)
            checkpoint("cleanup_registry_rebuilt")
    for seed in _conversion_seeds(provider):
        connector.execute(seed["sql"], seed["parameters"])
        if (
            connector.fetch_one(seed["validation_sql"], seed["validation_parameters"])
            != seed["expected_row"]
        ):
            raise SchemaEpochValidationError("New cleanup bootstrap fact differs")
    checkpoint("bootstrap_completed")
    if catalog.list_objects(connector) != expected:
        raise SchemaEpochValidationError("Converted schema object inventory differs")
    checkpoint("addition_validated")
    return False, cast(int, started_at)


def _convert(
    connector: SQLConnector,
    provider: GeneratedVNextSchemaProvider,
    *,
    checkpoint: Callable[[str], None],
) -> str:
    retained, additions = _conversion_slices(provider)
    backend = provider.backend
    catalog: SchemaEpochCatalog = (
        SQLiteSchemaEpochCatalog()
        if backend == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )
    target_manifest = bytes.fromhex(provider.definition.manifest_sha256)
    conversion_manifest = hashlib.sha256(
        b"h2hdb-offline-source-collection-schema-7-to-8\0"
        + bytes.fromhex(_OLD_MANIFESTS[backend])
        + target_manifest
    ).digest()
    with _exclusive_conversion(connector, backend):
        if backend == "sqlite":
            # Rebuild with incoming child FKs preserved verbatim. The entire
            # structural phase commits atomically before FK enforcement returns.
            connector.execute("PRAGMA foreign_keys = OFF")
            try:
                with connector.transaction():
                    already, started_at = _convert_structure(
                        connector,
                        provider,
                        checkpoint=checkpoint,
                        catalog=catalog,
                        retained=retained,
                        additions=additions,
                        conversion_manifest=conversion_manifest,
                    )
            finally:
                connector.execute("PRAGMA foreign_keys = ON")
        else:
            # DDL commits implicitly. The distinct BUILDING checksum is durable
            # before the first CREATE/ALTER, and every restart rechecks all shapes.
            already, started_at = _convert_structure(
                connector,
                provider,
                checkpoint=checkpoint,
                catalog=catalog,
                retained=retained,
                additions=additions,
                conversion_manifest=conversion_manifest,
            )
        if backend == "mariadb":
            # Structural validation SELECTs may open an implicit client read
            # transaction; close it before the explicit final audit transaction.
            connector.commit()
        if already:
            with connector.read_transaction():
                checkpoint("full_audit_started")
                SchemaEpochRunner(gate=None, catalog=catalog).validate_ready(
                    connector, provider
                )
            return "already_converted"
        checkpoint("schema_committed")
        with connector.transaction():
            if backend == "sqlite" and connector.fetch_one("PRAGMA foreign_key_check"):
                raise SchemaEpochValidationError(
                    "Converted database has foreign-key violations"
                )
            changed = connector.execute_affected(
                "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s "
                "WHERE singleton_id = 1 AND epoch = 3 AND schema_version = 8 "
                "AND state = 'BUILDING' AND manifest_sha256 = %s AND ready_at IS NULL",
                (target_manifest, conversion_manifest),
            )
            if changed != 1:
                raise SchemaEpochValidationError("Conversion lost its pending control")
            checkpoint("full_audit_started")
            audit_started = perf_counter_ns()
            SchemaEpochRunner(gate=None, catalog=catalog)._validate_ready_schema(
                connector,
                provider,
                provider.definition,
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
            checkpoint("audit_baseline_recorded")
            connector.execute(
                "UPDATE h2hdb_schema_epoch SET state = 'READY', ready_at = %s WHERE singleton_id = 1",
                (max(time_ns() // 1_000, started_at),),
            )
            checkpoint("ready_recorded")
        checkpoint("ready_committed")
    return "converted"


def _show_progress(checkpoint: str) -> None:
    messages = {
        "retained_schema_validated": "The retained database structure matches schema 7.",
        "conversion_marked": "Conversion recorded; consumers must remain stopped.",
        "object_created": "Added a generated source collection relation.",
        "owners_copied": "Copied and exactly verified existing staging owners.",
        "header_foreign_detached": "Recorded the exact intermediate staging owner reference shape.",
        "claim_foreign_detached": "Recorded the exact intermediate staging generation reference shape.",
        "header_rebuilt": "Rebuilt the shared staging header, preserving its children.",
        "claim_rebuilt": "Updated the staging generation reference.",
        "cleanup_registry_rebuilt": "Updated the cleanup registry constraints.",
        "bootstrap_completed": "Installed and verified source collection cleanup facts.",
        "addition_validated": "The converted structure passed validation.",
        "schema_committed": "Structural conversion committed; starting the full audit.",
        "full_audit_started": "Running the complete database audit; this may take as long as a manual check.",
        "full_audit_completed": "The complete database audit passed; recording its result.",
        "audit_baseline_recorded": "Recorded the completed audit and retired previous runtime ownership; committing activation.",
        "ready_recorded": "Recorded schema 8 readiness; the final transaction has not committed yet.",
        "ready_committed": "Schema 8 and its completed audit baseline are durably committed.",
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
        f"Source collection schema conversion: {result}; existing database facts and CBZs retained"
    )


if __name__ == "__main__":
    main()
