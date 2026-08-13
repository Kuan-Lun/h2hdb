"""Public administration boundary for the greenfield schema epoch.

This boundary intentionally does not use the legacy migration or maintenance
tables.  Epoch construction and validation own one connector and delegate
serialization to :mod:`h2hdb.schema_epoch`.
"""

from __future__ import annotations

__all__ = ["SchemaEpochReadiness", "VNextSchemaAdmin"]

from dataclasses import dataclass

from .repository import RepositoryContext
from .schema_epoch import (
    SCHEMA_EPOCH_CONTROL_TABLE,
    SchemaEpochAdmissionError,
    SchemaEpochDefinition,
    SchemaEpochProvider,
    SchemaEpochReport,
    SchemaEpochValidationError,
    run_mariadb_schema_epoch,
    run_sqlite_schema_epoch,
)
from .sql_connector import SQLConnector


@dataclass(frozen=True, slots=True)
class SchemaEpochReadiness:
    """Exact durable READY marker for the injected v2 schema manifest."""

    epoch: int
    schema_version: int
    state: str
    manifest_sha256: str
    started_at: int
    ready_at: int


class VNextSchemaAdmin:
    """Administer epoch v2 without entering any legacy database gate."""

    def __init__(self, context: RepositoryContext) -> None:
        self._context = context

    def initialize(
        self, provider: SchemaEpochProvider | None = None
    ) -> SchemaEpochReport:
        """Create/resume epoch v2, or fully validate an existing READY epoch."""

        resolved, _ = self._resolve_provider(provider)
        with self._context.SQLConnector() as connector:
            return self._run(connector, resolved)

    def check(self, provider: SchemaEpochProvider | None = None) -> SchemaEpochReport:
        """Fully validate an already-READY epoch without constructing it."""

        resolved, definition = self._resolve_provider(provider)
        with self._context.SQLConnector() as connector:
            self._readiness_with_connector(connector, definition)
            return self._run(connector, resolved)

    def check_readiness(
        self, provider: SchemaEpochProvider | None = None
    ) -> SchemaEpochReadiness:
        """Read the exact READY marker in O(1) database work.

        This deliberately does not perform closed-world or semantic validation;
        callers use :meth:`check` for that stronger, potentially expensive
        operation.  Provider resolution still happens before opening the
        database, so a stale or blocked generated artifact fails closed.
        """

        _, definition = self._resolve_provider(provider)
        with self._context.SQLConnector() as connector:
            return self._readiness_with_connector(connector, definition)

    def _resolve_provider(
        self, provider: SchemaEpochProvider | None
    ) -> tuple[SchemaEpochProvider, SchemaEpochDefinition]:
        resolved: SchemaEpochProvider
        if provider is None:
            # Keep the generated multi-thousand-object artifact off ordinary
            # production facade imports; only explicit epoch-v2 administration
            # loads and validates it.
            from .vnext_schema_provider import GeneratedVNextSchemaProvider

            if self._context.sql_type == "sqlite":
                resolved = GeneratedVNextSchemaProvider("sqlite")
            elif self._context.sql_type == "mariadb":
                resolved = GeneratedVNextSchemaProvider("mariadb")
            else:
                raise ValueError(f"Unsupported SQL type: {self._context.sql_type!r}")
        else:
            resolved = provider
        # Definition validation (including generated-artifact blockers) must
        # finish before a connector is opened or any database state is touched.
        return resolved, resolved.definition

    def _run(
        self, connector: SQLConnector, provider: SchemaEpochProvider
    ) -> SchemaEpochReport:
        if self._context.sql_type == "sqlite":
            return run_sqlite_schema_epoch(connector, provider)
        if self._context.sql_type == "mariadb":
            return run_mariadb_schema_epoch(connector, provider)
        raise ValueError(f"Unsupported SQL type: {self._context.sql_type!r}")

    @staticmethod
    def _readiness_with_connector(
        connector: SQLConnector,
        definition: SchemaEpochDefinition,
    ) -> SchemaEpochReadiness:
        if not connector.check_table_exists(SCHEMA_EPOCH_CONTROL_TABLE):
            raise SchemaEpochAdmissionError(
                "Schema epoch v2 is not initialized: its control table is missing"
            )
        try:
            rows = connector.fetch_all("""
                SELECT epoch, schema_version, state, manifest_sha256,
                       started_at, ready_at
                FROM h2hdb_schema_epoch
                WHERE singleton_id = 1
                LIMIT 2
                """)
        except Exception as error:
            raise SchemaEpochValidationError(
                "Schema epoch v2 readiness marker is unreadable"
            ) from error
        if len(rows) != 1 or len(rows[0]) != 6:
            raise SchemaEpochAdmissionError(
                "Schema epoch v2 readiness requires exactly one control row"
            )
        epoch, schema_version, state, manifest, started_at, ready_at = rows[0]
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            raise SchemaEpochValidationError("Schema epoch v2 epoch is invalid")
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            raise SchemaEpochValidationError(
                "Schema epoch v2 schema version is invalid"
            )
        if epoch != definition.epoch or schema_version != definition.schema_version:
            raise SchemaEpochAdmissionError(
                "Schema epoch v2 marker does not match the expected epoch/version"
            )
        if state != "READY":
            raise SchemaEpochAdmissionError(
                f"Schema epoch v2 is not READY (state={state!r})"
            )
        if not isinstance(manifest, (bytes, bytearray)) or bytes(
            manifest
        ) != bytes.fromhex(definition.manifest_sha256):
            raise SchemaEpochAdmissionError(
                "Schema epoch v2 marker does not match the expected manifest"
            )
        if (
            not isinstance(started_at, int)
            or isinstance(started_at, bool)
            or started_at < 0
            or not isinstance(ready_at, int)
            or isinstance(ready_at, bool)
            or ready_at < started_at
        ):
            raise SchemaEpochValidationError(
                "Schema epoch v2 READY timestamps are invalid"
            )
        return SchemaEpochReadiness(
            epoch=epoch,
            schema_version=schema_version,
            state=state,
            manifest_sha256=bytes(manifest).hex(),
            started_at=started_at,
            ready_at=ready_at,
        )
