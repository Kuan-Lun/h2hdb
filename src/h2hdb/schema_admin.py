"""Public administration boundary for the greenfield schema epoch.

The package has no numbered migration or legacy maintenance-table path. Epoch
construction and validation own one connector and delegate serialization to
:mod:`h2hdb.schema_epoch`.
"""

from __future__ import annotations

__all__ = ["SchemaEpochReadiness", "VNextSchemaAdmin"]

from dataclasses import dataclass

from .domain import StorageInstanceBinding
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
    validate_mariadb_schema_epoch,
    validate_sqlite_schema_epoch,
)
from .sql_connector import SQLConnector
from .vnext_storage_instance_repository import (
    StorageInstanceBindingError,
    StorageInstanceBindingUnavailableError,
    VNextStorageInstanceRepository,
)
from .vnext_transaction import VNextUnitOfWork


@dataclass(frozen=True, slots=True)
class SchemaEpochReadiness:
    """Exact durable READY marker for the injected v3 schema manifest."""

    epoch: int
    schema_version: int
    state: str
    manifest_sha256: str
    started_at: int
    ready_at: int


class VNextSchemaAdmin:
    """Administer the sole epoch-3 production schema."""

    def __init__(self, context: RepositoryContext) -> None:
        self._context = context

    def initialize(self) -> SchemaEpochReport:
        """Create/resume epoch v3, or fully validate an existing READY epoch."""

        resolved, _ = self._resolve_provider()
        with self._context.SQLConnector() as connector:
            return self._run(connector, resolved)

    def check(self) -> SchemaEpochReport:
        """Fully validate an already-READY epoch without constructing it."""

        resolved, definition = self._resolve_provider()
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                self._readiness_with_connector(connector, definition)
                return self._validate_ready(connector, resolved)

    def check_readiness(self) -> SchemaEpochReadiness:
        """Read the exact READY marker in O(1) database work.

        This deliberately does not perform closed-world or semantic validation;
        callers use :meth:`check` for that stronger, potentially expensive
        operation.  Provider resolution still happens before opening the
        database, so a stale or blocked generated artifact fails closed.
        """

        _, definition = self._resolve_provider()
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                return self._readiness_with_connector(connector, definition)

    def bind_storage_instance(
        self,
        storage_instance_uuid: bytes,
    ) -> StorageInstanceBinding:
        """Bind the exact generated READY schema to one external identity."""

        requested = StorageInstanceBinding(storage_instance_uuid)
        try:
            _, definition = self._resolve_provider()
        except Exception as error:
            raise StorageInstanceBindingUnavailableError(
                "storage binding schema provider is unavailable"
            ) from error
        try:
            with self._context.SQLConnector() as connector:
                with connector.transaction():
                    return VNextStorageInstanceRepository.bind(
                        VNextUnitOfWork(
                            connector,
                            backend=self._context.sql_type,
                        ),
                        storage_instance_uuid=requested.storage_instance_uuid,
                        expected_epoch=definition.epoch,
                        expected_schema_version=definition.schema_version,
                        expected_manifest_sha256=bytes.fromhex(
                            definition.manifest_sha256
                        ),
                    )
        except StorageInstanceBindingError:
            raise
        except Exception as error:
            raise StorageInstanceBindingUnavailableError(
                "storage binding database authority is unavailable"
            ) from error

    def _resolve_provider(
        self,
    ) -> tuple[SchemaEpochProvider, SchemaEpochDefinition]:
        # Keep the generated multi-thousand-object artifact off ordinary
        # production facade imports; only explicit epoch-v3 administration
        # loads and validates it.  This is intentionally not an injection seam:
        # the production administrator accepts only the wheel-owned provider.
        from .vnext_schema_provider import GeneratedVNextSchemaProvider

        resolved: SchemaEpochProvider
        if self._context.sql_type == "sqlite":
            resolved = GeneratedVNextSchemaProvider("sqlite")
        elif self._context.sql_type == "mariadb":
            resolved = GeneratedVNextSchemaProvider("mariadb")
        else:
            raise ValueError(f"Unsupported SQL type: {self._context.sql_type!r}")
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

    def _validate_ready(
        self,
        connector: SQLConnector,
        provider: SchemaEpochProvider,
    ) -> SchemaEpochReport:
        if self._context.sql_type == "sqlite":
            return validate_sqlite_schema_epoch(connector, provider)
        if self._context.sql_type == "mariadb":
            return validate_mariadb_schema_epoch(connector, provider)
        raise ValueError(f"Unsupported SQL type: {self._context.sql_type!r}")

    @staticmethod
    def _readiness_with_connector(
        connector: SQLConnector,
        definition: SchemaEpochDefinition,
    ) -> SchemaEpochReadiness:
        if not connector.check_table_exists(SCHEMA_EPOCH_CONTROL_TABLE):
            raise SchemaEpochAdmissionError(
                "Schema epoch v3 is not initialized: its control table is missing"
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
                "Schema epoch v3 readiness marker is unreadable"
            ) from error
        if len(rows) != 1 or len(rows[0]) != 6:
            raise SchemaEpochAdmissionError(
                "Schema epoch v3 readiness requires exactly one control row"
            )
        epoch, schema_version, state, manifest, started_at, ready_at = rows[0]
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            raise SchemaEpochValidationError("Schema epoch v3 epoch is invalid")
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            raise SchemaEpochValidationError(
                "Schema epoch v3 schema version is invalid"
            )
        if epoch != definition.epoch or schema_version != definition.schema_version:
            raise SchemaEpochAdmissionError(
                "Schema epoch v3 marker does not match the expected epoch/version"
            )
        if state != "READY":
            raise SchemaEpochAdmissionError(
                f"Schema epoch v3 is not READY (state={state!r})"
            )
        if not isinstance(manifest, (bytes, bytearray)) or bytes(
            manifest
        ) != bytes.fromhex(definition.manifest_sha256):
            raise SchemaEpochAdmissionError(
                "Schema epoch v3 marker does not match the expected manifest"
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
                "Schema epoch v3 READY timestamps are invalid"
            )
        return SchemaEpochReadiness(
            epoch=epoch,
            schema_version=schema_version,
            state=state,
            manifest_sha256=bytes(manifest).hex(),
            started_at=started_at,
            ready_at=ready_at,
        )
