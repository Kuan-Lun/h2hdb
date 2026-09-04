"""Immutable database binding for one consumer-owned storage instance."""

from __future__ import annotations

__all__ = [
    "StorageInstanceBindingError",
    "StorageInstanceBindingMismatchError",
    "StorageInstanceBindingUnavailableError",
    "VNextStorageInstanceRepository",
]

from .domain import StorageInstanceBinding
from .vnext_domains import require_digest32, require_uuid16
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_BINDING_TABLE = "operational_storage_instance_bindings"
_EPOCH_TABLE = "h2hdb_schema_epoch"


def _stored_binary(value: object) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise TypeError("stored storage instance UUID is not binary")


class StorageInstanceBindingError(RuntimeError):
    """The durable storage-instance binding cannot be used safely."""


class StorageInstanceBindingMismatchError(StorageInstanceBindingError):
    """The requested storage instance differs from the immutable binding."""


class StorageInstanceBindingUnavailableError(StorageInstanceBindingError):
    """The exact READY schema authority required for binding is unavailable."""


class VNextStorageInstanceRepository:
    """Bind the database once, replaying only the exact same UUID."""

    @staticmethod
    def bind(
        work: VNextUnitOfWork,
        *,
        storage_instance_uuid: bytes,
        expected_epoch: int,
        expected_schema_version: int,
        expected_manifest_sha256: bytes,
    ) -> StorageInstanceBinding:
        requested = StorageInstanceBinding(
            require_uuid16(
                storage_instance_uuid,
                field="storage instance UUID",
            )
        )
        expected_manifest = require_digest32(
            expected_manifest_sha256,
            field="expected schema manifest SHA-256",
        )
        try:
            epoch = work.lock_row(
                LockRank.MAINTENANCE_GATE,
                encode_lock_key("storage-instance-binding"),
                f"SELECT epoch, schema_version, state, manifest_sha256 "
                f"FROM {_EPOCH_TABLE} WHERE singleton_id = 1",
            )
        except Exception as error:
            raise StorageInstanceBindingUnavailableError(
                "storage instance binding requires a readable schema authority"
            ) from error
        try:
            stored_manifest = _stored_binary(epoch[3]) if len(epoch) == 4 else b""
        except TypeError:
            stored_manifest = b""
        if (
            len(epoch) != 4
            or epoch[:3] != (expected_epoch, expected_schema_version, "READY")
            or stored_manifest != expected_manifest
        ):
            raise StorageInstanceBindingUnavailableError(
                "storage instance binding requires the exact READY schema"
            )

        try:
            row = work.connector.fetch_one(
                f"SELECT singleton_id, storage_instance_uuid FROM {_BINDING_TABLE} "
                "WHERE singleton_id = 1"
            )
        except Exception as error:
            raise StorageInstanceBindingUnavailableError(
                "storage instance binding relation is unavailable"
            ) from error
        if row == ():
            try:
                work.connector.execute(
                    f"INSERT INTO {_BINDING_TABLE} "
                    "(singleton_id, storage_instance_uuid) VALUES (1, %s)",
                    (requested.storage_instance_uuid,),
                )
            except Exception as error:
                raise StorageInstanceBindingUnavailableError(
                    "storage instance binding could not be recorded"
                ) from error
            return requested
        if len(row) != 2 or row[0] != 1:
            raise StorageInstanceBindingError(
                "stored storage instance binding has an invalid shape"
            )
        try:
            stored = StorageInstanceBinding(_stored_binary(row[1]))
        except (TypeError, ValueError) as error:
            raise StorageInstanceBindingError(
                "stored storage instance UUID is invalid"
            ) from error
        if stored != requested:
            raise StorageInstanceBindingMismatchError(
                "database is already bound to a different storage instance"
            )
        return stored
