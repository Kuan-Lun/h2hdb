"""Bounded production refinements for the vNext operational plane.

Full CHECK validation is deliberately independent of most catalog and operational
corpus sizes except for the explicitly capped gallery-staging request budget audit,
at most 256 frozen PUBLICATION_COMMIT roots during transient event retirement, and
indexed global effect-root owner probes. It
reads the schema-epoch singleton, exact generated registries, the current ingest
head through primary-key lookups, the current 64-slot maintenance gate, the two
allocator registries, the fixed cleanup-shard control plane, and one bounded
``COUNT(*)`` over at most 1,500,000 retained staging requests. It never walks
unbounded queues, event payload history, canonical page trees, hash caches, or
source revisions. The separate public ``ready`` probe remains O(1).

Those high-cardinality and temporal properties belong to the named
same-transaction writer hooks. ``OPERATIONAL_RUNTIME_WRITER_BLOCKERS`` states
that boundary explicitly; a bounded READY callback is not evidence that its
writer hook has been implemented.
"""

from __future__ import annotations

__all__ = [
    "OPERATIONAL_RUNTIME_WRITER_BLOCKERS",
    "OperationalSemanticRegistryError",
    "OperationalSemanticValidationError",
    "builtin_operational_semantic_validators",
    "check_attempt_identity_contract_v1",
    "check_bootstrap_contract_v1",
    "check_bounded_work_contract_v1",
    "check_build_generation_contract_v1",
    "check_canonical_hash_cache_contract_v1",
    "check_cleanup_reachability_v1",
    "check_cleanup_frozen_root_set_v1",
    "check_download_ingest_handoff_contract_v1",
    "check_epoch_manifest_v1",
    "check_event_integrity_contract_v1",
    "check_fencing_contract_v1",
    "check_gallery_staging_contract_v1",
    "check_gallery_staging_request_budget_v1",
    "check_maintenance_gate_contract_v1",
    "check_physical_domains_v1",
    "check_queue_history_contract_v1",
    "check_revision_allocator_contract_v1",
    "validate_builtin_operational_manifest",
]

import hashlib
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast

from ._generated_vnext_schema import ARTIFACT
from .schema_epoch import SchemaEpochValidationError
from .sql_connector import SQLConnector

type Backend = Literal["sqlite", "mariadb"]
type SemanticValidator = Callable[[SQLConnector], None]

_INT63_MAX = 9223372036854775807
_CLEANUP_ID_DOMAIN = b"h2hdb-cleanup-cycle-v1\0"
_CLEANUP_CHAIN_DOMAIN = b"h2hdb-cleanup-chain-v1\0"
_CLEANUP_FROZEN_ROOT_SET_DOMAIN = b"h2hdb-cleanup-frozen-root-set-v1\0"
_CLEANUP_FROZEN_ROOT_SHAPES: Mapping[str, tuple[tuple[bytes, int], ...]] = (
    MappingProxyType(
        {
            "SOURCE_BUILD": ((b"b", 16),),
            "ANALYSIS_RUN": ((b"b", 16),),
            "CATALOG_PUBLICATION": ((b"i", 8), (b"b", 32)),
            "PUBLICATION_COMMIT": ((b"b", 16), (b"b", 16)),
            "CATALOG_REVISION_DESCRIPTOR": ((b"i", 8),),
            "SOURCE_REVISION_DESCRIPTOR": ((b"i", 8),),
            "PUBLICATION_GENERATION": ((b"i", 8),),
            "PUBLICATION_CANDIDATE": ((b"b", 16),),
            "OPERATIONAL_PREPARATION": ((b"b", 16),),
            "GALLERY_OBSERVATION": ((b"i", 8), (b"i", 8)),
            "GALLERY_OBSERVATION_STAGING": ((b"b", 16),),
            "ARTIFACT_BLOB": ((b"b", 32),),
            "CANONICAL_VALUE": ((b"b", 32),),
            "CONTENT_BLOB": ((b"b", 32),),
            "GALLERY_OBSERVATION_PAGE": ((b"b", 32),),
            "FILE_NAME_IDENTITY": ((b"b", 32),),
            "PUBLICATION_IDENTITY": ((b"b", 32),),
            "GALLERY_IDENTITY": ((b"i", 8),),
            "SOURCE_GALLERY_NAME_GID": ((b"b", 255),),
            "GALLERY_UPLOAD_TIME": ((b"i", 8),),
            "CANONICAL_VALUE_UPLOAD": ((b"i", 8), (b"b", 32)),
            "HASH_CACHE_OBSERVATION": ((b"b", 32), (b"b", 32)),
        }
    )
)
_CLEANUP_STATE_ROOT_PHASE_CHAINS = {
    "SOURCE_BUILD": ("SB_STATE", "SB_ROOT"),
    "ANALYSIS_RUN": ("AR_COMPLETION", "AR_STATE", "AR_ROOT"),
}


class OperationalSemanticRegistryError(RuntimeError):
    """The installed validator registry differs from the generated artifact."""


class OperationalSemanticValidationError(SchemaEpochValidationError):
    """Bounded operational authority does not refine the generated contract."""


# id, lifecycle, READY check, writer hook
_SPECS = (
    (
        "h2hdb.operational.physical-domains.v1",
        "ready_validation",
        "operational_refinement.check_physical_domains_v1",
        "operational_writer.validate_physical_domains",
    ),
    (
        "h2hdb.operational.epoch-manifest.v1",
        "building_to_ready",
        "operational_refinement.check_epoch_manifest_v1",
        "schema_epoch.validate_operational_manifest",
    ),
    (
        "h2hdb.operational.fencing.v1",
        "ready_and_runtime",
        "operational_refinement.check_fencing_contract_v1",
        "operational_writer.validate_ingest_fencing",
    ),
    (
        "h2hdb.operational.download-ingest-handoff.v1",
        "ready_and_runtime",
        "operational_refinement.check_download_ingest_handoff_contract_v1",
        "operational_writer.validate_download_ingest_handoff",
    ),
    (
        "h2hdb.operational.maintenance-gate.v1",
        "ready_and_runtime",
        "operational_refinement.check_maintenance_gate_contract_v1",
        "operational_writer.validate_maintenance_gate",
    ),
    (
        "h2hdb.operational.bounded-work.v1",
        "ready_and_runtime",
        "operational_refinement.check_bounded_work_contract_v1",
        "operational_writer.validate_bounded_work",
    ),
    (
        "h2hdb.operational.queue-history.v1",
        "ready_and_runtime",
        "operational_refinement.check_queue_history_contract_v1",
        "operational_writer.validate_queue_history",
    ),
    (
        "h2hdb.operational.canonical-hash-cache.v1",
        "ready_and_runtime",
        "operational_refinement.check_canonical_hash_cache_contract_v1",
        "operational_writer.validate_canonical_hash_cache",
    ),
    (
        "h2hdb.operational.event-integrity.v1",
        "ready_and_runtime",
        "operational_refinement.check_event_integrity_contract_v1",
        "operational_writer.validate_event_integrity",
    ),
    (
        "h2hdb.operational.build-generation.v1",
        "ready_and_runtime",
        "operational_refinement.check_build_generation_contract_v1",
        "operational_writer.validate_build_generation",
    ),
    (
        "h2hdb.operational.attempt-identity.v1",
        "ready_and_runtime",
        "operational_refinement.check_attempt_identity_contract_v1",
        "operational_writer.validate_attempt_identity",
    ),
    (
        "h2hdb.operational.cleanup-reachability.v1",
        "ready_and_runtime",
        "operational_refinement.check_cleanup_reachability_v1",
        "operational_writer.validate_cleanup_reachability",
    ),
    (
        "h2hdb.operational.cleanup-frozen-root-set.v1",
        "ready_and_runtime",
        "operational_refinement.check_cleanup_frozen_root_set_v1",
        "operational_writer.validate_cleanup_frozen_root_set",
    ),
    (
        "h2hdb.operational.revision-allocation.v1",
        "ready_and_runtime",
        "operational_refinement.check_revision_allocator_contract_v1",
        "operational_writer.validate_revision_allocation",
    ),
    (
        "h2hdb.operational.gallery-staging.v1",
        "ready_and_runtime",
        "operational_refinement.check_gallery_staging_contract_v1",
        "operational_writer.validate_gallery_staging",
    ),
    (
        "h2hdb.operational.gallery-staging-request-budget.v1",
        "ready_and_runtime",
        "operational_refinement.check_gallery_staging_request_budget_v1",
        "operational_writer.enforce_gallery_staging_request_budget",
    ),
    (
        "h2hdb.operational.bootstrap-genesis.v1",
        "building_only",
        "operational_refinement.check_bootstrap_contract_v1",
        "schema_epoch.write_operational_bootstrap",
    ),
)

_CONTRACT_KEYS = {
    "check",
    "class",
    "description",
    "hook",
    "id",
    "lifecycle",
    "ready_check",
    "relations",
    "scope",
    "version",
    "writer_hook",
    "writer_hook_version",
}


# These are intentionally specific. They document what READY does *not* prove
# and remain blockers until the named hook is installed at every mutation site.
OPERATIONAL_RUNTIME_WRITER_BLOCKERS: Mapping[str, str] = MappingProxyType(
    {
        "h2hdb.operational.physical-domains.v1": (
            "every operational writer must rely on generated types and CHECK "
            "constraints; bounded READY does not rescan stored corpus rows"
        ),
        "h2hdb.operational.fencing.v1": (
            "each mutation must lock/CAS the current ingest head and recheck the "
            "exact owner token plus unexpired lease; READY checks current shape only"
        ),
        "h2hdb.operational.download-ingest-handoff.v1": (
            "the coordinator must lock download before ingest authority, issue opaque "
            "capabilities, atomically move owner into handoff, consume one-to-one, "
            "and exact-replay durable handoff, consumption, and completion tuples"
        ),
        "h2hdb.operational.maintenance-gate.v1": (
            "slot acquisition, replacement, expiry, and authorization require exact "
            "observed-owner CAS in the committing transaction"
        ),
        "h2hdb.operational.bounded-work.v1": (
            "preparation and cleanup writers must atomically persist decisions, "
            "receipt, and checkpoint CAS; READY inspects only fixed cleanup shards"
        ),
        "h2hdb.operational.queue-history.v1": (
            "queue writers must preserve immutable attempts and exact GID congruence; "
            "READY never scans deletion history"
        ),
        "h2hdb.operational.canonical-hash-cache.v1": (
            "canonical writers/readers must stream the exact framed preimage, "
            "recompute SHA-256 and byte_count, and byte-compare collisions; READY "
            "never walks canonical page trees or hash-cache observations"
        ),
        "h2hdb.operational.event-integrity.v1": (
            "event and effect-seal writers must insert the exact subtype and advance "
            "preparation-coordinate coverage atomically; "
            "candidate sealing must bind exactly one sealed preparation one-to-one; "
            "full CHECK validates effect-root owners and accepts missing pairs only "
            "under exact frozen OPEN PCOM_EVENT cursor/compound receipt authority; "
            "the bounded writer and cleanup protocol remain completeness authority"
        ),
        "h2hdb.operational.build-generation.v1": (
            "begin/resume/takeover must reserve generation-to-build identity under "
            "the live ingest fence; READY validates generated key metadata only"
        ),
        "h2hdb.operational.attempt-identity.v1": (
            "preparation writers must preserve policy-qualified retry identity, and "
            "cleanup writers must CAS the exact monotone cycle generation"
        ),
        "h2hdb.operational.cleanup-reachability.v1": (
            "every destructive batch must use the closed target/phase registry, "
            "recheck all retention roots and blockers, and delete child-first"
        ),
        "h2hdb.operational.cleanup-frozen-root-set.v1": (
            "begin must freeze and seal at most 256 typed roots under the sole "
            "OPEN cleanup authority; every static phase must restrict itself to "
            "that exact set, and terminal completion must delete membership "
            "atomically"
        ),
        "h2hdb.operational.revision-allocation.v1": (
            "allocation must return-and-increment the exact stream row under lock/CAS, "
            "and publication must prove revision < current next_revision"
        ),
        "h2hdb.operational.gallery-staging.v1": (
            "staging writers must enforce live claims, complete request bytes and "
            "exact-one subtype, cursor/predecessor replay, canonical page/tree bounds, "
            "terminal metadata, allocation ownership, and final membership atomically"
        ),
        "h2hdb.operational.gallery-staging-request-budget.v1": (
            "fresh staging request identities must reserve the seeded singleton "
            "under HEAD before CHILD locks; in-band retirement and both generic "
            "cleanup paths must release only actual deletions atomically, while "
            "one build slot and the shared-fenced implicit ACK bound normal retention"
        ),
    }
)


def _operational_obligations() -> tuple[Mapping[str, Any], ...]:
    raw = ARTIFACT.get("semantic_obligations")
    if not isinstance(raw, tuple):
        raise OperationalSemanticRegistryError(
            "generated semantic-obligation registry is malformed"
        )
    result: list[Mapping[str, Any]] = []
    for value in raw:
        if not isinstance(value, Mapping):
            raise OperationalSemanticRegistryError(
                "generated semantic-obligation record is malformed"
            )
        if value.get("source") == "operational":
            result.append(value)
    return tuple(result)


def _known_relation_names() -> frozenset[str]:
    values: list[str] = []
    for key in ("data_relations", "operational_relations"):
        raw = ARTIFACT.get(key)
        if not isinstance(raw, tuple) or not all(
            isinstance(value, str) and value for value in raw
        ):
            raise OperationalSemanticRegistryError(
                f"generated {key} registry is malformed"
            )
        values.extend(cast(tuple[str, ...], raw))
    values.append("schema_epoch_control")
    return frozenset(values)


def validate_builtin_operational_manifest() -> None:
    """Validate exact IDs, lifecycles, versions, and executable bindings."""

    known_relations = _known_relation_names()
    actual: list[tuple[str, str, str, str]] = []
    for outer in _operational_obligations():
        obligation_id = outer.get("id")
        contract = outer.get("contract")
        if not isinstance(obligation_id, str) or not isinstance(contract, Mapping):
            raise OperationalSemanticRegistryError(
                "generated operational semantic obligation is malformed"
            )
        relations = contract.get("relations")
        if (
            set(outer) != {"contract", "id", "scope", "source", "version"}
            or set(contract) != _CONTRACT_KEYS
            or outer.get("version") != 1
            or contract.get("id") != obligation_id
            or contract.get("version") != 1
            or outer.get("scope") != contract.get("scope")
            or contract.get("hook")
            != "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider.semantic_validators"
            or contract.get("writer_hook_version") != 1
            or not isinstance(contract.get("scope"), str)
            or not contract.get("scope")
            or not isinstance(contract.get("class"), str)
            or not contract.get("class")
            or not isinstance(contract.get("description"), str)
            or not contract.get("description")
            or not isinstance(relations, list)
            or not relations
            or not all(
                isinstance(relation, str) and relation in known_relations
                for relation in relations
            )
        ):
            raise OperationalSemanticRegistryError(
                f"generated operational obligation {obligation_id!r} binding drifts"
            )
        lifecycle = contract.get("lifecycle")
        ready_check = contract.get("ready_check")
        writer_hook = contract.get("writer_hook")
        if (
            not isinstance(lifecycle, str)
            or not isinstance(ready_check, str)
            or contract.get("check") != ready_check
            or not isinstance(writer_hook, str)
        ):
            raise OperationalSemanticRegistryError(
                f"generated operational obligation {obligation_id!r} lacks an "
                "exact executable binding"
            )
        actual.append((obligation_id, lifecycle, ready_check, writer_hook))
    if tuple(actual) != _SPECS:
        raise OperationalSemanticRegistryError(
            "generated operational semantic manifest differs from the wheel registry"
        )

    expected_checks = {value[2].rsplit(".", 1)[1] for value in _SPECS}
    for name in expected_checks:
        if not callable(globals().get(name)):
            raise OperationalSemanticRegistryError(
                f"installed operational validator {name!r} is missing"
            )


def builtin_operational_semantic_validators() -> Mapping[str, SemanticValidator]:
    """Return the exact provider-eligible bounded validator mapping."""

    validate_builtin_operational_manifest()
    result: dict[str, SemanticValidator] = {}
    for obligation_id, lifecycle, ready_check, _writer_hook in _SPECS:
        if lifecycle == "building_only":
            continue
        validator = globals().get(ready_check.rsplit(".", 1)[1])
        if not callable(validator):
            raise OperationalSemanticRegistryError(
                f"installed operational validator for {obligation_id!r} is missing"
            )
        result[obligation_id] = validator
    return MappingProxyType(result)


def _payload(backend: Backend) -> Mapping[str, Any]:
    backends = ARTIFACT.get("backends")
    if not isinstance(backends, Mapping):
        raise OperationalSemanticRegistryError("generated backend registry is absent")
    value = backends.get(backend)
    if not isinstance(value, Mapping):
        raise OperationalSemanticRegistryError(
            f"generated {backend} backend payload is absent"
        )
    return value


def _manifest_sha256(backend: Backend) -> bytes:
    payload = _payload(backend)
    values = (
        payload.get("ddl_manifest_sha256"),
        payload.get("seed_manifest_sha256"),
        ARTIFACT.get("obligation_manifest_sha256"),
    )
    if not all(isinstance(value, str) and len(value) == 64 for value in values):
        raise OperationalSemanticRegistryError(
            f"generated {backend} epoch manifest inputs are malformed"
        )
    ddl_manifest, seed_manifest, obligation_manifest = cast(
        tuple[str, str, str], values
    )
    digest = hashlib.sha256()
    digest.update(b"h2hdb-schema-epoch-manifest-v2\0")
    digest.update(ddl_manifest.encode("ascii"))
    digest.update(b"\0")
    digest.update(seed_manifest.encode("ascii"))
    digest.update(b"\0")
    digest.update(obligation_manifest.encode("ascii"))
    return digest.digest()


def _fetch_all(
    connector: SQLConnector,
    label: str,
    query: str,
    data: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]:
    try:
        return connector.fetch_all(query, data)
    except OperationalSemanticValidationError:
        raise
    except Exception as error:
        raise OperationalSemanticValidationError(
            f"operational READY check {label} is unreadable"
        ) from error


def _fetch_one(
    connector: SQLConnector,
    label: str,
    query: str,
    data: tuple[Any, ...] = (),
) -> tuple[Any, ...]:
    try:
        return connector.fetch_one(query, data)
    except OperationalSemanticValidationError:
        raise
    except Exception as error:
        raise OperationalSemanticValidationError(
            f"operational READY check {label} is unreadable"
        ) from error


def _as_int(
    value: object,
    *,
    label: str,
    minimum: int = 0,
    maximum: int = _INT63_MAX,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum <= _INT63_MAX
    ):
        raise OperationalSemanticValidationError(
            f"operational READY {label} is outside portable int63"
        )
    return value


def _as_bytes(value: object, *, label: str, length: int | None = None) -> bytes:
    if isinstance(value, memoryview):
        result = value.tobytes()
    elif isinstance(value, bytearray):
        result = bytes(value)
    elif isinstance(value, bytes):
        result = value
    else:
        raise OperationalSemanticValidationError(
            f"operational READY {label} is not binary"
        )
    if length is not None and len(result) != length:
        raise OperationalSemanticValidationError(
            f"operational READY {label} is not {length} bytes"
        )
    return result


def _as_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise OperationalSemanticValidationError(
            f"operational READY {label} is not text"
        )
    return value


def _epoch_context(connector: SQLConnector) -> tuple[Backend, tuple[Any, ...]]:
    rows = _fetch_all(
        connector,
        "epoch manifest",
        """
        SELECT epoch, schema_version, state, manifest_sha256, started_at, ready_at
        FROM h2hdb_schema_epoch
        WHERE singleton_id = 1
        LIMIT 2
        """,
    )
    if len(rows) != 1 or len(rows[0]) != 6:
        raise OperationalSemanticValidationError(
            "operational READY requires exactly one epoch control row"
        )
    row = rows[0]
    epoch, schema_version, state, manifest, started_at, ready_at = row
    started = _as_int(started_at, label="epoch.started_at")
    if (
        _as_int(epoch, label="epoch.epoch") != ARTIFACT.get("epoch")
        or _as_int(schema_version, label="epoch.schema_version")
        != ARTIFACT.get("schema_version")
        or state not in {"BUILDING", "READY"}
    ):
        raise OperationalSemanticValidationError(
            "operational epoch control values are invalid"
        )
    if state == "BUILDING":
        if ready_at is not None:
            raise OperationalSemanticValidationError(
                "operational BUILDING epoch has ready_at"
            )
    elif _as_int(ready_at, label="epoch.ready_at") < started:
        raise OperationalSemanticValidationError(
            "operational READY epoch predates its start"
        )
    manifest_bytes = _as_bytes(manifest, label="epoch.manifest", length=32)
    candidates: tuple[Backend, ...] = ("sqlite", "mariadb")
    matches = tuple(
        backend for backend in candidates if manifest_bytes == _manifest_sha256(backend)
    )
    if len(matches) != 1:
        raise OperationalSemanticValidationError(
            "operational epoch marker does not bind one generated backend manifest"
        )
    return matches[0], row


def _relation(backend: Backend, name: str) -> Mapping[str, Any]:
    if name == "schema_epoch_control":
        value = _payload(backend).get("epoch_control")
        if isinstance(value, Mapping):
            return value
    raw = _payload(backend).get("relations")
    if not isinstance(raw, tuple):
        raise OperationalSemanticRegistryError(
            f"generated {backend} relation registry is malformed"
        )
    matches = tuple(
        value
        for value in raw
        if isinstance(value, Mapping) and value.get("relation") == name
    )
    if len(matches) != 1:
        raise OperationalSemanticRegistryError(
            f"generated relation {name!r} is not singular"
        )
    return matches[0]


def _table(backend: Backend, relation_name: str) -> str:
    table = _relation(backend, relation_name).get("table")
    if (
        not isinstance(table, str)
        or not table
        or not table.replace("_", "").isalnum()
        or table.lower() != table
    ):
        raise OperationalSemanticRegistryError(
            f"generated table for {relation_name!r} is unsafe"
        )
    return table


def _obligation_contract(obligation_id: str) -> Mapping[str, Any]:
    matches = tuple(
        value.get("contract")
        for value in _operational_obligations()
        if value.get("id") == obligation_id
    )
    if len(matches) != 1 or not isinstance(matches[0], Mapping):
        raise OperationalSemanticRegistryError(
            f"generated obligation {obligation_id!r} is not singular"
        )
    return matches[0]


def _validate_static_relations(backend: Backend, obligation_id: str) -> None:
    relations = _obligation_contract(obligation_id).get("relations")
    if not isinstance(relations, list) or not relations:
        raise OperationalSemanticRegistryError(
            f"generated obligation {obligation_id!r} has no relations"
        )
    for relation_name in relations:
        if not isinstance(relation_name, str):
            raise OperationalSemanticRegistryError(
                f"generated obligation {obligation_id!r} relation is malformed"
            )
        relation = _relation(backend, relation_name)
        if (
            not isinstance(relation.get("table"), str)
            or not isinstance(relation.get("columns"), tuple)
            or not isinstance(relation.get("primary_key"), tuple)
            or not isinstance(relation.get("checks"), tuple)
        ):
            raise OperationalSemanticRegistryError(
                f"generated relation {relation_name!r} metadata is malformed"
            )


def _ready_context(connector: SQLConnector, obligation_id: str) -> Backend:
    backend, _row = _epoch_context(connector)
    _validate_static_relations(backend, obligation_id)
    return backend


def _exact_lookup(
    connector: SQLConnector,
    *,
    label: str,
    table: str,
    columns: str,
    key_column: str,
    key: object,
) -> tuple[Any, ...] | None:
    rows = _fetch_all(
        connector,
        label,
        f"SELECT {columns} FROM {table} WHERE {key_column} = %s LIMIT 2",
        (key,),
    )
    if len(rows) > 1:
        raise OperationalSemanticValidationError(
            f"operational READY {label} is not singular"
        )
    return rows[0] if rows else None


def _seed_record(backend: Backend, relation_name: str) -> Mapping[str, Any]:
    records = _payload(backend).get("bootstrap_seeded_relations")
    if not isinstance(records, tuple):
        raise OperationalSemanticRegistryError(
            f"generated {backend} seeded-relation registry is malformed"
        )
    matches = tuple(
        value
        for value in records
        if isinstance(value, Mapping) and value.get("relation") == relation_name
    )
    if len(matches) != 1:
        raise OperationalSemanticRegistryError(
            f"generated seeded relation {relation_name!r} is not singular"
        )
    return matches[0]


def _exact_seeded_relation(
    connector: SQLConnector, backend: Backend, relation_name: str
) -> None:
    record = _seed_record(backend, relation_name)
    expected = record.get("expected_rows")
    query = record.get("validation_sql")
    if not isinstance(expected, tuple) or not isinstance(query, str):
        raise OperationalSemanticRegistryError(
            f"generated seeded relation {relation_name!r} is malformed"
        )
    rows = _fetch_all(
        connector,
        f"{relation_name} exact registry",
        f"{query} LIMIT {len(expected) + 1}",
    )
    if Counter(tuple(row) for row in rows) != Counter(expected):
        raise OperationalSemanticValidationError(
            f"operational READY registry {relation_name!r} differs from its "
            "generated rows"
        )


def _require_key_shape(
    backend: Backend,
    relation_name: str,
    *,
    primary_key: tuple[str, ...],
    unique_keys: tuple[tuple[str, ...], ...] | None = None,
) -> None:
    relation = _relation(backend, relation_name)
    if relation.get("primary_key") != primary_key:
        raise OperationalSemanticRegistryError(
            f"generated relation {relation_name!r} primary key drifts"
        )
    if unique_keys is not None and relation.get("unique_keys") != unique_keys:
        raise OperationalSemanticRegistryError(
            f"generated relation {relation_name!r} unique keys drift"
        )


def check_physical_domains_v1(connector: SQLConnector) -> None:
    """Bind generated physical-domain metadata without rescanning stored rows."""

    _ready_context(connector, "h2hdb.operational.physical-domains.v1")


def check_epoch_manifest_v1(connector: SQLConnector) -> None:
    """Validate the epoch singleton against exactly one generated manifest."""

    _ready_context(connector, "h2hdb.operational.epoch-manifest.v1")


def check_fencing_contract_v1(connector: SQLConnector) -> None:
    """Validate only the current ingest projection through exact key lookups."""

    obligation_id = "h2hdb.operational.fencing.v1"
    backend = _ready_context(connector, obligation_id)
    head_table = _table(backend, "ingest_coordination_head")
    generation_table = _table(backend, "ingest_generation")
    owner_table = _table(backend, "ingest_generation_owner")
    handoff_table = _table(backend, "ingest_generation_handoff")

    head = _exact_lookup(
        connector,
        label="current ingest head",
        table=head_table,
        columns="current_generation, completed_generation, phase",
        key_column="singleton_id",
        key=1,
    )
    if head is None:
        return
    if len(head) != 3:
        raise OperationalSemanticValidationError(
            "operational READY current ingest head shape is invalid"
        )
    current = _as_int(head[0], label="ingest.current_generation")
    completed = _as_int(head[1], label="ingest.completed_generation")
    phase = _as_text(head[2], label="ingest.phase")
    if completed > current or phase not in {
        "READY",
        "INGEST_REQUESTED",
        "DOWNLOADING",
        "INGESTING",
    }:
        raise OperationalSemanticValidationError(
            "operational READY current ingest head values are invalid"
        )

    current_row = _exact_lookup(
        connector,
        label="current ingest generation",
        table=generation_table,
        columns="generation, completed_at",
        key_column="generation",
        key=current,
    )
    completed_row = _exact_lookup(
        connector,
        label="completed ingest generation",
        table=generation_table,
        columns="generation, completed_at",
        key_column="generation",
        key=completed,
    )
    owner = _exact_lookup(
        connector,
        label="current ingest owner",
        table=owner_table,
        columns="owner_token, claimed_at, lease_expires_at",
        key_column="generation",
        key=current,
    )
    handoff = _exact_lookup(
        connector,
        label="current ingest handoff",
        table=handoff_table,
        columns="requested_at",
        key_column="generation",
        key=current,
    )

    if current_row is None or completed_row is None:
        raise OperationalSemanticValidationError(
            "operational READY ingest head lacks an exact generation row"
        )
    if _as_int(current_row[0], label="current generation key") != current:
        raise OperationalSemanticValidationError(
            "operational READY current generation lookup disagrees"
        )
    if (
        _as_int(completed_row[0], label="completed generation key") != completed
        or completed_row[1] is None
    ):
        raise OperationalSemanticValidationError(
            "operational READY completed generation is not complete"
        )
    _as_int(completed_row[1], label="completed generation timestamp")

    if phase == "READY":
        if (
            current != completed
            or current_row[1] is None
            or owner is not None
            or handoff is not None
        ):
            raise OperationalSemanticValidationError(
                "operational READY ingest phase has live resume authority"
            )
        _as_int(current_row[1], label="current generation completion")
        return

    if (
        phase == "INGEST_REQUESTED"
        and current == completed == 0
        and owner is None
        and handoff is None
    ):
        return

    if owner is None:
        raise OperationalSemanticValidationError(
            "operational READY active ingest generation lacks an exact owner"
        )
    if len(owner) != 3:
        raise OperationalSemanticValidationError(
            "operational READY active ingest owner shape is invalid"
        )
    _as_bytes(owner[0], label="current ingest owner token", length=16)
    _as_int(owner[1], label="current ingest owner claimed_at")
    _as_int(owner[2], label="current ingest owner lease_expires_at")

    if phase == "INGEST_REQUESTED":
        if current <= completed or current_row[1] is not None or handoff is None:
            raise OperationalSemanticValidationError(
                "operational READY requested ingest handoff shape is invalid"
            )
        _as_int(handoff[0], label="current ingest handoff requested_at")
        return

    if phase == "DOWNLOADING" and (
        current <= completed or current_row[1] is not None or handoff is not None
    ):
        raise OperationalSemanticValidationError(
            "operational READY downloading generation shape is invalid"
        )
    if handoff is not None:
        _as_int(handoff[0], label="current ingest handoff requested_at")


def check_download_ingest_handoff_contract_v1(connector: SQLConnector) -> None:
    """Validate only the exact current normalized download/ingest projection."""

    obligation_id = "h2hdb.operational.download-ingest-handoff.v1"
    backend = _ready_context(connector, obligation_id)
    head_table = _table(backend, "download_coordination_head")
    generation_table = _table(backend, "download_generation")
    owner_table = _table(backend, "download_generation_owner")
    handoff_table = _table(backend, "download_ingest_handoff")
    consumption_table = _table(backend, "download_ingest_consumption")
    completion_table = _table(backend, "coordinated_ingest_completion")

    head = _exact_lookup(
        connector,
        label="current download head",
        table=head_table,
        columns="current_generation, completed_generation",
        key_column="singleton_id",
        key=1,
    )
    if head is None:
        return
    current = _as_int(head[0], label="download.current_generation")
    completed = _as_int(head[1], label="download.completed_generation")
    if completed > current:
        raise OperationalSemanticValidationError(
            "operational READY completed download generation is ahead"
        )

    current_row = _exact_lookup(
        connector,
        label="current download generation",
        table=generation_table,
        columns="started_at, completed_at",
        key_column="generation",
        key=current,
    )
    completed_row = _exact_lookup(
        connector,
        label="completed download generation",
        table=generation_table,
        columns="completed_at",
        key_column="generation",
        key=completed,
    )
    owner = _exact_lookup(
        connector,
        label="current download owner",
        table=owner_table,
        columns="owner_token, claimed_at, lease_expires_at",
        key_column="generation",
        key=current,
    )
    handoff = _exact_lookup(
        connector,
        label="current download handoff",
        table=handoff_table,
        columns="owner_token, handoff_kind, requested_at",
        key_column="download_generation",
        key=current,
    )
    consumption = _exact_lookup(
        connector,
        label="current download consumption",
        table=consumption_table,
        columns="ingest_generation, consumed_at",
        key_column="download_generation",
        key=current,
    )
    if current_row is None or completed_row is None or completed_row[0] is None:
        raise OperationalSemanticValidationError(
            "operational READY download head lacks exact generation history"
        )
    _as_int(current_row[0], label="current download started_at")
    _as_int(completed_row[0], label="completed download completed_at")

    if owner is not None:
        if len(owner) != 3:
            raise OperationalSemanticValidationError(
                "operational READY live download owner shape is invalid"
            )
        _as_bytes(owner[0], label="current download owner token", length=16)
        _as_int(owner[1], label="current download owner claimed_at")
        _as_int(owner[2], label="current download owner lease_expires_at")
        if (
            current <= completed
            or current_row[1] is not None
            or handoff is not None
            or consumption is not None
        ):
            raise OperationalSemanticValidationError(
                "operational READY live download authority has invalid history"
            )
        return

    if handoff is not None:
        _as_bytes(handoff[0], label="download handoff owner token", length=16)
        kind = _as_text(handoff[1], label="download handoff kind")
        if kind not in {"DOWNLOADER", "EXPIRED_TAKEOVER"}:
            raise OperationalSemanticValidationError(
                "operational READY download handoff kind is invalid"
            )
        _as_int(handoff[2], label="download handoff requested_at")

    if current > completed:
        if current_row[1] is not None or handoff is None:
            raise OperationalSemanticValidationError(
                "operational READY pending download lacks transferred authority"
            )
        if consumption is None:
            return
        ingest_generation = _as_int(
            consumption[0], label="download consumption ingest generation"
        )
        _as_int(consumption[1], label="download consumption consumed_at")
        completion = _exact_lookup(
            connector,
            label="pending linked ingest completion",
            table=completion_table,
            columns="owner_token, completed_at",
            key_column="ingest_generation",
            key=ingest_generation,
        )
        if completion is not None:
            raise OperationalSemanticValidationError(
                "operational READY linked completion did not advance download head"
            )
        return

    if current_row[1] is None:
        raise OperationalSemanticValidationError(
            "operational READY quiescent download generation is incomplete"
        )
    _as_int(current_row[1], label="current download completed_at")
    if handoff is None:
        if consumption is not None:
            raise OperationalSemanticValidationError(
                "operational READY download consumption lacks handoff history"
            )
        return
    if consumption is None:
        raise OperationalSemanticValidationError(
            "operational READY completed linked download lacks consumption"
        )
    ingest_generation = _as_int(
        consumption[0], label="completed download ingest generation"
    )
    _as_int(consumption[1], label="completed download consumed_at")
    completion = _exact_lookup(
        connector,
        label="completed linked ingest receipt",
        table=completion_table,
        columns="owner_token, completed_at",
        key_column="ingest_generation",
        key=ingest_generation,
    )
    if completion is None:
        raise OperationalSemanticValidationError(
            "operational READY completed linked download lacks completion receipt"
        )
    _as_bytes(completion[0], label="coordinated completion owner token", length=16)
    completed_at = _as_int(completion[1], label="coordinated completion completed_at")
    if completed_at != _as_int(current_row[1], label="current download completed_at"):
        raise OperationalSemanticValidationError(
            "operational READY linked completion timestamps disagree"
        )


def _maintenance_owner_index_is_bounded(backend: Backend) -> None:
    relation = _relation(backend, "maintenance_gate_owner")
    indexes = relation.get("indexes")
    if not isinstance(indexes, tuple) or not any(
        isinstance(index, tuple)
        and len(index) == 3
        and index[1] == ("gate_generation",)
        for index in indexes
    ):
        raise OperationalSemanticRegistryError(
            "generated maintenance owner lookup lacks its generation index"
        )


def check_maintenance_gate_contract_v1(connector: SQLConnector) -> None:
    """Validate the current gate and its physically bounded 64 holder slots."""

    obligation_id = "h2hdb.operational.maintenance-gate.v1"
    backend = _ready_context(connector, obligation_id)
    _maintenance_owner_index_is_bounded(backend)
    head_table = _table(backend, "maintenance_gate_head")
    generation_table = _table(backend, "maintenance_gate_generation")
    owner_table = _table(backend, "maintenance_gate_owner")
    holder_table = _table(backend, "maintenance_gate_holder")

    head = _exact_lookup(
        connector,
        label="current maintenance head",
        table=head_table,
        columns="gate_generation",
        key_column="singleton_id",
        key=1,
    )
    if head is None:
        return
    generation = _as_int(head[0], label="maintenance gate generation")
    generation_row = _exact_lookup(
        connector,
        label="current maintenance generation",
        table=generation_table,
        columns="mode, created_at",
        key_column="gate_generation",
        key=generation,
    )
    if generation_row is None:
        raise OperationalSemanticValidationError(
            "operational READY maintenance head lacks its generation"
        )
    mode = _as_text(generation_row[0], label="maintenance mode")
    _as_int(generation_row[1], label="maintenance created_at")
    if mode not in {"SHARED", "EXCLUSIVE"}:
        raise OperationalSemanticValidationError(
            "operational READY maintenance mode is invalid"
        )

    owners = _fetch_all(
        connector,
        "current maintenance owners",
        f"""
        SELECT owner_token, lease_expires_at
        FROM {owner_table}
        WHERE gate_generation = %s
        ORDER BY owner_token
        LIMIT 65
        """,
        (generation,),
    )
    if len(owners) > 64:
        raise OperationalSemanticValidationError(
            "operational READY current gate has more than 64 owners"
        )
    owner_tokens: list[bytes] = []
    for owner_token, lease_expires_at in owners:
        owner_tokens.append(
            _as_bytes(owner_token, label="maintenance owner token", length=16)
        )
        _as_int(lease_expires_at, label="maintenance owner lease expiry")
    if len(set(owner_tokens)) != len(owner_tokens):
        raise OperationalSemanticValidationError(
            "operational READY current gate owner is duplicated"
        )

    holders = _fetch_all(
        connector,
        "maintenance holder slots",
        f"""
        SELECT slot, owner_token
        FROM {holder_table}
        ORDER BY slot
        LIMIT 65
        """,
    )
    if len(holders) > 64:
        raise OperationalSemanticValidationError(
            "operational READY maintenance gate exceeds 64 slots"
        )
    slots_by_owner: Counter[bytes] = Counter()
    ordered_slots: list[tuple[int, bytes]] = []
    for slot_value, token_value in holders:
        slot = _as_int(slot_value, label="maintenance slot")
        token = _as_bytes(token_value, label="maintenance holder", length=16)
        if slot >= 64:
            raise OperationalSemanticValidationError(
                "operational READY maintenance slot lies outside 0..63"
            )
        slots_by_owner[token] += 1
        ordered_slots.append((slot, token))

    if not owner_tokens:
        if ordered_slots:
            raise OperationalSemanticValidationError(
                "operational READY idle maintenance gate has orphan holder slots"
            )
        # Releasing the final owner intentionally leaves the immutable current
        # generation/head audit in place.  Either mode can therefore be idle
        # with no owner or holder until the next claim advances the generation.
        return

    if mode == "SHARED":
        if any(slots_by_owner[token] != 1 for token in owner_tokens):
            raise OperationalSemanticValidationError(
                "operational READY each current SHARED owner must hold exactly one slot"
            )
        return

    if len(owner_tokens) != 1:
        raise OperationalSemanticValidationError(
            "operational READY EXCLUSIVE gate must have exactly one current owner"
        )
    exclusive_owner = owner_tokens[0]
    expected = [(slot, exclusive_owner) for slot in range(64)]
    if ordered_slots != expected:
        raise OperationalSemanticValidationError(
            "operational READY EXCLUSIVE owner must hold exactly slots 0..63"
        )


@dataclass(frozen=True)
class _CleanupJob:
    cleanup_id: bytes
    target_key: bytes
    target_kind: str
    shard_no: int
    cycle_generation: int
    max_rows_per_transaction: int
    frozen_root_count: int
    frozen_root_set_sha256: bytes
    state: str


def _cleanup_layout(
    backend: Backend,
) -> tuple[
    tuple[tuple[str, int, bytes], ...],
    Mapping[str, tuple[tuple[str, int], ...]],
]:
    sweep_record = _seed_record(backend, "cleanup_sweep_target")
    phase_record = _seed_record(backend, "cleanup_phase")
    sweep_raw = sweep_record.get("expected_rows")
    phase_raw = phase_record.get("expected_rows")
    if not isinstance(sweep_raw, tuple) or not isinstance(phase_raw, tuple):
        raise OperationalSemanticRegistryError(
            "generated cleanup fixed layout is malformed"
        )
    sweep: list[tuple[str, int, bytes]] = []
    for row in sweep_raw:
        if not isinstance(row, tuple) or len(row) != 3:
            raise OperationalSemanticRegistryError(
                "generated cleanup sweep target is malformed"
            )
        target_kind, shard_no, target_key = row
        if (
            not isinstance(target_kind, str)
            or not isinstance(shard_no, int)
            or isinstance(shard_no, bool)
            or not 0 <= shard_no <= 255
            or not isinstance(target_key, bytes)
            or len(target_key) != 32
        ):
            raise OperationalSemanticRegistryError(
                "generated cleanup sweep target has invalid values"
            )
        sweep.append((target_kind, shard_no, target_key))

    phases: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)
    for row in phase_raw:
        if not isinstance(row, tuple) or len(row) != 3:
            raise OperationalSemanticRegistryError(
                "generated cleanup phase is malformed"
            )
        phase, target_kind, phase_order = row
        if (
            not isinstance(phase, str)
            or not isinstance(target_kind, str)
            or not isinstance(phase_order, int)
            or isinstance(phase_order, bool)
            or phase_order < 1
        ):
            raise OperationalSemanticRegistryError(
                "generated cleanup phase has invalid values"
            )
        phases[target_kind].append((phase, phase_order))
    normalized = {
        kind: tuple(sorted(values, key=lambda value: value[1]))
        for kind, values in phases.items()
    }
    if any(
        tuple(order for _phase, order in values) != tuple(range(1, len(values) + 1))
        for values in normalized.values()
    ):
        raise OperationalSemanticRegistryError(
            "generated cleanup phase order is not contiguous"
        )
    if any(kind not in normalized for kind, _shard, _key in sweep):
        raise OperationalSemanticRegistryError(
            "generated cleanup sweep target lacks phases"
        )
    for target_kind, required_tail in _CLEANUP_STATE_ROOT_PHASE_CHAINS.items():
        actual = tuple(phase for phase, _order in normalized.get(target_kind, ()))
        if actual[-len(required_tail) :] != required_tail:
            raise OperationalSemanticRegistryError(
                f"generated {target_kind} cleanup state-to-root phase chain drifts"
            )
    return tuple(sweep), MappingProxyType(normalized)


def _cleanup_id(target_kind: str, shard_no: int, cycle_generation: int) -> bytes:
    if not 0 <= shard_no <= 255 or not 1 <= cycle_generation <= _INT63_MAX:
        raise OperationalSemanticValidationError(
            "operational READY cleanup identity inputs are invalid"
        )
    tag = hashlib.sha256(_CLEANUP_ID_DOMAIN + target_kind.encode("ascii")).digest()[:7]
    return tag + bytes((shard_no,)) + cycle_generation.to_bytes(8, "big")


def _cleanup_jobs(
    connector: SQLConnector,
    backend: Backend,
    sweep: tuple[tuple[str, int, bytes], ...],
) -> dict[bytes, _CleanupJob]:
    job_table = _table(backend, "cleanup_job")
    sweep_table = _table(backend, "cleanup_sweep_target")
    completion_table = _table(backend, "cleanup_completion")
    rows = _fetch_all(
        connector,
        "fixed cleanup jobs",
        f"""
        SELECT j.cleanup_id, j.target_key, j.cycle_generation,
               j.algorithm_version, j.max_rows_per_transaction,
               j.frozen_root_count, j.frozen_root_set_sha256, j.state,
               j.created_at, j.completed_at,
               s.target_kind, s.shard_no,
               c.target_key, c.cycle_generation,
               c.final_chain_sha256, c.deleted_count
        FROM {job_table} AS j
        LEFT JOIN {sweep_table} AS s ON s.target_key = j.target_key
        LEFT JOIN {completion_table} AS c ON c.target_key = j.target_key
        ORDER BY j.target_key
        LIMIT {len(sweep) + 1}
        """,
    )
    if len(rows) > len(sweep):
        raise OperationalSemanticValidationError(
            "operational READY cleanup job cardinality exceeds fixed shards"
        )

    expected_targets = {target_key for _kind, _shard, target_key in sweep}
    result: dict[bytes, _CleanupJob] = {}
    for row in rows:
        (
            cleanup_id_value,
            target_key_value,
            cycle_generation_value,
            algorithm_version,
            max_rows,
            frozen_root_count_value,
            frozen_root_set_sha256_value,
            state_value,
            created_at,
            completed_at,
            target_kind_value,
            shard_no_value,
            completion_target_key,
            completion_generation,
            completion_chain,
            completion_deleted,
        ) = row
        cleanup_id = _as_bytes(cleanup_id_value, label="cleanup_id", length=16)
        target_key = _as_bytes(target_key_value, label="cleanup target key", length=32)
        if target_key not in expected_targets:
            raise OperationalSemanticValidationError(
                "operational READY cleanup job has no fixed sweep target"
            )
        target_kind = _as_text(target_kind_value, label="cleanup target kind")
        shard_no = _as_int(shard_no_value, label="cleanup shard")
        cycle_generation = _as_int(
            cycle_generation_value,
            label="cleanup cycle generation",
            minimum=1,
        )
        if (
            _as_int(algorithm_version, label="cleanup algorithm version", minimum=1)
            != 2
        ):
            raise OperationalSemanticValidationError(
                "operational READY cleanup algorithm version is unsupported"
            )
        maximum_rows = _as_int(max_rows, label="cleanup max rows", minimum=1)
        if maximum_rows > 256:
            raise OperationalSemanticValidationError(
                "operational READY cleanup max rows exceeds 256"
            )
        frozen_root_count = _as_int(
            frozen_root_count_value,
            label="cleanup frozen root count",
        )
        if frozen_root_count > maximum_rows:
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen roots exceed its policy"
            )
        frozen_root_set_sha256 = _as_bytes(
            frozen_root_set_sha256_value,
            label="cleanup frozen root set digest",
            length=32,
        )
        _as_int(created_at, label="cleanup created_at")
        state = _as_text(state_value, label="cleanup state")
        if cleanup_id != _cleanup_id(target_kind, shard_no, cycle_generation):
            raise OperationalSemanticValidationError(
                "operational READY cleanup_id does not match its fixed shard cycle"
            )
        if cleanup_id in result:
            raise OperationalSemanticValidationError(
                "operational READY cleanup_id is duplicated"
            )

        has_completion = completion_target_key is not None
        if state == "OPEN":
            if completed_at is not None or has_completion:
                raise OperationalSemanticValidationError(
                    "operational READY OPEN cleanup has terminal authority"
                )
        elif state == "COMPLETE":
            if completed_at is None or not has_completion:
                raise OperationalSemanticValidationError(
                    "operational READY COMPLETE cleanup lacks replay authority"
                )
            _as_int(completed_at, label="cleanup completed_at")
            if (
                _as_bytes(
                    completion_target_key,
                    label="cleanup completion target key",
                    length=32,
                )
                != target_key
            ):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup completion target disagrees"
                )
            if (
                _as_int(
                    completion_generation,
                    label="cleanup completion generation",
                    minimum=1,
                )
                != cycle_generation
            ):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup completion generation is stale"
                )
            _as_bytes(completion_chain, label="cleanup completion chain", length=32)
            _as_int(completion_deleted, label="cleanup completion deleted count")
        else:
            raise OperationalSemanticValidationError(
                "operational READY cleanup job state is invalid"
            )
        result[cleanup_id] = _CleanupJob(
            cleanup_id=cleanup_id,
            target_key=target_key,
            target_kind=target_kind,
            shard_no=shard_no,
            cycle_generation=cycle_generation,
            max_rows_per_transaction=maximum_rows,
            frozen_root_count=frozen_root_count,
            frozen_root_set_sha256=frozen_root_set_sha256,
            state=state,
        )

    orphan_completion = _fetch_all(
        connector,
        "fixed cleanup completions",
        f"""
        SELECT c.target_key
        FROM {completion_table} AS c
        LEFT JOIN {job_table} AS j ON j.target_key = c.target_key
        WHERE j.cleanup_id IS NULL
        LIMIT {len(sweep) + 1}
        """,
    )
    if orphan_completion:
        raise OperationalSemanticValidationError(
            "operational READY cleanup completion has no current fixed-shard job"
        )
    return result


def _validate_cleanup_frozen_root_frame(target_kind: str, frame: bytes) -> None:
    shape = _CLEANUP_FROZEN_ROOT_SHAPES.get(target_kind)
    if shape is None:
        raise OperationalSemanticValidationError(
            "operational READY cleanup frozen root kind is unregistered"
        )
    if not 3 <= len(frame) <= 260 or frame[0] != 1 or frame[1] != len(shape):
        raise OperationalSemanticValidationError(
            "operational READY cleanup frozen root frame header is invalid"
        )
    offset = 2
    for tag, maximum in shape:
        if offset >= len(frame) or frame[offset : offset + 1] != tag:
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root scalar tag is invalid"
            )
        offset += 1
        if tag == b"i":
            if offset + 8 > len(frame):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup frozen root integer is truncated"
                )
            if int.from_bytes(frame[offset : offset + 8], "big") > _INT63_MAX:
                raise OperationalSemanticValidationError(
                    "operational READY cleanup frozen root integer exceeds int63"
                )
            offset += 8
            continue
        if offset + 2 > len(frame):
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root bytes are truncated"
            )
        size = int.from_bytes(frame[offset : offset + 2], "big")
        offset += 2
        variable = target_kind == "SOURCE_GALLERY_NAME_GID"
        if (variable and not 1 <= size <= maximum) or (
            not variable and size != maximum
        ):
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root scalar length is invalid"
            )
        if offset + size > len(frame):
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root bytes are truncated"
            )
        offset += size
    if offset != len(frame):
        raise OperationalSemanticValidationError(
            "operational READY cleanup frozen root frame has trailing bytes"
        )


def _cleanup_frozen_root_set_sha256(
    cleanup_id: bytes,
    frames: tuple[bytes, ...],
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_CLEANUP_FROZEN_ROOT_SET_DOMAIN)
    digest.update(cleanup_id)
    digest.update(len(frames).to_bytes(2, "big"))
    for frame in sorted(frames):
        digest.update(len(frame).to_bytes(2, "big"))
        digest.update(frame)
    return digest.digest()


def _cleanup_next_chain_sha256(
    prior_chain_sha256: bytes,
    phase: str,
    generation: int,
    start_cursor: bytes,
    next_cursor: bytes,
    input_sha256: bytes,
    row_count: int,
) -> bytes:
    digest = hashlib.sha256()
    digest.update(_CLEANUP_CHAIN_DOMAIN)
    digest.update(prior_chain_sha256)
    digest.update(phase.encode("ascii"))
    digest.update(b"\0")
    digest.update(generation.to_bytes(8, "big"))
    digest.update(len(start_cursor).to_bytes(4, "big"))
    digest.update(start_cursor)
    digest.update(len(next_cursor).to_bytes(4, "big"))
    digest.update(next_cursor)
    digest.update(input_sha256)
    digest.update(row_count.to_bytes(8, "big"))
    return digest.digest()


def _validate_cleanup_frozen_roots(
    connector: SQLConnector,
    backend: Backend,
    jobs: Mapping[bytes, _CleanupJob],
) -> None:
    relation = _table(backend, "cleanup_cycle_root")
    rows = _fetch_all(
        connector,
        "cleanup frozen roots",
        f"SELECT cleanup_id, frozen_root_key FROM {relation} "
        "ORDER BY cleanup_id, frozen_root_key LIMIT 257",
    )
    if len(rows) > 256:
        raise OperationalSemanticValidationError(
            "operational READY cleanup frozen roots exceed global 256 cap"
        )
    if sum(job.state == "OPEN" for job in jobs.values()) > 1:
        raise OperationalSemanticValidationError(
            "operational READY cleanup has multiple OPEN jobs"
        )
    by_cleanup: defaultdict[bytes, list[bytes]] = defaultdict(list)
    for cleanup_id_value, frozen_root_key_value in rows:
        cleanup_id = _as_bytes(
            cleanup_id_value, label="cleanup frozen root cleanup_id", length=16
        )
        job = jobs.get(cleanup_id)
        if job is None:
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root has no job"
            )
        frame = _as_bytes(
            frozen_root_key_value,
            label="cleanup frozen root key",
        )
        _validate_cleanup_frozen_root_frame(job.target_kind, frame)
        by_cleanup[cleanup_id].append(frame)
    for cleanup_id, job in jobs.items():
        frames = tuple(by_cleanup.get(cleanup_id, ()))
        if job.state == "COMPLETE":
            if frames:
                raise OperationalSemanticValidationError(
                    "operational READY COMPLETE cleanup retains frozen roots"
                )
            continue
        if len(frames) != job.frozen_root_count:
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root count disagrees"
            )
        if len(set(frames)) != len(frames):
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root membership is duplicated"
            )
        if (
            _cleanup_frozen_root_set_sha256(cleanup_id, frames)
            != job.frozen_root_set_sha256
        ):
            raise OperationalSemanticValidationError(
                "operational READY cleanup frozen root digest disagrees"
            )


def _validate_cleanup_checkpoints(
    connector: SQLConnector,
    backend: Backend,
    sweep: tuple[tuple[str, int, bytes], ...],
    phases_by_kind: Mapping[str, tuple[tuple[str, int], ...]],
    jobs: Mapping[bytes, _CleanupJob],
) -> None:
    checkpoint_table = _table(backend, "cleanup_checkpoint")
    receipt_table = _table(backend, "cleanup_batch_receipt")
    max_checkpoints = sum(len(phases_by_kind[kind]) for kind, _shard, _key in sweep)
    rows = _fetch_all(
        connector,
        "fixed cleanup checkpoints",
        f"""
        SELECT p.cleanup_id, p.phase, p.generation, p.cursor_bytes,
               p.deleted_count, p.chain_sha256, p.state,
               r.cleanup_id, r.batch_key, r.start_cursor, r.next_cursor,
               r.prior_chain_sha256, r.prior_deleted_count,
               r.input_sha256, r.output_sha256, r.row_count,
               r.committed_generation, r.committed_at
        FROM {checkpoint_table} AS p
        LEFT JOIN {receipt_table} AS r
          ON r.cleanup_id = p.cleanup_id AND r.phase = p.phase
        ORDER BY p.cleanup_id, p.phase
        LIMIT {max_checkpoints + 1}
        """,
    )
    if len(rows) > max_checkpoints:
        raise OperationalSemanticValidationError(
            "operational READY cleanup checkpoint cardinality exceeds fixed layout"
        )

    states: dict[tuple[bytes, str], str] = {}
    for row in rows:
        (
            cleanup_id_value,
            phase_value,
            generation_value,
            cursor_value,
            deleted_count,
            chain_value,
            state_value,
            receipt_cleanup_id,
            batch_key,
            start_cursor,
            next_cursor,
            prior_chain_sha256,
            prior_deleted_count,
            input_sha256,
            output_sha256,
            row_count_value,
            committed_generation,
            committed_at,
        ) = row
        cleanup_id = _as_bytes(
            cleanup_id_value, label="cleanup checkpoint id", length=16
        )
        phase = _as_text(phase_value, label="cleanup checkpoint phase")
        generation = _as_int(generation_value, label="cleanup checkpoint generation")
        cursor = _as_bytes(cursor_value, label="cleanup checkpoint cursor")
        checkpoint_deleted_count = _as_int(
            deleted_count, label="cleanup checkpoint deleted count"
        )
        checkpoint_chain = _as_bytes(
            chain_value, label="cleanup checkpoint chain", length=32
        )
        state = _as_text(state_value, label="cleanup checkpoint state")
        job = jobs.get(cleanup_id)
        if job is None or job.state != "OPEN":
            raise OperationalSemanticValidationError(
                "operational READY cleanup checkpoint has no OPEN current job"
            )
        phase_order = dict(phases_by_kind[job.target_kind])
        if phase not in phase_order:
            raise OperationalSemanticValidationError(
                "operational READY cleanup checkpoint phase has the wrong target kind"
            )

        has_receipt = receipt_cleanup_id is not None
        if has_receipt:
            if (
                _as_bytes(
                    receipt_cleanup_id,
                    label="cleanup receipt id",
                    length=16,
                )
                != cleanup_id
            ):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup receipt owner disagrees"
                )
            _as_bytes(batch_key, label="cleanup receipt batch key", length=32)
            receipt_start_cursor = _as_bytes(
                start_cursor, label="cleanup receipt start cursor"
            )
            receipt_next_cursor = _as_bytes(
                next_cursor, label="cleanup receipt next cursor"
            )
            receipt_prior_chain = _as_bytes(
                prior_chain_sha256,
                label="cleanup receipt prior chain",
                length=32,
            )
            receipt_prior_deleted = _as_int(
                prior_deleted_count,
                label="cleanup receipt prior deleted count",
            )
            receipt_input = _as_bytes(
                input_sha256, label="cleanup receipt input", length=32
            )
            receipt_output = _as_bytes(
                output_sha256, label="cleanup receipt output", length=32
            )
            row_count = _as_int(row_count_value, label="cleanup receipt row count")
            receipt_generation = _as_int(
                committed_generation, label="cleanup receipt generation"
            )
            _as_int(committed_at, label="cleanup receipt committed_at")
            expected_output = _cleanup_next_chain_sha256(
                receipt_prior_chain,
                phase,
                receipt_generation,
                receipt_start_cursor,
                receipt_next_cursor,
                receipt_input,
                row_count,
            )
            expected_deleted = _as_int(
                receipt_prior_deleted + row_count,
                label="cleanup receipt next deleted count",
            )
            if (
                receipt_next_cursor != cursor
                or expected_output != receipt_output
                or receipt_output != checkpoint_chain
                or receipt_generation != generation
                or row_count > job.max_rows_per_transaction
                or checkpoint_deleted_count != expected_deleted
            ):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup receipt does not match checkpoint poststate"
                )
        else:
            if any(value is not None for value in row[8:]):
                raise OperationalSemanticValidationError(
                    "operational READY cleanup receipt is partially NULL"
                )
            row_count = -1

        terminal_receipt = has_receipt and row_count == 0
        if state not in {"OPEN", "COMPLETE"} or (
            (state == "COMPLETE") != terminal_receipt
        ):
            raise OperationalSemanticValidationError(
                "operational READY cleanup checkpoint terminal receipt equivalence fails"
            )
        if has_receipt and (
            (receipt_start_cursor == receipt_next_cursor) != terminal_receipt
        ):
            raise OperationalSemanticValidationError(
                "operational READY cleanup receipt cursor movement disagrees"
            )
        states[(cleanup_id, phase)] = state

    for (cleanup_id, phase), _state in states.items():
        job = jobs[cleanup_id]
        order = dict(phases_by_kind[job.target_kind])[phase]
        for earlier_phase, earlier_order in phases_by_kind[job.target_kind]:
            if earlier_order >= order:
                break
            if states.get((cleanup_id, earlier_phase)) != "COMPLETE":
                raise OperationalSemanticValidationError(
                    "operational READY cleanup phase bypasses an earlier phase"
                )

    orphan_receipts = _fetch_all(
        connector,
        "fixed cleanup receipt ownership",
        f"""
        SELECT r.cleanup_id, r.phase
        FROM {receipt_table} AS r
        LEFT JOIN {checkpoint_table} AS p
          ON p.cleanup_id = r.cleanup_id AND p.phase = r.phase
        WHERE p.cleanup_id IS NULL
        LIMIT {max_checkpoints + 1}
        """,
    )
    if orphan_receipts:
        raise OperationalSemanticValidationError(
            "operational READY cleanup receipt has no checkpoint"
        )


def _validate_fixed_cleanup_state(connector: SQLConnector, backend: Backend) -> None:
    sweep, phases_by_kind = _cleanup_layout(backend)
    jobs = _cleanup_jobs(connector, backend, sweep)
    _validate_cleanup_frozen_roots(connector, backend, jobs)
    _validate_cleanup_checkpoints(connector, backend, sweep, phases_by_kind, jobs)


def check_bounded_work_contract_v1(connector: SQLConnector) -> None:
    """Validate fixed cleanup cursor authority; preparation is writer-owned."""

    backend = _ready_context(connector, "h2hdb.operational.bounded-work.v1")
    _validate_fixed_cleanup_state(connector, backend)


def check_queue_history_contract_v1(connector: SQLConnector) -> None:
    """Bind queue metadata; immutable history remains a writer obligation."""

    _ready_context(connector, "h2hdb.operational.queue-history.v1")


def check_canonical_hash_cache_contract_v1(connector: SQLConnector) -> None:
    """Bind cache metadata; READY never recomputes canonical corpus hashes."""

    _ready_context(connector, "h2hdb.operational.canonical-hash-cache.v1")


_PCOM_TRANSITION_PHASE_ORDER = MappingProxyType(
    {
        "PCOM_RELEASE_BUILD_BASE": 1,
        "PCOM_PREPARATION_BINDING": 2,
        "PCOM_PREPARATION_BATCH": 3,
        "PCOM_PREPARATION_CHECKPOINT": 4,
        "PCOM_PREPARATION": 5,
        "PCOM_EVENT": 6,
        "PCOM_FINALIZATION_MARKER": 7,
        "PCOM_FINALIZATION_BATCH": 8,
        "PCOM_COMMIT_EFFECT_ROOT": 9,
        "PCOM_FINALIZATION_CHECKPOINT": 10,
        "PCOM_ANCHOR": 11,
    }
)


def _decode_pcom_frozen_authority(value: object) -> tuple[bytes, bytes]:
    frame = _as_bytes(value, label="PCOM frozen root")
    if (
        len(frame) != 40
        or frame[:5] != b"\x01\x02b\x00\x10"
        or frame[21:24] != b"b\x00\x10"
    ):
        raise OperationalSemanticValidationError(
            "operational READY PCOM frozen authority frame is malformed"
        )
    return frame[5:21], frame[24:40]


def _decode_pcom_cursor(
    value: object,
    *,
    integer_tail: bool,
) -> tuple[bytes, bytes, int | bytes]:
    cursor = _as_bytes(value, label="PCOM transition cursor")
    expected_length = 51 if integer_tail else 61
    if (
        len(cursor) != expected_length
        or cursor[:4] != b"\x01\x00\x00\x03"
        or cursor[4:7] != b"b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
    ):
        raise OperationalSemanticValidationError(
            "operational READY PCOM transition cursor is malformed"
        )
    receipt_id = cursor[7:23]
    second = cursor[26:42]
    if integer_tail:
        if cursor[42:43] != b"i":
            raise OperationalSemanticValidationError(
                "operational READY PCOM EVENT cursor has the wrong scalar type"
            )
        tail: int | bytes = int.from_bytes(cursor[43:51], "big")
        if len(cursor) != 51:
            raise OperationalSemanticValidationError(
                "operational READY PCOM EVENT cursor has trailing bytes"
            )
    else:
        if cursor[42:45] != b"b\x00\x10":
            raise OperationalSemanticValidationError(
                "operational READY PCOM compound cursor has the wrong scalar type"
            )
        tail = cursor[45:61]
        if len(cursor) != 61:
            raise OperationalSemanticValidationError(
                "operational READY PCOM compound cursor has trailing bytes"
            )
    return receipt_id, second, tail


def _decode_pcom_post_compound_cursor(value: object) -> bytes:
    cursor = _as_bytes(value, label="PCOM post-compound cursor")
    if (
        len(cursor) != 42
        or cursor[:7] != b"\x01\x00\x00\x02b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
        or cursor[7:23] != cursor[26:42]
    ):
        raise OperationalSemanticValidationError(
            "operational READY PCOM post-compound cursor is malformed"
        )
    return cursor[7:23]


def _pcom_single_row(
    connector: SQLConnector,
    *,
    label: str,
    query: str,
    parameters: tuple[object, ...],
) -> tuple[Any, ...]:
    rows = _fetch_all(connector, label, query, parameters)
    if len(rows) != 1:
        raise OperationalSemanticValidationError(
            f"operational READY {label} must have exactly one row"
        )
    return rows[0]


def _pcom_commit_preparation(
    connector: SQLConnector,
    backend: Backend,
    receipt_id: bytes,
) -> tuple[bytes, int]:
    commit = _table(backend, "publication_commit")
    seal = _table(backend, "operational_preparation_effect_seal")
    stream = _table(backend, "operational_event_stream")
    row = _pcom_single_row(
        connector,
        label="PCOM commit/effect authority",
        query=(
            f"SELECT committed.preparation_id, effect.event_count "
            f"FROM {commit} AS committed "
            f"JOIN {seal} AS effect "
            "ON effect.preparation_id = committed.preparation_id "
            f"JOIN {stream} AS event_stream "
            "ON event_stream.preparation_id = committed.preparation_id "
            "WHERE committed.receipt_id = %s LIMIT 2"
        ),
        parameters=(receipt_id,),
    )
    return (
        _as_bytes(row[0], label="PCOM preparation_id", length=16),
        _as_int(row[1], label="PCOM event_count"),
    )


def _pcom_require_no_events(
    connector: SQLConnector,
    backend: Backend,
    *,
    preparation_id: bytes,
    through_sequence_no: int | None = None,
) -> None:
    event = _table(backend, "operational_event")
    predicate = "preparation_id = %s"
    parameters: tuple[object, ...] = (preparation_id,)
    if through_sequence_no is not None:
        predicate += " AND sequence_no <= %s"
        parameters += (through_sequence_no,)
    rows = _fetch_all(
        connector,
        "PCOM retired event coordinates",
        f"SELECT 1 FROM {event} WHERE {predicate} LIMIT 1",
        parameters,
    )
    if rows:
        raise OperationalSemanticValidationError(
            "operational READY PCOM cursor-covered event coordinate reappeared"
        )


def _pcom_require_no_preparation(
    connector: SQLConnector,
    backend: Backend,
    *,
    preparation_id: bytes,
) -> None:
    preparation = _table(backend, "operational_preparation")
    if _fetch_all(
        connector,
        "PCOM retired preparation",
        f"SELECT 1 FROM {preparation} WHERE preparation_id = %s LIMIT 1",
        (preparation_id,),
    ):
        raise OperationalSemanticValidationError(
            "operational READY PCOM preparation reappeared after its root phase"
        )


def _pcom_require_compound_authority_absent(
    connector: SQLConnector,
    backend: Backend,
    *,
    preparation_id: bytes,
) -> None:
    for relation_name in (
        "publication_candidate_preparation",
        "operational_preparation_batch_receipt",
        "operational_preparation_checkpoint",
        "operational_preparation",
        "operational_event",
        "operational_preparation_effect_seal",
        "operational_event_stream",
    ):
        table = _table(backend, relation_name)
        if _fetch_all(
            connector,
            "PCOM retired effect root",
            f"SELECT 1 FROM {table} WHERE preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise OperationalSemanticValidationError(
                "operational READY PCOM compound-covered authority reappeared"
            )
    event = _table(backend, "operational_event")
    for relation_name in (
        "operational_removed_gid_event",
        "operational_deletion_consumption_event",
    ):
        subtype = _table(backend, relation_name)
        if _fetch_all(
            connector,
            "PCOM compound-covered typed event",
            f"SELECT 1 FROM {subtype} AS subtype "
            f"JOIN {event} AS event ON event.event_id = subtype.event_id "
            "WHERE event.preparation_id = %s LIMIT 1",
            (preparation_id,),
        ):
            raise OperationalSemanticValidationError(
                "operational READY PCOM compound-covered typed event reappeared"
            )


def _pcom_require_post_compound_transition(
    connector: SQLConnector,
    backend: Backend,
    *,
    phase: str,
    cursor: bytes,
    frozen_authorities: tuple[tuple[bytes, bytes], ...],
) -> None:
    if phase not in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}:
        raise OperationalSemanticValidationError(
            "operational READY PCOM post-compound phase is invalid"
        )
    cursor_receipt = _decode_pcom_post_compound_cursor(cursor) if cursor else None
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    if cursor_receipt is not None and cursor_receipt not in frozen_receipts:
        raise OperationalSemanticValidationError(
            "operational READY PCOM post-compound cursor is outside frozen roots"
        )
    marker = _table(backend, "publication_commit_finalization")
    batch = _table(backend, "publication_finalization_batch_receipt_stored")
    checkpoint = _table(backend, "publication_finalization_checkpoint")
    anchor = _table(backend, "publication_commit_anchor")
    source_base = _table(backend, "source_build_base_publication_commit")
    for receipt_id in frozen_receipts:
        if _fetch_all(
            connector,
            "PCOM post-compound source-build base",
            f"SELECT 1 FROM {source_base} WHERE base_receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise OperationalSemanticValidationError(
                "operational READY PCOM source-build base pin reappeared"
            )
        for table, label in (
            (marker, "finalization marker"),
            (batch, "finalization batch"),
        ):
            if _fetch_all(
                connector,
                f"PCOM post-compound {label}",
                f"SELECT 1 FROM {table} WHERE receipt_id = %s LIMIT 1",
                (receipt_id,),
            ):
                raise OperationalSemanticValidationError(
                    f"operational READY PCOM post-compound {label} reappeared"
                )
        checkpoint_rows = _fetch_all(
            connector,
            "PCOM finalization checkpoint transition",
            f"SELECT state FROM {checkpoint} WHERE receipt_id = %s LIMIT 2",
            (receipt_id,),
        )
        anchor_rows = _fetch_all(
            connector,
            "PCOM anchor transition",
            f"SELECT 1 FROM {anchor} WHERE receipt_id = %s LIMIT 2",
            (receipt_id,),
        )
        covered = cursor_receipt is not None and receipt_id <= cursor_receipt
        if phase == "PCOM_FINALIZATION_CHECKPOINT":
            expected_checkpoint = [] if covered else [("COMPLETE",)]
            if checkpoint_rows != expected_checkpoint:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM finalization-checkpoint coverage differs"
                )
            if anchor_rows != [(1,)]:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM anchor disappeared before its phase"
                )
            continue
        if checkpoint_rows:
            raise OperationalSemanticValidationError(
                "operational READY PCOM finalization checkpoint reappeared"
            )
        expected_anchor = [] if covered else [(1,)]
        if anchor_rows != expected_anchor:
            raise OperationalSemanticValidationError(
                "operational READY PCOM anchor coverage differs"
            )


def _validate_operational_event_root_owners(
    connector: SQLConnector,
    backend: Backend,
) -> None:
    preparation = _table(backend, "operational_preparation")
    commit = _table(backend, "publication_commit")
    seal = _table(backend, "operational_preparation_effect_seal")
    stream = _table(backend, "operational_event_stream")
    orphan_seal = _fetch_all(
        connector,
        "orphan operational effect seal",
        f"SELECT 1 FROM {seal} AS effect "
        f"LEFT JOIN {preparation} AS preparation "
        "ON preparation.preparation_id = effect.preparation_id "
        f"LEFT JOIN {commit} AS committed "
        "ON committed.preparation_id = effect.preparation_id "
        "WHERE preparation.preparation_id IS NULL "
        "AND committed.receipt_id IS NULL LIMIT 1",
    )
    if orphan_seal:
        raise OperationalSemanticValidationError(
            "operational READY effect seal lacks preparation or commit authority"
        )
    orphan_stream = _fetch_all(
        connector,
        "orphan operational event stream",
        f"SELECT 1 FROM {stream} AS event_stream "
        f"LEFT JOIN {preparation} AS preparation "
        "ON preparation.preparation_id = event_stream.preparation_id "
        f"LEFT JOIN {seal} AS effect "
        "ON effect.preparation_id = event_stream.preparation_id "
        "WHERE preparation.preparation_id IS NULL "
        "AND effect.preparation_id IS NULL LIMIT 1",
    )
    if orphan_stream:
        raise OperationalSemanticValidationError(
            "operational READY event stream lacks preparation or seal authority"
        )


def _validate_open_pcom_event_transition(
    connector: SQLConnector,
    backend: Backend,
) -> None:
    job = _table(backend, "cleanup_job")
    sweep = _table(backend, "cleanup_sweep_target")
    checkpoint = _table(backend, "cleanup_checkpoint")
    phase_registry = _table(backend, "cleanup_phase")
    root = _table(backend, "cleanup_cycle_root")
    receipt = _table(backend, "cleanup_batch_receipt")
    jobs = _fetch_all(
        connector,
        "OPEN PCOM job",
        f"SELECT current.cleanup_id, current.frozen_root_count "
        f"FROM {job} AS current "
        f"JOIN {sweep} AS target ON target.target_key = current.target_key "
        "WHERE current.state = 'OPEN' "
        "AND target.target_kind = 'PUBLICATION_COMMIT' LIMIT 2",
    )
    if not jobs:
        return
    if len(jobs) != 1:
        raise OperationalSemanticValidationError(
            "operational READY has multiple OPEN PCOM jobs"
        )
    cleanup_id = _as_bytes(jobs[0][0], label="OPEN PCOM cleanup_id", length=16)
    frozen_count = _as_int(jobs[0][1], label="OPEN PCOM frozen root count")
    if frozen_count > 256:
        raise OperationalSemanticValidationError(
            "operational READY OPEN PCOM roots exceed the hard cap"
        )
    root_rows = _fetch_all(
        connector,
        "OPEN PCOM frozen roots",
        f"SELECT frozen_root_key FROM {root} WHERE cleanup_id = %s "
        "ORDER BY frozen_root_key LIMIT 257",
        (cleanup_id,),
    )
    if len(root_rows) != frozen_count:
        raise OperationalSemanticValidationError(
            "operational READY OPEN PCOM root count differs"
        )
    frozen_authorities = tuple(
        _decode_pcom_frozen_authority(row[0]) for row in root_rows
    )
    frozen_receipts = tuple(receipt_id for receipt_id, _ in frozen_authorities)
    if len(set(frozen_receipts)) != len(frozen_receipts):
        raise OperationalSemanticValidationError(
            "operational READY PCOM frozen receipt authority is duplicated"
        )

    current = _pcom_single_row(
        connector,
        label="OPEN PCOM current checkpoint",
        query=(
            f"SELECT state.phase, state.cursor_bytes, state.state, "
            "state.deleted_count, registry.phase_order "
            f"FROM {checkpoint} AS state "
            f"JOIN {phase_registry} AS registry "
            "ON registry.phase = state.phase "
            "AND registry.target_kind = 'PUBLICATION_COMMIT' "
            "WHERE state.cleanup_id = %s "
            "AND state.state = 'OPEN' "
            "ORDER BY registry.phase_order DESC LIMIT 2"
        ),
        parameters=(cleanup_id,),
    )
    current_phase = _as_text(current[0], label="OPEN PCOM phase")
    current_cursor = _as_bytes(current[1], label="OPEN PCOM cursor")
    current_order = _as_int(current[4], label="OPEN PCOM phase order", minimum=1)
    if _PCOM_TRANSITION_PHASE_ORDER.get(current_phase) != current_order:
        raise OperationalSemanticValidationError(
            "operational READY OPEN PCOM phase order drifts"
        )
    live_authorities: dict[bytes, tuple[bytes, int]] = {}
    if current_order < _PCOM_TRANSITION_PHASE_ORDER["PCOM_COMMIT_EFFECT_ROOT"]:
        for receipt_id, frozen_preparation_id in frozen_authorities:
            authority = _pcom_commit_preparation(
                connector,
                backend,
                receipt_id,
            )
            if authority[0] != frozen_preparation_id:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM preparation differs from frozen authority"
                )
            live_authorities[receipt_id] = authority
    if current_order < _PCOM_TRANSITION_PHASE_ORDER["PCOM_EVENT"]:
        return

    if current_phase == "PCOM_EVENT":
        decoded_cursor: tuple[bytes, bytes, int] | None = None
        if current_cursor:
            cursor_receipt, cursor_preparation, sequence_value = _decode_pcom_cursor(
                current_cursor,
                integer_tail=True,
            )
            assert isinstance(sequence_value, int)
            decoded_cursor = (cursor_receipt, cursor_preparation, sequence_value)
            if cursor_receipt not in frozen_receipts:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM EVENT cursor is outside frozen roots"
                )
        for receipt_id, frozen_preparation_id in frozen_authorities:
            preparation_id, event_count = live_authorities[receipt_id]
            if preparation_id != frozen_preparation_id:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM preparation differs from frozen authority"
                )
            _pcom_require_no_preparation(
                connector,
                backend,
                preparation_id=preparation_id,
            )
            if decoded_cursor is None:
                continue
            cursor_receipt, cursor_preparation, sequence_value = decoded_cursor
            if receipt_id < cursor_receipt:
                _pcom_require_no_events(
                    connector,
                    backend,
                    preparation_id=preparation_id,
                )
            elif receipt_id == cursor_receipt:
                if (
                    cursor_preparation != preparation_id
                    or sequence_value >= event_count
                ):
                    raise OperationalSemanticValidationError(
                        "operational READY PCOM EVENT cursor exceeds its seal"
                    )
                _pcom_require_no_events(
                    connector,
                    backend,
                    preparation_id=preparation_id,
                    through_sequence_no=sequence_value,
                )
        return

    if current_order < _PCOM_TRANSITION_PHASE_ORDER["PCOM_COMMIT_EFFECT_ROOT"]:
        for receipt_id, frozen_preparation_id in frozen_authorities:
            preparation_id, _event_count = live_authorities[receipt_id]
            if preparation_id != frozen_preparation_id:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM preparation differs from frozen authority"
                )
            _pcom_require_no_preparation(
                connector,
                backend,
                preparation_id=preparation_id,
            )
            _pcom_require_no_events(
                connector,
                backend,
                preparation_id=preparation_id,
            )
        return

    compound = _pcom_single_row(
        connector,
        label="PCOM compound checkpoint",
        query=(
            f"SELECT cursor_bytes, deleted_count, state FROM {checkpoint} "
            "WHERE cleanup_id = %s AND phase = 'PCOM_COMMIT_EFFECT_ROOT' LIMIT 2"
        ),
        parameters=(cleanup_id,),
    )
    compound_cursor = _as_bytes(compound[0], label="PCOM compound cursor")
    compound_deleted = _as_int(compound[1], label="PCOM compound deleted count")
    compound_state = _as_text(compound[2], label="PCOM compound state")
    if not compound_cursor:
        if frozen_count == 0:
            if compound_deleted != 0:
                raise OperationalSemanticValidationError(
                    "operational READY empty PCOM compound proof deleted rows"
                )
            if current_phase == "PCOM_COMMIT_EFFECT_ROOT":
                if compound_state != "OPEN":
                    raise OperationalSemanticValidationError(
                        "operational READY empty PCOM compound prestate is invalid"
                    )
                return
            empty_proof = _pcom_single_row(
                connector,
                label="empty PCOM compound receipt proof",
                query=(
                    f"SELECT start_cursor, next_cursor, row_count FROM {receipt} "
                    "WHERE cleanup_id = %s "
                    "AND phase = 'PCOM_COMMIT_EFFECT_ROOT' LIMIT 2"
                ),
                parameters=(cleanup_id,),
            )
            empty_start = _as_bytes(empty_proof[0], label="empty PCOM receipt start")
            empty_next = _as_bytes(empty_proof[1], label="empty PCOM receipt next")
            empty_rows = _as_int(empty_proof[2], label="empty PCOM receipt row count")
            if compound_state != "COMPLETE" or (
                empty_start,
                empty_next,
                empty_rows,
            ) != (b"", b"", 0):
                raise OperationalSemanticValidationError(
                    "operational READY empty PCOM compound receipt proof differs"
                )
            return
        if current_phase != "PCOM_COMMIT_EFFECT_ROOT":
            raise OperationalSemanticValidationError(
                "operational READY later PCOM phase lacks compound cursor proof"
            )
        for receipt_id, frozen_preparation_id in frozen_authorities:
            preparation_id, _event_count = _pcom_commit_preparation(
                connector,
                backend,
                receipt_id,
            )
            if preparation_id != frozen_preparation_id:
                raise OperationalSemanticValidationError(
                    "operational READY PCOM preparation differs from frozen authority"
                )
            _pcom_require_no_preparation(
                connector,
                backend,
                preparation_id=preparation_id,
            )
            _pcom_require_no_events(
                connector,
                backend,
                preparation_id=preparation_id,
            )
        return

    cursor_root, cursor_receipt, cursor_preparation_value = _decode_pcom_cursor(
        compound_cursor,
        integer_tail=False,
    )
    assert isinstance(cursor_preparation_value, bytes)
    if (
        not frozen_receipts
        or cursor_root != frozen_receipts[-1]
        or cursor_receipt != cursor_root
        or cursor_preparation_value != frozen_authorities[-1][1]
        or compound_deleted != frozen_count
    ):
        raise OperationalSemanticValidationError(
            "operational READY PCOM compound cursor does not cover frozen roots"
        )
    commit = _table(backend, "publication_commit")
    for receipt_id, preparation_id in frozen_authorities:
        if _fetch_all(
            connector,
            "PCOM retired commit",
            f"SELECT 1 FROM {commit} WHERE receipt_id = %s LIMIT 1",
            (receipt_id,),
        ):
            raise OperationalSemanticValidationError(
                "operational READY PCOM compound-covered commit reappeared"
            )
        _pcom_require_compound_authority_absent(
            connector,
            backend,
            preparation_id=preparation_id,
        )

    proof = _pcom_single_row(
        connector,
        label="PCOM compound receipt proof",
        query=(
            f"SELECT start_cursor, next_cursor, row_count FROM {receipt} "
            "WHERE cleanup_id = %s AND phase = 'PCOM_COMMIT_EFFECT_ROOT' LIMIT 2"
        ),
        parameters=(cleanup_id,),
    )
    start_cursor = _as_bytes(proof[0], label="PCOM compound receipt start cursor")
    next_cursor = _as_bytes(proof[1], label="PCOM compound receipt next cursor")
    proof_rows = _as_int(proof[2], label="PCOM compound receipt row count")
    valid_open_proof = (
        current_phase == "PCOM_COMMIT_EFFECT_ROOT"
        and compound_state == "OPEN"
        and start_cursor == b""
        and next_cursor == compound_cursor
        and proof_rows == frozen_count
    )
    valid_terminal_proof = (
        current_order > _PCOM_TRANSITION_PHASE_ORDER["PCOM_COMMIT_EFFECT_ROOT"]
        and compound_state == "COMPLETE"
        and start_cursor == compound_cursor
        and next_cursor == compound_cursor
        and proof_rows == 0
    )
    if not (valid_open_proof or valid_terminal_proof):
        raise OperationalSemanticValidationError(
            "operational READY PCOM compound cursor lacks exact receipt proof"
        )
    if current_phase in {"PCOM_FINALIZATION_CHECKPOINT", "PCOM_ANCHOR"}:
        _pcom_require_post_compound_transition(
            connector,
            backend,
            phase=current_phase,
            cursor=current_cursor,
            frozen_authorities=frozen_authorities,
        )


def check_event_integrity_contract_v1(connector: SQLConnector) -> None:
    """Bind event metadata and validate exact OPEN cleanup transition authority."""

    backend = _ready_context(connector, "h2hdb.operational.event-integrity.v1")
    _require_key_shape(
        backend,
        "publication_candidate_preparation",
        primary_key=("candidate_id",),
        unique_keys=(("preparation_id",),),
    )
    _validate_operational_event_root_owners(connector, backend)
    _validate_open_pcom_event_transition(connector, backend)


def check_build_generation_contract_v1(connector: SQLConnector) -> None:
    """Validate the fixed source-build assembly authority key shapes."""

    backend = _ready_context(connector, "h2hdb.operational.build-generation.v1")
    _require_key_shape(
        backend,
        "source_build_generation",
        primary_key=("generation",),
        unique_keys=(),
    )
    _require_key_shape(
        backend,
        "source_build_discovery_checkpoint",
        primary_key=("build_id",),
        unique_keys=(),
    )
    _require_key_shape(
        backend,
        "source_build_discovery_batch_receipt",
        primary_key=("build_id", "batch_key"),
        unique_keys=(("build_id", "start_generation"),),
    )
    _require_key_shape(
        backend,
        "source_build_assembly_checkpoint",
        primary_key=("build_id",),
        unique_keys=(),
    )
    _require_key_shape(
        backend,
        "source_build_assembly_batch_receipt",
        primary_key=("build_id", "batch_key"),
        unique_keys=(("build_id", "start_generation"),),
    )


def check_attempt_identity_contract_v1(connector: SQLConnector) -> None:
    """Validate fixed cleanup cycle IDs; preparation retry identity is writer-owned."""

    backend = _ready_context(connector, "h2hdb.operational.attempt-identity.v1")
    sweep, _phases = _cleanup_layout(backend)
    _cleanup_jobs(connector, backend, sweep)


def check_cleanup_reachability_v1(connector: SQLConnector) -> None:
    """Validate fixed cleanup registries/state, not data-plane reachability."""

    backend = _ready_context(connector, "h2hdb.operational.cleanup-reachability.v1")
    for relation_name in (
        "cleanup_target_kind",
        "cleanup_phase",
        "cleanup_sweep_target",
    ):
        _exact_seeded_relation(connector, backend, relation_name)
    _validate_fixed_cleanup_state(connector, backend)


def check_cleanup_frozen_root_set_v1(connector: SQLConnector) -> None:
    """Validate the bounded sealed per-cycle cleanup root set."""

    backend = _ready_context(connector, "h2hdb.operational.cleanup-frozen-root-set.v1")
    _require_key_shape(
        backend,
        "cleanup_cycle_root",
        primary_key=("cleanup_id", "frozen_root_key"),
        unique_keys=(),
    )
    sweep, _phases = _cleanup_layout(backend)
    jobs = _cleanup_jobs(connector, backend, sweep)
    _validate_cleanup_frozen_roots(connector, backend, jobs)


def _validate_allocator_registry(
    connector: SQLConnector,
    backend: Backend,
    relation_name: str,
    counter_column: str,
) -> None:
    record = _seed_record(backend, relation_name)
    expected_rows = record.get("expected_rows")
    if not isinstance(expected_rows, tuple):
        raise OperationalSemanticRegistryError(
            f"generated allocator {relation_name!r} seed is malformed"
        )
    expected_streams = {
        row[0]
        for row in expected_rows
        if isinstance(row, tuple) and len(row) == 3 and isinstance(row[0], str)
    }
    if len(expected_streams) != len(expected_rows):
        raise OperationalSemanticRegistryError(
            f"generated allocator {relation_name!r} streams are malformed"
        )
    table = _table(backend, relation_name)
    rows = _fetch_all(
        connector,
        f"{relation_name} fixed streams",
        f"""
        SELECT stream, {counter_column}, updated_at
        FROM {table}
        ORDER BY stream
        LIMIT {len(expected_streams) + 1}
        """,
    )
    if len(rows) != len(expected_streams):
        raise OperationalSemanticValidationError(
            f"operational READY allocator {relation_name!r} has wrong cardinality"
        )
    actual_streams: set[str] = set()
    for stream_value, counter_value, updated_at in rows:
        stream = _as_text(stream_value, label=f"{relation_name} stream")
        actual_streams.add(stream)
        _as_int(
            counter_value,
            label=f"{relation_name} {counter_column}",
            minimum=1,
        )
        _as_int(updated_at, label=f"{relation_name} updated_at")
    if actual_streams != expected_streams:
        raise OperationalSemanticValidationError(
            f"operational READY allocator {relation_name!r} streams drift"
        )


def check_revision_allocator_contract_v1(connector: SQLConnector) -> None:
    """Validate the two fixed revision streams without scanning revisions."""

    backend = _ready_context(connector, "h2hdb.operational.revision-allocation.v1")
    _validate_allocator_registry(
        connector, backend, "revision_allocator", "next_revision"
    )


def check_gallery_staging_contract_v1(connector: SQLConnector) -> None:
    """Validate fixed identity streams; staging semantics remain writer-owned."""

    backend = _ready_context(connector, "h2hdb.operational.gallery-staging.v1")
    _validate_allocator_registry(connector, backend, "identity_allocator", "next_id")


def check_gallery_staging_request_budget_v1(connector: SQLConnector) -> None:
    """Validate exact capped request accounting and the one-slot physical key."""

    backend = _ready_context(
        connector, "h2hdb.operational.gallery-staging-request-budget.v1"
    )
    _require_key_shape(
        backend,
        "gallery_observation_staging_request_budget",
        primary_key=("singleton_id",),
        unique_keys=(),
    )
    _require_key_shape(
        backend,
        "gallery_observation_staging",
        primary_key=("staging_id",),
        unique_keys=(("build_id",), ("gallery_id", "observation_id")),
    )
    budget_table = _table(backend, "gallery_observation_staging_request_budget")
    request_table = _table(backend, "gallery_observation_staging_request")
    budget_rows = _fetch_all(
        connector,
        "gallery staging request budget singleton",
        f"SELECT singleton_id, retained_request_count FROM {budget_table} "
        "ORDER BY singleton_id LIMIT 2",
    )
    if len(budget_rows) != 1 or len(budget_rows[0]) != 2:
        raise OperationalSemanticValidationError(
            "gallery staging request budget singleton is missing or duplicated"
        )
    singleton_id = _as_int(
        budget_rows[0][0],
        label="gallery staging request budget singleton_id",
        minimum=1,
        maximum=1,
    )
    retained = _as_int(
        budget_rows[0][1],
        label="gallery staging retained_request_count",
        maximum=1_500_000,
    )
    if singleton_id != 1:
        raise OperationalSemanticValidationError(
            "gallery staging request budget singleton identity differs"
        )
    count_row = _fetch_one(
        connector,
        "gallery staging retained request count",
        f"SELECT COUNT(*) FROM {request_table}",
    )
    if (
        len(count_row) != 1
        or _as_int(
            count_row[0],
            label="gallery staging request identity count",
            maximum=1_500_000,
        )
        != retained
    ):
        raise OperationalSemanticValidationError(
            "gallery staging request budget disagrees with retained identities"
        )


def check_bootstrap_contract_v1(connector: SQLConnector) -> None:
    """Validate generated genesis rows and first-row emptiness while BUILDING."""

    backend, epoch_row = _epoch_context(connector)
    _validate_static_relations(backend, "h2hdb.operational.bootstrap-genesis.v1")
    if epoch_row[2] != "BUILDING":
        raise OperationalSemanticValidationError(
            "operational bootstrap validation is BUILDING-only"
        )
    payload = _payload(backend)
    seeded = payload.get("bootstrap_seeded_relations")
    operational_relations = ARTIFACT.get("operational_relations")
    if not isinstance(seeded, tuple) or not isinstance(operational_relations, tuple):
        raise OperationalSemanticRegistryError(
            "generated operational bootstrap registry is malformed"
        )
    seeded_names: set[str] = set()
    for record in seeded:
        if not isinstance(record, Mapping) or not isinstance(
            record.get("relation"), str
        ):
            raise OperationalSemanticRegistryError(
                "generated operational bootstrap seed is malformed"
            )
        relation_name = cast(str, record["relation"])
        if relation_name in seeded_names:
            raise OperationalSemanticRegistryError(
                "generated operational bootstrap seed is duplicated"
            )
        seeded_names.add(relation_name)
        _exact_seeded_relation(connector, backend, relation_name)

    for relation_name in operational_relations:
        if not isinstance(relation_name, str):
            raise OperationalSemanticRegistryError(
                "generated operational relation name is malformed"
            )
        if relation_name in seeded_names:
            continue
        table = _table(backend, relation_name)
        if _fetch_all(
            connector,
            f"bootstrap empty relation {relation_name}",
            f"SELECT 1 FROM {table} LIMIT 1",
        ):
            raise OperationalSemanticValidationError(
                f"operational bootstrap relation {relation_name!r} is not empty"
            )
