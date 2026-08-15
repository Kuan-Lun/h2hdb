from __future__ import annotations

from typing import Any, cast

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_domains import DomainValidationError, require_text
from h2hdb.vnext_gallery_identity_repository import (
    GalleryIdentityRepository,
    SourceLocatorCommand,
)
from h2hdb.vnext_gallery_staging_repository import (
    BatchAttempt,
    FileBatchCommand,
    FileContentReceipt,
    FileObservation,
    GalleryObservationStagingRepository,
)
from h2hdb.vnext_hash_cache_repository import (
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_operational_event_repository import RemovedGid, _require_effects
from h2hdb.vnext_physical_domains import (
    CATALOG_PHYSICAL_DOMAIN_GUARDS,
    CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_RELATIONS,
    CATALOG_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS,
    OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS,
    OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
    OPERATIONAL_SCHEMA_EPOCH_WRITERS,
)
from h2hdb.vnext_queue_repository import VNextDownloadRequest, VNextQueueRepository
from h2hdb.vnext_source_build_repository import (
    _DISCOVERY_BATCH_TOKEN,
    DiscoveryBatch,
    PreparedDiscoveryLocator,
    SourceBuildRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


class _NoDatabaseWork:
    """Fail if a malformed command gets as far as a database operation."""

    @property
    def connector(self) -> Any:  # pragma: no cover - a test failure path
        raise AssertionError("malformed physical-domain command reached SQL")


def _work_without_database() -> VNextUnitOfWork:
    return cast(VNextUnitOfWork, _NoDatabaseWork())


def _contract_relations(obligation_id: str) -> frozenset[str]:
    records = cast(
        tuple[dict[str, Any], ...],
        ARTIFACT["semantic_obligations"],
    )
    matches = tuple(record for record in records if record["id"] == obligation_id)
    assert len(matches) == 1
    contract = cast(dict[str, Any], matches[0]["contract"])
    return frozenset(cast(list[str], contract["relations"]))


def test_closed_writer_families_match_the_generated_contract_and_real_symbols() -> None:
    assert CATALOG_PHYSICAL_DOMAIN_RELATIONS == _contract_relations(
        "catalog.physical-domains.v1"
    )
    assert OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS == _contract_relations(
        "h2hdb.operational.physical-domains.v1"
    )
    assert CATALOG_PHYSICAL_DOMAIN_MUTATION_RELATIONS == (
        CATALOG_PHYSICAL_DOMAIN_RELATIONS
        - {"artifact_producer_fingerprint", "artifact_storage_codec"}
    )
    assert OPERATIONAL_PHYSICAL_DOMAIN_MUTATION_RELATIONS == (
        OPERATIONAL_PHYSICAL_DOMAIN_RELATIONS - {"schema_epoch_control"}
    )
    assert len(CATALOG_PHYSICAL_DOMAIN_WRITERS) == 45
    assert len(OPERATIONAL_PHYSICAL_DOMAIN_WRITERS) == 9
    assert len(OPERATIONAL_SCHEMA_EPOCH_WRITERS) == 1

    for symbol in (
        *CATALOG_PHYSICAL_DOMAIN_WRITERS,
        *OPERATIONAL_PHYSICAL_DOMAIN_WRITERS,
        *OPERATIONAL_SCHEMA_EPOCH_WRITERS,
    ):
        owner_name, method_name = symbol.__qualname__.split(".")
        module = __import__(symbol.__module__, fromlist=[owner_name])
        owner = getattr(module, owner_name)
        assert not method_name.startswith("_")
        assert getattr(owner, method_name) is symbol

    for guard in (
        *CATALOG_PHYSICAL_DOMAIN_GUARDS,
        *OPERATIONAL_PHYSICAL_DOMAIN_GUARDS,
    ):
        module = __import__(guard.__module__, fromlist=[guard.__name__])
        assert getattr(module, guard.__name__) is guard
        assert guard.__name__ != "<lambda>"


def test_unbounded_sql_text_still_requires_exact_str() -> None:
    assert require_text("https://example.invalid/a", field="url") == (
        "https://example.invalid/a"
    )
    for malformed in (b"url", 1, None):
        with pytest.raises(DomainValidationError):
            require_text(malformed, field="url")


def test_forged_download_completion_is_rejected_before_sql() -> None:
    request = VNextDownloadRequest(7, "", b"r" * 16, 1)
    object.__setattr__(request, "gid", True)
    with pytest.raises(DomainValidationError, match="gid"):
        VNextQueueRepository.complete_download_request(
            _work_without_database(),
            request=request,
        )


def test_forged_canonical_plan_is_rejected_before_sql() -> None:
    plan = CanonicalValueUploadPlan.from_parts("physical_test_v1", (b"payload",))
    try:
        plan.value_sha256 = b"short"
        with pytest.raises(DomainValidationError, match="value_sha256"):
            CanonicalValueRepository.allocate(
                _work_without_database(),
                gate_lease=cast(Any, object()),
                ingest_turn=cast(Any, object()),
                plan=plan,
                now=1,
            )
    finally:
        plan.close()


def test_forged_hash_observation_plan_is_rejected_before_sql() -> None:
    plan = FileHashObservationPlan.from_parts((b"payload",))
    object.__setattr__(plan, "size_bytes", True)
    with pytest.raises(DomainValidationError, match="size_bytes"):
        VNextHashCacheRepository.handoff(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            source_plan=cast(Any, object()),
            fingerprint_plan=cast(Any, object()),
            file_plan=plan,
            observed_at=1,
            cached_at=1,
            now=1,
        )


def test_forged_locator_command_is_revalidated_before_sql() -> None:
    command = SourceLocatorCommand(("gallery",))
    object.__setattr__(command, "components", ())
    with pytest.raises((ValueError, IndexError)):
        GalleryIdentityRepository.handoff_locator(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            build_id=b"b" * 16,
            command=command,
            locator_plan=cast(Any, object()),
            now=1,
        )


def test_forged_gallery_batch_entry_is_rejected_before_sql() -> None:
    content = FileContentReceipt.from_parts((b"payload",))
    entry = FileObservation(b"001.jpg", content, 1, 2, 3, 4)
    command = FileBatchCommand(
        (entry,),
        False,
        BatchAttempt(b"o" * 16, None),
    )
    object.__setattr__(entry, "device", True)
    with pytest.raises(ValueError, match="device"):
        GalleryObservationStagingRepository.put_files(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            handle=cast(Any, object()),
            command=command,
            now=1,
        )


def test_forged_discovery_locator_is_rejected_before_sql() -> None:
    plan_capability = object()
    locator = PreparedDiscoveryLocator(
        0,
        b"l" * 32,
        1,
        b"p" * 32,
        b"gallery",
        plan_capability,
    )
    batch = DiscoveryBatch(
        b"b" * 16,
        b"k" * 32,
        b"s" * 16,
        1,
        b"t" * 32,
        1,
        b"",
        0,
        (locator,),
        False,
        plan_capability,
        object(),
        _DISCOVERY_BATCH_TOKEN,
    )
    object.__setattr__(locator, "position", True)
    with pytest.raises(DomainValidationError, match="position"):
        SourceBuildRepository.resolve_discovery_locator(
            _work_without_database(),
            gate_lease=cast(Any, object()),
            ingest_turn=cast(Any, object()),
            batch=batch,
            locator=locator,
            upload_plan=cast(Any, object()),
            now=1,
        )


def test_forged_operational_effect_is_rejected_before_event_derivation() -> None:
    effect = RemovedGid(7, b"r" * 16)
    object.__setattr__(effect, "gid", True)
    with pytest.raises(DomainValidationError, match="removed gid"):
        _require_effects((effect,))
