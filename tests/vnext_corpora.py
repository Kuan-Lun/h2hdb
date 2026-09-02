"""Production corpora for the physical-domain and corruption matrices.

Every corpus is produced only through the public facades (plus the hash-cache
repository, which has no facade of its own) on a fresh temporary database:

* catalogs at rest (populated, a 2200-page gallery whose canonical values span
  several pages, an incremental spam-exclusion flip, pending and missing
  download requests, a hash-cache handoff); and
* turns abandoned right before an exact fenced boundary, which hold the
  transient staging, analysis, operational-event, candidate, commit, and
  activation relations, and one drain interrupted between cleanup batches.

A mid-flight corpus keeps its in-memory source and library so the abandoned
turn can be resumed by a later owner (``resume``) exactly as a restarted
resident would.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from test_vnext_hash_cache_repository import _authorities, _database, _put, _ready_build
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    Clock,
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    gallery,
    initialize_database,
    populate_catalog,
    run_ingest_turn,
    takeover_clock,
)

from h2hdb import CoreConfig, VNextDownloadQueueFacade, VNextIngestFacade
from h2hdb.vnext_canonical_value_repository import CanonicalValueUploadPlan
from h2hdb.vnext_hash_cache_repository import (
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

# Relations the manifest declares but no production writer populates; they
# cannot rest in any facade-produced corpus.
NO_PRODUCTION_WRITER = frozenset(
    {
        "catalog_gallery_observation_discovery_fingerprints",
        "catalog_gallery_observation_raw_content",
        "operational_gallery_redownload_states",
    }
)

# Transient relations that hold rows only inside one bounded transaction or
# only under a live download owner.
NEVER_AT_REST = frozenset({"operational_download_generation_owners"})


class _Stop(Exception):
    pass


@dataclass
class Corpus:
    name: str
    config: CoreConfig
    source: MemorySource | None
    library: MemoryLibrary | None
    stop: str | None = None
    periodic: bool = True
    """False when a download handoff awaits a linked (non-periodic) ingest."""

    @property
    def mid_flight(self) -> bool:
        return self.stop is not None

    @property
    def consumable(self) -> bool:
        """An at-rest corpus whose retained history the next incremental turn
        consumes (it keeps its source and library)."""

        return self.stop is None and self.source is not None

    def consume(self, config: CoreConfig | None = None) -> None:
        """Run one incremental turn that adds a gallery, on ``config``."""

        assert self.source is not None and self.library is not None
        source = copy.deepcopy(self.source)
        library = copy.deepcopy(self.library)
        library.source = source
        source.put(gallery(9999, pages=[b"consumed-page"], artists=["zed"]))
        facade = VNextIngestFacade(config or self.config, clock=takeover_clock())
        try:
            run_ingest_turn(
                facade, source=source, library=library, periodic=self.periodic
            )
            drain_maintenance(facade)
        finally:
            facade.close()

    def resume(self, config: CoreConfig | None = None) -> None:
        """Complete the abandoned turn as a later owner on ``config`` (a copy
        of this corpus's database) with deep-copied source and library."""

        assert self.source is not None and self.library is not None
        source = copy.deepcopy(self.source)
        library = copy.deepcopy(self.library)
        library.source = source
        facade = VNextIngestFacade(config or self.config, clock=takeover_clock())
        try:
            run_ingest_turn(
                facade, source=source, library=library, periodic=self.periodic
            )
            drain_maintenance(facade)
        finally:
            facade.close()


def _giant_source() -> MemorySource:
    return MemorySource(
        [
            gallery(1001, pages=[b"p%03d" % index for index in range(300)]),
            gallery(1002, pages=[b"q0"], artists=["bob"], language="japanese"),
        ]
    )


def _stopped(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    *,
    stop: str,
    occurrence: int = 1,
) -> None:
    seen = 0

    def boundary(label: str) -> None:
        nonlocal seen
        if label == stop:
            seen += 1
            if seen == occurrence:
                raise _Stop(label)

    facade = VNextIngestFacade(config, clock=Clock())
    try:
        try:
            run_ingest_turn(facade, source=source, library=library, boundary=boundary)
            drain_maintenance(facade, boundary=boundary)
        except _Stop:
            return
    finally:
        facade.close()
    raise AssertionError(f"boundary {stop!r} occurrence {occurrence} never happened")


def _turn(config: CoreConfig, source: MemorySource, library: MemoryLibrary) -> None:
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        run_ingest_turn(facade, source=source, library=library)
        drain_maintenance(facade)
    finally:
        facade.close()


INCREMENTAL_STOPS = (
    "source.commit:STAGING_SEAL",
    "source.commit:ASSEMBLY",
    "analysis.commit:gid_winner",
    "publication.commit:SEAL_OPERATIONAL",
    "publication.commit:COMMIT_PUBLICATION",
    "publication.commit:LIBRARY_ACTIVATION",
    "publication.commit:FINALIZE",
)


def build_corpora(
    root: Path,
    backend_config: Callable[[str], CoreConfig],
    *,
    names: set[str] | None = None,
) -> list[Corpus]:
    """Build every corpus (or only ``names``) on databases from
    ``backend_config``; each corpus needs its own fresh database."""

    del root
    corpora: list[Corpus] = []
    wanted = names

    def want(name: str) -> bool:
        return wanted is None or name in wanted

    if want("ready-populated"):
        ready = backend_config("ready")
        initialize_database(ready)
        ready_source, ready_library = populate_catalog(ready)
        corpora.append(Corpus("ready-populated", ready, ready_source, ready_library))
    if wanted is not None and wanted <= {"ready-populated"}:
        return corpora

    big = backend_config("big")
    initialize_database(big)
    big_source = MemorySource(
        [gallery(4001, pages=[b"x%04d" % index for index in range(2200)])]
    )
    big_library = MemoryLibrary(big_source)
    _turn(big, big_source, big_library)
    corpora.append(Corpus("ready-big", big, big_source, big_library))

    exclusion = backend_config("exclusion")
    initialize_database(exclusion)
    spam_source = MemorySource(
        [
            gallery(4000 + index, pages=[b"dup-0", b"dup-1"], artists=[artist])
            for index, artist in enumerate(("ann", "ben", "cid"))
        ]
        + [gallery(1002, pages=[b"q0"], artists=["bob"])]
    )
    spam_library = MemoryLibrary(spam_source)
    _turn(exclusion, spam_source, spam_library)
    spam_source.remove(("gallery-4002",))
    _turn(exclusion, spam_source, spam_library)
    corpora.append(Corpus("ready-exclusion", exclusion, spam_source, spam_library))

    pending = backend_config("download-pending")
    initialize_database(pending)
    pending_source, pending_library = populate_catalog(pending)
    queue = VNextDownloadQueueFacade(pending, clock=Clock())
    queue.request_download(3001, url="https://example.invalid/g/3001")
    corpora.append(Corpus("download-pending", pending, pending_source, pending_library))

    missing = backend_config("download-missing")
    initialize_database(missing)
    missing_source, missing_library = populate_catalog(missing)
    queue = VNextDownloadQueueFacade(missing, clock=Clock())
    request = queue.request_download(3002, url="https://example.invalid/g/3002")
    turn = queue.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
    queue.finish_missing_download_turn(turn, request, 3002)
    corpora.append(
        Corpus(
            "download-missing",
            missing,
            missing_source,
            missing_library,
            periodic=False,
        )
    )

    cache = backend_config("hash-cache")
    if cache.database.sql_type == "sqlite":
        # The hash cache has no facade; its repository is driven directly on a
        # generated database exactly like its own production tests.
        _hash_cache_handoff(cache)
        corpora.append(Corpus("hash-cache", cache, None, None))

    for index, stop in enumerate(INCREMENTAL_STOPS):
        config = backend_config(f"stop-{index}")
        initialize_database(config)
        source = _giant_source()
        library = MemoryLibrary(source)
        _turn(config, source, library)
        source.put(
            gallery(
                1001,
                pages=[b"p%03d" % index for index in range(300)] + [b"p-extra"],
            )
        )
        source.remove(("gallery-1002",))
        _stopped(config, source, library, stop=stop)
        corpora.append(Corpus(f"stop:{stop}", config, source, library, stop))

    cleanup = backend_config("mid-cleanup")
    initialize_database(cleanup)
    cleanup_source = MemorySource(
        [
            gallery(4001, pages=[b"x%04d" % index for index in range(2200)]),
            gallery(1002, pages=[b"q0"], artists=["bob"]),
        ]
    )
    cleanup_library = MemoryLibrary(cleanup_source)
    _turn(cleanup, cleanup_source, cleanup_library)
    cleanup_source.remove(("gallery-4001",))
    _stopped(
        cleanup,
        cleanup_source,
        cleanup_library,
        stop="maintenance",
        occurrence=_first_checkpointed_drain(cleanup, cleanup_source, cleanup_library),
    )
    corpora.append(
        Corpus("mid-cleanup", cleanup, cleanup_source, cleanup_library, "maintenance")
    )
    return corpora


def _first_checkpointed_drain(
    config: CoreConfig, source: MemorySource, library: MemoryLibrary
) -> int:
    """Find the drain call after which a cleanup checkpoint rests, on a copy."""

    probe_path = Path(config.database.database + ".probe")
    if config.database.sql_type != "sqlite":
        return 2
    import shutil

    shutil.copyfile(config.database.database, probe_path)
    probe = CoreConfig(
        database=type(config.database)(sql_type="sqlite", database=str(probe_path))
    )
    probe_source = copy.deepcopy(source)
    probe_library = copy.deepcopy(library)
    probe_library.source = probe_source
    facade = VNextIngestFacade(probe, clock=Clock())
    try:
        run_ingest_turn(facade, source=probe_source, library=probe_library)
        for call in range(1, 64):
            facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
            if _has_rows(probe, "operational_cleanup_checkpoints"):
                return call + 1
    finally:
        facade.close()
    raise AssertionError("no drain call left a cleanup checkpoint at rest")


def _has_rows(config: CoreConfig, table: str) -> bool:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return bool(connector.fetch_one(f"SELECT 1 FROM {table} LIMIT 1"))
    finally:
        connector.close()


def _hash_cache_handoff(config: CoreConfig) -> None:
    connector = _database(Path(config.database.database))
    source_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_source_identity_v1", (b"source-id-v1\0", b"/gallery/file.jpg")
    )
    fingerprint_plan = CanonicalValueUploadPlan.from_parts(
        "filesystem_fingerprint_v1", (b"fingerprint-v1\0", b"stat")
    )
    try:
        gate, turn = _authorities(connector)
        _ready_build(connector, gate, turn)
        _put(connector, gate, turn, source_plan, start=30)
        _put(connector, gate, turn, fingerprint_plan, start=40)
        with connector.transaction():
            VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend=config.database.sql_type),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source_plan,
                fingerprint_plan=fingerprint_plan,
                file_plan=FileHashObservationPlan.from_parts((b"file-", b"bytes")),
                observed_at=50,
                cached_at=51,
                now=52,
            )
    finally:
        source_plan.close()
        fingerprint_plan.close()
        connector.close()


def corpus_tables(corpora: list[Corpus], tables: set[str]) -> dict[str, Any]:
    """Map each table to the first corpus holding a row of it."""

    found: dict[str, Any] = {}
    for corpus in corpora:
        connector = open_connector(corpus.config)
        try:
            with connector.read_transaction():
                for table in tables - set(found):
                    if connector.fetch_one(f"SELECT 1 FROM {table} LIMIT 1"):
                        found[table] = corpus.name
        finally:
            connector.close()
    return found
