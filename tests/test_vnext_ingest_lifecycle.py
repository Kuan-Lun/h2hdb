from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from threading import Event, Thread
from typing import Any, cast

import pytest

import h2hdb
import h2hdb.vnext_ingest_publication as publication
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_artifact_family import PreparedArtifactFamily
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationAuthority,
    ArtifactPreparationReceipt,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCatalogProjectionPlan,
)


class _TrackedReceipt:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class _TrackedPlan:
    def __init__(self) -> None:
        self.close_count = 0

    def iter_canonical_value_plans(self) -> Iterator[object]:
        return iter(())

    def close(self) -> None:
        self.close_count += 1


class _BlockingCache(publication._ArtifactReceiptCache):
    __slots__ = ("entered", "release")

    def __init__(
        self,
        *,
        authority: ArtifactPreparationAuthority,
        families: tuple[PreparedArtifactFamily, ...],
        receipt: ArtifactPreparationReceipt,
        entered: Event,
        release: Event,
    ) -> None:
        super().__init__(
            authority=authority,
            families=families,
            receipt=receipt,
        )
        self.entered = entered
        self.release = release

    def take(self) -> publication._ArtifactReceiptOwner:
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test did not release the cache take")
        return super().take()


def _context(path: Path) -> RepositoryContext:
    return RepositoryContext.from_config(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )


def _artifact_cache(
    receipt: _TrackedReceipt,
) -> tuple[
    object,
    tuple[PreparedArtifactFamily, ...],
    publication._ArtifactReceiptCache,
]:
    authority = object()
    families: tuple[PreparedArtifactFamily, ...] = ()
    cached = publication._ArtifactReceiptCache(
        authority=cast(ArtifactPreparationAuthority, authority),
        families=families,
        receipt=cast(ArtifactPreparationReceipt, receipt),
    )
    return authority, families, cached


def _install_cache(
    machine: publication.VNextIngestPublication,
    cached: publication._ArtifactReceiptCache,
) -> None:
    private = cast(Any, machine)
    with private._VNextIngestPublication__artifact_receipt_lock:
        private._VNextIngestPublication__artifact_receipt = cached


def _join(thread: Thread) -> None:
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_public_ingest_facade_context_closes_owned_cache_and_fails_closed(
    tmp_path: Path,
) -> None:
    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(tmp_path / "facade-lifecycle.sqlite3"),
        )
    )
    receipt = _TrackedReceipt()
    with h2hdb.VNextIngestFacade(config) as facade:
        machine = cast(Any, facade)._VNextIngestFacade__publication_orchestrator()
        _authority, _families, cached = _artifact_cache(receipt)
        _install_cache(machine, cached)

    assert receipt.close_count == 1
    facade.close()
    assert receipt.close_count == 1

    closed_calls: tuple[Callable[[], object], ...] = (
        lambda: facade.prepare_source(cast(Any, object())),
        lambda: facade.issue_source_step(
            cast(Any, object()), cast(Any, object()), cast(Any, object())
        ),
        lambda: facade.prepare_source_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.commit_source_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.prepare_analysis(b"b" * 16, cast(Any, object()), max_rows=1),
        lambda: facade.issue_analysis_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.prepare_analysis_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.commit_analysis_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.issue_publication_step(cast(Any, object()), cast(Any, object())),
        lambda: facade.try_issue_publication_recovery_step(cast(Any, object())),
        lambda: facade.prepare_publication_step(
            cast(Any, object()),
            artifact_adapters={},
            finalization_adapters={},
            library_activation=cast(Any, object()),
        ),
        lambda: facade.commit_publication_step(
            cast(Any, object()), cast(Any, object())
        ),
        lambda: facade.try_claim_ingest(False, 1),
        lambda: facade.resume_ingest(cast(Any, object())),
        lambda: facade.renew_ingest(cast(Any, object()), 1),
        lambda: facade.complete_ingest(cast(Any, object())),
        lambda: facade.drain_current_only_maintenance(1),
        lambda: facade.ensure_policy(cast(Any, object()), cast(Any, object())),
        lambda: facade.__enter__(),
    )
    for call in closed_calls:
        with pytest.raises(ValueError, match="ingest facade is closed"):
            call()


def test_publication_close_defers_borrowed_plan_and_rejects_new_installs(
    tmp_path: Path,
) -> None:
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "plan-lifecycle.sqlite3")
    )
    plan = _TrackedPlan()
    cached = publication._PublicationPlanCache(
        action=publication._Action.BUILD_CATALOG,
        authority=object(),
        plan=cast(PublicationCatalogProjectionPlan, plan),
    )
    lease = cached.borrow()
    private = cast(Any, machine)
    with private._VNextIngestPublication__publication_plan_lock:
        private._VNextIngestPublication__publication_plan = cached

    machine.close()
    machine.close()

    assert plan.close_count == 0
    lease.close()
    assert plan.close_count == 1

    rejected_plan = _TrackedPlan()
    with pytest.raises(ValueError, match="orchestrator is closed"):
        private._VNextIngestPublication__install_plan(
            publication._Action.BUILD_CATALOG,
            object(),
            cast(PublicationCatalogProjectionPlan, rejected_plan),
        )
    assert rejected_plan.close_count == 1

    with pytest.raises(ValueError, match="orchestrator is closed"):
        machine.__enter__()


def test_close_wins_artifact_cache_install_and_take_races(tmp_path: Path) -> None:
    install_machine = publication.VNextIngestPublication(
        _context(tmp_path / "close-install.sqlite3")
    )
    install_receipt = _TrackedReceipt()
    _authority, _families, candidate = _artifact_cache(install_receipt)
    install_private = cast(Any, install_machine)
    install_lock = install_private._VNextIngestPublication__artifact_receipt_lock
    install_started = Event()
    failures: list[BaseException] = []

    def install() -> None:
        install_started.set()
        try:
            install_private._VNextIngestPublication__install_artifact_receipt(candidate)
        except BaseException as error:
            failures.append(error)

    install_lock.acquire()
    installer = Thread(target=install)
    closer = Thread(target=install_machine.close)
    installer.start()
    assert install_started.wait(timeout=5)
    closer.start()
    assert install_private._VNextIngestPublication__closed.wait(timeout=5)
    install_lock.release()
    _join(installer)
    _join(closer)

    assert not failures
    assert install_receipt.close_count == 1
    assert install_private._VNextIngestPublication__artifact_receipt is None

    take_machine = publication.VNextIngestPublication(
        _context(tmp_path / "close-take.sqlite3")
    )
    take_receipt = _TrackedReceipt()
    take_authority, take_families, take_cached = _artifact_cache(take_receipt)
    _install_cache(take_machine, take_cached)
    take_private = cast(Any, take_machine)
    take_lock = take_private._VNextIngestPublication__artifact_receipt_lock
    take_lock.acquire()
    take_errors: list[BaseException] = []

    def take() -> None:
        try:
            take_private._VNextIngestPublication__take_artifact_receipt(
                take_authority,
                take_families,
            )
        except BaseException as error:
            take_errors.append(error)

    take_closer = Thread(target=take_machine.close)
    taker = Thread(target=take)
    take_closer.start()
    assert take_private._VNextIngestPublication__closed.wait(timeout=5)
    taker.start()
    take_lock.release()
    _join(take_closer)
    _join(taker)

    assert len(take_errors) == 1
    assert isinstance(take_errors[0], ValueError)
    assert "orchestrator is closed" in str(take_errors[0])
    assert take_receipt.close_count == 1
    assert take_private._VNextIngestPublication__artifact_receipt is None


def test_cache_take_before_close_transfers_exactly_one_owner(tmp_path: Path) -> None:
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "take-close.sqlite3")
    )
    receipt = _TrackedReceipt()
    authority = cast(ArtifactPreparationAuthority, object())
    families: tuple[PreparedArtifactFamily, ...] = ()
    entered = Event()
    release = Event()
    cached = _BlockingCache(
        authority=authority,
        families=families,
        receipt=cast(ArtifactPreparationReceipt, receipt),
        entered=entered,
        release=release,
    )
    _install_cache(machine, cached)
    private = cast(Any, machine)
    owners: list[publication._ArtifactReceiptOwner | None] = []
    failures: list[BaseException] = []

    def take() -> None:
        try:
            owners.append(
                private._VNextIngestPublication__take_artifact_receipt(
                    authority,
                    families,
                )
            )
        except BaseException as error:
            failures.append(error)

    taker = Thread(target=take)
    closer = Thread(target=machine.close)
    taker.start()
    assert entered.wait(timeout=5)
    closer.start()
    assert private._VNextIngestPublication__closed.wait(timeout=5)
    release.set()
    _join(taker)
    _join(closer)

    assert not failures
    assert len(owners) == 1
    owner = owners[0]
    assert owner is not None
    assert receipt.close_count == 0
    assert private._VNextIngestPublication__artifact_receipt is None
    owner.close()
    owner.close()
    assert receipt.close_count == 1
