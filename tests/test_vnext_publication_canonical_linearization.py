from __future__ import annotations

import gc
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import h2hdb.vnext_ingest_publication as publication
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import VNextIngestSession
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationRepository,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValuePartialFamilyError,
    CanonicalValueReadReceipt,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateBatch,
    PublicationCandidateRepository,
)

_CANDIDATE_ID = b"c" * 16
_SESSION = VNextIngestSession(
    gate_owner_token=b"g" * 16,
    gate_generation=1,
    gate_slot=0,
    gate_lease_expires_at=100,
    ingest_generation=1,
    ingest_owner_token=b"i" * 16,
    ingest_lease_expires_at=100,
    download_generation=None,
    handoff_owner_token=None,
    handoff_kind=None,
    consumed_at=None,
)

type _TraceEntry = tuple[str, bytes | None, bytes | None, int, bool]


@dataclass(frozen=True, slots=True)
class _CanonicalFixture:
    domain: str
    payload: bytes
    value_sha256: bytes
    pages: tuple[PreparedCanonicalPage, ...]
    receipt: CanonicalValueReadReceipt


@dataclass(slots=True)
class _DurableState:
    payloads: dict[bytes, bytes]
    consumer_batches: dict[bytes, int]
    claims: set[bytes] = field(default_factory=set)
    pages: dict[bytes, bytes] = field(default_factory=dict)
    sealed: dict[bytes, CanonicalValueReadReceipt] = field(default_factory=dict)
    consumed_values: set[bytes] = field(default_factory=set)
    partial_values: set[bytes] = field(default_factory=set)
    sealed_payload_overrides: dict[bytes, bytes] = field(default_factory=dict)
    stage_batch_limit: int = 1
    stage_commits: int = 0
    stage_cursor: bytes = b""
    output_rows: int = 0
    stage_done: bool = False
    lose_next_commit_response: bool = False

    def clone(self) -> _DurableState:
        return _DurableState(
            payloads=dict(self.payloads),
            consumer_batches=dict(self.consumer_batches),
            claims=set(self.claims),
            pages=dict(self.pages),
            sealed=dict(self.sealed),
            consumed_values=set(self.consumed_values),
            partial_values=set(self.partial_values),
            sealed_payload_overrides=dict(self.sealed_payload_overrides),
            stage_batch_limit=self.stage_batch_limit,
            stage_commits=self.stage_commits,
            stage_cursor=self.stage_cursor,
            output_rows=self.output_rows,
            stage_done=self.stage_done,
            lose_next_commit_response=self.lose_next_commit_response,
        )

    def observable_snapshot(self) -> tuple[object, ...]:
        """Durable result excluding the explicitly disposable upload claims."""

        return (
            tuple(sorted(self.pages.items())),
            tuple(sorted(self.sealed.items())),
            frozenset(self.consumed_values),
            self.stage_commits,
            self.stage_cursor,
            self.output_rows,
            self.stage_done,
        )


def _fixtures(payloads: Sequence[bytes]) -> tuple[_CanonicalFixture, ...]:
    fixtures: list[_CanonicalFixture] = []
    for payload in payloads:
        with CanonicalValueUploadPlan.from_parts(
            "storage_object_key_v2",
            (payload,),
        ) as plan:
            pages = tuple(plan.iter_pages())
            fixtures.append(
                _CanonicalFixture(
                    "storage_object_key_v2",
                    payload,
                    plan.value_sha256,
                    pages,
                    CanonicalValueReadReceipt(
                        plan.value_sha256,
                        plan.digest_domain,
                        plan.byte_count,
                        plan.root_page_sha256,
                    ),
                )
            )
    return tuple(sorted(fixtures, key=lambda fixture: fixture.value_sha256))


def _initial_state(
    fixtures: Sequence[_CanonicalFixture],
    *,
    stage_batch_limit: int = 1,
) -> _DurableState:
    count = len(fixtures)
    consumers = {
        fixture.value_sha256: min(
            stage_batch_limit,
            (index * stage_batch_limit // max(1, count)) + 1,
        )
        for index, fixture in enumerate(fixtures)
    }
    return _DurableState(
        payloads={fixture.value_sha256: fixture.payload for fixture in fixtures},
        consumer_batches=consumers,
        stage_batch_limit=stage_batch_limit,
    )


def _canonical_trace(
    operation: str,
    fixture: _CanonicalFixture,
    page: PreparedCanonicalPage | None = None,
) -> _TraceEntry:
    return (
        operation,
        fixture.value_sha256,
        None if page is None else page.page_sha256,
        0,
        False,
    )


def _reference_next(
    fixtures: Sequence[_CanonicalFixture],
    state: _DurableState,
) -> tuple[_TraceEntry, _CanonicalFixture, PreparedCanonicalPage | None] | None:
    """Retained exhaustive oracle for the pre-optimization selector."""

    for fixture in fixtures:
        value = fixture.value_sha256
        if value in state.partial_values:
            raise RuntimeError("canonical sealed identity is partial or corrupt")
        sealed = state.sealed.get(value)
        if sealed is not None:
            actual_payload = state.sealed_payload_overrides.get(
                value,
                state.payloads[value],
            )
            if actual_payload != fixture.payload:
                raise RuntimeError(
                    "sealed canonical identity differs from the plan's exact preimage"
                )
            if sealed != fixture.receipt:
                raise RuntimeError(
                    "sealed canonical identity receipt differs from the upload plan"
                )
            if value not in state.claims:
                operation = (
                    "REDUNDANT_CANONICAL_ALLOCATE"
                    if value in state.consumed_values
                    else "CANONICAL_ALLOCATE"
                )
                return _canonical_trace(operation, fixture), fixture, None
            continue
        if value not in state.claims:
            return _canonical_trace("CANONICAL_ALLOCATE", fixture), fixture, None
        for page in fixture.pages:
            stored = state.pages.get(page.page_sha256)
            if stored is None:
                return _canonical_trace("CANONICAL_PAGE", fixture, page), fixture, page
            if stored != page.page_bytes:
                raise RuntimeError(
                    "canonical page digest collides with another exact preimage"
                )
        return _canonical_trace("CANONICAL_SEAL", fixture), fixture, None
    return None


def _apply_reference_action(
    state: _DurableState,
    fixture: _CanonicalFixture,
    page: PreparedCanonicalPage | None,
    operation: str,
) -> None:
    if operation in {
        "CANONICAL_ALLOCATE",
        "REDUNDANT_CANONICAL_ALLOCATE",
    }:
        state.claims.add(fixture.value_sha256)
        return
    if operation == "CANONICAL_PAGE":
        assert page is not None
        state.pages[page.page_sha256] = page.page_bytes
        return
    if operation == "CANONICAL_SEAL":
        state.sealed[fixture.value_sha256] = fixture.receipt
        return
    raise AssertionError(operation)


def _reference_run(
    fixtures: Sequence[_CanonicalFixture],
    state: _DurableState,
    *,
    stage_operation: str = "BUILD_CATALOG",
) -> tuple[_TraceEntry, ...]:
    trace: list[_TraceEntry] = []
    while not state.stage_done:
        selected = _reference_next(fixtures, state)
        if selected is not None:
            entry, fixture, page = selected
            trace.append(entry)
            _apply_reference_action(state, fixture, page, entry[0])
            continue
        state.stage_commits += 1
        state.stage_cursor = state.stage_commits.to_bytes(8, "big")
        terminal = state.stage_commits >= state.stage_batch_limit
        rows = 0 if terminal else 128
        trace.append((stage_operation, None, None, rows, terminal))
        state.output_rows += rows
        for fixture in fixtures:
            if state.consumer_batches[fixture.value_sha256] == state.stage_commits:
                state.claims.discard(fixture.value_sha256)
                state.consumed_values.add(fixture.value_sha256)
        state.stage_done = terminal
    return tuple(trace)


def _without_claim_churn(trace: Sequence[_TraceEntry]) -> tuple[_TraceEntry, ...]:
    return tuple(entry for entry in trace if entry[0] != "REDUNDANT_CANONICAL_ALLOCATE")


class _FakeConnector:
    def __init__(self, state: _DurableState) -> None:
        self._state = state

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield
        if self._state.lose_next_commit_response:
            self._state.lose_next_commit_response = False
            raise ConnectionError("injected committed response loss")

    @contextmanager
    def read_transaction(self) -> Iterator[None]:
        yield

    def fetch_one(
        self,
        query: str,
        parameters: Sequence[object] = (),
    ) -> tuple[object, ...]:
        assert len(parameters) == 2
        if "catalog_publication_checkpoints" in query:
            return (
                self._state.stage_cursor,
                "COMPLETE" if self._state.stage_done else "OPEN",
            )
        assert "operational_canonical_value_uploads" in query
        generation = int(cast(int, parameters[0]))
        value = bytes(cast(bytes, parameters[1]))
        return (generation, value) if value in self._state.claims else ()

    def close(self) -> None:
        return None


class _ProjectionPlan:
    def __init__(
        self,
        authority: object,
        fixtures: Sequence[_CanonicalFixture],
        *,
        validation: bool,
        consumer_batches: dict[bytes, int],
    ) -> None:
        self.authority = authority
        self.publication_count = len(fixtures)
        self.child_count = 0
        self.validation = validation
        self._fixtures = tuple(fixtures)
        self._consumer_batches = dict(consumer_batches)
        self._uploads: list[CanonicalValueUploadPlan] = []
        self.closed = False
        self.close_count = 0

    def iter_canonical_value_plans(self) -> Iterator[CanonicalValueUploadPlan]:
        if self.closed:
            raise RuntimeError("projection plan is closed")
        for fixture in self._fixtures:
            upload = CanonicalValueUploadPlan.from_parts(
                fixture.domain,
                (fixture.payload,),
            )
            assert upload.value_sha256 == fixture.value_sha256
            self._uploads.append(upload)
            yield upload

    def _canonical_consumer_cursor(self, value_sha256: bytes) -> bytes:
        return self._consumer_batches[value_sha256].to_bytes(8, "big")

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.close_count += 1
        for upload in self._uploads:
            upload.close()


class _NoopActivationAdapter:
    def begin(self, *_args: object, **_kwargs: object) -> Any:
        raise AssertionError("catalog preparation called activation begin")

    def activate_page(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog preparation called activation page")

    def seal(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog preparation called activation seal")

    def reconcile_page(self, *_args: object, **_kwargs: object) -> Any:
        raise AssertionError("catalog preparation called activation reconcile")

    def complete(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog preparation called activation complete")


class _OptimizedHarness:
    def __init__(
        self,
        fixtures: Sequence[_CanonicalFixture],
        state: _DurableState,
        *,
        action: publication._Action = publication._Action.BUILD_CATALOG,
    ) -> None:
        self.fixtures = tuple(fixtures)
        self.by_value = {fixture.value_sha256: fixture for fixture in self.fixtures}
        self.state = state
        self.action = action
        self.trace: list[_TraceEntry] = []
        self.plans: list[_ProjectionPlan] = []
        self.materialized: list[bytes] = []
        self.validated: list[bytes] = []
        self.issue_index = 0

    @property
    def stage_name(self) -> bytes:
        return {
            publication._Action.BUILD_CATALOG: b"BUILD_CATALOG_PROJECTION",
            publication._Action.VALIDATE_CATALOG: b"VALIDATE_CATALOG_PROJECTION",
            publication._Action.BUILD_ARTIFACT_INPUT: b"BUILD_ARTIFACT_INPUT",
            publication._Action.VALIDATE_ARTIFACT_INPUT: (
                b"VALIDATE_ARTIFACT_INPUT_DELTA"
            ),
        }[self.action]

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_from_parts = CanonicalValueUploadPlan.from_parts
        original_owned_resource = publication._owned_resource

        def counted_from_parts(
            _cls: type[CanonicalValueUploadPlan],
            digest_domain: str,
            parts: Iterable[bytes],
        ) -> CanonicalValueUploadPlan:
            upload = original_from_parts(digest_domain, parts)
            self.materialized.append(upload.value_sha256)
            return upload

        monkeypatch.setattr(
            CanonicalValueUploadPlan,
            "from_parts",
            classmethod(counted_from_parts),
        )

        def owned_resource(payload: object) -> object:
            if (
                isinstance(payload, publication._CandidateWork)
                and payload.resource_owner is not None
            ):
                return payload.resource_owner
            return original_owned_resource(payload)

        monkeypatch.setattr(publication, "_owned_resource", owned_resource)

        def prepare(
            _connector: object,
            *,
            backend: str,
            authority: object,
        ) -> _ProjectionPlan:
            assert backend == "sqlite"
            plan = _ProjectionPlan(
                authority,
                self.fixtures,
                validation=self.action
                in {
                    publication._Action.VALIDATE_CATALOG,
                    publication._Action.VALIDATE_ARTIFACT_INPUT,
                },
                consumer_batches=self.state.consumer_batches,
            )
            self.plans.append(plan)
            return plan

        monkeypatch.setattr(
            PublicationCandidateRepository,
            "prepare_catalog_projection",
            staticmethod(prepare),
        )
        monkeypatch.setattr(
            PublicationCandidateRepository,
            "prepare_catalog_projection_validation",
            staticmethod(prepare),
        )
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "prepare_artifact_input_projection",
            staticmethod(prepare),
        )
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "prepare_artifact_input_validation",
            staticmethod(prepare),
        )

        def load_sealed(
            _connector: object,
            *,
            value_sha256: bytes,
        ) -> CanonicalValueReadReceipt | None:
            if value_sha256 in self.state.partial_values:
                raise CanonicalValuePartialFamilyError("injected partial family")
            return self.state.sealed.get(value_sha256)

        def load_page(
            _connector: object,
            *,
            page_sha256: bytes,
        ) -> object | None:
            page_bytes = self.state.pages.get(page_sha256)
            return (
                None if page_bytes is None else SimpleNamespace(page_bytes=page_bytes)
            )

        monkeypatch.setattr(publication, "load_sealed_value_identity", load_sealed)
        monkeypatch.setattr(publication, "load_page_family", load_page)

        def stream_and_validate(
            _work: object,
            *,
            value_sha256: bytes,
            consume_provisional: Callable[[bytes], None],
        ) -> CanonicalValueReadReceipt:
            self.validated.append(value_sha256)
            payload = self.state.sealed_payload_overrides.get(
                value_sha256,
                self.state.payloads[value_sha256],
            )
            for offset in range(0, len(payload), 17):
                consume_provisional(payload[offset : offset + 17])
            return self.state.sealed[value_sha256]

        monkeypatch.setattr(
            CanonicalValueRepository,
            "stream_and_validate",
            staticmethod(stream_and_validate),
        )
        monkeypatch.setattr(publication, "_commit_action", self._commit_action)

    def context(self, path: Path) -> RepositoryContext:
        context = RepositoryContext.from_config(
            CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
        )
        connector_factory = cast(Any, lambda: _FakeConnector(self.state))
        return replace(context, SQLConnector=connector_factory)

    def machine(self, path: Path) -> publication.VNextIngestPublication:
        return publication.VNextIngestPublication(self.context(path), clock=lambda: 10)

    def step(
        self,
        machine: publication.VNextIngestPublication,
        *,
        authority: object,
    ) -> None:
        prepared = self.prepare(machine, authority=authority)
        machine.commit_step(_SESSION, prepared)

    def prepare(
        self,
        machine: publication.VNextIngestPublication,
        *,
        authority: object,
    ) -> publication.VNextPreparedPublicationStep:
        batch_key = b"batch-" + self.issue_index.to_bytes(4, "big")
        self.issue_index += 1
        issued = publication.VNextIssuedPublicationStep(
            action=self.action,
            payload=publication._CandidateWork(
                _CANDIDATE_ID,
                batch_key,
                authority,
            ),
            session=_SESSION,
            _token=publication._STEP_TOKEN,
        )
        prepared = machine.prepare_step(
            issued,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=_NoopActivationAdapter(),
        )
        return prepared

    def run(
        self,
        machine: publication.VNextIngestPublication,
        *,
        authority: object,
    ) -> None:
        maximum_steps = 8 + sum(len(fixture.pages) + 3 for fixture in self.fixtures)
        for _index in range(maximum_steps):
            self.step(machine, authority=authority)
            if self.state.stage_done:
                return
        raise AssertionError("optimized canonical stage did not terminate")

    def _commit_action(self, *_args: object, **kwargs: Any) -> object:
        action = cast(publication._Action, kwargs["action"])
        payload = kwargs["payload"]
        if action in {
            publication._Action.CANONICAL_ALLOCATE,
            publication._Action.CANONICAL_PAGE,
            publication._Action.CANONICAL_SEAL,
        }:
            canonical = cast(publication._CanonicalWork, payload)
            fixture = self.by_value[canonical.plan.value_sha256]
            page = canonical.page
            if action is publication._Action.CANONICAL_ALLOCATE:
                fence = canonical.stage_fence
                assert fence is not None
                assert fence.candidate_id == _CANDIDATE_ID
                assert fence.ingest_generation == _SESSION.ingest_generation
                if (
                    self.state.stage_done
                    or self.state.stage_cursor >= fence.first_consumer_cursor
                ):
                    raise RuntimeError(
                        "canonical allocation first consumer already advanced"
                    )
            self.trace.append(_canonical_trace(action.value, fixture, page))
            _apply_reference_action(self.state, fixture, page, action.value)
            return SimpleNamespace(row_count=0, replayed=False)
        assert action is self.action
        candidate = cast(publication._CandidateWork, payload)
        self.state.stage_commits += 1
        self.state.stage_cursor = self.state.stage_commits.to_bytes(8, "big")
        terminal = self.state.stage_commits >= self.state.stage_batch_limit
        rows = 0 if terminal else 128
        self.state.stage_done = terminal
        self.state.output_rows += rows
        if action in {
            publication._Action.BUILD_CATALOG,
            publication._Action.BUILD_ARTIFACT_INPUT,
        }:
            for fixture in self.fixtures:
                if (
                    self.state.consumer_batches[fixture.value_sha256]
                    == self.state.stage_commits
                ):
                    self.state.claims.discard(fixture.value_sha256)
                    self.state.consumed_values.add(fixture.value_sha256)
        self.trace.append((action.value, None, None, rows, terminal))
        return PublicationCandidateBatch(
            candidate_id=_CANDIDATE_ID,
            stage=self.stage_name,
            batch_key=candidate.batch_key,
            start_generation=1,
            start_cursor=b"",
            start_processed_count=0,
            next_cursor=b"",
            next_processed_count=rows,
            next_state="COMPLETE" if terminal else "OPEN",
            row_count=rows,
            terminal=terminal,
            committed_generation=1,
            committed_at=self.state.stage_commits,
            replayed=False,
        )


def _run_optimized(
    tmp_path: Path,
    fixtures: Sequence[_CanonicalFixture],
    state: _DurableState,
    *,
    action: publication._Action = publication._Action.BUILD_CATALOG,
) -> _OptimizedHarness:
    harness = _OptimizedHarness(fixtures, state, action=action)
    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        harness.run(
            harness.machine(tmp_path / "canonical-linear.sqlite3"), authority=object()
        )
    return harness


@settings(
    max_examples=30,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    payloads=st.lists(
        st.binary(min_size=0, max_size=96),
        min_size=0,
        max_size=5,
        unique=True,
    ),
    data=st.data(),
)
def test_optimized_stage_is_trace_equivalent_to_retained_reference_oracle(
    tmp_path: Path,
    payloads: list[bytes],
    data: st.DataObject,
) -> None:
    fixtures = _fixtures(payloads)
    stage_batch_limit = data.draw(st.integers(min_value=1, max_value=3))
    initial = _initial_state(fixtures, stage_batch_limit=stage_batch_limit)
    sealed_prefix = data.draw(st.integers(min_value=0, max_value=len(fixtures)))
    for fixture in fixtures[:sealed_prefix]:
        initial.claims.add(fixture.value_sha256)
        initial.pages.update(
            (page.page_sha256, page.page_bytes) for page in fixture.pages
        )
        initial.sealed[fixture.value_sha256] = fixture.receipt
    if sealed_prefix and data.draw(st.booleans()):
        initial.claims.remove(fixtures[sealed_prefix - 1].value_sha256)
    if sealed_prefix < len(fixtures):
        fixture = fixtures[sealed_prefix]
        progress = data.draw(st.sampled_from(("fresh", "claimed", "paged")))
        if progress != "fresh":
            initial.claims.add(fixture.value_sha256)
        if progress == "paged":
            page_count = data.draw(
                st.integers(min_value=0, max_value=len(fixture.pages))
            )
            initial.pages.update(
                (page.page_sha256, page.page_bytes)
                for page in fixture.pages[:page_count]
            )

    expected_state = initial.clone()
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert len(harness.plans) == 1
    assert harness.materialized == [fixture.value_sha256 for fixture in fixtures]
    expected_validated = [
        fixture.value_sha256
        for fixture in fixtures
        for _validation in range(
            2
            if fixture.value_sha256 in initial.sealed
            and fixture.value_sha256 not in initial.claims
            else 1
        )
    ]
    assert harness.validated == expected_validated
    assert harness.plans[0].closed
    assert harness.plans[0].close_count == 1


@pytest.mark.parametrize(
    "action",
    [publication._Action.BUILD_CATALOG, publication._Action.BUILD_ARTIFACT_INPUT],
)
def test_complexity_budget_is_linear_and_removes_consumed_claim_churn(
    tmp_path: Path,
    action: publication._Action,
) -> None:
    fixtures = _fixtures(tuple(f"value-{index}".encode() for index in range(9)))
    initial = _initial_state(fixtures, stage_batch_limit=2)
    expected_state = initial.clone()
    expected_trace = _reference_run(
        fixtures,
        expected_state,
        stage_operation=action.value,
    )
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state, action=action)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert any(entry[0] == "REDUNDANT_CANONICAL_ALLOCATE" for entry in expected_trace)
    assert actual_state.claims == set()
    assert expected_state.claims != actual_state.claims
    assert len(harness.plans) == 1
    assert len(harness.materialized) == len(fixtures)
    assert len(harness.validated) == len(fixtures)
    assert len(set(harness.materialized)) == len(fixtures)
    assert len(set(harness.validated)) == len(fixtures)


def test_empty_stage_reuses_one_plan_across_multiple_bounded_batches(
    tmp_path: Path,
) -> None:
    fixtures: tuple[_CanonicalFixture, ...] = ()
    initial = _initial_state(fixtures, stage_batch_limit=3)
    expected_state = initial.clone()
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state)

    assert tuple(harness.trace) == expected_trace
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert harness.materialized == []
    assert harness.validated == []
    assert len(harness.plans) == 1
    assert harness.plans[0].closed


def test_partially_sealed_prefix_preserves_exact_remaining_action_trace(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"already-sealed", b"partially-uploaded"))
    initial = _initial_state(fixtures, stage_batch_limit=2)
    sealed = fixtures[0]
    partial = fixtures[1]
    initial.claims.update((sealed.value_sha256, partial.value_sha256))
    initial.pages.update((page.page_sha256, page.page_bytes) for page in sealed.pages)
    initial.sealed[sealed.value_sha256] = sealed.receipt
    initial.pages[partial.pages[0].page_sha256] = partial.pages[0].page_bytes

    expected_state = initial.clone()
    legacy_trace = _reference_run(fixtures, expected_state)
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state)

    assert tuple(harness.trace) == _without_claim_churn(legacy_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert harness.validated == [fixture.value_sha256 for fixture in fixtures]
    assert len(harness.plans) == 1


@pytest.mark.parametrize(
    "action",
    [
        publication._Action.VALIDATE_CATALOG,
        publication._Action.VALIDATE_ARTIFACT_INPUT,
    ],
)
def test_reachable_validation_validates_sealed_values_without_allocating_claims(
    tmp_path: Path,
    action: publication._Action,
) -> None:
    fixtures = _fixtures(tuple(f"validated-{index}".encode() for index in range(7)))
    initial = _initial_state(fixtures, stage_batch_limit=2)
    for fixture in fixtures:
        initial.pages.update(
            (page.page_sha256, page.page_bytes) for page in fixture.pages
        )
        initial.sealed[fixture.value_sha256] = fixture.receipt
        initial.consumed_values.add(fixture.value_sha256)
    expected_state = initial.clone()
    legacy_trace = _reference_run(
        fixtures,
        expected_state,
        stage_operation=action.value,
    )
    actual_state = initial.clone()
    harness = _run_optimized(
        tmp_path,
        fixtures,
        actual_state,
        action=action,
    )

    assert tuple(harness.trace) == _without_claim_churn(legacy_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert actual_state.claims == set()
    assert sum(
        entry[0] == "REDUNDANT_CANONICAL_ALLOCATE" for entry in legacy_trace
    ) >= len(fixtures)
    assert all(entry[0] == action.value for entry in harness.trace)
    assert harness.materialized == [fixture.value_sha256 for fixture in fixtures]
    assert harness.validated == [fixture.value_sha256 for fixture in fixtures]
    assert len(harness.plans) == 1


def test_multi_page_value_keeps_exact_page_action_trace_with_linear_plan_work(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"m" * (3 * 32768 + 17),))
    assert len(fixtures[0].pages) > 1
    initial = _initial_state(fixtures, stage_batch_limit=2)
    expected_state = initial.clone()
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert sum(entry[0] == "CANONICAL_PAGE" for entry in harness.trace) == len(
        fixtures[0].pages
    )
    assert harness.materialized == [fixtures[0].value_sha256]
    assert harness.validated == [fixtures[0].value_sha256]
    assert len(harness.plans) == 1


def test_sealed_value_without_current_claim_is_revalidated_after_allocation(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"sealed-before-current-generation",))
    initial = _initial_state(fixtures)
    fixture = fixtures[0]
    initial.pages.update((page.page_sha256, page.page_bytes) for page in fixture.pages)
    initial.sealed[fixture.value_sha256] = fixture.receipt
    expected_state = initial.clone()
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = initial.clone()
    harness = _run_optimized(tmp_path, fixtures, actual_state)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert [entry[0] for entry in harness.trace] == [
        "CANONICAL_ALLOCATE",
        "BUILD_CATALOG",
    ]
    assert harness.validated == [fixture.value_sha256, fixture.value_sha256]


@pytest.mark.parametrize("mutation", ["disappeared", "changed"])
def test_sealed_observation_fails_closed_if_fresh_receipt_changes(
    tmp_path: Path,
    mutation: str,
) -> None:
    fixtures = _fixtures((b"sealed-receipt-mutation",))
    fixture = fixtures[0]
    actual_state = _initial_state(fixtures)
    actual_state.pages.update(
        (page.page_sha256, page.page_bytes) for page in fixture.pages
    )
    actual_state.sealed[fixture.value_sha256] = fixture.receipt
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / f"sealed-memo-{mutation}.sqlite3")
        harness.step(machine, authority=authority)
        assert harness.validated == [fixture.value_sha256]
        assert actual_state.claims == {fixture.value_sha256}

        if mutation == "disappeared":
            del actual_state.sealed[fixture.value_sha256]
        else:
            actual_state.sealed[fixture.value_sha256] = replace(
                fixture.receipt,
                root_page_sha256=b"x" * 32,
            )

        with pytest.raises(
            RuntimeError,
            match="sealed canonical identity changed after observation",
        ):
            harness.step(machine, authority=authority)

    assert [entry[0] for entry in harness.trace] == ["CANONICAL_ALLOCATE"]
    assert harness.validated == [fixture.value_sha256]
    assert len(harness.plans) == 1
    assert harness.plans[0].closed
    assert harness.plans[0].close_count == 1


def test_same_receipt_preimage_drift_is_revalidated_and_rejected(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"sealed-preimage-mutation",))
    fixture = fixtures[0]
    actual_state = _initial_state(fixtures)
    actual_state.pages.update(
        (page.page_sha256, page.page_bytes) for page in fixture.pages
    )
    actual_state.sealed[fixture.value_sha256] = fixture.receipt
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "sealed-preimage-drift.sqlite3")
        harness.step(machine, authority=authority)
        actual_state.sealed_payload_overrides[fixture.value_sha256] = b"drifted"

        with pytest.raises(
            RuntimeError,
            match="sealed canonical identity differs from the plan's exact preimage",
        ):
            harness.step(machine, authority=authority)

    assert [entry[0] for entry in harness.trace] == ["CANONICAL_ALLOCATE"]
    assert harness.validated == [fixture.value_sha256, fixture.value_sha256]
    assert len(harness.plans) == 1
    assert harness.plans[0].closed
    assert harness.plans[0].close_count == 1


@pytest.mark.parametrize("corruption", ["partial", "page", "sealed-preimage"])
def test_corruption_remains_fail_closed_and_retires_cached_plan(
    tmp_path: Path,
    corruption: str,
) -> None:
    fixtures = _fixtures((b"corruption-target",))
    fixture = fixtures[0]
    initial = _initial_state(fixtures)
    initial.claims.add(fixture.value_sha256)
    if corruption == "partial":
        initial.partial_values.add(fixture.value_sha256)
    elif corruption == "page":
        initial.pages[fixture.pages[0].page_sha256] = b"digest collision"
    else:
        initial.pages.update(
            (page.page_sha256, page.page_bytes) for page in fixture.pages
        )
        initial.sealed[fixture.value_sha256] = fixture.receipt
        initial.sealed_payload_overrides[fixture.value_sha256] = b"different bytes"

    with pytest.raises(RuntimeError) as reference_error:
        _reference_next(fixtures, initial.clone())

    actual_state = initial.clone()
    harness = _OptimizedHarness(fixtures, actual_state)
    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / f"canonical-{corruption}.sqlite3")
        with pytest.raises(RuntimeError) as optimized_error:
            harness.step(machine, authority=object())

    assert str(optimized_error.value) == str(reference_error.value)
    assert len(harness.plans) == 1
    assert harness.plans[0].closed
    assert harness.plans[0].close_count == 1
    assert harness.trace == []


def test_committed_response_loss_restarts_from_durable_state_without_trace_drift(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"response-loss",))
    expected_state = _initial_state(fixtures, stage_batch_limit=2)
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = _initial_state(fixtures, stage_batch_limit=2)
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        first = harness.machine(tmp_path / "canonical-response-loss.sqlite3")
        harness.step(first, authority=authority)
        harness.step(first, authority=authority)
        actual_state.lose_next_commit_response = True
        with pytest.raises(ConnectionError, match="committed response loss"):
            harness.step(first, authority=authority)

        assert fixtures[0].value_sha256 in actual_state.sealed
        assert len(harness.plans) == 1
        del first
        gc.collect()
        assert harness.plans[0].closed

        restarted = harness.machine(tmp_path / "canonical-response-loss.sqlite3")
        harness.run(restarted, authority=authority)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert len(harness.plans) == 2
    assert all(plan.closed for plan in harness.plans)
    assert harness.materialized == [
        fixtures[0].value_sha256,
        fixtures[0].value_sha256,
    ]
    assert harness.validated == [fixtures[0].value_sha256]


def test_committed_response_loss_reuses_cache_after_fresh_durable_observation(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"same-process-response-loss",))
    expected_state = _initial_state(fixtures, stage_batch_limit=2)
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = _initial_state(fixtures, stage_batch_limit=2)
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-same-process-loss.sqlite3")
        harness.step(machine, authority=authority)
        harness.step(machine, authority=authority)
        actual_state.lose_next_commit_response = True
        with pytest.raises(ConnectionError, match="committed response loss"):
            harness.step(machine, authority=authority)

        assert len(harness.plans) == 1
        assert not harness.plans[0].closed
        harness.run(machine, authority=authority)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert len(harness.plans) == 1
    assert harness.plans[0].closed
    assert harness.materialized == [fixtures[0].value_sha256]
    assert harness.validated == [fixtures[0].value_sha256]


def test_restart_after_first_consumer_commit_skips_consumed_claim_reallocation(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures(tuple(f"restart-{index}".encode() for index in range(6)))
    expected_state = _initial_state(fixtures, stage_batch_limit=2)
    legacy_trace = _reference_run(fixtures, expected_state)
    actual_state = _initial_state(fixtures, stage_batch_limit=2)
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        first = harness.machine(tmp_path / "canonical-consumer-restart.sqlite3")
        while actual_state.stage_commits == 0:
            harness.step(first, authority=authority)
        assert not actual_state.stage_done
        assert actual_state.consumed_values
        assert any(
            value not in actual_state.claims for value in actual_state.consumed_values
        )

        del first
        gc.collect()
        assert len(harness.plans) == 1
        assert harness.plans[0].closed

        restart_trace_offset = len(harness.trace)
        restarted = harness.machine(tmp_path / "canonical-consumer-restart.sqlite3")
        harness.run(restarted, authority=authority)

    assert tuple(harness.trace) == _without_claim_churn(legacy_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert all(
        entry[0] == publication._Action.BUILD_CATALOG.value
        for entry in harness.trace[restart_trace_offset:]
    )
    assert actual_state.claims == set()
    expected_values = [fixture.value_sha256 for fixture in fixtures]
    assert harness.materialized == expected_values + expected_values
    assert harness.validated == expected_values + expected_values
    assert len(harness.plans) == 2
    assert all(plan.closed for plan in harness.plans)


def test_authority_change_retires_old_cache_before_deriving_replacement(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"authority-change",))
    expected_state = _initial_state(fixtures)
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = _initial_state(fixtures)
    harness = _OptimizedHarness(fixtures, actual_state)

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-authority-change.sqlite3")
        harness.step(machine, authority=object())
        assert len(harness.plans) == 1
        assert not harness.plans[0].closed
        replacement_authority = object()
        harness.run(machine, authority=replacement_authority)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert len(harness.plans) == 2
    assert all(plan.closed for plan in harness.plans)


def test_retired_cache_waits_for_all_outstanding_prepared_borrowers(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"outstanding-borrowers",))
    expected_state = _initial_state(fixtures)
    expected_trace = _reference_run(fixtures, expected_state)
    actual_state = _initial_state(fixtures)
    harness = _OptimizedHarness(fixtures, actual_state)
    first_authority = object()
    replacement_authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-outstanding-borrowers.sqlite3")
        first = harness.prepare(machine, authority=first_authority)
        second = harness.prepare(machine, authority=first_authority)
        assert len(harness.plans) == 2
        assert not harness.plans[0].closed
        assert not harness.plans[1].closed

        machine.commit_step(_SESSION, first)
        assert harness.plans[0].closed
        assert harness.plans[0].close_count == 1
        replacement = harness.prepare(machine, authority=replacement_authority)
        assert len(harness.plans) == 3
        assert not harness.plans[1].closed

        second.close()
        assert harness.plans[1].closed
        assert harness.plans[1].close_count == 1

        replacement.close()
        assert not harness.plans[2].closed
        harness.run(machine, authority=replacement_authority)

    assert tuple(harness.trace) == _without_claim_churn(expected_trace)
    assert actual_state.observable_snapshot() == expected_state.observable_snapshot()
    assert all(plan.closed for plan in harness.plans)
    assert all(plan.close_count == 1 for plan in harness.plans)


def test_garbage_collected_prepared_step_releases_cache_lease(tmp_path: Path) -> None:
    fixtures = _fixtures((b"abandoned-prepared",))
    actual_state = _initial_state(fixtures)
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-abandoned-prepared.sqlite3")
        abandoned = harness.prepare(machine, authority=authority)
        assert len(harness.plans) == 1

        del abandoned
        gc.collect()
        harness.run(machine, authority=authority)

    assert len(harness.plans) == 1
    assert harness.plans[0].closed
    assert harness.plans[0].close_count == 1


def test_closed_prepared_step_does_not_retain_discarded_facade_cache(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"closed-prepared",))
    harness = _OptimizedHarness(fixtures, _initial_state(fixtures))

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-closed-prepared.sqlite3")
        prepared = harness.prepare(machine, authority=object())
        prepared.close()
        assert not harness.plans[0].closed

        del machine
        gc.collect()

        assert harness.plans[0].closed
        assert harness.plans[0].close_count == 1
        del prepared


def test_cache_closes_active_and_parent_plans_if_page_iterator_close_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixtures = _fixtures((b"exceptional-page-close",))
    parent = _ProjectionPlan(
        object(),
        fixtures,
        validation=False,
        consumer_batches={fixtures[0].value_sha256: 1},
    )
    cache = publication._PublicationPlanCache(
        action=publication._Action.BUILD_CATALOG,
        authority=parent.authority,
        plan=cast(Any, parent),
    )
    active = cache.current_canonical_plan()
    assert active is not None

    class FailingCloseIterator:
        def __init__(self) -> None:
            self.returned = False

        def __iter__(self) -> Self:
            return self

        def __next__(self) -> PreparedCanonicalPage:
            if self.returned:
                raise StopIteration
            self.returned = True
            return fixtures[0].pages[0]

        def close(self) -> None:
            raise OSError("injected page iterator close failure")

    iterator = FailingCloseIterator()
    monkeypatch.setattr(
        CanonicalValueUploadPlan,
        "iter_pages",
        lambda _plan: iterator,
    )
    assert cache.current_canonical_page(active) is fixtures[0].pages[0]

    with pytest.raises(OSError, match="page iterator close failure"):
        cache.retire()

    assert parent.closed
    assert parent.close_count == 1
    with pytest.raises(ValueError, match="upload plan is closed"):
        active._require_open()


def test_delayed_prepared_allocation_cannot_recreate_consumed_claim(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures((b"delayed-allocation",))
    fixture = fixtures[0]
    actual_state = _initial_state(fixtures)
    actual_state.pages.update(
        (page.page_sha256, page.page_bytes) for page in fixture.pages
    )
    actual_state.sealed[fixture.value_sha256] = fixture.receipt
    harness = _OptimizedHarness(fixtures, actual_state)
    authority = object()

    with pytest.MonkeyPatch.context() as monkeypatch:
        harness.install(monkeypatch)
        machine = harness.machine(tmp_path / "canonical-delayed-allocation.sqlite3")
        first = harness.prepare(machine, authority=authority)
        delayed = harness.prepare(machine, authority=authority)

        machine.commit_step(_SESSION, first)
        consumer = harness.prepare(machine, authority=authority)
        machine.commit_step(_SESSION, consumer)
        assert actual_state.stage_done
        assert actual_state.claims == set()

        with pytest.raises(
            RuntimeError,
            match="canonical allocation first consumer already advanced",
        ):
            machine.commit_step(_SESSION, delayed)

    assert [entry[0] for entry in harness.trace] == [
        "CANONICAL_ALLOCATE",
        "BUILD_CATALOG",
    ]
    assert actual_state.claims == set()
    assert fixture.value_sha256 in actual_state.consumed_values
    assert all(plan.closed for plan in harness.plans)
