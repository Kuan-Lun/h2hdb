"""Counterfactual locator reuse experiment; never installed production behavior.

Compile a small variation of the loaded implementation, preserving all original
write authority checks and exact single-page validation. Exact source matching
fails if the experiment no longer corresponds to the checkout implementation.
The production path is restored on exit. This explores query costs and rejection
behavior; it does not establish a whole-library completion time or justify a
production optimization by itself.
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
import textwrap
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "scripts")]

import ingest_batch_scaling_probe as scaling  # noqa: E402 - dev-only public pipeline.
import ingest_pipeline_probe as pipeline  # noqa: E402 - load evidence registry first.

from h2hdb import vnext_ingest_facade as facade  # noqa: E402 - scoped candidate.
from h2hdb import (  # noqa: E402 - scoped candidate.
    vnext_source_build_repository as source,
)
from h2hdb.vnext_canonical_value_repository import (  # noqa: E402 - existing validator.
    load_and_validate_single_page_canonical_values,
)
from h2hdb.vnext_identity import (  # noqa: E402 - runtime bound.
    CANONICAL_VALUE_CHUNK_BYTES,
)
from h2hdb.vnext_transaction import VNextUnitOfWork  # noqa: E402 - typed experiment.


def replace_once(text: str, old: str, new: str) -> str:
    assert text.count(old) == 1, (old, text.count(old))
    return text.replace(old, new)


def _install(stack: ExitStack) -> None:
    original = source.SourceBuildRepository.resolve_discovery_locator
    code = textwrap.dedent(inspect.getsource(original)).removeprefix("@staticmethod\n")
    code = replace_once(
        code,
        "    now: int,\n) -> ResolvedDiscoveryLocator:",
        "    now: int,\n    existing_only: bool = False,\n) -> ResolvedDiscoveryLocator | None:",
    )
    needle = "    connector = work.connector\n    claim = work.lock_row("
    code = replace_once(
        code,
        needle,
        """    connector = work.connector
    if existing_only:
        if upload_plan.byte_count > CANONICAL_VALUE_CHUNK_BYTES:
            return None
        stable = gallery_key(scope, locator.locator_sha256)
        existing = _load_gallery_identity(
            connector, scope=scope, locator_sha256=locator.locator_sha256,
            stable_key=stable,
        )
        if existing is None:
            return None
        payloads = load_and_validate_single_page_canonical_values(
            connector, references=((locator.locator_sha256, _LOCATOR_DOMAIN_BYTES),),
        )
        if (locator.locator_sha256, _LOCATOR_DOMAIN_BYTES) not in payloads:
            raise SourceBuildConflictError("existing locator has no exact single page")
    claim = work.lock_row(""",
    )
    scope = {
        **source.__dict__,
        "load_and_validate_single_page_canonical_values": load_and_validate_single_page_canonical_values,
        "CANONICAL_VALUE_CHUNK_BYTES": CANONICAL_VALUE_CHUNK_BYTES,
    }
    exec("from __future__ import annotations\n" + code, scope)
    scope["resolve_discovery_locator"].__module__ = original.__module__
    scope["resolve_discovery_locator"].__qualname__ = original.__qualname__
    stack.enter_context(
        patch.object(
            source.SourceBuildRepository,
            "resolve_discovery_locator",
            staticmethod(scope["resolve_discovery_locator"]),
        )
    )

    prepare_method = facade.VNextIngestFacade.prepare_source_step
    code = textwrap.dedent(inspect.getsource(prepare_method)).replace(
        "self.__", "self._VNextIngestFacade__"
    )
    code = replace_once(
        code,
        "            payload = (upload, upload.iter_pages())",
        """            pages = upload.iter_pages()
            if upload.byte_count <= CANONICAL_VALUE_CHUNK_BYTES:
                pages = iter(tuple(pages))
            payload = (upload, pages)""",
    )
    scope = {
        **facade.__dict__,
        "CANONICAL_VALUE_CHUNK_BYTES": CANONICAL_VALUE_CHUNK_BYTES,
    }
    exec("from __future__ import annotations\n" + code, scope)
    stack.enter_context(
        patch.object(
            facade.VNextIngestFacade,
            "prepare_source_step",
            scope["prepare_source_step"],
        )
    )

    commit_method = facade.VNextIngestFacade.commit_source_step
    code = textwrap.dedent(inspect.getsource(commit_method)).replace(
        "self.__", "self._VNextIngestFacade__"
    )
    needle = """            if action in {
                _SourceAction.INITIALIZE,
                _SourceAction.LOCATOR_INITIALIZE,
            }:"""
    code = replace_once(
        code,
        needle,
        """            if action is _SourceAction.LOCATOR_INITIALIZE:
                batch = _require_discovery_batch(machine)
                locator = batch.locators[machine.locator_index]
                upload, _pages = prepared_step._payload
                if upload.byte_count <= CANONICAL_VALUE_CHUNK_BYTES:
                    return SourceBuildRepository.resolve_discovery_locator(
                        work, gate_lease=gate, ingest_turn=turn.ingest_turn,
                        batch=batch, locator=locator, upload_plan=upload,
                        now=now, existing_only=True,
                    )
                return _resume_authority(work, session, now)
            if action is _SourceAction.INITIALIZE:""",
    )
    scope = {
        **facade.__dict__,
        "CANONICAL_VALUE_CHUNK_BYTES": CANONICAL_VALUE_CHUNK_BYTES,
    }
    exec("from __future__ import annotations\n" + code, scope)
    candidate_commit = scope["commit_source_step"]

    code = textwrap.dedent(inspect.getsource(facade._apply_source_outcome))
    needle = """        machine.action = _SourceAction.LOCATOR_ALLOCATE
    elif action is _SourceAction.LOCATOR_ALLOCATE:"""
    code = replace_once(
        code,
        needle,
        """        machine.action = _SourceAction.LOCATOR_ALLOCATE
        if isinstance(outcome, ResolvedDiscoveryLocator):
            synthetic = VNextPreparedSourceStep(
                issued=step._issued, action=_SourceAction.LOCATOR_RESOLVE,
                payload=None, _constructor_token=_PREPARED_SOURCE_STEP_TOKEN,
            )
            return _apply_source_outcome(source, synthetic, outcome)
    elif action is _SourceAction.LOCATOR_ALLOCATE:""",
    )
    # Development prototype reuses existing outcome handling. Production should
    # factor a common helper, not synthesize an opaque protocol step.
    scope = dict(facade.__dict__)
    exec("from __future__ import annotations\n" + code, scope)
    candidate_commit.__globals__["_apply_source_outcome"] = scope[
        "_apply_source_outcome"
    ]
    stack.enter_context(
        patch.object(facade, "_apply_source_outcome", scope["_apply_source_outcome"])
    )
    stack.enter_context(
        patch.object(facade.VNextIngestFacade, "commit_source_step", candidate_commit)
    )


@contextmanager
def candidate() -> Iterator[None]:
    """Install only within this experiment, restoring every original on exit."""
    with ExitStack() as stack:
        _install(stack)
        yield


def reuse_discovery_locator(
    work: VNextUnitOfWork, **kwargs: Any
) -> source.ResolvedDiscoveryLocator | None:
    """Call the candidate's bounded path from independent fault experiments."""
    method = cast(Any, source.SourceBuildRepository.resolve_discovery_locator)
    result = method(work, **kwargs, existing_only=True)
    if result is not None and not isinstance(result, source.ResolvedDiscoveryLocator):
        raise AssertionError("candidate returned an invalid resolution")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "mariadb"), default="sqlite")
    parser.add_argument("--case", type=scaling.parse_case, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    galleries, batch, pages = args.case
    report: dict[str, Any] = {
        "status": "incomplete",
        "provenance": pipeline.source_provenance(),
        "experiment_sources_sha256": scaling.experiment_source_hashes(),
        "notes": "Dev-only monkeypatch, not a production change. Unique synthetic content, public pipeline and final READY. Counts/SQL families, not a wall-clock SLO. One original and one candidate case; no claim of timing stationarity.",
    }
    pipeline.write_report(args.output, report)
    try:
        report["original"] = scaling.run_case(
            args.backend,
            galleries,
            batch,
            pages,
            progress=lambda row: print(
                json.dumps({"variant": "original", **row}), flush=True
            ),
        )
        pipeline.write_report(args.output, report)
        with candidate():
            report["candidate"] = scaling.run_case(
                args.backend,
                galleries,
                batch,
                pages,
                progress=lambda row: print(
                    json.dumps({"variant": "candidate", **row}), flush=True
                ),
            )
        if report["original"]["oracle"] != report["candidate"]["oracle"]:
            raise AssertionError("candidate changed the final public catalog")
        if (
            report["provenance"] != pipeline.source_provenance()
            or report["experiment_sources_sha256"] != scaling.experiment_source_hashes()
        ):
            raise RuntimeError("experiment source changed during the run")
        report["status"] = "completed"
    except BaseException as error:
        report.update(status="failed", error=type(error).__name__)
        raise
    finally:
        pipeline.write_report(args.output, report)


if __name__ == "__main__":
    main()
