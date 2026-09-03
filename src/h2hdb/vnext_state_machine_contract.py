"""Closed catalog lifecycle transition and mutation-site specification.

The database schema constrains stored state values while repository methods
own the legal edges between those values.  Keeping those two facts in separate
ad-hoc conditionals made it possible to add a lifecycle write without adding
it to the formal writer evidence.  This module is the small, immutable bridge:

* :data:`CATALOG_STATE_MACHINES` is the complete finite transition algebra;
* :data:`CATALOG_STATE_MUTATION_SITES` names every production INSERT/UPDATE of
  the corresponding state, terminal marker, or publication certificate; and
* :func:`require_catalog_state_mutation` binds each production write to its
  exact registered site and returns the validated SQL state/timestamp values;
  and
* :func:`validate_catalog_state_machine_contract` checks the wheel-resident
  generated schema against the algebra for both backends.

Source inspection remains a test/build concern.  The production artifact only
ships stable symbols and validates the generated provider, so importing h2hdb
does not depend on a checkout or on ``verification/`` files.
"""

from __future__ import annotations

__all__ = [
    "CATALOG_STATE_MACHINE_GATE_RELATIONS",
    "CATALOG_STATE_MACHINE_TRANSITION_GATES",
    "CATALOG_STATE_MACHINE_WRITER_ENTRYPOINTS",
    "CATALOG_STATE_MACHINES",
    "CATALOG_STATE_MUTATION_SITES",
    "CatalogStateMachine",
    "CatalogStateMachineContractError",
    "CatalogStateTimestampSink",
    "CatalogStateMutationSite",
    "ValidatedCatalogStateMutation",
    "catalog_state_snapshot_is_valid",
    "catalog_transition_is_valid",
    "require_catalog_state_mutation",
    "require_catalog_transition",
    "validate_catalog_state_machine_contract",
]

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


class CatalogStateMachineContractError(ValueError):
    """The closed lifecycle algebra or generated schema is inconsistent."""


@dataclass(frozen=True, slots=True)
class CatalogStateMachine:
    """One finite lifecycle, including exact optional-timestamp presence."""

    name: str
    states: frozenset[str]
    transitions: frozenset[tuple[str | None, str]]
    timestamp_attribute: str | None = None
    timestamp_states: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if (
            not self.name
            or not self.states
            or any(not state or not state.isascii() for state in self.states)
        ):
            raise CatalogStateMachineContractError(
                "catalog state-machine identity or state domain is malformed"
            )
        if not self.transitions or any(
            (previous is not None and previous not in self.states)
            or successor not in self.states
            for previous, successor in self.transitions
        ):
            raise CatalogStateMachineContractError(
                f"catalog state machine {self.name!r} has an out-of-domain edge"
            )
        initial = {
            successor for previous, successor in self.transitions if previous is None
        }
        if len(initial) != 1:
            raise CatalogStateMachineContractError(
                f"catalog state machine {self.name!r} needs one initial edge"
            )
        if self.timestamp_states - self.states:
            raise CatalogStateMachineContractError(
                f"catalog state machine {self.name!r} has an unknown timestamp state"
            )
        if (self.timestamp_attribute is None) != (not self.timestamp_states):
            raise CatalogStateMachineContractError(
                f"catalog state machine {self.name!r} has an incomplete timestamp rule"
            )


@dataclass(frozen=True, slots=True)
class CatalogStateTimestampSink:
    """One derived timestamp forwarded to a separate normalized INSERT."""

    function: str
    keyword: str
    table: str
    operation: str
    column: str

    def __post_init__(self) -> None:
        if (
            not self.function
            or not self.keyword
            or not self.table.startswith("catalog_")
            or self.operation != "INSERT"
            or not self.column
            or not all(
                value.isascii()
                for value in (self.function, self.keyword, self.table, self.column)
            )
        ):
            raise CatalogStateMachineContractError(
                "catalog lifecycle timestamp sink is malformed"
            )


@dataclass(frozen=True, slots=True)
class CatalogStateMutationSite:
    """One source-level INSERT/UPDATE of catalog lifecycle authority."""

    site_id: str
    module: str
    function: str
    table: str
    operation: str
    machine: str
    transitions: frozenset[tuple[str | None, str]]
    state_column: str | None = None
    timestamp_column: str | None = None
    marker: bool = False
    timestamp_sink: CatalogStateTimestampSink | None = None

    def __post_init__(self) -> None:
        if (
            not self.site_id
            or not self.site_id.isascii()
            or not self.module.startswith("h2hdb.")
            or not self.function
            or not self.table.startswith("catalog_")
            or self.operation not in {"INSERT", "UPDATE"}
            or type(self.marker) is not bool
        ):
            raise CatalogStateMachineContractError(
                "catalog state mutation site is malformed"
            )
        machine = _MACHINES_BY_NAME.get(self.machine)
        if (
            machine is None
            or not self.transitions
            or not self.transitions <= machine.transitions
        ):
            raise CatalogStateMachineContractError(
                "catalog state mutation site declares an illegal lifecycle edge"
            )
        columns = (self.state_column, self.timestamp_column)
        if any(
            value is not None and (not value or not value.isascii())
            for value in columns
        ) or self.marker == any(value is not None for value in columns):
            raise CatalogStateMachineContractError(
                "catalog state mutation site has an invalid SQL binding shape"
            )
        enters_timestamp_state = machine.timestamp_attribute is not None and any(
            next_state in machine.timestamp_states
            for _previous, next_state in self.transitions
        )
        if self.timestamp_sink is not None and (
            not self.marker or self.timestamp_column is not None
        ):
            raise CatalogStateMachineContractError(
                "catalog lifecycle timestamp sink is not attached to a marker"
            )
        if self.marker and enters_timestamp_state != (self.timestamp_sink is not None):
            raise CatalogStateMachineContractError(
                "catalog lifecycle timestamp-bearing marker lacks its derived sink"
            )


@dataclass(frozen=True, slots=True)
class ValidatedCatalogStateMutation:
    """Site-bound transition values that production writers pass to SQL."""

    site: CatalogStateMutationSite
    previous_state: str | None
    next_state: str
    timestamp: int | None

    @property
    def required_timestamp(self) -> int:
        """Return the validated timestamp for a timestamp-bearing edge."""

        if self.timestamp is None:
            raise CatalogStateMachineContractError(
                f"catalog lifecycle mutation site {self.site.site_id!r} "
                "has no registered timestamp"
            )
        return self.timestamp


CATALOG_STATE_MACHINES: tuple[CatalogStateMachine, ...] = (
    CatalogStateMachine(
        "source_build",
        frozenset({"OPEN", "SEALED", "ABANDONED"}),
        frozenset(
            {
                (None, "OPEN"),
                ("OPEN", "SEALED"),
                ("OPEN", "ABANDONED"),
            }
        ),
        "sealed_at",
        frozenset({"SEALED"}),
    ),
    CatalogStateMachine(
        "analysis_run",
        frozenset({"OPEN", "COMPLETE", "ABANDONED"}),
        frozenset(
            {
                (None, "OPEN"),
                ("OPEN", "COMPLETE"),
                ("OPEN", "ABANDONED"),
            }
        ),
        "completed_at",
        frozenset({"COMPLETE"}),
    ),
    CatalogStateMachine(
        "analysis_checkpoint",
        frozenset({"OPEN", "COMPLETE"}),
        frozenset(
            {
                (None, "OPEN"),
                ("OPEN", "OPEN"),
                ("OPEN", "COMPLETE"),
            }
        ),
        "updated_at",
        frozenset({"OPEN", "COMPLETE"}),
    ),
    CatalogStateMachine(
        "publication_candidate",
        frozenset({"OPEN", "SEALED"}),
        frozenset({(None, "OPEN"), ("OPEN", "SEALED")}),
    ),
    CatalogStateMachine(
        "publication_checkpoint",
        frozenset({"OPEN", "COMPLETE"}),
        frozenset(
            {
                (None, "OPEN"),
                ("OPEN", "OPEN"),
                ("OPEN", "COMPLETE"),
            }
        ),
        "updated_at",
        frozenset({"OPEN", "COMPLETE"}),
    ),
    CatalogStateMachine(
        "prepared_artifact",
        frozenset({"PENDING", "PREPARED", "COMMITTED"}),
        frozenset(
            {
                (None, "PENDING"),
                ("PENDING", "PREPARED"),
                ("PENDING", "COMMITTED"),
                ("PREPARED", "COMMITTED"),
            }
        ),
    ),
    CatalogStateMachine(
        "publication_finalization_checkpoint",
        frozenset({"OPEN", "COMPLETE"}),
        frozenset(
            {
                (None, "OPEN"),
                ("OPEN", "OPEN"),
                ("OPEN", "COMPLETE"),
            }
        ),
        "updated_at",
        frozenset({"OPEN", "COMPLETE"}),
    ),
    CatalogStateMachine(
        "publication_receipt",
        frozenset({"DB_COMMITTED", "PUBLISHED"}),
        frozenset({(None, "DB_COMMITTED"), ("DB_COMMITTED", "PUBLISHED")}),
        "finalized_at",
        frozenset({"PUBLISHED"}),
    ),
)

_MACHINES_BY_NAME: Mapping[str, CatalogStateMachine] = MappingProxyType(
    {machine.name: machine for machine in CATALOG_STATE_MACHINES}
)
if len(_MACHINES_BY_NAME) != len(CATALOG_STATE_MACHINES):
    raise CatalogStateMachineContractError("catalog state-machine names are repeated")


CATALOG_STATE_MACHINE_GATE_RELATIONS: tuple[str, ...] = (
    "source_build_descriptor",
    "source_build_state",
    "source_build_sealed_at",
    "source_build",
    "analysis_run",
    "analysis_checkpoint",
    "publication_candidate",
    "publication_candidate_projection_seal",
    "publication_checkpoint",
    "publication_batch_receipt",
    "prepared_artifact",
    "publication_receipt",
)

CATALOG_STATE_MACHINE_TRANSITION_GATES: tuple[str, ...] = (
    "analysis_state_component_seal",
    "analysis_checkpoint",
    "analysis_batch_receipt",
    "publication_candidate_projection_seal",
    "publication_checkpoint",
    "publication_batch_receipt",
    "catalog_revision",
)


CATALOG_STATE_MACHINE_WRITER_ENTRYPOINTS: frozenset[str] = frozenset(
    {
        "h2hdb.vnext_source_build_repository.SourceBuildRepository.handoff_root",
        "h2hdb.vnext_source_build_repository.SourceBuildRepository.abandon",
        "h2hdb.vnext_source_build_repository.SourceBuildRepository.assemble_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.begin",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.abandon",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_changed_gallery_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_changed_file_hash_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_file_hash_decision_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.validate_file_hash_decision_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_impacted_gallery_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_impacted_content_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_content_owner_candidate_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.validate_content_owner_candidate_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_content_owner_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.validate_content_owner_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_impacted_gid_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_gid_candidate_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.validate_gid_candidate_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.process_gid_winner_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.validate_gid_winner_batch",
        "h2hdb.vnext_analysis_repository.AnalysisRepository.handoff_snapshot_manifest",
        "h2hdb.vnext_publication_candidate_repository.PublicationCandidateRepository.begin",
        "h2hdb.vnext_publication_candidate_repository.PublicationCandidateRepository.process_selection_batch",
        "h2hdb.vnext_publication_candidate_repository.PublicationCandidateRepository.validate_selection_batch",
        "h2hdb.vnext_publication_candidate_repository.PublicationCandidateRepository.process_catalog_projection_batch",
        "h2hdb.vnext_publication_candidate_repository.PublicationCandidateRepository.validate_catalog_projection_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.process_artifact_input_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.process_artifact_delta_operation_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_artifact_input_delta_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_prepared_artifact_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_create_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_rebuild_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_delete_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_unchanged_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_new_gallery_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_changed_gallery_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_removed_gallery_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.validate_duplicate_loser_batch",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.persist_prepared_artifact",
        "h2hdb.vnext_artifact_preparation_repository.ArtifactPreparationRepository.confirm_prepared_artifact",
        "h2hdb.vnext_artifact_release_repository.ArtifactReleaseRepository.issue_page",
        "h2hdb.vnext_artifact_release_repository.ArtifactReleaseRepository.release_page",
        "h2hdb.vnext_artifact_release_repository.ArtifactReleaseRepository.commit_page",
        "h2hdb.vnext_publication_repository.PublicationRepository.commit",
        "h2hdb.vnext_publication_finalization_repository.PublicationFinalizationRepository.issue_page",
        "h2hdb.vnext_publication_finalization_repository.PublicationFinalizationRepository.release_page",
        "h2hdb.vnext_publication_finalization_repository.PublicationFinalizationRepository.commit_page",
    }
)


CATALOG_STATE_MUTATION_SITES: frozenset[CatalogStateMutationSite] = frozenset(
    {
        CatalogStateMutationSite(
            "source-build.initialize",
            "h2hdb.vnext_manifest_family",
            "ensure_source_build_family",
            "catalog_source_build_states",
            "INSERT",
            "source_build",
            frozenset({(None, "OPEN")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "source-build.seal-timestamp",
            "h2hdb.vnext_manifest_family",
            "ensure_build_manifest_family",
            "catalog_source_build_sealed_ats",
            "INSERT",
            "source_build",
            frozenset({("OPEN", "SEALED")}),
            timestamp_column="sealed_at",
        ),
        CatalogStateMutationSite(
            "source-build.abandon",
            "h2hdb.vnext_source_build_repository",
            "SourceBuildRepository.abandon",
            "catalog_source_build_states",
            "UPDATE",
            "source_build",
            frozenset({("OPEN", "ABANDONED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "source-build.seal-state",
            "h2hdb.vnext_source_build_repository",
            "SourceBuildRepository.assemble_batch",
            "catalog_source_build_states",
            "UPDATE",
            "source_build",
            frozenset({("OPEN", "SEALED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "source-build.recover-abandon",
            "h2hdb.vnext_source_build_repository",
            "_abandon_stale_open_working_build",
            "catalog_source_build_states",
            "UPDATE",
            "source_build",
            frozenset({("OPEN", "ABANDONED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "analysis-run.initialize",
            "h2hdb.vnext_analysis_family",
            "ensure_analysis_run_family",
            "catalog_analysis_run_states",
            "INSERT",
            "analysis_run",
            frozenset({(None, "OPEN")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "analysis-run.complete-timestamp",
            "h2hdb.vnext_analysis_family",
            "insert_analysis_run_completed_at",
            "catalog_analysis_run_completed_ats",
            "INSERT",
            "analysis_run",
            frozenset({("OPEN", "COMPLETE")}),
            timestamp_column="completed_at",
        ),
        CatalogStateMutationSite(
            "analysis-run.transition",
            "h2hdb.vnext_analysis_family",
            "cas_analysis_run_state",
            "catalog_analysis_run_states",
            "UPDATE",
            "analysis_run",
            frozenset({("OPEN", "COMPLETE"), ("OPEN", "ABANDONED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "analysis-checkpoint.initialize",
            "h2hdb.vnext_analysis_repository",
            "_initialize_checkpoint",
            "catalog_analysis_checkpoints",
            "INSERT",
            "analysis_checkpoint",
            frozenset({(None, "OPEN")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "analysis-checkpoint.advance",
            "h2hdb.vnext_analysis_repository",
            "_commit_batch",
            "catalog_analysis_checkpoints",
            "UPDATE",
            "analysis_checkpoint",
            frozenset({("OPEN", "OPEN"), ("OPEN", "COMPLETE")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "publication-candidate.initialize",
            "h2hdb.vnext_publication_family",
            "ensure_publication_candidate_family",
            "catalog_publication_candidates",
            "INSERT",
            "publication_candidate",
            frozenset({(None, "OPEN")}),
            marker=True,
        ),
        CatalogStateMutationSite(
            "publication-checkpoint.initialize",
            "h2hdb.vnext_publication_candidate_repository",
            "_initialize_candidate_checkpoints",
            "catalog_publication_checkpoints",
            "INSERT",
            "publication_checkpoint",
            frozenset({(None, "OPEN")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "publication-checkpoint.advance",
            "h2hdb.vnext_publication_candidate_repository",
            "_commit_candidate_batch",
            "catalog_publication_checkpoints",
            "UPDATE",
            "publication_checkpoint",
            frozenset({("OPEN", "OPEN"), ("OPEN", "COMPLETE")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "publication-candidate.reserve-revision",
            "h2hdb.vnext_publication_candidate_repository",
            "_ensure_reserved_catalog_revision",
            "catalog_revision_descriptors",
            "INSERT",
            "publication_candidate",
            frozenset({(None, "OPEN")}),
            marker=True,
        ),
        CatalogStateMutationSite(
            "publication-candidate.seal",
            "h2hdb.vnext_artifact_preparation_repository",
            "_seal_projection",
            "catalog_publication_candidate_projection_seals",
            "INSERT",
            "publication_candidate",
            frozenset({("OPEN", "SEALED")}),
            marker=True,
        ),
        CatalogStateMutationSite(
            "prepared-artifact.initialize",
            "h2hdb.vnext_artifact_family",
            "ensure_prepared_artifact_family",
            "catalog_prepared_artifacts",
            "INSERT",
            "prepared_artifact",
            frozenset({(None, "PENDING")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "prepared-artifact.transition",
            "h2hdb.vnext_artifact_family",
            "cas_prepared_artifact_state",
            "catalog_prepared_artifacts",
            "UPDATE",
            "prepared_artifact",
            frozenset({("PENDING", "PREPARED"), ("PREPARED", "COMMITTED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "artifact-release.commit",
            "h2hdb.vnext_artifact_release_repository",
            "ArtifactReleaseRepository.commit_page",
            "catalog_prepared_artifacts",
            "UPDATE",
            "prepared_artifact",
            frozenset({("PENDING", "COMMITTED"), ("PREPARED", "COMMITTED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "publication-receipt.anchor",
            "h2hdb.vnext_publication_repository",
            "_insert_publication_commit",
            "catalog_publication_commit_anchors",
            "INSERT",
            "publication_receipt",
            frozenset({(None, "DB_COMMITTED")}),
            marker=True,
        ),
        CatalogStateMutationSite(
            "publication-receipt.commit",
            "h2hdb.vnext_publication_repository",
            "_insert_publication_commit",
            "catalog_publication_commits",
            "INSERT",
            "publication_receipt",
            frozenset({(None, "DB_COMMITTED")}),
            marker=True,
        ),
        CatalogStateMutationSite(
            "publication-finalization.initialize",
            "h2hdb.vnext_publication_finalization_repository",
            "_initialize_finalization_checkpoint",
            "catalog_publication_finalization_checkpoints",
            "INSERT",
            "publication_finalization_checkpoint",
            frozenset({(None, "OPEN")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "publication-finalization.advance",
            "h2hdb.vnext_publication_finalization_repository",
            "_advance_checkpoint",
            "catalog_publication_finalization_checkpoints",
            "UPDATE",
            "publication_finalization_checkpoint",
            frozenset({("OPEN", "OPEN"), ("OPEN", "COMPLETE")}),
            state_column="state",
            timestamp_column="updated_at",
        ),
        CatalogStateMutationSite(
            "publication-finalization.artifact-commit",
            "h2hdb.vnext_publication_finalization_repository",
            "PublicationFinalizationRepository.commit_page",
            "catalog_prepared_artifacts",
            "UPDATE",
            "prepared_artifact",
            frozenset({("PREPARED", "COMMITTED")}),
            state_column="state",
        ),
        CatalogStateMutationSite(
            "publication-receipt.finalize",
            "h2hdb.vnext_publication_finalization_repository",
            "PublicationFinalizationRepository.commit_page",
            "catalog_publication_commit_finalizations",
            "INSERT",
            "publication_receipt",
            frozenset({("DB_COMMITTED", "PUBLISHED")}),
            marker=True,
            timestamp_sink=CatalogStateTimestampSink(
                "_insert_batch_receipt",
                "committed_at",
                "catalog_publication_finalization_batch_stored",
                "INSERT",
                "committed_at",
            ),
        ),
    }
)

_MUTATION_SITES_BY_ID: Mapping[str, CatalogStateMutationSite] = MappingProxyType(
    {site.site_id: site for site in CATALOG_STATE_MUTATION_SITES}
)
if len(_MUTATION_SITES_BY_ID) != len(CATALOG_STATE_MUTATION_SITES):
    raise CatalogStateMachineContractError(
        "catalog lifecycle mutation-site IDs are repeated"
    )


_PHYSICAL_ENUM_RELATIONS: Mapping[str, str] = MappingProxyType(
    {
        "source_build": "source_build_state",
        "analysis_run": "analysis_run_state",
        "analysis_checkpoint": "analysis_checkpoint",
        "publication_checkpoint": "publication_checkpoint",
        "prepared_artifact": "prepared_artifact",
    }
)

_STATE_RELATIONS_WITH_RUNTIME_ENUM: Mapping[str, str] = MappingProxyType(
    {"publication_finalization_checkpoint": "publication_finalization_checkpoint"}
)

_LIFECYCLE_PROJECTIONS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "source_build": (
            "source_build_descriptor",
            "source_build_state",
            "source_build_sealed_at",
        ),
        "analysis_run": (
            "analysis_run_descriptor",
            "analysis_run_state",
            "analysis_run_completed_at",
        ),
        "publication_receipt": (
            "publication_commit",
            "catalog_revision_descriptor",
            "source_revision_descriptor",
            "publication_commit_finalization",
            "publication_finalization_checkpoint",
            "publication_finalization_batch_receipt",
        ),
    }
)

_MARKER_RELATIONS = frozenset(
    {
        "publication_candidate",
        "publication_candidate_projection_seal",
        "catalog_revision_descriptor",
        "publication_commit_anchor",
        "publication_commit",
        "publication_commit_finalization",
    }
)

_STATE_ENUM_PATTERN = re.compile(r"\bstate\s+IN\s*\(([^)]*)\)", re.IGNORECASE)
_SQL_QUOTED_PATTERN = re.compile(r"'([^']+)'")


def _generated_artifact() -> Mapping[str, Any]:
    # Keep the 4 MiB compressed schema resource lazy.  Repository writers use
    # the finite in-module transition algebra without paying the READY audit's
    # schema decode cost merely because the public package was imported.
    from ._generated_vnext_schema import ARTIFACT

    return ARTIFACT


def _machine(name: str) -> CatalogStateMachine:
    try:
        return _MACHINES_BY_NAME[name]
    except KeyError as error:
        raise CatalogStateMachineContractError(
            f"unknown catalog state machine {name!r}"
        ) from error


def catalog_state_snapshot_is_valid(
    machine_name: str,
    *,
    state: str,
    timestamp_present: bool | None,
) -> bool:
    """Return whether one state has the exact optional-timestamp shape."""

    machine = _machine(machine_name)
    if state not in machine.states:
        return False
    if machine.timestamp_attribute is None:
        return timestamp_present is None
    return timestamp_present is (state in machine.timestamp_states)


def catalog_transition_is_valid(
    machine_name: str,
    *,
    previous_state: str | None,
    next_state: str,
    timestamp_present: bool | None,
) -> bool:
    """Classify one edge against the complete finite transition algebra."""

    machine = _machine(machine_name)
    return (
        previous_state,
        next_state,
    ) in machine.transitions and catalog_state_snapshot_is_valid(
        machine_name,
        state=next_state,
        timestamp_present=timestamp_present,
    )


def require_catalog_transition(
    machine_name: str,
    *,
    previous_state: str | None,
    next_state: str,
    timestamp_present: bool | None,
) -> None:
    """Reject an edge absent from the closed catalog transition algebra."""

    if not catalog_transition_is_valid(
        machine_name,
        previous_state=previous_state,
        next_state=next_state,
        timestamp_present=timestamp_present,
    ):
        raise CatalogStateMachineContractError(
            f"catalog state machine {machine_name!r} rejected transition "
            f"{previous_state!r} -> {next_state!r}"
        )


def require_catalog_state_mutation(
    site_id: str,
    *,
    previous_state: str | None,
    next_state: str,
    timestamp: int | None,
) -> ValidatedCatalogStateMutation:
    """Bind one production DML site to its declared lifecycle edge.

    Writers use the returned values as their SQL parameters.  A transition
    legal elsewhere in the same machine is still rejected when it is absent
    from this exact site's closed edge set.
    """

    try:
        site = _MUTATION_SITES_BY_ID[site_id]
    except KeyError as error:
        raise CatalogStateMachineContractError(
            f"unknown catalog lifecycle mutation site {site_id!r}"
        ) from error
    edge = (previous_state, next_state)
    if edge not in site.transitions:
        raise CatalogStateMachineContractError(
            f"catalog lifecycle mutation site {site_id!r} rejected transition "
            f"{previous_state!r} -> {next_state!r}"
        )
    machine = _machine(site.machine)
    if machine.timestamp_attribute is None:
        if timestamp is not None:
            raise CatalogStateMachineContractError(
                f"catalog lifecycle mutation site {site_id!r} rejected an "
                "unregistered timestamp"
            )
        timestamp_present: bool | None = None
    else:
        timestamp_present = timestamp is not None
    require_catalog_transition(
        site.machine,
        previous_state=previous_state,
        next_state=next_state,
        timestamp_present=timestamp_present,
    )
    return ValidatedCatalogStateMutation(
        site,
        previous_state,
        next_state,
        timestamp,
    )


def _semantic_obligation_relations() -> tuple[str, ...]:
    raw = _generated_artifact().get("semantic_obligations")
    if not isinstance(raw, tuple):
        raise CatalogStateMachineContractError(
            "generated semantic-obligation registry is malformed"
        )
    matches = tuple(
        value
        for value in raw
        if isinstance(value, Mapping) and value.get("id") == "catalog.state-machines.v1"
    )
    if len(matches) != 1:
        raise CatalogStateMachineContractError(
            "generated catalog state-machine obligation is not singular"
        )
    contract = matches[0].get("contract")
    relations = contract.get("relations") if isinstance(contract, Mapping) else None
    if not isinstance(relations, list) or not all(
        isinstance(value, str) for value in relations
    ):
        raise CatalogStateMachineContractError(
            "generated catalog state-machine relation closure is malformed"
        )
    return tuple(relations)


def _backend_relations(backend: str) -> Mapping[str, Mapping[str, Any]]:
    backends = _generated_artifact().get("backends")
    payload = backends.get(backend) if isinstance(backends, Mapping) else None
    raw = payload.get("relations") if isinstance(payload, Mapping) else None
    if not isinstance(raw, tuple):
        raise CatalogStateMachineContractError(
            f"generated {backend} relation registry is malformed"
        )
    result: dict[str, Mapping[str, Any]] = {}
    for value in raw:
        name = value.get("relation") if isinstance(value, Mapping) else None
        if not isinstance(name, str) or name in result:
            raise CatalogStateMachineContractError(
                f"generated {backend} relation registry is not closed-world"
            )
        result[name] = value
    return result


def _column_names(relation: Mapping[str, Any]) -> tuple[str, ...]:
    columns = relation.get("columns")
    if not isinstance(columns, tuple) or not all(
        isinstance(column, tuple) and len(column) == 5 and isinstance(column[0], str)
        for column in columns
    ):
        raise CatalogStateMachineContractError(
            "generated lifecycle relation has malformed columns"
        )
    return tuple(column[0] for column in columns)


def _state_enum(relation: Mapping[str, Any]) -> frozenset[str]:
    checks = relation.get("checks")
    if not isinstance(checks, tuple):
        raise CatalogStateMachineContractError(
            "generated lifecycle relation has malformed checks"
        )
    matches: list[str] = []
    for check in checks:
        if (
            not isinstance(check, tuple)
            or len(check) != 2
            or not isinstance(check[1], str)
        ):
            raise CatalogStateMachineContractError(
                "generated lifecycle relation has a malformed check"
            )
        matches.extend(_STATE_ENUM_PATTERN.findall(check[1]))
    if len(matches) != 1:
        raise CatalogStateMachineContractError(
            "generated lifecycle relation lacks one exact state enum check"
        )
    return frozenset(_SQL_QUOTED_PATTERN.findall(matches[0]))


def _require_relation(
    relations: Mapping[str, Mapping[str, Any]],
    name: str,
    *,
    backend: str,
) -> Mapping[str, Any]:
    try:
        return relations[name]
    except KeyError as error:
        raise CatalogStateMachineContractError(
            f"generated {backend} schema omits lifecycle relation {name!r}"
        ) from error


def validate_catalog_state_machine_contract() -> None:
    """Validate the closed algebra against the wheel-resident schema artifact."""

    if len(_MUTATION_SITES_BY_ID) != len(CATALOG_STATE_MUTATION_SITES):
        raise CatalogStateMachineContractError(
            "catalog lifecycle mutation-site IDs are repeated"
        )
    site_identities = tuple(
        (site.module, site.function, site.table, site.operation)
        for site in CATALOG_STATE_MUTATION_SITES
    )
    if len(site_identities) != len(set(site_identities)):
        raise CatalogStateMachineContractError(
            "catalog lifecycle mutation-site identities are repeated"
        )
    for machine in CATALOG_STATE_MACHINES:
        covered_edges = frozenset(
            edge
            for site in CATALOG_STATE_MUTATION_SITES
            if site.machine == machine.name
            for edge in site.transitions
        )
        if covered_edges != machine.transitions:
            raise CatalogStateMachineContractError(
                f"catalog state machine {machine.name!r} has incomplete writer coverage"
            )
    if _semantic_obligation_relations() != CATALOG_STATE_MACHINE_GATE_RELATIONS:
        raise CatalogStateMachineContractError(
            "generated catalog state-machine relation closure drifted"
        )
    for backend in ("sqlite", "mariadb"):
        relations = _backend_relations(backend)
        expected_state_tables = frozenset(
            (
                *_PHYSICAL_ENUM_RELATIONS.values(),
                *_STATE_RELATIONS_WITH_RUNTIME_ENUM.values(),
            )
        )
        actual_state_tables = frozenset(
            name
            for name, relation in relations.items()
            if relation.get("plane") == "data"
            and relation.get("kind") == "table"
            and "state" in _column_names(relation)
        )
        if actual_state_tables != expected_state_tables:
            raise CatalogStateMachineContractError(
                f"generated {backend} catalog state-table registry drifted"
            )
        actual_state_views = frozenset(
            name
            for name, relation in relations.items()
            if relation.get("plane") == "data"
            and relation.get("kind") == "view"
            and "state" in _column_names(relation)
        )
        if actual_state_views != frozenset(_LIFECYCLE_PROJECTIONS):
            raise CatalogStateMachineContractError(
                f"generated {backend} catalog state-view registry drifted"
            )
        for machine_name, relation_name in _PHYSICAL_ENUM_RELATIONS.items():
            relation = _require_relation(relations, relation_name, backend=backend)
            if (
                relation.get("kind") != "table"
                or "state" not in _column_names(relation)
                or _state_enum(relation) != _machine(machine_name).states
            ):
                raise CatalogStateMachineContractError(
                    f"generated {backend} {machine_name!r} state enum drifted"
                )
        for machine_name, relation_name in _STATE_RELATIONS_WITH_RUNTIME_ENUM.items():
            relation = _require_relation(relations, relation_name, backend=backend)
            columns = _column_names(relation)
            if (
                relation.get("kind") != "table"
                or "state" not in columns
                or _machine(machine_name).timestamp_attribute not in columns
            ):
                raise CatalogStateMachineContractError(
                    f"generated {backend} {machine_name!r} runtime state shape drifted"
                )
        for projection_name, dependencies in _LIFECYCLE_PROJECTIONS.items():
            relation = _require_relation(relations, projection_name, backend=backend)
            machine = _machine(projection_name)
            columns = _column_names(relation)
            if (
                relation.get("kind") != "view"
                or "state" not in columns
                or machine.timestamp_attribute not in columns
                or relation.get("view_dependencies") != dependencies
            ):
                raise CatalogStateMachineContractError(
                    f"generated {backend} {projection_name!r} lifecycle view drifted"
                )
        for relation_name in _MARKER_RELATIONS:
            relation = _require_relation(relations, relation_name, backend=backend)
            if relation.get("kind") != "table":
                raise CatalogStateMachineContractError(
                    f"generated {backend} lifecycle marker {relation_name!r} drifted"
                )
