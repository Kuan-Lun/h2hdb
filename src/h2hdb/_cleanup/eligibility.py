"""Immutable absence evidence owned by one current-only cleanup attempt.

This value is not a lease, a durable completion receipt, or an authorization
shortcut. Its owner must freshly lock and validate the exact EXCLUSIVE gate in
every transaction before consulting it, and only adopt returned evidence after
that transaction commits successfully. The public attempt never exports it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..vnext_domains import require_int63, require_uuid16
from ..vnext_maintenance_gate_repository import GateLease, GateMode

_PRESERVED_ABSENCES = frozenset({"CONTENT_BLOB", "FILE_NAME_IDENTITY"})


@dataclass(frozen=True, slots=True)
class CurrentOnlyEligibilityProof:
    owner_token: bytes
    gate_generation: int
    cycle_cutoff_at: int
    absent_targets: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        require_uuid16(self.owner_token, field="eligibility proof owner_token")
        require_int63(self.gate_generation, field="eligibility proof generation")
        require_int63(self.cycle_cutoff_at, field="eligibility proof cutoff")
        if not isinstance(self.absent_targets, frozenset) or not (
            self.absent_targets <= _PRESERVED_ABSENCES
        ):
            raise ValueError("eligibility proof contains an unsupported target")

    @classmethod
    def under_validated_gate(
        cls,
        lease: GateLease,
        cutoff: int,
        prior: CurrentOnlyEligibilityProof | None,
    ) -> CurrentOnlyEligibilityProof:
        """Bind evidence only after the caller's fresh durable gate validation."""

        if lease.mode is not GateMode.EXCLUSIVE or lease.slots != tuple(range(64)):
            raise ValueError("eligibility evidence requires the exact EXCLUSIVE gate")
        if prior is not None and (
            prior.owner_token == lease.owner_token
            and prior.gate_generation == lease.gate_generation
            and prior.cycle_cutoff_at == cutoff
        ):
            return prior
        return cls(lease.owner_token, lease.gate_generation, cutoff)

    def observes_absence(self, target: str) -> CurrentOnlyEligibilityProof:
        if target not in _PRESERVED_ABSENCES or target in self.absent_targets:
            return self
        return replace(self, absent_targets=self.absent_targets | {target})

    def after_committed_cleanup(self, target: str) -> CurrentOnlyEligibilityProof:
        """Retain only absences unaffected by canonical-value deletion.

        Canonical cleanup does not mutate either root relation or any relation
        read by these two eligibility predicates. Supported competing writers
        need SHARED ownership and cannot run inside this live EXCLUSIVE epoch.
        Other cleanup targets can release references, so they invalidate both
        facts even when their reported logical row count is zero. A source
        footprint regression checks this closed, deliberately narrow contract.
        """

        if target == "CANONICAL_VALUE" or not self.absent_targets:
            return self
        return replace(self, absent_targets=frozenset())
