"""Age-based hash-cache observation cleanup policy."""

from __future__ import annotations

from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup.model import CleanupTargetKind
from h2hdb._cleanup.plan import _StaticDeleteSpec, _StaticTargetPlan

_HASH_CACHE_ELIGIBILITY = "r.observed_at <= %s"


def _hash_cache_phases() -> dict[str, tuple[_StaticDeleteSpec, ...]]:
    root = "operational_hash_cache_observations"
    key = ("source_identity_sha256", "fingerprint_sha256")
    return {
        "HC_FILE": (
            _plan._owned_spec(
                "operational_file_hash_caches",
                key,
                root,
                key,
            ),
        ),
        "HC_ROOT": (_plan._owned_spec(root, key, root, key),),
    }


_HASH_CACHE_OBSERVATION_PLAN = _StaticTargetPlan(
    CleanupTargetKind.HASH_CACHE_OBSERVATION,
    "operational_hash_cache_observations",
    ("source_identity_sha256", "fingerprint_sha256"),
    "source_identity_sha256",
    32,
    _HASH_CACHE_ELIGIBILITY,
    _hash_cache_phases(),
)
