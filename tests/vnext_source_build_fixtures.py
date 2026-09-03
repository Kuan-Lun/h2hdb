from __future__ import annotations

from h2hdb.vnext_source_build_repository import _SourceBuildPolicyAuthority

SOURCE_BUILD_POLICY_AUTHORITY = _SourceBuildPolicyAuthority(
    manifest_policy_id=1,
    analysis_policy_id=1,
    artifact_policy_sha256=b"a" * 32,
    display_title_policy_id=1,
    title_sort_policy_id=1,
    operational_policy_id=1,
    artifacts_required=False,
)
