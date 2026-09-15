"""Human descriptions for core ingest telemetry, without adapter responsibilities."""

from __future__ import annotations

_ANALYSIS_ACTIVITIES = {
    "ISSUE": "issuing an analysis work request",
    "PREPARE": "preparing requested analysis work",
    "COMMIT": "saving analysis work results",
    "snapshot_manifest": "saving and sealing the analysis snapshot",
    "changed_gallery": "identifying changed galleries",
    "changed_file_hash": "identifying changed file hashes",
    "file_hash_decision": "evaluating duplicate-page decisions",
    "validate_file_hash_decision": "checking file hash decisions",
    "prepare_file_decision_validation": "preparing file hash validation data",
    "impacted_gallery": "identifying galleries affected by changed decisions",
    "impacted_content": "identifying affected content groups",
    "content_owner_candidate": "preparing candidates for duplicate-content selection",
    "validate_content_owner_candidate": "checking duplicate-content candidates",
    "content_owner": "selecting galleries for duplicate content",
    "validate_content_owner": "checking duplicate-content selections",
    "impacted_gid": "identifying affected gallery IDs",
    "gid_candidate": "preparing candidates for each gallery ID",
    "validate_gid_candidate": "checking gallery-ID candidates",
    "gid_winner": "selecting a gallery for each gallery ID",
    "validate_gid_winner": "checking gallery-ID selections",
    "PREPARE_BATCH": "preparing an analysis batch",
    "PREPARE_SNAPSHOT": "preparing the analysis snapshot",
    "UPLOAD_NEXT": "preparing the next analysis value",
    "UPLOAD_PAGE": "preparing an analysis value page",
    "UPLOAD_ALLOCATE": "reserving storage for an analysis value",
    "UPLOAD_PUT": "saving an analysis value page",
    "UPLOAD_SEAL": "verifying and sealing an analysis value",
    "PROCESS_BATCH": "saving an analysis batch",
    "HANDOFF_SNAPSHOT": "recording the completed analysis snapshot",
    "COMPLETE": "confirming analysis completion",
}
_PUBLICATION_ACTIVITIES = {
    "ISSUE": "issuing a publication work request",
    "PREPARE": "preparing requested publication work",
    "COMMIT": "saving publication work results",
    "BEGIN": "starting catalog publication",
    "BUILD_SELECTION": "selecting catalog entries",
    "VALIDATE_SELECTION": "checking selected catalog entries",
    "BUILD_CATALOG": "building catalog metadata and indexes",
    "VALIDATE_CATALOG": "checking catalog metadata and indexes",
    "BUILD_ARTIFACT_INPUT": "preparing artifact requirements",
    "BUILD_ARTIFACT_DELTA": "identifying artifacts that need changes",
    "VALIDATE_ARTIFACT_INPUT": "checking artifact requirements",
    "ABANDON_SUPERSEDED": "retiring superseded publication work",
    "BEGIN_OPERATIONAL": "starting artifact preparation work",
    "APPEND_OPERATIONAL": "recording artifact preparation work",
    "SEAL_OPERATIONAL": "sealing artifact preparation work",
    "PREPARE_ARTIFACT": "preparing artifacts through the configured adapter",
    "BIND_OPERATIONAL": "recording prepared artifact references",
    "VALIDATE_PREPARED": "checking prepared artifact references",
    "VALIDATE_CREATE": "checking artifact creation decisions",
    "VALIDATE_REBUILD": "checking artifact replacement decisions",
    "VALIDATE_DELETE": "checking artifact removal decisions",
    "VALIDATE_UNCHANGED": "checking unchanged catalog entries",
    "VALIDATE_NEW": "checking new catalog entries",
    "VALIDATE_CHANGED": "checking changed catalog entries",
    "VALIDATE_REMOVED": "checking removed catalog entries",
    "VALIDATE_DUPLICATE": "checking duplicate catalog entries",
    "COMMIT_PUBLICATION": "committing the catalog publication",
    "LIBRARY_ACTIVATION": "coordinating artifact activation with the adapter",
    "FINALIZE": "finalizing catalog publication",
    "RECOVERY": "checking for an interrupted publication",
    "RECOVERY_COMPLETE": "confirming publication recovery",
    "COMPLETE": "confirming catalog publication completion",
    "CANONICAL_BATCH": "saving a batch of catalog values",
    "CANONICAL_ALLOCATE": "reserving storage for a catalog value",
    "CANONICAL_PAGE": "saving a catalog value page",
    "CANONICAL_SEAL": "verifying and sealing a catalog value",
}


def activity(pipeline: str, operation: str) -> str:
    if pipeline == "analysis":
        return _ANALYSIS_ACTIVITIES.get(operation, "processing analysis work")
    if pipeline == "publication":
        return _PUBLICATION_ACTIVITIES.get(operation, "processing publication work")
    return "processing ingest work"


def pipeline_name(pipeline: str) -> str:
    return pipeline if pipeline in {"analysis", "publication"} else "work"


def duration(seconds: float | None) -> str:
    if seconds is None:
        return "unavailable"
    if seconds < 1:
        return f"{max(0, seconds) * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    whole = int(seconds)
    minutes, remainder = divmod(whole, 60)
    if minutes < 60:
        return f"{minutes}m {remainder:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m {remainder:02d}s"


def workload(
    *,
    elapsed: float | None,
    sql_seconds: float,
    connection_seconds: float,
    transaction_seconds: float,
    includes_reused_results: bool,
) -> str:
    parts = [f"elapsed {duration(elapsed)}"]
    database_seconds = sql_seconds + connection_seconds + transaction_seconds
    parts.append(
        f"database work {duration(database_seconds)} "
        f"(queries {duration(sql_seconds)}, connections {duration(connection_seconds)}, "
        f"transaction boundaries {duration(transaction_seconds)})"
    )
    if includes_reused_results:
        parts.append("includes reused results")
    return "; ".join(parts)


def stage_description(
    pipeline: str, operation: str, generation: int, event: str
) -> str:
    state = {
        "started": "started",
        "progress": "in progress",
        "transition": "finished",
        "terminal": "finished",
        "failed": "failed",
        "interrupted": "interrupted",
        "closed": "reporting closed before completion was confirmed",
        "overlap": "tracking stopped because calls overlapped",
    }.get(event, "completion not confirmed")
    return (
        f"Ingest {pipeline_name(pipeline)} stage {state}: "
        f"{activity(pipeline, operation)}; ingest generation {generation}"
    )
