"""Investigate retained-file READY scan costs on private generated databases.

This manual diagnostic never opens an existing database. The fixture contains
FK-valid file families and exact hash occurrences, not complete observation
descriptors, publication seals, or a full READY/E2E scenario. Production SQL is
measured unchanged against an independent fixture oracle. The former tuple
seek and a degraded ordering are dev-only baselines, never runtime fallbacks.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import signal
import statistics
import sys
import time
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from hashlib import sha256
from itertools import groupby, islice
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts"), str(ROOT / "tests")]

from ingest_growth_hash_probe import databases, write_report  # noqa: E402 - checkout.
from ingest_maintenance_mariadb import (  # noqa: E402 - checkout.
    HANDLER_STATUS,
    counter_delta,
    counters,
    plan_table_nodes,
    raw_mariadb,
)
from vnext_canonical_value_fixtures import (  # noqa: E402 - checkout fixture.
    seed_canonical_value,
)

from h2hdb import catalog_refinement as role  # noqa: E402 - checkout.
from h2hdb import vnext_identity as identity  # noqa: E402 - checkout.
from h2hdb.mariadb_connector import MariaDBConnector  # noqa: E402 - checkout.
from h2hdb.sql_connector import SQLConnector  # noqa: E402 - checkout.

PAGE_SIZE = 128
MAX_FILES = 2_097_152
SEED = 20260918
REGIMES = ("distinct", "duplicate", "metadata")
STREAMS = (
    "anchors",
    "file_nos",
    "file_sha256s",
    "artifact_role",
    "seals",
    "derived_hash_occurrences",
    "stored_hash_occurrences",
)
REGISTRY_QUERIES = (
    "catalog_channel_registry",
    "catalog_source_provider_registry",
    "catalog_resource_kinds",
    "catalog_search_policies",
    "catalog_analysis_stages",
    "catalog_publication_stages",
    "catalog_canonical_digest_policies",
)
CLEANUP_AUTHORITY_QUERY = "observation_cleanup_authority"
METADATA_IDENTITY_QUERY = "metadata_file_identity"
FIXED_QUERY_CALLS = dict.fromkeys(
    (*REGISTRY_QUERIES, CLEANUP_AUTHORITY_QUERY, METADATA_IDENTITY_QUERY), 1
)
type Regime = Literal["distinct", "duplicate", "metadata"]
type Parameters = tuple[Any, ...]


@dataclass(frozen=True)
class Shape:
    files: int
    regime: Regime = "distinct"
    galleries: int = 16
    observations: int = 2
    shared_names: bool = False

    def __post_init__(self) -> None:
        if type(self.files) is not int or not 1 <= self.files <= MAX_FILES:
            raise ValueError(f"files must be between 1 and {MAX_FILES}")
        if self.regime not in REGIMES:
            raise ValueError("unsupported fixture regime")
        if type(self.galleries) is not int or not 1 <= self.galleries <= 131_072:
            raise ValueError("galleries must be between 1 and 131072")
        if type(self.observations) is not int or not 1 <= self.observations <= 64:
            raise ValueError("observations must be between 1 and 64")
        if type(self.shared_names) is not bool:
            raise ValueError("shared_names must be boolean")


@dataclass(frozen=True, slots=True)
class FileFact:
    gallery: int
    observation: int
    key: bytes
    number: int
    digest: bytes
    name: bytes

    @property
    def coordinate(self) -> tuple[int, int, bytes]:
        return self.gallery, self.observation, self.key


def file_facts(shape: Shape) -> list[FileFact]:
    """Stable prefixes with independent gallery/history/name-reuse dimensions.

    Duplicate mode makes the first 257 files per observation share a digest,
    crossing two 128-row boundaries; later files are distinct. Metadata mode
    has one exact galleryinfo.txt per observation, excluded from CONTENT only.
    Neither digest multiplicity nor metadata exclusion is a returned-row bound.
    """
    result = []
    names: dict[int, tuple[bytes, bytes]] = {}
    owners = shape.galleries * shape.observations
    for index in range(shape.files):
        number = index // owners
        metadata = shape.regime == "metadata" and number == 0
        name_index = -1 if metadata else number if shape.shared_names else index
        pair = names.get(name_index)
        if pair is None:
            name = (
                b"galleryinfo.txt"
                if metadata
                else f"seed-{SEED}-file-{name_index:08}.png".encode()
            )
            pair = identity.file_key(name), name
            # Unique names are already retained by their facts. Only cache
            # reused names, keeping fixture construction memory proportional.
            if metadata or shape.shared_names:
                names[name_index] = pair
        key, name = pair
        digest_input = (
            b"duplicate-group"
            if shape.regime == "duplicate" and number < 257
            else index.to_bytes(8, "big")
        )
        result.append(
            FileFact(
                index % shape.galleries + 1,
                (index // shape.galleries) % shape.observations + 1,
                key,
                number,
                sha256(str(SEED).encode() + digest_input).digest(),
                name,
            )
        )
    return result


def _batch(
    connector: SQLConnector,
    sql: str,
    rows: Iterable[Parameters],
    progress: Callable[[str], None] | None = None,
) -> None:
    pending = []
    completed = 0
    for row in rows:
        pending.append(row)
        if len(pending) == 256:
            connector.execute_many(sql, pending)
            pending = []
            completed += 256
            if progress is not None and completed % 16384 == 0:
                progress(f"seed:{sql.split()[2]}:{completed}")
    if pending:
        connector.execute_many(sql, pending)


def _canonical_payload(
    connector: SQLConnector, *, domain: str, payload: bytes
) -> bytes:
    digest = identity.canonical_value_digest(domain, payload)
    page = identity.CanonicalValuePage(
        digest,
        identity.GalleryObservationNodeKind.LEAF,
        0,
        0,
        len(payload),
        (identity.CanonicalValueChunk(0, payload),),
    )
    page_bytes = identity.encode_canonical_value_page(page)
    seed_canonical_value(
        connector,
        value_sha256=digest,
        digest_domain=domain.encode("ascii"),
        page_sha256=identity.canonical_value_page_digest(page_bytes),
        page_bytes=page_bytes,
        subtree_item_count=len(payload),
        allocated_at=1,
    )
    return digest


def seed_fixture(
    connector: SQLConnector,
    facts: list[FileFact],
    progress: Callable[[str], None] | None = None,
) -> None:
    """Seed a fresh database, keeping FK enforcement and family uniqueness."""
    with connector.transaction():
        root = _canonical_payload(
            connector,
            domain="source_root_v1",
            payload=identity.encode_source_root(("synthetic-role-probe",)),
        )
        scope = identity.source_scope_key("filesystem", root, 1)
        connector.execute(
            "INSERT INTO catalog_source_scopes "
            "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
            "VALUES (%s,%s,%s,1)",
            (scope, b"filesystem", root),
        )
        owners: dict[int, set[int]] = defaultdict(set)
        for fact in facts:
            owners[fact.gallery].add(fact.observation)
        for gallery, observations in sorted(owners.items()):
            name = f"gallery-{gallery}"
            locator = _canonical_payload(
                connector,
                domain="source_relative_locator_v1",
                payload=identity.encode_source_relative_locator((name,)),
            )
            connector.execute(
                "INSERT INTO catalog_source_locator_identity "
                "(locator_sha256, source_gallery_name) VALUES (%s,%s)",
                (locator, name.encode()),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_identities "
                "(gallery_id,gallery_key,scope_key,locator_sha256) VALUES (%s,%s,%s,%s)",
                (gallery, identity.gallery_key(scope, locator), scope, locator),
            )
            for observation in sorted(observations):
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id,observation_id,allocated_at) VALUES (%s,%s,1)",
                    (gallery, observation),
                )
            if progress is not None and gallery % 256 == 0:
                progress(f"seed:gallery_identities:{gallery}")
        _batch(
            connector,
            "INSERT INTO catalog_file_name_identities (file_key,name_bytes) VALUES (%s,%s)",
            sorted({(fact.key, fact.name) for fact in facts}),
            progress,
        )
        _batch(
            connector,
            "INSERT INTO catalog_content_blobs (file_sha256,size_bytes) VALUES (%s,1)",
            ((digest,) for digest in sorted({fact.digest for fact in facts})),
            progress,
        )
        for suffix, field in (
            ("anchors", None),
            ("file_nos", "file_no"),
            ("file_sha256s", "file_sha256"),
            ("artifact_role", "artifact_role"),
            ("seals", None),
        ):
            columns = "gallery_id,observation_id,file_key" + (
                "" if field is None else "," + field
            )
            placeholders = "%s,%s,%s" + ("" if field is None else ",%s")
            _batch(
                connector,
                f"INSERT INTO catalog_gallery_observation_file_{suffix} "
                f"({columns}) VALUES ({placeholders})",
                (_family_row(fact, field) for fact in facts),
                progress,
            )
        _batch(
            connector,
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id,observation_id,file_sha256,occurrence_count) VALUES (%s,%s,%s,%s)",
            _occurrence_rows(_digest_order(facts)),
            progress,
        )


def _family_row(fact: FileFact, field: str | None) -> Parameters:
    match field:
        case "file_no":
            value: Any = fact.number
        case "file_sha256":
            value = fact.digest
        case "artifact_role":
            value = b"metadata" if fact.name == b"galleryinfo.txt" else b"page"
        case _:
            return fact.coordinate
    return *fact.coordinate, value


def _digest_order(facts: list[FileFact]) -> list[FileFact]:
    return sorted(
        facts,
        key=lambda fact: (fact.gallery, fact.observation, fact.digest, fact.key),
    )


def _occurrence_rows(ordered: list[FileFact]) -> Iterator[Parameters]:
    content = (fact for fact in ordered if fact.name != b"galleryinfo.txt")
    for key, members in groupby(
        content, key=lambda fact: (fact.gallery, fact.observation, fact.digest)
    ):
        yield *key, sum(1 for _ in members)


def content_occurrences(facts: list[FileFact]) -> Counter[tuple[int, int, bytes]]:
    return Counter(
        (fact.gallery, fact.observation, fact.digest)
        for fact in facts
        if fact.name != b"galleryinfo.txt"
    )


def stream_sizes(facts: list[FileFact]) -> dict[str, int]:
    return {
        **dict.fromkeys(STREAMS[:5], len(facts)),
        "derived_hash_occurrences": len(facts),
        "stored_hash_occurrences": len(content_occurrences(facts)),
    }


def expected_stream_rows(facts: list[FileFact]) -> dict[str, list[Parameters]]:
    """Construct each complete ordered stream from fixture facts, without SQL."""
    return {kind: list(rows) for kind, rows in fixture_streams(facts).items()}


def fixture_streams(facts: list[FileFact]) -> dict[str, Iterator[Parameters]]:
    """Two sorted fact-reference arrays, with no seven full tuple-row copies."""
    ordered = sorted(facts, key=lambda fact: fact.coordinate)
    by_digest = _digest_order(facts)
    return {
        "anchors": (
            (
                *fact.coordinate,
                fact.number,
                fact.digest,
                b"metadata" if fact.name == b"galleryinfo.txt" else b"page",
                fact.key,
                fact.key,
                fact.name,
            )
            for fact in ordered
        ),
        **{
            kind: ((*fact.coordinate, fact.key) for fact in ordered)
            for kind in STREAMS[1:5]
        },
        "derived_hash_occurrences": (
            (fact.gallery, fact.observation, fact.digest, fact.key)
            for fact in by_digest
        ),
        "stored_hash_occurrences": _occurrence_rows(by_digest),
    }


def query_kind(sql: str) -> str | None:
    if "FROM catalog_gallery_observation_file_anchors AS anchor" in sql:
        return "anchors"
    member = re.search(r"FROM (\w+) AS member", sql)
    if member:
        return member[1].removeprefix("catalog_gallery_observation_file_")
    if "FROM catalog_gallery_observation_file_file_sha256s AS file_sha" in sql:
        return "derived_hash_occurrences"
    if "FROM catalog_gallery_observation_file_hash_occurrences" in sql:
        return "stored_hash_occurrences"
    return None


def fixed_query_kind(sql: str) -> str | None:
    """Recognize registry probes and the no-OPEN-cleanup admission query."""

    normalized = " ".join(sql.split())
    relation = re.search(r"\bFROM (\w+)\b", normalized)
    if relation is not None and relation[1] in REGISTRY_QUERIES:
        return relation[1]
    if normalized.startswith(
        "SELECT file_key FROM catalog_file_name_identities WHERE name_bytes = %s"
    ):
        return METADATA_IDENTITY_QUERY
    if (
        "FROM operational_cleanup_jobs AS job" in normalized
        and "job.state = 'OPEN'" in normalized
        and "sweep.target_kind = 'GALLERY_OBSERVATION'" in normalized
        and "checkpoint.state = 'OPEN'" in normalized
        and normalized.endswith("LIMIT 257")
    ):
        return CLEANUP_AUTHORITY_QUERY
    return None


def tuple_seek_baseline(sql: str, parameters: Parameters) -> tuple[str, Parameters]:
    """Restore only the recognized former non-NULL keyset for cost comparison."""
    matched = re.fullmatch(
        r"(.* WHERE )(.*)( ORDER BY ([\w., ]+) LIMIT %s)", " ".join(sql.split())
    )
    if matched is None:
        raise ValueError("expected exactly one recognized role keyset")
    columns = [value.strip() for value in matched[4].split(",")]
    offset = int(matched[2].startswith("name.name_bytes <> %s AND "))
    prefix = "name.name_bytes <> %s AND " if offset else ""
    if len(columns) not in {3, 4} or len(parameters) != (
        offset + len(columns) * (len(columns) + 1) // 2 + len(columns) + 1
    ):
        raise ValueError("keyset parameters must be complete and non-NULL")
    values = parameters[-len(columns) - 1 : -1]
    if any(value is None for value in values):
        raise ValueError("keyset parameters must be complete and non-NULL")
    clauses: list[str] = []
    expanded: list[Any] = []
    for index, column in enumerate(columns):
        clause = " AND ".join(
            [*(f"{prior} = %s" for prior in columns[:index]), f"{column} > %s"]
        )
        clauses.append(f"({clause})" if index else clause)
        expanded.extend(values[: index + 1])
    tuple_comparison = (
        "(" + ", ".join(columns) + ") > (" + ", ".join("%s" for _ in columns) + ")"
    )
    if (
        matched[2]
        != (prefix + "(" + " OR ".join(clauses) + ") AND " + tuple_comparison)
        or tuple(expanded) != parameters[offset : -len(columns) - 1]
    ):
        raise ValueError("expected exactly one recognized role keyset")
    return (
        matched[1] + prefix + tuple_comparison + matched[3],
        (*parameters[:offset], *values, parameters[-1]),
    )


def degraded_order(sql: str, parameters: Parameters) -> tuple[str, Parameters]:
    """Preserve int63 order while denying the original ordered index scan."""
    parts = sql.split("ORDER BY")
    if len(parts) != 2:
        raise ValueError("expected exactly one ordered SELECT")
    order = parts[1]
    for column in ("gallery_id", "observation_id"):
        order, count = re.subn(rf"((?:[a-z_]+\.)?{column})", r"\1 + 0", order)
        if count != 1:
            raise ValueError("negative control requires both int63 key columns")
    return parts[0] + "ORDER BY" + order, parameters


def joined_content_baseline(sql: str, parameters: Parameters) -> tuple[str, Parameters]:
    """Restore the former joins/filter only on a captured raw derived query.

    Metadata fixtures intentionally return a different SQL page: its expected
    CONTENT page is independently constructed from the same cursor below.
    Neither this diagnostic nor its joins enter a production execution path.
    """
    if query_kind(sql) != "derived_hash_occurrences" or " JOIN " in " ".join(
        sql.split()
    ):
        raise ValueError("expected the production single-table derived scan")
    before, separator, after = sql.partition("WHERE")
    if not separator:
        raise ValueError("derived scan lacks its keyset predicate")
    joins = """
        JOIN catalog_gallery_observation_file_seals AS sealed
          ON sealed.gallery_id = file_sha.gallery_id
         AND sealed.observation_id = file_sha.observation_id
         AND sealed.file_key = file_sha.file_key
        JOIN catalog_file_name_identities AS name
          ON name.file_key = file_sha.file_key
        WHERE name.name_bytes <> %s AND
    """
    return before + joins + after, (b"galleryinfo.txt", *parameters)


def _escape_nonstandard_strings(raw: str) -> str:
    """Repair only invalid escapes/control bytes inside JSON string literals."""
    output: list[str] = []
    inside = False
    index = 0
    while index < len(raw):
        character = raw[index]
        if inside and character == "\\":
            following = raw[index + 1 : index + 2]
            unicode_escape = (
                following == "u"
                and len(raw[index + 2 : index + 6]) == 4
                and all(
                    c in "0123456789abcdefABCDEF" for c in raw[index + 2 : index + 6]
                )
            )
            if following in ('"', "\\", "/", "b", "f", "n", "r", "t") or unicode_escape:
                output.append(raw[index : index + 2])
                index += 2
                continue
            output.append("\\\\")
        elif character == '"':
            inside = not inside
            output.append(character)
        elif inside and ord(character) < 32:
            output.append(json.dumps(character)[1:-1])
        else:
            output.append(character)
        index += 1
    return "".join(output)


def _redact_plan_conditions(raw: str) -> tuple[str, int]:
    """Discard unusable diagnostic expression text, never execution counters.

    MariaDB 10.11 can put raw quotes inside binary-bound conditions. Its pretty
    output still terminates each expression at a line boundary followed by the
    next JSON field/container. This fallback is confined to two string fields
    in private fixture plans. Keep the original text separately; repaired
    conditions are not SQL-semantics evidence. Malformed numeric/structural
    output must still fail ordinary JSON parsing and counter validation.
    """
    pattern = re.compile(
        r'("(?:attached_condition|index_condition)"\s*:\s*)".*?"'
        r'(?=,?\s*\n\s*(?:"[A-Za-z_][A-Za-z_0-9]*"\s*:|[}\]]))',
        re.DOTALL,
    )
    return pattern.subn(lambda match: match[1] + '"<opaque binary SQL condition>"', raw)


def decode_plan(row: tuple[Any, ...]) -> dict[str, Any]:
    if len(row) != 1 or not isinstance(row[0], str):
        raise ValueError("plan must contain one JSON string")
    raw = row[0]
    repaired = False
    redacted_conditions = 0
    try:
        plan = json.loads(raw)
    except json.JSONDecodeError:
        try:
            plan = json.loads(_escape_nonstandard_strings(raw))
        except json.JSONDecodeError:
            sanitized, redacted_conditions = _redact_plan_conditions(raw)
            try:
                plan = json.loads(_escape_nonstandard_strings(sanitized))
            except json.JSONDecodeError as error:
                # Preserve exact input when a server JSON defect is unfamiliar.
                error.add_note("raw_plan_json=" + repr(raw))
                raise
        repaired = True
    if not isinstance(plan, dict) or not isinstance(plan.get("query_block"), dict):
        raise ValueError("plan lacks its query block")
    return {
        "raw_text": raw,
        "raw_sha256": sha256(raw.encode()).hexdigest(),
        "nonstandard_strings_repaired": repaired,
        "condition_strings_redacted": redacted_conditions,
        "plan": plan,
    }


def table_row_visits(plan: dict[str, Any]) -> float:
    nodes = plan_table_nodes(plan)
    if not nodes:
        raise ValueError("ANALYZE lacks table execution counters")
    total = 0.0
    for node in nodes:
        loops, rows = node.get("r_loops"), node.get("r_rows")
        if (
            isinstance(loops, bool)
            or not isinstance(loops, int | float)
            or not math.isfinite(loops)
            or loops < 0
        ):
            raise ValueError("ANALYZE lacks a valid r_loops counter")
        if loops == 0:
            continue
        if (
            isinstance(rows, bool)
            or not isinstance(rows, int | float)
            or not math.isfinite(rows)
            or rows < 0
        ):
            raise ValueError("executed ANALYZE node lacks a valid r_rows counter")
        total += loops * rows
    return total


def seek_budget(kind: str, facts: list[FileFact]) -> int:
    # Every production stream now pages raw index entries; metadata exclusions
    # happen after the derived SQL page. No whole-fixture cardinality may relax
    # the fixed budget. Eight accesses cover scan plus up to five anchor joins.
    if kind not in STREAMS or not facts:
        raise ValueError("seek budget requires a known stream and nonempty fixture")
    return 8 * (PAGE_SIZE + 1) + 32


def cost_verdict(visits: float, handler_operations: list[int], budget: int) -> str:
    if not handler_operations:
        raise ValueError("missing measured Handler operations")
    if (
        not math.isfinite(visits)
        or visits < 0
        or type(budget) is not int
        or budget <= 0
        or any(type(value) is not int or value < 0 for value in handler_operations)
    ):
        raise ValueError("invalid cost measurements or budget")
    return (
        "observed_within_budget"
        if visits <= budget and max(handler_operations) <= budget
        else "violated"
    )


@dataclass(frozen=True)
class CapturedQuery:
    sql: str
    parameters: Parameters
    returned_rows: int
    seconds: float


def capture_validator(
    connector: SQLConnector,
    facts: list[FileFact],
    progress: Callable[[str], None] | None = None,
) -> tuple[dict[str, list[CapturedQuery]], dict[str, Any]]:
    captured: dict[str, list[CapturedQuery]] = defaultdict(list)
    expected_rows = fixture_streams(facts)
    all_calls = 0
    fixed_calls: Counter[str] = Counter()
    unclassified_calls = 0
    original = connector.fetch_all
    original_one = connector.fetch_one

    def capture(sql: str, parameters: Parameters = ()) -> list[tuple[Any, ...]]:
        nonlocal all_calls, unclassified_calls
        started = time.perf_counter()
        rows = original(sql, parameters)
        elapsed = time.perf_counter() - started
        all_calls += 1
        kind = query_kind(sql)
        if kind is not None:
            if rows != list(islice(expected_rows[kind], PAGE_SIZE)):
                raise RuntimeError(f"role stream differs from fixture facts: {kind}")
            captured[kind].append(CapturedQuery(sql, parameters, len(rows), elapsed))
            if progress is not None and (len(captured[kind]) % 128 == 0 or not rows):
                progress(f"scan:{kind}:{len(captured[kind])}")
        elif (fixed_kind := fixed_query_kind(sql)) is not None:
            fixed_calls[fixed_kind] += 1
            if fixed_kind == CLEANUP_AUTHORITY_QUERY and rows:
                raise RuntimeError("role cost fixture must not have OPEN cleanup work")
        else:
            unclassified_calls += 1
        return rows

    def capture_one(sql: str, parameters: Parameters = ()) -> tuple[Any, ...]:
        nonlocal all_calls, unclassified_calls
        row = original_one(sql, parameters)
        all_calls += 1
        kind = fixed_query_kind(sql)
        if kind != METADATA_IDENTITY_QUERY:
            unclassified_calls += 1
            return row
        fixed_calls[kind] += 1
        expected = (
            (identity.file_key(b"galleryinfo.txt"),)
            if any(fact.name == b"galleryinfo.txt" for fact in facts)
            else ()
        )
        if parameters != (b"galleryinfo.txt",) or row != expected:
            raise RuntimeError("metadata file identity differs from fixture facts")
        return row

    started = time.perf_counter()
    with (
        patch.object(connector, "fetch_all", capture),
        patch.object(connector, "fetch_one", capture_one),
    ):
        role.check_role_derivation_v1(connector)
    elapsed = time.perf_counter() - started
    if set(captured) != set(STREAMS):
        raise RuntimeError("incomplete role stream instrumentation")
    if unclassified_calls:
        raise RuntimeError("unclassified role SELECT instrumentation")
    for kind, calls in FIXED_QUERY_CALLS.items():
        if fixed_calls[kind] != calls:
            raise RuntimeError(f"fixed role query count mismatch: {kind}")
    if any(next(rows, None) is not None for rows in expected_rows.values()):
        raise RuntimeError("role stream ended before all fixture facts were read")
    expected = {
        **dict.fromkeys(STREAMS[:6], len(facts)),
        "stored_hash_occurrences": sum(
            1 for _ in _occurrence_rows(_digest_order(facts))
        ),
    }
    for kind, queries in captured.items():
        if (
            len(queries) != (expected[kind] + PAGE_SIZE - 1) // PAGE_SIZE + 1
            or sum(query.returned_rows for query in queries) != expected[kind]
            or queries[-1].returned_rows != 0
            or any(query.returned_rows > PAGE_SIZE for query in queries)
        ):
            raise RuntimeError(f"role stream cardinality mismatch: {kind}")
    return captured, {
        "client_seconds": elapsed,
        "select_calls": all_calls,
        "stream_select_calls": sum(map(len, captured.values())),
        "fixed_select_calls": dict(fixed_calls),
        "fixed_select_calls_total": fixed_calls.total(),
        "streams": {
            kind: {
                "calls": len(queries),
                "returned_rows": sum(query.returned_rows for query in queries),
                "page_returned_rows": [query.returned_rows for query in queries],
                "page_client_seconds": [query.seconds for query in queries],
            }
            for kind, queries in captured.items()
        },
    }


def sample_positions(queries: list[CapturedQuery]) -> dict[str, int]:
    nonempty = [index for index, query in enumerate(queries) if query.returned_rows]
    positions = {"last_empty": len(queries) - 1}
    if nonempty:
        positions.update(
            first=nonempty[0],
            middle=nonempty[len(nonempty) // 2],
            last_nonempty=nonempty[-1],
        )
    return positions


def profile_query(
    connector: MariaDBConnector,
    sql: str,
    parameters: Parameters,
    expected: list[tuple[Any, ...]],
    *,
    repetitions: int,
    budget: int,
) -> dict[str, Any]:
    before = counters(connector, HANDLER_STATUS)
    control = counter_delta(before, counters(connector, HANDLER_STATUS))
    if any(control.values()):
        raise RuntimeError("diagnostic SHOW STATUS changes measured Handler counts")
    elapsed = []
    deltas = []
    for _ in range(repetitions):
        before = counters(connector, HANDLER_STATUS)
        started = time.perf_counter()
        rows = connector.fetch_all(sql, parameters)
        elapsed.append(time.perf_counter() - started)
        deltas.append(counter_delta(before, counters(connector, HANDLER_STATUS)))
        if rows != expected:
            raise RuntimeError("experimental SELECT changed ordered output")
    explain = decode_plan(connector.fetch_one("EXPLAIN FORMAT=JSON " + sql, parameters))
    analyze = decode_plan(connector.fetch_one("ANALYZE FORMAT=JSON " + sql, parameters))
    visits = table_row_visits(analyze["plan"])
    handlers = [sum(delta.values()) for delta in deltas]
    verdict = cost_verdict(visits, handlers, budget)
    # Pre-11.5 r_rows is after ICP/rowid filtering. Preserve raw plan evidence,
    # and decline positive confirmation when those hidden filters are present.
    hidden_filter = any(
        marker in analyze["raw_text"]
        for marker in ('"index_condition"', '"rowid_filter"')
    )
    if hidden_filter and verdict != "violated":
        verdict = "inconclusive_filtered_plan"
    return {
        "sql": sql,
        "sql_sha256": sha256(sql.encode()).hexdigest(),
        "parameters": parameters,
        "returned_rows": len(expected),
        "select_repetitions": repetitions,
        "client_seconds": elapsed,
        "median_client_seconds": statistics.median(elapsed),
        "handler_deltas": deltas,
        "handler_operations": handlers,
        "table_node_row_visits": visits,
        "budget": budget,
        "verdict": verdict,
        "explain": explain,
        "analyze": analyze,
    }


def measure_case(
    connector: SQLConnector,
    shape: Shape,
    repetitions: int,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if connector.fetch_one("SELECT @@foreign_key_checks")[0] != 1:
        raise RuntimeError("fixture requires active foreign-key enforcement")
    connector.rollback()
    facts = file_facts(shape)
    seed_fixture(connector, facts, progress)
    raw = raw_mariadb(connector)
    cycles = []
    for _cycle in range(3):
        if progress is not None:
            progress(f"validator_cycle:{_cycle + 1}")
        before = counters(raw, HANDLER_STATUS)
        captured, validator = capture_validator(connector, facts, progress)
        validator["handler_delta"] = counter_delta(
            before, counters(raw, HANDLER_STATUS)
        )
        cycles.append(validator)
    expected_rows = fixture_streams(facts)
    by_digest = _digest_order(facts)
    samples: list[dict[str, Any]] = []
    for kind, queries in captured.items():
        positions = sample_positions(queries)
        consumed = 0
        # One measurement per unique cursor; labels can alias on a one-page set.
        for index in sorted(set(positions.values())):
            query = queries[index]
            offset = index * PAGE_SIZE
            for _ in islice(expected_rows[kind], offset - consumed):
                pass
            expected = list(islice(expected_rows[kind], PAGE_SIZE))
            consumed = offset + len(expected)
            variants = {}
            queries_to_profile = {
                "production": (query.sql, query.parameters),
                "tuple_seek_baseline": tuple_seek_baseline(query.sql, query.parameters),
                "negative_order": degraded_order(query.sql, query.parameters),
            }
            if kind == "derived_hash_occurrences":
                queries_to_profile["former_joined_content"] = joined_content_baseline(
                    query.sql, query.parameters
                )
            for name, (sql, parameters) in queries_to_profile.items():
                if progress is not None:
                    progress(f"profile:{kind}:{index}:{name}")
                variant_expected = expected
                if name == "former_joined_content":
                    after = query.parameters[-5:-1]
                    start = bisect_right(
                        by_digest,
                        after,
                        key=lambda fact: (
                            fact.gallery,
                            fact.observation,
                            fact.digest,
                            fact.key,
                        ),
                    )
                    variant_expected = list(
                        islice(
                            (
                                (fact.gallery, fact.observation, fact.digest, fact.key)
                                for fact in islice(by_digest, start, None)
                                if fact.name != b"galleryinfo.txt"
                            ),
                            PAGE_SIZE,
                        )
                    )
                variants[name] = profile_query(
                    raw,
                    sql,
                    parameters,
                    variant_expected,
                    repetitions=repetitions,
                    budget=seek_budget(kind, facts),
                )
            samples.append(
                {
                    "stream": kind,
                    "page_index": index,
                    "positions": [
                        name for name, value in positions.items() if value == index
                    ],
                    "variants": variants,
                }
            )
    return {
        "shape": asdict(shape),
        "input_counts": {
            "galleries": len({fact.gallery for fact in facts}),
            "observations": len({(fact.gallery, fact.observation) for fact in facts}),
            "configured_observations_per_gallery": shape.observations,
            "distinct_file_names": len({fact.key for fact in facts}),
            "metadata_files": sum(fact.name == b"galleryinfo.txt" for fact in facts),
            "content_files": sum(fact.name != b"galleryinfo.txt" for fact in facts),
            "hash_occurrence_groups": sum(1 for _ in _occurrence_rows(by_digest)),
        },
        "validator": validator,
        "validator_cycles": cycles,
        "expected_stream_rows": stream_sizes(facts),
        "samples": samples,
        "production_verdict": "violated"
        if any(
            sample["variants"]["production"]["verdict"] == "violated"
            for sample in samples
        )
        else "no_violation_observed_at_this_shape",
        "negative_control_rejected": {
            kind: any(
                sample["stream"] == kind
                and sample["variants"]["negative_order"]["verdict"] == "violated"
                for sample in samples
            )
            for kind in STREAMS
        },
    }


def contract() -> dict[str, Any]:
    return {
        "seed": SEED,
        "page_cap": PAGE_SIZE,
        "units": [
            "SELECT calls",
            "returned rows",
            "Handler access requests",
            "ANALYZE table-node row visits",
            "client seconds",
        ],
        "target": "Avoid prefix-length work: seek descent plus bounded pages/point joins, not strict O(1) physical storage work.",
        "budget": "8*(128+1)+32 = 1064 access requests/table visits per sampled production page, independent of whole-database or metadata cardinality. Raw derived pages include metadata; Python excludes it after cursor advancement. Stored stream pages distinct CONTENT observation/hash groups.",
        "stream_call_model": "Sum ceil(stream_rows/128)+1 across six raw-file streams and one distinct CONTENT observation/hash-group stream, including each terminal empty page. For distinct CONTENT-only inputs this is 7*(ceil(N/128)+1).",
        "fixed_call_model": "Exactly seven registry SELECTs, one empty OPEN observation-cleanup authority SELECT and one exact metadata filename identity lookup per validator call. Total SELECTs equal stream SELECTs + 9; the fixed probe does not relax any stream or seek-work budget.",
        "counterexample": "Former joined CONTENT query is replayed only at sampled cursors: shared filenames expose name-first fan-out and filesort. Its independently computed CONTENT page differs from the raw production page only when metadata is present. Former tuple seek is a dev-only prefix-rescan baseline. Same-output +0 numeric ORDER BY denies ordered-index shortcut; every stream at the largest default shape must reject this control.",
        "independent_oracle": "Every production validator page and every profiled SELECT is compared to ordered rows built directly from fixture facts. Three complete validator cycles reuse unchanged input; each sampled query has three SELECT repetitions by default.",
        "limitations": [
            "FK-valid role file families only; not full observation descriptors, READY audit, publication or recovery.",
            "The fixture has no OPEN cleanup work. Exact retirement-authority validation for an active cleanup is outside this cost experiment, not included in its eight fixed SELECTs.",
            "Warm local MariaDB 10.11.11; fixed variant order; elapsed ratios are not NAS speedup predictions.",
            "ANALYZE r_rows*r_loops is not distinct examined rows and may exclude pushed filters; Handler counts are requests.",
            "Metadata and duplicate regimes have independent stream cardinalities; LIMIT 128 alone is not a server-work bound.",
            "Full fixture stream equality and sampled cost bounds are finite implementation evidence, not an unbounded equivalence or asymptotic proof. Captured production SQL is profiled unchanged; baseline transforms exist only in this dev probe.",
            "Completed means investigation completed; violated production cost targets remain violated.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scales", type=int, nargs="+", default=[127, 128, 129, 4096, 32768]
    )
    parser.add_argument("--regimes", choices=REGIMES, nargs="+", default=list(REGIMES))
    parser.add_argument("--galleries", type=int, default=16)
    parser.add_argument("--observations", type=int, default=2)
    parser.add_argument("--shared-names", action="store_true")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not all(hasattr(signal, name) for name in ("SIGALRM", "setitimer")):
        parser.error("manual probe requires a POSIX cooperative alarm")
    if not 1 <= args.repetitions <= 5 or not 1 <= args.timeout_seconds <= 1800:
        parser.error("repetitions must be 1..5 and timeout-seconds 1..1800")
    if args.output.exists():
        parser.error("output must be a new file")
    shapes = [
        Shape(n, regime, args.galleries, args.observations, args.shared_names)
        for regime in args.regimes
        for n in args.scales
    ]
    if len(shapes) > 15 or len(shapes) != len(set(shapes)):
        parser.error("at most 15 distinct cases per invocation")
    report: dict[str, Any] = {
        "status": "incomplete",
        "contract": contract(),
        "cases": [],
        "argv": sys.argv,
        "python": sys.version,
        "source_sha256": {
            str(path.relative_to(ROOT)): sha256(path.read_bytes()).hexdigest()
            for path in (
                Path(__file__),
                ROOT / "src/h2hdb/catalog_refinement.py",
                ROOT / "src/h2hdb/_generated_vnext_schema.py",
                ROOT / "scripts/ingest_growth_hash_probe.py",
                ROOT / "scripts/ingest_maintenance_mariadb.py",
                ROOT / "tests/vnext_canonical_value_fixtures.py",
            )
        },
        "timeout_contract": "POSIX cooperative alarm; Docker/driver teardown is not a hard deadline.",
    }
    write_report(args.output, report)

    def timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError("role probe exceeded cooperative deadline")

    previous = signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, args.timeout_seconds)
    try:
        with databases("mariadb", len(shapes)) as connections:
            for connector, shape in zip(connections, shapes, strict=True):
                report["server_version"] = connector.fetch_one("SELECT VERSION()")[0]
                if not str(report["server_version"]).startswith("10.11.11-"):
                    raise RuntimeError("unexpected MariaDB version")
                connector.execute("SET SESSION max_statement_time=60")
                report["server_settings"] = {
                    key: connector.fetch_one("SELECT @@" + key)[0]
                    for key in (
                        "innodb_buffer_pool_size",
                        "optimizer_switch",
                        "max_statement_time",
                    )
                }
                case = measure_case(
                    connector,
                    shape,
                    args.repetitions,
                    lambda event: print(json.dumps({"progress": event}), flush=True),
                )
                report["cases"].append(case)
                write_report(args.output, report)
                print(
                    json.dumps(
                        {
                            "shape": asdict(shape),
                            "production_verdict": case["production_verdict"],
                        }
                    ),
                    flush=True,
                )
        # A small selected shape cannot guarantee the degraded scan exceeds the
        # budget. Record that limitation; never call it successful rejection.
        controls = {
            regime: all(
                any(
                    case["shape"]["regime"] == regime
                    and case["negative_control_rejected"][kind]
                    for case in report["cases"]
                )
                for kind in STREAMS
            )
            for regime in args.regimes
        }
        report["negative_controls_rejected_per_regime"] = controls
        if 32768 in args.scales and not all(controls.values()):
            raise RuntimeError(
                "largest-shape negative control was not rejected in every stream"
            )
        report["status"] = "completed"
        report["database_owner_closed"] = True
        write_report(args.output, report)
    except BaseException as error:
        report["error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "notes": getattr(error, "__notes__", []),
        }
        write_report(args.output, report)
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == "__main__":
    main()
