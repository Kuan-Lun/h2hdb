"""Investigate retained-file READY scan costs on private generated databases.

This manual diagnostic never opens an existing database. The fixture contains
FK-valid file families and exact hash occurrences, not complete observation
descriptors, publication seals, or a full READY/E2E scenario. Production SQL is
measured unchanged; candidate SQL is experimental. A completed investigation
may correctly report a violated production performance target.
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
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from hashlib import sha256
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
type Regime = Literal["distinct", "duplicate", "metadata"]
type Parameters = tuple[Any, ...]


@dataclass(frozen=True)
class Shape:
    files: int
    regime: Regime = "distinct"

    def __post_init__(self) -> None:
        if type(self.files) is not int or not 1 <= self.files <= 32768:
            raise ValueError("files must be between 1 and 32768")
        if self.regime not in REGIMES:
            raise ValueError("unsupported fixture regime")


@dataclass(frozen=True)
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
    """Stable prefixes, 16 galleries, two historical observations per gallery.

    Duplicate mode makes the first 257 files per observation share a digest,
    crossing two 128-row boundaries; later files are distinct. Metadata mode
    has one exact galleryinfo.txt per observation, excluded from CONTENT only.
    Neither digest multiplicity nor metadata exclusion is a returned-row bound.
    """
    result = []
    for index in range(shape.files):
        number = index // 32
        metadata = shape.regime == "metadata" and number == 0
        name = (
            b"galleryinfo.txt"
            if metadata
            else f"seed-{SEED}-file-{index:08}.png".encode()
        )
        digest_input = (
            b"duplicate-group"
            if shape.regime == "duplicate" and number < 257
            else index.to_bytes(8, "big")
        )
        result.append(
            FileFact(
                index % 16 + 1,
                (index // 16) % 2 + 1,
                identity.file_key(name),
                number,
                sha256(str(SEED).encode() + digest_input).digest(),
                name,
            )
        )
    return result


def _batch(connector: SQLConnector, sql: str, rows: Iterable[Parameters]) -> None:
    pending = []
    for row in rows:
        pending.append(row)
        if len(pending) == 256:
            connector.execute_many(sql, pending)
            pending = []
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


def seed_fixture(connector: SQLConnector, facts: list[FileFact]) -> None:
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
        for gallery in range(1, 17):
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
            for observation in (1, 2):
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id,observation_id,allocated_at) VALUES (%s,%s,1)",
                    (gallery, observation),
                )
        _batch(
            connector,
            "INSERT INTO catalog_file_name_identities (file_key,name_bytes) VALUES (%s,%s)",
            sorted({(fact.key, fact.name) for fact in facts}),
        )
        _batch(
            connector,
            "INSERT INTO catalog_content_blobs (file_sha256,size_bytes) VALUES (%s,1)",
            ((digest,) for digest in sorted({fact.digest for fact in facts})),
        )
        for suffix, field in (
            ("anchors", None),
            ("file_nos", "file_no"),
            ("file_sha256s", "file_sha256"),
            ("artifact_role", "artifact_role"),
            ("seals", None),
        ):
            rows: list[Parameters] = []
            for fact in facts:
                match field:
                    case "file_no":
                        value: Any = fact.number
                    case "file_sha256":
                        value = fact.digest
                    case "artifact_role":
                        value = (
                            b"metadata" if fact.name == b"galleryinfo.txt" else b"page"
                        )
                    case _:
                        value = None
                rows.append(
                    fact.coordinate if field is None else (*fact.coordinate, value)
                )
            columns = "gallery_id,observation_id,file_key" + (
                "" if field is None else "," + field
            )
            placeholders = "%s,%s,%s" + ("" if field is None else ",%s")
            _batch(
                connector,
                f"INSERT INTO catalog_gallery_observation_file_{suffix} "
                f"({columns}) VALUES ({placeholders})",
                rows,
            )
        occurrences = content_occurrences(facts)
        _batch(
            connector,
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id,observation_id,file_sha256,occurrence_count) VALUES (%s,%s,%s,%s)",
            ((*key, count) for key, count in sorted(occurrences.items())),
        )


def content_occurrences(facts: list[FileFact]) -> Counter[tuple[int, int, bytes]]:
    return Counter(
        (fact.gallery, fact.observation, fact.digest)
        for fact in facts
        if fact.name != b"galleryinfo.txt"
    )


def stream_sizes(facts: list[FileFact]) -> dict[str, int]:
    return {
        **dict.fromkeys(STREAMS[:5], len(facts)),
        "derived_hash_occurrences": sum(
            fact.name != b"galleryinfo.txt" for fact in facts
        ),
        "stored_hash_occurrences": len(content_occurrences(facts)),
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


def expanded_seek(sql: str, parameters: Parameters) -> tuple[str, Parameters]:
    """Equivalent only for these manifest-owned non-NULL numeric/binary keys."""
    matches = list(re.finditer(r"\(([\w.,\s]+)\)\s*>\s*\(((?:%s,?\s*)+)\)", sql))
    if len(matches) != 1:
        raise ValueError("expected exactly one tuple keyset comparison")
    match = matches[0]
    columns = [value.strip() for value in match[1].split(",")]
    offset = sql[: match.start()].count("%s")
    values = parameters[offset : offset + len(columns)]
    if len(values) != len(columns) or any(value is None for value in values):
        raise ValueError("keyset parameters must be complete and non-NULL")
    clauses = []
    expanded: list[Any] = []
    for index, column in enumerate(columns):
        clauses.append(
            "("
            + " AND ".join(
                [*(f"{prior} = %s" for prior in columns[:index]), f"{column} > %s"]
            )
            + ")"
        )
        expanded.extend(values[: index + 1])
    return (
        sql[: match.start()] + "(" + " OR ".join(clauses) + ")" + sql[match.end() :],
        (*parameters[:offset], *expanded, *parameters[offset + len(columns) :]),
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
    # At most 128 returned CONTENT files plus every metadata exclusion in this
    # fixture. This is not a universal LIMIT-to-work implication. Eight access
    # units cover the leading scan and up to five anchor point joins, with seek
    # slack. B-tree depth/CPU/physical IO are not proven constant by this budget.
    filtered = (
        sum(fact.name == b"galleryinfo.txt" for fact in facts)
        if kind == "derived_hash_occurrences"
        else 0
    )
    return 8 * (PAGE_SIZE + filtered + 1) + 32


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
    connector: SQLConnector, facts: list[FileFact]
) -> tuple[dict[str, list[CapturedQuery]], dict[str, Any]]:
    captured: dict[str, list[CapturedQuery]] = defaultdict(list)
    all_calls = 0
    original = connector.fetch_all

    def capture(sql: str, parameters: Parameters = ()) -> list[tuple[Any, ...]]:
        nonlocal all_calls
        started = time.perf_counter()
        rows = original(sql, parameters)
        elapsed = time.perf_counter() - started
        all_calls += 1
        kind = query_kind(sql)
        if kind is not None:
            captured[kind].append(CapturedQuery(sql, parameters, len(rows), elapsed))
        return rows

    started = time.perf_counter()
    with patch.object(connector, "fetch_all", capture):
        role.check_role_derivation_v1(connector)
    elapsed = time.perf_counter() - started
    if set(captured) != set(STREAMS):
        raise RuntimeError("incomplete role stream instrumentation")
    expected = stream_sizes(facts)
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
    connector: SQLConnector, shape: Shape, repetitions: int
) -> dict[str, Any]:
    if connector.fetch_one("SELECT @@foreign_key_checks")[0] != 1:
        raise RuntimeError("fixture requires active foreign-key enforcement")
    connector.rollback()
    facts = file_facts(shape)
    seed_fixture(connector, facts)
    raw = raw_mariadb(connector)
    before = counters(raw, HANDLER_STATUS)
    captured, validator = capture_validator(connector, facts)
    validator["handler_delta"] = counter_delta(before, counters(raw, HANDLER_STATUS))
    samples: list[dict[str, Any]] = []
    for kind, queries in captured.items():
        positions = sample_positions(queries)
        # One measurement per unique cursor; labels can alias on a one-page set.
        for index in dict.fromkeys(positions.values()):
            query = queries[index]
            expected = connector.fetch_all(query.sql, query.parameters)
            variants = {}
            for name, (sql, parameters) in {
                "production": (query.sql, query.parameters),
                "expanded_seek": expanded_seek(query.sql, query.parameters),
                "negative_order": degraded_order(query.sql, query.parameters),
            }.items():
                variants[name] = profile_query(
                    raw,
                    sql,
                    parameters,
                    expected,
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
            "galleries": 16,
            "observations_per_gallery": 2,
            "metadata_files": sum(fact.name == b"galleryinfo.txt" for fact in facts),
            "content_files": sum(content_occurrences(facts).values()),
            "hash_occurrence_groups": len(content_occurrences(facts)),
        },
        "validator": validator,
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
        "budget": "8*(128 + fixture metadata exclusions for derived stream + 1)+32. Derived stream pages raw CONTENT files; stored stream pages distinct observation/hash groups, whose counts differ under duplication.",
        "stream_call_model": "Sum ceil(stream_rows/128)+1 across five raw-file streams, one CONTENT-file stream, and one distinct observation/hash-group stream, including each terminal empty page. For distinct CONTENT-only inputs this is 7*(ceil(N/128)+1).",
        "counterexample": "Same-output +0 numeric ORDER BY denies ordered-index shortcut; every stream at the largest default shape must reject this control.",
        "limitations": [
            "FK-valid role file families only; not full observation descriptors, READY audit, publication or recovery.",
            "Warm local MariaDB 10.11.11; fixed variant order; elapsed ratios are not NAS speedup predictions.",
            "ANALYZE r_rows*r_loops is not distinct examined rows and may exclude pushed filters; Handler counts are requests.",
            "Metadata and duplicate regimes have independent stream cardinalities; LIMIT 128 alone is not a server-work bound.",
            "Sampled candidate equality is not a full equivalence or asymptotic proof. No production query is replaced.",
            "Completed means investigation completed; violated production cost targets remain violated.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scales", type=int, nargs="+", default=[127, 128, 129, 4096, 32768]
    )
    parser.add_argument("--regimes", choices=REGIMES, nargs="+", default=list(REGIMES))
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
    shapes = [Shape(n, regime) for regime in args.regimes for n in args.scales]
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
                case = measure_case(connector, shape, args.repetitions)
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
