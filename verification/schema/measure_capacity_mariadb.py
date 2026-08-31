"""Run the manual MariaDB capacity measurements with deterministic keys.

This profile first fills the request table to the 1.5-million-row budget,
deletes that complete random-key fill in bounded commits without truncation,
then refills with a different key domain before appending a sixth synthetic
request per staging ID as a 1.8-million-row diagnostic.  It also measures the
exact retained planning shape of ``source_scope``.  Only the pinned
Testcontainers MariaDB profile is accepted.  The profile is not part of the
ordinary release gate; its preserved JSON output is the empirical input when
regenerating ``capacity_measurement.toml``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack
from pathlib import Path
from typing import Any, Protocol, cast

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT / "verification" / "schema"
PHYSICAL_PATH = SCHEMA_ROOT / "physical.toml"
OPERATIONAL_PHYSICAL_PATH = SCHEMA_ROOT / "operational_physical.toml"

MARIADB_IMAGE = "mariadb:10.11.11"
EXPECTED_INNODB_PAGE_SIZE = 16_384
EXPECTED_ENGINE = "INNODB"
EXPECTED_ROW_FORMAT = "DYNAMIC"
EXPECTED_TABLE_COLLATION = "utf8mb4_nopad_bin"
MEASUREMENT_SEED = 0x48324844425F43415041434954595F31
INSERT_BATCH_SIZE = 5_000
INSERTION_ORDER = (
    "full_churn_fill_delete_different_domain_refill_five_then_sixth_append_v2"
)
EXECUTION_MODE = "testcontainers"
REGISTRY_ROW_GENERATOR = "maximum_width_analysis_policy_v1"
REGISTRY_KEY_DISTRIBUTION = "bit_reversed_u63_policy_id_v1"
STAGING_ROW_GENERATOR = "different_domain_churn_then_five_then_sixth_requests_v2"
STAGING_KEY_DISTRIBUTION = (
    "independent_sha256_coordinate_request_and_staging_uuid_domains_v2"
)
SOURCE_SCOPE_ROW_GENERATOR = "filesystem_source_scope_sha256_coordinate_v1"
SOURCE_SCOPE_KEY_DISTRIBUTION = "sha256_coordinate_stream_scope_and_root_v1"

STAGING_ID_COUNT = 300_000
ACCEPTED_REQUESTS_PER_STAGING = 5
OVER_CAPACITY_REQUESTS_PER_STAGING = 6
SOURCE_SCOPE_ROW_COUNT = 300_019

_REGISTRY_TABLE = "capacity_analysis_policies"
_STAGING_TABLE = "capacity_gallery_observation_staging_requests"
_SOURCE_SCOPE_TABLE = "capacity_source_scopes"


class _Cursor(Protocol):
    def execute(
        self,
        operation: str,
        params: Sequence[object] | None = None,
    ) -> object: ...

    def executemany(
        self,
        operation: str,
        seq_params: Sequence[Sequence[object]],
    ) -> object: ...

    def fetchone(self) -> Sequence[object] | None: ...

    def fetchall(self) -> Sequence[Sequence[object]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _relation(document: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    relations = document.get("relation")
    if not isinstance(relations, list):
        raise RuntimeError("physical manifest relation array is missing")
    matches = [item for item in relations if item.get("name") == name]
    if len(matches) != 1 or not isinstance(matches[0], Mapping):
        raise RuntimeError(f"expected one physical relation {name!r}")
    return matches[0]


def _storage_shape(relation: Mapping[str, Any]) -> dict[str, object]:
    columns = relation.get("column")
    indexes = relation.get("required_index", [])
    if not isinstance(columns, list) or not isinstance(indexes, list):
        raise RuntimeError("physical relation columns or indexes are malformed")
    return {
        "primary_key": relation.get("primary_key"),
        "unique_keys": relation.get("unique_keys", []),
        "referential_unique_keys": relation.get("referential_unique_keys", []),
        "columns": [
            {
                "attribute": column.get("attribute"),
                "mariadb": column.get("mariadb"),
            }
            for column in columns
        ],
        "required_indexes": [
            {
                "attributes": index.get("attributes"),
                "unique": index.get("unique"),
            }
            for index in indexes
        ],
    }


def _shape_sha256(relation: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _storage_shape(relation),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _assert_measured_shapes() -> None:
    physical = _load(PHYSICAL_PATH)
    operational = _load(OPERATIONAL_PHYSICAL_PATH)
    registry = _relation(physical, "analysis_policy")
    source_scope = _relation(physical, "source_scope")
    staging = _relation(operational, "gallery_observation_staging_request")
    registry_columns = [
        (column.get("attribute"), column.get("mariadb"))
        for column in registry.get("column", [])
    ]
    if registry_columns != [
        (
            "policy_id",
            {"type": "BIGINT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
        (
            "algorithm_version",
            {"type": "INT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
        (
            "spam_artist_threshold",
            {"type": "BIGINT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
        (
            "spam_occurrence_threshold",
            {"type": "BIGINT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
        (
            "content_owner_rule_version",
            {"type": "INT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
        (
            "gid_winner_rule_version",
            {"type": "INT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
    ]:
        raise RuntimeError("analysis policy physical measurement shape drifted")
    staging_columns = [
        (column.get("attribute"), column.get("mariadb"))
        for column in staging.get("column", [])
    ]
    if staging_columns != [
        (
            "request_sha256",
            {"type": "BINARY(32)", "nullable": False, "collation": "NONE"},
        ),
        (
            "staging_id",
            {"type": "BINARY(16)", "nullable": False, "collation": "NONE"},
        ),
    ]:
        raise RuntimeError("staging request physical measurement shape drifted")
    source_scope_columns = [
        (column.get("attribute"), column.get("mariadb"))
        for column in source_scope.get("column", [])
    ]
    if source_scope_columns != [
        (
            "scope_key",
            {"type": "BINARY(32)", "nullable": False, "collation": "NONE"},
        ),
        (
            "source_provider",
            {"type": "VARBINARY(64)", "nullable": False, "collation": "NONE"},
        ),
        (
            "source_root_sha256",
            {"type": "BINARY(32)", "nullable": False, "collation": "NONE"},
        ),
        (
            "identity_policy_version",
            {"type": "INT UNSIGNED", "nullable": False, "collation": "NONE"},
        ),
    ]:
        raise RuntimeError("source_scope physical measurement shape drifted")
    registry_keys = (
        registry.get("primary_key"),
        registry.get("unique_keys"),
        registry.get("referential_unique_keys", []),
        [
            (index.get("attributes"), index.get("unique"))
            for index in registry.get("required_index", [])
        ],
    )
    if registry_keys != (
        ["policy_id"],
        [
            [
                "algorithm_version",
                "spam_artist_threshold",
                "spam_occurrence_threshold",
                "content_owner_rule_version",
                "gid_winner_rule_version",
            ],
        ],
        [
            ["policy_id", "algorithm_version"],
            ["policy_id", "spam_artist_threshold"],
            ["policy_id", "spam_occurrence_threshold"],
            ["policy_id", "content_owner_rule_version"],
            ["policy_id", "gid_winner_rule_version"],
        ],
        [],
    ):
        raise RuntimeError("analysis policy physical indexes drifted")
    staging_keys = (
        staging.get("primary_key"),
        staging.get("unique_keys"),
        staging.get("referential_unique_keys", []),
        [
            (index.get("attributes"), index.get("unique"))
            for index in staging.get("required_index", [])
        ],
    )
    if staging_keys != (
        ["request_sha256"],
        [],
        [],
        [(["staging_id"], False)],
    ):
        raise RuntimeError("staging request physical indexes drifted")
    source_scope_keys = (
        source_scope.get("primary_key"),
        source_scope.get("unique_keys"),
        source_scope.get("referential_unique_keys", []),
        [
            (index.get("attributes"), index.get("unique"))
            for index in source_scope.get("required_index", [])
        ],
    )
    if source_scope_keys != (
        ["scope_key"],
        [["source_provider", "source_root_sha256", "identity_policy_version"]],
        [
            ["scope_key", "source_provider"],
            ["scope_key", "source_root_sha256"],
            ["scope_key", "identity_policy_version"],
        ],
        [(["source_root_sha256"], False)],
    ):
        raise RuntimeError("source_scope physical indexes drifted")


def _coordinate_bytes(domain: bytes, coordinates: Sequence[int], length: int) -> bytes:
    payload = bytearray()
    block = 0
    while len(payload) < length:
        frame = (
            b"h2hdb-capacity-measurement-coordinate-v1\0"
            + MEASUREMENT_SEED.to_bytes(32, "big")
            + len(domain).to_bytes(4, "big")
            + domain
            + len(coordinates).to_bytes(4, "big")
            + b"".join(value.to_bytes(8, "big") for value in coordinates)
            + block.to_bytes(4, "big")
        )
        payload.extend(hashlib.sha256(frame).digest())
        block += 1
    return bytes(payload[:length])


def _coordinate_uuid_v4(domain: bytes, coordinates: Sequence[int]) -> bytes:
    value = bytearray(_coordinate_bytes(domain, coordinates, 16))
    value[6] = (value[6] & 0x0F) | 0x40
    value[8] = (value[8] & 0x3F) | 0x80
    return bytes(value)


def _registry_rows(
    *,
    row_count: int,
) -> Iterator[tuple[int, int, int, int, int, int]]:
    for row_index in range(row_count):
        ordinal = row_index + 1
        policy_id = int(f"{ordinal:063b}"[::-1], 2)
        yield (
            policy_id,
            ordinal,
            (1 << 63) - ordinal,
            (1 << 62) + ordinal,
            (1 << 32) - 1,
            (1 << 32) - 1,
        )


def _staging_rows(
    *,
    staging_id_count: int,
    request_indexes: range,
    request_domain: bytes,
    staging_id_domain: bytes,
) -> Iterator[tuple[bytes, bytes]]:
    for staging_index in range(staging_id_count):
        staging_id = _coordinate_uuid_v4(staging_id_domain, (staging_index,))
        for request_index in request_indexes:
            yield (
                _coordinate_bytes(
                    request_domain,
                    (staging_index, request_index),
                    32,
                ),
                staging_id,
            )


def _source_scope_rows(*, row_count: int) -> Iterator[tuple[bytes, bytes, bytes, int]]:
    for row_index in range(row_count):
        yield (
            _coordinate_bytes(b"source-scope", (row_index,), 32),
            b"filesystem",
            _coordinate_bytes(b"source-root", (row_index,), 32),
            1,
        )


def _batched[T](rows: Iterator[T], size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _create_tables(connection: _Connection) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {_REGISTRY_TABLE}")
        cursor.execute(f"DROP TABLE IF EXISTS {_STAGING_TABLE}")
        cursor.execute(f"DROP TABLE IF EXISTS {_SOURCE_SCOPE_TABLE}")
        cursor.execute(
            f"CREATE TABLE {_REGISTRY_TABLE} ("
            "policy_id BIGINT UNSIGNED NOT NULL, "
            "algorithm_version INT UNSIGNED NOT NULL, "
            "spam_artist_threshold BIGINT UNSIGNED NOT NULL, "
            "spam_occurrence_threshold BIGINT UNSIGNED NOT NULL, "
            "content_owner_rule_version INT UNSIGNED NOT NULL, "
            "gid_winner_rule_version INT UNSIGNED NOT NULL, "
            "PRIMARY KEY (policy_id), "
            "UNIQUE KEY uq_natural (algorithm_version, spam_artist_threshold, "
            "spam_occurrence_threshold, content_owner_rule_version, "
            "gid_winner_rule_version), "
            "UNIQUE KEY uq_policy_algorithm (policy_id, algorithm_version), "
            "UNIQUE KEY uq_policy_spam_artist (policy_id, spam_artist_threshold), "
            "UNIQUE KEY uq_policy_spam_occurrence "
            "(policy_id, spam_occurrence_threshold), "
            "UNIQUE KEY uq_policy_content_owner "
            "(policy_id, content_owner_rule_version), "
            "UNIQUE KEY uq_policy_gid_winner (policy_id, gid_winner_rule_version)"
            ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_nopad_bin"
        )
        cursor.execute(
            f"CREATE TABLE {_STAGING_TABLE} ("
            "request_sha256 BINARY(32) NOT NULL, "
            "staging_id BINARY(16) NOT NULL, "
            "PRIMARY KEY (request_sha256), "
            "KEY ix_staging (staging_id)"
            ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_nopad_bin"
        )
        cursor.execute(
            f"CREATE TABLE {_SOURCE_SCOPE_TABLE} ("
            "scope_key BINARY(32) NOT NULL, "
            "source_provider VARBINARY(64) NOT NULL, "
            "source_root_sha256 BINARY(32) NOT NULL, "
            "identity_policy_version INT UNSIGNED NOT NULL, "
            "PRIMARY KEY (scope_key), "
            "UNIQUE KEY uq_natural (source_provider, source_root_sha256, "
            "identity_policy_version), "
            "UNIQUE KEY uq_scope_provider (scope_key, source_provider), "
            "UNIQUE KEY uq_scope_root (scope_key, source_root_sha256), "
            "UNIQUE KEY uq_scope_version (scope_key, identity_policy_version), "
            "KEY ix_root (source_root_sha256)"
            ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_nopad_bin"
        )
        connection.commit()
    finally:
        cursor.close()


def _insert_registry_rows(
    connection: _Connection,
    *,
    row_count: int,
) -> None:
    cursor = connection.cursor()
    try:
        registry_sql = (
            f"INSERT INTO {_REGISTRY_TABLE} ("
            "policy_id, algorithm_version, spam_artist_threshold, "
            "spam_occurrence_threshold, content_owner_rule_version, "
            "gid_winner_rule_version) VALUES (%s, %s, %s, %s, %s, %s)"
        )
        for batch in _batched(_registry_rows(row_count=row_count), INSERT_BATCH_SIZE):
            cursor.executemany(registry_sql, batch)
            connection.commit()
    finally:
        cursor.close()


def _insert_staging_rows(
    connection: _Connection,
    *,
    staging_id_count: int,
    request_indexes: range,
    request_domain: bytes,
    staging_id_domain: bytes,
) -> None:
    cursor = connection.cursor()
    try:
        staging_sql = (
            f"INSERT INTO {_STAGING_TABLE} (request_sha256, staging_id) VALUES (%s, %s)"
        )
        for batch in _batched(
            _staging_rows(
                staging_id_count=staging_id_count,
                request_indexes=request_indexes,
                request_domain=request_domain,
                staging_id_domain=staging_id_domain,
            ),
            INSERT_BATCH_SIZE,
        ):
            cursor.executemany(staging_sql, batch)
            connection.commit()
    finally:
        cursor.close()


def _delete_staging_rows(
    connection: _Connection,
    *,
    staging_id_count: int,
    request_indexes: range,
) -> None:
    """Delete one complete deterministic full-cap fill without truncation."""

    cursor = connection.cursor()
    try:
        delete_sql = f"DELETE FROM {_STAGING_TABLE} WHERE request_sha256 = %s"
        rows = _staging_rows(
            staging_id_count=staging_id_count,
            request_indexes=request_indexes,
            request_domain=b"staging-request-churn",
            staging_id_domain=b"staging-id-churn",
        )
        for batch in _batched(rows, INSERT_BATCH_SIZE):
            cursor.executemany(
                delete_sql, [(request_sha256,) for request_sha256, _ in batch]
            )
            connection.commit()
    finally:
        cursor.close()


def _insert_source_scope_rows(
    connection: _Connection,
    *,
    row_count: int,
) -> None:
    cursor = connection.cursor()
    try:
        source_scope_sql = (
            f"INSERT INTO {_SOURCE_SCOPE_TABLE} (scope_key, source_provider, "
            "source_root_sha256, identity_policy_version) VALUES (%s, %s, %s, %s)"
        )
        for batch in _batched(
            _source_scope_rows(row_count=row_count), INSERT_BATCH_SIZE
        ):
            cursor.executemany(source_scope_sql, batch)
            connection.commit()
    finally:
        cursor.close()


def _analyze(connection: _Connection, *tables: str) -> None:
    cursor = connection.cursor()
    try:
        for table in tables:
            cursor.execute(f"ANALYZE TABLE {table}")
            cursor.fetchall()
    finally:
        cursor.close()


def _table_storage_settings(
    connection: _Connection,
    table: str,
) -> tuple[str, str, str]:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT ENGINE, ROW_FORMAT, TABLE_COLLATION "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table,),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None or len(row) != 3:
        raise RuntimeError(f"missing storage settings for {table}")
    engine, row_format, table_collation = (str(value) for value in row)
    normalized = (engine.upper(), row_format.upper(), table_collation)
    expected = (
        EXPECTED_ENGINE,
        EXPECTED_ROW_FORMAT,
        EXPECTED_TABLE_COLLATION,
    )
    if normalized != expected:
        raise RuntimeError(
            f"{table} storage settings must be {expected!r}, got {normalized!r}"
        )
    return normalized


def _assert_created_storage_settings(connection: _Connection) -> None:
    for table in (_REGISTRY_TABLE, _STAGING_TABLE, _SOURCE_SCOPE_TABLE):
        _table_storage_settings(connection, table)


def _measurement(connection: _Connection, table: str) -> dict[str, object]:
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        exact_row = cursor.fetchone()
        cursor.execute(
            "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table,),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
    if exact_row is None or len(exact_row) != 1:
        raise RuntimeError(f"missing exact row count for {table}")
    if row is None or len(row) != 3:
        raise RuntimeError(f"missing information_schema measurement for {table}")
    estimated_rows, data_bytes, index_bytes = (int(value) for value in row)
    engine, row_format, table_collation = _table_storage_settings(connection, table)
    return {
        "actual_rows": int(exact_row[0]),
        "information_schema_estimated_rows": estimated_rows,
        "data_bytes": data_bytes,
        "index_bytes": index_bytes,
        "total_bytes": data_bytes + index_bytes,
        "engine": engine,
        "row_format": row_format,
        "table_collation": table_collation,
    }


def _server_environment(connection: _Connection) -> tuple[str, int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT VERSION(), @@innodb_page_size")
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None or len(row) != 2:
        raise RuntimeError("MariaDB server environment query returned an invalid row")
    version = str(row[0])
    if not version.startswith("10.11.11"):
        raise RuntimeError(
            f"capacity benchmark requires MariaDB 10.11.11, got {version}"
        )
    page_size = int(row[1])
    if page_size != EXPECTED_INNODB_PAGE_SIZE:
        raise RuntimeError(
            "capacity benchmark requires innodb_page_size "
            f"{EXPECTED_INNODB_PAGE_SIZE}, got {page_size}"
        )
    return version, page_size


def _container_connection() -> tuple[AbstractContextManager[Any], _Connection]:
    try:
        import mysql.connector
        from testcontainers.community.mysql import MySqlContainer
    except ImportError as error:  # pragma: no cover - manual environment boundary
        raise RuntimeError(
            "manual capacity benchmark requires project development dependencies"
        ) from error
    container = MySqlContainer(
        image=MARIADB_IMAGE,
        username="capacity",
        password="capacity",
        dbname="capacity",
    )
    manager = ExitStack()
    try:
        container.start()
        manager.callback(container.stop)
        connection = mysql.connector.connect(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(container.port)),
            user="capacity",
            password="capacity",
            database="capacity",
        )
    except BaseException:
        manager.close()
        raise
    return manager, cast(_Connection, connection)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args(argv)

    _assert_measured_shapes()
    physical = _load(PHYSICAL_PATH)
    operational_physical = _load(OPERATIONAL_PHYSICAL_PATH)
    registry_shape_sha256 = _shape_sha256(_relation(physical, "analysis_policy"))
    source_scope_shape_sha256 = _shape_sha256(_relation(physical, "source_scope"))
    staging_shape_sha256 = _shape_sha256(
        _relation(operational_physical, "gallery_observation_staging_request")
    )
    manager, connection = _container_connection()
    try:
        with manager:
            version, innodb_page_size = _server_environment(connection)
            _create_tables(connection)
            _assert_created_storage_settings(connection)
            _insert_registry_rows(connection, row_count=50_000)
            _insert_source_scope_rows(connection, row_count=SOURCE_SCOPE_ROW_COUNT)
            _insert_staging_rows(
                connection,
                staging_id_count=STAGING_ID_COUNT,
                request_indexes=range(ACCEPTED_REQUESTS_PER_STAGING),
                request_domain=b"staging-request-churn",
                staging_id_domain=b"staging-id-churn",
            )
            _analyze(connection, _STAGING_TABLE)
            staging_churn_full = _measurement(connection, _STAGING_TABLE)
            if staging_churn_full["actual_rows"] != (
                STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING
            ):
                raise RuntimeError("staging churn full fill has the wrong row count")
            _delete_staging_rows(
                connection,
                staging_id_count=STAGING_ID_COUNT,
                request_indexes=range(ACCEPTED_REQUESTS_PER_STAGING),
            )
            _analyze(connection, _STAGING_TABLE)
            staging_post_churn_empty = _measurement(connection, _STAGING_TABLE)
            if staging_post_churn_empty["actual_rows"] != 0:
                raise RuntimeError("staging churn did not delete every synthetic row")
            _insert_staging_rows(
                connection,
                staging_id_count=STAGING_ID_COUNT,
                request_indexes=range(ACCEPTED_REQUESTS_PER_STAGING),
                request_domain=b"staging-request-accepted",
                staging_id_domain=b"staging-id-accepted",
            )
            _analyze(
                connection,
                _REGISTRY_TABLE,
                _SOURCE_SCOPE_TABLE,
                _STAGING_TABLE,
            )
            registry = _measurement(connection, _REGISTRY_TABLE)
            source_scope = _measurement(connection, _SOURCE_SCOPE_TABLE)
            staging_accepted = _measurement(connection, _STAGING_TABLE)
            _insert_staging_rows(
                connection,
                staging_id_count=STAGING_ID_COUNT,
                request_indexes=range(
                    ACCEPTED_REQUESTS_PER_STAGING,
                    OVER_CAPACITY_REQUESTS_PER_STAGING,
                ),
                request_domain=b"staging-request-accepted",
                staging_id_domain=b"staging-id-accepted",
            )
            _analyze(connection, _STAGING_TABLE)
            staging_over_capacity = _measurement(connection, _STAGING_TABLE)
    finally:
        connection.close()

    receipt = {
        "measurement_version": 1,
        "execution_mode": EXECUTION_MODE,
        "mariadb_version": version,
        "mariadb_image": MARIADB_IMAGE,
        "innodb_page_size": innodb_page_size,
        "benchmark_script_sha256": _sha256(Path(__file__)),
        "seed": MEASUREMENT_SEED,
        "insert_batch_size": INSERT_BATCH_SIZE,
        "insertion_order": INSERTION_ORDER,
        "registry": {
            "relation": "analysis_policy",
            "row_generator": REGISTRY_ROW_GENERATOR,
            "key_distribution": REGISTRY_KEY_DISTRIBUTION,
            "physical_shape_sha256": registry_shape_sha256,
            "row_count": 50_000,
            **registry,
        },
        "source_scope": {
            "relation": "source_scope",
            "row_generator": SOURCE_SCOPE_ROW_GENERATOR,
            "key_distribution": SOURCE_SCOPE_KEY_DISTRIBUTION,
            "physical_shape_sha256": source_scope_shape_sha256,
            "row_count": SOURCE_SCOPE_ROW_COUNT,
            **source_scope,
        },
        "staging_churn_full": {
            "relation": "gallery_observation_staging_request",
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "physical_shape_sha256": staging_shape_sha256,
            "row_count": STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING,
            **staging_churn_full,
        },
        "staging_churn_empty": {
            "relation": "gallery_observation_staging_request",
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "physical_shape_sha256": staging_shape_sha256,
            "inserted_rows": (STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING),
            "deleted_rows": STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING,
            "insert_commit_count": (
                STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING // INSERT_BATCH_SIZE
            ),
            "delete_commit_count": (
                STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING // INSERT_BATCH_SIZE
            ),
            "residual_live_rows": 0,
            **staging_post_churn_empty,
        },
        "staging_accepted": {
            "relation": "gallery_observation_staging_request",
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "physical_shape_sha256": staging_shape_sha256,
            "staging_id_count": STAGING_ID_COUNT,
            "requests_per_staging_id": ACCEPTED_REQUESTS_PER_STAGING,
            "row_count": STAGING_ID_COUNT * ACCEPTED_REQUESTS_PER_STAGING,
            **staging_accepted,
        },
        "staging_over_capacity_diagnostic": {
            "relation": "gallery_observation_staging_request",
            "row_generator": STAGING_ROW_GENERATOR,
            "key_distribution": STAGING_KEY_DISTRIBUTION,
            "physical_shape_sha256": staging_shape_sha256,
            "staging_id_count": STAGING_ID_COUNT,
            "requests_per_staging_id": OVER_CAPACITY_REQUESTS_PER_STAGING,
            "row_count": STAGING_ID_COUNT * OVER_CAPACITY_REQUESTS_PER_STAGING,
            **staging_over_capacity,
        },
    }
    arguments.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
