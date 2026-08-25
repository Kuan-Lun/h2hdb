from __future__ import annotations

from typing import Any

from h2hdb import CoreConfig
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_catalog_reader_repository import VNextCatalogReaderRepository


def _generated_mariadb(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    connector = MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["mariadb"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def test_mariadb_selected_cte_preserves_binary_publication_keys(
    mariadb_config: CoreConfig,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    publication_key = b"\x80\xff" + b"publication-key-binary-value!".ljust(30, b"!")
    artifact_sha256 = b"\x81" + b"artifact-digest-value".ljust(31, b"!")
    semantics_sha256 = b"\x82" + b"semantics-digest-value".ljust(31, b"!")
    locator_sha256 = b"\x83" + b"locator-digest-value".ljust(31, b"!")
    assert all(
        len(value) == 32
        for value in (
            publication_key,
            artifact_sha256,
            semantics_sha256,
            locator_sha256,
        )
    )
    try:
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                "VALUES (%s, %s, %s)",
                (artifact_sha256, 123, locator_sha256),
            )
            connector.execute(
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
                (1, publication_key, artifact_sha256, semantics_sha256),
            )
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")

        reader = VNextCatalogReaderRepository(backend="mariadb")
        with connector.read_transaction():
            facts = reader._artifact_facts_for_publications(
                connector,
                revision=1,
                publication_keys=(publication_key,),
            )

        assert facts == {
            publication_key: (
                artifact_sha256,
                123,
                locator_sha256,
                semantics_sha256,
            )
        }
    finally:
        connector.close()
