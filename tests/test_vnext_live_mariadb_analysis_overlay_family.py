from __future__ import annotations

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_analysis_overlay_family import (
    load_analysis_impacted_content_key_family,
    load_analysis_impacted_gid_key_family,
    record_analysis_impacted_content_provenance_page,
    record_analysis_impacted_gid_provenance_page,
)


def _connector(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    return MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )


def test_live_mariadb_provenance_preflight_preserves_raw_binary_keys(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
    analysis = b"binary-key-test!"
    content_sha256 = bytes(range(0x80, 0xA0))
    with _connector(mariadb_config) as connector:
        # This storage-level regression deliberately isolates the family query
        # from unrelated analysis/gallery parents. MariaDB still enforces every
        # family primary key and raw BINARY(32) comparison exercised below.
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        with connector.transaction():
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((1, content_sha256),),
            )
            record_analysis_impacted_gid_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((1, 17),),
            )

        # The second pages force preflight to rediscover the sealed families.
        # A parameter-only CTE coerces this invalid UTF-8 digest and attempts a
        # duplicate anchor insert instead of preserving witness gallery 1.
        with connector.transaction():
            record_analysis_impacted_content_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((2, content_sha256),),
            )
            record_analysis_impacted_gid_provenance_page(
                connector,
                analysis_id=analysis,
                entries=((2, 17),),
            )

        content_family = load_analysis_impacted_content_key_family(
            connector,
            analysis_id=analysis,
            content_sha256=content_sha256,
        )
        gid_family = load_analysis_impacted_gid_key_family(
            connector,
            analysis_id=analysis,
            gid=17,
        )
        assert content_family is not None
        assert content_family.content_sha256 == content_sha256
        assert content_family.witness_gallery_id == 1
        assert gid_family is not None and gid_family.witness_gallery_id == 1
        assert connector.fetch_all(
            "SELECT gallery_id, content_sha256 "
            "FROM catalog_a_impacted_content_provenance "
            "WHERE analysis_id = %s ORDER BY gallery_id",
            (analysis,),
        ) == [(1, content_sha256), (2, content_sha256)]
