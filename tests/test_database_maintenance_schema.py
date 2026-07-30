from collections.abc import Iterator

import pytest

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.sql_connector import DatabaseDuplicateKeyError


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def test_create_main_tables_adds_and_preserves_singleton_maintenance_state(
    db: H2HDB,
) -> None:
    existing_request = db.request_download(812345)

    with db.SQLConnector() as connector:
        connector.execute("DROP TABLE database_maintenance_state")
        assert connector.check_table_exists("database_maintenance_state") is False

    # A database created by the previous release has every other schema object
    # but not this additive state table.
    db.create_main_tables()

    with db.SQLConnector() as connector:
        state_row = connector.fetch_one("""
            SELECT
                state_id,
                accumulated_work,
                last_evaluated_at,
                last_optimized_at
            FROM database_maintenance_state
            """)
        connector.execute("""
            UPDATE database_maintenance_state
            SET accumulated_work = 37,
                last_evaluated_at = 123456789,
                last_optimized_at = 123456700
            WHERE state_id = 1
            """)

    assert state_row == (1, 0, None, None)

    db.create_main_tables()
    db.create_main_tables()

    with db.SQLConnector() as connector:
        assert connector.fetch_all("""
            SELECT
                state_id,
                accumulated_work,
                last_evaluated_at,
                last_optimized_at
            FROM database_maintenance_state
            """) == [(1, 37, 123456789, 123456700)]

    assert db.get_download_request(existing_request.gid) == existing_request

    with pytest.raises(DatabaseDuplicateKeyError):
        with db.SQLConnector() as connector:
            connector.execute("""
                INSERT INTO database_maintenance_state (
                    state_id,
                    accumulated_work,
                    last_evaluated_at,
                    last_optimized_at
                )
                VALUES (2, 0, NULL, NULL)
                """)

    with db.SQLConnector() as connector:
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM database_maintenance_state"
        ) == (1,)
