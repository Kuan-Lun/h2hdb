from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from test_vnext_publication_candidate_repository import (
    _authorities,
    _canonical_identity,
)
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    inspect_one,
    open_generated_database,
    set_foreign_key_checks,
)

from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_consumer import (
    CanonicalConsumerBatch,
    CanonicalConsumerValue,
)
from h2hdb.vnext_canonical_value_family import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
)
from h2hdb.vnext_canonical_value_repository import CanonicalValueUploadPlan
from h2hdb.vnext_transaction import VNextUnitOfWork

_DOMAIN = b"catalog_summary_utf8_v1"
_CONSUMER = b"first-child"


@pytest.fixture
def connector(database_factory: DatabaseFactory) -> Iterator[SQLConnector]:
    database = open_generated_database(database_factory.config())
    try:
        _authorities(database)
        yield database
    finally:
        database.close()


def _seed(connector: SQLConnector, count: int) -> tuple[CanonicalConsumerValue, ...]:
    values: list[CanonicalConsumerValue] = []
    with connector.transaction():
        for serial in range(count):
            payload = str(serial).encode()
            plan = CanonicalValueUploadPlan.from_parts(_DOMAIN.decode(), (payload,))
            try:
                digest = plan.value_sha256
                _canonical_identity(
                    connector, digest, domain=_DOMAIN, serial=serial, payload=payload
                )
                connector.execute(
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (%s, %s)",
                    (1, digest),
                )
                values.append(
                    CanonicalConsumerValue(digest, _DOMAIN, len(payload), _CONSUMER)
                )
            finally:
                plan.close()
    return tuple(sorted(values, key=lambda value: value.value_sha256))


def _require_all(
    batch: CanonicalConsumerBatch, values: tuple[CanonicalConsumerValue, ...]
) -> None:
    for value in values:
        batch.require(value.value_sha256, expected_domain=_DOMAIN, consumer=_CONSUMER)


@pytest.mark.parametrize("count", [1, 2, 5, 128, 129])
def test_handoff_queries_are_per_page_and_claims_survive_until_finish(
    connector: SQLConnector, count: int
) -> None:
    values = _seed(connector, count)
    with connector.transaction():
        work = VNextUnitOfWork(connector, backend=connector_backend(connector))
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as reads:
            batch = CanonicalConsumerBatch(
                work, generation=1, values=values, consumers=(_CONSUMER,)
            )
            assert reads.call_count == 2 * ((count + 127) // 128)
            _require_all(batch, values)
            assert reads.call_count == 2 * ((count + 127) // 128)
        assert inspect_one(
            connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
        ) == (count,)
        with patch.object(
            connector, "execute_affected", wraps=connector.execute_affected
        ) as deletes:
            batch.finish()
            assert deletes.call_count == (count + 127) // 128
        assert inspect_one(
            connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
        ) == (0,)


def test_partial_handoff_rolls_back_even_after_a_full_delete_page(
    connector: SQLConnector,
) -> None:
    values = _seed(connector, 129)
    original = connector.execute_affected
    calls = 0

    def lose_second_page(query: str, data: tuple[object, ...] = ()) -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("lost connection before last page")
        return original(query, data)

    with pytest.raises(RuntimeError, match="lost connection"):
        with connector.transaction():
            batch = CanonicalConsumerBatch(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                generation=1,
                values=values,
                consumers=(_CONSUMER,),
            )
            _require_all(batch, values)
            with patch.object(
                connector, "execute_affected", side_effect=lose_second_page
            ):
                batch.finish()
    assert inspect_one(
        connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
    ) == (129,)
    with connector.transaction():
        replay = CanonicalConsumerBatch(
            VNextUnitOfWork(connector, backend=connector_backend(connector)),
            generation=1,
            values=values,
            consumers=(_CONSUMER,),
        )
        _require_all(replay, values)
        replay.finish()
    assert inspect_one(
        connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
    ) == (0,)


def test_missing_claim_at_last_page_fails_before_handoff(
    connector: SQLConnector,
) -> None:
    values = _seed(connector, 129)
    with connector.transaction():
        connector.execute(
            "DELETE FROM operational_canonical_value_uploads WHERE value_sha256 = %s",
            (values[-1].value_sha256,),
        )
    with pytest.raises(CanonicalValueNotReadyError, match="exact generation"):
        with connector.transaction():
            CanonicalConsumerBatch(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                generation=1,
                values=values,
                consumers=(_CONSUMER,),
            )
    assert inspect_one(
        connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
    ) == (128,)


def test_unconsumed_claim_or_wrong_domain_cannot_finish(
    connector: SQLConnector,
) -> None:
    values = _seed(connector, 2)
    with connector.transaction():
        batch = CanonicalConsumerBatch(
            VNextUnitOfWork(connector, backend=connector_backend(connector)),
            generation=1,
            values=values,
            consumers=(_CONSUMER,),
        )
        batch.require(
            values[0].value_sha256, expected_domain=_DOMAIN, consumer=_CONSUMER
        )
        with pytest.raises(CanonicalValueCollisionError, match="domain differs"):
            batch.require(
                values[1].value_sha256, expected_domain=b"wrong", consumer=_CONSUMER
            )
        with pytest.raises(CanonicalValueCollisionError, match="exact planned claim"):
            batch.finish()
    assert inspect_one(
        connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
    ) == (2,)


def test_partial_sealed_family_fails_closed(connector: SQLConnector) -> None:
    values = _seed(connector, 2)
    # Inject out-of-band corruption that the normal foreign keys prevent.
    set_foreign_key_checks(connector, enabled=False)
    with connector.transaction():
        connector.execute(
            "DELETE FROM catalog_canonical_value_allocation_digest_domains WHERE value_sha256 = %s",
            (values[-1].value_sha256,),
        )
    set_foreign_key_checks(connector, enabled=True)
    with pytest.raises(CanonicalValueCollisionError, match="incomplete allocation"):
        with connector.transaction():
            CanonicalConsumerBatch(
                VNextUnitOfWork(connector, backend=connector_backend(connector)),
                generation=1,
                values=values,
                consumers=(_CONSUMER,),
            )
    assert inspect_one(
        connector, "SELECT COUNT(*) FROM operational_canonical_value_uploads"
    ) == (2,)
